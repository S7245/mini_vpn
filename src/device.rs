use crate::shared::errors::{FailureOrigin, FailurePhase, FailureResource, RetainedFailure};
use bytes::BytesMut;
use smoltcp::phy::{Device, DeviceCapabilities, Medium, RxToken, TxToken};
use std::collections::VecDeque;
use std::future::Future;
use std::io::{ErrorKind, Read};
use std::net::Ipv4Addr;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf}; // ⚠️ 极其重要：引入异步读写魔法
use tokio::sync::{Notify, mpsc};
use tokio::task::JoinHandle;
use tun::Device as TunDevice;

// 条件编译宏。这意味着如果在 Linux 系统上编译这段代码，编译器会自动忽略 PI 头逻辑，直接按标准处理。
#[cfg(target_os = "macos")]
const UTUN_IPV4_HEADER: [u8; 4] = [0, 0, 0, 2];

/// 虚拟 TUN 设备包装器：连接异步物理网卡与同步 smoltcp 协议栈的桥梁
pub struct VirtualTunDevice {
    backend: VirtualTunBackend,
    stopping: bool,
    shutdown_failure: Option<ErrorKind>,
    /// OS interface name when the platform exposes it (diagnostics only).
    interface_name: Option<String>,
    /// 刀14c：真实 TUN IP MTU。必须和 OS TUN MTU / smoltcp capability 保持一致。
    mtu: usize,
    /// 收货仓库：存放刚从网卡读出来、还没被 smoltcp 吃掉的一个完整 IP 包
    pub rx_buffer: Option<BytesMut>,
    /// 已完成旁路分类/SYN 检查、等待 smoltcp 在一次 poll 中按序消费的 TCP 包。
    iface_rx_queue: VecDeque<BytesMut>,
    /// 发货仓库：存放 smoltcp 已经打包好、排队等待发给物理网卡的 IP 包队列
    pub tx_queue: VecDeque<BytesMut>,
    flushed_tcp_packets: Vec<TunFlushedTcpPacket>,
}

enum VirtualTunBackend {
    Stopped,
    Direct(tun::AsyncDevice),
    Pumped {
        writer: WriteHalf<tun::AsyncDevice>,
        ingress: TunIngressPump,
    },
}

#[derive(Debug, Default)]
struct TunIngressPumpStats {
    packets: AtomicU64,
    bytes: AtomicU64,
    queue_high_water: AtomicU64,
    full_waits: AtomicU64,
    read_errors: AtomicU64,
    closed: AtomicU64,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct TunIngressPumpSnapshot {
    pub enabled: bool,
    pub packets: u64,
    pub bytes: u64,
    pub queue_high_water: u64,
    pub capacity_packets: usize,
    pub full_waits: u64,
    pub read_errors: u64,
    pub closed: u64,
}

impl TunIngressPumpStats {
    fn snapshot(&self, capacity_packets: usize) -> TunIngressPumpSnapshot {
        TunIngressPumpSnapshot {
            enabled: true,
            packets: self.packets.load(Ordering::Relaxed),
            bytes: self.bytes.load(Ordering::Relaxed),
            queue_high_water: self.queue_high_water.load(Ordering::Relaxed),
            capacity_packets,
            full_waits: self.full_waits.load(Ordering::Relaxed),
            read_errors: self.read_errors.load(Ordering::Relaxed),
            closed: self.closed.load(Ordering::Relaxed),
        }
    }
}

/// Local tasks, backend and owned queues are released; diagnostic shells may remain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeviceShutdownState {
    Settled,
    FailedSettled,
}

/// Non-cloneable shutdown result. Earlier I/O errors remain the caller's responsibility.
pub struct DeviceShutdown {
    failure_kind: Option<ErrorKind>,
    failure: Option<RetainedFailure>,
    additional_failures: u64,
}

impl DeviceShutdown {
    /// For implementations that have explicitly released all of their owned resources.
    pub fn settled() -> Self {
        Self::from_status(None)
    }

    /// Resources are released but the implementation failed. Its original error stays
    /// with its caller or owner; the implementation must preserve this kind on repeats.
    pub fn failed(kind: ErrorKind) -> Self {
        Self::from_status(Some(kind))
    }

    pub fn state(&self) -> DeviceShutdownState {
        if self.failure_kind.is_some() {
            DeviceShutdownState::FailedSettled
        } else {
            DeviceShutdownState::Settled
        }
    }

    pub fn io_kind(&self) -> Option<ErrorKind> {
        self.failure_kind
    }

    pub(crate) fn from_status(failure_kind: Option<ErrorKind>) -> Self {
        Self {
            failure_kind,
            failure: None,
            additional_failures: 0,
        }
    }

    /// Move the saturated extra-error delta once; repeated observations carry zero.
    pub(crate) fn take_additional_failures(&mut self) -> u64 {
        std::mem::take(&mut self.additional_failures)
    }

    pub(crate) fn take_failure(&mut self) -> Option<RetainedFailure> {
        self.failure.take()
    }
}

pub(crate) fn device_closed() -> std::io::Error {
    std::io::Error::from(ErrorKind::BrokenPipe)
}

/// The ingress role owns its receiver, one join handle and one bounded first-error slot.
/// Packet backing stays charged until dropped or moved to the device. The outer device,
/// writer and session are not settled here; lifecycle tracers live in tests/lifecycle.rs.
pub(crate) struct TunIngressPump {
    receiver: mpsc::Receiver<PumpPacket>,
    task: PumpTask,
    control: Arc<PumpControl>,
    capacity_packets: usize,
    error_delivered: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PumpExit {
    StopRequested,
    ConsumerClosed,
    Failed(PumpFailureId),
}

enum PumpTask {
    Running(JoinHandle<PumpExit>),
    Observed(PumpExit),
}

/// Identifies the sole failure slot within this physical pump role, not a global registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PumpFailureId;

enum PumpFailureSlot {
    Empty,
    Retained(RetainedFailure),
    Transferred(ErrorKind),
}

struct PumpControl {
    stop: AtomicBool,
    stop_wake: Notify,
    failure_wake: Notify,
    failed: AtomicBool,
    first_failure: Mutex<PumpFailureSlot>,
    additional_failures: AtomicU64,
    producer_backing: AtomicU64,
    queued_backing: AtomicU64,
    stats: TunIngressPumpStats,
}

impl PumpControl {
    fn new() -> Self {
        Self {
            stop: AtomicBool::new(false),
            stop_wake: Notify::new(),
            failure_wake: Notify::new(),
            failed: AtomicBool::new(false),
            first_failure: Mutex::new(PumpFailureSlot::Empty),
            additional_failures: AtomicU64::new(0),
            producer_backing: AtomicU64::new(0),
            queued_backing: AtomicU64::new(0),
            stats: TunIngressPumpStats::default(),
        }
    }

    fn origin(phase: FailurePhase) -> FailureOrigin {
        FailureOrigin {
            phase,
            resource: FailureResource::TunPump,
            resource_id: 0,
            attempt_id: None,
        }
    }

    fn record(&self, failure: RetainedFailure) {
        let mut slot = self.first_failure.lock().unwrap_or_else(|e| e.into_inner());
        if matches!(*slot, PumpFailureSlot::Empty) {
            *slot = PumpFailureSlot::Retained(failure);
            self.failed.store(true, Ordering::Release);
        } else {
            let _ =
                self.additional_failures
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                        Some(n.saturating_add(1))
                    });
        }
        drop(slot);
        self.failure_wake.notify_waiters();
    }

    fn exit(&self, terminal: PumpExit) -> PumpExit {
        if self.failed.load(Ordering::Acquire) {
            PumpExit::Failed(PumpFailureId)
        } else {
            terminal
        }
    }

    fn request_stop(&self) {
        self.stop.store(true, Ordering::Release);
        self.stop_wake.notify_waiters();
    }

    async fn stopped(&self) {
        let notified = self.stop_wake.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if !self.stop.load(Ordering::Acquire) {
            notified.await;
        }
    }

    fn failure_kind(&self) -> ErrorKind {
        let slot = self.first_failure.lock().unwrap_or_else(|e| e.into_inner());
        match &*slot {
            PumpFailureSlot::Retained(failure) => {
                failure.facts().io_kind.unwrap_or(ErrorKind::Other)
            }
            PumpFailureSlot::Transferred(kind) => *kind,
            PumpFailureSlot::Empty => ErrorKind::Other,
        }
    }
}

impl TunIngressPump {
    pub(crate) fn request_stop(&self) {
        self.control.request_stop();
    }

    /// Cancellation leaves the same handle in this role until JoinHandle is actually Ready.
    fn poll_join(&mut self, cx: &mut Context<'_>) -> Poll<PumpExit> {
        let result = match &mut self.task {
            PumpTask::Running(task) => std::task::ready!(Pin::new(task).poll(cx)),
            PumpTask::Observed(exit) => return Poll::Ready(self.control.exit(*exit)),
        };
        let exit = match result {
            Ok(exit) => self.control.exit(exit),
            Err(error) => {
                self.control.record(RetainedFailure::from_join(
                    PumpControl::origin(FailurePhase::DeviceJoin),
                    error,
                ));
                PumpExit::Failed(PumpFailureId)
            }
        };
        self.task = PumpTask::Observed(exit);
        Poll::Ready(exit)
    }

    async fn observe_join(&mut self) -> PumpExit {
        std::future::poll_fn(|cx| self.poll_join(cx)).await
    }

    pub(crate) fn poll_shutdown(&mut self, cx: &mut Context<'_>) -> Poll<DeviceShutdown> {
        self.request_stop();
        let exit = std::task::ready!(self.poll_join(cx));
        self.drain();
        let mut result = DeviceShutdown::settled();
        result.additional_failures = self.control.additional_failures.swap(0, Ordering::Relaxed);
        if matches!(exit, PumpExit::Failed(_)) {
            result.failure_kind = Some(self.control.failure_kind());
            result.failure = self.take_failure();
        }
        Poll::Ready(result)
    }

    pub(crate) fn snapshot(&self) -> TunIngressPumpSnapshot {
        self.control.stats.snapshot(self.capacity_packets)
    }

    fn take_failure(&mut self) -> Option<RetainedFailure> {
        let mut slot = self
            .control
            .first_failure
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let kind = match &*slot {
            PumpFailureSlot::Retained(failure) => {
                failure.facts().io_kind.unwrap_or(ErrorKind::Other)
            }
            _ => return None,
        };
        match std::mem::replace(&mut *slot, PumpFailureSlot::Transferred(kind)) {
            PumpFailureSlot::Retained(failure) => Some(failure),
            _ => None,
        }
    }

    pub(crate) async fn recv(&mut self) -> Option<std::io::Result<BytesMut>> {
        loop {
            match self.try_recv() {
                Ok(event) => return Some(event),
                Err(mpsc::error::TryRecvError::Disconnected) => return None,
                Err(mpsc::error::TryRecvError::Empty) => {}
            }
            let control = Arc::clone(&self.control);
            let notified = control.failure_wake.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            match self.try_recv() {
                Ok(event) => return Some(event),
                Err(mpsc::error::TryRecvError::Disconnected) => return None,
                Err(mpsc::error::TryRecvError::Empty) => {}
            }
            tokio::select! {
                biased;
                _ = &mut notified, if !self.error_delivered => continue,
                packet = self.receiver.recv() => {
                    if let Some(packet) = packet {
                        return Some(Ok(packet.into_bytes()));
                    }
                    // Re-enter the ready path to deliver a retained terminal error once.
                }
            }
        }
    }

    pub(crate) fn try_recv(
        &mut self,
    ) -> Result<std::io::Result<BytesMut>, mpsc::error::TryRecvError> {
        match self.receiver.try_recv() {
            Ok(packet) => Ok(Ok(packet.into_bytes())),
            Err(status) => {
                if self.error_delivered || !self.control.failed.load(Ordering::Acquire) {
                    return Err(status);
                }
                // Recheck after acquiring failure publication: every earlier packet was
                // enqueued before that publication, including a concurrent first check.
                if let Ok(packet) = self.receiver.try_recv() {
                    return Ok(Ok(packet.into_bytes()));
                }
                self.error_delivered = true;
                Ok(Err(std::io::Error::from(self.control.failure_kind())))
            }
        }
    }

    fn drain(&mut self) {
        while self.receiver.try_recv().is_ok() {}
    }
}

impl Drop for TunIngressPump {
    fn drop(&mut self) {
        self.request_stop();
    }
}

#[derive(Clone, Copy)]
enum PacketCustody {
    Producer,
    Queue,
    Transferred,
}

pub(crate) struct PumpPacket {
    bytes: BytesMut,
    backing: u64,
    custody: PacketCustody,
    control: Arc<PumpControl>,
}

impl PumpPacket {
    fn new(control: Arc<PumpControl>) -> Self {
        Self {
            bytes: BytesMut::new(),
            backing: 0,
            custody: PacketCustody::Producer,
            control,
        }
    }

    /// Adopt synchronously before the source can yield; empty packets retain their backing.
    pub(crate) fn adopt(&mut self, bytes: BytesMut) {
        self.release_charge();
        self.bytes = bytes;
        self.backing = self.bytes.capacity() as u64;
        self.custody = PacketCustody::Producer;
        self.control
            .producer_backing
            .fetch_add(self.backing, Ordering::Relaxed);
    }

    fn enqueue(&mut self) {
        self.control
            .producer_backing
            .fetch_sub(self.backing, Ordering::Relaxed);
        self.control
            .queued_backing
            .fetch_add(self.backing, Ordering::Relaxed);
        self.custody = PacketCustody::Queue;
    }

    fn release_charge(&mut self) {
        match self.custody {
            PacketCustody::Producer => &self.control.producer_backing,
            PacketCustody::Queue => &self.control.queued_backing,
            PacketCustody::Transferred => return,
        }
        .fetch_sub(self.backing, Ordering::Relaxed);
        self.custody = PacketCustody::Transferred;
    }

    fn into_bytes(mut self) -> BytesMut {
        self.release_charge();
        std::mem::take(&mut self.bytes)
    }
}

impl Drop for PumpPacket {
    fn drop(&mut self) {
        self.release_charge();
    }
}

/// Catches poll and destruction panics before Tokio can retain an opaque completed result.
struct PumpGuard<F> {
    inner: Option<Pin<Box<F>>>,
    control: Arc<PumpControl>,
    settled: bool,
}

impl<F> PumpGuard<F> {
    fn dispose(&mut self) {
        if let Some(inner) = self.inner.take() {
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(inner))) {
                self.control.record(RetainedFailure::from_panic(
                    PumpControl::origin(FailurePhase::DeviceJoin),
                    payload,
                ));
            }
        }
        if !self.settled {
            self.settled = true;
            self.control.stats.closed.fetch_add(1, Ordering::Relaxed);
        }
    }
}

impl<F: Future<Output = PumpExit>> Future for PumpGuard<F> {
    type Output = PumpExit;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let result = catch_unwind(AssertUnwindSafe(|| match this.inner.as_mut() {
            Some(inner) => inner.as_mut().poll(cx),
            None => Poll::Pending,
        }));
        match result {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(exit)) => {
                this.dispose();
                Poll::Ready(this.control.exit(exit))
            }
            Err(payload) => {
                this.control.record(RetainedFailure::from_panic(
                    PumpControl::origin(FailurePhase::DeviceRead),
                    payload,
                ));
                this.dispose();
                Poll::Ready(PumpExit::Failed(PumpFailureId))
            }
        }
    }
}

impl<F> Drop for PumpGuard<F> {
    fn drop(&mut self) {
        self.dispose();
    }
}

pub(crate) enum SourceRead {
    Packet,
    Eof,
}

/// Sources fill the borrowed owner slot before any await after acquiring a packet.
/// The source supplies framing; generic pump code neither interprets PI nor copies packets.
pub(crate) trait IngressPacketSource: Send + 'static {
    fn read_packet(
        &mut self,
        packet: &mut PumpPacket,
    ) -> impl Future<Output = std::io::Result<SourceRead>> + Send;
}

struct AsyncReadPacketSource<R> {
    reader: R,
    mtu: usize,
}

impl<R: AsyncRead + Unpin + Send + 'static> IngressPacketSource for AsyncReadPacketSource<R> {
    async fn read_packet(&mut self, packet: &mut PumpPacket) -> std::io::Result<SourceRead> {
        packet.adopt(BytesMut::zeroed(rx_buffer_capacity_for_mtu(self.mtu)));
        let bytes = self.reader.read(&mut packet.bytes).await?;
        packet.bytes.truncate(bytes);
        Ok(if bytes == 0 {
            SourceRead::Eof
        } else {
            SourceRead::Packet
        })
    }
}

fn spawn_tun_ingress_pump<R>(reader: R, mtu: usize, capacity_packets: usize) -> TunIngressPump
where
    R: AsyncRead + Unpin + Send + 'static,
{
    TunIngressPump::spawn_source(AsyncReadPacketSource { reader, mtu }, capacity_packets)
}

impl TunIngressPump {
    pub(crate) fn spawn_source<S: IngressPacketSource>(source: S, capacity_packets: usize) -> Self {
        let capacity_packets = capacity_packets.max(1);
        let (sender, receiver) = mpsc::channel(capacity_packets);
        let control = Arc::new(PumpControl::new());
        let inner = run_tun_ingress_pump(source, sender, Arc::clone(&control), capacity_packets);
        let task = tokio::spawn(PumpGuard {
            inner: Some(Box::pin(inner)),
            control: Arc::clone(&control),
            settled: false,
        });
        Self {
            receiver,
            task: PumpTask::Running(task),
            control,
            capacity_packets,
            error_delivered: false,
        }
    }
}

async fn run_tun_ingress_pump<S: IngressPacketSource>(
    mut source: S,
    sender: mpsc::Sender<PumpPacket>,
    control: Arc<PumpControl>,
    capacity_packets: usize,
) -> PumpExit {
    let stats = &control.stats;
    loop {
        let mut packet = PumpPacket::new(Arc::clone(&control));
        let result = tokio::select! {
            biased;
            _ = control.stopped() => return PumpExit::StopRequested,
            _ = sender.closed() => return PumpExit::ConsumerClosed,
            result = source.read_packet(&mut packet) => result,
        };
        let bytes = match result {
            Ok(SourceRead::Eof) => {
                control.record(RetainedFailure::from_io(
                    PumpControl::origin(FailurePhase::DeviceRead),
                    std::io::Error::from(ErrorKind::UnexpectedEof),
                ));
                return PumpExit::Failed(PumpFailureId);
            }
            Ok(SourceRead::Packet) => packet.bytes.len(),
            Err(error) if error.kind() == ErrorKind::Interrupted => {
                drop(error);
                tokio::task::yield_now().await;
                continue;
            }
            Err(error) => {
                stats.read_errors.fetch_add(1, Ordering::Relaxed);
                control.record(RetainedFailure::from_io(
                    PumpControl::origin(FailurePhase::DeviceRead),
                    error,
                ));
                return PumpExit::Failed(PumpFailureId);
            }
        };
        let occupied = capacity_packets.saturating_sub(sender.capacity());
        stats.queue_high_water.fetch_max(
            occupied.saturating_add(1).min(capacity_packets) as u64,
            Ordering::Relaxed,
        );
        let permit = match sender.try_reserve() {
            Ok(permit) => permit,
            Err(mpsc::error::TrySendError::Full(())) => {
                stats.full_waits.fetch_add(1, Ordering::Relaxed);
                tokio::select! {
                    biased;
                    _ = control.stopped() => return PumpExit::StopRequested,
                    permit = sender.reserve() => match permit {
                        Ok(permit) => permit,
                        Err(_) => return PumpExit::ConsumerClosed,
                    }
                }
            }
            Err(mpsc::error::TrySendError::Closed(())) => return PumpExit::ConsumerClosed,
        };
        packet.enqueue();
        permit.send(packet);
        stats.packets.fetch_add(1, Ordering::Relaxed);
        stats.bytes.fetch_add(bytes as u64, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TunFlushedTcpPacket {
    pub src_addr: Ipv4Addr,
    pub dst_addr: Ipv4Addr,
    pub src_port: u16,
    pub dst_port: u16,
    pub payload_bytes: usize,
}

// 让我们稍微打磨一下这个基础结构体，顺便给它加上一个创建实例的关联函数：
impl VirtualTunDevice {
    /// 构造函数
    pub fn new(device: tun::AsyncDevice, mtu: usize) -> Self {
        let interface_name = TunDevice::name(device.get_ref()).ok();
        Self {
            backend: VirtualTunBackend::Direct(device),
            stopping: false,
            shutdown_failure: None,
            interface_name,
            mtu,
            rx_buffer: None,
            iface_rx_queue: VecDeque::new(),
            tx_queue: VecDeque::new(),
            flushed_tcp_packets: Vec::new(),
        }
    }

    pub fn new_pumped(device: tun::AsyncDevice, mtu: usize, capacity_packets: usize) -> Self {
        let interface_name = TunDevice::name(device.get_ref()).ok();
        let (reader, writer): (ReadHalf<_>, WriteHalf<_>) = tokio::io::split(device);
        let capacity_packets = capacity_packets.max(1);
        let ingress = spawn_tun_ingress_pump(reader, mtu, capacity_packets);
        Self {
            backend: VirtualTunBackend::Pumped { writer, ingress },
            stopping: false,
            shutdown_failure: None,
            interface_name,
            mtu,
            rx_buffer: None,
            iface_rx_queue: VecDeque::new(),
            tx_queue: VecDeque::new(),
            flushed_tcp_packets: Vec::new(),
        }
    }

    // 主循环(tokio::select!) -> 异步方法(wait_for_rx()): 它是我们在“异步网卡”和“同步仓库”之间进货的桥梁
    // 当这个方法(wait_for_rx())被 .await 唤醒时，说明底层的 tun0 网卡来数据包了，我们需要把它读出来，存进 rx_buffer 这个“收货仓库”里，准备等下喂给 smoltcp。
    /// 异步进货：等待物理网卡吐出数据，存入 rx_buffer
    pub async fn wait_for_rx(&mut self) -> std::io::Result<()> {
        if self.stopping {
            return Err(device_closed());
        }
        if self.rx_buffer.is_some() {
            return Ok(());
        }
        let mut buf = match &mut self.backend {
            VirtualTunBackend::Stopped => return Err(device_closed()),
            VirtualTunBackend::Direct(device) => {
                let mut buf = BytesMut::zeroed(rx_buffer_capacity_for_mtu(self.mtu));
                let n = device.read(&mut buf).await?;
                buf.truncate(n);
                buf
            }
            VirtualTunBackend::Pumped { ingress, .. } => match ingress.recv().await {
                Some(Ok(packet)) => packet,
                Some(Err(error)) => return Err(error),
                None => {
                    return Err(std::io::Error::new(
                        ErrorKind::BrokenPipe,
                        "TUN ingress pump closed",
                    ));
                }
            },
        };
        let n = buf.len();
        self.store_rx_packet(std::mem::take(&mut buf), n)
    }

    fn store_rx_packet(&mut self, mut buf: BytesMut, n: usize) -> std::io::Result<()> {
        #[cfg(target_os = "macos")]
        {
            if n < UTUN_IPV4_HEADER.len() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "short utun packet",
                ));
            }
            buf.truncate(n);
            self.rx_buffer = Some(buf.split_off(UTUN_IPV4_HEADER.len()));
        }

        #[cfg(not(target_os = "macos"))]
        {
            // Linux TUN（Layer::L3 + IFF_NO_PI）没有 macOS utun 那 4 字节 PI 头，
            // 截到读出的 n 字节就是裸 IP 包，直接交给 smoltcp。
            // 中文要点：不要写 buf.advance(n) —— 那个属于 Buf trait 且返回 ()，会同时
            // 触发 trait 未导入 + 类型不匹配两个错误。
            buf.truncate(n);
            self.rx_buffer = Some(buf);
        }

        Ok(())
    }

    pub fn try_recv_rx(&mut self) -> std::io::Result<bool> {
        if self.stopping {
            return Err(device_closed());
        }
        if self.rx_buffer.is_some() {
            return Ok(true);
        }

        let packet = match &mut self.backend {
            VirtualTunBackend::Stopped => return Err(device_closed()),
            VirtualTunBackend::Direct(device) => {
                let mut buf = BytesMut::zeroed(rx_buffer_capacity_for_mtu(self.mtu));
                loop {
                    match device.get_mut().read(&mut buf) {
                        Ok(0) => return Ok(false),
                        Ok(n) => {
                            buf.truncate(n);
                            break buf;
                        }
                        Err(e) if e.kind() == ErrorKind::WouldBlock => return Ok(false),
                        Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                        Err(e) => return Err(e),
                    }
                }
            }
            VirtualTunBackend::Pumped { ingress, .. } => match ingress.try_recv() {
                Ok(Ok(packet)) => packet,
                Ok(Err(error)) => return Err(error),
                Err(mpsc::error::TryRecvError::Empty) => return Ok(false),
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    return Err(std::io::Error::new(
                        ErrorKind::BrokenPipe,
                        "TUN ingress pump closed",
                    ));
                }
            },
        };
        let n = packet.len();
        self.store_rx_packet(packet, n)?;
        Ok(true)
    }

    pub async fn flush_tx(&mut self) -> std::io::Result<()> {
        if self.stopping {
            return Err(device_closed());
        }
        // // 1. 从发货仓库里取一个包
        // let packet = self.tx_queue.pop_front().ok_or("发货仓库为空")?;
        // // 2. 异步写入网卡
        // self.device.write_all(&packet).await?;
        // // 3. 返回成功
        // Ok(())
        while let Some(packet) = self.tx_queue.pop_front() {
            let flushed_tcp_packet = summarize_flushed_tcp_packet(&packet);
            // 无论是 macOS 还是 Linux，发货仓库里的包已经是完美形态了，直接发！
            match &mut self.backend {
                VirtualTunBackend::Stopped => return Err(device_closed()),
                VirtualTunBackend::Direct(device) => device.write_all(&packet).await?,
                VirtualTunBackend::Pumped { writer, .. } => writer.write_all(&packet).await?,
            }
            if let Some(packet) = flushed_tcp_packet {
                self.flushed_tcp_packets.push(packet);
            }
        }
        Ok(())
    }

    /// 下行注入：把构造好的裸 IPv4/UDP 包塞进发货队列，等 `flush_tx` 发出。
    /// 中文要点：UDP relay 绕过 smoltcp，回程包由主循环用 etherparse 造好后经此注入。
    pub fn inject_ip_packet(&mut self, pkt: &[u8]) {
        if self.stopping {
            return;
        }
        push_injected(&mut self.tx_queue, pkt);
    }

    /// 只读窥视当前收货仓库（不取走）：classify_inbound / inspect_inbound_syn 热路径用。
    pub fn rx_peek(&self) -> Option<&[u8]> {
        if self.stopping {
            return None;
        }
        self.rx_buffer.as_deref()
    }

    /// 取走当前收货仓库（UDP relay 把包 take 走、不进 iface.poll）。
    pub fn rx_take(&mut self) -> Option<BytesMut> {
        if self.stopping {
            return None;
        }
        self.rx_buffer.take()
    }

    pub fn stage_rx_for_iface(&mut self) -> bool {
        if self.stopping {
            return false;
        }
        let Some(packet) = self.rx_buffer.take() else {
            return false;
        };
        self.iface_rx_queue.push_back(packet);
        true
    }

    pub fn take_flushed_tcp_packets(&mut self) -> Vec<TunFlushedTcpPacket> {
        std::mem::take(&mut self.flushed_tcp_packets)
    }

    pub fn tun_ingress_pump_snapshot(&self) -> TunIngressPumpSnapshot {
        match &self.backend {
            VirtualTunBackend::Stopped | VirtualTunBackend::Direct(_) => {
                TunIngressPumpSnapshot::default()
            }
            VirtualTunBackend::Pumped { ingress, .. } => {
                ingress.control.stats.snapshot(ingress.capacity_packets)
            }
        }
    }
}

impl Drop for VirtualTunDevice {
    fn drop(&mut self) {
        self.request_stop();
    }
}

/// 主循环与「TUN 设备」之间的 IO 接缝（knife1）。
///
/// 中文要点：把主循环对设备的全部依赖收口成一个 trait，使 `run_event_loop` 既能跑真 utun
/// (`VirtualTunDevice`)，也能跑内存回环设备（并发压测 harness），**生产与测试共用同一份循环**。
/// 同时要求 smoltcp `Device`（`iface.poll` 需要）。用泛型单态化承载（smoltcp `Device`
/// 含 GAT、非对象安全），生产热路径零 dyn 开销。
#[allow(async_fn_in_trait)] // 仅作泛型约束使用、从不 `dyn TunIo`，无需 auto-trait 边界
pub trait TunIo: Device {
    /// Close admission and wake owned background tasks. This does not claim settlement.
    fn request_stop(&mut self);
    /// Observe real joins and release this device's backend and owned queue backing.
    fn poll_shutdown(&mut self, cx: &mut Context<'_>) -> Poll<DeviceShutdown>;
    /// 异步进货：等待下一个 IP 包就绪并存入内部 rx 槽。
    async fn wait_for_rx(&mut self) -> std::io::Result<()>;
    /// 非阻塞进货：若已有 IP 包 ready，则存入内部 rx 槽；无包时返回 Ok(false)。
    fn try_recv_rx(&mut self) -> std::io::Result<bool>;
    /// 只读窥视当前 rx 槽（不取走）。
    fn rx_peek(&self) -> Option<&[u8]>;
    /// 取走当前 rx 槽。
    fn rx_take(&mut self) -> Option<BytesMut>;
    /// 将已分类的 TCP rx 包从原始单槽移交给 smoltcp FIFO。
    fn stage_rx_for_iface(&mut self) -> bool;
    /// True only when the staged FIFO is empty AND Device::receive consumes
    /// exclusively that FIFO. Unknown devices conservatively use full polling.
    fn can_stage_single_iface_rx(&self) -> bool {
        false
    }
    /// 异步发货：把发货队列里排队的包全部写出。
    async fn flush_tx(&mut self) -> std::io::Result<()>;
    /// 当前等待写入 OS TUN 的 IP 包字节数。默认 0，真实/回环设备可覆盖用于诊断本地 egress 进展。
    fn queued_tx_bytes(&self) -> usize {
        0
    }
    /// TCP payload packets successfully written by the latest `flush_tx` calls and not yet consumed.
    fn take_flushed_tcp_packets(&mut self) -> Vec<TunFlushedTcpPacket> {
        Vec::new()
    }
    fn tun_ingress_pump_snapshot(&self) -> TunIngressPumpSnapshot {
        TunIngressPumpSnapshot::default()
    }
    /// 下行注入：裸 IP 包入发货队列，等 `flush_tx` 发出。
    fn inject_ip_packet(&mut self, pkt: &[u8]);
    /// OS interface name when available. Diagnostics only; behavior must not depend on it.
    fn interface_name(&self) -> Option<&str> {
        None
    }
}

impl TunIo for VirtualTunDevice {
    fn request_stop(&mut self) {
        self.stopping = true;
        if let VirtualTunBackend::Pumped { ingress, .. } = &self.backend {
            ingress.request_stop();
        }
    }

    fn poll_shutdown(&mut self, cx: &mut Context<'_>) -> Poll<DeviceShutdown> {
        self.request_stop();
        let result = if let VirtualTunBackend::Pumped { ingress, .. } = &mut self.backend {
            std::task::ready!(ingress.poll_shutdown(cx))
        } else {
            DeviceShutdown::from_status(self.shutdown_failure)
        };
        self.shutdown_failure = result.io_kind();
        self.backend = VirtualTunBackend::Stopped;
        self.rx_buffer.take();
        drop(std::mem::take(&mut self.iface_rx_queue));
        drop(std::mem::take(&mut self.tx_queue));
        drop(std::mem::take(&mut self.flushed_tcp_packets));
        Poll::Ready(result)
    }

    // 委托既有 inherent 方法（inherent 优先于 trait 解析，不会递归）。
    async fn wait_for_rx(&mut self) -> std::io::Result<()> {
        VirtualTunDevice::wait_for_rx(self).await
    }
    fn try_recv_rx(&mut self) -> std::io::Result<bool> {
        VirtualTunDevice::try_recv_rx(self)
    }
    fn rx_peek(&self) -> Option<&[u8]> {
        VirtualTunDevice::rx_peek(self)
    }
    fn rx_take(&mut self) -> Option<BytesMut> {
        VirtualTunDevice::rx_take(self)
    }
    fn can_stage_single_iface_rx(&self) -> bool {
        self.iface_rx_queue.is_empty()
    }
    fn stage_rx_for_iface(&mut self) -> bool {
        VirtualTunDevice::stage_rx_for_iface(self)
    }
    async fn flush_tx(&mut self) -> std::io::Result<()> {
        VirtualTunDevice::flush_tx(self).await
    }
    fn queued_tx_bytes(&self) -> usize {
        self.tx_queue.iter().map(BytesMut::len).sum()
    }
    fn take_flushed_tcp_packets(&mut self) -> Vec<TunFlushedTcpPacket> {
        VirtualTunDevice::take_flushed_tcp_packets(self)
    }
    fn tun_ingress_pump_snapshot(&self) -> TunIngressPumpSnapshot {
        VirtualTunDevice::tun_ingress_pump_snapshot(self)
    }
    fn inject_ip_packet(&mut self, pkt: &[u8]) {
        VirtualTunDevice::inject_ip_packet(self, pkt)
    }
    fn interface_name(&self) -> Option<&str> {
        self.interface_name.as_deref()
    }
}

fn summarize_flushed_tcp_packet(packet: &[u8]) -> Option<TunFlushedTcpPacket> {
    let packet = tun_tx_ip_slice(packet)?;
    summarize_flushed_ip_tcp_packet(packet)
}

pub(crate) fn summarize_flushed_ip_tcp_packet(packet: &[u8]) -> Option<TunFlushedTcpPacket> {
    let parsed = etherparse::PacketHeaders::from_ip_slice(packet).ok()?;
    let etherparse::IpHeader::Version4(ipv4, _) = parsed.ip? else {
        return None;
    };
    let etherparse::TransportHeader::Tcp(tcp) = parsed.transport? else {
        return None;
    };
    if parsed.payload.is_empty() {
        return None;
    }
    Some(TunFlushedTcpPacket {
        src_addr: Ipv4Addr::from(ipv4.source),
        dst_addr: Ipv4Addr::from(ipv4.destination),
        src_port: tcp.source_port,
        dst_port: tcp.destination_port,
        payload_bytes: parsed.payload.len(),
    })
}

fn tun_tx_ip_slice(packet: &[u8]) -> Option<&[u8]> {
    #[cfg(target_os = "macos")]
    {
        packet.get(UTUN_IPV4_HEADER.len()..)
    }
    #[cfg(not(target_os = "macos"))]
    {
        Some(packet)
    }
}

/// 把一个裸 IPv4 包加帧后塞进发货队列。macOS utun 需要 4 字节 PI 头，Linux 不需要。
/// 中文要点：抽成纯函数便于单测帧格式（与 `TxToken::consume` 的加头逻辑一致）。
pub fn push_injected(queue: &mut VecDeque<BytesMut>, pkt: &[u8]) {
    #[cfg(target_os = "macos")]
    let mut framed = {
        let mut b = BytesMut::with_capacity(UTUN_IPV4_HEADER.len() + pkt.len());
        b.extend_from_slice(&UTUN_IPV4_HEADER);
        b
    };
    #[cfg(not(target_os = "macos"))]
    let mut framed = BytesMut::with_capacity(pkt.len());
    framed.extend_from_slice(pkt);
    queue.push_back(framed);
}

// ==========================================
// 下面是面向 smoltcp 的“同步面”实现
// ==========================================

/*
临时仓库” (Buffers/Queues)
- 收货仓库 (rx_buffer)：在主循环的 device.wait_for_rx().await 里，我们先异步地把网卡数据读出来，存进这个仓库。然后调用 iface.poll() 时，smoltcp 同步调用 receive()，我们就直接把仓库里的包递给它。
- 发货仓库 (tx_queue)：当 smoltcp 同步调用 transmit() 吐出回包（比如 SYN-ACK）时，我们不能立刻 .await 写入网卡，而是先把它塞进这个发货仓库。等 poll() 结束回到主循环，我们再慢慢用 write_all().await 把仓库里的包清空。
*/

/// 收货单：保管从 rx_buffer 拿出来的包裹
pub struct TunRxToken {
    buffer: BytesMut,
}

impl RxToken for TunRxToken {
    fn consume<R, F>(mut self, f: F) -> R
    where
        F: FnOnce(&mut [u8]) -> R,
    {
        // 将包裹拆开（转为可变切片）喂给 smoltcp 处理。
        // 注意：绝不在此 println! 整包字节——这是每个收包的热路径，同步 stdout 会拖垮
        // 单线程主循环（实测大并发下 TUN 缓冲溢出、UDP 大量丢包）。
        f(&mut self.buffer)
    }
}

/// 发货单：持有发货仓库的可变钥匙
pub struct TunTxToken<'a> {
    queue: &'a mut VecDeque<BytesMut>,
}

// “造箱子”：在发货仓库里造一个指定大小的箱子，准备把真实数据写进去
impl<'a> TxToken for TunTxToken<'a> {
    fn consume<R, F>(self, len: usize, f: F) -> R
    where
        F: FnOnce(&mut [u8]) -> R,
    {
        #[cfg(target_os = "macos")]
        {
            // 1. 造一个多出 4 字节的箱子
            let mut buffer = BytesMut::zeroed(4 + len);
            // 2. 提前把 PI 头印在箱子的最前面
            buffer[0..4].copy_from_slice(&[0, 0, 0, 2]);
            // 3. 接下来该让 smoltcp 填数据了
            // ❓ 任务：既然前 4 个字节已经被占用了，你应该如何写，才能把 buffer 中从索引 4 开始到最后的“剩余部分”，作为可变切片传给闭包 f 呢？
            let result = f(&mut buffer[4..]);
            self.queue.push_back(buffer);
            result
        }
        #[cfg(not(target_os = "macos"))]
        {
            // 1. 按 smoltcp 要求的长度，造一个填满 0 的新箱子
            // 1. 造一个指定大小的空箱子
            let mut buffer = BytesMut::zeroed(len);
            // 2. 把箱子交给 f，smoltcp 会把真实的 IP 数据写进去
            // 2. 让 smoltcp 把真实数据填进去
            let result = f(&mut buffer);
            // 3. 箱子装满了！现在把它塞进发货队列的尾部
            // 3. 扔进发货队列排队
            self.queue.push_back(buffer);
            // 4. 返回执行结果
            result
        }
    }
}

/// 正式登记为 smoltcp 认可的物理设备
impl Device for VirtualTunDevice {
    type RxToken<'a> = TunRxToken;
    type TxToken<'a> = TunTxToken<'a>;

    fn capabilities(&self) -> DeviceCapabilities {
        device_capabilities_for_mtu(self.mtu)
    }

    fn receive(
        &mut self,
        _timestamp: smoltcp::time::Instant,
    ) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        if self.stopping {
            return None;
        }
        // ⚠️ 这里的逻辑是将 rx_buffer 里的数据拿出来，打包成 RxToken
        // 如果仓库里有货，就 take() 拿走，打包成收货单和发货单交给 smoltcp
        // 如果仓库里没有货，就返回 None
        // println!("接收包：{:?}", self.rx_buffer);
        self.iface_rx_queue.pop_front().map(|buffer| {
            (
                TunRxToken { buffer },
                TunTxToken {
                    queue: &mut self.tx_queue,
                },
            )
        })
    }

    fn transmit(&mut self, _timestamp: smoltcp::time::Instant) -> Option<Self::TxToken<'_>> {
        if self.stopping {
            return None;
        }
        // 只需要返回一个持有 tx_queue 可变引用的 TxToken 即可
        // 直接给一张发货单，里面带着发货仓库的钥匙
        // （不在此 println! tx_queue——每次发包热路径，同步 stdout 会拖垮主循环）
        Some(TunTxToken {
            queue: &mut self.tx_queue,
        })
    }
}

fn rx_buffer_capacity_for_mtu(mtu: usize) -> usize {
    mtu + tun_packet_header_len()
}

fn tun_packet_header_len() -> usize {
    #[cfg(target_os = "macos")]
    {
        UTUN_IPV4_HEADER.len()
    }
    #[cfg(not(target_os = "macos"))]
    {
        0
    }
}

fn device_capabilities_for_mtu(mtu: usize) -> DeviceCapabilities {
    let mut caps = DeviceCapabilities::default();
    caps.max_transmission_unit = mtu;
    caps.medium = Medium::Ip;

    let mut cs = smoltcp::phy::ChecksumCapabilities::default();
    // 设置为 Tx 表示：接收 (Rx) 时不检查校验和，但发送 (Tx) 时强制计算校验和。
    cs.tcp = smoltcp::phy::Checksum::Tx;
    cs.ipv4 = smoltcp::phy::Checksum::Tx;
    cs.icmpv4 = smoltcp::phy::Checksum::Tx;
    caps.checksum = cs;

    caps
}

/*
设备能力：DeviceCapabilities { medium: Ip, max_transmission_unit: 1500, max_burst_size: None, checksum: ChecksumCapabilities { ipv4: Both, udp: Both, tcp: Both, icmpv4: Both } }
📡 收到来自操作系统的包，大小: 84 字节
🔍 包的前 4 字节: [69, 0, 0, 84]
接收包：Some([69, 0, 0, 84, 192, 228, 0, 0, 64, 1, 165, 194, 10, 0, 0, 1, 10, 0, 0, 2, 8, 0, 95, 174, 248, 212, 0, 3, 105, 235, 27, 20, 0, 1, 47, 118, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55])
收货单-RxToken:[69, 0, 0, 84, 192, 228, 0, 0, 64, 1, 165, 194, 10, 0, 0, 1, 10, 0, 0, 2, 8, 0, 95, 174, 248, 212, 0, 3, 105, 235, 27, 20, 0, 1, 47, 118, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55]
发货单-TxToken:84
接收包：None
*/
#[cfg(test)]
mod tests;
