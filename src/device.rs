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

fn exact_vec<T>(count: usize) -> std::io::Result<Vec<T>> {
    use std::alloc::{Layout, alloc};
    let layout =
        Layout::array::<T>(count).map_err(|_| std::io::Error::from(ErrorKind::InvalidInput))?;
    if layout.size() == 0 {
        return Err(ErrorKind::InvalidInput.into());
    }
    let ptr = unsafe { alloc(layout).cast::<T>() };
    if ptr.is_null() {
        return Err(ErrorKind::OutOfMemory.into());
    }
    // Exact nonzero allocation; length zero owns no uninitialized elements.
    Ok(unsafe { Vec::from_raw_parts(ptr, 0, count) })
}
pub(crate) mod cold;
mod failure;
pub use failure::DeviceIngressFailure;
pub(crate) mod cut;
mod callback;
mod rx_packet;
pub use rx_packet::RxPacket;
pub(crate) mod egress;
mod pump_control;
pub(crate) mod pump_native;
mod pump_work;
pub(crate) mod rx;
pub use cut::DeviceCutReceipt;

// 条件编译宏。这意味着如果在 Linux 系统上编译这段代码，编译器会自动忽略 PI 头逻辑，直接按标准处理。
#[cfg(target_os = "macos")]
const UTUN_IPV4_HEADER: [u8; 4] = [0, 0, 0, 2];

/// 虚拟 TUN 设备包装器：连接异步物理网卡与同步 smoltcp 协议栈的桥梁
pub struct VirtualTunDevice {
    backend: VirtualTunBackend,
    cut: cut::CutBoundary,
    stopping: bool,
    shutdown_failure: Option<ErrorKind>,
    shutdown_failure_transferred: bool,
    shutdown_callbacks: Option<egress::CallbackStatus>,
    /// OS interface name when the platform exposes it (diagnostics only).
    interface_name: Option<String>,
    /// 刀14c：真实 TUN IP MTU。必须和 OS TUN MTU / smoltcp capability 保持一致。
    mtu: usize,
    /// 收货仓库：存放刚从网卡读出来、还没被 smoltcp 吃掉的一个完整 IP 包
    rx: rx::RxStock,
    /// 发货仓库：存放 smoltcp 已经打包好、排队等待发给物理网卡的 IP 包队列
    egress: egress::Egress,
}

enum VirtualTunBackend {
    Stopped,
    Direct(tun::AsyncDevice),
    PreparedDirect {
        device: tun::AsyncDevice,
        runtime: tokio::runtime::Handle,
    },
    Pumped {
        writer: WriteHalf<tun::AsyncDevice>,
        ingress: pump_native::Startup<AsyncReadPacketSource<ReadHalf<tun::AsyncDevice>>>,
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

/// Local shutdown progress; pending callbacks must settle before a final verdict.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeviceShutdownState {
    PendingCallbacks,
    Settled,
    FailedSettled,
}

/// Non-cloneable shutdown result. Earlier I/O errors remain the caller's responsibility.
pub struct DeviceShutdown {
    failure_kind: Option<ErrorKind>,
    failure: Option<RetainedFailure>,
    failure_transferred: bool,
    additional_failures: u64,
    callback_tail: Option<egress::CallbackTail>,
    // Drop claimed source refs before the last guarded status ref.
    callback_claim: Option<egress::CallbackClaim>,
    callback_status: Option<egress::CallbackStatus>,
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
        if let Some(status) = &self.callback_status {
            let state = status.state();
            if state != DeviceShutdownState::Settled {
                return state;
            }
        }
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
            failure_transferred: false,
            additional_failures: 0,
            callback_tail: None,
            callback_claim: None,
            callback_status: None,
        }
    }

    pub(crate) fn with_callbacks(
        mut self,
        tail: Option<egress::CallbackTail>,
        status: Option<egress::CallbackStatus>,
    ) -> Self {
        self.callback_tail = tail;
        self.callback_status = status;
        self
    }
    pub(crate) async fn settle_callbacks(
        &mut self,
        origin: FailureOrigin,
    ) -> Option<egress::CallbackReport> {
        self.settle().await;
        self.callback_claim.take().map(|claim| claim.report(origin))
    }

    /// Wait for this receipt's original callback tail. Cancelling the wait keeps
    /// the tail in this receipt; a repeat receipt cannot claim for the original.
    /// No deadline is created or extended. Failure facts remain privately owned
    /// here for the original Session's one-time observation.
    /// A caller-supplied Waker may panic during polling. Owning drivers must
    /// catch that poll while borrowing their Device, as the Service driver does.
    pub async fn settle(&mut self) -> DeviceShutdownState {
        if let Some(tail) = self.callback_tail.as_mut() {
            self.callback_claim = Some(tail.settle().await);
            self.callback_tail.take();
        } else {
            // A repeat receipt cannot claim for the unique original tail.
            std::future::poll_fn(|cx| {
                if self.state() == DeviceShutdownState::PendingCallbacks {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                } else {
                    Poll::Ready(())
                }
            })
            .await;
        }
        self.state()
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
    receiver: pump_native::Receiver,
    selected: Option<PumpPacket>,
    task: PumpTask,
    control: Arc<PumpControl>,
    capacity_packets: usize,
    error_delivered: bool,
    cut_supported: bool,
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
    Callback(ErrorKind),
    CallbackTransferred(ErrorKind),
}

struct PumpControl {
    stop: AtomicBool,
    pause: cut::PauseControl,
    stop_wake: Arc<Notify>,
    failure_wake: Notify,
    failed: AtomicBool,
    first_failure: Mutex<pump_control::Failures>,
    native_meter: Option<bytes::BackingMeter>,
    native_lane: Option<quinn::owned_events::ChannelIdentity<PumpPacket>>,
    native_ingress: Option<rx_packet::Access>,
    notifications: pump_control::Notifications,
    additional_failures: AtomicU64,
    producer_backing: AtomicU64,
    queued_backing: AtomicU64,
    stats: TunIngressPumpStats,
    _native_layout: Option<bytes::BackingBudgetReservation>,
}

impl PumpControl {
    fn new() -> Self {
        Self {
            stop: AtomicBool::new(false),
            pause: cut::PauseControl::default(),
            stop_wake: Arc::new(Notify::new()),
            failure_wake: Notify::new(),
            failed: AtomicBool::new(false),
            first_failure: Mutex::new(pump_control::Failures::default()),
            native_meter: None,
            native_lane: None,
            native_ingress: None,
            notifications: pump_control::Notifications::default(),
            additional_failures: AtomicU64::new(0),
            producer_backing: AtomicU64::new(0),
            queued_backing: AtomicU64::new(0),
            stats: TunIngressPumpStats::default(),
            _native_layout: None,
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
        self.record_quiet(failure);
        self.notify(pump_control::FAILURE);
    }

    fn exit(&self, terminal: PumpExit) -> PumpExit {
        if self.failed.load(Ordering::Acquire) || self.native_quarantined() {
            PumpExit::Failed(PumpFailureId)
        } else {
            terminal
        }
    }

    fn request_stop(&self) {
        if !self.stop.swap(true, Ordering::AcqRel) {
            self.notify(pump_control::STOP);
        }
    }

    async fn stopped(&self) {
        let notified = self.stop_wake.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if !self.stop.load(Ordering::Acquire) {
            notified.await;
        }
    }

    fn take_failure(&self) -> Option<RetainedFailure> {
        let mut slot = self.lock_failures();
        let kind = match &slot.first {
            PumpFailureSlot::Retained(failure) => {
                failure.facts().io_kind.unwrap_or(ErrorKind::Other)
            }
            _ => return None,
        };
        match std::mem::replace(&mut slot.first, PumpFailureSlot::Transferred(kind)) {
            PumpFailureSlot::Retained(failure) => Some(failure),
            _ => None,
        }
    }

    fn failure_kind(&self) -> ErrorKind {
        let slot = self.lock_failures();
        match &slot.first {
            PumpFailureSlot::Retained(failure) => {
                failure.facts().io_kind.unwrap_or(ErrorKind::Other)
            }
            PumpFailureSlot::Transferred(kind) | PumpFailureSlot::Callback(kind) | PumpFailureSlot::CallbackTransferred(kind) => *kind,
            PumpFailureSlot::Empty => ErrorKind::Other,
        }
    }
}

impl TunIngressPump {
    pub(crate) fn take_failure(&mut self) -> Option<RetainedFailure> {
        self.control.take_failure()
    }

    pub(crate) fn request_stop(&self) {
        self.control.request_stop();
    }

    /// Cancellation leaves the same handle in this role until JoinHandle is actually Ready.
    fn poll_join(&mut self, cx: &mut Context<'_>) -> Poll<PumpExit> {
        if self.control.native_quarantined() {
            if let PumpTask::Running(task) = &self.task {
                task.abort();
            }
        }
        let result = match &mut self.task {
            PumpTask::Running(task) => std::task::ready!(Pin::new(task).poll(cx)),
            PumpTask::Observed(exit) => return Poll::Ready(self.control.exit(*exit)),
        };
        let exit = match result {
            Ok(exit) => self.control.exit(exit),
            Err(error) => {
                self.control.record_join(error);
                PumpExit::Failed(PumpFailureId)
            }
        };
        if self.control.native_quarantined() && !self.control.failed.load(Ordering::Acquire)
            && !self.control.record_callback_failure() {
            self.control.record_quiet(RetainedFailure::from_io(
                PumpControl::origin(FailurePhase::DeviceJoin),
                device_closed(),
            ));
        }
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
            result.failure = self.control.take_failure();
            result.failure_transferred = result.failure.is_none();
        }
        Poll::Ready(result)
    }

    pub(crate) fn snapshot(&self) -> TunIngressPumpSnapshot {
        self.control.stats.snapshot(self.capacity_packets)
    }

    pub(crate) async fn recv(&mut self) -> Option<std::io::Result<RxPacket>> {
        loop {
            if self.control.native_quarantined() && matches!(self.task, PumpTask::Running(_)) {
                self.observe_join().await;
            }
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
                        if let Some(bytes) = self.accept_packet(packet) { return Some(Ok(bytes)); }
                    }
                    // Re-enter the ready path to deliver a retained terminal error once.
                }
            }
        }
    }

    pub(crate) fn try_recv(
        &mut self,
    ) -> Result<std::io::Result<RxPacket>, mpsc::error::TryRecvError> {
        if self.control.native_quarantined() {
            if matches!(self.task, PumpTask::Running(_)) {
                return Err(mpsc::error::TryRecvError::Empty);
            }
            if self.error_delivered {
                return Err(mpsc::error::TryRecvError::Disconnected);
            }
            self.error_delivered = true;
            return Ok(Err(self.control.failure_error()));
        }
        match self.receiver.try_recv() {
            Ok(packet) => match self.accept_packet(packet) {
                Some(bytes) => Ok(Ok(bytes)),
                None => self.try_recv(),
            },
            Err(status) => {
                if self.error_delivered || !self.control.failed.load(Ordering::Acquire) {
                    return Err(status);
                }
                // Recheck after acquiring failure publication: every earlier packet was
                // enqueued before that publication, including a concurrent first check.
                if let Ok(packet) = self.receiver.try_recv() {
                    return match self.accept_packet(packet) {
                        Some(bytes) => Ok(Ok(bytes)),
                        None => self.try_recv(),
                    };
                }
                self.error_delivered = true;
                Ok(Err(self.control.failure_error()))
            }
        }
    }

    fn accept_packet(&mut self, packet: PumpPacket) -> Option<RxPacket> {
        if self.control.native_meter.is_some() && !packet.native_valid() {
            self.selected = Some(packet);
            self.control.quarantine_quiet();
            return None;
        }
        Some(packet.into_bytes())
    }
    fn drain(&mut self) {
        self.selected.take();
        self.receiver.drain();
    }
}

impl Drop for TunIngressPump {
    fn drop(&mut self) {
        if self.control.native_meter.is_some() && matches!(self.task, PumpTask::Running(_)) {
            self.control.quarantine_quiet();
            if let PumpTask::Running(task) = &self.task {
                task.abort();
            }
        }
        if !std::thread::panicking() {
            self.request_stop();
        }
    }
}

#[derive(Clone, Copy)]
enum PacketCustody {
    Producer,
    Queue,
    Transferred,
}

pub(crate) struct PumpPacket {
    bytes: RxPacket,
    backing: u64,
    adopted: bool,
    custody: PacketCustody,
    control: Arc<PumpControl>,
}

impl PumpPacket {
    fn new(control: Arc<PumpControl>) -> Self {
        Self {
            bytes: RxPacket::default(),
            backing: 0,
            adopted: false,
            custody: PacketCustody::Producer,
            control,
        }
    }

    /// Adopt synchronously before the source can yield; empty packets retain their backing.
    pub(crate) fn adopt(&mut self, bytes: impl Into<RxPacket>) {
        let bytes = bytes.into();
        if self
            .control
            .native_meter
            .as_ref()
            .is_some_and(|m| !bytes.precharged_backing_matches(m))
        {
            self.control.quarantine_quiet();
        }
        self.release_charge();
        self.bytes = bytes;
        self.backing = self.bytes.capacity() as u64;
        self.custody = PacketCustody::Producer;
        self.control
            .producer_backing
            .fetch_add(self.backing, Ordering::Relaxed);
        self.adopted = true;
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

    fn into_bytes(mut self) -> RxPacket {
        self.release_charge();
        std::mem::take(&mut self.bytes)
    }
}

impl Drop for PumpPacket {
    fn drop(&mut self) {
        if self.adopted
            && !matches!(self.custody, PacketCustody::Transferred)
            && self.control.native_meter.as_ref().is_some_and(|m| {
                !self.bytes.precharged_backing_matches(m)
                    || self.backing != self.bytes.capacity() as u64
            })
        {
            self.control.quarantine_quiet();
        }
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
                self.control.capture(pump_control::Site::WorkDrop, payload);
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
                this.control.capture(pump_control::Site::WorkPoll, payload);
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
    /// Cancelling read_packet releases acquired bytes through its owner slot.
    const CUT_SAFE: bool = false;
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
    const CUT_SAFE: bool = true;
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
        let source = pump_work::Source::new(source, Arc::clone(&control));
        let inner = run_tun_ingress_pump(source, sender, Arc::clone(&control), capacity_packets);
        let task = tokio::spawn(PumpGuard {
            inner: Some(Box::pin(inner)),
            control: Arc::clone(&control),
            settled: false,
        });
        Self {
            receiver: pump_native::Receiver::legacy(receiver, Arc::clone(&control)),
            selected: None,
            task: PumpTask::Running(task),
            control,
            capacity_packets,
            error_delivered: false,
            cut_supported: S::CUT_SAFE,
        }
    }
}

async fn run_tun_ingress_pump<S: IngressPacketSource>(
    mut source: pump_work::Source<S>,
    sender: mpsc::Sender<PumpPacket>,
    control: Arc<PumpControl>,
    capacity_packets: usize,
) -> PumpExit {
    let stats = &control.stats;
    loop {
        if control.pause.requested.load(Ordering::Acquire) {
            if !control.pause.park(&control).await {
                return PumpExit::StopRequested;
            }
        }
        let mut packet = PumpPacket::new(Arc::clone(&control));
        let result = tokio::select! {
            biased;
            _ = control.stopped() => return PumpExit::StopRequested,
            _ = sender.closed() => return PumpExit::ConsumerClosed,
            _ = control.pause.requested() => { control.pause.discard(packet); continue; },
            result = source.read_packet(&mut packet) => match result {
                Some(result) => result,
                None => return PumpExit::Failed(PumpFailureId),
            },
        };
        if control.failed.load(Ordering::Acquire) || control.native_quarantined() {
            return PumpExit::Failed(PumpFailureId);
        }
        let bytes = match result {
            Ok(SourceRead::Eof) => {
                control.record_io(std::io::Error::from(ErrorKind::UnexpectedEof));
                return PumpExit::Failed(PumpFailureId);
            }
            Ok(SourceRead::Packet) => packet.bytes.len(),
            Err(error) if error.kind() == ErrorKind::Interrupted => {
                if !control.dispose_interrupted(error) {
                    return PumpExit::Failed(PumpFailureId);
                }
                tokio::task::yield_now().await;
                continue;
            }
            Err(error) => {
                stats.read_errors.fetch_add(1, Ordering::Relaxed);
                control.record_io(error);
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
                    _ = control.pause.requested() => { control.pause.discard(packet); continue; },
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
    /// Actual payload sequence origin; SYN consumes one octet, FIN is not DATA.
    pub payload_start_seq: u32,
}

// 让我们稍微打磨一下这个基础结构体，顺便给它加上一个创建实例的关联函数：
impl VirtualTunDevice {
    fn unavailable(&self) -> std::io::Error {
        self.shutdown_callbacks.as_ref().map_or_else(
            || self.egress.unavailable(),
            crate::device::egress::CallbackStatus::error,
        )
    }
    /// 构造函数
    pub fn new(device: tun::AsyncDevice, mtu: usize) -> Self {
        let interface_name = TunDevice::name(device.get_ref()).ok();
        Self {
            backend: VirtualTunBackend::Direct(device),
            cut: cut::CutBoundary::default(),
            stopping: false,
            shutdown_failure: None,
            shutdown_failure_transferred: false,
            shutdown_callbacks: None,
            interface_name,
            mtu,
            rx: crate::device::rx::RxStock::default(),
            egress: egress::Egress::default(),
        }
    }

    pub fn new_pumped(device: tun::AsyncDevice, mtu: usize, capacity_packets: usize) -> Self {
        let interface_name = TunDevice::name(device.get_ref()).ok();
        let (reader, writer): (ReadHalf<_>, WriteHalf<_>) = tokio::io::split(device);
        let capacity_packets = capacity_packets.max(1);
        let ingress =
            pump_native::Startup::Running(spawn_tun_ingress_pump(reader, mtu, capacity_packets));
        Self {
            backend: VirtualTunBackend::Pumped { writer, ingress },
            cut: cut::CutBoundary::default(),
            stopping: false,
            shutdown_failure: None,
            shutdown_failure_transferred: false,
            shutdown_callbacks: None,
            interface_name,
            mtu,
            rx: crate::device::rx::RxStock::default(),
            egress: egress::Egress::default(),
        }
    }

    // 主循环(tokio::select!) -> 异步方法(wait_for_rx()): 它是我们在“异步网卡”和“同步仓库”之间进货的桥梁
    // 当这个方法(wait_for_rx())被 .await 唤醒时，说明底层的 tun0 网卡来数据包了，我们需要把它读出来，存进 rx_buffer 这个“收货仓库”里，准备等下喂给 smoltcp。
    /// 异步进货：等待物理网卡吐出数据，存入 rx_buffer
    pub async fn wait_for_rx(&mut self) -> std::io::Result<()> {
        if self.stopping || self.cut.active() || self.native_pending() {
            return Err(self.unavailable());
        }
        self.rx.admit()?;
        if self.rx.has_current() {
            return Ok(());
        }
        match &mut self.backend {
            VirtualTunBackend::Stopped | VirtualTunBackend::PreparedDirect { .. } => {
                Err(self.unavailable())
            }
            VirtualTunBackend::Direct(device) => {
                let n = self.rx.read(device, rx_buffer_capacity_for_mtu(self.mtu)).await?;
                self.rx.finish_read(n, tun_packet_header_len())
            }
            VirtualTunBackend::Pumped { ingress, .. } => {
                let mut packet = Some(
                    ingress
                        .as_mut()
                        .ok_or_else(device_closed)?
                        .recv()
                        .await
                        .ok_or_else(device_closed)??,
                );
                self.rx.install(&mut packet, tun_packet_header_len())
            }
        }
    }

    pub fn try_recv_rx(&mut self) -> std::io::Result<bool> {
        if self.stopping || self.cut.active() || self.native_pending() {
            return Err(self.unavailable());
        }
        self.rx.admit()?;
        if self.rx.has_current() {
            return Ok(true);
        }
        match &mut self.backend {
            VirtualTunBackend::Stopped | VirtualTunBackend::PreparedDirect { .. } => {
                Err(self.unavailable())
            }
            VirtualTunBackend::Direct(device) => {
                let Some(buffer) = self.rx.try_prepare(rx_buffer_capacity_for_mtu(self.mtu))? else { return Ok(false) };
                let n = loop {
                    match device.get_mut().read(buffer) {
                        Ok(0) => return Ok(false),
                        Ok(n) => break n,
                        Err(e) if e.kind() == ErrorKind::WouldBlock => return Ok(false),
                        Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                        Err(e) => return Err(e),
                    }
                };
                self.rx.finish_read(n, tun_packet_header_len())?;
                Ok(true)
            }
            VirtualTunBackend::Pumped { ingress, .. } => {
                let mut packet = match ingress.as_mut().ok_or_else(device_closed)?.try_recv() {
                    Ok(packet) => Some(packet?),
                    Err(mpsc::error::TryRecvError::Empty) => return Ok(false),
                    Err(mpsc::error::TryRecvError::Disconnected) => return Err(self.unavailable()),
                };
                self.rx.install(&mut packet, tun_packet_header_len())?;
                Ok(true)
            }
        }
    }

    pub async fn flush_tx(&mut self) -> std::io::Result<()> {
        if self.stopping || self.cut.active() || self.native_pending() {
            return Err(self.unavailable());
        }
        match &mut self.backend {
            VirtualTunBackend::Stopped | VirtualTunBackend::PreparedDirect { .. } => {
                Err(self.unavailable())
            }
            VirtualTunBackend::Direct(device) => self.egress.flush(device).await,
            VirtualTunBackend::Pumped { writer, .. } => self.egress.flush(writer).await,
        }
    }

    /// 下行注入：把构造好的裸 IPv4/UDP 包塞进发货队列，等 `flush_tx` 发出。
    /// 中文要点：UDP relay 绕过 smoltcp，回程包由主循环用 etherparse 造好后经此注入。
    pub fn inject_ip_packet(&mut self, pkt: &[u8]) {
        if self.stopping || self.cut.active() || self.native_pending() {
            return;
        }
        if self.egress.raw_injection_allowed() {
            push_injected(&mut self.egress.queue, pkt);
        }
    }

    /// 只读窥视当前收货仓库（不取走）：classify_inbound / inspect_inbound_syn 热路径用。
    pub fn rx_peek(&self) -> Option<&[u8]> {
        if self.stopping || self.cut.active() || self.native_pending() {
            return None;
        }
        self.rx.peek()
    }

    /// 取走当前收货仓库（UDP relay 把包 take 走、不进 iface.poll）。
    pub fn rx_take(&mut self) -> Option<BytesMut> {
        if self.stopping || self.cut.active() || self.native_pending() {
            return None;
        }
        self.rx.take()
    }

    pub fn stage_rx_for_iface(&mut self) -> bool {
        if self.stopping || self.cut.active() || self.native_pending() {
            return false;
        }
        self.rx.stage()
    }

    pub fn take_flushed_tcp_packets(&mut self) -> crate::device::FlushedTcpBatch<'_> {
        self.egress.summaries()
    }

    pub fn tun_ingress_pump_snapshot(&self) -> TunIngressPumpSnapshot {
        match &self.backend {
            VirtualTunBackend::Stopped
            | VirtualTunBackend::PreparedDirect { .. }
            | VirtualTunBackend::Direct(_) => TunIngressPumpSnapshot::default(),
            VirtualTunBackend::Pumped { ingress, .. } => ingress
                .as_ref()
                .map(TunIngressPump::snapshot)
                .unwrap_or_default(),
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
/// Expected cold profile supplied by the original Service composition root.
#[derive(Clone, Copy)]
pub enum DeviceBinding<'a> {
    Legacy,
    Native {
        meter: &'a bytes::BackingMeter,
        slots: usize,
        mtu: usize,
    },
}

#[allow(async_fn_in_trait)] // 仅作泛型约束使用、从不 `dyn TunIo`，无需 auto-trait 边界
pub trait TunIo: Device {
    /// Frozen Main capture mode; no inventory is inferred from a DATA record.
    fn main_capture_meter(&self) -> Option<&bytes::BackingMeter> {
        None
    }

    /// Capture uncertainty returns the original already-reported IO unchanged.
    /// Session adapters additionally publish first/stop in their original root.
    fn main_capture_failure(&mut self, prior: Option<std::io::Error>) -> std::io::Error {
        if let Some(meter) = self.main_capture_meter() {
            meter.quarantine();
        }
        self.request_stop();
        prior.unwrap_or_else(|| std::io::Error::other("Main capture custody is uncertain"))
    }

    /// Read-only access to the original ingress failure slot. Merely obtaining
    /// or dropping it never consumes facts, packets or a JOIN obligation.
    fn ingress_failure_observer(&self) -> Option<DeviceIngressFailure> {
        None
    }
    /// Cold binding only. Implementations must reject a hard authority unless
    /// every existing physical owner was admitted before its first operation.
    /// A refusal retains the device for its original shutdown/Drop transaction.
    fn bind_backing_meter(&mut self, binding: DeviceBinding<'_>) -> std::io::Result<()> {
        if matches!(binding, DeviceBinding::Native { .. }) {
            Err(std::io::Error::new(
                ErrorKind::Unsupported,
                "device has no cold Native inventory",
            ))
        } else {
            Ok(())
        }
    }

    /// Pause the original ingress producer and retire device-owned old-session
    /// buffers. Unsupported implementations cannot authorize a successor.
    async fn cut_ingress(&mut self) -> std::io::Result<DeviceCutReceipt> {
        Err(std::io::Error::from(ErrorKind::Unsupported))
    }
    /// Validate the original receipt before resuming that same producer.
    fn resume_ingress(&mut self, _receipt: &mut DeviceCutReceipt) -> std::io::Result<()> {
        Err(std::io::Error::from(ErrorKind::Unsupported))
    }
    /// Close admission and wake owned background tasks. This does not claim settlement.
    fn request_stop(&mut self);
    /// Observe real joins and release this device's backend and owned queue backing.
    /// Call [`DeviceShutdown::settle`] on the receipt before claiming final
    /// settlement; pending callbacks still own their original cold storage.
    fn poll_shutdown(&mut self, cx: &mut Context<'_>) -> Poll<DeviceShutdown>;
    /// 异步进货：等待下一个 IP 包就绪并存入内部 rx 槽。
    async fn wait_for_rx(&mut self) -> std::io::Result<()>;
    /// 非阻塞进货：若已有 IP 包 ready，则存入内部 rx 槽；无包时返回 Ok(false)。
    fn try_recv_rx(&mut self) -> std::io::Result<bool>;
    /// 只读窥视当前 rx 槽（不取走）。
    fn rx_peek(&self) -> Option<&[u8]>;
    /// 取走当前 rx 槽。
    fn rx_take(&mut self) -> Option<BytesMut>;
    /// Transfer exclusive ingress ownership without exposing a protected backing.
    fn rx_take_packet(&mut self) -> Option<RxPacket>;
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
    fn take_flushed_tcp_packets(&mut self) -> crate::device::FlushedTcpBatch<'_> {
        Vec::new().into()
    }
    fn tun_ingress_pump_snapshot(&self) -> TunIngressPumpSnapshot {
        TunIngressPumpSnapshot::default()
    }
    /// 下行注入：裸 IP 包入发货队列，等 `flush_tx` 发出。
    fn inject_ip_packet(&mut self, pkt: &[u8]);
    /// Prepare storage before calling the encoder. Native overrides retain the
    /// physical packet in the Device until final publication or known refusal.
    fn inject_ip_packet_with(
        &mut self,
        max_len: usize,
        encode: impl FnOnce(&mut [u8]) -> std::io::Result<usize>,
    ) -> std::io::Result<()> {
        if max_len == 0 || max_len > u16::MAX as usize {
            return Err(ErrorKind::InvalidInput.into());
        }
        let mut packet = Vec::new();
        packet
            .try_reserve_exact(max_len)
            .map_err(|_| std::io::Error::from(ErrorKind::OutOfMemory))?;
        packet.resize(max_len, 0);
        let len = encode(&mut packet)?;
        if len == 0 || len > max_len {
            return Err(ErrorKind::InvalidData.into());
        }
        self.inject_ip_packet(&packet[..len]);
        Ok(())
    }
    /// OS interface name when available. Diagnostics only; behavior must not depend on it.
    fn interface_name(&self) -> Option<&str> {
        None
    }
}

impl TunIo for VirtualTunDevice {
    fn main_capture_meter(&self) -> Option<&bytes::BackingMeter> {
        self.egress.native_meter()
    }

    fn ingress_failure_observer(&self) -> Option<DeviceIngressFailure> {
        match &self.backend {
            VirtualTunBackend::Pumped { ingress, .. } => {
                Some(ingress.as_ref()?.ingress_failure_observer())
            }
            _ => None,
        }
    }
    fn bind_backing_meter(&mut self, binding: DeviceBinding<'_>) -> std::io::Result<()> {
        self.bind_native(binding)
    }
    async fn cut_ingress(&mut self) -> std::io::Result<DeviceCutReceipt> {
        if self.stopping
            || self.native_pending()
            || matches!(self.backend, VirtualTunBackend::Stopped)
        {
            return Err(self.unavailable());
        }
        if let VirtualTunBackend::Pumped { ingress, .. } = &self.backend {
            ingress.as_ref().ok_or_else(device_closed)?.check_cut()?;
        }
        self.cut.begin()?;
        let mut discarded = match &mut self.backend {
            VirtualTunBackend::Stopped | VirtualTunBackend::PreparedDirect { .. } => {
                return Err(self.unavailable());
            }
            VirtualTunBackend::Direct(_) => (0, 0),
            VirtualTunBackend::Pumped { ingress, .. } => {
                ingress.as_mut().ok_or_else(device_closed)?.cut().await?
            }
        };
        self.rx.discard(&mut discarded);
        self.egress.discard(&mut discarded);
        Ok(self.cut.issue(discarded))
    }
    fn resume_ingress(&mut self, receipt: &mut DeviceCutReceipt) -> std::io::Result<()> {
        self.cut.validate(receipt)?;
        if self.stopping {
            return Err(self.unavailable());
        }
        if let VirtualTunBackend::Pumped { ingress, .. } = &self.backend {
            ingress
                .as_ref()
                .ok_or_else(device_closed)?
                .validate_resume()?;
        }
        let mut discarded = (receipt.discarded_packets, receipt.discarded_backing);
        self.rx.discard(&mut discarded);
        self.egress.discard(&mut discarded);
        receipt.discarded_packets = discarded.0;
        receipt.discarded_backing = discarded.1;
        if let VirtualTunBackend::Pumped { ingress, .. } = &self.backend {
            ingress.as_ref().ok_or_else(device_closed)?.resume();
        }
        self.cut.resumed();
        Ok(())
    }
    fn request_stop(&mut self) {
        self.stopping = true;
        if let VirtualTunBackend::Pumped { ingress, .. } = &self.backend {
            if let Some(pump) = ingress.as_ref() {
                pump.request_stop();
            }
        }
    }

    fn poll_shutdown(&mut self, cx: &mut Context<'_>) -> Poll<DeviceShutdown> {
        self.request_stop();
        let mut result = if let VirtualTunBackend::Pumped { ingress, .. } = &mut self.backend {
            std::task::ready!(ingress.poll_shutdown(cx, self.shutdown_failure))
        } else {
            DeviceShutdown::from_status(self.shutdown_failure)
        };
        result.failure_transferred |= self.shutdown_failure_transferred;
        self.shutdown_failure_transferred |= result.transfers_failure();
        self.shutdown_failure = result.io_kind();
        self.backend = VirtualTunBackend::Stopped;
        self.rx.discard(&mut (0, 0));
        let rx_stock = self.rx.take_stock();
        drop(std::mem::take(&mut self.rx));
        self.egress.discard(&mut (0, 0));
        if self.shutdown_callbacks.is_none() {
            self.shutdown_callbacks = self.egress.callback_custody().map(|c| c.status());
        }
        let result =
            result.with_callbacks(self.egress.shutdown_tail(rx_stock), self.shutdown_callbacks.clone());
        drop(std::mem::take(&mut self.egress));
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
    fn rx_take_packet(&mut self) -> Option<RxPacket> {
        if self.stopping || self.cut.active() || self.native_pending() { return None; }
        self.rx.take_packet()
    }
    fn can_stage_single_iface_rx(&self) -> bool {
        !self.stopping && !self.cut.active() && !self.native_pending() && self.rx.staged_empty()
    }
    fn stage_rx_for_iface(&mut self) -> bool {
        VirtualTunDevice::stage_rx_for_iface(self)
    }
    async fn flush_tx(&mut self) -> std::io::Result<()> {
        VirtualTunDevice::flush_tx(self).await
    }
    fn queued_tx_bytes(&self) -> usize {
        self.egress.owned_bytes()
    }
    fn take_flushed_tcp_packets(&mut self) -> crate::device::FlushedTcpBatch<'_> {
        VirtualTunDevice::take_flushed_tcp_packets(self)
    }
    fn tun_ingress_pump_snapshot(&self) -> TunIngressPumpSnapshot {
        VirtualTunDevice::tun_ingress_pump_snapshot(self)
    }
    fn inject_ip_packet(&mut self, pkt: &[u8]) {
        VirtualTunDevice::inject_ip_packet(self, pkt)
    }
    fn inject_ip_packet_with(
        &mut self,
        max_len: usize,
        encode: impl FnOnce(&mut [u8]) -> std::io::Result<usize>,
    ) -> std::io::Result<()> {
        if self.stopping || self.cut.active() || self.native_pending() {
            return Err(self.unavailable());
        }
        self.egress.inject(max_len, tun_packet_header_len(), encode)
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
        payload_start_seq: tcp.sequence_number.wrapping_add(u32::from(tcp.syn)),
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
    buffer: RxPacket,
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

pub use egress::{FlushedTcpBatch, PacketToken as TunTxToken};

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
        if self.stopping || self.cut.active() || self.native_pending() {
            return None;
        }
        // ⚠️ 这里的逻辑是将 rx_buffer 里的数据拿出来，打包成 RxToken
        // 如果仓库里有货，就 take() 拿走，打包成收货单和发货单交给 smoltcp
        // 如果仓库里没有货，就返回 None
        // println!("接收包：{:?}", self.rx.current);
        if self.rx.staged_empty() {
            return None;
        }
        let token = self.egress.token(self.mtu, tun_packet_header_len())?;
        self.rx
            .take_staged()
            .map(|buffer| (TunRxToken { buffer }, token))
    }

    fn transmit(&mut self, _timestamp: smoltcp::time::Instant) -> Option<Self::TxToken<'_>> {
        if self.stopping || self.cut.active() || self.native_pending() {
            return None;
        }
        // 只需要返回一个持有 tx_queue 可变引用的 TxToken 即可
        // 直接给一张发货单，里面带着发货仓库的钥匙
        // （不在此 println! tx_queue——每次发包热路径，同步 stdout 会拖垮主循环）
        self.egress.token(self.mtu, tun_packet_header_len())
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
