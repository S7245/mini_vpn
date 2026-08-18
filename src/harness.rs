//! knife1：大并发压测 harness（feature = "harness"）。
//!
//! 中文要点：把 mini_vpn **客户端主循环 + smoltcp + relay 调度** 的并发瓶颈从网络中**隔离**出来。
//! 做法（见 docs/tech/2026-06-12-knife1-concurrency-harness-spec）：
//! - 被测主循环（SUT）= [`crate::client_tun::run_event_loop`]，跑在 [`LoopbackTunDevice`](内存回环
//!   device，impl [`TunIo`]) 上，上游是 [`MockUpstream`]（echo/计数，不走网络）。
//! - 流量发生器 = **第二个 smoltcp 栈**，当 N 个 app，经内存包管道（[`PacketLink`]）连到 SUT。
//!   握手/数据全走真 smoltcp，忠实触发 SUT 的 SYN inspector → 建端口池 → accept → relay 全链路。
//! - [`RecordingSink`] 采集主循环三段（poll / relay 调度）耗时 + listener 全量遍历规模（量化 #1 O(n)）。
//!
//! 对外只暴露高层 [`run_tcp_scenario`] / [`run_udp_echo_scenario`] → [`Report`]，所有 smoltcp 复杂度
//! 封在本模块内，使 `tests/` 整合测试极薄。隔离不了的瓶颈 #3（单条 QUIC 连接）见 spec，deferred。

use crate::client_tun::{D16HarnessFlow, MetricsSink, TunRuntimeConfig, run_event_loop};
use crate::device::{
    TunFlushedTcpPacket, TunIngressPumpSnapshot, TunIo, summarize_flushed_ip_tcp_packet,
};
use crate::metrics::Metrics;
use crate::shared::{ClientError, TargetAddr};
use crate::tcp_downlink_pump::{ByteQueuePushError, D16GlobalByteBudget};
use crate::upstream::{
    DatagramUpstream, NativeTcpChunk, NativeTcpReader, NativeTcpRelayStream, OpenedTcpRelay,
    ProxyUpstream, RelayStream,
};

use bytes::{Bytes, BytesMut};
use smoltcp::iface::{Config as SmolConfig, Interface, SocketHandle, SocketSet};
use smoltcp::phy::{Device, DeviceCapabilities, Medium, RxToken, TxToken};
use smoltcp::socket::tcp;
use smoltcp::time::Instant as SmolInstant;
use smoltcp::wire::{IpAddress, IpCidr, Ipv4Address};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{Notify, mpsc};

// ============================ 内存包管道 ============================

/// 单向 IP 包管道（一端 push，另一端 pop + await）。内部 `Arc` 共享，可廉价 clone。
#[derive(Clone, Default)]
pub struct PacketLink {
    queue: Arc<Mutex<VecDeque<BytesMut>>>,
    notify: Arc<Notify>,
    capacity_packets: Option<usize>,
    dropped_packets: Arc<AtomicU64>,
    high_water_packets: Arc<AtomicU64>,
}

impl PacketLink {
    fn new() -> Self {
        Self::default()
    }
    fn bounded(capacity_packets: usize) -> Self {
        Self {
            capacity_packets: Some(capacity_packets.max(1)),
            ..Self::default()
        }
    }
    fn push(&self, pkt: BytesMut) -> bool {
        let mut queue = self.queue.lock().unwrap();
        if self
            .capacity_packets
            .is_some_and(|capacity| queue.len() >= capacity)
        {
            self.dropped_packets.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        queue.push_back(pkt);
        self.high_water_packets
            .fetch_max(queue.len() as u64, Ordering::Relaxed);
        drop(queue);
        self.notify.notify_one();
        true
    }
    fn pop(&self) -> Option<BytesMut> {
        self.queue.lock().unwrap().pop_front()
    }
    fn dropped_packets(&self) -> u64 {
        self.dropped_packets.load(Ordering::Relaxed)
    }
    fn high_water_packets(&self) -> u64 {
        self.high_water_packets.load(Ordering::Relaxed)
    }
}

// ============================ 回环 Rx/Tx token（裸 IP，无 PI 头）============================

/// 裸收货单：把回环管道里的一个 IP 包递给 smoltcp。
pub struct RawRxToken {
    buffer: BytesMut,
}
impl RxToken for RawRxToken {
    fn consume<R, F: FnOnce(&mut [u8]) -> R>(mut self, f: F) -> R {
        f(&mut self.buffer)
    }
}

/// 裸发货单（SUT 侧）：smoltcp 造好的包入本地 tx_queue，等 `flush_tx` 推进回环。
pub struct QueueTxToken<'a> {
    queue: &'a mut VecDeque<BytesMut>,
}
impl TxToken for QueueTxToken<'_> {
    fn consume<R, F: FnOnce(&mut [u8]) -> R>(self, len: usize, f: F) -> R {
        let mut buf = BytesMut::zeroed(len);
        let r = f(&mut buf);
        self.queue.push_back(buf);
        r
    }
}

/// 裸发货单（发生器侧）：smoltcp 造好的包立即 push 进对端管道。
pub struct LinkTxToken {
    outbound: PacketLink,
}
impl TxToken for LinkTxToken {
    fn consume<R, F: FnOnce(&mut [u8]) -> R>(self, len: usize, f: F) -> R {
        let mut buf = BytesMut::zeroed(len);
        let r = f(&mut buf);
        self.outbound.push(buf);
        r
    }
}

fn loopback_caps_with_mtu(mtu: usize) -> DeviceCapabilities {
    let mut caps = DeviceCapabilities::default();
    caps.max_transmission_unit = mtu;
    caps.medium = Medium::Ip;
    // 与 VirtualTunDevice 一致：发送时算校验和、接收时不校验（回环两端都算 → 包合法）。
    let mut cs = smoltcp::phy::ChecksumCapabilities::default();
    cs.tcp = smoltcp::phy::Checksum::Tx;
    cs.ipv4 = smoltcp::phy::Checksum::Tx;
    caps.checksum = cs;
    caps
}

/// 刀12：合成 CPU burn——忙等 `d` 时长，模拟 on-loop per-flush 处理成本（真分片/校验和等）。
/// `black_box` 防被优化掉；`d.is_zero()` 立即返回（默认零开销，不影响既有场景）。
fn busy_spin(d: Duration) {
    if d.is_zero() {
        return;
    }
    let start = Instant::now();
    let mut x: u64 = 0;
    while start.elapsed() < d {
        x = x.wrapping_add(1);
        std::hint::black_box(x);
    }
}

// ============================ SUT 侧回环设备（impl TunIo）============================

/// 被测主循环（SUT）用的内存回环 TUN 设备：结构镜像 `VirtualTunDevice`
/// （单槽 rx_buffer + tx_queue），只是数据来自 [`PacketLink`] 而非真 utun。
pub struct LoopbackTunDevice {
    rx_buffer: Option<BytesMut>,
    iface_rx_queue: VecDeque<BytesMut>,
    tx_queue: VecDeque<BytesMut>,
    flushed_tcp_packets: Vec<TunFlushedTcpPacket>,
    inbound: PacketLink,  // 发生器 → SUT
    outbound: PacketLink, // SUT → 发生器
    ingress_pump: Option<LoopbackIngressPump>,
    /// 刀12：每次 `flush_tx` 注入的合成 on-loop CPU 成本（busy-spin）。默认 ZERO。
    /// `flush_tx` 在主循环 poll 段内（enter_poll/leave_poll 括起）→ 该 burn 计入 poll_time / loop-active，
    /// 用于验证 profiler 能侦测「主循环被 on-loop CPU 拖满」的饱和信号（T4 spike）。
    cpu_burn_per_flush: Duration,
    suppress_wait_after_tcp_payload_flush: bool,
    wait_suppressed: bool,
    max_tcp_payload_packets_per_flush: Arc<AtomicU64>,
    mtu: usize,
}

struct LoopbackIngressPump {
    receiver: mpsc::Receiver<BytesMut>,
    task: tokio::task::JoinHandle<()>,
    stats: Arc<LoopbackIngressPumpStats>,
    capacity_packets: usize,
}

#[derive(Default)]
struct LoopbackIngressPumpStats {
    packets: AtomicU64,
    bytes: AtomicU64,
    queue_high_water: AtomicU64,
    full_waits: AtomicU64,
    closed: AtomicU64,
}

impl LoopbackIngressPumpStats {
    fn snapshot(&self, capacity_packets: usize) -> TunIngressPumpSnapshot {
        TunIngressPumpSnapshot {
            enabled: true,
            packets: self.packets.load(Ordering::Relaxed),
            bytes: self.bytes.load(Ordering::Relaxed),
            queue_high_water: self.queue_high_water.load(Ordering::Relaxed),
            capacity_packets,
            full_waits: self.full_waits.load(Ordering::Relaxed),
            read_errors: 0,
            closed: self.closed.load(Ordering::Relaxed),
        }
    }
}

impl LoopbackTunDevice {
    fn new(inbound: PacketLink, outbound: PacketLink) -> Self {
        Self::with_options(inbound, outbound, Duration::ZERO, false)
    }

    /// 带合成 on-loop CPU 成本的回环设备（T4：multi_thread 饱和 spike 用）。
    fn with_burn(inbound: PacketLink, outbound: PacketLink, cpu_burn_per_flush: Duration) -> Self {
        Self::with_options(inbound, outbound, cpu_burn_per_flush, false)
    }

    fn with_suppressed_wait_after_tcp_payload_flush(
        inbound: PacketLink,
        outbound: PacketLink,
    ) -> Self {
        Self::with_options(inbound, outbound, Duration::ZERO, true)
    }

    fn with_options(
        inbound: PacketLink,
        outbound: PacketLink,
        cpu_burn_per_flush: Duration,
        suppress_wait_after_tcp_payload_flush: bool,
    ) -> Self {
        Self {
            rx_buffer: None,
            iface_rx_queue: VecDeque::new(),
            tx_queue: VecDeque::new(),
            flushed_tcp_packets: Vec::new(),
            inbound,
            outbound,
            ingress_pump: None,
            cpu_burn_per_flush,
            suppress_wait_after_tcp_payload_flush,
            wait_suppressed: false,
            max_tcp_payload_packets_per_flush: Arc::new(AtomicU64::new(0)),
            mtu: 1500,
        }
    }

    fn with_tcp_payload_flush_counter(mut self, counter: Arc<AtomicU64>) -> Self {
        self.max_tcp_payload_packets_per_flush = counter;
        self
    }

    #[cfg(test)]
    fn with_mtu(mut self, mtu: usize) -> Self {
        self.mtu = mtu;
        self
    }

    #[cfg(test)]
    fn with_ingress_pump(mut self, capacity_packets: usize) -> Self {
        let capacity_packets = capacity_packets.max(1);
        let inbound = self.inbound.clone();
        let (sender, receiver) = mpsc::channel(capacity_packets);
        let stats = Arc::new(LoopbackIngressPumpStats::default());
        let task_stats = Arc::clone(&stats);
        let task = tokio::spawn(async move {
            loop {
                let packet = loop {
                    if let Some(packet) = inbound.pop() {
                        break packet;
                    }
                    inbound.notify.notified().await;
                };
                let occupied = capacity_packets.saturating_sub(sender.capacity());
                task_stats.queue_high_water.fetch_max(
                    occupied.saturating_add(1).min(capacity_packets) as u64,
                    Ordering::Relaxed,
                );
                if sender.capacity() == 0 {
                    task_stats.full_waits.fetch_add(1, Ordering::Relaxed);
                }
                let bytes = packet.len();
                if sender.send(packet).await.is_err() {
                    break;
                }
                task_stats.packets.fetch_add(1, Ordering::Relaxed);
                task_stats.bytes.fetch_add(bytes as u64, Ordering::Relaxed);
            }
            task_stats.closed.fetch_add(1, Ordering::Relaxed);
        });
        self.ingress_pump = Some(LoopbackIngressPump {
            receiver,
            task,
            stats,
            capacity_packets,
        });
        self
    }
}

impl Drop for LoopbackTunDevice {
    fn drop(&mut self) {
        if let Some(pump) = &self.ingress_pump {
            pump.task.abort();
        }
    }
}

impl TunIo for LoopbackTunDevice {
    async fn wait_for_rx(&mut self) -> std::io::Result<()> {
        if self.rx_buffer.is_some() {
            return Ok(());
        }
        if self.wait_suppressed {
            return std::future::pending::<std::io::Result<()>>().await;
        }
        if let Some(pump) = &mut self.ingress_pump {
            self.rx_buffer = Some(pump.receiver.recv().await.ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "loopback ingress pump closed",
                )
            })?);
            return Ok(());
        }
        loop {
            if let Some(pkt) = self.inbound.pop() {
                self.rx_buffer = Some(pkt);
                return Ok(());
            }
            // notify_one 在无 waiter 时保留 1 个 permit，不丢唤醒（pop→None 与 notified 之间的竞态安全）。
            self.inbound.notify.notified().await;
        }
    }
    fn try_recv_rx(&mut self) -> std::io::Result<bool> {
        if self.rx_buffer.is_some() {
            return Ok(true);
        }
        if let Some(pump) = &mut self.ingress_pump {
            return match pump.receiver.try_recv() {
                Ok(packet) => {
                    self.rx_buffer = Some(packet);
                    Ok(true)
                }
                Err(mpsc::error::TryRecvError::Empty) => Ok(false),
                Err(mpsc::error::TryRecvError::Disconnected) => Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "loopback ingress pump closed",
                )),
            };
        }
        if let Some(pkt) = self.inbound.pop() {
            self.rx_buffer = Some(pkt);
            return Ok(true);
        }
        Ok(false)
    }
    fn rx_peek(&self) -> Option<&[u8]> {
        self.rx_buffer.as_deref()
    }
    fn rx_take(&mut self) -> Option<BytesMut> {
        self.rx_buffer.take()
    }
    fn stage_rx_for_iface(&mut self) -> bool {
        let Some(packet) = self.rx_buffer.take() else {
            return false;
        };
        self.iface_rx_queue.push_back(packet);
        true
    }
    async fn flush_tx(&mut self) -> std::io::Result<()> {
        busy_spin(self.cpu_burn_per_flush); // 刀12：合成 on-loop CPU（默认 ZERO 即 no-op）。
        let mut tcp_payload_packets = 0u64;
        while let Some(pkt) = self.tx_queue.pop_front() {
            if let Some(packet) = summarize_flushed_ip_tcp_packet(&pkt) {
                tcp_payload_packets = tcp_payload_packets.saturating_add(1);
                if self.suppress_wait_after_tcp_payload_flush && packet.payload_bytes > 0 {
                    self.wait_suppressed = true;
                }
                self.flushed_tcp_packets.push(packet);
            }
            self.outbound.push(pkt);
        }
        self.max_tcp_payload_packets_per_flush
            .fetch_max(tcp_payload_packets, Ordering::Relaxed);
        Ok(())
    }
    fn queued_tx_bytes(&self) -> usize {
        self.tx_queue.iter().map(BytesMut::len).sum()
    }
    fn take_flushed_tcp_packets(&mut self) -> Vec<TunFlushedTcpPacket> {
        std::mem::take(&mut self.flushed_tcp_packets)
    }
    fn tun_ingress_pump_snapshot(&self) -> TunIngressPumpSnapshot {
        self.ingress_pump
            .as_ref()
            .map(|pump| pump.stats.snapshot(pump.capacity_packets))
            .unwrap_or_default()
    }
    fn inject_ip_packet(&mut self, pkt: &[u8]) {
        self.tx_queue.push_back(BytesMut::from(pkt));
    }
}

impl Device for LoopbackTunDevice {
    type RxToken<'a> = RawRxToken;
    type TxToken<'a> = QueueTxToken<'a>;
    fn capabilities(&self) -> DeviceCapabilities {
        loopback_caps_with_mtu(self.mtu)
    }
    fn receive(&mut self, _t: SmolInstant) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        self.iface_rx_queue.pop_front().map(|buffer| {
            (
                RawRxToken { buffer },
                QueueTxToken {
                    queue: &mut self.tx_queue,
                },
            )
        })
    }
    fn transmit(&mut self, _t: SmolInstant) -> Option<Self::TxToken<'_>> {
        Some(QueueTxToken {
            queue: &mut self.tx_queue,
        })
    }
}

// ============================ 发生器侧设备（同步驱动）============================

/// 流量发生器（第二 smoltcp 栈）用的设备：`receive` 直接从入站管道弹包、`transmit` 立即 push 出站。
struct GeneratorDevice {
    inbound: PacketLink,  // SUT → 发生器
    outbound: PacketLink, // 发生器 → SUT
    ingress_packets_per_poll: Option<usize>,
    ingress_packets_remaining: Option<usize>,
    mtu: usize,
}

impl GeneratorDevice {
    fn new(inbound: PacketLink, outbound: PacketLink) -> Self {
        Self {
            inbound,
            outbound,
            ingress_packets_per_poll: None,
            ingress_packets_remaining: None,
            mtu: 1500,
        }
    }

    fn with_ingress_packets_per_poll(
        inbound: PacketLink,
        outbound: PacketLink,
        packets: usize,
    ) -> Self {
        let packets = packets.max(1);
        Self {
            inbound,
            outbound,
            ingress_packets_per_poll: Some(packets),
            ingress_packets_remaining: Some(packets),
            mtu: 1500,
        }
    }

    #[cfg(test)]
    fn with_mtu(mut self, mtu: usize) -> Self {
        self.mtu = mtu;
        self
    }

    fn begin_poll(&mut self) {
        self.ingress_packets_remaining = self.ingress_packets_per_poll;
    }
}

impl Device for GeneratorDevice {
    type RxToken<'a> = RawRxToken;
    type TxToken<'a> = LinkTxToken;
    fn capabilities(&self) -> DeviceCapabilities {
        loopback_caps_with_mtu(self.mtu)
    }
    fn receive(&mut self, _t: SmolInstant) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        if self.ingress_packets_remaining == Some(0) {
            return None;
        }
        self.inbound.pop().map(|buffer| {
            if let Some(remaining) = self.ingress_packets_remaining.as_mut() {
                *remaining = remaining.saturating_sub(1);
            }
            (
                RawRxToken { buffer },
                LinkTxToken {
                    outbound: self.outbound.clone(),
                },
            )
        })
    }
    fn transmit(&mut self, _t: SmolInstant) -> Option<Self::TxToken<'_>> {
        Some(LinkTxToken {
            outbound: self.outbound.clone(),
        })
    }
}

struct HarnessNativeReader<R> {
    inner: R,
    next_offset: u64,
}

impl<R> HarnessNativeReader<R> {
    fn new(inner: R) -> Self {
        Self {
            inner,
            next_offset: 0,
        }
    }
}

impl<R> NativeTcpReader for HarnessNativeReader<R>
where
    R: tokio::io::AsyncRead + Unpin + Send,
{
    fn poll_read_chunk(
        &mut self,
        cx: &mut Context<'_>,
        max_len: usize,
    ) -> Poll<std::io::Result<Option<NativeTcpChunk>>> {
        let mut storage = vec![0u8; max_len.max(1)];
        let mut read_buf = tokio::io::ReadBuf::new(&mut storage);
        match std::pin::Pin::new(&mut self.inner).poll_read(cx, &mut read_buf) {
            Poll::Ready(Ok(())) if read_buf.filled().is_empty() => Poll::Ready(Ok(None)),
            Poll::Ready(Ok(())) => {
                let bytes = Bytes::copy_from_slice(read_buf.filled());
                let offset = self.next_offset;
                self.next_offset = self.next_offset.saturating_add(bytes.len() as u64);
                Poll::Ready(Ok(Some(NativeTcpChunk { offset, bytes })))
            }
            Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
            Poll::Pending => Poll::Pending,
        }
    }
}

// ============================ Mock 上游（echo + 计数）============================

/// 压测用 mock 上游：TCP `open_tcp` 返回内存 echo 流；UDP `send_udp` 把 datagram 原样回灌下行。
/// 不走任何真网络，把客户端处理能力从网络中隔离出来。
pub struct MockUpstream {
    tcp_opens: AtomicU64,
    udp_uplinks: AtomicU64,
    echo_buf: usize,
    downlink_tx: mpsc::Sender<Vec<u8>>,
    /// 刀3：>Some(chunk) 时，模拟 sing-box native 模式把大下行包拆成多个 `FRAG_TOTAL>1` datagram，
    /// 回灌到下行 channel → 经真主循环 `FragReassembler` 重组（端到端验证重组，无需真网络）。
    /// None = 原样回灌（passthrough echo）。
    frag_chunk: Option<usize>,
    /// 刀13：指定 TCP 目标端口进入“拥塞但不关闭”的 mock stall，用来复现一条慢流堵住
    /// relay channel 时是否拖死其它流。None = 普通 echo（既有场景零影响）。
    stall: Option<StallConfig>,
    /// 刀14d：指定 TCP 目标端口在 `open_tcp` 阶段等待，用来证明远端开流本身不能 inline 卡住主循环。
    open_stall: Option<OpenStallConfig>,
    native_byte_owned: bool,
    native_reverse_payload_bytes: Option<usize>,
}

#[derive(Debug, Clone)]
struct StallConfig {
    port: u16,
    after_bytes: usize,
    control: StallControl,
}

#[derive(Debug, Clone)]
struct OpenStallConfig {
    port: u16,
    control: OpenStallControl,
}

/// 刀13 harness：可释放的上游停读控制。`released` + `notify_one` permit 解决先发后等丢信号的问题。
#[derive(Debug, Clone)]
struct StallControl {
    released: Arc<AtomicBool>,
    stalled: Arc<AtomicBool>,
    notify: Arc<Notify>,
}

impl StallControl {
    fn new() -> Self {
        Self {
            released: Arc::new(AtomicBool::new(false)),
            stalled: Arc::new(AtomicBool::new(false)),
            notify: Arc::new(Notify::new()),
        }
    }

    fn mark_stalled(&self) {
        self.stalled.store(true, Ordering::Relaxed);
    }

    fn is_stalled(&self) -> bool {
        self.stalled.load(Ordering::Relaxed)
    }

    fn release(&self) {
        self.released.store(true, Ordering::Relaxed);
        self.notify.notify_one();
    }

    async fn wait_released(&self) {
        while !self.released.load(Ordering::Relaxed) {
            self.notify.notified().await;
        }
    }
}

#[derive(Debug, Clone)]
struct OpenStallControl {
    released: Arc<AtomicBool>,
    waiting: Arc<AtomicBool>,
    notify: Arc<Notify>,
}

impl OpenStallControl {
    fn new() -> Self {
        Self {
            released: Arc::new(AtomicBool::new(false)),
            waiting: Arc::new(AtomicBool::new(false)),
            notify: Arc::new(Notify::new()),
        }
    }

    fn mark_waiting(&self) {
        self.waiting.store(true, Ordering::Relaxed);
    }

    fn is_waiting(&self) -> bool {
        self.waiting.load(Ordering::Relaxed)
    }

    fn release(&self) {
        self.released.store(true, Ordering::Relaxed);
        self.notify.notify_one();
    }

    async fn wait_released(&self) {
        while !self.released.load(Ordering::Relaxed) {
            self.notify.notified().await;
        }
    }
}

impl MockUpstream {
    fn new(echo_buf: usize, downlink_tx: mpsc::Sender<Vec<u8>>) -> Self {
        Self::with_frag(echo_buf, downlink_tx, None)
    }

    fn with_frag(
        echo_buf: usize,
        downlink_tx: mpsc::Sender<Vec<u8>>,
        frag_chunk: Option<usize>,
    ) -> Self {
        Self {
            tcp_opens: AtomicU64::new(0),
            udp_uplinks: AtomicU64::new(0),
            echo_buf,
            downlink_tx,
            frag_chunk,
            stall: None,
            open_stall: None,
            native_byte_owned: false,
            native_reverse_payload_bytes: None,
        }
    }

    fn with_stall(
        echo_buf: usize,
        downlink_tx: mpsc::Sender<Vec<u8>>,
        stall_port: u16,
        stall_after_bytes: usize,
        control: StallControl,
    ) -> Self {
        Self {
            tcp_opens: AtomicU64::new(0),
            udp_uplinks: AtomicU64::new(0),
            echo_buf,
            downlink_tx,
            frag_chunk: None,
            stall: Some(StallConfig {
                port: stall_port,
                after_bytes: stall_after_bytes,
                control,
            }),
            open_stall: None,
            native_byte_owned: false,
            native_reverse_payload_bytes: None,
        }
    }

    fn with_open_stall(
        echo_buf: usize,
        downlink_tx: mpsc::Sender<Vec<u8>>,
        open_stall_port: u16,
        control: OpenStallControl,
    ) -> Self {
        Self {
            tcp_opens: AtomicU64::new(0),
            udp_uplinks: AtomicU64::new(0),
            echo_buf,
            downlink_tx,
            frag_chunk: None,
            stall: None,
            open_stall: Some(OpenStallConfig {
                port: open_stall_port,
                control,
            }),
            native_byte_owned: false,
            native_reverse_payload_bytes: None,
        }
    }

    fn with_d16_byte_owned(echo_buf: usize, downlink_tx: mpsc::Sender<Vec<u8>>) -> Self {
        let mut upstream = Self::new(echo_buf, downlink_tx);
        upstream.native_byte_owned = true;
        upstream
    }

    fn with_d16_reverse_payload(
        echo_buf: usize,
        downlink_tx: mpsc::Sender<Vec<u8>>,
        payload_bytes: usize,
    ) -> Self {
        let mut upstream = Self::with_d16_byte_owned(echo_buf, downlink_tx);
        upstream.native_reverse_payload_bytes = Some(payload_bytes);
        upstream
    }

    fn tcp_opens(&self) -> u64 {
        self.tcp_opens.load(Ordering::Relaxed)
    }
    fn udp_uplinks(&self) -> u64 {
        self.udp_uplinks.load(Ordering::Relaxed)
    }
}

fn target_port(target: &TargetAddr) -> u16 {
    match target {
        TargetAddr::IpPort(addr) => addr.port(),
        TargetAddr::DomainPort { port, .. } => *port,
    }
}

#[async_trait::async_trait]
impl ProxyUpstream for MockUpstream {
    async fn open_tcp(&self, target: &TargetAddr) -> Result<RelayStream, ClientError> {
        self.tcp_opens.fetch_add(1, Ordering::Relaxed);
        if let Some(open_stall) = self
            .open_stall
            .as_ref()
            .filter(|s| s.port == target_port(target))
            .cloned()
        {
            open_stall.control.mark_waiting();
            open_stall.control.wait_released().await;
        }
        let stall = self
            .stall
            .as_ref()
            .filter(|s| s.port == target_port(target))
            .cloned();
        let echo_buf = if stall.is_some() {
            self.echo_buf
        } else {
            // HoL 场景里 stall flow 需要小 buffer 快速制造背压；普通 flow 仍保留足够 buffer，
            // 避免 mock echo 自身在 write_all ↔ echo-write 之间形成全双工死锁。
            self.echo_buf.max(64 * 1024)
        };
        let (near, far) = tokio::io::duplex(echo_buf);
        // echo：把 relay 写来的上行字节原样写回（→ 成为下行）。
        if let Some(stall) = stall {
            let (mut rd, mut wr) = tokio::io::split(far);
            let (echo_tx, mut echo_rx) = mpsc::unbounded_channel::<Vec<u8>>();
            tokio::spawn(async move {
                while let Some(data) = echo_rx.recv().await {
                    if wr.write_all(&data).await.is_err() {
                        break;
                    }
                }
            });
            tokio::spawn(async move {
                let mut buf = vec![0u8; 16 * 1024];
                let mut read_total = 0usize;
                let mut stalled_once = false;
                loop {
                    match rd.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            read_total += n;
                            if echo_tx.send(buf[..n].to_vec()).is_err() {
                                break;
                            }
                            if !stalled_once && read_total >= stall.after_bytes {
                                stalled_once = true;
                                stall.control.mark_stalled();
                                stall.control.wait_released().await;
                            }
                        }
                    }
                }
            });
        } else {
            tokio::spawn(async move {
                let mut far = far;
                let mut buf = vec![0u8; 16 * 1024];
                loop {
                    match far.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            if far.write_all(&buf[..n]).await.is_err() {
                                break;
                            }
                        }
                    }
                }
            });
        }
        Ok(Box::new(near))
    }

    async fn open_tcp_relay(&self, target: &TargetAddr) -> Result<OpenedTcpRelay, ClientError> {
        if let Some(payload_bytes) = self.native_reverse_payload_bytes {
            self.tcp_opens.fetch_add(1, Ordering::Relaxed);
            let (near, far) = tokio::io::duplex(self.echo_buf.max(512 * 1024));
            let (mut far_reader, mut far_writer) = tokio::io::split(far);
            tokio::spawn(async move {
                let mut scratch = vec![0u8; 16 * 1024];
                while let Ok(read) = far_reader.read(&mut scratch).await {
                    if read == 0 {
                        break;
                    }
                }
            });
            tokio::spawn(async move {
                let chunk = vec![0x5Au8; 64 * 1024];
                let mut written = 0usize;
                while written < payload_bytes {
                    let len = (payload_bytes - written).min(chunk.len());
                    if far_writer.write_all(&chunk[..len]).await.is_err() {
                        break;
                    }
                    written += len;
                }
                let _ = far_writer.shutdown().await;
            });
            let (reader, writer) = tokio::io::split(near);
            return Ok(OpenedTcpRelay::NativeByteOwned(NativeTcpRelayStream {
                reader: Box::new(HarnessNativeReader::new(reader)),
                writer: Box::new(writer),
            }));
        }
        let stream = self.open_tcp(target).await?;
        if !self.native_byte_owned {
            return Ok(OpenedTcpRelay::Generic(stream));
        }
        let (reader, writer) = tokio::io::split(stream);
        Ok(OpenedTcpRelay::NativeByteOwned(NativeTcpRelayStream {
            reader: Box::new(HarnessNativeReader::new(reader)),
            writer: Box::new(writer),
        }))
    }

    fn open_is_cheap(&self) -> bool {
        self.open_stall.is_none()
    }
}

#[async_trait::async_trait]
impl DatagramUpstream for MockUpstream {
    async fn send_udp(&self, datagram: Vec<u8>) {
        self.udp_uplinks.fetch_add(1, Ordering::Relaxed);
        // 上行是 encode_packet(assoc_id,target,payload)；下行分支 decode_packet_meta 取回路由 + 重组。
        match self.frag_chunk {
            // passthrough echo：原样回灌（FRAG_TOTAL=1，主循环直通）。
            None => {
                let _ = self.downlink_tx.send(datagram).await;
            }
            // 分片回灌：模拟 server native 模式把大下行包拆成多帧 → 主循环 FragReassembler 重组。
            Some(chunk) => {
                let Some((assoc, payload)) = crate::tuic::decode_packet(&datagram) else {
                    return;
                };
                for frag in fragment_downlink(assoc, payload, chunk) {
                    if self.downlink_tx.send(frag).await.is_err() {
                        return;
                    }
                }
            }
        }
    }
}

/// 模拟 server native 分片：把 `payload` 按 `chunk` 拆成多个下行 Packet 命令字节。
/// 中文要点：ADDR 一律用 ATYP_NONE(0xff)（下行路由只用 assoc，ADDR 被跳过，简化模拟）；pkt_id 固定 0
/// （调用方保证每包 assoc 不同 → 重组 key `(assoc,pkt_id)` 不撞）。`payload<=chunk` 时退化为单帧。
fn fragment_downlink(assoc: u16, payload: &[u8], chunk: usize) -> Vec<Vec<u8>> {
    let chunk = chunk.max(1);
    let chunks: Vec<&[u8]> = if payload.is_empty() {
        vec![payload]
    } else {
        payload.chunks(chunk).collect()
    };
    // FRAG_ID/FRAG_TOTAL 是 u8 → 单包最多 255 帧。超出会静默截断 → 重组得到截断 payload（假丢包）。
    // 当前场景远不及（8000/1200≈7 帧）；加断言让未来大 payload 误用**响亮失败**而非静默错。
    debug_assert!(
        chunks.len() <= u8::MAX as usize,
        "fragment_downlink: >255 帧（payload {} / chunk {}）超 u8 FRAG 上限",
        payload.len(),
        chunk
    );
    let frag_total = chunks.len().min(u8::MAX as usize) as u8;
    chunks
        .into_iter()
        .enumerate()
        .take(u8::MAX as usize)
        .map(|(i, data)| {
            let mut v = Vec::with_capacity(11 + data.len());
            v.push(0x05); // VER
            v.push(0x02); // CMD_PACKET
            v.extend_from_slice(&assoc.to_be_bytes());
            v.extend_from_slice(&0u16.to_be_bytes()); // PKT_ID=0
            v.push(frag_total);
            v.push(i as u8); // FRAG_ID
            v.extend_from_slice(&(data.len() as u16).to_be_bytes()); // SIZE = 本分片 chunk 长
            v.push(0xff); // ATYP_NONE
            v.extend_from_slice(data);
            v
        })
        .collect()
}

// ============================ 分段插桩 sink ============================

#[derive(Debug, Clone, Default)]
struct Recorded {
    poll_time: Duration,
    poll_calls: u64,
    relay_time: Duration,
    relay_calls: u64,
    max_listeners: usize,
    listener_sum: u64,
    listener_obs: u64,
    /// 刀12：主循环 park（select! 空等）累计 + 迭代数，用于算 loop-active fraction。
    park_time: Duration,
    iters: u64,
    tun_rx_budget_exhausted: u64,
    backlog_pause_edges: u64,
    backlog_resume_edges: u64,
    tun_rx_tcp_packets: u64,
    tun_rx_tcp_batches: u64,
    tun_rx_batch_dirty_relay_passes: u64,
    tun_rx_batch_iface_polls: u64,
    tun_rx_batch_flushes: u64,
    tun_rx_tcp_batch_packets_high_water: u64,
    tun_rx_avoided_dirty_relay_passes: u64,
    tun_rx_avoided_iface_polls: u64,
    tun_rx_pump_packets: u64,
    tun_rx_pump_queue_high_water: u64,
    tun_rx_pump_capacity_packets: usize,
    tun_rx_pump_full_waits: u64,
    tun_rx_pump_read_errors: u64,
    actor_bypass_admitted_bytes: u64,
    uplink_recv_queue_max: usize,
    backlog_active: bool,
    d16_running_flows: usize,
    d16_drain_only_flows: usize,
    d16_recovery_flows: usize,
    d16_tun_rx_actor_wakes: u64,
    d16_eof_observed: bool,
    d16_eof_tail_bytes: usize,
    terminal_drop_bytes: u64,
    terminal_late_payload_bytes: u64,
    d16_owned_queue_bytes: usize,
    d16_pending_bytes: usize,
    d16_inflight_bytes: usize,
    d16_local_eof_sent_flows: usize,
}

/// 记录型插桩：把主循环三段耗时 + listener 全量遍历规模汇总进共享 [`Recorded`]，供测试读取。
#[derive(Clone)]
pub struct RecordingSink {
    shared: Arc<Mutex<Recorded>>,
    poll_start: Option<Instant>,
    relay_start: Option<Instant>,
    park_start: Option<Instant>,
}

impl RecordingSink {
    fn new(shared: Arc<Mutex<Recorded>>) -> Self {
        Self {
            shared,
            poll_start: None,
            relay_start: None,
            park_start: None,
        }
    }
}

impl MetricsSink for RecordingSink {
    fn wants_d16_harness_observations(&self) -> bool {
        true
    }
    fn enter_poll(&mut self) {
        self.poll_start = Some(Instant::now());
    }
    fn leave_poll(&mut self) {
        if let Some(s) = self.poll_start.take() {
            let mut r = self.shared.lock().unwrap();
            r.poll_time += s.elapsed();
            r.poll_calls += 1;
        }
    }
    fn enter_relay(&mut self) {
        self.relay_start = Some(Instant::now());
    }
    fn leave_relay(&mut self) {
        if let Some(s) = self.relay_start.take() {
            let mut r = self.shared.lock().unwrap();
            r.relay_time += s.elapsed();
            r.relay_calls += 1;
        }
    }
    fn note_listeners(&mut self, n: usize) {
        let mut r = self.shared.lock().unwrap();
        r.max_listeners = r.max_listeners.max(n);
        r.listener_sum += n as u64;
        r.listener_obs += 1;
    }
    fn loop_park_begin(&mut self) {
        self.park_start = Some(Instant::now());
    }
    fn loop_park_end(&mut self) {
        // 先在锁外算 elapsed（少持锁）；首迭代无 mark → 只 +iters。
        let dt = self.park_start.take().map(|s| s.elapsed());
        let mut r = self.shared.lock().unwrap();
        if let Some(d) = dt {
            r.park_time += d;
        }
        r.iters += 1;
    }
    fn note_tun_rx_backlog_guard(
        &mut self,
        active: bool,
        budget_exhausted: u64,
        pause_edges: u64,
        resume_edges: u64,
    ) {
        let mut r = self.shared.lock().unwrap();
        r.backlog_active = active;
        r.tun_rx_budget_exhausted = r.tun_rx_budget_exhausted.max(budget_exhausted);
        r.backlog_pause_edges = r.backlog_pause_edges.max(pause_edges);
        r.backlog_resume_edges = r.backlog_resume_edges.max(resume_edges);
    }
    fn note_tun_rx_batch_service(
        &mut self,
        tcp_packets: u64,
        tcp_batches: u64,
        batch_dirty_relay_passes: u64,
        batch_iface_polls: u64,
        batch_flushes: u64,
        tcp_batch_packets_high_water: u64,
        avoided_dirty_relay_passes: u64,
        avoided_iface_polls: u64,
        pump_packets: u64,
        pump_queue_high_water: u64,
        pump_capacity_packets: usize,
        pump_full_waits: u64,
        pump_read_errors: u64,
    ) {
        let mut r = self.shared.lock().unwrap();
        r.tun_rx_tcp_packets = r.tun_rx_tcp_packets.max(tcp_packets);
        r.tun_rx_tcp_batches = r.tun_rx_tcp_batches.max(tcp_batches);
        r.tun_rx_batch_dirty_relay_passes = r
            .tun_rx_batch_dirty_relay_passes
            .max(batch_dirty_relay_passes);
        r.tun_rx_batch_iface_polls = r.tun_rx_batch_iface_polls.max(batch_iface_polls);
        r.tun_rx_batch_flushes = r.tun_rx_batch_flushes.max(batch_flushes);
        r.tun_rx_tcp_batch_packets_high_water = r
            .tun_rx_tcp_batch_packets_high_water
            .max(tcp_batch_packets_high_water);
        r.tun_rx_avoided_dirty_relay_passes = r
            .tun_rx_avoided_dirty_relay_passes
            .max(avoided_dirty_relay_passes);
        r.tun_rx_avoided_iface_polls = r.tun_rx_avoided_iface_polls.max(avoided_iface_polls);
        r.tun_rx_pump_packets = r.tun_rx_pump_packets.max(pump_packets);
        r.tun_rx_pump_queue_high_water = r.tun_rx_pump_queue_high_water.max(pump_queue_high_water);
        r.tun_rx_pump_capacity_packets = r.tun_rx_pump_capacity_packets.max(pump_capacity_packets);
        r.tun_rx_pump_full_waits = r.tun_rx_pump_full_waits.max(pump_full_waits);
        r.tun_rx_pump_read_errors = r.tun_rx_pump_read_errors.max(pump_read_errors);
    }
    fn note_d16_actor_observation(
        &mut self,
        actor_bypass_admitted_bytes: u64,
        running_flows: usize,
        drain_only_flows: usize,
        recovery_flows: usize,
        uplink_recv_queue_max: usize,
    ) {
        let mut r = self.shared.lock().unwrap();
        r.actor_bypass_admitted_bytes = r
            .actor_bypass_admitted_bytes
            .max(actor_bypass_admitted_bytes);
        r.d16_running_flows = running_flows;
        r.d16_drain_only_flows = drain_only_flows;
        r.d16_recovery_flows = recovery_flows;
        r.uplink_recv_queue_max = r.uplink_recv_queue_max.max(uplink_recv_queue_max);
    }
    fn note_d16_tun_rx_actor_wake(&mut self) {
        let mut r = self.shared.lock().unwrap();
        r.d16_tun_rx_actor_wakes = r.d16_tun_rx_actor_wakes.saturating_add(1);
    }
    fn note_d16_flow_lifecycle(
        &mut self,
        owned_queue_bytes: usize,
        pending_bytes: usize,
        inflight_bytes: usize,
        local_eof_sent_flows: usize,
        terminal_drop_bytes: u64,
        terminal_late_payload_bytes: u64,
    ) {
        let mut r = self.shared.lock().unwrap();
        r.d16_owned_queue_bytes = owned_queue_bytes;
        r.d16_pending_bytes = pending_bytes;
        r.d16_inflight_bytes = inflight_bytes;
        r.d16_local_eof_sent_flows = local_eof_sent_flows;
        if local_eof_sent_flows > 0 {
            r.d16_eof_observed = true;
            r.d16_eof_tail_bytes = r.d16_eof_tail_bytes.max(
                owned_queue_bytes
                    .saturating_add(pending_bytes)
                    .saturating_add(inflight_bytes),
            );
        }
        r.terminal_drop_bytes = r.terminal_drop_bytes.max(terminal_drop_bytes);
        r.terminal_late_payload_bytes = r
            .terminal_late_payload_bytes
            .max(terminal_late_payload_bytes);
    }
}

// ============================ 场景参数 / 报告 ============================

/// 压测场景参数。
#[derive(Debug, Clone)]
pub struct ScenarioParams {
    /// 并发 TCP 连接数 N。
    pub connections: usize,
    /// 目标端口数（跨 ≥64 可压 MAX_INTERCEPTED_PORTS=64 上限，怀疑瓶颈 #2）。
    pub distinct_ports: usize,
    /// 每连接 echo 往返的负载字节数。
    pub payload_len: usize,
    /// SUT 每端口监听池槽位数（pool_size）。
    pub pool_size: usize,
    /// 整场超时。
    pub timeout: Duration,
    /// 刀12：每次 SUT `flush_tx`（poll 段内）注入的合成 on-loop CPU 成本。默认 ZERO（既有场景零影响）；
    /// T4 multi_thread spike 设非零造主循环单核饱和，验证 profiler 的 loop-active/poll 信号会随之上升。
    pub cpu_burn_per_flush: Duration,
}

impl Default for ScenarioParams {
    fn default() -> Self {
        Self {
            connections: 64,
            distinct_ports: 64,
            payload_len: 1024,
            pool_size: 8,
            timeout: Duration::from_secs(30),
            cpu_burn_per_flush: Duration::ZERO,
        }
    }
}

/// 压测结果（数据 + 定位信号）。
#[derive(Debug, Clone)]
pub struct Report {
    pub connections: usize,
    pub completed: usize,
    pub wall: Duration,
    pub bytes_echoed: usize,
    pub tcp_opens: u64,
    pub poll_time: Duration,
    pub poll_calls: u64,
    pub relay_time: Duration,
    pub relay_calls: u64,
    pub max_listeners: usize,
    pub avg_listeners: f64,
    pub per_socket_buffer_bytes: usize,
    /// 刀12：主循环 park 累计 + 迭代数（算 loop-active fraction，T4 饱和 spike 用）。
    pub park_time: Duration,
    pub iters: u64,
    /// 每连接 connect→echo 完成延迟（微秒），用于分位。
    pub latencies_us: Vec<u64>,
    /// 刀11：跑完后的数据面计数快照（读末态 Arc<Metrics>）。`relays_spawned` 随真 relay 增长；
    /// gauge（active_relays/fake_ip_*）仅 30s tick 发布，sub-second 压测里不触发 → 单独单测覆盖。
    pub metrics: crate::metrics::MetricsSnapshot,
}

impl Report {
    fn percentile(&self, p: f64) -> u64 {
        if self.latencies_us.is_empty() {
            return 0;
        }
        let mut v = self.latencies_us.clone();
        v.sort_unstable();
        // p∈[0,1] 时 (len-1)*p ≤ len-1，理论不越界；min 仅作浮点防御。
        let idx = (((v.len() as f64 - 1.0) * p).round() as usize).min(v.len() - 1);
        v[idx]
    }
    pub fn p50_us(&self) -> u64 {
        self.percentile(0.50)
    }
    pub fn p95_us(&self) -> u64 {
        self.percentile(0.95)
    }
    pub fn max_us(&self) -> u64 {
        self.latencies_us.iter().copied().max().unwrap_or(0)
    }
    pub fn throughput_mbps(&self) -> f64 {
        let secs = self.wall.as_secs_f64();
        if secs <= 0.0 {
            return 0.0;
        }
        (self.bytes_echoed as f64 * 8.0) / (secs * 1_000_000.0)
    }

    /// 刀12：把本场景的 poll/relay/park/wall 折成 [`LoopProfileSnapshot`]，复用其 fraction 数学。
    /// **注意**：harness wall 受 generator `sleep(200µs)` 节拍污染、不可信；段 fraction（尤其
    /// loop-active 在 burn 下的相对上升）才是可信信号——只作 #4 仪器自检，不当 100M 进度。
    pub fn loop_profile(&self) -> crate::loop_profiler::LoopProfileSnapshot {
        crate::loop_profiler::LoopProfileSnapshot {
            poll: self.poll_time,
            relay: self.relay_time,
            park: self.park_time,
            wall: self.wall,
            iters: self.iters,
        }
    }

    /// 打印一行人类可读的定位指标（测试 `--nocapture` 下显示）。
    pub fn print_row(&self) {
        println!(
            "N={:>4} done={:>4}/{:<4} wall={:>7.1}ms thrpt={:>7.2}Mb/s | \
             poll={:>7.1}ms/{:>6}calls relay={:>7.1}ms/{:>6}calls | \
             listeners max={:>3} avg={:>5.1} | lat p50={:>6}us p95={:>7}us max={:>7}us | \
             tcp_opens={} per_sock_buf={}KB",
            self.connections,
            self.completed,
            self.connections,
            self.wall.as_secs_f64() * 1e3,
            self.throughput_mbps(),
            self.poll_time.as_secs_f64() * 1e3,
            self.poll_calls,
            self.relay_time.as_secs_f64() * 1e3,
            self.relay_calls,
            self.max_listeners,
            self.avg_listeners,
            self.p50_us(),
            self.p95_us(),
            self.max_us(),
            self.tcp_opens,
            self.per_socket_buffer_bytes / 1024,
        );
    }
}

// ============================ TCP 压测场景 ============================

/// 一条发生器侧 TCP 连接的状态机。
struct GenConn {
    handle: SocketHandle,
    payload_len: usize,
    sent: usize,
    recvd: usize,
    started: Instant,
    done_us: Option<u64>,
    closed: bool,
}

const GEN_IP: Ipv4Address = Ipv4Address::new(10, 0, 0, 9);
const TARGET_IP: Ipv4Address = Ipv4Address::new(93, 184, 216, 34);
const TARGET_PORT_BASE: u16 = 9000;

/// 跑一场并发 TCP echo 压测：N 路连接跨 `distinct_ports` 个目标端口，每条往返 `payload_len` 字节。
///
/// 返回 [`Report`]（N/N 完成数 + 主循环三段耗时 + listener 遍历规模 + 吞吐/延迟）。
pub async fn run_tcp_scenario(params: ScenarioParams) -> Report {
    // ---- 1. 内存管道 + 下行 channel + mock 上游 ----
    let gen_to_sut = PacketLink::new();
    let sut_to_gen = PacketLink::new();
    let (downlink_tx, downlink_rx) = mpsc::channel::<Vec<u8>>(1024);
    let echo_buf = (params.payload_len * 2).max(8 * 1024);
    let mock = Arc::new(MockUpstream::new(echo_buf, downlink_tx));

    // ---- 2. 启动 SUT 主循环（内存回环 device + mock 上游 + recording sink）----
    // 刀12：注入合成 on-loop CPU（默认 ZERO；T4 spike 设非零造单核饱和信号）。
    let sut_device = LoopbackTunDevice::with_burn(
        gen_to_sut.clone(),
        sut_to_gen.clone(),
        params.cpu_burn_per_flush,
    );
    let shared = Arc::new(Mutex::new(Recorded::default()));
    let sink = RecordingSink::new(shared.clone());
    let config = TunRuntimeConfig::from_sources(Some(&params.pool_size.to_string()))
        .expect("valid pool size");
    let (tcp_rx_buffer_bytes, tcp_tx_buffer_bytes) = config.tcp_socket_buffer_bytes();
    let per_socket_buffer_bytes = tcp_rx_buffer_bytes + tcp_tx_buffer_bytes;
    // 刀11：bind 共享 Arc<Metrics>，跑完 .abort() 后读末态 snapshot 入 Report（仿 RecordingSink drain 模式）。
    let metrics = Arc::new(Metrics::new());
    let sut = tokio::spawn(run_event_loop(
        sut_device,
        mock.clone(),
        downlink_rx,
        config,
        Arc::clone(&metrics),
        sink,
    ));

    // ---- 3. 发生器：第二 smoltcp 栈，N 路 client 连接 ----
    let mut gen_device = GeneratorDevice::new(sut_to_gen, gen_to_sut);
    let mut gen_iface = {
        let cfg = SmolConfig::new(smoltcp::wire::HardwareAddress::Ip);
        let mut iface = Interface::new(cfg, &mut gen_device, SmolInstant::now());
        iface.update_ip_addrs(|a| {
            a.push(IpCidr::new(IpAddress::Ipv4(GEN_IP), 24)).unwrap();
        });
        // 默认路由（网关填自身 IP，仅为让 smoltcp 对 off-link 目标 emit IP 包）。
        iface.routes_mut().add_default_ipv4_route(GEN_IP).unwrap();
        iface
    };

    let mut sockets = SocketSet::new(vec![]);
    let buf_sz = (params.payload_len * 2).max(4096);
    let mut conns: Vec<GenConn> = Vec::with_capacity(params.connections);
    let payload = vec![0xABu8; params.payload_len];

    for i in 0..params.connections {
        let rx = tcp::SocketBuffer::new(vec![0u8; buf_sz]);
        let tx = tcp::SocketBuffer::new(vec![0u8; buf_sz]);
        let mut sock = tcp::Socket::new(rx, tx);
        let dst_port = TARGET_PORT_BASE + (i % params.distinct_ports.max(1)) as u16;
        let local_port = 40_000u16.wrapping_add(i as u16);
        sock.connect(
            gen_iface.context(),
            (IpAddress::Ipv4(TARGET_IP), dst_port),
            local_port,
        )
        .expect("connect");
        let handle = sockets.add(sock);
        conns.push(GenConn {
            handle,
            payload_len: params.payload_len,
            sent: 0,
            recvd: 0,
            started: Instant::now(),
            done_us: None,
            closed: false,
        });
    }

    // ---- 4. 驱动循环：poll 发生器 + 推进每条连接，直到 N/N 完成或超时 ----
    let wall_start = Instant::now();
    let mut completed = 0usize;
    let mut recv_scratch = vec![0u8; buf_sz];
    while completed < params.connections && wall_start.elapsed() < params.timeout {
        gen_iface.poll(SmolInstant::now(), &mut gen_device, &mut sockets);
        for c in conns.iter_mut() {
            if c.done_us.is_some() && c.closed {
                continue;
            }
            let sock = sockets.get_mut::<tcp::Socket>(c.handle);
            // 发送：连接可写就把负载尽量写出。
            if c.sent < c.payload_len
                && sock.can_send()
                && let Ok(n) = sock.send_slice(&payload[c.sent..])
            {
                c.sent += n;
            }
            // 接收：累计 echo 回来的字节。
            while sock.can_recv() {
                match sock.recv_slice(&mut recv_scratch) {
                    Ok(0) => break,
                    Ok(n) => c.recvd += n,
                    Err(_) => break,
                }
            }
            if c.done_us.is_none() && c.recvd >= c.payload_len {
                c.done_us = Some(c.started.elapsed().as_micros() as u64);
                completed += 1;
                sock.close(); // 释放 SUT 端 listener 槽位，让排队的 SYN 被接受。
            }
            if c.done_us.is_some() && !sock.is_active() {
                c.closed = true;
            }
        }
        // 让出 CPU 给 SUT 任务推进（单/多线程 runtime 都靠这个交错）。
        tokio::time::sleep(Duration::from_micros(200)).await;
    }
    let wall = wall_start.elapsed();

    // ---- 5. 收尾：停 SUT，汇总报告 ----
    sut.abort();
    let _ = sut.await;
    let rec = shared.lock().unwrap();
    let bytes_echoed: usize = conns.iter().map(|c| c.recvd).sum();
    let latencies_us: Vec<u64> = conns.iter().filter_map(|c| c.done_us).collect();
    let avg_listeners = if rec.listener_obs > 0 {
        rec.listener_sum as f64 / rec.listener_obs as f64
    } else {
        0.0
    };
    Report {
        connections: params.connections,
        completed,
        wall,
        bytes_echoed,
        tcp_opens: mock.tcp_opens(),
        poll_time: rec.poll_time,
        poll_calls: rec.poll_calls,
        relay_time: rec.relay_time,
        relay_calls: rec.relay_calls,
        max_listeners: rec.max_listeners,
        avg_listeners,
        per_socket_buffer_bytes,
        park_time: rec.park_time,
        iters: rec.iters,
        latencies_us,
        metrics: metrics.snapshot(0, 0),
    }
}

// ============================ TCP HoL 场景（刀13）============================

/// 刀13：一条上游停读的慢 TCP flow 不应阻塞另一条正常 flow。
#[derive(Debug, Clone)]
pub struct TcpHolReport {
    pub stall_observed: bool,
    pub normal_completed_while_stalled: bool,
    pub stalled_completed_while_stalled: bool,
    pub stalled_completed_after_release: bool,
    pub normal_bytes_match: bool,
    pub stalled_bytes_match: bool,
    pub normal_sent: usize,
    pub normal_received: usize,
    pub stalled_sent: usize,
    pub stalled_received: usize,
    pub tcp_opens_while_stalled: u64,
    pub tcp_opens_after_release: u64,
}

/// 刀14d：一条慢 `open_tcp` 不应阻塞另一条正常 TCP flow。
#[derive(Debug, Clone)]
pub struct TcpSlowOpenReport {
    pub slow_open_observed: bool,
    pub normal_completed_while_slow_open: bool,
    pub slow_completed_after_release: bool,
    pub normal_bytes_match: bool,
    pub slow_bytes_match: bool,
    pub tcp_opens_while_slow_open: u64,
    pub tcp_opens_after_release: u64,
}

struct HolConn {
    handle: SocketHandle,
    payload: Vec<u8>,
    send_chunk: usize,
    sent: usize,
    received: Vec<u8>,
    started: Instant,
    done_us: Option<u64>,
    closed: bool,
}

impl HolConn {
    fn new(handle: SocketHandle, payload: Vec<u8>, send_chunk: usize) -> Self {
        Self {
            handle,
            payload,
            send_chunk: send_chunk.max(1),
            sent: 0,
            received: Vec::new(),
            started: Instant::now(),
            done_us: None,
            closed: false,
        }
    }

    fn done(&self) -> bool {
        self.done_us.is_some()
    }

    fn bytes_match(&self) -> bool {
        self.received == self.payload
    }
}

fn hol_payload(seed: u8, len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| seed.wrapping_add((i & 0xff) as u8))
        .collect()
}

fn push_hol_conn(
    iface: &mut Interface,
    sockets: &mut SocketSet<'_>,
    dst_port: u16,
    local_port: u16,
    payload: Vec<u8>,
) -> SocketHandle {
    let buf_sz = (payload.len() * 2).max(4096);
    let rx = tcp::SocketBuffer::new(vec![0u8; buf_sz]);
    let tx = tcp::SocketBuffer::new(vec![0u8; buf_sz]);
    let mut sock = tcp::Socket::new(rx, tx);
    sock.connect(
        iface.context(),
        (IpAddress::Ipv4(TARGET_IP), dst_port),
        local_port,
    )
    .expect("connect");
    sockets.add(sock)
}

async fn drive_hol_generator<F>(
    iface: &mut Interface,
    device: &mut GeneratorDevice,
    sockets: &mut SocketSet<'_>,
    conns: &mut [HolConn],
    timeout: Duration,
    mut stop: F,
) where
    F: FnMut(&[HolConn]) -> bool,
{
    let start = Instant::now();
    while !stop(conns) && start.elapsed() < timeout {
        iface.poll(SmolInstant::now(), device, sockets);
        for c in conns.iter_mut() {
            if c.done_us.is_some() && c.closed {
                continue;
            }
            let sock = sockets.get_mut::<tcp::Socket>(c.handle);
            if c.sent < c.payload.len() && sock.can_send() {
                let end = (c.sent + c.send_chunk).min(c.payload.len());
                if let Ok(n) = sock.send_slice(&c.payload[c.sent..end]) {
                    c.sent += n;
                }
            }
            while sock.can_recv() {
                match sock.recv(|data| {
                    c.received.extend_from_slice(data);
                    (data.len(), ())
                }) {
                    Ok(()) => {}
                    Err(_) => break,
                }
            }
            if c.done_us.is_none() && c.received.len() >= c.payload.len() {
                c.done_us = Some(c.started.elapsed().as_micros() as u64);
                sock.close();
            }
            if c.done_us.is_some() && !sock.is_active() {
                c.closed = true;
            }
        }
        tokio::time::sleep(Duration::from_micros(50)).await;
    }
}

/// 刀13 HoL 高保真场景：
/// 1. A 流连到会停读但不关闭的 mock upstream，并用小消息把 relay channel 填满；
/// 2. A 仍堵住时启动 B 流，B 必须能完成 echo；
/// 3. 释放 A 后，A 也必须逐字节完整完成，且 stall 期间没有 spurious reopen。
pub async fn run_tcp_hol_scenario() -> TcpHolReport {
    let stall_port = TARGET_PORT_BASE;
    let normal_port = TARGET_PORT_BASE + 1;
    let stall_control = StallControl::new();

    let gen_to_sut = PacketLink::new();
    let sut_to_gen = PacketLink::new();
    let (downlink_tx, downlink_rx) = mpsc::channel::<Vec<u8>>(1024);
    let mock = Arc::new(MockUpstream::with_stall(
        64,
        downlink_tx,
        stall_port,
        16,
        stall_control.clone(),
    ));

    let sut_device = LoopbackTunDevice::new(gen_to_sut.clone(), sut_to_gen.clone());
    let shared = Arc::new(Mutex::new(Recorded::default()));
    let config = TunRuntimeConfig::from_sources(Some("4")).unwrap();
    let sut = tokio::spawn(run_event_loop(
        sut_device,
        mock.clone(),
        downlink_rx,
        config,
        Arc::new(Metrics::new()),
        RecordingSink::new(shared),
    ));

    let mut gen_device = GeneratorDevice::new(sut_to_gen, gen_to_sut);
    let mut gen_iface = {
        let cfg = SmolConfig::new(smoltcp::wire::HardwareAddress::Ip);
        let mut iface = Interface::new(cfg, &mut gen_device, SmolInstant::now());
        iface.update_ip_addrs(|a| {
            a.push(IpCidr::new(IpAddress::Ipv4(GEN_IP), 24)).unwrap();
        });
        iface.routes_mut().add_default_ipv4_route(GEN_IP).unwrap();
        iface
    };

    let mut sockets = SocketSet::new(vec![]);
    let stall_payload = hol_payload(0xA0, 16 * 1024);
    let stall_handle = push_hol_conn(
        &mut gen_iface,
        &mut sockets,
        stall_port,
        41_000,
        stall_payload.clone(),
    );
    let mut conns = vec![HolConn::new(stall_handle, stall_payload, 4)];

    // 先只驱动 A：等 mock 已经停读，并继续送足够多小消息来填满 A 的 relay channel。
    drive_hol_generator(
        &mut gen_iface,
        &mut gen_device,
        &mut sockets,
        &mut conns,
        Duration::from_secs(8),
        |c| stall_control.is_stalled() && c[0].sent >= 8 * 1024,
    )
    .await;
    let stall_observed = stall_control.is_stalled();

    let normal_payload = hol_payload(0x20, 8 * 1024);
    let normal_handle = push_hol_conn(
        &mut gen_iface,
        &mut sockets,
        normal_port,
        41_001,
        normal_payload.clone(),
    );
    conns.push(HolConn::new(normal_handle, normal_payload, 1024));

    // A 仍停读时，B 应能独立完成。旧实现会卡在 A 的 tx.send().await，这里超时失败。
    drive_hol_generator(
        &mut gen_iface,
        &mut gen_device,
        &mut sockets,
        &mut conns,
        Duration::from_secs(5),
        |c| c[1].done(),
    )
    .await;
    let normal_completed_while_stalled = conns[1].done();
    let stalled_completed_while_stalled = conns[0].done();
    let tcp_opens_while_stalled = mock.tcp_opens();

    stall_control.release();
    drive_hol_generator(
        &mut gen_iface,
        &mut gen_device,
        &mut sockets,
        &mut conns,
        Duration::from_secs(10),
        |c| c[0].done() && c[1].done(),
    )
    .await;

    sut.abort();

    TcpHolReport {
        stall_observed,
        normal_completed_while_stalled,
        stalled_completed_while_stalled,
        stalled_completed_after_release: conns[0].done(),
        normal_bytes_match: conns[1].bytes_match(),
        stalled_bytes_match: conns[0].bytes_match(),
        normal_sent: conns[1].sent,
        normal_received: conns[1].received.len(),
        stalled_sent: conns[0].sent,
        stalled_received: conns[0].received.len(),
        tcp_opens_while_stalled,
        tcp_opens_after_release: mock.tcp_opens(),
    }
}

/// 刀14d slow-open 高保真场景：
/// 1. A 流的 mock `open_tcp` 在返回 relay stream 前等待；
/// 2. A open 仍卡住时启动 B 流，B 必须完成；
/// 3. 释放 A 后，A 也必须逐字节完整完成，且没有 spurious reopen。
pub async fn run_tcp_slow_open_scenario() -> TcpSlowOpenReport {
    let slow_port = TARGET_PORT_BASE;
    let normal_port = TARGET_PORT_BASE + 1;
    let open_control = OpenStallControl::new();

    let gen_to_sut = PacketLink::new();
    let sut_to_gen = PacketLink::new();
    let (downlink_tx, downlink_rx) = mpsc::channel::<Vec<u8>>(1024);
    let mock = Arc::new(MockUpstream::with_open_stall(
        64 * 1024,
        downlink_tx,
        slow_port,
        open_control.clone(),
    ));

    let sut_device = LoopbackTunDevice::new(gen_to_sut.clone(), sut_to_gen.clone());
    let shared = Arc::new(Mutex::new(Recorded::default()));
    let config = TunRuntimeConfig::from_sources(Some("4")).unwrap();
    let sut = tokio::spawn(run_event_loop(
        sut_device,
        mock.clone(),
        downlink_rx,
        config,
        Arc::new(Metrics::new()),
        RecordingSink::new(shared),
    ));

    let mut gen_device = GeneratorDevice::new(sut_to_gen, gen_to_sut);
    let mut gen_iface = {
        let cfg = SmolConfig::new(smoltcp::wire::HardwareAddress::Ip);
        let mut iface = Interface::new(cfg, &mut gen_device, SmolInstant::now());
        iface.update_ip_addrs(|a| {
            a.push(IpCidr::new(IpAddress::Ipv4(GEN_IP), 24)).unwrap();
        });
        iface.routes_mut().add_default_ipv4_route(GEN_IP).unwrap();
        iface
    };

    let mut sockets = SocketSet::new(vec![]);
    let slow_payload = hol_payload(0xB0, 8 * 1024);
    let slow_handle = push_hol_conn(
        &mut gen_iface,
        &mut sockets,
        slow_port,
        42_000,
        slow_payload.clone(),
    );
    let mut conns = vec![HolConn::new(slow_handle, slow_payload, 1024)];

    drive_hol_generator(
        &mut gen_iface,
        &mut gen_device,
        &mut sockets,
        &mut conns,
        Duration::from_secs(3),
        |_| open_control.is_waiting(),
    )
    .await;
    let slow_open_observed = open_control.is_waiting();

    let normal_payload = hol_payload(0x30, 8 * 1024);
    let normal_handle = push_hol_conn(
        &mut gen_iface,
        &mut sockets,
        normal_port,
        42_001,
        normal_payload.clone(),
    );
    conns.push(HolConn::new(normal_handle, normal_payload, 1024));

    drive_hol_generator(
        &mut gen_iface,
        &mut gen_device,
        &mut sockets,
        &mut conns,
        Duration::from_secs(3),
        |c| c[1].done(),
    )
    .await;
    let normal_completed_while_slow_open = conns[1].done();
    let tcp_opens_while_slow_open = mock.tcp_opens();

    open_control.release();
    drive_hol_generator(
        &mut gen_iface,
        &mut gen_device,
        &mut sockets,
        &mut conns,
        Duration::from_secs(10),
        |c| c[0].done() && c[1].done(),
    )
    .await;

    sut.abort();

    TcpSlowOpenReport {
        slow_open_observed,
        normal_completed_while_slow_open,
        slow_completed_after_release: conns[0].done(),
        normal_bytes_match: conns[1].bytes_match(),
        slow_bytes_match: conns[0].bytes_match(),
        tcp_opens_while_slow_open,
        tcp_opens_after_release: mock.tcp_opens(),
    }
}

// ============================ UDP echo 场景（轻量 liveness）============================

/// 轻量 UDP 用例报告。
#[derive(Debug, Clone)]
pub struct UdpReport {
    pub sent: u64,
    pub uplinks: u64,
}

/// 轻量 UDP echo：发若干 UDP datagram 经 mock echo 上/下行往返，验证 datagram 面不被 TCP 饿死。
///
/// 注：UDP 主体吞吐压测留刀3。这里只做 liveness（mock echo 计数）。
pub async fn run_udp_echo_scenario(datagrams: usize, payload_len: usize) -> UdpReport {
    let gen_to_sut = PacketLink::new();
    let sut_to_gen = PacketLink::new();
    let (downlink_tx, downlink_rx) = mpsc::channel::<Vec<u8>>(1024);
    let mock = Arc::new(MockUpstream::new(8 * 1024, downlink_tx));
    let sut_device = LoopbackTunDevice::new(gen_to_sut.clone(), sut_to_gen);
    let shared = Arc::new(Mutex::new(Recorded::default()));
    let config = TunRuntimeConfig::from_sources(Some("2")).unwrap();
    let sut = tokio::spawn(run_event_loop(
        sut_device,
        mock.clone(),
        downlink_rx,
        config,
        Arc::new(Metrics::new()), // 刀11 T4：接线占位（UDP scenario 不读 snapshot）
        RecordingSink::new(shared),
    ));

    // 直接构造 UDP/IP 包注入 gen_to_sut（src=GEN, dst=TARGET:53 之外端口，走 UDP relay）。
    let mut sent = 0u64;
    for i in 0..datagrams {
        let pkt = build_udp_ip(
            GEN_IP,
            40_000u16.wrapping_add(i as u16),
            TARGET_IP,
            5000,
            &vec![0xCDu8; payload_len],
        );
        gen_to_sut.push(BytesMut::from(&pkt[..]));
        sent += 1;
        tokio::time::sleep(Duration::from_micros(200)).await;
    }
    // 给 SUT 时间处理上行。
    tokio::time::sleep(Duration::from_millis(50)).await;
    sut.abort();
    UdpReport {
        sent,
        uplinks: mock.udp_uplinks(),
    }
}

/// 构造一个裸 IPv4/UDP 包（用于 UDP relay 注入）。
fn build_udp_ip(
    src: Ipv4Address,
    src_port: u16,
    dst: Ipv4Address,
    dst_port: u16,
    payload: &[u8],
) -> Vec<u8> {
    use etherparse::PacketBuilder;
    let builder = PacketBuilder::ipv4(src.0, dst.0, 64).udp(src_port, dst_port);
    let mut out = Vec::with_capacity(builder.size(payload.len()));
    builder.write(&mut out, payload).unwrap();
    out
}

// ============================ UDP 吞吐 + 分片重组场景（刀3）============================

/// UDP 吞吐场景报告。`echoed_intact` = 下行回到「app」且**整 payload 逐字节匹配**的包数
/// （分片场景下即重组正确性）。`lost` = sent − echoed_intact。
#[derive(Debug, Clone)]
pub struct UdpThroughputReport {
    pub sent: usize,
    pub echoed_intact: usize,
    pub lost: usize,
    pub wall: Duration,
    pub payload_len: usize,
    pub fragmented: bool,
}

impl UdpThroughputReport {
    pub fn pps(&self) -> f64 {
        let s = self.wall.as_secs_f64();
        if s <= 0.0 {
            0.0
        } else {
            self.echoed_intact as f64 / s
        }
    }
    pub fn throughput_mbps(&self) -> f64 {
        let s = self.wall.as_secs_f64();
        if s <= 0.0 {
            return 0.0;
        }
        (self.echoed_intact as f64 * self.payload_len as f64 * 8.0) / (s * 1_000_000.0)
    }
    pub fn print_row(&self) {
        println!(
            "UDP frag={:<5} N={:>4} intact={:>4}/{:<4} lost={:>3} wall={:>7.1}ms | {:>8.0} pps {:>7.2} Mb/s | payload={}B",
            self.fragmented,
            self.sent,
            self.echoed_intact,
            self.sent,
            self.lost,
            self.wall.as_secs_f64() * 1e3,
            self.pps(),
            self.throughput_mbps(),
            self.payload_len,
        );
    }
}

/// app 第 `i` 包的期望 payload：前 2 字节 = marker(i, BE)，其余 `payload[j] = (i + j) & 0xff`
/// （位置相关 → 重组乱序/损坏可检出）。
fn throughput_payload(i: usize, payload_len: usize) -> Vec<u8> {
    let mut p = vec![0u8; payload_len];
    if payload_len >= 2 {
        p[0..2].copy_from_slice(&(i as u16).to_be_bytes());
    }
    for (j, b) in p.iter_mut().enumerate().skip(2) {
        *b = ((i + j) & 0xff) as u8;
    }
    p
}

/// 从 `sut_to_gen` 排空已回到「app」的下行 UDP 包，逐字节核对完整性，收集 intact 的 marker。
/// 返回是否至少排空了一个包。
fn drain_intact_echoes(
    link: &PacketLink,
    payload_len: usize,
    intact: &mut std::collections::HashSet<u16>,
) -> bool {
    let mut any = false;
    while let Some(pkt) = link.pop() {
        any = true;
        let Some(udp) = crate::udp_relay::parse_inbound_udp(&pkt) else {
            continue;
        };
        if udp.payload.len() != payload_len {
            continue;
        }
        let marker = u16::from_be_bytes([udp.payload[0], udp.payload[1]]) as usize;
        if udp.payload == throughput_payload(marker, payload_len).as_slice() {
            intact.insert(marker as u16);
        }
    }
    any
}

/// 跑一场 UDP 吞吐：N 个独立 flow（每包独立 src_port → 独立 assoc）发 `payload_len` 字节，
/// 经 mock 上游回灌（`frag_chunk=Some` 时拆多帧 → 主循环 `FragReassembler` 重组）→ 核对回到 app 的完整性。
///
/// 注：真 datagram `TooLarge` / stream 兜底走真 quinn，harness 测不到（同 #3 边界）→ 归 acceptance。
/// 本场景量化的是**主循环 UDP 路径 + 重组**的吞吐/丢包/正确性。
pub async fn run_udp_throughput_scenario(
    n: usize,
    payload_len: usize,
    frag_chunk: Option<usize>,
    timeout: Duration,
) -> UdpThroughputReport {
    let gen_to_sut = PacketLink::new();
    let sut_to_gen = PacketLink::new();
    let (downlink_tx, downlink_rx) = mpsc::channel::<Vec<u8>>(4096);
    let mock = Arc::new(MockUpstream::with_frag(64 * 1024, downlink_tx, frag_chunk));
    let sut_device = LoopbackTunDevice::new(gen_to_sut.clone(), sut_to_gen.clone());
    let shared = Arc::new(Mutex::new(Recorded::default()));
    let config = TunRuntimeConfig::from_sources(Some("2")).unwrap();
    let sut = tokio::spawn(run_event_loop(
        sut_device,
        mock.clone(),
        downlink_rx,
        config,
        Arc::new(Metrics::new()), // 刀11 T4：接线占位（UDP scenario 不读 snapshot）
        RecordingSink::new(shared),
    ));

    let mut intact = std::collections::HashSet::new();
    let start = Instant::now();
    for i in 0..n {
        let payload = throughput_payload(i, payload_len);
        let pkt = build_udp_ip(
            GEN_IP,
            40_000u16.wrapping_add(i as u16),
            TARGET_IP,
            5000,
            &payload,
        );
        gen_to_sut.push(BytesMut::from(&pkt[..]));
        tokio::time::sleep(Duration::from_micros(100)).await;
        drain_intact_echoes(&sut_to_gen, payload_len, &mut intact);
    }
    // 收尾排空：直到收齐或超时。
    while intact.len() < n && start.elapsed() < timeout {
        if !drain_intact_echoes(&sut_to_gen, payload_len, &mut intact) {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }
    let wall = start.elapsed();
    sut.abort();
    UdpThroughputReport {
        sent: n,
        echoed_intact: intact.len(),
        lost: n - intact.len(),
        wall,
        payload_len,
        fragmented: frag_chunk.is_some(),
    }
}

#[derive(Debug, Clone, Copy)]
pub struct D16BidirectionalControlReport {
    pub first_response_seen: bool,
    pub second_control_echoed: bool,
    pub round_trip_intact: bool,
    pub tcp_opens: u64,
}

/// Exercise the production D16 relay variant with two real smoltcp stacks.
/// After the first TCP payload is flushed to the generator, async TUN wait
/// readiness is deliberately suppressed; only the bounded nonblocking TUN RX
/// follow-up can ingest the generator's ACK and second control payload.
pub async fn run_d16_bidirectional_control_scenario() -> D16BidirectionalControlReport {
    let gen_to_sut = PacketLink::new();
    let sut_to_gen = PacketLink::new();
    let (downlink_tx, downlink_rx) = mpsc::channel::<Vec<u8>>(8);
    let mock = Arc::new(MockUpstream::with_d16_byte_owned(64 * 1024, downlink_tx));
    let sut_device = LoopbackTunDevice::with_suppressed_wait_after_tcp_payload_flush(
        gen_to_sut.clone(),
        sut_to_gen.clone(),
    );
    let config = TunRuntimeConfig::from_sources(Some("2")).unwrap();
    let sut = tokio::spawn(run_event_loop(
        sut_device,
        mock.clone(),
        downlink_rx,
        config,
        Arc::new(Metrics::new()),
        RecordingSink::new(Arc::new(Mutex::new(Recorded::default()))),
    ));

    let mut gen_device = GeneratorDevice::new(sut_to_gen, gen_to_sut);
    let mut gen_iface = {
        let cfg = SmolConfig::new(smoltcp::wire::HardwareAddress::Ip);
        let mut iface = Interface::new(cfg, &mut gen_device, SmolInstant::now());
        iface.update_ip_addrs(|addrs| {
            addrs
                .push(IpCidr::new(IpAddress::Ipv4(GEN_IP), 24))
                .unwrap();
        });
        iface.routes_mut().add_default_ipv4_route(GEN_IP).unwrap();
        iface
    };
    let mut sockets = SocketSet::new(vec![]);
    let rx = tcp::SocketBuffer::new(vec![0u8; 16 * 1024]);
    let tx = tcp::SocketBuffer::new(vec![0u8; 16 * 1024]);
    let mut socket = tcp::Socket::new(rx, tx);
    socket
        .connect(
            gen_iface.context(),
            (IpAddress::Ipv4(TARGET_IP), TARGET_PORT_BASE),
            42_000,
        )
        .unwrap();
    let handle = sockets.add(socket);

    let first_control = [0x11u8];
    let second_control = vec![0x22u8; 1532];
    let mut first_sent = 0usize;
    let mut second_sent = 0usize;
    let mut received = Vec::with_capacity(first_control.len() + second_control.len());
    let started = Instant::now();
    while received.len() < first_control.len() + second_control.len()
        && started.elapsed() < Duration::from_secs(2)
    {
        gen_iface.poll(SmolInstant::now(), &mut gen_device, &mut sockets);
        let socket = sockets.get_mut::<tcp::Socket>(handle);
        if first_sent < first_control.len() && socket.can_send() {
            if let Ok(sent) = socket.send_slice(&first_control[first_sent..]) {
                first_sent += sent;
            }
        }
        let mut scratch = [0u8; 2048];
        while socket.can_recv() {
            match socket.recv_slice(&mut scratch) {
                Ok(0) | Err(_) => break,
                Ok(read) => received.extend_from_slice(&scratch[..read]),
            }
        }
        if received.len() >= first_control.len()
            && second_sent < second_control.len()
            && socket.can_send()
        {
            if let Ok(sent) = socket.send_slice(&second_control[second_sent..]) {
                second_sent += sent;
            }
        }
        tokio::time::sleep(Duration::from_micros(200)).await;
    }
    sut.abort();

    let expected: Vec<u8> = first_control
        .iter()
        .copied()
        .chain(second_control.iter().copied())
        .collect();
    D16BidirectionalControlReport {
        first_response_seen: received.starts_with(&first_control),
        second_control_echoed: received.len() >= expected.len(),
        round_trip_intact: received == expected,
        tcp_opens: mock.tcp_opens(),
    }
}

#[derive(Debug, Clone, Copy)]
pub struct D16TunRxStarvationReport {
    pub payload_bytes: usize,
    pub bootstrap_sent_bytes: usize,
    pub received_bytes: usize,
    pub payload_elapsed: Duration,
    pub receiver_mbps: f64,
    pub modeled_tx_dropped: u64,
    pub ring_high_water_packets: u64,
    pub downlink_high_water_packets: u64,
    pub max_tcp_payload_packets_per_flush: u64,
    pub tcp_opens: u64,
    pub tun_rx_budget_exhausted: u64,
    pub backlog_pause_edges: u64,
    pub backlog_resume_edges: u64,
    pub actor_bypass_admitted_bytes: u64,
    pub tun_rx_actor_wakes: u64,
    pub first_drop_backlog_active: bool,
    pub first_drop_running_flows: usize,
    pub first_drop_drain_only_flows: usize,
    pub first_drop_recovery_flows: usize,
    pub final_backlog_active: bool,
    pub final_running_flows: usize,
    pub final_drain_only_flows: usize,
    pub final_recovery_flows: usize,
    pub remote_eof_seen: bool,
    pub eof_after_owned_queue_drain: bool,
    pub close_egress_bytes: usize,
    pub terminal_drop_bytes: u64,
    pub terminal_late_payload_bytes: u64,
    pub final_owned_queue_bytes: usize,
    pub final_pending_bytes: usize,
    pub final_inflight_bytes: usize,
    pub final_local_eof_sent_flows: usize,
}

#[derive(Debug, Clone, Copy)]
enum D16TunRxScenarioLimit {
    Wall(Duration),
    #[cfg(test)]
    SchedulerYields(usize),
}

impl D16TunRxScenarioLimit {
    fn permits_next_iteration(self, started: Instant, scheduler_yields: usize) -> bool {
        #[cfg(not(test))]
        let _ = scheduler_yields;
        match self {
            Self::Wall(timeout) => started.elapsed() < timeout,
            #[cfg(test)]
            Self::SchedulerYields(limit) => scheduler_yields < limit,
        }
    }
}

/// Model the Linux kernel-to-userspace TUN transmit ring as a bounded packet
/// queue while a native D16 relay sends a reverse-only payload. Generator TCP
/// ACK/control packets enter this bounded ring; mini_vpn must drain them through
/// the same `TunIo`/event-loop path as production.
pub async fn run_d16_tun_rx_starvation_scenario(
    payload_bytes: usize,
    ring_capacity_packets: usize,
    timeout: Duration,
) -> D16TunRxStarvationReport {
    run_d16_tun_rx_starvation_scenario_with_limit(
        payload_bytes,
        ring_capacity_packets,
        D16TunRxScenarioLimit::Wall(timeout),
    )
    .await
}

async fn run_d16_tun_rx_starvation_scenario_with_limit(
    payload_bytes: usize,
    ring_capacity_packets: usize,
    limit: D16TunRxScenarioLimit,
) -> D16TunRxStarvationReport {
    let gen_to_sut = PacketLink::bounded(ring_capacity_packets);
    let sut_to_gen = PacketLink::new();
    let (downlink_tx, downlink_rx) = mpsc::channel::<Vec<u8>>(8);
    let mock = Arc::new(MockUpstream::with_d16_reverse_payload(
        512 * 1024,
        downlink_tx,
        payload_bytes,
    ));
    let max_tcp_payload_packets_per_flush = Arc::new(AtomicU64::new(0));
    let sut_device = LoopbackTunDevice::new(gen_to_sut.clone(), sut_to_gen.clone())
        .with_tcp_payload_flush_counter(Arc::clone(&max_tcp_payload_packets_per_flush));
    let config = TunRuntimeConfig::from_sources(Some("2")).unwrap();
    let recorded = Arc::new(Mutex::new(Recorded::default()));
    let sut = tokio::spawn(run_event_loop(
        sut_device,
        mock.clone(),
        downlink_rx,
        config,
        Arc::new(Metrics::new()),
        RecordingSink::new(recorded.clone()),
    ));

    let mut gen_device =
        GeneratorDevice::with_ingress_packets_per_poll(sut_to_gen.clone(), gen_to_sut.clone(), 2);
    let mut gen_iface = {
        let cfg = SmolConfig::new(smoltcp::wire::HardwareAddress::Ip);
        let mut iface = Interface::new(cfg, &mut gen_device, SmolInstant::now());
        iface.update_ip_addrs(|addrs| {
            addrs
                .push(IpCidr::new(IpAddress::Ipv4(GEN_IP), 24))
                .unwrap();
        });
        iface.routes_mut().add_default_ipv4_route(GEN_IP).unwrap();
        iface
    };
    let mut sockets = SocketSet::new(vec![]);
    let rx = tcp::SocketBuffer::new(vec![0u8; 1024 * 1024]);
    let tx = tcp::SocketBuffer::new(vec![0u8; 64 * 1024]);
    let mut socket = tcp::Socket::new(rx, tx);
    socket.set_ack_delay(None);
    socket
        .connect(
            gen_iface.context(),
            (IpAddress::Ipv4(TARGET_IP), TARGET_PORT_BASE),
            42_000,
        )
        .unwrap();
    let handle = sockets.add(socket);

    let started = Instant::now();
    let bootstrap = [0xA5u8];
    let mut bootstrap_sent_bytes = 0usize;
    let mut received_bytes = 0usize;
    let mut payload_completed_elapsed = None;
    let mut remote_eof_seen = false;
    let mut first_drop_state: Option<(bool, usize, usize, usize)> = None;
    let mut scratch = vec![0u8; 64 * 1024];
    let mut scheduler_yields = 0usize;
    while limit.permits_next_iteration(started, scheduler_yields) {
        gen_device.begin_poll();
        gen_iface.poll(SmolInstant::now(), &mut gen_device, &mut sockets);
        let socket = sockets.get_mut::<tcp::Socket>(handle);
        if bootstrap_sent_bytes < bootstrap.len() && socket.can_send() {
            if let Ok(sent) = socket.send_slice(&bootstrap[bootstrap_sent_bytes..]) {
                bootstrap_sent_bytes = bootstrap_sent_bytes.saturating_add(sent);
            }
        }
        while socket.can_recv() {
            match socket.recv_slice(&mut scratch) {
                Ok(0) | Err(_) => break,
                Ok(read) => received_bytes = received_bytes.saturating_add(read),
            }
        }
        if received_bytes >= payload_bytes && payload_completed_elapsed.is_none() {
            payload_completed_elapsed = Some(started.elapsed());
        }
        remote_eof_seen |= received_bytes >= payload_bytes && !socket.may_recv();
        if first_drop_state.is_none() && gen_to_sut.dropped_packets() > 0 {
            let recorded = recorded.lock().unwrap();
            first_drop_state = Some((
                recorded.backlog_active,
                recorded.d16_running_flows,
                recorded.d16_drain_only_flows,
                recorded.d16_recovery_flows,
            ));
        }
        let eof_observed = recorded.lock().unwrap().d16_eof_observed;
        if received_bytes >= payload_bytes && remote_eof_seen && eof_observed {
            break;
        }
        tokio::task::yield_now().await;
        scheduler_yields = scheduler_yields.saturating_add(1);
    }
    if sockets.get::<tcp::Socket>(handle).is_active() {
        sockets.get_mut::<tcp::Socket>(handle).close();
    }
    sut.abort();
    let _ = sut.await;
    let payload_elapsed = payload_completed_elapsed.unwrap_or_else(|| started.elapsed());
    let receiver_mbps = if payload_elapsed.is_zero() {
        f64::INFINITY
    } else {
        received_bytes as f64 * 8.0 / payload_elapsed.as_secs_f64() / 1_000_000.0
    };
    let recorded = recorded.lock().unwrap();
    let (
        first_drop_backlog_active,
        first_drop_running_flows,
        first_drop_drain_only_flows,
        first_drop_recovery_flows,
    ) = first_drop_state.unwrap_or_default();

    D16TunRxStarvationReport {
        payload_bytes,
        bootstrap_sent_bytes,
        received_bytes,
        payload_elapsed,
        receiver_mbps,
        modeled_tx_dropped: gen_to_sut.dropped_packets(),
        ring_high_water_packets: gen_to_sut.high_water_packets(),
        downlink_high_water_packets: sut_to_gen.high_water_packets(),
        max_tcp_payload_packets_per_flush: max_tcp_payload_packets_per_flush
            .load(Ordering::Relaxed),
        tcp_opens: mock.tcp_opens(),
        tun_rx_budget_exhausted: recorded.tun_rx_budget_exhausted,
        backlog_pause_edges: recorded.backlog_pause_edges,
        backlog_resume_edges: recorded.backlog_resume_edges,
        actor_bypass_admitted_bytes: recorded.actor_bypass_admitted_bytes,
        tun_rx_actor_wakes: recorded.d16_tun_rx_actor_wakes,
        first_drop_backlog_active,
        first_drop_running_flows,
        first_drop_drain_only_flows,
        first_drop_recovery_flows,
        final_backlog_active: recorded.backlog_active,
        final_running_flows: recorded.d16_running_flows,
        final_drain_only_flows: recorded.d16_drain_only_flows,
        final_recovery_flows: recorded.d16_recovery_flows,
        remote_eof_seen,
        eof_after_owned_queue_drain: recorded.d16_eof_observed && recorded.d16_eof_tail_bytes == 0,
        close_egress_bytes: recorded.d16_eof_tail_bytes,
        terminal_drop_bytes: recorded.terminal_drop_bytes,
        terminal_late_payload_bytes: recorded.terminal_late_payload_bytes,
        final_owned_queue_bytes: recorded.d16_owned_queue_bytes,
        final_pending_bytes: recorded.d16_pending_bytes,
        final_inflight_bytes: recorded.d16_inflight_bytes,
        final_local_eof_sent_flows: recorded.d16_local_eof_sent_flows,
    }
}

const D16_INTEGRATED_STEP_BYTES: usize = 128 * 1024;
const D16_INTEGRATED_STEP_MILLIS: u64 = 5;
const D16_INTEGRATED_DURATION_SECS: u64 = 30;
const D16_INTEGRATED_STEPS: usize =
    (D16_INTEGRATED_DURATION_SECS * 1000 / D16_INTEGRATED_STEP_MILLIS) as usize;

#[derive(Debug)]
pub enum D16IntegratedHarnessError {
    Config,
    Queue(ByteQueuePushError),
    TailDidNotDrain,
}

#[derive(Debug, Clone, Copy)]
pub struct D16IntegratedReport {
    pub receiver_capacity_mbps: f64,
    pub remote_bytes: u64,
    pub actor_admitted_bytes: u64,
    pub actor_bypass_admitted_bytes: u64,
    pub tun_drop_bytes: u64,
    pub close_egress_bytes: usize,
    pub per_flow_high_water_bytes: usize,
    pub global_high_water_bytes: usize,
    pub readiness_wakes: u64,
}

pub async fn run_d16_byte_owned_pressure_scenario()
-> Result<D16IntegratedReport, D16IntegratedHarnessError> {
    let budget = D16GlobalByteBudget::new_default();
    let mut flow =
        D16HarnessFlow::new(budget.clone()).map_err(|_| D16IntegratedHarnessError::Config)?;
    let mut readiness_wakes = 0u64;
    let mut per_flow_high_water_bytes = 0usize;

    for _ in 0..D16_INTEGRATED_STEPS {
        if !flow.permissions().allow_read {
            return Err(D16IntegratedHarnessError::TailDidNotDrain);
        }
        if flow
            .enqueue_remote_chunk(D16_INTEGRATED_STEP_BYTES)
            .await
            .map_err(D16IntegratedHarnessError::Queue)?
        {
            readiness_wakes = readiness_wakes.saturating_add(1);
        }
        let _ = flow.cycle(false, false, D16_INTEGRATED_STEP_BYTES);
        per_flow_high_water_bytes =
            per_flow_high_water_bytes.max(flow.snapshot().queue.high_water_bytes);
    }

    if flow.close_remote().await {
        readiness_wakes = readiness_wakes.saturating_add(1);
    }
    for _ in 0..16 {
        let _ = flow.cycle(false, false, D16_INTEGRATED_STEP_BYTES);
        if flow.snapshot().eof_ready {
            break;
        }
    }
    let snapshot = flow.snapshot();
    if !snapshot.eof_ready {
        return Err(D16IntegratedHarnessError::TailDidNotDrain);
    }
    let seconds = D16_INTEGRATED_DURATION_SECS as f64;
    let receiver_capacity_mbps =
        (snapshot.committed_tun_bytes as f64 * 8.0) / (seconds * 1_000_000.0);
    Ok(D16IntegratedReport {
        receiver_capacity_mbps,
        remote_bytes: (D16_INTEGRATED_STEPS * D16_INTEGRATED_STEP_BYTES) as u64,
        actor_admitted_bytes: snapshot.actor_admitted_bytes,
        actor_bypass_admitted_bytes: snapshot.actor_bypass_admitted_bytes,
        tun_drop_bytes: 0,
        close_egress_bytes: snapshot
            .queue
            .owned_bytes()
            .saturating_add(snapshot.pending_bytes)
            .saturating_add(snapshot.inflight_bytes),
        per_flow_high_water_bytes,
        global_high_water_bytes: budget.snapshot().high_water_bytes,
        readiness_wakes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tcp_downlink_pump::{
        AsyncLeasedByteFlowQueue, D16_GLOBAL_BYTE_BUDGET_DEFAULT_BYTES, DownstreamPermitReleaseMode,
    };
    use crate::tcp_egress::EgressPhase;

    #[derive(Default)]
    struct FailAfterWriteTrigger {
        written: AtomicBool,
        reader_waker: Mutex<Option<std::task::Waker>>,
    }

    impl FailAfterWriteTrigger {
        fn fire(&self) {
            self.written.store(true, Ordering::Release);
            if let Some(waker) = self.reader_waker.lock().unwrap().take() {
                waker.wake();
            }
        }
    }

    struct FailAfterWriteReader {
        trigger: Arc<FailAfterWriteTrigger>,
        failed: bool,
    }

    impl NativeTcpReader for FailAfterWriteReader {
        fn poll_read_chunk(
            &mut self,
            cx: &mut Context<'_>,
            _max_len: usize,
        ) -> Poll<std::io::Result<Option<NativeTcpChunk>>> {
            if self.failed {
                return Poll::Pending;
            }
            if self.trigger.written.load(Ordering::Acquire) {
                self.failed = true;
                return Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::ConnectionReset,
                    "injected remote read failure",
                )));
            }
            *self.trigger.reader_waker.lock().unwrap() = Some(cx.waker().clone());
            if self.trigger.written.load(Ordering::Acquire) {
                cx.waker().wake_by_ref();
            }
            Poll::Pending
        }
    }

    struct FailAfterWriteWriter {
        trigger: Arc<FailAfterWriteTrigger>,
        written_bytes: Arc<AtomicU64>,
    }

    impl tokio::io::AsyncWrite for FailAfterWriteWriter {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            self.written_bytes
                .fetch_add(buf.len() as u64, Ordering::Relaxed);
            self.trigger.fire();
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    struct OneShotFailingD16Upstream {
        relay: Mutex<Option<NativeTcpRelayStream>>,
    }

    #[async_trait::async_trait]
    impl ProxyUpstream for OneShotFailingD16Upstream {
        async fn open_tcp(&self, _target: &TargetAddr) -> Result<RelayStream, ClientError> {
            Err(ClientError::InvalidTarget(
                "failing D16 harness requires native relay open".into(),
            ))
        }

        async fn open_tcp_relay(
            &self,
            _target: &TargetAddr,
        ) -> Result<OpenedTcpRelay, ClientError> {
            self.relay
                .lock()
                .unwrap()
                .take()
                .map(OpenedTcpRelay::NativeByteOwned)
                .ok_or_else(|| ClientError::InvalidTarget("failing relay already opened".into()))
        }
    }

    #[async_trait::async_trait]
    impl DatagramUpstream for OneShotFailingD16Upstream {
        async fn send_udp(&self, _datagram: Vec<u8>) {}
    }

    struct OneShotRealQuinnD16Upstream {
        relay: Mutex<Option<NativeTcpRelayStream>>,
    }

    #[async_trait::async_trait]
    impl ProxyUpstream for OneShotRealQuinnD16Upstream {
        async fn open_tcp(&self, _target: &TargetAddr) -> Result<RelayStream, ClientError> {
            Err(ClientError::InvalidTarget(
                "real Quinn D16 harness requires native relay open".into(),
            ))
        }

        async fn open_tcp_relay(
            &self,
            _target: &TargetAddr,
        ) -> Result<OpenedTcpRelay, ClientError> {
            self.relay
                .lock()
                .unwrap()
                .take()
                .map(OpenedTcpRelay::NativeByteOwned)
                .ok_or_else(|| ClientError::InvalidTarget("real Quinn relay already opened".into()))
        }
    }

    #[async_trait::async_trait]
    impl DatagramUpstream for OneShotRealQuinnD16Upstream {
        async fn send_udp(&self, _datagram: Vec<u8>) {}
    }

    #[test]
    fn loopback_try_recv_rx_reports_no_ready_packet_without_blocking() {
        let inbound = PacketLink::new();
        let outbound = PacketLink::new();
        let mut device = LoopbackTunDevice::new(inbound, outbound);

        assert!(!device.try_recv_rx().unwrap());
        assert!(device.rx_peek().is_none());
    }

    #[test]
    fn loopback_try_recv_rx_fills_single_rx_slot_without_overwriting() {
        let inbound = PacketLink::new();
        let outbound = PacketLink::new();
        inbound.push(BytesMut::from(&b"first"[..]));
        inbound.push(BytesMut::from(&b"second"[..]));
        let mut device = LoopbackTunDevice::new(inbound, outbound);

        assert!(device.try_recv_rx().unwrap());
        assert_eq!(device.rx_peek().unwrap(), b"first");

        assert!(device.try_recv_rx().unwrap());
        assert_eq!(
            device.rx_peek().unwrap(),
            b"first",
            "nonblocking receive must not overwrite an unconsumed rx slot",
        );

        assert_eq!(device.rx_take().unwrap(), BytesMut::from(&b"first"[..]));
        assert!(device.try_recv_rx().unwrap());
        assert_eq!(device.rx_take().unwrap(), BytesMut::from(&b"second"[..]));
    }

    #[tokio::test]
    async fn loopback_wait_for_rx_preserves_prefetched_single_rx_slot() {
        let inbound = PacketLink::new();
        let outbound = PacketLink::new();
        inbound.push(BytesMut::from(&b"first"[..]));
        inbound.push(BytesMut::from(&b"second"[..]));
        let mut device = LoopbackTunDevice::new(inbound, outbound);

        assert!(device.try_recv_rx().unwrap());
        device.wait_for_rx().await.unwrap();
        assert_eq!(
            device.rx_take().unwrap(),
            BytesMut::from(&b"first"[..]),
            "async wait must not overwrite the packet prefetched by the bounded-drain probe",
        );
        assert!(device.try_recv_rx().unwrap());
        assert_eq!(device.rx_take().unwrap(), BytesMut::from(&b"second"[..]));
    }

    #[tokio::test]
    async fn d16_integrated_heavy_flow_proves_capacity_ownership_and_clean_eof() {
        let report = run_d16_byte_owned_pressure_scenario()
            .await
            .expect("fixed D16 harness configuration must run");

        assert!(report.receiver_capacity_mbps >= 200.0, "{report:?}");
        assert_eq!(report.remote_bytes, report.actor_admitted_bytes);
        assert_eq!(report.actor_bypass_admitted_bytes, 0);
        assert_eq!(report.tun_drop_bytes, 0);
        assert_eq!(report.close_egress_bytes, 0);
        assert!(report.per_flow_high_water_bytes <= 512 * 1024);
        assert!(report.global_high_water_bytes <= D16_GLOBAL_BYTE_BUDGET_DEFAULT_BYTES);
        assert!(report.readiness_wakes > 0);
    }

    #[tokio::test]
    async fn d16_bidirectional_control_survives_suppressed_tun_wait_edge() {
        let report = run_d16_bidirectional_control_scenario().await;

        assert!(report.first_response_seen, "{report:?}");
        assert!(report.second_control_echoed, "{report:?}");
        assert!(report.round_trip_intact, "{report:?}");
        assert_eq!(report.tcp_opens, 1, "{report:?}");
    }

    #[tokio::test]
    async fn d16_remote_read_failure_reaches_local_tcp_client() {
        let gen_to_sut = PacketLink::new();
        let sut_to_gen = PacketLink::new();
        let (_downlink_tx, downlink_rx) = mpsc::channel::<Vec<u8>>(1);
        let trigger = Arc::new(FailAfterWriteTrigger::default());
        let written_bytes = Arc::new(AtomicU64::new(0));
        let upstream = Arc::new(OneShotFailingD16Upstream {
            relay: Mutex::new(Some(NativeTcpRelayStream {
                reader: Box::new(FailAfterWriteReader {
                    trigger: Arc::clone(&trigger),
                    failed: false,
                }),
                writer: Box::new(FailAfterWriteWriter {
                    trigger,
                    written_bytes: Arc::clone(&written_bytes),
                }),
            })),
        });
        let sut_device = LoopbackTunDevice::new(gen_to_sut.clone(), sut_to_gen.clone());
        let config = TunRuntimeConfig::h10d16_gate_a_for_test();
        let sut = tokio::spawn(run_event_loop(
            sut_device,
            upstream,
            downlink_rx,
            config,
            Arc::new(Metrics::new()),
            RecordingSink::new(Arc::new(Mutex::new(Recorded::default()))),
        ));

        let mut gen_device = GeneratorDevice::new(sut_to_gen, gen_to_sut);
        let mut gen_iface = {
            let cfg = SmolConfig::new(smoltcp::wire::HardwareAddress::Ip);
            let mut iface = Interface::new(cfg, &mut gen_device, SmolInstant::now());
            iface.update_ip_addrs(|addrs| {
                addrs
                    .push(IpCidr::new(IpAddress::Ipv4(GEN_IP), 24))
                    .unwrap();
            });
            iface.routes_mut().add_default_ipv4_route(GEN_IP).unwrap();
            iface
        };
        let mut sockets = SocketSet::new(vec![]);
        let rx = tcp::SocketBuffer::new(vec![0u8; 16 * 1024]);
        let tx = tcp::SocketBuffer::new(vec![0u8; 16 * 1024]);
        let mut socket = tcp::Socket::new(rx, tx);
        socket.set_ack_delay(None);
        socket
            .connect(
                gen_iface.context(),
                (IpAddress::Ipv4(TARGET_IP), TARGET_PORT_BASE),
                42_000,
            )
            .unwrap();
        let handle = sockets.add(socket);

        let control = [0xA5u8; 37];
        let mut sent = 0usize;
        let mut local_failure_observed = false;
        let started = Instant::now();
        while started.elapsed() < Duration::from_secs(2) {
            gen_iface.poll(SmolInstant::now(), &mut gen_device, &mut sockets);
            let socket = sockets.get_mut::<tcp::Socket>(handle);
            if sent < control.len()
                && socket.can_send()
                && let Ok(written) = socket.send_slice(&control[sent..])
            {
                sent = sent.saturating_add(written);
            }
            if sent == control.len() && (!socket.may_recv() || !socket.is_active()) {
                local_failure_observed = true;
                break;
            }
            tokio::time::sleep(Duration::from_micros(200)).await;
        }
        let socket = sockets.get::<tcp::Socket>(handle);
        let final_state = socket.state();
        let final_active = socket.is_active();
        let final_may_recv = socket.may_recv();
        sut.abort();
        let _ = sut.await;

        assert_eq!(
            sent,
            control.len(),
            "local client did not finish its control write"
        );
        assert_eq!(written_bytes.load(Ordering::Relaxed), control.len() as u64);
        assert!(
            local_failure_observed,
            "remote read failure did not reach local TCP client: state={final_state:?} active={final_active} may_recv={final_may_recv}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn d16_real_quinn_forward_tun_batch_path_exceeds_capacity_without_ring_drops() {
        let _capacity_guard = crate::test_support::local_capacity_test_guard().await;
        const PAYLOAD_BYTES: usize = 32 * 1024 * 1024;
        const WRITE_BYTES: usize = 64 * 1024;
        const TUN_MTU: usize = 1200;
        const RING_CAPACITY_PACKETS: usize = 500;
        const ACK: u8 = 0xAC;

        fn pattern_byte(offset: usize) -> u8 {
            (offset as u8).wrapping_mul(31).wrapping_add(7)
        }

        let (server_endpoint, client_endpoint, server_addr) =
            crate::tuic::d16_quinn_test_endpoints();
        let server_task = tokio::spawn(async move {
            let connection = server_endpoint.accept().await.unwrap().await.unwrap();
            let (mut send, mut recv) = connection.accept_bi().await.unwrap();
            let mut scratch = vec![0u8; WRITE_BYTES];
            let mut received = 0usize;
            let mut pattern_errors = 0usize;
            let mut first_byte_at = None;
            while received < PAYLOAD_BYTES {
                let read =
                    recv.read(&mut scratch).await.unwrap().unwrap_or_else(|| {
                        panic!("forward D16 stream closed after {received} bytes")
                    });
                first_byte_at.get_or_insert_with(Instant::now);
                for (index, byte) in scratch[..read].iter().copied().enumerate() {
                    if byte != pattern_byte(received + index) {
                        pattern_errors = pattern_errors.saturating_add(1);
                    }
                }
                received = received.saturating_add(read);
            }
            let payload_elapsed = first_byte_at.unwrap().elapsed();
            send.write_all(&[ACK]).await.unwrap();
            send.finish().unwrap();
            let clean_eof = recv.read(&mut scratch).await.unwrap().is_none();
            (received, pattern_errors, clean_eof, payload_elapsed)
        });

        let connection = client_endpoint
            .connect(server_addr, "example.com")
            .unwrap()
            .await
            .unwrap();
        let (send, recv) = connection.open_bi().await.unwrap();
        let upstream = Arc::new(OneShotRealQuinnD16Upstream {
            relay: Mutex::new(Some(NativeTcpRelayStream {
                reader: crate::tuic::d16_native_ordered_reader_for_test(recv, connection.clone()),
                writer: Box::new(send),
            })),
        });

        let gen_to_sut = PacketLink::bounded(RING_CAPACITY_PACKETS);
        let sut_to_gen = PacketLink::new();
        let (_downlink_tx, downlink_rx) = mpsc::channel::<Vec<u8>>(1);
        let sut_device = LoopbackTunDevice::new(gen_to_sut.clone(), sut_to_gen.clone())
            .with_mtu(TUN_MTU)
            .with_ingress_pump(RING_CAPACITY_PACKETS);
        let config = TunRuntimeConfig::h10d16_gate_a_for_test();
        let recorded = Arc::new(Mutex::new(Recorded::default()));
        let sut = tokio::spawn(run_event_loop(
            sut_device,
            upstream,
            downlink_rx,
            config,
            Arc::new(Metrics::new()),
            RecordingSink::new(recorded.clone()),
        ));

        let mut gen_device =
            GeneratorDevice::new(sut_to_gen.clone(), gen_to_sut.clone()).with_mtu(TUN_MTU);
        let mut gen_iface = {
            let cfg = SmolConfig::new(smoltcp::wire::HardwareAddress::Ip);
            let mut iface = Interface::new(cfg, &mut gen_device, SmolInstant::now());
            iface.update_ip_addrs(|addrs| {
                addrs
                    .push(IpCidr::new(IpAddress::Ipv4(GEN_IP), 24))
                    .unwrap();
            });
            iface.routes_mut().add_default_ipv4_route(GEN_IP).unwrap();
            iface
        };
        let mut sockets = SocketSet::new(vec![]);
        let rx = tcp::SocketBuffer::new(vec![0u8; 1024 * 1024]);
        let tx = tcp::SocketBuffer::new(vec![0u8; 1024 * 1024]);
        let mut socket = tcp::Socket::new(rx, tx);
        socket.set_ack_delay(None);
        socket
            .connect(
                gen_iface.context(),
                (IpAddress::Ipv4(TARGET_IP), TARGET_PORT_BASE),
                42_000,
            )
            .unwrap();
        let handle = sockets.add(socket);

        let mut sent = 0usize;
        let mut ack_received = false;
        let mut local_close_started = false;
        let mut send_chunk = vec![0u8; WRITE_BYTES];
        let mut recv_scratch = [0u8; 16];
        let completion = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                gen_device.begin_poll();
                gen_iface.poll(SmolInstant::now(), &mut gen_device, &mut sockets);
                let socket = sockets.get_mut::<tcp::Socket>(handle);
                if sent < PAYLOAD_BYTES && socket.can_send() {
                    let len = (PAYLOAD_BYTES - sent).min(send_chunk.len());
                    for (index, byte) in send_chunk[..len].iter_mut().enumerate() {
                        *byte = pattern_byte(sent + index);
                    }
                    if let Ok(written) = socket.send_slice(&send_chunk[..len]) {
                        sent = sent.saturating_add(written);
                    }
                }
                while socket.can_recv() {
                    match socket.recv_slice(&mut recv_scratch) {
                        Ok(0) | Err(_) => break,
                        Ok(read) => {
                            ack_received |= recv_scratch[..read].contains(&ACK);
                        }
                    }
                }
                if ack_received && !local_close_started {
                    socket.close();
                    local_close_started = true;
                }
                if ack_received && server_task.is_finished() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        if completion.is_err() {
            let socket = sockets.get::<tcp::Socket>(handle);
            let snapshot = recorded.lock().unwrap().clone();
            panic!(
                "forward D16 batch path timed out: sent={sent} ack={ack_received} state={:?} can_send={} can_recv={} may_recv={} ring_drops={} ring_high={} recorded={snapshot:?}",
                socket.state(),
                socket.can_send(),
                socket.can_recv(),
                socket.may_recv(),
                gen_to_sut.dropped_packets(),
                gen_to_sut.high_water_packets(),
            );
        }

        let (received, pattern_errors, clean_eof, payload_elapsed) =
            tokio::time::timeout(Duration::from_secs(2), server_task)
                .await
                .expect("forward D16 server must observe local EOF")
                .unwrap();
        sut.abort();
        let _ = sut.await;
        let snapshot = recorded.lock().unwrap().clone();
        let receiver_mbps = received as f64 * 8.0 / payload_elapsed.as_secs_f64() / 1_000_000.0;
        eprintln!(
            "d16_forward_tun_batch receiver_mbps={receiver_mbps:.3} elapsed={payload_elapsed:?} ring_high={} pump_high={}/{} pump_full_waits={} uplink_recv_queue_max={} tcp_packets={} tcp_batches={} batch_iface_polls={} batch_flushes={} avoided_dirty_relay_passes={} avoided_iface_polls={}",
            gen_to_sut.high_water_packets(),
            snapshot.tun_rx_pump_queue_high_water,
            snapshot.tun_rx_pump_capacity_packets,
            snapshot.tun_rx_pump_full_waits,
            snapshot.uplink_recv_queue_max,
            snapshot.tun_rx_tcp_packets,
            snapshot.tun_rx_tcp_batches,
            snapshot.tun_rx_batch_iface_polls,
            snapshot.tun_rx_batch_flushes,
            snapshot.tun_rx_avoided_dirty_relay_passes,
            snapshot.tun_rx_avoided_iface_polls,
        );

        assert_eq!(sent, PAYLOAD_BYTES, "{snapshot:?}");
        assert_eq!(received, PAYLOAD_BYTES, "{snapshot:?}");
        assert_eq!(pattern_errors, 0, "{snapshot:?}");
        assert!(ack_received, "{snapshot:?}");
        assert!(clean_eof, "{snapshot:?}");
        assert_eq!(gen_to_sut.dropped_packets(), 0, "{snapshot:?}");
        assert!(gen_to_sut.high_water_packets() > 1, "{snapshot:?}");
        assert!(
            gen_to_sut.high_water_packets() <= RING_CAPACITY_PACKETS as u64,
            "{snapshot:?}"
        );
        assert!(snapshot.tun_rx_tcp_packets > 0, "{snapshot:?}");
        assert_eq!(
            snapshot.tun_rx_tcp_batches, snapshot.tun_rx_batch_dirty_relay_passes,
            "{snapshot:?}"
        );
        assert_eq!(
            snapshot.tun_rx_tcp_batches, snapshot.tun_rx_batch_iface_polls,
            "{snapshot:?}"
        );
        assert_eq!(
            snapshot.tun_rx_tcp_batches, snapshot.tun_rx_batch_flushes,
            "{snapshot:?}"
        );
        assert!(
            snapshot.tun_rx_tcp_batch_packets_high_water > 1,
            "{snapshot:?}"
        );
        assert!(
            snapshot.tun_rx_avoided_dirty_relay_passes > 0,
            "{snapshot:?}"
        );
        assert!(snapshot.tun_rx_avoided_iface_polls > 0, "{snapshot:?}");
        assert!(snapshot.tun_rx_pump_packets > 0, "{snapshot:?}");
        assert_eq!(
            snapshot.tun_rx_pump_packets, snapshot.tun_rx_tcp_packets,
            "all pumped packets must leave the FIFO through the classified TCP path: {snapshot:?}"
        );
        assert_eq!(
            snapshot.tun_rx_pump_capacity_packets, RING_CAPACITY_PACKETS,
            "{snapshot:?}"
        );
        assert!(
            snapshot.tun_rx_pump_queue_high_water < RING_CAPACITY_PACKETS as u64,
            "{snapshot:?}"
        );
        assert_eq!(snapshot.tun_rx_pump_full_waits, 0, "{snapshot:?}");
        assert_eq!(snapshot.tun_rx_pump_read_errors, 0, "{snapshot:?}");
        assert!(
            snapshot.uplink_recv_queue_max <= 368_640,
            "local TCP admission must stay within EndpointWindowV1's 10ms bound: {snapshot:?}"
        );
        assert!(snapshot.d16_eof_observed, "{snapshot:?}");
        assert_eq!(snapshot.d16_eof_tail_bytes, 0, "{snapshot:?}");
        assert_eq!(snapshot.d16_owned_queue_bytes, 0, "{snapshot:?}");
        assert_eq!(snapshot.d16_pending_bytes, 0, "{snapshot:?}");
        assert_eq!(snapshot.d16_inflight_bytes, 0, "{snapshot:?}");
        assert_eq!(snapshot.terminal_drop_bytes, 0, "{snapshot:?}");
        assert_eq!(snapshot.terminal_late_payload_bytes, 0, "{snapshot:?}");
        assert!(
            receiver_mbps > 170.0,
            "real Quinn forward D16/TUN batch path must exceed the fixed gate: elapsed={payload_elapsed:?} rate={receiver_mbps:.3}M snapshot={snapshot:?}"
        );

        client_endpoint.close(0u32.into(), b"test complete");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn d16_real_quinn_full_tun_path_sustains_capacity_and_clean_eof() {
        let _capacity_guard = crate::test_support::local_capacity_test_guard().await;
        const PAYLOAD_BYTES: usize = 32 * 1024 * 1024;
        const WRITE_BYTES: usize = 64 * 1024;
        const RING_CAPACITY_PACKETS: usize = 64;

        let (server_endpoint, client_endpoint, server_addr) =
            crate::tuic::d16_quinn_test_endpoints();

        let (server_done_tx, server_done_rx) = tokio::sync::oneshot::channel();
        let server_task = tokio::spawn(async move {
            let connection = server_endpoint.accept().await.unwrap().await.unwrap();
            let (mut send, mut recv) = connection.accept_bi().await.unwrap();
            let mut bootstrap = [0u8; 1];
            recv.read_exact(&mut bootstrap).await.unwrap();
            assert_eq!(bootstrap, [0xA5]);
            let payload = vec![0x5a; WRITE_BYTES];
            for _ in 0..PAYLOAD_BYTES / WRITE_BYTES {
                send.write_all(&payload).await.unwrap();
            }
            send.finish().unwrap();
            let _ = server_done_rx.await;
        });

        let connection = client_endpoint
            .connect(server_addr, "example.com")
            .unwrap()
            .await
            .unwrap();
        let (send, recv) = connection.open_bi().await.unwrap();
        let upstream = Arc::new(OneShotRealQuinnD16Upstream {
            relay: Mutex::new(Some(NativeTcpRelayStream {
                reader: crate::tuic::d16_native_ordered_reader_for_test(recv, connection.clone()),
                writer: Box::new(send),
            })),
        });

        let gen_to_sut = PacketLink::bounded(RING_CAPACITY_PACKETS);
        let sut_to_gen = PacketLink::new();
        let (_downlink_tx, downlink_rx) = mpsc::channel::<Vec<u8>>(1);
        let max_tcp_payload_packets_per_flush = Arc::new(AtomicU64::new(0));
        let sut_device = LoopbackTunDevice::new(gen_to_sut.clone(), sut_to_gen.clone())
            .with_tcp_payload_flush_counter(Arc::clone(&max_tcp_payload_packets_per_flush));
        let config = TunRuntimeConfig::h10d16_gate_a_for_test();
        let recorded = Arc::new(Mutex::new(Recorded::default()));
        let sut = tokio::spawn(run_event_loop(
            sut_device,
            upstream,
            downlink_rx,
            config,
            Arc::new(Metrics::new()),
            RecordingSink::new(recorded.clone()),
        ));

        let mut gen_device = GeneratorDevice::with_ingress_packets_per_poll(
            sut_to_gen.clone(),
            gen_to_sut.clone(),
            2,
        );
        let mut gen_iface = {
            let cfg = SmolConfig::new(smoltcp::wire::HardwareAddress::Ip);
            let mut iface = Interface::new(cfg, &mut gen_device, SmolInstant::now());
            iface.update_ip_addrs(|addrs| {
                addrs
                    .push(IpCidr::new(IpAddress::Ipv4(GEN_IP), 24))
                    .unwrap();
            });
            iface.routes_mut().add_default_ipv4_route(GEN_IP).unwrap();
            iface
        };
        let mut sockets = SocketSet::new(vec![]);
        let rx = tcp::SocketBuffer::new(vec![0u8; 1024 * 1024]);
        let tx = tcp::SocketBuffer::new(vec![0u8; 64 * 1024]);
        let mut socket = tcp::Socket::new(rx, tx);
        socket.set_ack_delay(None);
        socket
            .connect(
                gen_iface.context(),
                (IpAddress::Ipv4(TARGET_IP), TARGET_PORT_BASE),
                42_000,
            )
            .unwrap();
        let handle = sockets.add(socket);

        let started = Instant::now();
        let mut bootstrap_sent = false;
        let mut received = 0usize;
        let mut remote_eof_seen = false;
        let mut scratch = vec![0u8; 64 * 1024];
        let completion = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                gen_device.begin_poll();
                gen_iface.poll(SmolInstant::now(), &mut gen_device, &mut sockets);
                let socket = sockets.get_mut::<tcp::Socket>(handle);
                if !bootstrap_sent && socket.can_send() {
                    bootstrap_sent = socket.send_slice(&[0xA5]).unwrap() == 1;
                }
                while socket.can_recv() {
                    match socket.recv_slice(&mut scratch) {
                        Ok(0) | Err(_) => break,
                        Ok(read) => received = received.saturating_add(read),
                    }
                }
                remote_eof_seen |= received >= PAYLOAD_BYTES && !socket.may_recv();
                if remote_eof_seen && recorded.lock().unwrap().d16_eof_observed {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        if completion.is_err() {
            let socket = sockets.get::<tcp::Socket>(handle);
            let socket_state = socket.state();
            let socket_can_recv = socket.can_recv();
            let socket_may_recv = socket.may_recv();
            let socket_can_send = socket.can_send();
            let recorded_snapshot = recorded.lock().unwrap().clone();
            let gen_to_sut_queued = gen_to_sut.queue.lock().unwrap().len();
            let sut_to_gen_queued = sut_to_gen.queue.lock().unwrap().len();
            let quinn_stats = connection.stats();
            let quinn_close_reason = connection.close_reason();
            let sut_finished = sut.is_finished();
            let server_finished = server_task.is_finished();

            if sockets.get::<tcp::Socket>(handle).is_active() {
                sockets.get_mut::<tcp::Socket>(handle).close();
            }
            sut.abort();
            let _ = sut.await;
            let _ = server_done_tx.send(());
            server_task.abort();
            let _ = server_task.await;
            client_endpoint.close(0u32.into(), b"test timed out");

            panic!(
                "real Quinn plus the complete D16 TCP/TUN path timed out: elapsed={:?} received={received}/{PAYLOAD_BYTES} bootstrap_sent={bootstrap_sent} remote_eof_seen={remote_eof_seen} socket_state={socket_state:?} socket_can_recv={socket_can_recv} socket_may_recv={socket_may_recv} socket_can_send={socket_can_send} gen_to_sut_queued={gen_to_sut_queued} gen_to_sut_dropped={} gen_to_sut_high_water={} sut_to_gen_queued={sut_to_gen_queued} sut_to_gen_high_water={} sut_finished={sut_finished} server_finished={server_finished} quinn_close_reason={quinn_close_reason:?} recorded={recorded_snapshot:?} quinn_stats={quinn_stats:?}",
                started.elapsed(),
                gen_to_sut.dropped_packets(),
                gen_to_sut.high_water_packets(),
                sut_to_gen.high_water_packets(),
            );
        }
        let elapsed = started.elapsed();
        let receiver_mbps = received as f64 * 8.0 / elapsed.as_secs_f64() / 1_000_000.0;

        if sockets.get::<tcp::Socket>(handle).is_active() {
            sockets.get_mut::<tcp::Socket>(handle).close();
        }
        sut.abort();
        let _ = sut.await;
        let (eof_observed, eof_tail_bytes, actor_bypass_admitted_bytes) = {
            let recorded = recorded.lock().unwrap();
            (
                recorded.d16_eof_observed,
                recorded.d16_eof_tail_bytes,
                recorded.actor_bypass_admitted_bytes,
            )
        };
        assert_eq!(received, PAYLOAD_BYTES);
        assert!(remote_eof_seen);
        assert!(eof_observed);
        assert_eq!(eof_tail_bytes, 0);
        assert_eq!(actor_bypass_admitted_bytes, 0);
        assert_eq!(gen_to_sut.dropped_packets(), 0);
        assert!(
            max_tcp_payload_packets_per_flush.load(Ordering::Relaxed)
                <= crate::tcp_egress::D16_ACTOR_PAYLOAD_PACKET_BUDGET as u64
        );
        assert!(
            receiver_mbps >= 170.0,
            "real Quinn full D16 TCP/TUN path must retain Gate B capacity: elapsed={elapsed:?} rate={receiver_mbps:.1}M"
        );

        let _ = server_done_tx.send(());
        server_task.await.unwrap();
        client_endpoint.close(0u32.into(), b"test complete");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn d16_reverse_flow_does_not_overrun_bounded_tun_rx_ring() {
        let _capacity_guard = crate::test_support::local_capacity_test_guard().await;
        const RING_CAPACITY_PACKETS: usize = 64;
        let report = run_d16_tun_rx_starvation_scenario(
            64 * 1024 * 1024,
            RING_CAPACITY_PACKETS,
            Duration::from_secs(15),
        )
        .await;

        assert_eq!(report.bootstrap_sent_bytes, 1, "{report:?}");
        assert_eq!(report.tcp_opens, 1, "{report:?}");
        assert_eq!(report.modeled_tx_dropped, 0, "{report:?}");
        assert!(report.tun_rx_budget_exhausted <= 8, "{report:?}");
        assert!(report.backlog_pause_edges <= 4, "{report:?}");
        assert_eq!(
            report.backlog_pause_edges, report.backlog_resume_edges,
            "{report:?}"
        );
        assert!(!report.final_backlog_active, "{report:?}");
        assert_eq!(report.actor_bypass_admitted_bytes, 0, "{report:?}");
        assert!(report.remote_eof_seen, "{report:?}");
        assert!(report.eof_after_owned_queue_drain, "{report:?}");
        assert_eq!(report.close_egress_bytes, 0, "{report:?}");
        assert_eq!(report.terminal_drop_bytes, 0, "{report:?}");
        assert_eq!(report.terminal_late_payload_bytes, 0, "{report:?}");
        assert_eq!(report.received_bytes, report.payload_bytes, "{report:?}");
        assert!(report.ring_high_water_packets > 0, "{report:?}");
        assert!(
            report.max_tcp_payload_packets_per_flush
                <= crate::tcp_egress::D16_ACTOR_PAYLOAD_PACKET_BUDGET as u64,
            "{report:?}"
        );
        assert!(report.receiver_mbps >= 170.0, "{report:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn d16_tun_rx_ack_reenters_actor_without_timer_tick() {
        const PAYLOAD_BYTES: usize = 256 * 1024;
        const RING_CAPACITY_PACKETS: usize = 64;
        let report = run_d16_tun_rx_starvation_scenario_with_limit(
            PAYLOAD_BYTES,
            RING_CAPACITY_PACKETS,
            D16TunRxScenarioLimit::SchedulerYields(50_000),
        )
        .await;

        assert_eq!(report.bootstrap_sent_bytes, 1, "{report:?}");
        assert_eq!(report.tcp_opens, 1, "{report:?}");
        assert_eq!(report.modeled_tx_dropped, 0, "{report:?}");
        assert!(report.tun_rx_actor_wakes > 0, "{report:?}");
        assert_eq!(
            report.received_bytes, report.payload_bytes,
            "ACK arrival must continue D16 actor admission without advancing the 5ms timer: {report:?}"
        );
    }

    #[tokio::test]
    async fn d16_pressure_stops_read_and_admission_while_other_flow_progresses() {
        let budget = D16GlobalByteBudget::new(512 * 1024).unwrap();
        let mut paused = D16HarnessFlow::new(budget.clone()).unwrap();
        let mut active = D16HarnessFlow::new(budget.clone()).unwrap();

        paused
            .enqueue_remote_chunk(D16_INTEGRATED_STEP_BYTES)
            .await
            .unwrap();
        assert_eq!(
            paused
                .cycle(false, false, D16_INTEGRATED_STEP_BYTES)
                .admitted_bytes,
            D16_INTEGRATED_STEP_BYTES
        );
        paused
            .enqueue_remote_chunk(D16_INTEGRATED_STEP_BYTES)
            .await
            .unwrap();
        let pressure = paused.cycle(true, true, D16_INTEGRATED_STEP_BYTES);
        assert_eq!(pressure.drained_bytes, D16_INTEGRATED_STEP_BYTES);
        assert_eq!(pressure.admitted_bytes, 0);
        assert!(!pressure.allow_read);
        assert!(!pressure.allow_admission);
        assert_eq!(pressure.phase, EgressPhase::DrainOnly);
        assert!(!paused.permissions().allow_read);

        active
            .enqueue_remote_chunk(D16_INTEGRATED_STEP_BYTES)
            .await
            .unwrap();
        let active_cycle = active.cycle(false, false, D16_INTEGRATED_STEP_BYTES);
        assert_eq!(active_cycle.admitted_bytes, D16_INTEGRATED_STEP_BYTES);
        assert_eq!(active_cycle.phase, EgressPhase::Running);
        assert!(budget.snapshot().total_bytes() <= 512 * 1024);

        let clean = paused.cycle(false, false, D16_INTEGRATED_STEP_BYTES);
        assert_eq!(clean.admitted_bytes, 0);
        assert_eq!(clean.phase, EgressPhase::Recovery { clean_cycles: 0 });
        for _ in 0..4 {
            paused.cycle(false, false, D16_INTEGRATED_STEP_BYTES);
        }
        assert_eq!(paused.snapshot().phase, EgressPhase::Running);
    }

    #[tokio::test]
    async fn d16_global_saturation_is_bounded_and_waiters_do_not_busy_spin() {
        const FLOW_BYTES: usize = 128 * 1024;
        let budget = D16GlobalByteBudget::new(4 * FLOW_BYTES).unwrap();
        let queues: Vec<_> = (0..8)
            .map(|_| {
                AsyncLeasedByteFlowQueue::new_with_global_budget(
                    FLOW_BYTES,
                    DownstreamPermitReleaseMode::OnEgressDrain,
                    budget.clone(),
                )
                .unwrap()
            })
            .collect();
        for queue in &queues[..4] {
            let reservation = queue.reserve_read_up_to(FLOW_BYTES).await.unwrap();
            reservation
                .commit(bytes::Bytes::from(vec![1; FLOW_BYTES]))
                .unwrap();
        }
        assert_eq!(budget.snapshot().total_bytes(), 4 * FLOW_BYTES);

        let waiters: Vec<_> = queues[4..]
            .iter()
            .cloned()
            .map(|queue| tokio::spawn(async move { queue.reserve_read_up_to(FLOW_BYTES).await }))
            .collect();
        tokio::task::yield_now().await;
        assert!(waiters.iter().all(|waiter| !waiter.is_finished()));

        for queue in &queues[..4] {
            let mut leased = queue.recv_up_to(FLOW_BYTES).await.unwrap();
            assert_eq!(leased.permit_mut().release(usize::MAX), FLOW_BYTES);
        }
        let mut reservations = Vec::new();
        for waiter in waiters {
            reservations.push(
                tokio::time::timeout(Duration::from_millis(200), waiter)
                    .await
                    .expect("released global bytes must wake every flow")
                    .unwrap()
                    .unwrap(),
            );
        }
        assert!(
            reservations
                .iter()
                .all(|reservation| reservation.max_len() == FLOW_BYTES)
        );
        drop(reservations);
        assert_eq!(budget.snapshot().total_bytes(), 0);
        assert!(budget.snapshot().high_water_bytes <= 4 * FLOW_BYTES);
    }

    mod knife16_resumable_adapter {
        use super::*;
        use crate::client_tun::{TCP_SOCKET_BUFFER_SIZE, run_owned_event_loop};
        use crate::owned_upstream::{
            DriverInput, FlowPortConfig, FlowPortProbe, OpenedOwnedTcp, OwnedUpstream,
            ResumableTcpDriver, ResumableTcpPortFactory, TcpOwnershipMode, UdpDownlinkSource,
            UdpUplink,
        };
        use crate::resumable::{
            AttachAlpn, AttachAuthority, AttachCredentials, AttachNonce, AttachPolicy,
            AttachRequest, AttachTransportBinding, ByteOffset, CommittedLeg, DevicePrincipal,
            DeviceSecret, Direction, FeatureOffer, FlowFinishReason, Frame, LegGeneration,
            LocalFlow, MAX_DATA_PAYLOAD_BYTES, OpenResultCode, OwnerIdentity, ReceiveBudgetLimits,
            Record, ReplayAcknowledged, ReplayBudgetLimits, ReplayStored, ResumeSecret,
            SESSION_PROTOCOL_VERSION, SessionConfig, SessionEffect, SessionEvent, SessionId,
            SessionModel, SessionRole, TcpWindowLimits, TlsExporterBinding, VersionRange,
        };
        const PORT_BYTES: usize = TCP_SOCKET_BUFFER_SIZE;
        const SUFFIX_BYTES: usize = 97;

        #[derive(Default)]
        struct ResumableTrace {
            probe: Mutex<Option<FlowPortProbe>>,
            data_lengths: Mutex<Vec<usize>>,
            sink_offers: Mutex<Vec<usize>>,
            sink_acceptances: Mutex<Vec<usize>>,
            local_close_seen: AtomicBool,
            local_close_events: AtomicU64,
            sink_half_closed_seen: AtomicBool,
            sink_half_closed_events: AtomicU64,
            graceful_terminal_seen: AtomicBool,
            graceful_terminal_events: AtomicU64,
            errors: Mutex<Vec<String>>,
        }

        impl ResumableTrace {
            fn fail(&self, error: impl Into<String>) {
                self.errors.lock().unwrap().push(error.into());
            }

            fn probe(&self) -> Option<FlowPortProbe> {
                self.probe.lock().unwrap().clone()
            }
        }

        struct FakeResumableUpstream {
            open_gate: OpenStallControl,
            driver_gate: OpenStallControl,
            first_ack_gate: OpenStallControl,
            trace: Arc<ResumableTrace>,
            opens: AtomicU64,
            port_factory: ResumableTcpPortFactory,
            tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
        }

        impl FakeResumableUpstream {
            fn new(
                open_gate: OpenStallControl,
                driver_gate: OpenStallControl,
                first_ack_gate: OpenStallControl,
                trace: Arc<ResumableTrace>,
            ) -> Self {
                Self {
                    open_gate,
                    driver_gate,
                    first_ack_gate,
                    trace,
                    opens: AtomicU64::new(0),
                    port_factory: ResumableTcpPortFactory::new_unbound_for_test(
                        FlowPortConfig::new(PORT_BYTES, 1, 8, 4).unwrap(),
                        PORT_BYTES,
                        8,
                        8,
                    )
                    .unwrap(),
                    tasks: Mutex::new(Vec::new()),
                }
            }

            fn stop_tasks(&self) {
                for task in self.tasks.lock().unwrap().drain(..) {
                    task.abort();
                }
            }
        }

        #[async_trait::async_trait]
        impl OwnedUpstream for FakeResumableUpstream {
            async fn open_tcp_flow(
                &self,
                target: &TargetAddr,
            ) -> Result<OpenedOwnedTcp, ClientError> {
                self.opens.fetch_add(1, Ordering::Relaxed);
                self.open_gate.mark_waiting();
                self.open_gate.wait_released().await;

                let (model, leg, flow) = open_client_model(target.clone()).map_err(|error| {
                    ClientError::Io(std::io::Error::other(format!(
                        "fake resumable open failed: {error}"
                    )))
                })?;
                let (port, driver) = self.port_factory.open_flow(flow).map_err(|error| {
                    ClientError::Io(std::io::Error::other(format!(
                        "fake resumable port admission failed: {error}"
                    )))
                })?;
                *self.trace.probe.lock().unwrap() = Some(port.probe());

                let trace = Arc::clone(&self.trace);
                let driver_gate = self.driver_gate.clone();
                let first_ack_gate = self.first_ack_gate.clone();
                let task = tokio::spawn(async move {
                    driver_gate.mark_waiting();
                    driver_gate.wait_released().await;
                    if let Err(error) =
                        run_fake_resumable_driver(driver, model, leg, first_ack_gate, &trace).await
                    {
                        trace.fail(error);
                    }
                });
                self.tasks.lock().unwrap().push(task);
                Ok(OpenedOwnedTcp::Resumable(port))
            }

            async fn send_udp(&self, _uplink: UdpUplink<'_>) {}

            fn tcp_ownership_mode(&self) -> TcpOwnershipMode {
                TcpOwnershipMode::Resumable
            }

            fn open_is_cheap(&self) -> bool {
                false
            }
        }

        fn attach_credentials() -> AttachCredentials {
            AttachCredentials::new(
                DeviceSecret::new([0x31; 32]).unwrap(),
                ResumeSecret::new([0x53; 32]).unwrap(),
            )
            .unwrap()
        }

        fn committed_leg() -> CommittedLeg {
            let session_id = SessionId::new([0x16; 16]).unwrap();
            let owner = OwnerIdentity::new([0x21; 32]).unwrap();
            let principal = DevicePrincipal::new([0x34; 16]).unwrap();
            let alpn = AttachAlpn::new(b"mini-vpn-owned/1").unwrap();
            let binding = AttachTransportBinding::new(
                owner,
                alpn,
                TlsExporterBinding::new([0x55; 32]).unwrap(),
                principal,
            );
            let policy =
                AttachPolicy::new(owner, alpn, principal, SESSION_PROTOCOL_VERSION, 0b1111)
                    .unwrap();
            let authority = AttachAuthority::new(
                session_id,
                LegGeneration::new(1).unwrap(),
                attach_credentials(),
                policy,
            );
            let request = AttachRequest::new(
                session_id,
                LegGeneration::new(2).unwrap(),
                AttachNonce::new([0x62; 16]).unwrap(),
                VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION).unwrap(),
                FeatureOffer::new(0b0111, 0b0001).unwrap(),
            );
            let proof = attach_credentials().prove(&request, &binding).unwrap();
            authority
                .verify_and_commit(&request, &binding, &proof)
                .unwrap()
        }

        fn session_config() -> SessionConfig {
            SessionConfig::new(
                2,
                8,
                TcpWindowLimits::new(PORT_BYTES, 8).unwrap(),
                TcpWindowLimits::new(PORT_BYTES, 8).unwrap(),
                ReplayBudgetLimits::new(PORT_BYTES, 8).unwrap(),
                ReplayBudgetLimits::new(PORT_BYTES, 8).unwrap(),
                ReceiveBudgetLimits::new(PORT_BYTES * 2, 16).unwrap(),
                MAX_DATA_PAYLOAD_BYTES,
            )
            .unwrap()
        }

        fn open_client_model(
            target: TargetAddr,
        ) -> Result<(SessionModel, CommittedLeg, LocalFlow), String> {
            let leg = committed_leg();
            let mut model = SessionModel::new(SessionRole::Client, session_config(), leg);
            let effects = model
                .reduce(SessionEvent::LocalOpen { leg, target })
                .map_err(|error| error.to_string())?;
            let flow = effects
                .into_iter()
                .find_map(|effect| match effect {
                    SessionEffect::LocalFlowOpened { flow } => Some(flow),
                    _ => None,
                })
                .ok_or_else(|| "LocalOpen emitted no LocalFlowOpened authority".to_string())?;
            let frame = Frame::try_new(
                leg.generation(),
                Record::OpenResult {
                    flow_id: flow.flow_id(),
                    result: OpenResultCode::Opened,
                },
            )
            .map_err(|error| error.to_string())?;
            model
                .reduce(SessionEvent::PeerFrame { leg, frame })
                .map_err(|error| error.to_string())?;
            Ok((model, leg, flow))
        }

        fn take_replay_stored(effects: &mut Vec<SessionEffect>) -> Result<ReplayStored, String> {
            let index = effects
                .iter()
                .position(|effect| matches!(effect, SessionEffect::ReplayStored { .. }))
                .ok_or_else(|| "LocalData emitted no ReplayStored authority".to_string())?;
            match effects.remove(index) {
                SessionEffect::ReplayStored { receipt } => Ok(receipt),
                _ => unreachable!("located exact ReplayStored variant"),
            }
        }

        fn take_replay_acknowledged(
            effects: &mut Vec<SessionEffect>,
        ) -> Result<ReplayAcknowledged, String> {
            let index = effects
                .iter()
                .position(|effect| matches!(effect, SessionEffect::ReplayAcknowledged { .. }))
                .ok_or_else(|| {
                    "accepted ACK emitted no ReplayAcknowledged authority".to_string()
                })?;
            match effects.remove(index) {
                SessionEffect::ReplayAcknowledged { receipt } => Ok(receipt),
                _ => unreachable!("located exact ReplayAcknowledged variant"),
            }
        }

        fn peer_frame(leg: CommittedLeg, record: Record) -> Result<SessionEvent, String> {
            let frame = Frame::try_new(leg.generation(), record)
                .map_err(|error| format!("invalid fake peer frame: {error}"))?;
            Ok(SessionEvent::PeerFrame { leg, frame })
        }

        fn dispatch_effects(
            driver: &mut ResumableTcpDriver,
            effects: Vec<SessionEffect>,
            trace: &ResumableTrace,
        ) -> Result<(), String> {
            for effect in effects {
                match effect {
                    SessionEffect::OfferToSink { offer, segments } => {
                        trace.sink_offers.lock().unwrap().push(offer.len());
                        driver
                            .try_deliver_sink(offer, segments)
                            .map_err(|error| format!("sink delivery failed: {error}"))?;
                    }
                    SessionEffect::HalfCloseSink { completion } => driver
                        .try_deliver_half_close(completion)
                        .map_err(|error| format!("half-close delivery failed: {error}"))?,
                    SessionEffect::FlowFinished {
                        reason: FlowFinishReason::Graceful,
                        ..
                    } => {
                        driver
                            .try_deliver_terminal_effect(&effect)
                            .map_err(|error| format!("terminal delivery failed: {error}"))?;
                        trace.graceful_terminal_seen.store(true, Ordering::Release);
                        trace
                            .graceful_terminal_events
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    SessionEffect::FlowFinished { .. } => {
                        driver
                            .try_deliver_terminal_effect(&effect)
                            .map_err(|error| format!("terminal delivery failed: {error}"))?;
                    }
                    _ => {}
                }
            }
            Ok(())
        }

        async fn handle_driver_data(
            driver: &mut ResumableTcpDriver,
            model: &mut SessionModel,
            leg: CommittedLeg,
            first_ack_gate: &OpenStallControl,
            trace: &ResumableTrace,
            data: crate::owned_upstream::UplinkData,
            peer_offset: &mut ByteOffset,
        ) -> Result<(), String> {
            let payload = Bytes::copy_from_slice(data.payload());
            let data_len = data.len();
            let (event, mut ownership) = data.into_event_and_ownership();
            let mut effects = model.reduce(event).map_err(|error| error.to_string())?;
            let stored = take_replay_stored(&mut effects)?;
            let mut replay = ownership
                .bind_replay(stored)
                .map_err(|error| format!("replay bind failed: {error}"))?;
            dispatch_effects(driver, effects, trace)?;

            let first = trace.data_lengths.lock().unwrap().is_empty();
            trace.data_lengths.lock().unwrap().push(data_len);
            if first {
                first_ack_gate.mark_waiting();
                first_ack_gate.wait_released().await;
            }

            let mut ack_effects = model
                .reduce(peer_frame(
                    leg,
                    Record::Ack {
                        flow_id: replay.flow_id(),
                        direction: replay.direction(),
                        next_accepted: replay.end(),
                        final_accepted: false,
                    },
                )?)
                .map_err(|error| error.to_string())?;
            let acknowledged = take_replay_acknowledged(&mut ack_effects)?;
            if !replay
                .release_after_reducer_ack(&acknowledged)
                .map_err(|error| format!("replay release failed: {error}"))?
            {
                return Err("complete reducer ACK retained the exact replay permit".into());
            }
            dispatch_effects(driver, ack_effects, trace)?;

            let offset = *peer_offset;
            *peer_offset = peer_offset
                .checked_advance(payload.len())
                .map_err(|error| error.to_string())?;
            let echo_effects = model
                .reduce(peer_frame(
                    leg,
                    Record::Data {
                        flow_id: replay.flow_id(),
                        direction: Direction::TargetToClient,
                        offset,
                        payload,
                    },
                )?)
                .map_err(|error| error.to_string())?;
            dispatch_effects(driver, echo_effects, trace)
        }

        fn handle_local_close(
            driver: &mut ResumableTcpDriver,
            model: &mut SessionModel,
            leg: CommittedLeg,
            flow: LocalFlow,
            peer_offset: ByteOffset,
            trace: &ResumableTrace,
        ) -> Result<(), String> {
            trace.local_close_seen.store(true, Ordering::Release);
            trace.local_close_events.fetch_add(1, Ordering::Relaxed);
            let effects = model
                .reduce(SessionEvent::LocalClose { flow })
                .map_err(|error| error.to_string())?;
            dispatch_effects(driver, effects, trace)?;

            let ack_effects = model
                .reduce(peer_frame(
                    leg,
                    Record::Ack {
                        flow_id: flow.flow_id(),
                        direction: Direction::ClientToTarget,
                        next_accepted: ByteOffset::new(
                            trace.data_lengths.lock().unwrap().iter().sum::<usize>() as u64,
                        ),
                        final_accepted: true,
                    },
                )?)
                .map_err(|error| error.to_string())?;
            dispatch_effects(driver, ack_effects, trace)?;

            let close_effects = model
                .reduce(peer_frame(
                    leg,
                    Record::Close {
                        flow_id: flow.flow_id(),
                        direction: Direction::TargetToClient,
                        final_offset: peer_offset,
                    },
                )?)
                .map_err(|error| error.to_string())?;
            dispatch_effects(driver, close_effects, trace)
        }

        async fn run_fake_resumable_driver(
            mut driver: ResumableTcpDriver,
            mut model: SessionModel,
            leg: CommittedLeg,
            first_ack_gate: OpenStallControl,
            trace: &ResumableTrace,
        ) -> Result<(), String> {
            let flow = driver.local_flow();
            let mut peer_offset = ByteOffset::new(0);
            while let Some(input) = driver.recv_next().await {
                match input {
                    DriverInput::Data(data) => {
                        handle_driver_data(
                            &mut driver,
                            &mut model,
                            leg,
                            &first_ack_gate,
                            trace,
                            data,
                            &mut peer_offset,
                        )
                        .await?;
                    }
                    DriverInput::Control(event) => {
                        match &event {
                            SessionEvent::SinkAccepted { bytes, .. } => {
                                trace.sink_acceptances.lock().unwrap().push(*bytes);
                            }
                            SessionEvent::SinkHalfClosed { .. } => {
                                trace.sink_half_closed_seen.store(true, Ordering::Release);
                                trace
                                    .sink_half_closed_events
                                    .fetch_add(1, Ordering::Relaxed);
                            }
                            SessionEvent::SinkAbandoned { .. }
                            | SessionEvent::SinkHalfCloseFailed { .. }
                            | SessionEvent::LocalReset { .. } => {
                                return Err(format!("unexpected happy-path control: {event:?}"));
                            }
                            _ => {}
                        }
                        if matches!(event, SessionEvent::LocalClose { .. }) {
                            handle_local_close(
                                &mut driver,
                                &mut model,
                                leg,
                                flow,
                                peer_offset,
                                trace,
                            )?;
                        } else {
                            let effects = model.reduce(event).map_err(|error| error.to_string())?;
                            dispatch_effects(&mut driver, effects, trace)?;
                        }
                    }
                }
            }
            Ok(())
        }

        fn generator_iface(device: &mut GeneratorDevice) -> Interface {
            let config = SmolConfig::new(smoltcp::wire::HardwareAddress::Ip);
            let mut iface = Interface::new(config, device, SmolInstant::now());
            iface.update_ip_addrs(|addresses| {
                addresses
                    .push(IpCidr::new(IpAddress::Ipv4(GEN_IP), 24))
                    .unwrap();
            });
            iface.routes_mut().add_default_ipv4_route(GEN_IP).unwrap();
            iface
        }

        #[tokio::test]
        async fn tun_selects_resumable_and_preserves_exact_ownership_through_fin() {
            let open_gate = OpenStallControl::new();
            let driver_gate = OpenStallControl::new();
            let first_ack_gate = OpenStallControl::new();
            let trace = Arc::new(ResumableTrace::default());
            let upstream = Arc::new(FakeResumableUpstream::new(
                open_gate.clone(),
                driver_gate.clone(),
                first_ack_gate.clone(),
                Arc::clone(&trace),
            ));

            let gen_to_sut = PacketLink::new();
            let sut_to_gen = PacketLink::new();
            let sut_device = LoopbackTunDevice::new(gen_to_sut.clone(), sut_to_gen.clone());
            let (_udp_tx, udp_rx) = mpsc::channel(1);
            let config = TunRuntimeConfig::from_sources(Some("1")).unwrap();
            let metrics = Arc::new(Metrics::new());
            let recorded = Arc::new(Mutex::new(Recorded::default()));
            let sut = tokio::spawn(run_owned_event_loop(
                sut_device,
                Arc::clone(&upstream),
                UdpDownlinkSource::owned(udp_rx),
                config,
                metrics,
                RecordingSink::new(recorded),
            ));

            let mut gen_device = GeneratorDevice::new(sut_to_gen, gen_to_sut);
            let mut gen_iface = generator_iface(&mut gen_device);
            let payload: Vec<u8> = (0..PORT_BYTES + SUFFIX_BYTES)
                .map(|index| (index as u8).wrapping_mul(17).wrapping_add(3))
                .collect();
            let buffer_bytes = payload.len() * 2;
            let mut socket = tcp::Socket::new(
                tcp::SocketBuffer::new(vec![0; buffer_bytes]),
                tcp::SocketBuffer::new(vec![0; buffer_bytes]),
            );
            socket
                .connect(
                    gen_iface.context(),
                    (IpAddress::Ipv4(TARGET_IP), TARGET_PORT_BASE),
                    40_016,
                )
                .unwrap();
            let mut sockets = SocketSet::new(vec![]);
            let handle = sockets.add(socket);
            let mut sent = 0usize;
            let mut received = Vec::with_capacity(payload.len());

            let preopen_started = Instant::now();
            while preopen_started.elapsed() < Duration::from_secs(2) {
                gen_iface.poll(SmolInstant::now(), &mut gen_device, &mut sockets);
                let socket = sockets.get_mut::<tcp::Socket>(handle);
                if sent < payload.len() && socket.can_send() {
                    sent += socket.send_slice(&payload[sent..]).unwrap_or(0);
                }
                if open_gate.is_waiting()
                    && sent == payload.len()
                    && socket.send_queue() == SUFFIX_BYTES
                {
                    break;
                }
                tokio::time::sleep(Duration::from_micros(100)).await;
            }
            assert!(open_gate.is_waiting(), "async resumable open never started");
            assert_eq!(sent, payload.len(), "generator did not enqueue all bytes");
            assert_eq!(
                sockets.get::<tcp::Socket>(handle).send_queue(),
                SUFFIX_BYTES,
                "open-pending path consumed bytes before a resumable port existed"
            );
            assert!(trace.probe().is_none());

            open_gate.release();
            let queued_started = Instant::now();
            loop {
                gen_iface.poll(SmolInstant::now(), &mut gen_device, &mut sockets);
                if trace
                    .probe()
                    .is_some_and(|probe| probe.snapshot().uplink_owned_bytes() == PORT_BYTES)
                {
                    break;
                }
                assert!(
                    queued_started.elapsed() < Duration::from_secs(2),
                    "resumable port never reserved the exact pre-recv extent"
                );
                tokio::time::sleep(Duration::from_micros(100)).await;
            }
            assert!(driver_gate.is_waiting());
            assert!(trace.data_lengths.lock().unwrap().is_empty());

            let suffix_arrival_started = Instant::now();
            while sockets.get::<tcp::Socket>(handle).send_queue() != 0
                && suffix_arrival_started.elapsed() < Duration::from_secs(2)
            {
                gen_iface.poll(SmolInstant::now(), &mut gen_device, &mut sockets);
                tokio::time::sleep(Duration::from_micros(100)).await;
            }
            assert_eq!(sockets.get::<tcp::Socket>(handle).send_queue(), 0);
            assert_eq!(
                trace.probe().unwrap().snapshot().uplink_owned_bytes(),
                PORT_BYTES,
                "full message lane must retain exact pre-recv byte ownership"
            );

            driver_gate.release();
            let first_data_started = Instant::now();
            while !first_ack_gate.is_waiting()
                && first_data_started.elapsed() < Duration::from_secs(2)
            {
                gen_iface.poll(SmolInstant::now(), &mut gen_device, &mut sockets);
                tokio::time::sleep(Duration::from_micros(100)).await;
            }
            assert!(first_ack_gate.is_waiting());
            assert_eq!(*trace.data_lengths.lock().unwrap(), vec![PORT_BYTES]);
            for _ in 0..100 {
                gen_iface.poll(SmolInstant::now(), &mut gen_device, &mut sockets);
                tokio::task::yield_now().await;
            }
            assert_eq!(*trace.data_lengths.lock().unwrap(), vec![PORT_BYTES]);
            assert_eq!(
                trace.probe().unwrap().snapshot().uplink_owned_bytes(),
                PORT_BYTES,
                "full byte ledger must leave the already-arrived suffix in smoltcp"
            );

            first_ack_gate.release();
            let echo_started = Instant::now();
            while received.len() < payload.len() && echo_started.elapsed() < Duration::from_secs(3)
            {
                gen_iface.poll(SmolInstant::now(), &mut gen_device, &mut sockets);
                let socket = sockets.get_mut::<tcp::Socket>(handle);
                while socket.can_recv() {
                    let result = socket.recv(|bytes| {
                        received.extend_from_slice(bytes);
                        (bytes.len(), ())
                    });
                    if result.is_err() {
                        break;
                    }
                }
                tokio::time::sleep(Duration::from_micros(100)).await;
            }
            assert_eq!(
                received, payload,
                "resumable echo changed or lost owned bytes"
            );
            assert_eq!(
                *trace.data_lengths.lock().unwrap(),
                vec![PORT_BYTES, SUFFIX_BYTES],
                "byte saturation must defer, not consume or merge, the suffix"
            );

            sockets.get_mut::<tcp::Socket>(handle).close();
            let fin_started = Instant::now();
            while !(trace.local_close_seen.load(Ordering::Acquire)
                && trace.sink_half_closed_seen.load(Ordering::Acquire)
                && trace.graceful_terminal_seen.load(Ordering::Acquire))
                && fin_started.elapsed() < Duration::from_secs(3)
            {
                gen_iface.poll(SmolInstant::now(), &mut gen_device, &mut sockets);
                tokio::time::sleep(Duration::from_micros(100)).await;
            }

            let terminal_delivery_started = Instant::now();
            while !trace.probe().unwrap().snapshot().is_quiescent()
                && terminal_delivery_started.elapsed() < Duration::from_secs(2)
            {
                gen_iface.poll(SmolInstant::now(), &mut gen_device, &mut sockets);
                tokio::time::sleep(Duration::from_micros(100)).await;
            }

            assert!(trace.local_close_seen.load(Ordering::Acquire));
            assert!(trace.sink_half_closed_seen.load(Ordering::Acquire));
            assert!(trace.graceful_terminal_seen.load(Ordering::Acquire));
            assert_eq!(trace.local_close_events.load(Ordering::Relaxed), 1);
            assert_eq!(trace.sink_half_closed_events.load(Ordering::Relaxed), 1);
            assert_eq!(trace.graceful_terminal_events.load(Ordering::Relaxed), 1);
            assert!(trace.probe().unwrap().snapshot().is_quiescent());
            let sink_offers = trace.sink_offers.lock().unwrap().clone();
            let sink_acceptances = trace.sink_acceptances.lock().unwrap().clone();
            assert_eq!(
                sink_acceptances.iter().sum::<usize>(),
                payload.len(),
                "SinkAccepted must account only exact bytes accepted by real send_slice calls"
            );
            assert!(sink_acceptances.iter().all(|accepted| *accepted > 0));
            assert_eq!(sink_acceptances.len(), sink_offers.len());
            assert!(
                sink_acceptances
                    .iter()
                    .zip(&sink_offers)
                    .all(|(accepted, offered)| accepted <= offered)
            );
            assert_eq!(upstream.opens.load(Ordering::Relaxed), 1);
            assert_eq!(upstream.port_factory.session_uplink_owned_bytes(), 0);
            let errors = trace.errors.lock().unwrap().clone();
            assert!(errors.is_empty(), "{errors:?}");

            sut.abort();
            let _ = sut.await;
            upstream.stop_tasks();
        }

        #[tokio::test]
        async fn async_resumable_open_reenters_zero_byte_closewait_and_emits_one_fin() {
            let open_gate = OpenStallControl::new();
            let driver_gate = OpenStallControl::new();
            let first_ack_gate = OpenStallControl::new();
            let trace = Arc::new(ResumableTrace::default());
            let upstream = Arc::new(FakeResumableUpstream::new(
                open_gate.clone(),
                driver_gate.clone(),
                first_ack_gate,
                Arc::clone(&trace),
            ));

            let gen_to_sut = PacketLink::new();
            let sut_to_gen = PacketLink::new();
            let sut_device = LoopbackTunDevice::new(gen_to_sut.clone(), sut_to_gen.clone());
            let (_udp_tx, udp_rx) = mpsc::channel(1);
            let config = TunRuntimeConfig::from_sources(Some("1")).unwrap();
            let metrics = Arc::new(Metrics::new());
            let recorded = Arc::new(Mutex::new(Recorded::default()));
            let sut = tokio::spawn(run_owned_event_loop(
                sut_device,
                Arc::clone(&upstream),
                UdpDownlinkSource::owned(udp_rx),
                config,
                metrics,
                RecordingSink::new(recorded),
            ));

            let mut gen_device = GeneratorDevice::new(sut_to_gen, gen_to_sut);
            let mut gen_iface = generator_iface(&mut gen_device);
            let mut socket = tcp::Socket::new(
                tcp::SocketBuffer::new(vec![0; 4096]),
                tcp::SocketBuffer::new(vec![0; 4096]),
            );
            socket
                .connect(
                    gen_iface.context(),
                    (IpAddress::Ipv4(TARGET_IP), TARGET_PORT_BASE),
                    40_017,
                )
                .unwrap();
            let mut sockets = SocketSet::new(vec![]);
            let handle = sockets.add(socket);

            let connect_started = Instant::now();
            while !sockets.get::<tcp::Socket>(handle).may_send()
                && connect_started.elapsed() < Duration::from_secs(2)
            {
                gen_iface.poll(SmolInstant::now(), &mut gen_device, &mut sockets);
                tokio::time::sleep(Duration::from_micros(100)).await;
            }
            assert!(sockets.get::<tcp::Socket>(handle).may_send());
            sockets.get_mut::<tcp::Socket>(handle).close();

            let pending_started = Instant::now();
            while !open_gate.is_waiting() && pending_started.elapsed() < Duration::from_secs(2) {
                gen_iface.poll(SmolInstant::now(), &mut gen_device, &mut sockets);
                tokio::time::sleep(Duration::from_micros(100)).await;
            }
            assert!(
                open_gate.is_waiting(),
                "zero-byte CloseWait never opened owner flow"
            );
            assert!(!trace.local_close_seen.load(Ordering::Acquire));
            assert!(trace.probe().is_none());

            open_gate.release();
            driver_gate.release();
            let fin_started = Instant::now();
            while !(trace.local_close_seen.load(Ordering::Acquire)
                && trace.sink_half_closed_seen.load(Ordering::Acquire)
                && trace.graceful_terminal_seen.load(Ordering::Acquire))
                && fin_started.elapsed() < Duration::from_secs(3)
            {
                gen_iface.poll(SmolInstant::now(), &mut gen_device, &mut sockets);
                tokio::time::sleep(Duration::from_micros(100)).await;
            }

            let terminal_delivery_started = Instant::now();
            while !trace.probe().unwrap().snapshot().is_quiescent()
                && terminal_delivery_started.elapsed() < Duration::from_secs(2)
            {
                gen_iface.poll(SmolInstant::now(), &mut gen_device, &mut sockets);
                tokio::time::sleep(Duration::from_micros(100)).await;
            }

            assert!(trace.local_close_seen.load(Ordering::Acquire));
            assert!(trace.sink_half_closed_seen.load(Ordering::Acquire));
            assert!(trace.graceful_terminal_seen.load(Ordering::Acquire));
            assert_eq!(trace.local_close_events.load(Ordering::Relaxed), 1);
            assert_eq!(trace.sink_half_closed_events.load(Ordering::Relaxed), 1);
            assert_eq!(trace.graceful_terminal_events.load(Ordering::Relaxed), 1);
            assert!(trace.probe().unwrap().snapshot().is_quiescent());
            assert!(trace.data_lengths.lock().unwrap().is_empty());
            assert!(trace.sink_offers.lock().unwrap().is_empty());
            assert!(trace.sink_acceptances.lock().unwrap().is_empty());
            assert_eq!(upstream.opens.load(Ordering::Relaxed), 1);
            assert_eq!(upstream.port_factory.session_uplink_owned_bytes(), 0);
            let errors = trace.errors.lock().unwrap().clone();
            assert!(errors.is_empty(), "{errors:?}");

            sut.abort();
            let _ = sut.await;
            upstream.stop_tasks();
        }
    }
}
