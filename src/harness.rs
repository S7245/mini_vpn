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

use crate::client_tun::{MetricsSink, TunRuntimeConfig, run_event_loop};
use crate::device::TunIo;
use crate::metrics::Metrics;
use crate::shared::{ClientError, TargetAddr};
use crate::upstream::{DatagramUpstream, ProxyUpstream, RelayStream};

use bytes::BytesMut;
use smoltcp::iface::{Config as SmolConfig, Interface, SocketHandle, SocketSet};
use smoltcp::phy::{Device, DeviceCapabilities, Medium, RxToken, TxToken};
use smoltcp::socket::tcp;
use smoltcp::time::Instant as SmolInstant;
use smoltcp::wire::{IpAddress, IpCidr, Ipv4Address};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{Notify, mpsc};

// ============================ 内存包管道 ============================

/// 单向 IP 包管道（一端 push，另一端 pop + await）。内部 `Arc` 共享，可廉价 clone。
#[derive(Clone, Default)]
pub struct PacketLink {
    queue: Arc<Mutex<VecDeque<BytesMut>>>,
    notify: Arc<Notify>,
}

impl PacketLink {
    fn new() -> Self {
        Self::default()
    }
    fn push(&self, pkt: BytesMut) {
        self.queue.lock().unwrap().push_back(pkt);
        self.notify.notify_one();
    }
    fn pop(&self) -> Option<BytesMut> {
        self.queue.lock().unwrap().pop_front()
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

fn loopback_caps() -> DeviceCapabilities {
    let mut caps = DeviceCapabilities::default();
    caps.max_transmission_unit = 1500;
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
    tx_queue: VecDeque<BytesMut>,
    inbound: PacketLink,  // 发生器 → SUT
    outbound: PacketLink, // SUT → 发生器
    /// 刀12：每次 `flush_tx` 注入的合成 on-loop CPU 成本（busy-spin）。默认 ZERO。
    /// `flush_tx` 在主循环 poll 段内（enter_poll/leave_poll 括起）→ 该 burn 计入 poll_time / loop-active，
    /// 用于验证 profiler 能侦测「主循环被 on-loop CPU 拖满」的饱和信号（T4 spike）。
    cpu_burn_per_flush: Duration,
}

impl LoopbackTunDevice {
    fn new(inbound: PacketLink, outbound: PacketLink) -> Self {
        Self::with_burn(inbound, outbound, Duration::ZERO)
    }

    /// 带合成 on-loop CPU 成本的回环设备（T4：multi_thread 饱和 spike 用）。
    fn with_burn(inbound: PacketLink, outbound: PacketLink, cpu_burn_per_flush: Duration) -> Self {
        Self {
            rx_buffer: None,
            tx_queue: VecDeque::new(),
            inbound,
            outbound,
            cpu_burn_per_flush,
        }
    }
}

impl TunIo for LoopbackTunDevice {
    async fn wait_for_rx(&mut self) -> std::io::Result<()> {
        loop {
            if let Some(pkt) = self.inbound.pop() {
                self.rx_buffer = Some(pkt);
                return Ok(());
            }
            // notify_one 在无 waiter 时保留 1 个 permit，不丢唤醒（pop→None 与 notified 之间的竞态安全）。
            self.inbound.notify.notified().await;
        }
    }
    fn rx_peek(&self) -> Option<&[u8]> {
        self.rx_buffer.as_deref()
    }
    fn rx_take(&mut self) -> Option<BytesMut> {
        self.rx_buffer.take()
    }
    async fn flush_tx(&mut self) -> std::io::Result<()> {
        busy_spin(self.cpu_burn_per_flush); // 刀12：合成 on-loop CPU（默认 ZERO 即 no-op）。
        while let Some(pkt) = self.tx_queue.pop_front() {
            self.outbound.push(pkt);
        }
        Ok(())
    }
    fn inject_ip_packet(&mut self, pkt: &[u8]) {
        self.tx_queue.push_back(BytesMut::from(pkt));
    }
}

impl Device for LoopbackTunDevice {
    type RxToken<'a> = RawRxToken;
    type TxToken<'a> = QueueTxToken<'a>;
    fn capabilities(&self) -> DeviceCapabilities {
        loopback_caps()
    }
    fn receive(&mut self, _t: SmolInstant) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        // 只取 rx_buffer（由 wait_for_rx 单包填充），保持「每次 wakeup 处理一个包」语义，
        // 让 SYN inspector / classify 的逐包窥视与真 utun 一致。
        self.rx_buffer.take().map(|buffer| {
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
}

impl Device for GeneratorDevice {
    type RxToken<'a> = RawRxToken;
    type TxToken<'a> = LinkTxToken;
    fn capabilities(&self) -> DeviceCapabilities {
        loopback_caps()
    }
    fn receive(&mut self, _t: SmolInstant) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        self.inbound.pop().map(|buffer| {
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
        }
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

#[derive(Default)]
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
    let mut gen_device = GeneratorDevice {
        inbound: sut_to_gen,
        outbound: gen_to_sut,
    };
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
    let rec = shared.lock().unwrap();
    let bytes_echoed: usize = conns.iter().map(|c| c.recvd).sum();
    let latencies_us: Vec<u64> = conns.iter().filter_map(|c| c.done_us).collect();
    let avg_listeners = if rec.listener_obs > 0 {
        rec.listener_sum as f64 / rec.listener_obs as f64
    } else {
        0.0
    };
    // per-socket 缓冲 = 每 listener 的 smoltcp rx+tx（引用真常量，避免与 client_tun 漂移）。
    let per_socket_buffer_bytes = 2 * crate::client_tun::TCP_SOCKET_BUFFER_SIZE;

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

    let mut gen_device = GeneratorDevice {
        inbound: sut_to_gen,
        outbound: gen_to_sut,
    };
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

    let mut gen_device = GeneratorDevice {
        inbound: sut_to_gen,
        outbound: gen_to_sut,
    };
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
