use crate::device::{TunIo, VirtualTunDevice};
use crate::shared::{ClientError, TargetAddr};
use crate::tuic::{
    AssocTable, FragReassembler, TuicClientConfig, TuicUpstream, decode_packet_meta, encode_packet,
};
use crate::upstream::{DatagramUpstream, ProxyUpstream, RelayStream};
use crate::reality_upstream::RealityUpstream;
use crate::failover::FailoverUpstream;
use crate::udp_relay::{
    FourTuple, UDP_FLOW_IDLE_SECS, UdpInbound, build_udp_ip_packet, parse_inbound_udp,
};
use crate::dns::{self, Answer};
use crate::fake_ip::FakeIpPool;
use crate::loop_profiler::LoopProfiler;
use crate::metrics::Metrics;
use std::net::Ipv4Addr;
use smoltcp::iface::{Config as SmolConfig, Interface, SocketHandle, SocketSet};
use smoltcp::socket::tcp::{Socket as TcpSocket, SocketBuffer as TcpSocketBuffer, State as TcpState};
use smoltcp::wire::{IpAddress, IpCidr};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::sync::Arc;

use std::collections::{HashMap, HashSet};
use tokio::sync::mpsc;

pub(crate) const TCP_SOCKET_BUFFER_SIZE: usize = 65_535;
const RELAY_CHANNEL_CAPACITY: usize = 1024;
const _: () = assert!(RELAY_CHANNEL_CAPACITY >= 1);
/// L2（刀9 F4）：一条 relay 双向静默多久判 idle → 退出 + shutdown。防慢/卡死上游（尤其 REALITY
/// TCP-only 手写 TLS 遇 server 不返回）长期挂住 relay task 泄漏。90s 偏宽松保稳（长轮询/SSE 不误杀）。
const RELAY_IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(90);
/// Knife14n：本地 Finish 已关闭上游写半边后，只剩远端读半边。若远端没有继续发数据或 EOF，
/// 用短窗口关闭，避免 read-only relay 卡住 active gauge 并污染下一轮 suite。
const RELAY_HALF_CLOSED_IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
/// Knife14f：单次本地→上游写入的进度预算。`write_all` 在 QUIC send window 黑洞时可长期 Pending；
/// 必须在 relay task 内部止血，否则外层 `select!` 的 idle timeout 永远没有机会运行。
const RELAY_WRITE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
/// Knife14o：relay 已关闭但仍有 downlink_pending 时，给 dirty flush 一个短窗口。
/// 超过窗口仍无法交付则回到 hard reap，防永久脏槽。
const DEFERRED_CLOSE_PENDING_GRACE_SECS: u64 = 5;
/// 刀11：数据面可观测性快照周期（30s，对齐 TuicUpstream UDP_STATS_LOG_SECS）。仅周期采样/打印，不进热路径。
const METRICS_SNAPSHOT_SECS: u64 = 30;

/// 刀13 ①：解析 `MINI_VPN_TRACE`（`1`/`true`，去空白、不区分大小写 → 开；其它/缺省 → 关）。
/// 对齐 [`parse_profile_loop`] 惯用法；抽纯函数便于单测（`trace_enabled` 只是它 + `OnceLock` 包壳）。
fn parse_trace(s: Option<&str>) -> bool {
    matches!(
        s.map(|v| v.trim().to_ascii_lowercase()).as_deref(),
        Some("1") | Some("true")
    )
}

/// 刀13 ①：热路径诊断日志总开关。**只读一次** env 缓存进 `OnceLock`——门控的是每包/每连接路径
/// （刀12 OS sample 实锤 `_print → write` 是主循环 #1 on-CPU 成本，22000 事件/秒），绝不能像 reality 的
/// `rdbg!` 那样每次调用一次 env syscall。默认关 → 主循环热路径零 stdout。
fn trace_enabled() -> bool {
    static TRACE: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *TRACE.get_or_init(|| parse_trace(std::env::var("MINI_VPN_TRACE").ok().as_deref()))
}

/// 刀13 ①：热路径诊断 `println!` 门控宏——`trace_enabled()` 为真才打印（默认静默；翻 `MINI_VPN_TRACE=1`
/// 全恢复，零信息损失）。保留 `println!`（stdout，acceptance 重定向到日志）语义不变。**仅门控每包/每连接/
/// churn 噪声**；启动/错误/周期 📊/acceptance 依赖信号（🪪 DNS / 🛡️ 阻断 / remote-open 失败）/run_relay
/// lifecycle 仍用裸 `println!`（见 `docs/tech/2026-06-27-knife13-loop-hotpath-spec.md` §1.2 门控清单）。
macro_rules! trace_log {
    ($($arg:tt)*) => {
        if trace_enabled() {
            println!($($arg)*);
        }
    };
}

/// 刀14c：TCP 下行/背压诊断日志开关。默认关，避免高基数 per-handle stdout 干扰 harness/热路径；
/// US-client 14c suite 显式 `MINI_VPN_TCP_DIAG=1` 打开。
fn tcp_diag_enabled() -> bool {
    static TCP_DIAG: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *TCP_DIAG.get_or_init(|| parse_trace(std::env::var("MINI_VPN_TCP_DIAG").ok().as_deref()))
}

macro_rules! tcp_diag_log {
    ($($arg:tt)*) => {
        if tcp_diag_enabled() {
            println!($($arg)*);
        }
    };
}

/// 主循环分段插桩接缝（knife1：并发压测定位瓶颈）。
///
/// 中文要点：生产传 [`NoopSink`]（空方法，单态化内联后**零开销**，热路径无 `Instant::now()`）；
/// 并发压测 harness 传 RecordingSink，在每个回调里采集每段耗时/调用次数。计时逻辑全部留在 sink
/// 实现内，主循环只做平凡方法调用——生产与测试**同一份循环**。
pub trait MetricsSink {
    /// 进入 smoltcp poll 段（poll → flush_tx）。
    fn enter_poll(&mut self) {}
    /// 离开 poll 段。
    fn leave_poll(&mut self) {}
    /// 进入 relay 调度段（脏集合驱动 `process_dirty_relay`，刀2 起 O(活跃 handle)、非全量遍历）。
    fn enter_relay(&mut self) {}
    /// 离开 relay 调度段。
    fn leave_relay(&mut self) {}
    /// 记录本 tick relay 段遍历的 listener handle 数（量化怀疑瓶颈 #1：O(n) 全量遍历）。
    fn note_listeners(&mut self, _n: usize) {}
    /// 刀12：主循环底部、即将停在 `tokio::select!` 空等下一个事件——park 开始。
    fn loop_park_begin(&mut self) {}
    /// 刀12：select! arm 首行——park 结束、active 开始（一次迭代）。
    fn loop_park_end(&mut self) {}
    /// 刀12：报告周期结束（metrics_tick）——输出 🔬 归因行并重置周期累计。
    fn report(&mut self) {}
}

/// 生产用空插桩：所有回调空实现，单态化后零开销。
pub struct NoopSink;
impl MetricsSink for NoopSink {}
// 中文要点：Stage 9 起按"每端口"配 pool，64 端口 * 2 槽 * 2 缓冲 ≈ 16MB。
const DEFAULT_TUN_POOL_SIZE: usize = 2;
/// 刀14c：TUN IP MTU 默认保持 1500（零惊喜）；US-client 14c suite 显式设 1200 作为测试基准。
const DEFAULT_TUN_MTU: usize = 1500;
const MIN_TUN_MTU: usize = 576;
const MAX_TUN_MTU: usize = 9000;

/// One listener socket's binding parameters.
/// 中文要点：Stage 9 起 pool_size 已上移到 `ListenerRegistry`，这里只剩端口。
#[derive(Debug, Clone, Copy)]
struct ListenerSpec {
    /// Local TCP port intercepted on the TUN-side smoltcp stack.
    local_port: u16,
}

/// Explicit lifecycle state for one listener slot.
/// 中文要点：每个 handle 都要有自己的状态，避免“一个槽位出错、全局都混乱”。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SocketState {
    /// Ready to accept a new intercepted local TCP session.
    Listening,
    /// Opening the remote substream **inline**（仅明确 cheap 的测试/mock 路径）。失败会卡在此态 →
    /// 由 `reap_dead_slots` 第三判据回收（兼容旧 inline 行为）。
    OpeningRemote,
    /// Remote TCP open has been spawned out of the main loop and is still in flight.
    /// 中文要点：与 `OpeningRemote` 区分——in-flight open 是**正常态**（由上游 open_tcp 内置超时
    /// 兜底 + `HandshakeDone` 必回灌），`reap_dead_slots` **不**按「OpeningRemote+无 uplink_tx」误杀它；
    /// 仅当本地 socket 已不 active，或 CloseWait 已无 live relay 时才回收（回收时 bump epoch → 迟到结果被丢）。
    HandshakePending,
    /// Local and remote sides are actively relaying payloads.
    Relaying,
    /// The current slot is closing after EOF or transport failure.
    Closing,
    /// The slot is re-entering the listening state after cleanup.
    Rearming,
}

#[derive(Debug, Default, Clone)]
struct TcpDownlinkDiag {
    /// Bytes accepted from relay task into the main-loop global_rx path.
    remote_to_global_rx_bytes: u64,
    /// Highest observed `SocketCtx.downlink_pending.len()`.
    downlink_pending_high_water: usize,
    /// Number of attempted `tcp_socket.send_slice` calls.
    send_slice_calls: u64,
    /// Bytes accepted by smoltcp `send_slice`.
    send_slice_accepted_bytes: u64,
    /// Times smoltcp accepted zero bytes while the socket was considered writable.
    send_slice_zero: u64,
    /// `send_slice` errors, usually socket close/reset while downlink still had bytes.
    send_slice_errors: u64,
    /// TCP downlink-triggered TUN flush attempts.
    tun_flush_tx_calls: u64,
    /// TCP downlink-triggered TUN flush failures.
    tun_flush_tx_failures: u64,
}

impl TcpDownlinkDiag {
    fn note_pending(&mut self, pending_current: usize) {
        self.downlink_pending_high_water =
            self.downlink_pending_high_water.max(pending_current);
    }

    fn note_remote_payload(&mut self, bytes: usize, pending_current: usize) {
        self.remote_to_global_rx_bytes += bytes as u64;
        self.note_pending(pending_current);
    }

    fn note_send_slice_ok(&mut self, accepted: usize, pending_current: usize) {
        self.send_slice_calls += 1;
        self.send_slice_accepted_bytes += accepted as u64;
        if accepted == 0 {
            self.send_slice_zero += 1;
        }
        self.note_pending(pending_current);
    }

    fn note_send_slice_error(&mut self) {
        self.send_slice_calls += 1;
        self.send_slice_errors += 1;
    }

    fn note_tun_flush(&mut self, ok: bool) {
        self.tun_flush_tx_calls += 1;
        if !ok {
            self.tun_flush_tx_failures += 1;
        }
    }
}

#[derive(Debug, Default, Clone)]
struct RelayTaskDiag {
    /// Bytes written from local smoltcp side into the remote stream.
    uplink_bytes: u64,
    /// Bytes read from the remote stream and accepted into the global_rx path.
    remote_to_global_rx_bytes: u64,
    /// Number of successful remote stream reads.
    remote_reads: u64,
    /// Number of successful local payload writes to the remote stream.
    local_writes: u64,
    /// Highest observed wait before global_rx accepted a remote payload.
    global_rx_wait_max_micros: u128,
    /// Count of remote payload sends whose wait crossed the diagnostic threshold.
    global_rx_pressure_events: u64,
}

impl RelayTaskDiag {
    fn note_uplink_write(&mut self, bytes: usize) {
        self.local_writes += 1;
        self.uplink_bytes += bytes as u64;
    }

    fn note_remote_read(&mut self, bytes: usize) {
        self.remote_reads += 1;
        self.remote_to_global_rx_bytes += bytes as u64;
    }

    fn note_global_rx_wait(
        &mut self,
        elapsed: std::time::Duration,
        pressure_threshold: std::time::Duration,
    ) {
        self.global_rx_wait_max_micros =
            self.global_rx_wait_max_micros.max(elapsed.as_micros());
        if elapsed >= pressure_threshold {
            self.global_rx_pressure_events += 1;
        }
    }
}

#[derive(Debug)]
enum RelayCommand {
    Data(Vec<u8>),
    Finish,
}

#[derive(Debug)]
enum RelayWriterSignal {
    Progress { bytes: usize },
    WriteHalfClosed { reason: &'static str },
    Closed {
        direction: &'static str,
        reason: &'static str,
    },
}

/// Terminal reason reported by one background TCP relay task.
#[derive(Debug, Clone, Copy)]
struct RelayClose {
    epoch: u64,
    direction: &'static str,
    reason: &'static str,
}

/// Event sent from one background TCP relay task back into the single owner main loop.
/// Data events preserve the existing downlink path; Closed makes every relay terminal reason visible to
/// the socket state machine instead of relying on smoltcp to notice the local side later.
#[derive(Debug)]
enum RelayEvent {
    Data { epoch: u64, bytes: Vec<u8> },
    Closed(RelayClose),
}

/// Per-handle runtime context owned by a single listener slot.
/// 中文要点：这是“房间上下文”，每个 handle 都有一份，专门存本槽位的状态和上行通道。
#[derive(Debug)]
struct SocketCtx {
    /// The local port that must be re-listened after this slot is rearmed.
    local_port: u16,
    /// Current lifecycle state for this listener slot.
    state: SocketState,
    /// Sender used to push local payloads into the remote relay task for this slot only.
    uplink_tx: Option<mpsc::Sender<RelayCommand>>,
    /// Whether this flow has already propagated the local TCP finish into the relay write half.
    local_fin_sent: bool,
    /// Downlink bytes not yet accepted by the smoltcp tx buffer.
    /// 中文要点：smoltcp send_slice 可能只写一部分（tx buffer 受 TCP ACK 释放制约），
    /// 写不下的字节必须留在这里、由后续 poll 持续 flush，否则丢字节 → TLS bad decrypt。
    downlink_pending: Vec<u8>,
    /// Relay has ended, but downlink bytes still need to be flushed before the slot can be rearmed.
    pending_relay_close: Option<RelayClose>,
    /// Monotonic event-loop seconds when `pending_relay_close` was installed.
    pending_relay_close_since_secs: Option<u64>,
    /// 本槽位当前 flow 占用的 fake-IP（若 target 经 fake-IP 改写）。
    /// 中文要点：刀2 引用计数——首次开远端时 `acquire`、rearm 时 `release`，
    /// 保证该 fake-IP 映射在本 flow 存活期间不被 sweep 回收（否则 resolve 失败 → 断连）。
    fake_ip: Option<Ipv4Addr>,
    /// 刀9 M3 / 刀14d：async remote-open 代次。每进 `HandshakePending` +1；`rearm` 也 +1（让在飞 open 失效）。
    /// 中文要点：spawn 的 open 任务捕获当时的 epoch；`handle_handshake_done` **先比 epoch**——不匹配
    /// 说明本槽已被 rearm/复位/换代，迟到的 open 结果直接丢弃，绝不装到新一代 socket（防串话核心）。
    conn_epoch: u64,
    /// Async remote-open 在飞期间，本地到达的上行字节暂存（`uplink_tx` 尚未建立）。open 成功后按序
    /// flush 进 relay。**256KB 硬上限**防 OOM——溢出丢弃（应用 TCP 会因未 ACK 而背压/重传，自愈）。
    uplink_buffer: Vec<u8>,
    /// 刀14c：TCP 下行诊断计数。主循环独占，按事件日志输出，不进全局 MetricsSnapshot 防高基数。
    downlink_diag: TcpDownlinkDiag,
}

impl SocketCtx {
    /// Create the initial per-slot runtime context.
    /// 中文要点：每个新建的监听槽位一开始都处于 Listening，没有绑定上行通道。
    /// Target 不再预置——它在首包到达时从被拦截连接的 `local_endpoint()` 提取。
    fn new(local_port: u16) -> Self {
        Self {
            local_port,
            state: SocketState::Listening,
            uplink_tx: None,
            local_fin_sent: false,
            downlink_pending: Vec::new(),
            pending_relay_close: None,
            pending_relay_close_since_secs: None,
            fake_ip: None,
            conn_epoch: 0,
            uplink_buffer: Vec::new(),
            downlink_diag: TcpDownlinkDiag::default(),
        }
    }

    /// Async remote-open 在飞期间往 `uplink_buffer` 追加上行字节，带 256KB 硬上限。返回是否被接受
    /// （false=溢出丢弃，应用 TCP 未 ACK 会背压/重传，自愈）。
    fn buffer_uplink(&mut self, payload: &[u8]) -> bool {
        if self.uplink_buffer.len() + payload.len() > MAX_UPLINK_BUFFER {
            return false;
        }
        self.uplink_buffer.extend_from_slice(payload);
        true
    }
}

/// Async remote-open 在飞期间 `uplink_buffer` 字节上限（防 OOM）。1000 连接×此上限 ≈ 256MB 上界，
/// 实际远低（open 通常 ~RTT 级、buffer 通常仅首包几 KB）；超限丢弃由应用 TCP 背压自愈。
const MAX_UPLINK_BUFFER: usize = 256 * 1024;

/// 刀9 M3 / 刀14d：一次 spawn 出主循环的 remote-open 完成事件，经 mpsc 回灌主循环 select。
/// 中文要点：`epoch` = spawn 时捕获的 open 代次，`handle_handshake_done` 据此防串话（迟到结果不装新代 socket）。
struct HandshakeDone {
    handle: SocketHandle,
    epoch: u64,
    result: Result<RelayStream, ClientError>,
}

/// 刀9 M3：`handshake_done` channel 容量。满时 spawn 的 send().await 背压（不丢，等主循环排空）。
const HANDSHAKE_DONE_CAPACITY: usize = 128;

/// Push as much of the handle's downlink backlog into the smoltcp tx buffer as fits;
/// keep the rest for the next poll. Partial `send_slice` writes are normal.
/// 中文要点：这是修 bad decrypt 的关键——绝不丢弃写不下的字节。
fn flush_downlink(handle: SocketHandle, tcp_socket: &mut TcpSocket, ctx: &mut SocketCtx) {
    if ctx.downlink_pending.is_empty() {
        return;
    }
    ctx.downlink_diag.note_pending(ctx.downlink_pending.len());
    if !tcp_socket.can_send() {
        return;
    }
    match tcp_socket.send_slice(&ctx.downlink_pending) {
        Ok(0) => {
            ctx.downlink_diag
                .note_send_slice_ok(0, ctx.downlink_pending.len());
        }
        Ok(n) => {
            ctx.downlink_pending.drain(..n);
            ctx.downlink_diag
                .note_send_slice_ok(n, ctx.downlink_pending.len());
        }
        Err(_) => {
            tcp_diag_log!(
                "🔎 tcp-send-slice-error handle={:?} pending={} state={:?} → clear",
                handle,
                ctx.downlink_pending.len(),
                ctx.state
            );
            ctx.downlink_diag.note_send_slice_error();
            // socket 不可发（已关闭/复位）：丢弃残留，避免无限堆积。
            ctx.downlink_pending.clear();
            ctx.downlink_diag.note_pending(0);
        }
    }
}

/// Hard cap on the number of distinct destination ports we will intercept.
/// 中文要点：防止 SYN flood 下 socket / 缓冲区无限增长，到顶就拒新端口。
const MAX_INTERCEPTED_PORTS: usize = 64;

/// 全局 listener socket 总数上限（#2 弹性扩容的兜底）。
/// 中文要点：放开了「每端口 pool_size 固定上限」后，仍需一个全局闸防 SYN flood 把内存撑爆。
/// 4096 槽 × 128KB ≈ 512MB 上界；实际按需扩容远小于此。
const MAX_TOTAL_LISTENERS: usize = 4096;

/// SYN 命中时为该端口保证的空闲 listening 槽数（#2 弹性扩容触发阈值）。
/// 中文要点：每个新 SYN 到来前确保该端口恒有 ≥2 个空闲 listening 槽，吸收突发并发，
/// 避免「所有槽都 Relaying 时新 SYN 无 socket 可握手 → SYN 退避重传 stall」。
const MIN_SPARE_LISTENERS: usize = 2;

/// fake-IP 映射回收 TTL（秒）：idle 且 refcount==0 超此时长才回收。
/// 中文要点：远大于 DNS A 记录 TTL（5s）。review #4：取 30min（而非 300s），给「应用已解析并缓存
/// fake-IP、但尚未发起连接」留足窗口——这段 refcount==0，过早回收会让后续用缓存 IP 的连接被 Refuse。
/// 活跃 flow（refcount>0）任何情况都不回收（见 FakeIpPool::sweep）。
const FAKE_IP_TTL: u64 = 1800;

/// Failure mode for `ListenerRegistry::ensure_port`.
/// 中文要点：到顶时优雅拒绝，不能 panic，已注册端口的 socket 不受影响。
#[derive(Debug, PartialEq, Eq)]
enum RegistryError {
    Capped,
}

/// Dynamic per-port listener pools.
/// 中文要点：一个目的端口对应一组（`pool_size` 个）smoltcp 监听 socket，
/// 由 SYN inspector 按需创建；主循环遍历所有 handle 处理首包。
#[derive(Debug)]
struct ListenerRegistry {
    ports: HashMap<u16, Vec<SocketHandle>>,
    pool_size: usize,
    /// 全局 listener socket 总数上限（#2 弹性扩容兜底；默认 [`MAX_TOTAL_LISTENERS`]）。
    max_total: usize,
    /// 当前 listener socket 总数（review #6：O(1) 计数器，避免每 SYN 在扩容循环里 O(端口) 求和）。
    /// 中文要点：槽只增不删（rearm/reap 复用回 Listen，从不从 ports 移除），故计数器随建槽单调累加。
    total: usize,
}

impl ListenerRegistry {
    fn new(pool_size: usize) -> Self {
        Self {
            ports: HashMap::new(),
            pool_size,
            max_total: MAX_TOTAL_LISTENERS,
            total: 0,
        }
    }

    #[cfg(test)]
    fn with_max_total(pool_size: usize, max_total: usize) -> Self {
        Self {
            ports: HashMap::new(),
            pool_size,
            max_total,
            total: 0,
        }
    }

    /// 当前所有端口的 listener socket 总数（O(1)，维护计数器）。
    fn total_handles(&self) -> usize {
        self.total
    }

    #[cfg(test)]
    fn port_count(&self) -> usize {
        self.ports.len()
    }

    /// Idempotently ensure a listener pool exists for `port`.
    /// 中文要点：端口在册时不重复建；首次建则一次性创建 `pool_size` 个监听槽位
    /// 并同步登记 `SocketCtx`，到顶返回 `Capped`。
    fn ensure_port(
        &mut self,
        port: u16,
        sockets: &mut SocketSet<'static>,
        socket_ctxs: &mut HashMap<SocketHandle, SocketCtx>,
    ) -> Result<(), RegistryError> {
        if self.ports.contains_key(&port) {
            return Ok(());
        }
        if self.ports.len() >= MAX_INTERCEPTED_PORTS {
            return Err(RegistryError::Capped);
        }
        let spec = ListenerSpec { local_port: port };
        let mut handles = Vec::with_capacity(self.pool_size);
        for _ in 0..self.pool_size {
            let h = sockets.add(build_listener_socket(&spec));
            socket_ctxs.insert(h, SocketCtx::new(port));
            handles.push(h);
        }
        self.total += self.pool_size;
        trace_log!(
            "🆕 listener pool created for port {port} (pool_size={})",
            self.pool_size
        );
        self.ports.insert(port, handles);
        Ok(())
    }

    /// Iterate every smoltcp handle across all currently intercepted ports.
    /// 中文要点：脏集合驱动后热路径不再用它；仅低频「死槽回收」(reap_dead_slots) + 测试遍历。
    fn all_handles(&self) -> impl Iterator<Item = SocketHandle> + '_ {
        self.ports.values().flatten().copied()
    }

    /// All listener handles registered for `port`（未注册端口返回空 slice）。
    /// 中文要点：#1 脏集合驱动——按 inbound 包的 dst_port 取该端口 pool 全部 handle 标脏，
    /// 替代每 tick 全量 `all_handles()` 遍历。
    fn handles_for_port(&self, port: u16) -> &[SocketHandle] {
        self.ports.get(&port).map(Vec::as_slice).unwrap_or(&[])
    }

    /// #2 弹性扩容：保证 `port` 当前至少有 `min_spare` 个 Listening 槽，不足则按需补建。
    ///
    /// 中文要点：这是放开「每端口 pool_size 固定上限」的核心——热门端口（如 :443）突发时，
    /// 已有槽都进了 Relaying，新 SYN 没有 listening socket 可握手就会 stall。每个 SYN 到来前
    /// 补足空闲槽即可吸收突发。rearm 回 Listening 的旧槽计入空闲、优先复用，不无限增长；
    /// 全局 `max_total` 兜底防 SYN flood，到顶返回 `Capped`（退回旧行为，不 panic）。
    /// 未注册端口（无 SYN 命中过）→ no-op，建池仍由 `ensure_port` 负责。
    fn ensure_spare_listeners(
        &mut self,
        port: u16,
        min_spare: usize,
        sockets: &mut SocketSet<'static>,
        socket_ctxs: &mut HashMap<SocketHandle, SocketCtx>,
    ) -> Result<(), RegistryError> {
        if !self.ports.contains_key(&port) {
            return Ok(());
        }
        // 「空闲」必须看 smoltcp socket 的真实状态（accept 后立即离开 Listen），不能看
        // SocketCtx.state——后者直到 process 首包才更新，SYN 刚被 accept 时仍是 Listening，
        // 计数虚高会导致永不补建（实测单端口仍 stall）。
        let listening = self.ports[&port]
            .iter()
            .filter(|h| sockets.get::<TcpSocket>(**h).state() == TcpState::Listen)
            .count();
        if listening >= min_spare {
            return Ok(());
        }
        let spec = ListenerSpec { local_port: port };
        for _ in listening..min_spare {
            if self.total_handles() >= self.max_total {
                return Err(RegistryError::Capped);
            }
            let h = sockets.add(build_listener_socket(&spec));
            socket_ctxs.insert(h, SocketCtx::new(port));
            self.ports.get_mut(&port).unwrap().push(h);
            self.total += 1;
        }
        Ok(())
    }
}

/// Local listener-side startup configuration for the TUN runtime.
/// 中文要点：这一层只关心本地拦截面，不关心怎么连上游 TLS/Yamux 服务。
#[derive(Debug, Clone)]
pub struct TunListenerConfig {
    /// Per-port pool size: number of smoltcp listener slots created for each
    /// intercepted destination port.
    /// 中文要点：Stage 9 起 pool 按"每端口"算，决定单个端口能并发承接多少条连接。
    pub pool_size: usize,
}

impl TunListenerConfig {
    /// Build listener config from optional string sources.
    /// 中文要点：Stage 9 起本地不再固定监听端口，端口由 SYN inspector 按需注册，
    /// 这里只保留 `pool_size` 一个旋钮。
    fn from_sources(pool_size: Option<&str>) -> Result<Self, ClientError> {
        let pool_size = match pool_size {
            Some(value) => value
                .parse::<usize>()
                .map_err(|_| ClientError::InvalidTarget(format!("invalid pool size: {value}")))?,
            None => DEFAULT_TUN_POOL_SIZE,
        };

        if pool_size == 0 {
            return Err(ClientError::InvalidTarget(
                "invalid pool size: must be at least 1".to_string(),
            ));
        }

        Ok(Self { pool_size })
    }
}

/// Startup configuration for the TUN runtime.
/// 中文要点：Stage 13d 退役 legacy 上游后，运行时只剩本地监听池配置；
/// TUIC 出口配置走 `MINI_VPN_TUIC_*`（见 tuic.rs），不在这里。
#[derive(Debug, Clone)]
pub struct TunRuntimeConfig {
    pub listener: TunListenerConfig,
    /// 刀14c：本进程创建 TUN 时使用的 IP MTU，同时喂给 smoltcp DeviceCapabilities。
    /// 默认保持 1500；真实 14c US-client 测试由脚本显式设 `MINI_VPN_TUN_MTU=1200`。
    pub tun_mtu: usize,
    /// 刀11：数据面可观测性 `📊` 快照周期（秒）。默认 [`METRICS_SNAPSHOT_SECS`]（30）；
    /// env `MINI_VPN_METRICS_SECS` 可调（acceptance 设小值如 5 秒级看指标；0/非法回落默认，**绝不为 0**
    /// 否则 `tokio::time::interval` panic）。仅 `from_env` 读 env，`from_sources`（harness/测试）恒用默认。
    pub metrics_secs: u64,
    /// 刀12：主循环 profiler 开关。`MINI_VPN_PROFILE_LOOP=1` 时主循环装 `LoopProfiler`（打 🔬 归因行
    /// 量化 #4 vs #3）；默认 `false` → 装 `NoopSink`（**零开销路径逐字不变**）。仅 `from_env` 读 env，
    /// `from_sources`（harness/测试）恒 `false`。
    pub profile_loop: bool,
}

impl TunRuntimeConfig {
    /// Build config from optional string sources.（metrics 周期恒默认；env 覆盖只在 `from_env`。）
    pub fn from_sources(pool_size: Option<&str>) -> Result<Self, ClientError> {
        Ok(Self {
            listener: TunListenerConfig::from_sources(pool_size)?,
            tun_mtu: DEFAULT_TUN_MTU,
            metrics_secs: METRICS_SNAPSHOT_SECS,
            profile_loop: false,
        })
    }

    /// Read config from process environment
    /// （`MINI_VPN_TUN_POOL_SIZE` + `MINI_VPN_METRICS_SECS` + `MINI_VPN_PROFILE_LOOP`）。
    fn from_env() -> Result<Self, ClientError> {
        let mut cfg = Self::from_sources(std::env::var("MINI_VPN_TUN_POOL_SIZE").ok().as_deref())?;
        cfg.tun_mtu = parse_tun_mtu(std::env::var("MINI_VPN_TUN_MTU").ok().as_deref());
        cfg.metrics_secs = parse_metrics_secs(std::env::var("MINI_VPN_METRICS_SECS").ok().as_deref());
        cfg.profile_loop = parse_profile_loop(std::env::var("MINI_VPN_PROFILE_LOOP").ok().as_deref());
        Ok(cfg)
    }
}

/// 解析 `MINI_VPN_METRICS_SECS`：默认 [`METRICS_SNAPSHOT_SECS`]；**0/非数字一律回落默认**
/// （0 会让 `tokio::time::interval` panic，必须挡住）。
fn parse_metrics_secs(s: Option<&str>) -> u64 {
    s.and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(METRICS_SNAPSHOT_SECS)
}

/// 刀14c：解析 `MINI_VPN_TUN_MTU`。有效 IPv4 MTU 范围采用；缺失/非法回落默认 1500。
fn parse_tun_mtu(s: Option<&str>) -> usize {
    s.and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|&n| (MIN_TUN_MTU..=MAX_TUN_MTU).contains(&n))
        .unwrap_or(DEFAULT_TUN_MTU)
}

/// 刀12：解析 `MINI_VPN_PROFILE_LOOP`：`1`/`true`（不区分大小写、去空白）→ 开；其它/缺省 → 关。
fn parse_profile_loop(s: Option<&str>) -> bool {
    matches!(
        s.map(|v| v.trim().to_ascii_lowercase()).as_deref(),
        Some("1") | Some("true")
    )
}

/// 刀12：按 `profile_loop` 在 `NoopSink`（默认零开销）与 `LoopProfiler`（🔬 归因）间**单态化二选一**
/// 装进共享 [`run_event_loop`]。分支收口一处，三上游 arm 各一行调用、零重复。
///
/// 中文要点：`profile_loop`（Copy bool）作首参先求值，`runtime_config` 作后参再 move——左到右求值序
/// 保证读字段不与 move 冲突。默认（`false`）分支逐字等价旧 `run_event_loop(.., NoopSink)`，零回归。
async fn run_event_loop_sinked<D, U>(
    profile_loop: bool,
    device: D,
    upstream: Arc<U>,
    downlink_rx: mpsc::Receiver<Vec<u8>>,
    runtime_config: TunRuntimeConfig,
    metrics: Arc<Metrics>,
) where
    D: TunIo,
    U: ProxyUpstream + DatagramUpstream + 'static,
{
    if profile_loop {
        run_event_loop(
            device,
            upstream,
            downlink_rx,
            runtime_config,
            metrics,
            LoopProfiler::new(),
        )
        .await;
    } else {
        run_event_loop(
            device,
            upstream,
            downlink_rx,
            runtime_config,
            metrics,
            NoopSink,
        )
        .await;
    }
}

/// 生产入口：建真 utun + 真 TUIC 上游，然后跑共享的 [`run_event_loop`]。
///
/// 中文要点：knife1 起把主循环抽成 `run_event_loop`（泛型 over [`TunIo`] 设备 + [`MetricsSink`]），
/// 生产与并发压测 harness **跑同一份循环代码**。本薄壳只负责构造真依赖；循环逻辑零回归。
pub async fn start_tun_proxy() {
    let runtime_config = match TunRuntimeConfig::from_env() {
        Ok(config) => config,
        Err(e) => {
            println!("加载 TUN 运行时配置失败: {e}");
            return;
        }
    };
    println!(
        "🚀 TUN runtime started with pool_size={}, tun_mtu={}",
        runtime_config.listener.pool_size, runtime_config.tun_mtu
    );
    // 刀11：数据面 📊 快照周期（MINI_VPN_METRICS_SECS 可调；acceptance 设 5 即秒级看指标）。
    println!(
        "📊 数据面可观测性：快照周期 = {}s（MINI_VPN_METRICS_SECS 可调）",
        runtime_config.metrics_secs
    );
    // 刀12：主循环 profiler（默认关；MINI_VPN_PROFILE_LOOP=1 开 → 打 🔬 归因行量化 #4 vs #3）。
    if runtime_config.profile_loop {
        println!(
            "🔬 主循环 profiler 已启用（loop-active/poll/relay 占比，每 {}s；MINI_VPN_PROFILE_LOOP）",
            runtime_config.metrics_secs
        );
    }

    // 1. 初始化 TUN 设备 / 创建操作系统的原生异步虚拟网卡。
    let raw_tun = match create_tun_device(runtime_config.tun_mtu).await {
        Ok(device) => device,
        Err(e) => {
            println!("无法创建 TUN 设备: {e}");
            return;
        }
    };
    let device = VirtualTunDevice::new(raw_tun, runtime_config.tun_mtu);

    // 刀11：进程级数据面可观测性句柄。**在选上游之前**构造一次 → 同一 `Arc<Metrics>` 既喂
    // run_event_loop（DNS/relay/gauge），又（T6 起）clone 进 TuicUpstream（UDP 下行 drop/背压）。
    let metrics = Arc::new(Metrics::new());

    // 2. 选上游 Transport：MINI_VPN_UPSTREAM=tuic（默认）| reality（VLESS over REALITY over TCP，刀8）。
    //    两分支各自单态化 run_event_loop（device 只在选中的分支被 move，互斥）。
    match select_upstream_kind(std::env::var("MINI_VPN_UPSTREAM").ok().as_deref()) {
        UpstreamKind::Tuic => {
            let cfg = match TuicClientConfig::from_env() {
                Ok(c) => c,
                Err(e) => {
                    println!("加载 TUIC 客户端配置失败（启动中止）: {e}");
                    return;
                }
            };
            let upstream = match TuicUpstream::connect(&cfg, Arc::clone(&metrics)).await {
                Ok(u) => {
                    println!("✅ 已连接 TUIC 出口 {} (sing-box)", cfg.server);
                    Arc::new(u)
                }
                Err(e) => {
                    println!("连接 TUIC 出口失败（启动中止）: {e:?}");
                    return;
                }
            };
            // 3. UDP over TUIC Packet 下行接收端（断线自愈，见 13b/13c）。
            println!("🌊 UDP relay 数据面就绪（TUIC Packet datagram → sing-box）");
            let tuic_downlink_rx = upstream.start_udp();
            // 4. 进入共享主循环（生产传 NoopSink：零插桩开销）。
            run_event_loop_sinked(
                runtime_config.profile_loop,
                device,
                upstream,
                tuic_downlink_rx,
                runtime_config,
                Arc::clone(&metrics),
            )
            .await;
        }
        UpstreamKind::Reality => {
            // REALITY 是 **TCP-only**（force-reality）：UDP no-op（DatagramUpstream 静默丢）+
            // **空 downlink channel**——持有 tx 永不 send → run_event_loop 的下行 select 分支永久 pending
            // （REALITY 无 UDP 下行；分离上游/UDP-over-VLESS/failover 留刀9）。
            let upstream = match RealityUpstream::from_env() {
                Ok(u) => {
                    println!("✅ 已配置 REALITY 出口（VLESS over REALITY over TCP；TCP-only，UDP no-op）");
                    Arc::new(u)
                }
                Err(e) => {
                    println!("加载 REALITY 客户端配置失败（启动中止）: {e:?}");
                    return;
                }
            };
            let (_dummy_tx, dummy_rx) = mpsc::channel::<Vec<u8>>(1); // 持 tx 不 drop → 下行分支永挂
            run_event_loop_sinked(
                runtime_config.profile_loop,
                device,
                upstream,
                dummy_rx,
                runtime_config,
                Arc::clone(&metrics),
            )
            .await;
        }
        UpstreamKind::Failover => {
            // 刀9：健康感知 TUIC↔REALITY。两腿都建——TUIC 既承 TCP relay 又是 **UDP 唯一出口**，
            // REALITY 是 TCP-only 备路。`FailoverUpstream::open_tcp` 按健康态选腿；`send_udp` 恒走 tuic。
            let tuic_cfg = match TuicClientConfig::from_env() {
                Ok(c) => c,
                Err(e) => {
                    println!("加载 TUIC 客户端配置失败（failover 启动中止）: {e}");
                    return;
                }
            };
            let tuic = match TuicUpstream::connect(&tuic_cfg, Arc::clone(&metrics)).await {
                Ok(u) => {
                    println!("✅ 已连接 TUIC 出口 {} (failover 主腿)", tuic_cfg.server);
                    Arc::new(u)
                }
                Err(e) => {
                    println!("连接 TUIC 出口失败（failover 启动中止）: {e:?}");
                    return;
                }
            };
            // UDP 下行接收端来源端 = TUIC（独立于 TCP 选腿；UDP 永久绑 TUIC）。
            println!("🌊 UDP relay 数据面就绪（TUIC Packet datagram → sing-box；UDP 永留 QUIC）");
            let tuic_downlink_rx = tuic.start_udp();
            let reality = match RealityUpstream::from_env() {
                Ok(u) => {
                    println!("✅ 已配置 REALITY 出口（failover 备腿；VLESS over REALITY over TCP）");
                    Arc::new(u)
                }
                Err(e) => {
                    println!("加载 REALITY 客户端配置失败（failover 需两腿都配齐，启动中止）: {e:?}");
                    return;
                }
            };
            let upstream = Arc::new(FailoverUpstream::new(tuic, reality));
            // 后台健康探针：REALITY 当班时每 30s 探 TUIC，按不对称迟滞（连续 3 + 60s 冷却）切回。
            upstream.spawn_health_probe();
            println!("🔀 failover 就绪：TCP relay 健康感知 TUIC↔REALITY，UDP 恒走 TUIC");
            run_event_loop_sinked(
                runtime_config.profile_loop,
                device,
                upstream,
                tuic_downlink_rx,
                runtime_config,
                Arc::clone(&metrics),
            )
            .await;
        }
    }
}

/// 选哪个上游 Transport（纯函数，便于单测）。默认 + 未知值 → TUIC（零回归）。
/// 刀9：新增 `failover`（健康感知 TUIC↔REALITY，需两腿都配齐）。`tuic`/`reality` 仍作强制单腿调试旁路。
/// **failover 设为 opt-in**（非默认）：默认/未设保持纯 TUIC，对既有 TUIC-only 部署零回归（稳定优先）。
#[derive(Debug, PartialEq, Eq)]
enum UpstreamKind {
    Tuic,
    Reality,
    Failover,
}

fn select_upstream_kind(env: Option<&str>) -> UpstreamKind {
    match env.map(|s| s.trim().to_ascii_lowercase()).as_deref() {
        Some("reality") => UpstreamKind::Reality,
        Some("failover") => UpstreamKind::Failover,
        _ => UpstreamKind::Tuic,
    }
}

/// 共享主循环：生产（真 utun + 真 TUIC）与并发压测 harness（内存回环 device + mock 上游）共用。
///
/// 中文要点：泛型 over [`TunIo`]（设备接缝）与 [`MetricsSink`]（分段插桩）。所有 smoltcp 装置
/// （sockets / iface / registry / fake DNS / fake_pool / assoc_table）在此内部构造，与生产逐字一致，
/// 使 harness 也忠实地走同一套 SYN inspector / DNS / relay 调度路径。
pub async fn run_event_loop<D, U, M>(
    mut device: D,
    upstream: Arc<U>,
    mut tuic_downlink_rx: mpsc::Receiver<Vec<u8>>,
    runtime_config: TunRuntimeConfig,
    // 刀11：进程级数据面计数/gauge 句柄（与计时导向的 `metrics: M` 正交）。run_event_loop 写
    // dns_*/relays_spawned + 30s tick 发布 gauge；同一 Arc 也被 TuicUpstream::start_udp clone（写 UDP 计数）。
    metrics_handle: Arc<Metrics>,
    mut metrics: M,
) where
    D: TunIo,
    U: ProxyUpstream + DatagramUpstream + 'static,
    M: MetricsSink,
{
    let pool_size = runtime_config.listener.pool_size;

    // 全局回信通道（TCP relay 通用回程）：接收端 global_rx 留在主循环，发送端 global_tx 克隆给每个后台车厢。
    let (global_tx, mut global_rx) =
        tokio::sync::mpsc::channel::<(SocketHandle, RelayEvent)>(RELAY_CHANNEL_CAPACITY);

    // 刀9 M3 / 刀14d：spawn 出主循环的 remote-open 完成后经此回灌主循环安装 relay。
    // 明确 cheap 的测试/mock 路径仍可 inline，不走此通道。
    let (handshake_done_tx, mut handshake_done_rx) =
        tokio::sync::mpsc::channel::<HandshakeDone>(HANDSHAKE_DONE_CAPACITY);

    // =========== 初始化 smoltcp 酒店和路由器 ===========
    let mut sockets = SocketSet::new(vec![]);

    // Stage 9: 监听端口不再固定，由 SYN inspector 在 rx 热路径按需注册。
    // 中文要点：启动时 registry 是空的；第一条到任意端口的 SYN 会触发该端口建池。
    let mut registry = ListenerRegistry::new(pool_size);
    let mut socket_ctxs: HashMap<SocketHandle, SocketCtx> = HashMap::new();

    // #1 脏集合驱动：只有「本 tick 有活动」的 listener handle 进集合，relay 段仅处理它们，
    // 替代每 tick O(总槽) 全量 sweep。入集 = inbound TCP 包标脏其端口 pool / 回程残留下行 pending；
    // 出集 = 处理后无 pending、不再 can_recv、且没有待发送本地 FIN（见 process_dirty_relay）。
    let mut dirty: HashSet<SocketHandle> = HashSet::new();

    // 刀5: fake-IP DNS 不再用 smoltcp socket。任意 resolver 的明文 :53 在 rx 热路径被
    // classify_inbound 判 Dns → handle_dns_hijack 裸包伪造 fake-IP 回包（绕过 smoltcp，
    // 源 = 被查询的 resolver，故不依赖系统 DNS 指向 198.18.0.1，见 ADR-0007）。
    let mut fake_pool = FakeIpPool::new();

    // 3. 初始化 smoltcp 的“虚拟路由器”
    let config = SmolConfig::new(smoltcp::wire::HardwareAddress::Ip);
    // 这里传入了包装好的 &mut device
    let mut iface = Interface::new(config, &mut device, smoltcp::time::Instant::now());

    // 给虚拟路由器配置 IP 地址 (10.0.0.1/24)
    iface.update_ip_addrs(|ip_addrs| {
        ip_addrs
            .push(IpCidr::new(IpAddress::v4(10, 0, 0, 2), 24))
            .unwrap();
        // 刀5：不再把 198.18.0.1/32 配成接口 IP——DNS 回包改由 handle_dns_hijack 裸包注入
        // （src = 被查询的 resolver），不经 smoltcp 选源，故无需本接口持有 resolver 地址。
    });

    // AnyIP：接收目的 IP 不是本接口自身地址的包（即被拦截连接真正想去的 Target）。
    // 中文要点：默认路由的网关填本接口自己的 IP 10.0.0.2 是 smoltcp AnyIP 接收判定的
    // 硬性要求（routes.lookup(dst) 必须命中一个本接口 IP 才放行），不是笔误。
    iface.set_any_ip(true);
    iface
        .routes_mut()
        .add_default_ipv4_route(smoltcp::wire::Ipv4Address::new(10, 0, 0, 2))
        .unwrap();

    // 初始化定时器 (每 5 毫秒触发一次)
    let mut timer = tokio::time::interval(std::time::Duration::from_millis(5));

    // Stage 13b：UDP over TUIC Packet。AssocTable 主循环独占；upstream 与下行 rx 由调用方注入
    // （生产=真 TuicUpstream::start_udp()；harness=mock echo 回环）。
    let mut assoc_table = AssocTable::new();
    // 刀3：native 下行分片重组器（主循环独占、无锁，与 AssocTable 同寿）。
    let mut reassembler = FragReassembler::new();
    let udp_clock = std::time::Instant::now();
    let mut udp_sweep = tokio::time::interval(std::time::Duration::from_secs(1));
    // review #7：fake-IP 池回收单独走低频 tick（TTL=300s，无需每秒全表扫）。
    let mut fake_ip_sweep = tokio::time::interval(std::time::Duration::from_secs(60));
    // 刀11：数据面可观测性快照 tick（30s）。在此**单写者** task 重算 loop-owned gauge
    // （active_relays / fake-IP 用量 / failover 当班腿）→ 发布进 Arc<Metrics> → 打统一 📊 快照行。
    // 与 TuicUpstream::start_udp 的 UDP-path 📊 行（RTT/cwnd/背压）是两条独立日志、各司其职（见 ADR-0012 §5）。
    let mut metrics_tick =
        tokio::time::interval(std::time::Duration::from_secs(runtime_config.metrics_secs));
    let mut tcp_loop_flush_tx_calls: u64 = 0;
    let mut tcp_loop_flush_tx_failures: u64 = 0;

    loop {
        tokio::select! {
            // TCP relay 回程：后台车厢把远端回传字节送回主循环 → 注入对应 smoltcp socket。
            //   TUIC 自重连（live_conn），不需要 legacy 的 disconnect/复位分支。
            Some((handle, event)) = global_rx.recv() =>{
                metrics.loop_park_end();
                match event {
                    RelayEvent::Data { epoch, bytes: payload } => {
                        trace_log!("📬 从大邮筒收到 {} 字节数据，准备送往房间 {:?}", payload.len(), handle);
                        if let Err(e) = handle_remote_payload(
                            handle,
                            epoch,
                            payload,
                            &mut sockets,
                            &mut socket_ctxs,
                            &mut iface,
                            &mut device,
                            &mut fake_pool,
                            udp_clock.elapsed().as_secs(),
                        )
                        .await
                        {
                            trace_log!("处理回程数据失败: {e}");
                        }
                        // #1：回程写不下的下行字节留在 downlink_pending（tx buffer 满）→ 标脏，
                        // 让 relay 段后续 tick 持续 flush，直到排空才出集（绝不丢字节）。
                        if socket_ctxs
                            .get(&handle)
                            .map(|c| !c.downlink_pending.is_empty())
                            .unwrap_or(false)
                        {
                            dirty.insert(handle);
                        }
                    }
                    RelayEvent::Closed(close) => {
                        if handle_relay_closed(
                            handle,
                            close,
                            &mut sockets,
                            &mut socket_ctxs,
                            &mut fake_pool,
                            udp_clock.elapsed().as_secs(),
                        ) {
                            dirty.insert(handle);
                        } else {
                            dirty.remove(&handle);
                        }
                    }
                }
            }
            // 分支 1: 全局回信通道接收到了新数据包
            // 分支 1: 物理网卡接收到了新数据包
            res = device.wait_for_rx() =>{
                metrics.loop_park_end();
                if res.is_ok(){
                    // rx 分流（stage-12 D1 + 刀5）：任意 :53 → 裸包 DNS 劫持；其它 UDP → 裸 relay；
                    // 非 UDP → 既有 smoltcp 路径。前两类 take 走、不进 iface.poll。
                    let class = device.rx_peek().map(classify_inbound);
                    if class == Some(Inbound::UdpRelay) {
                        if let Some(pkt) = device.rx_take() {
                            // Stage 13b: UDP → 编码 TUIC Packet → send_udp。
                            handle_tuic_udp_uplink(
                                &pkt,
                                &mut assoc_table,
                                &mut fake_pool,
                                &*upstream,
                                udp_clock.elapsed().as_secs(),
                            )
                            .await;
                        }
                    } else if class == Some(Inbound::Dns) {
                        // 刀5：任意 resolver 的明文 :53 → 裸包伪造 fake-IP 回包（绕过 smoltcp）。
                        if let Some(pkt) = device.rx_take() {
                            handle_dns_hijack(
                                &pkt,
                                &mut fake_pool,
                                &mut device,
                                udp_clock.elapsed().as_secs(),
                                &metrics_handle,
                            )
                            .await;
                        }
                    } else {
                        // 1) SYN inspector + 脏集合标脏：在 iface.poll 之前看一眼包。
                        //    - 干净 SYN 去往新端口 → 立刻建监听池，smoltcp 同一帧就能 accept。
                        //    - 任意去往拦截端口的 TCP 包 → 把该端口 pool 标脏（#1），覆盖 SYN 之后
                        //      让 listener can_recv 的首个 data 包；relay 段只处理脏集合，不再全扫。
                        if let Some(buf) = device.rx_peek()
                            && let Some((port, is_clean_syn)) = inspect_inbound_tcp(buf)
                        {
                            if is_clean_syn {
                                if let Err(e) =
                                    registry.ensure_port(port, &mut sockets, &mut socket_ctxs)
                                {
                                    println!(
                                        "⚠️ intercepted port cap reached, drop SYN to port {port}: {:?}",
                                        e
                                    );
                                }
                                // #2 弹性扩容：SYN accept 前确保该端口有空闲 listening 槽吸收突发，
                                // 打掉「每端口 pool_size 固定上限」导致的热门端口 stall。全局 cap 兜底。
                                if let Err(e) = registry.ensure_spare_listeners(
                                    port,
                                    MIN_SPARE_LISTENERS,
                                    &mut sockets,
                                    &mut socket_ctxs,
                                ) {
                                    println!(
                                        "⚠️ global listener cap reached, 端口 {port} 无法弹性扩容: {:?}",
                                        e
                                    );
                                }
                            }
                            // 任意去往拦截端口的 TCP 包 → 标脏该端口 pool（覆盖 SYN 之后的首个 data 包）。
                            for &h in registry.handles_for_port(port) {
                                dirty.insert(h);
                            }
                        }

                        metrics.enter_poll();
                        let timestamp = smoltcp::time::Instant::now();
                        iface.poll(timestamp, &mut device, &mut sockets);
                        tcp_loop_flush_tx_calls += 1;
                        if let Err(e) = device.flush_tx().await {
                            tcp_loop_flush_tx_failures += 1;
                            tcp_diag_log!(
                                "🔎 tcp-loop-flush-tx-fail stage=inbound_poll calls={} failures={} err={e}",
                                tcp_loop_flush_tx_calls, tcp_loop_flush_tx_failures
                            );
                        }
                        metrics.leave_poll();

                        process_dirty_relay(
                            &mut dirty,
                            &mut sockets,
                            &mut socket_ctxs,
                            &upstream,
                            &handshake_done_tx,
                            &global_tx,
                            &mut fake_pool,
                            udp_clock.elapsed().as_secs(),
                            &metrics_handle,
                            &mut metrics,
                        )
                        .await;
                    }
                }
            }
            // 刀9 M3 / 刀14d：spawn 出主循环的 remote-open 完成 → 安装 relay（成功）/ rearm（失败），
            // 带 epoch 防串话。
            Some(done) = handshake_done_rx.recv() => {
                metrics.loop_park_end();
                handle_handshake_done(
                    done,
                    &mut sockets,
                    &mut socket_ctxs,
                    &global_tx,
                    &mut fake_pool,
                    udp_clock.elapsed().as_secs(),
                    &metrics_handle,
                );
            }
            // Stage 13b/刀3: TUIC 下行（datagram 或 uni-stream）→ decode_packet_meta → 分片重组
            // → AssocTable 解路由 → 造回程 IP/UDP 注入 TUN。FRAG_TOTAL==1 直通；>1 集齐才注入。
            Some(dg) = tuic_downlink_rx.recv() => {
                metrics.loop_park_end();
                if let Some(meta) = decode_packet_meta(&dg) {
                    let assoc_id = meta.assoc_id;
                    // 分片重组：单帧直通；多帧集齐返回整包，否则缓存等后续帧。
                    if let Some(payload) = reassembler.accept(&meta, udp_clock.elapsed().as_secs()) {
                        // 先取出路由信息(Copy),释放 assoc_table 借用后再 touch。
                        let routed = assoc_table
                            .resolve(assoc_id)
                            .map(|e| (e.target_src(), e.app_endpoint()));
                        if let Some((src, dst)) = routed {
                            let pkt = build_udp_ip_packet(src, dst, &payload);
                            device.inject_ip_packet(&pkt);
                            assoc_table.touch(assoc_id, udp_clock.elapsed().as_secs());
                            if let Err(e) = device.flush_tx().await {
                                trace_log!("UDP 下行 flush 失败: {e}");
                            }
                        } else {
                            // assoc 已回收/未知 → 丢弃该回程(应用会重发/重查,自愈)。
                            trace_log!("🗑️ TUIC UDP↓ assoc={assoc_id} 无映射，丢弃 {}B", payload.len());
                        }
                    }
                }
            }
            // Stage 13b: 周期回收空闲 UDP assoc。刀2：同时回收 fake-IP 引用 + sweep fake-IP 池。
            _ = udp_sweep.tick() => {
                metrics.loop_park_end();
                let now = udp_clock.elapsed().as_secs();
                // 被回收的 UDP assoc → release 其占用的 fake-IP（引用计数归零，进可回收候选）。
                for ip in assoc_table.sweep(now, UDP_FLOW_IDLE_SECS) {
                    fake_pool.release(ip, now);
                }
                // 刀3：回收未集齐且超时的下行分片包（丢片自愈，防内存泄漏）。
                reassembler.sweep(now, crate::tuic::FRAG_REASSEMBLY_TTL_SECS);
                // review #1/#2：回收已死/卡住的 TCP listener 槽（本地关闭/开远端失败的 teardown 缺口），
                // 释放其 fake-IP refcount 并让槽回 Listen 复用，防 refcount 泄漏 + 槽数涨到 Capped。
                reap_dead_slots(&registry, &mut sockets, &mut socket_ctxs, &mut fake_pool, now);
            }
            // review #7：低频回收 idle 且 refcount==0 超 TTL 的 fake-IP 映射（长稳防泄漏）。
            _ = fake_ip_sweep.tick() => {
                metrics.loop_park_end();
                fake_pool.sweep(udp_clock.elapsed().as_secs(), FAKE_IP_TTL);
            }
            // 刀11：数据面可观测性快照（30s）。在此单写者 task 重算 loop-owned gauge → 发布进
            // Arc<Metrics> → snapshot 读出（上行计数经 upstream trait 访问器）→ **无门控**打统一 📊 行
            // （TCP/DNS/failover 在 UDP 空闲时也有意义；UDP-path 行另由 start_udp 发，见 ADR-0012 §5）。
            _ = metrics_tick.tick() => {
                metrics.loop_park_end();
                publish_gauges(
                    &metrics_handle,
                    socket_ctxs.values(),
                    &fake_pool,
                    upstream.failover_leg_u8(),
                );
                let snap = metrics_handle
                    .snapshot(upstream.udp_drops_up(), upstream.udp_stream_fallbacks());
                println!("{}", crate::metrics::format_metrics_snapshot(&snap));
                tcp_diag_log!(
                    "🔎 tcp-loop-flush-tx calls={} failures={}",
                    tcp_loop_flush_tx_calls, tcp_loop_flush_tx_failures
                );
                // 刀12：紧挨 📊 行打 🔬 主循环归因行（profiler 关闭时 NoopSink::report 空、零开销）。
                metrics.report();
            }
            // 分支 2: 时钟滴答，处理超时重传等后台任务
            _ = timer.tick() =>{
                metrics.loop_park_end();
                metrics.enter_poll();
                let timestamp = smoltcp::time::Instant::now();
                iface.poll(timestamp, &mut device, &mut sockets);
                tcp_loop_flush_tx_calls += 1;
                if let Err(e) = device.flush_tx().await {
                    tcp_loop_flush_tx_failures += 1;
                    tcp_diag_log!(
                        "🔎 tcp-loop-flush-tx-fail stage=timer_poll calls={} failures={} err={e}",
                        tcp_loop_flush_tx_calls, tcp_loop_flush_tx_failures
                    );
                }
                metrics.leave_poll();

                // #1：timer tick 无新 inbound 包，只续推进脏集合（主要是下行 pending flush +
                // smoltcp 超时重传释放 tx buffer 后继续写）。不再全量 sweep。
                process_dirty_relay(
                    &mut dirty,
                    &mut sockets,
                    &mut socket_ctxs,
                    &upstream,
                    &handshake_done_tx,
                    &global_tx,
                    &mut fake_pool,
                    udp_clock.elapsed().as_secs(),
                    &metrics_handle,
                    &mut metrics,
                )
                .await;
            }
        }
        // 刀12：循环底部——即将停在 select! 空等下一个事件，标记 park 开始
        //（NoopSink::loop_park_begin 空、零开销；仅 LoopProfiler 采时钟）。
        metrics.loop_park_begin();
    }
}

/// #1 脏集合驱动的 relay 调度段：只处理本 tick 标脏的 handle，替代每 tick 全量 `all_handles()`。
///
/// 中文要点：把 relay 段成本从 O(总 listener 槽数) 降到 O(活跃 handle)。处理完一个 handle 后，
/// 若它无下行 pending、smoltcp 侧不再 `can_recv`、且没有本地 FIN 待发（首包已 drain、已开远端进 Relaying），
/// 就出脏集合——后续回程走 `global_rx` 分支，残留 pending 或 FIN 重试时会留脏。仍有活就留在集合里下个 tick 续处理。
#[allow(clippy::too_many_arguments)]
async fn process_dirty_relay<U, M>(
    dirty: &mut HashSet<SocketHandle>,
    sockets: &mut SocketSet<'_>,
    socket_ctxs: &mut HashMap<SocketHandle, SocketCtx>,
    upstream: &Arc<U>,
    handshake_done_tx: &mpsc::Sender<HandshakeDone>,
    global_tx: &mpsc::Sender<(SocketHandle, RelayEvent)>,
    fake_pool: &mut FakeIpPool,
    now_secs: u64,
    metrics_handle: &Metrics,
    metrics: &mut M,
) where
    U: ProxyUpstream + 'static,
    M: MetricsSink,
{
    metrics.enter_relay();
    // 快照后处理：边遍历边 `dirty.remove` 会与迭代借用冲突。dirty 规模 = O(活跃)，分配可忽略。
    let snapshot: Vec<SocketHandle> = dirty.iter().copied().collect();
    metrics.note_listeners(snapshot.len());
    for handle in snapshot {
        if let Err(e) = process_listener_activity(
            handle,
            sockets,
            socket_ctxs,
            upstream,
            handshake_done_tx,
            global_tx,
            fake_pool,
            now_secs,
            metrics_handle,
        )
        .await
        {
            trace_log!("处理本地房间 {:?} 失败: {e}", handle);
        }
        let still_active = {
            let (has_recv, tcp_state) = {
                let socket = sockets.get_mut::<TcpSocket>(handle);
                (socket.can_recv(), socket.state())
            };
            let (has_pending, needs_local_finish) = socket_ctxs
                .get(&handle)
                .map(|c| {
                    (
                        !c.downlink_pending.is_empty(),
                        tcp_state == TcpState::CloseWait
                            && c.uplink_tx.is_some()
                            && !c.local_fin_sent,
                    )
                })
                .unwrap_or((false, false));
            has_recv || has_pending || needs_local_finish
        };
        if !still_active {
            dirty.remove(&handle);
        }
    }
    metrics.leave_relay();
}

/// 回收「已用过但已死/卡住」的 listener 槽（review #1/#2 修复）：本地 FIN/RST 关闭、双向关闭完成、
/// 或开远端失败卡住的槽，热路径的 rearm 只在「远端 EOF / Refuse」触发，覆盖不到这些路径——
/// 不回收则 ① 它持有的 fake-IP refcount 永不归零 → 映射永不被 sweep 回收（泄漏）；
/// ② 槽停在非 Listen，`ensure_spare_listeners` 不断新建 → `total_handles` 涨到 `MAX_TOTAL_LISTENERS`
/// → Capped → #2 修好的热门端口 stall 又回来。低频（1s tick）调用，非每包热路径。
///
/// 死槽判定（仅对「被用过」的槽，即 `ctx.state != Listening`；空闲 Listen 槽 ctx.state==Listening 永不命中）：
/// - `!is_active()`：Closed / TimeWait（RST、双向关闭完成）；**但**若 relay close 已延迟且仍有
///   `downlink_pending`，先给 dirty flush 一个短 grace 窗口，过期才 hard reap。此例外覆盖
///   Knife14n 看到的 `state=Closing pending>0` 被 sweep 抢先清尾包。
///   其余 inactive pending 仍按旧规则回收，防不可交付的残留永久占槽。也覆盖 `HandshakePending` 槽
///   在本地已关闭时**（remote-open 在飞但 app 已断）——回收时 rearm bump epoch，迟到的 `HandshakeDone` 被丢；
/// - `CloseWait` 且无 pending downlink、无 live relay：被拦截应用已结束发送半边，且远端 relay
///   也已卸下 → teardown；若 relay 仍活着，即使当前 pending 为空，也可能还有 reverse/downlink 字节稍后到达；
///   若仍有 `downlink_pending`，说明远端尾包已被接受但尚未全部写入 smoltcp，必须先让 dirty flush 排空；
/// - `OpeningRemote && uplink_tx.is_none()`：**inline** `open_tcp` 失败后状态卡在 OpeningRemote。
///   中文要点：此第三判据**仅匹配 `OpeningRemote`**（inline 路径），**不**匹配 `HandshakePending`
///   ——后者是 async remote-open 的**正常在飞态**（由上游超时兜底，`HandshakeDone` 必回灌解决），绝不能在飞就回收。
fn reap_dead_slots(
    registry: &ListenerRegistry,
    sockets: &mut SocketSet<'_>,
    socket_ctxs: &mut HashMap<SocketHandle, SocketCtx>,
    fake_pool: &mut FakeIpPool,
    now_secs: u64,
) -> usize {
    let handles: Vec<SocketHandle> = registry.all_handles().collect();
    let mut reaped = 0;
    for h in handles {
        let dead = {
            let s = sockets.get::<TcpSocket>(h);
            let st = s.state();
            let active = s.is_active();
            socket_ctxs
                .get(&h)
                .map(|ctx| should_reap_slot(ctx, st, active, now_secs))
                .unwrap_or(false)
        };
        if dead {
            let sock = sockets.get_mut::<TcpSocket>(h);
            if let Some(ctx) = socket_ctxs.get_mut(&h) {
                rearm_socket_with_reason(
                    h,
                    sock,
                    ctx,
                    fake_pool,
                    now_secs,
                    "local",
                    "dead_slot_reap",
                );
                reaped += 1;
            }
        }
    }
    reaped
}

fn should_reap_slot(ctx: &SocketCtx, tcp_state: TcpState, active: bool, now_secs: u64) -> bool {
    if ctx.state == SocketState::Listening {
        return false;
    }
    if ctx.pending_relay_close.is_some() && !ctx.downlink_pending.is_empty() {
        let Some(since) = ctx.pending_relay_close_since_secs else {
            return false;
        };
        return now_secs.saturating_sub(since) >= DEFERRED_CLOSE_PENDING_GRACE_SECS;
    }
    if !active {
        return true;
    }
    if !ctx.downlink_pending.is_empty() {
        return false;
    }
    if tcp_state == TcpState::CloseWait {
        return ctx.uplink_tx.is_none() && !ctx.local_fin_sent;
    }
    ctx.state == SocketState::OpeningRemote && ctx.uplink_tx.is_none()
}

fn relay_allows_remote_payload(ctx: &SocketCtx) -> bool {
    ctx.uplink_tx.is_some() || ctx.local_fin_sent
}

/// Allocate a fresh smoltcp TCP listener socket for one pool slot.
/// 中文要点：每次调用都创建一间独立房间，并立即挂上 listen 牌子。
fn build_listener_socket(spec: &ListenerSpec) -> TcpSocket<'static> {
    let tcp_rx_buffer = TcpSocketBuffer::new(vec![0; TCP_SOCKET_BUFFER_SIZE]);
    let tcp_tx_buffer = TcpSocketBuffer::new(vec![0; TCP_SOCKET_BUFFER_SIZE]);
    let mut tcp_socket = TcpSocket::new(tcp_rx_buffer, tcp_tx_buffer);
    tcp_socket.listen(spec.local_port).unwrap();
    tcp_socket
}

/// 解析入站 IPv4+TCP 包：返回 `(目的端口, 是否干净 SYN)`。一次解析同时供 SYN 建池与脏集合标脏。
/// 中文要点：review #5——原 `inspect_inbound_syn` + `inbound_tcp_dst_port` 对同一包解析两遍，
/// 每个入站 TCP 包在热路径白跑一次 etherparse。合并为一次解析：`is_clean_syn = syn && !ack`
/// 用于建池/扩容；端口对任意 TCP 包都返回，用于标脏（覆盖 SYN 之后让 listener can_recv 的首个 data 包）。
/// 非 IPv4 / 非 TCP / 解析失败 → None。
fn inspect_inbound_tcp(packet: &[u8]) -> Option<(u16, bool)> {
    let parsed = etherparse::PacketHeaders::from_ip_slice(packet).ok()?;
    let etherparse::TransportHeader::Tcp(tcp) = parsed.transport? else {
        return None;
    };
    Some((tcp.destination_port, tcp.syn && !tcp.ack))
}

/// Convert a smoltcp endpoint into a relay Target.
/// 中文要点：TUN 链路上目的地址在 IP 层已是裸 IP（域名早被 DNS 解析掉），
/// 这里统一转成 `TargetAddr::IpPort`。当前 crate 只开 `proto-ipv4`，故必为 IPv4。
fn target_from_endpoint(endpoint: smoltcp::wire::IpEndpoint) -> TargetAddr {
    let ip = std::net::IpAddr::from(endpoint.addr);
    TargetAddr::IpPort(std::net::SocketAddr::new(ip, endpoint.port))
}

/// fake-IP target 改写结果。
enum TargetResolve {
    /// 正常转发：IpPort 直连（`fake_ip=None`），或 fake-IP 查回的 DomainPort（`fake_ip=Some`）。
    /// 中文要点：刀2 透出 fake_ip，供上层在首开远端时 `acquire`、rearm 时 `release`（引用计数回收）。
    Direct {
        target: TargetAddr,
        fake_ip: Option<Ipv4Addr>,
    },
    /// fake-IP 段内但查不到映射（如客户端重启丢表、应用用旧缓存 IP）：拒绝，让应用重查。
    Refuse,
    /// 加密 DNS 端点（DoT/DoQ :853、DoH/DoH3 :443 命中名单）：阻断，逼应用回落明文 DNS。
    /// 中文要点（刀4）：TCP 发 RST（rearm）、UDP 丢包；应用回落 :53 → 我方伪造 fake-IP → 进隧道。
    Block,
}

/// 把提取出的 endpoint 解析成 relay target。
/// 中文要点：fake-IP → 查表得域名 → DomainPort（出口解析、绕污染）；非 fake → IpPort
/// （Stage 8/9 行为不变）；fake 但无映射 → Refuse（拒绝连接）。
fn resolve_target(endpoint: smoltcp::wire::IpEndpoint, fake_pool: &FakeIpPool) -> TargetResolve {
    // 刀4/刀5：DNS 端口拦截(先于常规解析)。:853 = DoT/DoQ(任意 IP)→ Block；
    // :53 = TCP 明文 DNS(UDP :53 已被 classify 截到劫持路径、不到此)→ Block(RST 逼回落 UDP :53)。
    if crate::dns_block::is_dns_relay_port(endpoint.port) {
        return TargetResolve::Block;
    }
    let std::net::IpAddr::V4(v4) = std::net::IpAddr::from(endpoint.addr) else {
        return TargetResolve::Direct {
            target: target_from_endpoint(endpoint),
            fake_ip: None,
        };
    };
    if fake_pool.is_fake(v4) {
        match fake_pool.resolve(v4) {
            Some(domain) => {
                // 刀4：DoH/DoH3 经 fake-IP——:443 且域名命中 DoH 名单 → Block（不碰普通 :443）。
                if endpoint.port == 443 && crate::dns_block::is_doh_domain(&domain) {
                    return TargetResolve::Block;
                }
                // 不在此 println!——resolve_target 在每个 UDP 包/每条 TCP 首包都会走到，
                // 热路径同步 stdout 会拖垮大并发。flow 创建的可观测性放在服务端日志。
                TargetResolve::Direct {
                    target: TargetAddr::DomainPort {
                        host: domain,
                        port: endpoint.port,
                    },
                    fake_ip: Some(v4),
                }
            }
            None => TargetResolve::Refuse,
        }
    } else {
        // 刀4：DoH/DoH3 硬编 bootstrap IP——:443 且 IP 命中 DoH-IP 名单 → Block。
        if endpoint.port == 443 && crate::dns_block::is_doh_ip(v4) {
            return TargetResolve::Block;
        }
        TargetResolve::Direct {
            target: target_from_endpoint(endpoint),
            fake_ip: None,
        }
    }
}

/// 刀5：把发往**任意** resolver 的明文 DNS 查询，本地伪造成 fake-IP 回包（裸包构造）。
/// 中文要点：A 查询 → 分配 fake-IP 并回伪造 A 记录；AAAA/其它 → NODATA；不可解析 → `None`
/// （调用方丢弃，**绝不转发真 DNS**——转发即泄漏真实 IP，绕过 fake-IP）。回包源 = app 当初查询的
/// resolver（`udp.dst_ip:53`），目的 = app 原端点；否则 app 的 socket 认不出回包而丢弃。裸包能任意
/// 设 src（smoltcp 受限于本接口 IP、对无界 resolver 集合做不到，见 ADR-0007）。纯逻辑（只依赖
/// `UdpInbound` + `&mut FakeIpPool`），无 device/async，便于单测。
fn forge_dns_reply(udp: &UdpInbound<'_>, fake_pool: &mut FakeIpPool, now_secs: u64) -> Option<Vec<u8>> {
    let q = dns::parse_query(udp.payload)?;
    let resp = if q.qtype == dns::QTYPE_A {
        let ip = fake_pool.alloc(&q.qname, now_secs);
        println!("🪪 DNS {} (A) → fake-IP {}", q.qname, ip);
        dns::build_response(&q, Answer::A(ip, 5))
    } else {
        let kind = if q.qtype == dns::QTYPE_AAAA {
            "AAAA"
        } else {
            "other"
        };
        println!("🪪 DNS {} ({}) → NODATA", q.qname, kind);
        dns::build_response(&q, Answer::NoData)
    };
    // 回包：src = 被查询的 resolver:53，dst = app 原端点（src/dst 对调）。
    Some(build_udp_ip_packet(
        (udp.dst_ip, 53),
        (udp.src_ip, udp.src_port),
        &resp,
    ))
}

/// 刀5：rx 热路径的裸包 DNS 劫持薄壳——解析入站 :53 包 → `forge_dns_reply` → 注入回包到 TUN。
/// 中文要点：`forge_dns_reply` 返回 `None`（不可解析）→ 静默丢弃（app 重查自愈，绝不转发真 DNS）。
/// 与 UDP relay 下行注入同款（`inject_ip_packet` + `flush_tx`）；泛型 `D: TunIo` 使生产/harness 共用。
async fn handle_dns_hijack<D: TunIo>(
    pkt: &[u8],
    fake_pool: &mut FakeIpPool,
    device: &mut D,
    now_secs: u64,
    metrics: &Metrics,
) {
    let Some(udp) = parse_inbound_udp(pkt) else {
        // 生产里 classify_inbound 已保证 parse 成功才路由到 Dns → 此早返在生产中实为 dead。
        // **不计数**（避免与下方 forge None 的 dns_dropped 双计；防御性早返不该污染指标）。
        return;
    };
    if let Some(reply) = forge_dns_reply(&udp, fake_pool, now_secs) {
        metrics.inc_dns_forged();
        device.inject_ip_packet(&reply);
        if let Err(e) = device.flush_tx().await {
            println!("DNS 劫持回包 flush 失败: {e}");
        }
    } else {
        // 不可解析查询：静默丢（forge_dns_reply 已记 app 重查自愈），计入 dns_dropped。
        metrics.inc_dns_dropped();
    }
}

/// Drain the currently available local payload from one listener slot.
/// 中文要点：这里只负责把 smoltcp 缓冲区里的数据取出来，不做任何异步外联动作。
fn extract_socket_payload(socket: &mut TcpSocket<'_>) -> Option<Vec<u8>> {
    if !socket.can_recv() {
        return None;
    }

    let mut payload = None;
    socket
        .recv(|data| {
            payload = Some(data.to_vec());
            (data.len(), ())
        })
        .unwrap();
    payload
}

fn rearm_socket_with_reason(
    handle: SocketHandle,
    socket: &mut TcpSocket<'_>,
    ctx: &mut SocketCtx,
    fake_pool: &mut FakeIpPool,
    now_secs: u64,
    close_direction: &'static str,
    close_reason: &'static str,
) {
    tcp_diag_log!(
        "🔎 tcp-handle-close handle={:?} direction={} reason={} state={:?} pending={} pending_high={} remote_to_global_rx_bytes={} send_slice_calls={} send_slice_accepted={} send_slice_zero={} send_slice_errors={} tun_flush_tx_calls={} tun_flush_tx_failures={}",
        handle,
        close_direction,
        close_reason,
        ctx.state,
        ctx.downlink_pending.len(),
        ctx.downlink_diag.downlink_pending_high_water,
        ctx.downlink_diag.remote_to_global_rx_bytes,
        ctx.downlink_diag.send_slice_calls,
        ctx.downlink_diag.send_slice_accepted_bytes,
        ctx.downlink_diag.send_slice_zero,
        ctx.downlink_diag.send_slice_errors,
        ctx.downlink_diag.tun_flush_tx_calls,
        ctx.downlink_diag.tun_flush_tx_failures
    );
    rearm_socket(socket, ctx, fake_pool, now_secs);
}

/// Reset a slot back into the listening state after the current relay ends.
/// 中文要点：单个 handle 退房只影响自己，不能误清理其他房间的状态。
fn rearm_socket(
    socket: &mut TcpSocket<'_>,
    ctx: &mut SocketCtx,
    fake_pool: &mut FakeIpPool,
    now_secs: u64,
) {
    ctx.state = SocketState::Closing;
    socket.abort();
    ctx.uplink_tx = None;
    ctx.local_fin_sent = false;
    ctx.downlink_pending.clear();
    ctx.pending_relay_close = None;
    ctx.pending_relay_close_since_secs = None;
    ctx.downlink_diag = TcpDownlinkDiag::default();
    // 清 async-open 在飞缓存 + bump epoch——让任何迟到 `HandshakeDone` 失配被丢
    // （绝不装到本次 rearm 后的新一代 socket 上，防串话核心）。对非 spawn 槽是无害的纯计数自增。
    ctx.uplink_buffer.clear();
    ctx.conn_epoch = ctx.conn_epoch.wrapping_add(1);
    // 刀2 引用计数：本 flow 占用的 fake-IP 释放（归零后该映射进入可回收候选，sweep 才回收）。
    if let Some(ip) = ctx.fake_ip.take() {
        fake_pool.release(ip, now_secs);
    }
    ctx.state = SocketState::Rearming;
    socket.listen(ctx.local_port).unwrap();
    ctx.state = SocketState::Listening;
    trace_log!("♻️ handle slot rearmed on local port {}", ctx.local_port);
}

/// Process one listener slot after iface polling.
/// 中文要点：主循环只负责遍历 handle，真正的房间处理逻辑都收口在这里。
#[allow(clippy::too_many_arguments)]
async fn process_listener_activity<U: ProxyUpstream + 'static>(
    handle: SocketHandle,
    sockets: &mut SocketSet<'_>,
    socket_ctxs: &mut HashMap<SocketHandle, SocketCtx>,
    upstream: &Arc<U>,
    handshake_done_tx: &mpsc::Sender<HandshakeDone>,
    global_tx: &mpsc::Sender<(SocketHandle, RelayEvent)>,
    fake_pool: &mut FakeIpPool,
    now_secs: u64,
    metrics_handle: &Metrics,
) -> Result<(), ClientError> {
    // 每轮先推进该 handle 的下行 pending：TCP ACK 释放 tx buffer 空间后继续写，
    // 直到把上一轮没写完的回程字节全部交付，绝不丢字节（修 bad decrypt 的另一半）。
    {
        let tcp_socket = sockets.get_mut::<TcpSocket>(handle);
        if let Some(ctx) = socket_ctxs.get_mut(&handle) {
            flush_downlink(handle, tcp_socket, ctx);
            if finish_deferred_relay_close_if_drained(
                handle,
                tcp_socket,
                ctx,
                fake_pool,
                now_secs,
            ) {
                return Ok(());
            }
        }
    }

    // 刀13 ②：已建立 relay 的上行必须非阻塞。先抢 mpsc permit，再从 smoltcp 取字节；
    // Full 时不 recv、不分配、不打印，让字节留在 smoltcp rx buffer → TCP 窗口自然背压 app。
    // 这消除一条慢上游 flow 在 `tx.send().await` 上 HoL 阻塞整个主循环的问题。
    enum EstablishedUplink {
        NotEstablished,
        Handled,
        Closed,
    }
    let established_uplink = {
        let Some(ctx) = socket_ctxs.get_mut(&handle) else {
            return Ok(());
        };
        if let Some(tx) = ctx.uplink_tx.as_mut() {
            match tx.try_reserve() {
                Ok(permit) => {
                    let (payload, tcp_state) = {
                        let tcp_socket = sockets.get_mut::<TcpSocket>(handle);
                        (extract_socket_payload(tcp_socket), tcp_socket.state())
                    };
                    if let Some(payload) = payload {
                        permit.send(RelayCommand::Data(payload));
                        ctx.state = SocketState::Relaying;
                    } else if tcp_state == TcpState::CloseWait && !ctx.local_fin_sent {
                        permit.send(RelayCommand::Finish);
                        ctx.local_fin_sent = true;
                    }
                    EstablishedUplink::Handled
                }
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => EstablishedUplink::Handled,
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => EstablishedUplink::Closed,
            }
        } else {
            EstablishedUplink::NotEstablished
        }
    };
    match established_uplink {
        EstablishedUplink::Handled => return Ok(()),
        EstablishedUplink::Closed => {
            let tcp_socket = sockets.get_mut::<TcpSocket>(handle);
            if let Some(ctx) = socket_ctxs.get_mut(&handle) {
                if ctx.local_fin_sent {
                    ctx.uplink_tx = None;
                    return Ok(());
                }
                rearm_socket_with_reason(
                    handle,
                    tcp_socket,
                    ctx,
                    fake_pool,
                    now_secs,
                    "local_to_remote",
                    "uplink_channel_closed",
                );
            }
            return Ok(());
        }
        EstablishedUplink::NotEstablished => {}
    }

    // 取首包的同时读 local_endpoint：它就是被拦截连接真正想去的目的 endpoint。
    // 中文要点：两者都需要 socket，合并在这一处借用里读出，避免二次借用。
    let extracted = {
        let tcp_socket = sockets.get_mut::<TcpSocket>(handle);
        let payload = extract_socket_payload(tcp_socket);
        let endpoint = tcp_socket.local_endpoint();
        payload.map(|p| (p, endpoint))
    };

    // 只有"有首包"且"有 local_endpoint"才继续；否则跳过。
    let Some((payload, Some(endpoint))) = extracted else {
        return Ok(());
    };

    // Stage 11：fake-IP → 查表换域名（DomainPort）；非 fake → IpPort；fake 无映射 → 拒绝。
    let (target, fake_ip) = match resolve_target(endpoint, fake_pool) {
        TargetResolve::Direct { target, fake_ip } => (target, fake_ip),
        TargetResolve::Refuse => {
            trace_log!(
                "🚫 fake-IP {} 无映射，拒绝连接（请重新解析）",
                endpoint.addr
            );
            let tcp_socket = sockets.get_mut::<TcpSocket>(handle);
            if let Some(ctx) = socket_ctxs.get_mut(&handle) {
                rearm_socket_with_reason(
                    handle,
                    tcp_socket,
                    ctx,
                    fake_pool,
                    now_secs,
                    "target",
                    "fake_ip_refuse",
                );
            }
            return Ok(());
        }
        // 刀4：加密 DNS（DoT :853 / DoH :443）→ RST（rearm），逼应用回落明文 DNS。
        TargetResolve::Block => {
            // 解析 fake-IP 回域名供日志（low-rate TCP block 路径，便于核对命中端点 / 调 DoH 名单）；
            // :853/DoH-IP（非 fake-IP）则显示 IP。
            let who = match std::net::IpAddr::from(endpoint.addr) {
                std::net::IpAddr::V4(v4) => {
                    fake_pool.resolve(v4).unwrap_or_else(|| endpoint.addr.to_string())
                }
                _ => endpoint.addr.to_string(),
            };
            println!("🛡️ 阻断加密 DNS {who} (@{}:{})（→ RST，逼回落明文 DNS）", endpoint.addr, endpoint.port);
            let tcp_socket = sockets.get_mut::<TcpSocket>(handle);
            if let Some(ctx) = socket_ctxs.get_mut(&handle) {
                rearm_socket_with_reason(
                    handle,
                    tcp_socket,
                    ctx,
                    fake_pool,
                    now_secs,
                    "policy",
                    "encrypted_dns_block",
                );
            }
            return Ok(());
        }
    };

    handle_local_payload(
        handle,
        payload,
        Some(target),
        fake_ip,
        sockets,
        socket_ctxs,
        upstream,
        handshake_done_tx,
        global_tx,
        fake_pool,
        now_secs,
        metrics_handle,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn handle_local_payload<U: ProxyUpstream + 'static>(
    handle: SocketHandle,
    payload: Vec<u8>,
    target: Option<TargetAddr>,
    fake_ip: Option<Ipv4Addr>,
    sockets: &mut SocketSet<'_>,
    socket_ctxs: &mut HashMap<SocketHandle, SocketCtx>,
    upstream: &Arc<U>,
    handshake_done_tx: &mpsc::Sender<HandshakeDone>,
    global_tx: &mpsc::Sender<(SocketHandle, RelayEvent)>,
    fake_pool: &mut FakeIpPool,
    now_secs: u64,
    metrics_handle: &Metrics,
) -> Result<(), ClientError> {
    let Some(ctx) = socket_ctxs.get_mut(&handle) else {
        return Ok(());
    };

    // Async remote-open 在飞中 → 后续上行包入 buffer（防双开 + 保序），不再开第二次 open。
    if ctx.state == SocketState::HandshakePending {
        if !ctx.buffer_uplink(&payload) {
            trace_log!(
                "⚠️ handle {:?} remote open 在飞、上行缓存超上限({}KB)，丢弃 {}B（应用 TCP 背压自愈）",
                handle,
                MAX_UPLINK_BUFFER / 1024,
                payload.len()
            );
        }
        return Ok(());
    }

    // 首次开远端必须有提取出的 Target；理论上首包时连接已 Established，local_endpoint 不应为 None。
    // 中文要点：缺 Target 时记录并跳过，绝不 panic、绝不退回写死地址。
    let Some(target) = target else {
        trace_log!("⚠️ handle {:?} 无 local_endpoint，跳过开远端", handle);
        return Ok(());
    };

    if upstream.open_is_cheap() {
        // —— 明确廉价的上游：inline 开远端（主要用于测试/mock 轻路径）。——
        ctx.state = SocketState::OpeningRemote;
        trace_log!("🎯 handle {:?} extracted target {}", handle, target.to_wire_string());
        trace_log!("🔄 handle {:?} entering {:?}", handle, ctx.state);
        let stream = upstream.open_tcp(&target).await?;
        trace_log!("🚪 handle {:?} remote session opened", handle);

        let (tx, rx) = tokio::sync::mpsc::channel(RELAY_CHANNEL_CAPACITY);
        if let Err(e) = tx.try_send(RelayCommand::Data(payload)) {
            trace_log!("❌ handle {:?} 新建中继通道写入失败({e}) → rearm（不静默丢首包）", handle);
            let tcp_socket = sockets.get_mut::<TcpSocket>(handle);
            rearm_socket_with_reason(
                handle,
                tcp_socket,
                ctx,
                fake_pool,
                now_secs,
                "local_to_remote",
                "initial_uplink_send_failed",
            );
            return Ok(());
        }
        ctx.uplink_tx = Some(tx);
        ctx.state = SocketState::Relaying;
        // 刀2 引用计数：inline 路径开远端成功后才 acquire（开失败走 `?` 早返回，不泄漏 refcount）。
        if let Some(ip) = fake_ip {
            fake_pool.acquire(ip, now_secs);
            ctx.fake_ip = Some(ip);
        }
        spawn_remote_relay(handle, ctx.conn_epoch, stream, rx, global_tx.clone(), metrics_handle);
        Ok(())
    } else {
        // —— 不廉价上游：把远端 TCP open spawn 出主循环并发化（刀9 M3 / 刀14d）——
        // 主循环立即返回处理其它 flow，不被这条慢 open stall。完成后经 handshake_done channel 回灌。
        ctx.state = SocketState::HandshakePending;
        ctx.conn_epoch = ctx.conn_epoch.wrapping_add(1); // 新 remote-open 代次
        let epoch = ctx.conn_epoch;
        // fake-IP 在 spawn 时 acquire（与 inline「成功后才 acquire」不同——spawn 路径无早返回，
        // 由 rearm 在 open 失败/复位时 release，平衡）。首包入 buffer，open 成功后按序 flush。
        if let Some(ip) = fake_ip {
            fake_pool.acquire(ip, now_secs);
            ctx.fake_ip = Some(ip);
        }
        if !ctx.buffer_uplink(&payload) {
            // 理论不可达（spawn 入口 buffer 必空、首包 << 256KB）；防御性对称上面 HandshakePending 分支。
            trace_log!("⚠️ handle {:?} spawn 入口缓存首包失败（{}B），丢弃", handle, payload.len());
        }
        trace_log!("🎯 handle {:?} target {} → spawn remote open（并发化，不 stall 主循环）", handle, target.to_wire_string());

        let up = Arc::clone(upstream);
        let done_tx = handshake_done_tx.clone();
        tokio::spawn(async move {
            let result = up.open_tcp(&target).await; // production open_tcp implementations carry their own timeout budget
            // channel 满 → send().await 背压（不丢，等主循环排空）；主循环已退出 → send 失败、忽略。
            let _ = done_tx.send(HandshakeDone { handle, epoch, result }).await;
        });
        Ok(())
    }
}

/// 处理一次 spawned remote-open 的完成事件。**epoch 防串话置于一切之前**——迟到结果绝不装到新一代 socket。
/// 成功 → 安装 uplink channel + 按序 flush open 期间缓存 + spawn relay；失败 → rearm。
fn handle_handshake_done(
    done: HandshakeDone,
    sockets: &mut SocketSet<'_>,
    socket_ctxs: &mut HashMap<SocketHandle, SocketCtx>,
    global_tx: &mpsc::Sender<(SocketHandle, RelayEvent)>,
    fake_pool: &mut FakeIpPool,
    now_secs: u64,
    metrics_handle: &Metrics,
) {
    let HandshakeDone { handle, epoch, result } = done;
    let Some(ctx) = socket_ctxs.get_mut(&handle) else {
        return; // 槽已不存在：丢弃（Ok 的流随作用域 drop 关闭）
    };
    // 防串话（V2 patch 3）：epoch 比较置于状态检查**之前**。不匹配 = 本槽已 rearm/换代/复位 →
    // 迟到的 open 结果丢弃，绝不装到新一代 socket。Ok 的流随作用域结束 drop 而干净关闭。
    if ctx.conn_epoch != epoch {
        if result.is_ok() {
            trace_log!("🗑️ handle {:?} 迟到 open 结果(epoch {epoch}≠{}) 丢弃，不装到新代 socket", handle, ctx.conn_epoch);
        }
        return;
    }
    // 二次防御：epoch 匹配但状态已非 HandshakePending（理论不该发生）→ 不装。
    if ctx.state != SocketState::HandshakePending {
        return;
    }
    match result {
        Ok(stream) => {
            let (tx, rx) = tokio::sync::mpsc::channel(RELAY_CHANNEL_CAPACITY);
            // 按序 flush open 期间缓存的上行字节（首包 + 在飞期到达的后续包，FIFO）。新 channel 容量充足、
            // rx 未 drop、仅 1 条消息 → try_send 必成（避免 await-with-borrow）。防御性：万一失败（未来
            // 容量被改小等），**不静默丢缓存**——log + rearm 撤掉本槽，让应用 TCP 重建（Finding 2）。
            if !ctx.uplink_buffer.is_empty() {
                let buffered = std::mem::take(&mut ctx.uplink_buffer);
                if let Err(e) = tx.try_send(RelayCommand::Data(buffered)) {
                    trace_log!("❌ handle {:?} flush open 缓存失败({e}) → rearm（不静默丢字节）", handle);
                    let socket = sockets.get_mut::<TcpSocket>(handle);
                    rearm_socket_with_reason(
                        handle,
                        socket,
                        ctx,
                        fake_pool,
                        now_secs,
                        "local_to_remote",
                        "handshake_buffer_flush_failed",
                    );
                    return; // stream 随作用域 drop 干净关闭
                }
            }
            ctx.uplink_tx = Some(tx);
            ctx.state = SocketState::Relaying;
            trace_log!("🚪 handle {:?} remote session opened（spawn open 成功）", handle);
            spawn_remote_relay(handle, epoch, stream, rx, global_tx.clone(), metrics_handle);
        }
        Err(e) => {
            println!("❌ handle {:?} spawned remote open failed: {e} → rearm", handle);
            let socket = sockets.get_mut::<TcpSocket>(handle);
            rearm_socket_with_reason(
                handle,
                socket,
                ctx,
                fake_pool,
                now_secs,
                "remote_open",
                "handshake_failed",
            ); // 释放 spawn 时 acquire 的 fake-IP（平衡）
        }
    }
}

/// 处理远端回信
#[allow(clippy::too_many_arguments)]
async fn handle_remote_payload<D: TunIo>(
    handle: SocketHandle,
    epoch: u64,
    payload: Vec<u8>,
    sockets: &mut SocketSet<'_>,
    socket_ctxs: &mut HashMap<SocketHandle, SocketCtx>,
    iface: &mut Interface,
    device: &mut D,
    fake_pool: &mut FakeIpPool,
    now_secs: u64,
) -> std::io::Result<()> {
    let tcp_socket = sockets.get_mut::<TcpSocket>(handle);
    let Some(ctx) = socket_ctxs.get_mut(&handle) else {
        return Ok(());
    };
    if ctx.conn_epoch != epoch {
        trace_log!(
            "🗑️ handle {:?} 丢弃旧 relay data(epoch {epoch}≠{}) {}B",
            handle,
            ctx.conn_epoch,
            payload.len()
        );
        return Ok(());
    }

    if payload.is_empty() {
        trace_log!("🔄 handle {:?} entering {:?}", handle, SocketState::Closing);
        rearm_socket_with_reason(
            handle,
            tcp_socket,
            ctx,
            fake_pool,
            now_secs,
            "remote_to_local",
            "remote_eof",
        );
        return Ok(());
    }

    // 防串话 / 防 panic（epoch guard 的轻量降级版）：若该 handle 已被重连流程复位回
    // Listening（uplink_tx 被清空），说明这是上一代上游连接的迟到回程数据，直接丢弃，
    // 绝不能往非 Established 的 socket 写（否则 send_slice 报错、旧版本会 unwrap panic）。
    if !relay_allows_remote_payload(ctx) {
        trace_log!(
            "🗑️ handle {:?} 已复位，丢弃旧连接迟到回程 {} 字节",
            handle,
            payload.len()
        );
        return Ok(());
    }

    // 不直接 send_slice（会丢写不下的字节）：先入下行 pending，再尽量 flush；
    // 剩余字节由主循环每轮 poll 持续推进（TCP ACK 释放 buffer 后继续）。
    ctx.downlink_pending.extend_from_slice(&payload);
    ctx.downlink_diag
        .note_remote_payload(payload.len(), ctx.downlink_pending.len());
    flush_downlink(handle, tcp_socket, ctx);
    ctx.state = SocketState::Relaying;

    let timestamp = smoltcp::time::Instant::now();
    iface.poll(timestamp, device, sockets);
    let result = device.flush_tx().await;
    let ok = result.is_ok();
    if let Some(ctx) = socket_ctxs.get_mut(&handle) {
        ctx.downlink_diag.note_tun_flush(ok);
    }
    if let Err(e) = &result {
        tcp_diag_log!("🔎 tcp-tun-flush-fail handle={:?} stage=remote_payload err={e}", handle);
    }
    result
}

fn handle_relay_closed(
    handle: SocketHandle,
    close: RelayClose,
    sockets: &mut SocketSet<'_>,
    socket_ctxs: &mut HashMap<SocketHandle, SocketCtx>,
    fake_pool: &mut FakeIpPool,
    now_secs: u64,
) -> bool {
    let Some(ctx) = socket_ctxs.get_mut(&handle) else {
        return false;
    };
    if ctx.conn_epoch != close.epoch {
        trace_log!(
            "🗑️ handle {:?} relay close({}/{}) epoch {}≠{}; ignored",
            handle,
            close.direction,
            close.reason,
            close.epoch,
            ctx.conn_epoch
        );
        return false;
    }
    if ctx.state == SocketState::Listening && ctx.uplink_tx.is_none() {
        trace_log!(
            "🗑️ handle {:?} relay close({}/{}) arrived after rearm; ignored",
            handle,
            close.direction,
            close.reason
        );
        return false;
    }
    if !ctx.downlink_pending.is_empty() {
        trace_log!(
            "⏳ handle {:?} relay close({}/{}) waits for {} pending downlink bytes",
            handle,
            close.direction,
            close.reason,
            ctx.downlink_pending.len()
        );
        ctx.state = SocketState::Closing;
        ctx.uplink_tx = None;
        ctx.pending_relay_close = Some(close);
        ctx.pending_relay_close_since_secs = Some(now_secs);
        return true;
    }

    let tcp_socket = sockets.get_mut::<TcpSocket>(handle);
    rearm_socket_with_reason(
        handle,
        tcp_socket,
        ctx,
        fake_pool,
        now_secs,
        close.direction,
        close.reason,
    );
    false
}

fn finish_deferred_relay_close_if_drained(
    handle: SocketHandle,
    socket: &mut TcpSocket<'_>,
    ctx: &mut SocketCtx,
    fake_pool: &mut FakeIpPool,
    now_secs: u64,
) -> bool {
    let Some(close) = ctx.pending_relay_close else {
        return false;
    };
    if !ctx.downlink_pending.is_empty() {
        return true;
    }
    rearm_socket_with_reason(
        handle,
        socket,
        ctx,
        fake_pool,
        now_secs,
        close.direction,
        close.reason,
    );
    true
}

/// 刀11：在 run_event_loop 单写者 task 重算 loop-owned gauge 并发布进 `Arc<Metrics>`（30s tick 调）。
/// 中文要点：`active_relays`（state==Relaying 计数）与 fake-IP 用量来自 loop **独占无锁**的
/// `socket_ctxs`/`fake_pool`，**不能跨 task 读**（严禁套 Arc<Mutex>）→ 只能在此 task 周期重算后 `store`，
/// 外部读者（snapshot/未来前端）见最新已发布值。`failover_leg_u8` 由 caller 经 upstream trait 读出后传入。
/// O(n) 扫描只在 30s tick，不进每包热路径。
fn publish_gauges<'a>(
    metrics_handle: &Metrics,
    ctxs: impl Iterator<Item = &'a SocketCtx>,
    fake_pool: &FakeIpPool,
    failover_leg_u8: u8,
) {
    let active = ctxs.filter(|c| c.state == SocketState::Relaying).count();
    metrics_handle.set_active_relays(active as u32);
    let (total, act) = fake_pool.usage();
    metrics_handle.set_fake_ip(total as u32, act as u32);
    metrics_handle.set_failover_leg(failover_leg_u8);
}

fn spawn_remote_relay(
    handle: SocketHandle,
    epoch: u64,
    stream: RelayStream,
    rx: mpsc::Receiver<RelayCommand>,
    back_tx: mpsc::Sender<(SocketHandle, RelayEvent)>,
    metrics_handle: &Metrics,
) {
    // 刀11：每条新 TCP flow 开远端成功后在此唯一入口计一次（覆盖 inline + spawned remote-open 两条路）。
    metrics_handle.inc_relays_spawned();
    tokio::spawn(run_relay(handle, epoch, stream, rx, back_tx));
}

/// 一条 TCP relay 的双向泵（独立 task body；抽出便于 idle 超时单测）。
/// 中文要点：L2（刀9 F4）select 加 idle 超时分支——双向 `RELAY_IDLE_TIMEOUT` 无活动 → 退出 + shutdown。
/// 任一方向有活动（本地→上游 write 成功 / 上游→本地 read）即重置（每轮 select 重建 sleep，计「距上次活动」）。
/// 适用 TUIC/REALITY 两种 RelayStream，与连接级 failover 探测无关（那是连接级，这是单 relay 级）。
async fn run_relay(
    handle: SocketHandle,
    epoch: u64,
    stream: RelayStream,
    rx: mpsc::Receiver<RelayCommand>,
    back_tx: mpsc::Sender<(SocketHandle, RelayEvent)>,
) {
    let (mut remote_reader, remote_writer) = tokio::io::split(stream);
    let (writer_signal_tx, mut writer_signal_rx) =
        mpsc::channel::<RelayWriterSignal>(RELAY_CHANNEL_CAPACITY);
    let (writer_stop_tx, writer_stop_rx) = tokio::sync::oneshot::channel::<()>();
    let mut writer_task = tokio::spawn(run_relay_writer(
        handle,
        remote_writer,
        rx,
        writer_signal_tx,
        writer_stop_rx,
    ));
    let mut buf = [0u8; 65_536];
    let mut diag = RelayTaskDiag::default();
    let global_rx_pressure_threshold = std::time::Duration::from_millis(5);
    let idle = tokio::time::sleep(RELAY_IDLE_TIMEOUT);
    tokio::pin!(idle);
    let mut writer_done = false;
    let mut read_only_after_local_finish = false;
    let (close_direction, close_reason) = loop {
        tokio::select! {
            signal = writer_signal_rx.recv(), if !writer_done => {
                match signal {
                    Some(RelayWriterSignal::Progress { bytes }) => {
                        diag.note_uplink_write(bytes);
                        idle.as_mut().reset(tokio::time::Instant::now() + RELAY_IDLE_TIMEOUT);
                    }
                    Some(RelayWriterSignal::WriteHalfClosed { reason }) => {
                        tcp_diag_log!(
                            "🔎 tcp-relay-write-half-closed handle={:?} reason={}",
                            handle,
                            reason
                        );
                        writer_done = true;
                        read_only_after_local_finish = true;
                        idle.as_mut().reset(
                            tokio::time::Instant::now() + RELAY_HALF_CLOSED_IDLE_TIMEOUT
                        );
                    }
                    Some(RelayWriterSignal::Closed { direction, reason }) => {
                        break (direction, reason);
                    }
                    None => {
                        writer_done = true;
                    }
                }
            }
            remote_msg = remote_reader.read(&mut buf) => {
                match remote_msg {
                    Ok(0) => {
                        println!("远端服务器关闭了车厢 {:?}", handle);
                        break ("remote_to_local", "remote_eof");
                    }
                    Ok(n) => {
                        diag.note_remote_read(n);
                        let data = buf[..n].to_vec();
                        let wait_started = std::time::Instant::now();
                        let send_result = back_tx
                            .send((handle, RelayEvent::Data { epoch, bytes: data }))
                            .await;
                        let waited = wait_started.elapsed();
                        diag.note_global_rx_wait(waited, global_rx_pressure_threshold);
                        if waited >= global_rx_pressure_threshold {
                            tcp_diag_log!(
                                "🔎 tcp-global-rx-pressure handle={:?} wait_us={} payload_bytes={} pressure_events={}",
                                handle,
                                waited.as_micros(),
                                n,
                                diag.global_rx_pressure_events
                            );
                        }
                        if send_result.is_err() {
                            break ("remote_to_local", "global_rx_closed");
                        }
                        let timeout = if read_only_after_local_finish {
                            RELAY_HALF_CLOSED_IDLE_TIMEOUT
                        } else {
                            RELAY_IDLE_TIMEOUT
                        };
                        idle.as_mut().reset(tokio::time::Instant::now() + timeout);
                    }
                    Err(e) => {
                        println!("读取上游流失败 direction=remote_to_local handle={:?} err={:?}", handle, e);
                        break ("remote_to_local", "remote_read_failed");
                    }
                }
            }
            // L2：双向静默超时 → 主动清理（防泄漏）。有活动的 select 分支会重启 loop → 重建 sleep。
            _ = &mut idle => {
                let (timeout, reason) = if read_only_after_local_finish {
                    (RELAY_HALF_CLOSED_IDLE_TIMEOUT, "half_closed_idle_timeout")
                } else {
                    (RELAY_IDLE_TIMEOUT, "idle_timeout")
                };
                println!("⏱️ relay {:?} 双向静默 {}s，idle 超时关闭（L2）", handle, timeout.as_secs());
                break ("timer", reason);
            }
        }
    };
    let _ = writer_stop_tx.send(());
    // M2 preserved after splitting the pump: let the write half drive shutdown so protocol-layer
    // pending bytes can flush, but keep the relay close path bounded if shutdown itself wedges.
    if writer_task.is_finished() {
        let _ = writer_task.await;
    } else if tokio::time::timeout(RELAY_WRITE_TIMEOUT, &mut writer_task)
        .await
        .is_err()
    {
        writer_task.abort();
        let _ = writer_task.await;
    }
    tcp_diag_log!(
        "🔎 tcp-relay-close handle={:?} direction={} reason={} uplink_bytes={} uplink_writes={} remote_to_global_rx_bytes={} remote_reads={} global_rx_wait_max_us={} global_rx_pressure_events={}",
        handle,
        close_direction,
        close_reason,
        diag.uplink_bytes,
        diag.local_writes,
        diag.remote_to_global_rx_bytes,
        diag.remote_reads,
        diag.global_rx_wait_max_micros,
        diag.global_rx_pressure_events
    );
    let _ = back_tx
        .send((
            handle,
            RelayEvent::Closed(RelayClose {
                epoch,
                direction: close_direction,
                reason: close_reason,
            }),
        ))
        .await;
}

async fn run_relay_writer(
    handle: SocketHandle,
    mut writer: tokio::io::WriteHalf<RelayStream>,
    mut rx: mpsc::Receiver<RelayCommand>,
    signal_tx: mpsc::Sender<RelayWriterSignal>,
    mut stop_rx: tokio::sync::oneshot::Receiver<()>,
) {
    loop {
        tokio::select! {
            _ = &mut stop_rx => {
                let _ = writer.shutdown().await;
                return;
            }
            local_msg = rx.recv() => {
                match local_msg {
                    Some(RelayCommand::Data(payload)) => {
                        let payload_len = payload.len();
                        let write_result = tokio::select! {
                            _ = &mut stop_rx => {
                                let _ = writer.shutdown().await;
                                return;
                            }
                            result = tokio::time::timeout(RELAY_WRITE_TIMEOUT, writer.write_all(&payload)) => result,
                        };
                        match write_result {
                            Ok(Ok(_)) => {
                                let _ = signal_tx
                                    .try_send(RelayWriterSignal::Progress { bytes: payload_len });
                            }
                            Ok(Err(e)) => {
                                println!(
                                    "写入上游流失败 direction=local_to_remote handle={:?} attempted_bytes={} err={:?}",
                                    handle, payload_len, e
                                );
                                let _ = signal_tx
                                    .send(RelayWriterSignal::Closed {
                                        direction: "local_to_remote",
                                        reason: "remote_write_failed",
                                    })
                                    .await;
                                let _ = writer.shutdown().await;
                                return;
                            }
                            Err(_) => {
                                println!(
                                    "写入上游流超时 direction=local_to_remote handle={:?} attempted_bytes={} timeout={}s",
                                    handle,
                                    payload_len,
                                    RELAY_WRITE_TIMEOUT.as_secs()
                                );
                                let _ = signal_tx
                                    .send(RelayWriterSignal::Closed {
                                        direction: "local_to_remote",
                                        reason: "remote_write_timeout",
                                    })
                                    .await;
                                let _ = writer.shutdown().await;
                                return;
                            }
                        }
                    }
                    Some(RelayCommand::Finish) => {
                        let shutdown_result = tokio::select! {
                            _ = &mut stop_rx => {
                                let _ = writer.shutdown().await;
                                return;
                            }
                            result = tokio::time::timeout(RELAY_WRITE_TIMEOUT, writer.shutdown()) => result,
                        };
                        match shutdown_result {
                            Ok(Ok(_)) => {
                                let _ = signal_tx
                                    .send(RelayWriterSignal::WriteHalfClosed {
                                        reason: "local_finish",
                                    })
                                    .await;
                                return;
                            }
                            Ok(Err(e)) => {
                                println!(
                                    "关闭上游写半边失败 direction=local_to_remote handle={:?} err={:?}",
                                    handle, e
                                );
                                let _ = signal_tx
                                    .send(RelayWriterSignal::Closed {
                                        direction: "local_to_remote",
                                        reason: "remote_shutdown_failed",
                                    })
                                    .await;
                                return;
                            }
                            Err(_) => {
                                println!(
                                    "关闭上游写半边超时 direction=local_to_remote handle={:?} timeout={}s",
                                    handle,
                                    RELAY_WRITE_TIMEOUT.as_secs()
                                );
                                let _ = signal_tx
                                    .send(RelayWriterSignal::Closed {
                                        direction: "local_to_remote",
                                        reason: "remote_shutdown_timeout",
                                    })
                                    .await;
                                return;
                            }
                        }
                    }
                    None => {
                        println!("本地房间 {:?} 已关闭通道", handle);
                        let _ = signal_tx
                            .send(RelayWriterSignal::Closed {
                                direction: "local_to_remote",
                                reason: "local_channel_closed",
                            })
                            .await;
                        let _ = writer.shutdown().await;
                        return;
                    }
                }
            }
        }
    }
}

/// rx 热路径分流结果。
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum Inbound {
    /// 任意 resolver 的明文 UDP/53 —— 走裸包 DNS 劫持（本地伪造 fake-IP，绕过 smoltcp，见 ADR-0007）。
    Dns,
    /// 其它 UDP —— 走裸包 UDP relay（绕过 smoltcp）。
    UdpRelay,
    /// 非 UDP（TCP/ICMP…）—— 走既有 smoltcp 路径。
    Other,
}

/// 给一个入站裸 IP 包分类（见 stage-12 spec 的 D1 规则；刀5 起任意 :53 → Dns）。
/// **load-bearing 不变量**：UDP :53 在此被判 `Dns`、`rx_take` 走劫持路径，**永不进 iface.poll /
/// resolve_target**——这正是 `resolve_target` 的 `is_dns_relay_port`（port==53 Block）只命中 **TCP** :53
/// 的依据（见 ADR-0007 / `dns_block::is_dns_relay_port`）。改动此分支前务必同步那条 Block 语义。
fn classify_inbound(pkt: &[u8]) -> Inbound {
    match parse_inbound_udp(pkt) {
        // 刀5：任意 resolver 的明文 :53 → Dns（裸包伪造 fake-IP，不依赖系统 DNS 指向 198.18.0.1）。
        Some(udp) if udp.dst_port == 53 => Inbound::Dns,
        Some(_) => Inbound::UdpRelay,
        None => Inbound::Other,
    }
}

/// 处理一个被拦截的 UDP 上行包：解析 → fake-IP 改写 target → 铸 assoc-id →
/// 编码 TUIC Packet → `TuicUpstream::send_udp`。
/// 中文要点：fake 无映射 → 丢弃(短 TTL 自愈)；send_udp 自带丢弃计数(UDP 语义)。
async fn handle_tuic_udp_uplink<U: DatagramUpstream>(
    pkt: &[u8],
    assoc_table: &mut AssocTable,
    fake_pool: &mut FakeIpPool,
    upstream: &U,
    now_secs: u64,
) {
    let Some(udp) = parse_inbound_udp(pkt) else {
        return;
    };
    let dst_ep = smoltcp::wire::IpEndpoint::new(
        IpAddress::Ipv4(smoltcp::wire::Ipv4Address::from_bytes(&udp.dst_ip.octets())),
        udp.dst_port,
    );
    let (target, fake_ip) = match resolve_target(dst_ep, fake_pool) {
        TargetResolve::Direct { target, fake_ip } => (target, fake_ip),
        TargetResolve::Refuse => {
            trace_log!("🚫 UDP fake-IP {} 无映射，丢弃（待应用重新解析）", udp.dst_ip);
            return;
        }
        // 刀4：加密 DNS（DoQ :853 / DoH3 :443）→ **静默丢包**，逼应用回落明文 DNS。
        // 中文要点：此处每个入站 UDP datagram 必经，**不在热路径 println!**（同 resolve_target 的纪律：
        // 同步 stdout 会拖垮大并发；DoQ/DoH3 被丢后 QUIC 会重传一串包 → 逐包打印即洪水）。丢弃即正确行为；
        // 需要可观测时另加计数器周期汇报（cheap follow-up，本刀从简）。
        TargetResolve::Block => return,
    };
    let tuple = FourTuple {
        src_ip: udp.src_ip,
        src_port: udp.src_port,
        dst_ip: udp.dst_ip,
        dst_port: udp.dst_port,
    };
    // 仅在「新 flow」时打日志（每流一次，不是每包），与 Stage 12 一致。
    let is_new = !assoc_table.contains(&tuple);
    let assoc_id = assoc_table.intern(tuple);
    assoc_table.touch(assoc_id, now_secs);
    if is_new {
        // 刀2 引用计数：UDP 新 flow 占用 fake-IP → 登记到 assoc + acquire，
        // 保证该映射在 flow 存活期间不被 fake_pool sweep 回收（回收会让回程 resolve 失败）。
        if let Some(ip) = fake_ip {
            assoc_table.set_fake_ip(assoc_id, ip);
            fake_pool.acquire(ip, now_secs);
        }
        trace_log!(
            "🌊 TUIC UDP↑ new assoc={assoc_id} → {} (first {}B)",
            target.to_wire_string(),
            udp.payload.len()
        );
    }
    // intern 可能 LRU 驱逐旧 assoc → 立即 release 其占用的 fake-IP（引用计数平衡）。
    for ip in assoc_table.take_reclaimed_fake_ips() {
        fake_pool.release(ip, now_secs);
    }
    upstream.send_udp(encode_packet(assoc_id, &target, udp.payload)).await;
}

pub async fn create_tun_device(tun_mtu: usize) -> tun::Result<tun::AsyncDevice> {
    let mut config = tun::Configuration::default();

    config
        .address((10, 0, 0, 1)) // 网卡的 IP 地址
        .destination((10, 0, 0, 2)) // 🌟 新增：告诉 OS 水管另一头是谁！
        .netmask((255, 255, 255, 0)) // 子网掩码
        .mtu(tun_mtu_for_config(tun_mtu)) // 刀14c：OS TUN MTU 与 smoltcp capability 对齐
        .up(); // 启动网卡

    #[cfg(target_os = "macos")]
    config.layer(tun::Layer::L3); // macOS 通常需要显式指定三层（IP层）

    // Create the async TUN device with an explicit error path.
    // 中文要点：这里不要 panic，启动失败应当以可观测的错误返回给上层。
    tun::create_as_async(&config)
}

fn tun_mtu_for_config(tun_mtu: usize) -> i32 {
    i32::try_from(tun_mtu).expect("validated MINI_VPN_TUN_MTU fits tun::Configuration::mtu(i32)")
}

#[cfg(test)]
mod tests {
    use super::*;
    use smoltcp::iface::SocketSet;

    /// 刀8/刀9：上游选择器——reality→Reality、failover→Failover（大小写/空白不敏感）；
    /// 其余（含 tuic/缺省/未知）→ Tuic（零回归，failover opt-in）。
    #[test]
    fn upstream_kind_selector() {
        assert_eq!(select_upstream_kind(Some("reality")), UpstreamKind::Reality);
        assert_eq!(select_upstream_kind(Some("  REALITY ")), UpstreamKind::Reality);
        assert_eq!(select_upstream_kind(Some("failover")), UpstreamKind::Failover);
        assert_eq!(select_upstream_kind(Some(" Failover ")), UpstreamKind::Failover);
        assert_eq!(select_upstream_kind(Some("tuic")), UpstreamKind::Tuic);
        assert_eq!(select_upstream_kind(None), UpstreamKind::Tuic, "缺省 → TUIC（failover opt-in）");
        assert_eq!(select_upstream_kind(Some("bogus")), UpstreamKind::Tuic, "未知 → TUIC（零回归）");
    }

    // ---- 刀9 F4：relay idle 超时（L2）----
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::task::{Context, Poll};

    /// 一条永不产数据的 mock 上游流：read 恒 Pending、write/flush 即成、shutdown 记账。
    /// 用于驱动 run_relay 的 idle 超时分支（唯一能 fire 的分支）。
    struct IdleStream {
        shutdown_called: Arc<AtomicBool>,
    }
    impl tokio::io::AsyncRead for IdleStream {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            Poll::Pending // 永不产数据/永不 EOF：只有 idle sleep 分支能完成
        }
    }
    impl tokio::io::AsyncWrite for IdleStream {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            Poll::Ready(Ok(buf.len())) // 上行 write 即成（活动 → 重置 idle）
        }
        fn poll_flush(self: std::pin::Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: std::pin::Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            self.shutdown_called.store(true, Ordering::SeqCst);
            Poll::Ready(Ok(()))
        }
    }

    /// 一条上行写立即失败的 mock 上游流，用来锁住 remote_write_failed 也会通知主循环 rearm。
    struct FailingWriteStream {
        shutdown_called: Arc<AtomicBool>,
    }
    impl tokio::io::AsyncRead for FailingWriteStream {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            Poll::Pending
        }
    }

    /// 一条上行写永远不完成的 mock 上游流，用来锁住 write_all 卡住时也会主动关 relay。
    struct PendingWriteStream {
        shutdown_called: Arc<AtomicBool>,
    }
    impl tokio::io::AsyncRead for PendingWriteStream {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            Poll::Pending
        }
    }

    /// 写侧 Pending 后读侧才产数据：锁住 relay 读写必须真正并发。
    struct PendingWriteThenReadableStream {
        write_polled: Arc<AtomicBool>,
        shutdown_called: Arc<AtomicBool>,
        read_sent: bool,
    }
    impl tokio::io::AsyncRead for PendingWriteThenReadableStream {
        fn poll_read(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            if self.read_sent {
                return Poll::Pending;
            }
            if !self.write_polled.load(Ordering::SeqCst) {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            buf.put_slice(b"remote-progress");
            self.read_sent = true;
            Poll::Ready(Ok(()))
        }
    }
    impl tokio::io::AsyncWrite for PendingWriteThenReadableStream {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            cx: &mut Context<'_>,
            _buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            self.write_polled.store(true, Ordering::SeqCst);
            cx.waker().wake_by_ref();
            Poll::Pending
        }
        fn poll_flush(self: std::pin::Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: std::pin::Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            self.shutdown_called.store(true, Ordering::SeqCst);
            Poll::Ready(Ok(()))
        }
    }

    /// 写半边 shutdown 后读侧才产数据：锁住本地 FIN 不应终止 remote-to-local 方向。
    struct LocalFinishThenReadableStream {
        shutdown_called: Arc<AtomicBool>,
        read_sent: bool,
    }
    impl tokio::io::AsyncRead for LocalFinishThenReadableStream {
        fn poll_read(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            if !self.shutdown_called.load(Ordering::SeqCst) {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            if self.read_sent {
                return Poll::Ready(Ok(()));
            }
            buf.put_slice(b"after-local-finish");
            self.read_sent = true;
            Poll::Ready(Ok(()))
        }
    }
    impl tokio::io::AsyncWrite for LocalFinishThenReadableStream {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            Poll::Ready(Ok(buf.len()))
        }
        fn poll_flush(self: std::pin::Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: std::pin::Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            self.shutdown_called.store(true, Ordering::SeqCst);
            Poll::Ready(Ok(()))
        }
    }
    impl tokio::io::AsyncWrite for PendingWriteStream {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            Poll::Pending
        }
        fn poll_flush(self: std::pin::Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: std::pin::Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            self.shutdown_called.store(true, Ordering::SeqCst);
            Poll::Ready(Ok(()))
        }
    }
    impl tokio::io::AsyncWrite for FailingWriteStream {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "test write reset",
            )))
        }
        fn poll_flush(self: std::pin::Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: std::pin::Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            self.shutdown_called.store(true, Ordering::SeqCst);
            Poll::Ready(Ok(()))
        }
    }

    fn mk_test_handle(sockets: &mut SocketSet<'static>) -> SocketHandle {
        sockets.add(build_listener_socket(&ListenerSpec { local_port: 12345 }))
    }

    /// idle：双向 90s 无活动 → relay task 退出 + stream.shutdown 被调（L2）。
    #[tokio::test(start_paused = true)]
    async fn relay_idle_timeout_shuts_down_stream() {
        let mut sockets = SocketSet::new(vec![]);
        let handle = mk_test_handle(&mut sockets);
        let flag = Arc::new(AtomicBool::new(false));
        let stream: RelayStream = Box::new(IdleStream { shutdown_called: flag.clone() });
        let (_tx, rx) = mpsc::channel::<RelayCommand>(8); // 持 _tx → rx 不关、永不收（无活动）
        let (back_tx, mut back_rx) = mpsc::channel(8);
        let task = tokio::spawn(run_relay(handle, 7, stream, rx, back_tx));

        tokio::time::advance(std::time::Duration::from_secs(89)).await;
        assert!(!task.is_finished(), "89s < 90s idle 阈值，relay 不应退出");
        tokio::time::advance(std::time::Duration::from_secs(2)).await;
        task.await.unwrap();
        assert!(flag.load(Ordering::SeqCst), "idle 超时应退出并调用 stream.shutdown（L2）");
        match back_rx.try_recv().expect("relay close should notify main loop") {
            (h, RelayEvent::Closed(close)) => {
                assert_eq!(h, handle);
                assert_eq!(close.epoch, 7);
                assert_eq!(close.direction, "timer");
                assert_eq!(close.reason, "idle_timeout");
            }
            other => panic!("expected relay close event, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn relay_remote_write_failed_notifies_main_loop() {
        let mut sockets = SocketSet::new(vec![]);
        let handle = mk_test_handle(&mut sockets);
        let flag = Arc::new(AtomicBool::new(false));
        let stream: RelayStream = Box::new(FailingWriteStream { shutdown_called: flag.clone() });
        let (tx, rx) = mpsc::channel::<RelayCommand>(8);
        let (back_tx, mut back_rx) = mpsc::channel(8);
        let task = tokio::spawn(run_relay(handle, 11, stream, rx, back_tx));

        tx.send(RelayCommand::Data(vec![1, 2, 3])).await.unwrap();
        task.await.unwrap();

        assert!(flag.load(Ordering::SeqCst), "write failure should still call stream.shutdown");
        match back_rx.try_recv().expect("write failure should notify main loop") {
            (h, RelayEvent::Closed(close)) => {
                assert_eq!(h, handle);
                assert_eq!(close.epoch, 11);
                assert_eq!(close.direction, "local_to_remote");
                assert_eq!(close.reason, "remote_write_failed");
            }
            other => panic!("expected relay close event, got {other:?}"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn relay_remote_write_timeout_notifies_main_loop() {
        let mut sockets = SocketSet::new(vec![]);
        let handle = mk_test_handle(&mut sockets);
        let flag = Arc::new(AtomicBool::new(false));
        let stream: RelayStream = Box::new(PendingWriteStream { shutdown_called: flag.clone() });
        let (tx, rx) = mpsc::channel::<RelayCommand>(8);
        let (back_tx, mut back_rx) = mpsc::channel(8);
        let task = tokio::spawn(run_relay(handle, 17, stream, rx, back_tx));

        tx.send(RelayCommand::Data(vec![1, 2, 3])).await.unwrap();
        tokio::task::yield_now().await;
        tokio::time::advance(RELAY_WRITE_TIMEOUT - std::time::Duration::from_secs(1)).await;
        assert!(!task.is_finished(), "relay should not close before write timeout");
        tokio::time::advance(std::time::Duration::from_secs(1)).await;
        task.await.unwrap();

        assert!(flag.load(Ordering::SeqCst), "write timeout should still call stream.shutdown");
        match back_rx.try_recv().expect("write timeout should notify main loop") {
            (h, RelayEvent::Closed(close)) => {
                assert_eq!(h, handle);
                assert_eq!(close.epoch, 17);
                assert_eq!(close.direction, "local_to_remote");
                assert_eq!(close.reason, "remote_write_timeout");
            }
            other => panic!("expected relay close event, got {other:?}"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn relay_remote_read_progresses_while_local_write_is_pending() {
        let mut sockets = SocketSet::new(vec![]);
        let handle = mk_test_handle(&mut sockets);
        let write_polled = Arc::new(AtomicBool::new(false));
        let shutdown_called = Arc::new(AtomicBool::new(false));
        let stream: RelayStream = Box::new(PendingWriteThenReadableStream {
            write_polled: write_polled.clone(),
            shutdown_called: shutdown_called.clone(),
            read_sent: false,
        });
        let (tx, rx) = mpsc::channel::<RelayCommand>(8);
        let (back_tx, mut back_rx) = mpsc::channel(8);
        let task = tokio::spawn(run_relay(handle, 23, stream, rx, back_tx));

        tx.send(RelayCommand::Data(vec![1, 2, 3])).await.unwrap();
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        assert!(
            write_polled.load(Ordering::SeqCst),
            "test stream should have entered the pending write"
        );
        match back_rx.try_recv().expect("remote read should not wait for write timeout") {
            (h, RelayEvent::Data { epoch, bytes }) => {
                assert_eq!(h, handle);
                assert_eq!(epoch, 23);
                assert_eq!(bytes, b"remote-progress");
            }
            other => panic!("expected remote data before write timeout, got {other:?}"),
        }
        assert!(
            !shutdown_called.load(Ordering::SeqCst),
            "relay should still be alive before write timeout"
        );

        tokio::time::advance(RELAY_WRITE_TIMEOUT).await;
        task.await.unwrap();
    }

    #[tokio::test]
    async fn relay_local_finish_keeps_remote_read_open() {
        let mut sockets = SocketSet::new(vec![]);
        let handle = mk_test_handle(&mut sockets);
        let shutdown_called = Arc::new(AtomicBool::new(false));
        let stream: RelayStream = Box::new(LocalFinishThenReadableStream {
            shutdown_called: shutdown_called.clone(),
            read_sent: false,
        });
        let (tx, rx) = mpsc::channel::<RelayCommand>(8);
        let (back_tx, mut back_rx) = mpsc::channel(8);
        let task = tokio::spawn(run_relay(handle, 29, stream, rx, back_tx));

        tx.send(RelayCommand::Finish).await.unwrap();
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        assert!(
            shutdown_called.load(Ordering::SeqCst),
            "local finish should shutdown only the remote write half"
        );
        match back_rx.recv().await.expect("remote data should still reach main loop") {
            (h, RelayEvent::Data { epoch, bytes }) => {
                assert_eq!(h, handle);
                assert_eq!(epoch, 29);
                assert_eq!(bytes, b"after-local-finish");
            }
            other => panic!("expected remote data after local finish, got {other:?}"),
        }

        task.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn relay_local_finish_without_remote_progress_uses_half_closed_idle_timeout() {
        let mut sockets = SocketSet::new(vec![]);
        let handle = mk_test_handle(&mut sockets);
        let shutdown_called = Arc::new(AtomicBool::new(false));
        let stream: RelayStream = Box::new(IdleStream { shutdown_called: shutdown_called.clone() });
        let (tx, rx) = mpsc::channel::<RelayCommand>(8);
        let (back_tx, mut back_rx) = mpsc::channel(8);
        let task = tokio::spawn(run_relay(handle, 31, stream, rx, back_tx));

        tx.send(RelayCommand::Finish).await.unwrap();
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        assert!(shutdown_called.load(Ordering::SeqCst), "Finish should shutdown the remote write half");

        tokio::time::advance(RELAY_HALF_CLOSED_IDLE_TIMEOUT - std::time::Duration::from_secs(1)).await;
        assert!(!task.is_finished(), "half-closed relay should stay open before the short idle timeout");
        tokio::time::advance(std::time::Duration::from_secs(1)).await;
        task.await.unwrap();

        match back_rx.try_recv().expect("half-closed idle should notify main loop") {
            (h, RelayEvent::Closed(close)) => {
                assert_eq!(h, handle);
                assert_eq!(close.epoch, 31);
                assert_eq!(close.direction, "timer");
                assert_eq!(close.reason, "half_closed_idle_timeout");
            }
            other => panic!("expected half-closed relay close event, got {other:?}"),
        }
    }

    /// 活动重置：临近阈值前来一次上行活动 → idle 计时重置，不退出；再静默满 90s 才退出。
    #[tokio::test(start_paused = true)]
    async fn relay_activity_resets_idle_timer() {
        let mut sockets = SocketSet::new(vec![]);
        let handle = mk_test_handle(&mut sockets);
        let flag = Arc::new(AtomicBool::new(false));
        let stream: RelayStream = Box::new(IdleStream { shutdown_called: flag.clone() });
        let (tx, rx) = mpsc::channel::<RelayCommand>(8);
        let (back_tx, _back_rx) = mpsc::channel(8);
        let task = tokio::spawn(run_relay(handle, 13, stream, rx, back_tx));

        tokio::time::advance(std::time::Duration::from_secs(89)).await;
        tx.send(RelayCommand::Data(vec![1, 2, 3])).await.unwrap(); // 活动（上行 write）→ 重置 idle 计时
        tokio::task::yield_now().await; // 让 relay 消费该活动并重建 sleep
        tokio::time::advance(std::time::Duration::from_secs(89)).await;
        assert!(!task.is_finished(), "活动重置了 idle 计时，第二个 89s 窗口内不应退出");
        tokio::time::advance(std::time::Duration::from_secs(2)).await;
        task.await.unwrap();
        assert!(flag.load(Ordering::SeqCst), "重置后再满 90s 静默才退出");
    }

    #[test]
    fn target_from_endpoint_builds_ipv4_target() {
        let ep = smoltcp::wire::IpEndpoint::new(IpAddress::v4(93, 184, 216, 34), 80);
        let target = target_from_endpoint(ep);
        assert_eq!(target.to_wire_string(), "93.184.216.34:80");
    }

    /// MetricsSink 契约：自定义 sink 能覆写默认空实现，逐回调被调用、listener 计数透传。
    /// 中文要点：锁住 run_event_loop 的插桩接缝形状（生产 NoopSink 零开销、harness 可记录）。
    #[derive(Default)]
    struct CountingSink {
        poll_enters: usize,
        relay_enters: usize,
        last_listeners: usize,
    }
    impl MetricsSink for CountingSink {
        fn enter_poll(&mut self) {
            self.poll_enters += 1;
        }
        fn enter_relay(&mut self) {
            self.relay_enters += 1;
        }
        fn note_listeners(&mut self, n: usize) {
            self.last_listeners = n;
        }
    }

    #[test]
    fn metrics_sink_records_per_phase_calls() {
        let mut sink = CountingSink::default();
        // 模拟一个 tick：poll 段 + relay 段（遍历 7 个 listener）。
        sink.enter_poll();
        sink.leave_poll();
        sink.enter_relay();
        sink.note_listeners(7);
        sink.leave_relay();
        assert_eq!(sink.poll_enters, 1);
        assert_eq!(sink.relay_enters, 1);
        assert_eq!(sink.last_listeners, 7);
    }

    #[test]
    fn noop_sink_is_zero_state() {
        // NoopSink 全空实现：可被反复调用且无副作用（生产热路径零开销的依据）。
        let mut sink = NoopSink;
        sink.enter_poll();
        sink.leave_poll();
        sink.enter_relay();
        sink.note_listeners(1024);
        sink.leave_relay();
    }

    /// Build a minimal IPv4+TCP packet with the requested flags for SYN-inspector tests.
    fn build_ipv4_tcp(
        src: [u8; 4],
        dst: [u8; 4],
        src_port: u16,
        dst_port: u16,
        syn: bool,
        ack: bool,
    ) -> Vec<u8> {
        let b = etherparse::PacketBuilder::ipv4(src, dst, 64).tcp(src_port, dst_port, 0, 1024);
        let b = if syn { b.syn() } else { b };
        let b = if ack { b.ack(0) } else { b };
        let mut buf = Vec::new();
        let payload: [u8; 0] = [];
        b.write(&mut buf, &payload).unwrap();
        buf
    }

    #[test]
    fn inspect_inbound_tcp_flags_clean_syn() {
        // 干净 SYN → (端口, is_clean_syn=true)。
        let syn = build_ipv4_tcp([10, 0, 0, 1], [1, 1, 1, 1], 60000, 443, true, false);
        assert_eq!(inspect_inbound_tcp(&syn), Some((443, true)));
        // SYN-ACK → 端口仍返回（用于标脏），但非干净 SYN（不建池）。
        let synack = build_ipv4_tcp([1, 1, 1, 1], [10, 0, 0, 1], 443, 60000, true, true);
        assert_eq!(inspect_inbound_tcp(&synack), Some((60000, false)));
        // 纯 ACK / data 包 → 端口返回，非 SYN（首包数据让 listener can_recv 的那一刻）。
        let ack = build_ipv4_tcp([10, 0, 0, 1], [1, 1, 1, 1], 60000, 80, false, true);
        assert_eq!(inspect_inbound_tcp(&ack), Some((80, false)));
    }

    #[test]
    fn inspect_inbound_tcp_rejects_non_tcp_and_garbage() {
        // 非 TCP（UDP）/ 垃圾 → None。
        assert_eq!(inspect_inbound_tcp(&udp_pkt([8, 8, 8, 8], 53)), None);
        assert_eq!(inspect_inbound_tcp(&[0u8; 4]), None);
    }

    #[test]
    fn registry_ensure_port_is_idempotent_and_capped() {
        let mut sockets = SocketSet::new(vec![]);
        let mut ctxs: HashMap<SocketHandle, SocketCtx> = HashMap::new();
        let mut reg = ListenerRegistry::new(2);

        for i in 0..MAX_INTERCEPTED_PORTS as u16 {
            reg.ensure_port(i + 1, &mut sockets, &mut ctxs).unwrap();
        }
        assert_eq!(reg.port_count(), MAX_INTERCEPTED_PORTS);
        // pool_size * port_count handles registered
        assert_eq!(ctxs.len(), 2 * MAX_INTERCEPTED_PORTS);

        // idempotent: re-adding an existing port does not grow the registry
        reg.ensure_port(1, &mut sockets, &mut ctxs).unwrap();
        assert_eq!(reg.port_count(), MAX_INTERCEPTED_PORTS);
        assert_eq!(ctxs.len(), 2 * MAX_INTERCEPTED_PORTS);

        // capped: a new port beyond the cap is rejected, existing state preserved
        let err = reg.ensure_port(9999, &mut sockets, &mut ctxs).unwrap_err();
        assert!(matches!(err, RegistryError::Capped));
        assert_eq!(reg.port_count(), MAX_INTERCEPTED_PORTS);
    }

    /// #2 弹性扩容：端口 Listening 槽不足 min_spare 时按需补建，已够则幂等不动，
    /// 未注册端口 no-op；rearm 回 Listening 的槽计入空闲、可复用。
    #[test]
    fn ensure_spare_listeners_grows_and_is_idempotent() {
        let mut sockets = SocketSet::new(vec![]);
        let mut ctxs: HashMap<SocketHandle, SocketCtx> = HashMap::new();
        let mut reg = ListenerRegistry::new(2);
        reg.ensure_port(443, &mut sockets, &mut ctxs).unwrap();
        assert_eq!(reg.handles_for_port(443).len(), 2);

        // 占满已有 2 槽（abort → 离开 Listen 状态，模拟被 accept 占用）→ 无空闲 listening。
        let hs: Vec<SocketHandle> = reg.handles_for_port(443).to_vec();
        for h in &hs {
            sockets.get_mut::<TcpSocket>(*h).abort();
        }
        // 要求 ≥2 空闲 → 补建 2 个。
        reg.ensure_spare_listeners(443, 2, &mut sockets, &mut ctxs)
            .unwrap();
        assert_eq!(reg.handles_for_port(443).len(), 4);
        // 幂等：已有 2 空闲，不再建。
        reg.ensure_spare_listeners(443, 2, &mut sockets, &mut ctxs)
            .unwrap();
        assert_eq!(reg.handles_for_port(443).len(), 4);
        // 未注册端口 → no-op（建池仍由 ensure_port 负责）。
        reg.ensure_spare_listeners(8080, 2, &mut sockets, &mut ctxs)
            .unwrap();
        assert!(reg.handles_for_port(8080).is_empty());
    }

    /// #2 全局总槽上限：弹性扩容受 `max_total` 兜底，达上限返回 Capped、不再增长（防 SYN flood）。
    #[test]
    fn ensure_spare_listeners_respects_global_cap() {
        let mut sockets = SocketSet::new(vec![]);
        let mut ctxs: HashMap<SocketHandle, SocketCtx> = HashMap::new();
        // cap=3：pool_size=2 首建占 2，弹性最多再加 1。
        let mut reg = ListenerRegistry::with_max_total(2, 3);
        reg.ensure_port(443, &mut sockets, &mut ctxs).unwrap();
        let hs: Vec<SocketHandle> = reg.handles_for_port(443).to_vec();
        for h in &hs {
            sockets.get_mut::<TcpSocket>(*h).abort();
        }
        // 要 4 空闲，但 cap=3：建到 total=3 即停，返回 Capped。
        let err = reg
            .ensure_spare_listeners(443, 4, &mut sockets, &mut ctxs)
            .unwrap_err();
        assert!(matches!(err, RegistryError::Capped));
        assert_eq!(reg.handles_for_port(443).len(), 3);
    }

    /// review #1/#2：reap_dead_slots 回收已用过且已死的槽（abort→Closed），rearm 回 Listen +
    /// release 其 fake-IP；空闲 Listen 槽（ctx.state==Listening）不被回收。
    #[test]
    fn reap_dead_slots_rearms_closed_slot_and_releases_fake_ip() {
        let mut sockets = SocketSet::new(vec![]);
        let mut ctxs: HashMap<SocketHandle, SocketCtx> = HashMap::new();
        let mut reg = ListenerRegistry::new(2);
        reg.ensure_port(443, &mut sockets, &mut ctxs).unwrap();
        let mut pool = FakeIpPool::new();
        let ip = pool.alloc("x.com", 0);
        pool.acquire(ip, 0);

        let handles: Vec<SocketHandle> = reg.handles_for_port(443).to_vec();
        let dead = handles[0];
        let idle_listen = handles[1];
        // dead 槽：模拟被用过后本地关闭（abort → Closed）+ 持有 fake-IP。
        sockets.get_mut::<TcpSocket>(dead).abort();
        {
            let ctx = ctxs.get_mut(&dead).unwrap();
            ctx.state = SocketState::Relaying;
            ctx.fake_ip = Some(ip);
        }
        // idle_listen 槽：空闲监听（ctx.state 默认 Listening）→ 不该被回收。

        let reaped = reap_dead_slots(&reg, &mut sockets, &mut ctxs, &mut pool, 1);
        assert_eq!(reaped, 1, "只回收 1 个死槽");
        let dead_ctx = ctxs.get(&dead).unwrap();
        assert_eq!(dead_ctx.state, SocketState::Listening, "死槽回 Listening");
        assert!(dead_ctx.fake_ip.is_none(), "死槽 fake-IP 已 release");
        assert_eq!(pool.sweep(1000, 300), 1, "release 后映射可回收");
        assert_eq!(
            ctxs.get(&idle_listen).unwrap().state,
            SocketState::Listening,
            "空闲 Listen 槽不动"
        );
    }

    #[test]
    fn reap_predicate_preserves_active_async_open() {
        let mut pending = SocketCtx::new(443);
        pending.state = SocketState::HandshakePending;
        assert!(
            !should_reap_slot(&pending, TcpState::Established, true, 0),
            "活跃 async-open 槽不能因 uplink_tx=None 被误 reap"
        );

        let mut opening = SocketCtx::new(443);
        opening.state = SocketState::OpeningRemote;
        assert!(
            should_reap_slot(&opening, TcpState::Established, true, 0),
            "旧 inline OpeningRemote 且无 uplink_tx 仍应视为卡住可 reap"
        );

        let listening = SocketCtx::new(443);
        assert!(
            !should_reap_slot(&listening, TcpState::Closed, false, 0),
            "空闲 Listening 槽永不 reap"
        );
    }

    #[test]
    fn reap_predicate_preserves_active_closewait_pending_downlink() {
        let mut ctx = SocketCtx::new(443);
        ctx.state = SocketState::Relaying;
        ctx.downlink_pending = vec![1, 2, 3];

        assert!(
            !should_reap_slot(&ctx, TcpState::CloseWait, true, 0),
            "active CloseWait with pending downlink must keep flushing instead of aborting"
        );

        ctx.downlink_pending.clear();
        assert!(
            should_reap_slot(&ctx, TcpState::CloseWait, true, 0),
            "CloseWait is reapable again once pending downlink drains"
        );

        ctx.downlink_pending = vec![1, 2, 3];
        assert!(
            should_reap_slot(&ctx, TcpState::Closed, false, 0),
            "inactive sockets still reap even if pending bytes can no longer be delivered"
        );
    }

    #[test]
    fn reap_predicate_preserves_active_closewait_live_relay() {
        let (tx, _rx) = mpsc::channel(1);
        let mut ctx = SocketCtx::new(443);
        ctx.state = SocketState::Relaying;
        ctx.uplink_tx = Some(tx);

        assert!(
            !should_reap_slot(&ctx, TcpState::CloseWait, true, 0),
            "active CloseWait with a live relay may still receive reverse/downlink bytes"
        );

        ctx.uplink_tx = None;
        assert!(
            should_reap_slot(&ctx, TcpState::CloseWait, true, 0),
            "CloseWait is reapable once there is no live relay and no pending downlink"
        );
    }

    #[test]
    fn reap_predicate_preserves_closewait_after_local_finish() {
        let mut ctx = SocketCtx::new(443);
        ctx.state = SocketState::Relaying;
        ctx.local_fin_sent = true;
        ctx.uplink_tx = None;

        assert!(
            !should_reap_slot(&ctx, TcpState::CloseWait, true, 0),
            "post-Finish CloseWait must wait for the relay close event instead of dead-slot reap"
        );
    }

    #[test]
    fn reap_predicate_graces_deferred_close_pending_downlink() {
        let mut ctx = SocketCtx::new(443);
        ctx.state = SocketState::Closing;
        ctx.downlink_pending = vec![1, 2, 3];
        ctx.pending_relay_close = Some(RelayClose {
            epoch: 9,
            direction: "remote_to_local",
            reason: "remote_eof",
        });
        ctx.pending_relay_close_since_secs = Some(10);

        assert!(
            !should_reap_slot(&ctx, TcpState::Closed, false, 14),
            "deferred close pending should get a short grace window to flush"
        );
        assert!(
            should_reap_slot(&ctx, TcpState::Closed, false, 15),
            "deferred close pending should still be bounded after grace"
        );

        ctx.pending_relay_close = None;
        ctx.pending_relay_close_since_secs = None;
        assert!(
            should_reap_slot(&ctx, TcpState::Closed, false, 14),
            "non-deferred inactive pending remains hard-reapable"
        );
    }

    #[test]
    fn remote_payload_allowed_after_local_finish_without_uplink_sender() {
        let mut ctx = SocketCtx::new(443);
        ctx.state = SocketState::Relaying;
        ctx.local_fin_sent = true;
        ctx.uplink_tx = None;

        assert!(
            relay_allows_remote_payload(&ctx),
            "post-Finish read-only relay must still accept remote payloads"
        );
    }

    /// 刀14d：本地已关闭的 in-flight open 必须回收、bump epoch，并让迟到 `HandshakeDone`
    /// 被 epoch guard 丢弃。
    #[tokio::test]
    async fn reap_dead_async_open_drops_stale_result() {
        let mut sockets = SocketSet::new(vec![]);
        let mut ctxs: HashMap<SocketHandle, SocketCtx> = HashMap::new();
        let mut reg = ListenerRegistry::new(1);
        reg.ensure_port(443, &mut sockets, &mut ctxs).unwrap();
        let handles: Vec<SocketHandle> = reg.handles_for_port(443).to_vec();
        let closed_pending = handles[0];

        let mut pool = FakeIpPool::new();
        let ip = pool.alloc("x.com", 0);
        pool.acquire(ip, 0);

        {
            let ctx = ctxs.get_mut(&closed_pending).unwrap();
            ctx.state = SocketState::HandshakePending;
            ctx.conn_epoch = 9;
            ctx.fake_ip = Some(ip);
        }
        sockets.get_mut::<TcpSocket>(closed_pending).abort();

        let reaped = reap_dead_slots(&reg, &mut sockets, &mut ctxs, &mut pool, 1);
        assert_eq!(reaped, 1, "应回收本地已关闭的 async-open 槽");

        let closed_ctx = ctxs.get(&closed_pending).unwrap();
        assert_eq!(closed_ctx.state, SocketState::Listening);
        assert_eq!(
            closed_ctx.conn_epoch, 10,
            "reap 应 bump epoch 让迟到 open 结果失效"
        );
        assert!(closed_ctx.fake_ip.is_none(), "reap 应释放 in-flight flow fake-IP");
        assert_eq!(pool.sweep(1000, 300), 1, "release 后映射可回收");

        let (global_tx, _grx) = mpsc::channel(8);
        let stale = HandshakeDone {
            handle: closed_pending,
            epoch: 9,
            result: Ok(Box::new(tokio::io::duplex(64).0)),
        };
        handle_handshake_done(
            stale,
            &mut sockets,
            &mut ctxs,
            &global_tx,
            &mut pool,
            2,
            &Metrics::new(),
        );
        let closed_ctx = ctxs.get(&closed_pending).unwrap();
        assert_eq!(
            closed_ctx.state,
            SocketState::Listening,
            "迟到 open 结果不应装到 rearm 后的新一代 socket"
        );
        assert!(closed_ctx.uplink_tx.is_none());
    }

    #[test]
    fn rearm_socket_restores_listening_state_and_releases_fake_ip() {
        let spec = ListenerSpec { local_port: 80 };
        let mut socket = build_listener_socket(&spec);
        let (tx, _rx) = mpsc::channel(1);
        let mut pool = FakeIpPool::new();
        let ip = pool.alloc("x.com", 0);
        pool.acquire(ip, 0); // 模拟本 flow 已 acquire
        let mut downlink_diag = TcpDownlinkDiag::default();
        downlink_diag.note_remote_payload(128, 128);
        let mut ctx = SocketCtx {
            local_port: 80,
            state: SocketState::Relaying,
            uplink_tx: Some(tx),
            local_fin_sent: true,
            downlink_pending: Vec::new(),
            pending_relay_close: None,
            pending_relay_close_since_secs: None,
            fake_ip: Some(ip),
            conn_epoch: 7,
            uplink_buffer: vec![1, 2, 3],
            downlink_diag,
        };

        rearm_socket(&mut socket, &mut ctx, &mut pool, 1);

        assert_eq!(ctx.state, SocketState::Listening);
        assert!(ctx.uplink_tx.is_none());
        assert!(!ctx.local_fin_sent, "rearm 应清空本地 FIN 发送状态");
        assert!(ctx.fake_ip.is_none(), "rearm 应清空 fake_ip");
        assert!(ctx.uplink_buffer.is_empty(), "rearm 应清空 uplink_buffer（M3 patch）");
        assert_eq!(
            ctx.downlink_diag.downlink_pending_high_water, 0,
            "rearm 应清空当前 flow 的 downlink diagnostics"
        );
        assert_eq!(ctx.conn_epoch, 8, "rearm 应 bump conn_epoch（让在飞 open 失效，M3）");
        // refcount 已归零 → idle 超 TTL 可回收（证明 rearm 走了 release）。
        assert_eq!(pool.sweep(1000, 300), 1);
    }

    #[test]
    fn relay_closed_epoch_guard_drops_stale_close() {
        let mut sockets = SocketSet::new(vec![]);
        let handle = sockets.add(build_listener_socket(&ListenerSpec { local_port: 12345 }));
        let (tx, _rx) = mpsc::channel(1);
        let mut ctx = SocketCtx::new(12345);
        ctx.state = SocketState::Relaying;
        ctx.conn_epoch = 22;
        ctx.uplink_tx = Some(tx);
        let mut socket_ctxs = HashMap::new();
        socket_ctxs.insert(handle, ctx);
        let mut pool = FakeIpPool::new();

        handle_relay_closed(
            handle,
            RelayClose {
                epoch: 21,
                direction: "timer",
                reason: "idle_timeout",
            },
            &mut sockets,
            &mut socket_ctxs,
            &mut pool,
            1,
        );

        let ctx = socket_ctxs.get(&handle).unwrap();
        assert_eq!(ctx.state, SocketState::Relaying, "stale close must not rearm a new epoch");
        assert_eq!(ctx.conn_epoch, 22);
        assert!(ctx.uplink_tx.is_some());
    }

    #[test]
    fn relay_closed_with_pending_downlink_defers_rearm() {
        let mut sockets = SocketSet::new(vec![]);
        let handle = sockets.add(build_listener_socket(&ListenerSpec { local_port: 12345 }));
        let (tx, _rx) = mpsc::channel(1);
        let mut ctx = SocketCtx::new(12345);
        ctx.state = SocketState::Relaying;
        ctx.conn_epoch = 5;
        ctx.uplink_tx = Some(tx);
        ctx.downlink_pending = vec![1, 2, 3];
        let mut socket_ctxs = HashMap::new();
        socket_ctxs.insert(handle, ctx);
        let mut pool = FakeIpPool::new();

        let keep_dirty = handle_relay_closed(
            handle,
            RelayClose {
                epoch: 5,
                direction: "remote_to_local",
                reason: "remote_eof",
            },
            &mut sockets,
            &mut socket_ctxs,
            &mut pool,
            1,
        );

        let ctx = socket_ctxs.get_mut(&handle).unwrap();
        assert!(keep_dirty, "pending downlink must stay dirty until it drains");
        assert_eq!(ctx.state, SocketState::Closing);
        assert!(ctx.uplink_tx.is_none());
        assert_eq!(ctx.downlink_pending, vec![1, 2, 3]);
        assert!(ctx.pending_relay_close.is_some());
        assert_eq!(ctx.pending_relay_close_since_secs, Some(1));
        assert_eq!(ctx.conn_epoch, 5, "deferred close must not bump epoch before drain");

        ctx.downlink_pending.clear();
        let socket = sockets.get_mut::<TcpSocket>(handle);
        assert!(finish_deferred_relay_close_if_drained(
            handle,
            socket,
            ctx,
            &mut pool,
            2
        ));
        assert_eq!(ctx.state, SocketState::Listening);
        assert!(ctx.pending_relay_close.is_none());
        assert!(ctx.pending_relay_close_since_secs.is_none());
        assert_eq!(ctx.conn_epoch, 6);
    }

    // ---- 刀9 M3：握手并发化（epoch 防串话 / buffer 上限 / flush 保序 / 失败 rearm）----

    /// buffer_uplink 256KB 硬上限：填满后溢出包被丢弃、buffer 不变。
    #[test]
    fn uplink_buffer_enforces_cap() {
        let mut ctx = SocketCtx::new(80);
        assert!(ctx.buffer_uplink(&[0u8; 1000]));
        assert_eq!(ctx.uplink_buffer.len(), 1000);
        let fill = vec![0u8; MAX_UPLINK_BUFFER - 1000];
        assert!(ctx.buffer_uplink(&fill), "恰好填到上限应接受");
        assert_eq!(ctx.uplink_buffer.len(), MAX_UPLINK_BUFFER);
        assert!(!ctx.buffer_uplink(&[0u8; 1]), "超上限 1B 应丢弃");
        assert_eq!(ctx.uplink_buffer.len(), MAX_UPLINK_BUFFER, "丢弃不改变 buffer");
    }

    fn mk_pending_ctx(epoch: u64) -> SocketCtx {
        let mut ctx = SocketCtx::new(12345);
        ctx.state = SocketState::HandshakePending;
        ctx.conn_epoch = epoch;
        ctx
    }

    /// epoch 防串话：迟到结果（epoch 不匹配）丢弃、不装到 socket；匹配 + Ok → 安装 relay。
    #[tokio::test]
    async fn handshake_done_epoch_guard_drops_stale_then_installs_match() {
        let mut sockets = SocketSet::new(vec![]);
        let handle = sockets.add(build_listener_socket(&ListenerSpec { local_port: 12345 }));
        let mut socket_ctxs = HashMap::new();
        socket_ctxs.insert(handle, mk_pending_ctx(5));
        let (global_tx, _grx) = mpsc::channel(8);
        let mut pool = FakeIpPool::new();

        // 迟到 epoch 3 ≠ 5 → 丢弃，状态/uplink_tx 不变。
        let stale = HandshakeDone { handle, epoch: 3, result: Ok(Box::new(tokio::io::duplex(64).0)) };
        handle_handshake_done(stale, &mut sockets, &mut socket_ctxs, &global_tx, &mut pool, 0, &Metrics::new());
        let ctx = socket_ctxs.get(&handle).unwrap();
        assert_eq!(ctx.state, SocketState::HandshakePending, "迟到 epoch → 不装、状态不变");
        assert!(ctx.uplink_tx.is_none(), "迟到 epoch → 不安装 uplink_tx");

        // 匹配 epoch 5 + Ok → 安装 relay。
        let ok = HandshakeDone { handle, epoch: 5, result: Ok(Box::new(tokio::io::duplex(64).0)) };
        handle_handshake_done(ok, &mut sockets, &mut socket_ctxs, &global_tx, &mut pool, 0, &Metrics::new());
        let ctx = socket_ctxs.get(&handle).unwrap();
        assert_eq!(ctx.state, SocketState::Relaying, "匹配 epoch → 安装 relay 进 Relaying");
        assert!(ctx.uplink_tx.is_some(), "匹配 epoch → 安装 uplink_tx");
    }

    /// Async open 失败（匹配 epoch + Err）→ rearm：回 Listening、释放 spawn 时 acquire 的 fake-IP、清 buffer。
    #[tokio::test]
    async fn handshake_done_failure_rearms_and_releases_fakeip() {
        let mut sockets = SocketSet::new(vec![]);
        let handle = sockets.add(build_listener_socket(&ListenerSpec { local_port: 12345 }));
        let mut pool = FakeIpPool::new();
        let ip = pool.alloc("x.com", 0);
        pool.acquire(ip, 0); // 模拟 spawn 路径已 acquire
        let mut ctx = mk_pending_ctx(2);
        ctx.fake_ip = Some(ip);
        ctx.buffer_uplink(b"buffered");
        let mut socket_ctxs = HashMap::new();
        socket_ctxs.insert(handle, ctx);
        let (global_tx, _grx) = mpsc::channel(8);

        let err = HandshakeDone { handle, epoch: 2, result: Err(ClientError::Reality("open 失败".into())) };
        handle_handshake_done(err, &mut sockets, &mut socket_ctxs, &global_tx, &mut pool, 1, &Metrics::new());
        let ctx = socket_ctxs.get(&handle).unwrap();
        assert_eq!(ctx.state, SocketState::Listening, "失败 → rearm 回 Listening");
        assert!(ctx.fake_ip.is_none(), "失败 rearm 释放 fake-IP（平衡 spawn 的 acquire）");
        assert!(ctx.uplink_buffer.is_empty(), "rearm 清 buffer");
        assert_eq!(pool.sweep(1000, 300), 1, "refcount 归零 → 可回收（acquire/release 平衡）");
    }

    /// Async open 成功 → 按序 flush open 期间缓存的上行字节到 relay 流。
    #[tokio::test]
    async fn handshake_done_flushes_buffered_uplink_in_order() {
        use tokio::io::AsyncReadExt;
        let mut sockets = SocketSet::new(vec![]);
        let handle = sockets.add(build_listener_socket(&ListenerSpec { local_port: 12345 }));
        let mut ctx = mk_pending_ctx(1);
        ctx.buffer_uplink(b"HELLO-BUFFERED");
        let mut socket_ctxs = HashMap::new();
        socket_ctxs.insert(handle, ctx);
        let (global_tx, _grx) = mpsc::channel(8);
        let mut pool = FakeIpPool::new();

        // near = relay 写入端（上游流）；far = 测试读端（模拟出口收到的上行）。
        let (near, mut far) = tokio::io::duplex(1024);
        let ok = HandshakeDone { handle, epoch: 1, result: Ok(Box::new(near)) };
        handle_handshake_done(ok, &mut sockets, &mut socket_ctxs, &global_tx, &mut pool, 0, &Metrics::new());

        let mut got = vec![0u8; b"HELLO-BUFFERED".len()];
        far.read_exact(&mut got).await.unwrap();
        assert_eq!(&got, b"HELLO-BUFFERED", "open 成功后按序 flush 缓存的上行字节");
    }

    #[test]
    fn tun_runtime_config_defaults_match_stage9_behavior() {
        let config = TunRuntimeConfig::from_sources(None).expect("config should load");
        // Stage 9 drops local_port; pool_size default lowered to 2 (per-port now).
        assert_eq!(config.listener.pool_size, 2);
        assert_eq!(config.tun_mtu, DEFAULT_TUN_MTU);
        // 刀11：from_sources（harness/测试路径）恒用默认快照周期。
        assert_eq!(config.metrics_secs, METRICS_SNAPSHOT_SECS);
    }

    /// 刀11：MINI_VPN_METRICS_SECS 解析——有效正整数采用；0/非数字/缺失回落默认（防 interval panic）。
    #[test]
    fn parse_metrics_secs_clamps_and_defaults() {
        assert_eq!(parse_metrics_secs(Some("5")), 5);
        assert_eq!(parse_metrics_secs(Some("  10 ")), 10);
        assert_eq!(parse_metrics_secs(Some("0")), METRICS_SNAPSHOT_SECS, "0 必回落（否则 interval panic）");
        assert_eq!(parse_metrics_secs(Some("abc")), METRICS_SNAPSHOT_SECS);
        assert_eq!(parse_metrics_secs(Some("")), METRICS_SNAPSHOT_SECS);
        assert_eq!(parse_metrics_secs(None), METRICS_SNAPSHOT_SECS);
    }

    #[test]
    fn parse_profile_loop_only_truthy_enables() {
        assert!(parse_profile_loop(Some("1")));
        assert!(parse_profile_loop(Some("true")));
        assert!(parse_profile_loop(Some(" TRUE ")));
        assert!(!parse_profile_loop(Some("0")));
        assert!(!parse_profile_loop(Some("false")));
        assert!(!parse_profile_loop(Some("yes")));
        assert!(!parse_profile_loop(Some("")));
        assert!(!parse_profile_loop(None), "缺省关——默认 NoopSink 零开销路径");
    }

    /// 刀14c：TUN MTU 解析。1200 是 US-client suite 的测试基准；默认仍 1500 以保持零惊喜。
    #[test]
    fn parse_tun_mtu_accepts_safe_range_and_defaults() {
        assert_eq!(parse_tun_mtu(Some("1200")), 1200);
        assert_eq!(parse_tun_mtu(Some("  1500 ")), 1500);
        assert_eq!(parse_tun_mtu(Some("576")), 576);
        assert_eq!(parse_tun_mtu(Some("9000")), 9000);
        assert_eq!(parse_tun_mtu(Some("0")), DEFAULT_TUN_MTU);
        assert_eq!(parse_tun_mtu(Some("575")), DEFAULT_TUN_MTU);
        assert_eq!(parse_tun_mtu(Some("9001")), DEFAULT_TUN_MTU);
        assert_eq!(parse_tun_mtu(Some("abc")), DEFAULT_TUN_MTU);
        assert_eq!(parse_tun_mtu(Some("")), DEFAULT_TUN_MTU);
        assert_eq!(parse_tun_mtu(None), DEFAULT_TUN_MTU);
    }

    #[test]
    fn tun_mtu_for_config_preserves_runtime_mtu() {
        assert_eq!(tun_mtu_for_config(1200), 1200);
        assert_eq!(tun_mtu_for_config(DEFAULT_TUN_MTU), 1500);
        assert_eq!(tun_mtu_for_config(MAX_TUN_MTU), 9000);
    }

    #[test]
    fn tcp_downlink_diag_tracks_pending_acceptance_and_flush_failures() {
        let mut diag = TcpDownlinkDiag::default();
        diag.note_remote_payload(100, 100);
        diag.note_send_slice_ok(60, 40);
        diag.note_send_slice_ok(0, 40);
        diag.note_send_slice_error();
        diag.note_tun_flush(true);
        diag.note_tun_flush(false);

        assert_eq!(diag.remote_to_global_rx_bytes, 100);
        assert_eq!(diag.downlink_pending_high_water, 100);
        assert_eq!(diag.send_slice_calls, 3);
        assert_eq!(diag.send_slice_accepted_bytes, 60);
        assert_eq!(diag.send_slice_zero, 1);
        assert_eq!(diag.send_slice_errors, 1);
        assert_eq!(diag.tun_flush_tx_calls, 2);
        assert_eq!(diag.tun_flush_tx_failures, 1);
    }

    #[test]
    fn relay_task_diag_tracks_bytes_and_global_rx_pressure() {
        let mut diag = RelayTaskDiag::default();
        diag.note_uplink_write(40);
        diag.note_remote_read(100);
        diag.note_global_rx_wait(
            std::time::Duration::from_micros(25),
            std::time::Duration::from_micros(10),
        );
        diag.note_global_rx_wait(
            std::time::Duration::from_micros(5),
            std::time::Duration::from_micros(10),
        );

        assert_eq!(diag.uplink_bytes, 40);
        assert_eq!(diag.remote_to_global_rx_bytes, 100);
        assert_eq!(diag.remote_reads, 1);
        assert_eq!(diag.global_rx_wait_max_micros, 25);
        assert_eq!(diag.global_rx_pressure_events, 1);
    }

    /// 刀13 ①：MINI_VPN_TRACE 解析——`1`/`true`（去空白、不区分大小写）开；其它/缺省关
    /// （默认关 = 主循环热路径零 stdout；翻 flag 恢复全部诊断打印）。
    #[test]
    fn parse_trace_only_truthy_enables() {
        assert!(parse_trace(Some("1")));
        assert!(parse_trace(Some("true")));
        assert!(parse_trace(Some(" TRUE ")));
        assert!(!parse_trace(Some("0")), "0 必须关——否则 MINI_VPN_TRACE=0 反而开是 footgun");
        assert!(!parse_trace(Some("false")));
        assert!(!parse_trace(Some("yes")));
        assert!(!parse_trace(Some("")));
        assert!(!parse_trace(None), "缺省关——热路径默认静默");
    }

    #[test]
    fn tun_runtime_config_defaults_profile_loop_off() {
        // from_sources（harness/测试）恒关；只有 from_env 读 MINI_VPN_PROFILE_LOOP。
        let cfg = TunRuntimeConfig::from_sources(None).expect("valid config");
        assert!(!cfg.profile_loop);
    }

    #[test]
    fn tun_runtime_config_rejects_zero_pool_size() {
        let err = TunRuntimeConfig::from_sources(Some("0")).expect_err("zero pool size should fail");
        assert!(err.to_string().contains("at least 1"));
    }

    #[test]
    fn tun_runtime_config_accepts_pool_size_override() {
        let config = TunRuntimeConfig::from_sources(Some("3")).expect("valid config should load");
        assert_eq!(config.listener.pool_size, 3);
    }

    fn udp_pkt(dst: [u8; 4], dst_port: u16) -> Vec<u8> {
        let mut v = Vec::new();
        etherparse::PacketBuilder::ipv4([10, 0, 0, 1], dst, 64)
            .udp(50000, dst_port)
            .write(&mut v, &[])
            .unwrap();
        v
    }

    /// 刀5：classify_inbound 把**任意** :53 路由到 Dns（裸包伪造），:853/:443 仍走 UdpRelay。
    #[test]
    fn classify_routes_dns_relay_and_other() {
        // 任意 resolver 的 :53 → Dns（不再只限 198.18.0.1）。
        assert_eq!(classify_inbound(&udp_pkt([198, 18, 0, 1], 53)), Inbound::Dns);
        assert_eq!(classify_inbound(&udp_pkt([8, 8, 8, 8], 53)), Inbound::Dns);
        assert_eq!(classify_inbound(&udp_pkt([1, 1, 1, 1], 53)), Inbound::Dns);
        // 其它 UDP（含 DoT/DoQ :853、DoH3/视频 :443）→ UdpRelay（刀4 Block 由 resolve_target 判）。
        assert_eq!(
            classify_inbound(&udp_pkt([198, 18, 0, 5], 443)),
            Inbound::UdpRelay
        );
        assert_eq!(classify_inbound(&udp_pkt([1, 1, 1, 1], 853)), Inbound::UdpRelay);
        // TCP SYN / 垃圾 → Other。
        let pkt = build_ipv4_tcp([10, 0, 0, 1], [1, 1, 1, 1], 60000, 443, true, false);
        assert_eq!(classify_inbound(&pkt), Inbound::Other);
        assert_eq!(classify_inbound(&[0u8; 4]), Inbound::Other);
    }

    /// 构造一个最小 DNS 查询(单 question, RD=1, QCLASS=IN)——刀5 forge 测试用。
    fn dns_query(id: u16, qname: &str, qtype: u16) -> Vec<u8> {
        let mut v = Vec::new();
        v.extend_from_slice(&id.to_be_bytes());
        v.extend_from_slice(&0x0100u16.to_be_bytes()); // RD=1
        v.extend_from_slice(&1u16.to_be_bytes()); // QDCOUNT
        v.extend_from_slice(&[0u8; 6]); // AN/NS/AR COUNT = 0
        for label in qname.split('.') {
            v.push(label.len() as u8);
            v.extend_from_slice(label.as_bytes());
        }
        v.push(0);
        v.extend_from_slice(&qtype.to_be_bytes());
        v.extend_from_slice(&1u16.to_be_bytes()); // QCLASS IN
        v
    }

    /// 取一个伪造回包的 A 记录 RDATA（响应末 4 字节 = fake-IP）。
    fn reply_rdata_ip(reply: &[u8]) -> Ipv4Addr {
        let g = parse_inbound_udp(reply).expect("回包应是合法 IPv4/UDP");
        let p = g.payload;
        Ipv4Addr::new(p[p.len() - 4], p[p.len() - 3], p[p.len() - 2], p[p.len() - 1])
    }

    /// 刀5 T1：任意 resolver 的明文 A 查询 → 本地伪造 fake-IP 回包；
    /// 回包 src=被查询的 resolver:53、dst=app、RDATA=fake-IP（不依赖 198.18.0.1）。
    #[test]
    fn forge_dns_reply_forges_fake_ip_for_any_resolver() {
        let mut pool = FakeIpPool::new();
        let app = (Ipv4Addr::new(10, 0, 0, 2), 50000);
        // app 把 A 查询发给 8.8.8.8:53（非我方 resolver）。
        let query = dns_query(0x1234, "example.com", dns::QTYPE_A);
        let pkt = build_udp_ip_packet(app, (Ipv4Addr::new(8, 8, 8, 8), 53), &query);
        let udp = parse_inbound_udp(&pkt).unwrap();

        let reply = forge_dns_reply(&udp, &mut pool, 0).expect("A 查询应被伪造");
        let r = parse_inbound_udp(&reply).expect("回包应是合法 IPv4/UDP");
        // 源 = 被查询的 resolver（否则 app socket 丢弃），目的 = app 原端点。
        assert_eq!((r.src_ip, r.src_port), (Ipv4Addr::new(8, 8, 8, 8), 53));
        assert_eq!((r.dst_ip, r.dst_port), app);
        // RDATA = fake-IP，落 198.18/15 且能 resolve 回域名。
        let fake = reply_rdata_ip(&reply);
        assert!(pool.is_fake(fake), "RDATA 应是 fake-IP, got {fake}");
        assert_eq!(pool.resolve(fake).as_deref(), Some("example.com"));
    }

    /// 刀5 T1：dst 落 fake-IP 段（app 把 resolver 配成被 fake 的域名）也照样伪造，不 Refuse。
    #[test]
    fn forge_dns_reply_forges_even_for_fake_range_resolver() {
        let mut pool = FakeIpPool::new();
        let app = (Ipv4Addr::new(10, 0, 0, 2), 50001);
        let resolver = (Ipv4Addr::new(198, 18, 0, 9), 53); // fake 段内
        let query = dns_query(1, "foo.com", dns::QTYPE_A);
        let pkt = build_udp_ip_packet(app, resolver, &query);
        let udp = parse_inbound_udp(&pkt).unwrap();
        let reply = forge_dns_reply(&udp, &mut pool, 0).expect("应伪造");
        let r = parse_inbound_udp(&reply).unwrap();
        assert_eq!((r.src_ip, r.src_port), resolver);
        assert!(pool.is_fake(reply_rdata_ip(&reply)));
    }

    /// 刀5 T1：AAAA 查询 → NODATA（ANCOUNT=0），不分配 fake-IP。
    #[test]
    fn forge_dns_reply_aaaa_is_nodata() {
        let mut pool = FakeIpPool::new();
        let app = (Ipv4Addr::new(10, 0, 0, 2), 50002);
        let query = dns_query(2, "example.com", dns::QTYPE_AAAA);
        let pkt = build_udp_ip_packet(app, (Ipv4Addr::new(1, 1, 1, 1), 53), &query);
        let udp = parse_inbound_udp(&pkt).unwrap();
        let reply = forge_dns_reply(&udp, &mut pool, 0).expect("AAAA 应回 NODATA（非 None）");
        let r = parse_inbound_udp(&reply).unwrap();
        // 响应 payload 偏移 6..8 = ANCOUNT。
        assert_eq!(u16::from_be_bytes([r.payload[6], r.payload[7]]), 0, "NODATA ANCOUNT=0");
    }

    /// 刀5 T1：不可解析的 :53 payload → None（调用方丢弃，绝不转发真 DNS = 不泄漏）。
    #[test]
    fn forge_dns_reply_unparseable_is_none() {
        let mut pool = FakeIpPool::new();
        let app = (Ipv4Addr::new(10, 0, 0, 2), 50003);
        let pkt = build_udp_ip_packet(app, (Ipv4Addr::new(8, 8, 8, 8), 53), &[0u8; 4]); // 截断
        let udp = parse_inbound_udp(&pkt).unwrap();
        assert!(forge_dns_reply(&udp, &mut pool, 0).is_none());
    }

    /// 刀5 T1：同域名两次查询 → 同一 fake-IP（稳定复用，DNS 给的 IP 与 TCP 时查表一致）。
    #[test]
    fn forge_dns_reply_stable_fake_ip_per_domain() {
        let mut pool = FakeIpPool::new();
        let app = (Ipv4Addr::new(10, 0, 0, 2), 50004);
        let res = (Ipv4Addr::new(8, 8, 8, 8), 53);
        let p1 = build_udp_ip_packet(app, res, &dns_query(1, "stable.com", dns::QTYPE_A));
        let p2 = build_udp_ip_packet(app, res, &dns_query(2, "stable.com", dns::QTYPE_A));
        let r1 = forge_dns_reply(&parse_inbound_udp(&p1).unwrap(), &mut pool, 0).unwrap();
        let r2 = forge_dns_reply(&parse_inbound_udp(&p2).unwrap(), &mut pool, 1).unwrap();
        assert_eq!(reply_rdata_ip(&r1), reply_rdata_ip(&r2));
    }

    /// 最小 TunIo mock（刀11 T4）：只支持 `handle_dns_hijack` 用到的 `inject_ip_packet`（记录注入回包）
    /// + `flush_tx`（no-op）；`Device` 超 trait 的 receive/transmit 在本测试路径不被调用 → 返 None。
    #[derive(Default)]
    struct DnsInjectRecorder {
        injected: Vec<Vec<u8>>,
    }
    struct DeadToken;
    impl smoltcp::phy::RxToken for DeadToken {
        fn consume<R, F: FnOnce(&mut [u8]) -> R>(self, _f: F) -> R {
            unreachable!("mock 不产生 rx")
        }
    }
    impl smoltcp::phy::TxToken for DeadToken {
        fn consume<R, F: FnOnce(&mut [u8]) -> R>(self, _len: usize, _f: F) -> R {
            unreachable!("mock 不经 smoltcp transmit")
        }
    }
    impl smoltcp::phy::Device for DnsInjectRecorder {
        type RxToken<'a> = DeadToken;
        type TxToken<'a> = DeadToken;
        fn capabilities(&self) -> smoltcp::phy::DeviceCapabilities {
            smoltcp::phy::DeviceCapabilities::default()
        }
        fn receive(
            &mut self,
            _t: smoltcp::time::Instant,
        ) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
            None
        }
        fn transmit(&mut self, _t: smoltcp::time::Instant) -> Option<Self::TxToken<'_>> {
            None
        }
    }
    impl TunIo for DnsInjectRecorder {
        async fn wait_for_rx(&mut self) -> std::io::Result<()> {
            unreachable!("测试直调 handle_dns_hijack，不进 wait_for_rx")
        }
        fn rx_peek(&self) -> Option<&[u8]> {
            None
        }
        fn rx_take(&mut self) -> Option<bytes::BytesMut> {
            None
        }
        async fn flush_tx(&mut self) -> std::io::Result<()> {
            Ok(())
        }
        fn inject_ip_packet(&mut self, pkt: &[u8]) {
            self.injected.push(pkt.to_vec());
        }
    }

    /// 刀11 T4：`handle_dns_hijack` 把 forge/drop 结局映射到 dns_forged/dns_dropped 计数。
    #[tokio::test]
    async fn handle_dns_hijack_counts_forge_and_drop() {
        let metrics = Metrics::new();
        let mut pool = FakeIpPool::new();
        let mut dev = DnsInjectRecorder::default();
        let app = (Ipv4Addr::new(10, 0, 0, 2), 50000);
        let res = (Ipv4Addr::new(8, 8, 8, 8), 53);

        // 可解析 A 查询 → forge++、注入一个回包。
        let q = build_udp_ip_packet(app, res, &dns_query(1, "example.com", dns::QTYPE_A));
        handle_dns_hijack(&q, &mut pool, &mut dev, 0, &metrics).await;
        let s = metrics.snapshot(0, 0);
        assert_eq!((s.dns_forged, s.dns_dropped), (1, 0));
        assert_eq!(dev.injected.len(), 1, "forge 应注入一个回包");

        // 不可解析 payload（截断）→ drop++、不注入。
        let bad = build_udp_ip_packet(app, res, &[0u8; 4]);
        handle_dns_hijack(&bad, &mut pool, &mut dev, 0, &metrics).await;
        let s = metrics.snapshot(0, 0);
        assert_eq!((s.dns_forged, s.dns_dropped), (1, 1), "drop 不增 forge");
        assert_eq!(dev.injected.len(), 1, "drop 不注入");
    }

    /// 刀11 T5：`publish_gauges` 从 loop-owned 状态重算 gauge 并发布——
    /// active_relays 只数 Relaying、fake-IP (total,active)、failover_leg 哨兵 → None。
    #[test]
    fn publish_gauges_samples_relaying_pool_and_leg() {
        use crate::metrics::{FailoverLegView, NO_FAILOVER};
        let mk = |s: SocketState| {
            let mut c = SocketCtx::new(0);
            c.state = s;
            c
        };
        let ctxs = [
            mk(SocketState::Relaying),
            mk(SocketState::Relaying),
            mk(SocketState::Listening),
            mk(SocketState::OpeningRemote), // 在飞、未成 relay → 不计 active
        ];
        let mut pool = FakeIpPool::new();
        let ip = pool.alloc("a.com", 0);
        pool.acquire(ip, 0); // 1 个有活跃 flow
        let _ = pool.alloc("b.com", 0); // 在册未 acquire

        let metrics = Metrics::new();
        publish_gauges(&metrics, ctxs.iter(), &pool, 1); // leg=Reality
        let s = metrics.snapshot(0, 0);
        assert_eq!(s.active_relays, 2, "只数 state==Relaying");
        assert_eq!((s.fake_ip_total, s.fake_ip_active), (2, 1));
        assert_eq!(s.failover_leg, FailoverLegView::Reality);

        // 非 failover 单腿上游传哨兵 → snapshot 视图 None。
        publish_gauges(&metrics, ctxs.iter(), &pool, NO_FAILOVER);
        assert_eq!(metrics.snapshot(0, 0).failover_leg, FailoverLegView::None);
    }

    /// 刀4：resolve_target 对加密 DNS 端点返回 Block，且精确不误伤普通 :443 / 零回归 Refuse。
    #[test]
    fn resolve_target_blocks_encrypted_dns() {
        use smoltcp::wire::IpEndpoint;
        let mut pool = FakeIpPool::new();
        let ep = |ip: [u8; 4], port: u16| {
            IpEndpoint::new(IpAddress::v4(ip[0], ip[1], ip[2], ip[3]), port)
        };

        // DoT/DoQ :853（任意 IP）→ Block。
        assert!(matches!(resolve_target(ep([1, 1, 1, 1], 853), &pool), TargetResolve::Block));
        assert!(matches!(
            resolve_target(ep([93, 184, 216, 34], 853), &pool),
            TargetResolve::Block
        ));

        // 刀5：TCP :53（明文 DNS over TCP，任意 IP）→ Block（RST，逼回落 UDP :53）。
        // 不变量：UDP :53 已被 classify_inbound 截到 Dns 路径、不到 resolve_target，故 port==53 只命中 TCP。
        assert!(matches!(resolve_target(ep([8, 8, 8, 8], 53), &pool), TargetResolve::Block));
        assert!(matches!(
            resolve_target(ep([198, 18, 0, 1], 53), &pool),
            TargetResolve::Block
        ));

        // DoH 经 fake-IP：dns.google:443 → Block；普通域名:443 → Direct；DoH 域名但 :80 → Direct（仅 :443）。
        let doh_fake = pool.alloc("dns.google", 0);
        assert!(matches!(resolve_target(ep(doh_fake.octets(), 443), &pool), TargetResolve::Block));
        assert!(matches!(resolve_target(ep(doh_fake.octets(), 80), &pool), TargetResolve::Direct { .. }));
        let normal_fake = pool.alloc("example.com", 0);
        assert!(matches!(
            resolve_target(ep(normal_fake.octets(), 443), &pool),
            TargetResolve::Direct { .. }
        ));

        // DoH 硬编 IP 1.1.1.1:443 → Block；普通真实 IP:443 → Direct。
        assert!(matches!(resolve_target(ep([1, 1, 1, 1], 443), &pool), TargetResolve::Block));
        assert!(matches!(
            resolve_target(ep([93, 184, 216, 34], 443), &pool),
            TargetResolve::Direct { .. }
        ));

        // fake-IP 段内无映射 → Refuse（零回归，不被 Block 吞掉）。
        assert!(matches!(
            resolve_target(IpEndpoint::new(IpAddress::v4(198, 18, 99, 99), 443), &pool),
            TargetResolve::Refuse
        ));
    }

    /// #1 脏集合驱动：`handles_for_port` 返回该端口 pool 的全部 handle；未注册端口空 slice。
    #[test]
    fn handles_for_port_returns_pool_handles() {
        let mut sockets = SocketSet::new(vec![]);
        let mut ctxs: HashMap<SocketHandle, SocketCtx> = HashMap::new();
        let mut reg = ListenerRegistry::new(3);
        reg.ensure_port(443, &mut sockets, &mut ctxs).unwrap();
        assert_eq!(reg.handles_for_port(443).len(), 3);
        assert!(reg.handles_for_port(8080).is_empty());
    }
}
