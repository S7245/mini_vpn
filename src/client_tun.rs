use crate::device::{TunIo, VirtualTunDevice};
use crate::dns::{self, Answer};
use crate::failover::FailoverUpstream;
use crate::fake_ip::FakeIpPool;
use crate::loop_profiler::LoopProfiler;
use crate::metrics::Metrics;
use crate::reality_upstream::RealityUpstream;
use crate::shared::{ClientError, TargetAddr};
use crate::tuic::{
    AssocTable, FragReassembler, TuicClientConfig, TuicUpstream, decode_packet_meta, encode_packet,
};
use crate::udp_relay::{
    FourTuple, UDP_FLOW_IDLE_SECS, UdpInbound, build_udp_ip_packet, parse_inbound_udp,
};
use crate::upstream::{DatagramUpstream, ProxyUpstream, RelayStream};
use smoltcp::iface::{Config as SmolConfig, Interface, SocketHandle, SocketSet};
use smoltcp::socket::tcp::{
    Socket as TcpSocket, SocketBuffer as TcpSocketBuffer, State as TcpState,
};
use smoltcp::wire::{IpAddress, IpCidr};
use std::net::Ipv4Addr;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt, ReadBuf};

use std::collections::{HashMap, HashSet};
use tokio::sync::{mpsc, watch};

pub(crate) const TCP_SOCKET_BUFFER_SIZE: usize = 65_535;
const MIN_TCP_SOCKET_BUFFER_BYTES: usize = 4 * 1024;
const MAX_TCP_SOCKET_BUFFER_BYTES: usize = 16 * 1024 * 1024;
const RELAY_CHANNEL_CAPACITY: usize = 1024;
const _: () = assert!(RELAY_CHANNEL_CAPACITY >= 1);
const RELAY_REMOTE_READ_BURST_MAX_CHUNKS: usize = 16;
const RELAY_REMOTE_READ_BURST_MAX_BYTES: usize = 512 * 1024;
const RELAY_REMOTE_READ_MIN_BATCH_BYTES: usize = 64 * 1024;
const RELAY_REMOTE_READ_ACK_DRAIN_PACKETS: usize = 1;
const RELAY_REMOTE_READ_ADAPTIVE_ACK_DRAIN_MAX_BYTES: usize = RELAY_REMOTE_READ_MIN_BATCH_BYTES;
const RELAY_REMOTE_READ_PRESSURE_MIN_PACKETS: usize = 16;
const RELAY_REMOTE_READ_PRESSURE_MIN_BYTES_FLOOR: usize = 8 * 1024;
const DOWNLINK_CREDIT_INITIAL_BATCH_BYTES: usize = RELAY_REMOTE_READ_MIN_BATCH_BYTES;
const DOWNLINK_CREDIT_PROGRESS_GAIN: usize = 2;
const DOWNLINK_CREDIT_MAX_NO_PROGRESS_STREAK: u8 = 8;
const _: () = assert!(RELAY_REMOTE_READ_BURST_MAX_CHUNKS >= 1);
const _: () = assert!(RELAY_REMOTE_READ_BURST_MAX_BYTES >= TCP_SOCKET_BUFFER_SIZE);
const _: () = assert!(RELAY_REMOTE_READ_BURST_MAX_BYTES >= RELAY_REMOTE_READ_MIN_BATCH_BYTES);
const _: () = assert!(RELAY_REMOTE_READ_ACK_DRAIN_PACKETS >= 1);
const _: () = assert!(RELAY_REMOTE_READ_PRESSURE_MIN_PACKETS >= 1);
const _: () = assert!(
    RELAY_REMOTE_READ_ADAPTIVE_ACK_DRAIN_MAX_BYTES >= RELAY_REMOTE_READ_PRESSURE_MIN_BYTES_FLOOR
);
const _: () =
    assert!(RELAY_REMOTE_READ_PRESSURE_MIN_BYTES_FLOOR <= RELAY_REMOTE_READ_MIN_BATCH_BYTES);
const _: () = assert!(DOWNLINK_CREDIT_INITIAL_BATCH_BYTES <= RELAY_REMOTE_READ_BURST_MAX_BYTES);
const _: () = assert!(DOWNLINK_CREDIT_PROGRESS_GAIN >= 1);
const MAX_ESTABLISHED_UPLINK_BATCH: usize = 64;
const _: () = assert!(MAX_ESTABLISHED_UPLINK_BATCH >= 1);
const RELAY_WRITER_COALESCE_MAX_MESSAGES: usize = MAX_ESTABLISHED_UPLINK_BATCH;
const RELAY_WRITER_COALESCE_MAX_BYTES: usize = TCP_SOCKET_BUFFER_SIZE;
const DEFAULT_DOWNLINK_BACKPRESSURE_HIGH_BYTES: usize = 512 * 1024;
const DEFAULT_DOWNLINK_BACKPRESSURE_LOW_BYTES: usize = 128 * 1024;
const _: () =
    assert!(DEFAULT_DOWNLINK_BACKPRESSURE_LOW_BYTES < DEFAULT_DOWNLINK_BACKPRESSURE_HIGH_BYTES);
/// Linux TUN commonly starts with txqueuelen 500. The runtime may not know the
/// actual qdisc length yet, so the product default sizes local TCP backpressure
/// against this conservative estimate unless the operator supplies an override.
const DEFAULT_TUN_TX_QUEUE_LEN_ESTIMATE: usize = 500;
const MAX_TUN_TX_QUEUE_LEN_ESTIMATE: usize = 1_000_000;
const TUN_EGRESS_SAFE_FLUSH_NUMERATOR: usize = 3;
const TUN_EGRESS_SAFE_FLUSH_DENOMINATOR: usize = 4;
const DEFAULT_DOWNLINK_BACKPRESSURE_LOW_RATIO: usize = 4;
const TX_QUEUE_FLUSH_RATIO_NUMERATOR: usize = 11;
const TX_QUEUE_FLUSH_RATIO_DENOMINATOR: usize = 8;
const TX_QUEUE_PAUSE_RATIO_NUMERATOR: usize = 7;
const TX_QUEUE_PAUSE_RATIO_DENOMINATOR: usize = 4;
const _: () = assert!(DEFAULT_TUN_TX_QUEUE_LEN_ESTIMATE >= 1);
const _: () = assert!(MAX_TUN_TX_QUEUE_LEN_ESTIMATE >= DEFAULT_TUN_TX_QUEUE_LEN_ESTIMATE);
const _: () = assert!(TUN_EGRESS_SAFE_FLUSH_NUMERATOR < TUN_EGRESS_SAFE_FLUSH_DENOMINATOR);
const _: () = assert!(DEFAULT_DOWNLINK_BACKPRESSURE_LOW_RATIO >= 2);
const _: () = assert!(TX_QUEUE_FLUSH_RATIO_DENOMINATOR >= 1);
const _: () = assert!(TX_QUEUE_PAUSE_RATIO_DENOMINATOR >= 1);
const DEFAULT_DOWNLINK_FLUSH_MAX_BYTES: usize = 256 * 1024;
const MIN_DOWNLINK_FLUSH_MAX_BYTES: usize = 4 * 1024;
const MAX_DOWNLINK_FLUSH_MAX_BYTES: usize = MAX_TCP_SOCKET_BUFFER_BYTES;
const _: () = assert!(MIN_DOWNLINK_FLUSH_MAX_BYTES <= DEFAULT_DOWNLINK_FLUSH_MAX_BYTES);
const _: () = assert!(DEFAULT_DOWNLINK_FLUSH_MAX_BYTES <= MAX_DOWNLINK_FLUSH_MAX_BYTES);
/// Knife14fa: drain QUIC streams with a healthy read window, but stage delivery
/// into the main loop so one ready remote burst cannot become one oversized
/// smoltcp/send_queue injection.
const RELAY_REMOTE_DISPATCH_SEGMENT_MAX_BYTES: usize = RELAY_REMOTE_READ_MIN_BATCH_BYTES;
const _: () = assert!(RELAY_REMOTE_DISPATCH_SEGMENT_MAX_BYTES < DEFAULT_DOWNLINK_FLUSH_MAX_BYTES);
const DEFAULT_DOWNLINK_EGRESS_IMMEDIATE_BYTES: usize = MAX_TCP_SOCKET_BUFFER_BYTES;
const MAX_DOWNLINK_EGRESS_IMMEDIATE_BYTES: usize = MAX_TCP_SOCKET_BUFFER_BYTES;
const _: () =
    assert!(DEFAULT_DOWNLINK_EGRESS_IMMEDIATE_BYTES <= MAX_DOWNLINK_EGRESS_IMMEDIATE_BYTES);
const BUFFERED_DOWNLINK_PER_FLOW_HIGH_MULTIPLIER: usize = 4;
const BUFFERED_DOWNLINK_PER_FLOW_HARD_MULTIPLIER: usize = 8;
const BUFFERED_DOWNLINK_GLOBAL_FLOW_SCALE_MAX: usize = 64;
const _: () = assert!(BUFFERED_DOWNLINK_PER_FLOW_HIGH_MULTIPLIER >= 2);
const _: () = assert!(
    BUFFERED_DOWNLINK_PER_FLOW_HARD_MULTIPLIER > BUFFERED_DOWNLINK_PER_FLOW_HIGH_MULTIPLIER
);
/// L2（刀9 F4）：一条 relay 双向静默多久判 idle → 退出 + shutdown。防慢/卡死上游（尤其 REALITY
/// TCP-only 手写 TLS 遇 server 不返回）长期挂住 relay task 泄漏。90s 偏宽松保稳（长轮询/SSE 不误杀）。
const RELAY_IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(90);
/// Knife14n：本地 Finish 已关闭上游写半边后，只剩远端读半边。若远端没有继续发数据或 EOF，
/// 用短窗口关闭，避免 read-only relay 卡住 active gauge 并污染下一轮 suite。
const RELAY_HALF_CLOSED_IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
/// Knife14ad：TCP 诊断打开时，relay task 的 live 计数采样周期。用于区分 reverse 数据是否真正到达
/// 客户端 TUIC stream，默认随 `MINI_VPN_TCP_DIAG` 关闭。
const RELAY_LIVE_DIAG_SECS: u64 = 5;
/// Knife14v：本地 TCP 已无上行时，先让 reverse/downlink 继续跑；远端静默超过这个窗口后才把
/// `Finish` 传播到上游写半边。避免 TUIC/exit 对早 FIN 的处理打断 reverse 流。
const LOCAL_FINISH_DEFER_SECS: u64 = 5;
/// Writer task 停止/关闭预算。注意：这不是单次 `write_all` 的进度 deadline；TUIC/QUIC
/// stream 的 pending write 可能只是正常 flow-control 背压，必须让 TCP 自然回压。
const RELAY_WRITER_STOP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
/// Knife14o/14r：relay 已关闭但仍有 downlink_pending 时，给 dirty flush 一个短窗口；
/// 14r 起窗口按“最近一次 pending 下降”刷新。无进展超过窗口才 hard reap，防永久脏槽。
const DEFERRED_CLOSE_PENDING_GRACE_SECS: u64 = 5;
/// Knife14as：TUN/qdisc drop feedback 后，本地 egress tail 可能需要一轮 TCP 重传/ACK
/// 才能下降；给它一个更长但仍有界的 close-drain 窗口，避免 dead-slot reap 抢跑。
const DEFERRED_CLOSE_EGRESS_DROP_GRACE_SECS: u64 = 15;
const DEFERRED_CLOSE_EGRESS_MAX_GRACE_SECS: u64 = 30;
/// Knife14as：relay 已结束后，只剩少量 terminal pending 时，允许这些字节在
/// hard egress guard 内进入 smoltcp，这样本地 FIN 可以排在真实尾包之后发出。
const CLOSE_DRAIN_TERMINAL_PENDING_MAX_BYTES: usize = 64 * 1024;
/// 刀11：数据面可观测性快照周期（30s，对齐 TuicUpstream UDP_STATS_LOG_SECS）。仅周期采样/打印，不进热路径。
const METRICS_SNAPSHOT_SECS: u64 = 30;
/// Knife14bf：低频 TUN egress drop feedback 控制周期。1s 足够覆盖 30s reverse probe，
/// 也避免把 Linux sysfs 读取放进每包热路径。
const TUN_EGRESS_FEEDBACK_SAMPLE_SECS: u64 = 1;
/// Knife14bu：smoltcp send_queue 高压刚 flush 到 TUN/qdisc 后，短暂保留本地 egress
/// 压力，避免下一轮 raw pressure=0 时立刻恢复远端读取。
const TUN_EGRESS_PRESSURE_HOLD_MS: u64 = 25;
/// Knife14bw：反向 TCP 窗口诊断低频采样周期。只影响 `MINI_VPN_TCP_DIAG=1` 日志密度。
const TCP_REVERSE_WINDOW_DIAG_INTERVAL_SECS: u64 = 5;
/// Knife14bi：Knife14bg 证明 opportunistic TUN RX drain 会让 reverse-first 退化；默认关闭。
const DEFAULT_TUN_RX_DRAIN_BUDGET: usize = 0;
/// Knife14bh：TUN RX drain 是诊断/公平性路径，允许 A/B 关闭或小幅放大，禁止无界热路径扫描。
const MAX_TUN_RX_DRAIN_BUDGET: usize = 64;
/// Knife14ck: default TUN RX drain stays pressure-gated, but the adaptive
/// budget targets ACK/window-sized packets rather than MTU-sized data packets.
const TUN_RX_PRESSURE_DRAIN_ESTIMATED_PACKET_BYTES: usize = 64;
const TUN_RX_PRESSURE_DRAIN_MAX_PACKETS: usize = 256;
/// Knife14co: active reverse flows get a smaller event-driven ACK/window drain
/// before the pressure edge so sender windows can progress without background polling.
const TUN_RX_ACTIVE_FLOW_DRAIN_MAX_PACKETS: usize = 32;
/// Knife14ex: bounded active-flow ACK/window service lane after downlink work.
/// The old unguarded timer drain regressed VPS throughput; this path stays
/// headroom-gated and can still be disabled with an explicit `0`.
const DEFAULT_TUN_RX_ACTIVE_FLOW_TIMER_DRAIN_MS: u64 = 10;
const MAX_TUN_RX_ACTIVE_FLOW_TIMER_DRAIN_MS: u64 = 1_000;
const TUN_RX_DRAIN_SOURCE_REMOTE_PAYLOAD_PRE: &str = "remote_payload_pre";
const TUN_RX_DRAIN_SOURCE_REMOTE_PAYLOAD: &str = "remote_payload";
const TUN_RX_DRAIN_SOURCE_REMOTE_PAYLOAD_DEFERRED: &str = "remote_payload_deferred";
const TUN_RX_DRAIN_SOURCE_REMOTE_PAYLOAD_DEFERRED_DELAYED: &str = "remote_payload_deferred_delayed";
const TUN_RX_DRAIN_SOURCE_REMOTE_PAYLOAD_DEFERRED_PRESSURE: &str =
    "remote_payload_deferred_pressure";
const TUN_RX_DRAIN_SOURCE_RELAY_GAP_HINT: &str = "relay_gap_hint";
const TUN_RX_DRAIN_SOURCE_RELAY_GAP_HINT_FOLLOWUP: &str = "relay_gap_hint_followup";
const TUN_RX_DRAIN_SOURCE_TIMER_PRESSURE: &str = "timer_pressure";
const TUN_RX_DRAIN_SOURCE_TIMER_ACTIVE_FLOW: &str = "timer_active_flow";
const DEFERRED_ACK_DRAIN_DELAY_MS: u64 = 1;
const DEFERRED_ACK_DRAIN_DELAYED_PROBE_MS: u64 = 40;
const RELAY_GAP_ACK_DRAIN_FOLLOWUP_DELAY_MS: u64 = 1;
const RELAY_GAP_ACK_DRAIN_EMPTY_FOLLOWUPS: u8 = 2;
const RELAY_GAP_ACK_DRAIN_PROGRESS_FOLLOWUPS: u8 = 1;
const RELAY_ACK_DRAIN_HINT_GAP_MS: u128 = 100;
const RELAY_REMOTE_READ_PROBE_INTERVAL_MS: u64 = 100;
const RELAY_ACK_DRAIN_HINT_INTERVAL_MS: u64 = RELAY_REMOTE_READ_PROBE_INTERVAL_MS;
const RELAY_ACK_DRAIN_HINT_MIN_DATA_BYTES: u64 = 64 * 1024;
const RELAY_REMOTE_READ_PROBE_MIN_BATCH_BYTES: usize = 1024;

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
    /// Remote payload bytes dropped because the local TCP socket was already terminal no-send.
    terminal_late_remote_payload_bytes: u64,
    /// Remote payload events dropped because the local TCP socket was already terminal no-send.
    terminal_late_remote_payload_events: u64,
    /// Highest observed `SocketCtx.downlink_pending.len()`.
    downlink_pending_high_water: usize,
    /// Number of times a non-empty pending backlog tried to progress through `flush_downlink`.
    flush_attempts: u64,
    /// Number of attempted `tcp_socket.send_slice` calls.
    send_slice_calls: u64,
    /// Bytes accepted by smoltcp `send_slice`.
    send_slice_accepted_bytes: u64,
    /// Largest single `send_slice` acceptance.
    send_slice_max_accepted_bytes: usize,
    /// Times smoltcp accepted zero bytes while the socket was considered writable.
    send_slice_zero: u64,
    /// Times pending existed but the smoltcp socket could not send yet.
    send_slice_no_capacity: u64,
    /// Times the configured per-flush budget clipped a larger pending backlog.
    send_slice_budget_limited: u64,
    /// Times the tx_queue clean egress headroom clipped a send attempt.
    send_slice_headroom_limited: u64,
    /// Bytes intentionally left pending because the tx_queue clean headroom was exhausted.
    send_slice_headroom_deferred_bytes: u64,
    /// Bytes of extra accept credit granted from observed local egress drain.
    send_slice_drain_credit_granted_bytes: u64,
    /// Bytes of drain credit included in planned `send_slice` limits.
    send_slice_drain_credit_planned_bytes: u64,
    /// Bytes of drain credit actually consumed by accepted `send_slice` bytes.
    send_slice_drain_credit_used_bytes: u64,
    /// Remaining drop debt observed while planning downlink flushes.
    send_slice_drop_credit_debt_bytes: usize,
    /// Bytes of observed egress drain spent repaying drop debt.
    send_slice_drop_credit_debt_paid_bytes: u64,
    /// Bytes of stale or would-be drain credit blocked by active drop debt.
    send_slice_drop_credit_blocked_bytes: u64,
    /// Remaining pressure debt observed while planning downlink flushes.
    send_slice_pressure_credit_debt_bytes: usize,
    /// Bytes of observed egress drain spent repaying pressure debt.
    send_slice_pressure_credit_debt_paid_bytes: u64,
    /// Bytes of stale or would-be drain credit blocked by pressure debt.
    send_slice_pressure_credit_blocked_bytes: u64,
    /// Largest derived guard reserved below the hard tx_queue pause edge.
    send_slice_hard_edge_guard_bytes: usize,
    /// Flush attempts limited specifically by the hard-edge guard.
    send_slice_hard_edge_guard_limited: u64,
    /// Bytes intentionally deferred by the hard-edge guard.
    send_slice_hard_edge_guard_deferred_bytes: u64,
    /// `send_slice` errors, usually socket close/reset while downlink still had bytes.
    send_slice_errors: u64,
    /// TCP downlink-triggered TUN flush attempts.
    tun_flush_tx_calls: u64,
    /// TCP downlink-triggered TUN flush failures.
    tun_flush_tx_failures: u64,
    /// Immediate remote-payload TUN flushes deferred to the timer egress path.
    tun_flush_deferred: u64,
    /// Number of smoltcp socket window snapshots observed while flushing downlink.
    send_window_samples: u64,
    /// Minimum observed smoltcp tx-buffer capacity.
    send_capacity_min: usize,
    /// Maximum observed smoltcp tx-buffer capacity.
    send_capacity_max: usize,
    /// Maximum observed queued bytes in smoltcp tx buffer.
    send_queue_max: usize,
    /// Maximum observed queued bytes in smoltcp rx buffer.
    recv_queue_max: usize,
    /// Times the socket state could no longer send at all.
    may_send_false: u64,
    /// Times the socket state could no longer receive from the local app.
    may_recv_false: u64,
    /// Current consecutive `can_send=false` streak across flush attempts.
    no_send_capacity_streak_current: u64,
    /// Maximum consecutive `can_send=false` streak across flush attempts.
    no_send_capacity_streak_max: u64,
    /// Largest pending backlog seen while `can_send=false`.
    no_send_capacity_pending_max: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TcpSendWindowSnapshot {
    can_send: bool,
    may_send: bool,
    may_recv: bool,
    send_capacity: usize,
    send_queue: usize,
    recv_queue: usize,
}

impl TcpSendWindowSnapshot {
    fn from_socket(socket: &TcpSocket<'_>) -> Self {
        Self {
            can_send: socket.can_send(),
            may_send: socket.may_send(),
            may_recv: socket.may_recv(),
            send_capacity: socket.send_capacity(),
            send_queue: socket.send_queue(),
            recv_queue: socket.recv_queue(),
        }
    }
}

impl TcpDownlinkDiag {
    fn note_pending(&mut self, pending_current: usize) {
        self.downlink_pending_high_water = self.downlink_pending_high_water.max(pending_current);
    }

    fn note_remote_payload(&mut self, bytes: usize, pending_current: usize) {
        self.remote_to_global_rx_bytes += bytes as u64;
        self.note_pending(pending_current);
    }

    fn note_terminal_late_remote_payload(&mut self, bytes: usize) {
        self.terminal_late_remote_payload_bytes = self
            .terminal_late_remote_payload_bytes
            .saturating_add(bytes as u64);
        self.terminal_late_remote_payload_events =
            self.terminal_late_remote_payload_events.saturating_add(1);
    }

    fn note_flush_attempt(&mut self, pending_current: usize, max_bytes_per_flush: usize) {
        self.flush_attempts += 1;
        if pending_current > bounded_downlink_flush_len(pending_current, max_bytes_per_flush) {
            self.send_slice_budget_limited += 1;
        }
        self.note_pending(pending_current);
    }

    fn note_flush_limit(
        &mut self,
        pending_current: usize,
        max_bytes_per_flush: usize,
        limit: DownlinkFlushLimit,
    ) {
        self.note_flush_attempt(pending_current, max_bytes_per_flush);
        if limit.headroom_limited {
            self.send_slice_headroom_limited = self.send_slice_headroom_limited.saturating_add(1);
            self.send_slice_headroom_deferred_bytes = self
                .send_slice_headroom_deferred_bytes
                .saturating_add(limit.headroom_deferred_bytes as u64);
        }
        self.send_slice_drain_credit_granted_bytes = self
            .send_slice_drain_credit_granted_bytes
            .saturating_add(limit.drain_credit_granted_bytes as u64);
        self.send_slice_drain_credit_planned_bytes = self
            .send_slice_drain_credit_planned_bytes
            .saturating_add(limit.drain_credit_planned_bytes as u64);
        self.send_slice_drop_credit_debt_bytes = self
            .send_slice_drop_credit_debt_bytes
            .max(limit.drop_credit_debt_bytes);
        self.send_slice_drop_credit_debt_paid_bytes = self
            .send_slice_drop_credit_debt_paid_bytes
            .saturating_add(limit.drop_credit_debt_paid_bytes as u64);
        self.send_slice_drop_credit_blocked_bytes = self
            .send_slice_drop_credit_blocked_bytes
            .saturating_add(limit.drop_credit_blocked_bytes as u64);
        self.send_slice_pressure_credit_debt_bytes = self
            .send_slice_pressure_credit_debt_bytes
            .max(limit.pressure_credit_debt_bytes);
        self.send_slice_pressure_credit_debt_paid_bytes = self
            .send_slice_pressure_credit_debt_paid_bytes
            .saturating_add(limit.pressure_credit_debt_paid_bytes as u64);
        self.send_slice_pressure_credit_blocked_bytes = self
            .send_slice_pressure_credit_blocked_bytes
            .saturating_add(limit.pressure_credit_blocked_bytes as u64);
        self.send_slice_hard_edge_guard_bytes = self
            .send_slice_hard_edge_guard_bytes
            .max(limit.hard_edge_guard_bytes);
        if limit.hard_edge_guard_deferred_bytes > 0 {
            self.send_slice_hard_edge_guard_limited =
                self.send_slice_hard_edge_guard_limited.saturating_add(1);
            self.send_slice_hard_edge_guard_deferred_bytes = self
                .send_slice_hard_edge_guard_deferred_bytes
                .saturating_add(limit.hard_edge_guard_deferred_bytes as u64);
        }
    }

    fn note_no_send_capacity(&mut self, pending_current: usize) {
        self.send_slice_no_capacity += 1;
        self.note_pending(pending_current);
    }

    fn note_send_window(&mut self, snapshot: TcpSendWindowSnapshot, pending_current: usize) {
        self.send_window_samples += 1;
        if self.send_window_samples == 1 {
            self.send_capacity_min = snapshot.send_capacity;
        } else {
            self.send_capacity_min = self.send_capacity_min.min(snapshot.send_capacity);
        }
        self.send_capacity_max = self.send_capacity_max.max(snapshot.send_capacity);
        self.send_queue_max = self.send_queue_max.max(snapshot.send_queue);
        self.recv_queue_max = self.recv_queue_max.max(snapshot.recv_queue);
        if !snapshot.may_send {
            self.may_send_false += 1;
        }
        if !snapshot.may_recv {
            self.may_recv_false += 1;
        }
        if snapshot.can_send {
            self.no_send_capacity_streak_current = 0;
        } else {
            self.no_send_capacity_streak_current += 1;
            self.no_send_capacity_streak_max = self
                .no_send_capacity_streak_max
                .max(self.no_send_capacity_streak_current);
            self.no_send_capacity_pending_max =
                self.no_send_capacity_pending_max.max(pending_current);
        }
        self.note_pending(pending_current);
    }

    fn note_send_slice_ok(&mut self, accepted: usize, pending_current: usize) {
        self.send_slice_calls += 1;
        self.send_slice_accepted_bytes += accepted as u64;
        self.send_slice_max_accepted_bytes = self.send_slice_max_accepted_bytes.max(accepted);
        if accepted == 0 {
            self.send_slice_zero += 1;
        }
        self.note_pending(pending_current);
    }

    fn note_drain_credit_used(&mut self, used: usize) {
        self.send_slice_drain_credit_used_bytes = self
            .send_slice_drain_credit_used_bytes
            .saturating_add(used as u64);
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

    fn note_tun_flush_deferred(&mut self) {
        self.tun_flush_deferred += 1;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClosePendingClass {
    NoPending,
    TerminalClosedNoSend,
    ActiveNoSend,
    ActiveSendCapable,
    InactiveSendCapable,
    InactiveNoSend,
    Unknown,
}

impl ClosePendingClass {
    fn as_str(self) -> &'static str {
        match self {
            Self::NoPending => "none",
            Self::TerminalClosedNoSend => "terminal_closed_no_send",
            Self::ActiveNoSend => "active_no_send",
            Self::ActiveSendCapable => "active_send_capable",
            Self::InactiveSendCapable => "inactive_send_capable",
            Self::InactiveNoSend => "inactive_no_send",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ClosePendingAccounting {
    class: ClosePendingClass,
    pending_bytes: usize,
    terminal_pending_reap_bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CloseEgressClass {
    NoQueuedEgress,
    TerminalClosedNoSend,
    ActiveNoSend,
    ActiveSendCapable,
    InactiveSendCapable,
    InactiveNoSend,
}

impl CloseEgressClass {
    fn as_str(self) -> &'static str {
        match self {
            Self::NoQueuedEgress => "none",
            Self::TerminalClosedNoSend => "terminal_closed_no_send",
            Self::ActiveNoSend => "active_no_send",
            Self::ActiveSendCapable => "active_send_capable",
            Self::InactiveSendCapable => "inactive_send_capable",
            Self::InactiveNoSend => "inactive_no_send",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CloseEgressAccounting {
    class: CloseEgressClass,
    bytes: usize,
    drain_candidate: bool,
}

fn classify_close_pending(pending: usize, snapshot: SocketCloseSnapshot) -> ClosePendingClass {
    if pending == 0 {
        return ClosePendingClass::NoPending;
    }
    if snapshot.tcp_state == TcpState::Closed && !snapshot.active && !snapshot.can_send {
        return ClosePendingClass::TerminalClosedNoSend;
    }
    if snapshot.active && !snapshot.can_send {
        return ClosePendingClass::ActiveNoSend;
    }
    if snapshot.active && snapshot.can_send {
        return ClosePendingClass::ActiveSendCapable;
    }
    if snapshot.can_send {
        return ClosePendingClass::InactiveSendCapable;
    }
    ClosePendingClass::InactiveNoSend
}

fn classify_close_egress(snapshot: SocketCloseSnapshot) -> CloseEgressClass {
    if snapshot.send_queue == 0 {
        return CloseEgressClass::NoQueuedEgress;
    }
    if snapshot.tcp_state == TcpState::Closed && !snapshot.active && !snapshot.can_send {
        return CloseEgressClass::TerminalClosedNoSend;
    }
    if snapshot.active && !snapshot.can_send {
        return CloseEgressClass::ActiveNoSend;
    }
    if snapshot.active && snapshot.can_send {
        return CloseEgressClass::ActiveSendCapable;
    }
    if snapshot.can_send {
        return CloseEgressClass::InactiveSendCapable;
    }
    CloseEgressClass::InactiveNoSend
}

fn close_pending_accounting(
    ctx: &SocketCtx,
    snapshot: SocketCloseSnapshot,
) -> ClosePendingAccounting {
    let pending_bytes = ctx.downlink_pending.len();
    let class = classify_close_pending(pending_bytes, snapshot);
    let terminal_pending_reap_bytes = if class == ClosePendingClass::TerminalClosedNoSend {
        pending_bytes
    } else {
        0
    };
    ClosePendingAccounting {
        class,
        pending_bytes,
        terminal_pending_reap_bytes,
    }
}

fn close_egress_accounting(snapshot: SocketCloseSnapshot) -> CloseEgressAccounting {
    let class = classify_close_egress(snapshot);
    CloseEgressAccounting {
        class,
        bytes: snapshot.send_queue,
        drain_candidate: class == CloseEgressClass::ActiveSendCapable,
    }
}

fn tcp_lifecycle_changed(prev: SocketCloseSnapshot, current: SocketCloseSnapshot) -> bool {
    prev.tcp_state != current.tcp_state
        || prev.active != current.active
        || prev.can_send != current.can_send
        || prev.can_recv != current.can_recv
        || prev.may_send != current.may_send
        || prev.may_recv != current.may_recv
}

fn option_secs(value: Option<u64>) -> String {
    value
        .map(|secs| secs.to_string())
        .unwrap_or_else(|| "none".to_string())
}

fn observe_tcp_lifecycle(
    handle: SocketHandle,
    ctx: &mut SocketCtx,
    snapshot: SocketCloseSnapshot,
    now_secs: u64,
    source: &'static str,
) -> Option<String> {
    let previous = ctx.last_tcp_lifecycle;
    ctx.last_tcp_lifecycle = Some(TcpLifecycleObservation {
        snapshot,
        source,
        observed_secs: now_secs,
    });

    let previous = previous?;
    if !tcp_lifecycle_changed(previous.snapshot, snapshot) {
        return None;
    }

    let terminal_candidate = !ctx.downlink_pending.is_empty()
        && snapshot.tcp_state == TcpState::Closed
        && !snapshot.active
        && !snapshot.can_send;

    Some(format!(
        "🔎 tcp-lifecycle-transition handle={handle:?} source={source} prev_source={} prev_observed_secs={} observed_secs={} prev_state={:?} state={:?} prev_active={} active={} prev_can_send={} can_send={} prev_can_recv={} can_recv={} prev_may_send={} may_send={} prev_may_recv={} may_recv={} prev_send_queue={} send_queue={} prev_recv_queue={} recv_queue={} ctx_state={:?} uplink_tx={} local_fin_sent={} local_fin_pending_since={} local_fin_last_remote_progress={} pending={} pending_high={} remote_to_global_rx_bytes={} send_slice_accepted={} flush_attempts={} terminal_candidate={}",
        previous.source,
        previous.observed_secs,
        now_secs,
        previous.snapshot.tcp_state,
        snapshot.tcp_state,
        previous.snapshot.active,
        snapshot.active,
        previous.snapshot.can_send,
        snapshot.can_send,
        previous.snapshot.can_recv,
        snapshot.can_recv,
        previous.snapshot.may_send,
        snapshot.may_send,
        previous.snapshot.may_recv,
        snapshot.may_recv,
        previous.snapshot.send_queue,
        snapshot.send_queue,
        previous.snapshot.recv_queue,
        snapshot.recv_queue,
        ctx.state,
        ctx.uplink_tx.is_some(),
        ctx.local_fin_sent,
        option_secs(ctx.local_fin_pending_since_secs),
        option_secs(ctx.local_fin_last_remote_progress_secs),
        ctx.downlink_pending.len(),
        ctx.downlink_diag.downlink_pending_high_water,
        ctx.downlink_diag.remote_to_global_rx_bytes,
        ctx.downlink_diag.send_slice_accepted_bytes,
        ctx.downlink_diag.flush_attempts,
        terminal_candidate
    ))
}

fn log_tcp_lifecycle_observation(
    handle: SocketHandle,
    ctx: &mut SocketCtx,
    snapshot: SocketCloseSnapshot,
    now_secs: u64,
    source: &'static str,
) {
    if let Some(line) = observe_tcp_lifecycle(handle, ctx, snapshot, now_secs, source) {
        tcp_diag_log!("{line}");
    }
}

fn should_log_tcp_reverse_window(
    ctx: &mut SocketCtx,
    snapshot: SocketCloseSnapshot,
    now_secs: u64,
) -> bool {
    let urgent = !snapshot.active || !snapshot.can_send || !snapshot.may_recv;
    let due = ctx
        .reverse_window_last_log_secs
        .map(|last| {
            urgent || now_secs.saturating_sub(last) >= TCP_REVERSE_WINDOW_DIAG_INTERVAL_SECS
        })
        .unwrap_or(true);
    if due {
        ctx.reverse_window_last_log_secs = Some(now_secs);
    }
    due
}

#[derive(Debug, Clone)]
struct RelayTaskDiag {
    /// Relay task creation time, used to measure first remote byte latency.
    started_at: std::time::Instant,
    /// Delay from relay start to the first successful remote read.
    first_remote_read_millis: Option<u128>,
    /// Timestamp of the first successful remote read.
    first_remote_read_at: Option<std::time::Instant>,
    /// Timestamp of the latest successful remote read.
    last_remote_read_at: Option<std::time::Instant>,
    /// Largest gap between successful remote reads.
    max_remote_read_gap_millis: u128,
    /// Bytes written from local smoltcp side into the remote stream.
    uplink_bytes: u64,
    /// Bytes read from the remote stream and accepted into the global_rx path.
    remote_to_global_rx_bytes: u64,
    /// Number of remote-to-main data batches sent after bounded ready-read coalescing.
    remote_batches: u64,
    /// Largest remote-to-main batch in bytes.
    remote_batch_bytes_max: usize,
    /// Largest number of remote read chunks coalesced into one remote-to-main batch.
    remote_batch_chunks_max: usize,
    /// Lowest ready-read byte budget observed from relay -> main-loop channel pressure.
    remote_batch_limit_bytes_min: usize,
    /// Count of remote ready-read batches whose byte budget was reduced by downstream pressure.
    remote_batch_limited: u64,
    /// Number of main-loop read-credit updates observed by the relay read half.
    read_credit_updates: u64,
    /// Number of read-credit updates that paused remote reads.
    read_credit_pause_updates: u64,
    /// Lowest egress-derived read-credit batch budget observed.
    read_credit_limit_bytes_min: usize,
    /// Bytes read from the remote stream after local `Finish` was observed.
    remote_after_local_finish_bytes: u64,
    /// Number of successful remote stream reads.
    remote_reads: u64,
    /// Number of periodic wakeups used only to keep payload-shaped remote reads polled.
    remote_read_probe_ticks: u64,
    /// Number of read-service retry ticks while an active remote stream was awaiting readiness.
    remote_read_service_ticks: u64,
    /// Smallest awaited remote read buffer length while read service was active.
    remote_read_service_len_min: usize,
    /// Largest awaited remote read buffer length while read service was active.
    remote_read_service_len_max: usize,
    /// Number of successful remote stream reads after local `Finish`.
    remote_after_local_finish_reads: u64,
    /// Number of local `Finish` signals observed by the relay writer half.
    local_finish_events: u64,
    /// Milliseconds from first remote read to the first local `Finish`.
    first_local_finish_after_first_remote_read_millis: Option<u128>,
    /// Highest remote read gap observed before local `Finish`.
    max_remote_read_gap_before_local_finish_millis: u128,
    /// Highest remote read gap observed after local `Finish`.
    max_remote_read_gap_after_local_finish_millis: u128,
    /// Number of successful local payload writes to the remote stream.
    local_writes: u64,
    /// Highest observed wait before global_rx accepted a remote payload.
    global_rx_wait_max_micros: u128,
    /// Count of remote payload sends whose wait crossed the diagnostic threshold.
    global_rx_pressure_events: u64,
    /// Highest observed used slots in the relay -> main-loop channel.
    global_rx_queue_used_max: usize,
    /// Bounded channel capacity used by the relay -> main-loop channel.
    global_rx_queue_capacity: usize,
    /// Highest observed wait for a local-to-remote writer batch.
    local_write_wait_max_micros: u128,
    /// Count of local writer batches whose wait crossed the diagnostic threshold.
    local_write_pressure_events: u64,
    /// Number of relay read-gap ACK drain hints that became due.
    ack_drain_hint_due: u64,
    /// Number of ACK drain hints accepted by the relay -> main-loop channel.
    ack_drain_hint_sent: u64,
    /// Number of ACK drain hints dropped because the relay -> main-loop channel could not accept them.
    ack_drain_hint_dropped: u64,
    local_finish_seen: bool,
}

impl Default for RelayTaskDiag {
    fn default() -> Self {
        Self::new(std::time::Instant::now())
    }
}

impl RelayTaskDiag {
    fn new(started_at: std::time::Instant) -> Self {
        Self {
            started_at,
            first_remote_read_millis: None,
            first_remote_read_at: None,
            last_remote_read_at: None,
            max_remote_read_gap_millis: 0,
            uplink_bytes: 0,
            remote_to_global_rx_bytes: 0,
            remote_batches: 0,
            remote_batch_bytes_max: 0,
            remote_batch_chunks_max: 0,
            remote_batch_limit_bytes_min: 0,
            remote_batch_limited: 0,
            read_credit_updates: 0,
            read_credit_pause_updates: 0,
            read_credit_limit_bytes_min: 0,
            remote_after_local_finish_bytes: 0,
            remote_reads: 0,
            remote_read_probe_ticks: 0,
            remote_read_service_ticks: 0,
            remote_read_service_len_min: 0,
            remote_read_service_len_max: 0,
            remote_after_local_finish_reads: 0,
            local_finish_events: 0,
            first_local_finish_after_first_remote_read_millis: None,
            max_remote_read_gap_before_local_finish_millis: 0,
            max_remote_read_gap_after_local_finish_millis: 0,
            local_writes: 0,
            global_rx_wait_max_micros: 0,
            global_rx_pressure_events: 0,
            global_rx_queue_used_max: 0,
            global_rx_queue_capacity: 0,
            local_write_wait_max_micros: 0,
            local_write_pressure_events: 0,
            ack_drain_hint_due: 0,
            ack_drain_hint_sent: 0,
            ack_drain_hint_dropped: 0,
            local_finish_seen: false,
        }
    }

    fn note_uplink_write(&mut self, bytes: usize) {
        self.local_writes += 1;
        self.uplink_bytes += bytes as u64;
    }

    fn note_local_finish(&mut self) {
        self.note_local_finish_at(std::time::Instant::now());
    }

    fn note_local_finish_at(&mut self, now: std::time::Instant) {
        self.local_finish_events = self.local_finish_events.saturating_add(1);
        self.local_finish_seen = true;
        if self
            .first_local_finish_after_first_remote_read_millis
            .is_none()
            && let Some(first_remote_read_at) = self.first_remote_read_at
        {
            self.first_local_finish_after_first_remote_read_millis = Some(
                now.saturating_duration_since(first_remote_read_at)
                    .as_millis(),
            );
        }
    }

    fn note_remote_read(&mut self, bytes: usize) {
        self.note_remote_read_at(bytes, std::time::Instant::now());
    }

    fn note_remote_read_at(&mut self, bytes: usize, now: std::time::Instant) {
        self.remote_reads += 1;
        self.remote_to_global_rx_bytes += bytes as u64;
        if self.first_remote_read_millis.is_none() {
            self.first_remote_read_millis =
                Some(now.saturating_duration_since(self.started_at).as_millis());
            self.first_remote_read_at = Some(now);
        }
        if let Some(last) = self.last_remote_read_at {
            let gap = now.saturating_duration_since(last).as_millis();
            self.max_remote_read_gap_millis = self.max_remote_read_gap_millis.max(gap);
            if self.local_finish_seen {
                self.max_remote_read_gap_after_local_finish_millis =
                    self.max_remote_read_gap_after_local_finish_millis.max(gap);
            } else {
                self.max_remote_read_gap_before_local_finish_millis =
                    self.max_remote_read_gap_before_local_finish_millis.max(gap);
            }
        }
        self.last_remote_read_at = Some(now);
        if self.local_finish_seen {
            self.remote_after_local_finish_reads += 1;
            self.remote_after_local_finish_bytes += bytes as u64;
        }
    }

    fn note_remote_batch(&mut self, chunks: usize, bytes: usize) {
        if chunks == 0 || bytes == 0 {
            return;
        }
        self.remote_batches = self.remote_batches.saturating_add(1);
        self.remote_batch_bytes_max = self.remote_batch_bytes_max.max(bytes);
        self.remote_batch_chunks_max = self.remote_batch_chunks_max.max(chunks);
    }

    fn note_remote_batch_limit(&mut self, byte_limit: usize) {
        if byte_limit == 0 {
            return;
        }
        if self.remote_batch_limit_bytes_min == 0 {
            self.remote_batch_limit_bytes_min = byte_limit;
        } else {
            self.remote_batch_limit_bytes_min = self.remote_batch_limit_bytes_min.min(byte_limit);
        }
        if byte_limit < RELAY_REMOTE_READ_BURST_MAX_BYTES {
            self.remote_batch_limited = self.remote_batch_limited.saturating_add(1);
        }
    }

    fn note_read_credit(&mut self, credit: RelayReadCredit) {
        self.read_credit_updates = self.read_credit_updates.saturating_add(1);
        if credit.paused {
            self.read_credit_pause_updates = self.read_credit_pause_updates.saturating_add(1);
            return;
        }
        if credit.max_batch_bytes == 0 {
            return;
        }
        if self.read_credit_limit_bytes_min == 0 {
            self.read_credit_limit_bytes_min = credit.max_batch_bytes;
        } else {
            self.read_credit_limit_bytes_min =
                self.read_credit_limit_bytes_min.min(credit.max_batch_bytes);
        }
    }

    fn first_remote_read_millis(&self) -> u128 {
        self.first_remote_read_millis.unwrap_or(0)
    }

    fn first_local_finish_after_first_remote_read_millis(&self) -> u128 {
        self.first_local_finish_after_first_remote_read_millis
            .unwrap_or(0)
    }

    fn current_remote_read_gap_millis_at(&self, now: std::time::Instant) -> u128 {
        let since = self.last_remote_read_at.unwrap_or(self.started_at);
        now.saturating_duration_since(since).as_millis()
    }

    fn note_global_rx_wait(
        &mut self,
        elapsed: std::time::Duration,
        pressure_threshold: std::time::Duration,
    ) {
        self.global_rx_wait_max_micros = self.global_rx_wait_max_micros.max(elapsed.as_micros());
        if elapsed >= pressure_threshold {
            self.global_rx_pressure_events += 1;
        }
    }

    fn note_global_rx_queue(&mut self, used: usize, max_capacity: usize) {
        self.global_rx_queue_used_max = self.global_rx_queue_used_max.max(used);
        self.global_rx_queue_capacity = self.global_rx_queue_capacity.max(max_capacity);
    }

    fn note_local_write_wait(
        &mut self,
        elapsed: std::time::Duration,
        pressure_threshold: std::time::Duration,
    ) {
        self.local_write_wait_max_micros =
            self.local_write_wait_max_micros.max(elapsed.as_micros());
        if elapsed >= pressure_threshold {
            self.local_write_pressure_events += 1;
        }
    }

    fn note_ack_drain_hint_due(&mut self) {
        self.ack_drain_hint_due = self.ack_drain_hint_due.saturating_add(1);
    }

    fn note_ack_drain_hint_sent(&mut self) {
        self.ack_drain_hint_sent = self.ack_drain_hint_sent.saturating_add(1);
    }

    fn note_ack_drain_hint_dropped(&mut self) {
        self.ack_drain_hint_dropped = self.ack_drain_hint_dropped.saturating_add(1);
    }

    fn note_remote_read_probe_tick(&mut self) {
        self.remote_read_probe_ticks = self.remote_read_probe_ticks.saturating_add(1);
    }

    fn note_remote_read_service_tick(&mut self, read_len: usize) {
        if read_len == 0 {
            return;
        }
        self.remote_read_service_ticks = self.remote_read_service_ticks.saturating_add(1);
        if self.remote_read_service_len_min == 0 {
            self.remote_read_service_len_min = read_len;
        } else {
            self.remote_read_service_len_min = self.remote_read_service_len_min.min(read_len);
        }
        self.remote_read_service_len_max = self.remote_read_service_len_max.max(read_len);
    }

    fn copy_reader_fields_from(&mut self, reader: &RelayTaskDiag) {
        self.first_remote_read_millis = reader.first_remote_read_millis;
        self.first_remote_read_at = reader.first_remote_read_at;
        self.last_remote_read_at = reader.last_remote_read_at;
        self.max_remote_read_gap_millis = reader.max_remote_read_gap_millis;
        self.remote_to_global_rx_bytes = reader.remote_to_global_rx_bytes;
        self.remote_batches = reader.remote_batches;
        self.remote_batch_bytes_max = reader.remote_batch_bytes_max;
        self.remote_batch_chunks_max = reader.remote_batch_chunks_max;
        self.remote_batch_limit_bytes_min = reader.remote_batch_limit_bytes_min;
        self.remote_batch_limited = reader.remote_batch_limited;
        self.read_credit_updates = reader.read_credit_updates;
        self.read_credit_pause_updates = reader.read_credit_pause_updates;
        self.read_credit_limit_bytes_min = reader.read_credit_limit_bytes_min;
        self.remote_after_local_finish_bytes = reader.remote_after_local_finish_bytes;
        self.remote_reads = reader.remote_reads;
        self.remote_read_service_ticks = reader.remote_read_service_ticks;
        self.remote_read_service_len_min = reader.remote_read_service_len_min;
        self.remote_read_service_len_max = reader.remote_read_service_len_max;
        self.remote_after_local_finish_reads = reader.remote_after_local_finish_reads;
        self.first_local_finish_after_first_remote_read_millis =
            reader.first_local_finish_after_first_remote_read_millis;
        self.max_remote_read_gap_before_local_finish_millis =
            reader.max_remote_read_gap_before_local_finish_millis;
        self.max_remote_read_gap_after_local_finish_millis =
            reader.max_remote_read_gap_after_local_finish_millis;
        self.global_rx_wait_max_micros = reader.global_rx_wait_max_micros;
        self.global_rx_pressure_events = reader.global_rx_pressure_events;
        self.global_rx_queue_used_max = reader.global_rx_queue_used_max;
        self.global_rx_queue_capacity = reader.global_rx_queue_capacity;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RelayAckDrainHintDue {
    gap_ms: u128,
    remote_to_global_rx_bytes: u64,
}

fn relay_ack_drain_hint_due(
    diag: &RelayTaskDiag,
    now: std::time::Instant,
    last_hint_at: Option<std::time::Instant>,
) -> Option<RelayAckDrainHintDue> {
    if diag.remote_reads == 0 || !relay_ack_drain_hint_has_serviceable_payload(diag) {
        return None;
    }
    let gap_ms = diag.current_remote_read_gap_millis_at(now);
    if gap_ms < RELAY_ACK_DRAIN_HINT_GAP_MS {
        return None;
    }
    if let Some(last) = last_hint_at
        && now.saturating_duration_since(last)
            < std::time::Duration::from_millis(RELAY_ACK_DRAIN_HINT_INTERVAL_MS)
    {
        return None;
    }

    Some(RelayAckDrainHintDue {
        gap_ms,
        remote_to_global_rx_bytes: diag.remote_to_global_rx_bytes,
    })
}

fn relay_ack_drain_hint_has_serviceable_payload(diag: &RelayTaskDiag) -> bool {
    diag.remote_to_global_rx_bytes >= RELAY_ACK_DRAIN_HINT_MIN_DATA_BYTES
        || (diag.remote_to_global_rx_bytes > 0
            && diag.remote_read_service_ticks > 0
            && diag.remote_batch_bytes_max >= RELAY_REMOTE_READ_PROBE_MIN_BATCH_BYTES)
}

fn should_poll_relay_ack_drain_hint(diag: &RelayTaskDiag, _writer_done: bool) -> bool {
    diag.remote_reads > 0 && relay_ack_drain_hint_has_serviceable_payload(diag)
}

fn should_poll_relay_remote_read_probe(diag: &RelayTaskDiag, read_credit: RelayReadCredit) -> bool {
    !read_credit.paused
        && read_credit.max_batch_bytes > 0
        && diag.remote_reads > 0
        && diag.remote_to_global_rx_bytes > 0
        && diag.remote_batch_bytes_max >= RELAY_REMOTE_READ_PROBE_MIN_BATCH_BYTES
}

fn note_global_rx_channel_occupancy<T>(diag: &mut RelayTaskDiag, tx: &mpsc::Sender<T>) {
    let max_capacity = tx.max_capacity();
    let used = max_capacity.saturating_sub(tx.capacity());
    diag.note_global_rx_queue(used, max_capacity);
}

fn relay_remote_ready_batch_byte_limit_for_queue(used: usize, max_capacity: usize) -> usize {
    if max_capacity == 0 {
        return RELAY_REMOTE_READ_BURST_MAX_BYTES;
    }

    let used = used.min(max_capacity);
    let low_water = max_capacity / 4;
    let high_water = max_capacity.saturating_mul(3) / 4;
    if used <= low_water {
        return RELAY_REMOTE_READ_BURST_MAX_BYTES;
    }
    if used >= high_water {
        return RELAY_REMOTE_READ_MIN_BATCH_BYTES;
    }
    let span = high_water.saturating_sub(low_water);
    if span == 0 {
        return RELAY_REMOTE_READ_MIN_BATCH_BYTES;
    }

    let remaining = high_water.saturating_sub(used);
    let elastic = RELAY_REMOTE_READ_BURST_MAX_BYTES - RELAY_REMOTE_READ_MIN_BATCH_BYTES;
    RELAY_REMOTE_READ_MIN_BATCH_BYTES + (elastic.saturating_mul(remaining) / span)
}

fn relay_remote_ready_batch_byte_limit<T>(tx: &mpsc::Sender<T>) -> usize {
    let max_capacity = tx.max_capacity();
    let used = max_capacity.saturating_sub(tx.capacity());
    relay_remote_ready_batch_byte_limit_for_queue(used, max_capacity)
}

fn relay_remote_read_pressure_floor_bytes(tun_mtu: usize) -> usize {
    tun_mtu
        .max(MIN_TUN_MTU)
        .saturating_mul(RELAY_REMOTE_READ_PRESSURE_MIN_PACKETS)
        .clamp(
            RELAY_REMOTE_READ_PRESSURE_MIN_BYTES_FLOOR,
            RELAY_REMOTE_READ_MIN_BATCH_BYTES,
        )
}

fn relay_remote_read_ack_drain_floor_bytes(tun_mtu: usize) -> usize {
    tun_mtu
        .max(MIN_TUN_MTU)
        .saturating_mul(RELAY_REMOTE_READ_ACK_DRAIN_PACKETS)
        .min(relay_remote_read_pressure_floor_bytes(tun_mtu))
        .max(1)
}

fn relay_remote_read_adaptive_ack_drain_max_bytes(tun_mtu: usize) -> usize {
    RELAY_REMOTE_READ_ADAPTIVE_ACK_DRAIN_MAX_BYTES
        .max(relay_remote_read_pressure_floor_bytes(tun_mtu))
        .min(RELAY_REMOTE_READ_BURST_MAX_BYTES)
        .max(relay_remote_read_ack_drain_floor_bytes(tun_mtu))
}

fn clamp_relay_remote_read_ack_drain_bytes(bytes: usize, tun_mtu: usize) -> usize {
    bytes.clamp(
        relay_remote_read_ack_drain_floor_bytes(tun_mtu),
        relay_remote_read_adaptive_ack_drain_max_bytes(tun_mtu),
    )
}

fn format_relay_live_diag(
    handle: SocketHandle,
    epoch: u64,
    writer_done: bool,
    read_only_after_local_finish: bool,
    diag: &RelayTaskDiag,
) -> String {
    format_relay_live_diag_at(
        handle,
        epoch,
        writer_done,
        read_only_after_local_finish,
        diag,
        std::time::Instant::now(),
    )
}

fn format_relay_live_diag_at(
    handle: SocketHandle,
    epoch: u64,
    writer_done: bool,
    read_only_after_local_finish: bool,
    diag: &RelayTaskDiag,
    now: std::time::Instant,
) -> String {
    format!(
        "🔎 tcp-relay-live handle={handle:?} epoch={epoch} writer_done={writer_done} \
         read_only_after_local_finish={read_only_after_local_finish} \
         uplink_bytes={} uplink_writes={} remote_to_global_rx_bytes={} remote_reads={} remote_read_probe_ticks={} remote_read_service_ticks={} remote_read_service_len_min={} remote_read_service_len_max={} \
         remote_batches={} remote_batch_bytes_max={} remote_batch_chunks_max={} remote_batch_limit_bytes_min={} remote_batch_limited={} read_credit_updates={} read_credit_pause_updates={} read_credit_limit_bytes_min={} \
         local_finish_events={} first_local_finish_after_first_remote_read_ms={} \
         remote_after_local_finish_bytes={} remote_after_local_finish_reads={} \
         first_remote_read_ms={} max_remote_read_gap_ms={} max_remote_read_gap_before_local_finish_ms={} max_remote_read_gap_after_local_finish_ms={} current_remote_read_gap_ms={} \
         ack_drain_hint_due={} ack_drain_hint_sent={} ack_drain_hint_dropped={} \
         global_rx_wait_max_us={} global_rx_pressure_events={} \
         global_rx_queue_used_max={} global_rx_queue_capacity={} \
         local_write_wait_max_us={} local_write_pressure_events={}",
        diag.uplink_bytes,
        diag.local_writes,
        diag.remote_to_global_rx_bytes,
        diag.remote_reads,
        diag.remote_read_probe_ticks,
        diag.remote_read_service_ticks,
        diag.remote_read_service_len_min,
        diag.remote_read_service_len_max,
        diag.remote_batches,
        diag.remote_batch_bytes_max,
        diag.remote_batch_chunks_max,
        diag.remote_batch_limit_bytes_min,
        diag.remote_batch_limited,
        diag.read_credit_updates,
        diag.read_credit_pause_updates,
        diag.read_credit_limit_bytes_min,
        diag.local_finish_events,
        diag.first_local_finish_after_first_remote_read_millis(),
        diag.remote_after_local_finish_bytes,
        diag.remote_after_local_finish_reads,
        diag.first_remote_read_millis(),
        diag.max_remote_read_gap_millis,
        diag.max_remote_read_gap_before_local_finish_millis,
        diag.max_remote_read_gap_after_local_finish_millis,
        diag.current_remote_read_gap_millis_at(now),
        diag.ack_drain_hint_due,
        diag.ack_drain_hint_sent,
        diag.ack_drain_hint_dropped,
        diag.global_rx_wait_max_micros,
        diag.global_rx_pressure_events,
        diag.global_rx_queue_used_max,
        diag.global_rx_queue_capacity,
        diag.local_write_wait_max_micros,
        diag.local_write_pressure_events
    )
}

fn format_relay_close_diag(
    handle: SocketHandle,
    close_direction: &'static str,
    close_reason: &'static str,
    diag: &RelayTaskDiag,
) -> String {
    format!(
        "🔎 tcp-relay-close handle={:?} direction={} reason={} uplink_bytes={} uplink_writes={} remote_to_global_rx_bytes={} remote_reads={} remote_read_probe_ticks={} remote_read_service_ticks={} remote_read_service_len_min={} remote_read_service_len_max={} remote_batches={} remote_batch_bytes_max={} remote_batch_chunks_max={} remote_batch_limit_bytes_min={} remote_batch_limited={} read_credit_updates={} read_credit_pause_updates={} read_credit_limit_bytes_min={} local_finish_events={} first_local_finish_after_first_remote_read_ms={} remote_after_local_finish_bytes={} remote_after_local_finish_reads={} first_remote_read_ms={} max_remote_read_gap_ms={} max_remote_read_gap_before_local_finish_ms={} max_remote_read_gap_after_local_finish_ms={} current_remote_read_gap_ms={} ack_drain_hint_due={} ack_drain_hint_sent={} ack_drain_hint_dropped={} global_rx_wait_max_us={} global_rx_pressure_events={} global_rx_queue_used_max={} global_rx_queue_capacity={} local_write_wait_max_us={} local_write_pressure_events={}",
        handle,
        close_direction,
        close_reason,
        diag.uplink_bytes,
        diag.local_writes,
        diag.remote_to_global_rx_bytes,
        diag.remote_reads,
        diag.remote_read_probe_ticks,
        diag.remote_read_service_ticks,
        diag.remote_read_service_len_min,
        diag.remote_read_service_len_max,
        diag.remote_batches,
        diag.remote_batch_bytes_max,
        diag.remote_batch_chunks_max,
        diag.remote_batch_limit_bytes_min,
        diag.remote_batch_limited,
        diag.read_credit_updates,
        diag.read_credit_pause_updates,
        diag.read_credit_limit_bytes_min,
        diag.local_finish_events,
        diag.first_local_finish_after_first_remote_read_millis(),
        diag.remote_after_local_finish_bytes,
        diag.remote_after_local_finish_reads,
        diag.first_remote_read_millis(),
        diag.max_remote_read_gap_millis,
        diag.max_remote_read_gap_before_local_finish_millis,
        diag.max_remote_read_gap_after_local_finish_millis,
        diag.current_remote_read_gap_millis_at(std::time::Instant::now()),
        diag.ack_drain_hint_due,
        diag.ack_drain_hint_sent,
        diag.ack_drain_hint_dropped,
        diag.global_rx_wait_max_micros,
        diag.global_rx_pressure_events,
        diag.global_rx_queue_used_max,
        diag.global_rx_queue_capacity,
        diag.local_write_wait_max_micros,
        diag.local_write_pressure_events
    )
}

#[derive(Debug, Clone, Copy)]
struct TcpReverseWindowDiag {
    ctx_state: SocketState,
    payload_bytes: usize,
    accepted_bytes: usize,
    pending: usize,
    remote_to_global_rx_bytes: u64,
    local_fin_sent: bool,
    snapshot: SocketCloseSnapshot,
}

fn format_tcp_reverse_window_diag(handle: SocketHandle, diag: TcpReverseWindowDiag) -> String {
    format!(
        "🔎 tcp-reverse-window handle={:?} ctx_state={:?} payload_bytes={} accepted_bytes={} pending={} remote_to_global_rx_bytes={} local_fin_sent={} tcp_state={:?} active={} can_send={} can_recv={} may_send={} may_recv={} send_capacity={} send_queue={} recv_queue={}",
        handle,
        diag.ctx_state,
        diag.payload_bytes,
        diag.accepted_bytes,
        diag.pending,
        diag.remote_to_global_rx_bytes,
        diag.local_fin_sent,
        diag.snapshot.tcp_state,
        diag.snapshot.active,
        diag.snapshot.can_send,
        diag.snapshot.can_recv,
        diag.snapshot.may_send,
        diag.snapshot.may_recv,
        diag.snapshot.send_capacity,
        diag.snapshot.send_queue,
        diag.snapshot.recv_queue
    )
}

#[derive(Debug)]
enum RelayCommand {
    Data(Vec<u8>),
    Finish,
}

#[derive(Debug)]
enum RelayWriterSignal {
    Progress {
        bytes: usize,
        write_wait: std::time::Duration,
    },
    WriteHalfClosed {
        reason: &'static str,
    },
    Closed {
        direction: &'static str,
        reason: &'static str,
    },
}

struct CoalescedRelayWrite {
    payload: Vec<u8>,
    finish_after: bool,
    deferred: Option<RelayCommand>,
}

fn coalesce_relay_payload(
    mut payload: Vec<u8>,
    rx: &mut mpsc::Receiver<RelayCommand>,
) -> CoalescedRelayWrite {
    let mut messages = 1usize;
    let mut finish_after = false;
    while messages < RELAY_WRITER_COALESCE_MAX_MESSAGES
        && payload.len() < RELAY_WRITER_COALESCE_MAX_BYTES
    {
        match rx.try_recv() {
            Ok(RelayCommand::Data(mut next)) => {
                if payload.len().saturating_add(next.len()) > RELAY_WRITER_COALESCE_MAX_BYTES {
                    return CoalescedRelayWrite {
                        payload,
                        finish_after,
                        deferred: Some(RelayCommand::Data(next)),
                    };
                }
                payload.append(&mut next);
                messages += 1;
            }
            Ok(RelayCommand::Finish) => {
                finish_after = true;
                break;
            }
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
            | Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => break,
        }
    }
    CoalescedRelayWrite {
        payload,
        finish_after,
        deferred: None,
    }
}

/// Terminal reason reported by one background TCP relay task.
#[derive(Debug, Clone, Copy)]
struct RelayClose {
    epoch: u64,
    direction: &'static str,
    reason: &'static str,
}

#[derive(Debug, Clone, Copy)]
struct SocketCloseSnapshot {
    tcp_state: TcpState,
    active: bool,
    can_send: bool,
    can_recv: bool,
    may_send: bool,
    may_recv: bool,
    send_capacity: usize,
    send_queue: usize,
    recv_queue: usize,
}

impl SocketCloseSnapshot {
    fn from_socket(socket: &TcpSocket<'_>) -> Self {
        Self {
            tcp_state: socket.state(),
            active: socket.is_active(),
            can_send: socket.can_send(),
            can_recv: socket.can_recv(),
            may_send: socket.may_send(),
            may_recv: socket.may_recv(),
            send_capacity: socket.send_capacity(),
            send_queue: socket.send_queue(),
            recv_queue: socket.recv_queue(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct TcpLifecycleObservation {
    snapshot: SocketCloseSnapshot,
    source: &'static str,
    observed_secs: u64,
}

/// Event sent from one background TCP relay task back into the single owner main loop.
/// Data events preserve the existing downlink path; Closed makes every relay terminal reason visible to
/// the socket state machine instead of relying on smoltcp to notice the local side later.
#[derive(Debug)]
enum RelayEvent {
    Data {
        epoch: u64,
        bytes: Vec<u8>,
    },
    AckDrainHint {
        epoch: u64,
        gap_ms: u128,
        remote_to_global_rx_bytes: u64,
    },
    Closed(RelayClose),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RelayReadCredit {
    paused: bool,
    max_batch_bytes: usize,
}

impl Default for RelayReadCredit {
    fn default() -> Self {
        Self {
            paused: false,
            max_batch_bytes: RELAY_REMOTE_READ_BURST_MAX_BYTES,
        }
    }
}

impl RelayReadCredit {
    fn initial() -> Self {
        Self::default()
    }
}

#[allow(clippy::too_many_arguments)]
fn relay_read_credit_for_local_pressure_with_ack_drain_floor(
    local_pressure_bytes: usize,
    pending_bytes: usize,
    egress_recovery_active: bool,
    hard_pause_active: bool,
    cfg: DownlinkBackpressureConfig,
    tun_mtu: usize,
    pressure_ack_drain_bytes: usize,
    _downlink_flush_max_bytes: usize,
) -> RelayReadCredit {
    let pressure_floor = relay_remote_read_pressure_floor_bytes(tun_mtu);
    let ack_drain_floor = relay_remote_read_ack_drain_floor_bytes(tun_mtu);
    let pressure_ack_drain_bytes =
        clamp_relay_remote_read_ack_drain_bytes(pressure_ack_drain_bytes, tun_mtu);
    if hard_pause_active || pending_bytes >= downlink_receive_window_high(cfg) {
        return RelayReadCredit {
            paused: true,
            max_batch_bytes: 0,
        };
    }

    let cap_pressure_region = |credit: RelayReadCredit| -> RelayReadCredit {
        if credit.paused || local_pressure_bytes <= cfg.high_bytes {
            return credit;
        }
        let pressure_edge = tx_queue_flush_threshold(cfg);
        let target_edge = tx_queue_egress_target_threshold(cfg);
        let predictive_ceiling = if local_pressure_bytes < pressure_edge {
            pressure_edge
                .saturating_sub(local_pressure_bytes)
                .max(pressure_ack_drain_bytes)
        } else if local_pressure_bytes < target_edge {
            pressure_ack_drain_bytes
        } else if local_pressure_bytes < tx_queue_pause_threshold(cfg)
            && pressure_ack_drain_bytes > ack_drain_floor
        {
            pressure_ack_drain_bytes.min(
                tx_queue_pause_threshold(cfg)
                    .saturating_sub(local_pressure_bytes)
                    .max(ack_drain_floor),
            )
        } else {
            ack_drain_floor
        };
        let max_batch_bytes = credit.max_batch_bytes.min(predictive_ceiling);
        RelayReadCredit {
            paused: max_batch_bytes == 0,
            max_batch_bytes,
        }
    };

    if egress_recovery_active
        || pending_bytes > 0
        || local_pressure_bytes >= tx_queue_flush_threshold(cfg)
    {
        return cap_pressure_region(RelayReadCredit {
            paused: false,
            max_batch_bytes: pressure_floor.max(pressure_ack_drain_bytes),
        });
    }

    let full_credit_edge = cfg.high_bytes;
    if local_pressure_bytes <= full_credit_edge {
        return cap_pressure_region(RelayReadCredit::default());
    }

    let taper_edge = tx_queue_flush_threshold(cfg);
    let span = taper_edge.saturating_sub(full_credit_edge);
    if span == 0 {
        return RelayReadCredit {
            paused: false,
            max_batch_bytes: pressure_floor,
        };
    }
    let headroom = taper_edge.saturating_sub(local_pressure_bytes);
    let elastic = RELAY_REMOTE_READ_BURST_MAX_BYTES.saturating_sub(pressure_floor);
    cap_pressure_region(RelayReadCredit {
        paused: false,
        max_batch_bytes: pressure_floor + elastic.saturating_mul(headroom).saturating_div(span),
    })
}

#[cfg(test)]
fn relay_read_credit_for_local_pressure(
    local_pressure_bytes: usize,
    pending_bytes: usize,
    egress_recovery_active: bool,
    hard_pause_active: bool,
    cfg: DownlinkBackpressureConfig,
    tun_mtu: usize,
) -> RelayReadCredit {
    relay_read_credit_for_local_pressure_with_ack_drain_floor(
        local_pressure_bytes,
        pending_bytes,
        egress_recovery_active,
        hard_pause_active,
        cfg,
        tun_mtu,
        relay_remote_read_ack_drain_floor_bytes(tun_mtu),
        DEFAULT_DOWNLINK_FLUSH_MAX_BYTES,
    )
}

#[allow(clippy::too_many_arguments)]
fn relay_read_credit_for_local_egress_with_ack_drain_floor(
    send_queue_bytes: usize,
    pending_bytes: usize,
    egress_recovery_active: bool,
    hard_pause_active: bool,
    cfg: DownlinkBackpressureConfig,
    tun_mtu: usize,
    pressure_ack_drain_bytes: usize,
    downlink_flush_max_bytes: usize,
) -> RelayReadCredit {
    relay_read_credit_for_local_pressure_with_ack_drain_floor(
        send_queue_bytes.saturating_add(pending_bytes),
        pending_bytes,
        egress_recovery_active,
        hard_pause_active,
        cfg,
        tun_mtu,
        pressure_ack_drain_bytes,
        downlink_flush_max_bytes,
    )
}

#[cfg(test)]
fn relay_read_credit_for_local_egress(
    send_queue_bytes: usize,
    pending_bytes: usize,
    egress_recovery_active: bool,
    hard_pause_active: bool,
    cfg: DownlinkBackpressureConfig,
    tun_mtu: usize,
) -> RelayReadCredit {
    relay_read_credit_for_local_pressure(
        send_queue_bytes.saturating_add(pending_bytes),
        pending_bytes,
        egress_recovery_active,
        hard_pause_active,
        cfg,
        tun_mtu,
    )
}

#[allow(clippy::too_many_arguments)]
fn relay_read_credit_for_projected_local_egress_with_ack_drain_floor(
    send_queue_bytes: usize,
    pending_bytes: usize,
    incoming_bytes: usize,
    egress_recovery_active: bool,
    hard_pause_active: bool,
    cfg: DownlinkBackpressureConfig,
    tun_mtu: usize,
    pressure_ack_drain_bytes: usize,
    downlink_flush_max_bytes: usize,
) -> RelayReadCredit {
    relay_read_credit_for_local_pressure_with_ack_drain_floor(
        send_queue_bytes
            .saturating_add(pending_bytes)
            .saturating_add(incoming_bytes),
        pending_bytes,
        egress_recovery_active,
        hard_pause_active,
        cfg,
        tun_mtu,
        pressure_ack_drain_bytes,
        downlink_flush_max_bytes,
    )
}

#[cfg(test)]
fn relay_read_credit_for_projected_local_egress(
    send_queue_bytes: usize,
    pending_bytes: usize,
    incoming_bytes: usize,
    egress_recovery_active: bool,
    hard_pause_active: bool,
    cfg: DownlinkBackpressureConfig,
    tun_mtu: usize,
) -> RelayReadCredit {
    relay_read_credit_for_local_pressure(
        send_queue_bytes
            .saturating_add(pending_bytes)
            .saturating_add(incoming_bytes),
        pending_bytes,
        egress_recovery_active,
        hard_pause_active,
        cfg,
        tun_mtu,
    )
}

fn relay_read_credit_for_terminal_no_recv_window(
    snapshot: SocketCloseSnapshot,
    pending_bytes: usize,
    cfg: DownlinkBackpressureConfig,
    tun_mtu: usize,
) -> Option<RelayReadCredit> {
    if snapshot.tcp_state != TcpState::CloseWait || snapshot.may_recv {
        return None;
    }
    if pending_bytes == 0
        && snapshot.active
        && snapshot.can_send
        && snapshot.send_queue < tx_queue_flush_threshold(cfg)
    {
        return None;
    }

    let staging_cap = relay_remote_read_ack_drain_floor_bytes(tun_mtu);
    let remaining = staging_cap.saturating_sub(pending_bytes);
    if remaining == 0 {
        return Some(RelayReadCredit {
            paused: true,
            max_batch_bytes: 0,
        });
    }
    Some(RelayReadCredit {
        paused: false,
        max_batch_bytes: remaining,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DownlinkCreditController {
    read_credit_ceiling_bytes: usize,
    adaptive_ack_drain_bytes: usize,
    ack_cadence_boost_bytes: usize,
    egress_payload_credit_bytes: usize,
    egress_progress_generation: u64,
    headroom_debt_bytes: usize,
    no_egress_progress_streak: u8,
}

impl Default for DownlinkCreditController {
    fn default() -> Self {
        Self {
            read_credit_ceiling_bytes: DOWNLINK_CREDIT_INITIAL_BATCH_BYTES,
            adaptive_ack_drain_bytes: 0,
            ack_cadence_boost_bytes: 0,
            egress_payload_credit_bytes: 0,
            egress_progress_generation: 0,
            headroom_debt_bytes: 0,
            no_egress_progress_streak: 0,
        }
    }
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
    /// Sender used to feed local egress pressure back to the relay read half.
    relay_read_credit_tx: Option<watch::Sender<RelayReadCredit>>,
    /// Latest egress-progress generation published to the relay read half.
    relay_read_credit_progress_generation_sent: u64,
    /// Whether this flow has already propagated the local TCP finish into the relay write half.
    local_fin_sent: bool,
    /// Whether remote EOF has been propagated to the intercepted local TCP peer with a FIN.
    local_eof_sent: bool,
    /// Monotonic event-loop seconds when local finish became pending but was deferred for reverse traffic.
    local_fin_pending_since_secs: Option<u64>,
    /// Latest remote-to-local progress while a local finish is pending.
    local_fin_last_remote_progress_secs: Option<u64>,
    /// Downlink bytes not yet accepted by the smoltcp tx buffer.
    /// 中文要点：smoltcp send_slice 可能只写一部分（tx buffer 受 TCP ACK 释放制约），
    /// 写不下的字节必须留在这里、由后续 poll 持续 flush，否则丢字节 → TLS bad decrypt。
    downlink_pending: Vec<u8>,
    /// Relay has ended, but downlink bytes still need to be flushed before the slot can be rearmed.
    pending_relay_close: Option<RelayClose>,
    /// Monotonic event-loop seconds when `pending_relay_close` was installed.
    pending_relay_close_since_secs: Option<u64>,
    /// Monotonic event-loop seconds when deferred close pending last made drain progress.
    pending_relay_close_last_progress_secs: Option<u64>,
    /// Last observed pending size for progress-sensitive deferred close reap.
    pending_relay_close_last_pending_bytes: usize,
    /// Latest event-loop second when queued smoltcp egress drained while relay close was deferred.
    pending_relay_close_last_egress_progress_secs: Option<u64>,
    /// Last observed smoltcp send queue size for egress-drain deferred close.
    pending_relay_close_last_egress_queue_bytes: usize,
    /// Monotonic event-loop seconds when generic pending downlink was first observed or last drained.
    downlink_pending_last_progress_secs: Option<u64>,
    /// Last observed pending size for generic pending downlink reap.
    downlink_pending_last_pending_bytes: usize,
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
    /// Knife14cb：observed local egress progress grants bounded extra downlink accept credit.
    downlink_egress_clock: DownlinkEgressClock,
    /// Knife14eo：per-flow controller coupling relay read credit, flush budget, and headroom debt.
    downlink_credit_controller: DownlinkCreditController,
    /// Last observed TCP socket lifecycle snapshot for transition diagnostics.
    last_tcp_lifecycle: Option<TcpLifecycleObservation>,
    /// Latest event-loop second when live reverse TCP window diagnostics were logged.
    reverse_window_last_log_secs: Option<u64>,
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
            relay_read_credit_tx: None,
            relay_read_credit_progress_generation_sent: 0,
            local_fin_sent: false,
            local_eof_sent: false,
            local_fin_pending_since_secs: None,
            local_fin_last_remote_progress_secs: None,
            downlink_pending: Vec::new(),
            pending_relay_close: None,
            pending_relay_close_since_secs: None,
            pending_relay_close_last_progress_secs: None,
            pending_relay_close_last_pending_bytes: 0,
            pending_relay_close_last_egress_progress_secs: None,
            pending_relay_close_last_egress_queue_bytes: 0,
            downlink_pending_last_progress_secs: None,
            downlink_pending_last_pending_bytes: 0,
            fake_ip: None,
            conn_epoch: 0,
            uplink_buffer: Vec::new(),
            downlink_diag: TcpDownlinkDiag::default(),
            downlink_egress_clock: DownlinkEgressClock::default(),
            downlink_credit_controller: DownlinkCreditController::default(),
            last_tcp_lifecycle: None,
            reverse_window_last_log_secs: None,
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
fn bounded_downlink_flush_len(pending_len: usize, max_bytes_per_flush: usize) -> usize {
    pending_len.min(max_bytes_per_flush.max(1))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DownlinkFlushLimit {
    len: usize,
    headroom_limited: bool,
    headroom_deferred_bytes: usize,
    clean_headroom_bytes: usize,
    drain_credit_granted_bytes: usize,
    drain_credit_planned_bytes: usize,
    drop_credit_debt_bytes: usize,
    drop_credit_debt_paid_bytes: usize,
    drop_credit_blocked_bytes: usize,
    pressure_credit_debt_bytes: usize,
    pressure_credit_debt_paid_bytes: usize,
    pressure_credit_blocked_bytes: usize,
    hard_edge_guard_bytes: usize,
    hard_edge_guard_deferred_bytes: usize,
}

fn downlink_pressure_staging_limit(cfg: DownlinkBackpressureConfig, tun_mtu: usize) -> usize {
    let floor = relay_remote_read_pressure_floor_bytes(tun_mtu);
    floor
        .saturating_mul(2)
        .max(cfg.low_bytes)
        .min(downlink_receive_window_high(cfg))
}

fn downlink_egress_payload_credit_cap(cfg: DownlinkBackpressureConfig, tun_mtu: usize) -> usize {
    downlink_egress_credit_span(cfg)
        .max(relay_remote_read_pressure_floor_bytes(tun_mtu))
        .min(RELAY_REMOTE_READ_BURST_MAX_BYTES)
}

impl DownlinkCreditController {
    fn payload_token_credit_for_pressure(
        self,
        local_pressure_bytes: usize,
        cfg: DownlinkBackpressureConfig,
        tun_mtu: usize,
    ) -> usize {
        if local_pressure_bytes < tx_queue_flush_threshold(cfg)
            || local_pressure_bytes >= tx_queue_pause_threshold(cfg)
        {
            return 0;
        }
        let payload_headroom =
            tx_queue_egress_target_threshold(cfg).saturating_sub(local_pressure_bytes);
        if payload_headroom < relay_remote_read_ack_drain_floor_bytes(tun_mtu) {
            return 0;
        }
        self.egress_payload_credit_bytes
            .min(downlink_egress_payload_credit_cap(cfg, tun_mtu))
            .min(payload_headroom)
    }

    fn note_payload_read_for_projected_pressure(
        &mut self,
        incoming_bytes: usize,
        projected_pressure_bytes: usize,
        cfg: DownlinkBackpressureConfig,
    ) {
        if incoming_bytes == 0 || projected_pressure_bytes < tx_queue_flush_threshold(cfg) {
            return;
        }
        self.egress_payload_credit_bytes = self
            .egress_payload_credit_bytes
            .saturating_sub(incoming_bytes);
    }

    fn adaptive_ack_drain_floor_for(
        self,
        local_pressure_bytes: usize,
        cfg: DownlinkBackpressureConfig,
        tun_mtu: usize,
    ) -> usize {
        let ack_drain_floor = relay_remote_read_ack_drain_floor_bytes(tun_mtu);
        if self.no_egress_progress_streak >= 2
            || local_pressure_bytes >= tx_queue_pause_threshold(cfg)
        {
            return ack_drain_floor;
        }
        let adaptive_ack_drain_max = relay_remote_read_adaptive_ack_drain_max_bytes(tun_mtu);
        let pressure_floor = relay_remote_read_pressure_floor_bytes(tun_mtu);
        let pause_headroom = tx_queue_pause_threshold(cfg).saturating_sub(local_pressure_bytes);
        let adaptive = if local_pressure_bytes >= tx_queue_egress_target_threshold(cfg) {
            if self.adaptive_ack_drain_bytes <= ack_drain_floor {
                ack_drain_floor
            } else {
                let adaptive_cap = if local_pressure_bytes >= tx_queue_credit_spend_threshold(cfg) {
                    pressure_floor
                } else {
                    adaptive_ack_drain_max
                };
                self.adaptive_ack_drain_bytes
                    .min(adaptive_ack_drain_max)
                    .min(adaptive_cap)
                    .min(pause_headroom.max(ack_drain_floor))
                    .max(ack_drain_floor)
            }
        } else {
            clamp_relay_remote_read_ack_drain_bytes(self.adaptive_ack_drain_bytes, tun_mtu)
        };
        let boost = if self.ack_cadence_boost_bytes == 0 {
            0
        } else {
            clamp_relay_remote_read_ack_drain_bytes(self.ack_cadence_boost_bytes, tun_mtu)
                .min(pause_headroom.max(ack_drain_floor))
        };
        adaptive.max(boost).max(ack_drain_floor)
    }

    fn note_ack_cadence_gap_hint(
        &mut self,
        local_pressure_bytes: usize,
        hard_pause_active: bool,
        cfg: DownlinkBackpressureConfig,
        tun_mtu: usize,
    ) {
        if hard_pause_active
            || self.no_egress_progress_streak >= 2
            || local_pressure_bytes >= tx_queue_pause_threshold(cfg)
        {
            self.ack_cadence_boost_bytes = 0;
            return;
        }

        let ack_drain_floor = relay_remote_read_ack_drain_floor_bytes(tun_mtu);
        let pressure_floor = relay_remote_read_pressure_floor_bytes(tun_mtu);
        let pause_headroom = tx_queue_pause_threshold(cfg).saturating_sub(local_pressure_bytes);
        let target = pressure_floor.min(pause_headroom.max(ack_drain_floor));
        self.ack_cadence_boost_bytes =
            clamp_relay_remote_read_ack_drain_bytes(target.max(ack_drain_floor), tun_mtu);
    }

    fn consume_ack_cadence_boost_for_publish(
        &mut self,
        local_pressure_bytes: usize,
        hard_pause_active: bool,
        cfg: DownlinkBackpressureConfig,
        tun_mtu: usize,
    ) -> usize {
        if hard_pause_active {
            self.ack_cadence_boost_bytes = 0;
        }
        let floor = self.adaptive_ack_drain_floor_for(local_pressure_bytes, cfg, tun_mtu);
        self.ack_cadence_boost_bytes = 0;
        floor
    }

    #[cfg(test)]
    fn note_flush_feedback(
        &mut self,
        limit: DownlinkFlushLimit,
        cfg: DownlinkBackpressureConfig,
        tun_mtu: usize,
    ) {
        self.note_flush_feedback_with_accepted(limit, 0, cfg, tun_mtu);
    }

    fn note_flush_feedback_with_accepted(
        &mut self,
        limit: DownlinkFlushLimit,
        accepted_egress_bytes: usize,
        cfg: DownlinkBackpressureConfig,
        tun_mtu: usize,
    ) {
        let pressure_floor = relay_remote_read_pressure_floor_bytes(tun_mtu);
        let ack_drain_floor = relay_remote_read_ack_drain_floor_bytes(tun_mtu);
        let adaptive_ack_drain_max = relay_remote_read_adaptive_ack_drain_max_bytes(tun_mtu);
        let accepted_egress_bytes = accepted_egress_bytes.min(limit.len);
        let observed_progress = limit
            .drain_credit_granted_bytes
            .saturating_add(limit.drop_credit_debt_paid_bytes)
            .saturating_add(limit.pressure_credit_debt_paid_bytes)
            .saturating_add(accepted_egress_bytes);
        if observed_progress > 0 {
            self.egress_progress_generation = self.egress_progress_generation.saturating_add(1);
            self.no_egress_progress_streak = 0;
            self.headroom_debt_bytes = self.headroom_debt_bytes.saturating_sub(observed_progress);
            self.egress_payload_credit_bytes = self
                .egress_payload_credit_bytes
                .saturating_add(observed_progress)
                .min(downlink_egress_payload_credit_cap(cfg, tun_mtu));
            let grown = observed_progress
                .saturating_mul(DOWNLINK_CREDIT_PROGRESS_GAIN)
                .max(pressure_floor);
            self.read_credit_ceiling_bytes = self
                .read_credit_ceiling_bytes
                .saturating_add(grown)
                .min(RELAY_REMOTE_READ_BURST_MAX_BYTES)
                .max(pressure_floor);
            let grow_by = observed_progress
                .min(adaptive_ack_drain_max)
                .max(pressure_floor);
            self.adaptive_ack_drain_bytes = self
                .adaptive_ack_drain_bytes
                .max(ack_drain_floor)
                .saturating_add(grow_by)
                .min(adaptive_ack_drain_max);
        }

        let hard_edge_guard_feedback_bytes = if observed_progress == 0 {
            limit.hard_edge_guard_deferred_bytes
        } else {
            0
        };
        let deferred = limit
            .headroom_deferred_bytes
            .saturating_add(hard_edge_guard_feedback_bytes)
            .saturating_add(limit.drop_credit_blocked_bytes);
        if deferred == 0 {
            return;
        }

        self.headroom_debt_bytes = self
            .headroom_debt_bytes
            .saturating_add(deferred)
            .min(downlink_receive_window_high(cfg));
        if observed_progress == 0 {
            self.no_egress_progress_streak = self
                .no_egress_progress_streak
                .saturating_add(1)
                .min(DOWNLINK_CREDIT_MAX_NO_PROGRESS_STREAK);
        }
        self.read_credit_ceiling_bytes = self
            .read_credit_ceiling_bytes
            .saturating_div(2)
            .max(ack_drain_floor);
        let hard_or_blocked =
            hard_edge_guard_feedback_bytes.saturating_add(limit.drop_credit_blocked_bytes);
        if observed_progress == 0 || hard_or_blocked > 0 {
            self.ack_cadence_boost_bytes = 0;
            self.egress_payload_credit_bytes = if self.no_egress_progress_streak >= 2 {
                0
            } else if observed_progress > 0 {
                self.egress_payload_credit_bytes
            } else {
                self.egress_payload_credit_bytes.saturating_div(2)
            };
            if observed_progress == 0 {
                self.adaptive_ack_drain_bytes = self
                    .adaptive_ack_drain_bytes
                    .saturating_div(2)
                    .max(ack_drain_floor);
                if self.no_egress_progress_streak >= 2 {
                    self.adaptive_ack_drain_bytes = ack_drain_floor;
                }
            }
        }
    }

    fn flush_budget(
        self,
        configured_budget: usize,
        _cfg: DownlinkBackpressureConfig,
        tun_mtu: usize,
    ) -> usize {
        let configured_budget = configured_budget.max(1);
        if self.headroom_debt_bytes == 0 && self.no_egress_progress_streak == 0 {
            return configured_budget;
        }
        configured_budget.min(
            self.read_credit_ceiling_bytes
                .max(relay_remote_read_ack_drain_floor_bytes(tun_mtu)),
        )
    }

    fn read_credit_for(
        self,
        base: RelayReadCredit,
        pending_bytes: usize,
        cfg: DownlinkBackpressureConfig,
        tun_mtu: usize,
    ) -> RelayReadCredit {
        if base.paused {
            return base;
        }

        let ack_drain_floor = relay_remote_read_ack_drain_floor_bytes(tun_mtu);
        let stalled_egress = self.no_egress_progress_streak >= 2;
        let batch_constrained = self.no_egress_progress_streak >= 2;
        let staging_limit = if stalled_egress {
            downlink_pressure_staging_limit(cfg, tun_mtu)
        } else if self.headroom_debt_bytes > 0 {
            cfg.high_bytes
                .max(downlink_pressure_staging_limit(cfg, tun_mtu))
                .min(downlink_receive_window_high(cfg))
        } else {
            downlink_receive_window_high(cfg)
        };
        if pending_bytes >= staging_limit {
            return RelayReadCredit {
                paused: true,
                max_batch_bytes: 0,
            };
        }

        let staging_remaining = staging_limit.saturating_sub(pending_bytes);
        if staging_remaining < ack_drain_floor {
            return RelayReadCredit {
                paused: true,
                max_batch_bytes: 0,
            };
        }
        let clean_empty_staging = self.headroom_debt_bytes == 0
            && self.no_egress_progress_streak == 0
            && pending_bytes == 0;
        let progress_ceiling = if batch_constrained {
            ack_drain_floor
        } else if clean_empty_staging {
            base.max_batch_bytes
                .max(self.read_credit_ceiling_bytes)
                .max(ack_drain_floor)
        } else {
            self.read_credit_ceiling_bytes.max(ack_drain_floor)
        };
        let max_batch_bytes = base
            .max_batch_bytes
            .min(progress_ceiling)
            .min(staging_remaining);
        RelayReadCredit {
            paused: max_batch_bytes == 0,
            max_batch_bytes,
        }
    }

    fn read_credit_for_local_pressure(
        self,
        base: RelayReadCredit,
        pending_bytes: usize,
        local_pressure_bytes: usize,
        cfg: DownlinkBackpressureConfig,
        tun_mtu: usize,
    ) -> RelayReadCredit {
        if base.paused {
            return self.read_credit_for(base, pending_bytes, cfg, tun_mtu);
        }

        let token_credit =
            self.payload_token_credit_for_pressure(local_pressure_bytes, cfg, tun_mtu);
        let base = RelayReadCredit {
            paused: false,
            max_batch_bytes: base.max_batch_bytes.max(token_credit),
        };
        self.read_credit_for(base, pending_bytes, cfg, tun_mtu)
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct DownlinkEgressClock {
    last_send_queue: Option<usize>,
    drain_credit_bytes: usize,
    drop_debt_generation_seen: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EgressCreditDebtSource {
    Drop,
    Pressure,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct DownlinkEgressDebtPayment {
    drop_bytes: usize,
    pressure_bytes: usize,
}

impl DownlinkEgressDebtPayment {
    fn total(self) -> usize {
        self.drop_bytes.saturating_add(self.pressure_bytes)
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct DownlinkEgressCreditDebt {
    generation: u64,
    debt_bytes: usize,
    drop_debt_bytes: usize,
    pressure_debt_bytes: usize,
    drop_events: u64,
    pressure_events: u64,
    last_source: Option<EgressCreditDebtSource>,
}

type DownlinkEgressDropDebt = DownlinkEgressCreditDebt;

impl DownlinkEgressCreditDebt {
    fn has_active_drop_debt(&self) -> bool {
        self.drop_debt_bytes > 0
    }

    fn note_tun_drop(&mut self, cfg: DownlinkBackpressureConfig) -> usize {
        let added = self.install(
            EgressCreditDebtSource::Drop,
            downlink_egress_credit_span(cfg),
        );
        if added > 0 {
            self.drop_events = self.drop_events.saturating_add(1);
        }
        added
    }

    fn note_egress_pressure(
        &mut self,
        stats: DownlinkPressureStats,
        cfg: DownlinkBackpressureConfig,
    ) -> usize {
        let debt_bytes = downlink_egress_pressure_debt_bytes(stats, cfg);
        if debt_bytes == 0 {
            return 0;
        }
        let added = self.install(EgressCreditDebtSource::Pressure, debt_bytes);
        if added > 0 {
            self.pressure_events = self.pressure_events.saturating_add(1);
        }
        added
    }

    #[cfg(test)]
    fn note_projected_egress_pressure(
        &mut self,
        projected_pressure_bytes: usize,
        cfg: DownlinkBackpressureConfig,
    ) -> usize {
        let debt_bytes =
            downlink_egress_projected_pressure_debt_bytes(projected_pressure_bytes, cfg);
        if debt_bytes == 0 {
            return 0;
        }
        let added = self.install(EgressCreditDebtSource::Pressure, debt_bytes);
        if added > 0 {
            self.pressure_events = self.pressure_events.saturating_add(1);
        }
        added
    }

    fn note_post_flush_projected_egress_pressure(
        &mut self,
        send_queue_bytes: usize,
        pending_bytes: usize,
        accepted_bytes: usize,
        cfg: DownlinkBackpressureConfig,
    ) -> usize {
        let debt_bytes = post_flush_projected_pressure_debt_bytes(
            send_queue_bytes,
            pending_bytes,
            accepted_bytes,
            cfg,
        );
        if debt_bytes == 0 {
            return 0;
        }
        let added = self.install(EgressCreditDebtSource::Pressure, debt_bytes);
        if added > 0 {
            self.pressure_events = self.pressure_events.saturating_add(1);
        }
        added
    }

    fn install(&mut self, source: EgressCreditDebtSource, installed: usize) -> usize {
        if installed == 0 {
            return 0;
        }
        let added = installed.saturating_sub(self.debt_bytes);
        if added == 0 {
            return 0;
        }
        self.generation = self.generation.saturating_add(1);
        self.last_source = Some(source);
        self.debt_bytes = self.debt_bytes.saturating_add(added).min(installed);
        match source {
            EgressCreditDebtSource::Drop => {
                self.drop_debt_bytes = self.drop_debt_bytes.saturating_add(added);
            }
            EgressCreditDebtSource::Pressure => {
                self.pressure_debt_bytes = self.pressure_debt_bytes.saturating_add(added);
            }
        }
        added
    }

    fn pay(&mut self, bytes: usize) -> DownlinkEgressDebtPayment {
        let pressure_bytes = bytes.min(self.pressure_debt_bytes);
        self.pressure_debt_bytes -= pressure_bytes;
        let remaining = bytes.saturating_sub(pressure_bytes);
        let drop_bytes = remaining.min(self.drop_debt_bytes);
        self.drop_debt_bytes -= drop_bytes;
        let total = pressure_bytes.saturating_add(drop_bytes);
        self.debt_bytes = self.debt_bytes.saturating_sub(total);
        DownlinkEgressDebtPayment {
            drop_bytes,
            pressure_bytes,
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct DownlinkEgressCreditObservation {
    granted_bytes: usize,
    drop_debt_paid_bytes: usize,
    drop_credit_blocked_bytes: usize,
    pressure_debt_paid_bytes: usize,
    pressure_credit_blocked_bytes: usize,
}

impl DownlinkEgressClock {
    fn note_observed_send_queue(
        &mut self,
        send_queue: usize,
        cfg: DownlinkBackpressureConfig,
        drop_debt: &mut DownlinkEgressDropDebt,
    ) -> DownlinkEgressCreditObservation {
        let mut observation = DownlinkEgressCreditObservation::default();
        if self.drop_debt_generation_seen != drop_debt.generation {
            self.drop_debt_generation_seen = drop_debt.generation;
            if drop_debt.has_active_drop_debt() {
                observation.drop_credit_blocked_bytes = self.drain_credit_bytes;
                self.drain_credit_bytes = 0;
            } else if matches!(
                drop_debt.last_source,
                Some(EgressCreditDebtSource::Pressure)
            ) {
                observation.pressure_credit_blocked_bytes = 0;
            }
        }

        let Some(last_send_queue) = self.last_send_queue else {
            self.last_send_queue = Some(send_queue);
            return observation;
        };
        if last_send_queue <= send_queue {
            self.last_send_queue = Some(send_queue);
            return observation;
        }

        let max_credit = downlink_egress_credit_span(cfg);
        let observed_drain = last_send_queue.saturating_sub(send_queue);
        let debt_payment = drop_debt.pay(observed_drain);
        let debt_paid = debt_payment.total();
        observation.drop_debt_paid_bytes = debt_payment.drop_bytes;
        observation.pressure_debt_paid_bytes = debt_payment.pressure_bytes;
        observation.drop_credit_blocked_bytes = observation
            .drop_credit_blocked_bytes
            .saturating_add(debt_payment.drop_bytes.min(max_credit));
        observation.pressure_credit_blocked_bytes = observation
            .pressure_credit_blocked_bytes
            .saturating_add(debt_payment.pressure_bytes.min(max_credit));
        let creditable_drain = if debt_paid == 0 { observed_drain } else { 0 };
        let granted = if !drop_debt.has_active_drop_debt() {
            creditable_drain.min(max_credit)
        } else {
            0
        };
        self.drain_credit_bytes = self
            .drain_credit_bytes
            .saturating_add(granted)
            .min(max_credit);
        self.last_send_queue = Some(send_queue);
        observation.granted_bytes = granted;
        observation
    }

    fn note_flush_result(
        &mut self,
        send_queue_before: usize,
        accepted: usize,
        limit: DownlinkFlushLimit,
    ) -> usize {
        let used = accepted
            .saturating_sub(limit.clean_headroom_bytes)
            .min(limit.drain_credit_planned_bytes);
        self.drain_credit_bytes = self.drain_credit_bytes.saturating_sub(used);
        self.last_send_queue = Some(send_queue_before.saturating_add(accepted));
        used
    }
}

fn downlink_egress_credit_span(cfg: DownlinkBackpressureConfig) -> usize {
    tx_queue_pause_threshold(cfg).saturating_sub(tx_queue_flush_threshold(cfg))
}

fn downlink_egress_pressure_debt_bytes(
    stats: DownlinkPressureStats,
    cfg: DownlinkBackpressureConfig,
) -> usize {
    if !reaches_downlink_credit_debt_pressure_threshold(stats, cfg) {
        return 0;
    }
    let span = downlink_egress_credit_span(cfg);
    if span == 0 {
        return 0;
    }
    let guard = tx_queue_credit_guard_bytes(cfg).max(1).min(span);
    let tx_overshoot = stats
        .max_tx_queue
        .saturating_sub(tx_queue_egress_target_threshold(cfg));
    let pending_overshoot = if stats.max_pending >= cfg.high_bytes
        && stats.max_tx_queue >= tx_queue_flush_threshold(cfg)
    {
        stats.max_pending.saturating_sub(cfg.high_bytes)
    } else {
        0
    };
    guard
        .saturating_add(tx_overshoot.max(pending_overshoot))
        .min(span)
}

fn downlink_projected_local_pressure_bytes(send_queue_bytes: usize, pending_bytes: usize) -> usize {
    send_queue_bytes.saturating_add(pending_bytes)
}

fn downlink_egress_projected_pressure_debt_bytes(
    projected_pressure_bytes: usize,
    cfg: DownlinkBackpressureConfig,
) -> usize {
    let target_edge = tx_queue_egress_target_threshold(cfg);
    if projected_pressure_bytes < tx_queue_credit_spend_threshold(cfg) {
        return 0;
    }
    let span = downlink_egress_credit_span(cfg);
    if span == 0 {
        return 0;
    }
    tx_queue_credit_guard_bytes(cfg)
        .max(1)
        .min(span)
        .saturating_add(projected_pressure_bytes.saturating_sub(target_edge))
        .min(span)
}

fn post_flush_projected_pressure_debt_bytes(
    send_queue_bytes: usize,
    pending_bytes: usize,
    accepted_bytes: usize,
    cfg: DownlinkBackpressureConfig,
) -> usize {
    if accepted_bytes > 0 && pending_bytes == 0 {
        return 0;
    }
    downlink_egress_projected_pressure_debt_bytes(
        downlink_projected_local_pressure_bytes(send_queue_bytes, pending_bytes),
        cfg,
    )
}

#[cfg(test)]
fn bounded_downlink_flush_limit_for_window(
    pending_len: usize,
    max_bytes_per_flush: usize,
    send_window: TcpSendWindowSnapshot,
    cfg: DownlinkBackpressureConfig,
) -> DownlinkFlushLimit {
    let budget_len = bounded_downlink_flush_len(pending_len, max_bytes_per_flush);
    let socket_len = budget_len.min(send_window.send_capacity);
    let egress_headroom = tx_queue_flush_threshold(cfg).saturating_sub(send_window.send_queue);
    let len = socket_len.min(egress_headroom);
    DownlinkFlushLimit {
        len,
        headroom_limited: len < socket_len,
        headroom_deferred_bytes: socket_len.saturating_sub(len),
        clean_headroom_bytes: egress_headroom,
        drain_credit_granted_bytes: 0,
        drain_credit_planned_bytes: 0,
        drop_credit_debt_bytes: 0,
        drop_credit_debt_paid_bytes: 0,
        drop_credit_blocked_bytes: 0,
        pressure_credit_debt_bytes: 0,
        pressure_credit_debt_paid_bytes: 0,
        pressure_credit_blocked_bytes: 0,
        hard_edge_guard_bytes: 0,
        hard_edge_guard_deferred_bytes: 0,
    }
}

#[cfg(test)]
fn bounded_downlink_flush_limit_for_window_with_clock(
    pending_len: usize,
    max_bytes_per_flush: usize,
    send_window: TcpSendWindowSnapshot,
    cfg: DownlinkBackpressureConfig,
    clock: &mut DownlinkEgressClock,
) -> DownlinkFlushLimit {
    let mut drop_debt = DownlinkEgressDropDebt::default();
    bounded_downlink_flush_limit_for_window_with_clock_and_drop(
        pending_len,
        max_bytes_per_flush,
        send_window,
        cfg,
        clock,
        &mut drop_debt,
    )
}

fn bounded_downlink_flush_limit_for_window_with_clock_and_drop(
    pending_len: usize,
    max_bytes_per_flush: usize,
    send_window: TcpSendWindowSnapshot,
    cfg: DownlinkBackpressureConfig,
    clock: &mut DownlinkEgressClock,
    drop_debt: &mut DownlinkEgressDropDebt,
) -> DownlinkFlushLimit {
    let budget_len = bounded_downlink_flush_len(pending_len, max_bytes_per_flush);
    let socket_len = budget_len.min(send_window.send_capacity);
    let clean_headroom = tx_queue_flush_threshold(cfg).saturating_sub(send_window.send_queue);
    let hard_headroom = tx_queue_pause_threshold(cfg).saturating_sub(send_window.send_queue);
    let target_edge = tx_queue_egress_target_threshold(cfg);
    let credit_observation = clock.note_observed_send_queue(send_window.send_queue, cfg, drop_debt);
    let hard_credit_blocked = drop_debt.has_active_drop_debt();
    let hard_limit_edge = if hard_credit_blocked {
        target_edge
    } else {
        tx_queue_credit_spend_threshold(cfg)
    };
    let hard_edge_guard = tx_queue_pause_threshold(cfg).saturating_sub(hard_limit_edge);
    let guarded_hard_headroom = hard_limit_edge.saturating_sub(send_window.send_queue);
    let usable_credit = if hard_credit_blocked {
        0
    } else {
        clock.drain_credit_bytes
    };
    let unguarded_available = clean_headroom
        .saturating_add(usable_credit)
        .min(hard_headroom);
    let available = unguarded_available.min(guarded_hard_headroom);
    let unguarded_len = socket_len.min(unguarded_available);
    let len = socket_len.min(available);
    let drain_credit_planned = len.saturating_sub(clean_headroom).min(usable_credit);
    DownlinkFlushLimit {
        len,
        headroom_limited: len < socket_len,
        headroom_deferred_bytes: socket_len.saturating_sub(len),
        clean_headroom_bytes: clean_headroom,
        drain_credit_granted_bytes: credit_observation.granted_bytes,
        drain_credit_planned_bytes: drain_credit_planned,
        drop_credit_debt_bytes: drop_debt.drop_debt_bytes,
        drop_credit_debt_paid_bytes: credit_observation.drop_debt_paid_bytes,
        drop_credit_blocked_bytes: credit_observation.drop_credit_blocked_bytes,
        pressure_credit_debt_bytes: drop_debt.pressure_debt_bytes,
        pressure_credit_debt_paid_bytes: credit_observation.pressure_debt_paid_bytes,
        pressure_credit_blocked_bytes: credit_observation.pressure_credit_blocked_bytes,
        hard_edge_guard_bytes: hard_edge_guard,
        hard_edge_guard_deferred_bytes: unguarded_len.saturating_sub(len),
    }
}

fn close_drain_terminal_pending_flush_limit(
    pending_len: usize,
    max_bytes_per_flush: usize,
    send_window: TcpSendWindowSnapshot,
    cfg: DownlinkBackpressureConfig,
    mut limit: DownlinkFlushLimit,
) -> DownlinkFlushLimit {
    let terminal_pending = pending_len.min(CLOSE_DRAIN_TERMINAL_PENDING_MAX_BYTES);
    if terminal_pending == 0 {
        return limit;
    }

    let budget_len = bounded_downlink_flush_len(terminal_pending, max_bytes_per_flush);
    let socket_len = budget_len.min(send_window.send_capacity);
    let close_drain_target_headroom =
        tx_queue_egress_target_threshold(cfg).saturating_sub(send_window.send_queue);
    let close_drain_credit_headroom =
        tx_queue_credit_spend_threshold(cfg).saturating_sub(send_window.send_queue);
    let close_drain_len = socket_len.min(close_drain_target_headroom);
    if close_drain_len <= limit.len {
        return limit;
    }

    limit.len = close_drain_len;
    limit.headroom_limited = close_drain_len < socket_len;
    limit.headroom_deferred_bytes = socket_len.saturating_sub(close_drain_len);
    limit.hard_edge_guard_deferred_bytes = socket_len
        .min(close_drain_credit_headroom)
        .saturating_sub(close_drain_len);
    limit
}

#[cfg(test)]
fn bounded_downlink_flush_len_for_window(
    pending_len: usize,
    max_bytes_per_flush: usize,
    send_window: TcpSendWindowSnapshot,
    cfg: DownlinkBackpressureConfig,
) -> usize {
    bounded_downlink_flush_limit_for_window(pending_len, max_bytes_per_flush, send_window, cfg).len
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DownlinkFlushOutcome {
    accepted_bytes: usize,
    headroom_limited: bool,
}

fn flush_downlink(
    handle: SocketHandle,
    tcp_socket: &mut TcpSocket,
    ctx: &mut SocketCtx,
    max_bytes_per_flush: usize,
    downlink_backpressure: DownlinkBackpressureConfig,
    tun_mtu: usize,
    downlink_egress_drop_debt: &mut DownlinkEgressDropDebt,
) -> DownlinkFlushOutcome {
    if ctx.downlink_pending.is_empty() {
        return DownlinkFlushOutcome {
            accepted_bytes: 0,
            headroom_limited: false,
        };
    }
    let send_window = TcpSendWindowSnapshot::from_socket(tcp_socket);
    ctx.downlink_diag
        .note_send_window(send_window, ctx.downlink_pending.len());
    if !send_window.can_send {
        ctx.downlink_diag
            .note_flush_attempt(ctx.downlink_pending.len(), max_bytes_per_flush);
        ctx.downlink_diag
            .note_no_send_capacity(ctx.downlink_pending.len());
        return DownlinkFlushOutcome {
            accepted_bytes: 0,
            headroom_limited: false,
        };
    }
    let controlled_max_bytes_per_flush = ctx.downlink_credit_controller.flush_budget(
        max_bytes_per_flush,
        downlink_backpressure,
        tun_mtu,
    );
    let mut flush_limit = bounded_downlink_flush_limit_for_window_with_clock_and_drop(
        ctx.downlink_pending.len(),
        controlled_max_bytes_per_flush,
        send_window,
        downlink_backpressure,
        &mut ctx.downlink_egress_clock,
        downlink_egress_drop_debt,
    );
    if ctx.pending_relay_close.is_some() && !ctx.local_eof_sent {
        let expanded = close_drain_terminal_pending_flush_limit(
            ctx.downlink_pending.len(),
            max_bytes_per_flush,
            send_window,
            downlink_backpressure,
            flush_limit,
        );
        if expanded.len > flush_limit.len {
            tcp_diag_log!(
                "🔎 tcp-close-drain-flush-credit handle={:?} pending={} send_queue={} len={} clean_high={} target_high={} credit_high={} pause_high={} target_deferred={}",
                handle,
                ctx.downlink_pending.len(),
                send_window.send_queue,
                expanded.len,
                tx_queue_flush_threshold(downlink_backpressure),
                tx_queue_egress_target_threshold(downlink_backpressure),
                tx_queue_credit_spend_threshold(downlink_backpressure),
                tx_queue_pause_threshold(downlink_backpressure),
                expanded.hard_edge_guard_deferred_bytes
            );
            flush_limit = expanded;
        }
    }
    ctx.downlink_diag.note_flush_limit(
        ctx.downlink_pending.len(),
        controlled_max_bytes_per_flush,
        flush_limit,
    );
    if flush_limit.len == 0 {
        let used =
            ctx.downlink_egress_clock
                .note_flush_result(send_window.send_queue, 0, flush_limit);
        ctx.downlink_diag.note_drain_credit_used(used);
        ctx.downlink_credit_controller
            .note_flush_feedback_with_accepted(flush_limit, 0, downlink_backpressure, tun_mtu);
        return DownlinkFlushOutcome {
            accepted_bytes: 0,
            headroom_limited: flush_limit.headroom_limited,
        };
    }
    let flush_len = flush_limit.len;
    match tcp_socket.send_slice(&ctx.downlink_pending[..flush_len]) {
        Ok(0) => {
            let used =
                ctx.downlink_egress_clock
                    .note_flush_result(send_window.send_queue, 0, flush_limit);
            ctx.downlink_diag.note_drain_credit_used(used);
            ctx.downlink_diag
                .note_send_slice_ok(0, ctx.downlink_pending.len());
            ctx.downlink_credit_controller
                .note_flush_feedback_with_accepted(flush_limit, 0, downlink_backpressure, tun_mtu);
            DownlinkFlushOutcome {
                accepted_bytes: 0,
                headroom_limited: flush_limit.headroom_limited,
            }
        }
        Ok(n) => {
            let used =
                ctx.downlink_egress_clock
                    .note_flush_result(send_window.send_queue, n, flush_limit);
            ctx.downlink_diag.note_drain_credit_used(used);
            ctx.downlink_pending.drain(..n);
            ctx.downlink_diag
                .note_send_slice_ok(n, ctx.downlink_pending.len());
            ctx.downlink_credit_controller
                .note_flush_feedback_with_accepted(flush_limit, n, downlink_backpressure, tun_mtu);
            DownlinkFlushOutcome {
                accepted_bytes: n,
                headroom_limited: flush_limit.headroom_limited,
            }
        }
        Err(_) => {
            let used =
                ctx.downlink_egress_clock
                    .note_flush_result(send_window.send_queue, 0, flush_limit);
            ctx.downlink_diag.note_drain_credit_used(used);
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
            ctx.downlink_credit_controller
                .note_flush_feedback_with_accepted(flush_limit, 0, downlink_backpressure, tun_mtu);
            DownlinkFlushOutcome {
                accepted_bytes: 0,
                headroom_limited: flush_limit.headroom_limited,
            }
        }
    }
}

fn update_pending_drain_progress(
    pending: usize,
    last_progress_secs: &mut Option<u64>,
    last_pending_bytes: &mut usize,
    now_secs: u64,
    accepted_bytes: usize,
) {
    if pending == 0 {
        *last_progress_secs = None;
        *last_pending_bytes = 0;
        return;
    }

    let last_pending = *last_pending_bytes;
    if last_pending == 0 || pending < last_pending || accepted_bytes > 0 {
        *last_pending_bytes = pending;
        *last_progress_secs = Some(now_secs);
    } else if pending > last_pending {
        *last_pending_bytes = pending;
    }
}

fn note_downlink_pending_progress(ctx: &mut SocketCtx, now_secs: u64, accepted_bytes: usize) {
    update_pending_drain_progress(
        ctx.downlink_pending.len(),
        &mut ctx.downlink_pending_last_progress_secs,
        &mut ctx.downlink_pending_last_pending_bytes,
        now_secs,
        accepted_bytes,
    );
    note_deferred_close_drain_progress(ctx, now_secs, accepted_bytes);
}

fn note_local_finish_remote_progress(ctx: &mut SocketCtx, now_secs: u64) {
    if ctx.local_fin_pending_since_secs.is_some() && !ctx.local_fin_sent {
        ctx.local_fin_last_remote_progress_secs = Some(now_secs);
    }
}

fn note_deferred_close_drain_progress(ctx: &mut SocketCtx, now_secs: u64, accepted_bytes: usize) {
    if ctx.pending_relay_close.is_none() {
        return;
    }
    update_pending_drain_progress(
        ctx.downlink_pending.len(),
        &mut ctx.pending_relay_close_last_progress_secs,
        &mut ctx.pending_relay_close_last_pending_bytes,
        now_secs,
        accepted_bytes,
    );
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
    socket_buffers: TcpSocketBufferConfig,
    /// 全局 listener socket 总数上限（#2 弹性扩容兜底；默认 [`MAX_TOTAL_LISTENERS`]）。
    max_total: usize,
    /// 当前 listener socket 总数（review #6：O(1) 计数器，避免每 SYN 在扩容循环里 O(端口) 求和）。
    /// 中文要点：槽只增不删（rearm/reap 复用回 Listen，从不从 ports 移除），故计数器随建槽单调累加。
    total: usize,
}

impl ListenerRegistry {
    #[cfg(test)]
    fn new(pool_size: usize) -> Self {
        Self::with_socket_buffers(pool_size, TcpSocketBufferConfig::default())
    }

    fn with_socket_buffers(pool_size: usize, socket_buffers: TcpSocketBufferConfig) -> Self {
        Self {
            ports: HashMap::new(),
            pool_size,
            socket_buffers,
            max_total: MAX_TOTAL_LISTENERS,
            total: 0,
        }
    }

    #[cfg(test)]
    fn with_max_total(pool_size: usize, max_total: usize) -> Self {
        Self {
            ports: HashMap::new(),
            pool_size,
            socket_buffers: TcpSocketBufferConfig::default(),
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
            let h = sockets.add(build_listener_socket_with_buffers(
                &spec,
                self.socket_buffers,
            ));
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
            let h = sockets.add(build_listener_socket_with_buffers(
                &spec,
                self.socket_buffers,
            ));
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DownlinkBackpressureConfig {
    high_bytes: usize,
    low_bytes: usize,
}

impl Default for DownlinkBackpressureConfig {
    fn default() -> Self {
        Self {
            high_bytes: DEFAULT_DOWNLINK_BACKPRESSURE_HIGH_BYTES,
            low_bytes: DEFAULT_DOWNLINK_BACKPRESSURE_LOW_BYTES,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BufferedDownlinkConfig {
    enabled: bool,
    per_flow_low_bytes: usize,
    per_flow_high_bytes: usize,
    per_flow_hard_bytes: usize,
    global_low_bytes: usize,
    global_high_bytes: usize,
    global_hard_bytes: usize,
}

impl BufferedDownlinkConfig {
    fn disabled(backpressure: DownlinkBackpressureConfig, pool_size: usize) -> Self {
        Self::from_backpressure(false, backpressure, pool_size)
    }

    fn from_backpressure(
        enabled: bool,
        backpressure: DownlinkBackpressureConfig,
        pool_size: usize,
    ) -> Self {
        let per_flow_low_bytes = backpressure.high_bytes.max(1);
        let per_flow_high_bytes = backpressure
            .high_bytes
            .saturating_mul(BUFFERED_DOWNLINK_PER_FLOW_HIGH_MULTIPLIER)
            .max(per_flow_low_bytes.saturating_add(1))
            .min(MAX_TCP_SOCKET_BUFFER_BYTES);
        let per_flow_hard_bytes = backpressure
            .high_bytes
            .saturating_mul(BUFFERED_DOWNLINK_PER_FLOW_HARD_MULTIPLIER)
            .max(per_flow_high_bytes.saturating_add(1))
            .min(MAX_TCP_SOCKET_BUFFER_BYTES);
        let global_scale = pool_size.clamp(1, BUFFERED_DOWNLINK_GLOBAL_FLOW_SCALE_MAX);
        let global_low_bytes = per_flow_low_bytes
            .saturating_mul(global_scale)
            .max(per_flow_low_bytes)
            .min(MAX_TCP_SOCKET_BUFFER_BYTES);
        let global_high_bytes = per_flow_high_bytes
            .saturating_mul(global_scale)
            .max(per_flow_high_bytes)
            .min(MAX_TCP_SOCKET_BUFFER_BYTES);
        let global_hard_bytes = per_flow_hard_bytes
            .saturating_mul(global_scale)
            .max(per_flow_hard_bytes)
            .min(MAX_TCP_SOCKET_BUFFER_BYTES);
        Self {
            enabled,
            per_flow_low_bytes,
            per_flow_high_bytes,
            per_flow_hard_bytes,
            global_low_bytes,
            global_high_bytes,
            global_hard_bytes,
        }
    }
}

fn tun_egress_capacity_bytes(tun_mtu: usize, tun_tx_queue_len: usize) -> usize {
    tun_mtu.max(1).saturating_mul(tun_tx_queue_len.max(1))
}

fn tun_egress_safe_flush_threshold(tun_mtu: usize, tun_tx_queue_len: usize) -> usize {
    tun_egress_capacity_bytes(tun_mtu, tun_tx_queue_len)
        .saturating_mul(TUN_EGRESS_SAFE_FLUSH_NUMERATOR)
        / TUN_EGRESS_SAFE_FLUSH_DENOMINATOR
}

fn low_watermark_for_high(high_bytes: usize) -> usize {
    (high_bytes / DEFAULT_DOWNLINK_BACKPRESSURE_LOW_RATIO)
        .max(1)
        .min(high_bytes.saturating_sub(1).max(1))
}

fn default_downlink_backpressure_for_tun_egress(
    tun_mtu: usize,
    tun_tx_queue_len: usize,
) -> DownlinkBackpressureConfig {
    let default = DownlinkBackpressureConfig::default();
    let safe_flush = tun_egress_safe_flush_threshold(tun_mtu, tun_tx_queue_len);
    let capacity = tun_egress_capacity_bytes(tun_mtu, tun_tx_queue_len);
    let high_by_flush = safe_flush.saturating_mul(TX_QUEUE_FLUSH_RATIO_DENOMINATOR)
        / TX_QUEUE_FLUSH_RATIO_NUMERATOR;
    let high_by_pause =
        capacity.saturating_mul(TX_QUEUE_PAUSE_RATIO_DENOMINATOR) / TX_QUEUE_PAUSE_RATIO_NUMERATOR;
    let high_bytes = default
        .high_bytes
        .min(high_by_flush)
        .min(high_by_pause)
        .max(2);
    DownlinkBackpressureConfig {
        high_bytes,
        low_bytes: low_watermark_for_high(high_bytes),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TcpSocketBufferConfig {
    rx_bytes: usize,
    tx_bytes: usize,
}

impl Default for TcpSocketBufferConfig {
    fn default() -> Self {
        Self {
            rx_bytes: TCP_SOCKET_BUFFER_SIZE,
            tx_bytes: TCP_SOCKET_BUFFER_SIZE,
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct DownlinkPressureStats {
    max_pending: usize,
    total_pending: usize,
    max_tx_queue: usize,
    total_tx_queue: usize,
}

impl DownlinkPressureStats {
    #[cfg(test)]
    fn new(
        max_pending: usize,
        total_pending: usize,
        max_tx_queue: usize,
        total_tx_queue: usize,
    ) -> Self {
        Self {
            max_pending,
            total_pending,
            max_tx_queue,
            total_tx_queue,
        }
    }

    #[cfg(test)]
    fn pending_only(max_pending: usize, total_pending: usize) -> Self {
        Self::new(max_pending, total_pending, 0, 0)
    }

    fn note_pending(&mut self, pending: usize) {
        self.max_pending = self.max_pending.max(pending);
        self.total_pending = self.total_pending.saturating_add(pending);
    }

    fn note_tx_queue(&mut self, tx_queue: usize) {
        self.max_tx_queue = self.max_tx_queue.max(tx_queue);
        self.total_tx_queue = self.total_tx_queue.saturating_add(tx_queue);
    }

    fn with_tx_queue_floor(mut self, max_tx_queue: usize, total_tx_queue: usize) -> Self {
        self.max_tx_queue = self.max_tx_queue.max(max_tx_queue);
        self.total_tx_queue = self.total_tx_queue.max(total_tx_queue);
        self
    }

    fn max_pressure(&self) -> usize {
        self.max_pending.max(self.max_tx_queue)
    }

    fn total_pressure(&self) -> usize {
        self.total_pending.saturating_add(self.total_tx_queue)
    }
}

fn tx_queue_pause_threshold(cfg: DownlinkBackpressureConfig) -> usize {
    let headroom = cfg.high_bytes.saturating_sub(cfg.low_bytes);
    cfg.high_bytes.saturating_add(headroom)
}

fn tx_queue_resume_threshold(cfg: DownlinkBackpressureConfig) -> usize {
    cfg.high_bytes
}

fn tx_queue_flush_threshold(cfg: DownlinkBackpressureConfig) -> usize {
    let headroom = cfg.high_bytes.saturating_sub(cfg.low_bytes);
    let bounded_flush_headroom = (headroom / 2).max(1);
    cfg.high_bytes.saturating_add(bounded_flush_headroom)
}

fn tx_queue_credit_guard_bytes(cfg: DownlinkBackpressureConfig) -> usize {
    let credit_span = tx_queue_pause_threshold(cfg).saturating_sub(tx_queue_flush_threshold(cfg));
    if credit_span == 0 {
        0
    } else {
        (credit_span / 8).max(1).min(credit_span)
    }
}

fn tx_queue_credit_spend_threshold(cfg: DownlinkBackpressureConfig) -> usize {
    tx_queue_pause_threshold(cfg).saturating_sub(tx_queue_credit_guard_bytes(cfg))
}

fn tx_queue_egress_target_threshold(cfg: DownlinkBackpressureConfig) -> usize {
    let flush = tx_queue_flush_threshold(cfg);
    let credit_edge = tx_queue_credit_spend_threshold(cfg);
    flush.saturating_add(credit_edge.saturating_sub(flush) / 2)
}

fn reaches_downlink_pressure_hold_threshold(
    stats: DownlinkPressureStats,
    cfg: DownlinkBackpressureConfig,
) -> bool {
    stats.max_pending >= cfg.high_bytes || stats.max_tx_queue >= tx_queue_pause_threshold(cfg)
}

fn reaches_downlink_credit_debt_pressure_threshold(
    stats: DownlinkPressureStats,
    cfg: DownlinkBackpressureConfig,
) -> bool {
    stats.max_tx_queue >= tx_queue_credit_spend_threshold(cfg)
        || (stats.max_pending >= cfg.high_bytes
            && stats.max_tx_queue >= tx_queue_flush_threshold(cfg))
}

fn should_install_downlink_pressure_credit_debt(
    was_paused: bool,
    _is_paused: bool,
    raw_stats: DownlinkPressureStats,
    cfg: DownlinkBackpressureConfig,
) -> bool {
    !was_paused && reaches_downlink_credit_debt_pressure_threshold(raw_stats, cfg)
}

fn downlink_receive_window_high(cfg: DownlinkBackpressureConfig) -> usize {
    cfg.high_bytes
        .saturating_mul(4)
        .max(cfg.high_bytes)
        .min(MAX_TCP_SOCKET_BUFFER_BYTES)
}

fn downlink_receive_window_low(cfg: DownlinkBackpressureConfig) -> usize {
    cfg.high_bytes.min(downlink_receive_window_high(cfg))
}

fn downlink_receive_window_total_high(cfg: DownlinkBackpressureConfig) -> usize {
    downlink_receive_window_high(cfg)
        .saturating_mul(DEFAULT_TUN_POOL_SIZE)
        .max(downlink_receive_window_high(cfg))
        .min(MAX_TCP_SOCKET_BUFFER_BYTES)
}

fn downlink_receive_window_total_low(cfg: DownlinkBackpressureConfig) -> usize {
    downlink_receive_window_low(cfg)
        .saturating_mul(DEFAULT_TUN_POOL_SIZE)
        .max(downlink_receive_window_low(cfg))
        .min(downlink_receive_window_total_high(cfg))
}

fn next_global_rx_receive_backpressure(
    was_paused: bool,
    stats: DownlinkPressureStats,
    cfg: DownlinkBackpressureConfig,
) -> bool {
    if was_paused {
        return stats.max_pending > downlink_receive_window_low(cfg)
            || stats.total_pending > downlink_receive_window_total_low(cfg);
    }

    stats.max_pending >= downlink_receive_window_high(cfg)
        || stats.total_pending >= downlink_receive_window_total_high(cfg)
}

#[cfg(test)]
fn downlink_pending_stats<'a>(
    ctxs: impl IntoIterator<Item = &'a SocketCtx>,
) -> DownlinkPressureStats {
    let mut stats = DownlinkPressureStats::default();
    for ctx in ctxs {
        stats.note_pending(ctx.downlink_pending.len());
    }
    stats
}

fn downlink_pressure_stats(
    dirty: &HashSet<SocketHandle>,
    socket_ctxs: &HashMap<SocketHandle, SocketCtx>,
    sockets: &SocketSet<'_>,
) -> DownlinkPressureStats {
    let mut stats = DownlinkPressureStats::default();
    for handle in dirty {
        let Some(ctx) = socket_ctxs.get(handle) else {
            continue;
        };
        stats.note_pending(ctx.downlink_pending.len());
    }
    for (handle, socket) in sockets.iter() {
        if !dirty.contains(&handle) || !socket_ctxs.contains_key(&handle) {
            continue;
        }
        if let smoltcp::socket::Socket::Tcp(tcp_socket) = socket {
            stats.note_tx_queue(tcp_socket.send_queue());
        }
    }
    stats
}

#[allow(clippy::too_many_arguments)]
fn publish_relay_read_credit_for_handle(
    handle: SocketHandle,
    sockets: &SocketSet<'_>,
    socket_ctxs: &mut HashMap<SocketHandle, SocketCtx>,
    cfg: DownlinkBackpressureConfig,
    tun_mtu: usize,
    downlink_flush_max_bytes: usize,
    egress_recovery_active: bool,
    hard_pause_active: bool,
    buffered_downlink: BufferedDownlinkConfig,
    global_pending_bytes: usize,
) {
    let snapshot = {
        let socket = sockets.get::<TcpSocket>(handle);
        SocketCloseSnapshot::from_socket(socket)
    };
    let Some(ctx) = socket_ctxs.get_mut(&handle) else {
        return;
    };
    let Some(tx) = ctx.relay_read_credit_tx.as_ref() else {
        return;
    };
    let pending_bytes = ctx.downlink_pending.len();
    if let Some(credit) =
        relay_read_credit_for_terminal_no_recv_window(snapshot, pending_bytes, cfg, tun_mtu)
    {
        if *tx.borrow() != credit {
            let _ = tx.send(credit);
        }
        return;
    }
    if buffered_downlink.enabled {
        let global_pending_bytes = global_pending_bytes.max(pending_bytes);
        let decision = buffered_downlink_read_credit(
            tx.borrow().paused,
            pending_bytes,
            global_pending_bytes,
            false,
            hard_pause_active,
            buffered_downlink,
        );
        if *tx.borrow() != decision.credit {
            let _ = tx.send(decision.credit);
            tcp_diag_log!(
                "{}",
                format_buffered_downlink_read_credit_diag(
                    handle,
                    ctx.conn_epoch,
                    decision,
                    pending_bytes,
                    global_pending_bytes,
                    buffered_downlink,
                )
            );
        }
        return;
    }
    let local_pressure_bytes = snapshot.send_queue.saturating_add(pending_bytes);
    let pressure_ack_drain_bytes = ctx
        .downlink_credit_controller
        .consume_ack_cadence_boost_for_publish(
            local_pressure_bytes,
            hard_pause_active,
            cfg,
            tun_mtu,
        );
    let credit = relay_read_credit_for_local_egress_with_ack_drain_floor(
        snapshot.send_queue,
        pending_bytes,
        egress_recovery_active,
        hard_pause_active,
        cfg,
        tun_mtu,
        pressure_ack_drain_bytes,
        downlink_flush_max_bytes,
    );
    let credit = ctx
        .downlink_credit_controller
        .read_credit_for_local_pressure(credit, pending_bytes, local_pressure_bytes, cfg, tun_mtu);
    let progress_generation = ctx.downlink_credit_controller.egress_progress_generation;
    let progress_wake = progress_generation != ctx.relay_read_credit_progress_generation_sent
        && !credit.paused
        && credit.max_batch_bytes > 0;
    let changed = *tx.borrow() != credit;
    if changed || progress_wake {
        match tx.send(credit) {
            Ok(()) if progress_wake => {
                ctx.relay_read_credit_progress_generation_sent = progress_generation;
            }
            _ => {}
        }
    }
}

fn note_ack_cadence_gap_hint_for_handle(
    handle: SocketHandle,
    epoch: u64,
    sockets: &SocketSet<'_>,
    socket_ctxs: &mut HashMap<SocketHandle, SocketCtx>,
    cfg: DownlinkBackpressureConfig,
    tun_mtu: usize,
    hard_pause_active: bool,
) -> Option<usize> {
    let snapshot = {
        let socket = sockets.get::<TcpSocket>(handle);
        SocketCloseSnapshot::from_socket(socket)
    };
    let ctx = socket_ctxs.get_mut(&handle)?;
    if ctx.conn_epoch != epoch || !matches!(ctx.state, SocketState::Relaying | SocketState::Closing)
    {
        return None;
    }
    let local_pressure_bytes = snapshot
        .send_queue
        .saturating_add(ctx.downlink_pending.len());
    ctx.downlink_credit_controller.note_ack_cadence_gap_hint(
        local_pressure_bytes,
        hard_pause_active,
        cfg,
        tun_mtu,
    );
    Some(ctx.downlink_credit_controller.adaptive_ack_drain_floor_for(
        local_pressure_bytes,
        cfg,
        tun_mtu,
    ))
}

#[allow(clippy::too_many_arguments)]
fn publish_projected_relay_read_credit_for_payload(
    handle: SocketHandle,
    epoch: u64,
    incoming_bytes: usize,
    sockets: &SocketSet<'_>,
    socket_ctxs: &mut HashMap<SocketHandle, SocketCtx>,
    cfg: DownlinkBackpressureConfig,
    tun_mtu: usize,
    downlink_flush_max_bytes: usize,
    egress_recovery_active: bool,
    hard_pause_active: bool,
) {
    if incoming_bytes == 0 {
        return;
    }
    let snapshot = {
        let socket = sockets.get::<TcpSocket>(handle);
        SocketCloseSnapshot::from_socket(socket)
    };
    let Some(ctx) = socket_ctxs.get_mut(&handle) else {
        return;
    };
    if ctx.conn_epoch != epoch
        || remote_payload_disposition(ctx, snapshot) != RemotePayloadDisposition::Accept
    {
        return;
    }
    let Some(tx) = ctx.relay_read_credit_tx.as_ref() else {
        return;
    };
    let pending_bytes = ctx.downlink_pending.len();
    let projected_pending = pending_bytes.saturating_add(incoming_bytes);
    if let Some(credit) =
        relay_read_credit_for_terminal_no_recv_window(snapshot, projected_pending, cfg, tun_mtu)
    {
        if *tx.borrow() != credit {
            let _ = tx.send(credit);
        }
        return;
    }
    let projected_pressure_bytes = snapshot
        .send_queue
        .saturating_add(pending_bytes)
        .saturating_add(incoming_bytes);
    let pressure_ack_drain_bytes = ctx.downlink_credit_controller.adaptive_ack_drain_floor_for(
        projected_pressure_bytes,
        cfg,
        tun_mtu,
    );
    ctx.downlink_credit_controller
        .note_payload_read_for_projected_pressure(incoming_bytes, projected_pressure_bytes, cfg);
    let credit = relay_read_credit_for_projected_local_egress_with_ack_drain_floor(
        snapshot.send_queue,
        pending_bytes,
        incoming_bytes,
        egress_recovery_active,
        hard_pause_active,
        cfg,
        tun_mtu,
        pressure_ack_drain_bytes,
        downlink_flush_max_bytes,
    );
    let credit = ctx
        .downlink_credit_controller
        .read_credit_for_local_pressure(
            credit,
            projected_pending,
            projected_pressure_bytes,
            cfg,
            tun_mtu,
        );
    let progress_generation = ctx.downlink_credit_controller.egress_progress_generation;
    let progress_wake = progress_generation != ctx.relay_read_credit_progress_generation_sent
        && !credit.paused
        && credit.max_batch_bytes > 0;
    if *tx.borrow() != credit || progress_wake {
        match tx.send(credit) {
            Ok(()) if progress_wake => {
                ctx.relay_read_credit_progress_generation_sent = progress_generation;
            }
            _ => {}
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn publish_relay_read_credit_for_dirty_handles(
    dirty: &HashSet<SocketHandle>,
    sockets: &SocketSet<'_>,
    socket_ctxs: &mut HashMap<SocketHandle, SocketCtx>,
    cfg: DownlinkBackpressureConfig,
    tun_mtu: usize,
    downlink_flush_max_bytes: usize,
    egress_recovery_active: bool,
    hard_pause_active: bool,
    buffered_downlink: BufferedDownlinkConfig,
    global_pending_bytes: usize,
) {
    let handles: Vec<SocketHandle> = dirty.iter().copied().collect();
    for handle in handles {
        publish_relay_read_credit_for_handle(
            handle,
            sockets,
            socket_ctxs,
            cfg,
            tun_mtu,
            downlink_flush_max_bytes,
            egress_recovery_active,
            hard_pause_active,
            buffered_downlink,
            global_pending_bytes,
        );
    }
}

fn has_dirty_close_drain(
    dirty: &HashSet<SocketHandle>,
    socket_ctxs: &HashMap<SocketHandle, SocketCtx>,
    cfg: DownlinkBackpressureConfig,
) -> bool {
    dirty.iter().any(|handle| {
        let Some(ctx) = socket_ctxs.get(handle) else {
            return false;
        };
        ctx.pending_relay_close.is_some()
            && (!ctx.downlink_pending.is_empty()
                || ctx.pending_relay_close_last_egress_queue_bytes > cfg.low_bytes)
    })
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct TcpDownlinkAggregate {
    pending_total: usize,
    pending_max: usize,
    pending_high: usize,
    remote_to_global_rx_bytes: u64,
    terminal_late_remote_payload_bytes: u64,
    terminal_late_remote_payload_events: u64,
    flush_attempts: u64,
    send_slice_no_capacity: u64,
    send_slice_calls: u64,
    send_slice_accepted_bytes: u64,
    send_slice_zero: u64,
    send_slice_errors: u64,
    send_slice_budget_limited: u64,
    send_slice_headroom_limited: u64,
    send_slice_headroom_deferred_bytes: u64,
    send_slice_drain_credit_granted_bytes: u64,
    send_slice_drain_credit_planned_bytes: u64,
    send_slice_drain_credit_used_bytes: u64,
    egress_payload_credit_bytes: usize,
    send_slice_drop_credit_debt_bytes: usize,
    send_slice_drop_credit_debt_paid_bytes: u64,
    send_slice_drop_credit_blocked_bytes: u64,
    send_slice_pressure_credit_debt_bytes: usize,
    send_slice_pressure_credit_debt_paid_bytes: u64,
    send_slice_pressure_credit_blocked_bytes: u64,
    send_slice_hard_edge_guard_bytes: usize,
    send_slice_hard_edge_guard_limited: u64,
    send_slice_hard_edge_guard_deferred_bytes: u64,
    send_slice_max_accepted_bytes: usize,
    tun_flush_tx_calls: u64,
    tun_flush_tx_failures: u64,
    tun_flush_deferred: u64,
    send_window_samples: u64,
    send_capacity_min: usize,
    send_capacity_max: usize,
    send_queue_max: usize,
    recv_queue_max: usize,
    may_send_false: u64,
    may_recv_false: u64,
    no_send_capacity_streak_max: u64,
    no_send_capacity_pending_max: usize,
}

fn tcp_downlink_aggregate<'a>(
    ctxs: impl IntoIterator<Item = &'a SocketCtx>,
) -> TcpDownlinkAggregate {
    let mut aggregate = TcpDownlinkAggregate::default();
    for ctx in ctxs {
        let pending = ctx.downlink_pending.len();
        aggregate.pending_total = aggregate.pending_total.saturating_add(pending);
        aggregate.pending_max = aggregate.pending_max.max(pending);
        aggregate.pending_high = aggregate
            .pending_high
            .max(ctx.downlink_diag.downlink_pending_high_water);
        aggregate.remote_to_global_rx_bytes = aggregate
            .remote_to_global_rx_bytes
            .saturating_add(ctx.downlink_diag.remote_to_global_rx_bytes);
        aggregate.terminal_late_remote_payload_bytes = aggregate
            .terminal_late_remote_payload_bytes
            .saturating_add(ctx.downlink_diag.terminal_late_remote_payload_bytes);
        aggregate.terminal_late_remote_payload_events = aggregate
            .terminal_late_remote_payload_events
            .saturating_add(ctx.downlink_diag.terminal_late_remote_payload_events);
        aggregate.flush_attempts = aggregate
            .flush_attempts
            .saturating_add(ctx.downlink_diag.flush_attempts);
        aggregate.send_slice_no_capacity = aggregate
            .send_slice_no_capacity
            .saturating_add(ctx.downlink_diag.send_slice_no_capacity);
        aggregate.send_slice_calls = aggregate
            .send_slice_calls
            .saturating_add(ctx.downlink_diag.send_slice_calls);
        aggregate.send_slice_accepted_bytes = aggregate
            .send_slice_accepted_bytes
            .saturating_add(ctx.downlink_diag.send_slice_accepted_bytes);
        aggregate.send_slice_zero = aggregate
            .send_slice_zero
            .saturating_add(ctx.downlink_diag.send_slice_zero);
        aggregate.send_slice_errors = aggregate
            .send_slice_errors
            .saturating_add(ctx.downlink_diag.send_slice_errors);
        aggregate.send_slice_budget_limited = aggregate
            .send_slice_budget_limited
            .saturating_add(ctx.downlink_diag.send_slice_budget_limited);
        aggregate.send_slice_headroom_limited = aggregate
            .send_slice_headroom_limited
            .saturating_add(ctx.downlink_diag.send_slice_headroom_limited);
        aggregate.send_slice_headroom_deferred_bytes = aggregate
            .send_slice_headroom_deferred_bytes
            .saturating_add(ctx.downlink_diag.send_slice_headroom_deferred_bytes);
        aggregate.send_slice_drain_credit_granted_bytes = aggregate
            .send_slice_drain_credit_granted_bytes
            .saturating_add(ctx.downlink_diag.send_slice_drain_credit_granted_bytes);
        aggregate.send_slice_drain_credit_planned_bytes = aggregate
            .send_slice_drain_credit_planned_bytes
            .saturating_add(ctx.downlink_diag.send_slice_drain_credit_planned_bytes);
        aggregate.send_slice_drain_credit_used_bytes = aggregate
            .send_slice_drain_credit_used_bytes
            .saturating_add(ctx.downlink_diag.send_slice_drain_credit_used_bytes);
        aggregate.egress_payload_credit_bytes = aggregate
            .egress_payload_credit_bytes
            .max(ctx.downlink_credit_controller.egress_payload_credit_bytes);
        aggregate.send_slice_drop_credit_debt_bytes = aggregate
            .send_slice_drop_credit_debt_bytes
            .max(ctx.downlink_diag.send_slice_drop_credit_debt_bytes);
        aggregate.send_slice_drop_credit_debt_paid_bytes = aggregate
            .send_slice_drop_credit_debt_paid_bytes
            .saturating_add(ctx.downlink_diag.send_slice_drop_credit_debt_paid_bytes);
        aggregate.send_slice_drop_credit_blocked_bytes = aggregate
            .send_slice_drop_credit_blocked_bytes
            .saturating_add(ctx.downlink_diag.send_slice_drop_credit_blocked_bytes);
        aggregate.send_slice_pressure_credit_debt_bytes = aggregate
            .send_slice_pressure_credit_debt_bytes
            .max(ctx.downlink_diag.send_slice_pressure_credit_debt_bytes);
        aggregate.send_slice_pressure_credit_debt_paid_bytes = aggregate
            .send_slice_pressure_credit_debt_paid_bytes
            .saturating_add(ctx.downlink_diag.send_slice_pressure_credit_debt_paid_bytes);
        aggregate.send_slice_pressure_credit_blocked_bytes = aggregate
            .send_slice_pressure_credit_blocked_bytes
            .saturating_add(ctx.downlink_diag.send_slice_pressure_credit_blocked_bytes);
        aggregate.send_slice_hard_edge_guard_bytes = aggregate
            .send_slice_hard_edge_guard_bytes
            .max(ctx.downlink_diag.send_slice_hard_edge_guard_bytes);
        aggregate.send_slice_hard_edge_guard_limited = aggregate
            .send_slice_hard_edge_guard_limited
            .saturating_add(ctx.downlink_diag.send_slice_hard_edge_guard_limited);
        aggregate.send_slice_hard_edge_guard_deferred_bytes = aggregate
            .send_slice_hard_edge_guard_deferred_bytes
            .saturating_add(ctx.downlink_diag.send_slice_hard_edge_guard_deferred_bytes);
        aggregate.send_slice_max_accepted_bytes = aggregate
            .send_slice_max_accepted_bytes
            .max(ctx.downlink_diag.send_slice_max_accepted_bytes);
        aggregate.tun_flush_tx_calls = aggregate
            .tun_flush_tx_calls
            .saturating_add(ctx.downlink_diag.tun_flush_tx_calls);
        aggregate.tun_flush_tx_failures = aggregate
            .tun_flush_tx_failures
            .saturating_add(ctx.downlink_diag.tun_flush_tx_failures);
        aggregate.tun_flush_deferred = aggregate
            .tun_flush_deferred
            .saturating_add(ctx.downlink_diag.tun_flush_deferred);
        if ctx.downlink_diag.send_window_samples > 0 {
            if aggregate.send_window_samples == 0 {
                aggregate.send_capacity_min = ctx.downlink_diag.send_capacity_min;
            } else {
                aggregate.send_capacity_min = aggregate
                    .send_capacity_min
                    .min(ctx.downlink_diag.send_capacity_min);
            }
            aggregate.send_capacity_max = aggregate
                .send_capacity_max
                .max(ctx.downlink_diag.send_capacity_max);
        }
        aggregate.send_window_samples = aggregate
            .send_window_samples
            .saturating_add(ctx.downlink_diag.send_window_samples);
        aggregate.send_queue_max = aggregate
            .send_queue_max
            .max(ctx.downlink_diag.send_queue_max);
        aggregate.recv_queue_max = aggregate
            .recv_queue_max
            .max(ctx.downlink_diag.recv_queue_max);
        aggregate.may_send_false = aggregate
            .may_send_false
            .saturating_add(ctx.downlink_diag.may_send_false);
        aggregate.may_recv_false = aggregate
            .may_recv_false
            .saturating_add(ctx.downlink_diag.may_recv_false);
        aggregate.no_send_capacity_streak_max = aggregate
            .no_send_capacity_streak_max
            .max(ctx.downlink_diag.no_send_capacity_streak_max);
        aggregate.no_send_capacity_pending_max = aggregate
            .no_send_capacity_pending_max
            .max(ctx.downlink_diag.no_send_capacity_pending_max);
    }
    aggregate
}

fn format_tcp_downlink_flush_diag(
    aggregate: &TcpDownlinkAggregate,
    dirty_handles: usize,
) -> String {
    format!(
        "🔎 tcp-downlink-flush pending_total={} pending_max={} pending_high={} remote_to_global_rx_bytes={} terminal_late_remote_payload_bytes={} terminal_late_remote_payload_events={} flush_attempts={} no_send_capacity={} send_window_samples={} send_capacity_min={} send_capacity_max={} send_queue_max={} recv_queue_max={} may_send_false={} may_recv_false={} no_send_capacity_streak_max={} no_send_capacity_pending_max={} send_slice_calls={} send_slice_accepted={} send_slice_zero={} send_slice_errors={} budget_limited_calls={} headroom_limited_calls={} headroom_deferred_bytes={} drain_credit_granted_bytes={} drain_credit_planned_bytes={} drain_credit_used_bytes={} egress_payload_credit_bytes={} drop_credit_debt_bytes={} drop_credit_debt_paid_bytes={} drop_credit_blocked_bytes={} pressure_credit_debt_bytes={} pressure_credit_debt_paid_bytes={} pressure_credit_blocked_bytes={} hard_edge_guard_bytes={} hard_edge_guard_limited_calls={} hard_edge_guard_deferred_bytes={} send_slice_max_accepted={} tun_flush_tx_calls={} tun_flush_tx_failures={} tun_flush_deferred={} dirty_handles={}",
        aggregate.pending_total,
        aggregate.pending_max,
        aggregate.pending_high,
        aggregate.remote_to_global_rx_bytes,
        aggregate.terminal_late_remote_payload_bytes,
        aggregate.terminal_late_remote_payload_events,
        aggregate.flush_attempts,
        aggregate.send_slice_no_capacity,
        aggregate.send_window_samples,
        aggregate.send_capacity_min,
        aggregate.send_capacity_max,
        aggregate.send_queue_max,
        aggregate.recv_queue_max,
        aggregate.may_send_false,
        aggregate.may_recv_false,
        aggregate.no_send_capacity_streak_max,
        aggregate.no_send_capacity_pending_max,
        aggregate.send_slice_calls,
        aggregate.send_slice_accepted_bytes,
        aggregate.send_slice_zero,
        aggregate.send_slice_errors,
        aggregate.send_slice_budget_limited,
        aggregate.send_slice_headroom_limited,
        aggregate.send_slice_headroom_deferred_bytes,
        aggregate.send_slice_drain_credit_granted_bytes,
        aggregate.send_slice_drain_credit_planned_bytes,
        aggregate.send_slice_drain_credit_used_bytes,
        aggregate.egress_payload_credit_bytes,
        aggregate.send_slice_drop_credit_debt_bytes,
        aggregate.send_slice_drop_credit_debt_paid_bytes,
        aggregate.send_slice_drop_credit_blocked_bytes,
        aggregate.send_slice_pressure_credit_debt_bytes,
        aggregate.send_slice_pressure_credit_debt_paid_bytes,
        aggregate.send_slice_pressure_credit_blocked_bytes,
        aggregate.send_slice_hard_edge_guard_bytes,
        aggregate.send_slice_hard_edge_guard_limited,
        aggregate.send_slice_hard_edge_guard_deferred_bytes,
        aggregate.send_slice_max_accepted_bytes,
        aggregate.tun_flush_tx_calls,
        aggregate.tun_flush_tx_failures,
        aggregate.tun_flush_deferred,
        dirty_handles
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TunEgressDropSample {
    Unavailable { reason: &'static str },
    First { total: u64 },
    Delta { total: u64, delta: u64 },
    Reset { previous: u64, total: u64 },
}

#[derive(Debug, Clone)]
struct TunEgressDropSampler {
    interface_name: Option<String>,
    last_tx_dropped: Option<u64>,
}

impl TunEgressDropSampler {
    fn new(interface_name: Option<&str>) -> Self {
        Self {
            interface_name: interface_name.map(str::to_owned),
            last_tx_dropped: None,
        }
    }

    fn sample(&mut self) -> TunEgressDropSample {
        self.sample_with(read_tun_tx_dropped_stat)
    }

    fn sample_with<F>(&mut self, read_stat: F) -> TunEgressDropSample
    where
        F: FnOnce(&str) -> std::io::Result<String>,
    {
        let Some(interface_name) = self.interface_name.as_deref() else {
            return TunEgressDropSample::Unavailable {
                reason: "no_interface",
            };
        };
        if !valid_sysfs_interface_name(interface_name) {
            return TunEgressDropSample::Unavailable {
                reason: "invalid_interface",
            };
        }
        let Ok(raw) = read_stat(interface_name) else {
            return TunEgressDropSample::Unavailable {
                reason: "read_error",
            };
        };
        let Some(total) = parse_u64_counter(raw.trim()) else {
            return TunEgressDropSample::Unavailable {
                reason: "parse_error",
            };
        };
        match self.last_tx_dropped.replace(total) {
            None => TunEgressDropSample::First { total },
            Some(previous) if total >= previous => TunEgressDropSample::Delta {
                total,
                delta: total - previous,
            },
            Some(previous) => TunEgressDropSample::Reset { previous, total },
        }
    }

    fn interface_label(&self) -> &str {
        self.interface_name.as_deref().unwrap_or("unknown")
    }
}

fn valid_sysfs_interface_name(interface_name: &str) -> bool {
    !interface_name.is_empty()
        && interface_name != "."
        && interface_name != ".."
        && !interface_name.contains('/')
        && !interface_name.contains('\0')
}

fn parse_u64_counter(raw: &str) -> Option<u64> {
    raw.trim().parse::<u64>().ok()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TunEgressFeedbackReason {
    DropDelta,
    PressureLow,
}

impl TunEgressFeedbackReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::DropDelta => "drop_delta",
            Self::PressureLow => "pressure_low",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TunEgressFeedbackEvent {
    paused: bool,
    reason: TunEgressFeedbackReason,
    tx_dropped_delta: u64,
    max_pressure: usize,
    total_pressure: usize,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct TunEgressFeedbackState {
    paused: bool,
    drop_events: u64,
    drop_delta_total: u64,
    max_drop_delta: u64,
    pause_edges: u64,
    resume_edges: u64,
    recent_max_pressure: usize,
    recent_total_pressure: usize,
    egress_hold_until: Option<std::time::Instant>,
    egress_hold_max_pressure: usize,
    egress_hold_total_pressure: usize,
}

impl TunEgressFeedbackState {
    fn is_paused(&self) -> bool {
        self.paused
    }

    #[cfg(test)]
    fn observe_pressure(&mut self, stats: DownlinkPressureStats, cfg: DownlinkBackpressureConfig) {
        self.observe_pressure_at(stats, cfg, std::time::Instant::now());
    }

    fn observe_pressure_at(
        &mut self,
        stats: DownlinkPressureStats,
        cfg: DownlinkBackpressureConfig,
        now: std::time::Instant,
    ) {
        self.expire_egress_pressure_hold(now);
        if stats.max_pressure() < cfg.high_bytes {
            return;
        }
        self.recent_max_pressure = self.recent_max_pressure.max(stats.max_pressure());
        self.recent_total_pressure = self.recent_total_pressure.max(stats.total_pressure());
        if !reaches_downlink_pressure_hold_threshold(stats, cfg) {
            return;
        }
        self.egress_hold_max_pressure = self.egress_hold_max_pressure.max(stats.max_pressure());
        self.egress_hold_total_pressure =
            self.egress_hold_total_pressure.max(stats.total_pressure());
        self.egress_hold_until =
            Some(now + std::time::Duration::from_millis(TUN_EGRESS_PRESSURE_HOLD_MS));
    }

    fn effective_downlink_pressure_at(
        &mut self,
        stats: DownlinkPressureStats,
        now: std::time::Instant,
    ) -> DownlinkPressureStats {
        self.expire_egress_pressure_hold(now);
        if self.egress_hold_until.is_none() {
            return stats;
        }
        stats.with_tx_queue_floor(
            self.egress_hold_max_pressure,
            self.egress_hold_total_pressure,
        )
    }

    fn expire_egress_pressure_hold(&mut self, now: std::time::Instant) {
        if !matches!(self.egress_hold_until, Some(until) if now >= until) {
            return;
        }
        self.egress_hold_until = None;
        self.egress_hold_max_pressure = 0;
        self.egress_hold_total_pressure = 0;
    }

    fn take_recent_pressure(&mut self) -> DownlinkPressureStats {
        let stats = DownlinkPressureStats {
            max_pending: 0,
            total_pending: 0,
            max_tx_queue: self.recent_max_pressure,
            total_tx_queue: self.recent_total_pressure,
        };
        self.recent_max_pressure = 0;
        self.recent_total_pressure = 0;
        stats
    }

    fn update(
        &mut self,
        sample: &TunEgressDropSample,
        stats: DownlinkPressureStats,
        cfg: DownlinkBackpressureConfig,
    ) -> Option<TunEgressFeedbackEvent> {
        let recent_stats = self.take_recent_pressure();
        let drop_delta = match sample {
            TunEgressDropSample::Delta { delta, .. } => *delta,
            _ => 0,
        };
        if drop_delta > 0 {
            self.drop_events = self.drop_events.saturating_add(1);
            self.drop_delta_total = self.drop_delta_total.saturating_add(drop_delta);
            self.max_drop_delta = self.max_drop_delta.max(drop_delta);
        }

        let max_pressure = stats.max_pressure();
        let total_pressure = stats.total_pressure();
        let drop_max_pressure = max_pressure.max(recent_stats.max_pressure());
        let drop_total_pressure = total_pressure.max(recent_stats.total_pressure());
        if drop_delta > 0 && drop_total_pressure > 0 {
            if !self.paused {
                self.paused = true;
                self.pause_edges = self.pause_edges.saturating_add(1);
            }
            return Some(TunEgressFeedbackEvent {
                paused: true,
                reason: TunEgressFeedbackReason::DropDelta,
                tx_dropped_delta: drop_delta,
                max_pressure: drop_max_pressure,
                total_pressure: drop_total_pressure,
            });
        }

        if self.paused && max_pressure <= cfg.low_bytes {
            self.paused = false;
            self.resume_edges = self.resume_edges.saturating_add(1);
            return Some(TunEgressFeedbackEvent {
                paused: false,
                reason: TunEgressFeedbackReason::PressureLow,
                tx_dropped_delta: drop_delta,
                max_pressure,
                total_pressure,
            });
        }

        None
    }
}

fn format_tun_egress_feedback_diag(
    event: TunEgressFeedbackEvent,
    state: &TunEgressFeedbackState,
    cfg: DownlinkBackpressureConfig,
    drop_debt: DownlinkEgressDropDebt,
) -> String {
    format!(
        "🔎 tcp-tun-egress-feedback paused={} reason={} tx_dropped_delta={} max_pressure={} total_pressure={} high={} low={} drop_events={} drop_delta_total={} max_delta={} pause_edges={} resume_edges={} egress_credit_generation={} egress_credit_debt_bytes={} drop_credit_debt_bytes={} pressure_credit_debt_bytes={} drop_credit_events={} pressure_credit_events={}",
        event.paused,
        event.reason.as_str(),
        event.tx_dropped_delta,
        event.max_pressure,
        event.total_pressure,
        cfg.high_bytes,
        cfg.low_bytes,
        state.drop_events,
        state.drop_delta_total,
        state.max_drop_delta,
        state.pause_edges,
        state.resume_edges,
        drop_debt.generation,
        drop_debt.debt_bytes,
        drop_debt.drop_debt_bytes,
        drop_debt.pressure_debt_bytes,
        drop_debt.drop_events,
        drop_debt.pressure_events
    )
}

fn format_downlink_egress_credit_debt_diag(
    reason: &str,
    installed_bytes: usize,
    stats: DownlinkPressureStats,
    cfg: DownlinkBackpressureConfig,
    credit_debt: DownlinkEgressCreditDebt,
) -> String {
    format!(
        "🔎 tcp-egress-credit-debt reason={} installed_bytes={} generation={} debt_bytes={} drop_credit_debt_bytes={} pressure_credit_debt_bytes={} drop_credit_events={} pressure_credit_events={} max_pending={} max_tx_queue={} max_pressure={} tx_queue_flush_high={} tx_queue_credit_high={} tx_queue_pause_high={}",
        reason,
        installed_bytes,
        credit_debt.generation,
        credit_debt.debt_bytes,
        credit_debt.drop_debt_bytes,
        credit_debt.pressure_debt_bytes,
        credit_debt.drop_events,
        credit_debt.pressure_events,
        stats.max_pending,
        stats.max_tx_queue,
        stats.max_pressure(),
        tx_queue_flush_threshold(cfg),
        tx_queue_credit_spend_threshold(cfg),
        tx_queue_pause_threshold(cfg),
    )
}

#[cfg(target_os = "linux")]
fn read_tun_tx_dropped_stat(interface_name: &str) -> std::io::Result<String> {
    std::fs::read_to_string(format!(
        "/sys/class/net/{interface_name}/statistics/tx_dropped"
    ))
}

#[cfg(not(target_os = "linux"))]
fn read_tun_tx_dropped_stat(_interface_name: &str) -> std::io::Result<String> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "TUN tx_dropped sysfs counter is Linux-only",
    ))
}

fn format_tun_egress_diag(
    interface_name: &str,
    sample: &TunEgressDropSample,
    downlink: &TcpDownlinkAggregate,
    dirty_handles: usize,
    global_rx_paused: bool,
) -> String {
    let (status, total, delta) = match sample {
        TunEgressDropSample::Unavailable { reason } => {
            (*reason, "unknown".to_string(), "unknown".to_string())
        }
        TunEgressDropSample::First { total } => ("first", total.to_string(), "0".to_string()),
        TunEgressDropSample::Delta { total, delta } => {
            ("delta", total.to_string(), delta.to_string())
        }
        TunEgressDropSample::Reset { previous, total } => (
            "reset",
            total.to_string(),
            format!("unknown_previous_{previous}"),
        ),
    };
    format!(
        "🔎 tcp-tun-egress if={} status={} tx_dropped_total={} tx_dropped_delta={} global_rx_paused={} pending_total={} pending_max={} pending_high={} remote_to_global_rx_bytes={} tun_flush_tx_calls={} dirty_handles={}",
        interface_name,
        status,
        total,
        delta,
        global_rx_paused,
        downlink.pending_total,
        downlink.pending_max,
        downlink.pending_high,
        downlink.remote_to_global_rx_bytes,
        downlink.tun_flush_tx_calls,
        dirty_handles
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TunRxPacketKind {
    Tcp,
    Dns,
    Udp,
}

#[derive(Debug, Default, Clone)]
struct TunRxDrainDiag {
    attempts: u64,
    pre_payload_attempts: u64,
    remote_payload_attempts: u64,
    remote_payload_deferred_attempts: u64,
    remote_payload_deferred_delayed_attempts: u64,
    remote_payload_deferred_pressure_attempts: u64,
    relay_gap_hint_attempts: u64,
    relay_gap_hint_followup_attempts: u64,
    maintenance_attempts: u64,
    timer_active_flow_attempts: u64,
    other_attempts: u64,
    packets: u64,
    tcp_packets: u64,
    dns_packets: u64,
    udp_packets: u64,
    budget_exhausted: u64,
    would_block: u64,
    errors: u64,
}

impl TunRxDrainDiag {
    fn note_attempt(&mut self, source: &str) {
        self.attempts = self.attempts.saturating_add(1);
        match source {
            TUN_RX_DRAIN_SOURCE_REMOTE_PAYLOAD_PRE => {
                self.pre_payload_attempts = self.pre_payload_attempts.saturating_add(1);
            }
            TUN_RX_DRAIN_SOURCE_REMOTE_PAYLOAD => {
                self.remote_payload_attempts = self.remote_payload_attempts.saturating_add(1);
            }
            TUN_RX_DRAIN_SOURCE_REMOTE_PAYLOAD_DEFERRED => {
                self.remote_payload_deferred_attempts =
                    self.remote_payload_deferred_attempts.saturating_add(1);
            }
            TUN_RX_DRAIN_SOURCE_REMOTE_PAYLOAD_DEFERRED_DELAYED => {
                self.remote_payload_deferred_delayed_attempts = self
                    .remote_payload_deferred_delayed_attempts
                    .saturating_add(1);
            }
            TUN_RX_DRAIN_SOURCE_REMOTE_PAYLOAD_DEFERRED_PRESSURE => {
                self.remote_payload_deferred_pressure_attempts = self
                    .remote_payload_deferred_pressure_attempts
                    .saturating_add(1);
            }
            TUN_RX_DRAIN_SOURCE_RELAY_GAP_HINT => {
                self.relay_gap_hint_attempts = self.relay_gap_hint_attempts.saturating_add(1);
            }
            TUN_RX_DRAIN_SOURCE_RELAY_GAP_HINT_FOLLOWUP => {
                self.relay_gap_hint_followup_attempts =
                    self.relay_gap_hint_followup_attempts.saturating_add(1);
            }
            TUN_RX_DRAIN_SOURCE_TIMER_PRESSURE => {
                self.maintenance_attempts = self.maintenance_attempts.saturating_add(1);
            }
            TUN_RX_DRAIN_SOURCE_TIMER_ACTIVE_FLOW => {
                self.timer_active_flow_attempts = self.timer_active_flow_attempts.saturating_add(1);
            }
            _ => {
                self.other_attempts = self.other_attempts.saturating_add(1);
            }
        }
    }

    fn note_packet(&mut self, kind: TunRxPacketKind) {
        self.packets = self.packets.saturating_add(1);
        match kind {
            TunRxPacketKind::Tcp => {
                self.tcp_packets = self.tcp_packets.saturating_add(1);
            }
            TunRxPacketKind::Dns => {
                self.dns_packets = self.dns_packets.saturating_add(1);
            }
            TunRxPacketKind::Udp => {
                self.udp_packets = self.udp_packets.saturating_add(1);
            }
        }
    }

    fn note_budget_exhausted(&mut self) {
        self.budget_exhausted = self.budget_exhausted.saturating_add(1);
    }

    fn note_would_block(&mut self) {
        self.would_block = self.would_block.saturating_add(1);
    }

    fn note_error(&mut self) {
        self.errors = self.errors.saturating_add(1);
    }
}

fn format_tun_rx_drain_diag(diag: &TunRxDrainDiag) -> String {
    format!(
        "🔎 tcp-tun-rx-drain attempts={} pre_payload_attempts={} remote_payload_attempts={} remote_payload_deferred_attempts={} remote_payload_deferred_delayed_attempts={} remote_payload_deferred_pressure_attempts={} relay_gap_hint_attempts={} relay_gap_hint_followup_attempts={} maintenance_attempts={} timer_active_flow_attempts={} other_attempts={} packets={} tcp={} dns={} udp={} budget_exhausted={} would_block={} errors={}",
        diag.attempts,
        diag.pre_payload_attempts,
        diag.remote_payload_attempts,
        diag.remote_payload_deferred_attempts,
        diag.remote_payload_deferred_delayed_attempts,
        diag.remote_payload_deferred_pressure_attempts,
        diag.relay_gap_hint_attempts,
        diag.relay_gap_hint_followup_attempts,
        diag.maintenance_attempts,
        diag.timer_active_flow_attempts,
        diag.other_attempts,
        diag.packets,
        diag.tcp_packets,
        diag.dns_packets,
        diag.udp_packets,
        diag.budget_exhausted,
        diag.would_block,
        diag.errors
    )
}

fn should_drain_tun_rx_after_remote_payload(
    ctx: Option<&SocketCtx>,
    accepted_bytes: usize,
) -> bool {
    accepted_bytes > 0 || ctx.map(|c| !c.downlink_pending.is_empty()).unwrap_or(false)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeferredAckDrainMode {
    ActiveFlow,
    DelayedAck,
    Pressure,
}

impl DeferredAckDrainMode {
    fn source(self) -> &'static str {
        match self {
            Self::ActiveFlow => TUN_RX_DRAIN_SOURCE_REMOTE_PAYLOAD_DEFERRED,
            Self::DelayedAck => TUN_RX_DRAIN_SOURCE_REMOTE_PAYLOAD_DEFERRED_DELAYED,
            Self::Pressure => TUN_RX_DRAIN_SOURCE_REMOTE_PAYLOAD_DEFERRED_PRESSURE,
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct DeferredAckDrainState {
    armed: Option<DeferredAckDrainMode>,
}

impl DeferredAckDrainState {
    fn is_armed(&self) -> bool {
        self.armed.is_some()
    }

    fn arm_after_remote_payload(&mut self) -> bool {
        if self.armed.is_some() {
            return false;
        }
        self.armed = Some(DeferredAckDrainMode::ActiveFlow);
        true
    }

    fn arm_after_budget_exhausted(&mut self) -> bool {
        match self.armed {
            None => {
                self.armed = Some(DeferredAckDrainMode::Pressure);
                true
            }
            Some(DeferredAckDrainMode::ActiveFlow | DeferredAckDrainMode::DelayedAck) => {
                self.armed = Some(DeferredAckDrainMode::Pressure);
                false
            }
            Some(DeferredAckDrainMode::Pressure) => false,
        }
    }

    fn arm_after_delayed_ack_probe(&mut self) -> bool {
        if self.armed.is_some() {
            return false;
        }
        self.armed = Some(DeferredAckDrainMode::DelayedAck);
        true
    }

    fn take_due(&mut self) -> Option<DeferredAckDrainMode> {
        let mode = self.armed?;
        self.armed = None;
        Some(mode)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RelayGapAckDrainFollowup {
    BudgetExhausted,
    Probe,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct RelayGapAckDrainState {
    armed: Option<RelayGapAckDrainFollowup>,
    probe_followups_remaining: u8,
}

impl RelayGapAckDrainState {
    fn is_armed(&self) -> bool {
        self.armed.is_some()
    }

    fn arm_after_budget_exhausted(&mut self) -> bool {
        if self.armed.is_some() {
            return false;
        }
        self.armed = Some(RelayGapAckDrainFollowup::BudgetExhausted);
        self.probe_followups_remaining = 0;
        true
    }

    fn arm_after_hint_drain(&mut self, drained: usize, budget: usize) -> bool {
        if budget == 0 || self.armed.is_some() {
            return false;
        }
        if drained >= budget {
            return self.arm_after_budget_exhausted();
        }
        self.probe_followups_remaining = if drained == 0 {
            RELAY_GAP_ACK_DRAIN_EMPTY_FOLLOWUPS
        } else {
            RELAY_GAP_ACK_DRAIN_PROGRESS_FOLLOWUPS
        };
        self.armed = Some(RelayGapAckDrainFollowup::Probe);
        true
    }

    fn arm_after_followup_drain(
        &mut self,
        mode: RelayGapAckDrainFollowup,
        drained: usize,
        budget: usize,
    ) -> bool {
        if budget == 0 || self.armed.is_some() {
            return false;
        }
        if drained >= budget {
            return self.arm_after_budget_exhausted();
        }
        match mode {
            RelayGapAckDrainFollowup::BudgetExhausted => false,
            RelayGapAckDrainFollowup::Probe => {
                if drained > 0 {
                    self.probe_followups_remaining = RELAY_GAP_ACK_DRAIN_PROGRESS_FOLLOWUPS;
                } else if self.probe_followups_remaining > 0 {
                    self.probe_followups_remaining -= 1;
                } else {
                    return false;
                }
                self.armed = Some(RelayGapAckDrainFollowup::Probe);
                true
            }
        }
    }

    fn take_due(&mut self) -> Option<RelayGapAckDrainFollowup> {
        let mode = self.armed?;
        self.armed = None;
        Some(mode)
    }
}

fn pressure_tun_rx_drain_budget(cfg: DownlinkBackpressureConfig, tun_mtu: usize) -> usize {
    let guard = tx_queue_credit_guard_bytes(cfg);
    if guard == 0 {
        return 0;
    }
    let packet_bytes = tun_mtu.clamp(1, TUN_RX_PRESSURE_DRAIN_ESTIMATED_PACKET_BYTES);
    let packets = guard.saturating_add(packet_bytes.saturating_sub(1)) / packet_bytes;
    packets.clamp(1, TUN_RX_PRESSURE_DRAIN_MAX_PACKETS)
}

fn active_flow_tun_rx_drain_budget(cfg: DownlinkBackpressureConfig, tun_mtu: usize) -> usize {
    let pressure_budget = pressure_tun_rx_drain_budget(cfg, tun_mtu);
    if pressure_budget == 0 {
        return 0;
    }
    pressure_budget
        .saturating_add(7)
        .saturating_div(8)
        .clamp(1, TUN_RX_ACTIVE_FLOW_DRAIN_MAX_PACKETS)
}

fn tun_rx_drain_budget_after_remote_payload(
    has_downlink_work: bool,
    send_queue_bytes: usize,
    configured_budget: usize,
    cfg: DownlinkBackpressureConfig,
    tun_mtu: usize,
) -> usize {
    if !has_downlink_work {
        return 0;
    }
    if configured_budget > 0 {
        return configured_budget;
    }
    if send_queue_bytes >= tx_queue_credit_spend_threshold(cfg) {
        pressure_tun_rx_drain_budget(cfg, tun_mtu)
    } else {
        active_flow_tun_rx_drain_budget(cfg, tun_mtu)
    }
}

fn tun_rx_drain_budget_before_remote_payload(
    ctx: Option<&SocketCtx>,
    snapshot: SocketCloseSnapshot,
    epoch: u64,
    payload_len: usize,
    configured_budget: usize,
    cfg: DownlinkBackpressureConfig,
    tun_mtu: usize,
) -> usize {
    let Some(ctx) = ctx else {
        return 0;
    };
    if payload_len == 0 || ctx.conn_epoch != epoch {
        return 0;
    }
    if remote_payload_disposition(ctx, snapshot) != RemotePayloadDisposition::Accept {
        return 0;
    }

    tun_rx_drain_budget_after_remote_payload(
        true,
        snapshot.send_queue,
        configured_budget,
        cfg,
        tun_mtu,
    )
}

fn active_ack_drain_budget_for_egress_headroom(
    stats: DownlinkPressureStats,
    cfg: DownlinkBackpressureConfig,
    tun_feedback_paused: bool,
    max_budget: usize,
) -> usize {
    if tun_feedback_paused || max_budget == 0 {
        return 0;
    }

    let pause_edge = tx_queue_pause_threshold(cfg);
    if stats.max_tx_queue >= pause_edge {
        return 0;
    }

    let credit_edge = tx_queue_credit_spend_threshold(cfg);
    if stats.max_tx_queue >= credit_edge {
        return max_budget.min(1);
    }

    let full_budget_edge = tx_queue_flush_threshold(cfg);
    if stats.max_tx_queue <= full_budget_edge {
        return max_budget;
    }

    let span = credit_edge.saturating_sub(full_budget_edge);
    if span == 0 {
        return max_budget.min(1);
    }
    let headroom = credit_edge.saturating_sub(stats.max_tx_queue);
    1 + max_budget
        .saturating_sub(1)
        .saturating_mul(headroom)
        .saturating_div(span)
}

fn tun_rx_drain_budget_for_deferred_ack_drain(
    mode: DeferredAckDrainMode,
    stats: DownlinkPressureStats,
    tun_feedback_paused: bool,
    configured_budget: usize,
    cfg: DownlinkBackpressureConfig,
    tun_mtu: usize,
) -> usize {
    let max_budget = if configured_budget > 0 {
        configured_budget
    } else {
        match mode {
            DeferredAckDrainMode::ActiveFlow | DeferredAckDrainMode::DelayedAck => {
                active_flow_tun_rx_drain_budget(cfg, tun_mtu)
            }
            DeferredAckDrainMode::Pressure => pressure_tun_rx_drain_budget(cfg, tun_mtu),
        }
    };
    active_ack_drain_budget_for_egress_headroom(stats, cfg, tun_feedback_paused, max_budget)
}

fn tun_rx_drain_budget_for_relay_gap_hint(
    ctx: Option<&SocketCtx>,
    epoch: u64,
    stats: DownlinkPressureStats,
    tun_feedback_paused: bool,
    configured_budget: usize,
    cfg: DownlinkBackpressureConfig,
    tun_mtu: usize,
) -> usize {
    let Some(ctx) = ctx else {
        return 0;
    };
    if ctx.conn_epoch != epoch || !matches!(ctx.state, SocketState::Relaying | SocketState::Closing)
    {
        return 0;
    }
    let max_budget = if configured_budget > 0 {
        configured_budget
    } else {
        pressure_tun_rx_drain_budget(cfg, tun_mtu)
    };
    active_ack_drain_budget_for_egress_headroom(stats, cfg, tun_feedback_paused, max_budget)
}

fn tun_rx_drain_budget_for_relay_gap_followup(
    stats: DownlinkPressureStats,
    tun_feedback_paused: bool,
    configured_budget: usize,
    cfg: DownlinkBackpressureConfig,
    tun_mtu: usize,
) -> usize {
    let max_budget = if configured_budget > 0 {
        configured_budget
    } else {
        pressure_tun_rx_drain_budget(cfg, tun_mtu)
    };
    active_ack_drain_budget_for_egress_headroom(stats, cfg, tun_feedback_paused, max_budget)
}

fn tun_rx_drain_budget_for_dirty_pressure(
    has_dirty_downlink: bool,
    has_dirty_close_drain: bool,
    stats: DownlinkPressureStats,
    configured_budget: usize,
    cfg: DownlinkBackpressureConfig,
    tun_mtu: usize,
) -> usize {
    let pressure_eligible = has_dirty_close_drain
        || reaches_downlink_credit_debt_pressure_threshold(stats, cfg)
        || (stats.max_pending > 0 && stats.max_tx_queue >= tx_queue_flush_threshold(cfg));
    if !has_dirty_downlink || !pressure_eligible {
        return 0;
    }
    if configured_budget > 0 {
        configured_budget
    } else {
        pressure_tun_rx_drain_budget(cfg, tun_mtu)
    }
}

fn tun_rx_drain_budget_for_recent_active_flow(
    has_recent_downlink_work: bool,
    active_flow_timer_ms: u64,
    stats: DownlinkPressureStats,
    tun_feedback_paused: bool,
    configured_budget: usize,
    cfg: DownlinkBackpressureConfig,
    tun_mtu: usize,
) -> usize {
    if !has_recent_downlink_work || active_flow_timer_ms == 0 {
        return 0;
    }
    let max_budget = if configured_budget > 0 {
        configured_budget
    } else {
        active_flow_tun_rx_drain_budget(cfg, tun_mtu)
    };
    active_ack_drain_budget_for_egress_headroom(stats, cfg, tun_feedback_paused, max_budget)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DownlinkEgressPacer {
    immediate_budget_bytes: usize,
    remaining_immediate_bytes: usize,
}

impl DownlinkEgressPacer {
    fn new(immediate_budget_bytes: usize) -> Self {
        Self {
            immediate_budget_bytes,
            remaining_immediate_bytes: immediate_budget_bytes,
        }
    }

    fn debit_budget(&mut self, accepted_bytes: usize) {
        self.remaining_immediate_bytes = self
            .remaining_immediate_bytes
            .saturating_sub(accepted_bytes);
    }

    fn allow_headroom_limited_flush(&mut self, accepted_bytes: usize) -> bool {
        if accepted_bytes == 0 || self.immediate_budget_bytes == 0 {
            return false;
        }
        if accepted_bytes > self.remaining_immediate_bytes {
            self.remaining_immediate_bytes = 0;
            return false;
        }
        self.remaining_immediate_bytes -= accepted_bytes;
        true
    }

    fn allow_remote_payload_flush(
        &mut self,
        accepted_bytes: usize,
        pending_bytes: usize,
        send_queue_bytes: usize,
        backpressure: DownlinkBackpressureConfig,
    ) -> bool {
        let send_queue_defer_threshold = if pending_bytes > 0 {
            backpressure.high_bytes
        } else {
            tx_queue_flush_threshold(backpressure)
        };
        if send_queue_bytes >= send_queue_defer_threshold {
            self.debit_budget(accepted_bytes);
            return false;
        }
        if pending_bytes > 0 {
            self.debit_budget(accepted_bytes);
            return true;
        }
        if accepted_bytes == 0 || self.immediate_budget_bytes == 0 {
            return false;
        }
        if accepted_bytes > self.remaining_immediate_bytes {
            self.remaining_immediate_bytes = 0;
            return false;
        }
        self.remaining_immediate_bytes -= accepted_bytes;
        true
    }

    fn on_timer_tick(&mut self) {
        self.remaining_immediate_bytes = self.immediate_budget_bytes;
    }
}

fn next_downlink_backpressure(
    was_paused: bool,
    stats: DownlinkPressureStats,
    cfg: DownlinkBackpressureConfig,
) -> bool {
    next_downlink_backpressure_with_credit_debt(was_paused, stats, cfg, false)
}

fn next_downlink_backpressure_with_credit_debt(
    was_paused: bool,
    stats: DownlinkPressureStats,
    cfg: DownlinkBackpressureConfig,
    has_active_credit_debt: bool,
) -> bool {
    if has_active_credit_debt && reaches_downlink_credit_debt_pressure_threshold(stats, cfg) {
        return true;
    }

    if was_paused && stats.max_pending > cfg.low_bytes {
        return true;
    }
    if !was_paused && stats.max_pending >= cfg.high_bytes {
        return true;
    }

    if was_paused {
        stats.max_tx_queue > tx_queue_resume_threshold(cfg)
    } else {
        stats.max_tx_queue >= tx_queue_pause_threshold(cfg)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BufferedDownlinkCreditReason {
    Open,
    SoftHigh,
    TerminalNoRecv,
    HardFeedback,
    PerFlowHard,
    GlobalHard,
}

impl BufferedDownlinkCreditReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::SoftHigh => "soft_high",
            Self::TerminalNoRecv => "terminal_no_recv",
            Self::HardFeedback => "hard_feedback",
            Self::PerFlowHard => "per_flow_hard",
            Self::GlobalHard => "global_hard",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BufferedDownlinkCreditDecision {
    credit: RelayReadCredit,
    reason: BufferedDownlinkCreditReason,
}

fn buffered_downlink_read_credit(
    was_paused: bool,
    per_flow_pending_bytes: usize,
    global_pending_bytes: usize,
    terminal_no_recv: bool,
    hard_feedback_active: bool,
    cfg: BufferedDownlinkConfig,
) -> BufferedDownlinkCreditDecision {
    let paused = |reason| BufferedDownlinkCreditDecision {
        credit: RelayReadCredit {
            paused: true,
            max_batch_bytes: 0,
        },
        reason,
    };

    if terminal_no_recv {
        return paused(BufferedDownlinkCreditReason::TerminalNoRecv);
    }
    if hard_feedback_active {
        return paused(BufferedDownlinkCreditReason::HardFeedback);
    }
    if per_flow_pending_bytes >= cfg.per_flow_hard_bytes
        || (was_paused && per_flow_pending_bytes > cfg.per_flow_low_bytes)
    {
        return paused(BufferedDownlinkCreditReason::PerFlowHard);
    }
    if global_pending_bytes >= cfg.global_hard_bytes
        || (was_paused && global_pending_bytes > cfg.global_low_bytes)
    {
        return paused(BufferedDownlinkCreditReason::GlobalHard);
    }

    let soft_limited = per_flow_pending_bytes >= cfg.per_flow_high_bytes
        || global_pending_bytes >= cfg.global_high_bytes;
    let max_batch_bytes = if soft_limited {
        RELAY_REMOTE_READ_MIN_BATCH_BYTES
    } else {
        RELAY_REMOTE_READ_BURST_MAX_BYTES
    };
    BufferedDownlinkCreditDecision {
        credit: RelayReadCredit {
            paused: false,
            max_batch_bytes,
        },
        reason: if soft_limited {
            BufferedDownlinkCreditReason::SoftHigh
        } else {
            BufferedDownlinkCreditReason::Open
        },
    }
}

fn next_buffered_global_rx_receive_backpressure(
    was_paused: bool,
    stats: DownlinkPressureStats,
    cfg: BufferedDownlinkConfig,
) -> bool {
    if was_paused {
        return stats.max_pending > cfg.per_flow_low_bytes
            || stats.total_pending > cfg.global_low_bytes;
    }
    stats.max_pending >= cfg.per_flow_hard_bytes || stats.total_pending >= cfg.global_hard_bytes
}

fn format_buffered_downlink_read_credit_diag(
    handle: SocketHandle,
    epoch: u64,
    decision: BufferedDownlinkCreditDecision,
    per_flow_pending_bytes: usize,
    global_pending_bytes: usize,
    cfg: BufferedDownlinkConfig,
) -> String {
    format!(
        "🔎 tcp-buffered-downlink-credit handle={handle:?} epoch={epoch} paused={} max_batch_bytes={} reason={} per_flow_pending={} global_pending={} per_flow_low={} per_flow_high={} per_flow_hard={} global_low={} global_high={} global_hard={}",
        decision.credit.paused,
        decision.credit.max_batch_bytes,
        decision.reason.as_str(),
        per_flow_pending_bytes,
        global_pending_bytes,
        cfg.per_flow_low_bytes,
        cfg.per_flow_high_bytes,
        cfg.per_flow_hard_bytes,
        cfg.global_low_bytes,
        cfg.global_high_bytes,
        cfg.global_hard_bytes
    )
}

fn format_tcp_downlink_backpressure_diag(
    paused: bool,
    raw: DownlinkPressureStats,
    effective: DownlinkPressureStats,
    cfg: DownlinkBackpressureConfig,
) -> String {
    format!(
        "🔎 tcp-downlink-backpressure paused={} max_pending={} total_pending={} max_tx_queue={} total_tx_queue={} max_pressure={} total_pressure={} high={} low={} tx_queue_flush_high={} tx_queue_pause_high={} tx_queue_resume_high={} raw_max_pending={} raw_total_pending={} raw_max_tx_queue={} raw_total_tx_queue={} raw_max_pressure={} raw_total_pressure={} effective_max_pending={} effective_total_pending={} effective_max_tx_queue={} effective_total_tx_queue={} effective_max_pressure={} effective_total_pressure={} held_pressure={}",
        paused,
        effective.max_pending,
        effective.total_pending,
        effective.max_tx_queue,
        effective.total_tx_queue,
        effective.max_pressure(),
        effective.total_pressure(),
        cfg.high_bytes,
        cfg.low_bytes,
        tx_queue_flush_threshold(cfg),
        tx_queue_pause_threshold(cfg),
        tx_queue_resume_threshold(cfg),
        raw.max_pending,
        raw.total_pending,
        raw.max_tx_queue,
        raw.total_tx_queue,
        raw.max_pressure(),
        raw.total_pressure(),
        effective.max_pending,
        effective.total_pending,
        effective.max_tx_queue,
        effective.total_tx_queue,
        effective.max_pressure(),
        effective.total_pressure(),
        raw != effective
    )
}

fn format_tcp_global_rx_backpressure_diag(
    paused: bool,
    stats: DownlinkPressureStats,
    cfg: DownlinkBackpressureConfig,
    local_egress_paused: bool,
    tun_feedback_paused: bool,
) -> String {
    format!(
        "🔎 tcp-global-rx-backpressure paused={} max_pending={} total_pending={} max_tx_queue={} total_tx_queue={} max_pressure={} total_pressure={} receive_high={} receive_low={} receive_total_high={} receive_total_low={} local_egress_paused={} tun_feedback_paused={}",
        paused,
        stats.max_pending,
        stats.total_pending,
        stats.max_tx_queue,
        stats.total_tx_queue,
        stats.max_pressure(),
        stats.total_pressure(),
        downlink_receive_window_high(cfg),
        downlink_receive_window_low(cfg),
        downlink_receive_window_total_high(cfg),
        downlink_receive_window_total_low(cfg),
        local_egress_paused,
        tun_feedback_paused
    )
}

fn parse_backpressure_bytes(s: Option<&str>, default: usize) -> usize {
    s.and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

#[cfg(test)]
fn parse_downlink_backpressure_config(
    high: Option<&str>,
    low: Option<&str>,
) -> DownlinkBackpressureConfig {
    parse_downlink_backpressure_config_with_default(
        high,
        low,
        DownlinkBackpressureConfig::default(),
    )
}

fn default_downlink_backpressure_for_tx_buffer(
    _tx_bytes: usize,
    tun_mtu: usize,
    tun_tx_queue_len: usize,
) -> DownlinkBackpressureConfig {
    // Knife14bs rejected tx-buffer-sized auto high watermarks. Knife14ed then
    // showed the fixed 512 KiB default can overrun a 1200-byte MTU TUN qdisc,
    // so the runtime default is now capped by estimated local egress capacity.
    default_downlink_backpressure_for_tun_egress(tun_mtu, tun_tx_queue_len)
}

fn parse_downlink_backpressure_config_for_tx_buffer(
    high: Option<&str>,
    low: Option<&str>,
    tx_bytes: usize,
    tun_mtu: usize,
    tun_tx_queue_len: usize,
) -> DownlinkBackpressureConfig {
    parse_downlink_backpressure_config_with_default(
        high,
        low,
        default_downlink_backpressure_for_tx_buffer(tx_bytes, tun_mtu, tun_tx_queue_len),
    )
}

fn parse_downlink_backpressure_config_with_default(
    high: Option<&str>,
    low: Option<&str>,
    default: DownlinkBackpressureConfig,
) -> DownlinkBackpressureConfig {
    let high_bytes = parse_backpressure_bytes(high, default.high_bytes);
    let mut low_bytes = parse_backpressure_bytes(low, default.low_bytes);
    if low_bytes >= high_bytes {
        low_bytes = default.low_bytes;
    }
    if low_bytes >= high_bytes {
        low_bytes = high_bytes.saturating_sub(1).max(1);
    }
    DownlinkBackpressureConfig {
        high_bytes,
        low_bytes,
    }
}

fn parse_downlink_flush_max_bytes(s: Option<&str>) -> usize {
    s.and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| {
            (MIN_DOWNLINK_FLUSH_MAX_BYTES..=MAX_DOWNLINK_FLUSH_MAX_BYTES).contains(value)
        })
        .unwrap_or(DEFAULT_DOWNLINK_FLUSH_MAX_BYTES)
}

fn parse_downlink_egress_immediate_bytes(s: Option<&str>) -> usize {
    s.and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value <= MAX_DOWNLINK_EGRESS_IMMEDIATE_BYTES)
        .unwrap_or(DEFAULT_DOWNLINK_EGRESS_IMMEDIATE_BYTES)
}

fn parse_tcp_socket_buffer_bytes(s: Option<&str>, default: usize) -> usize {
    s.and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| (MIN_TCP_SOCKET_BUFFER_BYTES..=MAX_TCP_SOCKET_BUFFER_BYTES).contains(value))
        .unwrap_or(default)
}

fn parse_tcp_socket_buffer_config(rx: Option<&str>, tx: Option<&str>) -> TcpSocketBufferConfig {
    let default = TcpSocketBufferConfig::default();
    TcpSocketBufferConfig {
        rx_bytes: parse_tcp_socket_buffer_bytes(rx, default.rx_bytes),
        tx_bytes: parse_tcp_socket_buffer_bytes(tx, default.tx_bytes),
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
    /// Estimated Linux TUN qdisc length used to derive safe default downlink
    /// egress watermarks. It is an estimate because the actual interface is
    /// created after config parsing; operators can override it for harnesses.
    tun_tx_queue_len: usize,
    /// 刀11：数据面可观测性 `📊` 快照周期（秒）。默认 [`METRICS_SNAPSHOT_SECS`]（30）；
    /// env `MINI_VPN_METRICS_SECS` 可调（acceptance 设小值如 5 秒级看指标；0/非法回落默认，**绝不为 0**
    /// 否则 `tokio::time::interval` panic）。仅 `from_env` 读 env，`from_sources`（harness/测试）恒用默认。
    pub metrics_secs: u64,
    /// 刀12：主循环 profiler 开关。`MINI_VPN_PROFILE_LOOP=1` 时主循环装 `LoopProfiler`（打 🔬 归因行
    /// 量化 #4 vs #3）；默认 `false` → 装 `NoopSink`（**零开销路径逐字不变**）。仅 `from_env` 读 env，
    /// `from_sources`（harness/测试）恒 `false`。
    pub profile_loop: bool,
    downlink_backpressure: DownlinkBackpressureConfig,
    downlink_flush_max_bytes: usize,
    downlink_egress_immediate_bytes: usize,
    tcp_socket_buffers: TcpSocketBufferConfig,
    tun_rx_drain_budget: usize,
    tun_rx_active_flow_timer_ms: u64,
    bounded_global_rx_receive_window: bool,
    buffered_downlink: BufferedDownlinkConfig,
}

impl TunRuntimeConfig {
    /// Build config from optional string sources.（metrics 周期恒默认；env 覆盖只在 `from_env`。）
    pub fn from_sources(pool_size: Option<&str>) -> Result<Self, ClientError> {
        let listener = TunListenerConfig::from_sources(pool_size)?;
        let downlink_backpressure = default_downlink_backpressure_for_tun_egress(
            DEFAULT_TUN_MTU,
            DEFAULT_TUN_TX_QUEUE_LEN_ESTIMATE,
        );
        let listener_pool_size = listener.pool_size;
        Ok(Self {
            listener,
            tun_mtu: DEFAULT_TUN_MTU,
            tun_tx_queue_len: DEFAULT_TUN_TX_QUEUE_LEN_ESTIMATE,
            metrics_secs: METRICS_SNAPSHOT_SECS,
            profile_loop: false,
            downlink_backpressure,
            downlink_flush_max_bytes: DEFAULT_DOWNLINK_FLUSH_MAX_BYTES,
            downlink_egress_immediate_bytes: DEFAULT_DOWNLINK_EGRESS_IMMEDIATE_BYTES,
            tcp_socket_buffers: TcpSocketBufferConfig::default(),
            tun_rx_drain_budget: DEFAULT_TUN_RX_DRAIN_BUDGET,
            tun_rx_active_flow_timer_ms: DEFAULT_TUN_RX_ACTIVE_FLOW_TIMER_DRAIN_MS,
            bounded_global_rx_receive_window: false,
            buffered_downlink: BufferedDownlinkConfig::disabled(
                downlink_backpressure,
                listener_pool_size,
            ),
        })
    }

    pub fn tcp_socket_buffer_bytes(&self) -> (usize, usize) {
        (
            self.tcp_socket_buffers.rx_bytes,
            self.tcp_socket_buffers.tx_bytes,
        )
    }

    /// Read config from process environment
    /// （`MINI_VPN_TUN_POOL_SIZE` + `MINI_VPN_METRICS_SECS` + `MINI_VPN_PROFILE_LOOP`
    /// + downlink backpressure watermarks + TCP socket buffers）。
    fn from_env() -> Result<Self, ClientError> {
        let mut cfg = Self::from_sources(std::env::var("MINI_VPN_TUN_POOL_SIZE").ok().as_deref())?;
        cfg.tun_mtu = parse_tun_mtu(std::env::var("MINI_VPN_TUN_MTU").ok().as_deref());
        cfg.tun_tx_queue_len =
            parse_tun_tx_queue_len(std::env::var("MINI_VPN_TUN_TX_QUEUE_LEN").ok().as_deref());
        cfg.metrics_secs =
            parse_metrics_secs(std::env::var("MINI_VPN_METRICS_SECS").ok().as_deref());
        cfg.profile_loop =
            parse_profile_loop(std::env::var("MINI_VPN_PROFILE_LOOP").ok().as_deref());
        let tcp_rx_buffer = std::env::var("MINI_VPN_TCP_RX_BUFFER_BYTES").ok();
        let tcp_tx_buffer = std::env::var("MINI_VPN_TCP_TX_BUFFER_BYTES").ok();
        cfg.tcp_socket_buffers =
            parse_tcp_socket_buffer_config(tcp_rx_buffer.as_deref(), tcp_tx_buffer.as_deref());
        let downlink_backpressure_high =
            std::env::var("MINI_VPN_DOWNLINK_BACKPRESSURE_HIGH_BYTES").ok();
        let downlink_backpressure_low =
            std::env::var("MINI_VPN_DOWNLINK_BACKPRESSURE_LOW_BYTES").ok();
        cfg.downlink_backpressure = parse_downlink_backpressure_config_for_tx_buffer(
            downlink_backpressure_high.as_deref(),
            downlink_backpressure_low.as_deref(),
            cfg.tcp_socket_buffers.tx_bytes,
            cfg.tun_mtu,
            cfg.tun_tx_queue_len,
        );
        cfg.downlink_flush_max_bytes = parse_downlink_flush_max_bytes(
            std::env::var("MINI_VPN_DOWNLINK_FLUSH_MAX_BYTES")
                .ok()
                .as_deref(),
        );
        cfg.downlink_egress_immediate_bytes = parse_downlink_egress_immediate_bytes(
            std::env::var("MINI_VPN_DOWNLINK_EGRESS_IMMEDIATE_BYTES")
                .ok()
                .as_deref(),
        );
        cfg.tun_rx_drain_budget = parse_tun_rx_drain_budget(
            std::env::var("MINI_VPN_TUN_RX_DRAIN_BUDGET")
                .ok()
                .as_deref(),
        );
        cfg.tun_rx_active_flow_timer_ms = parse_tun_rx_active_flow_timer_ms(
            std::env::var("MINI_VPN_TUN_RX_ACTIVE_FLOW_TIMER_MS")
                .ok()
                .as_deref(),
        );
        cfg.bounded_global_rx_receive_window = parse_trace(
            std::env::var("MINI_VPN_BOUNDED_GLOBAL_RX_RECEIVE_WINDOW")
                .ok()
                .as_deref(),
        );
        let buffered_downlink_enabled =
            parse_trace(std::env::var("MINI_VPN_BUFFERED_DOWNLINK").ok().as_deref());
        cfg.buffered_downlink = BufferedDownlinkConfig::from_backpressure(
            buffered_downlink_enabled,
            cfg.downlink_backpressure,
            cfg.listener.pool_size,
        );
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

fn parse_tun_tx_queue_len(s: Option<&str>) -> usize {
    s.and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|&n| (1..=MAX_TUN_TX_QUEUE_LEN_ESTIMATE).contains(&n))
        .unwrap_or(DEFAULT_TUN_TX_QUEUE_LEN_ESTIMATE)
}

/// Knife14bh：`0` 明确关闭 opportunistic drain；非法/越界回落默认，避免误把热路径扫成无界循环。
fn parse_tun_rx_drain_budget(s: Option<&str>) -> usize {
    s.and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|&n| n <= MAX_TUN_RX_DRAIN_BUDGET)
        .unwrap_or(DEFAULT_TUN_RX_DRAIN_BUDGET)
}

/// Knife14ex：默认启用短窗口 ACK/window service lane；0 仍可由 operator 显式关闭。
fn parse_tun_rx_active_flow_timer_ms(s: Option<&str>) -> u64 {
    s.and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|&n| n <= MAX_TUN_RX_ACTIVE_FLOW_TIMER_DRAIN_MS)
        .unwrap_or(DEFAULT_TUN_RX_ACTIVE_FLOW_TIMER_DRAIN_MS)
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
        "🚀 TUN runtime started with pool_size={}, tun_mtu={}, tun_tx_queue_len_estimate={}",
        runtime_config.listener.pool_size, runtime_config.tun_mtu, runtime_config.tun_tx_queue_len
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
    println!(
        "🧯 TCP 下行背压: high={}B low={}B egress_safe_flush={}B egress_capacity={}B（auto 按 MINI_VPN_TUN_MTU/MINI_VPN_TUN_TX_QUEUE_LEN 估算；MINI_VPN_DOWNLINK_BACKPRESSURE_* 可显式覆盖）",
        runtime_config.downlink_backpressure.high_bytes,
        runtime_config.downlink_backpressure.low_bytes,
        tun_egress_safe_flush_threshold(runtime_config.tun_mtu, runtime_config.tun_tx_queue_len),
        tun_egress_capacity_bytes(runtime_config.tun_mtu, runtime_config.tun_tx_queue_len)
    );
    println!(
        "🚰 TCP 下行 flush budget: max={}B（MINI_VPN_DOWNLINK_FLUSH_MAX_BYTES 可调）",
        runtime_config.downlink_flush_max_bytes
    );
    println!(
        "🚦 TCP 下行 egress pacing: immediate={}B/tick tx_queue_flush_high={}B tx_queue_credit_high={}B tx_queue_pause_high={}B（MINI_VPN_DOWNLINK_EGRESS_IMMEDIATE_BYTES 可调；pending backlog 在本地 egress 压力低于 high 时强制 immediate flush）",
        runtime_config.downlink_egress_immediate_bytes,
        tx_queue_flush_threshold(runtime_config.downlink_backpressure),
        tx_queue_credit_spend_threshold(runtime_config.downlink_backpressure),
        tx_queue_pause_threshold(runtime_config.downlink_backpressure)
    );
    println!(
        "🧱 TCP socket buffers: rx={}B tx={}B（MINI_VPN_TCP_*_BUFFER_BYTES 可调）",
        runtime_config.tcp_socket_buffers.rx_bytes, runtime_config.tcp_socket_buffers.tx_bytes
    );
    let adaptive_tun_rx_pressure_budget =
        pressure_tun_rx_drain_budget(runtime_config.downlink_backpressure, runtime_config.tun_mtu);
    println!(
        "🧺 TUN RX drain budget: {} packets/pass（0=仅 pressure-adaptive；pressure-adaptive={} packets near tx_queue_credit_high；MINI_VPN_TUN_RX_DRAIN_BUDGET>0 显式覆盖）",
        runtime_config.tun_rx_drain_budget, adaptive_tun_rx_pressure_budget
    );
    println!(
        "🧪 TUN RX active-flow ACK/window service: {}ms（0=disabled；MINI_VPN_TUN_RX_ACTIVE_FLOW_TIMER_MS 可覆盖）",
        runtime_config.tun_rx_active_flow_timer_ms
    );
    println!(
        "🧪 bounded global_rx receive window: {}（MINI_VPN_BOUNDED_GLOBAL_RX_RECEIVE_WINDOW=1 仅用于 Knife14cg A/B）",
        if runtime_config.bounded_global_rx_receive_window {
            "enabled"
        } else {
            "disabled"
        }
    );
    println!(
        "🧪 buffered downlink: {} per_flow(low/high/hard)={}/{}/{}B global(low/high/hard)={}/{}/{}B（MINI_VPN_BUFFERED_DOWNLINK=1）",
        if runtime_config.buffered_downlink.enabled {
            "enabled"
        } else {
            "disabled"
        },
        runtime_config.buffered_downlink.per_flow_low_bytes,
        runtime_config.buffered_downlink.per_flow_high_bytes,
        runtime_config.buffered_downlink.per_flow_hard_bytes,
        runtime_config.buffered_downlink.global_low_bytes,
        runtime_config.buffered_downlink.global_high_bytes,
        runtime_config.buffered_downlink.global_hard_bytes,
    );

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
                    println!(
                        "✅ 已配置 REALITY 出口（VLESS over REALITY over TCP；TCP-only，UDP no-op）"
                    );
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
                    println!(
                        "✅ 已配置 REALITY 出口（failover 备腿；VLESS over REALITY over TCP）"
                    );
                    Arc::new(u)
                }
                Err(e) => {
                    println!(
                        "加载 REALITY 客户端配置失败（failover 需两腿都配齐，启动中止）: {e:?}"
                    );
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
    let mut registry =
        ListenerRegistry::with_socket_buffers(pool_size, runtime_config.tcp_socket_buffers);
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
    let mut tun_egress_feedback_tick = tokio::time::interval(std::time::Duration::from_secs(
        TUN_EGRESS_FEEDBACK_SAMPLE_SECS,
    ));
    let mut tcp_loop_flush_tx_calls: u64 = 0;
    let mut tcp_loop_flush_tx_failures: u64 = 0;
    let downlink_backpressure = runtime_config.downlink_backpressure;
    let downlink_flush_max_bytes = runtime_config.downlink_flush_max_bytes;
    let tun_rx_drain_budget = runtime_config.tun_rx_drain_budget;
    let tun_rx_active_flow_timer_ms = runtime_config.tun_rx_active_flow_timer_ms;
    let tun_rx_active_flow_timer_duration = (tun_rx_active_flow_timer_ms > 0)
        .then(|| std::time::Duration::from_millis(tun_rx_active_flow_timer_ms));
    let bounded_global_rx_receive_window = runtime_config.bounded_global_rx_receive_window;
    let buffered_downlink = runtime_config.buffered_downlink;
    let mut downlink_egress_pacer =
        DownlinkEgressPacer::new(runtime_config.downlink_egress_immediate_bytes);
    let mut downlink_egress_drop_debt = DownlinkEgressDropDebt::default();
    let mut active_flow_tun_rx_drain_until: Option<std::time::Instant> = None;
    let mut tun_egress_drop_sampler = TunEgressDropSampler::new(device.interface_name());
    let mut tun_egress_feedback = TunEgressFeedbackState::default();
    let mut tun_rx_drain_diag = TunRxDrainDiag::default();
    let deferred_ack_drain_sleep = tokio::time::sleep(std::time::Duration::from_secs(60 * 60));
    tokio::pin!(deferred_ack_drain_sleep);
    let mut deferred_ack_drain = DeferredAckDrainState::default();
    let relay_gap_ack_drain_sleep = tokio::time::sleep(std::time::Duration::from_secs(60 * 60));
    tokio::pin!(relay_gap_ack_drain_sleep);
    let mut relay_gap_ack_drain = RelayGapAckDrainState::default();
    let mut downlink_rx_paused = false;
    let mut global_rx_receive_paused = false;

    loop {
        let downlink_stats_raw = downlink_pressure_stats(&dirty, &socket_ctxs, &sockets);
        let pressure_now = std::time::Instant::now();
        let downlink_stats =
            tun_egress_feedback.effective_downlink_pressure_at(downlink_stats_raw, pressure_now);
        tun_egress_feedback.observe_pressure_at(
            downlink_stats_raw,
            downlink_backpressure,
            pressure_now,
        );
        let next_downlink_rx_paused_without_debt =
            next_downlink_backpressure(downlink_rx_paused, downlink_stats, downlink_backpressure);
        let previous_downlink_rx_paused = downlink_rx_paused;
        if !buffered_downlink.enabled
            && should_install_downlink_pressure_credit_debt(
                previous_downlink_rx_paused,
                next_downlink_rx_paused_without_debt,
                downlink_stats_raw,
                downlink_backpressure,
            )
        {
            let installed_bytes = downlink_egress_drop_debt
                .note_egress_pressure(downlink_stats_raw, downlink_backpressure);
            if installed_bytes > 0 {
                tcp_diag_log!(
                    "{}",
                    format_downlink_egress_credit_debt_diag(
                        if next_downlink_rx_paused_without_debt {
                            "pressure_pause_edge"
                        } else {
                            "pressure_credit_edge"
                        },
                        installed_bytes,
                        downlink_stats_raw,
                        downlink_backpressure,
                        downlink_egress_drop_debt,
                    )
                );
            }
        }
        let next_downlink_rx_paused = next_downlink_backpressure_with_credit_debt(
            downlink_rx_paused,
            downlink_stats,
            downlink_backpressure,
            downlink_egress_drop_debt.has_active_drop_debt(),
        );
        if next_downlink_rx_paused != previous_downlink_rx_paused {
            downlink_rx_paused = next_downlink_rx_paused;
            tcp_diag_log!(
                "{}",
                format_tcp_downlink_backpressure_diag(
                    downlink_rx_paused,
                    downlink_stats_raw,
                    downlink_stats,
                    downlink_backpressure,
                )
            );
        }
        let global_rx_paused = if buffered_downlink.enabled {
            let previous_global_rx_receive_paused = global_rx_receive_paused;
            let next_global_rx_receive_paused = next_buffered_global_rx_receive_backpressure(
                global_rx_receive_paused,
                downlink_stats_raw,
                buffered_downlink,
            );
            if next_global_rx_receive_paused != previous_global_rx_receive_paused {
                global_rx_receive_paused = next_global_rx_receive_paused;
                tcp_diag_log!(
                    "{}",
                    format_tcp_global_rx_backpressure_diag(
                        global_rx_receive_paused,
                        downlink_stats_raw,
                        downlink_backpressure,
                        false,
                        tun_egress_feedback.is_paused(),
                    )
                );
            }
            global_rx_receive_paused
        } else if bounded_global_rx_receive_window {
            let previous_global_rx_receive_paused = global_rx_receive_paused;
            let next_global_rx_receive_paused = next_global_rx_receive_backpressure(
                global_rx_receive_paused,
                downlink_stats_raw,
                downlink_backpressure,
            );
            if next_global_rx_receive_paused != previous_global_rx_receive_paused {
                global_rx_receive_paused = next_global_rx_receive_paused;
                tcp_diag_log!(
                    "{}",
                    format_tcp_global_rx_backpressure_diag(
                        global_rx_receive_paused,
                        downlink_stats_raw,
                        downlink_backpressure,
                        downlink_rx_paused,
                        tun_egress_feedback.is_paused(),
                    )
                );
            }
            global_rx_receive_paused
        } else {
            downlink_rx_paused || tun_egress_feedback.is_paused()
        };
        publish_relay_read_credit_for_dirty_handles(
            &dirty,
            &sockets,
            &mut socket_ctxs,
            downlink_backpressure,
            runtime_config.tun_mtu,
            downlink_flush_max_bytes,
            downlink_egress_drop_debt.has_active_drop_debt(),
            if buffered_downlink.enabled {
                tun_egress_feedback.is_paused()
            } else {
                downlink_rx_paused || tun_egress_feedback.is_paused()
            },
            buffered_downlink,
            downlink_stats_raw.total_pending,
        );
        tokio::select! {
            // TCP relay 回程：后台车厢把远端回传字节送回主循环 → 注入对应 smoltcp socket。
            //   TUIC 自重连（live_conn），不需要 legacy 的 disconnect/复位分支。
            Some((handle, event)) = global_rx.recv(), if !global_rx_paused =>{
                metrics.loop_park_end();
                match event {
                    RelayEvent::Data { epoch, bytes: payload } => {
                        trace_log!("📬 从大邮筒收到 {} 字节数据，准备送往房间 {:?}", payload.len(), handle);
                        let pre_payload_tun_rx_budget = if payload.is_empty() {
                            0
                        } else if let Some(ctx) = socket_ctxs.get(&handle) {
                            let snapshot = {
                                let socket = sockets.get::<TcpSocket>(handle);
                                SocketCloseSnapshot::from_socket(socket)
                            };
                            tun_rx_drain_budget_before_remote_payload(
                                Some(ctx),
                                snapshot,
                                epoch,
                                payload.len(),
                                tun_rx_drain_budget,
                                downlink_backpressure,
                                runtime_config.tun_mtu,
                            )
                        } else {
                            0
                        };
                        if pre_payload_tun_rx_budget > 0 {
                            drain_ready_tun_rx(
                                &mut device,
                                &mut assoc_table,
                                &mut fake_pool,
                                &upstream,
                                udp_clock.elapsed().as_secs(),
                                &metrics_handle,
                                &mut registry,
                                &mut sockets,
                                &mut socket_ctxs,
                                &mut iface,
                                &mut dirty,
                                &handshake_done_tx,
                                &global_tx,
                                &mut metrics,
                                downlink_flush_max_bytes,
                                downlink_backpressure,
                                runtime_config.tun_mtu,
                                &mut downlink_egress_drop_debt,
                                &mut tcp_loop_flush_tx_calls,
                                &mut tcp_loop_flush_tx_failures,
                                &mut tun_rx_drain_diag,
                                pre_payload_tun_rx_budget,
                                TUN_RX_DRAIN_SOURCE_REMOTE_PAYLOAD_PRE,
                                if buffered_downlink.enabled {
                                    tun_egress_feedback.is_paused()
                                } else {
                                    downlink_rx_paused || tun_egress_feedback.is_paused()
                                },
                                buffered_downlink,
                            )
                            .await;
                        }
                        if !buffered_downlink.enabled {
                            publish_projected_relay_read_credit_for_payload(
                                handle,
                                epoch,
                                payload.len(),
                                &sockets,
                                &mut socket_ctxs,
                                downlink_backpressure,
                                runtime_config.tun_mtu,
                                downlink_flush_max_bytes,
                                downlink_egress_drop_debt.has_active_drop_debt(),
                                downlink_rx_paused || tun_egress_feedback.is_paused(),
                            );
                        }
                        let accepted_bytes = match handle_remote_payload(
                            handle,
                            epoch,
                            payload,
                            &mut sockets,
                            &mut socket_ctxs,
                            &mut iface,
                            &mut device,
                            &mut fake_pool,
                            udp_clock.elapsed().as_secs(),
                            downlink_flush_max_bytes,
                            &mut downlink_egress_pacer,
                            &mut downlink_egress_drop_debt,
                            downlink_backpressure,
                            runtime_config.tun_mtu,
                            buffered_downlink.enabled,
                        )
                        .await
                        {
                            Ok(accepted_bytes) => accepted_bytes,
                            Err(e) => {
                                trace_log!("处理回程数据失败: {e}");
                                0
                            }
                        };
                        // #1：回程下行可能留在 app pending，也可能已进入 smoltcp tx queue。
                        // 两者都需要继续标脏，直到本地 TCP/TUN egress 压力退到 low watermark。
                        if socket_ctxs
                            .get(&handle)
                            .map(|c| !c.downlink_pending.is_empty() || accepted_bytes > 0)
                            .unwrap_or(false)
                        {
                            dirty.insert(handle);
                        }
                        let current_global_pending =
                            downlink_pressure_stats(&dirty, &socket_ctxs, &sockets).total_pending;
                        publish_relay_read_credit_for_handle(
                            handle,
                            &sockets,
                            &mut socket_ctxs,
                            downlink_backpressure,
                            runtime_config.tun_mtu,
                            downlink_flush_max_bytes,
                            downlink_egress_drop_debt.has_active_drop_debt(),
                            if buffered_downlink.enabled {
                                tun_egress_feedback.is_paused()
                            } else {
                                downlink_rx_paused || tun_egress_feedback.is_paused()
                            },
                            buffered_downlink,
                            current_global_pending,
                        );
                        let has_downlink_work = should_drain_tun_rx_after_remote_payload(
                            socket_ctxs.get(&handle),
                            accepted_bytes,
                        );
                        if has_downlink_work
                            && let Some(duration) = tun_rx_active_flow_timer_duration
                        {
                            active_flow_tun_rx_drain_until =
                                Some(std::time::Instant::now() + duration);
                        }
                        let tun_rx_budget = if has_downlink_work {
                            let send_queue_bytes = sockets.get::<TcpSocket>(handle).send_queue();
                            tun_rx_drain_budget_after_remote_payload(
                                has_downlink_work,
                                send_queue_bytes,
                                tun_rx_drain_budget,
                                downlink_backpressure,
                                runtime_config.tun_mtu,
                            )
                        } else {
                            0
                        };
                        if tun_rx_budget > 0 {
                            drain_ready_tun_rx(
                                &mut device,
                                &mut assoc_table,
                                &mut fake_pool,
                                &upstream,
                                udp_clock.elapsed().as_secs(),
                                &metrics_handle,
                                &mut registry,
                                &mut sockets,
                                &mut socket_ctxs,
                                &mut iface,
                                &mut dirty,
                                &handshake_done_tx,
                                &global_tx,
                                &mut metrics,
                                downlink_flush_max_bytes,
                                downlink_backpressure,
                                runtime_config.tun_mtu,
                                &mut downlink_egress_drop_debt,
                                &mut tcp_loop_flush_tx_calls,
                                &mut tcp_loop_flush_tx_failures,
                                &mut tun_rx_drain_diag,
                                tun_rx_budget,
                                TUN_RX_DRAIN_SOURCE_REMOTE_PAYLOAD,
                                if buffered_downlink.enabled {
                                    tun_egress_feedback.is_paused()
                                } else {
                                    downlink_rx_paused || tun_egress_feedback.is_paused()
                                },
                                buffered_downlink,
                            )
                            .await;
                        }
                        if has_downlink_work && deferred_ack_drain.arm_after_remote_payload() {
                            deferred_ack_drain_sleep.as_mut().reset(
                                tokio::time::Instant::now()
                                    + std::time::Duration::from_millis(DEFERRED_ACK_DRAIN_DELAY_MS),
                            );
                        }
                    }
                    RelayEvent::AckDrainHint {
                        epoch,
                        gap_ms,
                        remote_to_global_rx_bytes,
                    } => {
                        let relay_read_hard_pause = if buffered_downlink.enabled {
                            tun_egress_feedback.is_paused()
                        } else {
                            downlink_rx_paused || tun_egress_feedback.is_paused()
                        };
                        let cadence_floor = note_ack_cadence_gap_hint_for_handle(
                            handle,
                            epoch,
                            &sockets,
                            &mut socket_ctxs,
                            downlink_backpressure,
                            runtime_config.tun_mtu,
                            relay_read_hard_pause,
                        )
                        .unwrap_or(0);
                        let current_global_pending =
                            downlink_pressure_stats(&dirty, &socket_ctxs, &sockets).total_pending;
                        publish_relay_read_credit_for_handle(
                            handle,
                            &sockets,
                            &mut socket_ctxs,
                            downlink_backpressure,
                            runtime_config.tun_mtu,
                            downlink_flush_max_bytes,
                            downlink_egress_drop_debt.has_active_drop_debt(),
                            relay_read_hard_pause,
                            buffered_downlink,
                            current_global_pending,
                        );
                        let current_downlink_stats =
                            downlink_pressure_stats(&dirty, &socket_ctxs, &sockets);
                        let tun_rx_budget = tun_rx_drain_budget_for_relay_gap_hint(
                            socket_ctxs.get(&handle),
                            epoch,
                            current_downlink_stats,
                            tun_egress_feedback.is_paused(),
                            tun_rx_drain_budget,
                            downlink_backpressure,
                            runtime_config.tun_mtu,
                        );
                        if tun_rx_budget > 0 {
                            tcp_diag_log!(
                                "🔎 tcp-relay-ack-drain-hint handle={:?} epoch={} gap_ms={} remote_to_global_rx_bytes={} budget={} cadence_floor={}",
                                handle,
                                epoch,
                                gap_ms,
                                remote_to_global_rx_bytes,
                                tun_rx_budget,
                                cadence_floor
                            );
                            let drained = drain_ready_tun_rx(
                                &mut device,
                                &mut assoc_table,
                                &mut fake_pool,
                                &upstream,
                                udp_clock.elapsed().as_secs(),
                                &metrics_handle,
                                &mut registry,
                                &mut sockets,
                                &mut socket_ctxs,
                                &mut iface,
                                &mut dirty,
                                &handshake_done_tx,
                                &global_tx,
                                &mut metrics,
                                downlink_flush_max_bytes,
                                downlink_backpressure,
                                runtime_config.tun_mtu,
                                &mut downlink_egress_drop_debt,
                                &mut tcp_loop_flush_tx_calls,
                                &mut tcp_loop_flush_tx_failures,
                                &mut tun_rx_drain_diag,
                                tun_rx_budget,
                                TUN_RX_DRAIN_SOURCE_RELAY_GAP_HINT,
                                if buffered_downlink.enabled {
                                    tun_egress_feedback.is_paused()
                                } else {
                                    downlink_rx_paused || tun_egress_feedback.is_paused()
                                },
                                buffered_downlink,
                            )
                            .await;
                            if relay_gap_ack_drain.arm_after_hint_drain(drained, tun_rx_budget) {
                                relay_gap_ack_drain_sleep.as_mut().reset(
                                    tokio::time::Instant::now()
                                        + std::time::Duration::from_millis(
                                            RELAY_GAP_ACK_DRAIN_FOLLOWUP_DELAY_MS,
                                        ),
                                );
                            }
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
                            downlink_backpressure,
                        ) {
                            dirty.insert(handle);
                        } else {
                            dirty.remove(&handle);
                        }
                    }
                }
            }
            _ = &mut relay_gap_ack_drain_sleep, if relay_gap_ack_drain.is_armed() => {
                metrics.loop_park_end();
                if let Some(mode) = relay_gap_ack_drain.take_due() {
                    let tun_rx_budget = tun_rx_drain_budget_for_relay_gap_followup(
                        downlink_pressure_stats(&dirty, &socket_ctxs, &sockets),
                        tun_egress_feedback.is_paused(),
                        tun_rx_drain_budget,
                        downlink_backpressure,
                        runtime_config.tun_mtu,
                    );
                    if tun_rx_budget > 0 {
                        let drained = drain_ready_tun_rx(
                            &mut device,
                            &mut assoc_table,
                            &mut fake_pool,
                            &upstream,
                            udp_clock.elapsed().as_secs(),
                            &metrics_handle,
                            &mut registry,
                            &mut sockets,
                            &mut socket_ctxs,
                            &mut iface,
                            &mut dirty,
                            &handshake_done_tx,
                            &global_tx,
                            &mut metrics,
                            downlink_flush_max_bytes,
                            downlink_backpressure,
                            runtime_config.tun_mtu,
                            &mut downlink_egress_drop_debt,
                            &mut tcp_loop_flush_tx_calls,
                            &mut tcp_loop_flush_tx_failures,
                            &mut tun_rx_drain_diag,
                            tun_rx_budget,
                            TUN_RX_DRAIN_SOURCE_RELAY_GAP_HINT_FOLLOWUP,
                            if buffered_downlink.enabled {
                                tun_egress_feedback.is_paused()
                            } else {
                                downlink_rx_paused || tun_egress_feedback.is_paused()
                            },
                            buffered_downlink,
                        )
                        .await;
                        if relay_gap_ack_drain.arm_after_followup_drain(
                            mode,
                            drained,
                            tun_rx_budget,
                        ) {
                            relay_gap_ack_drain_sleep.as_mut().reset(
                                tokio::time::Instant::now()
                                    + std::time::Duration::from_millis(
                                        RELAY_GAP_ACK_DRAIN_FOLLOWUP_DELAY_MS,
                                    ),
                            );
                        }
                    }
                }
            }
            _ = &mut deferred_ack_drain_sleep, if deferred_ack_drain.is_armed() => {
                metrics.loop_park_end();
                if let Some(mode) = deferred_ack_drain.take_due() {
                    let tun_rx_budget = tun_rx_drain_budget_for_deferred_ack_drain(
                        mode,
                        downlink_pressure_stats(&dirty, &socket_ctxs, &sockets),
                        tun_egress_feedback.is_paused(),
                        tun_rx_drain_budget,
                        downlink_backpressure,
                        runtime_config.tun_mtu,
                    );
                    if tun_rx_budget > 0 {
                        let drained = drain_ready_tun_rx(
                            &mut device,
                            &mut assoc_table,
                            &mut fake_pool,
                            &upstream,
                            udp_clock.elapsed().as_secs(),
                            &metrics_handle,
                            &mut registry,
                            &mut sockets,
                            &mut socket_ctxs,
                            &mut iface,
                            &mut dirty,
                            &handshake_done_tx,
                            &global_tx,
                            &mut metrics,
                            downlink_flush_max_bytes,
                            downlink_backpressure,
                            runtime_config.tun_mtu,
                            &mut downlink_egress_drop_debt,
                            &mut tcp_loop_flush_tx_calls,
                            &mut tcp_loop_flush_tx_failures,
                            &mut tun_rx_drain_diag,
                            tun_rx_budget,
                            mode.source(),
                            if buffered_downlink.enabled {
                                tun_egress_feedback.is_paused()
                            } else {
                                downlink_rx_paused || tun_egress_feedback.is_paused()
                            },
                            buffered_downlink,
                        )
                        .await;
                        if drained >= tun_rx_budget
                            && deferred_ack_drain.arm_after_budget_exhausted()
                        {
                            deferred_ack_drain_sleep.as_mut().reset(
                                tokio::time::Instant::now()
                                    + std::time::Duration::from_millis(DEFERRED_ACK_DRAIN_DELAY_MS),
                            );
                        } else if matches!(mode, DeferredAckDrainMode::ActiveFlow)
                            && deferred_ack_drain.arm_after_delayed_ack_probe()
                        {
                            deferred_ack_drain_sleep.as_mut().reset(
                                tokio::time::Instant::now()
                                    + std::time::Duration::from_millis(
                                        DEFERRED_ACK_DRAIN_DELAYED_PROBE_MS,
                                    ),
                            );
                        }
                    }
                }
            }
            // 分支 1: 全局回信通道接收到了新数据包
            // 分支 1: 物理网卡接收到了新数据包
            res = device.wait_for_rx() =>{
                metrics.loop_park_end();
                if res.is_ok(){
                    process_ready_tun_rx_packet(
                        &mut device,
                        &mut assoc_table,
                        &mut fake_pool,
                        &upstream,
                        udp_clock.elapsed().as_secs(),
                        &metrics_handle,
                        &mut registry,
                        &mut sockets,
                        &mut socket_ctxs,
                        &mut iface,
                        &mut dirty,
                        &handshake_done_tx,
                        &global_tx,
                        &mut metrics,
                        downlink_flush_max_bytes,
                        downlink_backpressure,
                        runtime_config.tun_mtu,
                        &mut downlink_egress_drop_debt,
                        &mut tcp_loop_flush_tx_calls,
                        &mut tcp_loop_flush_tx_failures,
                        if buffered_downlink.enabled {
                            tun_egress_feedback.is_paused()
                        } else {
                            downlink_rx_paused || tun_egress_feedback.is_paused()
                        },
                        buffered_downlink,
                        "inbound_poll",
                    )
                    .await;
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
                reap_dead_slots(
                    &registry,
                    &mut sockets,
                    &mut socket_ctxs,
                    &mut fake_pool,
                    now,
                    downlink_backpressure,
                    downlink_egress_drop_debt.has_active_drop_debt()
                        || tun_egress_feedback.is_paused(),
                );
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
                let tcp_downlink = tcp_downlink_aggregate(socket_ctxs.values());
                tcp_diag_log!(
                    "{}",
                    format_tcp_downlink_flush_diag(&tcp_downlink, dirty.len())
                );
                tcp_diag_log!("{}", format_tun_rx_drain_diag(&tun_rx_drain_diag));
                // 刀12：紧挨 📊 行打 🔬 主循环归因行（profiler 关闭时 NoopSink::report 空、零开销）。
                metrics.report();
            }
            // Knife14bf：TUN egress drop feedback。诊断日志仍受 MINI_VPN_TCP_DIAG 门控，
            // 但反馈状态每秒更新，避免 30s probe 结束后才发现 clean-window qdisc drops。
            _ = tun_egress_feedback_tick.tick() => {
                metrics.loop_park_end();
                let downlink_stats_raw = downlink_pressure_stats(&dirty, &socket_ctxs, &sockets);
                let pressure_now = std::time::Instant::now();
                tun_egress_feedback.expire_egress_pressure_hold(pressure_now);
                let tcp_downlink = tcp_downlink_aggregate(socket_ctxs.values());
                let tun_sample = tun_egress_drop_sampler.sample();
                let feedback_event =
                    tun_egress_feedback.update(&tun_sample, downlink_stats_raw, downlink_backpressure);
                if matches!(
                    feedback_event,
                    Some(TunEgressFeedbackEvent {
                        reason: TunEgressFeedbackReason::DropDelta,
                        ..
                    })
                ) {
                    downlink_egress_drop_debt.note_tun_drop(downlink_backpressure);
                }
                tun_egress_feedback.observe_pressure_at(
                    downlink_stats_raw,
                    downlink_backpressure,
                    pressure_now,
                );
                let sampled_global_rx_paused =
                    if buffered_downlink.enabled || bounded_global_rx_receive_window {
                    global_rx_receive_paused
                } else {
                    downlink_rx_paused || tun_egress_feedback.is_paused()
                };
                if tcp_diag_enabled() {
                    println!(
                        "{}",
                        format_tun_egress_diag(
                            tun_egress_drop_sampler.interface_label(),
                            &tun_sample,
                            &tcp_downlink,
                            dirty.len(),
                            sampled_global_rx_paused,
                        )
                    );
                    if let Some(event) = feedback_event {
                        println!(
                            "{}",
                            format_tun_egress_feedback_diag(
                                event,
                                &tun_egress_feedback,
                                downlink_backpressure,
                                downlink_egress_drop_debt,
                            )
                        );
                    }
                }
            }
            // 分支 2: 时钟滴答，处理超时重传等后台任务
            _ = timer.tick() =>{
                metrics.loop_park_end();
                downlink_egress_pacer.on_timer_tick();
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

                let timer_now = std::time::Instant::now();
                let has_recent_active_flow = match active_flow_tun_rx_drain_until {
                    Some(deadline) if timer_now <= deadline => true,
                    Some(_) => {
                        active_flow_tun_rx_drain_until = None;
                        false
                    }
                    None => false,
                };
                let dirty_downlink_stats = downlink_pressure_stats(&dirty, &socket_ctxs, &sockets);
                let dirty_close_drain =
                    has_dirty_close_drain(&dirty, &socket_ctxs, downlink_backpressure);
                let maintenance_tun_rx_budget = tun_rx_drain_budget_for_dirty_pressure(
                    !dirty.is_empty(),
                    dirty_close_drain,
                    dirty_downlink_stats,
                    tun_rx_drain_budget,
                    downlink_backpressure,
                    runtime_config.tun_mtu,
                );
                let (timer_tun_rx_budget, timer_tun_rx_source) = if maintenance_tun_rx_budget > 0 {
                    (
                        maintenance_tun_rx_budget,
                        TUN_RX_DRAIN_SOURCE_TIMER_PRESSURE,
                    )
                } else {
                    (
                        tun_rx_drain_budget_for_recent_active_flow(
                            has_recent_active_flow,
                            tun_rx_active_flow_timer_ms,
                            dirty_downlink_stats,
                            tun_egress_feedback.is_paused(),
                            tun_rx_drain_budget,
                            downlink_backpressure,
                            runtime_config.tun_mtu,
                        ),
                        TUN_RX_DRAIN_SOURCE_TIMER_ACTIVE_FLOW,
                    )
                };
                if timer_tun_rx_budget > 0 {
                    drain_ready_tun_rx(
                        &mut device,
                        &mut assoc_table,
                        &mut fake_pool,
                        &upstream,
                        udp_clock.elapsed().as_secs(),
                        &metrics_handle,
                        &mut registry,
                        &mut sockets,
                        &mut socket_ctxs,
                        &mut iface,
                        &mut dirty,
                        &handshake_done_tx,
                        &global_tx,
                        &mut metrics,
                        downlink_flush_max_bytes,
                        downlink_backpressure,
                        runtime_config.tun_mtu,
                        &mut downlink_egress_drop_debt,
                        &mut tcp_loop_flush_tx_calls,
                        &mut tcp_loop_flush_tx_failures,
                        &mut tun_rx_drain_diag,
                        timer_tun_rx_budget,
                        timer_tun_rx_source,
                        if buffered_downlink.enabled {
                            tun_egress_feedback.is_paused()
                        } else {
                            downlink_rx_paused || tun_egress_feedback.is_paused()
                        },
                        buffered_downlink,
                    )
                    .await;
                }

                // #1：timer tick 无新 inbound 包，只续推进脏集合（主要是下行 pending flush +
                // smoltcp 超时重传释放 tx buffer 后继续写）。不再全量 sweep。
                let close_egress_guard = tun_egress_feedback.is_paused()
                    || downlink_egress_drop_debt.has_active_drop_debt();
                let relay_read_recovery_active = downlink_egress_drop_debt.has_active_drop_debt();
                let relay_read_hard_pause = if buffered_downlink.enabled {
                    tun_egress_feedback.is_paused()
                } else {
                    downlink_rx_paused || tun_egress_feedback.is_paused()
                };
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
                    downlink_flush_max_bytes,
                    downlink_backpressure,
                    runtime_config.tun_mtu,
                    &mut downlink_egress_drop_debt,
                    close_egress_guard,
                    relay_read_recovery_active,
                    relay_read_hard_pause,
                    buffered_downlink,
                )
                .await;
            }
        }
        // 刀12：循环底部——即将停在 select! 空等下一个事件，标记 park 开始
        //（NoopSink::loop_park_begin 空、零开销；仅 LoopProfiler 采时钟）。
        metrics.loop_park_begin();
    }
}

#[allow(clippy::too_many_arguments)]
async fn process_ready_tun_rx_packet<D, U, M>(
    device: &mut D,
    assoc_table: &mut AssocTable,
    fake_pool: &mut FakeIpPool,
    upstream: &Arc<U>,
    now_secs: u64,
    metrics_handle: &Metrics,
    registry: &mut ListenerRegistry,
    sockets: &mut SocketSet<'static>,
    socket_ctxs: &mut HashMap<SocketHandle, SocketCtx>,
    iface: &mut Interface,
    dirty: &mut HashSet<SocketHandle>,
    handshake_done_tx: &mpsc::Sender<HandshakeDone>,
    global_tx: &mpsc::Sender<(SocketHandle, RelayEvent)>,
    metrics: &mut M,
    downlink_flush_max_bytes: usize,
    downlink_backpressure: DownlinkBackpressureConfig,
    tun_mtu: usize,
    downlink_egress_drop_debt: &mut DownlinkEgressDropDebt,
    tcp_loop_flush_tx_calls: &mut u64,
    tcp_loop_flush_tx_failures: &mut u64,
    relay_read_hard_pause: bool,
    buffered_downlink: BufferedDownlinkConfig,
    poll_stage: &str,
) -> TunRxPacketKind
where
    D: TunIo,
    U: ProxyUpstream + DatagramUpstream + 'static,
    M: MetricsSink,
{
    // rx 分流（stage-12 D1 + 刀5）：任意 :53 → 裸包 DNS 劫持；其它 UDP → 裸 relay；
    // 非 UDP → 既有 smoltcp 路径。前两类 take 走、不进 iface.poll。
    let class = device.rx_peek().map(classify_inbound);
    if class == Some(Inbound::UdpRelay) {
        if let Some(pkt) = device.rx_take() {
            handle_tuic_udp_uplink(&pkt, assoc_table, fake_pool, upstream.as_ref(), now_secs).await;
        }
        return TunRxPacketKind::Udp;
    }
    if class == Some(Inbound::Dns) {
        if let Some(pkt) = device.rx_take() {
            handle_dns_hijack(&pkt, fake_pool, device, now_secs, metrics_handle).await;
        }
        return TunRxPacketKind::Dns;
    }

    // 1) SYN inspector + 脏集合标脏：在 iface.poll 之前看一眼包。
    //    - 干净 SYN 去往新端口 → 立刻建监听池，smoltcp 同一帧就能 accept。
    //    - 任意去往拦截端口的 TCP 包 → 把该端口 pool 标脏（#1）。
    if let Some(buf) = device.rx_peek()
        && let Some((port, is_clean_syn)) = inspect_inbound_tcp(buf)
    {
        if is_clean_syn {
            if let Err(e) = registry.ensure_port(port, sockets, socket_ctxs) {
                println!(
                    "⚠️ intercepted port cap reached, drop SYN to port {port}: {:?}",
                    e
                );
            }
            if let Err(e) =
                registry.ensure_spare_listeners(port, MIN_SPARE_LISTENERS, sockets, socket_ctxs)
            {
                println!(
                    "⚠️ global listener cap reached, 端口 {port} 无法弹性扩容: {:?}",
                    e
                );
            }
        }
        for &h in registry.handles_for_port(port) {
            dirty.insert(h);
        }
    }

    metrics.enter_poll();
    let timestamp = smoltcp::time::Instant::now();
    iface.poll(timestamp, device, sockets);
    *tcp_loop_flush_tx_calls = tcp_loop_flush_tx_calls.saturating_add(1);
    if let Err(e) = device.flush_tx().await {
        *tcp_loop_flush_tx_failures = tcp_loop_flush_tx_failures.saturating_add(1);
        tcp_diag_log!(
            "🔎 tcp-loop-flush-tx-fail stage={} calls={} failures={} err={e}",
            poll_stage,
            *tcp_loop_flush_tx_calls,
            *tcp_loop_flush_tx_failures
        );
    }
    metrics.leave_poll();

    let close_egress_guard = downlink_egress_drop_debt.has_active_drop_debt();
    let relay_read_recovery_active = downlink_egress_drop_debt.has_active_drop_debt();
    process_dirty_relay(
        dirty,
        sockets,
        socket_ctxs,
        upstream,
        handshake_done_tx,
        global_tx,
        fake_pool,
        now_secs,
        metrics_handle,
        metrics,
        downlink_flush_max_bytes,
        downlink_backpressure,
        tun_mtu,
        downlink_egress_drop_debt,
        close_egress_guard,
        relay_read_recovery_active,
        relay_read_hard_pause,
        buffered_downlink,
    )
    .await;

    TunRxPacketKind::Tcp
}

#[allow(clippy::too_many_arguments)]
async fn drain_ready_tun_rx<D, U, M>(
    device: &mut D,
    assoc_table: &mut AssocTable,
    fake_pool: &mut FakeIpPool,
    upstream: &Arc<U>,
    now_secs: u64,
    metrics_handle: &Metrics,
    registry: &mut ListenerRegistry,
    sockets: &mut SocketSet<'static>,
    socket_ctxs: &mut HashMap<SocketHandle, SocketCtx>,
    iface: &mut Interface,
    dirty: &mut HashSet<SocketHandle>,
    handshake_done_tx: &mpsc::Sender<HandshakeDone>,
    global_tx: &mpsc::Sender<(SocketHandle, RelayEvent)>,
    metrics: &mut M,
    downlink_flush_max_bytes: usize,
    downlink_backpressure: DownlinkBackpressureConfig,
    tun_mtu: usize,
    downlink_egress_drop_debt: &mut DownlinkEgressDropDebt,
    tcp_loop_flush_tx_calls: &mut u64,
    tcp_loop_flush_tx_failures: &mut u64,
    diag: &mut TunRxDrainDiag,
    budget: usize,
    source: &str,
    relay_read_hard_pause: bool,
    buffered_downlink: BufferedDownlinkConfig,
) -> usize
where
    D: TunIo,
    U: ProxyUpstream + DatagramUpstream + 'static,
    M: MetricsSink,
{
    if budget == 0 {
        return 0;
    }

    diag.note_attempt(source);
    let mut drained = 0usize;
    while drained < budget {
        match device.try_recv_rx() {
            Ok(true) => {
                let kind = process_ready_tun_rx_packet(
                    device,
                    assoc_table,
                    fake_pool,
                    upstream,
                    now_secs,
                    metrics_handle,
                    registry,
                    sockets,
                    socket_ctxs,
                    iface,
                    dirty,
                    handshake_done_tx,
                    global_tx,
                    metrics,
                    downlink_flush_max_bytes,
                    downlink_backpressure,
                    tun_mtu,
                    downlink_egress_drop_debt,
                    tcp_loop_flush_tx_calls,
                    tcp_loop_flush_tx_failures,
                    relay_read_hard_pause,
                    buffered_downlink,
                    "tun_rx_drain",
                )
                .await;
                diag.note_packet(kind);
                drained += 1;
            }
            Ok(false) => {
                diag.note_would_block();
                return drained;
            }
            Err(e) => {
                diag.note_error();
                tcp_diag_log!(
                    "🔎 tcp-tun-rx-drain-error source={} drained={} err={e}",
                    source,
                    drained
                );
                return drained;
            }
        }
    }
    diag.note_budget_exhausted();
    drained
}

/// #1 脏集合驱动的 relay 调度段：只处理本 tick 标脏的 handle，替代每 tick 全量 `all_handles()`。
///
/// 中文要点：把 relay 段成本从 O(总 listener 槽数) 降到 O(活跃 handle)。处理完一个 handle 后，
/// 若它无下行 pending、smoltcp 侧不再 `can_recv`、tx queue 已降到 low watermark、且没有本地 FIN
/// 待发（首包已 drain、已开远端进 Relaying），就出脏集合——后续回程走 `global_rx` 分支，残留
/// pending、tx queue pressure 或 FIN 重试时会留脏。仍有活就留在集合里下个 tick 续处理。
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
    downlink_flush_max_bytes: usize,
    downlink_backpressure: DownlinkBackpressureConfig,
    tun_mtu: usize,
    downlink_egress_drop_debt: &mut DownlinkEgressDropDebt,
    close_egress_drop_guard: bool,
    relay_read_recovery_active: bool,
    relay_read_hard_pause: bool,
    buffered_downlink: BufferedDownlinkConfig,
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
            downlink_flush_max_bytes,
            downlink_backpressure,
            tun_mtu,
            downlink_egress_drop_debt,
            close_egress_drop_guard,
        )
        .await
        {
            trace_log!("处理本地房间 {:?} 失败: {e}", handle);
        }
        let still_active = {
            let (has_recv, snapshot) = {
                let socket = sockets.get_mut::<TcpSocket>(handle);
                (socket.can_recv(), SocketCloseSnapshot::from_socket(socket))
            };
            let (has_pending, needs_local_finish) = socket_ctxs
                .get_mut(&handle)
                .map(|c| {
                    log_tcp_lifecycle_observation(handle, c, snapshot, now_secs, "dirty_relay");
                    (
                        !c.downlink_pending.is_empty(),
                        snapshot.tcp_state == TcpState::CloseWait
                            && c.uplink_tx.is_some()
                            && !c.local_fin_sent,
                    )
                })
                .unwrap_or((false, false));
            has_recv
                || has_pending
                || needs_local_finish
                || snapshot.send_queue > downlink_backpressure.low_bytes
        };
        let current_global_pending =
            downlink_pressure_stats(dirty, socket_ctxs, sockets).total_pending;
        publish_relay_read_credit_for_handle(
            handle,
            sockets,
            socket_ctxs,
            downlink_backpressure,
            tun_mtu,
            downlink_flush_max_bytes,
            relay_read_recovery_active,
            relay_read_hard_pause,
            buffered_downlink,
            current_global_pending,
        );
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
/// - `!is_active()`：Closed / TimeWait（RST、双向关闭完成）；**但**若仍有
///   `downlink_pending`，先按最近一次 pending 观察/接受进展给 dirty flush 一个短 grace 窗口，
///   无进展过期才 hard reap。此例外覆盖 Knife14n/14s 看到的 `state=Closing/Relaying pending>0`
///   被 sweep 抢先清尾包，且仍保持 bounded lifecycle。也覆盖 `HandshakePending` 槽
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
    downlink_backpressure: DownlinkBackpressureConfig,
    close_egress_drop_guard: bool,
) -> usize {
    let handles: Vec<SocketHandle> = registry.all_handles().collect();
    let mut reaped = 0;
    for h in handles {
        let dead = {
            let s = sockets.get::<TcpSocket>(h);
            let snapshot = SocketCloseSnapshot::from_socket(s);
            socket_ctxs.get_mut(&h).and_then(|ctx| {
                let close_egress_guard =
                    close_egress_drop_guard || ctx_has_close_egress_pressure_evidence(ctx);
                let should_reap = should_reap_slot_with_snapshot(
                    ctx,
                    snapshot,
                    now_secs,
                    downlink_backpressure,
                    close_egress_guard,
                );
                let source = if should_reap {
                    "dead_slot_reap"
                } else {
                    "dead_slot_scan"
                };
                log_tcp_lifecycle_observation(h, ctx, snapshot, now_secs, source);
                should_reap.then_some(snapshot)
            })
        };
        if let Some(snapshot) = dead {
            let sock = sockets.get_mut::<TcpSocket>(h);
            if let Some(ctx) = socket_ctxs.get_mut(&h) {
                rearm_socket_with_reason_and_snapshot(
                    h,
                    sock,
                    ctx,
                    fake_pool,
                    now_secs,
                    "local",
                    "dead_slot_reap",
                    Some(snapshot),
                );
                reaped += 1;
            }
        }
    }
    reaped
}

fn should_reap_slot_with_snapshot(
    ctx: &mut SocketCtx,
    snapshot: SocketCloseSnapshot,
    now_secs: u64,
    _cfg: DownlinkBackpressureConfig,
    _close_egress_drop_guard: bool,
) -> bool {
    if ctx.pending_relay_close.is_some() && snapshot.active && snapshot.can_send {
        note_deferred_close_egress_progress(ctx, snapshot.send_queue, now_secs);
        return false;
    }
    should_reap_slot(
        ctx,
        snapshot.tcp_state,
        snapshot.active,
        snapshot.can_send,
        now_secs,
    )
}

fn should_reap_slot(
    ctx: &SocketCtx,
    tcp_state: TcpState,
    active: bool,
    can_send: bool,
    now_secs: u64,
) -> bool {
    if ctx.state == SocketState::Listening {
        return false;
    }
    if !ctx.downlink_pending.is_empty() {
        if active {
            return false;
        }
        if !can_send {
            return true;
        }
        let Some(deadline_base) = ctx
            .pending_relay_close_since_secs
            .max(ctx.pending_relay_close_last_progress_secs)
            .max(ctx.downlink_pending_last_progress_secs)
        else {
            return true;
        };
        return now_secs.saturating_sub(deadline_base) >= DEFERRED_CLOSE_PENDING_GRACE_SECS;
    }
    if ctx.pending_relay_close.is_some() && active && can_send {
        return false;
    }
    if !active {
        return true;
    }
    if tcp_state == TcpState::CloseWait {
        return ctx.uplink_tx.is_none() && !ctx.local_fin_sent;
    }
    ctx.state == SocketState::OpeningRemote && ctx.uplink_tx.is_none()
}

fn relay_allows_remote_payload(ctx: &SocketCtx) -> bool {
    ctx.uplink_tx.is_some() || ctx.local_fin_sent
}

fn should_start_deferred_close_for_egress(
    snapshot: SocketCloseSnapshot,
    cfg: DownlinkBackpressureConfig,
) -> bool {
    snapshot.active && snapshot.can_send && snapshot.send_queue > cfg.low_bytes
}

fn start_deferred_close_for_egress_if_needed(
    ctx: &mut SocketCtx,
    close: RelayClose,
    snapshot: SocketCloseSnapshot,
    cfg: DownlinkBackpressureConfig,
    now_secs: u64,
) -> bool {
    if !ctx.downlink_pending.is_empty() || !should_start_deferred_close_for_egress(snapshot, cfg) {
        return false;
    }

    ctx.state = SocketState::Closing;
    ctx.uplink_tx = None;
    ctx.pending_relay_close = Some(close);
    ctx.pending_relay_close_since_secs = Some(now_secs);
    ctx.pending_relay_close_last_progress_secs = None;
    ctx.pending_relay_close_last_pending_bytes = 0;
    ctx.pending_relay_close_last_egress_progress_secs = Some(now_secs);
    ctx.pending_relay_close_last_egress_queue_bytes = snapshot.send_queue;
    true
}

fn start_deferred_close_for_pending_downlink(
    ctx: &mut SocketCtx,
    close: RelayClose,
    now_secs: u64,
) -> bool {
    if ctx.downlink_pending.is_empty() {
        return false;
    }

    ctx.state = SocketState::Closing;
    ctx.uplink_tx = None;
    ctx.pending_relay_close = Some(close);
    ctx.pending_relay_close_since_secs = Some(now_secs);
    ctx.pending_relay_close_last_progress_secs = Some(now_secs);
    ctx.pending_relay_close_last_pending_bytes = ctx.downlink_pending.len();
    ctx.downlink_pending_last_progress_secs = Some(now_secs);
    ctx.downlink_pending_last_pending_bytes = ctx.downlink_pending.len();
    true
}

fn deferred_close_egress_deadline_base(ctx: &SocketCtx) -> Option<u64> {
    ctx.pending_relay_close_since_secs
        .max(ctx.pending_relay_close_last_egress_progress_secs)
}

fn ctx_has_close_egress_pressure_evidence(ctx: &SocketCtx) -> bool {
    ctx.downlink_diag.tun_flush_deferred > 0
        || ctx.downlink_diag.send_slice_drop_credit_debt_bytes > 0
        || ctx.downlink_diag.send_slice_pressure_credit_debt_bytes > 0
        || ctx.downlink_diag.send_slice_drop_credit_blocked_bytes > 0
        || ctx.downlink_diag.send_slice_pressure_credit_blocked_bytes > 0
}

fn deferred_close_egress_grace_secs(close_egress_drop_guard: bool) -> u64 {
    if close_egress_drop_guard {
        DEFERRED_CLOSE_EGRESS_DROP_GRACE_SECS
    } else {
        DEFERRED_CLOSE_PENDING_GRACE_SECS
    }
}

fn note_deferred_close_egress_progress(ctx: &mut SocketCtx, send_queue: usize, now_secs: u64) {
    if send_queue == 0 {
        ctx.pending_relay_close_last_egress_progress_secs = None;
        ctx.pending_relay_close_last_egress_queue_bytes = 0;
        return;
    }
    let last_queue = ctx.pending_relay_close_last_egress_queue_bytes;
    if last_queue == 0 || send_queue < last_queue {
        ctx.pending_relay_close_last_egress_queue_bytes = send_queue;
        ctx.pending_relay_close_last_egress_progress_secs = Some(now_secs);
    } else if send_queue > last_queue {
        ctx.pending_relay_close_last_egress_queue_bytes = send_queue;
    }
}

fn should_keep_deferred_close_for_egress(
    ctx: &mut SocketCtx,
    snapshot: SocketCloseSnapshot,
    cfg: DownlinkBackpressureConfig,
    now_secs: u64,
    close_egress_drop_guard: bool,
) -> bool {
    if ctx.pending_relay_close.is_none()
        || !snapshot.active
        || !snapshot.can_send
        || snapshot.send_queue <= cfg.low_bytes
    {
        return false;
    }
    note_deferred_close_egress_progress(ctx, snapshot.send_queue, now_secs);
    let Some(deadline_base) = deferred_close_egress_deadline_base(ctx) else {
        return false;
    };
    let Some(started_secs) = ctx.pending_relay_close_since_secs else {
        return false;
    };
    if now_secs.saturating_sub(started_secs) >= DEFERRED_CLOSE_EGRESS_MAX_GRACE_SECS {
        return false;
    }
    now_secs.saturating_sub(deadline_base)
        < deferred_close_egress_grace_secs(close_egress_drop_guard)
}

fn should_send_local_eof_for_deferred_close(
    ctx: &SocketCtx,
    snapshot: SocketCloseSnapshot,
) -> bool {
    ctx.pending_relay_close.is_some()
        && ctx.downlink_pending.is_empty()
        && !ctx.local_eof_sent
        && snapshot.active
        && snapshot.can_send
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RemotePayloadDisposition {
    Accept,
    StaleRelayReset,
    TerminalNoSend,
}

fn remote_payload_disposition(
    ctx: &SocketCtx,
    snapshot: SocketCloseSnapshot,
) -> RemotePayloadDisposition {
    if snapshot.tcp_state == TcpState::Closed && !snapshot.active && !snapshot.can_send {
        return RemotePayloadDisposition::TerminalNoSend;
    }
    if relay_allows_remote_payload(ctx) {
        RemotePayloadDisposition::Accept
    } else {
        RemotePayloadDisposition::StaleRelayReset
    }
}

/// Allocate a fresh smoltcp TCP listener socket for one pool slot.
/// 中文要点：每次调用都创建一间独立房间，并立即挂上 listen 牌子。
#[cfg(test)]
fn build_listener_socket(spec: &ListenerSpec) -> TcpSocket<'static> {
    build_listener_socket_with_buffers(spec, TcpSocketBufferConfig::default())
}

fn build_listener_socket_with_buffers(
    spec: &ListenerSpec,
    buffers: TcpSocketBufferConfig,
) -> TcpSocket<'static> {
    let tcp_rx_buffer = TcpSocketBuffer::new(vec![0; buffers.rx_bytes]);
    let tcp_tx_buffer = TcpSocketBuffer::new(vec![0; buffers.tx_bytes]);
    let mut tcp_socket = TcpSocket::new(tcp_rx_buffer, tcp_tx_buffer);
    configure_local_tcp_socket(&mut tcp_socket);
    tcp_socket.listen(spec.local_port).unwrap();
    tcp_socket
}

fn configure_local_tcp_socket(socket: &mut TcpSocket<'_>) {
    socket.set_nagle_enabled(false);
    socket.set_ack_delay(None);
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
fn forge_dns_reply(
    udp: &UdpInbound<'_>,
    fake_pool: &mut FakeIpPool,
    now_secs: u64,
) -> Option<Vec<u8>> {
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

enum EstablishedUplink {
    NotEstablished,
    Handled,
    Closed,
}

fn pump_established_uplink(
    ctx: &mut SocketCtx,
    tcp_state: TcpState,
    now_secs: u64,
    mut next_payload: impl FnMut() -> Option<Vec<u8>>,
) -> EstablishedUplink {
    let Some(tx) = ctx.uplink_tx.as_mut() else {
        return EstablishedUplink::NotEstablished;
    };

    for _ in 0..MAX_ESTABLISHED_UPLINK_BATCH {
        match tx.try_reserve() {
            Ok(permit) => {
                if let Some(payload) = next_payload() {
                    permit.send(RelayCommand::Data(payload));
                    ctx.state = SocketState::Relaying;
                } else {
                    if tcp_state == TcpState::CloseWait && !ctx.local_fin_sent {
                        let pending_since =
                            *ctx.local_fin_pending_since_secs.get_or_insert(now_secs);
                        let deadline_base = ctx
                            .local_fin_last_remote_progress_secs
                            .unwrap_or(pending_since)
                            .max(pending_since);
                        if now_secs.saturating_sub(deadline_base) >= LOCAL_FINISH_DEFER_SECS {
                            permit.send(RelayCommand::Finish);
                            ctx.local_fin_sent = true;
                            ctx.local_fin_pending_since_secs = None;
                            ctx.local_fin_last_remote_progress_secs = None;
                        }
                    }
                    return EstablishedUplink::Handled;
                }
            }
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                return EstablishedUplink::Handled;
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                return EstablishedUplink::Closed;
            }
        }
    }

    EstablishedUplink::Handled
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
    let snapshot = SocketCloseSnapshot::from_socket(socket);
    rearm_socket_with_reason_and_snapshot(
        handle,
        socket,
        ctx,
        fake_pool,
        now_secs,
        close_direction,
        close_reason,
        Some(snapshot),
    );
}

#[allow(clippy::too_many_arguments)]
fn rearm_socket_with_reason_and_snapshot(
    handle: SocketHandle,
    socket: &mut TcpSocket<'_>,
    ctx: &mut SocketCtx,
    fake_pool: &mut FakeIpPool,
    now_secs: u64,
    close_direction: &'static str,
    close_reason: &'static str,
    close_snapshot: Option<SocketCloseSnapshot>,
) {
    if let Some(snapshot) = close_snapshot {
        log_tcp_lifecycle_observation(handle, ctx, snapshot, now_secs, close_reason);
        let close_pending = close_pending_accounting(ctx, snapshot);
        let close_egress = close_egress_accounting(snapshot);
        tcp_diag_log!(
            "🔎 tcp-handle-close handle={:?} direction={} reason={} state={:?} pending={} pending_high={} remote_to_global_rx_bytes={} terminal_late_remote_payload_bytes={} terminal_late_remote_payload_events={} flush_attempts={} no_send_capacity={} send_window_samples={} send_capacity_min={} send_capacity_max={} send_queue_max={} recv_queue_max={} may_send_false={} may_recv_false={} no_send_capacity_streak_max={} no_send_capacity_pending_max={} send_slice_calls={} send_slice_accepted={} send_slice_zero={} send_slice_errors={} budget_limited_calls={} headroom_limited_calls={} headroom_deferred_bytes={} drain_credit_granted_bytes={} drain_credit_planned_bytes={} drain_credit_used_bytes={} drop_credit_debt_bytes={} drop_credit_debt_paid_bytes={} drop_credit_blocked_bytes={} pressure_credit_debt_bytes={} pressure_credit_debt_paid_bytes={} pressure_credit_blocked_bytes={} hard_edge_guard_bytes={} hard_edge_guard_limited_calls={} hard_edge_guard_deferred_bytes={} send_slice_max_accepted={} tun_flush_tx_calls={} tun_flush_tx_failures={} tun_flush_deferred={} close_pending_class={} close_pending_bytes={} terminal_pending_reap_bytes={} close_egress_class={} close_egress_bytes={} close_egress_drain_candidate={} tcp_state={:?} active={} can_send={} can_recv={} may_send={} may_recv={} send_capacity={} send_queue={} recv_queue={}",
            handle,
            close_direction,
            close_reason,
            ctx.state,
            ctx.downlink_pending.len(),
            ctx.downlink_diag.downlink_pending_high_water,
            ctx.downlink_diag.remote_to_global_rx_bytes,
            ctx.downlink_diag.terminal_late_remote_payload_bytes,
            ctx.downlink_diag.terminal_late_remote_payload_events,
            ctx.downlink_diag.flush_attempts,
            ctx.downlink_diag.send_slice_no_capacity,
            ctx.downlink_diag.send_window_samples,
            ctx.downlink_diag.send_capacity_min,
            ctx.downlink_diag.send_capacity_max,
            ctx.downlink_diag.send_queue_max,
            ctx.downlink_diag.recv_queue_max,
            ctx.downlink_diag.may_send_false,
            ctx.downlink_diag.may_recv_false,
            ctx.downlink_diag.no_send_capacity_streak_max,
            ctx.downlink_diag.no_send_capacity_pending_max,
            ctx.downlink_diag.send_slice_calls,
            ctx.downlink_diag.send_slice_accepted_bytes,
            ctx.downlink_diag.send_slice_zero,
            ctx.downlink_diag.send_slice_errors,
            ctx.downlink_diag.send_slice_budget_limited,
            ctx.downlink_diag.send_slice_headroom_limited,
            ctx.downlink_diag.send_slice_headroom_deferred_bytes,
            ctx.downlink_diag.send_slice_drain_credit_granted_bytes,
            ctx.downlink_diag.send_slice_drain_credit_planned_bytes,
            ctx.downlink_diag.send_slice_drain_credit_used_bytes,
            ctx.downlink_diag.send_slice_drop_credit_debt_bytes,
            ctx.downlink_diag.send_slice_drop_credit_debt_paid_bytes,
            ctx.downlink_diag.send_slice_drop_credit_blocked_bytes,
            ctx.downlink_diag.send_slice_pressure_credit_debt_bytes,
            ctx.downlink_diag.send_slice_pressure_credit_debt_paid_bytes,
            ctx.downlink_diag.send_slice_pressure_credit_blocked_bytes,
            ctx.downlink_diag.send_slice_hard_edge_guard_bytes,
            ctx.downlink_diag.send_slice_hard_edge_guard_limited,
            ctx.downlink_diag.send_slice_hard_edge_guard_deferred_bytes,
            ctx.downlink_diag.send_slice_max_accepted_bytes,
            ctx.downlink_diag.tun_flush_tx_calls,
            ctx.downlink_diag.tun_flush_tx_failures,
            ctx.downlink_diag.tun_flush_deferred,
            close_pending.class.as_str(),
            close_pending.pending_bytes,
            close_pending.terminal_pending_reap_bytes,
            close_egress.class.as_str(),
            close_egress.bytes,
            close_egress.drain_candidate,
            snapshot.tcp_state,
            snapshot.active,
            snapshot.can_send,
            snapshot.can_recv,
            snapshot.may_send,
            snapshot.may_recv,
            snapshot.send_capacity,
            snapshot.send_queue,
            snapshot.recv_queue
        );
        rearm_socket(socket, ctx, fake_pool, now_secs);
        return;
    }

    let close_pending_bytes = ctx.downlink_pending.len();
    let close_pending_class = if close_pending_bytes == 0 {
        ClosePendingClass::NoPending
    } else {
        ClosePendingClass::Unknown
    };
    tcp_diag_log!(
        "🔎 tcp-handle-close handle={:?} direction={} reason={} state={:?} pending={} pending_high={} remote_to_global_rx_bytes={} terminal_late_remote_payload_bytes={} terminal_late_remote_payload_events={} flush_attempts={} no_send_capacity={} send_window_samples={} send_capacity_min={} send_capacity_max={} send_queue_max={} recv_queue_max={} may_send_false={} may_recv_false={} no_send_capacity_streak_max={} no_send_capacity_pending_max={} send_slice_calls={} send_slice_accepted={} send_slice_zero={} send_slice_errors={} budget_limited_calls={} headroom_limited_calls={} headroom_deferred_bytes={} drain_credit_granted_bytes={} drain_credit_planned_bytes={} drain_credit_used_bytes={} drop_credit_debt_bytes={} drop_credit_debt_paid_bytes={} drop_credit_blocked_bytes={} pressure_credit_debt_bytes={} pressure_credit_debt_paid_bytes={} pressure_credit_blocked_bytes={} hard_edge_guard_bytes={} hard_edge_guard_limited_calls={} hard_edge_guard_deferred_bytes={} send_slice_max_accepted={} tun_flush_tx_calls={} tun_flush_tx_failures={} tun_flush_deferred={} close_pending_class={} close_pending_bytes={} terminal_pending_reap_bytes=0 close_egress_class=unknown close_egress_bytes=0 close_egress_drain_candidate=false",
        handle,
        close_direction,
        close_reason,
        ctx.state,
        ctx.downlink_pending.len(),
        ctx.downlink_diag.downlink_pending_high_water,
        ctx.downlink_diag.remote_to_global_rx_bytes,
        ctx.downlink_diag.terminal_late_remote_payload_bytes,
        ctx.downlink_diag.terminal_late_remote_payload_events,
        ctx.downlink_diag.flush_attempts,
        ctx.downlink_diag.send_slice_no_capacity,
        ctx.downlink_diag.send_window_samples,
        ctx.downlink_diag.send_capacity_min,
        ctx.downlink_diag.send_capacity_max,
        ctx.downlink_diag.send_queue_max,
        ctx.downlink_diag.recv_queue_max,
        ctx.downlink_diag.may_send_false,
        ctx.downlink_diag.may_recv_false,
        ctx.downlink_diag.no_send_capacity_streak_max,
        ctx.downlink_diag.no_send_capacity_pending_max,
        ctx.downlink_diag.send_slice_calls,
        ctx.downlink_diag.send_slice_accepted_bytes,
        ctx.downlink_diag.send_slice_zero,
        ctx.downlink_diag.send_slice_errors,
        ctx.downlink_diag.send_slice_budget_limited,
        ctx.downlink_diag.send_slice_headroom_limited,
        ctx.downlink_diag.send_slice_headroom_deferred_bytes,
        ctx.downlink_diag.send_slice_drain_credit_granted_bytes,
        ctx.downlink_diag.send_slice_drain_credit_planned_bytes,
        ctx.downlink_diag.send_slice_drain_credit_used_bytes,
        ctx.downlink_diag.send_slice_drop_credit_debt_bytes,
        ctx.downlink_diag.send_slice_drop_credit_debt_paid_bytes,
        ctx.downlink_diag.send_slice_drop_credit_blocked_bytes,
        ctx.downlink_diag.send_slice_pressure_credit_debt_bytes,
        ctx.downlink_diag.send_slice_pressure_credit_debt_paid_bytes,
        ctx.downlink_diag.send_slice_pressure_credit_blocked_bytes,
        ctx.downlink_diag.send_slice_hard_edge_guard_bytes,
        ctx.downlink_diag.send_slice_hard_edge_guard_limited,
        ctx.downlink_diag.send_slice_hard_edge_guard_deferred_bytes,
        ctx.downlink_diag.send_slice_max_accepted_bytes,
        ctx.downlink_diag.tun_flush_tx_calls,
        ctx.downlink_diag.tun_flush_tx_failures,
        ctx.downlink_diag.tun_flush_deferred,
        close_pending_class.as_str(),
        close_pending_bytes
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
    ctx.relay_read_credit_tx = None;
    ctx.local_fin_sent = false;
    ctx.local_eof_sent = false;
    ctx.local_fin_pending_since_secs = None;
    ctx.local_fin_last_remote_progress_secs = None;
    ctx.downlink_pending.clear();
    ctx.pending_relay_close = None;
    ctx.pending_relay_close_since_secs = None;
    ctx.pending_relay_close_last_progress_secs = None;
    ctx.pending_relay_close_last_pending_bytes = 0;
    ctx.pending_relay_close_last_egress_progress_secs = None;
    ctx.pending_relay_close_last_egress_queue_bytes = 0;
    ctx.downlink_pending_last_progress_secs = None;
    ctx.downlink_pending_last_pending_bytes = 0;
    ctx.downlink_diag = TcpDownlinkDiag::default();
    ctx.downlink_egress_clock = DownlinkEgressClock::default();
    ctx.downlink_credit_controller = DownlinkCreditController::default();
    ctx.last_tcp_lifecycle = None;
    ctx.reverse_window_last_log_secs = None;
    // 清 async-open 在飞缓存 + bump epoch——让任何迟到 `HandshakeDone` 失配被丢
    // （绝不装到本次 rearm 后的新一代 socket 上，防串话核心）。对非 spawn 槽是无害的纯计数自增。
    ctx.uplink_buffer.clear();
    ctx.conn_epoch = ctx.conn_epoch.wrapping_add(1);
    // 刀2 引用计数：本 flow 占用的 fake-IP 释放（归零后该映射进入可回收候选，sweep 才回收）。
    if let Some(ip) = ctx.fake_ip.take() {
        fake_pool.release(ip, now_secs);
    }
    ctx.state = SocketState::Rearming;
    configure_local_tcp_socket(socket);
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
    downlink_flush_max_bytes: usize,
    downlink_backpressure: DownlinkBackpressureConfig,
    tun_mtu: usize,
    downlink_egress_drop_debt: &mut DownlinkEgressDropDebt,
    close_egress_drop_guard: bool,
) -> Result<(), ClientError> {
    // 每轮先推进该 handle 的下行 pending：TCP ACK 释放 tx buffer 空间后继续写，
    // 直到把上一轮没写完的回程字节全部交付，绝不丢字节（修 bad decrypt 的另一半）。
    {
        let tcp_socket = sockets.get_mut::<TcpSocket>(handle);
        if let Some(ctx) = socket_ctxs.get_mut(&handle) {
            let flush_outcome = flush_downlink(
                handle,
                tcp_socket,
                ctx,
                downlink_flush_max_bytes,
                downlink_backpressure,
                tun_mtu,
                downlink_egress_drop_debt,
            );
            note_downlink_pending_progress(ctx, now_secs, flush_outcome.accepted_bytes);
            if finish_deferred_relay_close_if_drained(
                handle,
                tcp_socket,
                ctx,
                fake_pool,
                now_secs,
                downlink_backpressure,
                close_egress_drop_guard || ctx_has_close_egress_pressure_evidence(ctx),
            ) {
                return Ok(());
            }
        }
    }

    // 刀13 ② / 刀14s：已建立 relay 的上行必须非阻塞。先抢 mpsc permit，再从 smoltcp 取字节；
    // Full 时不 recv、不分配、不打印，让字节留在 smoltcp rx buffer → TCP 窗口自然背压 app。
    // 14s 起每个 dirty pass 抽一批 payload，避免退化成“每 5ms timer 只转发一个 MSS”。
    let established_uplink = {
        let Some(ctx) = socket_ctxs.get_mut(&handle) else {
            return Ok(());
        };
        let tcp_socket = sockets.get_mut::<TcpSocket>(handle);
        let tcp_state = tcp_socket.state();
        pump_established_uplink(ctx, tcp_state, now_secs, || {
            extract_socket_payload(tcp_socket)
        })
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
                let snapshot = SocketCloseSnapshot::from_socket(tcp_socket);
                let close = RelayClose {
                    epoch: ctx.conn_epoch,
                    direction: "local_to_remote",
                    reason: "uplink_channel_closed",
                };
                if start_deferred_close_for_pending_downlink(ctx, close, now_secs) {
                    tcp_diag_log!(
                        "🔎 tcp-deferred-close-pending handle={:?} direction={} reason={} pending={} started_secs={}",
                        handle,
                        close.direction,
                        close.reason,
                        ctx.downlink_pending.len(),
                        now_secs
                    );
                    return Ok(());
                }
                if start_deferred_close_for_egress_if_needed(
                    ctx,
                    close,
                    snapshot,
                    downlink_backpressure,
                    now_secs,
                ) {
                    tcp_diag_log!(
                        "🔎 tcp-deferred-close-egress handle={:?} direction={} reason={} send_queue={} high={} low={} started_secs={}",
                        handle,
                        close.direction,
                        close.reason,
                        snapshot.send_queue,
                        downlink_backpressure.high_bytes,
                        downlink_backpressure.low_bytes,
                        now_secs
                    );
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
                std::net::IpAddr::V4(v4) => fake_pool
                    .resolve(v4)
                    .unwrap_or_else(|| endpoint.addr.to_string()),
                _ => endpoint.addr.to_string(),
            };
            println!(
                "🛡️ 阻断加密 DNS {who} (@{}:{})（→ RST，逼回落明文 DNS）",
                endpoint.addr, endpoint.port
            );
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
        trace_log!(
            "🎯 handle {:?} extracted target {}",
            handle,
            target.to_wire_string()
        );
        trace_log!("🔄 handle {:?} entering {:?}", handle, ctx.state);
        let stream = upstream.open_tcp(&target).await?;
        trace_log!("🚪 handle {:?} remote session opened", handle);

        let (tx, rx) = tokio::sync::mpsc::channel(RELAY_CHANNEL_CAPACITY);
        if let Err(e) = tx.try_send(RelayCommand::Data(payload)) {
            trace_log!(
                "❌ handle {:?} 新建中继通道写入失败({e}) → rearm（不静默丢首包）",
                handle
            );
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
        ctx.relay_read_credit_tx = Some(spawn_remote_relay(
            handle,
            ctx.conn_epoch,
            stream,
            rx,
            global_tx.clone(),
            metrics_handle,
        ));
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
            trace_log!(
                "⚠️ handle {:?} spawn 入口缓存首包失败（{}B），丢弃",
                handle,
                payload.len()
            );
        }
        trace_log!(
            "🎯 handle {:?} target {} → spawn remote open（并发化，不 stall 主循环）",
            handle,
            target.to_wire_string()
        );

        let up = Arc::clone(upstream);
        let done_tx = handshake_done_tx.clone();
        tokio::spawn(async move {
            let result = up.open_tcp(&target).await; // production open_tcp implementations carry their own timeout budget
            // channel 满 → send().await 背压（不丢，等主循环排空）；主循环已退出 → send 失败、忽略。
            let _ = done_tx
                .send(HandshakeDone {
                    handle,
                    epoch,
                    result,
                })
                .await;
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
    let HandshakeDone {
        handle,
        epoch,
        result,
    } = done;
    let Some(ctx) = socket_ctxs.get_mut(&handle) else {
        return; // 槽已不存在：丢弃（Ok 的流随作用域 drop 关闭）
    };
    // 防串话（V2 patch 3）：epoch 比较置于状态检查**之前**。不匹配 = 本槽已 rearm/换代/复位 →
    // 迟到的 open 结果丢弃，绝不装到新一代 socket。Ok 的流随作用域结束 drop 而干净关闭。
    if ctx.conn_epoch != epoch {
        if result.is_ok() {
            trace_log!(
                "🗑️ handle {:?} 迟到 open 结果(epoch {epoch}≠{}) 丢弃，不装到新代 socket",
                handle,
                ctx.conn_epoch
            );
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
                    trace_log!(
                        "❌ handle {:?} flush open 缓存失败({e}) → rearm（不静默丢字节）",
                        handle
                    );
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
            trace_log!(
                "🚪 handle {:?} remote session opened（spawn open 成功）",
                handle
            );
            ctx.relay_read_credit_tx = Some(spawn_remote_relay(
                handle,
                epoch,
                stream,
                rx,
                global_tx.clone(),
                metrics_handle,
            ));
        }
        Err(e) => {
            println!(
                "❌ handle {:?} spawned remote open failed: {e} → rearm",
                handle
            );
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
    downlink_flush_max_bytes: usize,
    downlink_egress_pacer: &mut DownlinkEgressPacer,
    downlink_egress_drop_debt: &mut DownlinkEgressDropDebt,
    downlink_backpressure: DownlinkBackpressureConfig,
    tun_mtu: usize,
    buffered_downlink_enabled: bool,
) -> std::io::Result<usize> {
    let tcp_socket = sockets.get_mut::<TcpSocket>(handle);
    let Some(ctx) = socket_ctxs.get_mut(&handle) else {
        return Ok(0);
    };
    if ctx.conn_epoch != epoch {
        trace_log!(
            "🗑️ handle {:?} 丢弃旧 relay data(epoch {epoch}≠{}) {}B",
            handle,
            ctx.conn_epoch,
            payload.len()
        );
        return Ok(0);
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
        return Ok(0);
    }

    let before_payload_snapshot = SocketCloseSnapshot::from_socket(tcp_socket);
    match remote_payload_disposition(ctx, before_payload_snapshot) {
        RemotePayloadDisposition::Accept => {}
        RemotePayloadDisposition::StaleRelayReset => {
            trace_log!(
                "🗑️ handle {:?} 已复位，丢弃旧连接迟到回程 {} 字节",
                handle,
                payload.len()
            );
            return Ok(0);
        }
        RemotePayloadDisposition::TerminalNoSend => {
            ctx.downlink_diag
                .note_terminal_late_remote_payload(payload.len());
            tcp_diag_log!(
                "🔎 tcp-terminal-remote-payload handle={:?} bytes={} total_bytes={} events={} pending={} tcp_state={:?} active={} can_send={} can_recv={} may_send={} may_recv={} send_capacity={} send_queue={} recv_queue={}",
                handle,
                payload.len(),
                ctx.downlink_diag.terminal_late_remote_payload_bytes,
                ctx.downlink_diag.terminal_late_remote_payload_events,
                ctx.downlink_pending.len(),
                before_payload_snapshot.tcp_state,
                before_payload_snapshot.active,
                before_payload_snapshot.can_send,
                before_payload_snapshot.can_recv,
                before_payload_snapshot.may_send,
                before_payload_snapshot.may_recv,
                before_payload_snapshot.send_capacity,
                before_payload_snapshot.send_queue,
                before_payload_snapshot.recv_queue
            );
            rearm_socket_with_reason_and_snapshot(
                handle,
                tcp_socket,
                ctx,
                fake_pool,
                now_secs,
                "remote_to_local",
                "terminal_remote_payload",
                Some(before_payload_snapshot),
            );
            return Ok(0);
        }
    }

    // 不直接 send_slice（会丢写不下的字节）：先入下行 pending，再尽量 flush；
    // 剩余字节由主循环每轮 poll 持续推进（TCP ACK 释放 buffer 后继续）。
    ctx.downlink_pending.extend_from_slice(&payload);
    ctx.downlink_diag
        .note_remote_payload(payload.len(), ctx.downlink_pending.len());
    note_local_finish_remote_progress(ctx, now_secs);
    let flush_outcome = flush_downlink(
        handle,
        tcp_socket,
        ctx,
        downlink_flush_max_bytes,
        downlink_backpressure,
        tun_mtu,
        downlink_egress_drop_debt,
    );
    let accepted_bytes = flush_outcome.accepted_bytes;
    let snapshot = SocketCloseSnapshot::from_socket(tcp_socket);
    let post_flush_projected_pressure =
        downlink_projected_local_pressure_bytes(snapshot.send_queue, ctx.downlink_pending.len());
    let installed_projected_debt = if buffered_downlink_enabled {
        0
    } else {
        downlink_egress_drop_debt.note_post_flush_projected_egress_pressure(
            snapshot.send_queue,
            ctx.downlink_pending.len(),
            accepted_bytes,
            downlink_backpressure,
        )
    };
    if installed_projected_debt > 0 {
        tcp_diag_log!(
            "{}",
            format_downlink_egress_credit_debt_diag(
                "post_flush_projected_payload_credit_edge",
                installed_projected_debt,
                DownlinkPressureStats {
                    max_pending: ctx.downlink_pending.len(),
                    total_pending: ctx.downlink_pending.len(),
                    max_tx_queue: post_flush_projected_pressure,
                    total_tx_queue: post_flush_projected_pressure,
                },
                downlink_backpressure,
                *downlink_egress_drop_debt,
            )
        );
    }
    note_downlink_pending_progress(ctx, now_secs, accepted_bytes);
    ctx.state = SocketState::Relaying;
    log_tcp_lifecycle_observation(handle, ctx, snapshot, now_secs, "remote_payload");
    if should_log_tcp_reverse_window(ctx, snapshot, now_secs) {
        tcp_diag_log!(
            "{}",
            format_tcp_reverse_window_diag(
                handle,
                TcpReverseWindowDiag {
                    ctx_state: ctx.state,
                    payload_bytes: payload.len(),
                    accepted_bytes,
                    pending: ctx.downlink_pending.len(),
                    remote_to_global_rx_bytes: ctx.downlink_diag.remote_to_global_rx_bytes,
                    local_fin_sent: ctx.local_fin_sent,
                    snapshot,
                },
            )
        );
    }

    let allow_immediate_flush = if flush_outcome.headroom_limited && accepted_bytes > 0 {
        downlink_egress_pacer.allow_headroom_limited_flush(accepted_bytes)
    } else {
        downlink_egress_pacer.allow_remote_payload_flush(
            accepted_bytes,
            ctx.downlink_pending.len(),
            snapshot.send_queue,
            downlink_backpressure,
        )
    };
    if !allow_immediate_flush {
        if accepted_bytes > 0 {
            ctx.downlink_diag.note_tun_flush_deferred();
        }
        return Ok(accepted_bytes);
    }

    let timestamp = smoltcp::time::Instant::now();
    iface.poll(timestamp, device, sockets);
    let result = device.flush_tx().await;
    let ok = result.is_ok();
    if let Some(ctx) = socket_ctxs.get_mut(&handle) {
        ctx.downlink_diag.note_tun_flush(ok);
    }
    if let Err(e) = &result {
        tcp_diag_log!(
            "🔎 tcp-tun-flush-fail handle={:?} stage=remote_payload err={e}",
            handle
        );
    }
    // Even if this flush fails, accepted bytes are already queued in smoltcp and
    // must keep the handle dirty for tx-queue-aware backpressure.
    Ok(accepted_bytes)
}

fn handle_relay_closed(
    handle: SocketHandle,
    close: RelayClose,
    sockets: &mut SocketSet<'_>,
    socket_ctxs: &mut HashMap<SocketHandle, SocketCtx>,
    fake_pool: &mut FakeIpPool,
    now_secs: u64,
    downlink_backpressure: DownlinkBackpressureConfig,
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
    let snapshot = {
        let tcp_socket = sockets.get::<TcpSocket>(handle);
        SocketCloseSnapshot::from_socket(tcp_socket)
    };
    log_tcp_lifecycle_observation(handle, ctx, snapshot, now_secs, "relay_close");
    if start_deferred_close_for_pending_downlink(ctx, close, now_secs) {
        trace_log!(
            "⏳ handle {:?} relay close({}/{}) waits for {} pending downlink bytes",
            handle,
            close.direction,
            close.reason,
            ctx.downlink_pending.len()
        );
        tcp_diag_log!(
            "🔎 tcp-deferred-close-pending handle={:?} direction={} reason={} pending={} started_secs={}",
            handle,
            close.direction,
            close.reason,
            ctx.downlink_pending.len(),
            now_secs
        );
        return true;
    }

    if start_deferred_close_for_egress_if_needed(
        ctx,
        close,
        snapshot,
        downlink_backpressure,
        now_secs,
    ) {
        tcp_diag_log!(
            "🔎 tcp-deferred-close-egress handle={:?} direction={} reason={} send_queue={} high={} low={} started_secs={}",
            handle,
            close.direction,
            close.reason,
            snapshot.send_queue,
            downlink_backpressure.high_bytes,
            downlink_backpressure.low_bytes,
            now_secs
        );
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
    downlink_backpressure: DownlinkBackpressureConfig,
    close_egress_drop_guard: bool,
) -> bool {
    let Some(close) = ctx.pending_relay_close else {
        return false;
    };
    if !ctx.downlink_pending.is_empty() {
        return true;
    }
    let snapshot = SocketCloseSnapshot::from_socket(socket);
    if should_send_local_eof_for_deferred_close(ctx, snapshot) {
        tcp_diag_log!(
            "🔎 tcp-local-eof-close handle={:?} direction={} reason={} send_queue={} tcp_state={:?} active={} can_send={} can_recv={} may_send={} may_recv={}",
            handle,
            close.direction,
            close.reason,
            snapshot.send_queue,
            snapshot.tcp_state,
            snapshot.active,
            snapshot.can_send,
            snapshot.can_recv,
            snapshot.may_send,
            snapshot.may_recv
        );
        socket.close();
        ctx.local_eof_sent = true;
        return true;
    }
    if ctx.local_eof_sent && snapshot.active {
        return true;
    }
    if should_keep_deferred_close_for_egress(
        ctx,
        snapshot,
        downlink_backpressure,
        now_secs,
        close_egress_drop_guard,
    ) {
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
) -> watch::Sender<RelayReadCredit> {
    // 刀11：每条新 TCP flow 开远端成功后在此唯一入口计一次（覆盖 inline + spawned remote-open 两条路）。
    metrics_handle.inc_relays_spawned();
    let (read_credit_tx, read_credit_rx) = watch::channel(RelayReadCredit::initial());
    tokio::spawn(run_relay(
        handle,
        epoch,
        stream,
        rx,
        back_tx,
        read_credit_rx,
    ));
    read_credit_tx
}

type RelayCloseReason = (&'static str, &'static str);

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct RemoteReadBurst {
    chunks: usize,
    bytes: usize,
}

#[derive(Debug, Clone)]
enum RelayReaderSignal {
    Progress {
        diag: RelayTaskDiag,
    },
    Closed {
        direction: &'static str,
        reason: &'static str,
        diag: RelayTaskDiag,
    },
}

async fn send_remote_payload_batch_to_main(
    handle: SocketHandle,
    epoch: u64,
    payload: Vec<u8>,
    batch_chunks: usize,
    back_tx: &mpsc::Sender<(SocketHandle, RelayEvent)>,
    diag: &mut RelayTaskDiag,
    global_rx_pressure_threshold: std::time::Duration,
) -> Result<(), RelayCloseReason> {
    let payload_len = payload.len();
    diag.note_remote_batch(batch_chunks, payload_len);
    note_global_rx_channel_occupancy(diag, back_tx);
    let wait_started = std::time::Instant::now();
    let send_result = back_tx
        .send((
            handle,
            RelayEvent::Data {
                epoch,
                bytes: payload,
            },
        ))
        .await;
    let waited = wait_started.elapsed();
    note_global_rx_channel_occupancy(diag, back_tx);
    diag.note_global_rx_wait(waited, global_rx_pressure_threshold);
    if waited >= global_rx_pressure_threshold {
        tcp_diag_log!(
            "🔎 tcp-global-rx-pressure handle={:?} wait_us={} payload_bytes={} pressure_events={}",
            handle,
            waited.as_micros(),
            payload_len,
            diag.global_rx_pressure_events
        );
    }
    if send_result.is_err() {
        return Err(("remote_to_local", "global_rx_closed"));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn send_remote_payload_staged_to_main(
    handle: SocketHandle,
    epoch: u64,
    payload: Vec<u8>,
    batch_chunks: usize,
    dispatch_segment_max_bytes: usize,
    back_tx: &mpsc::Sender<(SocketHandle, RelayEvent)>,
    diag: &mut RelayTaskDiag,
    global_rx_pressure_threshold: std::time::Duration,
) -> Result<(), RelayCloseReason> {
    let segment_max = dispatch_segment_max_bytes.max(1);
    let mut offset = 0;
    while offset < payload.len() {
        let end = offset.saturating_add(segment_max).min(payload.len());
        let segment_chunks = if offset == 0 && end == payload.len() {
            batch_chunks
        } else {
            1
        };
        let segment = payload[offset..end].to_vec();
        send_remote_payload_batch_to_main(
            handle,
            epoch,
            segment,
            segment_chunks,
            back_tx,
            diag,
            global_rx_pressure_threshold,
        )
        .await?;
        offset = end;
        if offset < payload.len() {
            tokio::task::yield_now().await;
        }
    }
    Ok(())
}

fn append_remote_read_to_batch(
    diag: &mut RelayTaskDiag,
    burst: &mut RemoteReadBurst,
    batch: &mut Vec<u8>,
    chunk: &[u8],
) {
    if chunk.is_empty() {
        return;
    }
    diag.note_remote_read(chunk.len());
    burst.chunks = burst.chunks.saturating_add(1);
    burst.bytes = burst.bytes.saturating_add(chunk.len());
    batch.extend_from_slice(chunk);
}

fn relay_remote_awaited_read_len(read_credit: RelayReadCredit, buffer_len: usize) -> usize {
    if read_credit.paused {
        return 0;
    }
    read_credit.max_batch_bytes.min(buffer_len)
}

fn note_reader_local_finish_if_needed(
    local_finish_seen: &std::sync::atomic::AtomicBool,
    diag: &mut RelayTaskDiag,
) {
    if !diag.local_finish_seen && local_finish_seen.load(std::sync::atomic::Ordering::SeqCst) {
        diag.note_local_finish();
    }
}

fn drain_relay_read_credit_updates(
    read_credit_rx: &mut watch::Receiver<RelayReadCredit>,
    read_credit: &mut RelayReadCredit,
    diag: &mut RelayTaskDiag,
) {
    while let Ok(true) = read_credit_rx.has_changed() {
        *read_credit = *read_credit_rx.borrow_and_update();
        diag.note_read_credit(*read_credit);
    }
}

async fn poll_ready_remote_read(
    remote_reader: &mut tokio::io::ReadHalf<RelayStream>,
    buf: &mut [u8],
) -> Option<std::io::Result<usize>> {
    std::future::poll_fn(|cx| {
        let mut read_buf = ReadBuf::new(&mut *buf);
        match std::pin::Pin::new(&mut *remote_reader).poll_read(cx, &mut read_buf) {
            std::task::Poll::Ready(Ok(())) => {
                std::task::Poll::Ready(Some(Ok(read_buf.filled().len())))
            }
            std::task::Poll::Ready(Err(e)) => std::task::Poll::Ready(Some(Err(e))),
            std::task::Poll::Pending => std::task::Poll::Ready(None),
        }
    })
    .await
}

#[allow(clippy::too_many_arguments)]
async fn drain_ready_remote_reads(
    remote_reader: &mut tokio::io::ReadHalf<RelayStream>,
    handle: SocketHandle,
    epoch: u64,
    initial_payload: Vec<u8>,
    buf: &mut [u8],
    back_tx: &mpsc::Sender<(SocketHandle, RelayEvent)>,
    diag: &mut RelayTaskDiag,
    read_credit: RelayReadCredit,
    global_rx_pressure_threshold: std::time::Duration,
) -> Result<(RemoteReadBurst, Option<RelayCloseReason>), RelayCloseReason> {
    let mut burst = RemoteReadBurst::default();
    let byte_limit = relay_remote_ready_batch_byte_limit(back_tx)
        .min(read_credit.max_batch_bytes)
        .max(initial_payload.len());
    diag.note_remote_batch_limit(byte_limit);
    let mut batch = Vec::with_capacity(initial_payload.len().min(byte_limit));
    append_remote_read_to_batch(diag, &mut burst, &mut batch, &initial_payload);
    let mut close_after_batch = None;
    while burst.chunks < RELAY_REMOTE_READ_BURST_MAX_CHUNKS && burst.bytes < byte_limit {
        let remaining = byte_limit.saturating_sub(burst.bytes);
        if remaining == 0 {
            break;
        }
        let read_len = remaining.min(buf.len());
        match poll_ready_remote_read(remote_reader, &mut buf[..read_len]).await {
            None => break,
            Some(Ok(0)) => {
                println!("远端服务器关闭了车厢 {:?}", handle);
                close_after_batch = Some(("remote_to_local", "remote_eof"));
                break;
            }
            Some(Ok(n)) => {
                append_remote_read_to_batch(diag, &mut burst, &mut batch, &buf[..n]);
            }
            Some(Err(e)) => {
                println!(
                    "读取上游流失败 direction=remote_to_local handle={:?} err={:?}",
                    handle, e
                );
                close_after_batch = Some(("remote_to_local", "remote_read_failed"));
                break;
            }
        }
    }
    if batch.is_empty() {
        if let Some(reason) = close_after_batch {
            return Err(reason);
        }
        return Ok((burst, None));
    }
    send_remote_payload_staged_to_main(
        handle,
        epoch,
        batch,
        burst.chunks,
        RELAY_REMOTE_DISPATCH_SEGMENT_MAX_BYTES,
        back_tx,
        diag,
        global_rx_pressure_threshold,
    )
    .await?;
    Ok((burst, close_after_batch))
}

async fn send_relay_reader_progress(
    signal_tx: &mpsc::Sender<RelayReaderSignal>,
    diag: &RelayTaskDiag,
) -> bool {
    signal_tx
        .send(RelayReaderSignal::Progress { diag: diag.clone() })
        .await
        .is_ok()
}

async fn send_relay_reader_close(
    signal_tx: &mpsc::Sender<RelayReaderSignal>,
    direction: &'static str,
    reason: &'static str,
    diag: RelayTaskDiag,
) {
    let _ = signal_tx
        .send(RelayReaderSignal::Closed {
            direction,
            reason,
            diag,
        })
        .await;
}

#[allow(clippy::too_many_arguments)]
async fn run_relay_reader(
    handle: SocketHandle,
    epoch: u64,
    mut remote_reader: tokio::io::ReadHalf<RelayStream>,
    back_tx: mpsc::Sender<(SocketHandle, RelayEvent)>,
    mut read_credit_rx: watch::Receiver<RelayReadCredit>,
    signal_tx: mpsc::Sender<RelayReaderSignal>,
    local_finish_seen: Arc<std::sync::atomic::AtomicBool>,
    mut stop_rx: tokio::sync::oneshot::Receiver<()>,
) {
    let mut buf = [0u8; 65_536];
    let mut diag = RelayTaskDiag::default();
    let mut read_credit = *read_credit_rx.borrow();
    diag.note_read_credit(read_credit);
    let global_rx_pressure_threshold = std::time::Duration::from_millis(5);
    loop {
        note_reader_local_finish_if_needed(&local_finish_seen, &mut diag);
        drain_relay_read_credit_updates(&mut read_credit_rx, &mut read_credit, &mut diag);
        let remote_read_len = relay_remote_awaited_read_len(read_credit, buf.len());
        if remote_read_len == 0 {
            let credit_update = tokio::select! {
                _ = &mut stop_rx => return,
                update = read_credit_rx.changed() => update,
            };
            if credit_update.is_err() {
                return;
            }
            read_credit = *read_credit_rx.borrow_and_update();
            diag.note_read_credit(read_credit);
            if read_credit.paused {
                tcp_diag_log!(
                    "🔎 tcp-relay-read-credit handle={:?} epoch={} paused=true max_batch_bytes=0 updates={} pause_updates={}",
                    handle,
                    epoch,
                    diag.read_credit_updates,
                    diag.read_credit_pause_updates
                );
            }
            continue;
        }

        diag.note_remote_read_service_tick(remote_read_len);
        let remote_msg = tokio::select! {
            _ = &mut stop_rx => return,
            remote_msg = remote_reader.read(&mut buf[..remote_read_len]) => remote_msg,
        };
        match remote_msg {
            Ok(0) => {
                println!("远端服务器关闭了车厢 {:?}", handle);
                send_relay_reader_close(&signal_tx, "remote_to_local", "remote_eof", diag).await;
                return;
            }
            Ok(n) => {
                note_reader_local_finish_if_needed(&local_finish_seen, &mut diag);
                let initial_payload = buf[..n].to_vec();
                match drain_ready_remote_reads(
                    &mut remote_reader,
                    handle,
                    epoch,
                    initial_payload,
                    &mut buf,
                    &back_tx,
                    &mut diag,
                    read_credit,
                    global_rx_pressure_threshold,
                )
                .await
                {
                    Ok((_burst, close_after_batch)) => {
                        if !send_relay_reader_progress(&signal_tx, &diag).await {
                            return;
                        }
                        if let Some((direction, reason)) = close_after_batch {
                            send_relay_reader_close(&signal_tx, direction, reason, diag).await;
                            return;
                        }
                    }
                    Err((direction, reason)) => {
                        send_relay_reader_close(&signal_tx, direction, reason, diag).await;
                        return;
                    }
                }
            }
            Err(e) => {
                println!(
                    "读取上游流失败 direction=remote_to_local handle={:?} err={:?}",
                    handle, e
                );
                send_relay_reader_close(&signal_tx, "remote_to_local", "remote_read_failed", diag)
                    .await;
                return;
            }
        }
    }
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
    read_credit_rx: watch::Receiver<RelayReadCredit>,
) {
    let (remote_reader, remote_writer) = tokio::io::split(stream);
    let (writer_signal_tx, mut writer_signal_rx) =
        mpsc::channel::<RelayWriterSignal>(RELAY_CHANNEL_CAPACITY);
    let (writer_stop_tx, writer_stop_rx) = tokio::sync::oneshot::channel::<()>();
    let (reader_signal_tx, mut reader_signal_rx) =
        mpsc::channel::<RelayReaderSignal>(RELAY_CHANNEL_CAPACITY);
    let (reader_stop_tx, reader_stop_rx) = tokio::sync::oneshot::channel::<()>();
    let local_finish_seen = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let read_credit_probe_rx = read_credit_rx.clone();
    let mut reader_task = tokio::spawn(run_relay_reader(
        handle,
        epoch,
        remote_reader,
        back_tx.clone(),
        read_credit_rx,
        reader_signal_tx,
        local_finish_seen.clone(),
        reader_stop_rx,
    ));
    let mut writer_task = tokio::spawn(run_relay_writer(
        handle,
        remote_writer,
        rx,
        writer_signal_tx,
        writer_stop_rx,
    ));
    let mut diag = RelayTaskDiag::default();
    let local_write_pressure_threshold = std::time::Duration::from_millis(5);
    let idle = tokio::time::sleep(RELAY_IDLE_TIMEOUT);
    tokio::pin!(idle);
    let mut diag_tick = tokio::time::interval(std::time::Duration::from_secs(RELAY_LIVE_DIAG_SECS));
    diag_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    diag_tick.tick().await;
    let mut remote_read_probe_tick = tokio::time::interval(std::time::Duration::from_millis(
        RELAY_REMOTE_READ_PROBE_INTERVAL_MS,
    ));
    remote_read_probe_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    remote_read_probe_tick.tick().await;
    let mut writer_done = false;
    let mut read_only_after_local_finish = false;
    let mut last_ack_drain_hint_at = None;
    let (close_direction, close_reason) = loop {
        tokio::select! {
            _ = diag_tick.tick(), if tcp_diag_enabled() => {
                tcp_diag_log!(
                    "{}",
                    format_relay_live_diag(
                        handle,
                        epoch,
                        writer_done,
                        read_only_after_local_finish,
                        &diag
                    )
                );
            }
            _ = remote_read_probe_tick.tick(), if should_poll_relay_remote_read_probe(&diag, *read_credit_probe_rx.borrow()) => {
                diag.note_remote_read_probe_tick();
                let now = std::time::Instant::now();
                if should_poll_relay_ack_drain_hint(&diag, writer_done)
                    && let Some(hint) = relay_ack_drain_hint_due(&diag, now, last_ack_drain_hint_at)
                {
                    last_ack_drain_hint_at = Some(now);
                    diag.note_ack_drain_hint_due();
                    match back_tx.try_send((
                        handle,
                        RelayEvent::AckDrainHint {
                            epoch,
                            gap_ms: hint.gap_ms,
                            remote_to_global_rx_bytes: hint.remote_to_global_rx_bytes,
                        },
                    )) {
                        Ok(()) => diag.note_ack_drain_hint_sent(),
                        Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                            diag.note_ack_drain_hint_dropped();
                            tcp_diag_log!(
                                "🔎 tcp-relay-ack-drain-hint-drop handle={:?} epoch={} reason=global_rx_full gap_ms={} remote_to_global_rx_bytes={} dropped={}",
                                handle,
                                epoch,
                                hint.gap_ms,
                                hint.remote_to_global_rx_bytes,
                                diag.ack_drain_hint_dropped
                            );
                        }
                        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                            diag.note_ack_drain_hint_dropped();
                            tcp_diag_log!(
                                "🔎 tcp-relay-ack-drain-hint-drop handle={:?} epoch={} reason=global_rx_closed gap_ms={} remote_to_global_rx_bytes={} dropped={}",
                                handle,
                                epoch,
                                hint.gap_ms,
                                hint.remote_to_global_rx_bytes,
                                diag.ack_drain_hint_dropped
                            );
                        }
                    }
                }
            }
            signal = writer_signal_rx.recv(), if !writer_done => {
                match signal {
                    Some(RelayWriterSignal::Progress { bytes, write_wait }) => {
                        diag.note_uplink_write(bytes);
                        diag.note_local_write_wait(write_wait, local_write_pressure_threshold);
                        if write_wait >= local_write_pressure_threshold {
                            tcp_diag_log!(
                                "🔎 tcp-local-write-pressure handle={:?} wait_us={} payload_bytes={} pressure_events={}",
                                handle,
                                write_wait.as_micros(),
                                bytes,
                                diag.local_write_pressure_events
                            );
                        }
                        idle.as_mut().reset(tokio::time::Instant::now() + RELAY_IDLE_TIMEOUT);
                    }
                    Some(RelayWriterSignal::WriteHalfClosed { reason }) => {
                        diag.note_local_finish();
                        local_finish_seen.store(true, std::sync::atomic::Ordering::SeqCst);
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
            reader_signal = reader_signal_rx.recv() => {
                match reader_signal {
                    Some(RelayReaderSignal::Progress { diag: reader_diag }) => {
                        diag.copy_reader_fields_from(&reader_diag);
                        let timeout = if read_only_after_local_finish {
                            RELAY_HALF_CLOSED_IDLE_TIMEOUT
                        } else {
                            RELAY_IDLE_TIMEOUT
                        };
                        idle.as_mut().reset(tokio::time::Instant::now() + timeout);
                    }
                    Some(RelayReaderSignal::Closed { direction, reason, diag: reader_diag }) => {
                        diag.copy_reader_fields_from(&reader_diag);
                        break (direction, reason);
                    }
                    None => {
                        break ("remote_to_local", "reader_task_closed");
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
    let _ = reader_stop_tx.send(());
    let _ = writer_stop_tx.send(());
    if reader_task.is_finished() {
        let _ = reader_task.await;
    } else if tokio::time::timeout(RELAY_WRITER_STOP_TIMEOUT, &mut reader_task)
        .await
        .is_err()
    {
        reader_task.abort();
        let _ = reader_task.await;
    }
    // M2 preserved after splitting the pump: let the write half drive shutdown so protocol-layer
    // pending bytes can flush, but keep the relay close path bounded if shutdown itself wedges.
    if writer_task.is_finished() {
        let _ = writer_task.await;
    } else if tokio::time::timeout(RELAY_WRITER_STOP_TIMEOUT, &mut writer_task)
        .await
        .is_err()
    {
        writer_task.abort();
        let _ = writer_task.await;
    }
    tcp_diag_log!(
        "{}",
        format_relay_close_diag(handle, close_direction, close_reason, &diag)
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

async fn shutdown_relay_writer_after_finish(
    handle: SocketHandle,
    writer: &mut tokio::io::WriteHalf<RelayStream>,
    signal_tx: &mpsc::Sender<RelayWriterSignal>,
    stop_rx: &mut tokio::sync::oneshot::Receiver<()>,
) {
    let shutdown_result = tokio::select! {
        _ = stop_rx => {
            let _ = writer.shutdown().await;
            return;
        }
        result = tokio::time::timeout(RELAY_WRITER_STOP_TIMEOUT, writer.shutdown()) => result,
    };
    match shutdown_result {
        Ok(Ok(_)) => {
            let _ = signal_tx
                .send(RelayWriterSignal::WriteHalfClosed {
                    reason: "local_finish",
                })
                .await;
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
        }
        Err(_) => {
            println!(
                "关闭上游写半边超时 direction=local_to_remote handle={:?} timeout={}s",
                handle,
                RELAY_WRITER_STOP_TIMEOUT.as_secs()
            );
            let _ = signal_tx
                .send(RelayWriterSignal::Closed {
                    direction: "local_to_remote",
                    reason: "remote_shutdown_timeout",
                })
                .await;
        }
    }
}

async fn run_relay_writer(
    handle: SocketHandle,
    mut writer: tokio::io::WriteHalf<RelayStream>,
    mut rx: mpsc::Receiver<RelayCommand>,
    signal_tx: mpsc::Sender<RelayWriterSignal>,
    mut stop_rx: tokio::sync::oneshot::Receiver<()>,
) {
    let mut deferred_cmd = None;
    loop {
        let local_msg = if let Some(cmd) = deferred_cmd.take() {
            Some(cmd)
        } else {
            tokio::select! {
                _ = &mut stop_rx => {
                    let _ = writer.shutdown().await;
                    return;
                }
                local_msg = rx.recv() => local_msg,
            }
        };
        match local_msg {
            Some(RelayCommand::Data(payload)) => {
                let coalesced = coalesce_relay_payload(payload, &mut rx);
                deferred_cmd = coalesced.deferred;
                let payload = coalesced.payload;
                let payload_len = payload.len();
                let write_started = std::time::Instant::now();
                let write_result = tokio::select! {
                    _ = &mut stop_rx => {
                        let _ = writer.shutdown().await;
                        return;
                    }
                    result = async {
                        writer.write_all(&payload).await?;
                        writer.flush().await
                    } => result,
                };
                let write_wait = write_started.elapsed();
                match write_result {
                    Ok(_) => {
                        let _ = signal_tx.try_send(RelayWriterSignal::Progress {
                            bytes: payload_len,
                            write_wait,
                        });
                        if coalesced.finish_after {
                            shutdown_relay_writer_after_finish(
                                handle,
                                &mut writer,
                                &signal_tx,
                                &mut stop_rx,
                            )
                            .await;
                            return;
                        }
                    }
                    Err(e) => {
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
                }
            }
            Some(RelayCommand::Finish) => {
                shutdown_relay_writer_after_finish(handle, &mut writer, &signal_tx, &mut stop_rx)
                    .await;
                return;
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
            trace_log!(
                "🚫 UDP fake-IP {} 无映射，丢弃（待应用重新解析）",
                udp.dst_ip
            );
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
    upstream
        .send_udp(encode_packet(assoc_id, &target, udp.payload))
        .await;
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
    use std::sync::atomic::AtomicU64;

    /// 刀8/刀9：上游选择器——reality→Reality、failover→Failover（大小写/空白不敏感）；
    /// 其余（含 tuic/缺省/未知）→ Tuic（零回归，failover opt-in）。
    #[test]
    fn upstream_kind_selector() {
        assert_eq!(select_upstream_kind(Some("reality")), UpstreamKind::Reality);
        assert_eq!(
            select_upstream_kind(Some("  REALITY ")),
            UpstreamKind::Reality
        );
        assert_eq!(
            select_upstream_kind(Some("failover")),
            UpstreamKind::Failover
        );
        assert_eq!(
            select_upstream_kind(Some(" Failover ")),
            UpstreamKind::Failover
        );
        assert_eq!(select_upstream_kind(Some("tuic")), UpstreamKind::Tuic);
        assert_eq!(
            select_upstream_kind(None),
            UpstreamKind::Tuic,
            "缺省 → TUIC（failover opt-in）"
        );
        assert_eq!(
            select_upstream_kind(Some("bogus")),
            UpstreamKind::Tuic,
            "未知 → TUIC（零回归）"
        );
    }

    // ---- 刀9 F4：relay idle 超时（L2）----
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::task::{Context, Poll, Waker};

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
            self.shutdown_called.store(true, Ordering::SeqCst);
            Poll::Ready(Ok(()))
        }
    }

    struct RecordingWriteStream {
        shutdown_called: Arc<AtomicBool>,
        writes: Arc<Mutex<Vec<Vec<u8>>>>,
    }
    impl tokio::io::AsyncRead for RecordingWriteStream {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            Poll::Pending
        }
    }
    impl tokio::io::AsyncWrite for RecordingWriteStream {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            self.writes.lock().unwrap().push(buf.to_vec());
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
            self.shutdown_called.store(true, Ordering::SeqCst);
            Poll::Ready(Ok(()))
        }
    }

    struct FlushGatedReadableStream {
        shutdown_called: Arc<AtomicBool>,
        flushed: Arc<AtomicBool>,
        read_waker: Arc<Mutex<Option<Waker>>>,
        writes: Arc<Mutex<Vec<Vec<u8>>>>,
        read_sent: bool,
    }
    impl tokio::io::AsyncRead for FlushGatedReadableStream {
        fn poll_read(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            if self.read_sent {
                return Poll::Pending;
            }
            if !self.flushed.load(Ordering::SeqCst) {
                *self.read_waker.lock().unwrap() = Some(cx.waker().clone());
                return Poll::Pending;
            }
            buf.put_slice(b"remote-after-flush");
            self.read_sent = true;
            Poll::Ready(Ok(()))
        }
    }
    impl tokio::io::AsyncWrite for FlushGatedReadableStream {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            self.writes.lock().unwrap().push(buf.to_vec());
            Poll::Ready(Ok(buf.len()))
        }
        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<std::io::Result<()>> {
            self.flushed.store(true, Ordering::SeqCst);
            if let Some(waker) = self.read_waker.lock().unwrap().take() {
                waker.wake();
            }
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<std::io::Result<()>> {
            self.shutdown_called.store(true, Ordering::SeqCst);
            Poll::Ready(Ok(()))
        }
    }

    struct BurstReadableStream {
        shutdown_called: Arc<AtomicBool>,
        chunks: std::collections::VecDeque<Vec<u8>>,
        eof_after_chunks: bool,
    }
    impl tokio::io::AsyncRead for BurstReadableStream {
        fn poll_read(
            mut self: std::pin::Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            let Some(mut chunk) = self.chunks.pop_front() else {
                if self.eof_after_chunks {
                    return Poll::Ready(Ok(()));
                }
                return Poll::Pending;
            };
            let take = chunk.len().min(buf.remaining());
            if take == 0 {
                self.chunks.push_front(chunk);
                return Poll::Pending;
            }
            buf.put_slice(&chunk[..take]);
            if take < chunk.len() {
                chunk.drain(..take);
                self.chunks.push_front(chunk);
            }
            Poll::Ready(Ok(()))
        }
    }
    impl tokio::io::AsyncWrite for BurstReadableStream {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
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
            self.shutdown_called.store(true, Ordering::SeqCst);
            Poll::Ready(Ok(()))
        }
    }

    struct CreditWakeReadableStream {
        shutdown_called: Arc<AtomicBool>,
        poll_count: Arc<AtomicU64>,
        ready: Arc<AtomicBool>,
        read_waker: Arc<Mutex<Option<std::task::Waker>>>,
        read_sent: bool,
    }
    impl tokio::io::AsyncRead for CreditWakeReadableStream {
        fn poll_read(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            self.poll_count.fetch_add(1, Ordering::SeqCst);
            if !self.ready.load(Ordering::SeqCst) {
                *self.read_waker.lock().unwrap() = Some(cx.waker().clone());
                return Poll::Pending;
            }
            if !self.read_sent {
                buf.put_slice(b"after-egress-wake");
                self.read_sent = true;
                return Poll::Ready(Ok(()));
            }
            Poll::Ready(Ok(()))
        }
    }
    impl tokio::io::AsyncWrite for CreditWakeReadableStream {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
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

    /// 一条上行写永远不完成的 mock 上游流，用来锁住 pending write 只由 relay 级 idle 兜底清理。
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
            self.shutdown_called.store(true, Ordering::SeqCst);
            Poll::Ready(Ok(()))
        }
    }

    fn mk_test_handle(sockets: &mut SocketSet<'static>) -> SocketHandle {
        sockets.add(build_listener_socket(&ListenerSpec { local_port: 12345 }))
    }

    fn relay_read_credit_channel() -> (
        watch::Sender<RelayReadCredit>,
        watch::Receiver<RelayReadCredit>,
    ) {
        watch::channel(RelayReadCredit::default())
    }

    #[test]
    fn relay_ready_batch_limit_tapers_with_global_rx_queue_pressure() {
        assert_eq!(
            relay_remote_ready_batch_byte_limit_for_queue(0, RELAY_CHANNEL_CAPACITY),
            RELAY_REMOTE_READ_BURST_MAX_BYTES
        );
        assert_eq!(
            relay_remote_ready_batch_byte_limit_for_queue(
                RELAY_CHANNEL_CAPACITY / 4,
                RELAY_CHANNEL_CAPACITY
            ),
            RELAY_REMOTE_READ_BURST_MAX_BYTES
        );

        let mid_limit = relay_remote_ready_batch_byte_limit_for_queue(
            RELAY_CHANNEL_CAPACITY / 2,
            RELAY_CHANNEL_CAPACITY,
        );
        assert!(
            mid_limit > RELAY_REMOTE_READ_MIN_BATCH_BYTES
                && mid_limit < RELAY_REMOTE_READ_BURST_MAX_BYTES,
            "middle queue pressure should reduce but not clamp the read batch: {mid_limit}"
        );
        assert_eq!(
            relay_remote_ready_batch_byte_limit_for_queue(
                RELAY_CHANNEL_CAPACITY * 3 / 4,
                RELAY_CHANNEL_CAPACITY
            ),
            RELAY_REMOTE_READ_MIN_BATCH_BYTES
        );
        assert_eq!(
            relay_remote_ready_batch_byte_limit_for_queue(
                RELAY_CHANNEL_CAPACITY,
                RELAY_CHANNEL_CAPACITY
            ),
            RELAY_REMOTE_READ_MIN_BATCH_BYTES
        );
    }

    #[test]
    fn relay_initial_read_credit_keeps_quic_receive_window_open() {
        let initial = RelayReadCredit::initial();

        assert!(!initial.paused);
        assert_eq!(initial.max_batch_bytes, RELAY_REMOTE_READ_BURST_MAX_BYTES);
        assert!(
            initial.max_batch_bytes >= RELAY_REMOTE_READ_BURST_MAX_BYTES,
            "relay startup must keep QUIC receive cadence healthy; local dispatch is segmented separately"
        );
    }

    #[test]
    fn relay_read_credit_tapers_and_pauses_for_local_egress() {
        let cfg = DownlinkBackpressureConfig::default();
        let tun_mtu = 1200;
        let pressure_floor = relay_remote_read_pressure_floor_bytes(tun_mtu);
        let ack_drain_floor = relay_remote_read_ack_drain_floor_bytes(tun_mtu);
        assert_eq!(
            relay_read_credit_for_local_egress(0, 0, false, false, cfg, tun_mtu),
            RelayReadCredit {
                paused: false,
                max_batch_bytes: RELAY_REMOTE_READ_BURST_MAX_BYTES,
            }
        );
        assert_eq!(
            relay_read_credit_for_local_egress(cfg.high_bytes, 0, false, false, cfg, tun_mtu),
            RelayReadCredit {
                paused: false,
                max_batch_bytes: RELAY_REMOTE_READ_BURST_MAX_BYTES,
            }
        );

        let mid = relay_read_credit_for_local_egress(
            (cfg.high_bytes + tx_queue_flush_threshold(cfg)) / 2,
            0,
            false,
            false,
            cfg,
            tun_mtu,
        );
        assert!(!mid.paused);
        assert!(
            mid.max_batch_bytes > pressure_floor
                && mid.max_batch_bytes < RELAY_REMOTE_READ_BURST_MAX_BYTES,
            "mid-credit send queue should taper read credit: {mid:?}"
        );

        assert_eq!(
            relay_read_credit_for_local_egress(
                tx_queue_flush_threshold(cfg),
                0,
                false,
                false,
                cfg,
                tun_mtu,
            ),
            RelayReadCredit {
                paused: false,
                max_batch_bytes: ack_drain_floor,
            }
        );
        assert_eq!(
            relay_read_credit_for_local_egress(
                tx_queue_credit_spend_threshold(cfg),
                0,
                false,
                false,
                cfg,
                tun_mtu,
            ),
            RelayReadCredit {
                paused: false,
                max_batch_bytes: ack_drain_floor,
            }
        );
        assert_eq!(
            relay_read_credit_for_local_egress(0, cfg.high_bytes, false, false, cfg, tun_mtu),
            RelayReadCredit {
                paused: false,
                max_batch_bytes: pressure_floor,
            }
        );
        assert_eq!(
            relay_read_credit_for_local_egress(0, 0, true, false, cfg, tun_mtu),
            RelayReadCredit {
                paused: false,
                max_batch_bytes: pressure_floor,
            },
            "active egress debt should keep QUIC reads draining with minimum credit, not hard-pause them"
        );
        assert_eq!(
            relay_read_credit_for_local_egress(
                0,
                downlink_receive_window_high(cfg),
                false,
                false,
                cfg,
                tun_mtu,
            ),
            RelayReadCredit {
                paused: true,
                max_batch_bytes: 0,
            }
        );
        assert_eq!(
            relay_read_credit_for_local_egress(0, 0, false, true, cfg, tun_mtu),
            RelayReadCredit {
                paused: true,
                max_batch_bytes: 0,
            }
        );
    }

    #[test]
    fn relay_pressure_floor_tracks_tun_mtu_below_legacy_min_batch() {
        let pressure_floor = relay_remote_read_pressure_floor_bytes(1200);

        assert_eq!(
            pressure_floor,
            1200 * RELAY_REMOTE_READ_PRESSURE_MIN_PACKETS
        );
        assert!(pressure_floor < RELAY_REMOTE_READ_MIN_BATCH_BYTES);
        assert_eq!(
            relay_remote_awaited_read_len(
                RelayReadCredit {
                    paused: false,
                    max_batch_bytes: pressure_floor,
                },
                65_536,
            ),
            pressure_floor,
            "the first awaited remote read must obey pressure credit, not the old 64 KiB buffer size"
        );
        assert_eq!(
            relay_remote_awaited_read_len(
                RelayReadCredit {
                    paused: true,
                    max_batch_bytes: pressure_floor,
                },
                65_536,
            ),
            0
        );
    }

    #[test]
    fn projected_relay_read_credit_accounts_for_incoming_payload() {
        let cfg = DownlinkBackpressureConfig::default();
        let tun_mtu = 1200;
        let ack_drain_floor = relay_remote_read_ack_drain_floor_bytes(tun_mtu);

        assert_eq!(
            relay_read_credit_for_projected_local_egress(
                cfg.low_bytes,
                0,
                RELAY_REMOTE_READ_MIN_BATCH_BYTES,
                false,
                false,
                cfg,
                tun_mtu,
            ),
            RelayReadCredit {
                paused: false,
                max_batch_bytes: RELAY_REMOTE_READ_BURST_MAX_BYTES,
            },
            "small incoming payloads below the high watermark keep QUIC receive cadence open"
        );

        let tapered = relay_read_credit_for_projected_local_egress(
            cfg.high_bytes.saturating_sub(1),
            0,
            RELAY_REMOTE_READ_MIN_BATCH_BYTES,
            false,
            false,
            cfg,
            tun_mtu,
        );
        assert!(!tapered.paused);
        assert!(
            tapered.max_batch_bytes < RELAY_REMOTE_READ_BURST_MAX_BYTES,
            "projected pressure should reduce the next relay batch before the current payload is flushed: {tapered:?}"
        );

        assert_eq!(
            relay_read_credit_for_projected_local_egress(
                tx_queue_flush_threshold(cfg),
                0,
                RELAY_REMOTE_READ_BURST_MAX_BYTES,
                false,
                false,
                cfg,
                tun_mtu,
            ),
            RelayReadCredit {
                paused: false,
                max_batch_bytes: ack_drain_floor,
            },
            "a projected credit-edge overshoot should keep draining the QUIC stream with a tiny ACK/window batch"
        );
    }

    #[test]
    fn relay_read_credit_caps_batch_to_clean_headroom_after_high_watermark() {
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 327_272,
            low_bytes: 81_818,
        };
        let tun_mtu = 1200;
        let local_pressure = cfg.high_bytes + 1;
        let clean_headroom = tx_queue_flush_threshold(cfg).saturating_sub(local_pressure);

        let credit =
            relay_read_credit_for_local_egress(local_pressure, 0, false, false, cfg, tun_mtu);

        assert!(
            !credit.paused,
            "clean-headroom capped credit should keep the stream serviced"
        );
        assert_eq!(
            credit.max_batch_bytes, clean_headroom,
            "read-credit must not permit a single relay batch larger than remaining clean egress headroom"
        );
        assert!(
            credit.max_batch_bytes < RELAY_REMOTE_READ_BURST_MAX_BYTES,
            "Knife14ez active failure shape must not keep publishing 512KiB batches"
        );
    }

    #[test]
    fn relay_read_credit_preserves_low_pressure_quic_read_window() {
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 327_272,
            low_bytes: 81_818,
        };
        let tun_mtu = 1200;

        let credit = relay_read_credit_for_local_egress(0, 0, false, false, cfg, tun_mtu);

        assert_eq!(
            credit.max_batch_bytes, RELAY_REMOTE_READ_BURST_MAX_BYTES,
            "low-pressure read-credit must not starve TUIC stream polling"
        );
        assert!(
            credit.max_batch_bytes > tx_queue_flush_threshold(cfg),
            "local protection belongs to staged dispatch, not the QUIC receive window"
        );
    }

    #[test]
    fn projected_relay_read_credit_shrinks_below_pressure_floor_near_drop_edge() {
        let cfg = DownlinkBackpressureConfig::default();
        let tun_mtu = 1200;
        let ack_drain_floor = relay_remote_read_ack_drain_floor_bytes(tun_mtu);
        let pressure_floor = relay_remote_read_pressure_floor_bytes(tun_mtu);
        let credit = relay_read_credit_for_projected_local_egress(
            tx_queue_flush_threshold(cfg).saturating_sub(1),
            0,
            1,
            false,
            false,
            cfg,
            tun_mtu,
        );

        assert!(
            !credit.paused,
            "local pressure should keep a tiny ACK/window drain instead of hard-pausing QUIC reads"
        );
        assert!(
            credit.max_batch_bytes >= ack_drain_floor && credit.max_batch_bytes < pressure_floor,
            "near the local egress edge, projected credit must stay useful but below the old pressure floor: {credit:?}, ack_floor={ack_drain_floor}, pressure_floor={pressure_floor}"
        );
    }

    #[test]
    fn relay_read_credit_keeps_useful_floor_just_below_flush_edge() {
        let cfg = DownlinkBackpressureConfig::default();
        let tun_mtu = 1200;
        let ack_drain_floor = relay_remote_read_ack_drain_floor_bytes(tun_mtu);
        let remaining_headroom = 24;
        let credit = relay_read_credit_for_local_egress(
            tx_queue_flush_threshold(cfg).saturating_sub(remaining_headroom),
            0,
            false,
            false,
            cfg,
            tun_mtu,
        );

        assert_eq!(
            credit,
            RelayReadCredit {
                paused: false,
                max_batch_bytes: ack_drain_floor,
            },
            "read credit below the flush edge must not fragment remote reads into tiny residual-headroom batches"
        );
    }

    #[test]
    fn relay_read_credit_preserves_quic_window_drain_until_staging_is_full() {
        let cfg = DownlinkBackpressureConfig::default();
        let tun_mtu = 1200;
        let ack_drain_floor = relay_remote_read_ack_drain_floor_bytes(tun_mtu);

        assert_eq!(
            relay_read_credit_for_local_egress(
                tx_queue_credit_spend_threshold(cfg),
                0,
                false,
                false,
                cfg,
                tun_mtu,
            ),
            RelayReadCredit {
                paused: false,
                max_batch_bytes: ack_drain_floor,
            },
            "local egress pressure should slow relay reads without starving the QUIC stream"
        );
        assert_eq!(
            relay_read_credit_for_local_egress(
                tx_queue_credit_spend_threshold(cfg),
                cfg.high_bytes,
                false,
                false,
                cfg,
                tun_mtu,
            ),
            RelayReadCredit {
                paused: false,
                max_batch_bytes: ack_drain_floor,
            },
            "pending staging below the receive window should keep minimum drain credit"
        );
        assert_eq!(
            relay_read_credit_for_local_egress(
                tx_queue_credit_spend_threshold(cfg),
                downlink_receive_window_high(cfg),
                false,
                false,
                cfg,
                tun_mtu,
            ),
            RelayReadCredit {
                paused: true,
                max_batch_bytes: 0,
            },
            "the bounded staging receive window remains the actual relay-read pause edge"
        );
    }

    #[test]
    fn terminal_no_recv_relay_read_credit_caps_staging_at_ack_floor() {
        let cfg = DownlinkBackpressureConfig::default();
        let tun_mtu = 1200;
        let ack_drain_floor = relay_remote_read_ack_drain_floor_bytes(tun_mtu);
        let closewait_no_recv = SocketCloseSnapshot {
            tcp_state: TcpState::CloseWait,
            active: true,
            can_send: true,
            can_recv: false,
            may_send: true,
            may_recv: false,
            send_capacity: 1_048_576,
            send_queue: tx_queue_flush_threshold(cfg),
            recv_queue: 0,
        };

        assert_eq!(
            relay_read_credit_for_terminal_no_recv_window(closewait_no_recv, 0, cfg, tun_mtu),
            Some(RelayReadCredit {
                paused: false,
                max_batch_bytes: ack_drain_floor,
            }),
            "terminal no-recv pressure should allow only one ACK/window-sized staging drain"
        );
        assert_eq!(
            relay_read_credit_for_terminal_no_recv_window(
                closewait_no_recv,
                ack_drain_floor,
                cfg,
                tun_mtu
            ),
            Some(RelayReadCredit {
                paused: true,
                max_batch_bytes: 0,
            }),
            "once the terminal staging cap is occupied, relay reads must pause instead of expanding pending"
        );
        assert_eq!(
            relay_read_credit_for_terminal_no_recv_window(
                SocketCloseSnapshot {
                    send_queue: tx_queue_flush_threshold(cfg).saturating_sub(1),
                    ..closewait_no_recv
                },
                0,
                cfg,
                tun_mtu
            ),
            None,
            "a no-recv close tail below the egress flush edge can keep the normal relay credit"
        );
        assert_eq!(
            relay_read_credit_for_terminal_no_recv_window(
                SocketCloseSnapshot {
                    tcp_state: TcpState::Established,
                    may_recv: true,
                    ..closewait_no_recv
                },
                ack_drain_floor,
                cfg,
                tun_mtu
            ),
            None,
            "active receive-capable flows must not inherit terminal staging caps"
        );
    }

    #[test]
    fn downlink_credit_controller_shrinks_staging_after_headroom_without_progress() {
        let cfg = DownlinkBackpressureConfig::default();
        let tun_mtu = 1200;
        let pressure_floor = relay_remote_read_pressure_floor_bytes(tun_mtu);
        let ack_drain_floor = relay_remote_read_ack_drain_floor_bytes(tun_mtu);
        let staging_limit = downlink_pressure_staging_limit(cfg, tun_mtu);
        let mut controller = DownlinkCreditController::default();
        let stalled = DownlinkFlushLimit {
            len: 0,
            headroom_limited: true,
            headroom_deferred_bytes: 64,
            clean_headroom_bytes: 0,
            drain_credit_granted_bytes: 0,
            drain_credit_planned_bytes: 0,
            drop_credit_debt_bytes: 0,
            drop_credit_debt_paid_bytes: 0,
            drop_credit_blocked_bytes: 0,
            pressure_credit_debt_bytes: 0,
            pressure_credit_debt_paid_bytes: 0,
            pressure_credit_blocked_bytes: 0,
            hard_edge_guard_bytes: 3,
            hard_edge_guard_deferred_bytes: 64,
        };

        controller.note_flush_feedback(stalled, cfg, tun_mtu);
        controller.note_flush_feedback(stalled, cfg, tun_mtu);

        assert!(controller.headroom_debt_bytes > 0);
        assert!(controller.no_egress_progress_streak >= 2);
        assert_eq!(
            controller.read_credit_for(RelayReadCredit::default(), staging_limit, cfg, tun_mtu),
            RelayReadCredit {
                paused: true,
                max_batch_bytes: 0,
            },
            "stalled egress may keep only bounded staging and must not keep expanding pending"
        );
        let budget = controller.flush_budget(262_144, cfg, tun_mtu);
        assert!(
            budget >= ack_drain_floor && budget < pressure_floor,
            "headroom deferral should shrink the next flush budget below the old pressure floor: budget={budget}, ack_floor={ack_drain_floor}, pressure_floor={pressure_floor}"
        );
    }

    #[test]
    fn downlink_credit_controller_keeps_high_water_staging_while_egress_progresses() {
        let cfg = DownlinkBackpressureConfig::default();
        let tun_mtu = 1200;
        let ack_drain_floor = relay_remote_read_ack_drain_floor_bytes(tun_mtu);
        let low_pressure_staging = downlink_pressure_staging_limit(cfg, tun_mtu);
        let controller = DownlinkCreditController {
            headroom_debt_bytes: cfg.high_bytes,
            no_egress_progress_streak: 0,
            ..DownlinkCreditController::default()
        };

        let credit = controller.read_credit_for(
            RelayReadCredit::default(),
            low_pressure_staging.saturating_add(ack_drain_floor),
            cfg,
            tun_mtu,
        );

        assert!(
            !credit.paused,
            "headroom debt alone should not collapse staging to the low watermark while egress is still making progress: credit={credit:?}, low_pressure_staging={low_pressure_staging}, high={}",
            cfg.high_bytes
        );
        assert!(credit.max_batch_bytes >= ack_drain_floor);
    }

    #[test]
    fn downlink_credit_controller_pauses_instead_of_publishing_sub_mtu_staging_credit() {
        let cfg = DownlinkBackpressureConfig::default();
        let tun_mtu = 1200;
        let ack_drain_floor = relay_remote_read_ack_drain_floor_bytes(tun_mtu);
        let controller = DownlinkCreditController::default();

        let credit = controller.read_credit_for(
            RelayReadCredit::default(),
            downlink_receive_window_high(cfg).saturating_sub(ack_drain_floor / 2),
            cfg,
            tun_mtu,
        );

        assert_eq!(
            credit,
            RelayReadCredit {
                paused: true,
                max_batch_bytes: 0,
            },
            "bounded staging should not publish tiny residual read credits below the ACK/window floor"
        );
    }

    #[test]
    fn downlink_credit_controller_uses_full_clean_batch_before_pressure_feedback() {
        let cfg = DownlinkBackpressureConfig::default();
        let tun_mtu = 1200;
        let controller = DownlinkCreditController::default();

        let clean = controller.read_credit_for(RelayReadCredit::default(), 0, cfg, tun_mtu);

        assert_eq!(
            clean,
            RelayReadCredit {
                paused: false,
                max_batch_bytes: RELAY_REMOTE_READ_BURST_MAX_BYTES,
            },
            "clean empty staging should drain QUIC with the base relay batch instead of waiting for egress feedback to grow the initial 64KiB ceiling"
        );

        let staged = controller.read_credit_for(
            RelayReadCredit::default(),
            relay_remote_read_ack_drain_floor_bytes(tun_mtu),
            cfg,
            tun_mtu,
        );
        assert_eq!(
            staged.max_batch_bytes, DOWNLINK_CREDIT_INITIAL_BATCH_BYTES,
            "once staging is non-empty, the initial controller ceiling still bounds local pending growth"
        );
    }

    #[test]
    fn downlink_credit_controller_grows_credit_from_observed_egress_progress() {
        let cfg = DownlinkBackpressureConfig::default();
        let tun_mtu = 1200;
        let pressure_floor = relay_remote_read_pressure_floor_bytes(tun_mtu);
        let mut controller = DownlinkCreditController::default();
        let stalled = DownlinkFlushLimit {
            len: 0,
            headroom_limited: true,
            headroom_deferred_bytes: 64,
            clean_headroom_bytes: 0,
            drain_credit_granted_bytes: 0,
            drain_credit_planned_bytes: 0,
            drop_credit_debt_bytes: 0,
            drop_credit_debt_paid_bytes: 0,
            drop_credit_blocked_bytes: 0,
            pressure_credit_debt_bytes: 0,
            pressure_credit_debt_paid_bytes: 0,
            pressure_credit_blocked_bytes: 0,
            hard_edge_guard_bytes: 3,
            hard_edge_guard_deferred_bytes: 64,
        };
        controller.note_flush_feedback(stalled, cfg, tun_mtu);
        let shrunk = controller.flush_budget(262_144, cfg, tun_mtu);

        let progressed = DownlinkFlushLimit {
            drain_credit_granted_bytes: pressure_floor,
            headroom_limited: false,
            headroom_deferred_bytes: 0,
            hard_edge_guard_deferred_bytes: 0,
            ..stalled
        };
        controller.note_flush_feedback(progressed, cfg, tun_mtu);

        assert_eq!(controller.no_egress_progress_streak, 0);
        assert!(
            controller.flush_budget(262_144, cfg, tun_mtu) > shrunk,
            "observed send_queue drain should reopen the local flush budget additively"
        );
        let credit = controller.read_credit_for(RelayReadCredit::default(), 0, cfg, tun_mtu);
        assert!(
            credit.max_batch_bytes > pressure_floor,
            "observed egress progress should grow relay read credit"
        );
    }

    #[test]
    fn downlink_credit_controller_grows_pressure_ack_drain_from_clean_egress_progress() {
        let cfg = DownlinkBackpressureConfig::default();
        let tun_mtu = 1200;
        let ack_drain_floor = relay_remote_read_ack_drain_floor_bytes(tun_mtu);
        let pressure_floor = relay_remote_read_pressure_floor_bytes(tun_mtu);
        let mut controller = DownlinkCreditController::default();
        let progressed = DownlinkFlushLimit {
            len: pressure_floor,
            headroom_limited: false,
            headroom_deferred_bytes: 0,
            clean_headroom_bytes: pressure_floor,
            drain_credit_granted_bytes: pressure_floor,
            drain_credit_planned_bytes: 0,
            drop_credit_debt_bytes: 0,
            drop_credit_debt_paid_bytes: 0,
            drop_credit_blocked_bytes: 0,
            pressure_credit_debt_bytes: 0,
            pressure_credit_debt_paid_bytes: 0,
            pressure_credit_blocked_bytes: 0,
            hard_edge_guard_bytes: 0,
            hard_edge_guard_deferred_bytes: 0,
        };

        controller.note_flush_feedback(progressed, cfg, tun_mtu);
        controller.note_flush_feedback(progressed, cfg, tun_mtu);

        let local_pressure_bytes = tx_queue_flush_threshold(cfg);
        let pressure_ack_drain_bytes =
            controller.adaptive_ack_drain_floor_for(local_pressure_bytes, cfg, tun_mtu);
        let base = relay_read_credit_for_local_egress_with_ack_drain_floor(
            local_pressure_bytes,
            0,
            false,
            false,
            cfg,
            tun_mtu,
            pressure_ack_drain_bytes,
            DEFAULT_DOWNLINK_FLUSH_MAX_BYTES,
        );
        let credit = controller.read_credit_for(base, 0, cfg, tun_mtu);

        assert!(
            credit.max_batch_bytes > ack_drain_floor,
            "clean egress progress should reopen pressure ACK/window drain above the tiny floor: credit={credit:?}, ack_floor={ack_drain_floor}"
        );
        assert!(
            credit.max_batch_bytes >= pressure_floor,
            "adaptive pressure drain should include at least the old pressure floor: credit={credit:?}, pressure_floor={pressure_floor}"
        );
        assert!(
            credit.max_batch_bytes <= RELAY_REMOTE_READ_MIN_BATCH_BYTES,
            "adaptive pressure drain must stay bounded to a useful compressed batch: credit={credit:?}, min_batch={}",
            RELAY_REMOTE_READ_MIN_BATCH_BYTES
        );
    }

    #[test]
    fn downlink_credit_controller_progressing_headroom_deferral_keeps_useful_batch() {
        let cfg = DownlinkBackpressureConfig::default();
        let tun_mtu = 1200;
        let pressure_floor = relay_remote_read_pressure_floor_bytes(tun_mtu);
        let mut controller = DownlinkCreditController::default();
        let progressing_deferral = DownlinkFlushLimit {
            len: RELAY_REMOTE_READ_MIN_BATCH_BYTES,
            headroom_limited: true,
            headroom_deferred_bytes: RELAY_REMOTE_READ_MIN_BATCH_BYTES,
            clean_headroom_bytes: 0,
            drain_credit_granted_bytes: RELAY_REMOTE_READ_MIN_BATCH_BYTES,
            drain_credit_planned_bytes: 0,
            drop_credit_debt_bytes: 0,
            drop_credit_debt_paid_bytes: 0,
            drop_credit_blocked_bytes: 0,
            pressure_credit_debt_bytes: 0,
            pressure_credit_debt_paid_bytes: 0,
            pressure_credit_blocked_bytes: 0,
            hard_edge_guard_bytes: 0,
            hard_edge_guard_deferred_bytes: 0,
        };

        controller.note_flush_feedback(progressing_deferral, cfg, tun_mtu);
        controller.note_flush_feedback(progressing_deferral, cfg, tun_mtu);

        assert_eq!(
            controller.no_egress_progress_streak, 0,
            "headroom deferral with accepted egress bytes is pressure, not a stall"
        );
        let local_pressure_bytes = tx_queue_egress_target_threshold(cfg);
        let pressure_ack_drain_bytes =
            controller.adaptive_ack_drain_floor_for(local_pressure_bytes, cfg, tun_mtu);
        let base = relay_read_credit_for_local_egress_with_ack_drain_floor(
            local_pressure_bytes,
            0,
            false,
            false,
            cfg,
            tun_mtu,
            pressure_ack_drain_bytes,
            DEFAULT_DOWNLINK_FLUSH_MAX_BYTES,
        );
        let credit =
            controller.read_credit_for_local_pressure(base, 0, local_pressure_bytes, cfg, tun_mtu);

        assert!(
            credit.max_batch_bytes >= RELAY_REMOTE_READ_MIN_BATCH_BYTES,
            "progressing headroom deferral should keep a useful compressed batch instead of collapsing to the 19KB pressure floor: credit={credit:?}, pressure_floor={pressure_floor}"
        );
    }

    #[test]
    fn downlink_credit_controller_treats_accepted_flush_bytes_as_egress_progress() {
        let cfg = DownlinkBackpressureConfig::default();
        let tun_mtu = 1200;
        let ack_drain_floor = relay_remote_read_ack_drain_floor_bytes(tun_mtu);
        let pressure_floor = relay_remote_read_pressure_floor_bytes(tun_mtu);
        let mut controller = DownlinkCreditController::default();
        let accepted_deferral = DownlinkFlushLimit {
            len: pressure_floor,
            headroom_limited: true,
            headroom_deferred_bytes: pressure_floor,
            clean_headroom_bytes: pressure_floor,
            drain_credit_granted_bytes: 0,
            drain_credit_planned_bytes: 0,
            drop_credit_debt_bytes: 0,
            drop_credit_debt_paid_bytes: 0,
            drop_credit_blocked_bytes: 0,
            pressure_credit_debt_bytes: 0,
            pressure_credit_debt_paid_bytes: 0,
            pressure_credit_blocked_bytes: 0,
            hard_edge_guard_bytes: 0,
            hard_edge_guard_deferred_bytes: pressure_floor,
        };

        controller.note_flush_feedback_with_accepted(
            accepted_deferral,
            pressure_floor,
            cfg,
            tun_mtu,
        );
        controller.note_flush_feedback_with_accepted(
            accepted_deferral,
            pressure_floor,
            cfg,
            tun_mtu,
        );

        assert_eq!(
            controller.no_egress_progress_streak, 0,
            "bytes accepted by smoltcp are useful local egress progress, not a stalled flush"
        );
        assert!(
            controller.egress_progress_generation >= 2,
            "accepted flush progress must wake relay read-credit publishers"
        );
        let local_pressure_bytes = tx_queue_credit_spend_threshold(cfg);
        let pressure_ack_drain_bytes =
            controller.adaptive_ack_drain_floor_for(local_pressure_bytes, cfg, tun_mtu);
        let base = relay_read_credit_for_local_egress_with_ack_drain_floor(
            local_pressure_bytes,
            0,
            false,
            false,
            cfg,
            tun_mtu,
            pressure_ack_drain_bytes,
            DEFAULT_DOWNLINK_FLUSH_MAX_BYTES,
        );
        let credit = controller.read_credit_for(base, 0, cfg, tun_mtu);

        assert!(
            !credit.paused && credit.max_batch_bytes > ack_drain_floor,
            "accepted flush progress should keep a useful read-credit cadence at the credit edge: credit={credit:?}, ack_floor={ack_drain_floor}"
        );
    }

    #[test]
    fn downlink_credit_controller_earns_payload_tokens_from_egress_progress() {
        let cfg = DownlinkBackpressureConfig::default();
        let tun_mtu = 1200;
        let ack_drain_floor = relay_remote_read_ack_drain_floor_bytes(tun_mtu);
        let target_edge = tx_queue_egress_target_threshold(cfg);
        let payload_edge = target_edge.saturating_sub(ack_drain_floor.saturating_mul(2));
        let mut controller = DownlinkCreditController::default();
        let progressed = DownlinkFlushLimit {
            len: 64 * 1024,
            headroom_limited: false,
            headroom_deferred_bytes: 0,
            clean_headroom_bytes: 64 * 1024,
            drain_credit_granted_bytes: 64 * 1024,
            drain_credit_planned_bytes: 0,
            drop_credit_debt_bytes: 0,
            drop_credit_debt_paid_bytes: 0,
            drop_credit_blocked_bytes: 0,
            pressure_credit_debt_bytes: 0,
            pressure_credit_debt_paid_bytes: 0,
            pressure_credit_blocked_bytes: 0,
            hard_edge_guard_bytes: 0,
            hard_edge_guard_deferred_bytes: 0,
        };

        controller.note_flush_feedback(progressed, cfg, tun_mtu);
        let base = relay_read_credit_for_local_egress_with_ack_drain_floor(
            payload_edge,
            0,
            false,
            false,
            cfg,
            tun_mtu,
            ack_drain_floor,
            DEFAULT_DOWNLINK_FLUSH_MAX_BYTES,
        );
        let credit = controller.read_credit_for_local_pressure(base, 0, payload_edge, cfg, tun_mtu);

        assert!(
            credit.max_batch_bytes > ack_drain_floor,
            "egress-earned payload tokens should reopen useful payload read credit below the egress target: credit={credit:?}, ack_floor={ack_drain_floor}"
        );
        assert!(
            credit.max_batch_bytes <= target_edge - payload_edge,
            "payload token credit must stay inside the egress target headroom"
        );

        let at_target = relay_read_credit_for_local_egress_with_ack_drain_floor(
            target_edge,
            0,
            false,
            false,
            cfg,
            tun_mtu,
            ack_drain_floor,
            DEFAULT_DOWNLINK_FLUSH_MAX_BYTES,
        );
        let at_target =
            controller.read_credit_for_local_pressure(at_target, 0, target_edge, cfg, tun_mtu);
        assert_eq!(
            at_target.max_batch_bytes, ack_drain_floor,
            "at the egress target, ordinary payload tokens must yield to the ACK/window floor"
        );
    }

    #[test]
    fn downlink_credit_controller_spends_payload_tokens_only_under_projected_pressure() {
        let cfg = DownlinkBackpressureConfig::default();
        let tun_mtu = 1200;
        let mut controller = DownlinkCreditController::default();
        let progressed = DownlinkFlushLimit {
            len: 64 * 1024,
            headroom_limited: false,
            headroom_deferred_bytes: 0,
            clean_headroom_bytes: 64 * 1024,
            drain_credit_granted_bytes: 64 * 1024,
            drain_credit_planned_bytes: 0,
            drop_credit_debt_bytes: 0,
            drop_credit_debt_paid_bytes: 0,
            drop_credit_blocked_bytes: 0,
            pressure_credit_debt_bytes: 0,
            pressure_credit_debt_paid_bytes: 0,
            pressure_credit_blocked_bytes: 0,
            hard_edge_guard_bytes: 0,
            hard_edge_guard_deferred_bytes: 0,
        };

        controller.note_flush_feedback(progressed, cfg, tun_mtu);
        let earned = controller.egress_payload_credit_bytes;
        assert!(earned > 0, "progress should earn payload tokens");

        controller.note_payload_read_for_projected_pressure(
            earned / 2,
            tx_queue_flush_threshold(cfg).saturating_sub(1),
            cfg,
        );
        assert_eq!(
            controller.egress_payload_credit_bytes, earned,
            "below the pressure region, ordinary payload reads should not spend the egress token reservoir"
        );

        controller.note_payload_read_for_projected_pressure(
            earned / 2,
            tx_queue_flush_threshold(cfg),
            cfg,
        );
        assert_eq!(
            controller.egress_payload_credit_bytes,
            earned - earned / 2,
            "projected pressure payload reads must spend the reservoir they used"
        );
    }

    #[test]
    fn downlink_credit_controller_tokens_do_not_override_pause_or_staging() {
        let cfg = DownlinkBackpressureConfig::default();
        let tun_mtu = 1200;
        let ack_drain_floor = relay_remote_read_ack_drain_floor_bytes(tun_mtu);
        let mut controller = DownlinkCreditController::default();
        let progressed = DownlinkFlushLimit {
            len: 64 * 1024,
            headroom_limited: false,
            headroom_deferred_bytes: 0,
            clean_headroom_bytes: 64 * 1024,
            drain_credit_granted_bytes: 64 * 1024,
            drain_credit_planned_bytes: 0,
            drop_credit_debt_bytes: 0,
            drop_credit_debt_paid_bytes: 0,
            drop_credit_blocked_bytes: 0,
            pressure_credit_debt_bytes: 0,
            pressure_credit_debt_paid_bytes: 0,
            pressure_credit_blocked_bytes: 0,
            hard_edge_guard_bytes: 0,
            hard_edge_guard_deferred_bytes: 0,
        };

        controller.note_flush_feedback(progressed, cfg, tun_mtu);
        let base = RelayReadCredit {
            paused: false,
            max_batch_bytes: ack_drain_floor,
        };
        let at_pause = controller.read_credit_for_local_pressure(
            base,
            0,
            tx_queue_pause_threshold(cfg),
            cfg,
            tun_mtu,
        );
        assert_eq!(
            at_pause, base,
            "payload tokens must not expand relay credit at or beyond the hard pause edge"
        );

        let staged = controller.read_credit_for_local_pressure(
            RelayReadCredit::default(),
            downlink_receive_window_high(cfg),
            tx_queue_flush_threshold(cfg),
            cfg,
            tun_mtu,
        );
        assert_eq!(
            staged,
            RelayReadCredit {
                paused: true,
                max_batch_bytes: 0,
            },
            "payload tokens must not override the bounded staging limit"
        );
    }

    #[test]
    fn downlink_credit_controller_keeps_adaptive_ack_drain_at_credit_edge_while_progressing() {
        let cfg = DownlinkBackpressureConfig::default();
        let tun_mtu = 1200;
        let ack_drain_floor = relay_remote_read_ack_drain_floor_bytes(tun_mtu);
        let pressure_floor = relay_remote_read_pressure_floor_bytes(tun_mtu);
        let pause_edge = tx_queue_pause_threshold(cfg);
        let mut controller = DownlinkCreditController::default();
        let progressed = DownlinkFlushLimit {
            len: pressure_floor,
            headroom_limited: false,
            headroom_deferred_bytes: 0,
            clean_headroom_bytes: pressure_floor,
            drain_credit_granted_bytes: pressure_floor,
            drain_credit_planned_bytes: 0,
            drop_credit_debt_bytes: 0,
            drop_credit_debt_paid_bytes: 0,
            drop_credit_blocked_bytes: 0,
            pressure_credit_debt_bytes: 0,
            pressure_credit_debt_paid_bytes: 0,
            pressure_credit_blocked_bytes: 0,
            hard_edge_guard_bytes: 0,
            hard_edge_guard_deferred_bytes: 0,
        };

        controller.note_flush_feedback(progressed, cfg, tun_mtu);
        controller.note_flush_feedback(progressed, cfg, tun_mtu);

        let local_pressure_bytes = tx_queue_credit_spend_threshold(cfg);
        let pressure_ack_drain_bytes =
            controller.adaptive_ack_drain_floor_for(local_pressure_bytes, cfg, tun_mtu);
        let base = relay_read_credit_for_local_egress_with_ack_drain_floor(
            local_pressure_bytes,
            0,
            false,
            false,
            cfg,
            tun_mtu,
            pressure_ack_drain_bytes,
            DEFAULT_DOWNLINK_FLUSH_MAX_BYTES,
        );
        let credit = controller.read_credit_for(base, 0, cfg, tun_mtu);

        assert!(
            !credit.paused && credit.max_batch_bytes > ack_drain_floor,
            "clean egress progress should keep useful ACK/window drain at the credit edge: credit={credit:?}, ack_floor={ack_drain_floor}"
        );
        assert!(
            credit.max_batch_bytes <= pressure_floor,
            "credit edge ACK/window drain must stay pressure bounded: credit={credit:?}, pressure_floor={pressure_floor}"
        );
        assert!(
            credit.max_batch_bytes <= pause_edge - local_pressure_bytes,
            "credit edge ACK/window drain must not project past the hard pause edge"
        );
    }

    #[test]
    fn downlink_credit_controller_shrinks_adaptive_ack_drain_after_no_progress() {
        let cfg = DownlinkBackpressureConfig::default();
        let tun_mtu = 1200;
        let ack_drain_floor = relay_remote_read_ack_drain_floor_bytes(tun_mtu);
        let pressure_floor = relay_remote_read_pressure_floor_bytes(tun_mtu);
        let mut controller = DownlinkCreditController::default();
        let progressed = DownlinkFlushLimit {
            len: pressure_floor,
            headroom_limited: false,
            headroom_deferred_bytes: 0,
            clean_headroom_bytes: pressure_floor,
            drain_credit_granted_bytes: pressure_floor,
            drain_credit_planned_bytes: 0,
            drop_credit_debt_bytes: 0,
            drop_credit_debt_paid_bytes: 0,
            drop_credit_blocked_bytes: 0,
            pressure_credit_debt_bytes: 0,
            pressure_credit_debt_paid_bytes: 0,
            pressure_credit_blocked_bytes: 0,
            hard_edge_guard_bytes: 0,
            hard_edge_guard_deferred_bytes: 0,
        };
        let stalled = DownlinkFlushLimit {
            len: 0,
            headroom_limited: true,
            headroom_deferred_bytes: pressure_floor,
            clean_headroom_bytes: 0,
            drain_credit_granted_bytes: 0,
            drain_credit_planned_bytes: 0,
            drop_credit_debt_bytes: 0,
            drop_credit_debt_paid_bytes: 0,
            drop_credit_blocked_bytes: 0,
            pressure_credit_debt_bytes: 0,
            pressure_credit_debt_paid_bytes: 0,
            pressure_credit_blocked_bytes: 0,
            hard_edge_guard_bytes: 0,
            hard_edge_guard_deferred_bytes: 0,
        };

        controller.note_flush_feedback(progressed, cfg, tun_mtu);
        controller.note_flush_feedback(progressed, cfg, tun_mtu);
        assert!(
            controller.adaptive_ack_drain_floor_for(tx_queue_flush_threshold(cfg), cfg, tun_mtu)
                > ack_drain_floor
        );

        controller.note_flush_feedback(stalled, cfg, tun_mtu);
        controller.note_flush_feedback(stalled, cfg, tun_mtu);

        assert_eq!(
            controller.adaptive_ack_drain_floor_for(tx_queue_flush_threshold(cfg), cfg, tun_mtu),
            ack_drain_floor,
            "consecutive no-progress feedback should shrink adaptive drain back to the tiny floor"
        );
    }

    #[test]
    fn relay_read_credit_uses_cadence_boost_between_credit_and_pause_edge() {
        let cfg = DownlinkBackpressureConfig::default();
        let tun_mtu = 1200;
        let ack_drain_floor = relay_remote_read_ack_drain_floor_bytes(tun_mtu);
        let pressure_floor = relay_remote_read_pressure_floor_bytes(tun_mtu);
        let credit_edge = tx_queue_credit_spend_threshold(cfg);
        let pause_edge = tx_queue_pause_threshold(cfg);
        let local_pressure = credit_edge + (pause_edge - credit_edge) / 2;

        let credit = relay_read_credit_for_local_pressure_with_ack_drain_floor(
            local_pressure,
            0,
            false,
            false,
            cfg,
            tun_mtu,
            pressure_floor,
            DEFAULT_DOWNLINK_FLUSH_MAX_BYTES,
        );

        assert!(
            credit.max_batch_bytes > ack_drain_floor,
            "gap-driven ACK/window cadence should keep a bounded multi-MTU read credit until the hard pause edge: credit={credit:?}, ack_floor={ack_drain_floor}"
        );
        assert!(
            credit.max_batch_bytes <= pressure_floor,
            "cadence boost must stay below the pressure floor"
        );
        assert!(
            credit.max_batch_bytes <= pause_edge - local_pressure,
            "cadence boost must not project beyond the hard pause edge"
        );
    }

    #[test]
    fn downlink_credit_controller_gap_hint_boost_is_short_lived_and_hard_pause_safe() {
        let cfg = DownlinkBackpressureConfig::default();
        let tun_mtu = 1200;
        let ack_drain_floor = relay_remote_read_ack_drain_floor_bytes(tun_mtu);
        let pressure_floor = relay_remote_read_pressure_floor_bytes(tun_mtu);
        let mut controller = DownlinkCreditController::default();

        controller.note_ack_cadence_gap_hint(tx_queue_flush_threshold(cfg), false, cfg, tun_mtu);
        assert!(
            controller.adaptive_ack_drain_floor_for(tx_queue_flush_threshold(cfg), cfg, tun_mtu)
                > ack_drain_floor,
            "a relay read gap with healthy egress should grant a short-lived ACK/window cadence boost"
        );

        let boosted = controller.adaptive_ack_drain_floor_for(
            tx_queue_credit_spend_threshold(cfg),
            cfg,
            tun_mtu,
        );
        assert!(
            boosted > ack_drain_floor && boosted <= pressure_floor,
            "cadence boost should remain useful at the credit edge but bounded: boosted={boosted}"
        );

        let first = controller.consume_ack_cadence_boost_for_publish(
            tx_queue_flush_threshold(cfg),
            false,
            cfg,
            tun_mtu,
        );
        assert!(
            first > ack_drain_floor,
            "first publish should consume the boost"
        );
        let second = controller.consume_ack_cadence_boost_for_publish(
            tx_queue_flush_threshold(cfg),
            false,
            cfg,
            tun_mtu,
        );
        assert_eq!(
            second, ack_drain_floor,
            "cadence boost should be one-shot unless another gap hint arrives"
        );

        controller.note_ack_cadence_gap_hint(tx_queue_flush_threshold(cfg), false, cfg, tun_mtu);
        controller.note_ack_cadence_gap_hint(tx_queue_pause_threshold(cfg), true, cfg, tun_mtu);
        assert_eq!(
            controller.adaptive_ack_drain_floor_for(tx_queue_flush_threshold(cfg), cfg, tun_mtu),
            ack_drain_floor,
            "hard pause feedback must clear cadence boost"
        );
    }

    #[test]
    fn downlink_credit_controller_gap_hint_at_credit_edge_keeps_headroom_sized_boost() {
        let cfg = DownlinkBackpressureConfig::default();
        let tun_mtu = 1200;
        let ack_drain_floor = relay_remote_read_ack_drain_floor_bytes(tun_mtu);
        let pressure_floor = relay_remote_read_pressure_floor_bytes(tun_mtu);
        let credit_edge = tx_queue_credit_spend_threshold(cfg);
        let pause_headroom = tx_queue_pause_threshold(cfg).saturating_sub(credit_edge);
        let mut controller = DownlinkCreditController::default();

        controller.note_ack_cadence_gap_hint(credit_edge, false, cfg, tun_mtu);
        let boosted = controller.adaptive_ack_drain_floor_for(credit_edge, cfg, tun_mtu);

        assert!(
            boosted > ack_drain_floor,
            "a relay-read gap observed at the credit edge should not collapse to the single-MTU ACK floor: boosted={boosted}, ack_floor={ack_drain_floor}"
        );
        assert_eq!(
            boosted,
            pressure_floor.min(pause_headroom.max(ack_drain_floor)),
            "credit-edge gap boost should spend only the remaining pause headroom"
        );
    }

    #[test]
    fn relay_gap_hint_boost_requires_current_relay_context() {
        let cfg = DownlinkBackpressureConfig::default();
        let tun_mtu = 1200;
        let ack_drain_floor = relay_remote_read_ack_drain_floor_bytes(tun_mtu);
        let mut sockets = SocketSet::new(vec![]);
        let handle = sockets.add(TcpSocket::new(
            TcpSocketBuffer::new(vec![0; 4096]),
            TcpSocketBuffer::new(vec![0; 4096]),
        ));
        let mut ctx = SocketCtx::new(443);
        ctx.state = SocketState::Relaying;
        ctx.conn_epoch = 7;
        let mut ctxs = HashMap::from([(handle, ctx)]);

        assert_eq!(
            note_ack_cadence_gap_hint_for_handle(
                handle, 6, &sockets, &mut ctxs, cfg, tun_mtu, false,
            ),
            None,
            "stale relay gap hints must not alter the current flow"
        );
        assert_eq!(
            ctxs.get(&handle)
                .unwrap()
                .downlink_credit_controller
                .adaptive_ack_drain_floor_for(0, cfg, tun_mtu),
            ack_drain_floor
        );

        let boosted = note_ack_cadence_gap_hint_for_handle(
            handle, 7, &sockets, &mut ctxs, cfg, tun_mtu, false,
        )
        .expect("current relay gap hint should report a cadence floor");
        assert!(
            boosted > ack_drain_floor,
            "current relay gap hint should activate a bounded cadence boost"
        );

        ctxs.get_mut(&handle).unwrap().state = SocketState::Listening;
        assert_eq!(
            note_ack_cadence_gap_hint_for_handle(
                handle, 7, &sockets, &mut ctxs, cfg, tun_mtu, false,
            ),
            None,
            "non-relay slots must ignore late gap hints"
        );
    }

    #[tokio::test]
    async fn relay_read_credit_publish_wakes_on_egress_progress_even_when_credit_is_unchanged() {
        let cfg = DownlinkBackpressureConfig::default();
        let tun_mtu = 1200;
        let mut sockets = SocketSet::new(vec![]);
        let handle = sockets.add(TcpSocket::new(
            TcpSocketBuffer::new(vec![0; 4096]),
            TcpSocketBuffer::new(vec![0; 4096]),
        ));
        let (credit_tx, mut credit_rx) = watch::channel(RelayReadCredit::default());
        let mut ctx = SocketCtx::new(443);
        ctx.state = SocketState::Relaying;
        ctx.relay_read_credit_tx = Some(credit_tx);
        ctx.downlink_credit_controller.note_flush_feedback(
            DownlinkFlushLimit {
                len: 0,
                headroom_limited: false,
                headroom_deferred_bytes: 0,
                clean_headroom_bytes: 0,
                drain_credit_granted_bytes: relay_remote_read_pressure_floor_bytes(tun_mtu),
                drain_credit_planned_bytes: 0,
                drop_credit_debt_bytes: 0,
                drop_credit_debt_paid_bytes: 0,
                drop_credit_blocked_bytes: 0,
                pressure_credit_debt_bytes: 0,
                pressure_credit_debt_paid_bytes: 0,
                pressure_credit_blocked_bytes: 0,
                hard_edge_guard_bytes: 0,
                hard_edge_guard_deferred_bytes: 0,
            },
            cfg,
            tun_mtu,
        );
        let mut ctxs = HashMap::from([(handle, ctx)]);

        publish_relay_read_credit_for_handle(
            handle,
            &sockets,
            &mut ctxs,
            cfg,
            tun_mtu,
            DEFAULT_DOWNLINK_FLUSH_MAX_BYTES,
            false,
            false,
            BufferedDownlinkConfig::disabled(cfg, DEFAULT_TUN_POOL_SIZE),
            0,
        );

        tokio::time::timeout(std::time::Duration::from_millis(20), credit_rx.changed())
            .await
            .expect("egress progress must wake the relay reader even if credit bytes stay open")
            .expect("relay read-credit channel should remain open");
        assert_eq!(*credit_rx.borrow_and_update(), RelayReadCredit::default());
    }

    #[test]
    fn listener_socket_uses_local_virtual_link_tcp_policy() {
        let socket = build_listener_socket(&ListenerSpec { local_port: 12345 });

        assert!(
            !socket.nagle_enabled(),
            "local virtual-link TCP sockets should not add Nagle latency"
        );
        assert!(
            socket.ack_delay().is_none(),
            "local virtual-link TCP sockets should not delay ACKs"
        );
    }

    /// idle：双向 90s 无活动 → relay task 退出 + stream.shutdown 被调（L2）。
    #[tokio::test(start_paused = true)]
    async fn relay_idle_timeout_shuts_down_stream() {
        let mut sockets = SocketSet::new(vec![]);
        let handle = mk_test_handle(&mut sockets);
        let flag = Arc::new(AtomicBool::new(false));
        let stream: RelayStream = Box::new(IdleStream {
            shutdown_called: flag.clone(),
        });
        let (_tx, rx) = mpsc::channel::<RelayCommand>(8); // 持 _tx → rx 不关、永不收（无活动）
        let (back_tx, mut back_rx) = mpsc::channel(8);
        let (_credit_tx, credit_rx) = relay_read_credit_channel();
        let task = tokio::spawn(run_relay(handle, 7, stream, rx, back_tx, credit_rx));

        tokio::time::advance(std::time::Duration::from_secs(89)).await;
        assert!(!task.is_finished(), "89s < 90s idle 阈值，relay 不应退出");
        tokio::time::advance(std::time::Duration::from_secs(2)).await;
        task.await.unwrap();
        assert!(
            flag.load(Ordering::SeqCst),
            "idle 超时应退出并调用 stream.shutdown（L2）"
        );
        match back_rx
            .try_recv()
            .expect("relay close should notify main loop")
        {
            (h, RelayEvent::Closed(close)) => {
                assert_eq!(h, handle);
                assert_eq!(close.epoch, 7);
                assert_eq!(close.direction, "timer");
                assert_eq!(close.reason, "idle_timeout");
            }
            other => panic!("expected relay close event, got {other:?}"),
        }
    }

    #[test]
    fn coalesced_relay_payload_preserves_finish_order() {
        let (tx, mut rx) = mpsc::channel(4);
        tx.try_send(RelayCommand::Data(b"two".to_vec())).unwrap();
        tx.try_send(RelayCommand::Finish).unwrap();

        let coalesced = coalesce_relay_payload(b"one".to_vec(), &mut rx);

        assert_eq!(coalesced.payload, b"onetwo");
        assert!(
            coalesced.finish_after,
            "queued Finish must run after queued data"
        );
        assert!(coalesced.deferred.is_none());
        assert!(
            rx.try_recv().is_err(),
            "Finish should be consumed into finish_after"
        );
    }

    #[test]
    fn coalesced_relay_payload_defers_data_that_would_exceed_cap() {
        let (tx, mut rx) = mpsc::channel(4);
        tx.try_send(RelayCommand::Data(vec![2; 2])).unwrap();

        let coalesced =
            coalesce_relay_payload(vec![1; RELAY_WRITER_COALESCE_MAX_BYTES - 1], &mut rx);

        assert_eq!(coalesced.payload.len(), RELAY_WRITER_COALESCE_MAX_BYTES - 1);
        match coalesced
            .deferred
            .expect("oversized next data should be deferred")
        {
            RelayCommand::Data(bytes) => assert_eq!(bytes, vec![2; 2]),
            other => panic!("expected deferred data, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn relay_writer_coalesces_queued_data_before_single_write() {
        let mut sockets = SocketSet::new(vec![]);
        let handle = mk_test_handle(&mut sockets);
        let shutdown_called = Arc::new(AtomicBool::new(false));
        let writes = Arc::new(Mutex::new(Vec::new()));
        let stream: RelayStream = Box::new(RecordingWriteStream {
            shutdown_called: shutdown_called.clone(),
            writes: writes.clone(),
        });
        let (tx, rx) = mpsc::channel::<RelayCommand>(8);
        tx.send(RelayCommand::Data(b"one".to_vec())).await.unwrap();
        tx.send(RelayCommand::Data(b"two".to_vec())).await.unwrap();
        tx.send(RelayCommand::Data(b"three".to_vec()))
            .await
            .unwrap();
        drop(tx);
        let (back_tx, mut back_rx) = mpsc::channel(8);
        let (_credit_tx, credit_rx) = relay_read_credit_channel();

        let task = tokio::spawn(run_relay(handle, 41, stream, rx, back_tx, credit_rx));
        task.await.unwrap();

        assert_eq!(
            *writes.lock().unwrap(),
            vec![b"onetwothree".to_vec()],
            "queued data should become one upstream write batch"
        );
        assert!(
            shutdown_called.load(Ordering::SeqCst),
            "local channel close still shuts down writer"
        );
        match back_rx
            .try_recv()
            .expect("local channel close should notify main loop")
        {
            (h, RelayEvent::Closed(close)) => {
                assert_eq!(h, handle);
                assert_eq!(close.epoch, 41);
                assert_eq!(close.direction, "local_to_remote");
                assert_eq!(close.reason, "local_channel_closed");
            }
            other => panic!("expected relay close event, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn relay_writer_flushes_payload_to_release_remote_response() {
        let mut sockets = SocketSet::new(vec![]);
        let handle = mk_test_handle(&mut sockets);
        let shutdown_called = Arc::new(AtomicBool::new(false));
        let flushed = Arc::new(AtomicBool::new(false));
        let read_waker = Arc::new(Mutex::new(None));
        let writes = Arc::new(Mutex::new(Vec::new()));
        let stream: RelayStream = Box::new(FlushGatedReadableStream {
            shutdown_called: shutdown_called.clone(),
            flushed: flushed.clone(),
            read_waker,
            writes: writes.clone(),
            read_sent: false,
        });
        let (tx, rx) = mpsc::channel::<RelayCommand>(8);
        let (back_tx, mut back_rx) = mpsc::channel(8);
        let (_credit_tx, credit_rx) = relay_read_credit_channel();
        let task = tokio::spawn(run_relay(handle, 43, stream, rx, back_tx, credit_rx));

        tx.send(RelayCommand::Data(b"reverse-control".to_vec()))
            .await
            .unwrap();

        let event = tokio::time::timeout(std::time::Duration::from_millis(200), back_rx.recv())
            .await
            .expect("remote response gated on flush should arrive promptly")
            .expect("relay should still be alive");

        assert!(
            flushed.load(Ordering::SeqCst),
            "payload writes must be flushed after write_all"
        );
        assert_eq!(*writes.lock().unwrap(), vec![b"reverse-control".to_vec()]);
        match event {
            (h, RelayEvent::Data { epoch, bytes }) => {
                assert_eq!(h, handle);
                assert_eq!(epoch, 43);
                assert_eq!(bytes, b"remote-after-flush");
            }
            other => panic!("expected remote response after flushed payload, got {other:?}"),
        }

        drop(tx);
        task.await.unwrap();
        assert!(
            shutdown_called.load(Ordering::SeqCst),
            "local channel close still shuts down writer"
        );
    }

    #[tokio::test]
    async fn relay_remote_ready_burst_forwards_immediate_chunks() {
        let mut sockets = SocketSet::new(vec![]);
        let handle = mk_test_handle(&mut sockets);
        let stream: RelayStream = Box::new(BurstReadableStream {
            shutdown_called: Arc::new(AtomicBool::new(false)),
            chunks: std::collections::VecDeque::from([
                b"one".to_vec(),
                b"two".to_vec(),
                b"three".to_vec(),
            ]),
            eof_after_chunks: false,
        });
        let (mut remote_reader, _remote_writer) = tokio::io::split(stream);
        let (back_tx, mut back_rx) = mpsc::channel(8);
        let mut diag = RelayTaskDiag::default();
        let mut buf = [0u8; 65_536];

        let (burst, close_after_batch) = drain_ready_remote_reads(
            &mut remote_reader,
            handle,
            77,
            Vec::new(),
            &mut buf,
            &back_tx,
            &mut diag,
            RelayReadCredit::default(),
            std::time::Duration::from_millis(5),
        )
        .await
        .expect("ready burst should forward without closing");

        assert_eq!(close_after_batch, None);
        assert_eq!(burst.chunks, 3);
        assert_eq!(burst.bytes, b"onetwothree".len());
        assert_eq!(diag.remote_reads, 3);
        assert_eq!(diag.remote_to_global_rx_bytes, b"onetwothree".len() as u64);
        assert_eq!(diag.remote_batches, 1);
        assert_eq!(diag.remote_batch_bytes_max, b"onetwothree".len());
        assert_eq!(diag.remote_batch_chunks_max, 3);
        match back_rx
            .recv()
            .await
            .expect("burst payload should be queued")
        {
            (h, RelayEvent::Data { epoch, bytes }) => {
                assert_eq!(h, handle);
                assert_eq!(epoch, 77);
                assert_eq!(bytes, b"onetwothree");
            }
            other => panic!("expected coalesced burst data, got {other:?}"),
        }
        assert!(back_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn relay_remote_ready_burst_respects_byte_limit_without_overshoot() {
        let mut sockets = SocketSet::new(vec![]);
        let handle = mk_test_handle(&mut sockets);
        let chunks: std::collections::VecDeque<_> = std::iter::repeat_with(|| vec![7; 100_000])
            .take(8)
            .collect();
        let stream: RelayStream = Box::new(BurstReadableStream {
            shutdown_called: Arc::new(AtomicBool::new(false)),
            chunks,
            eof_after_chunks: false,
        });
        let (mut remote_reader, _remote_writer) = tokio::io::split(stream);
        let (back_tx, mut back_rx) = mpsc::channel(8);
        let mut diag = RelayTaskDiag::default();
        let mut buf = [0u8; 65_536];

        let (burst, close_after_batch) = drain_ready_remote_reads(
            &mut remote_reader,
            handle,
            79,
            Vec::new(),
            &mut buf,
            &back_tx,
            &mut diag,
            RelayReadCredit::default(),
            std::time::Duration::from_millis(5),
        )
        .await
        .expect("first ready burst should forward");

        assert_eq!(close_after_batch, None);
        assert_eq!(burst.bytes, RELAY_REMOTE_READ_BURST_MAX_BYTES);
        assert_eq!(
            diag.remote_batch_bytes_max,
            RELAY_REMOTE_DISPATCH_SEGMENT_MAX_BYTES
        );
        let mut first_dispatch_bytes = 0;
        while first_dispatch_bytes < RELAY_REMOTE_READ_BURST_MAX_BYTES {
            match back_rx
                .recv()
                .await
                .expect("capped batch segment should be queued")
            {
                (h, RelayEvent::Data { epoch, bytes }) => {
                    assert_eq!(h, handle);
                    assert_eq!(epoch, 79);
                    assert!(
                        bytes.len() <= RELAY_REMOTE_DISPATCH_SEGMENT_MAX_BYTES,
                        "dispatch segment must stay within local flush budget"
                    );
                    first_dispatch_bytes += bytes.len();
                }
                other => panic!("expected capped data segment, got {other:?}"),
            }
        }
        assert_eq!(first_dispatch_bytes, RELAY_REMOTE_READ_BURST_MAX_BYTES);

        let (next_burst, next_close) = drain_ready_remote_reads(
            &mut remote_reader,
            handle,
            79,
            Vec::new(),
            &mut buf,
            &back_tx,
            &mut diag,
            RelayReadCredit::default(),
            std::time::Duration::from_millis(5),
        )
        .await
        .expect("remaining bytes should stay readable after the cap");

        assert_eq!(next_close, None);
        assert_eq!(
            next_burst.bytes,
            800_000 - RELAY_REMOTE_READ_BURST_MAX_BYTES
        );
        let mut second_dispatch_bytes = 0;
        while second_dispatch_bytes < 800_000 - RELAY_REMOTE_READ_BURST_MAX_BYTES {
            match back_rx
                .recv()
                .await
                .expect("second remainder segment should be queued")
            {
                (h, RelayEvent::Data { epoch, bytes }) => {
                    assert_eq!(h, handle);
                    assert_eq!(epoch, 79);
                    assert!(
                        bytes.len() <= RELAY_REMOTE_DISPATCH_SEGMENT_MAX_BYTES,
                        "remainder dispatch segment must stay within local flush budget"
                    );
                    second_dispatch_bytes += bytes.len();
                }
                other => panic!("expected remainder data segment, got {other:?}"),
            }
        }
        assert_eq!(
            second_dispatch_bytes,
            800_000 - RELAY_REMOTE_READ_BURST_MAX_BYTES
        );
    }

    #[tokio::test]
    async fn relay_remote_ready_burst_tapers_when_global_rx_queue_is_busy() {
        let mut sockets = SocketSet::new(vec![]);
        let handle = mk_test_handle(&mut sockets);
        let chunks: std::collections::VecDeque<_> = std::iter::repeat_with(|| vec![5; 16 * 1024])
            .take(16)
            .collect();
        let stream: RelayStream = Box::new(BurstReadableStream {
            shutdown_called: Arc::new(AtomicBool::new(false)),
            chunks,
            eof_after_chunks: false,
        });
        let (mut remote_reader, _remote_writer) = tokio::io::split(stream);
        let (back_tx, mut back_rx) = mpsc::channel(4);
        for _ in 0..3 {
            back_tx
                .try_send((
                    handle,
                    RelayEvent::AckDrainHint {
                        epoch: 80,
                        gap_ms: 500,
                        remote_to_global_rx_bytes: 1,
                    },
                ))
                .unwrap();
        }
        let mut diag = RelayTaskDiag::default();
        let mut buf = [0u8; 65_536];

        let (burst, close_after_batch) = drain_ready_remote_reads(
            &mut remote_reader,
            handle,
            80,
            Vec::new(),
            &mut buf,
            &back_tx,
            &mut diag,
            RelayReadCredit::default(),
            std::time::Duration::from_millis(5),
        )
        .await
        .expect("pressure-limited ready burst should forward");

        assert_eq!(close_after_batch, None);
        assert_eq!(burst.bytes, RELAY_REMOTE_READ_MIN_BATCH_BYTES);
        assert_eq!(
            diag.remote_batch_bytes_max,
            RELAY_REMOTE_READ_MIN_BATCH_BYTES
        );
        assert_eq!(
            diag.remote_batch_limit_bytes_min,
            RELAY_REMOTE_READ_MIN_BATCH_BYTES
        );
        assert_eq!(diag.remote_batch_limited, 1);

        for _ in 0..3 {
            match back_rx
                .recv()
                .await
                .expect("prefilled hint should remain ahead of data")
            {
                (_, RelayEvent::AckDrainHint { .. }) => {}
                other => panic!("expected prefilled ack hint, got {other:?}"),
            }
        }
        match back_rx
            .recv()
            .await
            .expect("limited batch should use the last free slot")
        {
            (h, RelayEvent::Data { epoch, bytes }) => {
                assert_eq!(h, handle);
                assert_eq!(epoch, 80);
                assert_eq!(bytes.len(), RELAY_REMOTE_READ_MIN_BATCH_BYTES);
            }
            other => panic!("expected pressure-limited data batch, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn relay_ready_burst_delivers_data_before_eof_close() {
        let mut sockets = SocketSet::new(vec![]);
        let handle = mk_test_handle(&mut sockets);
        let shutdown_called = Arc::new(AtomicBool::new(false));
        let stream: RelayStream = Box::new(BurstReadableStream {
            shutdown_called: shutdown_called.clone(),
            chunks: std::collections::VecDeque::from([b"one".to_vec(), b"two".to_vec()]),
            eof_after_chunks: true,
        });
        let (_tx, rx) = mpsc::channel::<RelayCommand>(8);
        let (back_tx, mut back_rx) = mpsc::channel(8);
        let (_credit_tx, credit_rx) = relay_read_credit_channel();

        run_relay(handle, 78, stream, rx, back_tx, credit_rx).await;

        match back_rx
            .recv()
            .await
            .expect("coalesced data should arrive first")
        {
            (h, RelayEvent::Data { epoch, bytes }) => {
                assert_eq!(h, handle);
                assert_eq!(epoch, 78);
                assert_eq!(bytes, b"onetwo");
            }
            other => panic!("expected data before close, got {other:?}"),
        }
        match back_rx
            .recv()
            .await
            .expect("EOF should notify main loop after data")
        {
            (h, RelayEvent::Closed(close)) => {
                assert_eq!(h, handle);
                assert_eq!(close.epoch, 78);
                assert_eq!(close.direction, "remote_to_local");
                assert_eq!(close.reason, "remote_eof");
            }
            other => panic!("expected close after data, got {other:?}"),
        }
        assert!(
            shutdown_called.load(Ordering::SeqCst),
            "EOF path should stop the writer half"
        );
        assert!(back_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn relay_read_credit_pause_stops_remote_reads_until_resumed() {
        let mut sockets = SocketSet::new(vec![]);
        let handle = mk_test_handle(&mut sockets);
        let shutdown_called = Arc::new(AtomicBool::new(false));
        let stream: RelayStream = Box::new(BurstReadableStream {
            shutdown_called: shutdown_called.clone(),
            chunks: std::collections::VecDeque::from([b"after-credit".to_vec()]),
            eof_after_chunks: true,
        });
        let (_tx, rx) = mpsc::channel::<RelayCommand>(8);
        let (back_tx, mut back_rx) = mpsc::channel(8);
        let (credit_tx, credit_rx) = watch::channel(RelayReadCredit {
            paused: true,
            max_batch_bytes: 0,
        });
        let task = tokio::spawn(run_relay(handle, 81, stream, rx, back_tx, credit_rx));

        tokio::task::yield_now().await;
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), back_rx.recv())
                .await
                .is_err(),
            "paused read credit must keep ready remote bytes unread"
        );

        credit_tx.send(RelayReadCredit::default()).unwrap();
        match tokio::time::timeout(std::time::Duration::from_millis(200), back_rx.recv())
            .await
            .expect("remote data should arrive after credit resumes")
            .expect("relay should still be alive")
        {
            (h, RelayEvent::Data { epoch, bytes }) => {
                assert_eq!(h, handle);
                assert_eq!(epoch, 81);
                assert_eq!(bytes, b"after-credit");
            }
            other => panic!("expected data after read credit resumes, got {other:?}"),
        }
        task.await.unwrap();
        assert!(
            shutdown_called.load(Ordering::SeqCst),
            "remote EOF after resumed read should stop the writer half"
        );
    }

    #[tokio::test]
    async fn relay_read_credit_update_keeps_inflight_remote_read_armed() {
        let mut sockets = SocketSet::new(vec![]);
        let handle = mk_test_handle(&mut sockets);
        let shutdown_called = Arc::new(AtomicBool::new(false));
        let poll_count = Arc::new(AtomicU64::new(0));
        let ready = Arc::new(AtomicBool::new(false));
        let read_waker = Arc::new(Mutex::new(None));
        let stream: RelayStream = Box::new(CreditWakeReadableStream {
            shutdown_called: shutdown_called.clone(),
            poll_count: poll_count.clone(),
            ready: ready.clone(),
            read_waker: read_waker.clone(),
            read_sent: false,
        });
        let (_tx, rx) = mpsc::channel::<RelayCommand>(8);
        let (back_tx, mut back_rx) = mpsc::channel(8);
        let (credit_tx, credit_rx) = relay_read_credit_channel();
        let task = tokio::spawn(run_relay(handle, 83, stream, rx, back_tx, credit_rx));

        tokio::task::yield_now().await;
        assert_eq!(
            poll_count.load(Ordering::SeqCst),
            1,
            "test stream should have one pending read before egress wake"
        );
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), back_rx.recv())
                .await
                .is_err(),
            "a pending remote read should not progress before the remote stream wakes it"
        );

        credit_tx
            .send(RelayReadCredit {
                paused: false,
                max_batch_bytes: relay_remote_read_pressure_floor_bytes(1200),
            })
            .unwrap();
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), back_rx.recv())
                .await
                .is_err(),
            "non-pausing credit updates must not cancel and recreate an in-flight remote read"
        );
        assert_eq!(
            poll_count.load(Ordering::SeqCst),
            1,
            "non-pausing credit updates should be applied before the next remote read, not by repolling the current one"
        );

        ready.store(true, Ordering::SeqCst);
        read_waker
            .lock()
            .unwrap()
            .take()
            .expect("pending remote read should have registered a waker")
            .wake();
        match tokio::time::timeout(std::time::Duration::from_millis(200), back_rx.recv())
            .await
            .expect("remote stream wake should complete the pending read")
            .expect("relay should still be alive")
        {
            (h, RelayEvent::Data { epoch, bytes }) => {
                assert_eq!(h, handle);
                assert_eq!(epoch, 83);
                assert_eq!(bytes, b"after-egress-wake");
            }
            other => panic!("expected remote data after read-credit wake, got {other:?}"),
        }
        task.await.unwrap();
        assert!(
            shutdown_called.load(Ordering::SeqCst),
            "EOF after the test payload should stop the writer half"
        );
    }

    #[tokio::test]
    async fn relay_pressure_credit_caps_first_awaited_remote_read() {
        let mut sockets = SocketSet::new(vec![]);
        let handle = mk_test_handle(&mut sockets);
        let pressure_floor = relay_remote_read_pressure_floor_bytes(1200);
        let shutdown_called = Arc::new(AtomicBool::new(false));
        let stream: RelayStream = Box::new(BurstReadableStream {
            shutdown_called: shutdown_called.clone(),
            chunks: std::collections::VecDeque::from([vec![9; pressure_floor + 777]]),
            eof_after_chunks: true,
        });
        let (_tx, rx) = mpsc::channel::<RelayCommand>(8);
        let (back_tx, mut back_rx) = mpsc::channel(8);
        let (_credit_tx, credit_rx) = watch::channel(RelayReadCredit {
            paused: false,
            max_batch_bytes: pressure_floor,
        });
        let task = tokio::spawn(run_relay(handle, 82, stream, rx, back_tx, credit_rx));

        match tokio::time::timeout(std::time::Duration::from_millis(200), back_rx.recv())
            .await
            .expect("pressure-limited first remote read should arrive")
            .expect("relay should still be alive")
        {
            (h, RelayEvent::Data { epoch, bytes }) => {
                assert_eq!(h, handle);
                assert_eq!(epoch, 82);
                assert_eq!(
                    bytes.len(),
                    pressure_floor,
                    "pressure credit must cap the first awaited remote read before ready-drain batching"
                );
            }
            other => panic!("expected pressure-limited data batch, got {other:?}"),
        }

        task.await.unwrap();
        assert!(
            shutdown_called.load(Ordering::SeqCst),
            "EOF path should stop the writer half"
        );
    }

    #[tokio::test]
    async fn relay_remote_write_failed_notifies_main_loop() {
        let mut sockets = SocketSet::new(vec![]);
        let handle = mk_test_handle(&mut sockets);
        let flag = Arc::new(AtomicBool::new(false));
        let stream: RelayStream = Box::new(FailingWriteStream {
            shutdown_called: flag.clone(),
        });
        let (tx, rx) = mpsc::channel::<RelayCommand>(8);
        let (back_tx, mut back_rx) = mpsc::channel(8);
        let (_credit_tx, credit_rx) = relay_read_credit_channel();
        let task = tokio::spawn(run_relay(handle, 11, stream, rx, back_tx, credit_rx));

        tx.send(RelayCommand::Data(vec![1, 2, 3])).await.unwrap();
        task.await.unwrap();

        assert!(
            flag.load(Ordering::SeqCst),
            "write failure should still call stream.shutdown"
        );
        match back_rx
            .try_recv()
            .expect("write failure should notify main loop")
        {
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
    async fn relay_pending_remote_write_waits_for_relay_idle_timeout() {
        let mut sockets = SocketSet::new(vec![]);
        let handle = mk_test_handle(&mut sockets);
        let flag = Arc::new(AtomicBool::new(false));
        let stream: RelayStream = Box::new(PendingWriteStream {
            shutdown_called: flag.clone(),
        });
        let (tx, rx) = mpsc::channel::<RelayCommand>(8);
        let (back_tx, mut back_rx) = mpsc::channel(8);
        let (_credit_tx, credit_rx) = relay_read_credit_channel();
        let task = tokio::spawn(run_relay(handle, 17, stream, rx, back_tx, credit_rx));

        tx.send(RelayCommand::Data(vec![1, 2, 3])).await.unwrap();
        tokio::task::yield_now().await;
        tokio::time::advance(RELAY_WRITER_STOP_TIMEOUT - std::time::Duration::from_secs(1)).await;
        assert!(
            !task.is_finished(),
            "relay should not close before the former per-write timeout"
        );
        tokio::time::advance(std::time::Duration::from_secs(1)).await;
        assert!(
            !task.is_finished(),
            "normal upstream backpressure must not close the relay after a fixed per-write timeout"
        );
        assert!(
            back_rx.try_recv().is_err(),
            "pending write should not emit remote_write_timeout"
        );

        tokio::time::advance(RELAY_IDLE_TIMEOUT).await;
        task.await.unwrap();

        assert!(
            flag.load(Ordering::SeqCst),
            "relay idle cleanup should still call stream.shutdown"
        );
        match back_rx
            .try_recv()
            .expect("idle timeout should notify main loop")
        {
            (h, RelayEvent::Closed(close)) => {
                assert_eq!(h, handle);
                assert_eq!(close.epoch, 17);
                assert_eq!(close.direction, "timer");
                assert_eq!(close.reason, "idle_timeout");
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
        let (_credit_tx, credit_rx) = relay_read_credit_channel();
        let task = tokio::spawn(run_relay(handle, 23, stream, rx, back_tx, credit_rx));

        tx.send(RelayCommand::Data(vec![1, 2, 3])).await.unwrap();
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        assert!(
            write_polled.load(Ordering::SeqCst),
            "test stream should have entered the pending write"
        );
        match back_rx
            .try_recv()
            .expect("remote read should not wait for write timeout")
        {
            (h, RelayEvent::Data { epoch, bytes }) => {
                assert_eq!(h, handle);
                assert_eq!(epoch, 23);
                assert_eq!(bytes, b"remote-progress");
            }
            other => panic!("expected remote data before write timeout, got {other:?}"),
        }
        assert!(
            !shutdown_called.load(Ordering::SeqCst),
            "relay should still be alive while the pending write is not blocking remote reads"
        );

        tokio::time::advance(RELAY_IDLE_TIMEOUT).await;
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
        let (_credit_tx, credit_rx) = relay_read_credit_channel();
        let task = tokio::spawn(run_relay(handle, 29, stream, rx, back_tx, credit_rx));

        tx.send(RelayCommand::Finish).await.unwrap();
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        assert!(
            shutdown_called.load(Ordering::SeqCst),
            "local finish should shutdown only the remote write half"
        );
        match back_rx
            .recv()
            .await
            .expect("remote data should still reach main loop")
        {
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
        let stream: RelayStream = Box::new(IdleStream {
            shutdown_called: shutdown_called.clone(),
        });
        let (tx, rx) = mpsc::channel::<RelayCommand>(8);
        let (back_tx, mut back_rx) = mpsc::channel(8);
        let (_credit_tx, credit_rx) = relay_read_credit_channel();
        let task = tokio::spawn(run_relay(handle, 31, stream, rx, back_tx, credit_rx));

        tx.send(RelayCommand::Finish).await.unwrap();
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        assert!(
            shutdown_called.load(Ordering::SeqCst),
            "Finish should shutdown the remote write half"
        );

        tokio::time::advance(RELAY_HALF_CLOSED_IDLE_TIMEOUT - std::time::Duration::from_secs(1))
            .await;
        assert!(
            !task.is_finished(),
            "half-closed relay should stay open before the short idle timeout"
        );
        tokio::time::advance(std::time::Duration::from_secs(1)).await;
        task.await.unwrap();

        match back_rx
            .try_recv()
            .expect("half-closed idle should notify main loop")
        {
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
        let stream: RelayStream = Box::new(IdleStream {
            shutdown_called: flag.clone(),
        });
        let (tx, rx) = mpsc::channel::<RelayCommand>(8);
        let (back_tx, _back_rx) = mpsc::channel(8);
        let (_credit_tx, credit_rx) = relay_read_credit_channel();
        let task = tokio::spawn(run_relay(handle, 13, stream, rx, back_tx, credit_rx));

        tokio::time::advance(std::time::Duration::from_secs(89)).await;
        tx.send(RelayCommand::Data(vec![1, 2, 3])).await.unwrap(); // 活动（上行 write）→ 重置 idle 计时
        tokio::task::yield_now().await; // 让 relay 消费该活动并重建 sleep
        tokio::time::advance(std::time::Duration::from_secs(89)).await;
        assert!(
            !task.is_finished(),
            "活动重置了 idle 计时，第二个 89s 窗口内不应退出"
        );
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

        let reaped = reap_dead_slots(
            &reg,
            &mut sockets,
            &mut ctxs,
            &mut pool,
            1,
            DownlinkBackpressureConfig::default(),
            false,
        );
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
            !should_reap_slot(&pending, TcpState::Established, true, true, 0),
            "活跃 async-open 槽不能因 uplink_tx=None 被误 reap"
        );

        let mut opening = SocketCtx::new(443);
        opening.state = SocketState::OpeningRemote;
        assert!(
            should_reap_slot(&opening, TcpState::Established, true, true, 0),
            "旧 inline OpeningRemote 且无 uplink_tx 仍应视为卡住可 reap"
        );

        let listening = SocketCtx::new(443);
        assert!(
            !should_reap_slot(&listening, TcpState::Closed, false, false, 0),
            "空闲 Listening 槽永不 reap"
        );
    }

    #[test]
    fn reap_predicate_preserves_active_closewait_pending_downlink() {
        let mut ctx = SocketCtx::new(443);
        ctx.state = SocketState::Relaying;
        ctx.downlink_pending = vec![1, 2, 3];

        assert!(
            !should_reap_slot(&ctx, TcpState::CloseWait, true, true, 0),
            "active CloseWait with pending downlink must keep flushing instead of aborting"
        );

        ctx.downlink_pending.clear();
        assert!(
            should_reap_slot(&ctx, TcpState::CloseWait, true, true, 0),
            "CloseWait is reapable again once pending downlink drains"
        );

        ctx.downlink_pending = vec![1, 2, 3];
        ctx.downlink_pending_last_progress_secs = Some(10);
        ctx.downlink_pending_last_pending_bytes = 3;
        assert!(
            should_reap_slot(&ctx, TcpState::Closed, false, false, 14),
            "inactive pending that cannot be sent is undeliverable and should reap immediately"
        );
        assert!(
            !should_reap_slot(&ctx, TcpState::Closed, false, true, 14),
            "inactive pending with send capacity and recent progress should get a short flush grace"
        );
        assert!(
            should_reap_slot(&ctx, TcpState::Closed, false, true, 15),
            "inactive send-capable pending remains bounded after the no-progress grace"
        );
    }

    #[test]
    fn reap_predicate_preserves_active_closewait_live_relay() {
        let (tx, _rx) = mpsc::channel(1);
        let mut ctx = SocketCtx::new(443);
        ctx.state = SocketState::Relaying;
        ctx.uplink_tx = Some(tx);

        assert!(
            !should_reap_slot(&ctx, TcpState::CloseWait, true, true, 0),
            "active CloseWait with a live relay may still receive reverse/downlink bytes"
        );

        ctx.uplink_tx = None;
        assert!(
            should_reap_slot(&ctx, TcpState::CloseWait, true, true, 0),
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
            !should_reap_slot(&ctx, TcpState::CloseWait, true, true, 0),
            "post-Finish CloseWait must wait for the relay close event instead of dead-slot reap"
        );
    }

    #[test]
    fn reap_predicate_preserves_deferred_close_egress_from_dead_reap() {
        let mut ctx = SocketCtx::new(443);
        ctx.state = SocketState::Closing;
        ctx.pending_relay_close = Some(RelayClose {
            epoch: 7,
            direction: "local_to_remote",
            reason: "uplink_channel_closed",
        });
        ctx.pending_relay_close_since_secs = Some(10);
        ctx.pending_relay_close_last_egress_progress_secs = Some(10);
        ctx.pending_relay_close_last_egress_queue_bytes = 524_288;

        let cfg = DownlinkBackpressureConfig {
            high_bytes: 524_288,
            low_bytes: 131_072,
        };
        let snapshot = SocketCloseSnapshot {
            tcp_state: TcpState::CloseWait,
            active: true,
            can_send: true,
            can_recv: false,
            may_send: true,
            may_recv: false,
            send_capacity: 1_048_576,
            send_queue: 524_288,
            recv_queue: 0,
        };

        assert!(
            !should_reap_slot_with_snapshot(&mut ctx, snapshot, 14, cfg, false),
            "deferred egress close should not be reaped inside the bounded grace"
        );
        assert!(
            !should_reap_slot_with_snapshot(&mut ctx, snapshot, 15, cfg, false),
            "active send-capable close egress remains deliverable after the defer grace expires"
        );
    }

    #[test]
    fn reap_predicate_preserves_deferred_close_egress_after_drop_guard() {
        let mut ctx = SocketCtx::new(443);
        ctx.state = SocketState::Closing;
        ctx.pending_relay_close = Some(RelayClose {
            epoch: 7,
            direction: "local_to_remote",
            reason: "uplink_channel_closed",
        });
        ctx.pending_relay_close_since_secs = Some(10);
        ctx.pending_relay_close_last_egress_progress_secs = Some(10);
        ctx.pending_relay_close_last_egress_queue_bytes = 524_288;
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 524_288,
            low_bytes: 131_072,
        };
        let snapshot = SocketCloseSnapshot {
            tcp_state: TcpState::CloseWait,
            active: true,
            can_send: true,
            can_recv: false,
            may_send: true,
            may_recv: false,
            send_capacity: 1_048_576,
            send_queue: 524_288,
            recv_queue: 0,
        };

        assert!(
            !should_reap_slot_with_snapshot(&mut ctx, snapshot, 24, cfg, true),
            "drop/pressure close-drain gets the extended bounded grace"
        );
        assert!(
            !should_reap_slot_with_snapshot(&mut ctx, snapshot, 25, cfg, true),
            "drop/pressure close-drain expiry does not authorize dead reap while the socket can still send"
        );

        ctx.pending_relay_close_last_egress_progress_secs = Some(39);
        ctx.pending_relay_close_last_egress_queue_bytes = 400_000;
        assert!(
            !should_reap_slot_with_snapshot(&mut ctx, snapshot, 40, cfg, true),
            "the hard close-egress defer bound belongs to finish/rearm, not data-dropping dead reap"
        );
    }

    #[test]
    fn reap_predicate_preserves_active_send_capable_deferred_close_tail() {
        let mut ctx = SocketCtx::new(443);
        ctx.state = SocketState::Closing;
        ctx.downlink_pending = vec![0; 376_912];
        ctx.pending_relay_close = Some(RelayClose {
            epoch: 7,
            direction: "local_to_remote",
            reason: "uplink_channel_closed",
        });
        ctx.pending_relay_close_since_secs = Some(10);
        ctx.pending_relay_close_last_progress_secs = Some(38);
        ctx.pending_relay_close_last_pending_bytes = 548_944;
        ctx.pending_relay_close_last_egress_progress_secs = Some(38);
        ctx.pending_relay_close_last_egress_queue_bytes = 892_928;
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 524_288,
            low_bytes: 131_072,
        };
        let snapshot = SocketCloseSnapshot {
            tcp_state: TcpState::Established,
            active: true,
            can_send: true,
            can_recv: false,
            may_send: true,
            may_recv: true,
            send_capacity: 1_048_576,
            send_queue: 892_928,
            recv_queue: 0,
        };

        assert!(
            !should_reap_slot_with_snapshot(&mut ctx, snapshot, 68, cfg, true),
            "active send-capable deferred close still has deliverable tail bytes and must not be dead-reaped"
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
        ctx.pending_relay_close_last_progress_secs = Some(13);
        ctx.pending_relay_close_last_pending_bytes = 3;

        assert!(
            should_reap_slot(&ctx, TcpState::Closed, false, false, 14),
            "deferred close pending without local send capacity should reap immediately"
        );
        assert!(
            !should_reap_slot(&ctx, TcpState::Closed, false, true, 14),
            "deferred close pending should get a short grace window to flush"
        );
        assert!(
            !should_reap_slot(&ctx, TcpState::Closed, false, true, 15),
            "deferred close pending should keep draining when recent progress exists"
        );
        assert!(
            should_reap_slot(&ctx, TcpState::Closed, false, true, 18),
            "deferred close pending should still be bounded after no-progress grace"
        );

        ctx.pending_relay_close = None;
        ctx.pending_relay_close_since_secs = None;
        ctx.pending_relay_close_last_progress_secs = None;
        ctx.pending_relay_close_last_pending_bytes = 0;
        ctx.downlink_pending_last_progress_secs = Some(13);
        ctx.downlink_pending_last_pending_bytes = 3;
        assert!(
            should_reap_slot(&ctx, TcpState::Closed, false, false, 14),
            "non-deferred inactive pending without local send capacity should reap immediately"
        );
        assert!(
            !should_reap_slot(&ctx, TcpState::Closed, false, true, 14),
            "non-deferred inactive pending should use the same recent-progress grace"
        );
        assert!(
            should_reap_slot(&ctx, TcpState::Closed, false, true, 18),
            "non-deferred inactive pending should still be bounded"
        );
    }

    #[test]
    fn deferred_close_drain_progress_tracks_only_pending_decrease() {
        let mut ctx = SocketCtx::new(443);
        ctx.downlink_pending = vec![0; 100];
        ctx.pending_relay_close = Some(RelayClose {
            epoch: 1,
            direction: "remote_to_local",
            reason: "remote_eof",
        });
        ctx.pending_relay_close_since_secs = Some(10);
        ctx.pending_relay_close_last_progress_secs = Some(10);
        ctx.pending_relay_close_last_pending_bytes = 100;

        note_deferred_close_drain_progress(&mut ctx, 11, 0);
        assert_eq!(ctx.pending_relay_close_last_progress_secs, Some(10));
        assert_eq!(ctx.pending_relay_close_last_pending_bytes, 100);

        ctx.downlink_pending.truncate(60);
        note_deferred_close_drain_progress(&mut ctx, 12, 0);
        assert_eq!(ctx.pending_relay_close_last_progress_secs, Some(12));
        assert_eq!(ctx.pending_relay_close_last_pending_bytes, 60);

        ctx.downlink_pending.resize(80, 0);
        note_deferred_close_drain_progress(&mut ctx, 13, 0);
        assert_eq!(
            ctx.pending_relay_close_last_progress_secs,
            Some(12),
            "growth must not fake drain progress"
        );
        assert_eq!(ctx.pending_relay_close_last_pending_bytes, 80);

        ctx.pending_relay_close_last_pending_bytes = 0;
        ctx.pending_relay_close_last_progress_secs = Some(1);
        note_deferred_close_drain_progress(&mut ctx, 14, 0);
        assert_eq!(
            ctx.pending_relay_close_last_progress_secs,
            Some(14),
            "missing baseline should restart from the first observed pending size"
        );
        assert_eq!(ctx.pending_relay_close_last_pending_bytes, 80);
    }

    #[test]
    fn downlink_pending_progress_tracks_observation_accept_and_growth() {
        let mut ctx = SocketCtx::new(443);
        ctx.downlink_pending = vec![0; 100];
        note_downlink_pending_progress(&mut ctx, 10, 0);
        assert_eq!(ctx.downlink_pending_last_progress_secs, Some(10));
        assert_eq!(ctx.downlink_pending_last_pending_bytes, 100);

        note_downlink_pending_progress(&mut ctx, 11, 0);
        assert_eq!(
            ctx.downlink_pending_last_progress_secs,
            Some(10),
            "unchanged pending must not fake new progress"
        );
        assert_eq!(ctx.downlink_pending_last_pending_bytes, 100);

        ctx.downlink_pending.resize(150, 0);
        note_downlink_pending_progress(&mut ctx, 12, 0);
        assert_eq!(
            ctx.downlink_pending_last_progress_secs,
            Some(10),
            "growth without accepted bytes must not refresh progress"
        );
        assert_eq!(ctx.downlink_pending_last_pending_bytes, 150);

        ctx.downlink_pending.resize(180, 0);
        note_downlink_pending_progress(&mut ctx, 13, 25);
        assert_eq!(
            ctx.downlink_pending_last_progress_secs,
            Some(13),
            "accepted bytes are progress even if final pending grew"
        );
        assert_eq!(ctx.downlink_pending_last_pending_bytes, 180);

        ctx.downlink_pending.clear();
        note_downlink_pending_progress(&mut ctx, 14, 0);
        assert!(ctx.downlink_pending_last_progress_secs.is_none());
        assert_eq!(ctx.downlink_pending_last_pending_bytes, 0);
    }

    #[test]
    fn close_pending_accounting_classifies_pending_close_taxonomy() {
        let mut ctx = SocketCtx::new(443);
        ctx.state = SocketState::Relaying;
        ctx.downlink_pending = vec![0; 224_765];

        let terminal = SocketCloseSnapshot {
            tcp_state: TcpState::Closed,
            active: false,
            can_send: false,
            can_recv: false,
            may_send: false,
            may_recv: false,
            send_capacity: 1_048_576,
            send_queue: 0,
            recv_queue: 0,
        };
        let accounting = close_pending_accounting(&ctx, terminal);
        assert_eq!(accounting.class, ClosePendingClass::TerminalClosedNoSend);
        assert_eq!(accounting.pending_bytes, 224_765);
        assert_eq!(accounting.terminal_pending_reap_bytes, 224_765);

        let active_no_send = SocketCloseSnapshot {
            tcp_state: TcpState::Established,
            active: true,
            can_send: false,
            may_send: true,
            may_recv: true,
            send_queue: 1_048_576,
            ..terminal
        };
        let accounting = close_pending_accounting(&ctx, active_no_send);
        assert_eq!(accounting.class, ClosePendingClass::ActiveNoSend);
        assert_eq!(accounting.pending_bytes, 224_765);
        assert_eq!(accounting.terminal_pending_reap_bytes, 0);

        let active_send_capable = SocketCloseSnapshot {
            can_send: true,
            ..active_no_send
        };
        let accounting = close_pending_accounting(&ctx, active_send_capable);
        assert_eq!(accounting.class, ClosePendingClass::ActiveSendCapable);
        assert_eq!(accounting.terminal_pending_reap_bytes, 0);

        let inactive_send_capable = SocketCloseSnapshot {
            active: false,
            can_send: true,
            ..terminal
        };
        let accounting = close_pending_accounting(&ctx, inactive_send_capable);
        assert_eq!(accounting.class, ClosePendingClass::InactiveSendCapable);
        assert_eq!(accounting.terminal_pending_reap_bytes, 0);

        let inactive_no_send = SocketCloseSnapshot {
            tcp_state: TcpState::CloseWait,
            active: false,
            can_send: false,
            can_recv: false,
            may_send: true,
            may_recv: false,
            send_capacity: 1_048_576,
            send_queue: 1_048_576,
            recv_queue: 0,
        };
        let accounting = close_pending_accounting(&ctx, inactive_no_send);
        assert_eq!(accounting.class, ClosePendingClass::InactiveNoSend);
        assert_eq!(accounting.terminal_pending_reap_bytes, 0);

        ctx.downlink_pending.clear();
        let accounting = close_pending_accounting(&ctx, terminal);
        assert_eq!(accounting.class, ClosePendingClass::NoPending);
        assert_eq!(accounting.pending_bytes, 0);
        assert_eq!(accounting.terminal_pending_reap_bytes, 0);
    }

    #[test]
    fn close_egress_accounting_reports_send_queue_without_app_pending() {
        let knife14bu_close = SocketCloseSnapshot {
            tcp_state: TcpState::CloseWait,
            active: true,
            can_send: true,
            can_recv: false,
            may_send: true,
            may_recv: false,
            send_capacity: 1_048_576,
            send_queue: 524_288,
            recv_queue: 0,
        };

        let accounting = close_egress_accounting(knife14bu_close);

        assert_eq!(accounting.class, CloseEgressClass::ActiveSendCapable);
        assert_eq!(accounting.bytes, 524_288);
        assert!(accounting.drain_candidate);
    }

    #[test]
    fn tcp_lifecycle_transition_diag_reports_previous_state_and_context() {
        let mut sockets = SocketSet::new(vec![]);
        let handle = sockets.add(build_listener_socket(&ListenerSpec { local_port: 443 }));
        let (tx, _rx) = mpsc::channel(1);
        let mut ctx = SocketCtx::new(443);
        ctx.state = SocketState::Relaying;
        ctx.uplink_tx = Some(tx);
        ctx.downlink_pending = vec![0; 41_208];
        ctx.downlink_diag.note_remote_payload(221_884, 41_208);
        ctx.downlink_diag.note_send_slice_ok(180_676, 41_208);

        let established = SocketCloseSnapshot {
            tcp_state: TcpState::Established,
            active: true,
            can_send: true,
            can_recv: true,
            may_send: true,
            may_recv: true,
            send_capacity: 1_048_576,
            send_queue: 65_536,
            recv_queue: 0,
        };
        assert!(
            observe_tcp_lifecycle(handle, &mut ctx, established, 10, "remote_payload").is_none(),
            "first observation seeds previous state without logging a transition"
        );

        let closed = SocketCloseSnapshot {
            tcp_state: TcpState::Closed,
            active: false,
            can_send: false,
            can_recv: false,
            may_send: false,
            may_recv: false,
            send_capacity: 1_048_576,
            send_queue: 0,
            recv_queue: 0,
        };
        let line = observe_tcp_lifecycle(handle, &mut ctx, closed, 15, "dead_slot_reap")
            .expect("state transition should produce a diagnostic line");

        assert!(line.contains("tcp-lifecycle-transition"), "{line}");
        assert!(line.contains("source=dead_slot_reap"), "{line}");
        assert!(line.contains("prev_source=remote_payload"), "{line}");
        assert!(
            line.contains("prev_state=Established state=Closed"),
            "{line}"
        );
        assert!(line.contains("prev_active=true active=false"), "{line}");
        assert!(line.contains("ctx_state=Relaying"), "{line}");
        assert!(line.contains("uplink_tx=true"), "{line}");
        assert!(line.contains("local_fin_sent=false"), "{line}");
        assert!(line.contains("pending=41208"), "{line}");
        assert!(line.contains("remote_to_global_rx_bytes=221884"), "{line}");
        assert!(line.contains("send_slice_accepted=180676"), "{line}");
        assert!(line.contains("terminal_candidate=true"), "{line}");
    }

    #[test]
    fn remote_payload_allowed_after_local_finish_without_uplink_sender() {
        let mut ctx = SocketCtx::new(443);
        ctx.state = SocketState::Relaying;
        ctx.local_fin_sent = true;
        ctx.uplink_tx = None;
        let active_closewait = SocketCloseSnapshot {
            tcp_state: TcpState::CloseWait,
            active: true,
            can_send: true,
            can_recv: false,
            may_send: true,
            may_recv: false,
            send_capacity: 1_048_576,
            send_queue: 0,
            recv_queue: 0,
        };

        assert!(
            relay_allows_remote_payload(&ctx),
            "post-Finish read-only relay must still accept remote payloads"
        );
        assert_eq!(
            remote_payload_disposition(&ctx, active_closewait),
            RemotePayloadDisposition::Accept,
            "post-Finish read-only relay must still accept remote payloads"
        );
    }

    #[test]
    fn remote_payload_rejected_after_terminal_no_send_socket() {
        let (tx, _rx) = mpsc::channel(1);
        let mut ctx = SocketCtx::new(443);
        ctx.state = SocketState::Relaying;
        ctx.uplink_tx = Some(tx);
        let terminal = SocketCloseSnapshot {
            tcp_state: TcpState::Closed,
            active: false,
            can_send: false,
            can_recv: false,
            may_send: false,
            may_recv: false,
            send_capacity: 1_048_576,
            send_queue: 29_568,
            recv_queue: 0,
        };

        assert_eq!(
            remote_payload_disposition(&ctx, terminal),
            RemotePayloadDisposition::TerminalNoSend,
            "terminal no-send socket must not accumulate late remote payload in downlink_pending"
        );
    }

    #[tokio::test]
    async fn terminal_remote_payload_rearms_slot_and_stales_followup_payloads() {
        let mut sockets = SocketSet::new(vec![]);
        let handle = sockets.add(build_listener_socket(&ListenerSpec { local_port: 443 }));
        sockets.get_mut::<TcpSocket>(handle).abort();

        let (tx, _rx) = mpsc::channel(1);
        let mut ctx = SocketCtx::new(443);
        ctx.state = SocketState::Relaying;
        ctx.conn_epoch = 9;
        ctx.uplink_tx = Some(tx);
        let mut ctxs = HashMap::from([(handle, ctx)]);
        let mut pool = FakeIpPool::new();
        let mut device = DnsInjectRecorder::default();
        let mut iface = Interface::new(
            SmolConfig::new(smoltcp::wire::HardwareAddress::Ip),
            &mut device,
            smoltcp::time::Instant::now(),
        );
        iface.update_ip_addrs(|addrs| {
            addrs
                .push(IpCidr::new(IpAddress::v4(10, 0, 0, 2), 24))
                .unwrap();
        });
        let mut pacer = DownlinkEgressPacer::new(DEFAULT_DOWNLINK_EGRESS_IMMEDIATE_BYTES);
        let mut drop_debt = DownlinkEgressDropDebt::default();
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 524_288,
            low_bytes: 131_072,
        };

        let accepted = handle_remote_payload(
            handle,
            9,
            vec![7; 4096],
            &mut sockets,
            &mut ctxs,
            &mut iface,
            &mut device,
            &mut pool,
            42,
            DEFAULT_DOWNLINK_FLUSH_MAX_BYTES,
            &mut pacer,
            &mut drop_debt,
            cfg,
            1200,
            false,
        )
        .await
        .unwrap();

        let ctx = ctxs.get(&handle).unwrap();
        assert_eq!(accepted, 0);
        assert_eq!(ctx.state, SocketState::Listening);
        assert_eq!(
            ctx.conn_epoch, 10,
            "terminal late payload must retire the old relay epoch"
        );
        assert!(ctx.uplink_tx.is_none());
        assert!(ctx.downlink_pending.is_empty());

        let accepted = handle_remote_payload(
            handle,
            9,
            vec![8; 4096],
            &mut sockets,
            &mut ctxs,
            &mut iface,
            &mut device,
            &mut pool,
            43,
            DEFAULT_DOWNLINK_FLUSH_MAX_BYTES,
            &mut pacer,
            &mut drop_debt,
            cfg,
            1200,
            false,
        )
        .await
        .unwrap();

        let ctx = ctxs.get(&handle).unwrap();
        assert_eq!(accepted, 0);
        assert_eq!(ctx.state, SocketState::Listening);
        assert_eq!(
            ctx.conn_epoch, 10,
            "old relay payloads after terminal rearm must stay stale"
        );
        assert!(ctx.downlink_pending.is_empty());
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

        let reaped = reap_dead_slots(
            &reg,
            &mut sockets,
            &mut ctxs,
            &mut pool,
            1,
            DownlinkBackpressureConfig::default(),
            false,
        );
        assert_eq!(reaped, 1, "应回收本地已关闭的 async-open 槽");

        let closed_ctx = ctxs.get(&closed_pending).unwrap();
        assert_eq!(closed_ctx.state, SocketState::Listening);
        assert_eq!(
            closed_ctx.conn_epoch, 10,
            "reap 应 bump epoch 让迟到 open 结果失效"
        );
        assert!(
            closed_ctx.fake_ip.is_none(),
            "reap 应释放 in-flight flow fake-IP"
        );
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
        socket.set_nagle_enabled(true);
        socket.set_ack_delay(Some(smoltcp::time::Duration::from_millis(10)));
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
            relay_read_credit_tx: None,
            relay_read_credit_progress_generation_sent: 0,
            local_fin_sent: true,
            local_eof_sent: true,
            local_fin_pending_since_secs: Some(3),
            local_fin_last_remote_progress_secs: Some(4),
            downlink_pending: Vec::new(),
            pending_relay_close: None,
            pending_relay_close_since_secs: None,
            pending_relay_close_last_progress_secs: None,
            pending_relay_close_last_pending_bytes: 0,
            pending_relay_close_last_egress_progress_secs: Some(5),
            pending_relay_close_last_egress_queue_bytes: 12_345,
            downlink_pending_last_progress_secs: Some(9),
            downlink_pending_last_pending_bytes: 42,
            fake_ip: Some(ip),
            conn_epoch: 7,
            uplink_buffer: vec![1, 2, 3],
            downlink_diag,
            downlink_egress_clock: DownlinkEgressClock {
                last_send_queue: Some(12_345),
                drain_credit_bytes: 1024,
                drop_debt_generation_seen: 3,
            },
            downlink_credit_controller: DownlinkCreditController {
                read_credit_ceiling_bytes: 32 * 1024,
                adaptive_ack_drain_bytes: 4 * 1024,
                ack_cadence_boost_bytes: 4 * 1024,
                egress_payload_credit_bytes: 0,
                egress_progress_generation: 0,
                headroom_debt_bytes: 4096,
                no_egress_progress_streak: 3,
            },
            last_tcp_lifecycle: None,
            reverse_window_last_log_secs: Some(12),
        };

        rearm_socket(&mut socket, &mut ctx, &mut pool, 1);

        assert_eq!(ctx.state, SocketState::Listening);
        assert!(ctx.uplink_tx.is_none());
        assert!(ctx.relay_read_credit_tx.is_none());
        assert!(!ctx.local_fin_sent, "rearm 应清空本地 FIN 发送状态");
        assert!(!ctx.local_eof_sent, "rearm 应清空本地 EOF 发送状态");
        assert!(
            ctx.local_fin_pending_since_secs.is_none(),
            "rearm 应清空 deferred local FIN 状态"
        );
        assert!(
            ctx.local_fin_last_remote_progress_secs.is_none(),
            "rearm 应清空 deferred local FIN 进展时间"
        );
        assert!(
            ctx.pending_relay_close_last_egress_progress_secs.is_none(),
            "rearm 应清空 deferred close egress 进展时间"
        );
        assert_eq!(
            ctx.pending_relay_close_last_egress_queue_bytes, 0,
            "rearm 应清空 deferred close egress 队列状态"
        );
        assert!(ctx.fake_ip.is_none(), "rearm 应清空 fake_ip");
        assert!(
            ctx.uplink_buffer.is_empty(),
            "rearm 应清空 uplink_buffer（M3 patch）"
        );
        assert_eq!(
            ctx.downlink_diag.downlink_pending_high_water, 0,
            "rearm 应清空当前 flow 的 downlink diagnostics"
        );
        assert_eq!(
            ctx.downlink_egress_clock,
            DownlinkEgressClock::default(),
            "rearm 应清空当前 flow 的 egress progress clock"
        );
        assert_eq!(
            ctx.downlink_credit_controller,
            DownlinkCreditController::default(),
            "rearm 应清空当前 flow 的 local credit controller"
        );
        assert!(
            ctx.reverse_window_last_log_secs.is_none(),
            "rearm 应清空 reverse-window diagnostics 限流状态"
        );
        assert_eq!(
            ctx.conn_epoch, 8,
            "rearm 应 bump conn_epoch（让在飞 open 失效，M3）"
        );
        assert!(
            !socket.nagle_enabled(),
            "rearm 应恢复本地 virtual-link TCP no-Nagle 策略"
        );
        assert!(
            socket.ack_delay().is_none(),
            "rearm 应恢复本地 virtual-link TCP no-delayed-ACK 策略"
        );
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
            DownlinkBackpressureConfig::default(),
        );

        let ctx = socket_ctxs.get(&handle).unwrap();
        assert_eq!(
            ctx.state,
            SocketState::Relaying,
            "stale close must not rearm a new epoch"
        );
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
            DownlinkBackpressureConfig::default(),
        );

        let ctx = socket_ctxs.get_mut(&handle).unwrap();
        assert!(
            keep_dirty,
            "pending downlink must stay dirty until it drains"
        );
        assert_eq!(ctx.state, SocketState::Closing);
        assert!(ctx.uplink_tx.is_none());
        assert_eq!(ctx.downlink_pending, vec![1, 2, 3]);
        assert!(ctx.pending_relay_close.is_some());
        assert_eq!(ctx.pending_relay_close_since_secs, Some(1));
        assert_eq!(ctx.pending_relay_close_last_progress_secs, Some(1));
        assert_eq!(ctx.pending_relay_close_last_pending_bytes, 3);
        assert_eq!(ctx.downlink_pending_last_progress_secs, Some(1));
        assert_eq!(ctx.downlink_pending_last_pending_bytes, 3);
        assert_eq!(
            ctx.conn_epoch, 5,
            "deferred close must not bump epoch before drain"
        );

        ctx.downlink_pending.clear();
        let socket = sockets.get_mut::<TcpSocket>(handle);
        assert!(finish_deferred_relay_close_if_drained(
            handle,
            socket,
            ctx,
            &mut pool,
            2,
            DownlinkBackpressureConfig::default(),
            false
        ));
        assert_eq!(ctx.state, SocketState::Listening);
        assert!(ctx.pending_relay_close.is_none());
        assert!(ctx.pending_relay_close_since_secs.is_none());
        assert!(ctx.pending_relay_close_last_progress_secs.is_none());
        assert_eq!(ctx.pending_relay_close_last_pending_bytes, 0);
        assert!(ctx.downlink_pending_last_progress_secs.is_none());
        assert_eq!(ctx.downlink_pending_last_pending_bytes, 0);
        assert_eq!(ctx.conn_epoch, 6);
    }

    #[test]
    fn deferred_close_egress_starts_for_send_capable_closewait_queue() {
        let (tx, _rx) = mpsc::channel(1);
        let mut ctx = SocketCtx::new(12345);
        ctx.state = SocketState::Relaying;
        ctx.conn_epoch = 5;
        ctx.uplink_tx = Some(tx);
        let snapshot = SocketCloseSnapshot {
            tcp_state: TcpState::CloseWait,
            active: true,
            can_send: true,
            can_recv: false,
            may_send: true,
            may_recv: false,
            send_capacity: 1_048_576,
            send_queue: 524_288,
            recv_queue: 0,
        };
        let close = RelayClose {
            epoch: 5,
            direction: "local_to_remote",
            reason: "uplink_channel_closed",
        };
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 524_288,
            low_bytes: 131_072,
        };

        assert!(start_deferred_close_for_egress_if_needed(
            &mut ctx, close, snapshot, cfg, 10
        ));
        assert_eq!(ctx.state, SocketState::Closing);
        assert!(ctx.uplink_tx.is_none());
        assert_eq!(
            ctx.pending_relay_close.map(|c| c.reason),
            Some("uplink_channel_closed")
        );
        assert_eq!(ctx.pending_relay_close_since_secs, Some(10));
        assert_eq!(ctx.pending_relay_close_last_egress_progress_secs, Some(10));
        assert_eq!(ctx.pending_relay_close_last_egress_queue_bytes, 524_288);
    }

    #[test]
    fn deferred_close_egress_starts_for_send_capable_tail_above_low_watermark() {
        let (tx, _rx) = mpsc::channel(1);
        let mut ctx = SocketCtx::new(12345);
        ctx.state = SocketState::Relaying;
        ctx.conn_epoch = 5;
        ctx.uplink_tx = Some(tx);
        let snapshot = SocketCloseSnapshot {
            tcp_state: TcpState::CloseWait,
            active: true,
            can_send: true,
            can_recv: false,
            may_send: true,
            may_recv: false,
            send_capacity: 1_048_576,
            send_queue: 217_725,
            recv_queue: 0,
        };
        let close = RelayClose {
            epoch: 5,
            direction: "local_to_remote",
            reason: "uplink_channel_closed",
        };
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 524_288,
            low_bytes: 131_072,
        };

        assert!(
            start_deferred_close_for_egress_if_needed(&mut ctx, close, snapshot, cfg, 10),
            "send-capable close tails above the drain-complete low watermark must enter close-drain even when below high pressure"
        );
        assert_eq!(ctx.state, SocketState::Closing);
        assert!(ctx.uplink_tx.is_none());
        assert_eq!(
            ctx.pending_relay_close.map(|c| c.reason),
            Some("uplink_channel_closed")
        );
        assert_eq!(ctx.pending_relay_close_since_secs, Some(10));
        assert_eq!(ctx.pending_relay_close_last_egress_progress_secs, Some(10));
        assert_eq!(ctx.pending_relay_close_last_egress_queue_bytes, 217_725);
    }

    #[test]
    fn pending_downlink_close_deferral_is_direction_agnostic() {
        let (tx, _rx) = mpsc::channel(1);
        let mut ctx = SocketCtx::new(12345);
        ctx.state = SocketState::Relaying;
        ctx.conn_epoch = 5;
        ctx.uplink_tx = Some(tx);
        ctx.downlink_pending = vec![1, 2, 3, 4];
        let close = RelayClose {
            epoch: 5,
            direction: "local_to_remote",
            reason: "uplink_channel_closed",
        };

        assert!(start_deferred_close_for_pending_downlink(
            &mut ctx, close, 12
        ));
        assert_eq!(ctx.state, SocketState::Closing);
        assert!(ctx.uplink_tx.is_none());
        assert_eq!(
            ctx.pending_relay_close.map(|c| c.reason),
            Some("uplink_channel_closed")
        );
        assert_eq!(ctx.pending_relay_close_since_secs, Some(12));
        assert_eq!(ctx.pending_relay_close_last_progress_secs, Some(12));
        assert_eq!(ctx.pending_relay_close_last_pending_bytes, 4);
        assert_eq!(ctx.downlink_pending_last_progress_secs, Some(12));
        assert_eq!(ctx.downlink_pending_last_pending_bytes, 4);
        assert_eq!(
            ctx.conn_epoch, 5,
            "pending deferral must not rearm or bump epoch"
        );
    }

    #[test]
    fn pending_downlink_close_deferral_ignores_empty_pending() {
        let (tx, _rx) = mpsc::channel(1);
        let mut ctx = SocketCtx::new(12345);
        ctx.state = SocketState::Relaying;
        ctx.conn_epoch = 5;
        ctx.uplink_tx = Some(tx);
        let close = RelayClose {
            epoch: 5,
            direction: "local_to_remote",
            reason: "uplink_channel_closed",
        };

        assert!(!start_deferred_close_for_pending_downlink(
            &mut ctx, close, 12
        ));
        assert_eq!(ctx.state, SocketState::Relaying);
        assert!(ctx.uplink_tx.is_some());
        assert!(ctx.pending_relay_close.is_none());
        assert!(ctx.pending_relay_close_since_secs.is_none());
        assert!(ctx.downlink_pending_last_progress_secs.is_none());
    }

    #[test]
    fn deferred_close_egress_waits_until_low_or_grace() {
        let mut ctx = SocketCtx::new(12345);
        ctx.state = SocketState::Closing;
        ctx.pending_relay_close = Some(RelayClose {
            epoch: 5,
            direction: "local_to_remote",
            reason: "uplink_channel_closed",
        });
        ctx.pending_relay_close_since_secs = Some(10);
        ctx.pending_relay_close_last_egress_progress_secs = Some(10);
        ctx.pending_relay_close_last_egress_queue_bytes = 524_288;
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 524_288,
            low_bytes: 131_072,
        };
        let mut snapshot = SocketCloseSnapshot {
            tcp_state: TcpState::CloseWait,
            active: true,
            can_send: true,
            can_recv: false,
            may_send: true,
            may_recv: false,
            send_capacity: 1_048_576,
            send_queue: 524_288,
            recv_queue: 0,
        };

        assert!(
            should_keep_deferred_close_for_egress(&mut ctx, snapshot, cfg, 14, false),
            "high egress queue within grace should keep the close deferred"
        );

        snapshot.send_queue = 300_000;
        assert!(
            should_keep_deferred_close_for_egress(&mut ctx, snapshot, cfg, 14, false),
            "queue drain progress should refresh the bounded grace"
        );
        assert_eq!(ctx.pending_relay_close_last_egress_progress_secs, Some(14));
        assert_eq!(ctx.pending_relay_close_last_egress_queue_bytes, 300_000);

        assert!(
            should_keep_deferred_close_for_egress(&mut ctx, snapshot, cfg, 18, false),
            "still above low but within refreshed grace should keep waiting"
        );
        assert!(
            !should_keep_deferred_close_for_egress(&mut ctx, snapshot, cfg, 19, false),
            "above low without progress must remain bounded"
        );

        snapshot.send_queue = 131_072;
        assert!(
            !should_keep_deferred_close_for_egress(&mut ctx, snapshot, cfg, 20, false),
            "low watermark means queued egress drained enough to finish close"
        );
    }

    #[test]
    fn deferred_close_sends_local_eof_after_pending_drains() {
        let mut ctx = SocketCtx::new(12345);
        ctx.state = SocketState::Closing;
        ctx.pending_relay_close = Some(RelayClose {
            epoch: 5,
            direction: "remote_to_local",
            reason: "remote_eof",
        });
        ctx.downlink_pending = vec![1, 2, 3];
        let snapshot = SocketCloseSnapshot {
            tcp_state: TcpState::Established,
            active: true,
            can_send: true,
            can_recv: false,
            may_send: true,
            may_recv: true,
            send_capacity: 1_048_576,
            send_queue: 720_896,
            recv_queue: 0,
        };

        assert!(
            !should_send_local_eof_for_deferred_close(&ctx, snapshot),
            "remote EOF must not send local FIN before terminal pending bytes enter smoltcp"
        );

        ctx.downlink_pending.clear();
        assert!(
            should_send_local_eof_for_deferred_close(&ctx, snapshot),
            "once terminal pending is accepted, local FIN should be queued behind the egress tail"
        );

        ctx.local_eof_sent = true;
        assert!(
            !should_send_local_eof_for_deferred_close(&ctx, snapshot),
            "local FIN must be idempotent while the TCP state machine drains"
        );
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
        assert_eq!(
            ctx.uplink_buffer.len(),
            MAX_UPLINK_BUFFER,
            "丢弃不改变 buffer"
        );
    }

    #[test]
    fn established_uplink_batch_drains_multiple_payloads_in_one_pass() {
        use std::collections::VecDeque;

        let (tx, mut rx) = mpsc::channel(8);
        let mut ctx = SocketCtx::new(80);
        ctx.uplink_tx = Some(tx);
        let mut payloads: VecDeque<Vec<u8>> =
            VecDeque::from([b"one".to_vec(), b"two".to_vec(), b"three".to_vec()]);

        assert!(matches!(
            pump_established_uplink(&mut ctx, TcpState::Established, 0, || payloads.pop_front()),
            EstablishedUplink::Handled
        ));
        assert!(
            payloads.is_empty(),
            "one dirty pass should drain the readable batch"
        );
        assert_eq!(ctx.state, SocketState::Relaying);

        for expected in [b"one".as_slice(), b"two".as_slice(), b"three".as_slice()] {
            match rx.try_recv().expect("payload should be forwarded") {
                RelayCommand::Data(got) => assert_eq!(got, expected),
                other => panic!("expected data command, got {other:?}"),
            }
        }
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn established_uplink_batch_stops_at_fairness_cap() {
        use std::collections::VecDeque;

        let (tx, mut rx) = mpsc::channel(MAX_ESTABLISHED_UPLINK_BATCH + 2);
        let mut ctx = SocketCtx::new(80);
        ctx.uplink_tx = Some(tx);
        let mut payloads: VecDeque<Vec<u8>> = (0..=MAX_ESTABLISHED_UPLINK_BATCH)
            .map(|i| vec![i as u8])
            .collect();

        assert!(matches!(
            pump_established_uplink(&mut ctx, TcpState::Established, 0, || payloads.pop_front()),
            EstablishedUplink::Handled
        ));
        assert_eq!(
            payloads.len(),
            1,
            "batch cap should leave remaining readable data for the next dirty pass"
        );
        for _ in 0..MAX_ESTABLISHED_UPLINK_BATCH {
            assert!(matches!(
                rx.try_recv().expect("batch payload should be sent"),
                RelayCommand::Data(_)
            ));
        }
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn established_uplink_full_channel_does_not_consume_payload() {
        use std::collections::VecDeque;

        let (tx, _rx) = mpsc::channel(1);
        tx.try_send(RelayCommand::Data(b"occupied".to_vec()))
            .unwrap();
        let mut ctx = SocketCtx::new(80);
        ctx.uplink_tx = Some(tx);
        let mut payloads: VecDeque<Vec<u8>> = VecDeque::from([b"must-stay".to_vec()]);

        assert!(matches!(
            pump_established_uplink(&mut ctx, TcpState::Established, 0, || payloads.pop_front()),
            EstablishedUplink::Handled
        ));
        assert_eq!(
            payloads.len(),
            1,
            "mpsc full must preserve smoltcp bytes for TCP-window backpressure"
        );
    }

    #[test]
    fn established_uplink_defers_finish_after_payloads_drain() {
        let (tx, mut rx) = mpsc::channel(4);
        let mut ctx = SocketCtx::new(80);
        ctx.uplink_tx = Some(tx);

        assert!(matches!(
            pump_established_uplink(&mut ctx, TcpState::CloseWait, 10, || None),
            EstablishedUplink::Handled
        ));
        assert!(!ctx.local_fin_sent);
        assert_eq!(ctx.local_fin_pending_since_secs, Some(10));
        assert!(
            rx.try_recv().is_err(),
            "initial CloseWait should defer Finish"
        );

        assert!(matches!(
            pump_established_uplink(&mut ctx, TcpState::CloseWait, 14, || None),
            EstablishedUplink::Handled
        ));
        assert!(!ctx.local_fin_sent);
        assert!(rx.try_recv().is_err(), "defer window has not expired");

        assert!(matches!(
            pump_established_uplink(&mut ctx, TcpState::CloseWait, 15, || None),
            EstablishedUplink::Handled
        ));
        assert!(ctx.local_fin_sent);
        assert!(matches!(
            rx.try_recv().expect("finish should be sent"),
            RelayCommand::Finish
        ));
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn established_uplink_remote_progress_extends_finish_defer() {
        let (tx, mut rx) = mpsc::channel(4);
        let mut ctx = SocketCtx::new(80);
        ctx.uplink_tx = Some(tx);

        assert!(matches!(
            pump_established_uplink(&mut ctx, TcpState::CloseWait, 10, || None),
            EstablishedUplink::Handled
        ));
        note_local_finish_remote_progress(&mut ctx, 13);

        assert!(matches!(
            pump_established_uplink(&mut ctx, TcpState::CloseWait, 15, || None),
            EstablishedUplink::Handled
        ));
        assert!(!ctx.local_fin_sent);
        assert!(
            rx.try_recv().is_err(),
            "remote progress should extend Finish defer"
        );

        assert!(matches!(
            pump_established_uplink(&mut ctx, TcpState::CloseWait, 18, || None),
            EstablishedUplink::Handled
        ));
        assert!(ctx.local_fin_sent);
        assert!(matches!(
            rx.try_recv()
                .expect("finish should be sent after remote quiet"),
            RelayCommand::Finish
        ));
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
        let stale = HandshakeDone {
            handle,
            epoch: 3,
            result: Ok(Box::new(tokio::io::duplex(64).0)),
        };
        handle_handshake_done(
            stale,
            &mut sockets,
            &mut socket_ctxs,
            &global_tx,
            &mut pool,
            0,
            &Metrics::new(),
        );
        let ctx = socket_ctxs.get(&handle).unwrap();
        assert_eq!(
            ctx.state,
            SocketState::HandshakePending,
            "迟到 epoch → 不装、状态不变"
        );
        assert!(ctx.uplink_tx.is_none(), "迟到 epoch → 不安装 uplink_tx");

        // 匹配 epoch 5 + Ok → 安装 relay。
        let ok = HandshakeDone {
            handle,
            epoch: 5,
            result: Ok(Box::new(tokio::io::duplex(64).0)),
        };
        handle_handshake_done(
            ok,
            &mut sockets,
            &mut socket_ctxs,
            &global_tx,
            &mut pool,
            0,
            &Metrics::new(),
        );
        let ctx = socket_ctxs.get(&handle).unwrap();
        assert_eq!(
            ctx.state,
            SocketState::Relaying,
            "匹配 epoch → 安装 relay 进 Relaying"
        );
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

        let err = HandshakeDone {
            handle,
            epoch: 2,
            result: Err(ClientError::Reality("open 失败".into())),
        };
        handle_handshake_done(
            err,
            &mut sockets,
            &mut socket_ctxs,
            &global_tx,
            &mut pool,
            1,
            &Metrics::new(),
        );
        let ctx = socket_ctxs.get(&handle).unwrap();
        assert_eq!(
            ctx.state,
            SocketState::Listening,
            "失败 → rearm 回 Listening"
        );
        assert!(
            ctx.fake_ip.is_none(),
            "失败 rearm 释放 fake-IP（平衡 spawn 的 acquire）"
        );
        assert!(ctx.uplink_buffer.is_empty(), "rearm 清 buffer");
        assert_eq!(
            pool.sweep(1000, 300),
            1,
            "refcount 归零 → 可回收（acquire/release 平衡）"
        );
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
        let ok = HandshakeDone {
            handle,
            epoch: 1,
            result: Ok(Box::new(near)),
        };
        handle_handshake_done(
            ok,
            &mut sockets,
            &mut socket_ctxs,
            &global_tx,
            &mut pool,
            0,
            &Metrics::new(),
        );

        let mut got = vec![0u8; b"HELLO-BUFFERED".len()];
        far.read_exact(&mut got).await.unwrap();
        assert_eq!(
            &got, b"HELLO-BUFFERED",
            "open 成功后按序 flush 缓存的上行字节"
        );
    }

    #[test]
    fn tun_runtime_config_defaults_match_stage9_behavior() {
        let config = TunRuntimeConfig::from_sources(None).expect("config should load");
        // Stage 9 drops local_port; pool_size default lowered to 2 (per-port now).
        assert_eq!(config.listener.pool_size, 2);
        assert_eq!(config.tun_mtu, DEFAULT_TUN_MTU);
        // 刀11：from_sources（harness/测试路径）恒用默认快照周期。
        assert_eq!(config.metrics_secs, METRICS_SNAPSHOT_SECS);
        assert_eq!(
            config.tcp_socket_buffer_bytes(),
            (TCP_SOCKET_BUFFER_SIZE, TCP_SOCKET_BUFFER_SIZE)
        );
        assert_eq!(
            config.downlink_flush_max_bytes,
            DEFAULT_DOWNLINK_FLUSH_MAX_BYTES
        );
        assert_eq!(
            config.downlink_egress_immediate_bytes,
            DEFAULT_DOWNLINK_EGRESS_IMMEDIATE_BYTES
        );
        assert_eq!(config.tun_rx_drain_budget, DEFAULT_TUN_RX_DRAIN_BUDGET);
        assert_eq!(
            config.tun_rx_active_flow_timer_ms, DEFAULT_TUN_RX_ACTIVE_FLOW_TIMER_DRAIN_MS,
            "Knife14ex promotes a bounded active-flow ACK/window service lane to the default"
        );
        assert!(
            config.tun_rx_active_flow_timer_ms > 0,
            "the default service lane must not silently disable active-flow ACK/window drain"
        );
        assert!(
            !config.bounded_global_rx_receive_window,
            "Knife14cg bounded receive window is opt-in after VPS rejected it as a default"
        );
    }

    /// 刀11：MINI_VPN_METRICS_SECS 解析——有效正整数采用；0/非数字/缺失回落默认（防 interval panic）。
    #[test]
    fn parse_metrics_secs_clamps_and_defaults() {
        assert_eq!(parse_metrics_secs(Some("5")), 5);
        assert_eq!(parse_metrics_secs(Some("  10 ")), 10);
        assert_eq!(
            parse_metrics_secs(Some("0")),
            METRICS_SNAPSHOT_SECS,
            "0 必回落（否则 interval panic）"
        );
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
        assert!(
            !parse_profile_loop(None),
            "缺省关——默认 NoopSink 零开销路径"
        );
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
    fn parse_tun_tx_queue_len_accepts_positive_estimates_and_defaults() {
        assert_eq!(parse_tun_tx_queue_len(Some("5000")), 5000);
        assert_eq!(parse_tun_tx_queue_len(Some(" 500 ")), 500);
        assert_eq!(
            parse_tun_tx_queue_len(Some("0")),
            DEFAULT_TUN_TX_QUEUE_LEN_ESTIMATE
        );
        assert_eq!(
            parse_tun_tx_queue_len(Some("abc")),
            DEFAULT_TUN_TX_QUEUE_LEN_ESTIMATE
        );
        assert_eq!(
            parse_tun_tx_queue_len(None),
            DEFAULT_TUN_TX_QUEUE_LEN_ESTIMATE
        );
    }

    #[test]
    fn parse_tun_rx_drain_budget_allows_zero_and_bounds() {
        assert_eq!(parse_tun_rx_drain_budget(None), DEFAULT_TUN_RX_DRAIN_BUDGET);
        assert_eq!(
            parse_tun_rx_drain_budget(Some("abc")),
            DEFAULT_TUN_RX_DRAIN_BUDGET
        );
        assert_eq!(parse_tun_rx_drain_budget(Some("0")), 0);
        assert_eq!(parse_tun_rx_drain_budget(Some(" 8 ")), 8);
        assert_eq!(
            parse_tun_rx_drain_budget(Some("64")),
            MAX_TUN_RX_DRAIN_BUDGET
        );
        assert_eq!(
            parse_tun_rx_drain_budget(Some("65")),
            DEFAULT_TUN_RX_DRAIN_BUDGET
        );
    }

    #[test]
    fn parse_tun_rx_active_flow_timer_ms_defaults_to_service_lane_and_is_bounded() {
        assert_eq!(
            parse_tun_rx_active_flow_timer_ms(None),
            DEFAULT_TUN_RX_ACTIVE_FLOW_TIMER_DRAIN_MS
        );
        assert_eq!(
            parse_tun_rx_active_flow_timer_ms(Some("abc")),
            DEFAULT_TUN_RX_ACTIVE_FLOW_TIMER_DRAIN_MS
        );
        assert_eq!(parse_tun_rx_active_flow_timer_ms(Some("0")), 0);
        assert_eq!(parse_tun_rx_active_flow_timer_ms(Some(" 250 ")), 250);
        assert_eq!(
            parse_tun_rx_active_flow_timer_ms(Some("1000")),
            MAX_TUN_RX_ACTIVE_FLOW_TIMER_DRAIN_MS
        );
        assert_eq!(
            parse_tun_rx_active_flow_timer_ms(Some("1001")),
            DEFAULT_TUN_RX_ACTIVE_FLOW_TIMER_DRAIN_MS
        );
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
        diag.note_terminal_late_remote_payload(25);
        diag.note_send_slice_ok(60, 40);
        diag.note_send_slice_ok(0, 40);
        diag.note_send_slice_error();
        diag.note_tun_flush(true);
        diag.note_tun_flush(false);

        assert_eq!(diag.remote_to_global_rx_bytes, 100);
        assert_eq!(diag.terminal_late_remote_payload_bytes, 25);
        assert_eq!(diag.terminal_late_remote_payload_events, 1);
        assert_eq!(diag.downlink_pending_high_water, 100);
        assert_eq!(diag.send_slice_calls, 3);
        assert_eq!(diag.send_slice_accepted_bytes, 60);
        assert_eq!(diag.send_slice_zero, 1);
        assert_eq!(diag.send_slice_errors, 1);
        assert_eq!(diag.tun_flush_tx_calls, 2);
        assert_eq!(diag.tun_flush_tx_failures, 1);
    }

    #[test]
    fn tcp_downlink_flush_progress_diag_tracks_budget_and_capacity() {
        let mut diag = TcpDownlinkDiag::default();
        diag.note_flush_attempt(600_000, 262_144);
        diag.note_send_slice_ok(262_144, 337_856);
        diag.note_flush_attempt(337_856, 262_144);
        diag.note_no_send_capacity(337_856);
        diag.note_flush_attempt(337_856, 262_144);
        diag.note_send_slice_ok(0, 337_856);
        diag.note_flush_attempt(337_856, 262_144);
        diag.note_send_slice_error();

        assert_eq!(diag.flush_attempts, 4);
        assert_eq!(diag.send_slice_no_capacity, 1);
        assert_eq!(diag.send_slice_budget_limited, 4);
        assert_eq!(diag.send_slice_calls, 3);
        assert_eq!(diag.send_slice_zero, 1);
        assert_eq!(diag.send_slice_errors, 1);
        assert_eq!(diag.send_slice_accepted_bytes, 262_144);
        assert_eq!(diag.send_slice_max_accepted_bytes, 262_144);
        assert_eq!(diag.downlink_pending_high_water, 600_000);
    }

    #[test]
    fn tcp_downlink_diag_tracks_headroom_limited_flushes() {
        let mut diag = TcpDownlinkDiag::default();
        diag.note_flush_limit(
            64,
            64,
            DownlinkFlushLimit {
                len: 2,
                headroom_limited: true,
                headroom_deferred_bytes: 62,
                clean_headroom_bytes: 2,
                drain_credit_granted_bytes: 0,
                drain_credit_planned_bytes: 0,
                drop_credit_debt_bytes: 13,
                drop_credit_debt_paid_bytes: 5,
                drop_credit_blocked_bytes: 8,
                pressure_credit_debt_bytes: 21,
                pressure_credit_debt_paid_bytes: 3,
                pressure_credit_blocked_bytes: 11,
                hard_edge_guard_bytes: 0,
                hard_edge_guard_deferred_bytes: 0,
            },
        );

        assert_eq!(diag.flush_attempts, 1);
        assert_eq!(diag.send_slice_budget_limited, 0);
        assert_eq!(diag.send_slice_headroom_limited, 1);
        assert_eq!(diag.send_slice_headroom_deferred_bytes, 62);
        assert_eq!(diag.send_slice_drop_credit_debt_bytes, 13);
        assert_eq!(diag.send_slice_drop_credit_debt_paid_bytes, 5);
        assert_eq!(diag.send_slice_drop_credit_blocked_bytes, 8);
        assert_eq!(diag.send_slice_pressure_credit_debt_bytes, 21);
        assert_eq!(diag.send_slice_pressure_credit_debt_paid_bytes, 3);
        assert_eq!(diag.send_slice_pressure_credit_blocked_bytes, 11);
        assert_eq!(diag.send_slice_hard_edge_guard_limited, 0);
        assert_eq!(diag.send_slice_hard_edge_guard_deferred_bytes, 0);
        assert_eq!(diag.downlink_pending_high_water, 64);
    }

    #[test]
    fn tcp_downlink_flush_aggregate_formats_progress_signal() {
        let mut a = SocketCtx::new(443);
        a.downlink_pending = vec![0; 10];
        a.downlink_diag.note_remote_payload(100, 100);
        a.downlink_diag.note_flush_attempt(100, 64);
        a.downlink_diag.note_send_window(
            TcpSendWindowSnapshot {
                can_send: true,
                may_send: true,
                may_recv: true,
                send_capacity: 1_048_576,
                send_queue: 1_048_576,
                recv_queue: 4096,
            },
            100,
        );
        a.downlink_diag.note_send_slice_ok(64, 36);
        a.downlink_diag.note_tun_flush(true);
        a.downlink_diag.note_tun_flush_deferred();
        a.downlink_credit_controller.egress_payload_credit_bytes = 1234;

        let mut b = SocketCtx::new(443);
        b.downlink_pending = vec![0; 25];
        b.downlink_diag.note_remote_payload(200, 200);
        b.downlink_diag.note_terminal_late_remote_payload(10);
        b.downlink_diag.note_terminal_late_remote_payload(15);
        b.downlink_diag.note_flush_attempt(200, 64);
        b.downlink_diag.note_send_window(
            TcpSendWindowSnapshot {
                can_send: false,
                may_send: false,
                may_recv: false,
                send_capacity: 0,
                send_queue: 0,
                recv_queue: 0,
            },
            200,
        );
        b.downlink_diag.note_no_send_capacity(200);
        b.downlink_diag.note_tun_flush(false);

        let aggregate = tcp_downlink_aggregate([&a, &b]);
        let line = format_tcp_downlink_flush_diag(&aggregate, 2);

        assert!(line.contains("tcp-downlink-flush"));
        assert!(line.contains("pending_total=35"));
        assert!(line.contains("pending_max=25"));
        assert!(line.contains("pending_high=200"));
        assert!(line.contains("remote_to_global_rx_bytes=300"));
        assert!(line.contains("terminal_late_remote_payload_bytes=25"));
        assert!(line.contains("terminal_late_remote_payload_events=2"));
        assert!(line.contains("flush_attempts=2"));
        assert!(line.contains("no_send_capacity=1"));
        assert!(line.contains("send_slice_calls=1"));
        assert!(line.contains("send_slice_accepted=64"));
        assert!(line.contains("budget_limited_calls=2"));
        assert!(line.contains("headroom_limited_calls=0"));
        assert!(line.contains("headroom_deferred_bytes=0"));
        assert!(line.contains("egress_payload_credit_bytes=1234"));
        assert!(line.contains("drop_credit_debt_bytes=0"));
        assert!(line.contains("drop_credit_debt_paid_bytes=0"));
        assert!(line.contains("drop_credit_blocked_bytes=0"));
        assert!(line.contains("pressure_credit_debt_bytes=0"));
        assert!(line.contains("pressure_credit_debt_paid_bytes=0"));
        assert!(line.contains("pressure_credit_blocked_bytes=0"));
        assert!(line.contains("hard_edge_guard_bytes=0"));
        assert!(line.contains("hard_edge_guard_limited_calls=0"));
        assert!(line.contains("hard_edge_guard_deferred_bytes=0"));
        assert!(line.contains("send_slice_max_accepted=64"));
        assert!(line.contains("send_window_samples=2"));
        assert!(line.contains("send_capacity_min=0"));
        assert!(line.contains("send_capacity_max=1048576"));
        assert!(line.contains("send_queue_max=1048576"));
        assert!(line.contains("recv_queue_max=4096"));
        assert!(line.contains("may_send_false=1"));
        assert!(line.contains("may_recv_false=1"));
        assert!(line.contains("no_send_capacity_streak_max=1"));
        assert!(line.contains("no_send_capacity_pending_max=200"));
        assert!(line.contains("tun_flush_tx_calls=2"));
        assert!(line.contains("tun_flush_tx_failures=1"));
        assert!(line.contains("tun_flush_deferred=1"));
        assert!(line.contains("dirty_handles=2"));
    }

    #[test]
    fn tun_egress_drop_sampler_tracks_first_delta_and_reset() {
        let mut sampler = TunEgressDropSampler::new(Some("tun0"));

        assert_eq!(
            sampler.sample_with(|iface| {
                assert_eq!(iface, "tun0");
                Ok("100\n".to_string())
            }),
            TunEgressDropSample::First { total: 100 }
        );
        assert_eq!(
            sampler.sample_with(|iface| {
                assert_eq!(iface, "tun0");
                Ok("1523\n".to_string())
            }),
            TunEgressDropSample::Delta {
                total: 1523,
                delta: 1423,
            }
        );
        assert_eq!(
            sampler.sample_with(|iface| {
                assert_eq!(iface, "tun0");
                Ok("9\n".to_string())
            }),
            TunEgressDropSample::Reset {
                previous: 1523,
                total: 9,
            }
        );
    }

    #[test]
    fn tun_egress_drop_sampler_reports_unavailable_without_behavior_pressure() {
        let mut no_name = TunEgressDropSampler::new(None);
        assert_eq!(
            no_name.sample_with(|_| unreachable!("no interface should not read sysfs")),
            TunEgressDropSample::Unavailable {
                reason: "no_interface"
            }
        );

        let mut invalid = TunEgressDropSampler::new(Some("../tun0"));
        assert_eq!(
            invalid.sample_with(|_| unreachable!("invalid interface should not read sysfs")),
            TunEgressDropSample::Unavailable {
                reason: "invalid_interface"
            }
        );

        let mut parent_dir = TunEgressDropSampler::new(Some(".."));
        assert_eq!(
            parent_dir.sample_with(|_| unreachable!("parent dir should not read sysfs")),
            TunEgressDropSample::Unavailable {
                reason: "invalid_interface"
            }
        );

        let mut read_err = TunEgressDropSampler::new(Some("tun0"));
        assert_eq!(
            read_err.sample_with(|_| {
                Err(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "missing stat",
                ))
            }),
            TunEgressDropSample::Unavailable {
                reason: "read_error"
            }
        );

        let mut parse_err = TunEgressDropSampler::new(Some("tun0"));
        assert_eq!(
            parse_err.sample_with(|_| Ok("not-a-counter\n".to_string())),
            TunEgressDropSample::Unavailable {
                reason: "parse_error"
            }
        );
    }

    #[test]
    fn tun_egress_diag_formats_drop_delta_with_downlink_context() {
        let aggregate = TcpDownlinkAggregate {
            pending_total: 10,
            pending_max: 7,
            pending_high: 100,
            remote_to_global_rx_bytes: 4096,
            tun_flush_tx_calls: 12,
            ..TcpDownlinkAggregate::default()
        };
        let line = format_tun_egress_diag(
            "tun0",
            &TunEgressDropSample::Delta {
                total: 1523,
                delta: 1423,
            },
            &aggregate,
            2,
            true,
        );

        assert!(line.contains("tcp-tun-egress"));
        assert!(line.contains("if=tun0"));
        assert!(line.contains("status=delta"));
        assert!(line.contains("tx_dropped_total=1523"));
        assert!(line.contains("tx_dropped_delta=1423"));
        assert!(line.contains("global_rx_paused=true"));
        assert!(line.contains("pending_total=10"));
        assert!(line.contains("pending_high=100"));
        assert!(line.contains("remote_to_global_rx_bytes=4096"));
        assert!(line.contains("tun_flush_tx_calls=12"));
        assert!(line.contains("dirty_handles=2"));
    }

    #[test]
    fn downlink_egress_pacer_allows_budget_then_defers_until_timer_reset() {
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 1_000_000,
            low_bytes: 250_000,
        };
        let mut pacer = DownlinkEgressPacer::new(65_536);

        assert!(
            pacer.allow_remote_payload_flush(32_768, 0, 0, cfg),
            "first accepted slice should flush immediately while budget remains"
        );
        assert!(
            pacer.allow_remote_payload_flush(32_768, 0, 0, cfg),
            "budget may be consumed exactly"
        );
        assert!(
            !pacer.allow_remote_payload_flush(1, 0, 0, cfg),
            "extra accepted bytes in the same interval should defer to timer flush"
        );

        pacer.on_timer_tick();
        assert!(
            pacer.allow_remote_payload_flush(1, 0, 0, cfg),
            "timer reset should reopen the immediate flush budget"
        );
    }

    #[test]
    fn downlink_egress_pacer_zero_budget_defers_only_when_no_backlog_remains() {
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 1_000_000,
            low_bytes: 250_000,
        };
        let mut pacer = DownlinkEgressPacer::new(0);

        assert!(
            !pacer.allow_remote_payload_flush(1, 0, 0, cfg),
            "zero budget should defer when the current payload left no backlog"
        );
        assert!(
            !pacer.allow_remote_payload_flush(0, 0, 0, cfg),
            "zero accepted bytes should never force an immediate TUN flush"
        );
        assert!(
            pacer.allow_remote_payload_flush(0, 1, 0, cfg),
            "pending backlog should force an immediate flush attempt even with zero budget"
        );
        pacer.on_timer_tick();
        assert!(
            !pacer.allow_remote_payload_flush(65_536, 0, 0, cfg),
            "timer reset should preserve an explicitly disabled no-backlog budget"
        );
    }

    #[test]
    fn downlink_egress_pacer_forces_flush_while_pending_remains() {
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 1_000_000,
            low_bytes: 250_000,
        };
        let mut pacer = DownlinkEgressPacer::new(65_536);

        assert!(
            pacer.allow_remote_payload_flush(65_537, 1, 0, cfg),
            "pending backlog must force immediate egress even when accepted bytes exceed budget"
        );
    }

    #[test]
    fn downlink_egress_pacer_uses_tx_queue_headroom_when_no_pending_remains() {
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 100,
            low_bytes: 40,
        };
        let mut pacer = DownlinkEgressPacer::new(100);

        assert!(
            pacer.allow_remote_payload_flush(32, 0, 100, cfg),
            "tx-queue-only pressure at soft high should still allow immediate flush"
        );
        assert_eq!(
            pacer.remaining_immediate_bytes, 68,
            "allowed no-pending flush should consume the immediate budget"
        );

        pacer.on_timer_tick();
        assert!(
            pacer.allow_remote_payload_flush(32, 0, tx_queue_flush_threshold(cfg) - 1, cfg),
            "tx-queue-only pressure below the bounded flush cap should still allow immediate flush"
        );

        pacer.on_timer_tick();
        assert!(
            !pacer.allow_remote_payload_flush(32, 0, tx_queue_flush_threshold(cfg), cfg),
            "tx-queue-only pressure at the bounded flush cap should defer immediate flush"
        );
        assert_eq!(
            pacer.remaining_immediate_bytes, 68,
            "bytes accepted into smoltcp should still consume budget when deferred"
        );
    }

    #[test]
    fn downlink_egress_pacer_bounds_no_pending_flush_before_hard_cap() {
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 100,
            low_bytes: 40,
        };
        let mut pacer = DownlinkEgressPacer::new(100);

        assert!(
            pacer.allow_remote_payload_flush(16, 0, tx_queue_flush_threshold(cfg) - 1, cfg),
            "no-pending flush should keep some headroom above soft high"
        );

        pacer.on_timer_tick();
        assert!(
            !pacer.allow_remote_payload_flush(16, 0, tx_queue_flush_threshold(cfg), cfg),
            "no-pending flush should stop at the bounded midpoint before hard cap"
        );
        assert_eq!(
            pacer.remaining_immediate_bytes, 84,
            "bytes accepted into smoltcp should still consume budget when deferred"
        );
    }

    #[test]
    fn downlink_egress_pacer_defers_pending_flush_at_send_queue_high_watermark() {
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 100,
            low_bytes: 40,
        };
        let mut pacer = DownlinkEgressPacer::new(100);

        assert!(
            !pacer.allow_remote_payload_flush(32, 1, 100, cfg),
            "local send queue at high watermark should defer remote-payload TUN flush"
        );
        assert_eq!(
            pacer.remaining_immediate_bytes, 68,
            "bytes accepted into smoltcp should still consume the immediate budget"
        );

        pacer.on_timer_tick();
        assert!(
            pacer.allow_remote_payload_flush(0, 1, 99, cfg),
            "pending backlog can still force progress while send queue is below high"
        );
    }

    #[test]
    fn downlink_pending_stats_track_max_and_total() {
        let mut a = SocketCtx::new(443);
        a.downlink_pending = vec![0; 10];
        let mut b = SocketCtx::new(443);
        b.downlink_pending = vec![0; 25];

        let stats = downlink_pending_stats([&a, &b]);
        assert_eq!(stats.max_pending, 25);
        assert_eq!(stats.total_pending, 35);
        assert_eq!(stats.max_tx_queue, 0);
        assert_eq!(stats.total_tx_queue, 0);
        assert_eq!(stats.max_pressure(), 25);
        assert_eq!(stats.total_pressure(), 35);
    }

    #[test]
    fn buffered_downlink_config_derives_bounded_watermarks_from_backpressure() {
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 100,
            low_bytes: 40,
        };
        let buffered = BufferedDownlinkConfig::from_backpressure(true, cfg, 3);

        assert!(buffered.enabled);
        assert_eq!(buffered.per_flow_low_bytes, 100);
        assert_eq!(buffered.per_flow_high_bytes, 400);
        assert_eq!(buffered.per_flow_hard_bytes, 800);
        assert_eq!(buffered.global_low_bytes, 300);
        assert_eq!(buffered.global_high_bytes, 1200);
        assert_eq!(buffered.global_hard_bytes, 2400);
    }

    #[test]
    fn buffered_downlink_read_credit_ignores_transient_local_headroom_pressure() {
        let cfg = BufferedDownlinkConfig::from_backpressure(
            true,
            DownlinkBackpressureConfig {
                high_bytes: 100,
                low_bytes: 40,
            },
            1,
        );

        let decision = buffered_downlink_read_credit(false, 0, 0, false, false, cfg);

        assert_eq!(decision.reason, BufferedDownlinkCreditReason::Open);
        assert_eq!(
            decision.credit,
            RelayReadCredit {
                paused: false,
                max_batch_bytes: RELAY_REMOTE_READ_BURST_MAX_BYTES,
            }
        );
    }

    #[test]
    fn buffered_downlink_soft_high_keeps_useful_read_floor() {
        let cfg = BufferedDownlinkConfig::from_backpressure(
            true,
            DownlinkBackpressureConfig {
                high_bytes: 100,
                low_bytes: 40,
            },
            1,
        );

        let decision =
            buffered_downlink_read_credit(false, cfg.per_flow_high_bytes, 0, false, false, cfg);

        assert_eq!(decision.reason, BufferedDownlinkCreditReason::SoftHigh);
        assert_eq!(
            decision.credit,
            RelayReadCredit {
                paused: false,
                max_batch_bytes: RELAY_REMOTE_READ_MIN_BATCH_BYTES,
            },
            "soft buffered pressure must not collapse remote reads to MTU-sized 1200B service"
        );
    }

    #[test]
    fn buffered_downlink_pauses_only_at_hard_watermarks_and_resumes_at_low() {
        let cfg = BufferedDownlinkConfig::from_backpressure(
            true,
            DownlinkBackpressureConfig {
                high_bytes: 100,
                low_bytes: 40,
            },
            2,
        );

        assert!(
            !buffered_downlink_read_credit(
                false,
                cfg.per_flow_hard_bytes - 1,
                0,
                false,
                false,
                cfg
            )
            .credit
            .paused
        );
        let hard =
            buffered_downlink_read_credit(false, cfg.per_flow_hard_bytes, 0, false, false, cfg);
        assert_eq!(hard.reason, BufferedDownlinkCreditReason::PerFlowHard);
        assert!(hard.credit.paused);
        assert!(
            buffered_downlink_read_credit(true, cfg.per_flow_low_bytes + 1, 0, false, false, cfg)
                .credit
                .paused,
            "paused flow stays paused above low watermark"
        );
        assert!(
            !buffered_downlink_read_credit(true, cfg.per_flow_low_bytes, 0, false, false, cfg)
                .credit
                .paused,
            "flow resumes once buffered bytes drain to low watermark"
        );
    }

    #[test]
    fn buffered_downlink_global_hard_protects_memory_without_tx_queue_pressure() {
        let cfg = BufferedDownlinkConfig::from_backpressure(
            true,
            DownlinkBackpressureConfig {
                high_bytes: 100,
                low_bytes: 40,
            },
            2,
        );

        let hard = buffered_downlink_read_credit(
            false,
            cfg.per_flow_low_bytes,
            cfg.global_hard_bytes,
            false,
            false,
            cfg,
        );
        assert_eq!(hard.reason, BufferedDownlinkCreditReason::GlobalHard);
        assert!(hard.credit.paused);
    }

    #[test]
    fn buffered_global_rx_window_uses_buffer_watermarks_not_tx_queue_pressure() {
        let cfg = BufferedDownlinkConfig::from_backpressure(
            true,
            DownlinkBackpressureConfig {
                high_bytes: 100,
                low_bytes: 40,
            },
            2,
        );
        let tx_queue_only = DownlinkPressureStats::new(0, 0, usize::MAX / 4, usize::MAX / 4);

        assert!(
            !next_buffered_global_rx_receive_backpressure(false, tx_queue_only, cfg),
            "buffered global receive is governed by pending bytes, not smoltcp tx_queue-only pressure"
        );
        assert!(
            next_buffered_global_rx_receive_backpressure(
                false,
                DownlinkPressureStats::pending_only(cfg.per_flow_hard_bytes, cfg.global_low_bytes),
                cfg,
            ),
            "per-flow hard buffered bytes pause global_rx receive"
        );
        assert!(
            next_buffered_global_rx_receive_backpressure(
                false,
                DownlinkPressureStats::pending_only(cfg.per_flow_low_bytes, cfg.global_hard_bytes),
                cfg,
            ),
            "global hard buffered bytes pause global_rx receive"
        );
    }

    #[test]
    fn buffered_downlink_credit_diag_reports_attribution_fields() {
        let cfg = BufferedDownlinkConfig::from_backpressure(
            true,
            DownlinkBackpressureConfig {
                high_bytes: 100,
                low_bytes: 40,
            },
            2,
        );
        let mut sockets = SocketSet::new(vec![]);
        let handle = mk_test_handle(&mut sockets);
        let decision = buffered_downlink_read_credit(false, 401, 500, false, false, cfg);

        let line = format_buffered_downlink_read_credit_diag(handle, 7, decision, 401, 500, cfg);

        assert!(line.contains("tcp-buffered-downlink-credit"), "{line}");
        assert!(line.contains("epoch=7"), "{line}");
        assert!(line.contains("paused=false"), "{line}");
        assert!(line.contains("max_batch_bytes=65536"), "{line}");
        assert!(line.contains("reason=soft_high"), "{line}");
        assert!(line.contains("per_flow_pending=401"), "{line}");
        assert!(line.contains("global_pending=500"), "{line}");
        assert!(line.contains("per_flow_hard=800"), "{line}");
        assert!(line.contains("global_hard=1600"), "{line}");
    }

    #[test]
    fn downlink_backpressure_uses_high_low_hysteresis() {
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 100,
            low_bytes: 40,
        };

        assert!(
            !next_downlink_backpressure(false, DownlinkPressureStats::pending_only(99, 99), cfg),
            "below high watermark keeps global_rx enabled"
        );
        assert!(
            next_downlink_backpressure(false, DownlinkPressureStats::pending_only(100, 100), cfg),
            "hitting high watermark pauses global_rx"
        );
        assert!(
            next_downlink_backpressure(true, DownlinkPressureStats::pending_only(41, 41), cfg),
            "while paused, stay paused until below low watermark"
        );
        assert!(
            !next_downlink_backpressure(true, DownlinkPressureStats::pending_only(40, 40), cfg),
            "low watermark resumes global_rx"
        );
    }

    #[test]
    fn downlink_backpressure_uses_tx_queue_pressure_when_pending_is_empty() {
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 100,
            low_bytes: 40,
        };

        assert!(
            !next_downlink_backpressure(
                false,
                DownlinkPressureStats::new(
                    0,
                    0,
                    tx_queue_pause_threshold(cfg) - 1,
                    tx_queue_pause_threshold(cfg) - 1
                ),
                cfg
            ),
            "below the tx-queue-only hard cap keeps global_rx enabled when app pending is empty"
        );
        assert!(
            next_downlink_backpressure(
                false,
                DownlinkPressureStats::new(
                    0,
                    0,
                    tx_queue_pause_threshold(cfg),
                    tx_queue_pause_threshold(cfg)
                ),
                cfg
            ),
            "hitting the tx-queue-only hard cap pauses global_rx"
        );
        assert!(
            next_downlink_backpressure(true, DownlinkPressureStats::new(0, 0, 101, 101), cfg),
            "while paused, tx queue pressure stays paused above the soft high watermark"
        );
        assert!(
            !next_downlink_backpressure(true, DownlinkPressureStats::new(0, 0, 100, 100), cfg),
            "soft high watermark resumes global_rx when app pending is empty"
        );
    }

    #[test]
    fn downlink_backpressure_uses_tx_queue_headroom_before_pausing() {
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 100,
            low_bytes: 40,
        };

        assert!(
            !next_downlink_backpressure(false, DownlinkPressureStats::new(0, 0, 100, 100), cfg),
            "tx-queue-only pressure at the soft high watermark should not pause immediately"
        );
        assert!(
            !next_downlink_backpressure(
                false,
                DownlinkPressureStats::new(
                    0,
                    0,
                    tx_queue_pause_threshold(cfg) - 1,
                    tx_queue_pause_threshold(cfg) - 1
                ),
                cfg
            ),
            "bounded tx-queue-only headroom should keep global_rx enabled"
        );
        assert!(
            next_downlink_backpressure(
                false,
                DownlinkPressureStats::new(
                    0,
                    0,
                    tx_queue_pause_threshold(cfg),
                    tx_queue_pause_threshold(cfg)
                ),
                cfg
            ),
            "tx-queue-only pressure still pauses at the derived hard cap"
        );
        assert!(
            next_downlink_backpressure(true, DownlinkPressureStats::new(0, 0, 101, 101), cfg),
            "while paused by tx queue pressure, stay paused above the soft high watermark"
        );
        assert!(
            !next_downlink_backpressure(true, DownlinkPressureStats::new(0, 0, 100, 100), cfg),
            "tx-queue-only pressure resumes once it drains to the soft high watermark"
        );
    }

    #[test]
    fn downlink_backpressure_active_credit_debt_pauses_at_credit_edge() {
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 100,
            low_bytes: 40,
        };
        let credit_edge = DownlinkPressureStats::new(
            0,
            0,
            tx_queue_credit_spend_threshold(cfg),
            tx_queue_credit_spend_threshold(cfg),
        );
        let recovered = DownlinkPressureStats::new(0, 0, 100, 100);

        assert!(
            !next_downlink_backpressure(false, credit_edge, cfg),
            "without active credit debt, the tx-queue credit edge remains below the hard pause edge"
        );
        assert!(
            next_downlink_backpressure_with_credit_debt(false, credit_edge, cfg, true),
            "active credit debt should pause remote receive at the credit edge before more pending accumulates"
        );
        assert!(
            !next_downlink_backpressure_with_credit_debt(true, recovered, cfg, true),
            "once pressure reaches the ordinary resume threshold, active debt should not deadlock receive"
        );
    }

    #[test]
    fn downlink_backpressure_holds_recent_high_tx_queue_after_flush() {
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 100,
            low_bytes: 40,
        };
        let mut feedback = TunEgressFeedbackState::default();
        let now = std::time::Instant::now();

        feedback.observe_pressure_at(DownlinkPressureStats::new(0, 0, 160, 160), cfg, now);

        let held = feedback.effective_downlink_pressure_at(
            DownlinkPressureStats::default(),
            now + std::time::Duration::from_millis(TUN_EGRESS_PRESSURE_HOLD_MS - 1),
        );
        assert_eq!(held.max_pressure(), 160);
        assert!(
            next_downlink_backpressure(true, held, cfg),
            "recent hard egress pressure keeps global_rx paused briefly after raw pressure drains"
        );

        let expired = feedback.effective_downlink_pressure_at(
            DownlinkPressureStats::default(),
            now + std::time::Duration::from_millis(TUN_EGRESS_PRESSURE_HOLD_MS + 1),
        );
        assert_eq!(expired.max_pressure(), 0);
        assert!(
            !next_downlink_backpressure(true, expired, cfg),
            "after the bounded hold expires, ordinary low watermark resume applies"
        );
    }

    #[test]
    fn downlink_backpressure_does_not_hold_soft_tx_queue_pressure() {
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 100,
            low_bytes: 40,
        };
        let mut feedback = TunEgressFeedbackState::default();
        let now = std::time::Instant::now();

        feedback.observe_pressure_at(DownlinkPressureStats::new(0, 0, 100, 100), cfg, now);

        let effective = feedback.effective_downlink_pressure_at(
            DownlinkPressureStats::default(),
            now + std::time::Duration::from_millis(TUN_EGRESS_PRESSURE_HOLD_MS - 1),
        );
        assert_eq!(
            effective,
            DownlinkPressureStats::default(),
            "soft tx-queue-only pressure should not install an egress hold"
        );
    }

    #[test]
    fn downlink_backpressure_diag_reports_raw_and_effective_pressure() {
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 100,
            low_bytes: 40,
        };
        let raw = DownlinkPressureStats::default();
        let effective = DownlinkPressureStats::new(0, 0, 100, 100);

        let line = format_tcp_downlink_backpressure_diag(true, raw, effective, cfg);

        assert!(line.contains("tcp-downlink-backpressure"), "{line}");
        assert!(line.contains("paused=true"), "{line}");
        assert!(line.contains("max_tx_queue=100"), "{line}");
        assert!(line.contains("tx_queue_flush_high=130"), "{line}");
        assert!(line.contains("tx_queue_pause_high=160"), "{line}");
        assert!(line.contains("tx_queue_resume_high=100"), "{line}");
        assert!(line.contains("raw_max_tx_queue=0"), "{line}");
        assert!(line.contains("effective_max_tx_queue=100"), "{line}");
        assert!(line.contains("held_pressure=true"), "{line}");
    }

    #[test]
    fn global_rx_receive_window_ignores_tx_queue_only_pressure() {
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 100,
            low_bytes: 40,
        };
        let tx_queue_only_pressure = DownlinkPressureStats::new(0, 0, 160, 160);

        assert!(
            next_downlink_backpressure(false, tx_queue_only_pressure, cfg),
            "local egress backpressure should still pause at the tx-queue hard cap"
        );
        assert!(
            !next_global_rx_receive_backpressure(false, tx_queue_only_pressure, cfg),
            "tx-queue-only pressure must not stop bounded relay receive progress"
        );
    }

    #[test]
    fn global_rx_receive_window_uses_pending_high_low_hysteresis() {
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 100,
            low_bytes: 40,
        };

        assert_eq!(downlink_receive_window_high(cfg), 400);
        assert_eq!(downlink_receive_window_low(cfg), 100);

        assert!(
            !next_global_rx_receive_backpressure(
                false,
                DownlinkPressureStats::pending_only(399, 399),
                cfg,
            ),
            "pending below the receive-window high watermark should keep relay receive enabled"
        );
        assert!(
            next_global_rx_receive_backpressure(
                false,
                DownlinkPressureStats::pending_only(400, 400),
                cfg,
            ),
            "pending at the bounded receive-window high watermark should pause relay receive"
        );
        assert!(
            next_global_rx_receive_backpressure(
                true,
                DownlinkPressureStats::pending_only(101, 101),
                cfg,
            ),
            "while paused, receive stays paused above the receive low watermark"
        );
        assert!(
            !next_global_rx_receive_backpressure(
                true,
                DownlinkPressureStats::pending_only(100, 100),
                cfg,
            ),
            "receive resumes after pending drains to the receive low watermark"
        );
    }

    #[test]
    fn global_rx_receive_diag_reports_local_egress_and_feedback_state() {
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 100,
            low_bytes: 40,
        };
        let stats = DownlinkPressureStats::new(150, 275, 160, 160);

        let line = format_tcp_global_rx_backpressure_diag(true, stats, cfg, true, true);

        assert!(line.contains("tcp-global-rx-backpressure"), "{line}");
        assert!(line.contains("paused=true"), "{line}");
        assert!(line.contains("max_pending=150"), "{line}");
        assert!(line.contains("total_pending=275"), "{line}");
        assert!(line.contains("max_tx_queue=160"), "{line}");
        assert!(line.contains("receive_high=400"), "{line}");
        assert!(line.contains("receive_low=100"), "{line}");
        assert!(line.contains("local_egress_paused=true"), "{line}");
        assert!(line.contains("tun_feedback_paused=true"), "{line}");
    }

    #[test]
    fn tun_egress_feedback_pauses_on_drop_delta_with_local_pressure() {
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 4_194_304,
            low_bytes: 1_048_576,
        };
        let mut feedback = TunEgressFeedbackState::default();
        let event = feedback.update(
            &TunEgressDropSample::Delta {
                total: 28_123,
                delta: 14_167,
            },
            DownlinkPressureStats::new(0, 0, 3_786_786, 3_786_786),
            cfg,
        );

        assert!(feedback.is_paused());
        assert_eq!(feedback.drop_events, 1);
        assert_eq!(feedback.drop_delta_total, 14_167);
        assert_eq!(feedback.max_drop_delta, 14_167);
        assert_eq!(feedback.pause_edges, 1);
        assert_eq!(feedback.resume_edges, 0);
        assert_eq!(
            event,
            Some(TunEgressFeedbackEvent {
                paused: true,
                reason: TunEgressFeedbackReason::DropDelta,
                tx_dropped_delta: 14_167,
                max_pressure: 3_786_786,
                total_pressure: 3_786_786,
            })
        );
    }

    #[test]
    fn tun_egress_feedback_pauses_on_drop_delta_with_recent_high_pressure() {
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 4_194_304,
            low_bytes: 1_048_576,
        };
        let mut feedback = TunEgressFeedbackState::default();
        feedback.observe_pressure(DownlinkPressureStats::new(0, 0, 3_786_786, 3_786_786), cfg);
        feedback.observe_pressure(DownlinkPressureStats::new(0, 0, 4_194_304, 4_194_304), cfg);

        let event = feedback.update(
            &TunEgressDropSample::Delta {
                total: 28_123,
                delta: 14_167,
            },
            DownlinkPressureStats::default(),
            cfg,
        );

        assert!(feedback.is_paused());
        assert_eq!(feedback.drop_events, 1);
        assert_eq!(feedback.pause_edges, 1);
        assert_eq!(
            event,
            Some(TunEgressFeedbackEvent {
                paused: true,
                reason: TunEgressFeedbackReason::DropDelta,
                tx_dropped_delta: 14_167,
                max_pressure: 4_194_304,
                total_pressure: 4_194_304,
            }),
            "drop sampled after pressure drains should still be attributed to recent high pressure"
        );
    }

    #[test]
    fn tun_egress_feedback_resumes_after_pressure_drains_to_low() {
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 4_194_304,
            low_bytes: 1_048_576,
        };
        let mut feedback = TunEgressFeedbackState::default();
        assert!(
            feedback
                .update(
                    &TunEgressDropSample::Delta {
                        total: 10,
                        delta: 10,
                    },
                    DownlinkPressureStats::new(0, 0, 2_000_000, 2_000_000),
                    cfg,
                )
                .is_some()
        );

        let repeated_drop = feedback.update(
            &TunEgressDropSample::Delta {
                total: 15,
                delta: 5,
            },
            DownlinkPressureStats::new(0, 0, 1_500_000, 1_500_000),
            cfg,
        );
        assert_eq!(
            repeated_drop,
            Some(TunEgressFeedbackEvent {
                paused: true,
                reason: TunEgressFeedbackReason::DropDelta,
                tx_dropped_delta: 5,
                max_pressure: 1_500_000,
                total_pressure: 1_500_000,
            }),
            "drops while already paused should remain visible"
        );
        assert_eq!(
            feedback.pause_edges, 1,
            "already-paused drops must not add pause edges"
        );

        assert_eq!(
            feedback.update(
                &TunEgressDropSample::Delta {
                    total: 15,
                    delta: 0,
                },
                DownlinkPressureStats::new(0, 0, 1_048_577, 1_048_577),
                cfg,
            ),
            None,
            "stay paused above low watermark"
        );

        let event = feedback.update(
            &TunEgressDropSample::Delta {
                total: 15,
                delta: 0,
            },
            DownlinkPressureStats::new(0, 0, 1_048_576, 1_048_576),
            cfg,
        );
        assert!(!feedback.is_paused());
        assert_eq!(feedback.pause_edges, 1);
        assert_eq!(feedback.resume_edges, 1);
        assert_eq!(
            event,
            Some(TunEgressFeedbackEvent {
                paused: false,
                reason: TunEgressFeedbackReason::PressureLow,
                tx_dropped_delta: 0,
                max_pressure: 1_048_576,
                total_pressure: 1_048_576,
            })
        );
    }

    #[test]
    fn tun_egress_feedback_resumes_on_raw_low_while_hold_is_active() {
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 100,
            low_bytes: 40,
        };
        let mut feedback = TunEgressFeedbackState::default();
        let now = std::time::Instant::now();

        assert!(
            feedback
                .update(
                    &TunEgressDropSample::Delta {
                        total: 10,
                        delta: 10,
                    },
                    DownlinkPressureStats::new(0, 0, 160, 160),
                    cfg,
                )
                .is_some()
        );
        assert!(feedback.is_paused());

        feedback.observe_pressure_at(DownlinkPressureStats::new(0, 0, 160, 160), cfg, now);
        let held = feedback.effective_downlink_pressure_at(
            DownlinkPressureStats::default(),
            now + std::time::Duration::from_millis(1),
        );
        assert_eq!(
            held.max_pressure(),
            160,
            "ordinary downlink backpressure still gets the short egress hold"
        );

        let event = feedback.update(
            &TunEgressDropSample::Delta {
                total: 10,
                delta: 0,
            },
            DownlinkPressureStats::default(),
            cfg,
        );

        assert!(!feedback.is_paused());
        assert_eq!(feedback.resume_edges, 1);
        assert_eq!(
            event,
            Some(TunEgressFeedbackEvent {
                paused: false,
                reason: TunEgressFeedbackReason::PressureLow,
                tx_dropped_delta: 0,
                max_pressure: 0,
                total_pressure: 0,
            }),
            "feedback pause resumes from raw low pressure even while held pressure remains available for backpressure"
        );
    }

    #[test]
    fn tun_egress_feedback_ignores_unavailable_reset_first_and_zero_pressure() {
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 100,
            low_bytes: 40,
        };
        let mut feedback = TunEgressFeedbackState::default();
        let samples = [
            TunEgressDropSample::Unavailable {
                reason: "read_error",
            },
            TunEgressDropSample::First { total: 5 },
            TunEgressDropSample::Reset {
                previous: 10,
                total: 1,
            },
            TunEgressDropSample::Delta {
                total: 10,
                delta: 0,
            },
            TunEgressDropSample::Delta {
                total: 20,
                delta: 10,
            },
        ];
        let stats = [
            DownlinkPressureStats::new(0, 0, 99, 99),
            DownlinkPressureStats::new(0, 0, 99, 99),
            DownlinkPressureStats::new(0, 0, 99, 99),
            DownlinkPressureStats::new(0, 0, 99, 99),
            DownlinkPressureStats::default(),
        ];

        for (sample, stats) in samples.iter().zip(stats) {
            assert_eq!(feedback.update(sample, stats, cfg), None);
        }
        assert!(!feedback.is_paused());
        assert_eq!(
            feedback.drop_events, 1,
            "zero-pressure positive drops are counted but do not pause"
        );
        assert_eq!(feedback.pause_edges, 0);
    }

    #[test]
    fn tun_egress_feedback_diag_formats_transition_counters() {
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 4_194_304,
            low_bytes: 1_048_576,
        };
        let mut feedback = TunEgressFeedbackState::default();
        let event = feedback
            .update(
                &TunEgressDropSample::Delta {
                    total: 28_123,
                    delta: 14_167,
                },
                DownlinkPressureStats::new(0, 0, 3_786_786, 3_786_786),
                cfg,
            )
            .expect("drop delta should pause feedback");
        let mut drop_debt = DownlinkEgressDropDebt::default();
        drop_debt.note_tun_drop(cfg);
        let line = format_tun_egress_feedback_diag(event, &feedback, cfg, drop_debt);

        assert!(line.contains("tcp-tun-egress-feedback"));
        assert!(line.contains("paused=true"));
        assert!(line.contains("reason=drop_delta"));
        assert!(line.contains("tx_dropped_delta=14167"));
        assert!(line.contains("max_pressure=3786786"));
        assert!(line.contains("high=4194304"));
        assert!(line.contains("low=1048576"));
        assert!(line.contains("drop_events=1"));
        assert!(line.contains("drop_delta_total=14167"));
        assert!(line.contains("max_delta=14167"));
        assert!(line.contains("pause_edges=1"));
        assert!(line.contains("resume_edges=0"));
        assert!(line.contains("egress_credit_generation=1"));
        assert!(line.contains("egress_credit_debt_bytes=1572864"));
        assert!(line.contains("drop_credit_debt_bytes=1572864"));
        assert!(line.contains("pressure_credit_debt_bytes=0"));
        assert!(line.contains("drop_credit_events=1"));
        assert!(line.contains("pressure_credit_events=0"));
    }

    #[test]
    fn tun_rx_drain_diag_tracks_budget_and_packet_kinds() {
        let mut diag = TunRxDrainDiag::default();

        diag.note_attempt(TUN_RX_DRAIN_SOURCE_REMOTE_PAYLOAD_PRE);
        diag.note_attempt(TUN_RX_DRAIN_SOURCE_REMOTE_PAYLOAD);
        diag.note_attempt(TUN_RX_DRAIN_SOURCE_REMOTE_PAYLOAD_DEFERRED);
        diag.note_attempt(TUN_RX_DRAIN_SOURCE_REMOTE_PAYLOAD_DEFERRED_DELAYED);
        diag.note_attempt(TUN_RX_DRAIN_SOURCE_REMOTE_PAYLOAD_DEFERRED_PRESSURE);
        diag.note_attempt(TUN_RX_DRAIN_SOURCE_RELAY_GAP_HINT);
        diag.note_attempt(TUN_RX_DRAIN_SOURCE_RELAY_GAP_HINT_FOLLOWUP);
        diag.note_attempt(TUN_RX_DRAIN_SOURCE_TIMER_PRESSURE);
        diag.note_attempt(TUN_RX_DRAIN_SOURCE_TIMER_ACTIVE_FLOW);
        diag.note_attempt("other");
        diag.note_packet(TunRxPacketKind::Tcp);
        diag.note_packet(TunRxPacketKind::Dns);
        diag.note_packet(TunRxPacketKind::Udp);
        diag.note_budget_exhausted();
        diag.note_would_block();
        diag.note_error();

        let line = format_tun_rx_drain_diag(&diag);
        assert!(line.contains("tcp-tun-rx-drain"));
        assert!(line.contains("attempts=10"), "{line}");
        assert!(line.contains("pre_payload_attempts=1"), "{line}");
        assert!(line.contains("remote_payload_attempts=1"), "{line}");
        assert!(
            line.contains("remote_payload_deferred_attempts=1"),
            "{line}"
        );
        assert!(
            line.contains("remote_payload_deferred_delayed_attempts=1"),
            "{line}"
        );
        assert!(
            line.contains("remote_payload_deferred_pressure_attempts=1"),
            "{line}"
        );
        assert!(line.contains("relay_gap_hint_attempts=1"), "{line}");
        assert!(
            line.contains("relay_gap_hint_followup_attempts=1"),
            "{line}"
        );
        assert!(line.contains("maintenance_attempts=1"), "{line}");
        assert!(line.contains("timer_active_flow_attempts=1"), "{line}");
        assert!(line.contains("other_attempts=1"), "{line}");
        assert!(line.contains("packets=3"), "{line}");
        assert!(line.contains("tcp=1"), "{line}");
        assert!(line.contains("dns=1"), "{line}");
        assert!(line.contains("udp=1"), "{line}");
        assert!(line.contains("budget_exhausted=1"), "{line}");
        assert!(line.contains("would_block=1"), "{line}");
        assert!(line.contains("errors=1"), "{line}");
    }

    #[test]
    fn tun_rx_drain_after_remote_payload_tracks_acceptance_or_pending() {
        assert!(should_drain_tun_rx_after_remote_payload(None, 1));
        assert!(!should_drain_tun_rx_after_remote_payload(None, 0));

        let mut ctx = SocketCtx::new(443);
        assert!(!should_drain_tun_rx_after_remote_payload(Some(&ctx), 0));

        ctx.downlink_pending.extend_from_slice(&[1, 2, 3]);
        assert!(should_drain_tun_rx_after_remote_payload(Some(&ctx), 0));
    }

    #[test]
    fn relay_gap_ack_drain_state_coalesces_budget_exhausted_followups() {
        let mut state = RelayGapAckDrainState::default();

        assert!(!state.is_armed());
        assert!(state.arm_after_budget_exhausted());
        assert!(state.is_armed());
        assert!(
            !state.arm_after_budget_exhausted(),
            "additional exhausted gap drains should share the already armed follow-up"
        );
        assert_eq!(
            state.take_due(),
            Some(RelayGapAckDrainFollowup::BudgetExhausted)
        );
        assert!(!state.is_armed());
        assert!(
            state.take_due().is_none(),
            "a consumed follow-up should not fire again without another exhausted drain"
        );
        assert!(state.arm_after_budget_exhausted());
    }

    #[test]
    fn relay_gap_ack_drain_state_probes_after_empty_hints_then_stops() {
        let mut state = RelayGapAckDrainState::default();

        assert!(state.arm_after_hint_drain(0, 240));
        assert_eq!(state.take_due(), Some(RelayGapAckDrainFollowup::Probe));

        for remaining in (0..=RELAY_GAP_ACK_DRAIN_EMPTY_FOLLOWUPS).rev() {
            let should_rearm = remaining > 0;
            assert_eq!(
                state.arm_after_followup_drain(RelayGapAckDrainFollowup::Probe, 0, 240),
                should_rearm,
                "empty relay-gap probes must be bounded"
            );
            if should_rearm {
                assert_eq!(state.take_due(), Some(RelayGapAckDrainFollowup::Probe));
            }
        }
        assert!(!state.is_armed());
    }

    #[test]
    fn relay_gap_ack_drain_state_keeps_progress_probes_short() {
        let mut state = RelayGapAckDrainState::default();

        assert!(state.arm_after_hint_drain(1, 240));
        assert_eq!(state.take_due(), Some(RelayGapAckDrainFollowup::Probe));
        assert!(state.arm_after_followup_drain(RelayGapAckDrainFollowup::Probe, 0, 240));
        assert_eq!(state.take_due(), Some(RelayGapAckDrainFollowup::Probe));
        assert!(
            !state.arm_after_followup_drain(RelayGapAckDrainFollowup::Probe, 0, 240),
            "a progress probe gets one trailing empty retry, not an unbounded timer"
        );
    }

    #[test]
    fn relay_gap_ack_drain_state_escalates_probe_back_to_budget_followup() {
        let mut state = RelayGapAckDrainState::default();

        assert!(state.arm_after_hint_drain(0, 240));
        assert_eq!(state.take_due(), Some(RelayGapAckDrainFollowup::Probe));
        assert!(state.arm_after_followup_drain(RelayGapAckDrainFollowup::Probe, 240, 240));
        assert_eq!(
            state.take_due(),
            Some(RelayGapAckDrainFollowup::BudgetExhausted),
            "a probe that drains a full budget must switch to the existing pressure follow-up path"
        );
    }

    #[test]
    fn deferred_ack_drain_state_coalesces_remote_payload_bursts() {
        let mut state = DeferredAckDrainState::default();

        assert!(!state.is_armed());
        assert!(state.arm_after_remote_payload());
        assert!(state.is_armed());
        assert!(
            !state.arm_after_remote_payload(),
            "additional remote payloads should share the already armed one-shot drain"
        );
        assert_eq!(state.take_due(), Some(DeferredAckDrainMode::ActiveFlow));
        assert!(state.arm_after_delayed_ack_probe());
        assert_eq!(state.take_due(), Some(DeferredAckDrainMode::DelayedAck));
        assert!(
            state.take_due().is_none(),
            "a delayed ACK probe is a bounded second chance, not a repeating timer"
        );
        assert!(!state.is_armed());
        assert!(
            state.take_due().is_none(),
            "a consumed one-shot should not fire again without new downlink work"
        );
        assert!(state.arm_after_remote_payload());
    }

    #[test]
    fn deferred_ack_drain_state_escalates_after_budget_exhaustion() {
        let mut state = DeferredAckDrainState::default();

        assert!(state.take_due().is_none());
        assert!(state.arm_after_remote_payload());
        assert_eq!(state.take_due(), Some(DeferredAckDrainMode::ActiveFlow));

        assert!(state.arm_after_budget_exhausted());
        assert_eq!(state.take_due(), Some(DeferredAckDrainMode::Pressure));
        assert!(state.arm_after_delayed_ack_probe());
        assert_eq!(state.take_due(), Some(DeferredAckDrainMode::DelayedAck));
        assert!(state.arm_after_budget_exhausted());
        assert_eq!(
            state.take_due(),
            Some(DeferredAckDrainMode::Pressure),
            "a delayed ACK probe that exhausts its budget should reuse the bounded pressure path"
        );
        assert!(state.arm_after_budget_exhausted());
        assert_eq!(
            state.take_due(),
            Some(DeferredAckDrainMode::Pressure),
            "pressure follow-up should stay pressure-sized while it continues exhausting"
        );

        assert!(state.arm_after_remote_payload());
        assert_eq!(
            state.take_due(),
            Some(DeferredAckDrainMode::ActiveFlow),
            "a fresh remote payload starts with the normal active-flow budget"
        );
    }

    #[test]
    fn deferred_ack_drain_budget_tapers_with_egress_headroom() {
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 524_288,
            low_bytes: 131_072,
        };
        let healthy = DownlinkPressureStats::new(
            0,
            0,
            tx_queue_flush_threshold(cfg).saturating_sub(1),
            tx_queue_flush_threshold(cfg).saturating_sub(1),
        );
        let near_credit = DownlinkPressureStats::new(
            0,
            0,
            tx_queue_credit_spend_threshold(cfg).saturating_sub(1),
            tx_queue_credit_spend_threshold(cfg).saturating_sub(1),
        );
        let credit_edge = DownlinkPressureStats::new(
            0,
            0,
            tx_queue_credit_spend_threshold(cfg),
            tx_queue_credit_spend_threshold(cfg),
        );
        let pause_edge = DownlinkPressureStats::new(
            0,
            0,
            tx_queue_pause_threshold(cfg),
            tx_queue_pause_threshold(cfg),
        );

        assert_eq!(
            tun_rx_drain_budget_for_deferred_ack_drain(
                DeferredAckDrainMode::ActiveFlow,
                healthy,
                false,
                0,
                cfg,
                1_200
            ),
            active_flow_tun_rx_drain_budget(cfg, 1_200),
            "deferred ACK drain may run at full budget while egress has flush headroom"
        );
        assert_eq!(
            tun_rx_drain_budget_for_deferred_ack_drain(
                DeferredAckDrainMode::ActiveFlow,
                near_credit,
                false,
                0,
                cfg,
                1_200
            ),
            1,
            "deferred ACK drain should taper to a single ACK/window packet near the credit edge"
        );
        assert_eq!(
            tun_rx_drain_budget_for_deferred_ack_drain(
                DeferredAckDrainMode::ActiveFlow,
                credit_edge,
                false,
                0,
                cfg,
                1_200
            ),
            1,
            "credit edge still needs a trickle ACK drain to avoid TUIC stream starvation"
        );
        assert_eq!(
            tun_rx_drain_budget_for_deferred_ack_drain(
                DeferredAckDrainMode::ActiveFlow,
                pause_edge,
                false,
                0,
                cfg,
                1_200
            ),
            0,
            "deferred ACK drain must stop at the hard local egress pause edge"
        );
        assert_eq!(
            tun_rx_drain_budget_for_deferred_ack_drain(
                DeferredAckDrainMode::ActiveFlow,
                healthy,
                true,
                0,
                cfg,
                1_200
            ),
            0,
            "TUN egress feedback pause should suppress active ACK drain"
        );
        assert_eq!(
            tun_rx_drain_budget_for_deferred_ack_drain(
                DeferredAckDrainMode::ActiveFlow,
                healthy,
                false,
                9,
                cfg,
                1_200
            ),
            9,
            "explicit operator budget still controls the healthy maximum"
        );
        assert_eq!(
            tun_rx_drain_budget_for_deferred_ack_drain(
                DeferredAckDrainMode::ActiveFlow,
                credit_edge,
                false,
                9,
                cfg,
                1_200
            ),
            1,
            "explicit operator budget is still clamped by the safety headroom curve"
        );
        assert_eq!(
            tun_rx_drain_budget_for_deferred_ack_drain(
                DeferredAckDrainMode::DelayedAck,
                healthy,
                false,
                0,
                cfg,
                1_200
            ),
            active_flow_tun_rx_drain_budget(cfg, 1_200),
            "delayed ACK probe uses the same small active-flow budget as the immediate deferred drain"
        );
        assert_eq!(
            tun_rx_drain_budget_for_deferred_ack_drain(
                DeferredAckDrainMode::Pressure,
                healthy,
                false,
                0,
                cfg,
                1_200
            ),
            pressure_tun_rx_drain_budget(cfg, 1_200),
            "a budget-exhausted deferred drain escalates to pressure budget"
        );
    }

    #[test]
    fn relay_gap_hint_tun_rx_drain_budget_requires_current_relay_epoch() {
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 524_288,
            low_bytes: 131_072,
        };
        let healthy = DownlinkPressureStats::new(
            0,
            0,
            tx_queue_flush_threshold(cfg).saturating_sub(1),
            tx_queue_flush_threshold(cfg).saturating_sub(1),
        );
        let near_credit = DownlinkPressureStats::new(
            0,
            0,
            tx_queue_credit_spend_threshold(cfg).saturating_sub(1),
            tx_queue_credit_spend_threshold(cfg).saturating_sub(1),
        );
        let credit_edge = DownlinkPressureStats::new(
            0,
            0,
            tx_queue_credit_spend_threshold(cfg),
            tx_queue_credit_spend_threshold(cfg),
        );
        let pause_edge = DownlinkPressureStats::new(
            0,
            0,
            tx_queue_pause_threshold(cfg),
            tx_queue_pause_threshold(cfg),
        );
        let mut ctx = SocketCtx::new(443);
        ctx.state = SocketState::Relaying;
        ctx.conn_epoch = 7;

        assert_eq!(
            tun_rx_drain_budget_for_relay_gap_hint(Some(&ctx), 6, healthy, false, 0, cfg, 1_200),
            0,
            "stale relay gap hints must not scan TUN RX"
        );

        ctx.state = SocketState::Listening;
        assert_eq!(
            tun_rx_drain_budget_for_relay_gap_hint(Some(&ctx), 7, healthy, false, 0, cfg, 1_200),
            0,
            "non-relay slots must ignore late gap hints"
        );

        ctx.state = SocketState::Relaying;
        assert_eq!(
            tun_rx_drain_budget_for_relay_gap_hint(Some(&ctx), 7, healthy, false, 0, cfg, 1_200),
            pressure_tun_rx_drain_budget(cfg, 1_200),
            "current relay read gaps are a strong ACK/window starvation signal"
        );
        assert_eq!(
            tun_rx_drain_budget_for_relay_gap_hint(
                Some(&ctx),
                7,
                near_credit,
                false,
                0,
                cfg,
                1_200
            ),
            1,
            "relay gap hints should taper to a single packet near the credit edge"
        );
        assert_eq!(
            tun_rx_drain_budget_for_relay_gap_hint(
                Some(&ctx),
                7,
                credit_edge,
                false,
                0,
                cfg,
                1_200
            ),
            1,
            "relay gap hints must keep a trickle ACK drain at the credit edge"
        );
        assert_eq!(
            tun_rx_drain_budget_for_relay_gap_hint(Some(&ctx), 7, pause_edge, false, 0, cfg, 1_200),
            0,
            "relay gap hints should stop once local egress reaches the hard pause edge"
        );
        assert_eq!(
            tun_rx_drain_budget_for_relay_gap_hint(Some(&ctx), 7, healthy, true, 0, cfg, 1_200),
            0,
            "TUN feedback pause should suppress relay gap hints"
        );
        assert_eq!(
            tun_rx_drain_budget_for_relay_gap_hint(Some(&ctx), 7, healthy, false, 9, cfg, 1_200),
            9,
            "explicit operator drain budget still controls the healthy maximum"
        );
        assert_eq!(
            tun_rx_drain_budget_for_relay_gap_hint(
                Some(&ctx),
                7,
                credit_edge,
                false,
                9,
                cfg,
                1_200
            ),
            1,
            "explicit operator drain budget is still clamped by the safety headroom curve"
        );
    }

    #[test]
    fn relay_gap_followup_tun_rx_drain_budget_tapers_with_egress_headroom() {
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 524_288,
            low_bytes: 131_072,
        };
        let healthy = DownlinkPressureStats::new(
            0,
            0,
            tx_queue_flush_threshold(cfg).saturating_sub(1),
            tx_queue_flush_threshold(cfg).saturating_sub(1),
        );
        let near_credit = DownlinkPressureStats::new(
            0,
            0,
            tx_queue_credit_spend_threshold(cfg).saturating_sub(1),
            tx_queue_credit_spend_threshold(cfg).saturating_sub(1),
        );
        let credit_edge = DownlinkPressureStats::new(
            0,
            0,
            tx_queue_credit_spend_threshold(cfg),
            tx_queue_credit_spend_threshold(cfg),
        );
        let pause_edge = DownlinkPressureStats::new(
            0,
            0,
            tx_queue_pause_threshold(cfg),
            tx_queue_pause_threshold(cfg),
        );

        assert_eq!(
            tun_rx_drain_budget_for_relay_gap_followup(healthy, false, 0, cfg, 1_200),
            pressure_tun_rx_drain_budget(cfg, 1_200),
            "gap follow-up should continue at pressure budget while egress is healthy"
        );
        assert_eq!(
            tun_rx_drain_budget_for_relay_gap_followup(near_credit, false, 0, cfg, 1_200),
            1,
            "gap follow-up should taper to a single ACK/window packet near credit edge"
        );
        assert_eq!(
            tun_rx_drain_budget_for_relay_gap_followup(credit_edge, false, 0, cfg, 1_200),
            1,
            "credit edge still keeps a trickle ACK drain for window recovery"
        );
        assert_eq!(
            tun_rx_drain_budget_for_relay_gap_followup(pause_edge, false, 0, cfg, 1_200),
            0,
            "gap follow-up must stop at the hard local egress pause edge"
        );
        assert_eq!(
            tun_rx_drain_budget_for_relay_gap_followup(healthy, true, 0, cfg, 1_200),
            0,
            "TUN feedback pause should suppress gap follow-up"
        );
        assert_eq!(
            tun_rx_drain_budget_for_relay_gap_followup(healthy, false, 9, cfg, 1_200),
            9,
            "explicit operator budget still controls the healthy maximum"
        );
    }

    #[test]
    fn tun_rx_drain_budget_keeps_explicit_override() {
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 100,
            low_bytes: 40,
        };
        assert_eq!(
            tun_rx_drain_budget_after_remote_payload(false, 0, 7, cfg, 1200),
            0,
            "explicit budget should not drain when the payload has no local work"
        );
        assert_eq!(
            tun_rx_drain_budget_after_remote_payload(true, 0, 7, cfg, 1200),
            7,
            "explicit MINI_VPN_TUN_RX_DRAIN_BUDGET remains an override"
        );
        assert_eq!(
            tun_rx_drain_budget_after_remote_payload(
                true,
                tx_queue_credit_spend_threshold(cfg),
                7,
                cfg,
                1200,
            ),
            7,
            "explicit override should not be reshaped by adaptive pressure"
        );
    }

    #[test]
    fn tun_rx_drain_budget_stays_off_without_downlink_work() {
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 100,
            low_bytes: 40,
        };
        let credit_edge = tx_queue_credit_spend_threshold(cfg);

        assert_eq!(
            tun_rx_drain_budget_after_remote_payload(false, credit_edge, 0, cfg, 1200),
            0,
            "pressure alone should not scan TUN RX without downlink work"
        );
    }

    #[test]
    fn tun_rx_drain_budget_enables_active_flow_below_credit_edge() {
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 100,
            low_bytes: 40,
        };
        let credit_edge = tx_queue_credit_spend_threshold(cfg);

        assert_eq!(
            tun_rx_drain_budget_after_remote_payload(
                true,
                credit_edge.saturating_sub(1),
                0,
                cfg,
                1200,
            ),
            active_flow_tun_rx_drain_budget(cfg, 1200),
            "active reverse payload should get a bounded ACK/window drain before pressure"
        );
    }

    #[test]
    fn tun_rx_drain_budget_enables_adaptive_at_credit_edge() {
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 100,
            low_bytes: 40,
        };
        assert_eq!(tx_queue_credit_guard_bytes(cfg), 3);
        assert_eq!(tx_queue_credit_spend_threshold(cfg), 157);

        assert_eq!(
            tun_rx_drain_budget_after_remote_payload(
                true,
                tx_queue_credit_spend_threshold(cfg),
                0,
                cfg,
                1200,
            ),
            1,
            "small guards should still get one pressure ACK drain packet"
        );
    }

    #[test]
    fn pre_payload_tun_rx_drain_budget_requires_current_accepting_payload() {
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 100,
            low_bytes: 40,
        };
        let (tx, _rx) = mpsc::channel(1);
        let mut ctx = SocketCtx::new(443);
        ctx.state = SocketState::Relaying;
        ctx.uplink_tx = Some(tx);
        ctx.conn_epoch = 7;
        let active_pressure = SocketCloseSnapshot {
            tcp_state: TcpState::Established,
            active: true,
            can_send: true,
            can_recv: true,
            may_send: true,
            may_recv: true,
            send_capacity: 1_048_576,
            send_queue: tx_queue_credit_spend_threshold(cfg),
            recv_queue: 0,
        };

        assert_eq!(
            tun_rx_drain_budget_before_remote_payload(
                Some(&ctx),
                active_pressure,
                7,
                1_200,
                0,
                cfg,
                1_200,
            ),
            1,
            "current accepting payload at credit edge should pre-drain ACKs"
        );
        assert_eq!(
            tun_rx_drain_budget_before_remote_payload(
                Some(&ctx),
                active_pressure,
                7,
                0,
                0,
                cfg,
                1_200,
            ),
            0,
            "empty payload should not scan TUN RX"
        );
        assert_eq!(
            tun_rx_drain_budget_before_remote_payload(
                Some(&ctx),
                active_pressure,
                6,
                1_200,
                0,
                cfg,
                1_200,
            ),
            0,
            "stale epoch must not pre-drain for a rearmed socket"
        );
        assert_eq!(
            tun_rx_drain_budget_before_remote_payload(
                None,
                active_pressure,
                7,
                1_200,
                0,
                cfg,
                1_200,
            ),
            0,
            "missing context cannot prove lifecycle eligibility"
        );

        let terminal = SocketCloseSnapshot {
            tcp_state: TcpState::Closed,
            active: false,
            can_send: false,
            can_recv: false,
            may_send: false,
            may_recv: false,
            send_capacity: 1_048_576,
            send_queue: tx_queue_credit_spend_threshold(cfg),
            recv_queue: 0,
        };
        assert_eq!(
            tun_rx_drain_budget_before_remote_payload(
                Some(&ctx),
                terminal,
                7,
                1_200,
                0,
                cfg,
                1_200,
            ),
            0,
            "terminal no-send socket must not pre-drain before rejecting late payload"
        );
    }

    #[test]
    fn maintenance_tun_rx_drain_budget_requires_dirty_pressure() {
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 100,
            low_bytes: 40,
        };
        let below_target = DownlinkPressureStats::new(
            0,
            0,
            tx_queue_egress_target_threshold(cfg).saturating_sub(1),
            tx_queue_egress_target_threshold(cfg).saturating_sub(1),
        );
        let target_edge = DownlinkPressureStats::new(
            0,
            0,
            tx_queue_egress_target_threshold(cfg),
            tx_queue_egress_target_threshold(cfg),
        );
        let credit_edge = DownlinkPressureStats::new(
            0,
            0,
            tx_queue_credit_spend_threshold(cfg),
            tx_queue_credit_spend_threshold(cfg),
        );
        let pending_with_low_tx = DownlinkPressureStats::new(cfg.high_bytes, cfg.high_bytes, 1, 1);
        let pending_with_flush_edge = DownlinkPressureStats::new(
            cfg.high_bytes,
            cfg.high_bytes,
            tx_queue_flush_threshold(cfg),
            tx_queue_flush_threshold(cfg),
        );
        let pending_tail_below_high_at_flush_edge = DownlinkPressureStats::new(
            cfg.low_bytes,
            cfg.low_bytes,
            tx_queue_flush_threshold(cfg),
            tx_queue_flush_threshold(cfg),
        );

        assert_eq!(
            tun_rx_drain_budget_for_dirty_pressure(false, false, credit_edge, 0, cfg, 1_200),
            0,
            "pressure without dirty downlink work should not scan TUN RX"
        );
        assert_eq!(
            tun_rx_drain_budget_for_dirty_pressure(true, false, below_target, 0, cfg, 1_200),
            0,
            "maintenance drain should stay off below the egress target"
        );
        assert_eq!(
            tun_rx_drain_budget_for_dirty_pressure(true, true, below_target, 0, cfg, 1_200),
            1,
            "deferred close drain should keep ACK/window service even below the pressure edge"
        );
        assert_eq!(
            tun_rx_drain_budget_for_dirty_pressure(true, false, target_edge, 0, cfg, 1_200),
            0,
            "maintenance drain should treat the egress target as soft pressure"
        );
        assert_eq!(
            tun_rx_drain_budget_for_dirty_pressure(true, false, credit_edge, 0, cfg, 1_200),
            1,
            "the old credit edge remains pressure-eligible"
        );
        assert_eq!(
            tun_rx_drain_budget_for_dirty_pressure(true, false, credit_edge, 9, cfg, 1_200),
            9,
            "explicit drain budget remains an operator override once pressure is eligible"
        );
        assert_eq!(
            tun_rx_drain_budget_for_dirty_pressure(true, false, pending_with_low_tx, 0, cfg, 1_200),
            0,
            "pending-only pressure should not drain ACKs without egress queue pressure"
        );
        assert_eq!(
            tun_rx_drain_budget_for_dirty_pressure(
                true,
                false,
                pending_with_flush_edge,
                0,
                cfg,
                1_200
            ),
            1,
            "pending pressure plus flush-edge tx queue should maintain ACK drain"
        );
        assert_eq!(
            tun_rx_drain_budget_for_dirty_pressure(
                true,
                false,
                pending_tail_below_high_at_flush_edge,
                0,
                cfg,
                1_200
            ),
            1,
            "a dirty tail with pending below high but tx queue at the flush edge should keep ACK/window maintenance alive"
        );
    }

    #[test]
    fn dirty_close_drain_tracks_pending_or_egress_tail() {
        let mut sockets = SocketSet::new(vec![]);
        let handle = sockets.add(build_listener_socket(&ListenerSpec { local_port: 443 }));
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 100,
            low_bytes: 40,
        };
        let mut dirty = HashSet::from([handle]);
        let mut ctxs = HashMap::from([(handle, SocketCtx::new(443))]);

        assert!(
            !has_dirty_close_drain(&dirty, &ctxs, cfg),
            "ordinary dirty handles should not trigger close-drain maintenance"
        );

        {
            let ctx = ctxs.get_mut(&handle).unwrap();
            ctx.pending_relay_close = Some(RelayClose {
                epoch: 1,
                direction: "local_to_remote",
                reason: "uplink_channel_closed",
            });
            ctx.downlink_pending = vec![1, 2, 3];
        }
        assert!(
            has_dirty_close_drain(&dirty, &ctxs, cfg),
            "pending deferred close should keep ACK/window maintenance active"
        );

        {
            let ctx = ctxs.get_mut(&handle).unwrap();
            ctx.downlink_pending.clear();
            ctx.pending_relay_close_last_egress_queue_bytes = cfg.low_bytes + 1;
        }
        assert!(
            has_dirty_close_drain(&dirty, &ctxs, cfg),
            "egress-only deferred close above low watermark should keep maintenance active"
        );

        ctxs.get_mut(&handle)
            .unwrap()
            .pending_relay_close_last_egress_queue_bytes = cfg.low_bytes;
        assert!(
            !has_dirty_close_drain(&dirty, &ctxs, cfg),
            "close-drain maintenance can stop once pending is empty and egress reaches low"
        );

        dirty.clear();
        assert!(
            !has_dirty_close_drain(&dirty, &ctxs, cfg),
            "non-dirty close state should not scan TUN RX"
        );
    }

    #[test]
    fn recent_active_timer_drain_budget_requires_recent_downlink_work() {
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 100,
            low_bytes: 40,
        };
        let healthy = DownlinkPressureStats::default();
        let credit_edge = DownlinkPressureStats::new(
            0,
            0,
            tx_queue_credit_spend_threshold(cfg),
            tx_queue_credit_spend_threshold(cfg),
        );
        let pause_edge = DownlinkPressureStats::new(
            0,
            0,
            tx_queue_pause_threshold(cfg),
            tx_queue_pause_threshold(cfg),
        );

        assert_eq!(
            tun_rx_drain_budget_for_recent_active_flow(false, 250, healthy, false, 0, cfg, 1_200),
            0,
            "timer drain must stay off with no recent downlink work"
        );
        assert_eq!(
            tun_rx_drain_budget_for_recent_active_flow(true, 0, healthy, false, 0, cfg, 1_200),
            0,
            "operator override 0 should still disable the active-flow service lane"
        );
        assert_eq!(
            tun_rx_drain_budget_for_recent_active_flow(true, 250, healthy, false, 0, cfg, 1_200),
            active_flow_tun_rx_drain_budget(cfg, 1_200),
            "recent active downlink should get the bounded active-flow budget while egress is healthy"
        );
        assert_eq!(
            tun_rx_drain_budget_for_recent_active_flow(
                true,
                250,
                credit_edge,
                false,
                9,
                cfg,
                1_200
            ),
            1,
            "active-flow service lane must taper to one packet at the credit edge"
        );
        assert_eq!(
            tun_rx_drain_budget_for_recent_active_flow(true, 250, pause_edge, false, 9, cfg, 1_200),
            0,
            "active-flow service lane must stop at the hard local egress pause edge"
        );
        assert_eq!(
            tun_rx_drain_budget_for_recent_active_flow(true, 250, healthy, true, 9, cfg, 1_200),
            0,
            "TUN egress feedback pause must suppress the active-flow service lane"
        );
        assert_eq!(
            tun_rx_drain_budget_for_recent_active_flow(true, 250, healthy, false, 9, cfg, 1_200),
            9,
            "explicit drain budget remains the healthy maximum inside the recent-active window"
        );
    }

    #[test]
    fn tun_rx_pressure_drain_budget_is_ack_sized_and_capped() {
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 512 * 1024,
            low_bytes: 128 * 1024,
        };
        assert_eq!(tx_queue_credit_guard_bytes(cfg), 24 * 1024);
        assert_eq!(
            pressure_tun_rx_drain_budget(cfg, 1200),
            TUN_RX_PRESSURE_DRAIN_MAX_PACKETS,
            "default guard should be ACK-sized and capped, not MTU-sized"
        );
        assert_eq!(
            pressure_tun_rx_drain_budget(cfg, 1),
            TUN_RX_PRESSURE_DRAIN_MAX_PACKETS,
            "adaptive pressure drain remains bounded even with tiny MTU input"
        );
    }

    #[test]
    fn tun_rx_active_flow_drain_budget_is_smaller_than_pressure() {
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 512 * 1024,
            low_bytes: 128 * 1024,
        };

        assert_eq!(
            pressure_tun_rx_drain_budget(cfg, 1200),
            TUN_RX_PRESSURE_DRAIN_MAX_PACKETS
        );
        assert_eq!(
            active_flow_tun_rx_drain_budget(cfg, 1200),
            TUN_RX_ACTIVE_FLOW_DRAIN_MAX_PACKETS,
            "active-flow drain should be useful but smaller than pressure drain"
        );
        assert!(
            active_flow_tun_rx_drain_budget(cfg, 1200) < pressure_tun_rx_drain_budget(cfg, 1200)
        );
    }

    #[test]
    fn downlink_backpressure_defaults_match_knife14ao_ab() {
        let default = DownlinkBackpressureConfig::default();
        assert_eq!(default.high_bytes, 512 * 1024);
        assert_eq!(default.low_bytes, 128 * 1024);
    }

    #[test]
    fn downlink_flush_budget_caps_each_pass() {
        assert_eq!(
            bounded_downlink_flush_len(2_000_000, 262_144),
            262_144,
            "large pending should be split across multiple flush passes"
        );
        assert_eq!(
            bounded_downlink_flush_len(16_384, 262_144),
            16_384,
            "small pending should flush in one pass"
        );
        assert_eq!(
            bounded_downlink_flush_len(16_384, 0),
            1,
            "defensive zero budget must still make bounded progress"
        );
    }

    #[test]
    fn downlink_flush_len_respects_remaining_egress_headroom() {
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 100,
            low_bytes: 40,
        };
        let send_window = TcpSendWindowSnapshot {
            can_send: true,
            may_send: true,
            may_recv: true,
            send_capacity: 64,
            send_queue: tx_queue_flush_threshold(cfg) - 2,
            recv_queue: 0,
        };

        assert_eq!(
            bounded_downlink_flush_len_for_window(64, 64, send_window, cfg),
            2,
            "flush should only accept the remaining clean tx_queue headroom"
        );
    }

    #[test]
    fn auto_tx_queue_thresholds_fit_default_tun_egress_capacity() {
        let tun_mtu = 1_200;
        let tun_tx_queue_len = DEFAULT_TUN_TX_QUEUE_LEN_ESTIMATE;
        let cfg = default_downlink_backpressure_for_tun_egress(tun_mtu, tun_tx_queue_len);

        assert!(
            cfg.high_bytes < DownlinkBackpressureConfig::default().high_bytes,
            "1200-byte MTU with Linux default qlen must cap the old fixed high watermark"
        );
        assert!(
            tx_queue_flush_threshold(cfg)
                <= tun_egress_safe_flush_threshold(tun_mtu, tun_tx_queue_len),
            "clean flush edge must stay below the safe TUN qdisc fill level"
        );
        assert!(
            tx_queue_pause_threshold(cfg) <= tun_egress_capacity_bytes(tun_mtu, tun_tx_queue_len),
            "hard pause edge must fit inside the estimated TUN qdisc capacity"
        );
    }

    #[test]
    fn close_drain_flush_limit_accepts_terminal_tail_inside_credit_guard() {
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 524_288,
            low_bytes: 131_072,
        };
        let mut clock = DownlinkEgressClock::default();
        let mut drop_debt = DownlinkEgressDropDebt::default();
        drop_debt.install(EgressCreditDebtSource::Pressure, 36_431);
        let send_window = TcpSendWindowSnapshot {
            can_send: true,
            may_send: true,
            may_recv: true,
            send_capacity: 1_048_576,
            send_queue: tx_queue_flush_threshold(cfg),
            recv_queue: 0,
        };
        let normal = bounded_downlink_flush_limit_for_window_with_clock_and_drop(
            56_911,
            262_144,
            send_window,
            cfg,
            &mut clock,
            &mut drop_debt,
        );

        assert_eq!(
            normal.len, 0,
            "ordinary downlink flush stays blocked at clean high while pressure debt is active"
        );

        let close_drain =
            close_drain_terminal_pending_flush_limit(56_911, 262_144, send_window, cfg, normal);
        assert_eq!(
            close_drain.len, 56_911,
            "terminal close-drain may accept the small tail so FIN can follow real data"
        );
        assert!(
            send_window.send_queue + close_drain.len <= tx_queue_credit_spend_threshold(cfg),
            "close-drain tail remains below the hard credit guard"
        );
    }

    #[test]
    fn close_drain_large_tail_stays_below_egress_target() {
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 327_272,
            low_bytes: 81_818,
        };
        let mut clock = DownlinkEgressClock::default();
        let mut drop_debt = DownlinkEgressDropDebt::default();
        drop_debt.install(EgressCreditDebtSource::Pressure, 99_847);
        let send_window = TcpSendWindowSnapshot {
            can_send: true,
            may_send: true,
            may_recv: false,
            send_capacity: 1_048_576,
            send_queue: 496_761,
            recv_queue: 0,
        };
        let normal = bounded_downlink_flush_limit_for_window_with_clock_and_drop(
            91_438,
            262_144,
            send_window,
            cfg,
            &mut clock,
            &mut drop_debt,
        );

        assert_eq!(
            normal.len, 0,
            "ordinary flush must not expand while pressure debt is still active"
        );

        let close_drain =
            close_drain_terminal_pending_flush_limit(91_438, 262_144, send_window, cfg, normal);
        assert_eq!(
            send_window.send_queue + close_drain.len,
            tx_queue_egress_target_threshold(cfg),
            "large close-tail drain must stop at the egress target instead of the old credit edge"
        );
        assert!(
            send_window.send_queue + close_drain.len < tx_queue_credit_spend_threshold(cfg),
            "Knife14ey failure shape must not be allowed to refill to the old credit edge"
        );
        assert!(
            close_drain.hard_edge_guard_deferred_bytes > 0,
            "target-limited close-drain should feed hard feedback into the controller"
        );
    }

    #[test]
    fn downlink_flush_len_uses_observed_egress_drain_credit() {
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 100,
            low_bytes: 40,
        };
        let mut clock = DownlinkEgressClock::default();
        let at_clean_cap = TcpSendWindowSnapshot {
            can_send: true,
            may_send: true,
            may_recv: true,
            send_capacity: 100,
            send_queue: tx_queue_flush_threshold(cfg),
            recv_queue: 0,
        };

        let first = bounded_downlink_flush_limit_for_window_with_clock(
            100,
            100,
            at_clean_cap,
            cfg,
            &mut clock,
        );
        assert_eq!(first.len, 0, "no observed drain means no extra credit");
        clock.note_flush_result(at_clean_cap.send_queue, 0, first);

        let after_drain = TcpSendWindowSnapshot {
            send_queue: 100,
            ..at_clean_cap
        };
        let second = bounded_downlink_flush_limit_for_window_with_clock(
            100,
            100,
            after_drain,
            cfg,
            &mut clock,
        );
        assert_eq!(
            after_drain.send_queue + second.len,
            tx_queue_credit_spend_threshold(cfg),
            "observed drain should grant one-shot credit up to the credit edge"
        );
        assert_eq!(second.drain_credit_granted_bytes, 30);
        assert_eq!(
            second.drain_credit_planned_bytes,
            tx_queue_credit_spend_threshold(cfg) - tx_queue_flush_threshold(cfg)
        );
        assert_eq!(
            second.hard_edge_guard_bytes,
            tx_queue_pause_threshold(cfg) - tx_queue_credit_spend_threshold(cfg)
        );
        assert_eq!(
            second.hard_edge_guard_deferred_bytes,
            tx_queue_pause_threshold(cfg) - tx_queue_credit_spend_threshold(cfg)
        );
        clock.note_flush_result(after_drain.send_queue, second.len, second);

        let at_hard_pause = TcpSendWindowSnapshot {
            send_queue: tx_queue_pause_threshold(cfg),
            ..after_drain
        };
        let third = bounded_downlink_flush_limit_for_window_with_clock(
            100,
            100,
            at_hard_pause,
            cfg,
            &mut clock,
        );
        assert_eq!(
            third.len, 0,
            "drain credit must not let a pass plan beyond the hard pause threshold"
        );
    }

    #[test]
    fn downlink_flush_len_spends_clean_drain_credit_to_credit_edge() {
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 100,
            low_bytes: 40,
        };
        let mut clock = DownlinkEgressClock {
            last_send_queue: Some(tx_queue_flush_threshold(cfg)),
            drain_credit_bytes: downlink_egress_credit_span(cfg),
            drop_debt_generation_seen: 0,
        };
        let send_window = TcpSendWindowSnapshot {
            can_send: true,
            may_send: true,
            may_recv: true,
            send_capacity: 100,
            send_queue: tx_queue_flush_threshold(cfg),
            recv_queue: 0,
        };

        let limit = bounded_downlink_flush_limit_for_window_with_clock(
            100,
            100,
            send_window,
            cfg,
            &mut clock,
        );

        assert_eq!(
            send_window.send_queue + limit.len,
            tx_queue_credit_spend_threshold(cfg),
            "without active debt or drops, clean drain credit should spend up to the credit edge"
        );
        assert_eq!(
            limit.drain_credit_planned_bytes,
            tx_queue_credit_spend_threshold(cfg) - tx_queue_flush_threshold(cfg)
        );
    }

    #[test]
    fn downlink_drain_credit_keeps_guard_below_hard_pause_edge() {
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 100,
            low_bytes: 40,
        };
        let mut clock = DownlinkEgressClock::default();
        let at_clean_cap = TcpSendWindowSnapshot {
            can_send: true,
            may_send: true,
            may_recv: true,
            send_capacity: 100,
            send_queue: tx_queue_flush_threshold(cfg),
            recv_queue: 0,
        };

        let first = bounded_downlink_flush_limit_for_window_with_clock(
            100,
            100,
            at_clean_cap,
            cfg,
            &mut clock,
        );
        clock.note_flush_result(at_clean_cap.send_queue, 0, first);

        let after_drain = TcpSendWindowSnapshot {
            send_queue: 100,
            ..at_clean_cap
        };
        let second = bounded_downlink_flush_limit_for_window_with_clock(
            100,
            100,
            after_drain,
            cfg,
            &mut clock,
        );

        assert_eq!(
            after_drain.send_queue + second.len,
            tx_queue_credit_spend_threshold(cfg),
            "credit should leave a guard below hard pause"
        );
        assert_eq!(
            after_drain.send_queue + second.len,
            tx_queue_credit_spend_threshold(cfg),
            "planned send queue may reach the credit edge without active debt"
        );
    }

    #[test]
    fn downlink_credit_guard_never_reduces_clean_headroom() {
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 2,
            low_bytes: 1,
        };
        let mut clock = DownlinkEgressClock::default();
        let send_window = TcpSendWindowSnapshot {
            can_send: true,
            may_send: true,
            may_recv: true,
            send_capacity: 10,
            send_queue: 0,
            recv_queue: 0,
        };

        let limit = bounded_downlink_flush_limit_for_window_with_clock(
            10,
            10,
            send_window,
            cfg,
            &mut clock,
        );

        assert_eq!(
            tx_queue_credit_guard_bytes(cfg),
            0,
            "zero-width credit spans cannot reserve a guard below clean headroom"
        );
        assert_eq!(
            limit.len,
            tx_queue_flush_threshold(cfg),
            "clean headroom must remain usable even when credit span is tiny"
        );
        assert_eq!(limit.hard_edge_guard_deferred_bytes, 0);
    }

    #[test]
    fn downlink_egress_clock_consumes_only_accepted_credit() {
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 100,
            low_bytes: 40,
        };
        let mut clock = DownlinkEgressClock::default();
        let at_clean_cap = TcpSendWindowSnapshot {
            can_send: true,
            may_send: true,
            may_recv: true,
            send_capacity: 100,
            send_queue: tx_queue_flush_threshold(cfg),
            recv_queue: 0,
        };
        let first = bounded_downlink_flush_limit_for_window_with_clock(
            100,
            100,
            at_clean_cap,
            cfg,
            &mut clock,
        );
        clock.note_flush_result(at_clean_cap.send_queue, 0, first);

        let after_drain = TcpSendWindowSnapshot {
            send_queue: 100,
            ..at_clean_cap
        };
        let second = bounded_downlink_flush_limit_for_window_with_clock(
            100,
            100,
            after_drain,
            cfg,
            &mut clock,
        );
        assert_eq!(
            after_drain.send_queue + second.len,
            tx_queue_credit_spend_threshold(cfg)
        );
        assert_eq!(
            second.drain_credit_planned_bytes,
            tx_queue_credit_spend_threshold(cfg) - tx_queue_flush_threshold(cfg)
        );
        assert_eq!(
            clock.note_flush_result(after_drain.send_queue, 40, second),
            10,
            "only bytes accepted above clean headroom should consume credit"
        );

        let after_partial_accept = TcpSendWindowSnapshot {
            send_queue: 140,
            ..at_clean_cap
        };
        let third = bounded_downlink_flush_limit_for_window_with_clock(
            100,
            100,
            after_partial_accept,
            cfg,
            &mut clock,
        );
        assert_eq!(
            after_partial_accept.send_queue + third.len,
            tx_queue_credit_spend_threshold(cfg),
            "unused drain credit should remain available up to the credit edge"
        );
        assert_eq!(third.drain_credit_planned_bytes, third.len);
        assert_eq!(
            third.hard_edge_guard_deferred_bytes,
            tx_queue_pause_threshold(cfg) - tx_queue_credit_spend_threshold(cfg)
        );
    }

    #[test]
    fn downlink_drop_credit_debt_blocks_existing_credit_after_tun_drop() {
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 100,
            low_bytes: 40,
        };
        let mut clock = DownlinkEgressClock {
            last_send_queue: Some(tx_queue_flush_threshold(cfg)),
            drain_credit_bytes: 27,
            drop_debt_generation_seen: 0,
        };
        let mut drop_debt = DownlinkEgressDropDebt::default();
        assert_eq!(drop_debt.note_tun_drop(cfg), 30);

        let after_drop_drain = TcpSendWindowSnapshot {
            can_send: true,
            may_send: true,
            may_recv: true,
            send_capacity: 100,
            send_queue: 100,
            recv_queue: 0,
        };
        let limit = bounded_downlink_flush_limit_for_window_with_clock_and_drop(
            100,
            100,
            after_drop_drain,
            cfg,
            &mut clock,
            &mut drop_debt,
        );

        assert_eq!(
            limit.len, 30,
            "drop debt must leave clean headroom usable but block extra credit"
        );
        assert_eq!(limit.clean_headroom_bytes, 30);
        assert_eq!(limit.drain_credit_granted_bytes, 0);
        assert_eq!(limit.drain_credit_planned_bytes, 0);
        assert_eq!(limit.drop_credit_debt_paid_bytes, 30);
        assert_eq!(limit.drop_credit_debt_bytes, 0);
        assert_eq!(
            limit.drop_credit_blocked_bytes, 57,
            "stale credit plus drop-paid drain should be visible as blocked credit"
        );
    }

    #[test]
    fn downlink_drop_credit_debt_must_be_paid_before_new_credit() {
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 100,
            low_bytes: 40,
        };
        let mut clock = DownlinkEgressClock {
            last_send_queue: Some(tx_queue_pause_threshold(cfg)),
            drain_credit_bytes: 0,
            drop_debt_generation_seen: 0,
        };
        let mut drop_debt = DownlinkEgressDropDebt::default();
        drop_debt.note_tun_drop(cfg);

        let debt_payment_window = TcpSendWindowSnapshot {
            can_send: true,
            may_send: true,
            may_recv: true,
            send_capacity: 100,
            send_queue: tx_queue_flush_threshold(cfg),
            recv_queue: 0,
        };
        let debt_payment = bounded_downlink_flush_limit_for_window_with_clock_and_drop(
            100,
            100,
            debt_payment_window,
            cfg,
            &mut clock,
            &mut drop_debt,
        );
        assert_eq!(debt_payment.drop_credit_debt_paid_bytes, 30);
        assert_eq!(debt_payment.drain_credit_granted_bytes, 0);
        assert_eq!(debt_payment.len, 0);
        clock.note_flush_result(debt_payment_window.send_queue, 0, debt_payment);

        let clean_drain_window = TcpSendWindowSnapshot {
            send_queue: 100,
            ..debt_payment_window
        };
        let clean_drain = bounded_downlink_flush_limit_for_window_with_clock_and_drop(
            100,
            100,
            clean_drain_window,
            cfg,
            &mut clock,
            &mut drop_debt,
        );
        assert_eq!(
            clean_drain.drain_credit_granted_bytes, 30,
            "only drain observed after drop debt is paid can become credit"
        );
        assert_eq!(clean_drain.drop_credit_debt_paid_bytes, 0);
        assert_eq!(
            clean_drain.drain_credit_planned_bytes,
            tx_queue_credit_spend_threshold(cfg) - tx_queue_flush_threshold(cfg)
        );
        assert_eq!(
            clean_drain_window.send_queue + clean_drain.len,
            tx_queue_credit_spend_threshold(cfg)
        );
    }

    #[test]
    fn downlink_pressure_credit_debt_treats_single_target_edge_as_soft_pressure() {
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 100,
            low_bytes: 40,
        };
        let clean_edge = DownlinkPressureStats::new(
            0,
            0,
            tx_queue_flush_threshold(cfg),
            tx_queue_flush_threshold(cfg),
        );
        let credit_edge = DownlinkPressureStats::new(
            0,
            0,
            tx_queue_credit_spend_threshold(cfg),
            tx_queue_credit_spend_threshold(cfg),
        );
        let target_edge = DownlinkPressureStats::new(
            0,
            0,
            tx_queue_egress_target_threshold(cfg),
            tx_queue_egress_target_threshold(cfg),
        );
        let pending_with_low_tx_queue = DownlinkPressureStats::new(100, 100, 1, 1);
        let pending_with_flush_edge = DownlinkPressureStats::new(
            100,
            100,
            tx_queue_flush_threshold(cfg),
            tx_queue_flush_threshold(cfg),
        );

        assert!(!should_install_downlink_pressure_credit_debt(
            false, true, clean_edge, cfg
        ));
        assert!(
            !should_install_downlink_pressure_credit_debt(false, true, target_edge, cfg),
            "a single target-edge sample should not install pressure debt without stronger pressure evidence"
        );
        assert!(should_install_downlink_pressure_credit_debt(
            false,
            true,
            credit_edge,
            cfg
        ));
        assert!(!should_install_downlink_pressure_credit_debt(
            false,
            true,
            pending_with_low_tx_queue,
            cfg
        ));
        assert!(should_install_downlink_pressure_credit_debt(
            false,
            true,
            pending_with_flush_edge,
            cfg
        ));
        assert!(
            !should_install_downlink_pressure_credit_debt(true, true, credit_edge, cfg),
            "already-paused pressure should not keep installing new debt every loop"
        );
    }

    #[test]
    fn downlink_pressure_credit_debt_waits_for_credit_edge_after_target() {
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 100,
            low_bytes: 40,
        };
        let target_edge = DownlinkPressureStats::new(
            0,
            0,
            tx_queue_egress_target_threshold(cfg),
            tx_queue_egress_target_threshold(cfg),
        );

        assert!(
            !should_install_downlink_pressure_credit_debt(false, false, target_edge, cfg),
            "target-edge pressure is a soft warning; stronger pressure should install debt"
        );
    }

    #[test]
    fn post_flush_projected_pressure_debt_ignores_fully_accepted_transient_payload() {
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 100,
            low_bytes: 40,
        };
        let post_flush_send_queue = tx_queue_pause_threshold(cfg).saturating_add(1);

        assert_eq!(
            post_flush_projected_pressure_debt_bytes(post_flush_send_queue, 0, 64 * 1024, cfg),
            0,
            "payload bytes already accepted by smoltcp are real egress progress, not residual pending debt"
        );
    }

    #[test]
    fn post_flush_projected_pressure_debt_installs_for_residual_pending_pressure() {
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 100,
            low_bytes: 40,
        };
        let post_flush_send_queue = tx_queue_credit_spend_threshold(cfg);
        let pending_bytes = 17;
        let debt = post_flush_projected_pressure_debt_bytes(
            post_flush_send_queue,
            pending_bytes,
            64 * 1024,
            cfg,
        );

        assert!(
            debt > 0,
            "residual pending after a flush is still local pressure and must keep pressure debt protection"
        );
    }

    #[test]
    fn downlink_pressure_credit_debt_repeated_credit_edge_is_idempotent() {
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 100,
            low_bytes: 40,
        };
        let credit_edge = DownlinkPressureStats::new(
            0,
            0,
            tx_queue_credit_spend_threshold(cfg),
            tx_queue_credit_spend_threshold(cfg),
        );
        let mut credit_debt = DownlinkEgressCreditDebt::default();
        let expected_debt = downlink_egress_pressure_debt_bytes(credit_edge, cfg);

        assert_eq!(
            credit_debt.note_egress_pressure(credit_edge, cfg),
            expected_debt
        );
        assert_eq!(credit_debt.generation, 1);
        assert_eq!(credit_debt.pressure_events, 1);
        assert_eq!(credit_debt.note_egress_pressure(credit_edge, cfg), 0);
        assert_eq!(
            credit_debt.generation, 1,
            "same-sized pressure debt should not create a new generation"
        );
        assert_eq!(
            credit_debt.pressure_events, 1,
            "diagnostic pressure events count debt installs, not repeated observations"
        );
    }

    #[test]
    fn downlink_pressure_credit_debt_sizes_from_guard_and_overshoot() {
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 100,
            low_bytes: 40,
        };
        assert_eq!(downlink_egress_credit_span(cfg), 30);
        assert_eq!(tx_queue_credit_guard_bytes(cfg), 3);
        assert_eq!(tx_queue_egress_target_threshold(cfg), 143);
        assert_eq!(tx_queue_credit_spend_threshold(cfg), 157);

        assert_eq!(
            downlink_egress_pressure_debt_bytes(
                DownlinkPressureStats::new(
                    0,
                    0,
                    tx_queue_egress_target_threshold(cfg) - 1,
                    tx_queue_egress_target_threshold(cfg) - 1,
                ),
                cfg,
            ),
            0,
            "below the egress target should not install pressure debt"
        );
        assert_eq!(
            downlink_egress_pressure_debt_bytes(
                DownlinkPressureStats::new(
                    0,
                    0,
                    tx_queue_egress_target_threshold(cfg),
                    tx_queue_egress_target_threshold(cfg),
                ),
                cfg,
            ),
            0,
            "the egress target is a soft warning and does not install pressure debt"
        );
        assert_eq!(
            downlink_egress_pressure_debt_bytes(
                DownlinkPressureStats::new(
                    0,
                    0,
                    tx_queue_credit_spend_threshold(cfg),
                    tx_queue_credit_spend_threshold(cfg),
                ),
                cfg,
            ),
            17,
            "the old credit edge now counts as target overshoot"
        );
        assert_eq!(
            downlink_egress_pressure_debt_bytes(
                DownlinkPressureStats::new(
                    0,
                    0,
                    tx_queue_pause_threshold(cfg) + 5,
                    tx_queue_pause_threshold(cfg) + 5,
                ),
                cfg,
            ),
            25,
            "pressure overshoot should scale debt above the guard"
        );
        assert_eq!(
            downlink_egress_pressure_debt_bytes(
                DownlinkPressureStats::new(
                    0,
                    0,
                    tx_queue_pause_threshold(cfg) + 50,
                    tx_queue_pause_threshold(cfg) + 50,
                ),
                cfg,
            ),
            30,
            "pressure debt remains bounded by the original full credit span"
        );
        assert_eq!(
            downlink_egress_pressure_debt_bytes(
                DownlinkPressureStats::new(
                    110,
                    110,
                    tx_queue_flush_threshold(cfg),
                    tx_queue_flush_threshold(cfg),
                ),
                cfg,
            ),
            13,
            "pending pressure at the flush edge should also scale the early debt"
        );
    }

    #[test]
    fn projected_pressure_debt_sizes_from_combined_send_queue_and_pending() {
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 100,
            low_bytes: 40,
        };

        assert_eq!(
            downlink_egress_projected_pressure_debt_bytes(
                tx_queue_egress_target_threshold(cfg) - 1,
                cfg,
            ),
            0,
            "projected pressure below the egress target should not install debt"
        );
        assert_eq!(
            downlink_egress_projected_pressure_debt_bytes(
                tx_queue_egress_target_threshold(cfg),
                cfg,
            ),
            0,
            "the projected target edge is a soft warning and does not install pressure debt"
        );
        assert_eq!(
            downlink_egress_projected_pressure_debt_bytes(
                tx_queue_credit_spend_threshold(cfg),
                cfg,
            ),
            tx_queue_credit_guard_bytes(cfg).saturating_add(
                tx_queue_credit_spend_threshold(cfg)
                    .saturating_sub(tx_queue_egress_target_threshold(cfg))
            ),
            "the projected credit edge installs bounded pressure debt"
        );
        assert_eq!(
            downlink_egress_projected_pressure_debt_bytes(
                tx_queue_pause_threshold(cfg).saturating_add(50),
                cfg,
            ),
            downlink_egress_credit_span(cfg),
            "projected pressure debt remains bounded by the credit span"
        );
    }

    #[test]
    fn projected_payload_target_edge_does_not_block_stale_drain_credit_before_flush() {
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 100,
            low_bytes: 40,
        };
        let mut clock = DownlinkEgressClock {
            last_send_queue: Some(tx_queue_flush_threshold(cfg)),
            drain_credit_bytes: downlink_egress_credit_span(cfg),
            drop_debt_generation_seen: 0,
        };
        let mut credit_debt = DownlinkEgressCreditDebt::default();
        let send_window = TcpSendWindowSnapshot {
            can_send: true,
            may_send: true,
            may_recv: true,
            send_capacity: 100,
            send_queue: tx_queue_flush_threshold(cfg) - 1,
            recv_queue: 0,
        };
        let pending_after_payload =
            tx_queue_egress_target_threshold(cfg).saturating_sub(send_window.send_queue);
        let projected_pressure =
            downlink_projected_local_pressure_bytes(send_window.send_queue, pending_after_payload);

        assert_eq!(
            credit_debt.note_projected_egress_pressure(projected_pressure, cfg),
            0
        );
        let limit = bounded_downlink_flush_limit_for_window_with_clock_and_drop(
            pending_after_payload,
            100,
            send_window,
            cfg,
            &mut clock,
            &mut credit_debt,
        );

        assert_eq!(
            limit.len,
            tx_queue_egress_target_threshold(cfg).saturating_sub(send_window.send_queue),
            "target-edge projected pressure may still use drain credit up to the current hard target"
        );
        assert_eq!(limit.drain_credit_granted_bytes, 1);
        assert_eq!(
            limit.drain_credit_planned_bytes,
            tx_queue_egress_target_threshold(cfg).saturating_sub(tx_queue_flush_threshold(cfg))
        );
        assert_eq!(limit.pressure_credit_debt_bytes, 0);
    }

    #[test]
    fn downlink_pressure_credit_debt_preserves_healthy_credit_before_tun_drop() {
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 100,
            low_bytes: 40,
        };
        let mut clock = DownlinkEgressClock {
            last_send_queue: Some(tx_queue_flush_threshold(cfg)),
            drain_credit_bytes: 27,
            drop_debt_generation_seen: 0,
        };
        let mut credit_debt = DownlinkEgressCreditDebt::default();
        assert_eq!(
            credit_debt.note_egress_pressure(
                DownlinkPressureStats::new(
                    0,
                    0,
                    tx_queue_credit_spend_threshold(cfg),
                    tx_queue_credit_spend_threshold(cfg),
                ),
                cfg
            ),
            17
        );

        let after_pressure_drain = TcpSendWindowSnapshot {
            can_send: true,
            may_send: true,
            may_recv: true,
            send_capacity: 100,
            send_queue: 100,
            recv_queue: 0,
        };
        let limit = bounded_downlink_flush_limit_for_window_with_clock_and_drop(
            100,
            100,
            after_pressure_drain,
            cfg,
            &mut clock,
            &mut credit_debt,
        );

        assert_eq!(limit.len, 57);
        assert_eq!(limit.clean_headroom_bytes, 30);
        assert_eq!(limit.drain_credit_granted_bytes, 0);
        assert_eq!(limit.drain_credit_planned_bytes, 27);
        assert_eq!(limit.pressure_credit_debt_paid_bytes, 17);
        assert_eq!(limit.pressure_credit_debt_bytes, 0);
        assert_eq!(
            limit.pressure_credit_blocked_bytes, 17,
            "the drain that pays pressure debt is still attributed, but existing healthy credit remains usable before an actual TUN drop"
        );
        assert_eq!(limit.drop_credit_blocked_bytes, 0);
    }

    #[test]
    fn downlink_pressure_debt_keeps_healthy_drain_credit_usable() {
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 100,
            low_bytes: 40,
        };
        let mut pressure_clock = DownlinkEgressClock {
            last_send_queue: Some(tx_queue_flush_threshold(cfg)),
            drain_credit_bytes: downlink_egress_credit_span(cfg),
            drop_debt_generation_seen: 0,
        };
        let mut pressure_debt = DownlinkEgressCreditDebt::default();
        pressure_debt.note_egress_pressure(
            DownlinkPressureStats::new(
                0,
                0,
                tx_queue_credit_spend_threshold(cfg),
                tx_queue_credit_spend_threshold(cfg),
            ),
            cfg,
        );
        let pressure_window = TcpSendWindowSnapshot {
            can_send: true,
            may_send: true,
            may_recv: true,
            send_capacity: 100,
            send_queue: tx_queue_egress_target_threshold(cfg) + 2,
            recv_queue: 0,
        };
        let pressure_limit = bounded_downlink_flush_limit_for_window_with_clock_and_drop(
            100,
            100,
            pressure_window,
            cfg,
            &mut pressure_clock,
            &mut pressure_debt,
        );

        assert_eq!(
            pressure_limit.len,
            tx_queue_credit_spend_threshold(cfg) - pressure_window.send_queue,
            "soft pressure debt must not turn healthy drain credit into a zero-write freeze"
        );
        assert_eq!(
            pressure_limit.drain_credit_planned_bytes,
            pressure_limit.len
        );
        assert_eq!(pressure_limit.drop_credit_blocked_bytes, 0);

        let mut drop_clock = DownlinkEgressClock {
            last_send_queue: Some(tx_queue_flush_threshold(cfg)),
            drain_credit_bytes: downlink_egress_credit_span(cfg),
            drop_debt_generation_seen: 0,
        };
        let mut drop_debt = DownlinkEgressCreditDebt::default();
        drop_debt.note_tun_drop(cfg);
        let drop_limit = bounded_downlink_flush_limit_for_window_with_clock_and_drop(
            100,
            100,
            pressure_window,
            cfg,
            &mut drop_clock,
            &mut drop_debt,
        );

        assert_eq!(
            drop_limit.len, 0,
            "real drop debt still blocks credit once the queue is already beyond the target edge"
        );
        assert_eq!(
            drop_limit.drop_credit_blocked_bytes,
            downlink_egress_credit_span(cfg)
        );
    }

    #[test]
    fn pressure_debt_payment_with_progress_does_not_train_blocking_feedback() {
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 100,
            low_bytes: 40,
        };
        let tun_mtu = 1200;
        let pressure_floor = relay_remote_read_pressure_floor_bytes(tun_mtu);
        let initial_ceiling = pressure_floor * 4;
        let initial_boost = pressure_floor;
        let mut controller = DownlinkCreditController {
            read_credit_ceiling_bytes: initial_ceiling,
            adaptive_ack_drain_bytes: initial_boost,
            ack_cadence_boost_bytes: initial_boost,
            egress_payload_credit_bytes: 0,
            egress_progress_generation: 0,
            headroom_debt_bytes: 0,
            no_egress_progress_streak: 0,
        };

        controller.note_flush_feedback(
            DownlinkFlushLimit {
                len: 30,
                headroom_limited: false,
                headroom_deferred_bytes: 0,
                clean_headroom_bytes: 30,
                drain_credit_granted_bytes: 0,
                drain_credit_planned_bytes: 0,
                drop_credit_debt_bytes: 0,
                drop_credit_debt_paid_bytes: 0,
                drop_credit_blocked_bytes: 0,
                pressure_credit_debt_bytes: 0,
                pressure_credit_debt_paid_bytes: 17,
                pressure_credit_blocked_bytes: 17,
                hard_edge_guard_bytes: 0,
                hard_edge_guard_deferred_bytes: 0,
            },
            cfg,
            tun_mtu,
        );

        assert_eq!(controller.egress_progress_generation, 1);
        assert_eq!(
            controller.headroom_debt_bytes, 0,
            "soft pressure debt that was paid by observed egress progress is not deferred backlog"
        );
        assert!(
            controller.read_credit_ceiling_bytes >= initial_ceiling,
            "progress must not immediately train the read-credit ceiling downward"
        );
        assert_eq!(
            controller.ack_cadence_boost_bytes, initial_boost,
            "a paid soft-pressure sample must not clear the cadence boost as if it were a hard block"
        );
        assert_eq!(controller.no_egress_progress_streak, 0);
    }

    #[test]
    fn hard_edge_guard_with_progress_does_not_train_stalled_read_credit() {
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 100,
            low_bytes: 40,
        };
        let tun_mtu = 1200;
        let pressure_floor = relay_remote_read_pressure_floor_bytes(tun_mtu);
        let initial_ceiling = pressure_floor * 4;
        let initial_boost = pressure_floor;
        let mut controller = DownlinkCreditController {
            read_credit_ceiling_bytes: initial_ceiling,
            adaptive_ack_drain_bytes: initial_boost,
            ack_cadence_boost_bytes: initial_boost,
            egress_payload_credit_bytes: 0,
            egress_progress_generation: 0,
            headroom_debt_bytes: 0,
            no_egress_progress_streak: 0,
        };

        controller.note_flush_feedback(
            DownlinkFlushLimit {
                len: pressure_floor,
                headroom_limited: true,
                headroom_deferred_bytes: 0,
                clean_headroom_bytes: 0,
                drain_credit_granted_bytes: pressure_floor,
                drain_credit_planned_bytes: pressure_floor,
                drop_credit_debt_bytes: 0,
                drop_credit_debt_paid_bytes: 0,
                drop_credit_blocked_bytes: 0,
                pressure_credit_debt_bytes: 0,
                pressure_credit_debt_paid_bytes: 0,
                pressure_credit_blocked_bytes: 0,
                hard_edge_guard_bytes: 3,
                hard_edge_guard_deferred_bytes: pressure_floor,
            },
            cfg,
            tun_mtu,
        );

        assert_eq!(controller.egress_progress_generation, 1);
        assert_eq!(
            controller.headroom_debt_bytes, 0,
            "a bounded guard deferral with observed drain is not a stalled-egress backlog"
        );
        assert!(
            controller.read_credit_ceiling_bytes >= initial_ceiling,
            "the guard should keep local safety without training the TUIC read side downward while egress drains"
        );
        assert_eq!(controller.ack_cadence_boost_bytes, initial_boost);
        assert_eq!(controller.no_egress_progress_streak, 0);
    }

    #[test]
    fn downlink_credit_debt_overpay_requires_later_clean_drain_for_new_credit() {
        let cfg = DownlinkBackpressureConfig {
            high_bytes: 100,
            low_bytes: 40,
        };
        let mut clock = DownlinkEgressClock {
            last_send_queue: Some(tx_queue_pause_threshold(cfg)),
            drain_credit_bytes: 0,
            drop_debt_generation_seen: 0,
        };
        let mut credit_debt = DownlinkEgressCreditDebt::default();
        credit_debt.note_egress_pressure(
            DownlinkPressureStats::new(
                0,
                0,
                tx_queue_credit_spend_threshold(cfg),
                tx_queue_credit_spend_threshold(cfg),
            ),
            cfg,
        );

        let large_drain_window = TcpSendWindowSnapshot {
            can_send: true,
            may_send: true,
            may_recv: true,
            send_capacity: 100,
            send_queue: 100,
            recv_queue: 0,
        };
        let debt_payment = bounded_downlink_flush_limit_for_window_with_clock_and_drop(
            100,
            100,
            large_drain_window,
            cfg,
            &mut clock,
            &mut credit_debt,
        );
        assert_eq!(debt_payment.pressure_credit_debt_paid_bytes, 17);
        assert_eq!(debt_payment.drain_credit_granted_bytes, 0);
        assert_eq!(
            debt_payment.len, 30,
            "the same observed drain that pays debt may use clean headroom only"
        );
        clock.note_flush_result(large_drain_window.send_queue, 30, debt_payment);

        let later_clean_drain = TcpSendWindowSnapshot {
            send_queue: 100,
            ..large_drain_window
        };
        let clean_credit = bounded_downlink_flush_limit_for_window_with_clock_and_drop(
            100,
            100,
            later_clean_drain,
            cfg,
            &mut clock,
            &mut credit_debt,
        );
        assert_eq!(clean_credit.drain_credit_granted_bytes, 30);
        assert_eq!(
            clean_credit.drain_credit_planned_bytes,
            tx_queue_credit_spend_threshold(cfg) - tx_queue_flush_threshold(cfg)
        );
        assert_eq!(
            later_clean_drain.send_queue + clean_credit.len,
            tx_queue_credit_spend_threshold(cfg)
        );
    }

    #[test]
    fn parse_downlink_backpressure_config_defaults_and_rejects_bad_low() {
        let default = DownlinkBackpressureConfig::default();
        assert_eq!(
            parse_downlink_backpressure_config(None, None),
            default,
            "missing env uses defaults"
        );
        assert_eq!(
            parse_downlink_backpressure_config(Some("abc"), Some("0")),
            default,
            "invalid or zero values fall back to defaults"
        );

        let custom = parse_downlink_backpressure_config(Some("4096"), Some("1024"));
        assert_eq!(custom.high_bytes, 4096);
        assert_eq!(custom.low_bytes, 1024);

        let repaired = parse_downlink_backpressure_config(Some("4194304"), Some("4194304"));
        assert_eq!(repaired.high_bytes, 4194304);
        assert_eq!(repaired.low_bytes, default.low_bytes);
    }

    #[test]
    fn parse_downlink_backpressure_config_caps_auto_defaults_to_tun_egress() {
        let auto = parse_downlink_backpressure_config_for_tx_buffer(
            None,
            None,
            1_048_576,
            1_200,
            DEFAULT_TUN_TX_QUEUE_LEN_ESTIMATE,
        );
        assert_eq!(
            auto,
            default_downlink_backpressure_for_tun_egress(1_200, DEFAULT_TUN_TX_QUEUE_LEN_ESTIMATE)
        );
        assert!(
            tx_queue_flush_threshold(auto)
                <= tun_egress_safe_flush_threshold(1_200, DEFAULT_TUN_TX_QUEUE_LEN_ESTIMATE)
        );
        assert!(
            tx_queue_pause_threshold(auto)
                <= tun_egress_capacity_bytes(1_200, DEFAULT_TUN_TX_QUEUE_LEN_ESTIMATE)
        );

        let legacy = parse_downlink_backpressure_config_for_tx_buffer(
            None,
            None,
            65_535,
            1_200,
            DEFAULT_TUN_TX_QUEUE_LEN_ESTIMATE,
        );
        assert_eq!(
            legacy, auto,
            "TCP tx buffer size must not drive auto defaults"
        );

        let explicit = parse_downlink_backpressure_config_for_tx_buffer(
            Some("4096"),
            Some("1024"),
            1_048_576,
            1_200,
            DEFAULT_TUN_TX_QUEUE_LEN_ESTIMATE,
        );
        assert_eq!(explicit.high_bytes, 4096);
        assert_eq!(explicit.low_bytes, 1024);
    }

    #[test]
    fn parse_tcp_socket_buffer_config_defaults_and_bounds() {
        let default = TcpSocketBufferConfig::default();
        assert_eq!(parse_tcp_socket_buffer_config(None, None), default);
        assert_eq!(
            parse_tcp_socket_buffer_config(Some("abc"), Some("0")),
            default,
            "invalid and below-minimum values fall back to defaults"
        );

        let custom = parse_tcp_socket_buffer_config(Some("1048576"), Some("2097152"));
        assert_eq!(custom.rx_bytes, 1_048_576);
        assert_eq!(custom.tx_bytes, 2_097_152);

        let above_max = parse_tcp_socket_buffer_config(Some("33554432"), Some("33554432"));
        assert_eq!(
            above_max, default,
            "oversized socket buffers must not be accepted silently"
        );
    }

    #[test]
    fn parse_downlink_flush_max_bytes_defaults_and_bounds() {
        assert_eq!(
            parse_downlink_flush_max_bytes(None),
            DEFAULT_DOWNLINK_FLUSH_MAX_BYTES
        );
        assert_eq!(
            parse_downlink_flush_max_bytes(Some("abc")),
            DEFAULT_DOWNLINK_FLUSH_MAX_BYTES
        );
        assert_eq!(
            parse_downlink_flush_max_bytes(Some("0")),
            DEFAULT_DOWNLINK_FLUSH_MAX_BYTES
        );
        assert_eq!(
            parse_downlink_flush_max_bytes(Some("1024")),
            DEFAULT_DOWNLINK_FLUSH_MAX_BYTES,
            "below-minimum values should not silently underfeed the local path"
        );
        assert_eq!(parse_downlink_flush_max_bytes(Some("1048576")), 1_048_576);
        assert_eq!(
            parse_downlink_flush_max_bytes(Some("33554432")),
            DEFAULT_DOWNLINK_FLUSH_MAX_BYTES,
            "oversized values should not disable the burst bound by accident"
        );
    }

    #[test]
    fn parse_downlink_egress_immediate_bytes_defaults_and_bounds() {
        assert_eq!(
            DEFAULT_DOWNLINK_EGRESS_IMMEDIATE_BYTES, MAX_DOWNLINK_EGRESS_IMMEDIATE_BYTES,
            "Knife14ar default must preserve old immediate-flush behavior; small budgets are explicit A/B only"
        );
        assert_eq!(
            parse_downlink_egress_immediate_bytes(None),
            DEFAULT_DOWNLINK_EGRESS_IMMEDIATE_BYTES
        );
        assert_eq!(
            parse_downlink_egress_immediate_bytes(Some("abc")),
            DEFAULT_DOWNLINK_EGRESS_IMMEDIATE_BYTES
        );
        assert_eq!(
            parse_downlink_egress_immediate_bytes(Some("0")),
            0,
            "zero explicitly disables immediate remote-payload TUN flushes"
        );
        assert_eq!(parse_downlink_egress_immediate_bytes(Some("65536")), 65_536);
        assert_eq!(
            parse_downlink_egress_immediate_bytes(Some("33554432")),
            DEFAULT_DOWNLINK_EGRESS_IMMEDIATE_BYTES,
            "oversized values should not silently remove pacing"
        );
    }

    #[test]
    fn listener_registry_uses_configured_socket_buffers() {
        let buffers = TcpSocketBufferConfig {
            rx_bytes: 8192,
            tx_bytes: 16384,
        };
        let mut registry = ListenerRegistry::with_socket_buffers(1, buffers);
        let mut sockets = SocketSet::new(vec![]);
        let mut ctxs = HashMap::new();

        registry
            .ensure_port(443, &mut sockets, &mut ctxs)
            .expect("port should be registered");
        let handle = registry.handles_for_port(443)[0];
        let socket = sockets.get::<TcpSocket>(handle);

        assert_eq!(socket.recv_capacity(), buffers.rx_bytes);
        assert_eq!(socket.send_capacity(), buffers.tx_bytes);
    }

    #[test]
    fn relay_task_diag_tracks_bytes_and_global_rx_pressure() {
        let mut diag = RelayTaskDiag::default();
        diag.note_uplink_write(40);
        diag.note_remote_read(100);
        diag.note_local_finish();
        diag.note_remote_read(28);
        diag.note_remote_batch(2, 128);
        diag.note_global_rx_wait(
            std::time::Duration::from_micros(25),
            std::time::Duration::from_micros(10),
        );
        diag.note_global_rx_wait(
            std::time::Duration::from_micros(5),
            std::time::Duration::from_micros(10),
        );
        diag.note_global_rx_queue(7, 1024);
        diag.note_global_rx_queue(3, 1024);
        diag.note_local_write_wait(
            std::time::Duration::from_micros(30),
            std::time::Duration::from_micros(10),
        );
        diag.note_local_write_wait(
            std::time::Duration::from_micros(7),
            std::time::Duration::from_micros(10),
        );

        assert_eq!(diag.uplink_bytes, 40);
        assert_eq!(diag.remote_to_global_rx_bytes, 128);
        assert_eq!(diag.remote_reads, 2);
        assert_eq!(diag.remote_batches, 1);
        assert_eq!(diag.remote_batch_bytes_max, 128);
        assert_eq!(diag.remote_batch_chunks_max, 2);
        assert_eq!(diag.remote_after_local_finish_bytes, 28);
        assert_eq!(diag.remote_after_local_finish_reads, 1);
        assert_eq!(diag.global_rx_wait_max_micros, 25);
        assert_eq!(diag.global_rx_pressure_events, 1);
        assert_eq!(diag.global_rx_queue_used_max, 7);
        assert_eq!(diag.global_rx_queue_capacity, 1024);
        assert_eq!(diag.local_write_wait_max_micros, 30);
        assert_eq!(diag.local_write_pressure_events, 1);
    }

    #[test]
    fn relay_live_diag_line_includes_directional_counters() {
        let mut sockets = SocketSet::new(vec![]);
        let handle = sockets.add(TcpSocket::new(
            TcpSocketBuffer::new(vec![0; 16]),
            TcpSocketBuffer::new(vec![0; 16]),
        ));
        let mut diag = RelayTaskDiag::default();
        diag.note_uplink_write(64);
        diag.note_local_finish();
        diag.note_remote_read(128);
        diag.note_remote_read_service_tick(65_536);
        diag.note_remote_batch(1, 128);
        diag.note_global_rx_queue(5, 1024);
        diag.note_ack_drain_hint_due();
        diag.note_ack_drain_hint_sent();

        let line = format_relay_live_diag(handle, 7, true, true, &diag);

        assert!(line.contains("tcp-relay-live"), "{line}");
        assert!(line.contains("epoch=7"), "{line}");
        assert!(line.contains("writer_done=true"), "{line}");
        assert!(line.contains("read_only_after_local_finish=true"), "{line}");
        assert!(line.contains("uplink_bytes=64"), "{line}");
        assert!(line.contains("remote_to_global_rx_bytes=128"), "{line}");
        assert!(line.contains("remote_reads=1"), "{line}");
        assert!(line.contains("remote_read_service_ticks=1"), "{line}");
        assert!(line.contains("remote_read_service_len_min=65536"), "{line}");
        assert!(line.contains("remote_read_service_len_max=65536"), "{line}");
        assert!(line.contains("remote_batches=1"), "{line}");
        assert!(line.contains("remote_batch_bytes_max=128"), "{line}");
        assert!(line.contains("remote_batch_chunks_max=1"), "{line}");
        assert!(
            line.contains("remote_after_local_finish_bytes=128"),
            "{line}"
        );
        assert!(line.contains("remote_after_local_finish_reads=1"), "{line}");
        assert!(line.contains("ack_drain_hint_due=1"), "{line}");
        assert!(line.contains("ack_drain_hint_sent=1"), "{line}");
        assert!(line.contains("ack_drain_hint_dropped=0"), "{line}");
        assert!(line.contains("global_rx_queue_used_max=5"), "{line}");
        assert!(line.contains("global_rx_queue_capacity=1024"), "{line}");
    }

    #[test]
    fn relay_read_service_diag_records_awaited_read_window_range() {
        let mut diag = RelayTaskDiag::default();

        diag.note_remote_read_service_tick(65_536);
        diag.note_remote_read_service_tick(0);
        diag.note_remote_read_service_tick(8_192);
        diag.note_remote_read_service_tick(131_072);

        assert_eq!(diag.remote_read_service_ticks, 3);
        assert_eq!(diag.remote_read_service_len_min, 8_192);
        assert_eq!(diag.remote_read_service_len_max, 131_072);
    }

    #[test]
    fn relay_live_diag_line_includes_remote_read_timing() {
        let mut sockets = SocketSet::new(vec![]);
        let handle = sockets.add(TcpSocket::new(
            TcpSocketBuffer::new(vec![0; 16]),
            TcpSocketBuffer::new(vec![0; 16]),
        ));
        let start = std::time::Instant::now();
        let mut diag = RelayTaskDiag::new(start);
        diag.note_remote_read_at(100, start + std::time::Duration::from_millis(750));
        diag.note_remote_read_at(50, start + std::time::Duration::from_millis(2_250));

        let line = format_relay_live_diag_at(
            handle,
            7,
            false,
            false,
            &diag,
            start + std::time::Duration::from_millis(3_500),
        );

        assert!(line.contains("first_remote_read_ms=750"), "{line}");
        assert!(line.contains("max_remote_read_gap_ms=1500"), "{line}");
        assert!(line.contains("current_remote_read_gap_ms=1250"), "{line}");
    }

    #[test]
    fn relay_live_diag_splits_remote_read_gaps_around_local_finish() {
        let mut sockets = SocketSet::new(vec![]);
        let handle = sockets.add(TcpSocket::new(
            TcpSocketBuffer::new(vec![0; 16]),
            TcpSocketBuffer::new(vec![0; 16]),
        ));
        let start = std::time::Instant::now();
        let mut diag = RelayTaskDiag::new(start);
        diag.note_remote_read_at(10, start + std::time::Duration::from_millis(100));
        diag.note_remote_read_at(20, start + std::time::Duration::from_millis(900));
        diag.note_local_finish_at(start + std::time::Duration::from_millis(1_200));
        diag.note_remote_read_at(30, start + std::time::Duration::from_millis(2_700));
        diag.note_remote_read_at(40, start + std::time::Duration::from_millis(3_200));

        let line = format_relay_live_diag_at(
            handle,
            9,
            true,
            true,
            &diag,
            start + std::time::Duration::from_millis(3_700),
        );

        assert_eq!(diag.local_finish_events, 1);
        assert_eq!(
            diag.first_local_finish_after_first_remote_read_millis(),
            1_100
        );
        assert_eq!(diag.max_remote_read_gap_millis, 1_800);
        assert_eq!(diag.max_remote_read_gap_before_local_finish_millis, 800);
        assert_eq!(diag.max_remote_read_gap_after_local_finish_millis, 1_800);
        assert_eq!(diag.remote_after_local_finish_bytes, 70);
        assert_eq!(diag.remote_after_local_finish_reads, 2);
        assert!(line.contains("local_finish_events=1"), "{line}");
        assert!(
            line.contains("first_local_finish_after_first_remote_read_ms=1100"),
            "{line}"
        );
        assert!(
            line.contains("max_remote_read_gap_before_local_finish_ms=800"),
            "{line}"
        );
        assert!(
            line.contains("max_remote_read_gap_after_local_finish_ms=1800"),
            "{line}"
        );
    }

    #[test]
    fn relay_ack_drain_hint_services_low_byte_ordered_stream_gap() {
        let start = std::time::Instant::now();
        let mut diag = RelayTaskDiag::new(start);
        let low_byte_progress = 60_704usize;

        assert!(
            (low_byte_progress as u64) < RELAY_ACK_DRAIN_HINT_MIN_DATA_BYTES,
            "fixture must stay below the historical 64KiB ACK/window service gate"
        );

        diag.note_remote_read_at(low_byte_progress, start);
        diag.note_remote_batch(5, low_byte_progress);
        diag.note_remote_read_service_tick(RELAY_REMOTE_READ_BURST_MAX_BYTES);

        let due = relay_ack_drain_hint_due(
            &diag,
            start + std::time::Duration::from_millis(RELAY_ACK_DRAIN_HINT_GAP_MS as u64 + 50),
            None,
        )
        .expect("active ordered-stream gaps below 64KiB should still request ACK/window service");

        assert_eq!(due.gap_ms, RELAY_ACK_DRAIN_HINT_GAP_MS + 50);
        assert_eq!(due.remote_to_global_rx_bytes, low_byte_progress as u64);
    }

    #[test]
    fn relay_ack_drain_hint_polling_includes_low_byte_active_read_service() {
        let start = std::time::Instant::now();
        let mut diag = RelayTaskDiag::new(start);
        diag.note_remote_read_at(60_704, start);
        diag.note_remote_batch(5, 60_704);
        diag.note_remote_read_service_tick(RELAY_REMOTE_READ_BURST_MAX_BYTES);

        assert!(
            should_poll_relay_remote_read_probe(&diag, RelayReadCredit::default()),
            "payload-shaped active relays should keep the remote read probe armed"
        );
        assert!(
            should_poll_relay_ack_drain_hint(&diag, false),
            "low-byte active ordered stream gaps must reach the ACK/window hint branch"
        );
    }

    #[test]
    fn relay_ack_drain_hint_only_fires_for_rate_limited_remote_read_gap() {
        let start = std::time::Instant::now();
        let mut diag = RelayTaskDiag::new(start);
        let gap_due = start + std::time::Duration::from_millis(150);
        let mut last_hint_at = None;

        assert_eq!(
            relay_ack_drain_hint_due(&diag, gap_due, last_hint_at),
            None,
            "a relay with no remote data should not request ACK drain"
        );
        assert!(
            !should_poll_relay_remote_read_probe(&diag, RelayReadCredit::default()),
            "idle relays must not wake periodically before the first remote payload"
        );

        diag.note_remote_read_at(512, start);
        diag.note_remote_batch(1, 512);
        assert!(
            !should_poll_relay_remote_read_probe(&diag, RelayReadCredit::default()),
            "tiny control streams must not wake periodic remote-read probing"
        );
        assert!(
            !should_poll_relay_ack_drain_hint(&diag, false),
            "tiny control streams must not wake periodic ACK-drain hint polling"
        );
        assert_eq!(
            relay_ack_drain_hint_due(&diag, gap_due, last_hint_at),
            None,
            "tiny control-stream gaps should not scan TUN RX"
        );

        diag.note_remote_read_at(1412, start);
        diag.note_remote_batch(1, 1412);
        assert!(
            should_poll_relay_remote_read_probe(&diag, RelayReadCredit::default()),
            "payload-shaped streams should keep the remote read future polled before ACK hints are due"
        );
        assert!(
            should_poll_relay_remote_read_probe(
                &diag,
                RelayReadCredit {
                    paused: false,
                    max_batch_bytes: 1
                }
            ),
            "remote-read probing should respect the same non-paused credit gate as awaited reads"
        );
        assert!(
            !should_poll_relay_remote_read_probe(
                &diag,
                RelayReadCredit {
                    paused: true,
                    max_batch_bytes: RELAY_REMOTE_READ_BURST_MAX_BYTES
                }
            ),
            "paused receive credit must suppress probe wakeups"
        );
        assert!(
            !should_poll_relay_remote_read_probe(
                &diag,
                RelayReadCredit {
                    paused: false,
                    max_batch_bytes: 0
                }
            ),
            "zero receive credit must suppress probe wakeups"
        );
        assert!(
            !should_poll_relay_ack_drain_hint(&diag, false),
            "early payload-shaped streams should probe reads without emitting ACK-drain hints"
        );

        let remaining =
            (RELAY_ACK_DRAIN_HINT_MIN_DATA_BYTES - diag.remote_to_global_rx_bytes) as usize;
        diag.note_remote_read_at(remaining, start);
        diag.note_remote_batch(1, remaining);
        assert!(
            should_poll_relay_ack_drain_hint(&diag, false),
            "data-bearing relays may poll read-gap hints"
        );
        assert!(
            should_poll_relay_ack_drain_hint(&diag, true),
            "half-closed reverse streams still need ACK/window maintenance until remote EOF"
        );
        assert_eq!(
            relay_ack_drain_hint_due(
                &diag,
                start + std::time::Duration::from_millis(99),
                last_hint_at
            ),
            None,
            "remote read gaps below the short ACK/window service threshold should not scan TUN RX"
        );

        let due = relay_ack_drain_hint_due(&diag, gap_due, last_hint_at)
            .expect("short relay read gaps should request one ACK/window drain");
        assert_eq!(due.gap_ms, 150);
        assert_eq!(
            due.remote_to_global_rx_bytes,
            RELAY_ACK_DRAIN_HINT_MIN_DATA_BYTES
        );
        last_hint_at = Some(gap_due);

        assert_eq!(
            relay_ack_drain_hint_due(
                &diag,
                gap_due + std::time::Duration::from_millis(99),
                last_hint_at
            ),
            None,
            "hint emission must be rate-limited while the same stream remains stalled"
        );
        assert!(
            relay_ack_drain_hint_due(
                &diag,
                gap_due + std::time::Duration::from_millis(100),
                last_hint_at
            )
            .is_some(),
            "once the rate limit expires, a still-stalled stream may request another bounded drain"
        );
    }

    #[test]
    fn relay_close_diag_line_includes_post_finish_remote_counters() {
        let mut sockets = SocketSet::new(vec![]);
        let handle = sockets.add(TcpSocket::new(
            TcpSocketBuffer::new(vec![0; 16]),
            TcpSocketBuffer::new(vec![0; 16]),
        ));
        let mut diag = RelayTaskDiag::default();
        diag.note_uplink_write(37);
        diag.note_local_finish();
        diag.note_remote_read(43_772);
        diag.note_remote_read_service_tick(16_384);
        diag.note_remote_batch(3, 43_772);
        diag.note_global_rx_queue(9, 1024);

        let line = format_relay_close_diag(handle, "timer", "half_closed_idle_timeout", &diag);

        assert!(line.contains("tcp-relay-close"), "{line}");
        assert!(line.contains("direction=timer"), "{line}");
        assert!(line.contains("reason=half_closed_idle_timeout"), "{line}");
        assert!(
            line.contains("remote_after_local_finish_bytes=43772"),
            "{line}"
        );
        assert!(line.contains("remote_after_local_finish_reads=1"), "{line}");
        assert!(line.contains("remote_read_service_ticks=1"), "{line}");
        assert!(line.contains("remote_read_service_len_min=16384"), "{line}");
        assert!(line.contains("remote_read_service_len_max=16384"), "{line}");
        assert!(line.contains("remote_batches=1"), "{line}");
        assert!(line.contains("remote_batch_bytes_max=43772"), "{line}");
        assert!(line.contains("remote_batch_chunks_max=3"), "{line}");
        assert!(line.contains("first_remote_read_ms="), "{line}");
        assert!(line.contains("max_remote_read_gap_ms="), "{line}");
        assert!(line.contains("current_remote_read_gap_ms="), "{line}");
        assert!(line.contains("global_rx_queue_used_max=9"), "{line}");
        assert!(line.contains("global_rx_queue_capacity=1024"), "{line}");
    }

    #[test]
    fn reverse_window_diag_line_reports_live_tcp_window_state() {
        let mut sockets = SocketSet::new(vec![]);
        let handle = sockets.add(TcpSocket::new(
            TcpSocketBuffer::new(vec![0; 16]),
            TcpSocketBuffer::new(vec![0; 16]),
        ));
        let snapshot = SocketCloseSnapshot {
            tcp_state: TcpState::Established,
            active: true,
            can_send: true,
            can_recv: false,
            may_send: true,
            may_recv: true,
            send_capacity: 1_048_576,
            send_queue: 65_536,
            recv_queue: 0,
        };

        let line = format_tcp_reverse_window_diag(
            handle,
            TcpReverseWindowDiag {
                ctx_state: SocketState::Relaying,
                payload_bytes: 65_536,
                accepted_bytes: 65_536,
                pending: 0,
                remote_to_global_rx_bytes: 4_194_304,
                local_fin_sent: false,
                snapshot,
            },
        );

        assert!(line.contains("tcp-reverse-window"), "{line}");
        assert!(line.contains("payload_bytes=65536"), "{line}");
        assert!(line.contains("accepted_bytes=65536"), "{line}");
        assert!(line.contains("pending=0"), "{line}");
        assert!(line.contains("remote_to_global_rx_bytes=4194304"), "{line}");
        assert!(line.contains("ctx_state=Relaying"), "{line}");
        assert!(line.contains("local_fin_sent=false"), "{line}");
        assert!(line.contains("tcp_state=Established"), "{line}");
        assert!(line.contains("active=true"), "{line}");
        assert!(line.contains("can_send=true"), "{line}");
        assert!(line.contains("may_recv=true"), "{line}");
        assert!(line.contains("send_capacity=1048576"), "{line}");
        assert!(line.contains("send_queue=65536"), "{line}");
    }

    #[test]
    fn reverse_window_diag_rate_limiter_logs_first_and_interval() {
        let mut ctx = SocketCtx::new(5201);
        let snapshot = SocketCloseSnapshot {
            tcp_state: TcpState::Established,
            active: true,
            can_send: true,
            can_recv: false,
            may_send: true,
            may_recv: true,
            send_capacity: 1_048_576,
            send_queue: 0,
            recv_queue: 0,
        };

        assert!(
            should_log_tcp_reverse_window(&mut ctx, snapshot, 10),
            "first reverse payload should log the local TCP window"
        );
        assert!(
            !should_log_tcp_reverse_window(&mut ctx, snapshot, 14),
            "steady high-throughput reverse payloads should be rate-limited"
        );
        assert!(
            should_log_tcp_reverse_window(&mut ctx, snapshot, 15),
            "diagnostics should refresh after the bounded interval"
        );
    }

    /// 刀13 ①：MINI_VPN_TRACE 解析——`1`/`true`（去空白、不区分大小写）开；其它/缺省关
    /// （默认关 = 主循环热路径零 stdout；翻 flag 恢复全部诊断打印）。
    #[test]
    fn parse_trace_only_truthy_enables() {
        assert!(parse_trace(Some("1")));
        assert!(parse_trace(Some("true")));
        assert!(parse_trace(Some(" TRUE ")));
        assert!(
            !parse_trace(Some("0")),
            "0 必须关——否则 MINI_VPN_TRACE=0 反而开是 footgun"
        );
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
        let err =
            TunRuntimeConfig::from_sources(Some("0")).expect_err("zero pool size should fail");
        assert!(err.to_string().contains("at least 1"));
    }

    #[test]
    fn tun_runtime_config_accepts_pool_size_override() {
        let config = TunRuntimeConfig::from_sources(Some("3")).expect("valid config should load");
        assert_eq!(config.listener.pool_size, 3);
    }

    #[test]
    fn tun_runtime_config_disables_buffered_downlink_by_default() {
        let config = TunRuntimeConfig::from_sources(Some("3")).expect("valid config should load");

        assert!(!config.buffered_downlink.enabled);
        assert_eq!(
            config.buffered_downlink.per_flow_low_bytes,
            config.downlink_backpressure.high_bytes
        );
        assert_eq!(
            config.buffered_downlink.global_hard_bytes,
            config
                .buffered_downlink
                .per_flow_hard_bytes
                .saturating_mul(3)
                .min(MAX_TCP_SOCKET_BUFFER_BYTES)
        );
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
        assert_eq!(
            classify_inbound(&udp_pkt([198, 18, 0, 1], 53)),
            Inbound::Dns
        );
        assert_eq!(classify_inbound(&udp_pkt([8, 8, 8, 8], 53)), Inbound::Dns);
        assert_eq!(classify_inbound(&udp_pkt([1, 1, 1, 1], 53)), Inbound::Dns);
        // 其它 UDP（含 DoT/DoQ :853、DoH3/视频 :443）→ UdpRelay（刀4 Block 由 resolve_target 判）。
        assert_eq!(
            classify_inbound(&udp_pkt([198, 18, 0, 5], 443)),
            Inbound::UdpRelay
        );
        assert_eq!(
            classify_inbound(&udp_pkt([1, 1, 1, 1], 853)),
            Inbound::UdpRelay
        );
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
        Ipv4Addr::new(
            p[p.len() - 4],
            p[p.len() - 3],
            p[p.len() - 2],
            p[p.len() - 1],
        )
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
        assert_eq!(
            u16::from_be_bytes([r.payload[6], r.payload[7]]),
            0,
            "NODATA ANCOUNT=0"
        );
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
        fn try_recv_rx(&mut self) -> std::io::Result<bool> {
            Ok(false)
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
        assert!(matches!(
            resolve_target(ep([1, 1, 1, 1], 853), &pool),
            TargetResolve::Block
        ));
        assert!(matches!(
            resolve_target(ep([93, 184, 216, 34], 853), &pool),
            TargetResolve::Block
        ));

        // 刀5：TCP :53（明文 DNS over TCP，任意 IP）→ Block（RST，逼回落 UDP :53）。
        // 不变量：UDP :53 已被 classify_inbound 截到 Dns 路径、不到 resolve_target，故 port==53 只命中 TCP。
        assert!(matches!(
            resolve_target(ep([8, 8, 8, 8], 53), &pool),
            TargetResolve::Block
        ));
        assert!(matches!(
            resolve_target(ep([198, 18, 0, 1], 53), &pool),
            TargetResolve::Block
        ));

        // DoH 经 fake-IP：dns.google:443 → Block；普通域名:443 → Direct；DoH 域名但 :80 → Direct（仅 :443）。
        let doh_fake = pool.alloc("dns.google", 0);
        assert!(matches!(
            resolve_target(ep(doh_fake.octets(), 443), &pool),
            TargetResolve::Block
        ));
        assert!(matches!(
            resolve_target(ep(doh_fake.octets(), 80), &pool),
            TargetResolve::Direct { .. }
        ));
        let normal_fake = pool.alloc("example.com", 0);
        assert!(matches!(
            resolve_target(ep(normal_fake.octets(), 443), &pool),
            TargetResolve::Direct { .. }
        ));

        // DoH 硬编 IP 1.1.1.1:443 → Block；普通真实 IP:443 → Direct。
        assert!(matches!(
            resolve_target(ep([1, 1, 1, 1], 443), &pool),
            TargetResolve::Block
        ));
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
