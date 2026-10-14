mod dns_hijack;
#[cfg(test)]
use dns_hijack::forge_dns_reply;
use dns_hijack::handle_dns_hijack;

use crate::device::{TunFlushedTcpPacket, TunIngressPumpSnapshot, TunIo};
use crate::fake_ip::FakeIpPool;
use crate::metrics::Metrics;
use crate::owned_upstream::{
    FlowPortError, HalfCloseDelivery, LegacyOwnedUpstream, LocalAssociationId, OpenedOwnedTcp,
    OwnedUpstream, ResumableTcpDownlink, ResumableTcpUplink, SinkDelivery, TcpOwnershipMode,
    TerminalAction, TunAction, UdpDownlinkEvent, UdpDownlinkSource, UdpUplink,
};
use crate::resumable::{FlowFinishReason, LocalFlow, MAX_DATA_PAYLOAD_BYTES, ResetReason};
use crate::shared::{ClientError, TargetAddr};
#[cfg(feature = "harness")]
use crate::tcp_downlink_pump::ByteQueueConfigError;
use crate::tcp_downlink_pump::{
    AsyncByteBoundedFlowQueue, AsyncLeasedByteFlowQueue, ByteQueuePushError,
    D16_GLOBAL_BYTE_BUDGET_DEFAULT_BYTES, D16GlobalByteBudget, DownstreamBytePermit,
    DownstreamPermitReleaseMode, LeasedBytes, LeasedQueuePoll,
};
use crate::tcp_egress::{
    D16_ACTOR_PAYLOAD_PACKET_BUDGET, EgressPermissions, EgressPhase, TcpEgressPendingQueueView,
    transition_egress_phase,
};
use crate::tcp_stream_service::{
    LocalAdmissionProgress, StreamServiceBlockedReason, StreamServiceController,
    StreamServiceWindow, format_stream_service_window_fields,
};
use crate::tuic::AssocTable;
use crate::udp_relay::{FourTuple, UDP_FLOW_IDLE_SECS, build_udp_ip_packet, parse_inbound_udp};
use crate::upstream::{
    DatagramUpstream, NativeTcpChunk, NativeTcpReadHalf, NativeTcpRelayEngine, NativeTcpWriteHalf,
    OpenedTcpRelay, ProxyUpstream, RelayFamilyPolicy, RelayStream, TcpRelayEngine,
    TcpRelayOpenDiag, with_tcp_relay_open_diag,
};
#[cfg(test)]
use crate::upstream::{
    NativeTcpRelayStream, RelayFamilyPolicySources, select_native_tcp_relay_engine,
    select_tcp_relay_engine,
};
use bytes::{Bytes, BytesMut};
use smoltcp::iface::{Config as SmolConfig, Interface, SocketHandle, SocketSet};
use smoltcp::socket::tcp::{
    Socket as TcpSocket, SocketBuffer as TcpSocketBuffer, State as TcpState,
};
use smoltcp::wire::{IpAddress, IpCidr, IpEndpoint};
use std::net::Ipv4Addr;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fmt;
use tokio::sync::{mpsc, oneshot, watch};

pub(crate) const TCP_SOCKET_BUFFER_SIZE: usize = 65_535;
const MIN_TCP_SOCKET_BUFFER_BYTES: usize = 4 * 1024;
const MAX_TCP_SOCKET_BUFFER_BYTES: usize = 16 * 1024 * 1024;
const RELAY_CHANNEL_CAPACITY: usize = 1024;
const _: () = assert!(RELAY_CHANNEL_CAPACITY >= 1);
/// Protocol-owned sink/FIN/terminal completions must remain serviceable while
/// the legacy bulk-data channel is paused by local egress backpressure.
const OWNED_ACTION_CHANNEL_CAPACITY: usize = 1024;
const _: () = assert!(OWNED_ACTION_CHANNEL_CAPACITY >= 1);
const RELAY_GLOBAL_RX_CRITICAL_FREE_SLOTS: usize = 8;
const _: () = assert!(RELAY_GLOBAL_RX_CRITICAL_FREE_SLOTS > 0);
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
const LOCAL_EGRESS_SERVICE_TARGET_BYTES_PER_WINDOW: usize = 128 * 1024;
const D16_PER_FLOW_BYTE_CAPACITY: usize = 512 * 1024;
const D16_ACTOR_QUANTUM_BYTES: usize = 128 * 1024;
const LOCAL_EGRESS_SERVICE_MAX_CYCLES: usize = 8;
const LOCAL_EGRESS_SERVICE_TUN_RX_PACKETS_PER_CYCLE: usize = D16_ACTOR_PAYLOAD_PACKET_BUDGET * 2;
const _: () =
    assert!(LOCAL_EGRESS_SERVICE_TARGET_BYTES_PER_WINDOW <= DEFAULT_DOWNLINK_FLUSH_MAX_BYTES);
const _: () = assert!(LOCAL_EGRESS_SERVICE_MAX_CYCLES > 0);
const _: () = assert!(LOCAL_EGRESS_SERVICE_TUN_RX_PACKETS_PER_CYCLE > 0);
const _: () =
    assert!(D16_ACTOR_PAYLOAD_PACKET_BUDGET * 2 == LOCAL_EGRESS_SERVICE_TUN_RX_PACKETS_PER_CYCLE);
/// Knife14fa: drain QUIC streams with a healthy read window, but stage delivery
/// into the main loop so one ready remote burst cannot become one oversized
/// smoltcp/send_queue injection.
const RELAY_REMOTE_DISPATCH_SEGMENT_MAX_BYTES: usize = LOCAL_EGRESS_SERVICE_TARGET_BYTES_PER_WINDOW;
const _: () = assert!(RELAY_REMOTE_DISPATCH_SEGMENT_MAX_BYTES < DEFAULT_DOWNLINK_FLUSH_MAX_BYTES);
const _: () = assert!(RELAY_REMOTE_DISPATCH_SEGMENT_MAX_BYTES <= RELAY_REMOTE_READ_BURST_MAX_BYTES);
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
/// Knife14: feature-gated relay reader/dispatcher decoupling. The cap is in
/// staged payload messages; dispatch segmentation bounds each message size.
const THIN_TCP_RELAY_STAGING_CAPACITY: usize = 64;
const _: () = assert!(THIN_TCP_RELAY_STAGING_CAPACITY >= 1);
/// Knife14h8: feature-gated continuous relay pump. The cap is byte-based per
/// flow so remote reads pause at an explicit queue-full edge.
const CONTINUOUS_TCP_RELAY_QUEUE_BYTES: usize = 4 * 1024 * 1024;
const _: () = assert!(CONTINUOUS_TCP_RELAY_QUEUE_BYTES >= RELAY_REMOTE_READ_MIN_BATCH_BYTES);
/// A concrete upstream write may remain pending this long without progress before bounded cleanup.
/// Full-duplex Established relays have no application-payload idle lifetime.
const RELAY_STALLED_WRITE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(90);
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
/// A clean protocol-owned TCP close may make the local smoltcp socket
/// inactive just before the reducer's exact graceful terminal capability is
/// forwarded. Keep that race bounded without rewriting it as LocalReset.
const OWNED_TERMINAL_WAIT_GRACE_SECS: u64 = 5;
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
const D4_STALLED_READ_SERVICE_HOLD_MS: u64 = 1_000;
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
const TUN_RX_DRAIN_SOURCE_TIMER_STALLED_READ: &str = "timer_stalled_read";
const TUN_RX_DRAIN_SOURCE_LOCAL_EGRESS_SERVICE: &str = "local_egress_service";
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
const RELAY_RAW_READ_ACK_DRAIN_MIN_BYTES: u64 = 8 * 1024;

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

mod compatibility_runtime;
pub(crate) use compatibility_runtime::run_owned_event_loop;
use compatibility_runtime::{
    GenericLaunchRequest, HandshakeNotice, OpenDispatch, OpenEvent, RearmGate, RearmIntent,
    RemotePayloadRequest, SessionControl, flush_tx_and_release_downlink_permits, gate_legacy_rearm,
    launch_generic_relay, legacy_bare_rearm_from_loop, legacy_reasoned_rearm_from_loop,
    legacy_reasoned_rearm_with_gate, legacy_reasoned_rearm_with_snapshot_and_gate,
    reap_dead_slots_from_loop, run_relay, run_relay_ordered_egress_permit, run_relay_thin,
    run_relay_with_engine, run_relay_writer, send_remote_payload_batch_to_main,
};
#[cfg(test)]
use compatibility_runtime::{
    ListenerOwnedCounts, StartupProfileDisposition, StartupProfileObservation, reap_dead_slots,
    should_defer_owned_terminal_reap, should_reap_slot, should_reap_slot_with_snapshot,
};
#[cfg(test)]
use compatibility_runtime::{ReadyReadRequest, drain_ready_remote_reads};
pub use compatibility_runtime::{run_event_loop, start_tun_proxy};

mod legacy_event_loop;
use legacy_event_loop::activity::{HandshakeCompletion, ListenerActivity};
use legacy_event_loop::ingress::{drain_ready_tun_rx, process_ready_tun_rx_packet};
use legacy_event_loop::local_egress::service_local_egress_until;
#[cfg(test)]
use legacy_event_loop::read_credit::d16_relay_read_credit;
use legacy_event_loop::read_credit::{
    dirty_downlink_pending_total, global_pending_for_buffered_credit, initial_relay_read_credit,
    publish_projected_relay_read_credit_for_payload, publish_relay_read_credit_for_dirty_handles,
    publish_relay_read_credit_for_handle,
};
use legacy_event_loop::remote_payload::{
    handle_remote_payload_from_loop, handle_remote_payload_with_gate,
};
mod listener_registry;
mod native_permit_supervision;

pub(in crate::client_tun) use native_permit_supervision::run_relay_native_permit;

use legacy_event_loop::relay_events::{
    RelayEventIngressState, RelayEventLane, RelayEventSocketState, RelayEventTurn,
};
use legacy_event_loop::{RelayClose, RelayEvent, run_borrowed_event_loop};
#[cfg(test)]
use legacy_event_loop::{
    RelayGapAckDrainFollowup, RelayGapAckDrainState, arm_d4_stalled_read_service,
    clear_d4_stalled_read_service, handle_relay_closed, has_d4_stalled_read_service,
    note_ack_cadence_gap_hint_for_handle, remote_payload_left_local_egress_work,
    should_drain_tun_rx_after_remote_payload, tun_rx_drain_budget_before_remote_payload,
    tun_rx_drain_budget_for_relay_gap_followup, tun_rx_drain_budget_for_relay_gap_hint,
};
#[cfg(test)]
use listener_registry::ListenerCapacityRejection;
pub use listener_registry::TunListenerConfig;
use listener_registry::{
    ListenerCapacityProfile, ListenerRegistry, TcpSocketBufferConfig, configure_local_tcp_socket,
    parse_tcp_socket_buffer_config,
};
#[cfg(test)]
use listener_registry::{
    ListenerSpec, MAX_INTERCEPTED_PORTS, MAX_TOTAL_LISTENERS, RegistryError, build_listener_socket,
    build_listener_socket_with_buffers, inspect_inbound_tcp,
};

/// 主循环分段插桩接缝（knife1：并发压测定位瓶颈）。
///
/// 中文要点：生产传 [`NoopSink`]（空方法，单态化内联后**零开销**，热路径无 `Instant::now()`）；
/// 并发压测 harness 传 RecordingSink，在每个回调里采集每段耗时/调用次数。计时逻辑全部留在 sink
/// 实现内，主循环只做平凡方法调用——生产与测试**同一份循环**。
pub trait MetricsSink {
    /// Harness-only hot-path observation gate. Production [`NoopSink`] keeps
    /// this false so aggregate scans are compiled out of the data plane.
    fn wants_d16_harness_observations(&self) -> bool {
        false
    }
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
    /// H10d16 Task 11A：harness 记录 TUN RX backlog guard 的生产路径边缘。
    fn note_tun_rx_backlog_guard(
        &mut self,
        _active: bool,
        _budget_exhausted: u64,
        _pause_edges: u64,
        _resume_edges: u64,
    ) {
    }
    /// H10d16 TUN RX batch service: expose cumulative batch accounting to
    /// deterministic harnesses without adding per-packet observations.
    fn note_tun_rx_batch_service(
        &mut self,
        _tcp_packets: u64,
        _tcp_batches: u64,
        _batch_dirty_relay_passes: u64,
        _batch_iface_polls: u64,
        _batch_flushes: u64,
        _tcp_batch_packets_high_water: u64,
        _avoided_dirty_relay_passes: u64,
        _avoided_iface_polls: u64,
        _pump_packets: u64,
        _pump_queue_high_water: u64,
        _pump_capacity_packets: usize,
        _pump_full_waits: u64,
        _pump_read_errors: u64,
    ) {
    }
    /// H10d16 Task 11A：harness 记录 D16 actor bypass 聚合值。
    fn note_d16_actor_observation(
        &mut self,
        _actor_bypass_admitted_bytes: u64,
        _running_flows: usize,
        _drain_only_flows: usize,
        _recovery_flows: usize,
        _uplink_recv_queue_max: usize,
    ) {
    }
    /// H10d16 Task 11A follow-up: a ready local TCP packet directly scheduled
    /// bounded D16 actor service instead of waiting for the 5ms timer.
    fn note_d16_tun_rx_actor_wake(&mut self) {}
    /// H10d16 Task 11A: observe the exact byte tail at the local EOF boundary.
    fn note_d16_flow_lifecycle(
        &mut self,
        _owned_queue_bytes: usize,
        _pending_bytes: usize,
        _inflight_bytes: usize,
        _local_eof_sent_flows: usize,
        _terminal_drop_bytes: u64,
        _terminal_late_payload_bytes: u64,
    ) {
    }
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
    /// 本地不 active 仍回收；CloseWait 无 live relay 也回收，但 Resumable pending 必须等 open 后发送 FIN。
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
    /// Bytes received with D2.3 downstream permits.
    downstream_permit_bytes: u64,
    /// Permit bytes released after local admission or explicit drop.
    downstream_permit_released_bytes: u64,
    /// Highest remaining permit bytes held by this flow.
    downstream_permit_pending_high_water: usize,
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
    /// D16 bytes accepted by the actor-owned `send_slice` admission seam.
    actor_admitted_bytes: u64,
    /// D16 bytes accepted outside the actor seam; must remain zero.
    actor_bypass_admitted_bytes: u64,
    /// D16 egress phase transitions observed for this flow.
    d16_egress_phase_transitions: u64,
    /// Recovery cycles reset by renewed pressure or drop evidence.
    d16_recovery_resets: u64,
    /// Completed DrainOnly local-drain cycles.
    d16_drain_only_cycles: u64,
    /// Local egress bytes drained during DrainOnly cycles.
    d16_drain_only_drain_bytes: u64,
    /// D16 payload bytes explicitly released by a terminal local close.
    permit_terminal_drop_bytes: u64,
    /// Terminal local close events that released at least one owned D16 byte.
    permit_terminal_drop_events: u64,
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
    fn note_uplink_recv_queue(&mut self, recv_queue: usize) {
        self.recv_queue_max = self.recv_queue_max.max(recv_queue);
    }

    fn note_pending(&mut self, pending_current: usize) {
        self.downlink_pending_high_water = self.downlink_pending_high_water.max(pending_current);
    }

    fn note_remote_payload(&mut self, bytes: usize, pending_current: usize) {
        self.remote_to_global_rx_bytes += bytes as u64;
        self.note_pending(pending_current);
    }

    fn note_downstream_permit(&mut self, bytes: usize, pending_permit_bytes: usize) {
        self.downstream_permit_bytes = self.downstream_permit_bytes.saturating_add(bytes as u64);
        self.downstream_permit_pending_high_water = self
            .downstream_permit_pending_high_water
            .max(pending_permit_bytes);
    }

    fn note_downstream_permit_release(&mut self, bytes: usize, pending_permit_bytes: usize) {
        self.downstream_permit_released_bytes = self
            .downstream_permit_released_bytes
            .saturating_add(bytes as u64);
        self.downstream_permit_pending_high_water = self
            .downstream_permit_pending_high_water
            .max(pending_permit_bytes);
    }

    fn note_downstream_permit_inflight(&mut self, _bytes: usize, inflight_permit_bytes: usize) {
        self.downstream_permit_pending_high_water = self
            .downstream_permit_pending_high_water
            .max(inflight_permit_bytes);
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

    fn note_d16_admission(
        &mut self,
        origin: DownlinkAdmissionOrigin,
        accepted: usize,
        d16_flow: bool,
    ) {
        if !d16_flow || accepted == 0 {
            return;
        }
        if origin.allows_d16_admission() {
            self.actor_admitted_bytes = self.actor_admitted_bytes.saturating_add(accepted as u64);
        } else {
            self.actor_bypass_admitted_bytes = self
                .actor_bypass_admitted_bytes
                .saturating_add(accepted as u64);
        }
    }

    fn note_d16_egress_phase(
        &mut self,
        previous: EgressPhase,
        next: EgressPhase,
        completed_drain_bytes: Option<usize>,
    ) {
        if let Some(bytes) = completed_drain_bytes
            && previous == EgressPhase::DrainOnly
        {
            self.d16_drain_only_cycles = self.d16_drain_only_cycles.saturating_add(1);
            self.d16_drain_only_drain_bytes =
                self.d16_drain_only_drain_bytes.saturating_add(bytes as u64);
        }
        if previous != next {
            self.d16_egress_phase_transitions = self.d16_egress_phase_transitions.saturating_add(1);
        }
        if matches!(previous, EgressPhase::Recovery { .. }) && next == EgressPhase::DrainOnly {
            self.d16_recovery_resets = self.d16_recovery_resets.saturating_add(1);
        }
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
    /// Bytes read directly from a native QUIC stream before ordered dispatch.
    remote_raw_read_bytes: u64,
    /// Native QUIC stream chunks read before ordered dispatch.
    remote_raw_reads: u64,
    /// Number of remote-to-main data batches sent after bounded ready-read coalescing.
    remote_batches: u64,
    /// Combined stream-service state since this relay reader started.
    stream_service: StreamServiceController,
    /// Largest remote-to-main batch in bytes.
    remote_batch_bytes_max: usize,
    /// Largest number of remote read chunks coalesced into one remote-to-main batch.
    remote_batch_chunks_max: usize,
    /// Lowest ready-read byte budget observed from relay -> main-loop channel pressure.
    remote_batch_limit_bytes_min: usize,
    /// Count of remote ready-read batches whose byte budget was reduced by downstream pressure.
    remote_batch_limited: u64,
    /// Highest observed wait for byte capacity in the continuous relay queue.
    continuous_queue_wait_max_micros: u128,
    /// Count of continuous relay queue waits that crossed the diagnostic threshold.
    continuous_queue_wait_events: u64,
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
            remote_raw_read_bytes: 0,
            remote_raw_reads: 0,
            remote_batches: 0,
            stream_service: StreamServiceController::new(),
            remote_batch_bytes_max: 0,
            remote_batch_chunks_max: 0,
            remote_batch_limit_bytes_min: 0,
            remote_batch_limited: 0,
            continuous_queue_wait_max_micros: 0,
            continuous_queue_wait_events: 0,
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

    fn note_remote_raw_read(&mut self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        self.remote_raw_reads = self.remote_raw_reads.saturating_add(1);
        self.remote_raw_read_bytes = self.remote_raw_read_bytes.saturating_add(bytes as u64);
    }

    fn note_remote_read_at(&mut self, bytes: usize, now: std::time::Instant) {
        self.stream_service.note_remote_read(bytes);
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

    fn note_continuous_queue_wait(
        &mut self,
        elapsed: std::time::Duration,
        pressure_threshold: std::time::Duration,
    ) {
        self.continuous_queue_wait_max_micros = self
            .continuous_queue_wait_max_micros
            .max(elapsed.as_micros());
        if elapsed >= pressure_threshold {
            self.continuous_queue_wait_events = self.continuous_queue_wait_events.saturating_add(1);
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
        self.stream_service
            .note_global_rx_wait(elapsed, pressure_threshold);
        self.global_rx_wait_max_micros = self.global_rx_wait_max_micros.max(elapsed.as_micros());
        if elapsed >= pressure_threshold {
            self.global_rx_pressure_events += 1;
        }
    }

    fn note_global_rx_queue(&mut self, used: usize, max_capacity: usize) {
        self.stream_service.note_global_rx_queue(used, max_capacity);
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

    fn next_remote_read_len(&mut self, read_credit: RelayReadCredit, buffer_len: usize) -> usize {
        let read_len = self.stream_service.next_remote_read_len(
            read_credit.paused,
            read_credit.max_batch_bytes,
            buffer_len,
        );
        self.note_remote_read_service_tick(read_len);
        read_len
    }

    fn stream_service_window(&self) -> StreamServiceWindow {
        self.stream_service.window()
    }

    fn copy_reader_fields_from(&mut self, reader: &RelayTaskDiag) {
        self.first_remote_read_millis = reader.first_remote_read_millis;
        self.first_remote_read_at = reader.first_remote_read_at;
        self.last_remote_read_at = reader.last_remote_read_at;
        self.max_remote_read_gap_millis = reader.max_remote_read_gap_millis;
        self.remote_to_global_rx_bytes = reader.remote_to_global_rx_bytes;
        self.remote_raw_read_bytes = reader.remote_raw_read_bytes;
        self.remote_raw_reads = reader.remote_raw_reads;
        self.remote_batches = reader.remote_batches;
        self.stream_service = reader.stream_service.clone();
        self.remote_batch_bytes_max = reader.remote_batch_bytes_max;
        self.remote_batch_chunks_max = reader.remote_batch_chunks_max;
        self.remote_batch_limit_bytes_min = reader.remote_batch_limit_bytes_min;
        self.remote_batch_limited = reader.remote_batch_limited;
        self.continuous_queue_wait_max_micros = reader.continuous_queue_wait_max_micros;
        self.continuous_queue_wait_events = reader.continuous_queue_wait_events;
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
        || (diag.remote_to_global_rx_bytes > 0
            && diag.remote_read_service_ticks > 0
            && diag.remote_raw_read_bytes >= RELAY_RAW_READ_ACK_DRAIN_MIN_BYTES)
}

fn relay_remote_read_probe_has_serviceable_payload(diag: &RelayTaskDiag) -> bool {
    relay_ack_drain_hint_has_serviceable_payload(diag)
        || (diag.remote_to_global_rx_bytes > 0
            && diag.remote_batch_bytes_max >= RELAY_REMOTE_READ_PROBE_MIN_BATCH_BYTES)
}

fn should_poll_relay_ack_drain_hint(diag: &RelayTaskDiag, _writer_done: bool) -> bool {
    diag.remote_reads > 0 && relay_ack_drain_hint_has_serviceable_payload(diag)
}

fn should_poll_relay_remote_read_probe(diag: &RelayTaskDiag, read_credit: RelayReadCredit) -> bool {
    let stream_poll_credit = relay_stream_poll_credit_for_local_admission(read_credit);
    !stream_poll_credit.paused
        && stream_poll_credit.max_batch_bytes > 0
        && diag.remote_reads > 0
        && relay_remote_read_probe_has_serviceable_payload(diag)
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
    let free_slots = max_capacity.saturating_sub(used);
    if max_capacity >= RELAY_GLOBAL_RX_CRITICAL_FREE_SLOTS * 2
        && free_slots <= RELAY_GLOBAL_RX_CRITICAL_FREE_SLOTS
    {
        return 0;
    }
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
         uplink_bytes={} uplink_writes={} remote_to_global_rx_bytes={} remote_reads={} remote_raw_read_bytes={} remote_raw_reads={} remote_read_probe_ticks={} remote_read_service_ticks={} remote_read_service_len_min={} remote_read_service_len_max={} \
         remote_batches={} remote_batch_bytes_max={} remote_batch_chunks_max={} remote_batch_limit_bytes_min={} remote_batch_limited={} read_credit_updates={} read_credit_pause_updates={} read_credit_limit_bytes_min={} \
         continuous_queue_wait_max_us={} continuous_queue_wait_events={} \
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
        diag.remote_raw_read_bytes,
        diag.remote_raw_reads,
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
        diag.continuous_queue_wait_max_micros,
        diag.continuous_queue_wait_events,
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
        "🔎 tcp-relay-close handle={:?} direction={} reason={} uplink_bytes={} uplink_writes={} remote_to_global_rx_bytes={} remote_reads={} remote_raw_read_bytes={} remote_raw_reads={} remote_read_probe_ticks={} remote_read_service_ticks={} remote_read_service_len_min={} remote_read_service_len_max={} remote_batches={} remote_batch_bytes_max={} remote_batch_chunks_max={} remote_batch_limit_bytes_min={} remote_batch_limited={} read_credit_updates={} read_credit_pause_updates={} read_credit_limit_bytes_min={} continuous_queue_wait_max_us={} continuous_queue_wait_events={} local_finish_events={} first_local_finish_after_first_remote_read_ms={} remote_after_local_finish_bytes={} remote_after_local_finish_reads={} first_remote_read_ms={} max_remote_read_gap_ms={} max_remote_read_gap_before_local_finish_ms={} max_remote_read_gap_after_local_finish_ms={} current_remote_read_gap_ms={} ack_drain_hint_due={} ack_drain_hint_sent={} ack_drain_hint_dropped={} global_rx_wait_max_us={} global_rx_pressure_events={} global_rx_queue_used_max={} global_rx_queue_capacity={} local_write_wait_max_us={} local_write_pressure_events={}",
        handle,
        close_direction,
        close_reason,
        diag.uplink_bytes,
        diag.local_writes,
        diag.remote_to_global_rx_bytes,
        diag.remote_reads,
        diag.remote_raw_read_bytes,
        diag.remote_raw_reads,
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
        diag.continuous_queue_wait_max_micros,
        diag.continuous_queue_wait_events,
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
    WritePending,
    Progress {
        bytes: usize,
        write_wait: std::time::Duration,
    },
    WriteCompleted,
    WriteHalfClosed {
        reason: &'static str,
    },
    Closed {
        direction: &'static str,
        reason: &'static str,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RelayCloseTimerMode {
    Disarmed,
    StalledWrite,
    HalfClosed,
}

struct RelayCloseTimer {
    mode: RelayCloseTimerMode,
    sleep: std::pin::Pin<Box<tokio::time::Sleep>>,
}

impl RelayCloseTimer {
    fn new() -> Self {
        Self {
            mode: RelayCloseTimerMode::Disarmed,
            sleep: Box::pin(tokio::time::sleep(RELAY_STALLED_WRITE_TIMEOUT)),
        }
    }

    fn is_armed(&self) -> bool {
        self.mode != RelayCloseTimerMode::Disarmed
    }

    fn arm_stalled_write(&mut self) {
        self.mode = RelayCloseTimerMode::StalledWrite;
        self.reset(RELAY_STALLED_WRITE_TIMEOUT);
    }

    fn note_write_progress(&mut self) {
        if self.mode == RelayCloseTimerMode::StalledWrite {
            self.reset(RELAY_STALLED_WRITE_TIMEOUT);
        }
    }

    fn complete_write(&mut self) {
        if self.mode == RelayCloseTimerMode::StalledWrite {
            self.mode = RelayCloseTimerMode::Disarmed;
        }
    }

    fn arm_half_closed(&mut self) {
        self.mode = RelayCloseTimerMode::HalfClosed;
        self.reset(RELAY_HALF_CLOSED_IDLE_TIMEOUT);
    }

    fn note_read_progress(&mut self) {
        match self.mode {
            RelayCloseTimerMode::Disarmed => {}
            RelayCloseTimerMode::StalledWrite => self.reset(RELAY_STALLED_WRITE_TIMEOUT),
            RelayCloseTimerMode::HalfClosed => self.reset(RELAY_HALF_CLOSED_IDLE_TIMEOUT),
        }
    }

    fn reset(&mut self, timeout: std::time::Duration) {
        self.sleep
            .as_mut()
            .reset(tokio::time::Instant::now() + timeout);
    }

    fn terminal_reason(&self) -> &'static str {
        match self.mode {
            RelayCloseTimerMode::StalledWrite => "stalled_write_timeout",
            RelayCloseTimerMode::HalfClosed => "half_closed_idle_timeout",
            RelayCloseTimerMode::Disarmed => "relay_close_timer_disarmed",
        }
    }

    fn timeout(&self) -> Option<std::time::Duration> {
        match self.mode {
            RelayCloseTimerMode::Disarmed => None,
            RelayCloseTimerMode::StalledWrite => Some(RELAY_STALLED_WRITE_TIMEOUT),
            RelayCloseTimerMode::HalfClosed => Some(RELAY_HALF_CLOSED_IDLE_TIMEOUT),
        }
    }

    fn is_half_closed(&self) -> bool {
        self.mode == RelayCloseTimerMode::HalfClosed
    }

    fn wait(&mut self) -> std::pin::Pin<&mut tokio::time::Sleep> {
        self.sleep.as_mut()
    }
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

/// Protocol-owned sink/control wakeup on a lane independent of legacy bulk
/// DATA. The exact completion capability remains owned until the single TUN
/// actor observes real smoltcp acceptance or fails it closed.
#[derive(Debug)]
struct OwnedActionEvent {
    handle: SocketHandle,
    epoch: u64,
    action: OwnedAction,
}

#[derive(Debug)]
enum OwnedAction {
    Tun(TunAction),
    /// The sole session driver disappeared without first publishing an exact
    /// reducer terminal. Keep this epoch-tagged and flow-tagged so only the
    /// installed socket incarnation is failed closed.
    PortClosed {
        flow: LocalFlow,
    },
}

/// Cancellation owner for the one downlink-action forwarder attached to a
/// resumable flow. Dropping it wakes the task; any in-flight Sink/HalfClose
/// action then uses its fail-closed `Drop` completion.
struct ResumableActionBridge {
    cancel: Option<oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<()>,
}

impl fmt::Debug for ResumableActionBridge {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResumableActionBridge")
            .field("cancel_armed", &self.cancel.is_some())
            .field("task_finished", &self.task.is_finished())
            .finish()
    }
}

impl Drop for ResumableActionBridge {
    fn drop(&mut self) {
        if let Some(cancel) = self.cancel.take() {
            let _ = cancel.send(());
        }
        // Drop is synchronous, so it cannot await the join handle. Aborting
        // after the cooperative wake guarantees the task is not detached;
        // Tokio drops any in-flight TunAction and its fail-closed completion.
        self.task.abort();
    }
}

fn spawn_resumable_action_bridge(
    handle: SocketHandle,
    epoch: u64,
    mut downlink: ResumableTcpDownlink,
    owned_action_tx: mpsc::Sender<OwnedActionEvent>,
) -> ResumableActionBridge {
    let (cancel_tx, mut cancel_rx) = oneshot::channel();
    let flow = downlink.local_flow();
    let task = tokio::spawn(async move {
        loop {
            let received = tokio::select! {
                biased;
                _ = &mut cancel_rx => break,
                action = downlink.recv_action() => action,
            };
            let (action, stop_after_send) = match received {
                Some(action) => {
                    // An exact reducer terminal is the final action for this
                    // flow. Stop after forwarding it so the driver's normal
                    // teardown cannot be misreported as an owner-loss
                    // `PortClosed` for the same still-draining TCP epoch.
                    let is_terminal = matches!(&action, TunAction::Terminal(_));
                    (OwnedAction::Tun(action), is_terminal)
                }
                None => (OwnedAction::PortClosed { flow }, true),
            };
            let event = OwnedActionEvent {
                handle,
                epoch,
                action,
            };
            tokio::select! {
                biased;
                _ = &mut cancel_rx => break,
                sent = owned_action_tx.send(event) => {
                    if sent.is_err() {
                        break;
                    }
                }
            }
            if stop_after_send {
                break;
            }
        }
    });
    ResumableActionBridge {
        cancel: Some(cancel_tx),
        task,
    }
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

fn relay_stream_poll_credit_for_local_admission(read_credit: RelayReadCredit) -> RelayReadCredit {
    if read_credit.paused || read_credit.max_batch_bytes == 0 {
        return RelayReadCredit {
            paused: true,
            max_batch_bytes: 0,
        };
    }

    RelayReadCredit {
        paused: false,
        max_batch_bytes: read_credit.max_batch_bytes.clamp(
            RELAY_REMOTE_DISPATCH_SEGMENT_MAX_BYTES,
            RELAY_REMOTE_READ_BURST_MAX_BYTES,
        ),
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
    /// Knife16 Task 3: mutually exclusive protocol-owned uplink. Its byte
    /// reservation is acquired before smoltcp `recv`, unlike the legacy
    /// message-only relay channel above.
    resumable_uplink: Option<ResumableTcpUplink>,
    /// Exact sink offer currently awaiting real smoltcp acceptance.
    resumable_sink: Option<SinkDelivery>,
    /// Two-phase peer FIN awaiting application to the local smoltcp socket.
    resumable_half_close: Option<HalfCloseDelivery>,
    /// Reducer-owned terminal decision. Graceful completion waits for the
    /// local FIN to drain; reset/open-failure decisions abort this flow only.
    resumable_terminal: Option<TerminalAction>,
    /// Sole downlink receiver task for this flow; dropping the bridge cancels
    /// it and fail-closes any in-flight completion capability.
    resumable_action_bridge: Option<ResumableActionBridge>,
    /// Contract selected before an asynchronous upstream open. It prevents an
    /// adapter from returning a flow whose extraction order differs from the
    /// one used while the open was in flight.
    pending_open_mode: Option<TcpOwnershipMode>,
    /// Sender used to feed local egress pressure back to the relay read half.
    relay_read_credit_tx: Option<watch::Sender<RelayReadCredit>>,
    /// H10d16: the only remote payload reservoir for this flow. Global relay
    /// events carry readiness, while payload and permits stay in this queue.
    d16_downlink_queue: Option<AsyncLeasedByteFlowQueue>,
    /// H10d16: per-flow read/admission feedback phase. Drain permission remains
    /// with the single local egress actor in every phase.
    d16_egress_phase: EgressPhase,
    /// A device backlog episode observed local TCP bytes still awaiting ACKs
    /// for this flow. Only this flow remains DrainOnly until send_queue reaches zero.
    d16_tun_rx_ack_barrier: bool,
    /// Latest egress-progress generation published to the relay read half.
    relay_read_credit_progress_generation_sent: u64,
    /// Whether this flow has already propagated the local TCP finish into the relay write half.
    local_fin_sent: bool,
    /// Whether remote EOF has been propagated to the intercepted local TCP peer with a FIN.
    local_eof_sent: bool,
    /// An already-issued sink/FIN capability was invalidated by an exact
    /// reducer terminal before the TUN actor could apply it. While set, local
    /// source reads wait for the already-queued terminal action.
    owned_terminal_pending: bool,
    /// Bounded wait start for the exact reducer terminal after both TCP
    /// half-closes have been committed. This prevents the one-second dead-slot
    /// sweep from racing a queued graceful terminal action.
    owned_terminal_wait_since_secs: Option<u64>,
    /// Monotonic event-loop seconds when local finish became pending but was deferred for reverse traffic.
    local_fin_pending_since_secs: Option<u64>,
    /// Latest remote-to-local progress while a local finish is pending.
    local_fin_last_remote_progress_secs: Option<u64>,
    /// Downlink bytes not yet accepted by the smoltcp tx buffer.
    /// 中文要点：smoltcp send_slice 可能只写一部分（tx buffer 受 TCP ACK 释放制约），
    /// 写不下的字节必须留在这里、由后续 poll 持续 flush，否则丢字节 → TLS bad decrypt。
    downlink_pending: Vec<u8>,
    /// D2.3: byte-capacity permits tied to prefixes of `downlink_pending`.
    /// Permits are released when bytes are accepted by smoltcp or explicitly dropped.
    downlink_pending_permits: VecDeque<DownstreamBytePermit>,
    /// D6: permits for bytes accepted by smoltcp but not yet observed as local egress drain.
    downlink_inflight_permits: VecDeque<DownstreamBytePermit>,
    /// D6: this flow carries permits that must release on egress drain, not smoltcp admission.
    downlink_delay_permit_release_until_egress: bool,
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
    flow_mirror: Option<compatibility_runtime::FlowMirror>,
    local_reset: Option<compatibility_runtime::LocalReset>,
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
    /// H10d: D3 actor flows use the smoltcp/TUN owner as the local egress writer.
    downlink_d3_actor_flow: bool,
    /// H10d15 diagnostic: allow D3 actor flows to spend drain credit to the credit edge.
    downlink_d3_actor_adaptive_credit: bool,
    /// H10d25 diagnostic: replay legacy credit/debt semantics for D3 actor admission.
    downlink_d3_actor_legacy_credit: bool,
    /// H10d33 diagnostic: D3 actor uses socket-capacity guarded backpressure.
    downlink_d5_capacity_backpressure: bool,
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
            resumable_uplink: None,
            resumable_sink: None,
            resumable_half_close: None,
            resumable_terminal: None,
            resumable_action_bridge: None,
            pending_open_mode: None,
            relay_read_credit_tx: None,
            d16_downlink_queue: None,
            d16_egress_phase: EgressPhase::Running,
            d16_tun_rx_ack_barrier: false,
            relay_read_credit_progress_generation_sent: 0,
            local_fin_sent: false,
            local_eof_sent: false,
            owned_terminal_pending: false,
            owned_terminal_wait_since_secs: None,
            local_fin_pending_since_secs: None,
            local_fin_last_remote_progress_secs: None,
            downlink_pending: Vec::new(),
            downlink_pending_permits: VecDeque::new(),
            downlink_inflight_permits: VecDeque::new(),
            downlink_delay_permit_release_until_egress: false,
            pending_relay_close: None,
            pending_relay_close_since_secs: None,
            pending_relay_close_last_progress_secs: None,
            pending_relay_close_last_pending_bytes: 0,
            pending_relay_close_last_egress_progress_secs: None,
            pending_relay_close_last_egress_queue_bytes: 0,
            downlink_pending_last_progress_secs: None,
            downlink_pending_last_pending_bytes: 0,
            fake_ip: None,
            flow_mirror: None,
            local_reset: None,
            conn_epoch: 0,
            uplink_buffer: Vec::new(),
            downlink_diag: TcpDownlinkDiag::default(),
            downlink_egress_clock: DownlinkEgressClock::default(),
            downlink_credit_controller: DownlinkCreditController::default(),
            downlink_d3_actor_flow: false,
            downlink_d3_actor_adaptive_credit: false,
            downlink_d3_actor_legacy_credit: false,
            downlink_d5_capacity_backpressure: false,
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

    fn resumable_sink_pending_bytes(&self) -> usize {
        self.resumable_sink
            .as_ref()
            .map(SinkDelivery::offered_len)
            .unwrap_or(0)
    }

    fn pending_downlink_bytes(&self) -> usize {
        self.downlink_pending
            .len()
            .saturating_add(self.resumable_sink_pending_bytes())
    }

    fn has_live_uplink(&self) -> bool {
        self.uplink_tx.is_some() || self.resumable_uplink.is_some()
    }

    fn uses_owned_egress_actor(&self) -> bool {
        self.d16_downlink_queue.is_some() || self.downlink_d3_actor_flow
    }

    fn awaits_owned_graceful_terminal(&self) -> bool {
        self.resumable_uplink.is_some()
            && self.local_fin_sent
            && self.local_eof_sent
            && self.resumable_terminal.is_none()
    }

    fn append_downlink_pending_with_permit(
        &mut self,
        payload: &[u8],
        permit: Option<DownstreamBytePermit>,
    ) {
        if permit.as_ref().is_some_and(|permit| {
            permit.release_mode() == DownstreamPermitReleaseMode::OnEgressDrain
        }) {
            self.downlink_delay_permit_release_until_egress = true;
        }
        let outcome = {
            let mut pending = TcpEgressPendingQueueView::new(
                &mut self.downlink_pending,
                &mut self.downlink_pending_permits,
            );
            pending.append(payload, permit)
        };
        if outcome.retained_lease_bytes > 0 {
            self.downlink_diag
                .note_downstream_permit(outcome.retained_lease_bytes, outcome.leased_bytes_after);
        }
    }

    fn admit_downlink_pending_prefix(&mut self, bytes: usize) -> usize {
        let before = self.downlink_pending.len();
        let released = if self.downlink_delay_permit_release_until_egress {
            self.move_downlink_pending_prefix_to_inflight(bytes)
        } else {
            self.release_downlink_permits(bytes)
        };
        if let Some(mirror) = &mut self.flow_mirror {
            mirror.admit_pending(
                before,
                self.downlink_pending.len(),
                bytes,
                self.d16_downlink_queue.is_none()
                    && !self.downlink_delay_permit_release_until_egress,
            );
        }
        released
    }

    fn move_downlink_pending_prefix_to_inflight(&mut self, bytes: usize) -> usize {
        let outcome = {
            let mut pending = TcpEgressPendingQueueView::new(
                &mut self.downlink_pending,
                &mut self.downlink_pending_permits,
            );
            pending.transfer_prefix_to(bytes, &mut self.downlink_inflight_permits)
        };
        if outcome.transferred_lease_bytes > 0 {
            self.downlink_diag.note_downstream_permit_inflight(
                outcome.transferred_lease_bytes,
                outcome.inflight_leased_bytes_after,
            );
        }
        outcome.transferred_lease_bytes
    }

    fn release_downlink_permits(&mut self, bytes: usize) -> usize {
        let outcome = {
            let mut pending = TcpEgressPendingQueueView::new(
                &mut self.downlink_pending,
                &mut self.downlink_pending_permits,
            );
            pending.consume_prefix(bytes)
        };
        if outcome.released_lease_bytes > 0 {
            self.downlink_diag.note_downstream_permit_release(
                outcome.released_lease_bytes,
                outcome.leased_bytes_after,
            );
        }
        outcome.released_lease_bytes
    }

    fn release_downlink_inflight_permits(&mut self, mut bytes: usize) -> usize {
        let mut released = 0usize;
        while bytes > 0 {
            let Some(front) = self.downlink_inflight_permits.front_mut() else {
                break;
            };
            let n = front.release(bytes);
            if n == 0 {
                break;
            }
            released = released.saturating_add(n);
            bytes = bytes.saturating_sub(n);
            if front.remaining_bytes() == 0 {
                self.downlink_inflight_permits.pop_front();
            }
        }
        if released > 0 {
            self.downlink_diag
                .note_downstream_permit_release(released, self.downlink_inflight_permit_bytes());
        }
        released
    }

    fn clear_downlink_inflight_permits(&mut self) -> usize {
        let mut released = 0usize;
        while let Some(mut permit) = self.downlink_inflight_permits.pop_front() {
            released = released.saturating_add(permit.release(usize::MAX));
        }
        if released > 0 {
            self.downlink_diag
                .note_downstream_permit_release(released, 0);
        }
        released
    }

    fn downlink_inflight_permit_bytes(&self) -> usize {
        self.downlink_inflight_permits
            .iter()
            .map(DownstreamBytePermit::remaining_bytes)
            .sum()
    }

    #[cfg(test)]
    fn consume_downlink_pending_prefix(&mut self, bytes: usize) {
        self.release_downlink_permits(bytes);
    }

    fn clear_downlink_pending(&mut self) {
        let outcome = {
            let mut pending = TcpEgressPendingQueueView::new(
                &mut self.downlink_pending,
                &mut self.downlink_pending_permits,
            );
            pending.clear()
        };
        if outcome.released_lease_bytes > 0 {
            self.downlink_diag.note_downstream_permit_release(
                outcome.released_lease_bytes,
                outcome.leased_bytes_after,
            );
        }
        self.clear_downlink_inflight_permits();
        self.downlink_delay_permit_release_until_egress = false;
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct D16OwnedQueueDrain {
    bytes: usize,
    queue_closed: bool,
}

fn d16_queue_has_actor_work(ctx: &SocketCtx) -> bool {
    ctx.d16_downlink_queue
        .as_ref()
        .is_some_and(|queue| queue.snapshot_now().queued_bytes > 0)
}

fn d16_data_ready_matches_ctx(ctx: &SocketCtx, epoch: u64) -> bool {
    ctx.conn_epoch == epoch && ctx.d16_downlink_queue.is_some()
}

fn dirty_contains_d16_flow(
    dirty: &HashSet<SocketHandle>,
    socket_ctxs: &HashMap<SocketHandle, SocketCtx>,
) -> bool {
    dirty.iter().any(|handle| {
        socket_ctxs
            .get(handle)
            .is_some_and(SocketCtx::uses_owned_egress_actor)
    })
}

fn local_egress_actor_enabled(
    runtime_config: &TunRuntimeConfig,
    dirty: &HashSet<SocketHandle>,
    socket_ctxs: &HashMap<SocketHandle, SocketCtx>,
) -> bool {
    runtime_config.d3_egress_actor || dirty_contains_d16_flow(dirty, socket_ctxs)
}

fn arm_d16_tun_rx_ack_barrier(ctx: &mut SocketCtx, snapshot: SocketCloseSnapshot) {
    ctx.d16_tun_rx_ack_barrier |= snapshot.send_queue > 0;
}

fn next_d16_egress_phase_for_snapshot(
    ctx: &mut SocketCtx,
    snapshot: SocketCloseSnapshot,
    downlink_backpressure: DownlinkBackpressureConfig,
    external_hard_pressure: bool,
    drop_debt: bool,
    drain_progress: bool,
) -> EgressPhase {
    let terminal_no_send =
        snapshot.tcp_state == TcpState::Closed && !snapshot.active && !snapshot.can_send;
    // The barrier was armed only after observing a nonzero send queue. Keep
    // that historical drain obligation while a stronger stop condition still
    // dominates; otherwise a hard-pressure zero snapshot can consume the
    // evidence while leaving the phase in DrainOnly forever. The first clean
    // zero snapshot proves positive ACK completion even if cycle-local
    // before/after accounting did not observe the final decrease.
    let ack_completion_progress = ctx.d16_tun_rx_ack_barrier
        && snapshot.send_queue == 0
        && !external_hard_pressure
        && !drop_debt
        && !terminal_no_send;
    if ack_completion_progress {
        ctx.d16_tun_rx_ack_barrier = false;
    }
    transition_egress_phase(
        ctx.d16_egress_phase,
        external_hard_pressure
            || ctx.d16_tun_rx_ack_barrier
            || snapshot.send_queue >= tx_queue_pause_threshold(downlink_backpressure)
            || terminal_no_send,
        drop_debt,
        snapshot.send_queue <= downlink_backpressure.low_bytes,
        drain_progress || ack_completion_progress,
    )
}

#[allow(clippy::too_many_arguments)]
fn transition_dirty_d16_egress_phases(
    dirty: &HashSet<SocketHandle>,
    sockets: &SocketSet<'_>,
    socket_ctxs: &mut HashMap<SocketHandle, SocketCtx>,
    downlink_backpressure: DownlinkBackpressureConfig,
    hard_pressure: bool,
    tun_rx_backlog_active: bool,
    drop_debt: bool,
    completed_drain_bytes: Option<usize>,
) {
    for handle in dirty {
        let snapshot = SocketCloseSnapshot::from_socket(sockets.get::<TcpSocket>(*handle));
        let Some(ctx) = socket_ctxs.get_mut(handle) else {
            continue;
        };
        if !ctx.uses_owned_egress_actor() {
            continue;
        }
        let previous = ctx.d16_egress_phase;
        let next = next_d16_egress_phase_for_snapshot(
            ctx,
            snapshot,
            downlink_backpressure,
            hard_pressure || tun_rx_backlog_active,
            drop_debt,
            completed_drain_bytes.is_some_and(|bytes| bytes > 0),
        );
        ctx.downlink_diag
            .note_d16_egress_phase(previous, next, completed_drain_bytes);
        ctx.d16_egress_phase = next;
    }
}

fn apply_d16_global_recovery_evidence(
    dirty: &mut HashSet<SocketHandle>,
    sockets: &SocketSet<'_>,
    socket_ctxs: &mut HashMap<SocketHandle, SocketCtx>,
    downlink_backpressure: DownlinkBackpressureConfig,
    evidence: D16GlobalRecoveryEvidence,
) -> usize {
    if !evidence.clean_drain || evidence.observed_drain_bytes == 0 {
        return 0;
    }

    let handles: Vec<SocketHandle> = socket_ctxs
        .iter()
        .filter_map(|(handle, ctx)| {
            (ctx.uses_owned_egress_actor() && ctx.d16_egress_phase == EgressPhase::DrainOnly)
                .then_some(*handle)
        })
        .collect();
    let mut transitioned = 0usize;
    for handle in handles {
        let snapshot = SocketCloseSnapshot::from_socket(sockets.get::<TcpSocket>(handle));
        let Some(ctx) = socket_ctxs.get_mut(&handle) else {
            continue;
        };
        let previous = ctx.d16_egress_phase;
        let next = next_d16_egress_phase_for_snapshot(
            ctx,
            snapshot,
            downlink_backpressure,
            false,
            false,
            true,
        );
        ctx.downlink_diag
            .note_d16_egress_phase(previous, next, None);
        ctx.d16_egress_phase = next;
        if next != previous {
            transitioned = transitioned.saturating_add(1);
            dirty.insert(handle);
        }
    }
    transitioned
}

fn force_all_d16_flows_to_drain_only_for_tun_rx_backlog(
    dirty: &mut HashSet<SocketHandle>,
    sockets: &SocketSet<'_>,
    socket_ctxs: &mut HashMap<SocketHandle, SocketCtx>,
    downlink_backpressure: DownlinkBackpressureConfig,
    drop_debt: bool,
) -> usize {
    let handles: Vec<SocketHandle> = socket_ctxs
        .iter()
        .filter_map(|(handle, ctx)| ctx.uses_owned_egress_actor().then_some(*handle))
        .collect();
    let mut transitioned = 0usize;
    for handle in handles {
        let snapshot = SocketCloseSnapshot::from_socket(sockets.get::<TcpSocket>(handle));
        let Some(ctx) = socket_ctxs.get_mut(&handle) else {
            continue;
        };
        arm_d16_tun_rx_ack_barrier(ctx, snapshot);
        let previous = ctx.d16_egress_phase;
        let next = next_d16_egress_phase_for_snapshot(
            ctx,
            snapshot,
            downlink_backpressure,
            true,
            drop_debt,
            false,
        );
        ctx.downlink_diag
            .note_d16_egress_phase(previous, next, None);
        ctx.d16_egress_phase = next;
        dirty.insert(handle);
        if next != previous {
            transitioned = transitioned.saturating_add(1);
        }
    }
    transitioned
}

fn recover_all_eligible_d16_flows_after_clean_tun_rx_edge(
    dirty: &mut HashSet<SocketHandle>,
    sockets: &SocketSet<'_>,
    socket_ctxs: &mut HashMap<SocketHandle, SocketCtx>,
    downlink_backpressure: DownlinkBackpressureConfig,
    drop_debt: bool,
) -> usize {
    let handles: Vec<SocketHandle> = socket_ctxs
        .iter()
        .filter_map(|(handle, ctx)| {
            (ctx.uses_owned_egress_actor() && ctx.d16_egress_phase == EgressPhase::DrainOnly)
                .then_some(*handle)
        })
        .collect();
    let mut transitioned = 0usize;
    for handle in handles {
        let snapshot = SocketCloseSnapshot::from_socket(sockets.get::<TcpSocket>(handle));
        let Some(ctx) = socket_ctxs.get_mut(&handle) else {
            continue;
        };
        let previous = ctx.d16_egress_phase;
        let next = next_d16_egress_phase_for_snapshot(
            ctx,
            snapshot,
            downlink_backpressure,
            false,
            drop_debt,
            true,
        );
        ctx.downlink_diag
            .note_d16_egress_phase(previous, next, None);
        ctx.d16_egress_phase = next;
        if next != previous {
            transitioned = transitioned.saturating_add(1);
            dirty.insert(handle);
        }
    }
    transitioned
}

fn drain_d16_owned_queue_into_pending(ctx: &mut SocketCtx, max_bytes: usize) -> D16OwnedQueueDrain {
    let Some(queue) = ctx.d16_downlink_queue.clone() else {
        return D16OwnedQueueDrain::default();
    };
    let mut outcome = D16OwnedQueueDrain::default();
    while outcome.bytes < max_bytes {
        match queue.try_recv_up_to(max_bytes.saturating_sub(outcome.bytes)) {
            LeasedQueuePoll::Data(leased) => {
                let (bytes, permit) = leased.into_parts();
                outcome.bytes = outcome.bytes.saturating_add(bytes.len());
                ctx.append_downlink_pending_with_permit(&bytes, Some(permit));
            }
            LeasedQueuePoll::Empty => break,
            LeasedQueuePoll::Closed => {
                outcome.queue_closed = true;
                break;
            }
        }
    }
    if !outcome.queue_closed && matches!(queue.try_recv_up_to(0), LeasedQueuePoll::Closed) {
        outcome.queue_closed = true;
    }
    outcome
}

fn drop_d16_owned_for_terminal(
    ctx: &mut SocketCtx,
    direction: &'static str,
    reason: &'static str,
) -> usize {
    let Some(queue) = ctx.d16_downlink_queue.clone() else {
        return 0;
    };
    let before = queue.snapshot_now();
    let dropped_queued_bytes = queue.close_and_drop_queued_for_terminal(direction, reason);
    ctx.clear_downlink_pending();
    let after = queue.snapshot_now();
    let released_leased_bytes = before.leased_bytes.saturating_sub(after.leased_bytes);
    let dropped = dropped_queued_bytes.saturating_add(released_leased_bytes);
    if dropped > 0 {
        ctx.downlink_diag.permit_terminal_drop_bytes = ctx
            .downlink_diag
            .permit_terminal_drop_bytes
            .saturating_add(dropped as u64);
        ctx.downlink_diag.permit_terminal_drop_events = ctx
            .downlink_diag
            .permit_terminal_drop_events
            .saturating_add(1);
    }
    dropped
}

fn d16_terminal_cause_for_rearm(
    ctx: &SocketCtx,
    snapshot: SocketCloseSnapshot,
    close_direction: &'static str,
    close_reason: &'static str,
) -> Option<(&'static str, &'static str)> {
    if close_reason == "remote_eof" && ctx.local_eof_sent {
        return None;
    }
    if snapshot.tcp_state == TcpState::Closed && !snapshot.active && !snapshot.can_send {
        return Some(("local_to_remote", "local_socket_terminal"));
    }
    Some((close_direction, close_reason))
}

#[cfg(feature = "harness")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct D16HarnessCycle {
    pub admitted_bytes: usize,
    pub drained_bytes: usize,
    pub queue_to_pending_bytes: usize,
    pub phase: EgressPhase,
    pub allow_read: bool,
    pub allow_admission: bool,
}

#[cfg(feature = "harness")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct D16HarnessFlowSnapshot {
    pub queue: crate::tcp_downlink_pump::LeasedByteQueueSnapshot,
    pub pending_bytes: usize,
    pub inflight_bytes: usize,
    pub actor_admitted_bytes: u64,
    pub actor_bypass_admitted_bytes: u64,
    pub committed_tun_bytes: u64,
    pub phase: EgressPhase,
    pub eof_ready: bool,
}

#[cfg(feature = "harness")]
pub(crate) struct D16HarnessFlow {
    ctx: SocketCtx,
    queue: AsyncLeasedByteFlowQueue,
    committed_tun_bytes: u64,
}

#[cfg(feature = "harness")]
impl D16HarnessFlow {
    pub(crate) fn new(global_budget: D16GlobalByteBudget) -> Result<Self, ByteQueueConfigError> {
        let queue = AsyncLeasedByteFlowQueue::new_with_global_budget(
            512 * 1024,
            DownstreamPermitReleaseMode::OnEgressDrain,
            global_budget,
        )?;
        let mut ctx = SocketCtx::new(443);
        ctx.state = SocketState::Relaying;
        ctx.d16_downlink_queue = Some(queue.clone());
        Ok(Self {
            ctx,
            queue,
            committed_tun_bytes: 0,
        })
    }

    pub(crate) fn permissions(&self) -> EgressPermissions {
        EgressPermissions::for_phase(self.ctx.d16_egress_phase)
    }

    pub(crate) async fn enqueue_remote_chunk(
        &self,
        bytes: usize,
    ) -> Result<bool, ByteQueuePushError> {
        let reservation = self
            .queue
            .reserve_read_up_to(bytes)
            .await
            .map_err(|_| ByteQueuePushError::Closed)?;
        if reservation.max_len() != bytes {
            return Err(ByteQueuePushError::Full {
                len: bytes,
                available: reservation.max_len(),
            });
        }
        reservation.commit(Bytes::from(vec![0; bytes]))
    }

    pub(crate) async fn close_remote(&self) -> bool {
        self.queue.close_for_remote_eof_and_mark_ready().await
    }

    pub(crate) fn cycle(
        &mut self,
        hard_pressure: bool,
        drop_debt: bool,
        tun_drain_bytes: usize,
    ) -> D16HarnessCycle {
        let drained_bytes = self.ctx.release_downlink_inflight_permits(tun_drain_bytes);
        self.committed_tun_bytes = self
            .committed_tun_bytes
            .saturating_add(drained_bytes as u64);

        let previous = self.ctx.d16_egress_phase;
        let before_permissions = EgressPermissions::for_phase(previous);
        let before = self.queue.snapshot_now();
        let below_low = self.ctx.downlink_inflight_permit_bytes() <= 128 * 1024;
        let phase = transition_egress_phase(previous, hard_pressure, drop_debt, below_low, false);
        self.ctx
            .downlink_diag
            .note_d16_egress_phase(previous, phase, None);
        self.ctx.d16_egress_phase = phase;
        let permissions = EgressPermissions::for_phase(phase);

        let mut queue_to_pending_bytes = 0usize;
        let mut admitted_bytes = 0usize;
        if permissions.allow_admission {
            let drained = drain_d16_owned_queue_into_pending(
                &mut self.ctx,
                permissions.max_admit_quantum_bytes,
            );
            queue_to_pending_bytes = drained.bytes;
            let outcome = with_downlink_admission(
                &mut self.ctx,
                DownlinkAdmissionOrigin::EgressActor,
                |ctx, _| {
                    let accepted = ctx
                        .downlink_pending
                        .len()
                        .min(permissions.max_admit_quantum_bytes);
                    ctx.admit_downlink_pending_prefix(accepted);
                    if accepted > 0 {
                        ctx.downlink_diag
                            .note_send_slice_ok(accepted, ctx.downlink_pending.len());
                    }
                    DownlinkFlushOutcome {
                        accepted_bytes: accepted,
                        headroom_limited: false,
                    }
                },
            );
            admitted_bytes = outcome.accepted_bytes;
        }
        let _ = install_d16_remote_eof_if_ready(&mut self.ctx, 0);

        let previous = self.ctx.d16_egress_phase;
        let next = transition_egress_phase(previous, hard_pressure, drop_debt, below_low, true);
        self.ctx
            .downlink_diag
            .note_d16_egress_phase(previous, next, Some(drained_bytes));
        self.ctx.d16_egress_phase = next;
        debug_assert!(before.owned_bytes() <= 512 * 1024);
        debug_assert!(before_permissions.allow_drain);

        D16HarnessCycle {
            admitted_bytes,
            drained_bytes,
            queue_to_pending_bytes,
            phase: next,
            allow_read: permissions.allow_read,
            allow_admission: permissions.allow_admission,
        }
    }

    pub(crate) fn snapshot(&self) -> D16HarnessFlowSnapshot {
        let queue = self.queue.snapshot_now();
        let (queue_closed, queue_empty) = d16_remote_eof_queue_state(&self.ctx).unwrap_or_default();
        D16HarnessFlowSnapshot {
            queue,
            pending_bytes: self.ctx.downlink_pending.len(),
            inflight_bytes: self.ctx.downlink_inflight_permit_bytes(),
            actor_admitted_bytes: self.ctx.downlink_diag.actor_admitted_bytes,
            actor_bypass_admitted_bytes: self.ctx.downlink_diag.actor_bypass_admitted_bytes,
            committed_tun_bytes: self.committed_tun_bytes,
            phase: self.ctx.d16_egress_phase,
            eof_ready: remote_eof_ready_for_local_close(
                queue_closed,
                queue_empty,
                self.ctx.downlink_pending.len(),
                self.ctx.downlink_inflight_permit_bytes(),
            ),
        }
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
    result: Result<OpenedOwnedTcp, ClientError>,
}

async fn publish_handshake_done(sender: &mpsc::Sender<HandshakeNotice>, done: HandshakeDone) {
    if let Err(error) = sender.send(HandshakeNotice::Legacy(done)).await {
        let HandshakeNotice::Legacy(HandshakeDone { result, .. }) = error.0 else {
            return;
        };
        if let Ok(opened) = result {
            abandon_uninstalled_owned_tcp(opened, "handshake_receiver_closed");
        }
    }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DownlinkFlushPolicy {
    LegacyCredit,
    #[cfg(test)]
    CleanHeadroomOnly,
    ActorHybridCredit,
    ActorAdaptiveCredit,
    ActorCapacityCredit,
}

impl DownlinkFlushPolicy {
    fn allows_drain_credit(self) -> bool {
        match self {
            Self::LegacyCredit => true,
            Self::ActorHybridCredit => true,
            Self::ActorAdaptiveCredit => true,
            Self::ActorCapacityCredit => true,
            #[cfg(test)]
            Self::CleanHeadroomOnly => false,
        }
    }

    fn allows_close_drain_expansion(self) -> bool {
        match self {
            Self::LegacyCredit => true,
            Self::ActorHybridCredit => true,
            #[cfg(test)]
            Self::CleanHeadroomOnly => false,
            Self::ActorAdaptiveCredit | Self::ActorCapacityCredit => false,
        }
    }

    fn credit_limit_edge(self, cfg: DownlinkBackpressureConfig) -> usize {
        match self {
            Self::LegacyCredit => tx_queue_credit_spend_threshold(cfg),
            #[cfg(test)]
            Self::CleanHeadroomOnly => tx_queue_flush_threshold(cfg),
            Self::ActorHybridCredit => tx_queue_egress_target_threshold(cfg),
            Self::ActorAdaptiveCredit => tx_queue_credit_spend_threshold(cfg),
            Self::ActorCapacityCredit => tx_queue_egress_target_threshold(cfg),
        }
    }

    fn hard_headroom(
        self,
        cfg: DownlinkBackpressureConfig,
        send_window: TcpSendWindowSnapshot,
    ) -> usize {
        match self {
            Self::ActorCapacityCredit => {
                let guard = tx_queue_credit_guard_bytes(cfg).min(send_window.send_capacity);
                let capacity_headroom = send_window.send_capacity.saturating_sub(guard);
                capacity_headroom.min(
                    tx_queue_egress_target_threshold(cfg).saturating_sub(send_window.send_queue),
                )
            }
            _ => tx_queue_pause_threshold(cfg).saturating_sub(send_window.send_queue),
        }
    }
}

fn downlink_flush_policy_for_ctx(ctx: &SocketCtx) -> DownlinkFlushPolicy {
    if ctx.downlink_d3_actor_flow {
        if ctx.downlink_d5_capacity_backpressure {
            DownlinkFlushPolicy::ActorCapacityCredit
        } else if ctx.downlink_d3_actor_legacy_credit {
            DownlinkFlushPolicy::LegacyCredit
        } else if ctx.downlink_d3_actor_adaptive_credit {
            DownlinkFlushPolicy::ActorAdaptiveCredit
        } else {
            DownlinkFlushPolicy::ActorHybridCredit
        }
    } else {
        DownlinkFlushPolicy::LegacyCredit
    }
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

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct D16GlobalDropEpisode {
    active: bool,
    pressure_baseline_bytes: usize,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct D16GlobalRecoveryEvidence {
    observed_drain_bytes: usize,
    debt_paid_bytes: usize,
    clean_drain: bool,
}

fn apply_d16_global_tun_feedback(
    episode: &mut D16GlobalDropEpisode,
    event: Option<TunEgressFeedbackEvent>,
    cfg: DownlinkBackpressureConfig,
    debt: &mut DownlinkEgressDropDebt,
) -> D16GlobalRecoveryEvidence {
    match event {
        Some(TunEgressFeedbackEvent {
            reason: TunEgressFeedbackReason::DropDelta,
            total_pressure,
            ..
        }) => {
            debt.note_tun_drop(cfg);
            episode.active = true;
            episode.pressure_baseline_bytes = episode.pressure_baseline_bytes.max(total_pressure);
        }
        Some(TunEgressFeedbackEvent {
            reason: TunEgressFeedbackReason::PressureLow,
            total_pressure,
            ..
        }) if episode.active => {
            let observed_drain_bytes = episode
                .pressure_baseline_bytes
                .saturating_sub(total_pressure);
            episode.active = false;
            episode.pressure_baseline_bytes = total_pressure;
            let payment = debt.pay(observed_drain_bytes);
            return D16GlobalRecoveryEvidence {
                observed_drain_bytes,
                debt_paid_bytes: payment.total(),
                clean_drain: observed_drain_bytes > 0 && !debt.has_active_drop_debt(),
            };
        }
        _ => {}
    }
    D16GlobalRecoveryEvidence::default()
}

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

#[cfg(test)]
fn bounded_downlink_flush_limit_for_window_with_clock_and_drop(
    pending_len: usize,
    max_bytes_per_flush: usize,
    send_window: TcpSendWindowSnapshot,
    cfg: DownlinkBackpressureConfig,
    clock: &mut DownlinkEgressClock,
    drop_debt: &mut DownlinkEgressDropDebt,
) -> DownlinkFlushLimit {
    bounded_downlink_flush_limit_for_window_with_clock_drop_and_policy(
        pending_len,
        max_bytes_per_flush,
        send_window,
        cfg,
        clock,
        drop_debt,
        DownlinkFlushPolicy::LegacyCredit,
    )
}

fn bounded_downlink_flush_limit_for_window_with_clock_drop_and_policy(
    pending_len: usize,
    max_bytes_per_flush: usize,
    send_window: TcpSendWindowSnapshot,
    cfg: DownlinkBackpressureConfig,
    clock: &mut DownlinkEgressClock,
    drop_debt: &mut DownlinkEgressDropDebt,
    policy: DownlinkFlushPolicy,
) -> DownlinkFlushLimit {
    let budget_len = bounded_downlink_flush_len(pending_len, max_bytes_per_flush);
    let socket_len = budget_len.min(send_window.send_capacity);
    let clean_headroom = tx_queue_flush_threshold(cfg).saturating_sub(send_window.send_queue);
    let target_edge = tx_queue_egress_target_threshold(cfg);
    let credit_observation = clock.note_observed_send_queue(send_window.send_queue, cfg, drop_debt);
    let hard_credit_blocked = drop_debt.has_active_drop_debt();
    let hard_limit_edge = if hard_credit_blocked {
        target_edge
    } else {
        policy.credit_limit_edge(cfg)
    };
    let hard_headroom = if hard_credit_blocked {
        tx_queue_pause_threshold(cfg).saturating_sub(send_window.send_queue)
    } else {
        policy.hard_headroom(cfg, send_window)
    };
    let hard_edge = send_window.send_queue.saturating_add(hard_headroom);
    let hard_edge_guard = hard_edge.saturating_sub(hard_limit_edge);
    let guarded_hard_headroom =
        if matches!(policy, DownlinkFlushPolicy::ActorCapacityCredit) && !hard_credit_blocked {
            hard_headroom
        } else {
            hard_limit_edge.saturating_sub(send_window.send_queue)
        };
    let usable_credit = if hard_credit_blocked || !policy.allows_drain_credit() {
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

impl DownlinkFlushOutcome {
    const NO_ADMISSION: Self = Self {
        accepted_bytes: 0,
        headroom_limited: false,
    };
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DownlinkAdmissionOrigin {
    Legacy,
    EgressActor,
    ControlOnly,
}

impl DownlinkAdmissionOrigin {
    fn allows_d16_admission(self) -> bool {
        matches!(self, Self::EgressActor)
    }

    fn for_flow(self, d16_flow: bool) -> Self {
        if d16_flow { self } else { Self::Legacy }
    }
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
    let flush_policy = downlink_flush_policy_for_ctx(ctx);
    let mut flush_limit = bounded_downlink_flush_limit_for_window_with_clock_drop_and_policy(
        ctx.downlink_pending.len(),
        controlled_max_bytes_per_flush,
        send_window,
        downlink_backpressure,
        &mut ctx.downlink_egress_clock,
        downlink_egress_drop_debt,
        flush_policy,
    );
    if ctx.pending_relay_close.is_some()
        && !ctx.local_eof_sent
        && flush_policy.allows_close_drain_expansion()
    {
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
            ctx.admit_downlink_pending_prefix(n);
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
            ctx.clear_downlink_pending();
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

/// Knife16 application-ACK boundary for protocol-owned downlink DATA.
///
/// A `SinkDelivery` never enters the legacy byte vector or D16 permit path.
/// Its opaque completion is retained until this exact `send_slice` call says
/// how many bytes smoltcp accepted. A positive partial write destroys all old
/// suffix views; the reducer must issue a fresh offer for the remainder.
fn flush_resumable_sink(
    handle: SocketHandle,
    tcp_socket: &mut TcpSocket<'_>,
    ctx: &mut SocketCtx,
    max_bytes_per_flush: usize,
    downlink_backpressure: DownlinkBackpressureConfig,
    tun_mtu: usize,
    downlink_egress_drop_debt: &mut DownlinkEgressDropDebt,
) -> DownlinkFlushOutcome {
    let Some(delivery) = ctx.resumable_sink.take() else {
        return DownlinkFlushOutcome::NO_ADMISSION;
    };
    if !delivery.is_live() {
        // The reducer terminal won the lifecycle race before any local socket
        // mutation. The exact terminal action is already queued on the
        // independent lane; suppress uplink service until it arrives.
        ctx.owned_terminal_pending = true;
        return DownlinkFlushOutcome::NO_ADMISSION;
    }
    let offered = delivery.offered_len();
    let first_segment_len = delivery.first_segment_slice().len();
    let send_window = TcpSendWindowSnapshot::from_socket(tcp_socket);
    ctx.downlink_diag.note_send_window(send_window, offered);
    if !send_window.can_send {
        ctx.downlink_diag
            .note_flush_attempt(offered, max_bytes_per_flush);
        ctx.downlink_diag.note_no_send_capacity(offered);
        ctx.resumable_sink = Some(delivery);
        return DownlinkFlushOutcome::NO_ADMISSION;
    }
    let controlled_max_bytes_per_flush = ctx.downlink_credit_controller.flush_budget(
        max_bytes_per_flush,
        downlink_backpressure,
        tun_mtu,
    );
    let flush_policy = downlink_flush_policy_for_ctx(ctx);
    let flush_limit = bounded_downlink_flush_limit_for_window_with_clock_drop_and_policy(
        first_segment_len,
        controlled_max_bytes_per_flush,
        send_window,
        downlink_backpressure,
        &mut ctx.downlink_egress_clock,
        downlink_egress_drop_debt,
        flush_policy,
    );
    ctx.downlink_diag
        .note_flush_limit(offered, controlled_max_bytes_per_flush, flush_limit);
    if flush_limit.len == 0 {
        let used =
            ctx.downlink_egress_clock
                .note_flush_result(send_window.send_queue, 0, flush_limit);
        ctx.downlink_diag.note_drain_credit_used(used);
        ctx.downlink_credit_controller
            .note_flush_feedback_with_accepted(flush_limit, 0, downlink_backpressure, tun_mtu);
        ctx.resumable_sink = Some(delivery);
        return DownlinkFlushOutcome {
            accepted_bytes: 0,
            headroom_limited: flush_limit.headroom_limited,
        };
    }

    let write_result = match delivery
        .with_live_first_segment(|segment| tcp_socket.send_slice(&segment[..flush_limit.len]))
    {
        Ok(result) => result,
        Err(FlowPortError::SinkCapabilityRevoked) => {
            ctx.owned_terminal_pending = true;
            return DownlinkFlushOutcome::NO_ADMISSION;
        }
        Err(error) => {
            tcp_diag_log!(
                "🔎 owned-sink-live-guard-error handle={:?} offered={} error={error}",
                handle,
                offered
            );
            ctx.state = SocketState::Closing;
            return DownlinkFlushOutcome::NO_ADMISSION;
        }
    };

    match write_result {
        Ok(0) => {
            let used =
                ctx.downlink_egress_clock
                    .note_flush_result(send_window.send_queue, 0, flush_limit);
            ctx.downlink_diag.note_drain_credit_used(used);
            ctx.downlink_diag.note_send_slice_ok(0, offered);
            ctx.downlink_credit_controller
                .note_flush_feedback_with_accepted(flush_limit, 0, downlink_backpressure, tun_mtu);
            ctx.resumable_sink = delivery.complete_write(0).ok().flatten();
            DownlinkFlushOutcome {
                accepted_bytes: 0,
                headroom_limited: flush_limit.headroom_limited,
            }
        }
        Ok(accepted) => {
            let used = ctx.downlink_egress_clock.note_flush_result(
                send_window.send_queue,
                accepted,
                flush_limit,
            );
            ctx.downlink_diag.note_drain_credit_used(used);
            ctx.downlink_diag
                .note_send_slice_ok(accepted, offered.saturating_sub(accepted));
            ctx.downlink_credit_controller
                .note_flush_feedback_with_accepted(
                    flush_limit,
                    accepted,
                    downlink_backpressure,
                    tun_mtu,
                );
            if let Err(error) = delivery.complete_write(accepted) {
                tcp_diag_log!(
                    "🔎 owned-sink-completion-error handle={:?} accepted={} error={error}",
                    handle,
                    accepted
                );
                ctx.state = SocketState::Closing;
            }
            DownlinkFlushOutcome {
                accepted_bytes: accepted,
                headroom_limited: flush_limit.headroom_limited,
            }
        }
        Err(_) => {
            let used =
                ctx.downlink_egress_clock
                    .note_flush_result(send_window.send_queue, 0, flush_limit);
            ctx.downlink_diag.note_drain_credit_used(used);
            ctx.downlink_diag.note_send_slice_error();
            ctx.downlink_credit_controller
                .note_flush_feedback_with_accepted(flush_limit, 0, downlink_backpressure, tun_mtu);
            tcp_diag_log!(
                "🔎 owned-sink-send-slice-error handle={:?} offered={} state={:?} → abandon",
                handle,
                offered,
                ctx.state
            );
            delivery.abandon();
            ctx.state = SocketState::Closing;
            DownlinkFlushOutcome {
                accepted_bytes: 0,
                headroom_limited: flush_limit.headroom_limited,
            }
        }
    }
}

/// Apply peer FIN only after the reducer has observed acceptance of every
/// preceding byte. `HalfCloseDelivery` reports success only after the local
/// smoltcp state transition is actually invoked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OwnedHalfCloseDisposition {
    WaitForSink,
    Apply,
    Fail,
}

fn owned_half_close_disposition(
    snapshot: SocketCloseSnapshot,
    sink_pending: bool,
    local_eof_sent: bool,
) -> OwnedHalfCloseDisposition {
    if sink_pending {
        return OwnedHalfCloseDisposition::WaitForSink;
    }
    // `can_send` includes tx-buffer capacity. A full buffer does not prevent
    // smoltcp from queuing the FIN state transition; `may_send` is the exact
    // transmit-half lifecycle authority.
    if snapshot.active && snapshot.may_send && !local_eof_sent {
        OwnedHalfCloseDisposition::Apply
    } else {
        OwnedHalfCloseDisposition::Fail
    }
}

fn apply_resumable_half_close(tcp_socket: &mut TcpSocket<'_>, ctx: &mut SocketCtx) -> bool {
    let snapshot = SocketCloseSnapshot::from_socket(tcp_socket);
    if owned_half_close_disposition(snapshot, ctx.resumable_sink.is_some(), ctx.local_eof_sent)
        == OwnedHalfCloseDisposition::WaitForSink
    {
        return false;
    }
    let Some(completion) = ctx.resumable_half_close.take() else {
        return false;
    };
    if !completion.is_live() {
        ctx.owned_terminal_pending = true;
        return false;
    }
    match owned_half_close_disposition(snapshot, false, ctx.local_eof_sent) {
        OwnedHalfCloseDisposition::Apply => {
            match completion.with_live(|| tcp_socket.close()) {
                Ok(()) => {}
                Err(FlowPortError::SinkCapabilityRevoked) => {
                    ctx.owned_terminal_pending = true;
                    return false;
                }
                Err(error) => {
                    tcp_diag_log!("🔎 owned-half-close-live-guard-error error={error}");
                    completion.fail();
                    ctx.state = SocketState::Closing;
                    return false;
                }
            }
            ctx.local_eof_sent = true;
            completion.succeed();
            true
        }
        OwnedHalfCloseDisposition::Fail | OwnedHalfCloseDisposition::WaitForSink => {
            completion.fail();
            ctx.state = SocketState::Closing;
            false
        }
    }
}

fn with_downlink_admission<F>(
    ctx: &mut SocketCtx,
    origin: DownlinkAdmissionOrigin,
    admit: F,
) -> DownlinkFlushOutcome
where
    F: FnOnce(&mut SocketCtx, bool) -> DownlinkFlushOutcome,
{
    let actor_flow = ctx.uses_owned_egress_actor();
    let queue_flow = ctx.d16_downlink_queue.is_some();
    let origin = origin.for_flow(actor_flow);
    if actor_flow && !origin.allows_d16_admission() {
        return DownlinkFlushOutcome::NO_ADMISSION;
    }
    let outcome = admit(ctx, queue_flow);
    ctx.downlink_diag
        .note_d16_admission(origin, outcome.accepted_bytes, actor_flow);
    outcome
}

#[allow(clippy::too_many_arguments)]
fn admit_downlink_for_handle(
    handle: SocketHandle,
    tcp_socket: &mut TcpSocket,
    ctx: &mut SocketCtx,
    max_bytes_per_flush: usize,
    downlink_backpressure: DownlinkBackpressureConfig,
    tun_mtu: usize,
    downlink_egress_drop_debt: &mut DownlinkEgressDropDebt,
    origin: DownlinkAdmissionOrigin,
) -> DownlinkFlushOutcome {
    let existing_send_queue_bytes = tcp_socket.send_queue();
    let (origin, max_bytes_per_flush) = if ctx.uses_owned_egress_actor() {
        let permissions = EgressPermissions::for_phase(ctx.d16_egress_phase);
        (
            if permissions.allow_admission {
                origin
            } else {
                DownlinkAdmissionOrigin::ControlOnly
            },
            max_bytes_per_flush.min(
                permissions.available_admit_bytes_for_tun_mtu(tun_mtu, existing_send_queue_bytes),
            ),
        )
    } else {
        (origin, max_bytes_per_flush)
    };
    with_downlink_admission(ctx, origin, |ctx, d16_flow| {
        if d16_flow {
            let _ = drain_d16_owned_queue_into_pending(ctx, max_bytes_per_flush);
        }
        if ctx.resumable_sink.is_some() {
            flush_resumable_sink(
                handle,
                tcp_socket,
                ctx,
                max_bytes_per_flush,
                downlink_backpressure,
                tun_mtu,
                downlink_egress_drop_debt,
            )
        } else {
            flush_downlink(
                handle,
                tcp_socket,
                ctx,
                max_bytes_per_flush,
                downlink_backpressure,
                tun_mtu,
                downlink_egress_drop_debt,
            )
        }
    })
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

/// fake-IP 映射回收 TTL（秒）：idle 且 refcount==0 超此时长才回收。
/// 中文要点：远大于 DNS A 记录 TTL（5s）。review #4：取 30min（而非 300s），给「应用已解析并缓存
/// fake-IP、但尚未发起连接」留足窗口——这段 refcount==0，过早回收会让后续用缓存 IP 的连接被 Refuse。
/// 活跃 flow（refcount>0）任何情况都不回收（见 FakeIpPool::sweep）。
const FAKE_IP_TTL: u64 = 1800;

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

fn should_install_downlink_pressure_credit_debt_for_runtime(
    d3_egress_actor: bool,
    d3_egress_actor_legacy_credit: bool,
    buffered_downlink_enabled: bool,
    was_paused: bool,
    is_paused: bool,
    raw_stats: DownlinkPressureStats,
    cfg: DownlinkBackpressureConfig,
) -> bool {
    (!d3_egress_actor || d3_egress_actor_legacy_credit)
        && !buffered_downlink_enabled
        && should_install_downlink_pressure_credit_debt(was_paused, is_paused, raw_stats, cfg)
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
        stats.note_pending(ctx.pending_downlink_bytes());
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
        stats.note_pending(ctx.pending_downlink_bytes());
    }
    for (handle, socket) in sockets.iter() {
        if let smoltcp::socket::Socket::Tcp(tcp_socket) = socket {
            let queued = tcp_socket.send_queue();
            if queued > 0 && dirty.contains(&handle) && socket_ctxs.contains_key(&handle) {
                stats.note_tx_queue(queued);
            }
        }
    }
    stats
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
    downstream_permit_bytes: u64,
    downstream_permit_released_bytes: u64,
    downstream_permit_pending_high: usize,
    terminal_late_remote_payload_bytes: u64,
    terminal_late_remote_payload_events: u64,
    flush_attempts: u64,
    send_slice_no_capacity: u64,
    send_slice_calls: u64,
    send_slice_accepted_bytes: u64,
    actor_admitted_bytes: u64,
    actor_bypass_admitted_bytes: u64,
    d16_running_flows: usize,
    d16_drain_only_flows: usize,
    d16_recovery_flows: usize,
    d16_egress_phase_transitions: u64,
    d16_recovery_resets: u64,
    d16_drain_only_cycles: u64,
    d16_drain_only_drain_bytes: u64,
    permit_terminal_drop_bytes: u64,
    permit_terminal_drop_events: u64,
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
        let pending = ctx.pending_downlink_bytes();
        aggregate.pending_total = aggregate.pending_total.saturating_add(pending);
        aggregate.pending_max = aggregate.pending_max.max(pending);
        aggregate.pending_high = aggregate
            .pending_high
            .max(ctx.downlink_diag.downlink_pending_high_water);
        aggregate.remote_to_global_rx_bytes = aggregate
            .remote_to_global_rx_bytes
            .saturating_add(ctx.downlink_diag.remote_to_global_rx_bytes);
        aggregate.downstream_permit_bytes = aggregate
            .downstream_permit_bytes
            .saturating_add(ctx.downlink_diag.downstream_permit_bytes);
        aggregate.downstream_permit_released_bytes = aggregate
            .downstream_permit_released_bytes
            .saturating_add(ctx.downlink_diag.downstream_permit_released_bytes);
        aggregate.downstream_permit_pending_high = aggregate
            .downstream_permit_pending_high
            .max(ctx.downlink_diag.downstream_permit_pending_high_water);
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
        aggregate.actor_admitted_bytes = aggregate
            .actor_admitted_bytes
            .saturating_add(ctx.downlink_diag.actor_admitted_bytes);
        aggregate.actor_bypass_admitted_bytes = aggregate
            .actor_bypass_admitted_bytes
            .saturating_add(ctx.downlink_diag.actor_bypass_admitted_bytes);
        if ctx.d16_downlink_queue.is_some() {
            match ctx.d16_egress_phase {
                EgressPhase::Running => {
                    aggregate.d16_running_flows = aggregate.d16_running_flows.saturating_add(1);
                }
                EgressPhase::DrainOnly => {
                    aggregate.d16_drain_only_flows =
                        aggregate.d16_drain_only_flows.saturating_add(1);
                }
                EgressPhase::Recovery { .. } => {
                    aggregate.d16_recovery_flows = aggregate.d16_recovery_flows.saturating_add(1);
                }
            }
        }
        aggregate.d16_egress_phase_transitions = aggregate
            .d16_egress_phase_transitions
            .saturating_add(ctx.downlink_diag.d16_egress_phase_transitions);
        aggregate.d16_recovery_resets = aggregate
            .d16_recovery_resets
            .saturating_add(ctx.downlink_diag.d16_recovery_resets);
        aggregate.d16_drain_only_cycles = aggregate
            .d16_drain_only_cycles
            .saturating_add(ctx.downlink_diag.d16_drain_only_cycles);
        aggregate.d16_drain_only_drain_bytes = aggregate
            .d16_drain_only_drain_bytes
            .saturating_add(ctx.downlink_diag.d16_drain_only_drain_bytes);
        aggregate.permit_terminal_drop_bytes = aggregate
            .permit_terminal_drop_bytes
            .saturating_add(ctx.downlink_diag.permit_terminal_drop_bytes);
        aggregate.permit_terminal_drop_events = aggregate
            .permit_terminal_drop_events
            .saturating_add(ctx.downlink_diag.permit_terminal_drop_events);
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
        "🔎 tcp-downlink-flush pending_total={} pending_max={} pending_high={} remote_to_global_rx_bytes={} downstream_permit_bytes={} downstream_permit_released_bytes={} downstream_permit_pending_high={} terminal_late_remote_payload_bytes={} terminal_late_remote_payload_events={} flush_attempts={} no_send_capacity={} send_window_samples={} send_capacity_min={} send_capacity_max={} send_queue_max={} recv_queue_max={} may_send_false={} may_recv_false={} no_send_capacity_streak_max={} no_send_capacity_pending_max={} send_slice_calls={} send_slice_accepted={} actor_admitted_bytes={} actor_bypass_admitted_bytes={} permit_terminal_drop_bytes={} permit_terminal_drop_events={} d16_running_flows={} d16_drain_only_flows={} d16_recovery_flows={} d16_egress_phase_transitions={} d16_recovery_resets={} d16_drain_only_cycles={} d16_drain_only_drain_bytes={} send_slice_zero={} send_slice_errors={} budget_limited_calls={} headroom_limited_calls={} headroom_deferred_bytes={} drain_credit_granted_bytes={} drain_credit_planned_bytes={} drain_credit_used_bytes={} egress_payload_credit_bytes={} drop_credit_debt_bytes={} drop_credit_debt_paid_bytes={} drop_credit_blocked_bytes={} pressure_credit_debt_bytes={} pressure_credit_debt_paid_bytes={} pressure_credit_blocked_bytes={} hard_edge_guard_bytes={} hard_edge_guard_limited_calls={} hard_edge_guard_deferred_bytes={} send_slice_max_accepted={} tun_flush_tx_calls={} tun_flush_tx_failures={} tun_flush_deferred={} dirty_handles={}",
        aggregate.pending_total,
        aggregate.pending_max,
        aggregate.pending_high,
        aggregate.remote_to_global_rx_bytes,
        aggregate.downstream_permit_bytes,
        aggregate.downstream_permit_released_bytes,
        aggregate.downstream_permit_pending_high,
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
        aggregate.actor_admitted_bytes,
        aggregate.actor_bypass_admitted_bytes,
        aggregate.permit_terminal_drop_bytes,
        aggregate.permit_terminal_drop_events,
        aggregate.d16_running_flows,
        aggregate.d16_drain_only_flows,
        aggregate.d16_recovery_flows,
        aggregate.d16_egress_phase_transitions,
        aggregate.d16_recovery_resets,
        aggregate.d16_drain_only_cycles,
        aggregate.d16_drain_only_drain_bytes,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TunRxBacklogObservation {
    BacklogProven,
    CleanWouldBlock,
    ReadError,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct TunRxBacklogGuard {
    active: bool,
    clean_probe_seen: bool,
    control_epoch_completed: bool,
    pause_edges: u64,
    resume_edges: u64,
}

impl TunRxBacklogGuard {
    fn observe(&mut self, observation: TunRxBacklogObservation) {
        match observation {
            TunRxBacklogObservation::BacklogProven => {
                if !self.active {
                    self.active = true;
                    self.pause_edges = self.pause_edges.saturating_add(1);
                }
                self.clean_probe_seen = false;
                self.control_epoch_completed = false;
            }
            TunRxBacklogObservation::CleanWouldBlock
                if self.active && self.control_epoch_completed =>
            {
                self.active = false;
                self.clean_probe_seen = false;
                self.control_epoch_completed = false;
                self.resume_edges = self.resume_edges.saturating_add(1);
            }
            TunRxBacklogObservation::CleanWouldBlock if self.active => {
                self.clean_probe_seen = true;
            }
            TunRxBacklogObservation::CleanWouldBlock | TunRxBacklogObservation::ReadError => {}
        }
    }

    fn note_control_epoch_completed(&mut self) {
        if self.active && self.clean_probe_seen {
            self.control_epoch_completed = true;
        }
    }

    fn is_active(self) -> bool {
        self.active
    }

    fn pause_edges(self) -> u64 {
        self.pause_edges
    }

    fn resume_edges(self) -> u64 {
        self.resume_edges
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TunRxBacklogAction {
    None,
    ForceDrainOnly,
    Recover,
}

fn tun_rx_backlog_action(
    before: TunRxBacklogGuard,
    after: TunRxBacklogGuard,
) -> TunRxBacklogAction {
    if after.pause_edges() > before.pause_edges() {
        TunRxBacklogAction::ForceDrainOnly
    } else if after.resume_edges() > before.resume_edges() {
        TunRxBacklogAction::Recover
    } else {
        TunRxBacklogAction::None
    }
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
    timer_stalled_read_attempts: u64,
    local_egress_service_attempts: u64,
    other_attempts: u64,
    packets: u64,
    tcp_packets: u64,
    dns_packets: u64,
    udp_packets: u64,
    tcp_batches: u64,
    batch_dirty_relay_passes: u64,
    batch_iface_polls: u64,
    batch_flushes: u64,
    tcp_batch_packets_high_water: u64,
    pump: TunIngressPumpSnapshot,
    budget_exhausted: u64,
    would_block: u64,
    errors: u64,
    backlog_guard: TunRxBacklogGuard,
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
            TUN_RX_DRAIN_SOURCE_TIMER_STALLED_READ => {
                self.timer_stalled_read_attempts =
                    self.timer_stalled_read_attempts.saturating_add(1);
            }
            TUN_RX_DRAIN_SOURCE_LOCAL_EGRESS_SERVICE => {
                self.local_egress_service_attempts =
                    self.local_egress_service_attempts.saturating_add(1);
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

    fn note_tcp_batch_dirty_relay_pass(&mut self, tcp_packets: usize) {
        if tcp_packets == 0 {
            return;
        }
        self.tcp_batches = self.tcp_batches.saturating_add(1);
        self.batch_dirty_relay_passes = self.batch_dirty_relay_passes.saturating_add(1);
        self.tcp_batch_packets_high_water =
            self.tcp_batch_packets_high_water.max(tcp_packets as u64);
    }

    fn avoided_dirty_relay_passes(&self) -> u64 {
        self.tcp_packets
            .saturating_sub(self.batch_dirty_relay_passes)
    }

    fn note_tcp_batch_poll_flush(&mut self, tcp_packets: usize) {
        if tcp_packets == 0 {
            return;
        }
        self.batch_iface_polls = self.batch_iface_polls.saturating_add(1);
        self.batch_flushes = self.batch_flushes.saturating_add(1);
        self.tcp_batch_packets_high_water =
            self.tcp_batch_packets_high_water.max(tcp_packets as u64);
    }

    fn avoided_iface_polls(&self) -> u64 {
        self.tcp_packets.saturating_sub(self.batch_iface_polls)
    }

    fn note_budget_exhausted(&mut self) {
        self.budget_exhausted = self.budget_exhausted.saturating_add(1);
        self.backlog_guard
            .observe(TunRxBacklogObservation::BacklogProven);
    }

    fn note_would_block(&mut self) {
        self.would_block = self.would_block.saturating_add(1);
        self.backlog_guard
            .observe(TunRxBacklogObservation::CleanWouldBlock);
    }

    fn note_error(&mut self) {
        self.errors = self.errors.saturating_add(1);
        self.backlog_guard
            .observe(TunRxBacklogObservation::ReadError);
    }

    fn note_control_epoch_completed(&mut self) {
        self.backlog_guard.note_control_epoch_completed();
    }

    fn backlog_guard(&self) -> TunRxBacklogGuard {
        self.backlog_guard
    }
}

fn format_tun_rx_drain_diag(diag: &TunRxDrainDiag) -> String {
    format!(
        "🔎 tcp-tun-rx-drain attempts={} pre_payload_attempts={} remote_payload_attempts={} remote_payload_deferred_attempts={} remote_payload_deferred_delayed_attempts={} remote_payload_deferred_pressure_attempts={} relay_gap_hint_attempts={} relay_gap_hint_followup_attempts={} maintenance_attempts={} timer_active_flow_attempts={} timer_stalled_read_attempts={} local_egress_service_attempts={} other_attempts={} packets={} tcp={} dns={} udp={} tcp_batches={} batch_dirty_relay_passes={} batch_iface_polls={} batch_flushes={} tcp_batch_packets_high_water={} avoided_dirty_relay_passes={} avoided_iface_polls={} pump_enabled={} pump_packets={} pump_bytes={} pump_queue_high_water={} pump_capacity={} pump_full_waits={} pump_read_errors={} pump_closed={} budget_exhausted={} would_block={} errors={} backlog_active={} backlog_pause_edges={} backlog_resume_edges={}",
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
        diag.timer_stalled_read_attempts,
        diag.local_egress_service_attempts,
        diag.other_attempts,
        diag.packets,
        diag.tcp_packets,
        diag.dns_packets,
        diag.udp_packets,
        diag.tcp_batches,
        diag.batch_dirty_relay_passes,
        diag.batch_iface_polls,
        diag.batch_flushes,
        diag.tcp_batch_packets_high_water,
        diag.avoided_dirty_relay_passes(),
        diag.avoided_iface_polls(),
        diag.pump.enabled,
        diag.pump.packets,
        diag.pump.bytes,
        diag.pump.queue_high_water,
        diag.pump.capacity_packets,
        diag.pump.full_waits,
        diag.pump.read_errors,
        diag.pump.closed,
        diag.budget_exhausted,
        diag.would_block,
        diag.errors,
        diag.backlog_guard.is_active(),
        diag.backlog_guard.pause_edges(),
        diag.backlog_guard.resume_edges()
    )
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
enum LocalEgressServiceStopReason {
    TargetReached,
    NoProgress,
    CycleBudget,
    HardPause,
    #[default]
    NoWork,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LocalEgressServiceConfig {
    max_cycles: usize,
    target_bytes_per_window: usize,
    tun_rx_packets_per_cycle: usize,
}

impl Default for LocalEgressServiceConfig {
    fn default() -> Self {
        Self {
            max_cycles: LOCAL_EGRESS_SERVICE_MAX_CYCLES,
            target_bytes_per_window: LOCAL_EGRESS_SERVICE_TARGET_BYTES_PER_WINDOW,
            tun_rx_packets_per_cycle: LOCAL_EGRESS_SERVICE_TUN_RX_PACKETS_PER_CYCLE,
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct LocalEgressServiceProgress {
    cycles: usize,
    accepted_bytes: usize,
    egress_drain_bytes: usize,
    tun_rx_packets: usize,
    flush_tx_calls: usize,
    flush_tx_failures: usize,
    dirty_passes: usize,
    stop_reason: LocalEgressServiceStopReason,
}

impl LocalEgressServiceProgress {
    fn has_progress(&self) -> bool {
        self.accepted_bytes > 0 || self.egress_drain_bytes > 0 || self.tun_rx_packets > 0
    }
}

fn parse_d3_egress_actor(s: Option<&str>) -> bool {
    parse_trace(s)
}

fn format_h10d16_startup_line(enabled: bool) -> String {
    format!(
        "H10d16 byte-owned egress: {} per_flow_cap={} global_cap={} quantum={}",
        if enabled { "enabled" } else { "disabled" },
        D16_PER_FLOW_BYTE_CAPACITY,
        D16_GLOBAL_BYTE_BUDGET_DEFAULT_BYTES,
        D16_ACTOR_QUANTUM_BYTES,
    )
}

fn parse_d3_egress_actor_adaptive_credit(s: Option<&str>) -> bool {
    parse_trace(s)
}

fn parse_d3_egress_actor_legacy_credit(s: Option<&str>) -> bool {
    parse_trace(s)
}

fn parse_d3_egress_actor_self_wake(s: Option<&str>) -> bool {
    parse_trace(s)
}

fn parse_d4_stalled_read_service(s: Option<&str>) -> bool {
    parse_trace(s)
}

fn parse_d5_capacity_backpressure(s: Option<&str>) -> bool {
    parse_trace(s)
}

fn should_rearm_d3_egress_actor_self_wake(
    enabled: bool,
    dirty_has_work: bool,
    progress: LocalEgressServiceProgress,
) -> bool {
    enabled && dirty_has_work && progress.has_progress()
}

fn d3_egress_actor_self_wake_enabled(runtime_config: &TunRuntimeConfig) -> bool {
    runtime_config.d3_egress_actor && runtime_config.d3_egress_actor_self_wake
}

fn d4_stalled_read_service_enabled(runtime_config: &TunRuntimeConfig) -> bool {
    runtime_config.d3_egress_actor && runtime_config.d4_stalled_read_service
}

fn d5_capacity_backpressure_enabled(runtime_config: &TunRuntimeConfig) -> bool {
    runtime_config.d3_egress_actor && runtime_config.d5_capacity_backpressure
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct LocalEgressPressureSnapshot {
    queued_tx_bytes: usize,
    send_queue_bytes: usize,
}

impl LocalEgressPressureSnapshot {
    fn capture<D: TunIo>(
        device: &D,
        dirty: &HashSet<SocketHandle>,
        socket_ctxs: &HashMap<SocketHandle, SocketCtx>,
        sockets: &SocketSet<'static>,
    ) -> Self {
        Self {
            queued_tx_bytes: device.queued_tx_bytes(),
            send_queue_bytes: downlink_pressure_stats(dirty, socket_ctxs, sockets).total_tx_queue,
        }
    }

    fn drain_bytes_to(self, after: Self) -> usize {
        let tun_tx_drain_bytes = self.queued_tx_bytes.saturating_sub(after.queued_tx_bytes);
        let send_queue_drain_bytes = self.send_queue_bytes.saturating_sub(after.send_queue_bytes);
        tun_tx_drain_bytes.max(send_queue_drain_bytes)
    }
}

fn release_downlink_inflight_permits_for_observed_drain(
    ctx: &mut SocketCtx,
    send_queue_before: usize,
    send_queue_after: usize,
) -> usize {
    let drained = send_queue_before.saturating_sub(send_queue_after);
    if drained == 0 {
        return 0;
    }
    ctx.release_downlink_inflight_permits(drained)
}

fn capture_dirty_send_queues(
    dirty: &HashSet<SocketHandle>,
    sockets: &SocketSet<'static>,
) -> Vec<(SocketHandle, usize)> {
    dirty
        .iter()
        .copied()
        .map(|handle| {
            let send_queue = sockets.get::<TcpSocket>(handle).send_queue();
            (handle, send_queue)
        })
        .collect()
}

fn release_dirty_downlink_inflight_permits_for_send_queue_drain(
    before: &[(SocketHandle, usize)],
    sockets: &SocketSet<'static>,
    socket_ctxs: &mut HashMap<SocketHandle, SocketCtx>,
) -> usize {
    let mut released = 0usize;
    for (handle, send_queue_before) in before {
        let send_queue_after = sockets.get::<TcpSocket>(*handle).send_queue();
        let Some(ctx) = socket_ctxs.get_mut(handle) else {
            continue;
        };
        released = released.saturating_add(release_downlink_inflight_permits_for_observed_drain(
            ctx,
            *send_queue_before,
            send_queue_after,
        ));
    }
    released
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct TunFlushPermitRelease {
    flushed_tcp_payload_bytes: usize,
    released_inflight_permit_bytes: usize,
}

fn endpoint_ipv4(endpoint: IpEndpoint) -> Option<(Ipv4Addr, u16)> {
    let std::net::IpAddr::V4(addr) = std::net::IpAddr::from(endpoint.addr) else {
        return None;
    };
    Some((addr, endpoint.port))
}

fn flushed_tcp_packet_matches_endpoint_pair(
    packet: TunFlushedTcpPacket,
    local: Option<IpEndpoint>,
    remote: Option<IpEndpoint>,
) -> bool {
    let Some((local_addr, local_port)) = local.and_then(endpoint_ipv4) else {
        return false;
    };
    let Some((remote_addr, remote_port)) = remote.and_then(endpoint_ipv4) else {
        return false;
    };
    packet.src_addr == local_addr
        && packet.src_port == local_port
        && packet.dst_addr == remote_addr
        && packet.dst_port == remote_port
}

fn flushed_tcp_packet_matches_socket(packet: TunFlushedTcpPacket, socket: &TcpSocket<'_>) -> bool {
    flushed_tcp_packet_matches_endpoint_pair(
        packet,
        socket.local_endpoint(),
        socket.remote_endpoint(),
    )
}

fn release_downlink_inflight_permits_for_flushed_tcp_packets(
    packets: Vec<TunFlushedTcpPacket>,
    sockets: &SocketSet<'_>,
    socket_ctxs: &mut HashMap<SocketHandle, SocketCtx>,
) -> TunFlushPermitRelease {
    let mut release = TunFlushPermitRelease::default();
    if packets.is_empty() {
        return release;
    }
    let handles_with_inflight: Vec<SocketHandle> = socket_ctxs
        .iter()
        .filter_map(|(handle, ctx)| (ctx.downlink_inflight_permit_bytes() > 0).then_some(*handle))
        .collect();
    if handles_with_inflight.is_empty() {
        release.flushed_tcp_payload_bytes = packets
            .iter()
            .map(|packet| packet.payload_bytes)
            .sum::<usize>();
        return release;
    }

    for packet in packets {
        release.flushed_tcp_payload_bytes = release
            .flushed_tcp_payload_bytes
            .saturating_add(packet.payload_bytes);
        let Some(handle) = handles_with_inflight.iter().copied().find(|handle| {
            let socket = sockets.get::<TcpSocket>(*handle);
            flushed_tcp_packet_matches_socket(packet, socket)
        }) else {
            continue;
        };
        let Some(ctx) = socket_ctxs.get_mut(&handle) else {
            continue;
        };
        release.released_inflight_permit_bytes = release
            .released_inflight_permit_bytes
            .saturating_add(ctx.release_downlink_inflight_permits(packet.payload_bytes));
    }
    release
}

#[derive(Debug, Default, Clone)]
struct LocalEgressServiceDiag {
    windows: u64,
    cycles: u64,
    accepted_bytes: u64,
    egress_drain_bytes: u64,
    tun_rx_packets: u64,
    flush_tx_calls: u64,
    flush_tx_failures: u64,
    dirty_passes: u64,
    target_reached: u64,
    no_progress: u64,
    cycle_budget: u64,
    hard_pause: u64,
    no_work: u64,
    egress_actor_windows: u64,
    egress_actor_cycles: u64,
    egress_actor_admitted_bytes: u64,
    egress_actor_drain_bytes: u64,
    egress_actor_no_progress: u64,
    egress_actor_immediate_wake: u64,
    egress_actor_self_wake: u64,
    egress_actor_external_wait: u64,
}

impl LocalEgressServiceDiag {
    fn note_window(&mut self, progress: LocalEgressServiceProgress) {
        self.note_window_inner(progress, false);
    }

    fn note_egress_actor_window(&mut self, progress: LocalEgressServiceProgress) {
        self.note_window_inner(progress, true);
    }

    fn note_egress_actor_immediate_wake(&mut self) {
        self.egress_actor_immediate_wake = self.egress_actor_immediate_wake.saturating_add(1);
    }

    fn note_egress_actor_self_wake(&mut self) {
        self.egress_actor_self_wake = self.egress_actor_self_wake.saturating_add(1);
    }

    fn note_window_inner(&mut self, progress: LocalEgressServiceProgress, egress_actor: bool) {
        self.windows = self.windows.saturating_add(1);
        self.cycles = self.cycles.saturating_add(progress.cycles as u64);
        self.accepted_bytes = self
            .accepted_bytes
            .saturating_add(progress.accepted_bytes as u64);
        self.egress_drain_bytes = self
            .egress_drain_bytes
            .saturating_add(progress.egress_drain_bytes as u64);
        self.tun_rx_packets = self
            .tun_rx_packets
            .saturating_add(progress.tun_rx_packets as u64);
        self.flush_tx_calls = self
            .flush_tx_calls
            .saturating_add(progress.flush_tx_calls as u64);
        self.flush_tx_failures = self
            .flush_tx_failures
            .saturating_add(progress.flush_tx_failures as u64);
        self.dirty_passes = self
            .dirty_passes
            .saturating_add(progress.dirty_passes as u64);
        if egress_actor {
            self.egress_actor_windows = self.egress_actor_windows.saturating_add(1);
            self.egress_actor_cycles = self
                .egress_actor_cycles
                .saturating_add(progress.cycles as u64);
            self.egress_actor_admitted_bytes = self
                .egress_actor_admitted_bytes
                .saturating_add(progress.accepted_bytes as u64);
            self.egress_actor_drain_bytes = self
                .egress_actor_drain_bytes
                .saturating_add(progress.egress_drain_bytes as u64);
        }
        match progress.stop_reason {
            LocalEgressServiceStopReason::TargetReached => {
                self.target_reached = self.target_reached.saturating_add(1);
            }
            LocalEgressServiceStopReason::NoProgress => {
                self.no_progress = self.no_progress.saturating_add(1);
                if egress_actor {
                    self.egress_actor_no_progress = self.egress_actor_no_progress.saturating_add(1);
                    self.egress_actor_external_wait =
                        self.egress_actor_external_wait.saturating_add(1);
                }
            }
            LocalEgressServiceStopReason::CycleBudget => {
                self.cycle_budget = self.cycle_budget.saturating_add(1);
            }
            LocalEgressServiceStopReason::HardPause => {
                self.hard_pause = self.hard_pause.saturating_add(1);
                if egress_actor {
                    self.egress_actor_external_wait =
                        self.egress_actor_external_wait.saturating_add(1);
                }
            }
            LocalEgressServiceStopReason::NoWork => {
                self.no_work = self.no_work.saturating_add(1);
            }
        }
    }
}

fn format_local_egress_service_diag(
    diag: &LocalEgressServiceDiag,
    actor_bypass_admitted_bytes: u64,
) -> String {
    format!(
        "🔎 tcp-local-egress-service windows={} cycles={} accepted_bytes={} egress_drain_bytes={} tun_rx_packets={} flush_tx_calls={} flush_tx_failures={} dirty_passes={} target_reached={} no_progress={} cycle_budget={} hard_pause={} no_work={} egress_actor_windows={} egress_actor_cycles={} egress_actor_admitted_bytes={} actor_bypass_admitted_bytes={} egress_actor_drain_bytes={} egress_actor_no_progress={} egress_actor_immediate_wake={} egress_actor_self_wake={} egress_actor_external_wait={}",
        diag.windows,
        diag.cycles,
        diag.accepted_bytes,
        diag.egress_drain_bytes,
        diag.tun_rx_packets,
        diag.flush_tx_calls,
        diag.flush_tx_failures,
        diag.dirty_passes,
        diag.target_reached,
        diag.no_progress,
        diag.cycle_budget,
        diag.hard_pause,
        diag.no_work,
        diag.egress_actor_windows,
        diag.egress_actor_cycles,
        diag.egress_actor_admitted_bytes,
        actor_bypass_admitted_bytes,
        diag.egress_actor_drain_bytes,
        diag.egress_actor_no_progress,
        diag.egress_actor_immediate_wake,
        diag.egress_actor_self_wake,
        diag.egress_actor_external_wait
    )
}

fn note_local_egress_service_window(
    diag: &mut LocalEgressServiceDiag,
    progress: LocalEgressServiceProgress,
    egress_actor: bool,
) {
    if egress_actor {
        diag.note_egress_actor_window(progress);
    } else {
        diag.note_window(progress);
    }
}

fn record_local_egress_service_window<M: MetricsSink>(
    metrics: &mut M,
    socket_ctxs: &HashMap<SocketHandle, SocketCtx>,
    diag: &mut LocalEgressServiceDiag,
    progress: LocalEgressServiceProgress,
    egress_actor: bool,
) {
    record_d16_harness_observation(metrics, socket_ctxs);
    note_local_egress_service_window(diag, progress, egress_actor);
}

fn record_d16_harness_observation<M: MetricsSink>(
    metrics: &mut M,
    socket_ctxs: &HashMap<SocketHandle, SocketCtx>,
) {
    if !metrics.wants_d16_harness_observations() {
        return;
    }
    let aggregate = tcp_downlink_aggregate(socket_ctxs.values());
    metrics.note_d16_actor_observation(
        aggregate.actor_bypass_admitted_bytes,
        aggregate.d16_running_flows,
        aggregate.d16_drain_only_flows,
        aggregate.d16_recovery_flows,
        aggregate.recv_queue_max,
    );
    let mut owned_queue_bytes = 0usize;
    let mut pending_bytes = 0usize;
    let mut inflight_bytes = 0usize;
    let mut local_eof_sent_flows = 0usize;
    for ctx in socket_ctxs.values() {
        let Some(queue) = ctx.d16_downlink_queue.as_ref() else {
            continue;
        };
        owned_queue_bytes = owned_queue_bytes.saturating_add(queue.snapshot_now().owned_bytes());
        pending_bytes = pending_bytes.saturating_add(ctx.downlink_pending.len());
        inflight_bytes = inflight_bytes.saturating_add(ctx.downlink_inflight_permit_bytes());
        local_eof_sent_flows = local_eof_sent_flows.saturating_add(usize::from(ctx.local_eof_sent));
    }
    metrics.note_d16_flow_lifecycle(
        owned_queue_bytes,
        pending_bytes,
        inflight_bytes,
        local_eof_sent_flows,
        aggregate.permit_terminal_drop_bytes,
        aggregate.terminal_late_remote_payload_bytes,
    );
}

fn stream_service_blocked_reason_for_local_egress(
    reason: LocalEgressServiceStopReason,
) -> StreamServiceBlockedReason {
    match reason {
        LocalEgressServiceStopReason::TargetReached => StreamServiceBlockedReason::None,
        LocalEgressServiceStopReason::NoProgress => {
            StreamServiceBlockedReason::LocalAdmissionNoProgress
        }
        LocalEgressServiceStopReason::CycleBudget => {
            StreamServiceBlockedReason::LocalAdmissionCycleBudget
        }
        LocalEgressServiceStopReason::HardPause => {
            StreamServiceBlockedReason::LocalAdmissionHardPause
        }
        LocalEgressServiceStopReason::NoWork => StreamServiceBlockedReason::LocalAdmissionNoWork,
    }
}

fn format_stream_service_window_diag(
    handle: SocketHandle,
    epoch: u64,
    window: &StreamServiceWindow,
) -> String {
    format!(
        "🔎 tcp-stream-service-window handle={handle:?} epoch={epoch} {}",
        format_stream_service_window_fields(window)
    )
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
    capacity_backpressure: bool,
) -> usize {
    if !has_downlink_work {
        return 0;
    }
    if configured_budget > 0 {
        return configured_budget;
    }
    if capacity_backpressure {
        return active_flow_tun_rx_drain_budget(cfg, tun_mtu);
    }
    if send_queue_bytes >= tx_queue_credit_spend_threshold(cfg) {
        pressure_tun_rx_drain_budget(cfg, tun_mtu)
    } else {
        active_flow_tun_rx_drain_budget(cfg, tun_mtu)
    }
}

fn active_ack_drain_budget_for_egress_headroom(
    stats: DownlinkPressureStats,
    cfg: DownlinkBackpressureConfig,
    tun_feedback_paused: bool,
    max_budget: usize,
    capacity_backpressure: bool,
) -> usize {
    if tun_feedback_paused || max_budget == 0 {
        return 0;
    }
    if capacity_backpressure {
        return max_budget;
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
    capacity_backpressure: bool,
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
    active_ack_drain_budget_for_egress_headroom(
        stats,
        cfg,
        tun_feedback_paused,
        max_budget,
        capacity_backpressure,
    )
}

fn tun_rx_drain_budget_for_dirty_pressure(
    has_dirty_downlink: bool,
    has_dirty_close_drain: bool,
    stats: DownlinkPressureStats,
    configured_budget: usize,
    cfg: DownlinkBackpressureConfig,
    tun_mtu: usize,
    _capacity_backpressure: bool,
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
    capacity_backpressure: bool,
) -> usize {
    if !has_recent_downlink_work || active_flow_timer_ms == 0 {
        return 0;
    }
    let max_budget = if configured_budget > 0 {
        configured_budget
    } else {
        active_flow_tun_rx_drain_budget(cfg, tun_mtu)
    };
    active_ack_drain_budget_for_egress_headroom(
        stats,
        cfg,
        tun_feedback_paused,
        max_budget,
        capacity_backpressure,
    )
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

#[cfg(test)]
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

fn next_downlink_backpressure_for_runtime(
    capacity_backpressure: bool,
    was_paused: bool,
    stats: DownlinkPressureStats,
    cfg: DownlinkBackpressureConfig,
    has_active_credit_debt: bool,
) -> bool {
    if !capacity_backpressure {
        return next_downlink_backpressure_with_credit_debt(
            was_paused,
            stats,
            cfg,
            has_active_credit_debt,
        );
    }

    if has_active_credit_debt {
        return true;
    }
    if was_paused {
        stats.max_pending > downlink_receive_window_low(cfg)
    } else {
        stats.max_pending >= downlink_receive_window_high(cfg)
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

fn endpoint_window_v1_local_uplink_window_bytes() -> usize {
    let endpoint = quinn::EndpointPacingServiceConfig::endpoint_window_v1();
    let ten_millisecond_bound = endpoint
        .burst_bytes()
        .saturating_add(endpoint.rate_bytes_per_second() / 100);
    usize::try_from(ten_millisecond_bound).unwrap_or(usize::MAX)
}

/// Startup configuration for the TUN runtime.
/// 中文要点：Stage 13d 退役 legacy 上游后，运行时只剩本地监听池配置；
/// TUIC 出口配置走 `MINI_VPN_TUIC_*`（见 tuic.rs），不在这里。
#[derive(Debug, Clone)]
pub struct TunRuntimeConfig {
    relay_policy: Arc<RelayFamilyPolicy>,
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
    listener_capacity_profile: ListenerCapacityProfile,
    tun_rx_drain_budget: usize,
    tun_rx_active_flow_timer_ms: u64,
    bounded_global_rx_receive_window: bool,
    buffered_downlink: BufferedDownlinkConfig,
    d3_egress_actor: bool,
    d3_egress_actor_adaptive_credit: bool,
    d3_egress_actor_legacy_credit: bool,
    d3_egress_actor_self_wake: bool,
    d4_stalled_read_service: bool,
    d5_capacity_backpressure: bool,
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
            relay_policy: Arc::new(RelayFamilyPolicy::default()),
            listener,
            tun_mtu: DEFAULT_TUN_MTU,
            tun_tx_queue_len: DEFAULT_TUN_TX_QUEUE_LEN_ESTIMATE,
            metrics_secs: METRICS_SNAPSHOT_SECS,
            profile_loop: false,
            downlink_backpressure,
            downlink_flush_max_bytes: DEFAULT_DOWNLINK_FLUSH_MAX_BYTES,
            downlink_egress_immediate_bytes: DEFAULT_DOWNLINK_EGRESS_IMMEDIATE_BYTES,
            tcp_socket_buffers: TcpSocketBufferConfig::default(),
            listener_capacity_profile: ListenerCapacityProfile::Legacy,
            tun_rx_drain_budget: DEFAULT_TUN_RX_DRAIN_BUDGET,
            tun_rx_active_flow_timer_ms: DEFAULT_TUN_RX_ACTIVE_FLOW_TIMER_DRAIN_MS,
            bounded_global_rx_receive_window: false,
            buffered_downlink: BufferedDownlinkConfig::disabled(
                downlink_backpressure,
                listener_pool_size,
            ),
            d3_egress_actor: false,
            d3_egress_actor_adaptive_credit: false,
            d3_egress_actor_legacy_credit: false,
            d3_egress_actor_self_wake: false,
            d4_stalled_read_service: false,
            d5_capacity_backpressure: false,
        })
    }

    #[cfg(test)]
    fn select_f4096_for_test(&mut self) {
        self.listener_capacity_profile = ListenerCapacityProfile::F4096;
    }

    #[cfg(test)]
    pub(crate) fn h10d16_gate_a_for_test() -> Self {
        let mut config = Self::from_sources(Some("2")).expect("Gate A profile must be valid");
        config.tun_mtu = 1_200;
        config.tcp_socket_buffers.rx_bytes = 1024 * 1024;
        config.tcp_socket_buffers.tx_bytes = 1024 * 1024;
        config.downlink_backpressure =
            default_downlink_backpressure_for_tun_egress(config.tun_mtu, config.tun_tx_queue_len);
        config.buffered_downlink = BufferedDownlinkConfig::disabled(
            config.downlink_backpressure,
            config.listener.pool_size,
        );
        config = config.with_relay_policy(Arc::new(RelayFamilyPolicy::from_sources(
            RelayFamilyPolicySources {
                d16_byte_owned: Some("1"),
                ..Default::default()
            },
        )));
        config.tcp_socket_buffers.receive_window_limit =
            Some(endpoint_window_v1_local_uplink_window_bytes());
        config
    }

    pub fn with_relay_policy(mut self, policy: Arc<RelayFamilyPolicy>) -> Self {
        self.tcp_socket_buffers.receive_window_limit = policy
            .d16_byte_owned()
            .then_some(endpoint_window_v1_local_uplink_window_bytes());
        self.relay_policy = policy;
        self
    }

    pub fn relay_policy(&self) -> &Arc<RelayFamilyPolicy> {
        &self.relay_policy
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
    fn from_env_with_relay_policy(policy: Arc<RelayFamilyPolicy>) -> Result<Self, ClientError> {
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
        cfg.d3_egress_actor =
            parse_d3_egress_actor(std::env::var("MINI_VPN_D3_EGRESS_ACTOR").ok().as_deref());
        cfg.d3_egress_actor_adaptive_credit = parse_d3_egress_actor_adaptive_credit(
            std::env::var("MINI_VPN_D3_EGRESS_ACTOR_ADAPTIVE_CREDIT")
                .ok()
                .as_deref(),
        );
        cfg.d3_egress_actor_legacy_credit = parse_d3_egress_actor_legacy_credit(
            std::env::var("MINI_VPN_D3_EGRESS_ACTOR_LEGACY_CREDIT")
                .ok()
                .as_deref(),
        );
        cfg.d3_egress_actor_self_wake = parse_d3_egress_actor_self_wake(
            std::env::var("MINI_VPN_D3_EGRESS_ACTOR_SELF_WAKE")
                .ok()
                .as_deref(),
        );
        cfg.d4_stalled_read_service = parse_d4_stalled_read_service(
            std::env::var("MINI_VPN_D4_STALLED_READ_SERVICE")
                .ok()
                .as_deref(),
        );
        cfg.d5_capacity_backpressure = parse_d5_capacity_backpressure(
            std::env::var("MINI_VPN_D5_CAPACITY_BACKPRESSURE")
                .ok()
                .as_deref(),
        );
        cfg = cfg.with_relay_policy(policy);
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
    handshake_done_tx: &OpenDispatch,
    global_tx: &mpsc::Sender<(SocketHandle, RelayEvent)>,
    owned_action_tx: &mpsc::Sender<OwnedActionEvent>,
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
    admission_origin: DownlinkAdmissionOrigin,
) where
    U: OwnedUpstream + 'static,
    M: MetricsSink,
{
    metrics.enter_relay();
    // 快照后处理：边遍历边 `dirty.remove` 会与迭代借用冲突。dirty 规模 = O(活跃)，分配可忽略。
    let snapshot: Vec<SocketHandle> = dirty.iter().copied().collect();
    let mut current_global_pending =
        global_pending_for_buffered_credit(buffered_downlink.enabled, || {
            dirty_downlink_pending_total(dirty, socket_ctxs)
        });
    metrics.note_listeners(snapshot.len());
    for handle in snapshot {
        let pending_before = if buffered_downlink.enabled {
            socket_ctxs
                .get(&handle)
                .map(SocketCtx::pending_downlink_bytes)
                .unwrap_or(0)
        } else {
            0
        };
        if let Err(e) = (ListenerActivity {
            sockets,
            socket_ctxs,
            upstream,
            handshake_done_tx,
            global_tx,
            owned_action_tx,
            fake_pool,
            now_secs,
            metrics_handle,
            downlink_flush_max_bytes,
            downlink_backpressure,
            tun_mtu,
            downlink_egress_drop_debt,
            close_egress_drop_guard,
            relay_read_hard_pause,
            admission_origin,
        })
        .process(handle)
        .await
        {
            trace_log!("处理本地房间 {:?} 失败: {e}", handle);
        }
        let still_active = {
            let (has_recv, snapshot) = {
                let socket = sockets.get_mut::<TcpSocket>(handle);
                (socket.can_recv(), SocketCloseSnapshot::from_socket(socket))
            };
            let (
                has_pending,
                has_inflight_permits,
                has_d16_actor_work,
                has_d16_phase_work,
                needs_local_finish,
                has_owned_control_work,
            ) = socket_ctxs
                .get_mut(&handle)
                .map(|c| {
                    log_tcp_lifecycle_observation(handle, c, snapshot, now_secs, "dirty_relay");
                    (
                        c.pending_downlink_bytes() > 0,
                        c.downlink_inflight_permit_bytes() > 0,
                        d16_queue_has_actor_work(c),
                        c.uses_owned_egress_actor() && c.d16_egress_phase != EgressPhase::Running,
                        (if c.resumable_uplink.is_some() {
                            resumable_local_source_eof(snapshot)
                        } else {
                            snapshot.tcp_state == TcpState::CloseWait
                        }) && c.has_live_uplink()
                            && !c.local_fin_sent,
                        c.resumable_half_close.is_some()
                            || c.resumable_terminal.is_some()
                            || c.local_reset.is_some(),
                    )
                })
                .unwrap_or((false, false, false, false, false, false));
            has_recv
                || has_pending
                || has_inflight_permits
                || has_d16_actor_work
                || has_d16_phase_work
                || needs_local_finish
                || has_owned_control_work
                || snapshot.send_queue > downlink_backpressure.low_bytes
        };
        if buffered_downlink.enabled {
            let pending_after = socket_ctxs
                .get(&handle)
                .map(SocketCtx::pending_downlink_bytes)
                .unwrap_or(0);
            current_global_pending = current_global_pending
                .saturating_sub(pending_before)
                .saturating_add(pending_after);
        }
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
    #[cfg(all(test, feature = "harness"))]
    handshake_done_tx.observe_pressure(sockets, socket_ctxs);
    record_d16_harness_observation(metrics, socket_ctxs);
    metrics.leave_relay();
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
    let Some(close) = ctx.pending_relay_close else {
        return false;
    };
    let payload_ready = if ctx.d16_downlink_queue.is_some()
        && close.direction == "remote_to_local"
        && close.reason == "remote_eof"
    {
        let (queue_closed, queue_empty) = d16_remote_eof_queue_state(ctx).unwrap_or((false, false));
        remote_eof_ready_for_local_close(
            queue_closed,
            queue_empty,
            ctx.downlink_pending.len(),
            ctx.downlink_inflight_permit_bytes(),
        )
    } else {
        ctx.downlink_pending.is_empty()
    };
    payload_ready && !ctx.local_eof_sent && snapshot.active && snapshot.can_send
}

fn remote_eof_ready_for_local_close(
    queue_closed: bool,
    queue_empty: bool,
    pending_bytes: usize,
    inflight_permit_bytes: usize,
) -> bool {
    queue_closed && queue_empty && pending_bytes == 0 && inflight_permit_bytes == 0
}

fn d16_remote_eof_queue_state(ctx: &SocketCtx) -> Option<(bool, bool)> {
    let snapshot = ctx.d16_downlink_queue.as_ref()?.snapshot_now();
    Some((
        snapshot.closure.is_closed(),
        snapshot.queued_bytes == 0 && snapshot.reserved_bytes == 0,
    ))
}

fn install_d16_remote_eof_if_ready(ctx: &mut SocketCtx, now_secs: u64) -> bool {
    let Some((queue_closed, queue_empty)) = d16_remote_eof_queue_state(ctx) else {
        return false;
    };
    if !queue_closed || !queue_empty || ctx.pending_relay_close.is_some() {
        return false;
    }
    ctx.state = SocketState::Closing;
    ctx.uplink_tx = None;
    ctx.pending_relay_close = Some(RelayClose {
        epoch: ctx.conn_epoch,
        direction: "remote_to_local",
        reason: "remote_eof",
    });
    ctx.pending_relay_close_since_secs = Some(now_secs);
    ctx.pending_relay_close_last_egress_progress_secs = Some(now_secs);
    if ctx.downlink_pending.is_empty() {
        ctx.pending_relay_close_last_progress_secs = None;
        ctx.pending_relay_close_last_pending_bytes = 0;
    } else {
        ctx.pending_relay_close_last_progress_secs = Some(now_secs);
        ctx.pending_relay_close_last_pending_bytes = ctx.downlink_pending.len();
        ctx.downlink_pending_last_progress_secs = Some(now_secs);
        ctx.downlink_pending_last_pending_bytes = ctx.downlink_pending.len();
    }
    true
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

/// Protocol-owned uplink pump. `peek` determines the exact contiguous extent
/// without consuming it; the flow port then reserves both its message slot
/// and exact replay bytes before the extractor dequeues anything from smoltcp.
fn resumable_local_source_eof(snapshot: SocketCloseSnapshot) -> bool {
    !snapshot.may_recv
        && matches!(
            snapshot.tcp_state,
            TcpState::CloseWait | TcpState::Closing | TcpState::TimeWait | TcpState::LastAck
        )
}

fn resumable_uplink_admission_is_backpressure(error: &FlowPortError) -> bool {
    matches!(
        error,
        FlowPortError::UplinkMessageLaneFull
            | FlowPortError::UplinkByteBudgetExhausted { .. }
            | FlowPortError::UplinkGlobalByteBudgetExhausted { .. }
            | FlowPortError::UplinkSegmentBudgetExhausted { .. }
            | FlowPortError::UplinkGlobalSegmentBudgetExhausted { .. }
    )
}

fn pump_resumable_uplink(
    ctx: &mut SocketCtx,
    tcp_socket: &mut TcpSocket<'_>,
    snapshot: SocketCloseSnapshot,
) -> EstablishedUplink {
    if ctx.owned_terminal_pending {
        return EstablishedUplink::Handled;
    }
    let Some(uplink) = ctx.resumable_uplink.as_ref() else {
        return EstablishedUplink::NotEstablished;
    };

    for _ in 0..MAX_ESTABLISHED_UPLINK_BATCH {
        let byte_len = match tcp_socket.peek(MAX_DATA_PAYLOAD_BYTES) {
            Ok(bytes) => bytes.len(),
            Err(_) => 0,
        };
        if byte_len == 0 {
            if resumable_local_source_eof(snapshot) && !ctx.local_fin_sent {
                match uplink.try_send_close() {
                    Ok(()) => {
                        ctx.local_fin_sent = true;
                        ctx.state = SocketState::Relaying;
                    }
                    Err(FlowPortError::UplinkMessageLaneFull) => {}
                    Err(FlowPortError::Closed) => return EstablishedUplink::Closed,
                    Err(error) => {
                        tcp_diag_log!("🔎 owned-uplink-close-error error={error}");
                        return EstablishedUplink::Closed;
                    }
                }
            }
            return EstablishedUplink::Handled;
        }

        let admitted = uplink.try_send_uplink_vec_with(byte_len, || {
            let mut payload = vec![0u8; byte_len];
            match tcp_socket.recv_slice(&mut payload) {
                Ok(read) => {
                    payload.truncate(read);
                    payload
                }
                Err(_) => Vec::new(),
            }
        });
        match admitted {
            Ok(()) => ctx.state = SocketState::Relaying,
            Err(error) if resumable_uplink_admission_is_backpressure(&error) => {
                return EstablishedUplink::Handled;
            }
            Err(FlowPortError::Closed) => return EstablishedUplink::Closed,
            Err(error) => {
                // A length mismatch after reservation would mean the
                // single-owner peek/recv transaction drifted. Bytes may have
                // been consumed, so fail this flow closed rather than retrying
                // and risking duplication.
                tcp_diag_log!("🔎 owned-uplink-admission-error error={error}");
                return EstablishedUplink::Closed;
            }
        }
    }
    EstablishedUplink::Handled
}

/// Exact legacy extraction. A resumable flow is never coerced into
/// `AsyncRead/AsyncWrite`, because that would erase application-ACK and replay
/// ownership.
async fn open_legacy_owned_tcp<U: OwnedUpstream + ?Sized>(
    upstream: &U,
    target: &TargetAddr,
) -> Result<OpenedTcpRelay, ClientError> {
    match upstream.open_tcp_flow(target).await? {
        OpenedOwnedTcp::Legacy(relay) => Ok(relay),
        OpenedOwnedTcp::Resumable(flow) => {
            abandon_uninstalled_owned_tcp(
                OpenedOwnedTcp::Resumable(flow),
                "legacy_open_received_resumable",
            );
            Err(ClientError::Io(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "resumable TCP flow requires the owned SocketCtx path",
            )))
        }
    }
}

fn install_resumable_tcp_flow(
    handle: SocketHandle,
    epoch: u64,
    ctx: &mut SocketCtx,
    flow: crate::owned_upstream::ResumableTcpFlow,
    owned_action_tx: &mpsc::Sender<OwnedActionEvent>,
    hard_pause_active: bool,
) {
    let (uplink, downlink) = flow.into_split();
    ctx.uplink_tx = None;
    ctx.relay_read_credit_tx = None;
    ctx.resumable_sink = None;
    ctx.resumable_half_close = None;
    ctx.resumable_terminal = None;
    ctx.owned_terminal_pending = false;
    ctx.owned_terminal_wait_since_secs = None;
    ctx.resumable_action_bridge = Some(spawn_resumable_action_bridge(
        handle,
        epoch,
        downlink,
        owned_action_tx.clone(),
    ));
    ctx.resumable_uplink = Some(uplink);
    ctx.downlink_d3_actor_flow = true;
    ctx.d16_tun_rx_ack_barrier = false;
    ctx.d16_egress_phase = if hard_pause_active {
        EgressPhase::DrainOnly
    } else {
        EgressPhase::Running
    };
    ctx.pending_open_mode = None;
    ctx.state = SocketState::Relaying;
}

fn abandon_uninstalled_owned_tcp(opened: OpenedOwnedTcp, cause: &'static str) {
    let OpenedOwnedTcp::Resumable(flow) = opened else {
        return;
    };
    if let Err(error) = flow.abandon_before_install() {
        tcp_diag_log!("🔎 owned-open-abandon-error cause={cause} error={error}");
    }
}

/// 处理一次 spawned remote-open 的完成事件。**epoch 防串话置于一切之前**——迟到结果绝不装到新一代 socket。
/// 成功 → 安装 uplink channel + 按序 flush open 期间缓存 + spawn relay；失败 → rearm。
///
/// 返回值仅表示“新安装的 protocol-owned flow 需要立即重新进入 dirty
/// service”。Legacy relay 仍以原有 global event 作为唤醒权威，因此安装成功也返回
/// `false`，不应将该值解释为通用 success flag。
#[cfg(test)]
fn handle_handshake_done(
    done: HandshakeDone,
    sockets: &mut SocketSet<'_>,
    socket_ctxs: &mut HashMap<SocketHandle, SocketCtx>,
    global_tx: &mpsc::Sender<(SocketHandle, RelayEvent)>,
    owned_action_tx: &mpsc::Sender<OwnedActionEvent>,
    fake_pool: &mut FakeIpPool,
    now_secs: u64,
    metrics_handle: &Metrics,
    downlink_backpressure: DownlinkBackpressureConfig,
    relay_read_hard_pause: bool,
) -> bool {
    HandshakeCompletion {
        sockets,
        socket_ctxs,
        global_tx,
        owned_action_tx,
        fake_pool,
        now_secs,
        metrics_handle,
        downlink_backpressure,
        relay_read_hard_pause,
    }
    .legacy(done)
}

// Test-only compatibility facade for the established direct call sites.
#[cfg(test)]
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
    mut downstream_permit: Option<DownstreamBytePermit>,
    d3_egress_actor: bool,
    d3_egress_actor_adaptive_credit: bool,
    d3_egress_actor_legacy_credit: bool,
    d5_capacity_backpressure: bool,
) -> std::io::Result<usize> {
    handle_remote_payload_with_gate(
        RearmGate::legacy_facade(),
        handle,
        epoch,
        &payload,
        RemotePayloadRequest {
            sockets,
            socket_ctxs,
            iface,
            device,
            fake_pool,
            now_secs,
            downlink_flush_max_bytes,
            downlink_egress_pacer,
            downlink_egress_drop_debt,
            downlink_backpressure,
            tun_mtu,
            buffered_downlink_enabled,
            downstream_permit: &mut downstream_permit,
            d3_egress_actor,
            d3_egress_actor_adaptive_credit,
            d3_egress_actor_legacy_credit,
            d5_capacity_backpressure,
        },
    )
    .await
}

fn handle_owned_port_closed_from_loop(
    dispatch: &OpenDispatch,
    handle: SocketHandle,
    epoch: u64,
    flow: LocalFlow,
    sockets: &mut SocketSet<'_>,
    socket_ctxs: &mut HashMap<SocketHandle, SocketCtx>,
    fake_pool: &mut FakeIpPool,
    now_secs: u64,
) -> bool {
    handle_owned_port_closed_with_gate(
        RearmGate::original_loop(dispatch),
        handle,
        epoch,
        flow,
        sockets,
        socket_ctxs,
        fake_pool,
        now_secs,
    )
}

fn handle_owned_port_closed_with_gate(
    gate: RearmGate<'_>,
    handle: SocketHandle,
    epoch: u64,
    flow: LocalFlow,
    sockets: &mut SocketSet<'_>,
    socket_ctxs: &mut HashMap<SocketHandle, SocketCtx>,
    fake_pool: &mut FakeIpPool,
    now_secs: u64,
) -> bool {
    let Some(ctx) = socket_ctxs.get_mut(&handle) else {
        return false;
    };
    if ctx.conn_epoch != epoch || ctx.resumable_uplink.is_none() {
        return false;
    }
    let expected_flow = ctx
        .resumable_uplink
        .as_ref()
        .map(ResumableTcpUplink::local_flow);
    let reason = if ctx.owned_terminal_pending {
        "owned_terminal_delivery_lost"
    } else if expected_flow == Some(flow) {
        "owned_flow_port_closed"
    } else {
        "cross_flow_owned_port_close"
    };
    let socket = sockets.get_mut::<TcpSocket>(handle);
    legacy_reasoned_rearm_with_gate(
        gate,
        handle,
        socket,
        ctx,
        fake_pool,
        now_secs,
        "owner_to_local",
        reason,
    );
    false
}

fn handle_owned_tun_action_from_loop(
    dispatch: &OpenDispatch,
    handle: SocketHandle,
    epoch: u64,
    action: TunAction,
    sockets: &mut SocketSet<'_>,
    socket_ctxs: &mut HashMap<SocketHandle, SocketCtx>,
    fake_pool: &mut FakeIpPool,
    now_secs: u64,
) -> bool {
    handle_owned_tun_action_with_gate(
        RearmGate::original_loop(dispatch),
        handle,
        epoch,
        action,
        sockets,
        socket_ctxs,
        fake_pool,
        now_secs,
    )
}

fn handle_owned_tun_action_with_gate(
    gate: RearmGate<'_>,
    handle: SocketHandle,
    epoch: u64,
    action: TunAction,
    sockets: &mut SocketSet<'_>,
    socket_ctxs: &mut HashMap<SocketHandle, SocketCtx>,
    fake_pool: &mut FakeIpPool,
    now_secs: u64,
) -> bool {
    let Some(ctx) = socket_ctxs.get_mut(&handle) else {
        return false;
    };
    if ctx.conn_epoch != epoch || ctx.resumable_uplink.is_none() {
        // Dropping Sink/HalfClose is fail-closed and reports the exact
        // abandonment through its already-reserved completion slot.
        return false;
    }
    let expected_flow = ctx
        .resumable_uplink
        .as_ref()
        .map(ResumableTcpUplink::local_flow);
    let action_matches_flow = match &action {
        TunAction::Sink(delivery) => expected_flow.is_some_and(|flow| {
            delivery.offer().session_id() == flow.session_id()
                && delivery.offer().flow_id() == flow.flow_id()
        }),
        TunAction::HalfClose(delivery) => expected_flow.is_some_and(|flow| {
            delivery.capability().session_id() == flow.session_id()
                && delivery.capability().flow_id() == flow.flow_id()
        }),
        TunAction::Terminal(delivery) => expected_flow == Some(delivery.local_flow()),
    };
    if !gate_legacy_rearm(
        gate,
        RearmIntent::reasoned_ctx(
            handle,
            ctx,
            "owner_to_local",
            "owned_action_disposition",
            None,
        ),
    ) {
        return true;
    }
    if !action_matches_flow {
        let socket = sockets.get_mut::<TcpSocket>(handle);
        legacy_reasoned_rearm_with_gate(
            gate,
            handle,
            socket,
            ctx,
            fake_pool,
            now_secs,
            "owner_to_local",
            "cross_flow_owned_action",
        );
        return false;
    }

    let local_capability_live = match &action {
        TunAction::Sink(delivery) => delivery.is_live(),
        TunAction::HalfClose(delivery) => delivery.is_live(),
        TunAction::Terminal(_) => true,
    };
    if !local_capability_live {
        ctx.owned_terminal_pending = true;
        return false;
    }

    if matches!(&action, TunAction::Terminal(_)) {
        ctx.owned_terminal_pending = false;
    }
    match action {
        TunAction::Sink(delivery) => {
            if ctx.resumable_sink.is_some()
                || ctx.resumable_half_close.is_some()
                || ctx.resumable_terminal.is_some()
            {
                let socket = sockets.get_mut::<TcpSocket>(handle);
                legacy_reasoned_rearm_with_gate(
                    gate,
                    handle,
                    socket,
                    ctx,
                    fake_pool,
                    now_secs,
                    "owner_to_local",
                    "overlapping_owned_sink_action",
                );
                false
            } else {
                ctx.resumable_sink = Some(delivery);
                true
            }
        }
        TunAction::HalfClose(delivery) => {
            if ctx.resumable_sink.is_some()
                || ctx.resumable_half_close.is_some()
                || ctx.resumable_terminal.is_some()
            {
                let socket = sockets.get_mut::<TcpSocket>(handle);
                legacy_reasoned_rearm_with_gate(
                    gate,
                    handle,
                    socket,
                    ctx,
                    fake_pool,
                    now_secs,
                    "owner_to_local",
                    "out_of_order_owned_half_close",
                );
                false
            } else {
                ctx.resumable_half_close = Some(delivery);
                true
            }
        }
        TunAction::Terminal(delivery) => match delivery.action() {
            TerminalAction::FlowFinished(FlowFinishReason::Graceful) => {
                if ctx.resumable_sink.is_some()
                    || ctx.resumable_half_close.is_some()
                    || !ctx.local_eof_sent
                {
                    // Graceful completion without the two-phase local FIN is
                    // a protocol ownership violation; do not turn it into a
                    // silent clean close.
                    ctx.resumable_action_bridge = None;
                    ctx.resumable_uplink = None;
                    let socket = sockets.get_mut::<TcpSocket>(handle);
                    legacy_reasoned_rearm_with_gate(
                        gate,
                        handle,
                        socket,
                        ctx,
                        fake_pool,
                        now_secs,
                        "owner_to_local",
                        "premature_owned_graceful_finish",
                    );
                    false
                } else {
                    // Start a distinct bounded local-FIN drain window when
                    // the exact reducer terminal first arrives. Do not reuse
                    // time already spent waiting for this action in transit.
                    ctx.owned_terminal_wait_since_secs = None;
                    ctx.resumable_terminal = Some(delivery.action());
                    true
                }
            }
            TerminalAction::FlowFinished(
                FlowFinishReason::PeerReset(_)
                | FlowFinishReason::LocalReset(_)
                | FlowFinishReason::OpenFailed(_),
            ) => {
                // The reducer already owns this terminal decision. Clear the
                // uplink first so generic rearm cannot manufacture a second
                // LocalReset event for a retired flow.
                ctx.resumable_action_bridge = None;
                ctx.resumable_uplink = None;
                ctx.resumable_terminal = None;
                let socket = sockets.get_mut::<TcpSocket>(handle);
                legacy_reasoned_rearm_with_gate(
                    gate,
                    handle,
                    socket,
                    ctx,
                    fake_pool,
                    now_secs,
                    "owner_to_local",
                    "owned_flow_terminal",
                );
                false
            }
        },
    }
}

#[cfg(test)]
fn finish_deferred_relay_close_if_drained(
    handle: SocketHandle,
    socket: &mut TcpSocket<'_>,
    ctx: &mut SocketCtx,
    fake_pool: &mut FakeIpPool,
    now_secs: u64,
    downlink_backpressure: DownlinkBackpressureConfig,
    close_egress_drop_guard: bool,
) -> bool {
    finish_deferred_relay_close_with_gate(
        RearmGate::legacy_facade(),
        handle,
        socket,
        ctx,
        fake_pool,
        now_secs,
        downlink_backpressure,
        close_egress_drop_guard,
    )
}

fn finish_deferred_relay_close_from_loop(
    dispatch: &OpenDispatch,
    handle: SocketHandle,
    socket: &mut TcpSocket<'_>,
    ctx: &mut SocketCtx,
    fake_pool: &mut FakeIpPool,
    now_secs: u64,
    downlink_backpressure: DownlinkBackpressureConfig,
    close_egress_drop_guard: bool,
) -> bool {
    finish_deferred_relay_close_with_gate(
        RearmGate::original_loop(dispatch),
        handle,
        socket,
        ctx,
        fake_pool,
        now_secs,
        downlink_backpressure,
        close_egress_drop_guard,
    )
}

fn finish_deferred_relay_close_with_gate(
    gate: RearmGate<'_>,
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
    if !gate_legacy_rearm(
        gate,
        RearmIntent::reasoned_ctx(handle, ctx, close.direction, close.reason, None),
    ) {
        return true;
    }
    if !ctx.downlink_pending.is_empty() {
        return true;
    }
    if ctx.d16_downlink_queue.is_some()
        && close.direction == "remote_to_local"
        && close.reason == "remote_eof"
    {
        let (queue_closed, queue_empty) = d16_remote_eof_queue_state(ctx).unwrap_or((false, false));
        if !remote_eof_ready_for_local_close(
            queue_closed,
            queue_empty,
            ctx.downlink_pending.len(),
            ctx.downlink_inflight_permit_bytes(),
        ) {
            return true;
        }
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
    legacy_reasoned_rearm_with_gate(
        gate,
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

struct SpawnedRemoteRelay {
    read_credit_tx: watch::Sender<RelayReadCredit>,
    d16_downlink_queue: Option<AsyncLeasedByteFlowQueue>,
}

fn install_spawned_remote_relay(
    ctx: &mut SocketCtx,
    spawned: SpawnedRemoteRelay,
    hard_pause_active: bool,
) {
    let d16_flow = spawned.d16_downlink_queue.is_some();
    ctx.relay_read_credit_tx = Some(spawned.read_credit_tx);
    ctx.d16_downlink_queue = spawned.d16_downlink_queue;
    ctx.d16_tun_rx_ack_barrier = false;
    ctx.d16_egress_phase = if d16_flow && hard_pause_active {
        EgressPhase::DrainOnly
    } else {
        EgressPhase::Running
    };
}

fn d16_process_global_budget() -> D16GlobalByteBudget {
    static BUDGET: std::sync::OnceLock<D16GlobalByteBudget> = std::sync::OnceLock::new();
    BUDGET.get_or_init(D16GlobalByteBudget::new_default).clone()
}

#[allow(clippy::too_many_arguments)]
fn spawn_remote_relay(
    relay_policy: &RelayFamilyPolicy,
    handle: SocketHandle,
    epoch: u64,
    relay: OpenedTcpRelay,
    rx: mpsc::Receiver<RelayCommand>,
    back_tx: mpsc::Sender<(SocketHandle, RelayEvent)>,
    metrics_handle: &Metrics,
    downlink_backpressure: DownlinkBackpressureConfig,
    initial_read_credit: RelayReadCredit,
) -> SpawnedRemoteRelay {
    // 刀11：每条新 TCP flow 开远端成功后在此唯一入口计一次（覆盖 inline + spawned remote-open 两条路）。
    metrics_handle.inc_relays_spawned();
    let (read_credit_tx, read_credit_rx) = watch::channel(initial_read_credit);
    let stream = match relay {
        OpenedTcpRelay::Generic(stream) => stream,
        OpenedTcpRelay::NativeByteOwned(native) => {
            let Ok(queue) = AsyncLeasedByteFlowQueue::new_with_global_budget(
                D16_PER_FLOW_BYTE_CAPACITY,
                DownstreamPermitReleaseMode::OnEgressDrain,
                d16_process_global_budget(),
            ) else {
                let _ = back_tx.try_send((
                    handle,
                    RelayEvent::Closed(RelayClose {
                        epoch,
                        direction: "remote_to_local",
                        reason: "d16_queue_config_failed",
                    }),
                ));
                return SpawnedRemoteRelay {
                    read_credit_tx,
                    d16_downlink_queue: None,
                };
            };
            tcp_diag_log!(
                "🔎 tcp-relay-engine handle={:?} epoch={} engine=d16_byte_owned per_flow_cap={} global_cap={} quantum={}",
                handle,
                epoch,
                D16_PER_FLOW_BYTE_CAPACITY,
                D16_GLOBAL_BYTE_BUDGET_DEFAULT_BYTES,
                D16_ACTOR_QUANTUM_BYTES,
            );
            tokio::spawn(run_relay_d16(
                handle,
                epoch,
                native.reader,
                native.writer,
                rx,
                back_tx,
                read_credit_rx,
                queue.clone(),
            ));
            return SpawnedRemoteRelay {
                read_credit_tx,
                d16_downlink_queue: Some(queue),
            };
        }
        OpenedTcpRelay::Native(native) => {
            match relay_policy.native_engine() {
                NativeTcpRelayEngine::ChunkPump => {
                    tokio::spawn(run_relay_native_chunk(
                        handle,
                        epoch,
                        native.reader,
                        native.writer,
                        rx,
                        back_tx,
                        read_credit_rx,
                        false,
                    ));
                }
                NativeTcpRelayEngine::PermitPump => {
                    let native_read_service_floor = relay_policy.native_read_floor();
                    let queue_capacity_bytes =
                        native_tcp_permit_queue_capacity_bytes(downlink_backpressure);
                    tokio::spawn(run_relay_native_permit(
                        handle,
                        epoch,
                        native.reader,
                        native.writer,
                        rx,
                        back_tx,
                        read_credit_rx,
                        queue_capacity_bytes,
                        native_read_service_floor,
                    ));
                }
            }
            return SpawnedRemoteRelay {
                read_credit_tx,
                d16_downlink_queue: None,
            };
        }
    };
    let engine = relay_policy.generic_engine();
    launch_generic_relay(GenericLaunchRequest {
        engine,
        handle,
        epoch,
        stream,
        rx,
        back_tx,
        read_credit_rx,
        downlink_backpressure,
    });
    SpawnedRemoteRelay {
        read_credit_tx,
        d16_downlink_queue: None,
    }
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

fn native_tcp_permit_queue_capacity_bytes(cfg: DownlinkBackpressureConfig) -> usize {
    cfg.high_bytes.max(1)
}

fn native_tcp_read_credit_limit(read_credit: RelayReadCredit) -> usize {
    if read_credit.paused || read_credit.max_batch_bytes == 0 {
        0
    } else {
        read_credit
            .max_batch_bytes
            .min(RELAY_REMOTE_READ_BURST_MAX_BYTES)
            .max(1)
    }
}

fn native_tcp_read_service_limit(
    read_credit: RelayReadCredit,
    native_read_service_floor: bool,
) -> usize {
    let limit = native_tcp_read_credit_limit(read_credit);
    if limit == 0 || !native_read_service_floor {
        return limit;
    }

    limit
        .max(RELAY_REMOTE_DISPATCH_SEGMENT_MAX_BYTES)
        .min(RELAY_REMOTE_READ_BURST_MAX_BYTES)
}

fn log_native_tcp_read_credit_pause(
    handle: SocketHandle,
    epoch: u64,
    engine: &'static str,
    diag: &RelayTaskDiag,
) {
    tcp_diag_log!(
        "🔎 tcp-relay-read-credit handle={:?} epoch={} paused=true max_batch_bytes=0 updates={} pause_updates={} engine={} ignored_for_remote_read=false",
        handle,
        epoch,
        diag.read_credit_updates,
        diag.read_credit_pause_updates,
        engine
    );
}

#[derive(Debug)]
struct ThinRelayPayload {
    epoch: u64,
    payload: Vec<u8>,
    stream_service_window: StreamServiceWindow,
}

async fn dispatch_thin_payload_to_main(
    handle: SocketHandle,
    payload: ThinRelayPayload,
    back_tx: &mpsc::Sender<(SocketHandle, RelayEvent)>,
    diag: &mut RelayTaskDiag,
    global_rx_pressure_threshold: std::time::Duration,
) -> Result<(), RelayCloseReason> {
    let payload_len = payload.payload.len();
    note_global_rx_channel_occupancy(diag, back_tx);
    let wait_started = std::time::Instant::now();
    let permit_result = back_tx.reserve().await;
    let waited = wait_started.elapsed();
    diag.note_global_rx_wait(waited, global_rx_pressure_threshold);
    let Ok(permit) = permit_result else {
        return Err(("remote_to_local", "global_rx_closed"));
    };
    note_global_rx_channel_occupancy(diag, back_tx);
    let mut stream_service_window = payload.stream_service_window;
    stream_service_window.note_global_rx_wait(waited, global_rx_pressure_threshold);
    stream_service_window
        .note_global_rx_queue(diag.global_rx_queue_used_max, diag.global_rx_queue_capacity);
    permit.send((
        handle,
        RelayEvent::Data {
            epoch: payload.epoch,
            bytes: payload.payload,
            stream_service_window,
            downstream_permit: None,
        },
    ));
    note_global_rx_channel_occupancy(diag, back_tx);
    if waited >= global_rx_pressure_threshold {
        tcp_diag_log!(
            "🔎 tcp-global-rx-pressure handle={:?} wait_us={} payload_bytes={} pressure_events={} engine=thin_staging",
            handle,
            waited.as_micros(),
            payload_len,
            diag.global_rx_pressure_events
        );
    }
    Ok(())
}

async fn send_remote_payload_batch_to_thin_stage(
    handle: SocketHandle,
    epoch: u64,
    payload: Vec<u8>,
    batch_chunks: usize,
    thin_tx: &mpsc::Sender<ThinRelayPayload>,
    diag: &mut RelayTaskDiag,
) -> Result<(), RelayCloseReason> {
    let payload_len = payload.len();
    diag.note_remote_batch(batch_chunks, payload_len);
    let wait_started = std::time::Instant::now();
    let permit_result = thin_tx.reserve().await;
    let waited = wait_started.elapsed();
    let Ok(permit) = permit_result else {
        return Err(("remote_to_local", "thin_stage_closed"));
    };
    if waited >= std::time::Duration::from_millis(5) {
        tcp_diag_log!(
            "🔎 tcp-thin-relay-stage-pressure handle={:?} wait_us={} payload_bytes={} staged_capacity={}",
            handle,
            waited.as_micros(),
            payload_len,
            thin_tx.max_capacity()
        );
    }
    permit.send(ThinRelayPayload {
        epoch,
        payload,
        stream_service_window: diag.stream_service_window(),
    });
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn send_remote_payload_staged_to_thin_stage(
    handle: SocketHandle,
    epoch: u64,
    payload: Vec<u8>,
    batch_chunks: usize,
    dispatch_segment_max_bytes: usize,
    thin_tx: &mpsc::Sender<ThinRelayPayload>,
    diag: &mut RelayTaskDiag,
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
        send_remote_payload_batch_to_thin_stage(
            handle,
            epoch,
            segment,
            segment_chunks,
            thin_tx,
            diag,
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
async fn drain_ready_remote_reads_to_thin_stage(
    remote_reader: &mut tokio::io::ReadHalf<RelayStream>,
    handle: SocketHandle,
    epoch: u64,
    initial_payload: Vec<u8>,
    buf: &mut [u8],
    thin_tx: &mpsc::Sender<ThinRelayPayload>,
    diag: &mut RelayTaskDiag,
    read_credit: RelayReadCredit,
) -> Result<(RemoteReadBurst, Option<RelayCloseReason>), RelayCloseReason> {
    let mut burst = RemoteReadBurst::default();
    let byte_limit = RELAY_REMOTE_READ_BURST_MAX_BYTES
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
    send_remote_payload_staged_to_thin_stage(
        handle,
        epoch,
        batch,
        burst.chunks,
        RELAY_REMOTE_DISPATCH_SEGMENT_MAX_BYTES,
        thin_tx,
        diag,
    )
    .await?;
    Ok((burst, close_after_batch))
}

fn send_relay_reader_progress(
    signal_tx: &mpsc::Sender<RelayReaderSignal>,
    diag: &RelayTaskDiag,
) -> bool {
    match signal_tx.try_send(RelayReaderSignal::Progress { diag: diag.clone() }) {
        Ok(()) => true,
        Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => true,
        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => false,
    }
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
async fn run_relay_thin_reader(
    handle: SocketHandle,
    epoch: u64,
    mut remote_reader: tokio::io::ReadHalf<RelayStream>,
    thin_tx: mpsc::Sender<ThinRelayPayload>,
    mut read_credit_rx: watch::Receiver<RelayReadCredit>,
    signal_tx: mpsc::Sender<RelayReaderSignal>,
    local_finish_seen: Arc<std::sync::atomic::AtomicBool>,
    mut stop_rx: tokio::sync::oneshot::Receiver<()>,
) {
    let mut buf = [0u8; 65_536];
    let mut diag = RelayTaskDiag::default();
    let mut read_credit = *read_credit_rx.borrow();
    diag.note_read_credit(read_credit);
    loop {
        note_reader_local_finish_if_needed(&local_finish_seen, &mut diag);
        drain_relay_read_credit_updates(&mut read_credit_rx, &mut read_credit, &mut diag);
        let stream_poll_credit = relay_stream_poll_credit_for_local_admission(read_credit);
        let remote_read_len = diag.next_remote_read_len(stream_poll_credit, buf.len());
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
                    "🔎 tcp-relay-read-credit handle={:?} epoch={} paused=true max_batch_bytes=0 updates={} pause_updates={} engine=thin_staging",
                    handle,
                    epoch,
                    diag.read_credit_updates,
                    diag.read_credit_pause_updates
                );
            }
            continue;
        }

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
                match drain_ready_remote_reads_to_thin_stage(
                    &mut remote_reader,
                    handle,
                    epoch,
                    initial_payload,
                    &mut buf,
                    &thin_tx,
                    &mut diag,
                    stream_poll_credit,
                )
                .await
                {
                    Ok((_burst, close_after_batch)) => {
                        if !send_relay_reader_progress(&signal_tx, &diag) {
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

async fn run_thin_relay_dispatcher(
    handle: SocketHandle,
    back_tx: mpsc::Sender<(SocketHandle, RelayEvent)>,
    mut thin_rx: mpsc::Receiver<ThinRelayPayload>,
) {
    let mut diag = RelayTaskDiag::default();
    let global_rx_pressure_threshold = std::time::Duration::from_millis(5);
    while let Some(payload) = thin_rx.recv().await {
        if dispatch_thin_payload_to_main(
            handle,
            payload,
            &back_tx,
            &mut diag,
            global_rx_pressure_threshold,
        )
        .await
        .is_err()
        {
            return;
        }
    }
}

fn continuous_queue_push_close_reason(err: ByteQueuePushError) -> RelayCloseReason {
    match err {
        ByteQueuePushError::Closed => ("remote_to_local", "continuous_queue_closed"),
        ByteQueuePushError::Full { .. } => ("remote_to_local", "continuous_queue_full"),
        ByteQueuePushError::EmptyChunk => ("remote_to_local", "continuous_queue_empty_chunk"),
        ByteQueuePushError::ChunkExceedsCapacity { .. } => {
            ("remote_to_local", "continuous_queue_chunk_too_large")
        }
    }
}

const NATIVE_TCP_REASSEMBLY_MAX_BYTES: usize = 4 * 1024 * 1024;
const NATIVE_TCP_GAP_LOG_MS: u128 = 1_000;

#[derive(Debug, Default)]
struct NativeTcpChunkAssembler {
    next_offset: u64,
    chunks: BTreeMap<u64, Bytes>,
    buffered_bytes: usize,
    max_buffered_bytes: usize,
    raw_chunks: u64,
    raw_bytes: u64,
    out_of_order_chunks: u64,
    max_gap_bytes: u64,
    cap_hits: u64,
    last_gap_log_at: Option<std::time::Instant>,
}

impl NativeTcpChunkAssembler {
    fn buffered_bytes(&self) -> usize {
        self.buffered_bytes
    }

    fn push_chunk(&mut self, chunk: NativeTcpChunk) {
        let mut offset = chunk.offset;
        let mut bytes = chunk.bytes;
        if bytes.is_empty() {
            return;
        }
        let end = offset.saturating_add(bytes.len() as u64);
        if end <= self.next_offset {
            return;
        }
        if offset < self.next_offset {
            let trim = (self.next_offset - offset) as usize;
            bytes = bytes.slice(trim..);
            offset = self.next_offset;
        }
        let gap = offset.saturating_sub(self.next_offset);
        self.raw_chunks = self.raw_chunks.saturating_add(1);
        self.raw_bytes = self.raw_bytes.saturating_add(bytes.len() as u64);
        if gap > 0 {
            self.out_of_order_chunks = self.out_of_order_chunks.saturating_add(1);
            self.max_gap_bytes = self.max_gap_bytes.max(gap);
        }
        if let Some(old) = self.chunks.get(&offset)
            && old.len() >= bytes.len()
        {
            return;
        }
        if let Some(old) = self.chunks.insert(offset, bytes.clone()) {
            self.buffered_bytes = self.buffered_bytes.saturating_sub(old.len());
        }
        self.buffered_bytes = self.buffered_bytes.saturating_add(bytes.len());
        self.max_buffered_bytes = self.max_buffered_bytes.max(self.buffered_bytes);
    }

    fn drain_ordered_up_to(&mut self, limit: usize) -> Option<Bytes> {
        if limit == 0 {
            return None;
        }
        let mut remaining = limit;
        let mut out = BytesMut::new();
        while remaining > 0 {
            let Some((&offset, _)) = self.chunks.first_key_value() else {
                break;
            };
            if offset > self.next_offset {
                break;
            }
            let Some((_, mut bytes)) = self.chunks.remove_entry(&offset) else {
                break;
            };
            self.buffered_bytes = self.buffered_bytes.saturating_sub(bytes.len());
            if offset < self.next_offset {
                let trim = (self.next_offset - offset) as usize;
                if trim >= bytes.len() {
                    continue;
                }
                bytes = bytes.slice(trim..);
            }
            let n = bytes.len().min(remaining);
            out.extend_from_slice(&bytes[..n]);
            self.next_offset = self.next_offset.saturating_add(n as u64);
            remaining -= n;
            if n < bytes.len() {
                let rest = bytes.slice(n..);
                self.buffered_bytes = self.buffered_bytes.saturating_add(rest.len());
                self.chunks.insert(self.next_offset, rest);
                break;
            }
        }
        if out.is_empty() {
            None
        } else {
            Some(out.freeze())
        }
    }

    fn note_cap_hit(&mut self) {
        self.cap_hits = self.cap_hits.saturating_add(1);
    }

    fn maybe_log_gap(&mut self, handle: SocketHandle, epoch: u64, reason: &'static str) {
        let Some((&chunk_offset, chunk)) = self.chunks.first_key_value() else {
            return;
        };
        if chunk_offset <= self.next_offset {
            return;
        }
        let now = std::time::Instant::now();
        if self
            .last_gap_log_at
            .map(|last| now.saturating_duration_since(last).as_millis() < NATIVE_TCP_GAP_LOG_MS)
            .unwrap_or(false)
        {
            return;
        }
        self.last_gap_log_at = Some(now);
        tcp_diag_log!(
            "🔎 tcp-native-chunk-gap handle={:?} epoch={} reason={} next_offset={} first_chunk_offset={} first_chunk_bytes={} gap_bytes={} max_gap_bytes={} buffered={}B buffered_chunks={} max_buffered={}B raw_chunks={} raw_bytes={} out_of_order_chunks={} cap_hits={} cap={}B",
            handle,
            epoch,
            reason,
            self.next_offset,
            chunk_offset,
            chunk.len(),
            chunk_offset.saturating_sub(self.next_offset),
            self.max_gap_bytes,
            self.buffered_bytes,
            self.chunks.len(),
            self.max_buffered_bytes,
            self.raw_chunks,
            self.raw_bytes,
            self.out_of_order_chunks,
            self.cap_hits,
            NATIVE_TCP_REASSEMBLY_MAX_BYTES
        );
    }
}

async fn poll_native_tcp_chunk(
    remote_reader: &mut NativeTcpReadHalf,
    max_len: usize,
) -> std::io::Result<Option<NativeTcpChunk>> {
    std::future::poll_fn(|cx| remote_reader.as_mut().poll_read_chunk(cx, max_len)).await
}

#[allow(clippy::too_many_arguments)]
async fn push_native_ordered_payload(
    handle: SocketHandle,
    epoch: u64,
    payload: Bytes,
    queue: &AsyncByteBoundedFlowQueue,
    signal_tx: &mpsc::Sender<RelayReaderSignal>,
    diag: &mut RelayTaskDiag,
) -> Result<bool, RelayCloseReason> {
    let _ = (handle, epoch);
    diag.note_remote_read(payload.len());
    if let Err(err) = queue.try_push(payload).await {
        let (direction, reason) = continuous_queue_push_close_reason(err);
        queue.close().await;
        return Err((direction, reason));
    }
    Ok(send_relay_reader_progress(signal_tx, diag))
}

#[allow(clippy::too_many_arguments)]
async fn push_native_ordered_leased_payload(
    handle: SocketHandle,
    epoch: u64,
    payload: Bytes,
    queue: &AsyncLeasedByteFlowQueue,
    signal_tx: &mpsc::Sender<RelayReaderSignal>,
    diag: &mut RelayTaskDiag,
) -> Result<bool, RelayCloseReason> {
    let _ = (handle, epoch);
    diag.note_remote_read(payload.len());
    if let Err(err) = queue.push(payload).await {
        let (direction, reason) = continuous_queue_push_close_reason(err);
        queue.close().await;
        return Err((direction, reason));
    }
    Ok(send_relay_reader_progress(signal_tx, diag))
}

#[allow(clippy::too_many_arguments)]
async fn run_relay_native_chunk_reader(
    handle: SocketHandle,
    epoch: u64,
    mut remote_reader: NativeTcpReadHalf,
    queue: AsyncByteBoundedFlowQueue,
    mut read_credit_rx: watch::Receiver<RelayReadCredit>,
    signal_tx: mpsc::Sender<RelayReaderSignal>,
    local_finish_seen: Arc<std::sync::atomic::AtomicBool>,
    mut stop_rx: tokio::sync::oneshot::Receiver<()>,
    native_read_service_floor: bool,
) {
    let mut diag = RelayTaskDiag::default();
    let mut read_credit = *read_credit_rx.borrow();
    diag.note_read_credit(read_credit);
    let mut assembler = NativeTcpChunkAssembler::default();
    let queue_wait_threshold = std::time::Duration::from_millis(5);
    loop {
        note_reader_local_finish_if_needed(&local_finish_seen, &mut diag);
        drain_relay_read_credit_updates(&mut read_credit_rx, &mut read_credit, &mut diag);
        let read_credit_limit =
            native_tcp_read_service_limit(read_credit, native_read_service_floor);
        if read_credit_limit == 0 {
            if read_credit.paused {
                log_native_tcp_read_credit_pause(handle, epoch, "native_chunk_pump", &diag);
            }
            let update = tokio::select! {
                _ = &mut stop_rx => {
                    queue.close().await;
                    return;
                }
                update = read_credit_rx.changed() => update,
            };
            if update.is_err() {
                queue.close().await;
                return;
            }
            read_credit = *read_credit_rx.borrow_and_update();
            diag.note_read_credit(read_credit);
            continue;
        }
        let queue_wait_started = std::time::Instant::now();
        let read_len = tokio::select! {
            _ = &mut stop_rx => {
                queue.close().await;
                return;
            }
            result = queue.wait_read_len(read_credit_limit) => {
                match result {
                    Ok(read_len) => {
                        let waited = queue_wait_started.elapsed();
                        diag.note_continuous_queue_wait(waited, queue_wait_threshold);
                        if waited >= queue_wait_threshold {
                            tcp_diag_log!(
                                "🔎 tcp-native-queue-wait handle={:?} epoch={} wait_us={} read_len={} wait_events={}",
                                handle,
                                epoch,
                                waited.as_micros(),
                                read_len,
                                diag.continuous_queue_wait_events
                            );
                        }
                        read_len
                    }
                    Err(_) => return,
                }
            }
        };
        if read_len == 0 {
            continue;
        }
        if let Some(payload) = assembler.drain_ordered_up_to(read_len) {
            match push_native_ordered_payload(handle, epoch, payload, &queue, &signal_tx, &mut diag)
                .await
            {
                Ok(true) => continue,
                Ok(false) => {
                    queue.close().await;
                    return;
                }
                Err((direction, reason)) => {
                    send_relay_reader_close(&signal_tx, direction, reason, diag).await;
                    return;
                }
            }
        }

        let available_reassembly = NATIVE_TCP_REASSEMBLY_MAX_BYTES
            .saturating_sub(assembler.buffered_bytes())
            .max(1);
        let native_read_len = read_len.min(available_reassembly);
        if assembler.buffered_bytes() >= NATIVE_TCP_REASSEMBLY_MAX_BYTES {
            assembler.note_cap_hit();
            assembler.maybe_log_gap(handle, epoch, "cap");
        }
        diag.note_remote_read_service_tick(native_read_len);
        let chunk = tokio::select! {
            _ = &mut stop_rx => {
                queue.close().await;
                return;
            }
            chunk = poll_native_tcp_chunk(&mut remote_reader, native_read_len) => chunk,
        };
        match chunk {
            Ok(Some(chunk)) => {
                diag.note_remote_raw_read(chunk.bytes.len());
                assembler.push_chunk(chunk);
                if let Some(payload) = assembler.drain_ordered_up_to(read_len) {
                    match push_native_ordered_payload(
                        handle, epoch, payload, &queue, &signal_tx, &mut diag,
                    )
                    .await
                    {
                        Ok(true) => {}
                        Ok(false) => {
                            queue.close().await;
                            return;
                        }
                        Err((direction, reason)) => {
                            send_relay_reader_close(&signal_tx, direction, reason, diag).await;
                            return;
                        }
                    }
                } else {
                    assembler.maybe_log_gap(handle, epoch, "gap");
                }
            }
            Ok(None) => {
                println!("远端服务器关闭了车厢 {:?}", handle);
                if let Some(payload) = assembler.drain_ordered_up_to(read_len) {
                    let _ = push_native_ordered_payload(
                        handle, epoch, payload, &queue, &signal_tx, &mut diag,
                    )
                    .await;
                }
                queue.close().await;
                send_relay_reader_close(&signal_tx, "remote_to_local", "remote_eof", diag).await;
                return;
            }
            Err(e) => {
                println!(
                    "读取上游流失败 direction=remote_to_local handle={:?} err={:?}",
                    handle, e
                );
                queue.close().await;
                send_relay_reader_close(&signal_tx, "remote_to_local", "remote_read_failed", diag)
                    .await;
                return;
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_relay_native_permit_reader(
    handle: SocketHandle,
    epoch: u64,
    mut remote_reader: NativeTcpReadHalf,
    queue: AsyncLeasedByteFlowQueue,
    mut read_credit_rx: watch::Receiver<RelayReadCredit>,
    signal_tx: mpsc::Sender<RelayReaderSignal>,
    local_finish_seen: Arc<std::sync::atomic::AtomicBool>,
    mut stop_rx: tokio::sync::oneshot::Receiver<()>,
    native_read_service_floor: bool,
) {
    let mut diag = RelayTaskDiag::default();
    let mut read_credit = *read_credit_rx.borrow();
    diag.note_read_credit(read_credit);
    let mut assembler = NativeTcpChunkAssembler::default();
    let queue_wait_threshold = std::time::Duration::from_millis(5);
    loop {
        note_reader_local_finish_if_needed(&local_finish_seen, &mut diag);
        drain_relay_read_credit_updates(&mut read_credit_rx, &mut read_credit, &mut diag);
        let read_credit_limit =
            native_tcp_read_service_limit(read_credit, native_read_service_floor);
        if read_credit_limit == 0 {
            if read_credit.paused {
                log_native_tcp_read_credit_pause(handle, epoch, "native_permit_pump", &diag);
            }
            let update = tokio::select! {
                _ = &mut stop_rx => {
                    queue.close().await;
                    return;
                }
                update = read_credit_rx.changed() => update,
            };
            if update.is_err() {
                queue.close().await;
                return;
            }
            read_credit = *read_credit_rx.borrow_and_update();
            diag.note_read_credit(read_credit);
            continue;
        }
        let queue_wait_started = std::time::Instant::now();
        let read_len = tokio::select! {
            _ = &mut stop_rx => {
                queue.close().await;
                return;
            }
            result = queue.wait_read_len(read_credit_limit) => {
                match result {
                    Ok(read_len) => {
                        let waited = queue_wait_started.elapsed();
                        diag.note_continuous_queue_wait(waited, queue_wait_threshold);
                        if waited >= queue_wait_threshold {
                            let snapshot = queue.snapshot().await;
                            tcp_diag_log!(
                                "🔎 tcp-native-permit-queue-wait handle={:?} epoch={} wait_us={} read_len={} queued_bytes={} leased_bytes={} capacity_bytes={} wait_events={}",
                                handle,
                                epoch,
                                waited.as_micros(),
                                read_len,
                                snapshot.queued_bytes,
                                snapshot.leased_bytes,
                                snapshot.capacity_bytes,
                                diag.continuous_queue_wait_events
                            );
                        }
                        read_len
                    }
                    Err(_) => return,
                }
            }
        };
        if read_len == 0 {
            continue;
        }
        if let Some(payload) = assembler.drain_ordered_up_to(read_len) {
            match push_native_ordered_leased_payload(
                handle, epoch, payload, &queue, &signal_tx, &mut diag,
            )
            .await
            {
                Ok(true) => continue,
                Ok(false) => {
                    queue.close().await;
                    return;
                }
                Err((direction, reason)) => {
                    send_relay_reader_close(&signal_tx, direction, reason, diag).await;
                    return;
                }
            }
        }

        let available_reassembly = NATIVE_TCP_REASSEMBLY_MAX_BYTES
            .saturating_sub(assembler.buffered_bytes())
            .max(1);
        let native_read_len = read_len.min(available_reassembly);
        if assembler.buffered_bytes() >= NATIVE_TCP_REASSEMBLY_MAX_BYTES {
            assembler.note_cap_hit();
            assembler.maybe_log_gap(handle, epoch, "permit_cap");
        }
        diag.note_remote_read_service_tick(native_read_len);
        let chunk = tokio::select! {
            _ = &mut stop_rx => {
                queue.close().await;
                return;
            }
            chunk = poll_native_tcp_chunk(&mut remote_reader, native_read_len) => chunk,
        };
        match chunk {
            Ok(Some(chunk)) => {
                diag.note_remote_raw_read(chunk.bytes.len());
                assembler.push_chunk(chunk);
                if let Some(payload) = assembler.drain_ordered_up_to(read_len) {
                    match push_native_ordered_leased_payload(
                        handle, epoch, payload, &queue, &signal_tx, &mut diag,
                    )
                    .await
                    {
                        Ok(true) => {}
                        Ok(false) => {
                            queue.close().await;
                            return;
                        }
                        Err((direction, reason)) => {
                            send_relay_reader_close(&signal_tx, direction, reason, diag).await;
                            return;
                        }
                    }
                } else {
                    assembler.maybe_log_gap(handle, epoch, "permit_gap");
                }
            }
            Ok(None) => {
                println!("远端服务器关闭了车厢 {:?}", handle);
                if let Some(payload) = assembler.drain_ordered_up_to(read_len) {
                    let _ = push_native_ordered_leased_payload(
                        handle, epoch, payload, &queue, &signal_tx, &mut diag,
                    )
                    .await;
                }
                queue.close().await;
                send_relay_reader_close(&signal_tx, "remote_to_local", "remote_eof", diag).await;
                return;
            }
            Err(e) => {
                println!(
                    "读取上游流失败 direction=remote_to_local handle={:?} err={:?}",
                    handle, e
                );
                queue.close().await;
                send_relay_reader_close(&signal_tx, "remote_to_local", "remote_read_failed", diag)
                    .await;
                return;
            }
        }
    }
}

async fn send_d16_data_ready(
    handle: SocketHandle,
    epoch: u64,
    back_tx: &mpsc::Sender<(SocketHandle, RelayEvent)>,
) -> bool {
    back_tx
        .send((handle, RelayEvent::DataReady { epoch }))
        .await
        .is_ok()
}

#[allow(clippy::too_many_arguments)]
async fn run_relay_d16(
    handle: SocketHandle,
    epoch: u64,
    remote_reader: NativeTcpReadHalf,
    remote_writer: NativeTcpWriteHalf,
    rx: mpsc::Receiver<RelayCommand>,
    back_tx: mpsc::Sender<(SocketHandle, RelayEvent)>,
    read_credit_rx: watch::Receiver<RelayReadCredit>,
    queue: AsyncLeasedByteFlowQueue,
) {
    let (writer_signal_tx, mut writer_signal_rx) = mpsc::channel(8);
    let (writer_stop_tx, writer_stop_rx) = tokio::sync::oneshot::channel();
    let (reader_stop_tx, reader_stop_rx) = tokio::sync::oneshot::channel();
    let (activity_tx, mut activity_rx) = mpsc::channel(1);
    let mut reader_task = tokio::spawn(run_d16_native_reader(
        handle,
        epoch,
        remote_reader,
        queue.clone(),
        read_credit_rx,
        back_tx.clone(),
        Some(activity_tx),
        reader_stop_rx,
    ));
    let mut writer_task = tokio::spawn(run_relay_writer(
        handle,
        remote_writer,
        rx,
        writer_signal_tx,
        writer_stop_rx,
    ));
    let mut close_timer = RelayCloseTimer::new();
    let mut reader_done = false;
    let mut writer_done = false;
    let mut activity_open = true;
    let mut writer_progress_events = 0u64;
    let mut writer_progress_bytes = 0u64;
    let mut writer_wait_max_us = 0u128;
    let mut half_closed_idle_blocked_events = 0u64;
    let mut half_closed_idle_blocked_max_payload_bytes = 0usize;
    let mut half_closed_local_progress_bytes = None;
    let terminal_error = loop {
        tokio::select! {
            activity = activity_rx.recv(), if !reader_done && activity_open => {
                match activity {
                    Some(()) => {
                        close_timer.note_read_progress();
                    }
                    None => activity_open = false,
                }
            }
            signal = writer_signal_rx.recv(), if !writer_done => {
                match signal {
                    Some(RelayWriterSignal::WritePending) => {
                        close_timer.arm_stalled_write();
                    }
                    Some(RelayWriterSignal::Progress { bytes, write_wait }) => {
                        writer_progress_events = writer_progress_events.saturating_add(1);
                        writer_progress_bytes =
                            writer_progress_bytes.saturating_add(bytes as u64);
                        writer_wait_max_us = writer_wait_max_us.max(write_wait.as_micros());
                        close_timer.note_write_progress();
                    }
                    Some(RelayWriterSignal::WriteCompleted) => {
                        close_timer.complete_write();
                    }
                    Some(RelayWriterSignal::WriteHalfClosed { .. }) => {
                        writer_done = true;
                        half_closed_local_progress_bytes = None;
                        close_timer.arm_half_closed();
                        if reader_done {
                            break None;
                        }
                    }
                    Some(RelayWriterSignal::Closed { direction, reason }) => {
                        writer_done = true;
                        if reason == "local_channel_closed" {
                            let queue_snapshot = queue.snapshot_now();
                            if let Some(cause) = queue_snapshot.closure.terminal_cause() {
                                break Some((cause.direction, cause.reason));
                            }
                            if queue_snapshot.closure.is_closed() {
                                if reader_done {
                                    break None;
                                }
                            } else {
                                break Some((direction, reason));
                            }
                        } else {
                            break Some((direction, reason));
                        }
                    }
                    None => {
                        break Some(("local_to_remote", "writer_task_closed"));
                    }
                }
            }
            result = &mut reader_task, if !reader_done => {
                reader_done = true;
                if result.is_err() {
                    break Some(("remote_to_local", "reader_task_failed"));
                }
                if writer_done {
                    break None;
                }
            }
            _ = close_timer.wait(), if close_timer.is_armed() => {
                if close_timer.is_half_closed() {
                    let queue_snapshot = queue.snapshot_now();
                    let payload_owned_bytes = queue_snapshot
                        .queued_bytes
                        .saturating_add(queue_snapshot.leased_bytes);
                    if queue_snapshot.closure.terminal_cause().is_none()
                        && payload_owned_bytes > 0
                    {
                        let previous_local_progress_bytes =
                            half_closed_local_progress_bytes.unwrap_or(0);
                        let first_owned_window = half_closed_local_progress_bytes.is_none();
                        let made_local_progress = queue_snapshot.local_progress_bytes
                            > previous_local_progress_bytes;
                        if first_owned_window || made_local_progress {
                            half_closed_local_progress_bytes =
                                Some(queue_snapshot.local_progress_bytes);
                            half_closed_idle_blocked_events =
                                half_closed_idle_blocked_events.saturating_add(1);
                            half_closed_idle_blocked_max_payload_bytes =
                                half_closed_idle_blocked_max_payload_bytes.max(payload_owned_bytes);
                            tcp_diag_log!(
                                "🔎 tcp-d16-half-closed-idle-blocked handle={:?} epoch={} blocked_events={} payload_owned_bytes={} local_progress_bytes={} previous_local_progress_bytes={} queue_queued={} queue_leased={} queue_reserved={} queue_closed={}",
                                handle,
                                epoch,
                                half_closed_idle_blocked_events,
                                payload_owned_bytes,
                                queue_snapshot.local_progress_bytes,
                                previous_local_progress_bytes,
                                queue_snapshot.queued_bytes,
                                queue_snapshot.leased_bytes,
                                queue_snapshot.reserved_bytes,
                                queue_snapshot.closure.is_closed(),
                            );
                            close_timer.arm_half_closed();
                            continue;
                        }
                    }
                }
                break Some(("timer", close_timer.terminal_reason()));
            }
        }
    };

    if let Some((direction, reason)) = terminal_error {
        let _ = queue
            .close_for_terminal_and_mark_ready(direction, reason)
            .await;
    }
    let _ = reader_stop_tx.send(());
    if terminal_error.is_some() && !reader_done {
        if tokio::time::timeout(RELAY_WRITER_STOP_TIMEOUT, &mut reader_task)
            .await
            .is_err()
        {
            reader_task.abort();
            let _ = (&mut reader_task).await;
        }
        reader_done = true;
    }

    if let Some((direction, reason)) = terminal_error {
        let _ = back_tx
            .send((
                handle,
                RelayEvent::Closed(RelayClose {
                    epoch,
                    direction,
                    reason,
                }),
            ))
            .await;
    }
    let _ = writer_stop_tx.send(());
    if !reader_done
        && tokio::time::timeout(RELAY_WRITER_STOP_TIMEOUT, &mut reader_task)
            .await
            .is_err()
    {
        reader_task.abort();
        let _ = reader_task.await;
    }
    if tokio::time::timeout(RELAY_WRITER_STOP_TIMEOUT, &mut writer_task)
        .await
        .is_err()
    {
        writer_task.abort();
        let _ = writer_task.await;
    }
    let queue_snapshot = queue.snapshot_now();
    let (terminal_direction, terminal_reason) =
        terminal_error.unwrap_or(("none", "clean_queue_lifecycle"));
    tcp_diag_log!(
        "🔎 tcp-d16-relay-close handle={:?} epoch={} terminal_direction={} terminal_reason={} writer_progress_events={} writer_progress_bytes={} writer_wait_max_us={} half_closed_idle_blocked_events={} half_closed_idle_blocked_max_payload_bytes={} queue_queued={} queue_leased={} queue_reserved={} queue_closed={}",
        handle,
        epoch,
        terminal_direction,
        terminal_reason,
        writer_progress_events,
        writer_progress_bytes,
        writer_wait_max_us,
        half_closed_idle_blocked_events,
        half_closed_idle_blocked_max_payload_bytes,
        queue_snapshot.queued_bytes,
        queue_snapshot.leased_bytes,
        queue_snapshot.reserved_bytes,
        queue_snapshot.closure.is_closed(),
    );
}

async fn run_d16_native_reader(
    handle: SocketHandle,
    epoch: u64,
    mut remote_reader: NativeTcpReadHalf,
    queue: AsyncLeasedByteFlowQueue,
    mut read_credit_rx: watch::Receiver<RelayReadCredit>,
    back_tx: mpsc::Sender<(SocketHandle, RelayEvent)>,
    activity_tx: Option<mpsc::Sender<()>>,
    mut stop_rx: tokio::sync::oneshot::Receiver<()>,
) {
    let mut read_credit = *read_credit_rx.borrow_and_update();
    'reader: loop {
        if read_credit.paused || read_credit.max_batch_bytes == 0 {
            let update = tokio::select! {
                _ = &mut stop_rx => {
                    queue
                        .close_for_terminal_and_mark_ready("local_to_remote", "reader_stopped")
                        .await;
                    return;
                }
                update = read_credit_rx.changed() => update,
            };
            if update.is_err() {
                queue
                    .close_for_terminal_and_mark_ready("local_to_remote", "read_credit_closed")
                    .await;
                return;
            }
            read_credit = *read_credit_rx.borrow_and_update();
            continue;
        }

        let reservation = tokio::select! {
            _ = &mut stop_rx => {
                queue
                    .close_for_terminal_and_mark_ready("local_to_remote", "reader_stopped")
                    .await;
                return;
            }
            update = read_credit_rx.changed() => {
                if update.is_err() {
                    queue
                        .close_for_terminal_and_mark_ready(
                            "local_to_remote",
                            "read_credit_closed",
                        )
                        .await;
                    return;
                }
                read_credit = *read_credit_rx.borrow_and_update();
                continue;
            }
            reservation = queue.reserve_read_up_to(read_credit.max_batch_bytes) => {
                match reservation {
                    Ok(reservation) => reservation,
                    Err(_) => return,
                }
            }
        };
        let max_len = reservation.max_len();
        if max_len == 0 {
            drop(reservation);
            continue;
        }

        let chunk = loop {
            let result = tokio::select! {
                _ = &mut stop_rx => {
                    drop(reservation);
                    queue
                        .close_for_terminal_and_mark_ready("local_to_remote", "reader_stopped")
                        .await;
                    return;
                }
                update = read_credit_rx.changed() => {
                    if update.is_err() {
                        drop(reservation);
                        queue
                            .close_for_terminal_and_mark_ready(
                                "local_to_remote",
                                "read_credit_closed",
                            )
                            .await;
                        return;
                    }
                    read_credit = *read_credit_rx.borrow_and_update();
                    if read_credit.paused || read_credit.max_batch_bytes == 0 {
                        drop(reservation);
                        continue 'reader;
                    }
                    continue;
                }
                chunk = poll_native_tcp_chunk(&mut remote_reader, max_len) => chunk,
            };
            break result;
        };
        match chunk {
            Ok(Some(chunk)) => match reservation.commit(chunk.bytes) {
                Ok(should_wake) => {
                    if let Some(activity_tx) = &activity_tx {
                        let _ = activity_tx.try_send(());
                    }
                    if should_wake && !send_d16_data_ready(handle, epoch, &back_tx).await {
                        queue
                            .close_for_terminal_and_mark_ready(
                                "remote_to_local",
                                "global_rx_closed",
                            )
                            .await;
                        return;
                    }
                }
                Err(_) => {
                    let should_wake = queue
                        .close_for_terminal_and_mark_ready(
                            "remote_to_local",
                            "d16_queue_commit_failed",
                        )
                        .await;
                    if should_wake {
                        let _ = send_d16_data_ready(handle, epoch, &back_tx).await;
                    }
                    let _ = back_tx
                        .send((
                            handle,
                            RelayEvent::Closed(RelayClose {
                                epoch,
                                direction: "remote_to_local",
                                reason: "d16_queue_commit_failed",
                            }),
                        ))
                        .await;
                    return;
                }
            },
            Ok(None) => {
                drop(reservation);
                let should_wake = queue.close_for_remote_eof_and_mark_ready().await;
                if should_wake {
                    let _ = send_d16_data_ready(handle, epoch, &back_tx).await;
                }
                return;
            }
            Err(_) => {
                drop(reservation);
                let should_wake = queue
                    .close_for_terminal_and_mark_ready("remote_to_local", "remote_read_failed")
                    .await;
                if should_wake {
                    let _ = send_d16_data_ready(handle, epoch, &back_tx).await;
                }
                let _ = back_tx
                    .send((
                        handle,
                        RelayEvent::Closed(RelayClose {
                            epoch,
                            direction: "remote_to_local",
                            reason: "remote_read_failed",
                        }),
                    ))
                    .await;
                return;
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_relay_continuous_reader(
    handle: SocketHandle,
    epoch: u64,
    mut remote_reader: tokio::io::ReadHalf<RelayStream>,
    queue: AsyncByteBoundedFlowQueue,
    mut read_credit_rx: watch::Receiver<RelayReadCredit>,
    signal_tx: mpsc::Sender<RelayReaderSignal>,
    local_finish_seen: Arc<std::sync::atomic::AtomicBool>,
    mut stop_rx: tokio::sync::oneshot::Receiver<()>,
) {
    let mut buf = [0u8; 65_536];
    let mut diag = RelayTaskDiag::default();
    let mut read_credit = *read_credit_rx.borrow();
    diag.note_read_credit(read_credit);
    let continuous_queue_wait_threshold = std::time::Duration::from_millis(5);
    loop {
        note_reader_local_finish_if_needed(&local_finish_seen, &mut diag);
        drain_relay_read_credit_updates(&mut read_credit_rx, &mut read_credit, &mut diag);
        let queue_wait_started = std::time::Instant::now();
        let read_len = tokio::select! {
            _ = &mut stop_rx => {
                queue.close().await;
                return;
            }
            update = read_credit_rx.changed() => {
                if update.is_err() {
                    queue.close().await;
                    return;
                }
                read_credit = *read_credit_rx.borrow_and_update();
                diag.note_read_credit(read_credit);
                if read_credit.paused {
                    tcp_diag_log!(
                        "🔎 tcp-relay-read-credit handle={:?} epoch={} paused=true max_batch_bytes=0 updates={} pause_updates={} engine=continuous_pump ignored_for_remote_read=true",
                        handle,
                        epoch,
                        diag.read_credit_updates,
                        diag.read_credit_pause_updates
                    );
                }
                continue;
            }
            result = queue.wait_read_len(buf.len()) => {
                match result {
                    Ok(read_len) => {
                        let waited = queue_wait_started.elapsed();
                        diag.note_continuous_queue_wait(
                            waited,
                            continuous_queue_wait_threshold,
                        );
                        if waited >= continuous_queue_wait_threshold {
                            tcp_diag_log!(
                                "🔎 tcp-continuous-queue-wait handle={:?} epoch={} wait_us={} read_len={} wait_events={}",
                                handle,
                                epoch,
                                waited.as_micros(),
                                read_len,
                                diag.continuous_queue_wait_events
                            );
                        }
                        read_len
                    }
                    Err(_) => return,
                }
            }
        };
        if read_len == 0 {
            continue;
        }
        diag.note_remote_read_service_tick(read_len);

        let remote_msg = tokio::select! {
            _ = &mut stop_rx => {
                queue.close().await;
                return;
            }
            remote_msg = remote_reader.read(&mut buf[..read_len]) => remote_msg,
        };
        match remote_msg {
            Ok(0) => {
                println!("远端服务器关闭了车厢 {:?}", handle);
                queue.close().await;
                send_relay_reader_close(&signal_tx, "remote_to_local", "remote_eof", diag).await;
                return;
            }
            Ok(n) => {
                note_reader_local_finish_if_needed(&local_finish_seen, &mut diag);
                diag.note_remote_read(n);
                let payload = Bytes::copy_from_slice(&buf[..n]);
                if let Err(err) = queue.try_push(payload).await {
                    let (direction, reason) = continuous_queue_push_close_reason(err);
                    queue.close().await;
                    send_relay_reader_close(&signal_tx, direction, reason, diag).await;
                    return;
                }
                if !send_relay_reader_progress(&signal_tx, &diag) {
                    queue.close().await;
                    return;
                }
            }
            Err(e) => {
                println!(
                    "读取上游流失败 direction=remote_to_local handle={:?} err={:?}",
                    handle, e
                );
                queue.close().await;
                send_relay_reader_close(&signal_tx, "remote_to_local", "remote_read_failed", diag)
                    .await;
                return;
            }
        }
    }
}

async fn run_continuous_relay_dispatcher(
    handle: SocketHandle,
    epoch: u64,
    queue: AsyncByteBoundedFlowQueue,
    back_tx: mpsc::Sender<(SocketHandle, RelayEvent)>,
) {
    let mut diag = RelayTaskDiag::default();
    let global_rx_pressure_threshold = std::time::Duration::from_millis(5);
    while let Some(payload) = queue
        .recv_up_to(RELAY_REMOTE_DISPATCH_SEGMENT_MAX_BYTES)
        .await
    {
        diag.note_remote_read(payload.len());
        if send_remote_payload_batch_to_main(
            handle,
            epoch,
            payload.to_vec(),
            1,
            &back_tx,
            &mut diag,
            global_rx_pressure_threshold,
        )
        .await
        .is_err()
        {
            queue.close().await;
            return;
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_relay_permit_reader(
    handle: SocketHandle,
    epoch: u64,
    mut remote_reader: tokio::io::ReadHalf<RelayStream>,
    queue: AsyncLeasedByteFlowQueue,
    mut read_credit_rx: watch::Receiver<RelayReadCredit>,
    signal_tx: mpsc::Sender<RelayReaderSignal>,
    local_finish_seen: Arc<std::sync::atomic::AtomicBool>,
    mut stop_rx: tokio::sync::oneshot::Receiver<()>,
    respect_read_credit: bool,
    engine_label: &'static str,
) {
    let mut buf = [0u8; 65_536];
    let mut diag = RelayTaskDiag::default();
    let mut read_credit = *read_credit_rx.borrow();
    diag.note_read_credit(read_credit);
    let queue_wait_threshold = std::time::Duration::from_millis(5);
    loop {
        note_reader_local_finish_if_needed(&local_finish_seen, &mut diag);
        drain_relay_read_credit_updates(&mut read_credit_rx, &mut read_credit, &mut diag);
        let read_credit_limit = if respect_read_credit {
            native_tcp_read_credit_limit(read_credit)
        } else {
            buf.len()
        };
        if read_credit_limit == 0 {
            if read_credit.paused {
                tcp_diag_log!(
                    "🔎 tcp-relay-read-credit handle={:?} epoch={} paused=true max_batch_bytes=0 updates={} pause_updates={} engine={} ignored_for_remote_read=false",
                    handle,
                    epoch,
                    diag.read_credit_updates,
                    diag.read_credit_pause_updates,
                    engine_label
                );
            }
            let update = tokio::select! {
                _ = &mut stop_rx => {
                    queue.close().await;
                    return;
                }
                update = read_credit_rx.changed() => update,
            };
            if update.is_err() {
                queue.close().await;
                return;
            }
            read_credit = *read_credit_rx.borrow_and_update();
            diag.note_read_credit(read_credit);
            continue;
        }
        let max_read_len = read_credit_limit.min(buf.len());
        let queue_wait_started = std::time::Instant::now();
        let read_len = tokio::select! {
            _ = &mut stop_rx => {
                queue.close().await;
                return;
            }
            result = queue.wait_read_len(max_read_len) => {
                match result {
                    Ok(read_len) => {
                        let waited = queue_wait_started.elapsed();
                        diag.note_continuous_queue_wait(waited, queue_wait_threshold);
                        if waited >= queue_wait_threshold {
                            let snapshot = queue.snapshot().await;
                            tcp_diag_log!(
                                "🔎 tcp-permit-queue-wait handle={:?} epoch={} wait_us={} read_len={} queued_bytes={} leased_bytes={} capacity_bytes={} wait_events={} engine={}",
                                handle,
                                epoch,
                                waited.as_micros(),
                                read_len,
                                snapshot.queued_bytes,
                                snapshot.leased_bytes,
                                snapshot.capacity_bytes,
                                diag.continuous_queue_wait_events,
                                engine_label
                            );
                        }
                        read_len
                    }
                    Err(_) => return,
                }
            }
        };
        if read_len == 0 {
            continue;
        }
        diag.note_remote_read_service_tick(read_len);

        let remote_msg = tokio::select! {
            _ = &mut stop_rx => {
                queue.close().await;
                return;
            }
            remote_msg = remote_reader.read(&mut buf[..read_len]) => remote_msg,
        };
        match remote_msg {
            Ok(0) => {
                println!("远端服务器关闭了车厢 {:?}", handle);
                queue.close().await;
                send_relay_reader_close(&signal_tx, "remote_to_local", "remote_eof", diag).await;
                return;
            }
            Ok(n) => {
                note_reader_local_finish_if_needed(&local_finish_seen, &mut diag);
                diag.note_remote_read(n);
                let payload = Bytes::copy_from_slice(&buf[..n]);
                if let Err(err) = queue.push(payload).await {
                    let (direction, reason) = continuous_queue_push_close_reason(err);
                    queue.close().await;
                    send_relay_reader_close(&signal_tx, direction, reason, diag).await;
                    return;
                }
                if !send_relay_reader_progress(&signal_tx, &diag) {
                    queue.close().await;
                    return;
                }
            }
            Err(e) => {
                println!(
                    "读取上游流失败 direction=remote_to_local handle={:?} err={:?}",
                    handle, e
                );
                queue.close().await;
                send_relay_reader_close(&signal_tx, "remote_to_local", "remote_read_failed", diag)
                    .await;
                return;
            }
        }
    }
}

async fn dispatch_leased_payload_to_main(
    handle: SocketHandle,
    epoch: u64,
    payload: LeasedBytes,
    back_tx: &mpsc::Sender<(SocketHandle, RelayEvent)>,
    diag: &mut RelayTaskDiag,
    global_rx_pressure_threshold: std::time::Duration,
) -> Result<(), RelayCloseReason> {
    let (bytes, downstream_permit) = payload.into_parts();
    let payload_len = bytes.len();
    diag.note_remote_batch(1, payload_len);
    note_global_rx_channel_occupancy(diag, back_tx);
    let wait_started = std::time::Instant::now();
    let permit_result = back_tx.reserve().await;
    let waited = wait_started.elapsed();
    diag.note_global_rx_wait(waited, global_rx_pressure_threshold);
    let Ok(permit) = permit_result else {
        return Err(("remote_to_local", "global_rx_closed"));
    };
    note_global_rx_channel_occupancy(diag, back_tx);
    permit.send((
        handle,
        RelayEvent::Data {
            epoch,
            bytes: bytes.to_vec(),
            stream_service_window: diag.stream_service_window(),
            downstream_permit: Some(downstream_permit),
        },
    ));
    note_global_rx_channel_occupancy(diag, back_tx);
    if waited >= global_rx_pressure_threshold {
        tcp_diag_log!(
            "🔎 tcp-global-rx-pressure handle={:?} wait_us={} payload_bytes={} pressure_events={} engine=permit_pump",
            handle,
            waited.as_micros(),
            payload_len,
            diag.global_rx_pressure_events
        );
    }
    Ok(())
}

async fn run_permit_relay_dispatcher(
    handle: SocketHandle,
    epoch: u64,
    queue: AsyncLeasedByteFlowQueue,
    back_tx: mpsc::Sender<(SocketHandle, RelayEvent)>,
) {
    let mut diag = RelayTaskDiag::default();
    let global_rx_pressure_threshold = std::time::Duration::from_millis(5);
    while let Some(payload) = queue
        .recv_up_to(RELAY_REMOTE_DISPATCH_SEGMENT_MAX_BYTES)
        .await
    {
        diag.note_remote_read(payload.bytes().len());
        if dispatch_leased_payload_to_main(
            handle,
            epoch,
            payload,
            &back_tx,
            &mut diag,
            global_rx_pressure_threshold,
        )
        .await
        .is_err()
        {
            queue.close().await;
            return;
        }
    }
}

async fn run_relay_native_chunk(
    handle: SocketHandle,
    epoch: u64,
    remote_reader: NativeTcpReadHalf,
    remote_writer: NativeTcpWriteHalf,
    rx: mpsc::Receiver<RelayCommand>,
    back_tx: mpsc::Sender<(SocketHandle, RelayEvent)>,
    read_credit_rx: watch::Receiver<RelayReadCredit>,
    native_read_service_floor: bool,
) {
    let (writer_signal_tx, mut writer_signal_rx) =
        mpsc::channel::<RelayWriterSignal>(RELAY_CHANNEL_CAPACITY);
    let (writer_stop_tx, writer_stop_rx) = tokio::sync::oneshot::channel::<()>();
    let (reader_signal_tx, mut reader_signal_rx) =
        mpsc::channel::<RelayReaderSignal>(RELAY_CHANNEL_CAPACITY);
    let (reader_stop_tx, reader_stop_rx) = tokio::sync::oneshot::channel::<()>();
    let local_finish_seen = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let read_credit_probe_rx = read_credit_rx.clone();
    tcp_diag_log!(
        "🔎 tcp-relay-engine handle={:?} epoch={} engine=native_chunk_pump queue_bytes={} reassembly_cap={} native_read_floor={}",
        handle,
        epoch,
        CONTINUOUS_TCP_RELAY_QUEUE_BYTES,
        NATIVE_TCP_REASSEMBLY_MAX_BYTES,
        native_read_service_floor
    );
    let Ok(queue) = AsyncByteBoundedFlowQueue::new(CONTINUOUS_TCP_RELAY_QUEUE_BYTES) else {
        let _ = back_tx
            .send((
                handle,
                RelayEvent::Closed(RelayClose {
                    epoch,
                    direction: "remote_to_local",
                    reason: "native_queue_config_failed",
                }),
            ))
            .await;
        return;
    };
    let mut dispatcher_task = tokio::spawn(run_continuous_relay_dispatcher(
        handle,
        epoch,
        queue.clone(),
        back_tx.clone(),
    ));
    let mut reader_task = tokio::spawn(run_relay_native_chunk_reader(
        handle,
        epoch,
        remote_reader,
        queue,
        read_credit_rx,
        reader_signal_tx,
        local_finish_seen.clone(),
        reader_stop_rx,
        native_read_service_floor,
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
    let mut close_timer = RelayCloseTimer::new();
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
                    Some(RelayWriterSignal::WritePending) => {
                        close_timer.arm_stalled_write();
                    }
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
                        close_timer.note_write_progress();
                    }
                    Some(RelayWriterSignal::WriteCompleted) => {
                        close_timer.complete_write();
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
                        close_timer.arm_half_closed();
                    }
                    Some(RelayWriterSignal::Closed { direction, reason }) => {
                        break (direction, reason);
                    }
                    None => {
                        break ("local_to_remote", "writer_task_closed");
                    }
                }
            }
            reader_signal = reader_signal_rx.recv() => {
                match reader_signal {
                    Some(RelayReaderSignal::Progress { diag: reader_diag }) => {
                        diag.copy_reader_fields_from(&reader_diag);
                        close_timer.note_read_progress();
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
            _ = close_timer.wait(), if close_timer.is_armed() => {
                let timeout = close_timer.timeout().unwrap_or_default();
                let reason = close_timer.terminal_reason();
                println!("⏱️ relay {:?} lifecycle guard {}s expired reason={}", handle, timeout.as_secs(), reason);
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
    if writer_task.is_finished() {
        let _ = writer_task.await;
    } else if tokio::time::timeout(RELAY_WRITER_STOP_TIMEOUT, &mut writer_task)
        .await
        .is_err()
    {
        writer_task.abort();
        let _ = writer_task.await;
    }
    if dispatcher_task.is_finished() {
        let _ = dispatcher_task.await;
    } else if tokio::time::timeout(RELAY_WRITER_STOP_TIMEOUT, &mut dispatcher_task)
        .await
        .is_err()
    {
        dispatcher_task.abort();
        let _ = dispatcher_task.await;
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
/// 交给 transport-neutral `OwnedUpstream`。Legacy adapter 内部仍只编码一次 TUIC Packet。
/// 中文要点：fake 无映射 → 丢弃(短 TTL 自愈)；send_udp 自带丢弃计数(UDP 语义)。
async fn handle_tuic_udp_uplink<U: OwnedUpstream>(
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
    let Some(association) = LocalAssociationId::new(assoc_id) else {
        // AssocTable 的 0 值保留为未分配；如果该不变式被破坏，本包 fail closed，
        // 不把伪造 association 交给上游。
        trace_log!(
            "⚠️ UDP↑ AssocTable 返回保留的 assoc=0，丢弃 {}B",
            udp.payload.len()
        );
        return;
    };
    upstream
        .send_udp(UdpUplink::new(association, &target, udp.payload))
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
mod tests;
