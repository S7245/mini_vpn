//! TUIC v5 client (Stage 13a) — command codec + client upstream.
//!
//! 中文要点：实现成熟的 TUIC v5 协议(出口对接 sing-box,client-only,见 ADR-0004)。
//! 本文件先落「命令编码」纯函数(TDD 主战场),字节布局**严格按 TUIC v5 规范**,与 sing-box 字节级互通。
//! 线格式参考见 docs/tech/2026-06-08-stage-13a-tuic-tcp-connect-plan.md。

use crate::metrics::{Metrics, note_pressure_edge};
use crate::quic;
use crate::shared::{ClientError, TargetAddr};
use crate::tcp_stream_service::StreamPendingFreshness;
use crate::udp_relay::{FlowEntry, FourTuple, MAX_UDP_FLOWS};
use crate::upstream::{
    DatagramUpstream, NativeTcpChunk, NativeTcpReadHalf, NativeTcpReader, NativeTcpRelayStream,
    OpenedTcpRelay, ProxyUpstream, RelayStream, TcpRelayOpenDiag, current_tcp_relay_open_diag,
};
use quinn::{Connection, Endpoint, VarInt};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::io;
use std::net::{Ipv4Addr, SocketAddr};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock, Weak};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::Mutex;
use tokio::sync::{Notify, mpsc, watch};

/// 默认 ALPN：TUIC over QUIC 常用 `h3`，必须与 sing-box `tls.alpn` 一致。
const DEFAULT_TUIC_ALPN: &str = "h3";
const DEFAULT_TUIC_SNI: &str = "localhost";
const DEFAULT_TUIC_CA_PATH: &str = "cert.pem";
/// 默认拥塞控制器 = **cubic**（刀3.5 真出口实测裁决，2026-06-17）：在高 RTT(深圳→US ~175ms)+
/// datagram 数据面上，Cubic 显著优于 BBR——40M offered 下 Cubic 39.8M/0.25%，BBR 仅 30.1M/24%
/// （cwnd 暴涨到 245K、RTT 178→252ms bufferbloat，对不可靠 datagram 过驱狂发不退）。quinn 0.10 的 BBR
/// 对 unreliable datagram 有害。BBR 仍可经 `MINI_VPN_TUIC_CC=bbr` 显式选用（实验/特定链路）。
const DEFAULT_TUIC_CC: &str = "cubic";
const DEFAULT_TUIC_UDP_MODE: &str = "native";
const DEFAULT_TUIC_MTU_POLICY: &str = "default";
const DEFAULT_TUIC_GSO_POLICY: &str = "enabled";
const DEFAULT_TUIC_UDP_SEND_SERVICE: &str = "quinn";
const DEFAULT_TUIC_PACING_POLICY: &str = "quinn";
const MIN_TUIC_TCP_POOL: usize = 1;
// Knife14cd/dq: keep TCP control/data streams separated by default. Pool=1
// remains an explicit A/B knob, but one high pool=1 run was not stable evidence.
const DEFAULT_TUIC_TCP_POOL: usize = 2;
const MAX_TUIC_TCP_POOL: usize = 16;
const TUIC_TCP_POOL_AUX_CONNECT_ATTEMPTS: usize = 3;
const TUIC_TCP_POOL_AUX_CONNECT_RETRY_BASE_MS: u64 = 250;
const DEFAULT_TUIC_QUIC_STATS_SECS: u64 = 30;
const TUIC_TCP_STREAM_READ_GAP_LOG_MS: u128 = 1_000;
const TUIC_TCP_STREAM_PENDING_LOG_MS: u128 = TUIC_TCP_STREAM_READ_GAP_LOG_MS;
const TUIC_TCP_STREAM_PENDING_SELF_WAKE_MS: u64 = 2;
const TUIC_TCP_RELAY_MODE_ORDERED_JOIN: &str = "ordered_join";
const TUIC_TCP_RELAY_MODE_ORDERED_CHUNK: &str = "ordered_chunk";
const TUIC_TCP_RELAY_MODE_UNORDERED_REASSEMBLY: &str = "unordered_reassembly_diag";
const TUIC_TCP_RELAY_MODE_NATIVE_CHUNK_PUMP: &str = "native_chunk_pump_diag";
const TUIC_TCP_RELAY_MODE_NATIVE_ORDERED_PUMP: &str = "native_ordered_pump_diag";
const TUIC_TCP_RELAY_MODE_D16_DIRECT_ORDERED: &str = "d16_direct_ordered";
// Categorical one-step service class for a newly opened TCP stream. This is not a throughput
// knob: the stream returns to its prior Quinn priority on the first accepted business write.
const TUIC_TCP_STARTUP_PRIORITY_DELTA: i32 = 1;
const TUIC_TCP_UNORDERED_CHUNK_READ_MAX_BYTES: usize = 64 * 1024;
const TUIC_TCP_DIRECT_ORDERED_READ_MAX_BYTES: usize = TUIC_TCP_UNORDERED_CHUNK_READ_MAX_BYTES * 2;
const TUIC_TCP_UNORDERED_REASSEMBLY_MAX_BYTES: usize = 4 * 1024 * 1024;
const TUIC_TCP_UNORDERED_STAGING_LOG_MS: u128 = 1_000;
const TUIC_TCP_NATIVE_ORDERED_PUMP_CHANNEL_CHUNKS: usize = 64;
const TUIC_TCP_NATIVE_ORDERED_PUMP_READ_CHUNKS: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TuicTcpRelayMode {
    OrderedJoin,
    OrderedChunk,
    UnorderedReassembly,
    NativeChunkPump,
    NativeOrderedPump,
    D16DirectOrdered,
}

impl TuicTcpRelayMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::OrderedJoin => TUIC_TCP_RELAY_MODE_ORDERED_JOIN,
            Self::OrderedChunk => TUIC_TCP_RELAY_MODE_ORDERED_CHUNK,
            Self::UnorderedReassembly => TUIC_TCP_RELAY_MODE_UNORDERED_REASSEMBLY,
            Self::NativeChunkPump => TUIC_TCP_RELAY_MODE_NATIVE_CHUNK_PUMP,
            Self::NativeOrderedPump => TUIC_TCP_RELAY_MODE_NATIVE_ORDERED_PUMP,
            Self::D16DirectOrdered => TUIC_TCP_RELAY_MODE_D16_DIRECT_ORDERED,
        }
    }
}

/// TUIC 客户端配置（单一事实源；桌面从 env 加载，移动端将来从 file/FFI 注入）。
/// 中文要点：凭据(uuid/password)经自定义 Debug **脱敏**，绝不随日志泄漏。
#[derive(Clone)]
pub struct TuicClientConfig {
    pub server: SocketAddr,
    pub uuid: [u8; 16],
    pub password: String,
    pub sni: String,
    pub ca_path: String,
    pub alpn: String,
    pub congestion_control: String,
    pub udp_relay_mode: String,
    /// QUIC MTU policy. `default` keeps the production 1280 + PLPMTUD behavior; `safe1200`
    /// is a bounded diagnostic/product profile for problematic paths.
    pub mtu_policy: String,
    /// Quinn UDP segmentation-offload policy. Enabled preserves the production default;
    /// disabled is an explicit packet-burst tracer-bullet profile.
    pub gso_policy: String,
    /// Aggregate client-endpoint UDP send service. `quinn` preserves the production path;
    /// `bounded` installs the fixed 48 wire-datagram / 2ms tracer profile.
    pub udp_send_service: String,
    /// Quinn pacing profile. `quinn` preserves the exact upstream default; `pacer-cap64`
    /// bounds each connection's stored pacer tokens; `endpoint-window-v1` installs the
    /// endpoint-owned aggregate byte service.
    pub pacing_policy: String,
    pub tcp_pool: usize,
    /// QUIC connection stats logging interval. `None` keeps normal runtime quiet; acceptance
    /// enables it through `MINI_VPN_TCP_DIAG=1`, or explicitly via
    /// `MINI_VPN_TUIC_QUIC_STATS_SECS`.
    pub quic_stats_secs: Option<u64>,
    /// 重连是否尝试 QUIC 0-RTT（**默认 false**：quinn 0.10 在 0-RTT 阶段不支持 `export_keying_material`，
    /// TUIC auth 必失败、自愈回落 1-RTT；显式开仅供实验/未来 quinn 升级。失败时总能回落，不致命）。
    pub zero_rtt: bool,
}

impl std::fmt::Debug for TuicClientConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TuicClientConfig")
            .field("server", &self.server)
            .field("uuid", &"<redacted>")
            .field("password", &"<redacted>")
            .field("sni", &self.sni)
            .field("ca_path", &self.ca_path)
            .field("alpn", &self.alpn)
            .field("congestion_control", &self.congestion_control)
            .field("udp_relay_mode", &self.udp_relay_mode)
            .field("mtu_policy", &self.mtu_policy)
            .field("gso_policy", &self.gso_policy)
            .field("udp_send_service", &self.udp_send_service)
            .field("pacing_policy", &self.pacing_policy)
            .field("tcp_pool", &self.tcp_pool)
            .field("quic_stats_secs", &self.quic_stats_secs)
            .field("zero_rtt", &self.zero_rtt)
            .finish()
    }
}

/// 解析带连字符的 UUID 字符串 → 16 字节。非法返回 None。
fn parse_uuid(s: &str) -> Option<[u8; 16]> {
    let hex: String = s.chars().filter(|c| *c != '-').collect();
    if hex.len() != 32 {
        return None;
    }
    let mut out = [0u8; 16];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(out)
}

impl TuicClientConfig {
    /// 从可选字符串源构建（server/uuid/password 必填）。
    pub fn from_sources(
        server: Option<&str>,
        uuid: Option<&str>,
        password: Option<&str>,
        sni: Option<&str>,
        ca_path: Option<&str>,
        alpn: Option<&str>,
    ) -> Result<Self, ClientError> {
        let server = server
            .ok_or_else(|| ClientError::InvalidTarget("tuic server addr required".into()))?
            .parse::<SocketAddr>()
            .map_err(|_| ClientError::InvalidTarget("invalid tuic server addr".into()))?;
        let uuid = parse_uuid(
            uuid.ok_or_else(|| ClientError::InvalidTarget("tuic uuid required".into()))?,
        )
        .ok_or_else(|| ClientError::InvalidTarget("invalid tuic uuid".into()))?;
        let password = password
            .filter(|p| !p.is_empty())
            .ok_or_else(|| ClientError::InvalidTarget("tuic password required".into()))?
            .to_string();
        Ok(Self {
            server,
            uuid,
            password,
            sni: sni.unwrap_or(DEFAULT_TUIC_SNI).to_string(),
            ca_path: ca_path.unwrap_or(DEFAULT_TUIC_CA_PATH).to_string(),
            alpn: alpn.unwrap_or(DEFAULT_TUIC_ALPN).to_string(),
            congestion_control: DEFAULT_TUIC_CC.to_string(),
            udp_relay_mode: DEFAULT_TUIC_UDP_MODE.to_string(),
            mtu_policy: DEFAULT_TUIC_MTU_POLICY.to_string(),
            gso_policy: DEFAULT_TUIC_GSO_POLICY.to_string(),
            udp_send_service: DEFAULT_TUIC_UDP_SEND_SERVICE.to_string(),
            pacing_policy: DEFAULT_TUIC_PACING_POLICY.to_string(),
            tcp_pool: DEFAULT_TUIC_TCP_POOL,
            quic_stats_secs: None,
            // 默认关：quinn 0.10 在 0-RTT 阶段不支持 export_keying_material（TUIC auth 必失败回落）。
            // 显式 `MINI_VPN_TUIC_ZERO_RTT=true` 可启用（实验/未来 quinn 升级）。
            zero_rtt: false,
        })
    }

    /// 从进程环境读取（`MINI_VPN_TUIC_*`）。
    pub fn from_env() -> Result<Self, ClientError> {
        let g = |k: &str| std::env::var(k).ok();
        let mut cfg = Self::from_sources(
            g("MINI_VPN_TUIC_SERVER").as_deref(),
            g("MINI_VPN_TUIC_UUID").as_deref(),
            g("MINI_VPN_TUIC_PASSWORD").as_deref(),
            g("MINI_VPN_TUIC_SNI").as_deref(),
            g("MINI_VPN_TUIC_CA_PATH").as_deref(),
            g("MINI_VPN_TUIC_ALPN").as_deref(),
        )?;
        cfg.zero_rtt = parse_zero_rtt(g("MINI_VPN_TUIC_ZERO_RTT").as_deref());
        // 刀3.5：CC / UDP relay mode 经 env 覆盖（A/B 归因必需）。原始字符串存字段，
        // 解析+回落在使用点（`parse_cc` / `UdpRelayMode::parse`）——存而未用的字段终于接上。
        cfg.congestion_control = override_field(cfg.congestion_control, g("MINI_VPN_TUIC_CC"));
        cfg.udp_relay_mode = override_field(cfg.udp_relay_mode, g("MINI_VPN_TUIC_UDP_MODE"));
        cfg.mtu_policy = override_field(
            cfg.mtu_policy,
            g("MINI_VPN_TUIC_MTU_MODE").or_else(|| g("MINI_VPN_TUIC_MTU_POLICY")),
        );
        cfg.gso_policy = override_field(
            cfg.gso_policy,
            g("MINI_VPN_TUIC_GSO_POLICY").or_else(|| g("MINI_VPN_TUIC_GSO")),
        );
        cfg.udp_send_service =
            override_field(cfg.udp_send_service, g("MINI_VPN_TUIC_UDP_SEND_SERVICE"));
        cfg.pacing_policy = override_field(cfg.pacing_policy, g("MINI_VPN_TUIC_PACING_POLICY"));
        cfg.tcp_pool = parse_tcp_pool(g("MINI_VPN_TUIC_TCP_POOL").as_deref());
        cfg.quic_stats_secs = parse_quic_stats_secs(
            g("MINI_VPN_TUIC_QUIC_STATS_SECS").as_deref(),
            g("MINI_VPN_TCP_DIAG").as_deref(),
            g("MINI_VPN_METRICS_SECS").as_deref(),
        );
        Ok(cfg)
    }
}

/// 用 env 值覆盖默认：非空白则取 env 值，否则保留默认。
/// 中文要点：抽成纯函数便于单测（不污染进程环境），与 `parse_zero_rtt` 同 idiom。
fn override_field(default: String, env_val: Option<String>) -> String {
    match env_val {
        Some(v) if !v.trim().is_empty() => v,
        _ => default,
    }
}

/// 解析 `MINI_VPN_TUIC_TCP_POOL`：默认 2；非法/空白回默认；0 裁到最小 1；大值裁到上限。
/// 中文要点：默认隔离控制/数据 TCP streams；显式 1 仍可复现单连接 A/B，且永不产生空连接池。
fn parse_tcp_pool(s: Option<&str>) -> usize {
    s.and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(DEFAULT_TUIC_TCP_POOL)
        .clamp(MIN_TUIC_TCP_POOL, MAX_TUIC_TCP_POOL)
}

/// 解析 QUIC stats 日志周期。
/// 中文要点：acceptance 已用 `MINI_VPN_TCP_DIAG=1` 打开 TCP 诊断，因此默认跟随该开关并复用
/// `MINI_VPN_METRICS_SECS` 周期；生产默认静默。显式 `MINI_VPN_TUIC_QUIC_STATS_SECS=0` 可关闭。
fn parse_quic_stats_secs(
    explicit: Option<&str>,
    tcp_diag: Option<&str>,
    metrics_secs: Option<&str>,
) -> Option<u64> {
    if let Some(v) = explicit {
        return v
            .trim()
            .parse::<u64>()
            .ok()
            .and_then(|secs| (secs > 0).then_some(secs));
    }
    if !parse_truthy(tcp_diag) {
        return None;
    }
    Some(
        metrics_secs
            .and_then(|v| v.trim().parse::<u64>().ok())
            .filter(|&secs| secs > 0)
            .unwrap_or(DEFAULT_TUIC_QUIC_STATS_SECS),
    )
}

fn parse_truthy(s: Option<&str>) -> bool {
    matches!(
        s.map(|v| v.trim().to_ascii_lowercase()).as_deref(),
        Some("true") | Some("1") | Some("on") | Some("yes")
    )
}

fn tcp_diag_enabled() -> bool {
    static TCP_DIAG: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *TCP_DIAG.get_or_init(|| parse_truthy(std::env::var("MINI_VPN_TCP_DIAG").ok().as_deref()))
}

fn parse_tuic_tcp_relay_mode(
    unordered_reassembly: Option<&str>,
    ordered_chunk: Option<&str>,
) -> TuicTcpRelayMode {
    if parse_truthy(unordered_reassembly) {
        TuicTcpRelayMode::UnorderedReassembly
    } else if parse_truthy(ordered_chunk) {
        TuicTcpRelayMode::OrderedChunk
    } else {
        TuicTcpRelayMode::OrderedJoin
    }
}

fn tuic_tcp_relay_mode() -> TuicTcpRelayMode {
    static MODE: std::sync::OnceLock<TuicTcpRelayMode> = std::sync::OnceLock::new();
    *MODE.get_or_init(|| {
        parse_tuic_tcp_relay_mode(
            std::env::var("MINI_VPN_TUIC_TCP_UNORDERED_REASSEMBLY")
                .ok()
                .as_deref(),
            std::env::var("MINI_VPN_TUIC_TCP_ORDERED_CHUNK")
                .ok()
                .as_deref(),
        )
    })
}

fn tuic_tcp_native_chunk_pump_enabled() -> bool {
    parse_truthy(
        std::env::var("MINI_VPN_TUIC_TCP_NATIVE_CHUNK_PUMP")
            .ok()
            .as_deref(),
    )
}

fn tuic_tcp_native_ordered_pump_enabled() -> bool {
    parse_truthy(
        std::env::var("MINI_VPN_TUIC_TCP_NATIVE_ORDERED_PUMP")
            .ok()
            .as_deref(),
    )
}

fn h10d16_byte_owned_egress_enabled() -> bool {
    parse_truthy(
        std::env::var("MINI_VPN_H10D16_BYTE_OWNED_EGRESS")
            .ok()
            .as_deref(),
    )
}

fn select_tuic_native_relay_mode(
    d16_byte_owned: bool,
    native_ordered_pump: bool,
    native_chunk_pump: bool,
) -> Option<TuicTcpRelayMode> {
    if d16_byte_owned {
        Some(TuicTcpRelayMode::D16DirectOrdered)
    } else if native_ordered_pump {
        Some(TuicTcpRelayMode::NativeOrderedPump)
    } else if native_chunk_pump {
        Some(TuicTcpRelayMode::NativeChunkPump)
    } else {
        None
    }
}

fn tcp_pool_slot_stale(now_secs: u64, last_used_secs: u64) -> bool {
    now_secs.saturating_sub(last_used_secs) >= TUIC_TCP_POOL_STALE_RECONNECT_SECS
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TcpPoolIdleAction {
    Reuse,
    Probe,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TcpPoolProbeOutcome {
    Alive { rx_before: u64, rx_after: u64 },
    Closed,
    SendFailed,
    TimedOut { rx_datagrams: u64 },
}

fn tcp_pool_idle_action(
    index: usize,
    now_secs: u64,
    last_used_secs: u64,
    idle_exclusive: bool,
) -> TcpPoolIdleAction {
    if index == 0 || !idle_exclusive || !tcp_pool_slot_stale(now_secs, last_used_secs) {
        TcpPoolIdleAction::Reuse
    } else {
        TcpPoolIdleAction::Probe
    }
}

fn should_probe_tcp_pool_generation(
    index: usize,
    now_secs: u64,
    last_used_secs: u64,
    idle_exclusive: bool,
    replacement_installed: bool,
) -> bool {
    !replacement_installed
        && tcp_pool_idle_action(index, now_secs, last_used_secs, idle_exclusive)
            == TcpPoolIdleAction::Probe
}

async fn probe_tcp_pool_connection(conn: &Connection, timeout: Duration) -> TcpPoolProbeOutcome {
    if conn.close_reason().is_some() {
        return TcpPoolProbeOutcome::Closed;
    }
    let rx_before = conn.stats().udp_rx.datagrams;
    if conn.send_datagram(encode_heartbeat().into()).is_err() {
        return TcpPoolProbeOutcome::SendFailed;
    }

    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if conn.close_reason().is_some() {
            return TcpPoolProbeOutcome::Closed;
        }
        let rx_after = conn.stats().udp_rx.datagrams;
        if rx_after > rx_before {
            return TcpPoolProbeOutcome::Alive {
                rx_before,
                rx_after,
            };
        }
        if tokio::time::Instant::now() >= deadline {
            return TcpPoolProbeOutcome::TimedOut {
                rx_datagrams: rx_after,
            };
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

fn tcp_pool_probe_reconnect_reason(outcome: TcpPoolProbeOutcome) -> Option<&'static str> {
    match outcome {
        TcpPoolProbeOutcome::Alive { .. } => None,
        TcpPoolProbeOutcome::Closed => Some("transport_closed"),
        TcpPoolProbeOutcome::SendFailed => Some("liveness_probe_send_failed"),
        TcpPoolProbeOutcome::TimedOut { .. } => Some("liveness_probe_timeout"),
    }
}

struct TcpPoolOpenState {
    generation: AtomicU64,
    last_success_secs_plus_one: AtomicU64,
    reconnect_required: AtomicBool,
}

impl TcpPoolOpenState {
    #[cfg(test)]
    fn new() -> Self {
        Self::new_at_generation(1)
    }

    fn new_at_generation(generation: u64) -> Self {
        Self {
            generation: AtomicU64::new(generation),
            last_success_secs_plus_one: AtomicU64::new(0),
            reconnect_required: AtomicBool::new(false),
        }
    }

    fn generation(&self) -> u64 {
        self.generation.load(Ordering::Relaxed)
    }

    fn note_open_success(&self, now_secs: u64) {
        self.last_success_secs_plus_one
            .store(now_secs.saturating_add(1), Ordering::Relaxed);
        self.reconnect_required.store(false, Ordering::Release);
    }

    fn note_open_failure(&self, index: usize) {
        if index != 0 {
            self.reconnect_required.store(true, Ordering::Release);
        }
    }

    fn needs_reconnect(&self, index: usize) -> bool {
        index != 0 && self.reconnect_required.load(Ordering::Acquire)
    }

    fn note_reconnect_success(&self) {
        self.generation.fetch_add(1, Ordering::Relaxed);
        self.reconnect_required.store(false, Ordering::Release);
    }

    fn last_success_age_secs(&self, now_secs: u64) -> Option<u64> {
        let encoded = self.last_success_secs_plus_one.load(Ordering::Relaxed);
        (encoded != 0).then(|| now_secs.saturating_sub(encoded - 1))
    }

    fn last_success_secs(&self) -> Option<u64> {
        let encoded = self.last_success_secs_plus_one.load(Ordering::Relaxed);
        (encoded != 0).then_some(encoded - 1)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TcpPoolAuxFailureAction {
    FailStartup,
    ContinueWithEstablished,
}

fn tcp_pool_aux_failure_action(established_conns: usize) -> TcpPoolAuxFailureAction {
    if established_conns == 0 {
        TcpPoolAuxFailureAction::FailStartup
    } else {
        TcpPoolAuxFailureAction::ContinueWithEstablished
    }
}

fn tcp_pool_aux_retry_delay(attempt: usize) -> Option<Duration> {
    if attempt + 1 >= TUIC_TCP_POOL_AUX_CONNECT_ATTEMPTS {
        return None;
    }
    Some(Duration::from_millis(
        TUIC_TCP_POOL_AUX_CONNECT_RETRY_BASE_MS.saturating_mul((attempt as u64) + 1),
    ))
}

struct TcpPoolGenerationActivity {
    active: AtomicU64,
    zero: Notify,
}

impl TcpPoolGenerationActivity {
    fn new() -> Self {
        Self {
            active: AtomicU64::new(0),
            zero: Notify::new(),
        }
    }

    fn active(&self) -> u64 {
        self.active.load(Ordering::Acquire)
    }

    fn try_increment(&self) -> Result<(), TcpPoolReservationError> {
        self.active
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                active.checked_add(1)
            })
            .map(|_| ())
            .map_err(|_| TcpPoolReservationError::Saturated)
    }

    fn try_increment_from(&self, active_before: u64) -> Result<(), TcpPoolReservationError> {
        let Some(active_after) = active_before.checked_add(1) else {
            return Err(TcpPoolReservationError::Saturated);
        };
        self.active
            .compare_exchange(
                active_before,
                active_after,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map(|_| ())
            .map_err(|_| TcpPoolReservationError::Busy)
    }

    fn decrement(&self) {
        let Ok(active_before) =
            self.active
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                    active.checked_sub(1)
                })
        else {
            return;
        };
        if active_before == 1 {
            // One drain task owns one predecessor. `notify_one` retains a permit when the exact
            // zero transition races between the waiter's load and its first poll.
            self.zero.notify_one();
        }
    }

    async fn wait_for_zero(&self) {
        loop {
            let zero = self.zero.notified();
            if self.active() == 0 {
                return;
            }
            zero.await;
        }
    }
}

struct TcpPoolSlotLease {
    slot_active: Arc<AtomicU64>,
    pool_active_total: Option<Arc<AtomicU64>>,
    generation_active: Option<Arc<TcpPoolGenerationActivity>>,
}

impl TcpPoolSlotLease {
    #[cfg(test)]
    fn from_reserved(active: Arc<AtomicU64>) -> Self {
        Self {
            slot_active: active,
            pool_active_total: None,
            generation_active: None,
        }
    }

    #[cfg(test)]
    fn reserve_generation(
        generation_active: Arc<TcpPoolGenerationActivity>,
        slot_active: Arc<AtomicU64>,
        pool_active_total: Arc<AtomicU64>,
    ) -> Result<Self, TcpPoolReservationError> {
        generation_active.try_increment()?;
        if slot_active
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                active.checked_add(1)
            })
            .is_err()
        {
            generation_active.decrement();
            return Err(TcpPoolReservationError::Saturated);
        }
        if pool_active_total
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                active.checked_add(1)
            })
            .is_err()
        {
            slot_active.fetch_sub(1, Ordering::AcqRel);
            generation_active.decrement();
            return Err(TcpPoolReservationError::Saturated);
        }
        Ok(Self {
            slot_active,
            pool_active_total: Some(pool_active_total),
            generation_active: Some(generation_active),
        })
    }

    fn from_generation_reserved(
        generation_active: Arc<TcpPoolGenerationActivity>,
        slot_active: Arc<AtomicU64>,
        pool_active_total: Arc<AtomicU64>,
    ) -> Result<Self, TcpPoolReservationError> {
        if slot_active
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                active.checked_add(1)
            })
            .is_err()
        {
            generation_active.decrement();
            return Err(TcpPoolReservationError::Saturated);
        }
        if pool_active_total
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                active.checked_add(1)
            })
            .is_err()
        {
            slot_active.fetch_sub(1, Ordering::AcqRel);
            generation_active.decrement();
            return Err(TcpPoolReservationError::Saturated);
        }
        Ok(Self {
            slot_active,
            pool_active_total: Some(pool_active_total),
            generation_active: Some(generation_active),
        })
    }

    fn try_clone(&self) -> Result<Self, TcpPoolReservationError> {
        if let Some(active) = &self.generation_active {
            active.try_increment()?;
        }
        if self
            .slot_active
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                active.checked_add(1)
            })
            .is_err()
        {
            if let Some(active) = &self.generation_active {
                active.decrement();
            }
            return Err(TcpPoolReservationError::Saturated);
        }
        if let Some(active) = &self.pool_active_total
            && active
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                    active.checked_add(1)
                })
                .is_err()
        {
            self.slot_active.fetch_sub(1, Ordering::AcqRel);
            if let Some(active) = &self.generation_active {
                active.decrement();
            }
            return Err(TcpPoolReservationError::Saturated);
        }
        Ok(Self {
            slot_active: self.slot_active.clone(),
            pool_active_total: self.pool_active_total.clone(),
            generation_active: self.generation_active.clone(),
        })
    }

    #[cfg(test)]
    fn reserve(active: Arc<AtomicU64>) -> (Self, bool) {
        match active.compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => (Self::from_reserved(active), true),
            Err(_) => {
                active.fetch_add(1, Ordering::AcqRel);
                (Self::from_reserved(active), false)
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TcpPoolReservationError {
    Empty,
    Busy,
    Saturated,
}

struct TcpPoolSlotPreparation {
    preparing: Arc<AtomicBool>,
    available: Arc<Notify>,
}

impl Drop for TcpPoolSlotPreparation {
    fn drop(&mut self) {
        self.preparing.store(false, Ordering::Release);
        self.available.notify_one();
    }
}

struct TcpPoolSlotReservation {
    index: usize,
    lease: TcpPoolSlotLease,
    active_before: u64,
    path_service: TcpPoolPathService,
    path_service_tiebreak: bool,
    qualification: TcpPoolForwardQualification,
    qualification_anchor: Option<u64>,
    black_holes_current: Option<u64>,
    qualification_override: bool,
    all_degraded_fallback: bool,
    candidates: Vec<TcpPoolAdmissionCandidate>,
    preparation: TcpPoolSlotPreparation,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum TcpPoolPathService {
    #[default]
    Unknown,
    Known {
        cwnd: u64,
        rtt_micros: u64,
    },
}

impl TcpPoolPathService {
    fn known(cwnd: u64, rtt: Duration) -> Self {
        let rtt_micros = u64::try_from(rtt.as_micros()).unwrap_or(u64::MAX);
        if cwnd == 0 || rtt_micros == 0 {
            return Self::Unknown;
        }
        Self::Known { cwnd, rtt_micros }
    }

    fn has_greater_service_than(self, other: Self) -> bool {
        match (self, other) {
            (Self::Unknown, _) => false,
            (Self::Known { .. }, Self::Unknown) => true,
            (
                Self::Known {
                    cwnd: left_cwnd,
                    rtt_micros: left_rtt,
                },
                Self::Known {
                    cwnd: right_cwnd,
                    rtt_micros: right_rtt,
                },
            ) => {
                u128::from(left_cwnd) * u128::from(right_rtt)
                    > u128::from(right_cwnd) * u128::from(left_rtt)
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TcpPoolTransportIdentity {
    stable_id: usize,
    generation: u64,
}

impl TcpPoolTransportIdentity {
    fn new(stable_id: usize, generation: u64) -> Self {
        Self {
            stable_id,
            generation,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum TcpPoolPathObservation {
    #[default]
    Unknown,
    Known {
        identity: TcpPoolTransportIdentity,
        path_service: TcpPoolPathService,
        black_holes_detected: u64,
    },
}

impl TcpPoolPathObservation {
    fn known(
        identity: TcpPoolTransportIdentity,
        cwnd: u64,
        rtt: Duration,
        black_holes_detected: u64,
    ) -> Self {
        Self::Known {
            identity,
            path_service: TcpPoolPathService::known(cwnd, rtt),
            black_holes_detected,
        }
    }

    fn path_service(self) -> TcpPoolPathService {
        match self {
            Self::Unknown => TcpPoolPathService::Unknown,
            Self::Known { path_service, .. } => path_service,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum TcpPoolForwardQualification {
    #[default]
    Unknown,
    Qualified,
    Degraded,
}

impl TcpPoolForwardQualification {
    fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Qualified => "qualified",
            Self::Degraded => "degraded",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TcpPoolQualificationDecision {
    qualification: TcpPoolForwardQualification,
    anchor: Option<u64>,
    current: Option<u64>,
}

impl TcpPoolQualificationDecision {
    fn unknown(anchor: Option<u64>, current: Option<u64>) -> Self {
        Self {
            qualification: TcpPoolForwardQualification::Unknown,
            anchor,
            current,
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct TcpPoolQualificationEpoch {
    identity: Option<TcpPoolTransportIdentity>,
    black_hole_anchor: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TcpPoolAdmissionCandidate {
    index: usize,
    identity: Option<TcpPoolTransportIdentity>,
    active_before: u64,
    path_service: TcpPoolPathService,
    qualification: TcpPoolForwardQualification,
    qualification_anchor: Option<u64>,
    black_holes_current: Option<u64>,
    admitted: bool,
}

struct TcpPoolAuxiliaryReplacementPreparation {
    index: usize,
    identity: TcpPoolTransportIdentity,
    active_before: u64,
    qualification_anchor: u64,
    black_holes_current: u64,
    candidates: Vec<TcpPoolAdmissionCandidate>,
    preparation: TcpPoolSlotPreparation,
}

enum TcpPoolAdmissionDecision {
    ReserveCurrent(TcpPoolSlotReservation),
    ReplaceAuxiliary(TcpPoolAuxiliaryReplacementPreparation),
}

struct TcpPoolAdmission {
    /// Logical-slot ownership includes the current generation plus any draining predecessor.
    active_slots: Vec<Arc<AtomicU64>>,
    /// Admission load is current-generation-only so a predecessor cannot penalize its successor.
    current_generation_activity: Vec<StdMutex<Arc<TcpPoolGenerationActivity>>>,
    pool_active_total: Arc<AtomicU64>,
    preparing_slots: Vec<Arc<AtomicBool>>,
    qualification_epochs: Vec<StdMutex<TcpPoolQualificationEpoch>>,
    available: Arc<Notify>,
}

impl TcpPoolAdmission {
    #[cfg(test)]
    fn new(pool_len: usize) -> Self {
        Self::with_current_generation_activity(
            (0..pool_len)
                .map(|_| Arc::new(TcpPoolGenerationActivity::new()))
                .collect(),
        )
    }

    fn with_current_generation_activity(
        current_generation_activity: Vec<Arc<TcpPoolGenerationActivity>>,
    ) -> Self {
        let pool_len = current_generation_activity.len();
        Self {
            active_slots: (0..pool_len).map(|_| Arc::new(AtomicU64::new(0))).collect(),
            current_generation_activity: current_generation_activity
                .into_iter()
                .map(StdMutex::new)
                .collect(),
            pool_active_total: Arc::new(AtomicU64::new(0)),
            preparing_slots: (0..pool_len)
                .map(|_| Arc::new(AtomicBool::new(false)))
                .collect(),
            qualification_epochs: (0..pool_len)
                .map(|_| StdMutex::new(TcpPoolQualificationEpoch::default()))
                .collect(),
            available: Arc::new(Notify::new()),
        }
    }

    fn qualification_for(
        &self,
        index: usize,
        active_before: u64,
        observation: TcpPoolPathObservation,
    ) -> TcpPoolQualificationDecision {
        let TcpPoolPathObservation::Known {
            identity,
            black_holes_detected,
            ..
        } = observation
        else {
            return TcpPoolQualificationDecision::unknown(None, None);
        };
        let Some(epoch) = self.qualification_epochs.get(index) else {
            return TcpPoolQualificationDecision::unknown(None, Some(black_holes_detected));
        };
        let Ok(epoch) = epoch.lock() else {
            return TcpPoolQualificationDecision::unknown(None, Some(black_holes_detected));
        };

        if active_before == 0 {
            return TcpPoolQualificationDecision {
                qualification: TcpPoolForwardQualification::Qualified,
                anchor: Some(black_holes_detected),
                current: Some(black_holes_detected),
            };
        }
        if epoch.identity != Some(identity) {
            return TcpPoolQualificationDecision::unknown(None, Some(black_holes_detected));
        }

        match black_holes_detected.cmp(&epoch.black_hole_anchor) {
            std::cmp::Ordering::Less => TcpPoolQualificationDecision::unknown(
                Some(epoch.black_hole_anchor),
                Some(black_holes_detected),
            ),
            std::cmp::Ordering::Equal => TcpPoolQualificationDecision {
                qualification: TcpPoolForwardQualification::Qualified,
                anchor: Some(epoch.black_hole_anchor),
                current: Some(black_holes_detected),
            },
            std::cmp::Ordering::Greater => TcpPoolQualificationDecision {
                qualification: TcpPoolForwardQualification::Degraded,
                anchor: Some(epoch.black_hole_anchor),
                current: Some(black_holes_detected),
            },
        }
    }

    fn commit_idle_epoch(
        &self,
        index: usize,
        identity: TcpPoolTransportIdentity,
        black_hole_anchor: u64,
    ) {
        let Some(epoch) = self.qualification_epochs.get(index) else {
            return;
        };
        let Ok(mut epoch) = epoch.lock() else {
            return;
        };
        epoch.identity = Some(identity);
        epoch.black_hole_anchor = black_hole_anchor;
    }

    fn try_decide(
        &self,
        observations: &[TcpPoolPathObservation],
        replacement_allowed: &[bool],
    ) -> Result<TcpPoolAdmissionDecision, TcpPoolReservationError> {
        if self.active_slots.is_empty() {
            return Err(TcpPoolReservationError::Empty);
        }
        debug_assert_eq!(self.active_slots.len(), self.preparing_slots.len());
        debug_assert_eq!(
            self.active_slots.len(),
            self.current_generation_activity.len()
        );
        debug_assert_eq!(self.active_slots.len(), self.qualification_epochs.len());

        loop {
            let mut preparing = false;
            let mut candidates = Vec::with_capacity(self.active_slots.len());
            for (index, slot_preparing) in self.preparing_slots.iter().enumerate() {
                if slot_preparing.load(Ordering::Acquire) {
                    preparing = true;
                    continue;
                }
                let Some(generation_active) = self.current_generation_activity(index) else {
                    continue;
                };
                let active_before = generation_active.active();
                if active_before == u64::MAX {
                    continue;
                }
                let observation = observations.get(index).copied().unwrap_or_default();
                let path_service = observation.path_service();
                let qualification = self.qualification_for(index, active_before, observation);
                let identity = match observation {
                    TcpPoolPathObservation::Unknown => None,
                    TcpPoolPathObservation::Known { identity, .. } => Some(identity),
                };
                candidates.push(TcpPoolAdmissionCandidate {
                    index,
                    identity,
                    active_before,
                    path_service,
                    qualification: qualification.qualification,
                    qualification_anchor: qualification.anchor,
                    black_holes_current: qualification.current,
                    admitted: true,
                });
            }

            if candidates.is_empty() {
                return Err(if preparing {
                    TcpPoolReservationError::Busy
                } else {
                    TcpPoolReservationError::Saturated
                });
            }

            let all_busy = candidates
                .iter()
                .all(|candidate| candidate.active_before != 0);
            let all_degraded_fallback = all_busy
                && candidates.iter().all(|candidate| {
                    candidate.qualification == TcpPoolForwardQualification::Degraded
                });
            let qualification_override = all_busy
                && !all_degraded_fallback
                && candidates.iter().any(|candidate| {
                    candidate.qualification == TcpPoolForwardQualification::Degraded
                });
            if qualification_override {
                for candidate in &mut candidates {
                    candidate.admitted =
                        candidate.qualification != TcpPoolForwardQualification::Degraded;
                }
            }

            let replacement = qualification_override.then(|| {
                candidates
                    .iter()
                    .copied()
                    .filter(|candidate| {
                        candidate.index != 0
                            && replacement_allowed
                                .get(candidate.index)
                                .copied()
                                .unwrap_or(false)
                            && candidate.active_before != 0
                            && candidate.qualification == TcpPoolForwardQualification::Degraded
                            && !candidate.admitted
                    })
                    .min_by_key(|candidate| (candidate.active_before, candidate.index))
            });
            if let Some(Some(replacement)) = replacement {
                let Some(identity) = replacement.identity else {
                    continue;
                };
                let Some(qualification_anchor) = replacement.qualification_anchor else {
                    continue;
                };
                let Some(black_holes_current) = replacement.black_holes_current else {
                    continue;
                };
                let slot_preparing = self.preparing_slots[replacement.index].clone();
                if slot_preparing
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                    .is_err()
                {
                    continue;
                }
                return Ok(TcpPoolAdmissionDecision::ReplaceAuxiliary(
                    TcpPoolAuxiliaryReplacementPreparation {
                        index: replacement.index,
                        identity,
                        active_before: replacement.active_before,
                        qualification_anchor,
                        black_holes_current,
                        candidates,
                        preparation: TcpPoolSlotPreparation {
                            preparing: slot_preparing,
                            available: self.available.clone(),
                        },
                    },
                ));
            }

            let mut choice: Option<TcpPoolAdmissionCandidate> = None;
            let mut path_service_tiebreak = false;
            for candidate in candidates
                .iter()
                .copied()
                .filter(|candidate| candidate.admitted)
            {
                match choice {
                    None => choice = Some(candidate),
                    Some(current) if candidate.active_before < current.active_before => {
                        choice = Some(candidate);
                        path_service_tiebreak = false;
                    }
                    Some(current)
                        if candidate.active_before == current.active_before
                            && candidate.active_before != 0 =>
                    {
                        if candidate
                            .path_service
                            .has_greater_service_than(current.path_service)
                        {
                            choice = Some(candidate);
                            path_service_tiebreak = true;
                        } else if current
                            .path_service
                            .has_greater_service_than(candidate.path_service)
                        {
                            path_service_tiebreak = true;
                        }
                    }
                    Some(_) => {}
                }
            }

            let Some(selected) = choice else {
                return Err(TcpPoolReservationError::Saturated);
            };
            let index = selected.index;
            let active_before = selected.active_before;
            let slot_preparing = self.preparing_slots[index].clone();
            if slot_preparing
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                continue;
            }
            let preparation = TcpPoolSlotPreparation {
                preparing: slot_preparing,
                available: self.available.clone(),
            };
            let Some(generation_active) = self.current_generation_activity(index) else {
                drop(preparation);
                continue;
            };
            if generation_active.try_increment_from(active_before).is_ok() {
                let slot_active = self.active_slots[index].clone();
                let lease = match TcpPoolSlotLease::from_generation_reserved(
                    generation_active,
                    slot_active,
                    self.pool_active_total.clone(),
                ) {
                    Ok(lease) => lease,
                    Err(error) => {
                        drop(preparation);
                        return Err(error);
                    }
                };
                if active_before == 0
                    && let (Some(identity), Some(black_hole_anchor)) =
                        (selected.identity, selected.black_holes_current)
                {
                    self.commit_idle_epoch(index, identity, black_hole_anchor);
                }
                return Ok(TcpPoolAdmissionDecision::ReserveCurrent(
                    TcpPoolSlotReservation {
                        index,
                        lease,
                        active_before,
                        path_service: selected.path_service,
                        path_service_tiebreak,
                        qualification: selected.qualification,
                        qualification_anchor: selected.qualification_anchor,
                        black_holes_current: selected.black_holes_current,
                        qualification_override,
                        all_degraded_fallback,
                        candidates,
                        preparation,
                    },
                ));
            }
            drop(preparation);
        }
    }

    #[cfg(test)]
    fn try_reserve(
        &self,
        observations: &[TcpPoolPathObservation],
    ) -> Result<TcpPoolSlotReservation, TcpPoolReservationError> {
        match self.try_decide(observations, &[])? {
            TcpPoolAdmissionDecision::ReserveCurrent(reservation) => Ok(reservation),
            TcpPoolAdmissionDecision::ReplaceAuxiliary(_) => {
                unreachable!("replacement is disabled for reserve-only admission")
            }
        }
    }

    #[cfg(test)]
    async fn reserve(
        &self,
        mut sample_observations: impl FnMut() -> Vec<TcpPoolPathObservation>,
    ) -> Result<TcpPoolSlotReservation, TcpPoolReservationError> {
        loop {
            let available = self.available.notified();
            let observations = sample_observations();
            match self.try_reserve(&observations) {
                Err(TcpPoolReservationError::Busy) => available.await,
                result => return result,
            }
        }
    }

    fn active_total(&self) -> u64 {
        self.pool_active_total.load(Ordering::Acquire)
    }

    fn current_generation_activity(&self, index: usize) -> Option<Arc<TcpPoolGenerationActivity>> {
        self.current_generation_activity
            .get(index)?
            .lock()
            .ok()
            .map(|activity| activity.clone())
    }

    fn replace_current_generation_activity(
        &self,
        index: usize,
        expected: &Arc<TcpPoolGenerationActivity>,
        successor: Arc<TcpPoolGenerationActivity>,
    ) -> bool {
        let Some(activity) = self.current_generation_activity.get(index) else {
            return false;
        };
        let Ok(mut activity) = activity.lock() else {
            return false;
        };
        if !Arc::ptr_eq(&activity, expected) {
            return false;
        }
        *activity = successor;
        true
    }

    fn reserve_replacement_successor(
        &self,
        mut replacement: TcpPoolAuxiliaryReplacementPreparation,
        successor_activity: Arc<TcpPoolGenerationActivity>,
        successor_observation: TcpPoolPathObservation,
    ) -> Result<TcpPoolSlotReservation, TcpPoolReservationError> {
        let TcpPoolPathObservation::Known {
            identity,
            path_service,
            black_holes_detected,
        } = successor_observation
        else {
            return Err(TcpPoolReservationError::Busy);
        };
        let current_activity = self
            .current_generation_activity(replacement.index)
            .ok_or(TcpPoolReservationError::Busy)?;
        if !Arc::ptr_eq(&current_activity, &successor_activity) {
            return Err(TcpPoolReservationError::Busy);
        }
        successor_activity.try_increment_from(0)?;
        let lease = TcpPoolSlotLease::from_generation_reserved(
            successor_activity,
            self.active_slots[replacement.index].clone(),
            self.pool_active_total.clone(),
        )?;
        self.commit_idle_epoch(replacement.index, identity, black_holes_detected);
        if let Some(candidate) = replacement
            .candidates
            .iter_mut()
            .find(|candidate| candidate.index == replacement.index)
        {
            *candidate = TcpPoolAdmissionCandidate {
                index: replacement.index,
                identity: Some(identity),
                active_before: 0,
                path_service,
                qualification: TcpPoolForwardQualification::Qualified,
                qualification_anchor: Some(black_holes_detected),
                black_holes_current: Some(black_holes_detected),
                admitted: true,
            };
        }
        Ok(TcpPoolSlotReservation {
            index: replacement.index,
            lease,
            active_before: 0,
            path_service,
            path_service_tiebreak: false,
            qualification: TcpPoolForwardQualification::Qualified,
            qualification_anchor: Some(black_holes_detected),
            black_holes_current: Some(black_holes_detected),
            qualification_override: true,
            all_degraded_fallback: false,
            candidates: replacement.candidates,
            preparation: replacement.preparation,
        })
    }

    #[cfg(test)]
    fn set_active_for_test(&self, index: usize, active: u64) {
        let generation = self
            .current_generation_activity(index)
            .expect("test slot generation must exist");
        generation.active.store(active, Ordering::Release);
        self.active_slots[index].store(active, Ordering::Release);
        let total = self.active_slots.iter().fold(0u64, |total, slot| {
            total.saturating_add(slot.load(Ordering::Acquire))
        });
        self.pool_active_total.store(total, Ordering::Release);
    }
}

fn note_tcp_pool_activity_transition(previous: &mut Option<u64>, active: u64) -> Option<u64> {
    if *previous == Some(active) {
        return None;
    }
    *previous = Some(active);
    Some(active)
}

impl Drop for TcpPoolSlotLease {
    fn drop(&mut self) {
        if let Some(active) = &self.generation_active {
            active.decrement();
        }
        let _ = self
            .slot_active
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                active.checked_sub(1)
            });
        if let Some(active) = &self.pool_active_total {
            let _ = active.fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                active.checked_sub(1)
            });
        }
    }
}

trait OrderedChunkRecv: Unpin {
    fn poll_read_ordered_chunk(
        &mut self,
        cx: &mut Context<'_>,
        max_len: usize,
    ) -> Poll<io::Result<Option<bytes::Bytes>>>;
}

struct QuinnOrderedChunkRecv {
    recv: quinn::RecvStream,
}

impl QuinnOrderedChunkRecv {
    fn new(recv: quinn::RecvStream) -> Self {
        Self { recv }
    }
}

impl OrderedChunkRecv for QuinnOrderedChunkRecv {
    fn poll_read_ordered_chunk(
        &mut self,
        cx: &mut Context<'_>,
        max_len: usize,
    ) -> Poll<io::Result<Option<bytes::Bytes>>> {
        let fut = self.recv.read_chunk(max_len, true);
        tokio::pin!(fut);
        match fut.poll(cx) {
            Poll::Ready(Ok(Some(chunk))) => Poll::Ready(Ok(Some(chunk.bytes))),
            Poll::Ready(Ok(None)) => Poll::Ready(Ok(None)),
            Poll::Ready(Err(err)) => {
                Poll::Ready(Err(io::Error::other(format!("tuic ordered read: {err}"))))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

trait TuicTcpStartupPriorityWrite: AsyncWrite + Unpin {
    fn current_priority(&mut self) -> io::Result<i32>;
    fn set_stream_priority(&mut self, priority: i32) -> io::Result<()>;
    fn poll_write_then_set_priority(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
        priority_after: i32,
    ) -> Poll<io::Result<usize>>;
}

impl TuicTcpStartupPriorityWrite for quinn::SendStream {
    fn current_priority(&mut self) -> io::Result<i32> {
        self.priority()
            .map_err(|error| io::Error::other(format!("tuic TCP priority read: {error}")))
    }

    fn set_stream_priority(&mut self, priority: i32) -> io::Result<()> {
        self.set_priority(priority)
            .map_err(|error| io::Error::other(format!("tuic TCP priority set: {error}")))
    }

    fn poll_write_then_set_priority(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
        priority_after: i32,
    ) -> Poll<io::Result<usize>> {
        match quinn::SendStream::poll_write_then_set_priority(self, cx, buf, priority_after) {
            Poll::Ready(Ok(written)) => Poll::Ready(Ok(written)),
            Poll::Ready(Err(error)) => Poll::Ready(Err(error.into())),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Keeps a new TUIC TCP stream in one higher Quinn service class through Connect and the first
/// accepted nonempty business write, then atomically rejoins its original class. No payload is
/// copied or queued here; Quinn remains the only owner of transport backpressure.
struct TuicTcpStartupWriter<W> {
    inner: W,
    original_priority: i32,
    startup_priority: i32,
    first_payload_pending: bool,
    diag_meta: Option<TuicTcpStreamDiagMeta>,
}

impl<W: TuicTcpStartupPriorityWrite> TuicTcpStartupWriter<W> {
    fn arm(mut inner: W) -> io::Result<Self> {
        let original_priority = inner.current_priority()?;
        let startup_priority = original_priority
            .checked_add(TUIC_TCP_STARTUP_PRIORITY_DELTA)
            .ok_or_else(|| io::Error::other("tuic TCP startup priority overflow"))?;
        inner.set_stream_priority(startup_priority)?;
        Ok(Self {
            inner,
            original_priority,
            startup_priority,
            first_payload_pending: true,
            diag_meta: None,
        })
    }

    async fn write_connect(&mut self, connect: &[u8]) -> io::Result<()> {
        tokio::io::AsyncWriteExt::write_all(&mut self.inner, connect).await
    }

    fn set_diag_meta(&mut self, diag_meta: Option<TuicTcpStreamDiagMeta>) {
        self.diag_meta = diag_meta;
    }
}

impl TuicTcpStartupWriter<quinn::SendStream> {
    async fn prepare(
        send: quinn::SendStream,
        connect: &[u8],
    ) -> io::Result<(Self, quinn::SendStreamProgress)> {
        let progress = send.progress_handle();
        let mut writer = Self::arm(send)?;
        writer.write_connect(connect).await?;
        Ok((writer, progress))
    }
}

impl<W: TuicTcpStartupPriorityWrite> AsyncWrite for TuicTcpStartupWriter<W> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if buf.is_empty() || !self.first_payload_pending {
            return Pin::new(&mut self.inner).poll_write(cx, buf);
        }

        let original_priority = self.original_priority;
        match Pin::new(&mut self.inner).poll_write_then_set_priority(cx, buf, original_priority) {
            Poll::Ready(Ok(written)) if written > 0 => {
                self.first_payload_pending = false;
                if let Some(meta) = &self.diag_meta {
                    println!(
                        "{}",
                        format_tuic_tcp_startup_service_line(
                            meta,
                            written,
                            self.startup_priority,
                            self.original_priority,
                        )
                    );
                }
                Poll::Ready(Ok(written))
            }
            other => other,
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

struct TuicOrderedRelayStream<
    R = QuinnOrderedChunkRecv,
    S = TuicTcpStartupWriter<quinn::SendStream>,
> {
    recv: R,
    send: S,
    pending: bytes::Bytes,
    recv_eof: bool,
}

impl TuicOrderedRelayStream<QuinnOrderedChunkRecv, TuicTcpStartupWriter<quinn::SendStream>> {
    fn from_quinn(recv: quinn::RecvStream, send: TuicTcpStartupWriter<quinn::SendStream>) -> Self {
        Self::new(QuinnOrderedChunkRecv::new(recv), send)
    }
}

impl<R, S> TuicOrderedRelayStream<R, S> {
    fn new(recv: R, send: S) -> Self {
        Self {
            recv,
            send,
            pending: bytes::Bytes::new(),
            recv_eof: false,
        }
    }

    fn drain_pending_into(&mut self, buf: &mut ReadBuf<'_>) -> usize {
        let n = self.pending.len().min(buf.remaining());
        if n == 0 {
            return 0;
        }
        let rest = if n < self.pending.len() {
            Some(self.pending.slice(n..))
        } else {
            None
        };
        buf.put_slice(&self.pending[..n]);
        self.pending = rest.unwrap_or_else(bytes::Bytes::new);
        n
    }
}

impl<R: OrderedChunkRecv, S: Unpin> AsyncRead for TuicOrderedRelayStream<R, S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        if self.drain_pending_into(buf) > 0 {
            return Poll::Ready(Ok(()));
        }
        if self.recv_eof {
            return Poll::Ready(Ok(()));
        }

        match self
            .recv
            .poll_read_ordered_chunk(cx, buf.remaining().max(1))
        {
            Poll::Ready(Ok(Some(chunk))) => {
                if chunk.is_empty() {
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
                self.pending = chunk;
                self.drain_pending_into(buf);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Ok(None)) => {
                self.recv_eof = true;
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<R: Unpin, S: AsyncWrite + Unpin> AsyncWrite for TuicOrderedRelayStream<R, S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.send).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.send).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.send).poll_shutdown(cx)
    }
}

#[derive(Debug, Default)]
struct OrderedQuicChunkAssembler {
    next_offset: u64,
    chunks: BTreeMap<u64, bytes::Bytes>,
    buffered_bytes: usize,
    max_buffered_bytes: usize,
}

impl OrderedQuicChunkAssembler {
    fn next_offset(&self) -> u64 {
        self.next_offset
    }

    fn buffered_bytes(&self) -> usize {
        self.buffered_bytes
    }

    fn buffered_chunks(&self) -> usize {
        self.chunks.len()
    }

    fn max_buffered_bytes(&self) -> usize {
        self.max_buffered_bytes
    }

    fn push_chunk(&mut self, mut offset: u64, mut bytes: bytes::Bytes) {
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

    fn drain_into(&mut self, buf: &mut ReadBuf<'_>) -> usize {
        let before = buf.filled().len();
        while buf.remaining() > 0 {
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
            let n = bytes.len().min(buf.remaining());
            buf.put_slice(&bytes[..n]);
            self.next_offset = self.next_offset.saturating_add(n as u64);
            if n < bytes.len() {
                let rest = bytes.slice(n..);
                self.buffered_bytes = self.buffered_bytes.saturating_add(rest.len());
                self.chunks.insert(self.next_offset, rest);
            }
        }
        buf.filled().len().saturating_sub(before)
    }
}

struct TuicChunkRelayStream {
    recv: quinn::RecvStream,
    send: TuicTcpStartupWriter<quinn::SendStream>,
    rx: OrderedQuicChunkAssembler,
    recv_eof: bool,
    diag_meta: Option<TuicTcpStreamDiagMeta>,
    last_staging_log_at: Option<Instant>,
    unordered_chunks: u64,
    unordered_bytes: u64,
    out_of_order_chunks: u64,
    max_gap_bytes: u64,
    staging_cap_hits: u64,
}

impl TuicChunkRelayStream {
    fn new(
        recv: quinn::RecvStream,
        send: TuicTcpStartupWriter<quinn::SendStream>,
        diag_meta: Option<TuicTcpStreamDiagMeta>,
    ) -> Self {
        Self {
            recv,
            send,
            rx: OrderedQuicChunkAssembler::default(),
            recv_eof: false,
            diag_meta,
            last_staging_log_at: None,
            unordered_chunks: 0,
            unordered_bytes: 0,
            out_of_order_chunks: 0,
            max_gap_bytes: 0,
            staging_cap_hits: 0,
        }
    }

    fn read_error(err: quinn::ReadError) -> io::Error {
        io::Error::other(format!("tuic unordered read: {err}"))
    }

    fn note_staging_cap_hit(&mut self, now: Instant) {
        self.staging_cap_hits = self.staging_cap_hits.saturating_add(1);
        self.maybe_log_staging("cap", now, self.rx.next_offset(), 0, 0);
    }

    fn note_chunk_staged(&mut self, now: Instant, offset: u64, chunk_len: usize) {
        let expected = self.rx.next_offset();
        let gap = offset.saturating_sub(expected);
        self.unordered_chunks = self.unordered_chunks.saturating_add(1);
        self.unordered_bytes = self.unordered_bytes.saturating_add(chunk_len as u64);
        if gap > 0 {
            self.out_of_order_chunks = self.out_of_order_chunks.saturating_add(1);
            self.max_gap_bytes = self.max_gap_bytes.max(gap);
        }
        self.maybe_log_staging("chunk", now, offset, chunk_len, gap);
    }

    fn maybe_log_staging(
        &mut self,
        reason: &'static str,
        now: Instant,
        chunk_offset: u64,
        chunk_len: usize,
        gap_bytes: u64,
    ) {
        let should_log = reason == "cap" || gap_bytes > 0;
        if !should_log {
            return;
        }
        let Some(meta) = self.diag_meta.as_ref() else {
            return;
        };
        let rate_limited = self
            .last_staging_log_at
            .map(|last| {
                now.saturating_duration_since(last).as_millis() < TUIC_TCP_UNORDERED_STAGING_LOG_MS
            })
            .unwrap_or(false);
        if rate_limited {
            return;
        }
        self.last_staging_log_at = Some(now);
        println!(
            "{}",
            format_tuic_tcp_unordered_staging_line(
                meta,
                reason,
                self.rx.next_offset(),
                chunk_offset,
                chunk_len,
                self.rx.buffered_bytes(),
                self.rx.buffered_chunks(),
                self.rx.max_buffered_bytes(),
                self.unordered_chunks,
                self.unordered_bytes,
                self.out_of_order_chunks,
                self.max_gap_bytes,
                gap_bytes,
                self.staging_cap_hits,
                TUIC_TCP_UNORDERED_REASSEMBLY_MAX_BYTES,
            )
        );
    }
}

impl AsyncRead for TuicChunkRelayStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        if self.rx.drain_into(buf) > 0 {
            return Poll::Ready(Ok(()));
        }
        if self.recv_eof {
            return Poll::Ready(Ok(()));
        }

        let mut polls = 0usize;
        loop {
            if self.rx.buffered_bytes() >= TUIC_TCP_UNORDERED_REASSEMBLY_MAX_BYTES {
                self.note_staging_cap_hit(Instant::now());
                return Poll::Pending;
            }
            let read_len = TUIC_TCP_UNORDERED_CHUNK_READ_MAX_BYTES
                .min(TUIC_TCP_UNORDERED_REASSEMBLY_MAX_BYTES - self.rx.buffered_bytes());
            let chunk = {
                let fut = self.recv.read_chunk(read_len, false);
                tokio::pin!(fut);
                match fut.poll(cx) {
                    Poll::Ready(Ok(chunk)) => chunk,
                    Poll::Ready(Err(err)) => return Poll::Ready(Err(Self::read_error(err))),
                    Poll::Pending => return Poll::Pending,
                }
            };
            match chunk {
                Some(chunk) => {
                    let now = Instant::now();
                    let offset = chunk.offset;
                    let chunk_len = chunk.bytes.len();
                    self.rx.push_chunk(offset, chunk.bytes);
                    self.note_chunk_staged(now, offset, chunk_len);
                    if self.rx.drain_into(buf) > 0 {
                        return Poll::Ready(Ok(()));
                    }
                }
                None => {
                    self.recv_eof = true;
                    return Poll::Ready(Ok(()));
                }
            }
            polls += 1;
            if polls >= 32 {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
        }
    }
}

impl AsyncWrite for TuicChunkRelayStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.send).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.send).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.send).poll_shutdown(cx)
    }
}

trait DirectOrderedNativeChunkRecv: Unpin + Send {
    fn poll_read_ordered_native_chunk(
        &mut self,
        cx: &mut Context<'_>,
        max_len: usize,
    ) -> Poll<io::Result<Option<NativeTcpChunk>>>;
}

struct QuinnDirectOrderedNativeChunkRecv {
    recv: quinn::RecvStream,
}

impl QuinnDirectOrderedNativeChunkRecv {
    fn new(recv: quinn::RecvStream) -> Self {
        Self { recv }
    }
}

impl DirectOrderedNativeChunkRecv for QuinnDirectOrderedNativeChunkRecv {
    fn poll_read_ordered_native_chunk(
        &mut self,
        cx: &mut Context<'_>,
        max_len: usize,
    ) -> Poll<io::Result<Option<NativeTcpChunk>>> {
        let poll = {
            // Quinn documents `read_chunk` as cancellation-safe. Polling one
            // short-lived future here keeps RecvStream ownership local and
            // introduces no payload task or message-count channel.
            let fut = self.recv.read_chunk(max_len, true);
            tokio::pin!(fut);
            fut.poll(cx)
        };
        match poll {
            Poll::Ready(Ok(Some(chunk))) => Poll::Ready(Ok(Some(NativeTcpChunk {
                offset: chunk.offset,
                bytes: chunk.bytes,
            }))),
            Poll::Ready(Ok(None)) => Poll::Ready(Ok(None)),
            Poll::Ready(Err(err)) => Poll::Ready(Err(io::Error::other(format!(
                "tuic direct ordered read: {err}"
            )))),
            Poll::Pending => Poll::Pending,
        }
    }
}

struct TuicNativeOrderedReader<R = QuinnDirectOrderedNativeChunkRecv> {
    recv: R,
    tcp_diag: Option<TuicTcpStreamDiag>,
    transport_conn: Option<Connection>,
    next_offset: u64,
    pending_self_wake_deadline: Option<Instant>,
    _read_pressure: Option<TcpOrderedReadPressureReader>,
    _lease: TcpPoolSlotLease,
}

impl TuicNativeOrderedReader<QuinnDirectOrderedNativeChunkRecv> {
    fn new(
        recv: quinn::RecvStream,
        lease: TcpPoolSlotLease,
        tcp_diag: Option<TuicTcpStreamDiag>,
        transport_conn: Connection,
        read_pressure: Option<TcpOrderedReadPressureReader>,
    ) -> Self {
        Self::from_recv(
            QuinnDirectOrderedNativeChunkRecv::new(recv),
            lease,
            tcp_diag,
            Some(transport_conn),
            read_pressure,
        )
    }
}

#[cfg(test)]
pub(crate) fn d16_native_ordered_reader_for_test(
    recv: quinn::RecvStream,
    transport_conn: Connection,
) -> NativeTcpReadHalf {
    let active = Arc::new(AtomicU64::new(0));
    let (lease, reserved) = TcpPoolSlotLease::reserve(active);
    assert!(reserved, "test reader must reserve one synthetic pool slot");
    Box::new(TuicNativeOrderedReader::new(
        recv,
        lease,
        None,
        transport_conn,
        None,
    ))
}

#[cfg(test)]
pub(crate) fn d16_quinn_test_endpoints() -> (Endpoint, Endpoint, SocketAddr) {
    let cert_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let mut server_cert_reader =
        std::io::BufReader::new(std::fs::File::open(cert_dir.join("server-cert.pem")).unwrap());
    let server_certs = rustls_pemfile::certs(&mut server_cert_reader)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let mut server_key_reader =
        std::io::BufReader::new(std::fs::File::open(cert_dir.join("server-key.pem")).unwrap());
    let server_key = rustls_pemfile::private_key(&mut server_key_reader)
        .unwrap()
        .unwrap();
    let server_config = quinn::ServerConfig::with_single_cert(server_certs, server_key).unwrap();
    let server_endpoint = quinn::Endpoint::server(
        server_config,
        "127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap(),
    )
    .unwrap();
    let server_addr = server_endpoint.local_addr().unwrap();

    let mut ca_reader =
        std::io::BufReader::new(std::fs::File::open(cert_dir.join("ca-cert.pem")).unwrap());
    let mut roots = rustls::RootCertStore::empty();
    for cert in rustls_pemfile::certs(&mut ca_reader) {
        roots.add(cert.unwrap()).unwrap();
    }
    let mut client_endpoint =
        quinn::Endpoint::client("127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap()).unwrap();
    client_endpoint.set_default_client_config(
        quinn::ClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );
    (server_endpoint, client_endpoint, server_addr)
}

impl<R> TuicNativeOrderedReader<R> {
    fn from_recv(
        recv: R,
        lease: TcpPoolSlotLease,
        tcp_diag: Option<TuicTcpStreamDiag>,
        transport_conn: Option<Connection>,
        read_pressure: Option<TcpOrderedReadPressureReader>,
    ) -> Self {
        Self {
            recv,
            tcp_diag,
            transport_conn,
            next_offset: 0,
            pending_self_wake_deadline: None,
            _read_pressure: read_pressure,
            _lease: lease,
        }
    }

    fn arm_pending_self_wake(&mut self, cx: &Context<'_>, now: Instant) {
        let deadline = now + Duration::from_millis(TUIC_TCP_STREAM_PENDING_SELF_WAKE_MS);
        self.pending_self_wake_deadline = Some(deadline);
        if let Some(diag) = self.tcp_diag.as_mut() {
            diag.note_self_wake_armed();
        }
        let waker = cx.waker().clone();
        tokio::spawn(async move {
            tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
            waker.wake();
        });
    }

    fn note_pending(&mut self, cx: &Context<'_>, now: Instant) {
        let transport = self
            .transport_conn
            .as_ref()
            .map(sample_tuic_stream_transport);
        let pending_cause =
            if let (Some(diag), Some(transport)) = (self.tcp_diag.as_ref(), transport) {
                classify_tuic_stream_pending_cause(Some(TuicStreamTransportPending {
                    sample: transport,
                    since_last_read: transport_delta(
                        diag.last_read_transport_sample.unwrap_or_default(),
                        transport,
                    ),
                    since_last_pending: transport_delta(
                        diag.last_pending_transport_sample.unwrap_or_default(),
                        transport,
                    ),
                }))
            } else {
                TuicTcpStreamPendingCause::NoTransportSample
            };
        if should_arm_tuic_stream_pending_self_wake(
            pending_cause,
            self.pending_self_wake_deadline,
            now,
        ) {
            self.arm_pending_self_wake(cx, now);
        }
        if let Some(diag) = self.tcp_diag.as_mut()
            && let Some(event) = diag.note_pending_at(now, transport)
        {
            let meta = diag.meta.clone();
            println!(
                "{}",
                format_tuic_tcp_stream_pending_line(
                    &meta,
                    event.pending_gap_ms,
                    event.pending_polls,
                    event.polls,
                    event.max_poll_gap_ms,
                    event.rx_bytes,
                    event.reads,
                    event.pending_cause,
                    event.transport,
                    event.self_wake_armed,
                    event.self_wake_fired,
                )
            );
        }
    }

    fn note_chunk_read(&mut self, bytes: usize, now: Instant) {
        self.pending_self_wake_deadline = None;
        let transport = self
            .transport_conn
            .as_ref()
            .map(sample_tuic_stream_transport);
        if let Some(diag) = self.tcp_diag.as_mut() {
            let event = diag.note_read_at(bytes, now, transport);
            let meta = diag.meta.clone();
            if let Some(first_rx_ms) = event.first_rx_ms {
                println!(
                    "{}",
                    format_tuic_tcp_stream_first_rx_line(
                        &meta,
                        first_rx_ms,
                        event.read_bytes,
                        event.reads,
                    )
                );
            }
            if let Some(gap_ms) = event.gap_ms
                && gap_ms >= TUIC_TCP_STREAM_READ_GAP_LOG_MS
            {
                println!(
                    "{}",
                    format_tuic_tcp_stream_read_gap_line(
                        &meta,
                        gap_ms,
                        event.read_bytes,
                        event.reads,
                        event.rx_bytes,
                    )
                );
            }
        }
    }
}

impl<R> NativeTcpReader for TuicNativeOrderedReader<R>
where
    R: DirectOrderedNativeChunkRecv,
{
    fn poll_read_chunk(
        &mut self,
        cx: &mut Context<'_>,
        max_len: usize,
    ) -> Poll<io::Result<Option<NativeTcpChunk>>> {
        let now = Instant::now();
        if matches!(self.pending_self_wake_deadline, Some(deadline) if deadline <= now) {
            self.pending_self_wake_deadline = None;
            if let Some(diag) = self.tcp_diag.as_mut() {
                diag.note_self_wake_fired();
            }
        }
        if let Some(diag) = self.tcp_diag.as_mut() {
            diag.note_poll_at(now);
        }

        let read_len = max_len.max(1).min(TUIC_TCP_DIRECT_ORDERED_READ_MAX_BYTES);
        let chunk = match self.recv.poll_read_ordered_native_chunk(cx, read_len) {
            Poll::Ready(Ok(chunk)) => chunk,
            Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
            Poll::Pending => {
                self.note_pending(cx, now);
                return Poll::Pending;
            }
        };
        let Some(chunk) = chunk else {
            return Poll::Ready(Ok(None));
        };
        if chunk.bytes.len() > read_len {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "tuic direct ordered chunk exceeds max_len: bytes={} max_len={read_len}",
                    chunk.bytes.len()
                ),
            )));
        }
        if chunk.offset != self.next_offset {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "tuic direct ordered offset discontinuity: expected={} actual={}",
                    self.next_offset, chunk.offset
                ),
            )));
        }

        self.next_offset = self.next_offset.saturating_add(chunk.bytes.len() as u64);
        if !chunk.bytes.is_empty() {
            self.note_chunk_read(chunk.bytes.len(), now);
        }
        Poll::Ready(Ok(Some(chunk)))
    }
}

impl<R> Drop for TuicNativeOrderedReader<R> {
    fn drop(&mut self) {
        if let Some(diag) = &self.tcp_diag {
            let snapshot = diag.close_snapshot();
            println!(
                "{}",
                format_tuic_tcp_stream_close_line(&diag.meta, &snapshot)
            );
        }
    }
}

#[async_trait::async_trait]
trait TuicOrderedNativeChunkSource: Send + 'static {
    async fn read_ordered_native_chunk(
        &mut self,
        max_len: usize,
    ) -> io::Result<Option<NativeTcpChunk>>;
}

struct QuinnOrderedNativeChunkSource {
    recv: quinn::RecvStream,
    next_offset: u64,
}

impl QuinnOrderedNativeChunkSource {
    fn new(recv: quinn::RecvStream) -> Self {
        Self {
            recv,
            next_offset: 0,
        }
    }
}

fn combine_ordered_native_chunks(
    next_offset: &mut u64,
    chunks: &[bytes::Bytes],
) -> Option<NativeTcpChunk> {
    let total_len = chunks.iter().map(bytes::Bytes::len).sum::<usize>();
    if total_len == 0 {
        return None;
    }
    let offset = *next_offset;
    *next_offset = next_offset.saturating_add(total_len as u64);
    if chunks.len() == 1 {
        return Some(NativeTcpChunk {
            offset,
            bytes: chunks[0].clone(),
        });
    }
    let mut combined = bytes::BytesMut::with_capacity(total_len);
    for chunk in chunks {
        combined.extend_from_slice(chunk);
    }
    Some(NativeTcpChunk {
        offset,
        bytes: combined.freeze(),
    })
}

#[async_trait::async_trait]
impl TuicOrderedNativeChunkSource for QuinnOrderedNativeChunkSource {
    async fn read_ordered_native_chunk(
        &mut self,
        _max_len: usize,
    ) -> io::Result<Option<NativeTcpChunk>> {
        let mut chunks = vec![bytes::Bytes::new(); TUIC_TCP_NATIVE_ORDERED_PUMP_READ_CHUNKS];
        match self.recv.read_chunks(&mut chunks).await {
            Ok(Some(n)) => Ok(combine_ordered_native_chunks(
                &mut self.next_offset,
                &chunks[..n],
            )),
            Ok(None) => Ok(None),
            Err(err) => Err(io::Error::other(format!("tuic native ordered read: {err}"))),
        }
    }
}

struct TuicNativeOrderedPumpReader {
    rx: mpsc::Receiver<io::Result<Option<NativeTcpChunk>>>,
    task: tokio::task::JoinHandle<()>,
    tcp_diag: Option<TuicTcpStreamDiag>,
    transport_conn: Option<Connection>,
    pending_self_wake_deadline: Option<Instant>,
    pending_chunk: Option<NativeTcpChunk>,
    pending_eof: bool,
    pending_error: Option<io::Error>,
    _lease: TcpPoolSlotLease,
}

impl TuicNativeOrderedPumpReader {
    fn spawn(
        recv: quinn::RecvStream,
        lease: TcpPoolSlotLease,
        tcp_diag: Option<TuicTcpStreamDiag>,
        transport_conn: Connection,
    ) -> Self {
        Self::spawn_from_source(
            QuinnOrderedNativeChunkSource::new(recv),
            lease,
            tcp_diag,
            Some(transport_conn),
        )
    }

    fn spawn_from_source<S>(
        mut source: S,
        lease: TcpPoolSlotLease,
        tcp_diag: Option<TuicTcpStreamDiag>,
        transport_conn: Option<Connection>,
    ) -> Self
    where
        S: TuicOrderedNativeChunkSource,
    {
        let (tx, rx) = mpsc::channel::<io::Result<Option<NativeTcpChunk>>>(
            TUIC_TCP_NATIVE_ORDERED_PUMP_CHANNEL_CHUNKS,
        );
        let task = tokio::spawn(async move {
            loop {
                let result = source
                    .read_ordered_native_chunk(TUIC_TCP_UNORDERED_CHUNK_READ_MAX_BYTES)
                    .await;
                let terminal = !matches!(result, Ok(Some(_)));
                if tx.send(result).await.is_err() || terminal {
                    break;
                }
            }
        });
        Self {
            rx,
            task,
            tcp_diag,
            transport_conn,
            pending_self_wake_deadline: None,
            pending_chunk: None,
            pending_eof: false,
            pending_error: None,
            _lease: lease,
        }
    }

    fn arm_pending_self_wake(&mut self, cx: &Context<'_>, now: Instant) {
        let deadline = now + Duration::from_millis(TUIC_TCP_STREAM_PENDING_SELF_WAKE_MS);
        self.pending_self_wake_deadline = Some(deadline);
        if let Some(diag) = self.tcp_diag.as_mut() {
            diag.note_self_wake_armed();
        }
        let waker = cx.waker().clone();
        tokio::spawn(async move {
            tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
            waker.wake();
        });
    }

    fn note_pending(&mut self, cx: &Context<'_>, now: Instant) {
        let transport = self
            .transport_conn
            .as_ref()
            .map(sample_tuic_stream_transport);
        let pending_cause =
            if let (Some(diag), Some(transport)) = (self.tcp_diag.as_ref(), transport) {
                classify_tuic_stream_pending_cause(Some(TuicStreamTransportPending {
                    sample: transport,
                    since_last_read: transport_delta(
                        diag.last_read_transport_sample.unwrap_or_default(),
                        transport,
                    ),
                    since_last_pending: transport_delta(
                        diag.last_pending_transport_sample.unwrap_or_default(),
                        transport,
                    ),
                }))
            } else {
                TuicTcpStreamPendingCause::NoTransportSample
            };
        if should_arm_tuic_stream_pending_self_wake(
            pending_cause,
            self.pending_self_wake_deadline,
            now,
        ) {
            self.arm_pending_self_wake(cx, now);
        }
        if let Some(diag) = self.tcp_diag.as_mut()
            && let Some(event) = diag.note_pending_at(now, transport)
        {
            let meta = diag.meta.clone();
            println!(
                "{}",
                format_tuic_tcp_stream_pending_line(
                    &meta,
                    event.pending_gap_ms,
                    event.pending_polls,
                    event.polls,
                    event.max_poll_gap_ms,
                    event.rx_bytes,
                    event.reads,
                    event.pending_cause,
                    event.transport,
                    event.self_wake_armed,
                    event.self_wake_fired,
                )
            );
        }
    }

    fn note_chunk_read(&mut self, bytes: usize, now: Instant) {
        self.pending_self_wake_deadline = None;
        let transport = self
            .transport_conn
            .as_ref()
            .map(sample_tuic_stream_transport);
        if let Some(diag) = self.tcp_diag.as_mut() {
            let event = diag.note_read_at(bytes, now, transport);
            let meta = diag.meta.clone();
            if let Some(first_rx_ms) = event.first_rx_ms {
                println!(
                    "{}",
                    format_tuic_tcp_stream_first_rx_line(
                        &meta,
                        first_rx_ms,
                        event.read_bytes,
                        event.reads,
                    )
                );
            }
            if let Some(gap_ms) = event.gap_ms
                && gap_ms >= TUIC_TCP_STREAM_READ_GAP_LOG_MS
            {
                println!(
                    "{}",
                    format_tuic_tcp_stream_read_gap_line(
                        &meta,
                        gap_ms,
                        event.read_bytes,
                        event.reads,
                        event.rx_bytes,
                    )
                );
            }
        }
    }
}

impl NativeTcpReader for TuicNativeOrderedPumpReader {
    fn poll_read_chunk(
        &mut self,
        cx: &mut Context<'_>,
        max_len: usize,
    ) -> Poll<io::Result<Option<NativeTcpChunk>>> {
        let now = Instant::now();
        if matches!(self.pending_self_wake_deadline, Some(deadline) if deadline <= now) {
            self.pending_self_wake_deadline = None;
            if let Some(diag) = self.tcp_diag.as_mut() {
                diag.note_self_wake_fired();
            }
        }
        if let Some(diag) = self.tcp_diag.as_mut() {
            diag.note_poll_at(now);
        }

        if let Some(err) = self.pending_error.take() {
            return Poll::Ready(Err(err));
        }
        if self.pending_eof {
            self.pending_eof = false;
            return Poll::Ready(Ok(None));
        }

        let max_len = max_len.max(1);
        let first = if let Some(chunk) = self.pending_chunk.take() {
            chunk
        } else {
            match self.rx.poll_recv(cx) {
                Poll::Ready(Some(Ok(Some(chunk)))) => chunk,
                Poll::Ready(Some(Ok(None))) | Poll::Ready(None) => {
                    return Poll::Ready(Ok(None));
                }
                Poll::Ready(Some(Err(err))) => return Poll::Ready(Err(err)),
                Poll::Pending => {
                    self.note_pending(cx, now);
                    return Poll::Pending;
                }
            }
        };

        let offset = first.offset;
        let mut next_offset = offset;
        let mut combined = bytes::BytesMut::with_capacity(max_len.min(first.bytes.len().max(1)));
        let mut chunk = first;

        loop {
            if !chunk.bytes.is_empty() && chunk.offset == next_offset {
                let remaining = max_len.saturating_sub(combined.len());
                let take = remaining.min(chunk.bytes.len());
                combined.extend_from_slice(&chunk.bytes[..take]);
                next_offset = next_offset.saturating_add(take as u64);
                if take < chunk.bytes.len() {
                    self.pending_chunk = Some(NativeTcpChunk {
                        offset: chunk.offset.saturating_add(take as u64),
                        bytes: chunk.bytes.slice(take..),
                    });
                    break;
                }
            } else if !chunk.bytes.is_empty() {
                self.pending_chunk = Some(chunk);
                break;
            }

            if combined.len() >= max_len {
                break;
            }

            match self.rx.poll_recv(cx) {
                Poll::Ready(Some(Ok(Some(next)))) => {
                    chunk = next;
                }
                Poll::Ready(Some(Ok(None))) | Poll::Ready(None) => {
                    self.pending_eof = true;
                    break;
                }
                Poll::Ready(Some(Err(err))) => {
                    self.pending_error = Some(err);
                    break;
                }
                Poll::Pending => break,
            }
        }

        if combined.is_empty() {
            self.note_pending(cx, now);
            return Poll::Pending;
        }
        let bytes = combined.freeze();
        self.note_chunk_read(bytes.len(), now);
        Poll::Ready(Ok(Some(NativeTcpChunk { offset, bytes })))
    }
}

impl Drop for TuicNativeOrderedPumpReader {
    fn drop(&mut self) {
        self.task.abort();
        if let Some(diag) = &self.tcp_diag {
            let snapshot = diag.close_snapshot();
            println!(
                "{}",
                format_tuic_tcp_stream_close_line(&diag.meta, &snapshot)
            );
        }
    }
}

struct TuicNativeTcpReader {
    recv: quinn::RecvStream,
    tcp_diag: Option<TuicTcpStreamDiag>,
    transport_conn: Connection,
    pending_self_wake_deadline: Option<Instant>,
    _lease: TcpPoolSlotLease,
}

impl TuicNativeTcpReader {
    fn new(
        recv: quinn::RecvStream,
        lease: TcpPoolSlotLease,
        tcp_diag: Option<TuicTcpStreamDiag>,
        transport_conn: Connection,
    ) -> Self {
        Self {
            recv,
            tcp_diag,
            transport_conn,
            pending_self_wake_deadline: None,
            _lease: lease,
        }
    }

    fn read_error(err: quinn::ReadError) -> io::Error {
        io::Error::other(format!("tuic native unordered read: {err}"))
    }

    fn arm_pending_self_wake(&mut self, cx: &Context<'_>, now: Instant) {
        let deadline = now + Duration::from_millis(TUIC_TCP_STREAM_PENDING_SELF_WAKE_MS);
        self.pending_self_wake_deadline = Some(deadline);
        if let Some(diag) = self.tcp_diag.as_mut() {
            diag.note_self_wake_armed();
        }
        let waker = cx.waker().clone();
        tokio::spawn(async move {
            tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
            waker.wake();
        });
    }

    fn note_pending(&mut self, cx: &Context<'_>, now: Instant) {
        let transport = sample_tuic_stream_transport(&self.transport_conn);
        let pending_cause = if let Some(diag) = self.tcp_diag.as_ref() {
            classify_tuic_stream_pending_cause(Some(TuicStreamTransportPending {
                sample: transport,
                since_last_read: transport_delta(
                    diag.last_read_transport_sample.unwrap_or_default(),
                    transport,
                ),
                since_last_pending: transport_delta(
                    diag.last_pending_transport_sample.unwrap_or_default(),
                    transport,
                ),
            }))
        } else {
            TuicTcpStreamPendingCause::NoTransportSample
        };
        if should_arm_tuic_stream_pending_self_wake(
            pending_cause,
            self.pending_self_wake_deadline,
            now,
        ) {
            self.arm_pending_self_wake(cx, now);
        }
        if let Some(diag) = self.tcp_diag.as_mut()
            && let Some(event) = diag.note_pending_at(now, Some(transport))
        {
            let meta = diag.meta.clone();
            println!(
                "{}",
                format_tuic_tcp_stream_pending_line(
                    &meta,
                    event.pending_gap_ms,
                    event.pending_polls,
                    event.polls,
                    event.max_poll_gap_ms,
                    event.rx_bytes,
                    event.reads,
                    event.pending_cause,
                    event.transport,
                    event.self_wake_armed,
                    event.self_wake_fired,
                )
            );
        }
    }

    fn note_chunk_read(&mut self, bytes: usize, now: Instant) {
        self.pending_self_wake_deadline = None;
        let transport = sample_tuic_stream_transport(&self.transport_conn);
        if let Some(diag) = self.tcp_diag.as_mut() {
            let event = diag.note_read_at(bytes, now, Some(transport));
            let meta = diag.meta.clone();
            if let Some(first_rx_ms) = event.first_rx_ms {
                println!(
                    "{}",
                    format_tuic_tcp_stream_first_rx_line(
                        &meta,
                        first_rx_ms,
                        event.read_bytes,
                        event.reads,
                    )
                );
            }
            if let Some(gap_ms) = event.gap_ms
                && gap_ms >= TUIC_TCP_STREAM_READ_GAP_LOG_MS
            {
                println!(
                    "{}",
                    format_tuic_tcp_stream_read_gap_line(
                        &meta,
                        gap_ms,
                        event.read_bytes,
                        event.reads,
                        event.rx_bytes,
                    )
                );
            }
        }
    }
}

impl NativeTcpReader for TuicNativeTcpReader {
    fn poll_read_chunk(
        &mut self,
        cx: &mut Context<'_>,
        max_len: usize,
    ) -> Poll<io::Result<Option<NativeTcpChunk>>> {
        let now = Instant::now();
        if matches!(self.pending_self_wake_deadline, Some(deadline) if deadline <= now) {
            self.pending_self_wake_deadline = None;
            if let Some(diag) = self.tcp_diag.as_mut() {
                diag.note_self_wake_fired();
            }
        }
        if let Some(diag) = self.tcp_diag.as_mut() {
            diag.note_poll_at(now);
        }
        let read_len = max_len.max(1).min(TUIC_TCP_UNORDERED_CHUNK_READ_MAX_BYTES);
        let poll = {
            let fut = self.recv.read_chunk(read_len, false);
            tokio::pin!(fut);
            fut.poll(cx)
        };
        let chunk = match poll {
            Poll::Ready(Ok(chunk)) => chunk,
            Poll::Ready(Err(err)) => return Poll::Ready(Err(Self::read_error(err))),
            Poll::Pending => {
                self.note_pending(cx, now);
                return Poll::Pending;
            }
        };
        match chunk {
            Some(chunk) => {
                let bytes_len = chunk.bytes.len();
                if bytes_len > 0 {
                    self.note_chunk_read(bytes_len, now);
                }
                Poll::Ready(Ok(Some(NativeTcpChunk {
                    offset: chunk.offset,
                    bytes: chunk.bytes,
                })))
            }
            None => Poll::Ready(Ok(None)),
        }
    }
}

impl Drop for TuicNativeTcpReader {
    fn drop(&mut self) {
        if let Some(diag) = &self.tcp_diag {
            let snapshot = diag.close_snapshot();
            println!(
                "{}",
                format_tuic_tcp_stream_close_line(&diag.meta, &snapshot)
            );
        }
    }
}

struct TuicNativeTcpWriter {
    send: TcpWritePressureAdapter<TuicTcpStartupWriter<quinn::SendStream>>,
    _lease: TcpPoolSlotLease,
}

impl TuicNativeTcpWriter {
    fn new(
        send: TuicTcpStartupWriter<quinn::SendStream>,
        lease: TcpPoolSlotLease,
        pressure: TcpWritePressureWriter,
        progress: quinn::SendStreamProgress,
    ) -> Self {
        Self {
            send: TcpWritePressureAdapter::new_with_progress(send, pressure, progress),
            _lease: lease,
        }
    }
}

impl AsyncWrite for TuicNativeTcpWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.send).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.send).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.send).poll_shutdown(cx)
    }
}

struct TrackedRelayStream<S> {
    inner: S,
    tcp_diag: Option<TuicTcpStreamDiag>,
    transport_conn: Option<Connection>,
    pending_self_wake_deadline: Option<Instant>,
    _lease: TcpPoolSlotLease,
}

impl<S> TrackedRelayStream<S> {
    #[cfg(test)]
    fn new(inner: S, lease: TcpPoolSlotLease, tcp_diag: Option<TuicTcpStreamDiag>) -> Self {
        Self {
            inner,
            tcp_diag,
            transport_conn: None,
            pending_self_wake_deadline: None,
            _lease: lease,
        }
    }

    fn new_with_transport(
        inner: S,
        lease: TcpPoolSlotLease,
        tcp_diag: Option<TuicTcpStreamDiag>,
        transport_conn: Connection,
    ) -> Self {
        Self {
            inner,
            tcp_diag,
            transport_conn: Some(transport_conn),
            pending_self_wake_deadline: None,
            _lease: lease,
        }
    }

    fn arm_pending_self_wake(&mut self, cx: &Context<'_>, now: Instant) {
        let deadline = now + Duration::from_millis(TUIC_TCP_STREAM_PENDING_SELF_WAKE_MS);
        self.pending_self_wake_deadline = Some(deadline);
        if let Some(diag) = self.tcp_diag.as_mut() {
            diag.note_self_wake_armed();
        }
        let waker = cx.waker().clone();
        tokio::spawn(async move {
            tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
            waker.wake();
        });
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for TrackedRelayStream<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before_len = buf.filled().len();
        let now = Instant::now();
        if matches!(self.pending_self_wake_deadline, Some(deadline) if deadline <= now) {
            self.pending_self_wake_deadline = None;
            if let Some(diag) = self.tcp_diag.as_mut() {
                diag.note_self_wake_fired();
            }
        }
        if let Some(diag) = self.tcp_diag.as_mut() {
            diag.note_poll_at(now);
        }
        let poll = Pin::new(&mut self.inner).poll_read(cx, buf);
        match &poll {
            Poll::Ready(Ok(())) => {
                let read_bytes = buf.filled().len().saturating_sub(before_len);
                if read_bytes > 0 {
                    self.pending_self_wake_deadline = None;
                }
                let transport = self
                    .transport_conn
                    .as_ref()
                    .map(sample_tuic_stream_transport);
                if read_bytes > 0
                    && let Some(diag) = self.tcp_diag.as_mut()
                {
                    let event = diag.note_read_at(read_bytes, now, transport);
                    let meta = diag.meta.clone();
                    if let Some(first_rx_ms) = event.first_rx_ms {
                        println!(
                            "{}",
                            format_tuic_tcp_stream_first_rx_line(
                                &meta,
                                first_rx_ms,
                                event.read_bytes,
                                event.reads
                            )
                        );
                    }
                    if let Some(gap_ms) = event.gap_ms
                        && gap_ms >= TUIC_TCP_STREAM_READ_GAP_LOG_MS
                    {
                        println!(
                            "{}",
                            format_tuic_tcp_stream_read_gap_line(
                                &meta,
                                gap_ms,
                                event.read_bytes,
                                event.reads,
                                event.rx_bytes
                            )
                        );
                    }
                }
            }
            Poll::Pending => {
                let transport = self
                    .transport_conn
                    .as_ref()
                    .map(sample_tuic_stream_transport);
                let pending_cause =
                    if let (Some(diag), Some(sample)) = (self.tcp_diag.as_ref(), transport) {
                        classify_tuic_stream_pending_cause(Some(TuicStreamTransportPending {
                            sample,
                            since_last_read: transport_delta(
                                diag.last_read_transport_sample.unwrap_or_default(),
                                sample,
                            ),
                            since_last_pending: transport_delta(
                                diag.last_pending_transport_sample.unwrap_or_default(),
                                sample,
                            ),
                        }))
                    } else {
                        TuicTcpStreamPendingCause::NoTransportSample
                    };
                if should_arm_tuic_stream_pending_self_wake(
                    pending_cause,
                    self.pending_self_wake_deadline,
                    now,
                ) {
                    self.arm_pending_self_wake(cx, now);
                }
                if let Some(diag) = self.tcp_diag.as_mut()
                    && let Some(event) = diag.note_pending_at(now, transport)
                {
                    let meta = diag.meta.clone();
                    println!(
                        "{}",
                        format_tuic_tcp_stream_pending_line(
                            &meta,
                            event.pending_gap_ms,
                            event.pending_polls,
                            event.polls,
                            event.max_poll_gap_ms,
                            event.rx_bytes,
                            event.reads,
                            event.pending_cause,
                            event.transport,
                            event.self_wake_armed,
                            event.self_wake_fired,
                        )
                    );
                }
            }
            Poll::Ready(Err(_)) => {}
        }
        poll
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for TrackedRelayStream<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

impl<S> Drop for TrackedRelayStream<S> {
    fn drop(&mut self) {
        if let Some(diag) = &self.tcp_diag {
            let snapshot = diag.close_snapshot();
            println!(
                "{}",
                format_tuic_tcp_stream_close_line(&diag.meta, &snapshot)
            );
        }
    }
}

/// 解析 `MINI_VPN_TUIC_ZERO_RTT`：**默认关**；显式 `true`/`1`/`on`/`yes`（大小写无关）开。
/// 中文要点：quinn 0.10 / rustls 0.21 在 0-RTT(握手未完成)阶段**不支持 `export_keying_material`**，
/// 而 TUIC token 依赖它 → 0-RTT 认证必失败、自愈回落 1-RTT（实测 2026-06-11，见 13c 验收）。默认开纯属
/// 每次重连白跑一次握手，故默认关；保留开关供未来 quinn 支持 0-RTT keying-material 后启用。
fn parse_zero_rtt(s: Option<&str>) -> bool {
    parse_truthy(s)
}

/// TUIC 协议版本字节。
const TUIC_VER: u8 = 0x05;
/// 命令类型。
const CMD_AUTHENTICATE: u8 = 0x00;
const CMD_CONNECT: u8 = 0x01;
const CMD_PACKET: u8 = 0x02;
const CMD_HEARTBEAT: u8 = 0x04;
/// 地址 None 类型(回程可能省略地址)。
const ATYP_NONE: u8 = 0xff;

/// TUIC Heartbeat 周期：连接空闲时维持 NAT 映射/路径活性。取 3s，给 sing-box 的空闲超时留足余量。
const TUIC_HEARTBEAT_SECS: u64 = 3;
/// TUIC Heartbeat 活跃窗口：距上次 UDP 上行多久(秒)内仍算「UDP 活跃」、需要发心跳。
/// 中文要点：Heartbeat 是**应用层 UDP 会话保活**(让 sing-box 不回收关联)；纯 TCP 会话由 QUIC keep-alive
/// 保活、不需要它。取 60s 与 UDP flow 空闲回收(`UDP_FLOW_IDLE_SECS`)对齐：UDP 静默到该回收时，心跳也停。
const TUIC_HB_IDLE_WINDOW_SECS: u64 = 60;
/// 下行 datagram channel 容量。背压由 pump 承担（`send().await`），不丢下行（DNS 响应不能丢）。
const TUIC_DOWNLINK_CAPACITY: usize = 1024;
/// 一个 TUIC Packet 命令的字节上限（读 uni-stream 的 `read_to_end` size_limit，防恶意/异常无界流）。
/// UDP 载荷最大 65507 + TUIC 头/地址 ≈ 65537 → 取 66KiB 兜头。
const MAX_TUIC_PACKET_BYTES: usize = 66 * 1024;
/// 下行 uni-stream 并发读取上限：超额直接丢弃该 stream（reset），防 flood 下无界派生任务。
/// quic-relay-mode 下每包一条 uni-stream；256 并发够吸收突发，又不至失控。
const MAX_CONCURRENT_DOWNLINK_STREAMS: usize = 256;
/// UDP 上行统计行打印周期（秒）：周期性把 stream 兜底/丢弃计数打一行，供真出口 acceptance 与生产观测。
const UDP_STATS_LOG_SECS: u64 = 30;
/// UDP 驱动重连退避上限（确定性指数退避，无需 rand；UDP 自愈，重连节奏不敏感）。
const UDP_RECONNECT_CAP_MS: u64 = 30_000;
const UDP_RECONNECT_BASE_MS: u64 = 500;
/// 地址类型(注意：TUIC 的 ATYP 取值与我们 Stage-12 自定义的不同)。
const ATYP_DOMAIN: u8 = 0x00;
const ATYP_IPV4: u8 = 0x01;
const ATYP_IPV6: u8 = 0x02;

/// 编码 TUIC 地址：`[ATYP][ADDR][PORT:u16 BE]`。
/// 中文要点：域名 `[len:u8][bytes]`，IPv4 4B，IPv6 16B；域名超 255 字节按 255 截断(不 panic)。
pub fn encode_address(target: &TargetAddr) -> Vec<u8> {
    let mut v = Vec::new();
    match target {
        TargetAddr::IpPort(SocketAddr::V4(a)) => {
            v.push(ATYP_IPV4);
            v.extend_from_slice(&a.ip().octets());
            v.extend_from_slice(&a.port().to_be_bytes());
        }
        TargetAddr::IpPort(SocketAddr::V6(a)) => {
            v.push(ATYP_IPV6);
            v.extend_from_slice(&a.ip().octets());
            v.extend_from_slice(&a.port().to_be_bytes());
        }
        TargetAddr::DomainPort { host, port } => {
            let bytes = host.as_bytes();
            let len = bytes.len().min(u8::MAX as usize);
            v.push(ATYP_DOMAIN);
            v.push(len as u8);
            v.extend_from_slice(&bytes[..len]);
            v.extend_from_slice(&port.to_be_bytes());
        }
    }
    v
}

/// 编码 Authenticate 命令(走单向流)：`[0x05][0x00][UUID:16][TOKEN:32]`。
pub fn encode_authenticate(uuid: &[u8; 16], token: &[u8; 32]) -> Vec<u8> {
    let mut v = Vec::with_capacity(2 + 16 + 32);
    v.push(TUIC_VER);
    v.push(CMD_AUTHENTICATE);
    v.extend_from_slice(uuid);
    v.extend_from_slice(token);
    v
}

/// 编码 Connect 命令(走双向流，随后直接搬字节)：`[0x05][0x01][ADDR]`。
pub fn encode_connect(target: &TargetAddr) -> Vec<u8> {
    let mut v = Vec::with_capacity(2 + 19);
    v.push(TUIC_VER);
    v.push(CMD_CONNECT);
    v.extend_from_slice(&encode_address(target));
    v
}

/// 编码 TUIC `Packet`(native datagram)：
/// `[0x05][0x02][ASSOC:u16][PKT_ID:u16=0][FRAG_TOTAL=1][FRAG_ID=0][SIZE:u16][ADDR][data]`。
pub fn encode_packet(assoc_id: u16, target: &TargetAddr, data: &[u8]) -> Vec<u8> {
    let addr = encode_address(target);
    let mut v = Vec::with_capacity(10 + addr.len() + data.len());
    v.push(TUIC_VER);
    v.push(CMD_PACKET);
    v.extend_from_slice(&assoc_id.to_be_bytes());
    v.extend_from_slice(&0u16.to_be_bytes()); // PKT_ID(native 不重组,固定 0)
    v.push(1); // FRAG_TOTAL
    v.push(0); // FRAG_ID
    v.extend_from_slice(&(data.len() as u16).to_be_bytes()); // SIZE
    v.extend_from_slice(&addr);
    v.extend_from_slice(data);
    v
}

/// 下行 Packet 的 frag 感知元信息（native 分片重组用）。
/// 中文要点：`data` 是**本（分片）chunk**（SIZE 字节）；整包由同 `(assoc_id,pkt_id)` 的
/// 各 `frag_id` 按序拼接而成（ADDR 仅在 `frag_id==0`，后续分片为 ATYP_NONE，由 `address_len` 跳过）。
#[derive(Debug, PartialEq, Eq)]
pub struct PacketMeta<'a> {
    pub assoc_id: u16,
    pub pkt_id: u16,
    pub frag_total: u8,
    pub frag_id: u8,
    pub data: &'a [u8],
}

/// 解码下行 `Packet` 的完整元信息（含 FRAG 字段，供重组）。越界/地址类型未知返回 None（不 panic）。
pub fn decode_packet_meta(buf: &[u8]) -> Option<PacketMeta<'_>> {
    // 固定前缀 10 字节:ver type assoc(2) pkt(2) ftot fid size(2)。
    if buf.len() < 10 {
        return None;
    }
    let size = u16::from_be_bytes([buf[8], buf[9]]) as usize;
    let addr_len = address_len(buf, 10)?;
    let data_start = 10 + addr_len;
    let data_end = data_start.checked_add(size)?;
    if buf.len() < data_end {
        return None;
    }
    Some(PacketMeta {
        assoc_id: u16::from_be_bytes([buf[2], buf[3]]),
        pkt_id: u16::from_be_bytes([buf[4], buf[5]]),
        frag_total: buf[6],
        frag_id: buf[7],
        data: &buf[data_start..data_end],
    })
}

/// 解码下行 `Packet`,只取 `(assoc_id, data)`(跳过 ADDR/FRAG)。越界/地址类型未知返回 None。
/// 中文要点：`decode_packet_meta` 的薄包装，服务 `FRAG_TOTAL==1` 快路径（零回归）。
pub fn decode_packet(buf: &[u8]) -> Option<(u16, &[u8])> {
    decode_packet_meta(buf).map(|m| (m.assoc_id, m.data))
}

/// 下行 native 分片重组的并发未完成包上限（到顶 LRU 驱逐最老）。
pub const FRAG_REASSEMBLY_CAP: usize = 256;
/// 未集齐的分片包存活上限（秒）：超时即弃（一片丢 → 整包弃，保直播 liveness，不无限等）。
pub const FRAG_REASSEMBLY_TTL_SECS: u64 = 10;

/// 一个未完成 UDP 包的分片缓冲（按 `(assoc_id, pkt_id)` 索引）。
/// 中文要点：`frags.len()` 即 frag_total（构造后不变），不再单独存字段（单一事实源）。
struct FragPartial {
    frags: Vec<Option<Vec<u8>>>, // 下标 = frag_id；len == frag_total
    received: usize,
    first_seen: u64,
}

/// native 下行分片重组器（**纯状态机**，主循环独占、无锁，与 `AssocTable` 同寿）。
/// 中文要点：server native 模式把大下行包拆成多个 `FRAG_TOTAL>1` 的 Packet 命令，
/// 本器按 `(assoc_id, pkt_id)` 收集、集齐按 `frag_id` 序拼接还原整包。`FRAG_TOTAL==1` 直通快路径。
pub struct FragReassembler {
    partials: HashMap<(u16, u16), FragPartial>,
    cap: usize,
}

impl Default for FragReassembler {
    fn default() -> Self {
        Self::new()
    }
}

impl FragReassembler {
    pub fn new() -> Self {
        Self::with_cap(FRAG_REASSEMBLY_CAP)
    }

    pub fn with_cap(cap: usize) -> Self {
        Self {
            partials: HashMap::new(),
            cap: cap.max(1),
        }
    }

    /// 喂入一个（分片）Packet。集齐返回完整 payload；未齐/无效返回 None。
    /// 中文要点：`FRAG_TOTAL==1` 直通不入表；`frag_id>=frag_total` 或 `frag_total==0` 视为无效丢弃；
    /// 重复 frag_id **last-writer-wins**（保留较新；见下「跨重连」）；新 key 到 cap 触发 LRU（按 first_seen）驱逐最老。
    /// 跨重连：连接断/重连后 server 可能复用同 `(assoc_id, pkt_id)`。frag_total 变 → 整体重建；frag_total 同
    /// → last-writer-wins 让新连接的 frag 覆盖残片（减少串味），未被新包重发的残留 slot 仍可能混入，由
    /// `FRAG_REASSEMBLY_TTL_SECS` sweep 兜底（窄窗、可容忍：分片仅大包尾部，重组失败应用自愈）。
    pub fn accept(&mut self, m: &PacketMeta, now: u64) -> Option<Vec<u8>> {
        if m.frag_total == 0 || m.frag_id >= m.frag_total {
            return None; // 无效分片头，丢弃（防越界/除零）
        }
        if m.frag_total == 1 {
            return Some(m.data.to_vec()); // 快路径：单帧直通，不入表
        }
        let key = (m.assoc_id, m.pkt_id);
        let frag_total = m.frag_total as usize;
        // pkt_id 复用但 frag_total 变化 → 旧残留作废，按新分片重建。
        if self
            .partials
            .get(&key)
            .is_some_and(|p| p.frags.len() != frag_total)
        {
            self.partials.remove(&key);
        }
        // 新 key 到 cap → 先驱逐最老未完成（在借用 entry 前做，避免交叉借用）。
        if !self.partials.contains_key(&key) {
            self.evict_if_full();
        }
        let p = self.partials.entry(key).or_insert_with(|| FragPartial {
            frags: (0..frag_total).map(|_| None).collect(),
            received: 0,
            first_seen: now,
        });
        let slot = &mut p.frags[m.frag_id as usize];
        if slot.is_none() {
            p.received += 1;
        }
        *slot = Some(m.data.to_vec()); // last-writer-wins（覆盖；跨重连残片被新 frag 顶替）
        if p.received < p.frags.len() {
            return None;
        }
        // 集齐：按 frag_id 序拼接，清出该项。
        let done = self.partials.remove(&key).expect("present");
        let mut whole = Vec::with_capacity(done.frags.iter().flatten().map(Vec::len).sum());
        for frag in done.frags.into_iter().flatten() {
            whole.extend_from_slice(&frag);
        }
        Some(whole)
    }

    /// 回收未集齐且超 `ttl` 秒的分片包（丢片自愈，防内存泄漏）。
    pub fn sweep(&mut self, now: u64, ttl: u64) {
        self.partials
            .retain(|_, p| now.saturating_sub(p.first_seen) <= ttl);
    }

    /// 新 key 到 cap 前驱逐最老（first_seen 最小）未完成包。
    fn evict_if_full(&mut self) {
        if self.partials.len() < self.cap {
            return;
        }
        if let Some(victim) = self
            .partials
            .iter()
            .min_by_key(|(k, p)| (p.first_seen, **k))
            .map(|(k, _)| *k)
        {
            self.partials.remove(&victim);
        }
    }

    #[cfg(test)]
    fn pending_len(&self) -> usize {
        self.partials.len()
    }
}

/// 编码 Heartbeat：`[0x05][0x04]`。
pub fn encode_heartbeat() -> Vec<u8> {
    vec![TUIC_VER, CMD_HEARTBEAT]
}

/// ADDR 段的字节长度(用于解码时跳过地址)。
fn address_len(buf: &[u8], pos: usize) -> Option<usize> {
    match *buf.get(pos)? {
        ATYP_IPV4 => Some(1 + 4 + 2),
        ATYP_IPV6 => Some(1 + 16 + 2),
        ATYP_DOMAIN => {
            let l = *buf.get(pos + 1)? as usize;
            Some(1 + 1 + l + 2)
        }
        ATYP_NONE => Some(1),
        _ => None,
    }
}

/// TUIC UDP 关联表:每条 UDP flow(4 元组)分配一个 **u16 assoc-id**(≈ Stage 12 flow-id),
/// 双向 demux 用。主循环独占、无锁;`now` 注入便于单测。结构同 udp_relay::FlowTable,只是 id 宽 16 位。
/// 中文要点:复用 FourTuple/FlowEntry,不动 Stage 12 的 FlowTable(系统稳定优先,接受少量重复)。
#[derive(Debug)]
pub struct AssocTable {
    tuple_to_id: HashMap<FourTuple, u16>,
    id_to_entry: HashMap<u16, FlowEntry>,
    /// 刀2：assoc-id → 本 UDP flow 占用的 fake-IP（若 target 经 fake-IP 改写）。
    /// 中文要点：用于回收（evict/sweep）该 assoc 时知道要 `release` 哪个 fake-IP（引用计数）。
    id_to_fake_ip: HashMap<u16, Ipv4Addr>,
    /// 刀2：本轮被回收（evict/sweep）且有 fake-IP 的 assoc 的 fake-IP，待主循环 drain 后 `release`。
    reclaimed_fake_ips: Vec<Ipv4Addr>,
    next_id: u16,
    cap: usize,
}

impl Default for AssocTable {
    fn default() -> Self {
        Self::new()
    }
}

impl AssocTable {
    pub fn new() -> Self {
        Self::with_cap(MAX_UDP_FLOWS)
    }

    pub fn with_cap(cap: usize) -> Self {
        Self {
            tuple_to_id: HashMap::new(),
            id_to_entry: HashMap::new(),
            id_to_fake_ip: HashMap::new(),
            reclaimed_fake_ips: Vec::new(),
            next_id: 1,
            cap: cap.max(1).min(u16::MAX as usize),
        }
    }

    /// 刀2：登记该 assoc 占用的 fake-IP（UDP 新 flow 时调，调用方随后 `fake_pool.acquire`）。
    pub fn set_fake_ip(&mut self, assoc_id: u16, ip: Ipv4Addr) {
        self.id_to_fake_ip.insert(assoc_id, ip);
    }

    /// 刀2：取走本轮被回收（evict/sweep）的 fake-IP 列表，主循环对每个 `fake_pool.release`。
    /// 中文要点：assoc 回收与 fake-IP release 解耦——AssocTable 不持有 FakeIpPool，
    /// 只累积「该 release 谁」，由独占两者的主循环执行，避免交叉借用/循环依赖。
    pub fn take_reclaimed_fake_ips(&mut self) -> Vec<Ipv4Addr> {
        std::mem::take(&mut self.reclaimed_fake_ips)
    }

    /// assoc 被回收时，若它占用了 fake-IP，移出映射并记入待 release 队列。
    fn note_reclaimed(&mut self, assoc_id: u16) {
        if let Some(ip) = self.id_to_fake_ip.remove(&assoc_id) {
            self.reclaimed_fake_ips.push(ip);
        }
    }

    /// 查/铸 assoc-id（已在册稳定返回;否则铸新,到顶 LRU 驱逐）。
    pub fn intern(&mut self, tuple: FourTuple) -> u16 {
        if let Some(&id) = self.tuple_to_id.get(&tuple) {
            return id;
        }
        if self.id_to_entry.len() >= self.cap {
            self.evict_lru();
        }
        let id = self.alloc_id();
        self.id_to_entry.insert(
            id,
            FlowEntry {
                tuple,
                last_activity: 0,
            },
        );
        self.tuple_to_id.insert(tuple, id);
        id
    }

    /// 分配一个**空闲且非 0** 的 assoc-id（单调推进，跳过 0 与仍在册的 id）。
    /// 中文要点：u16 空间仅 65535,回绕后 `next_id` 可能落到仍在册的 flow 上——不跳过就会
    /// `insert` 覆盖活跃 flow(回程串到错误端点)并泄漏其 tuple_to_id 映射。活跃集(≤cap≤1024)
    /// 远小于 id 空间,故必有空闲 id,扫描代价极小。调用前已 evict 保证 len<cap,循环必然终止。
    fn alloc_id(&mut self) -> u16 {
        loop {
            let id = self.next_id;
            self.next_id = self.next_id.wrapping_add(1);
            if self.next_id == 0 {
                self.next_id = 1; // 跳过 0,保持非零、单调
            }
            if !self.id_to_entry.contains_key(&id) {
                return id;
            }
        }
    }

    pub fn resolve(&self, assoc_id: u16) -> Option<&FlowEntry> {
        self.id_to_entry.get(&assoc_id)
    }

    /// 该 4 元组是否已在册（用于"每流一次"日志，避免热路径刷屏）。
    pub fn contains(&self, tuple: &FourTuple) -> bool {
        self.tuple_to_id.contains_key(tuple)
    }

    pub fn touch(&mut self, assoc_id: u16, now: u64) {
        if let Some(e) = self.id_to_entry.get_mut(&assoc_id) {
            e.last_activity = now;
        }
    }

    /// 回收 idle 超 `idle_secs` 的 assoc，**直接返回**这些 assoc 占用的 fake-IP（供调用方 release）。
    /// 中文要点：review #8——sweep 自包含返回，不再依赖 stash-and-drain（调用方无需记得随后 take）；
    /// `take_reclaimed_fake_ips` 仅服务 intern 内部的 LRU 驱逐（那里无法返回值）。
    pub fn sweep(&mut self, now: u64, idle_secs: u64) -> Vec<Ipv4Addr> {
        let expired: Vec<(u16, FourTuple)> = self
            .id_to_entry
            .iter()
            .filter(|(_, e)| now.saturating_sub(e.last_activity) > idle_secs)
            .map(|(id, e)| (*id, e.tuple))
            .collect();
        let mut reclaimed = Vec::new();
        for (id, tuple) in expired {
            self.id_to_entry.remove(&id);
            self.tuple_to_id.remove(&tuple);
            if let Some(ip) = self.id_to_fake_ip.remove(&id) {
                reclaimed.push(ip); // 刀2：该 assoc 占用的 fake-IP → 调用方 release
            }
        }
        reclaimed
    }

    fn evict_lru(&mut self) {
        let victim = self
            .id_to_entry
            .iter()
            .min_by_key(|(id, e)| (e.last_activity, **id))
            .map(|(id, _)| *id);
        if let Some(id) = victim
            && let Some(e) = self.id_to_entry.remove(&id)
        {
            self.tuple_to_id.remove(&e.tuple);
            self.note_reclaimed(id); // 刀2：LRU 驱逐时同样回收 fake-IP
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.id_to_entry.len()
    }
}

/// 上行 UDP 的传输选择：datagram 快路径 vs uni-stream 兜底。
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum UdpSend {
    Datagram,
    Stream,
}

/// TUIC UDP relay mode（per-association；本项目按 config 全局选一种）。
/// 中文要点（已查证 TUIC SPEC + 刀3.5 spec）：`Native` = QUIC datagram（低延迟、但高 RTT/丢包路径有
/// ~5.3M 硬天花板、溢出静默丢）；`Quic` = 每个 `Packet` 一条 uni-stream（可靠、摆脱天花板，代价是每包建流）。
/// **server 按某 assoc 首包 mode 镜像下行**——故 `Quic` 模式首包即走 stream → 下行也镜像 stream（高码率必需）。
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum UdpRelayMode {
    Native,
    Quic,
}

impl UdpRelayMode {
    /// 解析配置字符串（大小写不敏感）。非法返回 None（调用方决定回落/报错）。
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "native" => Some(Self::Native),
            "quic" => Some(Self::Quic),
            _ => None,
        }
    }
}

/// 纯决策：给定 relay mode、当前 datagram 发送上限（`conn.max_datagram_size()`）与待发字节数，选传输。
/// 中文要点：
/// - `Quic` → **恒 Stream**（首包起即 uni-stream，触发 server 下行镜像 stream，摆脱 datagram 天花板）。
/// - `Native` → size-based：装得下走 datagram（含边界 `len==max`）；超上限或 datagram 不可用（`None`）→ stream 兜底
///   （刀3 现行语义，零回归）。主动分流（先查 max_datagram_size），避免 `send_datagram` 返回 `TooLarge` 的往返。
pub fn udp_send_plan(mode: UdpRelayMode, max_datagram: Option<usize>, len: usize) -> UdpSend {
    match mode {
        UdpRelayMode::Quic => UdpSend::Stream,
        UdpRelayMode::Native => match max_datagram {
            Some(max) if len <= max => UdpSend::Datagram,
            _ => UdpSend::Stream,
        },
    }
}

/// datagram 背压压力信号（刀3.5 可观测）：出向 datagram 缓冲剩余 `< 1 个 MTU` 时，
/// 再发大 datagram 极可能触发 quinn「丢最老」——不报错的静默丢（刀3 acceptance 盲点）。
/// 中文要点：这是**代理信号**（pressure），非逐包精确丢包数；逐包精确留待真做主动背压时一起上。
pub fn is_datagram_pressured(send_buffer_space: usize, mtu: usize) -> bool {
    send_buffer_space < mtu
}

/// 格式化 30s UDP 上行统计行（**纯函数**，便于单测；I/O 取数在 `start_udp`）。
/// 中文要点：在刀3 的「stream 兜底/丢弃」基础上加 quinn 级量化——`RTT/cwnd/丢包/send_buf 余`，
/// 供真出口 acceptance 归因 datagram 天花板成因（CC vs 无背压）。
#[allow(clippy::too_many_arguments)]
pub fn format_udp_stats(
    max_datagram: Option<usize>,
    fallbacks: u64,
    drops: u64,
    rtt_ms: u128,
    cwnd: u64,
    lost: u64,
    sent: u64,
    send_buffer_space: usize,
) -> String {
    format!(
        "📊 TUIC UDP↑: datagram 上限={max_datagram:?} stream 兜底={fallbacks} 丢弃={drops} \
         | RTT={rtt_ms}ms cwnd={cwnd} 丢包={lost}/{sent} send_buf 余={send_buffer_space}B"
    )
}

#[derive(Debug, Default, Clone, Copy)]
struct QuicStatsSnapshot {
    rtt_ms: u128,
    cwnd: u64,
    lost_packets: u64,
    sent_packets: u64,
    lost_bytes: u64,
    congestion_events: u64,
    sent_plpmtud_probes: u64,
    lost_plpmtud_probes: u64,
    black_holes_detected: u64,
    tx_ack_frames: u64,
    rx_ack_frames: u64,
    rx_stream_frames: u64,
    tx_data_blocked: u64,
    tx_stream_data_blocked: u64,
    tx_streams_blocked_bidi: u64,
    tx_streams_blocked_uni: u64,
    tx_max_data: u64,
    tx_max_stream_data: u64,
    rx_data_blocked: u64,
    rx_stream_data_blocked: u64,
    rx_max_data: u64,
    rx_max_stream_data: u64,
    udp_tx_datagrams: u64,
    udp_tx_bytes: u64,
    udp_rx_datagrams: u64,
    udp_rx_bytes: u64,
    datagram_max: Option<usize>,
    datagram_send_buffer_space: usize,
    pacing_uncapped_capacity_bytes: u64,
    pacing_capacity_bytes: u64,
    pacing_tokens_bytes: u64,
    current_mtu: u16,
    pacing_mtu: u16,
    pacing_cap_active: bool,
    pacing_delay_events: u64,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct TuicStreamTransportSample {
    udp_rx_datagrams: u64,
    udp_rx_bytes: u64,
    rx_stream_frames: u64,
    rx_ack_frames: u64,
    tx_ack_frames: u64,
    sent_plpmtud_probes: u64,
    lost_plpmtud_probes: u64,
    black_holes_detected: u64,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct TuicStreamTransportDelta {
    udp_rx_datagrams: u64,
    udp_rx_bytes: u64,
    rx_stream_frames: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TuicStreamTransportPending {
    sample: TuicStreamTransportSample,
    since_last_read: TuicStreamTransportDelta,
    since_last_pending: TuicStreamTransportDelta,
}

type TuicTcpStreamPendingCause = StreamPendingFreshness;

fn classify_tuic_stream_pending_cause(
    transport: Option<TuicStreamTransportPending>,
) -> TuicTcpStreamPendingCause {
    let Some(transport) = transport else {
        return TuicTcpStreamPendingCause::NoTransportSample;
    };
    if transport.since_last_read.rx_stream_frames > 0 {
        return if transport.since_last_pending.rx_stream_frames > 0 {
            TuicTcpStreamPendingCause::ConnectionFreshStreamFramesPending
        } else {
            TuicTcpStreamPendingCause::ConnectionStaleStreamFramesPending
        };
    }
    if transport.since_last_read.udp_rx_datagrams > 0 || transport.since_last_read.udp_rx_bytes > 0
    {
        return TuicTcpStreamPendingCause::ConnectionRxNoStreamFrames;
    }
    TuicTcpStreamPendingCause::NoConnectionRx
}

fn should_arm_tuic_stream_pending_self_wake(
    cause: TuicTcpStreamPendingCause,
    armed_deadline: Option<Instant>,
    now: Instant,
) -> bool {
    let active_connection_rx = matches!(
        cause,
        TuicTcpStreamPendingCause::ConnectionRxNoStreamFrames
            | TuicTcpStreamPendingCause::ConnectionFreshStreamFramesPending
            | TuicTcpStreamPendingCause::ConnectionStaleStreamFramesPending
    );
    if !active_connection_rx {
        return false;
    }
    match armed_deadline {
        Some(deadline) => deadline <= now,
        None => true,
    }
}

/// 格式化 QUIC 连接级诊断（刀14y）。
/// 中文要点：`tx_blocked` 是本端发送 DATA_BLOCKED/STREAM_DATA_BLOCKED，直接指向对端 flow-control；
/// `cwnd/lost/congestion_events` 指向拥塞/丢包。两组指标一起看，下一轮 acceptance 不再盲猜。
fn format_quic_stats_line(conn_index: usize, stable_id: usize, stats: QuicStatsSnapshot) -> String {
    format!(
        "📊 TUIC QUIC stats conn={conn_index} id={stable_id} \
         rtt={}ms cwnd={} lost={}/{} lost_bytes={} congestion_events={} \
         plpmtud(sent={},lost={},black_holes={}) frames(rx_stream={},rx_ack={},tx_ack={}) \
         tx_blocked(data={},stream={},streams_bidi={},streams_uni={}) \
         rx_blocked(data={},stream={}) tx_window(max_data={},max_stream_data={}) \
         rx_window(max_data={},max_stream_data={}) udp_tx={}/{}B udp_rx={}/{}B \
         dg_max={:?} dg_space={}B \
         pacing(uncapped={},capacity={},tokens={},current_mtu={},pacing_mtu={},cap_active={},delay_events={})",
        stats.rtt_ms,
        stats.cwnd,
        stats.lost_packets,
        stats.sent_packets,
        stats.lost_bytes,
        stats.congestion_events,
        stats.sent_plpmtud_probes,
        stats.lost_plpmtud_probes,
        stats.black_holes_detected,
        stats.rx_stream_frames,
        stats.rx_ack_frames,
        stats.tx_ack_frames,
        stats.tx_data_blocked,
        stats.tx_stream_data_blocked,
        stats.tx_streams_blocked_bidi,
        stats.tx_streams_blocked_uni,
        stats.rx_data_blocked,
        stats.rx_stream_data_blocked,
        stats.tx_max_data,
        stats.tx_max_stream_data,
        stats.rx_max_data,
        stats.rx_max_stream_data,
        stats.udp_tx_datagrams,
        stats.udp_tx_bytes,
        stats.udp_rx_datagrams,
        stats.udp_rx_bytes,
        stats.datagram_max,
        stats.datagram_send_buffer_space,
        stats.pacing_uncapped_capacity_bytes,
        stats.pacing_capacity_bytes,
        stats.pacing_tokens_bytes,
        stats.current_mtu,
        stats.pacing_mtu,
        stats.pacing_cap_active,
        stats.pacing_delay_events,
    )
}

fn format_tuic_tcp_open_line(
    target: &TargetAddr,
    conn_index: usize,
    stable_id: usize,
    stream_id: u64,
    startup_auth_attempts: u64,
    relay_mode: TuicTcpRelayMode,
) -> String {
    let open_diag = current_tcp_relay_open_diag();
    format_tuic_tcp_open_line_with_diag(
        target,
        conn_index,
        stable_id,
        stream_id,
        startup_auth_attempts,
        relay_mode,
        open_diag.as_ref(),
    )
}

#[derive(Debug, Clone, Copy)]
struct TuicTcpPoolSelectionDiag<'a> {
    conn_index: usize,
    stable_id: usize,
    active_before: u64,
    path_service: TcpPoolPathService,
    path_service_tiebreak: bool,
    qualification: TcpPoolForwardQualification,
    qualification_anchor: Option<u64>,
    black_holes_current: Option<u64>,
    qualification_override: bool,
    all_degraded_fallback: bool,
    candidates: &'a [TcpPoolAdmissionCandidate],
    generation: u64,
    last_success_age_secs: Option<u64>,
    probe_result: &'a str,
    reconnect_reason: Option<&'a str>,
    replacement_installed: bool,
}

fn format_tuic_tcp_pool_selection_line(diag: TuicTcpPoolSelectionDiag<'_>) -> String {
    let TuicTcpPoolSelectionDiag {
        conn_index,
        stable_id,
        active_before,
        path_service,
        path_service_tiebreak,
        qualification,
        qualification_anchor,
        black_holes_current,
        qualification_override,
        all_degraded_fallback,
        candidates,
        generation,
        last_success_age_secs,
        probe_result,
        reconnect_reason,
        replacement_installed,
    } = diag;
    let last_success_age_secs = last_success_age_secs
        .map(|age| age.to_string())
        .unwrap_or_else(|| "never".into());
    let (path_cwnd, path_rtt_us) = match path_service {
        TcpPoolPathService::Unknown => ("unknown".into(), "unknown".into()),
        TcpPoolPathService::Known { cwnd, rtt_micros } => {
            (cwnd.to_string(), rtt_micros.to_string())
        }
    };
    let optional_counter = |value: Option<u64>| {
        value
            .map(|value| value.to_string())
            .unwrap_or_else(|| "unknown".into())
    };
    let candidates = candidates
        .iter()
        .map(|candidate| {
            let identity = candidate
                .identity
                .map(|identity| format!("{}@{}", identity.stable_id, identity.generation))
                .unwrap_or_else(|| "unknown".into());
            let (cwnd, rtt_us) = match candidate.path_service {
                TcpPoolPathService::Unknown => ("unknown".into(), "unknown".into()),
                TcpPoolPathService::Known { cwnd, rtt_micros } => {
                    (cwnd.to_string(), rtt_micros.to_string())
                }
            };
            format!(
                "conn{}:id={identity},active={},qualification={},anchor={},current={},admitted={},cwnd={cwnd},rtt_us={rtt_us}",
                candidate.index,
                candidate.active_before,
                candidate.qualification.as_str(),
                optional_counter(candidate.qualification_anchor),
                optional_counter(candidate.black_holes_current),
                candidate.admitted,
            )
        })
        .collect::<Vec<_>>()
        .join(";");
    format!(
        "🔎 tuic-tcp-pool-selection conn={conn_index} id={stable_id} policy=busy_epoch_forward_qualification_then_least_active_then_path_service active_before={active_before} path_cwnd={path_cwnd} path_rtt_us={path_rtt_us} path_service_tiebreak={path_service_tiebreak} qualification={} black_hole_anchor={} black_holes_current={} qualification_override={qualification_override} all_degraded_fallback={all_degraded_fallback} candidates=[{candidates}] generation={generation} replacement_installed={replacement_installed} last_success_age_secs={last_success_age_secs} probe_result={probe_result} reconnect_reason={}",
        qualification.as_str(),
        optional_counter(qualification_anchor),
        optional_counter(black_holes_current),
        reconnect_reason.unwrap_or("none")
    )
}

fn format_tuic_tcp_open_line_with_diag(
    target: &TargetAddr,
    conn_index: usize,
    stable_id: usize,
    stream_id: u64,
    startup_auth_attempts: u64,
    relay_mode: TuicTcpRelayMode,
    open_diag: Option<&TcpRelayOpenDiag>,
) -> String {
    let bridge = open_diag.map_or_else(String::new, |diag| {
        format!(" handle={} epoch={}", diag.handle, diag.epoch)
    });
    format!(
        "🔎 tuic-open-tcp target={} conn={} id={} stream={} relay_mode={} startup_auth_attempts={}{}",
        target.to_wire_string(),
        conn_index,
        stable_id,
        stream_id,
        relay_mode.as_str(),
        startup_auth_attempts,
        bridge
    )
}

#[derive(Debug, Clone)]
struct TuicTcpStreamDiagMeta {
    target: String,
    conn_index: usize,
    stable_id: usize,
    stream_id: u64,
}

impl TuicTcpStreamDiagMeta {
    fn new(target: &TargetAddr, conn_index: usize, stable_id: usize, stream_id: u64) -> Self {
        Self {
            target: target.to_wire_string(),
            conn_index,
            stable_id,
            stream_id,
        }
    }
}

fn format_tuic_tcp_startup_service_line(
    meta: &TuicTcpStreamDiagMeta,
    first_payload_bytes: usize,
    priority_before: i32,
    priority_after: i32,
) -> String {
    format!(
        "🔎 tuic-tcp-startup-service target={} conn={} id={} stream={} first_payload_bytes={} priority_before={} priority_after={} state=consumed",
        meta.target,
        meta.conn_index,
        meta.stable_id,
        meta.stream_id,
        first_payload_bytes,
        priority_before,
        priority_after,
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TuicTcpStreamReadEvent {
    first_rx_ms: Option<u128>,
    gap_ms: Option<u128>,
    read_bytes: usize,
    rx_bytes: u64,
    reads: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TuicTcpStreamCloseSnapshot {
    first_rx_ms: u128,
    max_read_gap_ms: u128,
    rx_bytes: u64,
    reads: u64,
    pending_polls: u64,
    max_pending_gap_ms: u128,
    polls: u64,
    max_poll_gap_ms: u128,
    self_wake_armed: u64,
    self_wake_fired: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TuicTcpStreamPendingEvent {
    pending_gap_ms: u128,
    pending_polls: u64,
    polls: u64,
    max_poll_gap_ms: u128,
    rx_bytes: u64,
    reads: u64,
    pending_cause: TuicTcpStreamPendingCause,
    transport: Option<TuicStreamTransportPending>,
    self_wake_armed: u64,
    self_wake_fired: u64,
}

#[derive(Debug, Clone)]
struct TuicTcpStreamDiag {
    meta: TuicTcpStreamDiagMeta,
    opened_at: Instant,
    first_rx_ms: Option<u128>,
    last_rx_at: Option<Instant>,
    max_read_gap_ms: u128,
    rx_bytes: u64,
    reads: u64,
    polls: u64,
    last_poll_at: Option<Instant>,
    max_poll_gap_ms: u128,
    pending_polls: u64,
    max_pending_gap_ms: u128,
    last_pending_log_at: Option<Instant>,
    last_read_transport_sample: Option<TuicStreamTransportSample>,
    last_pending_transport_sample: Option<TuicStreamTransportSample>,
    self_wake_armed: u64,
    self_wake_fired: u64,
}

impl TuicTcpStreamDiag {
    fn new(meta: TuicTcpStreamDiagMeta, opened_at: Instant) -> Self {
        Self {
            meta,
            opened_at,
            first_rx_ms: None,
            last_rx_at: None,
            max_read_gap_ms: 0,
            rx_bytes: 0,
            reads: 0,
            polls: 0,
            last_poll_at: None,
            max_poll_gap_ms: 0,
            pending_polls: 0,
            max_pending_gap_ms: 0,
            last_pending_log_at: None,
            last_read_transport_sample: None,
            last_pending_transport_sample: None,
            self_wake_armed: 0,
            self_wake_fired: 0,
        }
    }

    fn note_self_wake_armed(&mut self) {
        self.self_wake_armed = self.self_wake_armed.saturating_add(1);
    }

    fn note_self_wake_fired(&mut self) {
        self.self_wake_fired = self.self_wake_fired.saturating_add(1);
    }

    fn note_poll_at(&mut self, now: Instant) {
        self.polls += 1;
        if let Some(last) = self.last_poll_at {
            self.max_poll_gap_ms = self
                .max_poll_gap_ms
                .max(now.saturating_duration_since(last).as_millis());
        }
        self.last_poll_at = Some(now);
    }

    fn note_read_at(
        &mut self,
        read_bytes: usize,
        now: Instant,
        transport: Option<TuicStreamTransportSample>,
    ) -> TuicTcpStreamReadEvent {
        let first_rx_ms = if self.first_rx_ms.is_none() {
            let elapsed = now.saturating_duration_since(self.opened_at).as_millis();
            self.first_rx_ms = Some(elapsed);
            Some(elapsed)
        } else {
            None
        };
        let gap_ms = self
            .last_rx_at
            .map(|last| now.saturating_duration_since(last).as_millis());
        if let Some(gap) = gap_ms {
            self.max_read_gap_ms = self.max_read_gap_ms.max(gap);
        }
        self.last_rx_at = Some(now);
        self.reads += 1;
        self.rx_bytes += read_bytes as u64;
        if let Some(sample) = transport {
            self.last_read_transport_sample = Some(sample);
        }

        TuicTcpStreamReadEvent {
            first_rx_ms,
            gap_ms,
            read_bytes,
            rx_bytes: self.rx_bytes,
            reads: self.reads,
        }
    }

    fn note_pending_at(
        &mut self,
        now: Instant,
        transport: Option<TuicStreamTransportSample>,
    ) -> Option<TuicTcpStreamPendingEvent> {
        self.pending_polls += 1;
        let gap_base = self.last_rx_at.unwrap_or(self.opened_at);
        let pending_gap_ms = now.saturating_duration_since(gap_base).as_millis();
        self.max_pending_gap_ms = self.max_pending_gap_ms.max(pending_gap_ms);

        if pending_gap_ms < TUIC_TCP_STREAM_PENDING_LOG_MS {
            return None;
        }
        let should_log = self
            .last_pending_log_at
            .map(|last| {
                now.saturating_duration_since(last).as_millis() >= TUIC_TCP_STREAM_PENDING_LOG_MS
            })
            .unwrap_or(true);
        if !should_log {
            return None;
        }
        self.last_pending_log_at = Some(now);
        let transport = transport.map(|sample| {
            let pending = TuicStreamTransportPending {
                sample,
                since_last_read: transport_delta(
                    self.last_read_transport_sample.unwrap_or_default(),
                    sample,
                ),
                since_last_pending: transport_delta(
                    self.last_pending_transport_sample.unwrap_or_default(),
                    sample,
                ),
            };
            self.last_pending_transport_sample = Some(sample);
            pending
        });
        let pending_cause = classify_tuic_stream_pending_cause(transport);
        Some(TuicTcpStreamPendingEvent {
            pending_gap_ms,
            pending_polls: self.pending_polls,
            polls: self.polls,
            max_poll_gap_ms: self.max_poll_gap_ms,
            rx_bytes: self.rx_bytes,
            reads: self.reads,
            pending_cause,
            transport,
            self_wake_armed: self.self_wake_armed,
            self_wake_fired: self.self_wake_fired,
        })
    }

    fn close_snapshot(&self) -> TuicTcpStreamCloseSnapshot {
        TuicTcpStreamCloseSnapshot {
            first_rx_ms: self.first_rx_ms.unwrap_or(0),
            max_read_gap_ms: self.max_read_gap_ms,
            rx_bytes: self.rx_bytes,
            reads: self.reads,
            pending_polls: self.pending_polls,
            max_pending_gap_ms: self.max_pending_gap_ms,
            polls: self.polls,
            max_poll_gap_ms: self.max_poll_gap_ms,
            self_wake_armed: self.self_wake_armed,
            self_wake_fired: self.self_wake_fired,
        }
    }
}

fn transport_delta(
    last: TuicStreamTransportSample,
    current: TuicStreamTransportSample,
) -> TuicStreamTransportDelta {
    TuicStreamTransportDelta {
        udp_rx_datagrams: current
            .udp_rx_datagrams
            .saturating_sub(last.udp_rx_datagrams),
        udp_rx_bytes: current.udp_rx_bytes.saturating_sub(last.udp_rx_bytes),
        rx_stream_frames: current
            .rx_stream_frames
            .saturating_sub(last.rx_stream_frames),
    }
}

fn format_tuic_tcp_stream_first_rx_line(
    meta: &TuicTcpStreamDiagMeta,
    first_rx_ms: u128,
    read_bytes: usize,
    reads: u64,
) -> String {
    format!(
        "🔎 tuic-tcp-stream-first-rx target={} conn={} id={} stream={} first_rx_ms={} read_bytes={} reads={}",
        meta.target,
        meta.conn_index,
        meta.stable_id,
        meta.stream_id,
        first_rx_ms,
        read_bytes,
        reads
    )
}

fn format_tuic_tcp_stream_read_gap_line(
    meta: &TuicTcpStreamDiagMeta,
    gap_ms: u128,
    read_bytes: usize,
    reads: u64,
    rx_bytes: u64,
) -> String {
    format!(
        "🔎 tuic-tcp-stream-read-gap target={} conn={} id={} stream={} gap_ms={} read_bytes={} reads={} rx_bytes={}",
        meta.target,
        meta.conn_index,
        meta.stable_id,
        meta.stream_id,
        gap_ms,
        read_bytes,
        reads,
        rx_bytes
    )
}

#[allow(clippy::too_many_arguments)]
fn format_tuic_tcp_stream_pending_line(
    meta: &TuicTcpStreamDiagMeta,
    pending_gap_ms: u128,
    pending_polls: u64,
    polls: u64,
    max_poll_gap_ms: u128,
    rx_bytes: u64,
    reads: u64,
    pending_cause: TuicTcpStreamPendingCause,
    transport: Option<TuicStreamTransportPending>,
    self_wake_armed: u64,
    self_wake_fired: u64,
) -> String {
    let mut line = format!(
        "🔎 tuic-tcp-stream-pending target={} conn={} id={} stream={} pending_gap_ms={} pending_polls={} polls={} max_poll_gap_ms={} rx_bytes={} reads={} pending_cause={} self_wake_armed={} self_wake_fired={}",
        meta.target,
        meta.conn_index,
        meta.stable_id,
        meta.stream_id,
        pending_gap_ms,
        pending_polls,
        polls,
        max_poll_gap_ms,
        rx_bytes,
        reads,
        pending_cause.as_str(),
        self_wake_armed,
        self_wake_fired
    );
    if let Some(transport) = transport {
        line.push_str(&format!(
            " conn_udp_rx={}/{}B conn_udp_rx_since_read={}/{}B conn_udp_rx_since_pending={}/{}B conn_rx_stream_frames={} conn_rx_stream_frames_since_read={} conn_rx_stream_frames_since_pending={} conn_ack_frames(rx={},tx={}) conn_plpmtud(sent={},lost={},black_holes={})",
            transport.sample.udp_rx_datagrams,
            transport.sample.udp_rx_bytes,
            transport.since_last_read.udp_rx_datagrams,
            transport.since_last_read.udp_rx_bytes,
            transport.since_last_pending.udp_rx_datagrams,
            transport.since_last_pending.udp_rx_bytes,
            transport.sample.rx_stream_frames,
            transport.since_last_read.rx_stream_frames,
            transport.since_last_pending.rx_stream_frames,
            transport.sample.rx_ack_frames,
            transport.sample.tx_ack_frames,
            transport.sample.sent_plpmtud_probes,
            transport.sample.lost_plpmtud_probes,
            transport.sample.black_holes_detected
        ));
    }
    line
}

#[allow(clippy::too_many_arguments)]
fn format_tuic_tcp_unordered_staging_line(
    meta: &TuicTcpStreamDiagMeta,
    reason: &str,
    next_offset: u64,
    chunk_offset: u64,
    chunk_bytes: usize,
    buffered_bytes: usize,
    buffered_chunks: usize,
    max_buffered_bytes: usize,
    unordered_chunks: u64,
    unordered_bytes: u64,
    out_of_order_chunks: u64,
    max_gap_bytes: u64,
    gap_bytes: u64,
    staging_cap_hits: u64,
    cap_bytes: usize,
) -> String {
    format!(
        "🔎 tuic-tcp-unordered-staging target={} conn={} id={} stream={} reason={} next_offset={} chunk_offset={} chunk_bytes={} gap_bytes={} max_gap_bytes={} buffered={}B buffered_chunks={} max_buffered={}B unordered_chunks={} unordered_bytes={} out_of_order_chunks={} cap_hits={} cap={}B",
        meta.target,
        meta.conn_index,
        meta.stable_id,
        meta.stream_id,
        reason,
        next_offset,
        chunk_offset,
        chunk_bytes,
        gap_bytes,
        max_gap_bytes,
        buffered_bytes,
        buffered_chunks,
        max_buffered_bytes,
        unordered_chunks,
        unordered_bytes,
        out_of_order_chunks,
        staging_cap_hits,
        cap_bytes
    )
}

fn format_tuic_tcp_stream_close_line(
    meta: &TuicTcpStreamDiagMeta,
    snapshot: &TuicTcpStreamCloseSnapshot,
) -> String {
    format!(
        "🔎 tuic-tcp-stream-close target={} conn={} id={} stream={} first_rx_ms={} max_read_gap_ms={} rx_bytes={} reads={} pending_polls={} max_pending_gap_ms={} polls={} max_poll_gap_ms={} self_wake_armed={} self_wake_fired={}",
        meta.target,
        meta.conn_index,
        meta.stable_id,
        meta.stream_id,
        snapshot.first_rx_ms,
        snapshot.max_read_gap_ms,
        snapshot.rx_bytes,
        snapshot.reads,
        snapshot.pending_polls,
        snapshot.max_pending_gap_ms,
        snapshot.polls,
        snapshot.max_poll_gap_ms,
        snapshot.self_wake_armed,
        snapshot.self_wake_fired
    )
}

fn quic_stats_snapshot(conn: &Connection) -> QuicStatsSnapshot {
    let stats = conn.stats();
    QuicStatsSnapshot {
        rtt_ms: stats.path.rtt.as_millis(),
        cwnd: stats.path.cwnd,
        lost_packets: stats.path.lost_packets,
        sent_packets: stats.path.sent_packets,
        lost_bytes: stats.path.lost_bytes,
        congestion_events: stats.path.congestion_events,
        sent_plpmtud_probes: stats.path.sent_plpmtud_probes,
        lost_plpmtud_probes: stats.path.lost_plpmtud_probes,
        black_holes_detected: stats.path.black_holes_detected,
        tx_ack_frames: stats.frame_tx.acks,
        rx_ack_frames: stats.frame_rx.acks,
        rx_stream_frames: stats.frame_rx.stream,
        tx_data_blocked: stats.frame_tx.data_blocked,
        tx_stream_data_blocked: stats.frame_tx.stream_data_blocked,
        tx_streams_blocked_bidi: stats.frame_tx.streams_blocked_bidi,
        tx_streams_blocked_uni: stats.frame_tx.streams_blocked_uni,
        tx_max_data: stats.frame_tx.max_data,
        tx_max_stream_data: stats.frame_tx.max_stream_data,
        rx_data_blocked: stats.frame_rx.data_blocked,
        rx_stream_data_blocked: stats.frame_rx.stream_data_blocked,
        rx_max_data: stats.frame_rx.max_data,
        rx_max_stream_data: stats.frame_rx.max_stream_data,
        udp_tx_datagrams: stats.udp_tx.datagrams,
        udp_tx_bytes: stats.udp_tx.bytes,
        udp_rx_datagrams: stats.udp_rx.datagrams,
        udp_rx_bytes: stats.udp_rx.bytes,
        datagram_max: conn.max_datagram_size(),
        datagram_send_buffer_space: conn.datagram_send_buffer_space(),
        pacing_uncapped_capacity_bytes: stats.path.pacing_uncapped_capacity_bytes,
        pacing_capacity_bytes: stats.path.pacing_capacity_bytes,
        pacing_tokens_bytes: stats.path.pacing_tokens_bytes,
        current_mtu: stats.path.current_mtu,
        pacing_mtu: stats.path.pacing_mtu,
        pacing_cap_active: stats.path.pacing_cap_active,
        pacing_delay_events: stats.path.pacing_delay_events,
    }
}

fn sample_tuic_stream_transport(conn: &Connection) -> TuicStreamTransportSample {
    let stats = conn.stats();
    TuicStreamTransportSample {
        udp_rx_datagrams: stats.udp_rx.datagrams,
        udp_rx_bytes: stats.udp_rx.bytes,
        rx_stream_frames: stats.frame_rx.stream,
        rx_ack_frames: stats.frame_rx.acks,
        tx_ack_frames: stats.frame_tx.acks,
        sent_plpmtud_probes: stats.path.sent_plpmtud_probes,
        lost_plpmtud_probes: stats.path.lost_plpmtud_probes,
        black_holes_detected: stats.path.black_holes_detected,
    }
}

fn spawn_quic_stats_logger(
    conn: Connection,
    conn_index: usize,
    interval_secs: u64,
    mut stop_rx: watch::Receiver<bool>,
) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(interval_secs));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = tick.tick() => {
                    let stable_id = conn.stable_id();
                    if let Some(reason) = conn.close_reason() {
                        println!("📊 TUIC QUIC stats conn={conn_index} id={stable_id} closed={reason:?}");
                        break;
                    }
                    println!(
                        "{}",
                        format_quic_stats_line(conn_index, stable_id, quic_stats_snapshot(&conn))
                    );
                    if let Some(stats) = conn.endpoint_pacing_snapshot() {
                        println!(
                            "{}",
                            format_endpoint_pacing_connection_stats_line(
                                conn_index,
                                stable_id,
                                stats,
                            )
                        );
                    }
                }
                changed = stop_rx.changed() => {
                    if changed.is_err() || *stop_rx.borrow() {
                        break;
                    }
                }
            }
        }
    });
}

fn format_endpoint_pacing_connection_stats_line(
    conn_index: usize,
    stable_id: usize,
    stats: quinn::EndpointPacingConnectionSnapshot,
) -> String {
    format!(
        "📊 TUIC endpoint pacing conn={conn_index} id={stable_id} handle={} generation={} \
         attached={} live={}B outstanding={}B grants(bulk={}B,control={}B) \
         turns={} waits={} max_service_gap={}us",
        stats.connection_handle,
        stats.path_generation,
        stats.attached,
        stats.live_reservation_bytes,
        stats.outstanding_bytes,
        stats.granted_bulk_bytes,
        stats.granted_control_bytes,
        stats.turn_count,
        stats.wait_count,
        stats.max_service_gap_nanos / 1_000,
    )
}

fn format_endpoint_pacing_stats_line(stats: quinn::EndpointPacingSnapshot) -> String {
    format!(
        "📊 TUIC endpoint pacing global config(rate={}B/s,burst={}B,control_reserve={}B,quantum={}B) \
         conservation(available={}B,live={}B,outstanding={}B,records={}) \
         grants={}/{}B refunds={}/{}B sent={}/{}B abandoned={}/{}B \
         outstanding_high_water={}B would_block(events={},high_water={}B) \
         delay(events={},max={}us) fairness_lead_high_water={}B \
         waiters(control={},bulk={},high_water={}) lifecycle(cancel={},migrate={},detach={}) \
         stale_wakers={} stateless(sent={},dropped={})",
        stats.rate_bytes_per_second,
        stats.burst_bytes,
        stats.control_reserve_bytes,
        stats.connection_quantum_bytes,
        stats.available_tokens,
        stats.live_reservation_bytes,
        stats.outstanding_bytes,
        stats.connection_records,
        stats.granted_datagrams,
        stats.granted_bytes,
        stats.refund_events,
        stats.refunded_bytes,
        stats.sent_datagrams,
        stats.sent_bytes,
        stats.abandoned_datagrams,
        stats.abandoned_bytes,
        stats.outstanding_bytes_high_water,
        stats.socket_would_block_events,
        stats.socket_would_block_outstanding_high_water,
        stats.endpoint_delay_events,
        stats.max_endpoint_delay_nanos / 1_000,
        stats.fairness_lead_high_water_bytes,
        stats.control_waiters,
        stats.bulk_waiters,
        stats.waiter_high_water,
        stats.cancellations,
        stats.migrations,
        stats.detaches,
        stats.stale_waker_events,
        stats.stateless_responses_sent,
        stats.stateless_responses_dropped,
    )
}

fn spawn_endpoint_pacing_stats_logger(
    endpoint: Endpoint,
    interval_secs: u64,
    mut stop_rx: watch::Receiver<bool>,
) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(interval_secs));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = tick.tick() => {
                    let Some(stats) = endpoint.endpoint_pacing_snapshot() else {
                        break;
                    };
                    println!("{}", format_endpoint_pacing_stats_line(stats));
                }
                changed = stop_rx.changed() => {
                    if changed.is_err() || *stop_rx.borrow() {
                        break;
                    }
                }
            }
        }
    });
}

fn format_udp_send_service_stats_line(stats: quic::QuicUdpSendServiceSnapshot) -> String {
    let mean_payload_bytes = stats
        .accepted_bytes
        .checked_div(stats.accepted_datagrams)
        .unwrap_or(0);
    let mean_rearm_period_us = if stats.cooldown_rearms == 0 {
        0.0
    } else {
        stats.service_elapsed_ns as f64 / stats.cooldown_rearms as f64 / 1_000.0
    };
    let total_rearm_lateness_us = stats.total_rearm_lateness_ns as f64 / 1_000.0;
    let mean_rearm_lateness_us = if stats.cooldown_rearms == 0 {
        0.0
    } else {
        total_rearm_lateness_us / stats.cooldown_rearms as f64
    };
    let max_rearm_lateness_us = stats.max_rearm_lateness_ns as f64 / 1_000.0;
    format!(
        "📊 QUIC UDP send service accepted={}/{}B service_elapsed_ms={:.3} \
         service_rate={}dg/s/{}B/s mean_payload={}B batch_closes={} timer_blocked_polls={} \
         gate_would_block={} cooldown_rearms={} mean_rearm_period_us={:.3} \
         rearm_lateness_us(total={:.3},mean={:.3},max={:.3}) wasted_datagrams={} \
         inner_would_block={} inner_errors={} invalid_transmits={}",
        stats.accepted_datagrams,
        stats.accepted_bytes,
        stats.service_elapsed_ns as f64 / 1_000_000.0,
        stats.accepted_datagrams_per_sec,
        stats.accepted_bytes_per_sec,
        mean_payload_bytes,
        stats.batch_closes,
        stats.timer_blocked_polls,
        stats.gate_would_block,
        stats.cooldown_rearms,
        mean_rearm_period_us,
        total_rearm_lateness_us,
        mean_rearm_lateness_us,
        max_rearm_lateness_us,
        stats.wasted_datagrams,
        stats.inner_would_block,
        stats.inner_errors,
        stats.invalid_transmits,
    )
}

fn spawn_udp_send_service_stats_logger(
    stats: quic::QuicUdpSendServiceStats,
    interval_secs: u64,
    mut stop_rx: watch::Receiver<bool>,
) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(interval_secs));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = tick.tick() => {
                    println!("{}", format_udp_send_service_stats_line(stats.snapshot()));
                }
                changed = stop_rx.changed() => {
                    if changed.is_err() || *stop_rx.borrow() {
                        break;
                    }
                }
            }
        }
    });
}

/// 把任意可显示错误包成 ClientError（统一错误面）。
fn io_err<E: std::fmt::Display>(ctx: &str, e: E) -> ClientError {
    ClientError::from(std::io::Error::other(format!("{ctx}: {e}")))
}

/// 刀9（spec §2.3）：QUIC 重连握手超时。黑洞/不可达 server 的握手默认受 idle 约束可阻塞数十秒，
/// 5s 封顶让 failover 快路（连接死 + 重连失败）快速暴露；正常重连 1-RTT 远小于 5s，不会误伤高 RTT 链路。
const TUIC_RECONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

const ENDPOINT_RECOVERY_MIN_STALL: Duration = Duration::from_secs(2);
const ENDPOINT_RECOVERY_MAX_STALL: Duration = Duration::from_secs(7);
const ENDPOINT_RECOVERY_RTT_MULTIPLIER: u32 = 8;
const ENDPOINT_RECOVERY_SAMPLE_INTERVAL: Duration = Duration::from_millis(250);

fn endpoint_recovery_stall_bound(max_rtt: Duration) -> Duration {
    max_rtt
        .saturating_mul(ENDPOINT_RECOVERY_RTT_MULTIPLIER)
        .clamp(ENDPOINT_RECOVERY_MIN_STALL, ENDPOINT_RECOVERY_MAX_STALL)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TcpWritePressureSnapshot {
    writer: u64,
    stream: u64,
    episode: u64,
    pending_for: Duration,
    ack_stalled_for: Duration,
    acknowledged_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TcpOrderedReadProgressSnapshot {
    reader: u64,
    stream: u64,
    read_offset: u64,
    next_received_offset: Option<u64>,
    highest_received_offset: u64,
    buffered_bytes: usize,
    ordered_gap_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecoveryEvidence {
    OrderedGapObserved {
        stable_id: usize,
        reader: u64,
        stream: u64,
        episode: u64,
        read_offset: u64,
        next_received_offset: u64,
        initial_highest_received_offset: u64,
        current_highest_received_offset: u64,
        initial_buffered_bytes: usize,
        current_buffered_bytes: usize,
        ordered_gap_bytes: u64,
        observations: u64,
        observed_for: Duration,
        tail_advanced: bool,
    },
    TcpWritePressureStarted {
        stable_id: usize,
        writer: u64,
        stream: u64,
        episode: u64,
        acknowledged_bytes: u64,
        pending_for: Duration,
        ack_stalled_for: Duration,
    },
    TcpWritePressureEnded {
        stable_id: usize,
        writer: u64,
        stream: u64,
        episode: u64,
        observations: u64,
        observed_for: Duration,
        initial_acknowledged_bytes: u64,
        final_acknowledged_bytes: u64,
        ack_progress_observations: u64,
        max_pending_for: Duration,
        max_ack_stalled_for: Duration,
    },
}

#[derive(Debug, Clone, Copy)]
struct OrderedGapEvidenceAnchor {
    initial: TcpOrderedReadProgressSnapshot,
    current: TcpOrderedReadProgressSnapshot,
    episode: u64,
    observed_at: Instant,
    observations: u64,
    reported: bool,
}

#[derive(Debug, Clone, Copy)]
struct TcpWriteEvidenceAnchor {
    stable_id: usize,
    initial: TcpWritePressureSnapshot,
    current: TcpWritePressureSnapshot,
    observed_at: Instant,
    observations: u64,
    ack_progress_observations: u64,
    max_pending_for: Duration,
    max_ack_stalled_for: Duration,
}

/// Pure, bounded evidence policy. It consumes scalar snapshots from the existing recovery
/// sampler and can only return diagnostic events; Endpoint and connection mutation remain in
/// `EndpointRecoveryState`.
#[derive(Debug, Default)]
struct RecoveryEvidenceObserver {
    ordered_gap_anchors: HashMap<(usize, u64), OrderedGapEvidenceAnchor>,
    next_ordered_gap_episode: u64,
    tcp_write_anchors: HashMap<(usize, u64), TcpWriteEvidenceAnchor>,
}

impl RecoveryEvidenceObserver {
    fn observe(&mut self, now: Instant, input: &EndpointRecoveryInput) -> Vec<RecoveryEvidence> {
        let mut events = self.observe_tcp_write_pressure(now, input);
        events.extend(self.observe_ordered_gaps(now, input));
        events
    }

    fn observe_tcp_write_pressure(
        &mut self,
        now: Instant,
        input: &EndpointRecoveryInput,
    ) -> Vec<RecoveryEvidence> {
        let current = input
            .connections
            .iter()
            .flat_map(|connection| {
                connection
                    .tcp_write_pressures
                    .iter()
                    .copied()
                    .map(move |pressure| ((connection.stable_id, pressure.writer), pressure))
            })
            .collect::<HashMap<_, _>>();
        let mut events = Vec::new();

        let mut ended = self
            .tcp_write_anchors
            .iter()
            .filter_map(|(key, anchor)| {
                current
                    .get(key)
                    .is_none_or(|pressure| pressure.episode != anchor.current.episode)
                    .then_some(*key)
            })
            .collect::<Vec<_>>();
        ended.sort_unstable();
        for key in ended {
            if let Some(anchor) = self.tcp_write_anchors.remove(&key) {
                events.push(RecoveryEvidence::TcpWritePressureEnded {
                    stable_id: anchor.stable_id,
                    writer: anchor.current.writer,
                    stream: anchor.current.stream,
                    episode: anchor.current.episode,
                    observations: anchor.observations,
                    observed_for: now.saturating_duration_since(anchor.observed_at),
                    initial_acknowledged_bytes: anchor.initial.acknowledged_bytes,
                    final_acknowledged_bytes: anchor.current.acknowledged_bytes,
                    ack_progress_observations: anchor.ack_progress_observations,
                    max_pending_for: anchor.max_pending_for,
                    max_ack_stalled_for: anchor.max_ack_stalled_for,
                });
            }
        }

        let mut live = current.into_iter().collect::<Vec<_>>();
        live.sort_unstable_by_key(|(key, _)| *key);
        for ((stable_id, writer), pressure) in live {
            if let Some(anchor) = self.tcp_write_anchors.get_mut(&(stable_id, writer)) {
                if pressure.acknowledged_bytes > anchor.current.acknowledged_bytes {
                    anchor.ack_progress_observations =
                        anchor.ack_progress_observations.saturating_add(1);
                }
                anchor.current = pressure;
                anchor.observations = anchor.observations.saturating_add(1);
                anchor.max_pending_for = anchor.max_pending_for.max(pressure.pending_for);
                anchor.max_ack_stalled_for =
                    anchor.max_ack_stalled_for.max(pressure.ack_stalled_for);
                continue;
            }

            self.tcp_write_anchors.insert(
                (stable_id, writer),
                TcpWriteEvidenceAnchor {
                    stable_id,
                    initial: pressure,
                    current: pressure,
                    observed_at: now,
                    observations: 1,
                    ack_progress_observations: 0,
                    max_pending_for: pressure.pending_for,
                    max_ack_stalled_for: pressure.ack_stalled_for,
                },
            );
            events.push(RecoveryEvidence::TcpWritePressureStarted {
                stable_id,
                writer,
                stream: pressure.stream,
                episode: pressure.episode,
                acknowledged_bytes: pressure.acknowledged_bytes,
                pending_for: pressure.pending_for,
                ack_stalled_for: pressure.ack_stalled_for,
            });
        }
        events
    }

    fn observe_ordered_gaps(
        &mut self,
        now: Instant,
        input: &EndpointRecoveryInput,
    ) -> Vec<RecoveryEvidence> {
        if input.active_tcp == 0 {
            self.ordered_gap_anchors.clear();
            return Vec::new();
        }

        let current = input
            .connections
            .iter()
            .flat_map(|connection| {
                connection
                    .tcp_ordered_read_progress
                    .iter()
                    .copied()
                    .map(move |progress| ((connection.stable_id, progress.reader), progress))
            })
            .collect::<HashMap<_, _>>();
        self.ordered_gap_anchors.retain(|key, _| {
            current
                .get(key)
                .is_some_and(|progress| progress.ordered_gap_bytes > 0)
        });

        let mut live = current.into_iter().collect::<Vec<_>>();
        live.sort_unstable_by_key(|(key, _)| *key);
        let mut events = Vec::new();
        for (key, progress) in live {
            if progress.ordered_gap_bytes == 0 {
                self.ordered_gap_anchors.remove(&key);
                continue;
            }
            let unchanged = self.ordered_gap_anchors.get(&key).is_some_and(|anchor| {
                anchor.current.stream == progress.stream
                    && anchor.current.read_offset == progress.read_offset
                    && anchor.current.next_received_offset == progress.next_received_offset
            });
            if unchanged {
                let Some(anchor) = self.ordered_gap_anchors.get_mut(&key) else {
                    continue;
                };
                anchor.current = progress;
                anchor.observations = anchor.observations.saturating_add(1);
                let observed_for = now.saturating_duration_since(anchor.observed_at);
                if !anchor.reported
                    && anchor.observations >= 2
                    && observed_for >= ENDPOINT_RECOVERY_SAMPLE_INTERVAL
                {
                    anchor.reported = true;
                    let next_received_offset = progress
                        .next_received_offset
                        .unwrap_or(progress.read_offset);
                    events.push(RecoveryEvidence::OrderedGapObserved {
                        stable_id: key.0,
                        reader: progress.reader,
                        stream: progress.stream,
                        episode: anchor.episode,
                        read_offset: progress.read_offset,
                        next_received_offset,
                        initial_highest_received_offset: anchor.initial.highest_received_offset,
                        current_highest_received_offset: progress.highest_received_offset,
                        initial_buffered_bytes: anchor.initial.buffered_bytes,
                        current_buffered_bytes: progress.buffered_bytes,
                        ordered_gap_bytes: progress.ordered_gap_bytes,
                        observations: anchor.observations,
                        observed_for,
                        tail_advanced: progress.highest_received_offset
                            > anchor.initial.highest_received_offset
                            || progress.buffered_bytes > anchor.initial.buffered_bytes,
                    });
                }
                continue;
            }

            self.next_ordered_gap_episode = self.next_ordered_gap_episode.saturating_add(1);
            self.ordered_gap_anchors.insert(
                key,
                OrderedGapEvidenceAnchor {
                    initial: progress,
                    current: progress,
                    episode: self.next_ordered_gap_episode,
                    observed_at: now,
                    observations: 1,
                    reported: false,
                },
            );
        }
        events
    }
}

fn log_recovery_evidence(event: RecoveryEvidence) {
    match event {
        RecoveryEvidence::OrderedGapObserved {
            stable_id,
            reader,
            stream,
            episode,
            read_offset,
            next_received_offset,
            initial_highest_received_offset,
            current_highest_received_offset,
            initial_buffered_bytes,
            current_buffered_bytes,
            ordered_gap_bytes,
            observations,
            observed_for,
            tail_advanced,
        } => println!(
            "🔎 tuic-recovery-evidence kind=tcp_ordered_gap_observed action=none conn={stable_id} reader={reader} stream={stream} episode={episode} read_offset={read_offset} next_received_offset={next_received_offset} initial_highest_received_offset={initial_highest_received_offset} current_highest_received_offset={current_highest_received_offset} initial_buffered={initial_buffered_bytes}B current_buffered={current_buffered_bytes}B gap={ordered_gap_bytes}B observations={observations} observed_ms={} tail_advanced={tail_advanced}",
            observed_for.as_millis(),
        ),
        RecoveryEvidence::TcpWritePressureStarted {
            stable_id,
            writer,
            stream,
            episode,
            acknowledged_bytes,
            pending_for,
            ack_stalled_for,
        } => println!(
            "🔎 tuic-recovery-evidence kind=tcp_write_pressure_start action=none conn={stable_id} writer={writer} stream={stream} episode={episode} acknowledged={acknowledged_bytes}B pending_ms={} ack_stalled_ms={}",
            pending_for.as_millis(),
            ack_stalled_for.as_millis(),
        ),
        RecoveryEvidence::TcpWritePressureEnded {
            stable_id,
            writer,
            stream,
            episode,
            observations,
            observed_for,
            initial_acknowledged_bytes,
            final_acknowledged_bytes,
            ack_progress_observations,
            max_pending_for,
            max_ack_stalled_for,
        } => println!(
            "🔎 tuic-recovery-evidence kind=tcp_write_pressure_end action=none conn={stable_id} writer={writer} stream={stream} episode={episode} observations={observations} observed_ms={} initial_acknowledged={initial_acknowledged_bytes}B final_acknowledged={final_acknowledged_bytes}B acknowledged_delta={}B ack_progress_observations={ack_progress_observations} max_pending_ms={} max_ack_stalled_ms={}",
            observed_for.as_millis(),
            final_acknowledged_bytes.saturating_sub(initial_acknowledged_bytes),
            max_pending_for.as_millis(),
            max_ack_stalled_for.as_millis(),
        ),
    }
}

/// Per-generation registry of live ordered TCP readers. Sampling happens only on the existing
/// 250ms recovery task; the receive hot path does not acquire this registry or a recovery lock.
struct TcpOrderedReadPressure {
    next_reader: AtomicU64,
    readers: StdMutex<HashMap<u64, TcpOrderedReadPressureEntry>>,
}

struct TcpOrderedReadPressureEntry {
    stream: u64,
    progress: quinn::RecvStreamProgress,
}

struct TcpOrderedReadPressureReader {
    pressure: Arc<TcpOrderedReadPressure>,
    reader: u64,
}

impl TcpOrderedReadPressure {
    fn new() -> Self {
        Self {
            next_reader: AtomicU64::new(0),
            readers: StdMutex::new(HashMap::new()),
        }
    }

    fn reader(
        self: &Arc<Self>,
        stream: u64,
        progress: quinn::RecvStreamProgress,
    ) -> TcpOrderedReadPressureReader {
        let reader = self
            .next_reader
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        self.readers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(reader, TcpOrderedReadPressureEntry { stream, progress });
        TcpOrderedReadPressureReader {
            pressure: self.clone(),
            reader,
        }
    }

    fn snapshots(&self) -> Vec<TcpOrderedReadProgressSnapshot> {
        self.readers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .filter_map(|(reader, entry)| {
                let progress = entry.progress.sample().ok()?;
                Some(TcpOrderedReadProgressSnapshot {
                    reader: *reader,
                    stream: entry.stream,
                    read_offset: progress.read_offset,
                    next_received_offset: progress.next_received_offset,
                    highest_received_offset: progress.highest_received_offset,
                    buffered_bytes: progress.buffered_bytes,
                    ordered_gap_bytes: progress.ordered_gap_bytes,
                })
            })
            .collect()
    }

    fn remove_reader(&self, reader: u64) {
        self.readers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&reader);
    }
}

impl Drop for TcpOrderedReadPressureReader {
    fn drop(&mut self) {
        self.pressure.remove_reader(self.reader);
    }
}

/// Per-QUIC-connection registry for TCP writers currently blocked in `poll_write`.
/// Each writer owns its own atomic progress clock so ACKs on unrelated streams cannot mask a
/// business-stream black hole. The registry lock is used only at relay create/drop and by the
/// 250ms recovery sampler; the write hot path touches only its writer-local atomics.
struct TcpWritePressure {
    origin: Instant,
    next_writer: AtomicU64,
    next_episode: AtomicU64,
    writers: StdMutex<HashMap<u64, Weak<TcpWritePressureWriterState>>>,
}

struct TcpWritePressureWriterState {
    writer: u64,
    stream: u64,
    episode: AtomicU64,
    pending_since_micros: AtomicU64,
    ack_stalled_since_micros: AtomicU64,
    acknowledged_bytes: AtomicU64,
    progress: OnceLock<quinn::SendStreamProgress>,
}

impl TcpWritePressure {
    fn new(origin: Instant) -> Self {
        Self {
            origin,
            next_writer: AtomicU64::new(0),
            next_episode: AtomicU64::new(0),
            writers: StdMutex::new(HashMap::new()),
        }
    }

    fn writer(self: &Arc<Self>, stream: u64) -> TcpWritePressureWriter {
        let writer = self
            .next_writer
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        let state = Arc::new(TcpWritePressureWriterState {
            writer,
            stream,
            episode: AtomicU64::new(0),
            pending_since_micros: AtomicU64::new(0),
            ack_stalled_since_micros: AtomicU64::new(0),
            acknowledged_bytes: AtomicU64::new(0),
            progress: OnceLock::new(),
        });
        self.writers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(writer, Arc::downgrade(&state));
        TcpWritePressureWriter {
            pressure: self.clone(),
            state,
        }
    }

    fn elapsed_micros_at(&self, now: Instant) -> u64 {
        now.saturating_duration_since(self.origin)
            .as_micros()
            .min(u64::MAX as u128) as u64
    }

    fn next_episode(&self) -> u64 {
        self.next_episode
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1)
    }

    fn snapshots_at(&self, now: Instant) -> Vec<TcpWritePressureSnapshot> {
        let now_micros = self.elapsed_micros_at(now);
        let mut writers = self
            .writers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut snapshots = Vec::new();
        writers.retain(|_, weak| {
            let Some(state) = weak.upgrade() else {
                return false;
            };
            state.refresh_acknowledged_at(now_micros);
            if let Some(snapshot) = state.snapshot_at(now_micros) {
                snapshots.push(snapshot);
            }
            true
        });
        snapshots
    }

    #[cfg(test)]
    fn snapshot_at(&self, now: Instant) -> Option<TcpWritePressureSnapshot> {
        self.snapshots_at(now)
            .into_iter()
            .max_by_key(|snapshot| (snapshot.ack_stalled_for, snapshot.pending_for))
    }

    fn remove_writer(&self, writer: u64) {
        self.writers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&writer);
    }
}

struct TcpWritePressureWriter {
    pressure: Arc<TcpWritePressure>,
    state: Arc<TcpWritePressureWriterState>,
}

impl TcpWritePressureWriterState {
    fn install_progress(&self, progress: quinn::SendStreamProgress) {
        let _ = self.progress.set(progress);
    }

    fn refresh_acknowledged_at(&self, now_micros: u64) {
        if self.episode.load(Ordering::Acquire) == 0 {
            return;
        }
        let acknowledged_bytes = self
            .progress
            .get()
            .and_then(|progress| progress.sample().ok())
            .map(|progress| progress.acknowledged_bytes);
        if let Some(acknowledged_bytes) = acknowledged_bytes {
            self.note_acknowledged_micros(now_micros, acknowledged_bytes);
        }
    }

    fn note_acknowledged_micros(&self, now_micros: u64, acknowledged_bytes: u64) {
        if self.episode.load(Ordering::Acquire) == 0 {
            return;
        }
        let previous = self
            .acknowledged_bytes
            .fetch_max(acknowledged_bytes, Ordering::AcqRel);
        if acknowledged_bytes <= previous {
            return;
        }
        self.ack_stalled_since_micros
            .store(now_micros, Ordering::Release);
    }

    fn snapshot_at(&self, now_micros: u64) -> Option<TcpWritePressureSnapshot> {
        let episode = self.episode.load(Ordering::Acquire);
        if episode == 0 {
            return None;
        }
        let pending_since = self.pending_since_micros.load(Ordering::Relaxed);
        let ack_stalled_since = self.ack_stalled_since_micros.load(Ordering::Relaxed);
        let acknowledged_bytes = self.acknowledged_bytes.load(Ordering::Relaxed);
        if self.episode.load(Ordering::Acquire) != episode {
            return None;
        }
        Some(TcpWritePressureSnapshot {
            writer: self.writer,
            stream: self.stream,
            episode,
            pending_for: Duration::from_micros(now_micros.saturating_sub(pending_since)),
            ack_stalled_for: Duration::from_micros(now_micros.saturating_sub(ack_stalled_since)),
            acknowledged_bytes,
        })
    }
}

impl TcpWritePressureWriter {
    fn install_progress(&mut self, progress: quinn::SendStreamProgress) {
        self.state.install_progress(progress);
    }

    fn note_pending_at(&mut self, now: Instant, acknowledged_bytes: u64) {
        if self.state.episode.load(Ordering::Acquire) == 0 {
            let now_micros = self.pressure.elapsed_micros_at(now);
            self.state
                .pending_since_micros
                .store(now_micros, Ordering::Relaxed);
            self.state
                .ack_stalled_since_micros
                .store(now_micros, Ordering::Relaxed);
            self.state
                .acknowledged_bytes
                .store(acknowledged_bytes, Ordering::Relaxed);
            self.state
                .episode
                .store(self.pressure.next_episode(), Ordering::Release);
        } else {
            self.note_acknowledged_at(now, acknowledged_bytes);
        }
    }

    fn note_ready(&mut self) {
        self.state.episode.store(0, Ordering::Release);
    }

    fn note_acknowledged_at(&mut self, now: Instant, acknowledged_bytes: u64) {
        self.state
            .note_acknowledged_micros(self.pressure.elapsed_micros_at(now), acknowledged_bytes);
    }
}

impl Drop for TcpWritePressureWriter {
    fn drop(&mut self) {
        self.note_ready();
        self.pressure.remove_writer(self.state.writer);
    }
}

struct TcpWritePressureAdapter<W> {
    inner: W,
    pressure: TcpWritePressureWriter,
    progress: Option<quinn::SendStreamProgress>,
}

impl<W> TcpWritePressureAdapter<W> {
    #[cfg(test)]
    fn new(inner: W, pressure: TcpWritePressureWriter) -> Self {
        Self {
            inner,
            pressure,
            progress: None,
        }
    }

    fn new_with_progress(
        inner: W,
        mut pressure: TcpWritePressureWriter,
        progress: quinn::SendStreamProgress,
    ) -> Self {
        pressure.install_progress(progress.clone());
        Self {
            inner,
            pressure,
            progress: Some(progress),
        }
    }
}

impl<W: AsyncRead + Unpin> AsyncRead for TcpWritePressureAdapter<W> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<W: AsyncWrite + Unpin> AsyncWrite for TcpWritePressureAdapter<W> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let result = Pin::new(&mut self.inner).poll_write(cx, buf);
        if result.is_pending() {
            let acknowledged_bytes = self
                .progress
                .as_ref()
                .and_then(|progress| progress.sample().ok())
                .map(|progress| progress.acknowledged_bytes)
                .unwrap_or(0);
            self.pressure
                .note_pending_at(Instant::now(), acknowledged_bytes);
        } else {
            self.pressure.note_ready();
        }
        result
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.pressure.note_ready();
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct EndpointRecoveryConnectionSample {
    stable_id: usize,
    tx_bytes: u64,
    rx_bytes: u64,
    rtt: Duration,
    black_holes_detected: u64,
    current_socket_rx_rebind_generation: u64,
    tcp_write_pressures: Vec<TcpWritePressureSnapshot>,
    tcp_ordered_read_progress: Vec<TcpOrderedReadProgressSnapshot>,
}

#[derive(Debug)]
struct EndpointRecoveryInput {
    active_tcp: u64,
    udp_active: bool,
    last_udp_activity_secs: u64,
    current_socket_rx_rebind_generation: u64,
    connections: Vec<EndpointRecoveryConnectionSample>,
}

struct EndpointRecoveryConnectionHandle {
    stable_id: usize,
    pool_index: usize,
    pool_generation: u64,
    connection: Connection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EndpointRecoveryTrigger {
    NoRx,
    TcpWriteStall {
        stable_id: usize,
        writer: u64,
        stream: u64,
        episode: u64,
        acknowledged_bytes: u64,
        pending_for: Duration,
    },
    TcpPathDegraded {
        stable_id: usize,
        writer: u64,
        stream: u64,
        episode: u64,
        acknowledged_bytes: u64,
        pending_for: Duration,
        black_hole_anchor: u64,
        black_holes_current: u64,
    },
}

impl EndpointRecoveryTrigger {
    fn label(self) -> &'static str {
        match self {
            Self::NoRx => "no_rx",
            Self::TcpWriteStall { .. } => "tcp_write_stall",
            Self::TcpPathDegraded { .. } => "tcp_path_degraded",
        }
    }

    fn tcp_write_fields(self) -> (usize, u64, u64, u64, u64, u128) {
        match self {
            Self::NoRx => (0, 0, 0, 0, 0, 0),
            Self::TcpWriteStall {
                stable_id,
                writer,
                stream,
                episode,
                acknowledged_bytes,
                pending_for,
            } => (
                stable_id,
                writer,
                stream,
                episode,
                acknowledged_bytes,
                pending_for.as_millis(),
            ),
            Self::TcpPathDegraded {
                stable_id,
                writer,
                stream,
                episode,
                acknowledged_bytes,
                pending_for,
                ..
            } => (
                stable_id,
                writer,
                stream,
                episode,
                acknowledged_bytes,
                pending_for.as_millis(),
            ),
        }
    }

    fn tcp_read_fields(self) -> (usize, u64, u64, u64, u64, u64, u64, usize, u64, u64) {
        (0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EndpointRecoveryAction {
    None,
    ResetConnectionPath {
        generation: u64,
        trigger: EndpointRecoveryTrigger,
        stall_bound: Duration,
        max_rtt: Duration,
    },
    Rebind {
        generation: u64,
        trigger: EndpointRecoveryTrigger,
        stalled_for: Duration,
        stall_bound: Duration,
        tx_bytes_since_rx: u64,
        max_rtt: Duration,
    },
    Recovered {
        generation: u64,
        socket_generation: u64,
        recovery_time: Duration,
        connection_count: usize,
    },
}

#[derive(Debug, Clone, Copy)]
struct EndpointRecoveryCounters {
    tx_bytes: u64,
    rx_bytes: u64,
}

#[derive(Debug, Default)]
struct EndpointRecoveryState {
    connections: HashMap<usize, EndpointRecoveryCounters>,
    armed_at: Option<Instant>,
    tx_bytes_since_rx: u64,
    rebind_issued: bool,
    rebind_generation: u64,
    rebound_at: Option<Instant>,
    expected_socket_generation: Option<u64>,
    pending_recovery_connections: HashSet<usize>,
    expected_recovery_connection_count: usize,
    covered_tcp_write_episodes: HashMap<(usize, u64), u64>,
    path_black_hole_anchors: HashMap<usize, u64>,
    path_reset_consumed: HashSet<usize>,
    path_reset_generation: u64,
    observed_udp_activity_secs: u64,
    udp_demand_since_rx: bool,
}

impl EndpointRecoveryState {
    fn cover_tcp_write_episodes(&mut self, input: &EndpointRecoveryInput) {
        for sample in &input.connections {
            for pressure in &sample.tcp_write_pressures {
                self.covered_tcp_write_episodes
                    .insert((sample.stable_id, pressure.writer), pressure.episode);
            }
        }
    }

    fn eligible_tcp_write_stall(
        &self,
        input: &EndpointRecoveryInput,
        stall_bound: Duration,
    ) -> Option<(usize, TcpWritePressureSnapshot)> {
        input
            .connections
            .iter()
            .flat_map(|sample| {
                sample
                    .tcp_write_pressures
                    .iter()
                    .copied()
                    .map(move |pressure| (sample.stable_id, pressure))
            })
            .filter(|(stable_id, pressure)| {
                pressure.pending_for >= stall_bound
                    && pressure.ack_stalled_for >= stall_bound
                    && self
                        .covered_tcp_write_episodes
                        .get(&(*stable_id, pressure.writer))
                        != Some(&pressure.episode)
            })
            .max_by_key(|(_, pressure)| (pressure.ack_stalled_for, pressure.pending_for))
    }

    fn refresh_path_recovery_ownership(&mut self, input: &EndpointRecoveryInput) {
        self.path_black_hole_anchors.retain(|stable_id, _| {
            input
                .connections
                .iter()
                .any(|sample| sample.stable_id == *stable_id)
        });
        self.path_reset_consumed.retain(|stable_id| {
            input
                .connections
                .iter()
                .any(|sample| sample.stable_id == *stable_id)
        });

        for sample in &input.connections {
            let has_pending_writer = !sample.tcp_write_pressures.is_empty();
            self.path_black_hole_anchors
                .entry(sample.stable_id)
                .and_modify(|anchor| {
                    if sample.black_holes_detected < *anchor || !has_pending_writer {
                        *anchor = sample.black_holes_detected;
                    }
                })
                .or_insert(sample.black_holes_detected);
        }
    }

    fn eligible_tcp_path_degradation(
        &self,
        input: &EndpointRecoveryInput,
        stall_bound: Duration,
    ) -> Option<(usize, TcpWritePressureSnapshot, u64, u64)> {
        input
            .connections
            .iter()
            .filter_map(|sample| {
                let anchor = *self.path_black_hole_anchors.get(&sample.stable_id)?;
                if self.path_reset_consumed.contains(&sample.stable_id)
                    || sample.black_holes_detected <= anchor
                {
                    return None;
                }
                sample
                    .tcp_write_pressures
                    .iter()
                    .copied()
                    .filter(|pressure| pressure.pending_for >= stall_bound)
                    .max_by_key(|pressure| pressure.pending_for)
                    .map(|pressure| {
                        (
                            sample.stable_id,
                            pressure,
                            anchor,
                            sample.black_holes_detected,
                        )
                    })
            })
            .max_by_key(|(stable_id, pressure, anchor, current)| {
                (
                    pressure.pending_for,
                    current.saturating_sub(*anchor),
                    *stable_id,
                )
            })
    }

    fn begin_path_reset(
        &mut self,
        stable_id: usize,
        pressure: TcpWritePressureSnapshot,
        black_hole_anchor: u64,
        black_holes_current: u64,
        stall_bound: Duration,
        max_rtt: Duration,
    ) -> EndpointRecoveryAction {
        self.path_reset_consumed.insert(stable_id);
        self.path_reset_generation = self.path_reset_generation.saturating_add(1);
        EndpointRecoveryAction::ResetConnectionPath {
            generation: self.path_reset_generation,
            trigger: EndpointRecoveryTrigger::TcpPathDegraded {
                stable_id,
                writer: pressure.writer,
                stream: pressure.stream,
                episode: pressure.episode,
                acknowledged_bytes: pressure.acknowledged_bytes,
                pending_for: pressure.pending_for,
                black_hole_anchor,
                black_holes_current,
            },
            stall_bound,
            max_rtt,
        }
    }

    fn begin_rebind(
        &mut self,
        input: &EndpointRecoveryInput,
        trigger: EndpointRecoveryTrigger,
        stalled_for: Duration,
        stall_bound: Duration,
        max_rtt: Duration,
    ) -> EndpointRecoveryAction {
        self.cover_tcp_write_episodes(input);
        self.rebind_generation = self.rebind_generation.saturating_add(1);
        self.rebind_issued = true;
        EndpointRecoveryAction::Rebind {
            generation: self.rebind_generation,
            trigger,
            stalled_for,
            stall_bound,
            tx_bytes_since_rx: self.tx_bytes_since_rx,
            max_rtt,
        }
    }

    fn observe(&mut self, now: Instant, input: &EndpointRecoveryInput) -> EndpointRecoveryAction {
        if input.last_udp_activity_secs < self.observed_udp_activity_secs {
            self.observed_udp_activity_secs = input.last_udp_activity_secs;
            self.clear_episode();
        } else if input.last_udp_activity_secs > self.observed_udp_activity_secs {
            self.observed_udp_activity_secs = input.last_udp_activity_secs;
            self.udp_demand_since_rx = true;
        }
        let mut next = HashMap::with_capacity(input.connections.len());
        let mut tx_progress = 0u64;
        let mut rx_progress = false;
        let had_connections = !self.connections.is_empty();
        let mut connection_replaced = false;
        let mut max_rtt = Duration::ZERO;

        for sample in &input.connections {
            max_rtt = max_rtt.max(sample.rtt);
            if let Some(previous) = self.connections.get(&sample.stable_id) {
                tx_progress =
                    tx_progress.saturating_add(sample.tx_bytes.saturating_sub(previous.tx_bytes));
                rx_progress |= sample.rx_bytes > previous.rx_bytes;
            } else if had_connections {
                connection_replaced = true;
            }
            next.insert(
                sample.stable_id,
                EndpointRecoveryCounters {
                    tx_bytes: sample.tx_bytes,
                    rx_bytes: sample.rx_bytes,
                },
            );
        }
        self.connections = next;
        self.refresh_path_recovery_ownership(input);
        self.covered_tcp_write_episodes
            .retain(|(stable_id, writer), episode| {
                input.connections.iter().any(|sample| {
                    sample.stable_id == *stable_id
                        && sample.tcp_write_pressures.iter().any(|pressure| {
                            pressure.writer == *writer && pressure.episode == *episode
                        })
                })
            });

        if let Some(expected_generation) = self.expected_socket_generation {
            for sample in &input.connections {
                if sample.current_socket_rx_rebind_generation >= expected_generation {
                    self.pending_recovery_connections.remove(&sample.stable_id);
                }
            }
        }

        if self.expected_socket_generation.is_some_and(|generation| {
            input.current_socket_rx_rebind_generation >= generation
                && self.pending_recovery_connections.is_empty()
        }) {
            let socket_generation = self.expected_socket_generation.unwrap_or(0);
            let recovered = self
                .rebound_at
                .map(|rebound_at| EndpointRecoveryAction::Recovered {
                    generation: self.rebind_generation,
                    socket_generation,
                    recovery_time: now.saturating_duration_since(rebound_at),
                    connection_count: self.expected_recovery_connection_count,
                });
            self.clear_episode();
            return recovered.unwrap_or(EndpointRecoveryAction::None);
        }

        if connection_replaced {
            if self.expected_socket_generation.is_some() {
                return EndpointRecoveryAction::None;
            }
            self.clear_episode();
            return EndpointRecoveryAction::None;
        }

        let active_workload = input.active_tcp > 0 || input.udp_active;
        if !active_workload || input.connections.is_empty() {
            self.clear_episode();
            return EndpointRecoveryAction::None;
        }

        let stall_bound = endpoint_recovery_stall_bound(max_rtt);
        if let Some((stable_id, pressure)) = self.eligible_tcp_write_stall(input, stall_bound)
            && !self.rebind_issued
        {
            return self.begin_rebind(
                input,
                EndpointRecoveryTrigger::TcpWriteStall {
                    stable_id,
                    writer: pressure.writer,
                    stream: pressure.stream,
                    episode: pressure.episode,
                    acknowledged_bytes: pressure.acknowledged_bytes,
                    pending_for: pressure.pending_for,
                },
                pressure.ack_stalled_for,
                stall_bound,
                max_rtt,
            );
        }

        if let Some((stable_id, pressure, anchor, current)) =
            self.eligible_tcp_path_degradation(input, stall_bound)
            && !self.rebind_issued
            && self.expected_socket_generation.is_none()
        {
            return self.begin_path_reset(
                stable_id,
                pressure,
                anchor,
                current,
                stall_bound,
                max_rtt,
            );
        }

        if !input.udp_active || !self.udp_demand_since_rx {
            if self.expected_socket_generation.is_none() {
                self.clear_episode();
            }
            return EndpointRecoveryAction::None;
        }

        if rx_progress {
            if self.expected_socket_generation.is_some() {
                return EndpointRecoveryAction::None;
            }
            let recovered =
                self.rebound_at
                    .take()
                    .map(|rebound_at| EndpointRecoveryAction::Recovered {
                        generation: self.rebind_generation,
                        socket_generation: 0,
                        recovery_time: now.saturating_duration_since(rebound_at),
                        connection_count: self.expected_recovery_connection_count,
                    });
            self.clear_episode();
            return recovered.unwrap_or(EndpointRecoveryAction::None);
        }

        if tx_progress > 0 {
            self.armed_at.get_or_insert(now);
            self.tx_bytes_since_rx = self.tx_bytes_since_rx.saturating_add(tx_progress);
        }

        let Some(armed_at) = self.armed_at else {
            return EndpointRecoveryAction::None;
        };
        if self.rebind_issued {
            return EndpointRecoveryAction::None;
        }

        let stalled_for = now.saturating_duration_since(armed_at);
        if stalled_for < stall_bound {
            return EndpointRecoveryAction::None;
        }

        self.begin_rebind(
            input,
            EndpointRecoveryTrigger::NoRx,
            stalled_for,
            stall_bound,
            max_rtt,
        )
    }

    fn note_rebind_succeeded(&mut self, now: Instant, socket_generation: u64) {
        if self.rebind_issued {
            self.rebound_at = Some(now);
            self.expected_socket_generation = Some(socket_generation);
            self.pending_recovery_connections = self.connections.keys().copied().collect();
            self.expected_recovery_connection_count = self.pending_recovery_connections.len();
        }
    }

    fn clear_episode(&mut self) {
        self.armed_at = None;
        self.tx_bytes_since_rx = 0;
        self.rebind_issued = false;
        self.rebound_at = None;
        self.expected_socket_generation = None;
        self.pending_recovery_connections.clear();
        self.expected_recovery_connection_count = 0;
        self.udp_demand_since_rx = false;
    }
}

/// 刀9（真出口 acceptance 修）：单条 TUIC TCP open（open_bi + write Connect）超时。黑洞连接上 write_all
/// 因 send 窗口满+无 ACK 会无限挂（连接尚未判死），5s 封顶让 failover 慢路收到「连接活但 open 失败」
/// 信号；正常 open 是本地操作远小于 5s，不误伤。**与 `TUIC_RECONNECT_TIMEOUT` 各自独立**（恰好都 5s，
/// 非耦合）；亦**不同于** spec §2.6 的「open_tcp 10s」——那指 **REALITY** open 的 H2 止血超时（reality_upstream.rs），
/// 此处是 TUIC open 的黑洞探测超时（acceptance 新加，spec 当时未有）。
const TUIC_OPEN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
/// An idle auxiliary slot needs fresh transport evidence before reuse. Elapsed time requests a
/// bounded heartbeat/ACK probe; it never grants permission to destroy a healthy connection.
const TUIC_TCP_POOL_STALE_RECONNECT_SECS: u64 = 10;
const TUIC_TCP_POOL_LIVENESS_PROBE_TIMEOUT: Duration = Duration::from_secs(1);

trait TcpPoolTransport: Clone {
    fn tcp_pool_stable_id(&self) -> usize;
}

impl TcpPoolTransport for Connection {
    fn tcp_pool_stable_id(&self) -> usize {
        self.stable_id()
    }
}

#[cfg(test)]
impl TcpPoolTransport for u64 {
    fn tcp_pool_stable_id(&self) -> usize {
        usize::try_from(*self).unwrap_or(usize::MAX)
    }
}

struct TcpPoolGeneration<T> {
    transport: T,
    activity: Arc<TcpPoolGenerationActivity>,
    write_pressure: Arc<TcpWritePressure>,
    read_pressure: Arc<TcpOrderedReadPressure>,
    open_state: Arc<TcpPoolOpenState>,
    auth_attempts: u64,
}

impl<T: TcpPoolTransport> TcpPoolGeneration<T> {
    fn new(transport: T, generation: u64, auth_attempts: u64, clock: Instant) -> Self {
        Self {
            transport,
            activity: Arc::new(TcpPoolGenerationActivity::new()),
            write_pressure: Arc::new(TcpWritePressure::new(clock)),
            read_pressure: Arc::new(TcpOrderedReadPressure::new()),
            open_state: Arc::new(TcpPoolOpenState::new_at_generation(generation)),
            auth_attempts,
        }
    }

    fn identity(&self) -> TcpPoolTransportIdentity {
        TcpPoolTransportIdentity::new(
            self.transport.tcp_pool_stable_id(),
            self.open_state.generation(),
        )
    }
}

struct TcpPoolDrainingGeneration<T> {
    generation: TcpPoolGeneration<T>,
    started_at: Instant,
}

struct TcpPoolGenerationSlotState<T> {
    current: TcpPoolGeneration<T>,
    draining: Option<TcpPoolDrainingGeneration<T>>,
}

struct TcpPoolGenerationSlot<T> {
    index: usize,
    state: Mutex<TcpPoolGenerationSlotState<T>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TcpPoolGenerationInstallError {
    Primary,
    PredecessorDraining,
    StalePredecessor,
    InvalidSuccessor,
}

struct TcpPoolGenerationInstall {
    predecessor_identity: TcpPoolTransportIdentity,
    successor_identity: TcpPoolTransportIdentity,
    predecessor_activity: Arc<TcpPoolGenerationActivity>,
}

struct TcpPoolDrainingPermit {
    owned: Arc<AtomicBool>,
    transferred: bool,
}

impl TcpPoolDrainingPermit {
    fn try_acquire(owned: Arc<AtomicBool>) -> Option<Self> {
        owned
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .ok()?;
        Some(Self {
            owned,
            transferred: false,
        })
    }

    fn transfer(mut self) -> Arc<AtomicBool> {
        self.transferred = true;
        self.owned.clone()
    }
}

impl Drop for TcpPoolDrainingPermit {
    fn drop(&mut self) {
        if !self.transferred {
            self.owned.store(false, Ordering::Release);
        }
    }
}

impl<T: TcpPoolTransport> TcpPoolGenerationSlot<T> {
    fn new(
        index: usize,
        transport: T,
        generation: u64,
        auth_attempts: u64,
        clock: Instant,
    ) -> Self {
        Self {
            index,
            state: Mutex::new(TcpPoolGenerationSlotState {
                current: TcpPoolGeneration::new(transport, generation, auth_attempts, clock),
                draining: None,
            }),
        }
    }

    fn replacement_available(&self) -> bool {
        if self.index == 0 {
            return false;
        }
        self.state
            .try_lock()
            .map(|state| state.draining.is_none())
            .unwrap_or(false)
    }

    async fn current_activity(&self) -> Arc<TcpPoolGenerationActivity> {
        self.state.lock().await.current.activity.clone()
    }

    #[cfg(test)]
    async fn current_transport(&self) -> T {
        self.state.lock().await.current.transport.clone()
    }

    #[cfg(test)]
    async fn draining_transport(&self) -> Option<T> {
        self.state
            .lock()
            .await
            .draining
            .as_ref()
            .map(|draining| draining.generation.transport.clone())
    }

    #[cfg(test)]
    async fn install_successor(
        &self,
        expected_identity: TcpPoolTransportIdentity,
        expected_activity: &Arc<TcpPoolGenerationActivity>,
        successor: TcpPoolGeneration<T>,
        started_at: Instant,
    ) -> Result<TcpPoolGenerationInstall, TcpPoolGenerationInstallError> {
        self.install_successor_with(
            expected_identity,
            expected_activity,
            successor,
            started_at,
            |_, _| true,
        )
        .await
    }

    async fn install_successor_with(
        &self,
        expected_identity: TcpPoolTransportIdentity,
        expected_activity: &Arc<TcpPoolGenerationActivity>,
        successor: TcpPoolGeneration<T>,
        started_at: Instant,
        install_activity: impl FnOnce(
            &Arc<TcpPoolGenerationActivity>,
            &Arc<TcpPoolGenerationActivity>,
        ) -> bool,
    ) -> Result<TcpPoolGenerationInstall, TcpPoolGenerationInstallError> {
        if self.index == 0 {
            return Err(TcpPoolGenerationInstallError::Primary);
        }
        let mut state = self.state.lock().await;
        if state.draining.is_some() {
            return Err(TcpPoolGenerationInstallError::PredecessorDraining);
        }
        if state.current.identity() != expected_identity
            || !Arc::ptr_eq(&state.current.activity, expected_activity)
        {
            return Err(TcpPoolGenerationInstallError::StalePredecessor);
        }
        let successor_identity = successor.identity();
        if successor_identity.stable_id == expected_identity.stable_id
            || expected_identity.generation.checked_add(1) != Some(successor_identity.generation)
        {
            return Err(TcpPoolGenerationInstallError::InvalidSuccessor);
        }
        if !install_activity(&state.current.activity, &successor.activity) {
            return Err(TcpPoolGenerationInstallError::StalePredecessor);
        }
        let predecessor = std::mem::replace(&mut state.current, successor);
        let predecessor_identity = predecessor.identity();
        let predecessor_activity = predecessor.activity.clone();
        state.draining = Some(TcpPoolDrainingGeneration {
            generation: predecessor,
            started_at,
        });
        Ok(TcpPoolGenerationInstall {
            predecessor_identity,
            successor_identity,
            predecessor_activity,
        })
    }

    async fn take_drained(
        &self,
        expected_identity: TcpPoolTransportIdentity,
    ) -> Option<TcpPoolDrainingGeneration<T>> {
        let mut state = self.state.lock().await;
        let draining = state.draining.as_ref()?;
        if draining.generation.identity() != expected_identity
            || draining.generation.activity.active() != 0
        {
            return None;
        }
        state.draining.take()
    }
}

struct TcpPoolConnectionSelection {
    conn_index: usize,
    conn: Connection,
    lease: TcpPoolSlotLease,
    active_before: u64,
    path_service: TcpPoolPathService,
    path_service_tiebreak: bool,
    qualification: TcpPoolForwardQualification,
    qualification_anchor: Option<u64>,
    black_holes_current: Option<u64>,
    qualification_override: bool,
    all_degraded_fallback: bool,
    candidates: Vec<TcpPoolAdmissionCandidate>,
    generation: u64,
    last_success_age_secs: Option<u64>,
    probe_result: &'static str,
    reconnect_reason: Option<&'static str>,
    open_state: Arc<TcpPoolOpenState>,
    write_pressure: Arc<TcpWritePressure>,
    read_pressure: Arc<TcpOrderedReadPressure>,
    startup_auth_attempts: u64,
    replacement_installed: bool,
}

/// TUIC 客户端上游：持有到 sing-box 的 QUIC 连接，每条 TCP 开一条 `Connect` 双向流。
/// 中文要点：连接断了按需重连+重认证(13a 最小实现;迁移/0-RTT 调优在 13c)。
pub struct TuicUpstream {
    endpoint: Endpoint,
    udp_send_service_policy: quic::QuicUdpSendServicePolicy,
    udp_send_service_stats: Option<quic::QuicUdpSendServiceStats>,
    endpoint_recovery_started: AtomicBool,
    server: SocketAddr,
    sni: String,
    uuid: [u8; 16],
    password: String,
    /// Fixed logical slots own current and optional draining generations as one lifecycle unit.
    /// Slot 0 remains the single primary source for UDP relay, heartbeat, and health probes.
    tcp_pool_slots: Vec<Arc<TcpPoolGenerationSlot<Connection>>>,
    /// The frozen two-slot pool permits at most one drain-only predecessor globally.
    tcp_pool_draining_predecessor: Arc<AtomicBool>,
    /// TCP-open admission owns active/opening counts plus busy-epoch forward qualification. Stale
    /// reconnect is only safe when a slot is idle; closing an active QUIC connection would cut
    /// every stream multiplexed on that slot.
    tcp_pool_admission: TcpPoolAdmission,
    /// 上行 UDP datagram 丢弃计数（连接不可用 / stream 兜底也失败）。可观测性，不影响 UDP 语义。
    udp_drops: AtomicU64,
    /// 上行走 uni-stream 兜底（超 datagram 上限）的次数。可观测性：判断 MTU 调优是否够、兜底是否热。
    udp_stream_fallbacks: AtomicU64,
    /// 上次 UDP 上行的秒数（`clock` 起点，0=从未）。`send_udp` 写、心跳读，决定是否按需保活。
    last_udp_activity: AtomicU64,
    /// 单调时钟：`send_udp`(写活跃时刻)与驱动任务(读 now)**同源**，避免双时钟漂移。
    clock: std::time::Instant,
    /// 重连是否尝试 0-RTT（来自 config；失败自愈回落 1-RTT）。
    zero_rtt: bool,
    /// QUIC 连接级 stats 日志周期；`None` 表示关闭。
    quic_stats_secs: Option<u64>,
    /// QUIC stats task 停止信号。`Drop` 时触发，避免诊断 task 持有 `Connection` clone 延长连接生命周期。
    quic_stats_stop: Option<watch::Sender<bool>>,
    /// UDP relay mode（刀3.5；来自 config）：`Quic` → 所有上行包走 uni-stream（首包即触发 server
    /// 下行镜像 stream，摆脱 datagram 天花板）；`Native` → datagram 主 + 超限 stream 兜底（刀3 行为）。
    udp_relay_mode: UdpRelayMode,
    /// 刀11：进程级数据面可观测性句柄（与 run_event_loop 共享同一 `Arc<Metrics>`）。`start_udp` spawn 的
    /// 下行/统计 task 经 `me.metrics` 写**下行** drop（`udp_drops_down`）+ datagram 背压上升沿
    /// （`datagram_pressure_events`）。上行 `udp_drops`/`udp_stream_fallbacks` 仍是上方既有字段（零回归）。
    metrics: Arc<Metrics>,
}

impl TuicUpstream {
    /// 建连 + 发 Authenticate（token 经 keying-material 导出，字节级对齐 sing-box）。
    pub async fn connect(
        cfg: &TuicClientConfig,
        metrics: Arc<Metrics>,
    ) -> Result<Self, ClientError> {
        // 刀3.5：从 config 解析 CC（未知值回落 Cubic + 告警，失败自愈不致命）。
        let (cc, fell_back) = quic::parse_cc(&cfg.congestion_control);
        if fell_back {
            println!(
                "⚠️ TUIC 未知 congestion_control={:?}，回落 Cubic（quinn 默认）",
                cfg.congestion_control
            );
        }
        // 刀3.5：解析 UDP relay mode（未知值回落 Native + 告警，与 CC 对称）。
        let udp_relay_mode = UdpRelayMode::parse(&cfg.udp_relay_mode).unwrap_or_else(|| {
            println!(
                "⚠️ TUIC 未知 udp_relay_mode={:?}，回落 native（datagram 主 + 超限 stream 兜底）",
                cfg.udp_relay_mode
            );
            UdpRelayMode::Native
        });
        let (mtu_policy, mtu_fell_back) = quic::parse_mtu_policy(Some(&cfg.mtu_policy));
        if mtu_fell_back {
            println!(
                "⚠️ TUIC 未知 mtu_policy={:?}，回落 default（1280 + PLPMTUD）",
                cfg.mtu_policy
            );
        }
        let (gso_policy, gso_fell_back) = quic::parse_gso_policy(Some(&cfg.gso_policy));
        if gso_fell_back {
            println!(
                "⚠️ TUIC 未知 gso_policy={:?}，回落 enabled（Quinn 生产默认）",
                cfg.gso_policy
            );
        }
        let (udp_send_service, udp_send_service_fell_back) =
            quic::parse_udp_send_service_policy(Some(&cfg.udp_send_service));
        if udp_send_service_fell_back {
            println!(
                "⚠️ TUIC 未知 udp_send_service={:?}，回落 quinn（生产默认）",
                cfg.udp_send_service
            );
        }
        let (pacing_policy, pacing_policy_fell_back) =
            quic::parse_pacing_policy(Some(&cfg.pacing_policy));
        if pacing_policy_fell_back {
            println!(
                "⚠️ TUIC 未知 pacing_policy={:?}，回落 quinn（生产默认）",
                cfg.pacing_policy
            );
        }
        quic::validate_quic_send_policies(pacing_policy, udp_send_service)
            .map_err(ClientError::InvalidTarget)?;
        let qcfg = quic::client_quic_config_alpn(
            &cfg.ca_path,
            vec![cfg.alpn.as_bytes().to_vec()],
            cc,
            mtu_policy,
            gso_policy,
            pacing_policy,
        )
        .map_err(ClientError::InvalidTarget)?;
        let (endpoint, udp_send_service_stats) =
            quic::client_endpoint_with_udp_send_service(qcfg, udp_send_service, pacing_policy)
                .map_err(ClientError::InvalidTarget)?;
        let conn = Self::handshake(
            &endpoint,
            cfg.server,
            &cfg.sni,
            &cfg.uuid,
            &cfg.password,
            cfg.zero_rtt,
        )
        .await?;
        // 刀3：记录初始 datagram 发送上限（acceptance 据此读真实 datagram 天花板；PLPMTUD 探完会更大）。
        // 超此上限的上行包走 uni-stream 兜底；下行大包由 server 分片、本端重组。
        println!(
            "📏 TUIC datagram 初始上限 = {:?} 字节（超此走 uni-stream 兜底；PLPMTUD 将继续上探）",
            conn.max_datagram_size()
        );
        // 刀3.5：打实际生效的 CC + relay mode，供 acceptance 确认 BBR/quic 真装上（A/B 归因）。
        println!(
            "🧭 TUIC 拥塞控制器={cc:?} | UDP relay mode={udp_relay_mode:?} | QUIC MTU policy={} | QUIC GSO policy={} | QUIC UDP send service={} | QUIC pacing policy={}",
            mtu_policy.label(),
            gso_policy.label(),
            udp_send_service.label(),
            pacing_policy.label(),
        );
        println!(
            "🪟 QUIC flow windows: bidi={} uni={} stream_rx={}B conn_rx={}B send={}B",
            quic::QUIC_MAX_CONCURRENT_BIDI_STREAMS,
            quic::QUIC_MAX_CONCURRENT_UNI_STREAMS,
            quic::quic_stream_receive_window_bytes(mtu_policy),
            quic::quic_receive_window_bytes(mtu_policy),
            quic::QUIC_SEND_WINDOW_BYTES
        );
        if let Some(stats) = endpoint.endpoint_pacing_snapshot() {
            println!("{}", format_endpoint_pacing_stats_line(stats));
        }
        let tcp_pool = cfg.tcp_pool.clamp(MIN_TUIC_TCP_POOL, MAX_TUIC_TCP_POOL);
        let mut conns = Vec::with_capacity(tcp_pool);
        let quic_stats_stop = cfg.quic_stats_secs.map(|_| watch::channel(false).0);
        if let Some(secs) = cfg.quic_stats_secs {
            println!("🔬 TUIC QUIC stats 已启用：每 {secs}s 打印连接级 flow/congestion 指标");
            if let Some(stop) = &quic_stats_stop {
                spawn_quic_stats_logger(conn.clone(), 0, secs, stop.subscribe());
                if endpoint.endpoint_pacing_snapshot().is_some() {
                    spawn_endpoint_pacing_stats_logger(endpoint.clone(), secs, stop.subscribe());
                }
                if let Some(stats) = udp_send_service_stats.clone() {
                    spawn_udp_send_service_stats_logger(stats, secs, stop.subscribe());
                }
            }
        }
        conns.push(conn);
        let mut tcp_startup_auth_attempts = Vec::with_capacity(tcp_pool);
        tcp_startup_auth_attempts.push(1_u64);
        for index in 1..tcp_pool {
            let (extra, auth_attempts) = match Self::handshake_aux_with_retries(
                &endpoint,
                cfg.server,
                &cfg.sni,
                &cfg.uuid,
                &cfg.password,
                cfg.zero_rtt,
                index,
            )
            .await
            {
                Ok(conn) => conn,
                Err(e) => match tcp_pool_aux_failure_action(conns.len()) {
                    TcpPoolAuxFailureAction::FailStartup => return Err(e),
                    TcpPoolAuxFailureAction::ContinueWithEstablished => {
                        println!(
                            "⚠️ TUIC TCP connection pool auxiliary slot {index} failed to authenticate/connect; continuing with {} established connection(s): {e:?}",
                            conns.len()
                        );
                        break;
                    }
                },
            };
            tcp_startup_auth_attempts.push(auth_attempts as u64);
            if let Some(secs) = cfg.quic_stats_secs
                && let Some(stop) = &quic_stats_stop
            {
                spawn_quic_stats_logger(extra.clone(), index, secs, stop.subscribe());
            }
            conns.push(extra);
        }
        let tcp_pool = conns.len();
        debug_assert_eq!(tcp_startup_auth_attempts.len(), tcp_pool);
        if tcp_pool > 1 {
            println!(
                "🧵 TUIC TCP connection pool={tcp_pool}（UDP/health 仍走 primary connection）"
            );
        }
        let clock = std::time::Instant::now();
        let tcp_pool_slots = conns
            .into_iter()
            .zip(tcp_startup_auth_attempts)
            .enumerate()
            .map(|(index, (conn, auth_attempts))| {
                Arc::new(TcpPoolGenerationSlot::new(
                    index,
                    conn,
                    1,
                    auth_attempts,
                    clock,
                ))
            })
            .collect::<Vec<_>>();
        let mut current_generation_activity = Vec::with_capacity(tcp_pool_slots.len());
        for slot in &tcp_pool_slots {
            current_generation_activity.push(slot.current_activity().await);
        }
        let tcp_pool_admission =
            TcpPoolAdmission::with_current_generation_activity(current_generation_activity);
        Ok(Self {
            endpoint,
            udp_send_service_policy: udp_send_service,
            udp_send_service_stats,
            endpoint_recovery_started: AtomicBool::new(false),
            server: cfg.server,
            sni: cfg.sni.clone(),
            uuid: cfg.uuid,
            password: cfg.password.clone(),
            tcp_pool_slots,
            tcp_pool_draining_predecessor: Arc::new(AtomicBool::new(false)),
            tcp_pool_admission,
            udp_drops: AtomicU64::new(0),
            udp_stream_fallbacks: AtomicU64::new(0),
            last_udp_activity: AtomicU64::new(0),
            clock,
            zero_rtt: cfg.zero_rtt,
            quic_stats_secs: cfg.quic_stats_secs,
            quic_stats_stop,
            udp_relay_mode,
            metrics,
        })
    }

    /// 当前 UDP relay mode（可观测/测试）。
    pub fn udp_relay_mode(&self) -> UdpRelayMode {
        self.udp_relay_mode
    }

    /// 建连 + 认证。`zero_rtt` 时先试 0-RTT（early data）；任何原因失败（无 ticket / 服务端不支持 /
    /// early-exporter 认证不齐）都**自愈回落 1-RTT**，绝不卡死重连循环。
    /// 中文要点：首连必无 ticket → `into_0rtt` 必失败 → 走 1-RTT（与 13a 行为一致，零回归）。
    async fn handshake(
        endpoint: &Endpoint,
        server: SocketAddr,
        sni: &str,
        uuid: &[u8; 16],
        password: &str,
        zero_rtt: bool,
    ) -> Result<Connection, ClientError> {
        if zero_rtt && let Some(conn) = Self::try_0rtt(endpoint, server, sni, uuid, password).await
        {
            println!("⚡ TUIC 0-RTT 重连成功（early data）");
            return Ok(conn);
        }
        // 关了 0-RTT，或 0-RTT 不可用（无 ticket/不支持/认证不齐）→ 回落 1-RTT。
        Self::handshake_1rtt(endpoint, server, sni, uuid, password).await
    }

    /// 尝试 0-RTT 握手 + early-data 认证；不可用返回 None（交由调用方回落 1-RTT）。
    async fn try_0rtt(
        endpoint: &Endpoint,
        server: SocketAddr,
        sni: &str,
        uuid: &[u8; 16],
        password: &str,
    ) -> Option<Connection> {
        let connecting = endpoint.connect(server, sni).ok()?;
        // into_0rtt 的 Err 返回原 Connecting（非 Error 类型），无 ticket/不支持时走这里。
        let (conn, _accepted) = match connecting.into_0rtt() {
            Ok(pair) => pair,
            Err(_connecting) => return None,
        };
        // early-exporter 认证：若与 sing-box 不齐则失败 → 记一行(便于 e2e 诊断)并 None 回落（不卡死）。
        if let Err(e) = Self::authenticate(&conn, uuid, password).await {
            println!(
                "⚠️ TUIC 0-RTT 认证失败(可能 early-exporter 与 sing-box 不齐)，回落 1-RTT: {e:?}"
            );
            return None;
        }
        Some(conn)
    }

    /// 普通 1-RTT 握手 + 认证（13a 行为）。
    async fn handshake_1rtt(
        endpoint: &Endpoint,
        server: SocketAddr,
        sni: &str,
        uuid: &[u8; 16],
        password: &str,
    ) -> Result<Connection, ClientError> {
        let conn = endpoint
            .connect(server, sni)
            .map_err(|e| io_err("tuic connect", e))?
            .await
            .map_err(|e| io_err("tuic handshake", e))?;
        Self::authenticate(&conn, uuid, password).await?;
        Ok(conn)
    }

    async fn handshake_aux_with_retries(
        endpoint: &Endpoint,
        server: SocketAddr,
        sni: &str,
        uuid: &[u8; 16],
        password: &str,
        zero_rtt: bool,
        index: usize,
    ) -> Result<(Connection, usize), ClientError> {
        for attempt in 0..TUIC_TCP_POOL_AUX_CONNECT_ATTEMPTS {
            match Self::handshake(endpoint, server, sni, uuid, password, zero_rtt).await {
                Ok(conn) => {
                    if attempt > 0 {
                        println!(
                            "✅ TUIC TCP connection pool auxiliary slot {index} recovered after {} attempt(s)",
                            attempt + 1
                        );
                    }
                    return Ok((conn, attempt + 1));
                }
                Err(e) => {
                    let Some(delay) = tcp_pool_aux_retry_delay(attempt) else {
                        return Err(e);
                    };
                    println!(
                        "⚠️ TUIC TCP connection pool auxiliary slot {index} failed to authenticate/connect on attempt {}/{}; retrying in {}ms: {e:?}",
                        attempt + 1,
                        TUIC_TCP_POOL_AUX_CONNECT_ATTEMPTS,
                        delay.as_millis()
                    );
                    tokio::time::sleep(delay).await;
                }
            }
        }
        unreachable!("auxiliary TCP pool retry loop must return on success or final error")
    }

    /// 在已建立(0-RTT 或 1-RTT)的连接上发 TUIC Authenticate（单向流）。
    /// token = export_keying_material(out=32, label=UUID(16), context=password) —— 字节级对齐 sing-box。
    async fn authenticate(
        conn: &Connection,
        uuid: &[u8; 16],
        password: &str,
    ) -> Result<(), ClientError> {
        let mut token = [0u8; 32];
        conn.export_keying_material(&mut token, uuid, password.as_bytes())
            .map_err(|_| ClientError::InvalidTarget("tuic keying-material export failed".into()))?;
        let mut uni = conn
            .open_uni()
            .await
            .map_err(|e| io_err("tuic open_uni", e))?;
        uni.write_all(&encode_authenticate(uuid, &token))
            .await
            .map_err(|e| io_err("tuic auth write", e))?;
        uni.finish().map_err(|e| io_err("tuic auth finish", e))?;
        Ok(())
    }

    /// 取当前主连接的克隆；若已关闭则就地重连+重认证（13a 逻辑，UDP/health 共用）。
    /// 中文要点：主连接(index 0)是 UDP/health 的单一事实源；TCP pool 通过 `live_tcp_conn` 分散到其它连接。
    /// 刀9（spec §2.3）：重连握手封 **5s 超时**——黑洞/不可达 server 的 QUIC 握手默认受 idle(15s, quic.rs) 约束、
    /// 可阻塞数十秒；5s 封顶让「连接死 + 重连失败」这个 failover 快路强信号**快速暴露**（is_dead 仍为
    /// true，调用方 record_tuic_failure(dead) 即切 REALITY），同时也避免纯 TUIC open 任务久挂。
    async fn live_conn(&self) -> Result<Connection, ClientError> {
        self.live_conn_at(0).await
    }

    /// 取一条 TCP pool 连接的克隆；连接自身已关闭则只重连该槽位。
    async fn live_conn_at(&self, index: usize) -> Result<Connection, ClientError> {
        self.live_conn_at_with_reason(index, None).await
    }

    async fn live_conn_at_with_reason(
        &self,
        index: usize,
        reconnect_reason: Option<&'static str>,
    ) -> Result<Connection, ClientError> {
        let slot = self
            .tcp_pool_slots
            .get(index)
            .ok_or_else(|| ClientError::InvalidTarget("tuic tcp pool index out of range".into()))?;
        let mut state = slot.state.lock().await;
        let current = &mut state.current;
        let closed = current.transport.close_reason().is_some();
        if closed || reconnect_reason.is_some() {
            let reason = reconnect_reason.unwrap_or("transport_closed");
            self.reconnect_locked(index, &mut current.transport, reason)
                .await?;
            current.open_state.note_reconnect_success();
        }
        Ok(current.transport.clone())
    }

    async fn reconnect_locked(
        &self,
        index: usize,
        conn: &mut Connection,
        reason: &'static str,
    ) -> Result<(), ClientError> {
        println!(
            "🔁 tuic-tcp-pool-reconnect conn={index} id={} reason={reason}",
            conn.stable_id()
        );
        conn.close(VarInt::from_u32(0), reason.as_bytes());
        let hs = Self::handshake(
            &self.endpoint,
            self.server,
            &self.sni,
            &self.uuid,
            &self.password,
            self.zero_rtt,
        );
        *conn = tokio::time::timeout(TUIC_RECONNECT_TIMEOUT, hs)
            .await
            .map_err(|_| {
                io_err(
                    "tuic reconnect",
                    "5s 超时（黑洞/不可达；failover 据 is_dead 切备腿）",
                )
            })??;
        if let Some(secs) = self.quic_stats_secs
            && let Some(stop) = &self.quic_stats_stop
        {
            spawn_quic_stats_logger(conn.clone(), index, secs, stop.subscribe());
        }
        Ok(())
    }

    fn sample_tcp_pool_observations(&self) -> Vec<TcpPoolPathObservation> {
        self.tcp_pool_slots
            .iter()
            .map(|slot| {
                let Ok(state) = slot.state.try_lock() else {
                    return TcpPoolPathObservation::Unknown;
                };
                let current = &state.current;
                let conn = &current.transport;
                if conn.close_reason().is_some() {
                    return TcpPoolPathObservation::Unknown;
                }
                let path = conn.stats().path;
                TcpPoolPathObservation::known(
                    current.identity(),
                    path.cwnd,
                    path.rtt,
                    path.black_holes_detected,
                )
            })
            .collect()
    }

    fn tcp_pool_replacement_allowed(&self) -> Vec<bool> {
        if self.tcp_pool_draining_predecessor.load(Ordering::Acquire) {
            return vec![false; self.tcp_pool_slots.len()];
        }
        self.tcp_pool_slots
            .iter()
            .map(|slot| slot.replacement_available())
            .collect()
    }

    async fn acquire_tcp_pool_reservation(
        &self,
    ) -> Result<(TcpPoolSlotReservation, bool), ClientError> {
        loop {
            let available = self.tcp_pool_admission.available.notified();
            let observations = self.sample_tcp_pool_observations();
            let replacement_allowed = self.tcp_pool_replacement_allowed();
            match self
                .tcp_pool_admission
                .try_decide(&observations, &replacement_allowed)
            {
                Ok(TcpPoolAdmissionDecision::ReserveCurrent(reservation)) => {
                    if self.tcp_pool_draining_predecessor.load(Ordering::Acquire)
                        && reservation.qualification_override
                        && reservation.candidates.iter().any(|candidate| {
                            candidate.index != 0
                                && candidate.qualification == TcpPoolForwardQualification::Degraded
                                && !candidate.admitted
                        })
                    {
                        println!(
                            "⚠️ tuic-tcp-pool-generation-replacement-blocked slot={} reason=predecessor_draining",
                            reservation.index
                        );
                    }
                    return Ok((reservation, false));
                }
                Ok(TcpPoolAdmissionDecision::ReplaceAuxiliary(replacement)) => {
                    let Some(permit) = TcpPoolDrainingPermit::try_acquire(
                        self.tcp_pool_draining_predecessor.clone(),
                    ) else {
                        drop(replacement);
                        continue;
                    };
                    return self
                        .replace_auxiliary_generation(replacement, permit)
                        .await
                        .map(|reservation| (reservation, true));
                }
                Err(TcpPoolReservationError::Busy) => available.await,
                Err(error) => {
                    return Err(ClientError::InvalidTarget(format!(
                        "tuic tcp pool forward admission failed: {error:?}"
                    )));
                }
            }
        }
    }

    async fn replace_auxiliary_generation(
        &self,
        replacement: TcpPoolAuxiliaryReplacementPreparation,
        permit: TcpPoolDrainingPermit,
    ) -> Result<TcpPoolSlotReservation, ClientError> {
        let slot = self
            .tcp_pool_slots
            .get(replacement.index)
            .ok_or_else(|| ClientError::InvalidTarget("tuic tcp pool index out of range".into()))?
            .clone();
        let expected_activity = self
            .tcp_pool_admission
            .current_generation_activity(replacement.index)
            .ok_or_else(|| {
                io_err(
                    "tuic tcp pool generation replacement",
                    "current generation activity unavailable",
                )
            })?;
        println!(
            "🔄 tuic-tcp-pool-generation-replacement-start slot={} predecessor_id={} generation={} active={} black_hole_anchor={} black_holes_current={}",
            replacement.index,
            replacement.identity.stable_id,
            replacement.identity.generation,
            replacement.active_before,
            replacement.qualification_anchor,
            replacement.black_holes_current,
        );
        let started_at = Instant::now();
        let handshake = Self::handshake_aux_with_retries(
            &self.endpoint,
            self.server,
            &self.sni,
            &self.uuid,
            &self.password,
            self.zero_rtt,
            replacement.index,
        );
        let (successor_conn, auth_attempts) =
            tokio::time::timeout(TUIC_RECONNECT_TIMEOUT, handshake)
                .await
                .map_err(|_| {
                    io_err(
                        "tuic tcp pool generation replacement",
                        "successor handshake exceeded 5s",
                    )
                })??;
        let successor_generation =
            replacement
                .identity
                .generation
                .checked_add(1)
                .ok_or_else(|| {
                    io_err(
                        "tuic tcp pool generation replacement",
                        "generation counter saturated",
                    )
                })?;
        let successor = TcpPoolGeneration::new(
            successor_conn.clone(),
            successor_generation,
            auth_attempts as u64,
            self.clock,
        );
        let successor_activity = successor.activity.clone();
        let successor_path = successor_conn.stats().path;
        let successor_observation = TcpPoolPathObservation::known(
            successor.identity(),
            successor_path.cwnd,
            successor_path.rtt,
            successor_path.black_holes_detected,
        );
        let install = slot
            .install_successor_with(
                replacement.identity,
                &expected_activity,
                successor,
                started_at,
                |predecessor, successor| {
                    self.tcp_pool_admission.replace_current_generation_activity(
                        replacement.index,
                        predecessor,
                        successor.clone(),
                    )
                },
            )
            .await
            .map_err(|error| {
                io_err(
                    "tuic tcp pool generation replacement",
                    format!("successor install failed: {error:?}"),
                )
            })?;
        if let Some(secs) = self.quic_stats_secs
            && let Some(stop) = &self.quic_stats_stop
        {
            spawn_quic_stats_logger(successor_conn, replacement.index, secs, stop.subscribe());
        }
        println!(
            "✅ tuic-tcp-pool-generation-replacement-installed slot={} predecessor_id={} successor_id={} generation={} handshake_ms={} active={}",
            replacement.index,
            install.predecessor_identity.stable_id,
            install.successor_identity.stable_id,
            install.successor_identity.generation,
            started_at.elapsed().as_millis(),
            install.predecessor_activity.active(),
        );
        let predecessor_identity = install.predecessor_identity;
        let predecessor_activity = install.predecessor_activity.clone();
        let draining_owned = permit.transfer();
        tokio::spawn(async move {
            predecessor_activity.wait_for_zero().await;
            if let Some(draining) = slot.take_drained(predecessor_identity).await {
                draining
                    .generation
                    .transport
                    .close(VarInt::from_u32(0), b"tcp_pool_predecessor_drained");
                println!(
                    "✅ tuic-tcp-pool-generation-drained slot={} predecessor_id={} generation={} drain_ms={}",
                    slot.index,
                    predecessor_identity.stable_id,
                    predecessor_identity.generation,
                    draining.started_at.elapsed().as_millis(),
                );
                draining_owned.store(false, Ordering::Release);
            } else {
                println!(
                    "⚠️ tuic-tcp-pool-generation-replacement-blocked slot={} reason=predecessor_reap_identity_mismatch predecessor_id={} generation={}",
                    slot.index, predecessor_identity.stable_id, predecessor_identity.generation,
                );
            }
        });
        self.tcp_pool_admission
            .reserve_replacement_successor(replacement, successor_activity, successor_observation)
            .map_err(|error| {
                ClientError::InvalidTarget(format!(
                    "tuic tcp pool successor reservation failed: {error:?}"
                ))
            })
    }

    /// TCP 专用连接选择。Admission samples current Quinn path and PLPMTUD evidence without waiting,
    /// then atomically reserves an eligible slot. A busy slot that added a black-hole detection in
    /// its current ownership epoch is isolated while a non-degraded alternative exists. Remaining
    /// candidates retain least-active ordering and equal nonzero `cwnd / RTT` tie-breaking; an idle
    /// pool keeps stable-index ordering so a following opener observes the first reservation.
    /// Idle age only requests a bounded liveness probe; it is not itself permission to destroy an
    /// auxiliary connection. The per-slot mutex still serializes probe/reconnect and connection
    /// cloning while the already-visible reservation steers unrelated opens toward other slots.
    async fn live_tcp_conn(&self) -> Result<TcpPoolConnectionSelection, ClientError> {
        let (reservation, replacement_installed) = self.acquire_tcp_pool_reservation().await?;
        let TcpPoolSlotReservation {
            index,
            lease,
            active_before,
            path_service,
            path_service_tiebreak,
            qualification,
            qualification_anchor,
            black_holes_current,
            qualification_override,
            all_degraded_fallback,
            candidates,
            preparation,
        } = reservation;
        let idle_exclusive = active_before == 0;
        let now_secs = self.clock.elapsed().as_secs();
        let slot = self
            .tcp_pool_slots
            .get(index)
            .ok_or_else(|| ClientError::InvalidTarget("tuic tcp pool index out of range".into()))?;
        let mut state = slot.state.lock().await;
        let current = &mut state.current;
        let open_state = current.open_state.clone();
        let last_success_age_secs = open_state.last_success_age_secs(now_secs);
        let last_success_secs = open_state.last_success_secs().unwrap_or(0);
        let mut probe_result = "not_due";
        let mut reconnect_reason = current
            .transport
            .close_reason()
            .is_some()
            .then_some("transport_closed");

        if reconnect_reason.is_none() && open_state.needs_reconnect(index) && idle_exclusive {
            reconnect_reason = Some("previous_open_failure");
        }
        if reconnect_reason.is_none()
            && should_probe_tcp_pool_generation(
                index,
                now_secs,
                last_success_secs,
                idle_exclusive,
                replacement_installed,
            )
        {
            let outcome =
                probe_tcp_pool_connection(&current.transport, TUIC_TCP_POOL_LIVENESS_PROBE_TIMEOUT)
                    .await;
            probe_result = match outcome {
                TcpPoolProbeOutcome::Alive { .. } => "alive",
                TcpPoolProbeOutcome::Closed => "closed",
                TcpPoolProbeOutcome::SendFailed => "send_failed",
                TcpPoolProbeOutcome::TimedOut { .. } => "timeout",
            };
            reconnect_reason = tcp_pool_probe_reconnect_reason(outcome);
            println!(
                "🔎 tuic-tcp-pool-probe conn={index} id={} generation={} idle_age_secs={} result={probe_result}",
                current.transport.stable_id(),
                open_state.generation(),
                now_secs.saturating_sub(last_success_secs),
            );
        }

        if let Some(reason) = reconnect_reason {
            self.reconnect_locked(index, &mut current.transport, reason)
                .await?;
            open_state.note_reconnect_success();
            if index != 0 {
                let ready = probe_tcp_pool_connection(
                    &current.transport,
                    TUIC_TCP_POOL_LIVENESS_PROBE_TIMEOUT,
                )
                .await;
                probe_result = match ready {
                    TcpPoolProbeOutcome::Alive { .. } => "reconnected_alive",
                    TcpPoolProbeOutcome::Closed => "reconnected_closed",
                    TcpPoolProbeOutcome::SendFailed => "reconnected_send_failed",
                    TcpPoolProbeOutcome::TimedOut { .. } => "reconnected_timeout",
                };
                if !matches!(ready, TcpPoolProbeOutcome::Alive { .. }) {
                    open_state.note_open_failure(index);
                    current
                        .transport
                        .close(VarInt::from_u32(0), b"tcp_pool_ready_probe_failed");
                    return Err(io_err(
                        "tuic tcp pool ready probe",
                        format!("conn={index} result={probe_result}"),
                    ));
                }
            }
        }

        let conn = current.transport.clone();
        let generation = open_state.generation();
        let write_pressure = current.write_pressure.clone();
        let read_pressure = current.read_pressure.clone();
        let startup_auth_attempts = current.auth_attempts;
        drop(state);
        drop(preparation);
        Ok(TcpPoolConnectionSelection {
            conn_index: index,
            conn,
            lease,
            active_before,
            path_service,
            path_service_tiebreak,
            qualification,
            qualification_anchor,
            black_holes_current,
            qualification_override,
            all_degraded_fallback,
            candidates,
            generation,
            last_success_age_secs,
            probe_result,
            reconnect_reason,
            open_state,
            write_pressure,
            read_pressure,
            startup_auth_attempts,
            replacement_installed,
        })
    }

    /// 取当前活连接克隆——**非阻塞、不重连**（刀9，ADR-0011 §3b）。锁被占（后台 start_udp 正在重连，持锁
    /// 数秒）→ `None`，**绝不 await 锁**（否则在主循环 inline 的 send_udp 会被 stall）。连接已死 → `None`。
    /// 重连交给后台 `start_udp` 自愈循环（背景退避重连），与主循环解耦。
    fn current_conn(&self) -> Option<Connection> {
        let guard = self.tcp_pool_slots.first()?.state.try_lock().ok()?;
        if guard.current.transport.close_reason().is_some() {
            None
        } else {
            Some(guard.current.transport.clone())
        }
    }

    /// 发一条**已编码**的 TUIC Packet（刀3：datagram 主路径 + uni-stream 兜底）。
    /// 中文要点：先按 `udp_send_plan(max_datagram_size, len)` 主动分流——装得下走 native datagram，
    /// 超上限/不可用走 **per-packet uni-stream 兜底**（持续大流量直播不丢包）。datagram 真发遇
    /// `TooLarge`（MTU 竞态收缩）→ 二次 stream 兜底。仅**真失败**才丢弃计数（UDP 语义，不阻塞调用方除重连）。
    pub async fn send_udp(&self, datagram: Vec<u8>) {
        // 记录 UDP 活跃时刻：驱动任务据此「仅活跃时发」Heartbeat（纯 TCP 不发，省流量/电量）。
        self.last_udp_activity
            .store(self.clock.elapsed().as_secs(), Ordering::Relaxed);
        // 刀9（真出口 acceptance 修，ADR-0011 §3b）：**绝不在此 inline 重连**——send_udp 在主循环 inline
        // await（handle_tuic_udp_uplink），黑洞期每个 UDP 包触发 5s 重连会 stall 整个事件循环、饿死 DNS 劫持
        // 等所有主循环分支。改用 `current_conn`（try_lock 非阻塞 + 不重连）：连接活就发；锁被占（后台 start_udp
        // 正在重连）或已死 → 快速静默丢（热路径勿刷屏，udp_drops 计数即观测）。重连由后台 start_udp 自愈循环负责。
        let conn = match self.current_conn() {
            Some(c) => c,
            None => {
                self.udp_drops.fetch_add(1, Ordering::Relaxed);
                return;
            }
        };
        // Bytes：datagram 路径下 `clone()` 为 O(1)（Arc 引用计数），TooLarge 竞态二次兜底不深拷贝。
        let bytes = bytes::Bytes::from(datagram);
        // 按 config 的 relay mode 分流：Quic→恒 uni-stream（首包触发下行镜像 stream）；Native→size-based。
        match udp_send_plan(self.udp_relay_mode, conn.max_datagram_size(), bytes.len()) {
            UdpSend::Datagram => match conn.send_datagram(bytes.clone()) {
                Ok(()) => {}
                Err(quinn::SendDatagramError::TooLarge) => {
                    // 分流时还装得下、真发时 MTU 已收缩 → 二次 stream **兜底**（计 fallback），不丢。
                    self.udp_stream_fallbacks.fetch_add(1, Ordering::Relaxed);
                    self.send_udp_via_stream(&conn, bytes).await;
                }
                Err(e) => {
                    self.udp_drops.fetch_add(1, Ordering::Relaxed);
                    println!("⚠️ TUIC UDP↑ 发送失败（连接将自愈），丢弃: {e:?}");
                }
            },
            UdpSend::Stream => {
                // Native 走到这里=超 datagram 上限的**兜底**（计数，acceptance 判 MTU 调优是否够）；
                // Quic 模式 stream 是**主发路径**（非兜底，不计 fallback，否则 `📊 stream 兜底` 会被
                // 误读为 MTU 失败——量级 = 全部包）。主发量看 path.sent_packets。
                if self.udp_relay_mode == UdpRelayMode::Native {
                    self.udp_stream_fallbacks.fetch_add(1, Ordering::Relaxed);
                }
                self.send_udp_via_stream(&conn, bytes).await;
            }
        }
    }

    /// 在 uni-stream 上发一条完整 TUIC Packet：开单向流 → 写整条已编码 Packet → finish。
    /// 中文要点：TUIC quic-relay-mode 即「一条 uni-stream 承载一个完整 Packet 命令」（`FRAG_TOTAL=1`），
    /// 字节与 datagram 模式**完全一致**（复用同一 `encode_packet` 产物）。任一步失败→丢弃计数（UDP 自愈重发）。
    /// fallback 计数由调用方负责（仅 Native 兜底场景计），本函数只管 I/O + 真失败丢弃。
    async fn send_udp_via_stream(&self, conn: &Connection, datagram: bytes::Bytes) {
        if let Err(e) = Self::write_uni_packet(conn, &datagram).await {
            self.udp_drops.fetch_add(1, Ordering::Relaxed);
            println!("⚠️ TUIC UDP↑ uni-stream 发送失败（连接将自愈），丢弃: {e:?}");
        }
    }

    /// 在连接上开 uni-stream 写一个 Packet 并 finish（抽出便于错误归一）。
    async fn write_uni_packet(conn: &Connection, packet: &[u8]) -> Result<(), ClientError> {
        let mut uni = conn
            .open_uni()
            .await
            .map_err(|e| io_err("udp open_uni", e))?;
        uni.write_all(packet)
            .await
            .map_err(|e| io_err("udp uni write", e))?;
        uni.finish().map_err(|e| io_err("udp uni finish", e))?;
        Ok(())
    }

    /// 累计丢弃的上行 UDP datagram 数（可观测性）。
    pub fn udp_drop_count(&self) -> u64 {
        self.udp_drops.load(Ordering::Relaxed)
    }

    /// 累计走 uni-stream 兜底的上行包数（可观测性：MTU 调优是否足够 / 兜底是否过热）。
    pub fn udp_stream_fallback_count(&self) -> u64 {
        self.udp_stream_fallbacks.load(Ordering::Relaxed)
    }

    /// 启动 UDP 驱动后台任务，返回**下行 datagram 接收端**给主循环 select。
    /// 任务职责：① 下行泵——`read_datagram`（native）+ `accept_uni`（quic-relay-mode / 大包）→ channel
    /// （主循环 `decode_packet_meta` + 重组后注入 TUN）；② 周期 Heartbeat 维持连接。连接断开则确定性退避后
    /// 经 `live_conn` 重连，泵与心跳自然恢复（等价于"重连后重启泵/心跳"）。
    /// 中文要点：单自愈循环，避免多任务各自重连产生竞态；下行用 `send().await` 施加背压、不丢 DNS 响应。
    /// uni-stream 读取有界派生（`Semaphore`），超并发上限丢弃该 stream（UDP 自愈），防 flood 无界 spawn。
    pub fn start_udp(self: &Arc<Self>) -> mpsc::Receiver<Vec<u8>> {
        self.start_endpoint_recovery_monitor();
        let (downlink_tx, downlink_rx) = mpsc::channel::<Vec<u8>>(TUIC_DOWNLINK_CAPACITY);
        let me = Arc::clone(self);
        // 跨重连共享：限制下行 uni-stream 的并发读取数。
        let stream_sem = Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_DOWNLINK_STREAMS));
        tokio::spawn(async move {
            let mut attempt: u32 = 0;
            loop {
                let conn = match me.live_conn().await {
                    Ok(c) => {
                        attempt = 0;
                        println!("🌊 TUIC UDP 驱动已就绪（datagram 泵 + heartbeat）");
                        c
                    }
                    Err(e) => {
                        println!("TUIC UDP 驱动重连失败: {e:?}");
                        tokio::time::sleep(udp_reconnect_backoff(attempt)).await;
                        attempt = attempt.saturating_add(1);
                        continue;
                    }
                };
                let mut hb = tokio::time::interval(Duration::from_secs(TUIC_HEARTBEAT_SECS));
                hb.tick().await; // 第一拍立即返回，跳过（避免连上瞬间立刻发心跳）。
                let mut stats = tokio::time::interval(Duration::from_secs(UDP_STATS_LOG_SECS));
                stats.tick().await; // 跳过第一拍（连上瞬间不打统计）。
                // 刀11：datagram 背压上升沿 latch（单 task 局部、无需原子；每重连复位，新连接从未背压起算）。
                let mut prev_pressured = false;
                loop {
                    tokio::select! {
                        dg = conn.read_datagram() => {
                            match dg {
                                Ok(bytes) => {
                                    if downlink_tx.send(bytes.to_vec()).await.is_err() {
                                        return; // 主循环已退出 → 结束任务
                                    }
                                }
                                Err(_) => break, // 连接断 → 外层 live_conn 重连
                            }
                        }
                        // quic-relay-mode / 超 datagram 上限的下行包：经 uni-stream 来。读满整条 Packet
                        // → 同一下行 channel（与 datagram 路径同构，主循环统一 decode + 重组）。
                        stream = conn.accept_uni() => {
                            match stream {
                                Ok(recv) => {
                                    // 有界派生：拿到 permit 才读；拿不到（达并发上限）→ 丢弃该 stream（UDP 自愈）。
                                    if let Ok(permit) = Arc::clone(&stream_sem).try_acquire_owned() {
                                        let tx = downlink_tx.clone();
                                        let m = Arc::clone(&me.metrics);
                                        tokio::spawn(async move {
                                            let _permit = permit; // 持有到读完，限并发
                                            match read_uni_packet(recv).await {
                                                Some(pkt) => {
                                                    let _ = tx.send(pkt).await;
                                                }
                                                // 刀11：下行 uni-stream 解码/读失败（超长/reset/空）→ 静默丢，计下行 drop。
                                                None => m.inc_udp_drops_down(),
                                            }
                                        });
                                    } else {
                                        // 刀11：下行并发达上限（信号量耗尽）→ 丢弃该 stream（UDP 自愈），计下行 drop。
                                        me.metrics.inc_udp_drops_down();
                                    }
                                }
                                Err(_) => break, // 连接断 → 外层 live_conn 重连
                            }
                        }
                        _ = stats.tick() => {
                            // 周期性观测（刀3.5）：UDP 最近活跃 或 有兜底/丢弃 才打（空闲静默，不刷屏）。
                            // 中文要点：native datagram acceptance 下 fb/drops 常为 0，但仍需看 cwnd/rtt/丢包，
                            // 故以「最近 UDP 活跃」为主闸门——量化 datagram 天花板成因（CC vs 无背压）。
                            let fb = me.udp_stream_fallbacks.load(Ordering::Relaxed);
                            let drops = me.udp_drops.load(Ordering::Relaxed);
                            let now = me.clock.elapsed().as_secs();
                            let last = me.last_udp_activity.load(Ordering::Relaxed);
                            let udp_active = should_send_heartbeat(last, now, TUIC_HB_IDLE_WINDOW_SECS);
                            let max_dg = conn.max_datagram_size();
                            let space = conn.datagram_send_buffer_space();
                            // datagram 背压代理信号：剩余 < 1 个 datagram 上限 → 即将丢最老（静默丢盲点）。
                            // 仅 Native 模式有意义（Quic 模式 relay 不走 datagram，背压无关，避免假警报）。
                            let pressured = me.udp_relay_mode == UdpRelayMode::Native
                                && is_datagram_pressured(
                                    space,
                                    max_dg.unwrap_or(quic::QUIC_INITIAL_MTU as usize),
                                );
                            // 刀11：**每 tick 都更新 latch**（独立于打印门控，否则空闲期 latch 不复位）。
                            // 只在 false→true 上升沿计一次「集」，避免一段持续背压每 tick 重复计数。
                            if note_pressure_edge(pressured, &mut prev_pressured) {
                                me.metrics.inc_datagram_pressure_events();
                            }
                            if udp_active || fb > 0 || drops > 0 {
                                let path = conn.stats().path;
                                let line = format_udp_stats(
                                    max_dg, fb, drops,
                                    path.rtt.as_millis(), path.cwnd,
                                    path.lost_packets, path.sent_packets, space,
                                );
                                if pressured {
                                    println!("{line} ⚠️背压(datagram 缓冲将丢最老)");
                                } else {
                                    println!("{line}");
                                }
                            }
                        }
                        _ = hb.tick() => {
                            // 仅在「最近有 UDP 上行」时发心跳；纯 TCP 会话由 QUIC keep-alive 保活，不发。
                            let now = me.clock.elapsed().as_secs();
                            let last = me.last_udp_activity.load(Ordering::Relaxed);
                            if should_send_heartbeat(last, now, TUIC_HB_IDLE_WINDOW_SECS)
                                && conn.send_datagram(encode_heartbeat().into()).is_err()
                            {
                                break; // 连接断 → 外层 live_conn 重连
                            }
                        }
                    }
                }
                println!("🔌 TUIC UDP 驱动连接断开，准备重连");
                tokio::time::sleep(udp_reconnect_backoff(attempt)).await;
                attempt = attempt.saturating_add(1);
            }
        });
        downlink_rx
    }

    fn start_endpoint_recovery_monitor(self: &Arc<Self>) {
        if self
            .endpoint_recovery_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }

        let weak = Arc::downgrade(self);
        tokio::spawn(async move {
            let mut state = EndpointRecoveryState::default();
            let mut evidence_observer = tcp_diag_enabled().then(RecoveryEvidenceObserver::default);
            let mut last_tcp_pool_activity = None;
            let mut tick = tokio::time::interval(ENDPOINT_RECOVERY_SAMPLE_INTERVAL);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                let Some(upstream) = weak.upgrade() else {
                    return;
                };
                let Some((input, connection_handles)) = upstream.endpoint_recovery_input() else {
                    continue;
                };
                if let Some(active_leases) =
                    note_tcp_pool_activity_transition(&mut last_tcp_pool_activity, input.active_tcp)
                {
                    println!("🔎 tuic-tcp-pool-activity active_leases={active_leases}");
                }
                let now = Instant::now();
                if let Some(observer) = evidence_observer.as_mut() {
                    for event in observer.observe(now, &input) {
                        log_recovery_evidence(event);
                    }
                }
                match state.observe(now, &input) {
                    EndpointRecoveryAction::None => {}
                    EndpointRecoveryAction::Recovered {
                        generation,
                        socket_generation,
                        recovery_time,
                        connection_count,
                    } => {
                        println!(
                            "✅ tuic-endpoint-rebind-recovered generation={generation} first_rx_ms={} socket_generation={socket_generation} connections={connection_count}",
                            recovery_time.as_millis(),
                        );
                    }
                    EndpointRecoveryAction::ResetConnectionPath {
                        generation,
                        trigger,
                        stall_bound,
                        max_rtt,
                    } => {
                        let (
                            write_conn,
                            write_writer,
                            write_stream,
                            write_episode,
                            write_acknowledged_bytes,
                            write_pending_ms,
                        ) = trigger.tcp_write_fields();
                        let (black_hole_anchor, black_holes_current) = match trigger {
                            EndpointRecoveryTrigger::TcpPathDegraded {
                                black_hole_anchor,
                                black_holes_current,
                                ..
                            } => (black_hole_anchor, black_holes_current),
                            _ => (0, 0),
                        };
                        match connection_handles
                            .iter()
                            .find(|handle| handle.stable_id == write_conn)
                        {
                            Some(handle) => {
                                let before = handle.connection.stats().path;
                                match handle.connection.path_changed() {
                                    Ok(()) => {
                                        let after = handle.connection.stats().path;
                                        println!(
                                            "🔄 tuic-connection-path-reset generation={generation} result=applied trigger={} write_conn={write_conn} pool_index={} pool_generation={} write_writer={write_writer} write_stream={write_stream} write_episode={write_episode} write_acknowledged={write_acknowledged_bytes}B write_pending_ms={write_pending_ms} black_hole_anchor={black_hole_anchor} black_holes_current={black_holes_current} bound_ms={} max_rtt_ms={} rtt_before_ms={} rtt_after_ms={} cwnd_before={} cwnd_after={} mtu_before={} mtu_after={}",
                                            trigger.label(),
                                            handle.pool_index,
                                            handle.pool_generation,
                                            stall_bound.as_millis(),
                                            max_rtt.as_millis(),
                                            before.rtt.as_millis(),
                                            after.rtt.as_millis(),
                                            before.cwnd,
                                            after.cwnd,
                                            before.current_mtu,
                                            after.current_mtu,
                                        );
                                    }
                                    Err(error) => {
                                        println!(
                                            "⚠️ tuic-connection-path-reset generation={generation} result=closed trigger={} write_conn={write_conn} pool_index={} pool_generation={} write_writer={write_writer} write_stream={write_stream} write_episode={write_episode} write_acknowledged={write_acknowledged_bytes}B write_pending_ms={write_pending_ms} black_hole_anchor={black_hole_anchor} black_holes_current={black_holes_current} bound_ms={} max_rtt_ms={} error={error}",
                                            trigger.label(),
                                            handle.pool_index,
                                            handle.pool_generation,
                                            stall_bound.as_millis(),
                                            max_rtt.as_millis(),
                                        );
                                    }
                                }
                            }
                            None => {
                                println!(
                                    "⚠️ tuic-connection-path-reset generation={generation} result=not_found trigger={} write_conn={write_conn} write_writer={write_writer} write_stream={write_stream} write_episode={write_episode} write_acknowledged={write_acknowledged_bytes}B write_pending_ms={write_pending_ms} black_hole_anchor={black_hole_anchor} black_holes_current={black_holes_current} bound_ms={} max_rtt_ms={}",
                                    trigger.label(),
                                    stall_bound.as_millis(),
                                    max_rtt.as_millis(),
                                );
                            }
                        }
                    }
                    EndpointRecoveryAction::Rebind {
                        generation,
                        trigger,
                        stalled_for,
                        stall_bound,
                        tx_bytes_since_rx,
                        max_rtt,
                    } => {
                        let (
                            write_conn,
                            write_writer,
                            write_stream,
                            write_episode,
                            write_acknowledged_bytes,
                            write_pending_ms,
                        ) = trigger.tcp_write_fields();
                        let (
                            read_conn,
                            read_reader,
                            read_stream,
                            read_episode,
                            read_offset,
                            read_next_offset,
                            read_highest_offset,
                            read_buffered_bytes,
                            read_gap_bytes,
                            read_observations,
                        ) = trigger.tcp_read_fields();
                        match quic::rebind_client_endpoint_udp_socket(
                            &upstream.endpoint,
                            upstream.udp_send_service_policy,
                            upstream.udp_send_service_stats.as_ref(),
                        ) {
                            Ok(rebound) => {
                                state.note_rebind_succeeded(now, rebound.rebind_generation);
                                println!(
                                    "🔄 tuic-endpoint-rebind generation={generation} trigger={} write_conn={} write_writer={} write_stream={} write_episode={} write_acknowledged={}B write_pending_ms={} read_conn={read_conn} read_reader={read_reader} read_stream={read_stream} read_episode={read_episode} read_offset={read_offset} read_next_offset={read_next_offset} read_highest_offset={read_highest_offset} read_buffered={read_buffered_bytes}B read_gap={read_gap_bytes}B read_observations={read_observations} old_local={} new_local={} socket_generation={} active_tcp={} udp_active={} live_connections={} stalled_ms={} bound_ms={} tx_since_rx={}B max_rtt_ms={}",
                                    trigger.label(),
                                    write_conn,
                                    write_writer,
                                    write_stream,
                                    write_episode,
                                    write_acknowledged_bytes,
                                    write_pending_ms,
                                    rebound.old_local_addr,
                                    rebound.new_local_addr,
                                    rebound.rebind_generation,
                                    input.active_tcp,
                                    input.udp_active,
                                    input.connections.len(),
                                    stalled_for.as_millis(),
                                    stall_bound.as_millis(),
                                    tx_bytes_since_rx,
                                    max_rtt.as_millis(),
                                );
                            }
                            Err(error) => {
                                println!(
                                    "⚠️ tuic-endpoint-rebind-failed generation={generation} trigger={} write_conn={} write_writer={} write_stream={} write_episode={} write_acknowledged={}B write_pending_ms={} read_conn={read_conn} read_reader={read_reader} read_stream={read_stream} read_episode={read_episode} read_offset={read_offset} read_next_offset={read_next_offset} read_highest_offset={read_highest_offset} read_buffered={read_buffered_bytes}B read_gap={read_gap_bytes}B read_observations={read_observations} active_tcp={} udp_active={} live_connections={} stalled_ms={} bound_ms={} tx_since_rx={}B max_rtt_ms={} error={error}",
                                    trigger.label(),
                                    write_conn,
                                    write_writer,
                                    write_stream,
                                    write_episode,
                                    write_acknowledged_bytes,
                                    write_pending_ms,
                                    input.active_tcp,
                                    input.udp_active,
                                    input.connections.len(),
                                    stalled_for.as_millis(),
                                    stall_bound.as_millis(),
                                    tx_bytes_since_rx,
                                    max_rtt.as_millis(),
                                );
                            }
                        }
                    }
                }
            }
        });
    }

    fn endpoint_recovery_input(
        &self,
    ) -> Option<(EndpointRecoveryInput, Vec<EndpointRecoveryConnectionHandle>)> {
        let mut connections = Vec::with_capacity(self.tcp_pool_slots.len() + 1);
        let mut connection_handles = Vec::with_capacity(self.tcp_pool_slots.len() + 1);
        let sampled_at = Instant::now();
        for slot in &self.tcp_pool_slots {
            let state = slot.state.try_lock().ok()?;
            for generation in std::iter::once(&state.current)
                .chain(state.draining.as_ref().map(|draining| &draining.generation))
            {
                let conn = &generation.transport;
                if conn.close_reason().is_some() {
                    continue;
                }
                let stats = conn.stats();
                let stable_id = conn.stable_id();
                connections.push(EndpointRecoveryConnectionSample {
                    stable_id,
                    tx_bytes: stats.udp_tx.bytes,
                    rx_bytes: stats.udp_rx.bytes,
                    rtt: stats.path.rtt,
                    black_holes_detected: stats.path.black_holes_detected,
                    current_socket_rx_rebind_generation: conn.current_socket_rx_rebind_generation(),
                    tcp_write_pressures: generation.write_pressure.snapshots_at(sampled_at),
                    tcp_ordered_read_progress: generation.read_pressure.snapshots(),
                });
                connection_handles.push(EndpointRecoveryConnectionHandle {
                    stable_id,
                    pool_index: slot.index,
                    pool_generation: generation.open_state.generation(),
                    connection: conn.clone(),
                });
            }
        }
        let active_tcp = self.tcp_pool_admission.active_total();
        let now = self.clock.elapsed().as_secs();
        let last_udp_activity = self.last_udp_activity.load(Ordering::Relaxed);
        let udp_active = should_send_heartbeat(last_udp_activity, now, TUIC_HB_IDLE_WINDOW_SECS);
        Some((
            EndpointRecoveryInput {
                active_tcp,
                udp_active,
                last_udp_activity_secs: last_udp_activity,
                current_socket_rx_rebind_generation: self
                    .endpoint
                    .stats()
                    .current_socket_rx_rebind_generation,
                connections,
            },
            connection_handles,
        ))
    }
}

/// 读满一条下行 uni-stream（一个完整 TUIC Packet 命令），上限 `MAX_TUIC_PACKET_BYTES`。
/// 中文要点：流过大/被 reset/读错 → None（丢弃该包，UDP 自愈）。空流（finish 无数据）→ None。
async fn read_uni_packet(mut recv: quinn::RecvStream) -> Option<Vec<u8>> {
    match recv.read_to_end(MAX_TUIC_PACKET_BYTES).await {
        Ok(buf) if !buf.is_empty() => Some(buf),
        _ => None,
    }
}

/// 是否该发 TUIC Heartbeat：仅当**最近有 UDP 上行活动**(距上次 ≤ 活跃窗口)。
/// `last_activity=0` 表示从未发过 UDP → 不发(纯 TCP 会话靠 QUIC keep-alive 保活)。
/// 中文要点：纯函数,`now`/`last` 同源单调秒数,便于单测;边界 `now-last==window` 取发(`<=`)。
fn should_send_heartbeat(last_activity: u64, now: u64, idle_window: u64) -> bool {
    last_activity != 0 && now.saturating_sub(last_activity) <= idle_window
}

/// UDP 驱动重连退避：确定性指数退避（`base * 2^attempt`，封顶 `CAP`）。
/// 中文要点：UDP 无连接状态、重连后下个 datagram 即自愈，对重连节奏不敏感，故不引入 jitter。
fn udp_reconnect_backoff(attempt: u32) -> Duration {
    let exp = UDP_RECONNECT_BASE_MS.saturating_mul(1u64.checked_shl(attempt).unwrap_or(u64::MAX));
    Duration::from_millis(exp.min(UDP_RECONNECT_CAP_MS))
}

#[async_trait::async_trait]
impl ProxyUpstream for TuicUpstream {
    async fn open_tcp(&self, target: &TargetAddr) -> Result<RelayStream, ClientError> {
        // 取 TCP pool 活连接，断了只重连该槽位；pool=1 时等价旧行为。
        let selection = self.live_tcp_conn().await?;
        let TcpPoolConnectionSelection {
            conn_index,
            conn,
            lease,
            active_before,
            path_service,
            path_service_tiebreak,
            qualification,
            qualification_anchor,
            black_holes_current,
            qualification_override,
            all_degraded_fallback,
            candidates,
            generation,
            last_success_age_secs,
            probe_result,
            reconnect_reason,
            open_state,
            write_pressure,
            read_pressure: _,
            startup_auth_attempts,
            replacement_installed,
        } = selection;
        // 刀9（真出口 acceptance 修）：open_bi + write Connect 在**黑洞连接**上会 hang——连接尚未被
        // 判死（close_reason 仍 None，因 keepalive/非对称封锁架空 idle 检测），但 QUIC send 窗口满、
        // 收不到 ACK → write_all 无限阻塞，failover 快/慢路都收不到信号。封 5s 超时让黑洞 open **快速失败**
        // → FailoverUpstream 据「连接活但 open 超时」走 **慢路计数**（并发 open 下 ~5s 累计 3 次即切 REALITY，
        // 不再死等 close_reason）。正常 open（open_bi + 写小 Connect 头）是本地操作、远小于 5s，不误伤。
        let open = async {
            let (send, recv) = conn
                .open_bi()
                .await
                .map_err(|e| io_err("tuic open_bi", e))?;
            let stable_id = conn.stable_id();
            let stream_id = recv.id().index();
            let (mut send, send_progress) =
                TuicTcpStartupWriter::prepare(send, &encode_connect(target))
                    .await
                    .map_err(|e| io_err("tuic connect write", e))?;
            open_state.note_open_success(self.clock.elapsed().as_secs());
            let relay_mode = tuic_tcp_relay_mode();
            let diag_meta = if tcp_diag_enabled() {
                println!(
                    "{}",
                    format_tuic_tcp_pool_selection_line(TuicTcpPoolSelectionDiag {
                        conn_index,
                        stable_id,
                        active_before,
                        path_service,
                        path_service_tiebreak,
                        qualification,
                        qualification_anchor,
                        black_holes_current,
                        qualification_override,
                        all_degraded_fallback,
                        candidates: &candidates,
                        generation,
                        last_success_age_secs,
                        probe_result,
                        reconnect_reason,
                        replacement_installed,
                    })
                );
                println!(
                    "{}",
                    format_tuic_tcp_open_line(
                        target,
                        conn_index,
                        stable_id,
                        stream_id,
                        startup_auth_attempts,
                        relay_mode,
                    )
                );
                Some(TuicTcpStreamDiagMeta::new(
                    target, conn_index, stable_id, stream_id,
                ))
            } else {
                None
            };
            let tcp_stream_diag = diag_meta
                .clone()
                .map(|meta| TuicTcpStreamDiag::new(meta, Instant::now()));
            send.set_diag_meta(diag_meta.clone());
            let pressure_writer = write_pressure.writer(stream_id);
            let relay: RelayStream = match relay_mode {
                TuicTcpRelayMode::OrderedJoin => Box::new(TrackedRelayStream::new_with_transport(
                    TcpWritePressureAdapter::new_with_progress(
                        tokio::io::join(recv, send),
                        pressure_writer,
                        send_progress,
                    ),
                    lease,
                    tcp_stream_diag,
                    conn.clone(),
                )),
                TuicTcpRelayMode::OrderedChunk => Box::new(TrackedRelayStream::new_with_transport(
                    TcpWritePressureAdapter::new_with_progress(
                        TuicOrderedRelayStream::from_quinn(recv, send),
                        pressure_writer,
                        send_progress,
                    ),
                    lease,
                    tcp_stream_diag,
                    conn.clone(),
                )),
                TuicTcpRelayMode::UnorderedReassembly => {
                    Box::new(TrackedRelayStream::new_with_transport(
                        TcpWritePressureAdapter::new_with_progress(
                            TuicChunkRelayStream::new(recv, send, diag_meta),
                            pressure_writer,
                            send_progress,
                        ),
                        lease,
                        tcp_stream_diag,
                        conn.clone(),
                    ))
                }
                TuicTcpRelayMode::NativeChunkPump => {
                    unreachable!("native TUIC TCP relay is returned by open_tcp_relay")
                }
                TuicTcpRelayMode::NativeOrderedPump => {
                    unreachable!("native ordered TUIC TCP relay is returned by open_tcp_relay")
                }
                TuicTcpRelayMode::D16DirectOrdered => {
                    unreachable!("D16 direct TUIC TCP relay is returned by open_tcp_relay")
                }
            };
            Ok::<RelayStream, ClientError>(relay)
        };
        let result = tokio::time::timeout(TUIC_OPEN_TIMEOUT, open)
            .await
            .map_err(|_| {
                io_err(
                    "tuic open_tcp",
                    "5s 超时（黑洞/send 窗口满无 ACK；failover 慢路据此累计切备腿）",
                )
            })
            .and_then(|result| result);
        if result.is_err() {
            open_state.note_open_failure(conn_index);
        }
        result
    }

    async fn open_tcp_relay(&self, target: &TargetAddr) -> Result<OpenedTcpRelay, ClientError> {
        let Some(relay_mode) = select_tuic_native_relay_mode(
            h10d16_byte_owned_egress_enabled(),
            tuic_tcp_native_ordered_pump_enabled(),
            tuic_tcp_native_chunk_pump_enabled(),
        ) else {
            return self.open_tcp(target).await.map(OpenedTcpRelay::Generic);
        };
        let d16_byte_owned = relay_mode == TuicTcpRelayMode::D16DirectOrdered;

        let selection = self.live_tcp_conn().await?;
        let TcpPoolConnectionSelection {
            conn_index,
            conn,
            lease,
            active_before,
            path_service,
            path_service_tiebreak,
            qualification,
            qualification_anchor,
            black_holes_current,
            qualification_override,
            all_degraded_fallback,
            candidates,
            generation,
            last_success_age_secs,
            probe_result,
            reconnect_reason,
            open_state,
            write_pressure,
            read_pressure,
            startup_auth_attempts,
            replacement_installed,
        } = selection;
        let open = async {
            let (send, recv) = conn
                .open_bi()
                .await
                .map_err(|e| io_err("tuic open_bi", e))?;
            let stable_id = conn.stable_id();
            let stream_id = recv.id().index();
            let (mut send, send_progress) =
                TuicTcpStartupWriter::prepare(send, &encode_connect(target))
                    .await
                    .map_err(|e| io_err("tuic connect write", e))?;
            open_state.note_open_success(self.clock.elapsed().as_secs());
            let diag_meta = if tcp_diag_enabled() {
                println!(
                    "{}",
                    format_tuic_tcp_pool_selection_line(TuicTcpPoolSelectionDiag {
                        conn_index,
                        stable_id,
                        active_before,
                        path_service,
                        path_service_tiebreak,
                        qualification,
                        qualification_anchor,
                        black_holes_current,
                        qualification_override,
                        all_degraded_fallback,
                        candidates: &candidates,
                        generation,
                        last_success_age_secs,
                        probe_result,
                        reconnect_reason,
                        replacement_installed,
                    })
                );
                println!(
                    "{}",
                    format_tuic_tcp_open_line(
                        target,
                        conn_index,
                        stable_id,
                        stream_id,
                        startup_auth_attempts,
                        relay_mode,
                    )
                );
                Some(TuicTcpStreamDiagMeta::new(
                    target, conn_index, stable_id, stream_id,
                ))
            } else {
                None
            };
            let tcp_stream_diag = diag_meta
                .clone()
                .map(|meta| TuicTcpStreamDiag::new(meta, Instant::now()));
            send.set_diag_meta(diag_meta);
            let reader_lease = lease.try_clone().map_err(|error| {
                io_err(
                    "tuic TCP generation lease split",
                    format!("failed to reserve reader ownership: {error:?}"),
                )
            })?;
            let reader: NativeTcpReadHalf = if d16_byte_owned {
                let read_pressure = tcp_diag_enabled()
                    .then(|| read_pressure.reader(stream_id, recv.progress_handle()));
                Box::new(TuicNativeOrderedReader::new(
                    recv,
                    reader_lease,
                    tcp_stream_diag,
                    conn.clone(),
                    read_pressure,
                ))
            } else if relay_mode == TuicTcpRelayMode::NativeOrderedPump {
                Box::new(TuicNativeOrderedPumpReader::spawn(
                    recv,
                    reader_lease,
                    tcp_stream_diag,
                    conn.clone(),
                ))
            } else {
                Box::new(TuicNativeTcpReader::new(
                    recv,
                    reader_lease,
                    tcp_stream_diag,
                    conn.clone(),
                ))
            };
            let writer = Box::new(TuicNativeTcpWriter::new(
                send,
                lease,
                write_pressure.writer(stream_id),
                send_progress,
            ));
            let relay = NativeTcpRelayStream { reader, writer };
            Ok::<OpenedTcpRelay, ClientError>(if d16_byte_owned {
                OpenedTcpRelay::NativeByteOwned(relay)
            } else {
                OpenedTcpRelay::Native(relay)
            })
        };
        let result = tokio::time::timeout(TUIC_OPEN_TIMEOUT, open)
            .await
            .map_err(|_| {
                io_err(
                    "tuic open_tcp",
                    "5s 超时（黑洞/send 窗口满无 ACK；failover 慢路据此累计切备腿）",
                )
            })
            .and_then(|result| result);
        if result.is_err() {
            open_state.note_open_failure(conn_index);
        }
        result
    }

    /// 刀14d：TUIC `open_tcp` 通常很快，但它仍可能 await QUIC reconnect、`open_bi` 或 Connect write
    /// flow-control up to `TUIC_OPEN_TIMEOUT`. 在单任务主循环里 inline await 会让一条慢 open 暂停
    /// 下行 flush、timer、DNS 和 reap；所以走已有 async-open 状态机。
    fn open_is_cheap(&self) -> bool {
        false
    }
}

#[async_trait::async_trait]
impl DatagramUpstream for TuicUpstream {
    // 委托既有 inherent `send_udp`（inherent 优先解析，不会递归）。
    async fn send_udp(&self, datagram: Vec<u8>) {
        TuicUpstream::send_udp(self, datagram).await
    }

    // 刀11：上行计数仍住 TuicUpstream（零回归）→ 经既有 inherent 访问器暴露给 snapshot。
    fn udp_drops_up(&self) -> u64 {
        self.udp_drop_count()
    }
    fn udp_stream_fallbacks(&self) -> u64 {
        self.udp_stream_fallback_count()
    }
}

impl Drop for TuicUpstream {
    fn drop(&mut self) {
        if let Some(stop) = &self.quic_stats_stop {
            let _ = stop.send(true);
        }
    }
}

/// 刀9 F1：failover 健康探测面。`probe` 主动探活（live_conn=QUIC 握手+TUIC 认证，非浅探）；
/// `is_dead` 区分 down 快路（黑洞，连接被 idle/keepalive 打死）/慢路（流失败）。
#[async_trait::async_trait]
impl crate::failover::HealthProbe for TuicUpstream {
    async fn probe(&self) -> bool {
        self.live_conn().await.is_ok()
    }
    async fn is_dead(&self) -> bool {
        self.tcp_pool_slots
            .first()
            .expect("TuicUpstream always has a primary connection")
            .state
            .lock()
            .await
            .current
            .transport
            .close_reason()
            .is_some()
    }
    /// 累计收到的 UDP datagram 数（quinn `stats().udp_rx.datagrams`）——黑洞存活信标：健康连接每 ~5s
    /// 有 keepalive ACK 进来→单调增；黑洞连 ACK 都收不到→停滞。down 探测据此判黑洞（不被乐观开流/keepalive 架空）。
    /// **非阻塞**（try_lock，同 current_conn）：锁被占（后台 start_udp 正在重连，持锁≤5s）→ `None`，**绝不 await
    /// 锁**——否则健康探针任务被卡 5s、把锁等待当成网络停滞、检测计时偏斜（review finding）。调用方对 None
    /// 跳过本次观察：连接正在重连本就不健康，停滞计时靠 `now` 累积、不重置，下次能读到（仍停滞）即判黑洞。
    async fn rx_datagrams(&self) -> Option<u64> {
        Some(
            self.tcp_pool_slots
                .first()?
                .state
                .try_lock()
                .ok()?
                .current
                .transport
                .stats()
                .udp_rx
                .datagrams,
        )
    }
}

#[cfg(test)]
#[path = "tuic_direct_probe.rs"]
mod direct_probe;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared::TargetAddr;
    use crate::tcp_downlink_pump::{AsyncLeasedByteFlowQueue, DownstreamPermitReleaseMode};
    use std::sync::Arc;
    use tokio::io::AsyncWriteExt;

    #[derive(Debug, Default)]
    struct StartupPriorityProbeState {
        priority: i32,
        atomic_pending_once: bool,
        atomic_calls: usize,
        queued_priorities: Vec<i32>,
        writes: Vec<Vec<u8>>,
    }

    struct StartupPriorityProbeWriter {
        state: Arc<StdMutex<StartupPriorityProbeState>>,
    }

    impl AsyncWrite for StartupPriorityProbeWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            self.state.lock().unwrap().writes.push(buf.to_vec());
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    impl TuicTcpStartupPriorityWrite for StartupPriorityProbeWriter {
        fn current_priority(&mut self) -> io::Result<i32> {
            Ok(self.state.lock().unwrap().priority)
        }

        fn set_stream_priority(&mut self, priority: i32) -> io::Result<()> {
            self.state.lock().unwrap().priority = priority;
            Ok(())
        }

        fn poll_write_then_set_priority(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
            priority_after: i32,
        ) -> Poll<io::Result<usize>> {
            let mut state = self.state.lock().unwrap();
            state.atomic_calls += 1;
            if state.atomic_pending_once {
                state.atomic_pending_once = false;
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            let priority = state.priority;
            state.queued_priorities.push(priority);
            state.priority = priority_after;
            state.writes.push(buf.to_vec());
            Poll::Ready(Ok(buf.len()))
        }
    }

    #[tokio::test]
    async fn tcp_startup_service_is_one_atomic_business_write_after_connect() {
        let state = Arc::new(StdMutex::new(StartupPriorityProbeState {
            priority: 7,
            atomic_pending_once: true,
            ..StartupPriorityProbeState::default()
        }));
        let probe = StartupPriorityProbeWriter {
            state: Arc::clone(&state),
        };
        let mut writer = TuicTcpStartupWriter::arm(probe).unwrap();

        writer.write_connect(b"connect").await.unwrap();
        std::future::poll_fn(|cx| Pin::new(&mut writer).poll_write(cx, &[]))
            .await
            .unwrap();
        writer.write_all(b"first-payload").await.unwrap();
        writer.write_all(b"later-payload").await.unwrap();

        let state = state.lock().unwrap();
        assert_eq!(state.priority, 7);
        assert_eq!(state.atomic_calls, 2, "Pending and successful admission");
        assert_eq!(state.queued_priorities, vec![8]);
        assert_eq!(
            state.writes,
            vec![
                b"connect".to_vec(),
                Vec::new(),
                b"first-payload".to_vec(),
                b"later-payload".to_vec()
            ]
        );
    }

    fn tcp_pool_observations(path_service: &[TcpPoolPathService]) -> Vec<TcpPoolPathObservation> {
        path_service
            .iter()
            .copied()
            .enumerate()
            .map(|(index, path_service)| match path_service {
                TcpPoolPathService::Unknown => TcpPoolPathObservation::Unknown,
                TcpPoolPathService::Known { .. } => TcpPoolPathObservation::Known {
                    identity: TcpPoolTransportIdentity::new(index, 1),
                    path_service,
                    black_holes_detected: 0,
                },
            })
            .collect()
    }

    fn endpoint_recovery_connection(
        stable_id: usize,
        black_holes_detected: u64,
        pressure: Option<TcpWritePressureSnapshot>,
    ) -> EndpointRecoveryConnectionSample {
        EndpointRecoveryConnectionSample {
            stable_id,
            tx_bytes: 100_000,
            rx_bytes: 10_000,
            rtt: Duration::from_millis(159),
            black_holes_detected,
            current_socket_rx_rebind_generation: 0,
            tcp_write_pressures: pressure.into_iter().collect(),
            tcp_ordered_read_progress: Vec::new(),
        }
    }

    fn endpoint_recovery_path_sample(
        connections: Vec<EndpointRecoveryConnectionSample>,
    ) -> EndpointRecoveryInput {
        EndpointRecoveryInput {
            active_tcp: 1,
            udp_active: false,
            last_udp_activity_secs: 0,
            current_socket_rx_rebind_generation: 0,
            connections,
        }
    }

    fn endpoint_recovery_pending(
        writer: u64,
        episode: u64,
        pending_for: Duration,
        ack_stalled_for: Duration,
        acknowledged_bytes: u64,
    ) -> TcpWritePressureSnapshot {
        TcpWritePressureSnapshot {
            writer,
            stream: writer.saturating_mul(2).saturating_add(1),
            episode,
            pending_for,
            ack_stalled_for,
            acknowledged_bytes,
        }
    }

    fn endpoint_recovery_ordered_progress(
        reader: u64,
        stream: u64,
        read_offset: u64,
        next_received_offset: Option<u64>,
        highest_received_offset: u64,
        buffered_bytes: usize,
    ) -> TcpOrderedReadProgressSnapshot {
        TcpOrderedReadProgressSnapshot {
            reader,
            stream,
            read_offset,
            next_received_offset,
            highest_received_offset,
            buffered_bytes,
            ordered_gap_bytes: next_received_offset
                .unwrap_or(read_offset)
                .saturating_sub(read_offset),
        }
    }

    #[test]
    fn recovery_evidence_observes_persistent_ordered_gap_without_endpoint_action() {
        let started = Instant::now();
        let mut recovery = EndpointRecoveryState::default();
        let mut evidence = RecoveryEvidenceObserver::default();
        let sample =
            |read_offset, next_received_offset, highest_received_offset, buffered_bytes| {
                let mut connection = endpoint_recovery_connection(11, 0, None);
                connection.tcp_ordered_read_progress = vec![endpoint_recovery_ordered_progress(
                    7,
                    21,
                    read_offset,
                    next_received_offset,
                    highest_received_offset,
                    buffered_bytes,
                )];
                endpoint_recovery_path_sample(vec![connection])
            };

        let first_gap = sample(569_624_937, Some(569_626_217), 569_721_669, 96_732);
        assert!(evidence.observe(started, &first_gap).is_empty());
        assert_eq!(
            recovery.observe(started, &first_gap),
            EndpointRecoveryAction::None,
            "read-only evidence cannot authorize Endpoint mutation"
        );
        let growing_tail = sample(569_624_937, Some(569_626_217), 570_721_669, 1_096_732);
        let events = evidence.observe(started + ENDPOINT_RECOVERY_SAMPLE_INTERVAL, &growing_tail);
        assert_eq!(events.len(), 1);
        assert!(matches!(
            events[0],
            RecoveryEvidence::OrderedGapObserved {
                stable_id: 11,
                reader: 7,
                stream: 21,
                episode: 1,
                read_offset: 569_624_937,
                next_received_offset: 569_626_217,
                initial_highest_received_offset: 569_721_669,
                current_highest_received_offset: 570_721_669,
                initial_buffered_bytes: 96_732,
                current_buffered_bytes: 1_096_732,
                ordered_gap_bytes: 1_280,
                observations: 2,
                tail_advanced: true,
                ..
            }
        ));
        assert_eq!(
            recovery.observe(started + ENDPOINT_RECOVERY_SAMPLE_INTERVAL, &growing_tail,),
            EndpointRecoveryAction::None,
            "even a persistent gap with a growing tail remains observation-only"
        );
        assert!(
            evidence
                .observe(
                    started + ENDPOINT_RECOVERY_SAMPLE_INTERVAL * 2,
                    &growing_tail,
                )
                .is_empty()
        );

        let progressed = sample(570_721_669, None, 570_721_669, 0);
        assert!(
            evidence
                .observe(started + ENDPOINT_RECOVERY_SAMPLE_INTERVAL * 3, &progressed,)
                .is_empty()
        );

        let later_gap = sample(700_000_000, Some(700_001_280), 700_080_000, 80_000);
        assert!(
            evidence
                .observe(started + ENDPOINT_RECOVERY_SAMPLE_INTERVAL * 4, &later_gap,)
                .is_empty(),
            "a later gap must establish fresh observation ownership"
        );
        assert!(matches!(
            evidence
                .observe(started + ENDPOINT_RECOVERY_SAMPLE_INTERVAL * 5, &later_gap,)
                .as_slice(),
            [RecoveryEvidence::OrderedGapObserved { episode: 2, .. }]
        ));
    }

    #[test]
    fn recovery_evidence_aggregates_writer_ack_progress_until_episode_end() {
        let started = Instant::now();
        let mut evidence = RecoveryEvidenceObserver::default();
        let sample = |pressure| {
            endpoint_recovery_path_sample(vec![endpoint_recovery_connection(11, 0, pressure)])
        };

        let first = sample(Some(endpoint_recovery_pending(
            3,
            7,
            Duration::from_millis(250),
            Duration::from_millis(250),
            1_000,
        )));
        assert!(matches!(
            evidence.observe(started, &first).as_slice(),
            [RecoveryEvidence::TcpWritePressureStarted {
                stable_id: 11,
                writer: 3,
                stream: 7,
                episode: 7,
                acknowledged_bytes: 1_000,
                ..
            }]
        ));

        let progressed = sample(Some(endpoint_recovery_pending(
            3,
            7,
            Duration::from_millis(500),
            Duration::from_millis(50),
            65_000,
        )));
        assert!(
            evidence
                .observe(started + ENDPOINT_RECOVERY_SAMPLE_INTERVAL, &progressed)
                .is_empty()
        );

        let ended = sample(None);
        assert!(matches!(
            evidence
                .observe(started + ENDPOINT_RECOVERY_SAMPLE_INTERVAL * 2, &ended)
                .as_slice(),
            [RecoveryEvidence::TcpWritePressureEnded {
                stable_id: 11,
                writer: 3,
                stream: 7,
                episode: 7,
                observations: 2,
                initial_acknowledged_bytes: 1_000,
                final_acknowledged_bytes: 65_000,
                ack_progress_observations: 1,
                max_pending_for,
                max_ack_stalled_for,
                ..
            }] if *max_pending_for == Duration::from_millis(500)
                && *max_ack_stalled_for == Duration::from_millis(250)
        ));
    }

    #[test]
    fn recovery_evidence_does_not_merge_writer_identity_or_episode_replacement() {
        let started = Instant::now();
        let mut evidence = RecoveryEvidenceObserver::default();
        let input = |stable_id, episode, acknowledged_bytes| {
            endpoint_recovery_path_sample(vec![endpoint_recovery_connection(
                stable_id,
                0,
                Some(endpoint_recovery_pending(
                    3,
                    episode,
                    Duration::from_millis(250),
                    Duration::from_millis(250),
                    acknowledged_bytes,
                )),
            )])
        };

        assert_eq!(evidence.observe(started, &input(11, 7, 1_000)).len(), 1);
        let episode_events = evidence.observe(
            started + ENDPOINT_RECOVERY_SAMPLE_INTERVAL,
            &input(11, 8, 2_000),
        );
        assert_eq!(episode_events.len(), 2);
        assert!(matches!(
            episode_events[0],
            RecoveryEvidence::TcpWritePressureEnded {
                stable_id: 11,
                episode: 7,
                observations: 1,
                ..
            }
        ));
        assert!(matches!(
            episode_events[1],
            RecoveryEvidence::TcpWritePressureStarted {
                stable_id: 11,
                episode: 8,
                acknowledged_bytes: 2_000,
                ..
            }
        ));

        let identity_events = evidence.observe(
            started + ENDPOINT_RECOVERY_SAMPLE_INTERVAL * 2,
            &input(12, 9, 3_000),
        );
        assert_eq!(identity_events.len(), 2);
        assert!(matches!(
            identity_events[0],
            RecoveryEvidence::TcpWritePressureEnded {
                stable_id: 11,
                episode: 8,
                observations: 1,
                ..
            }
        ));
        assert!(matches!(
            identity_events[1],
            RecoveryEvidence::TcpWritePressureStarted {
                stable_id: 12,
                episode: 9,
                acknowledged_bytes: 3_000,
                ..
            }
        ));
    }

    #[test]
    fn endpoint_recovery_writer_ack_stall_is_unchanged_by_ordered_gap_evidence() {
        let started = Instant::now();
        let rtt = Duration::from_millis(159);
        let bound = endpoint_recovery_stall_bound(rtt);
        let mut state = EndpointRecoveryState::default();
        let mut connection = endpoint_recovery_connection(
            11,
            0,
            Some(endpoint_recovery_pending(3, 7, bound, bound, 0)),
        );
        connection.tcp_ordered_read_progress = vec![endpoint_recovery_ordered_progress(
            9,
            21,
            569_624_937,
            Some(569_626_217),
            569_721_669,
            96_732,
        )];
        let sample = endpoint_recovery_path_sample(vec![connection]);

        assert!(matches!(
            state.observe(started, &sample),
            EndpointRecoveryAction::Rebind {
                trigger: EndpointRecoveryTrigger::TcpWriteStall {
                    stable_id: 11,
                    writer: 3,
                    episode: 7,
                    ..
                },
                ..
            }
        ));
    }

    #[test]
    fn endpoint_recovery_resets_only_the_pending_connection_on_black_hole_advance() {
        let started = Instant::now();
        let rtt = Duration::from_millis(159);
        let bound = endpoint_recovery_stall_bound(rtt);
        let mut state = EndpointRecoveryState::default();

        let baseline = endpoint_recovery_path_sample(vec![
            endpoint_recovery_connection(11, 0, None),
            endpoint_recovery_connection(12, 0, None),
        ]);
        assert_eq!(
            state.observe(started, &baseline),
            EndpointRecoveryAction::None
        );

        let degraded = endpoint_recovery_path_sample(vec![
            endpoint_recovery_connection(
                11,
                1,
                Some(endpoint_recovery_pending(
                    7,
                    9,
                    bound,
                    Duration::from_millis(250),
                    64 * 1024,
                )),
            ),
            endpoint_recovery_connection(
                12,
                0,
                Some(endpoint_recovery_pending(
                    8,
                    10,
                    bound + Duration::from_secs(1),
                    Duration::from_millis(250),
                    128 * 1024,
                )),
            ),
        ]);
        let action = state.observe(started + bound, &degraded);
        assert!(
            matches!(
                action,
                EndpointRecoveryAction::ResetConnectionPath {
                    generation: 1,
                    trigger: EndpointRecoveryTrigger::TcpPathDegraded {
                        stable_id: 11,
                        writer: 7,
                        stream: 15,
                        episode: 9,
                        acknowledged_bytes: 65_536,
                        black_hole_anchor: 0,
                        black_holes_current: 1,
                        ..
                    },
                    stall_bound,
                    max_rtt,
                } if stall_bound == bound && max_rtt == rtt
            ),
            "only the connection with current writer ownership plus black-hole advance may reset: {action:?}"
        );
    }

    #[test]
    fn endpoint_recovery_path_reset_requires_same_pending_window_black_hole_advance() {
        let started = Instant::now();
        let bound = endpoint_recovery_stall_bound(Duration::from_millis(159));
        let mut state = EndpointRecoveryState::default();

        assert_eq!(
            state.observe(
                started,
                &endpoint_recovery_path_sample(vec![endpoint_recovery_connection(11, 0, None)])
            ),
            EndpointRecoveryAction::None
        );
        assert_eq!(
            state.observe(
                started + Duration::from_millis(250),
                &endpoint_recovery_path_sample(vec![endpoint_recovery_connection(11, 1, None)])
            ),
            EndpointRecoveryAction::None,
            "an idle black-hole increment must only refresh the anchor"
        );
        assert_eq!(
            state.observe(
                started + bound,
                &endpoint_recovery_path_sample(vec![endpoint_recovery_connection(
                    11,
                    1,
                    Some(endpoint_recovery_pending(
                        7,
                        9,
                        bound,
                        Duration::from_millis(250),
                        65_536,
                    )),
                )])
            ),
            EndpointRecoveryAction::None,
            "Pending and ACK progress without a same-window black-hole increment is ordinary backpressure"
        );
        assert!(matches!(
            state.observe(
                started + bound + Duration::from_millis(250),
                &endpoint_recovery_path_sample(vec![endpoint_recovery_connection(
                    11,
                    2,
                    Some(endpoint_recovery_pending(
                        7,
                        9,
                        bound + Duration::from_millis(250),
                        Duration::from_millis(250),
                        131_072,
                    )),
                )])
            ),
            EndpointRecoveryAction::ResetConnectionPath { .. }
        ));
    }

    #[test]
    fn endpoint_recovery_path_reset_is_once_per_stable_identity() {
        let started = Instant::now();
        let bound = endpoint_recovery_stall_bound(Duration::from_millis(159));
        let mut state = EndpointRecoveryState::default();
        let idle = |stable_id, black_holes_detected| {
            endpoint_recovery_path_sample(vec![endpoint_recovery_connection(
                stable_id,
                black_holes_detected,
                None,
            )])
        };
        let pending = |stable_id, black_holes_detected, writer, episode| {
            endpoint_recovery_path_sample(vec![endpoint_recovery_connection(
                stable_id,
                black_holes_detected,
                Some(endpoint_recovery_pending(
                    writer,
                    episode,
                    bound,
                    Duration::from_millis(250),
                    64 * 1024,
                )),
            )])
        };

        assert_eq!(
            state.observe(started, &idle(11, 0)),
            EndpointRecoveryAction::None
        );
        assert!(matches!(
            state.observe(started + bound, &pending(11, 1, 7, 9)),
            EndpointRecoveryAction::ResetConnectionPath { generation: 1, .. }
        ));
        assert_eq!(
            state.observe(started + bound + Duration::from_millis(250), &idle(11, 1)),
            EndpointRecoveryAction::None
        );
        assert_eq!(
            state.observe(started + bound * 2, &pending(11, 2, 8, 10)),
            EndpointRecoveryAction::None,
            "a stable identity cannot enter a path-reset loop across writer episodes"
        );

        assert_eq!(
            state.observe(
                started + bound * 2 + Duration::from_millis(250),
                &idle(12, 0)
            ),
            EndpointRecoveryAction::None,
            "identity replacement starts with observation, not an inherited action"
        );
        assert!(matches!(
            state.observe(started + bound * 3, &pending(12, 1, 9, 11)),
            EndpointRecoveryAction::ResetConnectionPath { generation: 2, .. }
        ));
    }

    #[test]
    fn endpoint_recovery_counter_regression_is_not_path_reset_authority() {
        let started = Instant::now();
        let bound = endpoint_recovery_stall_bound(Duration::from_millis(159));
        let mut state = EndpointRecoveryState::default();
        assert_eq!(
            state.observe(
                started,
                &endpoint_recovery_path_sample(vec![endpoint_recovery_connection(11, 5, None)])
            ),
            EndpointRecoveryAction::None
        );
        assert_eq!(
            state.observe(
                started + bound,
                &endpoint_recovery_path_sample(vec![endpoint_recovery_connection(
                    11,
                    4,
                    Some(endpoint_recovery_pending(
                        7,
                        9,
                        bound,
                        Duration::from_millis(250),
                        65_536,
                    )),
                )])
            ),
            EndpointRecoveryAction::None,
            "a regressed cumulative counter is unknown evidence and must fail closed"
        );
    }

    #[test]
    fn endpoint_recovery_exact_ack_stall_precedes_path_reset() {
        let started = Instant::now();
        let bound = endpoint_recovery_stall_bound(Duration::from_millis(159));
        let mut state = EndpointRecoveryState::default();
        assert_eq!(
            state.observe(
                started,
                &endpoint_recovery_path_sample(vec![endpoint_recovery_connection(11, 0, None)])
            ),
            EndpointRecoveryAction::None
        );
        let action = state.observe(
            started + bound,
            &endpoint_recovery_path_sample(vec![endpoint_recovery_connection(
                11,
                1,
                Some(endpoint_recovery_pending(7, 9, bound, bound, 0)),
            )]),
        );
        assert!(matches!(
            action,
            EndpointRecoveryAction::Rebind {
                trigger: EndpointRecoveryTrigger::TcpWriteStall { stable_id: 11, .. },
                ..
            }
        ));
    }

    #[test]
    fn endpoint_recovery_does_not_overlap_path_reset_with_rebind_recovery() {
        let started = Instant::now();
        let bound = endpoint_recovery_stall_bound(Duration::from_millis(159));
        let mut state = EndpointRecoveryState::default();
        assert_eq!(
            state.observe(
                started,
                &endpoint_recovery_path_sample(vec![endpoint_recovery_connection(11, 0, None)])
            ),
            EndpointRecoveryAction::None
        );
        let hard_stall = endpoint_recovery_path_sample(vec![endpoint_recovery_connection(
            11,
            1,
            Some(endpoint_recovery_pending(7, 9, bound, bound, 0)),
        )]);
        assert!(matches!(
            state.observe(started + bound, &hard_stall),
            EndpointRecoveryAction::Rebind { .. }
        ));
        state.note_rebind_succeeded(started + bound, 1);

        let mut waiting_for_current_socket =
            endpoint_recovery_path_sample(vec![endpoint_recovery_connection(
                11,
                2,
                Some(endpoint_recovery_pending(
                    7,
                    9,
                    bound + Duration::from_millis(250),
                    bound + Duration::from_millis(250),
                    0,
                )),
            )]);
        waiting_for_current_socket.current_socket_rx_rebind_generation = 1;
        assert_eq!(
            state.observe(
                started + bound + Duration::from_millis(250),
                &waiting_for_current_socket,
            ),
            EndpointRecoveryAction::None,
            "an in-flight Endpoint rebind owns recovery until every sampled connection authenticates the current socket"
        );
    }

    #[test]
    fn endpoint_recovery_rebinds_on_continuous_tcp_write_stall_despite_rx_progress() {
        let started = Instant::now();
        let rtt = Duration::from_millis(159);
        let mut state = EndpointRecoveryState::default();
        let sample = |rx_bytes, pending_for| EndpointRecoveryInput {
            active_tcp: 2,
            udp_active: false,
            last_udp_activity_secs: 0,
            current_socket_rx_rebind_generation: 0,
            connections: vec![EndpointRecoveryConnectionSample {
                stable_id: 11,
                tx_bytes: 100_000,
                rx_bytes,
                rtt,
                black_holes_detected: 0,
                current_socket_rx_rebind_generation: 0,
                tcp_write_pressures: vec![TcpWritePressureSnapshot {
                    writer: 1,
                    stream: 3,
                    episode: 7,
                    pending_for,
                    ack_stalled_for: pending_for,
                    acknowledged_bytes: 0,
                }],
                tcp_ordered_read_progress: Vec::new(),
            }],
        };

        assert_eq!(
            state.observe(started, &sample(1_000, Duration::ZERO)),
            EndpointRecoveryAction::None
        );
        let bound = endpoint_recovery_stall_bound(rtt);
        let action = state.observe(started + bound, &sample(2_000, bound));
        assert!(
            matches!(
                action,
                EndpointRecoveryAction::Rebind {
                    trigger: EndpointRecoveryTrigger::TcpWriteStall {
                        stable_id: 11,
                        episode: 7,
                        ..
                    },
                    ..
                }
            ),
            "continuous business-stream write pressure must survive unrelated/ACK RX: {action:?}"
        );
    }

    #[test]
    fn endpoint_recovery_does_not_rebind_while_pending_stream_acknowledges_progress() {
        let started = Instant::now();
        let mut state = EndpointRecoveryState::default();
        let input = EndpointRecoveryInput {
            active_tcp: 2,
            udp_active: false,
            last_udp_activity_secs: 0,
            current_socket_rx_rebind_generation: 0,
            connections: vec![EndpointRecoveryConnectionSample {
                stable_id: 11,
                tx_bytes: 100_000,
                rx_bytes: 2_000,
                rtt: Duration::from_millis(159),
                black_holes_detected: 0,
                current_socket_rx_rebind_generation: 0,
                tcp_write_pressures: vec![TcpWritePressureSnapshot {
                    writer: 1,
                    stream: 3,
                    episode: 7,
                    pending_for: Duration::from_secs(5),
                    ack_stalled_for: Duration::from_millis(250),
                    acknowledged_bytes: 64 * 1024,
                }],
                tcp_ordered_read_progress: Vec::new(),
            }],
        };

        assert_eq!(
            state.observe(started, &input),
            EndpointRecoveryAction::None,
            "application backpressure with business-stream ACK progress is not a path black hole"
        );
    }

    #[test]
    fn endpoint_recovery_one_rebind_covers_every_current_tcp_write_episode() {
        let started = Instant::now();
        let bound = Duration::from_secs(2);
        let mut state = EndpointRecoveryState::default();
        let sample = |socket_generation| EndpointRecoveryInput {
            active_tcp: 2,
            udp_active: false,
            last_udp_activity_secs: 0,
            current_socket_rx_rebind_generation: socket_generation,
            connections: vec![
                EndpointRecoveryConnectionSample {
                    stable_id: 11,
                    tx_bytes: 100_000,
                    rx_bytes: 1_000 + socket_generation,
                    rtt: Duration::from_millis(100),
                    black_holes_detected: 0,
                    current_socket_rx_rebind_generation: socket_generation,
                    tcp_write_pressures: vec![TcpWritePressureSnapshot {
                        writer: 1,
                        stream: 3,
                        episode: 7,
                        pending_for: bound,
                        ack_stalled_for: bound,
                        acknowledged_bytes: 0,
                    }],
                    tcp_ordered_read_progress: Vec::new(),
                },
                EndpointRecoveryConnectionSample {
                    stable_id: 12,
                    tx_bytes: 200_000,
                    rx_bytes: 2_000 + socket_generation,
                    rtt: Duration::from_millis(150),
                    black_holes_detected: 0,
                    current_socket_rx_rebind_generation: socket_generation,
                    tcp_write_pressures: vec![TcpWritePressureSnapshot {
                        writer: 2,
                        stream: 5,
                        episode: 9,
                        pending_for: bound,
                        ack_stalled_for: bound,
                        acknowledged_bytes: 0,
                    }],
                    tcp_ordered_read_progress: Vec::new(),
                },
            ],
        };

        let action = state.observe(started, &sample(0));
        assert!(matches!(action, EndpointRecoveryAction::Rebind { .. }));
        state.note_rebind_succeeded(started, 1);
        assert!(matches!(
            state.observe(started + Duration::from_millis(250), &sample(1)),
            EndpointRecoveryAction::Recovered { .. }
        ));
        assert_eq!(
            state.observe(started + Duration::from_millis(500), &sample(1)),
            EndpointRecoveryAction::None,
            "one Endpoint migration must cover every write episode already pending at rebind time"
        );

        let cleared = EndpointRecoveryInput {
            active_tcp: 2,
            udp_active: false,
            last_udp_activity_secs: 0,
            current_socket_rx_rebind_generation: 1,
            connections: sample(1)
                .connections
                .into_iter()
                .map(|mut connection| {
                    connection.tcp_write_pressures.clear();
                    connection
                })
                .collect(),
        };
        assert_eq!(
            state.observe(started + Duration::from_millis(750), &cleared),
            EndpointRecoveryAction::None
        );
        let mut next_episode = sample(1);
        next_episode.connections[0].tcp_write_pressures.clear();
        next_episode.connections[1].tcp_write_pressures = vec![TcpWritePressureSnapshot {
            writer: 3,
            stream: 7,
            episode: 10,
            pending_for: bound,
            ack_stalled_for: bound,
            acknowledged_bytes: 0,
        }];
        assert!(matches!(
            state.observe(started + Duration::from_secs(3), &next_episode),
            EndpointRecoveryAction::Rebind {
                generation: 2,
                trigger: EndpointRecoveryTrigger::TcpWriteStall {
                    stable_id: 12,
                    episode: 10,
                    ..
                },
                ..
            }
        ));
    }

    #[test]
    fn endpoint_recovery_one_rebind_covers_all_pending_writers_on_one_connection() {
        let started = Instant::now();
        let bound = Duration::from_secs(2);
        let mut state = EndpointRecoveryState::default();
        let sample = |socket_generation| EndpointRecoveryInput {
            active_tcp: 2,
            udp_active: false,
            last_udp_activity_secs: 0,
            current_socket_rx_rebind_generation: socket_generation,
            connections: vec![EndpointRecoveryConnectionSample {
                stable_id: 11,
                tx_bytes: 100_000,
                rx_bytes: 1_000 + socket_generation,
                rtt: Duration::from_millis(100),
                black_holes_detected: 0,
                current_socket_rx_rebind_generation: socket_generation,
                tcp_write_pressures: vec![
                    TcpWritePressureSnapshot {
                        writer: 1,
                        stream: 3,
                        episode: 7,
                        pending_for: bound,
                        ack_stalled_for: bound,
                        acknowledged_bytes: 0,
                    },
                    TcpWritePressureSnapshot {
                        writer: 2,
                        stream: 5,
                        episode: 8,
                        pending_for: bound,
                        ack_stalled_for: bound,
                        acknowledged_bytes: 0,
                    },
                ],
                tcp_ordered_read_progress: Vec::new(),
            }],
        };

        assert!(matches!(
            state.observe(started, &sample(0)),
            EndpointRecoveryAction::Rebind { .. }
        ));
        state.note_rebind_succeeded(started, 1);
        assert!(matches!(
            state.observe(started + Duration::from_millis(250), &sample(1)),
            EndpointRecoveryAction::Recovered { .. }
        ));
        assert_eq!(
            state.observe(started + Duration::from_millis(500), &sample(1)),
            EndpointRecoveryAction::None,
            "one socket migration covers every writer sampled on that connection"
        );
    }

    #[test]
    fn tcp_write_pressure_selects_the_oldest_per_stream_ack_stall() {
        let started = Instant::now();
        let pressure = Arc::new(TcpWritePressure::new(started));
        let mut first = pressure.writer(3);
        let mut second = pressure.writer(5);

        first.note_pending_at(started, 0);
        let first_snapshot = pressure
            .snapshot_at(started + Duration::from_secs(1))
            .expect("first pending writer starts an episode");
        assert_eq!(first_snapshot.episode, 1);
        assert_eq!(first_snapshot.pending_for, Duration::from_secs(1));

        second.note_pending_at(started + Duration::from_millis(500), 0);
        first.note_ready();
        assert_eq!(
            pressure
                .snapshot_at(started + Duration::from_secs(2))
                .expect("second writer still owns the shared episode"),
            TcpWritePressureSnapshot {
                writer: 2,
                stream: 5,
                episode: 2,
                pending_for: Duration::from_millis(1500),
                ack_stalled_for: Duration::from_millis(1500),
                acknowledged_bytes: 0,
            }
        );

        drop(second);
        assert_eq!(pressure.snapshot_at(started + Duration::from_secs(2)), None);

        let mut third = pressure.writer(7);
        third.note_pending_at(started + Duration::from_secs(3), 0);
        assert_eq!(
            pressure.snapshot_at(started + Duration::from_secs(4)),
            Some(TcpWritePressureSnapshot {
                writer: 3,
                stream: 7,
                episode: 3,
                pending_for: Duration::from_secs(1),
                ack_stalled_for: Duration::from_secs(1),
                acknowledged_bytes: 0,
            })
        );
    }

    #[test]
    fn tcp_write_pressure_ack_progress_resets_stall_without_clearing_pending() {
        let started = Instant::now();
        let pressure = Arc::new(TcpWritePressure::new(started));
        let mut writer = pressure.writer(3);

        writer.note_pending_at(started, 0);
        writer.note_acknowledged_at(started + Duration::from_secs(2), 64 * 1024);

        assert_eq!(
            pressure
                .snapshot_at(started + Duration::from_millis(2250))
                .expect("the writer remains application-blocked")
                .ack_stalled_for,
            Duration::from_millis(250),
            "acknowledged business-stream bytes reset only the recovery-stall clock"
        );
    }

    #[tokio::test]
    async fn tcp_write_pressure_adapter_tracks_async_write_pending_edges() {
        struct GateWriter {
            ready: Arc<AtomicBool>,
        }

        impl AsyncWrite for GateWriter {
            fn poll_write(
                self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                buf: &[u8],
            ) -> Poll<io::Result<usize>> {
                if self.ready.load(Ordering::Acquire) {
                    Poll::Ready(Ok(buf.len()))
                } else {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }

            fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
                Poll::Ready(Ok(()))
            }

            fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
                Poll::Ready(Ok(()))
            }
        }

        let pressure = Arc::new(TcpWritePressure::new(Instant::now()));
        let ready = Arc::new(AtomicBool::new(false));
        let mut writer = TcpWritePressureAdapter::new(
            GateWriter {
                ready: ready.clone(),
            },
            pressure.writer(3),
        );
        let write = tokio::spawn(async move { writer.write_all(b"pending").await });
        tokio::task::yield_now().await;
        assert!(
            pressure.snapshot_at(Instant::now()).is_some(),
            "a real AsyncWrite Pending edge must arm connection pressure"
        );

        ready.store(true, Ordering::Release);
        write.await.expect("writer task").expect("write completes");
        assert_eq!(pressure.snapshot_at(Instant::now()), None);
    }

    #[tokio::test]
    async fn tcp_ordered_read_pressure_samples_live_reader_and_releases_on_drop() {
        let (server_endpoint, client_endpoint, server_addr) = d16_quinn_test_endpoints();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let server_task = tokio::spawn(async move {
            let connection = server_endpoint.accept().await.unwrap().await.unwrap();
            let (mut send, mut recv) = connection.accept_bi().await.unwrap();
            let mut marker = [0u8; 1];
            recv.read_exact(&mut marker).await.unwrap();
            send.write_all(b"buffered ordered bytes").await.unwrap();
            send.finish().unwrap();
            let _ = release_rx.await;
        });

        let connection = client_endpoint
            .connect(server_addr, "example.com")
            .unwrap()
            .await
            .unwrap();
        let (mut send, recv) = connection.open_bi().await.unwrap();
        let stream = recv.id().index();
        let pressure = Arc::new(TcpOrderedReadPressure::new());
        let reader = pressure.reader(stream, recv.progress_handle());
        send.write_all(&[0x5a]).await.unwrap();

        let snapshots = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let snapshots = pressure.snapshots();
                if !snapshots.is_empty() {
                    break snapshots;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("live ordered reader progress");
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].reader, 1);
        assert_eq!(snapshots[0].stream, stream);
        assert_eq!(snapshots[0].read_offset, 0);
        assert_eq!(snapshots[0].next_received_offset, Some(0));
        assert_eq!(snapshots[0].ordered_gap_bytes, 0);

        drop(reader);
        assert!(pressure.snapshots().is_empty());
        let _ = release_tx.send(());
        server_task.await.unwrap();
        client_endpoint.close(0u32.into(), b"test complete");
    }

    #[test]
    fn endpoint_recovery_silent_tcp_transport_tx_does_not_arm_generic_no_rx() {
        let started = Instant::now();
        let rtt = Duration::from_millis(171);
        let mut state = EndpointRecoveryState::default();
        let sample = |tx_bytes| EndpointRecoveryInput {
            active_tcp: 2,
            udp_active: false,
            last_udp_activity_secs: 0,
            current_socket_rx_rebind_generation: 0,
            connections: vec![EndpointRecoveryConnectionSample {
                stable_id: 11,
                tx_bytes,
                rx_bytes: 1_000,
                rtt,
                black_holes_detected: 0,
                current_socket_rx_rebind_generation: 0,
                tcp_write_pressures: Vec::new(),
                tcp_ordered_read_progress: Vec::new(),
            }],
        };

        assert_eq!(
            state.observe(started, &sample(10_000)),
            EndpointRecoveryAction::None
        );
        assert_eq!(
            state.observe(started + Duration::from_millis(250), &sample(10_037)),
            EndpointRecoveryAction::None
        );
        assert_eq!(
            state.armed_at, None,
            "silent TCP ownership plus transport ACK/control TX is not application demand"
        );

        let bound = endpoint_recovery_stall_bound(rtt);
        assert_eq!(
            state.observe(
                started + bound + Duration::from_millis(250),
                &sample(10_074)
            ),
            EndpointRecoveryAction::None,
            "TCP-only transport TX without an exact writer stall must never rebind"
        );
    }

    #[test]
    fn endpoint_recovery_rebinds_once_after_active_tx_without_rx() {
        let started = Instant::now();
        let rtt = Duration::from_millis(164);
        let mut state = EndpointRecoveryState::default();

        let sample = |tx_bytes| EndpointRecoveryInput {
            active_tcp: 0,
            udp_active: true,
            last_udp_activity_secs: 1,
            current_socket_rx_rebind_generation: 0,
            connections: vec![EndpointRecoveryConnectionSample {
                stable_id: 11,
                tx_bytes,
                rx_bytes: 1_000,
                rtt,
                black_holes_detected: 0,
                current_socket_rx_rebind_generation: 0,
                tcp_write_pressures: Vec::new(),
                tcp_ordered_read_progress: Vec::new(),
            }],
        };

        assert_eq!(
            state.observe(started, &sample(10_000)),
            EndpointRecoveryAction::None
        );
        let armed_at = started + Duration::from_millis(250);
        assert_eq!(
            state.observe(armed_at, &sample(20_000)),
            EndpointRecoveryAction::None
        );

        let bound = endpoint_recovery_stall_bound(rtt);
        assert_eq!(bound, Duration::from_secs(2));
        let first = state.observe(armed_at + bound, &sample(30_000));
        assert!(
            matches!(first, EndpointRecoveryAction::Rebind { generation: 1, .. }),
            "expected one endpoint rebind, got {first:?}"
        );
        assert_eq!(
            state.observe(armed_at + bound + Duration::from_secs(5), &sample(40_000)),
            EndpointRecoveryAction::None,
            "one continuous no-RX episode must never create a rebind loop"
        );
    }

    #[test]
    fn endpoint_recovery_new_connection_clears_the_old_stall_episode() {
        let started = Instant::now();
        let mut state = EndpointRecoveryState::default();
        let sample = |stable_id, tx_bytes, rx_bytes| EndpointRecoveryInput {
            active_tcp: 0,
            udp_active: true,
            last_udp_activity_secs: 1,
            current_socket_rx_rebind_generation: 0,
            connections: vec![EndpointRecoveryConnectionSample {
                stable_id,
                tx_bytes,
                rx_bytes,
                rtt: Duration::from_millis(100),
                black_holes_detected: 0,
                current_socket_rx_rebind_generation: 0,
                tcp_write_pressures: Vec::new(),
                tcp_ordered_read_progress: Vec::new(),
            }],
        };

        assert_eq!(
            state.observe(started, &sample(11, 1_000, 1_000)),
            EndpointRecoveryAction::None
        );
        assert_eq!(
            state.observe(
                started + Duration::from_millis(250),
                &sample(11, 2_000, 1_000),
            ),
            EndpointRecoveryAction::None
        );

        assert_eq!(
            state.observe(started + Duration::from_secs(3), &sample(12, 500, 500),),
            EndpointRecoveryAction::None,
            "a newly authenticated connection proves receive progress on the endpoint"
        );
        assert_eq!(
            state.observe(started + Duration::from_secs(6), &sample(12, 500, 500),),
            EndpointRecoveryAction::None,
            "the old connection's no-RX deadline must not survive replacement"
        );
    }

    #[test]
    fn endpoint_recovery_requires_endpoint_wide_stall_and_rearms_on_rx() {
        let started = Instant::now();
        let mut state = EndpointRecoveryState::default();
        let sample = |active_tcp,
                      first_tx,
                      first_rx,
                      second_tx,
                      second_rx,
                      socket_rx_generation,
                      first_socket_rx_generation,
                      second_socket_rx_generation,
                      last_udp_activity_secs| {
            EndpointRecoveryInput {
                active_tcp,
                udp_active: true,
                last_udp_activity_secs,
                current_socket_rx_rebind_generation: socket_rx_generation,
                connections: vec![
                    EndpointRecoveryConnectionSample {
                        stable_id: 11,
                        tx_bytes: first_tx,
                        rx_bytes: first_rx,
                        rtt: Duration::from_millis(100),
                        black_holes_detected: 0,
                        current_socket_rx_rebind_generation: first_socket_rx_generation,
                        tcp_write_pressures: Vec::new(),
                        tcp_ordered_read_progress: Vec::new(),
                    },
                    EndpointRecoveryConnectionSample {
                        stable_id: 12,
                        tx_bytes: second_tx,
                        rx_bytes: second_rx,
                        rtt: Duration::from_millis(150),
                        black_holes_detected: 0,
                        current_socket_rx_rebind_generation: second_socket_rx_generation,
                        tcp_write_pressures: Vec::new(),
                        tcp_ordered_read_progress: Vec::new(),
                    },
                ],
            }
        };

        assert_eq!(
            state.observe(started, &sample(2, 100, 100, 100, 100, 0, 0, 0, 1)),
            EndpointRecoveryAction::None
        );
        assert_eq!(
            state.observe(
                started + Duration::from_millis(250),
                &sample(2, 200, 100, 100, 100, 0, 0, 0, 1),
            ),
            EndpointRecoveryAction::None
        );
        assert_eq!(
            state.observe(
                started + Duration::from_secs(3),
                &sample(2, 300, 100, 100, 101, 0, 0, 0, 1),
            ),
            EndpointRecoveryAction::None,
            "RX on either connection proves that the shared endpoint is alive"
        );

        let second_episode = started + Duration::from_secs(4);
        assert_eq!(
            state.observe(second_episode, &sample(2, 400, 100, 100, 101, 0, 0, 0, 2),),
            EndpointRecoveryAction::None
        );
        let action = state.observe(
            second_episode + Duration::from_secs(2),
            &sample(2, 500, 100, 100, 101, 0, 0, 0, 2),
        );
        assert!(matches!(
            action,
            EndpointRecoveryAction::Rebind { generation: 1, .. }
        ));
        state.note_rebind_succeeded(second_episode + Duration::from_secs(2), 1);
        assert_eq!(
            state.observe(
                second_episode + Duration::from_millis(2_050),
                &sample(2, 500, 101, 100, 101, 0, 0, 0, 2),
            ),
            EndpointRecoveryAction::None,
            "a packet on Quinn's retained old socket must not complete rebind recovery"
        );
        assert_eq!(
            state.observe(
                second_episode + Duration::from_millis(2_100),
                &sample(2, 500, 101, 100, 101, 1, 1, 0, 2),
            ),
            EndpointRecoveryAction::None,
            "one current-socket connection cannot prove that the other pooled connection migrated"
        );
        assert_eq!(
            state.observe(
                second_episode + Duration::from_millis(2_150),
                &sample(2, 500, 101, 100, 101, 1, 1, 1, 2),
            ),
            EndpointRecoveryAction::Recovered {
                generation: 1,
                socket_generation: 1,
                recovery_time: Duration::from_millis(150),
                connection_count: 2,
            }
        );

        assert_eq!(
            endpoint_recovery_stall_bound(Duration::from_millis(1)),
            Duration::from_secs(2)
        );
        assert_eq!(
            endpoint_recovery_stall_bound(Duration::from_millis(500)),
            Duration::from_secs(4)
        );
        assert_eq!(
            endpoint_recovery_stall_bound(Duration::from_secs(2)),
            Duration::from_secs(7)
        );
    }

    #[test]
    fn endpoint_recovery_does_not_arm_without_workload_or_tx_progress() {
        let started = Instant::now();
        let mut state = EndpointRecoveryState::default();
        let sample = |udp_active, tx_bytes| EndpointRecoveryInput {
            active_tcp: 0,
            udp_active,
            last_udp_activity_secs: u64::from(udp_active),
            current_socket_rx_rebind_generation: 0,
            connections: vec![EndpointRecoveryConnectionSample {
                stable_id: 11,
                tx_bytes,
                rx_bytes: 100,
                rtt: Duration::from_millis(100),
                black_holes_detected: 0,
                current_socket_rx_rebind_generation: 0,
                tcp_write_pressures: Vec::new(),
                tcp_ordered_read_progress: Vec::new(),
            }],
        };

        assert_eq!(
            state.observe(started, &sample(false, 100)),
            EndpointRecoveryAction::None
        );
        assert_eq!(
            state.observe(started + Duration::from_secs(3), &sample(false, 200)),
            EndpointRecoveryAction::None
        );
        assert_eq!(
            state.observe(started + Duration::from_secs(6), &sample(true, 200)),
            EndpointRecoveryAction::None,
            "old background TX must not arm a later workload"
        );
        assert_eq!(
            state.observe(started + Duration::from_secs(9), &sample(true, 200)),
            EndpointRecoveryAction::None,
            "an active but transport-idle workload must not churn the socket"
        );
    }

    #[test]
    fn address_ipv4() {
        let a = encode_address(&TargetAddr::IpPort("1.2.3.4:443".parse().unwrap()));
        assert_eq!(a, vec![0x01, 1, 2, 3, 4, 0x01, 0xBB]); // ATYP=IPv4, port 443
    }

    #[test]
    fn address_domain() {
        let a = encode_address(&TargetAddr::DomainPort {
            host: "ab.com".into(),
            port: 443,
        });
        assert_eq!(
            a,
            vec![0x00, 6, b'a', b'b', b'.', b'c', b'o', b'm', 0x01, 0xBB]
        );
    }

    #[test]
    fn address_ipv6() {
        let a = encode_address(&TargetAddr::IpPort("[::1]:53".parse().unwrap()));
        assert_eq!(a[0], 0x02);
        assert_eq!(a.len(), 1 + 16 + 2);
        assert_eq!(&a[17..19], &[0x00, 0x35]); // port 53
    }

    #[test]
    fn authenticate_layout() {
        let uuid = [0xABu8; 16];
        let token = [0xCDu8; 32];
        let c = encode_authenticate(&uuid, &token);
        assert_eq!(c.len(), 2 + 16 + 32);
        assert_eq!(&c[..2], &[0x05, 0x00]);
        assert_eq!(&c[2..18], &uuid);
        assert_eq!(&c[18..50], &token);
    }

    #[test]
    fn connect_prefixes_header() {
        let c = encode_connect(&TargetAddr::IpPort("1.2.3.4:443".parse().unwrap()));
        assert_eq!(&c[..2], &[0x05, 0x01]);
        assert_eq!(&c[2..], &[0x01, 1, 2, 3, 4, 0x01, 0xBB]);
    }

    #[test]
    fn domain_over_255_truncated_safely() {
        let host = "a".repeat(300);
        let a = encode_address(&TargetAddr::DomainPort { host, port: 80 });
        assert_eq!(a[0], 0x00);
        assert_eq!(a[1], 255); // length byte capped
        assert_eq!(a.len(), 1 + 1 + 255 + 2);
    }

    const UUID: &str = "550e8400-e29b-41d4-a716-446655440000";

    #[test]
    fn config_valid_with_defaults() {
        let c = TuicClientConfig::from_sources(
            Some("1.2.3.4:8443"),
            Some(UUID),
            Some("secret123"),
            None,
            None,
            None,
        )
        .expect("valid");
        assert_eq!(c.server, "1.2.3.4:8443".parse().unwrap());
        assert_eq!(c.uuid[0], 0x55);
        assert_eq!(c.alpn, "h3"); // default
        assert_eq!(c.congestion_control, "cubic"); // 刀3.5 实测裁决：datagram 路径 Cubic 优于 BBR
        assert_eq!(c.udp_relay_mode, "native");
        assert_eq!(c.gso_policy, "enabled");
        assert_eq!(c.udp_send_service, "quinn");
        assert_eq!(c.pacing_policy, "quinn");
        assert_eq!(c.tcp_pool, 2);
        assert_eq!(c.quic_stats_secs, None);
        assert!(
            !c.zero_rtt,
            "0-RTT 默认关（quinn 0.10 在 0-RTT 不支持 keying-material 导出）"
        );
    }

    #[test]
    fn env_overrides_cc_and_udp_mode() {
        // 有值覆盖默认（A/B 切换：MINI_VPN_TUIC_CC=cubic / MINI_VPN_TUIC_UDP_MODE=quic）。
        assert_eq!(override_field("bbr".into(), Some("cubic".into())), "cubic");
        assert_eq!(override_field("native".into(), Some("quic".into())), "quic");
        // 无值 / 空白 → 保留默认。
        assert_eq!(override_field("bbr".into(), None), "bbr");
        assert_eq!(
            override_field("native".into(), Some("   ".into())),
            "native"
        );
    }

    #[test]
    fn tcp_pool_parse_defaults_and_clamps() {
        assert_eq!(parse_tcp_pool(None), 2);
        assert_eq!(parse_tcp_pool(Some("")), 2);
        assert_eq!(parse_tcp_pool(Some("0")), 1);
        assert_eq!(parse_tcp_pool(Some("nope")), 2);
        assert_eq!(parse_tcp_pool(Some("1")), 1);
        assert_eq!(parse_tcp_pool(Some("2")), 2);
        assert_eq!(parse_tcp_pool(Some("4")), 4);
        assert_eq!(parse_tcp_pool(Some("999")), MAX_TUIC_TCP_POOL);
    }

    #[test]
    fn quic_stats_secs_follows_tcp_diag_or_explicit_override() {
        assert_eq!(parse_quic_stats_secs(None, None, Some("5")), None);
        assert_eq!(parse_quic_stats_secs(None, Some("1"), Some("5")), Some(5));
        assert_eq!(
            parse_quic_stats_secs(None, Some("true"), Some("nope")),
            Some(DEFAULT_TUIC_QUIC_STATS_SECS)
        );
        assert_eq!(
            parse_quic_stats_secs(Some("7"), Some("0"), Some("5")),
            Some(7)
        );
        assert_eq!(parse_quic_stats_secs(Some("0"), Some("1"), Some("5")), None);
        assert_eq!(
            parse_quic_stats_secs(Some("nope"), Some("1"), Some("5")),
            None
        );
    }

    #[test]
    fn tuic_tcp_relay_mode_defaults_to_ordered_join_and_gates_diagnostics() {
        assert_eq!(
            parse_tuic_tcp_relay_mode(None, None),
            TuicTcpRelayMode::OrderedJoin
        );
        assert_eq!(
            parse_tuic_tcp_relay_mode(Some(""), Some("")),
            TuicTcpRelayMode::OrderedJoin
        );
        assert_eq!(
            parse_tuic_tcp_relay_mode(Some("0"), Some("0")),
            TuicTcpRelayMode::OrderedJoin
        );
        assert_eq!(
            parse_tuic_tcp_relay_mode(Some("false"), Some("false")),
            TuicTcpRelayMode::OrderedJoin
        );

        assert_eq!(
            parse_tuic_tcp_relay_mode(None, Some("1")),
            TuicTcpRelayMode::OrderedChunk
        );
        assert_eq!(
            parse_tuic_tcp_relay_mode(None, Some("true")),
            TuicTcpRelayMode::OrderedChunk
        );

        assert_eq!(
            parse_tuic_tcp_relay_mode(Some("1"), None),
            TuicTcpRelayMode::UnorderedReassembly
        );
        assert_eq!(
            parse_tuic_tcp_relay_mode(Some("true"), None),
            TuicTcpRelayMode::UnorderedReassembly
        );
        assert_eq!(
            parse_tuic_tcp_relay_mode(Some("on"), None),
            TuicTcpRelayMode::UnorderedReassembly
        );
        assert_eq!(
            parse_tuic_tcp_relay_mode(Some("yes"), None),
            TuicTcpRelayMode::UnorderedReassembly
        );
        assert_eq!(
            parse_tuic_tcp_relay_mode(Some("1"), Some("1")),
            TuicTcpRelayMode::UnorderedReassembly
        );
        assert_eq!(
            TuicTcpRelayMode::NativeChunkPump.as_str(),
            "native_chunk_pump_diag"
        );
        assert_eq!(
            TuicTcpRelayMode::NativeOrderedPump.as_str(),
            "native_ordered_pump_diag"
        );
    }

    #[test]
    fn d16_read_reservation_counts_before_remote_read() {
        let queue = AsyncLeasedByteFlowQueue::new_with_release_mode(
            512 * 1024,
            DownstreamPermitReleaseMode::OnEgressDrain,
        )
        .unwrap();
        assert_eq!(queue.blocking_snapshot_for_test().reserved_bytes, 0);
    }

    struct PendingOnceOrderedSource {
        calls: Arc<AtomicU64>,
        entered: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
        payload: bytes::Bytes,
    }

    #[async_trait::async_trait]
    impl TuicOrderedNativeChunkSource for PendingOnceOrderedSource {
        async fn read_ordered_native_chunk(
            &mut self,
            _max_len: usize,
        ) -> io::Result<Option<NativeTcpChunk>> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if call == 0 {
                self.entered.notify_one();
                self.release.notified().await;
                return Ok(Some(NativeTcpChunk {
                    offset: 0,
                    bytes: self.payload.clone(),
                }));
            }
            std::future::pending::<io::Result<Option<NativeTcpChunk>>>().await
        }
    }

    struct SequenceOrderedSource {
        calls: Arc<AtomicU64>,
        chunks: Vec<NativeTcpChunk>,
    }

    #[async_trait::async_trait]
    impl TuicOrderedNativeChunkSource for SequenceOrderedSource {
        async fn read_ordered_native_chunk(
            &mut self,
            _max_len: usize,
        ) -> io::Result<Option<NativeTcpChunk>> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst) as usize;
            if call < self.chunks.len() {
                Ok(Some(self.chunks[call].clone()))
            } else {
                Ok(None)
            }
        }
    }

    #[tokio::test]
    async fn native_ordered_pump_holds_pending_read_until_ready() {
        let calls = Arc::new(AtomicU64::new(0));
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let source = PendingOnceOrderedSource {
            calls: calls.clone(),
            entered: entered.clone(),
            release: release.clone(),
            payload: bytes::Bytes::from_static(b"ordered-pump"),
        };
        let active = Arc::new(AtomicU64::new(0));
        let (lease, reserved) = TcpPoolSlotLease::reserve(active.clone());
        assert!(reserved);
        let mut reader = TuicNativeOrderedPumpReader::spawn_from_source(source, lease, None, None);

        tokio::time::timeout(Duration::from_millis(200), entered.notified())
            .await
            .expect("read pump should enter the first source read");
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "the pump must hold the pending read future instead of starting another read"
        );

        release.notify_one();
        let chunk = tokio::time::timeout(Duration::from_millis(200), async {
            std::future::poll_fn(|cx| reader.poll_read_chunk(cx, 4096)).await
        })
        .await
        .expect("released ordered read should reach the native reader")
        .expect("pump read should succeed")
        .expect("released read should produce a chunk");
        assert_eq!(chunk.offset, 0);
        assert_eq!(&chunk.bytes[..], b"ordered-pump");

        drop(reader);
        assert_eq!(
            active.load(Ordering::SeqCst),
            0,
            "dropping the ordered pump reader must release its TCP pool lease"
        );
    }

    #[tokio::test]
    async fn native_ordered_pump_reader_batches_ready_chunks_to_max_len() {
        let calls = Arc::new(AtomicU64::new(0));
        let source = SequenceOrderedSource {
            calls: calls.clone(),
            chunks: vec![
                NativeTcpChunk {
                    offset: 0,
                    bytes: bytes::Bytes::from_static(b"ab"),
                },
                NativeTcpChunk {
                    offset: 2,
                    bytes: bytes::Bytes::from_static(b"cdef"),
                },
                NativeTcpChunk {
                    offset: 6,
                    bytes: bytes::Bytes::from_static(b"gh"),
                },
            ],
        };
        let active = Arc::new(AtomicU64::new(0));
        let (lease, reserved) = TcpPoolSlotLease::reserve(active.clone());
        assert!(reserved);
        let mut reader = TuicNativeOrderedPumpReader::spawn_from_source(source, lease, None, None);

        tokio::time::timeout(Duration::from_millis(200), async {
            while calls.load(Ordering::SeqCst) < 4 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("source should publish all ready chunks and EOF");

        let first = std::future::poll_fn(|cx| reader.poll_read_chunk(cx, 5))
            .await
            .expect("first batched read should succeed")
            .expect("first batched read should produce data");
        assert_eq!(first.offset, 0);
        assert_eq!(&first.bytes[..], b"abcde");

        let second = std::future::poll_fn(|cx| reader.poll_read_chunk(cx, 16))
            .await
            .expect("second batched read should succeed")
            .expect("second batched read should produce stashed remainder");
        assert_eq!(second.offset, 5);
        assert_eq!(&second.bytes[..], b"fgh");

        let eof = std::future::poll_fn(|cx| reader.poll_read_chunk(cx, 16))
            .await
            .expect("EOF read should succeed");
        assert!(eof.is_none(), "EOF must be delivered after stashed bytes");

        drop(reader);
        assert_eq!(active.load(Ordering::SeqCst), 0);
    }

    struct RecordingDirectOrderedRecv {
        max_lens: Arc<std::sync::Mutex<Vec<usize>>>,
        chunk: Option<NativeTcpChunk>,
    }

    impl DirectOrderedNativeChunkRecv for RecordingDirectOrderedRecv {
        fn poll_read_ordered_native_chunk(
            &mut self,
            _cx: &mut Context<'_>,
            max_len: usize,
        ) -> Poll<io::Result<Option<NativeTcpChunk>>> {
            self.max_lens.lock().unwrap().push(max_len);
            Poll::Ready(Ok(self.chunk.take()))
        }
    }

    struct PendingDirectOrderedRecv {
        starts: Arc<AtomicU64>,
        released: Arc<std::sync::atomic::AtomicBool>,
        in_flight: bool,
    }

    impl DirectOrderedNativeChunkRecv for PendingDirectOrderedRecv {
        fn poll_read_ordered_native_chunk(
            &mut self,
            _cx: &mut Context<'_>,
            _max_len: usize,
        ) -> Poll<io::Result<Option<NativeTcpChunk>>> {
            if !self.in_flight {
                self.in_flight = true;
                self.starts.fetch_add(1, Ordering::SeqCst);
            }
            if !self.released.load(Ordering::SeqCst) {
                return Poll::Pending;
            }
            self.in_flight = false;
            Poll::Ready(Ok(Some(NativeTcpChunk {
                offset: 0,
                bytes: bytes::Bytes::from_static(b"ready"),
            })))
        }
    }

    #[test]
    fn d16_direct_ordered_reader_forwards_exact_max_len() {
        let max_lens = Arc::new(std::sync::Mutex::new(Vec::new()));
        let recv = RecordingDirectOrderedRecv {
            max_lens: max_lens.clone(),
            chunk: Some(NativeTcpChunk {
                offset: 0,
                bytes: bytes::Bytes::from_static(b"direct"),
            }),
        };
        let active = Arc::new(AtomicU64::new(0));
        let (lease, reserved) = TcpPoolSlotLease::reserve(active.clone());
        assert!(reserved);
        let mut reader = TuicNativeOrderedReader::from_recv(recv, lease, None, None, None);
        let waker = std::task::Waker::noop();
        let mut cx = Context::from_waker(waker);

        let Poll::Ready(chunk) = reader.poll_read_chunk(&mut cx, 128 * 1024) else {
            panic!("recording direct reader should be ready");
        };
        let chunk = chunk
            .expect("recording direct reader should succeed")
            .expect("recording direct reader should return data");
        assert_eq!(&chunk.bytes[..], b"direct");
        assert_eq!(&*max_lens.lock().unwrap(), &[128 * 1024]);
    }

    #[test]
    fn h10d16_single_gate_selects_direct_ordered_native_relay() {
        assert_eq!(
            select_tuic_native_relay_mode(true, false, false),
            Some(TuicTcpRelayMode::D16DirectOrdered)
        );
        assert_eq!(
            select_tuic_native_relay_mode(true, true, true),
            Some(TuicTcpRelayMode::D16DirectOrdered),
            "the approved D16 gate must not depend on or lose to older diagnostic flags"
        );
        assert_eq!(select_tuic_native_relay_mode(false, false, false), None);
    }

    #[test]
    fn d16_direct_ordered_reader_never_returns_more_than_max_len() {
        let max_lens = Arc::new(std::sync::Mutex::new(Vec::new()));
        let recv = RecordingDirectOrderedRecv {
            max_lens: max_lens.clone(),
            chunk: Some(NativeTcpChunk {
                offset: 0,
                bytes: bytes::Bytes::from(vec![1; 16 * 1024 + 1]),
            }),
        };
        let active = Arc::new(AtomicU64::new(0));
        let (lease, reserved) = TcpPoolSlotLease::reserve(active);
        assert!(reserved);
        let mut reader = TuicNativeOrderedReader::from_recv(recv, lease, None, None, None);
        let waker = std::task::Waker::noop();
        let mut cx = Context::from_waker(waker);

        let Poll::Ready(result) = reader.poll_read_chunk(&mut cx, 16 * 1024) else {
            panic!("oversized direct chunk should be rejected immediately");
        };
        let err = result.expect_err("direct reader must reject bytes above max_len");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert_eq!(&*max_lens.lock().unwrap(), &[16 * 1024]);
    }

    #[tokio::test]
    async fn d16_direct_ordered_reader_keeps_one_pending_read_future() {
        let starts = Arc::new(AtomicU64::new(0));
        let released = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let recv = PendingDirectOrderedRecv {
            starts: starts.clone(),
            released: released.clone(),
            in_flight: false,
        };
        let active = Arc::new(AtomicU64::new(0));
        let (lease, reserved) = TcpPoolSlotLease::reserve(active);
        assert!(reserved);
        let mut reader = TuicNativeOrderedReader::from_recv(recv, lease, None, None, None);
        let waker = std::task::Waker::noop();
        let mut cx = Context::from_waker(waker);

        assert!(reader.poll_read_chunk(&mut cx, 128 * 1024).is_pending());
        assert!(reader.poll_read_chunk(&mut cx, 128 * 1024).is_pending());
        assert_eq!(
            starts.load(Ordering::SeqCst),
            1,
            "re-polling must not start a second concurrent ordered read"
        );

        released.store(true, Ordering::SeqCst);
        let Poll::Ready(chunk) = reader.poll_read_chunk(&mut cx, 128 * 1024) else {
            panic!("released direct read should complete");
        };
        assert_eq!(&chunk.unwrap().unwrap().bytes[..], b"ready");
        assert_eq!(starts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn d16_real_quinn_reader_wakes_after_delayed_ordered_payload() {
        let cert_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
        let mut server_cert_reader =
            std::io::BufReader::new(std::fs::File::open(cert_dir.join("server-cert.pem")).unwrap());
        let server_certs = rustls_pemfile::certs(&mut server_cert_reader)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let mut server_key_reader =
            std::io::BufReader::new(std::fs::File::open(cert_dir.join("server-key.pem")).unwrap());
        let server_key = rustls_pemfile::private_key(&mut server_key_reader)
            .unwrap()
            .unwrap();
        let server_config =
            quinn::ServerConfig::with_single_cert(server_certs, server_key).unwrap();
        let server_endpoint = quinn::Endpoint::server(
            server_config,
            "127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap(),
        )
        .unwrap();
        let server_addr = server_endpoint.local_addr().unwrap();

        let mut ca_reader =
            std::io::BufReader::new(std::fs::File::open(cert_dir.join("ca-cert.pem")).unwrap());
        let mut roots = rustls::RootCertStore::empty();
        for cert in rustls_pemfile::certs(&mut ca_reader) {
            roots.add(cert.unwrap()).unwrap();
        }
        let mut client_endpoint =
            quinn::Endpoint::client("127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap())
                .unwrap();
        client_endpoint.set_default_client_config(
            quinn::ClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
        );

        let reader_polled = Arc::new(tokio::sync::Notify::new());
        let server_reader_polled = reader_polled.clone();
        let (client_read_tx, client_read_rx) = tokio::sync::oneshot::channel();
        let server_task = tokio::spawn(async move {
            let connection = server_endpoint.accept().await.unwrap().await.unwrap();
            let (mut send, mut recv) = connection.accept_bi().await.unwrap();
            let mut marker = [0u8; 1];
            recv.read_exact(&mut marker).await.unwrap();
            assert_eq!(marker, [0x5a]);
            server_reader_polled.notified().await;
            tokio::time::sleep(Duration::from_millis(20)).await;
            send.write_all(b"delayed-quinn-payload").await.unwrap();
            send.finish().unwrap();
            let _ = client_read_rx.await;
        });

        let connection = client_endpoint
            .connect(server_addr, "example.com")
            .unwrap()
            .await
            .unwrap();
        let (mut send, recv) = connection.open_bi().await.unwrap();
        send.write_all(&[0x5a]).await.unwrap();

        let active = Arc::new(AtomicU64::new(0));
        let (lease, reserved) = TcpPoolSlotLease::reserve(active);
        assert!(reserved);
        let mut reader = TuicNativeOrderedReader::new(recv, lease, None, connection, None);
        let mut first_poll = true;
        let read = tokio::time::timeout(
            Duration::from_secs(1),
            std::future::poll_fn(|cx| {
                let poll = reader.poll_read_chunk(cx, 128 * 1024);
                if first_poll {
                    first_poll = false;
                    assert!(poll.is_pending(), "server has not released payload yet");
                    reader_polled.notify_one();
                }
                poll
            }),
        )
        .await
        .expect("real Quinn readability must wake the pending D16 reader")
        .unwrap()
        .unwrap();

        assert_eq!(&read.bytes[..], b"delayed-quinn-payload");
        let _ = client_read_tx.send(());
        server_task.await.unwrap();
        client_endpoint.close(0u32.into(), b"test complete");
    }

    #[tokio::test]
    async fn d16_real_quinn_reader_sustains_ordered_progress() {
        let _capacity_guard = crate::test_support::local_capacity_test_guard().await;
        const PAYLOAD_BYTES: usize = 32 * 1024 * 1024;
        const WRITE_BYTES: usize = 64 * 1024;

        let (server_endpoint, client_endpoint, server_addr) = d16_quinn_test_endpoints();

        let (client_read_tx, client_read_rx) = tokio::sync::oneshot::channel();
        let server_task = tokio::spawn(async move {
            let connection = server_endpoint.accept().await.unwrap().await.unwrap();
            let (mut send, mut recv) = connection.accept_bi().await.unwrap();
            let mut marker = [0u8; 1];
            recv.read_exact(&mut marker).await.unwrap();
            assert_eq!(marker, [0x5b]);
            let payload = vec![0x6d; WRITE_BYTES];
            for _ in 0..PAYLOAD_BYTES / WRITE_BYTES {
                send.write_all(&payload).await.unwrap();
            }
            send.finish().unwrap();
            let _ = client_read_rx.await;
        });

        let connection = client_endpoint
            .connect(server_addr, "example.com")
            .unwrap()
            .await
            .unwrap();
        let (mut send, recv) = connection.open_bi().await.unwrap();
        send.write_all(&[0x5b]).await.unwrap();

        let active = Arc::new(AtomicU64::new(0));
        let (lease, reserved) = TcpPoolSlotLease::reserve(active);
        assert!(reserved);
        let mut reader = TuicNativeOrderedReader::new(recv, lease, None, connection, None);
        let started = Instant::now();
        let mut last_progress = started;
        let mut max_progress_gap = Duration::ZERO;
        let mut received = 0usize;
        let mut reads = 0u64;
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let chunk = std::future::poll_fn(|cx| {
                    reader.poll_read_chunk(cx, TUIC_TCP_DIRECT_ORDERED_READ_MAX_BYTES)
                })
                .await
                .unwrap();
                let Some(chunk) = chunk else {
                    break;
                };
                let now = Instant::now();
                max_progress_gap =
                    max_progress_gap.max(now.saturating_duration_since(last_progress));
                last_progress = now;
                received = received.saturating_add(chunk.bytes.len());
                reads = reads.saturating_add(1);
            }
        })
        .await
        .expect("the direct ordered Quinn reader must not develop a multi-second service gap");
        let elapsed = started.elapsed();
        let receiver_mbps = received as f64 * 8.0 / elapsed.as_secs_f64() / 1_000_000.0;

        assert_eq!(received, PAYLOAD_BYTES);
        assert!(
            max_progress_gap < Duration::from_millis(250),
            "same-stream progress gap must stay sub-250ms: gap={max_progress_gap:?} reads={reads} rate={receiver_mbps:.1}M"
        );
        assert!(
            receiver_mbps >= 170.0,
            "same-stream direct reader must have plausible Gate B capacity: reads={reads} elapsed={elapsed:?} rate={receiver_mbps:.1}M"
        );
        let average_read_bytes = received as u64 / reads.max(1);
        assert!(
            average_read_bytes <= 4 * 1024,
            "the D16 reader must hand off cancellation-safe Quinn chunks without retaining a large application read buffer: reads={reads} average_read_bytes={average_read_bytes} rate={receiver_mbps:.1}M"
        );

        let _ = client_read_tx.send(());
        server_task.await.unwrap();
        client_endpoint.close(0u32.into(), b"test complete");
    }

    #[test]
    fn d16_direct_ordered_reader_rejects_offset_discontinuity() {
        let max_lens = Arc::new(std::sync::Mutex::new(Vec::new()));
        let recv = RecordingDirectOrderedRecv {
            max_lens,
            chunk: Some(NativeTcpChunk {
                offset: 7,
                bytes: bytes::Bytes::from_static(b"gap"),
            }),
        };
        let active = Arc::new(AtomicU64::new(0));
        let (lease, reserved) = TcpPoolSlotLease::reserve(active);
        assert!(reserved);
        let mut reader = TuicNativeOrderedReader::from_recv(recv, lease, None, None, None);
        let waker = std::task::Waker::noop();
        let mut cx = Context::from_waker(waker);

        let Poll::Ready(result) = reader.poll_read_chunk(&mut cx, 128 * 1024) else {
            panic!("offset discontinuity should be rejected immediately");
        };
        let err = result.expect_err("direct ordered reader must reject a gap");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("expected=0 actual=7"), "{err}");
    }

    #[test]
    fn native_ordered_chunk_batch_combines_contiguous_bytes_and_advances_offset() {
        let mut next_offset = 41;
        let first = combine_ordered_native_chunks(
            &mut next_offset,
            &[
                bytes::Bytes::from_static(b"ab"),
                bytes::Bytes::from_static(b"cde"),
            ],
        )
        .expect("nonempty chunk batch should combine");
        assert_eq!(first.offset, 41);
        assert_eq!(&first.bytes[..], b"abcde");
        assert_eq!(next_offset, 46);

        let second =
            combine_ordered_native_chunks(&mut next_offset, &[bytes::Bytes::from_static(b"fg")])
                .expect("single chunk batch should pass through");
        assert_eq!(second.offset, 46);
        assert_eq!(&second.bytes[..], b"fg");
        assert_eq!(next_offset, 48);

        assert!(combine_ordered_native_chunks(&mut next_offset, &[]).is_none());
        assert_eq!(next_offset, 48);
    }

    #[test]
    fn format_quic_stats_line_includes_flow_and_congestion_signals() {
        let line = format_quic_stats_line(
            2,
            99,
            QuicStatsSnapshot {
                rtt_ms: 181,
                cwnd: 65_535,
                lost_packets: 3,
                sent_packets: 100,
                lost_bytes: 4096,
                congestion_events: 2,
                sent_plpmtud_probes: 19,
                lost_plpmtud_probes: 20,
                black_holes_detected: 21,
                tx_ack_frames: 22,
                rx_ack_frames: 23,
                rx_stream_frames: 24,
                tx_data_blocked: 4,
                tx_stream_data_blocked: 5,
                tx_streams_blocked_bidi: 6,
                tx_streams_blocked_uni: 7,
                tx_max_data: 8,
                tx_max_stream_data: 9,
                rx_data_blocked: 10,
                rx_stream_data_blocked: 11,
                rx_max_data: 12,
                rx_max_stream_data: 13,
                udp_tx_datagrams: 14,
                udp_tx_bytes: 15,
                udp_rx_datagrams: 16,
                udp_rx_bytes: 17,
                datagram_max: Some(1375),
                datagram_send_buffer_space: 18,
                pacing_uncapped_capacity_bytes: 307_200,
                pacing_capacity_bytes: 76_800,
                pacing_tokens_bytes: 65_536,
                current_mtu: 1_375,
                pacing_mtu: 1_200,
                pacing_cap_active: true,
                pacing_delay_events: 25,
            },
        );
        assert!(line.contains("conn=2"), "{line}");
        assert!(line.contains("id=99"), "{line}");
        assert!(line.contains("rtt=181ms"), "{line}");
        assert!(line.contains("cwnd=65535"), "{line}");
        assert!(line.contains("lost=3/100"), "{line}");
        assert!(line.contains("congestion_events=2"), "{line}");
        assert!(
            line.contains("plpmtud(sent=19,lost=20,black_holes=21)"),
            "{line}"
        );
        assert!(
            line.contains("frames(rx_stream=24,rx_ack=23,tx_ack=22)"),
            "{line}"
        );
        assert!(line.contains("tx_blocked(data=4,stream=5"), "{line}");
        assert!(line.contains("rx_blocked(data=10,stream=11"), "{line}");
        assert!(
            line.contains("rx_window(max_data=12,max_stream_data=13"),
            "{line}"
        );
        assert!(line.contains("dg_max=Some(1375)"), "{line}");
        assert!(line.contains("dg_space=18B"), "{line}");
        assert!(line.contains("current_mtu=1375"), "{line}");
        assert!(line.contains("pacing_mtu=1200"), "{line}");
        assert!(
            line.contains(
                "pacing(uncapped=307200,capacity=76800,tokens=65536,current_mtu=1375,pacing_mtu=1200,cap_active=true,delay_events=25)"
            ),
            "{line}"
        );
    }

    #[test]
    fn format_udp_send_service_stats_line_includes_actual_service_timing() {
        let line = format_udp_send_service_stats_line(quic::QuicUdpSendServiceSnapshot {
            accepted_bytes: 512_000,
            accepted_datagrams: 400,
            accepted_bytes_per_sec: 25_600_000,
            accepted_datagrams_per_sec: 20_000,
            service_elapsed_ns: 20_000_000,
            gate_would_block: 3,
            batch_closes: 9,
            timer_blocked_polls: 10,
            cooldown_rearms: 8,
            total_rearm_lateness_ns: 4_000_000,
            max_rearm_lateness_ns: 1_500_000,
            wasted_datagrams: 7,
            inner_would_block: 2,
            inner_errors: 1,
            invalid_transmits: 0,
        });

        assert!(
            line.contains("service_rate=20000dg/s/25600000B/s"),
            "{line}"
        );
        assert!(line.contains("mean_payload=1280B"), "{line}");
        assert!(line.contains("batch_closes=9"), "{line}");
        assert!(line.contains("timer_blocked_polls=10"), "{line}");
        assert!(line.contains("mean_rearm_period_us=2500.000"), "{line}");
        assert!(
            line.contains("rearm_lateness_us(total=4000.000,mean=500.000,max=1500.000)"),
            "{line}"
        );
    }

    #[test]
    fn format_tuic_tcp_pool_selection_line_identifies_forward_qualification_policy() {
        let line = format_tuic_tcp_pool_selection_line(TuicTcpPoolSelectionDiag {
            conn_index: 1,
            stable_id: 42,
            active_before: 62,
            path_service: TcpPoolPathService::known(23_842, Duration::from_millis(163)),
            path_service_tiebreak: true,
            qualification: TcpPoolForwardQualification::Qualified,
            qualification_anchor: Some(0),
            black_holes_current: Some(0),
            qualification_override: true,
            all_degraded_fallback: false,
            candidates: &[TcpPoolAdmissionCandidate {
                index: 1,
                identity: Some(TcpPoolTransportIdentity::new(42, 7)),
                active_before: 62,
                path_service: TcpPoolPathService::known(23_842, Duration::from_millis(163)),
                qualification: TcpPoolForwardQualification::Qualified,
                qualification_anchor: Some(0),
                black_holes_current: Some(0),
                admitted: true,
            }],
            generation: 7,
            last_success_age_secs: Some(15),
            probe_result: "alive",
            reconnect_reason: Some("previous_open_failure"),
            replacement_installed: true,
        });

        assert!(line.contains("tuic-tcp-pool-selection"), "{line}");
        assert!(line.contains("conn=1 id=42"), "{line}");
        assert!(
            line.contains(
                "policy=busy_epoch_forward_qualification_then_least_active_then_path_service"
            ),
            "{line}"
        );
        assert!(line.contains("active_before=62"), "{line}");
        assert!(line.contains("path_cwnd=23842"), "{line}");
        assert!(line.contains("path_rtt_us=163000"), "{line}");
        assert!(line.contains("path_service_tiebreak=true"), "{line}");
        assert!(line.contains("qualification=qualified"), "{line}");
        assert!(line.contains("black_hole_anchor=0"), "{line}");
        assert!(line.contains("black_holes_current=0"), "{line}");
        assert!(line.contains("qualification_override=true"), "{line}");
        assert!(line.contains("all_degraded_fallback=false"), "{line}");
        assert!(
            line.contains("candidates=[conn1:id=42@7,active=62,qualification=qualified,anchor=0,current=0,admitted=true,cwnd=23842,rtt_us=163000]"),
            "{line}"
        );
        assert!(line.contains("generation=7"), "{line}");
        assert!(line.contains("replacement_installed=true"), "{line}");
        assert!(line.contains("last_success_age_secs=15"), "{line}");
        assert!(line.contains("probe_result=alive"), "{line}");
        assert!(
            line.contains("reconnect_reason=previous_open_failure"),
            "{line}"
        );
    }

    #[test]
    fn format_tuic_tcp_open_line_includes_target_pool_and_id() {
        let target = TargetAddr::parse("1.2.3.4:5201").unwrap();
        let line = format_tuic_tcp_open_line(&target, 3, 42, 8, 2, TuicTcpRelayMode::OrderedJoin);

        assert!(line.contains("tuic-open-tcp"), "{line}");
        assert!(line.contains("target=1.2.3.4:5201"), "{line}");
        assert!(line.contains("conn=3"), "{line}");
        assert!(line.contains("id=42"), "{line}");
        assert!(line.contains("stream=8"), "{line}");
        assert!(line.contains("relay_mode=ordered_join"), "{line}");
        assert!(line.contains("startup_auth_attempts=2"), "{line}");
        assert!(!line.contains("handle="), "{line}");
        assert!(!line.contains("epoch="), "{line}");

        let diagnostic =
            format_tuic_tcp_open_line(&target, 3, 42, 8, 2, TuicTcpRelayMode::OrderedChunk);
        assert!(
            diagnostic.contains("relay_mode=ordered_chunk"),
            "{diagnostic}"
        );

        let bridge = TcpRelayOpenDiag {
            handle: "SocketHandle(7)".to_string(),
            epoch: 41,
        };
        let bridged = format_tuic_tcp_open_line_with_diag(
            &target,
            3,
            42,
            8,
            2,
            TuicTcpRelayMode::OrderedJoin,
            Some(&bridge),
        );
        assert!(bridged.contains("handle=SocketHandle(7)"), "{bridged}");
        assert!(bridged.contains("epoch=41"), "{bridged}");
    }

    #[tokio::test]
    async fn format_tuic_tcp_open_line_reads_task_local_relay_bridge() {
        let target = TargetAddr::parse("1.2.3.4:5201").unwrap();
        let bridge = TcpRelayOpenDiag {
            handle: "SocketHandle(9)".to_string(),
            epoch: 17,
        };
        let line = crate::upstream::with_tcp_relay_open_diag(bridge, async {
            format_tuic_tcp_open_line(&target, 3, 42, 8, 2, TuicTcpRelayMode::OrderedJoin)
        })
        .await;

        assert!(line.contains("handle=SocketHandle(9)"), "{line}");
        assert!(line.contains("epoch=17"), "{line}");
    }

    #[test]
    fn format_tuic_tcp_stream_diag_lines_include_first_rx_and_gaps() {
        let target = TargetAddr::parse("1.2.3.4:5201").unwrap();
        let meta = TuicTcpStreamDiagMeta::new(&target, 3, 42, 8);
        let first = format_tuic_tcp_stream_first_rx_line(&meta, 20_500, 35_244, 1);
        let gap = format_tuic_tcp_stream_read_gap_line(&meta, 15_000, 39_884, 2, 75_128);
        let pending = format_tuic_tcp_stream_pending_line(
            &meta,
            12_000,
            24,
            31,
            5_000,
            75_128,
            2,
            TuicTcpStreamPendingCause::NoTransportSample,
            None,
            5,
            4,
        );
        let close_snapshot = TuicTcpStreamCloseSnapshot {
            first_rx_ms: 20_500,
            max_read_gap_ms: 15_000,
            rx_bytes: 109_304,
            reads: 3,
            pending_polls: 24,
            max_pending_gap_ms: 12_000,
            polls: 31,
            max_poll_gap_ms: 5_000,
            self_wake_armed: 5,
            self_wake_fired: 4,
        };
        let close = format_tuic_tcp_stream_close_line(&meta, &close_snapshot);

        assert!(first.contains("tuic-tcp-stream-first-rx"), "{first}");
        assert!(first.contains("target=1.2.3.4:5201"), "{first}");
        assert!(first.contains("conn=3"), "{first}");
        assert!(first.contains("id=42"), "{first}");
        assert!(first.contains("stream=8"), "{first}");
        assert!(first.contains("first_rx_ms=20500"), "{first}");
        assert!(first.contains("read_bytes=35244"), "{first}");
        assert!(first.contains("reads=1"), "{first}");

        assert!(gap.contains("tuic-tcp-stream-read-gap"), "{gap}");
        assert!(gap.contains("gap_ms=15000"), "{gap}");
        assert!(gap.contains("read_bytes=39884"), "{gap}");
        assert!(gap.contains("rx_bytes=75128"), "{gap}");

        assert!(pending.contains("tuic-tcp-stream-pending"), "{pending}");
        assert!(pending.contains("pending_gap_ms=12000"), "{pending}");
        assert!(pending.contains("pending_polls=24"), "{pending}");
        assert!(pending.contains("polls=31"), "{pending}");
        assert!(pending.contains("max_poll_gap_ms=5000"), "{pending}");
        assert!(pending.contains("rx_bytes=75128"), "{pending}");
        assert!(pending.contains("reads=2"), "{pending}");
        assert!(pending.contains("self_wake_armed=5"), "{pending}");
        assert!(pending.contains("self_wake_fired=4"), "{pending}");

        assert!(close.contains("tuic-tcp-stream-close"), "{close}");
        assert!(close.contains("first_rx_ms=20500"), "{close}");
        assert!(close.contains("max_read_gap_ms=15000"), "{close}");
        assert!(close.contains("rx_bytes=109304"), "{close}");
        assert!(close.contains("reads=3"), "{close}");
        assert!(close.contains("pending_polls=24"), "{close}");
        assert!(close.contains("max_pending_gap_ms=12000"), "{close}");
        assert!(close.contains("polls=31"), "{close}");
        assert!(close.contains("max_poll_gap_ms=5000"), "{close}");
        assert!(close.contains("self_wake_armed=5"), "{close}");
        assert!(close.contains("self_wake_fired=4"), "{close}");
    }

    #[test]
    fn format_tuic_tcp_stream_pending_line_includes_transport_delivery_counters() {
        let target = TargetAddr::parse("1.2.3.4:5201").unwrap();
        let meta = TuicTcpStreamDiagMeta::new(&target, 3, 42, 8);
        let transport = TuicStreamTransportPending {
            sample: TuicStreamTransportSample {
                udp_rx_datagrams: 101,
                udp_rx_bytes: 200_000,
                rx_stream_frames: 37,
                rx_ack_frames: 41,
                tx_ack_frames: 43,
                sent_plpmtud_probes: 5,
                lost_plpmtud_probes: 2,
                black_holes_detected: 1,
            },
            since_last_read: TuicStreamTransportDelta {
                udp_rx_datagrams: 11,
                udp_rx_bytes: 35_000,
                rx_stream_frames: 7,
            },
            since_last_pending: TuicStreamTransportDelta {
                udp_rx_datagrams: 3,
                udp_rx_bytes: 9_000,
                rx_stream_frames: 2,
            },
        };
        let pending = format_tuic_tcp_stream_pending_line(
            &meta,
            12_000,
            24,
            31,
            250,
            75_128,
            2,
            TuicTcpStreamPendingCause::ConnectionFreshStreamFramesPending,
            Some(transport),
            13,
            12,
        );

        assert!(
            pending.contains("pending_cause=connection_fresh_stream_frames_pending"),
            "{pending}"
        );
        assert!(pending.contains("conn_udp_rx=101/200000B"), "{pending}");
        assert!(
            pending.contains("conn_udp_rx_since_read=11/35000B"),
            "{pending}"
        );
        assert!(
            pending.contains("conn_udp_rx_since_pending=3/9000B"),
            "{pending}"
        );
        assert!(pending.contains("conn_rx_stream_frames=37"), "{pending}");
        assert!(pending.contains("self_wake_armed=13"), "{pending}");
        assert!(pending.contains("self_wake_fired=12"), "{pending}");
        assert!(
            pending.contains("conn_rx_stream_frames_since_read=7"),
            "{pending}"
        );
        assert!(
            pending.contains("conn_rx_stream_frames_since_pending=2"),
            "{pending}"
        );
        assert!(
            pending.contains("conn_ack_frames(rx=41,tx=43)"),
            "{pending}"
        );
        assert!(
            pending.contains("conn_plpmtud(sent=5,lost=2,black_holes=1)"),
            "{pending}"
        );
    }

    #[test]
    fn format_tuic_tcp_unordered_staging_line_includes_reassembly_pressure() {
        let target = TargetAddr::parse("1.2.3.4:5201").unwrap();
        let meta = TuicTcpStreamDiagMeta::new(&target, 3, 42, 8);

        let line = format_tuic_tcp_unordered_staging_line(
            &meta,
            "chunk",
            1024,
            4096,
            1200,
            3600,
            3,
            4800,
            7,
            8400,
            2,
            4096,
            3072,
            1,
            4 * 1024 * 1024,
        );

        assert!(line.contains("tuic-tcp-unordered-staging"), "{line}");
        assert!(line.contains("target=1.2.3.4:5201"), "{line}");
        assert!(line.contains("conn=3"), "{line}");
        assert!(line.contains("id=42"), "{line}");
        assert!(line.contains("stream=8"), "{line}");
        assert!(line.contains("reason=chunk"), "{line}");
        assert!(line.contains("next_offset=1024"), "{line}");
        assert!(line.contains("chunk_offset=4096"), "{line}");
        assert!(line.contains("chunk_bytes=1200"), "{line}");
        assert!(line.contains("gap_bytes=3072"), "{line}");
        assert!(line.contains("max_gap_bytes=4096"), "{line}");
        assert!(line.contains("buffered=3600B"), "{line}");
        assert!(line.contains("buffered_chunks=3"), "{line}");
        assert!(line.contains("max_buffered=4800B"), "{line}");
        assert!(line.contains("unordered_chunks=7"), "{line}");
        assert!(line.contains("unordered_bytes=8400"), "{line}");
        assert!(line.contains("out_of_order_chunks=2"), "{line}");
        assert!(line.contains("cap_hits=1"), "{line}");
        assert!(line.contains("cap=4194304B"), "{line}");
    }

    #[test]
    fn tuic_tcp_stream_pending_cause_classifies_transport_freshness() {
        assert_eq!(
            classify_tuic_stream_pending_cause(None),
            TuicTcpStreamPendingCause::NoTransportSample
        );

        let no_connection_rx = TuicStreamTransportPending {
            sample: TuicStreamTransportSample {
                udp_rx_datagrams: 100,
                udp_rx_bytes: 200_000,
                rx_stream_frames: 30,
                rx_ack_frames: 40,
                tx_ack_frames: 50,
                sent_plpmtud_probes: 0,
                lost_plpmtud_probes: 0,
                black_holes_detected: 0,
            },
            since_last_read: TuicStreamTransportDelta::default(),
            since_last_pending: TuicStreamTransportDelta::default(),
        };
        assert_eq!(
            classify_tuic_stream_pending_cause(Some(no_connection_rx)),
            TuicTcpStreamPendingCause::NoConnectionRx
        );

        let connection_rx_no_stream_frames = TuicStreamTransportPending {
            since_last_read: TuicStreamTransportDelta {
                udp_rx_datagrams: 12,
                udp_rx_bytes: 14_000,
                rx_stream_frames: 0,
            },
            ..no_connection_rx
        };
        assert_eq!(
            classify_tuic_stream_pending_cause(Some(connection_rx_no_stream_frames)),
            TuicTcpStreamPendingCause::ConnectionRxNoStreamFrames
        );

        let fresh_stream_frames_pending = TuicStreamTransportPending {
            since_last_read: TuicStreamTransportDelta {
                udp_rx_datagrams: 12,
                udp_rx_bytes: 14_000,
                rx_stream_frames: 3,
            },
            since_last_pending: TuicStreamTransportDelta {
                udp_rx_datagrams: 2,
                udp_rx_bytes: 4_000,
                rx_stream_frames: 1,
            },
            ..no_connection_rx
        };
        assert_eq!(
            classify_tuic_stream_pending_cause(Some(fresh_stream_frames_pending)),
            TuicTcpStreamPendingCause::ConnectionFreshStreamFramesPending
        );

        let stale_stream_frames_pending = TuicStreamTransportPending {
            since_last_read: TuicStreamTransportDelta {
                udp_rx_datagrams: 12,
                udp_rx_bytes: 14_000,
                rx_stream_frames: 3,
            },
            since_last_pending: TuicStreamTransportDelta {
                udp_rx_datagrams: 2,
                udp_rx_bytes: 4_000,
                rx_stream_frames: 0,
            },
            ..no_connection_rx
        };
        assert_eq!(
            classify_tuic_stream_pending_cause(Some(stale_stream_frames_pending)),
            TuicTcpStreamPendingCause::ConnectionStaleStreamFramesPending
        );
    }

    #[test]
    fn tuic_stream_pending_self_wake_arms_for_active_connection_rx_after_deadline() {
        let now = Instant::now();
        let future = now + Duration::from_millis(10);
        let past = now - Duration::from_millis(1);

        assert!(should_arm_tuic_stream_pending_self_wake(
            TuicTcpStreamPendingCause::ConnectionFreshStreamFramesPending,
            None,
            now
        ));
        assert!(!should_arm_tuic_stream_pending_self_wake(
            TuicTcpStreamPendingCause::ConnectionFreshStreamFramesPending,
            Some(future),
            now
        ));
        assert!(should_arm_tuic_stream_pending_self_wake(
            TuicTcpStreamPendingCause::ConnectionFreshStreamFramesPending,
            Some(past),
            now
        ));
        assert!(should_arm_tuic_stream_pending_self_wake(
            TuicTcpStreamPendingCause::ConnectionRxNoStreamFrames,
            None,
            now
        ));
        assert!(!should_arm_tuic_stream_pending_self_wake(
            TuicTcpStreamPendingCause::ConnectionRxNoStreamFrames,
            Some(future),
            now
        ));
        assert!(should_arm_tuic_stream_pending_self_wake(
            TuicTcpStreamPendingCause::ConnectionRxNoStreamFrames,
            Some(past),
            now
        ));
        assert!(!should_arm_tuic_stream_pending_self_wake(
            TuicTcpStreamPendingCause::NoConnectionRx,
            Some(past),
            now
        ));
    }

    #[test]
    fn tuic_stream_pending_self_wake_services_active_connection_rx() {
        let now = Instant::now();

        assert!(
            should_arm_tuic_stream_pending_self_wake(
                TuicTcpStreamPendingCause::ConnectionRxNoStreamFrames,
                None,
                now,
            ),
            "active QUIC receive without yet-deliverable stream frames should still wake pending reads"
        );
    }

    #[test]
    fn tuic_tcp_stream_diag_tracks_first_rx_and_read_gaps() {
        let target = TargetAddr::parse("1.2.3.4:5201").unwrap();
        let meta = TuicTcpStreamDiagMeta::new(&target, 3, 42, 8);
        let start = std::time::Instant::now();
        let mut diag = TuicTcpStreamDiag::new(meta, start);

        diag.note_poll_at(start + std::time::Duration::from_millis(20_500));
        let first = diag.note_read_at(
            35_244,
            start + std::time::Duration::from_millis(20_500),
            None,
        );
        assert_eq!(first.first_rx_ms, Some(20_500));
        assert_eq!(first.gap_ms, None);
        assert_eq!(first.rx_bytes, 35_244);
        assert_eq!(first.reads, 1);

        diag.note_poll_at(start + std::time::Duration::from_millis(35_500));
        let second = diag.note_read_at(
            39_884,
            start + std::time::Duration::from_millis(35_500),
            None,
        );
        assert_eq!(second.first_rx_ms, None);
        assert_eq!(second.gap_ms, Some(15_000));
        assert_eq!(second.rx_bytes, 75_128);
        assert_eq!(second.reads, 2);

        let close = diag.close_snapshot();
        assert_eq!(close.first_rx_ms, 20_500);
        assert_eq!(close.max_read_gap_ms, 15_000);
        assert_eq!(close.rx_bytes, 75_128);
        assert_eq!(close.reads, 2);
        assert_eq!(close.polls, 2);
        assert_eq!(close.max_poll_gap_ms, 15_000);
    }

    #[test]
    fn tuic_tcp_stream_diag_tracks_rate_limited_pending_gaps() {
        let target = TargetAddr::parse("1.2.3.4:5201").unwrap();
        let meta = TuicTcpStreamDiagMeta::new(&target, 3, 42, 8);
        let start = std::time::Instant::now();
        let mut diag = TuicTcpStreamDiag::new(meta, start);

        diag.note_poll_at(start + std::time::Duration::from_millis(999));
        assert_eq!(
            diag.note_pending_at(start + std::time::Duration::from_millis(999), None),
            None
        );
        diag.note_poll_at(start + std::time::Duration::from_millis(1_000));
        let first = diag
            .note_pending_at(start + std::time::Duration::from_millis(1_000), None)
            .expect("first threshold-crossing pending poll logs");
        assert_eq!(first.pending_gap_ms, 1_000);
        assert_eq!(first.pending_polls, 2);
        assert_eq!(first.polls, 2);
        assert_eq!(first.max_poll_gap_ms, 1);
        assert_eq!(first.rx_bytes, 0);
        assert_eq!(first.reads, 0);
        diag.note_poll_at(start + std::time::Duration::from_millis(1_500));
        assert_eq!(
            diag.note_pending_at(start + std::time::Duration::from_millis(1_500), None),
            None,
            "pending logs are rate limited"
        );
        diag.note_poll_at(start + std::time::Duration::from_millis(2_100));
        let second = diag
            .note_pending_at(start + std::time::Duration::from_millis(2_100), None)
            .expect("second pending log after the log interval");
        assert_eq!(second.pending_gap_ms, 2_100);
        assert_eq!(second.pending_polls, 4);
        assert_eq!(second.polls, 4);
        assert_eq!(second.max_poll_gap_ms, 600);

        diag.note_poll_at(start + std::time::Duration::from_millis(2_200));
        let read = diag.note_read_at(128, start + std::time::Duration::from_millis(2_200), None);
        assert_eq!(read.rx_bytes, 128);
        diag.note_poll_at(start + std::time::Duration::from_millis(3_300));
        let after_read = diag
            .note_pending_at(start + std::time::Duration::from_millis(3_300), None)
            .expect("pending gap is measured from the latest data read");
        assert_eq!(after_read.pending_gap_ms, 1_100);
        assert_eq!(after_read.polls, 6);
        assert_eq!(after_read.max_poll_gap_ms, 1_100);
        assert_eq!(after_read.rx_bytes, 128);
        assert_eq!(after_read.reads, 1);

        let close = diag.close_snapshot();
        assert_eq!(close.pending_polls, 5);
        assert_eq!(close.max_pending_gap_ms, 2_100);
        assert_eq!(close.polls, 6);
        assert_eq!(close.max_poll_gap_ms, 1_100);
    }

    #[test]
    fn tuic_tcp_stream_diag_reports_transport_delta_since_last_read() {
        let target = TargetAddr::parse("1.2.3.4:5201").unwrap();
        let meta = TuicTcpStreamDiagMeta::new(&target, 3, 42, 8);
        let start = std::time::Instant::now();
        let mut diag = TuicTcpStreamDiag::new(meta, start);

        diag.note_poll_at(start + std::time::Duration::from_millis(100));
        diag.note_read_at(
            128,
            start + std::time::Duration::from_millis(100),
            Some(TuicStreamTransportSample {
                udp_rx_datagrams: 10,
                udp_rx_bytes: 20_000,
                rx_stream_frames: 5,
                rx_ack_frames: 1,
                tx_ack_frames: 2,
                sent_plpmtud_probes: 3,
                lost_plpmtud_probes: 0,
                black_holes_detected: 0,
            }),
        );
        diag.note_poll_at(start + std::time::Duration::from_millis(1_200));
        let pending = diag
            .note_pending_at(
                start + std::time::Duration::from_millis(1_200),
                Some(TuicStreamTransportSample {
                    udp_rx_datagrams: 17,
                    udp_rx_bytes: 44_000,
                    rx_stream_frames: 8,
                    rx_ack_frames: 4,
                    tx_ack_frames: 6,
                    sent_plpmtud_probes: 4,
                    lost_plpmtud_probes: 1,
                    black_holes_detected: 1,
                }),
            )
            .expect("pending over threshold logs");
        let transport = pending.transport.expect("transport sample is attached");

        assert_eq!(transport.since_last_read.udp_rx_datagrams, 7);
        assert_eq!(transport.since_last_read.udp_rx_bytes, 24_000);
        assert_eq!(transport.since_last_read.rx_stream_frames, 3);
        assert_eq!(transport.since_last_pending.udp_rx_datagrams, 17);
        assert_eq!(transport.since_last_pending.udp_rx_bytes, 44_000);
        assert_eq!(transport.since_last_pending.rx_stream_frames, 8);
        assert_eq!(transport.sample.rx_ack_frames, 4);
        assert_eq!(transport.sample.tx_ack_frames, 6);
        assert_eq!(transport.sample.lost_plpmtud_probes, 1);
        assert_eq!(transport.sample.black_holes_detected, 1);
    }

    #[test]
    fn tuic_tcp_stream_pending_event_reports_since_last_pending_delta() {
        let target = TargetAddr::parse("1.2.3.4:5201").unwrap();
        let meta = TuicTcpStreamDiagMeta::new(&target, 3, 42, 8);
        let start = std::time::Instant::now();
        let mut diag = TuicTcpStreamDiag::new(meta, start);

        diag.note_pending_at(
            start + std::time::Duration::from_millis(1_000),
            Some(TuicStreamTransportSample {
                udp_rx_datagrams: 10,
                udp_rx_bytes: 20_000,
                rx_stream_frames: 5,
                rx_ack_frames: 1,
                tx_ack_frames: 2,
                sent_plpmtud_probes: 0,
                lost_plpmtud_probes: 0,
                black_holes_detected: 0,
            }),
        )
        .expect("first pending log arms the pending baseline");

        let pending = diag
            .note_pending_at(
                start + std::time::Duration::from_millis(2_000),
                Some(TuicStreamTransportSample {
                    udp_rx_datagrams: 12,
                    udp_rx_bytes: 22_882,
                    rx_stream_frames: 5,
                    rx_ack_frames: 2,
                    tx_ack_frames: 3,
                    sent_plpmtud_probes: 0,
                    lost_plpmtud_probes: 0,
                    black_holes_detected: 0,
                }),
            )
            .expect("second pending log reports delta since previous pending log");
        let transport = pending.transport.expect("transport sample is attached");

        assert_eq!(transport.since_last_read.udp_rx_datagrams, 12);
        assert_eq!(transport.since_last_pending.udp_rx_datagrams, 2);
        assert_eq!(transport.since_last_pending.udp_rx_bytes, 2_882);
        assert_eq!(
            transport.since_last_pending.rx_stream_frames, 0,
            "connection stream frames may be stale even when the since-read cause remains stream-frame-pending"
        );
    }

    #[test]
    fn ordered_quic_chunk_assembler_buffers_gap_then_drains_contiguously() {
        let mut assembler = OrderedQuicChunkAssembler::default();
        let mut out = [0u8; 6];
        let mut read_buf = ReadBuf::new(&mut out);

        assembler.push_chunk(3, bytes::Bytes::from_static(b"def"));
        assert_eq!(assembler.drain_into(&mut read_buf), 0);
        assert_eq!(read_buf.filled(), b"");

        assembler.push_chunk(0, bytes::Bytes::from_static(b"abc"));
        assert_eq!(assembler.drain_into(&mut read_buf), 6);
        assert_eq!(read_buf.filled(), b"abcdef");
        assert_eq!(assembler.next_offset(), 6);
        assert_eq!(assembler.buffered_bytes(), 0);
    }

    #[test]
    fn ordered_quic_chunk_assembler_trims_already_delivered_prefix() {
        let mut assembler = OrderedQuicChunkAssembler::default();
        let mut first_out = [0u8; 3];
        let mut first = ReadBuf::new(&mut first_out);
        assembler.push_chunk(0, bytes::Bytes::from_static(b"abc"));
        assert_eq!(assembler.drain_into(&mut first), 3);

        let mut second_out = [0u8; 3];
        let mut second = ReadBuf::new(&mut second_out);
        assembler.push_chunk(1, bytes::Bytes::from_static(b"bcdef"));
        assert_eq!(assembler.drain_into(&mut second), 3);
        assert_eq!(second.filled(), b"def");
        assert_eq!(assembler.next_offset(), 6);
        assert_eq!(assembler.buffered_bytes(), 0);
    }

    #[test]
    fn ordered_quic_chunk_assembler_keeps_longer_duplicate_at_same_offset() {
        let mut assembler = OrderedQuicChunkAssembler::default();
        let mut out = [0u8; 6];
        let mut read_buf = ReadBuf::new(&mut out);

        assembler.push_chunk(0, bytes::Bytes::from_static(b"abcdef"));
        assembler.push_chunk(0, bytes::Bytes::from_static(b"ab"));

        assert_eq!(assembler.drain_into(&mut read_buf), 6);
        assert_eq!(read_buf.filled(), b"abcdef");
        assert_eq!(assembler.next_offset(), 6);
        assert_eq!(assembler.buffered_bytes(), 0);
    }

    #[test]
    fn ordered_quic_chunk_assembler_replaces_short_duplicate_with_longer() {
        let mut assembler = OrderedQuicChunkAssembler::default();
        let mut out = [0u8; 6];
        let mut read_buf = ReadBuf::new(&mut out);

        assembler.push_chunk(0, bytes::Bytes::from_static(b"ab"));
        assembler.push_chunk(0, bytes::Bytes::from_static(b"abcdef"));

        assert_eq!(assembler.drain_into(&mut read_buf), 6);
        assert_eq!(read_buf.filled(), b"abcdef");
        assert_eq!(assembler.next_offset(), 6);
        assert_eq!(assembler.buffered_bytes(), 0);
    }

    #[derive(Debug)]
    struct MockOrderedChunkRecv {
        chunks: std::collections::VecDeque<io::Result<Option<bytes::Bytes>>>,
        max_lens: Vec<usize>,
        poll_count: usize,
    }

    impl MockOrderedChunkRecv {
        fn new(chunks: impl IntoIterator<Item = io::Result<Option<bytes::Bytes>>>) -> Self {
            Self {
                chunks: chunks.into_iter().collect(),
                max_lens: Vec::new(),
                poll_count: 0,
            }
        }
    }

    impl OrderedChunkRecv for MockOrderedChunkRecv {
        fn poll_read_ordered_chunk(
            &mut self,
            _cx: &mut Context<'_>,
            max_len: usize,
        ) -> Poll<io::Result<Option<bytes::Bytes>>> {
            self.poll_count += 1;
            self.max_lens.push(max_len);
            Poll::Ready(self.chunks.pop_front().unwrap_or(Ok(None)))
        }
    }

    #[tokio::test]
    async fn ordered_relay_stream_reads_ordered_chunks_without_join() {
        use tokio::io::AsyncReadExt;

        let recv = MockOrderedChunkRecv::new([
            Ok(Some(bytes::Bytes::from_static(b"abc"))),
            Ok(Some(bytes::Bytes::from_static(b"def"))),
            Ok(None),
        ]);
        let (send, _peer) = tokio::io::duplex(64);
        let mut stream = TuicOrderedRelayStream::new(recv, send);

        let mut out = Vec::new();
        stream.read_to_end(&mut out).await.unwrap();

        assert_eq!(out, b"abcdef");
        assert_eq!(stream.recv.poll_count, 3);
    }

    #[tokio::test]
    async fn ordered_relay_stream_keeps_remainder_without_repolling_recv() {
        use tokio::io::AsyncReadExt;

        let recv =
            MockOrderedChunkRecv::new([Ok(Some(bytes::Bytes::from_static(b"abcdef"))), Ok(None)]);
        let (send, _peer) = tokio::io::duplex(64);
        let mut stream = TuicOrderedRelayStream::new(recv, send);

        let mut first = [0u8; 3];
        let first_read = stream.read(&mut first).await.unwrap();
        assert_eq!(first_read, 3);
        assert_eq!(&first, b"abc");
        assert_eq!(stream.recv.max_lens, vec![3]);

        let mut second = [0u8; 3];
        let second_read = stream.read(&mut second).await.unwrap();
        assert_eq!(second_read, 3);
        assert_eq!(&second, b"def");
        assert_eq!(
            stream.recv.poll_count, 1,
            "remaining bytes from an oversized chunk must be served locally"
        );
    }

    #[tokio::test]
    async fn ordered_relay_stream_delegates_writes_to_quic_send_half() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let recv = MockOrderedChunkRecv::new([Ok(None)]);
        let (send, mut peer) = tokio::io::duplex(64);
        let mut stream = TuicOrderedRelayStream::new(recv, send);

        stream.write_all(b"ping").await.unwrap();
        stream.flush().await.unwrap();

        let mut out = [0u8; 4];
        peer.read_exact(&mut out).await.unwrap();
        assert_eq!(&out, b"ping");
    }

    #[tokio::test]
    async fn tracked_relay_stream_records_nonempty_reads() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let target = TargetAddr::parse("1.2.3.4:5201").unwrap();
        let meta = TuicTcpStreamDiagMeta::new(&target, 3, 42, 8);
        let active = Arc::new(AtomicU64::new(0));
        let (lease, _) = TcpPoolSlotLease::reserve(active);
        let (mut writer, inner) = tokio::io::duplex(64);
        let mut stream = TrackedRelayStream::new(
            inner,
            lease,
            Some(TuicTcpStreamDiag::new(meta, std::time::Instant::now())),
        );

        writer.write_all(b"abc").await.unwrap();

        let mut buf = [0u8; 3];
        let read = stream.read(&mut buf).await.unwrap();

        assert_eq!(read, 3);
        assert_eq!(&buf, b"abc");
        let snapshot = stream.tcp_diag.as_ref().unwrap().close_snapshot();
        assert_eq!(snapshot.rx_bytes, 3);
        assert_eq!(snapshot.reads, 1);
        assert!(
            snapshot.polls >= 1,
            "stream read should poll at least once: {snapshot:?}"
        );
        assert!(
            snapshot.first_rx_ms < 1_000,
            "first_rx_ms should be immediate in the in-memory stream: {snapshot:?}"
        );
    }

    #[test]
    fn tcp_pool_phase_history_cannot_shift_the_next_idle_pair() {
        let selector = TcpPoolAdmission::new(2);

        {
            let prior = selector
                .try_reserve(&[])
                .expect("an idle pool must accept a historical one-flow phase");
            assert_eq!(prior.index, 0);
            assert_eq!(prior.active_before, 0);
        }

        let control = selector
            .try_reserve(&[])
            .expect("the next control flow must reserve the primary slot");
        let data = selector
            .try_reserve(&[])
            .expect("the live control reservation must steer data to auxiliary");

        assert_eq!((control.index, control.active_before), (0, 0));
        assert_eq!((data.index, data.active_before), (1, 0));
        drop((control, data));
        assert_eq!(selector.active_slots[0].load(Ordering::Relaxed), 0);
        assert_eq!(selector.active_slots[1].load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn tcp_pool_generation_lease_zero_is_isolated_from_successor() {
        let pool_active = Arc::new(AtomicU64::new(0));
        let predecessor_slot_active = Arc::new(AtomicU64::new(0));
        let successor_slot_active = Arc::new(AtomicU64::new(0));
        let predecessor = Arc::new(TcpPoolGenerationActivity::new());
        let successor = Arc::new(TcpPoolGenerationActivity::new());

        let predecessor_lease = TcpPoolSlotLease::reserve_generation(
            predecessor.clone(),
            predecessor_slot_active,
            pool_active.clone(),
        )
        .expect("the predecessor generation must accept its first lease");
        let successor_lease = TcpPoolSlotLease::reserve_generation(
            successor.clone(),
            successor_slot_active,
            pool_active.clone(),
        )
        .expect("the successor generation must have an independent lease counter");

        assert_eq!(predecessor.active(), 1);
        assert_eq!(successor.active(), 1);
        assert_eq!(pool_active.load(Ordering::Acquire), 2);

        let predecessor_zero = tokio::spawn({
            let predecessor = predecessor.clone();
            async move { predecessor.wait_for_zero().await }
        });
        tokio::task::yield_now().await;
        drop(predecessor_lease);
        tokio::time::timeout(Duration::from_secs(1), predecessor_zero)
            .await
            .expect("the exact predecessor zero transition must notify its drain waiter")
            .expect("the predecessor zero waiter must not panic");

        assert_eq!(predecessor.active(), 0);
        assert_eq!(successor.active(), 1);
        assert_eq!(pool_active.load(Ordering::Acquire), 1);

        drop(successor_lease);
        assert_eq!(successor.active(), 0);
        assert_eq!(pool_active.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn tcp_pool_generation_slot_installs_successor_without_resetting_predecessor() {
        let clock = Instant::now();
        let slot = TcpPoolGenerationSlot::new(1, 11_u64, 1, 1, clock);
        let predecessor_activity = slot.current_activity().await;
        let slot_active = Arc::new(AtomicU64::new(0));
        let pool_active = Arc::new(AtomicU64::new(0));
        let predecessor_lease = TcpPoolSlotLease::reserve_generation(
            predecessor_activity.clone(),
            slot_active,
            pool_active,
        )
        .unwrap();
        let expected = TcpPoolTransportIdentity::new(11, 1);
        let successor = TcpPoolGeneration::new(22_u64, 2, 1, clock);

        let installed = slot
            .install_successor(expected, &predecessor_activity, successor, Instant::now())
            .await
            .expect("the exact predecessor identity must install one successor");

        assert_eq!(installed.predecessor_identity, expected);
        assert_eq!(
            installed.successor_identity,
            TcpPoolTransportIdentity::new(22, 2)
        );
        assert_eq!(slot.current_transport().await, 22);
        assert_eq!(slot.draining_transport().await, Some(11));
        assert_eq!(predecessor_activity.active(), 1);
        assert!(
            slot.take_drained(expected).await.is_none(),
            "a predecessor with a live lease must remain untouched"
        );

        drop(predecessor_lease);
        predecessor_activity.wait_for_zero().await;
        assert_eq!(predecessor_activity.active(), 0);
        let drained = slot
            .take_drained(expected)
            .await
            .expect("only the exact zero predecessor may be reaped");
        assert_eq!(drained.generation.transport, 11);
        assert_eq!(slot.draining_transport().await, None);
    }

    #[tokio::test]
    async fn tcp_pool_generation_install_rejects_primary_stale_and_second_predecessor() {
        let clock = Instant::now();
        let primary = TcpPoolGenerationSlot::new(0, 10_u64, 1, 1, clock);
        let primary_activity = primary.current_activity().await;
        let primary_result = primary
            .install_successor(
                TcpPoolTransportIdentity::new(10, 1),
                &primary_activity,
                TcpPoolGeneration::new(20_u64, 2, 1, clock),
                Instant::now(),
            )
            .await;
        assert!(matches!(
            primary_result,
            Err(TcpPoolGenerationInstallError::Primary)
        ));
        assert_eq!(primary.current_transport().await, 10);

        let auxiliary = TcpPoolGenerationSlot::new(1, 11_u64, 1, 1, clock);
        let predecessor_activity = auxiliary.current_activity().await;
        let stale_result = auxiliary
            .install_successor(
                TcpPoolTransportIdentity::new(99, 1),
                &predecessor_activity,
                TcpPoolGeneration::new(21_u64, 2, 1, clock),
                Instant::now(),
            )
            .await;
        assert!(matches!(
            stale_result,
            Err(TcpPoolGenerationInstallError::StalePredecessor)
        ));
        assert_eq!(auxiliary.current_transport().await, 11);
        assert_eq!(auxiliary.draining_transport().await, None);

        auxiliary
            .install_successor(
                TcpPoolTransportIdentity::new(11, 1),
                &predecessor_activity,
                TcpPoolGeneration::new(22_u64, 2, 1, clock),
                Instant::now(),
            )
            .await
            .unwrap();
        let successor_activity = auxiliary.current_activity().await;
        let second_result = auxiliary
            .install_successor(
                TcpPoolTransportIdentity::new(22, 2),
                &successor_activity,
                TcpPoolGeneration::new(23_u64, 3, 1, clock),
                Instant::now(),
            )
            .await;
        assert!(matches!(
            second_result,
            Err(TcpPoolGenerationInstallError::PredecessorDraining)
        ));
        assert_eq!(auxiliary.current_transport().await, 22);
        assert_eq!(auxiliary.draining_transport().await, Some(11));
    }

    #[tokio::test]
    async fn tcp_pool_replacement_trigger_reserves_only_the_installed_successor() {
        let clock = Instant::now();
        let slots = [
            TcpPoolGenerationSlot::new(0, 10_u64, 1, 1, clock),
            TcpPoolGenerationSlot::new(1, 11_u64, 1, 1, clock),
        ];
        let activities = vec![
            slots[0].current_activity().await,
            slots[1].current_activity().await,
        ];
        let admission = TcpPoolAdmission::with_current_generation_activity(activities.clone());
        let anchored = [
            TcpPoolPathObservation::known(
                TcpPoolTransportIdentity::new(10, 1),
                5_140,
                Duration::from_millis(175),
                0,
            ),
            TcpPoolPathObservation::known(
                TcpPoolTransportIdentity::new(11, 1),
                77_246,
                Duration::from_millis(179),
                0,
            ),
        ];
        let primary = admission.try_reserve(&anchored).unwrap();
        let auxiliary = admission.try_reserve(&anchored).unwrap();
        drop((primary, auxiliary));
        admission.set_active_for_test(0, 14);
        admission.set_active_for_test(1, 6);
        let observed = [
            anchored[0],
            TcpPoolPathObservation::known(
                TcpPoolTransportIdentity::new(11, 1),
                77_246,
                Duration::from_millis(179),
                16,
            ),
        ];
        let TcpPoolAdmissionDecision::ReplaceAuxiliary(replacement) =
            admission.try_decide(&observed, &[false, true]).unwrap()
        else {
            panic!("the replay must prepare an auxiliary successor");
        };
        let predecessor_activity = activities[1].clone();
        let successor = TcpPoolGeneration::new(22_u64, 2, 1, clock);
        let successor_activity = successor.activity.clone();
        let successor_observation = TcpPoolPathObservation::known(
            successor.identity(),
            12_000,
            Duration::from_millis(175),
            0,
        );
        slots[1]
            .install_successor_with(
                TcpPoolTransportIdentity::new(11, 1),
                &predecessor_activity,
                successor,
                Instant::now(),
                |predecessor, successor| {
                    admission.replace_current_generation_activity(1, predecessor, successor.clone())
                },
            )
            .await
            .unwrap();

        let selected = admission
            .reserve_replacement_successor(
                replacement,
                successor_activity.clone(),
                successor_observation,
            )
            .unwrap();

        assert_eq!((selected.index, selected.active_before), (1, 0));
        assert_eq!(predecessor_activity.active(), 6);
        assert_eq!(successor_activity.active(), 1);
        assert_eq!(admission.active_total(), 21);
        drop(selected);
        assert_eq!(predecessor_activity.active(), 6);
        assert_eq!(successor_activity.active(), 0);
        assert_eq!(admission.active_total(), 20);
    }

    #[test]
    fn tcp_pool_busy_epoch_black_hole_advancement_isolates_new_forward_opens() {
        let admission = TcpPoolAdmission::new(2);
        let anchored = [
            TcpPoolPathObservation::known(
                TcpPoolTransportIdentity::new(0, 1),
                6_665,
                Duration::from_millis(176),
                0,
            ),
            TcpPoolPathObservation::known(
                TcpPoolTransportIdentity::new(1, 1),
                12_887,
                Duration::from_millis(176),
                0,
            ),
        ];

        let first = admission
            .try_reserve(&anchored)
            .expect("the first idle slot must open its qualification epoch");
        let second = admission
            .try_reserve(&anchored)
            .expect("the second idle slot must open its qualification epoch");
        assert_eq!((first.index, second.index), (0, 1));
        drop((first, second));

        admission.set_active_for_test(0, 8);
        admission.set_active_for_test(1, 6);
        let advanced = [
            anchored[0],
            TcpPoolPathObservation::known(
                TcpPoolTransportIdentity::new(1, 1),
                12_887,
                Duration::from_millis(176),
                10,
            ),
        ];

        let selected = admission
            .try_reserve(&advanced)
            .expect("a qualified busy alternative must isolate the degraded slot");

        assert_eq!((selected.index, selected.active_before), (0, 8));
        assert_eq!(
            selected.qualification,
            TcpPoolForwardQualification::Qualified
        );
        assert!(selected.qualification_override);
        assert!(!selected.all_degraded_fallback);
    }

    #[test]
    fn tcp_pool_qualified_lane_collapse_requests_auxiliary_generation_replacement() {
        let admission = TcpPoolAdmission::new(2);
        let anchored = [
            TcpPoolPathObservation::known(
                TcpPoolTransportIdentity::new(0, 1),
                5_140,
                Duration::from_millis(175),
                0,
            ),
            TcpPoolPathObservation::known(
                TcpPoolTransportIdentity::new(1, 1),
                77_246,
                Duration::from_millis(179),
                0,
            ),
        ];
        let first = admission.try_reserve(&anchored).unwrap();
        let second = admission.try_reserve(&anchored).unwrap();
        drop((first, second));

        admission.set_active_for_test(0, 14);
        admission.set_active_for_test(1, 6);
        let observed = [
            anchored[0],
            TcpPoolPathObservation::known(
                TcpPoolTransportIdentity::new(1, 1),
                77_246,
                Duration::from_millis(179),
                16,
            ),
        ];

        let decision = admission
            .try_decide(&observed, &[false, true])
            .expect("the degraded auxiliary generation must remain replaceable");

        let TcpPoolAdmissionDecision::ReplaceAuxiliary(replacement) = decision else {
            panic!("qualified-lane collapse must request replacement, not reserve conn0");
        };
        assert_eq!(replacement.index, 1);
    }

    #[test]
    fn tcp_pool_active_zero_starts_a_fresh_forward_qualification_epoch() {
        let admission = TcpPoolAdmission::new(1);
        let identity = TcpPoolTransportIdentity::new(7, 3);
        let initial = [TcpPoolPathObservation::known(
            identity,
            10_000,
            Duration::from_millis(100),
            2,
        )];
        let first = admission.try_reserve(&initial).unwrap();
        assert_eq!(first.qualification, TcpPoolForwardQualification::Qualified);
        assert_eq!(first.qualification_anchor, Some(2));
        drop(first);

        admission.set_active_for_test(0, 1);
        let advanced = [TcpPoolPathObservation::known(
            identity,
            10_000,
            Duration::from_millis(100),
            5,
        )];
        let degraded = admission.try_reserve(&advanced).unwrap();
        assert_eq!(
            degraded.qualification,
            TcpPoolForwardQualification::Degraded
        );
        assert!(degraded.all_degraded_fallback);
        drop(degraded);

        admission.set_active_for_test(0, 0);
        let recovered = admission.try_reserve(&advanced).unwrap();
        assert_eq!(
            recovered.qualification,
            TcpPoolForwardQualification::Qualified
        );
        assert_eq!(recovered.qualification_anchor, Some(5));
        assert_eq!(recovered.black_holes_current, Some(5));
        assert!(!recovered.all_degraded_fallback);
    }

    #[test]
    fn tcp_pool_idle_observation_does_not_commit_an_epoch_before_reservation() {
        let admission = TcpPoolAdmission::new(1);
        let identity = TcpPoolTransportIdentity::new(7, 3);
        let anchored =
            TcpPoolPathObservation::known(identity, 10_000, Duration::from_millis(100), 0);
        let first = admission.try_reserve(&[anchored]).unwrap();
        assert_eq!(first.qualification_anchor, Some(0));

        let losing_idle_sample =
            TcpPoolPathObservation::known(identity, 10_000, Duration::from_millis(100), 7);
        let sampled = admission.qualification_for(0, 0, losing_idle_sample);
        assert_eq!(
            sampled.qualification,
            TcpPoolForwardQualification::Qualified
        );

        let advanced =
            TcpPoolPathObservation::known(identity, 10_000, Duration::from_millis(100), 1);
        let still_busy = admission.qualification_for(0, 1, advanced);
        assert_eq!(
            still_busy.qualification,
            TcpPoolForwardQualification::Degraded,
            "an idle observation that lost reservation ownership must not reset the busy epoch"
        );
    }

    #[test]
    fn tcp_pool_all_degraded_fallback_preserves_bounded_least_active_ordering() {
        let admission = TcpPoolAdmission::new(2);
        let identities = [
            TcpPoolTransportIdentity::new(0, 1),
            TcpPoolTransportIdentity::new(1, 1),
        ];
        let anchored = identities.map(|identity| {
            TcpPoolPathObservation::known(identity, 10_000, Duration::from_millis(100), 0)
        });
        let first = admission.try_reserve(&anchored).unwrap();
        let second = admission.try_reserve(&anchored).unwrap();
        drop((first, second));

        admission.set_active_for_test(0, 7);
        admission.set_active_for_test(1, 3);
        let degraded = [
            TcpPoolPathObservation::known(identities[0], 1_000_000, Duration::from_millis(1), 1),
            TcpPoolPathObservation::known(identities[1], 1, Duration::from_secs(1), 2),
        ];

        let selected = admission.try_reserve(&degraded).unwrap();

        assert_eq!((selected.index, selected.active_before), (1, 3));
        assert_eq!(
            selected.qualification,
            TcpPoolForwardQualification::Degraded
        );
        assert!(selected.all_degraded_fallback);
        assert!(!selected.qualification_override);
        drop(selected);

        admission.set_active_for_test(0, 6);
        admission.set_active_for_test(1, 6);
        let service_selected = admission.try_reserve(&degraded).unwrap();
        assert_eq!(
            (service_selected.index, service_selected.active_before),
            (0, 6)
        );
        assert!(service_selected.path_service_tiebreak);
        assert!(service_selected.all_degraded_fallback);
        drop(service_selected);

        let equal_service = identities.map(|identity| {
            TcpPoolPathObservation::known(identity, 10_000, Duration::from_millis(100), 3)
        });
        let stable = admission.try_reserve(&equal_service).unwrap();
        assert_eq!((stable.index, stable.active_before), (0, 6));
        assert!(!stable.path_service_tiebreak);
        assert!(stable.all_degraded_fallback);
    }

    #[test]
    fn tcp_pool_unknown_evidence_never_reuses_or_resets_a_busy_epoch() {
        let admission = TcpPoolAdmission::new(2);
        let identities = [
            TcpPoolTransportIdentity::new(10, 1),
            TcpPoolTransportIdentity::new(11, 1),
        ];
        let anchored = identities.map(|identity| {
            TcpPoolPathObservation::known(identity, 10_000, Duration::from_millis(100), 4)
        });
        let first = admission.try_reserve(&anchored).unwrap();
        let second = admission.try_reserve(&anchored).unwrap();
        drop((first, second));
        admission.set_active_for_test(0, 5);
        admission.set_active_for_test(1, 4);

        let missing_and_degraded = [
            TcpPoolPathObservation::Unknown,
            TcpPoolPathObservation::known(identities[1], 10_000, Duration::from_millis(100), 5),
        ];
        let missing = admission.try_reserve(&missing_and_degraded).unwrap();
        assert_eq!(missing.index, 0);
        assert_eq!(missing.qualification, TcpPoolForwardQualification::Unknown);
        assert!(missing.qualification_override);
        drop(missing);

        let regression_and_degraded = [
            TcpPoolPathObservation::known(identities[0], 10_000, Duration::from_millis(100), 3),
            missing_and_degraded[1],
        ];
        let regression = admission.try_reserve(&regression_and_degraded).unwrap();
        assert_eq!(regression.index, 0);
        assert_eq!(
            regression.qualification,
            TcpPoolForwardQualification::Unknown
        );
        assert_eq!(regression.qualification_anchor, Some(4));
        assert_eq!(regression.black_holes_current, Some(3));
        drop(regression);

        let new_identity_and_degraded = [
            TcpPoolPathObservation::known(
                TcpPoolTransportIdentity::new(10, 2),
                10_000,
                Duration::from_millis(100),
                0,
            ),
            missing_and_degraded[1],
        ];
        let replaced = admission.try_reserve(&new_identity_and_degraded).unwrap();
        assert_eq!(replaced.index, 0);
        assert_eq!(replaced.qualification, TcpPoolForwardQualification::Unknown);
        assert_eq!(replaced.qualification_anchor, None);
        assert_eq!(replaced.black_holes_current, Some(0));
    }

    #[test]
    fn tcp_pool_equal_busy_load_prefers_greater_path_service() {
        let selector = TcpPoolAdmission::new(2);
        selector.set_active_for_test(0, 62);
        selector.set_active_for_test(1, 62);
        let path_service = [
            TcpPoolPathService::known(10_124, Duration::from_millis(163)),
            TcpPoolPathService::known(23_842, Duration::from_millis(163)),
        ];

        let selected = selector
            .try_reserve(&tcp_pool_observations(&path_service))
            .expect("an equal-load busy pool must use current path service");

        assert_eq!((selected.index, selected.active_before), (1, 62));
        assert!(selected.path_service_tiebreak);
    }

    #[test]
    fn tcp_pool_path_service_compares_exact_ratio_and_rejects_invalid_samples() {
        let lower_window_better_service =
            TcpPoolPathService::known(10_000, Duration::from_millis(100));
        let higher_window_lower_service =
            TcpPoolPathService::known(15_000, Duration::from_millis(200));
        assert!(lower_window_better_service.has_greater_service_than(higher_window_lower_service));
        assert!(!higher_window_lower_service.has_greater_service_than(lower_window_better_service));

        let max_fast = TcpPoolPathService::known(u64::MAX, Duration::from_micros(1));
        let max_slow = TcpPoolPathService::known(u64::MAX, Duration::from_micros(2));
        assert!(max_fast.has_greater_service_than(max_slow));
        assert_eq!(
            TcpPoolPathService::known(0, Duration::from_millis(1)),
            TcpPoolPathService::Unknown
        );
        assert_eq!(
            TcpPoolPathService::known(1, Duration::ZERO),
            TcpPoolPathService::Unknown
        );
    }

    #[test]
    fn tcp_pool_idle_pair_ignores_stale_path_service_history() {
        let selector = TcpPoolAdmission::new(2);
        let path_service = [
            TcpPoolPathService::known(10_124, Duration::from_millis(163)),
            TcpPoolPathService::known(23_842, Duration::from_millis(163)),
        ];

        let observations = tcp_pool_observations(&path_service);
        let control = selector
            .try_reserve(&observations)
            .expect("idle control must reserve the stable primary slot");
        let data = selector
            .try_reserve(&observations)
            .expect("the live control reservation must steer data to auxiliary");

        assert_eq!((control.index, control.active_before), (0, 0));
        assert_eq!((data.index, data.active_before), (1, 0));
        assert!(!control.path_service_tiebreak);
        assert!(!data.path_service_tiebreak);
    }

    #[test]
    fn tcp_pool_unequal_load_precedes_path_service() {
        let selector = TcpPoolAdmission::new(2);
        selector.set_active_for_test(0, 3);
        selector.set_active_for_test(1, 4);
        let path_service = [
            TcpPoolPathService::known(1, Duration::from_secs(1)),
            TcpPoolPathService::known(1_000_000, Duration::from_millis(1)),
        ];

        let selected = selector
            .try_reserve(&tcp_pool_observations(&path_service))
            .expect("lease load remains the first ordering key");

        assert_eq!((selected.index, selected.active_before), (0, 3));
        assert!(!selected.path_service_tiebreak);
    }

    #[test]
    fn tcp_pool_known_path_service_precedes_unknown_for_equal_busy_load() {
        let selector = TcpPoolAdmission::new(2);
        selector.set_active_for_test(0, 4);
        selector.set_active_for_test(1, 4);
        let path_service = [
            TcpPoolPathService::Unknown,
            TcpPoolPathService::known(10_000, Duration::from_millis(100)),
        ];

        let selected = selector
            .try_reserve(&tcp_pool_observations(&path_service))
            .expect("known current service must win an equal busy-load tie");

        assert_eq!((selected.index, selected.active_before), (1, 4));
        assert!(selected.path_service_tiebreak);
    }

    #[test]
    fn tcp_pool_equal_or_unknown_path_service_keeps_stable_index() {
        let selector = TcpPoolAdmission::new(2);
        selector.set_active_for_test(0, 8);
        selector.set_active_for_test(1, 8);
        let equal_service = [
            TcpPoolPathService::known(10_000, Duration::from_millis(100)),
            TcpPoolPathService::known(20_000, Duration::from_millis(200)),
        ];

        let equal = selector
            .try_reserve(&tcp_pool_observations(&equal_service))
            .expect("equal path service must keep stable index ordering");
        assert_eq!((equal.index, equal.active_before), (0, 8));
        assert!(!equal.path_service_tiebreak);
        drop(equal);

        let unknown = selector
            .try_reserve(&[])
            .expect("missing samples must keep stable index ordering");
        assert_eq!((unknown.index, unknown.active_before), (0, 8));
        assert!(!unknown.path_service_tiebreak);
    }

    #[test]
    fn tcp_pool_preparing_slot_cannot_be_overtaken_before_connection_clone() {
        let selector = TcpPoolAdmission::new(2);
        selector.set_active_for_test(1, 4);

        let first = selector
            .try_reserve(&[])
            .expect("the idle primary slot must be selected first");
        let second = selector
            .try_reserve(&[])
            .expect("the preparing primary must steer the next opener to auxiliary");

        assert_eq!((first.index, first.active_before), (0, 0));
        assert_eq!((second.index, second.active_before), (1, 4));
        selector.set_active_for_test(1, 1);
        drop((first, second));
        assert_eq!(selector.active_slots[0].load(Ordering::Acquire), 0);
        assert_eq!(selector.active_slots[1].load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn tcp_pool_busy_waiter_wakes_after_preparation_releases() {
        let selector = Arc::new(TcpPoolAdmission::new(1));
        let first = selector
            .try_reserve(&[])
            .expect("the only idle slot must be reservable");
        let sample_calls = Arc::new(AtomicU64::new(0));
        let waiter = tokio::spawn({
            let selector = selector.clone();
            let sample_calls = sample_calls.clone();
            async move {
                selector
                    .reserve(|| {
                        let sample = sample_calls.fetch_add(1, Ordering::Relaxed) + 1;
                        vec![TcpPoolPathObservation::known(
                            TcpPoolTransportIdentity::new(0, 1),
                            10_000,
                            Duration::from_millis(100),
                            sample,
                        )]
                    })
                    .await
            }
        });
        tokio::task::yield_now().await;
        assert!(
            !waiter.is_finished(),
            "the preparing slot must not be overtaken"
        );

        drop(first);
        let second = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("preparation release must wake one blocked selector")
            .unwrap()
            .unwrap();
        assert_eq!((second.index, second.active_before), (0, 0));
        assert_eq!(second.qualification, TcpPoolForwardQualification::Qualified);
        assert_eq!(
            second.black_holes_current,
            Some(sample_calls.load(Ordering::Relaxed))
        );
        assert!(
            sample_calls.load(Ordering::Relaxed) >= 2,
            "a waiter must refresh path and qualification after preparation releases"
        );
        drop(second);
        assert_eq!(selector.active_slots[0].load(Ordering::Acquire), 0);
    }

    #[test]
    fn tcp_pool_simultaneous_first_reservations_use_distinct_slots() {
        let selector = Arc::new(TcpPoolAdmission::new(2));
        let held = Arc::new(std::sync::Barrier::new(3));
        let (selected_tx, selected_rx) = std::sync::mpsc::channel();

        let workers = (0..2)
            .map(|_| {
                let selector = selector.clone();
                let held = held.clone();
                let selected_tx = selected_tx.clone();
                std::thread::spawn(move || {
                    let reservation = selector
                        .try_reserve(&[])
                        .expect("an idle two-slot pool must accept both reservations");
                    selected_tx.send(reservation.index).unwrap();
                    held.wait();
                    drop(reservation);
                })
            })
            .collect::<Vec<_>>();
        drop(selected_tx);

        let mut selected = vec![selected_rx.recv().unwrap(), selected_rx.recv().unwrap()];
        selected.sort_unstable();
        assert_eq!(selected, vec![0, 1]);
        assert_eq!(selector.active_slots[0].load(Ordering::Acquire), 1);
        assert_eq!(selector.active_slots[1].load(Ordering::Acquire), 1);

        held.wait();
        for worker in workers {
            worker.join().unwrap();
        }
        assert_eq!(selector.active_slots[0].load(Ordering::Acquire), 0);
        assert_eq!(selector.active_slots[1].load(Ordering::Acquire), 0);
    }

    #[test]
    fn tcp_pool_reservation_fails_closed_for_empty_or_saturated_slots() {
        let empty = TcpPoolAdmission::new(0);
        assert!(matches!(
            empty.try_reserve(&[]),
            Err(TcpPoolReservationError::Empty)
        ));

        let saturated = TcpPoolAdmission::new(2);
        saturated.set_active_for_test(0, u64::MAX);
        saturated.set_active_for_test(1, u64::MAX);
        assert!(matches!(
            saturated.try_reserve(&[]),
            Err(TcpPoolReservationError::Saturated)
        ));
        assert_eq!(saturated.active_slots[0].load(Ordering::Acquire), u64::MAX);
        assert_eq!(saturated.active_slots[1].load(Ordering::Acquire), u64::MAX);
    }

    #[test]
    fn tcp_pool_slot_stale_after_threshold() {
        assert!(!tcp_pool_slot_stale(9, 0));
        assert!(tcp_pool_slot_stale(10, 0));
        assert!(tcp_pool_slot_stale(25, 15));
        assert!(!tcp_pool_slot_stale(24, 15));
        assert!(
            !tcp_pool_slot_stale(3, 15),
            "clock skew/saturation must not mark stale"
        );
    }

    #[test]
    fn tcp_pool_idle_auxiliary_requires_liveness_probe_before_reconnect() {
        assert_eq!(
            tcp_pool_idle_action(0, 100, 90, true),
            TcpPoolIdleAction::Reuse,
            "primary UDP/health connection keeps its existing lifecycle"
        );
        assert_eq!(
            tcp_pool_idle_action(1, 99, 90, true),
            TcpPoolIdleAction::Reuse,
            "a recently used auxiliary slot is reused"
        );
        assert_eq!(
            tcp_pool_idle_action(1, 100, 90, false),
            TcpPoolIdleAction::Reuse,
            "an active shared slot must never be probed or recycled"
        );
        assert_eq!(
            tcp_pool_idle_action(1, 100, 90, true),
            TcpPoolIdleAction::Probe,
            "elapsed idle time requests evidence; it is not failure evidence"
        );
        assert!(
            !should_probe_tcp_pool_generation(1, 10_000, 0, true, true),
            "an authenticated successor must not be recycled as stale before its triggering open"
        );
        assert!(should_probe_tcp_pool_generation(1, 100, 90, true, false));
    }

    #[tokio::test]
    async fn tcp_pool_liveness_probe_reuses_an_acked_quinn_connection() {
        let (server_endpoint, client_endpoint, server_addr) = d16_quinn_test_endpoints();
        let server_task = tokio::spawn(async move {
            let connection = server_endpoint.accept().await.unwrap().await.unwrap();
            let heartbeat = connection.read_datagram().await.unwrap();
            assert_eq!(&heartbeat[..], &encode_heartbeat());
            tokio::time::sleep(Duration::from_millis(50)).await;
        });

        let connection = client_endpoint
            .connect(server_addr, "localhost")
            .unwrap()
            .await
            .unwrap();
        let stable_id = connection.stable_id();
        let outcome = probe_tcp_pool_connection(&connection, Duration::from_secs(1)).await;

        assert!(matches!(outcome, TcpPoolProbeOutcome::Alive { .. }));
        assert_eq!(connection.stable_id(), stable_id);
        server_task.await.unwrap();
        client_endpoint.close(0u32.into(), b"test complete");
    }

    #[test]
    fn tcp_pool_probe_outcome_reconnects_only_without_liveness_evidence() {
        assert_eq!(
            tcp_pool_probe_reconnect_reason(TcpPoolProbeOutcome::Alive {
                rx_before: 10,
                rx_after: 11,
            }),
            None
        );
        assert_eq!(
            tcp_pool_probe_reconnect_reason(TcpPoolProbeOutcome::Closed),
            Some("transport_closed")
        );
        assert_eq!(
            tcp_pool_probe_reconnect_reason(TcpPoolProbeOutcome::SendFailed),
            Some("liveness_probe_send_failed")
        );
        assert_eq!(
            tcp_pool_probe_reconnect_reason(TcpPoolProbeOutcome::TimedOut { rx_datagrams: 10 }),
            Some("liveness_probe_timeout")
        );
    }

    #[test]
    fn tcp_pool_open_state_records_only_success_and_invalidates_failed_auxiliary() {
        let state = TcpPoolOpenState::new();
        state.note_open_failure(1);
        assert!(state.needs_reconnect(1));
        assert_eq!(state.last_success_age_secs(20), None);

        state.note_reconnect_success();
        assert!(!state.needs_reconnect(1));
        assert_eq!(state.generation(), 2);

        state.note_open_success(25);
        assert_eq!(state.last_success_age_secs(30), Some(5));

        state.note_open_failure(0);
        assert!(
            !state.needs_reconnect(0),
            "TCP opens cannot take authority away from primary UDP health"
        );
        assert_eq!(state.last_success_age_secs(30), Some(5));
    }

    #[test]
    fn tcp_pool_aux_failure_degrades_after_primary_connection() {
        assert_eq!(
            tcp_pool_aux_failure_action(0),
            TcpPoolAuxFailureAction::FailStartup,
            "without an authenticated primary connection startup must still fail"
        );
        assert_eq!(
            tcp_pool_aux_failure_action(1),
            TcpPoolAuxFailureAction::ContinueWithEstablished,
            "auxiliary TCP pool auth failure should not kill a usable TUIC data plane"
        );
        assert_eq!(
            tcp_pool_aux_failure_action(2),
            TcpPoolAuxFailureAction::ContinueWithEstablished,
            "later auxiliary failures should keep the already established pool"
        );
    }

    #[test]
    fn tcp_pool_aux_retry_delay_is_short_and_bounded() {
        assert_eq!(
            tcp_pool_aux_retry_delay(0),
            Some(Duration::from_millis(250))
        );
        assert_eq!(
            tcp_pool_aux_retry_delay(1),
            Some(Duration::from_millis(500))
        );
        assert_eq!(
            tcp_pool_aux_retry_delay(2),
            None,
            "the final configured attempt should surface the original failure"
        );
        assert_eq!(tcp_pool_aux_retry_delay(99), None);
    }

    #[test]
    fn tcp_pool_slot_lease_reserves_one_idle_owner() {
        let active = Arc::new(AtomicU64::new(0));
        {
            let (_first, first_idle) = TcpPoolSlotLease::reserve(active.clone());
            assert!(first_idle);
            assert_eq!(active.load(Ordering::Relaxed), 1);
            {
                let (_second, second_idle) = TcpPoolSlotLease::reserve(active.clone());
                assert!(!second_idle);
                assert_eq!(active.load(Ordering::Relaxed), 2);
            }
            assert_eq!(active.load(Ordering::Relaxed), 1);
        }
        assert_eq!(active.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn tcp_pool_activity_is_published_only_on_transitions() {
        let mut previous = None;

        assert_eq!(note_tcp_pool_activity_transition(&mut previous, 0), Some(0));
        assert_eq!(note_tcp_pool_activity_transition(&mut previous, 0), None);
        assert_eq!(note_tcp_pool_activity_transition(&mut previous, 4), Some(4));
        assert_eq!(note_tcp_pool_activity_transition(&mut previous, 2), Some(2));
        assert_eq!(note_tcp_pool_activity_transition(&mut previous, 2), None);
        assert_eq!(note_tcp_pool_activity_transition(&mut previous, 0), Some(0));
    }

    #[test]
    fn zero_rtt_defaults_off_and_parses_on() {
        assert!(!parse_zero_rtt(None)); // 默认关（quinn 0.10 限制）
        assert!(parse_zero_rtt(Some("true")));
        assert!(parse_zero_rtt(Some("1")));
        assert!(parse_zero_rtt(Some("on")));
        assert!(parse_zero_rtt(Some("YES")));
        // 其它一律关。
        assert!(!parse_zero_rtt(Some("false")));
        assert!(!parse_zero_rtt(Some("0")));
        assert!(!parse_zero_rtt(Some("maybe")));
    }

    #[test]
    fn config_requires_core_fields() {
        let bad = |s, u, p| TuicClientConfig::from_sources(s, u, p, None, None, None).is_err();
        assert!(bad(None, Some(UUID), Some("p"))); // no server
        assert!(bad(Some("1.2.3.4:8443"), None, Some("p"))); // no uuid
        assert!(bad(Some("1.2.3.4:8443"), Some(UUID), None)); // no password
        assert!(bad(Some("1.2.3.4:8443"), Some(UUID), Some(""))); // empty password
    }

    #[test]
    fn config_rejects_bad_server_and_uuid() {
        assert!(
            TuicClientConfig::from_sources(Some("nope"), Some(UUID), Some("p"), None, None, None)
                .is_err()
        );
        assert!(
            TuicClientConfig::from_sources(
                Some("1.2.3.4:8443"),
                Some("not-a-uuid"),
                Some("p"),
                None,
                None,
                None
            )
            .is_err()
        );
    }

    #[test]
    fn packet_ipv4_layout() {
        let p = encode_packet(7, &TargetAddr::IpPort("1.2.3.4:53".parse().unwrap()), b"hi");
        assert_eq!(&p[..2], &[0x05, 0x02]); // ver + Packet
        assert_eq!(&p[2..4], &[0x00, 0x07]); // assoc-id
        assert_eq!(&p[4..6], &[0x00, 0x00]); // pkt-id
        assert_eq!(p[6], 1); // frag total
        assert_eq!(p[7], 0); // frag id
        assert_eq!(&p[8..10], &[0x00, 0x02]); // size = 2
        assert_eq!(&p[10..15], &[0x01, 1, 2, 3, 4]); // atyp ipv4 + ip
        assert_eq!(&p[15..17], &[0x00, 0x35]); // port 53
        assert_eq!(&p[17..], b"hi");
    }

    #[test]
    fn packet_domain_roundtrips_assoc_and_data() {
        let p = encode_packet(
            9,
            &TargetAddr::DomainPort {
                host: "a.com".into(),
                port: 443,
            },
            b"q",
        );
        let (assoc, data) = decode_packet(&p).unwrap();
        assert_eq!(assoc, 9);
        assert_eq!(data, b"q");
    }

    #[test]
    fn packet_decode_rejects_truncated() {
        assert!(decode_packet(&[0u8; 5]).is_none());
        // size says 0, atyp domain len 200 overruns:
        assert!(decode_packet(&[0x05, 0x02, 0, 7, 0, 0, 1, 0, 0, 0, 0x00, 200]).is_none());
    }

    #[test]
    fn datagram_pressure_signal() {
        // 出向 datagram 缓冲剩余 < 1 MTU → 视为正在背压（quinn 会静默丢最老）。
        assert!(is_datagram_pressured(0, 1200));
        assert!(is_datagram_pressured(1199, 1200));
        assert!(!is_datagram_pressured(1200, 1200)); // 边界：刚好 1 MTU 不算压力
        assert!(!is_datagram_pressured(100_000, 1200));
    }

    #[test]
    fn format_udp_stats_includes_key_metrics() {
        let s = format_udp_stats(Some(1332), 7, 2, 180, 65535, 3, 1000, 4096);
        assert!(s.contains("stream 兜底=7"), "{s}");
        assert!(s.contains("丢弃=2"), "{s}");
        assert!(s.contains("RTT=180ms"), "{s}");
        assert!(s.contains("cwnd=65535"), "{s}");
        assert!(s.contains("3/1000"), "{s}"); // lost/sent
        assert!(s.contains("4096"), "{s}"); // send_buffer 余
    }

    #[test]
    fn udp_send_plan_native_is_size_based() {
        use UdpRelayMode::Native;
        use UdpSend::*;
        // Native：装得下 → datagram 快路径（含边界 ==max）——保刀3 现行语义，零回归。
        assert_eq!(udp_send_plan(Native, Some(1242), 1000), Datagram);
        assert_eq!(udp_send_plan(Native, Some(1242), 1242), Datagram);
        // 超上限 → stream 兜底。
        assert_eq!(udp_send_plan(Native, Some(1242), 1243), Stream);
        // datagram 不可用（对端不支持/未协商）→ stream。
        assert_eq!(udp_send_plan(Native, None, 100), Stream);
    }

    #[test]
    fn udp_send_plan_quic_is_always_stream() {
        use UdpRelayMode::Quic;
        use UdpSend::*;
        // Quic：无论装不装得下、datagram 是否可用，首包起恒走 uni-stream
        // （触发 TUIC server 镜像下行也走 stream，摆脱 datagram 天花板）。
        assert_eq!(udp_send_plan(Quic, Some(1242), 1000), Stream);
        assert_eq!(udp_send_plan(Quic, Some(1242), 1242), Stream);
        assert_eq!(udp_send_plan(Quic, Some(1242), 1243), Stream);
        assert_eq!(udp_send_plan(Quic, None, 100), Stream);
    }

    #[test]
    fn udp_relay_mode_parses() {
        assert_eq!(UdpRelayMode::parse("native"), Some(UdpRelayMode::Native));
        assert_eq!(UdpRelayMode::parse("quic"), Some(UdpRelayMode::Quic));
        assert_eq!(UdpRelayMode::parse("QUIC"), Some(UdpRelayMode::Quic));
        assert_eq!(UdpRelayMode::parse("nope"), None);
    }

    #[test]
    fn heartbeat_layout() {
        assert_eq!(encode_heartbeat(), vec![0x05, 0x04]);
    }

    fn meta(assoc: u16, pkt: u16, ftot: u8, fid: u8, data: &[u8]) -> PacketMeta<'_> {
        PacketMeta {
            assoc_id: assoc,
            pkt_id: pkt,
            frag_total: ftot,
            frag_id: fid,
            data,
        }
    }

    #[test]
    fn reassemble_single_fragment_passthrough() {
        let mut r = FragReassembler::new();
        // FRAG_TOTAL=1 立即返回整 payload，且不入表（无残留）。
        assert_eq!(
            r.accept(&meta(1, 0, 1, 0, b"hello"), 0),
            Some(b"hello".to_vec())
        );
        assert_eq!(r.pending_len(), 0);
    }

    #[test]
    fn reassemble_two_fragments_in_order() {
        let mut r = FragReassembler::new();
        assert_eq!(r.accept(&meta(5, 9, 2, 0, b"AB"), 0), None);
        assert_eq!(
            r.accept(&meta(5, 9, 2, 1, b"CD"), 0),
            Some(b"ABCD".to_vec())
        );
        assert_eq!(r.pending_len(), 0); // 集齐后清出
    }

    #[test]
    fn reassemble_two_fragments_out_of_order() {
        let mut r = FragReassembler::new();
        // 先到 frag_id=1，再到 0 → 仍按 frag_id 序拼接。
        assert_eq!(r.accept(&meta(5, 9, 2, 1, b"CD"), 0), None);
        assert_eq!(
            r.accept(&meta(5, 9, 2, 0, b"AB"), 0),
            Some(b"ABCD".to_vec())
        );
    }

    #[test]
    fn reassemble_duplicate_fragment_last_writer_wins() {
        let mut r = FragReassembler::new();
        // 重复 frag_id last-writer-wins：第二个 frag_id=0 覆盖第一个（received 不重复计数），
        // 既保跨重连残片被新 frag 顶替，又不破坏完成判定。
        assert_eq!(r.accept(&meta(5, 9, 2, 0, b"AB"), 0), None);
        assert_eq!(r.accept(&meta(5, 9, 2, 0, b"XX"), 0), None); // 覆盖 slot[0]
        assert_eq!(
            r.accept(&meta(5, 9, 2, 1, b"CD"), 0),
            Some(b"XXCD".to_vec())
        ); // 用较新的
    }

    #[test]
    fn reassemble_incomplete_swept_by_ttl() {
        let mut r = FragReassembler::new();
        assert_eq!(r.accept(&meta(5, 9, 3, 0, b"AB"), 0), None);
        assert_eq!(r.pending_len(), 1);
        r.sweep(11, 10); // first_seen=0，超 TTL=10 → 清
        assert_eq!(r.pending_len(), 0);
    }

    #[test]
    fn reassemble_rejects_bad_frag_id() {
        let mut r = FragReassembler::new();
        // frag_id >= frag_total → 丢弃，不入表（防越界）。
        assert_eq!(r.accept(&meta(5, 9, 2, 2, b"X"), 0), None);
        assert_eq!(r.pending_len(), 0);
        // frag_total=0 也无效。
        assert_eq!(r.accept(&meta(5, 9, 0, 0, b"X"), 0), None);
        assert_eq!(r.pending_len(), 0);
    }

    #[test]
    fn reassemble_cap_evicts_oldest() {
        let mut r = FragReassembler::with_cap(1);
        r.accept(&meta(1, 1, 2, 0, b"A"), 0); // partial #1，first_seen=0
        r.accept(&meta(2, 2, 2, 0, b"B"), 5); // 新 key 触发 evict 最老 → 仍 ≤cap
        assert_eq!(r.pending_len(), 1);
    }

    #[test]
    fn packet_meta_single_fragment() {
        // encode_packet 产出的单帧（FRAG_TOTAL=1）→ meta 各字段就位，data 即整 payload。
        let p = encode_packet(7, &TargetAddr::IpPort("1.2.3.4:53".parse().unwrap()), b"hi");
        let m = decode_packet_meta(&p).expect("meta");
        assert_eq!(m.assoc_id, 7);
        assert_eq!(m.pkt_id, 0);
        assert_eq!(m.frag_total, 1);
        assert_eq!(m.frag_id, 0);
        assert_eq!(m.data, b"hi");
    }

    #[test]
    fn packet_meta_non_first_fragment_skips_none_addr() {
        // 非首分片：ADDR=ATYP_NONE(0xff，跳 1 字节)，FRAG_ID=1，SIZE=本分片 chunk 长。
        // [ver type assoc(2)=9 pkt(2)=2 ftot=3 fid=1 size(2)=3 ATYP_NONE 'a' 'b' 'c']
        let buf = [
            0x05, 0x02, 0x00, 0x09, 0x00, 0x02, 0x03, 0x01, 0x00, 0x03, 0xff, b'a', b'b', b'c',
        ];
        let m = decode_packet_meta(&buf).expect("meta");
        assert_eq!(m.assoc_id, 9);
        assert_eq!(m.pkt_id, 2);
        assert_eq!(m.frag_total, 3);
        assert_eq!(m.frag_id, 1);
        assert_eq!(m.data, b"abc");
    }

    #[test]
    fn packet_meta_rejects_truncated() {
        assert!(decode_packet_meta(&[0u8; 5]).is_none());
        // size 说 200 但 buffer 不够 → None（不 panic）。
        assert!(
            decode_packet_meta(&[0x05, 0x02, 0, 7, 0, 0, 1, 0, 0, 200, 0x01, 1, 2, 3, 4]).is_none()
        );
    }

    #[test]
    fn decode_packet_delegates_to_meta() {
        // decode_packet 仍取 (assoc, data)，与 meta 一致（薄包装零回归）。
        let p = encode_packet(3, &TargetAddr::IpPort("1.2.3.4:53".parse().unwrap()), b"xy");
        assert_eq!(decode_packet(&p), Some((3, &b"xy"[..])));
    }

    #[test]
    fn heartbeat_only_while_udp_active() {
        let w = 60;
        // 从未发过 UDP(last=0)→ 不发,纯 TCP 会话靠 QUIC keepalive 保活。
        assert!(!should_send_heartbeat(0, 100, w));
        // 活跃窗口内(含同刻与边界 ==window)→ 发。
        assert!(should_send_heartbeat(100, 100, w));
        assert!(should_send_heartbeat(100, 130, w));
        assert!(should_send_heartbeat(100, 160, w)); // 边界 now-last==window
        // 超出活跃窗口 → 停发(此时也无活跃 flow 需要保活)。
        assert!(!should_send_heartbeat(100, 161, w));
    }

    fn tuple(p: u16) -> FourTuple {
        FourTuple {
            src_ip: std::net::Ipv4Addr::new(10, 0, 0, 1),
            src_port: p,
            dst_ip: std::net::Ipv4Addr::new(198, 18, 0, 5),
            dst_port: 443,
        }
    }

    #[test]
    fn assoc_intern_stable_and_unique() {
        let mut t = AssocTable::new();
        let a = t.intern(tuple(1000));
        assert_eq!(a, t.intern(tuple(1000)));
        assert_ne!(a, t.intern(tuple(1001)));
    }

    #[test]
    fn assoc_intern_skips_live_id_on_wraparound() {
        // u16 回绕后 next_id 可能落到仍在册的 id 上：intern 必须跳过，绝不覆盖活跃 flow。
        let mut t = AssocTable::with_cap(8);
        let keep = t.intern(tuple(1)); // 一条长寿命 flow 占住 id=keep
        t.next_id = keep; // 把分配游标强行推回 keep（模拟回绕撞上活跃 id）
        let other = t.intern(tuple(2));
        assert_ne!(other, keep, "intern 覆盖了仍在册的 flow");
        assert!(t.resolve(keep).is_some(), "长寿命 flow 被覆盖丢失");
        assert!(t.resolve(other).is_some());
        // 两条 flow 各自映射独立，未发生 tuple_to_id 泄漏/串号。
        assert_eq!(t.intern(tuple(1)), keep);
        assert_eq!(t.intern(tuple(2)), other);
    }

    #[test]
    fn assoc_contains_tracks_membership() {
        let mut t = AssocTable::new();
        assert!(!t.contains(&tuple(1000)));
        t.intern(tuple(1000));
        assert!(t.contains(&tuple(1000)));
        assert!(!t.contains(&tuple(1001)));
    }

    #[test]
    fn assoc_resolve_endpoints_and_sweep() {
        let mut t = AssocTable::new();
        let id = t.intern(tuple(1000));
        let e = t.resolve(id).expect("entry");
        assert_eq!(
            e.app_endpoint(),
            (std::net::Ipv4Addr::new(10, 0, 0, 1), 1000)
        );
        assert_eq!(
            e.target_src(),
            (std::net::Ipv4Addr::new(198, 18, 0, 5), 443)
        );
        t.sweep(61, 60);
        assert!(t.resolve(id).is_none());
    }

    /// 刀2：sweep 回收带 fake-IP 的 assoc 时，直接返回该 fake-IP（review #8：自包含返回）。
    #[test]
    fn assoc_sweep_reclaims_fake_ip() {
        let mut t = AssocTable::new();
        let id = t.intern(tuple(1000));
        t.set_fake_ip(id, Ipv4Addr::new(198, 18, 0, 5));
        t.touch(id, 0);
        // 未过期：不回收，返回空。
        assert!(t.sweep(30, 60).is_empty());
        // 过期：回收，返回含该 fake-IP。
        assert_eq!(t.sweep(61, 60), vec![Ipv4Addr::new(198, 18, 0, 5)]);
    }

    /// 刀2：LRU 驱逐带 fake-IP 的 assoc 时也累积 reclaimed（intern 到 cap 触发 evict）。
    #[test]
    fn assoc_evict_reclaims_fake_ip() {
        let mut t = AssocTable::with_cap(1);
        let id1 = t.intern(tuple(1));
        t.set_fake_ip(id1, Ipv4Addr::new(198, 18, 0, 9));
        // 再 intern 一条 → cap=1 触发 evict id1。
        let _id2 = t.intern(tuple(2));
        assert_eq!(
            t.take_reclaimed_fake_ips(),
            vec![Ipv4Addr::new(198, 18, 0, 9)]
        );
    }

    #[test]
    fn assoc_touch_keeps_alive() {
        let mut t = AssocTable::new();
        let id = t.intern(tuple(1000));
        t.touch(id, 50);
        t.sweep(100, 60);
        assert!(t.resolve(id).is_some());
    }

    #[test]
    fn assoc_lru_evicts_at_cap() {
        let mut t = AssocTable::with_cap(2);
        let a = t.intern(tuple(1));
        let _b = t.intern(tuple(2));
        let _c = t.intern(tuple(3));
        assert!(t.resolve(a).is_none());
        assert_eq!(t.len(), 2);
    }

    #[test]
    fn quic_config_builds_with_h3_alpn() {
        // TUIC 上游的 TLS 配置(自定义 ALPN)能构建 —— connect 的真验证在互通 e2e(Task 6)。
        assert!(
            crate::quic::client_quic_config_alpn(
                "certs/dev/ca-cert.pem",
                vec![b"h3".to_vec()],
                crate::quic::CcChoice::Bbr,
                crate::quic::MtuPolicy::Default,
                crate::quic::QuicGsoPolicy::Enabled,
                crate::quic::QuicPacingPolicy::QuinnDefault,
            )
            .is_ok()
        );
    }

    #[test]
    fn config_debug_redacts_credentials() {
        let c = TuicClientConfig::from_sources(
            Some("1.2.3.4:8443"),
            Some(UUID),
            Some("secret123"),
            None,
            None,
            None,
        )
        .unwrap();
        let s = format!("{c:?}");
        assert!(!s.contains("secret123"), "password leaked: {s}");
        assert!(!s.contains("550e8400"), "uuid leaked: {s}");
        assert!(s.contains("redacted"));
    }
}
