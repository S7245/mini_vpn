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
    DatagramUpstream, NativeTcpReadHalf, NativeTcpRelayStream, OpenedTcpRelay, ProxyUpstream,
    RelayFamilyPolicy, RelayStream, TcpRelayOpenDiag, TuicGenericStreamMode as TuicTcpRelayMode,
    TuicNativeOverride, current_tcp_relay_open_diag,
};
#[cfg(test)]
use crate::upstream::{
    NativeTcpChunk, NativeTcpReader, parse_tuic_tcp_relay_mode, select_tuic_native_relay_mode,
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
// Categorical one-step service class for a newly opened TCP stream. This is not a throughput
// knob: the stream returns to its prior Quinn priority on the first accepted business write.
const TUIC_TCP_STARTUP_PRIORITY_DELTA: i32 = 1;
const TUIC_TCP_UNORDERED_CHUNK_READ_MAX_BYTES: usize = 64 * 1024;
const TUIC_TCP_DIRECT_ORDERED_READ_MAX_BYTES: usize = TUIC_TCP_UNORDERED_CHUNK_READ_MAX_BYTES * 2;
const TUIC_TCP_UNORDERED_REASSEMBLY_MAX_BYTES: usize = 4 * 1024 * 1024;
const TUIC_TCP_UNORDERED_STAGING_LOG_MS: u128 = 1_000;
const TUIC_TCP_NATIVE_ORDERED_PUMP_CHANNEL_CHUNKS: usize = 64;
const TUIC_TCP_NATIVE_ORDERED_PUMP_READ_CHUNKS: usize = 64;

/// TUIC 客户端配置（单一事实源；桌面从 env 加载，移动端将来从 file/FFI 注入）。
/// 中文要点：凭据(uuid/password)经自定义 Debug **脱敏**，绝不随日志泄漏。
#[derive(Clone)]
pub struct TuicClientConfig {
    relay_policy: Arc<RelayFamilyPolicy>,
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
            relay_policy: Arc::new(RelayFamilyPolicy::default()),
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
    pub fn with_relay_policy(mut self, policy: Arc<RelayFamilyPolicy>) -> Self {
        self.relay_policy = policy;
        self
    }

    pub fn relay_policy(&self) -> &Arc<RelayFamilyPolicy> {
        &self.relay_policy
    }

    pub fn standalone_from_env() -> Result<Self, ClientError> {
        Self::from_env_with_relay_policy(Arc::new(RelayFamilyPolicy::from_env()))
    }

    pub fn from_env() -> Result<Self, ClientError> {
        Self::standalone_from_env()
    }

    pub fn from_env_with_relay_policy(policy: Arc<RelayFamilyPolicy>) -> Result<Self, ClientError> {
        let g = |k: &str| std::env::var(k).ok();
        let mut cfg = Self::from_sources(
            g("MINI_VPN_TUIC_SERVER").as_deref(),
            g("MINI_VPN_TUIC_UUID").as_deref(),
            g("MINI_VPN_TUIC_PASSWORD").as_deref(),
            g("MINI_VPN_TUIC_SNI").as_deref(),
            g("MINI_VPN_TUIC_CA_PATH").as_deref(),
            g("MINI_VPN_TUIC_ALPN").as_deref(),
        )?;
        cfg.relay_policy = policy;
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
        encoded.checked_sub(1)
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
    release: Option<tcp_pool_admission::LeaseReleaseReport>,
    drop_release: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TcpPoolReservationError {
    LeaseNotActive,
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
    service_normalized_ordering: bool,
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

    fn is_known(self) -> bool {
        matches!(
            self,
            Self::Known { cwnd, rtt_micros } if cwnd != 0 && rtt_micros != 0
        )
    }

    fn normalized_load_cmp(
        self,
        active: u64,
        other: Self,
        other_active: u64,
    ) -> Option<std::cmp::Ordering> {
        if active == 0 || other_active == 0 {
            return None;
        }
        let (
            Self::Known {
                cwnd: left_cwnd,
                rtt_micros: left_rtt,
            },
            Self::Known {
                cwnd: right_cwnd,
                rtt_micros: right_rtt,
            },
        ) = (self, other)
        else {
            return None;
        };
        if left_cwnd == 0 || left_rtt == 0 || right_cwnd == 0 || right_rtt == 0 {
            return None;
        }
        let left_numerator = u128::from(active) * u128::from(left_rtt);
        let right_numerator = u128::from(other_active) * u128::from(right_rtt);
        Some(compare_positive_rationals(
            left_numerator,
            u128::from(left_cwnd),
            right_numerator,
            u128::from(right_cwnd),
        ))
    }
}

fn compare_positive_rationals(
    mut left_numerator: u128,
    mut left_denominator: u128,
    mut right_numerator: u128,
    mut right_denominator: u128,
) -> std::cmp::Ordering {
    debug_assert!(left_denominator != 0 && right_denominator != 0);
    let mut reverse = false;
    loop {
        let left_quotient = left_numerator / left_denominator;
        let right_quotient = right_numerator / right_denominator;
        if left_quotient != right_quotient {
            let ordering = left_quotient.cmp(&right_quotient);
            return if reverse {
                ordering.reverse()
            } else {
                ordering
            };
        }

        let left_remainder = left_numerator % left_denominator;
        let right_remainder = right_numerator % right_denominator;
        let ordering = match (left_remainder == 0, right_remainder == 0) {
            (true, true) => return std::cmp::Ordering::Equal,
            (true, false) => std::cmp::Ordering::Less,
            (false, true) => std::cmp::Ordering::Greater,
            (false, false) => {
                left_numerator = left_denominator;
                left_denominator = left_remainder;
                right_numerator = right_denominator;
                right_denominator = right_remainder;
                reverse = !reverse;
                continue;
            }
        };
        return if reverse {
            ordering.reverse()
        } else {
            ordering
        };
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TcpPoolSuccessorServiceCertificate {
    identity: TcpPoolTransportIdentity,
    path_generation: u64,
    proved_cwnd_floor: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TcpPoolSuccessorForwardServiceProof {
    required_cwnd_floor: Option<u64>,
    initial_cwnd: u64,
    readiness_cwnd_floor: u64,
    final_cwnd: u64,
    rounds: u64,
    target_bytes: u64,
    sent_bytes: u64,
    acked_bytes: u64,
    lost_bytes: u64,
    path_generation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TcpPoolSuccessorInstallServiceProof {
    successor_identity: TcpPoolTransportIdentity,
    path_generation: u64,
    readiness_cwnd_floor: u64,
    final_cwnd: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TcpPoolSuccessorForwardServiceProgress {
    Complete,
    Continue,
    Stalled,
}

fn tcp_pool_successor_forward_service_progress(
    required_cwnd_floor: Option<u64>,
    previous_cwnd: u64,
    current_cwnd: u64,
) -> TcpPoolSuccessorForwardServiceProgress {
    if required_cwnd_floor.is_none_or(|floor| current_cwnd >= floor) {
        return TcpPoolSuccessorForwardServiceProgress::Complete;
    }
    if current_cwnd > previous_cwnd {
        TcpPoolSuccessorForwardServiceProgress::Continue
    } else {
        TcpPoolSuccessorForwardServiceProgress::Stalled
    }
}

async fn prove_tcp_pool_successor_forward_service(
    connection: &Connection,
    required_cwnd_floor: Option<u64>,
    deadline: tokio::time::Instant,
) -> Result<TcpPoolSuccessorForwardServiceProof, ClientError> {
    let required_cwnd_floor = required_cwnd_floor.filter(|floor| *floor != 0);
    let initial_cwnd = connection.stats().path.cwnd;
    let mut previous_cwnd = initial_cwnd;
    let mut proof = TcpPoolSuccessorForwardServiceProof {
        required_cwnd_floor,
        initial_cwnd,
        readiness_cwnd_floor: 0,
        final_cwnd: initial_cwnd,
        rounds: 0,
        target_bytes: 0,
        sent_bytes: 0,
        acked_bytes: 0,
        lost_bytes: 0,
        path_generation: 0,
    };

    loop {
        let turn = match tokio::time::timeout_at(deadline, connection.successor_service_turn())
            .await
        {
            Ok(result) => result.map_err(|error| {
                io_err(
                    "tuic tcp pool generation replacement",
                    format!(
                        "successor forward service proof failed at round {}: {error:?}",
                        proof.rounds.saturating_add(1)
                    ),
                )
            })?,
            Err(_) => {
                let partial = connection.successor_service_turn_stats();
                return Err(io_err(
                    "tuic tcp pool generation replacement",
                    format!(
                        "successor forward service proof exceeded the existing 5s replacement deadline after {} complete round(s): partial={partial:?}",
                        proof.rounds
                    ),
                ));
            }
        };

        if proof.rounds != 0 && proof.path_generation != turn.path_generation {
            return Err(io_err(
                "tuic tcp pool generation replacement",
                format!(
                    "successor forward service proof changed path between rounds: first={} current={}",
                    proof.path_generation, turn.path_generation
                ),
            ));
        }
        let current_path_generation = connection.current_path_generation();
        if current_path_generation != turn.path_generation {
            return Err(io_err(
                "tuic tcp pool generation replacement",
                format!(
                    "successor forward service proof no longer owns the current path: turn={} current={current_path_generation}",
                    turn.path_generation
                ),
            ));
        }
        proof.rounds = proof.rounds.saturating_add(1);
        proof.path_generation = turn.path_generation;
        proof.target_bytes = proof.target_bytes.saturating_add(turn.target_bytes);
        proof.sent_bytes = proof.sent_bytes.saturating_add(turn.sent_bytes);
        proof.acked_bytes = proof.acked_bytes.saturating_add(turn.acked_bytes);
        proof.lost_bytes = proof.lost_bytes.saturating_add(turn.lost_bytes);
        proof.final_cwnd = connection.stats().path.cwnd;
        if proof.readiness_cwnd_floor == 0 {
            proof.readiness_cwnd_floor = proof.final_cwnd;
        }

        match tcp_pool_successor_forward_service_progress(
            required_cwnd_floor,
            previous_cwnd,
            proof.final_cwnd,
        ) {
            TcpPoolSuccessorForwardServiceProgress::Complete => return Ok(proof),
            TcpPoolSuccessorForwardServiceProgress::Continue => {
                previous_cwnd = proof.final_cwnd;
            }
            TcpPoolSuccessorForwardServiceProgress::Stalled => {
                return Err(io_err(
                    "tuic tcp pool generation replacement",
                    format!(
                        "successor forward service proof made no cwnd progress below inherited floor: previous={} current={} required={} rounds={}",
                        previous_cwnd,
                        proof.final_cwnd,
                        required_cwnd_floor.unwrap_or_default(),
                        proof.rounds
                    ),
                ));
            }
        }
    }
}

impl TcpPoolSuccessorServiceCertificate {
    fn new(
        identity: TcpPoolTransportIdentity,
        path_generation: u64,
        proved_cwnd_floor: u64,
    ) -> Option<Self> {
        (proved_cwnd_floor != 0).then_some(Self {
            identity,
            path_generation,
            proved_cwnd_floor,
        })
    }
}

impl TcpPoolSuccessorInstallServiceProof {
    fn from_forward_service<T: TcpPoolTransport>(
        successor: &TcpPoolGeneration<T>,
        proof: &TcpPoolSuccessorForwardServiceProof,
    ) -> Option<Self> {
        let certificate = successor.successor_service_certificate?;
        (proof.path_generation == certificate.path_generation
            && proof.readiness_cwnd_floor == certificate.proved_cwnd_floor
            && proof.final_cwnd >= proof.readiness_cwnd_floor)
            .then_some(Self {
                successor_identity: successor.identity(),
                path_generation: proof.path_generation,
                readiness_cwnd_floor: proof.readiness_cwnd_floor,
                final_cwnd: proof.final_cwnd,
            })
    }

    #[cfg(test)]
    fn for_successor<T: TcpPoolTransport>(
        successor: &TcpPoolGeneration<T>,
        final_cwnd: u64,
    ) -> Option<Self> {
        let certificate = successor.successor_service_certificate?;
        (final_cwnd >= certificate.proved_cwnd_floor).then_some(Self {
            successor_identity: successor.identity(),
            path_generation: certificate.path_generation,
            readiness_cwnd_floor: certificate.proved_cwnd_floor,
            final_cwnd,
        })
    }

    fn matches_successor<T: TcpPoolTransport>(self, successor: &TcpPoolGeneration<T>) -> bool {
        successor
            .successor_service_certificate
            .is_some_and(|certificate| {
                self.successor_identity == successor.identity()
                    && certificate.identity == self.successor_identity
                    && certificate.path_generation == self.path_generation
                    && certificate.proved_cwnd_floor == self.readiness_cwnd_floor
                    && self.final_cwnd >= self.readiness_cwnd_floor
            })
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum TcpPoolSuccessorServiceReadiness {
    #[default]
    Unknown,
    Ready,
    StaleIdentity,
    StalePath,
    StaleCwnd,
}

impl TcpPoolSuccessorServiceReadiness {
    fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Ready => "ready",
            Self::StaleIdentity => "stale_identity",
            Self::StalePath => "stale_path",
            Self::StaleCwnd => "stale_cwnd",
        }
    }

    fn is_stale(self) -> bool {
        matches!(
            self,
            Self::StaleIdentity | Self::StalePath | Self::StaleCwnd
        )
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
        path_generation: Option<u64>,
        successor_service_certificate: Option<TcpPoolSuccessorServiceCertificate>,
    },
}

impl TcpPoolPathObservation {
    #[cfg(test)]
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
            path_generation: None,
            successor_service_certificate: None,
        }
    }

    #[cfg(test)]
    fn known_with_successor_service_certificate(
        identity: TcpPoolTransportIdentity,
        cwnd: u64,
        rtt: Duration,
        black_holes_detected: u64,
        path_generation: u64,
        proved_path_generation: u64,
        proved_cwnd_floor: u64,
    ) -> Self {
        Self::Known {
            identity,
            path_service: TcpPoolPathService::known(cwnd, rtt),
            black_holes_detected,
            path_generation: Some(path_generation),
            successor_service_certificate: TcpPoolSuccessorServiceCertificate::new(
                identity,
                proved_path_generation,
                proved_cwnd_floor,
            ),
        }
    }

    fn path_service(self) -> TcpPoolPathService {
        match self {
            Self::Unknown => TcpPoolPathService::Unknown,
            Self::Known { path_service, .. } => path_service,
        }
    }

    fn successor_service_readiness(self) -> TcpPoolSuccessorServiceReadiness {
        let Self::Known {
            identity,
            path_service,
            path_generation,
            successor_service_certificate: Some(certificate),
            ..
        } = self
        else {
            return TcpPoolSuccessorServiceReadiness::Unknown;
        };
        if certificate.identity != identity {
            return TcpPoolSuccessorServiceReadiness::StaleIdentity;
        }
        let Some(path_generation) = path_generation else {
            return TcpPoolSuccessorServiceReadiness::Unknown;
        };
        if certificate.path_generation != path_generation {
            return TcpPoolSuccessorServiceReadiness::StalePath;
        }
        match path_service {
            TcpPoolPathService::Known { cwnd, .. } if cwnd < certificate.proved_cwnd_floor => {
                TcpPoolSuccessorServiceReadiness::StaleCwnd
            }
            TcpPoolPathService::Known { .. } => TcpPoolSuccessorServiceReadiness::Ready,
            TcpPoolPathService::Unknown => TcpPoolSuccessorServiceReadiness::Unknown,
        }
    }

    fn successor_service_evidence(self) -> (Option<u64>, Option<u64>, Option<u64>) {
        let Self::Known {
            path_generation,
            successor_service_certificate,
            ..
        } = self
        else {
            return (None, None, None);
        };
        let (proved_path_generation, proved_cwnd_floor) = successor_service_certificate
            .map(|certificate| {
                (
                    Some(certificate.path_generation),
                    Some(certificate.proved_cwnd_floor),
                )
            })
            .unwrap_or((None, None));
        (path_generation, proved_path_generation, proved_cwnd_floor)
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
    successor_service_readiness: TcpPoolSuccessorServiceReadiness,
    successor_service_current_path_generation: Option<u64>,
    successor_service_proved_path_generation: Option<u64>,
    successor_service_proved_cwnd_floor: Option<u64>,
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

    #[cfg(test)]
    fn try_decide(
        &self,
        observations: &[TcpPoolPathObservation],
        replacement_allowed: &[bool],
    ) -> Result<TcpPoolAdmissionDecision, TcpPoolReservationError> {
        self.try_decide_excluding(observations, replacement_allowed, &[], false)
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
        let successor_service_readiness = successor_observation.successor_service_readiness();
        let (
            successor_service_current_path_generation,
            successor_service_proved_path_generation,
            successor_service_proved_cwnd_floor,
        ) = successor_observation.successor_service_evidence();
        let TcpPoolPathObservation::Known {
            identity,
            path_service,
            black_holes_detected,
            ..
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
                successor_service_readiness,
                successor_service_current_path_generation,
                successor_service_proved_path_generation,
                successor_service_proved_cwnd_floor,
                admitted: true,
            };
        }
        Ok(TcpPoolSlotReservation {
            index: replacement.index,
            lease,
            active_before: 0,
            path_service,
            path_service_tiebreak: false,
            service_normalized_ordering: false,
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
    /// Exact sum of occupied fragment payloads; only the reassembly transaction mutates it.
    payload_bytes: usize,
    first_seen: u64,
}

/// native 下行分片重组器（**纯状态机**，主循环独占、无锁，与 `AssocTable` 同寿）。
/// 中文要点：server native 模式把大下行包拆成多个 `FRAG_TOTAL>1` 的 Packet 命令，
/// 本器按 `(assoc_id, pkt_id)` 收集、集齐按 `frag_id` 序拼接还原整包。`FRAG_TOTAL==1` 直通快路径。
pub struct FragReassembler {
    partials: HashMap<(u16, u16), FragPartial>,
    cap: usize,
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
    gap_ack_reinforcements: u64,
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
         plpmtud(sent={},lost={},black_holes={}) frames(rx_stream={},rx_ack={},tx_ack={},gap_ack_reinforcements={}) \
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
        stats.gap_ack_reinforcements,
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
    relay_mode: &'static str,
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
    service_normalized_ordering: bool,
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
        service_normalized_ordering,
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
                "conn{}:id={identity},active={},qualification={},anchor={},current={},service_certificate={},service_current_path_generation={},service_proved_path_generation={},service_proved_cwnd_floor={},admitted={},cwnd={cwnd},rtt_us={rtt_us}",
                candidate.index,
                candidate.active_before,
                candidate.qualification.as_str(),
                optional_counter(candidate.qualification_anchor),
                optional_counter(candidate.black_holes_current),
                candidate.successor_service_readiness.as_str(),
                optional_counter(candidate.successor_service_current_path_generation),
                optional_counter(candidate.successor_service_proved_path_generation),
                optional_counter(candidate.successor_service_proved_cwnd_floor),
                candidate.admitted,
            )
        })
        .collect::<Vec<_>>()
        .join(";");
    format!(
        "🔎 tuic-tcp-pool-selection conn={conn_index} id={stable_id} policy=busy_epoch_forward_qualification_then_successor_service_certificate_then_service_normalized_load_then_least_active_then_path_service active_before={active_before} path_cwnd={path_cwnd} path_rtt_us={path_rtt_us} service_normalized_ordering={service_normalized_ordering} path_service_tiebreak={path_service_tiebreak} qualification={} black_hole_anchor={} black_holes_current={} qualification_override={qualification_override} all_degraded_fallback={all_degraded_fallback} candidates=[{candidates}] generation={generation} replacement_installed={replacement_installed} last_success_age_secs={last_success_age_secs} probe_result={probe_result} reconnect_reason={}",
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
    relay_mode: &'static str,
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
        relay_mode,
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
        gap_ack_reinforcements: stats.frame_tx.gap_ack_reinforcements,
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
        let mut writers = self
            .writers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let writer = self
            .next_writer
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        self.register_writer(stream, writer, &mut writers)
    }

    fn register_writer(
        self: &Arc<Self>,
        stream: u64,
        writer: u64,
        writers: &mut HashMap<u64, Weak<TcpWritePressureWriterState>>,
    ) -> TcpWritePressureWriter {
        let state = Arc::new(TcpWritePressureWriterState {
            writer,
            stream,
            episode: AtomicU64::new(0),
            pending_since_micros: AtomicU64::new(0),
            ack_stalled_since_micros: AtomicU64::new(0),
            acknowledged_bytes: AtomicU64::new(0),
            progress: OnceLock::new(),
        });
        writers.insert(writer, Arc::downgrade(&state));
        TcpWritePressureWriter {
            remove_on_drop: true,
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
    remove_on_drop: bool,
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
        if self.remove_on_drop {
            self.pressure.remove_writer(self.state.writer);
        }
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
}

impl EndpointRecoveryTrigger {
    fn label(self) -> &'static str {
        match self {
            Self::NoRx => "no_rx",
            Self::TcpWriteStall { .. } => "tcp_write_stall",
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
        }
    }

    fn tcp_read_fields(self) -> (usize, u64, u64, u64, u64, u64, u64, usize, u64, u64) {
        (0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EndpointRecoveryAction {
    None,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TcpPoolInstallServiceState {
    path_generation: u64,
    cwnd: u64,
    closed: bool,
}

trait TcpPoolTransport: Clone {
    fn tcp_pool_stable_id(&self) -> usize;
    fn tcp_pool_install_service_state(&self) -> Option<TcpPoolInstallServiceState>;
}

impl TcpPoolTransport for Connection {
    fn tcp_pool_stable_id(&self) -> usize {
        self.stable_id()
    }

    fn tcp_pool_install_service_state(&self) -> Option<TcpPoolInstallServiceState> {
        let cwnd = self.stats().path.cwnd;
        Some(TcpPoolInstallServiceState {
            path_generation: self.current_path_generation(),
            cwnd,
            closed: self.close_reason().is_some(),
        })
    }
}

#[cfg(test)]
impl TcpPoolTransport for u64 {
    fn tcp_pool_stable_id(&self) -> usize {
        usize::try_from(*self).unwrap_or(usize::MAX)
    }

    fn tcp_pool_install_service_state(&self) -> Option<TcpPoolInstallServiceState> {
        None
    }
}

struct TcpPoolGeneration<T> {
    transport: T,
    successor_service_certificate: Option<TcpPoolSuccessorServiceCertificate>,
    /// Exact current-service handoff for the active replacement attempt. A new attempt
    /// recomputes it from the immutable readiness certificate and current Quinn cwnd.
    replacement_forward_service_floor: Option<u64>,
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
            successor_service_certificate: None,
            replacement_forward_service_floor: None,
            activity: Arc::new(TcpPoolGenerationActivity::new()),
            write_pressure: Arc::new(TcpWritePressure::new(clock)),
            read_pressure: Arc::new(TcpOrderedReadPressure::new()),
            open_state: Arc::new(TcpPoolOpenState::new_at_generation(generation)),
            auth_attempts,
        }
    }

    fn new_successor(
        transport: T,
        generation: u64,
        auth_attempts: u64,
        clock: Instant,
        path_generation: u64,
        proved_cwnd_floor: u64,
    ) -> Option<Self> {
        let mut successor = Self::new(transport, generation, auth_attempts, clock);
        successor.successor_service_certificate = TcpPoolSuccessorServiceCertificate::new(
            successor.identity(),
            path_generation,
            proved_cwnd_floor,
        );
        successor.replacement_forward_service_floor = Some(proved_cwnd_floor).filter(|v| *v != 0);
        successor
            .successor_service_certificate
            .is_some()
            .then_some(successor)
    }

    fn identity(&self) -> TcpPoolTransportIdentity {
        TcpPoolTransportIdentity::new(
            self.transport.tcp_pool_stable_id(),
            self.open_state.generation(),
        )
    }

    fn record_forward_service_floor(&mut self, observed_cwnd: u64) -> Option<u64> {
        if observed_cwnd == 0 {
            return None;
        }
        let floor = self
            .successor_service_certificate
            .map(|certificate| certificate.proved_cwnd_floor)
            .unwrap_or(0)
            .max(observed_cwnd);
        self.replacement_forward_service_floor = Some(floor);
        Some(floor)
    }

    fn note_reconnect_success(&mut self) {
        self.successor_service_certificate = None;
        self.replacement_forward_service_floor = None;
        self.open_state.note_reconnect_success();
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
    InsufficientForwardService,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TcpPoolReplacementCurrentServiceHandoffError {
    StalePredecessor,
    ZeroWindow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TcpPoolReplacementCurrentServiceHandoff {
    observed_cwnd: u64,
    required_cwnd_floor: u64,
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
    async fn replacement_forward_service_floor(
        &self,
        expected_identity: TcpPoolTransportIdentity,
    ) -> Result<Option<u64>, TcpPoolGenerationInstallError> {
        let state = self.state.lock().await;
        if state.current.identity() != expected_identity {
            return Err(TcpPoolGenerationInstallError::StalePredecessor);
        }
        Ok(state.current.replacement_forward_service_floor)
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
        let proved_forward_service = TcpPoolSuccessorInstallServiceProof::for_successor(
            &successor,
            successor.replacement_forward_service_floor.unwrap_or(0),
        )
        .ok_or(TcpPoolGenerationInstallError::InvalidSuccessor)?;
        self.install_successor_with(
            expected_identity,
            expected_activity,
            successor,
            proved_forward_service,
            started_at,
            |_, _| true,
        )
        .await
    }

    #[cfg(test)]
    async fn install_successor_with_proved_service(
        &self,
        expected_identity: TcpPoolTransportIdentity,
        expected_activity: &Arc<TcpPoolGenerationActivity>,
        successor: TcpPoolGeneration<T>,
        final_cwnd: u64,
        started_at: Instant,
    ) -> Result<TcpPoolGenerationInstall, TcpPoolGenerationInstallError> {
        let proved_forward_service =
            TcpPoolSuccessorInstallServiceProof::for_successor(&successor, final_cwnd)
                .ok_or(TcpPoolGenerationInstallError::InvalidSuccessor)?;
        self.install_successor_with(
            expected_identity,
            expected_activity,
            successor,
            proved_forward_service,
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
        proved_forward_service: TcpPoolSuccessorInstallServiceProof,
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
        let successor_current_service = successor.transport.tcp_pool_install_service_state();
        if successor_identity.stable_id == expected_identity.stable_id
            || expected_identity.generation.checked_add(1) != Some(successor_identity.generation)
            || !proved_forward_service.matches_successor(&successor)
            || successor_current_service.is_some_and(|current| {
                current.closed || current.path_generation != proved_forward_service.path_generation
            })
        {
            return Err(TcpPoolGenerationInstallError::InvalidSuccessor);
        }
        if successor_current_service
            .is_some_and(|current| current.cwnd < proved_forward_service.final_cwnd)
        {
            return Err(TcpPoolGenerationInstallError::InsufficientForwardService);
        }
        if state
            .current
            .replacement_forward_service_floor
            .is_some_and(|required| proved_forward_service.final_cwnd < required)
        {
            return Err(TcpPoolGenerationInstallError::InsufficientForwardService);
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

impl TcpPoolGenerationSlot<Connection> {
    async fn prepare_replacement_current_service_handoff(
        &self,
        expected_identity: TcpPoolTransportIdentity,
    ) -> Result<TcpPoolReplacementCurrentServiceHandoff, TcpPoolReplacementCurrentServiceHandoffError>
    {
        let mut state = self.state.lock().await;
        if state.current.identity() != expected_identity {
            return Err(TcpPoolReplacementCurrentServiceHandoffError::StalePredecessor);
        }
        let observed_cwnd = state.current.transport.stats().path.cwnd;
        let required_cwnd_floor = state
            .current
            .record_forward_service_floor(observed_cwnd)
            .ok_or(TcpPoolReplacementCurrentServiceHandoffError::ZeroWindow)?;
        Ok(TcpPoolReplacementCurrentServiceHandoff {
            observed_cwnd,
            required_cwnd_floor,
        })
    }
}

struct TcpPoolConnectionSelection {
    conn_index: usize,
    conn: Connection,
    lease: TcpPoolSlotLease,
    active_before: u64,
    path_service: TcpPoolPathService,
    path_service_tiebreak: bool,
    service_normalized_ordering: bool,
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

/// TUIC DATA handle; the legacy constructor remains during NodeHost migration.
pub struct TuicUpstream {
    relay_policy: Arc<RelayFamilyPolicy>,
    endpoint: Endpoint,
    udp_send_service_policy: quic::QuicUdpSendServicePolicy,
    udp_send_service_stats: Option<quic::QuicUdpSendServiceStats>,
    udp_runtime: Arc<node_runtime::UdpRuntime>,
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
    udp_stream_sender: std::sync::Mutex<udp_send::UdpStreamSender>,
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
#[path = "tuic/tests/tcp_fixture.rs"]
pub(crate) mod tcp_fixture;
mod tcp_open;
mod tcp_pool_admission;
use tcp_open::TuicTcpStartupWriter;
mod tcp_readers;
#[cfg(test)]
use tcp_readers::{
    DirectOrderedNativeChunkRecv, TuicOrderedNativeChunkSource, combine_ordered_native_chunks,
};
use tcp_readers::{
    TrackedRelayStream, TuicNativeOrderedPumpReader, TuicNativeOrderedReader, TuicNativeTcpReader,
    TuicNativeTcpWriter,
};
mod node;
pub use node::{NodeHost, NodeShutdown, NodeStartup};
mod node_runtime;
use node_runtime::{
    spawn_endpoint_pacing_stats_logger, spawn_quic_stats_logger,
    spawn_udp_send_service_stats_logger,
};
mod udp_reassembly;
mod udp_send;

#[cfg(test)]
mod tests;
