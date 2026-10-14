//! Proxy upstream abstraction (Stage 13a).
//!
//! 中文要点：把「代理出口」收成一个 trait —— 给「目标」开一条中继流。TUIC 是第一个实现;
//! 将来 VLESS+REALITY 等只需新增一个 impl(见 ADR-0004 的两层扩展模型)。legacy(yamux)路径在
//! 退役前仍走原内联逻辑(零回归),不强行套进 trait。

use crate::shared::{ClientError, TargetAddr};
use bytes::Bytes;
use std::future::Future;
use std::io;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite};

/// 统一的中继流类型:legacy(yamux compat)与 tuic(QUIC 双向流 compat)都收成它,喂给同一套双向泵。
/// 中文要点:用 boxed trait object 收口,避免给 enum 手写 AsyncRead/Write 的易错样板(系统稳定优先)。
pub trait AsyncStream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> AsyncStream for T {}

/// 一条到出口的中继流(双向字节)。
pub type RelayStream = Box<dyn AsyncStream>;

/// Generic byte-stream pump selection; independent of the transport's native overrides.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TcpRelayEngine {
    Legacy,
    ThinStaging,
    ContinuousPump,
    PermitPump,
    OrderedEgressPermitPump,
}

impl TcpRelayEngine {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Legacy => "legacy",
            Self::ThinStaging => "thin_staging",
            Self::ContinuousPump => "continuous_pump",
            Self::PermitPump => "permit_pump",
            Self::OrderedEgressPermitPump => "ordered_egress_permit_pump",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeTcpRelayEngine {
    ChunkPump,
    PermitPump,
}

impl NativeTcpRelayEngine {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ChunkPump => "native_chunk_pump",
            Self::PermitPump => "native_permit_pump",
        }
    }
}

/// These are the only modes a Generic TUIC stream can return.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TuicGenericStreamMode {
    OrderedJoin,
    OrderedChunk,
    UnorderedReassembly,
}

impl TuicGenericStreamMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::OrderedJoin => "ordered_join",
            Self::OrderedChunk => "ordered_chunk",
            Self::UnorderedReassembly => "unordered_reassembly_diag",
        }
    }
}

/// A native override applies only to open_tcp_relay, never to open_tcp.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TuicNativeOverride {
    None,
    NativeChunkPump,
    NativeOrderedPump,
    D16DirectOrdered,
}

impl TuicNativeOverride {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::NativeChunkPump => "native_chunk_pump_diag",
            Self::NativeOrderedPump => "native_ordered_pump_diag",
            Self::D16DirectOrdered => "d16_direct_ordered",
        }
    }
}

/// Pure startup inputs. Missing/invalid values disable the corresponding selector.
#[derive(Default)]
pub struct RelayFamilyPolicySources<'a> {
    pub ordered_egress_permit: Option<&'a str>,
    pub permit: Option<&'a str>,
    pub continuous: Option<&'a str>,
    pub thin: Option<&'a str>,
    pub native_egress_permit: Option<&'a str>,
    pub native_read_floor: Option<&'a str>,
    pub tuic_unordered_reassembly: Option<&'a str>,
    pub tuic_ordered_chunk: Option<&'a str>,
    pub tuic_native_chunk: Option<&'a str>,
    pub tuic_native_ordered: Option<&'a str>,
    pub d16_byte_owned: Option<&'a str>,
}

/// Immutable family decisions shared by one command's TUN and upstream owners.
/// Owns no transport, task or admission. Constructors are the only parsing boundary;
/// per-flow consumers read these values without consulting process environment.
/// Public contract tests live in tests/relay_family_policy.rs.
#[derive(Debug, PartialEq, Eq)]
pub struct RelayFamilyPolicy {
    generic: TcpRelayEngine,
    native: NativeTcpRelayEngine,
    native_read_floor: bool,
    tuic_generic: TuicGenericStreamMode,
    tuic_native: TuicNativeOverride,
}

impl Default for RelayFamilyPolicy {
    fn default() -> Self {
        Self::from_sources(RelayFamilyPolicySources::default())
    }
}

impl RelayFamilyPolicy {
    pub fn from_sources(s: RelayFamilyPolicySources<'_>) -> Self {
        Self {
            generic: select_tcp_relay_engine(
                s.ordered_egress_permit,
                s.permit,
                s.continuous,
                s.thin,
            ),
            native: select_native_tcp_relay_engine(s.native_egress_permit),
            native_read_floor: strict_flag(s.native_read_floor),
            tuic_generic: parse_tuic_tcp_relay_mode(
                s.tuic_unordered_reassembly,
                s.tuic_ordered_chunk,
            ),
            tuic_native: select_tuic_native_relay_mode(
                strict_flag(s.d16_byte_owned),
                tuic_flag(s.tuic_native_ordered),
                tuic_flag(s.tuic_native_chunk),
            )
            .unwrap_or(TuicNativeOverride::None),
        }
    }

    pub fn from_env() -> Self {
        let ordered = std::env::var("MINI_VPN_D11_ORDERED_EGRESS_PERMIT").ok();
        let permit = std::env::var("MINI_VPN_D2_PERMIT_TCP_RELAY").ok();
        let continuous = std::env::var("MINI_VPN_CONTINUOUS_TCP_RELAY").ok();
        let thin = std::env::var("MINI_VPN_THIN_TCP_RELAY").ok();
        let native = std::env::var("MINI_VPN_D6_NATIVE_EGRESS_PERMIT").ok();
        let floor = std::env::var("MINI_VPN_D15_NATIVE_READ_FLOOR").ok();
        let unordered = std::env::var("MINI_VPN_TUIC_TCP_UNORDERED_REASSEMBLY").ok();
        let chunk = std::env::var("MINI_VPN_TUIC_TCP_ORDERED_CHUNK").ok();
        let native_chunk = std::env::var("MINI_VPN_TUIC_TCP_NATIVE_CHUNK_PUMP").ok();
        let native_ordered = std::env::var("MINI_VPN_TUIC_TCP_NATIVE_ORDERED_PUMP").ok();
        let d16 = std::env::var("MINI_VPN_H10D16_BYTE_OWNED_EGRESS").ok();
        Self::from_sources(RelayFamilyPolicySources {
            ordered_egress_permit: ordered.as_deref(),
            permit: permit.as_deref(),
            continuous: continuous.as_deref(),
            thin: thin.as_deref(),
            native_egress_permit: native.as_deref(),
            native_read_floor: floor.as_deref(),
            tuic_unordered_reassembly: unordered.as_deref(),
            tuic_ordered_chunk: chunk.as_deref(),
            tuic_native_chunk: native_chunk.as_deref(),
            tuic_native_ordered: native_ordered.as_deref(),
            d16_byte_owned: d16.as_deref(),
        })
    }

    pub fn generic_engine(&self) -> TcpRelayEngine {
        self.generic
    }
    pub fn native_engine(&self) -> NativeTcpRelayEngine {
        self.native
    }
    pub fn native_read_floor(&self) -> bool {
        self.native_read_floor
    }
    pub fn tuic_generic_mode(&self) -> TuicGenericStreamMode {
        self.tuic_generic
    }
    pub fn tuic_native_override(&self) -> TuicNativeOverride {
        self.tuic_native
    }
    pub fn d16_byte_owned(&self) -> bool {
        self.tuic_native == TuicNativeOverride::D16DirectOrdered
    }
}

fn strict_flag(s: Option<&str>) -> bool {
    matches!(
        s.map(|v| v.trim().to_ascii_lowercase()).as_deref(),
        Some("1" | "true")
    )
}

fn tuic_flag(s: Option<&str>) -> bool {
    matches!(
        s.map(|v| v.trim().to_ascii_lowercase()).as_deref(),
        Some("1" | "true" | "on" | "yes")
    )
}

pub(crate) fn select_tcp_relay_engine(
    ordered: Option<&str>,
    permit: Option<&str>,
    continuous: Option<&str>,
    thin: Option<&str>,
) -> TcpRelayEngine {
    if strict_flag(ordered) {
        TcpRelayEngine::OrderedEgressPermitPump
    } else if strict_flag(permit) {
        TcpRelayEngine::PermitPump
    } else if strict_flag(continuous) {
        TcpRelayEngine::ContinuousPump
    } else if strict_flag(thin) {
        TcpRelayEngine::ThinStaging
    } else {
        TcpRelayEngine::Legacy
    }
}

pub(crate) fn select_native_tcp_relay_engine(permit: Option<&str>) -> NativeTcpRelayEngine {
    if strict_flag(permit) {
        NativeTcpRelayEngine::PermitPump
    } else {
        NativeTcpRelayEngine::ChunkPump
    }
}

pub(crate) fn parse_tuic_tcp_relay_mode(
    unordered: Option<&str>,
    chunk: Option<&str>,
) -> TuicGenericStreamMode {
    if tuic_flag(unordered) {
        TuicGenericStreamMode::UnorderedReassembly
    } else if tuic_flag(chunk) {
        TuicGenericStreamMode::OrderedChunk
    } else {
        TuicGenericStreamMode::OrderedJoin
    }
}

pub(crate) fn select_tuic_native_relay_mode(
    d16: bool,
    ordered: bool,
    chunk: bool,
) -> Option<TuicNativeOverride> {
    if d16 {
        Some(TuicNativeOverride::D16DirectOrdered)
    } else if ordered {
        Some(TuicNativeOverride::NativeOrderedPump)
    } else if chunk {
        Some(TuicNativeOverride::NativeChunkPump)
    } else {
        None
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeTcpChunk {
    pub offset: u64,
    pub bytes: Bytes,
}

pub trait NativeTcpReader: Unpin + Send {
    /// Poll one transport-owned TCP chunk. Ordered readers selected by D16
    /// must return the next contiguous chunk and never return more than
    /// `max_len` bytes. D16 callers provide a nonzero byte reservation.
    fn poll_read_chunk(
        &mut self,
        cx: &mut Context<'_>,
        max_len: usize,
    ) -> Poll<io::Result<Option<NativeTcpChunk>>>;
}

pub type NativeTcpReadHalf = Box<dyn NativeTcpReader>;
pub type NativeTcpWriteHalf = Box<dyn AsyncWrite + Unpin + Send>;

pub struct NativeTcpRelayStream {
    pub reader: NativeTcpReadHalf,
    pub writer: NativeTcpWriteHalf,
}

pub enum OpenedTcpRelay {
    Generic(RelayStream),
    Native(NativeTcpRelayStream),
    NativeByteOwned(NativeTcpRelayStream),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TcpRelayOpenDiag {
    pub handle: String,
    pub epoch: u64,
}

tokio::task_local! {
    static TCP_RELAY_OPEN_DIAG: TcpRelayOpenDiag;
}

pub async fn with_tcp_relay_open_diag<F, T>(diag: TcpRelayOpenDiag, future: F) -> T
where
    F: Future<Output = T>,
{
    TCP_RELAY_OPEN_DIAG.scope(diag, future).await
}

pub fn current_tcp_relay_open_diag() -> Option<TcpRelayOpenDiag> {
    TCP_RELAY_OPEN_DIAG.try_with(Clone::clone).ok()
}

/// A value-only request. Construction must not reserve a pool slot, spawn, or perform I/O.
pub struct TcpOpenRequest {
    pub target: TargetAddr,
    pub deadline: std::time::Instant,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TcpOpenUnavailable {
    Unsupported,
    InvalidTarget,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TcpOpenCancel {
    Cancelled,
    Deadline,
    Shutdown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TcpOpenPhase {
    Unstarted,
    Acquiring,
    Reserved,
    Opening,
    Connecting,
    Preparing,
    Ready,
    Transferred,
    Retiring,
    Settled,
    Quarantined,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TcpOpenView {
    pub phase: TcpOpenPhase,
    pub lease_held: bool,
    pub retirement_pending: bool,
    pub connect_bytes: usize,
    pub connect_written: usize,
}

/// One persistent resource owner, stored by the control root BEFORE its first step.
/// Steps are bounded control turns, not Futures: the caller supplies a polling cadence.
/// After take_carrier, stop/join every actual role before calling settle; its receipt
/// covers provider resources only. Cancel/settle deadlines are independent of open,
/// fixed on first use and may only become earlier. Drop is never a settlement receipt.
pub trait TcpOpenAttempt<C>: Send {
    fn step(&mut self, cx: &mut Context<'_>, now: std::time::Instant);
    fn view(&self) -> TcpOpenView;
    fn cancel(&mut self, reason: TcpOpenCancel, deadline: std::time::Instant);
    fn take_carrier(&mut self, now: std::time::Instant) -> Option<C>;
    fn settle(
        &mut self,
        cx: &mut Context<'_>,
        now: std::time::Instant,
        deadline: std::time::Instant,
    );
    fn failure(&self) -> Option<&ClientError>;
}

pub type StagedTcpOpen<C> = Result<Box<dyn TcpOpenAttempt<C>>, TcpOpenUnavailable>;

/// 代理上游:给一个 Target 开一条到出口的 TCP 中继流。
/// 中文要点:async fn 要在 `Box<dyn ProxyUpstream>` 上分发,用成熟的 `async-trait`。
#[async_trait::async_trait]
pub trait ProxyUpstream: Send + Sync {
    fn staged_tcp_open(
        self: std::sync::Arc<Self>,
        _request: TcpOpenRequest,
    ) -> StagedTcpOpen<OpenedTcpRelay> {
        Err(TcpOpenUnavailable::Unsupported)
    }

    async fn open_tcp(&self, target: &TargetAddr) -> Result<RelayStream, ClientError>;

    async fn open_tcp_relay(&self, target: &TargetAddr) -> Result<OpenedTcpRelay, ClientError> {
        self.open_tcp(target).await.map(OpenedTcpRelay::Generic)
    }

    /// `open_tcp` 是否足够廉价、可在单任务主循环里 inline await 而不 stall 其它 flow（刀9 M3 / 刀14d）。
    /// 中文要点：默认 **true** 只适合明确不会等待网络/flow-control 的测试或轻量 mock。生产 Transport 若
    /// `open_tcp` 可能等待重连、握手、开流或写 Connect 头，必须 override 成 false，让主循环 spawn async
    /// remote-open，避免一条慢 open 饿死所有 flow。
    fn open_is_cheap(&self) -> bool {
        true
    }

    /// 当前 TCP 选腿编码（刀11 可观测性）：默认 [`NO_FAILOVER`](crate::metrics::NO_FAILOVER)——
    /// 非 failover 上游（纯 TUIC / 纯 REALITY 单腿）无选腿概念。仅 `FailoverUpstream` override 返回
    /// `state().active_leg().as_u8()`（0=TUIC / 1=REALITY）。30s tick 周期采样、**不在 UDP 数据路径**
    /// （独立 Relaxed 读 → 不破 ADR-0011 的 send_udp 铁律）。
    fn failover_leg_u8(&self) -> u8 {
        crate::metrics::NO_FAILOVER
    }
}

/// 代理上游的 UDP/datagram 面:把一条已编码好的 datagram 发往出口。
///
/// 中文要点:与 [`ProxyUpstream`](TCP) 并列。TUIC 的 `send_udp` 是第一个实现;并发压测的 mock
/// 上游是第二个,使 `run_event_loop` 的 UDP 上行能脱离真网络。**下行接收端不在 trait 里**——它是
/// 主循环 select 的一条独立分支(`mpsc::Receiver`),由调用方作参数注入(生产=`start_udp()`,
/// harness=mock echo 回环 channel),见 knife1 spec 决策 Q6。
#[async_trait::async_trait]
pub trait DatagramUpstream: Send + Sync {
    async fn send_udp(&self, datagram: Vec<u8>);

    /// 上行 UDP datagram 累计丢弃（刀11 可观测性）：默认 0；`TuicUpstream` override 返回
    /// `udp_drop_count()`，`FailoverUpstream` 转发 tuic 腿（UDP 恒 TUIC）。snapshot 时读，与新增的
    /// **下行** drop（`Metrics.udp_drops_down`）严格分离。
    fn udp_drops_up(&self) -> u64 {
        0
    }
    /// 上行走 uni-stream 兜底累计次数（刀11 可观测性）：默认 0；`TuicUpstream` override 返回
    /// `udp_stream_fallback_count()`，`FailoverUpstream` 转发 tuic 腿。
    fn udp_stream_fallbacks(&self) -> u64 {
        0
    }
}

#[cfg(test)]
mod tests;
