//! Bounded TCP ownership port between the TUN owner and the resumable-session
//! supervisor.
//!
//! This module deliberately contains no socket or transport loop.  The TUN
//! side owns [`ResumableTcpFlow`]; one session supervisor owns
//! [`ResumableTcpDriver`]. Uplink DATA and CLOSE share one ordered source lane;
//! terminal RESET and sink completions use physically separate bounded lanes.
//! DATA is admitted only after its message slot, exact byte permits, and one
//! per-flow plus session-global replay-segment permit are reserved. Capacity
//! remains attached through reducer replay ownership until exact ACK or
//! terminal release.

use std::fmt;
use std::sync::{
    Arc, Mutex, MutexGuard,
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
};

use bytes::Bytes;
use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};

use crate::resumable::{
    ByteOffset, Direction, FlowFinishReason, LocalFlow, MAX_DATA_PAYLOAD_BYTES, ReplayAcknowledged,
    ReplayStored, ResetReason, SessionEffect, SessionEvent, SessionFlowId, SessionId,
    SinkHalfClose, SinkOffer, TcpDataSegment,
};

use super::supervisor::TcpFactoryOrigin;

const TERMINAL_RESET_LANE_CAPACITY: usize = 1;
const TERMINAL_ACTION_LANE_CAPACITY: usize = 1;
const CONTROL_BURST_LIMIT: usize = 8;

/// Exact queue and byte bounds for one TCP flow port.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FlowPortConfig {
    uplink_byte_capacity: usize,
    uplink_data_messages: usize,
    control_messages: usize,
    downlink_actions: usize,
}

impl FlowPortConfig {
    pub fn new(
        uplink_byte_capacity: usize,
        uplink_data_messages: usize,
        control_messages: usize,
        downlink_actions: usize,
    ) -> Result<Self, FlowPortConfigError> {
        if uplink_byte_capacity == 0 {
            return Err(FlowPortConfigError::ZeroUplinkByteCapacity);
        }
        if u32::try_from(uplink_byte_capacity).is_err() {
            return Err(FlowPortConfigError::UplinkByteCapacityTooLarge {
                value: uplink_byte_capacity,
            });
        }
        if uplink_data_messages == 0 {
            return Err(FlowPortConfigError::ZeroUplinkMessageCapacity);
        }
        validate_channel_capacity("uplink DATA", uplink_data_messages)?;
        if control_messages == 0 {
            return Err(FlowPortConfigError::ZeroControlCapacity);
        }
        validate_channel_capacity("control", control_messages)?;
        if downlink_actions == 0 {
            return Err(FlowPortConfigError::ZeroDownlinkCapacity);
        }
        validate_channel_capacity("downlink", downlink_actions)?;
        Ok(Self {
            uplink_byte_capacity,
            uplink_data_messages,
            control_messages,
            downlink_actions,
        })
    }

    pub const fn uplink_byte_capacity(self) -> usize {
        self.uplink_byte_capacity
    }

    pub const fn uplink_data_messages(self) -> usize {
        self.uplink_data_messages
    }

    pub const fn control_messages(self) -> usize {
        self.control_messages
    }

    pub const fn downlink_actions(self) -> usize {
        self.downlink_actions
    }
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum FlowPortConfigError {
    #[error("uplink byte capacity must be non-zero")]
    ZeroUplinkByteCapacity,
    #[error("uplink byte capacity {value} exceeds the semaphore's exact u32 permit range")]
    UplinkByteCapacityTooLarge { value: usize },
    #[error("uplink DATA message capacity must be non-zero")]
    ZeroUplinkMessageCapacity,
    #[error("per-flow uplink replay segment capacity must be non-zero")]
    ZeroUplinkSegmentCapacity,
    #[error("session uplink replay segment capacity must be non-zero")]
    ZeroSessionUplinkSegmentCapacity,
    #[error("control message capacity must be non-zero")]
    ZeroControlCapacity,
    #[error("downlink action capacity must be non-zero")]
    ZeroDownlinkCapacity,
    #[error("{lane} channel capacity {value} exceeds Tokio's bound {max}")]
    ChannelCapacityTooLarge {
        lane: &'static str,
        value: usize,
        max: usize,
    },
    #[error("{scope} uplink replay segment capacity {value} exceeds Tokio's bound {max}")]
    SegmentCapacityTooLarge {
        scope: &'static str,
        value: usize,
        max: usize,
    },
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum FlowPortError {
    #[error("uplink DATA length must be between 1 and {MAX_DATA_PAYLOAD_BYTES} bytes, got {len}")]
    InvalidUplinkLength { len: usize },
    #[error("uplink extractor returned {actual} bytes after {reserved} bytes were reserved")]
    UplinkLengthMismatch { reserved: usize, actual: usize },
    #[error("uplink source byte offset space is exhausted")]
    UplinkOffsetExhausted,
    #[error("replay storage receipt does not match the admitted uplink extent")]
    UplinkReplayReceiptMismatch,
    #[error("uplink ownership is already bound to a replay extent")]
    UplinkOwnershipAlreadyBound,
    #[error("application ACK does not match the retained uplink replay extent")]
    UplinkAckMismatch,
    #[error("terminal reducer effect does not match the retained uplink replay extent")]
    UplinkTerminalMismatch,
    #[error("ordered uplink source message lane is full")]
    UplinkMessageLaneFull,
    #[error("an ordered uplink source reservation is already outstanding")]
    UplinkSourceReservationActive,
    #[error("control message lane is full")]
    ControlLaneFull,
    #[error("terminal reset lane already contains a reset")]
    TerminalLaneFull,
    #[error("downlink action lane is full")]
    DownlinkLaneFull,
    #[error("flow port is closed")]
    Closed,
    #[error("uplink byte budget cannot admit {requested} bytes; {available} are available")]
    UplinkByteBudgetExhausted { requested: usize, available: usize },
    #[error("session uplink byte budget cannot admit {requested} bytes; {available} are available")]
    UplinkGlobalByteBudgetExhausted { requested: usize, available: usize },
    #[error("uplink replay segment budget is exhausted; {available} segments are available")]
    UplinkSegmentBudgetExhausted { available: usize },
    #[error(
        "session uplink replay segment budget is exhausted; {available} segments are available"
    )]
    UplinkGlobalSegmentBudgetExhausted { available: usize },
    #[error("flow factory is already bound to another resumable session")]
    CrossSessionFlowFactory,
    #[error("flow factory direction {expected:?} does not match capability direction {actual:?}")]
    FlowFactoryDirectionMismatch {
        expected: Direction,
        actual: Direction,
    },
    #[error("local flow authority {attempted} is not newer than factory high-water {highest}")]
    LocalFlowAuthorityNotMonotonic { highest: u64, attempted: u64 },
    #[error("sink capability belongs to another flow")]
    CrossFlowSinkCapability,
    #[error("sink capability direction {actual:?} does not match expected {expected:?}")]
    SinkDirectionMismatch {
        expected: Direction,
        actual: Direction,
    },
    #[error("local sink capability was revoked by flow terminal authority")]
    SinkCapabilityRevoked,
    #[error("terminal effect belongs to another flow")]
    CrossFlowTerminalEffect,
    #[error("session effect is not a TUN terminal action")]
    NotTerminalEffect,
    #[error("flow terminal action was already delivered")]
    TerminalAlreadyDelivered,
    #[error("sink delivery segments do not exactly cover the offered extent")]
    InvalidSinkCoverage,
    #[error("sink reported {accepted} bytes but the exposed first segment contains {available}")]
    InvalidSinkAcceptance { accepted: usize, available: usize },
}

/// Cloneable handle to one session's exact aggregate uplink ownership budget.
///
/// Every flow port in the same resumable session must be constructed with a
/// clone of the same ledger. Per-flow limits remain independently enforced by
/// [`FlowPortConfig`].
#[derive(Clone)]
struct UplinkByteLedger {
    budget: Arc<Semaphore>,
    capacity: usize,
}

#[derive(Clone, Copy, Debug)]
enum FactoryFlowGate {
    Unbound,
    Bound {
        session_id: SessionId,
        direction: Direction,
        highest_authority_id: u64,
    },
}

impl FactoryFlowGate {
    fn admit(&mut self, flow: LocalFlow) -> Result<(), FlowPortError> {
        match self {
            Self::Unbound => {
                *self = Self::Bound {
                    session_id: flow.session_id(),
                    direction: flow.direction(),
                    highest_authority_id: flow.authority_id(),
                };
                Ok(())
            }
            Self::Bound {
                session_id,
                direction,
                highest_authority_id,
            } => {
                if *session_id != flow.session_id() {
                    return Err(FlowPortError::CrossSessionFlowFactory);
                }
                if *direction != flow.direction() {
                    return Err(FlowPortError::FlowFactoryDirectionMismatch {
                        expected: *direction,
                        actual: flow.direction(),
                    });
                }
                let attempted = flow.authority_id();
                if attempted <= *highest_authority_id {
                    return Err(FlowPortError::LocalFlowAuthorityNotMonotonic {
                        highest: *highest_authority_id,
                        attempted,
                    });
                }
                *highest_authority_id = attempted;
                Ok(())
            }
        }
    }
}

/// Cloneable session-global replay-segment ledger. One permit represents one
/// reducer-owned DATA extent, independent of its byte length.
#[derive(Clone)]
struct UplinkSegmentLedger {
    budget: Arc<Semaphore>,
    capacity: usize,
}

impl UplinkSegmentLedger {
    fn new(capacity: usize) -> Result<Self, FlowPortConfigError> {
        validate_segment_capacity(
            "session",
            capacity,
            FlowPortConfigError::ZeroSessionUplinkSegmentCapacity,
        )?;
        Ok(Self {
            budget: Arc::new(Semaphore::new(capacity)),
            capacity,
        })
    }

    const fn capacity(&self) -> usize {
        self.capacity
    }

    fn available_segments(&self) -> usize {
        self.budget.available_permits()
    }

    fn owned_segments(&self) -> usize {
        self.capacity.saturating_sub(self.available_segments())
    }
}

impl UplinkByteLedger {
    pub fn new(capacity: usize) -> Result<Self, FlowPortConfigError> {
        if capacity == 0 {
            return Err(FlowPortConfigError::ZeroUplinkByteCapacity);
        }
        if u32::try_from(capacity).is_err() {
            return Err(FlowPortConfigError::UplinkByteCapacityTooLarge { value: capacity });
        }
        Ok(Self {
            budget: Arc::new(Semaphore::new(capacity)),
            capacity,
        })
    }

    pub const fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn available_bytes(&self) -> usize {
        self.budget.available_permits()
    }

    pub fn owned_bytes(&self) -> usize {
        self.capacity.saturating_sub(self.available_bytes())
    }
}

impl fmt::Debug for UplinkByteLedger {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UplinkByteLedger")
            .field("capacity", &self.capacity)
            .field("owned_bytes", &self.owned_bytes())
            .finish()
    }
}

/// Session-scoped constructor for every TCP port owned by one resumable
/// session. Clones share the exact aggregate byte and replay-segment ledgers,
/// so an adapter cannot accidentally create fresh "global" budgets per flow.
#[derive(Clone)]
pub struct ResumableTcpPortFactory {
    config: FlowPortConfig,
    session_uplink_ledger: UplinkByteLedger,
    per_flow_uplink_segment_capacity: usize,
    session_uplink_segment_ledger: UplinkSegmentLedger,
    flow_gate: Arc<Mutex<FactoryFlowGate>>,
    origin: Option<TcpFactoryOrigin>,
}

impl ResumableTcpPortFactory {
    /// Isolated constructor for TCP unit tests and fake adapter fixtures.
    /// Production factories are minted exactly once by `SessionSupervisor` and
    /// carry its opaque origin capability.
    #[cfg(test)]
    pub(crate) fn new_unbound_for_test(
        config: FlowPortConfig,
        session_uplink_byte_capacity: usize,
        per_flow_uplink_segment_capacity: usize,
        session_uplink_segment_capacity: usize,
    ) -> Result<Self, FlowPortConfigError> {
        Self::new_internal(
            config,
            session_uplink_byte_capacity,
            per_flow_uplink_segment_capacity,
            session_uplink_segment_capacity,
            FactoryFlowGate::Unbound,
            None,
        )
    }

    pub(super) fn new_bound(
        config: FlowPortConfig,
        session_id: SessionId,
        direction: Direction,
        session_uplink_byte_capacity: usize,
        per_flow_uplink_segment_capacity: usize,
        session_uplink_segment_capacity: usize,
        origin: TcpFactoryOrigin,
    ) -> Result<Self, FlowPortConfigError> {
        Self::new_internal(
            config,
            session_uplink_byte_capacity,
            per_flow_uplink_segment_capacity,
            session_uplink_segment_capacity,
            FactoryFlowGate::Bound {
                session_id,
                direction,
                highest_authority_id: 0,
            },
            Some(origin),
        )
    }

    fn new_internal(
        config: FlowPortConfig,
        session_uplink_byte_capacity: usize,
        per_flow_uplink_segment_capacity: usize,
        session_uplink_segment_capacity: usize,
        flow_gate: FactoryFlowGate,
        origin: Option<TcpFactoryOrigin>,
    ) -> Result<Self, FlowPortConfigError> {
        validate_segment_capacity(
            "per-flow",
            per_flow_uplink_segment_capacity,
            FlowPortConfigError::ZeroUplinkSegmentCapacity,
        )?;
        Ok(Self {
            config,
            session_uplink_ledger: UplinkByteLedger::new(session_uplink_byte_capacity)?,
            per_flow_uplink_segment_capacity,
            session_uplink_segment_ledger: UplinkSegmentLedger::new(
                session_uplink_segment_capacity,
            )?,
            flow_gate: Arc::new(Mutex::new(flow_gate)),
            origin,
        })
    }

    /// Fault-injection seam proving that a reducer-capacity miss after source
    /// consumption is quarantined. Production cloning deliberately cannot
    /// create these independent ledgers.
    #[cfg(test)]
    pub(crate) fn fork_with_independent_ledgers_for_test(
        &self,
    ) -> Result<Self, FlowPortConfigError> {
        let gate = *self
            .flow_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Self::new_internal(
            self.config,
            self.session_uplink_byte_capacity(),
            self.per_flow_uplink_segment_capacity,
            self.session_uplink_segment_capacity(),
            gate,
            self.origin.clone(),
        )
    }

    pub const fn config(&self) -> FlowPortConfig {
        self.config
    }

    pub fn session_uplink_byte_capacity(&self) -> usize {
        self.session_uplink_ledger.capacity()
    }

    pub fn session_uplink_owned_bytes(&self) -> usize {
        self.session_uplink_ledger.owned_bytes()
    }

    pub const fn per_flow_uplink_segment_capacity(&self) -> usize {
        self.per_flow_uplink_segment_capacity
    }

    pub fn session_uplink_segment_capacity(&self) -> usize {
        self.session_uplink_segment_ledger.capacity()
    }

    pub fn session_uplink_owned_segments(&self) -> usize {
        self.session_uplink_segment_ledger.owned_segments()
    }

    pub fn open_flow(
        &self,
        flow: LocalFlow,
    ) -> Result<(ResumableTcpFlow, ResumableTcpDriver), FlowPortError> {
        self.flow_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .admit(flow)?;
        Ok(ResumableTcpFlow::bounded_pair(
            flow,
            self.config,
            self.session_uplink_ledger.clone(),
            self.per_flow_uplink_segment_capacity,
            self.session_uplink_segment_ledger.clone(),
            self.origin.clone(),
        ))
    }
}

impl fmt::Debug for ResumableTcpPortFactory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResumableTcpPortFactory")
            .field("config", &self.config)
            .field(
                "session_uplink_byte_capacity",
                &self.session_uplink_byte_capacity(),
            )
            .field(
                "session_uplink_owned_bytes",
                &self.session_uplink_owned_bytes(),
            )
            .field(
                "per_flow_uplink_segment_capacity",
                &self.per_flow_uplink_segment_capacity(),
            )
            .field(
                "session_uplink_segment_capacity",
                &self.session_uplink_segment_capacity(),
            )
            .field(
                "session_uplink_owned_segments",
                &self.session_uplink_owned_segments(),
            )
            .finish()
    }
}

struct PortState {
    uplink_budget: Arc<Semaphore>,
    uplink_byte_capacity: usize,
    session_uplink_ledger: UplinkByteLedger,
    uplink_segment_budget: Arc<Semaphore>,
    uplink_segment_capacity: usize,
    session_uplink_segment_ledger: UplinkSegmentLedger,
    pending_downlink_actions: AtomicUsize,
    reserved_completion_slots: AtomicUsize,
    observed_leg_losses: AtomicU64,
    next_uplink_offset: AtomicU64,
    source_reservation_active: AtomicBool,
    ordered_source_closed: AtomicBool,
    retired: AtomicBool,
    admission_gate: Mutex<()>,
    origin: Option<TcpFactoryOrigin>,
}

impl PortState {
    fn new(
        uplink_byte_capacity: usize,
        session_uplink_ledger: UplinkByteLedger,
        uplink_segment_capacity: usize,
        session_uplink_segment_ledger: UplinkSegmentLedger,
        origin: Option<TcpFactoryOrigin>,
    ) -> Self {
        Self {
            uplink_budget: Arc::new(Semaphore::new(uplink_byte_capacity)),
            uplink_byte_capacity,
            session_uplink_ledger,
            uplink_segment_budget: Arc::new(Semaphore::new(uplink_segment_capacity)),
            uplink_segment_capacity,
            session_uplink_segment_ledger,
            pending_downlink_actions: AtomicUsize::new(0),
            reserved_completion_slots: AtomicUsize::new(0),
            observed_leg_losses: AtomicU64::new(0),
            next_uplink_offset: AtomicU64::new(0),
            source_reservation_active: AtomicBool::new(false),
            ordered_source_closed: AtomicBool::new(false),
            retired: AtomicBool::new(false),
            admission_gate: Mutex::new(()),
            origin,
        }
    }

    fn admission_guard(&self) -> MutexGuard<'_, ()> {
        self.admission_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn is_retired(&self) -> bool {
        self.retired.load(Ordering::Acquire)
    }

    fn snapshot(&self) -> FlowPortSnapshot {
        FlowPortSnapshot {
            uplink_owned_bytes: self
                .uplink_byte_capacity
                .saturating_sub(self.uplink_budget.available_permits()),
            uplink_owned_segments: self
                .uplink_segment_capacity
                .saturating_sub(self.uplink_segment_budget.available_permits()),
            pending_downlink_actions: self.pending_downlink_actions.load(Ordering::Acquire),
            reserved_completion_slots: self.reserved_completion_slots.load(Ordering::Acquire),
            observed_leg_losses: self.observed_leg_losses.load(Ordering::Acquire),
        }
    }
}

/// Read-only ownership probe usable after both live endpoints have dropped.
#[derive(Clone)]
pub struct FlowPortProbe {
    state: Arc<PortState>,
}

impl FlowPortProbe {
    pub fn snapshot(&self) -> FlowPortSnapshot {
        self.state.snapshot()
    }
}

impl fmt::Debug for FlowPortProbe {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FlowPortProbe")
            .field("snapshot", &self.snapshot())
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FlowPortSnapshot {
    uplink_owned_bytes: usize,
    uplink_owned_segments: usize,
    pending_downlink_actions: usize,
    reserved_completion_slots: usize,
    observed_leg_losses: u64,
}

impl FlowPortSnapshot {
    pub const fn uplink_owned_bytes(self) -> usize {
        self.uplink_owned_bytes
    }

    pub const fn uplink_owned_segments(self) -> usize {
        self.uplink_owned_segments
    }

    pub const fn pending_downlink_actions(self) -> usize {
        self.pending_downlink_actions
    }

    pub const fn reserved_completion_slots(self) -> usize {
        self.reserved_completion_slots
    }

    pub const fn observed_leg_losses(self) -> u64 {
        self.observed_leg_losses
    }

    pub const fn is_quiescent(self) -> bool {
        self.uplink_owned_bytes == 0
            && self.uplink_owned_segments == 0
            && self.pending_downlink_actions == 0
            && self.reserved_completion_slots == 0
    }
}

/// TUN/smoltcp-side endpoint for one resumable TCP flow.
///
/// This endpoint is intentionally not `Clone`: it is the single local owner
/// of the flow's socket boundary.
pub struct ResumableTcpFlow {
    flow: LocalFlow,
    source_tx: mpsc::Sender<UplinkSource>,
    completion_liveness_tx: mpsc::Sender<FlowControl>,
    reset_tx: mpsc::Sender<LocalReset>,
    downlink_rx: mpsc::Receiver<TunAction>,
    terminal_rx: mpsc::Receiver<TunAction>,
    state: Arc<PortState>,
}

impl ResumableTcpFlow {
    fn bounded_pair(
        flow: LocalFlow,
        config: FlowPortConfig,
        session_uplink_ledger: UplinkByteLedger,
        per_flow_uplink_segment_capacity: usize,
        session_uplink_segment_ledger: UplinkSegmentLedger,
        origin: Option<TcpFactoryOrigin>,
    ) -> (Self, ResumableTcpDriver) {
        let (source_tx, source_rx) = mpsc::channel(config.uplink_data_messages);
        let (control_tx, control_rx) = mpsc::channel(config.control_messages);
        let (reset_tx, reset_rx) = mpsc::channel(TERMINAL_RESET_LANE_CAPACITY);
        let (downlink_tx, downlink_rx) = mpsc::channel(config.downlink_actions);
        let (terminal_tx, terminal_rx) = mpsc::channel(TERMINAL_ACTION_LANE_CAPACITY);
        let completion_tx = control_tx.downgrade();
        let state = Arc::new(PortState::new(
            config.uplink_byte_capacity,
            session_uplink_ledger,
            per_flow_uplink_segment_capacity,
            session_uplink_segment_ledger,
            origin,
        ));
        (
            Self {
                flow,
                source_tx,
                completion_liveness_tx: control_tx,
                reset_tx,
                downlink_rx,
                terminal_rx,
                state: Arc::clone(&state),
            },
            ResumableTcpDriver {
                flow,
                source_rx,
                control_rx,
                reset_rx,
                downlink_tx,
                terminal_tx,
                completion_tx,
                state,
                consecutive_controls: 0,
            },
        )
    }

    pub const fn local_flow(&self) -> LocalFlow {
        self.flow
    }

    pub fn probe(&self) -> FlowPortProbe {
        FlowPortProbe {
            state: Arc::clone(&self.state),
        }
    }

    /// Reserves one ordered source slot plus `max_bytes` of exact flow and
    /// session capacity before a socket source is touched.
    pub(crate) fn try_reserve_uplink_source(
        &self,
        max_bytes: usize,
    ) -> Result<UplinkSourceReservation, FlowPortError> {
        reserve_uplink_source(self.flow, &self.source_tx, &self.state, max_bytes)
    }

    /// Splits the single TUN-side owner into independently movable, still
    /// non-cloneable uplink and downlink halves. This lets the adapter keep
    /// pre-recv admission beside `SocketCtx` while a spawned receiver forwards
    /// downlink actions; neither half aliases its mutable channel endpoint.
    pub fn into_split(self) -> (ResumableTcpUplink, ResumableTcpDownlink) {
        (
            ResumableTcpUplink {
                flow: self.flow,
                source_tx: self.source_tx,
                _completion_liveness_tx: self.completion_liveness_tx,
                reset_tx: self.reset_tx,
                state: Arc::clone(&self.state),
            },
            ResumableTcpDownlink {
                flow: self.flow,
                downlink_rx: self.downlink_rx,
                terminal_rx: self.terminal_rx,
                state: self.state,
            },
        )
    }

    /// Atomically admits one uplink extent before touching the socket-backed
    /// extractor.  Both the bounded DATA slot and exact byte permit are held
    /// before `extractor` is invoked.
    pub fn try_send_uplink_with<F>(
        &self,
        byte_len: usize,
        extractor: F,
    ) -> Result<(), FlowPortError>
    where
        F: FnOnce() -> Bytes,
    {
        try_send_uplink_parts(self.flow, &self.source_tx, &self.state, byte_len, extractor)
    }

    /// Exact-owned extractor variant for the smoltcp receive boundary. The
    /// normal `Vec` path (`len == capacity`) transfers its allocation directly
    /// into `Bytes` without a second payload copy. Excess capacity is compacted
    /// before queue ownership so hostile backing remains bounded.
    pub fn try_send_uplink_vec_with<F>(
        &self,
        byte_len: usize,
        extractor: F,
    ) -> Result<(), FlowPortError>
    where
        F: FnOnce() -> Vec<u8>,
    {
        try_send_uplink_vec_parts(self.flow, &self.source_tx, &self.state, byte_len, extractor)
    }

    pub fn try_send_close(&self) -> Result<(), FlowPortError> {
        send_ordered_close(&self.source_tx, &self.state, self.flow)
    }

    pub fn try_send_reset(&self, reason: ResetReason) -> Result<(), FlowPortError> {
        send_reset(&self.reset_tx, &self.state, self.flow, reason)
    }

    /// Relinquish a flow returned by an asynchronous open that can no longer
    /// be installed into its original TUN socket incarnation. Consuming the
    /// endpoint prevents later installation while the reserved reset lane
    /// leaves one exact terminal fact for the session supervisor.
    pub fn abandon_before_install(self) -> Result<(), FlowPortError> {
        self.try_send_reset(ResetReason::LocalAbandon)
    }

    pub fn try_recv_action(&mut self) -> Result<Option<TunAction>, FlowPortError> {
        try_recv_tun_action(&mut self.terminal_rx, &mut self.downlink_rx, &self.state)
    }

    pub async fn recv_action(&mut self) -> Option<TunAction> {
        recv_tun_action(&mut self.terminal_rx, &mut self.downlink_rx, &self.state).await
    }
}

impl fmt::Debug for ResumableTcpFlow {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResumableTcpFlow")
            .field("flow_id", &self.flow.flow_id())
            .field("snapshot", &self.state.snapshot())
            .finish_non_exhaustive()
    }
}

/// Independently owned TUN-side uplink/control half.
pub struct ResumableTcpUplink {
    flow: LocalFlow,
    source_tx: mpsc::Sender<UplinkSource>,
    _completion_liveness_tx: mpsc::Sender<FlowControl>,
    reset_tx: mpsc::Sender<LocalReset>,
    state: Arc<PortState>,
}

impl ResumableTcpUplink {
    pub const fn local_flow(&self) -> LocalFlow {
        self.flow
    }

    pub fn probe(&self) -> FlowPortProbe {
        FlowPortProbe {
            state: Arc::clone(&self.state),
        }
    }

    /// Reserves one ordered source slot plus `max_bytes` of exact flow and
    /// session capacity before a socket source is touched.
    pub(crate) fn try_reserve_uplink_source(
        &self,
        max_bytes: usize,
    ) -> Result<UplinkSourceReservation, FlowPortError> {
        reserve_uplink_source(self.flow, &self.source_tx, &self.state, max_bytes)
    }

    pub fn try_send_uplink_with<F>(
        &self,
        byte_len: usize,
        extractor: F,
    ) -> Result<(), FlowPortError>
    where
        F: FnOnce() -> Bytes,
    {
        try_send_uplink_parts(self.flow, &self.source_tx, &self.state, byte_len, extractor)
    }

    pub fn try_send_uplink_vec_with<F>(
        &self,
        byte_len: usize,
        extractor: F,
    ) -> Result<(), FlowPortError>
    where
        F: FnOnce() -> Vec<u8>,
    {
        try_send_uplink_vec_parts(self.flow, &self.source_tx, &self.state, byte_len, extractor)
    }

    pub fn try_send_close(&self) -> Result<(), FlowPortError> {
        send_ordered_close(&self.source_tx, &self.state, self.flow)
    }

    pub fn try_send_reset(&self, reason: ResetReason) -> Result<(), FlowPortError> {
        send_reset(&self.reset_tx, &self.state, self.flow, reason)
    }
}

impl fmt::Debug for ResumableTcpUplink {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResumableTcpUplink")
            .field("flow_id", &self.flow.flow_id())
            .field("snapshot", &self.state.snapshot())
            .finish_non_exhaustive()
    }
}

/// Independently owned TUN-side downlink half. It is the sole receiver of
/// local sink/half-close/terminal actions and therefore cannot be cloned.
pub struct ResumableTcpDownlink {
    flow: LocalFlow,
    downlink_rx: mpsc::Receiver<TunAction>,
    terminal_rx: mpsc::Receiver<TunAction>,
    state: Arc<PortState>,
}

impl ResumableTcpDownlink {
    pub const fn local_flow(&self) -> LocalFlow {
        self.flow
    }

    pub fn probe(&self) -> FlowPortProbe {
        FlowPortProbe {
            state: Arc::clone(&self.state),
        }
    }

    pub fn try_recv_action(&mut self) -> Result<Option<TunAction>, FlowPortError> {
        try_recv_tun_action(&mut self.terminal_rx, &mut self.downlink_rx, &self.state)
    }

    pub async fn recv_action(&mut self) -> Option<TunAction> {
        recv_tun_action(&mut self.terminal_rx, &mut self.downlink_rx, &self.state).await
    }
}

impl fmt::Debug for ResumableTcpDownlink {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResumableTcpDownlink")
            .field("flow_id", &self.flow.flow_id())
            .field("snapshot", &self.state.snapshot())
            .finish_non_exhaustive()
    }
}

/// Single-supervisor endpoint for one resumable TCP flow.
///
/// Receiving [`UplinkData`] releases neither byte nor replay-segment permits.
/// The supervisor must bind [`UplinkOwnership`] to the exact DATA frame emitted
/// by the reducer and retain [`UplinkReplayOwnership`] until an application ACK
/// accepted by that reducer covers the whole extent.
pub struct ResumableTcpDriver {
    flow: LocalFlow,
    source_rx: mpsc::Receiver<UplinkSource>,
    control_rx: mpsc::Receiver<FlowControl>,
    reset_rx: mpsc::Receiver<LocalReset>,
    downlink_tx: mpsc::Sender<TunAction>,
    terminal_tx: mpsc::Sender<TunAction>,
    completion_tx: mpsc::WeakSender<FlowControl>,
    state: Arc<PortState>,
    consecutive_controls: usize,
}

impl ResumableTcpDriver {
    pub const fn local_flow(&self) -> LocalFlow {
        self.flow
    }

    pub fn probe(&self) -> FlowPortProbe {
        FlowPortProbe {
            state: Arc::clone(&self.state),
        }
    }

    /// Records a transport-leg loss without closing either flow lane.  Flow
    /// lifetime belongs to the session reducer, not to a particular leg.
    pub fn note_leg_lost(&self) {
        let _ = self.state.observed_leg_losses.fetch_update(
            Ordering::AcqRel,
            Ordering::Acquire,
            |value| Some(value.saturating_add(1)),
        );
    }

    /// Nonblocking weighted-priority receive. A terminal reset always wins.
    /// Ordinary sink completions may run for a bounded burst, after which one
    /// ready ordered source fact must make progress.
    pub fn try_recv_next(&mut self) -> Result<Option<DriverInput>, FlowPortError> {
        match self.reset_rx.try_recv() {
            Ok(reset) => return Ok(Some(DriverInput::Control(reset.into_session_event()))),
            Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected) => {}
        }
        if self.consecutive_controls < CONTROL_BURST_LIMIT {
            match self.control_rx.try_recv() {
                Ok(control) => {
                    self.consecutive_controls = self.consecutive_controls.saturating_add(1);
                    return Ok(Some(DriverInput::Control(control.into_session_event())));
                }
                Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected) => {
                }
            }
        }
        match self.source_rx.try_recv() {
            Ok(source) => {
                self.consecutive_controls = 0;
                return Ok(Some(source.into_driver_input()));
            }
            Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected) => {}
        }
        match self.control_rx.try_recv() {
            Ok(control) => {
                self.consecutive_controls = self.consecutive_controls.saturating_add(1);
                Ok(Some(DriverInput::Control(control.into_session_event())))
            }
            Err(mpsc::error::TryRecvError::Empty) => Ok(None),
            Err(mpsc::error::TryRecvError::Disconnected) => {
                if self.reset_rx.is_closed() && self.source_rx.is_closed() {
                    Err(FlowPortError::Closed)
                } else {
                    Ok(None)
                }
            }
        }
    }

    /// Awaitable counterpart to [`Self::try_recv_next`]. The biased select
    /// uses the same bounded control burst. One closed lane never terminates
    /// the supervisor while another lane can still produce a reserved fact.
    pub async fn recv_next(&mut self) -> Option<DriverInput> {
        loop {
            match self.try_recv_next() {
                Ok(Some(input)) => return Some(input),
                Ok(None) => {}
                Err(FlowPortError::Closed) => return None,
                Err(_) => return None,
            }

            let reset_open = !self.reset_rx.is_closed();
            let control_open = !self.control_rx.is_closed();
            let source_open = !self.source_rx.is_closed();
            if !reset_open && !control_open && !source_open {
                return None;
            }
            if self.consecutive_controls >= CONTROL_BURST_LIMIT {
                tokio::select! {
                    biased;
                    reset = self.reset_rx.recv(), if reset_open => if let Some(reset) = reset {
                        return Some(DriverInput::Control(reset.into_session_event()));
                    },
                    source = self.source_rx.recv(), if source_open => if let Some(source) = source {
                        self.consecutive_controls = 0;
                        return Some(source.into_driver_input());
                    },
                    control = self.control_rx.recv(), if control_open => if let Some(control) = control {
                        self.consecutive_controls = self.consecutive_controls.saturating_add(1);
                        return Some(DriverInput::Control(control.into_session_event()));
                    },
                    else => return None,
                }
            } else {
                tokio::select! {
                    biased;
                    reset = self.reset_rx.recv(), if reset_open => if let Some(reset) = reset {
                        return Some(DriverInput::Control(reset.into_session_event()));
                    },
                    control = self.control_rx.recv(), if control_open => if let Some(control) = control {
                        self.consecutive_controls = self.consecutive_controls.saturating_add(1);
                        return Some(DriverInput::Control(control.into_session_event()));
                    },
                    source = self.source_rx.recv(), if source_open => if let Some(source) = source {
                        self.consecutive_controls = 0;
                        return Some(source.into_driver_input());
                    },
                    else => return None,
                }
            }
        }
    }

    /// Queues a typed sink offer only after reserving the exact future
    /// completion slot.  Thus the TUN side can always report acceptance or
    /// abandonment without allocating or waiting.
    pub fn try_deliver_sink(
        &self,
        offer: SinkOffer,
        segments: Vec<TcpDataSegment>,
    ) -> Result<(), FlowPortError> {
        self.validate_sink_coverage(offer, &segments)?;
        let _admission = self.state.admission_guard();
        if self.state.is_retired() {
            return Err(FlowPortError::Closed);
        }
        let completion = self.reserve_completion()?;
        let action_slot = reserve_owned(&self.downlink_tx, FlowPortError::DownlinkLaneFull)?;
        let guard = ActionGuard::new(Arc::clone(&self.state));
        let _sender = action_slot.send(TunAction::Sink(SinkDelivery {
            offer,
            segments,
            completion: Some(completion),
            _guard: guard,
        }));
        Ok(())
    }

    pub fn try_deliver_half_close(
        &self,
        completion_capability: SinkHalfClose,
    ) -> Result<(), FlowPortError> {
        if completion_capability.session_id() != self.flow.session_id()
            || completion_capability.flow_id() != self.flow.flow_id()
        {
            return Err(FlowPortError::CrossFlowSinkCapability);
        }
        let expected = peer_direction(self.flow.direction());
        if completion_capability.direction() != expected {
            return Err(FlowPortError::SinkDirectionMismatch {
                expected,
                actual: completion_capability.direction(),
            });
        }
        let _admission = self.state.admission_guard();
        if self.state.is_retired() {
            return Err(FlowPortError::Closed);
        }
        let completion = self.reserve_completion()?;
        let action_slot = reserve_owned(&self.downlink_tx, FlowPortError::DownlinkLaneFull)?;
        let guard = ActionGuard::new(Arc::clone(&self.state));
        let _sender = action_slot.send(TunAction::HalfClose(HalfCloseDelivery {
            capability: completion_capability,
            completion: Some(completion),
            _guard: guard,
        }));
        Ok(())
    }

    /// Forwards only exact terminal effects already emitted by the reducer.
    /// This adapter deliberately cannot synthesize a new reset/finish reason.
    pub fn try_deliver_terminal_effect(
        &mut self,
        effect: &SessionEffect,
    ) -> Result<(), FlowPortError> {
        let (flow_id, action) = match effect {
            SessionEffect::FlowFinished {
                flow_id,
                reason,
                terminal,
            } => {
                if terminal.local_flow() != self.flow {
                    return Err(FlowPortError::CrossFlowTerminalEffect);
                }
                (*flow_id, TerminalAction::FlowFinished(*reason))
            }
            _ => return Err(FlowPortError::NotTerminalEffect),
        };
        self.try_deliver_terminal(flow_id, action)
    }

    fn try_deliver_terminal(
        &mut self,
        flow_id: SessionFlowId,
        action: TerminalAction,
    ) -> Result<(), FlowPortError> {
        if flow_id != self.flow.flow_id() {
            return Err(FlowPortError::CrossFlowTerminalEffect);
        }
        let state = Arc::clone(&self.state);
        let _admission = state.admission_guard();
        if state.is_retired() {
            return Err(FlowPortError::TerminalAlreadyDelivered);
        }
        let action_slot = reserve_owned(&self.terminal_tx, FlowPortError::DownlinkLaneFull)?;
        state.retired.store(true, Ordering::Release);
        self.reset_rx.close();
        self.control_rx.close();
        self.source_rx.close();
        while self.reset_rx.try_recv().is_ok() {}
        while self.control_rx.try_recv().is_ok() {}
        while self.source_rx.try_recv().is_ok() {}
        let guard = ActionGuard::new(Arc::clone(&self.state));
        let _sender = action_slot.send(TunAction::Terminal(TerminalDelivery {
            flow: self.flow,
            action,
            _guard: guard,
        }));
        Ok(())
    }

    fn reserve_completion(&self) -> Result<CompletionReservation, FlowPortError> {
        let sender = self.completion_tx.upgrade().ok_or(FlowPortError::Closed)?;
        let permit = reserve_owned(&sender, FlowPortError::ControlLaneFull)?;
        self.state
            .reserved_completion_slots
            .fetch_add(1, Ordering::AcqRel);
        Ok(CompletionReservation {
            permit: Some(permit),
            state: Arc::clone(&self.state),
        })
    }

    fn validate_sink_coverage(
        &self,
        offer: SinkOffer,
        segments: &[TcpDataSegment],
    ) -> Result<(), FlowPortError> {
        if offer.session_id() != self.flow.session_id() || offer.flow_id() != self.flow.flow_id() {
            return Err(FlowPortError::CrossFlowSinkCapability);
        }
        let expected = peer_direction(self.flow.direction());
        if offer.direction() != expected {
            return Err(FlowPortError::SinkDirectionMismatch {
                expected,
                actual: offer.direction(),
            });
        }
        if offer.is_empty() || segments.is_empty() {
            return Err(FlowPortError::InvalidSinkCoverage);
        }
        let mut next = offer.offset();
        let mut total = 0usize;
        for segment in segments {
            if segment.is_empty() || segment.offset() != next {
                return Err(FlowPortError::InvalidSinkCoverage);
            }
            next = segment.end_offset();
            total = total
                .checked_add(segment.len())
                .ok_or(FlowPortError::InvalidSinkCoverage)?;
        }
        if total != offer.len() {
            return Err(FlowPortError::InvalidSinkCoverage);
        }
        Ok(())
    }
}

const fn peer_direction(local_send: Direction) -> Direction {
    match local_send {
        Direction::ClientToTarget => Direction::TargetToClient,
        Direction::TargetToClient => Direction::ClientToTarget,
    }
}

impl fmt::Debug for ResumableTcpDriver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResumableTcpDriver")
            .field("flow_id", &self.flow.flow_id())
            .field("snapshot", &self.state.snapshot())
            .finish_non_exhaustive()
    }
}

/// One supervisor input.  DATA retains its exact byte ownership separately
/// from control so a full replay path cannot starve ACK/close completions.
pub enum DriverInput {
    Control(SessionEvent),
    Data(UplinkData),
}

impl fmt::Debug for DriverInput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(event) => formatter.debug_tuple("Control").field(event).finish(),
            Self::Data(data) => formatter.debug_tuple("Data").field(data).finish(),
        }
    }
}

/// FIFO facts produced by the local TCP source. CLOSE shares this queue with
/// DATA so it cannot freeze a final offset before every preceding byte has
/// reached the reducer. Urgent reset and sink completions use separate lanes.
enum UplinkSource {
    Data(UplinkData),
    LocalClose { flow: LocalFlow },
}

impl UplinkSource {
    fn into_driver_input(self) -> DriverInput {
        match self {
            Self::Data(data) => DriverInput::Data(data),
            Self::LocalClose { flow } => DriverInput::Control(SessionEvent::LocalClose { flow }),
        }
    }
}

/// One reserved terminal fact for the local socket boundary. It is physically
/// separate from both ordered source traffic and sink completions, so neither
/// form of bounded pressure can erase the first local reset.
struct LocalReset {
    flow: LocalFlow,
    reason: ResetReason,
}

impl LocalReset {
    fn into_session_event(self) -> SessionEvent {
        SessionEvent::LocalReset {
            flow: self.flow,
            reason: self.reason,
        }
    }
}

/// Admitted TUN-to-session DATA. It is non-cloneable and still owns its exact
/// byte and replay-segment permits.
pub struct UplinkData {
    flow: LocalFlow,
    payload: Bytes,
    ownership: UplinkOwnership,
}

impl UplinkData {
    pub const fn flow(&self) -> LocalFlow {
        self.flow
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub fn len(&self) -> usize {
        self.payload.len()
    }

    pub fn is_empty(&self) -> bool {
        self.payload.is_empty()
    }

    pub fn into_event_and_ownership(self) -> (SessionEvent, UplinkOwnership) {
        (
            SessionEvent::LocalData {
                flow: self.flow,
                payload: self.payload,
            },
            self.ownership,
        )
    }
}

impl fmt::Debug for UplinkData {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UplinkData")
            .field("flow_id", &self.flow.flow_id())
            .field("payload_len", &self.payload.len())
            .field("owned_bytes", &self.ownership.bytes)
            .finish()
    }
}

/// Exact uplink byte and replay-segment ownership separated from the reducer
/// event. Dropping it is safe cleanup; normal supervisor code binds it to the
/// reducer receipt so ownership changes remain reviewable.
pub struct UplinkOwnership {
    flow: LocalFlow,
    bytes: usize,
    expected_start: ByteOffset,
    expected_end: ByteOffset,
    payload: Option<Bytes>,
    permits: Option<UplinkPermits>,
    origin: Option<TcpFactoryOrigin>,
}

impl UplinkOwnership {
    pub const fn bytes(&self) -> usize {
        self.bytes
    }

    pub(super) fn matches_factory_origin(&self, expected: &TcpFactoryOrigin) -> bool {
        self.origin
            .as_ref()
            .is_some_and(|actual| actual.matches(expected))
    }

    /// Binds staging ownership only to the opaque authority emitted after
    /// `SessionModel::reduce(LocalData)` retained this exact flow, direction,
    /// range, and payload. A successful bind does not release any bytes.
    pub fn bind_replay(
        &mut self,
        stored: ReplayStored,
    ) -> Result<UplinkReplayOwnership, FlowPortError> {
        let Some(payload) = self.payload.as_ref() else {
            return Err(FlowPortError::UplinkOwnershipAlreadyBound);
        };
        if self.permits.is_none() {
            return Err(FlowPortError::UplinkOwnershipAlreadyBound);
        }
        let end = stored
            .start()
            .checked_advance(self.bytes)
            .map_err(|_| FlowPortError::UplinkReplayReceiptMismatch)?;
        if stored.local_flow() != self.flow
            || stored.direction() != self.flow.direction()
            || stored.start() != self.expected_start
            || stored.end() != self.expected_end
            || stored.end() != end
            || stored.payload() != payload
        {
            return Err(FlowPortError::UplinkReplayReceiptMismatch);
        }
        self.payload.take();
        Ok(UplinkReplayOwnership {
            flow: self.flow,
            direction: stored.direction(),
            start: stored.start(),
            end,
            bytes: self.bytes,
            permits: self.permits.take(),
            origin: self.origin.take(),
        })
    }
}

impl fmt::Debug for UplinkOwnership {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UplinkOwnership")
            .field("bytes", &self.bytes)
            .finish()
    }
}

/// Port-level byte and segment ownership for one reducer-assigned replay
/// extent.
///
/// This is deliberately non-cloneable. A partial ACK conservatively keeps the
/// whole permit. Release is authorized only by the opaque receipt emitted
/// after the same `SessionModel` accepted an authenticated current-leg ACK;
/// raw, stale, cross-flow, or wrong-direction frames have no release API.
pub struct UplinkReplayOwnership {
    flow: LocalFlow,
    direction: Direction,
    start: ByteOffset,
    end: ByteOffset,
    bytes: usize,
    permits: Option<UplinkPermits>,
    origin: Option<TcpFactoryOrigin>,
}

impl UplinkReplayOwnership {
    pub const fn flow_id(&self) -> SessionFlowId {
        self.flow.flow_id()
    }

    pub const fn direction(&self) -> Direction {
        self.direction
    }

    pub const fn start(&self) -> ByteOffset {
        self.start
    }

    pub const fn end(&self) -> ByteOffset {
        self.end
    }

    pub const fn bytes(&self) -> usize {
        self.bytes
    }

    pub(super) fn matches_factory_origin(&self, expected: &TcpFactoryOrigin) -> bool {
        self.origin
            .as_ref()
            .is_some_and(|actual| actual.matches(expected))
    }

    pub fn is_released(&self) -> bool {
        self.permits.is_none()
    }

    /// Releases the exact byte permit only when the reducer-accepted ACK
    /// covers this complete DATA extent. Partial coverage returns `false` and
    /// deliberately retains the whole permit.
    pub fn release_after_reducer_ack(
        &mut self,
        acknowledged: &ReplayAcknowledged,
    ) -> Result<bool, FlowPortError> {
        if acknowledged.local_flow() != self.flow || acknowledged.direction() != self.direction {
            return Err(FlowPortError::UplinkAckMismatch);
        }
        if acknowledged.next_accepted() < self.end {
            return Ok(false);
        }
        let released = self.permits.take().is_some();
        Ok(released)
    }

    /// Explicit cleanup after the reducer has emitted the exact terminal
    /// authority for this flow. A peer-reset notification alone is not enough;
    /// `FlowFinished` is the ownership-release point.
    pub fn release_after_reducer_terminal(
        &mut self,
        effect: &SessionEffect,
    ) -> Result<bool, FlowPortError> {
        let SessionEffect::FlowFinished {
            flow_id, terminal, ..
        } = effect
        else {
            return Err(FlowPortError::UplinkTerminalMismatch);
        };
        if *flow_id != self.flow.flow_id() || terminal.local_flow() != self.flow {
            return Err(FlowPortError::UplinkTerminalMismatch);
        }
        let released = self.permits.take().is_some();
        Ok(released)
    }
}

impl fmt::Debug for UplinkReplayOwnership {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UplinkReplayOwnership")
            .field("flow_id", &self.flow.flow_id())
            .field("direction", &self.direction)
            .field("start", &self.start)
            .field("end", &self.end)
            .field("bytes", &self.bytes)
            .field("released", &self.is_released())
            .finish()
    }
}

/// One bounded driver-to-TUN action. Neither variant is cloneable.
pub enum TunAction {
    Sink(SinkDelivery),
    HalfClose(HalfCloseDelivery),
    Terminal(TerminalDelivery),
}

impl fmt::Debug for TunAction {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sink(delivery) => formatter.debug_tuple("Sink").field(delivery).finish(),
            Self::HalfClose(delivery) => {
                formatter.debug_tuple("HalfClose").field(delivery).finish()
            }
            Self::Terminal(delivery) => formatter.debug_tuple("Terminal").field(delivery).finish(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TerminalAction {
    FlowFinished(FlowFinishReason),
}

/// Typed, non-cloneable terminal action derived directly from a reducer
/// effect. It contains no transport-leg identity because leg loss is not flow
/// terminal.
pub struct TerminalDelivery {
    flow: LocalFlow,
    action: TerminalAction,
    _guard: ActionGuard,
}

impl TerminalDelivery {
    pub const fn local_flow(&self) -> LocalFlow {
        self.flow
    }

    pub const fn action(&self) -> TerminalAction {
        self.action
    }
}

impl fmt::Debug for TerminalDelivery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TerminalDelivery")
            .field("flow_id", &self.flow.flow_id())
            .field("action", &self.action)
            .finish()
    }
}

/// A non-cloneable local sink write. Positive completion consumes all old
/// segment views; any suffix must arrive as a fresh `SinkOffer` from the
/// reducer after `SinkAccepted` is applied.
pub struct SinkDelivery {
    offer: SinkOffer,
    segments: Vec<TcpDataSegment>,
    completion: Option<CompletionReservation>,
    _guard: ActionGuard,
}

impl SinkDelivery {
    pub const fn offer(&self) -> SinkOffer {
        self.offer
    }

    pub fn first_segment_slice(&self) -> &[u8] {
        self.segments[0].payload()
    }

    pub fn offered_len(&self) -> usize {
        self.offer.len()
    }

    /// Exact liveness of this local-sink capability. A terminal reducer
    /// effect revokes already-forwarded deliveries before the adapter may
    /// mutate the socket again.
    pub fn is_live(&self) -> bool {
        self._guard.is_live()
    }

    /// Linearizes one synchronous socket mutation against terminal
    /// retirement. The closure runs while the port lifecycle gate is held;
    /// after it returns, terminal retirement may proceed but cannot overtake
    /// the mutation.
    pub fn with_live_first_segment<R>(
        &self,
        apply: impl FnOnce(&[u8]) -> R,
    ) -> Result<R, FlowPortError> {
        self._guard.with_live(|| apply(self.first_segment_slice()))
    }

    /// `0` retains the exact delivery and its reserved completion slot.
    /// Positive acceptance emits one exact typed completion and destroys all
    /// old suffix views. An impossible adapter count fails closed as abandon.
    pub fn complete_write(mut self, accepted: usize) -> Result<Option<Self>, FlowPortError> {
        if accepted == 0 {
            return Ok(Some(self));
        }
        let available = self.first_segment_slice().len();
        if accepted > available {
            self.finish(FlowControl::SinkAbandoned { offer: self.offer });
            return Err(FlowPortError::InvalidSinkAcceptance {
                accepted,
                available,
            });
        }
        self.finish(FlowControl::SinkAccepted {
            offer: self.offer,
            bytes: accepted,
        });
        Ok(None)
    }

    pub fn abandon(mut self) {
        self.finish(FlowControl::SinkAbandoned { offer: self.offer });
    }

    fn finish(&mut self, control: FlowControl) {
        if let Some(completion) = self.completion.take() {
            completion.send(control);
        }
    }
}

impl Drop for SinkDelivery {
    fn drop(&mut self) {
        self.finish(FlowControl::SinkAbandoned { offer: self.offer });
    }
}

impl fmt::Debug for SinkDelivery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SinkDelivery")
            .field("flow_id", &self.offer.flow_id())
            .field("offset", &self.offer.offset())
            .field("offered_len", &self.offer.len())
            .field("segment_count", &self.segments.len())
            .finish()
    }
}

/// A two-phase request to apply peer EOF to the local sink.
pub struct HalfCloseDelivery {
    capability: SinkHalfClose,
    completion: Option<CompletionReservation>,
    _guard: ActionGuard,
}

impl HalfCloseDelivery {
    pub const fn capability(&self) -> SinkHalfClose {
        self.capability
    }

    /// Exact liveness of this local half-close capability.
    pub fn is_live(&self) -> bool {
        self._guard.is_live()
    }

    /// Linearizes the synchronous local EOF mutation against terminal
    /// retirement.
    pub fn with_live<R>(&self, apply: impl FnOnce() -> R) -> Result<R, FlowPortError> {
        self._guard.with_live(apply)
    }

    pub fn succeed(mut self) {
        self.finish(FlowControl::SinkHalfClosed {
            completion: self.capability,
        });
    }

    pub fn fail(mut self) {
        self.finish(FlowControl::SinkHalfCloseFailed {
            completion: self.capability,
        });
    }

    fn finish(&mut self, control: FlowControl) {
        if let Some(completion) = self.completion.take() {
            completion.send(control);
        }
    }
}

impl Drop for HalfCloseDelivery {
    fn drop(&mut self) {
        self.finish(FlowControl::SinkHalfCloseFailed {
            completion: self.capability,
        });
    }
}

impl fmt::Debug for HalfCloseDelivery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HalfCloseDelivery")
            .field("flow_id", &self.capability.flow_id())
            .field("direction", &self.capability.direction())
            .field("final_offset", &self.capability.final_offset())
            .finish()
    }
}

fn drain_nonterminal_actions(actions: &mut mpsc::Receiver<TunAction>) {
    actions.close();
    while actions.try_recv().is_ok() {}
}

fn try_recv_tun_action(
    terminal: &mut mpsc::Receiver<TunAction>,
    actions: &mut mpsc::Receiver<TunAction>,
    state: &Arc<PortState>,
) -> Result<Option<TunAction>, FlowPortError> {
    match terminal.try_recv() {
        Ok(action) => {
            drain_nonterminal_actions(actions);
            return Ok(Some(action));
        }
        Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected) => {}
    }
    if state.is_retired() {
        drain_nonterminal_actions(actions);
        return if terminal.is_closed() {
            Err(FlowPortError::Closed)
        } else {
            Ok(None)
        };
    }
    match actions.try_recv() {
        Ok(action) => Ok(Some(action)),
        Err(mpsc::error::TryRecvError::Empty) => Ok(None),
        Err(mpsc::error::TryRecvError::Disconnected) => {
            if terminal.is_closed() {
                Err(FlowPortError::Closed)
            } else {
                Ok(None)
            }
        }
    }
}

async fn recv_tun_action(
    terminal: &mut mpsc::Receiver<TunAction>,
    actions: &mut mpsc::Receiver<TunAction>,
    state: &Arc<PortState>,
) -> Option<TunAction> {
    loop {
        match try_recv_tun_action(terminal, actions, state) {
            Ok(Some(action)) => return Some(action),
            Ok(None) => {}
            Err(FlowPortError::Closed) => return None,
            Err(_) => return None,
        }
        if state.is_retired() {
            let action = terminal.recv().await;
            drain_nonterminal_actions(actions);
            return action;
        }
        tokio::select! {
            biased;
            action = terminal.recv() => {
                if let Some(action) = action {
                    drain_nonterminal_actions(actions);
                    return Some(action);
                }
            },
            action = actions.recv() => match action {
                Some(action) if !state.is_retired() => return Some(action),
                Some(action) => drop(action),
                None if terminal.is_closed() => return None,
                None => {}
            },
        }
    }
}

enum FlowControl {
    SinkAccepted { offer: SinkOffer, bytes: usize },
    SinkAbandoned { offer: SinkOffer },
    SinkHalfClosed { completion: SinkHalfClose },
    SinkHalfCloseFailed { completion: SinkHalfClose },
}

impl FlowControl {
    fn into_session_event(self) -> SessionEvent {
        match self {
            Self::SinkAccepted { offer, bytes } => SessionEvent::SinkAccepted { offer, bytes },
            Self::SinkAbandoned { offer } => SessionEvent::SinkAbandoned { offer },
            Self::SinkHalfClosed { completion } => SessionEvent::SinkHalfClosed { completion },
            Self::SinkHalfCloseFailed { completion } => {
                SessionEvent::SinkHalfCloseFailed { completion }
            }
        }
    }
}

struct CompletionReservation {
    permit: Option<mpsc::OwnedPermit<FlowControl>>,
    state: Arc<PortState>,
}

impl CompletionReservation {
    fn send(mut self, control: FlowControl) {
        let permit = self.permit.take();
        self.state
            .reserved_completion_slots
            .fetch_sub(1, Ordering::AcqRel);
        if let Some(permit) = permit {
            let _admission = self.state.admission_guard();
            if !self.state.is_retired() {
                let _sender = permit.send(control);
            }
        }
    }
}

impl Drop for CompletionReservation {
    fn drop(&mut self) {
        if self.permit.take().is_some() {
            self.state
                .reserved_completion_slots
                .fetch_sub(1, Ordering::AcqRel);
        }
    }
}

struct ActionGuard {
    state: Arc<PortState>,
}

impl ActionGuard {
    fn new(state: Arc<PortState>) -> Self {
        state
            .pending_downlink_actions
            .fetch_add(1, Ordering::AcqRel);
        Self { state }
    }

    fn is_live(&self) -> bool {
        !self.state.is_retired()
    }

    fn with_live<R>(&self, apply: impl FnOnce() -> R) -> Result<R, FlowPortError> {
        let _admission = self.state.admission_guard();
        if self.state.is_retired() {
            return Err(FlowPortError::SinkCapabilityRevoked);
        }
        Ok(apply())
    }
}

impl Drop for ActionGuard {
    fn drop(&mut self) {
        self.state
            .pending_downlink_actions
            .fetch_sub(1, Ordering::AcqRel);
    }
}

fn try_send_uplink_parts<F>(
    flow: LocalFlow,
    source_tx: &mpsc::Sender<UplinkSource>,
    state: &Arc<PortState>,
    byte_len: usize,
    extractor: F,
) -> Result<(), FlowPortError>
where
    F: FnOnce() -> Bytes,
{
    let reservation = reserve_uplink_source(flow, source_tx, state, byte_len)?;
    let extracted = extractor();
    if extracted.len() != byte_len {
        return Err(FlowPortError::UplinkLengthMismatch {
            reserved: byte_len,
            actual: extracted.len(),
        });
    }
    // `Bytes` does not expose backing capacity. Normalize at this queue
    // ownership boundary so a tiny slice cannot retain an uncharged
    // attacker-sized allocation while waiting for the supervisor.
    reservation.commit_normalized(Bytes::copy_from_slice(&extracted))
}

fn try_send_uplink_vec_parts<F>(
    flow: LocalFlow,
    source_tx: &mpsc::Sender<UplinkSource>,
    state: &Arc<PortState>,
    byte_len: usize,
    extractor: F,
) -> Result<(), FlowPortError>
where
    F: FnOnce() -> Vec<u8>,
{
    let reservation = reserve_uplink_source(flow, source_tx, state, byte_len)?;
    let extracted = extractor();
    if extracted.len() != byte_len {
        return Err(FlowPortError::UplinkLengthMismatch {
            reserved: byte_len,
            actual: extracted.len(),
        });
    }
    let payload = if extracted.capacity() == extracted.len() {
        Bytes::from(extracted)
    } else {
        // A caller-controlled excess capacity is not byte-accounted. Compact
        // only this hostile/non-exact case; the normal smoltcp Vec transfers
        // allocation ownership without another payload copy.
        Bytes::copy_from_slice(&extracted)
    };
    reservation.commit_normalized(payload)
}

/// One pre-read reservation for a local TCP source. It owns both bounded byte
/// budgets and the exact FIFO message slot before any socket bytes are
/// consumed. The type is deliberately non-cloneable.
pub(crate) struct UplinkSourceReservation {
    flow: LocalFlow,
    reserved_bytes: usize,
    source_slot: Option<mpsc::OwnedPermit<UplinkSource>>,
    flow_byte_permit: Option<OwnedSemaphorePermit>,
    session_byte_permit: Option<OwnedSemaphorePermit>,
    flow_segment_permit: Option<OwnedSemaphorePermit>,
    session_segment_permit: Option<OwnedSemaphorePermit>,
    state: Arc<PortState>,
    active: bool,
}

impl fmt::Debug for UplinkSourceReservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UplinkSourceReservation")
            .field("reserved_bytes", &self.reserved_bytes)
            .field("active", &self.active)
            .finish_non_exhaustive()
    }
}

struct UplinkPermits {
    _flow: OwnedSemaphorePermit,
    _session: OwnedSemaphorePermit,
    _flow_segment: OwnedSemaphorePermit,
    _session_segment: OwnedSemaphorePermit,
}

impl UplinkSourceReservation {
    /// Commits the positive prefix actually consumed by the socket source.
    /// `Bytes` is normalized at this ownership boundary so a small slice cannot
    /// retain unaccounted backing storage.
    pub(crate) fn commit(self, payload: Bytes) -> Result<(), FlowPortError> {
        if payload.is_empty() || payload.len() > self.reserved_bytes {
            return Err(FlowPortError::UplinkLengthMismatch {
                reserved: self.reserved_bytes,
                actual: payload.len(),
            });
        }
        self.commit_normalized(Bytes::copy_from_slice(&payload))
    }

    /// Commits an already-observed local EOF through the exact ordered source
    /// slot reserved before the source was touched. No byte or replay-segment
    /// ownership survives because CLOSE contains no payload.
    pub(crate) fn commit_eof(self) -> Result<(), FlowPortError> {
        self.commit_close()
    }

    pub(crate) fn commit_close(mut self) -> Result<(), FlowPortError> {
        let flow = self.flow;
        let state = Arc::clone(&self.state);
        let _admission = state.admission_guard();
        if state.is_retired() {
            return Err(FlowPortError::Closed);
        }
        if state.ordered_source_closed.load(Ordering::Acquire) {
            return Err(FlowPortError::Closed);
        }
        let source_slot = self.source_slot.take().ok_or(FlowPortError::Closed)?;

        // EOF consumes no replay extent. Release all speculative capacity
        // before publishing the unique ordered close fact.
        self.flow_byte_permit.take();
        self.session_byte_permit.take();
        self.flow_segment_permit.take();
        self.session_segment_permit.take();
        let _sender = source_slot.send(UplinkSource::LocalClose { flow });
        state.ordered_source_closed.store(true, Ordering::Release);
        self.release_ordered_source();
        Ok(())
    }

    fn commit_normalized(mut self, payload: Bytes) -> Result<(), FlowPortError> {
        let actual = payload.len();
        if actual == 0 || actual > self.reserved_bytes {
            return Err(FlowPortError::UplinkLengthMismatch {
                reserved: self.reserved_bytes,
                actual,
            });
        }
        let flow = self.flow;
        let reserved_bytes = self.reserved_bytes;
        let state = Arc::clone(&self.state);
        let _admission = state.admission_guard();
        if state.is_retired() {
            return Err(FlowPortError::Closed);
        }
        let current = state.next_uplink_offset.load(Ordering::Acquire);
        let actual_u64 = u64::try_from(actual).map_err(|_| FlowPortError::UplinkOffsetExhausted)?;
        let next = current
            .checked_add(actual_u64)
            .ok_or(FlowPortError::UplinkOffsetExhausted)?;
        let expected_start = ByteOffset::new(current);
        let expected_end = expected_start
            .checked_advance(actual)
            .map_err(|_| FlowPortError::UplinkOffsetExhausted)?;
        let mut flow_byte_permit = self.flow_byte_permit.take().ok_or(FlowPortError::Closed)?;
        let mut session_byte_permit = self
            .session_byte_permit
            .take()
            .ok_or(FlowPortError::Closed)?;
        let flow_segment_permit = self
            .flow_segment_permit
            .take()
            .ok_or(FlowPortError::Closed)?;
        let session_segment_permit = self
            .session_segment_permit
            .take()
            .ok_or(FlowPortError::Closed)?;
        let exact_flow_permit =
            flow_byte_permit
                .split(actual)
                .ok_or(FlowPortError::UplinkLengthMismatch {
                    reserved: reserved_bytes,
                    actual,
                })?;
        let exact_session_permit =
            session_byte_permit
                .split(actual)
                .ok_or(FlowPortError::UplinkLengthMismatch {
                    reserved: reserved_bytes,
                    actual,
                })?;
        let source_slot = self.source_slot.take().ok_or(FlowPortError::Closed)?;
        let ownership = UplinkOwnership {
            flow,
            bytes: actual,
            expected_start,
            expected_end,
            payload: Some(payload.clone()),
            permits: Some(UplinkPermits {
                _flow: exact_flow_permit,
                _session: exact_session_permit,
                _flow_segment: flow_segment_permit,
                _session_segment: session_segment_permit,
            }),
            origin: state.origin.clone(),
        };
        state.next_uplink_offset.store(next, Ordering::Release);
        let _sender = source_slot.send(UplinkSource::Data(UplinkData {
            flow,
            payload,
            ownership,
        }));
        self.release_ordered_source();
        Ok(())
    }

    fn release_ordered_source(&mut self) {
        if self.active {
            self.state
                .source_reservation_active
                .store(false, Ordering::Release);
            self.active = false;
        }
    }
}

impl Drop for UplinkSourceReservation {
    fn drop(&mut self) {
        self.release_ordered_source();
    }
}

fn reserve_uplink_source(
    flow: LocalFlow,
    source_tx: &mpsc::Sender<UplinkSource>,
    state: &Arc<PortState>,
    byte_len: usize,
) -> Result<UplinkSourceReservation, FlowPortError> {
    if byte_len == 0 || byte_len > MAX_DATA_PAYLOAD_BYTES {
        return Err(FlowPortError::InvalidUplinkLength { len: byte_len });
    }
    if byte_len > state.uplink_byte_capacity {
        return Err(FlowPortError::UplinkByteBudgetExhausted {
            requested: byte_len,
            available: state.uplink_budget.available_permits(),
        });
    }

    let _admission = state.admission_guard();
    if state.is_retired() {
        return Err(FlowPortError::Closed);
    }
    if state.ordered_source_closed.load(Ordering::Acquire) {
        return Err(FlowPortError::Closed);
    }
    if state.source_reservation_active.load(Ordering::Acquire) {
        return Err(FlowPortError::UplinkSourceReservationActive);
    }
    let requested_u64 =
        u64::try_from(byte_len).map_err(|_| FlowPortError::UplinkOffsetExhausted)?;
    state
        .next_uplink_offset
        .load(Ordering::Acquire)
        .checked_add(requested_u64)
        .ok_or(FlowPortError::UplinkOffsetExhausted)?;
    let source_slot = reserve_owned(source_tx, FlowPortError::UplinkMessageLaneFull)?;
    let permits = u32::try_from(byte_len)
        .map_err(|_| FlowPortError::InvalidUplinkLength { len: byte_len })?;
    let flow_byte_permit = Arc::clone(&state.uplink_budget)
        .try_acquire_many_owned(permits)
        .map_err(|_| FlowPortError::UplinkByteBudgetExhausted {
            requested: byte_len,
            available: state.uplink_budget.available_permits(),
        })?;
    let session_byte_permit = Arc::clone(&state.session_uplink_ledger.budget)
        .try_acquire_many_owned(permits)
        .map_err(|_| FlowPortError::UplinkGlobalByteBudgetExhausted {
            requested: byte_len,
            available: state.session_uplink_ledger.available_bytes(),
        })?;
    let flow_segment_permit = Arc::clone(&state.uplink_segment_budget)
        .try_acquire_owned()
        .map_err(|_| FlowPortError::UplinkSegmentBudgetExhausted {
            available: state.uplink_segment_budget.available_permits(),
        })?;
    let session_segment_permit = Arc::clone(&state.session_uplink_segment_ledger.budget)
        .try_acquire_owned()
        .map_err(|_| FlowPortError::UplinkGlobalSegmentBudgetExhausted {
            available: state.session_uplink_segment_ledger.available_segments(),
        })?;
    state
        .source_reservation_active
        .store(true, Ordering::Release);
    Ok(UplinkSourceReservation {
        flow,
        reserved_bytes: byte_len,
        source_slot: Some(source_slot),
        flow_byte_permit: Some(flow_byte_permit),
        session_byte_permit: Some(session_byte_permit),
        flow_segment_permit: Some(flow_segment_permit),
        session_segment_permit: Some(session_segment_permit),
        state: Arc::clone(state),
        active: true,
    })
}

fn send_ordered_close(
    sender: &mpsc::Sender<UplinkSource>,
    state: &Arc<PortState>,
    flow: LocalFlow,
) -> Result<(), FlowPortError> {
    let _admission = state.admission_guard();
    if state.is_retired() {
        return Err(FlowPortError::Closed);
    }
    if state.ordered_source_closed.load(Ordering::Acquire) {
        return Err(FlowPortError::Closed);
    }
    if state.source_reservation_active.load(Ordering::Acquire) {
        return Err(FlowPortError::UplinkSourceReservationActive);
    }
    sender
        .try_send(UplinkSource::LocalClose { flow })
        .map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => FlowPortError::UplinkMessageLaneFull,
            mpsc::error::TrySendError::Closed(_) => FlowPortError::Closed,
        })?;
    state.ordered_source_closed.store(true, Ordering::Release);
    Ok(())
}

fn send_reset(
    sender: &mpsc::Sender<LocalReset>,
    state: &Arc<PortState>,
    flow: LocalFlow,
    reason: ResetReason,
) -> Result<(), FlowPortError> {
    let _admission = state.admission_guard();
    if state.is_retired() {
        return Err(FlowPortError::Closed);
    }
    sender
        .try_send(LocalReset { flow, reason })
        .map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => FlowPortError::TerminalLaneFull,
            mpsc::error::TrySendError::Closed(_) => FlowPortError::Closed,
        })
}

fn reserve_owned<T>(
    sender: &mpsc::Sender<T>,
    full_error: FlowPortError,
) -> Result<mpsc::OwnedPermit<T>, FlowPortError> {
    sender
        .clone()
        .try_reserve_owned()
        .map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => full_error,
            mpsc::error::TrySendError::Closed(_) => FlowPortError::Closed,
        })
}

fn validate_channel_capacity(lane: &'static str, value: usize) -> Result<(), FlowPortConfigError> {
    if value > Semaphore::MAX_PERMITS {
        return Err(FlowPortConfigError::ChannelCapacityTooLarge {
            lane,
            value,
            max: Semaphore::MAX_PERMITS,
        });
    }
    Ok(())
}

fn validate_segment_capacity(
    scope: &'static str,
    value: usize,
    zero_error: FlowPortConfigError,
) -> Result<(), FlowPortConfigError> {
    if value == 0 {
        return Err(zero_error);
    }
    if value > Semaphore::MAX_PERMITS {
        return Err(FlowPortConfigError::SegmentCapacityTooLarge {
            scope,
            value,
            max: Semaphore::MAX_PERMITS,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::resumable::{
        AttachAlpn, AttachAuthority, AttachCredentials, AttachNonce, AttachPolicy, AttachRequest,
        AttachTransportBinding, ByteOffset, DevicePrincipal, DeviceSecret, Direction, FeatureOffer,
        Frame, LegGeneration, OpenResultCode, OwnerIdentity, ReceiveBudgetLimits, Record,
        ReplayBudgetLimits, ResumeSecret, SESSION_PROTOCOL_VERSION, SessionConfig, SessionEffect,
        SessionId, SessionModel, SessionRole, TcpWindowLimits, TlsExporterBinding, VersionRange,
    };
    use crate::shared::TargetAddr;

    struct Fixture {
        model: SessionModel,
        leg: crate::resumable::CommittedLeg,
        flow: LocalFlow,
    }

    fn test_leg_for_session(session_byte: u8) -> crate::resumable::CommittedLeg {
        let session_id = SessionId::new([session_byte; 16]).unwrap();
        let owner = OwnerIdentity::new([0x31; 32]).unwrap();
        let principal = DevicePrincipal::new([0x41; 16]).unwrap();
        let alpn = AttachAlpn::new(b"mini-vpn-owned/1").unwrap();
        let binding = AttachTransportBinding::new(
            owner,
            alpn,
            TlsExporterBinding::new([0x51; 32]).unwrap(),
            principal,
        );
        let credentials = AttachCredentials::new(
            DeviceSecret::new([0x61; 32]).unwrap(),
            ResumeSecret::new([0x71; 32]).unwrap(),
        )
        .unwrap();
        let request = AttachRequest::new(
            session_id,
            LegGeneration::new(2).unwrap(),
            AttachNonce::new([0x81; 16]).unwrap(),
            VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION).unwrap(),
            FeatureOffer::new(0b11, 0b01).unwrap(),
        );
        let proof = credentials.prove(&request, &binding).unwrap();
        let authority = AttachAuthority::new(
            session_id,
            LegGeneration::new(1).unwrap(),
            credentials,
            AttachPolicy::new(owner, alpn, principal, SESSION_PROTOCOL_VERSION, 0b11).unwrap(),
        );
        authority
            .verify_and_commit(&request, &binding, &proof)
            .unwrap()
    }

    fn test_leg() -> crate::resumable::CommittedLeg {
        test_leg_for_session(0x21)
    }

    fn test_session_config() -> SessionConfig {
        SessionConfig::new(
            4,
            8,
            TcpWindowLimits::new(64, 8).unwrap(),
            TcpWindowLimits::new(64, 8).unwrap(),
            ReplayBudgetLimits::new(128, 16).unwrap(),
            ReplayBudgetLimits::new(128, 16).unwrap(),
            ReceiveBudgetLimits::new(128, 16).unwrap(),
            64,
        )
        .unwrap()
    }

    fn fixture_for_session(session_byte: u8) -> Fixture {
        let leg = test_leg_for_session(session_byte);
        let config = test_session_config();
        let mut model = SessionModel::new(SessionRole::Client, config, leg);
        let effects = model
            .reduce(SessionEvent::LocalOpen {
                leg,
                target: TargetAddr::IpPort(SocketAddr::V4(SocketAddrV4::new(
                    Ipv4Addr::LOCALHOST,
                    443,
                ))),
            })
            .unwrap();
        let flow = effects
            .iter()
            .find_map(|effect| match effect {
                SessionEffect::LocalFlowOpened { flow } => Some(*flow),
                _ => None,
            })
            .unwrap();
        Fixture { model, leg, flow }
    }

    fn fixture() -> Fixture {
        fixture_for_session(0x21)
    }

    fn owner_fixture() -> Fixture {
        let leg = test_leg();
        let mut model = SessionModel::new(SessionRole::Owner, test_session_config(), leg);
        let flow_id = SessionFlowId::new(1).unwrap();
        let effects = model
            .reduce(SessionEvent::PeerFrame {
                leg,
                frame: Frame::try_new(
                    leg.generation(),
                    Record::Open {
                        flow_id,
                        target: TargetAddr::IpPort(SocketAddr::V4(SocketAddrV4::new(
                            Ipv4Addr::LOCALHOST,
                            443,
                        ))),
                    },
                )
                .unwrap(),
            })
            .unwrap();
        let (request, flow) = effects
            .iter()
            .find_map(|effect| match effect {
                SessionEffect::PeerOpenRequested { request, flow, .. } => Some((*request, *flow)),
                _ => None,
            })
            .unwrap();
        model
            .reduce(SessionEvent::PeerOpenResolved {
                request,
                result: OpenResultCode::Opened,
            })
            .unwrap();
        Fixture { model, leg, flow }
    }

    fn port_config(
        bytes: usize,
        data_messages: usize,
        controls: usize,
        downlink: usize,
    ) -> FlowPortConfig {
        FlowPortConfig::new(bytes, data_messages, controls, downlink).unwrap()
    }

    fn isolated_pair(
        flow: LocalFlow,
        config: FlowPortConfig,
    ) -> (ResumableTcpFlow, ResumableTcpDriver) {
        ResumableTcpPortFactory::new_unbound_for_test(config, config.uplink_byte_capacity(), 8, 16)
            .unwrap()
            .open_flow(flow)
            .unwrap()
    }

    fn open_another_local_flow(fixture: &mut Fixture, port: u16) -> LocalFlow {
        fixture
            .model
            .reduce(SessionEvent::LocalOpen {
                leg: fixture.leg,
                target: TargetAddr::IpPort(SocketAddr::V4(SocketAddrV4::new(
                    Ipv4Addr::LOCALHOST,
                    port,
                ))),
            })
            .unwrap()
            .into_iter()
            .find_map(|effect| match effect {
                SessionEffect::LocalFlowOpened { flow } => Some(flow),
                _ => None,
            })
            .unwrap()
    }

    fn take_replay_stored(effects: &mut Vec<SessionEffect>) -> ReplayStored {
        let index = effects
            .iter()
            .position(|effect| matches!(effect, SessionEffect::ReplayStored { .. }))
            .expect("LocalData must return replay storage authority");
        match effects.remove(index) {
            SessionEffect::ReplayStored { receipt } => receipt,
            _ => unreachable!("located exact ReplayStored variant"),
        }
    }

    fn take_replay_acknowledged(effects: &mut Vec<SessionEffect>) -> ReplayAcknowledged {
        let index = effects
            .iter()
            .position(|effect| matches!(effect, SessionEffect::ReplayAcknowledged { .. }))
            .expect("accepted peer ACK must return replay release authority");
        match effects.remove(index) {
            SessionEffect::ReplayAcknowledged { receipt } => receipt,
            _ => unreachable!("located exact ReplayAcknowledged variant"),
        }
    }

    fn open_peer(fixture: &mut Fixture) {
        let frame = Frame::try_new(
            fixture.leg.generation(),
            Record::OpenResult {
                flow_id: fixture.flow.flow_id(),
                result: OpenResultCode::Opened,
            },
        )
        .unwrap();
        fixture
            .model
            .reduce(SessionEvent::PeerFrame {
                leg: fixture.leg,
                frame,
            })
            .unwrap();
    }

    fn sink_offer(fixture: &mut Fixture, payload: &[u8]) -> (SinkOffer, Vec<TcpDataSegment>) {
        open_peer(fixture);
        let frame = Frame::try_new(
            fixture.leg.generation(),
            Record::Data {
                flow_id: fixture.flow.flow_id(),
                direction: Direction::TargetToClient,
                offset: ByteOffset::new(0),
                payload: Bytes::copy_from_slice(payload),
            },
        )
        .unwrap();
        let effects = fixture
            .model
            .reduce(SessionEvent::PeerFrame {
                leg: fixture.leg,
                frame,
            })
            .unwrap();
        effects
            .into_iter()
            .find_map(|effect| match effect {
                SessionEffect::OfferToSink { offer, segments } => Some((offer, segments)),
                _ => None,
            })
            .unwrap()
    }

    fn half_close(fixture: &mut Fixture) -> SinkHalfClose {
        open_peer(fixture);
        let frame = Frame::try_new(
            fixture.leg.generation(),
            Record::Close {
                flow_id: fixture.flow.flow_id(),
                direction: Direction::TargetToClient,
                final_offset: ByteOffset::new(0),
            },
        )
        .unwrap();
        fixture
            .model
            .reduce(SessionEvent::PeerFrame {
                leg: fixture.leg,
                frame,
            })
            .unwrap()
            .into_iter()
            .find_map(|effect| match effect {
                SessionEffect::HalfCloseSink { completion } => Some(completion),
                _ => None,
            })
            .unwrap()
    }

    #[test]
    fn replay_segment_factory_exposes_exact_reducer_aligned_capacities() {
        let config = port_config(8, 1, 1, 1);
        let factory = ResumableTcpPortFactory::new_unbound_for_test(config, 32, 3, 11).unwrap();
        assert_eq!(factory.per_flow_uplink_segment_capacity(), 3);
        assert_eq!(factory.session_uplink_segment_capacity(), 11);
        assert_eq!(factory.session_uplink_owned_segments(), 0);
        assert_eq!(
            ResumableTcpPortFactory::new_unbound_for_test(config, 32, 0, 11).unwrap_err(),
            FlowPortConfigError::ZeroUplinkSegmentCapacity
        );
        assert_eq!(
            ResumableTcpPortFactory::new_unbound_for_test(config, 32, 3, 0).unwrap_err(),
            FlowPortConfigError::ZeroSessionUplinkSegmentCapacity
        );
    }

    #[test]
    fn cloned_factory_rejects_duplicate_local_flow_authority_before_source_admission() {
        let fixture = fixture();
        let factory =
            ResumableTcpPortFactory::new_unbound_for_test(port_config(8, 1, 1, 1), 16, 8, 16)
                .unwrap();
        let duplicate_caller = factory.clone();

        let (flow, mut driver) = factory.open_flow(fixture.flow).unwrap();
        assert!(matches!(
            duplicate_caller.open_flow(fixture.flow),
            Err(FlowPortError::LocalFlowAuthorityNotMonotonic {
                highest: 1,
                attempted: 1,
            })
        ));
        assert_eq!(factory.session_uplink_owned_bytes(), 0);
        assert_eq!(factory.session_uplink_owned_segments(), 0);

        flow.try_send_uplink_with(1, || Bytes::from_static(b"x"))
            .unwrap();
        assert!(matches!(
            driver.try_recv_next().unwrap(),
            Some(DriverInput::Data(data)) if data.payload() == b"x"
        ));
    }

    #[test]
    fn factory_binding_rejects_cross_session_and_cross_direction_capabilities() {
        let session = SessionId::new([0x91; 16]).unwrap();
        let factory =
            ResumableTcpPortFactory::new_unbound_for_test(port_config(8, 1, 1, 1), 16, 8, 16)
                .unwrap();
        let first = LocalFlow::for_adapter_test(
            session,
            SessionFlowId::new(1).unwrap(),
            Direction::ClientToTarget,
            1,
        );
        factory.open_flow(first).unwrap();

        let cross_session = LocalFlow::for_adapter_test(
            SessionId::new([0x92; 16]).unwrap(),
            SessionFlowId::new(2).unwrap(),
            Direction::ClientToTarget,
            2,
        );
        assert!(matches!(
            factory.open_flow(cross_session),
            Err(FlowPortError::CrossSessionFlowFactory)
        ));

        let cross_direction = LocalFlow::for_adapter_test(
            session,
            SessionFlowId::new(2).unwrap(),
            Direction::TargetToClient,
            2,
        );
        assert!(matches!(
            factory.open_flow(cross_direction),
            Err(FlowPortError::FlowFactoryDirectionMismatch {
                expected: Direction::ClientToTarget,
                actual: Direction::TargetToClient,
            })
        ));
    }

    #[test]
    fn factory_accepts_authority_gaps_but_rejects_a_later_stale_capability() {
        let session = SessionId::new([0x93; 16]).unwrap();
        let factory =
            ResumableTcpPortFactory::new_unbound_for_test(port_config(8, 1, 1, 1), 16, 8, 16)
                .unwrap();
        let flow = |flow_id, authority_id| {
            LocalFlow::for_adapter_test(
                session,
                SessionFlowId::new(flow_id).unwrap(),
                Direction::ClientToTarget,
                authority_id,
            )
        };

        factory.open_flow(flow(1, 1)).unwrap();
        factory.open_flow(flow(3, 3)).unwrap();
        assert!(matches!(
            factory.open_flow(flow(2, 2)),
            Err(FlowPortError::LocalFlowAuthorityNotMonotonic {
                highest: 3,
                attempted: 2,
            })
        ));
    }

    #[test]
    fn session_uplink_ledger_backpressures_across_flows_before_extraction() {
        let mut fixture = fixture();
        let second_flow = open_another_local_flow(&mut fixture, 8443);
        let factory =
            ResumableTcpPortFactory::new_unbound_for_test(port_config(4, 1, 1, 1), 4, 8, 16)
                .unwrap();
        let (first, mut first_driver) = factory.open_flow(fixture.flow).unwrap();
        let (second, _second_driver) = factory.open_flow(second_flow).unwrap();
        let calls = AtomicUsize::new(0);

        first
            .try_send_uplink_with(4, || Bytes::from_static(b"full"))
            .unwrap();
        assert_eq!(factory.session_uplink_owned_bytes(), 4);
        assert_eq!(
            second
                .try_send_uplink_with(1, || {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Bytes::from_static(b"x")
                })
                .unwrap_err(),
            FlowPortError::UplinkGlobalByteBudgetExhausted {
                requested: 1,
                available: 0,
            }
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);

        let Some(DriverInput::Data(first_data)) = first_driver.try_recv_next().unwrap() else {
            panic!("expected first flow DATA ownership")
        };
        drop(first_data);
        assert_eq!(factory.session_uplink_owned_bytes(), 0);
        second
            .try_send_uplink_with(1, || {
                calls.fetch_add(1, Ordering::SeqCst);
                Bytes::from_static(b"x")
            })
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(factory.session_uplink_owned_bytes(), 1);
    }

    #[test]
    fn replay_storage_receipt_rejects_equal_length_wrong_payload() {
        let mut fixture = fixture();
        let (flow, mut driver) = isolated_pair(fixture.flow, port_config(8, 2, 1, 1));

        // First advance both the port's source extent and the reducer's replay
        // extent in lockstep. The retained replay ownership intentionally
        // stays live while the second equal-sized extent is checked.
        flow.try_send_uplink_with(4, || Bytes::from_static(b"good"))
            .unwrap();
        let Some(DriverInput::Data(data)) = driver.try_recv_next().unwrap() else {
            panic!("expected admitted DATA")
        };
        let (first_event, mut first_ownership) = data.into_event_and_ownership();
        let mut first_effects = fixture.model.reduce(first_event).unwrap();
        let first_receipt = take_replay_stored(&mut first_effects);
        let first_replay = first_ownership.bind_replay(first_receipt).unwrap();
        assert_eq!(first_replay.start(), ByteOffset::new(0));
        assert_eq!(first_replay.end(), ByteOffset::new(4));

        flow.try_send_uplink_with(4, || Bytes::from_static(b"good"))
            .unwrap();
        let Some(DriverInput::Data(data)) = driver.try_recv_next().unwrap() else {
            panic!("expected second admitted DATA")
        };
        let (_real_event, mut ownership) = data.into_event_and_ownership();

        // Mint a genuine reducer receipt for the same flow, direction, range,
        // and length but different bytes. Metadata equality alone must never
        // transfer the second port permit to this replay extent.
        let wrong_receipt = fixture
            .model
            .reduce(SessionEvent::LocalData {
                flow: fixture.flow,
                payload: Bytes::from_static(b"evil"),
            })
            .unwrap()
            .into_iter()
            .find_map(|effect| match effect {
                SessionEffect::ReplayStored { receipt } => Some(receipt),
                _ => None,
            })
            .unwrap();
        assert_eq!(
            ownership.bind_replay(wrong_receipt).unwrap_err(),
            FlowPortError::UplinkReplayReceiptMismatch
        );
        assert_eq!(flow.probe().snapshot().uplink_owned_bytes(), 8);
        drop(ownership);
        assert_eq!(flow.probe().snapshot().uplink_owned_bytes(), 4);
        assert_eq!(flow.probe().snapshot().uplink_owned_segments(), 1);
        drop(first_replay);
        assert_eq!(flow.probe().snapshot().uplink_owned_bytes(), 0);
    }

    #[test]
    fn replay_storage_receipt_rejects_wrong_direction() {
        let client = fixture();
        let mut owner = owner_fixture();
        assert_eq!(client.flow.session_id(), owner.flow.session_id());
        assert_eq!(client.flow.flow_id(), owner.flow.flow_id());
        assert_ne!(client.flow.direction(), owner.flow.direction());

        let (flow, mut driver) = isolated_pair(client.flow, port_config(4, 1, 1, 1));
        flow.try_send_uplink_with(4, || Bytes::from_static(b"same"))
            .unwrap();
        let Some(DriverInput::Data(data)) = driver.try_recv_next().unwrap() else {
            panic!("expected admitted DATA")
        };
        let (_event, mut ownership) = data.into_event_and_ownership();
        let wrong_direction = owner
            .model
            .reduce(SessionEvent::LocalData {
                flow: owner.flow,
                payload: Bytes::from_static(b"same"),
            })
            .unwrap()
            .into_iter()
            .find_map(|effect| match effect {
                SessionEffect::ReplayStored { receipt } => Some(receipt),
                _ => None,
            })
            .unwrap();
        assert_eq!(
            ownership.bind_replay(wrong_direction).unwrap_err(),
            FlowPortError::UplinkReplayReceiptMismatch
        );
        assert_eq!(flow.probe().snapshot().uplink_owned_bytes(), 4);
        drop(ownership);
        assert_eq!(flow.probe().snapshot().uplink_owned_bytes(), 0);
    }

    #[test]
    fn identical_payload_receipt_cannot_bind_a_different_source_extent() {
        let mut fixture = fixture();
        let (flow, mut driver) = isolated_pair(fixture.flow, port_config(8, 2, 1, 1));
        flow.try_send_uplink_with(4, || Bytes::from_static(b"same"))
            .unwrap();
        flow.try_send_uplink_with(4, || Bytes::from_static(b"same"))
            .unwrap();
        let Some(DriverInput::Data(first)) = driver.try_recv_next().unwrap() else {
            panic!("expected first identical DATA")
        };
        let Some(DriverInput::Data(second)) = driver.try_recv_next().unwrap() else {
            panic!("expected second identical DATA")
        };
        let (first_event, first_ownership) = first.into_event_and_ownership();
        let (second_event, mut second_ownership) = second.into_event_and_ownership();
        let mut first_effects = fixture.model.reduce(first_event).unwrap();
        let first_receipt = take_replay_stored(&mut first_effects);
        let mut second_effects = fixture.model.reduce(second_event).unwrap();
        let second_receipt = take_replay_stored(&mut second_effects);

        assert_eq!(
            second_ownership.bind_replay(first_receipt).unwrap_err(),
            FlowPortError::UplinkReplayReceiptMismatch
        );
        let second_replay = second_ownership.bind_replay(second_receipt).unwrap();
        assert_eq!(second_replay.start(), ByteOffset::new(4));
        assert_eq!(second_replay.end(), ByteOffset::new(8));
        drop(first_ownership);
        assert_eq!(flow.probe().snapshot().uplink_owned_bytes(), 4);
        drop(second_replay);
        assert_eq!(flow.probe().snapshot().uplink_owned_bytes(), 0);
    }

    #[test]
    fn uplink_reserves_message_and_exact_bytes_before_extraction() {
        let mut fixture = fixture();
        let (flow, mut driver) = isolated_pair(fixture.flow, port_config(4, 1, 2, 1));
        let calls = AtomicUsize::new(0);

        flow.try_send_uplink_with(4, || {
            calls.fetch_add(1, Ordering::SeqCst);
            Bytes::from_static(b"data")
        })
        .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(flow.probe().snapshot().uplink_owned_bytes(), 4);

        let error = flow
            .try_send_uplink_with(1, || {
                calls.fetch_add(1, Ordering::SeqCst);
                Bytes::from_static(b"x")
            })
            .unwrap_err();
        assert_eq!(error, FlowPortError::UplinkMessageLaneFull);
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let DriverInput::Data(data) = driver.try_recv_next().unwrap().unwrap() else {
            panic!("expected DATA")
        };
        let (event, mut ownership) = data.into_event_and_ownership();
        let error = flow
            .try_send_uplink_with(1, || {
                calls.fetch_add(1, Ordering::SeqCst);
                Bytes::from_static(b"x")
            })
            .unwrap_err();
        assert_eq!(
            error,
            FlowPortError::UplinkByteBudgetExhausted {
                requested: 1,
                available: 0,
            }
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let mut stored_effects = fixture.model.reduce(event).unwrap();
        let stored = take_replay_stored(&mut stored_effects);
        let mut replay = ownership.bind_replay(stored).unwrap();
        assert_eq!(flow.probe().snapshot().uplink_owned_bytes(), 4);

        let partial_ack = Frame::try_new(
            fixture.leg.generation(),
            Record::Ack {
                flow_id: fixture.flow.flow_id(),
                direction: Direction::ClientToTarget,
                next_accepted: ByteOffset::new(2),
                final_accepted: false,
            },
        )
        .unwrap();
        let mut partial_effects = fixture
            .model
            .reduce(SessionEvent::PeerFrame {
                leg: fixture.leg,
                frame: partial_ack,
            })
            .unwrap();
        let partial_receipt = take_replay_acknowledged(&mut partial_effects);
        assert!(!replay.release_after_reducer_ack(&partial_receipt).unwrap());
        assert_eq!(flow.probe().snapshot().uplink_owned_bytes(), 4);

        let full_ack = Frame::try_new(
            fixture.leg.generation(),
            Record::Ack {
                flow_id: fixture.flow.flow_id(),
                direction: Direction::ClientToTarget,
                next_accepted: ByteOffset::new(4),
                final_accepted: false,
            },
        )
        .unwrap();
        let mut full_effects = fixture
            .model
            .reduce(SessionEvent::PeerFrame {
                leg: fixture.leg,
                frame: full_ack,
            })
            .unwrap();
        let full_receipt = take_replay_acknowledged(&mut full_effects);
        assert!(replay.release_after_reducer_ack(&full_receipt).unwrap());
        assert!(!replay.release_after_reducer_ack(&full_receipt).unwrap());
        assert_eq!(flow.probe().snapshot().uplink_owned_bytes(), 0);
    }

    #[test]
    fn replay_segment_permits_survive_source_dequeue_until_exact_ack() {
        let mut fixture = fixture();
        let factory =
            ResumableTcpPortFactory::new_unbound_for_test(port_config(8, 1, 2, 1), 8, 2, 2)
                .unwrap();
        let (flow, mut driver) = factory.open_flow(fixture.flow).unwrap();
        let mut retained = Vec::new();

        for payload in [b"a".as_slice(), b"b".as_slice()] {
            flow.try_send_uplink_with(1, || Bytes::copy_from_slice(payload))
                .unwrap();
            let Some(DriverInput::Data(data)) = driver.try_recv_next().unwrap() else {
                panic!("expected admitted DATA")
            };
            let (event, mut ownership) = data.into_event_and_ownership();
            let mut effects = fixture.model.reduce(event).unwrap();
            retained.push(
                ownership
                    .bind_replay(take_replay_stored(&mut effects))
                    .unwrap(),
            );
        }
        assert_eq!(flow.probe().snapshot().uplink_owned_segments(), 2);
        assert_eq!(factory.session_uplink_owned_segments(), 2);

        let extractions = AtomicUsize::new(0);
        assert_eq!(
            flow.try_send_uplink_with(1, || {
                extractions.fetch_add(1, Ordering::SeqCst);
                Bytes::from_static(b"c")
            })
            .unwrap_err(),
            FlowPortError::UplinkSegmentBudgetExhausted { available: 0 }
        );
        assert_eq!(extractions.load(Ordering::SeqCst), 0);

        let acknowledged = Frame::try_new(
            fixture.leg.generation(),
            Record::Ack {
                flow_id: fixture.flow.flow_id(),
                direction: Direction::ClientToTarget,
                next_accepted: ByteOffset::new(1),
                final_accepted: false,
            },
        )
        .unwrap();
        let mut effects = fixture
            .model
            .reduce(SessionEvent::PeerFrame {
                leg: fixture.leg,
                frame: acknowledged,
            })
            .unwrap();
        assert!(
            retained[0]
                .release_after_reducer_ack(&take_replay_acknowledged(&mut effects))
                .unwrap()
        );
        assert_eq!(flow.probe().snapshot().uplink_owned_segments(), 1);

        flow.try_send_uplink_with(1, || {
            extractions.fetch_add(1, Ordering::SeqCst);
            Bytes::from_static(b"c")
        })
        .unwrap();
        assert_eq!(extractions.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn replay_segment_limits_are_per_flow_and_session_global_before_extraction() {
        let mut fixture = fixture();
        let second_flow = open_another_local_flow(&mut fixture, 8443);
        let third_flow = open_another_local_flow(&mut fixture, 9443);
        let factory =
            ResumableTcpPortFactory::new_unbound_for_test(port_config(8, 1, 2, 1), 8, 1, 2)
                .unwrap();
        let (first, mut first_driver) = factory.open_flow(fixture.flow).unwrap();
        let (second, mut second_driver) = factory.open_flow(second_flow).unwrap();
        let (third, mut third_driver) = factory.open_flow(third_flow).unwrap();
        let extractions = AtomicUsize::new(0);

        first
            .try_send_uplink_with(1, || {
                extractions.fetch_add(1, Ordering::SeqCst);
                Bytes::from_static(b"a")
            })
            .unwrap();
        let Some(DriverInput::Data(first_data)) = first_driver.try_recv_next().unwrap() else {
            panic!("expected first DATA")
        };
        assert_eq!(
            first
                .try_send_uplink_with(1, || {
                    extractions.fetch_add(1, Ordering::SeqCst);
                    Bytes::from_static(b"x")
                })
                .unwrap_err(),
            FlowPortError::UplinkSegmentBudgetExhausted { available: 0 }
        );

        second
            .try_send_uplink_with(1, || {
                extractions.fetch_add(1, Ordering::SeqCst);
                Bytes::from_static(b"b")
            })
            .unwrap();
        let Some(DriverInput::Data(second_data)) = second_driver.try_recv_next().unwrap() else {
            panic!("expected second DATA")
        };
        assert_eq!(factory.session_uplink_owned_segments(), 2);
        assert_eq!(
            third
                .try_send_uplink_with(1, || {
                    extractions.fetch_add(1, Ordering::SeqCst);
                    Bytes::from_static(b"c")
                })
                .unwrap_err(),
            FlowPortError::UplinkGlobalSegmentBudgetExhausted { available: 0 }
        );
        assert_eq!(extractions.load(Ordering::SeqCst), 2);

        drop(first_data);
        assert_eq!(factory.session_uplink_owned_segments(), 1);
        third
            .try_send_uplink_with(1, || {
                extractions.fetch_add(1, Ordering::SeqCst);
                Bytes::from_static(b"c")
            })
            .unwrap();
        assert!(matches!(
            third_driver.try_recv_next().unwrap(),
            Some(DriverInput::Data(_))
        ));
        assert_eq!(extractions.load(Ordering::SeqCst), 3);
        drop(second_data);
    }

    #[test]
    fn source_reservation_commits_eof_through_its_pre_reserved_fifo_slot() {
        let fixture = fixture();
        let factory =
            ResumableTcpPortFactory::new_unbound_for_test(port_config(8, 1, 1, 1), 8, 1, 1)
                .unwrap();
        let (flow, mut driver) = factory.open_flow(fixture.flow).unwrap();
        let eof_reads = AtomicUsize::new(0);

        let reservation = flow.try_reserve_uplink_source(8).unwrap();
        eof_reads.fetch_add(1, Ordering::SeqCst);
        assert_eq!(
            flow.try_send_close().unwrap_err(),
            FlowPortError::UplinkSourceReservationActive
        );
        reservation.commit_eof().unwrap();

        assert_eq!(eof_reads.load(Ordering::SeqCst), 1);
        assert_eq!(flow.probe().snapshot().uplink_owned_bytes(), 0);
        assert_eq!(flow.probe().snapshot().uplink_owned_segments(), 0);
        assert_eq!(factory.session_uplink_owned_bytes(), 0);
        assert_eq!(factory.session_uplink_owned_segments(), 0);
        assert_eq!(flow.try_send_close().unwrap_err(), FlowPortError::Closed);
        let post_eof_extractions = AtomicUsize::new(0);
        assert_eq!(
            flow.try_send_uplink_with(1, || {
                post_eof_extractions.fetch_add(1, Ordering::SeqCst);
                Bytes::from_static(b"late")
            })
            .unwrap_err(),
            FlowPortError::Closed
        );
        assert_eq!(post_eof_extractions.load(Ordering::SeqCst), 0);
        assert!(matches!(
            driver.try_recv_next().unwrap(),
            Some(DriverInput::Control(SessionEvent::LocalClose { flow: observed }))
                if observed == fixture.flow
        ));
        assert!(driver.try_recv_next().unwrap().is_none());
    }

    #[test]
    fn committed_eof_cannot_overtake_preceding_data_on_a_one_slot_lane() {
        let fixture = fixture();
        let factory =
            ResumableTcpPortFactory::new_unbound_for_test(port_config(8, 1, 1, 1), 8, 2, 2)
                .unwrap();
        let (flow, mut driver) = factory.open_flow(fixture.flow).unwrap();
        let eof_reads = AtomicUsize::new(0);

        flow.try_send_uplink_with(1, || Bytes::from_static(b"x"))
            .unwrap();
        assert!(matches!(
            flow.try_reserve_uplink_source(1),
            Err(FlowPortError::UplinkMessageLaneFull)
        ));
        assert_eq!(eof_reads.load(Ordering::SeqCst), 0);

        let Some(DriverInput::Data(data)) = driver.try_recv_next().unwrap() else {
            panic!("preceding DATA must be observed before EOF can reserve its slot")
        };
        let reservation = flow.try_reserve_uplink_source(1).unwrap();
        eof_reads.fetch_add(1, Ordering::SeqCst);
        reservation.commit_eof().unwrap();

        assert_eq!(data.payload(), b"x");
        assert!(matches!(
            driver.try_recv_next().unwrap(),
            Some(DriverInput::Control(SessionEvent::LocalClose { flow: observed }))
                if observed == fixture.flow
        ));
        assert_eq!(eof_reads.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn uplink_source_reservation_shrinks_max_budget_to_committed_bytes() {
        let mut fixture = fixture();
        let factory =
            ResumableTcpPortFactory::new_unbound_for_test(port_config(8, 1, 1, 1), 8, 8, 16)
                .unwrap();
        let (flow, mut driver) = factory.open_flow(fixture.flow).unwrap();

        let reservation = flow.try_reserve_uplink_source(8).unwrap();
        assert_eq!(flow.probe().snapshot().uplink_owned_bytes(), 8);
        assert_eq!(factory.session_uplink_owned_bytes(), 8);
        reservation.commit(Bytes::from_static(b"abc")).unwrap();
        assert_eq!(flow.probe().snapshot().uplink_owned_bytes(), 3);
        assert_eq!(factory.session_uplink_owned_bytes(), 3);

        let Some(DriverInput::Data(data)) = driver.try_recv_next().unwrap() else {
            panic!("expected committed source DATA")
        };
        let (event, mut ownership) = data.into_event_and_ownership();
        let mut effects = fixture.model.reduce(event).unwrap();
        let stored = take_replay_stored(&mut effects);
        let replay = ownership.bind_replay(stored).unwrap();
        assert_eq!(replay.start(), ByteOffset::new(0));
        assert_eq!(replay.end(), ByteOffset::new(3));
        drop(replay);
        assert_eq!(factory.session_uplink_owned_bytes(), 0);
    }

    #[test]
    fn invalid_source_commit_releases_reservation_without_advancing_offset() {
        let mut fixture = fixture();
        let factory =
            ResumableTcpPortFactory::new_unbound_for_test(port_config(8, 1, 1, 1), 8, 8, 16)
                .unwrap();
        let (flow, mut driver) = factory.open_flow(fixture.flow).unwrap();

        assert_eq!(
            flow.try_reserve_uplink_source(8)
                .unwrap()
                .commit(Bytes::new())
                .unwrap_err(),
            FlowPortError::UplinkLengthMismatch {
                reserved: 8,
                actual: 0,
            }
        );
        assert_eq!(flow.probe().snapshot().uplink_owned_bytes(), 0);
        assert_eq!(factory.session_uplink_owned_bytes(), 0);
        assert!(driver.try_recv_next().unwrap().is_none());

        flow.try_reserve_uplink_source(8)
            .unwrap()
            .commit(Bytes::from_static(b"abc"))
            .unwrap();
        let Some(DriverInput::Data(data)) = driver.try_recv_next().unwrap() else {
            panic!("expected valid DATA after rejected empty commit")
        };
        let (event, mut ownership) = data.into_event_and_ownership();
        let mut effects = fixture.model.reduce(event).unwrap();
        let stored = take_replay_stored(&mut effects);
        let replay = ownership.bind_replay(stored).unwrap();
        assert_eq!(replay.start(), ByteOffset::new(0));
        assert_eq!(replay.end(), ByteOffset::new(3));
    }

    #[test]
    fn exhausted_source_offset_rejects_before_touching_the_socket_extractor() {
        let fixture = fixture();
        let (flow, mut driver) = isolated_pair(fixture.flow, port_config(8, 1, 1, 1));
        flow.state
            .next_uplink_offset
            .store(u64::MAX, Ordering::Release);
        let extractions = AtomicUsize::new(0);

        assert_eq!(
            flow.try_send_uplink_with(1, || {
                extractions.fetch_add(1, Ordering::SeqCst);
                Bytes::from_static(b"x")
            })
            .unwrap_err(),
            FlowPortError::UplinkOffsetExhausted
        );
        assert_eq!(extractions.load(Ordering::SeqCst), 0);
        assert_eq!(flow.probe().snapshot().uplink_owned_bytes(), 0);
        assert_eq!(flow.probe().snapshot().uplink_owned_segments(), 0);
        assert!(driver.try_recv_next().unwrap().is_none());
    }

    #[test]
    fn ordered_close_waits_for_outstanding_source_reservation() {
        let fixture = fixture();
        let (flow, mut driver) = isolated_pair(fixture.flow, port_config(8, 2, 1, 1));

        let reservation = flow.try_reserve_uplink_source(8).unwrap();
        assert_eq!(
            flow.try_send_close().unwrap_err(),
            FlowPortError::UplinkSourceReservationActive
        );
        assert!(driver.try_recv_next().unwrap().is_none());

        drop(reservation);
        flow.try_send_close().unwrap();
        assert!(matches!(
            driver.try_recv_next().unwrap(),
            Some(DriverInput::Control(SessionEvent::LocalClose { flow: observed }))
                if observed == fixture.flow
        ));
    }

    #[test]
    fn extractor_length_mismatch_does_not_advance_later_source_offsets() {
        let mut fixture = fixture();
        let (flow, mut driver) = isolated_pair(fixture.flow, port_config(8, 2, 1, 1));

        assert_eq!(
            flow.try_send_uplink_with(8, || Bytes::from_static(b"bad"))
                .unwrap_err(),
            FlowPortError::UplinkLengthMismatch {
                reserved: 8,
                actual: 3,
            }
        );
        assert_eq!(flow.probe().snapshot().uplink_owned_bytes(), 0);

        let mut replays = Vec::new();
        for payload in [b"abc".as_slice(), b"def".as_slice()] {
            flow.try_send_uplink_with(payload.len(), || Bytes::copy_from_slice(payload))
                .unwrap();
            let Some(DriverInput::Data(data)) = driver.try_recv_next().unwrap() else {
                panic!("expected valid DATA after mismatch")
            };
            let (event, mut ownership) = data.into_event_and_ownership();
            let mut effects = fixture.model.reduce(event).unwrap();
            let stored = take_replay_stored(&mut effects);
            replays.push(ownership.bind_replay(stored).unwrap());
        }

        assert_eq!(replays[0].start(), ByteOffset::new(0));
        assert_eq!(replays[0].end(), ByteOffset::new(3));
        assert_eq!(replays[1].start(), ByteOffset::new(3));
        assert_eq!(replays[1].end(), ByteOffset::new(6));
    }

    #[test]
    fn source_error_drops_reservation_without_capacity_or_offset_ownership() {
        let mut fixture = fixture();
        let factory =
            ResumableTcpPortFactory::new_unbound_for_test(port_config(8, 1, 1, 1), 8, 8, 16)
                .unwrap();
        let (flow, mut driver) = factory.open_flow(fixture.flow).unwrap();

        let reservation = flow.try_reserve_uplink_source(8).unwrap();
        assert_eq!(flow.probe().snapshot().uplink_owned_bytes(), 8);
        assert_eq!(factory.session_uplink_owned_bytes(), 8);
        let source_result = Result::<Bytes, &'static str>::Err("would block");
        match source_result {
            Ok(payload) => reservation.commit(payload).unwrap(),
            Err(_) => drop(reservation),
        }
        assert_eq!(flow.probe().snapshot().uplink_owned_bytes(), 0);
        assert_eq!(factory.session_uplink_owned_bytes(), 0);
        assert!(driver.try_recv_next().unwrap().is_none());

        flow.try_reserve_uplink_source(8)
            .unwrap()
            .commit(Bytes::from_static(b"abc"))
            .unwrap();
        let Some(DriverInput::Data(data)) = driver.try_recv_next().unwrap() else {
            panic!("expected valid DATA after source error")
        };
        let (event, mut ownership) = data.into_event_and_ownership();
        let mut effects = fixture.model.reduce(event).unwrap();
        let stored = take_replay_stored(&mut effects);
        let replay = ownership.bind_replay(stored).unwrap();
        assert_eq!(replay.start(), ByteOffset::new(0));
        assert_eq!(replay.end(), ByteOffset::new(3));
    }

    #[test]
    fn source_reservations_share_the_session_global_byte_budget() {
        let mut fixture = fixture();
        let second_flow = open_another_local_flow(&mut fixture, 8443);
        let factory =
            ResumableTcpPortFactory::new_unbound_for_test(port_config(8, 1, 1, 1), 4, 8, 16)
                .unwrap();
        let (first, _first_driver) = factory.open_flow(fixture.flow).unwrap();
        let (second, mut second_driver) = factory.open_flow(second_flow).unwrap();

        let first_reservation = first.try_reserve_uplink_source(4).unwrap();
        assert_eq!(factory.session_uplink_owned_bytes(), 4);
        let Err(error) = second.try_reserve_uplink_source(1) else {
            panic!("the second flow must not bypass the shared session byte budget")
        };
        assert_eq!(
            error,
            FlowPortError::UplinkGlobalByteBudgetExhausted {
                requested: 1,
                available: 0,
            }
        );

        drop(first_reservation);
        assert_eq!(factory.session_uplink_owned_bytes(), 0);
        second
            .try_reserve_uplink_source(1)
            .unwrap()
            .commit(Bytes::from_static(b"x"))
            .unwrap();
        assert_eq!(factory.session_uplink_owned_bytes(), 1);
        let Some(DriverInput::Data(data)) = second_driver.try_recv_next().unwrap() else {
            panic!("expected second flow DATA after shared capacity was released")
        };
        drop(data);
        assert_eq!(factory.session_uplink_owned_bytes(), 0);
    }

    #[test]
    fn one_source_reservation_blocks_ordered_sources_but_not_urgent_reset() {
        let fixture = fixture();
        let (flow, mut driver) = isolated_pair(fixture.flow, port_config(8, 2, 1, 1));

        let reservation = flow.try_reserve_uplink_source(8).unwrap();
        let Err(error) = flow.try_reserve_uplink_source(1) else {
            panic!("a flow must not hold two ordered-source reservations")
        };
        assert_eq!(error, FlowPortError::UplinkSourceReservationActive);
        flow.try_send_reset(ResetReason::LocalAbandon).unwrap();
        assert!(matches!(
            driver.try_recv_next().unwrap(),
            Some(DriverInput::Control(SessionEvent::LocalReset {
                flow: reset_flow,
                reason: ResetReason::LocalAbandon,
            })) if reset_flow == fixture.flow
        ));

        drop(reservation);
        assert_eq!(flow.probe().snapshot().uplink_owned_bytes(), 0);
        flow.try_reserve_uplink_source(1)
            .unwrap()
            .commit(Bytes::from_static(b"x"))
            .unwrap();
        assert!(matches!(
            driver.try_recv_next().unwrap(),
            Some(DriverInput::Data(_))
        ));
    }

    #[test]
    fn uplink_queue_normalizes_a_small_slice_before_taking_ownership() {
        let fixture = fixture();
        let (flow, mut driver) = isolated_pair(fixture.flow, port_config(8, 1, 1, 1));
        let backing = Bytes::from(vec![0x5a; 1024 * 1024]);
        let tiny = backing.slice(512..516);
        let unbounded_pointer = tiny.as_ptr();
        flow.try_send_uplink_with(4, move || tiny).unwrap();
        let Some(DriverInput::Data(data)) = driver.try_recv_next().unwrap() else {
            panic!("expected normalized DATA")
        };
        assert_eq!(data.payload(), [0x5a; 4]);
        assert_ne!(data.payload().as_ptr(), unbounded_pointer);
    }

    #[test]
    fn exact_vec_extractor_transfers_without_second_copy_and_compacts_excess_capacity() {
        let fixture = fixture();
        let (flow, mut driver) = isolated_pair(fixture.flow, port_config(4, 1, 1, 1));
        let mut exact_pointer = 0usize;
        flow.try_send_uplink_vec_with(4, || {
            let payload = vec![1, 2, 3, 4];
            assert_eq!(payload.capacity(), payload.len());
            exact_pointer = payload.as_ptr() as usize;
            payload
        })
        .unwrap();
        let Some(DriverInput::Data(exact)) = driver.try_recv_next().unwrap() else {
            panic!("expected exact Vec DATA")
        };
        assert_eq!(exact.payload().as_ptr() as usize, exact_pointer);

        let calls = AtomicUsize::new(0);
        assert_eq!(
            flow.try_send_uplink_vec_with(1, || {
                calls.fetch_add(1, Ordering::SeqCst);
                vec![9]
            })
            .unwrap_err(),
            FlowPortError::UplinkByteBudgetExhausted {
                requested: 1,
                available: 0,
            }
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        drop(exact);

        let mut excess_pointer = 0usize;
        flow.try_send_uplink_vec_with(4, || {
            let mut payload = Vec::with_capacity(1024 * 1024);
            payload.extend_from_slice(&[5, 6, 7, 8]);
            excess_pointer = payload.as_ptr() as usize;
            payload
        })
        .unwrap();
        let Some(DriverInput::Data(compacted)) = driver.try_recv_next().unwrap() else {
            panic!("expected compacted Vec DATA")
        };
        assert_eq!(compacted.payload(), [5, 6, 7, 8]);
        assert_ne!(compacted.payload().as_ptr() as usize, excess_pointer);
    }

    #[test]
    fn uplink_replay_budget_releases_only_on_exact_terminal_reducer_effect() {
        let mut fixture = fixture();
        let (flow, mut driver) = isolated_pair(fixture.flow, port_config(8, 1, 2, 1));
        flow.try_send_uplink_with(4, || Bytes::from_static(b"data"))
            .unwrap();
        let Some(DriverInput::Data(data)) = driver.try_recv_next().unwrap() else {
            panic!("expected DATA")
        };
        let (event, mut ownership) = data.into_event_and_ownership();
        let mut stored_effects = fixture.model.reduce(event).unwrap();
        let stored = take_replay_stored(&mut stored_effects);
        let mut replay = ownership.bind_replay(stored).unwrap();
        let terminal_effects = fixture
            .model
            .reduce(SessionEvent::LocalReset {
                flow: fixture.flow,
                reason: ResetReason::LocalAbandon,
            })
            .unwrap();
        let nonterminal = terminal_effects
            .iter()
            .find(|effect| matches!(effect, SessionEffect::Transmit(_)))
            .unwrap();
        assert_eq!(
            replay
                .release_after_reducer_terminal(nonterminal)
                .unwrap_err(),
            FlowPortError::UplinkTerminalMismatch
        );
        assert_eq!(flow.probe().snapshot().uplink_owned_bytes(), 4);
        assert_eq!(flow.probe().snapshot().uplink_owned_segments(), 1);
        let terminal = terminal_effects
            .iter()
            .find(|effect| matches!(effect, SessionEffect::FlowFinished { .. }))
            .unwrap();
        assert!(replay.release_after_reducer_terminal(terminal).unwrap());
        assert_eq!(flow.probe().snapshot().uplink_owned_bytes(), 0);
        assert_eq!(flow.probe().snapshot().uplink_owned_segments(), 0);
    }

    #[test]
    fn local_close_cannot_overtake_preceding_uplink_data() {
        let mut fixture = fixture();
        let (flow, mut driver) = isolated_pair(fixture.flow, port_config(8, 2, 2, 1));
        flow.try_send_uplink_with(1, || Bytes::from_static(b"x"))
            .unwrap();
        flow.try_send_close().unwrap();

        let Some(DriverInput::Data(data)) = driver.try_recv_next().unwrap() else {
            panic!("DATA queued before local FIN must be reduced first")
        };
        let (data_event, mut ownership) = data.into_event_and_ownership();
        let mut data_effects = fixture.model.reduce(data_event).unwrap();
        let stored = take_replay_stored(&mut data_effects);
        let data_frame = data_effects
            .iter()
            .find_map(|effect| match effect {
                SessionEffect::Transmit(frame) => Some(frame),
                _ => None,
            })
            .unwrap();
        assert!(matches!(
            data_frame.record(),
            Record::Data {
                offset,
                payload,
                ..
            } if *offset == ByteOffset::new(0) && payload.as_ref() == b"x"
        ));
        let _replay = ownership.bind_replay(stored).unwrap();

        let Some(DriverInput::Control(close_event @ SessionEvent::LocalClose { .. })) =
            driver.try_recv_next().unwrap()
        else {
            panic!("local FIN must follow every preceding DATA extent")
        };
        let close_frame = fixture
            .model
            .reduce(close_event)
            .unwrap()
            .into_iter()
            .find_map(|effect| match effect {
                SessionEffect::Transmit(frame) => Some(frame),
                _ => None,
            })
            .unwrap();
        assert!(matches!(
            close_frame.record(),
            Record::Close { final_offset, .. } if *final_offset == ByteOffset::new(1)
        ));
    }

    #[test]
    fn local_reset_has_reserved_terminal_capacity_and_urgent_priority() {
        let mut fixture = fixture();
        let (offer, segments) = sink_offer(&mut fixture, b"peer-data");
        let (flow, mut driver) = isolated_pair(fixture.flow, port_config(8, 1, 1, 1));

        // Reserving the sink completion occupies the sole ordinary control
        // slot. A queued DATA extent simultaneously occupies the source lane.
        driver.try_deliver_sink(offer, segments).unwrap();
        flow.try_send_uplink_with(1, || Bytes::from_static(b"x"))
            .unwrap();

        flow.try_send_reset(ResetReason::LocalAbandon).unwrap();
        let Some(DriverInput::Control(reset @ SessionEvent::LocalReset { .. })) =
            driver.try_recv_next().unwrap()
        else {
            panic!("the reserved terminal reset must outrank ordinary source/control work")
        };
        assert!(
            fixture
                .model
                .reduce(reset)
                .unwrap()
                .iter()
                .any(|effect| matches!(
                    effect,
                    SessionEffect::FlowFinished {
                        reason: FlowFinishReason::LocalReset(ResetReason::LocalAbandon),
                        ..
                    }
                ))
        );
    }

    #[test]
    fn abandoning_an_uninstalled_flow_emits_exact_local_reset() {
        let fixture = fixture();
        let (flow, mut driver) = isolated_pair(fixture.flow, port_config(8, 1, 1, 1));

        flow.abandon_before_install().unwrap();

        let Some(DriverInput::Control(SessionEvent::LocalReset {
            flow: reset_flow,
            reason,
        })) = driver.try_recv_next().unwrap()
        else {
            panic!("an uninstalled TUN owner must leave one exact reset for the supervisor")
        };
        assert_eq!(reset_flow, fixture.flow);
        assert_eq!(reason, ResetReason::LocalAbandon);
    }

    #[test]
    fn bounded_control_burst_cannot_starve_ready_ordered_source() {
        let mut fixture = fixture();
        let (offer, _) = sink_offer(&mut fixture, b"peer-data");
        let (flow, mut driver) =
            isolated_pair(fixture.flow, port_config(1, 1, CONTROL_BURST_LIMIT + 1, 1));
        flow.try_send_uplink_with(1, || Bytes::from_static(b"x"))
            .unwrap();
        let control = driver.completion_tx.upgrade().unwrap();
        for _ in 0..=CONTROL_BURST_LIMIT {
            control
                .try_send(FlowControl::SinkAbandoned { offer })
                .unwrap();
        }

        for _ in 0..CONTROL_BURST_LIMIT {
            assert!(matches!(
                driver.try_recv_next().unwrap(),
                Some(DriverInput::Control(_))
            ));
            control
                .try_send(FlowControl::SinkAbandoned { offer })
                .unwrap();
        }
        assert!(matches!(
            driver.try_recv_next().unwrap(),
            Some(DriverInput::Data(_))
        ));
    }

    #[test]
    fn ready_completion_preempts_an_ordered_source_flood() {
        let mut fixture = fixture();
        let (offer, _) = sink_offer(&mut fixture, b"peer-data");
        let (flow, mut driver) = isolated_pair(fixture.flow, port_config(4, 4, 1, 1));
        for _ in 0..4 {
            flow.try_send_uplink_with(1, || Bytes::from_static(b"x"))
                .unwrap();
        }
        driver
            .completion_tx
            .upgrade()
            .unwrap()
            .try_send(FlowControl::SinkAbandoned { offer })
            .unwrap();
        assert!(matches!(
            driver.try_recv_next().unwrap(),
            Some(DriverInput::Control(_))
        ));
        assert!(matches!(
            driver.try_recv_next().unwrap(),
            Some(DriverInput::Data(_))
        ));
    }

    #[test]
    fn sink_completion_has_priority_over_ordered_source_and_leg_loss_is_not_terminal() {
        let mut fixture = fixture();
        let (offer, segments) = sink_offer(&mut fixture, b"peer-data");
        let (mut flow, mut driver) = isolated_pair(fixture.flow, port_config(8, 2, 2, 1));
        flow.try_send_uplink_with(1, || Bytes::from_static(b"x"))
            .unwrap();
        driver.try_deliver_sink(offer, segments).unwrap();
        let TunAction::Sink(delivery) = flow.try_recv_action().unwrap().unwrap() else {
            panic!("expected sink delivery")
        };
        delivery.abandon();
        driver.note_leg_lost();

        assert!(matches!(
            driver.try_recv_next().unwrap(),
            Some(DriverInput::Control(SessionEvent::SinkAbandoned { offer: observed }))
                if observed == offer
        ));
        assert!(matches!(
            driver.try_recv_next().unwrap(),
            Some(DriverInput::Data(_))
        ));
        assert_eq!(driver.probe().snapshot().observed_leg_losses(), 1);
    }

    #[test]
    fn partial_sink_completion_discards_old_suffix_and_waits_for_new_offer() {
        let mut fixture = fixture();
        let (offer, segments) = sink_offer(&mut fixture, b"abcdef");
        let (mut flow, mut driver) = isolated_pair(fixture.flow, port_config(8, 2, 2, 2));
        driver.try_deliver_sink(offer, segments).unwrap();
        let TunAction::Sink(delivery) = flow.try_recv_action().unwrap().unwrap() else {
            panic!("expected sink delivery")
        };
        assert_eq!(delivery.first_segment_slice(), b"abcdef");
        let delivery = delivery.complete_write(0).unwrap().unwrap();
        assert_eq!(driver.probe().snapshot().reserved_completion_slots(), 1);
        assert!(delivery.complete_write(2).unwrap().is_none());

        let Some(DriverInput::Control(event @ SessionEvent::SinkAccepted { bytes: 2, .. })) =
            driver.try_recv_next().unwrap()
        else {
            panic!("expected exact sink acceptance")
        };
        let effects = fixture.model.reduce(event).unwrap();
        let (new_offer, new_segments) = effects
            .into_iter()
            .find_map(|effect| match effect {
                SessionEffect::OfferToSink { offer, segments } => Some((offer, segments)),
                _ => None,
            })
            .unwrap();
        assert_eq!(new_offer.offset(), ByteOffset::new(2));
        assert_eq!(new_offer.len(), 4);
        assert_eq!(new_segments[0].payload(), &Bytes::from_static(b"cdef"));
        assert_eq!(driver.probe().snapshot().reserved_completion_slots(), 0);
    }

    #[test]
    fn abandon_uses_pre_reserved_control_slot() {
        let mut fixture = fixture();
        let (offer, segments) = sink_offer(&mut fixture, b"payload");
        let (mut flow, mut driver) = isolated_pair(fixture.flow, port_config(8, 1, 1, 1));
        driver.try_deliver_sink(offer, segments).unwrap();
        assert_eq!(driver.probe().snapshot().reserved_completion_slots(), 1);
        let TunAction::Sink(delivery) = flow.try_recv_action().unwrap().unwrap() else {
            panic!("expected sink delivery")
        };
        delivery.abandon();
        assert!(matches!(
            driver.try_recv_next().unwrap(),
            Some(DriverInput::Control(SessionEvent::SinkAbandoned { offer: observed }))
                if observed == offer
        ));
    }

    #[test]
    fn cancellation_drop_fails_closed_for_sink_and_half_close() {
        let mut sink_fixture = fixture();
        let (offer, segments) = sink_offer(&mut sink_fixture, b"payload");
        let (mut sink_flow, mut sink_driver) =
            isolated_pair(sink_fixture.flow, port_config(8, 1, 1, 1));
        sink_driver.try_deliver_sink(offer, segments).unwrap();
        let TunAction::Sink(delivery) = sink_flow.try_recv_action().unwrap().unwrap() else {
            panic!("expected sink delivery")
        };
        drop(delivery);
        assert!(matches!(
            sink_driver.try_recv_next().unwrap(),
            Some(DriverInput::Control(SessionEvent::SinkAbandoned { offer: observed }))
                if observed == offer
        ));

        let mut close_fixture = fixture();
        let capability = half_close(&mut close_fixture);
        let (mut close_flow, mut close_driver) =
            isolated_pair(close_fixture.flow, port_config(8, 1, 1, 1));
        close_driver.try_deliver_half_close(capability).unwrap();
        let TunAction::HalfClose(delivery) = close_flow.try_recv_action().unwrap().unwrap() else {
            panic!("expected half-close delivery")
        };
        drop(delivery);
        assert!(matches!(
            close_driver.try_recv_next().unwrap(),
            Some(DriverInput::Control(SessionEvent::SinkHalfCloseFailed { completion }))
                if completion == capability
        ));
    }

    #[test]
    fn downlink_refuses_delivery_when_completion_lane_has_no_slot() {
        let mut fixture = fixture();
        let (offer, segments) = sink_offer(&mut fixture, b"payload");
        let retry_segments = segments.clone();
        let (mut flow, driver) = isolated_pair(fixture.flow, port_config(8, 1, 1, 1));
        driver.try_deliver_sink(offer, segments).unwrap();
        let TunAction::Sink(delivery) = flow.try_recv_action().unwrap().unwrap() else {
            panic!("expected sink delivery")
        };
        delivery.abandon();
        assert_eq!(
            driver.try_deliver_sink(offer, retry_segments).unwrap_err(),
            FlowPortError::ControlLaneFull
        );
        assert_eq!(driver.probe().snapshot().pending_downlink_actions(), 0);
    }

    #[test]
    fn sink_and_half_close_capabilities_cannot_cross_direction() {
        let client = fixture();
        let mut owner = owner_fixture();
        assert_eq!(client.flow.session_id(), owner.flow.session_id());
        assert_eq!(client.flow.flow_id(), owner.flow.flow_id());
        assert_ne!(client.flow.direction(), owner.flow.direction());
        let (_flow, driver) = isolated_pair(client.flow, port_config(8, 1, 2, 2));

        let offered = owner
            .model
            .reduce(SessionEvent::PeerFrame {
                leg: owner.leg,
                frame: Frame::try_new(
                    owner.leg.generation(),
                    Record::Data {
                        flow_id: owner.flow.flow_id(),
                        direction: Direction::ClientToTarget,
                        offset: ByteOffset::new(0),
                        payload: Bytes::from_static(b"x"),
                    },
                )
                .unwrap(),
            })
            .unwrap();
        let (offer, segments) = offered
            .into_iter()
            .find_map(|effect| match effect {
                SessionEffect::OfferToSink { offer, segments } => Some((offer, segments)),
                _ => None,
            })
            .unwrap();
        assert_eq!(
            driver.try_deliver_sink(offer, segments).unwrap_err(),
            FlowPortError::SinkDirectionMismatch {
                expected: Direction::TargetToClient,
                actual: Direction::ClientToTarget,
            }
        );

        let mut close_owner = owner_fixture();
        let closed = close_owner
            .model
            .reduce(SessionEvent::PeerFrame {
                leg: close_owner.leg,
                frame: Frame::try_new(
                    close_owner.leg.generation(),
                    Record::Close {
                        flow_id: close_owner.flow.flow_id(),
                        direction: Direction::ClientToTarget,
                        final_offset: ByteOffset::new(0),
                    },
                )
                .unwrap(),
            })
            .unwrap();
        let completion = closed
            .into_iter()
            .find_map(|effect| match effect {
                SessionEffect::HalfCloseSink { completion } => Some(completion),
                _ => None,
            })
            .unwrap();
        assert_eq!(
            driver.try_deliver_half_close(completion).unwrap_err(),
            FlowPortError::SinkDirectionMismatch {
                expected: Direction::TargetToClient,
                actual: Direction::ClientToTarget,
            }
        );
        assert!(driver.probe().snapshot().is_quiescent());
    }

    #[test]
    fn half_close_reports_success_or_failure_only_after_local_completion() {
        let mut success_fixture = fixture();
        let success_capability = half_close(&mut success_fixture);
        let (mut success_flow, mut success_driver) =
            isolated_pair(success_fixture.flow, port_config(8, 1, 1, 1));
        success_driver
            .try_deliver_half_close(success_capability)
            .unwrap();
        assert!(success_driver.try_recv_next().unwrap().is_none());
        let TunAction::HalfClose(delivery) = success_flow.try_recv_action().unwrap().unwrap()
        else {
            panic!("expected half-close")
        };
        delivery.succeed();
        assert!(matches!(
            success_driver.try_recv_next().unwrap(),
            Some(DriverInput::Control(SessionEvent::SinkHalfClosed { completion }))
                if completion == success_capability
        ));

        let mut failure_fixture = fixture();
        let failure_capability = half_close(&mut failure_fixture);
        let (mut failure_flow, mut failure_driver) =
            isolated_pair(failure_fixture.flow, port_config(8, 1, 1, 1));
        failure_driver
            .try_deliver_half_close(failure_capability)
            .unwrap();
        let TunAction::HalfClose(delivery) = failure_flow.try_recv_action().unwrap().unwrap()
        else {
            panic!("expected half-close")
        };
        delivery.fail();
        assert!(matches!(
            failure_driver.try_recv_next().unwrap(),
            Some(DriverInput::Control(SessionEvent::SinkHalfCloseFailed { completion }))
                if completion == failure_capability
        ));
    }

    #[test]
    fn split_halves_remain_independent_nonaliasing_owners() {
        let fixture = fixture();
        let (flow, mut driver) = isolated_pair(fixture.flow, port_config(8, 2, 2, 2));
        let probe = flow.probe();
        let (uplink, downlink) = flow.into_split();
        assert_eq!(uplink.local_flow(), fixture.flow);
        assert_eq!(downlink.local_flow(), fixture.flow);

        drop(downlink);
        uplink
            .try_send_uplink_with(4, || Bytes::from_static(b"data"))
            .unwrap();
        let Some(DriverInput::Data(data)) = driver.try_recv_next().unwrap() else {
            panic!("expected independently admitted uplink DATA")
        };
        assert_eq!(probe.snapshot().uplink_owned_bytes(), 4);
        drop(data);
        assert!(probe.snapshot().is_quiescent());
        drop(uplink);
        drop(driver);
        assert!(probe.snapshot().is_quiescent());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn async_driver_waits_for_reserved_completion_after_uplink_half_closes() {
        let mut fixture = fixture();
        let (offer, segments) = sink_offer(&mut fixture, b"payload");
        let (flow, mut driver) = isolated_pair(fixture.flow, port_config(8, 1, 1, 1));
        let (uplink, mut downlink) = flow.into_split();
        driver.try_deliver_sink(offer, segments).unwrap();
        let TunAction::Sink(delivery) = downlink.try_recv_action().unwrap().unwrap() else {
            panic!("expected sink delivery")
        };
        drop(uplink);

        let waiting = tokio::spawn(async move { driver.recv_next().await });
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());
        delivery.abandon();
        assert!(matches!(
            waiting.await.unwrap(),
            Some(DriverInput::Control(SessionEvent::SinkAbandoned { offer: observed }))
                if observed == offer
        ));
    }

    #[test]
    fn reducer_terminal_effects_are_forwarded_as_typed_tun_actions() {
        let mut fixture = fixture();
        let (flow, mut driver) = isolated_pair(fixture.flow, port_config(8, 1, 1, 2));
        let (_uplink, mut downlink) = flow.into_split();
        let constructible_notification = SessionEffect::PeerReset {
            flow_id: fixture.flow.flow_id(),
            reason: ResetReason::TargetFailure,
        };
        assert_eq!(
            driver
                .try_deliver_terminal_effect(&constructible_notification)
                .unwrap_err(),
            FlowPortError::NotTerminalEffect
        );
        let mut other = fixture_for_session(0x99);
        assert_eq!(other.flow.flow_id(), fixture.flow.flow_id());
        let other_effects = other
            .model
            .reduce(SessionEvent::PeerFrame {
                leg: other.leg,
                frame: Frame::try_new(
                    other.leg.generation(),
                    Record::Reset {
                        flow_id: other.flow.flow_id(),
                        reason: ResetReason::TargetFailure,
                    },
                )
                .unwrap(),
            })
            .unwrap();
        let other_terminal = other_effects
            .iter()
            .find(|effect| matches!(effect, SessionEffect::FlowFinished { .. }))
            .unwrap();
        assert_eq!(
            driver
                .try_deliver_terminal_effect(other_terminal)
                .unwrap_err(),
            FlowPortError::CrossFlowTerminalEffect
        );
        let mut opposite = owner_fixture();
        assert_eq!(opposite.flow.session_id(), fixture.flow.session_id());
        assert_eq!(opposite.flow.flow_id(), fixture.flow.flow_id());
        assert_ne!(opposite.flow.direction(), fixture.flow.direction());
        let opposite_effects = opposite
            .model
            .reduce(SessionEvent::LocalReset {
                flow: opposite.flow,
                reason: ResetReason::LocalAbandon,
            })
            .unwrap();
        let opposite_terminal = opposite_effects
            .iter()
            .find(|effect| matches!(effect, SessionEffect::FlowFinished { .. }))
            .unwrap();
        assert_eq!(
            driver
                .try_deliver_terminal_effect(opposite_terminal)
                .unwrap_err(),
            FlowPortError::CrossFlowTerminalEffect
        );
        let terminal_effects = fixture
            .model
            .reduce(SessionEvent::PeerFrame {
                leg: fixture.leg,
                frame: Frame::try_new(
                    fixture.leg.generation(),
                    Record::Reset {
                        flow_id: fixture.flow.flow_id(),
                        reason: ResetReason::TargetFailure,
                    },
                )
                .unwrap(),
            })
            .unwrap();
        let terminal = terminal_effects
            .iter()
            .find(|effect| matches!(effect, SessionEffect::FlowFinished { .. }))
            .unwrap();
        driver.try_deliver_terminal_effect(terminal).unwrap();
        assert_eq!(
            driver.try_deliver_terminal_effect(terminal).unwrap_err(),
            FlowPortError::TerminalAlreadyDelivered
        );
        let TunAction::Terminal(terminal) = downlink.try_recv_action().unwrap().unwrap() else {
            panic!("expected terminal action")
        };
        assert_eq!(terminal.local_flow(), fixture.flow);
        assert_eq!(
            terminal.action(),
            TerminalAction::FlowFinished(FlowFinishReason::PeerReset(ResetReason::TargetFailure))
        );
    }

    #[test]
    fn terminal_authority_drains_queued_source_and_completion_ownership() {
        let mut fixture = fixture();
        let (offer, segments) = sink_offer(&mut fixture, b"peer-data");
        let (mut flow, mut driver) = isolated_pair(fixture.flow, port_config(8, 2, 2, 1));
        let probe = flow.probe();
        flow.try_send_uplink_with(4, || Bytes::from_static(b"late"))
            .unwrap();
        driver.try_deliver_sink(offer, segments).unwrap();
        let TunAction::Sink(delivery) = flow.try_recv_action().unwrap().unwrap() else {
            panic!("expected sink delivery")
        };
        delivery.abandon();
        assert_eq!(probe.snapshot().uplink_owned_bytes(), 4);

        let effects = fixture
            .model
            .reduce(SessionEvent::PeerFrame {
                leg: fixture.leg,
                frame: Frame::try_new(
                    fixture.leg.generation(),
                    Record::Reset {
                        flow_id: fixture.flow.flow_id(),
                        reason: ResetReason::TargetFailure,
                    },
                )
                .unwrap(),
            })
            .unwrap();
        let terminal = effects
            .iter()
            .find(|effect| matches!(effect, SessionEffect::FlowFinished { .. }))
            .unwrap();
        driver.try_deliver_terminal_effect(terminal).unwrap();
        assert_eq!(probe.snapshot().uplink_owned_bytes(), 0);
        assert_eq!(probe.snapshot().reserved_completion_slots(), 0);
        assert!(!matches!(driver.try_recv_next(), Ok(Some(_))));

        let calls = AtomicUsize::new(0);
        assert_eq!(
            flow.try_send_uplink_with(1, || {
                calls.fetch_add(1, Ordering::SeqCst);
                Bytes::from_static(b"x")
            })
            .unwrap_err(),
            FlowPortError::Closed
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let TunAction::Terminal(terminal) = flow.try_recv_action().unwrap().unwrap() else {
            panic!("expected exact terminal delivery")
        };
        assert_eq!(
            terminal.action(),
            TerminalAction::FlowFinished(FlowFinishReason::PeerReset(ResetReason::TargetFailure))
        );
        drop(terminal);
        assert!(probe.snapshot().is_quiescent());
    }

    #[test]
    fn terminal_authority_revokes_already_forwarded_sink_capabilities() {
        let mut sink_fixture = fixture();
        let (offer, segments) = sink_offer(&mut sink_fixture, b"peer-data");
        let (mut sink_flow, mut sink_driver) =
            isolated_pair(sink_fixture.flow, port_config(8, 1, 1, 1));
        let sink_probe = sink_flow.probe();
        sink_driver.try_deliver_sink(offer, segments).unwrap();
        let TunAction::Sink(sink_delivery) = sink_flow.try_recv_action().unwrap().unwrap() else {
            panic!("expected forwarded sink capability")
        };
        assert!(sink_delivery.is_live());
        let sink_mutations = AtomicUsize::new(0);
        sink_delivery
            .with_live_first_segment(|segment| {
                assert_eq!(segment, b"peer-data");
                sink_mutations.fetch_add(1, Ordering::SeqCst);
            })
            .unwrap();
        let sink_terminal = sink_fixture
            .model
            .reduce(SessionEvent::LocalReset {
                flow: sink_fixture.flow,
                reason: ResetReason::LocalAbandon,
            })
            .unwrap();
        let sink_terminal = sink_terminal
            .iter()
            .find(|effect| matches!(effect, SessionEffect::FlowFinished { .. }))
            .unwrap();
        sink_driver
            .try_deliver_terminal_effect(sink_terminal)
            .unwrap();
        assert!(!sink_delivery.is_live());
        assert_eq!(
            sink_delivery
                .with_live_first_segment(|_| {
                    sink_mutations.fetch_add(1, Ordering::SeqCst);
                })
                .unwrap_err(),
            FlowPortError::SinkCapabilityRevoked
        );
        assert_eq!(sink_mutations.load(Ordering::SeqCst), 1);
        drop(sink_delivery);
        let terminal = sink_flow.try_recv_action().unwrap().unwrap();
        drop(terminal);
        assert!(sink_probe.snapshot().is_quiescent());

        let mut close_fixture = fixture();
        let completion = half_close(&mut close_fixture);
        let (mut close_flow, mut close_driver) =
            isolated_pair(close_fixture.flow, port_config(8, 1, 1, 1));
        let close_probe = close_flow.probe();
        close_driver.try_deliver_half_close(completion).unwrap();
        let TunAction::HalfClose(close_delivery) = close_flow.try_recv_action().unwrap().unwrap()
        else {
            panic!("expected forwarded half-close capability")
        };
        assert!(close_delivery.is_live());
        let close_mutations = AtomicUsize::new(0);
        close_delivery
            .with_live(|| {
                close_mutations.fetch_add(1, Ordering::SeqCst);
            })
            .unwrap();
        let close_terminal = close_fixture
            .model
            .reduce(SessionEvent::LocalReset {
                flow: close_fixture.flow,
                reason: ResetReason::LocalAbandon,
            })
            .unwrap();
        let close_terminal = close_terminal
            .iter()
            .find(|effect| matches!(effect, SessionEffect::FlowFinished { .. }))
            .unwrap();
        close_driver
            .try_deliver_terminal_effect(close_terminal)
            .unwrap();
        assert!(!close_delivery.is_live());
        assert_eq!(
            close_delivery
                .with_live(|| {
                    close_mutations.fetch_add(1, Ordering::SeqCst);
                })
                .unwrap_err(),
            FlowPortError::SinkCapabilityRevoked
        );
        assert_eq!(close_mutations.load(Ordering::SeqCst), 1);
        drop(close_delivery);
        let terminal = close_flow.try_recv_action().unwrap().unwrap();
        drop(terminal);
        assert!(close_probe.snapshot().is_quiescent());
    }

    #[test]
    fn debug_is_redacted_and_endpoint_drop_releases_all_port_ownership() {
        let mut fixture = fixture();
        let (offer, segments) = sink_offer(&mut fixture, b"DOWNLINK_SECRET_SENTINEL");
        let (mut flow, mut driver) = isolated_pair(fixture.flow, port_config(64, 2, 2, 2));
        let probe = flow.probe();
        flow.try_send_uplink_with(22, || Bytes::from_static(b"UPLINK_SECRET_SENTINEL"))
            .unwrap();
        let DriverInput::Data(data) = driver.try_recv_next().unwrap().unwrap() else {
            panic!("expected DATA")
        };
        let debug = format!("{data:?}");
        assert!(!debug.contains("UPLINK_SECRET_SENTINEL"));
        driver.try_deliver_sink(offer, segments).unwrap();
        let action = flow.try_recv_action().unwrap().unwrap();
        let debug = format!("{action:?}");
        assert!(!debug.contains("DOWNLINK_SECRET_SENTINEL"));
        assert!(!probe.snapshot().is_quiescent());

        drop(data);
        drop(action);
        drop(flow);
        drop(driver);
        assert!(probe.snapshot().is_quiescent());
    }
}
