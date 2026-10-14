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

pub(super) mod cold_inventory;
mod driver_receive;
mod factory_construction;
mod uplink_replay;
use std::fmt;
use std::sync::{
    Arc, Mutex, MutexGuard, Weak,
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
};

use bytes::Bytes;
use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch};

use crate::resumable::{
    ByteOffset, Direction, FlowFinishReason, LocalFlow, MAX_DATA_PAYLOAD_BYTES, ReplayAcknowledged,
    ReplayStored, ResetReason, SessionEffect, SessionEvent, SessionFlowId, SessionId,
    SinkHalfClose, SinkOffer, TcpDataSegment,
};

use super::supervisor::{ClientControllerDriverReducerGate, TcpFactoryOrigin};

const TERMINAL_RESET_LANE_CAPACITY: usize = 1;
const TERMINAL_ACTION_LANE_CAPACITY: usize = 1;
const CONTROL_BURST_LIMIT: usize = 8;
const MAX_EXACT_UPLINK_BYTE_CAPACITY: usize = if (u32::MAX as usize) < Semaphore::MAX_PERMITS {
    u32::MAX as usize
} else {
    Semaphore::MAX_PERMITS
};
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
        validate_uplink_byte_capacity(uplink_byte_capacity)?;
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

    pub(crate) const fn max_exact_uplink_byte_capacity() -> usize {
        MAX_EXACT_UPLINK_BYTE_CAPACITY
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
    #[error("uplink byte capacity {value} exceeds the exact permit ceiling {max}")]
    UplinkByteCapacityTooLarge { value: usize, max: usize },
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
    #[error("per-flow TCP channel slot envelope overflowed")]
    ChannelEnvelopeOverflow,
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
    #[error("flow driver admission is sealed for local abandonment")]
    DriverAdmissionSealed,
    #[error("flow factory admission is closed for session shutdown")]
    FactoryAdmissionClosed,
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
    #[error("Owner terminal retains completions={completions}, actions={actions}")]
    OwnerTerminalOwnershipPending { completions: usize, actions: usize },
    #[error("Owner source admission mutex is poisoned")]
    OwnerAdmissionPoisoned,
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
enum FactoryFlowBinding {
    Unbound,
    Bound {
        session_id: SessionId,
        direction: Direction,
        highest_authority_id: u64,
    },
}

#[derive(Clone, Copy, Debug)]
struct FactoryFlowGate {
    binding: FactoryFlowBinding,
    closed: bool,
}

impl FactoryFlowGate {
    const fn unbound() -> Self {
        Self {
            binding: FactoryFlowBinding::Unbound,
            closed: false,
        }
    }

    const fn bound(session_id: SessionId, direction: Direction) -> Self {
        Self {
            binding: FactoryFlowBinding::Bound {
                session_id,
                direction,
                highest_authority_id: 0,
            },
            closed: false,
        }
    }

    const fn bound_direction(self) -> Option<Direction> {
        match self.binding {
            FactoryFlowBinding::Unbound => None,
            FactoryFlowBinding::Bound { direction, .. } => Some(direction),
        }
    }

    fn admit(&mut self, flow: LocalFlow) -> Result<(), FlowPortError> {
        if self.closed {
            return Err(FlowPortError::FactoryAdmissionClosed);
        }
        match &mut self.binding {
            FactoryFlowBinding::Unbound => {
                self.binding = FactoryFlowBinding::Bound {
                    session_id: flow.session_id(),
                    direction: flow.direction(),
                    highest_authority_id: flow.authority_id(),
                };
                Ok(())
            }
            FactoryFlowBinding::Bound {
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

    fn close(&mut self) -> TcpFactoryAdmissionClose {
        if self.closed {
            return TcpFactoryAdmissionClose::AlreadyClosed;
        }
        self.closed = true;
        TcpFactoryAdmissionClose::ClosedNow
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum TcpFactoryAdmissionClose {
    ClosedNow,
    AlreadyClosed,
}

/// Opaque snapshot of one factory's shared progress sequence. Its private
/// weak identity prevents a cursor from one factory being accepted by a
/// sibling factory, while not extending the factory lifetime by itself.
#[derive(Clone)]
pub(crate) struct TcpFactoryProgressCursor {
    factory: Weak<FactoryProgress>,
    epoch: u64,
}

impl fmt::Debug for TcpFactoryProgressCursor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("TcpFactoryProgressCursor([OPAQUE])")
    }
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub(crate) enum TcpFactoryProgressWaitError {
    #[error("progress cursor belongs to another TCP flow factory")]
    WrongFactory,
    #[error("TCP flow factory progress source closed unexpectedly")]
    ProgressSourceClosed,
}

struct FactoryProgress {
    epoch: watch::Sender<u64>,
}

impl FactoryProgress {
    fn new() -> Arc<Self> {
        let (epoch, _initial_receiver) = watch::channel(0);
        Arc::new(Self { epoch })
    }

    fn cursor(progress: &Arc<Self>) -> TcpFactoryProgressCursor {
        let epoch = *progress.epoch.borrow();
        TcpFactoryProgressCursor {
            factory: Arc::downgrade(progress),
            epoch,
        }
    }

    fn publish(&self) {
        // Saturation preserves cursor monotonicity and cannot wrap back to an
        // ancient cursor. Reaching `u64::MAX` requires exhausting the process
        // lifetime's practical event budget, matching actor readiness policy.
        let _changed = self.epoch.send_if_modified(|epoch| {
            let next = epoch.saturating_add(1);
            if next == *epoch {
                false
            } else {
                *epoch = next;
                true
            }
        });
    }

    async fn wait(
        progress: &Arc<Self>,
        cursor: &TcpFactoryProgressCursor,
    ) -> Result<TcpFactoryProgressCursor, TcpFactoryProgressWaitError> {
        let Some(cursor_factory) = cursor.factory.upgrade() else {
            return Err(TcpFactoryProgressWaitError::WrongFactory);
        };
        if !Arc::ptr_eq(progress, &cursor_factory) {
            return Err(TcpFactoryProgressWaitError::WrongFactory);
        }
        drop(cursor_factory);

        // Subscribe before inspecting the epoch. A publication between the
        // caller's scan and this wait is therefore either already visible or
        // wakes `changed`; cancelling this receiver consumes no shared state.
        let mut changed = progress.epoch.subscribe();
        loop {
            let observed = *changed.borrow_and_update();
            if observed != cursor.epoch {
                return Ok(TcpFactoryProgressCursor {
                    factory: Arc::downgrade(progress),
                    epoch: observed,
                });
            }
            changed
                .changed()
                .await
                .map_err(|_| TcpFactoryProgressWaitError::ProgressSourceClosed)?;
        }
    }
}

/// Sole non-cloneable authority that seals every clone of one session's TCP
/// factory against later flow admission. Existing ports remain owned by their
/// drivers and must be drained or terminalized by the session root.
pub(super) struct TcpFactoryAdmission {
    flow_gate: Arc<Mutex<FactoryFlowGate>>,
}

impl TcpFactoryAdmission {
    pub(super) fn close(&mut self) -> TcpFactoryAdmissionClose {
        self.flow_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .close()
    }
}

impl Drop for TcpFactoryAdmission {
    fn drop(&mut self) {
        let _ = self.close();
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
        validate_uplink_byte_capacity(capacity)?;
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
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TcpPortFactoryCapacityWitness {
    flow_port: FlowPortConfig,
    bound_direction: Option<Direction>,
    session_uplink_byte_capacity: usize,
    per_flow_uplink_segment_capacity: usize,
    session_uplink_segment_capacity: usize,
    fixed_reset_slots: usize,
    fixed_terminal_slots: usize,
    per_flow_channel_slots: usize,
    max_single_data_payload_bytes: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct TcpPortFactoryCapacityLayout {
    flow_port: FlowPortConfig,
    session_uplink_byte_capacity: usize,
    per_flow_uplink_segment_capacity: usize,
    session_uplink_segment_capacity: usize,
    fixed_reset_slots: usize,
    fixed_terminal_slots: usize,
    per_flow_channel_slots: usize,
    max_single_data_payload_bytes: usize,
}

impl TcpPortFactoryCapacityLayout {
    fn checked(
        flow_port: FlowPortConfig,
        session_uplink_byte_capacity: usize,
        per_flow_uplink_segment_capacity: usize,
        session_uplink_segment_capacity: usize,
    ) -> Result<Self, FlowPortConfigError> {
        let per_flow_channel_slots = flow_port
            .uplink_data_messages()
            .checked_add(flow_port.control_messages())
            .and_then(|slots| slots.checked_add(flow_port.downlink_actions()))
            .and_then(|slots| slots.checked_add(TERMINAL_RESET_LANE_CAPACITY))
            .and_then(|slots| slots.checked_add(TERMINAL_ACTION_LANE_CAPACITY))
            .ok_or(FlowPortConfigError::ChannelEnvelopeOverflow)?;
        Ok(Self {
            flow_port,
            session_uplink_byte_capacity,
            per_flow_uplink_segment_capacity,
            session_uplink_segment_capacity,
            fixed_reset_slots: TERMINAL_RESET_LANE_CAPACITY,
            fixed_terminal_slots: TERMINAL_ACTION_LANE_CAPACITY,
            per_flow_channel_slots,
            max_single_data_payload_bytes: flow_port
                .uplink_byte_capacity()
                .min(MAX_DATA_PAYLOAD_BYTES),
        })
    }

    const fn observe(self, bound_direction: Option<Direction>) -> TcpPortFactoryCapacityWitness {
        TcpPortFactoryCapacityWitness {
            flow_port: self.flow_port,
            bound_direction,
            session_uplink_byte_capacity: self.session_uplink_byte_capacity,
            per_flow_uplink_segment_capacity: self.per_flow_uplink_segment_capacity,
            session_uplink_segment_capacity: self.session_uplink_segment_capacity,
            fixed_reset_slots: self.fixed_reset_slots,
            fixed_terminal_slots: self.fixed_terminal_slots,
            per_flow_channel_slots: self.per_flow_channel_slots,
            max_single_data_payload_bytes: self.max_single_data_payload_bytes,
        }
    }
}

impl TcpPortFactoryCapacityWitness {
    pub(crate) const fn flow_port(self) -> FlowPortConfig {
        self.flow_port
    }

    pub(crate) const fn bound_direction(self) -> Option<Direction> {
        self.bound_direction
    }

    pub(crate) const fn session_uplink_byte_capacity(self) -> usize {
        self.session_uplink_byte_capacity
    }

    pub(crate) const fn per_flow_uplink_segment_capacity(self) -> usize {
        self.per_flow_uplink_segment_capacity
    }

    pub(crate) const fn session_uplink_segment_capacity(self) -> usize {
        self.session_uplink_segment_capacity
    }

    pub(crate) const fn fixed_reset_slots(self) -> usize {
        self.fixed_reset_slots
    }

    pub(crate) const fn fixed_terminal_slots(self) -> usize {
        self.fixed_terminal_slots
    }

    pub(crate) const fn per_flow_channel_slots(self) -> usize {
        self.per_flow_channel_slots
    }

    pub(crate) const fn max_single_data_payload_bytes(self) -> usize {
        self.max_single_data_payload_bytes
    }
}

#[derive(Clone)]
pub struct ResumableTcpPortFactory {
    config: FlowPortConfig,
    capacity: TcpPortFactoryCapacityLayout,
    session_uplink_ledger: UplinkByteLedger,
    per_flow_uplink_segment_capacity: usize,
    session_uplink_segment_ledger: UplinkSegmentLedger,
    flow_gate: Arc<Mutex<FactoryFlowGate>>,
    progress: Arc<FactoryProgress>,
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
            FactoryFlowGate::unbound(),
            None,
        )
    }

    #[cfg(test)]
    fn new_unbound_with_admission_for_test(
        config: FlowPortConfig,
        session_uplink_byte_capacity: usize,
        per_flow_uplink_segment_capacity: usize,
        session_uplink_segment_capacity: usize,
    ) -> Result<(Self, TcpFactoryAdmission), FlowPortConfigError> {
        let factory = Self::new_internal(
            config,
            session_uplink_byte_capacity,
            per_flow_uplink_segment_capacity,
            session_uplink_segment_capacity,
            FactoryFlowGate::unbound(),
            None,
        )?;
        let admission = TcpFactoryAdmission {
            flow_gate: Arc::clone(&factory.flow_gate),
        };
        Ok((factory, admission))
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

    pub(super) fn capacity_witness(&self) -> TcpPortFactoryCapacityWitness {
        let bound_direction = self
            .flow_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .bound_direction();
        self.capacity.observe(bound_direction)
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

    /// Captures the current shared edge for all flow endpoints and drivers
    /// minted by this exact factory.
    pub(crate) fn progress_cursor(&self) -> TcpFactoryProgressCursor {
        FactoryProgress::cursor(&self.progress)
    }

    /// Waits without consuming any driver input. The returned cursor covers
    /// every publication since `cursor`; dropping this future leaves both the
    /// cursor and the shared progress sequence intact.
    pub(crate) async fn wait_for_progress(
        &self,
        cursor: &TcpFactoryProgressCursor,
    ) -> Result<TcpFactoryProgressCursor, TcpFactoryProgressWaitError> {
        FactoryProgress::wait(&self.progress, cursor).await
    }

    pub fn open_flow(
        &self,
        flow: LocalFlow,
    ) -> Result<(ResumableTcpFlow, ResumableTcpDriver), FlowPortError> {
        self.with_flow_admission(flow, || {
            ResumableTcpFlow::bounded_pair(
                flow,
                self.config,
                self.session_uplink_ledger.clone(),
                self.per_flow_uplink_segment_capacity,
                self.session_uplink_segment_ledger.clone(),
                Arc::clone(&self.progress),
                self.origin.clone(),
            )
        })
    }

    /// Keeps the shared admission gate held through construction of the
    /// admitted value. Session shutdown therefore cannot report the factory
    /// sealed while an earlier admission is still constructing its flow pair.
    fn with_flow_admission<T>(
        &self,
        flow: LocalFlow,
        construct: impl FnOnce() -> T,
    ) -> Result<T, FlowPortError> {
        let mut admission = self
            .flow_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        admission.admit(flow)?;
        Ok(construct())
    }
}

impl fmt::Debug for ResumableTcpPortFactory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResumableTcpPortFactory")
            .field("config", &self.config)
            .field("capacity", &self.capacity_witness())
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
    driver_admission_sealed: AtomicBool,
    reset_admitted: AtomicBool,
    retired: AtomicBool,
    admission_gate: Mutex<()>,
    progress: Arc<FactoryProgress>,
    origin: Option<TcpFactoryOrigin>,
}

impl PortState {
    fn new(
        uplink_byte_capacity: usize,
        session_uplink_ledger: UplinkByteLedger,
        uplink_segment_capacity: usize,
        session_uplink_segment_ledger: UplinkSegmentLedger,
        progress: Arc<FactoryProgress>,
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
            driver_admission_sealed: AtomicBool::new(false),
            reset_admitted: AtomicBool::new(false),
            retired: AtomicBool::new(false),
            admission_gate: Mutex::new(()),
            progress,
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

    fn is_driver_admission_sealed(&self) -> bool {
        self.driver_admission_sealed.load(Ordering::Acquire)
    }

    fn publish_progress(&self) {
        self.progress.publish();
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

struct FactoryProgressGuard {
    progress: Arc<FactoryProgress>,
}

impl FactoryProgressGuard {
    fn new(progress: Arc<FactoryProgress>) -> Self {
        Self { progress }
    }
}

impl Drop for FactoryProgressGuard {
    fn drop(&mut self) {
        self.progress.publish();
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
    // Keep both guards after every channel endpoint so their edges publish
    // only after endpoint drop has released queued capacity and ownership.
    _uplink_progress_on_drop: FactoryProgressGuard,
    _downlink_progress_on_drop: FactoryProgressGuard,
}

impl ResumableTcpFlow {
    fn bounded_pair(
        flow: LocalFlow,
        config: FlowPortConfig,
        session_uplink_ledger: UplinkByteLedger,
        per_flow_uplink_segment_capacity: usize,
        session_uplink_segment_ledger: UplinkSegmentLedger,
        progress: Arc<FactoryProgress>,
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
            Arc::clone(&progress),
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
                _uplink_progress_on_drop: FactoryProgressGuard::new(Arc::clone(&progress)),
                _downlink_progress_on_drop: FactoryProgressGuard::new(Arc::clone(&progress)),
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
                _progress_on_drop: self._uplink_progress_on_drop,
            },
            ResumableTcpDownlink {
                flow: self.flow,
                downlink_rx: self.downlink_rx,
                terminal_rx: self.terminal_rx,
                state: self.state,
                _progress_on_drop: self._downlink_progress_on_drop,
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
    // Must remain last so sender closure precedes the progress edge.
    _progress_on_drop: FactoryProgressGuard,
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
    // Must remain last so queued action release precedes the progress edge.
    _progress_on_drop: FactoryProgressGuard,
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

/// Ownership-preserving sink-admission failure for the session root. The
/// reducer-minted offer and its exact segment vector can be retried after
/// bounded pressure clears; diagnostics deliberately expose neither payload
/// bytes nor segment metadata.
pub(crate) struct SinkDeliveryFailure {
    kind: FlowPortError,
    offer: SinkOffer,
    segments: Vec<TcpDataSegment>,
}

impl SinkDeliveryFailure {
    fn new(kind: FlowPortError, offer: SinkOffer, segments: Vec<TcpDataSegment>) -> Self {
        Self {
            kind,
            offer,
            segments,
        }
    }

    pub(crate) const fn kind(&self) -> FlowPortError {
        self.kind
    }

    pub(crate) fn into_parts(self) -> (SinkOffer, Vec<TcpDataSegment>) {
        (self.offer, self.segments)
    }

    fn into_kind(self) -> FlowPortError {
        self.kind
    }
}

impl fmt::Debug for SinkDeliveryFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SinkDeliveryFailure")
            .field("kind", &self.kind)
            .field("ownership", &"[REDACTED]")
            .finish()
    }
}

/// Ownership-preserving half-close admission failure for the session root.
/// The capability remains available for one exact retry after pressure clears.
pub(crate) struct HalfCloseDeliveryFailure {
    kind: FlowPortError,
    capability: SinkHalfClose,
}

impl HalfCloseDeliveryFailure {
    fn new(kind: FlowPortError, capability: SinkHalfClose) -> Self {
        Self { kind, capability }
    }

    pub(crate) const fn kind(&self) -> FlowPortError {
        self.kind
    }

    pub(crate) const fn into_capability(self) -> SinkHalfClose {
        self.capability
    }

    fn into_kind(self) -> FlowPortError {
        self.kind
    }
}

impl fmt::Debug for HalfCloseDeliveryFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("HalfCloseDeliveryFailure([REDACTED])")
    }
}

/// One linearized disposition from the non-cloneable driver owner. A reset
/// dequeued before sealing remains owned by that caller and is never replaced
/// by a second synthesized reset.
#[must_use = "the root must retain or reduce the returned reset ownership"]
pub(crate) enum TcpDriverLocalAbandonSeal {
    Sealed(ClientControllerDriverInput),
    PriorResetAlreadyDequeued,
    AlreadySealed,
    TerminalRetired,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TcpTerminalDeliveryDisposition {
    Queued,
    ReceiverDetached,
}

impl fmt::Debug for TcpDriverLocalAbandonSeal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sealed(_) => formatter.write_str("TcpDriverLocalAbandonSeal::Sealed([REDACTED])"),
            Self::PriorResetAlreadyDequeued => {
                formatter.write_str("TcpDriverLocalAbandonSeal::PriorResetAlreadyDequeued")
            }
            Self::AlreadySealed => formatter.write_str("TcpDriverLocalAbandonSeal::AlreadySealed"),
            Self::TerminalRetired => {
                formatter.write_str("TcpDriverLocalAbandonSeal::TerminalRetired")
            }
        }
    }
}

impl ResumableTcpDriver {
    pub const fn local_flow(&self) -> LocalFlow {
        self.flow
    }

    /// Read-only root-shutdown witness. Only the non-cloneable driver owner
    /// can perform the seal, but the root needs to distinguish a flow that
    /// still requires its one exact shutdown transaction from one whose reset
    /// ownership is already retained by the reducer/effect pipeline.
    pub(crate) fn local_abandon_is_sealed(&self) -> bool {
        self.state.is_driver_admission_sealed()
    }

    pub fn probe(&self) -> FlowPortProbe {
        FlowPortProbe {
            state: Arc::clone(&self.state),
        }
    }

    /// Permanently seals local source, completion, and reset admission for one
    /// flow and drains every fact that has not entered the reducer. It returns
    /// the queued or synthesized reset input, or identifies the reset already
    /// dequeued by the root. The independent terminal action lane remains open
    /// for the reducer's later `FlowFinished` effect.
    pub(crate) fn seal_for_local_abandon(&mut self) -> TcpDriverLocalAbandonSeal {
        let state = Arc::clone(&self.state);
        let _admission = state.admission_guard();
        if state.is_retired() {
            return TcpDriverLocalAbandonSeal::TerminalRetired;
        }
        if state.is_driver_admission_sealed() {
            return TcpDriverLocalAbandonSeal::AlreadySealed;
        }

        state.driver_admission_sealed.store(true, Ordering::Release);
        self.source_rx.close();
        self.control_rx.close();
        self.reset_rx.close();

        // The reset lane has physical capacity one. Take it before draining
        // ordinary lanes so an already-admitted reason wins over synthesized
        // LocalAbandon regardless of queued DATA/completion pressure.
        let queued_reset = self.reset_rx.try_recv().ok();
        while self.reset_rx.try_recv().is_ok() {}
        while self.source_rx.try_recv().is_ok() {}
        while self.control_rx.try_recv().is_ok() {}
        self.consecutive_controls = 0;
        state.publish_progress();

        if let Some(reset) = queued_reset {
            state.reset_admitted.store(true, Ordering::Release);
            return TcpDriverLocalAbandonSeal::Sealed(
                ClientControllerDriverInput::from_driver_dequeue(DriverInput::Control(
                    reset.into_session_event(),
                )),
            );
        }
        if state.reset_admitted.load(Ordering::Acquire) {
            return TcpDriverLocalAbandonSeal::PriorResetAlreadyDequeued;
        }

        state.reset_admitted.store(true, Ordering::Release);
        TcpDriverLocalAbandonSeal::Sealed(ClientControllerDriverInput::from_driver_dequeue(
            DriverInput::Control(
                LocalReset {
                    flow: self.flow,
                    reason: ResetReason::LocalAbandon,
                }
                .into_session_event(),
            ),
        ))
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

    /// Dequeues one input with provenance that only the client controller may
    /// consume. The opaque wrapper cannot be built from a sibling-created
    /// `DriverInput`, so peer frames cannot enter through the local driver
    /// command seam and bypass current-leg observation.
    pub(super) fn try_recv_next_for_client_controller(
        &mut self,
    ) -> Result<Option<ClientControllerDriverInput>, FlowPortError> {
        self.try_recv_next()
            .map(|input| input.map(ClientControllerDriverInput::from_driver_dequeue))
    }

    /// Queues a typed sink offer only after reserving the exact future
    /// completion slot.  Thus the TUN side can always report acceptance or
    /// abandonment without allocating or waiting.
    pub fn try_deliver_sink(
        &self,
        offer: SinkOffer,
        segments: Vec<TcpDataSegment>,
    ) -> Result<(), FlowPortError> {
        self.try_deliver_sink_exact(offer, segments)
            .map_err(SinkDeliveryFailure::into_kind)
    }

    /// Root-facing ownership-preserving counterpart to
    /// [`Self::try_deliver_sink`]. Every rejection returns the exact input so
    /// the root can retain one bounded deferred effect and retry it unchanged.
    pub(crate) fn try_deliver_sink_exact(
        &self,
        offer: SinkOffer,
        segments: Vec<TcpDataSegment>,
    ) -> Result<(), SinkDeliveryFailure> {
        if let Err(kind) = self.validate_sink_coverage(offer, &segments) {
            return Err(SinkDeliveryFailure::new(kind, offer, segments));
        }
        let _admission = self.state.admission_guard();
        if self.state.is_retired() {
            return Err(SinkDeliveryFailure::new(
                FlowPortError::Closed,
                offer,
                segments,
            ));
        }
        if self.state.is_driver_admission_sealed() {
            return Err(SinkDeliveryFailure::new(
                FlowPortError::DriverAdmissionSealed,
                offer,
                segments,
            ));
        }
        let completion = match self.reserve_completion() {
            Ok(completion) => completion,
            Err(kind) => return Err(SinkDeliveryFailure::new(kind, offer, segments)),
        };
        let action_slot = match reserve_owned(&self.downlink_tx, FlowPortError::DownlinkLaneFull) {
            Ok(action_slot) => action_slot,
            Err(kind) => return Err(SinkDeliveryFailure::new(kind, offer, segments)),
        };
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
        self.try_deliver_half_close_exact(completion_capability)
            .map_err(HalfCloseDeliveryFailure::into_kind)
    }

    /// Root-facing ownership-preserving counterpart to
    /// [`Self::try_deliver_half_close`]. Every rejection returns the exact
    /// reducer-minted completion capability.
    pub(crate) fn try_deliver_half_close_exact(
        &self,
        completion_capability: SinkHalfClose,
    ) -> Result<(), HalfCloseDeliveryFailure> {
        if let Err(kind) = self.validate_half_close(completion_capability) {
            return Err(HalfCloseDeliveryFailure::new(kind, completion_capability));
        }
        let _admission = self.state.admission_guard();
        if self.state.is_retired() {
            return Err(HalfCloseDeliveryFailure::new(
                FlowPortError::Closed,
                completion_capability,
            ));
        }
        if self.state.is_driver_admission_sealed() {
            return Err(HalfCloseDeliveryFailure::new(
                FlowPortError::DriverAdmissionSealed,
                completion_capability,
            ));
        }
        let completion = match self.reserve_completion() {
            Ok(completion) => completion,
            Err(kind) => {
                return Err(HalfCloseDeliveryFailure::new(kind, completion_capability));
            }
        };
        let action_slot = match reserve_owned(&self.downlink_tx, FlowPortError::DownlinkLaneFull) {
            Ok(action_slot) => action_slot,
            Err(kind) => {
                return Err(HalfCloseDeliveryFailure::new(kind, completion_capability));
            }
        };
        let guard = ActionGuard::new(Arc::clone(&self.state));
        let _sender = action_slot.send(TunAction::HalfClose(HalfCloseDelivery {
            capability: completion_capability,
            completion: Some(completion),
            _guard: guard,
        }));
        Ok(())
    }

    fn validate_half_close(
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
        Ok(())
    }

    /// Forwards only exact terminal effects already emitted by the reducer.
    /// This adapter deliberately cannot synthesize a new reset/finish reason.
    pub fn try_deliver_terminal_effect(
        &mut self,
        effect: &SessionEffect,
    ) -> Result<TcpTerminalDeliveryDisposition, FlowPortError> {
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
    ) -> Result<TcpTerminalDeliveryDisposition, FlowPortError> {
        if flow_id != self.flow.flow_id() {
            return Err(FlowPortError::CrossFlowTerminalEffect);
        }
        let state = Arc::clone(&self.state);
        let _admission = state.admission_guard();
        if state.is_retired() {
            return Err(FlowPortError::TerminalAlreadyDelivered);
        }
        let action_slot = match reserve_owned(&self.terminal_tx, FlowPortError::DownlinkLaneFull) {
            Ok(action_slot) => Some(action_slot),
            // The endpoint may already be gone while a SinkDelivery or
            // HalfCloseDelivery lives in SocketCtx. Reducer terminal remains
            // the revocation point: retire under the same admission lock even
            // though there is no receiver for the terminal notification.
            Err(FlowPortError::Closed) => None,
            Err(error) => return Err(error),
        };
        state.retired.store(true, Ordering::Release);
        self.reset_rx.close();
        self.control_rx.close();
        self.source_rx.close();
        while self.reset_rx.try_recv().is_ok() {}
        while self.control_rx.try_recv().is_ok() {}
        while self.source_rx.try_recv().is_ok() {}
        let Some(action_slot) = action_slot else {
            state.publish_progress();
            return Ok(TcpTerminalDeliveryDisposition::ReceiverDetached);
        };
        let guard = ActionGuard::new(Arc::clone(&self.state));
        let _sender = action_slot.send(TunAction::Terminal(TerminalDelivery {
            flow: self.flow,
            action,
            _guard: guard,
        }));
        Ok(TcpTerminalDeliveryDisposition::Queued)
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

/// Opaque proof that an input came from the exact bounded TCP driver lanes.
/// Its tuple field is private to this module, and the type is deliberately
/// neither `Clone` nor publicly re-exported.
pub(crate) struct ClientControllerDriverInput(Box<DriverInput>);

impl ClientControllerDriverInput {
    fn from_driver_dequeue(input: DriverInput) -> Self {
        Self(Box::new(input))
    }

    pub(super) fn as_inner(&self) -> &DriverInput {
        &self.0
    }

    pub(super) fn into_inner(self, _gate: &ClientControllerDriverReducerGate) -> DriverInput {
        *self.0
    }

    /// Restores the opaque typed boundary only from a reducer rejection that
    /// returned the exact driver command. Only the supervisor can mint the
    /// reducer gate, so sibling adapters cannot wrap an arbitrary raw input.
    pub(super) fn from_rejected_driver(
        input: DriverInput,
        _gate: &ClientControllerDriverReducerGate,
    ) -> Self {
        Self(Box::new(input))
    }
}

impl fmt::Debug for ClientControllerDriverInput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ClientControllerDriverInput([REDACTED])")
    }
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
        self.bind_owner_replay(&stored)
    }
    /// Owner keeps the original receipt journal-held on every rejection.
    /// The legacy by-value entry above shares these exact checks and commit.
    pub(super) fn bind_owner_replay(
        &mut self,
        stored: &ReplayStored,
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

fn drain_nonterminal_actions(actions: &mut mpsc::Receiver<TunAction>, state: &Arc<PortState>) {
    actions.close();
    while let Ok(action) = actions.try_recv() {
        state.publish_progress();
        drop(action);
    }
}

fn try_recv_tun_action(
    terminal: &mut mpsc::Receiver<TunAction>,
    actions: &mut mpsc::Receiver<TunAction>,
    state: &Arc<PortState>,
) -> Result<Option<TunAction>, FlowPortError> {
    match terminal.try_recv() {
        Ok(action) => {
            state.publish_progress();
            drain_nonterminal_actions(actions, state);
            return Ok(Some(action));
        }
        Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected) => {}
    }
    if state.is_retired() || state.is_driver_admission_sealed() {
        drain_nonterminal_actions(actions, state);
        return if terminal.is_closed() {
            Err(FlowPortError::Closed)
        } else {
            Ok(None)
        };
    }
    match actions.try_recv() {
        Ok(action) => {
            state.publish_progress();
            Ok(Some(action))
        }
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
        if state.is_retired() || state.is_driver_admission_sealed() {
            let action = terminal.recv().await;
            if action.is_some() {
                state.publish_progress();
            }
            drain_nonterminal_actions(actions, state);
            return action;
        }
        tokio::select! {
            biased;
            action = terminal.recv() => {
                if let Some(action) = action {
                    state.publish_progress();
                    drain_nonterminal_actions(actions, state);
                    return Some(action);
                }
            },
            action = actions.recv() => match action {
                Some(action)
                    if !state.is_retired() && !state.is_driver_admission_sealed() => {
                    state.publish_progress();
                    return Some(action);
                },
                Some(action) => {
                    state.publish_progress();
                    drop(action);
                },
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
            if !self.state.is_retired() && !self.state.is_driver_admission_sealed() {
                let _sender = permit.send(control);
            }
        }
        self.state.publish_progress();
    }
}

impl Drop for CompletionReservation {
    fn drop(&mut self) {
        if let Some(permit) = self.permit.take() {
            drop(permit);
            self.state
                .reserved_completion_slots
                .fetch_sub(1, Ordering::AcqRel);
            self.state.publish_progress();
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
        !self.state.is_retired() && !self.state.is_driver_admission_sealed()
    }

    fn with_live<R>(&self, apply: impl FnOnce() -> R) -> Result<R, FlowPortError> {
        let _admission = self.state.admission_guard();
        if self.state.is_retired() || self.state.is_driver_admission_sealed() {
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
        self.state.publish_progress();
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
    // Must remain last: Rust drops struct fields in declaration order.
    _progress_on_drop: FactoryProgressGuard,
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
    // Must remain last so every byte/segment permit is released first.
    _progress_on_drop: FactoryProgressGuard,
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
        if state.is_driver_admission_sealed() {
            return Err(FlowPortError::DriverAdmissionSealed);
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
        if state.is_driver_admission_sealed() {
            return Err(FlowPortError::DriverAdmissionSealed);
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
                _progress_on_drop: FactoryProgressGuard::new(Arc::clone(&state.progress)),
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

    // This guard is declared before any queue or semaphore acquisition and is
    // therefore dropped last on every partial-reservation error. On success it
    // moves into the returned reservation and publishes only when that exact
    // ownership is committed or rolled back.
    let progress_on_rollback = FactoryProgressGuard::new(Arc::clone(&state.progress));

    let _admission = state.admission_guard();
    if state.is_retired() {
        return Err(FlowPortError::Closed);
    }
    if state.is_driver_admission_sealed() {
        return Err(FlowPortError::DriverAdmissionSealed);
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
        _progress_on_drop: progress_on_rollback,
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
    if state.is_driver_admission_sealed() {
        return Err(FlowPortError::DriverAdmissionSealed);
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
    state.publish_progress();
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
    if state.is_driver_admission_sealed() {
        return Err(FlowPortError::DriverAdmissionSealed);
    }
    if state.reset_admitted.load(Ordering::Acquire) {
        return Err(FlowPortError::TerminalLaneFull);
    }
    sender
        .try_send(LocalReset { flow, reason })
        .map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => FlowPortError::TerminalLaneFull,
            mpsc::error::TrySendError::Closed(_) => FlowPortError::Closed,
        })?;
    // Keep the logical once-only admission bit in the same critical section
    // as physical publication. A concurrent seal can therefore observe
    // either a queued reset or one already owned by the root, never a gap in
    // which it could synthesize a second terminal fact.
    state.reset_admitted.store(true, Ordering::Release);
    state.publish_progress();
    Ok(())
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

fn validate_uplink_byte_capacity(capacity: usize) -> Result<(), FlowPortConfigError> {
    if capacity == 0 {
        return Err(FlowPortConfigError::ZeroUplinkByteCapacity);
    }
    if capacity > MAX_EXACT_UPLINK_BYTE_CAPACITY {
        return Err(FlowPortConfigError::UplinkByteCapacityTooLarge {
            value: capacity,
            max: MAX_EXACT_UPLINK_BYTE_CAPACITY,
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
mod tests;
