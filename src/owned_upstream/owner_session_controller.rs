//! Sole Target/progress/clock owner: folds reducer facts before exposing outputs.

mod capacity;
mod cold_inventory;
mod construction;
mod ready_service;
mod terminal_grace;
use super::leg::{
    BoundInbound, EstablishedLeg, ExactLegTerminal, LegBoundFrame, LegTransportTerminalReason,
};
use super::leg_io::{
    LegControlEnqueued, LegIo, LegOutboundQueue, LegOutboundQueueErrorKind,
    PhysicalLegRuntimeIncarnation,
};
use super::owner_target::{
    EnqueuedOwnerAttachStatus, EnqueuedOwnerTargetAttach, OwnerLeglessHighWater,
    OwnerTargetAttachPublication, OwnerTargetError, OwnerTargetExecutor,
    OwnerTargetInitialAcceptanceSettlement, OwnerTargetInitialHandoff,
    OwnerTargetInitialHandoffOwnerFailure, OwnerTargetInitialRetirementErrorKind,
    OwnerTargetInputBlock, OwnerTargetObservedApply, OwnerTargetObservedRecoveryPage,
    OwnerTargetOutput, OwnerTargetRecoveryHandoff, OwnerTargetRecoveryOutput,
    OwnerTargetRecoveryPublishCursor, OwnerTargetRecoveryResume, OwnerTargetReducerObservations,
    OwnerTargetResume, OwnerTargetRetiredInitialHandoff, OwnerTargetRetiredInitialSettled,
    OwnerTargetRetiredInitialSettlementErrorKind, OwnerTerminalAttachDiscardFailure,
    PendingOwnerAttachLoss, PendingOwnerAttachStatusPublication,
    PendingOwnerTargetAttachPublication, QuarantinedOwnerAttachLoss,
};
use super::quinn_leg_actor::QuinnOwnerActorArrival;
use super::quinn_leg_composition::{QuinnLegCompositionGate, QuinnOwnerContinuationPermit};
use super::reverse_switch::{
    AdmittedOwnerSwitchHint, OwnerHintAdmissionError, OwnerHintDueCursor, OwnerHintError,
    OwnerHintPreparation, OwnerProgressCapacity, OwnerProgressError, OwnerReverseDirectionSnapshot,
    OwnerReverseProgress, OwnerSwitchHintIssuer,
};
use super::session_clock::{SessionClockError, SessionMonotonicClock};
use super::standby::{ExactAuthenticatedStandby, OwnerStandbySlot};
use super::target::TargetIo;
use super::two_leg::SimTime;
use crate::resumable::{
    Direction, LegGeneration, ProtocolError, SessionEvent, SessionFlowId, TerminalGrace,
};
pub(crate) use capacity::{
    OwnerSessionCapacityError, OwnerSessionCapacityPlan, OwnerSessionCapacityWitness,
};
use std::fmt;
use std::time::Duration;
use terminal_grace::{OwnerTerminalGraceState, TerminalGraceError};
use thiserror::Error;
/// One actor-decoded owner event inseparably paired with the immutable
/// endpoint-clock arrival witness. Only closed Quinn composition may build a
/// production value; no controller API accepts `(SessionEvent, SimTime)`.
pub(super) struct OwnerSessionActorEvent {
    event: Box<SessionEvent>,
    arrival: QuinnOwnerActorArrival,
}

impl OwnerSessionActorEvent {
    #[allow(
        dead_code,
        reason = "the physical owner Quinn composition slice will call this gated constructor"
    )]
    pub(super) fn from_quinn_actor(
        event: SessionEvent,
        arrival: QuinnOwnerActorArrival,
        _gate: &QuinnLegCompositionGate,
    ) -> Self {
        Self {
            event: Box::new(event),
            arrival,
        }
    }

    #[cfg(test)]
    pub(in crate::owned_upstream) fn capture_for_test(
        event: SessionEvent,
        clock: &SessionMonotonicClock,
    ) -> Result<Self, SessionClockError> {
        Ok(Self {
            event: Box::new(event),
            arrival: QuinnOwnerActorArrival::capture_for_test(clock)?,
        })
    }
}

impl fmt::Debug for OwnerSessionActorEvent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OwnerSessionActorEvent([REDACTED])")
    }
}
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub(super) enum OwnerSessionControllerBuildError {
    #[error("owner controller capacity rejected the installed authorities: {0}")]
    Capacity(OwnerSessionCapacityError),
    #[error("owner controller ready binding rejected the installed authorities: {0:?}")]
    Ready(super::owner_target::OwnerReadyInstallError),
    #[error("owner terminal grace preparation failed: {0}")]
    TerminalGrace(TerminalGraceError),
}
/// Checked constructor failure returns every unique input authority unchanged.
pub(super) struct OwnerSessionControllerBuildFailure<'r, T> {
    terminal_grace: Duration,
    clock: SessionMonotonicClock,
    executor: OwnerTargetExecutor<T>,
    plan: OwnerSessionCapacityPlan,
    ready: super::owner_ready::OwnerReadySessionReservation<'r>,
    source: OwnerSessionControllerBuildError,
}
/// Test-only compatibility constructor failure. Production builds expose only
/// the checked constructor above, so no controller can omit storage authority.
#[cfg(test)]
struct LegacyOwnerSessionControllerBuildFailure<T> {
    clock: SessionMonotonicClock,
    executor: OwnerTargetExecutor<T>,
    source: OwnerProgressError,
}
#[cfg(test)]
impl<T> LegacyOwnerSessionControllerBuildFailure<T> {
    fn into_parts(
        self,
    ) -> (
        SessionMonotonicClock,
        OwnerTargetExecutor<T>,
        OwnerProgressError,
    ) {
        (self.clock, self.executor, self.source)
    }
}
#[cfg(test)]
impl<T> fmt::Debug for LegacyOwnerSessionControllerBuildFailure<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LegacyOwnerSessionControllerBuildFailure")
            .field("clock", &"[REDACTED]")
            .field("executor", &"[REDACTED]")
            .field("source", &self.source)
            .finish()
    }
}

/// Checked storage bound for one ordinary controller output page.
///
/// `OwnerTargetExecutor::execute_queue_observed` performs at most one output-
/// producing action per configured effect step. Its optional `NeedsResume`
/// tail is removed by this controller and retained as a separate capability,
/// so it is not part of `max_outputs`. Every currently encodable `Frame`
/// variant is no larger than the maximum DATA frame: OPEN is bounded by the
/// protocol domain limit and all other records have fixed bodies smaller than
/// DATA's 64-KiB payload ceiling.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct OwnerOutputPageCapacity {
    max_outputs: usize,
    max_encoded_bytes: usize,
}

impl OwnerOutputPageCapacity {
    fn checked_from_max_outputs(max_outputs: usize) -> Result<Self, OwnerOutputPageCapacityError> {
        let max_frame_encoded_bytes = crate::resumable::MAX_ENCODED_DATA_FRAME_BYTES;
        let max_encoded_bytes = max_outputs.checked_mul(max_frame_encoded_bytes).ok_or(
            OwnerOutputPageCapacityError::EncodedByteCapacityOverflow {
                max_outputs,
                max_frame_encoded_bytes,
            },
        )?;
        Ok(Self {
            max_outputs,
            max_encoded_bytes,
        })
    }

    pub(crate) const fn max_outputs(self) -> usize {
        self.max_outputs
    }

    pub(crate) const fn max_encoded_bytes(self) -> usize {
        self.max_encoded_bytes
    }
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub(crate) enum OwnerOutputPageCapacityError {
    #[error(
        "owner output byte capacity overflowed for {max_outputs} frames of at most {max_frame_encoded_bytes} encoded bytes"
    )]
    EncodedByteCapacityOverflow {
        max_outputs: usize,
        max_frame_encoded_bytes: usize,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OwnerSessionControllerPhase {
    Active,
    SessionExpiring,
    SessionExpired,
    Failed,
}

struct AbortedOwnerControllerTurn {
    outputs: Vec<OwnerTargetOutput>,
    target_error: Option<OwnerTargetError>,
    observations: OwnerTargetReducerObservations,
    continuation: Option<OwnerTargetResume>,
    /// Exact origin of this page's outputs, facts, continuation and Target error.
    page_origin: OwnerContinuationOrigin,
    stored_output_count: usize,
    stored_frame_bytes: StoredOwnerControllerFrameBytes,
}

struct PendingOwnerContinuation {
    resume: OwnerTargetResume,
    origin: OwnerContinuationOrigin,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OwnerContinuationOrigin {
    Actor(SimTime),
    Local,
}

impl OwnerContinuationOrigin {
    const fn actor_arrived_at(self) -> Option<SimTime> {
        match self {
            Self::Actor(arrived_at) => Some(arrived_at),
            Self::Local => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum StoredOwnerControllerFrameBytes {
    Exact(usize),
    InvalidFrame(ProtocolError),
    SumOverflow,
}

enum OwnerControllerFrameByteAuditError {
    InvalidFrame(ProtocolError),
    SumOverflow,
}

impl OwnerControllerFrameByteAuditError {
    fn into_cold_inventory_error(self) -> OwnerSessionControllerColdInventoryError {
        match self {
            Self::InvalidFrame(source) => {
                OwnerSessionControllerColdInventoryError::InvalidRetainedFrame(source)
            }
            Self::SumOverflow => OwnerSessionControllerColdInventoryError::FrameByteSumOverflow,
        }
    }
}

impl StoredOwnerControllerFrameBytes {
    fn cold_inventory_result(&self) -> Result<usize, OwnerSessionControllerColdInventoryError> {
        match self {
            Self::Exact(bytes) => Ok(*bytes),
            Self::InvalidFrame(source) => {
                Err(OwnerSessionControllerColdInventoryError::StoredFrameInvalid(source.clone()))
            }
            Self::SumOverflow => {
                Err(OwnerSessionControllerColdInventoryError::StoredFrameByteSumOverflow)
            }
        }
    }
}

impl AbortedOwnerControllerTurn {
    fn new(
        outputs: Vec<OwnerTargetOutput>,
        observations: OwnerTargetReducerObservations,
        continuation: Option<OwnerTargetResume>,
        page_origin: OwnerContinuationOrigin,
    ) -> Self {
        let stored_output_count = Self::retained_output_count_in(&outputs);
        let stored_frame_bytes = match Self::sum_frame_bytes(&outputs) {
            Ok(bytes) => StoredOwnerControllerFrameBytes::Exact(bytes),
            Err(OwnerControllerFrameByteAuditError::InvalidFrame(source)) => {
                StoredOwnerControllerFrameBytes::InvalidFrame(source)
            }
            Err(OwnerControllerFrameByteAuditError::SumOverflow) => {
                StoredOwnerControllerFrameBytes::SumOverflow
            }
        };
        Self {
            outputs,
            target_error: None,
            observations,
            continuation,
            page_origin,
            stored_output_count,
            stored_frame_bytes,
        }
    }

    fn retained_output_count_in(outputs: &[OwnerTargetOutput]) -> usize {
        outputs
            .iter()
            .filter(|output| !matches!(output, OwnerTargetOutput::NeedsResume { .. }))
            .count()
    }

    /// Counts ordinary publication outputs without counting malformed raw
    /// continuation capabilities a second time. This is a cold snapshot scan
    /// over the already-bounded, immutable aborted page.
    fn retained_output_count(&self) -> usize {
        Self::retained_output_count_in(&self.outputs)
    }

    /// Includes every `NeedsResume` hidden anywhere in a malformed page plus
    /// the normally extracted tail capability. The original page stays
    /// intact, so this accounting adds no allocation, reserve, or unwind
    /// window on the invariant-failure path.
    fn retained_continuation_count(&self) -> usize {
        usize::from(self.continuation.is_some())
            + self
                .outputs
                .iter()
                .filter(|output| matches!(output, OwnerTargetOutput::NeedsResume { .. }))
                .count()
    }

    fn sum_frame_bytes(
        outputs: &[OwnerTargetOutput],
    ) -> Result<usize, OwnerControllerFrameByteAuditError> {
        Self::sum_checked_frame_lengths(outputs.iter().filter_map(|output| {
            let OwnerTargetOutput::Transmit(frame) = output else {
                return None;
            };
            Some(frame.encoded_len_checked())
        }))
    }

    fn sum_checked_frame_lengths(
        lengths: impl IntoIterator<Item = Result<usize, ProtocolError>>,
    ) -> Result<usize, OwnerControllerFrameByteAuditError> {
        let mut summed = 0_usize;
        for encoded in lengths {
            let encoded = encoded.map_err(OwnerControllerFrameByteAuditError::InvalidFrame)?;
            summed = summed
                .checked_add(encoded)
                .ok_or(OwnerControllerFrameByteAuditError::SumOverflow)?;
        }
        Ok(summed)
    }
}

enum PendingOwnerAttachedRecovery {
    Acceptance(PendingOwnerTargetAttachPublication),
    Initial(EnqueuedOwnerTargetAttach),
    Execute(OwnerTargetRecoveryResume),
    Publish(OwnerTargetRecoveryPublishCursor),
    HandoffReady(OwnerTargetRecoveryHandoff),
    TerminalBound(PendingOwnerAttachLoss),
}

impl PendingOwnerAttachedRecovery {
    fn retained_output_count(&self) -> usize {
        match self {
            Self::Publish(publish) => publish.retained_output_count(),
            Self::Acceptance(_)
            | Self::Initial(_)
            | Self::Execute(_)
            | Self::HandoffReady(_)
            | Self::TerminalBound(_) => 0,
        }
    }
}

struct PendingOwnerLeglessHighWater {
    high_water: OwnerLeglessHighWater,
}

enum PendingOwnerAttachStatus {
    Enqueue(PendingOwnerAttachStatusPublication),
    Delivery(EnqueuedOwnerAttachStatus),
}

/// Value-only result of one physical registered owner ATTACH transaction.
/// All acceptance, status, recovery, and AttachedLeg capabilities remain in
/// the controller or the exact actor FIFO.
pub(crate) enum OwnerSessionRegisteredAttachDrive {
    Recovery(OwnerSessionRecoveryDrive),
    AcceptancePending { kind: LegOutboundQueueErrorKind },
    StatusQueued,
    StatusPending { kind: LegOutboundQueueErrorKind },
}

/// Value-only installed identity plus the unique retired-A authority produced
/// by the same synchronous B commit.
#[must_use = "installed recovery returns the exact retired initial handoff"]
pub(super) struct OwnerSessionInstalledRecoveryWithRetiredInitial {
    generation: LegGeneration,
    retired_a: OwnerTargetRetiredInitialHandoff,
}

impl OwnerSessionInstalledRecoveryWithRetiredInitial {
    pub(super) const fn generation(&self) -> LegGeneration {
        self.generation
    }

    pub(super) fn into_retired_a(self) -> OwnerTargetRetiredInitialHandoff {
        self.retired_a
    }
}

/// B is not yet handoff-ready, so the exact initial A is returned unchanged.
pub(super) enum OwnerSessionRecoveryInstallRetirementDrive {
    Inert(OwnerTargetInitialHandoff),
    Installed(OwnerSessionInstalledRecoveryWithRetiredInitial),
}

/// Every rejected install keeps B in the controller's HandoffReady state and
/// returns A to the physical root for an exact retry.
#[must_use = "failed recovery install returns the exact initial-A handoff"]
pub(super) struct OwnerSessionRecoveryInstallRetirementFailure {
    error: OwnerSessionControllerError,
    initial_a: OwnerTargetInitialHandoff,
}

impl OwnerSessionRecoveryInstallRetirementFailure {
    pub(super) fn into_parts(self) -> (OwnerSessionControllerError, OwnerTargetInitialHandoff) {
        (self.error, self.initial_a)
    }
}

impl fmt::Debug for OwnerSessionRecoveryInstallRetirementFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OwnerSessionRecoveryInstallRetirementFailure")
            .field("error", &self.error)
            .field("initial_a", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OwnerSessionRetiredInitialClearErrorKind {
    MissingActiveB,
    Target(OwnerTargetRetiredInitialSettlementErrorKind),
}

/// Exact joined-retirement rejection.  The outer actor transaction restores
/// its terminal while the root restores `retired_a` in the same phase.
#[must_use = "failed retired-A clear returns terminal and handoff ownership"]
pub(super) struct OwnerSessionRetiredInitialClearFailure {
    kind: OwnerSessionRetiredInitialClearErrorKind,
    retired_a: OwnerTargetRetiredInitialHandoff,
    terminal: Option<ExactLegTerminal>,
}

impl OwnerSessionRetiredInitialClearFailure {
    pub(super) const fn kind(&self) -> OwnerSessionRetiredInitialClearErrorKind {
        self.kind
    }

    pub(super) fn into_parts(self) -> (OwnerTargetRetiredInitialHandoff, Option<ExactLegTerminal>) {
        (self.retired_a, self.terminal)
    }
}

impl fmt::Debug for OwnerSessionRetiredInitialClearFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OwnerSessionRetiredInitialClearFailure")
            .field("kind", &self.kind)
            .field("retired_a", &"[REDACTED]")
            .field("terminal", &"[REDACTED]")
            .finish()
    }
}

/// One controller-clock turn of the aggregate owner ACK-stall producer.
///
/// The due cursor, reducer entry, physical B binding and pending frame never
/// cross this interface.  A successful admission is the sole exception: its
/// exact queue receipt moves to the future physical owner root for retention
/// until ordered delivery or shutdown settlement.
pub(super) enum OwnerSessionHintDrive {
    Scanning,
    Inert,
    Pending {
        deadline: SimTime,
    },
    Admitted {
        preparation: OwnerHintPreparation,
        hint: AdmittedOwnerSwitchHint<LegControlEnqueued>,
    },
}

impl fmt::Debug for OwnerSessionHintDrive {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Scanning => formatter.write_str("Scanning"),
            Self::Inert => formatter.write_str("Inert"),
            Self::Pending { deadline } => formatter
                .debug_struct("Pending")
                .field("deadline", deadline)
                .finish(),
            Self::Admitted { preparation, hint } => formatter
                .debug_struct("Admitted")
                .field("preparation", preparation)
                .field("hint_sequence", &hint.hint_sequence())
                .field("receipt", &"[REDACTED]")
                .finish(),
        }
    }
}

#[derive(Debug)]
pub(super) enum OwnerSessionHintDriveError {
    Inactive,
    Clock(SessionClockError),
    Policy(OwnerHintError),
    Admission(OwnerHintAdmissionError),
}

impl OwnerSessionHintDriveError {
    pub(super) const fn is_queue_pressure(&self) -> bool {
        matches!(
            self,
            Self::Admission(OwnerHintAdmissionError::QueueRejected(_))
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum OwnerRegisteredRecoveryPreflightBlock {
    Inactive,
    ContinuationPending,
    RecoveryPending,
    StatusPending,
    Quarantined,
    Target(OwnerTargetInputBlock),
}

/// Non-cloneable vacancy minted only while the controller and Target admit
/// one registered ATTACH. The actor carries it from immutable preflight into
/// the immediately following commit under one first-failure lock.
pub(super) struct OwnerRegisteredRecoveryReady {
    _private: (),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum OwnerInitialStandbyRegistrationBlock {
    Inactive,
    ContinuationPending,
    RecoveryPending,
    StatusPending,
    Quarantined,
    Target(OwnerTargetInputBlock),
}

/// Non-cloneable vacancy proving that the sole controller and settled
/// initial-A handoff admit one B registration in the current synchronous
/// actor transaction.
pub(super) struct OwnerInitialStandbyRegistrationReady {
    _private: (),
}

struct AbortedOwnerRecoveryTurn {
    pending: Option<PendingOwnerAttachedRecovery>,
    outputs: Vec<OwnerTargetRecoveryOutput>,
    observations: Option<OwnerTargetReducerObservations>,
}

/// Capability-free lifecycle output from one attached-recovery publication.
/// Raw continuation and AttachedLeg authorities stay inside this Module.
pub(crate) enum OwnerSessionRecoveryOutput {
    LegActivated { generation: LegGeneration },
    ResumeGraceStarted { generation: LegGeneration },
    TerminalGraceStarted { terminal: TerminalGrace },
    SessionExpired,
}

impl fmt::Debug for OwnerSessionRecoveryOutput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LegActivated { generation } => formatter
                .debug_struct("LegActivated")
                .field("generation", generation)
                .finish(),
            Self::ResumeGraceStarted { generation } => formatter
                .debug_struct("ResumeGraceStarted")
                .field("generation", generation)
                .finish(),
            Self::TerminalGraceStarted { terminal } => formatter
                .debug_struct("TerminalGraceStarted")
                .field("terminal", terminal)
                .finish(),
            Self::SessionExpired => formatter.write_str("SessionExpired"),
        }
    }
}

/// Value-only scheduling result for attached recovery. Physical composition
/// can extract a completed AttachedLeg only while borrowing its private gate.
#[must_use = "attached recovery may require an exact execute or publish turn"]
pub(crate) struct OwnerSessionRecoveryDrive {
    outputs: Vec<OwnerSessionRecoveryOutput>,
    needs_execute: bool,
    needs_publish: bool,
    handoff_ready: bool,
}

impl OwnerSessionRecoveryDrive {
    pub(crate) fn outputs(&self) -> &[OwnerSessionRecoveryOutput] {
        &self.outputs
    }

    pub(crate) const fn needs_execute(&self) -> bool {
        self.needs_execute
    }

    pub(crate) const fn needs_publish(&self) -> bool {
        self.needs_publish
    }

    pub(crate) const fn handoff_ready(&self) -> bool {
        self.handoff_ready
    }
}

impl fmt::Debug for OwnerSessionRecoveryDrive {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OwnerSessionRecoveryDrive")
            .field("outputs", &self.outputs.len())
            .field("needs_execute", &self.needs_execute)
            .field("needs_publish", &self.needs_publish)
            .field("handoff_ready", &self.handoff_ready)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct OwnerSessionRecoveryTerminalBound {
    generation: LegGeneration,
    reason: LegTransportTerminalReason,
}

impl OwnerSessionRecoveryTerminalBound {
    pub(crate) const fn generation(self) -> LegGeneration {
        self.generation
    }

    pub(crate) const fn reason(self) -> LegTransportTerminalReason {
        self.reason
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OwnerSessionRecoveryTerminalBindErrorKind {
    NoPendingRecovery,
    TerminalAlreadyBound,
    LeglessHighWaterPending,
    WrongLegTerminal,
    Rejected(OwnerTargetInputBlock),
}

/// Pre-bind failure returns the exact terminal to the actor transaction. The
/// controller restores the complete recovery typestate before constructing
/// this value, so retry cannot skip or duplicate a recovery phase.
pub(super) struct OwnerSessionRecoveryTerminalBindFailure {
    kind: OwnerSessionRecoveryTerminalBindErrorKind,
    terminal: ExactLegTerminal,
}

impl OwnerSessionRecoveryTerminalBindFailure {
    pub(super) const fn kind(&self) -> OwnerSessionRecoveryTerminalBindErrorKind {
        self.kind
    }

    pub(super) fn into_terminal(self) -> ExactLegTerminal {
        self.terminal
    }
}

impl fmt::Debug for OwnerSessionRecoveryTerminalBindFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.kind {
            OwnerSessionRecoveryTerminalBindErrorKind::NoPendingRecovery => {
                formatter.write_str("NoPendingRecovery")
            }
            OwnerSessionRecoveryTerminalBindErrorKind::TerminalAlreadyBound => {
                formatter.write_str("TerminalAlreadyBound")
            }
            OwnerSessionRecoveryTerminalBindErrorKind::LeglessHighWaterPending => {
                formatter.write_str("LeglessHighWaterPending")
            }
            OwnerSessionRecoveryTerminalBindErrorKind::WrongLegTerminal => {
                formatter.write_str("WrongLegTerminal")
            }
            OwnerSessionRecoveryTerminalBindErrorKind::Rejected(block) => formatter
                .debug_tuple("RejectedRecoveryTerminal")
                .field(&block)
                .finish(),
        }
    }
}

pub(crate) struct OwnerSessionRecoveryTerminalDiscarded {
    identity: OwnerSessionRecoveryTerminalBound,
}

impl OwnerSessionRecoveryTerminalDiscarded {
    pub(crate) const fn generation(&self) -> LegGeneration {
        self.identity.generation
    }

    pub(crate) const fn reason(&self) -> LegTransportTerminalReason {
        self.identity.reason
    }
}

/// One bounded owner execution page. The raw executor resume capability never
/// crosses this Interface; callers receive only a value-only scheduling fact.
#[must_use = "owner session pages may require one exact continuation turn"]
pub(crate) struct OwnerSessionDrive {
    outputs: Vec<OwnerTargetOutput>,
    needs_continue: bool,
}

impl OwnerSessionDrive {
    pub(crate) fn outputs(&self) -> &[OwnerTargetOutput] {
        &self.outputs
    }

    pub(crate) fn into_outputs(self) -> Vec<OwnerTargetOutput> {
        self.outputs
    }

    pub(crate) const fn needs_continue(&self) -> bool {
        self.needs_continue
    }

    #[cfg(test)]
    pub(in crate::owned_upstream) fn from_parts_for_test(
        outputs: Vec<OwnerTargetOutput>,
        needs_continue: bool,
    ) -> Self {
        Self {
            outputs,
            needs_continue,
        }
    }
}

impl fmt::Debug for OwnerSessionDrive {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OwnerSessionDrive")
            .field("outputs", &self.outputs.len())
            .field("needs_continue", &self.needs_continue)
            .finish()
    }
}

/// Closed ownership settlement for one requested continuation turn.
///
/// The variant, rather than an independently inferred error kind, tells the
/// physical root whether the controller still owns the exact continuation
/// capability. Callers must settle their matching scheduling marker from this
/// value and cannot treat every error as if the old continuation survived.
#[must_use = "continuation outcomes must settle the physical scheduling owner"]
#[derive(Debug)]
pub(crate) enum OwnerSessionContinuationOutcome {
    /// The old capability was consumed and produced one bounded output page.
    Advanced(OwnerSessionDrive),
    /// The exact old capability remains installed, but no production wake can
    /// make the present failure safe to retry. It must enter terminal
    /// settlement without recreating a schedulable marker.
    TerminalRetained(OwnerSessionControllerError),
    /// The old capability was consumed into Target/controller failure state.
    Aborted(OwnerSessionControllerError),
    /// No exact controller continuation exists for the requested turn.
    Missing(OwnerSessionControllerError),
}

#[cfg(test)]
impl OwnerSessionContinuationOutcome {
    pub(in crate::owned_upstream) fn aborted_for_test() -> Self {
        Self::Aborted(OwnerSessionControllerError::new(
            OwnerSessionControllerErrorKind::Failed,
        ))
    }

    pub(in crate::owned_upstream) fn terminal_retained_for_test() -> Self {
        Self::TerminalRetained(OwnerSessionControllerError::new(
            OwnerSessionControllerErrorKind::Failed,
        ))
    }

    pub(in crate::owned_upstream) fn missing_for_test() -> Self {
        Self::Missing(OwnerSessionControllerError::new(
            OwnerSessionControllerErrorKind::NoPendingContinuation,
        ))
    }

    fn into_result_for_test(self) -> Result<OwnerSessionDrive, OwnerSessionControllerError> {
        match self {
            Self::Advanced(drive) => Ok(drive),
            Self::TerminalRetained(error) | Self::Aborted(error) | Self::Missing(error) => {
                Err(error)
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
enum OwnerSessionProgressError {
    #[error("terminal grace retention failed: {0}")]
    TerminalGrace(TerminalGraceError),
    #[error("reverse-progress observation failed: {0}")]
    Reverse(#[from] OwnerProgressError),
    #[error("a positive application ACK lacked actor-captured arrival evidence")]
    MissingActorArrival,
    #[error(
        "reducer observation generation {observed:?} does not match committed generation {committed:?}"
    )]
    ObservationGenerationNotCommitted {
        committed: LegGeneration,
        observed: LegGeneration,
    },
}

enum OwnerSessionControllerErrorKind {
    InactiveActorEvent {
        phase: OwnerSessionControllerPhase,
        input: Box<OwnerSessionActorEvent>,
    },
    RejectedActorEvent {
        block: OwnerTargetInputBlock,
        input: Box<OwnerSessionActorEvent>,
    },
    WrongActorClock {
        input: Box<OwnerSessionActorEvent>,
    },
    ActorClock {
        source: SessionClockError,
        input: Box<OwnerSessionActorEvent>,
    },
    Clock(SessionClockError),
    ReadyReceipt(OwnerTargetInputBlock),
    ContinueRequired,
    NoPendingContinuation,
    RecoveryRequired,
    NoPendingRecovery,
    RegisteredRecoveryBlocked {
        block: OwnerRegisteredRecoveryPreflightBlock,
    },
    RecoveryAcceptanceRequired,
    RecoveryAcceptanceQueueBlocked {
        kind: LegOutboundQueueErrorKind,
    },
    RecoveryExecuteRequired,
    RecoveryPublishRequired,
    RecoveryHandoffRequired,
    RecoveryHandoffOccupied,
    RecoveryHandoffWrongOwner,
    RecoveryHandoffWrongTransport,
    InitialRetirementRejected {
        kind: OwnerTargetInitialRetirementErrorKind,
    },
    RecoveryTerminalDiscardRequired,
    RejectedRecoveryTerminalDiscard {
        block: OwnerTargetInputBlock,
    },
    RecoveryTerminalQuarantined,
    WrongRecoveryOwner {
        recovery: Box<EnqueuedOwnerTargetAttach>,
    },
    RejectedRecoveryStart {
        recovery: Box<EnqueuedOwnerTargetAttach>,
    },
    RejectedRecoveryExecute {
        block: OwnerTargetInputBlock,
    },
    RejectedRecoveryPublish {
        block: OwnerTargetInputBlock,
    },
    RecoveryQueueBlocked {
        kind: LegOutboundQueueErrorKind,
    },
    AttachStatusQueueBlocked {
        kind: LegOutboundQueueErrorKind,
    },
    RejectedAttachStatusArm {
        block: OwnerTargetInputBlock,
    },
    InvalidRecoveryPage,
    RejectedContinuation {
        block: OwnerTargetInputBlock,
    },
    InvalidContinuationPage,
    Target(OwnerTargetError),
    TargetAndProgress {
        target: OwnerTargetError,
        progress: OwnerSessionProgressError,
    },
    PostCommitProgress(OwnerSessionProgressError),
    SessionExpired,
    Failed,
}
/// Opaque controller failure. Reducer observation batches remain nested in
/// controller/executor ownership and are never exposed as public fields or
/// through the error source chain.
pub(crate) struct OwnerSessionControllerError {
    kind: OwnerSessionControllerErrorKind,
}

impl OwnerSessionControllerError {
    fn new(kind: OwnerSessionControllerErrorKind) -> Self {
        Self { kind }
    }

    /// Value-only classification for physical scheduling. The exact pending
    /// publication/cursor is already restored inside the controller.
    pub(super) fn is_recovery_queue_pressure(&self) -> bool {
        matches!(
            self.kind,
            OwnerSessionControllerErrorKind::RecoveryAcceptanceQueueBlocked { .. }
                | OwnerSessionControllerErrorKind::RecoveryQueueBlocked { .. }
        )
    }

    pub(super) fn is_recovery_terminal_discard_blocked(&self) -> bool {
        matches!(
            self.kind,
            OwnerSessionControllerErrorKind::RejectedRecoveryTerminalDiscard { .. }
        )
    }

    /// Retry consumes the exact rejected actor event and its original arrival
    /// witness. Non-rejection errors are returned unchanged.
    #[allow(dead_code, reason = "the physical owner composition will own retry")]
    pub(super) fn retry_actor_event<T: TargetIo, C>(
        self,
        controller: &mut OwnerSessionControllerCore<T, C>,
    ) -> Result<OwnerSessionDrive, Self> {
        match self.kind {
            OwnerSessionControllerErrorKind::InactiveActorEvent { input, .. }
            | OwnerSessionControllerErrorKind::RejectedActorEvent { input, .. }
            | OwnerSessionControllerErrorKind::WrongActorClock { input }
            | OwnerSessionControllerErrorKind::ActorClock { input, .. } => {
                controller.apply_actor_event(*input)
            }
            kind => Err(Self { kind }),
        }
    }

    /// Retries one exact precommit recovery handoff without ever returning
    /// the raw attach capability to composition code.
    #[allow(
        dead_code,
        reason = "the physical owner Quinn composition slice will own gated retry"
    )]
    pub(super) fn retry_attached_recovery_start<T: TargetIo, C>(
        self,
        controller: &mut OwnerSessionControllerCore<T, C>,
        _gate: &QuinnLegCompositionGate,
    ) -> Result<OwnerSessionRecoveryDrive, Self> {
        self.retry_attached_recovery_start_inner(controller)
    }

    #[cfg(test)]
    fn retry_attached_recovery_start_for_test<T: TargetIo, C>(
        self,
        controller: &mut OwnerSessionControllerCore<T, C>,
    ) -> Result<OwnerSessionRecoveryDrive, Self> {
        self.retry_attached_recovery_start_inner(controller)
    }

    fn retry_attached_recovery_start_inner<T: TargetIo, C>(
        self,
        controller: &mut OwnerSessionControllerCore<T, C>,
    ) -> Result<OwnerSessionRecoveryDrive, Self> {
        match self.kind {
            OwnerSessionControllerErrorKind::WrongRecoveryOwner { recovery }
            | OwnerSessionControllerErrorKind::RejectedRecoveryStart { recovery } => {
                controller.begin_attached_recovery_inner(*recovery)
            }
            kind => Err(Self { kind }),
        }
    }
}

impl fmt::Debug for OwnerSessionControllerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            OwnerSessionControllerErrorKind::InactiveActorEvent { phase, .. } => formatter
                .debug_tuple("InactiveActorEvent")
                .field(phase)
                .finish(),
            OwnerSessionControllerErrorKind::RejectedActorEvent { block, .. } => formatter
                .debug_tuple("RejectedActorEvent")
                .field(block)
                .finish(),
            OwnerSessionControllerErrorKind::WrongActorClock { .. } => {
                formatter.write_str("WrongActorClock")
            }
            OwnerSessionControllerErrorKind::ActorClock { source, .. } => {
                formatter.debug_tuple("ActorClock").field(source).finish()
            }
            OwnerSessionControllerErrorKind::Clock(source) => {
                formatter.debug_tuple("Clock").field(source).finish()
            }
            OwnerSessionControllerErrorKind::ContinueRequired => {
                formatter.write_str("ContinueRequired")
            }
            OwnerSessionControllerErrorKind::NoPendingContinuation => {
                formatter.write_str("NoPendingContinuation")
            }
            OwnerSessionControllerErrorKind::RecoveryRequired => {
                formatter.write_str("RecoveryRequired")
            }
            OwnerSessionControllerErrorKind::NoPendingRecovery => {
                formatter.write_str("NoPendingRecovery")
            }
            OwnerSessionControllerErrorKind::RegisteredRecoveryBlocked { block } => formatter
                .debug_tuple("RegisteredRecoveryBlocked")
                .field(block)
                .finish(),
            OwnerSessionControllerErrorKind::RecoveryAcceptanceRequired => {
                formatter.write_str("RecoveryAcceptanceRequired")
            }
            OwnerSessionControllerErrorKind::RecoveryAcceptanceQueueBlocked { kind: _ } => {
                formatter.write_str("RecoveryAcceptanceQueueBlocked")
            }
            OwnerSessionControllerErrorKind::RecoveryExecuteRequired => {
                formatter.write_str("RecoveryExecuteRequired")
            }
            OwnerSessionControllerErrorKind::RecoveryPublishRequired => {
                formatter.write_str("RecoveryPublishRequired")
            }
            OwnerSessionControllerErrorKind::RecoveryHandoffRequired => {
                formatter.write_str("RecoveryHandoffRequired")
            }
            OwnerSessionControllerErrorKind::RecoveryHandoffOccupied => {
                formatter.write_str("RecoveryHandoffOccupied")
            }
            OwnerSessionControllerErrorKind::RecoveryHandoffWrongOwner => {
                formatter.write_str("RecoveryHandoffWrongOwner")
            }
            OwnerSessionControllerErrorKind::RecoveryHandoffWrongTransport => {
                formatter.write_str("RecoveryHandoffWrongTransport")
            }
            OwnerSessionControllerErrorKind::InitialRetirementRejected { kind } => formatter
                .debug_tuple("InitialRetirementRejected")
                .field(kind)
                .finish(),
            OwnerSessionControllerErrorKind::RecoveryTerminalDiscardRequired => {
                formatter.write_str("RecoveryTerminalDiscardRequired")
            }
            OwnerSessionControllerErrorKind::RejectedRecoveryTerminalDiscard { block } => formatter
                .debug_tuple("RejectedRecoveryTerminalDiscard")
                .field(block)
                .finish(),
            OwnerSessionControllerErrorKind::RecoveryTerminalQuarantined => {
                formatter.write_str("RecoveryTerminalQuarantined")
            }
            OwnerSessionControllerErrorKind::WrongRecoveryOwner { .. } => {
                formatter.write_str("WrongRecoveryOwner")
            }
            OwnerSessionControllerErrorKind::RejectedRecoveryStart { .. } => {
                formatter.write_str("RejectedRecoveryStart")
            }
            OwnerSessionControllerErrorKind::RejectedRecoveryExecute { block } => formatter
                .debug_tuple("RejectedRecoveryExecute")
                .field(block)
                .finish(),
            OwnerSessionControllerErrorKind::RejectedRecoveryPublish { block } => formatter
                .debug_tuple("RejectedRecoveryPublish")
                .field(block)
                .finish(),
            OwnerSessionControllerErrorKind::RecoveryQueueBlocked { kind: _ } => {
                formatter.write_str("RecoveryQueueBlocked")
            }
            OwnerSessionControllerErrorKind::AttachStatusQueueBlocked { kind: _ } => {
                formatter.write_str("AttachStatusQueueBlocked")
            }
            OwnerSessionControllerErrorKind::RejectedAttachStatusArm { block } => formatter
                .debug_tuple("RejectedAttachStatusArm")
                .field(block)
                .finish(),
            OwnerSessionControllerErrorKind::InvalidRecoveryPage => {
                formatter.write_str("InvalidRecoveryPage")
            }
            OwnerSessionControllerErrorKind::RejectedContinuation { block } => formatter
                .debug_tuple("RejectedContinuation")
                .field(block)
                .finish(),
            OwnerSessionControllerErrorKind::InvalidContinuationPage => {
                formatter.write_str("InvalidContinuationPage")
            }
            OwnerSessionControllerErrorKind::Target(source) => formatter
                .debug_tuple("Target")
                .field(&source.to_string())
                .finish(),
            OwnerSessionControllerErrorKind::TargetAndProgress { target, progress } => formatter
                .debug_struct("TargetAndProgress")
                .field("target", &target.to_string())
                .field("progress", progress)
                .finish(),
            OwnerSessionControllerErrorKind::PostCommitProgress(source) => formatter
                .debug_tuple("PostCommitProgress")
                .field(source)
                .finish(),
            OwnerSessionControllerErrorKind::SessionExpired => {
                formatter.write_str("SessionExpired")
            }
            OwnerSessionControllerErrorKind::Failed => formatter.write_str("Failed"),
            OwnerSessionControllerErrorKind::ReadyReceipt(block) => {
                formatter.debug_tuple("ReadyReceipt").field(block).finish()
            }
        }
    }
}
impl fmt::Display for OwnerSessionControllerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}
impl std::error::Error for OwnerSessionControllerError {}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct OwnerSessionControllerSnapshot {
    pub(crate) progress_entries: usize,
    pub(crate) session_expired: bool,
    pub(crate) failed: bool,
    pub(crate) retained_outputs: usize,
    pub(crate) retained_observations: usize,
    pub(crate) pending_continuations: usize,
    pub(crate) retained_continuations: usize,
    pub(crate) pending_recoveries: usize,
    pub(crate) pending_attach_statuses: usize,
    pub(crate) retained_recoveries: usize,
    pub(crate) retained_recovery_cursors: usize,
    pub(crate) retained_recovery_outputs: usize,
    pub(crate) retained_recovery_observations: usize,
    pub(crate) retained_recovery_attached: usize,
    pub(crate) recovery_needs_acceptance: bool,
    pub(crate) recovery_needs_execute: bool,
    pub(crate) recovery_needs_publish: bool,
    pub(crate) recovery_handoff_ready: bool,
    pub(crate) recovery_needs_terminal_discard: bool,
    pub(crate) recovery_terminal_quarantined: usize,
    pub(crate) pending_legless_high_waters: usize,
    pub(crate) attach_status_needs_enqueue: bool,
    pub(crate) attach_status_needs_delivery: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OwnerSessionControllerColdCount {
    TotalContinuations,
    ActorOrigins,
    LocalOrigins,
}
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub(crate) enum OwnerSessionControllerColdInventoryError {
    #[error("original progress storage audit failed: {0}")]
    Progress(OwnerProgressError),
    #[error("a retained controller frame is invalid: {0}")]
    InvalidRetainedFrame(ProtocolError),
    #[error("the independently summed retained controller frame bytes overflowed")]
    FrameByteSumOverflow,
    #[error("the stored retained controller frame audit rejected a frame: {0}")]
    StoredFrameInvalid(ProtocolError),
    #[error("the stored retained controller frame-byte sum overflowed")]
    StoredFrameByteSumOverflow,
    #[error(
        "retained controller output count drifted: stored {stored}, independently counted {summed}"
    )]
    StoredOutputCountMismatch { stored: usize, summed: usize },
    #[error(
        "retained controller frame bytes drifted: stored {stored}, independently summed {summed}"
    )]
    StoredFrameByteMismatch { stored: usize, summed: usize },
    #[error("controller cold count overflowed for {category:?}")]
    CountOverflow {
        category: OwnerSessionControllerColdCount,
    },
    #[error("a retained application ACK observation has no actor arrival origin")]
    RetainedApplicationAckWithoutActorOrigin,
    #[error("a retained recovery application ACK has no actor arrival origin")]
    RetainedRecoveryAckWithoutActorOrigin,
    #[error("retained recovery payload audit failed: {0:?}")]
    RecoveryPayload(super::owner_target::error_inventory::ErrorAuditFailure),
}
/// Allocation-free progress/page/error audit; Executor/physical authority is separate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct OwnerSessionControllerColdInventory {
    retained_outputs: usize,
    retained_target_errors: usize,
    retained_error_payload: super::owner_target::error_inventory::TargetErrorColdAudit,
    retained_recovery_payload: super::owner_target::error_inventory::ErrorColdInventory,
    independently_summed_frame_bytes: usize,
    stored_frame_bytes: usize,
    pending_continuations: usize,
    retained_continuations: usize,
    malformed_raw_continuations: usize,
    total_continuations: usize,
    actor_origin_witnesses: usize,
    local_origin_witnesses: usize,
    malformed_page_origin_witnesses: usize,
    malformed_page_arrival_witnesses: usize,
    retained_observations: usize,
    retained_application_ack_observations: usize,
    target_aborted_effect_guards: usize,
    target_aborted_effects: usize,
    target_retained_ordinary_outputs: usize,
    target_retained_ordinary_observations: usize,
    target_retained_recovery_outputs: usize,
    target_retained_recovery_observations: usize,
    controller_retained_recovery_turns: usize,
    target_retained_recovery_guards: usize,
}

/// Privately constructed proof of one exact reducer/factory/Target/queue/output plan.
pub(crate) struct CheckedOwnerControllerCapacity {
    witness: OwnerSessionCapacityWitness,
    inbox: super::session::SessionOwnerInbox,
    receipt: OwnerReadyReceipt,
}
enum OwnerReadyReceipt {
    Vacant,
    Held(super::session::SessionOwnerCommand),
    Unknown,
}
pub(super) enum OwnerReadyActivationBlock {
    OutputUnavailable,
    ControllerUnavailable,
    PendingContinuation,
    RecoveryRetained,
    ReceiptRetained,
    Target(super::owner_target::OwnerReadyInstallError),
}
/// Private legacy test capacity; it cannot satisfy physical composition signatures.
#[cfg(test)]
struct LegacyOwnerControllerCapacity;
/// Sole Target/progress/clock owner; physical callers require the checked alias.
pub(crate) struct OwnerSessionControllerCore<T, C> {
    terminal_grace: OwnerTerminalGraceState,
    executor: OwnerTargetExecutor<T>,
    capacity: C,
    progress: OwnerReverseProgress,
    clock: SessionMonotonicClock,
    phase: OwnerSessionControllerPhase,
    pending_continuation: Option<PendingOwnerContinuation>,
    aborted_turn: Option<AbortedOwnerControllerTurn>,
    recovery: Option<PendingOwnerAttachedRecovery>,
    legless_high_water: Option<PendingOwnerLeglessHighWater>,
    terminal_quarantine: Option<QuarantinedOwnerAttachLoss>,
    attach_status: Option<PendingOwnerAttachStatus>,
    aborted_recovery: Option<AbortedOwnerRecoveryTurn>,
    #[cfg(test)]
    fail_next_recovery_progress: bool,
    #[cfg(test)]
    fail_next_continuation_progress: bool,
    #[cfg(test)]
    panic_after_terminal_bind_commit: bool,
}
/// Production controller, already checked against one exact OwnerSessionCapacityPlan.
pub(crate) type OwnerSessionController<T> =
    OwnerSessionControllerCore<T, CheckedOwnerControllerCapacity>;
/// Distinct test-only type; unchecked fixtures cannot cross physical composition.
#[cfg(test)]
type LegacyOwnerSessionController<T> = OwnerSessionControllerCore<T, LegacyOwnerControllerCapacity>;
impl<T: TargetIo, C> OwnerSessionControllerCore<T, C> {
    pub(crate) fn snapshot(&self) -> OwnerSessionControllerSnapshot {
        let target = self.executor.snapshot();
        // Both terms are structurally binary: the controller owns one
        // `Option<AbortedOwnerRecoveryTurn>` and the Target snapshot derives
        // its guard count from one `Option<AbortedOwnerTargetEffects>` whose
        // recovery field is also an Option. Their exact sum is therefore at
        // most two and cannot overflow `usize` on any supported target.
        let retained_recoveries =
            usize::from(self.aborted_recovery.is_some()) + target.aborted_recovery_guards;
        let retained_controller_cursor = self
            .aborted_recovery
            .as_ref()
            .and_then(|turn| turn.pending.as_ref())
            .is_some_and(|pending| matches!(pending, PendingOwnerAttachedRecovery::Publish(_)));
        let retained_controller_attached = self.aborted_recovery.as_ref().map_or(0, |turn| {
            usize::from(turn.pending.is_some())
                + turn
                    .outputs
                    .iter()
                    .filter(|output| {
                        matches!(
                            output,
                            OwnerTargetRecoveryOutput::NeedsResume { .. }
                                | OwnerTargetRecoveryOutput::RecoveryCompleted { .. }
                        )
                    })
                    .count()
        });
        OwnerSessionControllerSnapshot {
            progress_entries: self.progress.len(),
            session_expired: matches!(
                self.phase,
                OwnerSessionControllerPhase::SessionExpiring
                    | OwnerSessionControllerPhase::SessionExpired
            ),
            failed: self.phase == OwnerSessionControllerPhase::Failed
                || self.terminal_grace.has_turn(),
            retained_outputs: self
                .aborted_turn
                .as_ref()
                .map_or(0, AbortedOwnerControllerTurn::retained_output_count)
                + target.aborted_ordinary_outputs,
            retained_observations: self
                .aborted_turn
                .as_ref()
                .map_or(0, |turn| turn.observations.fact_count())
                + target.aborted_ordinary_observations
                + self.terminal_grace.fact_count(),
            pending_continuations: usize::from(self.pending_continuation.is_some()),
            retained_continuations: self
                .aborted_turn
                .as_ref()
                .map_or(0, AbortedOwnerControllerTurn::retained_continuation_count),
            pending_recoveries: usize::from(self.recovery.is_some()),
            pending_attach_statuses: usize::from(self.attach_status.is_some()),
            retained_recoveries,
            retained_recovery_cursors: usize::from(retained_controller_cursor)
                + target.aborted_recovery_guards,
            retained_recovery_outputs: self.aborted_recovery.as_ref().map_or(0, |turn| {
                turn.outputs.len()
                    + turn
                        .pending
                        .as_ref()
                        .map_or(0, PendingOwnerAttachedRecovery::retained_output_count)
            }) + target.aborted_recovery_outputs,
            retained_recovery_observations: self
                .aborted_recovery
                .as_ref()
                .and_then(|turn| turn.observations.as_ref())
                .map_or(0, OwnerTargetReducerObservations::fact_count)
                + target.aborted_recovery_observations,
            retained_recovery_attached: retained_controller_attached
                + target.aborted_recovery_guards,
            recovery_needs_acceptance: matches!(
                self.recovery,
                Some(PendingOwnerAttachedRecovery::Acceptance(_))
            ),
            recovery_needs_execute: matches!(
                self.recovery,
                Some(
                    PendingOwnerAttachedRecovery::Initial(_)
                        | PendingOwnerAttachedRecovery::Execute(_)
                )
            ),
            recovery_needs_publish: matches!(
                self.recovery,
                Some(PendingOwnerAttachedRecovery::Publish(_))
            ),
            recovery_handoff_ready: matches!(
                self.recovery,
                Some(PendingOwnerAttachedRecovery::HandoffReady(_))
            ),
            recovery_needs_terminal_discard: matches!(
                self.recovery,
                Some(PendingOwnerAttachedRecovery::TerminalBound(_))
            ),
            recovery_terminal_quarantined: usize::from(self.terminal_quarantine.is_some()),
            pending_legless_high_waters: usize::from(self.legless_high_water.is_some()),
            attach_status_needs_enqueue: matches!(
                self.attach_status,
                Some(PendingOwnerAttachStatus::Enqueue(_))
            ),
            attach_status_needs_delivery: matches!(
                self.attach_status,
                Some(PendingOwnerAttachStatus::Delivery(_))
            ),
        }
    }
    /// Immutable preflight for one physical-B registration while initial A
    /// remains the controller's exact active handoff.  A blocked turn leaves
    /// the actor's arrival, ordinal, permits, and bytes in its inbound slot.
    pub(super) fn preflight_initial_standby_registration(
        &self,
        active: &OwnerTargetInitialHandoff,
        _gate: &QuinnLegCompositionGate,
    ) -> Result<OwnerInitialStandbyRegistrationReady, OwnerInitialStandbyRegistrationBlock> {
        if self.aborted_turn.is_some()
            || self.aborted_recovery.is_some()
            || self.terminal_quarantine.is_some()
        {
            return Err(OwnerInitialStandbyRegistrationBlock::Quarantined);
        }
        if self.phase != OwnerSessionControllerPhase::Active {
            return Err(OwnerInitialStandbyRegistrationBlock::Inactive);
        }
        if self.pending_continuation.is_some() {
            return Err(OwnerInitialStandbyRegistrationBlock::ContinuationPending);
        }
        if self.recovery.is_some() {
            return Err(OwnerInitialStandbyRegistrationBlock::RecoveryPending);
        }
        if self.attach_status.is_some() {
            return Err(OwnerInitialStandbyRegistrationBlock::StatusPending);
        }
        if let Some(block) = self.executor.initial_standby_authentication_block(active) {
            return Err(OwnerInitialStandbyRegistrationBlock::Target(block));
        }
        Ok(OwnerInitialStandbyRegistrationReady { _private: () })
    }

    /// Consumes only the immutable preflight vacancy and the exact classified
    /// registration.  The executor borrows the opaque initial handoff; no
    /// naked `AttachedLeg` crosses this controller boundary.
    pub(super) fn authenticate_initial_standby(
        &self,
        _ready: OwnerInitialStandbyRegistrationReady,
        active: &OwnerTargetInitialHandoff,
        candidate: &EstablishedLeg,
        inbound: BoundInbound,
        _gate: &QuinnLegCompositionGate,
    ) -> Result<ExactAuthenticatedStandby, OwnerTargetError> {
        self.executor
            .authenticate_owner_standby_from_initial(active, candidate, inbound)
    }

    /// Borrow-only vacancy check used before the actor envelope is decoded.
    /// A transient block therefore leaves arrival, ordinal, permits, and raw
    /// bytes wholly actor-owned for an exact retry.
    pub(super) fn preflight_registered_recovery(
        &self,
    ) -> Result<OwnerRegisteredRecoveryReady, OwnerRegisteredRecoveryPreflightBlock> {
        if self.aborted_turn.is_some()
            || self.aborted_recovery.is_some()
            || self.terminal_quarantine.is_some()
        {
            return Err(OwnerRegisteredRecoveryPreflightBlock::Quarantined);
        }
        if self.phase != OwnerSessionControllerPhase::Active {
            return Err(OwnerRegisteredRecoveryPreflightBlock::Inactive);
        }
        if self.pending_continuation.is_some() {
            return Err(OwnerRegisteredRecoveryPreflightBlock::ContinuationPending);
        }
        if self.recovery.is_some() {
            return Err(OwnerRegisteredRecoveryPreflightBlock::RecoveryPending);
        }
        if self.attach_status.is_some() {
            return Err(OwnerRegisteredRecoveryPreflightBlock::StatusPending);
        }
        if let Some(block) = self.executor.owner_attach_admission_block() {
            return Err(OwnerRegisteredRecoveryPreflightBlock::Target(block));
        }
        Ok(OwnerRegisteredRecoveryReady { _private: () })
    }

    /// Performs the registered-slot CAS and immediately transfers every
    /// resulting capability into either this controller or the actor FIFO.
    /// The closed Quinn owner must call `preflight_registered_recovery`
    /// before decoding the actor envelope in the same first-failure turn.
    pub(super) fn accept_registered_owner_attach(
        &mut self,
        _ready: OwnerRegisteredRecoveryReady,
        slot: &mut OwnerStandbySlot,
        leg: &EstablishedLeg,
        received: LegBoundFrame,
        queue: &mut LegOutboundQueue,
        _gate: &QuinnLegCompositionGate,
    ) -> Result<OwnerSessionRegisteredAttachDrive, OwnerSessionControllerError> {
        match self
            .executor
            .accept_registered_owner_attach(slot, leg, received, queue)
        {
            Ok(OwnerTargetAttachPublication::Installed { pending }) => {
                drop(self.legless_high_water.take());
                self.recovery = Some(PendingOwnerAttachedRecovery::Acceptance(pending));
                match self.enqueue_recovery_acceptance_inner(queue) {
                    Ok(drive) => Ok(OwnerSessionRegisteredAttachDrive::Recovery(drive)),
                    Err(OwnerSessionControllerError {
                        kind:
                            OwnerSessionControllerErrorKind::RecoveryAcceptanceQueueBlocked { kind },
                    }) => Ok(OwnerSessionRegisteredAttachDrive::AcceptancePending { kind }),
                    Err(error) => Err(error),
                }
            }
            Ok(OwnerTargetAttachPublication::Resynchronize { pending }) => {
                self.attach_status = Some(PendingOwnerAttachStatus::Enqueue(pending));
                match self.enqueue_attach_status_inner(queue) {
                    Ok(()) => Ok(OwnerSessionRegisteredAttachDrive::StatusQueued),
                    Err(OwnerSessionControllerError {
                        kind: OwnerSessionControllerErrorKind::AttachStatusQueueBlocked { kind },
                    }) => Ok(OwnerSessionRegisteredAttachDrive::StatusPending { kind }),
                    Err(error) => Err(error),
                }
            }
            Err(error) => Err(OwnerSessionControllerError::new(
                OwnerSessionControllerErrorKind::Target(error),
            )),
        }
    }

    /// Retries only FIFO admission; the installed attach and its reserved
    /// acceptance remain controller-owned on every failure.
    pub(super) fn enqueue_recovery_acceptance(
        &mut self,
        queue: &mut LegOutboundQueue,
        _gate: &QuinnLegCompositionGate,
    ) -> Result<OwnerSessionRecoveryDrive, OwnerSessionControllerError> {
        self.enqueue_recovery_acceptance_inner(queue)
    }

    fn enqueue_recovery_acceptance_inner(
        &mut self,
        queue: &mut LegOutboundQueue,
    ) -> Result<OwnerSessionRecoveryDrive, OwnerSessionControllerError> {
        self.require_continuable()?;
        let Some(pending) = self.recovery.take() else {
            return Err(OwnerSessionControllerError::new(
                OwnerSessionControllerErrorKind::NoPendingRecovery,
            ));
        };
        let publication = match pending {
            PendingOwnerAttachedRecovery::Acceptance(publication) => publication,
            execute @ (PendingOwnerAttachedRecovery::Initial(_)
            | PendingOwnerAttachedRecovery::Execute(_)) => {
                self.recovery = Some(execute);
                return Err(OwnerSessionControllerError::new(
                    OwnerSessionControllerErrorKind::RecoveryExecuteRequired,
                ));
            }
            publish @ PendingOwnerAttachedRecovery::Publish(_) => {
                self.recovery = Some(publish);
                return Err(OwnerSessionControllerError::new(
                    OwnerSessionControllerErrorKind::RecoveryPublishRequired,
                ));
            }
            handoff @ PendingOwnerAttachedRecovery::HandoffReady(_) => {
                self.recovery = Some(handoff);
                return Err(OwnerSessionControllerError::new(
                    OwnerSessionControllerErrorKind::RecoveryHandoffRequired,
                ));
            }
            terminal @ PendingOwnerAttachedRecovery::TerminalBound(_) => {
                self.recovery = Some(terminal);
                return Err(OwnerSessionControllerError::new(
                    OwnerSessionControllerErrorKind::RecoveryTerminalDiscardRequired,
                ));
            }
        };
        match publication.enqueue_acceptance(queue) {
            Ok(recovery) => {
                self.recovery = Some(PendingOwnerAttachedRecovery::Initial(recovery));
                Ok(self.recovery_drive(Vec::new()))
            }
            Err(error) => {
                let kind = error.kind().clone();
                self.recovery = Some(PendingOwnerAttachedRecovery::Acceptance(
                    error.into_publication(),
                ));
                Err(OwnerSessionControllerError::new(
                    OwnerSessionControllerErrorKind::RecoveryAcceptanceQueueBlocked { kind },
                ))
            }
        }
    }

    pub(super) fn enqueue_attach_status(
        &mut self,
        queue: &mut LegOutboundQueue,
        _gate: &QuinnLegCompositionGate,
    ) -> Result<(), OwnerSessionControllerError> {
        self.enqueue_attach_status_inner(queue)
    }

    fn enqueue_attach_status_inner(
        &mut self,
        queue: &mut LegOutboundQueue,
    ) -> Result<(), OwnerSessionControllerError> {
        self.require_continuable()?;
        let Some(pending) = self.attach_status.take() else {
            return Ok(());
        };
        let PendingOwnerAttachStatus::Enqueue(publication) = pending else {
            self.attach_status = Some(pending);
            return Ok(());
        };
        match publication.enqueue(queue) {
            Ok(status) => {
                self.attach_status = Some(PendingOwnerAttachStatus::Delivery(status));
                Ok(())
            }
            Err(error) => {
                let kind = error.kind().clone();
                self.attach_status =
                    Some(PendingOwnerAttachStatus::Enqueue(error.into_publication()));
                Err(OwnerSessionControllerError::new(
                    OwnerSessionControllerErrorKind::AttachStatusQueueBlocked { kind },
                ))
            }
        }
    }

    /// Consumes a queued status only after the exact FIFO reports ordered
    /// delivery. A premature/wrong-queue arm restores the whole publication.
    pub(super) fn arm_attach_status_after_delivery(
        &mut self,
        queue: &LegOutboundQueue,
        _gate: &QuinnLegCompositionGate,
    ) -> Result<bool, OwnerSessionControllerError> {
        self.require_continuable()?;
        let Some(pending) = self.attach_status.take() else {
            return Ok(false);
        };
        let PendingOwnerAttachStatus::Delivery(publication) = pending else {
            self.attach_status = Some(pending);
            return Ok(false);
        };
        match self
            .executor
            .arm_owner_catch_up_after_status_delivery(publication, queue)
        {
            Ok(()) => Ok(true),
            Err(OwnerTargetError::RejectedOwnerAttachStatusArm { block, publication }) => {
                self.attach_status = Some(PendingOwnerAttachStatus::Delivery(*publication));
                Err(OwnerSessionControllerError::new(
                    OwnerSessionControllerErrorKind::RejectedAttachStatusArm { block },
                ))
            }
            Err(error) => Err(OwnerSessionControllerError::new(
                OwnerSessionControllerErrorKind::Target(error),
            )),
        }
    }

    pub(crate) fn reverse_progress(
        &self,
        flow_id: SessionFlowId,
        direction: Direction,
    ) -> Result<OwnerReverseDirectionSnapshot, OwnerProgressError> {
        self.progress.direction_snapshot(flow_id, direction)
    }

    /// Selects and admits at most one authenticated owner hint without
    /// accepting caller-selected flow, direction, offset, ACK time, or service
    /// time.  Selection uses the controller's bounded reducer registry and its
    /// exact endpoint-session clock; physical preparation and queue admission
    /// then revalidate the same episode and exact B synchronously.
    pub(super) fn drive_next_due_owner_hint(
        &self,
        issuer: &mut OwnerSwitchHintIssuer,
        standby_slot: &OwnerStandbySlot,
        queue: &mut LegOutboundQueue,
        _gate: &QuinnLegCompositionGate,
    ) -> Result<OwnerSessionHintDrive, OwnerSessionHintDriveError> {
        if self.phase != OwnerSessionControllerPhase::Active
            || self.aborted_turn.is_some()
            || self.aborted_recovery.is_some()
        {
            return Err(OwnerSessionHintDriveError::Inactive);
        }
        let observed_at = self
            .clock
            .now()
            .map_err(OwnerSessionHintDriveError::Clock)?;
        let selected = issuer
            .select_next_due_hint(&self.progress, observed_at)
            .map_err(OwnerSessionHintDriveError::Policy)?;
        let selected = match selected {
            OwnerHintDueCursor::Scanning => return Ok(OwnerSessionHintDrive::Scanning),
            OwnerHintDueCursor::Inert => return Ok(OwnerSessionHintDrive::Inert),
            OwnerHintDueCursor::Pending { deadline } => {
                return Ok(OwnerSessionHintDrive::Pending { deadline });
            }
            OwnerHintDueCursor::Due(selected) => selected,
        };
        let preparation = issuer
            .prepare_selected_due_hint_for_exact_slot(selected, standby_slot)
            .map_err(OwnerSessionHintDriveError::Policy)?;
        let hint = issuer
            .try_admit_pending(&self.progress, observed_at, standby_slot, queue)
            .map_err(OwnerSessionHintDriveError::Admission)?;
        Ok(OwnerSessionHintDrive::Admitted { preparation, hint })
    }

    /// Closed-composition clock identity check. Equality of elapsed values is
    /// not sufficient; actor arrival and controller progress must share the
    /// exact monotonic source.
    pub(super) fn shares_session_clock(
        &self,
        clock: &SessionMonotonicClock,
        _gate: &QuinnLegCompositionGate,
    ) -> bool {
        self.clock.same_source(clock)
    }

    /// Branch-by-abstraction seam for the initial physical owner.  The
    /// controller remains the sole Target-executor owner while the closed
    /// Quinn composition binds the non-cloneable initial handoff to its exact
    /// actor/runtime/FIFO chain.
    pub(super) fn bind_initial_handoff_to_physical_actor(
        &self,
        handoff: OwnerTargetInitialHandoff,
        runtime: &PhysicalLegRuntimeIncarnation,
        io: &LegIo,
        queue: &LegOutboundQueue,
        _gate: &QuinnLegCompositionGate,
    ) -> Result<OwnerTargetInitialHandoff, OwnerTargetInitialHandoffOwnerFailure> {
        self.executor
            .bind_initial_handoff_to_physical_actor(handoff, runtime, io, queue)
    }

    /// Settles only the exact initial acceptance receipt already bound through
    /// this controller's Target executor.  Pending or mismatched delivery
    /// returns the original handoff unchanged.
    pub(super) fn settle_initial_acceptance_delivery(
        &self,
        handoff: OwnerTargetInitialHandoff,
        runtime: &PhysicalLegRuntimeIncarnation,
        io: &LegIo,
        queue: &LegOutboundQueue,
        _gate: &QuinnLegCompositionGate,
    ) -> Result<OwnerTargetInitialAcceptanceSettlement, OwnerTargetInitialHandoffOwnerFailure> {
        self.executor
            .settle_initial_acceptance_delivery(handoff, runtime, io, queue)
    }

    /// Transfers one exact enqueued owner attach into controller ownership.
    /// Only the closed Quinn composition may perform this physical handoff.
    #[allow(
        dead_code,
        reason = "the physical owner Quinn composition slice will call this gated entry point"
    )]
    pub(super) fn begin_attached_recovery(
        &mut self,
        recovery: EnqueuedOwnerTargetAttach,
        _gate: &QuinnLegCompositionGate,
    ) -> Result<OwnerSessionRecoveryDrive, OwnerSessionControllerError> {
        self.begin_attached_recovery_inner(recovery)
    }

    /// Executes one bounded recovery page without mutating its exact FIFO.
    #[allow(
        dead_code,
        reason = "the physical owner Quinn composition slice will call this gated entry point"
    )]
    pub(super) fn execute_attached_recovery(
        &mut self,
        queue: &LegOutboundQueue,
        _gate: &QuinnLegCompositionGate,
    ) -> Result<OwnerSessionRecoveryDrive, OwnerSessionControllerError> {
        self.execute_attached_recovery_inner(queue)
    }

    /// Publishes one already-executed page through the exact acceptance FIFO.
    #[allow(
        dead_code,
        reason = "the physical owner Quinn composition slice will call this gated entry point"
    )]
    pub(super) fn publish_attached_recovery(
        &mut self,
        queue: &mut LegOutboundQueue,
        _gate: &QuinnLegCompositionGate,
    ) -> Result<OwnerSessionRecoveryDrive, OwnerSessionControllerError> {
        self.publish_attached_recovery_inner(queue)
    }

    /// Binds the actor's exact terminal to whichever recovery typestate owns
    /// the physical B AttachedLeg. Every rejection restores that state and
    /// returns the same terminal for actor-side envelope restoration.
    pub(super) fn bind_recovery_terminal(
        &mut self,
        terminal: ExactLegTerminal,
        leg: &EstablishedLeg,
        queue: &mut LegOutboundQueue,
        _gate: &QuinnLegCompositionGate,
    ) -> Result<OwnerSessionRecoveryTerminalBound, OwnerSessionRecoveryTerminalBindFailure> {
        self.bind_recovery_terminal_physical_inner(terminal, leg, queue)
    }

    /// Initial-A counterpart to active recovery-handoff terminal binding. The
    /// initial handoff stays in the caller's sole slot until its record-specific
    /// acceptance receipt has been settled and the exact runtime/policy/FIFO
    /// chain accepts terminal cleanup. Every rejection restores the handoff and
    /// returns the same terminal to the actor envelope.
    pub(super) fn bind_initial_terminal(
        &mut self,
        active: &mut Option<OwnerTargetInitialHandoff>,
        terminal: ExactLegTerminal,
        runtime: &PhysicalLegRuntimeIncarnation,
        io: &LegIo,
        queue: &mut LegOutboundQueue,
        _gate: &QuinnLegCompositionGate,
    ) -> Result<OwnerSessionRecoveryTerminalBound, OwnerSessionRecoveryTerminalBindFailure> {
        if self.recovery.is_some() {
            return Err(OwnerSessionRecoveryTerminalBindFailure {
                kind: OwnerSessionRecoveryTerminalBindErrorKind::TerminalAlreadyBound,
                terminal,
            });
        }
        if self.legless_high_water.is_some() {
            return Err(OwnerSessionRecoveryTerminalBindFailure {
                kind: OwnerSessionRecoveryTerminalBindErrorKind::LeglessHighWaterPending,
                terminal,
            });
        }
        let Some(handoff) = active.take() else {
            return Err(OwnerSessionRecoveryTerminalBindFailure {
                kind: OwnerSessionRecoveryTerminalBindErrorKind::NoPendingRecovery,
                terminal,
            });
        };
        match self
            .executor
            .terminalize_initial_handoff_physical_exact(handoff, terminal, runtime, io, queue)
        {
            Ok(terminalized) => {
                let identity = OwnerSessionRecoveryTerminalBound {
                    generation: terminalized.generation(),
                    reason: terminalized.reason(),
                };
                self.recovery = Some(PendingOwnerAttachedRecovery::TerminalBound(terminalized));
                Ok(identity)
            }
            Err(failure) => {
                let block = failure.block();
                let (handoff, terminal) = failure.into_parts();
                *active = Some(handoff);
                Err(Self::recovery_terminal_bind_failure(block, terminal))
            }
        }
    }

    /// Active-B counterpart to recovery terminal binding. The executor-bound
    /// handoff remains in the root slot on every pre-bind rejection and moves
    /// into the same `TerminalBound` controller state on success.
    pub(super) fn bind_active_terminal(
        &mut self,
        active: &mut Option<OwnerTargetRecoveryHandoff>,
        terminal: ExactLegTerminal,
        leg: &EstablishedLeg,
        queue: &mut LegOutboundQueue,
        _gate: &QuinnLegCompositionGate,
    ) -> Result<OwnerSessionRecoveryTerminalBound, OwnerSessionRecoveryTerminalBindFailure> {
        if self.recovery.is_some() || self.legless_high_water.is_some() {
            return Err(OwnerSessionRecoveryTerminalBindFailure {
                kind: OwnerSessionRecoveryTerminalBindErrorKind::TerminalAlreadyBound,
                terminal,
            });
        }
        let Some(handoff) = active.take() else {
            return Err(OwnerSessionRecoveryTerminalBindFailure {
                kind: OwnerSessionRecoveryTerminalBindErrorKind::NoPendingRecovery,
                terminal,
            });
        };
        match self
            .executor
            .terminalize_recovery_handoff_physical_exact(handoff, terminal, leg, queue)
        {
            Ok(terminalized) => {
                let identity = OwnerSessionRecoveryTerminalBound {
                    generation: terminalized.generation(),
                    reason: terminalized.reason(),
                };
                self.recovery = Some(PendingOwnerAttachedRecovery::TerminalBound(terminalized));
                #[cfg(test)]
                self.maybe_panic_after_terminal_bind_commit_for_test();
                Ok(identity)
            }
            Err(failure) => {
                let block = failure.block();
                let (handoff, terminal) = failure.into_parts();
                *active = Some(handoff);
                Err(Self::recovery_terminal_bind_failure(block, terminal))
            }
        }
    }

    /// Second phase after actor terminal handoff. A retryable reducer block
    /// leaves `TerminalBound` intact; a postcommit invariant moves the exact
    /// loss into a typed quarantine and can never re-apply `LegLost`.
    pub(super) fn discard_recovery_terminal(
        &mut self,
        _gate: &QuinnLegCompositionGate,
    ) -> Result<OwnerSessionRecoveryTerminalDiscarded, OwnerSessionControllerError> {
        self.discard_recovery_terminal_inner()
    }

    #[cfg(test)]
    pub(super) fn arm_terminal_bind_postcommit_panic_for_test(&mut self) {
        self.panic_after_terminal_bind_commit = true;
    }

    #[cfg(test)]
    fn maybe_panic_after_terminal_bind_commit_for_test(&mut self) {
        if std::mem::take(&mut self.panic_after_terminal_bind_commit) {
            panic!("test owner recovery terminal bind postcommit unwind");
        }
    }

    /// Installs a completed recovery directly into the closed physical root's
    /// private active slot. The AttachedLeg never appears in a drive or error.
    #[cfg(test)]
    pub(super) fn install_recovery_handoff(
        &mut self,
        active: &mut Option<OwnerTargetRecoveryHandoff>,
        leg: &EstablishedLeg,
        _gate: &QuinnLegCompositionGate,
    ) -> Result<Option<LegGeneration>, OwnerSessionControllerError> {
        self.install_recovery_handoff_inner(active, leg)
    }

    /// Full physical-root install: every fallible A/B owner, actor, transport,
    /// and FIFO check completes before the controller removes HandoffReady B.
    /// Once B is written to `active`, converting prepared A to retired is the
    /// final infallible state move.
    #[allow(
        clippy::result_large_err,
        reason = "install rejection must return the exact initial-A handoff"
    )]
    pub(super) fn install_recovery_handoff_retiring_initial(
        &mut self,
        initial_a: OwnerTargetInitialHandoff,
        a_runtime: &PhysicalLegRuntimeIncarnation,
        a_io: &LegIo,
        a_queue: &mut LegOutboundQueue,
        active_b: &mut Option<OwnerTargetRecoveryHandoff>,
        b_leg: &EstablishedLeg,
        _gate: &QuinnLegCompositionGate,
    ) -> Result<
        OwnerSessionRecoveryInstallRetirementDrive,
        OwnerSessionRecoveryInstallRetirementFailure,
    > {
        if let Err(error) = self.require_continuable() {
            return Err(OwnerSessionRecoveryInstallRetirementFailure { error, initial_a });
        }
        let Some(PendingOwnerAttachedRecovery::HandoffReady(replacement)) = self.recovery.as_ref()
        else {
            return Ok(OwnerSessionRecoveryInstallRetirementDrive::Inert(initial_a));
        };
        if active_b.is_some() {
            return Err(OwnerSessionRecoveryInstallRetirementFailure {
                error: OwnerSessionControllerError::new(
                    OwnerSessionControllerErrorKind::RecoveryHandoffOccupied,
                ),
                initial_a,
            });
        }
        if !self.executor.owns_recovery_handoff(replacement) {
            return Err(OwnerSessionRecoveryInstallRetirementFailure {
                error: OwnerSessionControllerError::new(
                    OwnerSessionControllerErrorKind::RecoveryHandoffWrongOwner,
                ),
                initial_a,
            });
        }
        if !replacement.belongs_to_transport(b_leg) {
            return Err(OwnerSessionRecoveryInstallRetirementFailure {
                error: OwnerSessionControllerError::new(
                    OwnerSessionControllerErrorKind::RecoveryHandoffWrongTransport,
                ),
                initial_a,
            });
        }
        let prepared = match self.executor.prepare_initial_retirement(
            initial_a,
            replacement,
            a_runtime,
            a_io,
            a_queue,
        ) {
            Ok(prepared) => prepared,
            Err(failure) => {
                let kind = failure.kind();
                return Err(OwnerSessionRecoveryInstallRetirementFailure {
                    error: OwnerSessionControllerError::new(
                        OwnerSessionControllerErrorKind::InitialRetirementRejected { kind },
                    ),
                    initial_a: failure.into_initial(),
                });
            }
        };

        let pending = self.recovery.take();
        let replacement = match pending {
            Some(PendingOwnerAttachedRecovery::HandoffReady(replacement)) => replacement,
            pending => {
                self.recovery = pending;
                return Err(OwnerSessionRecoveryInstallRetirementFailure {
                    error: OwnerSessionControllerError::new(
                        OwnerSessionControllerErrorKind::RecoveryHandoffRequired,
                    ),
                    initial_a: prepared.restore_initial(),
                });
            }
        };
        let generation = replacement.generation();
        *active_b = Some(replacement);
        let retired_a = prepared.commit_after_b_install();
        Ok(OwnerSessionRecoveryInstallRetirementDrive::Installed(
            OwnerSessionInstalledRecoveryWithRetiredInitial {
                generation,
                retired_a,
            },
        ))
    }

    /// Consumes only historical retired-A ownership.  Current B may already
    /// have terminalized into the controller; the executor's committed
    /// generation plus the frozen replacement identity remain authoritative.
    #[allow(
        clippy::result_large_err,
        reason = "clear rejection returns exact retired handoff and terminal"
    )]
    pub(super) fn clear_retired_initial_after_physical_join(
        &self,
        retired_a: OwnerTargetRetiredInitialHandoff,
        active_b: Option<&OwnerTargetRecoveryHandoff>,
        a_runtime: &PhysicalLegRuntimeIncarnation,
        terminal: Option<ExactLegTerminal>,
        _gate: &QuinnLegCompositionGate,
    ) -> Result<OwnerTargetRetiredInitialSettled, OwnerSessionRetiredInitialClearFailure> {
        match self
            .executor
            .settle_retired_initial_terminal(retired_a, active_b, a_runtime, terminal)
        {
            Ok(settled) => Ok(settled),
            Err(failure) => {
                let kind = OwnerSessionRetiredInitialClearErrorKind::Target(failure.kind());
                let (retired_a, terminal) = failure.into_parts();
                Err(OwnerSessionRetiredInitialClearFailure {
                    kind,
                    retired_a,
                    terminal,
                })
            }
        }
    }

    #[cfg(test)]
    fn begin_attached_recovery_for_test(
        &mut self,
        recovery: EnqueuedOwnerTargetAttach,
    ) -> Result<OwnerSessionRecoveryDrive, OwnerSessionControllerError> {
        self.begin_attached_recovery_inner(recovery)
    }

    #[cfg(test)]
    fn execute_attached_recovery_for_test(
        &mut self,
        queue: &LegOutboundQueue,
    ) -> Result<OwnerSessionRecoveryDrive, OwnerSessionControllerError> {
        self.execute_attached_recovery_inner(queue)
    }

    #[cfg(test)]
    fn fail_next_recovery_progress_for_test(&mut self) {
        self.fail_next_recovery_progress = true;
    }

    /// Fails once after C owns a real ordinary/continuation page or Target
    /// error and its original observations, after terminal preflight but
    /// before reducer observations are consumed. This is test-only and
    /// cannot bypass a production progress commit.
    #[cfg(test)]
    fn fail_next_continuation_progress_for_test(&mut self) {
        self.fail_next_continuation_progress = true;
    }

    #[cfg(test)]
    fn publish_attached_recovery_for_test(
        &mut self,
        queue: &mut LegOutboundQueue,
    ) -> Result<OwnerSessionRecoveryDrive, OwnerSessionControllerError> {
        self.publish_attached_recovery_inner(queue)
    }

    #[cfg(test)]
    fn install_recovery_handoff_for_test(
        &mut self,
        active: &mut Option<OwnerTargetRecoveryHandoff>,
        leg: &EstablishedLeg,
    ) -> Result<Option<LegGeneration>, OwnerSessionControllerError> {
        self.install_recovery_handoff_inner(active, leg)
    }

    #[cfg(test)]
    fn bind_recovery_terminal_for_test(
        &mut self,
        terminal: ExactLegTerminal,
    ) -> Result<OwnerSessionRecoveryTerminalBound, OwnerSessionRecoveryTerminalBindFailure> {
        self.bind_recovery_terminal_inner(terminal)
    }

    #[cfg(test)]
    fn discard_recovery_terminal_for_test(
        &mut self,
    ) -> Result<OwnerSessionRecoveryTerminalDiscarded, OwnerSessionControllerError> {
        self.discard_recovery_terminal_inner()
    }

    fn bind_recovery_terminal_physical_inner(
        &mut self,
        terminal: ExactLegTerminal,
        leg: &EstablishedLeg,
        queue: &mut LegOutboundQueue,
    ) -> Result<OwnerSessionRecoveryTerminalBound, OwnerSessionRecoveryTerminalBindFailure> {
        if self.legless_high_water.is_some() {
            return Err(OwnerSessionRecoveryTerminalBindFailure {
                kind: OwnerSessionRecoveryTerminalBindErrorKind::LeglessHighWaterPending,
                terminal,
            });
        }
        let Some(pending) = self.recovery.take() else {
            return Err(OwnerSessionRecoveryTerminalBindFailure {
                kind: OwnerSessionRecoveryTerminalBindErrorKind::NoPendingRecovery,
                terminal,
            });
        };
        let terminalized = match pending {
            PendingOwnerAttachedRecovery::Acceptance(publication) => self
                .executor
                .terminalize_pending_attach_before_acceptance_physical_exact(
                    publication,
                    terminal,
                    leg,
                    queue,
                )
                .map_err(|failure| {
                    let block = failure.block();
                    let (publication, terminal) = failure.into_parts();
                    self.recovery = Some(PendingOwnerAttachedRecovery::Acceptance(publication));
                    Self::recovery_terminal_bind_failure(block, terminal)
                }),
            PendingOwnerAttachedRecovery::Initial(recovery) => self
                .executor
                .terminalize_enqueued_attach_recovery_physical_exact(recovery, terminal, leg, queue)
                .map_err(|failure| {
                    let block = failure.block();
                    let (recovery, terminal) = failure.into_parts();
                    self.recovery = Some(PendingOwnerAttachedRecovery::Initial(recovery));
                    Self::recovery_terminal_bind_failure(block, terminal)
                }),
            PendingOwnerAttachedRecovery::Execute(recovery) => self
                .executor
                .terminalize_attached_recovery_resume_physical_exact(recovery, terminal, leg, queue)
                .map_err(|failure| {
                    let block = failure.block();
                    let (recovery, terminal) = failure.into_parts();
                    self.recovery = Some(PendingOwnerAttachedRecovery::Execute(recovery));
                    Self::recovery_terminal_bind_failure(block, terminal)
                }),
            PendingOwnerAttachedRecovery::Publish(publish) => self
                .executor
                .terminalize_attached_recovery_drain_physical_exact(publish, terminal, leg, queue)
                .map_err(|failure| {
                    let block = failure.block();
                    let (publish, terminal) = failure.into_parts();
                    self.recovery = Some(PendingOwnerAttachedRecovery::Publish(publish));
                    Self::recovery_terminal_bind_failure(block, terminal)
                }),
            PendingOwnerAttachedRecovery::HandoffReady(handoff) => self
                .executor
                .terminalize_recovery_handoff_physical_exact(handoff, terminal, leg, queue)
                .map_err(|failure| {
                    let block = failure.block();
                    let (handoff, terminal) = failure.into_parts();
                    self.recovery = Some(PendingOwnerAttachedRecovery::HandoffReady(handoff));
                    Self::recovery_terminal_bind_failure(block, terminal)
                }),
            PendingOwnerAttachedRecovery::TerminalBound(pending) => {
                self.recovery = Some(PendingOwnerAttachedRecovery::TerminalBound(pending));
                return Err(OwnerSessionRecoveryTerminalBindFailure {
                    kind: OwnerSessionRecoveryTerminalBindErrorKind::TerminalAlreadyBound,
                    terminal,
                });
            }
        }?;
        let identity = OwnerSessionRecoveryTerminalBound {
            generation: terminalized.generation(),
            reason: terminalized.reason(),
        };
        self.recovery = Some(PendingOwnerAttachedRecovery::TerminalBound(terminalized));
        #[cfg(test)]
        self.maybe_panic_after_terminal_bind_commit_for_test();
        Ok(identity)
    }

    fn bind_recovery_terminal_inner(
        &mut self,
        terminal: ExactLegTerminal,
    ) -> Result<OwnerSessionRecoveryTerminalBound, OwnerSessionRecoveryTerminalBindFailure> {
        if self.legless_high_water.is_some() {
            return Err(OwnerSessionRecoveryTerminalBindFailure {
                kind: OwnerSessionRecoveryTerminalBindErrorKind::LeglessHighWaterPending,
                terminal,
            });
        }
        let Some(pending) = self.recovery.take() else {
            return Err(OwnerSessionRecoveryTerminalBindFailure {
                kind: OwnerSessionRecoveryTerminalBindErrorKind::NoPendingRecovery,
                terminal,
            });
        };
        let terminalized = match pending {
            PendingOwnerAttachedRecovery::Acceptance(publication) => self
                .executor
                .terminalize_pending_attach_before_acceptance_exact(publication, terminal)
                .map_err(|failure| {
                    let block = failure.block();
                    let (publication, terminal) = failure.into_parts();
                    self.recovery = Some(PendingOwnerAttachedRecovery::Acceptance(publication));
                    Self::recovery_terminal_bind_failure(block, terminal)
                }),
            PendingOwnerAttachedRecovery::Initial(recovery) => self
                .executor
                .terminalize_enqueued_attach_recovery_exact(recovery, terminal)
                .map_err(|failure| {
                    let block = failure.block();
                    let (recovery, terminal) = failure.into_parts();
                    self.recovery = Some(PendingOwnerAttachedRecovery::Initial(recovery));
                    Self::recovery_terminal_bind_failure(block, terminal)
                }),
            PendingOwnerAttachedRecovery::Execute(recovery) => self
                .executor
                .terminalize_attached_recovery_resume_exact(recovery, terminal)
                .map_err(|failure| {
                    let block = failure.block();
                    let (recovery, terminal) = failure.into_parts();
                    self.recovery = Some(PendingOwnerAttachedRecovery::Execute(recovery));
                    Self::recovery_terminal_bind_failure(block, terminal)
                }),
            PendingOwnerAttachedRecovery::Publish(publish) => self
                .executor
                .terminalize_attached_recovery_drain_exact(publish, terminal)
                .map_err(|failure| {
                    let block = failure.block();
                    let (publish, terminal) = failure.into_parts();
                    self.recovery = Some(PendingOwnerAttachedRecovery::Publish(publish));
                    Self::recovery_terminal_bind_failure(block, terminal)
                }),
            PendingOwnerAttachedRecovery::HandoffReady(handoff) => self
                .executor
                .terminalize_recovery_handoff_exact(handoff, terminal)
                .map_err(|failure| {
                    let block = failure.block();
                    let (handoff, terminal) = failure.into_parts();
                    self.recovery = Some(PendingOwnerAttachedRecovery::HandoffReady(handoff));
                    Self::recovery_terminal_bind_failure(block, terminal)
                }),
            PendingOwnerAttachedRecovery::TerminalBound(pending) => {
                self.recovery = Some(PendingOwnerAttachedRecovery::TerminalBound(pending));
                return Err(OwnerSessionRecoveryTerminalBindFailure {
                    kind: OwnerSessionRecoveryTerminalBindErrorKind::TerminalAlreadyBound,
                    terminal,
                });
            }
        }?;
        let identity = OwnerSessionRecoveryTerminalBound {
            generation: terminalized.generation(),
            reason: terminalized.reason(),
        };
        self.recovery = Some(PendingOwnerAttachedRecovery::TerminalBound(terminalized));
        Ok(identity)
    }

    fn recovery_terminal_bind_failure(
        block: OwnerTargetInputBlock,
        terminal: ExactLegTerminal,
    ) -> OwnerSessionRecoveryTerminalBindFailure {
        let kind = if block == OwnerTargetInputBlock::WrongLegTerminal {
            OwnerSessionRecoveryTerminalBindErrorKind::WrongLegTerminal
        } else {
            OwnerSessionRecoveryTerminalBindErrorKind::Rejected(block)
        };
        OwnerSessionRecoveryTerminalBindFailure { kind, terminal }
    }

    fn discard_recovery_terminal_inner(
        &mut self,
    ) -> Result<OwnerSessionRecoveryTerminalDiscarded, OwnerSessionControllerError> {
        if self.legless_high_water.is_some() {
            return Err(OwnerSessionControllerError::new(
                OwnerSessionControllerErrorKind::RecoveryTerminalDiscardRequired,
            ));
        }
        let Some(pending) = self.recovery.take() else {
            return Err(OwnerSessionControllerError::new(
                OwnerSessionControllerErrorKind::NoPendingRecovery,
            ));
        };
        let PendingOwnerAttachedRecovery::TerminalBound(pending) = pending else {
            self.recovery = Some(pending);
            return Err(OwnerSessionControllerError::new(
                OwnerSessionControllerErrorKind::RecoveryTerminalDiscardRequired,
            ));
        };
        let identity = OwnerSessionRecoveryTerminalBound {
            generation: pending.generation(),
            reason: pending.reason(),
        };
        match self
            .executor
            .discard_terminal_attach_recovery_exact(pending)
        {
            Ok(high_water) => {
                self.legless_high_water = Some(PendingOwnerLeglessHighWater { high_water });
                Ok(OwnerSessionRecoveryTerminalDiscarded { identity })
            }
            Err(OwnerTerminalAttachDiscardFailure::Blocked { block, pending }) => {
                self.recovery = Some(PendingOwnerAttachedRecovery::TerminalBound(pending));
                Err(OwnerSessionControllerError::new(
                    OwnerSessionControllerErrorKind::RejectedRecoveryTerminalDiscard { block },
                ))
            }
            Err(OwnerTerminalAttachDiscardFailure::Quarantined(quarantine)) => {
                self.phase = OwnerSessionControllerPhase::Failed;
                self.terminal_quarantine = Some(quarantine);
                Err(OwnerSessionControllerError::new(
                    OwnerSessionControllerErrorKind::RecoveryTerminalQuarantined,
                ))
            }
        }
    }

    fn install_recovery_handoff_inner(
        &mut self,
        active: &mut Option<OwnerTargetRecoveryHandoff>,
        leg: &EstablishedLeg,
    ) -> Result<Option<LegGeneration>, OwnerSessionControllerError> {
        self.require_continuable()?;
        let Some(pending) = self.recovery.take() else {
            return Ok(None);
        };
        let PendingOwnerAttachedRecovery::HandoffReady(handoff) = pending else {
            self.recovery = Some(pending);
            return Ok(None);
        };
        if active.is_some() {
            self.recovery = Some(PendingOwnerAttachedRecovery::HandoffReady(handoff));
            return Err(OwnerSessionControllerError::new(
                OwnerSessionControllerErrorKind::RecoveryHandoffOccupied,
            ));
        }
        if !self.executor.owns_recovery_handoff(&handoff) {
            self.recovery = Some(PendingOwnerAttachedRecovery::HandoffReady(handoff));
            return Err(OwnerSessionControllerError::new(
                OwnerSessionControllerErrorKind::RecoveryHandoffWrongOwner,
            ));
        }
        if !handoff.belongs_to_transport(leg) {
            self.recovery = Some(PendingOwnerAttachedRecovery::HandoffReady(handoff));
            return Err(OwnerSessionControllerError::new(
                OwnerSessionControllerErrorKind::RecoveryHandoffWrongTransport,
            ));
        }
        let generation = handoff.generation();
        *active = Some(handoff);
        Ok(Some(generation))
    }

    fn begin_attached_recovery_inner(
        &mut self,
        recovery: EnqueuedOwnerTargetAttach,
    ) -> Result<OwnerSessionRecoveryDrive, OwnerSessionControllerError> {
        if !self.executor.owns_enqueued_attached_recovery(&recovery) {
            return Err(OwnerSessionControllerError::new(
                OwnerSessionControllerErrorKind::WrongRecoveryOwner {
                    recovery: Box::new(recovery),
                },
            ));
        }
        if self.phase != OwnerSessionControllerPhase::Active
            || self.pending_continuation.is_some()
            || self.recovery.is_some()
            || self.attach_status.is_some()
            || self.aborted_turn.is_some()
            || self.aborted_recovery.is_some()
        {
            return Err(OwnerSessionControllerError::new(
                OwnerSessionControllerErrorKind::RejectedRecoveryStart {
                    recovery: Box::new(recovery),
                },
            ));
        }
        self.recovery = Some(PendingOwnerAttachedRecovery::Initial(recovery));
        Ok(self.recovery_drive(Vec::new()))
    }

    fn execute_attached_recovery_inner(
        &mut self,
        queue: &LegOutboundQueue,
    ) -> Result<OwnerSessionRecoveryDrive, OwnerSessionControllerError> {
        self.require_continuable()?;
        let observed_at = self.clock.now().map_err(|source| {
            OwnerSessionControllerError::new(OwnerSessionControllerErrorKind::Clock(source))
        })?;
        let Some(pending) = self.recovery.take() else {
            return Err(OwnerSessionControllerError::new(
                OwnerSessionControllerErrorKind::NoPendingRecovery,
            ));
        };
        let result = match pending {
            acceptance @ PendingOwnerAttachedRecovery::Acceptance(_) => {
                self.recovery = Some(acceptance);
                return Err(OwnerSessionControllerError::new(
                    OwnerSessionControllerErrorKind::RecoveryAcceptanceRequired,
                ));
            }
            PendingOwnerAttachedRecovery::Initial(recovery) => self
                .executor
                .execute_attached_recovery_observed(recovery, queue),
            PendingOwnerAttachedRecovery::Execute(recovery) => self
                .executor
                .resume_attached_recovery_effects_observed(recovery, queue),
            publish @ PendingOwnerAttachedRecovery::Publish(_) => {
                self.recovery = Some(publish);
                return Err(OwnerSessionControllerError::new(
                    OwnerSessionControllerErrorKind::RecoveryPublishRequired,
                ));
            }
            handoff @ PendingOwnerAttachedRecovery::HandoffReady(_) => {
                self.recovery = Some(handoff);
                return Err(OwnerSessionControllerError::new(
                    OwnerSessionControllerErrorKind::RecoveryHandoffRequired,
                ));
            }
            terminal @ PendingOwnerAttachedRecovery::TerminalBound(_) => {
                self.recovery = Some(terminal);
                return Err(OwnerSessionControllerError::new(
                    OwnerSessionControllerErrorKind::RecoveryTerminalDiscardRequired,
                ));
            }
        };
        match result {
            Ok(page) => self.finish_recovery_execute_page(page, observed_at),
            Err(OwnerTargetError::RejectedAttachedRecovery { block, recovery }) => {
                self.recovery = Some(PendingOwnerAttachedRecovery::Initial(*recovery));
                Err(OwnerSessionControllerError::new(
                    OwnerSessionControllerErrorKind::RejectedRecoveryExecute { block },
                ))
            }
            Err(OwnerTargetError::RejectedAttachedRecoveryResume { block, recovery }) => {
                self.recovery = Some(PendingOwnerAttachedRecovery::Execute(*recovery));
                Err(OwnerSessionControllerError::new(
                    OwnerSessionControllerErrorKind::RejectedRecoveryExecute { block },
                ))
            }
            Err(error) => {
                let error =
                    self.finish_target_error(error, observed_at, OwnerContinuationOrigin::Local);
                self.phase = OwnerSessionControllerPhase::Failed;
                Err(error)
            }
        }
    }

    fn finish_recovery_execute_page(
        &mut self,
        page: OwnerTargetObservedRecoveryPage,
        observed_at: SimTime,
    ) -> Result<OwnerSessionRecoveryDrive, OwnerSessionControllerError> {
        let (publish, observations) = page.into_progress_parts();
        #[cfg(test)]
        if std::mem::take(&mut self.fail_next_recovery_progress) {
            return self.abort_recovery_progress(
                publish,
                observations,
                OwnerSessionProgressError::MissingActorArrival,
            );
        }
        if let Err(source) = self.consume_observations(&observations, observed_at, None) {
            return self.abort_recovery_progress(publish, observations, source);
        }
        self.recovery = Some(PendingOwnerAttachedRecovery::Publish(publish));
        Ok(self.recovery_drive(Vec::new()))
    }

    fn abort_recovery_progress(
        &mut self,
        publish: OwnerTargetRecoveryPublishCursor,
        observations: OwnerTargetReducerObservations,
        source: OwnerSessionProgressError,
    ) -> Result<OwnerSessionRecoveryDrive, OwnerSessionControllerError> {
        self.phase = OwnerSessionControllerPhase::Failed;
        self.aborted_recovery = Some(AbortedOwnerRecoveryTurn {
            pending: Some(PendingOwnerAttachedRecovery::Publish(publish)),
            outputs: Vec::new(),
            observations: Some(observations),
        });
        Err(OwnerSessionControllerError::new(
            OwnerSessionControllerErrorKind::PostCommitProgress(source),
        ))
    }

    fn publish_attached_recovery_inner(
        &mut self,
        queue: &mut LegOutboundQueue,
    ) -> Result<OwnerSessionRecoveryDrive, OwnerSessionControllerError> {
        self.require_continuable()?;
        let Some(pending) = self.recovery.take() else {
            return Err(OwnerSessionControllerError::new(
                OwnerSessionControllerErrorKind::NoPendingRecovery,
            ));
        };
        let publish = match pending {
            PendingOwnerAttachedRecovery::Publish(publish) => publish,
            acceptance @ PendingOwnerAttachedRecovery::Acceptance(_) => {
                self.recovery = Some(acceptance);
                return Err(OwnerSessionControllerError::new(
                    OwnerSessionControllerErrorKind::RecoveryAcceptanceRequired,
                ));
            }
            execute @ (PendingOwnerAttachedRecovery::Initial(_)
            | PendingOwnerAttachedRecovery::Execute(_)) => {
                self.recovery = Some(execute);
                return Err(OwnerSessionControllerError::new(
                    OwnerSessionControllerErrorKind::RecoveryExecuteRequired,
                ));
            }
            handoff @ PendingOwnerAttachedRecovery::HandoffReady(_) => {
                self.recovery = Some(handoff);
                return Err(OwnerSessionControllerError::new(
                    OwnerSessionControllerErrorKind::RecoveryHandoffRequired,
                ));
            }
            terminal @ PendingOwnerAttachedRecovery::TerminalBound(_) => {
                self.recovery = Some(terminal);
                return Err(OwnerSessionControllerError::new(
                    OwnerSessionControllerErrorKind::RecoveryTerminalDiscardRequired,
                ));
            }
        };
        match self.executor.publish_attached_recovery_page(publish, queue) {
            Ok(outputs) => self.finish_recovery_publication(outputs),
            Err(OwnerTargetError::RejectedAttachedRecoveryDrain { block, drain }) => {
                self.recovery = Some(PendingOwnerAttachedRecovery::Publish(*drain));
                Err(OwnerSessionControllerError::new(
                    OwnerSessionControllerErrorKind::RejectedRecoveryPublish { block },
                ))
            }
            Err(OwnerTargetError::AttachedRecoveryQueueBlocked { kind, drain }) => {
                self.recovery = Some(PendingOwnerAttachedRecovery::Publish(*drain));
                Err(OwnerSessionControllerError::new(
                    OwnerSessionControllerErrorKind::RecoveryQueueBlocked { kind },
                ))
            }
            Err(OwnerTargetError::AttachedRecoveryOutputInvariant { drain }) => {
                self.phase = OwnerSessionControllerPhase::Failed;
                self.aborted_recovery = Some(AbortedOwnerRecoveryTurn {
                    pending: Some(PendingOwnerAttachedRecovery::Publish(*drain)),
                    outputs: Vec::new(),
                    observations: None,
                });
                Err(OwnerSessionControllerError::new(
                    OwnerSessionControllerErrorKind::InvalidRecoveryPage,
                ))
            }
            Err(error) => {
                self.phase = OwnerSessionControllerPhase::Failed;
                Err(OwnerSessionControllerError::new(
                    OwnerSessionControllerErrorKind::Target(error),
                ))
            }
        }
    }

    fn finish_recovery_publication(
        &mut self,
        mut outputs: Vec<OwnerTargetRecoveryOutput>,
    ) -> Result<OwnerSessionRecoveryDrive, OwnerSessionControllerError> {
        let capability_count = outputs
            .iter()
            .filter(|output| {
                matches!(
                    output,
                    OwnerTargetRecoveryOutput::NeedsResume { .. }
                        | OwnerTargetRecoveryOutput::RecoveryCompleted { .. }
                )
            })
            .count();
        let tail_is_capability = matches!(
            outputs.last(),
            Some(
                OwnerTargetRecoveryOutput::NeedsResume { .. }
                    | OwnerTargetRecoveryOutput::RecoveryCompleted { .. }
            )
        );
        if capability_count != 1 || !tail_is_capability {
            return self.abort_recovery_publication(outputs);
        }
        let Some(tail) = outputs.pop() else {
            return self.abort_recovery_publication(outputs);
        };
        let mut safe = Vec::with_capacity(outputs.len());
        for output in outputs {
            match output {
                OwnerTargetRecoveryOutput::LegActivated { generation } => {
                    safe.push(OwnerSessionRecoveryOutput::LegActivated { generation });
                }
                OwnerTargetRecoveryOutput::ResumeGraceStarted { generation } => {
                    safe.push(OwnerSessionRecoveryOutput::ResumeGraceStarted { generation });
                }
                OwnerTargetRecoveryOutput::TerminalGraceStarted { terminal } => {
                    safe.push(OwnerSessionRecoveryOutput::TerminalGraceStarted { terminal });
                }
                OwnerTargetRecoveryOutput::SessionExpired => {
                    safe.push(OwnerSessionRecoveryOutput::SessionExpired);
                }
                capability @ (OwnerTargetRecoveryOutput::NeedsResume { .. }
                | OwnerTargetRecoveryOutput::RecoveryCompleted { .. }) => {
                    let mut retained = safe
                        .into_iter()
                        .map(Self::restore_recovery_output)
                        .collect::<Vec<_>>();
                    retained.push(capability);
                    retained.push(tail);
                    return self.abort_recovery_publication(retained);
                }
            }
        }
        match tail {
            OwnerTargetRecoveryOutput::NeedsResume { resume } => {
                self.recovery = Some(PendingOwnerAttachedRecovery::Execute(resume));
                Ok(self.recovery_drive(safe))
            }
            OwnerTargetRecoveryOutput::RecoveryCompleted { handoff } => {
                self.recovery = Some(PendingOwnerAttachedRecovery::HandoffReady(handoff));
                Ok(self.recovery_drive(safe))
            }
            safe_tail => {
                let mut retained = safe
                    .into_iter()
                    .map(Self::restore_recovery_output)
                    .collect::<Vec<_>>();
                retained.push(safe_tail);
                self.abort_recovery_publication(retained)
            }
        }
    }

    fn restore_recovery_output(output: OwnerSessionRecoveryOutput) -> OwnerTargetRecoveryOutput {
        match output {
            OwnerSessionRecoveryOutput::LegActivated { generation } => {
                OwnerTargetRecoveryOutput::LegActivated { generation }
            }
            OwnerSessionRecoveryOutput::ResumeGraceStarted { generation } => {
                OwnerTargetRecoveryOutput::ResumeGraceStarted { generation }
            }
            OwnerSessionRecoveryOutput::TerminalGraceStarted { terminal } => {
                OwnerTargetRecoveryOutput::TerminalGraceStarted { terminal }
            }
            OwnerSessionRecoveryOutput::SessionExpired => OwnerTargetRecoveryOutput::SessionExpired,
        }
    }

    fn abort_recovery_publication(
        &mut self,
        outputs: Vec<OwnerTargetRecoveryOutput>,
    ) -> Result<OwnerSessionRecoveryDrive, OwnerSessionControllerError> {
        self.phase = OwnerSessionControllerPhase::Failed;
        self.aborted_recovery = Some(AbortedOwnerRecoveryTurn {
            pending: None,
            outputs,
            observations: None,
        });
        Err(OwnerSessionControllerError::new(
            OwnerSessionControllerErrorKind::InvalidRecoveryPage,
        ))
    }

    fn recovery_drive(
        &self,
        outputs: Vec<OwnerSessionRecoveryOutput>,
    ) -> OwnerSessionRecoveryDrive {
        OwnerSessionRecoveryDrive {
            outputs,
            needs_execute: matches!(
                self.recovery,
                Some(
                    PendingOwnerAttachedRecovery::Initial(_)
                        | PendingOwnerAttachedRecovery::Execute(_)
                )
            ),
            needs_publish: matches!(
                self.recovery,
                Some(PendingOwnerAttachedRecovery::Publish(_))
            ),
            handoff_ready: matches!(
                self.recovery,
                Some(PendingOwnerAttachedRecovery::HandoffReady(_))
            ),
        }
    }
    pub(crate) fn try_read_target(
        &mut self,
        flow_id: SessionFlowId,
    ) -> Result<OwnerSessionDrive, OwnerSessionControllerError> {
        self.require_active()?;
        self.require_no_recovery()?;
        if self.pending_continuation.is_some() {
            return Err(OwnerSessionControllerError::new(
                OwnerSessionControllerErrorKind::ContinueRequired,
            ));
        }
        let observed_at = self.begin_terminal_grace_turn().map_err(|source| {
            OwnerSessionControllerError::new(OwnerSessionControllerErrorKind::Clock(source))
        })?;
        match self.executor.try_read_target_observed(flow_id) {
            Ok(observed) => {
                self.finish_observed_page(observed, observed_at, OwnerContinuationOrigin::Local)
            }
            Err(error) => {
                Err(self.finish_target_error(error, observed_at, OwnerContinuationOrigin::Local))
            }
        }
    }
    /// Retries one retained Target sink offer through the same observation
    /// consumer as actor and Target-read turns. Root supplies only readiness;
    /// any resulting executor continuation remains controller-owned.
    pub(crate) fn retry_target_write(
        &mut self,
        flow_id: SessionFlowId,
    ) -> Result<OwnerSessionDrive, OwnerSessionControllerError> {
        self.require_active()?;
        self.require_no_recovery()?;
        if self.pending_continuation.is_some() {
            return Err(OwnerSessionControllerError::new(
                OwnerSessionControllerErrorKind::ContinueRequired,
            ));
        }
        let observed_at = self.begin_terminal_grace_turn().map_err(|source| {
            OwnerSessionControllerError::new(OwnerSessionControllerErrorKind::Clock(source))
        })?;
        match self.executor.retry_target_write_observed(flow_id) {
            Ok(observed) => {
                self.finish_observed_page(observed, observed_at, OwnerContinuationOrigin::Local)
            }
            Err(error) => {
                Err(self.finish_target_error(error, observed_at, OwnerContinuationOrigin::Local))
            }
        }
    }
    /// Executes exactly one bounded continuation page under the output root's
    /// exact per-turn authority. No production caller can mint this permit or
    /// erase the ownership disposition into a plain `Result`.
    pub(super) fn continue_effects(
        &mut self,
        _permit: QuinnOwnerContinuationPermit<'_>,
    ) -> OwnerSessionContinuationOutcome {
        self.continue_effects_inner()
    }
    /// The non-cloneable raw resume stays in this Module across every
    /// pre-take failure and rejected executor resume. Every return variant
    /// explicitly settles whether the exact capability advanced, remains
    /// retryable, remains only for terminal settlement, was aborted, or was
    /// missing.
    fn continue_effects_inner(&mut self) -> OwnerSessionContinuationOutcome {
        if let Err(error) = self.require_continuable() {
            return self.classify_unstarted_continuation(error);
        }
        if let Err(error) = self.require_no_recovery() {
            return self.classify_unstarted_continuation(error);
        }
        let observed_at = match self.begin_terminal_grace_turn() {
            Ok(observed_at) => observed_at,
            Err(source) => {
                return self.classify_unstarted_continuation(OwnerSessionControllerError::new(
                    OwnerSessionControllerErrorKind::Clock(source),
                ));
            }
        };
        let Some(pending) = self.pending_continuation.take() else {
            self.finish_terminal_grace_turn();
            return OwnerSessionContinuationOutcome::Missing(OwnerSessionControllerError::new(
                OwnerSessionControllerErrorKind::NoPendingContinuation,
            ));
        };
        let PendingOwnerContinuation { resume, origin } = pending;
        match self.executor.resume_pending_effects_observed(resume) {
            Ok(observed) => match self.finish_observed_page(observed, observed_at, origin) {
                Ok(drive) => OwnerSessionContinuationOutcome::Advanced(drive),
                Err(error) => OwnerSessionContinuationOutcome::Aborted(error),
            },
            Err(OwnerTargetError::RejectedEffectResume { block, resume }) => {
                self.finish_terminal_grace_turn();
                self.pending_continuation = Some(PendingOwnerContinuation {
                    resume: *resume,
                    origin,
                });
                OwnerSessionContinuationOutcome::TerminalRetained(OwnerSessionControllerError::new(
                    OwnerSessionControllerErrorKind::RejectedContinuation { block },
                ))
            }
            Err(error) => {
                let error = self.finish_target_error(error, observed_at, origin);
                if self.phase == OwnerSessionControllerPhase::Active {
                    self.phase = OwnerSessionControllerPhase::Failed;
                }
                OwnerSessionContinuationOutcome::Aborted(error)
            }
        }
    }
    fn classify_unstarted_continuation(
        &self,
        error: OwnerSessionControllerError,
    ) -> OwnerSessionContinuationOutcome {
        if self.pending_continuation.is_some() {
            // No production bounded wake/order seam presently proves that a
            // pre-take failure can be retried without spinning or violating
            // the controller/output transaction. Preserve exact ownership,
            // but remove it from live scheduling until terminal settlement.
            OwnerSessionContinuationOutcome::TerminalRetained(error)
        } else {
            OwnerSessionContinuationOutcome::Missing(error)
        }
    }
    /// Test-only adapter that exposes the closed disposition without granting
    /// production code an unscoped continuation capability.
    #[cfg(test)]
    fn continue_effects_for_test(&mut self) -> OwnerSessionContinuationOutcome {
        self.continue_effects_inner()
    }
    /// Compatibility adapter for transport-independent controller fixtures.
    /// Production composition cannot erase the continuation disposition back
    /// into a plain `Result`.
    #[cfg(test)]
    fn continue_effects_result_for_test(
        &mut self,
    ) -> Result<OwnerSessionDrive, OwnerSessionControllerError> {
        self.continue_effects_for_test().into_result_for_test()
    }
    pub(super) fn apply_actor_event(
        &mut self,
        input: OwnerSessionActorEvent,
    ) -> Result<OwnerSessionDrive, OwnerSessionControllerError> {
        if self.terminal_grace.has_turn() {
            self.phase = OwnerSessionControllerPhase::Failed;
        }
        if self.phase != OwnerSessionControllerPhase::Active {
            return Err(OwnerSessionControllerError::new(
                OwnerSessionControllerErrorKind::InactiveActorEvent {
                    phase: self.phase,
                    input: Box::new(input),
                },
            ));
        }
        if !input.arrival.same_clock(&self.clock) {
            return Err(OwnerSessionControllerError::new(
                OwnerSessionControllerErrorKind::WrongActorClock {
                    input: Box::new(input),
                },
            ));
        }
        if self.recovery.is_some() {
            return Err(OwnerSessionControllerError::new(
                OwnerSessionControllerErrorKind::RejectedActorEvent {
                    block: OwnerTargetInputBlock::AttachBarrierPending,
                    input: Box::new(input),
                },
            ));
        }
        if self.pending_continuation.is_some() {
            return Err(OwnerSessionControllerError::new(
                OwnerSessionControllerErrorKind::RejectedActorEvent {
                    block: OwnerTargetInputBlock::PendingEffectTurn,
                    input: Box::new(input),
                },
            ));
        }
        let observed_at = match self.begin_terminal_grace_turn() {
            Ok(observed_at) => observed_at,
            Err(source) => {
                return Err(OwnerSessionControllerError::new(
                    OwnerSessionControllerErrorKind::ActorClock {
                        source,
                        input: Box::new(input),
                    },
                ));
            }
        };
        let arrived_at = input.arrival.arrived_at();
        let OwnerSessionActorEvent { event, arrival } = input;
        match self.executor.apply_event_observed(*event) {
            Ok(observed) => {
                drop(arrival);
                self.finish_observed_page(
                    observed,
                    observed_at,
                    OwnerContinuationOrigin::Actor(arrived_at),
                )
            }
            Err(OwnerTargetError::RejectedEvent { block, event }) => {
                self.finish_terminal_grace_turn();
                Err(OwnerSessionControllerError::new(
                    OwnerSessionControllerErrorKind::RejectedActorEvent {
                        block,
                        input: Box::new(OwnerSessionActorEvent { event, arrival }),
                    },
                ))
            }
            Err(error) => {
                drop(arrival);
                Err(self.finish_target_error(
                    error,
                    observed_at,
                    OwnerContinuationOrigin::Actor(arrived_at),
                ))
            }
        }
    }
    #[cfg(test)]
    fn target_mut_for_test(&mut self) -> &mut T {
        self.executor.target_mut()
    }
    fn require_active(&self) -> Result<(), OwnerSessionControllerError> {
        if self.terminal_grace.has_turn() {
            return Err(OwnerSessionControllerError::new(
                OwnerSessionControllerErrorKind::Failed,
            ));
        }
        match self.phase {
            OwnerSessionControllerPhase::Active => Ok(()),
            OwnerSessionControllerPhase::SessionExpiring
            | OwnerSessionControllerPhase::SessionExpired => Err(OwnerSessionControllerError::new(
                OwnerSessionControllerErrorKind::SessionExpired,
            )),
            OwnerSessionControllerPhase::Failed => Err(OwnerSessionControllerError::new(
                OwnerSessionControllerErrorKind::Failed,
            )),
        }
    }
    fn require_no_recovery(&self) -> Result<(), OwnerSessionControllerError> {
        if self.recovery.is_some() {
            return Err(OwnerSessionControllerError::new(
                OwnerSessionControllerErrorKind::RecoveryRequired,
            ));
        }
        Ok(())
    }
    fn require_continuable(&self) -> Result<(), OwnerSessionControllerError> {
        if self.terminal_grace.has_turn() {
            return Err(OwnerSessionControllerError::new(
                OwnerSessionControllerErrorKind::Failed,
            ));
        }
        match self.phase {
            OwnerSessionControllerPhase::Active | OwnerSessionControllerPhase::SessionExpiring => {
                Ok(())
            }
            OwnerSessionControllerPhase::SessionExpired => Err(OwnerSessionControllerError::new(
                OwnerSessionControllerErrorKind::SessionExpired,
            )),
            OwnerSessionControllerPhase::Failed => Err(OwnerSessionControllerError::new(
                OwnerSessionControllerErrorKind::Failed,
            )),
        }
    }
}
#[cfg(test)]
mod tests;
