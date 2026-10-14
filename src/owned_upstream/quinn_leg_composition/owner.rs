//! Closed owner composition: actor, registered slot, probe and freshness stay local.
//! Arrival is actor-captured, never caller-selected, under the synchronous gate.
//! Value-only cold aggregation lives in inventory; resources remain here.
mod continuation;
mod inventory;
#[cfg(test)]
mod physical_root;
mod physical_session_root;
mod server_root;
pub(in crate::owned_upstream) use continuation::QuinnOwnerContinuationPermit;
#[cfg(test)]
pub(crate) use physical_root::QuinnOwnerRecoverySessionComposition;

use std::{fmt, sync::Arc};
use thiserror::Error;

use self::continuation::{
    QuinnOwnerControllerOutputInboundTransaction, QuinnOwnerControllerOutputRoot,
    QuinnOwnerOutputContinuationDrive, QuinnOwnerOutputContinuationTransaction,
};
#[cfg(test)]
use self::continuation::{
    QuinnOwnerControllerOutputTerminalBeginError, QuinnOwnerControllerOutputTerminalCallbackDrive,
    QuinnOwnerControllerOutputTerminalObservation,
};
use super::{QuinnLegCompositionGate, QuinnStandbyLivenessConfig};
use crate::owned_upstream::leg::{BoundInbound, LegTransportTerminalReason};
use crate::owned_upstream::leg_io::{LegOutboundQueue, LegOutboundQueueColdInventory};
use crate::owned_upstream::owner_output_drain::{
    OwnerOutputAdmissionFailure, OwnerOutputAdmissionInventory, OwnerOutputDrain,
    OwnerOutputDrainAuditError, OwnerOutputDrainBlock, OwnerOutputDrainConfigError,
    OwnerOutputDrainDrive, OwnerOutputDrainInventory, OwnerOutputDrainStopReason,
    OwnerOutputLifecycleAction, OwnerOutputLifecycleDrive, OwnerOutputLifecycleInventory,
    OwnerOutputLifecyclePort, OwnerOutputLifecycleResolution, OwnerOutputLifecycleSettlement,
    OwnerOutputLifecycleTransaction, OwnerOutputPageDrained,
};
use crate::owned_upstream::owner_session_controller::{
    OwnerSessionController, OwnerSessionControllerSnapshot, OwnerSessionDrive,
    OwnerSessionRecoveryDrive, OwnerSessionRecoveryTerminalBindErrorKind,
    OwnerSessionRegisteredAttachDrive,
};
use crate::owned_upstream::owner_target::error_inventory::TargetErrorColdAudit;
use crate::owned_upstream::owner_target::{OwnerTargetOutput, OwnerTargetRecoveryHandoff};
#[cfg(test)]
use crate::owned_upstream::quinn_leg_actor::QuinnLegActorJoinBlock;
use crate::owned_upstream::quinn_leg_actor::{
    QuinnLegActorArrivalDeadline, QuinnLegActorError, QuinnLegActorFlushDisposition,
    QuinnLegActorInboundDecision, QuinnLegActorInboundPoll, QuinnLegActorInboundPreflight,
    QuinnLegActorInboundTimerPoll, QuinnLegActorJoined, QuinnLegActorReportedTerminalPoll,
    QuinnLegActorSemanticTimer, QuinnLegActorTimerDecision, QuinnOwnerActorArrival,
    StartedQuinnLegActor,
};
use crate::owned_upstream::session_clock::{SessionClockError, SessionMonotonicClock};
use crate::owned_upstream::standby::{
    OwnerStandbyProbeAckErrorKind, OwnerStandbyProbeArmDrive, OwnerStandbyProbeArmError,
    OwnerStandbyProbeDrive, OwnerStandbyProbeDriveError, OwnerStandbyProbeExpiryDrive,
    OwnerStandbyProbeExpiryError, OwnerStandbyRetirementInventory, OwnerStandbySlot,
    RetiredOwnerStandby,
};
use crate::owned_upstream::target::TargetIo;
use crate::owned_upstream::two_leg::{SimTime, SimTimeError};
use crate::resumable::{LegGeneration, ProbeSequence, STANDBY_CONTROL_V1};

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub(crate) enum QuinnOwnerStandbyAdoptErrorKind {
    #[error("registered standby does not belong to the exact physical actor and FIFO")]
    WrongPhysicalStandby,
}

/// Failed adoption returns both non-cloneable owners unchanged.
pub(crate) struct QuinnOwnerStandbyAdoptFailure {
    kind: QuinnOwnerStandbyAdoptErrorKind,
    ownership: Box<(StartedQuinnLegActor, OwnerStandbySlot)>,
}

impl QuinnOwnerStandbyAdoptFailure {
    pub(crate) const fn kind(&self) -> QuinnOwnerStandbyAdoptErrorKind {
        self.kind
    }

    #[allow(
        dead_code,
        reason = "the production owner root will own adoption retry"
    )]
    pub(crate) fn into_parts(self) -> (StartedQuinnLegActor, OwnerStandbySlot) {
        *self.ownership
    }
}

impl fmt::Debug for QuinnOwnerStandbyAdoptFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QuinnOwnerStandbyAdoptFailure")
            .field("kind", &self.kind)
            .field("ownership", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QuinnOwnerStandbyProbeDrive {
    Enqueued { probe_sequence: ProbeSequence },
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub(crate) enum QuinnOwnerStandbyProbeDriveError {
    #[error("physical owner standby actor is unavailable")]
    ActorUnavailable,
    #[error("owner standby probe sequence is exhausted")]
    ProbeSequenceExhausted,
    #[error("owner standby endpoint clock is invalid: {0}")]
    Clock(#[from] SessionClockError),
    #[error("owner standby probe state rejected the drive: {0}")]
    Standby(#[from] OwnerStandbyProbeDriveError),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QuinnOwnerStandbyProbeArmDrive {
    NotDelivered,
    Armed { deadline: SimTime },
    AlreadyArmed { deadline: SimTime },
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub(crate) enum QuinnOwnerStandbyProbeArmError {
    #[error("owner standby endpoint clock is invalid: {0}")]
    EndpointClock(#[from] SessionClockError),
    #[error("owner standby probe timeout must be nonzero")]
    InvalidTimeout,
    #[error("owner standby probe endpoint or queue was lost before arming")]
    EndpointOrQueueLost,
    #[error("owner standby probe deadline clock is invalid: {0}")]
    DeadlineClock(SimTimeError),
    #[error("owner standby probe is not awaiting writer delivery")]
    WrongState,
    #[error("owner standby probe timer disagrees with its exact slot correlation")]
    CompositionInvariant,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QuinnOwnerStandbyProbeExpiryDrive {
    Inert,
    Pending {
        probe_sequence: ProbeSequence,
        deadline: SimTime,
    },
    InboundPending {
        probe_sequence: ProbeSequence,
        deadline: SimTime,
    },
    PublicationPending {
        probe_sequence: ProbeSequence,
        deadline: SimTime,
    },
    Expired {
        probe_sequence: ProbeSequence,
        deadline: SimTime,
    },
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub(crate) enum QuinnOwnerStandbyProbeExpiryError {
    #[error("physical owner standby endpoint clock is invalid: {0}")]
    EndpointClock(#[from] SessionClockError),
    #[error("physical owner standby actor is unavailable")]
    ActorUnavailable,
    #[error("owner standby probe timer disagrees with its exact slot correlation")]
    CompositionInvariant,
}

#[derive(Debug)]
pub(crate) enum QuinnOwnerStandbyInboundDrive {
    Idle,
    ProbeFresh {
        probe_sequence: ProbeSequence,
        fresh_until: SimTime,
    },
    Rejected(OwnerStandbyProbeAckErrorKind),
    CompositionInvariant,
    ActorFailed(QuinnLegActorError),
}

/// One bounded physical B owner. Probe state is O(1): the registered slot
/// retains at most one pending/awaiting/fresh probe and this composition owns
/// exactly one next sequence.
pub(crate) struct QuinnOwnerStandbySessionComposition {
    actor: Option<QuinnOwnerStandbyActorOwner>,
    slot: Option<OwnerStandbySlot>,
    clock: SessionMonotonicClock,
    liveness: QuinnStandbyLivenessConfig,
    next_probe_sequence: Option<ProbeSequence>,
    armed_probe_timer: Option<QuinnOwnerStandbyProbeTimer>,
    retirement: QuinnOwnerStandbyRetirementState,
}

/// One closed retirement ledger replaces the former independent retired-slot
/// and queue-audit Options. A physical terminal cannot publish only one half.
enum QuinnOwnerStandbyRetirementState {
    Unsettled,
    PhysicalPreJoin {
        reason: LegTransportTerminalReason,
        retired: RetiredOwnerStandby,
        queue_discarded: LegOutboundQueueColdInventory,
    },
    PhysicalPostJoin {
        reason: LegTransportTerminalReason,
        retired: RetiredOwnerStandby,
        queue_discarded: LegOutboundQueueColdInventory,
    },
    ShutdownPostJoin {
        reason: LegTransportTerminalReason,
        retired: RetiredOwnerStandby,
    },
    Quarantined,
}

impl QuinnOwnerStandbyRetirementState {
    const fn is_unsettled(&self) -> bool {
        matches!(self, Self::Unsettled)
    }

    const fn physical_reason(&self) -> Option<LegTransportTerminalReason> {
        match self {
            Self::PhysicalPreJoin { reason, .. } | Self::PhysicalPostJoin { reason, .. } => {
                Some(*reason)
            }
            Self::Unsettled | Self::ShutdownPostJoin { .. } | Self::Quarantined => None,
        }
    }

    fn install_physical_prejoin(
        &mut self,
        reason: LegTransportTerminalReason,
        retired: RetiredOwnerStandby,
        queue_discarded: LegOutboundQueueColdInventory,
    ) -> bool {
        if !self.is_unsettled() {
            return false;
        }
        *self = Self::PhysicalPreJoin {
            reason,
            retired,
            queue_discarded,
        };
        true
    }
}

enum QuinnOwnerStandbyActorOwner {
    Running(Box<StartedQuinnLegActor>),
    Joining(tokio::task::JoinHandle<QuinnOwnerStandbyJoinOutcome>),
    Joined(Box<QuinnLegActorJoined>),
    TaskLost(Arc<str>),
}

/// Copyable observation of the unique actor witness retained by the physical
/// owner. Returning this summary cannot move or destroy terminal ownership.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct QuinnOwnerStandbyJoined {
    owned_messages: usize,
    owned_bytes: usize,
    owned_terminals: usize,
    terminal_failure: bool,
}

impl QuinnOwnerStandbyJoined {
    fn observe(joined: &QuinnLegActorJoined) -> Self {
        Self {
            owned_messages: joined.owned_messages(),
            owned_bytes: joined.owned_bytes(),
            owned_terminals: joined.owned_terminals(),
            terminal_failure: joined.terminal_failure().is_some(),
        }
    }

    pub(crate) const fn owned_messages(self) -> usize {
        self.owned_messages
    }

    pub(crate) const fn owned_bytes(self) -> usize {
        self.owned_bytes
    }

    pub(crate) const fn owned_terminals(self) -> usize {
        self.owned_terminals
    }

    pub(crate) const fn terminal_failure(self) -> bool {
        self.terminal_failure
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QuinnOwnerStandbyShutdownTerminalDrive {
    Settled {
        reason: LegTransportTerminalReason,
        discarded: OwnerStandbyRetirementInventory,
    },
    AlreadySettled {
        reason: LegTransportTerminalReason,
        discarded: OwnerStandbyRetirementInventory,
    },
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub(crate) enum QuinnOwnerStandbyShutdownTerminalError {
    #[error("owner standby actor has not structurally joined")]
    NotJoined,
    #[error("owner standby slot ownership is missing")]
    MissingSlot,
    #[error("joined owner standby did not retain its exact shutdown terminal")]
    MissingTerminal,
    #[error("joined owner standby terminal did not match the exact registered slot")]
    WrongTerminal,
    #[error("owner standby retirement ledger is quarantined or inconsistent")]
    CompositionInvariant,
}

struct QuinnOwnerStandbyJoinOutcome {
    actor: StartedQuinnLegActor,
    joined: Result<QuinnLegActorJoined, QuinnLegActorError>,
}

fn spawn_owner_standby_join(
    runtime: &tokio::runtime::Handle,
    mut actor: StartedQuinnLegActor,
) -> tokio::task::JoinHandle<QuinnOwnerStandbyJoinOutcome> {
    runtime.spawn(async move {
        let joined = actor.join().await;
        QuinnOwnerStandbyJoinOutcome { actor, joined }
    })
}

#[cfg(test)]
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub(crate) enum QuinnOwnerRecoveryAdoptErrorKind {
    #[error("owner recovery controller does not share the physical B actor clock")]
    WrongSessionClock,
    #[error("owner recovery output FIFO could not be built: {0}")]
    OutputDrain(OwnerOutputDrainConfigError),
}

/// Failed deep adoption returns both exact owners unchanged.
#[cfg(test)]
pub(crate) struct QuinnOwnerRecoveryAdoptFailure<T> {
    kind: QuinnOwnerRecoveryAdoptErrorKind,
    ownership: Box<(
        QuinnOwnerStandbySessionComposition,
        OwnerSessionController<T>,
    )>,
}

#[cfg(test)]
impl<T> QuinnOwnerRecoveryAdoptFailure<T> {
    pub(crate) const fn kind(&self) -> &QuinnOwnerRecoveryAdoptErrorKind {
        &self.kind
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        QuinnOwnerStandbySessionComposition,
        OwnerSessionController<T>,
    ) {
        *self.ownership
    }
}

#[cfg(test)]
impl<T> fmt::Debug for QuinnOwnerRecoveryAdoptFailure<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QuinnOwnerRecoveryAdoptFailure")
            .field("kind", &self.kind)
            .field("ownership", &"[REDACTED]")
            .finish()
    }
}

#[cfg(test)]
#[derive(Debug)]
pub(crate) enum QuinnOwnerRecoveryInboundDrive {
    Idle,
    Blocked,
    ProbeFresh {
        probe_sequence: ProbeSequence,
        fresh_until: SimTime,
    },
    ProbeRejected(OwnerStandbyProbeAckErrorKind),
    RecoveryAccepted {
        needs_execute: bool,
    },
    AcceptancePending,
    StatusQueued,
    StatusPending,
    Ordinary {
        outputs: usize,
        needs_continue: bool,
    },
    OutputRejected,
    ControllerFailed,
    ActorFailed(QuinnLegActorError),
    CompositionInvariant,
}

#[cfg(test)]
#[derive(Debug)]
pub(crate) enum QuinnOwnerRecoveryServiceDrive {
    Inert,
    Progress {
        drive: OwnerSessionRecoveryDrive,
        installed_generation: Option<LegGeneration>,
    },
    Installed {
        generation: LegGeneration,
    },
    Pressure,
    ControllerFailed,
    ActorUnavailable,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QuinnOwnerRecoveryTerminalDrive {
    Pending,
    StandbyRetired,
    Applied {
        generation: LegGeneration,
        reason: LegTransportTerminalReason,
    },
    AlreadyApplied {
        generation: LegGeneration,
        reason: LegTransportTerminalReason,
    },
}

#[cfg(test)]
#[derive(Debug, Error)]
pub(crate) enum QuinnOwnerRecoveryTerminalError {
    #[error("owner physical B actor rejected terminal drive: {0}")]
    Actor(#[from] QuinnLegActorError),
    #[error("owner physical B local cancellation requires the shutdown-only terminal path")]
    UnexpectedLocalCancellation,
    #[error("owner hard terminal lacks the actor first-failure timestamp")]
    MissingFailureTimestamp,
    #[error("owner physical B terminal bind was rejected: {0:?}")]
    ControllerBind(OwnerSessionRecoveryTerminalBindErrorKind),
    #[error("owner physical B terminal cleanup entered a non-retryable controller failure")]
    ControllerDiscard,
    #[error("owner physical B terminal composition invariant failed")]
    CompositionInvariant,
}

#[cfg(test)]
enum QuinnOwnerRecoveryClassified {
    Probe(QuinnOwnerStandbyInboundDrive),
    Attach(OwnerSessionRegisteredAttachDrive),
    Ordinary(QuinnOwnerOrdinaryClassified),
}

struct QuinnOwnerOrdinaryClassified {
    outputs: usize,
    needs_continue: bool,
}

#[cfg(test)]
struct QuinnOwnerRecoveryInboundContext<'a, T: TargetIo> {
    controller_output: Option<QuinnOwnerControllerOutputInboundTransaction<'a, T>>,
    slot: &'a mut OwnerStandbySlot,
    active_b: &'a mut Option<OwnerTargetRecoveryHandoff>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum QuinnOwnerOutputPhase {
    Vacant,
    Page,
    Drained(OwnerOutputPageDrained),
}

impl QuinnOwnerOutputPhase {
    const fn marker_count(self) -> u8 {
        match self {
            Self::Vacant => 0,
            Self::Page | Self::Drained(_) => 1,
        }
    }

    const fn continuation_marker_count(self) -> u8 {
        match self {
            Self::Drained(page) if page.needs_continue() => 1,
            Self::Vacant | Self::Page | Self::Drained(_) => 0,
        }
    }

    const fn matches_drain(self, drain: OwnerOutputDrainInventory) -> bool {
        match self {
            Self::Page => drain.page_open(),
            Self::Vacant | Self::Drained(_) => drain.is_exact_zero(),
        }
    }
}

fn settle_output_page_boundary_for_stop(
    phase: &mut QuinnOwnerOutputPhase,
    drain: &mut OwnerOutputDrain,
) -> Result<(), OwnerOutputAdmissionFailure> {
    let QuinnOwnerOutputPhase::Drained(page) = *phase else {
        return Ok(());
    };
    if page.needs_continue() {
        match drain.admit_outputs(Vec::new(), true) {
            Ok(()) => *phase = QuinnOwnerOutputPhase::Page,
            Err(failure) => {
                // The rejected page now owns the exact continuation.  The
                // consumed drained marker must not remain as a second owner.
                *phase = QuinnOwnerOutputPhase::Vacant;
                return Err(failure);
            }
        }
    } else {
        *phase = QuinnOwnerOutputPhase::Vacant;
    }
    Ok(())
}

enum QuinnOwnerOutputQuarantineCause {
    DrainAudit { _source: OwnerOutputDrainAuditError },
    PhaseDrainMismatch,
    UnsettledOrdinary,
    ControllerFailedOrdinary,
    UnsettledContinuation,
    MissingControllerContinuation,
    AbortedControllerContinuation,
    TerminalRetainedControllerContinuation,
}

enum QuinnOwnerOutputStatus {
    Live {
        phase: QuinnOwnerOutputPhase,
    },
    /// The root reserved its sole vacant ordinary-input turn before any
    /// controller mutation. A forgotten guard leaves this visibly non-zero.
    OrdinaryInFlight {
        entered: bool,
    },
    /// Controller entry began but unwind prevented a closed disposition. The
    /// transaction cannot be restored or scheduled again.
    OrdinaryQuarantined {
        first_stop_reason: Option<OwnerOutputDrainStopReason>,
    },
    /// The controller committed one exact page, but checked output admission
    /// rejected it. The sibling rejection ledger owns that page until terminal
    /// settlement; no actor DELIVERY acknowledgement may be published.
    RejectedPage {
        first_stop_reason: Option<OwnerOutputDrainStopReason>,
    },
    /// The output marker has entered the same synchronous first-failure turn
    /// as the controller's exact resume. It is neither a schedulable marker nor
    /// a clean boundary until the closed controller outcome settles it.
    ContinuationInFlight {
        page: OwnerOutputPageDrained,
        entered: bool,
    },
    /// Drop/unwind after controller entry cannot prove whether the raw resume
    /// was retained or consumed. Preserve that uncertainty as its own sticky
    /// non-schedulable transaction residual rather than recreating a marker.
    ContinuationQuarantined {
        page: OwnerOutputPageDrained,
        first_stop_reason: Option<OwnerOutputDrainStopReason>,
    },
    Stopped {
        reason: OwnerOutputDrainStopReason,
        audited: OwnerOutputDrainInventory,
    },
    Quarantined {
        phase: QuinnOwnerOutputPhase,
        first_stop_reason: Option<OwnerOutputDrainStopReason>,
        _cause: QuinnOwnerOutputQuarantineCause,
    },
}

/// The exact drain never moves during a live-to-terminal transition. All
/// fallible auditing happens while this ledger still owns it; commit changes
/// only the O(1) status metadata.
struct QuinnOwnerOutputLedger {
    drain: OwnerOutputDrain,
    status: QuinnOwnerOutputStatus,
}

/// One structural owner for every output FIFO, page-boundary, rejection and
/// lifecycle ledger. The enclosing physical root may drive transactions on
/// this value, but it cannot move one ledger without moving the whole output
/// authority.
#[must_use = "owner output ownership requires explicit stop settlement"]
struct QuinnOwnerOutputSubroot {
    ledger: QuinnOwnerOutputLedger,
    admission_failure: Option<OwnerOutputAdmissionFailure>,
    lifecycle: OwnerOutputLifecyclePort,
}

struct QuinnOwnerOutputDriveTransaction<'a> {
    drain: &'a mut OwnerOutputDrain,
    phase: &'a mut QuinnOwnerOutputPhase,
    lifecycle: &'a mut OwnerOutputLifecyclePort,
}

impl QuinnOwnerOutputDriveTransaction<'_> {
    fn drive(
        self,
        queue: &mut LegOutboundQueue,
    ) -> Result<OwnerOutputDrainDrive, OwnerOutputDrainAuditError> {
        let driven = self.drain.drive_one(Some(queue), self.lifecycle)?;
        match driven {
            OwnerOutputDrainDrive::Transmitted {
                page_drained: Some(page),
            }
            | OwnerOutputDrainDrive::LifecycleInstalled {
                page_drained: Some(page),
            }
            | OwnerOutputDrainDrive::PageDrained(page) => {
                *self.phase = QuinnOwnerOutputPhase::Drained(page);
            }
            OwnerOutputDrainDrive::Idle
            | OwnerOutputDrainDrive::Blocked { .. }
            | OwnerOutputDrainDrive::LifecycleInstallBlocked
            | OwnerOutputDrainDrive::Transmitted { page_drained: None }
            | OwnerOutputDrainDrive::LifecycleInstalled { page_drained: None } => {}
        }
        Ok(driven)
    }
}

enum QuinnOwnerOutputServiceTurn<'a, T: TargetIo> {
    Inert,
    CompletedBoundary,
    Continue(QuinnOwnerOutputContinuationTransaction<'a, T>),
    Drive(QuinnOwnerOutputDriveTransaction<'a>),
    LifecycleInstallBlocked,
    ControllerFailed,
    Stopped {
        reason: OwnerOutputDrainStopReason,
        inventory: QuinnOwnerOutputRootInventory,
    },
    CompositionInvariant,
}

/// Complete output-subroot witness. A drain-local zero is insufficient while
/// an installed lifecycle action or an atomically rejected page is still live.
/// An unaudited or duplicated drain ledger fails closed without inventing a
/// byte count; the future full owner root must include this whole value rather
/// than selecting its `drain` member. After a live lifecycle commit this
/// subroot is allowed to reach zero only because the future full root must
/// separately inventory the typed timer/phase slot created by the callback.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct QuinnOwnerOutputRootInventory {
    drain: Option<OwnerOutputDrainInventory>,
    drain_ledgers: u8,
    drain_audit_failed: bool,
    phase_markers: u8,
    continuation_markers: u8,
    ordinary_transactions: u8,
    continuation_transactions: u8,
    recovery_transactions: u8,
    terminal_transactions: u8,
    terminal_residuals: u8,
    terminal_stamps: u8,
    phase_drain_consistent: bool,
    quarantined: bool,
    recovery_quarantined: bool,
    terminal_quarantined: bool,
    controller_failures: u8,
    controller_error_payload: TargetErrorColdAudit,
    controller_cold_audit_failed: bool,
    controller_cold_exact_zero: bool,
    controller_snapshot: Option<OwnerSessionControllerSnapshot>,
    controller_terminal_correlation_valid: bool,
    standby_terminal_correlation_valid: bool,
    lifecycle: OwnerOutputLifecycleInventory,
    rejected_page: Option<OwnerOutputAdmissionInventory>,
}

impl QuinnOwnerOutputSubroot {
    fn new(
        max_outputs: usize,
        max_encoded_bytes: usize,
    ) -> Result<Self, OwnerOutputDrainConfigError> {
        Ok(Self {
            ledger: QuinnOwnerOutputLedger {
                drain: OwnerOutputDrain::new(max_outputs, max_encoded_bytes)?,
                status: QuinnOwnerOutputStatus::Live {
                    phase: QuinnOwnerOutputPhase::Vacant,
                },
            },
            admission_failure: None,
            lifecycle: OwnerOutputLifecyclePort::new(),
        })
    }

    fn admission_failed(&self) -> bool {
        self.admission_failure.is_some()
    }

    fn inventory(&self) -> QuinnOwnerOutputRootInventory {
        let (
            drain,
            drain_audit_failed,
            phase_markers,
            continuation_markers,
            ordinary_transactions,
            continuation_transactions,
            phase_drain_consistent,
            quarantined,
        ) = match &self.ledger.status {
            QuinnOwnerOutputStatus::Live { phase } => match self.ledger.drain.inventory() {
                Ok(inventory) => (
                    Some(inventory),
                    false,
                    phase.marker_count(),
                    phase.continuation_marker_count(),
                    0,
                    0,
                    phase.matches_drain(inventory),
                    false,
                ),
                Err(_) => (
                    None,
                    true,
                    phase.marker_count(),
                    phase.continuation_marker_count(),
                    0,
                    0,
                    false,
                    false,
                ),
            },
            QuinnOwnerOutputStatus::OrdinaryInFlight { .. } => {
                let inventory = self.ledger.drain.inventory().ok();
                (
                    inventory,
                    inventory.is_none(),
                    0,
                    0,
                    1,
                    0,
                    inventory.is_some_and(OwnerOutputDrainInventory::is_exact_zero)
                        && self.admission_failure.is_none(),
                    false,
                )
            }
            QuinnOwnerOutputStatus::OrdinaryQuarantined { .. } => {
                let inventory = self.ledger.drain.inventory().ok();
                (
                    inventory,
                    inventory.is_none(),
                    0,
                    0,
                    1,
                    0,
                    inventory.is_some_and(OwnerOutputDrainInventory::is_exact_zero)
                        && self.admission_failure.is_none(),
                    true,
                )
            }
            QuinnOwnerOutputStatus::RejectedPage { .. } => {
                let inventory = self.ledger.drain.inventory().ok();
                (
                    inventory,
                    inventory.is_none(),
                    0,
                    0,
                    0,
                    0,
                    inventory.is_some_and(OwnerOutputDrainInventory::is_exact_zero)
                        && self.admission_failure.is_some(),
                    false,
                )
            }
            QuinnOwnerOutputStatus::Stopped { audited, .. } => {
                match self.ledger.drain.inventory() {
                    Ok(inventory) => (
                        Some(inventory),
                        inventory != *audited,
                        0,
                        0,
                        0,
                        0,
                        inventory == *audited,
                        false,
                    ),
                    Err(_) => (None, true, 0, 0, 0, 0, false, false),
                }
            }
            QuinnOwnerOutputStatus::ContinuationInFlight { page, .. } => {
                let inventory = self.ledger.drain.inventory().ok();
                (
                    inventory,
                    inventory.is_none(),
                    0,
                    0,
                    0,
                    1,
                    page.needs_continue()
                        && inventory.is_some_and(OwnerOutputDrainInventory::is_exact_zero),
                    false,
                )
            }
            QuinnOwnerOutputStatus::ContinuationQuarantined { page, .. } => {
                let inventory = self.ledger.drain.inventory().ok();
                (
                    inventory,
                    inventory.is_none(),
                    0,
                    0,
                    0,
                    1,
                    page.needs_continue()
                        && inventory.is_some_and(OwnerOutputDrainInventory::is_exact_zero),
                    true,
                )
            }
            QuinnOwnerOutputStatus::Quarantined { phase, .. } => {
                let inventory = self.ledger.drain.inventory().ok();
                let consistent = inventory.is_some_and(|inventory| phase.matches_drain(inventory));
                (
                    inventory,
                    true,
                    phase.marker_count(),
                    phase.continuation_marker_count(),
                    0,
                    0,
                    consistent,
                    true,
                )
            }
        };
        QuinnOwnerOutputRootInventory::from_sources(
            drain,
            1,
            drain_audit_failed,
            phase_markers,
            continuation_markers,
            ordinary_transactions,
            continuation_transactions,
            phase_drain_consistent,
            quarantined,
            self.lifecycle.inventory(),
            self.admission_failure
                .as_ref()
                .map(OwnerOutputAdmissionFailure::inventory),
        )
    }

    fn stopped_observation(&self) -> Option<QuinnOwnerOutputStopDrive> {
        let QuinnOwnerOutputStatus::Stopped { reason, .. } = self.ledger.status else {
            return None;
        };
        Some(QuinnOwnerOutputStopDrive::AlreadyStopped {
            reason,
            inventory: self.inventory(),
        })
    }

    fn preflight_live_phase(&self) -> Result<(), QuinnOwnerOutputQuarantineCause> {
        let QuinnOwnerOutputStatus::Live { phase } = &self.ledger.status else {
            return Err(QuinnOwnerOutputQuarantineCause::PhaseDrainMismatch);
        };
        let inventory =
            self.ledger.drain.inventory().map_err(|source| {
                QuinnOwnerOutputQuarantineCause::DrainAudit { _source: source }
            })?;
        if !phase.matches_drain(inventory) || self.admission_failure.is_some() {
            return Err(QuinnOwnerOutputQuarantineCause::PhaseDrainMismatch);
        }
        Ok(())
    }

    /// Cold, allocation-free prerequisite for the recovery-B hard-terminal
    /// transaction. Until the full no-ACK settlement is installed, the actor's
    /// terminal may be consumed only when no output, continuation, rejected
    /// page, or lifecycle action needs settlement.
    fn preflight_hard_terminal_exact_zero(&self) -> Option<OwnerOutputDrainInventory> {
        if self.admission_failure.is_some() || !self.lifecycle.inventory().is_exact_zero() {
            return None;
        }
        let inventory = self.ledger.drain.inventory().ok()?;
        if !inventory.is_exact_zero() {
            return None;
        }
        match self.ledger.status {
            QuinnOwnerOutputStatus::Live {
                phase: QuinnOwnerOutputPhase::Vacant,
            } => Some(inventory),
            QuinnOwnerOutputStatus::Stopped {
                reason: OwnerOutputDrainStopReason::Fatal,
                audited,
            } if audited == inventory => Some(inventory),
            QuinnOwnerOutputStatus::Live { .. }
            | QuinnOwnerOutputStatus::Stopped { .. }
            | QuinnOwnerOutputStatus::OrdinaryInFlight { .. }
            | QuinnOwnerOutputStatus::OrdinaryQuarantined { .. }
            | QuinnOwnerOutputStatus::RejectedPage { .. }
            | QuinnOwnerOutputStatus::ContinuationInFlight { .. }
            | QuinnOwnerOutputStatus::ContinuationQuarantined { .. }
            | QuinnOwnerOutputStatus::Quarantined { .. } => None,
        }
    }

    /// Final infallible metadata move for an already-audited empty output
    /// subroot. The enclosing transaction exclusively borrows this subroot, so
    /// the drain and lifecycle ledgers cannot change between preflight and this
    /// commit.
    fn commit_preflighted_hard_terminal_exact_zero(
        &mut self,
        audited: OwnerOutputDrainInventory,
    ) -> bool {
        if self.admission_failure.is_some() || !self.lifecycle.inventory().is_exact_zero() {
            return false;
        }
        match self.ledger.status {
            QuinnOwnerOutputStatus::Live {
                phase: QuinnOwnerOutputPhase::Vacant,
            } => {
                self.ledger.status = QuinnOwnerOutputStatus::Stopped {
                    reason: OwnerOutputDrainStopReason::Fatal,
                    audited,
                };
                true
            }
            QuinnOwnerOutputStatus::Stopped {
                reason: OwnerOutputDrainStopReason::Fatal,
                audited: installed,
            } => installed == audited,
            QuinnOwnerOutputStatus::Live { .. }
            | QuinnOwnerOutputStatus::Stopped { .. }
            | QuinnOwnerOutputStatus::OrdinaryInFlight { .. }
            | QuinnOwnerOutputStatus::OrdinaryQuarantined { .. }
            | QuinnOwnerOutputStatus::RejectedPage { .. }
            | QuinnOwnerOutputStatus::ContinuationInFlight { .. }
            | QuinnOwnerOutputStatus::ContinuationQuarantined { .. }
            | QuinnOwnerOutputStatus::Quarantined { .. } => false,
        }
    }

    fn quarantine_live(
        &mut self,
        first_stop_reason: Option<OwnerOutputDrainStopReason>,
        cause: QuinnOwnerOutputQuarantineCause,
    ) {
        let QuinnOwnerOutputStatus::Live { phase } = self.ledger.status else {
            return;
        };
        self.ledger.status = QuinnOwnerOutputStatus::Quarantined {
            phase,
            first_stop_reason,
            _cause: cause,
        };
    }

    fn stop(&mut self, reason: OwnerOutputDrainStopReason) -> QuinnOwnerOutputStopDrive {
        if let Some(observation) = self.stopped_observation() {
            return observation;
        }
        if let QuinnOwnerOutputStatus::OrdinaryInFlight { .. } = self.ledger.status {
            self.ledger.status = QuinnOwnerOutputStatus::OrdinaryQuarantined {
                first_stop_reason: Some(reason),
            };
            return QuinnOwnerOutputStopDrive::CompositionInvariant {
                inventory: self.inventory(),
            };
        }
        if let QuinnOwnerOutputStatus::OrdinaryQuarantined { first_stop_reason } =
            &mut self.ledger.status
        {
            if first_stop_reason.is_none() {
                *first_stop_reason = Some(reason);
            }
            return QuinnOwnerOutputStopDrive::CompositionInvariant {
                inventory: self.inventory(),
            };
        }
        if let QuinnOwnerOutputStatus::RejectedPage { first_stop_reason } = &mut self.ledger.status
        {
            if first_stop_reason.is_none() {
                *first_stop_reason = Some(reason);
            }
            return QuinnOwnerOutputStopDrive::CompositionInvariant {
                inventory: self.inventory(),
            };
        }
        if let QuinnOwnerOutputStatus::ContinuationInFlight { page, .. } = self.ledger.status {
            self.ledger.status = QuinnOwnerOutputStatus::ContinuationQuarantined {
                page,
                first_stop_reason: Some(reason),
            };
            return QuinnOwnerOutputStopDrive::CompositionInvariant {
                inventory: self.inventory(),
            };
        }
        if let QuinnOwnerOutputStatus::ContinuationQuarantined {
            first_stop_reason, ..
        } = &mut self.ledger.status
        {
            if first_stop_reason.is_none() {
                *first_stop_reason = Some(reason);
            }
            return QuinnOwnerOutputStopDrive::CompositionInvariant {
                inventory: self.inventory(),
            };
        }
        if let QuinnOwnerOutputStatus::Quarantined {
            first_stop_reason, ..
        } = &mut self.ledger.status
        {
            if first_stop_reason.is_none() {
                *first_stop_reason = Some(reason);
            }
            return QuinnOwnerOutputStopDrive::CompositionInvariant {
                inventory: self.inventory(),
            };
        }

        if let Err(cause) = self.preflight_live_phase() {
            self.quarantine_live(Some(reason), cause);
            return QuinnOwnerOutputStopDrive::CompositionInvariant {
                inventory: self.inventory(),
            };
        }

        // `drive_one` transfers the page marker to the subroot together with
        // the final output. A true marker retains the sole controller
        // continuation and must re-enter the stopped drain. A false marker
        // carries no future work; the sticky stop reason terminally consumes
        // that completed-page boundary instead of manufacturing residual
        // ownership during shutdown/fatal/cancel.
        let settlement = match &mut self.ledger.status {
            QuinnOwnerOutputStatus::Live { phase }
                if matches!(phase, QuinnOwnerOutputPhase::Drained(_)) =>
            {
                settle_output_page_boundary_for_stop(phase, &mut self.ledger.drain)
            }
            QuinnOwnerOutputStatus::Live { .. } => Ok(()),
            QuinnOwnerOutputStatus::Stopped { .. }
            | QuinnOwnerOutputStatus::OrdinaryInFlight { .. }
            | QuinnOwnerOutputStatus::OrdinaryQuarantined { .. }
            | QuinnOwnerOutputStatus::RejectedPage { .. }
            | QuinnOwnerOutputStatus::ContinuationInFlight { .. }
            | QuinnOwnerOutputStatus::ContinuationQuarantined { .. }
            | QuinnOwnerOutputStatus::Quarantined { .. } => {
                return QuinnOwnerOutputStopDrive::CompositionInvariant {
                    inventory: self.inventory(),
                };
            }
        };
        if let Err(failure) = settlement {
            if self.admission_failure.is_none() {
                self.admission_failure = Some(failure);
            } else {
                self.quarantine_live(
                    Some(reason),
                    QuinnOwnerOutputQuarantineCause::PhaseDrainMismatch,
                );
                return QuinnOwnerOutputStopDrive::CompositionInvariant {
                    inventory: self.inventory(),
                };
            }
        }

        let phase = match self.ledger.status {
            QuinnOwnerOutputStatus::Live { phase } => phase,
            QuinnOwnerOutputStatus::Stopped { .. }
            | QuinnOwnerOutputStatus::OrdinaryInFlight { .. }
            | QuinnOwnerOutputStatus::OrdinaryQuarantined { .. }
            | QuinnOwnerOutputStatus::RejectedPage { .. }
            | QuinnOwnerOutputStatus::ContinuationInFlight { .. }
            | QuinnOwnerOutputStatus::ContinuationQuarantined { .. }
            | QuinnOwnerOutputStatus::Quarantined { .. } => {
                return QuinnOwnerOutputStopDrive::CompositionInvariant {
                    inventory: self.inventory(),
                };
            }
        };
        let audited = match self.ledger.drain.inventory() {
            Ok(inventory) if phase.matches_drain(inventory) => inventory,
            Ok(_) => {
                self.quarantine_live(
                    Some(reason),
                    QuinnOwnerOutputQuarantineCause::PhaseDrainMismatch,
                );
                return QuinnOwnerOutputStopDrive::CompositionInvariant {
                    inventory: self.inventory(),
                };
            }
            Err(source) => {
                self.quarantine_live(
                    Some(reason),
                    QuinnOwnerOutputQuarantineCause::DrainAudit { _source: source },
                );
                return QuinnOwnerOutputStopDrive::CompositionInvariant {
                    inventory: self.inventory(),
                };
            }
        };
        self.ledger.status = QuinnOwnerOutputStatus::Stopped { reason, audited };
        let inventory = self.inventory();
        QuinnOwnerOutputStopDrive::Stopped { reason, inventory }
    }

    fn fail(&mut self) {
        let _ = self.stop(OwnerOutputDrainStopReason::Fatal);
    }

    fn transact_installed_lifecycle<'owner, 'slot>(
        &'owner mut self,
        commit: impl FnOnce(
            OwnerOutputLifecycleTransaction<'owner>,
        ) -> OwnerOutputLifecycleResolution<'owner, 'slot>,
    ) -> OwnerOutputLifecycleDrive {
        if let Err(cause) = self.preflight_live_phase() {
            if matches!(self.ledger.status, QuinnOwnerOutputStatus::Live { .. }) {
                self.quarantine_live(None, cause);
                return OwnerOutputLifecycleDrive::AuditFailed;
            }
            return OwnerOutputLifecycleDrive::WrongPhase;
        }
        self.lifecycle
            .transact(OwnerOutputLifecycleSettlement::Install, commit)
    }

    fn transact_stopped_lifecycle<'owner, 'slot>(
        &'owner mut self,
        commit: impl FnOnce(
            OwnerOutputLifecycleTransaction<'owner>,
        ) -> OwnerOutputLifecycleResolution<'owner, 'slot>,
    ) -> OwnerOutputLifecycleDrive {
        let (reason, audited) = match self.ledger.status {
            QuinnOwnerOutputStatus::Stopped { reason, audited } => (reason, audited),
            QuinnOwnerOutputStatus::Quarantined { .. } => {
                return OwnerOutputLifecycleDrive::AuditFailed;
            }
            QuinnOwnerOutputStatus::OrdinaryInFlight { .. }
            | QuinnOwnerOutputStatus::OrdinaryQuarantined { .. }
            | QuinnOwnerOutputStatus::RejectedPage { .. } => {
                return OwnerOutputLifecycleDrive::AuditFailed;
            }
            QuinnOwnerOutputStatus::ContinuationInFlight { .. } => {
                return OwnerOutputLifecycleDrive::AuditFailed;
            }
            QuinnOwnerOutputStatus::ContinuationQuarantined { .. } => {
                return OwnerOutputLifecycleDrive::AuditFailed;
            }
            QuinnOwnerOutputStatus::Live { .. } => {
                return OwnerOutputLifecycleDrive::WrongPhase;
            }
        };
        if !matches!(self.ledger.drain.inventory(), Ok(inventory) if inventory == audited) {
            return OwnerOutputLifecycleDrive::AuditFailed;
        }
        self.lifecycle
            .transact(OwnerOutputLifecycleSettlement::Discard { reason }, commit)
    }
}

const fn controller_snapshot_is_live_exact_zero(snapshot: OwnerSessionControllerSnapshot) -> bool {
    !snapshot.failed
        && !snapshot.session_expired
        && (snapshot.progress_entries == 0 && snapshot.retained_outputs == 0)
        && snapshot.retained_observations == 0
        && snapshot.pending_continuations == 0
        && snapshot.retained_continuations == 0
        && snapshot.pending_recoveries == 0
        && snapshot.pending_attach_statuses == 0
        && snapshot.retained_recoveries == 0
        && snapshot.retained_recovery_cursors == 0
        && snapshot.retained_recovery_outputs == 0
        && snapshot.retained_recovery_observations == 0
        && snapshot.retained_recovery_attached == 0
        && !snapshot.recovery_needs_acceptance
        && !snapshot.recovery_needs_execute
        && !snapshot.recovery_needs_publish
        && !snapshot.recovery_handoff_ready
        && !snapshot.recovery_needs_terminal_discard
        && snapshot.recovery_terminal_quarantined == 0
        && snapshot.pending_legless_high_waters == 0
        && !snapshot.attach_status_needs_enqueue
        && !snapshot.attach_status_needs_delivery
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QuinnOwnerOutputStopDrive {
    Stopped {
        reason: OwnerOutputDrainStopReason,
        inventory: QuinnOwnerOutputRootInventory,
    },
    AlreadyStopped {
        reason: OwnerOutputDrainStopReason,
        inventory: QuinnOwnerOutputRootInventory,
    },
    CompositionInvariant {
        inventory: QuinnOwnerOutputRootInventory,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QuinnOwnerOutputServiceDrive {
    Inert,
    Continued {
        outputs: usize,
        needs_continue: bool,
    },
    Transmitted {
        page_drained: bool,
    },
    LifecycleInstalled {
        page_drained: bool,
    },
    LifecycleInstallBlocked,
    PageDrained {
        needs_continue: bool,
    },
    Pressure,
    ControllerFailed,
    ActorUnavailable,
    Stopped {
        reason: OwnerOutputDrainStopReason,
        inventory: QuinnOwnerOutputRootInventory,
    },
    CompositionInvariant,
}

/// Exact armed-probe deadline plus the first due arbitration's frozen inbound
/// publication high-water. The scalar token stays inside the physical owner
/// and moves whole across every actor arbitration retry.
struct QuinnOwnerStandbyProbeTimer {
    probe_sequence: ProbeSequence,
    deadline: SimTime,
    inbound_publication_cutoff: Option<u64>,
}

impl QuinnOwnerStandbyProbeTimer {
    const fn new(probe_sequence: ProbeSequence, deadline: SimTime) -> Self {
        Self {
            probe_sequence,
            deadline,
            inbound_publication_cutoff: None,
        }
    }

    fn matches(&self, probe_sequence: ProbeSequence, deadline: SimTime) -> bool {
        self.probe_sequence == probe_sequence && self.deadline == deadline
    }
}

impl QuinnLegActorArrivalDeadline for QuinnOwnerStandbyProbeTimer {
    fn arrival_deadline(&self) -> SimTime {
        self.deadline
    }
}

impl QuinnLegActorSemanticTimer for QuinnOwnerStandbyProbeTimer {
    fn capture_inbound_publication_cutoff(&mut self, high_water: u64) -> u64 {
        *self.inbound_publication_cutoff.get_or_insert(high_water)
    }
}

impl QuinnOwnerStandbySessionComposition {
    pub(crate) fn adopt_registered(
        mut actor: StartedQuinnLegActor,
        slot: OwnerStandbySlot,
        liveness: QuinnStandbyLivenessConfig,
    ) -> Result<Self, QuinnOwnerStandbyAdoptFailure> {
        let gate = QuinnLegCompositionGate::mint();
        let clock = actor.session_clock(&gate);
        let exact_actor = actor.with_controller_parts_mut(&gate, |io, queue| {
            slot.registered_matches_actor(io.established_leg(), queue)
        });
        if exact_actor != Some(true) {
            return Err(QuinnOwnerStandbyAdoptFailure {
                kind: QuinnOwnerStandbyAdoptErrorKind::WrongPhysicalStandby,
                ownership: Box::new((actor, slot)),
            });
        }
        Ok(Self {
            actor: Some(QuinnOwnerStandbyActorOwner::Running(Box::new(actor))),
            slot: Some(slot),
            clock,
            liveness,
            next_probe_sequence: ProbeSequence::new(1).ok(),
            armed_probe_timer: None,
            retirement: QuinnOwnerStandbyRetirementState::Unsettled,
        })
    }

    fn actor_and_slot_are_running(&self) -> bool {
        matches!(
            self.actor.as_ref(),
            Some(QuinnOwnerStandbyActorOwner::Running(_))
        ) && self.slot.is_some()
    }

    fn actor_unavailable_error(&self) -> Option<QuinnLegActorError> {
        match self.actor.as_ref() {
            Some(QuinnOwnerStandbyActorOwner::Running(_)) => None,
            Some(QuinnOwnerStandbyActorOwner::Joining(_)) => Some(QuinnLegActorError::Closing),
            Some(QuinnOwnerStandbyActorOwner::Joined(_)) => Some(QuinnLegActorError::AlreadyJoined),
            Some(QuinnOwnerStandbyActorOwner::TaskLost(cause)) => {
                Some(QuinnLegActorError::TaskJoin(cause.to_string()))
            }
            None => Some(QuinnLegActorError::Protocol(
                "owner standby actor ownership is missing",
            )),
        }
    }

    fn running_actor_mut(&mut self) -> Result<&mut StartedQuinnLegActor, QuinnLegActorError> {
        match self.actor.as_mut() {
            Some(QuinnOwnerStandbyActorOwner::Running(actor)) => Ok(actor.as_mut()),
            Some(QuinnOwnerStandbyActorOwner::Joining(_)) => Err(QuinnLegActorError::Closing),
            Some(QuinnOwnerStandbyActorOwner::Joined(_)) => Err(QuinnLegActorError::AlreadyJoined),
            Some(QuinnOwnerStandbyActorOwner::TaskLost(cause)) => {
                Err(QuinnLegActorError::TaskJoin(cause.to_string()))
            }
            None => Err(QuinnLegActorError::Protocol(
                "owner standby actor ownership is missing",
            )),
        }
    }

    fn running_actor_and_slot_mut(
        &mut self,
    ) -> Result<(&mut StartedQuinnLegActor, &mut OwnerStandbySlot), QuinnLegActorError> {
        let Self { actor, slot, .. } = self;
        let actor = match actor.as_mut() {
            Some(QuinnOwnerStandbyActorOwner::Running(actor)) => actor.as_mut(),
            Some(QuinnOwnerStandbyActorOwner::Joining(_)) => {
                return Err(QuinnLegActorError::Closing);
            }
            Some(QuinnOwnerStandbyActorOwner::Joined(_)) => {
                return Err(QuinnLegActorError::AlreadyJoined);
            }
            Some(QuinnOwnerStandbyActorOwner::TaskLost(cause)) => {
                return Err(QuinnLegActorError::TaskJoin(cause.to_string()));
            }
            None => {
                return Err(QuinnLegActorError::Protocol(
                    "owner standby actor ownership is missing",
                ));
            }
        };
        let slot = slot.as_mut().ok_or(QuinnLegActorError::Protocol(
            "owner standby slot ownership is missing",
        ))?;
        Ok((actor, slot))
    }

    fn request_cancel_if_running(&mut self) {
        if let Some(QuinnOwnerStandbyActorOwner::Running(actor)) = self.actor.as_mut() {
            actor.request_cancel();
        }
    }

    fn request_controller_failure_if_running(&mut self) {
        if let Some(QuinnOwnerStandbyActorOwner::Running(actor)) = self.actor.as_mut() {
            actor.request_composition_failure();
        }
    }

    fn start_join_for_shutdown(&mut self) -> Result<(), QuinnLegActorError> {
        let actor = self.actor.as_ref().ok_or(QuinnLegActorError::Protocol(
            "owner standby actor ownership is missing",
        ))?;
        match actor {
            QuinnOwnerStandbyActorOwner::Joining(_) => return Ok(()),
            QuinnOwnerStandbyActorOwner::Joined(_) => return Ok(()),
            QuinnOwnerStandbyActorOwner::TaskLost(cause) => {
                return Err(QuinnLegActorError::TaskJoin(cause.to_string()));
            }
            QuinnOwnerStandbyActorOwner::Running(_) => {}
        }
        if self.slot.is_none() {
            return Err(QuinnLegActorError::Protocol(
                "owner standby slot ownership is missing",
            ));
        }
        let gate = QuinnLegCompositionGate::mint();
        if let QuinnOwnerStandbyActorOwner::Running(actor) = actor {
            let (messages, bytes) = actor.shutdown_outbound_inventory(&gate)?;
            if messages != 0 || bytes != 0 {
                return Err(QuinnLegActorError::ResidualOwnership { messages, bytes });
            }
        }
        let runtime = tokio::runtime::Handle::try_current().map_err(|error| {
            QuinnLegActorError::TaskJoin(format!(
                "owner standby join runtime is unavailable: {error}"
            ))
        })?;
        let actor = self.actor.take().ok_or(QuinnLegActorError::Protocol(
            "owner standby actor ownership is missing",
        ))?;
        let QuinnOwnerStandbyActorOwner::Running(actor) = actor else {
            self.actor = Some(actor);
            return Err(QuinnLegActorError::Protocol(
                "owner standby join start changed actor state",
            ));
        };
        let mut actor = *actor;
        actor.request_cancel();
        self.actor = Some(QuinnOwnerStandbyActorOwner::Joining(
            spawn_owner_standby_join(&runtime, actor),
        ));
        Ok(())
    }

    /// Retires expired freshness at the caller's service time, but derives
    /// and retains the exact probe sequence entirely inside this owner.
    pub(crate) fn drive_probe(
        &mut self,
    ) -> Result<QuinnOwnerStandbyProbeDrive, QuinnOwnerStandbyProbeDriveError> {
        if !self.actor_and_slot_are_running() {
            return Err(QuinnOwnerStandbyProbeDriveError::ActorUnavailable);
        }
        let now = self.clock.now()?;
        let probe_sequence = self
            .next_probe_sequence
            .ok_or(QuinnOwnerStandbyProbeDriveError::ProbeSequenceExhausted)?;
        let gate = QuinnLegCompositionGate::mint();
        let (actor, slot) = self
            .running_actor_and_slot_mut()
            .map_err(|_| QuinnOwnerStandbyProbeDriveError::ActorUnavailable)?;
        let _expired = slot.expire_probe_freshness_at(now);
        let Some(driven) = actor.with_controller_parts_mut(&gate, |_io, queue| {
            slot.drive_exact_probe(probe_sequence, queue)
        }) else {
            return Err(QuinnOwnerStandbyProbeDriveError::ActorUnavailable);
        };
        match driven? {
            OwnerStandbyProbeDrive::Enqueued { probe_sequence } => {
                Ok(QuinnOwnerStandbyProbeDrive::Enqueued { probe_sequence })
            }
        }
    }

    pub(crate) fn arm_probe_after_delivery(
        &mut self,
    ) -> Result<QuinnOwnerStandbyProbeArmDrive, QuinnOwnerStandbyProbeArmError> {
        if !self.actor_and_slot_are_running() {
            return Err(QuinnOwnerStandbyProbeArmError::EndpointOrQueueLost);
        }
        let now = self.clock.now()?;
        let armed = self
            .slot
            .as_mut()
            .ok_or(QuinnOwnerStandbyProbeArmError::EndpointOrQueueLost)?
            .arm_exact_probe_after_delivery(now, self.liveness.probe_timeout)
            .map_err(|error| match error {
                OwnerStandbyProbeArmError::InvalidTimeout => {
                    QuinnOwnerStandbyProbeArmError::InvalidTimeout
                }
                OwnerStandbyProbeArmError::EndpointOrQueueLost => {
                    QuinnOwnerStandbyProbeArmError::EndpointOrQueueLost
                }
                OwnerStandbyProbeArmError::Clock(error) => {
                    QuinnOwnerStandbyProbeArmError::DeadlineClock(error)
                }
                OwnerStandbyProbeArmError::WrongState => QuinnOwnerStandbyProbeArmError::WrongState,
            })?;
        match armed {
            OwnerStandbyProbeArmDrive::NotDelivered => {
                if self.armed_probe_timer.is_some() {
                    return Err(QuinnOwnerStandbyProbeArmError::CompositionInvariant);
                }
                Ok(QuinnOwnerStandbyProbeArmDrive::NotDelivered)
            }
            OwnerStandbyProbeArmDrive::Armed {
                probe_sequence,
                deadline,
            } => {
                if self.armed_probe_timer.is_some()
                    || self.next_probe_sequence != Some(probe_sequence)
                {
                    return Err(QuinnOwnerStandbyProbeArmError::CompositionInvariant);
                }
                self.armed_probe_timer =
                    Some(QuinnOwnerStandbyProbeTimer::new(probe_sequence, deadline));
                Ok(QuinnOwnerStandbyProbeArmDrive::Armed { deadline })
            }
            OwnerStandbyProbeArmDrive::AlreadyArmed {
                probe_sequence,
                deadline,
            } => {
                if self.next_probe_sequence != Some(probe_sequence)
                    || !self
                        .armed_probe_timer
                        .as_ref()
                        .is_some_and(|timer| timer.matches(probe_sequence, deadline))
                {
                    return Err(QuinnOwnerStandbyProbeArmError::CompositionInvariant);
                }
                Ok(QuinnOwnerStandbyProbeArmDrive::AlreadyArmed { deadline })
            }
        }
    }

    /// Commits one no-ACK expiry only through the actor's receive-before-timer
    /// gate. A published or in-flight record at/before the absolute deadline
    /// keeps the exact timer owned here for classification before retry.
    pub(crate) fn try_expire_probe(
        &mut self,
    ) -> Result<QuinnOwnerStandbyProbeExpiryDrive, QuinnOwnerStandbyProbeExpiryError> {
        if !self.actor_and_slot_are_running() {
            return Err(QuinnOwnerStandbyProbeExpiryError::ActorUnavailable);
        }
        let now = self.clock.now()?;
        let Some(timer) = self.armed_probe_timer.take() else {
            return Ok(QuinnOwnerStandbyProbeExpiryDrive::Inert);
        };
        let probe_sequence = timer.probe_sequence;
        let deadline = timer.deadline;
        if now < deadline {
            self.armed_probe_timer = Some(timer);
            return Ok(QuinnOwnerStandbyProbeExpiryDrive::Pending {
                probe_sequence,
                deadline,
            });
        }

        let gate = QuinnLegCompositionGate::mint();
        let (actor, slot) = self
            .running_actor_and_slot_mut()
            .map_err(|_| QuinnOwnerStandbyProbeExpiryError::ActorUnavailable)?;
        let arbitration = actor.try_transact_inbound_before_semantic_timer(
            &gate,
            timer,
            slot,
            |slot, _runtime, envelope, timer| {
                let _ = slot;
                Ok((QuinnLegActorInboundDecision::<()>::Blocked(envelope), timer))
            },
            |slot, _runtime, timer| match slot.expire_exact_probe_if_due(
                timer.probe_sequence,
                timer.deadline,
                now,
            ) {
                Ok(OwnerStandbyProbeExpiryDrive::Expired) => {
                    QuinnLegActorTimerDecision::Committed((timer.probe_sequence, timer.deadline))
                }
                Ok(OwnerStandbyProbeExpiryDrive::Pending) => {
                    QuinnLegActorTimerDecision::Rejected(timer)
                }
                Err(error) => QuinnLegActorTimerDecision::Invariant { timer, error },
            },
        );
        match arbitration {
            QuinnLegActorInboundTimerPoll::Inbound { poll, timer } => {
                self.armed_probe_timer = Some(timer);
                match poll {
                    QuinnLegActorInboundPoll::Blocked => {
                        Ok(QuinnOwnerStandbyProbeExpiryDrive::InboundPending {
                            probe_sequence,
                            deadline,
                        })
                    }
                    QuinnLegActorInboundPoll::Idle | QuinnLegActorInboundPoll::Classified(()) => {
                        Err(QuinnOwnerStandbyProbeExpiryError::CompositionInvariant)
                    }
                }
            }
            QuinnLegActorInboundTimerPoll::PublicationPending(timer) => {
                self.armed_probe_timer = Some(timer);
                Ok(QuinnOwnerStandbyProbeExpiryDrive::PublicationPending {
                    probe_sequence,
                    deadline,
                })
            }
            QuinnLegActorInboundTimerPoll::TimerCommitted((
                committed_sequence,
                committed_deadline,
            )) => {
                if committed_sequence != probe_sequence || committed_deadline != deadline {
                    return Err(QuinnOwnerStandbyProbeExpiryError::CompositionInvariant);
                }
                self.next_probe_sequence = probe_sequence.checked_next().ok();
                Ok(QuinnOwnerStandbyProbeExpiryDrive::Expired {
                    probe_sequence,
                    deadline,
                })
            }
            QuinnLegActorInboundTimerPoll::TimerRejected(timer) => {
                self.armed_probe_timer = Some(timer);
                Ok(QuinnOwnerStandbyProbeExpiryDrive::Pending {
                    probe_sequence,
                    deadline,
                })
            }
            QuinnLegActorInboundTimerPoll::TimerInvariant { timer, error } => {
                self.armed_probe_timer = Some(timer);
                match error {
                    OwnerStandbyProbeExpiryError::WrongState
                    | OwnerStandbyProbeExpiryError::WrongCorrelation => {
                        Err(QuinnOwnerStandbyProbeExpiryError::CompositionInvariant)
                    }
                }
            }
            QuinnLegActorInboundTimerPoll::Suppressed { timer, .. }
            | QuinnLegActorInboundTimerPoll::Failed { timer, .. }
            | QuinnLegActorInboundTimerPoll::LocalShutdown(timer) => {
                self.armed_probe_timer = Some(timer);
                Err(QuinnOwnerStandbyProbeExpiryError::ActorUnavailable)
            }
        }
    }

    pub(crate) fn try_flush(
        &mut self,
        now: SimTime,
    ) -> Result<QuinnLegActorFlushDisposition, QuinnLegActorError> {
        self.running_actor_mut()?.try_flush(now)
    }

    /// Consumes at most one actor-published record. A matching ACK advances
    /// freshness from its immutable actor arrival; a typed rejection commits
    /// and drops only that exact input while retaining the awaiting probe.
    pub(crate) fn try_receive_probe_ack(&mut self) -> QuinnOwnerStandbyInboundDrive {
        let gate = QuinnLegCompositionGate::mint();
        let fresh_for = self.liveness.freshness_window;
        let (actor, slot) = match self.running_actor_and_slot_mut() {
            Ok(parts) => parts,
            Err(error) => return QuinnOwnerStandbyInboundDrive::ActorFailed(error),
        };
        let transaction = actor.try_transact_composition_inbound(
            &gate,
            STANDBY_CONTROL_V1,
            |inbound, arrived_at, _queue| match slot
                .observe_exact_probe_ack(inbound, arrived_at, fresh_for)
            {
                Ok(fresh) => Ok(QuinnOwnerStandbyInboundDrive::ProbeFresh {
                    probe_sequence: fresh.probe_sequence(),
                    fresh_until: fresh.fresh_until(),
                }),
                Err(failure) => {
                    let kind = failure.kind();
                    drop(failure.into_inbound());
                    Ok(QuinnOwnerStandbyInboundDrive::Rejected(kind))
                }
            },
        );
        match transaction {
            Ok(QuinnLegActorInboundPoll::Idle) => QuinnOwnerStandbyInboundDrive::Idle,
            Ok(QuinnLegActorInboundPoll::Blocked) => {
                QuinnOwnerStandbyInboundDrive::CompositionInvariant
            }
            Ok(QuinnLegActorInboundPoll::Classified(outcome)) => {
                self.finish_classified_probe(outcome)
            }
            Err(error) => QuinnOwnerStandbyInboundDrive::ActorFailed(error),
        }
    }

    fn finish_classified_probe(
        &mut self,
        outcome: QuinnOwnerStandbyInboundDrive,
    ) -> QuinnOwnerStandbyInboundDrive {
        match outcome {
            outcome @ QuinnOwnerStandbyInboundDrive::ProbeFresh { probe_sequence, .. } => {
                if self.next_probe_sequence != Some(probe_sequence) {
                    return QuinnOwnerStandbyInboundDrive::CompositionInvariant;
                }
                if self
                    .armed_probe_timer
                    .as_ref()
                    .is_some_and(|timer| timer.probe_sequence != probe_sequence)
                {
                    return QuinnOwnerStandbyInboundDrive::CompositionInvariant;
                }
                self.armed_probe_timer = None;
                self.next_probe_sequence = probe_sequence.checked_next().ok();
                outcome
            }
            outcome @ QuinnOwnerStandbyInboundDrive::Rejected(
                OwnerStandbyProbeAckErrorKind::DeadlineExpired,
            ) => {
                let Some(timer) = self.armed_probe_timer.take() else {
                    return QuinnOwnerStandbyInboundDrive::CompositionInvariant;
                };
                if self.next_probe_sequence != Some(timer.probe_sequence) {
                    self.armed_probe_timer = Some(timer);
                    return QuinnOwnerStandbyInboundDrive::CompositionInvariant;
                }
                self.next_probe_sequence = self
                    .next_probe_sequence
                    .and_then(|probe_sequence| probe_sequence.checked_next().ok());
                outcome
            }
            outcome => outcome,
        }
    }

    pub(crate) fn probe_is_fresh_at(&self, now: SimTime) -> bool {
        self.actor_and_slot_are_running()
            && self
                .slot
                .as_ref()
                .is_some_and(|slot| slot.probe_is_fresh_at(now))
    }

    pub(crate) async fn join_for_shutdown(
        &mut self,
    ) -> Result<QuinnOwnerStandbyJoined, QuinnLegActorError> {
        self.start_join_for_shutdown()?;
        if let Some(QuinnOwnerStandbyActorOwner::Joined(joined)) = self.actor.as_ref() {
            return Ok(QuinnOwnerStandbyJoined::observe(joined));
        }
        let outcome = {
            let Some(QuinnOwnerStandbyActorOwner::Joining(task)) = self.actor.as_mut() else {
                return Err(QuinnLegActorError::Protocol(
                    "owner standby join task is missing after start",
                ));
            };
            task.await
        };
        let actor = self.actor.take().ok_or(QuinnLegActorError::Protocol(
            "owner standby actor ownership is missing after join",
        ))?;
        if !matches!(&actor, QuinnOwnerStandbyActorOwner::Joining(_)) {
            self.actor = Some(actor);
            return Err(QuinnLegActorError::Protocol(
                "owner standby join completed outside Joining state",
            ));
        }
        drop(actor);
        match outcome {
            Ok(QuinnOwnerStandbyJoinOutcome { actor, joined }) => match joined {
                Ok(joined) => {
                    drop(actor);
                    let observation = QuinnOwnerStandbyJoined::observe(&joined);
                    self.actor = Some(QuinnOwnerStandbyActorOwner::Joined(Box::new(joined)));
                    self.next_probe_sequence = None;
                    self.armed_probe_timer = None;
                    Ok(observation)
                }
                Err(error) => {
                    self.actor = Some(QuinnOwnerStandbyActorOwner::Running(Box::new(actor)));
                    Err(error)
                }
            },
            Err(error) => {
                let cause: Arc<str> = Arc::from(error.to_string());
                self.actor = Some(QuinnOwnerStandbyActorOwner::TaskLost(Arc::clone(&cause)));
                Err(QuinnLegActorError::TaskJoin(cause.to_string()))
            }
        }
    }

    /// Explicitly settles the exact terminal retained by a structurally joined
    /// actor against the still-retained registered slot. The discarded probe
    /// and registration inventory remains sticky in this composition; join
    /// observation alone can therefore never turn a terminal into false zero.
    pub(crate) fn settle_joined_terminal_for_shutdown(
        &mut self,
    ) -> Result<QuinnOwnerStandbyShutdownTerminalDrive, QuinnOwnerStandbyShutdownTerminalError>
    {
        match &self.retirement {
            QuinnOwnerStandbyRetirementState::PhysicalPostJoin {
                reason, retired, ..
            }
            | QuinnOwnerStandbyRetirementState::ShutdownPostJoin { reason, retired } => {
                return Ok(QuinnOwnerStandbyShutdownTerminalDrive::AlreadySettled {
                    reason: *reason,
                    discarded: retired.discarded(),
                });
            }
            QuinnOwnerStandbyRetirementState::PhysicalPreJoin { .. } => {
                if !matches!(
                    self.actor.as_ref(),
                    Some(QuinnOwnerStandbyActorOwner::Joined(_))
                ) {
                    return Err(QuinnOwnerStandbyShutdownTerminalError::NotJoined);
                }
                let previous = std::mem::replace(
                    &mut self.retirement,
                    QuinnOwnerStandbyRetirementState::Quarantined,
                );
                let QuinnOwnerStandbyRetirementState::PhysicalPreJoin {
                    reason,
                    retired,
                    queue_discarded,
                } = previous
                else {
                    return Err(QuinnOwnerStandbyShutdownTerminalError::CompositionInvariant);
                };
                let discarded = retired.discarded();
                self.retirement = QuinnOwnerStandbyRetirementState::PhysicalPostJoin {
                    reason,
                    retired,
                    queue_discarded,
                };
                return Ok(QuinnOwnerStandbyShutdownTerminalDrive::AlreadySettled {
                    reason,
                    discarded,
                });
            }
            QuinnOwnerStandbyRetirementState::Quarantined => {
                return Err(QuinnOwnerStandbyShutdownTerminalError::CompositionInvariant);
            }
            QuinnOwnerStandbyRetirementState::Unsettled => {}
        }
        let joined = match self.actor.as_mut() {
            Some(QuinnOwnerStandbyActorOwner::Joined(joined)) => joined.as_mut(),
            _ => return Err(QuinnOwnerStandbyShutdownTerminalError::NotJoined),
        };
        let slot = self
            .slot
            .as_mut()
            .ok_or(QuinnOwnerStandbyShutdownTerminalError::MissingSlot)?;
        let gate = QuinnLegCompositionGate::mint();
        let settled = joined.transact_terminal(&gate, |_runtime, terminal| {
            let Some(terminal) = terminal else {
                return Err((
                    None,
                    QuinnOwnerStandbyShutdownTerminalError::MissingTerminal,
                ));
            };
            let reason = terminal.reason();
            match slot.retire_exact_terminal(terminal) {
                Ok(retired) => Ok((reason, retired)),
                Err(failure) => Err((
                    Some(failure.into_terminal()),
                    QuinnOwnerStandbyShutdownTerminalError::WrongTerminal,
                )),
            }
        })?;
        let (reason, retired) = settled;
        let discarded = retired.discarded();
        self.retirement = QuinnOwnerStandbyRetirementState::ShutdownPostJoin { reason, retired };
        self.armed_probe_timer = None;
        Ok(QuinnOwnerStandbyShutdownTerminalDrive::Settled { reason, discarded })
    }

    #[cfg(test)]
    pub(in crate::owned_upstream) fn arm_join_block_for_test(
        &self,
    ) -> Option<QuinnLegActorJoinBlock> {
        match self.actor.as_ref()? {
            QuinnOwnerStandbyActorOwner::Running(actor) => Some(actor.arm_join_block_for_test()),
            QuinnOwnerStandbyActorOwner::Joining(_)
            | QuinnOwnerStandbyActorOwner::Joined(_)
            | QuinnOwnerStandbyActorOwner::TaskLost(_) => None,
        }
    }
}

impl Drop for QuinnOwnerStandbySessionComposition {
    fn drop(&mut self) {
        if let Some(QuinnOwnerStandbyActorOwner::Joining(task)) = self.actor.as_mut() {
            task.abort();
        }
    }
}

#[cfg(test)]
mod tests;
