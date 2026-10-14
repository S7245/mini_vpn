//! Exact controller/output authority for one physical owner session.
//!
//! The parent owner may drive the value-only turns exposed here, but it cannot
//! mint a continuation permit or split a controller from the output marker
//! that schedules it.

mod hard_terminal;

#[cfg(test)]
use super::QuinnOwnerRecoveryServiceDrive;
use super::{
    QuinnOwnerOrdinaryClassified, QuinnOwnerOutputDriveTransaction, QuinnOwnerOutputLedger,
    QuinnOwnerOutputPhase, QuinnOwnerOutputQuarantineCause, QuinnOwnerOutputServiceTurn,
    QuinnOwnerOutputStatus, QuinnOwnerOutputSubroot,
};
use crate::owned_upstream::leg::{EstablishedLeg, LegBoundFrame, LegTransportTerminalReason};
use crate::owned_upstream::leg_io::LegOutboundQueue;
use crate::owned_upstream::owner_output_drain::{
    OwnerOutputAdmissionFailure, OwnerOutputDrainConfigError, OwnerOutputDrainStopReason,
    OwnerOutputLifecycleDrive, OwnerOutputLifecycleResolution, OwnerOutputLifecycleTransaction,
    OwnerTerminalTimerRegistration,
};
use crate::owned_upstream::owner_ready::{
    OwnerPublicationAdmissionFailure, OwnerPublicationTurn, OwnerReadyCategory,
    OwnerReadyContinuationSelection, OwnerReadyReadResult, SendLane, SessionCommandVisit,
};
use crate::owned_upstream::owner_session_controller::{
    OwnerRegisteredRecoveryReady, OwnerSessionActorEvent, OwnerSessionContinuationOutcome,
    OwnerSessionController, OwnerSessionControllerError, OwnerSessionDrive,
    OwnerSessionRegisteredAttachDrive,
};
use crate::owned_upstream::owner_target::OwnerTargetRecoveryHandoff;
use crate::owned_upstream::owner_target::error_inventory::{
    ErrorAuditFailure, ErrorColdInventory, TargetErrorColdAudit,
};
use crate::owned_upstream::quinn_leg_actor::QuinnLegActorError;
use crate::owned_upstream::quinn_leg_composition::QuinnLegCompositionGate;
use crate::owned_upstream::session_clock::SessionMonotonicClock;
use crate::owned_upstream::standby::OwnerStandbySlot;
use crate::owned_upstream::target::TargetIo;
use hard_terminal::QuinnOwnerControllerOutputTerminalState;

/// Nominal authority whose constructor is visible only in this leaf.
struct QuinnOwnerContinuationLease;

/// Linear per-turn controller authority. The type is nameable by the sibling
/// controller, while its field and constructor remain private to this leaf.
pub(in crate::owned_upstream) struct QuinnOwnerContinuationPermit<'turn> {
    _lease: &'turn mut QuinnOwnerContinuationLease,
}

impl<'turn> QuinnOwnerContinuationPermit<'turn> {
    fn mint(lease: &'turn mut QuinnOwnerContinuationLease) -> Self {
        Self { _lease: lease }
    }
}

/// One non-test subroot binds the exact controller, output authority and first
/// controller failure for the lifetime of a physical owner session.
pub(super) struct QuinnOwnerControllerOutputRoot<T: TargetIo> {
    controller: OwnerSessionController<T>,
    output: QuinnOwnerOutputSubroot,
    unsupported_lifecycle: bool,
    controller_failure: QuinnOwnerControllerFailureLedger,
    recovery_service: QuinnOwnerRecoveryServiceState,
    terminal: QuinnOwnerControllerOutputTerminalState,
    continuation_lease: QuinnOwnerContinuationLease,
}

/// Closed construction failure. The not-yet-adopted controller remains paired
/// with the allocation/configuration error instead of escaping as a raw tuple
/// from the controller/output-root boundary.
#[must_use = "failed controller/output adoption retains the exact controller"]
pub(super) struct QuinnOwnerControllerOutputAdoptFailure<T: TargetIo> {
    controller: OwnerSessionController<T>,
    source: OwnerOutputDrainConfigError,
}

impl<T: TargetIo> QuinnOwnerControllerOutputAdoptFailure<T> {
    pub(super) fn into_parts(self) -> (OwnerSessionController<T>, OwnerOutputDrainConfigError) {
        (self.controller, self.source)
    }
}

/// The root retains the exact first controller error. Once failed, no further
/// transaction can be minted, so a second ownership-carrying error cannot be
/// silently discarded by an `Option`-style first-writer helper.
enum QuinnOwnerControllerFailureLedger {
    Ready,
    Failed(OwnerSessionControllerError),
    Invariant,
}

impl QuinnOwnerControllerFailureLedger {
    const fn is_failed(&self) -> bool {
        !matches!(self, Self::Ready)
    }

    fn cold_inventory(&self) -> TargetErrorColdAudit {
        match self {
            Self::Ready => Ok(ErrorColdInventory::empty()),
            Self::Failed(error) => error.cold_inventory(),
            Self::Invariant => Err(ErrorAuditFailure::Invariant),
        }
    }
}

/// Root-owned recovery service phase. A controller call is reachable only
/// from `InFlight { entered: true }`; an interrupted entered turn never
/// reconstructs `Idle` and therefore cannot be driven a second time.
#[derive(Clone, Copy)]
enum QuinnOwnerRecoveryServiceState {
    Idle,
    InFlight {
        entered: bool,
        work: QuinnOwnerServiceWork,
    },
    Quarantined,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum QuinnOwnerServiceWork {
    #[cfg(test)]
    Recovery,
    Send(SendLane),
    Target(OwnerReadyCategory),
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum QuinnOwnerControllerOutputTerminalObservation {
    Awaiting,
    InFlight,
    Applied {
        generation: crate::resumable::LegGeneration,
        reason: LegTransportTerminalReason,
    },
    StandbyRetired {
        reason: LegTransportTerminalReason,
    },
    Quarantined {
        controller_failed: bool,
    },
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum QuinnOwnerControllerOutputTerminalBeginError {
    InFlight,
    AlreadyApplied {
        generation: crate::resumable::LegGeneration,
        reason: LegTransportTerminalReason,
    },
    StandbyRetired {
        reason: LegTransportTerminalReason,
    },
    Quarantined {
        controller_failed: bool,
    },
    ControllerFailed,
    ConcurrentTransaction,
    OutputNotSettled,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum QuinnOwnerControllerOutputTerminalCallbackDrive {
    Applied {
        generation: crate::resumable::LegGeneration,
        reason: LegTransportTerminalReason,
    },
    StandbyRetired {
        reason: LegTransportTerminalReason,
    },
    Quarantined {
        controller_failed: bool,
    },
}

impl QuinnOwnerRecoveryServiceState {
    const fn inventory(self) -> (u8, bool) {
        match self {
            Self::Idle => (0, false),
            Self::InFlight { .. } => (1, false),
            Self::Quarantined => (1, true),
        }
    }
}

struct QuinnOwnerRecoveryServiceTransaction<'root, T: TargetIo> {
    root: &'root mut QuinnOwnerControllerOutputRoot<T>,
}

/// Exact ordinary input transaction minted only after the root has reserved
/// its current output admission seam. It exclusively borrows the complete root;
/// controller, output and failure ownership cannot be cross-paired.
pub(super) struct QuinnOwnerControllerOutputInboundTransaction<'root, T: TargetIo> {
    root: &'root mut QuinnOwnerControllerOutputRoot<T>,
}

/// A transaction borrows the whole root, so safe code cannot combine an
/// output marker from root X with a controller from root Y.
pub(super) struct QuinnOwnerOutputContinuationTransaction<'root, T: TargetIo> {
    root: &'root mut QuinnOwnerControllerOutputRoot<T>,
}

pub(super) enum QuinnOwnerOutputContinuationDrive {
    Admitted {
        outputs: usize,
        needs_continue: bool,
    },
    AdmissionRetained,
    ControllerFailed,
    ControllerMissing,
    CompositionInvariant,
}

pub(super) struct OwnerReadyServicePlan {
    pub(super) sources: bool,
    pub(super) terminal_install: bool,
    pub(super) terminal_timer: bool,
    pub(super) output_frame: bool,
    pub(super) eligible: usize,
    pub(super) requested: usize,
}

impl<T: TargetIo> QuinnOwnerControllerOutputRoot<T> {
    // Bounded classification of this original output ledger. No Flow walk,
    // guessed readiness or second page/continuation state is exported.
    pub(super) fn ready_service_plan(
        &mut self,
        output_can_admit: bool,
        timer: &OwnerTerminalTimerRegistration,
    ) -> Result<OwnerReadyServicePlan, QuinnLegActorError> {
        use OwnerReadyCategory as Category;
        if self.controller_failed() || self.output.admission_failure.is_some() {
            self.output.fail();
            return Err(QuinnLegActorError::Protocol(
                "ready service retains controller failure",
            ));
        }
        if !matches!(
            self.output.ledger.status,
            QuinnOwnerOutputStatus::Live { .. }
        ) {
            return Err(QuinnLegActorError::Protocol(
                "ready service requires live output custody",
            ));
        }
        if let Err(cause) = self.output.preflight_live_phase() {
            self.output.quarantine_live(None, cause);
            return Err(QuinnLegActorError::Protocol(
                "ready output phase failed preflight",
            ));
        }
        if self.output.lifecycle.has_pending() {
            // Only selected owning service inspects opaque scalar metadata.
            let terminal_install = !self.unsupported_lifecycle;
            return Ok(OwnerReadyServicePlan {
                sources: false,
                terminal_install,
                terminal_timer: false,
                output_frame: false,
                eligible: Category::Output.mask(),
                requested: if terminal_install {
                    Category::Output.mask()
                } else {
                    0
                },
            });
        }
        self.unsupported_lifecycle = false;
        let QuinnOwnerOutputStatus::Live { phase } = &self.output.ledger.status else {
            return Err(QuinnLegActorError::Protocol("ready output is not live"));
        };
        let (sources, requested) = match phase {
            QuinnOwnerOutputPhase::Vacant => (true, 0),
            QuinnOwnerOutputPhase::Page => (
                false,
                if output_can_admit {
                    Category::Output.mask()
                } else {
                    0
                },
            ),
            QuinnOwnerOutputPhase::Drained(page) if page.needs_continue() => {
                (false, Category::Effects.mask())
            }
            QuinnOwnerOutputPhase::Drained(_) => (false, Category::Output.mask()),
        };
        let eligible = Category::Output.mask()
            | if sources {
                Category::TargetCommon.mask()
                    | Category::Credit.mask()
                    | Category::ReadReady.mask()
                    | Category::ReadAttempt.mask()
                    | Category::Retirement.mask()
                    | Category::Completion.mask()
            } else if requested & Category::Effects.mask() != 0 {
                Category::Effects.mask()
            } else {
                0
            };
        let terminal_timer = sources && self.controller.terminal_timer_requested(timer);
        Ok(OwnerReadyServicePlan {
            sources,
            terminal_install: false,
            terminal_timer,
            output_frame: requested & Category::Output.mask() != 0,
            eligible,
            requested: requested
                | if terminal_timer {
                    Category::Output.mask()
                } else {
                    0
                },
        })
    }

    pub(super) fn install_terminal_action(&mut self) -> Result<(), QuinnLegActorError> {
        let kind = QuinnOwnerServiceWork::Target(OwnerReadyCategory::Output);
        let mut turn = self.begin_owner_service(kind)?;
        if !turn.enter(kind) {
            return Err(turn.settle_invariant("terminal installation could not enter"));
        }
        match turn
            .root
            .controller
            .install_terminal_action(&mut turn.root.output.lifecycle)
        {
            Ok(installed) => {
                turn.root.unsupported_lifecycle = !installed;
                turn.settle_idle();
                Ok(())
            }
            Err(error) => Err(turn.settle_controller_error(error)),
        }
    }

    pub(super) fn begin_ready_publication(
        &self,
    ) -> Result<OwnerPublicationTurn, OwnerPublicationAdmissionFailure> {
        self.controller.begin_ready_publication()
    }

    pub(super) fn service_ready_send(
        &mut self,
        lane: SendLane,
    ) -> Result<SessionCommandVisit, QuinnLegActorError> {
        let mut turn = self.begin_owner_service(QuinnOwnerServiceWork::Send(lane))?;
        if !turn.enter(QuinnOwnerServiceWork::Send(lane)) {
            return Err(turn.settle_invariant("selected send service could not enter"));
        }
        match turn.root.controller.service_ready_send(lane) {
            Ok(result) => {
                turn.settle_idle();
                Ok(result)
            }
            Err(error) => Err(turn.settle_controller_error(error)),
        }
    }

    pub(super) fn service_ready_target(
        &mut self,
        selected: &OwnerReadyContinuationSelection,
    ) -> Result<OwnerReadyReadResult, QuinnLegActorError> {
        let kind = QuinnOwnerServiceWork::Target(selected.category);
        let mut turn = self.begin_owner_service(kind)?;
        if !turn.enter(kind) {
            return Err(turn.settle_invariant("selected Target service could not enter"));
        }
        match turn.root.controller.service_ready_target(selected) {
            Ok(result) => {
                turn.settle_idle();
                Ok(result)
            }
            Err(error) => Err(turn.settle_controller_error(error)),
        }
    }

    pub(super) fn select_ready_continuation(
        &mut self,
        eligible: usize,
        requested: usize,
    ) -> Result<Option<OwnerReadyContinuationSelection>, QuinnLegActorError> {
        let mut turn = self.begin_owner_service(QuinnOwnerServiceWork::Target(
            OwnerReadyCategory::TargetCommon,
        ))?;
        if !turn.enter(QuinnOwnerServiceWork::Target(
            OwnerReadyCategory::TargetCommon,
        )) {
            return Err(turn.settle_invariant("continuation selection could not enter"));
        }
        match turn
            .root
            .controller
            .select_ready_continuation(eligible, requested)
        {
            Ok(selected) => {
                turn.settle_idle();
                Ok(selected)
            }
            Err(error) => Err(turn.settle_controller_error(error)),
        }
    }

    pub(super) fn finish_ready_continuation(
        &mut self,
        selected: &OwnerReadyContinuationSelection,
        result: OwnerReadyReadResult,
        eligible: usize,
    ) -> Result<usize, QuinnLegActorError> {
        let kind = QuinnOwnerServiceWork::Target(selected.category);
        let mut turn = self.begin_owner_service(kind)?;
        if !turn.enter(kind) {
            return Err(turn.settle_invariant("continuation bookkeeping could not enter"));
        }
        match turn
            .root
            .controller
            .finish_ready_continuation(selected, result, eligible)
        {
            Ok(mask) => {
                turn.settle_idle();
                Ok(mask)
            }
            Err(error) => Err(turn.settle_controller_error(error)),
        }
    }

    pub(super) fn quarantine_ready_service(&mut self) {
        self.recovery_service = QuinnOwnerRecoveryServiceState::Quarantined;
        self.output
            .quarantine_live(None, QuinnOwnerOutputQuarantineCause::UnsettledContinuation);
    }

    pub(super) fn new(
        controller: OwnerSessionController<T>,
    ) -> Result<Self, QuinnOwnerControllerOutputAdoptFailure<T>> {
        let capacity = controller.output_page_capacity();
        let output = match QuinnOwnerOutputSubroot::new(
            capacity.max_outputs(),
            capacity.max_encoded_bytes(),
        ) {
            Ok(output) => output,
            Err(source) => {
                return Err(QuinnOwnerControllerOutputAdoptFailure { controller, source });
            }
        };
        Ok(Self {
            controller,
            output,
            unsupported_lifecycle: false,
            controller_failure: QuinnOwnerControllerFailureLedger::Ready,
            recovery_service: QuinnOwnerRecoveryServiceState::Idle,
            terminal: QuinnOwnerControllerOutputTerminalState::Awaiting,
            continuation_lease: QuinnOwnerContinuationLease,
        })
    }

    pub(super) fn shares_session_clock(
        &self,
        clock: &SessionMonotonicClock,
        gate: &QuinnLegCompositionGate,
    ) -> bool {
        self.controller.shares_session_clock(clock, gate)
    }

    pub(super) const fn controller_failed(&self) -> bool {
        self.controller_failure.is_failed()
            || !matches!(self.recovery_service, QuinnOwnerRecoveryServiceState::Idle)
            || !matches!(
                self.terminal,
                QuinnOwnerControllerOutputTerminalState::Awaiting
            )
    }

    pub(super) fn output_inventory(&self) -> super::QuinnOwnerOutputRootInventory {
        let (transactions, quarantined) = self.recovery_service.inventory();
        let snapshot = self.controller.snapshot();
        let (cold_exact_zero, cold_audit_failed, c_error) = match self.controller.cold_inventory() {
            Ok(inventory) => (inventory.is_exact_zero(), false, inventory.error_payload()),
            Err(_) => (false, true, Err(ErrorAuditFailure::Invariant)),
        };
        // These are distinct current owners, not two summaries of the same
        // payload. Unknown stays in C; a normally returned error stays in Root.
        let error_payload = c_error.and_then(|c| {
            self.controller_failure
                .cold_inventory()
                .and_then(|root| c.checked_add(root))
        });
        let controller_failures = u8::from(self.controller_failure.is_failed());
        let (
            terminal_transactions,
            terminal_residuals,
            terminal_stamps,
            terminal_quarantined,
            controller_terminal_correlation_valid,
        ) = self.terminal.inventory(snapshot);
        self.output
            .inventory()
            .with_recovery_service(transactions, quarantined)
            .with_controller_terminal(
                snapshot,
                cold_exact_zero,
                cold_audit_failed,
                controller_failures,
                terminal_transactions,
                terminal_residuals,
                terminal_stamps,
                terminal_quarantined,
                controller_terminal_correlation_valid,
            )
            .with_controller_error_payload(error_payload)
    }

    pub(super) fn fail_output(&mut self) {
        self.output.fail();
    }

    pub(super) fn stop_output(
        &mut self,
        reason: OwnerOutputDrainStopReason,
    ) -> super::QuinnOwnerOutputStopDrive {
        match self.output.stop(reason) {
            super::QuinnOwnerOutputStopDrive::Stopped { reason, .. } => {
                super::QuinnOwnerOutputStopDrive::Stopped {
                    reason,
                    inventory: self.output_inventory(),
                }
            }
            super::QuinnOwnerOutputStopDrive::AlreadyStopped { reason, .. } => {
                super::QuinnOwnerOutputStopDrive::AlreadyStopped {
                    reason,
                    inventory: self.output_inventory(),
                }
            }
            super::QuinnOwnerOutputStopDrive::CompositionInvariant { .. } => {
                super::QuinnOwnerOutputStopDrive::CompositionInvariant {
                    inventory: self.output_inventory(),
                }
            }
        }
    }

    pub(super) fn transact_installed_lifecycle<'owner, 'slot>(
        &'owner mut self,
        commit: impl FnOnce(
            OwnerOutputLifecycleTransaction<'owner>,
        ) -> OwnerOutputLifecycleResolution<'owner, 'slot>,
    ) -> OwnerOutputLifecycleDrive {
        self.output.transact_installed_lifecycle(commit)
    }

    pub(super) fn transact_stopped_lifecycle<'owner, 'slot>(
        &'owner mut self,
        commit: impl FnOnce(
            OwnerOutputLifecycleTransaction<'owner>,
        ) -> OwnerOutputLifecycleResolution<'owner, 'slot>,
    ) -> OwnerOutputLifecycleDrive {
        self.output.transact_stopped_lifecycle(commit)
    }

    pub(super) fn begin_inbound_admission(
        &mut self,
    ) -> Option<QuinnOwnerControllerOutputInboundTransaction<'_, T>> {
        if self.controller_failed()
            || self.output.admission_failure.is_some()
            || self.output.lifecycle.has_pending()
        {
            return None;
        }
        if !matches!(
            self.output.ledger.status,
            QuinnOwnerOutputStatus::Live { .. }
        ) {
            return None;
        }
        if let Err(cause) = self.output.preflight_live_phase() {
            self.output.quarantine_live(None, cause);
            return None;
        }
        let QuinnOwnerOutputStatus::Live {
            phase: QuinnOwnerOutputPhase::Vacant,
        } = self.output.ledger.status
        else {
            return None;
        };
        let inventory = self.output.ledger.drain.inventory().ok()?;
        if !inventory.is_exact_zero() {
            return None;
        }
        self.output.ledger.status = QuinnOwnerOutputStatus::OrdinaryInFlight { entered: false };
        Some(QuinnOwnerControllerOutputInboundTransaction { root: self })
    }

    pub(super) fn activate_owner_ready(
        &mut self,
        ready: &mut crate::owned_upstream::owner_ready::ReadyIndex,
        position: usize,
        handoff: &crate::owned_upstream::owner_target::OwnerTargetInitialHandoff,
        runtime: &crate::owned_upstream::leg_io::PhysicalLegRuntimeIncarnation,
        io: &crate::owned_upstream::leg_io::LegIo,
        queue: &LegOutboundQueue,
    ) -> Result<
        crate::owned_upstream::owner_ready::OwnerReadyActivatedSession,
        crate::owned_upstream::owner_session_controller::OwnerReadyActivationBlock,
    > {
        use crate::owned_upstream::owner_session_controller::OwnerReadyActivationBlock as Block;
        if self.controller_failed()
            || self.output.admission_failure.is_some()
            || self.output.lifecycle.has_pending()
            || !matches!(
                self.output.ledger.status,
                QuinnOwnerOutputStatus::Live {
                    phase: QuinnOwnerOutputPhase::Vacant
                }
            )
            || self.output.preflight_live_phase().is_err()
            || !matches!(self.output.ledger.drain.inventory(), Ok(inventory) if inventory.is_exact_zero())
        {
            // Adoption does not enter an ordinary turn or normalize a retained
            // ledger. The rejected whole root keeps every existing disposition.
            return Err(Block::OutputUnavailable);
        }
        self.controller
            .activate_owner_ready(ready, position, handoff, runtime, io, queue)
    }

    pub(super) fn admission_failed(&self) -> bool {
        self.output.admission_failed()
    }

    pub(super) fn begin_service_turn(&mut self) -> QuinnOwnerOutputServiceTurn<'_, T> {
        if let QuinnOwnerOutputStatus::Stopped { reason, .. } = self.output.ledger.status {
            return QuinnOwnerOutputServiceTurn::Stopped {
                reason,
                inventory: self.output_inventory(),
            };
        }
        if matches!(
            self.output.ledger.status,
            QuinnOwnerOutputStatus::OrdinaryInFlight { .. }
                | QuinnOwnerOutputStatus::OrdinaryQuarantined { .. }
                | QuinnOwnerOutputStatus::RejectedPage { .. }
                | QuinnOwnerOutputStatus::ContinuationInFlight { .. }
                | QuinnOwnerOutputStatus::ContinuationQuarantined { .. }
                | QuinnOwnerOutputStatus::Quarantined { .. }
        ) {
            if let QuinnOwnerOutputStatus::ContinuationInFlight { page, .. } =
                self.output.ledger.status
            {
                self.output.ledger.status = QuinnOwnerOutputStatus::ContinuationQuarantined {
                    page,
                    first_stop_reason: None,
                };
            }
            return QuinnOwnerOutputServiceTurn::CompositionInvariant;
        }
        if let Err(cause) = self.output.preflight_live_phase() {
            self.output.quarantine_live(None, cause);
            return QuinnOwnerOutputServiceTurn::CompositionInvariant;
        }
        if self.output.admission_failure.is_some() {
            self.output.fail();
            return QuinnOwnerOutputServiceTurn::CompositionInvariant;
        }
        if self.controller_failed() {
            self.output.fail();
            return QuinnOwnerOutputServiceTurn::ControllerFailed;
        }
        if self.output.lifecycle.has_pending() {
            return QuinnOwnerOutputServiceTurn::LifecycleInstallBlocked;
        }

        let continuation_page = match &self.output.ledger.status {
            QuinnOwnerOutputStatus::Live {
                phase: QuinnOwnerOutputPhase::Drained(page),
            } if page.needs_continue() => Some(*page),
            _ => None,
        };
        if let Some(page) = continuation_page {
            self.output.ledger.status = QuinnOwnerOutputStatus::ContinuationInFlight {
                page,
                entered: false,
            };
            return QuinnOwnerOutputServiceTurn::Continue(
                QuinnOwnerOutputContinuationTransaction { root: self },
            );
        }

        let QuinnOwnerOutputStatus::Live { phase } = &mut self.output.ledger.status else {
            return QuinnOwnerOutputServiceTurn::CompositionInvariant;
        };
        match *phase {
            QuinnOwnerOutputPhase::Vacant => QuinnOwnerOutputServiceTurn::Inert,
            QuinnOwnerOutputPhase::Drained(page) if !page.needs_continue() => {
                *phase = QuinnOwnerOutputPhase::Vacant;
                QuinnOwnerOutputServiceTurn::CompletedBoundary
            }
            QuinnOwnerOutputPhase::Drained(_) => QuinnOwnerOutputServiceTurn::CompositionInvariant,
            QuinnOwnerOutputPhase::Page => {
                QuinnOwnerOutputServiceTurn::Drive(QuinnOwnerOutputDriveTransaction {
                    drain: &mut self.output.ledger.drain,
                    phase,
                    lifecycle: &mut self.output.lifecycle,
                })
            }
        }
    }

    #[cfg(test)]
    pub(super) fn service_recovery_with_parts(
        &mut self,
        active_b: &mut Option<OwnerTargetRecoveryHandoff>,
        established_leg: &EstablishedLeg,
        queue: &mut LegOutboundQueue,
        gate: &QuinnLegCompositionGate,
    ) -> Result<QuinnOwnerRecoveryServiceDrive, QuinnLegActorError> {
        self.begin_recovery_service()?
            .service(active_b, established_leg, queue, gate)
    }

    #[cfg(test)]
    fn begin_recovery_service(
        &mut self,
    ) -> Result<QuinnOwnerRecoveryServiceTransaction<'_, T>, QuinnLegActorError> {
        self.begin_owner_service(QuinnOwnerServiceWork::Recovery)
    }

    fn begin_owner_service(
        &mut self,
        work: QuinnOwnerServiceWork,
    ) -> Result<QuinnOwnerRecoveryServiceTransaction<'_, T>, QuinnLegActorError> {
        if self.controller_failure.is_failed() {
            self.output.fail();
            return Err(QuinnLegActorError::Protocol(
                "owner controller-output root was driven after controller failure",
            ));
        }
        match self.recovery_service {
            QuinnOwnerRecoveryServiceState::Idle => {
                self.recovery_service = QuinnOwnerRecoveryServiceState::InFlight {
                    entered: false,
                    work,
                };
            }
            QuinnOwnerRecoveryServiceState::InFlight { .. }
            | QuinnOwnerRecoveryServiceState::Quarantined => {
                self.recovery_service = QuinnOwnerRecoveryServiceState::Quarantined;
                self.output
                    .quarantine_live(None, QuinnOwnerOutputQuarantineCause::UnsettledContinuation);
                return Err(QuinnLegActorError::Protocol(
                    "owner recovery controller service transaction is unsettled",
                ));
            }
        }
        Ok(QuinnOwnerRecoveryServiceTransaction { root: self })
    }

    #[cfg(test)]
    pub(super) fn snapshot(
        &self,
    ) -> crate::owned_upstream::owner_session_controller::OwnerSessionControllerSnapshot {
        self.controller.snapshot()
    }

    #[cfg(test)]
    fn controller_failure_for_test(&self) -> Option<&OwnerSessionControllerError> {
        match &self.controller_failure {
            QuinnOwnerControllerFailureLedger::Failed(error) => Some(error),
            QuinnOwnerControllerFailureLedger::Ready
            | QuinnOwnerControllerFailureLedger::Invariant => None,
        }
    }
}

impl<T: TargetIo> QuinnOwnerRecoveryServiceTransaction<'_, T> {
    #[cfg(test)]
    fn service(
        mut self,
        active_b: &mut Option<OwnerTargetRecoveryHandoff>,
        established_leg: &EstablishedLeg,
        queue: &mut LegOutboundQueue,
        gate: &QuinnLegCompositionGate,
    ) -> Result<QuinnOwnerRecoveryServiceDrive, QuinnLegActorError> {
        if !self.enter(QuinnOwnerServiceWork::Recovery) {
            return Err(self.settle_invariant(
                "owner recovery controller service transaction could not enter",
            ));
        }
        if let Err(cause) = self.root.output.preflight_live_phase() {
            self.root.controller_failure = QuinnOwnerControllerFailureLedger::Invariant;
            self.root.recovery_service = QuinnOwnerRecoveryServiceState::Quarantined;
            self.root.output.quarantine_live(None, cause);
            return Err(QuinnLegActorError::Protocol(
                "owner recovery controller service output preflight failed",
            ));
        }

        let snapshot = self.root.controller.snapshot();
        if snapshot.recovery_handoff_ready {
            return match self.root.controller.install_recovery_handoff(
                active_b,
                established_leg,
                gate,
            ) {
                Ok(Some(generation)) => {
                    self.settle_idle();
                    Ok(QuinnOwnerRecoveryServiceDrive::Installed { generation })
                }
                Ok(None) => Err(self.settle_invariant(
                    "controller reported a ready recovery handoff without installing it",
                )),
                Err(error) if error.is_recovery_queue_pressure() => {
                    drop(error);
                    self.settle_idle();
                    Ok(QuinnOwnerRecoveryServiceDrive::Pressure)
                }
                Err(error) => Err(self.settle_controller_error(error)),
            };
        }

        let drive = match if snapshot.recovery_needs_acceptance {
            self.root
                .controller
                .enqueue_recovery_acceptance(queue, gate)
        } else if snapshot.recovery_needs_execute {
            self.root.controller.execute_attached_recovery(queue, gate)
        } else if snapshot.recovery_needs_publish {
            self.root.controller.publish_attached_recovery(queue, gate)
        } else {
            self.settle_idle();
            return Ok(QuinnOwnerRecoveryServiceDrive::Inert);
        } {
            Ok(drive) => drive,
            Err(error) if error.is_recovery_queue_pressure() => {
                drop(error);
                self.settle_idle();
                return Ok(QuinnOwnerRecoveryServiceDrive::Pressure);
            }
            Err(error) => return Err(self.settle_controller_error(error)),
        };

        let installed_generation = if drive.handoff_ready() {
            match self
                .root
                .controller
                .install_recovery_handoff(active_b, established_leg, gate)
            {
                Ok(Some(generation)) => Some(generation),
                Ok(None) => {
                    return Err(
                        self.settle_invariant("completed recovery handoff was not installed")
                    );
                }
                Err(error) if error.is_recovery_queue_pressure() => {
                    drop(error);
                    self.settle_idle();
                    return Ok(QuinnOwnerRecoveryServiceDrive::Pressure);
                }
                Err(error) => return Err(self.settle_controller_error(error)),
            }
        } else {
            None
        };
        self.settle_idle();
        Ok(QuinnOwnerRecoveryServiceDrive::Progress {
            drive,
            installed_generation,
        })
    }

    fn enter(&mut self, expected: QuinnOwnerServiceWork) -> bool {
        match &mut self.root.recovery_service {
            QuinnOwnerRecoveryServiceState::InFlight { entered, work }
                if !*entered && *work == expected =>
            {
                *entered = true;
                true
            }
            _ => false,
        }
    }

    fn settle_idle(&mut self) {
        self.root.recovery_service = QuinnOwnerRecoveryServiceState::Idle;
    }

    fn settle_controller_error(
        &mut self,
        error: OwnerSessionControllerError,
    ) -> QuinnLegActorError {
        debug_assert!(matches!(
            self.root.controller_failure,
            QuinnOwnerControllerFailureLedger::Ready
        ));
        self.root.controller_failure = QuinnOwnerControllerFailureLedger::Failed(error);
        self.root.output.fail();
        self.settle_idle();
        QuinnLegActorError::Protocol("owner recovery controller service failed")
    }

    fn settle_invariant(&mut self, message: &'static str) -> QuinnLegActorError {
        debug_assert!(matches!(
            self.root.controller_failure,
            QuinnOwnerControllerFailureLedger::Ready
        ));
        self.root.controller_failure = QuinnOwnerControllerFailureLedger::Invariant;
        self.root.recovery_service = QuinnOwnerRecoveryServiceState::Quarantined;
        self.root
            .output
            .quarantine_live(None, QuinnOwnerOutputQuarantineCause::UnsettledContinuation);
        QuinnLegActorError::Protocol(message)
    }
}

impl<T: TargetIo> Drop for QuinnOwnerRecoveryServiceTransaction<'_, T> {
    fn drop(&mut self) {
        match self.root.recovery_service {
            QuinnOwnerRecoveryServiceState::InFlight { entered: false, .. } => {
                self.root.recovery_service = QuinnOwnerRecoveryServiceState::Idle;
            }
            QuinnOwnerRecoveryServiceState::InFlight { entered: true, .. } => {
                self.root.recovery_service = QuinnOwnerRecoveryServiceState::Quarantined;
                self.root
                    .output
                    .quarantine_live(None, QuinnOwnerOutputQuarantineCause::UnsettledContinuation);
            }
            QuinnOwnerRecoveryServiceState::Idle | QuinnOwnerRecoveryServiceState::Quarantined => {}
        }
    }
}

impl<T: TargetIo> QuinnOwnerControllerOutputInboundTransaction<'_, T> {
    pub(super) fn expire_terminal_and_admit(
        mut self,
        timer: &mut OwnerTerminalTimerRegistration,
        context: &mut std::task::Context<'_>,
    ) -> Result<OwnerReadyReadResult, ()> {
        match self.root.controller.poll_terminal_timer(timer, context) {
            Ok(std::task::Poll::Pending) => return Ok(OwnerReadyReadResult::Pending),
            Ok(std::task::Poll::Ready(())) => {}
            Err(error) => {
                self.settle_controller_failure(error);
                return Err(());
            }
        }
        if !self.enter() {
            return Err(());
        }
        let drive = match self.root.controller.expire_original_terminal(timer) {
            Ok(drive) => drive,
            Err(error) => {
                self.settle_controller_failure(error);
                return Err(());
            }
        };
        self.admit_ordinary(drive)?;
        match self.root.controller.settle_terminal_expiry_output(timer) {
            Ok(()) => Ok(OwnerReadyReadResult::Pending),
            Err(error) => {
                // The returned page is still held by this output ledger;
                // retained C failure prevents premature frame publication.
                self.settle_controller_failure(error);
                Err(())
            }
        }
    }

    pub(super) fn complete_selected_and_admit(
        mut self,
        selected: &OwnerReadyContinuationSelection,
    ) -> Result<(QuinnOwnerOrdinaryClassified, OwnerReadyReadResult), ()> {
        if !self.enter() {
            return Err(());
        }
        match self.root.controller.apply_ready_completion(selected) {
            Ok((drive, result)) => self
                .admit_ordinary(drive)
                .map(|admitted| (admitted, result)),
            Err(error) => {
                self.settle_controller_failure(error);
                Err(())
            }
        }
    }

    /// Complete selected paired-inbox operation. No caller can receive a raw
    /// command, supply a timestamp, or drive reduction outside this guard.
    pub(super) fn receive_selected_and_admit(
        mut self,
        lane: crate::owned_upstream::session::SessionMailboxLane,
    ) -> Result<std::task::Poll<Option<QuinnOwnerOrdinaryClassified>>, ()> {
        use std::task::Poll;
        if !self.can_admit_ordinary() {
            return Err(());
        }
        let observed_at = match self.root.controller.poll_ready_receipt(lane) {
            Poll::Pending => return Ok(Poll::Pending),
            Poll::Ready(Ok(None)) => return Ok(Poll::Ready(None)),
            Poll::Ready(Ok(Some(observed_at))) => observed_at,
            Poll::Ready(Err(error)) => {
                self.settle_controller_failure(error);
                return Err(());
            }
        };
        if !self.enter() {
            return Err(());
        }
        match self.root.controller.apply_ready_receipt(observed_at) {
            Ok(drive) => self
                .admit_ordinary(drive)
                .map(|drive| Poll::Ready(Some(drive))),
            Err(error) => {
                self.settle_controller_failure(error);
                Err(())
            }
        }
    }

    pub(super) fn restore_unstarted(self) {
        drop(self);
    }

    pub(super) fn can_admit_ordinary(&self) -> bool {
        matches!(
            self.root.output.ledger.status,
            QuinnOwnerOutputStatus::OrdinaryInFlight { entered: false }
        ) && !self.root.controller_failure.is_failed()
            && self.root.output.admission_failure.is_none()
    }

    pub(super) fn preflight_registered_recovery(&self) -> Result<OwnerRegisteredRecoveryReady, ()> {
        if !self.can_admit_ordinary() {
            return Err(());
        }
        self.root
            .controller
            .preflight_registered_recovery()
            .map_err(drop)
    }

    pub(super) fn accept_registered_owner_attach(
        mut self,
        ready: OwnerRegisteredRecoveryReady,
        slot: &mut OwnerStandbySlot,
        established_leg: &EstablishedLeg,
        received: LegBoundFrame,
        queue: &mut LegOutboundQueue,
        gate: &QuinnLegCompositionGate,
    ) -> Result<OwnerSessionRegisteredAttachDrive, ()> {
        if !self.enter() {
            return Err(());
        }
        match self.root.controller.accept_registered_owner_attach(
            ready,
            slot,
            established_leg,
            received,
            queue,
            gate,
        ) {
            Ok(drive) => {
                self.settle_live(QuinnOwnerOutputPhase::Vacant);
                Ok(drive)
            }
            Err(error) => {
                self.settle_controller_failure(error);
                Err(())
            }
        }
    }

    pub(super) fn apply_actor_event_and_admit(
        mut self,
        input: OwnerSessionActorEvent,
    ) -> Result<QuinnOwnerOrdinaryClassified, ()> {
        if !self.enter() {
            return Err(());
        }
        match self.root.controller.apply_actor_event(input) {
            Ok(drive) => self.admit_ordinary(drive),
            Err(error) => {
                self.settle_controller_failure(error);
                Err(())
            }
        }
    }

    fn enter(&mut self) -> bool {
        match &mut self.root.output.ledger.status {
            QuinnOwnerOutputStatus::OrdinaryInFlight { entered } if !*entered => {
                *entered = true;
                true
            }
            _ => false,
        }
    }

    fn admit_ordinary(
        &mut self,
        drive: OwnerSessionDrive,
    ) -> Result<QuinnOwnerOrdinaryClassified, ()> {
        let needs_continue = drive.needs_continue();
        let outputs = drive.into_outputs();
        let output_count = outputs.len();
        match self
            .root
            .output
            .ledger
            .drain
            .admit_outputs(outputs, needs_continue)
        {
            Ok(()) => {
                self.settle_live(QuinnOwnerOutputPhase::Page);
                Ok(QuinnOwnerOrdinaryClassified {
                    outputs: output_count,
                    needs_continue,
                })
            }
            Err(failure) => {
                self.root.output.admission_failure = Some(failure);
                self.root.output.ledger.status = QuinnOwnerOutputStatus::RejectedPage {
                    first_stop_reason: None,
                };
                Err(())
            }
        }
    }

    fn settle_live(&mut self, phase: QuinnOwnerOutputPhase) {
        self.root.output.ledger.status = QuinnOwnerOutputStatus::Live { phase };
    }

    fn settle_controller_failure(&mut self, error: OwnerSessionControllerError) {
        debug_assert!(matches!(
            self.root.controller_failure,
            QuinnOwnerControllerFailureLedger::Ready
        ));
        self.root.controller_failure = QuinnOwnerControllerFailureLedger::Failed(error);
        self.root.output.ledger.status = QuinnOwnerOutputStatus::OrdinaryQuarantined {
            first_stop_reason: None,
        };
    }
}

impl<T: TargetIo> Drop for QuinnOwnerControllerOutputInboundTransaction<'_, T> {
    fn drop(&mut self) {
        match self.root.output.ledger.status {
            QuinnOwnerOutputStatus::OrdinaryInFlight { entered: false } => {
                self.root.output.ledger.status = QuinnOwnerOutputStatus::Live {
                    phase: QuinnOwnerOutputPhase::Vacant,
                };
            }
            QuinnOwnerOutputStatus::OrdinaryInFlight { entered: true } => {
                self.root.output.ledger.status = QuinnOwnerOutputStatus::OrdinaryQuarantined {
                    first_stop_reason: None,
                };
            }
            _ => {}
        }
    }
}

impl<T: TargetIo> QuinnOwnerOutputContinuationTransaction<'_, T> {
    pub(super) fn restore_unstarted(mut self) {
        self.restore_prepared();
    }

    pub(super) fn continue_and_settle(mut self) -> QuinnOwnerOutputContinuationDrive {
        if !self.enter() {
            self.quarantine(QuinnOwnerOutputQuarantineCause::UnsettledContinuation);
            return QuinnOwnerOutputContinuationDrive::CompositionInvariant;
        }
        let outcome = {
            let permit = QuinnOwnerContinuationPermit::mint(&mut self.root.continuation_lease);
            self.root.controller.continue_effects(permit)
        };
        self.settle_outcome(outcome)
    }

    fn settle_outcome(
        &mut self,
        outcome: OwnerSessionContinuationOutcome,
    ) -> QuinnOwnerOutputContinuationDrive {
        let expected = matches!(
            self.root.output.ledger.status,
            QuinnOwnerOutputStatus::ContinuationInFlight { entered: true, .. }
        );

        let drive = match outcome {
            OwnerSessionContinuationOutcome::Advanced(drive) => self.admit_advanced(drive),
            OwnerSessionContinuationOutcome::TerminalRetained(error) => {
                self.settle_controller_failure(error);
                let page = match self.root.output.ledger.status {
                    QuinnOwnerOutputStatus::ContinuationInFlight { page, .. } => page,
                    _ => {
                        self.quarantine(QuinnOwnerOutputQuarantineCause::UnsettledContinuation);
                        return QuinnOwnerOutputContinuationDrive::CompositionInvariant;
                    }
                };
                self.root.output.ledger.status = QuinnOwnerOutputStatus::ContinuationQuarantined {
                    page,
                    first_stop_reason: None,
                };
                QuinnOwnerOutputContinuationDrive::ControllerFailed
            }
            OwnerSessionContinuationOutcome::Aborted(error) => {
                self.settle_controller_failure(error);
                self.root.output.ledger.status = QuinnOwnerOutputStatus::Quarantined {
                    phase: QuinnOwnerOutputPhase::Vacant,
                    first_stop_reason: None,
                    _cause: QuinnOwnerOutputQuarantineCause::AbortedControllerContinuation,
                };
                QuinnOwnerOutputContinuationDrive::ControllerFailed
            }
            OwnerSessionContinuationOutcome::Missing(error) => {
                self.settle_controller_failure(error);
                self.root.output.ledger.status = QuinnOwnerOutputStatus::Quarantined {
                    phase: QuinnOwnerOutputPhase::Vacant,
                    first_stop_reason: None,
                    _cause: QuinnOwnerOutputQuarantineCause::MissingControllerContinuation,
                };
                QuinnOwnerOutputContinuationDrive::ControllerMissing
            }
        };
        if expected {
            drive
        } else {
            self.quarantine(QuinnOwnerOutputQuarantineCause::UnsettledContinuation);
            QuinnOwnerOutputContinuationDrive::CompositionInvariant
        }
    }

    fn admit_advanced(&mut self, drive: OwnerSessionDrive) -> QuinnOwnerOutputContinuationDrive {
        let needs_continue = drive.needs_continue();
        let outputs = drive.into_outputs();
        let output_count = outputs.len();
        match self
            .root
            .output
            .ledger
            .drain
            .admit_outputs(outputs, needs_continue)
        {
            Ok(()) => {
                self.root.output.ledger.status = QuinnOwnerOutputStatus::Live {
                    phase: QuinnOwnerOutputPhase::Page,
                };
                QuinnOwnerOutputContinuationDrive::Admitted {
                    outputs: output_count,
                    needs_continue,
                }
            }
            Err(failure) => {
                self.root.output.ledger.status = QuinnOwnerOutputStatus::RejectedPage {
                    first_stop_reason: None,
                };
                self.root.output.admission_failure = Some(failure);
                QuinnOwnerOutputContinuationDrive::AdmissionRetained
            }
        }
    }

    fn restore_prepared(&mut self) {
        let page = match self.root.output.ledger.status {
            QuinnOwnerOutputStatus::ContinuationInFlight {
                page,
                entered: false,
            } => page,
            _ => {
                self.quarantine(QuinnOwnerOutputQuarantineCause::UnsettledContinuation);
                return;
            }
        };
        self.root.output.ledger.status = QuinnOwnerOutputStatus::Live {
            phase: QuinnOwnerOutputPhase::Drained(page),
        };
    }

    fn enter(&mut self) -> bool {
        match &mut self.root.output.ledger.status {
            QuinnOwnerOutputStatus::ContinuationInFlight { entered, .. } if !*entered => {
                *entered = true;
                true
            }
            _ => false,
        }
    }

    fn settle_controller_failure(&mut self, error: OwnerSessionControllerError) {
        debug_assert!(matches!(
            self.root.controller_failure,
            QuinnOwnerControllerFailureLedger::Ready
        ));
        self.root.controller_failure = QuinnOwnerControllerFailureLedger::Failed(error);
    }

    fn quarantine(&mut self, cause: QuinnOwnerOutputQuarantineCause) {
        if let QuinnOwnerOutputStatus::ContinuationInFlight { page, .. } =
            self.root.output.ledger.status
        {
            self.root.output.ledger.status = QuinnOwnerOutputStatus::ContinuationQuarantined {
                page,
                first_stop_reason: None,
            };
            return;
        }
        if matches!(
            self.root.output.ledger.status,
            QuinnOwnerOutputStatus::ContinuationQuarantined { .. }
        ) {
            return;
        }
        let phase = match self.root.output.ledger.status {
            QuinnOwnerOutputStatus::Live { phase }
            | QuinnOwnerOutputStatus::Quarantined { phase, .. } => phase,
            QuinnOwnerOutputStatus::OrdinaryInFlight { .. }
            | QuinnOwnerOutputStatus::OrdinaryQuarantined { .. }
            | QuinnOwnerOutputStatus::RejectedPage { .. }
            | QuinnOwnerOutputStatus::ContinuationInFlight { .. }
            | QuinnOwnerOutputStatus::ContinuationQuarantined { .. }
            | QuinnOwnerOutputStatus::Stopped { .. } => QuinnOwnerOutputPhase::Vacant,
        };
        self.root.output.ledger.status = QuinnOwnerOutputStatus::Quarantined {
            phase,
            first_stop_reason: None,
            _cause: cause,
        };
    }
}

impl<T: TargetIo> Drop for QuinnOwnerOutputContinuationTransaction<'_, T> {
    fn drop(&mut self) {
        match self.root.output.ledger.status {
            QuinnOwnerOutputStatus::ContinuationInFlight { entered: false, .. } => {
                self.restore_prepared();
            }
            QuinnOwnerOutputStatus::ContinuationInFlight {
                page,
                entered: true,
            } => {
                self.root.output.ledger.status = QuinnOwnerOutputStatus::ContinuationQuarantined {
                    page,
                    first_stop_reason: None,
                };
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests;
