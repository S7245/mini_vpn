//! Target failure and pending-completion settlement in the existing Executor.
//!
//! Owns failure RESET/completion settlement, terminal join and exact retirement.
//! Flow storage and source reads remain at the parent Authority;
//! the read transaction lives in sibling read_source. Bound identity stays there.
//! Retain rejected completions without repeating Target I/O; aborts preserve the
//! original work, generated effects and outputs. Entry methods are parent-scoped.
//! Tests remain in parent tests, especially r7_nested_abort and r7_owner_ack.

use super::{
    DriverInput, FlowFinishReason, OwnerTargetError, OwnerTargetExecutor, OwnerTargetOutput,
    OwnerTargetReducerObservations, OwnerTargetWork, PendingTargetCompletion,
    PendingTargetCompletionKind, ReducerAdmissionBlock, ResetReason, SessionEffect, SessionEvent,
    SessionFlowId, SessionOwnerCommand, SessionSupervisorError, TargetCancelCompletion, TargetIo,
    TargetJoinAttempt, TargetJoinBlock, TargetJoinCompletion, TargetResetCompletion,
    TargetTerminalKind, TargetTombstoneRetireCompletion,
};
use std::collections::VecDeque;

mod retirement;

/// Original Flow's observed terminal, not a second tombstone or owner table.
/// Target join may finish before source lanes, receipts and publishers retire.
pub(super) struct TerminalFlowSettlement {
    terminal: super::TerminalGrace,
    reason: FlowFinishReason,
    feedback: TerminalFeedback,
    source: TerminalSourceSettlement,
    join: TerminalJoinPhase,
}

// Readiness errors belong to this terminal, separately from Failed-work cause.
// No clone, extraction or second RESET: the first returned outcome is sticky.
enum TerminalJoinPhase {
    AwaitingReadiness,
    Ready,
    OperationalFailure(super::super::target::TargetFailure),
    InvariantFailure(super::TargetIoError),
}

impl TerminalJoinPhase {
    fn is_failed(&self) -> bool {
        matches!(
            self,
            Self::OperationalFailure(_) | Self::InvariantFailure(_)
        )
    }
}

// This records selected EOFs, never substitutes for the retained actual Source.
// Revoking stays sticky if the admission operation or its progress wake fails.
enum TerminalSourceSettlement {
    // Cold finish retains proof without closing the original Unbound Source.
    Cold,
    Absent,
    Revoking,
    Releasing,
    Released,
    Draining {
        control_drained: bool,
        ordered_drained: bool,
    },
}

enum TerminalFeedback {
    Failed {
        work: OwnerTargetWork,
        cause: super::super::target::TargetFailure,
        feedback: TargetFailureFeedback,
    },
    // Original cause remains at the same Flow across destruction, including
    // an unknown unwind. Only successful destruction permits Released.
    FailedReleasing {
        cause: super::super::target::TargetFailure,
    },
    FailedReleased {
        cause: super::super::target::TargetFailure,
    },
    None,
    InTransit(crate::resumable::LocalFlow),
    Returned(SessionEvent),
    Open(OpenFlowSettlement),
    HalfClose(SessionEvent),
    Driver(DriverCompletionState),
}

impl TerminalFlowSettlement {
    fn feedback_released(&self) -> bool {
        match &self.feedback {
            TerminalFeedback::None | TerminalFeedback::FailedReleased { .. } => true,
            TerminalFeedback::Open(open) => open.phase == OpenFeedbackPhase::FinishPending,
            TerminalFeedback::Driver(
                DriverCompletionState::Reduced(local)
                | DriverCompletionState::TerminalDisposed(local),
            ) => *local == self.terminal.local_flow(),
            _ => false,
        }
    }
}

/// One Flow's failure feedback; original work/cause never travel with a marker.
/// Unknown staging or reduction is sticky and cannot manufacture another RESET.
pub(super) enum TargetFailureFeedback {
    Prepared(crate::resumable::LocalFlow),
    StagingUnknown(crate::resumable::LocalFlow),
    InTransit(crate::resumable::LocalFlow),
    Driver(DriverCompletionState),
    Settled,
}

impl TargetFailureFeedback {
    fn accepts_reset(&self, local: crate::resumable::LocalFlow) -> bool {
        matches!(self, Self::InTransit(expected) if *expected == local)
    }

    fn is_settled(&self) -> bool {
        matches!(
            self,
            Self::Driver(
                DriverCompletionState::Reduced(_) | DriverCompletionState::TerminalDisposed(_)
            ) | Self::Settled
        )
    }

    fn matches_live_terminal(&self, local: crate::resumable::LocalFlow) -> bool {
        self.accepts_reset(local)
            || matches!(self, Self::Driver(DriverCompletionState::Returned(command))
                if returned_reset_flow(command) == Some(local))
            || matches!(self, Self::Driver(DriverCompletionState::Reduced(expected)) if *expected == local)
    }
}

/// Only actual Bound carrying rejection creates this owning origin.
/// Local/cold completion remains distinct, even when it contains Driver syntax.
pub(super) enum DriverCompletionState {
    Returned(Box<SessionOwnerCommand>),
    Reducing(crate::resumable::LocalFlow),
    Reduced(crate::resumable::LocalFlow),
    // Disposition after the real terminal is not reducer acceptance. Unknown
    // consumption must stay non-retryable with its original transit charge.
    TerminalDisposing(crate::resumable::LocalFlow),
    TerminalDisposed(crate::resumable::LocalFlow),
}

impl DriverCompletionState {
    pub(super) fn is_returned_reset_for(&self, expected: crate::resumable::LocalFlow) -> bool {
        matches!(self, Self::Returned(command)
            if returned_reset_flow(command) == Some(expected))
    }
}

fn returned_reset_flow(command: &SessionOwnerCommand) -> Option<crate::resumable::LocalFlow> {
    match command {
        SessionOwnerCommand::Driver(DriverInput::Control(SessionEvent::LocalReset {
            flow,
            reason: ResetReason::TargetFailure,
        })) => Some(*flow),
        _ => None,
    }
}

/// Original Target.open disposition survives consumption of its feedback.
/// Reducing/FinishPending retain proof, not a second unreduced command.
pub(super) struct OpenFlowSettlement {
    flow: crate::resumable::LocalFlow,
    request: super::PeerOpenRequest,
    result: crate::resumable::OpenResultCode,
    target_terminal: Option<TargetTerminalKind>,
    phase: OpenFeedbackPhase,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum OpenFeedbackPhase {
    Prepared,
    Reducing,
    Rejected,
    FinishPending,
}

impl OpenFlowSettlement {
    fn matches_finished(
        &self,
        owner: &super::TargetFlowOwner,
        terminal: super::TerminalGrace,
        reason: FlowFinishReason,
    ) -> bool {
        self.phase == OpenFeedbackPhase::FinishPending
            && self.flow == terminal.local_flow()
            && self.request.flow_id() == self.flow.flow_id()
            && self.request.session_id() == self.flow.session_id()
            && reason == FlowFinishReason::OpenFailed(self.result)
            && self.result != crate::resumable::OpenResultCode::Opened
            && owner.source.is_none()
            && owner.target_present == self.target_terminal.is_some()
            && owner.terminal == self.target_terminal
    }
}

impl super::TargetFlowOwner {
    /// A cold proof replaces old None, not an outstanding command. Once bound,
    /// even that proof blocks ordinary admission until selected settlement.
    pub(super) fn completion_blocks_input(
        &self,
        service: &super::service::OwnerTargetServiceState,
    ) -> bool {
        match (&self.pending_completion, service) {
            (None, _) => false,
            (
                Some(PendingTargetCompletion::TerminalSettling(settlement)),
                super::service::OwnerTargetServiceState::Unbound,
            ) if matches!(settlement.source, TerminalSourceSettlement::Cold)
                && matches!(
                    settlement.feedback,
                    TerminalFeedback::None | TerminalFeedback::Open(_)
                ) =>
            {
                false
            }
            _ => true,
        }
    }

    /// The selected Driver caller already observed real EOF on this exact pair.
    /// Commit only that lane; completion is not source or publisher retirement.
    pub(super) fn settle_terminal_source_drain(
        &mut self,
        lane: crate::owned_upstream::owner_ready::SendLane,
    ) -> Result<bool, super::OwnerTargetInputBlock> {
        use crate::owned_upstream::owner_ready::SendLane;
        let invalid = super::OwnerTargetInputBlock::ReadyReceiptInvariant;
        let source = self.source.as_ref().ok_or(invalid)?;
        let Some(PendingTargetCompletion::TerminalSettling(settlement)) =
            &mut self.pending_completion
        else {
            return Err(invalid);
        };
        if source.flow.local_flow() != settlement.terminal.local_flow()
            || source.driver.local_flow() != settlement.terminal.local_flow()
        {
            return Err(invalid);
        }
        let TerminalSourceSettlement::Draining {
            control_drained,
            ordered_drained,
        } = &mut settlement.source
        else {
            return Err(invalid);
        };
        let drained = match lane {
            SendLane::Control => control_drained,
            SendLane::OrderedSource => ordered_drained,
        };
        let first = !*drained;
        *drained = true;
        Ok(first)
    }

    /// Cold association is immutable installation lineage, not a second phase.
    /// Accept its settled successor only with the same original Open proof.
    pub(super) fn terminal_open_preserves_no_source(
        &self,
        flow_id: SessionFlowId,
        phase: crate::owned_upstream::owner_ready::OwnerReadyNoSourcePhase,
    ) -> bool {
        use crate::owned_upstream::owner_ready::OwnerReadyNoSourcePhase as Phase;
        let Some(PendingTargetCompletion::TerminalSettling(settlement)) = &self.pending_completion
        else {
            return false;
        };
        let TerminalFeedback::Open(open) = &settlement.feedback else {
            return false;
        };
        matches!(phase, Phase::OpenCompletion | Phase::PendingFinishEffect)
            && self.source.is_none()
            && open.phase == OpenFeedbackPhase::FinishPending
            && open.flow == settlement.terminal.local_flow()
            && open.request.flow_id() == flow_id
            && open.flow.flow_id() == flow_id
            && open.result != crate::resumable::OpenResultCode::Opened
            && settlement.reason == FlowFinishReason::OpenFailed(open.result)
            && self.terminal == open.target_terminal
            && (!self.target_present || open.target_terminal.is_some())
    }
}

#[derive(Clone, Copy)]
pub(super) enum CompletionReceipt {
    Independent,
    Feedback,
    TerminalDisposition,
}

impl PendingTargetCompletion {
    fn is_selected_retry(&self) -> bool {
        self.is_open_pending()
            || matches!(
                self,
                Self::Failure { .. }
                    | Self::HalfClose { .. }
                    | Self::Failed {
                        feedback: TargetFailureFeedback::Driver(DriverCompletionState::Returned(_)),
                        ..
                    }
                    | Self::DriverReturned(DriverCompletionState::Returned(_))
            )
    }
    pub(super) fn open(
        flow: crate::resumable::LocalFlow,
        request: super::PeerOpenRequest,
        result: crate::resumable::OpenResultCode,
        target_terminal: Option<TargetTerminalKind>,
    ) -> Self {
        Self::Open(OpenFlowSettlement {
            flow,
            request,
            result,
            target_terminal,
            phase: OpenFeedbackPhase::Prepared,
        })
    }

    pub(super) fn is_open_pending(&self) -> bool {
        matches!(self, Self::Open(open) if matches!(open.phase,
            OpenFeedbackPhase::Prepared | OpenFeedbackPhase::Rejected))
    }

    pub(super) fn is_open_finish_pending(&self) -> bool {
        matches!(self, Self::Open(open) if open.phase == OpenFeedbackPhase::FinishPending)
    }

    pub(super) fn needs_feedback(&self) -> bool {
        match self {
            Self::Failed { feedback, .. } => !feedback.is_settled(),
            Self::DriverReturned(state) => !matches!(state, DriverCompletionState::Reduced(_)),
            Self::Open(open) => open.phase != OpenFeedbackPhase::FinishPending,
            Self::TerminalSettling(settlement) => {
                !matches!(
                    settlement.feedback,
                    TerminalFeedback::None
                        | TerminalFeedback::FailedReleased { .. }
                        | TerminalFeedback::Open(_)
                        | TerminalFeedback::Driver(DriverCompletionState::Reduced(_))
                        | TerminalFeedback::Driver(DriverCompletionState::TerminalDisposed(_))
                ) && !matches!(&settlement.feedback,
                TerminalFeedback::Failed { feedback, .. } if feedback.is_settled())
            }
            _ => true,
        }
    }

    pub(super) fn classify_receipt(
        &self,
        command: &SessionOwnerCommand,
    ) -> Option<CompletionReceipt> {
        use CompletionReceipt::{Feedback, Independent, TerminalDisposition};
        if matches!(self, Self::TerminalSettling(settlement)
            if matches!(&settlement.feedback,
                TerminalFeedback::Driver(DriverCompletionState::TerminalDisposing(_))
                | TerminalFeedback::FailedReleasing { .. }
                | TerminalFeedback::Failed { feedback: TargetFailureFeedback::Driver(
                    DriverCompletionState::TerminalDisposing(_)), .. }))
        {
            return None;
        }
        if matches!(
            self,
            Self::Failed {
                feedback: TargetFailureFeedback::Prepared(_)
                    | TargetFailureFeedback::StagingUnknown(_)
                    | TargetFailureFeedback::Driver(DriverCompletionState::Reducing(_)),
                ..
            }
        ) {
            return None;
        }
        match (self, command) {
            (Self::TerminalSettling(settlement), SessionOwnerCommand::Driver(input)) => {
                let local = match input {
                    DriverInput::Data(data) => data.flow(),
                    DriverInput::Control(SessionEvent::LocalClose { flow }) => *flow,
                    DriverInput::Control(SessionEvent::LocalReset {
                        flow,
                        reason: ResetReason::TargetFailure,
                    }) if matches!(&settlement.feedback,
                        TerminalFeedback::InTransit(expected) if *expected == *flow)
                        || matches!(&settlement.feedback, TerminalFeedback::Failed { feedback, .. }
                            if feedback.accepts_reset(*flow)) =>
                    {
                        *flow
                    }
                    _ => return None,
                };
                (settlement.terminal.local_flow() == local).then_some(TerminalDisposition)
            }
            (
                _,
                SessionOwnerCommand::Driver(
                    DriverInput::Data(_) | DriverInput::Control(SessionEvent::LocalClose { .. }),
                ),
            ) => Some(Independent),
            (
                Self::Failed { feedback, .. },
                SessionOwnerCommand::Driver(DriverInput::Control(SessionEvent::LocalReset {
                    flow,
                    reason: ResetReason::TargetFailure,
                })),
            ) if feedback.accepts_reset(*flow) => Some(Feedback),
            (
                Self::StagedReset(expected),
                SessionOwnerCommand::Driver(DriverInput::Control(SessionEvent::LocalReset {
                    flow,
                    reason: ResetReason::TargetFailure,
                })),
            ) if expected == flow => Some(Feedback),
            (
                Self::Failure { event: expected },
                SessionOwnerCommand::Driver(DriverInput::Control(actual)),
            ) if expected == actual => Some(Feedback),
            (
                Self::Open(open),
                SessionOwnerCommand::Event(SessionEvent::PeerOpenResolved { request, result }),
            ) if open.request == *request && open.result == *result && self.is_open_pending() => {
                Some(Feedback)
            }
            (Self::HalfClose { event: expected }, SessionOwnerCommand::Event(actual))
                if expected == actual =>
            {
                Some(Feedback)
            }
            _ => None,
        }
    }

    const fn kind(&self) -> PendingTargetCompletionKind {
        match self {
            Self::Open(_) => PendingTargetCompletionKind::Open,
            Self::Failed { .. } | Self::Failure { .. } | Self::DriverReturned(_) => {
                PendingTargetCompletionKind::Failure
            }
            Self::StagedReset(_) | Self::TerminalSettling(_) => {
                PendingTargetCompletionKind::Failure
            }
            Self::HalfClose { .. } => PendingTargetCompletionKind::HalfClose,
        }
    }
    fn into_command(self) -> Result<SessionOwnerCommand, Self> {
        Ok(match self {
            staged @ (Self::Failed { .. } | Self::StagedReset(_) | Self::TerminalSettling(_)) => {
                return Err(staged);
            }
            open @ (Self::Open(_) | Self::DriverReturned(_)) => return Err(open),
            Self::Failure { event } => SessionOwnerCommand::Driver(DriverInput::Control(event)),
            Self::HalfClose { event } => SessionOwnerCommand::Event(event),
        })
    }
    fn from_carried_command(
        expected: PendingTargetCompletionKind,
        flow_id: SessionFlowId,
        command: SessionOwnerCommand,
    ) -> Result<Self, OwnerTargetError> {
        match (expected, command) {
            (
                PendingTargetCompletionKind::Failure,
                SessionOwnerCommand::Driver(DriverInput::Control(
                    event @ (SessionEvent::LocalReset { flow, .. }
                    | SessionEvent::LocalClose { flow }),
                )),
            ) if flow.flow_id() == flow_id => Ok(Self::Failure { event }),
            (
                PendingTargetCompletionKind::HalfClose,
                SessionOwnerCommand::Event(event @ SessionEvent::SinkHalfClosed { completion }),
            ) if completion.flow_id() == flow_id => Ok(Self::HalfClose { event }),
            _ => Err(OwnerTargetError::CarriedTargetCompletionMismatch { flow_id }),
        }
    }
}

impl<T: TargetIo> OwnerTargetExecutor<T> {
    /// One claimed completion owns its original retry, effects and observations
    /// until the existing C/output transaction accepts them. Neutral ready
    /// metadata receives only the value result, never business resources.
    pub(in crate::owned_upstream) fn apply_ready_completion_observed(
        &mut self,
        selection: &crate::owned_upstream::owner_ready::OwnerReadyContinuationSelection,
    ) -> Result<
        (
            super::OwnerTargetObservedApply,
            crate::owned_upstream::owner_ready::OwnerReadyReadResult,
        ),
        OwnerTargetError,
    > {
        use super::service::{
            OwnerFlowServiceSlot, OwnerFlowWaitResource, OwnerTargetServiceState,
        };
        use crate::owned_upstream::owner_ready::{OwnerReadyCategory, OwnerReadyReadResult};
        use OwnerTargetError::BoundSourceRequiresSelectedService as Invalid;
        self.require_no_pending_effects()?;
        if selection.category != OwnerReadyCategory::Completion {
            return Err(Invalid);
        }
        let selected = selection.candidate.as_ref().ok_or(Invalid)?;
        let flow_id = selected.association.flow_id();
        let OwnerTargetServiceState::Bound { flows, binding, .. } = &self.service else {
            return Err(Invalid);
        };
        let Some(OwnerFlowServiceSlot::Live(record) | OwnerFlowServiceSlot::Retiring(record)) =
            flows.slots.get(selected.position)
        else {
            return Err(Invalid);
        };
        let owner = self.flows.get(&flow_id).ok_or(Invalid)?;
        if owner.service_position != Some(selected.position)
            || record.association != selected.association
            || record.generation != selected.generation
            || owner.retire_after_join
                != matches!(
                    flows.slots[selected.position],
                    OwnerFlowServiceSlot::Retiring(_)
                )
        {
            return Err(Invalid);
        }
        binding
            .validate_receipt_flow(
                flows.slots.len(),
                selected.position,
                (
                    record.association,
                    record.generation,
                    owner.retire_after_join,
                ),
            )
            .map_err(|_| Invalid)?;
        let retry = owner
            .pending_completion
            .as_ref()
            .is_some_and(PendingTargetCompletion::is_selected_retry);
        let retry = retry && record.wait.is_none();
        self.begin_observed_page(
            VecDeque::new(),
            Vec::new(),
            OwnerTargetReducerObservations::default(),
            None,
        )?;
        if let Some(page) = self.aborted_effects.as_mut() {
            page.current = Some(OwnerTargetWork::TargetIoInvariant {
                flow_id,
                operation: super::TargetIoOperation::FailureCompletion,
            });
        }
        if retry {
            self.retry_pending_completion_at_flow(flow_id)?;
        }
        let waiting = self.flows.get(&flow_id).is_some_and(|owner| {
            owner
                .pending_completion
                .as_ref()
                .is_some_and(PendingTargetCompletion::is_selected_retry)
        });
        let capacity_available = self.supervisor.snapshot().session.terminal_tombstones()
            < self.supervisor.session_config().max_terminal_tombstones();
        let scheduled =
            if let OwnerTargetServiceState::Bound { flows, binding, .. } = &mut self.service {
                if waiting {
                    flows.wait_for(selected.position, OwnerFlowWaitResource::CompletionCapacity)
                } else if capacity_available {
                    flows.handoff_waiter(OwnerFlowWaitResource::CompletionCapacity, binding)
                } else {
                    Ok(())
                }
            } else {
                Err(Invalid)
            };
        scheduled?;
        self.retire_entered_work()?;
        self.run_entered_effects()?;
        self.finish_observed_apply()
            .map(|observed| (observed, OwnerReadyReadResult::Pending))
    }

    /// Known Bound failure admits independent DATA/CLOSE and a peer terminal;
    /// original source/feedback stay owned. Global interruption exclusions win.
    pub(super) fn event_admission_block(
        &self,
        event: &SessionEvent,
    ) -> Option<super::OwnerTargetInputBlock> {
        let block = self.input_admission_block(super::event_flow_id(event));
        if let (
            Some(super::OwnerTargetInputBlock::PendingTargetCompletion { flow_id }),
            SessionEvent::TerminalGraceExpired { terminal },
        ) = (&block, event)
            && self.flows.get(flow_id).is_some_and(|owner| {
                matches!(&owner.pending_completion,
                    Some(PendingTargetCompletion::TerminalSettling(settlement))
                    if settlement.terminal == *terminal)
            })
        {
            return None;
        }
        if let (
            Some(super::OwnerTargetInputBlock::PendingTargetCompletion { flow_id }),
            SessionEvent::PeerFrame { frame, .. },
        ) = (&block, event)
            && matches!(
                frame.record(),
                super::Record::Reset { .. }
                    | super::Record::Data {
                        direction: super::Direction::ClientToTarget,
                        ..
                    }
                    | super::Record::Close {
                        direction: super::Direction::ClientToTarget,
                        ..
                    }
            )
            && matches!(
                self.service,
                super::service::OwnerTargetServiceState::Bound { .. }
            )
            && self.flows.get(flow_id).is_some_and(|owner| {
                owner.target_present
                    && owner.terminal.is_none()
                    && !owner.join_pending
                    && matches!((&owner.pending_completion, &owner.source),
                        (Some(PendingTargetCompletion::StagedReset(local)), Some(source))
                        if matches!(frame.record(), super::Record::Reset { .. })
                            && owner.pending_sink.is_none() && source.flow.local_flow() == *local
                            && source.driver.local_flow() == *local)
                    || (owner.target_present
                        && owner.terminal.is_none()
                        && !owner.join_pending
                        && matches!((&owner.pending_completion, &owner.source),
                            (Some(PendingTargetCompletion::Failed { feedback, .. }), Some(source))
                            if feedback.matches_live_terminal(source.flow.local_flow())
                                && source.driver.local_flow() == source.flow.local_flow()))
            })
        {
            // Admission is not terminal proof. The original Supervisor still
            // validates the peer leg/generation and reserves actual Z capacity.
            return None;
        }
        block
    }

    fn prepare_completion_retry(
        &mut self,
        flow_id: SessionFlowId,
        completion: PendingTargetCompletion,
    ) -> Result<crate::owned_upstream::session::SessionOwnerCommand, OwnerTargetError> {
        if let PendingTargetCompletion::Failed {
            work,
            cause,
            feedback,
        } = completion
        {
            let (feedback, command) = match feedback {
                TargetFailureFeedback::Driver(DriverCompletionState::Returned(command)) => {
                    match returned_reset_flow(&command) {
                        Some(local) => (
                            TargetFailureFeedback::Driver(DriverCompletionState::Reducing(local)),
                            Some(command),
                        ),
                        None => (
                            TargetFailureFeedback::Driver(DriverCompletionState::Returned(command)),
                            None,
                        ),
                    }
                }
                other => (other, None),
            };
            self.flows
                .get_mut(&flow_id)
                .ok_or(OwnerTargetError::UnknownTargetFlow { flow_id })?
                .pending_completion = Some(PendingTargetCompletion::Failed {
                work,
                cause,
                feedback,
            });
            return command
                .map(|command| *command)
                .ok_or(OwnerTargetError::BoundSourceRequiresSelectedService);
        }
        if let PendingTargetCompletion::DriverReturned(state) = completion {
            let DriverCompletionState::Returned(command) = state else {
                self.flows
                    .get_mut(&flow_id)
                    .ok_or(OwnerTargetError::UnknownTargetFlow { flow_id })?
                    .pending_completion = Some(PendingTargetCompletion::DriverReturned(state));
                return Err(OwnerTargetError::BoundSourceRequiresSelectedService);
            };
            let Some(local) = returned_reset_flow(&command) else {
                self.flows
                    .get_mut(&flow_id)
                    .ok_or(OwnerTargetError::UnknownTargetFlow { flow_id })?
                    .pending_completion = Some(PendingTargetCompletion::DriverReturned(
                    DriverCompletionState::Returned(command),
                ));
                return Err(OwnerTargetError::CarriedTargetCompletionMismatch { flow_id });
            };
            self.flows
                .get_mut(&flow_id)
                .ok_or(OwnerTargetError::UnknownTargetFlow { flow_id })?
                .pending_completion = Some(PendingTargetCompletion::DriverReturned(
                DriverCompletionState::Reducing(local),
            ));
            return Ok(*command);
        }
        if let PendingTargetCompletion::Open(mut open) = completion {
            let retryable = matches!(
                open.phase,
                OpenFeedbackPhase::Prepared | OpenFeedbackPhase::Rejected
            );
            let command = SessionOwnerCommand::Event(SessionEvent::PeerOpenResolved {
                request: open.request,
                result: open.result,
            });
            if retryable {
                open.phase = OpenFeedbackPhase::Reducing;
            }
            self.flows
                .get_mut(&flow_id)
                .ok_or(OwnerTargetError::UnknownTargetFlow { flow_id })?
                .pending_completion = Some(PendingTargetCompletion::Open(open));
            return if retryable {
                Ok(command)
            } else {
                Err(OwnerTargetError::BoundSourceRequiresSelectedService)
            };
        }
        match completion.into_command() {
            Ok(command) => Ok(command),
            Err(staged) => {
                // No queued RESET may be manufactured as a reducer retry.
                // Restore the same intent at its sole Flow before rejecting.
                self.flows
                    .get_mut(&flow_id)
                    .ok_or(OwnerTargetError::UnknownTargetFlow { flow_id })?
                    .pending_completion = Some(staged);
                Err(OwnerTargetError::BoundSourceRequiresSelectedService)
            }
        }
    }

    /// A carried Open rejection returns to its original disposition. It cannot
    /// reconstruct full LocalFlow/Target ownership from the numeric request.
    pub(super) fn retain_rejected_completion(
        &mut self,
        expected: PendingTargetCompletionKind,
        flow_id: SessionFlowId,
        command: Box<SessionOwnerCommand>,
    ) -> Result<(), OwnerTargetError> {
        if self.flows.get(&flow_id).is_some_and(|owner| {
            matches!(
                owner.pending_completion,
                Some(
                    PendingTargetCompletion::DriverReturned(DriverCompletionState::Reducing(_))
                        | PendingTargetCompletion::Failed {
                            feedback: TargetFailureFeedback::Driver(
                                DriverCompletionState::Reducing(_)
                            ),
                            ..
                        }
                )
            )
        }) {
            return self.retain_driver_rejection(flow_id, command);
        }
        if matches!(expected, PendingTargetCompletionKind::Open) {
            if let Some(PendingTargetCompletion::Open(open)) = self
                .flows
                .get_mut(&flow_id)
                .and_then(|owner| owner.pending_completion.as_mut())
                && open.phase == OpenFeedbackPhase::Reducing
                && matches!(command.as_ref(), SessionOwnerCommand::Event(
                    SessionEvent::PeerOpenResolved { request, result })
                    if *request == open.request && *result == open.result
                        && open.flow.flow_id() == flow_id)
            {
                open.phase = OpenFeedbackPhase::Rejected;
                return Ok(());
            }
            return Err(OwnerTargetError::RejectedCommand {
                block: super::OwnerTargetInputBlock::ReadyReceiptInvariant,
                command,
            });
        }
        let completion =
            PendingTargetCompletion::from_carried_command(expected, flow_id, *command)?;
        self.flows
            .get_mut(&flow_id)
            .ok_or(OwnerTargetError::UnknownTargetFlow { flow_id })?
            .pending_completion = Some(completion);
        Ok(())
    }

    /// Original command custody, origin and existing transit remain together.
    /// No charge is minted, copied, decremented or inferred from event syntax.
    pub(super) fn retain_driver_rejection(
        &mut self,
        flow_id: SessionFlowId,
        command: Box<SessionOwnerCommand>,
    ) -> Result<(), OwnerTargetError> {
        let local = returned_reset_flow(&command);
        let valid = local.is_some_and(|local| {
            self.driver_charge_position(flow_id, local).is_ok()
                && self.flows.get(&flow_id).is_some_and(|owner| matches!(
                    owner.pending_completion,
                    Some(PendingTargetCompletion::StagedReset(expected)
                        | PendingTargetCompletion::DriverReturned(DriverCompletionState::Reducing(expected))
                        | PendingTargetCompletion::Failed { feedback: TargetFailureFeedback::InTransit(expected)
                            | TargetFailureFeedback::Driver(DriverCompletionState::Reducing(expected)), .. })
                    if expected == local))
        });
        if !valid {
            return Err(OwnerTargetError::RejectedCommand {
                block: super::OwnerTargetInputBlock::ReadyReceiptInvariant,
                command,
            });
        }
        if let Some(PendingTargetCompletion::Failed { feedback, .. }) = self
            .flows
            .get_mut(&flow_id)
            .and_then(|owner| owner.pending_completion.as_mut())
        {
            *feedback = TargetFailureFeedback::Driver(DriverCompletionState::Returned(command));
            return Ok(());
        }
        self.flows
            .get_mut(&flow_id)
            .ok_or(OwnerTargetError::UnknownTargetFlow { flow_id })?
            .pending_completion = Some(PendingTargetCompletion::DriverReturned(
            DriverCompletionState::Returned(command),
        ));
        Ok(())
    }

    fn driver_charge_position(
        &self,
        flow_id: SessionFlowId,
        local: crate::resumable::LocalFlow,
    ) -> Result<usize, OwnerTargetError> {
        use super::service::{OwnerFlowServiceSlot, OwnerTargetServiceState};
        let invalid = || OwnerTargetError::CarriedTargetCompletionMismatch { flow_id };
        let owner = self.flows.get(&flow_id).ok_or_else(invalid)?;
        let source = owner.source.as_ref().ok_or_else(invalid)?;
        let position = owner.service_position.ok_or_else(invalid)?;
        let OwnerTargetServiceState::Bound { flows, binding, .. } = &self.service else {
            return Err(invalid());
        };
        let Some(OwnerFlowServiceSlot::Live(record) | OwnerFlowServiceSlot::Retiring(record)) =
            flows.slots.get(position)
        else {
            return Err(invalid());
        };
        if local.flow_id() != flow_id
            || source.flow.local_flow() != local
            || source.driver.local_flow() != local
            || record.transit == 0
            || record.association
                != crate::owned_upstream::owner_ready::OwnerReadyFlowAssociation::Source(local)
            || owner.retire_after_join
                != matches!(flows.slots[position], OwnerFlowServiceSlot::Retiring(_))
        {
            return Err(invalid());
        }
        binding
            .validate_receipt_flow(
                flows.slots.len(),
                position,
                (
                    record.association,
                    record.generation,
                    owner.retire_after_join,
                ),
            )
            .map_err(|_| invalid())?;
        Ok(position)
    }

    fn pending_driver_charge_position(
        &self,
        flow_id: SessionFlowId,
    ) -> Result<Option<usize>, OwnerTargetError> {
        let owner = self
            .flows
            .get(&flow_id)
            .ok_or(OwnerTargetError::UnknownTargetFlow { flow_id })?;
        match &owner.pending_completion {
            Some(
                PendingTargetCompletion::DriverReturned(state @ DriverCompletionState::Returned(_))
                | PendingTargetCompletion::Failed {
                    feedback:
                        TargetFailureFeedback::Driver(state @ DriverCompletionState::Returned(_)),
                    ..
                },
            ) => {
                let local = owner
                    .source
                    .as_ref()
                    .ok_or(OwnerTargetError::MissingTargetReadSource { flow_id })?
                    .flow
                    .local_flow();
                if !state.is_returned_reset_for(local) {
                    return Err(OwnerTargetError::CarriedTargetCompletionMismatch { flow_id });
                }
                self.driver_charge_position(flow_id, local).map(Some)
            }
            Some(
                PendingTargetCompletion::DriverReturned(_) | PendingTargetCompletion::Failed { .. },
            ) => Err(OwnerTargetError::BoundSourceRequiresSelectedService),
            _ => Ok(None),
        }
    }

    fn finish_driver_reduction(&mut self, flow_id: SessionFlowId, position: Option<usize>) {
        use super::service::{OwnerFlowServiceSlot, OwnerTargetServiceState};
        // Preflight and reduction exclusively borrow this same Executor.
        // Supervisor cannot mutate its Flow map/backing; these plain stores
        // settle one charge before any later effect or Target callback.
        if let Some(position) = position
            && let Some(owner) = self.flows.get_mut(&flow_id)
            && let Some(
                PendingTargetCompletion::DriverReturned(state @ DriverCompletionState::Reducing(_))
                | PendingTargetCompletion::Failed {
                    feedback:
                        TargetFailureFeedback::Driver(state @ DriverCompletionState::Reducing(_)),
                    ..
                },
            ) = &mut owner.pending_completion
            && let DriverCompletionState::Reducing(local) = state
            && let OwnerTargetServiceState::Bound { flows, .. } = &mut self.service
            && let OwnerFlowServiceSlot::Live(record) | OwnerFlowServiceSlot::Retiring(record) =
                &mut flows.slots[position]
        {
            *state = DriverCompletionState::Reduced(*local);
            record.transit -= 1;
        }
    }

    /// The matching receipt was already preflighted by the original Flow.
    /// Preserve its immutable disposition before consuming the actual command.
    pub(super) fn mark_feedback_reducing(&mut self, flow_id: SessionFlowId) {
        if let Some(PendingTargetCompletion::Open(open)) = self
            .flows
            .get_mut(&flow_id)
            .and_then(|owner| owner.pending_completion.as_mut())
        {
            open.phase = OpenFeedbackPhase::Reducing;
        }
        if let Some(PendingTargetCompletion::Failed { feedback, .. }) = self
            .flows
            .get_mut(&flow_id)
            .and_then(|owner| owner.pending_completion.as_mut())
            && let TargetFailureFeedback::InTransit(local) = feedback
        {
            *feedback = TargetFailureFeedback::Driver(DriverCompletionState::Reducing(*local));
        }
    }

    /// Successful reduction consumed the original feedback. A failed OPEN's
    /// finish may still be in the bounded effect queue; keep its disposition
    /// until that actual effect supplies the full terminal proof.
    pub(super) fn record_feedback_reduced(&mut self, flow_id: SessionFlowId) {
        if let Some(PendingTargetCompletion::Failed { feedback, .. }) = self
            .flows
            .get_mut(&flow_id)
            .and_then(|owner| owner.pending_completion.as_mut())
            && let TargetFailureFeedback::Driver(DriverCompletionState::Reducing(local)) = feedback
        {
            *feedback = TargetFailureFeedback::Driver(DriverCompletionState::Reduced(*local));
        }
        if let Some(owner) = self.flows.get_mut(&flow_id)
            && let Some(PendingTargetCompletion::Open(open)) = &mut owner.pending_completion
            && open.phase == OpenFeedbackPhase::Reducing
        {
            if open.result == crate::resumable::OpenResultCode::Opened {
                owner.pending_completion = None;
            } else {
                open.phase = OpenFeedbackPhase::FinishPending;
            }
        }
    }
    /// Existing Unbound read Adapter keeps its interface. It transfers the
    /// actual work/page here before RESET, and takes it back only after all
    /// nested completion callbacks have returned. Its caller immediately enters
    /// the same queue for execution; there is no intervening Target operation.
    pub(super) fn apply_target_failure(
        &mut self,
        flow_id: SessionFlowId,
        (cause, current): (super::super::target::TargetFailure, OwnerTargetWork),
        effects: &mut VecDeque<OwnerTargetWork>,
        outputs: &mut Vec<OwnerTargetOutput>,
        observations: &mut OwnerTargetReducerObservations,
    ) -> Result<(), OwnerTargetError> {
        self.begin_observed_page(
            std::mem::take(effects),
            std::mem::take(outputs),
            std::mem::take(observations),
            None,
        )?;
        if let Some(page) = self.aborted_effects.as_mut() {
            page.current = Some(current);
        }
        self.apply_entered_target_failure(flow_id, cause)?;
        self.retire_entered_work()?;
        let page = self
            .aborted_effects
            .as_ref()
            .ok_or(OwnerTargetError::AbortedEffectInvariant)?;
        if page.incoming.is_some() || page.source.is_some() || page.recovery.is_some() {
            return Err(OwnerTargetError::AbortedEffectInvariant);
        }
        if let Some(page) = self.aborted_effects.take() {
            *effects = page.effects;
            *outputs = page.outputs;
            *observations = page.observations.unwrap_or_default();
        }
        Ok(())
    }

    pub(super) fn apply_entered_target_failure(
        &mut self,
        flow_id: SessionFlowId,
        cause: super::super::target::TargetFailure,
    ) -> Result<(), OwnerTargetError> {
        if matches!(
            self.service,
            super::service::OwnerTargetServiceState::Bound { .. }
        ) {
            let current =
                self.take_entered_work(flow_id, super::TargetIoOperation::FailureCompletion)?;
            if let Err((current, error)) = self.retain_bound_target_failure(flow_id, cause, current)
            {
                if let Some(page) = self.aborted_effects.as_mut() {
                    page.current = Some(current);
                }
                return Err(error);
            }
            return self.stage_retained_failure_reset(flow_id);
        }
        let source = self
            .flows
            .get(&flow_id)
            .ok_or(OwnerTargetError::UnknownTargetFlow { flow_id })?
            .source
            .as_ref()
            .ok_or(OwnerTargetError::MissingTargetReadSource { flow_id })?;
        source.flow.try_send_reset(ResetReason::TargetFailure)?;
        let input = self
            .flows
            .get_mut(&flow_id)
            .ok_or(OwnerTargetError::UnknownTargetFlow { flow_id })?
            .source
            .as_mut()
            .ok_or(OwnerTargetError::MissingTargetReadSource { flow_id })?
            .driver
            .try_recv_next()?
            .ok_or(OwnerTargetError::MissingTargetFailureDriverInput { flow_id })?;
        let event = match input {
            DriverInput::Control(
                event @ SessionEvent::LocalReset {
                    flow,
                    reason: ResetReason::TargetFailure,
                },
            ) if flow.flow_id() == flow_id => event,
            _ => return Err(OwnerTargetError::UnexpectedTargetFailureDriverInput { flow_id }),
        };
        self.submit_target_completion(flow_id, PendingTargetCompletion::Failure { event })
    }

    pub(super) fn submit_target_completion(
        &mut self,
        flow_id: SessionFlowId,
        completion: PendingTargetCompletion,
    ) -> Result<(), OwnerTargetError> {
        if self
            .flows
            .get(&flow_id)
            .ok_or(OwnerTargetError::UnknownTargetFlow { flow_id })?
            .pending_completion
            .is_some()
        {
            return Err(OwnerTargetError::DuplicatePendingTargetCompletion { flow_id });
        }
        self.flows
            .get_mut(&flow_id)
            .ok_or(OwnerTargetError::UnknownTargetFlow { flow_id })?
            .pending_completion = Some(completion);
        self.retry_pending_completion_at_flow(flow_id)
    }

    pub(super) fn retry_one_pending_target_completion(&mut self) -> Result<(), OwnerTargetError> {
        if let super::service::OwnerTargetServiceState::Bound { flows, binding, .. } =
            &mut self.service
        {
            flows.handoff_waiter(
                super::service::OwnerFlowWaitResource::CompletionCapacity,
                binding,
            )?;
            return Ok(());
        }
        let Some(flow_id) = self.flows.iter().find_map(|(flow_id, flow)| {
            flow.pending_completion
                .as_ref()
                .is_some_and(|pending| {
                    pending.is_open_pending()
                        || matches!(
                            pending,
                            PendingTargetCompletion::Failure { .. }
                                | PendingTargetCompletion::Failed {
                                    feedback: TargetFailureFeedback::Driver(
                                        DriverCompletionState::Returned(_)
                                    ),
                                    ..
                                }
                                | PendingTargetCompletion::HalfClose { .. }
                                | PendingTargetCompletion::DriverReturned(
                                    DriverCompletionState::Returned(_)
                                )
                        )
                })
                .then_some(*flow_id)
        }) else {
            return Ok(());
        };
        self.retry_pending_completion_at_flow(flow_id)
    }

    /// One original Flow's complete retry transaction. Selection is separate;
    /// a cold/local origin never acquires a Driver charge by entering Bound.
    fn retry_pending_completion_at_flow(
        &mut self,
        flow_id: SessionFlowId,
    ) -> Result<(), OwnerTargetError> {
        let driver_position = self.pending_driver_charge_position(flow_id)?;
        self.prepare_effect_entry(3)?;
        let completion = self
            .flows
            .get_mut(&flow_id)
            .ok_or(OwnerTargetError::UnknownTargetFlow { flow_id })?
            .pending_completion
            .take()
            .ok_or(OwnerTargetError::MissingPendingTargetCompletion { flow_id })?;
        let expected = completion.kind();
        let command = self.prepare_completion_retry(flow_id, completion)?;
        match self.reduce_owner_command_observed(command) {
            Ok((effects, committed)) => {
                let bounds = self.stage_entered_reduction(effects, committed)?;
                self.finish_driver_reduction(flow_id, driver_position);
                self.record_feedback_reduced(flow_id);
                self.finish_entered_reduction(bounds)
            }
            Err(SessionSupervisorError::RejectedCommand {
                block: ReducerAdmissionBlock::TerminalTombstoneCapacityExceeded { .. },
                command,
            }) => {
                self.retain_rejected_completion(expected, flow_id, command)?;
                if matches!(
                    self.service,
                    super::service::OwnerTargetServiceState::Bound { .. }
                ) {
                    self.wait_bound_completion_capacity(flow_id)?;
                }
                Ok(())
            }
            Err(SessionSupervisorError::RejectedCommand { block, command }) => {
                self.retain_rejected_completion(expected, flow_id, command)?;
                Err(OwnerTargetError::UnexpectedTargetCompletionAdmission { flow_id, block })
            }
            Err(error) => Err(OwnerTargetError::Supervisor(error)),
        }
    }
}
