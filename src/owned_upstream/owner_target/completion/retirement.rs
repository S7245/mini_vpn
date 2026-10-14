//! Terminal finish, join, receipt disposition and exact Target retirement.
//!
//! Owns the existing Executor's complete terminal/join closure, not a new owner.
//! Original Flow/phase/feedback storage stays in the parent completion Module;
//! source reads and Bound scheduling stay at their existing seams.
//! Preserve original errors, ordering, retry ownership and effective caller scope.
//! Entry methods finish/retire/join/settle/expire remain owner_target-scoped.
//! Tests live in owner_target tests and commands/tests/bound/retirement.rs.

use super::*;

impl<T: TargetIo> OwnerTargetExecutor<T> {
    pub(in crate::owned_upstream::owner_target) fn finish_ready_retirement(
        &mut self,
        selection: &crate::owned_upstream::owner_ready::OwnerReadyContinuationSelection,
        eligible: usize,
    ) -> Result<usize, super::super::OwnerTargetInputBlock> {
        use super::super::OwnerTargetInputBlock::ReadyReceiptInvariant as Invalid;
        use super::super::service::{OwnerFlowServiceSlot, OwnerTargetServiceState};
        use crate::owned_upstream::owner_ready::OwnerReadyReadResult;
        let selected = selection.candidate.as_ref().ok_or(Invalid)?;
        let flow_id = selected.association.flow_id();
        let owner = self.flows.get(&flow_id).ok_or(Invalid)?;
        let OwnerTargetServiceState::Bound { flows, binding, .. } = &mut self.service else {
            return Err(Invalid);
        };
        let Some(OwnerFlowServiceSlot::Retiring(record)) = flows.slots.get(selected.position)
        else {
            return Err(Invalid);
        };
        if owner.service_position != Some(selected.position)
            || !owner.retire_after_join
            || owner.target_present
            || owner.source.is_some()
            || owner.join_pending
            || owner.pending_sink.is_some()
            || self.target_tombstones.contains_key(&flow_id)
            || record.association != selected.association
            || record.generation != selected.generation
            || record.transit != 0
            || record.wait.is_some()
            || !record.tickets.retirement_references_are_unique()
            || !matches!(&owner.pending_completion, Some(PendingTargetCompletion::TerminalSettling(settlement))
                if matches!(settlement.source, TerminalSourceSettlement::Released)
                && !settlement.join.is_failed()
                && settlement.feedback_released())
        {
            return Err(Invalid);
        }
        // All resource destruction already ran while the original Flow held
        // Releasing. Final metadata release and runnable computation are one
        // operation, followed only by resource-free original record removal.
        let runnable = binding
            .finish_continuation(
                selection,
                OwnerReadyReadResult::RetirementReleased,
                eligible,
            )
            .map_err(|_| Invalid)?;
        flows.slots[selected.position] = OwnerFlowServiceSlot::Free;
        self.flows.remove(&flow_id);
        Ok(runnable)
    }

    /// One selected unit on the original terminal. Cold revocation, each
    /// Source receive, readiness, join and publisher release are separate
    /// visits. No global retry, unselected drain or fabricated EOF is allowed.
    pub(in crate::owned_upstream::owner_target) fn service_ready_retirement(
        &mut self,
        selection: &crate::owned_upstream::owner_ready::OwnerReadyContinuationSelection,
    ) -> Result<crate::owned_upstream::owner_ready::OwnerReadyReadResult, OwnerTargetError> {
        use super::super::service::{OwnerFlowServiceSlot, OwnerTargetServiceState};
        use crate::owned_upstream::owner_ready::{
            OwnerReadyCategory, OwnerReadyReadResult as Value, SendLane,
        };
        use crate::owned_upstream::target::{TargetReadyInterest, TargetReadyOutcome};
        use OwnerTargetError::BoundSourceRequiresSelectedService as Invalid;
        use std::task::{Context, Poll};
        let selected = selection.candidate.as_ref().ok_or(Invalid)?;
        let flow_id = selected.association.flow_id();
        let OwnerTargetServiceState::Bound { flows, .. } = &self.service else {
            return Err(Invalid);
        };
        let Some(OwnerFlowServiceSlot::Retiring(record)) = flows.slots.get(selected.position)
        else {
            return Err(Invalid);
        };
        if record.wait.is_some() {
            return Ok(Value::Pending);
        }
        if matches!(&self.flows.get(&flow_id).ok_or(Invalid)?.pending_completion,
            Some(PendingTargetCompletion::TerminalSettling(settlement))
            if settlement.join.is_failed())
        {
            return Err(Invalid);
        }
        if self.dispose_terminal_returned_completion(flow_id)? {
            return Ok(Value::Hint);
        }
        let OwnerTargetServiceState::Bound { flows, .. } = &self.service else {
            return Err(Invalid);
        };
        let Some(OwnerFlowServiceSlot::Retiring(record)) = flows.slots.get(selected.position)
        else {
            return Err(Invalid);
        };
        let owner = self.flows.get_mut(&flow_id).ok_or(Invalid)?;
        let Some(PendingTargetCompletion::TerminalSettling(settlement)) =
            &mut owner.pending_completion
        else {
            return Err(Invalid);
        };
        if matches!(settlement.source, TerminalSourceSettlement::Cold) {
            if let Some(source) = &mut owner.source {
                let local = settlement.terminal.local_flow();
                if source.flow.local_flow() != local || source.driver.local_flow() != local {
                    return Err(Invalid);
                }
                // Sticky before the actual admission lock, receiver close and
                // any Source progress callback. Never roll an unwind to Cold.
                settlement.source = TerminalSourceSettlement::Revoking;
                source.driver.revoke_owner_terminal_source(
                    &source.flow,
                    &self.source_factory,
                    settlement.terminal,
                )?;
                settlement.source = TerminalSourceSettlement::Draining {
                    control_drained: false,
                    ordered_drained: false,
                };
            } else {
                settlement.source = TerminalSourceSettlement::Absent;
            }
            return Ok(Value::Hint);
        }
        let lane = match settlement.source {
            TerminalSourceSettlement::Draining {
                control_drained: false,
                ..
            } => Some(SendLane::Control),
            TerminalSourceSettlement::Draining {
                ordered_drained: false,
                ..
            } => Some(SendLane::OrderedSource),
            TerminalSourceSettlement::Revoking | TerminalSourceSettlement::Releasing => {
                return Err(Invalid);
            }
            _ => None,
        };
        if let Some(lane) = lane {
            return self.drain_ready_retirement_source(selection, lane);
        }
        // Every actual receipt owns its original transit charge. A later inbox
        // disposition explicitly requests this same Retirement candidate.
        if record.transit != 0 {
            return Ok(Value::Pending);
        }
        match &settlement.feedback {
            TerminalFeedback::Failed {
                cause, feedback, ..
            } if matches!(feedback, TargetFailureFeedback::Settled)
                || matches!(feedback, TargetFailureFeedback::Driver(
                    DriverCompletionState::Reduced(local) | DriverCompletionState::TerminalDisposed(local))
                    if *local == settlement.terminal.local_flow()) =>
            {
                let cause = *cause;
                let work = std::mem::replace(
                    &mut settlement.feedback,
                    TerminalFeedback::FailedReleasing { cause },
                );
                // The original terminal, Source and F stay resident. If any
                // destructor unwinds, Releasing and the first cause survive.
                drop(work);
                settlement.feedback = TerminalFeedback::FailedReleased { cause };
                return Ok(Value::Hint);
            }
            TerminalFeedback::FailedReleasing { .. } => return Err(Invalid),
            _ => {}
        }
        if !settlement.feedback_released() {
            return Ok(Value::Pending);
        }
        if owner.target_present {
            if matches!(settlement.join, TerminalJoinPhase::AwaitingReadiness) {
                let waker = record
                    .tickets
                    .target_waker(OwnerReadyCategory::JoinReady)
                    .map_err(|_| Invalid)?;
                match self.target.poll_readiness(
                    flow_id,
                    TargetReadyInterest::Join,
                    &mut Context::from_waker(&waker),
                ) {
                    Poll::Pending => return Ok(Value::Pending),
                    Poll::Ready(Ok(TargetReadyOutcome::Attempt)) => {
                        settlement.join = TerminalJoinPhase::Ready;
                        return Ok(Value::Hint);
                    }
                    Poll::Ready(Ok(TargetReadyOutcome::Failed(cause))) => {
                        settlement.join = TerminalJoinPhase::OperationalFailure(cause);
                        return Err(Invalid);
                    }
                    Poll::Ready(Err(error)) => {
                        settlement.join = TerminalJoinPhase::InvariantFailure(error);
                        return Err(Invalid);
                    }
                }
            }
            settlement.join = TerminalJoinPhase::AwaitingReadiness;
            self.try_join_target_flow(flow_id)?;
            // A Pending join must next register on the original named Join
            // ticket; never park solely on the synchronous Pending value.
            return Ok(Value::Hint);
        }
        if owner.join_pending
            || owner.pending_sink.is_some()
            || self.target_tombstones.contains_key(&flow_id)
        {
            return Err(Invalid);
        }
        if !matches!(settlement.source, TerminalSourceSettlement::Released) {
            settlement.source = TerminalSourceSettlement::Releasing;
            // Original Flow remains F-owned and sticky across destructor/wake.
            drop(owner.source.take());
            settlement.source = TerminalSourceSettlement::Released;
            return Ok(Value::Hint);
        }
        if !record.tickets.retirement_references_are_unique() {
            return Ok(Value::Pending);
        }
        Ok(Value::RetirementReleased)
    }

    /// A Returned RESET is already at this terminal owner, not in the inbox.
    /// Preflight its full Source/terminal and original nonzero Driver charge
    /// before extracting the command. This never calls the retired reducer.
    fn dispose_terminal_returned_completion(
        &mut self,
        flow_id: SessionFlowId,
    ) -> Result<bool, OwnerTargetError> {
        use super::super::service::{OwnerFlowServiceSlot, OwnerTargetServiceState};
        let invalid = || OwnerTargetError::CarriedTargetCompletionMismatch { flow_id };
        let owner = self.flows.get(&flow_id).ok_or_else(invalid)?;
        let Some(PendingTargetCompletion::TerminalSettling(settlement)) = &owner.pending_completion
        else {
            return Err(invalid());
        };
        let state = match &settlement.feedback {
            TerminalFeedback::Driver(state)
            | TerminalFeedback::Failed {
                feedback: TargetFailureFeedback::Driver(state),
                ..
            } => state,
            _ => return Ok(false),
        };
        let command = match state {
            DriverCompletionState::Returned(command) => command,
            DriverCompletionState::TerminalDisposing(_) => return Err(invalid()),
            _ => return Ok(false),
        };
        let local = settlement.terminal.local_flow();
        if returned_reset_flow(command) != Some(local) {
            return Err(invalid());
        }
        let position = self.driver_charge_position(flow_id, local)?;
        let source = owner.source.as_ref().ok_or_else(invalid)?;
        let SessionOwnerCommand::Driver(input) = command.as_ref() else {
            return Err(invalid());
        };
        source.driver.preflight_owner_terminal_input(
            &source.flow,
            &self.source_factory,
            settlement.terminal,
            input,
        )?;

        let owner = self.flows.get_mut(&flow_id).ok_or_else(invalid)?;
        let Some(PendingTargetCompletion::TerminalSettling(settlement)) =
            &mut owner.pending_completion
        else {
            return Err(invalid());
        };
        let state = match &mut settlement.feedback {
            TerminalFeedback::Driver(state)
            | TerminalFeedback::Failed {
                feedback: TargetFailureFeedback::Driver(state),
                ..
            } => state,
            _ => return Err(invalid()),
        };
        let OwnerTargetServiceState::Bound { flows, .. } = &mut self.service else {
            return Err(invalid());
        };
        let Some(OwnerFlowServiceSlot::Live(record) | OwnerFlowServiceSlot::Retiring(record)) =
            flows.slots.get_mut(position)
        else {
            return Err(invalid());
        };
        // The same exclusive Executor was held throughout preflight. Publish
        // sticky ownership before consuming; no callback/fallibility afterward.
        let old = std::mem::replace(state, DriverCompletionState::TerminalDisposing(local));
        let DriverCompletionState::Returned(command) = old else {
            *state = old;
            return Err(invalid());
        };
        drop(command);
        record.transit -= 1;
        *state = DriverCompletionState::TerminalDisposed(local);
        Ok(true)
    }

    pub(in crate::owned_upstream::owner_target) fn finish_target_flow(
        &mut self,
        terminal_proof: super::super::TerminalGrace,
        reason: FlowFinishReason,
    ) -> Result<(), OwnerTargetError> {
        let flow_id = terminal_proof.flow_id();
        let bound = matches!(
            self.service,
            super::super::service::OwnerTargetServiceState::Bound { .. }
        );
        if !bound {
            let owner = self
                .flows
                .get_mut(&flow_id)
                .ok_or(OwnerTargetError::UnknownTargetFlow { flow_id })?;
            if let Some(source) = &owner.source
                && (source.flow.local_flow() != terminal_proof.local_flow()
                    || source.driver.local_flow() != terminal_proof.local_flow())
            {
                return Err(OwnerTargetError::CarriedTargetCompletionMismatch { flow_id });
            }
            if let Some(PendingTargetCompletion::Open(open)) = &owner.pending_completion
                && !open.matches_finished(owner, terminal_proof, reason)
            {
                return Err(OwnerTargetError::CarriedTargetCompletionMismatch { flow_id });
            }
            let feedback = match owner.pending_completion.take() {
                None => TerminalFeedback::None,
                Some(PendingTargetCompletion::Open(open)) => TerminalFeedback::Open(open),
                Some(pending) => {
                    owner.pending_completion = Some(pending);
                    return Err(OwnerTargetError::DuplicatePendingTargetCompletion { flow_id });
                }
            };
            // No revoke or callback here. The real proof survives Target pressure
            // and a later binding move; Unbound successful join still removes it.
            owner.pending_completion = Some(PendingTargetCompletion::TerminalSettling(
                TerminalFlowSettlement {
                    terminal: terminal_proof,
                    reason,
                    feedback,
                    source: TerminalSourceSettlement::Cold,
                    join: TerminalJoinPhase::AwaitingReadiness,
                },
            ));
        }
        if bound {
            let owner = self
                .flows
                .get_mut(&flow_id)
                .ok_or(OwnerTargetError::UnknownTargetFlow { flow_id })?;
            if matches!(
                owner.pending_completion,
                Some(PendingTargetCompletion::TerminalSettling(_))
            ) {
                return Err(OwnerTargetError::DuplicatePendingTargetCompletion { flow_id });
            }
            let drain_wakers = if let Some(source) = &owner.source {
                use super::super::service::{OwnerFlowServiceSlot, OwnerTargetServiceState};
                use crate::owned_upstream::owner_ready::{OwnerReadyFlowAssociation, SendLane};
                let invalid = || OwnerTargetError::CarriedTargetCompletionMismatch { flow_id };
                let local = terminal_proof.local_flow();
                if source.flow.local_flow() != local || source.driver.local_flow() != local {
                    return Err(invalid());
                }
                let position = owner.service_position.ok_or_else(invalid)?;
                let OwnerTargetServiceState::Bound { flows, .. } = &self.service else {
                    return Err(invalid());
                };
                let Some(
                    OwnerFlowServiceSlot::Live(record) | OwnerFlowServiceSlot::Retiring(record),
                ) = flows.slots.get(position)
                else {
                    return Err(invalid());
                };
                if record.generation == 0
                    || record.association != OwnerReadyFlowAssociation::Source(local)
                    || owner.retire_after_join
                        != matches!(flows.slots[position], OwnerFlowServiceSlot::Retiring(_))
                {
                    return Err(invalid());
                }
                Some([
                    record
                        .tickets
                        .driver_waker(SendLane::Control)
                        .map_err(|_| invalid())?,
                    record
                        .tickets
                        .driver_waker(SendLane::OrderedSource)
                        .map_err(|_| invalid())?,
                ])
            } else {
                if !matches!(&owner.pending_completion,
                    Some(PendingTargetCompletion::Open(open))
                    if open.matches_finished(owner, terminal_proof, reason))
                {
                    return Err(OwnerTargetError::MissingTargetReadSource { flow_id });
                }
                None
            };
            // Publish the real terminal before any Target callback or release.
            // Source/Target retirement is deliberately still owed by this Flow.
            let feedback = match owner.pending_completion.take() {
                Some(PendingTargetCompletion::Failed {
                    work,
                    cause,
                    feedback,
                }) => TerminalFeedback::Failed {
                    work,
                    cause,
                    feedback,
                },
                None => TerminalFeedback::None,
                Some(PendingTargetCompletion::StagedReset(local))
                    if reason == FlowFinishReason::LocalReset(ResetReason::TargetFailure)
                        && local == terminal_proof.local_flow() =>
                {
                    TerminalFeedback::None
                }
                Some(PendingTargetCompletion::StagedReset(local)) => {
                    TerminalFeedback::InTransit(local)
                }
                Some(PendingTargetCompletion::Failure { event }) => {
                    TerminalFeedback::Returned(event)
                }
                Some(PendingTargetCompletion::DriverReturned(DriverCompletionState::Reduced(
                    local,
                ))) if local == terminal_proof.local_flow()
                    && reason == FlowFinishReason::LocalReset(ResetReason::TargetFailure) =>
                {
                    TerminalFeedback::None
                }
                Some(PendingTargetCompletion::DriverReturned(state)) => {
                    TerminalFeedback::Driver(state)
                }
                Some(PendingTargetCompletion::Open(open)) => TerminalFeedback::Open(open),
                Some(PendingTargetCompletion::HalfClose { event }) => {
                    TerminalFeedback::HalfClose(event)
                }
                Some(settled @ PendingTargetCompletion::TerminalSettling(_)) => {
                    owner.pending_completion = Some(settled);
                    return Err(OwnerTargetError::DuplicatePendingTargetCompletion { flow_id });
                }
            };
            owner.pending_completion = Some(PendingTargetCompletion::TerminalSettling(
                TerminalFlowSettlement {
                    terminal: terminal_proof,
                    reason,
                    feedback,
                    source: if owner.source.is_some() {
                        TerminalSourceSettlement::Revoking
                    } else {
                        TerminalSourceSettlement::Absent
                    },
                    join: TerminalJoinPhase::AwaitingReadiness,
                },
            ));
            if drain_wakers.is_some() {
                self.suspend_bound_reads(flow_id)?;
            }
            let owner = self
                .flows
                .get_mut(&flow_id)
                .ok_or(OwnerTargetError::UnknownTargetFlow { flow_id })?;
            if let Some(source) = &mut owner.source {
                let Some(PendingTargetCompletion::TerminalSettling(settlement)) =
                    &mut owner.pending_completion
                else {
                    return Err(OwnerTargetError::MissingPendingTargetCompletion { flow_id });
                };
                source.driver.revoke_owner_terminal_source(
                    &source.flow,
                    &self.source_factory,
                    terminal_proof,
                )?;
                settlement.source = TerminalSourceSettlement::Draining {
                    control_drained: false,
                    ordered_drained: false,
                };
            }
            // Receiver close does not wake an empty parked lane. The original
            // pre-acquired publishers retain inactive due or rearm active work;
            // Draining is already owned before either notification can escape.
            if let Some(wakers) = drain_wakers {
                for waker in wakers {
                    waker.wake_by_ref();
                }
            }
        }
        let target_present = self
            .flows
            .get(&flow_id)
            .ok_or(OwnerTargetError::UnknownTargetFlow { flow_id })?
            .target_present;
        if !target_present {
            if !bound {
                self.flows.remove(&flow_id);
            }
            return Ok(());
        }
        match reason {
            FlowFinishReason::PeerReset(_) => {
                if self
                    .flows
                    .get(&flow_id)
                    .is_some_and(|flow| flow.terminal.is_none())
                {
                    let terminal = match self.target.reset(flow_id)? {
                        TargetResetCompletion::Reset { .. }
                        | TargetResetCompletion::AlreadyReset => TargetTerminalKind::Reset,
                        TargetResetCompletion::AlreadyTerminal(kind) => kind,
                    };
                    self.flows
                        .get_mut(&flow_id)
                        .ok_or(OwnerTargetError::UnknownTargetFlow { flow_id })?
                        .terminal = Some(terminal);
                }
            }
            FlowFinishReason::LocalReset(_) | FlowFinishReason::OpenFailed(_) => {
                if self
                    .flows
                    .get(&flow_id)
                    .is_some_and(|flow| flow.terminal.is_none())
                {
                    let terminal = match self.target.cancel(flow_id)? {
                        TargetCancelCompletion::Cancelled { .. }
                        | TargetCancelCompletion::AlreadyCancelled => TargetTerminalKind::Cancelled,
                        TargetCancelCompletion::AlreadyTerminal(kind) => kind,
                    };
                    self.flows
                        .get_mut(&flow_id)
                        .ok_or(OwnerTargetError::UnknownTargetFlow { flow_id })?
                        .terminal = Some(terminal);
                }
            }
            FlowFinishReason::Graceful => {
                // The Target adapter owns the graceful terminal transition:
                // all reads reached EOF and the write side was half-closed.
            }
        }
        self.flows
            .get_mut(&flow_id)
            .ok_or(OwnerTargetError::UnknownTargetFlow { flow_id })?
            .pending_sink = None;
        let _ = self.try_join_target_flow(flow_id)?;
        Ok(())
    }

    pub(in crate::owned_upstream::owner_target) fn retire_target_tombstone(
        &mut self,
        flow_id: SessionFlowId,
    ) -> Result<(), OwnerTargetError> {
        use super::super::service::{OwnerFlowServiceSlot, OwnerTargetServiceState};
        if let OwnerTargetServiceState::Bound { flows, binding, .. } = &mut self.service
            && let Some(owner) = self.flows.get_mut(&flow_id)
        {
            use OwnerTargetError::BoundSourceRequiresSelectedService as Invalid;
            let position = owner.service_position.ok_or(Invalid)?;
            let Some(OwnerFlowServiceSlot::Live(record) | OwnerFlowServiceSlot::Retiring(record)) =
                flows.slots.get(position)
            else {
                return Err(Invalid);
            };
            if record.association.flow_id() != flow_id
                || !matches!(
                    owner.pending_completion,
                    Some(PendingTargetCompletion::TerminalSettling(_))
                )
            {
                return Err(Invalid);
            }
            binding
                .request_retained_flow_work(
                    position,
                    (
                        record.association,
                        record.generation,
                        owner.retire_after_join,
                    ),
                    crate::owned_upstream::owner_ready::OwnerReadyCategory::Retirement,
                )
                .map_err(|_| Invalid)?;
            owner.retire_after_join = true;
            let slot = std::mem::replace(&mut flows.slots[position], OwnerFlowServiceSlot::Free);
            flows.slots[position] = match slot {
                OwnerFlowServiceSlot::Live(record) | OwnerFlowServiceSlot::Retiring(record) => {
                    OwnerFlowServiceSlot::Retiring(record)
                }
                OwnerFlowServiceSlot::Free => return Err(Invalid),
            };
        }
        if self.target_tombstones.contains_key(&flow_id) {
            self.retire_recorded_target_tombstone(flow_id)?;
        } else {
            match self.target.retire_terminal_tombstone(flow_id)? {
                TargetTombstoneRetireCompletion::Retired(actual) => {
                    return Err(OwnerTargetError::UnexpectedTargetTombstone { flow_id, actual });
                }
                TargetTombstoneRetireCompletion::Absent => {
                    if let Some(flow) = self
                        .flows
                        .get_mut(&flow_id)
                        .filter(|flow| flow.target_present && flow.join_pending)
                    {
                        // Terminal grace can expire while the Target still
                        // owns application buffers or while the bounded
                        // tombstone table blocks join. Preserve that exact
                        // retirement fact and consume it immediately after
                        // a later successful join.
                        flow.retire_after_join = true;
                    }
                }
            }
        }
        if matches!(self.service, OwnerTargetServiceState::Bound { .. }) {
            Ok(())
        } else {
            self.drain_pending_target_joins()
        }
    }

    fn retire_recorded_target_tombstone(
        &mut self,
        flow_id: SessionFlowId,
    ) -> Result<(), OwnerTargetError> {
        let expected = self
            .target_tombstones
            .get(&flow_id)
            .copied()
            .ok_or(OwnerTargetError::UnrecordedTargetTombstone { flow_id })?;
        match self.target.retire_terminal_tombstone(flow_id)? {
            TargetTombstoneRetireCompletion::Retired(actual) if actual == expected => {
                self.target_tombstones.remove(&flow_id);
                Ok(())
            }
            TargetTombstoneRetireCompletion::Retired(actual) => {
                Err(OwnerTargetError::TargetTombstoneKindMismatch {
                    flow_id,
                    expected,
                    actual,
                })
            }
            TargetTombstoneRetireCompletion::Absent => {
                Err(OwnerTargetError::MissingTargetTombstone { flow_id })
            }
        }
    }

    pub(in crate::owned_upstream::owner_target) fn try_join_target_flow(
        &mut self,
        flow_id: SessionFlowId,
    ) -> Result<TargetJoinAttempt, OwnerTargetError> {
        let retire_after_join = self
            .flows
            .get(&flow_id)
            .ok_or(OwnerTargetError::UnknownTargetFlow { flow_id })?
            .retire_after_join;
        if !retire_after_join && self.target_tombstones.contains_key(&flow_id) {
            return Err(OwnerTargetError::DuplicateTargetTombstone { flow_id });
        }
        let terminal = match self.target.join(flow_id)? {
            TargetJoinCompletion::Joined(terminal)
            | TargetJoinCompletion::AlreadyJoined(terminal) => terminal,
            TargetJoinCompletion::PendingOwnedState { .. } => {
                self.flows
                    .get_mut(&flow_id)
                    .ok_or(OwnerTargetError::UnknownTargetFlow { flow_id })?
                    .join_pending = true;
                return Ok(TargetJoinAttempt::Pending(TargetJoinBlock::OwnedState));
            }
            TargetJoinCompletion::PendingTerminalCapacity { .. } => {
                self.flows
                    .get_mut(&flow_id)
                    .ok_or(OwnerTargetError::UnknownTargetFlow { flow_id })?
                    .join_pending = true;
                return Ok(TargetJoinAttempt::Pending(
                    TargetJoinBlock::TerminalCapacity,
                ));
            }
        };
        if retire_after_join {
            self.retire_just_joined_tombstone(flow_id, terminal)?;
        } else {
            let previous = self.target_tombstones.insert(flow_id, terminal);
            debug_assert!(previous.is_none());
        }
        if matches!(
            self.service,
            super::super::service::OwnerTargetServiceState::Bound { .. }
        ) {
            let owner = self
                .flows
                .get_mut(&flow_id)
                .ok_or(OwnerTargetError::UnknownTargetFlow { flow_id })?;
            if !matches!(
                owner.pending_completion,
                Some(PendingTargetCompletion::TerminalSettling(_))
            ) {
                return Err(OwnerTargetError::MissingPendingTargetCompletion { flow_id });
            }
            owner.target_present = false;
            owner.join_pending = false;
            owner.terminal = Some(terminal);
        } else {
            self.flows.remove(&flow_id);
        }
        Ok(TargetJoinAttempt::Joined)
    }

    /// Called only after the closed receipt preflight proved original source,
    /// metadata and nonzero transit. A terminal disposition is not reduction.
    pub(in crate::owned_upstream::owner_target) fn settle_terminal_receipt(
        &mut self,
        flow_id: SessionFlowId,
        command: SessionOwnerCommand,
    ) -> Result<super::super::OwnerTargetObservedApply, OwnerTargetError> {
        let owner = self
            .flows
            .get_mut(&flow_id)
            .ok_or(OwnerTargetError::UnknownTargetFlow { flow_id })?;
        let Some(PendingTargetCompletion::TerminalSettling(settlement)) =
            &mut owner.pending_completion
        else {
            return Err(OwnerTargetError::MissingPendingTargetCompletion { flow_id });
        };
        let valid = match (&owner.source, &command) {
            (Some(source), SessionOwnerCommand::Driver(input)) => source
                .driver
                .preflight_owner_terminal_input(
                    &source.flow,
                    &self.source_factory,
                    settlement.terminal,
                    input,
                )
                .is_ok(),
            _ => false,
        };
        if !valid {
            return Err(OwnerTargetError::RejectedCommand {
                block: super::super::OwnerTargetInputBlock::ReadyReceiptInvariant,
                command: Box::new(command),
            });
        }
        if matches!(
            command,
            SessionOwnerCommand::Driver(DriverInput::Control(SessionEvent::LocalReset { .. }))
        ) {
            match &mut settlement.feedback {
                TerminalFeedback::Failed { feedback, .. } => {
                    *feedback = TargetFailureFeedback::Settled
                }
                feedback => *feedback = TerminalFeedback::None,
            }
        }
        // Actual DATA releases its original permits here, under the retained
        // full-source terminal. It never enters the already-retired reducer.
        drop(command);
        Ok(super::super::OwnerTargetObservedApply {
            outputs: Vec::new(),
            observations: OwnerTargetReducerObservations::default(),
        })
    }

    fn retire_just_joined_tombstone(
        &mut self,
        flow_id: SessionFlowId,
        expected: TargetTerminalKind,
    ) -> Result<(), OwnerTargetError> {
        match self.target.retire_terminal_tombstone(flow_id)? {
            TargetTombstoneRetireCompletion::Retired(actual) if actual == expected => Ok(()),
            TargetTombstoneRetireCompletion::Retired(actual) => {
                Err(OwnerTargetError::TargetTombstoneKindMismatch {
                    flow_id,
                    expected,
                    actual,
                })
            }
            TargetTombstoneRetireCompletion::Absent => {
                Err(OwnerTargetError::MissingTargetTombstone { flow_id })
            }
        }
    }

    fn drain_pending_target_joins(&mut self) -> Result<(), OwnerTargetError> {
        let pending = self
            .flows
            .iter()
            .filter_map(|(flow_id, flow)| flow.join_pending.then_some(*flow_id))
            .collect::<Vec<_>>();
        for flow_id in pending {
            match self.try_join_target_flow(flow_id)? {
                TargetJoinAttempt::Joined
                | TargetJoinAttempt::Pending(TargetJoinBlock::OwnedState) => {}
                TargetJoinAttempt::Pending(TargetJoinBlock::TerminalCapacity) => {
                    // The table is session-scoped: once one join observes it
                    // full, no later pending flow can make progress until the
                    // next exact retirement. Avoid an O(pending) tail of
                    // equivalent failed Target calls.
                    break;
                }
            }
        }
        Ok(())
    }

    /// Performs at most one Target retirement/expiry transaction. Returning
    /// `true` asks the bounded executor to schedule another exact cleanup
    /// step; `false` proves all Target ownership is gone.
    pub(in crate::owned_upstream::owner_target) fn expire_next_target(
        &mut self,
    ) -> Result<bool, OwnerTargetError> {
        // Session expiry invalidates every reducer terminal capability. Retire
        // already-joined Target tombstones first so the finite table cannot
        // block cancellation/join of a live flow.
        if let Some(flow_id) = self.target_tombstones.keys().next().copied() {
            self.retire_recorded_target_tombstone(flow_id)?;
            return Ok(true);
        }
        let Some(flow_id) = self.flows.keys().next().copied() else {
            return Ok(false);
        };
        if !self
            .flows
            .get(&flow_id)
            .is_some_and(|flow| flow.target_present)
        {
            self.flows.remove(&flow_id);
            return Ok(true);
        }
        let terminal = self.target.expire_session_flow(flow_id)?.terminal;
        if let Some(flow) = self.flows.get_mut(&flow_id) {
            flow.pending_sink = None;
            flow.terminal = Some(terminal);
        }
        let joined_terminal = match self.target.join(flow_id)? {
            TargetJoinCompletion::Joined(terminal)
            | TargetJoinCompletion::AlreadyJoined(terminal) => terminal,
            TargetJoinCompletion::PendingOwnedState { .. }
            | TargetJoinCompletion::PendingTerminalCapacity { .. } => {
                return Err(OwnerTargetError::UnexpectedPendingTargetJoin { flow_id });
            }
        };
        // Do not fill the bounded tombstone table during session-wide
        // teardown. Join and retire this exact flow before moving to the next.
        self.retire_just_joined_tombstone(flow_id, joined_terminal)?;
        self.flows.remove(&flow_id);
        Ok(true)
    }
}
