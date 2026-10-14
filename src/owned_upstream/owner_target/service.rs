//! Bound Flow association, selected source work and exact ownership retention.
//!
//! Owns the service positions/tickets, not second Target or Driver instances.
//! Original TargetFlowOwner keeps all business resources and its stable position.
//! Binding imports existing state once; later work uses one indexed candidate.
//! Commands, Flow backing and the ready binding drop in that declared order.
//! Entries are the Executor's closed binding and selected service operations;
//! tests live in commands/tests/bound.rs and the actual CurrentOnly Bound tracer.

use super::commands::SessionCommands;
use super::completion::CompletionReceipt;
use super::{
    DriverInput, OwnerTargetInputBlock, PendingTargetCompletion, ReducerAdmissionBlock,
    SessionEvent, SessionOwnerCommand, SessionSupervisorError,
};
use crate::owned_upstream::owner_ready::{
    OwnerReadyFlowAssociation, OwnerReadyFlowTickets, OwnerReadySessionBinding,
    OwnerReadySessionTickets,
};
use crate::owned_upstream::session::SessionMailboxLane;

// A borrowed preflight records only the actual owning position. It neither
// grants reduction authority nor replaces the command or its transit charge.
struct ReadyReceiptPosition {
    position: usize,
    flow_id: super::SessionFlowId,
    disposition: CompletionReceipt,
}

pub(super) enum OwnerTargetServiceState {
    Unbound,
    Bound {
        commands: SessionCommands,
        flows: OwnerFlowServiceBacking,
        binding: OwnerReadySessionBinding,
    },
}

pub(super) struct OwnerFlowServiceBacking {
    pub(super) slots: Vec<OwnerFlowServiceSlot>,
    pub(super) session: OwnerReadySessionTickets,
    pub(super) waits: [OwnerFlowWaitQueue; 3],
}

// Scheduling links borrow positions in the original F backing. They never
// own a command, Source, Target or another completion. One membership per F.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum OwnerFlowWaitResource {
    ControlCell,
    OrderedCell,
    CompletionCapacity,
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct OwnerFlowWaitKey {
    position: usize,
    generation: u64,
}

#[derive(Clone, Copy, Default)]
pub(super) struct OwnerFlowWaitQueue {
    first: Option<OwnerFlowWaitKey>,
    last: Option<OwnerFlowWaitKey>,
}

#[derive(Clone, Copy)]
pub(super) struct OwnerFlowWait {
    resource: OwnerFlowWaitResource,
    next: Option<OwnerFlowWaitKey>,
}

pub(super) enum OwnerFlowServiceSlot {
    Free,
    Live(OwnerFlowServicePosition),
    Retiring(OwnerFlowServicePosition),
}

pub(super) struct OwnerFlowServicePosition {
    pub(super) association: OwnerReadyFlowAssociation,
    pub(super) generation: u64,
    pub(super) transit: usize,
    pub(super) tickets: OwnerReadyFlowTickets,
    pub(super) wait: Option<OwnerFlowWait>,
}

impl OwnerFlowServiceBacking {
    fn wait_record(
        &self,
        key: OwnerFlowWaitKey,
    ) -> Result<&OwnerFlowServicePosition, super::OwnerTargetError> {
        match self.slots.get(key.position) {
            Some(OwnerFlowServiceSlot::Live(record) | OwnerFlowServiceSlot::Retiring(record))
                if record.generation == key.generation && key.generation != 0 =>
            {
                Ok(record)
            }
            _ => Err(super::OwnerTargetError::BoundSourceRequiresSelectedService),
        }
    }

    pub(super) fn wait_for(
        &mut self,
        position: usize,
        resource: OwnerFlowWaitResource,
    ) -> Result<(), super::OwnerTargetError> {
        use super::OwnerTargetError::BoundSourceRequiresSelectedService as Invalid;
        let Some(OwnerFlowServiceSlot::Live(record) | OwnerFlowServiceSlot::Retiring(record)) =
            self.slots.get(position)
        else {
            return Err(Invalid);
        };
        let key = OwnerFlowWaitKey {
            position,
            generation: record.generation,
        };
        if let Some(wait) = record.wait {
            return if wait.resource == resource {
                Ok(())
            } else {
                Err(Invalid)
            };
        }
        let queue = self.waits[resource as usize];
        if (queue.first.is_none() != queue.last.is_none()) || key.generation == 0 {
            return Err(Invalid);
        }
        if let Some(last) = queue.last {
            let wait = self.wait_record(last)?.wait.ok_or(Invalid)?;
            if wait.resource != resource || wait.next.is_some() || last == key {
                return Err(Invalid);
            }
        }
        // All checks precede the plain linking moves; no callback in between.
        if let Some(last) = queue.last {
            if let OwnerFlowServiceSlot::Live(record) | OwnerFlowServiceSlot::Retiring(record) =
                &mut self.slots[last.position]
            {
                if let Some(wait) = &mut record.wait {
                    wait.next = Some(key);
                }
            }
        } else {
            self.waits[resource as usize].first = Some(key);
        }
        if let OwnerFlowServiceSlot::Live(record) | OwnerFlowServiceSlot::Retiring(record) =
            &mut self.slots[position]
        {
            record.wait = Some(OwnerFlowWait {
                resource,
                next: None,
            });
        }
        self.waits[resource as usize].last = Some(key);
        Ok(())
    }

    // Wake one exact original waiter, not a scan or a draining loop. If that
    // visit observes EOF without occupying the resource it must hand off again.
    pub(super) fn handoff_waiter(
        &mut self,
        resource: OwnerFlowWaitResource,
        binding: &OwnerReadySessionBinding,
    ) -> Result<(), super::OwnerTargetError> {
        use super::OwnerTargetError::BoundSourceRequiresSelectedService as Invalid;
        use crate::owned_upstream::owner_ready::OwnerReadyCategory;
        let queue = self.waits[resource as usize];
        let Some(key) = queue.first else {
            return if queue.last.is_none() {
                Ok(())
            } else {
                Err(Invalid)
            };
        };
        let record = self.wait_record(key)?;
        let wait = record.wait.ok_or(Invalid)?;
        if wait.resource != resource || (wait.next.is_none() != (queue.last == Some(key))) {
            return Err(Invalid);
        }
        if let Some(next) = wait.next {
            if next == key
                || self
                    .wait_record(next)?
                    .wait
                    .is_none_or(|wait| wait.resource != resource)
            {
                return Err(Invalid);
            }
        }
        let retiring = matches!(self.slots[key.position], OwnerFlowServiceSlot::Retiring(_));
        binding
            .request_retained_flow_work(
                key.position,
                (record.association, key.generation, retiring),
                if resource == OwnerFlowWaitResource::CompletionCapacity {
                    OwnerReadyCategory::Completion
                } else {
                    OwnerReadyCategory::Retirement
                },
            )
            .map_err(|_| Invalid)?;
        // Publication may fail or unwind: membership is unchanged until it has
        // returned. Only resource-free scheduling links are removed afterward.
        self.waits[resource as usize].first = wait.next;
        if wait.next.is_none() {
            self.waits[resource as usize].last = None;
        }
        if let OwnerFlowServiceSlot::Live(record) | OwnerFlowServiceSlot::Retiring(record) =
            &mut self.slots[key.position]
        {
            record.wait = None;
        }
        Ok(())
    }
}

impl<T: super::TargetIo> super::OwnerTargetExecutor<T> {
    pub(super) fn wait_bound_completion_capacity(
        &mut self,
        flow_id: super::SessionFlowId,
    ) -> Result<(), super::OwnerTargetError> {
        use super::OwnerTargetError::BoundSourceRequiresSelectedService as Invalid;
        let OwnerTargetServiceState::Bound { flows, binding, .. } = &mut self.service else {
            return Err(Invalid);
        };
        let owner = self.flows.get(&flow_id).ok_or(Invalid)?;
        let position = owner.service_position.ok_or(Invalid)?;
        let Some(OwnerFlowServiceSlot::Live(record) | OwnerFlowServiceSlot::Retiring(record)) =
            flows.slots.get(position)
        else {
            return Err(Invalid);
        };
        if record.association.flow_id() != flow_id
            || owner.pending_completion.is_none()
            || owner.retire_after_join
                != matches!(flows.slots[position], OwnerFlowServiceSlot::Retiring(_))
        {
            return Err(Invalid);
        }
        // The original completion stays owned in both Bound phases. One
        // operation validates the full phase/correlation, suspends reads and
        // links its derived waiter; no Active-only receipt admission is borrowed.
        binding
            .suspend_flow_reads(
                flows.slots.len(),
                position,
                (
                    record.association,
                    record.generation,
                    owner.retire_after_join,
                ),
            )
            .map_err(|_| Invalid)?;
        flows.wait_for(position, OwnerFlowWaitResource::CompletionCapacity)
    }

    pub(in crate::owned_upstream) fn begin_ready_publication(
        &self,
    ) -> Result<
        crate::owned_upstream::owner_ready::OwnerPublicationTurn,
        crate::owned_upstream::owner_ready::OwnerPublicationAdmissionFailure,
    > {
        match &self.service {
            OwnerTargetServiceState::Bound { binding, .. } => binding.begin_publication(),
            OwnerTargetServiceState::Unbound => Err(crate::owned_upstream::owner_ready::OwnerPublicationAdmissionFailure {
                fault: crate::owned_upstream::owner_ready::ReadyFault::Corrupt,
                disposition: crate::owned_upstream::owner_ready::OwnerPublicationAdmissionDisposition::NotEntered,
            }),
        }
    }

    pub(in crate::owned_upstream) fn select_ready_continuation(
        &self,
        eligible: usize,
        requested: usize,
    ) -> Result<
        Option<crate::owned_upstream::owner_ready::OwnerReadyContinuationSelection>,
        OwnerTargetInputBlock,
    > {
        let OwnerTargetServiceState::Bound { binding, .. } = &self.service else {
            return Err(OwnerTargetInputBlock::ReadyReceiptInvariant);
        };
        binding
            .select_continuation(eligible, requested)
            .map_err(|_| OwnerTargetInputBlock::ReadyReceiptInvariant)
    }

    pub(in crate::owned_upstream) fn finish_ready_continuation(
        &mut self,
        selection: &crate::owned_upstream::owner_ready::OwnerReadyContinuationSelection,
        result: crate::owned_upstream::owner_ready::OwnerReadyReadResult,
        eligible: usize,
    ) -> Result<usize, OwnerTargetInputBlock> {
        if matches!(
            result,
            crate::owned_upstream::owner_ready::OwnerReadyReadResult::RetirementReleased
        ) {
            return self.finish_ready_retirement(selection, eligible);
        }
        let OwnerTargetServiceState::Bound { binding, .. } = &self.service else {
            return Err(OwnerTargetInputBlock::ReadyReceiptInvariant);
        };
        binding
            .finish_continuation(selection, result, eligible)
            .map_err(|_| OwnerTargetInputBlock::ReadyReceiptInvariant)
    }

    pub(in crate::owned_upstream) fn service_ready_target(
        &mut self,
        selection: &crate::owned_upstream::owner_ready::OwnerReadyContinuationSelection,
    ) -> Result<crate::owned_upstream::owner_ready::OwnerReadyReadResult, super::OwnerTargetError>
    {
        use super::OwnerTargetError::BoundSourceRequiresSelectedService as Invalid;
        use crate::owned_upstream::owner_ready::{
            OwnerReadyCategory as Category, OwnerReadyReadResult as ResultValue,
        };
        use crate::owned_upstream::target::{TargetReadyInterest, TargetReadyOutcome};
        use std::task::{Context, Poll};
        self.require_no_pending_effects()?;
        if selection.category == Category::TargetCommon {
            let OwnerTargetServiceState::Bound { flows, .. } = &self.service else {
                return Err(Invalid);
            };
            let waker = flows.session.target_common_waker();
            return match self
                .target
                .poll_ready_notifications(&mut Context::from_waker(&waker))
            {
                Poll::Pending => Ok(ResultValue::Pending),
                Poll::Ready(Ok(())) => Ok(ResultValue::Hint),
                Poll::Ready(Err(error)) => Err(super::OwnerTargetError::Target(error)),
            };
        }
        let selected = selection.candidate.as_ref().ok_or(Invalid)?;
        let OwnerTargetServiceState::Bound { flows, binding, .. } = &self.service else {
            return Err(Invalid);
        };
        let Some(OwnerFlowServiceSlot::Live(record) | OwnerFlowServiceSlot::Retiring(record)) =
            flows.slots.get(selected.position)
        else {
            return Err(Invalid);
        };
        let flow_id = selected.association.flow_id();
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
        if selection.category == Category::Retirement {
            return self.service_ready_retirement(selection);
        }
        let source = owner.source.as_ref().ok_or(Invalid)?;
        if selected.association != OwnerReadyFlowAssociation::Source(source.flow.local_flow())
            || source.driver.local_flow() != source.flow.local_flow()
            || !owner.target_present
            || owner.retire_after_join
            || owner.terminal.is_some()
        {
            return Err(Invalid);
        }
        self.require_no_pending_target_completion(flow_id)?;
        match selection.category {
            Category::Credit => Ok(ResultValue::Hint),
            Category::ReadReady => {
                let waker = record
                    .tickets
                    .target_waker(Category::ReadReady)
                    .map_err(|_| Invalid)?;
                match self.target.poll_readiness(
                    flow_id,
                    TargetReadyInterest::Read,
                    &mut Context::from_waker(&waker),
                ) {
                    Poll::Pending => Ok(ResultValue::Pending),
                    Poll::Ready(Ok(TargetReadyOutcome::Attempt)) => Ok(ResultValue::Hint),
                    Poll::Ready(Ok(TargetReadyOutcome::Failed(cause))) => {
                        self.retain_bound_target_failure(
                            flow_id,
                            cause,
                            super::OwnerTargetWork::TargetIoInvariant {
                                flow_id,
                                operation: super::TargetIoOperation::Read,
                            },
                        )
                        .map_err(|(_, error)| error)?;
                        self.stage_retained_failure_reset(flow_id)?;
                        Ok(ResultValue::FailureStaged)
                    }
                    Poll::Ready(Err(error)) => {
                        self.abort_direct_target_io(flow_id, super::TargetIoOperation::Read, error)
                    }
                }
            }
            Category::ReadAttempt => {
                use super::read_source::BoundTargetRead;
                self.read_ready_target(flow_id)
                    .map(|outcome| match outcome {
                        BoundTargetRead::Data => ResultValue::Data,
                        BoundTargetRead::Eof => ResultValue::Eof,
                        BoundTargetRead::WouldBlock => ResultValue::Pending,
                        BoundTargetRead::CreditBlocked => ResultValue::CreditBlocked,
                        BoundTargetRead::FailureStaged => ResultValue::FailureStaged,
                    })
            }
            _ => Err(Invalid),
        }
    }

    pub(super) fn request_bound_driver(
        &self,
        flow_id: super::SessionFlowId,
        lane: crate::owned_upstream::owner_ready::SendLane,
    ) -> Result<(), super::OwnerTargetError> {
        let OwnerTargetServiceState::Bound { flows, .. } = &self.service else {
            return Err(super::OwnerTargetError::BoundSourceRequiresSelectedService);
        };
        let owner = self
            .flows
            .get(&flow_id)
            .ok_or(super::OwnerTargetError::UnknownTargetFlow { flow_id })?;
        let position = owner
            .service_position
            .ok_or(super::OwnerTargetError::BoundSourceRequiresSelectedService)?;
        let Some(OwnerFlowServiceSlot::Live(record) | OwnerFlowServiceSlot::Retiring(record)) =
            flows.slots.get(position)
        else {
            return Err(super::OwnerTargetError::BoundSourceRequiresSelectedService);
        };
        let source = owner
            .source
            .as_ref()
            .ok_or(super::OwnerTargetError::MissingTargetReadSource { flow_id })?;
        if record.association != OwnerReadyFlowAssociation::Source(source.flow.local_flow())
            || source.driver.local_flow() != source.flow.local_flow()
        {
            return Err(super::OwnerTargetError::BoundSourceRequiresSelectedService);
        }
        let waker = record
            .tickets
            .driver_waker(lane)
            .map_err(|_| super::OwnerTargetError::BoundSourceRequiresSelectedService)?;
        waker.wake_by_ref();
        Ok(())
    }

    pub(super) fn suspend_bound_reads(
        &self,
        flow_id: super::SessionFlowId,
    ) -> Result<(), super::OwnerTargetError> {
        use super::OwnerTargetError::BoundSourceRequiresSelectedService as Invalid;
        let OwnerTargetServiceState::Bound { flows, binding, .. } = &self.service else {
            return Ok(());
        };
        let owner = self.flows.get(&flow_id).ok_or(Invalid)?;
        let position = owner.service_position.ok_or(Invalid)?;
        let (record, retiring) = match flows.slots.get(position) {
            Some(OwnerFlowServiceSlot::Live(record)) => (record, false),
            Some(OwnerFlowServiceSlot::Retiring(record)) => (record, true),
            _ => return Err(Invalid),
        };
        if owner.pending_completion.is_none()
            || record.association.flow_id() != flow_id
            || retiring != owner.retire_after_join
        {
            return Err(Invalid);
        }
        binding
            .suspend_flow_reads(
                flows.slots.len(),
                position,
                (record.association, record.generation, retiring),
            )
            .map_err(|_| Invalid)
    }

    pub(super) fn stage_bound_target_failure(
        &mut self,
        flow_id: super::SessionFlowId,
    ) -> Result<(), super::OwnerTargetError> {
        let owner = self
            .flows
            .get_mut(&flow_id)
            .ok_or(super::OwnerTargetError::UnknownTargetFlow { flow_id })?;
        if owner.pending_completion.is_some() {
            return Err(super::OwnerTargetError::DuplicatePendingTargetCompletion { flow_id });
        }
        let source = owner
            .source
            .as_ref()
            .ok_or(super::OwnerTargetError::MissingTargetReadSource { flow_id })?;
        // Legacy intent has no fabricated failed work or cause. Real Target
        // failures enter retain_bound_target_failure before any notification.
        owner.pending_completion = Some(PendingTargetCompletion::StagedReset(
            source.flow.local_flow(),
        ));
        self.stage_retained_failure_reset(flow_id)
    }

    pub(super) fn retain_bound_target_failure(
        &mut self,
        flow_id: super::SessionFlowId,
        cause: super::super::target::TargetFailure,
        work: super::OwnerTargetWork,
    ) -> Result<(), (super::OwnerTargetWork, super::OwnerTargetError)> {
        use super::OwnerTargetError as Error;
        let Some(owner) = self.flows.get_mut(&flow_id) else {
            return Err((work, Error::UnknownTargetFlow { flow_id }));
        };
        if owner.pending_completion.is_some() {
            return Err((work, Error::DuplicatePendingTargetCompletion { flow_id }));
        }
        let Some(source) = &owner.source else {
            return Err((work, Error::MissingTargetReadSource { flow_id }));
        };
        // Sole work move before reservation Drop, source/Target callbacks or wake.
        owner.pending_completion = Some(PendingTargetCompletion::Failed {
            work,
            cause,
            feedback: super::completion::TargetFailureFeedback::Prepared(source.flow.local_flow()),
        });
        Ok(())
    }

    pub(super) fn stage_retained_failure_reset(
        &mut self,
        flow_id: super::SessionFlowId,
    ) -> Result<(), super::OwnerTargetError> {
        use super::completion::TargetFailureFeedback as Feedback;
        self.suspend_bound_reads(flow_id)?;
        if let Some(PendingTargetCompletion::Failed { feedback, .. }) = self
            .flows
            .get_mut(&flow_id)
            .and_then(|owner| owner.pending_completion.as_mut())
        {
            let Feedback::Prepared(local) = feedback else {
                return Err(super::OwnerTargetError::BoundSourceRequiresSelectedService);
            };
            *feedback = Feedback::StagingUnknown(*local);
        }
        self.flows
            .get(&flow_id)
            .and_then(|owner| owner.source.as_ref())
            .ok_or(super::OwnerTargetError::MissingTargetReadSource { flow_id })?
            .flow
            .try_send_reset(super::ResetReason::TargetFailure)?;
        if let Some(PendingTargetCompletion::Failed { feedback, .. }) = self
            .flows
            .get_mut(&flow_id)
            .and_then(|owner| owner.pending_completion.as_mut())
            && let Feedback::StagingUnknown(local) = feedback
        {
            *feedback = Feedback::InTransit(*local);
        }
        self.request_bound_driver(
            flow_id,
            crate::owned_upstream::owner_ready::SendLane::Control,
        )
    }

    pub(in crate::owned_upstream) fn ready_inbox_waker(
        &self,
        lane: SessionMailboxLane,
    ) -> Result<std::task::Waker, OwnerTargetInputBlock> {
        let OwnerTargetServiceState::Bound { flows, .. } = &self.service else {
            return Err(OwnerTargetInputBlock::ReadyReceiptInvariant);
        };
        Ok(flows.session.inbox_waker(lane))
    }

    pub(in crate::owned_upstream) fn preflight_ready_receipt(
        &self,
        command: &SessionOwnerCommand,
    ) -> Result<(), OwnerTargetInputBlock> {
        self.ready_receipt_position(command).map(drop)
    }

    fn ready_receipt_position(
        &self,
        command: &SessionOwnerCommand,
    ) -> Result<ReadyReceiptPosition, OwnerTargetInputBlock> {
        use OwnerTargetInputBlock::ReadyReceiptInvariant as Invalid;
        if let Some(block) = self.input_admission_block(None) {
            return Err(block);
        }
        let flow_id = super::command_flow_id(command).ok_or(Invalid)?;
        let owner = self.flows.get(&flow_id).ok_or(Invalid)?;
        let position = owner.service_position.ok_or(Invalid)?;
        let OwnerTargetServiceState::Bound { flows, binding, .. } = &self.service else {
            return Err(Invalid);
        };
        let record = match flows.slots.get(position) {
            Some(OwnerFlowServiceSlot::Live(record) | OwnerFlowServiceSlot::Retiring(record)) => {
                record
            }
            _ => return Err(Invalid),
        };
        if record.transit == 0
            || record.association.flow_id() != flow_id
            || owner.retire_after_join
                != matches!(flows.slots[position], OwnerFlowServiceSlot::Retiring(_))
        {
            return Err(Invalid);
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
            .map_err(|_| Invalid)?;
        // Driver commands name the complete LocalFlow, not just a numeric ID.
        // DATA's unforgeable permit origin is additionally checked by the
        // original Supervisor gate before model reduction, with actual custody.
        let local = match command {
            SessionOwnerCommand::Driver(DriverInput::Data(data)) => Some(data.flow()),
            SessionOwnerCommand::Driver(DriverInput::Control(
                SessionEvent::LocalClose { flow } | SessionEvent::LocalReset { flow, .. },
            )) => Some(*flow),
            SessionOwnerCommand::Event(SessionEvent::PeerOpenResolved { .. }) => {
                if !owner.pending_completion.as_ref().is_some_and(|pending| {
                    matches!(
                        pending.classify_receipt(command),
                        Some(CompletionReceipt::Feedback)
                    )
                }) {
                    return Err(Invalid);
                }
                None
            }
            SessionOwnerCommand::Event(event @ SessionEvent::SinkHalfClosed { .. }) => {
                if !matches!(&owner.pending_completion,
                    Some(PendingTargetCompletion::HalfClose { event: expected }) if expected == event)
                {
                    return Err(Invalid);
                }
                None
            }
            _ => return Err(Invalid),
        };
        if let Some(local) = local {
            let source = owner.source.as_ref().ok_or(Invalid)?;
            if source.flow.local_flow() != local
                || source.driver.local_flow() != local
                || record.association != OwnerReadyFlowAssociation::Source(local)
            {
                return Err(Invalid);
            }
        }
        let disposition = match &owner.pending_completion {
            Some(pending) => pending
                .classify_receipt(command)
                .ok_or(OwnerTargetInputBlock::PendingTargetCompletion { flow_id })?,
            None => CompletionReceipt::Independent,
        };
        Ok(ReadyReceiptPosition {
            position,
            flow_id,
            disposition,
        })
    }

    /// The exact command is consumed only here, under C's entered ordinary
    /// guard. Expected completion pressure is retained at its original Flow;
    /// all other rejected commands remain owned by the returned typed error.
    pub(in crate::owned_upstream) fn apply_ready_receipt_observed(
        &mut self,
        command: SessionOwnerCommand,
    ) -> Result<super::OwnerTargetObservedApply, super::OwnerTargetError> {
        let ReadyReceiptPosition {
            position,
            flow_id,
            disposition,
        } = match self.ready_receipt_position(&command) {
            Ok(position) => position,
            Err(block) => {
                return Err(super::OwnerTargetError::RejectedCommand {
                    block,
                    command: Box::new(command),
                });
            }
        };
        if let Err(source) = self.prepare_effect_entry(3) {
            return Err(super::OwnerTargetError::RejectedCommand {
                block: OwnerTargetInputBlock::EffectStorage(source),
                command: Box::new(command),
            });
        }
        if matches!(disposition, CompletionReceipt::TerminalDisposition) {
            let disposed = self.settle_terminal_receipt(flow_id, command)?;
            if let OwnerTargetServiceState::Bound { flows, .. } = &mut self.service {
                if let OwnerFlowServiceSlot::Live(record) | OwnerFlowServiceSlot::Retiring(record) =
                    &mut flows.slots[position]
                {
                    record.transit -= 1;
                }
            }
            self.request_bound_driver(
                flow_id,
                crate::owned_upstream::owner_ready::SendLane::Control,
            )?;
            return Ok(disposed);
        }
        if matches!(disposition, CompletionReceipt::Feedback) {
            self.mark_feedback_reducing(flow_id);
        }
        let reduced = self.reduce_owner_command_observed(command);
        match reduced {
            Ok((effects, observations)) => {
                self.begin_observed_effects(effects, observations)?;
                // Only observed success settles this Driver receipt. A carrying
                // rejection or unknown outcome still owns its original charge.
                if let OwnerTargetServiceState::Bound { flows, .. } = &mut self.service {
                    if let OwnerFlowServiceSlot::Live(record)
                    | OwnerFlowServiceSlot::Retiring(record) = &mut flows.slots[position]
                    {
                        record.transit -= 1;
                    }
                }
                self.record_feedback_reduced(flow_id);
                let terminal_pending = self.aborted_effects.as_ref().is_some_and(|page| {
                    page.effects.iter().any(|work| {
                        matches!(work,
                        super::OwnerTargetWork::Effect(super::SessionEffect::FlowFinished {
                            flow_id: finished, ..
                        }) if *finished == flow_id)
                    })
                });
                if matches!(disposition, CompletionReceipt::Feedback)
                    && !terminal_pending
                    && let Some(owner) = self.flows.get_mut(&flow_id)
                    && !owner
                        .pending_completion
                        .as_ref()
                        .is_some_and(PendingTargetCompletion::is_open_finish_pending)
                    && !matches!(
                        owner.pending_completion,
                        Some(PendingTargetCompletion::Failed { .. })
                    )
                {
                    owner.pending_completion = None;
                }
                self.run_entered_effects()?;
                self.finish_observed_apply()
            }
            Err(SessionSupervisorError::RejectedCommand { block, command }) => {
                let pressure = matches!(
                    block,
                    ReducerAdmissionBlock::TerminalTombstoneCapacityExceeded { .. }
                );
                // Classify by borrowing before destructuring. Unexpected
                // payloads retain the original Box, never a reconstructed event.
                let completion = match command.as_ref() {
                    SessionOwnerCommand::Event(SessionEvent::PeerOpenResolved { .. }) => true,
                    SessionOwnerCommand::Event(SessionEvent::SinkHalfClosed { .. }) => true,
                    SessionOwnerCommand::Driver(DriverInput::Control(
                        SessionEvent::LocalReset { .. } | SessionEvent::LocalClose { .. },
                    )) => true,
                    _ => false,
                };
                if pressure && completion && matches!(disposition, CompletionReceipt::Feedback) {
                    if matches!(command.as_ref(), SessionOwnerCommand::Driver(_)) {
                        self.retain_driver_rejection(flow_id, command)?;
                        self.wait_bound_completion_capacity(flow_id)?;
                        return Ok(super::OwnerTargetObservedApply {
                            outputs: Vec::new(),
                            observations: super::OwnerTargetReducerObservations::default(),
                        });
                    }
                    if matches!(
                        command.as_ref(),
                        SessionOwnerCommand::Event(SessionEvent::PeerOpenResolved { .. })
                    ) {
                        self.retain_rejected_completion(
                            super::PendingTargetCompletionKind::Open,
                            flow_id,
                            command,
                        )?;
                        self.wait_bound_completion_capacity(flow_id)?;
                        return Ok(super::OwnerTargetObservedApply {
                            outputs: Vec::new(),
                            observations: super::OwnerTargetReducerObservations::default(),
                        });
                    }
                    let Some(owner) = self.flows.get_mut(&flow_id) else {
                        return Err(super::OwnerTargetError::RejectedCommand {
                            block: OwnerTargetInputBlock::ReadyReceiptInvariant,
                            command,
                        });
                    };
                    let retained = match *command {
                        SessionOwnerCommand::Event(event @ SessionEvent::SinkHalfClosed { .. }) => {
                            PendingTargetCompletion::HalfClose { event }
                        }
                        command => {
                            return Err(super::OwnerTargetError::RejectedCommand {
                                block: OwnerTargetInputBlock::ReducerAdmission(block),
                                command: Box::new(command),
                            });
                        }
                    };
                    // The same synchronous reducer cannot remove this record
                    // on a rejected command; no Target effect ran.
                    owner.pending_completion = Some(retained);
                    self.wait_bound_completion_capacity(flow_id)?;
                    return Ok(super::OwnerTargetObservedApply {
                        outputs: Vec::new(),
                        observations: super::OwnerTargetReducerObservations::default(),
                    });
                }
                Err(super::OwnerTargetError::RejectedCommand {
                    block: OwnerTargetInputBlock::ReducerAdmission(block),
                    command,
                })
            }
            Err(error) => Err(super::OwnerTargetError::Supervisor(error)),
        }
    }

    pub(in crate::owned_upstream) fn activate_owner_ready(
        &mut self,
        ready: &mut crate::owned_upstream::owner_ready::ReadyIndex,
        position: usize,
        handoff: &super::OwnerTargetInitialHandoff,
        runtime: &super::PhysicalLegRuntimeIncarnation,
        io: &super::LegIo,
        queue: &super::LegOutboundQueue,
    ) -> Result<
        crate::owned_upstream::owner_ready::OwnerReadyActivatedSession,
        super::OwnerReadyInstallError,
    > {
        use super::{OwnerReadyInstallError as Error, OwnerTargetInputBlock as Block};
        if let Some(block) = self.recovery_owner_block(&handoff.owner) {
            return Err(Error::Handoff(block));
        }
        let physical = handoff
            .physical_actor
            .as_ref()
            .ok_or(Error::Handoff(Block::InitialPhysicalActorNotBound))?;
        if !physical.matches(runtime, io, queue)
            || !handoff.matches_transport_identity(io.established_leg())
        {
            return Err(Error::Handoff(Block::WrongInitialPhysicalActor));
        }
        if handoff.acceptance.is_some() {
            return Err(Error::Handoff(Block::InitialAcceptanceSettlementPending));
        }
        if let Some(block) = self.input_admission_block(None) {
            return Err(Error::Handoff(block));
        }
        let OwnerTargetServiceState::Bound { flows, binding, .. } = &mut self.service else {
            return Err(Error::NotBound);
        };
        self.supervisor
            .validate_owner_ready_activation(
                &super::OwnerTargetColdPreparationGate(()),
                binding,
                &self.source_factory,
                &handoff.attached,
            )
            .map_err(Error::Source)?;
        // Both directions are checked against the actual BTreeMap and fixed
        // positions, including Free slots. No Flow is reconstructed or cloned.
        for (&flow_id, owner) in &self.flows {
            let Some(position) = owner.service_position else {
                return Err(Error::FlowState(flow_id));
            };
            let Some(OwnerFlowServiceSlot::Live(record) | OwnerFlowServiceSlot::Retiring(record)) =
                flows.slots.get(position)
            else {
                return Err(Error::FlowState(flow_id));
            };
            if record.association.flow_id() != flow_id {
                return Err(Error::FlowState(flow_id));
            }
        }
        for (position, slot) in flows.slots.iter().enumerate() {
            let expected = match slot {
                OwnerFlowServiceSlot::Free => None,
                OwnerFlowServiceSlot::Live(record) | OwnerFlowServiceSlot::Retiring(record) => {
                    let flow_id = record.association.flow_id();
                    let owner = self.flows.get(&flow_id).ok_or(Error::FlowState(flow_id))?;
                    if owner.service_position != Some(position)
                        || owner.retire_after_join
                            != matches!(slot, OwnerFlowServiceSlot::Retiring(_))
                        || record.transit != 0
                    {
                        return Err(Error::FlowState(flow_id));
                    }
                    match (&owner.source, record.association) {
                        (Some(source), OwnerReadyFlowAssociation::Source(local))
                            if source.flow.local_flow() == local
                                && source.driver.local_flow() == local => {}
                        (None, OwnerReadyFlowAssociation::NoSource { phase, .. }) => {
                            use crate::owned_upstream::owner_ready::OwnerReadyNoSourcePhase as Phase;
                            let actual = if owner
                                .pending_completion
                                .as_ref()
                                .is_some_and(PendingTargetCompletion::is_open_pending)
                            {
                                Some(Phase::OpenCompletion)
                            } else if owner.terminal.is_some() {
                                Some(Phase::TargetTerminal)
                            } else if owner.pending_completion.is_some() {
                                Some(Phase::RetainedCompletion)
                            } else if owner.join_pending || owner.retire_after_join {
                                Some(Phase::Retirement)
                            } else {
                                // Pending effect turns were rejected above;
                                // an old finish tag is not current ownership.
                                None
                            };
                            if actual != Some(phase)
                                && !owner.terminal_open_preserves_no_source(flow_id, phase)
                            {
                                return Err(Error::FlowState(flow_id));
                            }
                        }
                        _ => return Err(Error::FlowState(flow_id)),
                    }
                    Some((
                        record.association,
                        record.generation,
                        owner.retire_after_join,
                    ))
                }
            };
            binding
                .compare_flow_position(flows.slots.len(), position, expected)
                .map_err(Error::Reservation)?;
        }
        // C/output and the actual Active handoff were preflighted by the
        // caller in this same synchronous operation. All five sources are
        // eligible here; subsequent selected turns recompute their own gates.
        binding
            .activate(ready, position, &flows.session, 0b1_1111)
            .map_err(Error::Reservation)
    }
}
