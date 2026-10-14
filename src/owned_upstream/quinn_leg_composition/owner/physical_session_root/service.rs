//! Same-owner activation and selected physical service.
//!
//! Keeps actor, settled handoff, Controller/output and ready publication in one
//! physical transaction. It exports no Actor, FIFO, inbox or Controller. Root
//! retains the complete resident and scheduler claim. Activation failure leaves
//! this whole root available to its caller, with original failure state intact.
//! Entry: activate_owner_ready. Real tests are in tests/current_only/bound_source.rs.

use super::super::QuinnOwnerOutputServiceTurn;
use super::super::continuation::QuinnOwnerOutputContinuationDrive;
use super::*;
use crate::owned_upstream::owner_output_drain::{
    OwnerOutputDrainAuditError, OwnerOutputDrainBlock, OwnerOutputDrainDrive,
};
use crate::owned_upstream::owner_ready::{
    OwnerPublicationTurn, OwnerReadyActivatedSession, OwnerReadyCategory as Category,
    OwnerReadyReadResult, OwnerReadySource, OwnerReadyUnit, ReadyFault, ReadyIndex, SendLane,
    SessionCommandVisit,
};
use crate::owned_upstream::quinn_leg_actor::QuinnLegActorFlushDisposition;
use crate::owned_upstream::session::SessionMailboxLane;
use std::task::{Context, Poll};

// This is only the notification interval, never a second actor/receipt owner.
pub(super) enum OwnerPhysicalPublication {
    Idle,
    Entered(OwnerPublicationTurn),
    Finishing,
    Quarantined,
}

#[derive(Clone, Copy)]
pub(super) enum OwnerPhysicalFlush {
    Idle,
    Due,
    Registered,
}

pub(super) enum OwnerPhysicalServiceFailure {
    Actor(QuinnLegActorError),
    Audit(OwnerOutputDrainAuditError),
    Queue(OwnerOutputDrainBlock),
    Controller,
    Invariant,
}

const CONTINUATION: usize = 1 << 4;

impl<T: TargetIo> OwnerPhysicalSessionRoot<T> {
    pub(in crate::owned_upstream::quinn_leg_composition::owner) fn activate_owner_ready(
        &mut self,
        ready: &mut ReadyIndex,
        position: usize,
    ) -> Result<(crate::resumable::SessionId, std::task::Waker), OwnerPhysicalReadyActivationFailure>
    {
        use OwnerPhysicalReadyActivationCause as Cause;
        let Self::CurrentOnly(current) = self;
        let fail = |cause| OwnerPhysicalReadyActivationFailure { cause };
        if !matches!(current.ready, OwnerPhysicalReadyState::Unactivated) {
            return Err(fail(Cause::AlreadyActive));
        }
        if !matches!(current.actor.phase, OwnerPhysicalCurrentActorPhase::Running) {
            return Err(fail(Cause::Physical(current.actor.phase.observed_error())));
        }
        current
            .inventory()
            .map_err(|cause| fail(Cause::Physical(cause)))?;
        let gate = QuinnLegCompositionGate::mint();
        let (actor, output, handoff) = (
            &mut current.actor.actor,
            &mut current.output,
            &current.handoff,
        );
        let activated =
            actor.with_physical_controller_parts_mut_input(&gate, (), |(), runtime, io, queue| {
                output.activate_owner_ready(ready, position, handoff, runtime, io, queue)
            });
        let OwnerReadyActivatedSession {
            session_id,
            capacity,
            output,
        } = match activated {
            Ok(Ok(activated)) => activated,
            Ok(Err(cause)) => return Err(fail(Cause::Controller(cause))),
            Err(((), cause)) => {
                return Err(fail(Cause::Physical(OwnerPhysicalSessionRootError::Actor(
                    cause,
                ))));
            }
        };
        // Inward activation was the final fallible edge. Publish only owning
        // values, without registering/polling a dependency or allocating here.
        current.ready = OwnerPhysicalReadyState::Active {
            output,
            publication: OwnerPhysicalPublication::Idle,
            flush: OwnerPhysicalFlush::Idle,
            failure: None,
        };
        Ok((session_id, capacity))
    }

    pub(in crate::owned_upstream::quinn_leg_composition::owner) fn visit_ready_source(
        &mut self,
        source: OwnerReadySource,
        _cx: &mut Context<'_>,
    ) -> Result<SessionCommandVisit, ()> {
        let Self::CurrentOnly(current) = self;
        let OwnerPhysicalReadyState::Active {
            publication,
            failure,
            ..
        } = &mut current.ready
        else {
            return Err(());
        };
        if failure.is_some() || !matches!(publication, OwnerPhysicalPublication::Idle) {
            return Err(());
        }
        let turn = current.output.begin_ready_publication().map_err(|_| ())?;
        *publication = OwnerPhysicalPublication::Entered(turn);
        let visited = current.visit_selected(source);
        match visited {
            Ok(completed) => Ok(completed),
            Err(error) => {
                // Preserve the actual outer error before returning a value-only
                // disposition. Inner C/output and actor ledgers remain owned.
                if let OwnerPhysicalReadyState::Active { failure, .. } = &mut current.ready {
                    *failure = Some(error);
                }
                Err(())
            }
        }
    }

    pub(in crate::owned_upstream::quinn_leg_composition::owner) fn finish_ready_source(
        &mut self,
        completed: &SessionCommandVisit,
    ) -> Result<(), ReadyFault> {
        let Self::CurrentOnly(current) = self;
        let OwnerPhysicalReadyState::Active { publication, .. } = &mut current.ready else {
            return Err(ReadyFault::Corrupt);
        };
        let OwnerPhysicalPublication::Entered(_) = publication else {
            return Err(ReadyFault::Corrupt);
        };
        let phase = std::mem::replace(publication, OwnerPhysicalPublication::Finishing);
        if let OwnerPhysicalPublication::Entered(turn) = phase {
            turn.finish(completed).map_err(|failure| failure.fault)?;
            *publication = OwnerPhysicalPublication::Idle;
            Ok(())
        } else {
            Err(ReadyFault::Corrupt)
        }
    }

    pub(in crate::owned_upstream::quinn_leg_composition::owner) fn quarantine_ready_source(
        &mut self,
    ) -> Result<(), ReadyFault> {
        let Self::CurrentOnly(current) = self;
        current.output.quarantine_ready_service();
        let OwnerPhysicalReadyState::Active { publication, .. } = &mut current.ready else {
            return Err(ReadyFault::Corrupt);
        };
        // Root already quarantined the same indexed resident. Concrete C/output
        // custody above precedes interval close; this is not a completed visit.
        let phase = std::mem::replace(publication, OwnerPhysicalPublication::Quarantined);
        match phase {
            OwnerPhysicalPublication::Entered(turn) => turn.finish_quarantined(),
            OwnerPhysicalPublication::Idle | OwnerPhysicalPublication::Quarantined => Ok(()),
            OwnerPhysicalPublication::Finishing => Err(ReadyFault::Corrupt),
        }
    }
}

impl<T: TargetIo> OwnerPhysicalCurrentOnly<T> {
    fn visit_selected(
        &mut self,
        source: OwnerReadySource,
    ) -> Result<SessionCommandVisit, OwnerPhysicalServiceFailure> {
        use OwnerPhysicalServiceFailure as Failure;
        if !matches!(self.actor.phase, OwnerPhysicalCurrentActorPhase::Running) {
            return Err(Failure::Invariant);
        }
        let OwnerPhysicalReadyState::Active {
            output: output_waker,
            flush,
            ..
        } = &mut self.ready
        else {
            return Err(Failure::Invariant);
        };
        let gate = QuinnLegCompositionGate::mint();
        let plan = self
            .output
            .ready_service_plan(
                !matches!(flush, OwnerPhysicalFlush::Registered),
                &self.terminal_timer,
            )
            .map_err(Failure::Actor)?;
        if source == OwnerReadySource::OwnerContinuation {
            let selected = self
                .output
                .select_ready_continuation(
                    plan.eligible,
                    plan.requested
                        | if matches!(flush, OwnerPhysicalFlush::Due) {
                            Category::Output.mask()
                        } else {
                            0
                        },
                )
                .map_err(Failure::Actor)?;
            let Some(selected) = selected else {
                return Ok(SessionCommandVisit {
                    work: OwnerReadyUnit::Waiting,
                    runnable: 0,
                    blocked: None,
                });
            };
            // Frame/flush, installation and the unique timer share Output's
            // bounded fair turn; no sub-operation can consume a second unit.
            let candidates = [
                plan.output_frame || !matches!(flush, OwnerPhysicalFlush::Idle),
                plan.terminal_install,
                plan.terminal_timer,
            ];
            let output_unit = if selected.category == Category::Output {
                (0..3)
                    .map(|offset| (self.output_cursor + offset) % 3)
                    .find(|index| candidates[*index])
            } else {
                None
            };
            if let Some(index) = output_unit {
                self.output_cursor = (index + 1) % 3;
            }
            let (work, result) = if output_unit == Some(1) {
                self.output
                    .install_terminal_action()
                    .map_err(Failure::Actor)?;
                (OwnerReadyUnit::SourceRead, OwnerReadyReadResult::Pending)
            } else if output_unit == Some(2) {
                let result = self
                    .output
                    .begin_inbound_admission()
                    .ok_or(Failure::Controller)?
                    .expire_terminal_and_admit(
                        &mut self.terminal_timer,
                        &mut Context::from_waker(output_waker),
                    )
                    .map_err(|()| Failure::Controller)?;
                (OwnerReadyUnit::SourceRead, result)
            } else if selected.category == Category::Output
                && !matches!(flush, OwnerPhysicalFlush::Idle)
            {
                let now = self
                    .actor
                    .actor
                    .session_clock(&gate)
                    .now()
                    .map_err(|error| Failure::Actor(error.into()))?;
                let mut context = Context::from_waker(output_waker);
                match self
                    .actor
                    .actor
                    .poll_owner_output_flush(&gate, now, &mut context)
                {
                    Poll::Pending => {
                        *flush = OwnerPhysicalFlush::Registered;
                        (OwnerReadyUnit::Registered, OwnerReadyReadResult::Pending)
                    }
                    Poll::Ready(Err(error)) => return Err(Failure::Actor(error)),
                    Poll::Ready(Ok(QuinnLegActorFlushDisposition::Submitted)) => {
                        *flush = OwnerPhysicalFlush::Due;
                        (OwnerReadyUnit::SourceRead, OwnerReadyReadResult::Hint)
                    }
                    Poll::Ready(Ok(QuinnLegActorFlushDisposition::Idle)) => {
                        *flush = OwnerPhysicalFlush::Idle;
                        (OwnerReadyUnit::Waiting, OwnerReadyReadResult::Pending)
                    }
                    Poll::Ready(Ok(QuinnLegActorFlushDisposition::OrderedDeliveryPending)) => {
                        return Err(Failure::Invariant);
                    }
                }
            } else {
                let output = &mut self.output;
                let operated = self.actor.actor.with_physical_controller_parts_mut_input(
                    &gate,
                    (),
                    |(), _runtime, _io, queue| {
                        if matches!(selected.category, Category::Output | Category::Effects) {
                            match output.begin_service_turn() {
                                QuinnOwnerOutputServiceTurn::Inert
                                | QuinnOwnerOutputServiceTurn::CompletedBoundary => {
                                    Ok((OwnerReadyUnit::Waiting, OwnerReadyReadResult::Pending))
                                }
                                QuinnOwnerOutputServiceTurn::Drive(turn) => match turn
                                    .drive(queue)
                                    .map_err(Failure::Audit)?
                                {
                                    OwnerOutputDrainDrive::Transmitted { .. } => {
                                        *flush = OwnerPhysicalFlush::Due;
                                        Ok((
                                            OwnerReadyUnit::OutputAdmitted,
                                            OwnerReadyReadResult::Pending,
                                        ))
                                    }
                                    OwnerOutputDrainDrive::Blocked { reason }
                                        if reason.is_retryable() =>
                                    {
                                        *flush = OwnerPhysicalFlush::Due;
                                        Ok((OwnerReadyUnit::Waiting, OwnerReadyReadResult::Pending))
                                    }
                                    OwnerOutputDrainDrive::Blocked { reason } => {
                                        Err(Failure::Queue(reason))
                                    }
                                    OwnerOutputDrainDrive::LifecycleInstalled { .. }
                                    | OwnerOutputDrainDrive::LifecycleInstallBlocked
                                    | OwnerOutputDrainDrive::PageDrained(_) => {
                                        Ok((OwnerReadyUnit::Waiting, OwnerReadyReadResult::Pending))
                                    }
                                    OwnerOutputDrainDrive::Idle => Err(Failure::Invariant),
                                },
                                QuinnOwnerOutputServiceTurn::Continue(turn) => {
                                    match turn.continue_and_settle() {
                                        QuinnOwnerOutputContinuationDrive::Admitted { .. } => Ok((
                                            OwnerReadyUnit::SourceRead,
                                            OwnerReadyReadResult::Pending,
                                        )),
                                        _ => Err(Failure::Controller),
                                    }
                                }
                                QuinnOwnerOutputServiceTurn::LifecycleInstallBlocked => {
                                    Ok((OwnerReadyUnit::Waiting, OwnerReadyReadResult::Pending))
                                }
                                _ => Err(Failure::Controller),
                            }
                        } else if selected.category == Category::Completion {
                            output
                                .begin_inbound_admission()
                                .ok_or(Failure::Controller)?
                                .complete_selected_and_admit(&selected)
                                .map(|(admitted, result)| {
                                    let work = if admitted.outputs != 0 || admitted.needs_continue {
                                        OwnerReadyUnit::SourceRead
                                    } else {
                                        OwnerReadyUnit::Waiting
                                    };
                                    (work, result)
                                })
                                .map_err(|()| Failure::Controller)
                        } else {
                            output
                                .service_ready_target(&selected)
                                .map(|result| {
                                    let work = match &result {
                                        OwnerReadyReadResult::Data
                                        | OwnerReadyReadResult::Eof
                                        | OwnerReadyReadResult::FailureStaged
                                        | OwnerReadyReadResult::CommandHeld(_)
                                        | OwnerReadyReadResult::RetirementReleased => {
                                            OwnerReadyUnit::SourceRead
                                        }
                                        OwnerReadyReadResult::Pending => OwnerReadyUnit::Registered,
                                        OwnerReadyReadResult::Hint
                                        | OwnerReadyReadResult::CreditBlocked => {
                                            OwnerReadyUnit::Waiting
                                        }
                                    };
                                    (work, result)
                                })
                                .map_err(Failure::Actor)
                        }
                    },
                );
                operated.map_err(|((), error)| Failure::Actor(error))??
            };
            let after = self
                .output
                .ready_service_plan(
                    !matches!(flush, OwnerPhysicalFlush::Registered),
                    &self.terminal_timer,
                )
                .map_err(Failure::Actor)?;
            let mut runnable = self
                .output
                .finish_ready_continuation(&selected, result, after.eligible)
                .map_err(Failure::Actor)?;
            if after.requested != 0 || matches!(flush, OwnerPhysicalFlush::Due) {
                runnable |= CONTINUATION;
            }
            if after.sources {
                runnable |= 0b1111;
            }
            return Ok(SessionCommandVisit {
                work,
                runnable,
                blocked: None,
            });
        }
        if !plan.sources {
            return Ok(SessionCommandVisit {
                work: OwnerReadyUnit::Waiting,
                runnable: if plan.requested != 0 { CONTINUATION } else { 0 },
                blocked: None,
            });
        }
        let output = &mut self.output;
        let operated = self.actor.actor.with_physical_controller_parts_mut_input(
            &gate,
            source,
            |source, _runtime, _io, _queue| match source {
                OwnerReadySource::ControlSend | OwnerReadySource::OrderedSend => {
                    let lane = if source == OwnerReadySource::ControlSend {
                        SendLane::Control
                    } else {
                        SendLane::OrderedSource
                    };
                    output.service_ready_send(lane).map_err(Failure::Actor)
                }
                OwnerReadySource::ControlInbox | OwnerReadySource::OrderedInbox => {
                    let lane = if source == OwnerReadySource::ControlInbox {
                        SessionMailboxLane::Control
                    } else {
                        SessionMailboxLane::OrderedSource
                    };
                    let turn = output
                        .begin_inbound_admission()
                        .ok_or(Failure::Controller)?;
                    match turn
                        .receive_selected_and_admit(lane)
                        .map_err(|()| Failure::Controller)?
                    {
                        Poll::Pending => Ok(SessionCommandVisit {
                            work: OwnerReadyUnit::Registered,
                            runnable: 0,
                            blocked: None,
                        }),
                        Poll::Ready(None) => Ok(SessionCommandVisit {
                            work: OwnerReadyUnit::Waiting,
                            runnable: 0,
                            blocked: None,
                        }),
                        Poll::Ready(Some(_)) => {
                            output_waker.wake_by_ref();
                            Ok(SessionCommandVisit {
                                work: OwnerReadyUnit::SourceRead,
                                runnable: CONTINUATION,
                                blocked: None,
                            })
                        }
                    }
                }
                OwnerReadySource::OwnerContinuation => Err(Failure::Invariant),
            },
        );
        operated.map_err(|(_, error)| Failure::Actor(error))?
    }
}
