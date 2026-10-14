//! One owner's bounded command custody, mailbox transfer and capacity rearm.
//!
//! Owns ingress, its sole observer and both exact send positions as one
//! transaction. It owns neither Root budgets/claims nor a second Driver.
//! Unknown send disposition remains nonzero; mailbox transfer is not settlement.
//! The actual Executor installs this owner with its freshly paired inbox.
//! Private tests retain the original command characterizations; the cfg(test)
//! Prototype admits real OPEN completions for the outer Root characterizations.

use crate::owned_upstream::owner_ready::{
    OwnerCapacityRearm, OwnerReadyDrive, OwnerReadyUnit, SendCursor, SendLane, SessionCommandVisit,
};
use crate::owned_upstream::session::{
    SessionMailboxCapacityChange, SessionMailboxCapacityObservation, SessionMailboxConfigError,
    SessionMailboxObserverBusy, SessionMailboxTrySendError, SessionOwnerCommand, SessionOwnerInbox,
    SessionOwnerIngress, SessionOwnerMailbox,
};
use crate::owned_upstream::tcp::ResumableTcpDriver;
use crate::resumable::{LocalFlow, OpenResultCode, PeerOpenRequest, SessionEvent};
use std::task::{Context, Poll};

pub(super) struct SessionCommands {
    ingress: SessionOwnerIngress,
    capacity: SessionMailboxCapacityObservation,
    cells: [SendCell; 2],
    selector: SendCursor,
    quarantined: bool,
}

// No Drop: success can move these fields after the final metadata commit.
// On preparation rejection the command owner (registration before lease)
// drops before its matching inbox. This graph never accepts separate endpoints.
pub(super) struct PreparedSessionMailbox {
    pub(super) commands: SessionCommands,
    pub(super) inbox: SessionOwnerInbox,
}

#[derive(Debug, PartialEq, Eq)]
pub(in crate::owned_upstream) enum OwnerMailboxPreparationError {
    Configuration(SessionMailboxConfigError),
    ObserverBusy(SessionMailboxObserverBusy),
}

impl Clone for OwnerMailboxPreparationError {
    fn clone(&self) -> Self {
        match self {
            Self::Configuration(cause) => Self::Configuration(match cause {
                SessionMailboxConfigError::ZeroControlCapacity => {
                    SessionMailboxConfigError::ZeroControlCapacity
                }
                SessionMailboxConfigError::ZeroDataCapacity => {
                    SessionMailboxConfigError::ZeroDataCapacity
                }
                SessionMailboxConfigError::ControlCapacityTooLarge { requested, max } => {
                    SessionMailboxConfigError::ControlCapacityTooLarge {
                        requested: *requested,
                        max: *max,
                    }
                }
                SessionMailboxConfigError::DataCapacityTooLarge { requested, max } => {
                    SessionMailboxConfigError::DataCapacityTooLarge {
                        requested: *requested,
                        max: *max,
                    }
                }
            }),
            Self::ObserverBusy(_) => Self::ObserverBusy(SessionMailboxObserverBusy),
        }
    }
}

#[cfg(test)]
#[derive(Debug)]
pub(in crate::owned_upstream) struct SessionCommandsBuildFailure {
    ingress: SessionOwnerIngress,
    request: PeerOpenRequest,
    result: OpenResultCode,
    cause: SessionMailboxObserverBusy,
}

// Copyable correlation is never a substitute for the command or its permits.
#[derive(Clone, Copy)]
enum CommandOrigin {
    OpenCompletion(PeerOpenRequest),
    Driver(SendLane, LocalFlow),
}

enum CommandStorage {
    Fresh(SessionOwnerCommand),
    Returned(Box<SessionOwnerCommand>),
}

struct HeldCommand {
    origin: CommandOrigin,
    storage: CommandStorage,
}

#[derive(PartialEq, Eq)]
enum SendEligibility {
    Ready,
    CapacityBlocked,
}

enum SendCell {
    // Only this root position is empty; a transferred command may still live
    // in the mailbox or another actual owner.
    Vacant,
    Held {
        packet: HeldCommand,
        eligibility: SendEligibility,
    },
    // The send may already have enqueued. No payload is fabricated here.
    TransferringUnknown {
        origin: CommandOrigin,
    },
    ClosedReturned(HeldCommand),
}

#[derive(Debug, PartialEq, Eq)]
enum OwnerLaneAdmission {
    Admitted,
    Occupied,
    Drained,
    Quarantined,
    TransitExhausted,
}

impl CommandStorage {
    fn try_send(
        self,
        ingress: &SessionOwnerIngress,
    ) -> Result<Result<(), SessionMailboxTrySendError>, Self> {
        match self {
            Self::Fresh(SessionOwnerCommand::Event(event)) => Ok(ingress.try_send_event(event)),
            Self::Fresh(SessionOwnerCommand::Driver(input)) => Ok(ingress.try_send_driver(input)),
            Self::Returned(command) => match *command {
                SessionOwnerCommand::Event(event) => Ok(ingress.try_send_event(event)),
                SessionOwnerCommand::Driver(input) => Ok(ingress.try_send_driver(input)),
                _ => Err(Self::Returned(command)),
            },
            storage => Err(storage),
        }
    }
}

impl HeldCommand {
    fn is_valid_for(&self, lane: SendLane) -> bool {
        let command = match &self.storage {
            CommandStorage::Fresh(command) => command,
            CommandStorage::Returned(command) => command.as_ref(),
        };
        let origin_matches = match self.origin {
            CommandOrigin::OpenCompletion(expected) => matches!(
                command,
                SessionOwnerCommand::Event(SessionEvent::PeerOpenResolved { request, .. })
                    if *request == expected
            ),
            CommandOrigin::Driver(expected_lane, expected_flow) => {
                expected_lane == lane
                    && match command {
                        SessionOwnerCommand::Driver(
                            crate::owned_upstream::tcp::DriverInput::Data(data),
                        ) => data.flow() == expected_flow,
                        SessionOwnerCommand::Driver(
                            crate::owned_upstream::tcp::DriverInput::Control(event),
                        ) => match event {
                            SessionEvent::LocalClose { flow }
                            | SessionEvent::LocalReset { flow, .. } => *flow == expected_flow,
                            SessionEvent::SinkAccepted { offer, .. }
                            | SessionEvent::SinkAbandoned { offer } => {
                                offer.session_id() == expected_flow.session_id()
                                    && offer.flow_id() == expected_flow.flow_id()
                            }
                            SessionEvent::SinkHalfClosed { completion }
                            | SessionEvent::SinkHalfCloseFailed { completion } => {
                                completion.session_id() == expected_flow.session_id()
                                    && completion.flow_id() == expected_flow.flow_id()
                            }
                            _ => false,
                        },
                        _ => false,
                    }
            }
        };
        origin_matches && command.is_ordered_source() == (lane == SendLane::OrderedSource)
    }
}

impl SendCell {
    fn is_ready(&self) -> bool {
        matches!(
            self,
            Self::Held {
                eligibility: SendEligibility::Ready,
                ..
            }
        )
    }

    fn is_capacity_blocked(&self) -> bool {
        matches!(
            self,
            Self::Held {
                eligibility: SendEligibility::CapacityBlocked,
                ..
            }
        )
    }

    fn rearm(&mut self) -> bool {
        match self {
            Self::Held { eligibility, .. } if *eligibility == SendEligibility::CapacityBlocked => {
                *eligibility = SendEligibility::Ready;
                true
            }
            _ => false,
        }
    }

    fn close(&mut self) {
        // Plain moves only: no allocation, callback, notification or yield
        // between taking the old phase and publishing the same storage.
        let prior = std::mem::replace(self, Self::Vacant);
        *self = match prior {
            Self::Held { packet, .. } => Self::ClosedReturned(packet),
            prior => prior,
        };
    }
}

impl SessionCommands {
    pub(super) fn prepare_pair(
        control_capacity: usize,
        ordered_capacity: usize,
    ) -> Result<PreparedSessionMailbox, OwnerMailboxPreparationError> {
        let (ingress, inbox) = SessionOwnerMailbox::bounded(control_capacity, ordered_capacity)
            .map_err(OwnerMailboxPreparationError::Configuration)?;
        let capacity = ingress
            .observe_capacity()
            .map_err(OwnerMailboxPreparationError::ObserverBusy)?;
        Ok(PreparedSessionMailbox {
            commands: Self {
                ingress,
                capacity,
                cells: [SendCell::Vacant, SendCell::Vacant],
                selector: SendCursor::new(),
                quarantined: false,
            },
            inbox,
        })
    }

    #[cfg(test)]
    fn from_open_completion(
        ingress: SessionOwnerIngress,
        request: PeerOpenRequest,
        result: OpenResultCode,
    ) -> Result<Self, SessionCommandsBuildFailure> {
        let capacity = match ingress.observe_capacity() {
            Ok(capacity) => capacity,
            Err(cause) => {
                return Err(SessionCommandsBuildFailure {
                    ingress,
                    request,
                    result,
                    cause,
                });
            }
        };
        // Complete fallible admission before publishing the actual command.
        Ok(Self {
            ingress,
            capacity,
            cells: [
                SendCell::Held {
                    packet: HeldCommand {
                        origin: CommandOrigin::OpenCompletion(request),
                        storage: CommandStorage::Fresh(SessionOwnerCommand::Event(
                            SessionEvent::PeerOpenResolved { request, result },
                        )),
                    },
                    eligibility: SendEligibility::Ready,
                },
                SendCell::Vacant,
            ],
            selector: SendCursor::new(),
            quarantined: false,
        })
    }

    #[cfg(test)]
    fn poll_driver_lane(
        &mut self,
        driver: &mut ResumableTcpDriver,
        lane: SendLane,
        cx: &mut Context<'_>,
    ) -> Poll<OwnerLaneAdmission> {
        // Legacy primitive characterizations own no Bound Flow ledger. They
        // use the same receive implementation, not a production bypass.
        self.pull_bound_driver_lane(driver, lane, &mut 0, cx)
    }

    fn pull_bound_driver_lane(
        &mut self,
        driver: &mut ResumableTcpDriver,
        lane: SendLane,
        transit: &mut usize,
        cx: &mut Context<'_>,
    ) -> Poll<OwnerLaneAdmission> {
        if self.quarantine_if_invalid() {
            return Poll::Ready(OwnerLaneAdmission::Quarantined);
        }
        if !matches!(self.cells[lane.index()], SendCell::Vacant) {
            return Poll::Ready(OwnerLaneAdmission::Occupied);
        }
        let Some(next_transit) = transit.checked_add(1) else {
            return Poll::Ready(OwnerLaneAdmission::TransitExhausted);
        };
        let local = driver.local_flow();
        let received = match lane {
            SendLane::Control => driver.poll_owner_control(cx),
            SendLane::OrderedSource => driver.poll_owner_ordered_source(cx),
        };
        let Poll::Ready(input) = received else {
            return Poll::Pending;
        };
        let Some(input) = input else {
            return Poll::Ready(OwnerLaneAdmission::Drained);
        };
        // Plain moves only after the audited receive. Publish real custody
        // before validation or any operation that could notify or yield.
        self.cells[lane.index()] = SendCell::Held {
            packet: HeldCommand {
                origin: CommandOrigin::Driver(lane, local),
                storage: CommandStorage::Fresh(SessionOwnerCommand::Driver(input)),
            },
            eligibility: SendEligibility::Ready,
        };
        *transit = next_transit;
        if self.quarantine_if_invalid() {
            Poll::Ready(OwnerLaneAdmission::Quarantined)
        } else {
            Poll::Ready(OwnerLaneAdmission::Admitted)
        }
    }

    fn drive_one_unit(&mut self) -> OwnerReadyDrive {
        if self.quarantine_if_invalid() {
            return OwnerReadyDrive::Quarantined;
        }
        let Some(lane) = self
            .selector
            .select(self.cells.each_ref().map(SendCell::is_ready))
        else {
            return OwnerReadyDrive::NoRunnable;
        };
        self.transfer_held_lane(lane)
    }

    // Completes one command-side visit without owning a Root claim or budget.
    // Physical command/observer custody stays here; only work and masks return.
    fn visit_selected_lane(
        &mut self,
        lane: SendLane,
        cx: &mut Context<'_>,
    ) -> Result<SessionCommandVisit, ()> {
        if self.quarantine_if_invalid() {
            return Err(());
        }
        let work = if self.cells[lane.index()].is_ready() {
            OwnerReadyUnit::Transfer(self.transfer_held_lane(lane))
        } else if self.cells[lane.index()].is_capacity_blocked() {
            use std::future::Future;
            match std::pin::pin!(self.rearm_full_commands()).as_mut().poll(cx) {
                Poll::Ready(outcome) => OwnerReadyUnit::Rearm(outcome),
                Poll::Pending => OwnerReadyUnit::Waiting,
            }
        } else {
            OwnerReadyUnit::Transfer(OwnerReadyDrive::NoRunnable)
        };
        self.summarize_visit(work)
    }

    fn summarize_visit(&mut self, work: OwnerReadyUnit) -> Result<SessionCommandVisit, ()> {
        if self.quarantine_if_invalid() {
            return Err(());
        }
        let mut runnable = 0;
        let mut blocked = 0;
        for index in 0..2 {
            if self.cells[index].is_ready() {
                runnable |= 1 << index;
            }
            if self.cells[index].is_capacity_blocked() {
                blocked |= 1 << index;
            }
        }
        if matches!(
            &work,
            OwnerReadyUnit::Transfer(OwnerReadyDrive::Full) | OwnerReadyUnit::Rearm(_)
        ) {
            // Ready resets the sole observer even for an unrelated lane.
            // Retained Full needs another registration unit, never resend.
            runnable |= blocked;
        }
        Ok(SessionCommandVisit {
            work,
            runnable,
            blocked: Some(blocked),
        })
    }

    fn transfer_held_lane(&mut self, lane: SendLane) -> OwnerReadyDrive {
        let index = lane.index();
        let origin = match &self.cells[index] {
            SendCell::Held { packet, .. } => packet.origin,
            _ => {
                self.quarantined = true;
                return OwnerReadyDrive::Quarantined;
            }
        };
        let prior = std::mem::replace(
            &mut self.cells[index],
            SendCell::TransferringUnknown { origin },
        );
        let SendCell::Held { packet, .. } = prior else {
            self.cells[index] = prior;
            self.quarantined = true;
            return OwnerReadyDrive::Quarantined;
        };
        // Unknown is already root-held before either send port can invoke a
        // receiver Waker. Only an actual return resolves this disposition.
        let outcome = match packet.storage.try_send(&self.ingress) {
            Ok(outcome) => outcome,
            Err(storage) => {
                self.cells[index] = SendCell::Held {
                    packet: HeldCommand { origin, storage },
                    eligibility: SendEligibility::Ready,
                };
                self.quarantined = true;
                return OwnerReadyDrive::Quarantined;
            }
        };
        match outcome {
            Ok(()) => {
                self.cells[index] = SendCell::Vacant;
                OwnerReadyDrive::MailboxOwned
            }
            Err(SessionMailboxTrySendError::Full(command)) => {
                self.cells[index] = SendCell::Held {
                    packet: HeldCommand {
                        origin,
                        storage: CommandStorage::Returned(command),
                    },
                    eligibility: SendEligibility::CapacityBlocked,
                };
                OwnerReadyDrive::Full
            }
            Err(SessionMailboxTrySendError::Closed(command)) => {
                self.cells[index] = SendCell::ClosedReturned(HeldCommand {
                    origin,
                    storage: CommandStorage::Returned(command),
                });
                OwnerReadyDrive::Closed
            }
        }
    }

    async fn rearm_full_commands(&mut self) -> OwnerCapacityRearm {
        if self.quarantine_if_invalid() {
            return OwnerCapacityRearm::Quarantined;
        }
        if !self.cells.iter().any(SendCell::is_capacity_blocked) {
            return OwnerCapacityRearm::NoBlocked;
        }
        let change = self.capacity.changed().await;
        // The observer has consumed its epoch pair. Commit all applicable
        // phase changes in this root before returning or yielding again.
        self.apply_capacity_change(change)
    }

    fn apply_capacity_change(
        &mut self,
        change: SessionMailboxCapacityChange,
    ) -> OwnerCapacityRearm {
        if self.quarantine_if_invalid() {
            return OwnerCapacityRearm::Quarantined;
        }
        match change {
            SessionMailboxCapacityChange::Closed => {
                for cell in &mut self.cells {
                    cell.close();
                }
                OwnerCapacityRearm::Closed
            }
            SessionMailboxCapacityChange::EpochExhausted => {
                self.quarantined = true;
                OwnerCapacityRearm::Quarantined
            }
            change => {
                let mut rearmed = false;
                for lane in [SendLane::Control, SendLane::OrderedSource] {
                    let matches_lane = matches!(
                        (&change, lane),
                        (SessionMailboxCapacityChange::Both, _)
                            | (SessionMailboxCapacityChange::Control, SendLane::Control)
                            | (
                                SessionMailboxCapacityChange::OrderedSource,
                                SendLane::OrderedSource
                            )
                    );
                    if matches_lane {
                        rearmed |= self.cells[lane.index()].rearm();
                    }
                }
                if rearmed {
                    OwnerCapacityRearm::Rearmed
                } else {
                    OwnerCapacityRearm::UnrelatedLane
                }
            }
        }
    }

    fn custody_is_known_and_valid(&self) -> bool {
        [SendLane::Control, SendLane::OrderedSource]
            .into_iter()
            .all(|lane| match &self.cells[lane.index()] {
                SendCell::Vacant => true,
                SendCell::Held { packet, .. } | SendCell::ClosedReturned(packet) => {
                    packet.is_valid_for(lane)
                }
                SendCell::TransferringUnknown { .. } => false,
            })
    }

    fn quarantine_if_invalid(&mut self) -> bool {
        if self.quarantined || !self.custody_is_known_and_valid() {
            self.quarantined = true;
            true
        } else {
            false
        }
    }
}

impl<T: super::TargetIo> super::OwnerTargetExecutor<T> {
    /// Own the complete terminal Source-to-cell operation here. Neither the
    /// raw receiver nor a cell reference escapes to the completion Module.
    pub(super) fn drain_ready_retirement_source(
        &mut self,
        selection: &crate::owned_upstream::owner_ready::OwnerReadyContinuationSelection,
        lane: SendLane,
    ) -> Result<crate::owned_upstream::owner_ready::OwnerReadyReadResult, super::OwnerTargetError>
    {
        use super::OwnerTargetError::BoundSourceRequiresSelectedService as Invalid;
        use super::service::{
            OwnerFlowServiceSlot, OwnerFlowWaitResource, OwnerTargetServiceState,
        };
        use crate::owned_upstream::owner_ready::{
            OwnerReadyFlowAssociation, OwnerReadyReadResult as Value,
        };
        let selected = selection.candidate.as_ref().ok_or(Invalid)?;
        let OwnerReadyFlowAssociation::Source(local) = selected.association else {
            return Err(Invalid);
        };
        let OwnerTargetServiceState::Bound {
            commands,
            flows,
            binding,
        } = &mut self.service
        else {
            return Err(Invalid);
        };
        binding
            .validate_receipt_flow(
                flows.slots.len(),
                selected.position,
                (selected.association, selected.generation, true),
            )
            .map_err(|_| Invalid)?;
        let owner = self.flows.get_mut(&local.flow_id()).ok_or(Invalid)?;
        if owner.service_position != Some(selected.position) || !owner.retire_after_join {
            return Err(Invalid);
        }
        let Some(OwnerFlowServiceSlot::Retiring(record)) = flows.slots.get_mut(selected.position)
        else {
            return Err(Invalid);
        };
        if record.association != selected.association
            || record.generation != selected.generation
            || record.wait.is_some()
        {
            return Err(Invalid);
        }
        let source = owner.source.as_mut().ok_or(Invalid)?;
        if source.flow.local_flow() != local || source.driver.local_flow() != local {
            return Err(Invalid);
        }
        let waker = record.tickets.driver_waker(lane).map_err(|_| Invalid)?;
        let resource = if lane == SendLane::Control {
            OwnerFlowWaitResource::ControlCell
        } else {
            OwnerFlowWaitResource::OrderedCell
        };
        let outcome = commands.pull_bound_driver_lane(
            &mut source.driver,
            lane,
            &mut record.transit,
            &mut Context::from_waker(&waker),
        );
        match outcome {
            Poll::Ready(OwnerLaneAdmission::Admitted) => Ok(Value::CommandHeld(lane)),
            Poll::Ready(OwnerLaneAdmission::Occupied) => {
                flows.wait_for(selected.position, resource)?;
                Ok(Value::Pending)
            }
            Poll::Ready(OwnerLaneAdmission::Drained) => {
                owner
                    .settle_terminal_source_drain(lane)
                    .map_err(|_| Invalid)?;
                flows.handoff_waiter(resource, binding)?;
                Ok(Value::Hint)
            }
            Poll::Pending => {
                flows.handoff_waiter(resource, binding)?;
                Ok(Value::Pending)
            }
            _ => Err(Invalid),
        }
    }

    /// One selected send-source unit: either pull one original Driver fact,
    /// transfer one retained command, or rearm the sole capacity observer.
    /// No command/Driver leaves this Executor and no inbox reduction is fused.
    pub(in crate::owned_upstream) fn visit_ready_send(
        &mut self,
        lane: SendLane,
    ) -> Result<SessionCommandVisit, super::OwnerTargetInputBlock> {
        use super::OwnerTargetInputBlock::ReadyReceiptInvariant as Invalid;
        use super::service::{OwnerFlowServiceSlot, OwnerTargetServiceState};
        use crate::owned_upstream::owner_ready::OwnerReadyFlowAssociation;
        if let Some(block) = self.input_admission_block(None) {
            return Err(block);
        }
        let OwnerTargetServiceState::Bound {
            commands,
            flows,
            binding,
        } = &mut self.service
        else {
            return Err(Invalid);
        };
        if commands.quarantine_if_invalid() {
            return Err(Invalid);
        }
        let vacant = matches!(commands.cells[lane.index()], SendCell::Vacant);
        let selection = binding
            .select_driver_source(lane, vacant)
            .map_err(|_| Invalid)?;
        let mut pulled = false;
        let mut visit = if vacant {
            if let Some(selected) = &selection.candidate {
                binding
                    .validate_receipt_flow(
                        flows.slots.len(),
                        selected.position,
                        (selected.association, selected.generation, false),
                    )
                    .map_err(|_| Invalid)?;
                let OwnerReadyFlowAssociation::Source(local) = selected.association else {
                    return Err(Invalid);
                };
                let owner = self.flows.get_mut(&local.flow_id()).ok_or(Invalid)?;
                let Some(OwnerFlowServiceSlot::Live(record)) =
                    flows.slots.get_mut(selected.position)
                else {
                    return Err(Invalid);
                };
                if record.association != selected.association
                    || record.generation != selected.generation
                    || owner.service_position != Some(selected.position)
                    || owner.retire_after_join
                {
                    return Err(Invalid);
                }
                let source = owner.source.as_mut().ok_or(Invalid)?;
                if source.flow.local_flow() != local || source.driver.local_flow() != local {
                    return Err(Invalid);
                }
                let waker = record.tickets.driver_waker(lane).map_err(|_| Invalid)?;
                let result = commands.pull_bound_driver_lane(
                    &mut source.driver,
                    lane,
                    &mut record.transit,
                    &mut Context::from_waker(&waker),
                );
                let work = match result {
                    Poll::Ready(OwnerLaneAdmission::Admitted) => {
                        pulled = true;
                        OwnerReadyUnit::SourcePulled
                    }
                    Poll::Ready(OwnerLaneAdmission::Drained) => {
                        if owner.settle_terminal_source_drain(lane)? {
                            OwnerReadyUnit::SourceDrained
                        } else {
                            OwnerReadyUnit::Waiting
                        }
                    }
                    Poll::Pending => OwnerReadyUnit::Waiting,
                    _ => return Err(Invalid),
                };
                commands.summarize_visit(work).map_err(|_| Invalid)?
            } else {
                commands
                    .summarize_visit(OwnerReadyUnit::Waiting)
                    .map_err(|_| Invalid)?
            }
        } else {
            let local = match &commands.cells[lane.index()] {
                SendCell::Held {
                    packet:
                        HeldCommand {
                            origin: CommandOrigin::Driver(_, local),
                            ..
                        },
                    ..
                } => *local,
                _ => return Err(Invalid),
            };
            let owner = self.flows.get(&local.flow_id()).ok_or(Invalid)?;
            let position = owner.service_position.ok_or(Invalid)?;
            let Some(OwnerFlowServiceSlot::Live(record) | OwnerFlowServiceSlot::Retiring(record)) =
                flows.slots.get(position)
            else {
                return Err(Invalid);
            };
            if record.transit == 0
                || record.association != OwnerReadyFlowAssociation::Source(local)
                || owner.retire_after_join
                    != matches!(flows.slots[position], OwnerFlowServiceSlot::Retiring(_))
            {
                return Err(Invalid);
            }
            let source = owner.source.as_ref().ok_or(Invalid)?;
            if source.flow.local_flow() != local || source.driver.local_flow() != local {
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
            let waker = flows.session.capacity_waker();
            commands
                .visit_selected_lane(lane, &mut Context::from_waker(&waker))
                .map_err(|_| Invalid)?
        };
        let vacant_mask = commands
            .cells
            .iter()
            .enumerate()
            .fold(0, |mask, (index, cell)| {
                mask | if matches!(cell, SendCell::Vacant) {
                    1 << index
                } else {
                    0
                }
            });
        if !vacant && vacant_mask & (1 << lane.index()) != 0 {
            let resource = if lane == SendLane::Control {
                super::service::OwnerFlowWaitResource::ControlCell
            } else {
                super::service::OwnerFlowWaitResource::OrderedCell
            };
            flows
                .handoff_waiter(resource, binding)
                .map_err(|_| Invalid)?;
        }
        visit.runnable |= binding
            .finish_driver_source(&selection, pulled, vacant_mask)
            .map_err(|_| Invalid)?;
        Ok(visit)
    }
}

#[cfg(test)]
mod tests;
