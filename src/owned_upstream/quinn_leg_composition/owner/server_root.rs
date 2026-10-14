//! Process-local whole-session custody and bounded ready dispatch.
//!
//! Root owns the sole ready selector, admitted claim, budgets and park waiter.
//! Each real resident owns its complete physical session; commands stay in its
//! Executor. cfg(test) Prototype compatibility drives that same command owner.
//! A ready transfer or actor join does not prove whole-session settlement.
//! Cold preparation must finish binding/adoption before Root can be polled.
//! Tests: real Bound path plus unchanged outer scheduling/fault scenarios.

use super::physical_session_root::OwnerPhysicalSessionRoot;
use crate::owned_upstream::owner_ready::{
    self as scheduler, OwnerReadySource, OwnerReadyUnit, ReadyGeometry, ReadyGeometryError,
    SessionCommandVisit,
};
#[cfg(test)]
use crate::owned_upstream::owner_ready::{OwnerCapacityRearm, OwnerReadyDrive, SendLane};
#[cfg(test)]
use crate::owned_upstream::owner_target::OwnerCommandPrototype;
use crate::owned_upstream::target::TargetIo;
use std::task::{Context, Poll};

mod construction;

pub(super) struct OwnerServerPreparation<T: TargetIo> {
    root: OwnerServerRoot<T>,
}

pub(super) struct OwnerControllerPreparation<T> {
    terminal_grace: std::time::Duration,
    session_id: crate::resumable::SessionId,
    executor: crate::owned_upstream::owner_target::OwnerTargetExecutor<T>,
    clock: crate::owned_upstream::session_clock::SessionMonotonicClock,
    plan: crate::owned_upstream::owner_session_capacity::OwnerSessionCapacityPlan,
    limits: crate::owned_upstream::owner_ready::OwnerReadySessionLimits,
}

pub(super) struct OwnerServerColdBuildFailure {
    plan: ReadyProcessPlan,
    cause: scheduler::ReadyFault,
}

pub(super) struct OwnerServerStartFailure<T: TargetIo> {
    preparation: OwnerServerPreparation<T>,
    cause: scheduler::ReadyFault,
}

pub(super) struct OwnerServerAdoptFailure<T: TargetIo> {
    current: OwnerPhysicalSessionRoot<T>,
    cause: OwnerServerAdoptCause,
}

enum OwnerServerAdoptCause {
    Ready(scheduler::ReadyFault),
    NotIdle,
    Full,
    Physical(super::physical_session_root::OwnerPhysicalReadyActivationFailure),
}

pub(super) enum OwnerControllerPreparationError {
    NotIdle,
    TerminalServiceCapacityOverflow,
    Ready(scheduler::ReadyFault),
    Reservation(scheduler::OwnerReadyReservationError),
    Constructor(crate::owned_upstream::owner_session_controller::OwnerSessionControllerBuildError),
}

pub(super) enum OwnerControllerPreparationFailure<T> {
    Rejected {
        input: OwnerControllerPreparation<T>,
        cause: OwnerControllerPreparationError,
    },
    Quarantined(scheduler::ReadyFault),
}

// This one inline terminal destination retains the actual failed constructor;
// it is not a parent/session table, copied inventory or reusable reservation.
struct OwnerConstructorQuarantine<T> {
    input: OwnerControllerPreparation<T>,
    constructor_cause:
        crate::owned_upstream::owner_session_controller::OwnerSessionControllerBuildError,
    ready: scheduler::OwnerReadyConstructorQuarantine,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ReadyClass {
    Settlement,
    Bootstrap,
    TargetOpen,
    Session,
}

impl ReadyClass {
    const ALL: [Self; 4] = [
        Self::Settlement,
        Self::Bootstrap,
        Self::TargetOpen,
        Self::Session,
    ];
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum ReadyProcessPlanError {
    ZeroSessions,
    ZeroClassBudget { class: ReadyClass },
    ProcessCapacityOverflow,
    TotalBudgetOverflow,
    ServiceBoundOverflow { class: ReadyClass },
}

// Checked configuration only: these are service-delay bounds under continuous
// readiness and dedicated class slices, never completion or wall-clock claims.
#[derive(Debug)]
pub(super) struct ReadyProcessPlan {
    geometry: ReadyGeometry,
    budgets: [usize; 4],
    turn_budget: usize,
    turn_bounds: [usize; 4],
    quantum_bounds: [usize; 4],
}

impl ReadyProcessPlan {
    /// Cold scheduling units, not elapsed time or constant-time Target work.
    /// Uses this real Root's checked Session-class quantum bound and the
    /// original session plan: one unit, Z due units, F visits, completion sweep.
    pub(super) fn terminal_service_units(
        &self,
        plan: crate::owned_upstream::owner_session_capacity::OwnerSessionCapacityPlan,
    ) -> Option<[usize; 4]> {
        let session = plan.session_config();
        let q = self.quantum_bounds[ReadyClass::Session as usize];
        let unit = 3usize.checked_mul(13)?.checked_mul(5)?.checked_mul(q)?;
        let due = session.max_terminal_tombstones().checked_mul(unit)?;
        let flows = session.max_flows().checked_mul(195)?.checked_mul(q)?;
        let completion = session
            .max_flows()
            .checked_mul(3)?
            .checked_add(2)?
            .checked_mul(195)?
            .checked_mul(q)?;
        // Installation and initial registration are separate units per entry;
        // also reject an overflowing complete cold service envelope.
        due.checked_mul(3)?
            .checked_add(flows)?
            .checked_add(completion)?;
        Some([unit, due, flows, completion])
    }

    pub(super) fn checked(
        session_capacity: usize,
        budgets: [usize; 4],
    ) -> Result<Self, ReadyProcessPlanError> {
        let session_capacity = std::num::NonZeroUsize::new(session_capacity)
            .ok_or(ReadyProcessPlanError::ZeroSessions)?;
        for (class, budget) in ReadyClass::ALL.into_iter().zip(budgets) {
            if budget == 0 {
                return Err(ReadyProcessPlanError::ZeroClassBudget { class });
            }
        }
        let geometry = ReadyGeometry::checked(session_capacity).map_err(
            |ReadyGeometryError::ProcessCapacityOverflow| {
                ReadyProcessPlanError::ProcessCapacityOverflow
            },
        )?;
        let class_capacities = geometry.class_capacities();
        let turn_budget = budgets
            .into_iter()
            .try_fold(0usize, |sum, budget| sum.checked_add(budget))
            .ok_or(ReadyProcessPlanError::TotalBudgetOverflow)?;
        let mut turn_bounds = [0; 4];
        let mut quantum_bounds = [0; 4];
        for (index, class) in ReadyClass::ALL.into_iter().enumerate() {
            let capacity = class_capacities[index];
            let divisor = budgets[index].min(capacity);
            let turns = (capacity / divisor)
                .checked_add(usize::from(capacity % divisor != 0))
                .and_then(|rounds| rounds.checked_add(1))
                .ok_or(ReadyProcessPlanError::ServiceBoundOverflow { class })?;
            let quanta = turns
                .checked_mul(turn_budget)
                .ok_or(ReadyProcessPlanError::ServiceBoundOverflow { class })?;
            turn_bounds[index] = turns;
            quantum_bounds[index] = quanta;
        }
        Ok(Self {
            geometry,
            budgets,
            turn_budget,
            turn_bounds,
            quantum_bounds,
        })
    }
}

enum OwnerResident<T: TargetIo> {
    Current(OwnerPhysicalSessionRoot<T>),
    #[cfg(test)]
    Prototype(OwnerCommandPrototype),
}

struct CommandResident<T: TargetIo> {
    session_id: crate::resumable::SessionId,
    owner: OwnerResident<T>,
    capacity_waker: std::task::Waker,
    fault: Option<scheduler::ReadyFault>,
}

#[cfg(test)]
struct OwnerServerBuildFailure {
    plan: ReadyProcessPlan,
    commands: Vec<OwnerCommandPrototype>,
    cause: scheduler::ReadyFault,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum OwnerReadyStep {
    Visited {
        session: crate::resumable::SessionId,
        source: OwnerReadySource,
        work: OwnerReadyUnit,
    },
    Rescan,
    Quarantined,
}

enum ReadyVisitState<T> {
    Idle,
    Running(scheduler::ReadyClaim, ReadyVisitCompletion),
    ConstructorQuarantined(OwnerConstructorQuarantine<T>),
}

enum ReadyVisitCompletion {
    Entered,
    Completed(SessionCommandVisit),
}

// Owns whole physical residents and the only mutable ready selector; their
// inward authorities remain intact. No P4 bootstrap/admission table lives here.
// Construction is cold; all recoverable failure returns the original inputs.
// Allocator abort is not an exact-return or recovery claim.
pub(super) struct OwnerServerRoot<T: TargetIo> {
    plan: ReadyProcessPlan,
    residents: Vec<CommandResident<T>>,
    ready: scheduler::ReadyIndex,
    in_flight: ReadyVisitState<T>,
    class: usize,
    used: usize,
    registration: std::pin::Pin<Box<tokio::sync::futures::OwnedNotified>>,
    notification: std::sync::Arc<tokio::sync::Notify>,
}

impl<T: TargetIo> OwnerServerRoot<T> {
    #[cfg(test)]
    fn from_commands(
        plan: ReadyProcessPlan,
        commands: Vec<OwnerCommandPrototype>,
    ) -> Result<Self, OwnerServerBuildFailure> {
        use scheduler::ReadyFault;
        let prepared = (|| {
            if commands.len() > plan.geometry.session_capacity() {
                return Err(ReadyFault::InvalidResident);
            }
            // Cold validation only, before any custody transfer or publication.
            for (index, command) in commands.iter().enumerate() {
                let id = command
                    .check_open_completion_admission()
                    .map(|(id, _)| id)
                    .ok_or(ReadyFault::InvalidResident)?;
                if commands[..index].iter().any(|other| {
                    other.check_open_completion_admission().map(|(id, _)| id) == Some(id)
                }) {
                    return Err(ReadyFault::InvalidResident);
                }
            }
            std::alloc::Layout::array::<CommandResident<T>>(plan.geometry.session_capacity())
                .map_err(|_| ReadyFault::Layout)?;
            std::alloc::Layout::array::<[usize; 2]>(plan.geometry.session_capacity())
                .map_err(|_| ReadyFault::Layout)?;
            std::alloc::Layout::array::<crate::resumable::SessionId>(
                plan.geometry.session_capacity(),
            )
            .map_err(|_| ReadyFault::Layout)?;
            let mut residents = Vec::new();
            residents
                .try_reserve_exact(plan.geometry.session_capacity())
                .map_err(|_| ReadyFault::Reserve)?;
            let mut initial = Vec::new();
            initial
                .try_reserve_exact(plan.geometry.session_capacity())
                .map_err(|_| ReadyFault::Reserve)?;
            let mut wakers = Vec::new();
            wakers
                .try_reserve_exact(plan.geometry.session_capacity())
                .map_err(|_| ReadyFault::Reserve)?;
            let mut identities = Vec::new();
            identities
                .try_reserve_exact(plan.geometry.session_capacity())
                .map_err(|_| ReadyFault::Reserve)?;
            for command in &commands {
                identities.push(
                    command
                        .check_open_completion_admission()
                        .map(|(id, _)| id)
                        .ok_or(ReadyFault::InvalidResident)?,
                );
                let (_, masks) = command
                    .check_open_completion_admission()
                    .ok_or(ReadyFault::InvalidResident)?;
                initial.push(masks);
            }
            let ready = scheduler::ReadyIndex::new(&plan.geometry, &initial)?;
            let notification = ready.notification();
            let registration = Box::pin(std::sync::Arc::clone(&notification).notified_owned());
            for slot in 0..commands.len() {
                wakers.push(ready.capacity_waker(slot));
            }
            Ok((
                residents,
                ready,
                notification,
                registration,
                wakers,
                identities,
            ))
        })();
        let (mut residents, ready, notification, registration, wakers, identities) = match prepared
        {
            Ok(prepared) => prepared,
            Err(cause) => {
                return Err(OwnerServerBuildFailure {
                    plan,
                    commands,
                    cause,
                });
            }
        };
        // Every input was validated and remains unmodified. Derive correlation
        // from the real sealed request; no later cell can erase this identity.
        // This final move has no allocation, callback, poll or fallible operation.
        for ((commands, capacity_waker), session_id) in
            commands.into_iter().zip(wakers).zip(identities)
        {
            residents.push(CommandResident {
                session_id,
                owner: OwnerResident::Prototype(commands),
                capacity_waker,
                fault: None,
            });
        }
        Ok(Self {
            plan,
            residents,
            ready,
            in_flight: ReadyVisitState::Idle,
            class: 0,
            used: 0,
            registration,
            notification,
        })
    }

    fn quarantine_resident(&mut self, slot: usize, cause: scheduler::ReadyFault) {
        let Some(resident) = self.residents.get_mut(slot) else {
            self.ready.fail_root(scheduler::ReadyFault::Corrupt);
            return;
        };
        if self.ready.quarantine(slot).is_err() {
            return;
        }
        resident.fault.get_or_insert(cause);
        match &mut resident.owner {
            OwnerResident::Current(current) => {
                if let Err(fault) = current.quarantine_ready_source() {
                    self.ready.fail_root(fault);
                    return;
                }
            }
            #[cfg(test)]
            OwnerResident::Prototype(commands) => commands.quarantine_source(),
        }
        self.in_flight = ReadyVisitState::Idle;
    }

    fn advance_class(&mut self) {
        self.class = (self.class + 1) % 4;
        self.used = 0;
    }

    pub(super) fn poll_ready_unit(&mut self, cx: &mut Context<'_>) -> Poll<OwnerReadyStep> {
        use scheduler::ReadyFault;
        // Scheduler corruption is root-fatal. Preserve any unfinished visit
        // and its physical disposition instead of treating it as source unwind.
        if self.ready.check_root().is_err() {
            return Poll::Ready(OwnerReadyStep::Quarantined);
        }
        if matches!(self.in_flight, ReadyVisitState::ConstructorQuarantined(_)) {
            self.ready.fail_root(ReadyFault::Corrupt);
            return Poll::Ready(OwnerReadyStep::Quarantined);
        }
        // A previous source call unwound. Its physical command disposition is
        // still in SessionCommands; quarantine only that resident and leases.
        if matches!(
            self.in_flight,
            ReadyVisitState::Running(_, ReadyVisitCompletion::Completed(_))
        ) {
            return Poll::Ready(self.finish_ready_visit());
        }
        if let ReadyVisitState::Running(claim, ReadyVisitCompletion::Entered) = &self.in_flight {
            self.quarantine_resident(claim.slot, ReadyFault::Corrupt);
            return Poll::Ready(OwnerReadyStep::Quarantined);
        }
        for _ in 0..4 {
            let slot = match self.ready.select(self.class) {
                Ok(Some(slot)) => slot,
                Ok(None) => {
                    self.advance_class();
                    continue;
                }
                Err(fault) => {
                    self.ready.fail_root(fault);
                    return Poll::Ready(OwnerReadyStep::Quarantined);
                }
            };
            // These other classes have real reserved backing but no active
            // source in this packet. Do not invent a bootstrap/Target driver.
            if self.class != 3 || slot >= self.residents.len() {
                self.ready.fail_root(ReadyFault::Corrupt);
                return Poll::Ready(OwnerReadyStep::Quarantined);
            }
            self.used += 1;
            if self.used == self.plan.budgets[self.class] {
                self.advance_class();
            }
            let claim = match self.ready.claim(slot) {
                Ok(claim) => claim,
                Err(fault) => {
                    self.ready.fail_root(fault);
                    return Poll::Ready(OwnerReadyStep::Quarantined);
                }
            };
            let source = claim.source;
            self.in_flight = ReadyVisitState::Running(claim, ReadyVisitCompletion::Entered);
            let resident = &mut self.residents[slot];
            if resident.fault.is_some() {
                self.quarantine_resident(slot, ReadyFault::Corrupt);
                return Poll::Ready(OwnerReadyStep::Quarantined);
            }
            let visited = {
                let mut context = Context::from_waker(&resident.capacity_waker);
                match &mut resident.owner {
                    OwnerResident::Current(current) => {
                        current.visit_ready_source(source, &mut context)
                    }
                    #[cfg(test)]
                    OwnerResident::Prototype(commands) => match source {
                        OwnerReadySource::ControlSend => {
                            commands.visit_selected_lane(SendLane::Control, &mut context)
                        }
                        OwnerReadySource::OrderedSend => {
                            commands.visit_selected_lane(SendLane::OrderedSource, &mut context)
                        }
                        OwnerReadySource::ControlInbox
                        | OwnerReadySource::OrderedInbox
                        | OwnerReadySource::OwnerContinuation => Err(()),
                    },
                }
            };
            let completed = match visited {
                Ok(visited) => visited,
                Err(()) => {
                    self.quarantine_resident(slot, ReadyFault::Corrupt);
                    return Poll::Ready(OwnerReadyStep::Quarantined);
                }
            };
            let ReadyVisitState::Running(_, completion) = &mut self.in_flight else {
                self.ready.fail_root(ReadyFault::Corrupt);
                return Poll::Ready(OwnerReadyStep::Quarantined);
            };
            // No outward Notify occurs before this actual result is stored
            // beside the original, unmoved claim.
            *completion = ReadyVisitCompletion::Completed(completed);
            return Poll::Ready(self.finish_ready_visit());
        }
        let epoch = match self.ready.park_epoch() {
            Ok(Some(epoch)) => epoch,
            Ok(None) => return Poll::Ready(OwnerReadyStep::Rescan),
            Err(fault) => {
                self.ready.fail_root(fault);
                return Poll::Ready(OwnerReadyStep::Quarantined);
            }
        };
        use std::future::Future;
        if self.registration.as_mut().poll(cx).is_ready() {
            self.registration
                .as_mut()
                .set(std::sync::Arc::clone(&self.notification).notified_owned());
            return Poll::Ready(OwnerReadyStep::Rescan);
        }
        if self.ready.park_is_unchanged(epoch) {
            Poll::Pending
        } else {
            Poll::Ready(OwnerReadyStep::Rescan)
        }
    }

    fn finish_ready_visit(&mut self) -> OwnerReadyStep {
        use scheduler::ReadyFault;
        let ReadyVisitState::Running(claim, ReadyVisitCompletion::Completed(completed)) =
            &mut self.in_flight
        else {
            self.ready.fail_root(ReadyFault::Corrupt);
            return OwnerReadyStep::Quarantined;
        };
        let Some(resident) = self.residents.get_mut(claim.slot) else {
            self.ready.fail_root(ReadyFault::Corrupt);
            return OwnerReadyStep::Quarantined;
        };
        let publication = match &mut resident.owner {
            OwnerResident::Current(current) => current.finish_ready_source(completed),
            #[cfg(test)]
            OwnerResident::Prototype(_) => Ok(()),
        };
        // A late publisher failure cannot skip settlement of admitted work.
        // Keep both the original claim and Completed value on either failure.
        let finished = self
            .ready
            .finish(claim, completed.runnable, completed.blocked);
        if let Err(fault) = publication.and(finished) {
            self.ready.fail_root(fault);
            return OwnerReadyStep::Quarantined;
        }
        let session = resident.session_id;
        let source = claim.source;
        let work = std::mem::replace(&mut completed.work, OwnerReadyUnit::Waiting);
        self.in_flight = ReadyVisitState::Idle;
        OwnerReadyStep::Visited {
            session,
            source,
            work,
        }
    }
}

#[cfg(test)]
mod tests;
