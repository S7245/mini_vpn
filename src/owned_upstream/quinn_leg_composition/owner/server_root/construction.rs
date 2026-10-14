//! Unpublished construction of the actual process/session custodian.
//!
//! Owns the same Root, not a second ready index or a published facade. Cold
//! allocation precedes session reservation; every modeled failure returns the
//! original process plan. Global allocator abort is not a recoverable return.
//! Controller binding and whole-root adoption must finish before start makes
//! the Root pollable. No producer is polled or registered by cold construction.
//! Entries: OwnerServerRoot::cold and OwnerControllerPreparation::new.
//! Actual construction/activation tests live in the real Bound source tracer.

use super::*;
use crate::owned_upstream::owner_ready::OwnerReadySessionLimits;
use crate::owned_upstream::owner_session_capacity::OwnerSessionCapacityPlan;
use crate::owned_upstream::owner_session_controller::OwnerSessionController;
use crate::owned_upstream::owner_target::OwnerTargetExecutor;
use crate::owned_upstream::session_clock::SessionMonotonicClock;
use crate::resumable::SessionId;

#[cfg(test)]
std::thread_local! {
    pub(super) static FAIL_RESIDENT_RESERVE: std::cell::Cell<bool> = const {
        std::cell::Cell::new(false)
    };
}

impl<T> OwnerControllerPreparation<T> {
    pub(in crate::owned_upstream::quinn_leg_composition::owner) fn new(
        session_id: SessionId,
        executor: OwnerTargetExecutor<T>,
        clock: SessionMonotonicClock,
        plan: OwnerSessionCapacityPlan,
        limits: OwnerReadySessionLimits,
        terminal_grace: std::time::Duration,
    ) -> Self {
        Self {
            terminal_grace,
            session_id,
            executor,
            clock,
            plan,
            limits,
        }
    }
}

impl<T: TargetIo> OwnerServerPreparation<T> {
    pub(in crate::owned_upstream::quinn_leg_composition::owner) fn adopt_current_only(
        &mut self,
        mut current: OwnerPhysicalSessionRoot<T>,
    ) -> Result<(), OwnerServerAdoptFailure<T>> {
        use OwnerServerAdoptCause as Cause;
        if let Err(cause) = self.root.ready.check_root() {
            return Err(OwnerServerAdoptFailure {
                current,
                cause: Cause::Ready(cause),
            });
        }
        if !matches!(self.root.in_flight, ReadyVisitState::Idle) {
            return Err(OwnerServerAdoptFailure {
                current,
                cause: Cause::NotIdle,
            });
        }
        let position = self.root.residents.len();
        match self.root.ready.check_preparation_boundary(position) {
            Ok(()) | Err(scheduler::ReadyFault::InvalidResident) => {
                // Only a checked unpublished suffix returns InvalidResident.
                // The consuming physical operation below must match its exact
                // binding at this next position; all prefix faults remain fatal.
            }
            Err(cause) => {
                return Err(OwnerServerAdoptFailure {
                    current,
                    cause: Cause::Ready(cause),
                });
            }
        }
        if position >= self.root.plan.geometry.session_capacity()
            || position >= self.root.residents.capacity()
        {
            return Err(OwnerServerAdoptFailure {
                current,
                cause: Cause::Full,
            });
        }
        let (session_id, capacity_waker) =
            match current.activate_owner_ready(&mut self.root.ready, position) {
                Ok(activated) => activated,
                Err(cause) => {
                    return Err(OwnerServerAdoptFailure {
                        current,
                        cause: Cause::Physical(cause),
                    });
                }
            };
        self.root.residents.push(CommandResident {
            session_id,
            owner: OwnerResident::Current(current),
            capacity_waker,
            fault: None,
        });
        Ok(())
    }

    pub(in crate::owned_upstream::quinn_leg_composition::owner) fn start(
        self,
    ) -> Result<OwnerServerRoot<T>, OwnerServerStartFailure<T>> {
        let checked = self.root.ready.check_root().and_then(|()| {
            if !matches!(self.root.in_flight, ReadyVisitState::Idle) {
                return Err(scheduler::ReadyFault::InvalidResident);
            }
            self.root
                .ready
                .check_preparation_boundary(self.root.residents.len())?;
            for (slot, resident) in self.root.residents.iter().enumerate() {
                if resident.fault.is_some() || !matches!(resident.owner, OwnerResident::Current(_))
                {
                    return Err(scheduler::ReadyFault::InvalidResident);
                }
                self.root
                    .ready
                    .check_prepared_resident(slot, resident.session_id)?;
            }
            Ok(())
        });
        match checked {
            Ok(()) => Ok(self.root),
            Err(cause) => Err(OwnerServerStartFailure {
                preparation: self,
                cause,
            }),
        }
    }

    pub(in crate::owned_upstream::quinn_leg_composition::owner) fn build_checked_controller(
        &mut self,
        input: OwnerControllerPreparation<T>,
    ) -> Result<OwnerSessionController<T>, OwnerControllerPreparationFailure<T>> {
        use OwnerControllerPreparationError as Error;
        use OwnerControllerPreparationFailure as Failure;
        if !matches!(self.root.in_flight, ReadyVisitState::Idle) {
            return Err(Failure::Rejected {
                input,
                cause: Error::NotIdle,
            });
        }
        if let Err(cause) = self
            .root
            .ready
            .check_preparation_boundary(self.root.residents.len())
        {
            return Err(Failure::Rejected {
                input,
                cause: Error::Ready(cause),
            });
        }
        let OwnerControllerPreparation {
            terminal_grace,
            session_id,
            executor,
            clock,
            plan,
            limits,
        } = input;
        if self.root.plan.terminal_service_units(plan).is_none() {
            return Err(Failure::Rejected {
                input: OwnerControllerPreparation {
                    terminal_grace,
                    session_id,
                    executor,
                    clock,
                    plan,
                    limits,
                },
                cause: Error::TerminalServiceCapacityOverflow,
            });
        }
        let ready = match self.root.ready.reserve_session(session_id, limits) {
            Ok(ready) => ready,
            Err(cause) => {
                return Err(Failure::Rejected {
                    input: OwnerControllerPreparation {
                        terminal_grace,
                        session_id,
                        executor,
                        clock,
                        plan,
                        limits,
                    },
                    cause: Error::Reservation(cause),
                });
            }
        };
        let failure =
            match OwnerSessionController::new_checked(executor, clock, plan, ready, terminal_grace)
            {
                Ok(controller) => return Ok(controller),
                Err(failure) => failure,
            };
        let (clock, executor, plan, ready, terminal_grace, constructor_cause) =
            failure.into_parts();
        let input = OwnerControllerPreparation {
            terminal_grace,
            session_id,
            executor,
            clock,
            plan,
            limits,
        };
        match ready.cancel() {
            Ok(()) => Err(Failure::Rejected {
                input,
                cause: Error::Constructor(constructor_cause),
            }),
            Err(failure) => {
                let ready = failure.into_constructor_quarantine();
                self.root.in_flight =
                    ReadyVisitState::ConstructorQuarantined(OwnerConstructorQuarantine {
                        input,
                        constructor_cause,
                        ready,
                    });
                // The original first fault is already sticky. Store every real
                // constructor input before publishing this fatal observation.
                let cause = self.root.ready.fail_root(scheduler::ReadyFault::Corrupt);
                Err(Failure::Quarantined(cause))
            }
        }
    }
}

impl<T: TargetIo> OwnerServerRoot<T> {
    /// Includes each actual cold allocation once, excluding future residents'
    /// dynamic pointees. Moves and Notify clones do not duplicate backing.
    pub(super) fn cold_layout(
        plan: &ReadyProcessPlan,
    ) -> Result<std::alloc::Layout, scheduler::ReadyFault> {
        use scheduler::ReadyFault::Layout as InvalidLayout;
        use std::alloc::Layout;
        let residents = Layout::array::<CommandResident<T>>(plan.geometry.session_capacity())
            .map_err(|_| InvalidLayout)?;
        let mut peak = Layout::new::<Self>();
        for piece in [
            residents,
            scheduler::ReadyIndex::cold_backing_layout(&plan.geometry)?,
            Layout::new::<tokio::sync::futures::OwnedNotified>(),
        ] {
            peak = peak.extend(piece).map_err(|_| InvalidLayout)?.0;
        }
        Ok(peak.pad_to_align())
    }

    pub(in crate::owned_upstream::quinn_leg_composition::owner) fn cold(
        plan: ReadyProcessPlan,
    ) -> Result<OwnerServerPreparation<T>, OwnerServerColdBuildFailure> {
        let capacity = plan.geometry.session_capacity();
        if let Err(cause) = Self::cold_layout(&plan) {
            return Err(OwnerServerColdBuildFailure { plan, cause });
        }
        #[cfg(test)]
        if FAIL_RESIDENT_RESERVE.replace(false) {
            return Err(OwnerServerColdBuildFailure {
                plan,
                cause: scheduler::ReadyFault::Reserve,
            });
        }
        let residents = match crate::resumable::effect_storage::allocate_exact_vec(capacity) {
            Ok(residents) => residents,
            Err(error) => {
                return Err(OwnerServerColdBuildFailure {
                    plan,
                    cause: match error {
                        crate::resumable::effect_storage::EffectStorageError::Reserve => {
                            scheduler::ReadyFault::Reserve
                        }
                        _ => scheduler::ReadyFault::Layout,
                    },
                });
            }
        };
        let ready = match scheduler::ReadyIndex::cold(&plan.geometry) {
            Ok(ready) => ready,
            Err(cause) => return Err(OwnerServerColdBuildFailure { plan, cause }),
        };
        let notification = ready.notification();
        let registration = Box::pin(std::sync::Arc::clone(&notification).notified_owned());
        Ok(OwnerServerPreparation {
            root: Self {
                plan,
                residents,
                ready,
                in_flight: ReadyVisitState::Idle,
                class: 0,
                used: 0,
                registration,
                notification,
            },
        })
    }
}
