//! Checked and test-only cold construction within the existing Controller authority.
//!
//! Owns capacity validation, exact build-failure returns and immutable witness access.
//! Storage, continuation, recovery and lifecycle settlement stay in the parent.
//! Construction returns the original inputs on rejection and publishes only after
//! validation and consuming installation of the same ready binding/mailbox pair.
//! Entries: new_checked, test-only new_for_test, build-failure and capacity witnesses.
//! Tests remain in the parent tests/capacity.rs and physical composition fixtures.

use std::fmt;

use super::{
    CheckedOwnerControllerCapacity, Duration, OwnerOutputPageCapacity, OwnerReadyReceipt,
    OwnerReverseProgress, OwnerSessionCapacityPlan, OwnerSessionCapacityWitness,
    OwnerSessionControllerBuildError, OwnerSessionControllerBuildFailure,
    OwnerSessionControllerCore, OwnerSessionControllerPhase, OwnerTargetExecutor,
    OwnerTerminalGraceState, SessionMonotonicClock, TargetIo,
};
use crate::owned_upstream::owner_ready::OwnerReadySessionReservation;

#[cfg(test)]
use super::{
    LegacyOwnerControllerCapacity, LegacyOwnerSessionControllerBuildFailure,
    OwnerOutputPageCapacityError, OwnerProgressCapacity, OwnerProgressError,
};

impl<'r, T> OwnerSessionControllerBuildFailure<'r, T> {
    pub(in crate::owned_upstream) const fn source(&self) -> &OwnerSessionControllerBuildError {
        &self.source
    }

    pub(in crate::owned_upstream) fn into_parts(
        self,
    ) -> (
        SessionMonotonicClock,
        OwnerTargetExecutor<T>,
        OwnerSessionCapacityPlan,
        OwnerReadySessionReservation<'r>,
        Duration,
        OwnerSessionControllerBuildError,
    ) {
        (
            self.clock,
            self.executor,
            self.plan,
            self.ready,
            self.terminal_grace,
            self.source,
        )
    }
}

impl<T> fmt::Debug for OwnerSessionControllerBuildFailure<'_, T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OwnerSessionControllerBuildFailure")
            .field("clock", &"[REDACTED]")
            .field("executor", &"[REDACTED]")
            .field("plan", &self.plan)
            .field("ready", &"[REDACTED]")
            .field("source", &self.source)
            .finish()
    }
}

impl<T: TargetIo> OwnerSessionControllerCore<T, CheckedOwnerControllerCapacity> {
    /// Builds the production controller only after proving that the installed
    /// reducer, Target adapter, executor queues, and output page all derive
    /// from one injected replay-storage plan.
    pub(in crate::owned_upstream) fn new_checked<'r>(
        executor: OwnerTargetExecutor<T>,
        clock: SessionMonotonicClock,
        plan: OwnerSessionCapacityPlan,
        ready: OwnerReadySessionReservation<'r>,
        terminal_grace: Duration,
    ) -> Result<Self, OwnerSessionControllerBuildFailure<'r, T>> {
        let capacity = match OwnerSessionCapacityWitness::derive(&executor, plan) {
            Ok(capacity) => capacity,
            Err(source) => {
                return Err(OwnerSessionControllerBuildFailure {
                    terminal_grace,
                    clock,
                    executor,
                    plan,
                    ready,
                    source: OwnerSessionControllerBuildError::Capacity(source),
                });
            }
        };
        let grace = match OwnerTerminalGraceState::checked(&executor, &clock, terminal_grace) {
            Ok(grace) => grace,
            Err(source) => {
                return Err(OwnerSessionControllerBuildFailure {
                    terminal_grace,
                    clock,
                    executor,
                    plan,
                    ready,
                    source: OwnerSessionControllerBuildError::TerminalGrace(source),
                });
            }
        };
        let progress_capacity = capacity.progress_capacity();
        let progress = match OwnerReverseProgress::from_capacity(progress_capacity) {
            Ok(progress) => progress,
            Err(source) => {
                return Err(OwnerSessionControllerBuildFailure {
                    terminal_grace,
                    clock,
                    executor,
                    plan,
                    ready,
                    source: OwnerSessionControllerBuildError::Capacity(
                        super::OwnerSessionCapacityError::ProgressStorage(source),
                    ),
                });
            }
        };
        let (executor, inbox) = match executor.install_owner_ready(ready, plan) {
            Ok(installed) => installed,
            Err((executor, ready, source)) => {
                return Err(OwnerSessionControllerBuildFailure {
                    terminal_grace,
                    clock,
                    executor,
                    plan,
                    ready,
                    source: OwnerSessionControllerBuildError::Ready(source),
                });
            }
        };
        Ok(Self {
            terminal_grace: grace,
            executor,
            capacity: CheckedOwnerControllerCapacity {
                witness: capacity,
                inbox,
                receipt: OwnerReadyReceipt::Vacant,
            },
            progress,
            clock,
            phase: OwnerSessionControllerPhase::Active,
            pending_continuation: None,
            aborted_turn: None,
            recovery: None,
            legless_high_water: None,
            terminal_quarantine: None,
            attach_status: None,
            aborted_recovery: None,
            #[cfg(test)]
            fail_next_recovery_progress: false,
            #[cfg(test)]
            fail_next_continuation_progress: false,
            #[cfg(test)]
            panic_after_terminal_bind_commit: false,
        })
    }

    /// Returns the exact immutable witness installed by checked construction.
    pub(crate) const fn capacity_witness(&self) -> OwnerSessionCapacityWitness {
        self.capacity.witness
    }

    /// Returns the already-checked output storage authority. Publication code
    /// cannot fall back to re-deriving it from a mutable executor surface.
    pub(crate) const fn output_page_capacity(&self) -> OwnerOutputPageCapacity {
        self.capacity.witness.output_page()
    }
}

#[cfg(test)]
impl<T: TargetIo> OwnerSessionControllerCore<T, LegacyOwnerControllerCapacity> {
    /// Test-only compatibility seam for existing reducer/controller tracers.
    /// It intentionally has no conversion into the checked type.
    pub(super) fn new_for_test(
        executor: OwnerTargetExecutor<T>,
        clock: SessionMonotonicClock,
    ) -> Result<Self, LegacyOwnerSessionControllerBuildFailure<T>> {
        let installed = executor.installed_session_config();
        let progress_capacity = match OwnerProgressCapacity::checked(
            installed.max_flows(),
            installed.max_terminal_tombstones(),
        ) {
            Ok(capacity) => capacity,
            Err(_) => {
                return Err(LegacyOwnerSessionControllerBuildFailure {
                    clock,
                    executor,
                    source: OwnerProgressError::CapacityLimitOverflow,
                });
            }
        };
        let progress = match OwnerReverseProgress::from_capacity(progress_capacity) {
            Ok(progress) => progress,
            Err(source) => {
                return Err(LegacyOwnerSessionControllerBuildFailure {
                    clock,
                    executor,
                    source,
                });
            }
        };
        Ok(Self {
            executor,
            capacity: LegacyOwnerControllerCapacity,
            terminal_grace: OwnerTerminalGraceState::LegacyUntracked,
            progress,
            clock,
            phase: OwnerSessionControllerPhase::Active,
            pending_continuation: None,
            aborted_turn: None,
            recovery: None,
            legless_high_water: None,
            terminal_quarantine: None,
            attach_status: None,
            aborted_recovery: None,
            #[cfg(test)]
            fail_next_recovery_progress: false,
            #[cfg(test)]
            fail_next_continuation_progress: false,
            #[cfg(test)]
            panic_after_terminal_bind_commit: false,
        })
    }
    pub(super) fn output_page_capacity_for_test(
        &self,
    ) -> Result<OwnerOutputPageCapacity, OwnerOutputPageCapacityError> {
        OwnerOutputPageCapacity::checked_from_max_outputs(self.executor.config().max_effect_steps())
    }
}
