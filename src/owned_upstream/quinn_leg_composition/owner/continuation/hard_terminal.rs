//! Root-local hard-terminal state and transaction implementation.
//!
//! Owns terminal state, correlation/quarantine payloads, and the exclusive
//! transaction guard over the parent controller/output root. It does not own
//! stable caller outcomes or whole physical-session settlement.
//!
//! Invariants are one root borrow, exact terminal/stamp correlation, and
//! sticky non-zero quarantine after uncertain interruption. Entry methods live
//! on the parent-owned root; continuation tests and Knife16 provenance lock
//! behavior and source locality.

#[cfg(test)]
use super::super::{
    QuinnOwnerOutputStatus, QuinnOwnerRecoveryTerminalError, QuinnOwnerStandbyRetirementState,
};
use super::QuinnOwnerControllerOutputRoot;
#[cfg(test)]
use super::{
    QuinnOwnerControllerFailureLedger, QuinnOwnerControllerOutputTerminalBeginError,
    QuinnOwnerControllerOutputTerminalCallbackDrive, QuinnOwnerControllerOutputTerminalObservation,
    QuinnOwnerRecoveryServiceState,
};
#[cfg(test)]
use crate::owned_upstream::leg::{EstablishedLeg, ExactLegTerminal, LegTransportTerminalReason};
#[cfg(test)]
use crate::owned_upstream::leg_io::LegOutboundQueue;
#[cfg(test)]
use crate::owned_upstream::owner_output_drain::OwnerOutputDrainInventory;
#[cfg(test)]
use crate::owned_upstream::owner_session_controller::{
    OwnerSessionRecoveryTerminalBound, OwnerSessionRecoveryTerminalDiscarded,
};
#[cfg(test)]
use crate::owned_upstream::owner_target::OwnerTargetRecoveryHandoff;
#[cfg(test)]
use crate::owned_upstream::quinn_leg_actor::{
    ActorTerminalStampWitness, QuinnLegActorReportedTerminal,
};
#[cfg(test)]
use crate::owned_upstream::quinn_leg_composition::QuinnLegCompositionGate;
#[cfg(test)]
use crate::owned_upstream::standby::OwnerStandbySlot;
use crate::owned_upstream::target::TargetIo;

/// The exact terminal handoff is one closed controller/output-root state.
/// A controller bind and its discard may not publish through separate outer
/// fields: once actor consumption begins, every interrupted state remains
/// visibly non-zero here.
#[cfg(test)]
pub(super) enum QuinnOwnerControllerOutputTerminalState {
    Awaiting,
    InFlight {
        entered: bool,
        correlation: QuinnOwnerTerminalCorrelation,
        stamp: Option<ActorTerminalStampWitness>,
        output_audit: OwnerOutputDrainInventory,
    },
    Applied {
        discarded: OwnerSessionRecoveryTerminalDiscarded,
        stamp: ActorTerminalStampWitness,
    },
    StandbyRetired {
        reason: LegTransportTerminalReason,
        stamp: ActorTerminalStampWitness,
    },
    Quarantined {
        correlation: QuinnOwnerTerminalCorrelation,
        stamp: Option<ActorTerminalStampWitness>,
        cause: QuinnOwnerTerminalQuarantineCause,
    },
}

#[cfg(not(test))]
pub(super) enum QuinnOwnerControllerOutputTerminalState {
    Awaiting,
}

#[cfg(test)]
impl QuinnOwnerControllerOutputTerminalState {
    pub(super) fn inventory(
        &self,
        snapshot: crate::owned_upstream::owner_session_controller::OwnerSessionControllerSnapshot,
    ) -> (u8, u8, u8, bool, bool) {
        match self {
            Self::Awaiting => (
                0,
                0,
                0,
                false,
                !snapshot.recovery_needs_terminal_discard
                    && snapshot.pending_legless_high_waters == 0
                    && snapshot.recovery_terminal_quarantined == 0,
            ),
            Self::InFlight {
                correlation, stamp, ..
            } => {
                let correlation_valid = match correlation {
                    QuinnOwnerTerminalCorrelation::Unknown => {
                        !snapshot.recovery_needs_terminal_discard
                            && snapshot.pending_legless_high_waters == 0
                            && snapshot.recovery_terminal_quarantined == 0
                    }
                    QuinnOwnerTerminalCorrelation::Controller(_) => {
                        snapshot.recovery_needs_terminal_discard
                            && snapshot.pending_legless_high_waters == 0
                            && snapshot.recovery_terminal_quarantined == 0
                    }
                    QuinnOwnerTerminalCorrelation::Standby(_) => {
                        !snapshot.recovery_needs_terminal_discard
                            && snapshot.pending_legless_high_waters == 0
                            && snapshot.recovery_terminal_quarantined == 0
                    }
                };
                (1, 1, u8::from(stamp.is_some()), false, correlation_valid)
            }
            Self::Applied { .. } => (
                0,
                1,
                1,
                false,
                !snapshot.recovery_needs_terminal_discard
                    && snapshot.pending_legless_high_waters == 1
                    && snapshot.recovery_terminal_quarantined == 0,
            ),
            Self::StandbyRetired { .. } => (
                0,
                1,
                1,
                false,
                !snapshot.recovery_needs_terminal_discard
                    && snapshot.pending_legless_high_waters == 0
                    && snapshot.recovery_terminal_quarantined == 0,
            ),
            Self::Quarantined { stamp, .. } => (1, 1, u8::from(stamp.is_some()), true, false),
        }
    }
}

#[cfg(not(test))]
impl QuinnOwnerControllerOutputTerminalState {
    pub(super) fn inventory(
        &self,
        snapshot: crate::owned_upstream::owner_session_controller::OwnerSessionControllerSnapshot,
    ) -> (u8, u8, u8, bool, bool) {
        match self {
            Self::Awaiting => (
                0,
                0,
                0,
                false,
                !snapshot.recovery_needs_terminal_discard
                    && snapshot.pending_legless_high_waters == 0
                    && snapshot.recovery_terminal_quarantined == 0,
            ),
        }
    }
}

#[cfg(test)]
#[derive(Clone, Copy)]
pub(super) enum QuinnOwnerTerminalCorrelation {
    Unknown,
    Controller(OwnerSessionRecoveryTerminalBound),
    Standby(LegTransportTerminalReason),
}

#[cfg(test)]
#[derive(Clone, Copy)]
pub(super) enum QuinnOwnerTerminalQuarantineCause {
    ActorUncertain,
    ControllerDiscard,
    IdentityMismatch,
    StandbyMismatch,
    Unwind,
}

#[cfg(test)]
pub(in crate::owned_upstream::quinn_leg_composition::owner) struct QuinnOwnerControllerOutputTerminalTransaction<
    'root,
    T: TargetIo,
> {
    root: &'root mut QuinnOwnerControllerOutputRoot<T>,
}

impl<T: TargetIo> QuinnOwnerControllerOutputRoot<T> {
    #[cfg(test)]
    pub(in crate::owned_upstream::quinn_leg_composition::owner) fn terminal_observation(
        &self,
    ) -> QuinnOwnerControllerOutputTerminalObservation {
        match &self.terminal {
            QuinnOwnerControllerOutputTerminalState::Awaiting => {
                QuinnOwnerControllerOutputTerminalObservation::Awaiting
            }
            QuinnOwnerControllerOutputTerminalState::InFlight { .. } => {
                QuinnOwnerControllerOutputTerminalObservation::InFlight
            }
            QuinnOwnerControllerOutputTerminalState::Applied { discarded, .. } => {
                QuinnOwnerControllerOutputTerminalObservation::Applied {
                    generation: discarded.generation(),
                    reason: discarded.reason(),
                }
            }
            QuinnOwnerControllerOutputTerminalState::StandbyRetired { reason, .. } => {
                QuinnOwnerControllerOutputTerminalObservation::StandbyRetired { reason: *reason }
            }
            QuinnOwnerControllerOutputTerminalState::Quarantined { .. } => {
                QuinnOwnerControllerOutputTerminalObservation::Quarantined {
                    controller_failed: self.controller_failure.is_failed(),
                }
            }
        }
    }

    #[cfg(test)]
    pub(in crate::owned_upstream::quinn_leg_composition::owner) fn begin_terminal_transaction(
        &mut self,
    ) -> Result<
        QuinnOwnerControllerOutputTerminalTransaction<'_, T>,
        QuinnOwnerControllerOutputTerminalBeginError,
    > {
        match self.terminal_observation() {
            QuinnOwnerControllerOutputTerminalObservation::Awaiting => {}
            QuinnOwnerControllerOutputTerminalObservation::InFlight => {
                return Err(QuinnOwnerControllerOutputTerminalBeginError::InFlight);
            }
            QuinnOwnerControllerOutputTerminalObservation::Applied { generation, reason } => {
                return Err(
                    QuinnOwnerControllerOutputTerminalBeginError::AlreadyApplied {
                        generation,
                        reason,
                    },
                );
            }
            QuinnOwnerControllerOutputTerminalObservation::StandbyRetired { reason } => {
                return Err(
                    QuinnOwnerControllerOutputTerminalBeginError::StandbyRetired { reason },
                );
            }
            QuinnOwnerControllerOutputTerminalObservation::Quarantined { controller_failed } => {
                return Err(QuinnOwnerControllerOutputTerminalBeginError::Quarantined {
                    controller_failed,
                });
            }
        }
        if self.controller_failure.is_failed() {
            return Err(QuinnOwnerControllerOutputTerminalBeginError::ControllerFailed);
        }
        if !matches!(self.recovery_service, QuinnOwnerRecoveryServiceState::Idle)
            || matches!(
                self.output.ledger.status,
                QuinnOwnerOutputStatus::OrdinaryInFlight { .. }
                    | QuinnOwnerOutputStatus::ContinuationInFlight { .. }
            )
        {
            return Err(QuinnOwnerControllerOutputTerminalBeginError::ConcurrentTransaction);
        }
        if matches!(
            self.output.ledger.status,
            QuinnOwnerOutputStatus::OrdinaryQuarantined { .. }
                | QuinnOwnerOutputStatus::ContinuationQuarantined { .. }
                | QuinnOwnerOutputStatus::Quarantined { .. }
        ) {
            self.terminal = QuinnOwnerControllerOutputTerminalState::Quarantined {
                correlation: QuinnOwnerTerminalCorrelation::Unknown,
                stamp: None,
                cause: QuinnOwnerTerminalQuarantineCause::ActorUncertain,
            };
            return Err(QuinnOwnerControllerOutputTerminalBeginError::Quarantined {
                controller_failed: false,
            });
        }
        // Until the unified physical no-ACK settlement owns output, rejection,
        // lifecycle, and probe state, a hard terminal may consume the actor's
        // exact envelope only from an already empty output subroot. This is a
        // pre-entry safety gate: no controller or actor ownership has moved.
        let Some(output_audit) = self.output.preflight_hard_terminal_exact_zero() else {
            return Err(QuinnOwnerControllerOutputTerminalBeginError::OutputNotSettled);
        };
        self.terminal = QuinnOwnerControllerOutputTerminalState::InFlight {
            entered: false,
            correlation: QuinnOwnerTerminalCorrelation::Unknown,
            stamp: None,
            output_audit,
        };
        Ok(QuinnOwnerControllerOutputTerminalTransaction { root: self })
    }

    #[cfg(test)]
    pub(in crate::owned_upstream::quinn_leg_composition::owner) fn arm_terminal_bind_postcommit_panic_for_test(
        &mut self,
    ) {
        self.controller
            .arm_terminal_bind_postcommit_panic_for_test();
    }
}

#[cfg(test)]
impl<T: TargetIo> QuinnOwnerControllerOutputTerminalTransaction<'_, T> {
    /// Marks the complete actor call as entered. This happens immediately
    /// before calling the actor transaction, so an unwind anywhere after that
    /// point cannot restore a false-clean Awaiting state.
    pub(in crate::owned_upstream::quinn_leg_composition::owner) fn enter_actor_call(
        &mut self,
    ) -> bool {
        match &mut self.root.terminal {
            QuinnOwnerControllerOutputTerminalState::InFlight {
                entered,
                correlation: QuinnOwnerTerminalCorrelation::Unknown,
                stamp: None,
                ..
            } if !*entered => {
                *entered = true;
                true
            }
            _ => false,
        }
    }

    /// Actor Pending/Rejected is known to retain or restore the exact
    /// envelope. Only that closed poll result may return this root to Awaiting.
    pub(in crate::owned_upstream::quinn_leg_composition::owner) fn restore_actor_retained(
        &mut self,
    ) -> bool {
        let previous = std::mem::replace(
            &mut self.root.terminal,
            QuinnOwnerControllerOutputTerminalState::Quarantined {
                correlation: QuinnOwnerTerminalCorrelation::Unknown,
                stamp: None,
                cause: QuinnOwnerTerminalQuarantineCause::ActorUncertain,
            },
        );
        match previous {
            QuinnOwnerControllerOutputTerminalState::InFlight {
                entered: true,
                correlation: QuinnOwnerTerminalCorrelation::Unknown,
                stamp: None,
                ..
            } => {
                self.root.terminal = QuinnOwnerControllerOutputTerminalState::Awaiting;
                true
            }
            QuinnOwnerControllerOutputTerminalState::InFlight {
                correlation, stamp, ..
            } => {
                self.root.terminal = QuinnOwnerControllerOutputTerminalState::Quarantined {
                    correlation,
                    stamp,
                    cause: QuinnOwnerTerminalQuarantineCause::ActorUncertain,
                };
                false
            }
            state => {
                self.root.terminal = state;
                false
            }
        }
    }

    pub(in crate::owned_upstream::quinn_leg_composition::owner) fn quarantine_actor_uncertain(
        &mut self,
    ) {
        self.quarantine(QuinnOwnerTerminalQuarantineCause::ActorUncertain);
        self.root.output.fail();
    }

    pub(in crate::owned_upstream::quinn_leg_composition::owner) fn apply_reported(
        &mut self,
        active_b: &mut Option<OwnerTargetRecoveryHandoff>,
        slot: &mut Option<OwnerStandbySlot>,
        retirement: &mut QuinnOwnerStandbyRetirementState,
        established_leg: &EstablishedLeg,
        queue: &mut LegOutboundQueue,
        reported: QuinnLegActorReportedTerminal,
        gate: &QuinnLegCompositionGate,
    ) -> Result<
        QuinnOwnerControllerOutputTerminalCallbackDrive,
        (
            QuinnLegActorReportedTerminal,
            QuinnOwnerRecoveryTerminalError,
        ),
    > {
        if !matches!(
            self.root.terminal,
            QuinnOwnerControllerOutputTerminalState::InFlight {
                entered: true,
                correlation: QuinnOwnerTerminalCorrelation::Unknown,
                stamp: None,
                ..
            }
        ) {
            self.quarantine_actor_uncertain();
            return Ok(
                QuinnOwnerControllerOutputTerminalCallbackDrive::Quarantined {
                    controller_failed: false,
                },
            );
        }
        if reported.reason() == LegTransportTerminalReason::LocalCancelled {
            return Err((
                reported,
                QuinnOwnerRecoveryTerminalError::UnexpectedLocalCancellation,
            ));
        }
        if reported.failure_observed_at().is_none() {
            return Err((
                reported,
                QuinnOwnerRecoveryTerminalError::MissingFailureTimestamp,
            ));
        }

        let snapshot = self.root.controller.snapshot();
        if active_b.is_some() && snapshot.pending_recoveries != 0 {
            return Err((
                reported,
                QuinnOwnerRecoveryTerminalError::CompositionInvariant,
            ));
        }
        if active_b.is_none()
            && snapshot.pending_recoveries == 0
            && snapshot.pending_attach_statuses == 0
            && !retirement.is_unsettled()
        {
            return Err((
                reported,
                QuinnOwnerRecoveryTerminalError::CompositionInvariant,
            ));
        }

        if !self.commit_hard_terminal_output_stop() {
            self.quarantine(QuinnOwnerTerminalQuarantineCause::ActorUncertain);
            return Err((
                reported,
                QuinnOwnerRecoveryTerminalError::CompositionInvariant,
            ));
        }

        let (terminal, stamp) = reported.into_parts();
        if let Err(stamp) = self.stage_binding_stamp(stamp) {
            self.quarantine(QuinnOwnerTerminalQuarantineCause::ActorUncertain);
            return Err((
                QuinnLegActorReportedTerminal::restore(terminal, stamp),
                QuinnOwnerRecoveryTerminalError::CompositionInvariant,
            ));
        }
        if active_b.is_some() || snapshot.pending_recoveries != 0 {
            let bound = if active_b.is_some() {
                self.root.controller.bind_active_terminal(
                    active_b,
                    terminal,
                    established_leg,
                    queue,
                    gate,
                )
            } else {
                self.root
                    .controller
                    .bind_recovery_terminal(terminal, established_leg, queue, gate)
            };
            let bound = match bound {
                Ok(bound) => bound,
                Err(failure) => {
                    let kind = failure.kind();
                    return self.reject_precommit(
                        failure.into_terminal(),
                        QuinnOwnerRecoveryTerminalError::ControllerBind(kind),
                    );
                }
            };
            if !self.stage_controller_bound(bound) {
                self.root.controller_failure = QuinnOwnerControllerFailureLedger::Invariant;
                self.root.output.fail();
                return Ok(
                    QuinnOwnerControllerOutputTerminalCallbackDrive::Quarantined {
                        controller_failed: false,
                    },
                );
            }
            return match self.root.controller.discard_recovery_terminal(gate) {
                Ok(discarded) => Ok(self.close_controller_applied(bound, discarded)),
                Err(error) => {
                    debug_assert!(matches!(
                        self.root.controller_failure,
                        QuinnOwnerControllerFailureLedger::Ready
                    ));
                    self.root.controller_failure = QuinnOwnerControllerFailureLedger::Failed(error);
                    self.quarantine(QuinnOwnerTerminalQuarantineCause::ControllerDiscard);
                    self.root.output.fail();
                    Ok(
                        QuinnOwnerControllerOutputTerminalCallbackDrive::Quarantined {
                            controller_failed: true,
                        },
                    )
                }
            };
        }

        if snapshot.pending_attach_statuses != 0 {
            return self.reject_precommit(
                terminal,
                QuinnOwnerRecoveryTerminalError::ControllerBind(
                    crate::owned_upstream::owner_session_controller::OwnerSessionRecoveryTerminalBindErrorKind::NoPendingRecovery,
                ),
            );
        }
        let reason = terminal.reason();
        let Some(cleanup) = queue.begin_owner_terminal_cleanup(established_leg, &terminal) else {
            return self.reject_precommit(
                terminal,
                QuinnOwnerRecoveryTerminalError::CompositionInvariant,
            );
        };
        let Some(slot) = slot.as_mut() else {
            return self.reject_precommit(
                terminal,
                QuinnOwnerRecoveryTerminalError::CompositionInvariant,
            );
        };
        let retired = match slot.retire_exact_terminal(terminal) {
            Ok(retired) => retired,
            Err(failure) => {
                return self.reject_precommit(
                    failure.into_terminal(),
                    QuinnOwnerRecoveryTerminalError::CompositionInvariant,
                );
            }
        };
        if !self.stage_standby_bound(reason) {
            drop(retired);
            self.root.output.fail();
            return Ok(
                QuinnOwnerControllerOutputTerminalCallbackDrive::Quarantined {
                    controller_failed: false,
                },
            );
        }
        let queue_discarded = cleanup.commit();
        if !retirement.install_physical_prejoin(reason, retired, queue_discarded) {
            self.quarantine(QuinnOwnerTerminalQuarantineCause::StandbyMismatch);
            self.root.output.fail();
            return Ok(
                QuinnOwnerControllerOutputTerminalCallbackDrive::Quarantined {
                    controller_failed: false,
                },
            );
        }
        Ok(self.close_standby_retired(reason))
    }

    fn stage_binding_stamp(
        &mut self,
        stamp: ActorTerminalStampWitness,
    ) -> Result<(), ActorTerminalStampWitness> {
        match &mut self.root.terminal {
            QuinnOwnerControllerOutputTerminalState::InFlight {
                entered: true,
                correlation: QuinnOwnerTerminalCorrelation::Unknown,
                stamp: root_stamp,
                ..
            } if root_stamp.is_none() => {
                *root_stamp = Some(stamp);
                Ok(())
            }
            _ => Err(stamp),
        }
    }

    fn reject_precommit(
        &mut self,
        terminal: ExactLegTerminal,
        error: QuinnOwnerRecoveryTerminalError,
    ) -> Result<
        QuinnOwnerControllerOutputTerminalCallbackDrive,
        (
            QuinnLegActorReportedTerminal,
            QuinnOwnerRecoveryTerminalError,
        ),
    > {
        let stamp = match &mut self.root.terminal {
            QuinnOwnerControllerOutputTerminalState::InFlight {
                entered: true,
                correlation: QuinnOwnerTerminalCorrelation::Unknown,
                stamp,
                ..
            } => stamp.take(),
            _ => None,
        };
        let Some(stamp) = stamp else {
            self.quarantine(QuinnOwnerTerminalQuarantineCause::ActorUncertain);
            drop(terminal);
            return Ok(
                QuinnOwnerControllerOutputTerminalCallbackDrive::Quarantined {
                    controller_failed: false,
                },
            );
        };
        Err((
            QuinnLegActorReportedTerminal::restore(terminal, stamp),
            error,
        ))
    }

    fn stage_controller_bound(&mut self, bound: OwnerSessionRecoveryTerminalBound) -> bool {
        self.stage_bound(QuinnOwnerTerminalCorrelation::Controller(bound))
    }

    fn stage_standby_bound(&mut self, reason: LegTransportTerminalReason) -> bool {
        self.stage_bound(QuinnOwnerTerminalCorrelation::Standby(reason))
    }

    fn stage_bound(&mut self, correlation: QuinnOwnerTerminalCorrelation) -> bool {
        match &mut self.root.terminal {
            QuinnOwnerControllerOutputTerminalState::InFlight {
                entered: true,
                correlation: root_correlation,
                stamp: Some(_),
                ..
            } if matches!(root_correlation, QuinnOwnerTerminalCorrelation::Unknown) => {
                *root_correlation = correlation;
                true
            }
            _ => {
                self.quarantine(QuinnOwnerTerminalQuarantineCause::ActorUncertain);
                false
            }
        }
    }

    fn commit_hard_terminal_output_stop(&mut self) -> bool {
        let output_audit = match self.root.terminal {
            QuinnOwnerControllerOutputTerminalState::InFlight {
                entered: true,
                correlation: QuinnOwnerTerminalCorrelation::Unknown,
                stamp: None,
                output_audit,
            } => output_audit,
            _ => return false,
        };
        self.root
            .output
            .commit_preflighted_hard_terminal_exact_zero(output_audit)
    }

    fn close_controller_applied(
        &mut self,
        bound: OwnerSessionRecoveryTerminalBound,
        discarded: OwnerSessionRecoveryTerminalDiscarded,
    ) -> QuinnOwnerControllerOutputTerminalCallbackDrive {
        let previous = self.take_for_close();
        let QuinnOwnerControllerOutputTerminalState::InFlight {
            entered: true,
            correlation: QuinnOwnerTerminalCorrelation::Controller(staged),
            stamp: Some(stamp),
            ..
        } = previous
        else {
            self.root.controller_failure = QuinnOwnerControllerFailureLedger::Invariant;
            self.root.output.fail();
            return QuinnOwnerControllerOutputTerminalCallbackDrive::Quarantined {
                controller_failed: false,
            };
        };
        if staged != bound
            || discarded.generation() != bound.generation()
            || discarded.reason() != bound.reason()
        {
            self.root.terminal = QuinnOwnerControllerOutputTerminalState::Quarantined {
                correlation: QuinnOwnerTerminalCorrelation::Controller(bound),
                stamp: Some(stamp),
                cause: QuinnOwnerTerminalQuarantineCause::IdentityMismatch,
            };
            self.root.controller_failure = QuinnOwnerControllerFailureLedger::Invariant;
            self.root.output.fail();
            return QuinnOwnerControllerOutputTerminalCallbackDrive::Quarantined {
                controller_failed: false,
            };
        }
        let generation = discarded.generation();
        let reason = discarded.reason();
        self.root.terminal = QuinnOwnerControllerOutputTerminalState::Applied { discarded, stamp };
        QuinnOwnerControllerOutputTerminalCallbackDrive::Applied { generation, reason }
    }

    fn close_standby_retired(
        &mut self,
        reason: LegTransportTerminalReason,
    ) -> QuinnOwnerControllerOutputTerminalCallbackDrive {
        let previous = self.take_for_close();
        let QuinnOwnerControllerOutputTerminalState::InFlight {
            entered: true,
            correlation: QuinnOwnerTerminalCorrelation::Standby(staged_reason),
            stamp: Some(stamp),
            ..
        } = previous
        else {
            self.root.output.fail();
            return QuinnOwnerControllerOutputTerminalCallbackDrive::Quarantined {
                controller_failed: false,
            };
        };
        if staged_reason != reason {
            self.root.terminal = QuinnOwnerControllerOutputTerminalState::Quarantined {
                correlation: QuinnOwnerTerminalCorrelation::Standby(reason),
                stamp: Some(stamp),
                cause: QuinnOwnerTerminalQuarantineCause::StandbyMismatch,
            };
            self.root.output.fail();
            return QuinnOwnerControllerOutputTerminalCallbackDrive::Quarantined {
                controller_failed: false,
            };
        }
        self.root.terminal =
            QuinnOwnerControllerOutputTerminalState::StandbyRetired { reason, stamp };
        QuinnOwnerControllerOutputTerminalCallbackDrive::StandbyRetired { reason }
    }

    fn take_for_close(&mut self) -> QuinnOwnerControllerOutputTerminalState {
        std::mem::replace(
            &mut self.root.terminal,
            QuinnOwnerControllerOutputTerminalState::Quarantined {
                correlation: QuinnOwnerTerminalCorrelation::Unknown,
                stamp: None,
                cause: QuinnOwnerTerminalQuarantineCause::ActorUncertain,
            },
        )
    }

    fn quarantine(&mut self, cause: QuinnOwnerTerminalQuarantineCause) {
        let previous = self.take_for_close();
        self.root.terminal = match previous {
            QuinnOwnerControllerOutputTerminalState::InFlight {
                correlation, stamp, ..
            } => QuinnOwnerControllerOutputTerminalState::Quarantined {
                correlation,
                stamp,
                cause,
            },
            QuinnOwnerControllerOutputTerminalState::Quarantined {
                correlation,
                stamp,
                cause,
            } => QuinnOwnerControllerOutputTerminalState::Quarantined {
                correlation,
                stamp,
                cause,
            },
            state => state,
        };
    }
}

#[cfg(test)]
impl<T: TargetIo> Drop for QuinnOwnerControllerOutputTerminalTransaction<'_, T> {
    fn drop(&mut self) {
        let previous = std::mem::replace(
            &mut self.root.terminal,
            QuinnOwnerControllerOutputTerminalState::Quarantined {
                correlation: QuinnOwnerTerminalCorrelation::Unknown,
                stamp: None,
                cause: QuinnOwnerTerminalQuarantineCause::Unwind,
            },
        );
        self.root.terminal = match previous {
            QuinnOwnerControllerOutputTerminalState::InFlight {
                entered: false,
                correlation: QuinnOwnerTerminalCorrelation::Unknown,
                stamp: None,
                ..
            } => QuinnOwnerControllerOutputTerminalState::Awaiting,
            QuinnOwnerControllerOutputTerminalState::InFlight {
                correlation, stamp, ..
            } => QuinnOwnerControllerOutputTerminalState::Quarantined {
                correlation,
                stamp,
                cause: QuinnOwnerTerminalQuarantineCause::Unwind,
            },
            state => state,
        };
    }
}
