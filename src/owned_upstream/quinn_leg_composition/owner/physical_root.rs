//! Test-only physical owner adapter kept as one deep composition slice.
//!
//! This module owns the registered-B/controller/output transaction used by the
//! deterministic R7 harness. It does not own the future production physical
//! root, public TUN selection, or whole-root no-ACK settlement.

use super::*;

/// Deep physical owner of registered B, the only owner controller, every
/// recovery continuation, and the final active-B capability. Public drives
/// are value-only; no AttachedLeg or raw recovery cursor can escape.
#[must_use = "the physical owner adapter requires an explicit fatal, cancel, or shutdown stop"]
#[cfg(test)]
pub(crate) struct QuinnOwnerRecoverySessionComposition<T: TargetIo> {
    standby: QuinnOwnerStandbySessionComposition,
    controller_output: QuinnOwnerControllerOutputRoot<T>,
    active_b: Option<OwnerTargetRecoveryHandoff>,
}
#[cfg(test)]
impl<T: TargetIo> QuinnOwnerRecoverySessionComposition<T> {
    pub(crate) fn adopt(
        standby: QuinnOwnerStandbySessionComposition,
        controller: OwnerSessionController<T>,
    ) -> Result<Self, QuinnOwnerRecoveryAdoptFailure<T>> {
        let gate = QuinnLegCompositionGate::mint();
        if !controller.shares_session_clock(&standby.clock, &gate) {
            return Err(QuinnOwnerRecoveryAdoptFailure {
                kind: QuinnOwnerRecoveryAdoptErrorKind::WrongSessionClock,
                ownership: Box::new((standby, controller)),
            });
        }
        let controller_output = match QuinnOwnerControllerOutputRoot::new(controller) {
            Ok(root) => root,
            Err(failure) => {
                let (controller, source) = failure.into_parts();
                return Err(QuinnOwnerRecoveryAdoptFailure {
                    kind: QuinnOwnerRecoveryAdoptErrorKind::OutputDrain(source),
                    ownership: Box::new((standby, controller)),
                });
            }
        };
        Ok(Self {
            standby,
            controller_output,
            active_b: None,
        })
    }

    pub(crate) fn active_generation(&self) -> Option<LegGeneration> {
        self.active_b
            .as_ref()
            .map(OwnerTargetRecoveryHandoff::generation)
    }

    pub(crate) fn output_root_inventory(&self) -> QuinnOwnerOutputRootInventory {
        let terminal = self.controller_output.terminal_observation();
        let standby_terminal_correlation_valid = match terminal {
            QuinnOwnerControllerOutputTerminalObservation::Awaiting
            | QuinnOwnerControllerOutputTerminalObservation::Applied { .. } => {
                self.standby.retirement.is_unsettled()
            }
            QuinnOwnerControllerOutputTerminalObservation::StandbyRetired { reason } => {
                self.standby.retirement.physical_reason() == Some(reason)
            }
            QuinnOwnerControllerOutputTerminalObservation::InFlight => true,
            QuinnOwnerControllerOutputTerminalObservation::Quarantined { .. } => false,
        };
        self.controller_output
            .output_inventory()
            .with_standby_terminal_correlation(standby_terminal_correlation_valid)
    }

    pub(crate) fn arm_terminal_bind_postcommit_panic_for_test(&mut self) {
        self.controller_output
            .arm_terminal_bind_postcommit_panic_for_test();
    }

    fn fail_output_drain(&mut self) {
        self.controller_output.fail_output();
    }

    pub(crate) fn stop_output_for_cancel(&mut self) -> QuinnOwnerOutputStopDrive {
        self.stop_output(OwnerOutputDrainStopReason::Cancelled)
    }

    pub(crate) fn stop_output_for_shutdown(&mut self) -> QuinnOwnerOutputStopDrive {
        self.stop_output(OwnerOutputDrainStopReason::Shutdown)
    }

    fn stop_output(&mut self, requested: OwnerOutputDrainStopReason) -> QuinnOwnerOutputStopDrive {
        match self.controller_output.stop_output(requested) {
            QuinnOwnerOutputStopDrive::Stopped { reason, .. } => {
                QuinnOwnerOutputStopDrive::Stopped {
                    reason,
                    inventory: self.output_root_inventory(),
                }
            }
            QuinnOwnerOutputStopDrive::AlreadyStopped { reason, .. } => {
                QuinnOwnerOutputStopDrive::AlreadyStopped {
                    reason,
                    inventory: self.output_root_inventory(),
                }
            }
            QuinnOwnerOutputStopDrive::CompositionInvariant { .. } => {
                QuinnOwnerOutputStopDrive::CompositionInvariant {
                    inventory: self.output_root_inventory(),
                }
            }
        }
    }

    /// Lets the future full owner root synchronously transform the exact
    /// pending action into its typed live timer/phase slot. The callback sees
    /// a non-cloneable borrow and clearing occurs only after `Commit`.
    pub(crate) fn transact_installed_lifecycle<'owner, 'slot>(
        &'owner mut self,
        commit: impl FnOnce(
            OwnerOutputLifecycleTransaction<'owner>,
        ) -> OwnerOutputLifecycleResolution<'owner, 'slot>,
    ) -> OwnerOutputLifecycleDrive {
        self.controller_output.transact_installed_lifecycle(commit)
    }

    /// Terminally accounts for the exact pending action after the output
    /// drain has stopped. The discard reason is derived from the sticky stop
    /// ledger; callers cannot select or rewrite it.
    pub(crate) fn transact_stopped_lifecycle<'owner, 'slot>(
        &'owner mut self,
        commit: impl FnOnce(
            OwnerOutputLifecycleTransaction<'owner>,
        ) -> OwnerOutputLifecycleResolution<'owner, 'slot>,
    ) -> OwnerOutputLifecycleDrive {
        self.controller_output.transact_stopped_lifecycle(commit)
    }

    pub(crate) fn try_receive_owner_b(&mut self) -> QuinnOwnerRecoveryInboundDrive {
        if self.controller_output.controller_failed() {
            return QuinnOwnerRecoveryInboundDrive::ControllerFailed;
        }
        let gate = QuinnLegCompositionGate::mint();
        let clock = self.standby.clock.clone();
        let fresh_for = self.standby.liveness.freshness_window;
        if let Some(error) = self.standby.actor_unavailable_error() {
            self.fail_output_drain();
            return QuinnOwnerRecoveryInboundDrive::ActorFailed(error);
        }
        let transaction = {
            let Some(QuinnOwnerStandbyActorOwner::Running(actor)) = self.standby.actor.as_mut()
            else {
                self.fail_output_drain();
                return QuinnOwnerRecoveryInboundDrive::CompositionInvariant;
            };
            let actor = actor.as_mut();
            let Some(slot) = self.standby.slot.as_mut() else {
                return QuinnOwnerRecoveryInboundDrive::CompositionInvariant;
            };
            let Some(controller_output) = self.controller_output.begin_inbound_admission() else {
                return QuinnOwnerRecoveryInboundDrive::CompositionInvariant;
            };
            let mut context = QuinnOwnerRecoveryInboundContext {
                controller_output: Some(controller_output),
                slot,
                active_b: &mut self.active_b,
            };
            actor.try_transact_controller_inbound_with_parts(
                &gate,
                &mut context,
                |context, io, queue| {
                    if !context
                        .controller_output
                        .as_ref()
                        .is_some_and(
                            QuinnOwnerControllerOutputInboundTransaction::can_admit_ordinary,
                        )
                    {
                        return Ok(QuinnLegActorInboundPreflight::Blocked);
                    }
                    if context.active_b.is_none()
                        && !context
                            .slot
                            .registered_matches_actor(io.established_leg(), queue)
                    {
                        return Err(QuinnLegActorError::Protocol(
                            "owner recovery slot does not match physical B",
                        ));
                    }
                    let Some(controller_output) = context.controller_output.as_ref() else {
                        return Err(QuinnLegActorError::Protocol(
                            "owner controller-output transaction was already consumed",
                        ));
                    };
                    match controller_output.preflight_registered_recovery() {
                        Ok(ready) => Ok(QuinnLegActorInboundPreflight::Ready(ready)),
                        Err(_block) => Ok(QuinnLegActorInboundPreflight::Blocked),
                    }
                },
                |context, ready, envelope, io, queue| {
                    let (delivery, arrived_at) = envelope.into_parts();
                    let inbound = io.receive_classified_delivery(delivery, STANDBY_CONTROL_V1)?;
                    match inbound {
                        BoundInbound::LegControl(inbound) => {
                            let Some(controller_output) = context.controller_output.take() else {
                                return Err(QuinnLegActorError::Protocol(
                                    "owner controller-output transaction was already consumed",
                                ));
                            };
                            let outcome = match context.slot.observe_exact_probe_ack(
                                BoundInbound::LegControl(inbound),
                                arrived_at,
                                fresh_for,
                            ) {
                                Ok(fresh) => QuinnOwnerStandbyInboundDrive::ProbeFresh {
                                    probe_sequence: fresh.probe_sequence(),
                                    fresh_until: fresh.fresh_until(),
                                },
                                Err(failure) => {
                                    let kind = failure.kind();
                                    drop(failure.into_inbound());
                                    QuinnOwnerStandbyInboundDrive::Rejected(kind)
                                }
                            };
                            controller_output.restore_unstarted();
                            Ok(QuinnOwnerRecoveryClassified::Probe(outcome))
                        }
                        BoundInbound::Session(received) => {
                            let Some(controller_output) = context.controller_output.take() else {
                                return Err(QuinnLegActorError::Protocol(
                                    "owner controller-output transaction was already consumed",
                                ));
                            };
                            if let Some(active) = context.active_b.as_ref() {
                                let event = active.accept_frame(received).map_err(|_| {
                                    QuinnLegActorError::Protocol(
                                        "active owner B rejected its bound session frame",
                                    )
                                })?;
                                let arrival = QuinnOwnerActorArrival::from_actor_capture(
                                    &clock,
                                    arrived_at,
                                    &gate,
                                );
                                let input = crate::owned_upstream::owner_session_controller::OwnerSessionActorEvent::from_quinn_actor(
                                    event,
                                    arrival,
                                    &gate,
                                );
                                controller_output
                                    .apply_actor_event_and_admit(input)
                                    .map(QuinnOwnerRecoveryClassified::Ordinary)
                                    .map_err(|()| {
                                        QuinnLegActorError::Protocol(
                                            "owner session composition failed",
                                        )
                                    })
                            } else {
                                controller_output
                                    .accept_registered_owner_attach(
                                        ready,
                                        context.slot,
                                        io.established_leg(),
                                        received,
                                        queue,
                                        &gate,
                                    )
                                    .map(QuinnOwnerRecoveryClassified::Attach)
                                    .map_err(|()| {
                                        QuinnLegActorError::Protocol(
                                            "owner session composition failed",
                                        )
                                    })
                            }
                        }
                    }
                },
            )
        };
        match transaction {
            Ok(QuinnLegActorInboundPoll::Idle) => QuinnOwnerRecoveryInboundDrive::Idle,
            Ok(QuinnLegActorInboundPoll::Blocked) => QuinnOwnerRecoveryInboundDrive::Blocked,
            Ok(QuinnLegActorInboundPoll::Classified(QuinnOwnerRecoveryClassified::Probe(
                outcome,
            ))) => match self.standby.finish_classified_probe(outcome) {
                QuinnOwnerStandbyInboundDrive::ProbeFresh {
                    probe_sequence,
                    fresh_until,
                } => QuinnOwnerRecoveryInboundDrive::ProbeFresh {
                    probe_sequence,
                    fresh_until,
                },
                QuinnOwnerStandbyInboundDrive::Rejected(kind) => {
                    QuinnOwnerRecoveryInboundDrive::ProbeRejected(kind)
                }
                QuinnOwnerStandbyInboundDrive::CompositionInvariant => {
                    QuinnOwnerRecoveryInboundDrive::CompositionInvariant
                }
                QuinnOwnerStandbyInboundDrive::ActorFailed(error) => {
                    QuinnOwnerRecoveryInboundDrive::ActorFailed(error)
                }
                QuinnOwnerStandbyInboundDrive::Idle => QuinnOwnerRecoveryInboundDrive::Idle,
            },
            Ok(QuinnLegActorInboundPoll::Classified(QuinnOwnerRecoveryClassified::Attach(
                OwnerSessionRegisteredAttachDrive::Recovery(drive),
            ))) => QuinnOwnerRecoveryInboundDrive::RecoveryAccepted {
                needs_execute: drive.needs_execute(),
            },
            Ok(QuinnLegActorInboundPoll::Classified(QuinnOwnerRecoveryClassified::Attach(
                OwnerSessionRegisteredAttachDrive::AcceptancePending { kind },
            ))) => {
                drop(kind);
                QuinnOwnerRecoveryInboundDrive::AcceptancePending
            }
            Ok(QuinnLegActorInboundPoll::Classified(QuinnOwnerRecoveryClassified::Attach(
                OwnerSessionRegisteredAttachDrive::StatusQueued,
            ))) => QuinnOwnerRecoveryInboundDrive::StatusQueued,
            Ok(QuinnLegActorInboundPoll::Classified(QuinnOwnerRecoveryClassified::Attach(
                OwnerSessionRegisteredAttachDrive::StatusPending { kind },
            ))) => {
                drop(kind);
                QuinnOwnerRecoveryInboundDrive::StatusPending
            }
            Ok(QuinnLegActorInboundPoll::Classified(QuinnOwnerRecoveryClassified::Ordinary(
                applied,
            ))) => QuinnOwnerRecoveryInboundDrive::Ordinary {
                outputs: applied.outputs,
                needs_continue: applied.needs_continue,
            },
            Err(error) => {
                if self.controller_output.controller_failed() {
                    QuinnOwnerRecoveryInboundDrive::ControllerFailed
                } else if self.controller_output.admission_failed() {
                    QuinnOwnerRecoveryInboundDrive::OutputRejected
                } else {
                    self.fail_output_drain();
                    QuinnOwnerRecoveryInboundDrive::ActorFailed(error)
                }
            }
        }
    }

    /// Services exactly one ordinary output or one retained continuation.
    /// Every transport admission happens while borrowing the physical actor's
    /// exact queue through its first-failure closure. A continuation turn can
    /// begin only on a later call after the previous page emitted its
    /// `PageDrained(true)` marker.
    pub(crate) fn service_output_once(&mut self) -> QuinnOwnerOutputServiceDrive {
        let (standby, controller_output) = (&mut self.standby, &mut self.controller_output);
        let (drive, fail_output_after_turn) = 'turn: {
            let turn = controller_output.begin_service_turn();
            match turn {
                QuinnOwnerOutputServiceTurn::Inert => (QuinnOwnerOutputServiceDrive::Inert, false),
                QuinnOwnerOutputServiceTurn::CompletedBoundary => (
                    QuinnOwnerOutputServiceDrive::PageDrained {
                        needs_continue: false,
                    },
                    false,
                ),
                QuinnOwnerOutputServiceTurn::LifecycleInstallBlocked => {
                    (QuinnOwnerOutputServiceDrive::LifecycleInstallBlocked, false)
                }
                QuinnOwnerOutputServiceTurn::ControllerFailed => {
                    (QuinnOwnerOutputServiceDrive::ControllerFailed, false)
                }
                QuinnOwnerOutputServiceTurn::Stopped { reason, inventory } => (
                    QuinnOwnerOutputServiceDrive::Stopped { reason, inventory },
                    false,
                ),
                QuinnOwnerOutputServiceTurn::CompositionInvariant => {
                    (QuinnOwnerOutputServiceDrive::CompositionInvariant, false)
                }
                QuinnOwnerOutputServiceTurn::Continue(transaction) => {
                    let gate = QuinnLegCompositionGate::mint();
                    let continued = match standby.running_actor_mut() {
                        Ok(actor) => actor.with_controller_parts_mut_input(
                            &gate,
                            transaction,
                            |transaction, _io, _queue| transaction.continue_and_settle(),
                        ),
                        Err(_) => {
                            transaction.restore_unstarted();
                            break 'turn (QuinnOwnerOutputServiceDrive::ActorUnavailable, true);
                        }
                    };
                    match continued {
                        Err((transaction, _actor_error)) => {
                            transaction.restore_unstarted();
                            standby.request_controller_failure_if_running();
                            (QuinnOwnerOutputServiceDrive::ActorUnavailable, true)
                        }
                        Ok(QuinnOwnerOutputContinuationDrive::Admitted {
                            outputs,
                            needs_continue,
                        }) => (
                            QuinnOwnerOutputServiceDrive::Continued {
                                outputs,
                                needs_continue,
                            },
                            false,
                        ),
                        Ok(QuinnOwnerOutputContinuationDrive::ControllerFailed) => {
                            standby.request_controller_failure_if_running();
                            (QuinnOwnerOutputServiceDrive::ControllerFailed, true)
                        }
                        Ok(QuinnOwnerOutputContinuationDrive::ControllerMissing) => {
                            standby.request_controller_failure_if_running();
                            (QuinnOwnerOutputServiceDrive::CompositionInvariant, true)
                        }
                        Ok(
                            QuinnOwnerOutputContinuationDrive::AdmissionRetained
                            | QuinnOwnerOutputContinuationDrive::CompositionInvariant,
                        ) => {
                            standby.request_controller_failure_if_running();
                            (QuinnOwnerOutputServiceDrive::CompositionInvariant, true)
                        }
                    }
                }
                QuinnOwnerOutputServiceTurn::Drive(transaction) => {
                    let gate = QuinnLegCompositionGate::mint();
                    let driven = match standby.running_actor_mut() {
                        Ok(actor) => actor.with_controller_parts_mut(&gate, |_io, queue| {
                            transaction.drive(queue)
                        }),
                        Err(_) => {
                            drop(transaction);
                            break 'turn (QuinnOwnerOutputServiceDrive::ActorUnavailable, true);
                        }
                    };
                    let driven = match driven {
                        Some(Ok(driven)) => driven,
                        None => {
                            standby.request_controller_failure_if_running();
                            break 'turn (QuinnOwnerOutputServiceDrive::ActorUnavailable, true);
                        }
                        Some(Err(_audit)) => {
                            standby.request_controller_failure_if_running();
                            break 'turn (QuinnOwnerOutputServiceDrive::CompositionInvariant, true);
                        }
                    };
                    match driven {
                        OwnerOutputDrainDrive::Idle => {
                            standby.request_controller_failure_if_running();
                            (QuinnOwnerOutputServiceDrive::CompositionInvariant, true)
                        }
                        OwnerOutputDrainDrive::Blocked { reason } if reason.is_retryable() => {
                            (QuinnOwnerOutputServiceDrive::Pressure, false)
                        }
                        OwnerOutputDrainDrive::Blocked { reason: _ } => {
                            standby.request_controller_failure_if_running();
                            (QuinnOwnerOutputServiceDrive::ControllerFailed, true)
                        }
                        OwnerOutputDrainDrive::LifecycleInstallBlocked => {
                            (QuinnOwnerOutputServiceDrive::LifecycleInstallBlocked, false)
                        }
                        OwnerOutputDrainDrive::Transmitted { page_drained } => (
                            QuinnOwnerOutputServiceDrive::Transmitted {
                                page_drained: page_drained.is_some(),
                            },
                            false,
                        ),
                        OwnerOutputDrainDrive::LifecycleInstalled { page_drained } => (
                            QuinnOwnerOutputServiceDrive::LifecycleInstalled {
                                page_drained: page_drained.is_some(),
                            },
                            false,
                        ),
                        OwnerOutputDrainDrive::PageDrained(page) => (
                            QuinnOwnerOutputServiceDrive::PageDrained {
                                needs_continue: page.needs_continue(),
                            },
                            false,
                        ),
                    }
                }
            }
        };
        if fail_output_after_turn {
            controller_output.fail_output();
        }
        drive
    }

    /// Legacy B-only adapter retained for deterministic compatibility tests.
    /// Production physical composition must retire exact A in the same
    /// transaction that commits B and therefore cannot call this policy-only
    /// installer.
    #[cfg(test)]
    pub(crate) fn service_recovery_once(&mut self) -> QuinnOwnerRecoveryServiceDrive {
        if self.controller_output.controller_failed() {
            self.fail_output_drain();
            return QuinnOwnerRecoveryServiceDrive::ControllerFailed;
        }
        let gate = QuinnLegCompositionGate::mint();
        let (standby, controller_output, active_b) = (
            &mut self.standby,
            &mut self.controller_output,
            &mut self.active_b,
        );
        let actor = match standby.running_actor_mut() {
            Ok(actor) => actor,
            Err(_) => {
                controller_output.fail_output();
                return QuinnOwnerRecoveryServiceDrive::ActorUnavailable;
            }
        };
        let stepped = actor.with_controller_parts_mut_result(&gate, |io, queue| {
            controller_output.service_recovery_with_parts(
                active_b,
                io.established_leg(),
                queue,
                &gate,
            )
        });
        match stepped {
            Ok(drive) => drive,
            Err(_) if controller_output.controller_failed() => {
                controller_output.fail_output();
                QuinnOwnerRecoveryServiceDrive::ControllerFailed
            }
            Err(_) => {
                controller_output.fail_output();
                QuinnOwnerRecoveryServiceDrive::ActorUnavailable
            }
        }
    }

    pub(crate) fn try_flush(
        &mut self,
        now: SimTime,
    ) -> Result<QuinnLegActorFlushDisposition, QuinnLegActorError> {
        let flushed = self.standby.try_flush(now);
        if flushed.is_err() {
            self.fail_output_drain();
        }
        flushed
    }

    #[cfg(test)]
    pub(crate) async fn join_physical_actor_after_terminal_for_test(
        &mut self,
    ) -> Result<QuinnOwnerStandbyJoined, QuinnLegActorError> {
        let _ = self.stop_output_for_shutdown();
        self.standby.join_for_shutdown().await
    }

    /// Moves the physical B actor's exact hard terminal into either the
    /// installed/recovering controller capability or the still-registered
    /// standby slot. Pre-bind rejection restores the complete actor envelope;
    /// post-bind retry never reconstructs or re-applies `LegLost`.
    pub(crate) fn try_drive_reported_terminal(
        &mut self,
    ) -> Result<QuinnOwnerRecoveryTerminalDrive, QuinnOwnerRecoveryTerminalError> {
        // The current B-only adapter does not yet own the unified physical
        // timer settlement. Refuse to consume the actor terminal while a live
        // probe timer remains outside the controller/output transaction.
        if self.standby.armed_probe_timer.is_some() {
            return Err(QuinnOwnerRecoveryTerminalError::CompositionInvariant);
        }
        let gate = QuinnLegCompositionGate::mint();
        let Self {
            standby,
            controller_output,
            active_b,
        } = self;
        let mut terminal = match controller_output.begin_terminal_transaction() {
            Ok(transaction) => transaction,
            Err(QuinnOwnerControllerOutputTerminalBeginError::AlreadyApplied {
                generation,
                reason,
            }) => {
                return Ok(QuinnOwnerRecoveryTerminalDrive::AlreadyApplied { generation, reason });
            }
            Err(QuinnOwnerControllerOutputTerminalBeginError::StandbyRetired { .. }) => {
                return Ok(QuinnOwnerRecoveryTerminalDrive::StandbyRetired);
            }
            Err(QuinnOwnerControllerOutputTerminalBeginError::ControllerFailed) => {
                return Err(QuinnOwnerRecoveryTerminalError::ControllerDiscard);
            }
            Err(QuinnOwnerControllerOutputTerminalBeginError::Quarantined {
                controller_failed: true,
            }) => {
                return Err(QuinnOwnerRecoveryTerminalError::ControllerDiscard);
            }
            Err(
                QuinnOwnerControllerOutputTerminalBeginError::InFlight
                | QuinnOwnerControllerOutputTerminalBeginError::Quarantined {
                    controller_failed: false,
                }
                | QuinnOwnerControllerOutputTerminalBeginError::ConcurrentTransaction
                | QuinnOwnerControllerOutputTerminalBeginError::OutputNotSettled,
            ) => {
                return Err(QuinnOwnerRecoveryTerminalError::CompositionInvariant);
            }
        };
        let QuinnOwnerStandbySessionComposition {
            actor,
            slot,
            armed_probe_timer,
            retirement,
            ..
        } = standby;
        let actor = match actor.as_mut() {
            Some(QuinnOwnerStandbyActorOwner::Running(actor)) => actor.as_mut(),
            Some(QuinnOwnerStandbyActorOwner::Joining(_)) => {
                drop(terminal);
                return Err(QuinnOwnerRecoveryTerminalError::Actor(
                    QuinnLegActorError::Closing,
                ));
            }
            Some(QuinnOwnerStandbyActorOwner::Joined(_)) => {
                drop(terminal);
                return Err(QuinnOwnerRecoveryTerminalError::Actor(
                    QuinnLegActorError::AlreadyJoined,
                ));
            }
            Some(QuinnOwnerStandbyActorOwner::TaskLost(cause)) => {
                let cause = cause.to_string();
                drop(terminal);
                return Err(QuinnOwnerRecoveryTerminalError::Actor(
                    QuinnLegActorError::TaskJoin(cause),
                ));
            }
            None => {
                drop(terminal);
                return Err(QuinnOwnerRecoveryTerminalError::CompositionInvariant);
            }
        };
        if !terminal.enter_actor_call() {
            terminal.quarantine_actor_uncertain();
            return Err(QuinnOwnerRecoveryTerminalError::CompositionInvariant);
        }
        let poll = actor.try_transact_reported_terminal_with_parts(
            &gate,
            |_runtime, io, queue, reported| {
                terminal.apply_reported(
                    active_b,
                    slot,
                    retirement,
                    io.established_leg(),
                    queue,
                    reported,
                    &gate,
                )
            },
        );
        let poll = match poll {
            Ok(poll) => poll,
            Err(error) => {
                terminal.quarantine_actor_uncertain();
                return Err(error.into());
            }
        };
        match poll {
            QuinnLegActorReportedTerminalPoll::Pending => {
                if !terminal.restore_actor_retained() {
                    return Err(QuinnOwnerRecoveryTerminalError::CompositionInvariant);
                }
                Ok(QuinnOwnerRecoveryTerminalDrive::Pending)
            }
            QuinnLegActorReportedTerminalPoll::Rejected(error) => {
                if !terminal.restore_actor_retained() {
                    return Err(QuinnOwnerRecoveryTerminalError::CompositionInvariant);
                }
                drop(terminal);
                if matches!(
                    error,
                    QuinnOwnerRecoveryTerminalError::UnexpectedLocalCancellation
                ) {
                    let _ = controller_output.stop_output(OwnerOutputDrainStopReason::Cancelled);
                } else {
                    controller_output.fail_output();
                }
                Err(error)
            }
            QuinnLegActorReportedTerminalPoll::AlreadyConsumed => {
                terminal.quarantine_actor_uncertain();
                Err(QuinnOwnerRecoveryTerminalError::CompositionInvariant)
            }
            QuinnLegActorReportedTerminalPoll::Applied(
                QuinnOwnerControllerOutputTerminalCallbackDrive::StandbyRetired { reason },
            ) => {
                debug_assert_eq!(retirement.physical_reason(), Some(reason));
                debug_assert!(armed_probe_timer.is_none());
                Ok(QuinnOwnerRecoveryTerminalDrive::StandbyRetired)
            }
            QuinnLegActorReportedTerminalPoll::Applied(
                QuinnOwnerControllerOutputTerminalCallbackDrive::Applied { generation, reason },
            ) => {
                debug_assert!(armed_probe_timer.is_none());
                Ok(QuinnOwnerRecoveryTerminalDrive::Applied { generation, reason })
            }
            QuinnLegActorReportedTerminalPoll::Applied(
                QuinnOwnerControllerOutputTerminalCallbackDrive::Quarantined { controller_failed },
            ) => {
                debug_assert!(armed_probe_timer.is_none());
                if controller_failed {
                    Err(QuinnOwnerRecoveryTerminalError::ControllerDiscard)
                } else {
                    Err(QuinnOwnerRecoveryTerminalError::CompositionInvariant)
                }
            }
        }
    }
}
