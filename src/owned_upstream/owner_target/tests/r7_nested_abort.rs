//! Fail-once Target Adapter and its exact Owner failure/terminal tests.
//!
//! Owns injected operational/invariant outcomes and all FailOnceTarget tests,
//! including nested abort; the historical route preserves that test identity.
//! Parent owns shared setup and production Executor; this leaf adds no API.
//! Entry points are seven tests; retain exact effects, offers and retry counts.

use super::*;
use crate::owned_upstream::target::{TargetReadyInterest, TargetReadyOutcome};
use std::task::{Context, Poll};

mod bound_failure;

struct FailOnceTarget {
    inner: MemoryTarget,
    open: Option<TargetOpenFailure>,
    write: Option<TargetFailure>,
    write_invariant: Option<TargetIoError>,
    read: Option<TargetFailure>,
    half_close: Option<TargetFailure>,
    half_close_invariant: Option<TargetIoError>,
    join_readiness: Option<Result<TargetReadyOutcome, TargetIoError>>,
    join_readiness_calls: usize,
    join_calls: usize,
    open_calls: usize,
    write_calls: usize,
    read_calls: usize,
    half_close_calls: usize,
}

impl FailOnceTarget {
    fn new() -> Self {
        Self {
            inner: MemoryTarget::new(MemoryTargetConfig::new(4, 256, 512, 128, 128, 16).unwrap()),
            open: None,
            write: None,
            write_invariant: None,
            read: None,
            half_close: None,
            half_close_invariant: None,
            join_readiness: None,
            join_readiness_calls: 0,
            join_calls: 0,
            open_calls: 0,
            write_calls: 0,
            read_calls: 0,
            half_close_calls: 0,
        }
    }
}

impl TargetIo for FailOnceTarget {
    fn capacity_witness(&self) -> TargetIoCapacityWitness {
        self.inner.capacity_witness()
    }

    fn cold_inventory(&self) -> Result<TargetIoColdInventory, TargetIoColdInventoryError> {
        self.inner.cold_inventory()
    }

    fn open(
        &mut self,
        flow_id: SessionFlowId,
        target: &TargetAddr,
    ) -> Result<TargetOpenCompletion, TargetIoError> {
        self.open_calls += 1;
        if let Some(failure) = self.open.take() {
            return Ok(TargetOpenCompletion::Failed(failure));
        }
        self.inner.open(flow_id, target)
    }

    fn write(
        &mut self,
        flow_id: SessionFlowId,
        bytes: &[u8],
    ) -> Result<TargetWriteCompletion, TargetIoError> {
        self.write_calls += 1;
        if let Some(error) = self.write_invariant.take() {
            return Err(error);
        }
        if let Some(failure) = self.write.take() {
            return Ok(TargetWriteCompletion::Failed(failure));
        }
        self.inner.write(flow_id, bytes)
    }

    fn read(
        &mut self,
        flow_id: SessionFlowId,
        max_bytes: usize,
    ) -> Result<TargetReadCompletion, TargetIoError> {
        self.read_calls += 1;
        if let Some(failure) = self.read.take() {
            return Ok(TargetReadCompletion::Failed(failure));
        }
        self.inner.read(flow_id, max_bytes)
    }

    fn half_close_write(
        &mut self,
        flow_id: SessionFlowId,
    ) -> Result<TargetHalfCloseCompletion, TargetIoError> {
        self.half_close_calls += 1;
        if let Some(error) = self.half_close_invariant.take() {
            return Err(error);
        }
        if let Some(failure) = self.half_close.take() {
            return Ok(TargetHalfCloseCompletion::Failed(failure));
        }
        self.inner.half_close_write(flow_id)
    }

    fn reset(&mut self, flow_id: SessionFlowId) -> Result<TargetResetCompletion, TargetIoError> {
        self.inner.reset(flow_id)
    }

    fn cancel(&mut self, flow_id: SessionFlowId) -> Result<TargetCancelCompletion, TargetIoError> {
        self.inner.cancel(flow_id)
    }

    fn expire_session_flow(
        &mut self,
        flow_id: SessionFlowId,
    ) -> Result<TargetExpireCompletion, TargetIoError> {
        self.inner.expire_session_flow(flow_id)
    }

    fn poll_readiness(
        &mut self,
        flow_id: SessionFlowId,
        interest: TargetReadyInterest,
        cx: &mut Context<'_>,
    ) -> Poll<Result<TargetReadyOutcome, TargetIoError>> {
        if interest == TargetReadyInterest::Join {
            self.join_readiness_calls += 1;
            if let Some(result) = self.join_readiness.take() {
                return Poll::Ready(result);
            }
        }
        self.inner.poll_readiness(flow_id, interest, cx)
    }

    fn poll_ready_notifications(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), TargetIoError>> {
        self.inner.poll_ready_notifications(cx)
    }

    fn readiness(&self, flow_id: SessionFlowId) -> Result<TargetReadiness, TargetIoError> {
        self.inner.readiness(flow_id)
    }

    fn join(&mut self, flow_id: SessionFlowId) -> Result<TargetJoinCompletion, TargetIoError> {
        self.join_calls += 1;
        self.inner.join(flow_id)
    }

    fn retire_terminal_tombstone(
        &mut self,
        flow_id: SessionFlowId,
    ) -> Result<TargetTombstoneRetireCompletion, TargetIoError> {
        self.inner.retire_terminal_tombstone(flow_id)
    }
}

#[test]
fn operational_open_failures_map_exactly_once_without_target_ownership() {
    let cases = [
        (TargetOpenFailure::Refused, OpenResultCode::TargetRefused),
        (
            TargetOpenFailure::Unreachable,
            OpenResultCode::TargetUnreachable,
        ),
        (TargetOpenFailure::TimedOut, OpenResultCode::TimedOut),
        (
            TargetOpenFailure::ResourceExhausted,
            OpenResultCode::ResourceExhausted,
        ),
        (
            TargetOpenFailure::PolicyDenied,
            OpenResultCode::PolicyDenied,
        ),
        (TargetOpenFailure::Internal, OpenResultCode::Internal),
    ];
    for (index, (failure, expected)) in cases.into_iter().enumerate() {
        let leg = committed_leg(2, 0x22, 0x42);
        let model = SessionModel::new(SessionRole::Owner, session_config(), leg);
        let supervisor = SessionSupervisor::new(model);
        let mut target_io = FailOnceTarget::new();
        target_io.open = Some(failure);
        let mut executor = OwnerTargetExecutor::new(
            OwnerTargetConfig::new(128, 1_024, 64).unwrap(),
            supervisor,
            target_io,
            source_config(),
        )
        .unwrap();
        let flow_id = SessionFlowId::new((index + 1) as u64).unwrap();

        let outputs = open_flow(&mut executor, leg, flow_id, target(443));
        assert_eq!(
            transmitted(&outputs)
                .iter()
                .filter(|record| matches!(
                    record,
                    Record::OpenResult {
                        flow_id: observed,
                        result,
                    } if *observed == flow_id && *result == expected
                ))
                .count(),
            1
        );
        assert_eq!(executor.target().open_calls, 1);
        assert_eq!(executor.snapshot().target_flows, 0);
        assert_eq!(executor.snapshot().target_read_owned_bytes, 0);
        assert_eq!(executor.snapshot().target_read_owned_segments, 0);
        assert_eq!(executor.target().inner.snapshot().live_flows, 0);
    }
}

#[test]
fn operational_read_failure_is_one_urgent_target_failure_reset_and_other_flow_progresses() {
    let leg = committed_leg(2, 0x22, 0x42);
    let model = SessionModel::new(SessionRole::Owner, session_config(), leg);
    let supervisor = SessionSupervisor::new(model);
    let mut target_io = FailOnceTarget::new();
    target_io.read = Some(TargetFailure::ConnectionLost);
    let mut executor = OwnerTargetExecutor::new(
        OwnerTargetConfig::new(128, 1_024, 64).unwrap(),
        supervisor,
        target_io,
        source_config(),
    )
    .unwrap();
    let failed = SessionFlowId::new(1).unwrap();
    let live = SessionFlowId::new(2).unwrap();
    open_flow(&mut executor, leg, failed, target(443));
    open_flow(&mut executor, leg, live, target(8443));

    let outputs = executor.try_read_target(failed).unwrap();
    assert_eq!(executor.target().read_calls, 1);
    assert!(transmitted(&outputs).iter().any(|record| matches!(
        record,
        Record::Reset {
            flow_id,
            reason: ResetReason::TargetFailure,
        } if *flow_id == failed
    )));
    assert_eq!(executor.snapshot().target_flows, 1);
    assert_eq!(executor.target().inner.snapshot().live_flows, 1);
    assert!(matches!(
        executor.try_read_target(failed),
        Err(OwnerTargetError::UnknownTargetFlow { flow_id }) if flow_id == failed
    ));
    assert_eq!(executor.target().read_calls, 1);

    executor
        .target_mut()
        .inner
        .feed_read(live, Bytes::from_static(b"ok"))
        .unwrap();
    let progressed = executor.try_read_target(live).unwrap();
    assert!(transmitted(&progressed).iter().any(|record| matches!(
        record,
        Record::Data { flow_id, payload, .. }
            if *flow_id == live && payload.as_ref() == b"ok"
    )));
}

#[test]
fn operational_write_and_half_close_failures_reset_but_invariant_errors_stay_fatal() {
    let leg = committed_leg(2, 0x22, 0x42);
    let model = SessionModel::new(SessionRole::Owner, session_config(), leg);
    let supervisor = SessionSupervisor::new(model);
    let mut target_io = FailOnceTarget::new();
    target_io.write = Some(TargetFailure::ResourceExhausted);
    target_io.half_close = Some(TargetFailure::ConnectionLost);
    let mut executor = OwnerTargetExecutor::new(
        OwnerTargetConfig::new(128, 1_024, 64).unwrap(),
        supervisor,
        target_io,
        source_config(),
    )
    .unwrap();
    let write_failed = SessionFlowId::new(1).unwrap();
    let close_failed = SessionFlowId::new(2).unwrap();
    open_flow(&mut executor, leg, write_failed, target(443));
    open_flow(&mut executor, leg, close_failed, target(8443));

    let write = peer_frame(
        &mut executor,
        leg,
        Record::Data {
            flow_id: write_failed,
            direction: Direction::ClientToTarget,
            offset: ByteOffset::new(0),
            payload: Bytes::from_static(b"owned"),
        },
    )
    .unwrap();
    assert!(transmitted(&write).iter().any(|record| matches!(
        record,
        Record::Reset { flow_id, reason: ResetReason::TargetFailure }
            if *flow_id == write_failed
    )));
    assert_eq!(executor.target().write_calls, 1);

    let close = peer_frame(
        &mut executor,
        leg,
        Record::Close {
            flow_id: close_failed,
            direction: Direction::ClientToTarget,
            final_offset: ByteOffset::new(0),
        },
    )
    .unwrap();
    assert!(transmitted(&close).iter().any(|record| matches!(
        record,
        Record::Reset { flow_id, reason: ResetReason::TargetFailure }
            if *flow_id == close_failed
    )));
    assert_eq!(executor.target().half_close_calls, 1);
    assert_eq!(executor.snapshot().target_flows, 0);

    let model = SessionModel::new(SessionRole::Owner, session_config(), leg);
    let supervisor = SessionSupervisor::new(model);
    let mut target_io = FailOnceTarget::new();
    target_io.write_invariant = Some(TargetIoError::CounterOverflow {
        counter: "injected_write_invariant",
    });
    let mut invariant_executor = OwnerTargetExecutor::new(
        OwnerTargetConfig::new(128, 1_024, 64).unwrap(),
        supervisor,
        target_io,
        source_config(),
    )
    .unwrap();
    let flow_id = SessionFlowId::new(1).unwrap();
    open_flow(&mut invariant_executor, leg, flow_id, target(443));
    assert!(matches!(
        peer_frame(
            &mut invariant_executor,
            leg,
            Record::Data {
                flow_id,
                direction: Direction::ClientToTarget,
                offset: ByteOffset::new(0),
                payload: Bytes::from_static(b"fatal"),
            },
        ),
        Err(OwnerTargetError::Target(TargetIoError::CounterOverflow {
            counter: "injected_write_invariant"
        }))
    ));
    assert_eq!(invariant_executor.target().write_calls, 1);
    assert_eq!(invariant_executor.snapshot().aborted_effects, 1);
    assert_eq!(
        invariant_executor
            .snapshot()
            .session
            .session
            .outstanding_sink_offers(),
        1
    );
    match invariant_executor.apply_event(SessionEvent::LegLost { leg }) {
        Err(OwnerTargetError::RejectedEvent {
            block: OwnerTargetInputBlock::AbortedEffectInvariant,
            event,
        }) => assert_eq!(*event, SessionEvent::LegLost { leg }),
        other => panic!("poisoned owner must return exact rejected event, got {other:?}"),
    }
    assert_eq!(invariant_executor.target().write_calls, 1);
    assert_eq!(
        invariant_executor.snapshot().session.session.phase(),
        crate::resumable::SessionPhase::Active
    );
}

#[test]
fn terminal_capacity_retains_failed_read_completion_without_repeating_target_io() {
    let leg = committed_leg(2, 0x22, 0x42);
    let config = SessionConfig::new(
        4,
        1,
        TcpWindowLimits::new(128, 16).unwrap(),
        TcpWindowLimits::new(128, 16).unwrap(),
        ReplayBudgetLimits::new(128, 16).unwrap(),
        ReplayBudgetLimits::new(128, 16).unwrap(),
        ReceiveBudgetLimits::new(256, 32).unwrap(),
        128,
    )
    .unwrap();
    let model = SessionModel::new(SessionRole::Owner, config, leg);
    let supervisor = SessionSupervisor::new(model);
    let mut target_io = FailOnceTarget::new();
    target_io.read = Some(TargetFailure::TimedOut);
    let mut executor = OwnerTargetExecutor::new(
        OwnerTargetConfig::new(128, 1_024, 64).unwrap(),
        supervisor,
        target_io,
        source_config(),
    )
    .unwrap();
    let terminal_flow = SessionFlowId::new(1).unwrap();
    let failed = SessionFlowId::new(2).unwrap();
    let live = SessionFlowId::new(3).unwrap();
    open_flow(&mut executor, leg, terminal_flow, target(443));
    let terminal_outputs = peer_frame(
        &mut executor,
        leg,
        Record::Reset {
            flow_id: terminal_flow,
            reason: ResetReason::TargetFailure,
        },
    )
    .unwrap();
    let terminal = terminal_grace(&terminal_outputs);
    open_flow(&mut executor, leg, failed, target(8443));

    assert!(executor.try_read_target(failed).unwrap().is_empty());
    assert_eq!(executor.target().read_calls, 1);
    assert_eq!(executor.snapshot().pending_target_completions, 1);
    assert!(matches!(
        executor.try_read_target(failed),
        Err(OwnerTargetError::PendingTargetCompletion { flow_id }) if flow_id == failed
    ));
    assert_eq!(executor.target().read_calls, 1);

    let live_open = open_flow(&mut executor, leg, live, target(9443));
    assert!(transmitted(&live_open).iter().any(|record| matches!(
        record,
        Record::OpenResult { flow_id, result: OpenResultCode::Opened }
            if *flow_id == live
    )));
    let retried = executor
        .apply_event(SessionEvent::TerminalGraceExpired { terminal })
        .unwrap();
    assert!(transmitted(&retried).iter().any(|record| matches!(
        record,
        Record::Reset { flow_id, reason: ResetReason::TargetFailure }
            if *flow_id == failed
    )));
    assert_eq!(executor.target().read_calls, 1);
    assert_eq!(executor.snapshot().pending_target_completions, 0);
    assert_eq!(executor.snapshot().target_flows, 1);
    assert_eq!(executor.target().inner.snapshot().live_flows, 1);
}

#[test]
fn successive_terminal_retirements_advance_pending_completions_in_flow_order() {
    let leg = committed_leg(2, 0x22, 0x42);
    let config = SessionConfig::new(
        4,
        1,
        TcpWindowLimits::new(128, 16).unwrap(),
        TcpWindowLimits::new(128, 16).unwrap(),
        ReplayBudgetLimits::new(128, 16).unwrap(),
        ReplayBudgetLimits::new(128, 16).unwrap(),
        ReceiveBudgetLimits::new(256, 32).unwrap(),
        128,
    )
    .unwrap();
    let model = SessionModel::new(SessionRole::Owner, config, leg);
    let supervisor = SessionSupervisor::new(model);
    let mut executor = OwnerTargetExecutor::new(
        OwnerTargetConfig::new(128, 1_024, 64).unwrap(),
        supervisor,
        FailOnceTarget::new(),
        source_config(),
    )
    .unwrap();
    let occupying = SessionFlowId::new(1).unwrap();
    let first = SessionFlowId::new(2).unwrap();
    let second = SessionFlowId::new(3).unwrap();
    open_flow(&mut executor, leg, occupying, target(443));
    open_flow(&mut executor, leg, first, target(8443));
    open_flow(&mut executor, leg, second, target(9443));
    let occupying_terminal = terminal_grace(
        &peer_frame(
            &mut executor,
            leg,
            Record::Reset {
                flow_id: occupying,
                reason: ResetReason::TargetFailure,
            },
        )
        .unwrap(),
    );

    executor.target_mut().read = Some(TargetFailure::TimedOut);
    assert!(executor.try_read_target(first).unwrap().is_empty());
    executor.target_mut().read = Some(TargetFailure::ConnectionLost);
    assert!(executor.try_read_target(second).unwrap().is_empty());
    assert_eq!(executor.snapshot().pending_target_completions, 2);
    assert_eq!(executor.target().read_calls, 2);

    let first_retry = executor
        .apply_event(SessionEvent::TerminalGraceExpired {
            terminal: occupying_terminal,
        })
        .unwrap();
    let first_terminal = terminal_grace(&first_retry);
    assert_eq!(first_terminal.flow_id(), first);
    assert_eq!(executor.snapshot().pending_target_completions, 1);
    assert_eq!(executor.target().read_calls, 2);

    let second_retry = executor
        .apply_event(SessionEvent::TerminalGraceExpired {
            terminal: first_terminal,
        })
        .unwrap();
    let second_terminal = terminal_grace(&second_retry);
    assert_eq!(second_terminal.flow_id(), second);
    assert_eq!(executor.snapshot().pending_target_completions, 0);
    assert_eq!(executor.target().read_calls, 2);

    executor
        .apply_event(SessionEvent::TerminalGraceExpired {
            terminal: second_terminal,
        })
        .unwrap();
    assert_eq!(executor.snapshot().target_flows, 0);
    assert_eq!(executor.snapshot().target_tombstones, 0);
    assert_eq!(executor.target().inner.snapshot().joined_flows, 0);
}

#[test]
fn terminal_capacity_retains_successful_half_close_without_repeating_target_io() {
    let leg = committed_leg(2, 0x22, 0x42);
    let config = SessionConfig::new(
        4,
        1,
        TcpWindowLimits::new(128, 16).unwrap(),
        TcpWindowLimits::new(128, 16).unwrap(),
        ReplayBudgetLimits::new(128, 16).unwrap(),
        ReplayBudgetLimits::new(128, 16).unwrap(),
        ReceiveBudgetLimits::new(256, 32).unwrap(),
        128,
    )
    .unwrap();
    let model = SessionModel::new(SessionRole::Owner, config, leg);
    let supervisor = SessionSupervisor::new(model);
    let mut executor = OwnerTargetExecutor::new(
        OwnerTargetConfig::new(128, 1_024, 64).unwrap(),
        supervisor,
        FailOnceTarget::new(),
        source_config(),
    )
    .unwrap();
    let occupying = SessionFlowId::new(1).unwrap();
    let blocked = SessionFlowId::new(2).unwrap();
    open_flow(&mut executor, leg, occupying, target(443));
    open_flow(&mut executor, leg, blocked, target(8443));

    let occupying_outputs = peer_frame(
        &mut executor,
        leg,
        Record::Reset {
            flow_id: occupying,
            reason: ResetReason::TargetFailure,
        },
    )
    .unwrap();
    let occupying_terminal = terminal_grace(&occupying_outputs);

    executor.target_mut().inner.finish_read(blocked).unwrap();
    let eof = executor.try_read_target(blocked).unwrap();
    assert!(transmitted(&eof).iter().any(|record| matches!(
        record,
        Record::Close {
            flow_id,
            direction: Direction::TargetToClient,
            final_offset,
        } if *flow_id == blocked && *final_offset == ByteOffset::new(0)
    )));
    peer_frame(
        &mut executor,
        leg,
        Record::Ack {
            flow_id: blocked,
            direction: Direction::TargetToClient,
            next_accepted: ByteOffset::new(0),
            final_accepted: true,
        },
    )
    .unwrap();

    let blocked_completion = peer_frame(
        &mut executor,
        leg,
        Record::Close {
            flow_id: blocked,
            direction: Direction::ClientToTarget,
            final_offset: ByteOffset::new(0),
        },
    )
    .unwrap();
    assert!(
        !blocked_completion
            .iter()
            .any(|output| matches!(output, OwnerTargetOutput::TerminalGraceStarted { .. }))
    );
    assert_eq!(executor.target().half_close_calls, 1);
    assert_eq!(executor.snapshot().pending_target_completions, 1);

    let deferred_event = SessionEvent::PeerFrame {
        leg,
        frame: Frame::try_new(
            leg.generation(),
            Record::Close {
                flow_id: blocked,
                direction: Direction::ClientToTarget,
                final_offset: ByteOffset::new(0),
            },
        )
        .unwrap(),
    };
    let retained_event = match executor.apply_event(deferred_event) {
        Err(OwnerTargetError::RejectedEvent {
            block: OwnerTargetInputBlock::PendingTargetCompletion { flow_id },
            event,
        }) if flow_id == blocked => event,
        other => panic!("blocked completion must return exact same-flow input, got {other:?}"),
    };
    assert_eq!(event_flow_id(&retained_event), Some(blocked));
    assert_eq!(executor.target().half_close_calls, 1);

    let retried = executor
        .apply_event(SessionEvent::TerminalGraceExpired {
            terminal: occupying_terminal,
        })
        .unwrap();
    let blocked_terminal = terminal_grace(&retried);
    assert_eq!(blocked_terminal.flow_id(), blocked);
    assert_eq!(executor.target().half_close_calls, 1);
    assert_eq!(executor.snapshot().pending_target_completions, 0);
    assert_eq!(executor.snapshot().target_flows, 0);

    let duplicate = executor.apply_event(*retained_event).unwrap();
    assert!(transmitted(&duplicate).iter().any(|record| matches!(
        record,
        Record::Ack {
            flow_id,
            direction: Direction::ClientToTarget,
            final_accepted: true,
            ..
        } if *flow_id == blocked
    )));
    assert_eq!(executor.target().half_close_calls, 1);

    executor
        .apply_event(SessionEvent::TerminalGraceExpired {
            terminal: blocked_terminal,
        })
        .unwrap();
    assert_eq!(executor.snapshot().target_tombstones, 0);
    assert_eq!(executor.target().inner.snapshot().joined_flows, 0);
}

#[test]
fn nested_target_failure_abort_retains_only_the_outer_offer_with_exact_segments() {
    let leg = committed_leg(2, 0x22, 0x42);
    let model = SessionModel::new(SessionRole::Owner, session_config(), leg);
    let supervisor = SessionSupervisor::new(model);
    let mut target_io = FailOnceTarget::new();
    target_io.write = Some(TargetFailure::ResourceExhausted);
    let mut executor = OwnerTargetExecutor::new(
        OwnerTargetConfig::new(128, 1_024, 64).unwrap(),
        supervisor,
        target_io,
        source_config(),
    )
    .unwrap();
    let flow_id = SessionFlowId::new(1).unwrap();
    open_flow(&mut executor, leg, flow_id, target(443));

    // Derive the real reducer Offer without running the Target executor yet,
    // then inject the exact impossible duplicate-completion invariant that
    // makes `apply_target_failure` fail after the Target write was attempted.
    let generated = executor
        .supervisor
        .apply_event(SessionEvent::PeerFrame {
            leg,
            frame: Frame::try_new(
                leg.generation(),
                Record::Data {
                    flow_id,
                    direction: Direction::ClientToTarget,
                    offset: ByteOffset::new(0),
                    payload: Bytes::from_static(b"outer-owned"),
                },
            )
            .unwrap(),
        })
        .unwrap();
    assert!(matches!(
        generated.as_slice(),
        [SessionEffect::OfferToSink { .. }]
    ));
    let local_flow = executor
        .flows
        .get(&flow_id)
        .and_then(|flow| flow.source.as_ref())
        .expect("opened Target flow owns its source")
        .flow
        .local_flow();
    executor
        .flows
        .get_mut(&flow_id)
        .expect("opened Target flow remains live")
        .pending_completion = Some(PendingTargetCompletion::Failure {
        event: SessionEvent::LocalReset {
            flow: local_flow,
            reason: ResetReason::TargetFailure,
        },
    });

    let result =
        executor.execute_queue(generated.into_iter().map(OwnerTargetWork::Effect).collect());
    assert!(matches!(
        result,
        Err(OwnerTargetError::DuplicatePendingTargetCompletion {
            flow_id: observed,
        }) if observed == flow_id
    ));

    let aborted = executor
        .aborted_effects
        .as_ref()
        .expect("nested fatal must retain one fail-closed ownership quarantine");
    assert_eq!(
        aborted.effects.len() + usize::from(aborted.current.is_some()),
        1,
        "a diagnostic inner marker must not duplicate the exact outer owner",
    );
    let Some(OwnerTargetWork::Effect(SessionEffect::OfferToSink { offer, segments })) =
        aborted.current.as_ref()
    else {
        panic!("the exact outer OfferToSink must occupy the inline quarantine");
    };
    assert_eq!(offer.flow_id(), flow_id);
    assert_eq!(segments.len(), 1);
    assert_eq!(segments[0].payload().as_ref(), b"outer-owned");
    assert_eq!(executor.target().write_calls, 1);
    assert_eq!(executor.snapshot().aborted_outputs, 0);

    assert!(matches!(
        executor.apply_event(SessionEvent::LegLost { leg }),
        Err(OwnerTargetError::RejectedEvent {
            block: OwnerTargetInputBlock::AbortedEffectInvariant,
            ..
        })
    ));
    assert_eq!(executor.target().write_calls, 1);
}
