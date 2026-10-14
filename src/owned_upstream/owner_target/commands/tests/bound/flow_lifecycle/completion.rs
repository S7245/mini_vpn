//! Actual returned RESET pressure and selected Completion retry evidence.
//!
//! Owns the original Flow/Driver charge assertions through capacity rejection
//! and once-only retry; not physical C/Root lifecycle or new Flow admission.

use super::*;
use crate::owned_upstream::owner_ready::{OwnerReadyCategory, OwnerReadySource};

#[test]
fn received_reset_retries_once_after_real_terminal_pressure_releases() {
    use crate::owned_upstream::owner_target::PendingTargetCompletion;
    let FlowLifecycleFixture {
        mut ready,
        mut executor,
        mut inbox,
        local,
        mut terminal,
    } = pressure_fixture();
    let leg = source_leg();
    let mut claim = ready.claim(0).unwrap();
    let mut turn = executor.begin_ready_publication().unwrap();
    executor
        .stage_bound_target_failure(local.flow_id())
        .unwrap();
    let mut completed = send_source_to_mailbox(&mut executor, SendLane::Control);
    let mut cx = Context::from_waker(Waker::noop());
    let Poll::Ready(Some(reset)) = inbox.poll_selected(SessionMailboxLane::Control, &mut cx) else {
        panic!("only the actual RESET receipt may become pressure-retained completion");
    };
    assert_eq!(live_record(&executor, local.flow_id()).transit, 1);
    let observed = executor.apply_ready_receipt_observed(reset).unwrap();
    assert!(observed.into_progress_parts().0.is_empty());
    let record = live_record(&executor, local.flow_id());
    assert_eq!(
        record.transit, 1,
        "carried rejection still owns its original Driver charge"
    );
    assert!(
        matches!(&executor.flows[&local.flow_id()].pending_completion,
        Some(PendingTargetCompletion::DriverReturned(state)) if state.is_returned_reset_for(local))
    );
    assert_eq!(executor.snapshot().pending_target_completions, 1);
    for round in 0..=3 {
        let outputs = executor
            .apply_event(SessionEvent::TerminalGraceExpired { terminal })
            .unwrap();
        assert!(
            outputs.is_empty(),
            "expiry only schedules the actual waiter"
        );
        assert_eq!(
            executor.supervisor.snapshot().session.terminal_tombstones(),
            0
        );
        assert!(live_record(&executor, local.flow_id()).wait.is_none());
        assert_eq!(live_record(&executor, local.flow_id()).transit, 1);
        if round < 3 {
            let competitor = SessionFlowId::new(3 + round).unwrap();
            assert!(executor.flows[&competitor].service_position.is_some());
            let outputs = peer_record(
                &mut executor,
                leg,
                Record::Reset {
                    flow_id: competitor,
                    reason: ResetReason::TargetFailure,
                },
            );
            terminal = outputs
                .iter()
                .find_map(|output| match output {
                    OwnerTargetOutput::TerminalGraceStarted { terminal } => Some(*terminal),
                    _ => None,
                })
                .expect("the pre-imported competitor must occupy the actual Z slot");
            assert_eq!(terminal.flow_id(), competitor);
            assert_eq!(
                executor.supervisor.snapshot().session.terminal_tombstones(),
                1
            );
            assert!(!outputs.iter().any(|output| matches!(output,
                OwnerTargetOutput::Transmit(frame) if matches!(frame.record(),
                    Record::Reset { flow_id, .. } if *flow_id == local.flow_id()))));
        }
        assert!(turn.finish(&completed).is_ok());
        ready
            .finish(&claim, completed.runnable, completed.blocked)
            .unwrap();

        // Service real dirty send sources before the next actual Continuation
        // claim. No fabricated wake, copied F position or global retry helper.
        let mut visits = 0;
        let (next_claim, next_turn, next_completed, outputs) = loop {
            visits += 1;
            assert!(visits <= 32, "finite source probes must reach Completion");
            let next_claim = ready.claim(0).unwrap();
            let next_turn = executor.begin_ready_publication().unwrap();
            if next_claim.source == OwnerReadySource::OwnerContinuation {
                let mask = OwnerReadyCategory::Completion.mask();
                let selected = executor
                    .select_ready_continuation(mask, 0)
                    .unwrap()
                    .expect("real capacity release must select the original waiter");
                assert_eq!(
                    selected.candidate.as_ref().unwrap().association.flow_id(),
                    local.flow_id()
                );
                assert!(
                    live_record(&executor, local.flow_id()).wait.is_none(),
                    "this visit must actually retry, not remain resource-waiting"
                );
                let (observed, result) =
                    executor.apply_ready_completion_observed(&selected).unwrap();
                let runnable = executor
                    .finish_ready_continuation(&selected, result, mask)
                    .unwrap();
                break (
                    next_claim,
                    next_turn,
                    SessionCommandVisit {
                        work: OwnerReadyUnit::SourceRead,
                        runnable,
                        blocked: None,
                    },
                    observed.into_progress_parts().0,
                );
            }
            let lane = match next_claim.source {
                OwnerReadySource::ControlSend => SendLane::Control,
                OwnerReadySource::OrderedSend => SendLane::OrderedSource,
                _ => panic!("this component has no registered inbox consumer"),
            };
            let visit = executor.visit_ready_send(lane).unwrap();
            assert!(
                matches!(
                    visit.work,
                    OwnerReadyUnit::Waiting | OwnerReadyUnit::SourceDrained
                ),
                "empty/EOF source service must not create another command"
            );
            assert!(next_turn.finish(&visit).is_ok());
            ready
                .finish(&next_claim, visit.runnable, visit.blocked)
                .unwrap();
        };
        claim = next_claim;
        turn = next_turn;
        completed = next_completed;
        if round < 3 {
            assert!(
                outputs.is_empty(),
                "occupied Z must reject each actual retry"
            );
            assert_eq!(live_record(&executor, local.flow_id()).transit, 1);
            assert!(
                live_record(&executor, local.flow_id()).wait.is_some(),
                "actual rejection must put the original Flow back on its wait chain"
            );
            assert!(
                matches!(&executor.flows[&local.flow_id()].pending_completion,
                Some(PendingTargetCompletion::DriverReturned(state))
                    if state.is_returned_reset_for(local))
            );
            assert_eq!(executor.snapshot().pending_target_completions, 1);
        } else {
            assert_eq!(
                outputs
                    .iter()
                    .filter(|output| matches!(output,
                OwnerTargetOutput::Transmit(frame) if matches!(frame.record(),
                    Record::Reset { flow_id, .. } if *flow_id == local.flow_id())))
                    .count(),
                1
            );
            assert_eq!(executor.snapshot().pending_target_completions, 0);
            assert_eq!(live_record(&executor, local.flow_id()).transit, 0);
        }
    }
    assert!(
        inbox
            .poll_selected(SessionMailboxLane::Control, &mut cx)
            .is_pending()
    );
    assert!(turn.finish(&completed).is_ok());
    ready
        .finish(&claim, completed.runnable, completed.blocked)
        .unwrap();
}

// All live competitors enter through the unchanged real cold constructor.
// Model F=4/Target=4 and Z=1 remain fixed; no post-bind OPEN needs invented F.
fn pressure_fixture() -> FlowLifecycleFixture {
    let leg = source_leg();
    let (plan, mut executor) = lifecycle_executor(leg, 4, 128);
    assert_eq!(plan.session_config().max_flows(), 4);
    assert_eq!(plan.session_config().max_terminal_tombstones(), 1);
    let first = SessionFlowId::new(1).unwrap();
    peer_record(
        &mut executor,
        leg,
        Record::Open {
            flow_id: first,
            target: TargetAddr::IpPort(([127, 0, 0, 1], 443).into()),
        },
    );
    let outputs = peer_record(
        &mut executor,
        leg,
        Record::Reset {
            flow_id: first,
            reason: ResetReason::TargetFailure,
        },
    );
    let terminal = outputs
        .iter()
        .find_map(|output| match output {
            OwnerTargetOutput::TerminalGraceStarted { terminal } => Some(*terminal),
            _ => None,
        })
        .expect("the original real RESET occupies Z before binding");
    for id in 2..=5 {
        let flow_id = SessionFlowId::new(id).unwrap();
        peer_record(
            &mut executor,
            leg,
            Record::Open {
                flow_id,
                target: TargetAddr::IpPort(([127, 0, 0, 1], 8443).into()),
            },
        );
        assert!(executor.flows[&flow_id].target_present);
    }
    let local = executor.flows[&SessionFlowId::new(2).unwrap()]
        .source
        .as_ref()
        .unwrap()
        .flow
        .local_flow();
    let geometry = ReadyGeometry::checked(NonZeroUsize::new(1).unwrap()).unwrap();
    let mut ready = ReadyIndex::cold(&geometry).unwrap();
    let limits =
        OwnerReadySessionLimits::checked(plan.session_config().max_flows(), 1, 2, 2).unwrap();
    let reservation = ready.reserve_session(leg.session_id(), limits).unwrap();
    let (mut executor, inbox) = match executor.install_owner_ready(reservation, plan) {
        Ok(installed) => installed,
        Err(_) => panic!("the same original factory and F4 plan must install"),
    };
    for id in 2..=5 {
        let owner = &executor.flows[&SessionFlowId::new(id).unwrap()];
        assert!(owner.service_position.is_some());
        assert_eq!(
            live_record(&executor, SessionFlowId::new(id).unwrap()).transit,
            0
        );
    }
    let OwnerTargetServiceState::Bound { binding, flows, .. } = &mut executor.service else {
        panic!("the consuming installation must own the actual binding");
    };
    binding
        .activate(&mut ready, 0, &flows.session, 0b1_1111)
        .unwrap();
    FlowLifecycleFixture {
        executor,
        inbox,
        local,
        terminal,
        ready,
    }
}

// This separate preparation preserves the old RESET fixture. EOF and its peer
// ACK happen through real ports before installation; no Active Unbound read.
fn half_close_pressure_fixture() -> (FlowLifecycleFixture, Vec<OwnerTargetOutput>) {
    let leg = source_leg();
    let (plan, mut executor) = lifecycle_executor(leg, 4, 128);
    assert_eq!(plan.session_config().max_flows(), 4);
    assert_eq!(plan.session_config().max_terminal_tombstones(), 1);
    let first = SessionFlowId::new(1).unwrap();
    let blocked = SessionFlowId::new(2).unwrap();
    let mut held_outputs = peer_record(
        &mut executor,
        leg,
        Record::Open {
            flow_id: first,
            target: TargetAddr::IpPort(([127, 0, 0, 1], 443).into()),
        },
    );
    let outputs = peer_record(
        &mut executor,
        leg,
        Record::Reset {
            flow_id: first,
            reason: ResetReason::TargetFailure,
        },
    );
    let terminal = outputs
        .iter()
        .find_map(|output| match output {
            OwnerTargetOutput::TerminalGraceStarted { terminal } => Some(*terminal),
            _ => None,
        })
        .expect("actual A RESET occupies the sole terminal slot");
    held_outputs.extend(outputs);
    for id in 2..=5 {
        let flow_id = SessionFlowId::new(id).unwrap();
        let outputs = peer_record(
            &mut executor,
            leg,
            Record::Open {
                flow_id,
                target: TargetAddr::IpPort(([127, 0, 0, 1], 8443).into()),
            },
        );
        assert!(
            matches!(outputs.as_slice(), [OwnerTargetOutput::Transmit(frame)]
            if matches!(frame.record(), Record::OpenResult { flow_id: actual, .. } if *actual == flow_id))
        );
        held_outputs.extend(outputs);
    }
    executor.target_mut().finish_read(blocked).unwrap();
    let eof = executor.try_read_target(blocked).unwrap();
    assert!(
        matches!(eof.as_slice(), [OwnerTargetOutput::Transmit(frame)]
        if matches!(frame.record(), Record::Close {
            flow_id, direction: Direction::TargetToClient, final_offset,
        } if *flow_id == blocked && *final_offset == ByteOffset::new(0)))
    );
    held_outputs.extend(eof);
    assert!(
        peer_record(
            &mut executor,
            leg,
            Record::Ack {
                flow_id: blocked,
                direction: Direction::TargetToClient,
                next_accepted: ByteOffset::new(0),
                final_accepted: true,
            }
        )
        .is_empty()
    );
    assert_eq!(executor.target().snapshot().read_eof, 1);
    assert_eq!(executor.target().snapshot().write_half_closes, 0);
    let local = executor.flows[&blocked]
        .source
        .as_ref()
        .unwrap()
        .flow
        .local_flow();
    let geometry = ReadyGeometry::checked(NonZeroUsize::new(1).unwrap()).unwrap();
    let mut ready = ReadyIndex::cold(&geometry).unwrap();
    let limits =
        OwnerReadySessionLimits::checked(plan.session_config().max_flows(), 1, 2, 2).unwrap();
    let reservation = ready.reserve_session(leg.session_id(), limits).unwrap();
    let (mut executor, inbox) = match executor.install_owner_ready(reservation, plan) {
        Ok(installed) => installed,
        Err(_) => panic!("the real EOF-acknowledged owner must bind without new state"),
    };
    let OwnerTargetServiceState::Bound { binding, flows, .. } = &mut executor.service else {
        panic!("actual consuming binding");
    };
    binding
        .activate(&mut ready, 0, &flows.session, 0b1_1111)
        .unwrap();
    (
        FlowLifecycleFixture {
            executor,
            inbox,
            local,
            terminal,
            ready,
        },
        held_outputs,
    )
}

#[test]
fn half_close_repressure_preserves_exact_completion_and_source_association() {
    use crate::owned_upstream::owner_ready::OwnerReadyFlowAssociation;
    use crate::owned_upstream::owner_target::PendingTargetCompletion;
    let (
        FlowLifecycleFixture {
            mut ready,
            mut executor,
            mut inbox,
            local,
            mut terminal,
        },
        mut held_outputs,
    ) = half_close_pressure_fixture();
    let leg = source_leg();
    let mut claim = ready.claim(0).unwrap();
    let mut turn = executor.begin_ready_publication().unwrap();
    let lane = match claim.source {
        OwnerReadySource::ControlSend => SendLane::Control,
        OwnerReadySource::OrderedSend => SendLane::OrderedSource,
        _ => panic!("actual initial source claim"),
    };
    let mut completed = executor.visit_ready_send(lane).unwrap();
    assert!(matches!(
        completed.work,
        OwnerReadyUnit::Waiting | OwnerReadyUnit::SourceDrained
    ));
    let close = peer_record(
        &mut executor,
        leg,
        Record::Close {
            flow_id: local.flow_id(),
            direction: Direction::ClientToTarget,
            final_offset: ByteOffset::new(0),
        },
    );
    assert!(
        matches!(close.as_slice(), [OwnerTargetOutput::Transmit(frame)]
        if matches!(frame.record(), Record::Ack {
            flow_id, direction: Direction::ClientToTarget, next_accepted, final_accepted: false,
        } if *flow_id == local.flow_id() && *next_accepted == ByteOffset::new(0)))
    );
    held_outputs.extend(close);
    let original = match &executor.flows[&local.flow_id()].pending_completion {
        Some(PendingTargetCompletion::HalfClose {
            event: SessionEvent::SinkHalfClosed { completion },
        }) => *completion,
        _ => panic!("real HalfClose must retain its original successful completion"),
    };
    assert_eq!(original.session_id(), local.session_id());
    assert_eq!(original.flow_id(), local.flow_id());
    assert_eq!(original.direction(), Direction::ClientToTarget);
    assert_eq!(original.final_offset(), ByteOffset::new(0));
    let position = executor.flows[&local.flow_id()].service_position.unwrap();
    let generation = live_record(&executor, local.flow_id()).generation;
    assert!(live_record(&executor, local.flow_id()).wait.is_some());
    assert_eq!(live_record(&executor, local.flow_id()).transit, 0);
    assert_eq!(executor.target().snapshot().write_half_closes, 1);
    for round in 0..=3 {
        // The input is the original captured terminal capability. This is a
        // component capacity event, not a physical-clock/Root expiry claim.
        let expired = executor
            .apply_event(SessionEvent::TerminalGraceExpired { terminal })
            .unwrap();
        assert!(
            expired.is_empty(),
            "actual expiry schedules the original waiter"
        );
        assert_eq!(
            executor.supervisor.snapshot().session.terminal_tombstones(),
            0
        );
        assert!(live_record(&executor, local.flow_id()).wait.is_none());
        if round < 3 {
            let competitor = SessionFlowId::new(3 + round).unwrap();
            let outputs = peer_record(
                &mut executor,
                leg,
                Record::Reset {
                    flow_id: competitor,
                    reason: ResetReason::TargetFailure,
                },
            );
            terminal = outputs
                .iter()
                .find_map(|output| match output {
                    OwnerTargetOutput::TerminalGraceStarted { terminal } => Some(*terminal),
                    _ => None,
                })
                .expect("the real pre-imported competitor must reoccupy Z");
            assert_eq!(terminal.flow_id(), competitor);
            assert_eq!(
                executor.supervisor.snapshot().session.terminal_tombstones(),
                1
            );
            assert!(!outputs.iter().any(|output| matches!(output,
                OwnerTargetOutput::Transmit(frame) if matches!(frame.record(),
                    Record::Ack { flow_id, final_accepted: true, .. } if *flow_id == local.flow_id()))));
            held_outputs.extend(outputs);
        }
        assert!(turn.finish(&completed).is_ok());
        ready
            .finish(&claim, completed.runnable, completed.blocked)
            .unwrap();
        let mut visits = 0;
        let (next_claim, next_turn, next_completed, outputs) = loop {
            visits += 1;
            assert!(
                visits <= 32,
                "actual dirty-source service must reach Completion"
            );
            let next_claim = ready.claim(0).unwrap();
            let next_turn = executor.begin_ready_publication().unwrap();
            if next_claim.source == OwnerReadySource::OwnerContinuation {
                let mask = OwnerReadyCategory::Completion.mask();
                let selected = executor
                    .select_ready_continuation(mask, 0)
                    .unwrap()
                    .expect("real release must select the retained HalfClose");
                let candidate = selected.candidate.as_ref().unwrap();
                assert_eq!(
                    candidate.association,
                    OwnerReadyFlowAssociation::Source(local)
                );
                assert_eq!(candidate.position, position);
                assert_eq!(candidate.generation, generation);
                let owner = &executor.flows[&local.flow_id()];
                assert_eq!(owner.service_position, Some(position));
                let source = owner.source.as_ref().unwrap();
                assert_eq!(source.flow.local_flow(), local);
                assert_eq!(source.driver.local_flow(), local);
                let record = live_record(&executor, local.flow_id());
                assert_eq!(record.association, candidate.association);
                assert_eq!(record.generation, generation);
                assert!(
                    record.wait.is_none(),
                    "do not mistake an unattempted wait for retry"
                );
                assert_eq!(
                    record.transit, 0,
                    "local HalfClose cannot acquire Driver charge"
                );
                assert!(matches!(&owner.pending_completion,
                    Some(PendingTargetCompletion::HalfClose {
                        event: SessionEvent::SinkHalfClosed { completion },
                    }) if *completion == original));
                let (observed, result) =
                    executor.apply_ready_completion_observed(&selected).unwrap();
                let runnable = executor
                    .finish_ready_continuation(&selected, result, mask)
                    .unwrap();
                break (
                    next_claim,
                    next_turn,
                    SessionCommandVisit {
                        work: OwnerReadyUnit::SourceRead,
                        runnable,
                        blocked: None,
                    },
                    observed.into_progress_parts().0,
                );
            }
            let lane = match next_claim.source {
                OwnerReadySource::ControlSend => SendLane::Control,
                OwnerReadySource::OrderedSend => SendLane::OrderedSource,
                _ => panic!("no unselected inbox or Target service"),
            };
            let visit = executor.visit_ready_send(lane).unwrap();
            assert!(matches!(
                visit.work,
                OwnerReadyUnit::Waiting | OwnerReadyUnit::SourceDrained
            ));
            assert!(next_turn.finish(&visit).is_ok());
            ready
                .finish(&next_claim, visit.runnable, visit.blocked)
                .unwrap();
        };
        claim = next_claim;
        turn = next_turn;
        completed = next_completed;
        assert_eq!(executor.target().snapshot().write_half_closes, 1);
        if round < 3 {
            assert!(
                outputs.is_empty(),
                "actual full Z rejects each genuine retry"
            );
            let record = live_record(&executor, local.flow_id());
            assert!(
                record.wait.is_some(),
                "both registrations retain one original waiter"
            );
            assert_eq!(record.transit, 0);
            assert!(
                matches!(&executor.flows[&local.flow_id()].pending_completion,
                Some(PendingTargetCompletion::HalfClose {
                    event: SessionEvent::SinkHalfClosed { completion },
                }) if *completion == original)
            );
            assert_eq!(executor.snapshot().pending_target_completions, 1);
        } else {
            let expected = Frame::try_new(
                leg.generation(),
                Record::Ack {
                    flow_id: local.flow_id(),
                    direction: Direction::ClientToTarget,
                    next_accepted: ByteOffset::new(0),
                    final_accepted: true,
                },
            )
            .unwrap();
            let frames: Vec<_> = outputs
                .iter()
                .filter_map(|output| match output {
                    OwnerTargetOutput::Transmit(frame) => Some(frame),
                    _ => None,
                })
                .collect();
            assert_eq!(
                frames,
                vec![&expected],
                "one exact final ACK after capacity release"
            );
            let terminals: Vec<_> = outputs
                .iter()
                .filter_map(|output| match output {
                    OwnerTargetOutput::TerminalGraceStarted { terminal } => Some(*terminal),
                    _ => None,
                })
                .collect();
            assert_eq!(terminals.len(), 1);
            assert_eq!(terminals[0].local_flow(), local);
            assert!(!matches!(
                &executor.flows[&local.flow_id()].pending_completion,
                Some(PendingTargetCompletion::HalfClose { .. })
            ));
            // Original TerminalSettling/Source custody may remain. F or whole
            // root zero is not a consequence of successful HalfClose feedback.
            held_outputs.extend(outputs);
        }
    }
    let mut cx = Context::from_waker(Waker::noop());
    assert!(
        inbox
            .poll_selected(SessionMailboxLane::Control, &mut cx)
            .is_pending()
    );
    assert!(
        inbox
            .poll_selected(SessionMailboxLane::OrderedSource, &mut cx)
            .is_pending()
    );
    assert!(turn.finish(&completed).is_ok());
    ready
        .finish(&claim, completed.runnable, completed.blocked)
        .unwrap();
    assert_eq!(
        held_outputs
            .iter()
            .filter(|output| matches!(output,
        OwnerTargetOutput::Transmit(frame) if matches!(frame.record(),
            Record::Ack { flow_id, final_accepted: true, .. } if *flow_id == local.flow_id())))
            .count(),
        1
    );
}
