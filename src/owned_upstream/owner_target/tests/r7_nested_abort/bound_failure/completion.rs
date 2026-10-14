//! Selected completion and real Z-pressure failure custody.
//! Owns cold/local charge isolation and retained Failed feedback through retry;
//! uses the unchanged parent fixture, not sink I/O or C/root lifecycle.
//! Tests enter through actual claims and the original Executor operations.

use super::*;
use crate::owned_upstream::owner_ready::{OwnerReadyCategory, OwnerReadySource};

#[test]
fn cold_local_failure_retries_without_borrowing_another_flows_driver_charge() {
    let mut fixture = fixture_with_cold_failure(128, 4, true);
    let active = SessionFlowId::new(6).unwrap();
    assert_eq!(fixture.transit(), 0);
    let claim = fixture.ready.claim(0).unwrap();
    let turn = fixture.executor.begin_ready_publication().unwrap();
    let control = fixture
        .executor
        .visit_ready_send(SendLane::Control)
        .unwrap();
    assert!(turn.finish(&control).is_ok());
    fixture
        .ready
        .finish(&claim, control.runnable, control.blocked)
        .unwrap();
    let claim = fixture.ready.claim(0).unwrap();
    let turn = fixture.executor.begin_ready_publication().unwrap();
    fixture
        .executor
        .target_mut()
        .inner
        .feed_read(active, Bytes::from_static(b"live"))
        .unwrap();
    assert!(matches!(
        fixture.executor.read_ready_target(active).unwrap(),
        crate::owned_upstream::owner_target::read_source::BoundTargetRead::Data
    ));
    for expected in [OwnerReadyUnit::Waiting, OwnerReadyUnit::SourcePulled] {
        let pulled = fixture
            .executor
            .visit_ready_send(SendLane::OrderedSource)
            .unwrap();
        assert_eq!(pulled.work, expected);
    }
    let visit = fixture
        .executor
        .visit_ready_send(SendLane::OrderedSource)
        .unwrap();
    assert_eq!(
        visit.work,
        OwnerReadyUnit::Transfer(OwnerReadyDrive::MailboxOwned)
    );
    assert_eq!(fixture.transit_for(active), 1);
    assert_eq!(fixture.transit(), 0);
    assert_eq!(fixture.executor.snapshot().target_read_owned_bytes, 4);
    let initial_outputs = fixture
        .executor
        .apply_event(SessionEvent::TerminalGraceExpired {
            terminal: fixture.terminals[0],
        })
        .expect("actual cold Failure reduction has no Driver charge to consume");
    assert!(
        initial_outputs.is_empty(),
        "capacity release cannot retry unselected work"
    );
    assert!(turn.finish(&visit).is_ok());
    fixture
        .ready
        .finish(&claim, visit.runnable, visit.blocked)
        .unwrap();
    let claim = fixture.ready.claim(0).unwrap();
    assert_eq!(claim.source, OwnerReadySource::OwnerContinuation);
    let turn = fixture.executor.begin_ready_publication().unwrap();
    let mask = OwnerReadyCategory::Completion.mask();
    let selected = fixture
        .executor
        .select_ready_continuation(mask, 0)
        .unwrap()
        .expect("original cold failure is selected after real capacity release");
    assert_eq!(
        selected.candidate.as_ref().unwrap().association.flow_id(),
        fixture.local.flow_id()
    );
    let (observed, result) = fixture
        .executor
        .apply_ready_completion_observed(&selected)
        .unwrap();
    let outputs = observed.into_progress_parts().0;
    let runnable = fixture
        .executor
        .finish_ready_continuation(&selected, result, mask)
        .unwrap();
    let visit = SessionCommandVisit {
        work: OwnerReadyUnit::SourceRead,
        runnable,
        blocked: None,
    };
    assert_eq!(outputs.iter().filter(|output| matches!(output,
        OwnerTargetOutput::TerminalGraceStarted { terminal } if terminal.local_flow() == fixture.local)).count(), 1);
    assert_eq!(fixture.executor.target().write_calls, 1);
    assert!(matches!(
        fixture.executor.flows[&fixture.local.flow_id()].pending_completion,
        Some(PendingTargetCompletion::TerminalSettling(_))
    ));
    assert_eq!(fixture.transit(), 0);
    assert_eq!(fixture.transit_for(active), 1);
    assert_eq!(fixture.executor.snapshot().target_read_owned_bytes, 4);
    let Poll::Ready(Some(data)) = fixture.inbox.poll_selected(
        SessionMailboxLane::OrderedSource,
        &mut Context::from_waker(Waker::noop()),
    ) else {
        panic!("other Flow DATA remains in its original paired inbox");
    };
    let (outputs, _) = fixture
        .executor
        .apply_ready_receipt_observed(data)
        .unwrap()
        .into_progress_parts();
    let frames = transmitted(&outputs);
    assert_eq!(frames.len(), 1);
    assert!(matches!(frames[0], Record::Data { flow_id, payload, .. }
        if *flow_id == active && payload.as_ref() == b"live"));
    assert_eq!(fixture.transit_for(active), 0);
    assert_eq!(fixture.transit(), 0);
    assert!(turn.finish(&visit).is_ok());
    fixture
        .ready
        .finish(&claim, visit.runnable, visit.blocked)
        .unwrap();
}

#[test]
fn real_z_pressure_retains_failed_work_and_driver_charge_until_actual_grace_release() {
    let mut fixture = fixture_with_bounds(128, 4);
    let claim = fixture.ready.claim(0).unwrap();
    let turn = fixture.executor.begin_ready_publication().unwrap();
    fixture.stage_write_failure();
    let (original, pointer) =
        match &fixture.executor.flows[&fixture.local.flow_id()].pending_completion {
            Some(PendingTargetCompletion::Failed {
                work: OwnerTargetWork::Effect(SessionEffect::OfferToSink { offer, segments }),
                ..
            }) => (*offer, segments[0].payload().as_ptr()),
            _ => panic!("actual work must be retained"),
        };
    let (visit, reset) = fixture.send_control_to_inbox();
    assert!(
        fixture
            .executor
            .apply_ready_receipt_observed(reset)
            .unwrap()
            .into_progress_parts()
            .0
            .is_empty()
    );
    assert_eq!(fixture.transit(), 1);
    let Some(PendingTargetCompletion::Failed {
        work: OwnerTargetWork::Effect(SessionEffect::OfferToSink { offer, segments }),
        cause,
        feedback: TargetFailureFeedback::Driver(DriverCompletionState::Returned(command)),
    }) = &fixture.executor.flows[&fixture.local.flow_id()].pending_completion
    else {
        panic!("Z pressure must preserve Failed work and the actual carrying rejection together");
    };
    assert_eq!(*offer, original);
    assert_eq!(segments[0].payload().as_ptr(), pointer);
    assert_eq!(*cause, TargetFailure::ConnectionLost);
    assert!(
        matches!(command.as_ref(), SessionOwnerCommand::Driver(DriverInput::Control(
        SessionEvent::LocalReset { flow, reason: ResetReason::TargetFailure })) if *flow == fixture.local)
    );
    let initial_outputs = fixture
        .executor
        .apply_event(SessionEvent::TerminalGraceExpired {
            terminal: fixture.terminals[0],
        })
        .unwrap();
    assert!(
        initial_outputs.is_empty(),
        "capacity release cannot retry unselected work"
    );
    assert_eq!(
        fixture.transit(),
        1,
        "the actual returned RESET still owns its charge"
    );
    assert!(turn.finish(&visit).is_ok());
    fixture
        .ready
        .finish(&claim, visit.runnable, visit.blocked)
        .unwrap();
    let claim = fixture.ready.claim(0).unwrap();
    assert_eq!(claim.source, OwnerReadySource::OrderedSend);
    let turn = fixture.executor.begin_ready_publication().unwrap();
    let visit = fixture
        .executor
        .visit_ready_send(SendLane::OrderedSource)
        .unwrap();
    assert_eq!(visit.work, OwnerReadyUnit::Waiting);
    assert!(turn.finish(&visit).is_ok());
    fixture
        .ready
        .finish(&claim, visit.runnable, visit.blocked)
        .unwrap();
    let claim = fixture.ready.claim(0).unwrap();
    assert_eq!(claim.source, OwnerReadySource::OwnerContinuation);
    let turn = fixture.executor.begin_ready_publication().unwrap();
    let mask = OwnerReadyCategory::Completion.mask();
    let selected = fixture
        .executor
        .select_ready_continuation(mask, 0)
        .unwrap()
        .expect("the actual capacity waiter must be selected, not globally retried");
    assert_eq!(
        selected.candidate.as_ref().unwrap().association.flow_id(),
        fixture.local.flow_id()
    );
    let (observed, result) = fixture
        .executor
        .apply_ready_completion_observed(&selected)
        .unwrap();
    let outputs = observed.into_progress_parts().0;
    let runnable = fixture
        .executor
        .finish_ready_continuation(&selected, result, mask)
        .unwrap();
    let visit = SessionCommandVisit {
        work: OwnerReadyUnit::SourceRead,
        runnable,
        blocked: None,
    };
    assert_eq!(fixture.transit(), 0);
    assert_eq!(outputs.iter().filter(|output| matches!(output,
        OwnerTargetOutput::TerminalGraceStarted { terminal } if terminal.local_flow() == fixture.local)).count(), 1);
    assert_eq!(fixture.executor.target().write_calls, 1);
    assert!(matches!(
        fixture.executor.flows[&fixture.local.flow_id()].pending_completion,
        Some(PendingTargetCompletion::TerminalSettling(_))
    ));
    assert!(turn.finish(&visit).is_ok());
    fixture
        .ready
        .finish(&claim, visit.runnable, visit.blocked)
        .unwrap();
}

#[test]
fn selected_terminal_disposes_returned_reset_without_waiting_for_a_second_receipt() {
    let mut fixture = fixture_with_bounds(128, 4);
    let id = fixture.local.flow_id();
    let position = fixture.executor.flows[&id].service_position.unwrap();
    let claim = fixture.ready.claim(0).unwrap();
    let turn = fixture.executor.begin_ready_publication().unwrap();
    fixture.stage_write_failure();
    let (visit, reset) = fixture.send_control_to_inbox();
    assert!(
        fixture
            .executor
            .apply_ready_receipt_observed(reset)
            .unwrap()
            .into_progress_parts()
            .0
            .is_empty()
    );
    assert_eq!(fixture.transit(), 1);
    assert!(matches!(&fixture.executor.flows[&id].pending_completion,
        Some(PendingTargetCompletion::Failed {
            cause: TargetFailure::ConnectionLost,
            feedback: TargetFailureFeedback::Driver(DriverCompletionState::Returned(command)), ..
        }) if matches!(command.as_ref(), SessionOwnerCommand::Driver(DriverInput::Control(
            SessionEvent::LocalReset { flow, reason: ResetReason::TargetFailure }
        )) if *flow == fixture.local)));
    assert!(
        fixture
            .executor
            .apply_event(SessionEvent::TerminalGraceExpired {
                terminal: fixture.terminals[0],
            })
            .unwrap()
            .is_empty(),
        "actual capacity release does not retry globally"
    );
    // A real peer terminal wins before the pending returned RESET is retried.
    let outputs = peer_frame(
        &mut fixture.executor,
        fixture.leg,
        Record::Reset {
            flow_id: id,
            reason: ResetReason::LocalAbandon,
        },
    )
    .unwrap();
    let terminal = outputs
        .into_iter()
        .find_map(|output| match output {
            OwnerTargetOutput::TerminalGraceStarted { terminal }
                if terminal.local_flow() == fixture.local =>
            {
                Some(terminal)
            }
            _ => None,
        })
        .expect("original Supervisor supplies actual terminal proof");
    assert!(
        fixture
            .executor
            .apply_event(SessionEvent::TerminalGraceExpired { terminal })
            .unwrap()
            .is_empty()
    );
    assert!(turn.finish(&visit).is_ok());
    fixture
        .ready
        .finish(&claim, visit.runnable, visit.blocked)
        .unwrap();
    let mask = OwnerReadyCategory::Retirement.mask();
    let mut retirement_visits = 0;
    for _ in 0..16 {
        if fixture.ready.select(3).unwrap().is_none() {
            break;
        }
        let claim = fixture.ready.claim(0).unwrap();
        let turn = fixture.executor.begin_ready_publication().unwrap();
        let completed = match claim.source {
            OwnerReadySource::ControlSend | OwnerReadySource::OrderedSend => {
                let lane = if claim.source == OwnerReadySource::ControlSend {
                    SendLane::Control
                } else {
                    SendLane::OrderedSource
                };
                fixture.executor.visit_ready_send(lane).unwrap()
            }
            OwnerReadySource::OwnerContinuation => {
                let selected = fixture
                    .executor
                    .select_ready_continuation(mask, 0)
                    .unwrap()
                    .expect("actual terminal remains selected");
                assert_eq!(
                    selected.candidate.as_ref().unwrap().association.flow_id(),
                    id
                );
                retirement_visits += 1;
                let result = fixture.executor.service_ready_target(&selected).unwrap();
                let runnable = fixture
                    .executor
                    .finish_ready_continuation(&selected, result, mask)
                    .unwrap();
                SessionCommandVisit {
                    work: OwnerReadyUnit::SourceRead,
                    runnable,
                    blocked: None,
                }
            }
            _ => panic!("the paired inbox already returned the only actual receipt"),
        };
        assert!(turn.finish(&completed).is_ok());
        fixture
            .ready
            .finish(&claim, completed.runnable, completed.blocked)
            .unwrap();
        let OwnerTargetServiceState::Bound { flows, .. } = &fixture.executor.service else {
            panic!("original binding");
        };
        let OwnerFlowServiceSlot::Retiring(record) = &flows.slots[position] else {
            panic!("original terminal F");
        };
        if record.transit == 0 {
            break;
        }
    }
    assert!(retirement_visits > 0);
    let OwnerTargetServiceState::Bound { flows, .. } = &fixture.executor.service else {
        panic!("original binding");
    };
    let OwnerFlowServiceSlot::Retiring(record) = &flows.slots[position] else {
        panic!("original terminal F");
    };
    assert_eq!(
        record.association,
        crate::owned_upstream::owner_ready::OwnerReadyFlowAssociation::Source(fixture.local)
    );
    assert_eq!(
        record.transit, 0,
        "Returned already owns the command; no second receipt will arrive"
    );
    assert!(matches!(
        fixture.executor.flows[&id].pending_completion,
        Some(PendingTargetCompletion::TerminalSettling(_))
    ));
    assert!(
        fixture.executor.flows[&id].source.is_some(),
        "Failed work and Source release are distinct visits"
    );
    assert_eq!(fixture.executor.target().write_calls, 1);
    let mut cx = Context::from_waker(Waker::noop());
    assert!(
        fixture
            .inbox
            .poll_selected(SessionMailboxLane::Control, &mut cx)
            .is_pending()
    );
}

#[test]
fn selected_terminal_releases_actual_failed_work_before_reclaiming_its_flow() {
    for (occupied, receipt_after_terminal) in [(0, false), (4, false), (0, true)] {
        let mut fixture = fixture_with_bounds(128, occupied);
        let id = fixture.local.flow_id();
        let position = fixture.executor.flows[&id].service_position.unwrap();
        let claim = fixture.ready.claim(0).unwrap();
        let turn = fixture.executor.begin_ready_publication().unwrap();
        fixture.stage_write_failure();
        let payload = match &fixture.executor.flows[&id].pending_completion {
            Some(PendingTargetCompletion::Failed {
                cause: TargetFailure::ConnectionLost,
                work: OwnerTargetWork::Effect(SessionEffect::OfferToSink { segments, .. }),
                ..
            }) if segments.len() == 1 && segments[0].payload().as_ref() == b"held" => {
                segments[0].payload().clone()
            }
            _ => panic!("actual normalized Failed payload"),
        };
        let (visit, reset) = fixture.send_control_to_inbox();
        let outputs = if receipt_after_terminal {
            let outputs = peer_frame(
                &mut fixture.executor,
                fixture.leg,
                Record::Reset {
                    flow_id: id,
                    reason: ResetReason::LocalAbandon,
                },
            )
            .unwrap();
            assert_eq!(
                fixture.transit(),
                1,
                "a real in-flight receipt still owns its charge"
            );
            assert!(!payload.is_unique());
            assert!(
                fixture
                    .executor
                    .apply_ready_receipt_observed(reset)
                    .unwrap()
                    .into_progress_parts()
                    .0
                    .is_empty(),
                "late RESET settles only its own charge, not a second terminal"
            );
            outputs
        } else {
            fixture
                .executor
                .apply_ready_receipt_observed(reset)
                .unwrap()
                .into_progress_parts()
                .0
        };
        let outputs = if occupied == 0 {
            outputs
        } else {
            assert!(outputs.is_empty());
            assert_eq!(fixture.transit(), 1);
            assert!(
                fixture
                    .executor
                    .apply_event(SessionEvent::TerminalGraceExpired {
                        terminal: fixture.terminals[0],
                    })
                    .unwrap()
                    .is_empty()
            );
            peer_frame(
                &mut fixture.executor,
                fixture.leg,
                Record::Reset {
                    flow_id: id,
                    reason: ResetReason::LocalAbandon,
                },
            )
            .unwrap()
        };
        let terminal = outputs
            .into_iter()
            .find_map(|output| match output {
                OwnerTargetOutput::TerminalGraceStarted { terminal }
                    if terminal.local_flow() == fixture.local =>
                {
                    Some(terminal)
                }
                _ => None,
            })
            .expect("actual reducer supplies the original terminal");
        assert_eq!(fixture.transit(), usize::from(occupied != 0));
        assert!(
            !payload.is_unique(),
            "settled RESET is not Failed work release"
        );
        assert!(
            fixture
                .executor
                .apply_event(SessionEvent::TerminalGraceExpired { terminal })
                .unwrap()
                .is_empty()
        );
        assert!(turn.finish(&visit).is_ok());
        fixture
            .ready
            .finish(&claim, visit.runnable, visit.blocked)
            .unwrap();
        let mask = OwnerReadyCategory::Retirement.mask();
        let mut observed_release_with_original_source = false;
        for _ in 0..32 {
            if fixture.ready.select(3).unwrap().is_none() {
                break;
            }
            let claim = fixture.ready.claim(0).unwrap();
            let turn = fixture.executor.begin_ready_publication().unwrap();
            let completed = match claim.source {
                OwnerReadySource::ControlSend | OwnerReadySource::OrderedSend => {
                    let lane = if claim.source == OwnerReadySource::ControlSend {
                        SendLane::Control
                    } else {
                        SendLane::OrderedSource
                    };
                    fixture.executor.visit_ready_send(lane).unwrap()
                }
                OwnerReadySource::OwnerContinuation => {
                    let selected = fixture
                        .executor
                        .select_ready_continuation(mask, 0)
                        .unwrap()
                        .expect("real retained terminal is selected");
                    assert_eq!(
                        selected.candidate.as_ref().unwrap().association.flow_id(),
                        id
                    );
                    let before = payload.is_unique();
                    let result = fixture.executor.service_ready_target(&selected).unwrap();
                    if !before && payload.is_unique() {
                        assert!(
                            fixture.executor.flows[&id].source.is_some(),
                            "original Source remains owned throughout Failed destruction"
                        );
                        let OwnerTargetServiceState::Bound { flows, .. } =
                            &fixture.executor.service
                        else {
                            panic!("original binding");
                        };
                        let OwnerFlowServiceSlot::Retiring(record) = &flows.slots[position] else {
                            panic!("original F not released by destructor");
                        };
                        assert_eq!(record.transit, 0);
                        observed_release_with_original_source = true;
                    }
                    let runnable = fixture
                        .executor
                        .finish_ready_continuation(&selected, result, mask)
                        .unwrap();
                    SessionCommandVisit {
                        work: OwnerReadyUnit::SourceRead,
                        runnable,
                        blocked: None,
                    }
                }
                _ => panic!("no second receipt or unrelated source is expected"),
            };
            assert!(turn.finish(&completed).is_ok());
            fixture
                .ready
                .finish(&claim, completed.runnable, completed.blocked)
                .unwrap();
            if !fixture.executor.flows.contains_key(&id) {
                break;
            }
        }
        assert!(
            payload.is_unique(),
            "actual Failed work must release its original normalized backing"
        );
        assert!(observed_release_with_original_source);
        assert!(!fixture.executor.flows.contains_key(&id));
        let OwnerTargetServiceState::Bound { flows, .. } = &fixture.executor.service else {
            panic!("original binding");
        };
        assert!(matches!(flows.slots[position], OwnerFlowServiceSlot::Free));
        assert_eq!(fixture.executor.target().write_calls, 1);
        let mut cx = Context::from_waker(Waker::noop());
        assert!(
            fixture
                .inbox
                .poll_selected(SessionMailboxLane::Control, &mut cx)
                .is_pending()
        );
    }
}

#[test]
fn selected_join_readiness_failure_stays_at_original_terminal_without_repoll_or_join() {
    for fault in [
        Ok(TargetReadyOutcome::Failed(TargetFailure::TimedOut)),
        Err(TargetIoError::ReadinessInvariant("selected Join readiness")),
    ] {
        // Ready is bound first so it outlives the original Executor and inbox.
        let BoundFailureFixture {
            mut ready,
            mut executor,
            mut inbox,
            leg,
            local,
            ..
        } = fixture();
        let id = local.flow_id();
        let position = executor.flows[&id].service_position.unwrap();
        // This instance needs a real graceful backlog, not a failed Write.
        executor.target_mut().write = None;
        let claim = ready.claim(0).unwrap();
        let turn = executor.begin_ready_publication().unwrap();
        let control = executor.visit_ready_send(SendLane::Control).unwrap();
        assert!(turn.finish(&control).is_ok());
        ready
            .finish(&claim, control.runnable, control.blocked)
            .unwrap();
        let claim = ready.claim(0).unwrap();
        assert_eq!(claim.source, OwnerReadySource::OrderedSend);
        let turn = executor.begin_ready_publication().unwrap();
        for record in [
            Record::Data {
                flow_id: id,
                direction: Direction::ClientToTarget,
                offset: ByteOffset::new(0),
                payload: Bytes::from_static(b"held"),
            },
            Record::Close {
                flow_id: id,
                direction: Direction::ClientToTarget,
                final_offset: ByteOffset::new(4),
            },
        ] {
            let outputs = peer_frame(&mut executor, leg, record).unwrap();
            assert!(!transmitted(&outputs).is_empty());
        }
        executor.target_mut().inner.finish_read(id).unwrap();
        assert!(matches!(
            executor.read_ready_target(id).unwrap(),
            crate::owned_upstream::owner_target::read_source::BoundTargetRead::Eof
        ));
        let pulled = executor.visit_ready_send(SendLane::OrderedSource).unwrap();
        assert_eq!(pulled.work, OwnerReadyUnit::SourcePulled);
        let visit = executor.visit_ready_send(SendLane::OrderedSource).unwrap();
        assert_eq!(
            visit.work,
            OwnerReadyUnit::Transfer(OwnerReadyDrive::MailboxOwned)
        );
        let Poll::Ready(Some(close)) = inbox.poll_selected(
            SessionMailboxLane::OrderedSource,
            &mut Context::from_waker(Waker::noop()),
        ) else {
            panic!("actual EOF receipt");
        };
        let outputs = executor
            .apply_ready_receipt_observed(close)
            .unwrap()
            .into_progress_parts()
            .0;
        assert!(matches!(transmitted(&outputs).as_slice(), [Record::Close {
            flow_id, direction: Direction::TargetToClient, final_offset,
        }] if *flow_id == id && *final_offset == ByteOffset::new(0)));
        let outputs = peer_frame(
            &mut executor,
            leg,
            Record::Ack {
                flow_id: id,
                direction: Direction::TargetToClient,
                next_accepted: ByteOffset::new(0),
                final_accepted: true,
            },
        )
        .unwrap();
        let [OwnerTargetOutput::TerminalGraceStarted { terminal }] = outputs.as_slice() else {
            panic!("actual graceful protocol completion");
        };
        assert_eq!(terminal.local_flow(), local);
        assert_eq!(executor.snapshot().pending_joins, 1);
        assert_eq!(executor.target().inner.snapshot().buffered_bytes, 4);
        // Component expiry of the original proof, not a physical timer claim.
        assert!(
            executor
                .apply_event(SessionEvent::TerminalGraceExpired {
                    terminal: *terminal,
                })
                .unwrap()
                .is_empty()
        );
        let join_calls = executor.target().join_calls;
        executor.target_mut().join_readiness = Some(fault);
        assert!(turn.finish(&visit).is_ok());
        ready.finish(&claim, visit.runnable, visit.blocked).unwrap();
        let mask = OwnerReadyCategory::Retirement.mask();
        let mut reached_fault = false;
        for _ in 0..24 {
            let claim = ready.claim(0).unwrap();
            let turn = executor.begin_ready_publication().unwrap();
            let completed = match claim.source {
                OwnerReadySource::ControlSend | OwnerReadySource::OrderedSend => {
                    let lane = if claim.source == OwnerReadySource::ControlSend {
                        SendLane::Control
                    } else {
                        SendLane::OrderedSource
                    };
                    executor.visit_ready_send(lane).unwrap()
                }
                OwnerReadySource::OwnerContinuation => {
                    let selected = executor
                        .select_ready_continuation(mask, 0)
                        .unwrap()
                        .expect("original Retirement candidate");
                    assert_eq!(
                        selected.candidate.as_ref().unwrap().association.flow_id(),
                        id
                    );
                    let result = executor.service_ready_target(&selected);
                    if executor.target().join_readiness.is_none() {
                        assert!(result.is_err(), "the actual injected result is a failure");
                        assert_eq!(executor.target().join_readiness_calls, 1);
                        // Same valid publication/selection: no fatal-domain shortcut.
                        for _ in 0..3 {
                            assert!(
                                executor.service_ready_target(&selected).is_err(),
                                "a retained Join fault cannot resume polling or joining"
                            );
                        }
                        assert_eq!(executor.target().join_readiness_calls, 1);
                        assert_eq!(executor.target().join_calls, join_calls);
                        let owner = &executor.flows[&id];
                        assert!(
                            owner.target_present && owner.join_pending && owner.retire_after_join
                        );
                        assert_eq!(
                            executor.target().readiness(id).unwrap().terminal,
                            Some(TargetTerminalKind::Graceful)
                        );
                        let source = owner.source.as_ref().expect("original Source retained");
                        assert_eq!(source.flow.local_flow(), local);
                        assert_eq!(source.driver.local_flow(), local);
                        assert!(matches!(
                            owner.pending_completion,
                            Some(PendingTargetCompletion::TerminalSettling(_))
                        ));
                        let OwnerTargetServiceState::Bound { flows, .. } = &executor.service else {
                            panic!("original Bound owner");
                        };
                        let OwnerFlowServiceSlot::Retiring(record) = &flows.slots[position] else {
                            panic!("original F remains Retiring");
                        };
                        assert_eq!(record.transit, 0);
                        let inventory = executor.target().inner.snapshot();
                        assert_eq!(
                            (
                                inventory.live_flows,
                                inventory.joined_flows,
                                inventory.buffered_bytes
                            ),
                            (1, 0, 4)
                        );
                        assert!(
                            inbox
                                .poll_selected(
                                    SessionMailboxLane::Control,
                                    &mut Context::from_waker(Waker::noop())
                                )
                                .is_pending()
                        );
                        reached_fault = true;
                        // Only now settle the actual failed publication as fatal;
                        // the re-entry assertions above did not rely on quarantine.
                        drop(turn);
                        break;
                    }
                    let runnable = executor
                        .finish_ready_continuation(&selected, result.unwrap(), mask)
                        .unwrap();
                    SessionCommandVisit {
                        work: OwnerReadyUnit::SourceRead,
                        runnable,
                        blocked: None,
                    }
                }
                _ => panic!("no second receipt or unrelated source"),
            };
            assert!(turn.finish(&completed).is_ok());
            ready
                .finish(&claim, completed.runnable, completed.blocked)
                .unwrap();
        }
        assert!(reached_fault, "the real selected Join poll must execute");
    }
}
