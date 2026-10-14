//! Independent incoming sink custody through real failure and terminal ordering.
//! Owns Write/HalfClose retention tests; not source-credit or whole-root cleanup.
//! Uses the parent owning fixture and unchanged failure Adapter.

use super::*;

#[test]
fn terminal_revoke_wake_unwind_preserves_failed_sink_and_real_terminal() {
    use crate::owned_upstream::owner_target::completion::CompletionReceipt;
    use crate::owned_upstream::tcp::FlowPortError;

    let mut fixture = fixture_with_steps(1);
    let flow_id = fixture.local.flow_id();
    fixture.executor.target_mut().write = None;
    fixture.executor.target_mut().read = Some(TargetFailure::TimedOut);
    let _claim = fixture.ready.claim(0).unwrap();
    let turn = fixture.executor.begin_ready_publication().unwrap();
    assert!(matches!(
        fixture.executor.read_ready_target(flow_id).unwrap(),
        crate::owned_upstream::owner_target::read_source::BoundTargetRead::FailureStaged
    ));
    assert!(
        peer_frame(
            &mut fixture.executor,
            fixture.leg,
            Record::Data {
                flow_id,
                direction: Direction::ClientToTarget,
                offset: ByteOffset::new(0),
                payload: Bytes::from_static(b"sink"),
            }
        )
        .unwrap()
        .is_empty()
    );
    let Some(PendingTargetSink::Write(pending)) = &fixture.executor.flows[&flow_id].pending_sink
    else {
        panic!("real incoming DATA must retain its original Write capability");
    };
    let (offer, pointer) = (pending.offer, pending.segments[0].payload().as_ptr());
    let (_visit, reset) = fixture.send_control_to_inbox();
    assert_eq!(fixture.transit(), 1);
    let outputs = peer_frame(
        &mut fixture.executor,
        fixture.leg,
        Record::Reset {
            flow_id,
            reason: ResetReason::TargetFailure,
        },
    )
    .unwrap();
    let resume = outputs
        .into_iter()
        .find_map(|output| match output {
            OwnerTargetOutput::NeedsResume { resume } => Some(resume),
            _ => None,
        })
        .expect("one-step PeerReset must leave the real FlowFinished queued");
    let Some(OwnerTargetWork::Effect(SessionEffect::FlowFinished {
        flow_id: finished,
        terminal,
        reason,
    })) = fixture.executor.pending_effects.front()
    else {
        panic!("capture only the actual Supervisor-minted terminal, never a synthetic proof");
    };
    assert_eq!(*finished, flow_id);
    let (terminal, reason) = (*terminal, *reason);
    assert_eq!(terminal.local_flow(), fixture.local);
    let before_target = fixture.executor.target().inner.snapshot();
    let factory = fixture.executor.source_factory.clone();
    let cursor = factory.progress_cursor();
    let mut wait = Box::pin(factory.wait_for_progress(&cursor));
    let signal = Arc::new(PanicProgressOnce(AtomicBool::new(true)));
    let waker = Waker::from(Arc::clone(&signal));
    assert!(
        wait.as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            fixture.executor.resume_pending_effects(resume)
        }))
        .is_err()
    );
    assert!(!signal.0.load(Ordering::SeqCst));
    drop(wait);

    // Re-entry cannot consume the retained proof or perform a second Target operation.
    for _ in 0..2 {
        let owner = &fixture.executor.flows[&flow_id];
        let completion = owner.pending_completion.as_ref().unwrap();
        assert!(matches!(
            completion,
            PendingTargetCompletion::TerminalSettling(_)
        ));
        assert!(matches!(
            completion.classify_receipt(&reset),
            Some(CompletionReceipt::TerminalDisposition)
        ));
        assert_eq!(
            fixture
                .executor
                .event_admission_block(&SessionEvent::TerminalGraceExpired { terminal }),
            Some(OwnerTargetInputBlock::AbortedEffectInvariant),
            "the owning outer journal blocks unknown terminal disposition"
        );
        assert!(
            matches!(
                fixture.executor.aborted_effects.as_ref().unwrap().current.as_ref(),
                Some(OwnerTargetWork::Effect(SessionEffect::FlowFinished { terminal: retained, .. }))
                    if *retained == terminal
            ),
            "the original full terminal proof remains journal-held"
        );
        assert!(owner.target_present && owner.terminal.is_none() && !owner.join_pending);
        let source = owner.source.as_ref().unwrap();
        assert_eq!(source.flow.local_flow(), fixture.local);
        assert_eq!(source.driver.local_flow(), fixture.local);
        let Some(PendingTargetSink::Write(pending)) = &owner.pending_sink else {
            panic!("unwinding the revoke wake must retain the same sink capability");
        };
        assert_eq!(pending.offer, offer);
        assert_eq!(pending.segments.len(), 1);
        assert_eq!(pending.segments[0].payload().as_ptr(), pointer);
        assert_eq!(pending.segments[0].payload().as_ref(), b"sink");
        assert_eq!(fixture.transit(), 1);
        assert_eq!(fixture.executor.target().inner.snapshot(), before_target);
        assert_eq!(fixture.executor.target().read_calls, 1);
        assert_eq!(fixture.executor.target().write_calls, 0);
        assert_eq!(fixture.executor.target().half_close_calls, 0);
        assert!(matches!(
            fixture.executor.finish_target_flow(terminal, reason),
            Err(OwnerTargetError::DuplicatePendingTargetCompletion { flow_id: duplicate })
                if duplicate == flow_id
        ));
    }
    let source = fixture
        .executor
        .flows
        .get_mut(&flow_id)
        .unwrap()
        .source
        .as_mut()
        .unwrap();
    assert_eq!(
        source
            .driver
            .revoke_owner_terminal_source(&source.flow, &factory, terminal),
        Err(FlowPortError::TerminalAlreadyDelivered),
        "revocation committed and released the real admission lock before the wake"
    );
    assert_eq!(
        source.flow.try_send_reset(ResetReason::TargetFailure),
        Err(FlowPortError::Closed)
    );
    drop(turn); // Unknown publication and the retained RESET are not clean completion.
}

#[test]
fn incoming_close_after_read_failure_retains_half_close_until_real_terminal() {
    let mut fixture = fixture_with_steps(1);
    let flow_id = fixture.local.flow_id();
    fixture.executor.target_mut().read = Some(TargetFailure::TimedOut);
    let claim = fixture.ready.claim(0).unwrap();
    let turn = fixture.executor.begin_ready_publication().unwrap();
    assert!(matches!(
        fixture.executor.read_ready_target(flow_id).unwrap(),
        crate::owned_upstream::owner_target::read_source::BoundTargetRead::FailureStaged
    ));
    let outputs = peer_frame(
        &mut fixture.executor,
        fixture.leg,
        Record::Close {
            flow_id,
            direction: Direction::ClientToTarget,
            final_offset: ByteOffset::new(0),
        },
    )
    .expect("legal CLOSE must coexist with the original Failed read");
    assert_eq!(
        transmitted(&outputs),
        vec![&Record::Ack {
            flow_id,
            direction: Direction::ClientToTarget,
            next_accepted: ByteOffset::new(0),
            final_accepted: false,
        }]
    );
    let completion = match fixture.executor.pending_effects.front().unwrap() {
        OwnerTargetWork::Effect(SessionEffect::HalfCloseSink { completion }) => *completion,
        _ => panic!("one-step CLOSE retains the actual half-close effect"),
    };
    assert_eq!(completion.flow_id(), flow_id);
    assert_eq!(completion.session_id(), fixture.local.session_id());
    assert_eq!(completion.direction(), Direction::ClientToTarget);
    assert_eq!(completion.final_offset(), ByteOffset::new(0));
    let resume = outputs
        .into_iter()
        .find_map(|output| match output {
            OwnerTargetOutput::NeedsResume { resume } => Some(resume),
            _ => None,
        })
        .unwrap();
    assert!(
        fixture
            .executor
            .resume_pending_effects(resume)
            .unwrap()
            .is_empty()
    );
    assert_eq!(fixture.executor.target().half_close_calls, 0);
    let owner = &fixture.executor.flows[&flow_id];
    assert!(owner.target_present && owner.terminal.is_none());
    assert!(
        matches!(&owner.pending_sink, Some(PendingTargetSink::HalfClose(held))
        if *held == completion)
    );
    assert!(
        matches!(&owner.pending_completion, Some(PendingTargetCompletion::Failed {
        work: OwnerTargetWork::TargetIoInvariant { flow_id: failed, operation: TargetIoOperation::Read },
        cause: TargetFailure::TimedOut, feedback: TargetFailureFeedback::InTransit(local),
    }) if *failed == flow_id && *local == fixture.local)
    );
    assert_eq!(fixture.executor.snapshot().pending_writes, 0);
    assert_eq!(fixture.executor.snapshot_without_supervisor().2, 0);
    assert!(matches!(fixture.executor.retry_target_write(flow_id),
        Err(OwnerTargetError::PendingTargetCompletion { flow_id: blocked }) if blocked == flow_id));
    assert_eq!(fixture.executor.target().half_close_calls, 0);
    assert!(matches!(&fixture.executor.flows[&flow_id].pending_sink,
        Some(PendingTargetSink::HalfClose(held)) if *held == completion));
    let outputs = peer_frame(
        &mut fixture.executor,
        fixture.leg,
        Record::Reset {
            flow_id,
            reason: ResetReason::TargetFailure,
        },
    )
    .unwrap();
    let owner = &fixture.executor.flows[&flow_id];
    assert!(owner.target_present && owner.terminal.is_none());
    assert!(
        matches!(&owner.pending_sink, Some(PendingTargetSink::HalfClose(held))
        if *held == completion)
    );
    assert!(owner.target_present && owner.terminal.is_none());
    assert!(matches!(
        owner.pending_completion,
        Some(PendingTargetCompletion::Failed { .. })
    ));
    let resume = outputs
        .into_iter()
        .find_map(|output| match output {
            OwnerTargetOutput::NeedsResume { resume } => Some(resume),
            _ => None,
        })
        .unwrap();
    let outputs = fixture.executor.resume_pending_effects(resume).unwrap();
    assert_eq!(outputs.iter().filter(|output| matches!(output,
        OwnerTargetOutput::TerminalGraceStarted { terminal } if terminal.local_flow() == fixture.local)).count(), 1);
    let owner = &fixture.executor.flows[&flow_id];
    assert!(owner.pending_sink.is_none() && owner.source.is_some() && !owner.target_present);
    assert!(matches!(
        owner.pending_completion,
        Some(PendingTargetCompletion::TerminalSettling(_))
    ));
    let (visit, reset) = fixture.send_control_to_inbox();
    assert_eq!(fixture.transit(), 1);
    let (outputs, observed) = fixture
        .executor
        .apply_ready_receipt_observed(reset)
        .unwrap()
        .into_progress_parts();
    assert!(outputs.is_empty());
    assert_eq!(observed.fact_count(), 0);
    assert_eq!(fixture.transit(), 0);
    assert_eq!(fixture.executor.target().read_calls, 1);
    assert_eq!(fixture.executor.target().half_close_calls, 0);
    assert!(turn.finish(&visit).is_ok());
    fixture
        .ready
        .finish(&claim, visit.runnable, visit.blocked)
        .unwrap();
}

#[test]
fn incoming_data_after_read_failure_retains_original_offer_until_real_terminal() {
    let mut fixture = fixture_with_steps(1);
    let flow_id = fixture.local.flow_id();
    fixture.executor.target_mut().write = None;
    fixture.executor.target_mut().read = Some(TargetFailure::TimedOut);
    let claim = fixture.ready.claim(0).unwrap();
    let turn = fixture.executor.begin_ready_publication().unwrap();
    assert!(matches!(
        fixture.executor.read_ready_target(flow_id).unwrap(),
        crate::owned_upstream::owner_target::read_source::BoundTargetRead::FailureStaged
    ));
    assert!(
        peer_frame(
            &mut fixture.executor,
            fixture.leg,
            Record::Data {
                flow_id,
                direction: Direction::ClientToTarget,
                offset: ByteOffset::new(0),
                payload: Bytes::from_static(b"sink"),
            }
        )
        .expect("legal incoming DATA must retain sink work beside the original Failed read")
        .is_empty()
    );
    let Some(PendingTargetSink::Write(pending)) = &fixture.executor.flows[&flow_id].pending_sink
    else {
        panic!("actual incoming Write capability must stay in the original sink slot");
    };
    let (offer, pointer) = (pending.offer, pending.segments[0].payload().as_ptr());
    assert_eq!(offer.flow_id(), flow_id);
    assert_eq!(pending.segments.len(), 1);
    assert_eq!(pending.segments[0].payload().as_ref(), b"sink");
    let owner = &fixture.executor.flows[&flow_id];
    let Some(PendingTargetCompletion::Failed {
        work:
            OwnerTargetWork::TargetIoInvariant {
                flow_id: failed,
                operation: TargetIoOperation::Read,
            },
        cause: TargetFailure::TimedOut,
        feedback: TargetFailureFeedback::InTransit(local),
    }) = &owner.pending_completion
    else {
        panic!("real read failure and source identity must remain")
    };
    assert_eq!(*failed, flow_id);
    assert_eq!(*local, fixture.local);
    let Some(PendingTargetSink::Write(pending)) = &owner.pending_sink else {
        panic!("original Write capability must stay retained");
    };
    assert_eq!(pending.offer, offer);
    assert_eq!(pending.segments[0].payload().as_ptr(), pointer);
    assert_eq!(fixture.executor.snapshot().target_read_owned_bytes, 0);
    assert_eq!(fixture.executor.snapshot().target_read_owned_segments, 0);
    assert!(fixture.executor.retry_target_write(flow_id).is_err());
    assert!(fixture.executor.read_ready_target(flow_id).is_err());
    assert_eq!(fixture.executor.target().read_calls, 1);
    assert_eq!(fixture.executor.target().write_calls, 0);
    let outputs = peer_frame(
        &mut fixture.executor,
        fixture.leg,
        Record::Reset {
            flow_id,
            reason: ResetReason::TargetFailure,
        },
    )
    .expect(
        "actual same-Flow peer terminal must not be excluded by Failed or its independent sink",
    );
    let owner = &fixture.executor.flows[&flow_id];
    let Some(PendingTargetSink::Write(pending)) = &owner.pending_sink else {
        panic!("PeerReset must retain the original Write until actual FlowFinished");
    };
    assert_eq!(pending.offer, offer);
    assert_eq!(pending.segments[0].payload().as_ptr(), pointer);
    assert!(owner.target_present && owner.terminal.is_none());
    assert!(matches!(
        owner.pending_completion,
        Some(PendingTargetCompletion::Failed { .. })
    ));
    let resume = outputs
        .into_iter()
        .find_map(|output| match output {
            OwnerTargetOutput::NeedsResume { resume } => Some(resume),
            _ => None,
        })
        .expect("one-step budget leaves the actual FlowFinished in its original continuation");
    let outputs = fixture.executor.resume_pending_effects(resume).unwrap();
    assert_eq!(outputs.iter().filter(|output| matches!(output,
        OwnerTargetOutput::TerminalGraceStarted { terminal } if terminal.local_flow() == fixture.local)).count(), 1);
    let owner = &fixture.executor.flows[&flow_id];
    assert!(matches!(
        owner.pending_completion,
        Some(PendingTargetCompletion::TerminalSettling(_))
    ));
    assert!(owner.pending_sink.is_none() && owner.source.is_some() && !owner.target_present);
    let (visit, reset) = fixture.send_control_to_inbox();
    assert_eq!(fixture.transit(), 1);
    let (outputs, observed) = fixture
        .executor
        .apply_ready_receipt_observed(reset)
        .unwrap()
        .into_progress_parts();
    assert!(outputs.is_empty());
    assert_eq!(
        observed.fact_count(),
        0,
        "late RESET disposition cannot generate another terminal or ACK"
    );
    assert_eq!(fixture.transit(), 0);
    assert_eq!(fixture.executor.target().read_calls, 1);
    assert_eq!(fixture.executor.target().write_calls, 0);
    assert!(turn.finish(&visit).is_ok());
    fixture
        .ready
        .finish(&claim, visit.runnable, visit.blocked)
        .unwrap();
}

#[test]
fn read_failure_preserves_independent_sink_until_earlier_peer_terminal() {
    let mut fixture = fixture_with_steps(1);
    let flow_id = fixture.local.flow_id();
    fixture.executor.target_mut().write = None;
    fixture.executor.target_mut().read = Some(TargetFailure::TimedOut);
    fixture
        .executor
        .target_mut()
        .inner
        .push_write_directive(flow_id, MemoryWriteDirective::WouldBlock)
        .unwrap();
    let claim = fixture.ready.claim(0).unwrap();
    let turn = fixture.executor.begin_ready_publication().unwrap();
    assert!(
        peer_frame(
            &mut fixture.executor,
            fixture.leg,
            Record::Data {
                flow_id,
                direction: Direction::ClientToTarget,
                offset: ByteOffset::new(0),
                payload: Bytes::from_static(b"sink"),
            }
        )
        .unwrap()
        .is_empty()
    );
    let Some(PendingTargetSink::Write(pending)) = &fixture.executor.flows[&flow_id].pending_sink
    else {
        panic!("actual incoming Write capability must stay in the original sink slot");
    };
    let (offer, pointer) = (pending.offer, pending.segments[0].payload().as_ptr());
    assert!(matches!(
        fixture.executor.read_ready_target(flow_id).unwrap(),
        crate::owned_upstream::owner_target::read_source::BoundTargetRead::FailureStaged
    ));
    let owner = &fixture.executor.flows[&flow_id];
    let Some(PendingTargetCompletion::Failed {
        work:
            OwnerTargetWork::TargetIoInvariant {
                flow_id: failed,
                operation: TargetIoOperation::Read,
            },
        cause: TargetFailure::TimedOut,
        feedback: TargetFailureFeedback::InTransit(local),
    }) = &owner.pending_completion
    else {
        panic!("real read failure and source identity must remain")
    };
    assert_eq!(*failed, flow_id);
    assert_eq!(*local, fixture.local);
    let Some(PendingTargetSink::Write(pending)) = &owner.pending_sink else {
        panic!("original Write capability must stay retained");
    };
    assert_eq!(pending.offer, offer);
    assert_eq!(pending.segments[0].payload().as_ptr(), pointer);
    assert_eq!(fixture.executor.snapshot().target_read_owned_bytes, 0);
    assert_eq!(fixture.executor.snapshot().target_read_owned_segments, 0);
    assert!(fixture.executor.retry_target_write(flow_id).is_err());
    assert!(fixture.executor.read_ready_target(flow_id).is_err());
    assert_eq!(fixture.executor.target().read_calls, 1);
    assert_eq!(fixture.executor.target().write_calls, 1);
    let outputs = peer_frame(
        &mut fixture.executor,
        fixture.leg,
        Record::Reset {
            flow_id,
            reason: ResetReason::TargetFailure,
        },
    )
    .expect(
        "actual same-Flow peer terminal must not be excluded by Failed or its independent sink",
    );
    let owner = &fixture.executor.flows[&flow_id];
    let Some(PendingTargetSink::Write(pending)) = &owner.pending_sink else {
        panic!("PeerReset must retain the original Write until actual FlowFinished");
    };
    assert_eq!(pending.offer, offer);
    assert_eq!(pending.segments[0].payload().as_ptr(), pointer);
    assert!(owner.target_present && owner.terminal.is_none());
    assert!(matches!(
        owner.pending_completion,
        Some(PendingTargetCompletion::Failed { .. })
    ));
    let resume = outputs
        .into_iter()
        .find_map(|output| match output {
            OwnerTargetOutput::NeedsResume { resume } => Some(resume),
            _ => None,
        })
        .expect("one-step budget leaves the actual FlowFinished in its original continuation");
    let outputs = fixture.executor.resume_pending_effects(resume).unwrap();
    assert_eq!(outputs.iter().filter(|output| matches!(output,
        OwnerTargetOutput::TerminalGraceStarted { terminal } if terminal.local_flow() == fixture.local)).count(), 1);
    let owner = &fixture.executor.flows[&flow_id];
    assert!(matches!(
        owner.pending_completion,
        Some(PendingTargetCompletion::TerminalSettling(_))
    ));
    assert!(owner.pending_sink.is_none() && owner.source.is_some() && !owner.target_present);
    let (visit, reset) = fixture.send_control_to_inbox();
    assert_eq!(fixture.transit(), 1);
    let (outputs, observed) = fixture
        .executor
        .apply_ready_receipt_observed(reset)
        .unwrap()
        .into_progress_parts();
    assert!(outputs.is_empty());
    assert_eq!(
        observed.fact_count(),
        0,
        "late RESET disposition cannot generate another terminal or ACK"
    );
    assert_eq!(fixture.transit(), 0);
    assert_eq!(fixture.executor.target().read_calls, 1);
    assert_eq!(fixture.executor.target().write_calls, 1);
    assert!(turn.finish(&visit).is_ok());
    fixture
        .ready
        .finish(&claim, visit.runnable, visit.blocked)
        .unwrap();
}
