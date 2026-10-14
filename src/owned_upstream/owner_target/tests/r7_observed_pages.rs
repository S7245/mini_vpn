use super::*;

#[test]
fn no_pending_resume_rejection_returns_the_exact_capability() {
    let (_, mut executor) = executor();
    let expected_id = 17;
    let resume = OwnerTargetResume {
        owner: Arc::downgrade(&executor.identity),
        id: expected_id,
    };

    let error = executor.resume_pending_effects(resume).unwrap_err();
    let OwnerTargetError::RejectedEffectResume { block, resume } = error else {
        panic!("no-pending rejection must retain the exact resume capability");
    };

    assert_eq!(block, OwnerTargetInputBlock::NoPendingEffectTurn);
    assert_eq!(resume.id, expected_id);
    assert!(
        resume
            .owner
            .upgrade()
            .is_some_and(|identity| Arc::ptr_eq(&identity, &executor.identity))
    );
}

#[test]
fn wrong_id_resume_rejection_returns_the_exact_capability_without_consuming_the_turn() {
    let (leg, mut executor) = executor();
    executor.config.max_effect_steps = 1;
    let flow_id = SessionFlowId::new(1).unwrap();
    let mut outputs = peer_frame(
        &mut executor,
        leg,
        Record::Open {
            flow_id,
            target: target(443),
        },
    )
    .unwrap();
    let OwnerTargetOutput::NeedsResume { resume: expected } = outputs.pop().unwrap() else {
        panic!("one-step OPEN must return its exact resume capability");
    };
    let expected_id = expected.id;
    let wrong_id = expected_id.checked_add(1).unwrap();
    let wrong = OwnerTargetResume {
        owner: expected.owner.clone(),
        id: wrong_id,
    };
    let before = executor.snapshot();

    let error = executor.resume_pending_effects(wrong).unwrap_err();
    let OwnerTargetError::RejectedEffectResume { block, resume } = error else {
        panic!("wrong-ID rejection must retain the exact resume capability");
    };

    assert_eq!(
        block,
        OwnerTargetInputBlock::InvalidEffectResume {
            expected: expected_id,
            actual: wrong_id,
        }
    );
    assert_eq!(resume.id, wrong_id);
    assert_eq!(executor.snapshot(), before);
    let resumed = executor.resume_pending_effects(expected).unwrap();
    assert!(transmitted(&resumed).iter().any(|record| matches!(
        record,
        Record::OpenResult {
            flow_id: observed,
            result: OpenResultCode::Opened,
        } if *observed == flow_id
    )));
}

fn pending_half_close_resume() -> (
    OwnerTargetExecutor<MemoryTarget>,
    SessionFlowId,
    OwnerTargetResume,
) {
    let (leg, mut executor) = executor();
    executor.config.max_effect_steps = 1;
    let flow_id = SessionFlowId::new(1).unwrap();

    let mut open = open_flow(&mut executor, leg, flow_id, target(443));
    let OwnerTargetOutput::NeedsResume {
        resume: open_resume,
    } = open.pop().unwrap()
    else {
        panic!("one-step OPEN must return a continuation");
    };
    executor
        .resume_pending_effects_observed(open_resume)
        .unwrap();

    executor
        .target_mut()
        .feed_read(flow_id, Bytes::from_static(b"r"))
        .unwrap();
    executor.try_read_target(flow_id).unwrap();
    executor.target_mut().finish_read(flow_id).unwrap();
    executor.try_read_target(flow_id).unwrap();
    peer_frame(
        &mut executor,
        leg,
        Record::Ack {
            flow_id,
            direction: Direction::TargetToClient,
            next_accepted: ByteOffset::new(1),
            final_accepted: true,
        },
    )
    .unwrap();

    executor
        .target_mut()
        .push_write_directive(flow_id, MemoryWriteDirective::WouldBlock)
        .unwrap();
    peer_frame(
        &mut executor,
        leg,
        Record::Data {
            flow_id,
            direction: Direction::ClientToTarget,
            offset: ByteOffset::new(0),
            payload: Bytes::from_static(b"x"),
        },
    )
    .unwrap();
    peer_frame(
        &mut executor,
        leg,
        Record::Close {
            flow_id,
            direction: Direction::ClientToTarget,
            final_offset: ByteOffset::new(1),
        },
    )
    .unwrap();

    let (mut accepted, accepted_observations) = executor
        .retry_target_write_observed(flow_id)
        .unwrap()
        .into_progress_parts();
    assert!(accepted_observations.flow_terminal().is_none());
    let resume = accepted
        .iter()
        .position(|output| matches!(output, OwnerTargetOutput::NeedsResume { .. }))
        .map(|index| accepted.remove(index))
        .and_then(|output| match output {
            OwnerTargetOutput::NeedsResume { resume } => Some(resume),
            _ => None,
        })
        .expect("write completion must retain its following half-close effect");
    (executor, flow_id, resume)
}

#[test]
fn observed_resume_returns_the_nested_terminal_observation_from_its_exact_page() {
    let (mut executor, flow_id, resume) = pending_half_close_resume();

    let (_, observations) = executor
        .resume_pending_effects_observed(resume)
        .unwrap()
        .into_progress_parts();
    let terminal = observations
        .flow_terminal()
        .expect("nested half-close commit must remain on its exact resume page");
    assert_eq!(terminal.flow_id(), flow_id);
    assert_eq!(terminal.direction(), Direction::TargetToClient);
}

#[test]
fn fatal_after_page_progress_retains_prefix_outputs_and_observations() {
    let (mut executor, flow_id, resume) = pending_half_close_resume();
    executor
        .pending_effects
        .push_back(OwnerTargetWork::Effect(SessionEffect::LegActivated {
            generation: executor.supervisor.snapshot().session.generation(),
        }));
    executor
        .pending_effects
        .push_back(OwnerTargetWork::TargetIoInvariant {
            flow_id,
            operation: TargetIoOperation::WriteCompletion,
        });
    executor.config.max_effect_steps = 128;

    let error = match executor.resume_pending_effects_observed(resume) {
        Ok(_) => panic!("injected post-commit invariant must fail closed"),
        Err(error) => error,
    };

    assert!(matches!(error, OwnerTargetError::AbortedEffectInvariant));
    let snapshot = executor.snapshot();
    assert_eq!(snapshot.aborted_outputs, 1);
    assert_eq!(snapshot.aborted_observations, 1);
    let retained = executor
        .take_retained_observations_after_failure()
        .expect("fatal page must retain its nested reducer observation");
    let terminal = retained
        .flow_terminal()
        .expect("retained page must keep the exact terminal fact");
    assert_eq!(terminal.flow_id(), flow_id);
    assert_eq!(terminal.direction(), Direction::TargetToClient);
}

#[test]
fn real_target_reset_wake_unwind_retains_current_suffix_and_original_fact() {
    use crate::owned_upstream::target::TargetReadyInterest;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll, Wake, Waker};

    struct PanicResetWake(AtomicUsize);
    impl Wake for PanicResetWake {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
                panic!("single-use actual Target RESET readiness unwind");
            }
        }
    }

    let (leg, mut executor) = executor();
    let flow_id = SessionFlowId::new(1).unwrap();
    let outputs = open_flow(&mut executor, leg, flow_id, target(443));
    let _outputs = drain_resumes(&mut executor, outputs);
    let before = executor.snapshot();
    assert_eq!(before.target_flows, 1);
    assert_eq!(before.aborted_effect_guards, 0);
    assert_eq!(executor.target().snapshot().reset_flows, 0);
    let wake = Arc::new(PanicResetWake(AtomicUsize::new(0)));
    let waker = Waker::from(Arc::clone(&wake));
    let mut cx = Context::from_waker(&waker);
    assert_eq!(
        executor
            .target_mut()
            .poll_readiness(flow_id, TargetReadyInterest::Read, &mut cx),
        Poll::Pending
    );
    assert_eq!(wake.0.load(Ordering::SeqCst), 0);
    let reset = || SessionEvent::PeerFrame {
        leg,
        frame: Frame::try_new(
            leg.generation(),
            Record::Reset {
                flow_id,
                reason: ResetReason::TargetFailure,
            },
        )
        .unwrap(),
    };

    let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        executor.apply_event_observed(reset())
    }));
    let panic = caught.err().expect("actual Target readiness must unwind");
    assert_eq!(
        panic.downcast_ref::<&'static str>(),
        Some(&"single-use actual Target RESET readiness unwind")
    );
    assert_eq!(wake.0.load(Ordering::SeqCst), 1);
    let target_after = executor.target().snapshot();
    assert_eq!(target_after.reset_flows, 1);
    assert_eq!(
        executor.target().readiness(flow_id).unwrap().terminal,
        Some(TargetTerminalKind::Reset)
    );
    let held = executor.snapshot();
    assert_eq!(held.session.session.flow_count(), 0);
    assert_eq!(held.session.session.terminal_tombstones(), 1);
    assert_eq!(held.session.retained_receipt_effects, 0);
    assert!(
        !held.session.poisoned,
        "Supervisor completed its owning handoff"
    );
    assert_eq!(
        held.aborted_effect_guards, 1,
        "the original Executor must retain its entered page before Target wake"
    );
    assert_eq!(
        held.aborted_effects, 2,
        "current RESET and FlowFinished suffix"
    );
    assert_eq!(held.aborted_ordinary_observations, 1);
    assert_eq!(held.target_flows, 1);
    assert_eq!(held.aborted_outputs, 0);
    let journal = executor.aborted_effects.as_ref().unwrap();
    let fact = journal
        .observations
        .as_ref()
        .unwrap()
        .flow_terminal()
        .unwrap();
    assert_eq!(fact.flow_id(), flow_id);
    assert!(matches!(journal.current.as_ref(),
        Some(OwnerTargetWork::Effect(SessionEffect::PeerReset { flow_id: id, .. }))
            if *id == flow_id));
    assert!(matches!(journal.effects.front(),
        Some(OwnerTargetWork::Effect(SessionEffect::FlowFinished { terminal, .. }))
            if terminal == fact.terminal()));
    let error = match executor.apply_event_observed(reset()) {
        Err(error) => error,
        Ok(_) => panic!("unknown Target disposition cannot admit a retry"),
    };
    assert!(matches!(
        error,
        OwnerTargetError::RejectedEvent {
            block: OwnerTargetInputBlock::AbortedEffectInvariant,
            ..
        }
    ));
    assert_eq!(executor.snapshot(), held);
    assert_eq!(executor.target().snapshot(), target_after);
    assert_eq!(wake.0.load(Ordering::SeqCst), 1);
}

#[test]
fn rejected_open_source_drop_keeps_actual_outcome_and_unknown_release() {
    use std::future::Future;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Wake, Waker};

    struct PanicSourceDrop(AtomicUsize);
    impl Wake for PanicSourceDrop {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
                panic!("actual rejected Source drop wake");
            }
        }
    }
    let (leg, mut executor) = executor();
    executor.target =
        MemoryTarget::new(MemoryTargetConfig::new(1, 256, 512, 128, 128, 16).unwrap());
    let live = SessionFlowId::new(1).unwrap();
    let outputs = open_flow(&mut executor, leg, live, target(443));
    let _outputs = drain_resumes(&mut executor, outputs);
    let rejected = SessionFlowId::new(2).unwrap();
    let factory = executor.source_factory.clone();
    let cursor = factory.progress_cursor();
    let mut wait = Box::pin(factory.wait_for_progress(&cursor));
    let wake = Arc::new(PanicSourceDrop(AtomicUsize::new(0)));
    let waker = Waker::from(Arc::clone(&wake));
    assert!(
        wait.as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    let open = || SessionEvent::PeerFrame {
        leg,
        frame: Frame::try_new(
            leg.generation(),
            Record::Open {
                flow_id: rejected,
                target: target(8443),
            },
        )
        .unwrap(),
    };
    let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        executor.apply_event_observed(open())
    }));
    let panic = caught
        .err()
        .expect("real factory progress must interrupt Source destruction");
    assert_eq!(
        panic.downcast_ref::<&str>(),
        Some(&"actual rejected Source drop wake")
    );
    assert!(wake.0.load(Ordering::SeqCst) >= 1);
    drop(wait);
    let held = executor.snapshot();
    let target_after = executor.target().snapshot();
    assert_eq!(held.target_flows, 1);
    assert_eq!(held.aborted_effect_guards, 1);
    assert_eq!(held.aborted_effects, 1);
    assert_eq!(target_after.live_flows, 1);
    assert_eq!(target_after.highest_flow_id, Some(rejected));
    let page = executor.aborted_effects.as_ref().unwrap();
    assert!(page.source_releasing, "unknown destruction is not Released");
    assert!(
        page.source.is_none(),
        "do not claim the Source survives its own Drop"
    );
    assert_eq!(
        page.open_outcome,
        Some(TargetOpenCompletion::Failed(
            TargetOpenFailure::ResourceExhausted
        ))
    );
    assert!(matches!(page.current.as_ref(),
        Some(OwnerTargetWork::Effect(SessionEffect::PeerOpenRequested { request, flow, .. }))
            if request.flow_id() == rejected && flow.flow_id() == rejected));
    assert!(matches!(
        executor.apply_event_observed(open()),
        Err(OwnerTargetError::RejectedEvent {
            block: OwnerTargetInputBlock::AbortedEffectInvariant,
            ..
        })
    ));
    assert_eq!(executor.snapshot(), held);
    assert_eq!(executor.target().snapshot(), target_after);
}

#[test]
fn retry_write_wake_retains_original_inline_offer_without_work_backing() {
    use crate::owned_upstream::target::{TargetReadyInterest, TargetReadyOutcome};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll, Wake, Waker};

    struct PanicRetryWake(AtomicUsize);
    impl Wake for PanicRetryWake {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
                panic!("actual retry Target write wake");
            }
        }
    }
    let (leg, mut executor) = executor();
    let flow_id = SessionFlowId::new(1).unwrap();
    let outputs = open_flow(&mut executor, leg, flow_id, target(443));
    let _outputs = drain_resumes(&mut executor, outputs);
    executor
        .target_mut()
        .push_write_directive(flow_id, MemoryWriteDirective::WouldBlock)
        .unwrap();
    let outputs = peer_frame(
        &mut executor,
        leg,
        Record::Data {
            flow_id,
            direction: Direction::ClientToTarget,
            offset: ByteOffset::new(0),
            payload: Bytes::from_static(b"held"),
        },
    )
    .unwrap();
    let _outputs = drain_resumes(&mut executor, outputs);
    let Some(PendingTargetSink::Write(pending)) = executor.flows[&flow_id].pending_sink.as_ref()
    else {
        panic!("WouldBlock must preserve the real pending write");
    };
    let (offer, pointer) = (pending.offer, pending.segments[0].payload().as_ptr());
    executor
        .target_mut()
        .push_write_directive(flow_id, MemoryWriteDirective::accept_at_most(4).unwrap())
        .unwrap();
    let wake = Arc::new(PanicRetryWake(AtomicUsize::new(0)));
    let waker = Waker::from(Arc::clone(&wake));
    assert_eq!(
        executor.target_mut().poll_readiness(
            flow_id,
            TargetReadyInterest::Write,
            &mut Context::from_waker(&waker),
        ),
        Poll::Ready(Ok(TargetReadyOutcome::Attempt))
    );
    assert!(executor.pending_effects.is_empty());
    let original_work = (
        executor.pending_effects.as_slices().0.as_ptr(),
        executor.pending_effects.capacity(),
    );
    assert!(original_work.1 > 0);
    let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        executor.retry_target_write_observed(flow_id)
    }));
    let panic = caught
        .err()
        .expect("real Target write must reach the registered wake");
    assert_eq!(
        panic.downcast_ref::<&str>(),
        Some(&"actual retry Target write wake")
    );
    assert_eq!(wake.0.load(Ordering::SeqCst), 1);
    let held = executor.snapshot();
    let target_after = executor.target().snapshot();
    assert_eq!(target_after.write_calls, 2);
    assert_eq!(target_after.write_accepted_bytes, 4);
    assert_eq!(held.aborted_effects, 1);
    let page = executor.aborted_effects.as_ref().unwrap();
    assert_eq!(
        (page.effects.as_slices().0.as_ptr(), page.effects.capacity()),
        original_work,
        "retry moves the original nonempty-capacity backing, without a new work allocation"
    );
    assert!(executor.pending_effects.is_empty());
    assert_eq!(executor.pending_effects.capacity(), 0);
    assert!(page.effects.is_empty() && page.outputs.is_empty());
    let Some(OwnerTargetWork::Effect(SessionEffect::OfferToSink {
        offer: retained,
        segments,
    })) = page.current.as_ref()
    else {
        panic!("the original inline offer must remain held");
    };
    assert_eq!(*retained, offer);
    assert_eq!(segments[0].payload().as_ptr(), pointer);
    assert_eq!(segments[0].payload().as_ref(), b"held");
    assert!(matches!(
        executor.retry_target_write_observed(flow_id),
        Err(OwnerTargetError::AbortedEffectInvariant)
    ));
    assert_eq!(executor.snapshot(), held);
    assert_eq!(executor.target().snapshot(), target_after);
}
