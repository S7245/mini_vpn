//! Cold and source-less Open completion characterization.
//!
//! Owns the original Open/Z retry and resume tests plus their private helper.
//! Uses the parent real Executor fixture; does not own source retirement,
//! physical activation or whole-root cleanup. Test entries live in this leaf.

use super::*;

#[test]
fn cold_source_less_open_keeps_its_disposition_across_real_z_retry() {
    use crate::owned_upstream::owner_ready::OwnerReadySource;
    let FlowLifecycleFixture {
        mut ready,
        mut executor,
        inbox: _inbox,
        local,
        terminal,
    } = lifecycle_fixture_with_cold_open(true);
    let failed = SessionFlowId::new(3).unwrap();
    let opens_before = executor.target().snapshot().open_attempts;
    let claim = ready.claim(0).unwrap();
    let turn = executor.begin_ready_publication().unwrap();
    let control = executor.visit_ready_send(SendLane::Control).unwrap();
    assert!(turn.finish(&control).is_ok());
    ready
        .finish(&claim, control.runnable, control.blocked)
        .unwrap();
    let claim = ready.claim(0).unwrap();
    let turn = executor.begin_ready_publication().unwrap();
    executor
        .target_mut()
        .feed_read(local.flow_id(), Bytes::from_static(b"held"))
        .unwrap();
    assert!(matches!(
        executor.read_ready_target(local.flow_id()).unwrap(),
        crate::owned_upstream::owner_target::read_source::BoundTargetRead::Data
    ));
    let completed = send_source_to_mailbox(&mut executor, SendLane::OrderedSource);
    assert_eq!(live_record(&executor, local.flow_id()).transit, 1);
    assert_eq!(live_record(&executor, failed).transit, 0);
    assert_eq!(executor.snapshot().target_read_owned_bytes, 4);
    assert_eq!(executor.snapshot().target_read_owned_segments, 1);
    let initial_outputs = executor.apply_event(SessionEvent::TerminalGraceExpired { terminal })
        .expect("the actual cold Open completion must retain its no-source disposition through reduction");
    assert!(
        initial_outputs.is_empty(),
        "capacity release cannot perform an unselected retry"
    );
    assert!(turn.finish(&completed).is_ok());
    ready
        .finish(&claim, completed.runnable, completed.blocked)
        .unwrap();
    // The original DATA remains mailbox-owned. Do not receive or reduce it
    // merely to exercise this unrelated selected Completion.
    let claim = ready.claim(0).unwrap();
    assert_eq!(claim.source, OwnerReadySource::OwnerContinuation);
    let turn = executor.begin_ready_publication().unwrap();
    let mask = crate::owned_upstream::owner_ready::OwnerReadyCategory::Completion.mask();
    let selected = executor
        .select_ready_continuation(mask, 0)
        .unwrap()
        .expect("original cold completion is due");
    let (observed, result) = executor.apply_ready_completion_observed(&selected).unwrap();
    let outputs = observed.into_progress_parts().0;
    let runnable = executor
        .finish_ready_continuation(&selected, result, mask)
        .unwrap();
    let completed = SessionCommandVisit {
        work: OwnerReadyUnit::SourceRead,
        runnable,
        blocked: None,
    };
    assert_eq!(executor.target().snapshot().open_attempts, opens_before);
    assert_eq!(
        outputs
            .iter()
            .filter(|output| matches!(output,
        OwnerTargetOutput::Transmit(frame) if matches!(frame.record(), Record::OpenResult {
            flow_id, result: OpenResultCode::ResourceExhausted } if *flow_id == failed)))
            .count(),
        1
    );
    assert_eq!(
        outputs
            .iter()
            .filter(|output| matches!(output,
        OwnerTargetOutput::TerminalGraceStarted { terminal } if terminal.flow_id() == failed))
            .count(),
        1
    );
    let retained = &executor.flows[&failed];
    assert!(retained.source.is_none() && !retained.target_present);
    assert_eq!(executor.snapshot().pending_target_completions, 0);
    let live_source = executor.flows[&local.flow_id()].source.as_ref().unwrap();
    assert_eq!(live_source.flow.local_flow(), local);
    assert_eq!(live_record(&executor, local.flow_id()).transit, 1);
    assert_eq!(live_record(&executor, failed).transit, 0);
    assert_eq!(executor.snapshot().target_read_owned_bytes, 4);
    assert_eq!(executor.snapshot().target_read_owned_segments, 1);
    assert!(turn.finish(&completed).is_ok());
    ready
        .finish(&claim, completed.runnable, completed.blocked)
        .unwrap();
}

fn consume_original_resumes(
    executor: &mut OwnerTargetExecutor<MemoryTarget>,
    mut outputs: Vec<OwnerTargetOutput>,
) -> Vec<OwnerTargetOutput> {
    for _ in 0..8 {
        let Some(index) = outputs
            .iter()
            .position(|output| matches!(output, OwnerTargetOutput::NeedsResume { .. }))
        else {
            return outputs;
        };
        let OwnerTargetOutput::NeedsResume { resume } = outputs.remove(index) else {
            unreachable!()
        };
        outputs.extend(executor.resume_pending_effects(resume).unwrap());
    }
    panic!("the finite OPEN effect sequence must finish within eight original resume tokens");
}

#[test]
fn cold_pending_finish_effect_consumes_its_original_resume_after_binding() {
    use crate::owned_upstream::owner_ready::{OwnerReadyFlowAssociation, OwnerReadyNoSourcePhase};
    let leg = source_leg();
    let (plan, mut executor) = lifecycle_executor(leg, 1, 1);
    let live = SessionFlowId::new(1).unwrap();
    let failed = SessionFlowId::new(2).unwrap();
    let open = |flow_id| SessionEvent::PeerFrame {
        leg,
        frame: Frame::try_new(
            leg.generation(),
            Record::Open {
                flow_id,
                target: TargetAddr::IpPort(([127, 0, 0, 1], 443).into()),
            },
        )
        .unwrap(),
    };
    let initial = executor.apply_event(open(live)).unwrap();
    let _initial_outputs = consume_original_resumes(&mut executor, initial);
    let pending = executor.apply_event(open(failed)).unwrap();
    assert_eq!(
        pending
            .iter()
            .filter(|output| matches!(output, OwnerTargetOutput::NeedsResume { .. }))
            .count(),
        1
    );
    assert!(executor.pending_effects.iter().any(|work| matches!(work,
        crate::owned_upstream::owner_target::OwnerTargetWork::Effect(
            crate::resumable::SessionEffect::FlowFinished { flow_id, .. }) if *flow_id == failed)));
    assert!(executor.flows[&failed].source.is_none());
    assert!(!executor.flows[&failed].target_present);
    let opens_before = executor.target().snapshot().open_attempts;
    let geometry = ReadyGeometry::checked(NonZeroUsize::new(1).unwrap()).unwrap();
    let mut ready = ReadyIndex::cold(&geometry).unwrap();
    let limits =
        OwnerReadySessionLimits::checked(plan.session_config().max_flows(), 1, 2, 2).unwrap();
    let reservation = ready.reserve_session(leg.session_id(), limits).unwrap();
    let (mut executor, _inbox) = match executor.install_owner_ready(reservation, plan) {
        Ok(installed) => installed,
        Err(_) => panic!("real PendingFinishEffect must be a valid cold population"),
    };
    let association = live_record(&executor, failed).association;
    assert_eq!(
        association,
        OwnerReadyFlowAssociation::NoSource {
            flow_id: failed,
            phase: OwnerReadyNoSourcePhase::PendingFinishEffect,
        }
    );
    let outputs = consume_original_resumes(&mut executor, pending);
    assert_eq!(executor.target().snapshot().open_attempts, opens_before);
    assert_eq!(
        outputs
            .iter()
            .filter(|output| matches!(output,
        OwnerTargetOutput::Transmit(frame) if matches!(frame.record(), Record::OpenResult {
            flow_id, result: OpenResultCode::ResourceExhausted } if *flow_id == failed)))
            .count(),
        1
    );
    assert_eq!(
        outputs
            .iter()
            .filter(|output| matches!(output,
        OwnerTargetOutput::TerminalGraceStarted { terminal } if terminal.flow_id() == failed))
            .count(),
        1
    );
    let retained = &executor.flows[&failed];
    assert!(retained.source.is_none() && !retained.target_present);
    assert!(
        retained.terminal_open_preserves_no_source(
            failed,
            OwnerReadyNoSourcePhase::PendingFinishEffect
        )
    );
    assert_eq!(executor.snapshot().pending_target_completions, 0);
    assert!(executor.flows[&live].source.is_some());
    let record = live_record(&executor, failed);
    assert_eq!(
        record.association, association,
        "installation lineage must not be rewritten"
    );
    assert_eq!(record.transit, 0, "effect resume is not a Driver receipt");
}

#[test]
fn unbound_already_terminal_open_retries_the_original_capacity_blocked_join() {
    use crate::owned_upstream::target::{
        TargetIo, TargetJoinCompletion, TargetOpenCompletion, TargetTombstoneRetireCompletion,
    };
    let leg = source_leg();
    let (_, mut executor) = lifecycle_executor(leg, 2, 128);
    let occupied = SessionFlowId::new(1).unwrap();
    let original = SessionFlowId::new(2).unwrap();
    let address = TargetAddr::IpPort(([127, 0, 0, 1], 443).into());
    for flow in [occupied, original] {
        assert_eq!(
            executor.target_mut().open(flow, &address).unwrap(),
            TargetOpenCompletion::Opened
        );
        executor.target_mut().cancel(flow).unwrap();
        if flow == occupied {
            assert!(matches!(
                executor.target_mut().join(flow).unwrap(),
                TargetJoinCompletion::Joined(_)
            ));
        }
    }
    let outputs = peer_record(
        &mut executor,
        leg,
        Record::Open {
            flow_id: original,
            target: address,
        },
    );
    assert_eq!(
        outputs
            .iter()
            .filter(|output| matches!(output,
        OwnerTargetOutput::TerminalGraceStarted { terminal } if terminal.flow_id() == original))
            .count(),
        1
    );
    let retained = &executor.flows[&original];
    assert!(retained.source.is_none() && retained.target_present && retained.join_pending);
    assert!(matches!(
        executor
            .target_mut()
            .retire_terminal_tombstone(occupied)
            .unwrap(),
        TargetTombstoneRetireCompletion::Retired(_)
    ));
    let opens_before = executor.target().snapshot().open_attempts;
    executor
        .retry_target_join(original)
        .expect("completed Open proof must not exclude the original Unbound join retry");
    assert_eq!(executor.target().snapshot().open_attempts, opens_before);
    assert!(!executor.flows.contains_key(&original));
}
