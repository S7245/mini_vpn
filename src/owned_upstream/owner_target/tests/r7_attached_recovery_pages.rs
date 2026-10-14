use super::*;

fn enqueued_replacement_recovery(
    open_flows: usize,
    max_effect_steps: usize,
    queue_frames: usize,
) -> (
    OwnerTargetExecutor<MemoryTarget>,
    LegIo,
    LegOutboundQueue,
    EnqueuedOwnerTargetAttach,
) {
    let initial = committed_leg(2, 0x22, 0x42);
    let model = SessionModel::new(SessionRole::Owner, session_config(), initial);
    let authority = AttachAuthority::new(
        SessionId::new([0x11; 16]).unwrap(),
        LegGeneration::new(2).unwrap(),
        credentials(),
        AttachPolicy::new(
            OwnerIdentity::new([0x31; 32]).unwrap(),
            AttachAlpn::new(b"mini-vpn-owned/1").unwrap(),
            DevicePrincipal::new([0x53; 16]).unwrap(),
            SESSION_PROTOCOL_VERSION,
            0b1111,
        )
        .unwrap(),
    );
    let supervisor = SessionSupervisor::new_owner(model, authority).unwrap();
    let mut executor = OwnerTargetExecutor::new(
        OwnerTargetConfig::new(128, 1_024, 64).unwrap(),
        supervisor,
        MemoryTarget::new(MemoryTargetConfig::new(4, 256, 512, 128, 128, 16).unwrap()),
        source_config(),
    )
    .unwrap();
    for flow in 1..=open_flows {
        let flow_id = SessionFlowId::new(flow as u64).unwrap();
        let outputs = open_flow(&mut executor, initial, flow_id, target(440 + flow as u16));
        assert!(transmitted(&outputs).iter().any(|record| matches!(
            record,
            Record::OpenResult {
                flow_id: observed,
                result: OpenResultCode::Opened,
            } if *observed == flow_id
        )));
    }
    executor
        .apply_event(SessionEvent::LegLost { leg: initial })
        .unwrap();

    let transport_binding = binding(0x43);
    let endpoint = LegIo::for_authenticated_transport(
        LegId::A,
        LegEndpointRole::Owner,
        transport_binding,
        LegIoLimits::new(1_024, 8, 8_192, 8, 8_192).unwrap(),
    );
    let established = endpoint.established_leg();
    let request = AttachRequest::new(
        SessionId::new([0x11; 16]).unwrap(),
        LegGeneration::new(3).unwrap(),
        AttachNonce::new([0x33; 16]).unwrap(),
        VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION).unwrap(),
        FeatureOffer::new(0b111, 0b001).unwrap(),
    );
    let proof = credentials().prove(&request, &transport_binding).unwrap();
    let OwnerTargetAttachPublication::Installed { pending } = executor
        .accept_owner_attach_for_test(
            established,
            established.bind_received_frame(request.to_attach_frame(proof)),
        )
        .unwrap()
    else {
        panic!("exact-next replacement must install");
    };
    let mut queue = LegOutboundQueue::for_leg(established, queue_frames, 8_192).unwrap();
    let enqueued = pending.enqueue_acceptance(&mut queue).unwrap();
    executor.config.max_effect_steps = max_effect_steps;
    (executor, endpoint, queue, enqueued)
}

#[test]
fn observed_second_page_is_executed_once_and_queue_pressure_retries_only_publish() {
    let (mut executor, mut endpoint, mut queue, recovery) = enqueued_replacement_recovery(1, 1, 1);
    let open_attempts = executor.target().snapshot().open_attempts;

    let first = executor
        .execute_attached_recovery_observed(recovery, &queue)
        .unwrap();
    let (first_cursor, first_observations) = first.into_progress_parts();
    assert!(first_observations.is_empty());
    let first_outputs = executor
        .publish_attached_recovery_page(first_cursor, &mut queue)
        .unwrap();
    let resume = first_outputs
        .into_iter()
        .find_map(|output| match output {
            OwnerTargetRecoveryOutput::NeedsResume { resume } => Some(resume),
            _ => None,
        })
        .expect("one-step recovery must return the exact second-page capability");

    let second = executor
        .resume_attached_recovery_effects_observed(resume, &queue)
        .unwrap();
    let (second_cursor, second_observations) = second.into_progress_parts();
    assert!(second_observations.is_empty());
    let second_cursor = match executor
        .publish_attached_recovery_page(second_cursor, &mut queue)
        .unwrap_err()
    {
        OwnerTargetError::AttachedRecoveryQueueBlocked {
            kind: LegOutboundQueueErrorKind::CapacityExceeded { .. },
            drain,
        } => *drain,
        other => panic!("full acceptance queue must return only its publish cursor: {other:?}"),
    };
    assert_eq!(executor.target().snapshot().open_attempts, open_attempts);
    assert_eq!(executor.snapshot().aborted_recovery_guards, 0);

    let mut transport = CapturingTransport::default();
    queue
        .try_flush(&mut endpoint, &mut transport, SimTime::ZERO)
        .unwrap()
        .unwrap();
    let completed = executor
        .publish_attached_recovery_page(second_cursor, &mut queue)
        .unwrap();
    assert!(
        completed
            .iter()
            .any(|output| matches!(output, OwnerTargetRecoveryOutput::RecoveryCompleted { .. }))
    );
    assert_eq!(executor.target().snapshot().open_attempts, open_attempts);
    assert_eq!(executor.snapshot().aborted_recovery_guards, 0);

    queue
        .try_flush(&mut endpoint, &mut transport, SimTime::ZERO)
        .unwrap()
        .unwrap();
    assert!(matches!(
        Frame::decode_owned_exact(transport.frames[0].clone())
            .unwrap()
            .record(),
        Record::AttachAccepted { .. }
    ));
    assert!(matches!(
        Frame::decode_owned_exact(transport.frames[1].clone())
            .unwrap()
            .record(),
        Record::OpenResult { .. }
    ));
}

#[test]
fn legacy_adapter_never_silently_discards_a_recovery_observation() {
    let (mut executor, _endpoint, mut queue, recovery) = enqueued_replacement_recovery(1, 1, 4);
    let mut page = executor
        .execute_attached_recovery_observed(recovery, &queue)
        .unwrap();
    page.observations.flow_retired = Some(OwnerTargetFlowRetired {
        session_id: SessionId::new([0x11; 16]).unwrap(),
        flow_id: SessionFlowId::new(1).unwrap(),
        generation: LegGeneration::new(3).unwrap(),
    });

    let error = executor
        .publish_legacy_attached_recovery_page(page, &mut queue)
        .unwrap_err();

    assert!(matches!(
        error,
        OwnerTargetError::AttachedRecoveryObservationInvariant
    ));
    let snapshot = executor.snapshot();
    assert_eq!(snapshot.aborted_observations, 1);
    assert_eq!(snapshot.aborted_recovery_guards, 1);
    assert_eq!(queue.len(), 1, "the legacy adapter published no page bytes");
    assert!(executor.attach_barrier_pending);
}

#[test]
fn stale_recovery_resume_rejection_returns_the_exact_attached_capability() {
    let (mut executor, _endpoint, mut queue, recovery) = enqueued_replacement_recovery(1, 1, 4);
    let first = executor
        .execute_attached_recovery_observed(recovery, &queue)
        .unwrap();
    let (first_cursor, observations) = first.into_progress_parts();
    assert!(observations.is_empty());
    let resume = executor
        .publish_attached_recovery_page(first_cursor, &mut queue)
        .unwrap()
        .into_iter()
        .find_map(|output| match output {
            OwnerTargetRecoveryOutput::NeedsResume { resume } => Some(resume),
            _ => None,
        })
        .expect("first page must return a recovery continuation");
    let resume_id = resume.resume.id;
    let wrong_expected = resume_id.checked_add(1).unwrap();
    executor.outstanding_resume = Some(wrong_expected);
    let before = executor.snapshot();

    let recovery = match executor
        .resume_attached_recovery_effects_observed(resume, &queue)
        .unwrap_err()
    {
        OwnerTargetError::RejectedAttachedRecoveryResume {
            block: OwnerTargetInputBlock::InvalidEffectResume { expected, actual },
            recovery,
        } => {
            assert_eq!(expected, wrong_expected);
            assert_eq!(actual, resume_id);
            *recovery
        }
        other => panic!("stale recovery resume must be returned intact: {other:?}"),
    };
    assert_eq!(executor.snapshot(), before);
    assert_eq!(recovery.resume.id, resume_id);

    executor.outstanding_resume = Some(resume_id);
    let page = executor
        .resume_attached_recovery_effects_observed(recovery, &queue)
        .unwrap();
    let (cursor, observations) = page.into_progress_parts();
    assert!(observations.is_empty());
    assert!(
        executor
            .publish_attached_recovery_page(cursor, &mut queue)
            .unwrap()
            .iter()
            .any(|output| matches!(output, OwnerTargetRecoveryOutput::RecoveryCompleted { .. }))
    );
}

#[test]
fn initial_execute_fatal_sticky_retains_recovery_guard_and_exact_page_state() {
    let (mut executor, _endpoint, queue, recovery) = enqueued_replacement_recovery(1, 1, 4);
    executor.next_resume_id = None;

    let error = executor
        .execute_attached_recovery_observed(recovery, &queue)
        .unwrap_err();

    assert!(matches!(error, OwnerTargetError::EffectResumeIdExhausted));
    let snapshot = executor.snapshot();
    assert_eq!(snapshot.aborted_recovery_guards, 1);
    assert_eq!(snapshot.aborted_effects, 1);
    assert_eq!(snapshot.aborted_outputs, 1);
    assert_eq!(queue.len(), 1, "the accepted barrier remains queue-owned");
    assert!(executor.attach_barrier_pending);
}

#[test]
fn resumed_execute_fatal_sticky_retains_recovery_guard_and_unpublished_page() {
    let (mut executor, _endpoint, mut queue, recovery) = enqueued_replacement_recovery(2, 1, 4);
    let first = executor
        .execute_attached_recovery_observed(recovery, &queue)
        .unwrap();
    let (first_cursor, first_observations) = first.into_progress_parts();
    assert!(first_observations.is_empty());
    let resume = executor
        .publish_attached_recovery_page(first_cursor, &mut queue)
        .unwrap()
        .into_iter()
        .find_map(|output| match output {
            OwnerTargetRecoveryOutput::NeedsResume { resume } => Some(resume),
            _ => None,
        })
        .expect("first page must publish the exact recovery continuation");
    executor.next_resume_id = None;

    let error = executor
        .resume_attached_recovery_effects_observed(resume, &queue)
        .unwrap_err();

    assert!(matches!(error, OwnerTargetError::EffectResumeIdExhausted));
    let snapshot = executor.snapshot();
    assert_eq!(snapshot.aborted_recovery_guards, 1);
    assert_eq!(snapshot.aborted_effects, 1);
    assert_eq!(snapshot.aborted_outputs, 1);
    assert_eq!(queue.len(), 1, "no resumed-page bytes were published");
    assert!(executor.attach_barrier_pending);
}
