//! Original backing refusal and defensive incoming custody; no whole-root claim.
use super::*;

#[test]
fn installed_owner_data_producer_reuses_ordinary_backing_without_effect_allocation() {
    let (mut executor, handoff, _owner, queue) = checked_owner(0x9d, 128);
    let leg = committed(&handoff.attached);
    let flow_id = SessionFlowId::new(1).unwrap();
    open_flow(&mut executor, leg, flow_id, target(443));
    let source = executor
        .flows
        .get_mut(&flow_id)
        .unwrap()
        .source
        .as_mut()
        .unwrap();
    source
        .flow
        .try_send_uplink_with(4, || Bytes::from_static(b"data"))
        .unwrap();
    let input = source.driver.try_recv_next().unwrap().unwrap();
    assert!(matches!(input, DriverInput::Data(_)));
    let probe = source.flow.probe();
    let before = probe.snapshot();
    let DriverInput::Data(data) = &input else {
        unreachable!()
    };
    let payload_identity = data.payload().as_ptr();
    crate::resumable::effect_storage::fail_next_effect_allocation_for_test();
    let error = match executor.apply_command(SessionOwnerCommand::Driver(input)) {
        Err(error) => error,
        Ok(_) => panic!("real pre-entry reserve must reject the original Driver input"),
    };
    let audit = error
        .cold_inventory()
        .expect("complete original Driver audit");
    assert!(!audit.is_exact_zero());
    assert_eq!((audit.owner.commands, audit.owner.driver_inputs), (1, 1));
    assert_eq!(
        (audit.uplink.data_values, audit.uplink.ownership_values),
        (1, 1)
    );
    assert_eq!(
        (audit.uplink.payload_bytes, audit.uplink.stored_bytes),
        (4, 4)
    );
    assert_eq!(
        (
            audit.uplink.staging_payload_views,
            audit.uplink.staging_bytes
        ),
        (1, 4)
    );
    assert_eq!(audit.uplink.permit_bundles, 1);
    assert_eq!(
        (
            audit.uplink.flow_byte_permits,
            audit.uplink.session_byte_permits
        ),
        (4, 4)
    );
    assert_eq!(
        (
            audit.uplink.flow_segment_permits,
            audit.uplink.session_segment_permits
        ),
        (1, 1)
    );
    assert_eq!(
        (audit.uplink.factory_origins, audit.uplink.progress_guards),
        (1, 1)
    );
    assert_eq!(error.cold_inventory(), Ok(audit));
    assert_eq!(probe.snapshot(), before);
    let OwnerTargetError::RejectedCommand { block, command } = error else {
        panic!("same command must remain returned");
    };
    assert_eq!(
        block,
        OwnerTargetInputBlock::EffectStorage(
            crate::resumable::effect_storage::EffectStorageError::Reserve
        )
    );
    let SessionOwnerCommand::Driver(input) = *command else {
        unreachable!()
    };
    let DriverInput::Data(data) = &input else {
        unreachable!()
    };
    assert_eq!(data.payload().as_ptr(), payload_identity);
    assert_eq!(data.payload(), b"data");
    crate::resumable::effect_storage::fail_next_effect_allocation_for_test();
    let (batch, observations) = executor
        .reduce_owner_command_observed(SessionOwnerCommand::Driver(input))
        .unwrap();
    // The effect-allocator refusal is still armed: this is not a claim about
    // normalization, replay maps or allocation outside that explicit seam.
    assert_eq!(
        crate::resumable::effect_storage::exact_vec::<u8>(1),
        Err(crate::resumable::effect_storage::EffectStorageError::Reserve)
    );
    assert_eq!(batch.capacity(), 3);
    assert_eq!(batch.len(), 1);
    assert!(
        batch
            .iter()
            .any(|effect| matches!(effect, SessionEffect::Transmit(frame)
        if matches!(frame.record(), Record::Data { flow_id: retained, payload, .. }
            if *retained == flow_id && payload.as_ref() == b"data")))
    );
    assert_eq!(
        observations.replay_became_outstanding().unwrap().flow_id(),
        flow_id
    );
    executor
        .execute_effects_observed(batch, observations)
        .unwrap();
    assert_eq!(executor.snapshot().target_read_owned_bytes, 4);
    assert_eq!(executor.snapshot().target_read_owned_segments, 1);
    assert!(handoff.acceptance_belongs_to_queue(&queue));
}

#[test]
fn entry_output_reserve_returns_original_data_before_reducer_or_target_commit() {
    let (mut executor, handoff, _owner, queue) = checked_owner(0x9c, 128);
    let leg = committed(&handoff.attached);
    let flow_id = SessionFlowId::new(1).unwrap();
    open_flow(&mut executor, leg, flow_id, target(443));
    assert_eq!(executor.prepared_outputs.capacity(), 0);
    let payload = Bytes::from_owner(vec![1u8, 2, 3, 4]);
    let frame = Frame::try_new(
        leg.generation(),
        Record::Data {
            flow_id,
            direction: Direction::ClientToTarget,
            offset: ByteOffset::new(0),
            payload,
        },
    )
    .unwrap();
    // Frame construction already normalizes DATA; observe the exact admitted
    // Frame backing, not the earlier caller buffer that normalization replaces.
    let Record::Data { payload, .. } = frame.record() else {
        unreachable!()
    };
    let address = payload.as_ptr();
    let encoded_bytes = frame.encoded_len_checked().unwrap();
    let before = executor.snapshot();
    let target_before = executor.target().snapshot();
    crate::resumable::effect_storage::fail_next_effect_allocation_for_test();
    let error = executor
        .apply_event_observed(SessionEvent::PeerFrame { leg, frame })
        .err()
        .expect("pre-entry output preparation must refuse");
    // Audit the actual returned owner before consuming it; this cannot replace
    // the original payload, commit Target work or manufacture a clean result.
    let audit = error
        .cold_inventory()
        .expect("complete original DATA error audit");
    assert!(!audit.is_exact_zero());
    assert_eq!(audit.target_error_nodes, 1);
    assert_eq!(audit.payload.events, 1);
    assert_eq!(audit.payload.session_frames, 1);
    assert_eq!(audit.payload.session_encoded_bytes, encoded_bytes);
    assert_eq!(
        (audit.payload.byte_views, audit.payload.logical_bytes),
        (1, 4)
    );
    assert_eq!(audit.payload.capability_values, 1);
    assert_eq!(error.cold_inventory(), Ok(audit));
    let OwnerTargetError::RejectedEvent { block, event } = error else {
        panic!("return untouched event");
    };
    assert_eq!(
        block,
        OwnerTargetInputBlock::EffectStorage(
            crate::resumable::effect_storage::EffectStorageError::Reserve
        )
    );
    let SessionEvent::PeerFrame {
        leg: returned,
        frame,
    } = event.as_ref()
    else {
        panic!("same DATA event");
    };
    assert_eq!(*returned, leg);
    let Record::Data { payload, .. } = frame.record() else {
        panic!("same DATA frame");
    };
    assert_eq!(payload.as_ptr(), address);
    assert_eq!(payload.as_ref(), &[1, 2, 3, 4]);
    assert_eq!(executor.snapshot(), before);
    assert_eq!(executor.target().snapshot(), target_before);
    assert!(executor.aborted_effects.is_none());
    let observed = executor.apply_event_observed(*event).unwrap();
    assert_eq!(executor.target().snapshot().write_accepted_bytes, 4);
    assert!(transmitted(&observed.into_outputs()).iter().any(|record| matches!(record,
        Record::Ack { flow_id: retained, next_accepted, .. } if *retained == flow_id && next_accepted.get() == 4)));
    assert!(handoff.acceptance_belongs_to_queue(&queue));
}

#[test]
fn actual_target_read_invariant_keeps_inline_current_and_original_backing() {
    let (mut executor, handoff, _owner, queue) = checked_owner(0x9a, 128);
    let leg = committed(&handoff.attached);
    let flow_id = SessionFlowId::new(1).unwrap();
    open_flow(&mut executor, leg, flow_id, target(443));
    let backing = (
        executor.pending_effects.as_slices().0.as_ptr(),
        executor.pending_effects.capacity(),
    );
    assert!(backing.1 > 0);
    // Deliberately fault the real Target's state outside normal Executor
    // settlement. Its actual read then returns FlowTerminal, not a fake port.
    executor.target_mut().reset(flow_id).unwrap();
    let before = executor.target().snapshot();
    let error = executor
        .try_read_target_observed(flow_id)
        .err()
        .expect("real Target invariant");
    assert!(
        matches!(error, OwnerTargetError::Target(TargetIoError::FlowTerminal {
        flow_id: retained, kind: TargetTerminalKind::Reset,
    }) if retained == flow_id)
    );
    assert_eq!(
        executor.target().snapshot().read_calls,
        before.read_calls + 1
    );
    let page = executor.aborted_effects.as_ref().unwrap();
    assert!(
        matches!(page.current, Some(OwnerTargetWork::TargetIoInvariant {
        flow_id: retained, operation: TargetIoOperation::Read,
    }) if retained == flow_id)
    );
    assert!(page.effects.is_empty());
    assert_eq!(
        (page.effects.as_slices().0.as_ptr(), page.effects.capacity()),
        backing
    );
    assert_eq!(executor.pending_effects.capacity(), 0);
    assert_eq!(page.retained_inventory().0, 1);
    let retained = executor.snapshot();
    let calls = executor.target().snapshot().read_calls;
    assert!(matches!(
        executor.try_read_target_observed(flow_id),
        Err(OwnerTargetError::AbortedEffectInvariant)
    ));
    assert_eq!(executor.target().snapshot().read_calls, calls);
    assert_eq!(executor.snapshot(), retained);
    assert!(handoff.acceptance_belongs_to_queue(&queue));
}

#[test]
fn original_effect_layout_rejection_returns_bootstrap_and_target_before_factory_mint() {
    let (bootstrap, _owner, queue) = checked_bootstrap(0x96);
    let invalid = OwnerTargetConfig::new(1, usize::MAX - 3, 64).unwrap();
    assert_eq!(
        invalid.max_pending_effects().checked_add(3),
        Some(usize::MAX)
    );
    let source = FlowPortConfig::new(64, 8, 8, 8).unwrap();
    let drops = Arc::new(AtomicUsize::new(0));
    let target = DropTrackedTarget::new(Arc::clone(&drops));
    let identity = target.identity_ptr();
    let queue_before = queue.cold_inventory().unwrap();
    let failure = OwnerTargetExecutor::from_initial_bootstrap(invalid, bootstrap, target, source)
        .unwrap_err();
    let (error, returned_config, bootstrap, target, returned_source) = failure.into_parts();
    assert_eq!(
        error,
        OwnerTargetConfigError::EffectStorage(
            crate::resumable::effect_storage::EffectStorageError::Layout,
        )
    );
    assert_eq!(returned_config, invalid);
    assert_eq!(returned_source, source);
    assert_eq!(target.identity_ptr(), identity);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert_eq!(queue.cold_inventory().unwrap(), queue_before);
    let valid = OwnerTargetConfig::new(1, 1_024, 64).unwrap();
    let (executor, handoff) =
        OwnerTargetExecutor::from_initial_bootstrap(valid, bootstrap, target, returned_source)
            .unwrap();
    assert_eq!(executor.target().identity_ptr(), identity);
    assert_eq!(executor.pending_effects.capacity(), 3);
    assert_eq!(executor.prepared_outputs.capacity(), 1);
    assert!(handoff.acceptance_belongs_to_queue(&queue));
    assert_eq!(handoff.retained_acceptance_receipts(), 1);
    assert_eq!(queue.cold_inventory().unwrap(), queue_before);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop((handoff, executor));
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn entered_output_reserve_failure_retains_actual_open_before_target_callback() {
    let (mut executor, handoff, _owner, queue) = checked_owner(0x97, 1);
    let leg = committed(&handoff.attached);
    let flow_id = SessionFlowId::new(1).unwrap();
    let before_target = executor.target().snapshot();
    let target = target(443);
    crate::resumable::effect_storage::fail_next_effect_allocation_for_test();
    let error = executor
        .apply_event_observed(SessionEvent::PeerFrame {
            leg,
            frame: Frame::try_new(
                leg.generation(),
                Record::Open {
                    flow_id,
                    target: target.clone(),
                },
            )
            .unwrap(),
        })
        .err()
        .expect("output growth must refuse before the Target callback");
    assert!(matches!(
        error,
        OwnerTargetError::EffectStorage(
            crate::resumable::effect_storage::EffectStorageError::Reserve,
        )
    ));
    assert_eq!(executor.target().snapshot(), before_target);
    let page = executor.aborted_effects.as_ref().unwrap();
    let Some(OwnerTargetWork::Effect(SessionEffect::PeerOpenRequested {
        request,
        flow,
        target: retained,
    })) = &page.current
    else {
        panic!("the actual committed OPEN stays in the original current");
    };
    assert_eq!(request.flow_id(), flow_id);
    assert_eq!(flow.flow_id(), flow_id);
    assert_eq!(retained, &target);
    assert!(page.effects.is_empty());
    assert!(page.outputs.is_empty());
    assert!(page.incoming.is_none());
    assert!(page.source.is_none());
    assert_eq!(page.effects.capacity(), 3);
    assert_eq!(page.outputs.capacity(), 1);
    assert_eq!(page.retained_inventory().0, 1);
    let before_retry = executor.snapshot();
    let error = executor
        .apply_event_observed(SessionEvent::LegLost { leg })
        .err()
        .expect("quarantined current rejects reentry");
    let OwnerTargetError::RejectedEvent { block, event } = error else {
        panic!("entered failure blocks a new event without consuming it");
    };
    assert_eq!(block, OwnerTargetInputBlock::AbortedEffectInvariant);
    assert!(matches!(*event, SessionEvent::LegLost { leg: returned } if returned == leg));
    assert_eq!(executor.snapshot(), before_retry);
    assert_eq!(executor.target().snapshot(), before_target);
    assert!(handoff.acceptance_belongs_to_queue(&queue));
}

#[test]
fn occupied_incoming_returns_second_real_batch_and_keeps_first_original_work() {
    // Defensive admission characterization: two real producers, not a claim
    // that serialized production reenters an occupied incoming position.
    let (mut first, first_handoff, _first_owner, first_queue) = checked_owner(0x98, 128);
    let (mut second, second_handoff, _second_owner, second_queue) = checked_owner(0x99, 128);
    let reduce_terminal = |executor: &mut OwnerTargetExecutor<MemoryTarget>, leg, flow_id| {
        open_flow(executor, leg, flow_id, target(443));
        executor
            .reduce_owner_command_observed(SessionOwnerCommand::Event(SessionEvent::PeerFrame {
                leg,
                frame: Frame::try_new(
                    leg.generation(),
                    Record::Reset {
                        flow_id,
                        reason: ResetReason::TargetFailure,
                    },
                )
                .unwrap(),
            }))
            .unwrap()
    };
    let (batch_a, facts_a) = reduce_terminal(
        &mut first,
        committed(&first_handoff.attached),
        SessionFlowId::new(1).unwrap(),
    );
    let (batch_b, facts_b) = reduce_terminal(
        &mut second,
        committed(&second_handoff.attached),
        SessionFlowId::new(2).unwrap(),
    );
    let backing_a = (batch_a.as_ptr(), batch_a.capacity());
    let backing_b = (batch_b.as_ptr(), batch_b.capacity());
    let count_a = batch_a.len();
    let count_b = batch_b.len();
    let terminal_a = *facts_a.flow_terminal().unwrap().terminal();
    let terminal_b = *facts_b.flow_terminal().unwrap().terminal();
    assert!(count_a > 0 && count_b > 0);
    let missing = first.stage_entered_reduction(batch_a, facts_a).unwrap_err();
    let missing_audit = missing
        .cold_inventory()
        .expect("complete original batch audit");
    assert_eq!(missing_audit.target_error_nodes, 2);
    assert_eq!(missing_audit.observation_facts, 1);
    assert_eq!(missing_audit.payload.effect_batches, 1);
    assert_eq!(missing_audit.payload.effects, count_a);
    assert_eq!(missing_audit.payload.effect_capacity, backing_a.1);
    assert_eq!(
        missing_audit.payload.effect_array_bytes,
        std::alloc::Layout::array::<SessionEffect>(backing_a.1)
            .unwrap()
            .size()
    );
    assert_eq!(missing.cold_inventory(), Ok(missing_audit));
    let OwnerTargetError::PostCommitObservationRetentionInvariant {
        error,
        observations: facts_a,
        effects: batch_a,
    } = missing
    else {
        panic!("missing page must return the original already-produced batch and facts");
    };
    assert!(matches!(*error, OwnerTargetError::AbortedEffectInvariant));
    assert_eq!(
        (batch_a.as_ptr(), batch_a.capacity(), batch_a.len()),
        (backing_a.0, backing_a.1, count_a)
    );
    assert_eq!(facts_a.flow_terminal().unwrap().terminal(), &terminal_a);
    first
        .begin_observed_page(
            VecDeque::new(),
            Vec::new(),
            OwnerTargetReducerObservations::default(),
            None,
        )
        .unwrap();
    first.stage_entered_reduction(batch_a, *facts_a).unwrap();
    let blocked = first
        .execute_effects_observed(batch_b, facts_b)
        .err()
        .expect("entered page rejects new execution");
    let OwnerTargetError::PostCommitObservationRetentionInvariant {
        error,
        observations: facts_b,
        effects: batch_b,
    } = blocked
    else {
        panic!("entry guard must return its complete original batch and facts");
    };
    assert!(matches!(*error, OwnerTargetError::AbortedEffectInvariant));
    assert_eq!(
        (batch_b.as_ptr(), batch_b.capacity(), batch_b.len()),
        (backing_b.0, backing_b.1, count_b)
    );
    assert_eq!(facts_b.flow_terminal().unwrap().terminal(), &terminal_b);
    let error = first
        .stage_entered_reduction(batch_b, *facts_b)
        .unwrap_err();
    let OwnerTargetError::PostCommitObservationRetentionInvariant {
        error,
        observations,
        effects,
    } = error
    else {
        panic!("occupied incoming must carry the complete second original batch");
    };
    assert!(matches!(*error, OwnerTargetError::AbortedEffectInvariant));
    assert_eq!((effects.as_ptr(), effects.capacity()), backing_b);
    assert_eq!(effects.len(), count_b);
    assert_eq!(
        observations.flow_terminal().unwrap().terminal(),
        &terminal_b
    );
    let page = first.aborted_effects.as_ref().unwrap();
    let incoming = page.incoming.as_ref().unwrap();
    assert_eq!(
        (incoming.effects.as_ptr(), incoming.effects.capacity()),
        backing_a
    );
    assert!(incoming.effects.is_empty());
    assert_eq!(
        incoming.observations.flow_terminal().unwrap().terminal(),
        &terminal_a
    );
    assert_eq!(page.effects.len(), count_a);
    assert!(page.effects.iter().any(|work| matches!(work,
        OwnerTargetWork::Effect(SessionEffect::FlowFinished { terminal, .. }) if *terminal == terminal_a)));
    assert!(effects.iter().any(|effect| matches!(effect,
        SessionEffect::FlowFinished { terminal, .. } if *terminal == terminal_b)));
    assert_eq!(page.retained_inventory().0, count_a);
    assert!(first_handoff.acceptance_belongs_to_queue(&first_queue));
    assert!(second_handoff.acceptance_belongs_to_queue(&second_queue));

    // Retry the returned second original batch only after A's normal incoming
    // transfer. The two real terminal facts then challenge the merge guard.
    first.finish_entered_reduction((count_a, count_a)).unwrap();
    first.prepare_effect_entry(count_b).unwrap();
    let bounds = first
        .stage_entered_reduction(effects, *observations)
        .unwrap();
    let error = first.finish_entered_reduction(bounds).unwrap_err();
    let OwnerTargetError::PostCommitObservationConflict { kind, observations } = error else {
        panic!("conflicting actual terminal facts retain B without replacing A");
    };
    assert_eq!(kind, OwnerTargetObservationKind::FlowTerminal);
    assert_eq!(
        observations.flow_terminal().unwrap().terminal(),
        &terminal_b
    );
    let page = first.aborted_effects.as_ref().unwrap();
    assert_eq!(
        page.observations
            .as_ref()
            .unwrap()
            .flow_terminal()
            .unwrap()
            .terminal(),
        &terminal_a
    );
    let incoming = page.incoming.as_ref().unwrap();
    assert_eq!(
        (incoming.effects.as_ptr(), incoming.effects.capacity()),
        backing_b
    );
    assert!(incoming.effects.is_empty());
    assert_eq!(page.effects.len(), count_a + count_b);
    for expected in [terminal_a, terminal_b] {
        assert_eq!(page.effects.iter().filter(|work| matches!(work,
            OwnerTargetWork::Effect(SessionEffect::FlowFinished { terminal, .. }) if *terminal == expected)).count(), 1);
    }
}
