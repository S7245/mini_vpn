//! Original checked Controller recovery through real FIFO pressure and delivery.
//! Owns this characterization; no actor lifecycle or whole-root zero claim.
use super::*;

#[tokio::test(start_paused = true)]
async fn checked_bound_recovery_retries_the_same_publication_after_real_fifo_pressure() {
    checked_recovery_fixture(false);
}

#[tokio::test(start_paused = true)]
async fn checked_bound_recovery_resume_exhaustion_retains_original_fatal_tail() {
    checked_recovery_fixture(true);
}

fn checked_recovery_fixture(exhaust_resume: bool) {
    use crate::owned_upstream::owner_ready::{OwnerReadySessionLimits, ReadyGeometry, ReadyIndex};
    use crate::owned_upstream::owner_session_controller::{
        OwnerSessionController, OwnerSessionRecoveryOutput, OwnerSessionRegisteredAttachDrive,
    };
    use crate::owned_upstream::quinn_leg_composition::QuinnLegCompositionGate;
    use crate::owned_upstream::session_clock::SessionMonotonicClock;

    let plan = capacity_plan();
    let mut wire = wire();
    let mut client_a = endpoint(LegId::A, LegEndpointRole::Client, 0xa1);
    let mut owner_a = endpoint(LegId::A, LegEndpointRole::Owner, 0xa1);
    let mut client_queue =
        LegOutboundQueue::for_leg(client_a.established_leg(), 8, 32_768).unwrap();
    let mut owner_queue = LegOutboundQueue::for_leg(owner_a.established_leg(), 8, 32_768).unwrap();
    let request = standby_attach_request(2, 0x22);
    let proof = credentials().prove(&request, &binding(0xa1)).unwrap();
    let awaiting = client_a
        .established_leg()
        .begin_attach(request)
        .bind_initial_frame(request.to_attach_frame(proof))
        .unwrap()
        .enqueue(&mut client_queue)
        .unwrap();
    let BoundInbound::Session(received) =
        deliver(&mut client_queue, &mut client_a, &mut owner_a, &mut wire)
    else {
        panic!("actual initial ATTACH");
    };
    let bootstrap = SessionSupervisor::prepare_owner(plan, standby_authority(1))
        .accept(owner_a.established_leg(), received, &mut owner_queue)
        .unwrap()
        .enqueue_acceptance(&mut owner_queue)
        .unwrap();
    let BoundInbound::Session(accepted) =
        deliver(&mut owner_queue, &mut owner_a, &mut client_a, &mut wire)
    else {
        panic!("actual initial acceptance");
    };
    let accepted = awaiting.validate_response(&client_queue, accepted).unwrap();
    retire_wait(&mut owner_queue, &mut owner_a, &mut wire);
    retire_wait(&mut client_queue, &mut client_a, &mut wire);
    let client = ClientSessionController::bootstrap_initial(
        plan.session_config(),
        accepted,
        client_a,
        client_queue,
    )
    .unwrap();
    let (mut executor, handoff) = OwnerTargetExecutor::from_initial_bootstrap(
        OwnerTargetConfig::new(
            1,
            plan.session_config().owner_pending_work_bound().unwrap(),
            64,
        )
        .unwrap(),
        bootstrap,
        MemoryTarget::new(MemoryTargetConfig::new(4, 256, 512, 128, 128, 16).unwrap()),
        FlowPortConfig::new(64, 8, 8, 8).unwrap(),
    )
    .unwrap();
    let initial = committed(&handoff.attached);
    let flow_id = SessionFlowId::new(1).unwrap();
    let frame = Frame::try_new(
        initial.generation(),
        Record::Open {
            flow_id,
            target: target(443),
        },
    )
    .unwrap();
    wire.controller_sender()
        .send_encoded(
            SimTime::ZERO,
            owner_a.inbound_route(),
            WireLane::Control,
            frame.encode().unwrap().into(),
        )
        .unwrap();
    let BoundInbound::Session(received) = receive(&mut wire, &mut owner_a) else {
        panic!("actual original OPEN");
    };
    let outputs = executor
        .apply_event(handoff.attached.accept_frame(received).unwrap())
        .unwrap();
    let outputs = drain_resumes(&mut executor, outputs);
    assert_eq!(
        transmitted(&outputs),
        vec![&Record::OpenResult {
            flow_id,
            result: OpenResultCode::Opened,
        }]
    );
    assert_eq!(executor.target().snapshot().open_attempts, 1);

    let mut client_b = endpoint(LegId::B, LegEndpointRole::Client, 0xa2);
    let mut owner_b = endpoint(LegId::B, LegEndpointRole::Owner, 0xa2);
    let mut client_b_queue =
        LegOutboundQueue::for_leg(client_b.established_leg(), 8, 32_768).unwrap();
    let mut owner_b_queue =
        LegOutboundQueue::for_leg(owner_b.established_leg(), 1, 32_768).unwrap();
    let awaiting = client
        .begin_current_standby_registration(
            client_b.established_leg(),
            StandbyNonce::new([0x55; 16]).unwrap(),
            &credentials(),
        )
        .unwrap()
        .enqueue(&mut client_b_queue)
        .unwrap();
    let inbound = deliver(&mut client_b_queue, &mut client_b, &mut owner_b, &mut wire);
    let authenticated = executor
        .authenticate_owner_standby(&handoff.attached, owner_b.established_leg(), inbound)
        .unwrap();
    let mut slot = OwnerStandbySlot::new();
    assert_eq!(
        slot.admit(authenticated, &mut owner_b_queue).unwrap(),
        OwnerStandbyAdmission::Installed
    );
    let inbound = deliver(&mut owner_b_queue, &mut owner_b, &mut client_b, &mut wire);
    let registered = awaiting.validate(inbound).unwrap();
    retire_wait(&mut client_b_queue, &mut client_b, &mut wire);
    retire_wait(&mut owner_b_queue, &mut owner_b, &mut wire);

    // The original opened Executor crosses the production checked constructor.
    // This binding is deliberately BoundInactive, not an Active physical root.
    if exhaust_resume {
        executor.exhaust_effect_resume_ids_for_test();
    }
    let geometry = ReadyGeometry::checked(std::num::NonZeroUsize::new(1).unwrap()).unwrap();
    let mut index = ReadyIndex::cold(&geometry).unwrap();
    let limits =
        OwnerReadySessionLimits::checked(plan.session_config().max_flows(), 1, 8, 8).unwrap();
    let ready = index.reserve_session(initial.session_id(), limits).unwrap();
    let mut controller = OwnerSessionController::new_checked(
        executor,
        SessionMonotonicClock::start(),
        plan,
        ready,
        Duration::from_millis(50),
    )
    .unwrap();
    let gate = QuinnLegCompositionGate::mint_for_test();
    let awaiting = registered
        .begin_exact_next_attach(AttachNonce::new([0x66; 16]).unwrap(), &credentials())
        .unwrap()
        .enqueue(&mut client_b_queue)
        .unwrap();
    let BoundInbound::Session(received) =
        deliver(&mut client_b_queue, &mut client_b, &mut owner_b, &mut wire)
    else {
        panic!("actual registered ATTACH");
    };
    let ready = controller.preflight_registered_recovery().unwrap();
    let OwnerSessionRegisteredAttachDrive::Recovery(drive) = controller
        .accept_registered_owner_attach(
            ready,
            &mut slot,
            owner_b.established_leg(),
            received,
            &mut owner_b_queue,
            &gate,
        )
        .unwrap()
    else {
        panic!("the registered acceptance enters the one-place FIFO");
    };
    assert!(drive.needs_execute());
    assert_eq!(owner_b_queue.len(), 1);
    let initial_payload = controller.cold_inventory().unwrap().recovery_payload();
    assert_eq!(initial_payload.owner.unexecuted_recoveries, 1);
    assert_eq!(initial_payload.recovery.attached_authorities, 1);
    assert_eq!(initial_payload.recovery.acceptance_receipts, 1);
    assert_eq!(initial_payload.payload.effect_vectors, 1);
    assert!(initial_payload.payload.effects > 0);
    if exhaust_resume {
        let fifo = owner_b_queue.cold_inventory().unwrap();
        let error = controller
            .execute_attached_recovery(&owner_b_queue, &gate)
            .unwrap_err();
        assert_eq!(
            format!("{error:?}"),
            "Target(\"owner Target effect resume ID space is exhausted\")"
        );
        assert_eq!(error.cold_inventory().unwrap().target_error_nodes, 1);
        let held = controller.snapshot();
        assert!(held.failed);
        assert_eq!(held.pending_recoveries, 0);
        assert_eq!(held.retained_recoveries, 1);
        assert_eq!(held.retained_recovery_cursors, 1);
        assert_eq!(held.retained_recovery_outputs, 1);
        assert_eq!(held.retained_recovery_observations, 0);
        assert_eq!(held.retained_recovery_attached, 1);
        let cold = controller.cold_inventory().unwrap();
        assert!(!cold.is_exact_zero());
        assert_eq!(cold.target_aborted_effect_guards(), 1);
        assert_eq!(cold.target_aborted_effects(), 1);
        assert_eq!(cold.target_retained_recovery_outputs(), 1);
        assert_eq!(cold.target_retained_recovery_observations(), 0);
        assert_eq!(cold.target_retained_recovery_guards(), 1);
        assert_eq!(cold.controller_retained_recovery_turns(), 0);
        assert!(
            cold.recovery_payload().is_exact_zero(),
            "Executor alone owns the cursor"
        );
        assert!(
            cold.error_payload().unwrap().is_exact_zero(),
            "caller alone owns the cause"
        );
        assert_eq!(owner_b_queue.len(), 1);
        for _ in 0..2 {
            assert_eq!(
                format!(
                    "{:?}",
                    controller
                        .execute_attached_recovery(&owner_b_queue, &gate)
                        .unwrap_err()
                ),
                "Failed"
            );
            assert_eq!(
                format!(
                    "{:?}",
                    controller
                        .publish_attached_recovery(&mut owner_b_queue, &gate)
                        .unwrap_err()
                ),
                "Failed"
            );
            assert_eq!(controller.snapshot(), held);
            assert_eq!(controller.cold_inventory().unwrap(), cold);
            assert_eq!(owner_b_queue.cold_inventory().unwrap(), fifo);
        }
        // Fatal retention is deliberately nonzero, not cleanup/whole-root proof.
        assert!(handoff.acceptance_belongs_to_queue(&owner_queue));
        return;
    }
    assert!(
        controller
            .execute_attached_recovery(&owner_b_queue, &gate)
            .unwrap()
            .needs_publish()
    );
    let drive = controller
        .publish_attached_recovery(&mut owner_b_queue, &gate)
        .unwrap();
    assert!(drive.needs_execute());
    assert!(
        matches!(drive.outputs(), [OwnerSessionRecoveryOutput::LegActivated { generation }] if generation.get() == 3)
    );
    let execute_payload = controller.cold_inventory().unwrap().recovery_payload();
    assert_eq!(execute_payload.recovery.resumes, 1);
    assert_eq!(execute_payload.recovery.attached_authorities, 1);
    assert_eq!(execute_payload.recovery.acceptance_receipts, 1);
    assert_eq!(execute_payload.recovery.cursors, 0);
    assert!(execute_payload.payload.is_exact_zero());
    assert!(
        controller
            .execute_attached_recovery(&owner_b_queue, &gate)
            .unwrap()
            .needs_publish()
    );
    let held = controller.snapshot();
    let occupied = owner_b_queue.cold_inventory().unwrap();
    assert!(held.recovery_needs_publish);
    assert_eq!(held.pending_recoveries, 1);
    assert!(!held.recovery_handoff_ready);
    assert!(
        !controller.cold_inventory().unwrap().is_exact_zero(),
        "the actual C-held Publish cursor cannot be cold exact zero"
    );
    let cold = controller.cold_inventory().unwrap();
    let payload = cold.recovery_payload();
    assert_eq!(payload.target_error_nodes, 0);
    assert!(cold.error_payload().unwrap().is_exact_zero());
    assert_eq!(payload.recovery.cursors, 1);
    assert_eq!(payload.recovery.output_values, 1);
    assert_eq!(payload.recovery.attached_authorities, 1);
    assert_eq!(payload.recovery.acceptance_receipts, 1);
    assert_eq!(payload.recovery.resumes, 0);
    assert_eq!(payload.recovery.frames, 1);
    assert_eq!(payload.payload.session_frames, 1);
    let expected_frame = Frame::try_new(
        LegGeneration::new(3).unwrap(),
        Record::OpenResult {
            flow_id,
            result: OpenResultCode::Opened,
        },
    )
    .unwrap();
    assert_eq!(
        payload.recovery.frame_encoded_bytes,
        expected_frame.encode().unwrap().len()
    );
    assert_eq!(
        payload.payload.session_encoded_bytes,
        payload.recovery.frame_encoded_bytes
    );
    assert!(payload.recovery.pending_capacity >= 1);
    assert_eq!(payload.recovery.ready_capacity, 0);
    assert_eq!(
        payload.recovery.container_bytes,
        std::alloc::Layout::array::<OwnerTargetOutput>(payload.recovery.pending_capacity)
            .unwrap()
            .size()
    );
    for _ in 0..3 {
        let error = controller
            .publish_attached_recovery(&mut owner_b_queue, &gate)
            .unwrap_err();
        assert!(error.is_recovery_queue_pressure());
        assert_eq!(controller.snapshot(), held);
        assert_eq!(owner_b_queue.cold_inventory().unwrap(), occupied);
        assert_eq!(controller.cold_inventory().unwrap(), cold);
        assert!(
            error.cold_inventory().unwrap().is_exact_zero(),
            "C alone retains this cursor"
        );
        assert!(
            controller
                .execute_attached_recovery(&owner_b_queue, &gate)
                .is_err()
        );
        assert_eq!(controller.snapshot(), held);
        assert_eq!(controller.cold_inventory().unwrap(), cold);
    }
    // Only actual delivery frees the FIFO. Never forge a delivery notification.
    let inbound = deliver(&mut owner_b_queue, &mut owner_b, &mut client_b, &mut wire);
    let RegisteredAttachResponse::Accepted(_accepted) =
        awaiting.validate_response(inbound).unwrap()
    else {
        panic!("peer validates the original registered acceptance exactly once");
    };
    retire_wait(&mut owner_b_queue, &mut owner_b, &mut wire);
    retire_wait(&mut client_b_queue, &mut client_b, &mut wire);
    let drive = controller
        .publish_attached_recovery(&mut owner_b_queue, &gate)
        .unwrap();
    assert!(drive.handoff_ready());
    assert!(!drive.needs_execute());
    assert!(!drive.needs_publish());
    assert!(drive.outputs().is_empty());
    let handoff_cold = controller.cold_inventory().unwrap();
    let handoff_payload = handoff_cold.recovery_payload();
    assert!(!handoff_cold.is_exact_zero());
    assert_eq!(handoff_payload.recovery.attached_authorities, 1);
    assert_eq!(handoff_payload.recovery.acceptance_receipts, 0);
    assert_eq!(handoff_payload.recovery.cursors, 0);
    assert_eq!(handoff_payload.recovery.output_values, 0);
    assert_eq!(handoff_payload.recovery.container_bytes, 0);
    assert!(handoff_payload.payload.is_exact_zero());
    let queued = owner_b_queue.cold_inventory().unwrap();
    assert_eq!(owner_b_queue.len(), 1);
    assert!(
        controller
            .publish_attached_recovery(&mut owner_b_queue, &gate)
            .is_err()
    );
    assert_eq!(owner_b_queue.cold_inventory().unwrap(), queued);
    let BoundInbound::Session(received) =
        deliver(&mut owner_b_queue, &mut owner_b, &mut client_b, &mut wire)
    else {
        panic!("the retained original recovery page reaches the real peer");
    };
    let frame = received.into_frame_for_test();
    assert_eq!(frame.leg_generation().get(), 3);
    assert_eq!(
        frame.record(),
        &Record::OpenResult {
            flow_id,
            result: OpenResultCode::Opened
        }
    );
    retire_wait(&mut owner_b_queue, &mut owner_b, &mut wire);
    assert!(
        wire.try_recv_next(client_b.inbound_route())
            .unwrap()
            .is_none()
    );
    assert!(controller.snapshot().recovery_handoff_ready);
    assert_eq!(controller.snapshot().pending_recoveries, 1);
    assert!(!controller.snapshot().failed);
    assert_eq!(controller.cold_inventory().unwrap(), handoff_cold);
    // C still owns its exact handoff/Executor; A is also retained, not clean zero.
    assert!(handoff.acceptance_belongs_to_queue(&owner_queue));
}

#[test]
fn normal_quarantine_audits_pending_terminal_and_each_original_cause() {
    use crate::resumable::effect_storage::SessionEffectBatch;
    for kind in 0..3 {
        let exporter = 0xb0 + kind;
        let (io, reporter) = LegIo::for_authenticated_transport_with_reporter(
            LegId::A,
            LegEndpointRole::Owner,
            binding(exporter),
            LegIoLimits::new(4_096, 8, 32_768, 8, 32_768).unwrap(),
        );
        let request = standby_attach_request(2, 0x22);
        let proof = credentials().prove(&request, &binding(exporter)).unwrap();
        let mut queue = LegOutboundQueue::for_leg(io.established_leg(), 8, 32_768).unwrap();
        let bootstrap = SessionSupervisor::prepare_owner(capacity_plan(), standby_authority(1))
            .accept(
                io.established_leg(),
                io.established_leg()
                    .bind_received_frame(request.to_attach_frame(proof)),
                &mut queue,
            )
            .unwrap()
            .enqueue_acceptance(&mut queue)
            .unwrap();
        let (_executor, handoff) = OwnerTargetExecutor::from_initial_bootstrap(
            OwnerTargetConfig::new(1, 1_024, 64).unwrap(),
            bootstrap,
            MemoryTarget::new(MemoryTargetConfig::new(4, 256, 512, 128, 128, 16).unwrap()),
            FlowPortConfig::new(64, 8, 8, 8).unwrap(),
        )
        .unwrap();
        let generation = handoff.generation();
        let event = handoff.attached.resume_grace_expired_event();
        let loss = handoff
            .attached
            .bind_terminal(
                reporter
                    .report(LegTransportTerminalReason::PeerClosed)
                    .unwrap(),
            )
            .unwrap();
        // Defensive retained phases, not a valid terminalization/discard flow.
        // Move the same event/weak correlation from high-water into quarantine.
        let water = OwnerLeglessHighWater {
            owner: handoff.owner,
            generation,
            reason: loss.reason(),
            resume_grace_expired: event,
        };
        let water_audit = water.cold_inventory().unwrap();
        assert_eq!(water_audit.owner.high_waters, 1);
        assert_eq!(water_audit.payload.events, 1);
        assert_eq!(water_audit.payload.capability_values, 1);
        assert_eq!(water.cold_inventory(), Ok(water_audit));
        let pending = PendingOwnerAttachLoss {
            owner: water.owner,
            generation,
            reason: loss.reason(),
            resume_grace_expired: water.resume_grace_expired,
            loss,
            tail: TerminalOwnerAttachTail::HandoffReady,
            phase: OwnerRecoveryLossPhase::BarrierCleared,
        };
        let pending_audit = pending.cold_inventory().unwrap();
        assert_eq!(pending_audit.owner.pending_losses, 1);
        assert_eq!(pending_audit.owner.exact_terminals, 1);
        let source = match kind {
            0 => OwnerTerminalAttachDiscardQuarantineSource::Supervisor(
                SessionSupervisorError::PoisonedOwnershipInvariant,
            ),
            1 => {
                let mut effects = SessionEffectBatch::prepared(3).unwrap();
                effects.push(SessionEffect::SessionExpired).unwrap();
                effects.mark_mutation();
                OwnerTerminalAttachDiscardQuarantineSource::Storage {
                    source: SessionSupervisorError::PoisonedOwnershipInvariant,
                    effects,
                }
            }
            2 => {
                let mut effects = Vec::with_capacity(3);
                effects.push(SessionEffect::SessionExpired);
                OwnerTerminalAttachDiscardQuarantineSource::Effects {
                    generation,
                    effects,
                }
            }
            _ => unreachable!(),
        };
        let quarantine = Box::new(QuarantinedOwnerAttachLoss { pending, source });
        let identity = std::ptr::from_ref(quarantine.as_ref());
        let audit = quarantine.cold_inventory().unwrap();
        assert_eq!(audit.owner, pending_audit.owner);
        assert_eq!(audit.recovery, pending_audit.recovery);
        assert_eq!(audit.payload.events, 1);
        assert_eq!(audit.payload.effects, usize::from(kind > 0));
        assert_eq!(audit.payload.effect_batches, usize::from(kind == 1));
        assert_eq!(audit.payload.mutated_batches, usize::from(kind == 1));
        assert_eq!(audit.payload.effect_vectors, usize::from(kind == 2));
        let capacity = match &quarantine.source {
            OwnerTerminalAttachDiscardQuarantineSource::Supervisor(_) => 0,
            OwnerTerminalAttachDiscardQuarantineSource::Storage { effects, .. } => {
                effects.capacity()
            }
            OwnerTerminalAttachDiscardQuarantineSource::Effects { effects, .. } => {
                effects.capacity()
            }
        };
        assert_eq!(audit.payload.effect_capacity, capacity);
        assert_eq!(
            audit.payload.effect_array_bytes,
            std::alloc::Layout::array::<SessionEffect>(capacity)
                .unwrap()
                .size()
        );
        assert_eq!(quarantine.cold_inventory(), Ok(audit));
        assert_eq!(std::ptr::from_ref(quarantine.as_ref()), identity);
    }
}

#[test]
fn normal_cursor_audit_preserves_original_frame_and_backing() {
    let mut pending = std::collections::VecDeque::with_capacity(3);
    let frame = Frame::try_new(
        LegGeneration::new(2).unwrap(),
        Record::Data {
            flow_id: SessionFlowId::new(1).unwrap(),
            direction: Direction::TargetToClient,
            offset: ByteOffset::new(0),
            payload: bytes::Bytes::from_static(b"retained"),
        },
    )
    .unwrap();
    let encoded_len = frame.encode().unwrap().len();
    let Record::Data { payload, .. } = frame.record() else {
        unreachable!()
    };
    let backing = payload.as_ptr();
    pending.push_back(OwnerTargetOutput::Transmit(frame));
    // Defensive holder without correlation; never execute as a valid recovery.
    let cursor = OwnerTargetRecoveryDrain {
        owner: std::sync::Weak::new(),
        attached: None,
        acceptance: None,
        pending,
        ready: Vec::with_capacity(2),
    };
    let identity = std::ptr::from_ref(cursor.pending.front().unwrap());
    let audit = cursor.cold_inventory().unwrap();
    assert_eq!(audit.recovery.cursors, 1);
    assert_eq!(audit.recovery.attached_authorities, 0);
    assert_eq!(audit.recovery.acceptance_receipts, 0);
    assert_eq!(audit.recovery.frames, 1);
    assert_eq!(audit.recovery.frame_encoded_bytes, encoded_len);
    assert_eq!(audit.payload.logical_bytes, 8);
    assert_eq!(audit.recovery.pending_capacity, cursor.pending.capacity());
    assert_eq!(audit.recovery.ready_capacity, cursor.ready.capacity());
    for _ in 0..2 {
        assert_eq!(cursor.cold_inventory(), Ok(audit));
        assert_eq!(
            std::ptr::from_ref(cursor.pending.front().unwrap()),
            identity
        );
        assert_eq!(cursor.pending.len(), 1);
        assert_eq!(cursor.ready.len(), 0);
        let OwnerTargetOutput::Transmit(frame) = cursor.pending.front().unwrap() else {
            unreachable!()
        };
        let Record::Data { payload, .. } = frame.record() else {
            unreachable!()
        };
        assert_eq!(payload.as_ptr(), backing);
    }
}
