//! Checked original effect backing across real protocol, recovery and reuse.
//! Memory endpoints execute ordered delivery; no physical-root or WAN claim.
use super::*;
mod controller_inventory;
mod error_inventory;
mod faults;
mod recovery_faults;
use crate::owned_upstream::client_controller::ClientSessionController;
use crate::owned_upstream::leg_io::{MemoryFaultScript, MemoryLegTransport};
use crate::owned_upstream::standby::RegisteredAttachResponse;
use crate::owned_upstream::two_leg::{TwoLegWire, WireBounds, WireCapacitySpec};
use crate::resumable::CommittedLeg;

fn capacity_plan() -> OwnerSessionCapacityPlan {
    let rates = DirectionalRates::symmetric(BitsPerSecond::new(4_096).unwrap());
    let storage = ReplayCapacitySpec::new(rates, rates, Duration::from_millis(500), Duration::ZERO)
        .unwrap()
        .derive()
        .unwrap()
        .storage_plan(ReplayStorageGeometry::new(32, 4).unwrap())
        .unwrap();
    OwnerSessionCapacityPlan::derive(storage, 4, 64).unwrap()
}

fn wire() -> MemoryLegTransport {
    let capacity = WireCapacitySpec::new(80_000, Duration::from_secs(1), 256, 128, 1)
        .unwrap()
        .with_fault_copies(1, 1)
        .unwrap()
        .derive()
        .unwrap();
    MemoryLegTransport::new(
        TwoLegWire::new(WireBounds::new(capacity, 64, 32_000, 4, 4).unwrap()),
        MemoryFaultScript::new(0),
    )
}

fn endpoint(leg: LegId, role: LegEndpointRole, exporter: u8) -> LegIo {
    LegIo::for_authenticated_transport(
        leg,
        role,
        binding(exporter),
        LegIoLimits::new(4_096, 8, 32_768, 8, 32_768).unwrap(),
    )
}

fn receive(wire: &mut MemoryLegTransport, peer: &mut LegIo) -> BoundInbound {
    while wire.advance_one_due(SimTime::ZERO).unwrap() {}
    let delivery = wire.try_recv_next(peer.inbound_route()).unwrap().unwrap();
    peer.receive_classified_delivery(delivery, FeatureSet::new(STANDBY_FEATURES))
        .unwrap()
}

fn deliver(
    queue: &mut LegOutboundQueue,
    from: &mut LegIo,
    peer: &mut LegIo,
    wire: &mut MemoryLegTransport,
) -> BoundInbound {
    queue
        .try_flush(from, &mut wire.controller_sender(), SimTime::ZERO)
        .unwrap()
        .unwrap();
    receive(wire, peer)
}

fn retire_wait(queue: &mut LegOutboundQueue, io: &mut LegIo, wire: &mut MemoryLegTransport) {
    assert_eq!(queue.len(), 0);
    assert!(
        queue
            .try_flush(io, &mut wire.controller_sender(), SimTime::ZERO)
            .unwrap()
            .is_none()
    );
    assert!(!queue.cold_inventory().unwrap().ordered_delivery_wait());
}

fn committed(attached: &AttachedLeg) -> CommittedLeg {
    let SessionEvent::ReplacementAttached { leg } = attached.replacement_attached_event() else {
        panic!("exact attached authority supplies its committed leg");
    };
    leg
}

// Component constructor fixtures use the production checked plan. Incoming
// ATTACH is locally bound here; only the full fixture below proves wire delivery.
fn checked_bootstrap(exporter: u8) -> (OwnerSessionBootstrap, LegIo, LegOutboundQueue) {
    let owner = endpoint(LegId::A, LegEndpointRole::Owner, exporter);
    let request = standby_attach_request(2, 0x22);
    let proof = credentials().prove(&request, &binding(exporter)).unwrap();
    let mut queue = LegOutboundQueue::for_leg(owner.established_leg(), 8, 32_768).unwrap();
    let bootstrap = SessionSupervisor::prepare_owner(capacity_plan(), standby_authority(1))
        .accept(
            owner.established_leg(),
            owner
                .established_leg()
                .bind_received_frame(request.to_attach_frame(proof)),
            &mut queue,
        )
        .unwrap()
        .enqueue_acceptance(&mut queue)
        .unwrap();
    (bootstrap, owner, queue)
}

fn checked_owner(
    exporter: u8,
    steps: usize,
) -> (
    OwnerTargetExecutor<MemoryTarget>,
    OwnerTargetInitialHandoff,
    LegIo,
    LegOutboundQueue,
) {
    let (bootstrap, owner, queue) = checked_bootstrap(exporter);
    let (executor, handoff) = OwnerTargetExecutor::from_initial_bootstrap(
        OwnerTargetConfig::new(steps, 1_024, 64).unwrap(),
        bootstrap,
        MemoryTarget::new(MemoryTargetConfig::new(4, 256, 512, 128, 128, 16).unwrap()),
        FlowPortConfig::new(64, 8, 8, 8).unwrap(),
    )
    .unwrap();
    (executor, handoff, owner, queue)
}

#[test]
fn checked_owner_effect_storage_survives_registered_recovery_and_reuse() {
    let plan = capacity_plan();
    let mut wire = wire();
    let mut client_a = endpoint(LegId::A, LegEndpointRole::Client, 0x91);
    let mut owner_a = endpoint(LegId::A, LegEndpointRole::Owner, 0x91);
    let mut client_queue =
        LegOutboundQueue::for_leg(client_a.established_leg(), 8, 32_768).unwrap();
    let mut owner_queue = LegOutboundQueue::for_leg(owner_a.established_leg(), 8, 32_768).unwrap();
    let request = standby_attach_request(2, 0x22);
    let proof = credentials().prove(&request, &binding(0x91)).unwrap();
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
        panic!("initial ATTACH remains a session frame");
    };
    let bootstrap = SessionSupervisor::prepare_owner(plan, standby_authority(1))
        .accept(owner_a.established_leg(), received, &mut owner_queue)
        .unwrap()
        .enqueue_acceptance(&mut owner_queue)
        .unwrap();
    let BoundInbound::Session(accepted) =
        deliver(&mut owner_queue, &mut owner_a, &mut client_a, &mut wire)
    else {
        panic!("initial acceptance remains a session frame");
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
    let config = OwnerTargetConfig::new(
        1,
        plan.session_config().owner_pending_work_bound().unwrap(),
        64,
    )
    .unwrap();
    let (mut executor, handoff) = OwnerTargetExecutor::from_initial_bootstrap(
        config,
        bootstrap,
        MemoryTarget::new(MemoryTargetConfig::new(4, 256, 512, 128, 128, 16).unwrap()),
        FlowPortConfig::new(64, 8, 8, 8).unwrap(),
    )
    .unwrap();
    let cold_work_capacity = executor.pending_effects.capacity();
    let initial = committed(&handoff.attached);
    for raw in 1..=3 {
        let flow_id = SessionFlowId::new(raw).unwrap();
        let frame = Frame::try_new(
            initial.generation(),
            Record::Open {
                flow_id,
                target: target(440 + raw as u16),
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
            panic!("real OPEN stays on the session lane");
        };
        let outputs = executor
            .apply_event(handoff.attached.accept_frame(received).unwrap())
            .unwrap();
        let outputs = drain_resumes(&mut executor, outputs);
        assert_eq!(
            transmitted(&outputs),
            vec![&Record::OpenResult {
                flow_id,
                result: OpenResultCode::Opened
            }]
        );
    }
    assert_eq!(executor.target().snapshot().open_attempts, 3);

    let mut client_b = endpoint(LegId::B, LegEndpointRole::Client, 0x92);
    let mut owner_b = endpoint(LegId::B, LegEndpointRole::Owner, 0x92);
    let mut client_b_queue =
        LegOutboundQueue::for_leg(client_b.established_leg(), 8, 32_768).unwrap();
    let mut owner_b_queue =
        LegOutboundQueue::for_leg(owner_b.established_leg(), 3, 32_768).unwrap();
    let awaiting = client
        .begin_current_standby_registration(
            client_b.established_leg(),
            StandbyNonce::new([0x33; 16]).unwrap(),
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

    // Inspect at the existing real producer/consumer boundary; no fabricated effects.
    let (batch, facts) = executor
        .reduce_owner_command_observed(SessionOwnerCommand::Event(SessionEvent::LegLost {
            leg: initial,
        }))
        .unwrap();
    let ordinary_before = (batch.as_ptr(), batch.capacity());
    executor.execute_effects_observed(batch, facts).unwrap();
    let awaiting = registered
        .begin_exact_next_attach(AttachNonce::new([0x44; 16]).unwrap(), &credentials())
        .unwrap()
        .enqueue(&mut client_b_queue)
        .unwrap();
    let BoundInbound::Session(received) =
        deliver(&mut client_b_queue, &mut client_b, &mut owner_b, &mut wire)
    else {
        panic!("registered ATTACH stays on the session lane");
    };
    let before_rejected = executor.snapshot();
    let queue_before_rejected = owner_b_queue.cold_inventory().unwrap();
    // Defensive composed-error characterization. A legal outer call keeps
    // one queue borrowed throughout, so cannot substitute this wrong queue.
    let gate = slot
        .begin_exact_next_attach(owner_b.established_leg(), &received, &mut owner_b_queue)
        .unwrap();
    crate::resumable::effect_storage::fail_next_effect_allocation_for_test();
    let error = Box::new(
        executor
            .supervisor
            .accept_registered_owner_attach(&gate, owner_b.established_leg(), received)
            .err()
            .expect("real pre-CAS storage refusal"),
    );
    let error_identity = std::ptr::from_ref(error.as_ref());
    let wrong_owner = endpoint(LegId::A, LegEndpointRole::Owner, 0x9b);
    let mut wrong_queue =
        LegOutboundQueue::for_leg(wrong_owner.established_leg(), 8, 32_768).unwrap();
    let reserved_queue = owner_b_queue.cold_inventory().unwrap();
    let wrong_before = wrong_queue.cold_inventory().unwrap();
    let source = gate.restore(&mut slot, &mut wrong_queue).unwrap_err();
    let composed = OwnerTargetError::RegisteredOwnerAttachRestoreFailure(Box::new(
        crate::owned_upstream::owner_target::effect_storage::OwnerRegisteredRestoreFailure {
            source,
            error,
        },
    ));
    let OwnerTargetError::RegisteredOwnerAttachRestoreFailure(failure) = composed else {
        unreachable!()
    };
    assert_eq!(
        failure.source,
        crate::owned_upstream::standby::OwnerRegisteredAttachRestoreError::QueueReservation
    );
    assert_eq!(std::ptr::from_ref(failure.error.as_ref()), error_identity);
    assert!(slot.has_retained_attach_restore());
    assert_eq!(owner_b_queue.cold_inventory().unwrap(), reserved_queue);
    assert_eq!(wrong_queue.cold_inventory().unwrap(), wrong_before);
    let crate::owned_upstream::supervisor::SessionSupervisorError::RecoveryEffectStorage {
        source,
        received,
    } = *failure.error
    else {
        panic!("composed failure keeps the original received ATTACH");
    };
    assert_eq!(source, crate::resumable::SessionError::EffectStorageReserve);
    assert!(received.belongs_to_transport(owner_b.established_leg()));
    slot.retry_retained_attach_restore(&mut owner_b_queue)
        .unwrap();
    assert!(!slot.has_retained_attach_restore());
    assert_eq!(
        owner_b_queue.cold_inventory().unwrap(),
        queue_before_rejected
    );
    assert_eq!(executor.snapshot(), before_rejected);
    crate::resumable::effect_storage::fail_next_effect_allocation_for_test();
    let rejected = executor
        .accept_registered_owner_attach(
            &mut slot,
            owner_b.established_leg(),
            received,
            &mut owner_b_queue,
        )
        .unwrap_err();
    let OwnerTargetError::Supervisor(
        crate::owned_upstream::supervisor::SessionSupervisorError::RecoveryEffectStorage {
            source,
            received,
        },
    ) = rejected
    else {
        panic!("return the original received ATTACH on new storage refusal");
    };
    assert_eq!(source, crate::resumable::SessionError::EffectStorageReserve);
    assert!(received.belongs_to_transport(owner_b.established_leg()));
    assert_eq!(executor.snapshot(), before_rejected);
    assert_eq!(
        owner_b_queue.cold_inventory().unwrap(),
        queue_before_rejected
    );
    let OwnerTargetAttachPublication::Installed { pending } = executor
        .accept_registered_owner_attach(
            &mut slot,
            owner_b.established_leg(),
            received,
            &mut owner_b_queue,
        )
        .unwrap()
    else {
        panic!("the real registered next generation installs");
    };
    let recovery = pending.enqueue_acceptance(&mut owner_b_queue).unwrap();
    let mut page = executor
        .execute_attached_recovery_observed(recovery, &owner_b_queue)
        .unwrap();
    let grown_work_capacity = executor.pending_effects.capacity();
    let mut page_capacities = Vec::new();
    let mut early_acceptance = None;
    let recovered = loop {
        page_capacities.push(page.publish.pending.capacity());
        let (cursor, facts) = page.into_progress_parts();
        assert!(facts.is_empty());
        let outputs = match executor.publish_attached_recovery_page(cursor, &mut owner_b_queue) {
            Ok(outputs) => outputs,
            Err(error) => {
                assert!(matches!(
                    &error,
                    OwnerTargetError::AttachedRecoveryQueueBlocked { .. }
                ));
                assert!(
                    early_acceptance.is_none(),
                    "exactly one real FIFO pressure return"
                );
                assert_eq!(page_capacities.len(), 4);
                assert_eq!(owner_b_queue.len(), 3);
                let audit = error.cold_inventory();
                assert!(
                    audit.is_ok(),
                    "original recovery cursor must be audited: {audit:?}"
                );
                let audit = audit.unwrap();
                assert_eq!((audit.target_error_nodes, audit.observation_facts), (1, 0));
                assert_eq!(error.cold_inventory(), Ok(audit));
                assert!(!audit.is_exact_zero());
                let payload = audit.recovery;
                assert_eq!(
                    (payload.cursors, payload.output_values, payload.frames),
                    (1, 1, 1)
                );
                assert_eq!(
                    (
                        payload.resumes,
                        payload.attached_authorities,
                        payload.acceptance_receipts
                    ),
                    (0, 1, 1)
                );
                assert_eq!((payload.pending_capacity, payload.ready_capacity), (1, 0));
                assert_eq!(
                    payload.container_bytes,
                    std::mem::size_of::<OwnerTargetOutput>()
                );
                let original = Frame::try_new(
                    LegGeneration::new(3).unwrap(),
                    Record::OpenResult {
                        flow_id: SessionFlowId::new(3).unwrap(),
                        result: OpenResultCode::Opened,
                    },
                )
                .unwrap();
                assert_eq!(
                    payload.frame_encoded_bytes,
                    original.encoded_len_checked().unwrap()
                );
                let OwnerTargetError::AttachedRecoveryQueueBlocked { drain, .. } = error else {
                    unreachable!()
                };
                early_acceptance = Some(deliver(
                    &mut owner_b_queue,
                    &mut owner_b,
                    &mut client_b,
                    &mut wire,
                ));
                executor
                    .publish_attached_recovery_page(*drain, &mut owner_b_queue)
                    .unwrap()
            }
        };
        let mut next = None;
        let mut complete = None;
        for output in outputs {
            match output {
                OwnerTargetRecoveryOutput::NeedsResume { resume } => next = Some(resume),
                OwnerTargetRecoveryOutput::RecoveryCompleted { handoff } => {
                    complete = Some(handoff)
                }
                OwnerTargetRecoveryOutput::LegActivated { generation } => {
                    assert_eq!(generation.get(), 3)
                }
                _ => panic!("unexpected recovery output"),
            }
        }
        if let Some(handoff) = complete {
            break handoff;
        }
        assert!(page_capacities.len() < 5);
        page = executor
            .resume_attached_recovery_effects_observed(next.unwrap(), &owner_b_queue)
            .unwrap();
    };
    let inbound = early_acceptance.expect("original acceptance was received at real pressure");
    let RegisteredAttachResponse::Accepted(_accepted) =
        awaiting.validate_response(inbound).unwrap()
    else {
        panic!("the peer validates the exact registered acceptance");
    };
    for raw in 1..=3 {
        let BoundInbound::Session(frame) =
            deliver(&mut owner_b_queue, &mut owner_b, &mut client_b, &mut wire)
        else {
            panic!("recovery emits original session records");
        };
        assert_eq!(
            frame.into_frame_for_test().record(),
            &Record::OpenResult {
                flow_id: SessionFlowId::new(raw).unwrap(),
                result: OpenResultCode::Opened,
            }
        );
    }
    retire_wait(&mut owner_b_queue, &mut owner_b, &mut wire);
    retire_wait(&mut client_b_queue, &mut client_b, &mut wire);
    assert_eq!(executor.target().snapshot().open_attempts, 3);
    assert_eq!(page_capacities.len(), 4);
    assert_eq!(executor.snapshot().aborted_effects, 0);
    assert_eq!(handoff.retained_acceptance_receipts(), 1);
    assert!(handoff.acceptance_belongs_to_queue(&owner_queue));

    let (batch, facts) = executor
        .reduce_owner_command_observed(SessionOwnerCommand::Event(SessionEvent::LegLost {
            leg: committed(&recovered.attached),
        }))
        .unwrap();
    let ordinary_after = (batch.as_ptr(), batch.capacity());
    executor.execute_effects_observed(batch, facts).unwrap();
    let retained_work_capacity = executor.pending_effects.capacity();
    // All protocol/recovery work above runs before the intended storage RED.
    assert_eq!(
        cold_work_capacity, 3,
        "cold original work backing is small, not dense K"
    );
    assert_eq!(
        grown_work_capacity, 6,
        "four recovery items choose the next 3*2 level"
    );
    assert_eq!(
        retained_work_capacity, 6,
        "drain/reuse keeps the same work allocation"
    );
    assert_eq!(ordinary_before.1, 3);
    assert_eq!(
        ordinary_after, ordinary_before,
        "ordinary source backing returns unchanged, not replaced by recovery"
    );
    assert_eq!(
        page_capacities,
        [2, 2, 2, 1],
        "raw pages hold E=1 plus resume only when needed"
    );
}
