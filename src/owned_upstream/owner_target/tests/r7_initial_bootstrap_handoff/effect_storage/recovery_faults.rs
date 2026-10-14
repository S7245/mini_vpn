//! Real checked catch-up storage rejection and defensive release custody.
//! Uses actual status delivery/arm; no whole-root or natural double-failure claim.
use super::*;

#[test]
fn catch_up_storage_refusal_keeps_received_permit_and_exact_reservation_for_retry() {
    let (mut executor, handoff, _initial_owner, initial_queue) = checked_owner(0xa0, 128);
    let initial = committed(&handoff.attached);
    open_flow(
        &mut executor,
        initial,
        SessionFlowId::new(1).unwrap(),
        target(443),
    );
    executor
        .apply_event(SessionEvent::LegLost { leg: initial })
        .unwrap();
    let mut wire = wire();
    let mut status_owner = endpoint(LegId::B, LegEndpointRole::Owner, 0xa1);
    let mut status_client = endpoint(LegId::B, LegEndpointRole::Client, 0xa1);
    let mut status_queue =
        LegOutboundQueue::for_leg(status_owner.established_leg(), 8, 32_768).unwrap();
    let stale = registered_exact_request(2, 0x61);
    let proof = credentials().prove(&stale, &binding(0xa1)).unwrap();
    wire.controller_sender()
        .send_encoded(
            SimTime::ZERO,
            status_owner.inbound_route(),
            WireLane::Control,
            stale.to_attach_frame(proof).encode().unwrap().into(),
        )
        .unwrap();
    let BoundInbound::Session(received) = receive(&mut wire, &mut status_owner) else {
        panic!("actual stale ATTACH");
    };
    let pending_status = executor
        .resynchronize_owner_attach(status_owner.established_leg(), received)
        .unwrap();
    let status_before = executor.snapshot();
    let queue_before_status = status_queue.cold_inventory().unwrap();
    let rejected = executor
        .abandon_lost_pending_owner_attach_status(pending_status)
        .unwrap_err();
    let status_audit = rejected
        .cold_inventory()
        .expect("complete live status rejection audit");
    assert!(!status_audit.is_exact_zero());
    assert_eq!(
        (
            status_audit.owner.pending_status,
            status_audit.owner.catch_up_arms
        ),
        (1, 1)
    );
    assert_eq!(
        (
            status_audit.payload.session_frames,
            status_audit.owner.status_receipts
        ),
        (1, 0)
    );
    assert_eq!(rejected.cold_inventory(), Ok(status_audit));
    assert_eq!(executor.snapshot(), status_before);
    assert_eq!(status_queue.cold_inventory().unwrap(), queue_before_status);
    let OwnerTargetError::RejectedPendingOwnerAttachStatusAbandon { block, publication } = rejected
    else {
        panic!("actual live pending status must remain returned");
    };
    assert_eq!(block, OwnerTargetInputBlock::StatusPublicationStillLive);
    let status = publication.enqueue(&mut status_queue).unwrap();
    let BoundInbound::Session(delivered) = deliver(
        &mut status_queue,
        &mut status_owner,
        &mut status_client,
        &mut wire,
    ) else {
        panic!("actual generation status");
    };
    assert!(
        matches!(delivered.into_frame_for_test().record(), Record::AttachGenerationStatus { nonce, .. } if *nonce == stale.nonce())
    );
    retire_wait(&mut status_queue, &mut status_owner, &mut wire);
    let rejected = executor
        .arm_owner_catch_up_after_status_delivery(status, &initial_queue)
        .unwrap_err();
    let delivered_audit = rejected
        .cold_inventory()
        .expect("complete delivered status rejection audit");
    assert!(!delivered_audit.is_exact_zero());
    assert_eq!(
        (
            delivered_audit.owner.enqueued_status,
            delivered_audit.owner.catch_up_arms
        ),
        (1, 1)
    );
    assert_eq!(
        (
            delivered_audit.owner.status_receipts,
            delivered_audit.payload.session_frames
        ),
        (1, 0)
    );
    assert_eq!(rejected.cold_inventory(), Ok(delivered_audit));
    let OwnerTargetError::RejectedOwnerAttachStatusArm { block, publication } = rejected else {
        panic!("wrong queue must return original delivered status");
    };
    assert_eq!(block, OwnerTargetInputBlock::WrongStatusQueue);
    executor
        .arm_owner_catch_up_after_status_delivery(*publication, &status_queue)
        .unwrap();
    assert!(executor.snapshot().session.owner_catch_up_permit_armed);

    let mut owner = endpoint(LegId::A, LegEndpointRole::Owner, 0xa2);
    let mut client = endpoint(LegId::A, LegEndpointRole::Client, 0xa2);
    let mut queue = LegOutboundQueue::for_leg(owner.established_leg(), 8, 32_768).unwrap();
    let request = registered_exact_request(3, 0x62);
    let proof = credentials().prove(&request, &binding(0xa2)).unwrap();
    wire.controller_sender()
        .send_encoded(
            SimTime::ZERO,
            owner.inbound_route(),
            WireLane::Control,
            request.to_attach_frame(proof).encode().unwrap().into(),
        )
        .unwrap();
    let BoundInbound::Session(received) = receive(&mut wire, &mut owner) else {
        panic!("actual successor ATTACH");
    };
    let before = executor.snapshot();
    let queue_before = queue.cold_inventory().unwrap();
    crate::resumable::effect_storage::fail_next_effect_allocation_for_test();
    let error = executor
        .accept_catch_up_owner_attach(owner.established_leg(), received, &mut queue)
        .unwrap_err();
    let audit = error
        .cold_inventory()
        .expect("complete original sealed ATTACH audit");
    assert_eq!(audit.target_error_nodes, 1);
    assert_eq!(audit.payload.session_frames, 1);
    assert!(audit.payload.session_encoded_bytes > 0);
    assert_eq!(error.cold_inventory(), Ok(audit));
    let OwnerTargetError::Supervisor(
        crate::owned_upstream::supervisor::SessionSupervisorError::RecoveryEffectStorage {
            source,
            received,
        },
    ) = error
    else {
        panic!("original received frame must survive catch-up pre-CAS refusal");
    };
    assert_eq!(source, crate::resumable::SessionError::EffectStorageReserve);
    assert!(received.belongs_to_transport(owner.established_leg()));
    assert_eq!(executor.snapshot(), before);
    assert_eq!(queue.cold_inventory().unwrap(), queue_before);

    // Real primitive rejection plus manual composed-error characterization.
    // The serialized outer API cannot swap its exclusively borrowed queue.
    let reservation = queue
        .reserve_attach_acceptance(owner.established_leg())
        .unwrap();
    crate::resumable::effect_storage::fail_next_effect_allocation_for_test();
    let error = Box::new(
        executor
            .supervisor
            .accept_catch_up_owner_attach(owner.established_leg(), received)
            .err()
            .expect("same real catch-up Reserve refusal"),
    );
    let error_identity = std::ptr::from_ref(error.as_ref());
    let reserved_before = queue.cold_inventory().unwrap();
    let wrong_owner = endpoint(LegId::A, LegEndpointRole::Owner, 0xa3);
    let mut wrong_queue =
        LegOutboundQueue::for_leg(wrong_owner.established_leg(), 8, 32_768).unwrap();
    let wrong_before = wrong_queue.cold_inventory().unwrap();
    let reservation = Box::new(reservation.release(&mut wrong_queue).unwrap_err());
    let reservation_identity = std::ptr::from_ref(reservation.as_ref());
    let composed = OwnerTargetError::CatchUpOwnerAttachReleaseFailure(Box::new(
        crate::owned_upstream::owner_target::effect_storage::OwnerCatchUpReleaseFailure {
            error,
            reservation,
        },
    ));
    let composed_audit = composed
        .cold_inventory()
        .expect("complete original reservation audit");
    assert!(!composed_audit.is_exact_zero());
    assert_eq!(composed_audit.owner.acceptance_reservations, 1);
    assert_eq!(
        (
            composed_audit.owner.sealed_inbounds,
            composed_audit.payload.session_frames
        ),
        (1, 1)
    );
    assert_eq!(composed.cold_inventory(), Ok(composed_audit));
    let OwnerTargetError::CatchUpOwnerAttachReleaseFailure(failure) = composed else {
        unreachable!()
    };
    assert_eq!(std::ptr::from_ref(failure.error.as_ref()), error_identity);
    assert_eq!(
        std::ptr::from_ref(failure.reservation.as_ref()),
        reservation_identity
    );
    assert!(failure.reservation.belongs_to_queue(&queue));
    assert!(failure.reservation.belongs_to_leg(owner.established_leg()));
    assert_eq!(queue.cold_inventory().unwrap(), reserved_before);
    assert_eq!(wrong_queue.cold_inventory().unwrap(), wrong_before);
    let crate::owned_upstream::supervisor::SessionSupervisorError::RecoveryEffectStorage {
        source,
        received,
    } = *failure.error
    else {
        panic!("same error retains received through defensive release failure");
    };
    assert_eq!(source, crate::resumable::SessionError::EffectStorageReserve);
    failure.reservation.release(&mut queue).unwrap();
    assert_eq!(executor.snapshot(), before);
    assert_eq!(queue.cold_inventory().unwrap(), queue_before);
    let OwnerTargetAttachPublication::Installed { pending } = executor
        .accept_catch_up_owner_attach(owner.established_leg(), received, &mut queue)
        .unwrap()
    else {
        panic!("same original received commits its one catch-up permit");
    };
    let recovery = pending.enqueue_acceptance(&mut queue).unwrap();
    let backing = (
        recovery.recovery.effects.as_ptr(),
        recovery.recovery.effects.capacity(),
    );
    let effect_count = recovery.recovery.effects.len();
    let before_retry = executor.snapshot();
    let queue_before_retry = queue.cold_inventory().unwrap();
    let rejected = executor
        .execute_attached_recovery(recovery, &mut wrong_queue)
        .unwrap_err();
    let recovery_audit = rejected
        .cold_inventory()
        .expect("complete original unexecuted recovery audit");
    assert!(!recovery_audit.is_exact_zero());
    assert_eq!(recovery_audit.owner.unexecuted_recoveries, 1);
    assert_eq!(recovery_audit.recovery.attached_authorities, 1);
    assert_eq!(recovery_audit.recovery.acceptance_receipts, 1);
    assert_eq!(recovery_audit.payload.effect_vectors, 1);
    assert_eq!(recovery_audit.payload.effects, effect_count);
    assert_eq!(recovery_audit.payload.effect_capacity, backing.1);
    assert_eq!(
        recovery_audit.payload.effect_array_bytes,
        std::alloc::Layout::array::<SessionEffect>(backing.1)
            .unwrap()
            .size()
    );
    assert_eq!(rejected.cold_inventory(), Ok(recovery_audit));
    assert_eq!(executor.snapshot(), before_retry);
    assert_eq!(queue.cold_inventory().unwrap(), queue_before_retry);
    let OwnerTargetError::RejectedAttachedRecovery { block, recovery } = rejected else {
        panic!("wrong FIFO must return same unexecuted recovery");
    };
    assert_eq!(block, OwnerTargetInputBlock::WrongAcceptanceQueue);
    assert_eq!(
        (
            recovery.recovery.effects.as_ptr(),
            recovery.recovery.effects.capacity()
        ),
        backing
    );
    assert_eq!(recovery.recovery.effects.len(), effect_count);
    let outputs = executor
        .execute_attached_recovery(*recovery, &mut queue)
        .unwrap();
    assert!(
        outputs
            .iter()
            .any(|output| matches!(output, OwnerTargetRecoveryOutput::RecoveryCompleted { .. }))
    );
    let BoundInbound::Session(accepted) = deliver(&mut queue, &mut owner, &mut client, &mut wire)
    else {
        panic!("real acceptance");
    };
    assert!(
        matches!(accepted.into_frame_for_test().record(), Record::AttachAccepted { nonce, .. } if *nonce == request.nonce())
    );
    let BoundInbound::Session(replayed) = deliver(&mut queue, &mut owner, &mut client, &mut wire)
    else {
        panic!("real recovery OPEN result");
    };
    assert!(
        matches!(replayed.into_frame_for_test().record(), Record::OpenResult { flow_id, result: OpenResultCode::Opened } if flow_id.get() == 1)
    );
    retire_wait(&mut queue, &mut owner, &mut wire);
    assert_eq!(executor.snapshot().session.session.generation().get(), 3);
    assert!(!executor.snapshot().session.owner_catch_up_permit);
    assert_eq!(executor.target().snapshot().open_attempts, 1);
    assert!(handoff.acceptance_belongs_to_queue(&initial_queue));
}
