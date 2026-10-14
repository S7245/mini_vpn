//! Physical-attach permit, replacement and shutdown characterizations.
//!
//! Owns the nine original lifecycle scenarios. Real fixtures and exact cleanup
//! stay in the parent and its existing cleanup child; this module adds no
//! production ownership or policy behavior. The unchanged tests are its entries.

use super::*;

#[tokio::test(flavor = "current_thread")]
async fn r7_physical_acceptance_delivery_waits_for_local_commit() {
    let now = SimTime::from_nanos(35);
    let mut fixture = registered_fixture().await;
    let mut arbiter = admit_soft_physical_attempt(&mut fixture, 0xa7);
    let owner_active_b = advance_physical_to_accepted(&mut fixture, now).await;

    assert_owner_recovery_waits_for_physical_commit(&mut fixture, now).await;
    let committed = fixture
        .composition
        .commit_owned_standby_physical_replacement(&mut arbiter)
        .expect("one synchronous transaction installs B before accepting delivery");
    assert_eq!(committed.generation(), owner_active_b.generation());
    assert!(!committed.is_post_loss());
    assert!(
        fixture
            .composition
            .physical_attach_inventory()
            .expect("a committed physical replacement leaves a readable ledger")
            .is_empty()
    );
    assert!(arbiter.inventory().is_live_exact_zero());

    let recovery = drive_owner_recovery_through_committed_current(&mut fixture, now).await;
    assert_eq!(fixture.local_flows.len(), 1);
    assert!(recovery.leg_activated);
    assert!(recovery.admission_completed);
    assert_eq!(recovery.admitted_frames, 0);
    assert_eq!(recovery.applied_records, 1);
    assert!(recovery.applied_effects.is_empty());
    assert_eq!(
        fixture
            .composition
            .try_flush_current(now)
            .expect("fully acknowledged recovery leaves no client replay"),
        crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition::Idle,
    );
    assert!(matches!(
        fixture.composition.settle_completed_recovery_tail(),
        Ok(ClientRecoveryTailSettlement::Settled { .. }
            | ClientRecoveryTailSettlement::AlreadySettled)
    ));

    drop(owner_active_b);
    cleanup_registered_fixture("physical-commit-order", fixture).await;
}

#[tokio::test(flavor = "current_thread")]
async fn r7_physical_commit_wrong_arbiter_restores_exact_held_acceptance_for_retry() {
    let now = SimTime::from_nanos(36);
    let tag = 0xa8;
    let mut fixture = registered_fixture().await;
    let session_id = SessionId::new(PHYSICAL_SESSION).expect("nonzero session");
    let generation = fixture.composition.snapshot().session.generation();
    let standby_nonce = StandbyNonce::new(PHYSICAL_STANDBY_NONCE).expect("nonzero standby nonce");
    let attach_nonce = AttachNonce::new([tag; 16]).expect("nonzero attach nonce");
    let switch_epoch = SwitchEpoch::new(u64::from(tag)).expect("nonzero switch epoch");
    let mut exact = admit_soft_physical_attempt(&mut fixture, tag);
    let mut scalar_equal = ClientSwitchArbiter::new(
        session_id,
        generation,
        standby_nonce,
        attach_nonce,
        switch_epoch,
    )
    .expect("scalar-equal arbiter");
    let scalar_equal_permit = soft_permit(&mut scalar_equal, session_id, generation);
    let owner_active_b = advance_physical_to_accepted(&mut fixture, now).await;

    assert_eq!(
        fixture
            .composition
            .physical_attach_inventory_with_slot_omitted_for_test(),
        Err(QuinnPhysicalAttachAuditError::AcceptedMarkerWithoutAuthority),
        "an orphan accepted marker must fail closed instead of reporting exact-zero",
    );
    let physical_before = fixture
        .composition
        .physical_attach_inventory()
        .expect("accepted physical ownership has one checked ledger");
    let held_before = physical_before
        .accepted_pending_commit()
        .expect("the physical slot retains one held acceptance");
    assert_eq!(
        held_before.accepted_generation(),
        owner_active_b.generation()
    );
    assert_eq!(held_before.marker_generation(), owner_active_b.generation());
    assert_eq!(held_before.held_records(), 1);
    assert!(held_before.held_encoded_bytes() > 0);
    assert_eq!(held_before.queued_inbound_markers(), 1);
    assert_eq!(held_before.composition_message_permits(), 1);
    assert_eq!(held_before.reserved_delivery_acks(), 1);
    assert_eq!(held_before.reserved_delivery_ack_bytes(), 18);

    let standby_before = fixture
        .composition
        .registered_standby_actor_snapshot_for_test()
        .expect("physical B remains a running standby actor before commit");
    assert_eq!(standby_before.queued_inbound_messages(), 1);
    assert_eq!(
        standby_before.inbound_owned_bytes(),
        held_before.held_encoded_bytes()
    );
    assert_eq!(standby_before.composition_owned_messages(), 1);
    assert_eq!(standby_before.reserved_delivery_acks(), 1);
    assert_eq!(standby_before.ack_owned_bytes(), 18);
    let composition_before = fixture.composition.snapshot();
    let current_before = fixture.composition.current_actor_snapshot();
    let retired_before = fixture.composition.retired_actor_snapshot();
    let exact_before = exact.snapshot();
    let scalar_equal_before = scalar_equal.snapshot();

    assert_eq!(
        fixture
            .composition
            .commit_owned_standby_physical_replacement(&mut scalar_equal),
        Err(QuinnPhysicalReplacementCommitError::Arbiter(
            SwitchArbiterError::WrongAttachAcceptance,
        )),
    );
    assert_eq!(fixture.composition.snapshot(), composition_before);
    assert_eq!(fixture.composition.current_actor_snapshot(), current_before);
    assert_eq!(fixture.composition.retired_actor_snapshot(), retired_before);
    assert_eq!(exact.snapshot(), exact_before);
    assert_eq!(scalar_equal.snapshot(), scalar_equal_before);
    assert_eq!(
        fixture
            .composition
            .registered_standby_actor_snapshot_for_test()
            .expect("rejected commit restores the exact B actor"),
        standby_before,
    );
    assert_eq!(
        fixture
            .composition
            .physical_attach_inventory()
            .expect("rejected commit restores the exact held ledger"),
        physical_before,
    );
    assert!(matches!(
        fixture.composition.try_receive_registered_standby(),
        QuinnStandbyInboundDrive::AttachAcceptedPendingCommit { generation }
            if generation == owner_active_b.generation()
    ));
    assert_owner_recovery_waits_for_physical_commit(&mut fixture, now).await;

    let committed = fixture
        .composition
        .commit_owned_standby_physical_replacement(&mut exact)
        .expect("the same restored held acceptance remains commit-capable");
    assert_eq!(committed.generation(), owner_active_b.generation());
    let recovery = drive_owner_recovery_through_committed_current(&mut fixture, now).await;
    assert!(recovery.leg_activated);
    assert!(recovery.admission_completed);
    assert_eq!(recovery.admitted_frames, 0);
    assert_eq!(recovery.applied_records, 1);
    assert!(recovery.applied_effects.is_empty());
    assert!(matches!(
        fixture.composition.settle_completed_recovery_tail(),
        Ok(ClientRecoveryTailSettlement::Settled { .. }
            | ClientRecoveryTailSettlement::AlreadySettled)
    ));

    scalar_equal.close_switch_admission_for_shutdown();
    scalar_equal
        .settle_shutdown_pending(
            scalar_equal_permit.into_shutdown_settlement(PhysicalPendingAttachStage::Unbegun),
        )
        .expect("the rejected scalar-equal permit settles independently");
    assert!(scalar_equal.inventory().is_live_exact_zero());
    drop(owner_active_b);
    cleanup_registered_fixture("physical-commit-rollback", fixture).await;
}

#[tokio::test(flavor = "current_thread")]
async fn r7_physical_replacement_rejection_restores_exact_held_acceptance_for_retry() {
    let now = SimTime::from_nanos(37);
    let mut fixture = registered_fixture().await;
    let mut arbiter = admit_soft_physical_attempt(&mut fixture, 0xa9);
    let owner_active_b = advance_physical_to_accepted(&mut fixture, now).await;

    let delayed_a_delivered = Arc::clone(&fixture.server_a.delivery_writer_gate);
    delayed_a_delivered.arm();
    let queued_a = fixture
        .composition
        .begin_current_probe()
        .expect("current A queues one exact ordered probe");
    let probe_generation = queued_a.generation();
    let probe_sequence = queued_a.probe_sequence();
    assert_eq!(
        fixture
            .composition
            .try_flush_current(now)
            .expect("current A probe flush remains live"),
        crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition::Submitted,
    );
    let inbound = tokio::time::timeout(
        TEST_TIMEOUT,
        fixture.server_a.recv_classified(STANDBY_CONTROL_V1),
    )
    .await
    .expect("current A probe reaches its peer")
    .expect("current A peer remains live")
    .expect("current A publishes one probe");
    let responder =
        ExactLegProbeResponder::from_installed(fixture.owner_active_a.installed_leg_probe_source());
    let pending_ack = match responder.classify(take_actor_inbound(inbound)) {
        ExactLegProbeResponse::Pending(pending) => pending,
        ExactLegProbeResponse::Unhandled(_) => {
            panic!("owner A rejected its exact classified probe")
        }
    };
    tokio::time::timeout(TEST_TIMEOUT, delayed_a_delivered.wait_until_blocked())
        .await
        .expect("A DELIVERY is withheld after physical submission");

    let physical_before = fixture
        .composition
        .physical_attach_inventory()
        .expect("accepted physical ownership has one checked ledger");
    let standby_before = fixture
        .composition
        .registered_standby_actor_snapshot_for_test()
        .expect("physical B remains a running standby actor before commit");
    let composition_before = fixture.composition.snapshot();
    let current_before = fixture.composition.current_actor_snapshot();
    let retired_before = fixture.composition.retired_actor_snapshot();
    let arbiter_before = arbiter.snapshot();

    assert_eq!(
        fixture
            .composition
            .commit_owned_standby_physical_replacement(&mut arbiter),
        Err(QuinnPhysicalReplacementCommitError::Replacement {
            kind: crate::owned_upstream::quinn_leg_composition::QuinnRegisteredReplacementErrorKind::Controller(
                crate::owned_upstream::client_controller::ClientRecoveryBeginErrorKind::CurrentProbeSettlementPending,
            ),
            action: crate::owned_upstream::quinn_leg_composition::QuinnOwnedStandbyInstallAction::Retryable,
        }),
    );
    assert_eq!(fixture.composition.snapshot(), composition_before);
    assert_eq!(fixture.composition.current_actor_snapshot(), current_before);
    assert_eq!(fixture.composition.retired_actor_snapshot(), retired_before);
    assert_eq!(arbiter.snapshot(), arbiter_before);
    assert_eq!(
        fixture
            .composition
            .registered_standby_actor_snapshot_for_test()
            .expect("replacement rejection restores the exact B actor"),
        standby_before,
    );
    assert_eq!(
        fixture
            .composition
            .physical_attach_inventory()
            .expect("replacement rejection restores the exact held ledger"),
        physical_before,
    );
    assert!(matches!(
        fixture.composition.try_receive_registered_standby(),
        QuinnStandbyInboundDrive::AttachAcceptedPendingCommit { generation }
            if generation == owner_active_b.generation()
    ));
    assert_owner_recovery_waits_for_physical_commit(&mut fixture, now).await;

    delayed_a_delivered.release();
    tokio::time::timeout(TEST_TIMEOUT, async {
        while fixture
            .composition
            .current_actor_snapshot()
            .pending_delivery_count()
            != 0
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("late A DELIVERY clears only actor-private ownership");
    let probe_arm_at = fixture
        .composition
        .session_clock()
        .now()
        .expect("current A session clock remains representable");
    let deadline = fixture
        .composition
        .arm_current_probe(queued_a, probe_arm_at, Duration::from_millis(50))
        .expect("the exact queued scheduler owner remains valid after rejection");
    pending_ack
        .enqueue(
            fixture
                .server_a
                .queue
                .as_mut()
                .expect("owner A retains its exact outbound FIFO"),
        )
        .expect("owner A queues the exact contract-derived ACK");
    assert_eq!(
        fixture
            .server_a
            .try_flush(now)
            .expect("owner A ACK flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    let acked = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match fixture.composition.try_receive_current() {
                QuinnCurrentInboundDrive::Idle => tokio::task::yield_now().await,
                outcome => break outcome,
            }
        }
    })
    .await
    .expect("exact A ACK reaches the controller");
    assert!(matches!(
        acked,
        QuinnCurrentInboundDrive::ProbeAcked {
            generation,
            probe_sequence: sequence,
        } if generation == probe_generation && sequence == probe_sequence
    ));
    wait_actor_ordered_delivery(&mut fixture.server_a).await;
    fixture
        .composition
        .reconcile_current_probe_ack(
            QuinnCurrentProbeSchedulerAuthority::Armed(deadline),
            probe_generation,
            probe_sequence,
        )
        .expect("root returns the pointer-identical armed scheduler owner");
    assert_eq!(
        fixture
            .composition
            .try_flush_current(now)
            .expect("A observes its already-delivered probe receipt"),
        crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition::Idle,
    );

    let committed = fixture
        .composition
        .commit_owned_standby_physical_replacement(&mut arbiter)
        .expect("the same restored held acceptance commits after A settles");
    assert_eq!(committed.generation(), owner_active_b.generation());
    let recovery = drive_owner_recovery_through_committed_current(&mut fixture, now).await;
    assert!(recovery.leg_activated);
    assert!(recovery.admission_completed);
    assert_eq!(recovery.admitted_frames, 0);
    assert_eq!(recovery.applied_records, 1);
    assert!(recovery.applied_effects.is_empty());
    assert!(matches!(
        fixture.composition.settle_completed_recovery_tail(),
        Ok(ClientRecoveryTailSettlement::Settled { .. }
            | ClientRecoveryTailSettlement::AlreadySettled)
    ));

    drop(owner_active_b);
    cleanup_registered_fixture("physical-replacement-rejection", fixture).await;
}

#[tokio::test(flavor = "current_thread")]
async fn r7_physical_attach_rejects_scalar_equal_permit_and_bounds_the_slot() {
    let mut fixture = registered_fixture().await;
    let session_id = SessionId::new(PHYSICAL_SESSION).expect("nonzero session");
    let generation = fixture.composition.snapshot().session.generation();
    let standby_nonce = StandbyNonce::new(PHYSICAL_STANDBY_NONCE).expect("nonzero standby nonce");
    let attach_nonce = AttachNonce::new([0xa1; 16]).expect("nonzero attach nonce");
    let epoch = SwitchEpoch::new(1).expect("nonzero switch epoch");
    let mut exact =
        ClientSwitchArbiter::new(session_id, generation, standby_nonce, attach_nonce, epoch)
            .expect("exact arbiter");
    let mut scalar_equal =
        ClientSwitchArbiter::new(session_id, generation, standby_nonce, attach_nonce, epoch)
            .expect("scalar-equal arbiter");
    let exact_permit = soft_permit(&mut exact, session_id, generation);
    let wrong_permit = soft_permit(&mut scalar_equal, session_id, generation);
    let before = fixture.composition.snapshot();

    let failure = fixture
        .composition
        .admit_pending_physical_attach(&exact, wrong_permit)
        .expect_err("another arbiter's scalar-equal permit is rejected");
    assert_eq!(
        failure.kind(),
        QuinnPhysicalAttachAdmissionErrorKind::Arbiter(
            SwitchArbiterError::WrongPhysicalAttachPermit,
        ),
    );
    let wrong_permit = failure.into_permit();
    assert_eq!(fixture.composition.snapshot(), before);
    assert!(
        fixture
            .composition
            .physical_attach_inventory()
            .expect("the empty physical ledger is auditable")
            .is_empty()
    );

    fixture
        .composition
        .admit_pending_physical_attach(&exact, exact_permit)
        .expect("the exact permit enters the sole physical slot");
    let inventory = fixture
        .composition
        .physical_attach_inventory()
        .expect("the admitted permit has an exact zero-frame ledger");
    assert_eq!(inventory.live_authorities(), 1);
    assert_eq!(
        inventory.authority_kind(),
        Some(QuinnPhysicalAttachAuthorityKind::Pending),
    );

    let occupied = fixture
        .composition
        .admit_pending_physical_attach(&scalar_equal, wrong_permit)
        .expect_err("the capacity-one slot cannot admit a second attempt");
    assert_eq!(
        occupied.kind(),
        QuinnPhysicalAttachAdmissionErrorKind::SlotOccupied,
    );
    let wrong_permit = occupied.into_permit();
    assert_eq!(
        fixture
            .composition
            .physical_attach_inventory()
            .expect("slot rejection cannot corrupt the retained ledger"),
        inventory,
    );

    settle_physical_and_assert_exact_zero(
        &mut fixture,
        &mut exact,
        PhysicalPendingAttachStage::Unbegun,
    );
    scalar_equal.close_switch_admission_for_shutdown();
    scalar_equal
        .settle_shutdown_pending(
            wrong_permit.into_shutdown_settlement(PhysicalPendingAttachStage::Unbegun),
        )
        .expect("the scalar-equal arbiter settles its own rejected permit");
    assert!(scalar_equal.inventory().is_live_exact_zero());
    cleanup_registered_fixture("scalar-equal-slot", fixture).await;
}

#[tokio::test(flavor = "current_thread")]
async fn r7_soft_attach_pressure_retains_one_permit_nonce_and_frame() {
    let mut fixture = registered_fixture().await;
    let session_id = SessionId::new(PHYSICAL_SESSION).expect("nonzero session");
    let generation = fixture.composition.snapshot().session.generation();
    let mut arbiter = ClientSwitchArbiter::new(
        session_id,
        generation,
        StandbyNonce::new(PHYSICAL_STANDBY_NONCE).expect("nonzero standby nonce"),
        AttachNonce::new([0xa2; 16]).expect("nonzero attach nonce"),
        SwitchEpoch::new(2).expect("nonzero switch epoch"),
    )
    .expect("one arbiter");
    let permit = soft_permit(&mut arbiter, session_id, generation);
    fixture
        .composition
        .admit_pending_physical_attach(&arbiter, permit)
        .expect("the exact permit enters the sole physical slot");

    for sequence in 1..=4 {
        fixture
            .composition
            .enqueue_registered_standby_control_for_test(ordered_control_for_generation(
                generation,
                8_000 + sequence,
            ))
            .expect("the real B FIFO is filled to its checked message capacity");
    }
    assert_eq!(
        fixture
            .composition
            .drive_owned_standby_physical_attach(
                SimTime::from_nanos(30),
                &actor_attach_credentials()
            )
            .expect("bounded queue pressure is retryable"),
        crate::owned_upstream::quinn_leg_composition::QuinnStandbyAttachDrive::Pressure,
    );
    let pressured = fixture
        .composition
        .physical_attach_inventory()
        .expect("the pressure-retained frame has a checked byte ledger");
    assert_eq!(pressured.live_authorities(), 1);
    assert_eq!(pressured.pending_frames(), 1);
    assert!(pressured.pending_frame_bytes() > 0);
    assert_eq!(
        pressured.summed_pending_frame_bytes(),
        pressured.stored_pending_frame_bytes(),
    );
    assert_eq!(
        fixture.composition.snapshot().session.generation(),
        generation,
    );
    assert_eq!(arbiter.inventory().pending_physical_settlements(), 1);

    settle_physical_and_assert_exact_zero(
        &mut fixture,
        &mut arbiter,
        PhysicalPendingAttachStage::AttachPending,
    );
    cleanup_registered_fixture("physical-attach-pressure", fixture).await;
}

#[test]
fn r7_physical_attach_inventory_rejects_encoding_failure_without_zero_substitution() {
    assert_eq!(
        audit_physical_pending_frame_bytes_for_test(Err(ProtocolError::FrameTooLarge), 0),
        Err(QuinnPhysicalAttachAuditError::PendingFrameEncoding(
            ProtocolError::FrameTooLarge,
        )),
    );
    assert_eq!(
        audit_physical_pending_frame_bytes_for_test(Ok(73), 72),
        Err(QuinnPhysicalAttachAuditError::PendingFrameBytesMismatch {
            summed: 73,
            stored: 72,
        }),
    );
    assert_eq!(
        audit_physical_pending_frame_bytes_for_test(Ok(73), 73),
        Ok(73),
    );
}

#[tokio::test(flavor = "current_thread")]
async fn r7_physical_attach_shutdown_settles_every_retained_stage_exactly() {
    let mut unbegun = registered_fixture().await;
    let mut unbegun_arbiter = admit_soft_physical_attempt(&mut unbegun, 0xb1);
    settle_physical_and_assert_exact_zero(
        &mut unbegun,
        &mut unbegun_arbiter,
        PhysicalPendingAttachStage::Unbegun,
    );
    cleanup_registered_fixture("unbegun", unbegun).await;

    let mut pending = registered_fixture().await;
    let generation = pending.composition.snapshot().session.generation();
    let mut pending_arbiter = admit_soft_physical_attempt(&mut pending, 0xb2);
    for sequence in 1..=4 {
        pending
            .composition
            .enqueue_registered_standby_control_for_test(ordered_control_for_generation(
                generation,
                9_000 + sequence,
            ))
            .expect("the real B FIFO is filled to its checked message capacity");
    }
    assert_eq!(
        pending
            .composition
            .drive_owned_standby_physical_attach(
                SimTime::from_nanos(40),
                &actor_attach_credentials(),
            )
            .expect("bounded pressure retains the exact pending attach"),
        crate::owned_upstream::quinn_leg_composition::QuinnStandbyAttachDrive::Pressure,
    );
    settle_physical_and_assert_exact_zero(
        &mut pending,
        &mut pending_arbiter,
        PhysicalPendingAttachStage::AttachPending,
    );
    cleanup_registered_fixture("attach-pending", pending).await;

    let mut awaiting = registered_fixture().await;
    let mut awaiting_arbiter = admit_soft_physical_attempt(&mut awaiting, 0xb3);
    drive_physical_to_awaiting(&mut awaiting, SimTime::from_nanos(50));
    settle_physical_and_assert_exact_zero(
        &mut awaiting,
        &mut awaiting_arbiter,
        PhysicalPendingAttachStage::AwaitingAttach,
    );
    cleanup_registered_fixture("awaiting-attach", awaiting).await;

    let mut accepted_raw = registered_fixture().await;
    let mut accepted_arbiter = admit_soft_physical_attempt(&mut accepted_raw, 0xb4);
    let mut accepted = compose_registered_owner_b(accepted_raw);
    let owner_generation =
        advance_composed_physical_to_accepted(&mut accepted, SimTime::from_nanos(60)).await;
    settle_physical_composition_and_assert_exact_zero(
        &mut accepted.composition,
        &mut accepted_arbiter,
        PhysicalPendingAttachStage::Accepted,
    );
    let accepted_discard = accepted
        .composition
        .accepted_physical_shutdown_discard()
        .expect("accepted no-ACK shutdown retains one historical ownership witness");
    assert_eq!(accepted_discard.generation(), owner_generation);
    assert_eq!(accepted_discard.held_records(), 1);
    assert!(accepted_discard.held_encoded_bytes() > 0);
    assert_eq!(accepted_discard.queued_inbound_markers(), 1);
    assert_eq!(accepted_discard.composition_message_permits(), 1);
    assert_eq!(accepted_discard.reserved_delivery_acks_discarded(), 1);
    assert_eq!(accepted_discard.reserved_delivery_ack_bytes_discarded(), 18);
    assert_eq!(accepted_discard.delivery_acks_emitted(), 0);
    assert_eq!(accepted_discard.accepted_authorities(), 1);
    assert_eq!(accepted_discard.request_receipts(), 1);
    assert_eq!(accepted_discard.policy_settlements(), 1);
    cleanup_composed_registered_fixture("accepted", owner_generation, accepted).await;

    let mut status = registered_fixture().await;
    let status_nonce = AttachNonce::new([0xb5; 16]).expect("nonzero status attach nonce");
    let mut status_arbiter = admit_soft_physical_attempt(&mut status, 0xb5);
    advance_physical_to_generation_status(&mut status, SimTime::from_nanos(70), status_nonce).await;
    settle_physical_and_assert_exact_zero(
        &mut status,
        &mut status_arbiter,
        PhysicalPendingAttachStage::GenerationStatus,
    );
    cleanup_registered_fixture("generation-status", status).await;
}

#[tokio::test(flavor = "current_thread")]
async fn r7_physical_attach_settlement_rejection_restores_exact_authority() {
    let mut fixture = registered_fixture().await;
    let session_id = SessionId::new(PHYSICAL_SESSION).expect("nonzero session");
    let generation = fixture.composition.snapshot().session.generation();
    let standby_nonce = StandbyNonce::new(PHYSICAL_STANDBY_NONCE).expect("nonzero standby nonce");
    let attach_nonce = AttachNonce::new([0xb6; 16]).expect("nonzero attach nonce");
    let epoch = SwitchEpoch::new(0xb6).expect("nonzero switch epoch");
    let mut exact =
        ClientSwitchArbiter::new(session_id, generation, standby_nonce, attach_nonce, epoch)
            .expect("exact arbiter");
    let mut scalar_equal =
        ClientSwitchArbiter::new(session_id, generation, standby_nonce, attach_nonce, epoch)
            .expect("scalar-equal arbiter");
    let exact_permit = soft_permit(&mut exact, session_id, generation);
    let wrong_permit = soft_permit(&mut scalar_equal, session_id, generation);
    fixture
        .composition
        .admit_pending_physical_attach(&exact, exact_permit)
        .expect("the exact permit enters the physical slot");

    assert_eq!(
        fixture
            .composition
            .settle_pending_physical_attach_for_shutdown(&mut scalar_equal),
        Err(QuinnPhysicalAttachShutdownError::Arbiter(
            SwitchArbiterError::WrongPhysicalAttachSettlement,
        )),
    );
    let restored = fixture
        .composition
        .physical_attach_inventory()
        .expect("a rejected settlement restores a readable physical ledger");
    assert_eq!(restored.live_authorities(), 1);
    assert_eq!(
        restored.authority_kind(),
        Some(QuinnPhysicalAttachAuthorityKind::Shutdown),
    );
    assert_eq!(restored.stage(), Some(PhysicalPendingAttachStage::Unbegun));

    settle_physical_and_assert_exact_zero(
        &mut fixture,
        &mut exact,
        PhysicalPendingAttachStage::Unbegun,
    );
    scalar_equal
        .settle_shutdown_pending(
            wrong_permit.into_shutdown_settlement(PhysicalPendingAttachStage::Unbegun),
        )
        .expect("the scalar-equal arbiter's own permit also settles exactly");
    assert!(scalar_equal.inventory().is_live_exact_zero());
    cleanup_registered_fixture("settlement-rejection", fixture).await;
}

#[tokio::test(flavor = "current_thread")]
async fn r7_physical_attach_settlement_precedes_cancelled_structural_shutdown() {
    let mut fixture = registered_fixture().await;
    let mut arbiter = admit_soft_physical_attempt(&mut fixture, 0xb7);
    settle_physical_and_assert_exact_zero(
        &mut fixture,
        &mut arbiter,
        PhysicalPendingAttachStage::Unbegun,
    );
    let standby_join_block = fixture
        .composition
        .arm_standby_join_block_for_test()
        .expect("the composition still owns physical B");
    fixture.server_a.request_cancel();
    fixture.server_b.request_cancel();
    let server_a_task = tokio::spawn(async move { fixture.server_a.join().await });
    let server_b_task = tokio::spawn(async move { fixture.server_b.join().await });
    let mut first_shutdown = Box::pin(fixture.composition.join_all_for_shutdown());
    tokio::select! {
        biased;
        _outcome = &mut first_shutdown => {
            panic!("structural shutdown bypassed the retained B join gate")
        }
        () = standby_join_block.wait_until_blocked() => {}
    }
    drop(first_shutdown);
    assert!(arbiter.inventory().is_live_exact_zero());
    assert!(
        fixture
            .composition
            .physical_attach_inventory()
            .expect("cancellation cannot recreate settled physical authority")
            .is_empty()
    );
    standby_join_block.release();
    let shutdown = fixture.composition.join_all_for_shutdown().await;
    let (joined, shutdown_error) = shutdown.into_parts();
    assert!(
        shutdown_error.is_none(),
        "shutdown retry failed: {shutdown_error:?}"
    );
    let joined = joined.expect("shutdown retry returns current A's zero witness");
    assert_eq!(joined.owned_messages(), 0);
    assert_eq!(joined.owned_bytes(), 0);
    let server_a_joined = server_a_task
        .await
        .expect("server A join task remains live")
        .expect("server A joins after synchronous local cancellation");
    settle_raw_local_cancelled_actor_join("cancelled-shutdown", "server A", server_a_joined);
    let server_b_joined = server_b_task
        .await
        .expect("server B join task remains live")
        .expect("server B joins after synchronous local cancellation");
    settle_raw_local_cancelled_actor_join("cancelled-shutdown", "server B", server_b_joined);
    drop((
        fixture.composition,
        fixture.tcp_factory,
        fixture.local_flows,
        fixture.peer_policy,
        fixture.owner_executor,
        fixture.owner_active_a,
        fixture.standby_slot,
    ));
    fixture
        .listener
        .shutdown()
        .await
        .expect("cancelled shutdown retry releases both actor slots");
}
