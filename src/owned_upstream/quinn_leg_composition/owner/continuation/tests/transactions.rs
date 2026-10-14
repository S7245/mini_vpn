//! Continuation transaction lifecycle characterizations.
//!
//! Owns the original ordinary, recovery, continuation and terminal guard tests;
//! shared real-root fixtures remain private in the parent. This module adds no
//! production ownership, scheduler or settlement behavior. Tests are its entries.

use super::*;

#[tokio::test(start_paused = true)]
async fn checked_output_failure_ledger_audits_its_original_returned_error() {
    use crate::owned_upstream::owner_target::error_inventory::{
        ErrorAuditFailure, ErrorColdInventory,
    };
    for retained_input in [false, true] {
        let plan = OwnerSessionCapacityPlan::derive(replay_storage(), 8, 128).unwrap();
        let session_id = SessionId::new([0x11; 16]).unwrap();
        let transport = binding();
        let request = crate::resumable::AttachRequest::new(
            session_id,
            LegGeneration::new(2).unwrap(),
            AttachNonce::new([0x22; 16]).unwrap(),
            VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION).unwrap(),
            FeatureOffer::new(0b111, 0b001).unwrap(),
        );
        let proof = credentials().prove(&request, &transport).unwrap();
        let authority = AttachAuthority::new(
            session_id,
            LegGeneration::new(1).unwrap(),
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
        let io = LegIo::for_authenticated_transport(
            LegId::A,
            LegEndpointRole::Owner,
            transport,
            LegIoLimits::new(4_096, 8, 32_768, 8, 32_768).unwrap(),
        );
        let mut queue = LegOutboundQueue::for_leg(io.established_leg(), 4, 32_768).unwrap();
        let bootstrap = SessionSupervisor::prepare_owner(plan, authority)
            .accept(
                io.established_leg(),
                io.established_leg()
                    .bind_received_frame(request.to_attach_frame(proof)),
                &mut queue,
            )
            .unwrap()
            .enqueue_acceptance(&mut queue)
            .unwrap();
        let (executor, handoff) = OwnerTargetExecutor::from_initial_bootstrap(
            OwnerTargetConfig::new(
                1,
                plan.session_config().owner_pending_work_bound().unwrap(),
                64,
            )
            .unwrap(),
            bootstrap,
            MemoryTarget::new(
                MemoryTargetConfig::new(4, 256, 512, 128, 128, 16)
                    .unwrap()
                    .with_max_terminal_tombstones(8)
                    .unwrap(),
            ),
            FlowPortConfig::new(128, 8, 8, 8).unwrap(),
        )
        .unwrap();
        let mut index = ReadyIndex::cold(
            &ReadyGeometry::checked(std::num::NonZeroUsize::new(1).unwrap()).unwrap(),
        )
        .unwrap();
        let ready = index
            .reserve_session(
                session_id,
                OwnerReadySessionLimits::checked(plan.session_config().max_flows(), 1, 8, 8)
                    .unwrap(),
            )
            .unwrap();
        let clock = SessionMonotonicClock::start();
        let controller =
            OwnerSessionController::new_checked(executor, clock.clone(), plan, ready, TTL).unwrap();
        let mut root = QuinnOwnerControllerOutputRoot::new(controller)
            .map_err(|failure| failure.into_parts().1)
            .unwrap();
        // Actual checked BoundInactive, not an Active scheduler or wire fixture.
        if retained_input {
            // Deliberately wrong arrival clock, with otherwise matching leg
            // identity. Binding alone does not prohibit direct actor admission.
            let other_clock = SessionMonotonicClock::start();
            let input = OwnerSessionActorEvent::capture_for_test(
                SessionEvent::PeerFrame {
                    leg: committed_leg(),
                    frame: Frame::try_new(
                        LegGeneration::new(2).unwrap(),
                        Record::Open {
                            flow_id: SessionFlowId::new(1).unwrap(),
                            target: TargetAddr::IpPort(SocketAddr::V4(SocketAddrV4::new(
                                Ipv4Addr::LOCALHOST,
                                TEST_PORT,
                            ))),
                        },
                    )
                    .unwrap(),
                },
                &other_clock,
            )
            .unwrap();
            assert!(
                root.begin_inbound_admission()
                    .unwrap()
                    .apply_actor_event_and_admit(input)
                    .is_err()
            );
        } else {
            assert!(root.service_ready_send(SendLane::Control).is_err());
        }
        let QuinnOwnerControllerFailureLedger::Failed(error) = &root.controller_failure else {
            panic!("the real service rejection must reach the original failure ledger");
        };
        let expected = if retained_input {
            assert_eq!(format!("{error:?}"), "WrongActorClock");
            let payload = error
                .cold_inventory()
                .expect("complete original Controller actor input audit");
            assert_eq!(
                (payload.owner.actor_events, payload.owner.arrival_witnesses),
                (1, 1)
            );
            assert_eq!(
                (payload.payload.events, payload.payload.session_frames),
                (1, 1)
            );
            assert_eq!(
                (payload.payload.targets, payload.payload.capability_values),
                (1, 1)
            );
            assert!(payload.payload.session_encoded_bytes > 0);
            assert!(!payload.is_exact_zero());
            Ok(payload)
        } else {
            Ok(ErrorColdInventory::empty())
        };
        assert_eq!(error.cold_inventory(), expected);
        let inventory = root.output_inventory();
        assert_eq!(inventory.controller_error_payload(), expected);
        assert_eq!(inventory.controller_failures(), 1);
        assert!(!inventory.controller_cold_audit_failed());
        assert!(!inventory.is_controller_output_terminal_exact_zero());
        for _ in 0..2 {
            assert!(root.begin_inbound_admission().is_none());
            assert!(root.service_ready_send(SendLane::Control).is_err());
            assert_eq!(root.output_inventory(), inventory);
        }
        // Explicit ledger fault: retain its original error while auditing the
        // Invariant branch, then restore it. Not an unknown/unwind simulation.
        let original = std::mem::replace(
            &mut root.controller_failure,
            QuinnOwnerControllerFailureLedger::Invariant,
        );
        let invariant = root.output_inventory();
        assert_eq!(
            invariant.controller_error_payload(),
            Err(ErrorAuditFailure::Invariant)
        );
        assert!(invariant.controller_cold_audit_failed());
        assert!(!invariant.is_controller_output_terminal_exact_zero());
        root.controller_failure = original;
        assert_eq!(root.output_inventory(), inventory);
        assert!(handoff.acceptance_belongs_to_queue(&queue));
    }
}

#[tokio::test(start_paused = true)]
async fn ordinary_prepared_drop_restores_exact_cold_root() {
    let mut ready_index_1 =
        ReadyIndex::cold(&ReadyGeometry::checked(std::num::NonZeroUsize::new(1).unwrap()).unwrap())
            .unwrap();
    let (_clock, _leg, mut root) = checked_root(&mut ready_index_1);
    assert!(root.output_inventory().is_output_exact_zero());

    let transaction = root
        .begin_inbound_admission()
        .expect("one prepared ordinary transaction");
    drop(transaction);

    let restored = root.output_inventory();
    assert!(restored.is_output_exact_zero());
    assert_eq!(restored.ordinary_transactions(), 0);
    assert!(!restored.quarantined());
}

#[tokio::test(start_paused = true)]
async fn forgotten_ordinary_transaction_remains_visible_and_nonzero() {
    let mut ready_index_2 =
        ReadyIndex::cold(&ReadyGeometry::checked(std::num::NonZeroUsize::new(1).unwrap()).unwrap())
            .unwrap();
    let (_clock, _leg, mut root) = checked_root(&mut ready_index_2);
    let transaction = root
        .begin_inbound_admission()
        .expect("one prepared ordinary transaction");

    std::mem::forget(transaction);

    let inventory = root.output_inventory();
    assert_eq!(inventory.ordinary_transactions(), 1);
    assert_eq!(inventory.phase_markers(), 0);
    assert!(!inventory.is_output_exact_zero());
    assert!(inventory.phase_drain_consistent());
    assert!(root.begin_inbound_admission().is_none());
}

#[tokio::test(start_paused = true)]
async fn prepared_real_continuation_drop_restores_the_exact_marker() {
    let mut ready_index_3 =
        ReadyIndex::cold(&ReadyGeometry::checked(std::num::NonZeroUsize::new(1).unwrap()).unwrap())
            .unwrap();
    let mut root = root_with_real_pending_open(&mut ready_index_3);
    drain_real_first_page(&mut root);
    let before = root.output_inventory();

    let transaction = match root.begin_service_turn() {
        QuinnOwnerOutputServiceTurn::Continue(transaction) => transaction,
        _ => panic!("real drained page must mint its continuation"),
    };
    drop(transaction);

    assert_eq!(root.output_inventory(), before);
    assert_eq!(root.snapshot().pending_continuations, 1);
}

#[tokio::test(start_paused = true)]
async fn forgotten_real_continuation_remains_visible_and_nonzero() {
    let mut ready_index_4 =
        ReadyIndex::cold(&ReadyGeometry::checked(std::num::NonZeroUsize::new(1).unwrap()).unwrap())
            .unwrap();
    let mut root = root_with_real_pending_open(&mut ready_index_4);
    drain_real_first_page(&mut root);
    let transaction = match root.begin_service_turn() {
        QuinnOwnerOutputServiceTurn::Continue(transaction) => transaction,
        _ => panic!("real drained page must mint its continuation"),
    };

    std::mem::forget(transaction);

    let inventory = root.output_inventory();
    assert_eq!(inventory.phase_markers(), 0);
    assert_eq!(inventory.continuation_markers(), 0);
    assert_eq!(inventory.continuation_transactions(), 1);
    assert!(inventory.phase_drain_consistent());
    assert!(!inventory.is_output_exact_zero());
    assert!(matches!(
        root.begin_service_turn(),
        QuinnOwnerOutputServiceTurn::CompositionInvariant
    ));
    let quarantined = root.output_inventory();
    assert_eq!(quarantined.continuation_transactions(), 1);
    assert!(quarantined.quarantined());
}

#[tokio::test(start_paused = true)]
async fn prepared_recovery_service_drop_restores_exact_cold_root() {
    let mut ready_index_5 =
        ReadyIndex::cold(&ReadyGeometry::checked(std::num::NonZeroUsize::new(1).unwrap()).unwrap())
            .unwrap();
    let (_clock, _leg, mut root) = checked_root(&mut ready_index_5);
    let before = root.output_inventory();

    let transaction = root
        .begin_recovery_service()
        .expect("one prepared recovery service transaction");
    drop(transaction);

    assert_eq!(root.output_inventory(), before);
    assert_eq!(root.output_inventory().recovery_transactions(), 0);
    assert!(!root.output_inventory().recovery_quarantined());
    assert!(!root.controller_failed());
}

#[tokio::test(start_paused = true)]
async fn entered_recovery_service_drop_is_sticky_quarantine() {
    let mut ready_index_6 =
        ReadyIndex::cold(&ReadyGeometry::checked(std::num::NonZeroUsize::new(1).unwrap()).unwrap())
            .unwrap();
    let (_clock, _leg, mut root) = checked_root(&mut ready_index_6);
    let mut transaction = root
        .begin_recovery_service()
        .expect("one prepared recovery service transaction");
    assert!(transaction.enter(QuinnOwnerServiceWork::Recovery));

    drop(transaction);

    let inventory = root.output_inventory();
    assert_eq!(inventory.recovery_transactions(), 1);
    assert!(inventory.recovery_quarantined());
    assert!(!inventory.is_output_exact_zero());
    assert!(root.controller_failed());
    assert!(root.begin_inbound_admission().is_none());
}

#[tokio::test(start_paused = true)]
async fn forgotten_recovery_service_transaction_remains_visible_and_blocks_reentry() {
    let mut ready_index_7 =
        ReadyIndex::cold(&ReadyGeometry::checked(std::num::NonZeroUsize::new(1).unwrap()).unwrap())
            .unwrap();
    let (_clock, _leg, mut root) = checked_root(&mut ready_index_7);
    let transaction = root
        .begin_recovery_service()
        .expect("one prepared recovery service transaction");

    std::mem::forget(transaction);

    let forgotten = root.output_inventory();
    assert_eq!(forgotten.recovery_transactions(), 1);
    assert!(!forgotten.recovery_quarantined());
    assert!(!forgotten.is_output_exact_zero());
    assert!(root.controller_failed());
    assert!(root.begin_inbound_admission().is_none());

    assert!(root.begin_recovery_service().is_err());
    let quarantined = root.output_inventory();
    assert_eq!(quarantined.recovery_transactions(), 1);
    assert!(quarantined.recovery_quarantined());
    assert!(!quarantined.is_output_exact_zero());
}

#[tokio::test(start_paused = true)]
async fn prepared_terminal_transaction_drop_restores_exact_cold_root() {
    let mut ready_index_8 =
        ReadyIndex::cold(&ReadyGeometry::checked(std::num::NonZeroUsize::new(1).unwrap()).unwrap())
            .unwrap();
    let (_clock, _leg, mut root) = checked_root(&mut ready_index_8);
    let before = root.output_inventory();
    assert!(before.is_controller_output_terminal_exact_zero());

    let transaction = root
        .begin_terminal_transaction()
        .expect("one prepared terminal transaction");
    drop(transaction);

    let restored = root.output_inventory();
    assert_eq!(restored, before);
    assert_eq!(restored.terminal_transactions(), 0);
    assert_eq!(restored.terminal_residuals(), 0);
    assert_eq!(restored.terminal_stamps(), 0);
    assert!(!restored.terminal_quarantined());
    assert!(restored.is_controller_output_terminal_exact_zero());
}

#[tokio::test(start_paused = true)]
async fn entered_terminal_transaction_drop_is_sticky_nonzero_quarantine() {
    let mut ready_index_9 =
        ReadyIndex::cold(&ReadyGeometry::checked(std::num::NonZeroUsize::new(1).unwrap()).unwrap())
            .unwrap();
    let (_clock, _leg, mut root) = checked_root(&mut ready_index_9);
    let mut transaction = root
        .begin_terminal_transaction()
        .expect("one prepared terminal transaction");
    assert!(transaction.enter_actor_call());
    drop(transaction);

    let inventory = root.output_inventory();
    assert_eq!(inventory.terminal_transactions(), 1);
    assert_eq!(inventory.terminal_residuals(), 1);
    assert_eq!(inventory.terminal_stamps(), 0);
    assert!(inventory.terminal_quarantined());
    assert!(!inventory.is_controller_output_terminal_exact_zero());
    assert!(matches!(
        root.begin_terminal_transaction(),
        Err(QuinnOwnerControllerOutputTerminalBeginError::Quarantined {
            controller_failed: false
        })
    ));
}

#[tokio::test(start_paused = true)]
async fn forgotten_terminal_transaction_stays_visible_and_blocks_reentry() {
    let mut ready_index_10 =
        ReadyIndex::cold(&ReadyGeometry::checked(std::num::NonZeroUsize::new(1).unwrap()).unwrap())
            .unwrap();
    let (_clock, _leg, mut root) = checked_root(&mut ready_index_10);
    let transaction = root
        .begin_terminal_transaction()
        .expect("one prepared terminal transaction");

    std::mem::forget(transaction);

    let inventory = root.output_inventory();
    assert_eq!(inventory.terminal_transactions(), 1);
    assert_eq!(inventory.terminal_residuals(), 1);
    assert_eq!(inventory.terminal_stamps(), 0);
    assert!(!inventory.terminal_quarantined());
    assert!(!inventory.is_controller_output_terminal_exact_zero());
    assert!(matches!(
        root.begin_terminal_transaction(),
        Err(QuinnOwnerControllerOutputTerminalBeginError::InFlight)
    ));
}

#[tokio::test(start_paused = true)]
async fn terminal_transaction_refuses_live_output_before_actor_consumption() {
    let mut ready_index_11 =
        ReadyIndex::cold(&ReadyGeometry::checked(std::num::NonZeroUsize::new(1).unwrap()).unwrap())
            .unwrap();
    let mut root = root_with_real_pending_open(&mut ready_index_11);
    let before = root.output_inventory();
    assert!(!before.is_output_exact_zero());
    assert_eq!(before.terminal_transactions(), 0);

    assert!(matches!(
        root.begin_terminal_transaction(),
        Err(QuinnOwnerControllerOutputTerminalBeginError::OutputNotSettled)
    ));

    let after = root.output_inventory();
    assert_eq!(after, before);
    assert_eq!(after.terminal_transactions(), 0);
    assert_eq!(after.terminal_residuals(), 0);
    assert_eq!(after.terminal_stamps(), 0);
    assert!(!after.terminal_quarantined());
}

#[tokio::test(start_paused = true)]
async fn terminal_transaction_refuses_every_unsettled_output_ledger() {
    let mut ready_index_12 =
        ReadyIndex::cold(&ReadyGeometry::checked(std::num::NonZeroUsize::new(1).unwrap()).unwrap())
            .unwrap();
    let (_clock, _leg, mut drained) = checked_root(&mut ready_index_12);
    drained.output.ledger.status = QuinnOwnerOutputStatus::Live {
        phase: QuinnOwnerOutputPhase::Drained(OwnerOutputPageDrained::new(true)),
    };
    let drained_before = drained.output_inventory();
    assert!(matches!(
        drained.begin_terminal_transaction(),
        Err(QuinnOwnerControllerOutputTerminalBeginError::OutputNotSettled)
    ));
    assert_eq!(drained.output_inventory(), drained_before);

    let mut ready_index_13 =
        ReadyIndex::cold(&ReadyGeometry::checked(std::num::NonZeroUsize::new(1).unwrap()).unwrap())
            .unwrap();
    let (_clock, _leg, mut lifecycle) = checked_root(&mut ready_index_13);
    lifecycle
        .output
        .lifecycle
        .try_install(OwnerOutputLifecycleAction::session_expired_for_test())
        .expect("the empty lifecycle port accepts one exact action");
    let lifecycle_before = lifecycle.output_inventory();
    assert!(matches!(
        lifecycle.begin_terminal_transaction(),
        Err(QuinnOwnerControllerOutputTerminalBeginError::OutputNotSettled)
    ));
    assert_eq!(lifecycle.output_inventory(), lifecycle_before);

    let mut ready_index_14 =
        ReadyIndex::cold(&ReadyGeometry::checked(std::num::NonZeroUsize::new(1).unwrap()).unwrap())
            .unwrap();
    let (_clock, _leg, mut rejected) = checked_root(&mut ready_index_14);
    let failure = OwnerOutputDrain::new(1, 1)
        .expect("one-slot rejection fixture")
        .admit_outputs(
            vec![
                OwnerTargetOutput::LegActivated {
                    generation: LegGeneration::new(2).unwrap(),
                },
                OwnerTargetOutput::SessionExpired,
            ],
            true,
        )
        .expect_err("two outputs cannot enter one slot");
    rejected.output.admission_failure = Some(failure);
    rejected.output.ledger.status = QuinnOwnerOutputStatus::RejectedPage {
        first_stop_reason: None,
    };
    let rejected_before = rejected.output_inventory();
    assert!(matches!(
        rejected.begin_terminal_transaction(),
        Err(QuinnOwnerControllerOutputTerminalBeginError::OutputNotSettled)
    ));
    assert_eq!(rejected.output_inventory(), rejected_before);
}

#[tokio::test(start_paused = true)]
async fn real_nonpressure_recovery_error_closes_inside_root_and_blocks_controller_reentry() {
    let mut ready_index_15 =
        ReadyIndex::cold(&ReadyGeometry::checked(std::num::NonZeroUsize::new(1).unwrap()).unwrap())
            .unwrap();
    let (mut root, replacement_io, acceptance_queue, gate) =
        checked_root_with_pending_recovery(&mut ready_index_15);
    let wrong_io = LegIo::for_authenticated_transport(
        LegId::A,
        LegEndpointRole::Owner,
        AttachTransportBinding::new(
            OwnerIdentity::new([0x31; 32]).unwrap(),
            AttachAlpn::new(b"mini-vpn-owned/1").unwrap(),
            TlsExporterBinding::new([0x44; 32]).unwrap(),
            DevicePrincipal::new([0x53; 16]).unwrap(),
        ),
        LegIoLimits::new(1_024, 8, 8_192, 8, 8_192).unwrap(),
    );
    let mut wrong_queue = LegOutboundQueue::for_leg(wrong_io.established_leg(), 4, 8_192).unwrap();
    let mut active_b = None;
    let before = root.snapshot();
    assert!(before.recovery_needs_execute);

    let first = root.service_recovery_with_parts(
        &mut active_b,
        replacement_io.established_leg(),
        &mut wrong_queue,
        &gate,
    );
    assert!(matches!(first, Err(QuinnLegActorError::Protocol(_))));
    assert_eq!(
        format!(
            "{:?}",
            root.controller_failure_for_test()
                .expect("the root retains the exact first controller error")
        ),
        "RejectedRecoveryExecute(WrongAcceptanceQueue)"
    );
    assert_eq!(root.snapshot(), before);
    assert!(root.controller_failed());
    assert_eq!(root.output_inventory().recovery_transactions(), 0);
    assert!(!root.output_inventory().recovery_quarantined());
    assert_eq!(acceptance_queue.len(), 1);
    assert_eq!(wrong_queue.len(), 0);
    assert!(active_b.is_none());

    let failed_snapshot = root.snapshot();
    let second = root.service_recovery_with_parts(
        &mut active_b,
        replacement_io.established_leg(),
        &mut wrong_queue,
        &gate,
    );
    assert!(matches!(second, Err(QuinnLegActorError::Protocol(_))));
    assert_eq!(root.snapshot(), failed_snapshot);
    assert_eq!(acceptance_queue.len(), 1);
    assert_eq!(wrong_queue.len(), 0);
    assert!(active_b.is_none());
}

#[tokio::test(start_paused = true)]
async fn real_continuation_advanced_consumes_old_marker_and_closes_output_root() {
    let mut ready_index_16 =
        ReadyIndex::cold(&ReadyGeometry::checked(std::num::NonZeroUsize::new(1).unwrap()).unwrap())
            .unwrap();
    let mut root = root_with_real_pending_open(&mut ready_index_16);
    drain_real_first_page(&mut root);

    let continued = match root.begin_service_turn() {
        QuinnOwnerOutputServiceTurn::Continue(transaction) => transaction.continue_and_settle(),
        _ => panic!("real drained page must mint its continuation"),
    };
    assert!(matches!(
        continued,
        QuinnOwnerOutputContinuationDrive::Admitted {
            needs_continue: false,
            ..
        }
    ));
    let admitted = root.output_inventory();
    assert_eq!(admitted.continuation_markers(), 0);
    assert_eq!(admitted.continuation_transactions(), 0);
    assert_eq!(root.snapshot().pending_continuations, 0);

    drain_real_terminal_page(&mut root);

    let closed = root.output_inventory();
    assert!(closed.is_output_exact_zero());
    assert_eq!(closed.phase_markers(), 0);
    assert_eq!(closed.continuation_markers(), 0);
    assert_eq!(closed.continuation_transactions(), 0);
}

#[tokio::test(start_paused = true)]
async fn fault_injected_vacant_with_open_drain_is_sticky_quarantine() {
    let mut ready_index_17 =
        ReadyIndex::cold(&ReadyGeometry::checked(std::num::NonZeroUsize::new(1).unwrap()).unwrap())
            .unwrap();
    let (_clock, _leg, mut root) = checked_root(&mut ready_index_17);
    root.output
        .ledger
        .drain
        .admit_outputs(Vec::new(), false)
        .unwrap();

    assert!(root.begin_inbound_admission().is_none());
    let first = root.output_inventory();
    assert!(first.quarantined());
    assert!(!first.phase_drain_consistent());
    assert!(!first.is_output_exact_zero());

    assert!(matches!(
        root.begin_service_turn(),
        QuinnOwnerOutputServiceTurn::CompositionInvariant
    ));
    assert!(root.output_inventory().quarantined());
}

#[tokio::test(start_paused = true)]
async fn fault_injected_drained_marker_with_open_drain_is_sticky_quarantine() {
    let mut ready_index_18 =
        ReadyIndex::cold(&ReadyGeometry::checked(std::num::NonZeroUsize::new(1).unwrap()).unwrap())
            .unwrap();
    let (_clock, _leg, mut root) = checked_root(&mut ready_index_18);
    root.output
        .ledger
        .drain
        .admit_outputs(Vec::new(), true)
        .unwrap();
    root.output.ledger.status = QuinnOwnerOutputStatus::Live {
        phase: QuinnOwnerOutputPhase::Drained(OwnerOutputPageDrained::new(true)),
    };

    assert!(matches!(
        root.begin_service_turn(),
        QuinnOwnerOutputServiceTurn::CompositionInvariant
    ));
    let first = root.output_inventory();
    assert!(first.quarantined());
    assert!(!first.phase_drain_consistent());
    assert!(!first.is_output_exact_zero());

    assert!(root.begin_inbound_admission().is_none());
    assert!(root.output_inventory().quarantined());
}
