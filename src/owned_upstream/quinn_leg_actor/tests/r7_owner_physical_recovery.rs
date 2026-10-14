use std::time::Duration;

use super::*;
use crate::owned_upstream::owner_output_drain::OwnerOutputDrainStopReason;
use crate::owned_upstream::owner_ready::{OwnerReadySessionLimits, ReadyGeometry, ReadyIndex};
use crate::owned_upstream::owner_session_controller::OwnerSessionController;
use crate::owned_upstream::quinn_leg_composition::{
    QuinnLegCompositionGate, QuinnOwnerOutputServiceDrive, QuinnOwnerOutputStopDrive,
    QuinnOwnerRecoveryInboundDrive, QuinnOwnerRecoveryServiceDrive,
    QuinnOwnerRecoverySessionComposition, QuinnOwnerRecoveryTerminalDrive,
    QuinnOwnerRecoveryTerminalError, QuinnOwnerStandbySessionComposition,
    QuinnStandbyLivenessConfig,
};

mod post_bind_unwind;

struct LegacyActiveBTerminalFixture {
    listener: QuinnLegActorListener,
    composition: QuinnClientSessionComposition,
    server_a: StartedQuinnLegActor,
    owner: QuinnOwnerRecoverySessionComposition<MemoryTarget>,
    tcp_factory: ResumableTcpPortFactory,
    local_flows: Vec<LocalFlow>,
    owner_active_a: AttachedLeg,
    installed_generation: LegGeneration,
    ready_index: ReadyIndex,
}

async fn legacy_active_b_terminal_fixture() -> LegacyActiveBTerminalFixture {
    let mut ready_index =
        ReadyIndex::cold(&ReadyGeometry::checked(std::num::NonZeroUsize::new(1).unwrap()).unwrap())
            .unwrap();
    let (fixture, owner_capacity_plan) =
        real_registered_standby_fixture_with_target_state_checked(2, 1, false).await;
    let RealRegisteredStandbyFixture {
        listener,
        mut composition,
        mut server_a,
        mut client_b,
        server_b,
        registered_b,
        tcp_factory,
        local_flows,
        peer_policy: _,
        owner_executor,
        owner_active_a,
        standby_slot,
    } = fixture;
    let server_a_cold_before = server_a
        .outbound_queue_cold_observation_for_test()
        .expect("server A retains a cold-auditable FIFO after fixture setup");
    assert_eq!(server_a_cold_before.phase, "Active");
    assert_eq!(server_a_cold_before.queued_frames, 0);
    assert_eq!(server_a_cold_before.summed_frame_bytes, 0);
    assert_eq!(server_a_cold_before.stored_owned_bytes, 0);
    assert!(
        server_a_cold_before.ordered_delivery_wait,
        "server A retains exactly the initial-acceptance delivery receipt",
    );
    wait_actor_ordered_delivery(&mut server_a).await;
    let server_a_cold_after = server_a
        .outbound_queue_cold_observation_for_test()
        .expect("server A FIFO remains cold-auditable after exact delivery");
    assert_eq!(server_a_cold_after.phase, "Active");
    assert_eq!(server_a_cold_after.queued_frames, 0);
    assert_eq!(server_a_cold_after.summed_frame_bytes, 0);
    assert_eq!(server_a_cold_after.stored_owned_bytes, 0);
    assert!(
        !server_a_cold_after.ordered_delivery_wait,
        "real ordered delivery settles only the retained acceptance receipt",
    );
    let owner_clock = server_b.session_clock_for_test();
    let ready_limits =
        OwnerReadySessionLimits::checked(owner_capacity_plan.session_config().max_flows(), 1, 8, 8)
            .unwrap();
    // Declared real_registered_standby_fixture request input, not an identity capability.
    let ready = ready_index
        .reserve_session(SessionId::new([0x93; 16]).unwrap(), ready_limits)
        .unwrap();
    let controller = OwnerSessionController::new_checked(
        owner_executor,
        owner_clock,
        owner_capacity_plan,
        ready,
        std::time::Duration::from_secs(1),
    )
    .expect("physical owner controller uses actor B's exact clock and capacity plan");
    let standby = QuinnOwnerStandbySessionComposition::adopt_registered(
        server_b,
        standby_slot,
        QuinnStandbyLivenessConfig::new(Duration::from_millis(30), Duration::from_millis(200))
            .expect("bounded owner B liveness"),
    )
    .expect("physical owner adopts exact registered B actor and FIFO");
    let mut owner = QuinnOwnerRecoverySessionComposition::adopt(standby, controller)
        .expect("physical owner adopts controller with the same actor clock");

    let awaiting_attach = registered_b
        .begin_exact_next_attach(
            AttachNonce::new([0x98; 16]).expect("nonzero B attach nonce"),
            &actor_attach_credentials(),
        )
        .expect("registered B derives its exact next attach")
        .enqueue(client_b.queue.as_mut().expect("client B owns its FIFO"))
        .expect("client B queues its exact next attach");
    assert_eq!(
        client_b
            .try_flush(SimTime::ZERO)
            .expect("client B attach flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match owner.try_receive_owner_b() {
                QuinnOwnerRecoveryInboundDrive::Idle | QuinnOwnerRecoveryInboundDrive::Blocked => {
                    tokio::task::yield_now().await
                }
                QuinnOwnerRecoveryInboundDrive::RecoveryAccepted { .. } => break,
                outcome => panic!("physical owner did not accept exact B attach: {outcome:?}"),
            }
        }
    })
    .await
    .expect("physical owner accepts exact B attach within the local bound");

    let installed_generation = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match owner.service_recovery_once() {
                QuinnOwnerRecoveryServiceDrive::Progress {
                    installed_generation: Some(generation),
                    ..
                }
                | QuinnOwnerRecoveryServiceDrive::Installed { generation } => break generation,
                QuinnOwnerRecoveryServiceDrive::Progress {
                    installed_generation: None,
                    ..
                }
                | QuinnOwnerRecoveryServiceDrive::Pressure
                | QuinnOwnerRecoveryServiceDrive::Inert => tokio::task::yield_now().await,
                outcome => panic!("physical owner recovery did not install B: {outcome:?}"),
            }
        }
    })
    .await
    .expect("physical owner installs active B within the local bound");
    assert_eq!(owner.active_generation(), Some(installed_generation));

    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match owner
                .try_flush(SimTime::ZERO)
                .expect("owner B acceptance flush remains live")
            {
                QuinnLegActorFlushDisposition::Submitted => break,
                QuinnLegActorFlushDisposition::OrderedDeliveryPending
                | QuinnLegActorFlushDisposition::Idle => tokio::task::yield_now().await,
            }
        }
    })
    .await
    .expect("physical owner submits B acceptance within the local bound");
    let attach_acceptance =
        tokio::time::timeout(TEST_TIMEOUT, client_b.recv_classified(STANDBY_CONTROL_V1))
            .await
            .expect("client B attach acceptance receive is bounded")
            .expect("client B remains live")
            .expect("client B receives exact attach acceptance");
    let accepted = match awaiting_attach
        .validate_response(take_actor_inbound(attach_acceptance))
        .expect("client validates physical owner B response")
    {
        RegisteredAttachResponse::Accepted(accepted) => accepted,
        RegisteredAttachResponse::GenerationStatus(_) => {
            panic!("exact-next physical B unexpectedly returned generation status")
        }
    };
    assert_eq!(accepted.generation(), installed_generation);
    let started_b = composition
        .begin_registered_replacement(accepted, client_b)
        .expect("client composition atomically installs authenticated B");
    assert_eq!(started_b.generation, installed_generation);
    assert_eq!(local_flows.len(), 1);
    let ordinary_flow = local_flows[0].flow_id();
    assert!(matches!(
        composition.drive_recovery_one(),
        ClientRecoveryDrive::Progress(ClientRecoveryObservation::LegActivated {
            generation
        }) if generation == installed_generation
    ));
    assert!(matches!(
        composition.drive_recovery_one(),
        ClientRecoveryDrive::AdmissionCompleted { generation }
            if generation == installed_generation
    ));
    assert_eq!(
        composition
            .try_flush_current(SimTime::ZERO)
            .expect("replayed ordinary B OPEN flush remains live"),
        crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition::Submitted,
    );
    let (ordinary_outputs, ordinary_needs_continue) = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match owner.try_receive_owner_b() {
                QuinnOwnerRecoveryInboundDrive::Idle | QuinnOwnerRecoveryInboundDrive::Blocked => {
                    tokio::task::yield_now().await
                }
                QuinnOwnerRecoveryInboundDrive::Ordinary {
                    outputs,
                    needs_continue,
                } => break (outputs, needs_continue),
                outcome => panic!("physical owner did not classify ordinary B OPEN: {outcome:?}"),
            }
        }
    })
    .await
    .expect("physical owner classifies ordinary B OPEN within the local bound");
    assert_eq!(ordinary_outputs, 1);
    assert!(!ordinary_needs_continue);
    assert_eq!(
        owner.service_output_once(),
        QuinnOwnerOutputServiceDrive::Transmitted { page_drained: true },
        "the complete controller page crosses only the exact physical B FIFO",
    );
    assert_eq!(
        owner.service_output_once(),
        QuinnOwnerOutputServiceDrive::PageDrained {
            needs_continue: false,
        },
    );
    assert_eq!(
        owner.service_output_once(),
        QuinnOwnerOutputServiceDrive::Inert
    );
    assert_eq!(
        owner
            .try_flush(SimTime::ZERO)
            .expect("owner B ordinary output flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    let ordinary_effects = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match composition.try_receive_current() {
                QuinnCurrentInboundDrive::Idle => tokio::task::yield_now().await,
                QuinnCurrentInboundDrive::Applied {
                    effects,
                    observation: None,
                } => break effects,
                _ => panic!("client composition did not apply ordinary B output"),
            }
        }
    })
    .await
    .expect("ordinary owner output applies within the local bound");
    assert!(matches!(
        ordinary_effects.as_slice(),
        [SessionEffect::LocalOpenResolved { flow_id, .. }] if flow_id == &ordinary_flow
    ));
    assert!(owner.output_root_inventory().is_output_exact_zero());

    LegacyActiveBTerminalFixture {
        listener,
        composition,
        server_a,
        owner,
        tcp_factory,
        local_flows,
        owner_active_a,
        installed_generation,
        ready_index,
    }
}

async fn shutdown_client_and_settle_external_a(
    composition: &mut QuinnClientSessionComposition,
    server_a: &mut StartedQuinnLegActor,
) {
    let (client_shutdown, server_a_join_result) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(composition.join_all_for_shutdown(), server_a.join())
    })
    .await
    .expect("client composition and retired-A peer shutdown are bounded");
    let (client_composition_joined, first_error) = client_shutdown.into_parts();
    let client_composition_joined =
        client_composition_joined.expect("client composition retains its structural witness");
    if let Some(error) = first_error {
        panic!("client composition shutdown retained a typed error: {error:?}");
    }
    let mut server_a_joined =
        server_a_join_result.expect("server A joins structurally with retired A");
    assert_eq!(client_composition_joined.owned_messages(), 0);
    assert_eq!(client_composition_joined.owned_bytes(), 0);
    assert_eq!(client_composition_joined.owned_terminals(), 0);
    assert!(!client_composition_joined.terminal_failure());
    assert_eq!(server_a_joined.owned_messages(), 0);
    assert_eq!(server_a_joined.owned_bytes(), 0);
    assert_eq!(
        server_a_joined.owned_terminals(),
        1,
        "the legacy fixture keeps owner A outside the B-only adapter"
    );
    assert!(server_a_joined.terminal_failure().is_none());
    let fixture_gate = QuinnLegCompositionGate::mint_for_test();
    let server_a_reason = server_a_joined
        .transact_terminal(&fixture_gate, |_runtime, terminal| match terminal {
            Some(terminal) => Ok(terminal.reason()),
            None => Err((None, ())),
        })
        .expect("fixture cleanup consumes the exact owner-A terminal once");
    assert_eq!(server_a_reason, LegTransportTerminalReason::LocalCancelled);
    let server_a_stamp = server_a_joined
        .take_terminal_stamp()
        .expect("fixture cleanup consumes owner A's exact root-outcome stamp");
    assert_eq!(server_a_stamp.observed_at(), None);
    drop(server_a_stamp);
    assert_eq!(server_a_joined.owned_terminals(), 0);
    drop((client_composition_joined, server_a_joined));
}

#[tokio::test(flavor = "current_thread")]
async fn legacy_b_only_active_terminal_is_applied_once_and_never_reconstructed() {
    let LegacyActiveBTerminalFixture {
        ready_index: _ready_index,
        mut listener,
        mut composition,
        mut server_a,
        mut owner,
        tcp_factory,
        local_flows,
        owner_active_a,
        installed_generation,
    } = legacy_active_b_terminal_fixture().await;
    shutdown_client_and_settle_external_a(&mut composition, &mut server_a).await;

    let (applied_generation, applied_reason) = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match owner.try_drive_reported_terminal() {
                Ok(QuinnOwnerRecoveryTerminalDrive::Pending) => tokio::task::yield_now().await,
                Ok(QuinnOwnerRecoveryTerminalDrive::Applied { generation, reason }) => {
                    break (generation, reason);
                }
                outcome => panic!("physical owner did not apply exact B terminal: {outcome:?}"),
            }
        }
    })
    .await
    .expect("physical owner applies B terminal within the local bound");
    assert_eq!(applied_generation, installed_generation);
    assert!(matches!(
        applied_reason,
        LegTransportTerminalReason::PeerClosed | LegTransportTerminalReason::Reset
    ));
    assert_eq!(owner.active_generation(), None);
    let retained = owner.output_root_inventory();
    assert!(retained.is_output_exact_zero());
    assert!(!retained.is_controller_output_terminal_exact_zero());
    assert_eq!(retained.terminal_residuals(), 1);
    assert_eq!(retained.terminal_stamps(), 1);
    assert_eq!(
        retained
            .controller_snapshot()
            .expect("the converged subroot exposes one controller snapshot")
            .pending_legless_high_waters,
        1,
    );
    match owner.stop_output_for_shutdown() {
        QuinnOwnerOutputStopDrive::AlreadyStopped { reason, inventory } => {
            assert_eq!(reason, OwnerOutputDrainStopReason::Fatal);
            assert!(inventory.is_output_exact_zero());
            assert!(!inventory.is_controller_output_terminal_exact_zero());
        }
        outcome => {
            panic!("hard terminal did not leave a sticky stopped output ledger: {outcome:?}")
        }
    }
    assert_eq!(
        owner
            .try_drive_reported_terminal()
            .expect("already-applied terminal remains value-only"),
        QuinnOwnerRecoveryTerminalDrive::AlreadyApplied {
            generation: installed_generation,
            reason: applied_reason,
        },
    );

    let owner_b_joined = tokio::time::timeout(
        TEST_TIMEOUT,
        owner.join_physical_actor_after_terminal_for_test(),
    )
    .await
    .expect("physical owner B join is bounded")
    .expect("physical owner B joins structurally after terminal application");
    assert_eq!(owner_b_joined.owned_messages(), 0);
    assert_eq!(owner_b_joined.owned_bytes(), 0);
    assert_eq!(owner_b_joined.owned_terminals(), 0);
    drop((owner, owner_b_joined));
    drop((composition, tcp_factory, local_flows, owner_active_a));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("physical owner tracer listener cleanup is bounded")
        .expect("physical owner tracer releases both server actor slots");
}
