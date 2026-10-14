use super::*;
use crate::owned_upstream::client_controller::{
    ClientControllerObservationSettlementSource, ClientRecoveryTailOwnership,
    ClientRecoveryTailSettlement, ClientRecoveryTailSettlementBlock,
};
use crate::owned_upstream::leg_io::ClientRecoveryTailInventory;
use crate::owned_upstream::quinn_leg_composition::{
    QuinnCurrentFlushDisposition, QuinnCurrentProbeDeadline, QuinnCurrentProbeSchedulerAuthority,
    QuinnRecoveryTailSettlementError,
};

fn drive_replacement_to_recovery_tail(
    composition: &mut QuinnClientSessionComposition,
    started: ClientRecoveryStarted,
) {
    assert!(matches!(
        composition.drive_recovery_one(),
        ClientRecoveryDrive::Progress(ClientRecoveryObservation::LegActivated {
            generation
        }) if generation == started.generation
    ));
    loop {
        match composition.drive_recovery_one() {
            ClientRecoveryDrive::Progress(ClientRecoveryObservation::FrameAdmitted) => {}
            ClientRecoveryDrive::AdmissionCompleted { generation } => {
                assert_eq!(generation, started.generation);
                break;
            }
            outcome => panic!("physical recovery did not retain its exact tail: {outcome:?}"),
        }
    }
}

async fn deliver_current_recovery_tail(
    composition: &mut QuinnClientSessionComposition,
    peer: &mut StartedQuinnLegActor,
) {
    assert_eq!(
        composition
            .try_flush_current(SimTime::ZERO)
            .expect("current recovery flush remains live"),
        QuinnCurrentFlushDisposition::Submitted,
    );
    let inbound = tokio::time::timeout(TEST_TIMEOUT, peer.recv_classified(STANDBY_CONTROL_V1))
        .await
        .expect("physical recovery reaches its exact peer within the local bound")
        .expect("physical recovery peer remains live")
        .expect("physical recovery publishes one exact frame");
    drop(take_actor_inbound(inbound));
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match composition
                .try_flush_current(SimTime::ZERO)
                .expect("current recovery delivery observation remains live")
            {
                QuinnCurrentFlushDisposition::Idle => break,
                QuinnCurrentFlushDisposition::Submitted
                | QuinnCurrentFlushDisposition::OrderedDeliveryPending => {
                    tokio::task::yield_now().await;
                }
            }
        }
    })
    .await
    .expect("physical recovery DELIVERED reaches the exact client actor");
}

async fn arm_delivered_current_probe(
    composition: &mut QuinnClientSessionComposition,
    peer: &mut StartedQuinnLegActor,
) -> QuinnCurrentProbeDeadline {
    let queued = composition
        .begin_current_probe()
        .expect("current B queues one exact liveness probe");
    assert_eq!(
        composition
            .try_flush_current(SimTime::ZERO)
            .expect("current B probe flush remains live"),
        QuinnCurrentFlushDisposition::Submitted,
    );
    let inbound = tokio::time::timeout(TEST_TIMEOUT, peer.recv_classified(STANDBY_CONTROL_V1))
        .await
        .expect("current B probe reaches its exact peer")
        .expect("current B peer remains live")
        .expect("current B publishes one exact probe");
    drop(take_actor_inbound(inbound));
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match composition
                .try_flush_current(SimTime::ZERO)
                .expect("current B probe delivery observation remains live")
            {
                QuinnCurrentFlushDisposition::Idle => break,
                QuinnCurrentFlushDisposition::Submitted
                | QuinnCurrentFlushDisposition::OrderedDeliveryPending => {
                    tokio::task::yield_now().await;
                }
            }
        }
    })
    .await
    .expect("current B probe DELIVERED reaches the exact receipt");
    composition
        .arm_current_probe(queued, SimTime::ZERO, Duration::from_millis(100))
        .expect("delivered probe arms one exact deadline")
}

#[tokio::test(flavor = "current_thread")]
async fn recovery_tail_rejects_wrong_actor_then_consumes_actual_delivery_once() {
    let RealRegisteredReplacementFixture {
        mut listener,
        mut composition,
        mut server_a,
        mut server_b,
        started_b,
        _tcp_factory,
        local_flows,
        peer_policy,
        _owner_executor,
        _owner_active_a,
        _standby_slot,
    } = real_registered_replacement_fixture(2, 1).await;
    wait_actor_ordered_delivery(&mut server_a).await;
    drive_replacement_to_recovery_tail(&mut composition, started_b);

    let before = composition.snapshot();
    let before_actor = composition.current_actor_snapshot();
    let before_inventory = composition
        .recovery_tail_inventory_for_test()
        .expect("active B exposes the exact recovery-tail inventory");
    assert_eq!(before_inventory.live_tails, 1);
    assert!(before_inventory.undelivered_frames > 0);
    assert!(before_inventory.matching_queue);
    assert!(before_inventory.matching_endpoint);
    assert_eq!(
        before_inventory.matching_generation,
        Some(started_b.generation)
    );
    assert!(before_inventory.endpoint_open);

    assert!(composition.swap_current_with_actor_for_test(&mut server_b));
    assert!(matches!(
        composition.settle_completed_recovery_tail(),
        Err(QuinnRecoveryTailSettlementError::Controller(
            ClientRecoveryTailSettlementBlock::WrongPhysicalActor,
        ))
    ));
    assert_eq!(
        composition
            .recovery_tail_inventory_for_test()
            .expect("wrong actor restores the exact recovery tail"),
        before_inventory,
    );
    assert_eq!(composition.snapshot(), before);
    assert!(composition.swap_current_with_actor_for_test(&mut server_b));
    assert_eq!(composition.current_actor_snapshot(), before_actor);

    assert!(matches!(
        composition.settle_completed_recovery_tail(),
        Err(QuinnRecoveryTailSettlementError::Controller(
            ClientRecoveryTailSettlementBlock::DeliveryPending,
        ))
    ));
    assert_eq!(
        composition
            .recovery_tail_inventory_for_test()
            .expect("incomplete settlement restores the exact recovery tail"),
        before_inventory,
    );
    assert_eq!(composition.snapshot(), before);
    assert_eq!(composition.current_actor_snapshot(), before_actor);

    deliver_current_recovery_tail(&mut composition, &mut server_b).await;
    let delivered_inventory = composition
        .recovery_tail_inventory_for_test()
        .expect("delivered tail remains explicitly owned until settlement");
    assert_eq!(delivered_inventory.live_tails, 1);
    assert_eq!(delivered_inventory.undelivered_frames, 0);
    assert_eq!(
        composition
            .settle_completed_recovery_tail()
            .expect("actual delivery completes the exact recovery tail"),
        ClientRecoveryTailSettlement::Settled {
            generation: started_b.generation,
        },
    );
    assert_eq!(
        composition.recovery_tail_inventory_for_test(),
        Some(ClientRecoveryTailInventory::default()),
    );
    let after_settlement = composition.snapshot();
    assert_eq!(
        composition
            .settle_completed_recovery_tail()
            .expect("repeated recovery-tail settlement is inert"),
        ClientRecoveryTailSettlement::AlreadySettled,
    );
    assert_eq!(composition.snapshot(), after_settlement);

    let opened = composition
        .open_local(TargetAddr::IpPort(
            "127.0.0.1:9443".parse().expect("loopback post-tail target"),
        ))
        .expect("settled recovery tail reopens normal policy admission")
        .expect("post-tail local OPEN remains reducer-valid");
    assert!(
        opened
            .iter()
            .any(|effect| matches!(effect, SessionEffect::LocalFlowOpened { .. })),
    );

    let (client_joined, server_a_joined, server_b_joined) =
        tokio::time::timeout(TEST_TIMEOUT, async {
            tokio::join!(
                composition.join_current_for_shutdown(),
                server_a.join(),
                server_b.join(),
            )
        })
        .await
        .expect("settled-tail actors join within the local bound");
    let client_joined = client_joined.expect("settled tail permits direct structural shutdown");
    let server_a_joined = server_a_joined.expect("retired-A peer joins structurally");
    let server_b_joined = server_b_joined.expect("current-B peer joins structurally");
    assert_eq!(client_joined.owned_messages(), 0);
    assert_eq!(client_joined.owned_bytes(), 0);
    assert_eq!(server_a_joined.owned_messages(), 0);
    assert_eq!(server_a_joined.owned_bytes(), 0);
    assert_eq!(server_b_joined.owned_messages(), 0);
    assert_eq!(server_b_joined.owned_bytes(), 0);
    drop((composition, local_flows, peer_policy));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("settled-tail listener cleanup is bounded")
        .expect("settled-tail actors release both listener slots");
}

#[tokio::test(flavor = "current_thread")]
async fn recovery_tail_incomplete_direct_shutdown_blocks_before_cancel_and_retries() {
    let RealRegisteredReplacementFixture {
        mut listener,
        mut composition,
        mut server_a,
        mut server_b,
        started_b,
        _tcp_factory,
        local_flows,
        peer_policy,
        _owner_executor,
        _owner_active_a,
        _standby_slot,
    } = real_registered_replacement_fixture(2, 1).await;
    wait_actor_ordered_delivery(&mut server_a).await;
    drive_replacement_to_recovery_tail(&mut composition, started_b);
    let before = composition.snapshot();
    let before_actor = composition.current_actor_snapshot();
    let before_inventory = composition
        .recovery_tail_inventory_for_test()
        .expect("active B exposes its exact recovery tail");

    let join_error = composition
        .join_current_for_shutdown()
        .await
        .expect_err("incomplete recovery tail blocks direct shutdown before cancellation");
    assert!(matches!(
        join_error,
        QuinnCurrentJoinError::RecoveryTailSettlement(
            QuinnRecoveryTailSettlementError::Controller(
                ClientRecoveryTailSettlementBlock::DeliveryPending,
            ),
        )
    ));
    assert_eq!(composition.snapshot(), before);
    assert_eq!(composition.current_actor_snapshot(), before_actor);
    assert_eq!(
        composition
            .recovery_tail_inventory_for_test()
            .expect("rejected direct shutdown preserves the exact tail"),
        before_inventory,
    );

    deliver_current_recovery_tail(&mut composition, &mut server_b).await;
    let (client_joined, server_a_joined, server_b_joined) =
        tokio::time::timeout(TEST_TIMEOUT, async {
            tokio::join!(
                composition.join_current_for_shutdown(),
                server_a.join(),
                server_b.join(),
            )
        })
        .await
        .expect("delivered-tail direct shutdown retry joins every actor");
    let client_joined = client_joined.expect("direct retry settles and joins current B");
    assert_eq!(client_joined.owned_messages(), 0);
    assert_eq!(client_joined.owned_bytes(), 0);
    assert_eq!(
        server_a_joined
            .expect("retired-A peer joins structurally")
            .owned_messages(),
        0,
    );
    assert_eq!(
        server_b_joined
            .expect("current-B peer joins structurally")
            .owned_messages(),
        0,
    );
    drop((composition, local_flows, peer_policy));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("direct-retry listener cleanup is bounded")
        .expect("direct-retry actors release both listener slots");
}

#[tokio::test(flavor = "current_thread")]
async fn recovery_tail_incomplete_join_all_is_snapshot_inert_and_retryable() {
    let RealRegisteredReplacementFixture {
        mut listener,
        mut composition,
        mut server_a,
        mut server_b,
        started_b,
        _tcp_factory,
        local_flows,
        peer_policy,
        _owner_executor,
        _owner_active_a,
        _standby_slot,
    } = real_registered_replacement_fixture(2, 1).await;
    wait_actor_ordered_delivery(&mut server_a).await;
    drive_replacement_to_recovery_tail(&mut composition, started_b);
    let before = composition.snapshot();
    let before_actor = composition.current_actor_snapshot();
    let before_inventory = composition
        .recovery_tail_inventory_for_test()
        .expect("active B exposes its exact recovery tail");

    let (joined, error) = composition.join_all_for_shutdown().await.into_parts();
    assert!(joined.is_none());
    assert!(matches!(
        error,
        Some(QuinnCurrentJoinError::RecoveryTailSettlement(
            QuinnRecoveryTailSettlementError::Controller(
                ClientRecoveryTailSettlementBlock::DeliveryPending,
            ),
        ))
    ));
    assert_eq!(composition.snapshot(), before);
    assert_eq!(composition.current_actor_snapshot(), before_actor);
    assert_eq!(
        composition
            .recovery_tail_inventory_for_test()
            .expect("incomplete join-all restores the exact tail"),
        before_inventory,
    );

    deliver_current_recovery_tail(&mut composition, &mut server_b).await;
    let (shutdown, server_a_joined, server_b_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(
            composition.join_all_for_shutdown(),
            server_a.join(),
            server_b.join(),
        )
    })
    .await
    .expect("delivered-tail shutdown retry joins every physical actor");
    let (client_joined, first_error) = shutdown.into_parts();
    let client_joined = client_joined.expect("shutdown retry retains its structural witness");
    assert!(
        first_error.is_none(),
        "delivered-tail retry returned {first_error:?}",
    );
    assert_eq!(client_joined.owned_messages(), 0);
    assert_eq!(client_joined.owned_bytes(), 0);
    assert_eq!(client_joined.owned_terminals(), 0);
    assert!(!client_joined.terminal_failure());
    assert_eq!(
        server_a_joined
            .expect("retired-A peer joins structurally")
            .owned_messages(),
        0,
    );
    assert_eq!(
        server_b_joined
            .expect("current-B peer joins structurally")
            .owned_messages(),
        0,
    );

    let (repeat_joined, repeat_error) = composition.join_all_for_shutdown().await.into_parts();
    assert_eq!(
        repeat_joined.expect("joined repeat remains inert"),
        client_joined,
    );
    assert!(repeat_error.is_none());
    drop((composition, local_flows, peer_policy));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("retry-tail listener cleanup is bounded")
        .expect("retry-tail actors release both listener slots");
}

#[tokio::test(flavor = "current_thread")]
async fn fatal_active_recovery_tail_uses_terminal_path_and_still_structurally_joins() {
    let RealRegisteredReplacementFixture {
        mut listener,
        mut composition,
        mut server_a,
        mut server_b,
        started_b,
        _tcp_factory,
        local_flows,
        peer_policy,
        _owner_executor,
        _owner_active_a,
        _standby_slot,
    } = real_registered_replacement_fixture(2, 1).await;
    wait_actor_ordered_delivery(&mut server_a).await;
    drive_replacement_to_recovery_tail(&mut composition, started_b);
    assert_eq!(
        composition.recovery_tail_ownership_for_test(),
        ClientRecoveryTailOwnership::Active,
    );
    let progress_epoch = composition.current_actor_readiness_epoch_for_test();
    let physical_cleanup = composition.arm_current_post_terminal_cleanup_block_for_test();

    server_b.request_cancel();
    let server_b_joined = tokio::time::timeout(TEST_TIMEOUT, server_b.join())
        .await
        .expect("fatal-tail peer cleanup is bounded")
        .expect("fatal-tail peer joins structurally");
    assert_eq!(server_b_joined.owned_messages(), 0);
    assert_eq!(server_b_joined.owned_bytes(), 0);
    let first_cause = tokio::time::timeout(
        TEST_TIMEOUT,
        composition.wait_for_current_actor_progress(progress_epoch),
    )
    .await
    .expect("fatal active tail wakes the exact current actor waiter")
    .expect_err("peer close publishes one hard current-actor failure");
    assert!(matches!(first_cause, QuinnLegActorError::RootFailed(_)));
    assert!(matches!(
        composition.try_drive_current_terminal(),
        Ok(crate::owned_upstream::quinn_leg_composition::QuinnCurrentTerminalDrive::Pending),
    ));
    tokio::time::timeout(TEST_TIMEOUT, physical_cleanup.wait_until_blocked())
        .await
        .expect("fatal tail terminal publishes before its physical close tail");

    let mut shutdown = Box::pin(composition.join_all_for_shutdown());
    tokio::select! {
        biased;
        outcome = &mut shutdown => {
            panic!("fatal-tail shutdown bypassed physical cleanup: {:?}", outcome.into_parts())
        }
        () = tokio::task::yield_now() => {}
    }
    physical_cleanup.release();
    let (outcome, server_a_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(shutdown, server_a.join())
    })
    .await
    .expect("fatal-tail structural shutdown remains bounded");
    let (client_joined, first_error) = outcome.into_parts();
    let client_joined = client_joined.expect("fatal tail cannot skip structural join");
    assert_eq!(client_joined.owned_messages(), 0);
    assert_eq!(client_joined.owned_bytes(), 0);
    assert!(client_joined.terminal_failure());
    assert!(
        !matches!(
            &first_error,
            Some(QuinnCurrentJoinError::RecoveryTailSettlement(_)),
        ),
        "hard failure cannot be rewritten as a recovery-tail settlement error",
    );
    assert_eq!(
        server_a_joined
            .expect("fatal-tail retired-A peer joins structurally")
            .owned_messages(),
        0,
    );

    let (repeated_joined, repeated_error) = composition.join_all_for_shutdown().await.into_parts();
    assert_eq!(
        repeated_joined.expect("fatal joined repeat retains structural inventory"),
        client_joined,
    );
    assert_eq!(
        format!("{repeated_error:?}"),
        format!("{first_error:?}"),
        "fatal joined repeat preserves the same sticky semantic observation",
    );
    drop((composition, local_flows, peer_policy));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("fatal-tail listener cleanup is bounded")
        .expect("fatal-tail actors release both listener slots");
}

#[tokio::test(flavor = "current_thread")]
async fn fatal_tail_prepublication_shutdown_cancellation_retries_with_exact_cause() {
    let RealRegisteredReplacementFixture {
        mut listener,
        mut composition,
        mut server_a,
        mut server_b,
        started_b,
        _tcp_factory,
        local_flows,
        peer_policy,
        _owner_executor,
        _owner_active_a,
        _standby_slot,
    } = real_registered_replacement_fixture(2, 1).await;
    wait_actor_ordered_delivery(&mut server_a).await;
    drive_replacement_to_recovery_tail(&mut composition, started_b);
    assert_eq!(
        composition.recovery_tail_ownership_for_test(),
        ClientRecoveryTailOwnership::Active,
    );

    // Pause the current-B root after its child has committed the canonical
    // first failure, but before the actor can publish its exact terminal.
    let terminal_prepublication = composition.arm_current_join_block_for_test();
    let progress_epoch = composition.current_actor_readiness_epoch_for_test();
    server_b.request_cancel();
    let server_b_joined = tokio::time::timeout(TEST_TIMEOUT, server_b.join())
        .await
        .expect("fatal-tail peer cleanup is bounded")
        .expect("fatal-tail peer joins structurally");
    assert_eq!(server_b_joined.owned_messages(), 0);
    assert_eq!(server_b_joined.owned_bytes(), 0);
    tokio::time::timeout(TEST_TIMEOUT, terminal_prepublication.wait_until_blocked())
        .await
        .expect("current B pauses before terminal publication");
    let first_cause = tokio::time::timeout(
        TEST_TIMEOUT,
        composition.wait_for_current_actor_progress(progress_epoch),
    )
    .await
    .expect("fatal active tail wakes the exact current actor waiter")
    .expect_err("peer close commits one hard current-actor failure");
    let QuinnLegActorError::RootFailed(first_cause) = first_cause else {
        panic!("fatal active tail lost its exact first cause: {first_cause:?}");
    };
    assert!(matches!(
        composition.try_drive_current_terminal(),
        Ok(crate::owned_upstream::quinn_leg_composition::QuinnCurrentTerminalDrive::Pending),
    ));

    // The first observer reaches the actor join and is then cancelled. The
    // composition must retain every join/tail/terminal authority for retry.
    {
        let mut first_shutdown = Box::pin(composition.join_all_for_shutdown());
        tokio::select! {
            biased;
            outcome = &mut first_shutdown => {
                panic!("prepublication shutdown bypassed the actor gate: {:?}", outcome.into_parts())
            }
            () = tokio::task::yield_now() => {}
        }
    }
    assert!(matches!(
        composition.try_drive_current_terminal(),
        Err(crate::owned_upstream::quinn_leg_composition::QuinnCurrentTerminalError::SessionShuttingDown),
    ));

    terminal_prepublication.release();
    let (outcome, server_a_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(composition.join_all_for_shutdown(), server_a.join())
    })
    .await
    .expect("cancelled fatal-tail shutdown retry remains bounded");
    let (client_joined, first_error) = outcome.into_parts();
    let client_joined = client_joined.expect("retry retains the structural current witness");
    assert_eq!(client_joined.owned_messages(), 0);
    assert_eq!(client_joined.owned_bytes(), 0);
    assert_eq!(client_joined.owned_terminals(), 0);
    assert!(client_joined.terminal_failure());
    let Some(QuinnCurrentJoinError::Actor(QuinnLegActorError::RootFailed(shutdown_cause))) =
        first_error
    else {
        panic!("fatal-tail retry lost its exact hard cause: {first_error:?}");
    };
    assert!(std::sync::Arc::ptr_eq(&shutdown_cause, &first_cause));
    assert_eq!(
        server_a_joined
            .expect("fatal-tail retired-A peer joins structurally")
            .owned_messages(),
        0,
    );

    let (repeated_joined, repeated_error) = composition.join_all_for_shutdown().await.into_parts();
    assert_eq!(
        repeated_joined.expect("fatal joined repeat retains structural inventory"),
        client_joined,
    );
    let Some(QuinnCurrentJoinError::Actor(QuinnLegActorError::RootFailed(repeated_cause))) =
        repeated_error
    else {
        panic!("fatal joined repeat lost its sticky hard cause: {repeated_error:?}");
    };
    assert!(std::sync::Arc::ptr_eq(&repeated_cause, &first_cause));

    drop((composition, local_flows, peer_policy));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("fatal-tail listener cleanup is bounded")
        .expect("fatal-tail actors release both listener slots");
}

#[tokio::test(flavor = "current_thread")]
async fn fatal_tail_shutdown_settles_armed_probe_without_masking_the_exact_cause() {
    let RealRegisteredReplacementFixture {
        mut listener,
        mut composition,
        mut server_a,
        mut server_b,
        started_b,
        _tcp_factory,
        local_flows,
        peer_policy,
        _owner_executor,
        _owner_active_a,
        _standby_slot,
    } = real_registered_replacement_fixture(2, 1).await;
    wait_actor_ordered_delivery(&mut server_a).await;
    drive_replacement_to_recovery_tail(&mut composition, started_b);
    deliver_current_recovery_tail(&mut composition, &mut server_b).await;

    let deadline = arm_delivered_current_probe(&mut composition, &mut server_b).await;
    assert_eq!(
        composition.current_outbound_inventory_for_test(),
        Some((0, 0)),
        "the controller's armed token is the only remaining probe authority",
    );

    let terminal_prepublication = composition.arm_current_join_block_for_test();
    let progress_epoch = composition.current_actor_readiness_epoch_for_test();
    server_b.request_cancel();
    let server_b_joined = tokio::time::timeout(TEST_TIMEOUT, server_b.join())
        .await
        .expect("fatal-probe peer cleanup is bounded")
        .expect("fatal-probe peer joins structurally");
    assert_eq!(server_b_joined.owned_messages(), 0);
    tokio::time::timeout(TEST_TIMEOUT, terminal_prepublication.wait_until_blocked())
        .await
        .expect("current B pauses before exact terminal publication");
    let first_cause = tokio::time::timeout(
        TEST_TIMEOUT,
        composition.wait_for_current_actor_progress(progress_epoch),
    )
    .await
    .expect("fatal current B wakes the exact actor waiter")
    .expect_err("peer close commits one hard current-actor failure");
    let QuinnLegActorError::RootFailed(first_cause) = first_cause else {
        panic!("fatal armed-probe path lost its exact first cause: {first_cause:?}");
    };
    assert!(matches!(
        composition.try_drive_current_terminal(),
        Ok(crate::owned_upstream::quinn_leg_composition::QuinnCurrentTerminalDrive::Pending),
    ));
    terminal_prepublication.release();
    composition
        .retain_current_probe_scheduler_for_shutdown(QuinnCurrentProbeSchedulerAuthority::Armed(
            deadline,
        ))
        .expect("root synchronously transfers the exact armed scheduler owner");

    let (outcome, server_a_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(composition.join_all_for_shutdown(), server_a.join())
    })
    .await
    .expect("fatal armed-probe structural shutdown remains bounded");
    let controller = outcome.controller_ownership_ledger();
    let (joined, first_error) = outcome.into_parts();
    let joined = joined.expect("fatal armed-probe path retains structural inventory");
    assert_eq!(joined.owned_messages(), 0);
    assert_eq!(joined.owned_bytes(), 0);
    assert_eq!(joined.owned_terminals(), 0);
    let Some(QuinnCurrentJoinError::Actor(QuinnLegActorError::RootFailed(shutdown_cause))) =
        first_error
    else {
        panic!("fatal armed-probe shutdown lost its exact hard cause: {first_error:?}");
    };
    assert!(Arc::ptr_eq(&shutdown_cause, &first_cause));
    assert!(controller.live_is_zero());
    assert_eq!(controller.settled_observations(), 1);
    assert_eq!(controller.settled_probe_deadlines(), 1);
    let settlement = controller
        .last_settlement()
        .expect("shutdown outcome exposes the armed liveness settlement");
    assert_eq!(
        settlement.source(),
        ClientControllerObservationSettlementSource::JoinedShutdown,
    );
    assert_eq!(
        settlement.probe_phase(),
        crate::owned_upstream::client_controller::ClientControllerShutdownProbePhase::Armed,
    );
    assert!(!settlement.probe_receipt());
    assert!(!settlement.probe_receipt_delivered());
    assert!(!settlement.application_ack());
    assert_eq!(settlement.application_ack_replay_ranges(), 0);
    assert_eq!(settlement.application_ack_replay_bytes(), 0);
    assert_eq!(
        server_a_joined
            .expect("fatal-probe retired A peer joins structurally")
            .owned_messages(),
        0,
    );

    drop((composition, local_flows, peer_policy));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("fatal-probe listener cleanup is bounded")
        .expect("fatal-probe actors release both listener slots");
}

#[tokio::test(flavor = "current_thread")]
async fn prebound_fatal_terminal_live_settles_armed_probe_and_scheduler_deadline_exactly_once() {
    let RealRegisteredReplacementFixture {
        mut listener,
        mut composition,
        mut server_a,
        mut server_b,
        started_b,
        _tcp_factory,
        local_flows,
        peer_policy,
        _owner_executor,
        _owner_active_a,
        _standby_slot,
    } = real_registered_replacement_fixture(2, 1).await;
    wait_actor_ordered_delivery(&mut server_a).await;
    drive_replacement_to_recovery_tail(&mut composition, started_b);
    deliver_current_recovery_tail(&mut composition, &mut server_b).await;
    let deadline = arm_delivered_current_probe(&mut composition, &mut server_b).await;

    let progress_epoch = composition.current_actor_readiness_epoch_for_test();
    let physical_cleanup = composition.arm_current_post_terminal_cleanup_block_for_test();
    server_b.request_cancel();
    let server_b_joined = tokio::time::timeout(TEST_TIMEOUT, server_b.join())
        .await
        .expect("prebound fatal peer cleanup is bounded")
        .expect("prebound fatal peer joins structurally");
    assert_eq!(server_b_joined.owned_messages(), 0);
    let first_cause = tokio::time::timeout(
        TEST_TIMEOUT,
        composition.wait_for_current_actor_progress(progress_epoch),
    )
    .await
    .expect("prebound fatal current B wakes the actor waiter")
    .expect_err("peer close commits one hard current-actor failure");
    let QuinnLegActorError::RootFailed(first_cause) = first_cause else {
        panic!("prebound armed-probe path lost its exact hard cause: {first_cause:?}");
    };
    assert!(matches!(
        composition.try_drive_current_terminal(),
        Ok(crate::owned_upstream::quinn_leg_composition::QuinnCurrentTerminalDrive::Pending),
    ));
    tokio::time::timeout(TEST_TIMEOUT, physical_cleanup.wait_until_blocked())
        .await
        .expect("prebound fatal terminal publishes before physical cleanup");
    assert!(matches!(
        composition.try_drive_current_terminal(),
        Ok(crate::owned_upstream::quinn_leg_composition::QuinnCurrentTerminalDrive::Applied(_)),
    ));
    let (pending_generation, _pending_sequence) = composition
        .pending_current_observation_probe_key()
        .expect("applied terminal exposes the copy-only scheduler selector");
    assert_eq!(
        pending_generation,
        composition.snapshot().session.generation()
    );
    let live_settlement = composition
        .settle_pending_current_observation(QuinnCurrentProbeSchedulerAuthority::Armed(deadline))
        .expect("root returns the exact armed scheduler owner before replacement or shutdown");
    assert_eq!(
        live_settlement.source(),
        ClientControllerObservationSettlementSource::CurrentTerminal,
    );
    assert!(
        composition
            .pending_current_observation_probe_key()
            .is_none()
    );
    physical_cleanup.release();

    let (outcome, server_a_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(composition.join_all_for_shutdown(), server_a.join())
    })
    .await
    .expect("prebound fatal armed-probe structural shutdown remains bounded");
    let controller = outcome.controller_ownership_ledger();
    let (joined, first_error) = outcome.into_parts();
    let joined = joined.expect("prebound fatal path retains structural inventory");
    assert_eq!(joined.owned_messages(), 0);
    assert_eq!(joined.owned_bytes(), 0);
    assert_eq!(joined.owned_terminals(), 0);
    assert!(joined.terminal_failure());
    assert!(
        first_error.is_none(),
        "terminal was already applied: {first_error:?}"
    );
    assert!(controller.live_is_zero());
    assert_eq!(controller.live_current_transport_owners(), 0);
    assert_eq!(controller.live_terminal_bound_authorities(), 0);
    assert_eq!(controller.live_pending_loss_authorities(), 0);
    assert_eq!(controller.settled_observations(), 1);
    assert_eq!(controller.settled_probe_deadlines(), 1);
    let settlement = controller
        .last_settlement()
        .expect("prebound terminal settlement is root-visible");
    assert_eq!(
        settlement.source(),
        ClientControllerObservationSettlementSource::CurrentTerminal,
    );
    assert_eq!(
        settlement.probe_phase(),
        crate::owned_upstream::client_controller::ClientControllerShutdownProbePhase::Armed,
    );
    assert!(!settlement.application_ack());
    assert_eq!(
        server_a_joined
            .expect("prebound fatal retired A peer joins structurally")
            .owned_messages(),
        0,
    );
    drop((first_cause, composition, local_flows, peer_policy));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("prebound fatal listener cleanup is bounded")
        .expect("prebound fatal actors release both listener slots");
}
