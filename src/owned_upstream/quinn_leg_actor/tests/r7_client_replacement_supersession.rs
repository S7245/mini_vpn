use super::*;

use crate::owned_upstream::quinn_leg_composition::QuinnCurrentProbeSchedulerAuthority;
use crate::owned_upstream::standby::{ExactLegProbeResponder, ExactLegProbeResponse};

fn drive_replacement_to_tail(
    composition: &mut QuinnClientSessionComposition,
    started: ClientRecoveryStarted,
) {
    assert!(matches!(
        composition.drive_recovery_one(),
        ClientRecoveryDrive::Progress(ClientRecoveryObservation::LegActivated { generation })
            if generation == started.generation
    ));
    loop {
        match composition.drive_recovery_one() {
            ClientRecoveryDrive::Progress(ClientRecoveryObservation::FrameAdmitted) => {}
            ClientRecoveryDrive::AdmissionCompleted { generation } => {
                assert_eq!(generation, started.generation);
                break;
            }
            outcome => panic!("replacement did not retain its exact recovery cursor: {outcome:?}"),
        }
    }
}

async fn deliver_replacement_tail(
    composition: &mut QuinnClientSessionComposition,
    peer: &mut StartedQuinnLegActor,
) {
    loop {
        match composition
            .try_flush_current(SimTime::ZERO)
            .expect("replacement recovery flush remains live")
        {
            crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition::Submitted => {
                let inbound = tokio::time::timeout(
                    TEST_TIMEOUT,
                    peer.recv_classified(STANDBY_CONTROL_V1),
                )
                .await
                .expect("replacement recovery reaches B within the local bound")
                .expect("replacement peer remains live")
                .expect("replacement publishes one exact frame");
                drop(take_actor_inbound(inbound));
            }
            crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition::OrderedDeliveryPending => {
                tokio::task::yield_now().await;
            }
            crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition::Idle => {
                break;
            }
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn normal_replacement_audits_unflushed_current_fifo_before_b_publication() {
    let RealAcceptedReplacementFixture {
        mut listener,
        mut composition,
        mut server_a,
        client_b,
        mut server_b,
        accepted_b,
        tcp_factory: _tcp_factory,
        local_flows: _local_flows,
        peer_policy: _peer_policy,
        owner_executor: _owner_executor,
        owner_active_a: _owner_active_a,
        owner_active_b: _owner_active_b,
        standby_slot: _standby_slot,
    } = real_accepted_replacement_fixture(2, 0).await;
    wait_actor_ordered_delivery(&mut server_a).await;

    let generation_a = composition.snapshot().session.generation();
    let effects = composition
        .open_local(TargetAddr::IpPort(
            "127.0.0.1:9443".parse().expect("loopback target"),
        ))
        .expect("current A remains installed")
        .expect("local OPEN is reducer-valid");
    let mut queued = None;
    for effect in effects {
        match effect {
            SessionEffect::LocalFlowOpened { .. } => {}
            SessionEffect::Transmit(frame) => {
                assert!(queued.replace(frame).is_none());
            }
            other => panic!("unexpected local-open effect: {other:?}"),
        }
    }
    let queued = queued.expect("local OPEN emits one exact frame");
    let queued_bytes = queued
        .encode()
        .expect("reducer-derived OPEN remains encodable")
        .len();
    composition
        .enqueue_current_frame(queued)
        .expect("current A admits one unflushed OPEN");
    assert_eq!(
        composition.current_outbound_inventory_for_test(),
        Some((1, queued_bytes)),
    );

    let started_b = composition
        .begin_registered_replacement(accepted_b, client_b)
        .expect("exact B commits the normal replacement");
    assert_eq!(
        composition.retired_supersession_for_test(),
        Some((generation_a, started_b.generation, 1, queued_bytes)),
        "A's exact unflushed FIFO must become explicit historical supersession evidence",
    );

    drive_replacement_to_tail(&mut composition, started_b);
    deliver_replacement_tail(&mut composition, &mut server_b).await;
    assert!(matches!(
        composition.settle_completed_recovery_tail(),
        Ok(crate::owned_upstream::client_controller::ClientRecoveryTailSettlement::Settled {
            generation
        }) if generation == started_b.generation
    ));
    let (shutdown, server_a_joined, server_b_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(
            composition.join_all_for_shutdown(),
            server_a.join(),
            server_b.join(),
        )
    })
    .await
    .expect("supersession cleanup joins every physical actor");
    let (client_joined, first_error) = shutdown.into_parts();
    assert!(
        first_error.is_none(),
        "supersession shutdown returned {first_error:?}"
    );
    let client_joined = client_joined.expect("supersession shutdown retains a joined witness");
    assert_eq!(client_joined.owned_messages(), 0);
    assert_eq!(client_joined.owned_bytes(), 0);
    assert_eq!(client_joined.owned_terminals(), 0);
    assert!(!client_joined.terminal_failure());
    assert_eq!(
        server_a_joined
            .expect("retired A peer joins structurally")
            .owned_messages(),
        0,
    );
    assert_eq!(
        server_b_joined
            .expect("current B peer joins structurally")
            .owned_messages(),
        0,
    );
    assert_eq!(
        composition.retired_supersession_for_test(),
        Some((generation_a, started_b.generation, 1, queued_bytes)),
        "retired clear moves the bounded audit into checked history",
    );
    drop(composition);
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("supersession listener cleanup is bounded")
        .expect("supersession actors release both listener slots");
}

#[tokio::test(flavor = "current_thread")]
async fn normal_replacement_waits_for_exact_old_probe_settlement_before_retry() {
    let RealAcceptedReplacementFixture {
        mut listener,
        mut composition,
        mut server_a,
        client_b,
        mut server_b,
        accepted_b,
        tcp_factory: _tcp_factory,
        local_flows: _local_flows,
        peer_policy: _peer_policy,
        owner_executor: _owner_executor,
        owner_active_a,
        owner_active_b: _owner_active_b,
        standby_slot: _standby_slot,
    } = real_accepted_replacement_fixture(2, 0).await;
    wait_actor_ordered_delivery(&mut server_a).await;
    let generation_a = composition.snapshot().session.generation();

    let delayed_a_delivered = Arc::clone(&server_a.delivery_writer_gate);
    delayed_a_delivered.arm();
    let queued_a = composition
        .begin_current_probe()
        .expect("current A queues one exact ordered probe");
    let probe_generation = queued_a.generation();
    let probe_sequence = queued_a.probe_sequence();
    assert_eq!(
        composition
            .try_flush_current(SimTime::ZERO)
            .expect("current A probe flush remains live"),
        crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition::Submitted,
    );
    let inbound = tokio::time::timeout(TEST_TIMEOUT, server_a.recv_classified(STANDBY_CONTROL_V1))
        .await
        .expect("current A probe reaches its peer")
        .expect("current A peer remains live")
        .expect("current A publishes one probe");
    let responder =
        ExactLegProbeResponder::from_installed(owner_active_a.installed_leg_probe_source());
    let pending_ack = match responder.classify(take_actor_inbound(inbound)) {
        ExactLegProbeResponse::Pending(pending) => pending,
        ExactLegProbeResponse::Unhandled(_) => {
            panic!("owner A rejected its exact classified probe")
        }
    };
    tokio::time::timeout(TEST_TIMEOUT, delayed_a_delivered.wait_until_blocked())
        .await
        .expect("A DELIVERED is withheld after physical submission");
    assert_eq!(
        composition.current_outbound_inventory_for_test(),
        Some((0, 0)),
        "the exact old ownership is now the ordered wait, not a queued frame",
    );

    let rejected = match composition.begin_registered_replacement(accepted_b, client_b) {
        Ok(_) => panic!("replacement orphaned A's exact queued scheduler owner"),
        Err(failure) => failure,
    };
    assert_eq!(
        rejected.kind(),
        crate::owned_upstream::quinn_leg_composition::QuinnRegisteredReplacementErrorKind::Controller(
            ClientRecoveryBeginErrorKind::CurrentProbeSettlementPending,
        ),
    );
    let (accepted_b, client_b) = rejected.into_parts();
    assert_eq!(composition.snapshot().session.generation(), generation_a);
    assert!(composition.retired_supersession_for_test().is_none());

    delayed_a_delivered.release();
    tokio::time::timeout(TEST_TIMEOUT, async {
        while composition
            .current_actor_snapshot()
            .pending_delivery_count()
            != 0
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("late A DELIVERED clears only actor-private ownership");
    let deadline = composition
        .arm_current_probe(queued_a, SimTime::ZERO, Duration::from_millis(50))
        .expect("the exact queued scheduler owner remains valid after rejection");
    pending_ack
        .enqueue(
            server_a
                .queue
                .as_mut()
                .expect("owner A retains its exact outbound FIFO"),
        )
        .expect("owner A queues the exact contract-derived ACK");
    assert_eq!(
        server_a
            .try_flush(SimTime::ZERO)
            .expect("owner A ACK flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    let acked = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match composition.try_receive_current() {
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
    wait_actor_ordered_delivery(&mut server_a).await;
    composition
        .reconcile_current_probe_ack(
            QuinnCurrentProbeSchedulerAuthority::Armed(deadline),
            probe_generation,
            probe_sequence,
        )
        .expect("root returns the pointer-identical armed scheduler owner");
    assert_eq!(
        composition
            .try_flush_current(SimTime::ZERO)
            .expect("A observes its already-delivered probe receipt"),
        crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition::Idle,
    );

    let started_b = composition
        .begin_registered_replacement(accepted_b, client_b)
        .expect("the same B commits only after A probe settlement");
    assert_eq!(
        composition.retired_supersession_for_test(),
        Some((generation_a, started_b.generation, 0, 0)),
    );
    assert_eq!(
        composition.retired_supersession_detail_for_test(),
        Some((false, "Idle", false)),
    );

    drive_replacement_to_tail(&mut composition, started_b);
    deliver_replacement_tail(&mut composition, &mut server_b).await;
    assert!(composition.settle_completed_recovery_tail().is_ok());
    let (shutdown, server_a_joined, server_b_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(
            composition.join_all_for_shutdown(),
            server_a.join(),
            server_b.join(),
        )
    })
    .await
    .expect("probe supersession joins every actor");
    let (client_joined, first_error) = shutdown.into_parts();
    assert!(
        first_error.is_none(),
        "probe supersession returned {first_error:?}"
    );
    let client_joined = client_joined.expect("probe supersession retains joined inventory");
    assert_eq!(client_joined.owned_messages(), 0);
    assert_eq!(client_joined.owned_bytes(), 0);
    assert_eq!(client_joined.owned_terminals(), 0);
    assert_eq!(server_a_joined.expect("A peer joins").owned_messages(), 0,);
    assert_eq!(server_b_joined.expect("B peer joins").owned_messages(), 0,);
    assert_eq!(
        composition.retired_supersession_detail_for_test(),
        Some((false, "Idle", false)),
    );
    drop(composition);
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("probe-supersession listener cleanup is bounded")
        .expect("probe-supersession actors release both slots");
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NormalReplacementResetState {
    Queued,
    SubmittedWait,
    RootDeliveredBeforeAttempt,
    DeliveryCompletesBetweenSnapshotLoads,
    DeadlineWon,
}

async fn run_normal_replacement_reset_rebind(a_state: NormalReplacementResetState) {
    let RealAcceptedReplacementFixture {
        mut listener,
        mut composition,
        mut server_a,
        client_b,
        mut server_b,
        accepted_b,
        tcp_factory,
        local_flows,
        peer_policy: _peer_policy,
        owner_executor: _owner_executor,
        owner_active_a: _owner_active_a,
        owner_active_b: _owner_active_b,
        standby_slot: _standby_slot,
    } = real_accepted_replacement_fixture(2, 1).await;
    wait_actor_ordered_delivery(&mut server_a).await;
    let flow_id = local_flows[0].flow_id();
    let (source, mut driver) = tcp_factory
        .open_flow(local_flows[0])
        .expect("the exact local flow enters the typed source seam");
    source
        .try_send_reset(crate::resumable::ResetReason::LocalAbandon)
        .expect("the local source emits one exact RESET");
    let input = driver
        .try_recv_next_for_client_controller()
        .expect("the typed source remains live")
        .expect("the exact RESET input is ready");
    let effects = match composition
        .apply_driver_input(input)
        .expect("current A accepts the exact RESET input")
    {
        crate::owned_upstream::client_controller::ClientControllerDriverApply::Applied(effects) => {
            effects
        }
        crate::owned_upstream::client_controller::ClientControllerDriverApply::Deferred {
            ..
        } => panic!("small RESET unexpectedly met reducer pressure"),
        crate::owned_upstream::client_controller::ClientControllerDriverApply::Failed(error) => {
            panic!("small RESET unexpectedly failed the reducer: {error}")
        }
    };
    let mut reset = None;
    let mut terminal = None;
    for effect in effects {
        match effect {
            SessionEffect::Transmit(frame) if matches!(frame.record(), Record::Reset { flow_id: actual, .. } if *actual == flow_id) =>
            {
                assert!(reset.replace(frame).is_none());
            }
            SessionEffect::FlowFinished {
                terminal: candidate,
                ..
            } if candidate.flow_id() == flow_id => {
                assert!(terminal.replace(candidate).is_none());
            }
            _ => {}
        }
    }
    let reset = reset.expect("local RESET emits one exact wire frame");
    let reset_bytes = reset
        .encode()
        .expect("reducer-derived RESET remains encodable")
        .len();
    let terminal = terminal.expect("local RESET emits one exact terminal timer authority");
    let deadline = SimTime::ZERO
        .checked_add(Duration::from_millis(500))
        .expect("bounded reset-delivery deadline");
    let timer = QuinnTerminalGraceTimer::new_with_ordered_reset_delivery(terminal, deadline);
    let reset_delivery = timer
        .reset_delivery_for_queue(flow_id)
        .expect("timer lends its pointer-identical RESET delivery authority");
    composition
        .enqueue_current_reset_frame(reset, reset_delivery)
        .expect("current A retains the exact RESET without flushing it");
    assert_eq!(
        composition.current_outbound_inventory_for_test(),
        Some((1, reset_bytes)),
    );

    let delayed_a_delivered = matches!(
        a_state,
        NormalReplacementResetState::SubmittedWait
            | NormalReplacementResetState::RootDeliveredBeforeAttempt
            | NormalReplacementResetState::DeliveryCompletesBetweenSnapshotLoads
    )
    .then(|| {
        let gate = Arc::clone(&server_a.delivery_writer_gate);
        gate.arm();
        gate
    });
    if let Some(delayed_a_delivered) = &delayed_a_delivered {
        assert_eq!(
            composition
                .try_flush_current(SimTime::ZERO)
                .expect("A RESET flush remains live"),
            crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition::Submitted,
        );
        let inbound =
            tokio::time::timeout(TEST_TIMEOUT, server_a.recv_classified(STANDBY_CONTROL_V1))
                .await
                .expect("A RESET reaches its peer within the local bound")
                .expect("A peer remains live")
                .expect("A publishes the exact RESET");
        let BoundInbound::Session(inbound) = take_actor_inbound(inbound) else {
            panic!("A RESET was misclassified as leg control");
        };
        assert!(matches!(
            inbound.into_frame_for_test().record(),
            Record::Reset { flow_id: actual, .. } if actual == &flow_id
        ));
        tokio::time::timeout(TEST_TIMEOUT, delayed_a_delivered.wait_until_blocked())
            .await
            .expect("A peer holds the exact physical DELIVERED publication");
        assert_eq!(
            composition.current_outbound_inventory_for_test(),
            Some((0, 0)),
            "submitted A ownership moves from its FIFO into the exact ordered wait",
        );
        assert!(timer.reset_delivery_pending());
    }

    if a_state == NormalReplacementResetState::RootDeliveredBeforeAttempt {
        assert!(
            composition.force_submitted_reset_root_delivered_before_attempt_for_test(),
            "the tracer linearizes the shared root decision while A's exact physical wait remains pending",
        );
        assert!(
            !timer.reset_delivery_pending(),
            "the root RESET decision is Delivered before the per-attempt child publication",
        );
        assert!(
            composition
                .current_actor_snapshot()
                .pending_delivery_count()
                > 0,
            "A still owns the exact lagging physical completion",
        );
    }

    if a_state == NormalReplacementResetState::DeadlineWon {
        assert_eq!(
            timer.try_commit_reset_delivery_deadline(),
            crate::owned_upstream::leg_io::OrderedResetDeadlineCommit::DeadlineWon,
        );
        let candidate_before = client_b.actor_snapshot();
        let failure = composition
            .begin_registered_replacement(accepted_b, client_b)
            .expect_err("a won RESET deadline rejects B before reducer commit");
        assert!(matches!(
            failure.kind(),
            crate::owned_upstream::quinn_leg_composition::QuinnRegisteredReplacementErrorKind::Controller(
                crate::owned_upstream::client_controller::ClientRecoveryBeginErrorKind::InstallRejected,
            ),
        ));
        let (accepted_b, mut client_b) = failure.into_parts();
        assert_eq!(
            client_b.actor_snapshot(),
            candidate_before,
            "precommit rejection returns B's exact physical ownership unchanged",
        );
        assert_eq!(
            composition.current_outbound_inventory_for_test(),
            Some((1, reset_bytes)),
            "precommit rejection restores A's exact queued RESET",
        );
        assert_eq!(
            timer.try_commit_reset_delivery_deadline(),
            crate::owned_upstream::leg_io::OrderedResetDeadlineCommit::DeadlineWon,
            "rejected B cannot rewrite the root deadline winner",
        );

        assert_eq!(
            composition
                .try_flush_current(SimTime::ZERO)
                .expect("restored A RESET remains physically deliverable"),
            crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition::Submitted,
        );
        let inbound =
            tokio::time::timeout(TEST_TIMEOUT, server_a.recv_classified(STANDBY_CONTROL_V1))
                .await
                .expect("restored A RESET reaches its peer")
                .expect("A peer remains live")
                .expect("restored A publishes its exact RESET");
        let BoundInbound::Session(inbound) = take_actor_inbound(inbound) else {
            panic!("restored A RESET was misclassified as leg control");
        };
        assert!(matches!(
            inbound.into_frame_for_test().record(),
            Record::Reset { flow_id: actual, .. } if actual == &flow_id
        ));
        tokio::time::timeout(TEST_TIMEOUT, async {
            loop {
                match composition
                    .try_flush_current(SimTime::ZERO)
                    .expect("restored A delivery progress remains live")
                {
                    crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition::Idle => break,
                    crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition::Submitted
                    | crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition::OrderedDeliveryPending => {
                        tokio::task::yield_now().await;
                    }
                }
            }
        })
        .await
        .expect("restored A per-attempt delivery settles independently");
        assert_eq!(
            timer.try_commit_reset_delivery_deadline(),
            crate::owned_upstream::leg_io::OrderedResetDeadlineCommit::DeadlineWon,
            "late A delivery cannot overwrite the deadline winner",
        );
        assert!(composition.close_root_flow_admission());
        match composition.force_expire_terminal_grace_for_root_shutdown(timer) {
            QuinnTerminalGraceTimerDrive::Expired { effects } => assert!(effects.is_empty()),
            _ => panic!("root shutdown did not consume the exact won-deadline timer"),
        }
        assert_eq!(composition.snapshot().session.terminal_tombstones(), 0);

        drop(accepted_b);
        client_b.request_cancel();
        let (shutdown, server_a_joined, client_b_joined, server_b_joined) =
            tokio::time::timeout(TEST_TIMEOUT, async {
                tokio::join!(
                    composition.join_all_for_shutdown(),
                    server_a.join(),
                    client_b.join(),
                    server_b.join(),
                )
            })
            .await
            .expect("deadline-rejection cleanup remains bounded");
        let (joined, first_error) = shutdown.into_parts();
        assert!(first_error.is_none(), "A shutdown returned {first_error:?}");
        let joined = joined.expect("A joins structurally after timer settlement");
        assert_eq!(joined.owned_messages(), 0);
        assert_eq!(joined.owned_bytes(), 0);
        assert_eq!(joined.owned_terminals(), 0);
        assert_eq!(server_a_joined.expect("A peer joins").owned_messages(), 0);
        assert_eq!(
            client_b_joined.expect("rejected B joins").owned_messages(),
            0
        );
        assert_eq!(server_b_joined.expect("B peer joins").owned_messages(), 0);
        drop((composition, source, driver));
        tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
            .await
            .expect("deadline-rejection listener cleanup is bounded")
            .expect("deadline-rejection actors release both listener slots");
        return;
    }

    let (mut composition, started_b) = if a_state
        == NormalReplacementResetState::DeliveryCompletesBetweenSnapshotLoads
    {
        let snapshot_gate =
            Arc::new(crate::owned_upstream::leg_io::ClientResetRebindSnapshotGate::new());
        let snapshot_observer = Arc::clone(&snapshot_gate);
        let delayed_a_delivered = Arc::clone(
            delayed_a_delivered
                .as_ref()
                .expect("the interleaving tracer holds A's real delivery child"),
        );
        let replacement = tokio::task::spawn_blocking(move || {
            crate::owned_upstream::leg_io::with_client_reset_rebind_snapshot_gate_for_test(
                snapshot_gate,
                || {
                    let started = composition
                            .begin_registered_replacement(accepted_b, client_b)
                            .expect(
                                "a delivery completing between snapshot loads remains a valid replacement race",
                            );
                    (composition, started)
                },
            )
        });
        tokio::time::timeout(TEST_TIMEOUT, async {
            while !snapshot_observer.first_load_reached() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("replacement preflight reaches the deterministic snapshot seam");
        delayed_a_delivered.release();
        snapshot_observer.release_after_delivery();
        tokio::time::timeout(TEST_TIMEOUT, replacement)
            .await
            .expect("the interleaved replacement remains bounded")
            .expect("the blocking replacement task remains live")
    } else {
        let started = composition
            .begin_registered_replacement(accepted_b, client_b)
            .expect("B replacement atomically rebinds the pending RESET delivery");
        (composition, started)
    };
    if a_state == NormalReplacementResetState::DeliveryCompletesBetweenSnapshotLoads {
        assert_eq!(
            timer.try_commit_reset_delivery_deadline(),
            crate::owned_upstream::leg_io::OrderedResetDeadlineCommit::Delivered,
            "second-pass arbitration must preserve A's concurrent root-delivery win",
        );
    }
    drive_replacement_to_tail(&mut composition, started_b);
    if matches!(
        a_state,
        NormalReplacementResetState::RootDeliveredBeforeAttempt
            | NormalReplacementResetState::DeliveryCompletesBetweenSnapshotLoads
    ) {
        deliver_replacement_tail(&mut composition, &mut server_b).await;
        composition
            .settle_completed_recovery_tail()
            .expect("B recovery tail settles without rebinding an already-delivered root RESET");
        assert_eq!(
            timer.try_commit_reset_delivery_deadline(),
            crate::owned_upstream::leg_io::OrderedResetDeadlineCommit::Delivered,
            "the exact A delivery remains the root winner across replacement",
        );
        match composition.try_receive_current_before_terminal_grace(timer, deadline) {
            QuinnTerminalGraceTimerDrive::Expired { effects } => assert!(effects.is_empty()),
            _ => panic!("the already-delivered root RESET did not retire its exact timer"),
        }
    } else {
        assert!(
            timer.reset_delivery_pending(),
            "precommit rebind must not forge a Delivered root decision",
        );

        let delayed_b_delivered = Arc::clone(&server_b.delivery_writer_gate);
        delayed_b_delivered.arm();
        assert_eq!(
            composition
                .try_flush_current(SimTime::ZERO)
                .expect("B recovery RESET flush remains live"),
            crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition::Submitted,
        );
        let inbound =
            tokio::time::timeout(TEST_TIMEOUT, server_b.recv_classified(STANDBY_CONTROL_V1))
                .await
                .expect("B recovery RESET reaches its peer within the local bound")
                .expect("B peer remains live")
                .expect("B recovery publishes the exact RESET");
        let BoundInbound::Session(inbound) = take_actor_inbound(inbound) else {
            panic!("B recovery RESET was misclassified as leg control");
        };
        let delivered_frame = inbound.into_frame_for_test();
        assert!(matches!(
            delivered_frame.record(),
            Record::Reset { flow_id: actual, .. } if actual == &flow_id
        ));
        tokio::time::timeout(TEST_TIMEOUT, delayed_b_delivered.wait_until_blocked())
            .await
            .expect("B peer holds the exact physical DELIVERED publication");
        assert!(
            timer.reset_delivery_pending(),
            "peer receipt alone cannot satisfy the timer before B's exact DELIVERED completion",
        );
        delayed_b_delivered.release();
        tokio::time::timeout(TEST_TIMEOUT, async {
            while timer.reset_delivery_pending() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("B's exact DELIVERED completion reaches the shared root state");
        deliver_replacement_tail(&mut composition, &mut server_b).await;
        composition
            .settle_completed_recovery_tail()
            .expect("B recovery tail settles after exact delivery");
        assert_eq!(
            timer.try_commit_reset_delivery_deadline(),
            crate::owned_upstream::leg_io::OrderedResetDeadlineCommit::Delivered,
            "B's recovery RESET must satisfy the same timer authority that A originally held",
        );
        match composition.try_receive_current_before_terminal_grace(timer, deadline) {
            QuinnTerminalGraceTimerDrive::Expired { effects } => assert!(effects.is_empty()),
            _ => panic!("delivered replacement RESET did not retire its exact terminal timer"),
        }
    }
    assert_eq!(composition.snapshot().session.terminal_tombstones(), 0);

    if let Some(delayed_a_delivered) = delayed_a_delivered {
        delayed_a_delivered.release();
        tokio::time::timeout(TEST_TIMEOUT, async {
            while composition
                .retired_actor_snapshot()
                .expect("A remains the exact retired physical actor")
                .pending_delivery_count()
                != 0
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("late A DELIVERED drains only A's per-attempt actor ownership");
    }

    let (shutdown, server_a_joined, server_b_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(
            composition.join_all_for_shutdown(),
            server_a.join(),
            server_b.join(),
        )
    })
    .await
    .expect("reset-rebind cleanup remains bounded");
    let (joined, first_error) = shutdown.into_parts();
    assert!(
        first_error.is_none(),
        "reset rebind returned {first_error:?}"
    );
    let joined = joined.expect("reset rebind joins every client actor");
    assert_eq!(joined.owned_messages(), 0);
    assert_eq!(joined.owned_bytes(), 0);
    assert_eq!(joined.owned_terminals(), 0);
    assert_eq!(server_a_joined.expect("A peer joins").owned_messages(), 0);
    assert_eq!(server_b_joined.expect("B peer joins").owned_messages(), 0);
    drop((composition, source, driver));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("reset-rebind listener cleanup is bounded")
        .expect("reset-rebind actors release both listener slots");
}

#[tokio::test(flavor = "current_thread")]
async fn normal_replacement_rebinds_unflushed_reset_delivery_to_b_recovery() {
    run_normal_replacement_reset_rebind(NormalReplacementResetState::Queued).await;
}

#[tokio::test(flavor = "current_thread")]
async fn normal_replacement_rebinds_submitted_reset_wait_to_b_recovery() {
    run_normal_replacement_reset_rebind(NormalReplacementResetState::SubmittedWait).await;
}

#[tokio::test(flavor = "current_thread")]
async fn normal_replacement_accepts_root_delivered_reset_while_attempt_completion_lags() {
    run_normal_replacement_reset_rebind(NormalReplacementResetState::RootDeliveredBeforeAttempt)
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn normal_replacement_accepts_delivery_completing_between_reset_snapshot_loads() {
    run_normal_replacement_reset_rebind(
        NormalReplacementResetState::DeliveryCompletesBetweenSnapshotLoads,
    )
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn single_shot_shutdown_settles_pending_replacement_ack_without_forging_replay_release() {
    let RealAcceptedReplacementFixture {
        mut listener,
        mut composition,
        mut server_a,
        client_b,
        mut server_b,
        accepted_b,
        tcp_factory,
        local_flows,
        peer_policy: _peer_policy,
        owner_executor: _owner_executor,
        owner_active_a: _owner_active_a,
        owner_active_b: _owner_active_b,
        standby_slot: _standby_slot,
    } = real_accepted_replacement_fixture(2, 1).await;
    wait_actor_ordered_delivery(&mut server_a).await;

    let (source, mut driver) = tcp_factory
        .open_flow(local_flows[0])
        .expect("the exact local flow enters the typed source seam");
    source
        .try_send_uplink_with(1, || Bytes::from_static(b"x"))
        .expect("one pending replay byte is reserved");
    let input = driver
        .try_recv_next_for_client_controller()
        .expect("typed source dequeue remains live")
        .expect("one exact DATA input is ready");
    assert!(matches!(
        composition
            .apply_driver_input(input)
            .expect("current A accepts the exact replay input"),
        crate::owned_upstream::client_controller::ClientControllerDriverApply::Applied(_)
    ));
    assert_eq!(composition.snapshot().uplink_replay_owned_bytes, 1);

    let started_b = composition
        .begin_registered_replacement(accepted_b, client_b)
        .expect("exact B commits while one replacement application-ACK remains outstanding");
    drive_replacement_to_tail(&mut composition, started_b);
    deliver_replacement_tail(&mut composition, &mut server_b).await;
    composition
        .settle_completed_recovery_tail()
        .expect("B's exact physical recovery tail settles before shutdown");
    assert_eq!(
        composition.snapshot().uplink_replay_owned_bytes,
        1,
        "physical recovery delivery is not an application ACK",
    );

    let (outcome, server_a_joined, server_b_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(
            composition.join_all_for_shutdown(),
            server_a.join(),
            server_b.join(),
        )
    })
    .await
    .expect("single-shot pending-ACK shutdown joins both physical legs");
    let (joined, first_error) = outcome.into_parts();
    assert!(
        first_error.is_none(),
        "controller settlement must complete before retired-A clear: {first_error:?}",
    );
    let joined = joined.expect("single-shot shutdown retains the checked actor inventory");
    assert_eq!(joined.owned_messages(), 0);
    assert_eq!(joined.owned_bytes(), 0);
    assert_eq!(joined.owned_terminals(), 0);
    assert_eq!(server_a_joined.expect("A peer joins").owned_messages(), 0);
    assert_eq!(server_b_joined.expect("B peer joins").owned_messages(), 0);

    let settlement = composition
        .controller_shutdown_settlement_for_test()
        .expect("single-shot shutdown retains a copy-only ACK abandonment audit");
    assert_eq!(
        settlement.probe_phase(),
        crate::owned_upstream::client_controller::ClientControllerShutdownProbePhase::Idle,
    );
    assert_eq!(settlement.probe_generation(), started_b.generation);
    assert_eq!(settlement.probe_switch_epoch(), started_b.generation.get(),);
    assert_eq!(settlement.probe_sequence(), None);
    assert_eq!(
        settlement.application_ack_generation(),
        Some(started_b.generation),
    );
    assert!(settlement.application_ack());
    assert_eq!(settlement.application_ack_replay_ranges(), 1);
    assert_eq!(settlement.application_ack_replay_bytes(), 1);
    assert_eq!(
        composition.snapshot().uplink_replay_owned_bytes,
        1,
        "settling the observation authority cannot forge reducer ACK progress",
    );

    let (repeat_joined, repeat_error) = composition.join_all_for_shutdown().await.into_parts();
    assert_eq!(repeat_joined, Some(joined));
    assert!(repeat_error.is_none());
    assert_eq!(
        composition
            .controller_shutdown_settlement_for_test()
            .expect("repeat shutdown preserves the one exact audit"),
        settlement,
    );
    drop((composition, source, driver, tcp_factory, local_flows));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("pending-ACK listener cleanup is bounded")
        .expect("pending-ACK actors release both listener slots");
}

#[tokio::test(flavor = "current_thread")]
async fn won_reset_deadline_rejects_b_and_restores_a_before_reducer_commit() {
    run_normal_replacement_reset_rebind(NormalReplacementResetState::DeadlineWon).await;
}

#[tokio::test(flavor = "current_thread")]
async fn supersession_history_overflow_cannot_mint_a_false_zero_shutdown_witness() {
    let RealAcceptedReplacementFixture {
        mut listener,
        mut composition,
        mut server_a,
        client_b,
        mut server_b,
        accepted_b,
        tcp_factory: _tcp_factory,
        local_flows: _local_flows,
        peer_policy: _peer_policy,
        owner_executor: _owner_executor,
        owner_active_a: _owner_active_a,
        owner_active_b: _owner_active_b,
        standby_slot: _standby_slot,
    } = real_accepted_replacement_fixture(2, 0).await;
    wait_actor_ordered_delivery(&mut server_a).await;

    let effects = composition
        .open_local(TargetAddr::IpPort(
            "127.0.0.1:9444".parse().expect("loopback target"),
        ))
        .expect("current A remains installed")
        .expect("local OPEN is reducer-valid");
    for effect in effects {
        match effect {
            SessionEffect::LocalFlowOpened { .. } => {}
            SessionEffect::Transmit(frame) => composition
                .enqueue_current_frame(frame)
                .expect("current A retains one audited frame"),
            other => panic!("unexpected local-open effect: {other:?}"),
        }
    }
    let started_b = composition
        .begin_registered_replacement(accepted_b, client_b)
        .expect("exact B commits the normal replacement");
    composition.force_supersession_history_replacements_for_test(usize::MAX);

    drive_replacement_to_tail(&mut composition, started_b);
    deliver_replacement_tail(&mut composition, &mut server_b).await;
    composition
        .settle_completed_recovery_tail()
        .expect("replacement tail settles before structural shutdown");

    let (shutdown, server_a_joined, server_b_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(
            composition.join_all_for_shutdown(),
            server_a.join(),
            server_b.join(),
        )
    })
    .await
    .expect("overflow cleanup remains bounded");
    let (joined, first_error) = shutdown.into_parts();
    assert!(
        matches!(
            first_error,
            Some(QuinnCurrentJoinError::RetiredController(
                crate::owned_upstream::quinn_leg_composition::QuinnRetiredControllerClearErrorKind::SupersessionHistoryOverflow,
            )),
        ),
        "overflow must remain an explicit retired-controller failure: {first_error:?}",
    );
    let joined = joined.expect("every actor still joins structurally");
    assert_eq!(joined.owned_messages(), 0);
    assert_eq!(joined.owned_bytes(), 0);
    assert_eq!(
        joined.owned_terminals(),
        1,
        "the retained retired terminal must remain visible beside the overflow",
    );
    assert!(
        !composition.shutdown_phase_is_joined_for_test(),
        "retained retired ownership cannot publish the composition Joined phase",
    );
    assert_eq!(server_a_joined.expect("A peer joins").owned_messages(), 0);
    assert_eq!(server_b_joined.expect("B peer joins").owned_messages(), 0);
    drop(composition);
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("overflow listener cleanup is bounded")
        .expect("overflow actors release both listener slots");
}
