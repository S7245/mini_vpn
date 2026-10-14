use super::*;

use crate::owned_upstream::client_controller::ClientCurrentProbeAckSettlement;
use crate::owned_upstream::quinn_leg_actor::{
    QuinnLegActorArrivalDeadline, QuinnLegActorInboundDeadlinePoll,
};
use crate::owned_upstream::quinn_leg_composition::{
    QuinnCurrentProbeDeadline, QuinnCurrentProbeDeadlineDrive, QuinnCurrentProbeSchedulerAuthority,
    QuinnLegCompositionGate,
};

struct ArmedCurrentProbeFixture {
    listener: QuinnLegActorListener,
    composition: QuinnClientSessionComposition,
    server: StartedQuinnLegActor,
    owner_bootstrap: OwnerSessionBootstrap,
    exact_ack: LegControlFrame,
    token: QuinnCurrentProbeDeadline,
    deadline: SimTime,
}

async fn armed_current_probe_fixture(identity_byte: u8) -> ArmedCurrentProbeFixture {
    let limits = composition_actor_limits();
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let peer_policy = AttachPeerPolicy::new(
        OwnerIdentity::new([identity_byte; 32]).expect("nonzero owner identity"),
        DevicePrincipal::new([identity_byte.wrapping_add(1); 16])
            .expect("nonzero device principal"),
    );
    let mut listener = QuinnLegActorListener::bind(
        QuinnLegActorServerConfig::new(
            "127.0.0.1:0".parse().expect("loopback listener address"),
            cert_dir.join("server-cert.pem"),
            cert_dir.join("server-key.pem"),
            1,
        )
        .expect("single-slot listener configuration"),
    )
    .await
    .expect("bind probe-ownership listener");
    let (mut client, mut server) =
        connect_real_actor_pair(&mut listener, LegId::A, limits, peer_policy).await;
    let (accepted, owner_bootstrap) = complete_initial_actor_attach(&mut client, &mut server).await;
    wait_actor_ordered_delivery(&mut server).await;
    let mut composition =
        QuinnClientSessionComposition::bootstrap_initial(actor_session_config(), accepted, client)
            .expect("composition consumes exact current A");

    let queued = composition
        .begin_current_probe()
        .expect("one exact current probe enters A's ordered FIFO");
    assert_eq!(
        composition
            .try_flush_current(SimTime::ZERO)
            .expect("current probe flush remains live"),
        crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition::Submitted,
    );
    let inbound_probe =
        tokio::time::timeout(TEST_TIMEOUT, server.recv_classified(STANDBY_CONTROL_V1))
            .await
            .expect("current probe receive is bounded")
            .expect("server A remains live")
            .expect("server A receives the exact probe");
    let BoundInbound::LegControl(inbound_probe) = take_actor_inbound(inbound_probe) else {
        panic!("current probe was misclassified as a session frame");
    };
    let (_probe_seal, probe) = inbound_probe.into_parts();
    let LegControlRecord::LegProbe {
        session_id,
        leg_epoch_nonce,
        probe_sequence,
    } = probe.record().clone()
    else {
        panic!("current liveness emitted a non-probe record");
    };
    assert_eq!(probe_sequence, queued.probe_sequence());
    let exact_ack = LegControlFrame::try_new(
        probe.leg_generation(),
        LegControlRecord::LegProbeAck {
            session_id,
            leg_epoch_nonce,
            probe_sequence,
        },
        STANDBY_CONTROL_V1,
    )
    .expect("matching current probe ACK is valid");

    wait_actor_ordered_delivery(&mut server).await;
    wait_composition_current_ordered_delivery(&mut composition).await;
    let deadline = SimTime::from_nanos(300_000_000);
    let token = tokio::time::timeout(TEST_TIMEOUT, async {
        let mut queued = queued;
        loop {
            match composition.arm_current_probe(
                queued,
                SimTime::ZERO,
                Duration::from_millis(300),
            ) {
                Ok(token) => break token,
                Err(crate::owned_upstream::quinn_leg_composition::QuinnCurrentProbeArmError::NotDelivered(returned)) => {
                    queued = returned;
                    tokio::task::yield_now().await;
                }
                Err(error) => panic!("current probe failed to arm: {error:?}"),
            }
        }
    })
    .await
    .expect("current probe arms after exact writer completion");
    assert_eq!(token.instant(), deadline);

    ArmedCurrentProbeFixture {
        listener,
        composition,
        server,
        owner_bootstrap,
        exact_ack,
        token,
        deadline,
    }
}

fn expect_non_consuming_deadline(
    outcome: QuinnCurrentProbeDeadlineDrive,
    expected: SimTime,
) -> QuinnCurrentProbeDeadline {
    let deadline = match outcome {
        QuinnCurrentProbeDeadlineDrive::Stale(deadline)
        | QuinnCurrentProbeDeadlineDrive::NotDue(deadline)
        | QuinnCurrentProbeDeadlineDrive::PublicationPending(deadline)
        | QuinnCurrentProbeDeadlineDrive::SessionShuttingDown(deadline)
        | QuinnCurrentProbeDeadlineDrive::CompositionInvariant(deadline) => deadline,
        QuinnCurrentProbeDeadlineDrive::Receive { deadline, .. }
        | QuinnCurrentProbeDeadlineDrive::ProbeAcked { deadline, .. }
        | QuinnCurrentProbeDeadlineDrive::ActorFailureWon { deadline, .. } => deadline,
        QuinnCurrentProbeDeadlineDrive::Committed { .. } => {
            panic!("committed current-probe expiry consumed deadline authority")
        }
    };
    assert_eq!(deadline.instant(), expected);
    deadline
}

fn reconcile_armed_ack(
    composition: &mut QuinnClientSessionComposition,
    outcome: QuinnCurrentProbeDeadlineDrive,
    expected: SimTime,
) -> ClientCurrentProbeAckSettlement {
    let QuinnCurrentProbeDeadlineDrive::ProbeAcked {
        deadline,
        generation,
        probe_sequence,
    } = outcome
    else {
        panic!("matching ACK did not return scheduler deadline authority");
    };
    assert_eq!(deadline.instant(), expected);
    composition
        .reconcile_current_probe_ack(
            QuinnCurrentProbeSchedulerAuthority::Armed(deadline),
            generation,
            probe_sequence,
        )
        .expect("the exact returned scheduler deadline reconciles its ACK winner")
}

#[tokio::test(flavor = "current_thread")]
async fn ordinary_receive_ack_winner_returns_the_original_scheduler_deadline() {
    let ArmedCurrentProbeFixture {
        mut listener,
        mut composition,
        mut server,
        owner_bootstrap,
        exact_ack,
        token,
        deadline,
    } = armed_current_probe_fixture(0xd5).await;

    server
        .enqueue_ordered_leg_control(exact_ack, STANDBY_CONTROL_V1)
        .expect("server A queues the matching current probe ACK");
    assert_eq!(
        server
            .try_flush(SimTime::ZERO)
            .expect("matching probe ACK flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    tokio::time::timeout(TEST_TIMEOUT, async {
        while composition
            .current_actor_snapshot()
            .queued_inbound_messages()
            != 1
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("matching probe ACK reaches the current actor queue");
    assert!(matches!(
        composition.try_receive_current(),
        QuinnCurrentInboundDrive::ProbeAcked { .. }
    ));
    wait_actor_ordered_delivery(&mut server).await;

    let acked = composition.try_receive_current_before_probe_deadline(token, deadline);
    let settlement = reconcile_armed_ack(&mut composition, acked, deadline);
    assert_eq!(
        settlement.generation(),
        composition.snapshot().session.generation()
    );

    let (client_joined, server_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(composition.join_current_for_shutdown(), server.join())
    })
    .await
    .expect("stale-token actors join within the local bound");
    let client_joined = client_joined.expect("stale-token client joins structurally");
    let server_joined = server_joined.expect("stale-token server joins structurally");
    assert_eq!(client_joined.owned_messages(), 0);
    assert_eq!(client_joined.owned_bytes(), 0);
    assert_eq!(server_joined.owned_messages(), 0);
    assert_eq!(server_joined.owned_bytes(), 0);
    drop((composition, owner_bootstrap));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("stale-token listener cleanup is bounded")
        .expect("stale-token actors release the listener slot");
}

#[tokio::test(flavor = "current_thread")]
async fn wrong_composition_identity_is_stale_and_returns_the_original_exact_deadline() {
    let ArmedCurrentProbeFixture {
        listener: mut listener_a,
        composition: mut composition_a,
        server: mut server_a,
        owner_bootstrap: owner_bootstrap_a,
        exact_ack: exact_ack_a,
        token: token_a,
        deadline: deadline_a,
    } = armed_current_probe_fixture(0xdd).await;
    let ArmedCurrentProbeFixture {
        listener: mut listener_b,
        composition: mut composition_b,
        server: mut server_b,
        owner_bootstrap: owner_bootstrap_b,
        exact_ack: exact_ack_b,
        token: token_b,
        deadline: deadline_b,
    } = armed_current_probe_fixture(0xdf).await;

    let wrong_identity =
        composition_b.try_receive_current_before_probe_deadline(token_a, deadline_a);
    let returned_a = match wrong_identity {
        QuinnCurrentProbeDeadlineDrive::Stale(returned) => {
            assert_eq!(returned.instant(), deadline_a);
            returned
        }
        _ => panic!("another composition accepted a pointer-distinct probe identity"),
    };
    let generation_b = composition_b.snapshot().session.generation();
    let failure = composition_b
        .reconcile_current_probe_ack(
            QuinnCurrentProbeSchedulerAuthority::Armed(returned_a),
            generation_b,
            ProbeSequence::new(1).expect("nonzero wrong-identity sequence"),
        )
        .expect_err("an active non-ACK-winner cannot consume foreign scheduler authority");
    assert_eq!(
        failure.kind(),
        crate::owned_upstream::client_controller::ClientCurrentProbeAckReconcileErrorKind::NotAckWon,
    );
    let returned_a = match failure.into_scheduler() {
        QuinnCurrentProbeSchedulerAuthority::Armed(returned) => returned,
        QuinnCurrentProbeSchedulerAuthority::Queued(_) => {
            panic!("failed armed reconciliation changed scheduler authority phase")
        }
    };
    let returned_a = expect_non_consuming_deadline(
        composition_a.try_receive_current_before_probe_deadline(
            returned_a,
            SimTime::from_nanos(deadline_a.as_nanos() - 1),
        ),
        deadline_a,
    );
    composition_a
        .retain_current_probe_scheduler_for_shutdown(QuinnCurrentProbeSchedulerAuthority::Armed(
            returned_a,
        ))
        .expect("A shutdown retains its original exact scheduler deadline");
    composition_b
        .retain_current_probe_scheduler_for_shutdown(QuinnCurrentProbeSchedulerAuthority::Armed(
            token_b,
        ))
        .expect("B shutdown retains its unrelated exact scheduler deadline");
    drop((exact_ack_a, exact_ack_b));

    let (client_a, joined_server_a, client_b, joined_server_b) =
        tokio::time::timeout(TEST_TIMEOUT, async {
            tokio::join!(
                composition_a.join_current_for_shutdown(),
                server_a.join(),
                composition_b.join_current_for_shutdown(),
                server_b.join(),
            )
        })
        .await
        .expect("wrong-identity actor pairs join within the local bound");
    let client_a = client_a.expect("wrong-identity client A joins structurally");
    let joined_server_a = joined_server_a.expect("wrong-identity server A joins structurally");
    let client_b = client_b.expect("wrong-identity client B joins structurally");
    let joined_server_b = joined_server_b.expect("wrong-identity server B joins structurally");
    assert_eq!(client_a.owned_messages(), 0);
    assert_eq!(client_a.owned_bytes(), 0);
    assert_eq!(joined_server_a.owned_messages(), 0);
    assert_eq!(joined_server_a.owned_bytes(), 0);
    assert_eq!(client_b.owned_messages(), 0);
    assert_eq!(client_b.owned_bytes(), 0);
    assert_eq!(joined_server_b.owned_messages(), 0);
    assert_eq!(joined_server_b.owned_bytes(), 0);
    assert_eq!(deadline_a, deadline_b);
    drop((
        composition_a,
        composition_b,
        owner_bootstrap_a,
        owner_bootstrap_b,
    ));
    let (shutdown_a, shutdown_b) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(listener_a.shutdown(), listener_b.shutdown())
    })
    .await
    .expect("wrong-identity listener cleanup is bounded");
    shutdown_a.expect("wrong-identity A actors release their listener slot");
    shutdown_b.expect("wrong-identity B actors release their listener slot");
}

#[tokio::test(flavor = "current_thread")]
async fn matching_probe_ack_returns_the_original_scheduler_deadline() {
    let ArmedCurrentProbeFixture {
        mut listener,
        mut composition,
        mut server,
        owner_bootstrap,
        exact_ack,
        token,
        deadline,
    } = armed_current_probe_fixture(0xdb).await;

    server
        .enqueue_ordered_leg_control(exact_ack, STANDBY_CONTROL_V1)
        .expect("server A queues the matching current probe ACK");
    assert_eq!(
        server
            .try_flush(SimTime::ZERO)
            .expect("matching probe ACK flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    tokio::time::timeout(TEST_TIMEOUT, async {
        while composition
            .current_actor_snapshot()
            .queued_inbound_messages()
            != 1
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("matching probe ACK reaches the current actor queue");

    let acked = composition.try_receive_current_before_probe_deadline(token, deadline);
    let settlement = reconcile_armed_ack(&mut composition, acked, deadline);
    assert_eq!(
        settlement.phase(),
        crate::owned_upstream::client_controller::ClientCurrentProbeAckSettlementPhase::Armed,
    );
    wait_actor_ordered_delivery(&mut server).await;

    let (client_joined, server_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(composition.join_current_for_shutdown(), server.join())
    })
    .await
    .expect("ACK-winner actors join within the local bound");
    let client_joined = client_joined.expect("ACK-winner client joins structurally");
    let server_joined = server_joined.expect("ACK-winner server joins structurally");
    assert_eq!(client_joined.owned_messages(), 0);
    assert_eq!(client_joined.owned_bytes(), 0);
    assert_eq!(server_joined.owned_messages(), 0);
    assert_eq!(server_joined.owned_bytes(), 0);
    drop((composition, owner_bootstrap));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("ACK-winner listener cleanup is bounded")
        .expect("ACK-winner actors release the listener slot");
}

struct ExactActorDeadline(SimTime);

impl QuinnLegActorArrivalDeadline for ExactActorDeadline {
    fn arrival_deadline(&self) -> SimTime {
        self.0
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actor_failed_and_suppressed_timer_polls_return_the_original_exact_deadline() {
    const INJECTED_FAILURE: &str = "test current-probe deadline classification failure";

    let limits = composition_actor_limits();
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let peer_policy = AttachPeerPolicy::new(
        OwnerIdentity::new([0xd7; 32]).expect("nonzero owner identity"),
        DevicePrincipal::new([0xd8; 16]).expect("nonzero device principal"),
    );
    let mut listener = QuinnLegActorListener::bind(
        QuinnLegActorServerConfig::new(
            "127.0.0.1:0".parse().expect("loopback listener address"),
            cert_dir.join("server-cert.pem"),
            cert_dir.join("server-key.pem"),
            1,
        )
        .expect("single-slot listener configuration"),
    )
    .await
    .expect("bind actor timer-ownership listener");
    let (mut client, mut server) =
        connect_real_actor_pair(&mut listener, LegId::A, limits, peer_policy).await;
    let (accepted, owner_bootstrap) = complete_initial_actor_attach(&mut client, &mut server).await;
    wait_actor_ordered_delivery(&mut server).await;
    let generation = accepted.generation();

    server
        .queue
        .as_mut()
        .expect("server A retains its exact FIFO")
        .push(
            Frame::try_new(
                generation,
                Record::Ack {
                    flow_id: crate::resumable::SessionFlowId::new(1).expect("nonzero test flow id"),
                    direction: Direction::ClientToTarget,
                    next_accepted: ByteOffset::new(0),
                    final_accepted: false,
                },
            )
            .expect("actor timer test frame is valid"),
        )
        .expect("server A queues one actor timer test frame");
    assert_eq!(
        server
            .try_flush(SimTime::ZERO)
            .expect("actor timer test frame flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    tokio::time::timeout(TEST_TIMEOUT, async {
        while client.actor_snapshot().queued_inbound_messages() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("actor timer test frame reaches the client queue");

    let exact = SimTime::from_nanos(9_000_000_000);
    let gate = QuinnLegCompositionGate::mint_for_test();
    let failed: QuinnLegActorInboundDeadlinePoll<ExactActorDeadline, (), ()> = client
        .try_transact_inbound_before_probe_deadline(
            &gate,
            ExactActorDeadline(exact),
            &mut (),
            |_, _, _envelope, timer| Err((timer, QuinnLegActorError::Protocol(INJECTED_FAILURE))),
            |_, _, timer| -> Result<(), ExactActorDeadline> { Err(timer) },
        );
    let failed_cause = match failed {
        QuinnLegActorInboundDeadlinePoll::Failed { timer, cause } => {
            assert_eq!(timer.0, exact);
            assert!(matches!(
                cause.as_ref(),
                QuinnLegActorError::Protocol(INJECTED_FAILURE)
            ));
            cause
        }
        _ => panic!("injected actor classification failure did not win arbitration"),
    };

    let second_exact = SimTime::from_nanos(9_000_000_001);
    let suppressed: QuinnLegActorInboundDeadlinePoll<ExactActorDeadline, (), ()> = client
        .try_transact_inbound_before_probe_deadline(
            &gate,
            ExactActorDeadline(second_exact),
            &mut (),
            |_, _, _envelope, timer| {
                Err((
                    timer,
                    QuinnLegActorError::Protocol("unexpected second receive"),
                ))
            },
            |_, _, timer| -> Result<(), ExactActorDeadline> { Err(timer) },
        );
    match suppressed {
        QuinnLegActorInboundDeadlinePoll::Suppressed { timer, cause } => {
            assert_eq!(timer.0, second_exact);
            assert!(Arc::ptr_eq(&cause, &failed_cause));
        }
        _ => panic!("the actor's stable first failure did not suppress the second timer"),
    }

    drop((accepted, owner_bootstrap));
    server.request_cancel();
    let (client_joined, server_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(client.join(), server.join())
    })
    .await
    .expect("actor timer-ownership pair joins within the local bound");
    let client_joined = client_joined.expect("failed timer actor joins structurally");
    let server_joined = server_joined.expect("timer peer joins structurally");
    assert_eq!(client_joined.owned_messages(), 0);
    assert_eq!(client_joined.owned_bytes(), 0);
    assert_eq!(server_joined.owned_messages(), 0);
    assert_eq!(server_joined.owned_bytes(), 0);
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("actor timer-ownership listener cleanup is bounded")
        .expect("actor timer-ownership pair releases the listener slot");
}

#[tokio::test(flavor = "current_thread")]
async fn actor_failure_winner_returns_the_original_composition_deadline_and_cause() {
    let ArmedCurrentProbeFixture {
        mut listener,
        mut composition,
        mut server,
        owner_bootstrap,
        exact_ack: _,
        token,
        deadline,
    } = armed_current_probe_fixture(0xd9).await;
    let readiness = composition.current_actor_readiness_epoch_for_test();

    server.request_cancel();
    let server_joined = tokio::time::timeout(TEST_TIMEOUT, server.join())
        .await
        .expect("probe peer joins within the local bound")
        .expect("probe peer joins structurally");
    assert_eq!(server_joined.owned_messages(), 0);
    assert_eq!(server_joined.owned_bytes(), 0);
    let observed = tokio::time::timeout(
        TEST_TIMEOUT,
        composition.wait_for_current_actor_progress(readiness),
    )
    .await
    .expect("peer close wakes current actor progress")
    .expect_err("peer close installs one actor first failure");
    let QuinnLegActorError::RootFailed(observed_cause) = observed else {
        panic!("peer close did not expose the actor's exact first cause: {observed:?}");
    };

    let winner = composition.try_receive_current_before_probe_deadline(token, deadline);
    let returned = match winner {
        QuinnCurrentProbeDeadlineDrive::ActorFailureWon {
            deadline: returned,
            cause,
        } => {
            assert_eq!(returned.instant(), deadline);
            assert!(Arc::ptr_eq(&cause, &observed_cause));
            returned
        }
        _ => panic!("the installed actor failure did not win due arbitration"),
    };
    composition
        .retain_current_probe_scheduler_for_shutdown(QuinnCurrentProbeSchedulerAuthority::Armed(
            returned,
        ))
        .expect("shutdown retains the same exact armed probe deadline");

    let applied = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match composition
                .try_drive_current_terminal()
                .expect("actor-failure terminal drive remains live")
            {
                QuinnCurrentTerminalDrive::Pending => tokio::task::yield_now().await,
                QuinnCurrentTerminalDrive::Applied(applied) => break applied,
                outcome => panic!("actor failure did not publish one terminal: {outcome:?}"),
            }
        }
    })
    .await
    .expect("actor-failure terminal publishes within the local bound");
    assert_eq!(applied.reason(), LegTransportTerminalReason::PeerClosed);
    let client_joined = tokio::time::timeout(TEST_TIMEOUT, composition.join_current_for_shutdown())
        .await
        .expect("actor-failure current join is bounded")
        .expect("actor-failure current joins structurally");
    assert_eq!(client_joined.owned_messages(), 0);
    assert_eq!(client_joined.owned_bytes(), 0);
    drop((composition, owner_bootstrap));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("actor-failure listener cleanup is bounded")
        .expect("actor-failure actors release the listener slot");
}
