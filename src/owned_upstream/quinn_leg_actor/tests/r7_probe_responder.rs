use super::{
    QuinnCurrentInboundDrive, QuinnLegActorFlushDisposition, RealRegisteredStandbyFixture,
    RealUnregisteredStandbyFixture, TEST_TIMEOUT, ordered_control, real_registered_standby_fixture,
    real_unregistered_standby_fixture_with_target_state, take_actor_inbound,
    wait_actor_ordered_delivery,
};
use crate::owned_upstream::leg::BoundInbound;
use crate::owned_upstream::leg_io::LegControlQueueErrorKind;
use crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition;
use crate::owned_upstream::standby::{ExactLegProbeResponder, ExactLegProbeResponse};
use crate::owned_upstream::two_leg::SimTime;
use crate::resumable::{LegControlRecord, ProbeSequence, STANDBY_CONTROL_V1};

#[tokio::test(flavor = "current_thread")]
async fn installed_current_a_mints_the_same_responder_from_authenticated_provenance() {
    let RealUnregisteredStandbyFixture {
        mut listener,
        mut composition,
        mut server_a,
        mut client_b,
        mut server_b,
        tcp_factory,
        local_flows,
        peer_policy: _,
        owner_executor,
        owner_active_a,
    } = real_unregistered_standby_fixture_with_target_state(2, 1, true).await;
    let _client_current_source_gate = composition
        .current_exact_leg_probe_responder()
        .expect("installed client current A mints the production responder");
    let owner_current_responder =
        ExactLegProbeResponder::from_installed(owner_active_a.installed_leg_probe_source());
    let queued = composition
        .begin_current_probe()
        .expect("healthy current A queues its exact probe");
    let expected_generation = queued.generation();
    let expected_sequence = queued.probe_sequence();
    assert_eq!(
        composition
            .try_flush_current(SimTime::ZERO)
            .expect("current A probe flush remains live"),
        QuinnCurrentFlushDisposition::Submitted,
    );
    let owner_inbound =
        tokio::time::timeout(TEST_TIMEOUT, server_a.recv_classified(STANDBY_CONTROL_V1))
            .await
            .expect("owner current-A probe receive is bounded")
            .expect("owner current A remains live")
            .expect("owner current A receives the classified probe");
    let pending_ack = match owner_current_responder.classify(take_actor_inbound(owner_inbound)) {
        ExactLegProbeResponse::Pending(pending) => pending,
        ExactLegProbeResponse::Unhandled(_) => {
            panic!("installed owner current A rejected its exact probe")
        }
    };
    assert_eq!(pending_ack.probe_sequence(), expected_sequence);
    pending_ack
        .enqueue(
            server_a
                .queue
                .as_mut()
                .expect("owner current A owns its FIFO"),
        )
        .expect("owner current A queues the exact contract-derived ACK");
    assert_eq!(
        server_a
            .try_flush(SimTime::ZERO)
            .expect("owner current-A ACK flush remains live"),
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
    .expect("current-A ACK reaches the controller within the local bound");
    assert!(matches!(
        acked,
        QuinnCurrentInboundDrive::ProbeAcked {
            generation,
            probe_sequence,
        } if generation == expected_generation && probe_sequence == expected_sequence
    ));
    wait_actor_ordered_delivery(&mut server_a).await;

    let (client_joined, server_a_joined, client_b_joined, server_b_joined) =
        tokio::time::timeout(TEST_TIMEOUT, async {
            tokio::join!(
                composition.join_current_for_shutdown(),
                server_a.join(),
                client_b.join(),
                server_b.join(),
            )
        })
        .await
        .expect("current-A responder actors join within the local bound");
    let client_joined = client_joined.expect("client A joins cleanly");
    assert_eq!(client_joined.owned_messages(), 0);
    assert_eq!(client_joined.owned_bytes(), 0);
    for joined in [
        server_a_joined.expect("server A joins cleanly"),
        client_b_joined.expect("client B joins cleanly"),
        server_b_joined.expect("server B joins cleanly"),
    ] {
        assert_eq!(joined.owned_messages(), 0);
        assert_eq!(joined.owned_bytes(), 0);
    }
    drop((
        composition,
        tcp_factory,
        local_flows,
        owner_executor,
        owner_active_a,
    ));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("current-A responder listener cleanup is bounded")
        .expect("current-A responder releases both actor slots");
}

#[tokio::test(flavor = "current_thread")]
async fn client_and_owner_registered_b_respond_on_the_exact_physical_fifo() {
    let RealRegisteredStandbyFixture {
        mut listener,
        mut composition,
        mut server_a,
        mut client_b,
        mut server_b,
        registered_b,
        tcp_factory,
        local_flows,
        peer_policy: _,
        owner_executor,
        owner_active_a,
        standby_slot,
    } = real_registered_standby_fixture(2, 1).await;
    let sequence = ProbeSequence::new(17).expect("nonzero exact probe sequence");
    let client_responder = registered_b.exact_leg_probe_responder();
    let pending_probe = registered_b
        .begin_exact_probe(sequence)
        .expect("registered client B derives its exact probe");
    let awaiting_probe = pending_probe
        .enqueue(
            client_b
                .queue
                .as_mut()
                .expect("client B owns its sole FIFO"),
        )
        .expect("client B queues its exact probe");
    assert_eq!(
        client_b
            .try_flush(SimTime::ZERO)
            .expect("client B probe flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    let owner_inbound =
        tokio::time::timeout(TEST_TIMEOUT, server_b.recv_classified(STANDBY_CONTROL_V1))
            .await
            .expect("owner B probe receive is bounded")
            .expect("owner B remains live")
            .expect("owner B receives the classified probe");
    let owner_inbound = take_actor_inbound(owner_inbound);
    let owner_probe_for_client = match &owner_inbound {
        BoundInbound::LegControl(bound) => bound.parts().1.clone(),
        BoundInbound::Session(_) => panic!("probe entered the session namespace"),
    };
    let owner_pending_ack = match standby_slot.classify_exact_leg_probe(owner_inbound) {
        ExactLegProbeResponse::Pending(pending) => pending,
        ExactLegProbeResponse::Unhandled(_) => {
            panic!("authenticated owner B did not accept its exact probe")
        }
    };
    assert_eq!(owner_pending_ack.probe_sequence(), sequence);
    for filler_sequence in 100..104 {
        server_b
            .enqueue_ordered_leg_control(ordered_control(filler_sequence), STANDBY_CONTROL_V1)
            .expect("owner B fills only its existing bounded FIFO");
    }
    let pressure = owner_pending_ack
        .enqueue(server_b.queue.as_mut().expect("owner B owns its sole FIFO"))
        .expect_err("a full exact FIFO returns the pending ACK and inbound probe");
    assert!(matches!(
        pressure.kind(),
        LegControlQueueErrorKind::CapacityExceeded {
            queued_frames: 4,
            ..
        }
    ));
    let owner_pending_ack = pressure.into_pending();
    assert_eq!(owner_pending_ack.probe_sequence(), sequence);
    for _ in 0..4 {
        tokio::time::timeout(TEST_TIMEOUT, async {
            loop {
                match server_b
                    .try_flush(SimTime::ZERO)
                    .expect("pressure filler flush remains live")
                {
                    QuinnLegActorFlushDisposition::Submitted => break,
                    QuinnLegActorFlushDisposition::OrderedDeliveryPending => {
                        tokio::task::yield_now().await;
                    }
                    QuinnLegActorFlushDisposition::Idle => {
                        panic!("full owner B FIFO lost a pressure filler before submission")
                    }
                }
            }
        })
        .await
        .expect("each pressure filler submission is bounded");
        let filler =
            tokio::time::timeout(TEST_TIMEOUT, client_b.recv_classified(STANDBY_CONTROL_V1))
                .await
                .expect("pressure filler receive is bounded")
                .expect("client B remains live while pressure drains")
                .expect("every pressure filler reaches the classified boundary");
        drop(take_actor_inbound(filler));
    }
    wait_actor_ordered_delivery(&mut server_b).await;
    let owner_enqueued = owner_pending_ack
        .enqueue(server_b.queue.as_mut().expect("owner B owns its sole FIFO"))
        .expect("owner B queues the derived ACK on the exact FIFO");
    assert_eq!(owner_enqueued.probe_sequence(), sequence);
    assert_eq!(
        server_b
            .try_flush(SimTime::ZERO)
            .expect("owner B ACK flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    let client_ack =
        tokio::time::timeout(TEST_TIMEOUT, client_b.recv_classified(STANDBY_CONTROL_V1))
            .await
            .expect("client B ACK receive is bounded")
            .expect("client B remains live")
            .expect("client B receives the classified ACK");
    let fresh = awaiting_probe
        .validate_exact_ack(
            take_actor_inbound(client_ack),
            SimTime::from_nanos(50),
            SimTime::from_nanos(50),
        )
        .expect("owner production responder emitted the exact ACK");
    wait_actor_ordered_delivery(&mut server_b).await;

    let endpoint_loss_probe = owner_probe_for_client.clone();
    server_b
        .enqueue_ordered_leg_control(owner_probe_for_client, STANDBY_CONTROL_V1)
        .expect("owner B queues a peer probe through its existing FIFO");
    assert_eq!(
        server_b
            .try_flush(SimTime::ZERO)
            .expect("owner B peer-probe flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    let client_inbound =
        tokio::time::timeout(TEST_TIMEOUT, client_b.recv_classified(STANDBY_CONTROL_V1))
            .await
            .expect("client B peer-probe receive is bounded")
            .expect("client B remains live")
            .expect("client B receives the classified peer probe");
    let client_pending_ack = match client_responder.classify(take_actor_inbound(client_inbound)) {
        ExactLegProbeResponse::Pending(pending) => pending,
        ExactLegProbeResponse::Unhandled(_) => {
            panic!("authenticated client B did not accept its exact peer probe")
        }
    };
    let client_enqueued = client_pending_ack
        .enqueue(
            client_b
                .queue
                .as_mut()
                .expect("client B owns its sole FIFO"),
        )
        .expect("client B queues the derived ACK on the exact FIFO");
    assert_eq!(client_enqueued.probe_sequence(), sequence);
    assert_eq!(
        client_b
            .try_flush(SimTime::ZERO)
            .expect("client B ACK flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    let owner_ack =
        tokio::time::timeout(TEST_TIMEOUT, server_b.recv_classified(STANDBY_CONTROL_V1))
            .await
            .expect("owner B ACK receive is bounded")
            .expect("owner B remains live")
            .expect("owner B receives the classified ACK");
    let BoundInbound::LegControl(owner_ack) = take_actor_inbound(owner_ack) else {
        panic!("probe ACK entered the session namespace");
    };
    assert!(matches!(
        owner_ack.parts().1.record(),
        LegControlRecord::LegProbeAck { probe_sequence, .. } if *probe_sequence == sequence
    ));
    wait_actor_ordered_delivery(&mut client_b).await;

    client_b
        .enqueue_ordered_leg_control(endpoint_loss_probe, STANDBY_CONTROL_V1)
        .expect("client B queues a second exact probe for endpoint-loss ownership");
    assert_eq!(
        client_b
            .try_flush(SimTime::ZERO)
            .expect("endpoint-loss probe flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    let owner_inbound =
        tokio::time::timeout(TEST_TIMEOUT, server_b.recv_classified(STANDBY_CONTROL_V1))
            .await
            .expect("endpoint-loss probe receive is bounded")
            .expect("owner B remains live through classification")
            .expect("owner B receives the endpoint-loss probe");
    let pending_after_terminal =
        match standby_slot.classify_exact_leg_probe(take_actor_inbound(owner_inbound)) {
            ExactLegProbeResponse::Pending(pending) => pending,
            ExactLegProbeResponse::Unhandled(_) => {
                panic!("owner B lost exact probe provenance before terminal")
            }
        };
    server_b.request_cancel();
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let endpoint_open = server_b
                .io
                .as_ref()
                .and_then(|io| io.established_leg().endpoint_liveness().upgrade())
                .is_some_and(|endpoint| endpoint.is_open());
            if !endpoint_open {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("owner B terminal publication is bounded");
    let endpoint_loss = pending_after_terminal
        .enqueue(
            server_b
                .queue
                .as_mut()
                .expect("terminal owner B retains its FIFO"),
        )
        .expect_err("endpoint loss returns the exact pending ACK and inbound probe");
    assert_eq!(
        endpoint_loss.kind(),
        &LegControlQueueErrorKind::EndpointLost
    );
    let retained_after_terminal = endpoint_loss.into_pending();
    assert_eq!(retained_after_terminal.probe_sequence(), sequence);

    drop(fresh);
    drop(retained_after_terminal);
    let (client_joined, server_a_joined, client_b_joined, server_b_joined) =
        tokio::time::timeout(TEST_TIMEOUT, async {
            tokio::join!(
                composition.join_current_for_shutdown(),
                server_a.join(),
                client_b.join(),
                server_b.join(),
            )
        })
        .await
        .expect("dual-role responder actors join within the local bound");
    let client_joined = client_joined.expect("client A joins cleanly");
    assert_eq!(client_joined.owned_messages(), 0);
    assert_eq!(client_joined.owned_bytes(), 0);
    for joined in [
        server_a_joined.expect("server A joins cleanly"),
        client_b_joined.expect("client B joins cleanly"),
        server_b_joined.expect("server B joins cleanly"),
    ] {
        assert_eq!(joined.owned_messages(), 0);
        assert_eq!(joined.owned_bytes(), 0);
    }
    drop((
        composition,
        tcp_factory,
        local_flows,
        owner_executor,
        owner_active_a,
        standby_slot,
    ));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("dual-role responder listener cleanup is bounded")
        .expect("dual-role responder releases both actor slots");
}
