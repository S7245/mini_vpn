use super::{
    QuinnLegActorError, QuinnLegActorFlushDisposition, RealRegisteredStandbyFixture, TEST_TIMEOUT,
    real_registered_standby_fixture, take_actor_inbound, wait_actor_ordered_delivery,
};
use crate::owned_upstream::leg::BoundInbound;
use crate::owned_upstream::leg_io::LegControlQueueErrorKind;
use crate::owned_upstream::owner_session_controller::{
    OwnerSessionActorEvent, OwnerSessionController, OwnerSessionHintDrive,
};
use crate::owned_upstream::quinn_leg_actor::{
    QuinnLegActorInboundPoll, QuinnLegActorInboundPreflight, QuinnOwnerActorArrival,
};
use crate::owned_upstream::quinn_leg_composition::{
    QuinnLegCompositionGate, QuinnOwnerStandbyInboundDrive, QuinnOwnerStandbyProbeArmDrive,
    QuinnOwnerStandbyProbeDrive, QuinnOwnerStandbyProbeExpiryDrive,
    QuinnOwnerStandbySessionComposition, QuinnOwnerStandbyShutdownTerminalDrive,
    QuinnStandbyLivenessConfig,
};
use crate::owned_upstream::reverse_switch::{
    OwnerHintAdmissionError, OwnerHintPreparation, OwnerReverseProgress, OwnerSwitchHintIssuer,
};
use crate::owned_upstream::standby::{
    ClientRegisteredStandby, ExactLegProbeResponse, OwnerStandbyHintEnqueueErrorKind,
    OwnerStandbyHintEvidenceError, OwnerStandbyProbeAckErrorKind, OwnerStandbyProbeDrive,
    OwnerStandbySlot,
};
use crate::owned_upstream::two_leg::SimTime;
use crate::resumable::{
    ByteOffset, Direction, Frame, HintSequence, LegControlFrame, LegControlRecord, ProbeSequence,
    Record, STANDBY_CONTROL_V1, SessionId, SwitchHintCause,
};
use std::time::Duration;

async fn make_exact_owner_slot_fresh(
    slot: &mut OwnerStandbySlot,
    server_b: &mut super::StartedQuinnLegActor,
    client_b: &mut super::StartedQuinnLegActor,
    registered_b: &ClientRegisteredStandby,
    probe_sequence: ProbeSequence,
    arrived_at: SimTime,
    fresh_for: Duration,
) {
    assert_eq!(
        slot.drive_exact_probe(
            probe_sequence,
            server_b.queue.as_mut().expect("owner B owns its sole FIFO"),
        )
        .expect("exact owner B probe enters its FIFO"),
        OwnerStandbyProbeDrive::Enqueued { probe_sequence },
    );
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match server_b
                .try_flush(SimTime::ZERO)
                .expect("owner B probe flush remains live")
            {
                QuinnLegActorFlushDisposition::Submitted => break,
                QuinnLegActorFlushDisposition::OrderedDeliveryPending
                | QuinnLegActorFlushDisposition::Idle => tokio::task::yield_now().await,
            }
        }
    })
    .await
    .expect("owner B probe submission is bounded");
    let inbound = tokio::time::timeout(TEST_TIMEOUT, client_b.recv_classified(STANDBY_CONTROL_V1))
        .await
        .expect("client B probe receive is bounded")
        .expect("client B remains live")
        .expect("client B receives the exact probe");
    let pending_ack = match registered_b
        .exact_leg_probe_responder()
        .classify(take_actor_inbound(inbound))
    {
        ExactLegProbeResponse::Pending(pending) => pending,
        ExactLegProbeResponse::Unhandled(_) => panic!("exact owner B probe lost provenance"),
    };
    pending_ack
        .enqueue(client_b.queue.as_mut().expect("client B owns its FIFO"))
        .expect("client B enqueues the exact ACK");
    assert_eq!(
        client_b
            .try_flush(SimTime::ZERO)
            .expect("client B ACK flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    let inbound = tokio::time::timeout(TEST_TIMEOUT, server_b.recv_classified(STANDBY_CONTROL_V1))
        .await
        .expect("owner B ACK receive is bounded")
        .expect("owner B remains live")
        .expect("owner B receives the exact ACK");
    slot.observe_exact_probe_ack(take_actor_inbound(inbound), arrived_at, fresh_for)
        .expect("exact probe ACK mints slot-owned freshness");
    wait_actor_ordered_delivery(client_b).await;
    wait_actor_ordered_delivery(server_b).await;
}

/// Physical counterpart used by the owner-controller tracer. The ACK arrival
/// timestamp is taken from the actor envelope under the same first-failure
/// transaction that moves the exact probe ACK into the slot.
async fn make_exact_owner_slot_fresh_from_actor_arrival(
    slot: &mut OwnerStandbySlot,
    server_b: &mut super::StartedQuinnLegActor,
    client_b: &mut super::StartedQuinnLegActor,
    registered_b: &ClientRegisteredStandby,
    probe_sequence: ProbeSequence,
    fresh_for: Duration,
) {
    assert_eq!(
        slot.drive_exact_probe(
            probe_sequence,
            server_b.queue.as_mut().expect("owner B owns its sole FIFO"),
        )
        .expect("exact owner B probe enters its FIFO"),
        OwnerStandbyProbeDrive::Enqueued { probe_sequence },
    );
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match server_b
                .try_flush(SimTime::ZERO)
                .expect("owner B probe flush remains live")
            {
                QuinnLegActorFlushDisposition::Submitted => break,
                QuinnLegActorFlushDisposition::OrderedDeliveryPending
                | QuinnLegActorFlushDisposition::Idle => tokio::task::yield_now().await,
            }
        }
    })
    .await
    .expect("owner B probe submission is bounded");
    let inbound = tokio::time::timeout(TEST_TIMEOUT, client_b.recv_classified(STANDBY_CONTROL_V1))
        .await
        .expect("client B probe receive is bounded")
        .expect("client B remains live")
        .expect("client B receives the exact probe");
    let pending_ack = match registered_b
        .exact_leg_probe_responder()
        .classify(take_actor_inbound(inbound))
    {
        ExactLegProbeResponse::Pending(pending) => pending,
        ExactLegProbeResponse::Unhandled(_) => panic!("exact owner B probe lost provenance"),
    };
    pending_ack
        .enqueue(client_b.queue.as_mut().expect("client B owns its FIFO"))
        .expect("client B enqueues the exact ACK");
    assert_eq!(
        client_b
            .try_flush(SimTime::ZERO)
            .expect("client B ACK flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );

    let gate = QuinnLegCompositionGate::mint_for_test();
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let applied = server_b
                .try_transact_controller_inbound_with_parts(
                    &gate,
                    slot,
                    |_slot, _io, _queue| Ok(QuinnLegActorInboundPreflight::Ready(())),
                    |slot, (), envelope, io, _queue| {
                        let (delivery, arrived_at) = envelope.into_parts();
                        let inbound =
                            io.receive_classified_delivery(delivery, STANDBY_CONTROL_V1)?;
                        slot.observe_exact_probe_ack(inbound, arrived_at, fresh_for)
                            .map_err(|_| {
                                QuinnLegActorError::Protocol(
                                    "exact actor-arrival owner B probe ACK was rejected",
                                )
                            })
                    },
                )
                .expect("owner B exact ACK transaction remains live");
            match applied {
                QuinnLegActorInboundPoll::Idle => tokio::task::yield_now().await,
                QuinnLegActorInboundPoll::Blocked => {
                    panic!("owner B exact ACK transaction unexpectedly blocked")
                }
                QuinnLegActorInboundPoll::Classified(fresh) => {
                    assert_eq!(fresh.probe_sequence(), probe_sequence);
                    break;
                }
            }
        }
    })
    .await
    .expect("actor-captured owner B ACK becomes fresh within the bound");
    wait_actor_ordered_delivery(client_b).await;
    wait_actor_ordered_delivery(server_b).await;
}

async fn shutdown_registered_fixture_clean(fixture: RealRegisteredStandbyFixture) {
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
    } = fixture;
    let (client_a_joined, server_a_joined, client_b_joined, server_b_joined) =
        tokio::time::timeout(TEST_TIMEOUT, async {
            tokio::join!(
                composition.join_current_for_shutdown(),
                server_a.join(),
                client_b.join(),
                server_b.join(),
            )
        })
        .await
        .expect("registered fixture actors join within the local bound");
    let client_a_joined = client_a_joined.expect("client A joins cleanly");
    assert_eq!(client_a_joined.owned_messages(), 0);
    assert_eq!(client_a_joined.owned_bytes(), 0);
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
        registered_b,
        tcp_factory,
        local_flows,
        owner_executor,
        owner_active_a,
        standby_slot,
    ));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("registered fixture listener cleanup is bounded")
        .expect("registered fixture releases both actor slots");
}

#[tokio::test(flavor = "current_thread")]
async fn owner_b_cancelled_join_observation_retries_the_same_retained_task() {
    let RealRegisteredStandbyFixture {
        mut listener,
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
    } = real_registered_standby_fixture(2, 1).await;
    let liveness =
        QuinnStandbyLivenessConfig::new(Duration::from_millis(100), Duration::from_millis(20))
            .expect("bounded cancellation liveness policy");
    let mut owner_b =
        QuinnOwnerStandbySessionComposition::adopt_registered(server_b, standby_slot, liveness)
            .expect("owner composition adopts the cancellation-test physical B");
    assert!(matches!(
        owner_b.join_for_shutdown().await,
        Err(QuinnLegActorError::ResidualOwnership {
            messages: 1,
            bytes: 0,
        }),
    ));
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match owner_b
                .try_flush(SimTime::ZERO)
                .expect("rejected shutdown leaves owner B Running")
            {
                QuinnLegActorFlushDisposition::Idle => break,
                QuinnLegActorFlushDisposition::Submitted
                | QuinnLegActorFlushDisposition::OrderedDeliveryPending => {
                    tokio::task::yield_now().await;
                }
            }
        }
    })
    .await
    .expect("owner B registration acceptance delivery is bounded");
    let join_block = owner_b
        .arm_join_block_for_test()
        .expect("running owner B exposes its deterministic structural join gate");
    let mut cancelled_observation = Box::pin(owner_b.join_for_shutdown());
    tokio::select! {
        biased;
        result = &mut cancelled_observation => {
            panic!("owner B join bypassed its structural cancellation gate: {result:?}")
        }
        () = join_block.wait_until_blocked() => {}
    }
    drop(cancelled_observation);

    assert!(matches!(
        owner_b.try_flush(SimTime::ZERO),
        Err(QuinnLegActorError::Closing),
    ));
    assert!(matches!(
        owner_b.try_receive_probe_ack(),
        QuinnOwnerStandbyInboundDrive::ActorFailed(QuinnLegActorError::Closing),
    ));

    let mut retried_observation = Box::pin(owner_b.join_for_shutdown());
    tokio::select! {
        biased;
        result = &mut retried_observation => {
            panic!("retry detached from or duplicated the blocked owner B task: {result:?}")
        }
        () = tokio::task::yield_now() => {}
    }
    join_block.release();
    let (owner_b_joined, client_a_joined, server_a_joined, client_b_joined) =
        tokio::time::timeout(TEST_TIMEOUT, async {
            tokio::join!(
                retried_observation,
                composition.join_current_for_shutdown(),
                server_a.join(),
                client_b.join(),
            )
        })
        .await
        .expect("the retained owner B and peer joins complete after release");
    let owner_b_joined = owner_b_joined.expect("the retained owner B actor joins cleanly");
    let client_a_joined = client_a_joined.expect("client A joins cleanly");
    let server_a_joined = server_a_joined.expect("server A joins cleanly");
    let client_b_joined = client_b_joined.expect("client B joins cleanly");
    assert_eq!(client_a_joined.owned_messages(), 0);
    assert_eq!(client_a_joined.owned_bytes(), 0);
    assert_eq!(owner_b_joined.owned_messages(), 0);
    assert_eq!(owner_b_joined.owned_bytes(), 0);
    assert_eq!(owner_b_joined.owned_terminals(), 1);
    assert!(!owner_b_joined.terminal_failure());
    let settled = owner_b
        .settle_joined_terminal_for_shutdown()
        .expect("joined owner B settles its exact local-shutdown terminal");
    let discarded = match settled {
        QuinnOwnerStandbyShutdownTerminalDrive::Settled { discarded, .. } => discarded,
        QuinnOwnerStandbyShutdownTerminalDrive::AlreadySettled { .. } => {
            panic!("first owner B terminal settlement was already complete")
        }
    };
    assert_eq!(discarded.registered_authorities(), 1);
    assert_eq!(discarded.registration_receipts(), 1);
    assert_eq!(discarded.hint_registration_identities(), 1);
    assert_eq!(discarded.unclaimed_hint_bindings(), 1);
    assert_eq!(discarded.pending_probe_frames(), 0);
    assert_eq!(discarded.pending_probe_bytes(), 0);
    assert_eq!(discarded.probe_receipts(), 0);
    assert_eq!(discarded.armed_timers(), 0);
    assert_eq!(discarded.freshness_witnesses(), 0);
    let settled_joined = owner_b
        .join_for_shutdown()
        .await
        .expect("retained owner B join witness remains observable after settlement");
    assert_eq!(settled_joined.owned_terminals(), 0);
    for joined in [server_a_joined, client_b_joined] {
        assert_eq!(joined.owned_messages(), 0);
        assert_eq!(joined.owned_bytes(), 0);
    }

    drop((
        composition,
        registered_b,
        tcp_factory,
        local_flows,
        owner_executor,
        owner_active_a,
    ));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("owner B cancellation listener cleanup is bounded")
        .expect("owner B cancellation releases both actor slots");
}

#[tokio::test(flavor = "current_thread")]
async fn owner_registered_b_becomes_fresh_only_after_its_exact_physical_probe_ack() {
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
        mut standby_slot,
    } = real_registered_standby_fixture(2, 1).await;
    let sequence = ProbeSequence::new(23).expect("nonzero exact owner B probe sequence");
    let client_responder = registered_b.exact_leg_probe_responder();

    assert_eq!(
        standby_slot
            .drive_exact_probe(
                sequence,
                server_b.queue.as_mut().expect("owner B owns its sole FIFO"),
            )
            .expect("authenticated owner B derives and queues its exact probe"),
        OwnerStandbyProbeDrive::Enqueued {
            probe_sequence: sequence
        },
    );
    assert!(!standby_slot.probe_is_fresh_at(SimTime::ZERO));
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match server_b
                .try_flush(SimTime::ZERO)
                .expect("owner B probe flush remains live")
            {
                QuinnLegActorFlushDisposition::Submitted => break,
                QuinnLegActorFlushDisposition::OrderedDeliveryPending => {
                    tokio::task::yield_now().await;
                }
                QuinnLegActorFlushDisposition::Idle => {
                    panic!("owner B probe disappeared before FIFO submission")
                }
            }
        }
    })
    .await
    .expect("owner B probe waits for the registration acceptance tail within the bound");
    let client_inbound =
        tokio::time::timeout(TEST_TIMEOUT, client_b.recv_classified(STANDBY_CONTROL_V1))
            .await
            .expect("client B probe receive is bounded")
            .expect("client B remains live")
            .expect("client B receives the classified owner probe");
    let pending_ack = match client_responder.classify(take_actor_inbound(client_inbound)) {
        ExactLegProbeResponse::Pending(pending) => pending,
        ExactLegProbeResponse::Unhandled(_) => {
            panic!("authenticated client B rejected the exact owner probe")
        }
    };
    assert_eq!(pending_ack.probe_sequence(), sequence);
    pending_ack
        .enqueue(
            client_b
                .queue
                .as_mut()
                .expect("client B owns its sole FIFO"),
        )
        .expect("client B queues the exact owner probe ACK");
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
            .expect("owner B receives the classified probe ACK");
    let arrived_at = SimTime::from_nanos(50);
    let fresh_for = Duration::from_nanos(200);
    let fresh = standby_slot
        .observe_exact_probe_ack(take_actor_inbound(owner_ack), arrived_at, fresh_for)
        .expect("only the exact B ACK makes owner B fresh");
    assert_eq!(fresh.probe_sequence(), sequence);
    assert_eq!(fresh.fresh_until(), SimTime::from_nanos(250));
    assert!(standby_slot.probe_is_fresh_at(SimTime::from_nanos(250)));
    assert!(!standby_slot.probe_is_fresh_at(SimTime::from_nanos(251)));
    wait_actor_ordered_delivery(&mut client_b).await;
    make_exact_owner_slot_fresh(
        &mut standby_slot,
        &mut server_b,
        &mut client_b,
        &registered_b,
        ProbeSequence::new(31).expect("nonzero hint-test probe sequence"),
        SimTime::from_nanos(50),
        Duration::from_nanos(200),
    )
    .await;

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
        .expect("owner B freshness actors join within the local bound");
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
        registered_b,
        standby_slot,
        tcp_factory,
        local_flows,
        owner_executor,
        owner_active_a,
    ));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("owner B freshness listener cleanup is bounded")
        .expect("owner B freshness releases both actor slots");
}

#[tokio::test(flavor = "current_thread")]
async fn owner_switch_hint_adapter_preserves_wrong_and_queues_only_on_exact_physical_b() {
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
        mut standby_slot,
    } = real_registered_standby_fixture(2, 1).await;
    wait_actor_ordered_delivery(&mut server_b).await;
    let hint_binding = standby_slot
        .take_exact_hint_binding(server_b.queue.as_ref().expect("owner B owns its sole FIFO"))
        .expect("delivered exact registration exposes one owner hint binding");
    let contract = standby_slot
        .hint_contract()
        .expect("authenticated physical B exposes one immutable hint contract");
    let flow_id = local_flows[0].flow_id();
    let make_hint = |session_id| {
        LegControlFrame::try_new(
            contract.generation(),
            LegControlRecord::SwitchHint {
                session_id,
                standby_nonce: contract.standby_nonce(),
                hint_sequence: HintSequence::new(1).expect("nonzero hint sequence"),
                flow_id,
                direction: Direction::TargetToClient,
                oldest_unacknowledged: ByteOffset::new(0),
                cause: SwitchHintCause::ReverseApplicationAckStall,
            },
            contract.features(),
        )
        .expect("bounded hint frame")
    };

    let wrong = make_hint(SessionId::new([0xa5; 16]).expect("nonzero wrong session"));
    let wrong_bytes = wrong
        .encode(contract.features())
        .expect("wrong-correlation hint still has deterministic wire bytes");
    let failure = standby_slot
        .enqueue_exact_switch_hint(
            &hint_binding,
            SimTime::from_nanos(50),
            wrong,
            server_b.queue.as_mut().expect("owner B owns its sole FIFO"),
        )
        .expect_err("wrong session cannot enter physical B");
    assert_eq!(
        failure.kind(),
        &OwnerStandbyHintEnqueueErrorKind::WrongCorrelation,
    );
    let (wrong, kind) = failure.into_parts();
    assert_eq!(kind, OwnerStandbyHintEnqueueErrorKind::WrongCorrelation);
    assert_eq!(
        wrong
            .encode(contract.features())
            .expect("returned wrong hint remains encodable"),
        wrong_bytes,
    );

    let exact = make_hint(contract.session_id());
    let exact_bytes = exact
        .encode(contract.features())
        .expect("exact hint wire bytes");
    let receipt = standby_slot
        .enqueue_exact_switch_hint(
            &hint_binding,
            SimTime::from_nanos(50),
            exact,
            server_b.queue.as_mut().expect("owner B owns its sole FIFO"),
        )
        .expect("only exact correlation enters the authenticated physical B FIFO");
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match server_b
                .try_flush(SimTime::ZERO)
                .expect("owner B hint flush remains live")
            {
                QuinnLegActorFlushDisposition::Submitted => break,
                QuinnLegActorFlushDisposition::OrderedDeliveryPending
                | QuinnLegActorFlushDisposition::Idle => tokio::task::yield_now().await,
            }
        }
    })
    .await
    .expect("exact owner hint reaches B transport within the local bound");
    let inbound = tokio::time::timeout(TEST_TIMEOUT, client_b.recv_classified(STANDBY_CONTROL_V1))
        .await
        .expect("client B hint receive is bounded")
        .expect("client B remains live")
        .expect("client B receives the exact physical hint");
    let BoundInbound::LegControl(bound) = take_actor_inbound(inbound) else {
        panic!("physical owner hint was misclassified as session data")
    };
    assert_eq!(
        bound
            .parts()
            .1
            .encode(contract.features())
            .expect("received exact hint remains encodable"),
        exact_bytes,
    );
    wait_actor_ordered_delivery(&mut server_b).await;
    assert!(receipt.was_delivered());

    let (client_a_joined, server_a_joined, client_b_joined, server_b_joined) =
        tokio::time::timeout(TEST_TIMEOUT, async {
            tokio::join!(
                composition.join_current_for_shutdown(),
                server_a.join(),
                client_b.join(),
                server_b.join(),
            )
        })
        .await
        .expect("owner hint adapter actors join within the local bound");
    let client_a_joined = client_a_joined.expect("client A joins cleanly");
    assert_eq!(client_a_joined.owned_messages(), 0);
    assert_eq!(client_a_joined.owned_bytes(), 0);
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
        registered_b,
        tcp_factory,
        local_flows,
        owner_executor,
        owner_active_a,
        standby_slot,
    ));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("owner hint adapter listener cleanup is bounded")
        .expect("owner hint adapter releases both actor slots");
}

#[tokio::test(flavor = "current_thread")]
async fn owner_hint_issuer_rejects_scalar_equal_b_crosswire_and_restores_real_fifo_pressure() {
    let mut exact = real_registered_standby_fixture(2, 1).await;
    let mut alternate = real_registered_standby_fixture(2, 1).await;
    wait_actor_ordered_delivery(&mut exact.server_b).await;
    wait_actor_ordered_delivery(&mut alternate.server_b).await;
    make_exact_owner_slot_fresh(
        &mut exact.standby_slot,
        &mut exact.server_b,
        &mut exact.client_b,
        &exact.registered_b,
        ProbeSequence::new(41).expect("nonzero exact-B probe sequence"),
        SimTime::from_nanos(50),
        Duration::from_nanos(200),
    )
    .await;
    make_exact_owner_slot_fresh(
        &mut alternate.standby_slot,
        &mut alternate.server_b,
        &mut alternate.client_b,
        &alternate.registered_b,
        ProbeSequence::new(42).expect("nonzero alternate-B probe sequence"),
        SimTime::from_nanos(50),
        Duration::from_nanos(200),
    )
    .await;

    let exact_contract = exact
        .standby_slot
        .hint_contract()
        .expect("exact B exposes its authenticated contract");
    let alternate_contract = alternate
        .standby_slot
        .hint_contract()
        .expect("alternate B exposes its authenticated contract");
    assert_eq!(
        exact_contract, alternate_contract,
        "the adversary deliberately uses indistinguishable scalar correlation"
    );
    let binding = exact
        .standby_slot
        .take_exact_hint_binding(
            exact
                .server_b
                .queue
                .as_ref()
                .expect("exact B owns its sole FIFO"),
        )
        .expect("delivered exact B registration exposes one binding");
    let flow_id = exact.local_flows[0].flow_id();
    let make_filler = |sequence: u64| {
        LegControlFrame::try_new(
            exact_contract.generation(),
            LegControlRecord::SwitchHint {
                session_id: exact_contract.session_id(),
                standby_nonce: exact_contract.standby_nonce(),
                hint_sequence: HintSequence::new(sequence).expect("nonzero filler sequence"),
                flow_id,
                direction: Direction::TargetToClient,
                oldest_unacknowledged: ByteOffset::new(0),
                cause: SwitchHintCause::ReverseApplicationAckStall,
            },
            exact_contract.features(),
        )
        .expect("bounded physical FIFO filler")
    };
    let mut filler_receipts = Vec::with_capacity(4);
    for sequence in 100..104 {
        filler_receipts.push(
            exact
                .standby_slot
                .enqueue_exact_switch_hint(
                    &binding,
                    SimTime::from_nanos(50),
                    make_filler(sequence),
                    exact
                        .server_b
                        .queue
                        .as_mut()
                        .expect("exact B owns its sole FIFO"),
                )
                .expect("four exact controls fill the real bounded FIFO"),
        );
    }
    assert_eq!(
        exact
            .server_b
            .queue
            .as_ref()
            .expect("exact B owns its sole FIFO")
            .len(),
        4
    );

    let mut issuer = OwnerSwitchHintIssuer::from_exact_binding(
        binding,
        Duration::from_nanos(20),
        HintSequence::new(7).expect("nonzero issuer sequence"),
    )
    .expect("exact physical binding negotiated owner hints");
    let mut progress = OwnerReverseProgress::new(1).expect("bounded reverse progress");
    progress
        .register_live(
            exact_contract.generation(),
            flow_id,
            Direction::TargetToClient,
        )
        .expect("one exact live reverse direction");
    progress
        .observe_outstanding(
            exact_contract.generation(),
            flow_id,
            Direction::TargetToClient,
            ByteOffset::new(8),
            SimTime::from_nanos(10),
        )
        .expect("one exact outstanding replay episode");
    assert_eq!(
        issuer
            .prepare_next_due_hint_for_exact_slot(
                &progress,
                &exact.standby_slot,
                SimTime::from_nanos(50),
            )
            .expect("exact B is fresh before the hint frame is retained"),
        OwnerHintPreparation::Prepared {
            hint_sequence: HintSequence::new(7).unwrap(),
        }
    );
    assert_eq!(
        issuer.prepare_next_due_hint_for_exact_slot(
            &progress,
            &alternate.standby_slot,
            SimTime::from_nanos(50),
        ),
        Err(
            crate::owned_upstream::reverse_switch::OwnerHintError::PhysicalStandby(
                OwnerStandbyHintEvidenceError::WrongRegistrationBinding,
            )
        ),
        "scalar-equal B2 is rejected at prepare before it can replace B1's pending frame",
    );
    assert_eq!(
        issuer
            .prepare_next_due_hint_for_exact_slot(
                &progress,
                &exact.standby_slot,
                SimTime::from_nanos(50),
            )
            .expect("the exact B1 retries its retained pending frame"),
        OwnerHintPreparation::Retry {
            hint_sequence: HintSequence::new(7).unwrap(),
        },
    );

    let alternate_len = alternate
        .server_b
        .queue
        .as_ref()
        .expect("alternate B owns its sole FIFO")
        .len();
    assert!(matches!(
        issuer.try_admit_pending(
            &progress,
            SimTime::from_nanos(50),
            &alternate.standby_slot,
            alternate
                .server_b
                .queue
                .as_mut()
                .expect("alternate B owns its sole FIFO"),
        ),
        Err(OwnerHintAdmissionError::PhysicalMismatch(
            OwnerStandbyHintEvidenceError::WrongRegistrationBinding
        ))
    ));
    assert_eq!(
        alternate
            .server_b
            .queue
            .as_ref()
            .expect("alternate B owns its sole FIFO")
            .len(),
        alternate_len,
        "wrong-binding rejection must precede alternate FIFO admission"
    );

    let pressure = match issuer.try_admit_pending(
        &progress,
        SimTime::from_nanos(50),
        &exact.standby_slot,
        exact
            .server_b
            .queue
            .as_mut()
            .expect("exact B owns its sole FIFO"),
    ) {
        Ok(_) => panic!("a full four-frame physical FIFO cannot admit the owner hint"),
        Err(error) => error,
    };
    assert!(matches!(
        pressure,
        OwnerHintAdmissionError::QueueRejected(OwnerStandbyHintEnqueueErrorKind::Queue(
            LegControlQueueErrorKind::CapacityExceeded {
                queued_frames: 4,
                ..
            }
        ))
    ));
    assert_eq!(
        exact
            .server_b
            .queue
            .as_ref()
            .expect("exact B owns its sole FIFO")
            .len(),
        4,
        "pressure restores the exact pending hint without changing the FIFO"
    );

    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match exact
                .server_b
                .try_flush(SimTime::ZERO)
                .expect("exact B filler flush remains live")
            {
                QuinnLegActorFlushDisposition::Submitted => break,
                QuinnLegActorFlushDisposition::OrderedDeliveryPending
                | QuinnLegActorFlushDisposition::Idle => tokio::task::yield_now().await,
            }
        }
    })
    .await
    .expect("one FIFO slot becomes available within the local bound");
    let first_filler = tokio::time::timeout(
        TEST_TIMEOUT,
        exact.client_b.recv_classified(STANDBY_CONTROL_V1),
    )
    .await
    .expect("first filler receive is bounded")
    .expect("exact client B remains live")
    .expect("exact client B receives the first filler");
    drop(take_actor_inbound(first_filler));
    tokio::time::timeout(TEST_TIMEOUT, async {
        while !filler_receipts[0].was_delivered() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the first exact filler receipt reaches delivered without advancing the FIFO");

    let admitted = issuer
        .try_admit_pending(
            &progress,
            SimTime::from_nanos(51),
            &exact.standby_slot,
            exact
                .server_b
                .queue
                .as_mut()
                .expect("exact B owns its sole FIFO"),
        )
        .expect("the restored exact hint retries after one real FIFO slot opens");
    assert_eq!(admitted.hint_sequence(), HintSequence::new(7).unwrap());
    let receipt = admitted.into_receipt();
    for remaining_index in 0..4 {
        tokio::time::timeout(TEST_TIMEOUT, async {
            loop {
                match exact
                    .server_b
                    .try_flush(SimTime::ZERO)
                    .expect("exact B pressured tail flush remains live")
                {
                    QuinnLegActorFlushDisposition::Submitted => break,
                    QuinnLegActorFlushDisposition::OrderedDeliveryPending
                    | QuinnLegActorFlushDisposition::Idle => tokio::task::yield_now().await,
                }
            }
        })
        .await
        .expect("each pressured tail frame submits within the local bound");
        let inbound = tokio::time::timeout(
            TEST_TIMEOUT,
            exact.client_b.recv_classified(STANDBY_CONTROL_V1),
        )
        .await
        .expect("pressured tail receive is bounded")
        .expect("exact client B remains live")
        .expect("exact client B receives the pressured tail");
        if remaining_index == 3 {
            let BoundInbound::LegControl(bound) = take_actor_inbound(inbound) else {
                panic!("admitted owner hint was misclassified as session data")
            };
            assert!(matches!(
                bound.parts().1.record(),
                LegControlRecord::SwitchHint { hint_sequence, .. }
                    if *hint_sequence == HintSequence::new(7).unwrap()
            ));
        } else {
            drop(take_actor_inbound(inbound));
        }
        let exact_receipt = if remaining_index < 3 {
            &filler_receipts[remaining_index + 1]
        } else {
            &receipt
        };
        tokio::time::timeout(TEST_TIMEOUT, async {
            while !exact_receipt.was_delivered() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the exact submitted receipt reaches delivered without advancing the FIFO");
    }
    assert!(receipt.was_delivered());
    assert_eq!(
        issuer.last_published_sequence(),
        Some(HintSequence::new(7).unwrap())
    );
    assert_eq!(
        exact
            .server_b
            .queue
            .as_ref()
            .expect("exact B owns its sole FIFO")
            .len(),
        0
    );
    assert_eq!(
        exact
            .server_b
            .queue
            .as_ref()
            .expect("exact B owns its sole FIFO")
            .owned_bytes(),
        0
    );
    assert_eq!(
        exact
            .server_b
            .try_flush(SimTime::ZERO)
            .expect("delivered wait retirement remains live"),
        QuinnLegActorFlushDisposition::Idle,
        "an empty FIFO can only retire the pointer-identical delivered wait"
    );

    drop(filler_receipts);
    drop(issuer);
    shutdown_registered_fixture_clean(exact).await;
    shutdown_registered_fixture_clean(alternate).await;
}

#[tokio::test(flavor = "current_thread")]
async fn owner_b_composition_uses_actor_arrival_and_refreshes_only_after_expiry() {
    let RealRegisteredStandbyFixture {
        mut listener,
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
    } = real_registered_standby_fixture(2, 1).await;
    let fresh_for = Duration::from_millis(20);
    let probe_timeout = Duration::from_millis(100);
    let liveness = QuinnStandbyLivenessConfig::new(probe_timeout, fresh_for)
        .expect("bounded owner B liveness policy");
    let server_clock = server_a.session_clock_for_test();
    let client_responder = registered_b.exact_leg_probe_responder();
    let inbound_publication = server_b.arm_inbound_publication_block_for_test();
    let mut owner_b =
        QuinnOwnerStandbySessionComposition::adopt_registered(server_b, standby_slot, liveness)
            .expect("one owner composition adopts the exact registered physical B");

    assert_eq!(
        owner_b
            .drive_probe()
            .expect("owner composition derives its first exact B probe"),
        QuinnOwnerStandbyProbeDrive::Enqueued {
            probe_sequence: ProbeSequence::new(1).unwrap(),
        },
    );
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match owner_b
                .try_flush(SimTime::ZERO)
                .expect("owner composition probe flush remains live")
            {
                QuinnLegActorFlushDisposition::Submitted => break,
                QuinnLegActorFlushDisposition::OrderedDeliveryPending => {
                    tokio::task::yield_now().await;
                }
                QuinnLegActorFlushDisposition::Idle => {
                    panic!("owner composition lost its exact B probe before submission")
                }
            }
        }
    })
    .await
    .expect("owner composition waits for the registration tail within the bound");

    let client_inbound =
        tokio::time::timeout(TEST_TIMEOUT, client_b.recv_classified(STANDBY_CONTROL_V1))
            .await
            .expect("client B probe receive is bounded")
            .expect("client B remains live")
            .expect("client B receives the composition-owned probe");
    let pending_ack = match client_responder.classify(take_actor_inbound(client_inbound)) {
        ExactLegProbeResponse::Pending(pending) => pending,
        ExactLegProbeResponse::Unhandled(_) => {
            panic!("authenticated client B rejected the composition-owned probe")
        }
    };
    assert_eq!(pending_ack.probe_sequence(), ProbeSequence::new(1).unwrap());
    let probe_deadline = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match owner_b
                .arm_probe_after_delivery()
                .expect("owner probe arming remains live")
            {
                QuinnOwnerStandbyProbeArmDrive::NotDelivered => {
                    tokio::task::yield_now().await;
                }
                QuinnOwnerStandbyProbeArmDrive::Armed { deadline }
                | QuinnOwnerStandbyProbeArmDrive::AlreadyArmed { deadline } => break deadline,
            }
        }
    })
    .await
    .expect("owner probe writer receipt completes within the local bound");
    pending_ack
        .enqueue(
            client_b
                .queue
                .as_mut()
                .expect("client B owns its sole FIFO"),
        )
        .expect("client B queues the exact owner probe ACK");
    let arrival_lower_bound = server_clock
        .now()
        .expect("owner endpoint clock remains representable before ACK flush");
    assert!(arrival_lower_bound <= probe_deadline);
    assert_eq!(
        client_b
            .try_flush(SimTime::ZERO)
            .expect("client B ACK flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    tokio::time::timeout(TEST_TIMEOUT, inbound_publication.wait_until_blocked())
        .await
        .expect("the owner B actor captures the ACK before publication");
    assert!(
        server_clock
            .now()
            .expect("captured owner ACK time remains representable")
            <= probe_deadline,
        "the physical ACK must arrive before the exact probe deadline",
    );
    tokio::time::timeout(TEST_TIMEOUT, async {
        while server_clock
            .now()
            .expect("delayed owner timer clock remains representable")
            <= probe_deadline
        {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("owner service advances beyond the exact probe deadline");
    let delayed_service_at = server_clock
        .now()
        .expect("delayed owner service time remains representable");
    let in_flight_expiry = owner_b
        .try_expire_probe()
        .expect("in-flight ACK arbitrates before expiry");
    inbound_publication.release();
    assert_eq!(
        in_flight_expiry,
        QuinnOwnerStandbyProbeExpiryDrive::PublicationPending {
            probe_sequence: ProbeSequence::new(1).unwrap(),
            deadline: probe_deadline,
        },
    );
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match owner_b
                .try_expire_probe()
                .expect("published ACK continues to arbitrate before expiry")
            {
                QuinnOwnerStandbyProbeExpiryDrive::PublicationPending { .. } => {
                    tokio::task::yield_now().await;
                }
                QuinnOwnerStandbyProbeExpiryDrive::InboundPending {
                    probe_sequence,
                    deadline,
                } => {
                    assert_eq!(probe_sequence, ProbeSequence::new(1).unwrap());
                    assert_eq!(deadline, probe_deadline);
                    break;
                }
                outcome => panic!("pre-deadline ACK lost receive-before-expiry: {outcome:?}"),
            }
        }
    })
    .await
    .expect("published pre-deadline ACK wins within the local bound");
    let fresh_until = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match owner_b.try_receive_probe_ack() {
                QuinnOwnerStandbyInboundDrive::Idle => tokio::task::yield_now().await,
                QuinnOwnerStandbyInboundDrive::ProbeFresh {
                    probe_sequence,
                    fresh_until,
                } => {
                    assert_eq!(probe_sequence, ProbeSequence::new(1).unwrap());
                    break fresh_until;
                }
                outcome => panic!("owner composition rejected its exact B ACK: {outcome:?}"),
            }
        }
    })
    .await
    .expect("owner composition consumes the actor-published ACK within the bound");
    assert!(
        fresh_until
            >= arrival_lower_bound
                .checked_add(fresh_for)
                .expect("bounded actor-arrival freshness lower bound"),
    );
    assert!(
        fresh_until < delayed_service_at,
        "freshness must use actor arrival, not delayed owner service time",
    );
    assert!(!owner_b.probe_is_fresh_at(delayed_service_at));
    assert_eq!(
        owner_b
            .drive_probe()
            .expect("expired freshness permits exactly one next probe"),
        QuinnOwnerStandbyProbeDrive::Enqueued {
            probe_sequence: ProbeSequence::new(2).unwrap(),
        },
    );

    wait_actor_ordered_delivery(&mut client_b).await;
    let (client_joined, server_a_joined, client_b_joined, owner_b_joined) =
        tokio::time::timeout(TEST_TIMEOUT, async {
            tokio::join!(
                composition.join_current_for_shutdown(),
                server_a.join(),
                client_b.join(),
                owner_b.join_for_shutdown(),
            )
        })
        .await
        .expect("owner B composition actors join within the local bound");
    let client_joined = client_joined.expect("client A joins cleanly");
    assert_eq!(client_joined.owned_messages(), 0);
    assert_eq!(client_joined.owned_bytes(), 0);
    let owner_b_joined = owner_b_joined.expect("owner B joins structurally");
    assert_eq!(owner_b_joined.owned_messages(), 0);
    assert_eq!(owner_b_joined.owned_bytes(), 0);
    assert_eq!(owner_b_joined.owned_terminals(), 1);
    assert!(!owner_b_joined.terminal_failure());
    for joined in [
        server_a_joined.expect("server A joins cleanly"),
        client_b_joined.expect("client B joins cleanly"),
    ] {
        assert_eq!(joined.owned_messages(), 0);
        assert_eq!(joined.owned_bytes(), 0);
    }
    drop((
        composition,
        registered_b,
        tcp_factory,
        local_flows,
        owner_executor,
        owner_active_a,
    ));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("owner B composition listener cleanup is bounded")
        .expect("owner B composition releases both actor slots");
}

#[tokio::test(flavor = "current_thread")]
async fn owner_b_composition_rejects_ack_that_physically_arrives_after_probe_deadline() {
    let RealRegisteredStandbyFixture {
        mut listener,
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
    } = real_registered_standby_fixture(2, 1).await;
    let server_clock = server_a.session_clock_for_test();
    let client_responder = registered_b.exact_leg_probe_responder();
    let liveness =
        QuinnStandbyLivenessConfig::new(Duration::from_millis(10), Duration::from_millis(20))
            .expect("bounded late-ACK liveness policy");
    let mut owner_b =
        QuinnOwnerStandbySessionComposition::adopt_registered(server_b, standby_slot, liveness)
            .expect("owner composition adopts the late-ACK physical B");

    assert_eq!(
        owner_b.drive_probe().expect("first exact probe is derived"),
        QuinnOwnerStandbyProbeDrive::Enqueued {
            probe_sequence: ProbeSequence::new(1).unwrap(),
        },
    );
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match owner_b
                .try_flush(SimTime::ZERO)
                .expect("late-ACK probe flush remains live")
            {
                QuinnLegActorFlushDisposition::Submitted => break,
                QuinnLegActorFlushDisposition::OrderedDeliveryPending => {
                    tokio::task::yield_now().await;
                }
                QuinnLegActorFlushDisposition::Idle => {
                    panic!("late-ACK probe disappeared before submission")
                }
            }
        }
    })
    .await
    .expect("late-ACK probe waits for registration tail within the bound");
    let client_inbound =
        tokio::time::timeout(TEST_TIMEOUT, client_b.recv_classified(STANDBY_CONTROL_V1))
            .await
            .expect("late-ACK client probe receive is bounded")
            .expect("late-ACK client B remains live")
            .expect("late-ACK client B receives the exact probe");
    let pending_ack = match client_responder.classify(take_actor_inbound(client_inbound)) {
        ExactLegProbeResponse::Pending(pending) => pending,
        ExactLegProbeResponse::Unhandled(_) => panic!("late-ACK probe lost authentication"),
    };
    let deadline = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match owner_b
                .arm_probe_after_delivery()
                .expect("late-ACK probe arming remains live")
            {
                QuinnOwnerStandbyProbeArmDrive::NotDelivered => {
                    tokio::task::yield_now().await;
                }
                QuinnOwnerStandbyProbeArmDrive::Armed { deadline }
                | QuinnOwnerStandbyProbeArmDrive::AlreadyArmed { deadline } => break deadline,
            }
        }
    })
    .await
    .expect("late-ACK probe arms after exact writer delivery");
    tokio::time::timeout(TEST_TIMEOUT, async {
        while server_clock
            .now()
            .expect("late-ACK owner clock remains representable")
            <= deadline
        {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("late-ACK test advances beyond the absolute probe deadline");

    pending_ack
        .enqueue(
            client_b
                .queue
                .as_mut()
                .expect("late-ACK client B owns its sole FIFO"),
        )
        .expect("late-ACK client B queues the exact correlated ACK");
    assert_eq!(
        client_b
            .try_flush(SimTime::ZERO)
            .expect("late-ACK flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    let rejected_at = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match owner_b.try_receive_probe_ack() {
                QuinnOwnerStandbyInboundDrive::Idle => tokio::task::yield_now().await,
                QuinnOwnerStandbyInboundDrive::Rejected(
                    OwnerStandbyProbeAckErrorKind::DeadlineExpired,
                ) => {
                    break server_clock
                        .now()
                        .expect("late-ACK rejection clock remains representable");
                }
                outcome => panic!("late physical ACK changed freshness: {outcome:?}"),
            }
        }
    })
    .await
    .expect("late physical ACK is rejected within the local bound");
    assert!(!owner_b.probe_is_fresh_at(rejected_at));
    assert_eq!(
        owner_b
            .drive_probe()
            .expect("expired probe correlation permits one next probe"),
        QuinnOwnerStandbyProbeDrive::Enqueued {
            probe_sequence: ProbeSequence::new(2).unwrap(),
        },
    );

    wait_actor_ordered_delivery(&mut client_b).await;
    let (client_joined, server_a_joined, client_b_joined, owner_b_joined) =
        tokio::time::timeout(TEST_TIMEOUT, async {
            tokio::join!(
                composition.join_current_for_shutdown(),
                server_a.join(),
                client_b.join(),
                owner_b.join_for_shutdown(),
            )
        })
        .await
        .expect("late-ACK actors join within the local bound");
    let client_joined = client_joined.expect("late-ACK client A joins cleanly");
    assert_eq!(client_joined.owned_messages(), 0);
    assert_eq!(client_joined.owned_bytes(), 0);
    let owner_b_joined = owner_b_joined.expect("late-ACK owner B joins structurally");
    assert_eq!(owner_b_joined.owned_messages(), 0);
    assert_eq!(owner_b_joined.owned_bytes(), 0);
    assert_eq!(owner_b_joined.owned_terminals(), 1);
    assert!(!owner_b_joined.terminal_failure());
    for joined in [
        server_a_joined.expect("late-ACK server A joins cleanly"),
        client_b_joined.expect("late-ACK client B joins cleanly"),
    ] {
        assert_eq!(joined.owned_messages(), 0);
        assert_eq!(joined.owned_bytes(), 0);
    }
    drop((
        composition,
        registered_b,
        tcp_factory,
        local_flows,
        owner_executor,
        owner_active_a,
    ));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("late-ACK listener cleanup is bounded")
        .expect("late-ACK physical B releases both actor slots");
}

#[tokio::test(flavor = "current_thread")]
async fn owner_b_composition_expires_no_ack_probe_through_actor_timer_gate() {
    let RealRegisteredStandbyFixture {
        mut listener,
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
    } = real_registered_standby_fixture(2, 1).await;
    let server_clock = server_a.session_clock_for_test();
    let liveness =
        QuinnStandbyLivenessConfig::new(Duration::from_millis(10), Duration::from_millis(20))
            .expect("bounded no-ACK liveness policy");
    let mut owner_b =
        QuinnOwnerStandbySessionComposition::adopt_registered(server_b, standby_slot, liveness)
            .expect("owner composition adopts the no-ACK physical B");

    assert_eq!(
        owner_b.drive_probe().expect("no-ACK probe is derived"),
        QuinnOwnerStandbyProbeDrive::Enqueued {
            probe_sequence: ProbeSequence::new(1).unwrap(),
        },
    );
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match owner_b
                .try_flush(SimTime::ZERO)
                .expect("no-ACK probe flush remains live")
            {
                QuinnLegActorFlushDisposition::Submitted => break,
                QuinnLegActorFlushDisposition::OrderedDeliveryPending => {
                    tokio::task::yield_now().await;
                }
                QuinnLegActorFlushDisposition::Idle => {
                    panic!("no-ACK probe disappeared before submission")
                }
            }
        }
    })
    .await
    .expect("no-ACK probe waits for registration tail within the bound");
    let client_inbound =
        tokio::time::timeout(TEST_TIMEOUT, client_b.recv_classified(STANDBY_CONTROL_V1))
            .await
            .expect("no-ACK client probe receive is bounded")
            .expect("no-ACK client B remains live")
            .expect("no-ACK client B receives the exact probe");
    drop(
        match registered_b
            .exact_leg_probe_responder()
            .classify(take_actor_inbound(client_inbound))
        {
            ExactLegProbeResponse::Pending(pending) => pending,
            ExactLegProbeResponse::Unhandled(_) => panic!("no-ACK probe lost authentication"),
        },
    );
    let deadline = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match owner_b
                .arm_probe_after_delivery()
                .expect("no-ACK probe arming remains live")
            {
                QuinnOwnerStandbyProbeArmDrive::NotDelivered => {
                    tokio::task::yield_now().await;
                }
                QuinnOwnerStandbyProbeArmDrive::Armed { deadline }
                | QuinnOwnerStandbyProbeArmDrive::AlreadyArmed { deadline } => break deadline,
            }
        }
    })
    .await
    .expect("no-ACK probe arms after exact writer delivery");
    tokio::time::timeout(TEST_TIMEOUT, async {
        while server_clock
            .now()
            .expect("no-ACK owner clock remains representable")
            < deadline
        {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("no-ACK timer reaches its absolute deadline");
    assert_eq!(
        owner_b
            .try_expire_probe()
            .expect("no-ACK actor timer arbitration remains live"),
        QuinnOwnerStandbyProbeExpiryDrive::Expired {
            probe_sequence: ProbeSequence::new(1).unwrap(),
            deadline,
        },
    );
    assert_eq!(
        owner_b
            .drive_probe()
            .expect("expired no-ACK probe permits one next correlation"),
        QuinnOwnerStandbyProbeDrive::Enqueued {
            probe_sequence: ProbeSequence::new(2).unwrap(),
        },
    );

    wait_actor_ordered_delivery(&mut client_b).await;
    let (client_joined, server_a_joined, client_b_joined, owner_b_joined) =
        tokio::time::timeout(TEST_TIMEOUT, async {
            tokio::join!(
                composition.join_current_for_shutdown(),
                server_a.join(),
                client_b.join(),
                owner_b.join_for_shutdown(),
            )
        })
        .await
        .expect("no-ACK actors join within the local bound");
    let client_joined = client_joined.expect("no-ACK client A joins cleanly");
    assert_eq!(client_joined.owned_messages(), 0);
    assert_eq!(client_joined.owned_bytes(), 0);
    let owner_b_joined = owner_b_joined.expect("no-ACK owner B joins structurally");
    assert_eq!(owner_b_joined.owned_messages(), 0);
    assert_eq!(owner_b_joined.owned_bytes(), 0);
    assert_eq!(owner_b_joined.owned_terminals(), 1);
    assert!(!owner_b_joined.terminal_failure());
    for joined in [
        server_a_joined.expect("no-ACK server A joins cleanly"),
        client_b_joined.expect("no-ACK client B joins cleanly"),
    ] {
        assert_eq!(joined.owned_messages(), 0);
        assert_eq!(joined.owned_bytes(), 0);
    }
    drop((
        composition,
        registered_b,
        tcp_factory,
        local_flows,
        owner_executor,
        owner_active_a,
    ));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("no-ACK listener cleanup is bounded")
        .expect("no-ACK physical B releases both actor slots");
}
