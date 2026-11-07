//! Settles the second exact B probe through its original actor FIFO before test shutdown.
//! The owner retains queue authority; this helper only drives and observes delivery.
//! Invariant: the peer receives the same authenticated sequence that the owner queued.
//! Entry: `drain_second_probe`; callers live in the parent R7 freshness tests.

use super::*;

pub(super) async fn make_exact_owner_slot_fresh(
    slot: &mut OwnerStandbySlot,
    server_b: &mut super::super::StartedQuinnLegActor,
    client_b: &mut super::super::StartedQuinnLegActor,
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

pub(super) async fn establish_hint_freshness(
    slot: &mut OwnerStandbySlot,
    owner: &mut super::super::StartedQuinnLegActor,
    peer: &mut super::super::StartedQuinnLegActor,
    registration: &ClientRegisteredStandby,
) {
    make_exact_owner_slot_fresh(
        slot,
        owner,
        peer,
        registration,
        ProbeSequence::new(17).expect("nonzero hint fixture probe sequence"),
        SimTime::from_nanos(50),
        Duration::from_nanos(200),
    )
    .await;
}

pub(super) async fn drain_second_probe(
    owner: &mut QuinnOwnerStandbySessionComposition,
    peer: &mut super::super::StartedQuinnLegActor,
    registration: &ClientRegisteredStandby,
) {
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match owner
                .try_flush(SimTime::ZERO)
                .expect("second exact B probe flush remains live")
            {
                QuinnLegActorFlushDisposition::Submitted => break,
                QuinnLegActorFlushDisposition::OrderedDeliveryPending => {
                    tokio::task::yield_now().await;
                }
                QuinnLegActorFlushDisposition::Idle => {
                    panic!("second exact B probe vanished before submission")
                }
            }
        }
    })
    .await
    .expect("second exact B probe reaches the original writer");
    let inbound = tokio::time::timeout(TEST_TIMEOUT, peer.recv_classified(STANDBY_CONTROL_V1))
        .await
        .expect("second exact B probe reaches the peer within the bound")
        .expect("second exact B peer remains live")
        .expect("second exact B probe remains classified");
    let pending = match registration
        .exact_leg_probe_responder()
        .classify(take_actor_inbound(inbound))
    {
        ExactLegProbeResponse::Pending(pending) => pending,
        ExactLegProbeResponse::Unhandled(_) => panic!("second exact B probe lost authentication"),
    };
    assert_eq!(pending.probe_sequence(), ProbeSequence::new(2).unwrap());
    drop(pending);
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match owner
                .try_flush(SimTime::ZERO)
                .expect("second exact B ordered delivery remains live")
            {
                QuinnLegActorFlushDisposition::Idle => break,
                QuinnLegActorFlushDisposition::OrderedDeliveryPending
                | QuinnLegActorFlushDisposition::Submitted => tokio::task::yield_now().await,
            }
        }
    })
    .await
    .expect("second exact B ordered delivery settles before shutdown");
}
