use super::*;
use crate::owned_upstream::client_controller::ClientInboundObservation;
use crate::resumable::{ByteOffset, Direction, LegGeneration, SessionFlowId, SessionId};

fn session(byte: u8) -> SessionId {
    SessionId::new([byte; 16]).expect("nonzero test session")
}

fn generation(value: u64) -> LegGeneration {
    LegGeneration::new(value).expect("nonzero test generation")
}

fn replay_observation(
    session_id: SessionId,
    session_became_nonempty: bool,
) -> RootReplayObservation {
    RootReplayObservation {
        session_id,
        generation: generation(3),
        session_became_nonempty,
    }
}

fn positive_ack(
    session_id: SessionId,
    arrived_at: SimTime,
    session_has_outstanding_replay: bool,
) -> ClientInboundObservation {
    ClientInboundObservation::ApplicationAckAdvanced {
        arrived_at,
        session_id,
        flow_id: SessionFlowId::new(7).expect("nonzero test flow"),
        generation: generation(3),
        direction: Direction::ClientToTarget,
        previous_accepted: ByteOffset::new(1),
        next_accepted: ByteOffset::new(2),
        released_bytes: 1,
        final_accepted: !session_has_outstanding_replay,
        remaining_oldest: session_has_outstanding_replay.then(|| ByteOffset::new(2)),
        session_has_outstanding_replay,
    }
}

#[test]
fn actor_arrival_refreshes_the_aggregate_deadline_without_root_service_time() {
    let session_id = session(0x41);
    let mut progress = RootClientProgressState::new();
    progress
        .observe_replay_became_outstanding(
            replay_observation(session_id, true),
            SimTime::from_nanos(10),
            Duration::from_nanos(20),
        )
        .expect("first aggregate outstanding episode starts");
    assert_eq!(progress.next_wake_deadline(), Some(SimTime::from_nanos(30)));

    // The actor stamped this ACK at 25, although the root is deliberately
    // imagined to service it at 1_000. The root API accepts no service-time
    // argument, so the only possible refresh baseline is the actor stamp.
    progress
        .observe_inbound(Some(positive_ack(
            session_id,
            SimTime::from_nanos(25),
            true,
        )))
        .expect("positive reducer-committed ACK refreshes the episode");
    let delayed_root_service_time = SimTime::from_nanos(1_000);
    assert!(delayed_root_service_time > progress.next_wake_deadline().unwrap());
    assert_eq!(progress.next_wake_deadline(), Some(SimTime::from_nanos(45)));
}

#[test]
fn actor_arrival_at_deadline_wins_but_late_ack_preserves_selected_due_state() {
    let session_id = session(0x45);
    let mut exact = RootClientProgressState::new();
    exact
        .observe_replay_became_outstanding(
            replay_observation(session_id, true),
            SimTime::from_nanos(10),
            Duration::from_nanos(20),
        )
        .expect("exact-deadline episode starts");
    assert!(
        exact
            .observe_due(SimTime::from_nanos(30))
            .expect("root selects the due deadline")
    );
    exact
        .observe_inbound(Some(positive_ack(
            session_id,
            SimTime::from_nanos(30),
            true,
        )))
        .expect("an ACK arriving exactly at the old deadline wins");
    assert_eq!(exact.next_wake_deadline(), Some(SimTime::from_nanos(50)));
    assert_eq!(exact.live_inventory().due_authorities(), 0);

    let mut late = RootClientProgressState::new();
    late.observe_replay_became_outstanding(
        replay_observation(session_id, true),
        SimTime::from_nanos(10),
        Duration::from_nanos(20),
    )
    .expect("post-deadline episode starts");
    assert!(
        late.observe_due(SimTime::from_nanos(30))
            .expect("root selects the due deadline")
    );
    late.observe_inbound(Some(positive_ack(
        session_id,
        SimTime::from_nanos(31),
        true,
    )))
    .expect("the committed late ACK remains observable without winning");
    assert_eq!(late.next_wake_deadline(), None);
    assert_eq!(late.live_inventory().due_authorities(), 1);
    assert_eq!(
        late.source
            .as_ref()
            .and_then(ClientLocalAckStallSource::due_at),
        Some(SimTime::from_nanos(30)),
    );

    let mut late_final = RootClientProgressState::new();
    late_final
        .observe_replay_became_outstanding(
            replay_observation(session_id, true),
            SimTime::from_nanos(10),
            Duration::from_nanos(20),
        )
        .expect("post-deadline final-ACK episode starts");
    assert!(
        late_final
            .observe_due(SimTime::from_nanos(30))
            .expect("root selects the due deadline")
    );
    late_final
        .observe_inbound(Some(positive_ack(
            session_id,
            SimTime::from_nanos(31),
            false,
        )))
        .expect("the committed final ACK remains observable without clearing selected due state");
    assert_eq!(
        late_final.live_inventory(),
        RootClientProgressLiveInventory {
            sources: 1,
            episodes: 1,
            timer_slots: 1,
            due_authorities: 1,
        },
    );
}

#[test]
fn additional_outstanding_and_missing_ack_observation_do_not_refresh() {
    let session_id = session(0x42);
    let mut progress = RootClientProgressState::new();
    progress
        .observe_replay_became_outstanding(
            replay_observation(session_id, true),
            SimTime::from_nanos(10),
            Duration::from_nanos(20),
        )
        .expect("first aggregate outstanding episode starts");

    progress
        .observe_replay_became_outstanding(
            replay_observation(session_id, false),
            SimTime::from_nanos(19),
            Duration::from_nanos(20),
        )
        .expect("another outstanding direction shares the aggregate episode");
    progress
        .observe_inbound(None)
        .expect("duplicate/nonpositive ACK has no opaque positive observation");
    assert_eq!(progress.next_wake_deadline(), Some(SimTime::from_nanos(30)));
    assert_eq!(progress.history().replay_became_outstanding(), 2);
    assert_eq!(progress.history().positive_application_acks(), 0);
}

#[test]
fn final_ack_clears_the_episode_but_session_terminal_clears_the_source() {
    let session_id = session(0x43);
    let mut progress = RootClientProgressState::new();
    progress
        .observe_replay_became_outstanding(
            replay_observation(session_id, true),
            SimTime::from_nanos(10),
            Duration::from_nanos(20),
        )
        .expect("first aggregate outstanding episode starts");
    progress
        .observe_inbound(Some(positive_ack(
            session_id,
            SimTime::from_nanos(20),
            false,
        )))
        .expect("final positive ACK clears aggregate outstanding");
    assert_eq!(
        progress.live_inventory(),
        RootClientProgressLiveInventory {
            sources: 1,
            episodes: 0,
            timer_slots: 0,
            due_authorities: 0,
        },
        "ordinary progress retains same-generation clock correlation",
    );

    progress
        .observe_replay_became_outstanding(
            replay_observation(session_id, true),
            SimTime::from_nanos(30),
            Duration::from_nanos(20),
        )
        .expect("a later aggregate episode can start");
    progress
        .observe_reducer(
            RootReducerObservationView {
                replay_became_outstanding: None,
                flow_direction_terminal: None,
                session_expired: Some(RootSessionTerminalObservation {
                    session_id,
                    generation: generation(3),
                }),
                session_has_outstanding_replay: false,
            },
            SimTime::from_nanos(31),
            Duration::from_nanos(20),
        )
        .expect("session terminal consumes and clears policy state");
    assert_eq!(
        progress.live_inventory(),
        RootClientProgressLiveInventory::default(),
        "session terminal destroys the whole policy source",
    );
    assert_eq!(progress.history().session_expired(), 1);
}

#[test]
fn local_timer_has_one_named_slot_and_rejects_n_plus_one_exactly() {
    let mut slot = RootLocalAckStallTimerSlot::new();
    assert_eq!(slot.capacity(), ROOT_LOCAL_ACK_STALL_TIMER_SLOTS);
    assert_eq!(slot.try_install(SimTime::from_nanos(10)), Ok(()));
    assert_eq!(
        slot.try_install(SimTime::from_nanos(11)),
        Err(SimTime::from_nanos(11)),
        "N+1 returns the exact uninstalled deadline",
    );
    assert_eq!(slot.next_wake_deadline(), Some(SimTime::from_nanos(10)));
}

#[test]
fn shutdown_retires_the_episode_and_timer_before_zero_inventory() {
    let mut progress = RootClientProgressState::new();
    progress
        .observe_replay_became_outstanding(
            replay_observation(session(0x44), true),
            SimTime::from_nanos(10),
            Duration::from_nanos(20),
        )
        .expect("first aggregate outstanding episode starts");
    assert_eq!(
        progress.live_inventory(),
        RootClientProgressLiveInventory {
            sources: 1,
            episodes: 1,
            timer_slots: 1,
            due_authorities: 0,
        }
    );

    let audit = progress.close_for_shutdown();
    assert_eq!(audit.discarded_sources(), 1);
    assert_eq!(audit.discarded_episodes(), 1);
    assert_eq!(audit.discarded_timer_slots(), 1);
    assert_eq!(
        progress.live_inventory(),
        RootClientProgressLiveInventory::default()
    );
}
