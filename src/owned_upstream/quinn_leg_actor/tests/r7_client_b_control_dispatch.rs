use super::{
    QuinnLegActorFlushDisposition, RealUnregisteredStandbyFixture, StartedQuinnLegActor,
    TEST_TIMEOUT, actor_attach_credentials, apply_real_silent_current_probe_loss,
    ordered_control_for_generation, real_unregistered_standby_fixture_with_target_state,
    register_owned_and_make_real_b_fresh, register_owned_real_b, take_actor_inbound,
    wait_actor_ordered_delivery,
};
use crate::owned_upstream::leg::BoundInbound;
use crate::owned_upstream::leg::LegTransportTerminalReason;
use crate::owned_upstream::quinn_leg_actor::{
    QuinnLegActorReportedTerminal, QuinnLegActorReportedTerminalPoll,
};
use crate::owned_upstream::quinn_leg_composition::{
    QuinnCurrentJoinError, QuinnLegCompositionGate, QuinnStandbyAttachDrive,
    QuinnStandbyInboundDrive, QuinnStandbyPendingControlKind, QuinnStandbyPhysicalSwitchHintReject,
    QuinnStandbyProbeArmDrive, QuinnStandbyProbeDrive, QuinnStandbyShutdownStateKind,
    QuinnStandbySwitchHintDecision, QuinnStandbySwitchHintTransaction,
};
use crate::owned_upstream::standby::{OwnerStandbyProbeDrive, OwnerStandbySlot};
use crate::owned_upstream::two_leg::SimTime;
use crate::resumable::{
    AttachNonce, ByteOffset, Direction, HintSequence, LegControlFrame, LegControlRecord,
    LegGeneration, ProbeSequence, STANDBY_CONTROL_V1, SessionId, StandbyNonce, SwitchHintCause,
};
use std::sync::Arc;
use std::time::Duration;

/// The client shutdown-discard deliberately emits no transport DELIVERED.
/// This peer-side harness therefore waits for the real PeerClosed/Reset fact
/// and uses the existing exact terminal+sole-FIFO transaction to account for
/// its one undelivered ordered wait before structurally joining the peer.
async fn settle_owner_b_wait_after_real_peer_close(
    server_b: &mut StartedQuinnLegActor,
    standby_slot: &mut OwnerStandbySlot,
) -> LegTransportTerminalReason {
    let gate = QuinnLegCompositionGate::mint_for_test();
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let observed = server_b
                .try_transact_reported_terminal_with_parts(
                    &gate,
                    |_runtime, io, queue, reported| {
                        let (terminal, terminal_stamp) = reported.into_parts();
                        let reason = terminal.reason();
                        let Some(cleanup) =
                            queue.begin_owner_terminal_cleanup(io.established_leg(), &terminal)
                        else {
                            return Err((
                                QuinnLegActorReportedTerminal::restore(terminal, terminal_stamp),
                                "terminal did not bind the exact owner B FIFO",
                            ));
                        };
                        match standby_slot.retire_exact_terminal(terminal) {
                            Ok(retired) => {
                                let discarded = cleanup.commit();
                                Ok((reason, retired, discarded))
                            }
                            Err(failure) => Err((
                                QuinnLegActorReportedTerminal::restore(
                                    failure.into_terminal(),
                                    terminal_stamp,
                                ),
                                "terminal did not bind the exact owner B slot",
                            )),
                        }
                    },
                )
                .expect("owner B terminal transaction remains structurally available");
            match observed {
                QuinnLegActorReportedTerminalPoll::Pending => tokio::task::yield_now().await,
                QuinnLegActorReportedTerminalPoll::Applied((reason, retired, discarded)) => {
                    assert!(matches!(
                        reason,
                        LegTransportTerminalReason::PeerClosed | LegTransportTerminalReason::Reset
                    ));
                    assert_eq!(discarded.queued_frames(), 0);
                    assert_eq!(discarded.summed_frame_bytes(), 0);
                    assert_eq!(discarded.stored_owned_bytes(), 0);
                    assert!(discarded.ordered_delivery_wait());
                    drop(retired);
                    break reason;
                }
                QuinnLegActorReportedTerminalPoll::Rejected(error) => {
                    panic!("owner B exact terminal cleanup rejected: {error}")
                }
                QuinnLegActorReportedTerminalPoll::AlreadyConsumed => {
                    panic!("owner B terminal was consumed before FIFO cleanup")
                }
            }
        }
    })
    .await
    .expect("owner B observes the real peer terminal within the local bound")
}

#[tokio::test(flavor = "current_thread")]
async fn composition_owned_registered_b_dispatches_peer_probe_before_typestate_policy() {
    let RealUnregisteredStandbyFixture {
        mut listener,
        mut composition,
        mut server_a,
        client_b,
        mut server_b,
        tcp_factory,
        local_flows,
        peer_policy: _,
        mut owner_executor,
        owner_active_a,
    } = real_unregistered_standby_fixture_with_target_state(2, 1, true).await;
    let mut standby_slot = OwnerStandbySlot::new();
    let observed_at = composition
        .session_clock()
        .now()
        .expect("the client B dispatcher clock remains representable");
    register_owned_real_b(
        &mut composition,
        client_b,
        &mut server_b,
        &mut owner_executor,
        &owner_active_a,
        &mut standby_slot,
        observed_at,
    )
    .await;

    let sequence = ProbeSequence::new(41).expect("nonzero owner B probe sequence");
    assert_eq!(
        standby_slot
            .drive_exact_probe(
                sequence,
                server_b
                    .queue
                    .as_mut()
                    .expect("owner B owns its exact FIFO"),
            )
            .expect("owner B derives and admits its exact peer probe"),
        OwnerStandbyProbeDrive::Enqueued {
            probe_sequence: sequence,
        },
    );
    assert_eq!(
        server_b
            .try_flush(observed_at)
            .expect("owner B probe flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );

    let dispatched = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match composition.try_receive_registered_standby() {
                QuinnStandbyInboundDrive::Idle => tokio::task::yield_now().await,
                outcome => break outcome,
            }
        }
    })
    .await
    .expect("the physical B dispatcher makes bounded progress");
    assert!(matches!(
        dispatched,
        QuinnStandbyInboundDrive::ProbeAckQueued { probe_sequence }
            if probe_sequence == sequence
    ));
    assert_eq!(
        composition
            .try_flush_registered_standby(observed_at)
            .expect("client B ACK flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    let ack = tokio::time::timeout(TEST_TIMEOUT, server_b.recv_classified(STANDBY_CONTROL_V1))
        .await
        .expect("owner B ACK receive is bounded")
        .expect("owner B remains live")
        .expect("owner B receives one classified ACK");
    let ack = take_actor_inbound(ack);
    let BoundInbound::LegControl(bound) = &ack else {
        panic!("client B probe response entered the session namespace");
    };
    assert!(matches!(
        bound.parts().1.record(),
        LegControlRecord::LegProbeAck { probe_sequence, .. }
            if *probe_sequence == sequence
    ));
    standby_slot
        .observe_exact_probe_ack(ack, observed_at, Duration::from_millis(200))
        .expect("owner B consumes the exact ACK receipt before shutdown");
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match composition
                .try_flush_registered_standby(observed_at)
                .expect("client B ordered-delivery progress remains live")
            {
                QuinnLegActorFlushDisposition::Idle => break,
                QuinnLegActorFlushDisposition::OrderedDeliveryPending
                | QuinnLegActorFlushDisposition::Submitted => tokio::task::yield_now().await,
            }
        }
    })
    .await
    .expect("client B ACK delivery ownership drains within the test bound");
    wait_actor_ordered_delivery(&mut server_b).await;

    let (client_joined, server_a_joined, server_b_joined) =
        tokio::time::timeout(TEST_TIMEOUT, async {
            tokio::join!(
                composition.join_current_for_shutdown(),
                server_a.join(),
                server_b.join(),
            )
        })
        .await
        .expect("client B dispatcher actors join within the local bound");
    let client_joined = client_joined.expect("settled probe B joins without residual ownership");
    assert_eq!(client_joined.owned_messages(), 0);
    assert_eq!(client_joined.owned_bytes(), 0);
    assert_eq!(
        client_joined.owned_terminals(),
        1,
        "the direct API retains only current A's legacy terminal witness"
    );
    assert!(!client_joined.terminal_failure());
    assert_eq!(
        composition.standby_shutdown_terminal_reason(),
        Some(LegTransportTerminalReason::LocalCancelled),
    );
    let retired_b = composition
        .standby_shutdown_state_inventory()
        .expect("registered B has one explicit retirement witness");
    assert_eq!(retired_b.kind(), QuinnStandbyShutdownStateKind::Registered);
    assert_eq!(retired_b.authorities(), 1);
    assert_eq!(retired_b.pending_frames(), 0);
    assert_eq!(retired_b.pending_frame_bytes(), 0);
    assert_eq!(retired_b.receipts(), 1);
    assert_eq!(retired_b.timers(), 0);
    assert_eq!(retired_b.freshness(), 0);
    let (root_joined, root_error) = composition.join_all_for_shutdown().await.into_parts();
    assert!(
        root_error.is_none(),
        "repeat root shutdown returned {root_error:?}"
    );
    let root_joined = root_joined.expect("repeat root shutdown preserves joined witnesses");
    assert_eq!(root_joined.owned_messages(), 0);
    assert_eq!(root_joined.owned_bytes(), 0);
    assert_eq!(root_joined.owned_terminals(), 0);
    assert!(!root_joined.terminal_failure());
    assert_eq!(
        server_a_joined
            .expect("server A joins cleanly")
            .owned_messages(),
        0
    );
    assert_eq!(
        server_b_joined
            .expect("server B joins cleanly")
            .owned_messages(),
        0
    );
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
        .expect("client B dispatcher listener cleanup is bounded")
        .expect("client B dispatcher releases both actor slots");
}

#[tokio::test(flavor = "current_thread")]
async fn unprobed_physical_b_hint_is_final_before_semantic_callback() {
    let RealUnregisteredStandbyFixture {
        mut listener,
        mut composition,
        mut server_a,
        client_b,
        mut server_b,
        tcp_factory,
        local_flows,
        peer_policy: _,
        mut owner_executor,
        owner_active_a,
    } = real_unregistered_standby_fixture_with_target_state(2, 1, true).await;
    let mut standby_slot = OwnerStandbySlot::new();
    let observed_at = composition
        .session_clock()
        .now()
        .expect("unprobed physical hint clock remains representable");
    register_owned_real_b(
        &mut composition,
        client_b,
        &mut server_b,
        &mut owner_executor,
        &owner_active_a,
        &mut standby_slot,
        observed_at,
    )
    .await;

    let receipt = server_b
        .enqueue_ordered_leg_control(
            LegControlFrame::try_new(
                composition.snapshot().session.generation(),
                LegControlRecord::SwitchHint {
                    session_id: SessionId::new([0x93; 16]).expect("nonzero unprobed-hint session"),
                    standby_nonce: StandbyNonce::new([0x97; 16])
                        .expect("nonzero unprobed-hint standby nonce"),
                    hint_sequence: HintSequence::new(6).expect("nonzero unprobed-hint sequence"),
                    flow_id: local_flows[0].flow_id(),
                    direction: Direction::TargetToClient,
                    oldest_unacknowledged: ByteOffset::new(0),
                    cause: SwitchHintCause::ReverseApplicationAckStall,
                },
                STANDBY_CONTROL_V1,
            )
            .expect("well-formed unprobed physical B hint"),
            STANDBY_CONTROL_V1,
        )
        .expect("owner B admits the unprobed ordered hint");
    assert_eq!(
        server_b
            .try_flush(observed_at)
            .expect("unprobed B hint flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match composition.try_receive_registered_standby() {
                QuinnStandbyInboundDrive::Idle => tokio::task::yield_now().await,
                QuinnStandbyInboundDrive::SwitchHintPending { .. } => break,
                outcome => panic!("unprobed B hint reached wrong outcome: {outcome:?}"),
            }
        }
    })
    .await
    .expect("unprobed B hint reaches the physical gate");
    assert!(!receipt.was_delivered());

    let mut callback_called = false;
    assert!(matches!(
        composition.try_transact_pending_switch_hint(|_| {
            callback_called = true;
            QuinnStandbySwitchHintDecision::<(), ()>::Settled(())
        }),
        QuinnStandbySwitchHintTransaction::PhysicalRejected {
            kind: QuinnStandbyPhysicalSwitchHintReject::ProbeNotFresh,
            ..
        }
    ));
    assert!(
        !callback_called,
        "registration without an exact matching probe ACK cannot reach semantic policy",
    );
    wait_actor_ordered_delivery(&mut server_b).await;
    assert!(receipt.was_delivered());
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match composition
                .try_flush_registered_standby(observed_at)
                .expect("unprobed rejection delivery progress remains live")
            {
                QuinnLegActorFlushDisposition::Idle => break,
                QuinnLegActorFlushDisposition::OrderedDeliveryPending
                | QuinnLegActorFlushDisposition::Submitted => tokio::task::yield_now().await,
            }
        }
    })
    .await
    .expect("unprobed rejection ownership drains");

    let (shutdown, server_a_joined, server_b_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(
            composition.join_all_for_shutdown(),
            server_a.join(),
            server_b.join(),
        )
    })
    .await
    .expect("unprobed-hint actors join within the local bound");
    let (joined, shutdown_error) = shutdown.into_parts();
    assert!(
        shutdown_error.is_none(),
        "unprobed-hint shutdown returned {shutdown_error:?}",
    );
    let joined = joined.expect("unprobed-hint shutdown retains the joined witness");
    assert_eq!(joined.owned_messages(), 0);
    assert_eq!(joined.owned_bytes(), 0);
    assert_eq!(joined.owned_terminals(), 0);
    assert!(!joined.terminal_failure());
    let retired = composition
        .standby_shutdown_state_inventory()
        .expect("unprobed registered B is explicitly retired");
    assert_eq!(retired.kind(), QuinnStandbyShutdownStateKind::Registered);
    assert_eq!(retired.pending_frames(), 0);
    assert_eq!(retired.timers(), 0);
    assert_eq!(retired.freshness(), 0);
    assert_eq!(
        server_a_joined
            .expect("unprobed-hint server A joins structurally")
            .owned_messages(),
        0,
    );
    assert_eq!(
        server_b_joined
            .expect("unprobed-hint server B joins structurally")
            .owned_messages(),
        0,
    );
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
        .expect("unprobed-hint listener cleanup is bounded")
        .expect("unprobed-hint actors release both listener slots");
}

#[tokio::test(flavor = "current_thread")]
async fn composition_held_b_hint_retains_exact_arrival_until_root_settles_it() {
    let RealUnregisteredStandbyFixture {
        mut listener,
        mut composition,
        mut server_a,
        client_b,
        mut server_b,
        tcp_factory,
        local_flows,
        peer_policy: _,
        mut owner_executor,
        owner_active_a,
    } = real_unregistered_standby_fixture_with_target_state(2, 1, true).await;
    let mut standby_slot = OwnerStandbySlot::new();
    let observed_at = composition
        .session_clock()
        .now()
        .expect("the physical hint clock remains representable");
    register_owned_and_make_real_b_fresh(
        &mut composition,
        client_b,
        &mut server_b,
        &mut owner_executor,
        &owner_active_a,
        &mut standby_slot,
        observed_at,
    )
    .await;

    let wrong_generation = LegGeneration::new(1).expect("nonzero old raw-hint generation");
    let generation = composition.snapshot().session.generation();
    assert_ne!(
        wrong_generation, generation,
        "this tracer requires the old scalar-only fixture to name a non-current generation",
    );
    let session_id = SessionId::new([0x93; 16]).expect("nonzero physical hint session");
    let standby_nonce = StandbyNonce::new([0x97; 16]).expect("nonzero physical hint standby nonce");
    let flow_id = local_flows[0].flow_id();

    let wrong_receipt = server_b
        .enqueue_ordered_leg_control(
            LegControlFrame::try_new(
                wrong_generation,
                LegControlRecord::SwitchHint {
                    session_id: session_id.clone(),
                    standby_nonce,
                    hint_sequence: HintSequence::new(7)
                        .expect("nonzero wrong-generation hint sequence"),
                    flow_id,
                    direction: Direction::TargetToClient,
                    oldest_unacknowledged: ByteOffset::new(0),
                    cause: SwitchHintCause::ReverseApplicationAckStall,
                },
                STANDBY_CONTROL_V1,
            )
            .expect("well-formed but wrong-generation physical B hint"),
            STANDBY_CONTROL_V1,
        )
        .expect("owner B admits the wrong-generation ordered hint");
    assert_eq!(
        server_b
            .try_flush(observed_at)
            .expect("wrong-generation B hint flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match composition.try_receive_registered_standby() {
                QuinnStandbyInboundDrive::Idle => tokio::task::yield_now().await,
                QuinnStandbyInboundDrive::SwitchHintPending { .. } => break,
                outcome => panic!("wrong-generation B hint reached wrong outcome: {outcome:?}"),
            }
        }
    })
    .await
    .expect("wrong-generation B hint reaches the physical gate");
    let mut wrong_callback_called = false;
    assert!(matches!(
        composition.try_transact_pending_switch_hint(|_| {
            wrong_callback_called = true;
            QuinnStandbySwitchHintDecision::<(), ()>::Settled(())
        }),
        QuinnStandbySwitchHintTransaction::PhysicalRejected {
            kind: QuinnStandbyPhysicalSwitchHintReject::WrongGeneration,
            ..
        }
    ));
    assert!(
        !wrong_callback_called,
        "a wrong-generation physical hint must be final before semantic policy",
    );
    wait_actor_ordered_delivery(&mut server_b).await;
    assert!(wrong_receipt.was_delivered());
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match composition
                .try_flush_registered_standby(observed_at)
                .expect("wrong-generation hint settlement remains live")
            {
                QuinnLegActorFlushDisposition::Idle => break,
                QuinnLegActorFlushDisposition::OrderedDeliveryPending
                | QuinnLegActorFlushDisposition::Submitted => tokio::task::yield_now().await,
            }
        }
    })
    .await
    .expect("wrong-generation hint delivery ownership drains");

    let hint_sequence = HintSequence::new(8).expect("nonzero physical hint sequence");
    let receipt = server_b
        .enqueue_ordered_leg_control(
            LegControlFrame::try_new(
                generation,
                LegControlRecord::SwitchHint {
                    session_id: session_id.clone(),
                    standby_nonce,
                    hint_sequence,
                    flow_id,
                    direction: Direction::TargetToClient,
                    oldest_unacknowledged: ByteOffset::new(0),
                    cause: SwitchHintCause::ReverseApplicationAckStall,
                },
                STANDBY_CONTROL_V1,
            )
            .expect("well-formed physical B hint"),
            STANDBY_CONTROL_V1,
        )
        .expect("owner B admits the exact ordered hint");
    assert_eq!(
        server_b
            .try_flush(observed_at)
            .expect("owner B hint flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    let first_metadata = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match composition.try_receive_registered_standby() {
                QuinnStandbyInboundDrive::Idle => tokio::task::yield_now().await,
                QuinnStandbyInboundDrive::SwitchHintPending {
                    arrived_at,
                    publication_epoch,
                    send_ordinal,
                    encoded_bytes,
                } => break (arrived_at, publication_epoch, send_ordinal, encoded_bytes),
                outcome => {
                    panic!("physical B hint reached the wrong dispatcher outcome: {outcome:?}")
                }
            }
        }
    })
    .await
    .expect("physical B hint dispatch remains bounded");
    assert!(!receipt.was_delivered());

    let mut exact_actor_seal = None;
    let retained = composition.try_transact_pending_switch_hint(|hint| {
        assert_eq!(hint.session_id(), session_id);
        assert_eq!(hint.generation(), generation);
        assert_eq!(hint.standby_nonce(), standby_nonce);
        assert_eq!(hint.hint_sequence(), hint_sequence);
        assert_eq!(hint.flow_id(), flow_id);
        assert_eq!(hint.direction(), Direction::TargetToClient);
        assert_eq!(hint.oldest_unacknowledged(), ByteOffset::new(0));
        assert_eq!(hint.cause(), SwitchHintCause::ReverseApplicationAckStall);
        assert_eq!(hint.arrived_at(), first_metadata.0);
        exact_actor_seal = Some(hint.actor_seal() as *const _);
        QuinnStandbySwitchHintDecision::<(), _>::Retain("arbiter-pressure")
    });
    let QuinnStandbySwitchHintTransaction::Retained { blocker, metadata } = retained else {
        panic!("arbiter pressure did not retain the exact physical B hint");
    };
    assert_eq!(blocker, "arbiter-pressure");
    assert_eq!(
        (
            metadata.arrived_at(),
            metadata.publication_epoch(),
            metadata.send_ordinal(),
            metadata.encoded_bytes(),
        ),
        first_metadata,
    );
    assert!(!receipt.was_delivered());

    assert!(matches!(
        composition.try_receive_registered_standby(),
        QuinnStandbyInboundDrive::SwitchHintPending {
            arrived_at,
            publication_epoch,
            send_ordinal,
            encoded_bytes,
        } if (arrived_at, publication_epoch, send_ordinal, encoded_bytes) == first_metadata
    ));
    let settled = composition.try_transact_pending_switch_hint(|hint| {
        assert_eq!(hint.hint_sequence(), hint_sequence);
        assert_eq!(Some(hint.actor_seal() as *const _), exact_actor_seal);
        QuinnStandbySwitchHintDecision::<_, ()>::Settled("semantic-rejected")
    });
    let QuinnStandbySwitchHintTransaction::Settled { outcome, metadata } = settled else {
        panic!("typed semantic rejection did not settle the exact physical B hint");
    };
    assert_eq!(outcome, "semantic-rejected");
    assert_eq!(
        (
            metadata.arrived_at(),
            metadata.publication_epoch(),
            metadata.send_ordinal(),
            metadata.encoded_bytes(),
        ),
        first_metadata,
    );
    wait_actor_ordered_delivery(&mut server_b).await;
    assert!(receipt.was_delivered());
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match composition
                .try_flush_registered_standby(observed_at)
                .expect("settled B hint delivery progress remains live")
            {
                QuinnLegActorFlushDisposition::Idle => break,
                QuinnLegActorFlushDisposition::OrderedDeliveryPending
                | QuinnLegActorFlushDisposition::Submitted => tokio::task::yield_now().await,
            }
        }
    })
    .await
    .expect("settled B hint delivery ownership drains within the test bound");
    assert!(matches!(
        composition.try_transact_pending_switch_hint(|_| {
            QuinnStandbySwitchHintDecision::<(), ()>::Settled(())
        }),
        QuinnStandbySwitchHintTransaction::NoPendingHint
    ));

    tokio::time::sleep(Duration::from_millis(225)).await;
    let stale_flush_at = composition
        .session_clock()
        .now()
        .expect("stale-hint B clock remains representable");
    let stale_receipt = server_b
        .enqueue_ordered_leg_control(
            LegControlFrame::try_new(
                generation,
                LegControlRecord::SwitchHint {
                    session_id: session_id.clone(),
                    standby_nonce,
                    hint_sequence: HintSequence::new(9)
                        .expect("nonzero stale physical hint sequence"),
                    flow_id,
                    direction: Direction::TargetToClient,
                    oldest_unacknowledged: ByteOffset::new(0),
                    cause: SwitchHintCause::ReverseApplicationAckStall,
                },
                STANDBY_CONTROL_V1,
            )
            .expect("well-formed stale physical B hint"),
            STANDBY_CONTROL_V1,
        )
        .expect("owner B admits the stale ordered hint");
    assert_eq!(
        server_b
            .try_flush(stale_flush_at)
            .expect("stale B hint flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match composition.try_receive_registered_standby() {
                QuinnStandbyInboundDrive::Idle => tokio::task::yield_now().await,
                QuinnStandbyInboundDrive::SwitchHintPending { .. } => break,
                outcome => panic!("stale B hint reached wrong outcome: {outcome:?}"),
            }
        }
    })
    .await
    .expect("stale B hint reaches the physical gate");
    let mut stale_callback_called = false;
    assert!(matches!(
        composition.try_transact_pending_switch_hint(|_| {
            stale_callback_called = true;
            QuinnStandbySwitchHintDecision::<(), ()>::Settled(())
        }),
        QuinnStandbySwitchHintTransaction::PhysicalRejected {
            kind: QuinnStandbyPhysicalSwitchHintReject::Stale,
            ..
        }
    ));
    assert!(
        !stale_callback_called,
        "stale physical B evidence must be final before semantic policy",
    );
    wait_actor_ordered_delivery(&mut server_b).await;
    assert!(stale_receipt.was_delivered());
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match composition
                .try_flush_registered_standby(stale_flush_at)
                .expect("stale hint settlement remains live")
            {
                QuinnLegActorFlushDisposition::Idle => break,
                QuinnLegActorFlushDisposition::OrderedDeliveryPending
                | QuinnLegActorFlushDisposition::Submitted => tokio::task::yield_now().await,
            }
        }
    })
    .await
    .expect("stale hint delivery ownership drains");

    let (client_joined, server_a_joined, server_b_joined) =
        tokio::time::timeout(TEST_TIMEOUT, async {
            tokio::join!(
                composition.join_all_for_shutdown(),
                server_a.join(),
                server_b.join(),
            )
        })
        .await
        .expect("physical hint actors join within the local bound");
    let (client_joined, shutdown_error) = client_joined.into_parts();
    assert!(
        shutdown_error.is_none(),
        "settled hint shutdown returned {shutdown_error:?}"
    );
    let client_joined = client_joined.expect("settled hint root preserves joined witnesses");
    assert_eq!(client_joined.owned_messages(), 0);
    assert_eq!(client_joined.owned_bytes(), 0);
    assert_eq!(client_joined.owned_terminals(), 0);
    assert!(!client_joined.terminal_failure());
    assert_eq!(
        composition.standby_shutdown_terminal_reason(),
        Some(LegTransportTerminalReason::LocalCancelled),
    );
    let retired_b = composition
        .standby_shutdown_state_inventory()
        .expect("fresh B has one explicit retirement witness");
    assert_eq!(retired_b.kind(), QuinnStandbyShutdownStateKind::Fresh);
    assert_eq!(retired_b.authorities(), 1);
    assert_eq!(retired_b.pending_frames(), 0);
    assert_eq!(retired_b.pending_frame_bytes(), 0);
    assert_eq!(retired_b.receipts(), 2);
    assert_eq!(retired_b.timers(), 0);
    assert_eq!(retired_b.freshness(), 1);
    assert_eq!(
        server_a_joined
            .expect("server A joins cleanly")
            .owned_messages(),
        0
    );
    assert_eq!(
        server_b_joined
            .expect("server B joins cleanly")
            .owned_messages(),
        0
    );
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
        .expect("physical hint listener cleanup is bounded")
        .expect("physical hint actors release both listener slots");
}

#[tokio::test(flavor = "current_thread")]
async fn shutdown_discards_exact_held_b_hint_before_join_and_keeps_copy_audit() {
    let RealUnregisteredStandbyFixture {
        mut listener,
        mut composition,
        mut server_a,
        client_b,
        mut server_b,
        tcp_factory,
        local_flows,
        peer_policy: _,
        mut owner_executor,
        owner_active_a,
    } = real_unregistered_standby_fixture_with_target_state(2, 1, true).await;
    let mut standby_slot = OwnerStandbySlot::new();
    let observed_at = composition
        .session_clock()
        .now()
        .expect("held-hint shutdown clock remains representable");
    register_owned_and_make_real_b_fresh(
        &mut composition,
        client_b,
        &mut server_b,
        &mut owner_executor,
        &owner_active_a,
        &mut standby_slot,
        observed_at,
    )
    .await;

    let hint = LegControlFrame::try_new(
        LegGeneration::new(1).expect("nonzero held-hint generation"),
        LegControlRecord::SwitchHint {
            session_id: SessionId::new([0x93; 16]).expect("nonzero held-hint session"),
            standby_nonce: StandbyNonce::new([0x97; 16]).expect("nonzero held-hint standby nonce"),
            hint_sequence: HintSequence::new(81).expect("nonzero held-hint sequence"),
            flow_id: local_flows[0].flow_id(),
            direction: Direction::TargetToClient,
            oldest_unacknowledged: ByteOffset::new(0),
            cause: SwitchHintCause::ReverseApplicationAckStall,
        },
        STANDBY_CONTROL_V1,
    )
    .expect("well-formed held physical B hint");
    let receipt = server_b
        .enqueue_ordered_leg_control(hint, STANDBY_CONTROL_V1)
        .expect("owner B admits the held shutdown hint");
    assert_eq!(
        server_b
            .try_flush(observed_at)
            .expect("held shutdown hint flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    let pending_metadata = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match composition.try_receive_registered_standby() {
                QuinnStandbyInboundDrive::Idle => tokio::task::yield_now().await,
                QuinnStandbyInboundDrive::SwitchHintPending {
                    arrived_at,
                    publication_epoch,
                    send_ordinal,
                    encoded_bytes,
                } => break (arrived_at, publication_epoch, send_ordinal, encoded_bytes),
                outcome => panic!("held shutdown hint reached wrong outcome: {outcome:?}"),
            }
        }
    })
    .await
    .expect("held shutdown hint reaches the dispatcher");
    assert!(!receipt.was_delivered());

    let (shutdown, server_a_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(composition.join_all_for_shutdown(), server_a.join())
    })
    .await
    .expect("held-hint client shutdown and current peer join are bounded");
    let _peer_reason =
        settle_owner_b_wait_after_real_peer_close(&mut server_b, &mut standby_slot).await;
    assert!(
        !receipt.was_delivered(),
        "shutdown discard must not synthesize a transport DELIVERED"
    );
    let server_b_joined = tokio::time::timeout(TEST_TIMEOUT, server_b.join())
        .await
        .expect("terminal-settled held-hint peer join is bounded");
    let (joined, shutdown_error) = shutdown.into_parts();
    assert!(
        shutdown_error.is_none(),
        "held-hint shutdown returned {shutdown_error:?}"
    );
    let joined = joined.expect("held-hint shutdown retains aggregate joined witness");
    assert_eq!(joined.owned_messages(), 0);
    assert_eq!(joined.owned_bytes(), 0);
    assert_eq!(joined.owned_terminals(), 0);
    assert!(!joined.terminal_failure());
    let discarded = composition
        .standby_shutdown_control_discard()
        .expect("held hint commits before its copy-only shutdown audit");
    assert_eq!(discarded.kind(), QuinnStandbyPendingControlKind::SwitchHint);
    assert_eq!(discarded.messages(), 1);
    assert_eq!(discarded.bytes(), pending_metadata.3);
    assert_eq!(discarded.arrived_at(), pending_metadata.0);
    assert_eq!(discarded.publication_epoch(), pending_metadata.1);
    assert_eq!(discarded.send_ordinal(), pending_metadata.2);
    assert_eq!(
        composition.standby_shutdown_terminal_reason(),
        Some(LegTransportTerminalReason::LocalCancelled),
    );
    assert_eq!(
        composition
            .standby_shutdown_state_inventory()
            .expect("fresh B state is explicitly retired")
            .kind(),
        QuinnStandbyShutdownStateKind::Fresh,
    );
    assert_eq!(
        server_a_joined
            .expect("held-hint server A joins structurally")
            .owned_messages(),
        0,
    );
    assert_eq!(
        server_b_joined
            .expect("held-hint server B joins structurally")
            .owned_messages(),
        0,
    );
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
        .expect("held-hint listener cleanup is bounded")
        .expect("held-hint shutdown releases both actor slots");
}

#[tokio::test(flavor = "current_thread")]
async fn shutdown_discards_exact_pressure_held_probe_before_join() {
    let RealUnregisteredStandbyFixture {
        mut listener,
        mut composition,
        mut server_a,
        client_b,
        mut server_b,
        tcp_factory,
        local_flows,
        peer_policy: _,
        mut owner_executor,
        owner_active_a,
    } = real_unregistered_standby_fixture_with_target_state(2, 1, true).await;
    let mut standby_slot = OwnerStandbySlot::new();
    let observed_at = composition
        .session_clock()
        .now()
        .expect("held-probe shutdown clock remains representable");
    register_owned_real_b(
        &mut composition,
        client_b,
        &mut server_b,
        &mut owner_executor,
        &owner_active_a,
        &mut standby_slot,
        observed_at,
    )
    .await;

    let generation = LegGeneration::new(1).expect("nonzero B generation");
    for sequence in 201..205 {
        assert!(
            composition
                .enqueue_registered_standby_control_for_test(ordered_control_for_generation(
                    generation, sequence
                ),)
                .is_ok(),
            "the real B FIFO accepts exactly four pressure fillers",
        );
    }
    let probe_sequence = ProbeSequence::new(91).expect("nonzero peer probe sequence");
    assert_eq!(
        standby_slot
            .drive_exact_probe(
                probe_sequence,
                server_b
                    .queue
                    .as_mut()
                    .expect("owner B owns its exact FIFO"),
            )
            .expect("owner B admits its exact pressure probe"),
        OwnerStandbyProbeDrive::Enqueued { probe_sequence },
    );
    assert_eq!(
        server_b
            .try_flush(observed_at)
            .expect("pressure probe flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    let pressured = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match composition.try_receive_registered_standby() {
                QuinnStandbyInboundDrive::Idle => tokio::task::yield_now().await,
                outcome => break outcome,
            }
        }
    })
    .await
    .expect("pressure probe reaches the dispatcher");
    assert!(matches!(
        pressured,
        QuinnStandbyInboundDrive::ProbeAckPressure { probe_sequence: observed }
            if observed == probe_sequence
    ));
    for _ in 0..4 {
        tokio::time::timeout(TEST_TIMEOUT, async {
            loop {
                match composition
                    .try_flush_registered_standby(observed_at)
                    .expect("pressure filler flush remains live")
                {
                    QuinnLegActorFlushDisposition::Submitted => break,
                    QuinnLegActorFlushDisposition::OrderedDeliveryPending => {
                        tokio::task::yield_now().await;
                    }
                    QuinnLegActorFlushDisposition::Idle => {
                        panic!("a pressure filler disappeared before submission")
                    }
                }
            }
        })
        .await
        .expect("each pressure filler submission is bounded");
        let filler =
            tokio::time::timeout(TEST_TIMEOUT, server_b.recv_classified(STANDBY_CONTROL_V1))
                .await
                .expect("pressure filler receive is bounded")
                .expect("owner B remains live while fillers drain")
                .expect("every pressure filler reaches physical B");
        drop(take_actor_inbound(filler));
    }
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match composition
                .try_flush_registered_standby(observed_at)
                .expect("pressure filler delivery progress remains live")
            {
                QuinnLegActorFlushDisposition::Idle => break,
                QuinnLegActorFlushDisposition::OrderedDeliveryPending
                | QuinnLegActorFlushDisposition::Submitted => tokio::task::yield_now().await,
            }
        }
    })
    .await
    .expect("all real pressure filler ownership drains before cutoff");

    let (shutdown, server_a_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(composition.join_all_for_shutdown(), server_a.join())
    })
    .await
    .expect("pressure-held client shutdown and current peer join are bounded");
    let _peer_reason =
        settle_owner_b_wait_after_real_peer_close(&mut server_b, &mut standby_slot).await;
    let server_b_joined = tokio::time::timeout(TEST_TIMEOUT, server_b.join())
        .await
        .expect("terminal-settled pressure peer join is bounded");
    let (joined, shutdown_error) = shutdown.into_parts();
    assert!(
        shutdown_error.is_none(),
        "pressure-held probe shutdown returned {shutdown_error:?}"
    );
    let joined = joined.expect("pressure-held shutdown retains aggregate joined witness");
    assert_eq!(joined.owned_messages(), 0);
    assert_eq!(joined.owned_bytes(), 0);
    assert_eq!(joined.owned_terminals(), 0);
    assert!(!joined.terminal_failure());
    let discarded = composition
        .standby_shutdown_control_discard()
        .expect("held probe commits before its copy-only shutdown audit");
    assert_eq!(discarded.kind(), QuinnStandbyPendingControlKind::ProbeAck);
    assert_eq!(discarded.messages(), 1);
    assert!(discarded.bytes() > 0);
    assert_eq!(
        composition
            .standby_shutdown_state_inventory()
            .expect("registered B state is explicitly retired")
            .kind(),
        QuinnStandbyShutdownStateKind::Registered,
    );
    assert_eq!(
        server_a_joined
            .expect("pressure-held server A joins structurally")
            .owned_messages(),
        0,
    );
    assert_eq!(
        server_b_joined
            .expect("pressure-held server B joins structurally")
            .owned_messages(),
        0,
    );
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
        .expect("pressure-held listener cleanup is bounded")
        .expect("pressure-held shutdown releases both actor slots");
}

#[tokio::test(flavor = "current_thread")]
async fn shutdown_pending_ordered_wait_is_snapshot_inert_and_retryable() {
    let RealUnregisteredStandbyFixture {
        mut listener,
        mut composition,
        mut server_a,
        client_b,
        mut server_b,
        tcp_factory,
        local_flows,
        peer_policy: _,
        mut owner_executor,
        owner_active_a,
    } = real_unregistered_standby_fixture_with_target_state(2, 1, true).await;
    let mut standby_slot = OwnerStandbySlot::new();
    let observed_at = composition
        .session_clock()
        .now()
        .expect("shutdown retry clock remains representable");
    register_owned_real_b(
        &mut composition,
        client_b,
        &mut server_b,
        &mut owner_executor,
        &owner_active_a,
        &mut standby_slot,
        observed_at,
    )
    .await;

    let delayed_delivery = Arc::clone(&server_b.delivery_writer_gate);
    delayed_delivery.arm();
    composition
        .enqueue_registered_standby_control_for_test(ordered_control_for_generation(
            LegGeneration::new(1).expect("nonzero retry-test B generation"),
            301,
        ))
        .expect("registered B admits one retry-test ordered frame");
    assert_eq!(
        composition
            .try_flush_registered_standby(observed_at)
            .expect("retry-test B flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    let inbound = tokio::time::timeout(TEST_TIMEOUT, server_b.recv_classified(STANDBY_CONTROL_V1))
        .await
        .expect("retry-test peer receive is bounded")
        .expect("retry-test peer remains live")
        .expect("retry-test frame reaches physical B");
    drop(take_actor_inbound(inbound));
    tokio::time::timeout(TEST_TIMEOUT, delayed_delivery.wait_until_blocked())
        .await
        .expect("peer holds the real DELIVERED before publication");

    let (first, server_a_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(composition.join_all_for_shutdown(), server_a.join())
    })
    .await
    .expect("first shutdown retry observation is bounded");
    let (first_joined, first_error) = first.into_parts();
    assert!(first_joined.is_none());
    assert!(matches!(
        first_error,
        Some(QuinnCurrentJoinError::StandbyOutboundRetirementPending {
            queued_frames: 0,
            queued_bytes: 0,
            ordered_delivery_wait: true,
        })
    ));
    assert!(
        composition.standby_shutdown_retry_is_retained_for_test(),
        "pending wait keeps exact B Running and composition ShuttingDown",
    );

    delayed_delivery.release();
    let (joined, retry_error) = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let retry = composition.join_all_for_shutdown().await;
            let (joined, error) = retry.into_parts();
            if joined.is_some() {
                break (joined, error);
            }
            assert!(matches!(
                error,
                Some(QuinnCurrentJoinError::StandbyOutboundRetirementPending {
                    queued_frames: 0,
                    queued_bytes: 0,
                    ordered_delivery_wait: true,
                })
            ));
            assert!(composition.standby_shutdown_retry_is_retained_for_test());
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("real DELIVERED makes the retained shutdown retry progress");
    assert!(
        retry_error.is_none(),
        "shutdown retry returned {retry_error:?}"
    );
    let joined = joined.expect("shutdown retry returns aggregate joined inventory");
    assert_eq!(joined.owned_messages(), 0);
    assert_eq!(joined.owned_bytes(), 0);
    assert_eq!(joined.owned_terminals(), 0);
    assert!(!joined.terminal_failure());
    assert_eq!(
        server_a_joined
            .expect("retry-test server A joins structurally")
            .owned_messages(),
        0,
    );
    let server_b_joined = tokio::time::timeout(TEST_TIMEOUT, server_b.join())
        .await
        .expect("retry-test server B join is bounded")
        .expect("retry-test server B joins structurally");
    assert_eq!(server_b_joined.owned_messages(), 0);
    assert_eq!(server_b_joined.owned_bytes(), 0);
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
        .expect("retry-test listener cleanup is bounded")
        .expect("retry-test shutdown releases both actor slots");
}

#[tokio::test(flavor = "current_thread")]
async fn fatal_b_shutdown_is_sticky_across_repeated_root_observation() {
    let RealUnregisteredStandbyFixture {
        mut listener,
        mut composition,
        mut server_a,
        client_b,
        mut server_b,
        tcp_factory,
        local_flows,
        peer_policy: _,
        mut owner_executor,
        owner_active_a,
    } = real_unregistered_standby_fixture_with_target_state(2, 1, true).await;
    let mut standby_slot = OwnerStandbySlot::new();
    let observed_at = composition
        .session_clock()
        .now()
        .expect("fatal B shutdown clock remains representable");
    register_owned_real_b(
        &mut composition,
        client_b,
        &mut server_b,
        &mut owner_executor,
        &owner_active_a,
        &mut standby_slot,
        observed_at,
    )
    .await;
    server_b.request_cancel();
    let server_b_joined = tokio::time::timeout(TEST_TIMEOUT, server_b.join())
        .await
        .expect("hard-failure peer B cleanup is bounded")
        .expect("hard-failure peer B joins structurally");
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match composition.try_receive_registered_standby() {
                QuinnStandbyInboundDrive::Actor(_) | QuinnStandbyInboundDrive::StandbyFailed => {
                    break;
                }
                QuinnStandbyInboundDrive::Idle => tokio::task::yield_now().await,
                outcome => panic!("hard B terminal reached wrong dispatcher outcome: {outcome:?}"),
            }
        }
    })
    .await
    .expect("physical peer close becomes the sticky B hard terminal");

    let (first, server_a_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(composition.join_all_for_shutdown(), server_a.join())
    })
    .await
    .expect("fatal B shutdown structurally joins every retained actor");
    let (first_joined, first_error) = first.into_parts();
    let first_joined = first_joined.expect("fatal B still returns aggregate joined inventory");
    assert_eq!(first_joined.owned_messages(), 0);
    assert_eq!(first_joined.owned_bytes(), 0);
    assert_eq!(first_joined.owned_terminals(), 1);
    assert!(first_joined.terminal_failure());
    let first_reason = match first_error {
        Some(QuinnCurrentJoinError::StandbyTerminalRejected {
            reason: Some(reason),
            terminal_failure: true,
        }) => reason,
        other => panic!("hard B terminal lost its typed rejection: {other:?}"),
    };
    assert!(matches!(
        first_reason,
        LegTransportTerminalReason::PeerClosed | LegTransportTerminalReason::Reset
    ));
    assert_eq!(composition.standby_shutdown_terminal_reason(), None);
    assert_eq!(composition.standby_shutdown_state_inventory(), None);

    let repeated = composition.join_all_for_shutdown().await;
    let (repeated_joined, repeated_error) = repeated.into_parts();
    let repeated_joined = repeated_joined.expect("repeat retains fatal B joined inventory");
    assert_eq!(repeated_joined, first_joined);
    assert!(matches!(
        repeated_error,
        Some(QuinnCurrentJoinError::StandbyTerminalRejected {
            reason: Some(reason),
            terminal_failure: true,
        }) if reason == first_reason
    ));
    assert_eq!(composition.standby_shutdown_terminal_reason(), None);
    assert_eq!(composition.standby_shutdown_state_inventory(), None);
    assert_eq!(
        server_a_joined
            .expect("fatal-B server A joins structurally")
            .owned_messages(),
        0,
    );
    assert_eq!(server_b_joined.owned_messages(), 0);
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
        .expect("fatal-B listener cleanup is bounded")
        .expect("fatal-B shutdown releases both actor slots");
}
