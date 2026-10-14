use std::sync::Arc;
use std::time::Duration;

use super::super::{
    ActorFirstFailure, ActorTerminalStampPair, QuinnLegActorError, QuinnLegActorReportedTerminal,
    QuinnLegActorReportedTerminalPoll,
};
use super::*;
use crate::owned_upstream::quinn_leg_composition::QuinnLegCompositionGate;
use crate::owned_upstream::session_clock::SessionMonotonicClock;
use crate::owned_upstream::two_leg::SimTime;

/// The late deadline loser deliberately receives no transport DELIVERED.
/// Settle its peer-side ordered wait only under the real peer terminal before
/// asking the raw peer actor for its structural join witness.
async fn settle_raw_peer_wait_after_real_close(server: &mut StartedQuinnLegActor) {
    let gate = QuinnLegCompositionGate::mint_for_test();
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let observed = server
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
                                "terminal did not bind the exact raw-peer FIFO",
                            ));
                        };
                        Ok((reason, cleanup.commit()))
                    },
                )
                .expect("raw-peer terminal transaction remains structurally available");
            match observed {
                QuinnLegActorReportedTerminalPoll::Pending => tokio::task::yield_now().await,
                QuinnLegActorReportedTerminalPoll::Applied((reason, discarded)) => {
                    assert!(matches!(
                        reason,
                        LegTransportTerminalReason::PeerClosed | LegTransportTerminalReason::Reset
                    ));
                    assert_eq!(discarded.queued_frames(), 0);
                    assert_eq!(discarded.summed_frame_bytes(), 0);
                    assert_eq!(discarded.stored_owned_bytes(), 0);
                    assert!(discarded.ordered_delivery_wait());
                    break;
                }
                QuinnLegActorReportedTerminalPoll::Rejected(error) => {
                    panic!("raw-peer exact terminal cleanup rejected: {error}")
                }
                QuinnLegActorReportedTerminalPoll::AlreadyConsumed => {
                    panic!("raw-peer terminal was consumed before FIFO cleanup")
                }
            }
        }
    })
    .await
    .expect("raw peer observes the real terminal within the local bound");
}

#[tokio::test(start_paused = true)]
async fn first_fatal_keeps_the_exact_session_time_across_late_cleanup() {
    let clock = SessionMonotonicClock::start();
    let first_failure = Arc::new(ActorFirstFailure::new(clock));
    let ActorTerminalStampPair {
        handoff: first_stamp,
        root_outcome: second_stamp,
    } = ActorTerminalStampPair::mint(&first_failure);

    tokio::time::advance(Duration::from_nanos(11)).await;
    let first = first_failure
        .record(QuinnLegActorError::Protocol("first hard terminal"))
        .expect("the open actor accepts its first fatal");
    assert_eq!(
        first_failure.failure_observed_at(),
        Some(SimTime::from_nanos(11)),
    );

    tokio::time::advance(Duration::from_nanos(37)).await;
    let repeated = first_failure
        .record(QuinnLegActorError::Protocol("late cleanup failure"))
        .expect("the first fatal remains observable");
    assert!(std::sync::Arc::ptr_eq(&first, &repeated));
    assert_eq!(
        first_failure.failure_observed_at(),
        Some(SimTime::from_nanos(11)),
        "cleanup and join latency must not recapture the hard-terminal time",
    );
    assert!(first_stamp.same_source(&second_stamp));
}

#[tokio::test(start_paused = true)]
async fn local_shutdown_winner_never_mints_a_hard_terminal_time() {
    let first_failure = ActorFirstFailure::new(SessionMonotonicClock::start());

    tokio::time::advance(Duration::from_nanos(5)).await;
    first_failure.mark_local_shutdown_if_open();
    tokio::time::advance(Duration::from_nanos(29)).await;

    assert!(
        first_failure
            .record(QuinnLegActorError::Protocol("late peer close"))
            .is_none(),
    );
    assert_eq!(first_failure.failure_observed_at(), None);
}

#[tokio::test(flavor = "current_thread")]
async fn probe_deadline_stamp_survives_live_handoff_delayed_cleanup_and_join() {
    let limits = composition_actor_limits();
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let peer_policy = AttachPeerPolicy::new(
        OwnerIdentity::new([0xd3; 32]).expect("nonzero owner identity"),
        DevicePrincipal::new([0xd4; 16]).expect("nonzero device principal"),
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
    .expect("bind shared actor listener");
    let (mut client, mut server) =
        connect_real_actor_pair(&mut listener, LegId::A, limits, peer_policy).await;
    let (accepted, owner_bootstrap) = complete_initial_actor_attach(&mut client, &mut server).await;
    wait_actor_ordered_delivery(&mut server).await;
    let mut composition =
        QuinnClientSessionComposition::bootstrap_initial(actor_session_config(), accepted, client)
            .expect("composition consumes exact current A");
    let observed_at = composition
        .session_clock()
        .now()
        .expect("shared actor clock remains representable");
    let probe_timeout = Duration::from_millis(30);
    let expected_deadline = observed_at
        .checked_add(probe_timeout)
        .expect("bounded probe deadline");

    let (_generation, physical_cleanup) = apply_real_silent_current_probe_loss(
        &mut composition,
        &mut server,
        observed_at,
        probe_timeout,
    )
    .await;
    assert_eq!(
        composition.current_terminal_failure_observed_at(),
        Some(expected_deadline),
        "late root service must retain the opaque timer's absolute instant",
    );
    let live_stamp_source = composition
        .current_terminal_stamp_for_test()
        .expect("applied hard terminal retains one shared witness")
        .source_id_for_test();

    tokio::time::sleep(Duration::from_millis(5)).await;
    assert_eq!(
        composition.current_terminal_failure_observed_at(),
        Some(expected_deadline),
    );
    physical_cleanup.release();
    settle_raw_peer_wait_after_real_close(&mut server).await;
    let (client_joined, server_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(composition.join_current_for_shutdown(), server.join())
    })
    .await
    .expect("probe-loss actors join within the local bound");
    let client_joined = client_joined.expect("probe-loss client joins structurally");
    let server_joined = server_joined.expect("probe-loss peer joins structurally");
    assert_eq!(client_joined.owned_messages(), 0);
    assert_eq!(client_joined.owned_bytes(), 0);
    assert_eq!(server_joined.owned_messages(), 0);
    assert_eq!(server_joined.owned_bytes(), 0);
    let joined_stamp = composition
        .current_joined_terminal_stamp_for_test()
        .expect("joined actor retains the same terminal witness");
    assert_eq!(live_stamp_source, joined_stamp.source_id_for_test());
    assert_eq!(joined_stamp.observed_at(), Some(expected_deadline));

    drop((composition, owner_bootstrap));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("listener cleanup is bounded")
        .expect("probe-loss tracer releases the listener slot");
}
