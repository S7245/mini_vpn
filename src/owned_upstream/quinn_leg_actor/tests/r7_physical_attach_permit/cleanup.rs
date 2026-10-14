//! Test-only shutdown settlement for the physical-attach fixtures.
//!
//! This module owns external peer-actor cancellation, join observation, and
//! exact terminal/stamp consumption. It does not own production composition or
//! controller settlement. Tests enter through the two fixture cleanup helpers
//! or the raw joined-actor settlement helper.

use super::*;
use crate::owned_upstream::quinn_leg_actor::QuinnLegActorJoined;

pub(super) fn settle_raw_local_cancelled_actor_join(
    stage: &'static str,
    actor_label: &'static str,
    mut joined: QuinnLegActorJoined,
) {
    assert_eq!(
        joined.owned_messages(),
        0,
        "{stage}: {actor_label} joined with residual messages"
    );
    assert_eq!(
        joined.owned_bytes(),
        0,
        "{stage}: {actor_label} joined with residual bytes"
    );
    assert_eq!(
        joined.owned_terminals(),
        1,
        "{stage}: {actor_label} must retain exactly one local-cancel terminal"
    );
    assert!(
        joined.terminal_failure().is_none(),
        "{stage}: {actor_label} local cancellation must not retain a fatal error"
    );
    let gate =
        crate::owned_upstream::quinn_leg_composition::QuinnLegCompositionGate::mint_for_test();
    let reason = joined
        .transact_terminal(&gate, |_runtime, terminal| match terminal {
            Some(terminal) => Ok(terminal.reason()),
            None => Err((None, ())),
        })
        .unwrap_or_else(|()| panic!("{stage}: {actor_label} lost its exact terminal"));
    assert_eq!(
        reason,
        LegTransportTerminalReason::LocalCancelled,
        "{stage}: {actor_label} must retain the synchronous local-cancel outcome"
    );
    let stamp = joined
        .take_terminal_stamp()
        .unwrap_or_else(|| panic!("{stage}: {actor_label} lost its root-outcome stamp"));
    assert_eq!(
        stamp.observed_at(),
        None,
        "{stage}: local cancellation must not invent a peer observation timestamp"
    );
    drop(stamp);
    assert_eq!(
        joined.owned_terminals(),
        0,
        "{stage}: {actor_label} terminal and stamp must settle exactly once"
    );
}

pub(super) async fn cleanup_registered_fixture(
    stage: &'static str,
    mut fixture: OwnedRegisteredFixture,
) {
    fixture.server_a.request_cancel();
    fixture.server_b.request_cancel();
    let server_a_task = tokio::spawn(async move { fixture.server_a.join().await });
    let server_b_task = tokio::spawn(async move { fixture.server_b.join().await });
    let shutdown = fixture.composition.join_all_for_shutdown().await;
    let server_a_result = server_a_task.await;
    let server_b_result = server_b_task.await;
    let (joined, shutdown_error) = shutdown.into_parts();
    assert!(
        shutdown_error.is_none(),
        "{stage}: client composition cleanup failed: {shutdown_error:?}"
    );
    let joined = joined.unwrap_or_else(|| {
        panic!("{stage}: structural shutdown did not return current A's zero witness")
    });
    assert_eq!(joined.owned_messages(), 0);
    assert_eq!(joined.owned_bytes(), 0);
    assert_eq!(joined.owned_terminals(), 0);
    assert!(!joined.terminal_failure());
    let server_a_joined = match server_a_result {
        Ok(Ok(joined)) => joined,
        Ok(Err(error)) => panic!("{stage}: server A actor join failed: {error:?}"),
        Err(error) => panic!("{stage}: server A join task failed: {error:?}"),
    };
    settle_raw_local_cancelled_actor_join(stage, "server A", server_a_joined);
    let server_b_joined = match server_b_result {
        Ok(Ok(joined)) => joined,
        Ok(Err(error)) => panic!("{stage}: server B actor join failed: {error:?}"),
        Err(error) => panic!("{stage}: server B join task failed: {error:?}"),
    };
    settle_raw_local_cancelled_actor_join(stage, "server B", server_b_joined);
    drop((
        fixture.composition,
        fixture.tcp_factory,
        fixture.local_flows,
        fixture.peer_policy,
        fixture.owner_executor,
        fixture.owner_active_a,
        fixture.standby_slot,
    ));
    fixture.listener.shutdown().await.unwrap_or_else(|error| {
        panic!(
            "{stage}: physical fixture listener shutdown failed after clean peer joins: {error:?}"
        )
    });
}

pub(super) async fn cleanup_composed_registered_fixture(
    stage: &'static str,
    expected_generation: LegGeneration,
    fixture: OwnedComposedRegisteredFixture,
) {
    let OwnedComposedRegisteredFixture {
        ready_index: _ready_index,
        mut listener,
        mut composition,
        mut server_a,
        mut owner_b,
        tcp_factory,
        local_flows,
        peer_policy,
        owner_active_a,
    } = fixture;
    server_a.request_cancel();
    let (client_shutdown, server_a_result) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(composition.join_all_for_shutdown(), server_a.join())
    })
    .await
    .unwrap_or_else(|_| panic!("{stage}: client/A shutdown exceeded the local bound"));
    let (client_joined, client_error) = client_shutdown.into_parts();
    assert!(
        client_error.is_none(),
        "{stage}: client composition cleanup failed: {client_error:?}",
    );
    let client_joined = client_joined
        .unwrap_or_else(|| panic!("{stage}: client composition did not return its zero witness"));
    assert_eq!(client_joined.owned_messages(), 0);
    assert_eq!(client_joined.owned_bytes(), 0);
    assert_eq!(client_joined.owned_terminals(), 0);
    assert!(!client_joined.terminal_failure());
    let server_a_joined = server_a_result
        .unwrap_or_else(|error| panic!("{stage}: server A actor join failed: {error:?}"));
    settle_raw_local_cancelled_actor_join(stage, "server A", server_a_joined);

    let (terminal_generation, terminal_reason) = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match owner_b.try_drive_reported_terminal() {
                Ok(QuinnOwnerRecoveryTerminalDrive::Pending) => tokio::task::yield_now().await,
                Ok(QuinnOwnerRecoveryTerminalDrive::Applied { generation, reason }) => {
                    break (generation, reason);
                }
                outcome => {
                    panic!("{stage}: physical owner did not apply exact B terminal: {outcome:?}")
                }
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{stage}: physical owner terminal drive exceeded the bound"));
    assert_eq!(terminal_generation, expected_generation);
    assert!(matches!(
        terminal_reason,
        LegTransportTerminalReason::PeerClosed | LegTransportTerminalReason::Reset
    ));
    assert_eq!(owner_b.active_generation(), None);
    let owner_b_joined = tokio::time::timeout(
        TEST_TIMEOUT,
        owner_b.join_physical_actor_after_terminal_for_test(),
    )
    .await
    .unwrap_or_else(|_| panic!("{stage}: physical owner B join exceeded the local bound"))
    .unwrap_or_else(|error| panic!("{stage}: physical owner B join failed: {error:?}"));
    assert_eq!(owner_b_joined.owned_messages(), 0);
    assert_eq!(owner_b_joined.owned_bytes(), 0);
    assert_eq!(owner_b_joined.owned_terminals(), 0);
    drop((
        composition,
        owner_b,
        tcp_factory,
        local_flows,
        peer_policy,
        owner_active_a,
        client_joined,
        owner_b_joined,
    ));
    listener.shutdown().await.unwrap_or_else(|error| {
        panic!("{stage}: physical fixture listener shutdown failed: {error:?}")
    });
}
