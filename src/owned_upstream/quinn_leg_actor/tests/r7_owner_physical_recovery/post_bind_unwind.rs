//! Owns the real-actor post-bind unwind tracer for the legacy B-only adapter.
//! It does not claim whole physical-root zero; the parent fixture retains A,
//! controller, output, and listener cleanup authority.

use super::*;

#[tokio::test(flavor = "current_thread")]
async fn legacy_b_only_post_bind_unwind_retains_root_stamp_and_refuses_reentry() {
    let LegacyActiveBTerminalFixture {
        ready_index: _ready_index,
        mut listener,
        mut composition,
        mut server_a,
        mut owner,
        tcp_factory,
        local_flows,
        owner_active_a,
        installed_generation,
    } = legacy_active_b_terminal_fixture().await;
    assert_eq!(owner.active_generation(), Some(installed_generation));
    assert!(owner.output_root_inventory().is_output_exact_zero());
    owner.arm_terminal_bind_postcommit_panic_for_test();

    shutdown_client_and_settle_external_a(&mut composition, &mut server_a).await;

    let panic_payload = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                owner.try_drive_reported_terminal()
            })) {
                Ok(Ok(QuinnOwnerRecoveryTerminalDrive::Pending)) => {
                    tokio::task::yield_now().await;
                }
                Err(payload) => break payload,
                Ok(outcome) => panic!("post-bind failpoint was not reached: {outcome:?}"),
            }
        }
    })
    .await
    .expect("post-bind unwind occurs within the local bound");
    let panic_text = panic_payload
        .downcast_ref::<&'static str>()
        .copied()
        .or_else(|| panic_payload.downcast_ref::<String>().map(String::as_str));
    assert_eq!(
        panic_text,
        Some("test owner recovery terminal bind postcommit unwind"),
        "the tracer must catch only the intended controller postcommit failpoint",
    );

    assert_eq!(owner.active_generation(), None);
    let quarantined = owner.output_root_inventory();
    assert!(quarantined.is_output_exact_zero());
    assert!(!quarantined.is_controller_output_terminal_exact_zero());
    assert_eq!(quarantined.terminal_transactions(), 1);
    assert_eq!(quarantined.terminal_residuals(), 1);
    assert_eq!(quarantined.terminal_stamps(), 1);
    assert!(quarantined.terminal_quarantined());
    assert_eq!(quarantined.controller_failures(), 0);
    assert!(!quarantined.controller_terminal_correlation_valid());
    assert!(!quarantined.standby_terminal_correlation_valid());
    let snapshot = quarantined
        .controller_snapshot()
        .expect("the quarantined root retains its exact controller snapshot");
    assert!(!snapshot.failed);
    assert_eq!(snapshot.pending_recoveries, 1);
    assert!(snapshot.recovery_needs_terminal_discard);
    assert_eq!(snapshot.pending_legless_high_waters, 0);
    assert_eq!(snapshot.recovery_terminal_quarantined, 0);

    assert!(matches!(
        owner.try_drive_reported_terminal(),
        Err(QuinnOwnerRecoveryTerminalError::CompositionInvariant)
    ));
    assert_eq!(owner.output_root_inventory(), quarantined);
    match owner.stop_output_for_shutdown() {
        QuinnOwnerOutputStopDrive::AlreadyStopped { reason, inventory } => {
            assert_eq!(reason, OwnerOutputDrainStopReason::Fatal);
            assert!(inventory.is_output_exact_zero());
            assert!(!inventory.is_controller_output_terminal_exact_zero());
        }
        outcome => panic!("post-bind unwind did not retain fatal stop state: {outcome:?}"),
    }

    let owner_b_joined = tokio::time::timeout(
        TEST_TIMEOUT,
        owner.join_physical_actor_after_terminal_for_test(),
    )
    .await
    .expect("quarantined physical owner B join is bounded")
    .expect("quarantined physical owner B still joins structurally");
    assert_eq!(owner_b_joined.owned_messages(), 0);
    assert_eq!(owner_b_joined.owned_bytes(), 0);
    assert_eq!(owner_b_joined.owned_terminals(), 0);
    drop((owner, owner_b_joined));
    drop((
        composition,
        server_a,
        tcp_factory,
        local_flows,
        owner_active_a,
    ));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("post-bind unwind listener cleanup is bounded")
        .expect("post-bind unwind releases both server actor slots");
}
