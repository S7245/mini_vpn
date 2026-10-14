//! Initial physical-root adoption, terminal settlement and whole-parts tests.
//!
//! Owns the four original composition lifecycle tests and their sole terminal
//! binding helper. Other initial-acceptance fixtures remain private in the parent;
//! production ownership and terminal semantics are unchanged. Tests are entries.

use super::*;

async fn bind_real_initial_terminal(
    actor: &mut StartedQuinnLegActor,
    controller: &mut OwnerSessionController<MemoryTarget>,
    active: &mut Option<OwnerTargetInitialHandoff>,
) -> Result<OwnerSessionRecoveryTerminalBound, OwnerSessionRecoveryTerminalBindErrorKind> {
    let gate = QuinnLegCompositionGate::mint_for_test();
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let observed = actor
                .try_transact_reported_terminal_with_parts(&gate, |runtime, io, queue, reported| {
                    let (terminal, terminal_stamp) = reported.into_parts();
                    match controller
                        .bind_initial_terminal(active, terminal, runtime, io, queue, &gate)
                    {
                        Ok(bound) => Ok(bound),
                        Err(failure) => {
                            let kind = failure.kind();
                            Err((
                                QuinnLegActorReportedTerminal::restore(
                                    failure.into_terminal(),
                                    terminal_stamp,
                                ),
                                kind,
                            ))
                        }
                    }
                })
                .expect("initial terminal transaction remains physically available");
            match observed {
                QuinnLegActorReportedTerminalPoll::Pending => tokio::task::yield_now().await,
                QuinnLegActorReportedTerminalPoll::Applied(bound) => break Ok(bound),
                QuinnLegActorReportedTerminalPoll::Rejected(kind) => break Err(kind),
                QuinnLegActorReportedTerminalPoll::AlreadyConsumed => {
                    panic!("initial actor terminal was consumed before controller binding")
                }
            }
        }
    })
    .await
    .expect("initial actor publishes its exact terminal within the local bound")
}

#[tokio::test(flavor = "current_thread")]
async fn initial_physical_root_adopts_the_prebuilt_controller_before_bounded_service() {
    let limits = composition_actor_limits();
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let peer_policy = AttachPeerPolicy::new(
        OwnerIdentity::new([0x95; 32]).expect("nonzero owner identity"),
        DevicePrincipal::new([0x96; 16]).expect("nonzero device principal"),
    );
    let mut listener = QuinnLegActorListener::bind(
        QuinnLegActorServerConfig::new(
            "127.0.0.1:0".parse().expect("loopback listener"),
            cert_dir.join("server-cert.pem"),
            cert_dir.join("server-key.pem"),
            1,
        )
        .expect("one exact initial owner actor fits"),
    )
    .await
    .expect("bind controller-adoption listener");
    let (mut client, mut owner) =
        connect_real_actor_pair(&mut listener, LegId::A, limits, peer_policy).await;
    let capacity_plans = PhysicalActorSessionPlans::derive();
    let (_accepted, bootstrap) =
        complete_initial_actor_attach_with_capacity_plans(&mut client, &mut owner, capacity_plans)
            .await;
    wait_actor_ordered_delivery(&mut owner).await;

    let (executor, handoff) = initial_owner_executor(bootstrap);
    let gate = QuinnLegCompositionGate::mint_for_test();
    let owner_clock = owner.session_clock(&gate);
    // Declared input of complete_initial_actor_attach_with_capacity_plans; not an identity capability.
    let mut controller_ready =
        ReadyIndex::cold(&ReadyGeometry::checked(std::num::NonZeroUsize::new(1).unwrap()).unwrap())
            .unwrap();
    let ready_limits = OwnerReadySessionLimits::checked(
        capacity_plans.owner.session_config().max_flows(),
        1,
        8,
        8,
    )
    .unwrap();
    let ready = controller_ready
        .reserve_session(SessionId::new([0x93; 16]).unwrap(), ready_limits)
        .unwrap();
    let controller = OwnerSessionController::new_checked(
        executor,
        owner_clock,
        capacity_plans.owner,
        ready,
        std::time::Duration::from_secs(1),
    )
    .expect("the sole initial controller accepts its actor clock and capacity plan");
    let mut root =
        QuinnOwnerInitialSessionComposition::adopt_controller(owner, controller, handoff)
            .expect("actor, controller, and initial handoff enter one physical owner");

    assert_eq!(
        root.drive_initial_acceptance()
            .expect("the exact delivered acceptance settles in one bounded turn"),
        QuinnOwnerInitialAcceptanceDrive::Settled {
            disposition: OwnerTargetInitialAcceptanceSettlementDisposition::SettledNow,
            inventory: root
                .handoff_inventory()
                .expect("the settled handoff remains root-owned"),
        },
    );
    let snapshot = root.controller_snapshot();
    assert_eq!(snapshot.progress_entries, 0);
    assert_eq!(snapshot.retained_outputs, 0);
    assert_eq!(snapshot.pending_continuations, 0);
    let capacity = root.output_page_capacity();
    assert_eq!(capacity.max_outputs(), 64);
    assert_eq!(
        capacity.max_encoded_bytes(),
        64 * MAX_ENCODED_DATA_FRAME_BYTES,
    );

    let (root_joined, client_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(root.join_for_shutdown(), client.join())
    })
    .await
    .expect("controller-owning initial root and peer join within the local bound");
    let root_joined = root_joined.expect("initial root joins structurally");
    assert_eq!(root_joined.retained_controllers(), 1);
    assert_eq!(root_joined.retained_executors(), 1);
    assert_eq!(root_joined.retained_attached_authorities(), 1);
    assert_eq!(root_joined.retained_acceptance_receipts(), 0);
    let client_joined = client_joined.expect("initial peer joins structurally");
    assert_eq!(client_joined.owned_messages(), 0);
    assert_eq!(client_joined.owned_bytes(), 0);
    drop(root);
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("controller-adoption listener cleanup is bounded")
        .expect("the exact actor slot is released");
}

#[tokio::test(flavor = "current_thread")]
async fn accepted_initial_root_parts_transfer_the_current_actor_once() {
    let limits = composition_actor_limits();
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let peer_policy = AttachPeerPolicy::new(
        OwnerIdentity::new([0x95; 32]).expect("nonzero owner identity"),
        DevicePrincipal::new([0x96; 16]).expect("nonzero device principal"),
    );
    let mut listener = QuinnLegActorListener::bind(
        QuinnLegActorServerConfig::new(
            "127.0.0.1:0".parse().expect("loopback listener"),
            cert_dir.join("server-cert.pem"),
            cert_dir.join("server-key.pem"),
            1,
        )
        .expect("one exact initial owner actor fits"),
    )
    .await
    .expect("bind accepted-root-parts listener");
    let (mut client, mut owner) =
        connect_real_actor_pair(&mut listener, LegId::A, limits, peer_policy).await;
    let held_delivery = Arc::clone(&client.delivery_writer_gate);
    held_delivery.arm();
    let capacity_plans = PhysicalActorSessionPlans::derive();
    let (_accepted, bootstrap) =
        complete_initial_actor_attach_with_capacity_plans(&mut client, &mut owner, capacity_plans)
            .await;
    tokio::time::timeout(TEST_TIMEOUT, held_delivery.wait_until_blocked())
        .await
        .expect("the exact acceptance stays physically undelivered");

    let (executor, handoff) = initial_owner_executor(bootstrap);
    let gate = QuinnLegCompositionGate::mint_for_test();
    let owner_clock = owner.session_clock(&gate);
    // Declared input of complete_initial_actor_attach_with_capacity_plans; not an identity capability.
    let mut controller_ready =
        ReadyIndex::cold(&ReadyGeometry::checked(std::num::NonZeroUsize::new(1).unwrap()).unwrap())
            .unwrap();
    let ready_limits = OwnerReadySessionLimits::checked(
        capacity_plans.owner.session_config().max_flows(),
        1,
        8,
        8,
    )
    .unwrap();
    let ready = controller_ready
        .reserve_session(SessionId::new([0x93; 16]).unwrap(), ready_limits)
        .unwrap();
    let controller = OwnerSessionController::new_checked(
        executor,
        owner_clock,
        capacity_plans.owner,
        ready,
        std::time::Duration::from_secs(1),
    )
    .expect("the sole initial controller accepts its actor clock and capacity plan");
    let root = QuinnOwnerInitialSessionComposition::adopt_controller(owner, controller, handoff)
        .expect("actor, controller, and handoff enter one physical root");

    let pending = root
        .try_into_accepted_root_parts()
        .expect_err("admission cannot transfer an acceptance-pending current actor");
    let pending_inventory = pending
        .inventory()
        .expect("acceptance rejection carries its exact inventory");
    assert_eq!(
        pending_inventory.delivery(),
        OwnerTargetInitialAcceptanceDelivery::AdmittedDeliveryPending,
    );
    assert_eq!(pending_inventory.acceptance_receipts(), 1);
    let mut root = pending.into_root();

    held_delivery.release();
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match root
                .handoff_inventory()
                .expect("the rejected transfer retains the exact root")
                .delivery()
            {
                OwnerTargetInitialAcceptanceDelivery::DeliveredAwaitingSettlement => break,
                OwnerTargetInitialAcceptanceDelivery::AdmittedDeliveryPending => {
                    tokio::task::yield_now().await;
                }
                OwnerTargetInitialAcceptanceDelivery::Settled => {
                    panic!("the rejected transfer cannot settle the acceptance")
                }
            }
        }
    })
    .await
    .expect("the already-admitted acceptance reaches exact delivery");
    assert!(matches!(
        root.drive_initial_acceptance()
            .expect("exact acceptance settlement is bounded"),
        QuinnOwnerInitialAcceptanceDrive::Settled {
            disposition: OwnerTargetInitialAcceptanceSettlementDisposition::SettledNow,
            ..
        }
    ));

    let mut parts = root
        .try_into_accepted_root_parts()
        .expect("only the settled current actor crosses the root-parts seam");
    assert_eq!(parts.retained_current_actors(), 1);
    assert_eq!(parts.retained_controllers(), 1);
    assert_eq!(parts.retained_executors(), 1);
    assert_eq!(parts.retained_attached_authorities(), 1);
    assert_eq!(parts.retained_acceptance_receipts(), 0);
    assert_eq!(parts.retained_physical_actor_identities(), 1);
    assert!(parts.controller_shares_current_actor_clock());
    assert_eq!(
        parts
            .current_actor_inventory()
            .expect("accepted current actor remains physically auditable")
            .delivery(),
        OwnerTargetInitialAcceptanceDelivery::Settled,
    );

    let mut root = parts.into_initial_composition();
    let (root_joined, client_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(root.join_for_shutdown(), client.join())
    })
    .await
    .expect("transferred initial root parts and peer join within the local bound");
    let root_joined = root_joined.expect("reassembled current actor joins structurally");
    assert_eq!(root_joined.actor_owned_messages(), 0);
    assert_eq!(root_joined.actor_owned_bytes(), 0);
    assert_eq!(root_joined.retained_controllers(), 1);
    assert_eq!(root_joined.retained_acceptance_receipts(), 0);
    let client_joined = client_joined.expect("initial peer joins structurally");
    assert_eq!(client_joined.owned_messages(), 0);
    assert_eq!(client_joined.owned_bytes(), 0);
    drop(root);
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("accepted-root-parts listener cleanup is bounded")
        .expect("the exact actor slot is released");
}

#[tokio::test(flavor = "current_thread")]
async fn initial_terminal_requires_settlement_and_installs_one_legless_high_water() {
    let limits = composition_actor_limits();
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let peer_policy = AttachPeerPolicy::new(
        OwnerIdentity::new([0x95; 32]).expect("nonzero owner identity"),
        DevicePrincipal::new([0x96; 16]).expect("nonzero device principal"),
    );
    let mut listener = QuinnLegActorListener::bind(
        QuinnLegActorServerConfig::new(
            "127.0.0.1:0".parse().expect("loopback listener"),
            cert_dir.join("server-cert.pem"),
            cert_dir.join("server-key.pem"),
            2,
        )
        .expect("two exact initial terminal actors fit"),
    )
    .await
    .expect("bind initial-terminal listener");
    let (mut client_unsettled, mut owner_unsettled) =
        connect_real_actor_pair(&mut listener, LegId::A, limits, peer_policy).await;
    let (mut client_settled, mut owner_settled) =
        connect_real_actor_pair(&mut listener, LegId::A, limits, peer_policy).await;
    let capacity_plans = PhysicalActorSessionPlans::derive();
    let (_accepted_unsettled, bootstrap_unsettled) =
        complete_initial_actor_attach_with_capacity_plans(
            &mut client_unsettled,
            &mut owner_unsettled,
            capacity_plans,
        )
        .await;
    let (_accepted_settled, bootstrap_settled) = complete_initial_actor_attach_with_capacity_plans(
        &mut client_settled,
        &mut owner_settled,
        capacity_plans,
    )
    .await;
    wait_actor_ordered_delivery(&mut owner_unsettled).await;
    wait_actor_ordered_delivery(&mut owner_settled).await;

    let gate = QuinnLegCompositionGate::mint_for_test();
    let (executor_unsettled, handoff_unsettled) = initial_owner_executor(bootstrap_unsettled);
    // Declared input of complete_initial_actor_attach_with_capacity_plans; not an identity capability.
    let mut controller_unsettled_ready =
        ReadyIndex::cold(&ReadyGeometry::checked(std::num::NonZeroUsize::new(1).unwrap()).unwrap())
            .unwrap();
    let ready_limits = OwnerReadySessionLimits::checked(
        capacity_plans.owner.session_config().max_flows(),
        1,
        8,
        8,
    )
    .unwrap();
    let ready = controller_unsettled_ready
        .reserve_session(SessionId::new([0x93; 16]).unwrap(), ready_limits)
        .unwrap();
    let mut controller_unsettled = OwnerSessionController::new_checked(
        executor_unsettled,
        owner_unsettled.session_clock(&gate),
        capacity_plans.owner,
        ready,
        std::time::Duration::from_secs(1),
    )
    .expect("unsettled controller accepts the exact actor clock and capacity plan");
    let handoff_unsettled = controller_unsettled
        .bind_initial_handoff_to_physical_actor(
            handoff_unsettled,
            owner_unsettled
                .physical_incarnation(&gate)
                .expect("unsettled actor owns its runtime incarnation"),
            owner_unsettled
                .io
                .as_ref()
                .expect("unsettled actor owns its policy leg"),
            owner_unsettled
                .queue
                .as_ref()
                .expect("unsettled actor owns its sole FIFO"),
            &gate,
        )
        .expect("unsettled handoff binds to its exact physical actor");
    let mut active_unsettled = Some(handoff_unsettled);

    let (executor_settled, handoff_settled) = initial_owner_executor(bootstrap_settled);
    // Declared input of complete_initial_actor_attach_with_capacity_plans; not an identity capability.
    let mut controller_settled_ready =
        ReadyIndex::cold(&ReadyGeometry::checked(std::num::NonZeroUsize::new(1).unwrap()).unwrap())
            .unwrap();
    let ready_limits = OwnerReadySessionLimits::checked(
        capacity_plans.owner.session_config().max_flows(),
        1,
        8,
        8,
    )
    .unwrap();
    let ready = controller_settled_ready
        .reserve_session(SessionId::new([0x93; 16]).unwrap(), ready_limits)
        .unwrap();
    let mut controller_settled = OwnerSessionController::new_checked(
        executor_settled,
        owner_settled.session_clock(&gate),
        capacity_plans.owner,
        ready,
        std::time::Duration::from_secs(1),
    )
    .expect("settled controller accepts the exact actor clock and capacity plan");
    let handoff_settled = controller_settled
        .bind_initial_handoff_to_physical_actor(
            handoff_settled,
            owner_settled
                .physical_incarnation(&gate)
                .expect("settled actor owns its runtime incarnation"),
            owner_settled
                .io
                .as_ref()
                .expect("settled actor owns its policy leg"),
            owner_settled
                .queue
                .as_ref()
                .expect("settled actor owns its sole FIFO"),
            &gate,
        )
        .expect("settled handoff binds to its exact physical actor");
    let settled = controller_settled
        .settle_initial_acceptance_delivery(
            handoff_settled,
            owner_settled
                .physical_incarnation(&gate)
                .expect("settled actor retains its runtime incarnation"),
            owner_settled
                .io
                .as_ref()
                .expect("settled actor retains its policy leg"),
            owner_settled
                .queue
                .as_ref()
                .expect("settled actor retains its sole FIFO"),
            &gate,
        )
        .expect("delivered initial acceptance settles before terminal binding");
    assert_eq!(
        settled.disposition(),
        OwnerTargetInitialAcceptanceSettlementDisposition::SettledNow,
    );
    let mut active_settled = Some(settled.into_handoff());

    client_unsettled.request_cancel();
    client_settled.request_cancel();
    let (client_unsettled_joined, client_settled_joined) =
        tokio::time::timeout(TEST_TIMEOUT, async {
            tokio::join!(client_unsettled.join(), client_settled.join())
        })
        .await
        .expect("both initial peers join within the local bound");
    for joined in [client_unsettled_joined, client_settled_joined] {
        let joined = joined.expect("initial peer joins structurally");
        assert_eq!(joined.owned_messages(), 0);
        assert_eq!(joined.owned_bytes(), 0);
    }

    for attempt in 0..2 {
        assert_eq!(
            bind_real_initial_terminal(
                &mut owner_unsettled,
                &mut controller_unsettled,
                &mut active_unsettled,
            )
            .await
            .expect_err("physical delivery cannot substitute acceptance settlement"),
            OwnerSessionRecoveryTerminalBindErrorKind::Rejected(
                OwnerTargetInputBlock::InitialAcceptanceSettlementPending,
            ),
            "unsettled terminal rejection attempt {attempt} changed discriminator",
        );
        let handoff = active_unsettled
            .as_ref()
            .expect("rejection restores the exact initial handoff");
        assert_eq!(handoff.retained_acceptance_receipts(), 1);
        assert_eq!(handoff.physical_actor_identities(), 1);
        let snapshot = controller_unsettled.snapshot();
        assert_eq!(snapshot.pending_recoveries, 0);
        assert_eq!(snapshot.pending_legless_high_waters, 0);
    }

    let bound = bind_real_initial_terminal(
        &mut owner_settled,
        &mut controller_settled,
        &mut active_settled,
    )
    .await
    .expect("settled initial handoff binds the exact actor terminal once");
    assert!(active_settled.is_none());
    let terminal_bound = controller_settled.snapshot();
    assert_eq!(terminal_bound.pending_recoveries, 1);
    assert!(terminal_bound.recovery_needs_terminal_discard);
    assert_eq!(terminal_bound.pending_legless_high_waters, 0);
    let discarded = controller_settled
        .discard_recovery_terminal(&gate)
        .expect("terminal-bound initial handoff discards into one high-water");
    assert_eq!(discarded.generation(), bound.generation());
    assert_eq!(discarded.reason(), bound.reason());
    let legless = controller_settled.snapshot();
    assert_eq!(legless.pending_recoveries, 0);
    assert!(!legless.recovery_needs_terminal_discard);
    assert_eq!(legless.pending_legless_high_waters, 1);

    let (owner_unsettled_joined, owner_settled_joined) =
        tokio::time::timeout(TEST_TIMEOUT, async {
            tokio::join!(owner_unsettled.join(), owner_settled.join())
        })
        .await
        .expect("both terminal owner actors join within the local bound");
    let owner_unsettled_joined =
        owner_unsettled_joined.expect("unsettled actor joins with restored terminal");
    assert_eq!(owner_unsettled_joined.owned_messages(), 0);
    assert_eq!(owner_unsettled_joined.owned_bytes(), 0);
    assert_eq!(owner_unsettled_joined.owned_terminals(), 1);
    let owner_settled_joined =
        owner_settled_joined.expect("settled actor joins after terminal consumption");
    assert_eq!(owner_settled_joined.owned_messages(), 0);
    assert_eq!(owner_settled_joined.owned_bytes(), 0);
    assert_eq!(owner_settled_joined.owned_terminals(), 0);
    drop((active_unsettled, controller_unsettled, controller_settled));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("initial-terminal listener cleanup is bounded")
        .expect("both exact actor slots are released");
}

#[tokio::test(flavor = "current_thread")]
async fn accepted_current_actor_terminalizes_and_joins_before_legless_transfer() {
    let limits = composition_actor_limits();
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let peer_policy = AttachPeerPolicy::new(
        OwnerIdentity::new([0x95; 32]).expect("nonzero owner identity"),
        DevicePrincipal::new([0x96; 16]).expect("nonzero device principal"),
    );
    let mut listener = QuinnLegActorListener::bind(
        QuinnLegActorServerConfig::new(
            "127.0.0.1:0".parse().expect("loopback listener"),
            cert_dir.join("server-cert.pem"),
            cert_dir.join("server-key.pem"),
            1,
        )
        .expect("one exact current owner actor fits"),
    )
    .await
    .expect("bind current-terminal listener");
    let (mut client, mut owner) =
        connect_real_actor_pair(&mut listener, LegId::A, limits, peer_policy).await;
    let capacity_plans = PhysicalActorSessionPlans::derive();
    let (_accepted, bootstrap) =
        complete_initial_actor_attach_with_capacity_plans(&mut client, &mut owner, capacity_plans)
            .await;
    wait_actor_ordered_delivery(&mut owner).await;

    let (executor, handoff) = initial_owner_executor(bootstrap);
    let gate = QuinnLegCompositionGate::mint_for_test();
    // Declared input of complete_initial_actor_attach_with_capacity_plans; not an identity capability.
    let mut controller_ready =
        ReadyIndex::cold(&ReadyGeometry::checked(std::num::NonZeroUsize::new(1).unwrap()).unwrap())
            .unwrap();
    let ready_limits = OwnerReadySessionLimits::checked(
        capacity_plans.owner.session_config().max_flows(),
        1,
        8,
        8,
    )
    .unwrap();
    let ready = controller_ready
        .reserve_session(SessionId::new([0x93; 16]).unwrap(), ready_limits)
        .unwrap();
    let controller = OwnerSessionController::new_checked(
        executor,
        owner.session_clock(&gate),
        capacity_plans.owner,
        ready,
        std::time::Duration::from_secs(1),
    )
    .expect("the sole controller accepts the current actor clock and capacity plan");
    let mut initial =
        QuinnOwnerInitialSessionComposition::adopt_controller(owner, controller, handoff)
            .expect("current actor, controller, and handoff enter one owner domain");
    assert!(matches!(
        initial
            .drive_initial_acceptance()
            .expect("the exact delivered acceptance settles"),
        QuinnOwnerInitialAcceptanceDrive::Settled {
            disposition: OwnerTargetInitialAcceptanceSettlementDisposition::SettledNow,
            ..
        }
    ));
    let accepted = initial
        .try_into_accepted_root_parts()
        .expect("settled initial A transfers exactly once");
    let mut current = accepted.into_terminal_tracked_current();
    assert_eq!(
        current.terminal_transaction_unwind_matrix_for_test(),
        (true, true, true),
        "prepared Drop restores; callback-entered unwind or forget remains quarantined",
    );

    client.request_cancel();
    let client_joined = tokio::time::timeout(TEST_TIMEOUT, client.join())
        .await
        .expect("current peer join is bounded")
        .expect("current peer joins structurally");
    assert_eq!(client_joined.owned_messages(), 0);
    assert_eq!(client_joined.owned_bytes(), 0);

    let applied = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let drive = current
                .try_drive_reported_terminal()
                .expect("the exact hard terminal remains root-owned");
            if drive.is_pending() {
                tokio::task::yield_now().await;
                continue;
            }
            break drive
                .newly_applied_identity()
                .expect("the first non-pending current-terminal turn applies exactly once");
        }
    })
    .await
    .expect("current terminal reaches controller high-water within the local bound");
    assert_eq!(current.retained_active_handoffs(), 0);
    assert_eq!(current.retained_terminal_stamps(), 2);
    let snapshot = current.controller_snapshot();
    assert_eq!(snapshot.pending_recoveries, 0);
    assert_eq!(snapshot.pending_legless_high_waters, 1);
    let repeated = current
        .try_drive_reported_terminal()
        .expect("an applied terminal retry is value-only");
    assert_eq!(repeated.already_applied_identity(), Some(applied));

    let joined = current
        .join_current_after_terminal()
        .await
        .expect("terminal-consumed current actor joins structurally");
    assert_eq!(joined.actor_owned_messages(), 0);
    assert_eq!(joined.actor_owned_bytes(), 0);
    assert_eq!(joined.actor_owned_terminals(), 0);
    assert_eq!(joined.retained_controllers(), 1);
    assert_eq!(joined.retained_legless_high_waters(), 1);
    assert_eq!(joined.retained_terminal_stamps(), 1);
    assert!(joined.terminal_stamp_pair_verified());
    let actor_terminal_failure = joined.actor_terminal_failure();
    let legless = current
        .try_into_legless_root_parts()
        .expect("only a joined terminal-applied current actor can leave the current slot");
    assert_eq!(legless.retained_current_actors(), 0);
    assert_eq!(legless.retained_joined_actors(), 1);
    assert_eq!(legless.retained_controllers(), 1);
    assert_eq!(legless.retained_executors(), 1);
    assert_eq!(legless.retained_active_handoffs(), 0);
    assert_eq!(legless.retained_legless_high_waters(), 1);
    assert_eq!(legless.retained_terminal_stamps(), 1);
    assert!(legless.terminal_stamp_pair_verified());
    assert_eq!(legless.joined_actor_owned_messages(), 0);
    assert_eq!(legless.joined_actor_owned_bytes(), 0);
    assert_eq!(legless.joined_actor_owned_terminals(), 0);
    assert_eq!(
        legless.joined_actor_terminal_failure(),
        actor_terminal_failure,
    );

    drop(legless);
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("current-terminal listener cleanup is bounded")
        .expect("joined current actor releases its exact listener slot");
}
