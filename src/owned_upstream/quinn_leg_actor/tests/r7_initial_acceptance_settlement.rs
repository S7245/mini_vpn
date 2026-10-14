use super::*;
use crate::owned_upstream::owner_ready::{OwnerReadySessionLimits, ReadyGeometry, ReadyIndex};
use crate::owned_upstream::owner_session_controller::{
    OwnerSessionController, OwnerSessionRecoveryTerminalBindErrorKind,
    OwnerSessionRecoveryTerminalBound,
};
use crate::owned_upstream::owner_target::{
    OwnerTargetInitialAcceptanceDelivery, OwnerTargetInitialAcceptanceSettlement,
    OwnerTargetInitialAcceptanceSettlementDisposition, OwnerTargetInitialHandoff,
    OwnerTargetInitialHandoffOwnerFailure, OwnerTargetInputBlock,
};
use crate::owned_upstream::quinn_leg_actor::{
    QuinnLegActorReportedTerminal, QuinnLegActorReportedTerminalPoll,
};
use crate::owned_upstream::quinn_leg_composition::QuinnLegCompositionGate;
use crate::owned_upstream::quinn_leg_composition::{
    QuinnOwnerInitialAcceptanceDrive, QuinnOwnerInitialSessionComposition,
    QuinnOwnerInitialShutdownError,
};

mod initial_composition;

fn initial_owner_executor(
    bootstrap: OwnerSessionBootstrap,
) -> (OwnerTargetExecutor<MemoryTarget>, OwnerTargetInitialHandoff) {
    OwnerTargetExecutor::from_initial_bootstrap(
        OwnerTargetConfig::new(64, 1_024, 64).expect("valid owner Target limits"),
        bootstrap,
        MemoryTarget::new(
            MemoryTargetConfig::new(4, 256, 512, 128, 128, 16).expect("bounded memory Target"),
        ),
        FlowPortConfig::new(64, 8, 8, 8).expect("bounded owner source"),
    )
    .expect("whole initial bootstrap installs without decomposition")
}

fn bind_initial_to_actor(
    executor: &OwnerTargetExecutor<MemoryTarget>,
    handoff: OwnerTargetInitialHandoff,
    actor: &StartedQuinnLegActor,
    gate: &QuinnLegCompositionGate,
) -> Result<OwnerTargetInitialHandoff, OwnerTargetInitialHandoffOwnerFailure> {
    executor.bind_initial_handoff_to_physical_actor(
        handoff,
        actor
            .physical_incarnation(gate)
            .expect("running actor owns its runtime incarnation"),
        actor.io.as_ref().expect("running actor owns its LegIo"),
        actor
            .queue
            .as_ref()
            .expect("running actor owns its sole FIFO"),
    )
}

fn settle_initial_on_actor(
    executor: &OwnerTargetExecutor<MemoryTarget>,
    handoff: OwnerTargetInitialHandoff,
    actor: &StartedQuinnLegActor,
    gate: &QuinnLegCompositionGate,
) -> Result<OwnerTargetInitialAcceptanceSettlement, OwnerTargetInitialHandoffOwnerFailure> {
    executor.settle_initial_acceptance_delivery(
        handoff,
        actor
            .physical_incarnation(gate)
            .expect("running actor owns its runtime incarnation"),
        actor.io.as_ref().expect("running actor owns its LegIo"),
        actor
            .queue
            .as_ref()
            .expect("running actor owns its sole FIFO"),
    )
}

#[tokio::test(flavor = "current_thread")]
async fn same_scalar_wrong_physical_actor_and_queue_return_exact_initial_handoff() {
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
        .expect("two exact physical A actors fit"),
    )
    .await
    .expect("bind exact-actor listener");
    let (mut client_a, mut owner_a) =
        connect_real_actor_pair(&mut listener, LegId::A, limits, peer_policy).await;
    let (mut client_same_scalar, mut owner_same_scalar) =
        connect_real_actor_pair(&mut listener, LegId::A, limits, peer_policy).await;
    let capacity_plans = PhysicalActorSessionPlans::derive();
    let (_accepted_a, bootstrap_a) = complete_initial_actor_attach_with_capacity_plans(
        &mut client_a,
        &mut owner_a,
        capacity_plans,
    )
    .await;
    let (_accepted_same_scalar, bootstrap_same_scalar) =
        complete_initial_actor_attach_with_capacity_plans(
            &mut client_same_scalar,
            &mut owner_same_scalar,
            capacity_plans,
        )
        .await;
    let (executor_a, handoff_a) = initial_owner_executor(bootstrap_a);
    let (executor_same_scalar, handoff_same_scalar) = initial_owner_executor(bootstrap_same_scalar);
    assert_eq!(handoff_a.generation(), handoff_same_scalar.generation());
    assert_eq!(handoff_a.nonce(), handoff_same_scalar.nonce());

    let gate = QuinnLegCompositionGate::mint_for_test();
    let wrong_runtime = owner_same_scalar
        .physical_incarnation(&gate)
        .expect("same-scalar actor owns its runtime incarnation");
    let wrong_io = owner_same_scalar
        .io
        .as_ref()
        .expect("same-scalar actor owns its policy incarnation");
    let wrong_queue = owner_same_scalar
        .queue
        .as_ref()
        .expect("same-scalar actor owns its sole FIFO");
    let failure = executor_a
        .bind_initial_handoff_to_physical_actor(handoff_a, wrong_runtime, wrong_io, wrong_queue)
        .expect_err("scalar equality cannot substitute physical actor identity");
    assert_eq!(
        failure.block(),
        OwnerTargetInputBlock::WrongInitialPhysicalActor
    );
    let handoff_a = failure.into_handoff();

    let exact_runtime = owner_a
        .physical_incarnation(&gate)
        .expect("exact A actor owns its runtime incarnation");
    let exact_io = owner_a
        .io
        .as_ref()
        .expect("exact A actor owns its policy incarnation");
    let exact_queue = owner_a
        .queue
        .as_ref()
        .expect("exact A actor owns its sole FIFO");
    let failure = executor_a
        .bind_initial_handoff_to_physical_actor(handoff_a, exact_runtime, exact_io, wrong_queue)
        .expect_err("a foreign FIFO cannot be detached from its physical actor");
    assert_eq!(
        failure.block(),
        OwnerTargetInputBlock::WrongInitialPhysicalActor
    );
    let handoff_a = failure.into_handoff();

    let handoff_a = executor_a
        .bind_initial_handoff_to_physical_actor(handoff_a, exact_runtime, exact_io, exact_queue)
        .expect("exact runtime-policy-queue chain binds once");
    assert_eq!(handoff_a.physical_actor_identities(), 1);

    wait_actor_ordered_delivery(&mut owner_a).await;
    wait_actor_ordered_delivery(&mut owner_same_scalar).await;
    // Declared input of complete_initial_actor_attach_with_capacity_plans; not an identity capability.
    let mut controller_same_scalar_ready =
        ReadyIndex::cold(&ReadyGeometry::checked(std::num::NonZeroUsize::new(1).unwrap()).unwrap())
            .unwrap();
    let ready_limits = OwnerReadySessionLimits::checked(
        capacity_plans.owner.session_config().max_flows(),
        1,
        8,
        8,
    )
    .unwrap();
    let ready = controller_same_scalar_ready
        .reserve_session(SessionId::new([0x93; 16]).unwrap(), ready_limits)
        .unwrap();
    let controller_same_scalar = OwnerSessionController::new_checked(
        executor_same_scalar,
        owner_same_scalar.session_clock(&gate),
        capacity_plans.owner,
        ready,
        std::time::Duration::from_secs(1),
    )
    .expect("same-scalar controller retains its exact installed capacity plan");
    let capacity_witness = controller_same_scalar.capacity_witness();
    let failure = match QuinnOwnerInitialSessionComposition::adopt_controller(
        owner_a,
        controller_same_scalar,
        handoff_same_scalar,
    ) {
        Err(failure) => failure,
        Ok(_) => panic!("a same-scalar foreign actor cannot adopt the checked controller"),
    };
    assert!(matches!(
        failure.kind(),
        crate::owned_upstream::quinn_leg_composition::QuinnOwnerInitialAdoptErrorKind::WrongSessionClock
    ));
    let (mut owner_a, controller_same_scalar, handoff_same_scalar) = failure.into_parts();
    assert_eq!(
        controller_same_scalar.capacity_witness(),
        capacity_witness,
        "wrong-clock rejection returns the exact checked controller unchanged",
    );
    let mut same_scalar_root = QuinnOwnerInitialSessionComposition::adopt_controller(
        owner_same_scalar,
        controller_same_scalar,
        handoff_same_scalar,
    )
    .expect("the returned checked controller and handoff retry with their exact actor");
    assert!(matches!(
        same_scalar_root
            .drive_initial_acceptance()
            .expect("the exact same-scalar acceptance settles after retry"),
        QuinnOwnerInitialAcceptanceDrive::Settled {
            disposition: OwnerTargetInitialAcceptanceSettlementDisposition::SettledNow,
            ..
        }
    ));
    drop((executor_a, handoff_a));
    client_a.request_cancel();
    owner_a.request_cancel();
    client_same_scalar.request_cancel();
    let (_, _, _, same_scalar_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(
            client_a.join(),
            owner_a.join(),
            client_same_scalar.join(),
            same_scalar_root.join_for_shutdown(),
        )
    })
    .await
    .expect("two physical actor pairs join within the local bound");
    same_scalar_joined.expect("the retried checked composition joins structurally");
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("listener cleanup is bounded")
        .expect("both exact actor slots are released");
}

#[tokio::test(flavor = "current_thread")]
async fn only_exact_acceptance_delivery_settles_once_behind_its_physical_actor() {
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
        .expect("the exact A actor and one independent control actor fit"),
    )
    .await
    .expect("bind exact-delivery listener");
    let (mut client, mut owner) =
        connect_real_actor_pair(&mut listener, LegId::A, limits, peer_policy).await;
    let (mut other_client, mut other_owner) =
        connect_real_actor_pair(&mut listener, LegId::B, limits, peer_policy).await;
    let delayed_acceptance_delivery = Arc::clone(&client.delivery_writer_gate);
    delayed_acceptance_delivery.arm();
    let (_accepted, bootstrap) = complete_initial_actor_attach(&mut client, &mut owner).await;
    tokio::time::timeout(
        TEST_TIMEOUT,
        delayed_acceptance_delivery.wait_until_blocked(),
    )
    .await
    .expect("client holds the exact acceptance DELIVERED before its write");
    let (executor, handoff) = initial_owner_executor(bootstrap);
    let gate = QuinnLegCompositionGate::mint_for_test();
    let handoff = bind_initial_to_actor(&executor, handoff, &owner, &gate)
        .expect("initial handoff binds to exact owner A actor");
    let pending = handoff
        .inventory(owner.queue.as_ref().expect("owner FIFO remains live"))
        .expect("exact queue inventories its own acceptance");
    assert_eq!(pending.physical_actor_identities(), 1);
    assert!(pending.physical_actor_identity_live());
    assert_eq!(pending.acceptance_receipts(), 1);
    assert_eq!(
        pending.delivery(),
        OwnerTargetInitialAcceptanceDelivery::AdmittedDeliveryPending
    );
    let pending_failure = settle_initial_on_actor(&executor, handoff, &owner, &gate)
        .expect_err("admission cannot impersonate delivery");
    assert_eq!(
        pending_failure.block(),
        OwnerTargetInputBlock::InitialAcceptanceDeliveryPending
    );
    let handoff = pending_failure.into_handoff();

    let other_receipt = other_client
        .enqueue_ordered_leg_control(ordered_control(41), STANDBY_CONTROL_V1)
        .expect("another ordered record enters its independent actor FIFO");
    assert_eq!(
        other_client
            .try_flush(SimTime::ZERO)
            .expect("other ordered record submission remains live"),
        QuinnLegActorFlushDisposition::Submitted
    );
    let other_inbound = tokio::time::timeout(
        TEST_TIMEOUT,
        other_owner.recv_classified(STANDBY_CONTROL_V1),
    )
    .await
    .expect("other ordered record receive is bounded")
    .expect("independent owner remains live")
    .expect("independent owner receives the other exact ordered record");
    drop(take_actor_inbound(other_inbound));
    tokio::time::timeout(TEST_TIMEOUT, async {
        while !other_receipt.was_delivered() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the other exact ordered record reports delivery");
    assert_eq!(
        handoff
            .inventory(owner.queue.as_ref().expect("owner FIFO remains live"))
            .expect("initial receipt remains independently auditable")
            .delivery(),
        OwnerTargetInitialAcceptanceDelivery::AdmittedDeliveryPending,
        "another record's pointer-distinct delivery cannot settle the acceptance",
    );
    let pending_failure = settle_initial_on_actor(&executor, handoff, &owner, &gate)
        .expect_err("another ordered record cannot settle initial acceptance");
    assert_eq!(
        pending_failure.block(),
        OwnerTargetInputBlock::InitialAcceptanceDeliveryPending
    );
    let handoff = pending_failure.into_handoff();

    delayed_acceptance_delivery.release();
    wait_actor_ordered_delivery(&mut owner).await;
    assert_eq!(
        handoff
            .inventory(owner.queue.as_ref().expect("owner FIFO remains live"))
            .expect("delivered acceptance remains exact")
            .delivery(),
        OwnerTargetInitialAcceptanceDelivery::DeliveredAwaitingSettlement,
    );
    let settled = settle_initial_on_actor(&executor, handoff, &owner, &gate)
        .expect("exact delivered acceptance settles");
    assert_eq!(
        settled.disposition(),
        OwnerTargetInitialAcceptanceSettlementDisposition::SettledNow
    );
    let handoff = settled.into_handoff();
    let settled_inventory = handoff
        .inventory(owner.queue.as_ref().expect("owner FIFO remains live"))
        .expect("settled handoff retains its physical queue binding");
    assert_eq!(settled_inventory.acceptance_receipts(), 0);
    assert_eq!(
        settled_inventory.delivery(),
        OwnerTargetInitialAcceptanceDelivery::Settled
    );
    let repeated = settle_initial_on_actor(&executor, handoff, &owner, &gate)
        .expect("repeated settlement is typed and inert");
    assert_eq!(
        repeated.disposition(),
        OwnerTargetInitialAcceptanceSettlementDisposition::AlreadySettled
    );
    let handoff = repeated.into_handoff();
    assert_eq!(handoff.retained_acceptance_receipts(), 0);

    drop((executor, handoff));
    client.request_cancel();
    owner.request_cancel();
    other_client.request_cancel();
    other_owner.request_cancel();
    let _ = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(
            client.join(),
            owner.join(),
            other_client.join(),
            other_owner.join()
        )
    })
    .await
    .expect("exact-delivery actor pair joins within the local bound");
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("listener cleanup is bounded")
        .expect("the exact actor slot is released");
}

#[tokio::test(flavor = "current_thread")]
async fn terminal_barrier_before_and_after_delivery_returns_exact_handoff_ownership() {
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
        .expect("two exact terminal-order actors fit"),
    )
    .await
    .expect("bind terminal-order listener");
    let (mut client_pending, mut owner_pending) =
        connect_real_actor_pair(&mut listener, LegId::A, limits, peer_policy).await;
    let (mut client_delivered, mut owner_delivered) =
        connect_real_actor_pair(&mut listener, LegId::B, limits, peer_policy).await;

    let held_delivery = Arc::clone(&client_pending.delivery_writer_gate);
    held_delivery.arm();
    let capacity_plans = PhysicalActorSessionPlans::derive();
    let (_accepted_pending, bootstrap_pending) = complete_initial_actor_attach_with_capacity_plans(
        &mut client_pending,
        &mut owner_pending,
        capacity_plans,
    )
    .await;
    tokio::time::timeout(TEST_TIMEOUT, held_delivery.wait_until_blocked())
        .await
        .expect("pending acceptance ACK is held before physical delivery");
    let (_accepted_delivered, bootstrap_delivered) =
        complete_initial_actor_attach_with_capacity_plans(
            &mut client_delivered,
            &mut owner_delivered,
            capacity_plans,
        )
        .await;
    wait_actor_ordered_delivery(&mut owner_delivered).await;

    let (executor_pending, handoff_pending) = initial_owner_executor(bootstrap_pending);
    let (executor_delivered, handoff_delivered) = initial_owner_executor(bootstrap_delivered);
    let gate = QuinnLegCompositionGate::mint_for_test();
    // Declared input of complete_initial_actor_attach_with_capacity_plans; not an identity capability.
    let mut controller_pending_ready =
        ReadyIndex::cold(&ReadyGeometry::checked(std::num::NonZeroUsize::new(1).unwrap()).unwrap())
            .unwrap();
    let ready_limits = OwnerReadySessionLimits::checked(
        capacity_plans.owner.session_config().max_flows(),
        1,
        8,
        8,
    )
    .unwrap();
    let ready = controller_pending_ready
        .reserve_session(SessionId::new([0x93; 16]).unwrap(), ready_limits)
        .unwrap();
    let controller_pending = OwnerSessionController::new_checked(
        executor_pending,
        owner_pending.session_clock(&gate),
        capacity_plans.owner,
        ready,
        std::time::Duration::from_secs(1),
    )
    .expect("pending controller retains the exact installed owner plan");
    // Declared input of complete_initial_actor_attach_with_capacity_plans; not an identity capability.
    let mut controller_delivered_ready =
        ReadyIndex::cold(&ReadyGeometry::checked(std::num::NonZeroUsize::new(1).unwrap()).unwrap())
            .unwrap();
    let ready_limits = OwnerReadySessionLimits::checked(
        capacity_plans.owner.session_config().max_flows(),
        1,
        8,
        8,
    )
    .unwrap();
    let ready = controller_delivered_ready
        .reserve_session(SessionId::new([0x93; 16]).unwrap(), ready_limits)
        .unwrap();
    let controller_delivered = OwnerSessionController::new_checked(
        executor_delivered,
        owner_delivered.session_clock(&gate),
        capacity_plans.owner,
        ready,
        std::time::Duration::from_secs(1),
    )
    .expect("delivered controller retains the exact installed owner plan");
    let mut root_pending = QuinnOwnerInitialSessionComposition::adopt_controller(
        owner_pending,
        controller_pending,
        handoff_pending,
    )
    .expect("pending handoff and actor enter one exact owner root");
    let mut root_delivered = QuinnOwnerInitialSessionComposition::adopt_controller(
        owner_delivered,
        controller_delivered,
        handoff_delivered,
    )
    .expect("delivered handoff and actor enter one exact owner root");
    assert_eq!(
        root_pending
            .handoff_inventory()
            .expect("pending root inventories its exact handoff")
            .delivery(),
        OwnerTargetInitialAcceptanceDelivery::AdmittedDeliveryPending
    );
    assert_eq!(
        root_delivered
            .handoff_inventory()
            .expect("delivered root inventories its exact handoff")
            .delivery(),
        OwnerTargetInitialAcceptanceDelivery::DeliveredAwaitingSettlement
    );

    let pending = root_pending
        .join_for_shutdown()
        .await
        .expect_err("root shutdown must defer physical cancellation until exact delivery");
    let QuinnOwnerInitialShutdownError::AcceptancePending(pending_inventory) = pending else {
        panic!("pending root returned the wrong retry discriminator: {pending:?}");
    };
    assert!(root_pending.shutdown_requested());
    assert_eq!(pending_inventory.acceptance_receipts(), 1);
    assert_eq!(pending_inventory.physical_actor_identities(), 1);
    assert_eq!(
        pending_inventory.delivery(),
        OwnerTargetInitialAcceptanceDelivery::AdmittedDeliveryPending,
    );
    assert_eq!(
        root_pending
            .handoff_inventory()
            .expect("rejected shutdown leaves the exact Running root auditable"),
        pending_inventory,
        "the lifecycle cutoff cannot detach or consume the pending handoff",
    );

    held_delivery.release();
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let delivery = root_pending
                .handoff_inventory()
                .expect("root keeps the exact acceptance receipt auditable")
                .delivery();
            match delivery {
                OwnerTargetInitialAcceptanceDelivery::DeliveredAwaitingSettlement => break,
                OwnerTargetInitialAcceptanceDelivery::AdmittedDeliveryPending => {
                    tokio::task::yield_now().await;
                }
                OwnerTargetInitialAcceptanceDelivery::Settled => {
                    panic!("terminal cannot consume the exact acceptance receipt")
                }
            }
        }
    })
    .await
    .expect("the already-submitted exact ACK completes after its writer gate opens");

    let (
        pending_root_joined,
        delivered_root_joined,
        client_pending_joined,
        client_delivered_joined,
    ) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(
            root_pending.join_for_shutdown(),
            root_delivered.join_for_shutdown(),
            client_pending.join(),
            client_delivered.join(),
        )
    })
    .await
    .expect("both initial owner roots and their peers join within the local bound");
    let pending_root_joined =
        pending_root_joined.expect("delivery retry structurally joins pending A");
    let delivered_root_joined =
        delivered_root_joined.expect("already-delivered initial root joins directly");
    for joined in [pending_root_joined, delivered_root_joined] {
        assert_eq!(joined.actor_owned_messages(), 0);
        assert_eq!(joined.actor_owned_bytes(), 0);
        assert_eq!(joined.actor_owned_terminals(), 1);
        assert!(!joined.actor_terminal_failure());
        assert_eq!(joined.retained_executors(), 1);
        assert_eq!(joined.retained_attached_authorities(), 1);
        assert_eq!(joined.retained_acceptance_receipts(), 0);
        assert_eq!(joined.retained_physical_actor_identities(), 1);
    }
    for joined in [
        client_pending_joined.expect("pending client joins structurally"),
        client_delivered_joined.expect("delivered client joins structurally"),
    ] {
        assert_eq!(joined.owned_messages(), 0);
        assert_eq!(joined.owned_bytes(), 0);
    }
    assert_eq!(
        root_pending
            .join_for_shutdown()
            .await
            .expect("joined pending-root retry is inert"),
        pending_root_joined,
    );
    assert_eq!(
        root_delivered
            .join_for_shutdown()
            .await
            .expect("joined delivered-root retry is inert"),
        delivered_root_joined,
    );
    drop((root_pending, root_delivered));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("terminal-order listener cleanup is bounded")
        .expect("dropping both terminal actors releases their slots");
}

#[test]
fn current_terminal_bind_and_discard_share_one_closed_root_transaction() {
    let source = include_str!("../../quinn_leg_composition/owner_initial.rs");
    let root = source
        .split_once("struct QuinnOwnerInitialControllerTerminalRoot")
        .expect("controller, active handoff, terminal phase, and failure share one root")
        .1
        .split_once("struct QuinnOwnerInitialTerminalTransaction")
        .expect("the complete controller-terminal root has one RAII transaction")
        .0;
    assert!(root.contains("controller: OwnerSessionController<T>"));
    assert!(root.contains("active: Option<OwnerTargetInitialHandoff>"));
    assert!(root.contains("state: QuinnOwnerInitialCurrentTerminalState"));
    assert!(!root.contains("terminal_stamp_pair_verified"));
    assert!(!root.contains("Option<OwnerSessionControllerError>"));
    assert!(!root.contains("failure:"));
    assert!(source.contains("ControllerDiscard { _error: OwnerSessionControllerError }"));

    let callback = source
        .split_once("fn apply_reported_terminal(")
        .expect("the RAII transaction owns the actor callback")
        .1
        .split_once("impl<T: TargetIo> Drop for QuinnOwnerInitialTerminalTransaction")
        .expect("the callback is closed before the transaction drop policy")
        .0;
    let bind = callback
        .find(".bind_initial_terminal(")
        .expect("the actor callback binds exact current A");
    let discard = callback
        .find(".discard_recovery_terminal(")
        .expect("the same actor callback settles the bound terminal");
    assert!(bind < discard, "discard follows binding in one callback");
    assert!(callback.contains("self.record_bound_terminal(bound, terminal_stamp);"));
    let bound_publication = source
        .split_once("fn record_bound_terminal(")
        .expect("post-bind state publication has one root-owned helper")
        .1
        .split_once("fn quarantine_controller_discard(")
        .expect("bound publication completes before discard-failure settlement")
        .0;
    assert!(bound_publication.contains("QuinnOwnerInitialCurrentTerminalState::Quarantined"));
    assert!(!source.contains("RetryBlocked"));
}
