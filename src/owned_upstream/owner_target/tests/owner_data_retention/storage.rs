//! Original Owner DATA admission, prepared backing and receipt-release custody.
//! Uses the existing real Driver/Executor gate and exact four permit ledgers.
//! Not checked Bound scheduling or whole-root shutdown acceptance.

#[test]
fn owner_terminal_unwind_retains_current_and_unreleased_original_nodes() {
    use std::future::Future;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    use std::task::Wake;
    struct TerminalWake(AtomicBool);
    impl Wake for TerminalWake {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            if !self.0.swap(true, Ordering::SeqCst) {
                panic!("real terminal permit wake");
            }
        }
    }
    let QueuedOwnerData {
        leg: _,
        mut executor,
        local,
        input,
        probe,
        factory,
    } = queued_owner_data();
    let second = pull_data(&mut executor, local);
    for input in [input, second] {
        let (effects, observations) = executor
            .reduce_owner_command_observed(SessionOwnerCommand::Driver(input))
            .unwrap();
        let outputs = executor
            .execute_effects_observed(effects, observations)
            .unwrap()
            .into_outputs();
        let _outputs = drain_resumes(&mut executor, outputs);
    }
    // Queue and take control before arming: RESET publication also signals
    // progress, but this tracer targets the later exact permit release.
    let source = executor
        .flows
        .get_mut(&local.flow_id())
        .unwrap()
        .source
        .as_mut()
        .unwrap();
    source
        .flow
        .try_send_reset(ResetReason::TargetFailure)
        .unwrap();
    let Poll::Ready(Some(reset)) = source
        .driver
        .poll_owner_control(&mut Context::from_waker(Waker::noop()))
    else {
        panic!("queued real RESET");
    };
    assert!(
        matches!(&reset, DriverInput::Control(SessionEvent::LocalReset { flow, .. }) if *flow == local)
    );
    let cursor = factory.progress_cursor();
    let waker = Waker::from(Arc::new(TerminalWake(AtomicBool::new(false))));
    let mut waiter = Box::pin(factory.wait_for_progress(&cursor));
    assert!(
        waiter
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        executor.reduce_owner_command_observed(SessionOwnerCommand::Driver(reset))
    }));
    assert_eq!(
        caught.as_ref().err().and_then(|c| c.downcast_ref::<&str>()),
        Some(&"real terminal permit wake")
    );
    drop(waiter);
    let inventory = executor.supervisor.uplink_storage_cold_inventory().unwrap();
    assert_eq!(
        (
            inventory.replay_nodes,
            inventory.vacant_nodes,
            inventory.candidate_len
        ),
        (2, 0, 2)
    );
    assert_eq!(
        (
            inventory.uplink.stored_bytes,
            inventory.uplink.permit_bundles
        ),
        (8, 1)
    );
    let snapshot = executor.snapshot();
    assert!(snapshot.session.poisoned);
    assert_eq!(snapshot.session.retained_receipt_effects, 2);
    assert_owned(&probe, &factory, 4, 1);
    drop(caught);
    drop(executor);
    assert_owned(&probe, &factory, 0, 0);
}

#[test]
fn owner_expiry_unwind_retains_current_and_unreleased_original_nodes() {
    use std::future::Future;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    use std::task::Wake;
    struct ExpiryWake(AtomicBool);
    impl Wake for ExpiryWake {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            if !self.0.swap(true, Ordering::SeqCst) {
                panic!("real expiry permit wake");
            }
        }
    }
    let QueuedOwnerData {
        leg,
        mut executor,
        local,
        input,
        probe,
        factory,
    } = queued_owner_data();
    let second = pull_data(&mut executor, local);
    for input in [input, second] {
        let (effects, observations) = executor
            .reduce_owner_command_observed(SessionOwnerCommand::Driver(input))
            .unwrap();
        let outputs = executor
            .execute_effects_observed(effects, observations)
            .unwrap()
            .into_outputs();
        let _outputs = drain_resumes(&mut executor, outputs);
    }
    let outputs = executor
        .apply_event_observed(SessionEvent::LegLost { leg })
        .unwrap()
        .into_outputs();
    let _outputs = drain_resumes(&mut executor, outputs);
    let cursor = factory.progress_cursor();
    let waker = Waker::from(Arc::new(ExpiryWake(AtomicBool::new(false))));
    let mut waiter = Box::pin(factory.wait_for_progress(&cursor));
    assert!(
        waiter
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        executor.apply_event_observed(SessionEvent::ResumeGraceExpired { leg })
    }));
    assert_eq!(
        caught.as_ref().err().and_then(|c| c.downcast_ref::<&str>()),
        Some(&"real expiry permit wake")
    );
    drop(waiter);
    let inventory = executor.supervisor.uplink_storage_cold_inventory().unwrap();
    assert_eq!(
        (
            inventory.replay_nodes,
            inventory.vacant_nodes,
            inventory.candidate_len
        ),
        (2, 0, 0)
    );
    assert_eq!(
        (
            inventory.uplink.stored_bytes,
            inventory.uplink.permit_bundles
        ),
        (8, 1)
    );
    let snapshot = executor.snapshot();
    assert!(snapshot.session.poisoned);
    assert_eq!(snapshot.session.retained_receipt_effects, 1);
    assert_owned(&probe, &factory, 4, 1);
    drop(caught);
    drop(executor);
    assert_owned(&probe, &factory, 0, 0);
}

#[test]
fn real_second_permit_wake_retains_prefix_and_poisoned_growth_preserves_it() {
    use std::future::Future;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::task::Wake;
    struct PanicOnce(AtomicUsize);
    impl Wake for PanicOnce {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
                panic!("second original replay permit wake");
            }
        }
    }
    let (leg, mut executor) = executor();
    // Only fault fixtures may create independent progress/permit ledgers. The
    // first node can release normally; the second's real callback then unwinds.
    let fork = executor
        .source_factory
        .fork_with_independent_ledgers_for_test()
        .unwrap();
    let flow_id = SessionFlowId::new(1).unwrap();
    let outputs = open_flow(&mut executor, leg, flow_id, target(443));
    let _outputs = drain_resumes(&mut executor, outputs);
    let source = executor
        .flows
        .get(&flow_id)
        .unwrap()
        .source
        .as_ref()
        .unwrap();
    let local = source.flow.local_flow();
    let probe = source.flow.probe();
    let factory = executor.source_factory.clone();
    let first = pull_data(&mut executor, local);
    let (flow, mut driver) = fork.open_flow(local).unwrap();
    // Advance only this adversarial Driver's original byte offset to 4; no
    // manufactured ReplayStored, permit, or node is introduced.
    flow.try_send_uplink_with(4, || Bytes::from_static(b"skip"))
        .unwrap();
    drop(driver.try_recv_next().unwrap().unwrap());
    flow.try_send_uplink_with(4, || Bytes::from_static(b"late"))
        .unwrap();
    let second = driver.try_recv_next().unwrap().unwrap();
    flow.try_send_uplink_with(4, || Bytes::from_static(b"late"))
        .unwrap();
    let third = driver.try_recv_next().unwrap().unwrap();
    for input in [first, second, third] {
        let (effects, observations) = executor
            .reduce_owner_command_observed(SessionOwnerCommand::Driver(input))
            .unwrap();
        let outputs = executor
            .execute_effects_observed(effects, observations)
            .unwrap()
            .into_outputs();
        let _outputs = drain_resumes(&mut executor, outputs);
    }
    let before = executor.supervisor.uplink_storage_cold_inventory().unwrap();
    assert_eq!((before.replay_nodes, before.candidate_capacity), (3, 4));
    let cursor = fork.progress_cursor();
    let callback = Arc::new(PanicOnce(AtomicUsize::new(0)));
    let waker = Waker::from(Arc::clone(&callback));
    let mut waiter = Box::pin(fork.wait_for_progress(&cursor));
    assert!(
        waiter
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    let ack = SessionEvent::PeerFrame {
        leg,
        frame: Frame::try_new(
            leg.generation(),
            Record::Ack {
                flow_id,
                direction: Direction::TargetToClient,
                next_accepted: ByteOffset::new(12),
                final_accepted: false,
            },
        )
        .unwrap(),
    };
    let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        executor.apply_event_observed(ack)
    }));
    assert_eq!(
        caught.as_ref().err().and_then(|c| c.downcast_ref::<&str>()),
        Some(&"second original replay permit wake")
    );
    assert_eq!(callback.0.load(Ordering::SeqCst), 1);
    drop(waiter);
    let retained = executor.supervisor.uplink_storage_cold_inventory().unwrap();
    assert_eq!(
        (
            retained.replay_nodes,
            retained.candidate_len,
            retained.candidate_capacity
        ),
        (3, 1, 4)
    );
    assert_eq!(
        (retained.uplink.stored_bytes, retained.uplink.permit_bundles),
        (12, 1)
    );
    assert_eq!(executor.snapshot().session.retained_receipt_effects, 1);
    assert_owned(&probe, &factory, 0, 0);
    assert_owned(&flow.probe(), &fork, 4, 1);

    let fourth = pull_data(&mut executor, local);
    assert!(matches!(
        executor.reduce_owner_command_observed(SessionOwnerCommand::Driver(fourth)),
        Err(SessionSupervisorError::PoisonedOwnershipInvariant)
    ));
    let before_refusal = executor.supervisor.uplink_storage_cold_inventory().unwrap();
    assert_eq!(
        (
            before_refusal.quarantine_nodes,
            before_refusal.candidate_len
        ),
        (1, 1)
    );
    let fifth = pull_data(&mut executor, local);
    let DriverInput::Data(data) = &fifth else {
        panic!("queued fifth DATA");
    };
    let pointer = data.payload().as_ptr();
    executor
        .supervisor
        .fail_next_uplink_scratch_reserve_for_test();
    let Err(SessionSupervisorError::UplinkStorageReserve { input }) =
        executor.reduce_owner_command_observed(SessionOwnerCommand::Driver(fifth))
    else {
        panic!("prepared growth refusal");
    };
    let DriverInput::Data(data) = &input else {
        panic!("returned fifth DATA");
    };
    assert_eq!(data.payload().as_ptr(), pointer);
    assert_eq!(
        executor.supervisor.uplink_storage_cold_inventory().unwrap(),
        before_refusal
    );
    assert!(matches!(
        executor.reduce_owner_command_observed(SessionOwnerCommand::Driver(input)),
        Err(SessionSupervisorError::PoisonedOwnershipInvariant)
    ));
    let grown = executor.supervisor.uplink_storage_cold_inventory().unwrap();
    assert_eq!(
        (
            grown.replay_nodes,
            grown.quarantine_nodes,
            grown.candidate_len,
            grown.candidate_capacity
        ),
        (3, 2, 1, 8)
    );
    assert_eq!(
        (grown.uplink.stored_bytes, grown.uplink.permit_bundles),
        (20, 3)
    );
    assert_owned(&probe, &factory, 8, 2);
    assert_owned(&flow.probe(), &fork, 4, 1);
    drop(caught);
    drop(executor);
    assert_owned(&probe, &factory, 0, 0);
    assert_owned(&flow.probe(), &fork, 0, 0);
}

#[test]
fn owner_quarantine_bound_returns_third_original_input_with_independent_fault_ledgers() {
    let leg = committed_leg(2, 0x22, 0x42);
    let config = SessionConfig::new(
        4,
        8,
        TcpWindowLimits::new(128, 16).unwrap(),
        TcpWindowLimits::new(128, 16).unwrap(),
        ReplayBudgetLimits::new(128, 1).unwrap(),
        ReplayBudgetLimits::new(128, 1).unwrap(),
        ReceiveBudgetLimits::new(256, 32).unwrap(),
        128,
    )
    .unwrap();
    let mut executor = OwnerTargetExecutor::new(
        OwnerTargetConfig::new(128, 1024, 64).unwrap(),
        SessionSupervisor::new(SessionModel::new(SessionRole::Owner, config, leg)),
        MemoryTarget::new(MemoryTargetConfig::new(4, 256, 512, 128, 128, 16).unwrap()),
        source_config(),
    )
    .unwrap();
    // Fork the test-only ledgers before OPEN advances their monotonic gate.
    let first_factory = executor.source_factory.clone();
    let second_factory = first_factory
        .fork_with_independent_ledgers_for_test()
        .unwrap();
    let third_factory = first_factory
        .fork_with_independent_ledgers_for_test()
        .unwrap();
    let flow_id = SessionFlowId::new(1).unwrap();
    let outputs = open_flow(&mut executor, leg, flow_id, target(443));
    let _outputs = drain_resumes(&mut executor, outputs);
    let source = executor
        .flows
        .get(&flow_id)
        .unwrap()
        .source
        .as_ref()
        .unwrap();
    let local = source.flow.local_flow();
    let first_probe = source.flow.probe();
    let first = pull_data(&mut executor, local);
    // Explicit adversarial ledgers, not a claim that production Clone increases
    // its Kr=1 allowance. All values still come through real Driver ownership.
    let (second_flow, mut second_driver) = second_factory.open_flow(local).unwrap();
    let (third_flow, mut third_driver) = third_factory.open_flow(local).unwrap();
    second_flow
        .try_send_uplink_with(4, || Bytes::from_static(b"late"))
        .unwrap();
    third_flow
        .try_send_uplink_with(4, || Bytes::from_static(b"late"))
        .unwrap();
    let second = second_driver.try_recv_next().unwrap().unwrap();
    let third = third_driver.try_recv_next().unwrap().unwrap();
    let DriverInput::Data(data) = &third else {
        panic!("real third DATA");
    };
    let pointer = data.payload().as_ptr();
    let original = data.cold_inventory();
    retire_with_real_reset(&mut executor, local);
    assert!(matches!(
        executor.reduce_owner_command_observed(SessionOwnerCommand::Driver(first)),
        Err(SessionSupervisorError::Reducer(
            crate::resumable::SessionError::RetiredFlow(_)
        ))
    ));
    assert!(matches!(
        executor.reduce_owner_command_observed(SessionOwnerCommand::Driver(second)),
        Err(SessionSupervisorError::PoisonedOwnershipInvariant)
    ));
    let before = executor.snapshot();
    let storage = executor.supervisor.uplink_storage_cold_inventory().unwrap();
    assert_eq!(
        (
            storage.replay_nodes,
            storage.quarantine_nodes,
            storage.vacant_nodes
        ),
        (0, 2, 0)
    );
    assert_eq!(
        (storage.uplink.stored_bytes, storage.uplink.permit_bundles),
        (8, 2)
    );
    let error = executor
        .reduce_owner_command_observed(SessionOwnerCommand::Driver(third))
        .unwrap_err();
    assert_eq!(executor.snapshot(), before);
    assert_eq!(
        executor.supervisor.uplink_storage_cold_inventory().unwrap(),
        storage
    );
    let error = OwnerTargetError::Supervisor(error);
    let audit = error.cold_inventory().unwrap();
    assert_eq!((audit.owner.driver_inputs, audit.uplink), (1, original));
    let OwnerTargetError::Supervisor(SessionSupervisorError::UplinkStorageCapacity { input }) =
        error
    else {
        panic!("exact node admission bound");
    };
    let DriverInput::Data(data) = &input else {
        panic!("same third DATA");
    };
    assert_eq!(data.payload().as_ptr(), pointer);
    assert_eq!(data.flow(), local);
    assert_eq!(data.cold_inventory(), original);
    assert_owned(&first_probe, &first_factory, 4, 1);
    assert_owned(&second_flow.probe(), &second_factory, 4, 1);
    assert_owned(&third_flow.probe(), &third_factory, 4, 1);
    drop(executor);
    assert_owned(&first_probe, &first_factory, 0, 0);
    assert_owned(&second_flow.probe(), &second_factory, 0, 0);
    assert_owned(&third_flow.probe(), &third_factory, 4, 1);
    drop(input);
    assert_owned(&third_flow.probe(), &third_factory, 0, 0);
}

use super::*;

#[test]
fn owner_uplink_scratch_refusal_returns_exact_data_before_model_mutation() {
    let QueuedOwnerData {
        mut executor,
        input,
        probe,
        factory,
        ..
    } = queued_owner_data();
    let DriverInput::Data(data) = &input else {
        panic!("the original source queued DATA");
    };
    let pointer = data.payload().as_ptr();
    let local = data.flow();
    let original = data.cold_inventory();
    let before = executor.snapshot();
    let target_before = executor.target().snapshot();
    executor
        .supervisor
        .fail_next_uplink_scratch_reserve_for_test();
    let error = executor
        .reduce_owner_command_observed(SessionOwnerCommand::Driver(input))
        .unwrap_err();
    assert_eq!(executor.snapshot(), before);
    assert_eq!(executor.target().snapshot(), target_before);
    assert_owned(&probe, &factory, 4, 1);
    let error = OwnerTargetError::Supervisor(error);
    let audit = error.cold_inventory().unwrap();
    assert_eq!(audit.owner.driver_inputs, 1);
    assert_eq!(audit.uplink, original);
    let OwnerTargetError::Supervisor(SessionSupervisorError::UplinkStorageReserve { input }) =
        error
    else {
        panic!("expected exact storage Reserve with original DATA");
    };
    let DriverInput::Data(data) = &input else {
        panic!("storage refusal retains the DATA variant");
    };
    assert_eq!(data.payload().as_ptr(), pointer);
    assert_eq!(data.payload(), b"late");
    assert_eq!(data.flow(), local);
    assert_eq!(data.cold_inventory(), original);
    // The actual scratch hook is single-use. A node failure before reaching it
    // would leave it armed and make this retry fail, not silently pass.
    let (effects, observations) = executor
        .reduce_owner_command_observed(SessionOwnerCommand::Driver(input))
        .unwrap();
    let outputs = executor
        .execute_effects_observed(effects, observations)
        .unwrap()
        .into_outputs();
    let _outputs = drain_resumes(&mut executor, outputs);
    assert_eq!(executor.snapshot().session.uplink_replay_extents, 1);
    assert_owned(&probe, &factory, 4, 1);
    drop(executor);
    assert_owned(&probe, &factory, 0, 0);
}

#[test]
fn acknowledged_owner_nodes_and_empty_scratch_keep_exact_reusable_backing() {
    let QueuedOwnerData {
        leg,
        mut executor,
        local,
        input,
        probe,
        factory,
    } = queued_owner_data();
    let (effects, observations) = executor
        .reduce_owner_command_observed(SessionOwnerCommand::Driver(input))
        .unwrap();
    let outputs = executor
        .execute_effects_observed(effects, observations)
        .unwrap()
        .into_outputs();
    let _outputs = drain_resumes(&mut executor, outputs);
    let live = executor.supervisor.uplink_storage_cold_inventory().unwrap();
    assert_eq!(
        (live.replay_nodes, live.quarantine_nodes, live.vacant_nodes),
        (1, 0, 0)
    );
    assert_eq!((live.candidate_len, live.candidate_capacity), (0, 1));
    assert_eq!(
        (
            live.uplink.stored_bytes,
            live.uplink.flow_byte_permits,
            live.uplink.session_segment_permits
        ),
        (4, 4, 1)
    );
    let ack = SessionEvent::PeerFrame {
        leg,
        frame: Frame::try_new(
            leg.generation(),
            Record::Ack {
                flow_id: local.flow_id(),
                direction: Direction::TargetToClient,
                next_accepted: ByteOffset::new(4),
                final_accepted: false,
            },
        )
        .unwrap(),
    };
    let outputs = executor.apply_event_observed(ack).unwrap().into_outputs();
    let _outputs = drain_resumes(&mut executor, outputs);
    let released = executor.supervisor.uplink_storage_cold_inventory().unwrap();
    assert_eq!((released.replay_nodes, released.vacant_nodes), (0, 1));
    assert!(released.uplink.is_exact_zero());
    assert_eq!(released.node_bytes, live.node_bytes);
    assert_eq!(released.candidate_bytes, live.candidate_bytes);
    assert_eq!(
        (released.candidate_len, released.candidate_capacity),
        (0, 1)
    );
    assert_owned(&probe, &factory, 0, 0);

    executor
        .supervisor
        .fail_next_uplink_scratch_reserve_for_test();
    let next = pull_data(&mut executor, local);
    let (effects, observations) = executor
        .reduce_owner_command_observed(SessionOwnerCommand::Driver(next))
        .unwrap();
    let outputs = executor
        .execute_effects_observed(effects, observations)
        .unwrap()
        .into_outputs();
    let _outputs = drain_resumes(&mut executor, outputs);
    let reused = executor.supervisor.uplink_storage_cold_inventory().unwrap();
    assert_eq!((reused.replay_nodes, reused.vacant_nodes), (1, 0));
    assert_eq!(
        (reused.node_bytes, reused.candidate_bytes),
        (live.node_bytes, live.candidate_bytes)
    );
    assert_owned(&probe, &factory, 4, 1);
    drop(executor);
    assert_owned(&probe, &factory, 0, 0);
}
