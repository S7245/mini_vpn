//! Actual Owner observed DATA custody through the original Executor gate.
//!
//! These tests use real Driver DATA/RESET/peer ACK and the four permit ledgers.
//! Final-ACK tests cover filtered-prefix and real permit-wake unwind custody
//! through the Unbound Executor, not checked Bound Root scheduling.
//! TCP normalizes payload backing; pre-consumption payload plus whole-custody
//! source review is not a post-retention byte getter or a backing-Drop probe.
//! Root scheduling, shutdown and whole-session-zero acceptance are separate.

mod storage;

use super::*;
use crate::owned_upstream::tcp::{DriverInput, FlowPortProbe, ResumableTcpPortFactory};
use crate::resumable::{CommittedLeg, LocalFlow};
use std::task::{Context, Poll, Waker};

struct QueuedOwnerData {
    leg: CommittedLeg,
    executor: OwnerTargetExecutor<MemoryTarget>,
    local: LocalFlow,
    input: DriverInput,
    probe: FlowPortProbe,
    factory: ResumableTcpPortFactory,
}

fn pull_data(executor: &mut OwnerTargetExecutor<MemoryTarget>, local: LocalFlow) -> DriverInput {
    let source = executor
        .flows
        .get_mut(&local.flow_id())
        .unwrap()
        .source
        .as_mut()
        .unwrap();
    source
        .flow
        .try_send_uplink_with(4, || Bytes::from_static(b"late"))
        .unwrap();
    let mut cx = Context::from_waker(Waker::noop());
    let Poll::Ready(Some(input)) = source.driver.poll_owner_ordered_source(&mut cx) else {
        panic!("the exact queued DATA must be ready");
    };
    assert!(matches!(&input, DriverInput::Data(data)
        if data.flow() == local && data.payload() == b"late"));
    input
}

fn queued_owner_data() -> QueuedOwnerData {
    let (leg, mut executor) = executor();
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
    let input = pull_data(&mut executor, local);
    QueuedOwnerData {
        leg,
        executor,
        local,
        input,
        probe,
        factory,
    }
}

fn retire_with_real_reset(executor: &mut OwnerTargetExecutor<MemoryTarget>, local: LocalFlow) {
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
    let mut cx = Context::from_waker(Waker::noop());
    let Poll::Ready(Some(reset)) = source.driver.poll_owner_control(&mut cx) else {
        panic!("the exact queued RESET must be ready");
    };
    assert!(
        matches!(&reset, DriverInput::Control(SessionEvent::LocalReset { flow, .. }) if *flow == local)
    );
    let (effects, observations) = executor
        .reduce_owner_command_observed(SessionOwnerCommand::Driver(reset))
        .unwrap();
    let outputs = executor
        .execute_effects_observed(effects, observations)
        .unwrap()
        .into_outputs();
    let _outputs = drain_resumes(executor, outputs);
}

fn assert_owned(
    probe: &FlowPortProbe,
    factory: &ResumableTcpPortFactory,
    bytes: usize,
    segments: usize,
) {
    assert_eq!(probe.snapshot().uplink_owned_bytes(), bytes);
    assert_eq!(probe.snapshot().uplink_owned_segments(), segments);
    assert_eq!(factory.session_uplink_owned_bytes(), bytes);
    assert_eq!(factory.session_uplink_owned_segments(), segments);
}

#[test]
fn owner_observed_late_data_retains_original_payload_and_four_permits() {
    let QueuedOwnerData {
        mut executor,
        local,
        input,
        probe,
        factory,
        ..
    } = queued_owner_data();
    assert_owned(&probe, &factory, 4, 1);
    retire_with_real_reset(&mut executor, local);
    let error = executor
        .reduce_owner_command_observed(SessionOwnerCommand::Driver(input))
        .unwrap_err();
    assert!(matches!(&error,
        SessionSupervisorError::Reducer(crate::resumable::SessionError::RetiredFlow(id))
            if *id == local.flow_id()
    ));
    let snapshot = executor.snapshot().session;
    assert!(snapshot.poisoned);
    assert_eq!(snapshot.uplink_replay_extents, 0);
    assert_eq!(snapshot.quarantined_uplink_extents, 1);
    assert_eq!(snapshot.quarantined_uplink_owned_bytes, 4);
    assert_owned(&probe, &factory, 4, 1);
    drop(error);
    assert_owned(&probe, &factory, 4, 1);
    drop(executor);
    assert_owned(&probe, &factory, 0, 0);
}

#[test]
fn already_poisoned_owner_retains_next_exact_data_without_reduction() {
    let QueuedOwnerData {
        mut executor,
        local,
        input,
        probe,
        factory,
        ..
    } = queued_owner_data();
    let next = pull_data(&mut executor, local);
    assert_owned(&probe, &factory, 8, 2);
    retire_with_real_reset(&mut executor, local);
    assert!(
        matches!(executor.reduce_owner_command_observed(SessionOwnerCommand::Driver(input)),
        Err(SessionSupervisorError::Reducer(crate::resumable::SessionError::RetiredFlow(id)))
            if id == local.flow_id())
    );
    let before = executor.snapshot().session.session;
    assert!(matches!(
        executor.reduce_owner_command_observed(SessionOwnerCommand::Driver(next)),
        Err(SessionSupervisorError::PoisonedOwnershipInvariant)
    ));
    let after = executor.snapshot().session;
    assert_eq!(after.session, before);
    assert!(after.poisoned);
    assert_eq!(after.uplink_replay_extents, 0);
    assert_eq!(after.quarantined_uplink_extents, 2);
    assert_eq!(after.quarantined_uplink_owned_bytes, 8);
    assert_owned(&probe, &factory, 8, 2);
    drop(executor);
    assert_owned(&probe, &factory, 0, 0);
}

#[test]
fn owner_data_unwind_keeps_real_staging_or_published_replay() {
    use crate::owned_upstream::supervisor::OwnerDataUnwindPoint;
    for point in [
        OwnerDataUnwindPoint::Staging,
        OwnerDataUnwindPoint::ReplayBound,
        OwnerDataUnwindPoint::ReplayPublished,
    ] {
        let QueuedOwnerData {
            mut executor,
            input,
            probe,
            factory,
            ..
        } = queued_owner_data();
        executor.supervisor.arm_owner_data_unwind(point);
        let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            executor.reduce_owner_command_observed(SessionOwnerCommand::Driver(input))
        }));
        assert!(
            caught.is_err(),
            "single-use fault must execute in the actual Owner transaction"
        );
        let snapshot = executor.snapshot().session;
        assert!(snapshot.poisoned);
        let published = usize::from(point == OwnerDataUnwindPoint::ReplayPublished);
        assert_eq!(snapshot.uplink_replay_extents, published);
        assert_eq!(snapshot.uplink_replay_owned_bytes, 4 * published);
        assert_eq!(snapshot.quarantined_uplink_extents, 1 - published);
        assert_eq!(snapshot.quarantined_uplink_owned_bytes, 4 * (1 - published));
        assert_owned(&probe, &factory, 4, 1);
        drop(caught);
        assert_owned(&probe, &factory, 4, 1);
        drop(executor);
        assert_owned(&probe, &factory, 0, 0);
    }
}

fn finish_data_before_final_ack(
    executor: &mut OwnerTargetExecutor<MemoryTarget>,
    leg: CommittedLeg,
    local: LocalFlow,
    input: DriverInput,
) {
    let (effects, observations) = executor
        .reduce_owner_command_observed(SessionOwnerCommand::Driver(input))
        .unwrap();
    let outputs = executor
        .execute_effects_observed(effects, observations)
        .unwrap()
        .into_outputs();
    let outputs = drain_resumes(executor, outputs);
    assert!(transmitted(&outputs).iter().any(|record| matches!(record,
        Record::Data { flow_id, direction: Direction::TargetToClient, offset, payload }
            if *flow_id == local.flow_id() && *offset == ByteOffset::new(0)
                && payload.as_ref() == b"late")));
    let close = peer_frame(
        executor,
        leg,
        Record::Close {
            flow_id: local.flow_id(),
            direction: Direction::ClientToTarget,
            final_offset: ByteOffset::new(0),
        },
    )
    .unwrap();
    let close = drain_resumes(executor, close);
    assert!(
        !close
            .iter()
            .any(|output| matches!(output, OwnerTargetOutput::TerminalGraceStarted { .. }))
    );
    assert_eq!(executor.target().snapshot().write_half_closes, 1);
    executor.target_mut().finish_read(local.flow_id()).unwrap();
    let eof = executor.try_read_target(local.flow_id()).unwrap();
    let eof = drain_resumes(executor, eof);
    assert!(transmitted(&eof).iter().any(|record| matches!(record,
        Record::Close { flow_id, direction: Direction::TargetToClient, final_offset }
            if *flow_id == local.flow_id() && *final_offset == ByteOffset::new(4))));
    let snapshot = executor.snapshot();
    assert_eq!(snapshot.session.session.flow_count(), 1);
    assert_eq!(snapshot.session.uplink_replay_extents, 1);
    assert_eq!(snapshot.session.uplink_replay_owned_bytes, 4);
    assert_eq!(snapshot.target_flows, 1);
}

fn final_data_ack(leg: CommittedLeg, local: LocalFlow) -> SessionEvent {
    SessionEvent::PeerFrame {
        leg,
        frame: Frame::try_new(
            leg.generation(),
            Record::Ack {
                flow_id: local.flow_id(),
                direction: Direction::TargetToClient,
                next_accepted: ByteOffset::new(4),
                final_accepted: true,
            },
        )
        .unwrap(),
    }
}

#[test]
fn owner_final_ack_filters_receipt_before_terminal_unwind_without_losing_suffix() {
    use crate::owned_upstream::supervisor::OwnerDataUnwindPoint;
    let QueuedOwnerData {
        leg,
        mut executor,
        local,
        input,
        probe,
        factory,
    } = queued_owner_data();
    finish_data_before_final_ack(&mut executor, leg, local, input);
    assert_owned(&probe, &factory, 4, 1);
    let target_before = executor.target().snapshot();
    executor
        .supervisor
        .arm_owner_data_unwind(OwnerDataUnwindPoint::ReceiptTerminal);
    let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        executor.apply_event_observed(final_data_ack(leg, local))
    }));
    assert_eq!(
        caught
            .as_ref()
            .err()
            .and_then(|cause| cause.downcast_ref::<&str>()),
        Some(&"single-use Owner terminal receipt unwind")
    );
    let retained = executor.snapshot();
    assert!(retained.session.poisoned);
    assert_eq!(retained.session.retained_receipt_effects, 1);
    assert_eq!(retained.session.retained_receipt_observations, 1);
    assert_eq!(retained.session.uplink_replay_extents, 0);
    assert_eq!(retained.session.uplink_replay_owned_bytes, 0);
    assert_eq!(retained.session.session.flow_count(), 0);
    assert_eq!(retained.session.session.terminal_tombstones(), 1);
    let replay = retained
        .session
        .session
        .replay_usage(Direction::TargetToClient);
    assert_eq!((replay.bytes(), replay.segments()), (0, 0));
    assert_eq!(
        retained.target_flows, 1,
        "terminal effect has not reached Target"
    );
    assert_eq!(executor.target().snapshot(), target_before);
    assert_owned(&probe, &factory, 0, 0);
    assert!(matches!(
        executor.apply_event_observed(final_data_ack(leg, local)),
        Err(OwnerTargetError::Supervisor(
            SessionSupervisorError::PoisonedOwnershipInvariant
        ))
    ));
    assert_eq!(executor.snapshot(), retained);
    assert_eq!(executor.target().snapshot(), target_before);
    assert_owned(&probe, &factory, 0, 0);
    drop(caught);
    drop(executor);
    assert_owned(&probe, &factory, 0, 0);
}

#[test]
fn owner_final_ack_permit_callback_unwind_retains_current_and_terminal_suffix() {
    use std::future::Future;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::task::Wake;

    struct ReceiptDropWake {
        calls: AtomicUsize,
        released: Mutex<Option<[usize; 4]>>,
        probe: FlowPortProbe,
        factory: ResumableTcpPortFactory,
    }
    impl Wake for ReceiptDropWake {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                let flow = self.probe.snapshot();
                *self.released.lock().unwrap() = Some([
                    flow.uplink_owned_bytes(),
                    flow.uplink_owned_segments(),
                    self.factory.session_uplink_owned_bytes(),
                    self.factory.session_uplink_owned_segments(),
                ]);
                panic!("single-use real permit progress callback unwind");
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
    finish_data_before_final_ack(&mut executor, leg, local, input);
    assert_owned(&probe, &factory, 4, 1);
    let target_before = executor.target().snapshot();
    let cursor = factory.progress_cursor();
    let callback = Arc::new(ReceiptDropWake {
        calls: AtomicUsize::new(0),
        released: Mutex::new(None),
        probe: probe.clone(),
        factory: factory.clone(),
    });
    let waker = Waker::from(Arc::clone(&callback));
    let mut waiter = Box::pin(factory.wait_for_progress(&cursor));
    assert!(
        waiter
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    assert_eq!(callback.calls.load(Ordering::SeqCst), 0);

    let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        executor.apply_event_observed(final_data_ack(leg, local))
    }));
    assert_eq!(
        caught
            .as_ref()
            .err()
            .and_then(|cause| cause.downcast_ref::<&str>()),
        Some(&"single-use real permit progress callback unwind")
    );
    assert_eq!(callback.calls.load(Ordering::SeqCst), 1);
    assert_eq!(*callback.released.lock().unwrap(), Some([0, 0, 0, 0]));
    drop(waiter);
    let retained = executor.snapshot();
    assert!(retained.session.poisoned);
    assert_eq!(
        retained.session.retained_receipt_effects, 2,
        "ACK current item has not been filtered; original FlowFinished follows it"
    );
    assert_eq!(retained.session.retained_receipt_observations, 0);
    // The original extent metadata remains in the map, but its four permits
    // have actually been released. The map's bytes are not a semaphore balance.
    assert_eq!(retained.session.uplink_replay_extents, 1);
    assert_eq!(retained.session.uplink_replay_owned_bytes, 4);
    assert_eq!(retained.session.session.flow_count(), 0);
    assert_eq!(retained.session.session.terminal_tombstones(), 1);
    let replay = retained
        .session
        .session
        .replay_usage(Direction::TargetToClient);
    assert_eq!((replay.bytes(), replay.segments()), (0, 0));
    assert_owned(&probe, &factory, 0, 0);
    assert_eq!(retained.target_flows, 1);
    assert_eq!(executor.target().snapshot(), target_before);
    assert!(matches!(
        executor.apply_event_observed(final_data_ack(leg, local)),
        Err(OwnerTargetError::Supervisor(
            SessionSupervisorError::PoisonedOwnershipInvariant
        ))
    ));
    assert_eq!(executor.snapshot(), retained);
    assert_eq!(executor.target().snapshot(), target_before);
    assert_eq!(callback.calls.load(Ordering::SeqCst), 1);
    drop(caught);
    drop(executor);
    assert_owned(&probe, &factory, 0, 0);
    assert_eq!(callback.calls.load(Ordering::SeqCst), 1);
}

#[test]
fn owner_committed_data_retains_frame_and_fact_through_bind_and_publish_unwind() {
    use crate::owned_upstream::supervisor::OwnerDataUnwindPoint;
    for point in [
        OwnerDataUnwindPoint::ReplayBound,
        OwnerDataUnwindPoint::ReplayPublished,
    ] {
        let QueuedOwnerData {
            leg,
            mut executor,
            local,
            input,
            probe,
            factory,
        } = queued_owner_data();
        let target_before = executor.target().snapshot();
        executor.supervisor.arm_owner_data_unwind(point);
        let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            executor.reduce_owner_command_observed(SessionOwnerCommand::Driver(input))
        }));
        assert_eq!(
            caught
                .as_ref()
                .err()
                .and_then(|cause| cause.downcast_ref::<&str>()),
            Some(&"single-use Owner DATA custody unwind")
        );
        let retained = executor.snapshot();
        assert!(retained.session.poisoned);
        assert_eq!(
            retained.session.retained_receipt_effects, 1,
            "actual committed DATA Transmit must remain in its Supervisor"
        );
        assert_eq!(
            retained.session.retained_receipt_observations, 1,
            "actual first-outstanding fact must survive before receipt publication"
        );
        let published = usize::from(point == OwnerDataUnwindPoint::ReplayPublished);
        assert_eq!(retained.session.uplink_replay_extents, published);
        assert_eq!(retained.session.quarantined_uplink_extents, 1 - published);
        let replay = retained
            .session
            .session
            .replay_usage(Direction::TargetToClient);
        assert_eq!((replay.bytes(), replay.segments()), (4, 1));
        assert_owned(&probe, &factory, 4, 1);
        assert_eq!(executor.target().snapshot(), target_before);
        assert!(matches!(
            executor.apply_event_observed(final_data_ack(leg, local)),
            Err(OwnerTargetError::Supervisor(
                SessionSupervisorError::PoisonedOwnershipInvariant
            ))
        ));
        assert_eq!(executor.snapshot(), retained);
        assert_eq!(executor.target().snapshot(), target_before);
        assert_owned(&probe, &factory, 4, 1);
        drop(caught);
        drop(executor);
        assert_owned(&probe, &factory, 0, 0);
    }
}

#[test]
fn borrowed_owner_bind_rejection_keeps_original_receipt_and_all_permits() {
    fn stored(effects: &[SessionEffect]) -> &crate::resumable::ReplayStored {
        effects
            .iter()
            .find_map(|effect| match effect {
                SessionEffect::ReplayStored { receipt } => Some(receipt),
                _ => None,
            })
            .unwrap()
    }
    // Direct TCP binding contract: real reducer receipts and two real sources.
    // This does not claim the Supervisor observation gate or Root scheduling.
    let leg = committed_leg(2, 0x22, 0x42);
    let mut model = SessionModel::new(SessionRole::Owner, session_config(), leg);
    let factory =
        ResumableTcpPortFactory::new_unbound_for_test(source_config(), 128, 8, 16).unwrap();
    let mut cases = Vec::new();
    for id in [1, 3] {
        let effects = model
            .reduce(SessionEvent::PeerFrame {
                leg,
                frame: Frame::try_new(
                    leg.generation(),
                    Record::Open {
                        flow_id: SessionFlowId::new(id).unwrap(),
                        target: target(443),
                    },
                )
                .unwrap(),
            })
            .unwrap();
        let (request, flow) = effects
            .iter()
            .find_map(|effect| match effect {
                SessionEffect::PeerOpenRequested { request, flow, .. } => Some((*request, *flow)),
                _ => None,
            })
            .unwrap();
        model
            .reduce(SessionEvent::PeerOpenResolved {
                request,
                result: OpenResultCode::Opened,
            })
            .unwrap();
        let (endpoint, mut driver) = factory.open_flow(flow).unwrap();
        endpoint
            .try_send_uplink_with(4, || Bytes::from_static(b"good"))
            .unwrap();
        let Poll::Ready(Some(DriverInput::Data(data))) =
            driver.poll_owner_ordered_source(&mut Context::from_waker(Waker::noop()))
        else {
            panic!("actual source DATA must be ready");
        };
        let (event, ownership) = data.into_event_and_ownership();
        let effects = model.reduce(event).unwrap();
        cases.push((endpoint, driver, ownership, effects));
    }
    let (first, first_driver, mut first_staging, first_effects) = cases.remove(0);
    let (second, second_driver, mut second_staging, second_effects) = cases.remove(0);
    let receipt = stored(&second_effects);
    let address = receipt as *const _;
    let payload = receipt.payload().as_ptr();
    for _ in 0..2 {
        assert_eq!(
            first_staging.bind_owner_replay(receipt).unwrap_err(),
            FlowPortError::UplinkReplayReceiptMismatch
        );
        assert_eq!(stored(&second_effects) as *const _, address);
        assert_eq!(receipt.payload().as_ptr(), payload);
        assert_eq!(receipt.payload().as_ref(), b"good");
        assert_eq!(first.probe().snapshot().uplink_owned_bytes(), 4);
        assert_eq!(first.probe().snapshot().uplink_owned_segments(), 1);
        assert_eq!(second.probe().snapshot().uplink_owned_bytes(), 4);
        assert_eq!(second.probe().snapshot().uplink_owned_segments(), 1);
        assert_eq!(factory.session_uplink_owned_bytes(), 8);
        assert_eq!(factory.session_uplink_owned_segments(), 2);
    }
    let first_replay = first_staging
        .bind_owner_replay(stored(&first_effects))
        .unwrap();
    assert_eq!(
        first_staging.bind_owner_replay(receipt).unwrap_err(),
        FlowPortError::UplinkOwnershipAlreadyBound
    );
    assert_eq!(stored(&second_effects) as *const _, address);
    assert_eq!(receipt.payload().as_ptr(), payload);
    let second_replay = second_staging.bind_owner_replay(receipt).unwrap();
    assert_eq!(stored(&second_effects) as *const _, address);
    assert_eq!(factory.session_uplink_owned_bytes(), 8);
    assert_eq!(factory.session_uplink_owned_segments(), 2);
    drop(first_replay);
    assert_eq!(first.probe().snapshot().uplink_owned_bytes(), 0);
    assert_eq!(second.probe().snapshot().uplink_owned_bytes(), 4);
    assert_eq!(factory.session_uplink_owned_bytes(), 4);
    drop(second_replay);
    assert_eq!(factory.session_uplink_owned_bytes(), 0);
    assert_eq!(factory.session_uplink_owned_segments(), 0);
    drop((
        first,
        first_driver,
        second,
        second_driver,
        first_staging,
        second_staging,
    ));
}

#[test]
fn actual_target_acceptance_keeps_ack_and_frontier_until_final_journal_take() {
    use crate::owned_upstream::supervisor::OwnerDataUnwindPoint;
    let (leg, mut executor) = executor();
    let flow_id = SessionFlowId::new(1).unwrap();
    let outputs = open_flow(&mut executor, leg, flow_id, target(443));
    let _outputs = drain_resumes(&mut executor, outputs);
    executor
        .supervisor
        .arm_owner_data_unwind(OwnerDataUnwindPoint::ReceiptSinkAccepted);
    let incoming = || SessionEvent::PeerFrame {
        leg,
        frame: Frame::try_new(
            leg.generation(),
            Record::Data {
                flow_id,
                direction: Direction::ClientToTarget,
                offset: ByteOffset::new(0),
                payload: Bytes::from_static(b"peer"),
            },
        )
        .unwrap(),
    };
    let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        executor.apply_event_observed(incoming())
    }));
    assert_eq!(
        caught
            .as_ref()
            .err()
            .and_then(|cause| cause.downcast_ref::<&str>()),
        Some(&"single-use actual SinkAccepted journal unwind")
    );
    let retained = executor.snapshot();
    assert!(retained.session.poisoned);
    assert_eq!(retained.session.retained_receipt_effects, 1);
    assert_eq!(retained.session.retained_receipt_observations, 1);
    assert_eq!(retained.session.session.receive_owned_bytes(), 0);
    let target_before = executor.target().snapshot();
    assert_eq!(target_before.write_calls, 1);
    assert_eq!(target_before.write_accepted_bytes, 4);
    let rejected = incoming();
    let original_payload = match &rejected {
        SessionEvent::PeerFrame { frame, .. } => match frame.record() {
            Record::Data { payload, .. } => payload.as_ptr(),
            _ => panic!("fixture DATA"),
        },
        _ => panic!("fixture PeerFrame"),
    };
    let Err(OwnerTargetError::RejectedEvent { block, event }) =
        executor.apply_event_observed(rejected)
    else {
        panic!("the owning outer journal must reject first");
    };
    assert_eq!(block, OwnerTargetInputBlock::AbortedEffectInvariant);
    assert!(
        matches!(*event, SessionEvent::PeerFrame { leg: returned, ref frame }
        if returned == leg && matches!(frame.record(), Record::Data {
            flow_id: actual, direction: Direction::ClientToTarget, offset, payload,
        } if *actual == flow_id && *offset == ByteOffset::new(0)
            && payload.as_ptr() == original_payload && payload.as_ref() == b"peer"))
    );
    assert_eq!(executor.snapshot(), retained);
    assert_eq!(executor.target().snapshot(), target_before);
    drop(caught);
    drop(executor);
}
