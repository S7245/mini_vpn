//! Real Owner source ownership for later two-lane root characterizations.
//!
//! Owns test fixtures and exact DATA/CLOSE/ACK observations, not production
//! source polling, send-cell custody, or whole-session shutdown settlement.

mod send_cells;

use super::*;
use crate::owned_upstream::supervisor::SessionSupervisor;
use crate::owned_upstream::tcp::{
    DriverInput, FlowPortConfig, ResumableTcpDriver, ResumableTcpFlow, ResumableTcpPortFactory,
    SinkDelivery, TunAction,
};
use crate::resumable::{ByteOffset, Direction, ResetReason, SinkOffer};
use bytes::Bytes;
use std::future::{Future, poll_fn};
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};

#[derive(Default)]
struct SourcePollWake(AtomicUsize);

impl Wake for SourcePollWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

struct OwnerSourceFixture {
    supervisor: SessionSupervisor,
    factory: ResumableTcpPortFactory,
    leg: CommittedLeg,
    local_flow: LocalFlow,
    flow: ResumableTcpFlow,
    driver: ResumableTcpDriver,
}

fn owner_source() -> OwnerSourceFixture {
    let (model, leg) = owner_session();
    let mut supervisor = SessionSupervisor::new(model);
    let factory = supervisor
        .mint_tcp_port_factory(FlowPortConfig::new(8, 2, 2, 2).unwrap())
        .unwrap();
    let effects = supervisor
        .apply_event(SessionEvent::PeerFrame {
            leg,
            frame: Frame::try_new(
                leg.generation(),
                Record::Open {
                    flow_id: SessionFlowId::new(1).unwrap(),
                    target: TargetAddr::IpPort(([127, 0, 0, 1], 443).into()),
                },
            )
            .unwrap(),
        })
        .unwrap();
    let (request, local_flow) = effects
        .into_iter()
        .find_map(|effect| match effect {
            SessionEffect::PeerOpenRequested { request, flow, .. } => Some((request, flow)),
            _ => None,
        })
        .unwrap();
    assert_eq!(local_flow.direction(), Direction::TargetToClient);
    supervisor
        .apply_event(SessionEvent::PeerOpenResolved {
            request,
            result: OpenResultCode::Opened,
        })
        .unwrap();
    let (flow, driver) = factory.open_flow(local_flow).unwrap();
    OwnerSourceFixture {
        supervisor,
        factory,
        leg,
        local_flow,
        flow,
        driver,
    }
}

fn pending_owner_sink(source: &mut OwnerSourceFixture) -> (SinkOffer, SinkDelivery) {
    let effects = source
        .supervisor
        .apply_event(SessionEvent::PeerFrame {
            leg: source.leg,
            frame: Frame::try_new(
                source.leg.generation(),
                Record::Data {
                    flow_id: source.local_flow.flow_id(),
                    direction: Direction::ClientToTarget,
                    offset: ByteOffset::new(0),
                    payload: Bytes::from_static(b"sink"),
                },
            )
            .unwrap(),
        })
        .unwrap();
    let (offer, segments) = effects
        .into_iter()
        .find_map(|effect| match effect {
            SessionEffect::OfferToSink { offer, segments } => Some((offer, segments)),
            _ => None,
        })
        .unwrap();
    source.driver.try_deliver_sink(offer, segments).unwrap();
    let TunAction::Sink(delivery) = source.flow.try_recv_action().unwrap().unwrap() else {
        panic!("the exact Owner sink offer must reach its existing delivery port");
    };
    (offer, delivery)
}

#[test]
fn owner_source_data_and_close_keep_exact_permits_until_application_ack() {
    let OwnerSourceFixture {
        mut supervisor,
        factory,
        leg,
        local_flow,
        flow,
        mut driver,
    } = owner_source();
    let probe = flow.probe();
    flow.try_send_uplink_with(4, || Bytes::from_static(b"one!"))
        .unwrap();
    flow.try_send_uplink_with(4, || Bytes::from_static(b"two!"))
        .unwrap();
    assert_eq!(probe.snapshot().uplink_owned_bytes(), 8);
    assert_eq!(probe.snapshot().uplink_owned_segments(), 2);
    assert_eq!(factory.session_uplink_owned_bytes(), 8);
    assert_eq!(factory.session_uplink_owned_segments(), 2);

    for (index, payload) in [b"one!", b"two!"].into_iter().enumerate() {
        let input = driver.try_recv_next().unwrap().unwrap();
        let DriverInput::Data(data) = &input else {
            panic!("both admitted DATA extents must precede CLOSE");
        };
        assert_eq!(data.flow(), local_flow);
        assert_eq!(data.payload(), payload);
        if index == 0 {
            // The first dequeue frees the existing source slot. CLOSE joins
            // that FIFO behind the second DATA, without another byte permit.
            flow.try_send_close().unwrap();
        }
        let command = SessionOwnerCommand::Driver(input);
        assert!(command.is_ordered_source());
        let effects = supervisor.apply_command(command).unwrap();
        // Replay normalizes backing storage. Its receipt binds the exact
        // flow/range/content and permits, not the source allocation address.
        assert!(effects.iter().any(|effect| matches!(
            effect,
            SessionEffect::Transmit(frame)
                if matches!(frame.record(), Record::Data {
                    flow_id, direction: Direction::TargetToClient, offset, payload: actual,
                } if *flow_id == local_flow.flow_id()
                    && *offset == ByteOffset::new((index * 4) as u64)
                    && actual.as_ref() == payload)
        )));
        assert_eq!(probe.snapshot().uplink_owned_bytes(), 8);
        assert_eq!(probe.snapshot().uplink_owned_segments(), 2);
    }
    let close = driver.try_recv_next().unwrap().unwrap();
    assert!(matches!(&close,
        DriverInput::Control(SessionEvent::LocalClose { flow }) if *flow == local_flow
    ));
    let close = SessionOwnerCommand::Driver(close);
    assert!(close.is_ordered_source());
    let effects = supervisor.apply_command(close).unwrap();
    assert!(effects.iter().any(|effect| matches!(
        effect,
        SessionEffect::Transmit(frame)
            if matches!(frame.record(), Record::Close {
                flow_id, direction: Direction::TargetToClient, final_offset,
            } if *flow_id == local_flow.flow_id() && *final_offset == ByteOffset::new(8))
    )));

    for (accepted, bytes, segments) in [(2, 8, 2), (4, 4, 1), (8, 0, 0)] {
        supervisor
            .apply_event(SessionEvent::PeerFrame {
                leg,
                frame: Frame::try_new(
                    leg.generation(),
                    Record::Ack {
                        flow_id: local_flow.flow_id(),
                        direction: Direction::TargetToClient,
                        next_accepted: ByteOffset::new(accepted),
                        final_accepted: false,
                    },
                )
                .unwrap(),
            })
            .unwrap();
        assert_eq!(probe.snapshot().uplink_owned_bytes(), bytes);
        assert_eq!(probe.snapshot().uplink_owned_segments(), segments);
        assert_eq!(factory.session_uplink_owned_bytes(), bytes);
        assert_eq!(factory.session_uplink_owned_segments(), segments);
        assert_eq!(supervisor.snapshot().uplink_replay_owned_bytes, bytes);
        assert_eq!(supervisor.snapshot().uplink_replay_extents, segments);
    }
    // Only these source/replay permits have settled. The session, flow ports,
    // CLOSE disposition and other lifecycle authorities still exist.
}

#[test]
fn owner_driver_control_poll_leaves_ordered_data_for_its_own_turn() {
    let mut source = owner_source();
    let probe = source.flow.probe();
    source
        .flow
        .try_send_uplink_with(4, || Bytes::from_static(b"held"))
        .unwrap();
    let mut cx = Context::from_waker(Waker::noop());
    assert!(source.driver.poll_owner_control(&mut cx).is_pending());
    assert_eq!(probe.snapshot().uplink_owned_bytes(), 4);
    assert_eq!(probe.snapshot().uplink_owned_segments(), 1);

    source
        .flow
        .try_send_reset(ResetReason::TargetFailure)
        .unwrap();
    let Poll::Ready(Some(reset)) = source.driver.poll_owner_control(&mut cx) else {
        panic!("Control must receive RESET without dequeuing OrderedSource");
    };
    assert!(matches!(&reset,
        DriverInput::Control(SessionEvent::LocalReset {
            flow, reason: ResetReason::TargetFailure,
        }) if *flow == source.local_flow
    ));
    assert_eq!(probe.snapshot().uplink_owned_bytes(), 4);
    assert_eq!(probe.snapshot().uplink_owned_segments(), 1);

    let Poll::Ready(Some(input)) = source.driver.poll_owner_ordered_source(&mut cx) else {
        panic!("OrderedSource must still own the previously admitted DATA");
    };
    let DriverInput::Data(data) = &input else {
        panic!("the ordered lane must return DATA, not the retained RESET");
    };
    assert_eq!(data.flow(), source.local_flow);
    assert_eq!(data.payload(), b"held");
    assert_eq!(probe.snapshot().uplink_owned_bytes(), 4);
    assert_eq!(probe.snapshot().uplink_owned_segments(), 1);
    assert_eq!(source.factory.session_uplink_owned_bytes(), 4);
    assert_eq!(source.factory.session_uplink_owned_segments(), 1);
    // Both returned commands are retained here; RESET has not been reduced.
    // Dequeue is not an application ACK or a terminal settlement proof.
}

#[test]
fn owner_control_poll_returns_sink_completion_while_reset_is_pending() {
    let mut source = owner_source();
    source
        .flow
        .try_send_uplink_with(4, || Bytes::from_static(b"wait"))
        .unwrap();
    let (offer, delivery) = pending_owner_sink(&mut source);
    assert!(delivery.complete_write(4).unwrap().is_none());

    let mut cx = Context::from_waker(Waker::noop());
    let Poll::Ready(Some(input)) = source.driver.poll_owner_control(&mut cx) else {
        panic!("Pending RESET must not hide an already-owned sink completion");
    };
    assert!(matches!(&input,
        DriverInput::Control(SessionEvent::SinkAccepted { offer: observed, bytes: 4 })
            if *observed == offer
    ));
    assert_eq!(source.flow.probe().snapshot().uplink_owned_bytes(), 4);
    assert_eq!(source.factory.session_uplink_owned_segments(), 1);
}

#[test]
fn owner_control_poll_waits_for_reserved_completion_after_source_closes() {
    let mut source = owner_source();
    let (offer, delivery) = pending_owner_sink(&mut source);
    drop(source.flow);

    let mut cx = Context::from_waker(Waker::noop());
    assert!(source.driver.poll_owner_control(&mut cx).is_pending());
    delivery.abandon();
    let Poll::Ready(Some(input)) = source.driver.poll_owner_control(&mut cx) else {
        panic!("the retained completion permit must keep Control alive");
    };
    assert!(matches!(&input,
        DriverInput::Control(SessionEvent::SinkAbandoned { offer: observed })
            if *observed == offer
    ));
    assert!(matches!(
        source.driver.poll_owner_control(&mut cx),
        Poll::Ready(None)
    ));
    // The reserved fact has left the queues, but remains an unreduced command.
    // Channel drainage is not sink, flow, or session terminal settlement.
}

#[test]
fn owner_ordered_poll_drains_data_then_close_before_reporting_end() {
    let mut source = owner_source();
    let probe = source.flow.probe();
    source
        .flow
        .try_send_uplink_with(4, || Bytes::from_static(b"last"))
        .unwrap();
    source.flow.try_send_close().unwrap();
    drop(source.flow);

    let mut cx = Context::from_waker(Waker::noop());
    let Poll::Ready(Some(input)) = source.driver.poll_owner_ordered_source(&mut cx) else {
        panic!("closing the producer must not skip its retained DATA");
    };
    let DriverInput::Data(data) = &input else {
        panic!("DATA must precede the same FIFO's CLOSE");
    };
    assert_eq!(data.flow(), source.local_flow);
    assert_eq!(data.payload(), b"last");
    let Poll::Ready(Some(close)) = source.driver.poll_owner_ordered_source(&mut cx) else {
        panic!("CLOSE must leave the source FIFO before end-of-stream");
    };
    assert!(matches!(&close,
        DriverInput::Control(SessionEvent::LocalClose { flow }) if *flow == source.local_flow
    ));
    assert!(matches!(
        source.driver.poll_owner_ordered_source(&mut cx),
        Poll::Ready(None)
    ));
    assert_eq!(probe.snapshot().uplink_owned_bytes(), 4);
    assert_eq!(probe.snapshot().uplink_owned_segments(), 1);
    assert_eq!(source.factory.session_uplink_owned_bytes(), 4);
    assert_eq!(source.factory.session_uplink_owned_segments(), 1);
    // The drained receiver no longer owns DATA, but this exact command does.
    // Its byte/segment permits cannot disappear merely because the lane ended.
}

#[test]
fn owner_control_poll_reentry_replaces_cancelled_waiter_and_wakes_on_real_reset() {
    let mut source = owner_source();
    source
        .flow
        .try_send_uplink_with(4, || Bytes::from_static(b"held"))
        .unwrap();
    let cancelled = Arc::new(SourcePollWake::default());
    let cancelled_waker = Waker::from(Arc::clone(&cancelled));
    {
        let mut cx = Context::from_waker(&cancelled_waker);
        let mut wait = pin!(poll_fn(|cx| source.driver.poll_owner_control(cx)));
        assert!(wait.as_mut().poll(&mut cx).is_pending());
    }

    let resumed = Arc::new(SourcePollWake::default());
    let resumed_waker = Waker::from(Arc::clone(&resumed));
    let mut cx = Context::from_waker(&resumed_waker);
    let mut wait = pin!(poll_fn(|cx| source.driver.poll_owner_control(cx)));
    assert!(wait.as_mut().poll(&mut cx).is_pending());
    source
        .flow
        .try_send_reset(ResetReason::TargetFailure)
        .unwrap();
    assert_eq!(cancelled.0.load(Ordering::SeqCst), 0);
    assert_eq!(resumed.0.load(Ordering::SeqCst), 1);
    let Poll::Ready(Some(input)) = wait.as_mut().poll(&mut cx) else {
        panic!("the new borrowed waiter must receive the real RESET exactly once");
    };
    assert!(matches!(&input,
        DriverInput::Control(SessionEvent::LocalReset {
            flow, reason: ResetReason::TargetFailure,
        }) if *flow == source.local_flow
    ));
    assert_eq!(source.flow.probe().snapshot().uplink_owned_bytes(), 4);
    assert_eq!(source.factory.session_uplink_owned_segments(), 1);
}

#[test]
fn owner_ordered_poll_reentry_replaces_cancelled_waiter_and_wakes_on_real_data() {
    let mut source = owner_source();
    let cancelled = Arc::new(SourcePollWake::default());
    let cancelled_waker = Waker::from(Arc::clone(&cancelled));
    {
        let mut cx = Context::from_waker(&cancelled_waker);
        let mut wait = pin!(poll_fn(|cx| source.driver.poll_owner_ordered_source(cx)));
        assert!(wait.as_mut().poll(&mut cx).is_pending());
    }

    let resumed = Arc::new(SourcePollWake::default());
    let resumed_waker = Waker::from(Arc::clone(&resumed));
    let mut cx = Context::from_waker(&resumed_waker);
    let mut wait = pin!(poll_fn(|cx| source.driver.poll_owner_ordered_source(cx)));
    assert!(wait.as_mut().poll(&mut cx).is_pending());
    source
        .flow
        .try_send_uplink_with(4, || Bytes::from_static(b"wake"))
        .unwrap();
    assert_eq!(cancelled.0.load(Ordering::SeqCst), 0);
    assert_eq!(resumed.0.load(Ordering::SeqCst), 1);
    let Poll::Ready(Some(input)) = wait.as_mut().poll(&mut cx) else {
        panic!("the resumed ordered waiter must receive the admitted DATA");
    };
    let DriverInput::Data(data) = &input else {
        panic!("the real source wake must not substitute a Control command");
    };
    assert_eq!(data.flow(), source.local_flow);
    assert_eq!(data.payload(), b"wake");
    assert_eq!(source.flow.probe().snapshot().uplink_owned_bytes(), 4);
    assert_eq!(source.flow.probe().snapshot().uplink_owned_segments(), 1);
    assert_eq!(source.factory.session_uplink_owned_bytes(), 4);
    assert_eq!(source.factory.session_uplink_owned_segments(), 1);
}

#[test]
fn owner_control_poll_returns_reset_before_ready_sink_completion() {
    let mut source = owner_source();
    let (offer, delivery) = pending_owner_sink(&mut source);
    assert!(delivery.complete_write(4).unwrap().is_none());
    source
        .flow
        .try_send_reset(ResetReason::TargetFailure)
        .unwrap();

    let mut cx = Context::from_waker(Waker::noop());
    let Poll::Ready(Some(reset)) = source.driver.poll_owner_control(&mut cx) else {
        panic!("a ready RESET must win the Control selection");
    };
    assert!(matches!(&reset,
        DriverInput::Control(SessionEvent::LocalReset {
            flow, reason: ResetReason::TargetFailure,
        }) if *flow == source.local_flow
    ));
    let Poll::Ready(Some(completion)) = source.driver.poll_owner_control(&mut cx) else {
        panic!("the other ready Control fact must remain for its own turn");
    };
    assert!(matches!(&completion,
        DriverInput::Control(SessionEvent::SinkAccepted { offer: observed, bytes: 4 })
            if *observed == offer
    ));
    assert!(source.driver.poll_owner_control(&mut cx).is_pending());
}

#[test]
fn owner_lane_polls_are_woken_by_actual_producer_close() {
    let mut source = owner_source();
    let control = Arc::new(SourcePollWake::default());
    let ordered = Arc::new(SourcePollWake::default());
    let control_waker = Waker::from(Arc::clone(&control));
    let ordered_waker = Waker::from(Arc::clone(&ordered));
    let mut control_cx = Context::from_waker(&control_waker);
    let mut ordered_cx = Context::from_waker(&ordered_waker);
    assert!(
        source
            .driver
            .poll_owner_control(&mut control_cx)
            .is_pending()
    );
    assert!(
        source
            .driver
            .poll_owner_ordered_source(&mut ordered_cx)
            .is_pending()
    );
    assert_eq!(control.0.load(Ordering::SeqCst), 0);
    assert_eq!(ordered.0.load(Ordering::SeqCst), 0);

    drop(source.flow);
    assert!(control.0.load(Ordering::SeqCst) > 0);
    assert!(ordered.0.load(Ordering::SeqCst) > 0);
    assert!(matches!(
        source.driver.poll_owner_control(&mut control_cx),
        Poll::Ready(None)
    ));
    assert!(matches!(
        source.driver.poll_owner_ordered_source(&mut ordered_cx),
        Poll::Ready(None)
    ));
}
