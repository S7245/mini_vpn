//! Real mailbox progress checks; no raw sender or synthetic command transport.

use super::*;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Wake;
use std::task::{Context, Poll, Waker};

struct PanicCapacityWake;

impl Wake for PanicCapacityWake {
    fn wake(self: Arc<Self>) {
        panic!("real capacity observer wake interrupted");
    }
}

#[derive(Default)]
struct CountCapacityWake(AtomicUsize);

impl Wake for CountCapacityWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

const _: fn() = || {
    let _ = <SessionMailboxCapacityObservation as AmbiguousIfClone<_>>::marker;
};

#[tokio::test]
async fn capacity_wake_unwind_retains_exact_prefetch_before_another_dequeue() {
    let (ingress, mut inbox) = SessionOwnerMailbox::bounded(1, 1).unwrap();
    let first = SessionOwnerCommand::Event(peer_data(b"prefetched-first"));
    let mut observation = ingress.observe_capacity().unwrap();
    ingress.try_send_command(first).unwrap();
    let mut changed = std::pin::pin!(observation.changed());
    let panic_waker = Waker::from(Arc::new(PanicCapacityWake));
    assert!(
        changed
            .as_mut()
            .poll(&mut Context::from_waker(&panic_waker))
            .is_pending()
    );
    let mut cx = Context::from_waker(Waker::noop());
    let interrupted = catch_unwind(AssertUnwindSafe(|| inbox.poll_next(&mut cx)));
    assert!(
        interrupted.is_err(),
        "the real registered capacity wake must run"
    );
    assert!(changed.as_mut().poll(&mut cx).is_ready());
    // Real freed channel capacity may accept another command while the first
    // still belongs to inbox prefetch. Re-entry must not pop over the first.
    ingress.try_send_event(peer_data(b"queued-second")).unwrap();
    let Poll::Ready(Some(SessionOwnerCommand::Event(first))) = inbox.poll_next(&mut cx) else {
        panic!("interrupted receive must preserve the exact first command");
    };
    assert_eq!(first, peer_data(b"prefetched-first"));
    let Poll::Ready(Some(SessionOwnerCommand::Event(second))) = inbox.poll_next(&mut cx) else {
        panic!("the next command must remain independently queued");
    };
    assert_eq!(second, peer_data(b"queued-second"));
    assert!(inbox.poll_next(&mut cx).is_pending());
    inbox.close();
    assert!(matches!(inbox.poll_next(&mut cx), Poll::Ready(None)));
}

#[tokio::test]
async fn direct_mailbox_poll_preserves_priority_and_half_closed_drain() {
    let (ingress, mut inbox) = SessionOwnerMailbox::bounded(1, 1).unwrap();
    let mut cx = Context::from_waker(Waker::noop());
    assert!(inbox.poll_next(&mut cx).is_pending());
    ingress.try_send_event(peer_data(b"ordered")).unwrap();
    ingress.try_send_event(peer_ack()).unwrap();
    let Poll::Ready(Some(SessionOwnerCommand::Event(event))) = inbox.poll_next(&mut cx) else {
        panic!("ready control must be selected before ordered source");
    };
    assert_eq!(event, peer_ack());
    // One closed lane cannot hide already-owned or future work on the other.
    inbox.control_rx.close();
    let Poll::Ready(Some(SessionOwnerCommand::Event(event))) = inbox.poll_next(&mut cx) else {
        panic!("the open ordered lane must drain its actual command");
    };
    assert_eq!(event, peer_data(b"ordered"));
    assert!(inbox.poll_next(&mut cx).is_pending());
    ingress
        .try_send_event(peer_data(b"after-control-close"))
        .unwrap();
    let Poll::Ready(Some(SessionOwnerCommand::Event(event))) = inbox.poll_next(&mut cx) else {
        panic!("the surviving lane must remain usable");
    };
    assert_eq!(event, peer_data(b"after-control-close"));
    inbox.close();
    assert!(matches!(inbox.poll_next(&mut cx), Poll::Ready(None)));
}

#[tokio::test]
async fn capacity_progress_reports_both_lanes_changed_before_registration() {
    let (ingress, mut inbox) = SessionOwnerMailbox::bounded(1, 1).unwrap();
    let mut observation = ingress.observe_capacity().unwrap();
    ingress.try_send_event(peer_ack()).unwrap();
    ingress.try_send_event(peer_data(b"both")).unwrap();
    let mut cx = Context::from_waker(Waker::noop());
    assert!(inbox.poll_next(&mut cx).is_ready());
    assert!(inbox.poll_next(&mut cx).is_ready());
    assert_eq!(
        observation.changed().await,
        SessionMailboxCapacityChange::Both
    );
    // No lane changed after the exact reported pair. Full alone or a new wait
    // registration is not another reason to run a send.
    let mut changed = std::pin::pin!(observation.changed());
    for _ in 0..10 {
        assert!(changed.as_mut().poll(&mut cx).is_pending());
    }
}

#[tokio::test]
async fn capacity_observer_cancellation_preserves_epoch_and_exclusive_lease() {
    let (ingress, mut inbox) = SessionOwnerMailbox::bounded(1, 1).unwrap();
    let mut observation = ingress.observe_capacity().unwrap();
    let cloned = ingress.clone();
    assert!(matches!(
        cloned.observe_capacity(),
        Err(SessionMailboxObserverBusy)
    ));
    let count = Arc::new(CountCapacityWake::default());
    let waker = Waker::from(Arc::clone(&count));
    let mut cx = Context::from_waker(&waker);
    {
        let mut changed = std::pin::pin!(observation.changed());
        assert!(changed.as_mut().poll(&mut cx).is_pending());
    }
    assert!(matches!(
        cloned.observe_capacity(),
        Err(SessionMailboxObserverBusy)
    ));
    ingress.try_send_event(peer_ack()).unwrap();
    assert!(
        inbox
            .poll_next(&mut Context::from_waker(Waker::noop()))
            .is_ready()
    );
    assert_eq!(
        count.0.load(Ordering::SeqCst),
        1,
        "the owning observer retains registration after borrow cancellation"
    );
    assert_eq!(
        observation.changed().await,
        SessionMailboxCapacityChange::Control
    );
    ingress.try_send_event(peer_data(b"registered")).unwrap();
    {
        let mut changed = std::pin::pin!(observation.changed());
        assert!(changed.as_mut().poll(&mut cx).is_pending());
        assert!(
            inbox
                .poll_next(&mut Context::from_waker(Waker::noop()))
                .is_ready()
        );
        assert_eq!(count.0.load(Ordering::SeqCst), 2);
        assert_eq!(
            changed.as_mut().poll(&mut cx),
            Poll::Ready(SessionMailboxCapacityChange::OrderedSource)
        );
    }
    drop(observation);
    let mut replacement = cloned.observe_capacity().unwrap();
    let mut changed = std::pin::pin!(replacement.changed());
    assert!(changed.as_mut().poll(&mut cx).is_pending());
}

#[tokio::test]
async fn close_commits_both_lanes_before_wake_unwind_without_discarding_queue() {
    let (ingress, mut inbox) = SessionOwnerMailbox::bounded(1, 1).unwrap();
    let mut observation = ingress.observe_capacity().unwrap();
    ingress.try_send_event(peer_ack()).unwrap();
    ingress.try_send_event(peer_data(b"close-held")).unwrap();
    let mut changed = std::pin::pin!(observation.changed());
    let waker = Waker::from(Arc::new(PanicCapacityWake));
    assert!(
        changed
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    assert!(catch_unwind(AssertUnwindSafe(|| inbox.close())).is_err());
    assert!(inbox.control_rx.is_closed());
    assert!(inbox.data_rx.is_closed());
    assert_eq!(inbox.control_rx.len(), 1);
    assert_eq!(inbox.data_rx.len(), 1);
    for (event, expected) in [
        (peer_ack(), peer_ack()),
        (peer_data(b"rejected"), peer_data(b"rejected")),
    ] {
        let Err(SessionMailboxTrySendError::Closed(returned)) = ingress.try_send_event(event)
        else {
            panic!("both lanes must reject new admission after interrupted close");
        };
        let SessionOwnerCommand::Event(returned) = *returned else {
            panic!("rejection must preserve the event variant");
        };
        assert_eq!(returned, expected);
    }
    let mut cx = Context::from_waker(Waker::noop());
    assert_eq!(
        changed.as_mut().poll(&mut cx),
        Poll::Ready(SessionMailboxCapacityChange::Closed)
    );
    assert_eq!(recv_event(&mut inbox).await, Some(peer_ack()));
    assert_eq!(recv_event(&mut inbox).await, Some(peer_data(b"close-held")));
    assert!(matches!(inbox.poll_next(&mut cx), Poll::Ready(None)));
}

#[tokio::test]
async fn inbox_destruction_terminates_the_surviving_observer_without_zero_claim() {
    for unwind in [false, true] {
        let (ingress, inbox) = SessionOwnerMailbox::bounded(1, 1).unwrap();
        let mut observation = ingress.observe_capacity().unwrap();
        ingress
            .try_send_event(peer_data(b"not-settled-by-drop"))
            .unwrap();
        let count = Arc::new(CountCapacityWake::default());
        let waker = Waker::from(Arc::clone(&count));
        let mut cx = Context::from_waker(&waker);
        let mut changed = std::pin::pin!(observation.changed());
        assert!(changed.as_mut().poll(&mut cx).is_pending());
        if unwind {
            let result = catch_unwind(AssertUnwindSafe(move || {
                let _owned_inbox = inbox;
                panic!("ordinary caller unwind drops its inbox");
            }));
            assert!(result.is_err());
        } else {
            drop(inbox);
        }
        assert_eq!(count.0.load(Ordering::SeqCst), 1);
        assert_eq!(
            changed.as_mut().poll(&mut cx),
            Poll::Ready(SessionMailboxCapacityChange::Closed)
        );
        let Err(SessionMailboxTrySendError::Closed(returned)) = ingress.try_send_event(peer_ack())
        else {
            panic!("a surviving ingress cannot reopen a destroyed inbox");
        };
        let SessionOwnerCommand::Event(event) = *returned else {
            panic!("the rejected command must be returned intact");
        };
        assert_eq!(event, peer_ack());
    }
}

#[tokio::test]
async fn capacity_epoch_accepts_max_then_exhausts_sticky_without_wrapping() {
    let (ingress, mut inbox) = SessionOwnerMailbox::bounded(1, 1).unwrap();
    // Boundary fixture only; production epochs are advanced by real dequeue.
    inbox.progress.epochs[0].store(u64::MAX - 1, Ordering::Release);
    let mut observation = ingress.observe_capacity().unwrap();
    ingress.try_send_event(peer_ack()).unwrap();
    assert_eq!(recv_event(&mut inbox).await, Some(peer_ack()));
    assert_eq!(
        observation.changed().await,
        SessionMailboxCapacityChange::Control
    );
    assert_eq!(inbox.progress.epochs(), [u64::MAX, 0]);
    ingress.try_send_event(peer_ack()).unwrap();
    ingress
        .try_send_event(peer_data(b"must-still-drain"))
        .unwrap();
    let mut changed = std::pin::pin!(observation.changed());
    let waker = Waker::from(Arc::new(PanicCapacityWake));
    assert!(
        changed
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    let mut cx = Context::from_waker(Waker::noop());
    assert!(catch_unwind(AssertUnwindSafe(|| inbox.poll_next(&mut cx))).is_err());
    assert_eq!(inbox.progress.epochs(), [u64::MAX, 0]);
    assert!(inbox.control_rx.is_closed() && inbox.data_rx.is_closed());
    assert_eq!(
        changed.as_mut().poll(&mut cx),
        Poll::Ready(SessionMailboxCapacityChange::EpochExhausted)
    );
    assert_eq!(recv_event(&mut inbox).await, Some(peer_ack()));
    assert_eq!(
        recv_event(&mut inbox).await,
        Some(peer_data(b"must-still-drain"))
    );
    inbox.close();
    assert_eq!(
        inbox.progress.terminal.load(Ordering::Acquire),
        MAILBOX_EPOCH_EXHAUSTED
    );
    assert_eq!(inbox.progress.epochs(), [u64::MAX, 0]);
    assert!(matches!(
        ingress.try_send_event(peer_ack()),
        Err(SessionMailboxTrySendError::Closed(_))
    ));
    assert!(matches!(inbox.poll_next(&mut cx), Poll::Ready(None)));
}

#[tokio::test]
async fn full_return_retries_only_after_actual_capacity_before_or_after_registration() {
    for progress_before_registration in [true, false] {
        let (ingress, mut inbox) = SessionOwnerMailbox::bounded(1, 1).unwrap();
        ingress.try_send_event(peer_ack()).unwrap();
        let mut observation = ingress.observe_capacity().unwrap();
        let Err(SessionMailboxTrySendError::Full(held)) = ingress.try_send_event(peer_reset())
        else {
            panic!("the actual full control lane must return the exact command");
        };
        let address = std::ptr::from_ref(held.as_ref());
        let count = Arc::new(CountCapacityWake::default());
        let waker = Waker::from(Arc::clone(&count));
        let mut cx = Context::from_waker(&waker);
        if progress_before_registration {
            assert_eq!(recv_event(&mut inbox).await, Some(peer_ack()));
        }
        {
            let mut changed = std::pin::pin!(observation.changed());
            if !progress_before_registration {
                for _ in 0..10 {
                    assert!(changed.as_mut().poll(&mut cx).is_pending());
                    assert_eq!(std::ptr::from_ref(held.as_ref()), address);
                    assert_eq!(inbox.control_rx.len(), 1);
                }
                assert_eq!(count.0.load(Ordering::SeqCst), 0);
                assert_eq!(recv_event(&mut inbox).await, Some(peer_ack()));
                assert_eq!(count.0.load(Ordering::SeqCst), 1);
            }
            assert_eq!(
                changed.as_mut().poll(&mut cx),
                Poll::Ready(SessionMailboxCapacityChange::Control)
            );
        }
        assert_eq!(std::ptr::from_ref(held.as_ref()), address);
        ingress.try_send_command(*held).unwrap();
        assert_eq!(recv_event(&mut inbox).await, Some(peer_reset()));
        assert!(inbox.poll_next(&mut cx).is_pending());
    }
}

#[tokio::test]
async fn nonclone_driver_data_survives_capacity_wake_unwind_with_exact_byte_ownership() {
    use crate::owned_upstream::supervisor::SessionSupervisor;
    use crate::owned_upstream::tcp::FlowPortConfig;
    let leg = committed_leg();
    let mut supervisor =
        SessionSupervisor::new(SessionModel::new(SessionRole::Client, config(), leg));
    let factory = supervisor
        .mint_tcp_port_factory(FlowPortConfig::new(8, 2, 2, 2).unwrap())
        .unwrap();
    let effects = supervisor
        .apply_event(SessionEvent::LocalOpen {
            leg,
            target: TargetAddr::IpPort(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 443))),
        })
        .unwrap();
    let local_flow = effects
        .iter()
        .find_map(|effect| match effect {
            SessionEffect::LocalFlowOpened { flow } => Some(*flow),
            _ => None,
        })
        .unwrap();
    supervisor
        .apply_event(SessionEvent::PeerFrame {
            leg,
            frame: Frame::try_new(
                leg.generation(),
                Record::OpenResult {
                    flow_id: local_flow.flow_id(),
                    result: crate::resumable::OpenResultCode::Opened,
                },
            )
            .unwrap(),
        })
        .unwrap();
    let (flow, mut driver) = factory.open_flow(local_flow).unwrap();
    let probe = flow.probe();
    flow.try_send_uplink_with(4, || Bytes::from_static(b"live"))
        .unwrap();
    let input = driver.try_recv_next().unwrap().unwrap();
    let DriverInput::Data(data) = &input else {
        panic!("fixture must own real admitted DATA");
    };
    let address = data.payload().as_ptr();
    let (ingress, mut inbox) = SessionOwnerMailbox::bounded(1, 1).unwrap();
    let mut observation = ingress.observe_capacity().unwrap();
    ingress.try_send_driver(input).unwrap();
    let mut changed = std::pin::pin!(observation.changed());
    let waker = Waker::from(Arc::new(PanicCapacityWake));
    assert!(
        changed
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    let mut cx = Context::from_waker(Waker::noop());
    assert!(catch_unwind(AssertUnwindSafe(|| inbox.poll_next(&mut cx))).is_err());
    assert_eq!(probe.snapshot().uplink_owned_bytes(), 4);
    assert_eq!(
        changed.as_mut().poll(&mut cx),
        Poll::Ready(SessionMailboxCapacityChange::OrderedSource)
    );
    flow.try_send_uplink_with(4, || Bytes::from_static(b"tail"))
        .unwrap();
    ingress
        .try_send_driver(driver.try_recv_next().unwrap().unwrap())
        .unwrap();
    assert_eq!(probe.snapshot().uplink_owned_bytes(), 8);
    for (payload, replay_bytes) in [(b"live", 4), (b"tail", 8)] {
        let command = inbox.recv_next().await.unwrap();
        let SessionOwnerCommand::Driver(DriverInput::Data(data)) = &command else {
            panic!("receive must retain the original non-Clone driver command");
        };
        assert_eq!(data.flow(), local_flow);
        assert_eq!(data.payload(), payload);
        if replay_bytes == 4 {
            assert_eq!(data.payload().as_ptr(), address);
        }
        supervisor.apply_command(command).unwrap();
        assert_eq!(probe.snapshot().uplink_owned_bytes(), 8);
        assert_eq!(
            supervisor.snapshot().uplink_replay_owned_bytes,
            replay_bytes
        );
    }
    assert!(inbox.poll_next(&mut cx).is_pending());
}

#[tokio::test]
async fn drained_sender_closure_publishes_terminal_before_notify_unwind() {
    let (ingress, mut inbox) = SessionOwnerMailbox::bounded(1, 1).unwrap();
    let mut observation = ingress.observe_capacity().unwrap();
    ingress.try_send_event(peer_ack()).unwrap();
    ingress
        .try_send_event(peer_data(b"before-sender-exit"))
        .unwrap();
    drop(ingress);
    assert_eq!(recv_event(&mut inbox).await, Some(peer_ack()));
    assert_eq!(
        recv_event(&mut inbox).await,
        Some(peer_data(b"before-sender-exit"))
    );
    assert_eq!(
        observation.changed().await,
        SessionMailboxCapacityChange::Both
    );
    let mut changed = std::pin::pin!(observation.changed());
    let waker = Waker::from(Arc::new(PanicCapacityWake));
    assert!(
        changed
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    let mut cx = Context::from_waker(Waker::noop());
    let terminal = catch_unwind(AssertUnwindSafe(|| inbox.poll_next(&mut cx)));
    assert!(
        terminal.is_err(),
        "observed dual-lane termination must notify before returning None"
    );
    assert_eq!(
        changed.as_mut().poll(&mut cx),
        Poll::Ready(SessionMailboxCapacityChange::Closed)
    );
    assert_eq!(
        inbox.progress.terminal.load(Ordering::Acquire),
        MAILBOX_CLOSED
    );
    // Keep the same inbox alive. Its later Drop cannot repair this observation.
    for _ in 0..3 {
        assert!(matches!(inbox.poll_next(&mut cx), Poll::Ready(None)));
    }
}
