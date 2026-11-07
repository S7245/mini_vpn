use super::*;
use bytes::BackingBudget;
use std::{future::Future, task::Waker};

#[test]
fn terminal_ready_preserves_bits_before_and_after_wait_registration() {
    let ready = TerminalReady::default();
    ready.mark(18);
    let mut wait = Box::pin(ready.notify.clone().notified_owned());
    let mut cx = Context::from_waker(Waker::noop());
    assert!(wait.as_mut().poll(&mut cx).is_ready());
    assert_eq!(ready.take(), Some(18));
    wait.set(ready.notify.clone().notified_owned());
    assert!(wait.as_mut().poll(&mut cx).is_pending());
    ready.mark(0);
    ready.mark(18);
    assert!(wait.as_mut().poll(&mut cx).is_ready());
    assert_eq!(ready.take(), Some(0));
    assert_eq!(ready.take(), Some(18));
    assert_eq!(ready.take(), None);
}

#[test]
fn synchronous_control_owner_loss_is_quarantined_without_deferred_success() {
    let budget = BackingBudget::new(1024).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (tx, mut rx) = mpsc::unbounded_channel();
    let sender =
        EndpointEventSender::new(tx, Some(std::sync::Weak::new()), Some(meter.clone()), None);
    assert!(sender
        .send((
            proto::ConnectionHandle(0),
            crate::ConnectionToEndpointEvent::AuthenticatedCurrentSocketRx(None, 1)
        ))
        .is_err());
    assert!(meter.snapshot().quarantined);
    assert!(rx.try_recv().is_err());
}

#[test]
fn terminal_event_keeps_deferred_removal_witness_without_endpoint_lock() {
    let budget = BackingBudget::new(1024).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (tx, mut rx) = mpsc::unbounded_channel();
    let (terminal, mut receiver) = terminal_channel(0, &meter).unwrap();
    assert_eq!(
        budget.used(),
        proto::owned_events::slot_layout::<crate::ConnectionToEndpointEvent>(1)
            .unwrap()
            .size()
    );
    let sender = EndpointEventSender::new(
        tx,
        Some(std::sync::Weak::new()),
        Some(meter.clone()),
        Some(terminal),
    );
    let (ack, mut receipt) = tokio::sync::oneshot::channel();
    assert!(matches!(
        sender.send((
            proto::ConnectionHandle(0),
            crate::ConnectionToEndpointEvent::Proto {
                event: proto::EndpointEvent::drained(),
                endpoint_released: Some(ack)
            }
        )),
        Ok(None)
    ));
    assert!(receipt.try_recv().is_err());
    assert!(
        rx.try_recv().is_err(),
        "budgeted terminal must not allocate an mpsc node"
    );
    let mut cx = Context::from_waker(Waker::noop());
    let Poll::Ready(Some(crate::ConnectionToEndpointEvent::Proto {
        endpoint_released: Some(ack),
        ..
    })) = receiver.poll_recv(&mut cx)
    else {
        panic!("original terminal witness missing")
    };
    ack.send(()).unwrap();
    assert_eq!(receipt.try_recv(), Ok(()));
    assert!(!meter.snapshot().quarantined);
    drop(sender);
    drop(receiver);
    assert_eq!(budget.used(), 0);
}

#[test]
fn terminal_receiver_loss_fails_ack_and_quarantines() {
    let budget = BackingBudget::new(1024).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (terminal, receiver) = terminal_channel(0, &meter).unwrap();
    let (tx, _) = mpsc::unbounded_channel();
    let sender = EndpointEventSender::new(
        tx,
        Some(std::sync::Weak::new()),
        Some(meter.clone()),
        Some(terminal),
    );
    drop(receiver);
    let (ack, mut receipt) = tokio::sync::oneshot::channel();
    assert!(sender
        .send((
            proto::ConnectionHandle(0),
            crate::ConnectionToEndpointEvent::Proto {
                event: proto::EndpointEvent::drained(),
                endpoint_released: Some(ack)
            }
        ))
        .is_err());
    assert_eq!(
        receipt.try_recv(),
        Err(tokio::sync::oneshot::error::TryRecvError::Closed)
    );
    assert!(meter.snapshot().quarantined);
}

#[cfg(any(feature = "rustls-aws-lc-rs", feature = "rustls-ring"))]
mod registration;

#[test]
fn unpublished_native_source_cannot_fall_back_to_legacy_deferred_delivery() {
    let budget = BackingBudget::new(1024).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut sender = EndpointEventSender::new(tx, None, Some(meter), None);
    sender.prepare_registration();
    assert!(sender
        .send((
            proto::ConnectionHandle(0),
            crate::ConnectionToEndpointEvent::AuthenticatedCurrentSocketRx(None, 1)
        ))
        .is_err());
    assert!(rx.try_recv().is_err());
    assert!(budget.quarantined());
    drop(sender);
    assert_eq!(budget.used(), 1);
}

#[test]
fn control_adapter_rejects_foreign_lane_and_wrong_kind_without_delivery() {
    use super::super::*;
    let budget = bytes::BackingBudget::new(65536).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let (a, mut ar) = forward_channel(Some(&meter), 2).unwrap();
    let (b, mut br) = forward_channel(Some(&meter), 2).unwrap();
    let close = || crate::ConnectionEvent::Close {
        error_code: 1u32.into(),
        reason: bytes::Bytes::new(),
    };
    let ticket = a.reserve_control(ControlKind::Close).unwrap();
    assert!(b.send_control(ticket, close()).is_err());
    let wrong_kind = a.reserve_control(ControlKind::Rebind).unwrap();
    assert!(a.send_control(wrong_kind, close()).is_err());
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(ar.poll_recv(&mut cx).is_pending());
    assert!(br.poll_recv(&mut cx).is_pending());
    a.reserve_control(ControlKind::Close).unwrap();
    a.reserve_control(ControlKind::Rebind).unwrap();
    drop((a, b, ar, br));
    assert!(!budget.quarantined());
    assert_eq!(budget.used(), 0);
}
