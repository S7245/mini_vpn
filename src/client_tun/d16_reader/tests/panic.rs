//! Original reader panic remains a joined Root remainder, never clean EOF.
use super::*;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

struct PanicWake;

impl Wake for PanicWake {
    fn wake(self: Arc<Self>) {
        std::panic::panic_any(std::io::Error::from_raw_os_error(72));
    }

    fn wake_by_ref(self: &Arc<Self>) {
        std::panic::panic_any(std::io::Error::from_raw_os_error(72));
    }
}

struct GatedErrorReader(tokio::sync::oneshot::Receiver<()>);

impl crate::upstream::NativeTcpReader for GatedErrorReader {
    fn poll_read_chunk(
        &mut self,
        cx: &mut Context<'_>,
        _: usize,
    ) -> Poll<std::io::Result<Option<NativeTcpChunk>>> {
        match Pin::new(&mut self.0).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(_) => Poll::Ready(Err(std::io::Error::from_raw_os_error(54))),
        }
    }
}

struct GatedPanicReader(tokio::sync::oneshot::Receiver<()>);

impl crate::upstream::NativeTcpReader for GatedPanicReader {
    fn poll_read_chunk(
        &mut self,
        cx: &mut Context<'_>,
        _: usize,
    ) -> Poll<std::io::Result<Option<NativeTcpChunk>>> {
        match Pin::new(&mut self.0).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(_) => std::panic::panic_any(std::io::Error::from_raw_os_error(71)),
        }
    }
}

fn full_reservation_queue() -> (AsyncLeasedByteFlowQueue, D16GlobalByteBudget) {
    let budget = D16GlobalByteBudget::new(8).unwrap();
    let queue = AsyncLeasedByteFlowQueue::new_with_global_budget(
        8,
        DownstreamPermitReleaseMode::OnEgressDrain,
        budget.clone(),
    )
    .unwrap();
    (queue, budget)
}

struct PanicReader;

impl crate::upstream::NativeTcpReader for PanicReader {
    fn poll_read_chunk(
        &mut self,
        _: &mut std::task::Context<'_>,
        _: usize,
    ) -> std::task::Poll<std::io::Result<Option<NativeTcpChunk>>> {
        std::panic::panic_any(std::io::Error::from_raw_os_error(71))
    }
}

#[tokio::test]
async fn original_poll_panic_returns_joined_window_and_original_payload() {
    let (queue, budget) = queue();
    let clock = std::sync::Arc::new(crate::shared::errors::FailureOrder::default());
    let (_issuer, accounting) = paired_accounting(clock);
    let (_credit, credit_rx) = watch::channel(RelayReadCredit {
        paused: false,
        max_batch_bytes: 8,
    });
    let (back_tx, _back_rx) = crate::client_tun::relay_channel::channel(1);
    let (_stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    let joined = tokio::spawn(run_d16_native_reader(
        handle(),
        7,
        Box::new(PanicReader),
        queue.clone(),
        credit_rx,
        back_tx,
        None,
        stop_rx,
        Some(accounting),
    ))
    .await;
    assert!(
        joined.is_ok(),
        "Root must receive the original panic remainder"
    );
    let mut outcome = joined.unwrap();
    assert_eq!(outcome.reason, ("root", "reader_panicked"));
    assert_eq!(outcome.panic_order, Some(0));
    assert_eq!(
        outcome
            .body_panic
            .as_ref()
            .and_then(|payload| payload.downcast_ref::<std::io::Error>())
            .and_then(std::io::Error::raw_os_error),
        Some(71)
    );
    assert_eq!(outcome.window.as_ref().map(ReaderWindow::read), Some(0));
    assert!(outcome.remainder.is_some());
    assert_eq!(queue.snapshot_now().reserved_bytes, 8);
    assert!(outcome.remainder.as_ref().unwrap().reservation.is_some());
    drop(outcome.remainder.as_mut().unwrap().reservation.take());
    assert_eq!(queue.snapshot_now().reserved_bytes, 0);
    outcome
        .remainder
        .as_ref()
        .unwrap()
        .queue
        .close_for_terminal_and_mark_ready("root", "reader_panicked")
        .await;
    assert!(queue.d16_terminal_quota_zero(&budget));
}

#[tokio::test]
async fn queue_commit_wake_panic_keeps_charged_queue_and_window() {
    let (queue, budget) = queue();
    let clock = Arc::new(crate::shared::errors::FailureOrder::default());
    let (_issuer, accounting) = paired_accounting(clock);
    let (_credit, credit_rx) = watch::channel(RelayReadCredit {
        paused: false,
        max_batch_bytes: 8,
    });
    let (back_tx, _back_rx) = crate::client_tun::relay_channel::channel(1);
    let (_stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    let (gate_tx, gate_rx) = tokio::sync::oneshot::channel();
    let bytes = Bytes::from(vec![6; 8]);
    let reader = GatedReader {
        gate: gate_rx,
        chunk: Some(NativeTcpChunk { offset: 0, bytes }),
    };
    let task = tokio::spawn(run_d16_native_reader(
        handle(),
        7,
        Box::new(reader),
        queue.clone(),
        credit_rx,
        back_tx,
        None,
        stop_rx,
        Some(accounting),
    ));
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while budget.snapshot().reserved_bytes != 8 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    let waker = Waker::from(Arc::new(PanicWake));
    let mut context = Context::from_waker(&waker);
    let mut waiting = Box::pin(queue.recv_up_to(8));
    assert!(matches!(waiting.as_mut().poll(&mut context), Poll::Pending));
    gate_tx.send(()).unwrap();

    let joined = tokio::time::timeout(std::time::Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(joined.reason, ("root", "reader_panicked"));
    assert_eq!(joined.panic_order, Some(0));
    assert_eq!(
        joined
            .body_panic
            .as_ref()
            .and_then(|payload| payload.downcast_ref::<std::io::Error>())
            .and_then(std::io::Error::raw_os_error),
        Some(72)
    );
    assert_eq!(joined.window.as_ref().map(ReaderWindow::read), Some(8));
    assert!(joined.remainder.is_some());
    let snapshot = queue.snapshot_now();
    assert_eq!(snapshot.queued_bytes, 8);
    assert_eq!(snapshot.reserved_bytes, 0);
    assert_eq!(budget.snapshot().total_bytes(), 8);
    drop(waiting);
    let mut leased = queue.recv_up_to(8).await.unwrap();
    assert_eq!(leased.bytes().as_ref(), &[6; 8]);
    assert_eq!(leased.permit_mut().release(8), 8);
    joined
        .remainder
        .as_ref()
        .unwrap()
        .queue
        .close_for_terminal_and_mark_ready("root", "reader_panicked")
        .await;
    assert!(queue.d16_terminal_quota_zero(&budget));
}

#[tokio::test]
async fn rejected_commit_then_event_wake_panic_retains_first_error_and_backing() {
    let (queue, budget) = queue();
    let clock = Arc::new(crate::shared::errors::FailureOrder::default());
    let (_issuer, accounting) = paired_accounting(clock);
    let (_credit, credit_rx) = watch::channel(RelayReadCredit {
        paused: false,
        max_batch_bytes: 8,
    });
    let (back_tx, mut back_rx) = crate::client_tun::relay_channel::channel(1);
    let (_stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    let (gate_tx, gate_rx) = tokio::sync::oneshot::channel();
    let bytes = Bytes::from(vec![7; 8]);
    let backing = bytes.as_ptr();
    let reader = GatedReader {
        gate: gate_rx,
        chunk: Some(NativeTcpChunk { offset: 0, bytes }),
    };
    let task = tokio::spawn(run_d16_native_reader(
        handle(),
        7,
        Box::new(reader),
        queue.clone(),
        credit_rx,
        back_tx,
        None,
        stop_rx,
        Some(accounting),
    ));
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while budget.snapshot().reserved_bytes != 8 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    queue
        .close_for_terminal_and_mark_ready("root", "cancelled")
        .await;

    let waker = Waker::from(Arc::new(PanicWake));
    let mut context = Context::from_waker(&waker);
    assert!(matches!(back_rx.poll_recv(&mut context), Poll::Pending));
    gate_tx.send(()).unwrap();
    let outcome = tokio::time::timeout(std::time::Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        outcome.reason,
        ("remote_to_local", "d16_queue_commit_failed")
    );
    assert_eq!(outcome.queue_error, Some(ByteQueuePushError::Closed));
    assert_eq!(outcome.error_order, Some(0));
    assert_eq!(outcome.panic_order, Some(1));
    assert_eq!(outcome.retained.as_ref().unwrap().as_ptr(), backing);
    assert_eq!(outcome.window.as_ref().map(ReaderWindow::read), Some(8));
    assert!(outcome.body_panic.is_some());
    assert!(outcome.remainder.is_some());
    assert!(queue.d16_terminal_quota_zero(&budget));
}

#[tokio::test]
async fn read_error_then_event_wake_panic_retains_first_io() {
    let (queue, budget) = queue();
    let clock = Arc::new(crate::shared::errors::FailureOrder::default());
    let (_issuer, accounting) = paired_accounting(clock);
    let (_credit, credit_rx) = watch::channel(RelayReadCredit {
        paused: false,
        max_batch_bytes: 8,
    });
    let (back_tx, mut back_rx) = crate::client_tun::relay_channel::channel(1);
    let (_stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    let waker = Waker::from(Arc::new(PanicWake));
    let mut context = Context::from_waker(&waker);
    assert!(matches!(back_rx.poll_recv(&mut context), Poll::Pending));
    let outcome = tokio::spawn(run_d16_native_reader(
        handle(),
        7,
        Box::new(ErrorReader),
        queue.clone(),
        credit_rx,
        back_tx,
        None,
        stop_rx,
        Some(accounting),
    ))
    .await
    .unwrap();
    assert_eq!(outcome.reason, ("remote_to_local", "remote_read_failed"));
    assert_eq!(
        outcome
            .error
            .as_ref()
            .and_then(std::io::Error::raw_os_error),
        Some(54)
    );
    assert_eq!(outcome.error_order, Some(0));
    assert_eq!(outcome.panic_order, Some(1));
    assert_eq!(outcome.window.as_ref().map(ReaderWindow::read), Some(0));
    assert!(outcome.body_panic.is_some());
    assert!(outcome.remainder.is_some());
    assert!(queue.d16_terminal_quota_zero(&budget));
}

#[tokio::test]
async fn read_error_then_reservation_drop_wake_panic_retains_first_io() {
    let (queue, budget) = full_reservation_queue();
    let clock = Arc::new(crate::shared::errors::FailureOrder::default());
    let (_issuer, accounting) = paired_accounting(clock);
    let (_credit, credit_rx) = watch::channel(RelayReadCredit {
        paused: false,
        max_batch_bytes: 8,
    });
    let (back_tx, _back_rx) = crate::client_tun::relay_channel::channel(1);
    let (_stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    let (gate_tx, gate_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(run_d16_native_reader(
        handle(),
        7,
        Box::new(GatedErrorReader(gate_rx)),
        queue.clone(),
        credit_rx,
        back_tx,
        None,
        stop_rx,
        Some(accounting),
    ));
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while budget.snapshot().reserved_bytes != 8 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    let waker = Waker::from(Arc::new(PanicWake));
    let mut context = Context::from_waker(&waker);
    let mut waiting = Box::pin(queue.reserve_read_up_to(8));
    assert!(matches!(waiting.as_mut().poll(&mut context), Poll::Pending));
    gate_tx.send(()).unwrap();
    let outcome = tokio::time::timeout(std::time::Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(outcome.reason, ("remote_to_local", "remote_read_failed"));
    assert_eq!(
        outcome
            .error
            .as_ref()
            .and_then(std::io::Error::raw_os_error),
        Some(54)
    );
    assert_eq!(outcome.error_order, Some(0));
    assert_eq!(outcome.panic_order, Some(1));
    assert!(outcome.body_panic.is_some());
    assert!(outcome.remainder.is_some());
    drop(waiting);
    outcome
        .remainder
        .as_ref()
        .unwrap()
        .queue
        .close_for_terminal_and_mark_ready("root", "reader_panicked")
        .await;
    assert!(queue.d16_terminal_quota_zero(&budget));
}

#[tokio::test]
async fn overdraw_then_reservation_drop_wake_panic_retains_original_backing() {
    let (queue, budget) = full_reservation_queue();
    let clock = Arc::new(crate::shared::errors::FailureOrder::default());
    let (_issuer, accounting) = paired_accounting(clock);
    let (_credit, credit_rx) = watch::channel(RelayReadCredit {
        paused: false,
        max_batch_bytes: 8,
    });
    let (back_tx, _back_rx) = crate::client_tun::relay_channel::channel(1);
    let (_stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    let (gate_tx, gate_rx) = tokio::sync::oneshot::channel();
    let bytes = Bytes::from(vec![4; 65_537]);
    let backing = bytes.as_ptr();
    let reader = GatedReader {
        gate: gate_rx,
        chunk: Some(NativeTcpChunk { offset: 0, bytes }),
    };
    let task = tokio::spawn(run_d16_native_reader(
        handle(),
        7,
        Box::new(reader),
        queue.clone(),
        credit_rx,
        back_tx,
        None,
        stop_rx,
        Some(accounting),
    ));
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while budget.snapshot().reserved_bytes != 8 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    let waker = Waker::from(Arc::new(PanicWake));
    let mut context = Context::from_waker(&waker);
    let mut waiting = Box::pin(queue.reserve_read_up_to(8));
    assert!(matches!(waiting.as_mut().poll(&mut context), Poll::Pending));
    gate_tx.send(()).unwrap();
    let outcome = tokio::time::timeout(std::time::Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(outcome.reason, ("root", "receive_window_debit_failed"));
    assert_eq!(outcome.retained.as_ref().unwrap().as_ptr(), backing);
    assert_eq!(outcome.error_order, Some(0));
    assert_eq!(outcome.panic_order, Some(1));
    assert_eq!(outcome.window.as_ref().map(ReaderWindow::read), Some(0));
    assert!(outcome.body_panic.is_some());
    assert!(outcome.remainder.is_some());
    drop(waiting);
    outcome
        .remainder
        .as_ref()
        .unwrap()
        .queue
        .close_for_terminal_and_mark_ready("root", "reader_panicked")
        .await;
    assert!(queue.d16_terminal_quota_zero(&budget));
}

#[tokio::test]
async fn poll_panic_retains_original_reservation_outside_future_unwind() {
    let (queue, budget) = full_reservation_queue();
    let clock = Arc::new(crate::shared::errors::FailureOrder::default());
    let (_issuer, accounting) = paired_accounting(clock);
    let (_credit, credit_rx) = watch::channel(RelayReadCredit {
        paused: false,
        max_batch_bytes: 8,
    });
    let (back_tx, _back_rx) = crate::client_tun::relay_channel::channel(1);
    let (_stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    let (gate_tx, gate_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(run_d16_native_reader(
        handle(),
        7,
        Box::new(GatedPanicReader(gate_rx)),
        queue.clone(),
        credit_rx,
        back_tx,
        None,
        stop_rx,
        Some(accounting),
    ));
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while budget.snapshot().reserved_bytes != 8 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let waker = Waker::from(Arc::new(PanicWake));
    let mut context = Context::from_waker(&waker);
    let mut waiting = Box::pin(queue.reserve_read_up_to(8));
    assert!(matches!(waiting.as_mut().poll(&mut context), Poll::Pending));
    gate_tx.send(()).unwrap();
    let mut outcome = tokio::time::timeout(std::time::Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(outcome.reason, ("root", "reader_panicked"));
    assert_eq!(outcome.panic_order, Some(0));
    assert_eq!(
        outcome
            .body_panic
            .as_ref()
            .and_then(|payload| payload.downcast_ref::<std::io::Error>())
            .and_then(std::io::Error::raw_os_error),
        Some(71)
    );
    assert!(outcome.future_drop_panic.is_none());
    assert_eq!(outcome.window.as_ref().map(ReaderWindow::read), Some(0));
    assert!(outcome.remainder.is_some());
    assert!(outcome.remainder.as_ref().unwrap().reservation.is_some());
    assert_eq!(budget.snapshot().reserved_bytes, 8);
    drop(waiting);
    drop(outcome.remainder.as_mut().unwrap().reservation.take());
    assert_eq!(budget.snapshot().reserved_bytes, 0);
    outcome
        .remainder
        .as_ref()
        .unwrap()
        .queue
        .close_for_terminal_and_mark_ready("root", "reader_panicked")
        .await;
    assert!(queue.d16_terminal_quota_zero(&budget));
}

#[tokio::test]
async fn global_unused_notify_panic_follows_original_queue_and_budget_commit() {
    let budget = D16GlobalByteBudget::new(8).unwrap();
    let queue = AsyncLeasedByteFlowQueue::new_with_global_budget(
        16,
        DownstreamPermitReleaseMode::OnEgressDrain,
        budget.clone(),
    )
    .unwrap();
    let other = AsyncLeasedByteFlowQueue::new_with_global_budget(
        16,
        DownstreamPermitReleaseMode::OnEgressDrain,
        budget.clone(),
    )
    .unwrap();
    let clock = Arc::new(crate::shared::errors::FailureOrder::default());
    let (_issuer, accounting) = paired_accounting(clock);
    let (_credit, credit_rx) = watch::channel(RelayReadCredit {
        paused: false,
        max_batch_bytes: 8,
    });
    let (back_tx, _back_rx) = crate::client_tun::relay_channel::channel(1);
    let (_stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    let (gate_tx, gate_rx) = tokio::sync::oneshot::channel();
    let bytes = Bytes::from(vec![3; 4]);
    let reader = GatedReader {
        gate: gate_rx,
        chunk: Some(NativeTcpChunk { offset: 0, bytes }),
    };
    let task = tokio::spawn(run_d16_native_reader(
        handle(),
        7,
        Box::new(reader),
        queue.clone(),
        credit_rx,
        back_tx,
        None,
        stop_rx,
        Some(accounting),
    ));
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while budget.snapshot().reserved_bytes != 8 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let waker = Waker::from(Arc::new(PanicWake));
    let mut context = Context::from_waker(&waker);
    let mut waiting = Box::pin(other.reserve_read_up_to(8));
    assert!(matches!(waiting.as_mut().poll(&mut context), Poll::Pending));
    gate_tx.send(()).unwrap();
    let outcome = tokio::time::timeout(std::time::Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(outcome.reason, ("root", "reader_panicked"));
    assert_eq!(outcome.panic_order, Some(0));
    assert_eq!(outcome.window.as_ref().map(ReaderWindow::read), Some(4));
    assert!(outcome.body_panic.is_some());
    assert!(outcome.remainder.is_some());
    assert_eq!(queue.snapshot_now().queued_bytes, 4);
    assert_eq!(queue.snapshot_now().reserved_bytes, 0);
    assert_eq!(budget.snapshot().owned_bytes, 4);
    assert_eq!(budget.snapshot().reserved_bytes, 0);
    drop(waiting);
    let mut leased = queue.recv_up_to(4).await.unwrap();
    assert_eq!(leased.bytes().as_ref(), &[3; 4]);
    assert_eq!(leased.permit_mut().release(4), 4);
    outcome
        .remainder
        .as_ref()
        .unwrap()
        .queue
        .close_for_terminal_and_mark_ready("root", "reader_panicked")
        .await;
    assert!(queue.d16_terminal_quota_zero(&budget));
}
