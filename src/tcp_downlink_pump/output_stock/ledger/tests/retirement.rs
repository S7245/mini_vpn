//! DATA disposal is distinct from actual pool and callback retirement.
use super::*;

#[tokio::test(flavor = "current_thread")]
async fn explicit_retirement_requires_live_notification_owner_while_actual_alias_remains() {
    let budget = BackingBudget::new(65536).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (clock, facts) = crate::tcp_downlink_pump::tests::native_waiters::clock(&meter);
    let (queue, mut stock) = AsyncByteBoundedFlowQueue::new_with_output_stock(4, &meter, clock, 8, 4).unwrap();
    let mut producer = stock.take_producer().unwrap();
    let mut space = producer.try_prepare(4).unwrap().unwrap();
    space.write(b"tail").unwrap();
    let alias = space.finish().unwrap();
    let identity = alias.allocation_identity();
    drop(producer);
    drop(queue);
    assert!(!budget.quarantined());
    let result = stock.wait_returned().await;
    assert!(matches!(result, Err(super::super::StockWaitError::Admission(bytes::MeteredCopyError::Quarantined))));
    assert!(budget.quarantined());
    assert!(budget.used() > 0);
    assert_eq!(alias.allocation_identity(), identity);
    assert_eq!(alias.as_ref(), b"tail");
    assert!(facts.0.lock().unwrap().is_empty());
    assert!(facts.1.load(Ordering::SeqCst) > 0);
    drop(alias);
    assert!(!stock.try_retire());
}

#[tokio::test(flavor = "current_thread")]
async fn retirement_cancel_preserves_original_owner_and_live_writer_is_pressure() {
    let budget = BackingBudget::new(65536).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (clock, _) = crate::tcp_downlink_pump::tests::native_waiters::clock(&meter);
    let (queue, mut stock) = AsyncByteBoundedFlowQueue::new_with_output_stock(4, &meter, clock, 8, 4).unwrap();
    let mut producer = stock.take_producer().unwrap();
    assert!(matches!(stock.wait_returned().await, Err(super::super::StockWaitError::Pressure)));
    let mut space = producer.try_prepare(4).unwrap().unwrap();
    space.write(b"tail").unwrap();
    let alias = space.finish().unwrap();
    drop(producer);
    queue.close().await;
    assert!(queue.dispose_data().unwrap().is_some());
    let charged = budget.used();
    drop(stock.wait_returned()); // zero poll
    let mut waiting = Box::pin(stock.wait_returned());
    assert!(waiting.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());
    drop(waiting); // actual guarded waiting work and callback retire
    assert_eq!(budget.used(), charged);
    assert!(!budget.quarantined());
    let mut successor = Box::pin(stock.wait_returned());
    assert!(successor.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());
    drop(alias);
    assert!(matches!(successor.as_mut().poll(&mut Context::from_waker(Waker::noop())), Poll::Ready(Ok(()))));
    drop(successor);
    assert!(stock.try_retire());
    assert!(queue.dispose_custodied().unwrap().is_some());
    drop(queue);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[tokio::test(flavor = "current_thread")]
async fn joined_writer_data_drain_preserves_retirement_wake_until_actual_last_alias() {
    let budget = BackingBudget::new(65536).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (clock, facts) = crate::tcp_downlink_pump::tests::native_waiters::clock(&meter);
    let (queue, mut stock) = AsyncByteBoundedFlowQueue::new_with_output_stock(4, &meter, clock, 8, 4).unwrap();
    let mut producer = stock.take_producer().unwrap();
    let mut operation = None;
    producer.prepare_read(4, &mut operation).await.unwrap();
    let mut operation = operation.take().unwrap();
    operation.space().as_mut().unwrap().write(b"tail").unwrap();
    let mut pending = Some(OwnedPayload::Native(operation.space().take().unwrap().finish().unwrap()));
    operation.publish(&mut pending).await.unwrap();
    drop(operation);
    let mut held = None;
    assert!(queue.recv_custodied(4, &mut held).await.unwrap());
    let batch = held.as_ref().unwrap();
    let last = OwnedPayload::from_segment(&batch.bytes, 0..4);
    drop(producer); // Actual writer disposal, not a JOIN marker.
    queue.close().await;
    let data = queue.dispose_data().unwrap().unwrap();
    assert_eq!((data.custody().consumed, data.custody().emitted), (4, 4));
    let charged = budget.used();
    let wakes = Arc::new(CountWake(AtomicUsize::new(0)));
    let waker = Waker::from(wakes.clone());
    let mut cx = Context::from_waker(&waker);
    let mut retiring = Box::pin(stock.wait_returned());
    assert!(retiring.as_mut().poll(&mut cx).is_pending());
    drop(held);
    assert_eq!(wakes.0.load(Ordering::SeqCst), 0);
    assert_eq!(budget.used(), charged);
    drop(last);
    assert!(wakes.0.load(Ordering::SeqCst) > 0);
    assert!(matches!(retiring.as_mut().poll(&mut cx), Poll::Ready(Ok(()))));
    drop(retiring); // Real future/Root reference before physical retirement.
    assert!(stock.try_retire());
    assert!(queue.dispose_custodied().unwrap().is_some());
    assert!(facts.0.lock().unwrap().is_empty());
    drop((queue, stock));
    assert!(!budget.quarantined());
    assert_eq!(budget.used(), 0);
}
