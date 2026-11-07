//! Actual closed/cancel/cold-refusal and interrupted physical settlement.
use super::*;
use crate::tcp_downlink_pump::output_stock::StockWaitError;

#[tokio::test(flavor = "current_thread")]
async fn original_close_ends_stock_pressure_without_dropping_any_alias_early() {
    let budget = BackingBudget::new(65536).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (clock, _facts) = crate::tcp_downlink_pump::tests::native_waiters::clock(&meter);
    let (queue, mut stock) = AsyncByteBoundedFlowQueue::new_with_output_stock(4, &meter, clock, 8, 4).unwrap();
    let mut producer = stock.take_producer().unwrap();
    let mut held = Vec::new();
    while let Some(mut space) = producer.try_prepare(8).unwrap() {
        space.write(b"12345678").unwrap(); held.push(space.finish().unwrap());
    }
    let charged = budget.used();
    let mut waiting = Box::pin(producer.wait_prepare(4));
    assert!(waiting.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());
    queue.close().await;
    assert!(matches!(waiting.as_mut().poll(&mut Context::from_waker(Waker::noop())), Poll::Ready(Err(StockWaitError::Closed))));
    drop(waiting);
    assert_eq!(held.len(), 6);
    assert_eq!(budget.used(), charged);
    drop(producer);
    assert!(queue.dispose_custodied().unwrap().is_some());
    drop(queue);
    assert!(!stock.try_retire());
    drop(held);
    assert!(stock.try_retire());
    assert!(!budget.quarantined());
    assert_eq!(budget.used(), 0);
}

#[test]
fn cancelled_unpublished_copy_preserves_the_real_stock_and_next_prefix() {
    let budget = BackingBudget::new(65536).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (clock, _facts) = crate::tcp_downlink_pump::tests::native_waiters::clock(&meter);
    let (queue, mut stock) = AsyncByteBoundedFlowQueue::new_with_output_stock(4, &meter, clock, 8, 4).unwrap();
    let mut producer = stock.take_producer().unwrap();
    let charged = budget.used();
    drop(producer.try_prepare(4).unwrap().unwrap());
    let mut abandoned = producer.try_prepare(4).unwrap().unwrap();
    abandoned.write(b"old").unwrap();
    drop(abandoned);
    let mut next = producer.try_prepare(4).unwrap().unwrap();
    next.write(b"next").unwrap();
    let view = next.finish().unwrap();
    assert_eq!(view.as_ref(), b"next");
    assert_eq!(budget.used(), charged);
    drop((view, producer));
    assert!(queue.dispose_custodied().unwrap().is_some());
    assert!(stock.try_retire());
    drop(queue);
    assert!(!budget.quarantined());
    assert_eq!(budget.used(), 0);
}

#[test]
fn real_cold_backing_full_rolls_back_without_publishing_a_stock_or_actor() {
    let budget = BackingBudget::new(4096).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (clock, facts) = crate::tcp_downlink_pump::tests::native_waiters::clock(&meter);
    let before = budget.used();
    let result = AsyncByteBoundedFlowQueue::new_with_output_stock(4, &meter, clock.clone(), 1024, 4);
    assert!(matches!(result, Err(bytes::MeteredCopyError::BudgetFull)));
    assert_eq!(budget.used(), before, "all partially prepared real layouts roll back");
    assert!(!budget.quarantined());
    assert!(facts.0.lock().unwrap().is_empty());
    assert_eq!(facts.1.load(Ordering::SeqCst), 0);
    drop((clock, facts));
    assert_eq!(budget.used(), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn completed_actual_chunk_drop_survives_original_fifo_relock_contention() {
    let budget = BackingBudget::new(65536).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (clock, _facts) = crate::tcp_downlink_pump::tests::native_waiters::clock(&meter);
    let (queue, mut stock) = AsyncByteBoundedFlowQueue::new_with_output_stock(4, &meter, clock, 8, 4).unwrap();
    let mut producer = stock.take_producer().unwrap();
    let mut space = producer.try_prepare(4).unwrap().unwrap();
    space.write(b"tail").unwrap();
    queue.push_stock(&producer, &mut Some(OwnedPayload::Native(space.finish().unwrap()))).await.unwrap();
    drop(producer);
    let root = stock.root.as_ref().unwrap();
    // Block the actual Loan ledger, after the actual FIFO has moved its chunk
    // out. No synthetic completion marker or production scheduling hook.
    let blocked = root.lock().unwrap();
    let other = queue.clone();
    let thread = std::thread::spawn(move || other.dispose_custodied());
    let until = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let guard = loop {
        if queue.inner.waiters.get().unwrap().disposal_state() == 1 {
            if let Ok(guard) = queue.inner.state.try_lock() { break Some(guard); }
        }
        if std::time::Instant::now() >= until { break None; }
        std::thread::yield_now();
    };
    assert!(matches!(queue.dispose_custodied(), Ok(None)), "another turn cannot steal the in-flight physical disposition");
    drop(blocked); // Actual Loan/Bytes return can now finish against the held FIFO.
    let result = thread.join().unwrap();
    assert!(guard.is_some(), "actual Drop reached its original ownership ledger");
    assert!(matches!(result, Ok(None)));
    let guard = guard.unwrap();
    assert!(guard.custody.interrupted);
    assert_eq!(queue.inner.waiters.get().unwrap().disposal_state(), 2);
    assert!(!budget.quarantined(), "ordinary relock contention retains a known pending commit");
    drop(guard);
    let receipt = queue.dispose_custodied().unwrap().unwrap();
    assert_eq!((receipt.discarded_chunks, receipt.discarded_bytes), (1, 4));
    assert!(stock.try_retire());
    drop(queue);
    assert!(!budget.quarantined());
    assert_eq!(budget.used(), 0);
}


#[test]
fn original_app_clock_and_distinct_node_meter_share_the_cold_stock_budget() {
    let budget = BackingBudget::new(65536).unwrap();
    let app_meter = BackingMeter::with_budget(&budget);
    let node_meter = BackingMeter::with_budget(&budget);
    assert!(!app_meter.same_authority(&node_meter));
    let (clock, facts) = crate::tcp_downlink_pump::tests::native_waiters::clock(&app_meter);
    let (queue, mut stock) = AsyncByteBoundedFlowQueue::new_with_output_stock(4, &node_meter, clock.clone(), 8, 4).unwrap();
    assert!(Arc::ptr_eq(&queue.inner.waiters.get().unwrap().bound_clock().unwrap(), &clock));
    assert!(Arc::ptr_eq(&stock.root.as_ref().unwrap().body.clock, &clock));
    let mut producer = stock.take_producer().unwrap();
    let mut space = producer.try_prepare(4).unwrap().unwrap();
    space.write(b"node").unwrap();
    let payload = space.finish().unwrap();
    assert!(payload.backing_meter().unwrap().same_authority(&node_meter));
    assert!(!payload.backing_meter().unwrap().same_authority(&app_meter));
    drop((payload, producer));
    assert!(queue.dispose_custodied().unwrap().is_some());
    assert!(stock.try_retire());
    drop((stock, queue, clock));
    assert!(!budget.quarantined());
    assert_eq!(facts.1.load(Ordering::SeqCst), 0);
    assert_eq!(budget.used(), 0);
}

#[test]
fn cold_record_identity_mismatch_cannot_publish_a_clean_retirement() {
    let budget = BackingBudget::new(65536).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (clock, facts) = crate::tcp_downlink_pump::tests::native_waiters::clock(&meter);
    let (_queue, mut stock) = AsyncByteBoundedFlowQueue::new_with_output_stock(4, &meter, clock, 8, 4).unwrap();
    stock.root.as_ref().unwrap().lock().unwrap().slab_mut(0).unwrap().identity.base = 0;
    assert!(!stock.try_retire(), "stored identity must match the actual cold allocation before retirement");
    assert!(budget.quarantined());
    assert!(facts.1.load(Ordering::SeqCst) > 0);
    assert!(budget.used() > 0);
}

#[test]
fn missing_original_cold_record_cannot_be_normalized_to_an_empty_stock() {
    let budget = BackingBudget::new(65536).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (clock, facts) = crate::tcp_downlink_pump::tests::native_waiters::clock(&meter);
    let (_queue, mut stock) = AsyncByteBoundedFlowQueue::new_with_output_stock(4, &meter, clock, 8, 4).unwrap();
    let missing = stock.root.as_ref().unwrap().lock().unwrap().slabs.pop().unwrap();
    assert!(!stock.try_retire(), "stored zero views does not prove the complete physical inventory");
    assert!(budget.quarantined());
    assert!(facts.1.load(Ordering::SeqCst) > 0);
    assert!(budget.used() > 0);
    drop(missing);
}


#[test]
fn ready_marker_on_a_live_actual_slab_is_rejected_without_repairing_it_to_clean() {
    let budget = BackingBudget::new(65536).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (clock, facts) = crate::tcp_downlink_pump::tests::native_waiters::clock(&meter);
    let (_queue, mut stock) = AsyncByteBoundedFlowQueue::new_with_output_stock(4, &meter, clock, 8, 4).unwrap();
    let mut producer = stock.take_producer().unwrap();
    let mut space = producer.try_prepare(8).unwrap().unwrap();
    space.write(b"original").unwrap();
    let view = space.finish().unwrap();
    stock.root.as_ref().unwrap().lock().unwrap().ready |= 1;
    assert!(matches!(producer.try_prepare(4), Err(bytes::MeteredCopyError::Quarantined)));
    assert_eq!(view.as_ref(), b"original");
    drop((view, producer));
    assert!(!stock.try_retire());
    assert!(budget.quarantined());
    assert!(facts.1.load(Ordering::SeqCst) > 0);
    assert!(budget.used() > 0);
}
