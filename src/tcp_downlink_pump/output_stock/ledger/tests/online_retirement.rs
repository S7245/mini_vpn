//! Online disposal shares the original Producer role and actual loan returns.
use super::*;
use super::super::{OutputStock, StockProducer, StockWaitError, Tail};
use crate::tcp_downlink_pump::CustodiedBytes;

fn funded() -> (BackingBudget, BackingMeter, AsyncByteBoundedFlowQueue,
    OutputStock, StockProducer, CustodiedBytes, CustodiedBytes)
{
    let budget = BackingBudget::new(65536).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (clock, _) = crate::tcp_downlink_pump::tests::native_waiters::clock(&meter);
    let (queue, mut stock) = AsyncByteBoundedFlowQueue::new_with_progressive_output_stock(
        4, &meter, clock, 8, 4).unwrap();
    let mut producer = stock.take_producer().unwrap();
    let mut first = producer.try_prepare(4).unwrap().unwrap();
    first.write(b"boot").unwrap();
    let first = first.finish().unwrap();
    let mut last = producer.try_prepare(8).unwrap().unwrap();
    last.write(b"lastloan").unwrap();
    let last = last.finish().unwrap();
    (budget, meter, queue, stock, producer, first, last)
}

#[tokio::test(flavor = "current_thread")]
async fn online_retirement_waits_for_last_actual_loan_then_refunds_only_large_backing() {
    let (budget, meter, queue, mut stock, mut producer, bootstrap, last) = funded();
    let alias = last.clone();
    let bootstrap_id = bootstrap.allocation_identity();
    let charged = budget.used();
    let wakes = Arc::new(CountWake(AtomicUsize::new(0)));
    let waker = Waker::from(wakes.clone());
    let mut cx = Context::from_waker(&waker);
    let mut retiring = Box::pin(producer.retire_idle_output());
    assert!(retiring.as_mut().poll(&mut cx).is_pending());
    drop(last);
    assert_eq!(budget.used(), charged);
    assert_eq!(wakes.0.load(Ordering::SeqCst), 0);
    assert!(retiring.as_mut().poll(&mut cx).is_pending());
    drop(alias);
    assert!(wakes.0.load(Ordering::SeqCst) > 0);
    assert_eq!(budget.used(), charged, "Loan Drop only returns availability");
    assert!(matches!(retiring.as_mut().poll(&mut cx), Poll::Ready(Ok(()))));
    drop(retiring);
    assert_eq!(charged - budget.used(), bytes::BytesMut::metered_copy_layout_for_len(8).unwrap());
    assert_eq!((meter.snapshot().allocations, meter.snapshot().payload_bytes), (1, 4));
    assert_eq!(bootstrap.allocation_identity(), bootstrap_id);
    {
        let root = stock.root.as_ref().unwrap();
        let state = root.lock().unwrap();
        assert!(root.inventory_matches(&state));
        assert_eq!((state.funded, state.funded_capacity, state.retiring), (1, 4, false));
    }
    // Re-funding is actual new backing; online disposal did not close the stock.
    let operation = producer.try_prepare(8).unwrap().unwrap();
    assert_eq!(meter.snapshot().allocations, 2);
    drop(operation);
    drop((bootstrap, producer));
    queue.close().await;
    assert!(queue.dispose_custodied().unwrap().is_some());
    assert!(stock.try_retire());
    drop(queue);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[tokio::test(flavor = "current_thread")]
async fn online_retirement_zero_poll_cancel_wait_cancel_and_old_grant_keep_ownership() {
    let (budget, _, queue, mut stock, mut producer, bootstrap, last) = funded();
    let charged = budget.used();
    drop(producer.retire_idle_output());
    let mut wait = Box::pin(producer.retire_idle_output());
    assert!(wait.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());
    drop(wait);
    assert_eq!(budget.used(), charged);
    assert!(!budget.quarantined());
    drop(last);
    let mut grant = None;
    producer.prepare_read(8, &mut grant).await.unwrap();
    assert!(matches!(producer.retire_idle_output().await, Err(StockWaitError::Pressure)));
    grant.as_ref().unwrap().validate_unused().unwrap();
    assert_eq!(budget.used(), charged);
    drop(grant);
    producer.retire_idle_output().await.unwrap();
    assert!(!budget.quarantined());
    drop((bootstrap, producer));
    queue.close().await;
    assert!(queue.dispose_custodied().unwrap().is_some());
    assert!(stock.try_retire());
    drop(queue);
    assert_eq!(budget.used(), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn online_retirement_keeps_bootstrap_and_distinguishes_capacity_from_closed() {
    let (budget, _, queue, mut stock, mut producer, bootstrap, last) = funded();
    drop(last);
    producer.retire_idle_output().await.unwrap();
    let charged = budget.used();
    assert!(matches!(producer.retire_idle_output().await,
        Err(StockWaitError::Admission(bytes::MeteredCopyError::BudgetFull))));
    assert_eq!(budget.used(), charged);
    queue.close().await;
    assert!(matches!(producer.retire_idle_output().await, Err(StockWaitError::Closed)));
    assert_eq!(budget.used(), charged);
    assert!(!budget.quarantined());
    drop((bootstrap, producer));
    assert!(queue.dispose_custodied().unwrap().is_some());
    assert!(stock.try_retire());
    drop(queue);
    assert_eq!(budget.used(), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn online_retirement_unknown_actual_alias_retains_original_allocation_in_quarantine() {
    let (budget, meter, queue, mut stock, mut producer, bootstrap, last) = funded();
    let identity = last.allocation_identity();
    drop(last);
    let hidden = {
        let root = stock.root.as_ref().unwrap();
        let mut state = root.lock().unwrap();
        let slab = state.slab_mut(1).unwrap();
        // A real untracked Shared reference contradicts the nominal zero views.
        // Restore an initialized full tail first; published last loan used it all.
        let Some(Tail::Mutable(tail)) = slab.tail.take() else { panic!() };
        let mut tail = tail.freeze().try_into_mut().unwrap();
        tail.clear();
        assert!(tail.try_reclaim(8));
        unsafe { tail.set_len(8); }
        let hidden = tail.split_to(1).freeze();
        slab.tail = Some(Tail::Mutable(tail));
        assert!(root.inventory_matches(&state));
        hidden
    };
    let charged = budget.used();
    assert!(matches!(producer.retire_idle_output().await,
        Err(StockWaitError::Admission(bytes::MeteredCopyError::Quarantined))));
    assert!(budget.quarantined());
    assert_eq!(hidden.allocation_identity(), identity);
    assert_eq!(budget.used(), charged);
    {
        let root = stock.root.as_ref().unwrap();
        let state = root.lock().unwrap();
        let Some(Tail::Quarantined(original)) = &state.slab(1).unwrap().tail else { panic!() };
        assert_eq!(original.allocation_identity(), identity);
        assert_eq!(state.funded, 2);
    }
    let fifo_layout = {
        let state = queue.inner.state.lock().await;
        assert!(state.queue.chunks.is_empty());
        std::alloc::Layout::array::<crate::tcp_downlink_pump::ByteChunk>(state.queue.chunks.capacity()).unwrap().size()
    };
    let physical = meter.snapshot().physical_bytes;
    drop((hidden, bootstrap, producer, queue));
    assert!(!stock.try_retire());
    drop(stock);
    assert_eq!(budget.used(), charged - fifo_layout, "empty FIFO storage really drops; uncertain stock does not");
    assert_eq!(meter.snapshot().physical_bytes, physical);
    assert_eq!(meter.snapshot().allocations, 2);
}

#[tokio::test(flavor = "current_thread")]
async fn online_retirement_abandoned_turn_keeps_tail_but_post_drop_refusal_keeps_only_live_inventory() {
    for actually_drop in [false, true] {
        let (budget, meter, queue, mut stock, producer, bootstrap, last) = funded();
        let identity = last.allocation_identity();
        drop(last);
        let root = stock.root.as_ref().unwrap();
        let tail = {
            let mut state = root.lock().unwrap();
            let Some(Tail::Mutable(tail)) = state.slab_mut(1).unwrap().tail.take() else { panic!() };
            let original = tail.freeze();
            assert_eq!(original.allocation_identity(), identity);
            let tail = original.try_into_mut().unwrap();
            state.retiring = true;
            // Fault injection: the original funded record deliberately keeps
            // its missing tail. No terminal inventory can accept this state.
            tail
        };
        let charged = budget.used();
        let physical = meter.snapshot().physical_bytes;
        let turn = super::super::RetiringTail {
            root, tail: std::mem::ManuallyDrop::new(tail), complete: false,
        };
        if actually_drop {
            assert!(matches!(turn.finish(),
                Err(StockWaitError::Admission(bytes::MeteredCopyError::Quarantined))));
        } else {
            drop(turn);
        }
        let released = if actually_drop { bytes::BytesMut::metered_copy_layout_for_len(8).unwrap() } else { 0 };
        assert!(budget.quarantined());
        assert_eq!(budget.used(), charged - released);
        assert_eq!(meter.snapshot().physical_bytes, physical - released);
        assert_eq!(meter.snapshot().allocations, if actually_drop { 1 } else { 2 });
        assert!(root.lock().unwrap().retiring, "no refused turn publishes physical completion");
        assert_eq!(bootstrap.as_ref(), b"boot");
        assert!(!stock.try_retire());
        drop((producer, bootstrap, stock));
        // Keep the separate FIFO alive so only this actual disposal is measured.
        assert_eq!(budget.used(), charged - released, "no second physical Drop or nominal cleanup refund");
        drop(queue);
    }
}
