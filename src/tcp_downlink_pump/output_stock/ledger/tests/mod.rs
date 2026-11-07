//! Actual stock/queue/Bytes lifecycle, not the two-Flow ACK acceptance.
use crate::tcp_downlink_pump::{AsyncByteBoundedFlowQueue, OwnedPayload};
impl super::StockKey {
    pub(crate) fn inventory_for_test(&self) -> Option<String> {
        let root = self.0.upgrade()?;
        let state = root.lock().ok()?;
        Some(format!("funded={} capacity={} busy={} views={} bytes={} active={:?} ready={:#x} operation={:?} writer={} closed={} retiring={} inventory_matches={}",
            state.funded, state.funded_capacity, state.busy, state.views, state.view_bytes,
            state.active, state.ready, state.operation.map(|op| (op.id, op.index, op.max, op.written)),
            state.writer, state.closed, state.retiring, root.inventory_matches(&state)))
    }
}
impl super::OutputSpace {
    pub(crate) fn operation_for_test(&self) -> Option<(usize, u64, usize, usize)> {
        let state = self.root.lock().unwrap();
        state.operation.map(|op| (Arc::as_ptr(&self.root) as usize, op.id, op.max, op.written))
    }
}
use bytes::{BackingBudget, BackingMeter};
use std::{
    future::Future,
    sync::{Arc, atomic::{AtomicUsize, Ordering}},
    task::{Context, Poll, Wake, Waker},
};

struct CountWake(AtomicUsize);
impl Wake for CountWake {
    fn wake(self: Arc<Self>) { self.0.fetch_add(1, Ordering::SeqCst); }
    fn wake_by_ref(self: &Arc<Self>) { self.0.fetch_add(1, Ordering::SeqCst); }
}

#[tokio::test(flavor = "current_thread")]
async fn old_pop_cannot_reuse_slab_and_only_last_alias_drop_wakes_original_producer() {
    let budget = BackingBudget::new(65536).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (clock, facts) = crate::tcp_downlink_pump::tests::native_waiters::clock(&meter);
    // Small explicit geometry exercises the same checked stock constructor.
    // Real Native Roles supply their original C/S/W, not this test geometry.
    let (queue, mut stock) = AsyncByteBoundedFlowQueue::new_with_output_stock(4, &meter, clock, 8, 4).unwrap();
    let mut producer = stock.take_producer().unwrap();
    let mut held = Vec::new();
    while let Some(mut space) = producer.try_prepare(4).unwrap() {
        space.write(b"abcd").unwrap();
        let mut pending = Some(OwnedPayload::Native(space.finish().unwrap()));
        queue.push_stock(&producer, &mut pending).await.unwrap();
        assert!(pending.is_none());
        let mut batch = None;
        assert!(queue.recv_custodied(4, &mut batch).await.unwrap());
        held.push(batch.unwrap());
    }
    assert_eq!(held.len(), 12, "six actual slabs each carry two four-byte views");
    // Every real queue-pop notification has already occurred. Keep exactly one
    // final alias of the first slab, while all other slabs remain loaned.
    let first = held.remove(0);
    let original = first.bytes.allocation_identity().unwrap();
    let last = OwnedPayload::from_segment(&first.bytes, 0..first.bytes.len());
    drop(first);
    drop(held.remove(0));
    let charged = budget.used();
    let wakes = Arc::new(CountWake(AtomicUsize::new(0)));
    let waker = Waker::from(wakes.clone());
    let mut context = Context::from_waker(&waker);
    let mut waiting = Box::pin(producer.wait_prepare(4));
    assert!(waiting.as_mut().poll(&mut context).is_pending());
    assert_eq!(wakes.0.load(Ordering::SeqCst), 0);
    assert_eq!(budget.used(), charged);
    drop(last);
    // No new DATA, pop, timer or manual poll before the real notification.
    assert!(wakes.0.load(Ordering::SeqCst) > 0, "actual final Bytes Drop must notify the producer");
    let Poll::Ready(Ok(mut next)) = waiting.as_mut().poll(&mut context) else {
        panic!("actual returned original slab must become writable");
    };
    drop(waiting);
    next.write(b"wxyz").unwrap();
    let returned = next.finish().unwrap();
    assert_eq!(returned.allocation_identity(), Some(original));
    assert_eq!(returned.as_ref(), b"wxyz");
    assert_eq!(budget.used(), charged, "last view returns availability, not global credit");
    drop((returned, held));
    drop(producer);
    queue.close().await;
    assert!(queue.dispose_custodied().unwrap().is_some());
    assert!(stock.try_retire(), "actual writer and all views are gone before Root retirement");
    assert!(!budget.quarantined());
    assert!(facts.0.lock().unwrap().is_empty());
    drop((stock, queue));
    assert_eq!(meter.snapshot().allocations, 0);
    assert_eq!(budget.used(), 0);
}


#[tokio::test(flavor = "current_thread")]
async fn joined_producer_late_actual_alias_returns_after_original_queue_is_gone() {
    let budget = BackingBudget::new(65536).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (clock, facts) = crate::tcp_downlink_pump::tests::native_waiters::clock(&meter);
    let (queue, mut stock) = AsyncByteBoundedFlowQueue::new_with_output_stock(4, &meter, clock, 8, 4).unwrap();
    let mut producer = stock.take_producer().unwrap();
    let mut space = producer.try_prepare(4).unwrap().unwrap();
    space.write(b"tail").unwrap();
    let alias = space.finish().unwrap();
    drop(producer);
    queue.close().await;
    assert!(queue.dispose_custodied().unwrap().is_some());
    drop(queue);
    assert!(!stock.try_retire(), "actual alias still holds the slab");
    drop(alias);
    assert!(!budget.quarantined(), "no producer remains to notify on normal late return");
    assert!(stock.try_retire());
    assert!(facts.0.lock().unwrap().is_empty());
    assert_eq!(budget.used(), 0);
}

async fn foreign_old_entry(leased: bool, native: bool) {
    use crate::tcp_downlink_pump::{AsyncLeasedByteFlowQueue, ByteCustodyError};
    let budget = BackingBudget::new(65536).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (clock, _facts) = crate::tcp_downlink_pump::tests::native_waiters::clock(&meter);
    let (_queue, mut stock) = AsyncByteBoundedFlowQueue::new_with_output_stock(4, &meter, clock.clone(), 8, 4).unwrap();
    let mut producer = stock.take_producer().unwrap();
    let mut space = producer.try_prepare(4).unwrap().unwrap();
    space.write(b"loan").unwrap();
    let mut pending = Some(OwnedPayload::Native(space.finish().unwrap()));
    let identity = match pending.as_ref().unwrap() { OwnedPayload::Native(p) => p.allocation_identity(), _ => unreachable!() };
    let result = if leased {
        let other = AsyncLeasedByteFlowQueue::new_custodied(4).unwrap();
        if native {
            other.bind_native_meter(Some(meter.clone())).unwrap();
            assert!(other.bind_native_failures(clock));
        }
        let result = other.push_custodied(&mut pending).await;
        assert_eq!(result, Err(ByteCustodyError::Invalid));
        assert_eq!(other.blocking_snapshot_for_test().queued_bytes, 0);
        result
    } else {
        let other = AsyncByteBoundedFlowQueue::new(4).unwrap();
        if native {
            other.bind_native_meter(Some(meter.clone())).unwrap();
            assert!(other.bind_native_failures(clock));
        }
        let result = other.push_custodied(&mut pending).await;
        assert_eq!(result, Err(ByteCustodyError::Invalid));
        assert_eq!(other.custody_snapshot().await.0.queued_bytes, 0);
        result
    };
    assert_eq!(result, Err(ByteCustodyError::Invalid));
    assert!(pending.is_some(), "foreign rejection must preserve original payload");
    assert_eq!(match pending.as_ref().unwrap() { OwnedPayload::Native(p) => p.allocation_identity(), _ => unreachable!() }, identity);
    assert!(budget.quarantined());
    assert!(budget.used() > 0);
}
#[tokio::test(flavor = "current_thread")]
async fn old_byte_entry_rejects_foreign_stock_before_taking_pending() { foreign_old_entry(false, true).await; }
#[tokio::test(flavor = "current_thread")]
async fn old_leased_entry_rejects_foreign_stock_before_taking_pending() { foreign_old_entry(true, true).await; }


#[tokio::test(flavor = "current_thread")]
async fn stock_owner_loss_publishes_original_quiet_without_another_actor_poll() {
    let budget = BackingBudget::new(65536).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (clock, facts) = crate::tcp_downlink_pump::tests::native_waiters::clock(&meter);
    let (queue, mut stock) = AsyncByteBoundedFlowQueue::new_with_output_stock(4, &meter, clock, 8, 4).unwrap();
    let producer = stock.take_producer().unwrap();
    assert_eq!(facts.1.load(Ordering::SeqCst), 0);
    drop(stock); // No actor poll, synthetic IO, queue loss or loan return.
    assert!(budget.quarantined());
    assert!(facts.1.load(Ordering::SeqCst) > 0, "original quiet must publish at actual owner loss");
    assert!(facts.0.lock().unwrap().is_empty(), "owner loss must not invent an IO ordinal");
    assert!(budget.used() > 0);
    drop((producer, queue));
}

mod lifecycle;
mod quiet;

#[tokio::test(flavor = "current_thread")]
async fn old_legacy_byte_entry_rejects_stock_without_erasing_original_payload() { foreign_old_entry(false, false).await; }
#[tokio::test(flavor = "current_thread")]
async fn old_legacy_leased_entry_rejects_stock_without_erasing_original_payload() { foreign_old_entry(true, false).await; }

mod capacity;

mod progressive;

mod read_port;
mod retirement;
mod online_retirement;
mod pending_read;

impl super::StockKey {
    pub(crate) fn retirement_waiter_matches_for_test(&self, waker: &Waker) -> bool {
        let Some(root) = self.0.upgrade() else { return false; };
        match &root.notify {
            super::QueueReturn::Bytes(queue) => queue.upgrade().is_some_and(|queue| queue.waiters.get()
                .is_some_and(|waiters| waiters.producer_waiter_matches_for_test(waker))),
            super::QueueReturn::Leased(queue) => queue.upgrade().is_some_and(|queue| queue.waiters()
                .is_some_and(|waiters| waiters.producer_waiter_matches_for_test(waker))),
        }
    }
    pub(crate) fn loans_for_test(&self) -> Option<(u8, usize, usize, bool)> {
        let root = self.0.upgrade()?;
        let state = root.lock().ok()?;
        Some((state.writer, state.views, state.view_bytes, state.operation.is_some()))
    }
}

impl super::StockKey {
    pub(in crate::tcp_downlink_pump) fn assert_leased_pending_credit_for_test(
        &self, expected: &crate::tcp_downlink_pump::leased::stock::StockQueue,
        meter: &BackingMeter, max: usize,
    ) {
        let root = self.0.upgrade().unwrap();
        assert!(root.meter.same_authority(meter));
        let super::QueueReturn::Leased(actual) = &root.notify else { panic!("original Leased owner"); };
        assert!(actual.upgrade().unwrap().same(expected));
        let state = root.lock().unwrap();
        assert!(root.inventory_matches(&state));
        let operation = state.operation.unwrap();
        assert!(operation.id > 0);
        assert_eq!((operation.max, operation.written), (max, 0));
    }
}
