//! Actual read grant rejection at the original queue/stock ownership seam.
use super::*;
use std::{cell::Cell, task::Poll};

#[tokio::test(flavor = "current_thread")]
async fn foreign_space_with_same_meter_is_rejected_before_read_and_publishes_both_clocks() {
    let budget = bytes::BackingBudget::new(65536).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (clock_a, facts_a) = crate::tcp_downlink_pump::tests::native_waiters::clock(&meter);
    let (clock_b, facts_b) = crate::tcp_downlink_pump::tests::native_waiters::clock(&meter);
    let (queue_a, mut stock_a) = AsyncByteBoundedFlowQueue::new_with_progressive_output_stock(
        16, &meter, clock_a, 8, 4).unwrap();
    let (queue_b, mut stock_b) = AsyncByteBoundedFlowQueue::new_with_progressive_output_stock(
        16, &meter, clock_b, 8, 4).unwrap();
    let mut producer_a = stock_a.take_producer().unwrap();
    let mut producer_b = stock_b.take_producer().unwrap();
    let mut grant = None;
    producer_a.prepare_read(4, &mut grant).await.unwrap();
    let foreign = producer_b.try_prepare(4).unwrap().unwrap();
    drop(grant.as_mut().unwrap().space.replace(foreign));
    let reads = Cell::new(0);
    let result = producer_a.read_or_trim(&mut grant, |_, _| {
        reads.set(reads.get() + 1); Poll::Ready(())
    }).await;
    assert_eq!(reads.get(), 0, "same meter cannot authorize another Root's physical grant");
    assert!(matches!(result, Err((StockWaitError::Admission(MeteredCopyError::Quarantined), None))));
    assert!(facts_a.1.load(Ordering::SeqCst) > 0 && facts_b.1.load(Ordering::SeqCst) > 0);
    assert!(facts_a.0.lock().unwrap().is_empty() && facts_b.0.lock().unwrap().is_empty());
    assert!(budget.quarantined() && budget.used() > 0);
    drop((grant, producer_a, producer_b, stock_a, stock_b, queue_a, queue_b));
}

impl ReadAdmission {
    pub(in crate::tcp_downlink_pump) fn active_for_test(&self) -> (u64, usize) {
        (self.active.load(Ordering::Acquire), self.reserved.load(Ordering::Acquire))
    }
}
