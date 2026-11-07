//! Actual reserved/queued/leased credit and separate slab/callback lifetimes.
use super::*;
use crate::tcp_downlink_pump::{OwnedPayload, StockReadOperation, StockWaitError};
use bytes::{BackingBudget, BackingMeter};
use std::{future::Future, task::{Context, Waker}};

async fn queue(mode: DownstreamPermitReleaseMode) -> (BackingBudget, AsyncLeasedByteFlowQueue, crate::tcp_downlink_pump::OutputStock, Arc<crate::tcp_downlink_pump::tests::native_waiters::Facts>) {
    let budget = BackingBudget::new(65536).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (clock, facts) = crate::tcp_downlink_pump::tests::native_waiters::clock(&meter);
    let (queue, stock) = AsyncLeasedByteFlowQueue::new_with_progressive_output_stock(8, mode, &meter, clock, 8, 4).unwrap();
    (budget, queue, stock, facts)
}
fn payload(read: &mut StockReadOperation, bytes: &[u8]) -> Option<OwnedPayload> {
    read.space().as_mut().unwrap().write(bytes).unwrap();
    Some(OwnedPayload::Native(read.space().take().unwrap().finish().unwrap()))
}

#[tokio::test(flavor = "current_thread")]
async fn leased_original_grant_close_tail_preserves_reserved_queued_and_actual_permit_debt() {
    for mode in [DownstreamPermitReleaseMode::OnAdmission, DownstreamPermitReleaseMode::OnEgressDrain] {
        let (budget, queue, mut stock, _facts) = queue(mode).await;
        let mut producer = stock.take_producer().unwrap();
        let mut grant = None;
        producer.prepare_read(8, &mut grant).await.unwrap();
        let mut read = grant.take().unwrap();
        assert_eq!(read.capacity(), 4); // Real bootstrap, no larger unbacked grant.
        assert_eq!(queue.snapshot_now().reserved_bytes, 4);
        let mut pending = payload(&mut read, b"tail");
        queue.close().await;
        assert!(queue.dispose_data().unwrap().is_none());
        let mut held = None;
        let mut receiving = Box::pin(queue.recv_custodied(8, &mut held));
        assert!(receiving.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());
        read.publish(&mut pending).await.unwrap();
        assert!(pending.is_none());
        let published = queue.snapshot_now();
        assert_eq!((published.reserved_bytes, published.queued_bytes, published.leased_bytes), (0, 4, 0));
        assert!(receiving.await.unwrap());
        let batch = held.take().unwrap();
        assert_eq!(batch.data.bytes.as_ref(), b"tail");
        assert_eq!(batch.permit.release_mode(), mode);
        let loan = batch.data.bytes.clone();
        let allocation = loan.allocation_identity();
        drop((batch.data, read, producer));
        let data = queue.dispose_data().unwrap().unwrap();
        assert_eq!((data.custody().emitted, data.custody().consumed), (4, 4));
        assert!(!queue.physical_zero());
        assert_eq!(queue.snapshot_now().leased_bytes, 4);
        let charged = budget.used();
        let mut wait = Box::pin(stock.wait_returned());
        assert!(wait.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());
        assert_eq!(loan.allocation_identity(), allocation);
        drop(loan);
        assert_eq!(budget.used(), charged, "last loan returns availability only");
        wait.await.unwrap();
        assert!(stock.try_retire());
        assert!(!queue.physical_zero(), "actual permit still owns protocol debt after slab retirement");
        assert_eq!(queue.snapshot_now().leased_bytes, 4);
        drop(batch.permit);
        assert_eq!(queue.snapshot_now().leased_bytes, 0);
        assert!(queue.dispose_custodied().unwrap().is_some());
        assert!(queue.physical_zero());
        drop(queue);
        assert_eq!(budget.used(), 0);
        assert!(!budget.quarantined());
    }
}

#[tokio::test(flavor = "current_thread")]
async fn leased_read_cancel_and_full_credit_wait_preserve_original_owner() {
    let (budget, queue, mut stock, _facts) = queue(DownstreamPermitReleaseMode::OnEgressDrain).await;
    let mut producer = stock.take_producer().unwrap();
    let mut grant = None;
    producer.prepare_read(4, &mut grant).await.unwrap();
    let mut successor = None;
    assert!(matches!(producer.prepare_read(4, &mut successor).await, Err(StockWaitError::Pressure)));
    assert_eq!(queue.snapshot_now().reserved_bytes, 4);
    drop(async move { drop(grant); }); // Zero-poll cancellation destroys the actual grant.
    assert_eq!(queue.snapshot_now().reserved_bytes, 0);
    let mut held = None;
    let mut permits = Vec::new();
    for _ in 0..2 {
        producer.prepare_read(4, &mut successor).await.unwrap();
        let mut read = successor.take().unwrap();
        let mut pending = payload(&mut read, b"data");
        read.publish(&mut pending).await.unwrap();
        drop(read);
        assert!(queue.recv_custodied(4, &mut held).await.unwrap());
        let batch = held.take().unwrap();
        drop(batch.data); // Physical loan returned; credit remains leased.
        permits.push(batch.permit);
    }
    assert_eq!((queue.snapshot_now().leased_bytes, queue.snapshot_now().reserved_bytes), (8, 0));
    let charged = budget.used();
    let mut waiting = Box::pin(producer.prepare_read(4, &mut successor));
    assert!(waiting.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());
    assert_eq!(budget.used(), charged);
    drop(permits.pop()); // Actual original permit returns credit, not slab funding.
    waiting.await.unwrap();
    assert_eq!(queue.snapshot_now().reserved_bytes, 4);
    drop(successor.take());
    drop(permits);
    drop(producer);
    queue.close().await;
    assert!(queue.dispose_data().unwrap().is_some());
    stock.wait_returned().await.unwrap();
    assert!(stock.try_retire());
    assert!(queue.dispose_custodied().unwrap().is_some());
    assert!(queue.physical_zero());
    drop(queue);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

impl AsyncLeasedByteFlowQueue {
    // Stable current-thread fixture observation, not a DATA hot-path audit.
    pub(crate) fn assert_stock_credit_for_test(&self, snapshot: LeasedByteQueueSnapshot) {
        assert_eq!(snapshot, self.snapshot_now());
        assert!(snapshot.owned_bytes() <= snapshot.capacity_bytes);
        if snapshot.reserved_bytes > 0 && self.has_output_stock() {
            let (id, max) = self.inner.read.active_for_test();
            assert!(id > 0);
            assert_eq!(max, snapshot.reserved_bytes);
            let owner = StockReturn(Arc::downgrade(&self.inner)).upgrade().unwrap();
            self.inner.stock.get().unwrap().assert_leased_pending_credit_for_test(
                &owner, self.inner.meter.get().unwrap(), max);
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn leased_descriptor_release_wakes_without_returning_payload_or_permit() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct WakeCount(AtomicUsize);
    impl std::task::Wake for WakeCount {
        fn wake(self: Arc<Self>) { self.wake_by_ref(); }
        fn wake_by_ref(self: &Arc<Self>) { self.0.fetch_add(1, Ordering::SeqCst); }
    }
    let budget = BackingBudget::new(65536).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (clock, _facts) = crate::tcp_downlink_pump::tests::native_waiters::clock(&meter);
    let (queue, mut stock) = AsyncLeasedByteFlowQueue::new_with_progressive_output_stock(
        32, DownstreamPermitReleaseMode::OnEgressDrain, &meter, clock, 32, 16).unwrap();
    let mut producer = stock.take_producer().unwrap();
    for _ in 0..4 {
        let mut grant = None;
        producer.prepare_read(1, &mut grant).await.unwrap();
        let mut read = grant.unwrap();
        let mut pending = payload(&mut read, b"x");
        read.publish(&mut pending).await.unwrap();
    }
    assert_eq!(queue.inner.lock_state().chunks.len(), 4);
    assert_eq!(queue.snapshot_now().queued_bytes, 4);
    let header = bytes::BytesMut::metered_copy_layout_for_len(1).unwrap() - 1;
    let pressure = bytes::BytesMut::try_zeroed_metered(budget.limit() - budget.used() - header, &meter).unwrap();
    assert_eq!(budget.used(), budget.limit());
    let wakes = Arc::new(WakeCount(AtomicUsize::new(0)));
    let waker = Waker::from(wakes.clone());
    let mut grant = None;
    let mut waiting = Box::pin(producer.prepare_read(1, &mut grant));
    assert!(waiting.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());
    let before = wakes.0.load(Ordering::SeqCst);
    let mut held = None;
    assert!(queue.recv_custodied(1, &mut held).await.unwrap());
    assert!(wakes.0.load(Ordering::SeqCst) > before, "actual descriptor release must wake the original producer with payload and permit still held");
    assert!(matches!(waiting.as_mut().poll(&mut Context::from_waker(&waker)), std::task::Poll::Ready(Ok(()))));
    drop(waiting);
    assert_eq!((queue.snapshot_now().leased_bytes, queue.snapshot_now().reserved_bytes), (1, 1));
    assert_eq!(budget.used(), budget.limit());
    drop((grant, held, pressure, producer));
    queue.close().await;
    assert!(queue.dispose_data().unwrap().is_some());
    stock.wait_returned().await.unwrap();
    assert!(stock.try_retire());
    assert!(queue.dispose_custodied().unwrap().is_some());
    drop(queue);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[tokio::test(flavor = "current_thread")]
async fn leased_raw_foreign_stock_marks_both_owners_before_original_quiet() {
    for sealed in [false, true] { raw_refusal(sealed).await; }
}
async fn raw_refusal(sealed: bool) {
    use crate::shared::errors::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    struct Observe { receiver: BackingBudget, saw: AtomicBool, calls: AtomicUsize }
    impl QuietFailureSink for Observe {
        fn record_quiet(&self, _: RetainedFailure) {}
        fn quarantine_quiet(&self) {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                self.saw.store(self.receiver.quarantined(), Ordering::SeqCst);
            }
        }
    }
    let (budget_a, queue_a, stock_a, facts_a) = queue(DownstreamPermitReleaseMode::OnAdmission).await;
    let stock_a = if sealed {
        drop(stock_a);
        assert!(queue_a.dispose_custodied().unwrap().is_some());
        assert!(!queue_a.inner.waiters.get().unwrap().stock_bound());
        None
    } else { Some(stock_a) };
    let budget_b = BackingBudget::new(65536).unwrap();
    let meter_b = BackingMeter::with_budget(&budget_b);
    let observed = Arc::new(Observe { receiver: budget_a.clone(), saw: AtomicBool::new(false), calls: AtomicUsize::new(0) });
    let sink: Arc<dyn QuietFailureSink> = observed.clone();
    let clock = Arc::new(FailureOrder::default());
    clock.bind(Arc::downgrade(&sink), FailureOrigin { phase: FailurePhase::TcpOpen, resource: FailureResource::TcpOpen, resource_id: 1, attempt_id: Some(1) }, Some(meter_b.clone())).unwrap();
    let (queue_b, mut stock_b) = AsyncLeasedByteFlowQueue::new_with_progressive_output_stock(
        8, DownstreamPermitReleaseMode::OnAdmission, &meter_b, clock, 8, 4).unwrap();
    let mut producer = stock_b.take_producer().unwrap();
    let mut space = producer.try_prepare(4).unwrap().unwrap();
    space.write(b"same").unwrap();
    let bytes = space.finish().unwrap();
    let identity = bytes.allocation_identity();
    let mut pending = Some(OwnedPayload::Native(bytes));
    assert_eq!(queue_a.push_custodied(&mut pending).await, Err(ByteCustodyError::Invalid));
    assert!(observed.calls.load(Ordering::SeqCst) > 0);
    assert!(observed.saw.load(Ordering::SeqCst), "payload quiet must observe receiver already Q even with an independent budget");
    let Some(OwnedPayload::Native(bytes)) = pending.as_ref() else { panic!("original pending retained") };
    assert_eq!(bytes.allocation_identity(), identity);
    assert_eq!(bytes.as_ref(), b"same");
    assert!(budget_a.quarantined() && budget_b.quarantined());
    assert!(facts_a.1.load(Ordering::SeqCst) > 0, "original receiver quiet survives actual callback seal");
    assert!(budget_a.used() > 0 && budget_b.used() > 0);
    drop((pending, producer, stock_b, queue_b, stock_a, queue_a));
}

#[test]
fn leased_actual_cold_cost_exact_and_one_byte_short_rollback() {
    for (capacity, mode) in [(409090, DownstreamPermitReleaseMode::OnEgressDrain), (4 << 20, DownstreamPermitReleaseMode::OnAdmission)] {
        let construct = |limit| {
            let budget = BackingBudget::new(limit).unwrap();
            let meter = BackingMeter::with_budget(&budget);
            let (clock, facts) = crate::tcp_downlink_pump::tests::native_waiters::clock(&meter);
            let result = AsyncLeasedByteFlowQueue::new_with_progressive_output_stock(capacity, mode, &meter, clock, 512 << 10, 64 << 10);
            (budget, meter, facts, result)
        };
        let (budget, meter, _, result) = construct(1 << 20);
        let (queue, stock) = result.unwrap();
        let bill = budget.used();
        let shape = meter.snapshot();
        assert_eq!(shape.allocations, 1);
        assert_eq!(shape.payload_bytes, 64 << 10);
        eprintln!("[N1-Leased-cold] capacity={capacity} mode={mode:?} actual_cold={bill} payload={shape:?} queue_instance_header={} waiters_cold={} stock={:?} F4096_cold={} two_generations_cold={}",
            std::mem::size_of::<AsyncLeasedByteFlowQueueInner>(), crate::tcp_downlink_pump::native_wait::Waiters::layout_bytes(),
            stock.key().unwrap().inventory_for_test(), bill * 4096, bill * 4096 * 2);
        drop((stock, queue));
        assert_eq!(budget.used(), 0); assert!(!budget.quarantined());
        for limit in [bill - 1, bill] {
            let (budget, _, _, result) = construct(limit);
            if limit < bill { assert!(matches!(result, Err(bytes::MeteredCopyError::BudgetFull))); }
            else { let (queue, stock) = result.unwrap(); assert_eq!(budget.used(), bill); drop((stock, queue)); }
            assert_eq!(budget.used(), 0, "partial cold construction must destroy all actual stock");
            assert!(!budget.quarantined());
        }
    }
}

#[test]
fn leased_finished_callback_control_keeps_original_clock_until_queue_drop() {
    let budget = BackingBudget::new(65536).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (clock, facts) = crate::tcp_downlink_pump::tests::native_waiters::clock(&meter);
    let weak = Arc::downgrade(&clock);
    let (queue, stock) = AsyncLeasedByteFlowQueue::new_with_progressive_output_stock(
        8, DownstreamPermitReleaseMode::OnAdmission, &meter, clock, 8, 4).unwrap();
    drop(stock);
    assert!(queue.dispose_custodied().unwrap().is_some());
    let waiters = queue.inner.waiters.get().unwrap();
    assert!(!waiters.stock_bound()); // Control and its callbacks actually retired.
    assert!(waiters.finish());
    let original = weak.upgrade().unwrap();
    assert!(Arc::ptr_eq(&original, &waiters.bound_clock().unwrap()));
    drop(original);
    assert!(budget.used() > 0 && facts.0.lock().unwrap().is_empty());
    drop(queue);
    assert!(weak.upgrade().is_none());
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}
