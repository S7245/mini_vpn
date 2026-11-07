//! Original dual quiet publication with passive owner-lock probes.
use super::*;
use super::super::Root;
use crate::{shared::errors::*, tcp_downlink_pump::AsyncByteBoundedFlowQueueInner};
use std::sync::{OnceLock, Weak, atomic::AtomicBool};

struct Probe {
    queue: OnceLock<Weak<AsyncByteBoundedFlowQueueInner>>,
    root: OnceLock<Weak<Root>>,
    calls: AtomicUsize,
    unlocked: AtomicBool,
    deadline: tokio::time::Instant,
}
impl Probe {
    fn new(deadline: tokio::time::Instant) -> Arc<Self> {
        Arc::new(Self { queue: OnceLock::new(), root: OnceLock::new(), calls: AtomicUsize::new(0), unlocked: AtomicBool::new(true), deadline })
    }
    fn attach(&self, queue: &AsyncByteBoundedFlowQueue, stock: &super::super::OutputStock) {
        assert!(self.queue.set(Arc::downgrade(&queue.inner)).is_ok());
        assert!(self.root.set(Arc::downgrade(stock.root.as_ref().unwrap())).is_ok());
    }
    fn assert_published(&self) {
        assert!(self.calls.load(Ordering::SeqCst) > 0);
        assert!(self.unlocked.load(Ordering::SeqCst), "quiet must run after both original owner locks release");
    }
}
impl QuietFailureSink for Probe {
    fn record_quiet(&self, _: RetainedFailure) { panic!("stock Q must not manufacture an IO/panic ordinal"); }
    fn quarantine_quiet(&self) {
        if let Some(queue) = self.queue.get().and_then(Weak::upgrade) {
            if queue.state.try_lock().is_err() { self.unlocked.store(false, Ordering::SeqCst); }
        }
        if let Some(root) = self.root.get().and_then(Weak::upgrade) {
            if root.body.state.try_lock().is_err() { self.unlocked.store(false, Ordering::SeqCst); }
        }
        self.calls.fetch_add(1, Ordering::SeqCst);
    }
    fn deadline_quiet(&self) -> Option<tokio::time::Instant> { Some(self.deadline) }
}
fn original_clock(meter: &BackingMeter) -> (FailureClock, Arc<Probe>, Arc<Probe>) {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    let owner = Probe::new(deadline);
    let parent = Probe::new(deadline);
    let owner_sink: Arc<dyn QuietFailureSink> = owner.clone();
    let parent_sink: Arc<dyn QuietFailureSink> = parent.clone();
    let clock = Arc::new(FailureOrder::default());
    clock.bind_with_parent(Arc::downgrade(&owner_sink), Some(Arc::downgrade(&parent_sink)), FailureOrigin {
        phase: FailurePhase::TcpOpen, resource: FailureResource::TcpOpen,
        resource_id: 81, attempt_id: Some(9),
    }, Some(meter.clone())).unwrap();
    (clock, owner, parent)
}

#[test]
fn actual_stock_owner_loss_publishes_both_original_sinks_outside_their_data_locks() {
    let budget = BackingBudget::new(65536).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (clock, owner, parent) = original_clock(&meter);
    let (queue, mut stock) = AsyncByteBoundedFlowQueue::new_with_output_stock(4, &meter, clock.clone(), 8, 4).unwrap();
    owner.attach(&queue, &stock); parent.attach(&queue, &stock);
    let producer = stock.take_producer().unwrap();
    drop(stock);
    owner.assert_published(); parent.assert_published();
    assert!(budget.quarantined());
    assert!(budget.used() > 0);
    assert_eq!(clock.parent_deadline_for_stop(), Some(parent.deadline));
    drop((producer, queue));
}

#[tokio::test(flavor = "current_thread")]
async fn actual_typed_disposal_keeps_unknown_loan_positive_and_publishes_quiet_lock_free() {
    use crate::tcp_downlink_pump::ByteCustodyError;
    let budget = BackingBudget::new(65536).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (clock, owner, parent) = original_clock(&meter);
    let (queue, mut stock) = AsyncByteBoundedFlowQueue::new_with_output_stock(4, &meter, clock.clone(), 8, 4).unwrap();
    owner.attach(&queue, &stock); parent.attach(&queue, &stock);
    let mut producer = stock.take_producer().unwrap();
    let mut space = producer.try_prepare(4).unwrap().unwrap();
    space.write(b"tail").unwrap();
    queue.push_stock(&producer, &mut Some(OwnedPayload::Native(space.finish().unwrap()))).await.unwrap();
    drop(producer);
    // Corrupt one original physical ledger witness while retaining its actual
    // value. This is an invalid-inventory tracer, never a production proof.
    stock.root.as_ref().unwrap().lock().unwrap().slab_mut(0).unwrap().generation += 1;
    assert!(matches!(queue.dispose_custodied(), Err(ByteCustodyError::Invalid)));
    owner.assert_published(); parent.assert_published();
    assert!(budget.quarantined());
    assert_eq!(queue.inner.waiters.get().unwrap().disposal_state(), 1);
    assert!(queue.inner.state.try_lock().unwrap().custody.interrupted);
    assert!(!stock.try_retire());
    assert!(budget.used() > 0);
    assert_eq!(clock.parent_deadline_for_stop(), Some(parent.deadline));
}

#[test]
fn live_producer_original_queue_loss_publishes_quiet_without_a_later_root_drop() {
    use crate::tcp_downlink_pump::output_stock::StockWaitError;
    let budget = BackingBudget::new(65536).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (clock, owner, parent) = original_clock(&meter);
    let (queue, mut stock) = AsyncByteBoundedFlowQueue::new_with_output_stock(4, &meter, clock.clone(), 8, 4).unwrap();
    owner.attach(&queue, &stock); parent.attach(&queue, &stock);
    let mut producer = stock.take_producer().unwrap();
    drop(queue); // Writer is actually alive; this is not a joined late return.
    let mut waiting = Box::pin(producer.wait_prepare(4));
    assert!(matches!(waiting.as_mut().poll(&mut Context::from_waker(Waker::noop())), Poll::Ready(Err(StockWaitError::Admission(bytes::MeteredCopyError::Quarantined)))));
    drop(waiting);
    assert!(budget.quarantined());
    owner.assert_published(); parent.assert_published();
    assert!(budget.used() > 0);
    assert_eq!(clock.parent_deadline_for_stop(), Some(parent.deadline));
    drop(producer);
    assert!(!stock.try_retire());
}

#[tokio::test(flavor = "current_thread")]
async fn foreign_producer_port_keeps_pending_and_quarantines_both_original_owners() {
    use crate::tcp_downlink_pump::ByteCustodyError;
    let first_budget = BackingBudget::new(65536).unwrap();
    let second_budget = BackingBudget::new(65536).unwrap();
    let first_meter = BackingMeter::with_budget(&first_budget);
    let second_meter = BackingMeter::with_budget(&second_budget);
    let (first_clock, first_facts) = crate::tcp_downlink_pump::tests::native_waiters::clock(&first_meter);
    let (second_clock, second_facts) = crate::tcp_downlink_pump::tests::native_waiters::clock(&second_meter);
    let (_first_queue, mut first_stock) = AsyncByteBoundedFlowQueue::new_with_output_stock(4, &first_meter, first_clock, 8, 4).unwrap();
    let (second_queue, mut second_stock) = AsyncByteBoundedFlowQueue::new_with_output_stock(4, &second_meter, second_clock, 8, 4).unwrap();
    let mut producer = first_stock.take_producer().unwrap();
    let mut space = producer.try_prepare(4).unwrap().unwrap();
    space.write(b"keep").unwrap();
    let bytes = space.finish().unwrap();
    let original = bytes.allocation_identity();
    let mut pending = Some(OwnedPayload::Native(bytes));
    assert_eq!(second_queue.push_stock(&producer, &mut pending).await, Err(ByteCustodyError::Invalid));
    assert!(first_budget.quarantined());
    assert!(second_budget.quarantined(), "receiver cannot silently remain usable after invalid port authority");
    assert!(first_facts.1.load(Ordering::SeqCst) > 0, "source Q requires its original quiet publication");
    assert!(second_facts.1.load(Ordering::SeqCst) > 0);
    let Some(OwnedPayload::Native(bytes)) = pending.as_ref() else { panic!("original typed value must remain"); };
    assert_eq!(bytes.as_ref(), b"keep");
    assert_eq!(bytes.allocation_identity(), original);
    assert_eq!(second_queue.custody_snapshot().await.0.queued_bytes, 0);
    drop((pending, producer));
    assert!(!first_stock.try_retire());
    assert!(!second_stock.try_retire());
    assert!(first_budget.used() > 0 && second_budget.used() > 0);
}

#[tokio::test(flavor = "current_thread")]
async fn third_pending_root_rejection_publishes_its_original_quiet_and_keeps_loan() {
    use crate::tcp_downlink_pump::ByteCustodyError;
    let budgets: [_; 3] = std::array::from_fn(|_| BackingBudget::new(65536).unwrap());
    let meters = budgets.each_ref().map(BackingMeter::with_budget);
    let (clock_a, facts_a) = crate::tcp_downlink_pump::tests::native_waiters::clock(&meters[0]);
    let (clock_b, facts_b) = crate::tcp_downlink_pump::tests::native_waiters::clock(&meters[1]);
    let (clock_c, facts_c) = crate::tcp_downlink_pump::tests::native_waiters::clock(&meters[2]);
    let (_queue_a, mut stock_a) = AsyncByteBoundedFlowQueue::new_with_output_stock(4, &meters[0], clock_a, 8, 4).unwrap();
    let (queue_b, mut stock_b) = AsyncByteBoundedFlowQueue::new_with_output_stock(4, &meters[1], clock_b, 8, 4).unwrap();
    let (_queue_c, mut stock_c) = AsyncByteBoundedFlowQueue::new_with_output_stock(4, &meters[2], clock_c, 8, 4).unwrap();
    let producer_a = stock_a.take_producer().unwrap();
    let mut producer_c = stock_c.take_producer().unwrap();
    let mut space = producer_c.try_prepare(4).unwrap().unwrap();
    space.write(b"root").unwrap();
    let payload = space.finish().unwrap();
    let identity = payload.allocation_identity();
    let mut pending = Some(OwnedPayload::Native(payload));
    assert_eq!(queue_b.push_stock(&producer_a, &mut pending).await, Err(ByteCustodyError::Invalid));
    for budget in &budgets { assert!(budget.quarantined()); assert!(budget.used() > 0); }
    for facts in [&facts_a, &facts_b, &facts_c] {
        assert!(facts.1.load(Ordering::SeqCst) > 0, "each original Root needs its own quiet publication");
        assert!(facts.0.lock().unwrap().is_empty(), "invalid port must not invent an IO ordinal");
    }
    let Some(OwnedPayload::Native(payload)) = pending.as_ref() else { panic!("original pending was consumed"); };
    assert_eq!(payload.as_ref(), b"root");
    assert_eq!(payload.allocation_identity(), identity);
    assert_eq!(queue_b.custody_snapshot().await.0.queued_bytes, 0);
    assert!(!stock_a.try_retire() && !stock_b.try_retire() && !stock_c.try_retire());
    drop((pending, producer_a, producer_c));
}
