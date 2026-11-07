//! Original queue control/role seams, including actual Native cold binding.
use super::*;
use crate::shared::errors::*;
use std::{
    future::Future,
    sync::{Arc, Mutex as StdMutex},
    task::{Context, Waker},
};
#[derive(Default)]
pub(in crate::tcp_downlink_pump) struct Facts(
    pub(in crate::tcp_downlink_pump) StdMutex<Vec<RetainedFailure>>,
    pub(in crate::tcp_downlink_pump) std::sync::atomic::AtomicUsize,
);
impl QuietFailureSink for Facts {
    fn quarantine_quiet(&self) { self.1.fetch_add(1, std::sync::atomic::Ordering::SeqCst); }
    fn record_quiet(&self, f: RetainedFailure) {
        self.0.lock().unwrap().push(f);
    }
}
pub(in crate::tcp_downlink_pump) fn clock(meter: &bytes::BackingMeter) -> (FailureClock, Arc<Facts>) {
    let facts = Arc::new(Facts::default());
    let sink: Arc<dyn QuietFailureSink> = facts.clone();
    let clock = Arc::new(FailureOrder::default());
    clock
        .bind(
            Arc::downgrade(&sink),
            FailureOrigin {
                phase: FailurePhase::TcpOpen,
                resource: FailureResource::TcpOpen,
                resource_id: 8,
                attempt_id: Some(2),
            },
            Some(meter.clone()),
        )
        .unwrap();
    (clock, facts)
}
#[tokio::test]
async fn actual_full_queue_close_does_not_compete_for_producer_wait_role() {
    let budget = bytes::BackingBudget::new(65536).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let q = AsyncByteBoundedFlowQueue::new(4).unwrap();
    q.bind_native_meter(Some(meter.clone())).unwrap();
    let (clock, _facts) = clock(&meter);
    assert!(q.bind_native_failures(clock));
    let mut pending = Some(OwnedPayload::Native((Bytes::copy_from_slice_metered(b"abcd", &meter).unwrap()).into()));
    q.push_custodied(&mut pending).await.unwrap();
    let mut producer = Box::pin(q.wait_read_len(1));
    assert!(
        producer
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    q.close().await;
    assert!(
        !budget.quarantined(),
        "normal close is not a second producer"
    );
    assert_eq!(producer.await, Err(AsyncByteQueueWait::Closed));
    let mut held = None;
    assert_eq!(q.recv_custodied(4, &mut held).await, Ok(true));
    assert_eq!(held.as_ref().unwrap().bytes.as_ref(), b"abcd");
    drop(held.take());
    assert_eq!(q.recv_custodied(4, &mut held).await, Ok(false));
    assert!(q.dispose_custodied().unwrap().is_some());
    drop(q);
    assert_eq!(budget.used(), 0);
}

#[tokio::test]
async fn cold_bind_rejects_preexisting_raw_waiter_and_read_reservation() {
    let budget = bytes::BackingBudget::new(65536).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let q = AsyncByteBoundedFlowQueue::new(4).unwrap();
    let mut recv = Box::pin(q.recv_up_to(4));
    assert!(
        recv.as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert!(q.bind_native_meter(Some(meter.clone())).is_none());
    drop(recv);
    assert!(
        q.bind_native_meter(Some(meter.clone())).is_none(),
        "cancel cannot undo first use"
    );
    let leased = AsyncLeasedByteFlowQueue::new(4).unwrap();
    let reservation = leased.reserve_read_up_to(4).await.unwrap();
    assert!(leased.bind_native_meter(Some(meter.clone())).is_none());
    drop(reservation);
    assert_eq!(budget.used(), 0, "reject before any native cold allocation");
    assert!(!budget.quarantined());
}

#[tokio::test]
async fn native_queue_requires_original_clock_before_work_and_rejects_foreign_budget() {
    let budget = bytes::BackingBudget::new(65536).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let q = AsyncByteBoundedFlowQueue::new(4).unwrap();
    q.bind_native_meter(Some(meter.clone())).unwrap();
    let mut pending = Some(OwnedPayload::Native((Bytes::copy_from_slice_metered(b"abcd", &meter).unwrap()).into()));
    assert_eq!(
        q.push_custodied(&mut pending).await,
        Err(ByteCustodyError::Invalid)
    );
    assert!(pending.is_some());
    assert!(budget.quarantined());
    assert_eq!(q.inner.state.lock().await.queue.queued_bytes, 0);
    drop((pending, q));
    assert_eq!(budget.used(), native_wait::Waiters::layout_bytes());

    let foreign = bytes::BackingBudget::new(65536).unwrap();
    let foreign_meter = bytes::BackingMeter::with_budget(&foreign);
    let (clock, _facts) = clock(&foreign_meter);
    let own = bytes::BackingBudget::new(65536).unwrap();
    let own_meter = bytes::BackingMeter::with_budget(&own);
    let q = AsyncLeasedByteFlowQueue::new_custodied(4).unwrap();
    q.bind_native_meter(Some(own_meter)).unwrap();
    assert!(!q.bind_native_failures(clock));
    assert!(own.quarantined());
    assert!(!foreign.quarantined());
}

#[tokio::test]
async fn disposed_queue_late_capacity_wait_is_known_closed() {
    let budget = bytes::BackingBudget::new(65536).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let q = AsyncByteBoundedFlowQueue::new(4).unwrap();
    q.bind_native_meter(Some(meter.clone())).unwrap();
    let (clock, _facts) = clock(&meter);
    assert!(q.bind_native_failures(clock));
    q.close().await;
    assert!(q.dispose_custodied().unwrap().is_some());
    assert_eq!(q.wait_read_len(4).await, Err(AsyncByteQueueWait::Closed));
    let mut pending = Some(OwnedPayload::Native((Bytes::copy_from_slice_metered(b"abcd", &meter).unwrap()).into()));
    assert_eq!(
        q.push_custodied(&mut pending).await,
        Err(ByteCustodyError::Rejected)
    );
    assert!(pending.is_some());
    assert!(!budget.quarantined());
    drop((pending, q));
    assert_eq!(budget.used(), 0);
}

struct BlockWake {
    entered: std::sync::Barrier,
    resume: std::sync::Barrier,
}
impl std::task::Wake for BlockWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.entered.wait();
        self.resume.wait();
    }
}
#[tokio::test]
async fn cancelled_producer_and_blocked_original_wake_hold_stock_until_return() {
    let budget = bytes::BackingBudget::new(65536).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let q = AsyncByteBoundedFlowQueue::new(4).unwrap();
    q.bind_native_meter(Some(meter.clone())).unwrap();
    let (clock, _facts) = clock(&meter);
    assert!(q.bind_native_failures(clock));
    q.push_custodied(&mut Some(OwnedPayload::Native((Bytes::copy_from_slice_metered(b"abcd", &meter).unwrap()).into())))
    .await
    .unwrap();
    let block = Arc::new(BlockWake {
        entered: std::sync::Barrier::new(2),
        resume: std::sync::Barrier::new(2),
    });
    let waker = Waker::from(block.clone());
    let mut producer = Box::pin(q.wait_read_len(1));
    assert!(
        producer
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    let closing = q.clone();
    let thread = std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(closing.close())
    });
    block.entered.wait();
    drop(producer);
    let pending = q.dispose_custodied();
    let charged = budget.used();
    block.resume.wait(); // Always unblock before making an assertion.
    thread.join().unwrap();
    assert!(matches!(pending, Ok(None)));
    assert!(charged >= native_wait::Waiters::retained_role_bytes());
    assert!(!budget.quarantined());
    assert!(q.dispose_custodied().unwrap().is_some());
    drop(q);
    assert_eq!(budget.used(), 0);
}

#[test]
fn fixed_native_byte_head_and_exact_callback_box_layouts_are_observable() {
    eprintln!(
        "Native fixed Byte queue head: {} bytes, at most one per Flow; callback real cold Boxes: {} bytes",
        std::mem::size_of::<AsyncByteBoundedFlowQueueInner>(),
        native_wait::Waiters::layout_bytes()
    );
}



struct CountRetirementWake(std::sync::atomic::AtomicUsize);
impl std::task::Wake for CountRetirementWake {
    fn wake(self: Arc<Self>) { self.wake_by_ref(); }
    fn wake_by_ref(self: &Arc<Self>) { self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst); }
}

#[tokio::test(flavor = "current_thread")]
async fn completed_work_cannot_reuse_original_role_while_old_wake_is_in_flight() {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::task::Poll;
    let budget = bytes::BackingBudget::new(65536).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let (clock, _facts) = clock(&meter);
    let waiters = native_wait::Waiters::prepare(&meter).unwrap();
    assert!(waiters.bind(clock));
    let ready = AtomicBool::new(false);
    let proxy = StdMutex::new(None::<Waker>);
    let block = Arc::new(BlockWake {
        entered: std::sync::Barrier::new(2), resume: std::sync::Barrier::new(2),
    });
    let waker = Waker::from(block.clone());
    let mut first = Box::pin(waiters.run(native_wait::Role::Producer, std::future::poll_fn(|cx| {
        if ready.load(Ordering::Acquire) { Poll::Ready(()) }
        else { *proxy.lock().unwrap() = Some(cx.waker().clone()); Poll::Pending }
    })));
    assert!(first.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());
    ready.store(true, Ordering::Release);
    let wake = proxy.lock().unwrap().take().unwrap();
    let thread = std::thread::spawn(move || wake.wake());
    block.entered.wait();
    let first_result = first.as_mut().poll(&mut Context::from_waker(Waker::noop()));
    drop(first);
    let calls = AtomicUsize::new(0);
    let mut second = Box::pin(waiters.run(native_wait::Role::Producer, async {
        calls.fetch_add(1, Ordering::SeqCst);
    }));
    let retirement = Arc::new(CountRetirementWake(AtomicUsize::new(0)));
    let retirement_waker = Waker::from(retirement.clone());
    let second_result = second.as_mut().poll(&mut Context::from_waker(&retirement_waker));
    let premature = calls.load(Ordering::SeqCst);
    let was_q = budget.quarantined();
    block.resume.wait(); // Unblock the actual callback before any assertion.
    thread.join().unwrap();
    assert!(matches!(first_result, Poll::Ready(Ok(()))));
    assert!(second_result.is_pending(), "callback in flight keeps the original role unavailable");
    assert_eq!(premature, 0, "next work must not poll until actual old Wake and Drop finish");
    assert!(!was_q, "legal scheduling overlap is pressure, not Q");
    assert!(retirement.0.load(Ordering::SeqCst) > 0, "actual callback exit must wake the next acquisition without a timer or new DATA");
    assert!(matches!(second.as_mut().poll(&mut Context::from_waker(Waker::noop())), Poll::Ready(Ok(()))));
    drop(second);
    assert!(waiters.finish());
    drop(waiters);
    assert!(!budget.quarantined());
    assert_eq!(budget.used(), 0);
}


#[tokio::test(flavor = "current_thread")]
async fn retiring_original_role_has_exactly_one_unpolled_acquisition_claim() {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::task::Poll;
    let budget = bytes::BackingBudget::new(65536).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let (clock, _facts) = clock(&meter);
    let waiters = native_wait::Waiters::prepare(&meter).unwrap();
    assert!(waiters.bind(clock));
    let ready = AtomicBool::new(false);
    let proxy = StdMutex::new(None::<Waker>);
    let block = Arc::new(BlockWake {
        entered: std::sync::Barrier::new(2), resume: std::sync::Barrier::new(2),
    });
    let waker = Waker::from(block.clone());
    let mut first = Box::pin(waiters.run(native_wait::Role::Producer, std::future::poll_fn(|cx| {
        if ready.load(Ordering::Acquire) { Poll::Ready(()) }
        else { *proxy.lock().unwrap() = Some(cx.waker().clone()); Poll::Pending }
    })));
    assert!(first.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());
    ready.store(true, Ordering::Release);
    let wake = proxy.lock().unwrap().take().unwrap();
    let thread = std::thread::spawn(move || wake.wake());
    block.entered.wait();
    let completed = first.as_mut().poll(&mut Context::from_waker(Waker::noop()));
    drop(first);
    let calls = AtomicUsize::new(0);
    let mut second = Box::pin(waiters.run(native_wait::Role::Producer, async { calls.fetch_add(1, Ordering::SeqCst); }));
    let mut third = Box::pin(waiters.run(native_wait::Role::Producer, async { calls.fetch_add(1, Ordering::SeqCst); }));
    let second_result = second.as_mut().poll(&mut Context::from_waker(Waker::noop()));
    let third_result = third.as_mut().poll(&mut Context::from_waker(Waker::noop()));
    let q = budget.quarantined();
    block.resume.wait();
    thread.join().unwrap();
    assert!(matches!(completed, Poll::Ready(Ok(()))));
    assert!(second_result.is_pending());
    assert!(matches!(third_result, Poll::Ready(Err(native_wait::RunError::Quarantined))), "second independent acquisition cannot replace the unique owner's waker");
    assert!(q);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    drop((second, third));
    assert!(!waiters.finish());
    assert!(budget.used() > 0);
}

struct UnpolledDrop {
    original: Arc<usize>,
    polled: Arc<std::sync::atomic::AtomicUsize>,
}
impl Future for UnpolledDrop {
    type Output = ();
    fn poll(self: std::pin::Pin<&mut Self>, _: &mut Context<'_>) -> std::task::Poll<()> {
        self.polled.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        std::task::Poll::Pending
    }
}
impl Drop for UnpolledDrop {
    fn drop(&mut self) { std::panic::panic_any(self.original.clone()); }
}
#[tokio::test(flavor = "current_thread")]
async fn cancelled_unpolled_acquisition_keeps_original_work_drop_box_and_quiet() {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::task::Poll;
    let budget = bytes::BackingBudget::new(65536).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let (clock, facts) = clock(&meter);
    let waiters = native_wait::Waiters::prepare(&meter).unwrap();
    assert!(waiters.bind(clock));
    let ready = AtomicBool::new(false);
    let proxy = StdMutex::new(None::<Waker>);
    let block = Arc::new(BlockWake { entered: std::sync::Barrier::new(2), resume: std::sync::Barrier::new(2) });
    let waker = Waker::from(block.clone());
    let mut first = Box::pin(waiters.run(native_wait::Role::Producer, std::future::poll_fn(|cx| {
        if ready.load(Ordering::Acquire) { Poll::Ready(()) }
        else { *proxy.lock().unwrap() = Some(cx.waker().clone()); Poll::Pending }
    })));
    assert!(first.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());
    ready.store(true, Ordering::Release);
    let wake = proxy.lock().unwrap().take().unwrap();
    let thread = std::thread::spawn(move || wake.wake());
    block.entered.wait();
    let completed = first.as_mut().poll(&mut Context::from_waker(Waker::noop()));
    drop(first);
    let original = Arc::new(93usize);
    let polled = Arc::new(AtomicUsize::new(0));
    let mut successor = Box::pin(waiters.run(native_wait::Role::Producer, UnpolledDrop { original: original.clone(), polled: polled.clone() }));
    let pressure = successor.as_mut().poll(&mut Context::from_waker(Waker::noop()));
    let escaped = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(successor))).err();
    block.resume.wait();
    thread.join().unwrap();
    assert!(matches!(completed, Poll::Ready(Ok(()))));
    assert!(pressure.is_pending());
    assert_eq!(polled.load(Ordering::SeqCst), 0);
    assert!(escaped.is_none(), "actual zero-poll Work destructor has fixed custody");
    assert!(waiters.retains_arc(&original));
    assert!(facts.1.load(Ordering::SeqCst) > 0);
    assert_eq!(facts.0.lock().unwrap().len(), 1, "one actual Work Drop panic, no replacement IO");
    assert!(budget.quarantined());
    assert!(!waiters.finish());
    assert!(budget.used() > 0);
}
