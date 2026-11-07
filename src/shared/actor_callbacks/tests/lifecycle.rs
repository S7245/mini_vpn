use super::*;
use std::{
    future::Future,
    pin::Pin,
    sync::{Barrier, Mutex},
    task::{Context, Poll, Wake, Waker},
};
struct ReadyDropPanic;
impl Future for ReadyDropPanic {
    type Output = usize;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<usize> {
        Poll::Ready(73)
    }
}
impl Drop for ReadyDropPanic {
    fn drop(&mut self) {
        std::panic::panic_any(91usize);
    }
}
#[tokio::test]
async fn completed_future_drop_failure_is_not_success() {
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let (clock, facts) = clock(&meter);
    let stock = Callbacks::prepare(&meter, clock).unwrap();
    let mut lease = stock.acquire(Role::Parent).unwrap();
    let result = lease.run(&mut Some(ReadyDropPanic)).unwrap().await;
    assert!(budget.quarantined());
    assert_eq!(facts.0.lock().unwrap().len(), 1);
    assert!(
        result.is_err(),
        "Work Drop Q must not return successful completed output"
    );
    let failure = result.unwrap_err();
    assert_eq!(failure.output, Some(73));
    assert_eq!(failure.error, CallbackError::Quarantined);
    assert_eq!(
        lease.signal.lock().failures[Site::WorkDrop as usize]
            .as_ref()
            .unwrap()
            .downcast_ref::<usize>(),
        Some(&91)
    );
    drop((lease, stock));
    assert!(budget.used() > 0);
}
struct CaptureProxy(Arc<Mutex<Option<Waker>>>);
impl Future for CaptureProxy {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        *self.0.lock().unwrap() = Some(cx.waker().clone());
        Poll::Pending
    }
}
struct Block {
    entered: Barrier,
    release: Barrier,
}
impl Wake for Block {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.entered.wait();
        self.release.wait();
    }
}
#[test]
fn cancelling_future_does_not_reuse_role_while_original_wake_is_in_flight() {
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let (clock, _facts) = clock(&meter);
    let stock = Callbacks::prepare(&meter, clock.clone()).unwrap();
    let mut lease = stock.acquire(Role::Parent).unwrap();
    let saved = Arc::new(Mutex::new(None));
    let block = Arc::new(Block {
        entered: Barrier::new(2),
        release: Barrier::new(2),
    });
    let original = Waker::from(block.clone());
    let mut work = Box::pin(lease.run(&mut Some(CaptureProxy(saved.clone()))).unwrap());
    assert!(
        work.as_mut()
            .poll(&mut Context::from_waker(&original))
            .is_pending()
    );
    let proxy = saved.lock().unwrap().take().unwrap();
    let firing = std::thread::spawn(move || proxy.wake());
    block.entered.wait();
    drop(work);
    drop(lease);
    assert!(matches!(
        stock.acquire(Role::Parent),
        Err(CallbackError::Capacity)
    ));
    assert!(!stock.quiescent());
    block.release.wait();
    firing.join().unwrap();
    drop(stock.acquire(Role::Parent).unwrap());
    assert!(stock.quiescent());
    drop((stock, clock));
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}
#[test]
fn source_q_cancel_during_concurrent_wake_preserves_running_role() {
    let budget = bytes::BackingBudget::new(65536).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let (clock, facts) = clock(&meter);
    let stock = SourceCallbacks::prepare_source(&meter, clock.clone()).unwrap();
    let mut lease = stock.acquire_source().unwrap();
    let saved = Arc::new(Mutex::new(None));
    let block = Arc::new(Block { entered: Barrier::new(2), release: Barrier::new(2) });
    let original = Waker::from(block.clone());
    let mut work = Box::pin(lease.run(&mut Some(CaptureProxy(saved.clone()))).unwrap());
    assert!(work.as_mut().poll(&mut Context::from_waker(&original)).is_pending());
    let proxy = saved.lock().unwrap().take().unwrap();
    let concurrent = proxy.clone();
    meter.quarantine();
    let firing = std::thread::spawn(move || proxy.wake());
    block.entered.wait();
    for _ in 0..8 { concurrent.wake_by_ref(); }
    assert!(matches!(work.as_mut().poll(&mut Context::from_waker(&original)), Poll::Ready(Err(_))));
    drop(work);
    drop((lease, concurrent));
    let state = stock.slots[0].as_ref().unwrap().lock();
    assert!(state.running && !state.leased && state.waiter.is_none());
    drop(state);
    assert!(!stock.quiescent());
    assert!(matches!(stock.acquire_source(), Err(CallbackError::Quarantined)));
    block.release.wait();
    firing.join().unwrap();
    assert!(stock.slots[0].as_ref().unwrap().lock().idle(), "only the actual original callback exit removes the running role");
    assert!(!stock.quiescent(), "sticky Q still forbids stock retirement");
    assert!(facts.0.lock().unwrap().is_empty());
    drop((stock, clock));
    assert!(budget.quarantined() && budget.used() > 0);
}
#[test]
fn cold_partial_preparation_rolls_back_without_q() {
    let amount = Callbacks::layout_bytes() + FailureOrder::layout_bytes();
    let budget = bytes::BackingBudget::new(amount - 1).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let (clock, _facts) = clock(&meter);
    assert!(matches!(
        Callbacks::prepare(&meter, clock.clone()),
        Err(bytes::MeteredCopyError::BudgetFull)
    ));
    assert_eq!(budget.used(), FailureOrder::layout_bytes());
    drop(clock);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}
#[test]
fn zero_poll_cancellation_owns_original_future_drop() {
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let (clock, facts) = clock(&meter);
    let stock = Callbacks::prepare(&meter, clock).unwrap();
    let mut lease = stock.acquire(Role::Parent).unwrap();
    let mut original = Some(ReadyDropPanic);
    let work = lease.run(&mut original).unwrap();
    assert!(original.is_none());
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(work)));
    assert!(
        result.is_ok(),
        "zero poll cancellation must enter original Work Drop custody"
    );
    assert!(budget.quarantined());
    assert_eq!(facts.0.lock().unwrap().len(), 1);
    assert_eq!(
        lease.signal.lock().failures[Site::WorkDrop as usize]
            .as_ref()
            .unwrap()
            .downcast_ref::<usize>(),
        Some(&91)
    );
    drop((lease, stock));
    assert!(budget.used() > 0);
}
#[tokio::test]
async fn q_rejects_new_work_before_taking_its_future() {
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let (clock, facts) = clock(&meter);
    let stock = Callbacks::prepare(&meter, clock).unwrap();
    let mut lease = stock.acquire(Role::Parent).unwrap();
    let _ = lease.run(&mut Some(ReadyDropPanic)).unwrap().await;
    struct Unpolled(std::sync::Arc<std::sync::atomic::AtomicUsize>);
    impl Future for Unpolled {
        type Output = ();
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
            panic!("Q must not poll");
        }
    }
    impl Drop for Unpolled {
        fn drop(&mut self) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }
    let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut original = Some(Unpolled(drops.clone()));
    for _ in 0..100 {
        assert!(matches!(
            lease.run(&mut original),
            Err(CallbackError::Quarantined)
        ));
        assert!(original.is_some());
    }
    assert_eq!(drops.load(std::sync::atomic::Ordering::SeqCst), 0);
    drop(original);
    assert_eq!(drops.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(
        facts.0.lock().unwrap().len(),
        1,
        "Q must not admit another Work destructor into the same fixed slot"
    );
}
