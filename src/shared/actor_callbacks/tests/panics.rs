use super::*;
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Wake, Waker},
};
#[derive(Debug)]
struct Marker(u8);
struct Hostile {
    wake: Arc<Marker>,
    destruction: Arc<Marker>,
}
impl Wake for Hostile {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        std::panic::panic_any(self.wake.clone());
    }
}
impl Drop for Hostile {
    fn drop(&mut self) {
        std::panic::panic_any(self.destruction.clone());
    }
}
struct Capture(Arc<Mutex<Option<Waker>>>);
impl Future for Capture {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        *self.0.lock().unwrap() = Some(cx.waker().clone());
        Poll::Pending
    }
}
#[test]
fn source_q_cancel_keeps_both_callback_boxes_and_original_first() {
    let budget = bytes::BackingBudget::new(65536).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let (clock, facts) = clock(&meter);
    clock.io(&std::io::Error::from_raw_os_error(5));
    let stock = SourceCallbacks::prepare_source(&meter, clock.clone()).unwrap();
    let mut lease = stock.acquire_source().unwrap();
    let first = Arc::new(Marker(11));
    let second = Arc::new(Marker(22));
    let original = Waker::from(Arc::new(Hostile {
        wake: first.clone(), destruction: second.clone(),
    }));
    let saved = Arc::new(Mutex::new(None));
    let mut work = Box::pin(lease.run(&mut Some(Capture(saved.clone()))).unwrap());
    assert!(work.as_mut().poll(&mut Context::from_waker(&original)).is_pending());
    drop(original);
    let proxy = saved.lock().unwrap().take().unwrap();
    meter.quarantine();
    for _ in 0..8 { proxy.wake_by_ref(); }
    assert!(matches!(work.as_mut().poll(&mut Context::from_waker(Waker::noop())), Poll::Ready(Err(_))));
    drop(work);
    let state = lease.signal.lock();
    for (site, expected) in [(Site::Wake, &first), (Site::WakeDrop, &second)] {
        let actual = state.failures[site as usize].as_ref().unwrap()
            .downcast_ref::<Arc<Marker>>().unwrap();
        assert!(Arc::ptr_eq(actual, expected));
    }
    assert_eq!(state.failures.iter().flatten().count(), 2);
    drop(state);
    assert_eq!(facts.0.lock().unwrap().len(), 3);
    assert_eq!(facts.0.lock().unwrap()[0].facts().raw_os_code, Some(5));
    drop((proxy, lease));
    assert!(!stock.quiescent(), "retained original boxes are physical inventory");
    drop((stock, clock));
    assert!(budget.quarantined() && budget.used() > 0);
}
#[test]
fn original_wake_and_drop_keep_both_boxes_and_existing_first_io() {
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let (clock, facts) = clock(&meter);
    clock.io(&std::io::Error::from_raw_os_error(5));
    let stock = Callbacks::prepare(&meter, clock.clone()).unwrap();
    let mut lease = stock.acquire(Role::Source).unwrap();
    let a = Arc::new(Marker(11));
    let b = Arc::new(Marker(22));
    let original = Waker::from(Arc::new(Hostile {
        wake: a.clone(),
        destruction: b.clone(),
    }));
    let proxy = Arc::new(Mutex::new(None));
    let mut work = Box::pin(lease.run(&mut Some(Capture(proxy.clone()))).unwrap());
    assert!(
        work.as_mut()
            .poll(&mut Context::from_waker(&original))
            .is_pending()
    );
    drop(original);
    proxy.lock().unwrap().take().unwrap().wake();
    assert!(
        work.as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_ready()
    );
    drop(work);
    let state = lease.signal.lock();
    for (site, actual) in [(Site::Wake, &a), (Site::WakeDrop, &b)] {
        let kept = state.failures[site as usize]
            .as_ref()
            .unwrap()
            .downcast_ref::<Arc<Marker>>()
            .unwrap();
        assert!(Arc::ptr_eq(kept, actual));
    }
    assert_eq!(a.0, 11);
    assert_eq!(b.0, 22);
    drop(state);
    assert!(budget.quarantined());
    let facts = facts.0.lock().unwrap();
    assert_eq!(facts.len(), 3);
    assert_eq!(facts[0].facts().raw_os_code, Some(5));
    drop(facts);
    drop((lease, stock, clock));
    assert!(budget.used() >= Callbacks::layout_bytes());
}
#[test]
fn late_safe_proxy_retains_actual_inventory_and_prevents_false_quiescence() {
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let (clock, _facts) = clock(&meter);
    let stock = Callbacks::prepare(&meter, clock.clone()).unwrap();
    let mut lease = stock.acquire(Role::Parent).unwrap();
    let proxy = Arc::new(Mutex::new(None));
    let mut work = Box::pin(lease.run(&mut Some(Capture(proxy.clone()))).unwrap());
    assert!(
        work.as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    drop(work);
    drop(lease);
    assert!(!stock.quiescent());
    proxy.lock().unwrap().take().unwrap().wake();
    assert!(stock.quiescent());
    drop((stock, clock));
    assert_eq!(budget.used(), 0);
}

#[tokio::test]
async fn actual_actor_fields_drop_independently_during_poll_unwind() {
    struct Bomb(u8);
    impl Drop for Bomb {
        fn drop(&mut self) {
            std::panic::panic_any(self.0);
        }
    }
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let (clock, _facts) = clock(&meter);
    let stock = Callbacks::prepare(&meter, clock).unwrap();
    let mut lease = stock.acquire(Role::Parent).unwrap();
    let guard = lease.guard();
    let mut original = Some(async {
        let _first = guard.own(Bomb(11), Field::Source0);
        let _second = guard.own(Bomb(22), Field::Source1);
        panic!("original actor poll");
    });
    assert!(lease.run(&mut original).unwrap().await.is_err());
    drop(original);
    let state = stock.slots[0].as_ref().unwrap().lock();
    assert!(state.failures[Site::Poll as usize].is_some());
    assert_eq!(
        state.failures[Site::Source0 as usize]
            .as_ref()
            .unwrap()
            .downcast_ref::<u8>(),
        Some(&11)
    );
    assert_eq!(
        state.failures[Site::Source1 as usize]
            .as_ref()
            .unwrap()
            .downcast_ref::<u8>(),
        Some(&22)
    );
    drop(state);
    assert!(budget.quarantined());
    assert!(budget.used() > 0);
}
