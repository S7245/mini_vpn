use super::*;

#[test]
fn udp_still_rejects_another_meter_on_the_same_budget() {
    let budget = bytes::BackingBudget::new(65536).unwrap();
    let first = bytes::BackingMeter::with_budget(&budget);
    let other = bytes::BackingMeter::with_budget(&budget);
    let (clock, _) = clock(&first);
    assert!(matches!(Callbacks::prepare(&other, clock), Err(bytes::MeteredCopyError::Quarantined)));
    assert!(budget.quarantined());
}

#[test]
fn single_source_zero_poll_cancellation_disposes_both_fields_and_keeps_both_boxes() {
    struct Explosive(Arc<usize>);
    impl Drop for Explosive {
        fn drop(&mut self) { std::panic::panic_any(self.0.clone()); }
    }
    let budget = bytes::BackingBudget::new(65536).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let (clock, _) = clock(&meter);
    let stock = SourceCallbacks::prepare_source(&meter, clock).unwrap();
    let mut lease = stock.acquire_source().unwrap();
    let guard = lease.guard();
    let first = Arc::new(11usize);
    let second = Arc::new(29usize);
    let a = guard.own(Explosive(first.clone()), Field::Source0);
    let b = guard.own(Explosive(second.clone()), Field::Source1);
    let mut original = Some(async move {
        let _fields = (a, b);
        std::future::pending::<()>().await;
    });
    let work = lease.run(&mut original).unwrap();
    assert!(original.is_none());
    drop(work); // Never polled; individual fields precede the generic Work guard.
    drop((lease, guard));
    let state = stock.slots[0].as_ref().unwrap().lock();
    for (site, expected) in [(Site::Source0, &first), (Site::Source1, &second)] {
        let original = state.failures[site as usize].as_ref().unwrap();
        assert!(Arc::ptr_eq(original.downcast_ref::<Arc<usize>>().unwrap(), expected));
    }
    drop(state);
    assert!(budget.quarantined());
    assert!(!stock.quiescent());
    assert!(budget.used() > 0);
}

#[test]
fn source_single_role_uses_original_node_meter_and_session_clock_budget() {
    let budget = bytes::BackingBudget::new(65536).unwrap();
    let app = bytes::BackingMeter::with_budget(&budget);
    let node = bytes::BackingMeter::with_budget(&budget);
    let (clock, _facts) = clock(&app);
    let before = budget.used();
    let stock = SourceCallbacks::prepare_source(&node, clock.clone()).unwrap();
    assert_eq!(budget.used() - before, SourceCallbacks::layout_bytes());
    let lease = stock.acquire_source().unwrap();
    assert!(matches!(stock.acquire_source(), Err(CallbackError::Capacity)));
    let guard = lease.guard();
    drop(lease);
    assert!(!stock.quiescent());
    assert!(matches!(stock.acquire_source(), Err(CallbackError::Capacity)));
    drop(guard);
    assert!(stock.quiescent());
    drop(stock);
    assert_eq!(budget.used(), before);
    assert!(!budget.quarantined());
    drop(clock);
    assert_eq!(budget.used(), 0);
}

#[test]
fn source_foreign_budget_is_typed_quarantine_and_zero_inventory_is_rejected() {
    let first = bytes::BackingBudget::new(65536).unwrap();
    let second = bytes::BackingBudget::new(65536).unwrap();
    let app = bytes::BackingMeter::with_budget(&first);
    let node = bytes::BackingMeter::with_budget(&second);
    let (clock, _facts) = clock(&app);
    assert!(matches!(SourceCallbacks::prepare_source(&node, clock), Err(bytes::MeteredCopyError::Quarantined)));
    assert!(first.quarantined() && second.quarantined());
    let clean_budget = bytes::BackingBudget::new(65536).unwrap();
    let clean = bytes::BackingMeter::with_budget(&clean_budget);
    let (clock, _) = super::clock(&clean);
    assert!(matches!(Inventory::<0>::prepare_bound(&clean, clock, false), Err(bytes::MeteredCopyError::TooLarge)));
    assert!(!clean.is_quarantined());
}

impl SourceCallbacks {
    pub(crate) fn source_wake_payload_matches<T: Send + Sync + 'static>(&self, expected: &Arc<T>) -> bool {
        self.slots[0].as_ref().is_some_and(|signal| signal.lock().failures[Site::Wake as usize]
            .as_ref().and_then(|payload| payload.downcast_ref::<Arc<T>>()).is_some_and(|actual| Arc::ptr_eq(actual, expected)))
    }
    pub(crate) fn source_payload_matches<T: Send + Sync + 'static>(&self, field: Field, expected: &Arc<T>) -> bool {
        self.slots[0].as_ref().is_some_and(|signal| signal.lock().failures[field.site() as usize]
            .as_ref().and_then(|payload| payload.downcast_ref::<Arc<T>>()).is_some_and(|actual| Arc::ptr_eq(actual, expected)))
    }
}

#[test]
fn source_q_wake_exits_original_work_without_polling_data_again() {
    struct Counter(std::sync::atomic::AtomicUsize);
    impl std::task::Wake for Counter {
        fn wake(self: Arc<Self>) { self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst); }
        fn wake_by_ref(self: &Arc<Self>) { self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst); }
    }
    let budget = bytes::BackingBudget::new(65536).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let (clock, _owner) = clock(&meter);
    let stock = SourceCallbacks::prepare_source(&meter, clock).unwrap();
    let mut lease = stock.acquire_source().unwrap();
    let polls = std::cell::Cell::new(0);
    let proxy = std::cell::RefCell::new(None);
    let mut body = Some(std::future::poll_fn(|cx| {
        polls.set(polls.get() + 1);
        *proxy.borrow_mut() = Some(cx.waker().clone());
        Poll::<()>::Pending
    }));
    let mut work = Box::pin(lease.run(&mut body).unwrap());
    let counter = Arc::new(Counter(std::sync::atomic::AtomicUsize::new(0)));
    let waker = Waker::from(counter.clone());
    assert!(work.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());
    meter.quarantine();
    proxy.borrow().as_ref().unwrap().wake_by_ref();
    assert_eq!(counter.0.load(std::sync::atomic::Ordering::SeqCst), 1, "Source Q must wake its original executor to cancel Work");
    assert!(matches!(work.as_mut().poll(&mut Context::from_waker(&waker)), Poll::Ready(Err(_))));
    assert_eq!(polls.get(), 1, "Q cancellation never polls original DATA again");
}

impl SourceCallbacks {
    pub(crate) fn source_inventory_for_test(&self) -> Option<(usize, bool, usize, bool, bool, usize)> {
        let signal = self.slots[0].as_ref()?;
        let state = signal.lock();
        Some((Arc::strong_count(signal), state.leased, state.operations, state.waiter.is_some(), state.running, state.failures.iter().flatten().count()))
    }
}
