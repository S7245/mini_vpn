//! Actual lane-level context destruction, including its two opaque fields.
use super::*;
struct OriginalPayload {
    id: usize,
    dropped: Arc<AtomicUsize>,
}
impl Drop for OriginalPayload {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}
struct Bomb(usize, Arc<AtomicUsize>);
impl Drop for Bomb {
    fn drop(&mut self) {
        std::panic::panic_any(OriginalPayload {
            id: self.0,
            dropped: self.1.clone(),
        });
    }
}
struct ContextStock {
    first: Option<Bomb>,
    second: Option<Bomb>,
}
struct ContextItem;
impl Value for ContextItem {
    type Disposition = ContextStock;
    fn dispose_context(context: ContextStock, fault: &Fault<'_>) -> Option<ContextStock> {
        let ContextStock { first, second } = context;
        fault.dispose_context_head(first);
        fault.dispose_context_tail(second);
        None
    }
    fn lane_bytes(&self) -> usize {
        0
    }
    fn valid_for(&self, _: &BackingMeter) -> bool {
        true
    }
    fn dispose(self, _: &Fault<'_>, _: &ContextStock) -> Option<Self> {
        None
    }
}
fn actual_context_tail(two: bool) {
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (clock, facts) = clock(&meter);
    clock.io(&std::io::Error::from_raw_os_error(5));
    let payload_drops = Arc::new(AtomicUsize::new(0));
    let mut context = Some(ContextStock {
        first: Some(Bomb(11, payload_drops.clone())),
        second: two.then(|| Bomb(22, payload_drops.clone())),
    });
    let (tx, rx, observer) =
        prepare_with::<ContextItem>(1, 2, &meter, clock, &mut context).unwrap();
    drop((tx, rx));
    assert!(observer.settled());
    let result = catch_unwind(AssertUnwindSafe(|| observer.finish()));
    assert!(
        result.is_ok(),
        "context destructor escaped actual Observer::finish"
    );
    let Ok(Err(observer)) = result else {
        panic!("context fault must return original observer");
    };
    {
        let callbacks = observer.0.core.callbacks();
        let first = callbacks.failures[Site::ContextHead as usize]
            .as_ref()
            .unwrap();
        assert_eq!(first.downcast_ref::<OriginalPayload>().unwrap().id, 11);
        if two {
            let second = callbacks.failures[Site::ContextTail as usize]
                .as_ref()
                .unwrap();
            assert_eq!(second.downcast_ref::<OriginalPayload>().unwrap().id, 22);
        }
    }
    drop(observer);
    assert!(meter.is_quarantined());
    assert!(
        budget.used() > 1,
        "retain actual Core and cold charge after opaque Drop"
    );
    assert_eq!(payload_drops.load(Ordering::SeqCst), 0);
    let ledger = facts.0.lock().unwrap();
    assert_eq!(ledger[0].facts().raw_os_code, Some(5));
    assert_eq!(ledger.len(), if two { 3 } else { 2 });
}
fn child(name: &str, marker: &str, two: bool) {
    if std::env::var_os(marker).is_some() {
        actual_context_tail(two);
        return;
    }
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", name, "--nocapture"])
        .env(marker, "1")
        .env("RUST_BACKTRACE", "0")
        .output()
        .unwrap();
    eprintln!(
        "context child: {:?}\n{}\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.status.success(),
        "actual context child must retain first/Boxes/Q without abort"
    );
}
#[test]
fn final_context_single_drop_is_captured_in_original_core() {
    child(
        "shared::bounded_channel::native::tests::context_drop::final_context_single_drop_is_captured_in_original_core",
        "N1_CONTEXT_SINGLE_CHILD",
        false,
    );
}
#[test]
fn final_context_two_fields_do_not_double_unwind() {
    child(
        "shared::bounded_channel::native::tests::context_drop::final_context_two_fields_do_not_double_unwind",
        "N1_CONTEXT_DOUBLE_CHILD",
        true,
    );
}

#[test]
fn cold_failure_leaves_original_context_for_retry_without_drop() {
    let total = layout_bytes::<ContextItem>(1, 2).unwrap() + FailureOrder::layout_bytes();
    let budget = bytes::BackingBudget::new(total + 16).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (clock, facts) = clock(&meter);
    clock.io(&std::io::Error::from_raw_os_error(5));
    let filler = meter.reserve_cold_layout(17).unwrap();
    let payload_drops = Arc::new(AtomicUsize::new(0));
    let mut context = Some(ContextStock {
        first: Some(Bomb(11, payload_drops.clone())),
        second: None,
    });
    let failed = prepare_with::<ContextItem>(1, 2, &meter, clock.clone(), &mut context);
    assert!(failed.is_err());
    assert!(context.is_some());
    assert_eq!(payload_drops.load(Ordering::SeqCst), 0);
    assert_eq!(budget.used(), FailureOrder::layout_bytes() + 17);
    assert!(!budget.quarantined());
    assert_eq!(facts.0.lock().unwrap().len(), 1);
    drop(filler);
    let (tx, rx, observer) =
        prepare_with::<ContextItem>(1, 2, &meter, clock, &mut context).unwrap();
    assert!(context.is_none());
    drop((tx, rx));
    let Err(observer) = observer.finish() else {
        panic!("opaque context drop must retain observer");
    };
    let callbacks = observer.0.core.callbacks();
    assert_eq!(
        callbacks.failures[Site::ContextHead as usize]
            .as_ref()
            .unwrap()
            .downcast_ref::<OriginalPayload>()
            .unwrap()
            .id,
        11
    );
    drop(callbacks);
    drop(observer);
    assert_eq!(payload_drops.load(Ordering::SeqCst), 0);
    assert_eq!(facts.0.lock().unwrap().len(), 2);
    assert!(budget.quarantined());
    assert!(budget.used() > 1);
}
struct Remainder(Arc<AtomicUsize>);
impl Drop for Remainder {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
struct PendingItem;
impl Value for PendingItem {
    type Disposition = Remainder;
    fn lane_bytes(&self) -> usize {
        0
    }
    fn valid_for(&self, _: &BackingMeter) -> bool {
        true
    }
    fn dispose(self, _: &Fault<'_>, _: &Remainder) -> Option<Self> {
        None
    }
    fn dispose_context(context: Remainder, _: &Fault<'_>) -> Option<Remainder> {
        Some(context)
    }
}
#[test]
fn unresolved_context_and_direct_inner_drop_keep_actual_stock() {
    for direct in [false, true] {
        let budget = bytes::BackingBudget::new(1 << 20).unwrap();
        let meter = BackingMeter::with_budget(&budget);
        let (clock, _facts) = clock(&meter);
        let drops = Arc::new(AtomicUsize::new(0));
        let mut context = Some(Remainder(drops.clone()));
        let (tx, rx, observer) =
            prepare_with::<PendingItem>(1, 2, &meter, clock, &mut context).unwrap();
        drop((tx, rx));
        assert!(observer.settled());
        if direct {
            drop(observer);
        } else {
            let Err(observer) = observer.finish() else {
                panic!("unresolved context must not settle");
            };
            assert!(unsafe { (&*observer.0.core.disposition.get()).is_some() });
            drop(observer);
        }
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert!(budget.quarantined());
        assert!(budget.used() > 1);
    }
}
