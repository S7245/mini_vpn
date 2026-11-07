use super::*;
use crate::shared::errors::*;
use std::task::Wake;
#[derive(Default)]
struct Facts(Mutex<Vec<RetainedFailure>>);
impl QuietFailureSink for Facts {
    fn record_quiet(&self, f: RetainedFailure) {
        self.0.lock().unwrap().push(f);
    }
}
fn clock(meter: &BackingMeter) -> (FailureClock, Arc<Facts>) {
    let facts = Arc::new(Facts::default());
    let sink: Arc<dyn QuietFailureSink> = facts.clone();
    let clock = Arc::new(FailureOrder::default());
    clock
        .bind(
            Arc::downgrade(&sink),
            FailureOrigin {
                phase: FailurePhase::SessionLoop,
                resource: FailureResource::Session,
                resource_id: 4,
                attempt_id: None,
            },
            Some(meter.clone()),
        )
        .unwrap();
    (clock, facts)
}
struct Item {
    bytes: usize,
    drops: Arc<AtomicUsize>,
}
impl Drop for Item {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}
impl Value for Item {
    type Disposition = ();
    fn dispose_context(_: (), _: &Fault<'_>) -> Option<()> {
        None
    }
    fn lane_bytes(&self) -> usize {
        self.bytes
    }
    fn valid_for(&self, _: &BackingMeter) -> bool {
        true
    }
    fn dispose(self, fault: &Fault<'_>, _: &Self::Disposition) -> Option<Self> {
        fault.dispose(self);
        None
    }
}
#[tokio::test]
async fn actual_units_publish_pop_pressure_and_late_permit_close_release_stock() {
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (clock, _facts) = clock(&meter);
    let (tx, mut rx, observer) = prepare::<Item>(2, 2, &meter, clock).unwrap();
    let drops = Arc::new(AtomicUsize::new(0));
    for size in [11, 22] {
        tx.reserve().await.unwrap().send(Item {
            bytes: size,
            drops: drops.clone(),
        });
    }
    assert!(matches!(tx.try_reserve(), Err(ReserveError::Capacity)));
    assert_eq!(tx.capacity(), 0);
    let first = rx.try_recv().unwrap();
    assert_eq!(first.bytes, 11);
    assert_eq!(tx.capacity(), 1);
    drop(first);
    let old = tx.reserve().await.unwrap();
    let weak = tx.downgrade();
    drop(rx);
    assert!(tx.is_closed());
    assert!(matches!(tx.try_reserve(), Err(ReserveError::Closed)));
    old.send(Item {
        bytes: 33,
        drops: drops.clone(),
    });
    drop(tx);
    assert!(weak.upgrade().is_none());
    assert!(observer.settled());
    assert_eq!(drops.load(Ordering::SeqCst), 3);
    assert!(observer.finish().is_ok());
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}
#[tokio::test]
async fn already_transferred_permit_after_q_keeps_original_slot_value_and_charge() {
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (clock, _facts) = clock(&meter);
    let (tx, rx, observer) = prepare::<Item>(2, 2, &meter, clock).unwrap();
    let drops = Arc::new(AtomicUsize::new(0));
    let old = tx.reserve().await.unwrap();
    let stock = budget.used();
    meter.quarantine();
    old.send(Item {
        bytes: 7,
        drops: drops.clone(),
    });
    assert!(matches!(tx.try_reserve(), Err(ReserveError::Quarantined)));
    drop(rx);
    drop(tx);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert!(observer.0.core.quiescent());
    assert_eq!(
        unsafe { observer.0.core.ring.inventory(Value::lane_bytes) },
        Some((1, 7))
    );
    assert!(!observer.settled());
    drop(observer);
    assert_eq!(budget.used(), stock);
    assert!(budget.quarantined());
}
struct Twice;
impl Wake for Twice {
    fn wake(self: Arc<Self>) {
        panic!("original wake A");
    }
    fn wake_by_ref(self: &Arc<Self>) {
        panic!("original wake A");
    }
}
impl Drop for Twice {
    fn drop(&mut self) {
        panic!("original drop B");
    }
}
#[test]
fn receiver_original_last_arc_wake_and_drop_are_separately_retained() {
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (clock, facts) = clock(&meter);
    let (tx, mut rx, observer) = prepare::<Item>(2, 2, &meter, clock).unwrap();
    let drops = Arc::new(AtomicUsize::new(0));
    let waker = Waker::from(Arc::new(Twice));
    assert!(rx.poll_recv(&mut Context::from_waker(&waker)).is_pending());
    drop(waker);
    tx.try_reserve().unwrap().send(Item {
        bytes: 9,
        drops: drops.clone(),
    });
    assert!(meter.is_quarantined());
    assert_eq!(facts.0.lock().unwrap().len(), 2);
    assert_eq!(
        observer
            .0
            .core
            .callbacks()
            .failures
            .iter()
            .filter(|p| p.is_some())
            .count(),
        2
    );
    drop(rx);
    drop(tx);
    drop(observer);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert!(budget.used() > 1);
}

mod cancellation;
mod publication;
mod retirement;

// Passive cross-owner event tests retain their original private RelayEvent
// implementation at client_tun. These observers do not add a state owner.
impl<T: Value> Observer<T> {
    pub(crate) fn held_for_test(&self) -> bool {
        assert!(self.0.core.receiver_closed.load(Ordering::Acquire));
        assert_eq!(self.0.core.operations.load(Ordering::Acquire), 0);
        unsafe { (&*self.0.core.held.get()).is_some() }
    }
    /// Caller excludes every publisher and receiver for this whole inspection.
    pub(crate) unsafe fn inventory_for_test(&self) -> Option<(usize, usize)> {
        unsafe { self.0.core.ring.inventory(Value::lane_bytes) }
    }
}
mod control;

mod context_drop;

impl<T: Value> Observer<T> {
    /// Caller excludes all publishers/readers for this synchronous inspection.
    /// The visitor cannot reenter, dispose, clone or retain original values.
    pub(crate) unsafe fn inspect_values_for_test(
        &self,
        inspect: impl Fn(&T, bool) -> usize,
    ) -> Option<((usize, usize), (usize, usize), (usize, usize))> {
        let core = &self.0.core;
        assert_eq!(core.operations.load(Ordering::Acquire), 0);
        assert_eq!(core.notification.load(Ordering::Acquire) & ACTIVE, 0);
        assert_eq!(core.drain.load(Ordering::Acquire) & ACTIVE, 0);
        let ring = unsafe { core.ring.inventory(|value| inspect(value, false)) }?;
        let held = unsafe { &*core.held.get() }
            .as_ref()
            .map_or((0, 0), |value| (1, inspect(value, true)));
        let stored = (
            core.messages.load(Ordering::Acquire),
            core.bytes.load(Ordering::Acquire),
        );
        assert_eq!(
            stored,
            (ring.0.checked_add(held.0)?, ring.1.checked_add(held.1)?)
        );
        Some((ring, held, stored))
    }
}
