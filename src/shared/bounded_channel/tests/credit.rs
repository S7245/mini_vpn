//! Actual private unit-credit ownership through pressure and cancellation.
use super::super::credit::{Credit, ReserveError};
use crate::shared::errors::FailureOrder;
use std::{
    future::Future,
    sync::Arc,
    task::{Context, Poll, Waker},
};

#[test]
fn fifo_pressure_preserves_exact_units_and_cold_stock() {
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let (clock, _facts) = original_clock(&meter);
    let credit = Credit::prepare(1, 2, &meter, clock).unwrap();
    let cold = budget.used();
    assert!(cold > 0);
    let first = credit.try_acquire().unwrap();
    let mut a = std::pin::pin!(credit.acquire());
    let mut b = std::pin::pin!(credit.acquire());
    let mut cx = Context::from_waker(Waker::noop());
    assert!(a.as_mut().poll(&mut cx).is_pending());
    assert!(b.as_mut().poll(&mut cx).is_pending());
    let mut excess = std::pin::pin!(credit.acquire());
    assert!(matches!(
        excess.as_mut().poll(&mut cx),
        Poll::Ready(Err(ReserveError::Capacity))
    ));
    drop(first);
    let Poll::Ready(Ok(unit)) = a.as_mut().poll(&mut cx) else {
        panic!("FIFO first grant")
    };
    assert_eq!(credit.available(), 0);
    drop(unit);
    let Poll::Ready(Ok(unit)) = b.as_mut().poll(&mut cx) else {
        panic!("FIFO second grant")
    };
    assert_eq!(budget.used(), cold);
    drop(unit);
    assert_eq!(credit.available(), 1);
}

#[test]
fn granted_unit_is_not_transferred_after_original_budget_quarantine() {
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let (clock, _facts) = original_clock(&meter);
    let credit = Credit::prepare(1, 1, &meter, clock).unwrap();
    let first = credit.try_acquire().unwrap();
    let mut pending = Box::pin(credit.acquire());
    let mut cx = Context::from_waker(Waker::noop());
    assert!(pending.as_mut().poll(&mut cx).is_pending());
    drop(first); // actual grant, not yet observed by the acquire future
    meter.quarantine();
    let rejected = matches!(
        pending.as_mut().poll(&mut cx),
        Poll::Ready(Err(ReserveError::Quarantined))
    );
    drop(pending);
    let available = credit.available();
    drop(credit);
    assert!(
        rejected,
        "a previously granted unit must not become new DATA authority after Q"
    );
    assert_eq!(available, 1);
    assert!(budget.quarantined());
    assert_eq!(budget.used(), 1);
}

#[test]
fn queued_quarantine_unlinks_the_original_registration() {
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let (clock, _facts) = original_clock(&meter);
    let credit = Credit::prepare(1, 1, &meter, clock).unwrap();
    let first = credit.try_acquire().unwrap();
    let mut pending = Box::pin(credit.acquire());
    let mut cx = Context::from_waker(Waker::noop());
    assert!(pending.as_mut().poll(&mut cx).is_pending());
    meter.quarantine();
    assert!(matches!(
        pending.as_mut().poll(&mut cx),
        Poll::Ready(Err(ReserveError::Quarantined))
    ));
    drop(pending);
    drop(first);
    assert_eq!(credit.available(), 1);
    drop(credit);
    assert_eq!(budget.used(), 1);
}

struct BlockingWake {
    entered: Arc<std::sync::Barrier>,
    resume: Arc<std::sync::Barrier>,
}
impl std::task::Wake for BlockingWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.entered.wait();
        self.resume.wait();
    }
}
#[test]
fn cancelled_grant_does_not_recycle_record_while_original_callback_is_running() {
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let (clock, _facts) = original_clock(&meter);
    let credit = Credit::prepare(1, 1, &meter, clock).unwrap();
    let entered = Arc::new(std::sync::Barrier::new(2));
    let resume = Arc::new(std::sync::Barrier::new(2));
    let waker = Waker::from(Arc::new(BlockingWake {
        entered: entered.clone(),
        resume: resume.clone(),
    }));
    let first = credit.try_acquire().unwrap();
    let mut a = Box::pin(credit.acquire());
    assert!(
        a.as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    std::thread::scope(|scope| {
        let worker = scope.spawn(move || drop(first));
        entered.wait();
        drop(a); // cancel actual Granted before poll, while detached wake is blocked
        let reused_unit = credit.try_acquire().unwrap();
        let mut successor = Box::pin(credit.acquire());
        let pressure = matches!(
            successor
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(Err(ReserveError::Capacity))
        );
        drop(successor);
        // Always let the real callback finish before any assertion may unwind.
        resume.wait();
        worker.join().unwrap();
        assert!(
            pressure,
            "live old callback retains its physical record across future cancellation"
        );
        let mut successor = Box::pin(credit.acquire());
        assert!(
            successor
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        drop(successor); // Queued cancellation returns the record, no unit
        drop(reused_unit);
    });
    assert_eq!(credit.available(), 1);
    drop(credit);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

struct CascadeWake {
    credit: Arc<Credit>,
    release: bool,
    depth: Arc<std::sync::atomic::AtomicUsize>,
    peak: Arc<std::sync::atomic::AtomicUsize>,
}
impl std::task::Wake for CascadeWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        use std::sync::atomic::Ordering::SeqCst;
        let depth = self.depth.fetch_add(1, SeqCst) + 1;
        self.peak.fetch_max(depth, SeqCst);
        if self.release {
            self.credit.release();
        }
        self.depth.fetch_sub(1, SeqCst);
    }
}
#[test]
fn reentrant_unit_return_does_not_recursively_execute_the_next_callback() {
    use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let (clock, _facts) = original_clock(&meter);
    let credit = Arc::new(Credit::prepare(2, 2, &meter, clock).unwrap());
    let first = credit.try_acquire().unwrap();
    credit.try_acquire().unwrap().commit(); // actual committed unit returned by A's callback
    let depth = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let make = |release| {
        Waker::from(Arc::new(CascadeWake {
            credit: credit.clone(),
            release,
            depth: depth.clone(),
            peak: peak.clone(),
        }))
    };
    let mut a = Box::pin(credit.acquire());
    let mut b = Box::pin(credit.acquire());
    assert!(
        a.as_mut()
            .poll(&mut Context::from_waker(&make(true)))
            .is_pending()
    );
    assert!(
        b.as_mut()
            .poll(&mut Context::from_waker(&make(false)))
            .is_pending()
    );
    drop(first);
    let mut cx = Context::from_waker(Waker::noop());
    let Poll::Ready(Ok(a_unit)) = a.as_mut().poll(&mut cx) else {
        panic!("A granted")
    };
    let Poll::Ready(Ok(b_unit)) = b.as_mut().poll(&mut cx) else {
        panic!("B granted")
    };
    drop(a_unit);
    drop(b_unit);
    drop(a);
    drop(b);
    drop(credit);
    assert_eq!(budget.used(), 0);
    assert_eq!(
        peak.load(SeqCst),
        1,
        "one owner runner must flatten callback reentry"
    );
}

#[derive(Default)]
struct Facts(std::sync::Mutex<Vec<(Option<i32>, bool)>>);
impl crate::shared::errors::QuietFailureSink for Facts {
    fn record_quiet(&self, failure: crate::shared::errors::RetainedFailure) {
        self.0.lock().unwrap().push((
            failure.facts().raw_os_code,
            matches!(
                failure.facts().class,
                crate::shared::errors::FailureClass::Panic
            ),
        ));
    }
}
struct OriginalPayload(Arc<std::sync::atomic::AtomicUsize>);
impl Drop for OriginalPayload {
    fn drop(&mut self) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}
struct BothPanic(Arc<std::sync::atomic::AtomicUsize>);
impl std::task::Wake for BothPanic {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        std::panic::panic_any(OriginalPayload(self.0.clone()));
    }
}
impl Drop for BothPanic {
    fn drop(&mut self) {
        std::panic::panic_any(OriginalPayload(self.0.clone()));
    }
}
#[test]
fn close_retains_original_wake_and_drop_panics_without_double_unwind() {
    use crate::shared::errors::*;
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let facts = Arc::new(Facts::default());
    let sink: Arc<dyn QuietFailureSink> = facts.clone();
    let clock = Arc::new(FailureOrder::default());
    clock
        .bind(
            Arc::downgrade(&sink),
            FailureOrigin {
                phase: FailurePhase::SessionLoop,
                resource: FailureResource::Session,
                resource_id: 1,
                attempt_id: None,
            },
            Some(meter.clone()),
        )
        .unwrap();
    clock.io(&std::io::Error::from_raw_os_error(5));
    let credit = Credit::prepare(1, 1, &meter, clock.clone()).unwrap();
    let unit = credit.try_acquire().unwrap();
    let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut pending = Box::pin(credit.acquire());
    let waker = Waker::from(Arc::new(BothPanic(drops.clone())));
    assert!(
        pending
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    drop(waker); // sole original Waker is now the actual fixed record's
    credit.close();
    assert!(matches!(
        pending
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Err(ReserveError::Quarantined))
    ));
    drop(pending);
    drop(unit);
    drop(credit);
    drop(clock);
    assert_eq!(drops.load(std::sync::atomic::Ordering::SeqCst), 0);
    let observed = facts.0.lock().unwrap();
    assert_eq!(observed.len(), 3);
    assert_eq!(observed[0].0, Some(5));
    assert!(observed[1].1);
    assert!(observed[2].1);
    assert!(budget.quarantined());
    assert_eq!(budget.used(), 1);
}

#[test]
fn cold_credit_rejects_unbound_and_foreign_clock_before_records() {
    use crate::shared::errors::*;
    for foreign in [false, true] {
        let budget = bytes::BackingBudget::new(1 << 20).unwrap();
        let meter = bytes::BackingMeter::with_budget(&budget);
        let other = bytes::BackingMeter::with_budget(&budget);
        let facts: Arc<dyn QuietFailureSink> = Arc::new(Facts::default());
        let clock = Arc::new(FailureOrder::default());
        if foreign {
            clock
                .bind(
                    Arc::downgrade(&facts),
                    FailureOrigin {
                        phase: FailurePhase::SessionLoop,
                        resource: FailureResource::Session,
                        resource_id: 8,
                        attempt_id: None,
                    },
                    Some(other),
                )
                .unwrap();
        }
        let before = budget.used();
        let result = Credit::prepare(1, 1, &meter, clock.clone());
        let rejected = matches!(result, Err(bytes::MeteredCopyError::Quarantined));
        drop(result);
        drop(clock);
        assert!(
            rejected,
            "unbound or same-budget foreign authority cannot supply original failure ownership"
        );
        assert!(budget.quarantined());
        assert!(budget.used() <= before.max(1));
    }
}

fn original_clock(
    meter: &bytes::BackingMeter,
) -> (crate::shared::errors::FailureClock, Arc<Facts>) {
    use crate::shared::errors::*;
    let facts = Arc::new(Facts::default());
    let sink: Arc<dyn QuietFailureSink> = facts.clone();
    let clock = Arc::new(FailureOrder::default());
    clock
        .bind(
            Arc::downgrade(&sink),
            FailureOrigin {
                phase: FailurePhase::SessionLoop,
                resource: FailureResource::Session,
                resource_id: 1,
                attempt_id: None,
            },
            Some(meter.clone()),
        )
        .unwrap();
    (clock, facts)
}
#[test]
fn exact_cold_stock_rejects_one_byte_short_and_releases_only_at_last_owner() {
    let stock = Credit::layout_bytes(4096).unwrap();
    let clock_size = FailureOrder::layout_bytes();
    for extra in [stock - 1, stock] {
        let budget = bytes::BackingBudget::new(clock_size + extra).unwrap();
        let meter = bytes::BackingMeter::with_budget(&budget);
        let (clock, _facts) = original_clock(&meter);
        let result = Credit::prepare(1024, 4096, &meter, clock);
        if extra < stock {
            assert!(matches!(result, Err(bytes::MeteredCopyError::BudgetFull)));
        } else {
            let owner = Arc::new(result.unwrap());
            assert_eq!(budget.used(), clock_size + stock);
            let last = owner.clone();
            drop(owner);
            assert_eq!(budget.used(), clock_size + stock);
            drop(last);
        }
        assert_eq!(budget.used(), 0);
        assert!(!budget.quarantined());
    }
}
#[test]
fn normal_close_keeps_grant_and_disposes_all_4096_records() {
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let (clock, _facts) = original_clock(&meter);
    let credit = Credit::prepare(1, 4096, &meter, clock).unwrap();
    let mut cx = Context::from_waker(Waker::noop());
    let first = credit.try_acquire().unwrap();
    let mut granted = Box::pin(credit.acquire());
    assert!(granted.as_mut().poll(&mut cx).is_pending());
    drop(first);
    let mut waiting: Vec<_> = (0..4095).map(|_| Box::pin(credit.acquire())).collect();
    for future in &mut waiting {
        assert!(future.as_mut().poll(&mut cx).is_pending());
    }
    credit.close();
    let Poll::Ready(Ok(unit)) = granted.as_mut().poll(&mut cx) else {
        panic!("ordinary close cannot revoke original granted unit")
    };
    for future in &mut waiting {
        assert!(matches!(
            future.as_mut().poll(&mut cx),
            Poll::Ready(Err(ReserveError::Closed))
        ));
    }
    drop(waiting);
    drop(granted);
    unit.commit();
    credit.release();
    assert_eq!(credit.available(), 1);
    assert!(matches!(credit.try_acquire(), Err(ReserveError::Closed)));
    drop(credit);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}
