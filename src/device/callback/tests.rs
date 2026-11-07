use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
struct Count(AtomicUsize);
impl Wake for Count {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}
#[test]
fn cancelled_waiter_and_late_wrapper_cannot_overwrite_successor() {
    let budget = bytes::BackingBudget::new(65536).unwrap();
    let custody = Custody::new(&BackingMeter::with_budget(&budget)).unwrap();
    let a = Arc::new(Count(AtomicUsize::new(0)));
    let b = Arc::new(Count(AtomicUsize::new(0)));
    let late = custody.register(&Waker::from(a.clone())).unwrap();
    custody.retire();
    let current = custody.register(&Waker::from(b.clone())).unwrap();
    late.wake(); // Permitted spurious poll of B; no old waiter can be restored.
    assert_eq!(a.0.load(Ordering::Relaxed), 0);
    assert_eq!(b.0.load(Ordering::Relaxed), 1);
    assert!(custody.0.lock().roles[TX].waiter.is_none());
    custody.register(&Waker::from(b.clone())).unwrap();
    current.wake();
    assert_eq!(b.0.load(Ordering::Relaxed), 2);
    custody.retire();
    drop(custody);
    assert_eq!(budget.used(), 0);
}
#[test]
fn secondary_is_original_box_in_named_once_slot() {
    let budget = bytes::BackingBudget::new(65536).unwrap();
    let custody = Custody::new(&BackingMeter::with_budget(&budget)).unwrap();
    let alive = Arc::new(());
    let original: Box<dyn Any + Send> = Box::new(alive.clone());
    let pointer = &*original as *const dyn Any as *const ();
    custody.retain_secondary(original);
    {
        let state = custody.0.lock();
        let retained = &**state.roles[TX].payloads[Site::NotifyDrop as usize].as_ref().unwrap();
        assert_eq!(&**retained as *const dyn Any as *const (), pointer);
        assert!(Arc::ptr_eq(
            retained.downcast_ref::<Arc<()>>().unwrap(),
            &alive
        ));
    }
    drop(custody);
    assert_eq!(Arc::strong_count(&alive), 2);
    assert_eq!(budget.used(), layout_for(1));
}

#[test]
fn completion_effect_cannot_follow_terminal_claim() {
    use std::{
        future::Future,
        pin::Pin,
        sync::{Condvar, Weak, atomic::AtomicBool},
        task::{Context, Poll},
    };
    type Work = Pin<Box<dyn Future<Output = Report> + Send>>;
    struct Blocking {
        entered: std::sync::mpsc::Sender<()>,
        release: Arc<(Mutex<bool>, Condvar)>,
    }
    impl Wake for Blocking {
        fn wake(self: Arc<Self>) {
            self.entered.send(()).unwrap();
            let (lock, condition) = &*self.release;
            let mut released = lock.lock().unwrap();
            while !*released {
                released = condition.wait(released).unwrap();
            }
        }
    }
    struct Reenter {
        work: Weak<Mutex<Option<Work>>>,
        clean: Arc<AtomicBool>,
    }
    impl Wake for Reenter {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            if let Some(work) = self.work.upgrade() {
                // Re-poll is legal only after the earlier poll has returned.
                // In-poll self-wake cannot obtain this unique future borrow.
                if let Ok(mut slot) = work.try_lock() {
                    if let Some(work) = slot.as_mut() {
                        if let Poll::Ready(report) =
                            work.as_mut().poll(&mut Context::from_waker(Waker::noop()))
                        {
                            self.clean
                                .store(report.failure.is_none(), Ordering::Release);
                        }
                    }
                }
            }
            panic!("terminal completion callback after scheduling its consumer");
        }
    }
    let budget = bytes::BackingBudget::new(65536).unwrap();
    let custody = Custody::new(&BackingMeter::with_budget(&budget)).unwrap();
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let (sent, received) = std::sync::mpsc::channel();
    let waker = Waker::from(Arc::new(Blocking {
        entered: sent,
        release: release.clone(),
    }));
    let controlled = custody.register(&waker).unwrap();
    let runner = std::thread::spawn(move || controlled.wake());
    received
        .recv_timeout(std::time::Duration::from_secs(2))
        .unwrap();
    let mut tail = Tail::new(custody.clone());
    let work: Work = Box::pin(async move {
        tail.settle()
            .await
            .report(crate::shared::errors::FailureOrigin {
                phase: crate::shared::errors::FailurePhase::DeviceWrite,
                resource: crate::shared::errors::FailureResource::Session,
                resource_id: 0,
                attempt_id: None,
            })
    });
    let slot = Arc::new(Mutex::new(Some(work)));
    let clean = Arc::new(AtomicBool::new(false));
    let completion = Waker::from(Arc::new(Reenter {
        work: Arc::downgrade(&slot),
        clean: clean.clone(),
    }));
    let first = catch_unwind(AssertUnwindSafe(|| {
        slot.lock()
            .unwrap()
            .as_mut()
            .unwrap()
            .as_mut()
            .poll(&mut Context::from_waker(&completion))
    }));
    assert!(
        !matches!(&first, Ok(Poll::Ready(_))),
        "the original callback is still executing"
    );
    {
        let (lock, condition) = &*release;
        *lock.lock().unwrap() = true;
        condition.notify_all();
    }
    runner.join().unwrap();
    slot.lock().unwrap_or_else(|e| e.into_inner()).take();
    drop(first);
    drop(completion);
    drop(waker);
    drop(custody);
    assert!(
        !clean.load(Ordering::Acquire),
        "terminal receipt escaped before its completion callback failed"
    );
}

#[tokio::test]
async fn device_roles_keep_independent_waiters_and_one_terminal_claim() {
    let budget = bytes::BackingBudget::new(65536).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (tx, rx) = Custody::pair(&meter).unwrap();
    let a = Arc::new(Count(AtomicUsize::new(0)));
    let b = Arc::new(Count(AtomicUsize::new(0)));
    let tx_wake = tx.register(&Waker::from(a.clone())).unwrap();
    let rx_wake = rx.register(&Waker::from(b.clone())).unwrap();
    rx.notify();
    assert_eq!(a.0.load(Ordering::Relaxed), 0);
    assert_eq!(b.0.load(Ordering::Relaxed), 1);
    rx.register(&Waker::from(b.clone())).unwrap();
    tx_wake.wake();
    assert_eq!(a.0.load(Ordering::Relaxed), 1);
    assert_eq!(b.0.load(Ordering::Relaxed), 1);
    let mut tail = Tail::new(tx.clone());
    rx_wake.wake();
    assert_eq!(b.0.load(Ordering::Relaxed), 1);
    assert!(tx.admit().is_err());
    assert!(rx.admit().is_err());
    let claim = tail.settle().await;
    assert!(claim.first.is_none());
    assert_eq!(claim.additional, 0);
    drop((tail, tx, rx, claim));
    assert_eq!(budget.used(), 0);
    assert!(!meter.is_quarantined());
}

#[test]
fn device_roles_preserve_first_io_and_deduplicate_shared_carrier() {
    let budget = bytes::BackingBudget::new(65536).unwrap();
    let (tx, rx) = Custody::pair(&BackingMeter::with_budget(&budget)).unwrap();
    rx.note_write(Err(io::ErrorKind::ConnectionReset.into()));
    let original = rx.finish().unwrap_err();
    let retained = original_io(&original).unwrap();
    tx.note_write(Err(original));
    assert_eq!(tx.finish().unwrap_err().kind(), io::ErrorKind::ConnectionReset);
    let claim = tx.claim_inner(false);
    assert!(matches!(claim.first, Some(First::Io(RX, _))));
    assert!(Arc::ptr_eq(claim.source.as_ref().unwrap(), &retained));
    assert_eq!(claim.additional, 0);
    let second = rx.claim_inner(false);
    assert!(second.first.is_none());
    assert_eq!(second.additional, 0);
}

#[test]
fn terminal_seals_both_roles_before_original_waiter_drop() {
    struct Reenter {
        rx: std::sync::Weak<Inner>,
        admitted: Arc<AtomicUsize>,
    }
    impl Wake for Reenter { fn wake(self: Arc<Self>) {} }
    impl Drop for Reenter {
        fn drop(&mut self) {
            if self.rx.upgrade().is_some_and(|rx| rx.admit().is_ok()) {
                self.admitted.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
    let budget = bytes::BackingBudget::new(65536).unwrap();
    let (tx, rx) = Custody::pair(&BackingMeter::with_budget(&budget)).unwrap();
    let admitted = Arc::new(AtomicUsize::new(0));
    let original = Waker::from(Arc::new(Reenter { rx: Arc::downgrade(&rx.0), admitted: admitted.clone() }));
    let proxy = tx.register(&original).unwrap();
    drop(original);
    let tail = Tail::new(tx.clone());
    assert_eq!(admitted.load(Ordering::Relaxed), 0, "RX must already be sealed before TX original Drop reenters");
    drop((tail, proxy, tx, rx));
}

#[test]
fn last_device_owner_disposes_two_hostile_original_waiters_independently() {
    struct Hostile(Arc<AtomicUsize>);
    impl Wake for Hostile { fn wake(self: Arc<Self>) {} }
    impl Drop for Hostile {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
            std::panic::panic_any(self.0.clone());
        }
    }
    let budget = bytes::BackingBudget::new(65536).unwrap();
    let (tx, rx) = Custody::pair(&BackingMeter::with_budget(&budget)).unwrap();
    let a = Arc::new(AtomicUsize::new(0));
    let b = Arc::new(AtomicUsize::new(0));
    for (role, counter) in [(&tx, a.clone()), (&rx, b.clone())] {
        let original = Waker::from(Arc::new(Hostile(counter)));
        drop(role.register(&original).unwrap());
        drop(original);
    }
    assert!(catch_unwind(AssertUnwindSafe(|| drop((tx, rx)))).is_ok());
    assert_eq!(a.load(Ordering::SeqCst), 1);
    assert_eq!(b.load(Ordering::SeqCst), 1);
    assert_eq!(Arc::strong_count(&a), 2, "original TX panic Box retained");
    assert_eq!(Arc::strong_count(&b), 2, "original RX panic Box retained");
    assert!(budget.quarantined());
    assert!(budget.used() > 0);
}

// External Session tests use the actual cold Core and terminal flow; these
// helpers expose no production bypass or fabricated terminal marker.
impl Custody {
    pub(crate) fn test_pair(meter: &BackingMeter) -> (Self, Self) { Self::pair(meter).unwrap() }
    pub(crate) fn test_cold_bytes(roles: usize) -> usize { layout_for(roles) }
    pub(crate) async fn test_terminal_report(&self) -> Report {
        Tail::new(self.clone()).settle().await.report(super::super::PumpControl::origin(
            crate::shared::errors::FailurePhase::DeviceShutdown))
    }
}
