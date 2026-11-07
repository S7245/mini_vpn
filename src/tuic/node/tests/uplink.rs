use super::super::*;
use crate::shared::errors::QuietFailureSink;
use std::sync::atomic::{AtomicUsize, Ordering};
#[test]
fn unbound_normal_parent_read_holds_concurrent_quiet_until_original_turn_retires() {
    struct Read {
        owner: std::sync::Weak<StartupResources>,
        entered: Arc<std::sync::Barrier>,
        leave: Arc<std::sync::Barrier>,
        reads: AtomicUsize,
        quiet: AtomicUsize,
        deadline: tokio::time::Instant,
    }
    impl QuietFailureSink for Read {
        fn record_quiet(&self, _: RetainedFailure) {}
        fn deadline_quiet(&self) -> Option<tokio::time::Instant> {
            if self.reads.fetch_add(1, Ordering::SeqCst) == 0 {
                self.entered.wait();
                self.leave.wait();
            }
            Some(self.deadline)
        }
        fn quarantine_quiet(&self) {
            self.quiet.fetch_add(1, Ordering::SeqCst);
            self.owner.upgrade().unwrap().uplink_quarantine();
        }
    }
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let root = StartupResources::udp_owner_for_test(bytes::BackingMeter::with_budget(&budget));
    let sink = Arc::new(Read {
        owner: Arc::downgrade(&root),
        entered: Arc::new(std::sync::Barrier::new(2)),
        leave: Arc::new(std::sync::Barrier::new(2)),
        reads: AtomicUsize::new(0),
        quiet: AtomicUsize::new(0),
        deadline: tokio::time::Instant::now() + Duration::from_secs(3),
    });
    let parent: Arc<dyn QuietFailureSink> = sink.clone();
    root.state().udp_parent = Some(Arc::downgrade(&parent));
    let reader = root.clone();
    let thread = std::thread::spawn(move || reader.uplink_stop_deadline());
    sink.entered.wait();
    assert!(!root.state().stopping && !budget.quarantined());
    assert!(root.state().shutdown_deadline.is_none());
    root.uplink_quarantine();
    assert!(budget.quarantined());
    assert_eq!(sink.quiet.load(Ordering::SeqCst), 0);
    sink.leave.wait();
    assert_eq!(thread.join().unwrap(), Some(sink.deadline));
    assert_eq!(sink.quiet.load(Ordering::SeqCst), 1);
    assert_eq!(sink.reads.load(Ordering::SeqCst), 3);
    assert!(matches!(
        root.state().uplink_parent,
        super::super::uplink::Parent::Ready
    ));
}

struct Parent {
    hold: StdMutex<Option<Arc<Self>>>,
    deadline: tokio::time::Instant,
    effect_panic: bool,
    drop_panic: bool,
    calls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
    original: Arc<()>,
}

#[test]
fn normal_parent_read_last_arc_drop_keeps_concurrent_q() {
    normal_drop(false);
}
#[test]
fn normal_parent_read_last_arc_drop_panic_preserves_original_box() {
    normal_drop(true);
}
fn normal_drop(panic: bool) {
    struct DropRead {
        hold: StdMutex<Option<Arc<Self>>>,
        entered: Arc<std::sync::Barrier>,
        leave: Arc<std::sync::Barrier>,
        deadline: tokio::time::Instant,
        drops: Arc<AtomicUsize>,
        quiet: Arc<AtomicUsize>,
        original: Arc<()>,
        panic: bool,
    }
    impl QuietFailureSink for DropRead {
        fn record_quiet(&self, _: RetainedFailure) {}
        fn deadline_quiet(&self) -> Option<tokio::time::Instant> {
            drop(self.hold.lock().unwrap().take());
            Some(self.deadline)
        }
        fn quarantine_quiet(&self) {
            self.quiet.fetch_add(1, Ordering::SeqCst);
        }
    }
    impl Drop for DropRead {
        fn drop(&mut self) {
            self.entered.wait();
            self.leave.wait();
            self.drops.fetch_add(1, Ordering::SeqCst);
            if self.panic {
                std::panic::panic_any(self.original.clone());
            }
        }
    }
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let root = StartupResources::udp_owner_for_test(bytes::BackingMeter::with_budget(&budget));
    let entered = Arc::new(std::sync::Barrier::new(2));
    let leave = Arc::new(std::sync::Barrier::new(2));
    let drops = Arc::new(AtomicUsize::new(0));
    let quiet = Arc::new(AtomicUsize::new(0));
    let original = Arc::new(());
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    let parent = Arc::new(DropRead {
        hold: StdMutex::new(None),
        entered: entered.clone(),
        leave: leave.clone(),
        deadline,
        drops: drops.clone(),
        quiet: quiet.clone(),
        original: original.clone(),
        panic,
    });
    *parent.hold.lock().unwrap() = Some(parent.clone());
    let sink: Arc<dyn QuietFailureSink> = parent;
    let weak = Arc::downgrade(&sink);
    root.state().udp_parent = Some(weak.clone());
    drop(sink);
    let reader = root.clone();
    let thread = std::thread::spawn(move || reader.uplink_stop_deadline());
    entered.wait();
    assert_eq!(root.state().shutdown_deadline, Some(deadline));
    assert!(!budget.quarantined());
    root.uplink_quarantine();
    assert!(matches!(
        root.state().uplink_parent,
        super::super::uplink::Parent::Running { quarantine: true }
    ));
    leave.wait();
    assert_eq!(thread.join().unwrap(), Some(deadline));
    assert!(budget.quarantined() && root.state().quarantined);
    assert!(weak.upgrade().is_none()); // Parent physically died; no obsolete Arc may be reused.
    assert_eq!(quiet.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    if panic {
        let state = root.state();
        let super::super::uplink::Parent::Failed { _fault, .. } = &state.uplink_parent else {
            panic!("lost Drop fault")
        };
        assert!(Arc::ptr_eq(
            _fault.downcast_ref::<Arc<()>>().unwrap(),
            &original
        ));
        assert_eq!(
            state.first.as_ref().unwrap().facts().class,
            crate::shared::errors::FailureClass::Panic
        );
    }
    root.uplink_quarantine();
    assert_eq!(root.state().additional, 0);
}

impl QuietFailureSink for Parent {
    fn record_quiet(&self, _: RetainedFailure) {}
    fn deadline_quiet(&self) -> Option<tokio::time::Instant> {
        Some(self.deadline)
    }
    fn quarantine_quiet(&self) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        drop(self.hold.lock().unwrap().take());
        if self.effect_panic {
            std::panic::panic_any(self.original.clone());
        }
    }
}
impl Drop for Parent {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
        if self.drop_panic {
            std::panic::panic_any(self.original.clone());
        }
    }
}
#[tokio::test]
async fn original_unbound_uplink_parent_actual_last_arc_drop_is_contained() {
    parent_failure(false).await;
}
#[tokio::test]
async fn original_unbound_uplink_parent_effect_retains_actual_parent() {
    parent_failure(true).await;
}
async fn parent_failure(effect: bool) {
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let root = StartupResources::udp_owner_for_test(bytes::BackingMeter::with_budget(&budget));
    let original = Arc::new(());
    let calls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    let parent = Arc::new(Parent {
        hold: StdMutex::new(None),
        deadline,
        effect_panic: effect,
        drop_panic: !effect,
        calls: calls.clone(),
        drops: drops.clone(),
        original: original.clone(),
    });
    *parent.hold.lock().unwrap() = Some(parent.clone());
    let sink: Arc<dyn QuietFailureSink> = parent;
    root.state().udp_parent = Some(Arc::downgrade(&sink));
    drop(sink);
    let outcome = catch_unwind(AssertUnwindSafe(|| root.uplink_quarantine()));
    assert!(
        outcome.is_ok(),
        "original parent effect and last Arc Drop are separate retained sites"
    );
    assert_eq!(root.state().shutdown_deadline, Some(deadline));
    assert!(root.state().quarantined && budget.quarantined());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(drops.load(Ordering::SeqCst), usize::from(!effect));
    assert_eq!(Arc::strong_count(&original), if effect { 3 } else { 2 });
    root.uplink_quarantine();
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "failed parent is never entered again"
    );
}

#[test]
fn original_unbound_parent_in_flight_already_preserves_earliest_deadline() {
    struct Gate {
        deadline: tokio::time::Instant,
        entered: Arc<std::sync::Barrier>,
        leave: Arc<std::sync::Barrier>,
    }
    impl QuietFailureSink for Gate {
        fn record_quiet(&self, _: RetainedFailure) {}
        fn deadline_quiet(&self) -> Option<tokio::time::Instant> {
            Some(self.deadline)
        }
        fn quarantine_quiet(&self) {
            self.entered.wait();
            self.leave.wait();
        }
    }
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let root = StartupResources::udp_owner_for_test(bytes::BackingMeter::with_budget(&budget));
    let entered = Arc::new(std::sync::Barrier::new(2));
    let leave = Arc::new(std::sync::Barrier::new(2));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    let sink: Arc<dyn QuietFailureSink> = Arc::new(Gate {
        deadline,
        entered: entered.clone(),
        leave: leave.clone(),
    });
    root.state().udp_parent = Some(Arc::downgrade(&sink));
    let work = root.clone();
    let thread = std::thread::spawn(move || work.uplink_quarantine());
    entered.wait();
    let observed = root.state().shutdown_deadline;
    root.uplink_quarantine(); // Legal concurrent effect observes Running; no re-entry.
    leave.wait();
    thread.join().unwrap();
    assert_eq!(
        observed,
        Some(deadline),
        "known parent minimum cannot wait for the opaque effect to finish"
    );
}
