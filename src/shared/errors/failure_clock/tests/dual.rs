//! One Node-first ordinal and two independently guarded original owner drops.
use super::*;
use std::any::Any;
struct DualSink {
    id: usize,
    facts: Arc<Mutex<Vec<(usize, Option<i32>)>>>,
    peer_ledger: Arc<Mutex<()>>,
    own_ledger: Arc<Mutex<()>>,
    caller: Arc<Mutex<()>>,
    drop_panics: bool,
    payload_drops: Arc<AtomicUsize>,
}
impl QuietFailureSink for DualSink {
    fn record_quiet(&self, failure: RetainedFailure) {
        let _own = self.own_ledger.lock().unwrap();
        assert!(
            self.peer_ledger.try_lock().is_ok(),
            "Node/Session ledger locks must not overlap"
        );
        self.facts
            .lock()
            .unwrap()
            .push((self.id, failure.facts().raw_os_code));
    }
}
struct Payload(usize, Arc<AtomicUsize>);
impl Drop for Payload {
    fn drop(&mut self) {
        self.1.fetch_add(1, Ordering::SeqCst);
    }
}
impl Drop for DualSink {
    fn drop(&mut self) {
        assert!(
            self.caller.try_lock().is_ok(),
            "last owner cannot drop under original caller lock"
        );
        assert!(self.own_ledger.try_lock().is_ok());
        assert!(self.peer_ledger.try_lock().is_ok());
        if self.drop_panics {
            std::panic::panic_any(Payload(self.id, self.payload_drops.clone()));
        }
    }
}
#[test]
fn same_ordinal_delivers_node_then_session_and_preserves_both_last_drop_boxes() {
    let budget = bytes::BackingBudget::new(1 << 16).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let clock = FailureOrder::default();
    let facts = Arc::new(Mutex::new(Vec::new()));
    let node_ledger = Arc::new(Mutex::new(()));
    let session_ledger = Arc::new(Mutex::new(()));
    let caller = Arc::new(Mutex::new(()));
    let payload_drops = Arc::new(AtomicUsize::new(0));
    let node: Arc<dyn QuietFailureSink> = Arc::new(DualSink {
        id: 11,
        facts: facts.clone(),
        peer_ledger: session_ledger.clone(),
        own_ledger: node_ledger.clone(),
        caller: caller.clone(),
        drop_panics: true,
        payload_drops: payload_drops.clone(),
    });
    let parent: Arc<dyn QuietFailureSink> = Arc::new(DualSink {
        id: 22,
        facts: facts.clone(),
        peer_ledger: node_ledger,
        own_ledger: session_ledger,
        caller: caller.clone(),
        drop_panics: true,
        payload_drops: payload_drops.clone(),
    });
    clock
        .bind_with_parent(
            Arc::downgrade(&node),
            Some(Arc::downgrade(&parent)),
            origin(),
            Some(meter),
        )
        .unwrap();
    let guard = caller.lock().unwrap();
    let publication = clock.io_deferred(&std::io::Error::from_raw_os_error(5));
    drop((node, parent));
    assert_eq!(publication.order, 0);
    assert_eq!(*facts.lock().unwrap(), vec![(11, Some(5)), (22, Some(5))]);
    assert!(clock.reported(0));
    assert_eq!(clock.load(Ordering::SeqCst), 1);
    assert!(!budget.quarantined());
    drop(guard);
    drop(publication);
    assert_eq!(payload_drops.load(Ordering::SeqCst), 0);
    assert!(budget.quarantined());
    let retained = clock.native.get().unwrap().value.owner_drop.lock().unwrap();
    for (index, id) in [11, 22].into_iter().enumerate() {
        let value: &dyn Any = &***retained[index].as_ref().unwrap();
        assert_eq!(value.downcast_ref::<Payload>().unwrap().0, id);
    }
    drop(retained);
    drop(clock);
    assert_eq!(payload_drops.load(Ordering::SeqCst), 0);
    assert!(budget.used() > 0);
}
#[test]
fn missing_parent_quarantines_without_retracting_real_node_delivery() {
    let budget = bytes::BackingBudget::new(1 << 16).unwrap();
    let node = Arc::new(Sink::default());
    let node_erased: Arc<dyn QuietFailureSink> = node.clone();
    let parent: Arc<dyn QuietFailureSink> = Arc::new(Sink::default());
    let clock = FailureOrder::default();
    clock
        .bind_with_parent(
            Arc::downgrade(&node_erased),
            Some(Arc::downgrade(&parent)),
            origin(),
            Some(bytes::BackingMeter::with_budget(&budget)),
        )
        .unwrap();
    drop(parent);
    let order = clock.io(&std::io::Error::from_raw_os_error(5));
    assert_eq!(node.0.load(Ordering::SeqCst), 1);
    assert!(
        clock.reported(order),
        "JOIN must not duplicate the already delivered Node fact"
    );
    assert!(budget.quarantined());
    assert_eq!(clock.load(Ordering::SeqCst), usize::MAX);
    drop(clock);
    assert!(budget.used() > 0);
}

#[test]
fn duplicated_owner_cannot_be_bound_as_its_own_parent() {
    let budget = bytes::BackingBudget::new(1 << 16).unwrap();
    let sink: Arc<dyn QuietFailureSink> = Arc::new(Sink::default());
    let clock = FailureOrder::default();
    let result = clock.bind_with_parent(
        Arc::downgrade(&sink),
        Some(Arc::downgrade(&sink)),
        origin(),
        Some(bytes::BackingMeter::with_budget(&budget)),
    );
    assert_eq!(result, Err(bytes::MeteredCopyError::Quarantined));
    assert!(!clock.is_bound());
    assert!(budget.quarantined());
    assert!(budget.used() > 0);
}
#[test]
fn missing_node_does_not_publish_parent_or_fabricate_reported_fact() {
    let budget = bytes::BackingBudget::new(1 << 16).unwrap();
    let node: Arc<dyn QuietFailureSink> = Arc::new(Sink::default());
    let parent = Arc::new(Sink::default());
    let erased: Arc<dyn QuietFailureSink> = parent.clone();
    let clock = FailureOrder::default();
    clock
        .bind_with_parent(
            Arc::downgrade(&node),
            Some(Arc::downgrade(&erased)),
            origin(),
            Some(bytes::BackingMeter::with_budget(&budget)),
        )
        .unwrap();
    drop(node);
    let order = clock.io(&std::io::Error::from_raw_os_error(5));
    assert!(!clock.reported(order));
    assert_eq!(parent.0.load(Ordering::SeqCst), 0);
    assert!(budget.quarantined());
    drop(clock);
    assert!(budget.used() > 0);
}

#[test]
fn parent_last_drop_reaches_still_live_original_node() {
    let budget = bytes::BackingBudget::new(1 << 16).unwrap();
    let facts = Arc::new(Mutex::new(Vec::new()));
    let node_ledger = Arc::new(Mutex::new(()));
    let parent_ledger = Arc::new(Mutex::new(()));
    let caller = Arc::new(Mutex::new(()));
    let drops = Arc::new(AtomicUsize::new(0));
    let node: Arc<dyn QuietFailureSink> = Arc::new(DualSink {
        id: 11,
        facts: facts.clone(),
        peer_ledger: parent_ledger.clone(),
        own_ledger: node_ledger.clone(),
        caller: caller.clone(),
        drop_panics: false,
        payload_drops: drops.clone(),
    });
    let parent: Arc<dyn QuietFailureSink> = Arc::new(DualSink {
        id: 22,
        facts: facts.clone(),
        peer_ledger: node_ledger,
        own_ledger: parent_ledger,
        caller,
        drop_panics: true,
        payload_drops: drops.clone(),
    });
    let clock = FailureOrder::default();
    clock
        .bind_with_parent(
            Arc::downgrade(&node),
            Some(Arc::downgrade(&parent)),
            origin(),
            Some(bytes::BackingMeter::with_budget(&budget)),
        )
        .unwrap();
    let publication = clock.io_deferred(&std::io::Error::from_raw_os_error(5));
    drop(parent);
    drop(publication);
    assert_eq!(
        *facts.lock().unwrap(),
        vec![(11, Some(5)), (22, Some(5)), (11, None)]
    );
    assert!(clock.reported(0));
    assert!(
        clock.reported(1),
        "Node really received the distinct parent Drop failure"
    );
    assert!(budget.quarantined());
    drop((clock, node));
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert!(budget.used() > 0);
}

struct ReleasedNode {
    external: Weak<Mutex<Option<Arc<dyn QuietFailureSink>>>>,
    clock: Weak<FailureOrder>,
    facts: Arc<Mutex<Vec<(usize, Option<i32>)>>>,
    drops: Arc<AtomicUsize>,
}
impl QuietFailureSink for ReleasedNode {
    fn record_quiet(&self, failure: RetainedFailure) {
        self.facts
            .lock()
            .unwrap()
            .push((11, failure.facts().raw_os_code));
        if failure.facts().class == crate::shared::errors::FailureClass::Panic {
            // A real external owner retires in response to the parent's failure.
            // The Publication must still own the one reference guarding this call.
            drop(self.external.upgrade().unwrap().lock().unwrap().take());
        }
    }
}
impl Drop for ReleasedNode {
    fn drop(&mut self) {
        let clock = self.clock.upgrade().unwrap();
        assert!(
            clock.native.get().unwrap().value.state.try_lock().is_ok(),
            "the third release must follow ordinal/publication unlock"
        );
        std::panic::panic_any(Payload(11, self.drops.clone()));
    }
}
#[test]
fn reverse_reacquisition_last_drop_is_guarded_once_without_recursion() {
    let budget = bytes::BackingBudget::new(1 << 16).unwrap();
    let clock = Arc::new(FailureOrder::default());
    let external = Arc::new(Mutex::new(None));
    let facts = Arc::new(Mutex::new(Vec::new()));
    let drops = Arc::new(AtomicUsize::new(0));
    let node: Arc<dyn QuietFailureSink> = Arc::new(ReleasedNode {
        external: Arc::downgrade(&external),
        clock: Arc::downgrade(&clock),
        facts: facts.clone(),
        drops: drops.clone(),
    });
    *external.lock().unwrap() = Some(node.clone());
    let parent: Arc<dyn QuietFailureSink> = Arc::new(DualSink {
        id: 22,
        facts: facts.clone(),
        peer_ledger: Arc::new(Mutex::new(())),
        own_ledger: Arc::new(Mutex::new(())),
        caller: Arc::new(Mutex::new(())),
        drop_panics: true,
        payload_drops: drops.clone(),
    });
    clock
        .bind_with_parent(
            Arc::downgrade(&node),
            Some(Arc::downgrade(&parent)),
            origin(),
            Some(bytes::BackingMeter::with_budget(&budget)),
        )
        .unwrap();
    let publication = clock.io_deferred(&std::io::Error::from_raw_os_error(5));
    drop((node, parent));
    drop(publication);
    assert!(external.lock().unwrap().is_none());
    assert_eq!(
        *facts.lock().unwrap(),
        vec![(11, Some(5)), (22, Some(5)), (11, None)]
    );
    assert!(clock.reported(0));
    assert!(clock.reported(1));
    assert!(!clock.reported(2));
    let retained = clock.native.get().unwrap().value.owner_drop.lock().unwrap();
    for (index, id) in [11, 22].into_iter().enumerate() {
        assert_eq!(
            retained[index]
                .as_ref()
                .unwrap()
                .downcast_ref::<Payload>()
                .unwrap()
                .0,
            id
        );
    }
    drop(retained);
    drop(clock);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert!(budget.quarantined());
    assert!(budget.used() > 0);
}
struct DeadlineParent {
    external: Weak<Mutex<Option<Arc<dyn QuietFailureSink>>>>,
    drops: Arc<AtomicUsize>,
}
impl QuietFailureSink for DeadlineParent {
    fn record_quiet(&self, _: RetainedFailure) {}
    fn deadline_quiet(&self) -> Option<tokio::time::Instant> {
        drop(self.external.upgrade().unwrap().lock().unwrap().take());
        Some(tokio::time::Instant::now())
    }
}
impl Drop for DeadlineParent {
    fn drop(&mut self) {
        std::panic::panic_any(Payload(22, self.drops.clone()));
    }
}
#[test]
fn deadline_accessor_guards_the_actual_last_parent_reference() {
    let budget = bytes::BackingBudget::new(1 << 16).unwrap();
    let clock = FailureOrder::default();
    let external = Arc::new(Mutex::new(None));
    let drops = Arc::new(AtomicUsize::new(0));
    let node = Arc::new(Sink::default());
    let erased: Arc<dyn QuietFailureSink> = node.clone();
    let parent: Arc<dyn QuietFailureSink> = Arc::new(DeadlineParent {
        external: Arc::downgrade(&external),
        drops: drops.clone(),
    });
    *external.lock().unwrap() = Some(parent.clone());
    clock
        .bind_with_parent(
            Arc::downgrade(&erased),
            Some(Arc::downgrade(&parent)),
            origin(),
            Some(bytes::BackingMeter::with_budget(&budget)),
        )
        .unwrap();
    drop(parent);
    assert!(clock.parent_deadline().is_err());
    assert!(external.lock().unwrap().is_none());
    assert_eq!(node.0.load(Ordering::SeqCst), 1);
    assert!(clock.reported(0));
    drop(clock);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert!(budget.quarantined());
    assert!(budget.used() > 0);
}
