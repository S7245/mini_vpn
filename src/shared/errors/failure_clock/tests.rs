//! Physical cold stock and the failure-only publication linearization boundary.
use super::*;
use std::sync::Barrier;

#[derive(Default)]
struct Sink(AtomicUsize);
impl QuietFailureSink for Sink {
    fn record_quiet(&self, _: RetainedFailure) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
fn origin() -> FailureOrigin {
    FailureOrigin {
        phase: super::super::FailurePhase::TcpOpen,
        resource: super::super::FailureResource::TcpOpen,
        resource_id: 2,
        attempt_id: Some(7),
    }
}
#[test]
fn cold_box_is_precharged_and_last_observer_releases_actual_stock() {
    let sink: Arc<dyn QuietFailureSink> = Arc::new(Sink::default());
    let size = FailureOrder::layout_bytes();
    let short = bytes::BackingBudget::new(size - 1).unwrap();
    let clock = FailureOrder::default();
    assert_eq!(
        clock.bind(
            Arc::downgrade(&sink),
            origin(),
            Some(bytes::BackingMeter::with_budget(&short))
        ),
        Err(bytes::MeteredCopyError::TooLarge)
    );
    assert!(!clock.is_bound());
    assert_eq!(short.used(), 0);
    assert!(!short.quarantined());
    let budget = bytes::BackingBudget::new(size).unwrap();
    let clock = Arc::new(FailureOrder::default());
    clock
        .bind(
            Arc::downgrade(&sink),
            origin(),
            Some(bytes::BackingMeter::with_budget(&budget)),
        )
        .unwrap();
    assert_eq!(budget.used(), size);
    let observer = clock.clone();
    drop(clock);
    assert_eq!(budget.used(), size);
    drop(observer);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn poisoned_original_clock_does_not_publish_or_claim_clean_order() {
    let sink = Arc::new(Sink::default());
    let erased: Arc<dyn QuietFailureSink> = sink.clone();
    let budget = bytes::BackingBudget::new(4096).unwrap();
    let clock = FailureOrder::default();
    clock
        .bind(
            Arc::downgrade(&erased),
            origin(),
            Some(bytes::BackingMeter::with_budget(&budget)),
        )
        .unwrap();
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _guard = clock.native.get().unwrap().value.state.lock().unwrap();
        panic!("original clock interruption");
    }));
    let order = clock.io(&std::io::Error::from_raw_os_error(5));
    assert!(!clock.reported(order));
    assert_eq!(clock.load(Ordering::SeqCst), usize::MAX);
    assert_eq!(sink.0.load(Ordering::SeqCst), 0);
    drop(clock);
    assert!(budget.quarantined());
    assert_eq!(budget.used(), 1);
}

struct BlockedSink {
    entered: Arc<Barrier>,
    release: Arc<Barrier>,
    faults: Arc<Mutex<()>>,
    dropped: Arc<AtomicUsize>,
}
impl QuietFailureSink for BlockedSink {
    fn record_quiet(&self, _: RetainedFailure) {
        self.entered.wait();
        self.release.wait();
    }
}
impl Drop for BlockedSink {
    fn drop(&mut self) {
        assert!(
            self.faults.try_lock().is_ok(),
            "last owner destructor must be outside fault lock"
        );
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}
#[test]
fn stamp_and_fact_publication_have_no_gap_and_last_owner_drops_outside_fault_lock() {
    let entered = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let faults = Arc::new(Mutex::new(()));
    let dropped = Arc::new(AtomicUsize::new(0));
    let sink: Arc<dyn QuietFailureSink> = Arc::new(BlockedSink {
        entered: entered.clone(),
        release: release.clone(),
        faults: faults.clone(),
        dropped: dropped.clone(),
    });
    let clock = Arc::new(FailureOrder::default());
    clock.bind(Arc::downgrade(&sink), origin(), None).unwrap();
    let worker_clock = clock.clone();
    let worker_dropped = dropped.clone();
    let worker = std::thread::spawn(move || {
        let guard = faults.lock().unwrap();
        let publication = worker_clock.io_deferred(&std::io::Error::from_raw_os_error(5));
        assert_eq!(worker_dropped.load(Ordering::SeqCst), 0);
        assert_eq!(publication.order, 0);
        drop(guard);
        drop(publication);
    });
    entered.wait();
    // The actual ordinal/report state is inaccessible until the original sink
    // accepts the facts. No atomic stamp is externally visible in this window.
    assert!(clock.native.get().unwrap().value.state.try_lock().is_err());
    drop(sink);
    release.wait();
    worker.join().unwrap();
    assert!(clock.reported(0));
    assert_eq!(clock.load(Ordering::SeqCst), 1);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}

mod dual;
