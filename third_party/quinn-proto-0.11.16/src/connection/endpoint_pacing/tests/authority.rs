use super::*;
use std::task::{RawWaker, RawWakerVTable};

struct Probe {
    owner: std::sync::Weak<EndpointPacingServiceInner>,
    action: AtomicUsize,
    unlocked: AtomicUsize,
    payload: Arc<()>,
    drops: AtomicUsize,
    locked_drops: AtomicUsize,
}
impl Probe {
    fn call(&self, action: usize) {
        if self.action.load(Ordering::Acquire) != action {
            return;
        }
        let owner = self.owner.upgrade().unwrap();
        if !matches!(
            owner.state.try_lock(),
            Err(std::sync::TryLockError::WouldBlock)
        ) {
            self.unlocked.fetch_add(1, Ordering::AcqRel);
        }
        std::panic::panic_any(self.payload.clone());
    }
}
unsafe fn clone_waker(ptr: *const ()) -> RawWaker {
    let probe = std::mem::ManuallyDrop::new(unsafe { Arc::from_raw(ptr.cast::<Probe>()) });
    probe.call(1);
    RawWaker::new(Arc::into_raw(Arc::clone(&probe)).cast(), &VTABLE)
}
unsafe fn wake(ptr: *const ()) {
    let probe = unsafe { Arc::from_raw(ptr.cast::<Probe>()) };
    probe.call(3);
}
unsafe fn wake_ref(ptr: *const ()) {
    let probe = std::mem::ManuallyDrop::new(unsafe { Arc::from_raw(ptr.cast::<Probe>()) });
    probe.call(3);
}
unsafe fn drop_waker(ptr: *const ()) {
    let probe = unsafe { Arc::from_raw(ptr.cast::<Probe>()) };
    probe.drops.fetch_add(1, Ordering::AcqRel);
    if probe.owner.upgrade().is_some_and(|owner| {
        matches!(
            owner.state.try_lock(),
            Err(std::sync::TryLockError::WouldBlock)
        )
    }) {
        probe.locked_drops.fetch_add(1, Ordering::AcqRel);
    }
    probe.call(2);
}
static VTABLE: RawWakerVTable = RawWakerVTable::new(clone_waker, wake, wake_ref, drop_waker);
fn fixture() -> (EndpointPacingService, bytes::BackingBudget) {
    let budget = bytes::BackingBudget::new(EndpointPacingService::native_layout_bytes()).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    (
        EndpointPacingService::prepare(
            EndpointPacingServiceConfig::endpoint_window_v1(),
            Some(&meter),
        )
        .unwrap(),
        budget,
    )
}
fn probe(service: &EndpointPacingService) -> (Waker, Arc<Probe>) {
    let probe = Arc::new(Probe {
        owner: Arc::downgrade(&service.0),
        action: AtomicUsize::new(0),
        unlocked: AtomicUsize::new(0),
        payload: Arc::new(()),
        drops: AtomicUsize::new(0),
        locked_drops: AtomicUsize::new(0),
    });
    let raw = RawWaker::new(Arc::into_raw(probe.clone()).cast(), &VTABLE);
    (unsafe { Waker::from_raw(raw) }, probe)
}
#[test]
fn native_actual_service_guard_unwind_keeps_original_payload_and_live_lease_q() {
    let (service, budget) = fixture();
    assert!(service.attach_connection(TEST_KEY));
    let now = Instant::now();
    let lease = service.reserve(TEST_KEY, now, 7).unwrap();
    let stateless = service.reserve_stateless_response(now, 11).unwrap();
    let payload = Arc::new(());
    let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _state = service.lock_state();
        std::panic::panic_any(payload.clone());
    }))
    .unwrap_err();
    assert!(Arc::ptr_eq(
        caught.downcast_ref::<Arc<()>>().unwrap(),
        &payload
    ));
    assert!(budget.quarantined());
    assert!(lease.settle_built(7).is_err());
    stateless.note_sent(now);
    assert_eq!(service.snapshot().live_reservation_bytes, 7);
    assert_eq!(service.lock_state().stateless_outstanding_bytes, 11);
    assert!(!service.attach_connection(test_key(2, 0)));
    assert!(!service.rollback_unpublished(TEST_KEY));
    drop(service);
    assert_eq!(budget.used(), 1);
}
#[test]
fn prepared_waker_clone_and_retired_waker_drop_preserve_first_payload_outside_lock() {
    for action in [1, 2] {
        let (service, budget) = fixture();
        assert!(service.attach_connection(TEST_KEY));
        let (waker, probe) = probe(&service);
        let now = Instant::now();
        if action == 2 {
            assert!(matches!(
                service.poll_reserve(
                    TEST_KEY,
                    PacingTrafficClass::Bulk,
                    now + Duration::from_secs(1),
                    1,
                    Some(&waker),
                    now
                ),
                PollReservation::Blocked { .. }
            ));
        }
        probe.action.store(action, Ordering::Release);
        let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            if action == 1 {
                service.poll_reserve(
                    TEST_KEY,
                    PacingTrafficClass::Bulk,
                    now,
                    1,
                    Some(&waker),
                    now,
                );
            } else {
                service.cancel_waiter(TEST_KEY);
            }
        }))
        .unwrap_err();
        probe.action.store(0, Ordering::Release);
        assert!(Arc::ptr_eq(
            caught.downcast_ref::<Arc<()>>().unwrap(),
            &probe.payload
        ));
        assert!(probe.unlocked.load(Ordering::Acquire) > 0);
        assert!(budget.quarantined());
        drop((waker, probe, service));
        assert_eq!(budget.used(), 1);
    }
}
#[test]
fn malformed_native_queue_inventory_fails_closed_at_control_gate() {
    let (service, budget) = fixture();
    assert!(service.attach_connection(TEST_KEY));
    service.lock_state().bulk_waiters.push_back(TEST_KEY);
    assert!(!service.attach_connection(test_key(3, 0)));
    assert!(budget.quarantined());
    assert_eq!(service.snapshot().connection_records, 1);
    drop(service);
    assert_eq!(budget.used(), 1);
}

#[test]
fn cancellation_retired_and_selected_waker_panics_preserve_original_without_abort() {
    let (service, budget) = fixture();
    let keys = [TEST_KEY, test_key(2, 0)];
    let (a, pa) = probe(&service);
    let (b, pb) = probe(&service);
    let now = Instant::now();
    for (key, waker) in keys.into_iter().zip([&a, &b]) {
        assert!(service.attach_connection(key));
        assert!(matches!(
            service.poll_reserve(
                key,
                PacingTrafficClass::Bulk,
                now + Duration::from_secs(1),
                1,
                Some(waker),
                now
            ),
            PollReservation::Blocked { .. }
        ));
    }
    pa.action.store(2, Ordering::Release);
    pb.action.store(2, Ordering::Release);
    let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        service.cancel_waiter(keys[0])
    }))
    .unwrap_err();
    pa.action.store(0, Ordering::Release);
    pb.action.store(0, Ordering::Release);
    assert!(Arc::ptr_eq(
        caught.downcast_ref::<Arc<()>>().unwrap(),
        &pa.payload
    ));
    assert_eq!(pa.unlocked.load(Ordering::Acquire), 1);
    assert_eq!(pb.unlocked.load(Ordering::Acquire), 1);
    assert!(budget.quarantined());
    drop((a, b, pa, pb, service));
    assert_eq!(budget.used(), 1);
}

#[test]
fn explicit_adapter_detach_waker_drop_preserves_first_and_immediate_q() {
    let (service, budget) = fixture();
    let mut adapter = ConnectionEndpointPacing::try_attach(service.clone(), TEST_KEY).unwrap();
    let (waker, probe) = probe(&service);
    adapter.set_driver_waker(&waker);
    probe.action.store(2, Ordering::Release);
    let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        adapter.detach(Instant::now())
    }))
    .unwrap_err();
    probe.action.store(0, Ordering::Release);
    let immediate_q = budget.quarantined();
    assert!(Arc::ptr_eq(
        caught.downcast_ref::<Arc<()>>().unwrap(),
        &probe.payload
    ));
    assert_eq!(probe.unlocked.load(Ordering::Acquire), 1);
    drop((adapter, waker, probe, service));
    assert!(
        immediate_q,
        "explicit detach must quarantine at the first opaque failure"
    );
    assert_eq!(budget.used(), 1);
}

#[test]
fn selected_wake_panic_preserves_original_and_immediate_q() {
    let (service, budget) = fixture();
    let other = test_key(2, 0);
    assert!(service.attach_connection(TEST_KEY));
    assert!(service.attach_connection(other));
    let (waker, probe) = probe(&service);
    let now = Instant::now();
    for (key, waker) in [(TEST_KEY, None), (other, Some(&waker))] {
        assert!(matches!(
            service.poll_reserve(
                key,
                PacingTrafficClass::Bulk,
                now + Duration::from_secs(1),
                1,
                waker,
                now
            ),
            PollReservation::Blocked { .. }
        ));
    }
    probe.action.store(3, Ordering::Release);
    let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        service.cancel_waiter(TEST_KEY)
    }))
    .unwrap_err();
    probe.action.store(0, Ordering::Release);
    assert!(Arc::ptr_eq(
        caught.downcast_ref::<Arc<()>>().unwrap(),
        &probe.payload
    ));
    assert_eq!(probe.unlocked.load(Ordering::Acquire), 1);
    assert!(budget.quarantined());
    drop((waker, probe, service));
    assert_eq!(budget.used(), 1);
}

#[test]
fn exact_two_retired_callbacks_cover_class_switch_and_same_waker_grant_without_allocation() {
    for next_class in [PacingTrafficClass::Bulk, PacingTrafficClass::Control] {
        let (service, budget) = fixture();
        assert!(service.attach_connection(TEST_KEY));
        let (waker, probe) = probe(&service);
        let now = Instant::now();
        assert!(matches!(
            service.poll_reserve(
                TEST_KEY,
                PacingTrafficClass::Bulk,
                now + Duration::from_secs(1),
                1,
                Some(&waker),
                now
            ),
            PollReservation::Blocked { .. }
        ));
        let before = probe.drops.load(Ordering::Acquire);
        let allocations = crate::tests::allocations_in(|| {
            let PollReservation::Granted(lease) =
                service.poll_reserve(TEST_KEY, next_class, now, 1, Some(&waker), now)
            else {
                panic!("ready grant");
            };
            drop(lease);
        });
        assert_eq!(allocations, 0);
        assert_eq!(probe.drops.load(Ordering::Acquire) - before, 2);
        assert_eq!(probe.locked_drops.load(Ordering::Acquire), 0);
        assert!(!service.lock_state().callback_overflow);
        service.detach_connection(TEST_KEY, now).unwrap();
        drop((waker, probe, service));
        assert_eq!(budget.used(), 0);
        assert!(!budget.quarantined());
    }
}

#[test]
fn migration_callbacks_run_after_adapter_key_commit_and_cancellation_retains_q() {
    for cancel in [false, true] {
        let (service, budget) = fixture();
        let mut adapter = ConnectionEndpointPacing::try_attach(service.clone(), TEST_KEY).unwrap();
        let (waker, probe) = probe(&service);
        let now = Instant::now();
        assert!(matches!(
            service.poll_reserve(
                TEST_KEY,
                PacingTrafficClass::Bulk,
                now + Duration::from_secs(1),
                1,
                Some(&waker),
                now
            ),
            PollReservation::Blocked { .. }
        ));
        let effects = adapter.migrate_deferred(1).unwrap();
        assert_eq!(adapter.key, TEST_KEY.with_path(1));
        assert!(
            service
                .lock_state()
                .connections
                .get(&adapter.key)
                .unwrap()
                .attached
        );
        assert_eq!(probe.drops.load(Ordering::Acquire), 0);
        if cancel {
            drop(effects);
        } else {
            probe.action.store(2, Ordering::Release);
            let caught =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| effects.finish()))
                    .unwrap_err();
            probe.action.store(0, Ordering::Release);
            assert!(Arc::ptr_eq(
                caught.downcast_ref::<Arc<()>>().unwrap(),
                &probe.payload
            ));
        }
        assert!(budget.quarantined());
        assert_eq!(probe.locked_drops.load(Ordering::Acquire), 0);
        drop((adapter, waker, probe, service));
        assert_eq!(budget.used(), 1);
    }
}
