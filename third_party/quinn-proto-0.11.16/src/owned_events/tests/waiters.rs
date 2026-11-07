use super::*;
use std::{
    panic::{catch_unwind, AssertUnwindSafe},
    sync::Weak,
    task::{RawWaker, RawWakerVTable},
};

struct Probe {
    lane: Weak<Inner<u64>>,
    clones: AtomicUsize,
    drops: AtomicUsize,
    panic_clone: bool,
    panic_drop: bool,
    payload: Arc<()>,
}
impl Probe {
    fn unlocked(&self) {
        if let Some(lane) = self.lane.upgrade() {
            assert!(
                !matches!(
                    lane.state.try_lock(),
                    Err(std::sync::TryLockError::WouldBlock)
                ),
                "opaque Waker callback held lane lock"
            );
        }
    }
}
unsafe fn clone_probe(data: *const ()) -> RawWaker {
    let probe = mem::ManuallyDrop::new(unsafe { Arc::from_raw(data.cast::<Probe>()) });
    probe.unlocked();
    probe.clones.fetch_add(1, Ordering::SeqCst);
    if probe.panic_clone {
        std::panic::panic_any(probe.payload.clone());
    }
    RawWaker::new(Arc::into_raw(Arc::clone(&probe)).cast(), &VTABLE)
}
unsafe fn drop_probe(data: *const ()) {
    let probe = unsafe { Arc::from_raw(data.cast::<Probe>()) };
    probe.unlocked();
    probe.drops.fetch_add(1, Ordering::SeqCst);
    if probe.panic_drop {
        std::panic::panic_any(probe.payload.clone());
    }
}
unsafe fn wake_ref_probe(data: *const ()) {
    let probe = mem::ManuallyDrop::new(unsafe { Arc::from_raw(data.cast::<Probe>()) });
    probe.unlocked();
}
static VTABLE: RawWakerVTable =
    RawWakerVTable::new(clone_probe, drop_probe, wake_ref_probe, drop_probe);
fn probe(sender: &Sender<u64>, panic_clone: bool, panic_drop: bool) -> (Arc<Probe>, Waker) {
    let probe = Arc::new(Probe {
        lane: Arc::downgrade(&sender.0),
        clones: AtomicUsize::new(0),
        drops: AtomicUsize::new(0),
        panic_clone,
        panic_drop,
        payload: Arc::new(()),
    });
    let raw = RawWaker::new(Arc::into_raw(probe.clone()).cast(), &VTABLE);
    (probe, unsafe { Waker::from_raw(raw) })
}

#[test]
fn waiting_callbacks_run_outside_lane_lock_and_replacement_drops_once() {
    for producer in [false, true] {
        let budget = BackingBudget::new(4096).unwrap();
        let meter = BackingMeter::with_budget(&budget);
        let (sender, mut receiver) = channel::<u64>(1, &meter).unwrap();
        if producer {
            sender.try_reserve().unwrap().send(1).unwrap();
        }
        let (probe, waker) = probe(&sender, false, false);
        let mut cx = Context::from_waker(&waker);
        for _ in 0..2 {
            if producer {
                assert!(sender.poll_reserve(&mut cx).is_pending());
            } else {
                assert!(receiver.poll_recv(&mut cx).is_pending());
            }
        }
        assert_eq!(probe.clones.load(Ordering::SeqCst), 2);
        assert_eq!(probe.drops.load(Ordering::SeqCst), 1);
        drop((sender, receiver, waker));
        assert_eq!(budget.used(), 0);
        assert!(!budget.quarantined());
    }
}

#[test]
fn waiting_clone_or_retired_drop_panic_keeps_original_payload_and_immediate_q() {
    for producer in [false, true] {
        for clone in [false, true] {
            let budget = BackingBudget::new(4096).unwrap();
            let meter = BackingMeter::with_budget(&budget);
            let (sender, mut receiver) = channel::<u64>(1, &meter).unwrap();
            if producer {
                sender.try_reserve().unwrap().send(1).unwrap();
            }
            let (probe, waker) = probe(&sender, clone, !clone);
            let caught = catch_unwind(AssertUnwindSafe(|| {
                let mut cx = Context::from_waker(&waker);
                if producer {
                    let _ = sender.poll_reserve(&mut cx);
                } else {
                    let _ = receiver.poll_recv(&mut cx);
                }
                let mut cx = Context::from_waker(Waker::noop());
                if producer {
                    let _ = sender.poll_reserve(&mut cx);
                } else {
                    let _ = receiver.poll_recv(&mut cx);
                }
            }))
            .unwrap_err();
            assert!(Arc::ptr_eq(
                caught.downcast_ref::<Arc<()>>().unwrap(),
                &probe.payload
            ));
            assert!(meter.snapshot().quarantined);
            assert!(sender.0.state.try_lock().is_ok());
            let refused = matches!(sender.try_reserve(), Err(ReserveError::Closed));
            let _ = catch_unwind(AssertUnwindSafe(|| drop(waker)));
            drop((sender, receiver));
            assert_eq!(budget.used(), 1);
            assert!(refused, "Q cannot readmit an ordinary operation");
        }
    }
}

#[test]
fn first_native_lane_use_after_cold_construction_allocates_nothing() {
    let budget = BackingBudget::new(4096).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (sender, mut receiver) = channel::<u64>(64, &meter).unwrap();
    let allocations = crate::tests::allocations_in(|| {
        sender.try_reserve().unwrap().send(7).unwrap();
        assert_eq!(
            receiver.poll_recv(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(Some(7))
        );
    });
    assert_eq!(allocations, 0);
    drop((sender, receiver));
    assert_eq!(budget.used(), 0);
}

#[test]
fn shared_q_closes_all_lane_admission_without_opaque_callbacks() {
    let budget = BackingBudget::new(4096).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (sender, receiver) = channel::<u64>(1, &meter).unwrap();
    meter.quarantine();
    let refused = matches!(sender.try_reserve(), Err(ReserveError::Closed));
    drop((sender, receiver));
    assert_eq!(budget.used(), 1);
    assert!(refused);
}

#[test]
fn quarantine_closes_dispatch_but_preserves_full_physical_inventory_observation() {
    for terminal in [false, true] {
        let budget = BackingBudget::new(4096).unwrap();
        let meter = BackingMeter::with_budget(&budget);
        let (sender, mut receiver) = if terminal {
            terminal_channel::<u64>(1, &meter)
        } else {
            control_channel::<u64>(1, &meter)
        }
        .unwrap();
        sender.try_reserve().unwrap().send(7).unwrap();
        if terminal {
            sender.send_terminal(9).unwrap();
        } else {
            sender
                .try_reserve_control(ControlKind::Rebind)
                .unwrap()
                .send(9)
                .unwrap();
            sender
                .try_reserve_control(ControlKind::Close)
                .unwrap()
                .send(11)
                .unwrap();
        }
        meter.quarantine();
        assert_eq!(
            receiver.poll_recv(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(None)
        );
        assert_eq!(receiver.len(), if terminal { 2 } else { 3 });
        assert!(!receiver.is_empty());
        let mut disposed = Vec::new();
        receiver.close_and_drain(|value| disposed.push(value));
        assert_eq!(disposed, if terminal { vec![7, 9] } else { vec![7, 9, 11] });
        assert!(receiver.is_empty());
        drop((sender, receiver));
        assert_eq!(budget.used(), 1);
    }
}
