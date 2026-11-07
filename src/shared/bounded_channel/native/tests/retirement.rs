//! The original registration clone publishes between the two real receive probes.
use super::*;
use std::task::{RawWaker, RawWakerVTable};
struct Registration {
    sender: Mutex<Option<Sender<Item>>>,
    drops: Arc<AtomicUsize>,
}
struct Handle {
    registration: Arc<Registration>,
    retire_panics: bool,
}
unsafe fn clone_raw(data: *const ()) -> RawWaker {
    let handle = unsafe { &*(data.cast::<Handle>()) };
    if let Some(tx) = handle.registration.sender.lock().unwrap().take() {
        tx.try_reserve().unwrap().send(Item {
            bytes: 17,
            drops: handle.registration.drops.clone(),
        });
    }
    RawWaker::new(
        Box::into_raw(Box::new(Handle {
            registration: handle.registration.clone(),
            retire_panics: true,
        }))
        .cast(),
        &TABLE,
    )
}
unsafe fn wake_raw(data: *const ()) {
    unsafe {
        drop_raw(data);
    }
}
unsafe fn wake_ref(_: *const ()) {}
unsafe fn drop_raw(data: *const ()) {
    let handle = unsafe { Box::from_raw(data.cast_mut().cast::<Handle>()) };
    let panic = handle.retire_panics;
    drop(handle);
    if panic {
        panic!("original registration retirement Drop");
    }
}
static TABLE: RawWakerVTable = RawWakerVTable::new(clone_raw, wake_raw, wake_ref, drop_raw);
#[test]
fn retirement_q_keeps_second_probe_value_in_original_core() {
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (clock, facts) = clock(&meter);
    let (tx, mut rx, observer) = prepare::<Item>(2, 2, &meter, clock).unwrap();
    let drops = Arc::new(AtomicUsize::new(0));
    let handle = Handle {
        registration: Arc::new(Registration {
            sender: Mutex::new(Some(tx.clone())),
            drops: drops.clone(),
        }),
        retire_panics: false,
    };
    let waker = unsafe {
        Waker::from_raw(RawWaker::new(
            Box::into_raw(Box::new(handle)).cast(),
            &TABLE,
        ))
    };
    let result = rx.poll_recv(&mut Context::from_waker(&waker));
    let escaped = matches!(&result, Poll::Ready(Some(_)));
    // Keep an erroneous escaped value from automatic Drop while reporting RED.
    let result = ManuallyDrop::new(result);
    assert!(
        !escaped,
        "value escaped after original registration Drop published Q"
    );
    assert!(meter.is_quarantined());
    assert_eq!(facts.0.lock().unwrap().len(), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert!(unsafe { (&*observer.0.core.held.get()).is_some() });
    drop(waker);
    drop(rx);
    drop(tx);
    drop(observer);
    let _ = result;
    assert!(budget.used() > 1);
}
