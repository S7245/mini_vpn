use super::*;
use std::{future::Future, sync::Barrier};
#[test]
fn actual_admitted_first_publication_hole_preserves_fifo_and_pending_receiver() {
    use crate::shared::bounded_channel::ring::PublicationPause;
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (clock, _facts) = clock(&meter);
    let (tx, mut rx, observer) = prepare::<Item>(2, 2, &meter, clock).unwrap();
    let pause = Arc::new(PublicationPause {
        entered: Barrier::new(2),
        resume: Barrier::new(2),
    });
    *observer.0.core.ring.pause.lock().unwrap() = Some(pause.clone());
    let drops = Arc::new(AtomicUsize::new(0));
    std::thread::scope(|scope| {
        let first = scope.spawn(|| {
            tx.try_reserve().unwrap().send(Item {
                bytes: 11,
                drops: drops.clone(),
            })
        });
        pause.entered.wait();
        tx.try_reserve().unwrap().send(Item {
            bytes: 22,
            drops: drops.clone(),
        });
        let (done, received) = std::sync::mpsc::channel();
        let receiver = &mut rx;
        let probe = scope.spawn(move || {
            done.send(receiver.poll_recv(&mut Context::from_waker(Waker::noop())))
                .unwrap();
        });
        let observed = received.recv_timeout(std::time::Duration::from_secs(1));
        pause.resume.wait();
        first.join().unwrap();
        probe.join().unwrap();
        assert!(
            matches!(observed, Ok(Poll::Pending)),
            "no spin, false EOF or skip across first CAS hole"
        );
    });
    assert_eq!(rx.try_recv().unwrap().bytes, 11);
    assert_eq!(rx.try_recv().unwrap().bytes, 22);
    drop((tx, rx));
    assert!(observer.finish().is_ok());
    assert_eq!(drops.load(Ordering::SeqCst), 2);
    assert_eq!(budget.used(), 0);
}
#[test]
fn actual_wrapper_fixed_record_exhaustion_returns_typed_capacity_and_original_value() {
    use crate::shared::bounded_channel::{SendError, native_channel};
    let budget = bytes::BackingBudget::new(1 << 22).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (clock, _facts) = clock(&meter);
    let (tx, rx, observer) = native_channel::<Item>(1, &meter, clock).unwrap();
    let held = tx.try_send(Item {
        bytes: 1,
        drops: Arc::new(AtomicUsize::new(0)),
    });
    assert!(held.is_ok());
    let mut pending = Vec::new();
    for _ in 0..4096 {
        let mut reserve = Box::pin(tx.reserve());
        assert!(
            reserve
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        pending.push(reserve);
    }
    let drops = Arc::new(AtomicUsize::new(0));
    let mut send = Box::pin(tx.send(Item {
        bytes: 9,
        drops: drops.clone(),
    }));
    let Poll::Ready(Err(SendError(original, reason))) =
        send.as_mut().poll(&mut Context::from_waker(Waker::noop()))
    else {
        panic!("must refuse at exact original record bound");
    };
    assert_eq!(reason, ReserveError::Capacity);
    assert_eq!(reason.reason(), "global_rx_capacity");
    assert_eq!(original.bytes, 9);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(original);
    drop(send);
    drop(pending);
    drop((tx, rx));
    assert!(observer.finish().is_ok());
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn actual_final_inventory_disagreement_cannot_become_clean_zero() {
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (clock, _facts) = clock(&meter);
    let (tx, rx, observer) = prepare::<Item>(2, 2, &meter, clock).unwrap();
    observer.0.core.bytes.store(1, Ordering::Release); // passive corruption of independent inventory
    drop((tx, rx));
    assert!(!observer.settled());
    let stock = budget.used();
    drop(observer);
    assert!(budget.quarantined());
    assert_eq!(budget.used(), stock);
    assert!(stock > 1);
}
