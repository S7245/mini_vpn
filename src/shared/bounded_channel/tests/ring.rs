//! Actual physical ring layout and transferred values, not logical credit.
use super::super::ring::{Pop, Ring};
#[test]
fn exact_cold_slots_full_fifo_wrap_and_last_reference() {
    let bytes = Ring::<usize>::layout_bytes(1024).unwrap();
    for limit in [bytes - 1, bytes] {
        let budget = bytes::BackingBudget::new(limit).unwrap();
        let meter = bytes::BackingMeter::with_budget(&budget);
        let ring = Ring::prepare(1024, &meter);
        if limit < bytes {
            assert!(matches!(ring, Err(bytes::MeteredCopyError::TooLarge)));
        } else {
            let ring = std::sync::Arc::new(ring.unwrap());
            for lap in 0..3 {
                for i in 0..1024 {
                    assert!(ring.try_push(lap * 1024 + i).is_ok());
                }
                assert_eq!(ring.try_push(usize::MAX), Err(usize::MAX));
                for i in 0..1024 {
                    assert!(matches!(ring.pop(), Pop::Value(value) if value == lap * 1024 + i));
                }
                assert!(matches!(ring.pop(), Pop::Empty));
            }
            let last = ring.clone();
            drop(ring);
            assert_eq!(budget.used(), bytes);
            drop(last);
        }
        assert_eq!(budget.used(), 0);
        assert!(!budget.quarantined());
    }
}

#[test]
fn unfinished_first_publication_is_inflight_and_does_not_skip_to_the_second() {
    use super::super::ring::PublicationPause;
    use std::sync::{Arc, Barrier};
    let budget = bytes::BackingBudget::new(4096).unwrap();
    let ring = Ring::prepare(2, &bytes::BackingMeter::with_budget(&budget)).unwrap();
    let pause = Arc::new(PublicationPause {
        entered: Barrier::new(2),
        resume: Barrier::new(2),
    });
    *ring.pause.lock().unwrap() = Some(pause.clone());
    std::thread::scope(|scope| {
        let original = scope.spawn(|| ring.try_push(11));
        pause.entered.wait(); // original real tail CAS has completed
        assert_eq!(ring.try_push(22), Ok(()));
        let (done, observation) = std::sync::mpsc::channel();
        let observed_ring = &ring;
        let probe = scope.spawn(move || {
            let result = observed_ring.pop();
            let _ = done.send(result);
        });
        let result = observation.recv_timeout(std::time::Duration::from_secs(1));
        pause.resume.wait(); // release original publication before any assertion unwinds
        assert_eq!(original.join().unwrap(), Ok(()));
        probe.join().unwrap();
        assert!(
            matches!(result, Ok(Pop::InFlight)),
            "no spin on publisher and no false empty/skip"
        );
    });
    assert!(matches!(ring.pop(), Pop::Value(11)));
    assert!(matches!(ring.pop(), Pop::Value(22)));
    drop(ring);
    assert_eq!(budget.used(), 0);
}

#[test]
fn concurrent_producers_preserve_each_actual_fifo_and_all_40000_values() {
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let ring = Ring::prepare(1024, &bytes::BackingMeter::with_budget(&budget)).unwrap();
    let mut counts = [0usize; 4];
    std::thread::scope(|scope| {
        for producer in 0..4 {
            let ring = &ring;
            scope.spawn(move || {
                for sequence in 0..10000 {
                    let mut value = (producer, sequence);
                    loop {
                        match ring.try_push(value) {
                            Ok(()) => break,
                            Err(returned) => {
                                value = returned;
                                std::thread::yield_now();
                            }
                        }
                    }
                }
            });
        }
        for _ in 0..40000 {
            loop {
                match ring.pop() {
                    Pop::Value((producer, sequence)) => {
                        assert_eq!(sequence, counts[producer]);
                        counts[producer] += 1;
                        break;
                    }
                    Pop::Empty | Pop::InFlight => std::thread::yield_now(),
                }
            }
        }
    });
    assert_eq!(counts, [10000; 4]);
    assert!(matches!(ring.pop(), Pop::Empty));
    drop(ring);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

struct OriginalDrop(std::sync::Arc<std::sync::atomic::AtomicUsize>);
impl Drop for OriginalDrop {
    fn drop(&mut self) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        panic!("unknown original ring value must not be automatically disposed");
    }
}
#[test]
fn unknown_final_stock_keeps_original_values_and_actual_charged_allocation() {
    let stock = Ring::<(bytes::Bytes, OriginalDrop)>::layout_bytes(2).unwrap();
    let budget = bytes::BackingBudget::new(stock + 4096).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let drops = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let ring = Ring::prepare(2, &meter).unwrap();
    let bytes = bytes::Bytes::copy_from_slice_metered(b"old", &meter).unwrap();
    let backing = meter.snapshot().physical_bytes;
    assert!(backing > 3);
    assert_eq!(meter.snapshot().payload_bytes, 3);
    assert_eq!(budget.used(), stock + backing);
    assert!(ring.try_push((bytes, OriginalDrop(drops.clone()))).is_ok());
    drop(ring);
    assert!(budget.quarantined());
    assert_eq!(budget.used(), stock + backing);
    assert_eq!(drops.load(std::sync::atomic::Ordering::SeqCst), 0);
}

#[test]
fn stale_tail_with_a_free_slot_does_not_reject_an_already_admitted_publisher() {
    use super::super::ring::PublicationPause;
    use std::sync::{Arc, Barrier};
    let budget = bytes::BackingBudget::new(4096).unwrap();
    let ring = Ring::prepare(2, &bytes::BackingMeter::with_budget(&budget)).unwrap();
    let pause = Arc::new(PublicationPause {
        entered: Barrier::new(2),
        resume: Barrier::new(2),
    });
    *ring.before_claim.lock().unwrap() = Some(pause.clone());
    let published = std::thread::scope(|scope| {
        let original = scope.spawn(|| ring.try_push(11));
        pause.entered.wait();
        assert_eq!(ring.try_push(22), Ok(()));
        pause.resume.wait();
        original.join().unwrap()
    });
    let first = ring.pop();
    let second = ring.pop();
    drop(ring);
    assert_eq!(
        published,
        Ok(()),
        "single original call, no outer retry to conceal false Full"
    );
    assert!(matches!(first, Pop::Value(22)));
    assert!(matches!(second, Pop::Value(11)));
    assert_eq!(budget.used(), 0);
}
