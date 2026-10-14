use super::super::*;
use std::future::Future;

fn full_lease() -> (
    TcpPoolSlotLease,
    Arc<TcpPoolGenerationActivity>,
    Arc<AtomicU64>,
    Arc<AtomicU64>,
) {
    let generation = Arc::new(TcpPoolGenerationActivity::new());
    let slot = Arc::new(AtomicU64::new(0));
    let total = Arc::new(AtomicU64::new(0));
    let lease =
        TcpPoolSlotLease::reserve_generation(generation.clone(), slot.clone(), total.clone())
            .unwrap();
    (lease, generation, slot, total)
}

#[test]
fn release_once_retains_each_debit_and_cannot_steal_a_later_owner() {
    for mask in 0..8 {
        let (mut lease, generation, slot, total) = full_lease();
        let counters = [&generation.active, &*slot, &*total];
        for (i, counter) in counters.iter().enumerate() {
            if mask & (1 << i) != 0 {
                counter.store(0, Ordering::Release);
            }
        }
        let report = lease.release_once();
        assert_eq!(report.complete(), mask == 0);
        for (i, actual) in [report.generation, report.slot, report.total]
            .iter()
            .enumerate()
        {
            assert_eq!(
                *actual,
                if mask & (1 << i) != 0 {
                    CounterRelease::Underflow { observed: 0 }
                } else {
                    CounterRelease::Decremented {
                        before: 1,
                        after: 0,
                    }
                }
            );
        }
        assert_eq!(lease.release_once(), report);
        assert!(matches!(
            lease.try_clone(),
            Err(TcpPoolReservationError::LeaseNotActive)
        ));
        // Replenished counters belong exclusively to the new owner.
        let next =
            TcpPoolSlotLease::reserve_generation(generation.clone(), slot.clone(), total.clone())
                .unwrap();
        drop(lease);
        assert_eq!(
            (
                generation.active(),
                slot.load(Ordering::Acquire),
                total.load(Ordering::Acquire)
            ),
            (1, 1, 1)
        );
        drop(next);
        assert_eq!(
            (
                generation.active(),
                slot.load(Ordering::Acquire),
                total.load(Ordering::Acquire)
            ),
            (0, 0, 0)
        );
    }
}

#[tokio::test]
async fn release_reports_one_owner_and_preserves_the_real_zero_notification() {
    let (mut lease, generation, slot, total) = full_lease();
    let other = lease.try_clone().unwrap();
    let report = lease.release_once();
    assert!(report.complete());
    assert_eq!(
        report.generation,
        CounterRelease::Decremented {
            before: 2,
            after: 1
        }
    );
    assert_eq!(
        (
            generation.active(),
            slot.load(Ordering::Acquire),
            total.load(Ordering::Acquire)
        ),
        (1, 1, 1)
    );
    let zero = generation.zero.notified();
    tokio::pin!(zero);
    assert!(
        std::future::poll_fn(|cx| Poll::Ready(zero.as_mut().poll(cx)))
            .await
            .is_pending()
    );
    drop(lease);
    assert_eq!(generation.active(), 1);
    drop(other);
    tokio::time::timeout(Duration::from_secs(1), zero)
        .await
        .unwrap();
    assert_eq!(generation.active(), 0);
}

#[test]
fn incomplete_backing_or_interrupted_release_cannot_be_a_full_receipt() {
    let mut slot_only = TcpPoolSlotLease::from_reserved(Arc::new(AtomicU64::new(1)));
    let report = slot_only.release_once();
    assert!(!report.complete());
    assert_eq!(report.generation, CounterRelease::Absent);
    assert_eq!(report.total, CounterRelease::Absent);

    struct PanicWake;
    impl std::task::Wake for PanicWake {
        fn wake(self: Arc<Self>) {
            panic!("injected real zero waiter wake");
        }
        fn wake_by_ref(self: &Arc<Self>) {
            panic!("injected real zero waiter wake");
        }
    }
    let (mut lease, generation, slot, total) = full_lease();
    let mut notified = Box::pin(generation.zero.notified());
    let waker = std::task::Waker::from(Arc::new(PanicWake));
    assert!(
        notified
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    let interrupted =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| lease.release_once()));
    assert!(interrupted.is_err());
    let retained = lease.release_once();
    assert_eq!(
        retained.generation,
        CounterRelease::Decremented {
            before: 1,
            after: 0
        }
    );
    assert_eq!(retained.slot, CounterRelease::Pending);
    assert_eq!(retained.total, CounterRelease::Pending);
    assert!(!retained.complete());
    drop(lease);
    assert_eq!(
        (slot.load(Ordering::Acquire), total.load(Ordering::Acquire)),
        (1, 1)
    );
}

#[test]
fn retained_lease_drop_and_clone_cannot_forge_settlement() {
    let (mut lease, generation, slot, total) = full_lease();
    lease.retain_until_release();
    assert!(matches!(
        lease.try_clone(),
        Err(TcpPoolReservationError::LeaseNotActive)
    ));
    drop(lease);
    assert_eq!(
        (
            generation.active(),
            slot.load(Ordering::Acquire),
            total.load(Ordering::Acquire)
        ),
        (1, 1, 1)
    );
    let (mut lease, generation, slot, total) = full_lease();
    lease.retain_until_release();
    assert!(lease.release_once().complete());
    drop(lease);
    assert_eq!(
        (
            generation.active(),
            slot.load(Ordering::Acquire),
            total.load(Ordering::Acquire)
        ),
        (0, 0, 0)
    );
}
