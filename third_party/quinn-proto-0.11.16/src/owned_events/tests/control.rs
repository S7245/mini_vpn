use super::*;

#[test]
fn held_control_pins_receiver_through_all_fixed_slot_commits() {
    let budget = BackingBudget::new(4096).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (a, mut ar) = control_channel::<u64>(2, &meter).unwrap();
    let (b, mut br) = control_channel::<u64>(2, &meter).unwrap();
    let mut ap = a.try_reserve_control(ControlKind::Rebind).unwrap();
    let mut bp = b.try_reserve_control(ControlKind::Rebind).unwrap();
    assert!(ap.belongs_to(&a, ControlKind::Rebind));
    assert!(!ap.belongs_to(&b, ControlKind::Rebind));
    assert!(!ap.belongs_to(&a, ControlKind::Close));
    assert!(!ap.same_lane(&bp));
    let ah = ap.hold().unwrap();
    let bh = bp.hold().unwrap();
    let ac = ah.commit(10);
    assert!(a.0.state.try_lock().is_err());
    assert!(b.0.state.try_lock().is_err());
    let bc = bh.commit(20);
    ac.unlock();
    bc.unlock();
    let mut cx = Context::from_waker(Waker::noop());
    assert_eq!(ar.poll_recv(&mut cx), Poll::Ready(Some(10)));
    assert_eq!(br.poll_recv(&mut cx), Poll::Ready(Some(20)));
    let successor = a.try_reserve_control(ControlKind::Rebind).unwrap();
    drop(ap); // old consumed ticket must not clear a successor reservation
    assert!(matches!(
        a.try_reserve_control(ControlKind::Rebind),
        Err(ReserveError::Full)
    ));
    drop((successor, bp, a, ar, b, br));
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn receiver_close_before_hold_rejects_without_publication() {
    let budget = BackingBudget::new(4096).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (sender, receiver) = control_channel::<u64>(2, &meter).unwrap();
    let mut ticket = sender.try_reserve_control(ControlKind::Close).unwrap();
    drop(receiver);
    assert!(matches!(ticket.hold(), Err(ReserveError::Closed)));
    assert!(sender
        .0
        .state
        .lock()
        .unwrap()
        .slots
        .iter()
        .all(|slot| slot.value.is_none()));
    drop((ticket, sender));
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn held_control_blocks_actual_receiver_drop_until_publication() {
    let budget = BackingBudget::new(4096).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (sender, receiver) = control_channel::<u64>(2, &meter).unwrap();
    let mut ticket = sender.try_reserve_control(ControlKind::Close).unwrap();
    let held = ticket.hold().unwrap();
    let done = Arc::new(AtomicUsize::new(0));
    std::thread::scope(|scope| {
        let done = done.clone();
        let (started, start) = std::sync::mpsc::sync_channel(0);
        let worker = scope.spawn(move || {
            started.send(()).unwrap();
            drop(receiver);
            done.store(1, Ordering::Release);
        });
        start.recv().unwrap();
        // Direct physical mutex check proves receiver cannot enter close_and_drain.
        assert!(sender.0.state.try_lock().is_err());
        let published = held.commit(42);
        assert!(sender.0.state.try_lock().is_err());
        published.unlock();
        worker.join().unwrap();
    });
    assert_eq!(done.load(Ordering::Acquire), 1);
    drop((ticket, sender));
    assert_eq!(budget.used(), 0);
}

#[test]
fn held_cancel_returns_original_sequence_without_clearing_other_control() {
    let budget = BackingBudget::new(4096).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (sender, mut receiver) = control_channel::<u64>(2, &meter).unwrap();
    let mut ticket = sender.try_reserve_control(ControlKind::Rebind).unwrap();
    sender
        .try_reserve_control(ControlKind::Close)
        .unwrap()
        .send(9)
        .unwrap();
    let held = ticket.hold().unwrap();
    drop(held);
    drop(ticket);
    assert_eq!(
        receiver.poll_recv(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Some(9))
    );
    drop((sender, receiver));
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn held_unwind_quarantines_before_observation_and_cleanup_does_not_wake_again() {
    struct PanicWake;
    impl Wake for PanicWake {
        fn wake(self: Arc<Self>) {
            panic!("secondary wake forbidden");
        }
    }
    let budget = BackingBudget::new(4096).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (sender, mut receiver) = control_channel::<u64>(2, &meter).unwrap();
    let waker = Waker::from(Arc::new(PanicWake));
    receiver.poll_recv(&mut Context::from_waker(&waker));
    let payload = Arc::new(());
    let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut ticket = sender.try_reserve_control(ControlKind::Close).unwrap();
        let _held = ticket.hold().unwrap();
        std::panic::panic_any(payload.clone());
    }))
    .unwrap_err();
    assert!(Arc::ptr_eq(
        caught.downcast_ref::<Arc<()>>().unwrap(),
        &payload
    ));
    assert!(budget.quarantined());
    assert_eq!(
        receiver.poll_recv(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(None)
    );
    assert!(matches!(
        sender.try_reserve_control(ControlKind::Close),
        Err(ReserveError::Closed)
    ));
    drop((sender, receiver));
    assert_eq!(budget.used(), 1);
}

#[test]
fn damaged_sequence_cannot_commit_or_erase_another_reservation() {
    let budget = BackingBudget::new(4096).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (sender, receiver) = control_channel::<u64>(2, &meter).unwrap();
    let mut ticket = sender.try_reserve_control(ControlKind::Close).unwrap();
    sender.0.state.lock().unwrap().control_reserved[1] = Some(ticket.seq + 1);
    assert!(matches!(ticket.hold(), Err(ReserveError::Closed)));
    let next = ticket.seq + 1;
    drop(ticket);
    assert_eq!(
        sender.0.state.lock().unwrap().control_reserved[1],
        Some(next)
    );
    assert!(budget.quarantined());
    drop((sender, receiver));
    assert_eq!(budget.used(), 1);
}
