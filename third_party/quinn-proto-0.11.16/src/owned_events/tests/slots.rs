use super::*;

#[test]
fn operation_reserves_whole_burst_before_control_and_releases_unused_credit() {
    let budget = BackingBudget::new(4096).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (sender, mut receiver) = control_channel::<u64>(3, &meter).unwrap();
    let baseline = budget.used();
    let mut batch = sender.try_reserve_batch(3).unwrap();
    assert_eq!(batch.remaining(), 3);
    batch.send(11).unwrap();
    sender
        .try_reserve_control(ControlKind::Close)
        .unwrap()
        .send(99)
        .unwrap();
    let mut cx = Context::from_waker(Waker::noop());
    assert_eq!(receiver.poll_recv(&mut cx), Poll::Ready(Some(11)));
    assert!(receiver.poll_recv(&mut cx).is_pending());
    assert!(matches!(
        sender.try_reserve_batch(1),
        Err(ReserveError::Full)
    ));
    batch.send(22).unwrap();
    assert_eq!(receiver.poll_recv(&mut cx), Poll::Ready(Some(22)));
    drop(batch);
    assert_eq!(receiver.poll_recv(&mut cx), Poll::Ready(Some(99)));
    assert_eq!(budget.used(), baseline);
    let mut next = sender.try_reserve_batch(3).unwrap();
    for value in [1, 2, 3] {
        next.send(value).unwrap();
    }
    assert_eq!(next.remaining(), 0);
    assert_eq!(next.send(4), Err(4));
    drop(next);
    drop(sender);
    drop(receiver);
    assert_eq!(budget.used(), 0);
}

#[test]
fn completing_operation_wakes_producer_when_consumption_already_freed_slots() {
    let budget = BackingBudget::new(4096).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (sender, mut receiver) = channel::<u64>(3, &meter).unwrap();
    let mut batch = sender.try_reserve_batch(2).unwrap();
    batch.send(1).unwrap();
    let mut cx = Context::from_waker(Waker::noop());
    assert_eq!(receiver.poll_recv(&mut cx), Poll::Ready(Some(1)));
    let wake = Arc::new(CountWake(AtomicUsize::new(0)));
    let waker = Waker::from(wake.clone());
    let mut cx = Context::from_waker(&waker);
    assert!(sender.poll_reserve(&mut cx).is_pending());
    batch.send(2).unwrap();
    assert_eq!(wake.0.load(Ordering::Relaxed), 1);
    assert!(sender.poll_reserve(&mut cx).is_ready());
    drop(batch);
    drop(sender);
    drop(receiver);
    assert_eq!(budget.used(), 0);
}

#[test]
fn single_credit_commit_also_wakes_waiter_after_earlier_item_was_consumed() {
    let budget = BackingBudget::new(4096).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (sender, mut receiver) = channel::<u64>(3, &meter).unwrap();
    sender.try_reserve().unwrap().send(1).unwrap();
    let permit = sender.try_reserve().unwrap();
    let mut cx = Context::from_waker(Waker::noop());
    assert_eq!(receiver.poll_recv(&mut cx), Poll::Ready(Some(1)));
    let wake = Arc::new(CountWake(AtomicUsize::new(0)));
    let waker = Waker::from(wake.clone());
    let mut cx = Context::from_waker(&waker);
    assert!(sender.poll_reserve(&mut cx).is_pending());
    permit.send(2).unwrap();
    assert_eq!(wake.0.load(Ordering::Relaxed), 1);
    assert!(sender.poll_reserve(&mut cx).is_ready());
    drop(sender);
    drop(receiver);
    assert_eq!(budget.used(), 0);
}

#[test]
fn partial_operation_cancellation_unblocks_reserved_terminal_after_prior_items() {
    let budget = BackingBudget::new(4096).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (sender, mut receiver) = terminal_channel::<u64>(3, &meter).unwrap();
    let mut batch = sender.try_reserve_batch(3).unwrap();
    batch.send(1).unwrap();
    sender.send_terminal(99).unwrap();
    let mut cx = Context::from_waker(Waker::noop());
    assert_eq!(receiver.poll_recv(&mut cx), Poll::Ready(Some(1)));
    assert!(receiver.poll_recv(&mut cx).is_pending());
    drop(batch);
    assert_eq!(receiver.poll_recv(&mut cx), Poll::Ready(Some(99)));
    assert!(matches!(
        sender.try_reserve_batch(1),
        Err(ReserveError::Closed)
    ));
    drop(sender);
    drop(receiver);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn rejected_operation_leaves_slot_state_and_sequence_unchanged() {
    let budget = BackingBudget::new(4096).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (sender, receiver) = channel::<u64>(2, &meter).unwrap();
    sender.try_reserve().unwrap().send(7).unwrap();
    let before = {
        let state = sender.0.state.lock().unwrap();
        (state.len, state.next_seq, state.reserved_seq)
    };
    assert!(matches!(
        sender.try_reserve_batch(2),
        Err(ReserveError::Full)
    ));
    let after = {
        let state = sender.0.state.lock().unwrap();
        (state.len, state.next_seq, state.reserved_seq)
    };
    assert_eq!(before, after);
    let empty = sender.try_reserve_batch(0).unwrap();
    assert_eq!(empty.remaining(), 0);
    drop(empty);
    assert!(!budget.quarantined());
    drop(sender);
    drop(receiver);
    assert_eq!(budget.used(), 0);
}

#[test]
fn exact_slots_and_linear_permits_release_with_last_owner() {
    let budget = BackingBudget::new(1024).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (sender, mut receiver) = channel::<u64>(2, &meter).unwrap();
    assert_eq!(budget.used(), 2 * mem::size_of::<Slot<u64>>());

    let first = sender.try_reserve().unwrap();
    assert!(matches!(sender.try_reserve(), Err(ReserveError::Full)));
    first.send(11).unwrap();
    let abandoned = sender.try_reserve().unwrap();
    assert!(matches!(sender.try_reserve(), Err(ReserveError::Full)));
    drop(abandoned);
    let second = sender.try_reserve().unwrap();
    second.send(22).unwrap();
    assert!(matches!(sender.try_reserve(), Err(ReserveError::Full)));

    let mut cx = Context::from_waker(Waker::noop());
    assert_eq!(receiver.poll_recv(&mut cx), Poll::Ready(Some(11)));
    assert_eq!(receiver.poll_recv(&mut cx), Poll::Ready(Some(22)));
    drop(receiver);
    assert!(matches!(sender.try_reserve(), Err(ReserveError::Closed)));
    drop(sender);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

struct CountWake(AtomicUsize);

impl Wake for CountWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn full_producer_is_woken_after_real_consume() {
    let budget = BackingBudget::new(1024).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (sender, mut receiver) = channel::<u8>(1, &meter).unwrap();
    sender.try_reserve().unwrap().send(7).unwrap();
    let wake = Arc::new(CountWake(AtomicUsize::new(0)));
    let waker = Waker::from(wake.clone());
    let mut cx = Context::from_waker(&waker);
    assert!(sender.poll_reserve(&mut cx).is_pending());
    assert_eq!(wake.0.load(Ordering::Relaxed), 0);
    assert_eq!(receiver.poll_recv(&mut cx), Poll::Ready(Some(7)));
    assert_eq!(wake.0.load(Ordering::Relaxed), 1);
    assert!(sender.poll_reserve(&mut cx).is_ready());
    drop(receiver);
    drop(sender);
    assert_eq!(budget.used(), 0);
}

#[test]
fn insufficient_budget_rejects_before_slot_allocation() {
    let budget = BackingBudget::new(1).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    assert!(channel::<u64>(2, &meter).is_err());
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn terminal_event_survives_full_ordinary_lane_without_early_ack() {
    let budget = BackingBudget::new(1024).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (sender, mut receiver) = terminal_channel::<u64>(1, &meter).unwrap();
    assert_eq!(budget.used(), 2 * mem::size_of::<Slot<u64>>());

    sender.try_reserve().unwrap().send(11).unwrap();
    assert!(matches!(sender.try_reserve(), Err(ReserveError::Full)));
    sender.send_terminal(99).unwrap();
    assert_eq!(sender.send_terminal(100), Err(100));

    let mut cx = Context::from_waker(Waker::noop());
    assert_eq!(receiver.poll_recv(&mut cx), Poll::Ready(Some(11)));
    assert_eq!(receiver.poll_recv(&mut cx), Poll::Ready(Some(99)));
    assert!(receiver.poll_recv(&mut cx).is_pending());
    assert_eq!(budget.used(), 2 * mem::size_of::<Slot<u64>>());
    drop(receiver);
    drop(sender);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn close_drains_outside_lock_even_while_sender_and_permit_live() {
    let budget = BackingBudget::new(1024).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (sender, mut receiver) = terminal_channel::<u64>(1, &meter).unwrap();
    sender.try_reserve().unwrap().send(11).unwrap();
    sender.send_terminal(99).unwrap();
    let mut disposed = Vec::new();
    receiver.close_and_drain(|event| {
        assert!(matches!(sender.try_reserve(), Err(ReserveError::Closed)));
        disposed.push(event);
    });
    assert_eq!(disposed, [11, 99]);
    let mut cx = Context::from_waker(Waker::noop());
    assert_eq!(receiver.poll_recv(&mut cx), Poll::Ready(None));
    drop(receiver);
    drop(sender);
    assert_eq!(budget.used(), 0);
}

#[test]
fn zero_sized_payload_uses_nonzero_ordered_slot_layout() {
    let budget = BackingBudget::new(1024).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (sender, receiver) = channel::<std::convert::Infallible>(1, &meter).unwrap();
    assert_eq!(
        budget.used(),
        mem::size_of::<Slot<std::convert::Infallible>>()
    );
    drop(receiver);
    drop(sender);
    assert_eq!(budget.used(), 0);
}

#[test]
fn terminal_closes_admission_and_waits_for_prior_permit() {
    let budget = BackingBudget::new(1024).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (sender, mut receiver) = terminal_channel::<u64>(1, &meter).unwrap();
    let prior = sender.try_reserve().unwrap();
    sender.send_terminal(99).unwrap();
    assert!(matches!(sender.try_reserve(), Err(ReserveError::Closed)));

    let wake = Arc::new(CountWake(AtomicUsize::new(0)));
    let waker = Waker::from(wake.clone());
    let mut cx = Context::from_waker(&waker);
    assert!(receiver.poll_recv(&mut cx).is_pending());
    prior.send(11).unwrap();
    assert_eq!(wake.0.load(Ordering::Relaxed), 1);
    assert_eq!(receiver.poll_recv(&mut cx), Poll::Ready(Some(11)));
    assert_eq!(receiver.poll_recv(&mut cx), Poll::Ready(Some(99)));
    drop(receiver);
    drop(sender);
    assert_eq!(budget.used(), 0);
}

#[test]
fn abandoned_prior_permit_wakes_terminal_delivery() {
    let budget = BackingBudget::new(1024).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (sender, mut receiver) = terminal_channel::<u64>(1, &meter).unwrap();
    let prior = sender.try_reserve().unwrap();
    sender.send_terminal(99).unwrap();
    let mut cx = Context::from_waker(Waker::noop());
    assert!(receiver.poll_recv(&mut cx).is_pending());
    drop(prior);
    assert_eq!(receiver.poll_recv(&mut cx), Poll::Ready(Some(99)));
    drop(receiver);
    drop(sender);
    assert_eq!(budget.used(), 0);
}

#[test]
fn control_slots_preserve_ordinary_rebind_close_order_at_full() {
    let budget = BackingBudget::new(4096).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (sender, mut receiver) = control_channel::<u64>(1, &meter).unwrap();
    assert_eq!(budget.used(), 3 * mem::size_of::<Slot<u64>>());
    sender.try_reserve().unwrap().send(11).unwrap();
    sender
        .try_reserve_control(ControlKind::Rebind)
        .unwrap()
        .send(22)
        .unwrap();
    sender
        .try_reserve_control(ControlKind::Close)
        .unwrap()
        .send(33)
        .unwrap();
    assert!(matches!(sender.try_reserve(), Err(ReserveError::Full)));
    assert!(matches!(
        sender.try_reserve_control(ControlKind::Rebind),
        Err(ReserveError::Full)
    ));
    let mut cx = Context::from_waker(Waker::noop());
    assert_eq!(receiver.poll_recv(&mut cx), Poll::Ready(Some(11)));
    assert_eq!(receiver.poll_recv(&mut cx), Poll::Ready(Some(22)));
    sender.try_reserve().unwrap().send(44).unwrap();
    assert_eq!(receiver.poll_recv(&mut cx), Poll::Ready(Some(33)));
    assert_eq!(receiver.poll_recv(&mut cx), Poll::Ready(Some(44)));
    drop(receiver);
    drop(sender);
    assert_eq!(budget.used(), 0);
}

#[test]
fn abandoned_rebind_reservation_does_not_publish_or_leak() {
    let budget = BackingBudget::new(4096).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (sender, mut receiver) = control_channel::<u64>(1, &meter).unwrap();
    let pending = sender.try_reserve_control(ControlKind::Rebind).unwrap();
    assert!(matches!(
        sender.try_reserve_control(ControlKind::Rebind),
        Err(ReserveError::Full)
    ));
    drop(pending);
    sender
        .try_reserve_control(ControlKind::Rebind)
        .unwrap()
        .send(7)
        .unwrap();
    let mut cx = Context::from_waker(Waker::noop());
    assert_eq!(receiver.poll_recv(&mut cx), Poll::Ready(Some(7)));
    drop(receiver);
    drop(sender);
    assert_eq!(budget.used(), 0);
}

#[test]
fn earlier_ordinary_reservation_blocks_later_control_delivery() {
    let budget = BackingBudget::new(4096).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (sender, mut receiver) = control_channel::<u64>(1, &meter).unwrap();
    let prior = sender.try_reserve().unwrap();
    sender
        .try_reserve_control(ControlKind::Rebind)
        .unwrap()
        .send(22)
        .unwrap();
    let mut cx = Context::from_waker(Waker::noop());
    assert!(receiver.poll_recv(&mut cx).is_pending());
    prior.send(11).unwrap();
    assert_eq!(receiver.poll_recv(&mut cx), Poll::Ready(Some(11)));
    assert_eq!(receiver.poll_recv(&mut cx), Poll::Ready(Some(22)));
    drop(receiver);
    drop(sender);
    assert_eq!(budget.used(), 0);
}

#[test]
fn earlier_control_reservation_blocks_later_ordinary_delivery() {
    let budget = BackingBudget::new(4096).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (sender, mut receiver) = control_channel::<u64>(1, &meter).unwrap();
    let prior = sender.try_reserve_control(ControlKind::Rebind).unwrap();
    sender.try_reserve().unwrap().send(22).unwrap();
    let mut cx = Context::from_waker(Waker::noop());
    assert!(receiver.poll_recv(&mut cx).is_pending());
    prior.send(11).unwrap();
    assert_eq!(receiver.poll_recv(&mut cx), Poll::Ready(Some(11)));
    assert_eq!(receiver.poll_recv(&mut cx), Poll::Ready(Some(22)));
    drop(receiver);
    drop(sender);
    assert_eq!(budget.used(), 0);
}
