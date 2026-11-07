use super::*;

#[test]
fn cancelled_event_growth_refunds_and_reserved_publication_needs_no_allocation() {
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let mut queue = Queue::new(Some(meter));
    let first = queue.prepare(8, 2).unwrap();
    assert!(budget.used() > 0);
    drop(first);
    assert_eq!(budget.used(), 0);
    assert!(queue.pop_front().is_none());
    let plan = queue.prepare(8, 2).unwrap();
    let pressure = budget.try_reserve(budget.limit() - budget.used()).unwrap();
    queue.commit(plan).unwrap();
    for _ in 0..8 {
        queue
            .push_reserved(QueuedEvent::DatagramReceived, 2)
            .unwrap();
    }
    queue
        .push_reserved(QueuedEvent::DatagramsUnblocked, 1)
        .unwrap();
    queue.push_reserved(QueuedEvent::Connected, 0).unwrap();
    for _ in 0..8 {
        assert!(matches!(queue.pop_front(), Some(Event::DatagramReceived)));
    }
    assert!(matches!(queue.pop_front(), Some(Event::DatagramsUnblocked)));
    assert!(matches!(queue.pop_front(), Some(Event::Connected)));
    assert!(queue.pop_front().is_none());
    drop((pressure, queue));
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn event_growth_full_preserves_existing_events_and_held_terminal_slot() {
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let mut queue = Queue::new(Some(BackingMeter::with_budget(&budget)));
    queue.reserve(3, 1).unwrap();
    for _ in 0..3 {
        queue
            .push_reserved(QueuedEvent::DatagramReceived, 1)
            .unwrap();
    }
    let before = queue.snapshot();
    let pressure = budget.try_reserve(budget.limit() - budget.used()).unwrap();
    assert!(matches!(
        queue.prepare(1, 1),
        Err(MeteredCopyError::BudgetFull)
    ));
    assert_eq!(queue.snapshot(), before);
    queue
        .push_reserved(QueuedEvent::DatagramsUnblocked, 0)
        .unwrap();
    for _ in 0..3 {
        assert!(matches!(queue.pop_front(), Some(Event::DatagramReceived)));
    }
    assert!(matches!(queue.pop_front(), Some(Event::DatagramsUnblocked)));
    drop((pressure, queue));
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn unknown_event_commit_interruption_preserves_nonzero_quarantine() {
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let interrupted = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut queue = Queue::new(Some(meter));
        queue.reserve(1, 0).unwrap();
        queue
            .push_reserved(QueuedEvent::DatagramReceived, 0)
            .unwrap();
        panic!("interrupt the physical application-event owner");
    }));
    assert!(interrupted.is_err());
    assert!(budget.quarantined());
    assert_eq!(
        budget.used(),
        1,
        "known physical destruction must not normalize Q to zero"
    );
}

#[test]
fn unqualified_event_layout_retains_actual_stock_and_charge() {
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let mut queue = Queue::new(Some(BackingMeter::with_budget(&budget)));
    queue.reserve(1, 0).unwrap();
    queue
        .push_reserved(QueuedEvent::DatagramReceived, 0)
        .unwrap();
    let misplaced = queue.allowance.take();
    let charge = budget.used();
    drop(queue);
    assert!(budget.quarantined());
    assert_eq!(budget.used(), charge);
    assert!(charge > 1);
    // The fault holder also retains the displaced original receipt. Neither
    // missing custody nor the invalid queue is converted to a clean refund.
    std::mem::forget(misplaced);
}
