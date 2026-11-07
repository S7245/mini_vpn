use super::*;

#[test]
fn retirement_preparation_full_cancellation_and_control_sharing_return_exact_stock() {
    let (budget, pool) = owner(2);
    let mut pending = Queue::<u64>::new(Some(&pool));
    let reset = Queue::new(Some(&pool));
    let held = reset.prepare(value(4)).unwrap();
    let mut candidate = pending.empty_like();
    candidate.push(candidate.prepare(10).unwrap());
    assert!(candidate.prepare(11).is_err());
    assert!(pending.is_empty());
    drop(candidate); // Partial prepare rolls back only its own exact node.
    assert_eq!(pending.cold_stock(), Some((2, 1, 0)));
    drop(held);
    for n in [10, 11] {
        pending.push(pending.prepare(n).unwrap());
    }
    let mut sent = pending.empty_like();
    for n in [11, 10] {
        let mut entry = pending.pop().unwrap();
        assert_eq!(entry.value(), n);
        entry.begin_mutation();
        sent.push(entry);
    }
    assert_eq!(pending.cold_stock(), Some((2, 0, 2)));
    assert_eq!(crate::tests::allocations_in(|| pending.append(sent)), 0);
    assert_eq!(pending.iter().collect::<Vec<_>>(), [11, 10]);
    assert_eq!(pending.pop().unwrap().value(), 10);
    assert_eq!(pending.pop().unwrap().value(), 11);
    assert_eq!(pending.cold_stock(), Some((2, 2, 0)));
    drop(pending);
    drop(reset);
    drop(pool);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn interrupted_retirement_encoding_keeps_shared_unknown_charge_after_known_drop() {
    let (budget, pool) = owner(1);
    let mut queue = Queue::<u64>::new(Some(&pool));
    queue.push(queue.prepare(10).unwrap());
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut entry = queue.pop().unwrap();
        entry.begin_mutation();
        panic!("injected retirement writer interruption");
    }));
    assert!(result.is_err());
    assert_eq!(queue.cold_stock(), Some((1, 1, 0)));
    assert!(queue.prepare(11).is_err());
    drop(queue);
    drop(pool);
    assert_eq!(budget.used(), 1);
    assert!(budget.quarantined());
}
