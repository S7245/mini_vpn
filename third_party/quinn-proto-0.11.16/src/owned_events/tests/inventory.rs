//! Cold-only identity and physical ring validation for the real ordinary lane.
use super::*;

#[test]
fn cold_inventory_accepts_wrap_and_cancel_gaps_with_original_sender() {
    let budget = BackingBudget::new(4096).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (sender, mut receiver) = channel::<u64>(3, &meter).unwrap();
    let identity = sender.channel_identity();
    receiver
        .validate_ordinary(&meter, 3, &identity, true)
        .unwrap();
    for value in 0..6 {
        drop(sender.try_reserve().unwrap()); // Legal sequence gap.
        sender.try_reserve().unwrap().send(value).unwrap();
        let mut held = sender.try_reserve_batch(2).unwrap();
        held.send(value + 10).unwrap();
        receiver
            .validate_ordinary(&meter, 3, &identity, false)
            .unwrap();
        drop(held);
        assert_eq!(receiver.try_recv().unwrap(), value);
        receiver
            .validate_ordinary(&meter, 3, &identity, false)
            .unwrap();
        assert_eq!(receiver.try_recv().unwrap(), value + 10);
    }
    drop(sender);
    drop(receiver);
    // A Weak identity is not another allocation owner or a refund authority.
    assert_eq!(budget.used(), 0);
    drop(identity);
}

#[test]
fn cold_inventory_rejects_wrong_lane_even_under_the_same_meter() {
    let budget = BackingBudget::new(4096).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (sender, receiver) = channel::<u64>(2, &meter).unwrap();
    let (other, other_receiver) = channel::<u64>(2, &meter).unwrap();
    assert!(receiver
        .validate_ordinary(&meter, 2, &other.channel_identity(), false)
        .is_err());
    drop((sender, receiver, other, other_receiver));
    assert!(budget.quarantined());
    assert_eq!(budget.used(), 1);
}

#[test]
fn cold_inventory_rejects_physical_position_count_layout_and_sequence_damage() {
    for damage in 0..6 {
        let budget = BackingBudget::new(4096).unwrap();
        let meter = BackingMeter::with_budget(&budget);
        let (sender, receiver) = channel::<u64>(3, &meter).unwrap();
        sender.try_reserve().unwrap().send(17).unwrap();
        let held = sender.try_reserve_batch(2).unwrap();
        {
            let mut state = receiver.0.state.lock().unwrap();
            match damage {
                0 => state.slots.swap(0, 2), // Same Some count, wrong position.
                1 => state.len = 0,
                2 => state._reservation = budget.try_reserve(1).unwrap(),
                3 => state.slots[0].seq = state.next_seq,
                4 => state.reserved_seq = Some(state.next_seq),
                5 => state.ordinary_capacity = 2,
                _ => unreachable!(),
            }
        }
        assert!(
            receiver
                .validate_ordinary(&meter, 3, &sender.channel_identity(), false)
                .is_err(),
            "damage {damage}"
        );
        drop(held);
        drop(sender);
        drop(receiver);
        assert!(budget.quarantined());
        assert_eq!(budget.used(), 1);
    }
}

#[test]
fn cold_inventory_expected_profile_rejection_keeps_valid_original_lane_clean() {
    let budget = BackingBudget::new(4096).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let foreign = BackingMeter::with_budget(&budget);
    let (sender, mut receiver) = channel::<u64>(3, &meter).unwrap();
    sender.try_reserve().unwrap().send(17).unwrap();
    let identity = sender.channel_identity();
    assert!(receiver
        .validate_ordinary(&meter, 2, &identity, false)
        .is_err());
    assert!(receiver
        .validate_ordinary(&foreign, 3, &identity, false)
        .is_err());
    assert!(receiver
        .validate_ordinary(&meter, 3, &identity, true)
        .is_err());
    receiver
        .validate_ordinary(&meter, 3, &identity, false)
        .unwrap();
    assert_eq!(receiver.try_recv().unwrap(), 17);
    drop((sender, receiver, identity));
    assert!(!budget.quarantined());
    assert_eq!(budget.used(), 0);
}

#[test]
fn cold_inventory_rejects_healthy_other_profiles_without_quarantine() {
    let budget = BackingBudget::new(4096).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    for terminal in [false, true] {
        let (sender, receiver) = if terminal {
            terminal_channel::<u64>(2, &meter)
        } else {
            control_channel::<u64>(2, &meter)
        }
        .unwrap();
        assert_eq!(
            receiver
                .validate_ordinary(&meter, 2, &sender.channel_identity(), true)
                .unwrap_err()
                .kind(),
            io::ErrorKind::Unsupported
        );
        drop((sender, receiver));
        assert!(!budget.quarantined());
        assert_eq!(budget.used(), 0);
    }
}
