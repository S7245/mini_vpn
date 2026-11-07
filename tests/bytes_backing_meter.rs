//! Original Shared header owns one Node charge across all datagram slices.

use bytes::{BackingBudget, BackingMeter, Bytes, BytesMut, MeteredCopyError};

#[test]
fn two_nodes_cannot_reserve_more_than_one_shared_backing_budget() {
    let budget = BackingBudget::new(128).unwrap();
    let first = budget.try_reserve(80).expect("first generation");
    let second = budget.clone();
    let rejected = std::thread::spawn(move || second.try_reserve(80).is_none())
        .join()
        .unwrap();
    assert!(rejected);
    assert_eq!(budget.used(), 80);
    drop(first);
    assert_eq!(budget.used(), 0);
    let successor = budget.try_reserve(80).expect("released physical capacity");
    assert_eq!(budget.used(), 80);
    drop(successor);
    assert_eq!(budget.used(), 0);
}

#[test]
fn budgeted_copy_refuses_before_allocation_and_releases_at_last_reference() {
    let budget = BackingBudget::new(256).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let input = [7u8; 200];
    let first = Bytes::copy_from_slice_metered(&input, &meter).expect("first backing");
    let held = first.clone();
    let charged = budget.used();
    assert_eq!(charged, meter.snapshot().physical_bytes);
    assert_eq!(&first[..], &input);
    assert!(Bytes::copy_from_slice_metered(&input, &meter).is_none());
    assert_eq!(
        budget.used(),
        charged,
        "rejection preserves the original charge"
    );
    assert_eq!(meter.snapshot().allocations, 1);
    drop(first);
    assert_eq!(
        budget.used(),
        charged,
        "another Flow still owns this backing"
    );
    drop(held);
    assert_eq!(budget.used(), 0);
    assert_eq!(meter.snapshot().physical_bytes, 0);
    assert!(!meter.snapshot().quarantined);
}

#[test]
fn ordered_copy_keeps_source_and_copied_backings_separately_charged() {
    let budget = BackingBudget::new(500).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let source = Bytes::copy_from_slice_metered(&[3u8; 200], &meter).unwrap();
    let source_charge = budget.used();
    let copied = Bytes::copy_slices_metered(&[source.clone()], &meter).unwrap();
    let both_charges = budget.used();
    assert!(both_charges > source_charge);
    assert_eq!(both_charges, meter.snapshot().physical_bytes);
    assert!(Bytes::copy_slices_metered(&[source.clone()], &meter).is_none());
    assert_eq!(budget.used(), both_charges);
    assert_eq!(&source[..], &copied[..]);
    drop(copied);
    assert_eq!(budget.used(), source_charge);
    drop(source);
    assert_eq!(budget.used(), 0);
    assert!(!meter.snapshot().quarantined);
}

#[test]
fn budgeted_inline_vec_escape_keeps_unknown_debt_and_blocks_another_node() {
    let budget = BackingBudget::new(512).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let backing = Bytes::copy_from_slice_metered(&[5u8; 64], &meter).unwrap();
    let charged = budget.used();
    let escaped: Vec<u8> = backing.into();
    assert_eq!(escaped, [5u8; 64]);
    assert!(meter.snapshot().quarantined && budget.quarantined());
    assert!(
        budget.used() >= charged,
        "unknown escaped backing remains charged"
    );
    let another = BackingMeter::with_budget(&budget);
    assert!(Bytes::copy_from_slice_metered(b"another", &another).is_none());
}

#[test]
fn budgeted_inline_mut_escape_keeps_unknown_debt() {
    let budget = BackingBudget::new(512).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let backing = Bytes::copy_from_slice_metered(&[6u8; 64], &meter).unwrap();
    let charged = budget.used();
    let escaped: BytesMut = backing.into();
    assert_eq!(&escaped[..], &[6u8; 64]);
    assert!(meter.snapshot().quarantined && budget.quarantined());
    assert!(budget.used() >= charged);
    assert!(budget.try_reserve(1).is_none());
}

#[test]
fn late_shared_charge_quarantines_common_budget_without_clean_zero() {
    let budget = BackingBudget::new(512).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let mut allocated = BytesMut::with_capacity(64);
    allocated.extend_from_slice(b"late");
    let split = allocated.split_to(4);
    assert!(!split.charge_shared_backing(&meter));
    drop(split);
    drop(allocated);
    assert!(meter.snapshot().quarantined && budget.quarantined());
    assert!(budget.used() > 0, "unknown allocation remains charged Q");
    assert!(budget.try_reserve(1).is_none());
}

#[test]
fn metered_datagram_copy_precharges_one_shared_backing_across_slices() {
    let budget = BackingBudget::new(200).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let mut packet = BytesMut::copy_from_slice_metered(&[9u8; 64], &meter).unwrap();
    let first = packet.split_to(32).freeze();
    let second = packet.freeze();
    assert_eq!(first.allocation_identity(), second.allocation_identity());
    let charged = budget.used();
    assert_eq!(charged, meter.snapshot().physical_bytes);
    assert!(BytesMut::copy_from_slice_metered(&[9u8; 64], &meter).is_none());
    assert_eq!(budget.used(), charged);
    drop(first);
    assert_eq!(budget.used(), charged);
    drop(second);
    assert_eq!(budget.used(), 0);
    assert!(!meter.snapshot().quarantined);
}

#[test]
fn empty_metered_mutable_backing_cannot_escape_future_writes() {
    let budget = BackingBudget::new(200).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    assert!(BytesMut::copy_from_slice_metered(&[], &meter).is_none());
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn datagram_budget_full_is_recoverable_but_quarantine_is_not() {
    let budget = BackingBudget::new(200).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let held = BytesMut::try_copy_from_slice_metered(&[1u8; 64], &meter).unwrap();
    let charged = budget.used();
    assert!(matches!(
        BytesMut::try_copy_from_slice_metered(&[2u8; 64], &meter),
        Err(MeteredCopyError::BudgetFull)
    ));
    assert_eq!(budget.used(), charged);
    assert!(!budget.quarantined());
    meter.quarantine();
    assert!(matches!(
        BytesMut::try_copy_from_slice_metered(&[2u8; 64], &meter),
        Err(MeteredCopyError::Quarantined)
    ));
    drop(held);
    assert!(budget.quarantined() && budget.used() > 0);
}

#[test]
fn datagram_larger_than_whole_budget_is_permanent_rejection() {
    let budget = BackingBudget::new(32).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    assert!(matches!(
        BytesMut::try_copy_from_slice_metered(&[1u8; 64], &meter),
        Err(MeteredCopyError::TooLarge)
    ));
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn split_clone_freeze_release_one_original_backing_after_last_slice() {
    let meter = BackingMeter::new();
    let mut packet = BytesMut::with_capacity(256);
    packet.extend_from_slice(b"abcdefgh");
    let first = packet.split_to(3);
    assert!(first.charge_shared_backing(&meter));
    assert!(packet.charge_shared_backing(&meter));
    let snapshot = meter.snapshot();
    assert_eq!((snapshot.allocations, snapshot.payload_bytes), (1, 256));
    assert!(snapshot.physical_bytes > snapshot.payload_bytes);
    let first = first.freeze();
    let other_flow = first.clone();
    let tail = packet.freeze();
    let identity = first.allocation_identity().unwrap();
    assert_eq!(other_flow.allocation_identity(), Some(identity));
    assert_eq!(tail.allocation_identity(), Some(identity));
    assert_eq!(
        meter.snapshot().allocations,
        1,
        "two Flow views share one physical charge"
    );
    drop(first);
    drop(tail);
    let held_by_other_flow = meter.snapshot();
    assert_eq!(
        (
            held_by_other_flow.allocations,
            held_by_other_flow.payload_bytes,
            held_by_other_flow.physical_bytes
        ),
        (1, 256, snapshot.physical_bytes)
    );
    drop(other_flow);
    let settled = meter.snapshot();
    assert_eq!(
        (
            settled.allocations,
            settled.payload_bytes,
            settled.physical_bytes
        ),
        (0, 0, 0)
    );
    assert!(!settled.quarantined);
}

#[test]
fn zero_copy_vec_escape_remains_charged_quarantine() {
    let meter = BackingMeter::new();
    let mut packet = BytesMut::with_capacity(256);
    packet.extend_from_slice(b"abcdefgh");
    let first = packet.split_to(8);
    assert!(first.charge_shared_backing(&meter));
    drop(packet);
    let escaped: Vec<u8> = first.freeze().into();
    assert_eq!(escaped, b"abcdefgh");
    let snapshot = meter.snapshot();
    assert!(snapshot.quarantined);
    assert_eq!(snapshot.payload_bytes, 256);
}

#[test]
fn tracked_unique_reserve_cannot_normalize_old_charge() {
    let meter = BackingMeter::new();
    let mut packet = BytesMut::with_capacity(256);
    packet.extend_from_slice(b"abcdefgh");
    let mut first = packet.split_to(8);
    assert!(first.charge_shared_backing(&meter));
    drop(packet);
    first.reserve(1024);
    drop(first);
    let snapshot = meter.snapshot();
    assert!(snapshot.quarantined);
    assert_eq!(snapshot.payload_bytes, 256);
}

#[test]
fn foreign_meter_rejection_quarantines_both_authorities() {
    let first_meter = BackingMeter::new();
    let foreign = BackingMeter::new();
    let mut packet = BytesMut::from(&b"abcd"[..]);
    let first = packet.split_to(4);
    assert!(first.charge_shared_backing(&first_meter));
    assert!(!packet.charge_shared_backing(&foreign));
    assert!(first_meter.snapshot().quarantined);
    assert!(foreign.snapshot().quarantined);
}

#[test]
fn shared_bytes_to_mut_copy_cannot_leave_a_clean_untracked_backing() {
    let meter = BackingMeter::new();
    let mut packet = BytesMut::with_capacity(256);
    packet.extend_from_slice(b"abcdefgh");
    let first = packet.split_to(8);
    assert!(first.charge_shared_backing(&meter));
    drop(packet);
    let bytes = first.freeze();
    let sibling = bytes.clone();
    let copied: BytesMut = bytes.into();
    drop(sibling);
    assert_eq!(&copied[..], b"abcdefgh");
    assert!(meter.snapshot().quarantined);
}

#[test]
fn bytes_mut_unique_vec_escape_cannot_leave_a_clean_untracked_backing() {
    let meter = BackingMeter::new();
    let mut packet = BytesMut::with_capacity(256);
    packet.extend_from_slice(b"abcdefgh");
    let first = packet.split_to(8);
    assert!(first.charge_shared_backing(&meter));
    drop(packet);
    let escaped: Vec<u8> = first.into();
    assert_eq!(escaped, b"abcdefgh");
    assert!(meter.snapshot().quarantined);
    assert_eq!(meter.snapshot().payload_bytes, 256);
}

#[test]
fn another_generation_quarantine_is_visible_to_precharged_owners() {
    let budget = BackingBudget::new(4096).unwrap();
    let first = BackingMeter::with_budget(&budget);
    let second = BackingMeter::with_budget(&budget);
    let layout = second.reserve_cold_layout(256).unwrap().unwrap();
    let charged = budget.used();
    second.admit_precharged().unwrap();
    assert_eq!(
        budget.used(),
        charged,
        "logical fence does not charge/refund bytes"
    );
    assert!(!first.is_quarantined());
    assert!(!second.is_quarantined());
    first.quarantine();
    assert!(first.is_quarantined());
    assert!(second.is_quarantined());
    assert!(matches!(
        second.admit_precharged(),
        Err(MeteredCopyError::Quarantined)
    ));
    assert!(budget.quarantined());
    assert!(
        second.snapshot().quarantined,
        "a cold owner must observe the shared authority's quarantine"
    );
    assert!(matches!(
        second.reserve_cold_layout(1),
        Err(MeteredCopyError::Quarantined)
    ));
    drop(layout);
    assert_eq!(
        budget.used(),
        1,
        "unknown shared debt cannot become clean zero"
    );
}

#[path = "bytes_backing_meter/allocation.rs"]
mod allocation;
#[path = "bytes_backing_meter/protocol_tier.rs"]
mod protocol_tier;

#[test]
fn writable_backing_has_exact_precharged_layout_and_last_reference_release() {
    const LEN: usize = 127;
    let physical = BytesMut::metered_copy_layout_for_len(LEN).unwrap();
    let budget = BackingBudget::new(physical).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (result, events) = allocation::observe(None, || BytesMut::try_zeroed_metered(LEN, &meter));
    let mut backing = result.unwrap();
    assert_eq!(
        (events.calls, events.allocated, events.freed),
        (2, physical, 0)
    );
    assert_eq!((backing.len(), backing.capacity()), (LEN, LEN));
    assert!(backing.iter().all(|x| *x == 0));
    backing[..7].copy_from_slice(b"payload");
    let charged = meter.snapshot();
    assert_eq!(
        (budget.used(), charged.allocations, charged.physical_bytes),
        (physical, 1, physical)
    );
    let (full, events) = allocation::observe(None, || BytesMut::try_zeroed_metered(1, &meter));
    assert!(matches!(full, Err(MeteredCopyError::BudgetFull)));
    assert_eq!(events.calls, 0);
    assert_eq!(meter.snapshot(), charged);
    let ((head, tail), events) = allocation::observe(None, || {
        let head = backing.split_to(7).freeze();
        (head, backing.freeze())
    });
    assert_eq!(events.calls, 0);
    assert_eq!(head.allocation_identity(), tail.allocation_identity());
    assert_eq!(&head[..], b"payload");
    let (_, first_drop) = allocation::observe(None, || drop(head));
    assert_eq!(first_drop.freed, 0);
    assert_eq!(budget.used(), physical);
    let (_, final_drop) = allocation::observe(None, || drop(tail));
    assert_eq!(final_drop.freed, physical);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
    let (copy, events) = allocation::observe(None, || {
        BytesMut::try_copy_from_slice_metered(&[3; LEN], &meter)
    });
    assert_eq!((events.calls, events.allocated), (2, physical));
    assert_eq!(&copy.unwrap()[..], &[3; LEN]);
    assert_eq!(budget.used(), 0);
}

#[test]
fn writable_backing_rejection_and_allocator_failure_preserve_existing_inventory() {
    let physical = BytesMut::metered_copy_layout_for_len(64).unwrap();
    let budget = BackingBudget::new(physical * 2).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let held = BytesMut::try_copy_from_slice_metered(&[9; 64], &meter).unwrap();
    let before = meter.snapshot();
    for fail_at in [1, 2] {
        let (result, events) =
            allocation::observe(Some(fail_at), || BytesMut::try_zeroed_metered(64, &meter));
        assert!(matches!(result, Err(MeteredCopyError::Allocation)));
        assert_eq!(events.calls, fail_at);
        assert_eq!(events.allocated, events.freed);
        assert_eq!(budget.used(), physical);
        assert_eq!(meter.snapshot(), before);
        assert_eq!(&held[..], &[9; 64]);
    }
    for len in [0, usize::MAX] {
        let (result, events) =
            allocation::observe(None, || BytesMut::try_zeroed_metered(len, &meter));
        assert!(matches!(result, Err(MeteredCopyError::Allocation)));
        assert_eq!(events.calls, 0);
        assert_eq!(meter.snapshot(), before);
    }
    let (large, events) = allocation::observe(None, || {
        BytesMut::try_zeroed_metered(budget.limit(), &meter)
    });
    assert!(matches!(large, Err(MeteredCopyError::TooLarge)));
    assert_eq!(events.calls, 0);
    assert_eq!(meter.snapshot(), before);
    meter.quarantine();
    let charged = budget.used();
    let (result, events) = allocation::observe(None, || BytesMut::try_zeroed_metered(64, &meter));
    assert!(matches!(result, Err(MeteredCopyError::Quarantined)));
    assert_eq!(events.calls, 0);
    assert_eq!(budget.used(), charged);
    drop(held);
    assert!(budget.quarantined() && budget.used() > 0);
}

#[test]
fn writable_backing_drop_and_unwind_release_only_known_layout() {
    for fatal in [false, true] {
        let budget = BackingBudget::new(1024).unwrap();
        let meter = BackingMeter::with_budget(&budget);
        let stopped = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut backing = BytesMut::try_zeroed_metered(64, &meter).unwrap();
            backing[..4].copy_from_slice(b"held");
            if fatal {
                meter.quarantine();
                panic!("injected writer interruption");
            }
            drop(backing);
        }));
        assert_eq!(stopped.is_err(), fatal);
        assert_eq!(meter.snapshot().allocations, 0);
        assert_eq!(meter.snapshot().physical_bytes, 0);
        if fatal {
            assert!(budget.quarantined() && budget.used() > 0);
        } else {
            assert!(!budget.quarantined());
            assert_eq!(budget.used(), 0);
        }
    }
}

#[test]
fn writable_backing_escape_and_foreign_authority_keep_quarantine() {
    for escape in [false, true] {
        let budget = BackingBudget::new(1024).unwrap();
        let meter = BackingMeter::with_budget(&budget);
        let backing = BytesMut::try_zeroed_metered(64, &meter).unwrap();
        if escape {
            let escaped: Vec<u8> = backing.freeze().into();
            assert_eq!(escaped, [0; 64]);
            assert!(budget.quarantined() && budget.used() > 0);
            drop(escaped);
        } else {
            let foreign_budget = BackingBudget::new(1024).unwrap();
            let foreign = BackingMeter::with_budget(&foreign_budget);
            assert!(!backing.charge_shared_backing(&foreign));
            assert!(foreign_budget.quarantined() && foreign_budget.used() > 0);
            drop(backing);
        }
        assert!(budget.quarantined() && budget.used() > 0);
        assert!(matches!(
            BytesMut::try_zeroed_metered(1, &meter),
            Err(MeteredCopyError::Quarantined)
        ));
    }
}
