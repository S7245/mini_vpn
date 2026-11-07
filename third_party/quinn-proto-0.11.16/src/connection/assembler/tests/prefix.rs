//! Received prefix and actual sparse inventory under the original hard budget.
use super::*;
use bytes::BackingBudget;

#[test]
fn contiguous_prefix_does_not_consume_a_sparse_slot_at_actual_full() {
    let budget = BackingBudget::new(65536).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let input = BytesMut::try_copy_from_slice_metered(b"abcdefghijklmnop", &meter).unwrap().freeze();
    let mut assembler = Assembler::new();
    assembler.ensure_ordering(false).unwrap();
    for range in [0..4usize, 6..7, 8..9, 10..11] {
        assembler.insert_checked(range.start as u64, input.slice(range.clone()), input.len(), Some(&meter)).unwrap();
        let actual = assembler.read(64, false).unwrap();
        assert_eq!(actual.offset, range.start as u64);
        assert_eq!(actual.bytes, input.slice(range));
    }
    let hold = meter.reserve_cold_layout(budget.limit() - budget.used()).unwrap().unwrap();
    let before = budget.used();
    assert!(assembler.closed_inventory_matches(&meter));
    assembler.insert_checked(12, input.slice(12..13), input.len(), Some(&meter))
        .expect("the received prefix must leave all four funded slots for sparse intervals");
    assert_eq!(assembler.read(64, false).unwrap().bytes, b"m"[..]);
    assert_eq!(budget.used(), before);
    let progress = assembler.progress();
    assert!(matches!(assembler.insert_checked(14, input.slice(14..15), input.len(), Some(&meter)),
        Err(InsertError::Capacity(MeteredCopyError::BudgetFull))));
    assert_eq!(assembler.progress(), progress, "a fifth sparse interval still refuses atomically");
    assert!(assembler.closed_inventory_matches(&meter));
    // All already delivered bytes remain duplicates, including the scalar prefix.
    for range in [0..4usize, 6..7, 8..9, 10..11, 12..13] {
        assembler.insert_checked(range.start as u64, input.slice(range), input.len(), Some(&meter)).unwrap();
        assert!(assembler.read(64, false).is_none());
    }
    // Bridging the prefix consumes only the four original holes and retires
    // all sparse records; no new slab/payload layout can be funded here.
    assembler.insert_checked(4, input.slice(4..13), input.len(), Some(&meter)).unwrap();
    for range in [4..6usize, 7..8, 9..10, 11..12] {
        let actual = assembler.read(64, false).unwrap();
        assert_eq!(actual.offset, range.start as u64);
        assert_eq!(actual.bytes, input.slice(range));
    }
    assert!(assembler.read(64, false).is_none());
    assembler.insert_checked(0, input.slice(0..13), input.len(), Some(&meter)).unwrap();
    assert!(assembler.read(64, false).is_none());
    assert_eq!(budget.used(), before);
    assert!(assembler.closed_inventory_matches(&meter));
    assert!(!budget.quarantined());
    drop((assembler, input, hold));
    assert_eq!(budget.used(), 0);
}

#[test]
fn ordered_read_prefix_is_seeded_before_unordered_retransmit_and_bridge() {
    let budget = BackingBudget::new(65536).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let input = BytesMut::try_copy_from_slice_metered(b"abcdefghijklmnop", &meter).unwrap().freeze();
    let mut assembler = Assembler::new();
    assembler.insert_checked(0, input.slice(0..8), input.len(), Some(&meter)).unwrap();
    assert_eq!(assembler.read(3, true).unwrap().bytes, b"abc"[..]);
    assembler.insert_checked(10, input.slice(10..12), input.len(), Some(&meter)).unwrap();
    assembler.ensure_ordering(false).unwrap();
    assert_eq!(assembler.bytes_read(), 3);
    assert!(assembler.closed_inventory_matches(&meter));
    for range in [3..8usize, 10..12] {
        let actual = assembler.read(64, false).unwrap();
        assert_eq!(actual.offset, range.start as u64);
        assert_eq!(actual.bytes, input.slice(range));
    }
    let hold = meter.reserve_cold_layout(budget.limit() - budget.used()).unwrap().unwrap();
    assembler.insert_checked(0, input.slice(0..5), input.len(), Some(&meter)).unwrap();
    assert!(assembler.read(64, false).is_none());
    assembler.insert_checked(4, input.slice(4..14), input.len(), Some(&meter)).unwrap();
    for range in [8..10usize, 12..14] {
        let actual = assembler.read(64, false).unwrap();
        assert_eq!(actual.offset, range.start as u64);
        assert_eq!(actual.bytes, input.slice(range));
    }
    assert_eq!(assembler.bytes_read(), 14);
    assembler.insert_checked(0, input.slice(0..14), input.len(), Some(&meter)).unwrap();
    assert!(assembler.read(64, false).is_none());
    assert!(assembler.closed_inventory_matches(&meter));
    assert!(!budget.quarantined());
    drop((assembler, input, hold));
    assert_eq!(budget.used(), 0);
}
