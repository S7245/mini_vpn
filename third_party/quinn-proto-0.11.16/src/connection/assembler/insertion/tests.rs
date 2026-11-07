use super::*;

#[test]
fn cancelled_preparation_preserves_original_stock_and_refunds_actual_growth() {
    let budget = bytes::BackingBudget::new(65536).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let mut assembler = Assembler::new();
    let before = budget.used();
    let input = BytesMut::try_copy_from_slice_metered(b"abc", &meter)
        .unwrap()
        .freeze();
    let prepared = assembler.prepare_insert(0, input, 3, Some(&meter)).unwrap();
    assert!(budget.used() > before);
    drop(prepared);
    assert_eq!(budget.used(), before);
    assert_eq!(assembler.buffered, 0);
    assert_eq!(assembler.end, 0);
    assert!(assembler.backing_meter.is_none());
    assert!(!budget.quarantined());
}

#[test]
fn prepared_growth_commits_with_budget_full_without_another_allocation() {
    let budget = bytes::BackingBudget::new(65536).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let mut assembler = Assembler::new();
    let input = BytesMut::try_copy_from_slice_metered(b"abc", &meter)
        .unwrap()
        .freeze();
    let prepared = assembler.prepare_insert(0, input, 3, Some(&meter)).unwrap();
    let pressure = budget.try_reserve(budget.limit() - budget.used()).unwrap();
    prepared.commit().unwrap();
    assert_eq!(assembler.buffered, 3);
    assert_eq!(assembler.end, 3);
    assert_eq!(budget.used(), budget.limit());
    drop(pressure);
    let chunk = assembler.read_checked(3, true).unwrap().unwrap();
    assert_eq!(chunk.bytes.as_ref(), b"abc");
    drop((chunk, assembler));
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn refused_growth_leaves_owner_binding_and_original_stock_unpublished() {
    let budget = bytes::BackingBudget::new(65536).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let mut assembler = Assembler::new();
    let input = BytesMut::try_copy_from_slice_metered(b"abc", &meter)
        .unwrap()
        .freeze();
    let input_charge = budget.used();
    let pressure = budget.try_reserve(budget.limit() - input_charge).unwrap();
    assert!(matches!(
        assembler.prepare_insert(0, input, 3, Some(&meter)),
        Err(InsertError::Capacity(bytes::MeteredCopyError::BudgetFull))
    ));
    assert!(assembler.backing_meter.is_none());
    assert_eq!(assembler.buffered, 0);
    assert_eq!(assembler.end, 0);
    assert_eq!(budget.used(), budget.limit() - input_charge);
    drop(pressure);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn packet_defragmentation_is_funded_before_any_receive_publication() {
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let mut assembler = Assembler::new();
    let mut backing = BytesMut::try_zeroed_metered(40000, &meter).unwrap();
    backing[..3].copy_from_slice(b"abc");
    let input = backing.split_to(3).freeze();
    drop(backing);
    let prepared = assembler
        .prepare_insert(0, input, 40000, Some(&meter))
        .unwrap()
        .prepare_defragmentation()
        .unwrap();
    let pressure = budget.try_reserve(budget.limit() - budget.used()).unwrap();
    prepared.commit().unwrap();
    assert_eq!(assembler.buffered, 3);
    assert_eq!(assembler.allocated, 3);
    drop(pressure);
    let chunk = assembler.read_checked(3, true).unwrap().unwrap();
    assert_eq!(chunk.bytes.as_ref(), b"abc");
    drop((chunk, assembler));
    assert_eq!(budget.used(), 0);
}

#[test]
fn defrag_full_cancels_prepared_input_and_heap_before_publication() {
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let mut assembler = Assembler::new();
    let mut backing = BytesMut::try_zeroed_metered(40000, &meter).unwrap();
    backing[..3].copy_from_slice(b"abc");
    let input = backing.split_to(3).freeze();
    drop(backing);
    let prepared = assembler
        .prepare_insert(0, input, 40000, Some(&meter))
        .unwrap();
    let held = budget.limit() - budget.used();
    let pressure = budget.try_reserve(held).unwrap();
    assert!(matches!(
        prepared.prepare_defragmentation(),
        Err(InsertError::Capacity(MeteredCopyError::BudgetFull))
    ));
    assert_eq!(budget.used(), held);
    assert!(assembler.backing_meter.is_none());
    assert_eq!(assembler.buffered, 0);
    assert_eq!(assembler.end, 0);
    drop(pressure);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn prepared_defrag_matches_original_ordered_and_unordered_overlap_results() {
    for ordered in [true, false] {
        let budget = bytes::BackingBudget::new(4 << 20).unwrap();
        let meter = BackingMeter::with_budget(&budget);
        let mut original = Assembler::new();
        let mut prepared = Assembler::new();
        for owner in [&mut original, &mut prepared] {
            owner.ensure_ordering(ordered).unwrap();
        }
        for (offset, data) in [(0, b"abcd"), (8, b"ijkl"), (2, b"cdef"), (5, b"fghi")] {
            let mut backing = BytesMut::try_zeroed_metered(40000, &meter).unwrap();
            backing[..4].copy_from_slice(data);
            let input = backing.split_to(4).freeze();
            drop(backing);
            original
                .insert_checked(offset, input.clone(), 40000, Some(&meter))
                .unwrap();
            prepared
                .prepare_insert(offset, input, 40000, Some(&meter))
                .unwrap()
                .prepare_defragmentation()
                .unwrap()
                .commit()
                .unwrap();
            assert_eq!(original.progress(), prepared.progress());
        }
        loop {
            let expected = original.read_checked(3, ordered).unwrap();
            let actual = prepared.read_checked(3, ordered).unwrap();
            match (expected, actual) {
                (Some(expected), Some(actual)) => {
                    assert_eq!(actual.offset, expected.offset);
                    assert_eq!(actual.bytes, expected.bytes);
                }
                (None, None) => break,
                _ => panic!("prepared defrag changed original read semantics"),
            }
        }
        drop((original, prepared));
        assert_eq!(budget.used(), 0);
        assert!(!budget.quarantined());
    }
}
