use super::*;

#[test]
fn packet_batch_matches_original_each_frame_at_full_in_both_read_modes() {
    for ordered in [true, false] {
        for allocation in [16, 40000] {
            let budget = bytes::BackingBudget::new(4 << 20).unwrap();
            let meter = BackingMeter::with_budget(&budget);
            let mut original = Assembler::new();
            let mut target = Assembler::new();
            original.ensure_ordering(ordered).unwrap();
            target.ensure_ordering(ordered).unwrap();
            let inputs: Vec<_> = [
                (8, b"ijkl"),
                (2, b"cdef"),
                (0, b"abcd"),
                (5, b"fghi"),
                (2, b"cdef"),
            ]
            .into_iter()
            .map(|(offset, data)| {
                let mut backing = BytesMut::try_zeroed_metered(allocation, &meter).unwrap();
                backing[..4].copy_from_slice(data);
                (offset, backing.split_to(4).freeze(), allocation)
            })
            .collect();
            let mut batch =
                BatchInsertion::prepare(&target, inputs.iter().cloned(), &meter).unwrap();
            for (offset, bytes, allocation) in &inputs {
                original
                    .insert_checked(*offset, bytes.clone(), *allocation, Some(&meter))
                    .unwrap();
                let pressure = budget.try_reserve(budget.limit() - budget.used()).unwrap();
                batch.commit_next(&mut target).unwrap();
                drop(pressure);
                assert_eq!(target.progress(), original.progress());
                assert_eq!(target.allocated, original.allocated);
                assert!(target.closed_inventory_matches(&meter));
            }
            loop {
                match (
                    original.read_checked(3, ordered).unwrap(),
                    target.read_checked(3, ordered).unwrap(),
                ) {
                    (Some(a), Some(b)) => {
                        assert_eq!(a.offset, b.offset);
                        assert_eq!(a.bytes, b.bytes);
                    }
                    (None, None) => break,
                    _ => panic!("batch changed read semantics"),
                }
            }
            drop((original, target, batch, inputs));
            assert_eq!(budget.used(), 0);
            assert!(!budget.quarantined());
        }
    }
}

#[test]
fn later_batch_defrag_full_preserves_original_and_refunds_preparation() {
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let mut target = Assembler::new();
    let seed = BytesMut::try_copy_from_slice_metered(b"x", &meter)
        .unwrap()
        .freeze();
    target.insert_checked(0, seed, 1, Some(&meter)).unwrap();
    let bytes = BytesMut::try_copy_from_slice_metered(b"abcd", &meter)
        .unwrap()
        .freeze();
    let mut backing = BytesMut::try_zeroed_metered(40000, &meter).unwrap();
    let later = backing.split_to(4).freeze();
    drop(backing);
    let before = budget.used();
    let shape = target.packet_shape();
    let pressure = budget.try_reserve(budget.limit() - before).unwrap();
    // First frame fits in existing physical stock even at exact Full.
    drop(
        target
            .prepare_insert(1, bytes.clone(), 4, Some(&meter))
            .unwrap()
            .prepare_defragmentation()
            .unwrap(),
    );
    assert!(matches!(
        BatchInsertion::prepare(
            &target,
            [(1, bytes.clone(), 4), (5, later.clone(), 40000)].into_iter(),
            &meter
        ),
        Err(InsertError::Capacity(MeteredCopyError::BudgetFull))
    ));
    assert_eq!(target.packet_shape(), shape);
    assert!(target.closed_inventory_matches(&meter));
    drop(pressure);
    assert_eq!(budget.used(), before);
    drop((bytes, later, target));
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn batch_spill_and_existing_sparse_history_match_every_original_transition() {
    for ordered in [true, false] {
        for seed in 0..16u64 {
            let budget = bytes::BackingBudget::new(8 << 20).unwrap();
            let meter = BackingMeter::with_budget(&budget);
            let mut original = Assembler::new();
            let mut target = Assembler::new();
            for owner in [&mut original, &mut target] {
                owner.ensure_ordering(ordered).unwrap();
                for offset in [0, 12, 24, 40] {
                    let data: Vec<_> = (offset..offset + 4).map(|n| n as u8).collect();
                    let bytes = BytesMut::try_copy_from_slice_metered(&data, &meter)
                        .unwrap()
                        .freeze();
                    owner
                        .insert_checked(offset, bytes, 4, Some(&meter))
                        .unwrap();
                }
                drop(owner.read_checked(4, ordered).unwrap());
            }
            let inputs: Vec<_> = (0..24)
                .map(|i| {
                    let offset = (seed * 13 + i * 7) % 51;
                    let len = 1 + ((seed + i * 3) % 9);
                    let allocation = if (i + seed) % 5 == 0 { 40000 } else { 1472 };
                    let mut bytes = BytesMut::try_zeroed_metered(allocation, &meter).unwrap();
                    for n in 0..len {
                        bytes[n as usize] = (offset + n) as u8;
                    }
                    (offset, bytes.split_to(len as usize).freeze(), allocation)
                })
                .collect();
            let before = target.packet_shape();
            let charged = budget.used();
            let cancelled =
                BatchInsertion::prepare(&target, inputs.iter().cloned(), &meter).unwrap();
            assert!(budget.used() > charged);
            drop(cancelled);
            assert_eq!(budget.used(), charged);
            assert_eq!(target.packet_shape(), before);
            let mut batch =
                BatchInsertion::prepare(&target, inputs.iter().cloned(), &meter).unwrap();
            for (index, (offset, bytes, allocation)) in inputs.iter().enumerate() {
                original
                    .insert_checked(*offset, bytes.clone(), *allocation, Some(&meter))
                    .unwrap();
                let pressure = budget.try_reserve(budget.limit() - budget.used()).unwrap();
                batch.commit_next(&mut target).unwrap();
                drop(pressure);
                assert_eq!(
                    target.progress(),
                    original.progress(),
                    "mode={ordered} seed={seed} frame={index}"
                );
                assert_eq!(target.allocated, original.allocated);
                assert!(target.closed_inventory_matches(&meter));
            }
            loop {
                match (
                    original.read_checked(3, ordered).unwrap(),
                    target.read_checked(3, ordered).unwrap(),
                ) {
                    (Some(a), Some(b)) => {
                        assert_eq!(a.offset, b.offset);
                        assert_eq!(a.bytes, b.bytes);
                    }
                    (None, None) => break,
                    _ => panic!("batch changed sparse history read semantics"),
                }
            }
            drop((original, target, inputs, batch));
            assert_eq!(budget.used(), 0);
            assert!(!budget.quarantined());
        }
    }
}
