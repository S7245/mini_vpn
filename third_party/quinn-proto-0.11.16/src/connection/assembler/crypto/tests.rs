use super::*;

#[test]
fn crypto_schedule_matches_original_insertion_and_drain_at_full() {
    for seed in 0..24u64 {
        let budget = bytes::BackingBudget::new(8 << 20).unwrap();
        let meter = BackingMeter::with_budget(&budget);
        let mut original = Assembler::new();
        let mut target = Assembler::new();
        for owner in [&mut original, &mut target] {
            for offset in [12, 24, 40] {
                let bytes = BytesMut::try_copy_from_slice_metered(&[offset as u8; 4], &meter)
                    .unwrap()
                    .freeze();
                owner
                    .insert_checked(offset, bytes, 4, Some(&meter))
                    .unwrap();
            }
        }
        let inputs: Vec<_> = (0..32)
            .map(|i| {
                let offset = if i == 31 { 0 } else { (seed * 13 + i * 7) % 51 };
                let len = if i == 31 {
                    60
                } else {
                    1 + ((seed + i * 3) % 9)
                };
                let allocation = if (i + seed) % 5 == 0 { 40000 } else { 1472 };
                let mut bytes = BytesMut::try_zeroed_metered(allocation, &meter).unwrap();
                for n in 0..len {
                    bytes[n as usize] = (offset + n) as u8;
                }
                (offset, bytes.split_to(len as usize).freeze(), allocation)
            })
            .collect();
        let shape = target.packet_shape();
        let used = budget.used();
        let cancelled =
            CryptoInsertion::prepare(&target, inputs.iter().cloned(), &meter, 65536).unwrap();
        drop(cancelled);
        assert_eq!(budget.used(), used);
        assert_eq!(target.packet_shape(), shape);
        let mut plan =
            CryptoInsertion::prepare(&target, inputs.iter().cloned(), &meter, 65536).unwrap();
        let mut reads = 0;
        for (index, (offset, bytes, allocation)) in inputs.iter().enumerate() {
            original
                .insert_checked(*offset, bytes.clone(), *allocation, Some(&meter))
                .unwrap();
            let pressure = budget.try_reserve(budget.limit() - budget.used()).unwrap();
            plan.insert_next(&mut target).unwrap();
            loop {
                match (
                    original.read_checked(usize::MAX, true).unwrap(),
                    target.read_checked(usize::MAX, true).unwrap(),
                ) {
                    (Some(a), Some(b)) => {
                        reads += 1;
                        assert_eq!((a.offset, a.bytes), (b.offset, b.bytes));
                    }
                    (None, None) => break,
                    _ => panic!("drain differs: seed={seed} frame={index}"),
                }
            }
            plan.check_drained(&target).unwrap();
            assert_eq!(target.packet_shape(), original.packet_shape());
            assert!(target.closed_inventory_matches(&meter));
            drop(pressure);
        }
        assert_eq!(plan.reads, reads);
        drop((plan, target, original, inputs));
        assert_eq!(budget.used(), 0);
        assert!(!budget.quarantined());
    }
}

#[test]
fn crypto_late_defrag_refusal_preserves_original_heap_and_bytes() {
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let mut target = Assembler::new();
    let bytes = BytesMut::try_copy_from_slice_metered(b"abcd", &meter)
        .unwrap()
        .freeze();
    target
        .insert_checked(9, bytes.clone(), 4, Some(&meter))
        .unwrap();
    let mut big = BytesMut::try_zeroed_metered(40000, &meter).unwrap();
    let later = big.split_to(4).freeze();
    drop(big);
    let used = budget.used();
    let shape = target.packet_shape();
    let held = budget.try_reserve(budget.limit() - used).unwrap();
    assert!(matches!(
        CryptoInsertion::prepare(
            &target,
            [(0, bytes.clone(), 4), (20, later.clone(), 40000)].into_iter(),
            &meter,
            65536
        ),
        Err(PrepareError::Insert(InsertError::Capacity(
            MeteredCopyError::BudgetFull
        )))
    ));
    assert_eq!(target.packet_shape(), shape);
    assert!(target.closed_inventory_matches(&meter));
    drop(held);
    assert_eq!(budget.used(), used);
    drop((target, bytes, later));
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn crypto_buffer_limit_validation_uses_per_frame_drained_prefix_at_full() {
    let budget = bytes::BackingBudget::new(1024).unwrap();
    let held = budget.try_reserve(1024).unwrap();
    let target = Assembler::new();
    assert!(CryptoInsertion::check(&target, || [(0, 4), (4, 4)].into_iter(), 4).is_ok());
    assert!(matches!(
        CryptoInsertion::check(&target, || [(4, 4), (0, 4)].into_iter(), 4),
        Err(PrepareError::BufferLimit)
    ));
    assert!(CryptoInsertion::check(&target, || [(4, 4), (0, 4), (8, 8)].into_iter(), 8).is_ok());
    assert_eq!(target.packet_shape(), (0, 0, 0, 0, 0));
    drop(held);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}
