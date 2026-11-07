//! Actual allocator/layout failure, ownership and publication evidence for the
//! same Assembler transaction. Service lifecycle remains a separate joint gate.
use super::*;
use bytes::{BackingBudget, BackingBudgetReservation};

fn owner() -> (BackingBudget, BackingMeter, Assembler) {
    let budget = BackingBudget::new(1024 * 1024).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    (budget, meter, Assembler::new())
}
fn input(meter: &BackingMeter, payload: &[u8]) -> Bytes {
    BytesMut::try_copy_from_slice_metered(payload, meter)
        .unwrap()
        .freeze()
}
fn fragmented(meter: &BackingMeter, payload: &[u8]) -> Bytes {
    let mut full = BytesMut::try_zeroed_metered(256, meter).unwrap();
    full[..payload.len()].copy_from_slice(payload);
    full.split_to(payload.len()).freeze()
}
fn hold_remaining(
    budget: &BackingBudget,
    meter: &BackingMeter,
    remaining: usize,
) -> BackingBudgetReservation {
    meter
        .reserve_cold_layout(budget.limit() - budget.used() - remaining)
        .unwrap()
        .unwrap()
}
fn populate(assembler: &mut Assembler, meter: &BackingMeter) {
    for (offset, data) in [(0, b"abc"), (6, b"ghi"), (12, b"mno")] {
        assembler
            .insert_checked(offset, fragmented(meter, data), 256, Some(meter))
            .unwrap();
    }
}

#[test]
fn every_unordered_preparation_allocator_refusal_preserves_original_inventory() {
    // Scratch, replacement heap, payload, Shared, and interval slab are actual
    // original allocator calls, not injected success/failure of a fake port.
    for fail_at in 1..=5 {
        let (budget, meter, mut assembler) = owner();
        populate(&mut assembler, &meter);
        let before = assembler.progress();
        let used = budget.used();
        let snapshot = meter.snapshot();
        let mut result = None;
        crate::tests::allocator_failure_in(fail_at, || {
            result = Some(assembler.ensure_ordering(false))
        });
        assert!(
            matches!(result, Some(Err(ReadableError::BackingAuthorityLost))),
            "allocation {fail_at}"
        );
        assert!(assembler.state.is_ordered());
        assert_eq!(assembler.progress(), before);
        assert_eq!(budget.used(), used);
        assert_eq!(meter.snapshot().allocations, snapshot.allocations);
        assert!(!budget.quarantined());
        assembler.ensure_ordering(false).unwrap();
        assert_eq!(assembler.read(10, false).unwrap().bytes, b"abc"[..]);
        assert_eq!(assembler.read(10, false).unwrap().bytes, b"ghi"[..]);
        assert_eq!(assembler.read(10, false).unwrap().bytes, b"mno"[..]);
        drop(assembler);
        assert_eq!(budget.used(), 0);
    }
}

#[test]
fn hard_full_at_each_prepare_layout_keeps_old_backings_and_mode() {
    // Scratch is released before interval preparation. For three chunks its
    // returned layout already covers the interval slab; one chunk exposes the
    // independently larger interval peak. Offset 1 preserves a real sparse gap;
    // contiguous prefix receipt now needs no interval slab. Do not sum lifetimes.
    for count in [1usize, 3] {
        let scratch = std::alloc::Layout::array::<Buffer>(count).unwrap().size();
        let payload = BytesMut::metered_copy_layout_for_len(count * 3).unwrap();
        let interval = std::alloc::Layout::array::<crate::connection::storage::intervals::Node>(4)
            .unwrap()
            .size();
        let mut boundaries = vec![0, scratch, 2 * scratch];
        if interval > scratch {
            boundaries.push(2 * scratch + payload);
        }
        for remaining in boundaries {
            let (budget, meter, mut assembler) = owner();
            for i in 0..count {
                assembler
                    .insert_checked(1 + i as u64 * 6, fragmented(&meter, b"abc"), 256, Some(&meter))
                    .unwrap();
            }
            let held = hold_remaining(&budget, &meter, remaining);
            let used = budget.used();
            let before = assembler.progress();
            assert!(
                matches!(
                    assembler.ensure_ordering(false),
                    Err(ReadableError::BackingAuthorityLost)
                ),
                "count={count} remaining={remaining}"
            );
            assert!(assembler.state.is_ordered());
            assert_eq!(assembler.progress(), before);
            assert_eq!(budget.used(), used);
            assert_eq!(meter.snapshot().allocations, count);
            assert!(!budget.quarantined());
            drop(held);
            drop(assembler);
            assert_eq!(budget.used(), 0);
        }
    }
}

#[test]
fn ordered_and_unordered_insert_growth_refuse_before_useful_state_then_recover() {
    for unordered in [false, true] {
        let (budget, meter, mut assembler) = owner();
        if unordered {
            assembler.ensure_ordering(false).unwrap();
        }
        for i in 0..4 {
            assembler
                .insert_checked(i * 3, input(&meter, b"x"), 1, Some(&meter))
                .unwrap();
        }
        let incoming = input(&meter, b"y");
        let held = hold_remaining(&budget, &meter, 0);
        let used = budget.used();
        let before = assembler.progress();
        assert!(matches!(
            assembler.insert_checked(12, incoming.clone(), 1, Some(&meter)),
            Err(InsertError::Capacity(MeteredCopyError::BudgetFull))
        ));
        assert_eq!(assembler.progress(), before);
        assert_eq!(budget.used(), used);
        assert!(!budget.quarantined());
        drop(held);
        assembler
            .insert_checked(12, incoming, 1, Some(&meter))
            .unwrap();
        if !unordered {
            assembler.ensure_ordering(false).unwrap();
        }
        let chunks: Vec<_> = std::iter::from_fn(|| assembler.read(10, false)).collect();
        assert_eq!(
            chunks.iter().map(|c| c.offset).collect::<Vec<_>>(),
            [0, 3, 6, 9, 12]
        );
        assert_eq!(chunks[4].bytes, b"y"[..]);
        drop(chunks);
        drop(assembler);
        assert_eq!(budget.used(), 0);
    }
}

#[test]
fn rejected_post_insert_defrag_keeps_the_newly_admitted_input_for_failstop() {
    let (budget, meter, mut assembler) = owner();
    assembler
        .insert_checked(0, input(&meter, b"a"), 1, Some(&meter))
        .unwrap();
    let incoming = input(&meter, b"b");
    let held = hold_remaining(&budget, &meter, 0);
    assert!(matches!(
        assembler.insert_checked(1, incoming, 40000, Some(&meter)),
        Err(InsertError::Capacity(MeteredCopyError::BudgetFull))
    ));
    assert_eq!(assembler.progress().buffered_bytes, 2);
    assert_eq!(assembler.progress().highest_received_offset, 2);
    assert_eq!(assembler.read(10, true).unwrap().bytes, b"a"[..]);
    assert_eq!(assembler.read(10, true).unwrap().bytes, b"b"[..]);
    drop(held);
    drop(assembler);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn prepared_cancel_clear_reinit_and_last_reference_have_exact_release() {
    let (budget, meter, mut assembler) = owner();
    populate(&mut assembler, &meter);
    let used = budget.used();
    let before = assembler.progress();
    let replacement = Replacement::prepare(&assembler, freeze_metered).unwrap();
    assert!(budget.used() > used);
    drop(replacement);
    assert_eq!(budget.used(), used);
    assert_eq!(assembler.progress(), before);
    assembler.ensure_ordering(false).unwrap();
    let saved = assembler.read(3, false).unwrap().bytes;
    let backing = meter.snapshot().physical_bytes;
    assert!(backing > 9);
    assembler.clear();
    assert_eq!(meter.snapshot().physical_bytes, backing);
    assembler.reinit();
    assert!(assembler.state.is_ordered());
    assert!(assembler
        .backing_meter
        .as_ref()
        .unwrap()
        .same_authority(&meter));
    assert_eq!(assembler.progress().highest_received_offset, 0);
    let heap = std::alloc::Layout::array::<Buffer>(assembler.data.capacity())
        .unwrap()
        .size();
    assert_eq!(budget.used(), heap + backing);
    drop(assembler);
    assert_eq!(budget.used(), backing);
    drop(saved);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn dirty_interruption_and_preexisting_fatal_never_become_clean() {
    let (budget, meter, mut assembler) = owner();
    populate(&mut assembler, &meter);
    let before = assembler.progress();
    let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _mutation = Mutation::begin(Some(&meter)).unwrap();
        assembler.clear();
        panic!("original admitted mutation interrupted");
    }));
    assert!(caught.is_err());
    assert!(budget.quarantined());
    assert!(matches!(
        assembler.ensure_ordering(false),
        Err(ReadableError::BackingAuthorityLost)
    ));
    assert!(assembler.state.is_ordered());
    assert_eq!(
        assembler.progress().highest_received_offset,
        before.highest_received_offset
    );
    drop(assembler);
    assert_eq!(budget.used(), 1);
}
