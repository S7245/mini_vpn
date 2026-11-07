//! Actual descriptor backing and staged growth on the original heap owner.
use super::*;
use bytes::{BackingBudget, BytesMut};

#[test]
fn actual_2049th_descriptor_fits_measured_headroom_without_replacing_old_array() {
    let budget = BackingBudget::new(1 << 20).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let payload = BytesMut::try_copy_from_slice_metered(b"x", &meter).unwrap().freeze();
    let mut heap = Heap::prepare(2048, Some(&meter)).unwrap();
    for offset in 0..2048 { heap.push(Buffer::new(offset, payload.clone(), 1)); }
    let hold = meter.reserve_cold_layout(budget.limit() - budget.used() - 77599).unwrap().unwrap();
    let before = budget.used();
    let old_identity = payload.allocation_identity();
    // This is the actual end-to-end RED headroom, not a raised global quota.
    let prepared = heap.growth(1, Some(&meter))
        .expect("one new descriptor must not require a second whole 4096-record array");
    assert_eq!(heap.len(), 2048, "prepare cannot publish ownership");
    drop(prepared);
    assert_eq!(budget.used(), before, "cancelled preparation refunds only its own actual layouts");
    let prepared = heap.growth(1, Some(&meter)).unwrap();
    heap.install(prepared);
    heap.push(Buffer::new(2048, payload.clone(), 1));
    assert_eq!(heap.len(), 2049);
    assert!(heap.layout_matches(&meter));
    assert!(budget.used() > before && budget.used() - before < 77599);
    assert!(heap.iter().all(|buffer| buffer.bytes.allocation_identity() == old_identity));
    let full = meter.reserve_cold_layout(budget.limit() - budget.used()).unwrap().unwrap();
    let same_page = heap.growth(1, Some(&meter)).unwrap();
    heap.install(same_page);
    heap.push(Buffer::new(2049, payload.clone(), 1));
    let before_len = heap.len();
    assert!(matches!(heap.growth(heap.capacity() - heap.len() + 1, Some(&meter)),
        Err(InsertError::Capacity(MeteredCopyError::BudgetFull))));
    assert_eq!(heap.len(), before_len);
    assert!(heap.layout_matches(&meter));
    assert!(!budget.quarantined());
    drop((heap, payload, full, hold));
    assert_eq!(budget.used(), 0);
}

#[test]
fn missing_promised_tail_is_quarantine_before_any_read_accounting() {
    let budget = BackingBudget::new(1 << 20).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let payload = BytesMut::try_copy_from_slice_metered(b"x", &meter).unwrap().freeze();
    let mut assembler = crate::connection::assembler::Assembler::new();
    assembler.data = Heap::prepare(PAGE_RECORDS + 1, Some(&meter)).unwrap();
    assembler.backing_meter = Some(meter.clone());
    for offset in 0..=PAGE_RECORDS {
        assembler.insert_checked(offset as u64, payload.clone(), 1, Some(&meter)).unwrap();
    }
    let before = (assembler.buffered, assembler.allocated, assembler.bytes_read());
    let held = match &mut assembler.data {
        Heap::Large { body, .. } => body.pages.values[1].values.pop().unwrap(),
        _ => panic!("actual large heap required"),
    };
    assert!(matches!(assembler.read_checked(1, true), Err(crate::connection::streams::ReadableError::BackingAuthorityLost)));
    assert_eq!((assembler.buffered, assembler.allocated, assembler.bytes_read()), before);
    assert!(budget.quarantined(), "lost physical tail is not ordinary empty/blocked");
    assert!(budget.used() > 0);
    assert_eq!(held.offset, PAGE_RECORDS as u64);
    drop((assembler, held, payload));
    assert!(budget.quarantined());
    assert!(budget.used() > 0);
}


#[test]
fn paged_reads_match_original_binary_heap_across_page_sifts_and_partial_heads() {
    let budget = BackingBudget::new(1 << 22).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let payload = BytesMut::try_copy_from_slice_metered(&[7; 31], &meter).unwrap().freeze();
    let mut paged = Heap::prepare(PAGE_RECORDS + 1, Some(&meter)).unwrap();
    let mut original = Heap::default();
    for index in 0..(PAGE_RECORDS * 9 + 7) {
        // Repeated offsets with different lengths exercise the full original Ord.
        let offset = ((index * 73) % 97) as u64;
        let len = 1 + index % 31;
        let item = Buffer::new(offset, payload.slice(..len), 31);
        let prepared = paged.growth(1, Some(&meter)).unwrap();
        paged.install(prepared);
        paged.push(item.clone());
        let prepared = original.growth(1, None).unwrap();
        original.install(prepared);
        original.push(item);
    }
    let charged = budget.used();
    let mut turn = 0;
    while !original.is_empty() {
        let head = original.peek().unwrap();
        let skip = if turn % 5 == 0 { head.bytes.len() / 2 } else { 0 };
        let max = [0, 1, 7, usize::MAX][turn % 4];
        let expected = original.read_head(skip, max).unwrap().unwrap();
        let actual = paged.read_head(skip, max).unwrap().unwrap();
        assert_eq!((actual.offset, actual.discarded, actual.allocation_removed),
            (expected.offset, expected.discarded, expected.allocation_removed));
        assert_eq!(actual.bytes, expected.bytes);
        assert_eq!(actual.bytes.allocation_identity(), expected.bytes.allocation_identity());
        assert_eq!(paged.len(), original.len());
        assert!(paged.layout_matches(&meter));
        assert_eq!(budget.used(), charged, "read returns availability, not cold backing charge");
        turn += 1;
    }
    assert!(paged.read_head(0, 1).unwrap().is_none());
    drop((paged, original, payload));
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}


impl crate::connection::assembler::Assembler {
    pub(in crate::connection) fn take_page_allowance_for_probe(&mut self) -> bytes::BackingBudgetReservation {
        match &mut self.data {
            Heap::Large { body, .. } => body.pages.values[1].allowance.take().unwrap(),
            _ => panic!("probe requires actual retained paged heap"),
        }
    }
}

#[test]
fn cold_settlement_rejects_missing_owner_and_out_of_bounds_counters_without_clear() {
    for fault in 0..4 {
        let budget = BackingBudget::new(1 << 20).unwrap();
        let meter = BackingMeter::with_budget(&budget);
        let payload = BytesMut::try_copy_from_slice_metered(b"xy", &meter).unwrap().freeze();
        let mut assembler = crate::connection::assembler::Assembler::new();
        assembler.data = Heap::prepare(PAGE_RECORDS + 1, Some(&meter)).unwrap();
    assembler.backing_meter = Some(meter.clone());
        assembler.insert_checked(10, payload.clone(), 2, Some(&meter)).unwrap();
        match fault {
            0 => assembler.backing_meter = None,
            1 => assembler.bytes_read = 13,
            2 => assembler.end = 10,
            3 => {
                assembler.ensure_ordering(false).unwrap();
                assembler.end = 10;
            }
            _ => unreachable!(),
        }
        let before = (assembler.buffered, assembler.allocated, assembler.bytes_read, assembler.end);
        assert!(assembler.clear_checked().is_err(), "fault {fault} must not normalize to cleared");
        assert_eq!((assembler.buffered, assembler.allocated, assembler.bytes_read, assembler.end), before);
        assert_eq!(assembler.data.len(), 1);
        assert!(assembler.reinit_checked().is_err());
        assert!(budget.quarantined());
        drop((assembler, payload));
        assert!(budget.used() > 0);
    }
}


#[test]
fn every_growth_allocation_failure_preserves_originals_and_commit_allocates_nothing() {
    for initial in [PAGE_RECORDS, PAGE_RECORDS * 2] {
        let budget = BackingBudget::new(1 << 22).unwrap();
        let meter = BackingMeter::with_budget(&budget);
        let payload = BytesMut::try_copy_from_slice_metered(b"x", &meter).unwrap().freeze();
        let mut heap = Heap::prepare(initial, Some(&meter)).unwrap();
        for offset in 0..initial { heap.push(Buffer::new(offset as u64, payload.clone(), 1)); }
        let old: Vec<_> = heap.iter().map(|v| (v as *const Buffer, v.offset, v.bytes.as_ptr(), v.bytes.allocation_identity())).collect();
        let baseline = budget.used();
        let additional = PAGE_RECORDS * 3 + 1;
        let mut prepared = None;
        let calls = crate::tests::allocations_in(|| {
            prepared = heap.growth(additional, Some(&meter)).unwrap();
        });
        assert!(calls >= 5, "actual directory/pages and promotion layout must be counted");
        drop(prepared);
        assert_eq!(budget.used(), baseline);
        for fail_at in 1..=calls {
            let mut result = None;
            crate::tests::allocator_failure_in(fail_at, || {
                result = Some(heap.growth(additional, Some(&meter)));
            });
            assert!(matches!(result.unwrap(), Err(InsertError::Capacity(MeteredCopyError::Allocation))), "stage {fail_at}");
            assert_eq!(budget.used(), baseline, "failed stage {fail_at} leaked or refunded old stock");
            let actual: Vec<_> = heap.iter().map(|v| (v as *const Buffer, v.offset, v.bytes.as_ptr(), v.bytes.allocation_identity())).collect();
            assert_eq!(actual, old);
            assert!(heap.layout_matches(&meter));
            assert!(!budget.quarantined());
        }
        let prepared = heap.growth(additional, Some(&meter)).unwrap();
        let allocations = crate::tests::allocations_in(|| {
            heap.install(prepared);
            for offset in initial..initial + additional {
                heap.push(Buffer::new(offset as u64, payload.clone(), 1));
            }
        });
        assert_eq!(allocations, 0, "publication may only move already funded inventory");
        assert!(heap.layout_matches(&meter));
        if initial > PAGE_RECORDS {
            // Paged growth never relocates any old descriptor page.
            let actual: Vec<_> = heap.iter().take(initial).map(|v| (v as *const Buffer, v.offset, v.bytes.as_ptr(), v.bytes.allocation_identity())).collect();
            assert_eq!(actual, old);
        }
        let charged = budget.used();
        heap.clear();
        assert_eq!(budget.used(), charged, "empty cold pages remain real inventory");
        assert!(heap.layout_matches(&meter));
        drop((heap, payload));
        assert_eq!(budget.used(), 0);
        assert!(!budget.quarantined());
    }
}


impl crate::connection::assembler::Assembler {
    pub(in crate::connection) fn lose_root_with_sparse_stock_for_probe(&mut self) {
        assert!(matches!(self.state, crate::connection::assembler::State::Unordered { .. }));
        self.clear_checked().unwrap();
        self.data = Heap::default();
        self.backing_meter = None;
    }
}


#[test]
fn actual_receive_layout_and_empty_paged_cold_inventory() {
    let budget = BackingBudget::new(1 << 20).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let heap = Heap::prepare(2048, Some(&meter)).unwrap();
    let expected = Layout::new::<Paged>().size()
        + Layout::array::<Slab<Buffer>>(2048 / PAGE_RECORDS).unwrap().size()
        + Layout::array::<Buffer>(2048).unwrap().size();
    assert_eq!(budget.used(), expected);
    eprintln!("receive-layout Assembler={} Heap={} Buffer={} Paged={} Slab={} records_per_page={} page_bytes={} empty_2048={}",
        mem::size_of::<crate::connection::assembler::Assembler>(), mem::size_of::<Heap>(),
        mem::size_of::<Buffer>(), mem::size_of::<Paged>(), mem::size_of::<Slab<Buffer>>(),
        PAGE_RECORDS, Layout::array::<Buffer>(PAGE_RECORDS).unwrap().size(), expected);
    drop(heap);
    assert_eq!(budget.used(), 0);
}


#[test]
fn lost_root_is_neither_rebound_by_next_data_nor_consumed_by_read() {
    for read in [false, true] {
        let budget = BackingBudget::new(1 << 20).unwrap();
        let meter = BackingMeter::with_budget(&budget);
        let payload = BytesMut::try_copy_from_slice_metered(b"x", &meter).unwrap().freeze();
        let mut assembler = crate::connection::assembler::Assembler::new();
        assembler.insert_checked(0, payload.clone(), 1, Some(&meter)).unwrap();
        assembler.backing_meter = None;
        let before = (assembler.buffered, assembler.allocated, assembler.bytes_read(), assembler.end);
        if read { assert!(assembler.read_checked(1, true).is_err()); }
        else { assert!(matches!(assembler.insert_checked(1, payload.clone(), 1, Some(&meter)), Err(InsertError::AuthorityMismatch))); }
        assert!(assembler.backing_meter.is_none());
        assert_eq!((assembler.buffered, assembler.allocated, assembler.bytes_read(), assembler.end), before);
        assert!(budget.quarantined());
        drop((assembler, payload));
        assert!(budget.used() > 0);
    }
}


impl crate::connection::assembler::Assembler {
    pub(in crate::connection) fn take_actual_tail_for_probe(&mut self) -> bytes::Bytes {
        match &mut self.data {
            Heap::Large { body, .. } => body.pages.values[(body.len - 1) / PAGE_RECORDS].values.pop().unwrap().bytes,
            _ => panic!("actual paged heap required"),
        }
    }
}
