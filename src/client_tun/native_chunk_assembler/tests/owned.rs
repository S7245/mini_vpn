use super::*;

#[test]
fn owned_drain_preserves_gap_order_and_exposes_held_capacity() {
    let mut assembler = NativeTcpChunkAssembler::default();
    assembler
        .push_chunk(NativeTcpChunk {
            offset: 4,
            bytes: Bytes::from_static(b"efgh"),
        })
        .unwrap();
    assert!(assembler.drain_owned_up_to(8).unwrap().is_none());
    assembler
        .push_chunk(NativeTcpChunk {
            offset: 0,
            bytes: Bytes::from_static(b"abcd"),
        })
        .unwrap();
    let first = assembler
        .drain_owned_up_to(3)
        .unwrap()
        .expect("first prefix");
    assert_eq!(first.as_ref(), b"abc");
    assert!(first.capacity() >= first.len());
    let rest = assembler
        .drain_owned_up_to(8)
        .unwrap()
        .expect("remaining prefix");
    assert_eq!(rest.as_ref(), b"defgh");
    assert_eq!(assembler.buffered_bytes(), 0);
}

#[test]
fn overlap_and_replacement_disposal_balance_raw_native_receipts() {
    let mut assembler = NativeTcpChunkAssembler::default();
    let mut raw = 0usize;
    let chunks: &[(u64, &[u8])] = &[
        (4, b"ef"),
        (4, b"efgh"),
        (4, b"ef"),
        (0, b"abcd"),
        (0, b"ab"),
        (6, b"ghij"),
    ];
    for &(offset, bytes) in chunks {
        raw += bytes.len();
        assembler
            .push_chunk(NativeTcpChunk {
                offset,
                bytes: Bytes::copy_from_slice(bytes),
            })
            .unwrap();
    }
    let first = assembler
        .drain_owned_up_to(10)
        .unwrap()
        .expect("ordered prefix");
    assert_eq!(first.as_ref(), b"abcdefghij");
    assert_eq!(assembler.buffered_bytes(), 0);
    assert_eq!(first.len() + assembler.discarded_bytes().unwrap(), raw);
}

#[test]
fn original_hard_drain_rejects_full_before_consuming_tree_offset_or_backings() {
    let budget = bytes::BackingBudget::new(65536).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let first = Bytes::copy_from_slice_metered(b"abcd", &meter).unwrap();
    let second = Bytes::copy_from_slice_metered(b"efgh", &meter).unwrap();
    let identities = [first.allocation_identity(), second.allocation_identity()];
    let mut assembler = NativeTcpChunkAssembler::default();
    assembler
        .push_chunk(NativeTcpChunk {
            offset: 0,
            bytes: first,
        })
        .unwrap();
    assembler
        .push_chunk(NativeTcpChunk {
            offset: 4,
            bytes: second,
        })
        .unwrap();
    let before = (
        assembler.next_offset,
        assembler.buffered_bytes,
        assembler.discarded_bytes,
        assembler.raw_bytes,
    );
    let held = budget.try_reserve(budget.limit() - budget.used()).unwrap();
    let output = assembler.drain_owned_up_to(6);
    let rejected = matches!(output, Err(bytes::MeteredCopyError::BudgetFull));
    let unchanged = (
        assembler.next_offset,
        assembler.buffered_bytes,
        assembler.discarded_bytes,
        assembler.raw_bytes,
    ) == before
        && assembler
            .chunks
            .get(&0)
            .and_then(Bytes::allocation_identity)
            == identities[0]
        && assembler
            .chunks
            .get(&4)
            .and_then(Bytes::allocation_identity)
            == identities[1];
    drop((output, held, assembler));
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
    assert!(
        rejected && unchanged,
        "hard drain must reject before allocating unmetered output or consuming original prefix"
    );
}

#[test]
fn partial_remainder_collision_balances_discard_and_output_last_reference() {
    let budget = bytes::BackingBudget::new(65536).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let mut assembler = NativeTcpChunkAssembler::with_meter(Some(meter.clone()));
    for (offset, bytes) in [(0, b"abcdefgh".as_slice()), (3, b"def".as_slice())] {
        assembler
            .push_chunk(NativeTcpChunk {
                offset,
                bytes: Bytes::copy_from_slice_metered(bytes, &meter).unwrap(),
            })
            .unwrap();
    }
    let first = assembler.drain_owned_up_to(3).unwrap().unwrap();
    assert_eq!(first.as_ref(), b"abc");
    assert_eq!(assembler.buffered_bytes(), 5);
    assert_eq!(assembler.discarded_bytes(), Some(3));
    assert_eq!(assembler.metered_buffered_bytes(&meter), Some(5));
    let last = assembler
        .drain_owned_up_to(8)
        .unwrap()
        .unwrap()
        .into_custodied_bytes();
    assert_eq!(last.as_ref(), b"defgh");
    let retained = last.slice(1..);
    assert_eq!(retained.allocation_identity(), last.allocation_identity());
    drop((assembler, first, last));
    assert!(budget.used() > 0);
    assert_eq!(retained.backing_meter().unwrap().snapshot().allocations, 1);
    drop(retained);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn metadata_refusal_returns_original_unconsumed_input_and_q_rejects_output() {
    let budget = bytes::BackingBudget::new(65536).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let input = Bytes::copy_from_slice_metered(b"abcdef", &meter).unwrap();
    let identity = input.allocation_identity();
    let mut assembler = NativeTcpChunkAssembler::with_meter(Some(meter.clone()));
    let held = budget.try_reserve(budget.limit() - budget.used()).unwrap();
    let (error, chunk) = assembler
        .push_chunk(NativeTcpChunk {
            offset: 0,
            bytes: input,
        })
        .unwrap_err();
    assert_eq!(error, bytes::MeteredCopyError::BudgetFull);
    assert_eq!(chunk.bytes.allocation_identity(), identity);
    assert_eq!(
        (
            assembler.next_offset,
            assembler.raw_bytes,
            assembler.buffered_bytes
        ),
        (0, 0, 0)
    );
    drop(held);
    assembler.push_chunk(chunk).unwrap();
    meter.quarantine();
    assert!(matches!(
        assembler.drain_owned_up_to(6),
        Err(bytes::MeteredCopyError::Quarantined)
    ));
    assert_eq!(assembler.buffered_bytes, 6);
    drop(assembler);
    assert_eq!(budget.used(), 1);
}

#[test]
fn covered_zero_output_prefix_is_discarded_without_allocating_output() {
    let budget = bytes::BackingBudget::new(65536).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let mut assembler = NativeTcpChunkAssembler::with_meter(Some(meter.clone()));
    for (offset, bytes) in [(1, b"bc".as_slice()), (0, b"abc".as_slice())] {
        assembler
            .push_chunk(NativeTcpChunk {
                offset,
                bytes: Bytes::copy_from_slice_metered(bytes, &meter).unwrap(),
            })
            .unwrap();
    }
    let output = assembler.drain_owned_up_to(3).unwrap().unwrap();
    assert_eq!(output.as_ref(), b"abc");
    let held = budget.try_reserve(budget.limit() - budget.used()).unwrap();
    assert!(assembler.drain_owned_up_to(3).unwrap().is_none());
    let observed = (assembler.buffered_bytes(), assembler.discarded_bytes());
    drop((held, output, assembler));
    assert_eq!(budget.used(), 0);
    assert_eq!(observed, (0, Some(2)));
}

#[test]
fn logically_empty_legacy_slab_cannot_be_promoted_to_unpaid_native_inventory() {
    let budget = bytes::BackingBudget::new(65536).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let mut assembler = NativeTcpChunkAssembler::default();
    assembler
        .push_chunk(NativeTcpChunk {
            offset: 0,
            bytes: Bytes::from_static(b"a"),
        })
        .unwrap();
    drop(assembler.drain_owned_up_to(1).unwrap());
    let chunk = NativeTcpChunk {
        offset: 1,
        bytes: Bytes::copy_from_slice_metered(b"b", &meter).unwrap(),
    };
    let held = budget.try_reserve(budget.limit() - budget.used()).unwrap();
    let rejected = assembler.push_chunk(chunk).is_err();
    drop((held, assembler));
    assert!(
        rejected,
        "retained legacy metadata must not become uncharged Native storage"
    );
}

#[test]
fn empty_assembler_still_checks_original_authority() {
    let own_budget = bytes::BackingBudget::new(65536).unwrap();
    let foreign_budget = bytes::BackingBudget::new(65536).unwrap();
    let own = BackingMeter::with_budget(&own_budget);
    let foreign = BackingMeter::with_budget(&foreign_budget);
    let assembler = NativeTcpChunkAssembler::with_meter(Some(own.clone()));
    assert_eq!(assembler.metered_buffered_bytes(&own), Some(0));
    assert_eq!(assembler.metered_buffered_bytes(&foreign), None);
    drop(assembler);
    assert!(own_budget.quarantined() && foreign_budget.quarantined());
    assert_eq!((own_budget.used(), foreign_budget.used()), (1, 1));
}

#[tokio::test(flavor = "current_thread")]
async fn admitted_original_fifo_operation_copies_and_publishes_under_full_without_new_backing() {
    use crate::shared::errors::*;
    use crate::tcp_downlink_pump::AsyncByteBoundedFlowQueue;
    struct Sink;
    impl QuietFailureSink for Sink {
        fn record_quiet(&self, _: RetainedFailure) { panic!("healthy admitted copy must not fail"); }
    }
    let budget = bytes::BackingBudget::new(65536).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let sink: std::sync::Arc<dyn QuietFailureSink> = std::sync::Arc::new(Sink);
    let clock = std::sync::Arc::new(FailureOrder::default());
    clock.bind(std::sync::Arc::downgrade(&sink), FailureOrigin {
        phase: FailurePhase::TcpOpen, resource: FailureResource::TcpOpen,
        resource_id: 0, attempt_id: Some(1),
    }, Some(meter.clone())).unwrap();
    let (queue, mut stock) = AsyncByteBoundedFlowQueue::new_with_progressive_output_stock(
        16, &meter, clock.clone(), 8, 4).unwrap();
    let mut producer = stock.take_producer().unwrap();
    let mut operation = None;
    producer.prepare_read(6, &mut operation).await.unwrap();
    let mut operation = operation.unwrap();
    assert_eq!(operation.capacity(), 4, "actual 4B bootstrap bounds the original read");
    let mut assembler = NativeTcpChunkAssembler::with_meter(Some(meter.clone()));
    assembler.push_chunk(NativeTcpChunk { offset: 4,
        bytes: Bytes::copy_from_slice_metered(b"efgh", &meter).unwrap() }).unwrap();
    assert!(assembler.drain_admitted(operation.space(), usize::MAX).unwrap().is_none());
    assert!(operation.space().is_some(), "gap keeps the exact original output operation");
    assembler.push_chunk(NativeTcpChunk { offset: 0,
        bytes: Bytes::copy_from_slice_metered(b"abcd", &meter).unwrap() }).unwrap();
    let full = budget.try_reserve(budget.limit() - budget.used()).unwrap();
    let before = (assembler.next_offset, assembler.buffered_bytes, assembler.discarded_bytes);
    assert!(matches!(assembler.drain_owned_up_to(4), Err(MeteredCopyError::BudgetFull)));
    assert_eq!((assembler.next_offset, assembler.buffered_bytes, assembler.discarded_bytes), before);
    let registered = meter.snapshot().registered_total;
    let mut pending = assembler.drain_admitted(operation.space(), usize::MAX).unwrap();
    assert_eq!(pending.as_ref().unwrap().as_ref(), b"abcd");
    assert!(operation.space().is_none());
    operation.publish(&mut pending).await.unwrap();
    assert!(pending.is_none());
    assert_eq!(meter.snapshot().registered_total, registered, "copy/publish uses only pre-admitted allocations");
    assert_eq!((assembler.next_offset, assembler.buffered_bytes), (4, 4));
    drop(operation);
    let mut held = None;
    assert!(queue.recv_custodied(4, &mut held).await.unwrap());
    assert_eq!(held.as_ref().unwrap().bytes.as_ref(), b"abcd");
    let identity = held.as_ref().unwrap().bytes.allocation_identity();
    let charged = budget.used();
    drop(held);
    assert_eq!(budget.used(), charged, "last loan returns availability, not cold stock credit");
    let mut next = None;
    producer.prepare_read(4, &mut next).await.unwrap();
    let mut next = next.unwrap();
    next.space().as_mut().unwrap().write(b"next").unwrap();
    let reused = next.space().take().unwrap().finish().unwrap();
    assert_eq!(reused.allocation_identity(), identity);
    drop((reused, next, producer, full, assembler));
    queue.close().await;
    assert!(queue.dispose_data().unwrap().is_some());
    stock.wait_returned().await.unwrap();
    assert!(stock.try_retire());
    assert!(queue.dispose_custodied().unwrap().is_some());
    drop((queue, stock, clock));
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[tokio::test(flavor = "current_thread")]
async fn q_after_copy_preserves_original_space_operation_and_uncommitted_input() {
    use crate::shared::errors::*;
    use crate::tcp_downlink_pump::AsyncByteBoundedFlowQueue;
    struct Sink;
    impl QuietFailureSink for Sink { fn record_quiet(&self, _: RetainedFailure) {} }
    let budget = bytes::BackingBudget::new(65536).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let sink: std::sync::Arc<dyn QuietFailureSink> = std::sync::Arc::new(Sink);
    let clock = std::sync::Arc::new(FailureOrder::default());
    clock.bind(std::sync::Arc::downgrade(&sink), FailureOrigin {
        phase: FailurePhase::TcpOpen, resource: FailureResource::TcpOpen,
        resource_id: 0, attempt_id: Some(1),
    }, Some(meter.clone())).unwrap();
    let (queue, mut stock) = AsyncByteBoundedFlowQueue::new_with_progressive_output_stock(
        16, &meter, clock, 8, 4).unwrap();
    let mut producer = stock.take_producer().unwrap();
    let mut operation = None;
    producer.prepare_read(4, &mut operation).await.unwrap();
    let mut operation = operation.unwrap();
    let original = operation.space().as_ref().unwrap().operation_for_test().unwrap();
    let mut assembler = NativeTcpChunkAssembler::with_meter(Some(meter.clone()));
    let input = Bytes::copy_from_slice_metered(b"original", &meter).unwrap();
    let identity = input.allocation_identity();
    assembler.push_chunk(NativeTcpChunk { offset: 0, bytes: input }).unwrap();
    let before = (assembler.next_offset, assembler.buffered_bytes, assembler.discarded_bytes);
    Q_BEFORE_FINISH.with(|flag| flag.set(true));
    assert!(matches!(assembler.drain_admitted(operation.space(), usize::MAX), Err(MeteredCopyError::Quarantined)));
    assert_eq!((assembler.next_offset, assembler.buffered_bytes, assembler.discarded_bytes), before);
    assert_eq!(assembler.chunks.first().unwrap().1.allocation_identity(), identity);
    let retained = operation.space().as_ref().expect("original caller still owns its admitted OutputSpace");
    assert_eq!(retained.operation_for_test(), Some((original.0, original.1, original.2, 4)));
    assert!(budget.quarantined() && budget.used() > 0);
    assert!(!stock.try_retire());
    // No retry or clean assertion: the original inputs and grant remain in Q.
    drop((assembler, operation, producer, stock, queue));
    assert!(budget.quarantined() && budget.used() > 0);
}

#[test]
fn missing_admitted_space_preserves_input_and_quarantines_original_assembler() {
    let budget = bytes::BackingBudget::new(65536).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let mut assembler = NativeTcpChunkAssembler::with_meter(Some(meter.clone()));
    let input = Bytes::copy_from_slice_metered(b"original", &meter).unwrap();
    let identity = input.allocation_identity();
    assembler.push_chunk(NativeTcpChunk { offset: 0, bytes: input }).unwrap();
    let before = (assembler.next_offset, assembler.buffered_bytes, assembler.discarded_bytes);
    assert!(matches!(assembler.drain_admitted(&mut None, usize::MAX), Err(MeteredCopyError::Quarantined)));
    assert_eq!((assembler.next_offset, assembler.buffered_bytes, assembler.discarded_bytes), before);
    assert_eq!(assembler.chunks.first().unwrap().1.allocation_identity(), identity);
    assert!(budget.quarantined() && budget.used() > 0);
}
