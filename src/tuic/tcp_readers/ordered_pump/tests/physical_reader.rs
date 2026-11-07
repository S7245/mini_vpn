//! Original ordered reader backing, pressure and terminal ordering tracers.
use super::*;

#[tokio::test]
async fn copied_reader_chunk_keeps_source_meter_until_last_output_drop() {
    let meter = bytes::BackingMeter::new();
    let mut backing = bytes::BytesMut::with_capacity(256);
    backing.extend_from_slice(b"metered-output");
    let source_chunk = backing.split_to(14);
    assert!(source_chunk.charge_shared_backing(&meter));
    drop(backing);
    let source_bytes = source_chunk.freeze();
    let source_identity = source_bytes.allocation_identity();
    let source = SequenceOrderedSource {
        calls: Arc::new(AtomicU64::new(0)),
        chunks: vec![NativeTcpChunk {
            offset: 0,
            bytes: source_bytes,
        }],
        meter: Some(meter.clone()),
    };
    let (lease, reserved) = TcpPoolSlotLease::reserve(Arc::new(AtomicU64::new(0)));
    assert!(reserved);
    let (mut reader, prepared) =
        TuicNativeOrderedPumpReader::prepare_from_source(source, lease, None, None);
    let mut pump = prepared.commit(None);
    let output = std::future::poll_fn(|cx| reader.poll_read_chunk(cx, 4096))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&output.bytes[..], b"metered-output");
    assert_eq!(output.bytes.allocation_identity(), source_identity);
    drop(reader);
    tokio::time::timeout(Duration::from_secs(1), pump.settle())
        .await
        .unwrap()
        .unwrap();
    assert!(
        output
            .bytes
            .backing_meter()
            .is_some_and(|actual| actual.same_authority(&meter))
    );
    assert_eq!(meter.snapshot().allocations, 1);
    drop(output);
    assert_eq!(meter.snapshot().allocations, 0);
    assert_eq!(meter.snapshot().physical_bytes, 0);
}

#[tokio::test]
async fn two_source_chunks_make_one_inline_metered_reader_output() {
    let meter = bytes::BackingMeter::new();
    let mut chunks = Vec::new();
    for (offset, payload) in [(0, b"ab".as_slice()), (2, b"cde".as_slice())] {
        let mut backing = bytes::BytesMut::with_capacity(128);
        backing.extend_from_slice(payload);
        let chunk = backing.split_to(payload.len());
        assert!(chunk.charge_shared_backing(&meter));
        drop(backing);
        chunks.push(NativeTcpChunk {
            offset,
            bytes: chunk.freeze(),
        });
    }
    let source = SequenceOrderedSource {
        calls: Arc::new(AtomicU64::new(0)),
        chunks,
        meter: Some(meter.clone()),
    };
    let (lease, reserved) = TcpPoolSlotLease::reserve(Arc::new(AtomicU64::new(0)));
    assert!(reserved);
    let (mut reader, prepared) =
        TuicNativeOrderedPumpReader::prepare_from_source(source, lease, None, None);
    let mut pump = prepared.commit(None);
    let output = std::future::poll_fn(|cx| reader.poll_read_chunk(cx, 5))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&output.bytes[..], b"abcde");
    assert_eq!(
        meter.snapshot().allocations,
        3,
        "two source backings and one copied output"
    );
    drop(reader);
    tokio::time::timeout(Duration::from_secs(1), pump.settle())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(meter.snapshot().allocations, 1);
    drop(output);
    assert_eq!(meter.snapshot().allocations, 0);
    assert_eq!(meter.snapshot().physical_bytes, 0);
}

#[tokio::test]
async fn empty_ready_chunks_yield_after_one_bounded_reader_poll() {
    let calls = Arc::new(AtomicU64::new(0));
    let mut chunks = Vec::new();
    for _ in 0..65 {
        chunks.push(NativeTcpChunk {
            offset: 0,
            bytes: bytes::Bytes::new(),
        });
    }
    chunks.push(NativeTcpChunk {
        offset: 0,
        bytes: bytes::Bytes::from_static(b"x"),
    });
    let source = SequenceOrderedSource {
        calls: calls.clone(),
        chunks,
        meter: None,
    };
    let (lease, reserved) = TcpPoolSlotLease::reserve(Arc::new(AtomicU64::new(0)));
    assert!(reserved);
    let (mut reader, prepared) =
        TuicNativeOrderedPumpReader::prepare_from_source(source, lease, None, None);
    let mut pump = prepared.commit(None);
    tokio::time::timeout(Duration::from_secs(1), async {
        while calls.load(Ordering::SeqCst) < 65 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("producer fills rx64 with empty chunks");
    let (wakes, waker) = counter();
    let mut cx = Context::from_waker(&waker);
    assert!(reader.poll_read_chunk(&mut cx, 4096).is_pending());
    assert_eq!(reader.received_items, 64);
    assert!(wakes.0.load(Ordering::SeqCst) > 0);
    let output = tokio::time::timeout(
        Duration::from_secs(1),
        std::future::poll_fn(|cx| reader.poll_read_chunk(cx, 4096)),
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();
    assert_eq!(&output.bytes[..], b"x");
    drop(output);
    drop(reader);
    tokio::time::timeout(Duration::from_secs(1), pump.settle())
        .await
        .unwrap()
        .unwrap();
}

struct PrefixThenError {
    calls: Arc<AtomicU64>,
}

#[async_trait::async_trait]
impl TuicOrderedNativeChunkSource for PrefixThenError {
    async fn read_ordered_native_chunk(&mut self, _: usize) -> io::Result<Option<NativeTcpChunk>> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            Ok(Some(NativeTcpChunk {
                offset: 0,
                bytes: bytes::Bytes::from_static(b"prefix"),
            }))
        } else {
            Err(io::Error::other("source error after prefix"))
        }
    }
}

#[tokio::test]
async fn reader_delivers_prefix_before_deferred_source_error() {
    let calls = Arc::new(AtomicU64::new(0));
    let (lease, reserved) = TcpPoolSlotLease::reserve(Arc::new(AtomicU64::new(0)));
    assert!(reserved);
    let (mut reader, prepared) = TuicNativeOrderedPumpReader::prepare_from_source(
        PrefixThenError {
            calls: calls.clone(),
        },
        lease,
        None,
        None,
    );
    let clock = Arc::new(crate::shared::errors::FailureOrder::default());
    let mut pump = prepared.commit(Some(clock.clone()));
    let first = tokio::time::timeout(
        Duration::from_secs(1),
        std::future::poll_fn(|cx| reader.poll_read_chunk(cx, 4096)),
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();
    assert_eq!(&first.bytes[..], b"prefix");
    let error = std::future::poll_fn(|cx| reader.poll_read_chunk(cx, 4096)).await;
    assert!(error.is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(pump.error_order(), Some(0));
    let failure = pump
        .take_failure()
        .expect("original source error after prefix");
    assert_eq!(failure.order, Some(0));
    let crate::upstream::NativePumpFailureValue::Io(original) = failure.into_value() else {
        panic!("original source IO value required");
    };
    assert_eq!(original.to_string(), "source error after prefix");
    drop(first);
    drop(reader);
    assert!(
        tokio::time::timeout(Duration::from_secs(1), pump.settle())
            .await
            .unwrap()
            .is_ok()
    );
    assert_eq!(clock.load(Ordering::SeqCst), 1);
    assert!(pump.take_failure().is_none());
}

#[tokio::test]
async fn reader_rejects_foreign_backing_before_forwarding() {
    let expected = bytes::BackingMeter::new();
    let foreign = bytes::BackingMeter::new();
    let mut backing = bytes::BytesMut::with_capacity(128);
    backing.extend_from_slice(b"foreign");
    let chunk = backing.split_to(7);
    assert!(chunk.charge_shared_backing(&foreign));
    drop(backing);
    let source = SequenceOrderedSource {
        calls: Arc::new(AtomicU64::new(0)),
        chunks: vec![NativeTcpChunk {
            offset: 0,
            bytes: chunk.freeze(),
        }],
        meter: Some(expected.clone()),
    };
    let (lease, reserved) = TcpPoolSlotLease::reserve(Arc::new(AtomicU64::new(0)));
    assert!(reserved);
    let (mut reader, prepared) =
        TuicNativeOrderedPumpReader::prepare_from_source(source, lease, None, None);
    let mut pump = prepared.commit(None);
    let result = std::future::poll_fn(|cx| reader.poll_read_chunk(cx, 4096)).await;
    assert!(result.is_err());
    assert!(expected.snapshot().quarantined);
    assert!(foreign.snapshot().quarantined);
    drop(reader);
    let settled = tokio::time::timeout(Duration::from_secs(1), pump.settle())
        .await
        .expect("failed pump joins without pretending disposal");
    assert!(settled.is_err());
    assert_eq!(foreign.snapshot().allocations, 1);
}

#[tokio::test]
async fn full_rx64_and_split_pending_chunk_release_on_join_but_keep_output_charged() {
    let meter = bytes::BackingMeter::new();
    let calls = Arc::new(AtomicU64::new(0));
    let mut chunks = Vec::new();
    for index in 0..65u64 {
        let mut backing = bytes::BytesMut::with_capacity(128);
        backing.extend_from_slice(b"payload");
        let chunk = backing.split_to(7);
        assert!(chunk.charge_shared_backing(&meter));
        drop(backing);
        chunks.push(NativeTcpChunk {
            offset: index * 7,
            bytes: chunk.freeze(),
        });
    }
    let source = SequenceOrderedSource {
        calls: calls.clone(),
        chunks,
        meter: Some(meter.clone()),
    };
    let (lease, reserved) = TcpPoolSlotLease::reserve(Arc::new(AtomicU64::new(0)));
    assert!(reserved);
    let (mut reader, prepared) =
        TuicNativeOrderedPumpReader::prepare_from_source(source, lease, None, None);
    let mut pump = prepared.commit(None);
    tokio::time::timeout(Duration::from_secs(1), async {
        while calls.load(Ordering::SeqCst) < 65 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("producer reaches full rx64 pressure");
    let output = std::future::poll_fn(|cx| reader.poll_read_chunk(cx, 3))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&output.bytes[..], b"pay");
    assert!(
        meter.snapshot().allocations >= 65,
        "rx64 and pending slice remain charged"
    );
    drop(reader);
    tokio::time::timeout(Duration::from_secs(1), pump.settle())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        meter.snapshot().allocations,
        1,
        "only held output survives join"
    );
    drop(output);
    assert_eq!(meter.snapshot().allocations, 0);
    assert_eq!(meter.snapshot().physical_bytes, 0);
    assert!(!meter.snapshot().quarantined);
}

#[tokio::test(flavor = "current_thread")]
async fn hard_reader_full_keeps_original_two_part_backings_and_split_tail() {
    hard_full_reader(false).await;
}
#[tokio::test(flavor = "current_thread")]
async fn hard_reader_full_cancel_keeps_original_split_tail_until_actual_settle() {
    hard_full_reader(true).await;
}
async fn hard_full_reader(cancel: bool) {
    let budget = bytes::BackingBudget::new(65536).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let first = bytes::Bytes::copy_from_slice_metered(b"ab", &meter).unwrap();
    let second = bytes::Bytes::copy_from_slice_metered(b"cde", &meter).unwrap();
    let identities = [first.allocation_identity(), second.allocation_identity()];
    let source = SequenceOrderedSource { calls: Arc::new(AtomicU64::new(0)),
        chunks: vec![NativeTcpChunk { offset: 0, bytes: first }, NativeTcpChunk { offset: 2, bytes: second }],
        meter: Some(meter.clone()) };
    let active = Arc::new(AtomicU64::new(0));
    let (lease, reserved) = TcpPoolSlotLease::reserve(active.clone());
    assert!(reserved);
    let (mut reader, prepared) = TuicNativeOrderedPumpReader::prepare_from_source(source, lease, None, None);
    let mut pump = prepared.commit(None);
    tokio::time::timeout(Duration::from_secs(1), async {
        while reader.rx.as_ref().unwrap().len() < 2 { tokio::task::yield_now().await; }
    }).await.unwrap();
    let header = bytes::BytesMut::metered_copy_layout_for_len(1).unwrap() - 1;
    let pressure = bytes::BytesMut::try_zeroed_metered(budget.limit() - budget.used() - header, &meter).unwrap();
    assert_eq!(budget.used(), budget.limit());
    let mut outputs = Vec::new();
    let result = tokio::time::timeout(Duration::from_secs(1), async {
        // First request permits both parts: Hard must still hand off the original.
        let first = std::future::poll_fn(|cx| reader.poll_read_chunk(cx, 5)).await?;
        if let Some(first) = first { outputs.push(first); }
        while let Some(next) = std::future::poll_fn(|cx| reader.poll_read_chunk(cx, 2)).await? {
            outputs.push(next);
            if cancel { break; }
        }
        Ok::<(), io::Error>(())
    }).await;
    let non_q_at_full = !budget.quarantined();
    drop(pressure);
    drop(reader);
    let settled = tokio::time::timeout(Duration::from_secs(1), pump.settle()).await.unwrap();
    assert!(result.as_ref().is_ok_and(|r| r.is_ok()), "original read under actual Full: {result:?}");
    assert!(non_q_at_full);
    let expected = [(0, b"ab".as_slice()), (2, b"cd".as_slice()), (4, b"e".as_slice())];
    assert_eq!(outputs.iter().map(|c| (c.offset, c.bytes.as_ref())).collect::<Vec<_>>(), expected[..if cancel { 2 } else { 3 }]);
    assert_eq!(outputs[0].bytes.allocation_identity(), identities[0]);
    assert_eq!(outputs[1].bytes.allocation_identity(), identities[1]);
    if !cancel { assert_eq!(outputs[2].bytes.allocation_identity(), identities[1]); }
    let receipt = settled.unwrap();
    assert_eq!((receipt.source_read_bytes, receipt.forwarded_bytes), (5, if cancel { 4 } else { 5 }));
    assert_eq!((receipt.reader_discarded_bytes, receipt.producer_discarded_bytes, receipt.queue_discarded_bytes), (usize::from(cancel), 0, 0));
    assert!(pump.joined());
    drop((outputs, pump));
    assert_eq!(active.load(Ordering::SeqCst), 0);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}
