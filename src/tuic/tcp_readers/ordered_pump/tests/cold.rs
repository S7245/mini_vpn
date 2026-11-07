use super::super::cold as inventory;
use super::*;
use bytes::{BackingBudget, BackingMeter};

#[test]
fn both_cold_layouts_reject_before_allocation_and_cancel_exactly() {
    let source = inventory::SourceSlots::layout().size();
    let queue = quinn::owned_events::slot_layout::<custody::Item>(64)
        .unwrap()
        .size();
    for limit in [source - 1, source + queue - 1, source + queue] {
        let budget = BackingBudget::new(limit).unwrap();
        let meter = BackingMeter::with_budget(&budget);
        let result = (|| {
            let slots = inventory::SourceSlots::prepare(Some(&meter))?;
            assert_eq!(budget.used(), source);
            let channel = inventory::channel(Some(&meter))?;
            Ok::<_, io::Error>((slots, channel))
        })();
        if limit < source + queue {
            let error = result.err().unwrap();
            assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
            assert_eq!(
                error
                    .get_ref()
                    .unwrap()
                    .downcast_ref::<quinn::owned_events::AdmissionError>()
                    .unwrap()
                    .0,
                if limit < source {
                    bytes::MeteredCopyError::TooLarge
                } else {
                    bytes::MeteredCopyError::BudgetFull
                }
            );
        } else {
            assert_eq!(budget.used(), source + queue);
            drop(result.unwrap());
        }
        assert_eq!(budget.used(), 0);
        assert!(!budget.quarantined());
    }
}

#[tokio::test]
async fn hard_fifo_full_and_both_pending_values_settle_only_after_real_join_and_return() {
    let budget = BackingBudget::new(65536).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let bytes = bytes::Bytes::copy_from_slice_metered(b"xy", &meter).unwrap();
    let calls = Arc::new(AtomicU64::new(0));
    let source = SequenceOrderedSource {
        calls: calls.clone(),
        chunks: (0..66)
            .map(|i| NativeTcpChunk {
                offset: i * 2,
                bytes: bytes.clone(),
            })
            .collect(),
        meter: Some(meter.clone()),
    };
    let (mut reader, prepared) =
        TuicNativeOrderedPumpReader::prepare_from_source(source, lease(), None, None);
    assert!(matches!(reader.rx, Some(inventory::Receiver::Native(_))));
    let mut pump = prepared.commit(None);
    tokio::time::timeout(Duration::from_secs(2), async {
        while pump.source_read_bytes() != 130 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(reader.rx.as_ref().unwrap().len(), 64);
    let chunk = std::future::poll_fn(|cx| reader.poll_read_chunk(cx, 1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&chunk.bytes[..], b"x");
    assert_eq!(reader.pending_chunk.as_ref().unwrap().bytes.len(), 1);
    tokio::time::timeout(Duration::from_secs(2), async {
        while pump.source_read_bytes() != 132 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(reader.rx.as_ref().unwrap().len(), 64);
    assert_eq!(calls.load(Ordering::SeqCst), 66);
    pump.stop();
    tokio::time::timeout(Duration::from_secs(2), async {
        while !pump.joined() {
            pump.poll_join(&mut Context::from_waker(Waker::noop()));
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        pump.poll_settle(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert!(budget.used() > 0);
    drop(reader);
    let receipt = tokio::time::timeout(Duration::from_secs(2), pump.settle())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.source_read_bytes, 132);
    assert_eq!(receipt.sent_items, 65);
    assert_eq!(receipt.queue_discarded_items, 64);
    assert_eq!(receipt.queue_discarded_bytes, 128);
    assert_eq!(receipt.reader_discarded_bytes, 1);
    assert_eq!(receipt.producer_discarded_bytes, 2);
    assert_eq!(receipt.forwarded_bytes, 1);
    drop((pump, bytes, chunk));
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[tokio::test]
async fn shared_budget_quarantine_never_becomes_reader_eof_or_clean_join_receipt() {
    let budget = BackingBudget::new(65536).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let source = SequenceOrderedSource {
        calls: Arc::new(AtomicU64::new(0)),
        chunks: Vec::new(),
        meter: Some(meter.clone()),
    };
    let (mut reader, prepared) =
        TuicNativeOrderedPumpReader::prepare_from_source(source, lease(), None, None);
    let mut pump = prepared.commit(None);
    meter.quarantine();
    assert!(matches!(
        reader.poll_read_chunk(&mut Context::from_waker(Waker::noop()), 8),
        Poll::Ready(Err(_))
    ));
    drop(reader);
    assert!(
        tokio::time::timeout(Duration::from_secs(2), pump.settle())
            .await
            .unwrap()
            .is_err()
    );
    assert!(pump.receipt().is_none());
    assert!(budget.used() > 0);
    // Existing Q custody deliberately retains its outcome instead of minting a receipt.
    drop(pump);
    assert!(budget.used() > 0 && budget.quarantined());
}

#[tokio::test]
async fn original_reader_waiter_clone_panic_retains_first_before_any_recovery_poll() {
    use std::{
        mem::ManuallyDrop,
        task::{RawWaker, RawWakerVTable},
    };
    unsafe fn clone_panic(data: *const ()) -> RawWaker {
        let payload = ManuallyDrop::new(unsafe { Arc::from_raw(data.cast::<()>()) });
        std::panic::panic_any(Arc::clone(&payload));
    }
    unsafe fn drop_arc(data: *const ()) {
        drop(unsafe { Arc::from_raw(data.cast::<()>()) });
    }
    unsafe fn noop(_: *const ()) {}
    static TABLE: RawWakerVTable = RawWakerVTable::new(clone_panic, drop_arc, noop, drop_arc);
    let budget = BackingBudget::new(65536).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let source = SequenceOrderedSource {
        calls: Arc::new(AtomicU64::new(0)),
        chunks: Vec::new(),
        meter: Some(meter.clone()),
    };
    let (mut reader, prepared) =
        TuicNativeOrderedPumpReader::prepare_from_source(source, lease(), None, None);
    let payload = Arc::new(());
    let waker =
        unsafe { Waker::from_raw(RawWaker::new(Arc::into_raw(payload.clone()).cast(), &TABLE)) };
    assert!(matches!(
        reader.poll_read_chunk(&mut Context::from_waker(&waker), 8),
        Poll::Ready(Err(_))
    ));
    assert!(budget.quarantined() && meter.snapshot().quarantined);
    let failure = reader.shared.take_failure().unwrap();
    let crate::upstream::NativePumpFailureValue::Panic(original) = failure.into_value() else {
        panic!("original clone panic")
    };
    assert!(Arc::ptr_eq(
        original.downcast_ref::<Arc<()>>().unwrap(),
        &payload
    ));
    assert!(reader.shared.take_failure().is_none());
    drop((waker, reader, prepared));
    assert_eq!(budget.used(), 1);
}

#[tokio::test]
async fn hard_legacy_gate_returns_original_uncommitted_pump_and_cancels_exactly() {
    for hard in [true, false] {
        let budget = BackingBudget::new(65536).unwrap();
        let meter = if hard {
            BackingMeter::with_budget(&budget)
        } else {
            BackingMeter::new()
        };
        let calls = Arc::new(AtomicU64::new(0));
        let bytes = bytes::Bytes::copy_from_slice_metered(b"original", &meter).unwrap();
        let source = SequenceOrderedSource {
            calls: calls.clone(),
            chunks: vec![NativeTcpChunk { offset: 0, bytes }],
            meter: Some(meter.clone()),
        };
        let (reader, prepared) =
            TuicNativeOrderedPumpReader::prepare_from_source(source, lease(), None, None);
        let carrier = crate::upstream::NativeTcpRelayStream::new(
            Box::new(reader),
            Box::new(tokio::io::sink()),
        )
        .with_prepared_pump(prepared);
        let result = carrier.try_start_legacy();
        if hard {
            let original = result.err().expect("hard capability must stay cold");
            assert!(original.has_prepared_pump());
            assert!(
                original
                    .reader
                    .receive_backing_meter()
                    .unwrap()
                    .same_authority(&meter)
            );
            for _ in 0..8 {
                tokio::task::yield_now().await;
            }
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            assert!(budget.used() > 0);
            drop(original);
        } else {
            let (mut reader, writer, mut pump) =
                result.ok().expect("observer-only legacy remains supported");
            let chunk = std::future::poll_fn(|cx| reader.poll_read_chunk(cx, 8))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(&chunk.bytes[..], b"original");
            drop((reader, writer, chunk));
            pump.stop(Duration::from_secs(1));
            pump.finish().await;
            drop(pump);
            assert!(calls.load(Ordering::SeqCst) > 0);
        }
        assert_eq!(budget.used(), 0);
        assert_eq!(meter.snapshot().physical_bytes, 0);
        assert!(!budget.quarantined() && !meter.is_quarantined());
    }
}
