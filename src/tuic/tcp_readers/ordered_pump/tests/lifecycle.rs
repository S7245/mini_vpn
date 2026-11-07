use super::*;
mod publication;

struct PendingOnceOrderedSource {
    calls: Arc<AtomicU64>,
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
    payload: bytes::Bytes,
}

#[async_trait::async_trait]
impl TuicOrderedNativeChunkSource for PendingOnceOrderedSource {
    async fn read_ordered_native_chunk(
        &mut self,
        _max_len: usize,
    ) -> io::Result<Option<NativeTcpChunk>> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        if call == 0 {
            self.entered.notify_one();
            self.release.notified().await;
            return Ok(Some(NativeTcpChunk {
                offset: 0,
                bytes: self.payload.clone(),
            }));
        }
        std::future::pending::<io::Result<Option<NativeTcpChunk>>>().await
    }
}

#[tokio::test]
async fn native_ordered_pump_holds_pending_read_until_ready() {
    let calls = Arc::new(AtomicU64::new(0));
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let source = PendingOnceOrderedSource {
        calls: calls.clone(),
        entered: entered.clone(),
        release: release.clone(),
        payload: bytes::Bytes::from_static(b"ordered-pump"),
    };
    let active = Arc::new(AtomicU64::new(0));
    let (lease, reserved) = TcpPoolSlotLease::reserve(active.clone());
    assert!(reserved);
    let (mut reader, prepared) =
        TuicNativeOrderedPumpReader::prepare_from_source(source, lease, None, None);
    let mut pump = prepared.commit(None);

    tokio::time::timeout(Duration::from_millis(200), entered.notified())
        .await
        .expect("read pump should enter the first source read");
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "the pump must hold the pending read future instead of starting another read"
    );

    release.notify_one();
    let chunk = tokio::time::timeout(Duration::from_millis(200), async {
        std::future::poll_fn(|cx| reader.poll_read_chunk(cx, 4096)).await
    })
    .await
    .expect("released ordered read should reach the native reader")
    .expect("pump read should succeed")
    .expect("released read should produce a chunk");
    assert_eq!(chunk.offset, 0);
    assert_eq!(&chunk.bytes[..], b"ordered-pump");

    drop(reader);
    tokio::time::timeout(Duration::from_millis(200), pump.settle())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        active.load(Ordering::SeqCst),
        0,
        "actual join and disposition must release the TCP pool lease"
    );
}

#[tokio::test]
async fn native_ordered_pump_reader_batches_ready_chunks_to_max_len() {
    let calls = Arc::new(AtomicU64::new(0));
    let source = SequenceOrderedSource {
        calls: calls.clone(),
        meter: None,
        chunks: vec![
            NativeTcpChunk {
                offset: 0,
                bytes: bytes::Bytes::from_static(b"ab"),
            },
            NativeTcpChunk {
                offset: 2,
                bytes: bytes::Bytes::from_static(b"cdef"),
            },
            NativeTcpChunk {
                offset: 6,
                bytes: bytes::Bytes::from_static(b"gh"),
            },
        ],
    };
    let active = Arc::new(AtomicU64::new(0));
    let (lease, reserved) = TcpPoolSlotLease::reserve(active.clone());
    assert!(reserved);
    let (mut reader, prepared) =
        TuicNativeOrderedPumpReader::prepare_from_source(source, lease, None, None);
    let mut pump = prepared.commit(None);

    tokio::time::timeout(Duration::from_millis(200), async {
        while calls.load(Ordering::SeqCst) < 4 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("source should publish all ready chunks and EOF");

    let first = std::future::poll_fn(|cx| reader.poll_read_chunk(cx, 5))
        .await
        .expect("first batched read should succeed")
        .expect("first batched read should produce data");
    assert_eq!(first.offset, 0);
    assert_eq!(&first.bytes[..], b"abcde");

    let second = std::future::poll_fn(|cx| reader.poll_read_chunk(cx, 16))
        .await
        .expect("second batched read should succeed")
        .expect("second batched read should produce stashed remainder");
    assert_eq!(second.offset, 5);
    assert_eq!(&second.bytes[..], b"fgh");

    let eof = std::future::poll_fn(|cx| reader.poll_read_chunk(cx, 16))
        .await
        .expect("EOF read should succeed");
    assert!(eof.is_none(), "EOF must be delivered after stashed bytes");

    drop(reader);
    tokio::time::timeout(Duration::from_millis(200), pump.settle())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(active.load(Ordering::SeqCst), 0);
}

struct FullSource {
    calls: Arc<AtomicU64>,
    dropped: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl TuicOrderedNativeChunkSource for FullSource {
    async fn read_ordered_native_chunk(&mut self, _: usize) -> io::Result<Option<NativeTcpChunk>> {
        let index = self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(Some(NativeTcpChunk {
            offset: index * 7,
            bytes: bytes::Bytes::from_static(b"payload"),
        }))
    }
}
impl Drop for FullSource {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn full_channel_retains_producer_and_partial_reader_until_actual_join_disposal() {
    for forwarded in [0, 3] {
        let calls = Arc::new(AtomicU64::new(0));
        let dropped = Arc::new(AtomicUsize::new(0));
        let active = Arc::new(AtomicU64::new(0));
        let (lease, reserved) = TcpPoolSlotLease::reserve(active.clone());
        assert!(reserved);
        let (mut reader, prepared) = TuicNativeOrderedPumpReader::prepare_from_source(
            FullSource {
                calls: calls.clone(),
                dropped: dropped.clone(),
            },
            lease,
            None,
            None,
        );
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        assert_eq!(calls.load(Ordering::SeqCst), 0, "prepared is actually cold");
        let mut pump = prepared.commit(None);
        tokio::time::timeout(Duration::from_secs(5), async {
            while calls.load(Ordering::SeqCst) < 65 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(reader.rx.as_ref().unwrap().len(), 64);
        if forwarded != 0 {
            let chunk = std::future::poll_fn(|cx| reader.poll_read_chunk(cx, forwarded))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(&chunk.bytes[..], b"pay");
        }
        pump.stop();
        tokio::time::timeout(
            Duration::from_secs(5),
            std::future::poll_fn(|cx| {
                pump.poll_join(cx);
                if pump.joined() {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }),
        )
        .await
        .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 65);
        assert_eq!(
            active.load(Ordering::SeqCst),
            1,
            "actual join alone does not dispose custody"
        );
        assert_eq!(dropped.load(Ordering::SeqCst), 0);
        assert!(pump.receipt().is_none());
        drop(reader);
        let receipt = tokio::time::timeout(Duration::from_secs(5), pump.settle())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(receipt.sent_items, 64);
        assert_eq!(receipt.sent_bytes, 64 * 7);
        assert_eq!(receipt.forwarded_bytes, forwarded);
        assert_eq!(receipt.received_items, usize::from(forwarded != 0));
        assert_eq!(receipt.received_bytes, if forwarded == 0 { 0 } else { 7 });
        assert_eq!(
            receipt.queue_discarded_items,
            64 - usize::from(forwarded != 0)
        );
        assert_eq!(
            receipt.queue_discarded_bytes,
            receipt.queue_discarded_items * 7
        );
        assert_eq!(
            receipt.reader_discarded_bytes,
            if forwarded == 0 { 0 } else { 4 }
        );
        assert_eq!(receipt.producer_discarded_bytes, 7);
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert_eq!(active.load(Ordering::SeqCst), 0);
        assert!(!pump.quarantined());
    }
}

#[derive(Debug)]
struct OriginalIo(Arc<AtomicUsize>);
impl std::fmt::Display for OriginalIo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("original Native IO")
    }
}
impl std::error::Error for OriginalIo {}
impl Drop for OriginalIo {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
struct OriginalFailureSource {
    io: Option<io::Error>,
    panic: Option<Box<usize>>,
}
#[async_trait::async_trait]
impl TuicOrderedNativeChunkSource for OriginalFailureSource {
    async fn read_ordered_native_chunk(&mut self, _: usize) -> io::Result<Option<NativeTcpChunk>> {
        if let Some(error) = self.io.take() {
            return Err(error);
        }
        if let Some(payload) = self.panic.take() {
            std::panic::resume_unwind(payload);
        }
        std::future::pending().await
    }
}

#[tokio::test]
async fn original_io_value_order_and_text_remain_owned_after_reader_notification() {
    let drops = Arc::new(AtomicUsize::new(0));
    let error = io::Error::new(io::ErrorKind::ConnectionReset, OriginalIo(drops.clone()));
    let pointer = error
        .get_ref()
        .unwrap()
        .downcast_ref::<OriginalIo>()
        .unwrap() as *const OriginalIo as usize;
    let active = Arc::new(AtomicU64::new(0));
    let (lease, reserved) = TcpPoolSlotLease::reserve(active.clone());
    assert!(reserved);
    let (mut reader, prepared) = TuicNativeOrderedPumpReader::prepare_from_source(
        OriginalFailureSource {
            io: Some(error),
            panic: None,
        },
        lease,
        None,
        None,
    );
    let clock = Arc::new(crate::shared::errors::FailureOrder::default());
    let mut pump = prepared.commit(Some(clock.clone()));
    let notice = tokio::time::timeout(
        Duration::from_secs(5),
        std::future::poll_fn(|cx| reader.poll_read_chunk(cx, 8)),
    )
    .await
    .unwrap();
    assert!(notice.is_err());
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert_eq!(pump.error_order(), Some(0));
    let failure = pump
        .take_failure()
        .expect("original failure, not the reader notice");
    assert_eq!(failure.order, Some(0));
    let crate::upstream::NativePumpFailureValue::Io(error) = failure.into_value() else {
        panic!("original IO required");
    };
    assert_eq!(error.kind(), io::ErrorKind::ConnectionReset);
    assert_eq!(error.to_string(), "original Native IO");
    assert_eq!(
        error
            .get_ref()
            .unwrap()
            .downcast_ref::<OriginalIo>()
            .unwrap() as *const OriginalIo as usize,
        pointer
    );
    drop(reader);
    pump.settle().await.unwrap();
    assert!(pump.joined());
    assert_eq!(clock.load(Ordering::SeqCst), 1);
    assert!(pump.take_failure().is_none());
    assert_eq!(active.load(Ordering::SeqCst), 0);
    drop(error);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn original_panic_wakes_reader_and_q_still_consumes_actual_join() {
    let payload = Box::new(913usize);
    let pointer = &*payload as *const usize as usize;
    let active = Arc::new(AtomicU64::new(0));
    let (lease, reserved) = TcpPoolSlotLease::reserve(active.clone());
    assert!(reserved);
    let (mut reader, prepared) = TuicNativeOrderedPumpReader::prepare_from_source(
        OriginalFailureSource {
            io: None,
            panic: Some(payload),
        },
        lease,
        None,
        None,
    );
    let clock = Arc::new(crate::shared::errors::FailureOrder::default());
    let mut pump = prepared.commit(Some(clock.clone()));
    let notice = tokio::time::timeout(
        Duration::from_secs(5),
        std::future::poll_fn(|cx| reader.poll_read_chunk(cx, 8)),
    )
    .await
    .unwrap();
    assert!(
        notice.is_err(),
        "Q cannot become clean channel EOF or permanent Pending"
    );
    drop(reader);
    assert!(
        tokio::time::timeout(Duration::from_secs(5), pump.settle())
            .await
            .unwrap()
            .is_err()
    );
    assert!(
        pump.joined(),
        "Q still consumes the one original JoinHandle"
    );
    assert!(pump.receipt().is_none());
    assert_eq!(
        active.load(Ordering::SeqCst),
        1,
        "Q retains its original lease"
    );
    let failure = pump.take_failure().expect("original panic Box");
    assert_eq!(failure.order, Some(0));
    let crate::upstream::NativePumpFailureValue::Panic(payload) = failure.into_value() else {
        panic!("original Box required");
    };
    let payload = payload.downcast::<usize>().unwrap();
    assert_eq!(&*payload as *const usize as usize, pointer);
    assert_eq!(*payload, 913);
    assert_eq!(clock.load(Ordering::SeqCst), 1);
    assert!(pump.take_failure().is_none());
}

#[derive(Debug)]
struct DropToken(Arc<AtomicUsize>);
impl Drop for DropToken {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
struct StoppedFuture(Arc<AtomicUsize>);
impl Drop for StoppedFuture {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
struct ForcedSource {
    mode: usize,
    entered: Arc<tokio::sync::Notify>,
    future_dropped: Arc<AtomicUsize>,
    source_dropped: Arc<AtomicUsize>,
    error: Option<io::Error>,
    payload: Option<Box<DropToken>>,
}
impl Drop for ForcedSource {
    fn drop(&mut self) {
        self.source_dropped.fetch_add(1, Ordering::SeqCst);
    }
}
#[async_trait::async_trait]
impl TuicOrderedNativeChunkSource for ForcedSource {
    async fn read_ordered_native_chunk(&mut self, _: usize) -> io::Result<Option<NativeTcpChunk>> {
        let _future = StoppedFuture(self.future_dropped.clone());
        self.entered.notify_one();
        match self.mode {
            0 => Err(self.error.take().unwrap()),
            1 => std::panic::resume_unwind(self.payload.take().unwrap()),
            _ => std::future::pending().await,
        }
    }
}
#[tokio::test]
async fn forced_owner_drop_requests_stop_without_destructing_unknown_custody() {
    for mode in 0..3 {
        let entered = Arc::new(tokio::sync::Notify::new());
        let future_dropped = Arc::new(AtomicUsize::new(0));
        let source_dropped = Arc::new(AtomicUsize::new(0));
        let io_dropped = Arc::new(AtomicUsize::new(0));
        let box_dropped = Arc::new(AtomicUsize::new(0));
        let active = Arc::new(AtomicU64::new(0));
        let (lease, reserved) = TcpPoolSlotLease::reserve(active.clone());
        assert!(reserved);
        let (reader, prepared) = TuicNativeOrderedPumpReader::prepare_from_source(
            ForcedSource {
                mode,
                entered: entered.clone(),
                future_dropped: future_dropped.clone(),
                source_dropped: source_dropped.clone(),
                error: Some(io::Error::new(
                    io::ErrorKind::Other,
                    OriginalIo(io_dropped.clone()),
                )),
                payload: Some(Box::new(DropToken(box_dropped.clone()))),
            },
            lease,
            None,
            None,
        );
        let pump = prepared.commit(None);
        tokio::time::timeout(Duration::from_secs(5), entered.notified())
            .await
            .unwrap();
        assert!(pump.receipt().is_none());
        if mode == 2 {
            assert_eq!(future_dropped.load(Ordering::SeqCst), 0);
        }
        drop(pump);
        tokio::time::timeout(Duration::from_secs(5), async {
            while future_dropped.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        assert!(reader.shared.quarantined());
        assert_eq!(source_dropped.load(Ordering::SeqCst), 0);
        assert_eq!(io_dropped.load(Ordering::SeqCst), 0);
        assert_eq!(box_dropped.load(Ordering::SeqCst), 0);
        assert_eq!(
            active.load(Ordering::SeqCst),
            1,
            "unknown outcome stays charged"
        );
        drop(reader);
        println!(
            "B73 forced mode={mode}: original future ended (pending mode2 observes stop), unknown destructors0; actual join/receipt Unknown, no runtime-destruction cleanup claim"
        );
    }
}
