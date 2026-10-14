use super::*;

#[tokio::test]
async fn relay_remote_ready_burst_forwards_immediate_chunks() {
    let mut sockets = SocketSet::new(vec![]);
    let handle = mk_test_handle(&mut sockets);
    let stream: RelayStream = Box::new(BurstReadableStream {
        shutdown_called: Arc::new(AtomicBool::new(false)),
        chunks: std::collections::VecDeque::from([
            b"one".to_vec(),
            b"two".to_vec(),
            b"three".to_vec(),
        ]),
        eof_after_chunks: false,
    });
    let (mut remote_reader, _remote_writer) = tokio::io::split(stream);
    let (back_tx, mut back_rx) = mpsc::channel(8);
    let mut diag = RelayTaskDiag::default();
    let mut buf = [0u8; 65_536];

    let (burst, close_after_batch) = drain_ready_remote_reads(ReadyReadRequest {
        remote_reader: &mut remote_reader,
        handle,
        epoch: 77,
        initial_payload: Vec::new(),
        buf: &mut buf,
        back_tx: &back_tx,
        diag: &mut diag,
        read_credit: RelayReadCredit::default(),
        global_rx_pressure_threshold: std::time::Duration::from_millis(5),
    })
    .await
    .expect("ready burst should forward without closing");

    assert_eq!(close_after_batch, None);
    assert_eq!(burst.chunks, 3);
    assert_eq!(burst.bytes, b"onetwothree".len());
    assert_eq!(diag.remote_reads, 3);
    assert_eq!(diag.remote_to_global_rx_bytes, b"onetwothree".len() as u64);
    assert_eq!(diag.remote_batches, 1);
    assert_eq!(diag.remote_batch_bytes_max, b"onetwothree".len());
    assert_eq!(diag.remote_batch_chunks_max, 3);
    match back_rx
        .recv()
        .await
        .expect("burst payload should be queued")
    {
        (
            h,
            RelayEvent::Data {
                epoch,
                bytes,
                stream_service_window,
                downstream_permit: _,
            },
        ) => {
            assert_eq!(h, handle);
            assert_eq!(epoch, 77);
            assert_eq!(bytes, b"onetwothree");
            assert_eq!(
                stream_service_window.remote_read_bytes,
                b"onetwothree".len() as u64
            );
            assert_eq!(stream_service_window.remote_read_chunks, 3);
        }
        other => panic!("expected coalesced burst data, got {other:?}"),
    }
    assert!(back_rx.try_recv().is_err());
}

#[tokio::test]
async fn relay_remote_ready_burst_respects_byte_limit_without_overshoot() {
    let mut sockets = SocketSet::new(vec![]);
    let handle = mk_test_handle(&mut sockets);
    let chunks: std::collections::VecDeque<_> = std::iter::repeat_with(|| vec![7; 100_000])
        .take(8)
        .collect();
    let stream: RelayStream = Box::new(BurstReadableStream {
        shutdown_called: Arc::new(AtomicBool::new(false)),
        chunks,
        eof_after_chunks: false,
    });
    let (mut remote_reader, _remote_writer) = tokio::io::split(stream);
    let (back_tx, mut back_rx) = mpsc::channel(8);
    let mut diag = RelayTaskDiag::default();
    let mut buf = [0u8; 65_536];

    let (burst, close_after_batch) = drain_ready_remote_reads(ReadyReadRequest {
        remote_reader: &mut remote_reader,
        handle,
        epoch: 79,
        initial_payload: Vec::new(),
        buf: &mut buf,
        back_tx: &back_tx,
        diag: &mut diag,
        read_credit: RelayReadCredit::default(),
        global_rx_pressure_threshold: std::time::Duration::from_millis(5),
    })
    .await
    .expect("first ready burst should forward");

    assert_eq!(close_after_batch, None);
    assert_eq!(burst.bytes, RELAY_REMOTE_READ_BURST_MAX_BYTES);
    assert_eq!(
        diag.remote_batch_bytes_max,
        RELAY_REMOTE_DISPATCH_SEGMENT_MAX_BYTES
    );
    let mut first_dispatch_bytes = 0;
    while first_dispatch_bytes < RELAY_REMOTE_READ_BURST_MAX_BYTES {
        match back_rx
            .recv()
            .await
            .expect("capped batch segment should be queued")
        {
            (
                h,
                RelayEvent::Data {
                    epoch,
                    bytes,
                    stream_service_window: _,
                    downstream_permit: _,
                },
            ) => {
                assert_eq!(h, handle);
                assert_eq!(epoch, 79);
                assert!(
                    bytes.len() <= RELAY_REMOTE_DISPATCH_SEGMENT_MAX_BYTES,
                    "dispatch segment must stay within local flush budget"
                );
                first_dispatch_bytes += bytes.len();
            }
            other => panic!("expected capped data segment, got {other:?}"),
        }
    }
    assert_eq!(first_dispatch_bytes, RELAY_REMOTE_READ_BURST_MAX_BYTES);

    let (next_burst, next_close) = drain_ready_remote_reads(ReadyReadRequest {
        remote_reader: &mut remote_reader,
        handle,
        epoch: 79,
        initial_payload: Vec::new(),
        buf: &mut buf,
        back_tx: &back_tx,
        diag: &mut diag,
        read_credit: RelayReadCredit::default(),
        global_rx_pressure_threshold: std::time::Duration::from_millis(5),
    })
    .await
    .expect("remaining bytes should stay readable after the cap");

    assert_eq!(next_close, None);
    assert_eq!(
        next_burst.bytes,
        800_000 - RELAY_REMOTE_READ_BURST_MAX_BYTES
    );
    let mut second_dispatch_bytes = 0;
    while second_dispatch_bytes < 800_000 - RELAY_REMOTE_READ_BURST_MAX_BYTES {
        match back_rx
            .recv()
            .await
            .expect("second remainder segment should be queued")
        {
            (
                h,
                RelayEvent::Data {
                    epoch,
                    bytes,
                    stream_service_window: _,
                    downstream_permit: _,
                },
            ) => {
                assert_eq!(h, handle);
                assert_eq!(epoch, 79);
                assert!(
                    bytes.len() <= RELAY_REMOTE_DISPATCH_SEGMENT_MAX_BYTES,
                    "remainder dispatch segment must stay within local flush budget"
                );
                second_dispatch_bytes += bytes.len();
            }
            other => panic!("expected remainder data segment, got {other:?}"),
        }
    }
    assert_eq!(
        second_dispatch_bytes,
        800_000 - RELAY_REMOTE_READ_BURST_MAX_BYTES
    );
}

#[tokio::test]
async fn relay_remote_ready_burst_tapers_when_global_rx_queue_is_busy() {
    let mut sockets = SocketSet::new(vec![]);
    let handle = mk_test_handle(&mut sockets);
    let chunks: std::collections::VecDeque<_> = std::iter::repeat_with(|| vec![5; 16 * 1024])
        .take(16)
        .collect();
    let stream: RelayStream = Box::new(BurstReadableStream {
        shutdown_called: Arc::new(AtomicBool::new(false)),
        chunks,
        eof_after_chunks: false,
    });
    let (mut remote_reader, _remote_writer) = tokio::io::split(stream);
    let (back_tx, mut back_rx) = mpsc::channel(4);
    for _ in 0..3 {
        back_tx
            .try_send((
                handle,
                RelayEvent::AckDrainHint {
                    epoch: 80,
                    gap_ms: 500,
                    remote_to_global_rx_bytes: 1,
                },
            ))
            .unwrap();
    }
    let mut diag = RelayTaskDiag::default();
    let mut buf = [0u8; 65_536];

    let (burst, close_after_batch) = drain_ready_remote_reads(ReadyReadRequest {
        remote_reader: &mut remote_reader,
        handle,
        epoch: 80,
        initial_payload: Vec::new(),
        buf: &mut buf,
        back_tx: &back_tx,
        diag: &mut diag,
        read_credit: RelayReadCredit::default(),
        global_rx_pressure_threshold: std::time::Duration::from_millis(5),
    })
    .await
    .expect("pressure-limited ready burst should forward");

    assert_eq!(close_after_batch, None);
    assert_eq!(burst.bytes, RELAY_REMOTE_READ_MIN_BATCH_BYTES);
    assert_eq!(
        diag.remote_batch_bytes_max,
        RELAY_REMOTE_READ_MIN_BATCH_BYTES
    );
    assert_eq!(
        diag.remote_batch_limit_bytes_min,
        RELAY_REMOTE_READ_MIN_BATCH_BYTES
    );
    assert_eq!(diag.remote_batch_limited, 1);

    for _ in 0..3 {
        match back_rx
            .recv()
            .await
            .expect("prefilled hint should remain ahead of data")
        {
            (_, RelayEvent::AckDrainHint { .. }) => {}
            other => panic!("expected prefilled ack hint, got {other:?}"),
        }
    }
    match back_rx
        .recv()
        .await
        .expect("limited batch should use the last free slot")
    {
        (
            h,
            RelayEvent::Data {
                epoch,
                bytes,
                stream_service_window: _,
                downstream_permit: _,
            },
        ) => {
            assert_eq!(h, handle);
            assert_eq!(epoch, 80);
            assert_eq!(bytes.len(), RELAY_REMOTE_READ_MIN_BATCH_BYTES);
        }
        other => panic!("expected pressure-limited data batch, got {other:?}"),
    }
}

#[tokio::test]
async fn relay_remote_ready_burst_stops_extra_reads_at_global_rx_critical_edge() {
    let mut sockets = SocketSet::new(vec![]);
    let handle = mk_test_handle(&mut sockets);
    let chunks: std::collections::VecDeque<_> = std::iter::repeat_with(|| vec![6; 16 * 1024])
        .take(8)
        .collect();
    let stream: RelayStream = Box::new(BurstReadableStream {
        shutdown_called: Arc::new(AtomicBool::new(false)),
        chunks,
        eof_after_chunks: false,
    });
    let (mut remote_reader, _remote_writer) = tokio::io::split(stream);
    let (back_tx, mut back_rx) = mpsc::channel(1024);
    for _ in 0..1019 {
        back_tx
            .try_send((
                handle,
                RelayEvent::AckDrainHint {
                    epoch: 81,
                    gap_ms: 500,
                    remote_to_global_rx_bytes: 1,
                },
            ))
            .unwrap();
    }
    let mut diag = RelayTaskDiag::default();
    let mut buf = [0u8; 65_536];
    let initial_payload = vec![9; 16 * 1024];

    let (burst, close_after_batch) = drain_ready_remote_reads(ReadyReadRequest {
        remote_reader: &mut remote_reader,
        handle,
        epoch: 81,
        initial_payload: initial_payload.clone(),
        buf: &mut buf,
        back_tx: &back_tx,
        diag: &mut diag,
        read_credit: RelayReadCredit::default(),
        global_rx_pressure_threshold: std::time::Duration::from_millis(5),
    })
    .await
    .expect("critical-edge initial payload should still forward");

    assert_eq!(close_after_batch, None);
    assert_eq!(
        burst.bytes,
        initial_payload.len(),
        "critical global_rx occupancy must stop extra ready reads beyond the payload already read"
    );
    assert_eq!(burst.chunks, 1);
    assert_eq!(diag.remote_batch_bytes_max, initial_payload.len());
    assert_eq!(diag.remote_batch_limit_bytes_min, initial_payload.len());
    assert_eq!(diag.remote_batch_limited, 1);

    for _ in 0..1019 {
        match back_rx
            .recv()
            .await
            .expect("prefilled hint should remain ahead of data")
        {
            (_, RelayEvent::AckDrainHint { .. }) => {}
            other => panic!("expected prefilled ack hint, got {other:?}"),
        }
    }
    match back_rx
        .recv()
        .await
        .expect("initial payload should use one remaining slot")
    {
        (
            h,
            RelayEvent::Data {
                epoch,
                bytes,
                stream_service_window: _,
                downstream_permit: _,
            },
        ) => {
            assert_eq!(h, handle);
            assert_eq!(epoch, 81);
            assert_eq!(bytes, initial_payload);
        }
        other => panic!("expected initial data batch, got {other:?}"),
    }
}
