use super::*;
mod real_consumers;

#[tokio::test]
async fn native_permit_relay_respects_paused_read_credit() {
    let mut sockets = SocketSet::new(vec![]);
    let handle = mk_test_handle(&mut sockets);
    let chunk_len = 16 * 1024;
    let queue_capacity = 128 * 1024;
    let reader: NativeTcpReadHalf = Box::new(BurstNativeReader {
        chunks: std::collections::VecDeque::from([NativeTcpChunk {
            offset: 0,
            bytes: Bytes::from(vec![7; chunk_len]),
        }]),
        eof_after_chunks: false,
    });
    let writer_shutdown = Arc::new(AtomicBool::new(false));
    let writer: NativeTcpWriteHalf = Box::new(NoopNativeWriter {
        shutdown_called: writer_shutdown,
    });
    let (_tx, rx) = crate::client_tun::uplink::channel(8);
    let (back_tx, mut back_rx) = crate::client_tun::relay_channel::channel(8);
    let (credit_tx, credit_rx) = watch::channel(RelayReadCredit {
        paused: true,
        max_batch_bytes: 0,
    });
    let task = tokio::spawn(run_relay_native_permit(
        handle,
        94,
        NativeTcpRelayStream::new(reader, writer),
        rx,
        back_tx,
        credit_rx,
        queue_capacity,
        false,
    ));

    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(30), back_rx.recv())
            .await
            .is_err(),
        "paused native permit read credit must keep ready remote bytes unread"
    );

    credit_tx
        .send(RelayReadCredit {
            paused: false,
            max_batch_bytes: chunk_len,
        })
        .unwrap();
    match tokio::time::timeout(std::time::Duration::from_millis(200), back_rx.recv())
        .await
        .expect("remote data should arrive after native permit credit resumes")
        .expect("relay should still be alive")
    {
        (
            h,
            RelayEvent::Data {
                epoch,
                bytes,
                downstream_permit,
                ..
            },
        ) => {
            assert_eq!(h, handle);
            assert_eq!(epoch, 94);
            assert_eq!(bytes.len(), chunk_len);
            assert!(downstream_permit.is_some());
        }
        other => panic!("expected native permit data after credit resumes, got {other:?}"),
    }

    task.abort();
    let _ = task.await;
}

#[tokio::test]
async fn native_permit_relay_read_floor_services_full_quantum_from_tiny_credit() {
    let mut sockets = SocketSet::new(vec![]);
    let handle = mk_test_handle(&mut sockets);
    let floor = RELAY_REMOTE_DISPATCH_SEGMENT_MAX_BYTES;
    let reader: NativeTcpReadHalf = Box::new(BurstNativeReader {
        chunks: std::collections::VecDeque::from([NativeTcpChunk {
            offset: 0,
            bytes: Bytes::from(vec![7; floor]),
        }]),
        eof_after_chunks: false,
    });
    let writer_shutdown = Arc::new(AtomicBool::new(false));
    let writer: NativeTcpWriteHalf = Box::new(NoopNativeWriter {
        shutdown_called: writer_shutdown,
    });
    let (_tx, rx) = crate::client_tun::uplink::channel(8);
    let (back_tx, mut back_rx) = crate::client_tun::relay_channel::channel(8);
    let (_credit_tx, credit_rx) = watch::channel(RelayReadCredit {
        paused: false,
        max_batch_bytes: relay_remote_read_ack_drain_floor_bytes(1200),
    });
    let task = tokio::spawn(run_relay_native_permit(
        handle,
        96,
        NativeTcpRelayStream::new(reader, writer),
        rx,
        back_tx,
        credit_rx,
        floor,
        true,
    ));

    match tokio::time::timeout(std::time::Duration::from_millis(200), back_rx.recv())
        .await
        .expect("native read floor should keep remote reads service-sized")
        .expect("relay should still be alive")
    {
        (
            h,
            RelayEvent::Data {
                epoch,
                bytes,
                downstream_permit,
                ..
            },
        ) => {
            assert_eq!(h, handle);
            assert_eq!(epoch, 96);
            assert_eq!(
                bytes.len(),
                floor,
                "tiny read credit should be lifted to the service floor"
            );
            assert!(downstream_permit.is_some());
        }
        other => panic!("expected native floor data, got {other:?}"),
    }

    task.abort();
    let _ = task.await;
}

#[tokio::test]
async fn d6_native_permit_relay_rearms_only_after_egress_permit_release() {
    let mut sockets = SocketSet::new(vec![]);
    let handle = mk_test_handle(&mut sockets);
    let chunk_len = 64 * 1024;
    let queue_capacity = 128 * 1024;
    let chunks = (0..((queue_capacity / chunk_len) + 4))
        .map(|idx| NativeTcpChunk {
            offset: (idx * chunk_len) as u64,
            bytes: Bytes::from(vec![7; chunk_len]),
        })
        .collect();
    let reader: NativeTcpReadHalf = Box::new(BurstNativeReader {
        chunks,
        eof_after_chunks: false,
    });
    let writer_shutdown = Arc::new(AtomicBool::new(false));
    let writer: NativeTcpWriteHalf = Box::new(NoopNativeWriter {
        shutdown_called: writer_shutdown,
    });
    let (_tx, rx) = crate::client_tun::uplink::channel(8);
    let (back_tx, mut back_rx) = crate::client_tun::relay_channel::channel(256);
    let (_credit_tx, credit_rx) = watch::channel(RelayReadCredit {
        paused: false,
        max_batch_bytes: chunk_len,
    });
    let task = tokio::spawn(run_relay_native_permit(
        handle,
        95,
        NativeTcpRelayStream::new(reader, writer),
        rx,
        back_tx,
        credit_rx,
        queue_capacity,
        false,
    ));

    let mut leased_payload_bytes = 0usize;
    let mut permits = Vec::new();
    tokio::time::timeout(std::time::Duration::from_millis(500), async {
        while leased_payload_bytes < queue_capacity {
            match back_rx
                .recv()
                .await
                .expect("native permit pump should emit data")
            {
                (
                    h,
                    RelayEvent::Data {
                        epoch,
                        bytes,
                        downstream_permit,
                        ..
                    },
                ) => {
                    assert_eq!(h, handle);
                    assert_eq!(epoch, 95);
                    leased_payload_bytes = leased_payload_bytes.saturating_add(bytes.len());
                    let permit =
                        downstream_permit.expect("native D6 data must carry downstream permit");
                    assert_eq!(
                        permit.release_mode(),
                        DownstreamPermitReleaseMode::OnEgressDrain
                    );
                    permits.push(permit);
                }
                other => panic!("expected native permit data, got {other:?}"),
            }
        }
    })
    .await
    .expect("native permit pump should fill leased capacity");
    assert_eq!(leased_payload_bytes, queue_capacity);

    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(30), back_rx.recv())
            .await
            .is_err(),
        "native permit pump must stop while all bytes are leased downstream"
    );

    assert_eq!(permits[0].release(chunk_len), chunk_len);
    match tokio::time::timeout(std::time::Duration::from_millis(200), back_rx.recv())
        .await
        .expect("egress permit release should re-arm native remote reads")
        .expect("relay should remain alive")
    {
        (
            h,
            RelayEvent::Data {
                epoch,
                bytes,
                downstream_permit,
                ..
            },
        ) => {
            assert_eq!(h, handle);
            assert_eq!(epoch, 95);
            assert_eq!(bytes.len(), chunk_len);
            assert!(
                downstream_permit.is_some(),
                "newly re-armed native data must carry a downstream permit"
            );
        }
        other => panic!("expected native data after permit release, got {other:?}"),
    }

    task.abort();
    let _ = task.await;
}
