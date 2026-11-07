//! The original Quinn source must honor the producer's byte reservation.
use super::*;

async fn generated_pair(
    meter: Option<bytes::BackingMeter>,
) -> (
    quinn::Endpoint,
    quinn::Endpoint,
    quinn::Connection,
    quinn::Connection,
) {
    let identity = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let cert = rustls::pki_types::CertificateDer::from(identity.cert.der().to_vec());
    let key = rustls::pki_types::PrivateKeyDer::Pkcs8(rustls::pki_types::PrivatePkcs8KeyDer::from(
        identity.signing_key.serialize_der(),
    ));
    let mut tls = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert.clone()], key)
        .unwrap();
    tls.alpn_protocols = vec![b"h3".to_vec()];
    let server_config = quinn::ServerConfig::with_crypto(std::sync::Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(tls).unwrap(),
    ));
    let server = quinn::Endpoint::server(server_config, "127.0.0.1:0".parse().unwrap()).unwrap();
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert).unwrap();
    let mut client_tls = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    client_tls.alpn_protocols = vec![b"h3".to_vec()];
    let client_config = quinn::ClientConfig::new(std::sync::Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(client_tls).unwrap(),
    ));
    let runtime = quinn::default_runtime().unwrap();
    let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let wrapped = runtime.wrap_udp_socket(socket).unwrap();
    let mut client = match meter {
        Some(meter) => quinn::Endpoint::new_with_metered_abstract_socket(
            quinn::EndpointConfig::default(),
            None,
            wrapped,
            runtime,
            meter,
        )
        .unwrap(),
        None => quinn::Endpoint::new_with_abstract_socket(
            quinn::EndpointConfig::default(),
            None,
            wrapped,
            runtime,
        )
        .unwrap(),
    };
    client.set_default_client_config(client_config);
    let address = server.local_addr().unwrap();
    let (connection, peer) = tokio::join!(
        async { client.connect(address, "localhost").unwrap().await.unwrap() },
        async { server.accept().await.unwrap().await.unwrap() },
    );
    (server, client, connection, peer)
}

#[tokio::test]
async fn managed_source_rejects_missing_original_endpoint_authority() {
    let (server, client, connection, peer) = generated_pair(None).await;
    let (_, recv) = connection.open_bi().await.unwrap();
    let expected = bytes::BackingMeter::new();
    assert!(QuinnOrderedNativeChunkSource::new(recv, Some(&expected)).is_err());
    assert!(expected.snapshot().quarantined);
    connection.close(0u32.into(), b"done");
    peer.close(0u32.into(), b"done");
    client.close(0u32.into(), b"done");
    server.close(0u32.into(), b"done");
}

#[tokio::test]
async fn direct_source_rejects_missing_endpoint_meter_before_transfer() {
    let actual = bytes::BackingMeter::new();
    let (server, client, connection, peer) = generated_pair(Some(actual.clone())).await;
    let (_, recv) = connection.open_bi().await.unwrap();
    assert!(crate::tuic::tcp_readers::preflight_native_direct_source(&recv, None).is_err());
    assert!(actual.snapshot().quarantined);
    connection.close(0u32.into(), b"done");
    peer.close(0u32.into(), b"done");
    client.close(0u32.into(), b"done");
    server.close(0u32.into(), b"done");
}

#[tokio::test]
async fn direct_source_rejects_foreign_endpoint_meter_before_transfer() {
    let actual = bytes::BackingMeter::new();
    let expected = bytes::BackingMeter::new();
    let (server, client, connection, peer) = generated_pair(Some(actual.clone())).await;
    let (_, recv) = connection.open_bi().await.unwrap();
    assert!(
        crate::tuic::tcp_readers::preflight_native_direct_source(&recv, Some(&expected)).is_err()
    );
    assert!(actual.snapshot().quarantined);
    assert!(expected.snapshot().quarantined);
    connection.close(0u32.into(), b"done");
    peer.close(0u32.into(), b"done");
    client.close(0u32.into(), b"done");
    server.close(0u32.into(), b"done");
}

#[tokio::test]
async fn managed_source_keeps_original_meter_through_real_quinn_read() {
    let meter = bytes::BackingMeter::new();
    let (server, mut client, connection, peer) = generated_pair(Some(meter.clone())).await;
    let drain = client.take_driver_drain_handle().unwrap();
    let (mut announce, recv) = connection.open_bi().await.unwrap();
    announce.write_all(b"x").await.unwrap();
    let (mut send, _) = peer.accept_bi().await.unwrap();
    send.write_all(&vec![0x5a; 8192]).await.unwrap();
    send.finish().unwrap();
    let mut source = QuinnOrderedNativeChunkSource::new(recv, Some(&meter)).unwrap();
    let mut total = 0;
    tokio::time::timeout(Duration::from_secs(3), async {
        while let Some(chunk) = source.read_ordered_native_chunk(8192).await.unwrap() {
            assert!(
                chunk
                    .bytes
                    .backing_meter()
                    .is_some_and(|actual| meter.same_authority(&actual))
            );
            assert_eq!(chunk.offset as usize, total);
            total += chunk.bytes.len();
        }
    })
    .await
    .unwrap();
    assert_eq!(total, 8192);
    assert!(
        source.copied_batches > 0,
        "actual Quinn read must exercise copied source batch"
    );
    drop(source);
    drop(send);
    drop(announce);
    connection.close(0u32.into(), b"done");
    peer.close(0u32.into(), b"done");
    client.close(0u32.into(), b"done");
    server.close(0u32.into(), b"done");
    drop(connection);
    drop(peer);
    drop(client);
    drop(server);
    assert!(
        tokio::time::timeout(Duration::from_secs(3), drain.drained())
            .await
            .unwrap()
    );
    assert_eq!(meter.snapshot().allocations, 0);
    assert_eq!(meter.snapshot().physical_bytes, 0);
    assert!(!meter.snapshot().quarantined);
}

#[tokio::test]
async fn two_real_streams_share_one_receive_meter_until_both_backings_release() {
    let meter = bytes::BackingMeter::new();
    let (server, mut client, connection, peer) = generated_pair(Some(meter.clone())).await;
    let drain = client.take_driver_drain_handle().unwrap();
    let (mut announce_a, recv_a) = connection.open_bi().await.unwrap();
    let (mut announce_b, recv_b) = connection.open_bi().await.unwrap();
    announce_a.write_all(b"a").await.unwrap();
    announce_b.write_all(b"b").await.unwrap();
    let (mut send_a, _) = peer.accept_bi().await.unwrap();
    let (mut send_b, _) = peer.accept_bi().await.unwrap();
    send_a.write_all(&[0x61; 512]).await.unwrap();
    send_b.write_all(&[0x62; 512]).await.unwrap();
    send_a.finish().unwrap();
    send_b.finish().unwrap();
    let mut source_a = QuinnOrderedNativeChunkSource::new(recv_a, Some(&meter)).unwrap();
    let mut source_b = QuinnOrderedNativeChunkSource::new(recv_b, Some(&meter)).unwrap();
    let first = tokio::time::timeout(
        Duration::from_secs(3),
        source_a.read_ordered_native_chunk(512),
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();
    let second = tokio::time::timeout(
        Duration::from_secs(3),
        source_b.read_ordered_native_chunk(512),
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();
    assert_eq!(&first.bytes[..], &[0x61; 512]);
    assert_eq!(&second.bytes[..], &[0x62; 512]);
    assert!(
        first
            .bytes
            .backing_meter()
            .is_some_and(|actual| meter.same_authority(&actual))
    );
    assert!(
        second
            .bytes
            .backing_meter()
            .is_some_and(|actual| meter.same_authority(&actual))
    );
    let held = meter.snapshot();
    assert!(held.allocations > 0 && held.physical_bytes > 0);
    drop(first);
    assert!(
        second
            .bytes
            .backing_meter()
            .is_some_and(|actual| meter.same_authority(&actual))
    );
    drop(second);
    drop(source_a);
    drop(source_b);
    drop(send_a);
    drop(send_b);
    drop(announce_a);
    drop(announce_b);
    connection.close(0u32.into(), b"done");
    peer.close(0u32.into(), b"done");
    client.close(0u32.into(), b"done");
    server.close(0u32.into(), b"done");
    drop(connection);
    drop(peer);
    drop(client);
    drop(server);
    assert!(
        tokio::time::timeout(Duration::from_secs(3), drain.drained())
            .await
            .unwrap()
    );
    let terminal = meter.snapshot();
    assert_eq!(
        (
            terminal.allocations,
            terminal.payload_bytes,
            terminal.physical_bytes
        ),
        (0, 0, 0)
    );
    assert!(!terminal.quarantined);
}

#[tokio::test]
async fn real_reset_after_buffered_prefix_is_returned_after_that_prefix() {
    let meter = bytes::BackingMeter::new();
    let (server, client, connection, peer) = generated_pair(Some(meter.clone())).await;
    let (mut announce, recv) = connection.open_bi().await.unwrap();
    announce.write_all(b"x").await.unwrap();
    let (mut send, _) = peer.accept_bi().await.unwrap();
    send.write_all(&vec![0x6b; 2048]).await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    send.reset(quinn::VarInt::from_u32(37)).unwrap();
    let mut source = QuinnOrderedNativeChunkSource::new(recv, Some(&meter)).unwrap();
    let first = tokio::time::timeout(
        Duration::from_secs(3),
        source.read_ordered_native_chunk(8192),
    )
    .await
    .unwrap()
    .unwrap()
    .expect("buffered prefix");
    assert_eq!(first.bytes.len(), 2048);
    let error = source.read_ordered_native_chunk(8192).await.unwrap_err();
    assert!(error.to_string().contains("37"), "{error}");
    drop(first);
    drop(source);
    drop(announce);
    connection.close(0u32.into(), b"done");
    peer.close(0u32.into(), b"done");
    client.close(0u32.into(), b"done");
    server.close(0u32.into(), b"done");
}

#[tokio::test]
async fn original_quinn_source_never_exceeds_requested_read_bytes() {
    let (server, client, connection, peer) = generated_pair(None).await;
    let (mut announce, recv) = connection.open_bi().await.unwrap();
    announce.write_all(b"x").await.unwrap();
    let (mut send, _) = peer.accept_bi().await.unwrap();
    let payload = vec![0x5a; 131_072];
    send.write_all(&payload).await.unwrap();
    send.finish().unwrap();

    let mut source = QuinnOrderedNativeChunkSource::new(recv, None).unwrap();
    let mut total = 0usize;
    tokio::time::timeout(Duration::from_secs(2), async {
        while let Some(chunk) = source.read_ordered_native_chunk(4096).await.unwrap() {
            assert!(!chunk.bytes.is_empty());
            assert!(
                chunk.bytes.len() <= 4096,
                "source exceeded requested byte capacity"
            );
            assert_eq!(chunk.offset as usize, total);
            total += chunk.bytes.len();
        }
    })
    .await
    .unwrap();
    assert_eq!(total, payload.len());
    drop(source);
    drop(send);
    drop(announce);
    connection.close(0u32.into(), b"done");
    peer.close(0u32.into(), b"done");
    client.close(0u32.into(), b"done");
    server.close(0u32.into(), b"done");
}

#[tokio::test]
async fn original_ordered_source_preflight_rejects_exhausted_shared_budget_before_transfer() {
    let budget = bytes::BackingBudget::new(16 * 1024 * 1024).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let (server, mut client, connection, peer) = generated_pair(Some(meter.clone())).await;
    let drain = client.take_driver_drain_handle().unwrap();
    let (send, recv) = connection.open_bi().await.unwrap();
    let original_stream = recv.id();
    // This is the real stream's original shared authority after the handshake.
    // No source, task, reader or pool lease is transferred by this borrowed call.
    let held = budget.try_reserve(budget.limit() - budget.used()).unwrap();
    let before = budget.used();
    let preflight = preflight_native_ordered_source(&recv, Some(&meter));
    let refused = preflight.is_err();
    assert_eq!(recv.id(), original_stream);
    assert_eq!(budget.used(), before);
    drop(preflight);
    drop(held);
    drop((send, recv));
    connection.close(0u32.into(), b"done");
    peer.close(0u32.into(), b"done");
    client.close(0u32.into(), b"done");
    server.close(0u32.into(), b"done");
    drop((connection, peer, client, server));
    assert!(
        tokio::time::timeout(Duration::from_secs(3), drain.drained())
            .await
            .unwrap()
    );
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
    assert!(
        refused,
        "ordered source/pump cold inventory must be admitted before stream or lease transfer"
    );
}

#[tokio::test]
async fn real_hard_source_preflight_cancel_and_pump_eof_hold_cold_owners_until_join() {
    let budget = bytes::BackingBudget::new(16 * 1024 * 1024).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let (server, mut client, connection, peer) = generated_pair(Some(meter.clone())).await;
    let drain = client.take_driver_drain_handle().unwrap();
    let (mut announce, recv) = connection.open_bi().await.unwrap();
    let baseline = budget.used();
    let ticket = preflight_native_ordered_source(&recv, Some(&meter)).unwrap();
    let layout = super::super::cold::SourceSlots::layout().size()
        + quinn::owned_events::slot_layout::<custody::Item>(64)
            .unwrap()
            .size();
    assert_eq!(budget.used(), baseline + layout);
    drop(ticket);
    assert_eq!(budget.used(), baseline);
    assert!(!budget.quarantined());
    let (mut reader, prepared) =
        TuicNativeOrderedPumpReader::prepare(recv, lease(), None, connection.clone(), Some(&meter))
            .unwrap();
    assert_eq!(budget.used(), baseline + layout);
    let mut pump = prepared.commit(None);
    announce.write_all(b"x").await.unwrap();
    let (mut send, mut peer_recv) = peer.accept_bi().await.unwrap();
    let mut marker = [0];
    peer_recv.read_exact(&mut marker).await.unwrap();
    send.write_all(&[0x5a; 8192]).await.unwrap();
    send.finish().unwrap();
    let mut total = 0;
    tokio::time::timeout(Duration::from_secs(3), async {
        while let Some(chunk) = std::future::poll_fn(|cx| reader.poll_read_chunk(cx, 1024))
            .await
            .unwrap()
        {
            assert_eq!(chunk.offset as usize, total);
            assert!(chunk.bytes.iter().all(|byte| *byte == 0x5a));
            assert!(chunk.bytes.backing_meter().unwrap().same_authority(&meter));
            total += chunk.bytes.len();
        }
    })
    .await
    .unwrap();
    assert_eq!(total, 8192);
    assert!(budget.used() >= layout);
    drop(reader);
    let receipt = tokio::time::timeout(Duration::from_secs(3), pump.settle())
        .await
        .unwrap()
        .unwrap();
    assert!(pump.joined());
    assert_eq!(receipt.source_read_bytes, 8192);
    assert_eq!(receipt.forwarded_bytes, 8192);
    assert_eq!(
        receipt.reader_discarded_bytes
            + receipt.producer_discarded_bytes
            + receipt.queue_discarded_bytes,
        0
    );
    drop((pump, announce, send, peer_recv));
    connection.close(0u32.into(), b"done");
    peer.close(0u32.into(), b"done");
    client.close(0u32.into(), b"done");
    server.close(0u32.into(), b"done");
    drop((connection, peer, client, server));
    assert!(
        tokio::time::timeout(Duration::from_secs(3), drain.drained())
            .await
            .unwrap()
    );
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[tokio::test]
async fn mismatched_preflight_cannot_report_clean_eof_before_original_producer_starts() {
    let budget = bytes::BackingBudget::new(16 * 1024 * 1024).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let (server_a, mut client_a, conn_a, peer_a) = generated_pair(Some(meter.clone())).await;
    let drain_a = client_a.take_driver_drain_handle().unwrap();
    let (server_b, mut client_b, conn_b, peer_b) = generated_pair(None).await;
    let drain_b = client_b.take_driver_drain_handle().unwrap();
    let (send_a, recv_a) = conn_a.open_bi().await.unwrap();
    let (send_b, recv_b) = conn_b.open_bi().await.unwrap();
    let ticket = preflight_native_ordered_source(&recv_a, Some(&meter)).unwrap();
    // Caller fault injection with two real receive halves, before any pump spawn.
    let (mut reader, prepared) = TuicNativeOrderedPumpReader::prepare_preflighted(
        recv_b,
        lease(),
        None,
        conn_b.clone(),
        ticket,
    );
    let result = reader.poll_read_chunk(&mut Context::from_waker(Waker::noop()), 8);
    let shared_q = reader.shared.quarantined();
    drop((reader, prepared, send_a, recv_a, send_b));
    conn_a.close(0u32.into(), b"done");
    peer_a.close(0u32.into(), b"done");
    conn_b.close(0u32.into(), b"done");
    peer_b.close(0u32.into(), b"done");
    client_a.close(0u32.into(), b"done");
    server_a.close(0u32.into(), b"done");
    client_b.close(0u32.into(), b"done");
    server_b.close(0u32.into(), b"done");
    drop((
        conn_a, peer_a, conn_b, peer_b, client_a, server_a, client_b, server_b,
    ));
    let _ = tokio::time::timeout(Duration::from_secs(3), drain_a.drained())
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(3), drain_b.drained())
            .await
            .unwrap()
    );
    assert!(budget.quarantined() && budget.used() > 0);
    assert!(
        shared_q,
        "wrong admitted authority cannot be hidden by unmetered actual recv"
    );
    assert!(
        matches!(result, Poll::Ready(Err(_))),
        "must not report clean EOF before producer starts"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn real_hard_quinn_source_reads_acknowledged_backings_under_full() {
    let budget = bytes::BackingBudget::new(16 << 20).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let (mut server, mut client, connection, peer) = generated_pair(Some(meter.clone())).await;
    let client_drain = client.take_driver_drain_handle().unwrap();
    let server_drain = server.take_driver_drain_handle().unwrap();
    let (mut announce, recv) = connection.open_bi().await.unwrap();
    let mut source = QuinnOrderedNativeChunkSource::new(recv, Some(&meter)).unwrap();
    announce.write_all(b"x").await.unwrap();
    let (mut send, mut peer_recv) = peer.accept_bi().await.unwrap();
    peer_recv.read_exact(&mut [0; 1]).await.unwrap();
    let data: Vec<_> = (0..8192).map(|i| (i % 251) as u8).collect();
    let progress = send.progress_handle();
    send.write_all(&data).await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while progress.sample().unwrap().acknowledged_bytes != data.len() as u64 { tokio::task::yield_now().await; }
    }).await.unwrap();
    let header = bytes::BytesMut::metered_copy_layout_for_len(1).unwrap() - 1;
    let pressure = bytes::BytesMut::try_zeroed_metered(budget.limit() - budget.used() - header, &meter).unwrap();
    assert_eq!(budget.used(), budget.limit());
    let before = meter.snapshot();
    let mut received = Vec::new();
    let mut parts = Vec::new();
    let result = tokio::time::timeout(Duration::from_secs(1), async {
        while received.len() < data.len() {
            let chunk = source.read_ordered_native_chunk(8192).await?.unwrap();
            assert_eq!(chunk.offset as usize, received.len());
            assert!(chunk.bytes.len() <= 8192);
            assert!(chunk.bytes.backing_meter().unwrap().same_authority(&meter));
            assert!(chunk.bytes.allocation_identity().is_some());
            received.extend_from_slice(&chunk.bytes);
            parts.push(chunk);
        }
        Ok::<(), io::Error>(())
    }).await;
    let after = meter.snapshot();
    let non_q_at_full = !budget.quarantined();
    drop(pressure);
    if result.as_ref().is_ok_and(|r| r.is_ok()) {
        send.finish().unwrap();
        assert!(tokio::time::timeout(Duration::from_secs(1), source.read_ordered_native_chunk(8192)).await.unwrap().unwrap().is_none());
    }
    drop((source, send, announce, peer_recv, parts, progress));
    connection.close(0u32.into(), b"done"); peer.close(0u32.into(), b"done");
    client.close(0u32.into(), b"done"); server.close(0u32.into(), b"done");
    drop((connection, peer, client, server));
    assert!(tokio::time::timeout(Duration::from_secs(3), client_drain.drained()).await.unwrap());
    assert!(tokio::time::timeout(Duration::from_secs(3), server_drain.drained()).await.unwrap());
    assert!(result.as_ref().is_ok_and(|r| r.is_ok()), "real Quinn ordered read at actual Full: {result:?}");
    assert!(non_q_at_full);
    assert_eq!(before.registered_total, after.registered_total, "no replacement output backing allocated");
    assert_eq!(received, data);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}
