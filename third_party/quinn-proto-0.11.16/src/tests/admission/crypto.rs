//! Complete original Connection CRYPTO transaction; actual TLS and wire.
use super::*;

#[test]
fn original_client_hello_is_metered_before_endpoint_publication() {
    let budget = cold_fixture_budget(2);
    let mut config = admission_endpoint_config();
    config.receive_datagram_limit(1472).unwrap();
    config.receive_backing_meter(bytes::BackingMeter::with_budget(&budget));
    let mut pair = Pair::new(Arc::new(config), server_config());
    let client = pair.begin_connect(client_config());
    let (frames, metered, bytes) = pair
        .client_conn_mut(client)
        .crypto_pending_backings_for_probe();
    assert!(frames > 0 && bytes > 0);
    assert_eq!(
        metered, frames,
        "original constructor output must already own its hard charge"
    );
    assert!(!pair.drive_bounded());
    let server = pair.server.assert_accept();
    pair.finish_connect(client, server);
    let client_state = pair.client_conn_mut(client).crypto_snapshot_for_probe();
    let server_state = pair.server_conn_mut(server).crypto_snapshot_for_probe();
    assert_eq!(client_state.barriers[1], 2);
    assert_eq!(server_state.barriers[1], 2);
    assert!(
        client_state.barriers[2] > 0,
        "actual client key-only barrier"
    );
    for state in [client_state, server_state] {
        assert_eq!(state.free, state.capacity);
        assert_eq!((state.stored, state.pending, state.sent), (0, 0, 0));
        assert!(state.hello.is_none());
    }
    control_records::stop(&mut pair, &[client]);
    drop(pair);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn constructor_crypto_full_has_no_route_or_ownerless_backing() {
    let mut config = admission_endpoint_config();
    config.receive_crypto_record_limit(1).unwrap();
    config.receive_datagram_limit(1472).unwrap();
    let layout = crate::connection::ColdResources::layout_bytes(&config, None).unwrap();
    let budget = bytes::BackingBudget::new(layout * 2 + 64 * 1024).unwrap();
    config.receive_backing_meter(bytes::BackingMeter::with_budget(&budget));
    let mut pair = Pair::new(Arc::new(config), server_config());
    let before = budget.used();
    let result =
        pair.client
            .endpoint
            .connect(pair.time, client_config(), pair.server.addr, "localhost");
    assert!(matches!(result, Err(ConnectError::CryptoOutput(_))));
    assert_eq!(pair.client.endpoint.open_connections(), 0);
    assert_eq!(pair.client.endpoint.known_cids(), 0);
    assert_eq!(budget.used(), before);
    assert!(!budget.quarantined());
    drop(pair);
    assert_eq!(budget.used(), 0);
}

#[test]
fn crypto_split_admission_full_emits_only_close_and_keeps_original_flight() {
    let mut config = admission_endpoint_config();
    config.receive_crypto_record_limit(3).unwrap();
    config.receive_datagram_limit(1472).unwrap();
    let layout = crate::connection::ColdResources::layout_bytes(&config, None).unwrap();
    let budget = bytes::BackingBudget::new(layout * 2 + 64 * 1024).unwrap();
    config.receive_backing_meter(bytes::BackingMeter::with_budget(&budget));
    let mut pair = Pair::new(Arc::new(config), server_config());
    let config = ClientConfig::new(Arc::new(client_crypto_with_alpn(vec![vec![b'a'; 240]; 20])));
    let client = pair.begin_connect(config);
    assert!(
        pair.client_conn_mut(client)
            .crypto_pending_backings_for_probe()
            .2
            > 1472
    );
    let mut prefix = Vec::with_capacity(1472);
    let now = pair.time;
    let tx = pair
        .client_conn_mut(client)
        .poll_transmit(now, 1, &mut prefix)
        .unwrap();
    assert_eq!(tx.size, prefix.len());
    assert!(!prefix.is_empty());
    // This encrypted first fragment is actually in flight. Withhold it to keep
    // the original pending tail, sent prefix and Retry owner all charged.
    let prefix_flight = pair.client_conn_mut(client).bytes_in_flight();
    assert!(prefix_flight > 0);
    let before = pair.client_conn_mut(client).crypto_snapshot_for_probe();
    let crypto_sent = pair.client_conn_mut(client).stats().frame_tx.crypto;
    assert!(pair.client_conn_mut(client).retained_error().is_none());
    assert_eq!(before.free, 0); // Real sent prefix, pending tail and original Retry custody.
    let now = pair.time;
    let deadline = pair
        .client_conn_mut(client)
        .original_close_deadline_for_stream_probe(now);
    let mut wire = Vec::with_capacity(1472);
    let tx = pair
        .client_conn_mut(client)
        .poll_transmit(now, 1, &mut wire)
        .unwrap();
    assert_eq!(tx.size, wire.len());
    let after = pair.client_conn_mut(client).crypto_snapshot_for_probe();
    assert_eq!(after.pn, [before.pn[0] + 1, 0, 0]); // One original Close packet.
    assert_eq!(
        pair.client_conn_mut(client).stats().frame_tx.crypto,
        crypto_sent
    );
    assert_eq!((after.pending, after.sent), (before.pending, before.sent));
    assert_eq!(after.offsets, before.offsets);
    assert_eq!(
        pair.client_conn_mut(client).bytes_in_flight(),
        prefix_flight
    );
    assert_eq!(pair.client_conn_mut(client).poll_timeout(), Some(deadline));
    let first = pair.client_conn_mut(client).retained_error().unwrap();
    assert!(
        matches!(&first, ConnectionError::TransportError(e) if e.code == TransportErrorCode::INTERNAL_ERROR)
    );
    pair.client_conn_mut(client)
        .close(now, 0u32.into(), Bytes::new());
    assert_eq!(pair.client_conn_mut(client).retained_error(), Some(first));
    // Deliver the actual incomplete ClientHello followed by the encrypted Close.
    pair.server
        .inbound
        .push_back((now, None, BytesMut::from(prefix.as_slice())));
    pair.server
        .inbound
        .push_back((now, None, BytesMut::from(wire.as_slice())));
    pair.server.drive_incoming(now, pair.client.addr);
    let server = pair.server.assert_accept();
    pair.server.process_connection_events(server);
    assert!(matches!(pair.server_conn_mut(server).retained_error(),
        Some(ConnectionError::ConnectionClosed(e)) if e.error_code == TransportErrorCode::INTERNAL_ERROR));
    assert!(!pair.drive_bounded());
    assert_eq!(pair.client.endpoint.open_connections(), 0);
    assert_eq!(pair.client.endpoint.known_cids(), 0);
    drop(pair);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn crypto_real_retry_reuses_original_backing_then_fragment_loss_and_ack_settle() {
    let budget = cold_fixture_budget(2);
    let mut config = admission_endpoint_config();
    config.receive_datagram_limit(1472).unwrap();
    config.receive_backing_meter(bytes::BackingMeter::with_budget(&budget));
    let alpn = vec![vec![b'a'; 240]; 20];
    let server = ServerConfig::with_crypto(Arc::new(server_crypto_with_alpn(alpn.clone())));
    let mut pair = Pair::new(Arc::new(config), server);
    pair.server.handle_incoming = Box::new(validate_incoming);
    let client = pair.begin_connect(ClientConfig::new(Arc::new(client_crypto_with_alpn(alpn))));
    let before = pair.client_conn_mut(client).crypto_snapshot_for_probe();
    let (ptr, length) = before.hello.unwrap();
    assert!(length > 1472);
    pair.drive_client();
    assert!(pair.client_conn_mut(client).stats().frame_tx.crypto > 1);
    pair.drive_server(); // Actual Retry; no server connection accepted yet.
    pair.server.assert_no_accept();
    pair.client.drive_incoming(pair.time, pair.server.addr);
    pair.client.process_connection_events(client);
    let retry = pair.client_conn_mut(client).crypto_snapshot_for_probe();
    assert_eq!(retry.initial, Some((0, ptr, length)));
    assert!(retry.hello.is_none());
    assert_eq!(retry.barriers, before.barriers); // TLS was not called to recreate ClientHello.
    assert_eq!(retry.sent, 0);
    assert_eq!(retry.pending, 1);
    assert_eq!(pair.client_conn_mut(client).bytes_in_flight(), 0);
    pair.client.drive_outgoing(pair.time);
    assert!(!pair.client.outbound.is_empty());
    pair.client.outbound.clear(); // Lose real encrypted fragments after Retry.
    let sent = pair.client_conn_mut(client).stats().frame_tx.crypto;
    assert!(
        pair.client_conn_mut(client)
            .crypto_snapshot_for_probe()
            .sent
            > 1
    );
    pair.time = pair
        .client_conn_mut(client)
        .loss_detection_deadline()
        .unwrap();
    pair.client.drive_outgoing(pair.time);
    assert!(pair.client_conn_mut(client).stats().frame_tx.crypto > sent);
    assert!(!pair.drive_bounded());
    let server = pair.server.assert_accept();
    pair.finish_connect(client, server);
    for connection in [
        pair.client.connections.get(&client).unwrap(),
        pair.server.connections.get(&server).unwrap(),
    ] {
        let state = connection.crypto_snapshot_for_probe();
        assert_eq!(state.free, state.capacity);
        assert_eq!((state.stored, state.pending, state.sent), (0, 0, 0));
        assert!(connection.retained_error().is_none());
        assert_eq!(connection.bytes_in_flight(), 0);
    }
    control_records::stop(&mut pair, &[client]);
    drop(pair);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn crypto_real_zero_rtt_acceptance_and_rejection_keep_custody() {
    for reject in [false, true] {
        let budget = cold_fixture_budget(2);
        let mut config = admission_endpoint_config();
        config.receive_datagram_limit(1472).unwrap();
        config.receive_backing_meter(bytes::BackingMeter::with_budget(&budget));
        let server = ServerConfig::with_crypto(Arc::new(server_crypto_with_alpn(vec![
            b"foo".to_vec(),
            b"bar".to_vec(),
        ])));
        let mut pair = Pair::new(Arc::new(config), server);
        let mut crypto = Arc::new(client_crypto_with_alpn(vec![b"foo".to_vec()]));
        let client = pair.begin_connect(ClientConfig::new(crypto.clone()));
        assert!(!pair.drive_bounded());
        let server = pair.server.assert_accept();
        pair.finish_connect(client, server);
        control_records::stop(&mut pair, &[client]);
        pair.client.connections.clear();
        pair.server.connections.clear();
        if reject {
            Arc::get_mut(&mut Arc::get_mut(&mut crypto).unwrap().inner)
                .unwrap()
                .alpn_protocols = vec![b"bar".to_vec()];
        }
        let client = pair.begin_connect(ClientConfig::new(crypto));
        assert!(pair.client_conn_mut(client).has_0rtt());
        let id = pair.client_streams(client).open(Dir::Uni).unwrap();
        let message = b"actual early data";
        pair.client_send(client, id).write(message).unwrap();
        pair.client.drive_outgoing(pair.time);
        assert!(pair.client_conn_mut(client).stats().frame_tx.stream > 0);
        assert!(
            pair.client_conn_mut(client)
                .crypto_snapshot_for_probe()
                .sent
                > 0
        );
        assert!(!pair.drive_bounded());
        let server = pair.server.assert_accept();
        pair.finish_connect(client, server);
        assert_eq!(pair.client_conn_mut(client).accepted_0rtt(), !reject);
        if reject {
            assert_eq!(pair.client_streams(client).open(Dir::Uni), Some(id));
        }
        let mut recv = pair.server_recv(server, id);
        let mut chunks = recv.read(false).unwrap();
        if reject {
            assert_eq!(chunks.next(usize::MAX), Err(ReadError::Blocked));
        } else {
            assert_eq!(
                chunks.next(usize::MAX).unwrap().unwrap().bytes.as_ref(),
                message
            );
        }
        let _ = chunks.finalize();
        for connection in [
            pair.client.connections.get(&client).unwrap(),
            pair.server.connections.get(&server).unwrap(),
        ] {
            let state = connection.crypto_snapshot_for_probe();
            assert_eq!(state.free, state.capacity);
            assert_eq!((state.stored, state.pending, state.sent), (0, 0, 0));
            assert!(connection.retained_error().is_none());
        }
        control_records::stop(&mut pair, &[client]);
        drop(pair);
        assert_eq!(budget.used(), 0);
        assert!(!budget.quarantined());
    }
}

#[test]
fn crypto_backing_full_rejects_before_routes_and_known_handshake_drop_releases() {
    let mut config = admission_endpoint_config();
    config.receive_datagram_limit(1472).unwrap();
    let layout = crate::connection::ColdResources::layout_bytes(&config, None).unwrap();
    let budget =
        bytes::BackingBudget::new(layout + Endpoint::native_registration_layout_bytes()).unwrap();
    config.receive_backing_meter(bytes::BackingMeter::with_budget(&budget));
    let mut pair = Pair::new(Arc::new(config), server_config());
    let before = budget.used();
    assert!(matches!(
        pair.client
            .endpoint
            .connect(pair.time, client_config(), pair.server.addr, "localhost"),
        Err(ConnectError::CryptoOutput(_))
    ));
    assert_eq!(pair.client.endpoint.open_connections(), 0);
    assert_eq!(pair.client.endpoint.known_cids(), 0);
    assert_eq!(budget.used(), before);
    drop(pair);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());

    let budget = cold_fixture_budget(2);
    let mut config = admission_endpoint_config();
    config.receive_datagram_limit(1472).unwrap();
    config.receive_backing_meter(bytes::BackingMeter::with_budget(&budget));
    let mut pair = Pair::new(Arc::new(config), server_config());
    let client = pair.begin_connect(client_config());
    pair.client.drive_outgoing(pair.time);
    assert!(
        pair.client_conn_mut(client)
            .crypto_snapshot_for_probe()
            .sent
            > 0
    );
    assert!(budget.used() > 0);
    // Unpublished attempt: bootstrap abandon and consume its actual Drained before Drop.
    pair.client_conn_mut(client).abandon();
    pair.client.drive_outgoing(pair.time);
    assert_eq!(pair.client.endpoint.open_connections(), 0);
    drop(pair);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}
