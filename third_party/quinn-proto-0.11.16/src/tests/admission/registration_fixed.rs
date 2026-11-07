//! Explicit contract-maximum Root boundary. No claim that the default peer,
//! which advertises five CIDs, negotiated eight or sent these synthetic requests.
use super::*;

#[test]
fn actual_nineteen_slot_eight_cid_stock_replaces_at_full_route_bound_without_allocation() {
    let budget = cold_fixture_budget(38);
    let mut config = admission_endpoint_config();
    config.receive_datagram_limit(1472).unwrap();
    config.receive_backing_meter(bytes::BackingMeter::with_budget(&budget));
    let mut endpoint = Endpoint::try_new(Arc::new(config), None, true, Some([0x83; 32])).unwrap();
    let baseline = budget.used();
    let client = client_config();
    let now = Instant::now();
    let mut connections = Vec::new();
    for _ in 0..crate::BUDGETED_CONNECTION_LIMIT {
        let (ch, mut conn) = endpoint
            .connect(
                now,
                client.clone(),
                "[::1]:4433".parse().unwrap(),
                "localhost",
            )
            .unwrap();
        let event = EndpointEvent(crate::shared::EndpointEventInner::NeedIdentifiers(now, 7), conn.registration_key());
        assert_eq!(endpoint.handle_connection_event(ch, event, &mut conn).disposition, crate::EndpointEventDisposition::Accepted);
        assert!(!conn.is_closed());
        connections.push((ch, conn));
    }
    assert_eq!(
        endpoint.assert_registration_inventory_for_probe(),
        (19, 152, 0)
    );
    let full = budget.used();
    assert!(matches!(
        endpoint.connect(
            now,
            client.clone(),
            "[::1]:4433".parse().unwrap(),
            "localhost"
        ),
        Err(ConnectError::EventCapacityUnavailable)
    ));
    assert_eq!(budget.used(), full);
    assert!(!budget.quarantined());
    let (ch, conn) = &mut connections[0];
    conn.retire_local_for_root_boundary(now, 0, 8).unwrap();
    let original_event = conn.poll_endpoint_events().unwrap();
    let mut accepted = false;
    let allocations = allocations_in(|| {
        accepted = endpoint.handle_connection_event(*ch, original_event, conn).disposition == crate::EndpointEventDisposition::Accepted;
    });
    assert_eq!(
        allocations, 0,
        "fixed Root and already-owned frame nodes cannot allocate during replacement"
    );
    assert!(accepted);
    assert!(!conn.is_closed());
    assert_eq!(
        endpoint.assert_registration_inventory_for_probe(),
        (19, 152, 0)
    );
    assert!(!endpoint.routes_local_cid_for_probe(*ch, 0));
    assert!(endpoint.routes_local_cid_for_probe(*ch, 8));
    assert!(!budget.quarantined());
    for (ch, mut conn) in connections {
        conn.abandon();
        endpoint.handle_event(ch, conn.take_drained_event().unwrap());
        drop(conn);
    }
    assert_eq!(
        endpoint.assert_registration_inventory_for_probe(),
        (0, 0, 0)
    );
    drop(client);
    assert_eq!(budget.used(), baseline);
    drop(endpoint);
    assert_eq!(budget.used(), 0);
    eprintln!("native-fixed-root: owner-boundary19x8 finalCID152 transient153 precharged512 zero-allocation-replace=true original20th-instance-refused=true genuine-unpublished-drains=19 last0 Q=false");
}

#[test]
fn encrypted_retire_frame_refusal_preserves_root_first_clock_peer_close_and_other_connection() {
    for repeat in [false, true] {
        let budget = cold_fixture_budget(6);
        let mut config = admission_endpoint_config();
        config.receive_datagram_limit(1472).unwrap();
        config.receive_backing_meter(bytes::BackingMeter::with_budget(&budget));
        let mut pair = Pair::new(Arc::new(config), server_config());
        let (client, peer) = control_records::connect(&mut pair);
        let (other, other_peer) = control_records::connect(&mut pair);
        let before = pair
            .client
            .endpoint
            .assert_registration_inventory_for_probe();
        let held = if repeat {
            Vec::new()
        } else {
            pair.client.endpoint.hold_identifier_nodes_for_probe(client)
        };
        let calls = repeat.then(|| pair.client.endpoint.repeat_current_cid_for_probe(client));
        let (_, seq) = pair.client_conn_mut(client).active_local_cid_seq();
        let now = pair.time;
        pair.server_conn_mut(peer)
            .queue_repeated_retirement_for_capacity_probe(seq, 1);
        pair.server.drive_outgoing(now);
        for (_, wire) in pair.server.outbound.drain(..) {
            pair.client
                .inbound
                .push_back((now, None, BytesMut::from(wire.as_ref())));
        }
        pair.client.drive_incoming(now, pair.server.addr);
        pair.client.process_connection_events(client);
        let expected = pair
            .client_conn_mut(client)
            .original_close_deadline_for_stream_probe(now);
        assert!(pair.client_conn_mut(client).retained_error().is_none());
        pair.client.drive_outgoing(now); // Original stamped Endpoint event and direct protocol reply.
        let first = pair.client_conn_mut(client).retained_error().unwrap();
        let expected_reason = if repeat {
            "identifier generation exhausted"
        } else {
            "identifier frame capacity unavailable"
        };
        assert!(
            matches!(&first,ConnectionError::TransportError(error) if error.code==TransportErrorCode::INTERNAL_ERROR && error.reason==expected_reason)
        );
        assert_eq!(pair.client_conn_mut(client).poll_timeout(), Some(expected));
        assert!(!pair.client_conn_mut(client).is_drained());
        assert_eq!(
            pair.client
                .endpoint
                .assert_registration_inventory_for_probe(),
            before,
            "rejected prepare cannot retire the old Root route or publish a prefix"
        );
        if let Some(calls) = calls {
            assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 32);
        }
        assert!(!budget.quarantined());
        for (_, wire) in pair.client.outbound.drain(..) {
            pair.server
                .inbound
                .push_back((now, None, BytesMut::from(wire.as_ref())));
        }
        pair.server.drive_incoming(now, pair.client.addr);
        pair.server.process_connection_events(peer);
        assert!(
            matches!(pair.server_conn_mut(peer).retained_error(),Some(ConnectionError::ConnectionClosed(close)) if close.error_code==TransportErrorCode::INTERNAL_ERROR)
        );
        let stream = pair
            .server_conn_mut(other_peer)
            .streams()
            .open(Dir::Uni)
            .unwrap();
        pair.server_send(other_peer, stream)
            .write(b"other connection progresses")
            .unwrap();
        pair.server_send(other_peer, stream).finish().unwrap();
        assert!(!pair.drive_bounded());
        let mut receive = pair.client_recv(other, stream);
        let mut chunks = receive.read(false).unwrap();
        let data = chunks.next(64).unwrap().unwrap();
        assert_eq!(data.bytes.as_ref(), b"other connection progresses");
        chunks.finalize();
        drop(receive);
        drop(data);
        assert!(pair.client_conn_mut(other).retained_error().is_none());
        assert_eq!(
            pair.client_conn_mut(client)
                .retained_error()
                .unwrap()
                .to_string(),
            first.to_string()
        );
        assert!(pair.client_conn_mut(client).is_drained());
        control_records::stop(&mut pair, &[other]);
        drop(pair);
        if !repeat {
            assert_eq!(
                budget.used(),
                crate::connection::cid_frames::Pool::layout()
                    .unwrap()
                    .size()
            );
        }
        drop(held);
        assert_eq!(budget.used(), 0);
        assert!(!budget.quarantined());
    }
}

#[test]
fn provider_panic_keeps_original_payload_and_immediate_root_quarantine() {
    struct Bomb {
        inner: Arc<dyn crate::crypto::HmacKey>,
        armed: Arc<std::sync::atomic::AtomicBool>,
        marker: Arc<()>,
    }
    impl crate::crypto::HmacKey for Bomb {
        fn signature_len(&self) -> usize {
            self.inner.signature_len()
        }
        fn sign(&self, data: &[u8], out: &mut [u8]) {
            if self.armed.load(std::sync::atomic::Ordering::Relaxed) {
                out[0] = 1;
                std::panic::panic_any(self.marker.clone());
            }
            self.inner.sign(data, out);
        }
        fn verify(&self, data: &[u8], signature: &[u8]) -> Result<(), crate::crypto::CryptoError> {
            self.inner.verify(data, signature)
        }
    }
    let budget = cold_fixture_budget(2);
    let armed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let marker = Arc::new(());
    let mut config = admission_endpoint_config();
    let inner = config.reset_key.clone();
    config.reset_key = Arc::new(Bomb {
        inner,
        armed: armed.clone(),
        marker: marker.clone(),
    });
    config.receive_datagram_limit(1472).unwrap();
    config.receive_backing_meter(bytes::BackingMeter::with_budget(&budget));
    let mut pair = Pair::new(Arc::new(config), server_config());
    let (client, _) = control_records::connect(&mut pair);
    let before = pair
        .client
        .endpoint
        .assert_registration_inventory_for_probe();
    let key = pair.client_conn_mut(client).registration_key();
    armed.store(true, std::sync::atomic::Ordering::Relaxed);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        pair.client.endpoint.handle_connection_event(
            client,
            EndpointEvent(
                crate::shared::EndpointEventInner::NeedIdentifiers(pair.time, 1),
                key,
            ),
            pair.client.connections.get_mut(&client).unwrap(),
        )
    }));
    let payload = result.unwrap_err();
    assert!(Arc::ptr_eq(
        payload.downcast_ref::<Arc<()>>().unwrap(),
        &marker
    ));
    assert_eq!(
        pair.client
            .endpoint
            .assert_registration_inventory_for_probe(),
        before
    );
    assert!(
        budget.quarantined() && budget.used() > 1,
        "unwind Q is immediate while original actors and Root still live"
    );
    drop(pair);
    assert_eq!(budget.used(), 1);
}
