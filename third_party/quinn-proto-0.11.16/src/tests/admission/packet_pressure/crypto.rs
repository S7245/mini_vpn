//! First real server 1-RTT flight, including its actual post-handshake TLS bytes.
use super::*;

struct FirstFlight {
    budget: bytes::BackingBudget,
    pair: Pair,
    client: ConnectionHandle,
    server: ConnectionHandle,
    stream: StreamId,
    wire: Bytes,
}
fn first_flight() -> FirstFlight {
    first_flight_with(client_config())
}
fn first_flight_with(client_config: ClientConfig) -> FirstFlight {
    let budget = cold_fixture_budget(2);
    let mut config = admission_endpoint_config();
    config.receive_datagram_limit(1472).unwrap();
    config.receive_backing_meter(bytes::BackingMeter::with_budget(&budget));
    let mut pair = Pair::new(Arc::new(config), server_config());
    let client = pair.begin_connect(client_config);
    pair.drive_client();
    pair.server.drive(pair.time, pair.client.addr);
    let server = pair.server.assert_accept();
    for (_, bytes) in pair.server.outbound.drain(..) {
        pair.client
            .inbound
            .push_back((pair.time, None, BytesMut::from(bytes.as_ref())));
    }
    pair.client.drive_incoming(pair.time, pair.server.addr);
    pair.client.process_connection_events(client);
    assert!(!pair.client_conn_mut(client).is_handshaking());
    pair.drive_client();
    pair.server.drive_incoming(pair.time, pair.client.addr);
    pair.server.process_connection_events(server);
    assert!(!pair.server_conn_mut(server).is_handshaking());
    let stream = pair
        .server_conn_mut(server)
        .streams()
        .open(Dir::Uni)
        .unwrap();
    pair.server_send(server, stream)
        .write(b"first response")
        .unwrap();
    pair.server_send(server, stream).finish().unwrap();
    pair.server.drive_outgoing(pair.time);
    let mut selected = None;
    for (_, wire) in pair.server.outbound.drain(..).collect::<Vec<_>>() {
        let types = pair
            .client_conn_mut(client)
            .packet_frame_types_for_probe(&wire);
        if wire.len() < 1200 {
            eprintln!(
                "[N1-first-data-candidate] bytes={} frames={types:?}",
                wire.len()
            );
        }
        if types.iter().any(|t| *t == frame::FrameType::CRYPTO)
            && types.iter().any(|t| format!("{t}") == "STREAM")
        {
            assert!(selected.is_none(), "one mixed first-flight packet");
            selected = Some(wire);
        }
    }
    let wire = selected.expect("real first flight must contain CRYPTO and STREAM together");
    assert_eq!(wire[0] & packet::LONG_HEADER_FORM, 0);
    FirstFlight {
        budget,
        pair,
        client,
        server,
        stream,
        wire,
    }
}
#[test]
fn actual_first_crypto_control_and_stream_full_refuses_then_peer_recovers() {
    let FirstFlight {
        budget,
        mut pair,
        client,
        server,
        stream,
        wire,
    } = first_flight();
    let before = (
        pair.client_conn_mut(client)
            .packet_admission_snapshot_for_probe(),
        pair.client_conn_mut(client)
            .packet_spaces_snapshot_for_probe(),
    );
    pair.client.capture_inbound_packets = true;
    pair.client
        .inbound
        .push_back((pair.time, None, BytesMut::from(wire.as_ref())));
    pair.client.drive_incoming(pair.time, pair.server.addr);
    let frames: Vec<_> = frame::Iter::new(Bytes::copy_from_slice(
        pair.client.captured_packets.last().unwrap(),
    ))
    .unwrap()
    .map(Result::unwrap)
    .collect();
    eprintln!(
        "[N1-first-data-wire] {:?}",
        frames.iter().map(|f| f.ty()).collect::<Vec<_>>()
    );
    assert!(frames.iter().any(|f| matches!(f, frame::Frame::Crypto(_))));
    assert!(frames.iter().any(|f| matches!(f, frame::Frame::Stream(_))));
    let pressure = budget.try_reserve(budget.limit() - budget.used()).unwrap();
    assert!(pair.client.process_one_connection_event(client));
    assert_eq!(
        (
            pair.client_conn_mut(client)
                .packet_admission_snapshot_for_probe(),
            pair.client_conn_mut(client)
                .packet_spaces_snapshot_for_probe()
        ),
        before
    );
    assert!(pair.client_conn_mut(client).retained_error().is_none());
    assert!(!budget.quarantined());
    drop(pressure);
    assert!(!pair.drive_bounded());
    assert_eq!(
        pair.client_conn_mut(client).streams().accept(Dir::Uni),
        Some(stream)
    );
    let mut recv = pair.client_recv(client, stream);
    let mut chunks = recv.read(true).unwrap();
    let mut data = Vec::new();
    while let Some(chunk) = chunks.next(1024).unwrap() {
        data.extend_from_slice(&chunk.bytes);
    }
    chunks.finalize();
    assert_eq!(data, b"first response");
    assert!(pair.server_conn_mut(server).stats().path.lost_packets > 0);
    let other = pair
        .server_conn_mut(server)
        .streams()
        .open(Dir::Uni)
        .unwrap();
    pair.server_send(server, other)
        .write(b"independent")
        .unwrap();
    pair.server_send(server, other).finish().unwrap();
    assert!(!pair.drive_bounded());
    assert_eq!(
        pair.client_conn_mut(client).streams().accept(Dir::Uni),
        Some(other)
    );
    let mut recv = pair.client_recv(client, other);
    let mut chunks = recv.read(true).unwrap();
    assert_eq!(
        chunks.next(1024).unwrap().unwrap().bytes.as_ref(),
        b"independent"
    );
    assert!(chunks.next(1024).unwrap().is_none());
    chunks.finalize();
    control_records::stop(&mut pair, &[client]);
    drop((wire, pair));
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn first_crypto_full_keeps_original_idle_deadline_until_shutdown() {
    let FirstFlight {
        budget,
        mut pair,
        client,
        server,
        wire,
        ..
    } = first_flight();
    let start = pair.time;
    let deadline = pair
        .client_conn_mut(client)
        .idle_deadline_for_packet_probe()
        .unwrap();
    let before = (
        pair.client_conn_mut(client)
            .packet_admission_snapshot_for_probe(),
        pair.client_conn_mut(client)
            .packet_spaces_snapshot_for_probe(),
    );
    let refused = pair
        .client_conn_mut(client)
        .stats()
        .udp_rx
        .admission_refused;
    for attempt in 0..8 {
        pair.time = start + (deadline - start) * attempt / 8;
        pair.client
            .inbound
            .push_back((pair.time, None, BytesMut::from(wire.as_ref())));
        pair.client.drive_incoming(pair.time, pair.server.addr);
        let pressure = budget.try_reserve(budget.limit() - budget.used()).unwrap();
        assert!(pair.client.process_one_connection_event(client));
        assert_eq!(
            (
                pair.client_conn_mut(client)
                    .packet_admission_snapshot_for_probe(),
                pair.client_conn_mut(client)
                    .packet_spaces_snapshot_for_probe()
            ),
            before
        );
        assert_eq!(
            pair.client_conn_mut(client)
                .idle_deadline_for_packet_probe(),
            Some(deadline)
        );
        assert_eq!(
            pair.client_conn_mut(client)
                .stats()
                .udp_rx
                .admission_refused,
            refused + u64::from(attempt) + 1
        );
        drop(pressure);
    }
    pair.client_conn_mut(client).handle_timeout(deadline);
    assert_eq!(
        pair.client_conn_mut(client).retained_error(),
        Some(ConnectionError::TimedOut)
    );
    let server_deadline = pair
        .server_conn_mut(server)
        .idle_deadline_for_packet_probe()
        .unwrap();
    pair.time = deadline.max(server_deadline);
    let now = pair.time;
    pair.server_conn_mut(server).handle_timeout(now);
    control_records::stop(&mut pair, &[client]);
    drop((pair, wire));
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn first_crypto_full_never_calls_provider_and_unknown_read_stops_later_frames() {
    use std::sync::atomic::Ordering;
    for mode in [5, 6] {
        let (config, armed, calls) = keys::fault_config();
        let store = config.token_store.clone();
        let FirstFlight {
            budget,
            mut pair,
            client,
            wire,
            ..
        } = first_flight_with(config);
        let meter = bytes::BackingMeter::with_budget(&budget);
        armed.store(mode, Ordering::Relaxed);
        let before = (
            pair.client_conn_mut(client)
                .packet_admission_snapshot_for_probe(),
            pair.client_conn_mut(client)
                .packet_spaces_snapshot_for_probe(),
        );
        pair.client
            .inbound
            .push_back((pair.time, None, BytesMut::from(wire.as_ref())));
        pair.client.drive_incoming(pair.time, pair.server.addr);
        let held = budget.try_reserve(budget.limit() - budget.used()).unwrap();
        assert!(pair.client.process_one_connection_event(client));
        assert_eq!(calls.load(Ordering::Relaxed), 0);
        assert_eq!(
            (
                pair.client_conn_mut(client)
                    .packet_admission_snapshot_for_probe(),
                pair.client_conn_mut(client)
                    .packet_spaces_snapshot_for_probe()
            ),
            before
        );
        assert!(store.take_metered("localhost", &meter).unwrap().is_none());
        drop(held);
        pair.client
            .inbound
            .push_back((pair.time, None, BytesMut::from(wire.as_ref())));
        pair.client.drive_incoming(pair.time, pair.server.addr);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            pair.client.process_one_connection_event(client)
        }));
        assert_eq!(result.is_err(), mode == 6);
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert!(budget.quarantined() && budget.used() > 0);
        let conn = pair.client_conn_mut(client);
        assert_eq!(conn.stats().frame_rx.stream, 0);
        assert_eq!(conn.stats().frame_rx.new_token, 0);
        assert_eq!(conn.stats().frame_rx.retire_connection_id, 0);
        if mode == 5 {
            let first = conn.retained_error().unwrap();
            assert!(
                matches!(&first, ConnectionError::TransportError(e) if e.reason == "fault after actual TLS input")
            );
            let now = pair.time;
            pair.client_conn_mut(client)
                .close(now, 0u32.into(), Bytes::new());
            assert_eq!(pair.client_conn_mut(client).retained_error(), Some(first));
        }
        let retained = super::super::send_buffer_inventory::retained_layouts(&pair);
        drop((wire, pair, store));
        retained.assert_quarantined(&budget);
    }
}
