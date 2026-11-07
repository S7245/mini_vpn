use super::*;

struct Coalesced {
    budget: bytes::BackingBudget,
    pair: Pair,
    client: ConnectionHandle,
    server: ConnectionHandle,
    stream: StreamId,
    wire: Vec<u8>,
}
fn fixture(key_update: bool) -> Coalesced {
    fixture_with_client(key_update, client_config())
}
fn fixture_with_client(key_update: bool, client_config: ClientConfig) -> Coalesced {
    fixture_with_early(key_update, client_config, None)
}
fn fixture_with_early(
    key_update: bool,
    client_config: ClientConfig,
    early_count: Option<usize>,
) -> Coalesced {
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
    assert!(pair.server_conn_mut(server).is_handshaking());
    // Deliver only the real Initial ACK. Withhold Finished so the original
    // loss timer selects Handshake rather than retransmitting the Initial.
    pair.client.drive_outgoing(pair.time);
    let mut finished = Vec::new();
    for (_, bytes) in pair.client.outbound.drain(..).collect::<Vec<_>>() {
        for (space, wire) in pair
            .server_conn_mut(server)
            .packet_spaces_wire_for_probe(&bytes)
        {
            if space == Some(packet::SpaceId::Initial) {
                pair.server
                    .inbound
                    .push_back((pair.time, None, BytesMut::from(wire.as_slice())));
            } else {
                finished.push(wire);
            }
        }
    }
    assert!(!finished.is_empty());
    pair.server.drive_incoming(pair.time, pair.client.addr);
    pair.server.process_connection_events(server);
    pair.time = pair
        .server_conn_mut(server)
        .loss_detection_deadline()
        .unwrap();
    let timeout = pair.time;
    pair.server_conn_mut(server).handle_timeout(timeout);
    pair.server.drive_outgoing(pair.time);
    let mut handshake = None;
    for (_, bytes) in pair.server.outbound.drain(..).collect::<Vec<_>>() {
        if handshake.is_none() {
            handshake = pair
                .client_conn_mut(client)
                .unseen_handshake_packet_for_probe(&bytes);
        }
    }
    let handshake = handshake.expect("original server PTO emitted unseen Handshake CRYPTO");
    let handshake = if let Some(count) = early_count {
        let mut wire = Vec::new();
        for _ in 0..count {
            // Finished was sent and withheld. Ask the original writer for a
            // separate Handshake PING, polling before the overdue timer so it
            // is not combined with retransmitted Finished. No packet is forged.
            pair.client_conn_mut(client)
                .request_handshake_ping_for_probe();
            let mut ping = Vec::new();
            let now = pair.time;
            let sent = pair
                .client_conn_mut(client)
                .poll_transmit(now, 1, &mut ping)
                .unwrap();
            assert_eq!(sent.size, ping.len());
            let mut client_pings = 0;
            for (space, packet) in pair
                .server_conn_mut(server)
                .packet_spaces_wire_for_probe(&ping)
            {
                if space == Some(packet::SpaceId::Handshake) {
                    let frames = pair
                        .server_conn_mut(server)
                        .packet_frame_types_for_probe(&packet);
                    assert!(frames.contains(&frame::FrameType::PING));
                    assert!(!frames.contains(&frame::FrameType::CRYPTO));
                    pair.server.inbound.push_back((
                        pair.time,
                        None,
                        BytesMut::from(packet.as_slice()),
                    ));
                    client_pings += 1;
                }
            }
            assert_eq!(client_pings, 1);
            pair.server.drive_incoming(pair.time, pair.client.addr);
            pair.server.process_connection_events(server);
            assert!(pair.server_conn_mut(server).is_handshaking());
            pair.server_conn_mut(server)
                .request_handshake_ping_for_probe();
            pair.server.drive_outgoing(pair.time);
            let outgoing: Vec<_> = pair.server.outbound.drain(..).collect();
            let mut produced = 0;
            for (_, bytes) in outgoing {
                for (space, packet) in pair
                    .client_conn_mut(client)
                    .packet_spaces_wire_for_probe(&bytes)
                {
                    if space == Some(packet::SpaceId::Handshake) {
                        let frames = pair
                            .client_conn_mut(client)
                            .packet_frame_types_for_probe(&packet);
                        assert!(frames.contains(&frame::FrameType::PING));
                        assert!(frames.contains(&frame::FrameType::ACK));
                        assert!(frames.iter().all(|f| matches!(
                            *f,
                            frame::FrameType::PING
                                | frame::FrameType::PADDING
                                | frame::FrameType::ACK
                        )));
                        wire.extend_from_slice(&packet);
                        produced += 1;
                    }
                }
            }
            assert_eq!(produced, 1, "one original encrypted early PING per request");
        }
        wire
    } else {
        handshake
    };
    for wire in finished {
        pair.server
            .inbound
            .push_back((pair.time, None, BytesMut::from(wire.as_slice())));
    }
    pair.server.drive_incoming(pair.time, pair.client.addr);
    pair.server.process_connection_events(server);
    assert!(!pair.server_conn_mut(server).is_handshaking());
    let stream = pair
        .server_conn_mut(server)
        .streams()
        .open(Dir::Uni)
        .unwrap();
    pair.server_send(server, stream).write(b"gap").unwrap();
    pair.server.drive_outgoing(pair.time);
    assert!(!pair.server.outbound.is_empty());
    pair.server.outbound.clear(); // Lose gap and HANDSHAKE_DONE; client keeps Handshake keys.
    if key_update {
        let before = pair.server_conn_mut(server).key_phase_for_packet_probe();
        pair.server_conn_mut(server).force_key_update();
        assert_ne!(
            pair.server_conn_mut(server).key_phase_for_packet_probe(),
            before
        );
    }
    if early_count.is_some() {
        let now = pair.time;
        pair.server_conn_mut(server).rotate_local_cid(5, now);
        let conn = pair.server.connections.get_mut(&server).unwrap();
        while let Some(event) = conn.poll_endpoint_events() {
            pair.server
                .endpoint
                .handle_connection_event(server, event, conn);
        }
    }
    pair.server_send(server, stream).write(b"tail").unwrap();
    pair.server_send(server, stream).finish().unwrap();
    pair.server.drive_outgoing(pair.time);
    assert_eq!(pair.server.outbound.len(), 1);
    let (_, tail) = pair.server.outbound.pop_front().unwrap();
    assert_eq!(tail[0] & packet::LONG_HEADER_FORM, 0);
    if early_count.is_some() {
        let frames = pair.client_conn_mut(client).packet_frames_for_probe(&tail);
        assert!(frames
            .iter()
            .any(|f| matches!(f, frame::Frame::NewConnectionId(_))));
        assert!(frames.iter().any(|f| matches!(f, frame::Frame::Stream(_))));
    }
    let mut wire = handshake;
    wire.extend_from_slice(&tail);
    assert!(wire.len() <= 1472);
    Coalesced {
        budget,
        pair,
        client,
        server,
        stream,
        wire,
    }
}
#[test]
fn real_pto_handshake_then_stream_full_refuses_before_both_spaces_commit() {
    recovery(false, false);
}
#[test]
fn real_coalesced_replay_commits_both_spaces_once() {
    recovery(true, false);
}
#[test]
fn real_coalesced_key_rotation_waits_for_all_packet_resources() {
    recovery(false, true);
}
fn recovery(replay: bool, key_update: bool) {
    recovery_with_fixture(fixture(key_update), replay, 0);
}
fn recovery_with_fixture(fixture: Coalesced, replay: bool, headroom: usize) {
    let Coalesced {
        budget,
        mut pair,
        client,
        server,
        stream,
        wire,
    } = fixture;
    let mut unique_packets: Vec<_> = pair
        .client_conn_mut(client)
        .packet_spaces_wire_for_probe(&wire)
        .into_iter()
        .map(|(_, p)| p)
        .collect();
    unique_packets.sort();
    unique_packets.dedup();
    let auth_before = pair.client_conn_mut(client).authenticated_packets();
    let before = (
        pair.client_conn_mut(client)
            .packet_admission_snapshot_for_probe(),
        pair.client_conn_mut(client)
            .packet_spaces_snapshot_for_probe(),
    );
    let baseline = budget.used();
    pair.client
        .inbound
        .push_back((pair.time, None, BytesMut::from(wire.as_slice())));
    pair.client.drive_incoming(pair.time, pair.server.addr);
    let pressure = budget
        .try_reserve(budget.limit() - budget.used() - headroom)
        .unwrap();
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
    drop(pressure);
    assert_eq!(
        budget.used(),
        baseline,
        "all packet preparations and refused backing retired"
    );
    if replay {
        pair.client
            .inbound
            .push_back((pair.time, None, BytesMut::from(wire.as_slice())));
        pair.client.drive_incoming(pair.time, pair.server.addr);
        pair.client.process_connection_events(client);
        assert!(pair.client_conn_mut(client).retained_error().is_none());
        assert_eq!(
            pair.client_conn_mut(client).authenticated_packets() - auth_before,
            unique_packets.len() as u64,
            "each original PN commits once even within the same datagram"
        );
        let accepted = pair
            .client_conn_mut(client)
            .packet_spaces_snapshot_for_probe();
        let frames = pair.client_conn_mut(client).stats().frame_rx.stream;
        let authenticated = pair.client_conn_mut(client).authenticated_packets();
        pair.client
            .inbound
            .push_back((pair.time, None, BytesMut::from(wire.as_slice())));
        pair.client.drive_incoming(pair.time, pair.server.addr);
        pair.client.process_connection_events(client);
        assert_eq!(
            pair.client_conn_mut(client)
                .packet_spaces_snapshot_for_probe(),
            accepted
        );
        assert_eq!(pair.client_conn_mut(client).stats().frame_rx.stream, frames);
        assert_eq!(
            pair.client_conn_mut(client).authenticated_packets(),
            authenticated
        );
    }
    assert!(!pair.drive_bounded());
    assert!(
        pair.client_conn_mut(client).retained_error().is_none()
            && pair.server_conn_mut(server).retained_error().is_none(),
        "recovery headroom={headroom}: client={:?}, server={:?}",
        pair.client_conn_mut(client).retained_error(),
        pair.server_conn_mut(server).retained_error()
    );
    assert_eq!(
        pair.client_conn_mut(client).streams().accept(Dir::Uni),
        Some(stream)
    );
    let mut recv = pair.client_recv(client, stream);
    let mut chunks = recv.read(true).unwrap();
    let mut got = Vec::new();
    while let Some(chunk) = chunks.next(1024).unwrap() {
        got.extend_from_slice(&chunk.bytes);
    }
    chunks.finalize();
    assert_eq!(got, b"gaptail");
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
    assert!(
        pair.client_conn_mut(client).retained_error().is_none()
            && pair.server_conn_mut(server).retained_error().is_none(),
        "recovery headroom={headroom}: client={:?}, server={:?}",
        pair.client_conn_mut(client).retained_error(),
        pair.server_conn_mut(server).retained_error()
    );
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
    drop(pair);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn repeated_coalesced_full_keeps_original_deadline_in_both_key_phases() {
    for key_update in [false, true] {
        deadline(fixture(key_update));
    }
}
fn deadline(fixture: Coalesced) {
    let Coalesced {
        budget,
        mut pair,
        client,
        server,
        wire,
        ..
    } = fixture;
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
            .push_back((pair.time, None, BytesMut::from(wire.as_slice())));
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
                .stats()
                .udp_rx
                .admission_refused,
            refused + u64::from(attempt) + 1
        );
        assert_eq!(
            pair.client_conn_mut(client)
                .idle_deadline_for_packet_probe(),
            Some(deadline)
        );
        drop(pressure);
    }
    pair.time = deadline;
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
    drop(pair);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn coalesced_full_never_calls_tls_output_and_unknown_commit_retains_quarantine() {
    use std::sync::atomic::Ordering;
    for (mode, count, fault_at) in [
        (3, None, 1),
        (4, None, 1),
        (3, Some(8), 1),
        (4, Some(8), 1),
        (3, Some(8), 4),
        (4, Some(8), 8),
    ] {
        let (config, armed, calls) = keys::fault_config();
        let Coalesced {
            budget,
            mut pair,
            client,
            wire,
            ..
        } = fixture_with_early(false, config, count);
        armed.store(mode | (fault_at << 8), Ordering::Relaxed);
        let authed = pair.client_conn_mut(client).authenticated_packets();
        let cids = pair.client_conn_mut(client).remote_cid_state_for_probe();
        let phase = pair.client_conn_mut(client).key_phase_for_packet_probe();
        let before = (
            pair.client_conn_mut(client)
                .packet_admission_snapshot_for_probe(),
            pair.client_conn_mut(client)
                .packet_spaces_snapshot_for_probe(),
        );
        pair.client
            .inbound
            .push_back((pair.time, None, BytesMut::from(wire.as_slice())));
        pair.client.drive_incoming(pair.time, pair.server.addr);
        let pressure = budget.try_reserve(budget.limit() - budget.used()).unwrap();
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
        assert!(!budget.quarantined());
        drop(pressure);
        let streams = pair.client_conn_mut(client).stats().frame_rx.stream;
        pair.client
            .inbound
            .push_back((pair.time, None, BytesMut::from(wire.as_slice())));
        pair.client.drive_incoming(pair.time, pair.server.addr);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            pair.client.process_one_connection_event(client)
        }));
        assert_eq!(result.is_err(), mode == 4);
        assert_eq!(calls.load(Ordering::Relaxed), fault_at);
        assert_eq!(
            pair.client_conn_mut(client).authenticated_packets(),
            authed + fault_at as u64
        );
        assert_eq!(
            pair.client_conn_mut(client).remote_cid_state_for_probe(),
            cids
        );
        assert_eq!(
            pair.client_conn_mut(client).key_phase_for_packet_probe(),
            phase
        );
        assert!(budget.quarantined() && budget.used() > 0);
        assert_eq!(
            pair.client_conn_mut(client).stats().frame_rx.stream,
            streams,
            "unknown early output cannot commit later DATA"
        );
        if mode == 3 {
            let first = pair.client_conn_mut(client).retained_error().unwrap();
            assert!(matches!(&first, ConnectionError::TransportError(error)
                if error.reason == "prepared idle TLS output changed"));
            let now = pair.time;
            pair.client_conn_mut(client)
                .close(now, 0u32.into(), Bytes::new());
            assert_eq!(pair.client_conn_mut(client).retained_error(), Some(first));
        }
        let retained = super::super::send_buffer_inventory::retained_layouts(&pair);
        drop(pair);
        retained.assert_quarantined(&budget);
    }
}

#[path = "coalesced/stock.rs"]
mod stock;
