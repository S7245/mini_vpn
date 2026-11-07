use super::*;

#[test]
fn first_remote_stream_mixed_full_keeps_original_slots_then_peer_recovers() {
    recovery(false, PacketShape::TwoStreams);
}

#[test]
fn first_remote_stream_mixed_repeated_full_keeps_original_deadline() {
    deadline(PacketShape::TwoStreams);
}

#[test]
fn first_local_response_none_and_actual_cached_recv_keep_identity_on_full() {
    for (cached, events_full) in [(false, false), (true, false), (true, true)] {
        first_response(cached, events_full);
    }
}

fn read_all(pair: &mut Pair, client: bool, connection: ConnectionHandle, id: StreamId) -> Vec<u8> {
    let mut recv = if client {
        pair.client_recv(connection, id)
    } else {
        pair.server_recv(connection, id)
    };
    let mut chunks = recv.read(true).unwrap();
    let mut data = Vec::new();
    while let Some(chunk) = chunks.next(1024).unwrap() {
        data.extend_from_slice(&chunk.bytes);
    }
    chunks.finalize();
    data
}

fn first_response(cached: bool, events_full: bool) {
    let budget = cold_fixture_budget(2);
    let mut config = admission_endpoint_config();
    config.receive_datagram_limit(1472).unwrap();
    config.receive_backing_meter(bytes::BackingMeter::with_budget(&budget));
    let mut pair = Pair::new(Arc::new(config), server_config());
    let (client, server) = control_records::connect(&mut pair);
    let cached_pointer = if cached {
        let old = pair
            .client_conn_mut(client)
            .streams()
            .open(Dir::Bi)
            .unwrap();
        pair.client_send(client, old).write(b"old request").unwrap();
        pair.client_send(client, old).finish().unwrap();
        assert!(!pair.drive_bounded());
        assert_eq!(
            pair.server_conn_mut(server).streams().accept(Dir::Bi),
            Some(old)
        );
        assert_eq!(read_all(&mut pair, false, server, old), b"old request");
        pair.server_send(server, old).write(b"old reply").unwrap();
        pair.server_send(server, old).finish().unwrap();
        assert!(!pair.drive_bounded());
        let pointer = pair
            .client_conn_mut(client)
            .receive_slot_for_probe(old)
            .unwrap()
            .1;
        assert_eq!(read_all(&mut pair, true, client, old), b"old reply");
        assert!(!pair.drive_bounded());
        assert!(pair
            .client_conn_mut(client)
            .receive_slot_for_probe(old)
            .is_none());
        Some(pointer)
    } else {
        None
    };
    let id = pair
        .client_conn_mut(client)
        .streams()
        .open(Dir::Bi)
        .unwrap();
    pair.client_send(client, id).write(b"request").unwrap();
    assert!(!pair.drive_bounded());
    assert_eq!(
        pair.server_conn_mut(server).streams().accept(Dir::Bi),
        Some(id)
    );
    let slot = pair
        .client_conn_mut(client)
        .receive_slot_for_probe(id)
        .unwrap();
    assert_eq!(
        slot.0,
        u8::from(cached),
        "actual None or recycled Free instance"
    );
    if let Some(pointer) = cached_pointer {
        assert_eq!(slot.1, pointer);
    }
    if events_full {
        let filler = pair
            .client_conn_mut(client)
            .streams()
            .open(Dir::Bi)
            .unwrap();
        pair.client_send(client, filler)
            .write(b"filler request")
            .unwrap();
        assert!(!pair.drive_bounded());
        assert_eq!(
            pair.server_conn_mut(server).streams().accept(Dir::Bi),
            Some(filler)
        );
        for _ in 0..16 {
            let (len, capacity) = pair.client_conn_mut(client).stream_event_slots_for_probe();
            if len != 0 && len == capacity {
                break;
            }
            pair.server_send(server, filler).write(b"filler").unwrap();
            assert!(!pair.drive_bounded());
        }
        let (len, capacity) = pair.client_conn_mut(client).stream_event_slots_for_probe();
        assert!(
            len > 0 && len == capacity,
            "actual pending Readable events fill the original layout"
        );
        assert_eq!(
            pair.client_conn_mut(client).receive_slot_for_probe(id),
            Some(slot)
        );
    }
    // A real missing first response produces a sparse first authenticated tail.
    pair.server_send(server, id).write(b"gap").unwrap();
    pair.server.drive_outgoing(pair.time);
    assert_eq!(pair.server.outbound.drain(..).count(), 1);
    pair.server_send(server, id).write(b"tail").unwrap();
    pair.server.drive_outgoing(pair.time);
    let (_, packet) = pair.server.outbound.pop_front().unwrap();
    assert!(pair.server.outbound.is_empty());
    let before = pair
        .client_conn_mut(client)
        .packet_admission_snapshot_for_probe();
    let refused = pair
        .client_conn_mut(client)
        .stats()
        .udp_rx
        .admission_refused;
    pair.client
        .inbound
        .push_back((pair.time, None, BytesMut::from(packet.as_ref())));
    pair.client.drive_incoming(pair.time, pair.server.addr);
    let pressure = budget.try_reserve(budget.limit() - budget.used()).unwrap();
    assert!(pair.client.process_one_connection_event(client));
    if !cached || events_full {
        assert_eq!(
            pair.client_conn_mut(client)
                .packet_admission_snapshot_for_probe(),
            before
        );
        assert_eq!(
            pair.client_conn_mut(client).receive_slot_for_probe(id),
            Some(slot)
        );
        assert_eq!(
            pair.client_conn_mut(client)
                .stats()
                .udp_rx
                .admission_refused,
            refused + 1
        );
    } else {
        assert_eq!(
            pair.client_conn_mut(client).receive_slot_for_probe(id),
            Some((2, slot.1))
        );
        assert_eq!(
            pair.client_conn_mut(client)
                .stats()
                .udp_rx
                .admission_refused,
            refused
        );
        assert!(pair.client_conn_mut(client).retained_error().is_none());
    }
    assert!(!budget.quarantined());
    drop(pressure);
    // No ciphertext replay: the original peer recovers both missing responses.
    pair.server_send(server, id).finish().unwrap();
    assert!(!pair.drive_bounded());
    let opened = pair
        .client_conn_mut(client)
        .receive_slot_for_probe(id)
        .unwrap();
    assert_eq!(opened.0, 2);
    if let Some(pointer) = cached_pointer {
        assert_eq!(opened.1, pointer);
    }
    assert_eq!(read_all(&mut pair, true, client, id), b"gaptail");
    assert!(pair.server_conn_mut(server).stats().path.lost_packets > 0);
    let other = pair
        .client_conn_mut(client)
        .streams()
        .open(Dir::Bi)
        .unwrap();
    pair.client_send(client, other)
        .write(b"other request")
        .unwrap();
    assert!(!pair.drive_bounded());
    assert_eq!(
        pair.server_conn_mut(server).streams().accept(Dir::Bi),
        Some(other)
    );
    pair.server_send(server, other)
        .write(b"other response")
        .unwrap();
    assert!(!pair.drive_bounded());
    pair.server_send(server, other).finish().unwrap();
    assert!(!pair.drive_bounded());
    assert_eq!(read_all(&mut pair, true, client, other), b"other response");
    control_records::stop(&mut pair, &[client]);
    drop((packet, pair));
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}
