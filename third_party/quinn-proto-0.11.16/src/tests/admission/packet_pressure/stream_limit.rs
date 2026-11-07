//! Actual encrypted illegal stream ID after an earlier retirement in the packet.
use super::*;

#[test]
fn unadvertised_same_packet_stream_limit_is_rejected_before_retirement() {
    for (full, late_reset) in [(false, false), (true, false), (false, true), (true, true)] {
        let budget = cold_fixture_budget(2);
        let mut config = admission_endpoint_config();
        config.receive_datagram_limit(1472).unwrap();
        config.receive_backing_meter(bytes::BackingMeter::with_budget(&budget));
        let mut transport = TransportConfig::default();
        transport.max_concurrent_uni_streams(1u32.into());
        let mut client_config = client_config();
        client_config.transport_config(Arc::new(transport));
        let mut pair = Pair::new(Arc::new(config), server_config());
        let client = pair.begin_connect(client_config);
        assert!(!pair.drive_bounded());
        let server = pair.server.assert_accept();
        pair.finish_connect(client, server);
        let old = pair.server_conn_mut(server).streams().open(Dir::Uni).unwrap();
        pair.server_send(server, old).write(b"old").unwrap();
        assert!(!pair.drive_bounded());
        assert_eq!(pair.client_conn_mut(client).streams().accept(Dir::Uni), Some(old));
        assert!(pair.server_conn_mut(server).streams().open(Dir::Uni).is_none());
        pair.client_recv(client, old).stop(42u32.into()).unwrap();
        // No client transmit runs here: no new MAX_STREAMS can have been sent.
        pair.server_send(server, old).reset(42u32.into()).unwrap();
        pair.server_conn_mut(server).overgrant_sender_for_packet_probe(Dir::Uni, 2);
        let illegal = pair.server_conn_mut(server).streams().open(Dir::Uni).unwrap();
        assert_eq!(illegal.index(), 1);
        if late_reset { pair.server_send(server, illegal).reset(42u32.into()).unwrap(); }
        else { pair.server_send(server, illegal).write(b"unadvertised").unwrap(); }
        pair.server.drive_outgoing(pair.time);
        let outbound: Vec<_> = pair.server.outbound.drain(..).collect();
        let packet = outbound.into_iter().map(|(_, p)| p).find(|p| {
            let types = pair.client_conn_mut(client).packet_frame_types_for_probe(p);
            if late_reset { types.iter().filter(|t| **t == frame::FrameType::RESET_STREAM).count() == 2 }
            else { types.contains(&frame::FrameType::RESET_STREAM) && types.iter().any(|t| format!("{t}") == "STREAM") }
        }).expect("actual encrypted RESET then STREAM");
        let before = pair.client_conn_mut(client).recv_settlement_snapshot_for_probe(old);
        let spaces = pair.client_conn_mut(client).packet_spaces_snapshot_for_probe();
        let reset_count = pair.client_conn_mut(client).stats().frame_rx.reset_stream;
        pair.client.inbound.push_back((pair.time, None, BytesMut::from(packet.as_ref())));
        pair.client.drive_incoming(pair.time, pair.server.addr);
        let held = full.then(|| budget.try_reserve(budget.limit() - budget.used()).unwrap());
        pair.client.process_connection_events(client);
        assert!(matches!(pair.client_conn_mut(client).retained_error(),
            Some(ConnectionError::TransportError(e)) if e.code == TransportErrorCode::STREAM_LIMIT_ERROR),
            "unadvertised stream must not become valid because this packet first retires another stream");
        assert_eq!(pair.client_conn_mut(client).recv_settlement_snapshot_for_probe(old), before);
        assert_eq!(pair.client_conn_mut(client).packet_spaces_snapshot_for_probe(), spaces);
        assert_eq!(pair.client_conn_mut(client).stats().frame_rx.reset_stream, reset_count);
        assert!(!budget.quarantined(), "known peer protocol error is not unknown inventory");
        drop(held);
        control_records::stop(&mut pair, &[client]);
        drop(pair);
        assert_eq!(budget.used(), 0);
    }
}

#[test]
fn actual_advertised_successor_uses_original_retired_receiver_at_full() {
    let budget = cold_fixture_budget(2);
    let mut config = admission_endpoint_config();
    config.receive_datagram_limit(1472).unwrap();
    config.receive_backing_meter(bytes::BackingMeter::with_budget(&budget));
    let mut transport = TransportConfig::default();
    transport.max_concurrent_uni_streams(1u32.into());
    let mut client_config = client_config();
    client_config.transport_config(Arc::new(transport));
    let mut pair = Pair::new(Arc::new(config), server_config());
    let client = pair.begin_connect(client_config);
    assert!(!pair.drive_bounded());
    let server = pair.server.assert_accept();
    pair.finish_connect(client, server);
    let old = pair.server_conn_mut(server).streams().open(Dir::Uni).unwrap();
    pair.server_send(server, old).write(b"old").unwrap();
    assert!(!pair.drive_bounded());
    assert_eq!(pair.client_conn_mut(client).streams().accept(Dir::Uni), Some(old));
    let pointer = pair.client_conn_mut(client).recv_settlement_snapshot_for_probe(old).0;
    assert!(pair.server_conn_mut(server).streams().open(Dir::Uni).is_none());
    pair.client_recv(client, old).stop(42u32.into()).unwrap();
    // Original peer receives STOP; its application must explicitly reset.
    assert!(!pair.drive_bounded());
    assert_eq!(pair.server_send(server, old).stopped().unwrap(), Some(42u32.into()));
    pair.server_send(server, old).reset(42u32.into()).unwrap();
    assert!(!pair.drive_bounded());
    let next = pair.server_conn_mut(server).streams().open(Dir::Uni).unwrap();
    assert_eq!(next.index(), 1);
    let cached = pair.client_conn_mut(client).recv_settlement_snapshot_for_probe(next);
    assert_eq!((cached.0, cached.1), (pointer, true));
    pair.server_send(server, next).write(b"successor").unwrap();
    pair.server_send(server, next).finish().unwrap();
    pair.server.drive_outgoing(pair.time);
    for (_, p) in pair.server.outbound.drain(..) {
        pair.client.inbound.push_back((pair.time, None, BytesMut::from(p.as_ref())));
    }
    pair.client.drive_incoming(pair.time, pair.server.addr);
    // Original reused heap is already funded. Full must not reject this valid
    // successor or replace its actual Box with another receiver.
    let held = budget.try_reserve(budget.limit() - budget.used()).unwrap();
    pair.client.process_connection_events(client);
    assert!(pair.client_conn_mut(client).retained_error().is_none());
    let active = pair.client_conn_mut(client).recv_settlement_snapshot_for_probe(next);
    assert_eq!((active.0, active.1), (pointer, false));
    drop(held);
    assert_eq!(pair.client_conn_mut(client).streams().accept(Dir::Uni), Some(next));
    {
        let mut recv = pair.client_recv(client, next);
        let mut chunks = recv.read(true).unwrap();
        assert_eq!(chunks.next(1024).unwrap().unwrap().bytes.as_ref(), b"successor");
        assert!(chunks.next(1024).unwrap().is_none());
        chunks.finalize();
    }
    control_records::stop(&mut pair, &[client]);
    drop(pair);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}
