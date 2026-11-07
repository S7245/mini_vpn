//! Original Connection STOP control with a retained cached Recv Box.
use super::*;

#[test]
fn stop_rejects_damaged_cached_recv_before_materialization_and_control_commit() {
    let budget = cold_fixture_budget(2);
    let mut config = admission_endpoint_config();
    config.receive_datagram_limit(1472).unwrap();
    config.receive_backing_meter(bytes::BackingMeter::with_budget(&budget));
    let mut pair = Pair::new(Arc::new(config), server_config());
    let (client, _) = control_records::connect(&mut pair);
    let id = pair.client_conn_mut(client).streams().open(Dir::Bi).unwrap();
    let held = pair.client_conn_mut(client).damaged_cached_recv_for_probe(id);
    let before = pair.client_conn_mut(client).recv_settlement_snapshot_for_probe(id);
    assert!(before.1);
    assert!(!budget.quarantined());
    assert!(pair.client_conn_mut(client).retained_error().is_none());
    assert!(pair.client_recv(client, id).stop(9u32.into()).is_err());
    assert_eq!(pair.client_conn_mut(client).recv_settlement_snapshot_for_probe(id), before);
    let first = pair.client_conn_mut(client).retained_error().expect("original Connection must own failure");
    assert!(matches!(first, ConnectionError::TransportError(ref error) if error.code == TransportErrorCode::INTERNAL_ERROR));
    assert!(budget.quarantined());
    assert!(pair.client_recv(client, id).stop(10u32.into()).is_err());
    assert_eq!(pair.client_conn_mut(client).retained_error(), Some(first));
    drop((pair, held));
    assert!(budget.used() > 0, "unknown inventory cannot normalize to clean");
}


#[test]
fn stop_retains_orphan_sparse_stock_and_quarantines_original_connection_authority() {
    let budget = cold_fixture_budget(2);
    let mut config = admission_endpoint_config();
    config.receive_datagram_limit(1472).unwrap();
    config.receive_backing_meter(bytes::BackingMeter::with_budget(&budget));
    let mut pair = Pair::new(Arc::new(config), server_config());
    let (client, _) = control_records::connect(&mut pair);
    let id = pair.client_conn_mut(client).streams().open(Dir::Bi).unwrap();
    pair.client_conn_mut(client).orphan_sparse_recv_for_probe(id);
    let before = pair.client_conn_mut(client).recv_settlement_snapshot_for_probe(id);
    assert!(!budget.quarantined());
    assert!(pair.client_recv(client, id).stop(9u32.into()).is_err());
    assert_eq!(pair.client_conn_mut(client).recv_settlement_snapshot_for_probe(id), before);
    assert!(pair.client_conn_mut(client).retained_error().is_some());
    assert!(budget.quarantined(), "original Connection must quarantine when the local owner was lost");
    drop(pair);
    assert!(budget.used() > 0);
}


#[test]
fn encrypted_empty_fin_cannot_retire_internally_consistent_foreign_recv() {
    let budget = cold_fixture_budget(2);
    let foreign = bytes::BackingBudget::new(1 << 20).unwrap();
    let foreign_meter = bytes::BackingMeter::with_budget(&foreign);
    let mut config = admission_endpoint_config();
    config.receive_datagram_limit(1472).unwrap();
    config.receive_backing_meter(bytes::BackingMeter::with_budget(&budget));
    let mut pair = Pair::new(Arc::new(config), server_config());
    let (client, server) = control_records::connect(&mut pair);
    let id = pair.client_conn_mut(client).streams().open(Dir::Bi).unwrap();
    pair.client_send(client, id).write(b"request").unwrap();
    assert!(!pair.drive_bounded());
    assert_eq!(pair.server_conn_mut(server).streams().accept(Dir::Bi), Some(id));
    pair.server_send(server, id).write(b"x").unwrap();
    assert!(!pair.drive_bounded());
    pair.client_conn_mut(client).foreign_stopped_recv_for_probe(id, &foreign_meter);
    let before = pair.client_conn_mut(client).recv_settlement_snapshot_for_probe(id);
    let frames_before = pair.client_conn_mut(client).stats().frame_rx.stream;
    pair.client.capture_inbound_packets = true;
    pair.server_send(server, id).finish().unwrap();
    // Real encrypted FIN, whose empty payload cannot supply a substitute meter.
    pair.server.drive_outgoing(pair.time);
    for (transmit, packet) in pair.server.outbound.drain(..) {
        let segment = transmit.segment_size.unwrap_or(transmit.size);
        for data in packet.chunks(segment) {
            pair.client.inbound.push_back((pair.time, None, BytesMut::from(data)));
        }
    }
    pair.client.drive_incoming(pair.time, pair.server.addr);
    pair.client.process_connection_events(client);
    assert!(pair.client.captured_packets.iter().any(|wire| frame::Iter::new(Bytes::copy_from_slice(wire)).unwrap()
        .any(|f| matches!(f, Ok(frame::Frame::Stream(s)) if s.id == id && s.fin && s.data.is_empty()))),
        "actual encrypted empty FIN must reach Connection preparation");
    assert_eq!(pair.client_conn_mut(client).stats().frame_rx.stream, frames_before,
        "foreign inventory is rejected before frame publication");
    assert!(pair.client_conn_mut(client).retained_error().is_some());
    assert!(budget.quarantined() && foreign.quarantined());
    assert_eq!(pair.client_conn_mut(client).recv_settlement_snapshot_for_probe(id), before);
    drop(pair);
    assert!(budget.used() > 0 && foreign.used() > 0);
}


#[test]
fn mismatched_next_at_does_not_mask_lost_actual_tail_as_blocked() {
    let budget = cold_fixture_budget(2);
    let mut config = admission_endpoint_config();
    config.receive_datagram_limit(1472).unwrap();
    config.receive_backing_meter(bytes::BackingMeter::with_budget(&budget));
    let mut pair = Pair::new(Arc::new(config), server_config());
    let (client, _) = control_records::connect(&mut pair);
    let id = pair.client_conn_mut(client).streams().open(Dir::Bi).unwrap();
    let held = pair.client_conn_mut(client).damaged_tail_recv_for_probe(id);
    let before = pair.client_conn_mut(client).recv_settlement_snapshot_for_probe(id);
    let mut stream = pair.client_recv(client, id);
    let mut chunks = stream.read(true).unwrap();
    // Ordered next offset is zero; mismatch must remain non-consuming but cannot
    // hide an actually missing descriptor as legitimate pressure.
    assert!(matches!(chunks.next_at(10, 1), Err(ReadError::BackingAuthorityLost)));
    chunks.finalize();
    assert_eq!(pair.client_conn_mut(client).recv_settlement_snapshot_for_probe(id), before);
    assert!(pair.client_conn_mut(client).retained_error().is_some());
    assert!(budget.quarantined());
    drop((pair, held));
    assert!(budget.used() > 0);
}

