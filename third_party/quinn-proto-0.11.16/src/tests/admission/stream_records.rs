//! Actual encrypted STREAM metadata pressure, with original ACK/PTO and UDP.
use super::{control_records::stop, sent_gso::Receipts, *};
use std::sync::Mutex;

fn pair(
    capacity: usize,
) -> (
    Pair,
    bytes::BackingBudget,
    Receipts,
    ConnectionHandle,
    ConnectionHandle,
) {
    let mut config = admission_endpoint_config();
    config.receive_datagram_limit(1472).unwrap();
    config.receive_stream_record_limit(capacity).unwrap();
    config.receive_control_record_limit(4).unwrap();
    let budget = bytes::BackingBudget::new(
        crate::connection::ColdResources::layout_bytes(&config, None).unwrap() * 2
            + Endpoint::native_registration_layout_bytes()
            + 64 * 1024,
    )
    .unwrap();
    config.receive_backing_meter(bytes::BackingMeter::with_budget(&budget));
    let mut server = server_config();
    Arc::get_mut(&mut server.transport)
        .unwrap()
        .mtu_discovery_config(None);
    let receipts = Receipts(Arc::new(Mutex::new(Vec::new())));
    let mut client = client_config_with_deterministic_pns();
    Arc::get_mut(&mut client.transport)
        .unwrap()
        .mtu_discovery_config(None)
        .congestion_controller_factory(Arc::new(receipts.clone()));
    let mut pair = Pair::new(Arc::new(config), server);
    let ch = pair.begin_connect(client);
    assert!(!pair.drive_bounded());
    let peer = pair.server.assert_accept();
    pair.finish_connect(ch, peer);
    assert!(pair.client_conn_mut(ch).retained_error().is_none());
    assert!(pair.server_conn_mut(peer).retained_error().is_none());
    (pair, budget, receipts, ch, peer)
}
fn stock(pair: &mut Pair, ch: ConnectionHandle, capacity: usize, free: usize) {
    assert_eq!(
        pair.client_conn_mut(ch).stream_record_stock_for_probe(),
        (capacity, free, capacity - free, capacity - free)
    );
}
fn receive(pair: &mut Pair, peer: ConnectionHandle, id: StreamId, expected: &[u8]) {
    let mut recv = pair.server_recv(peer, id);
    let mut chunks = recv.read(true).unwrap();
    let mut bytes = Vec::new();
    while let Some(chunk) = chunks.next(8192).unwrap() {
        bytes.extend_from_slice(&chunk.bytes);
    }
    chunks.finalize();
    assert_eq!(bytes, expected);
}

#[test]
fn two_stream_records_preserve_prefix_and_udp_control_then_real_ack_resumes_fin() {
    let (mut pair, budget, receipts, ch, peer) = pair(2);
    let mut streams = Vec::new();
    for byte in [0x31, 0x52, 0x73] {
        let id = pair.client_conn_mut(ch).streams().open(Dir::Uni).unwrap();
        pair.client_send(ch, id).write(&[byte; 64]).unwrap();
        pair.client_send(ch, id).finish().unwrap();
        streams.push((id, byte));
    }
    receipts.0.lock().unwrap().clear();
    let pn = pair.client_conn_mut(ch).next_data_number_for_record_probe();
    let now = pair.time;
    let mut wire = Vec::with_capacity(64 * 1472);
    let tx = pair
        .client_conn_mut(ch)
        .poll_transmit(now, 16, &mut wire)
        .unwrap();
    let next = pair.client_conn_mut(ch).next_data_number_for_record_probe();
    assert_eq!(next, pn + 1);
    assert_eq!(pair.client_conn_mut(ch).stats().frame_tx.stream, 2);
    assert_eq!(*receipts.0.lock().unwrap(), [(tx.size as u64, next - 1)]);
    stock(&mut pair, ch, 2, 0);
    let waiting = streams
        .iter()
        .find_map(|(id, _)| {
            (pair.client_conn_mut(ch).stream_encoding_for_probe(*id) == Some((0, true)))
                .then_some(*id)
        })
        .unwrap();
    let prefix = (tx, Bytes::from(wire));
    let mut empty = Vec::new();
    assert!(pair
        .client_conn_mut(ch)
        .poll_transmit(now, 16, &mut empty)
        .is_none());
    assert!(empty.is_empty());
    assert_eq!(
        pair.client_conn_mut(ch).next_data_number_for_record_probe(),
        next
    );
    assert_eq!(
        pair.client_conn_mut(ch).stream_encoding_for_probe(waiting),
        Some((0, true))
    );
    assert_eq!(receipts.0.lock().unwrap().len(), 1);

    // Ordinary Full cannot consume the separate control class or suppress UDP.
    let reset = pair.client_conn_mut(ch).streams().open(Dir::Uni).unwrap();
    pair.client_send(ch, reset).reset(17u32.into()).unwrap();
    pair.client_datagrams(ch)
        .send(BytesMut::try_copy_from_slice_metered(b"UDP while STREAM full", &bytes::BackingMeter::with_budget(&budget)).unwrap().freeze(), true)
        .unwrap();
    let mut wire = Vec::with_capacity(4096);
    let tx = pair
        .client_conn_mut(ch)
        .poll_transmit(now, 16, &mut wire)
        .unwrap();
    assert_eq!(pair.client_conn_mut(ch).stats().frame_tx.stream, 2);
    assert_eq!(pair.client_conn_mut(ch).stats().frame_tx.reset_stream, 1);
    for packet in wire[..tx.size].chunks(tx.segment_size.unwrap_or(tx.size)) {
        pair.server
            .inbound
            .push_back((now, None, BytesMut::from(packet)));
    }
    pair.server.drive_incoming(now, pair.client.addr);
    pair.server.process_connection_events(peer);
    assert_eq!(
        pair.server_datagrams(peer).recv().unwrap(),
        b"UDP while STREAM full"[..]
    );
    stock(&mut pair, ch, 2, 0); // No ACK has reached the original sender yet.
    pair.client.outbound.push_back(prefix);
    assert!(!pair.drive_bounded());
    for (id, byte) in streams {
        receive(&mut pair, peer, id, &[byte; 64]);
    }
    stock(&mut pair, ch, 2, 2);
    assert!(pair.client_conn_mut(ch).retained_error().is_none());
    stop(&mut pair, &[ch]);
    drop(pair);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn full_stream_stock_keeps_original_pto_probe_and_real_loss_releases_credit() {
    let (mut pair, budget, _, ch, peer) = pair(1);
    let id = pair.client_conn_mut(ch).streams().open(Dir::Uni).unwrap();
    let payload: Vec<_> = (0..4096).map(|n| (n % 251) as u8).collect();
    pair.client_send(ch, id).write(&payload).unwrap();
    pair.client_send(ch, id).finish().unwrap();
    pair.client.drive_outgoing(pair.time);
    assert!(!pair.client.outbound.is_empty());
    pair.client.outbound.clear(); // Real encrypted tail packet loss.
    stock(&mut pair, ch, 1, 0);
    let frames = pair.client_conn_mut(ch).stats().frame_tx;
    let progress = pair
        .client_conn_mut(ch)
        .stream_encoding_for_probe(id)
        .unwrap();
    pair.time = pair.client_conn_mut(ch).loss_detection_deadline().unwrap();
    pair.client.drive_outgoing(pair.time);
    let after = pair.client_conn_mut(ch).stats().frame_tx;
    assert_eq!(after.immediate_ack - frames.immediate_ack, 2);
    assert_eq!(after.ping, frames.ping);
    assert_eq!(pair.client.outbound.len(), 2);
    assert_eq!(after.stream, frames.stream);
    assert_eq!(
        pair.client_conn_mut(ch).stream_encoding_for_probe(id),
        Some(progress)
    );
    stock(&mut pair, ch, 1, 0);
    assert!(!pair.drive_bounded());
    assert!(pair.client_conn_mut(ch).stats().path.lost_packets > 0);
    receive(&mut pair, peer, id, &payload);
    stock(&mut pair, ch, 1, 1);
    stop(&mut pair, &[ch]);
    drop(pair);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn original_close_drains_with_unacked_stream_credit_still_owned() {
    let (mut pair, budget, _, ch, peer) = pair(1);
    let id = pair.client_conn_mut(ch).streams().open(Dir::Uni).unwrap();
    pair.client_send(ch, id).write(&[0x71; 4096]).unwrap();
    pair.client_send(ch, id).finish().unwrap();
    pair.client.drive_outgoing(pair.time);
    assert!(!pair.client.outbound.is_empty());
    pair.client.outbound.clear(); // Peer cannot ACK this real encrypted packet.
    stock(&mut pair, ch, 1, 0);
    assert_eq!(pair.server_conn_mut(peer).stats().frame_rx.stream, 0);
    assert!(pair.client_conn_mut(ch).retained_error().is_none());
    assert!(budget.used() > 0);
    stop(&mut pair, &[ch]); // Original close/PTO, both actual Proto Drained events.
                            // Proto's explicit local close changes state; only the Quinn facade reports
                            // LocallyClosed. Do not invent a protocol failure for a normal shutdown.
    assert!(pair.client_conn_mut(ch).retained_error().is_none());
    assert!(pair.client_conn_mut(ch).is_drained());
    assert!(pair.server_conn_mut(peer).is_drained());
    // The fixture still owns Connection objects after their Endpoint maps retire.
    // Only destroying these last real owners proves the cold layout is released.
    drop(pair);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn queued_stream_quarantine_retains_first_error_before_any_pn_or_fin_change() {
    let (mut pair, budget, _, ch, _) = pair(1);
    let id = pair.client_conn_mut(ch).streams().open(Dir::Uni).unwrap();
    pair.client_send(ch, id).write(b"retained").unwrap();
    pair.client_send(ch, id).finish().unwrap();
    let now = pair.time;
    let retained = send_buffer_inventory::retained_layouts(&pair);
    let conn = pair.client_conn_mut(ch);
    let pn = conn.next_data_number_for_record_probe();
    let encoding = conn.stream_encoding_for_probe(id);
    let deadline = conn.original_close_deadline_for_stream_probe(now);
    conn.quarantine_event_inventory(); // Explicit shared authority fault, not natural Full.
    let mut wire = Vec::new();
    assert!(conn.poll_transmit(now, 16, &mut wire).is_none());
    assert!(wire.is_empty());
    assert_eq!(conn.next_data_number_for_record_probe(), pn);
    assert_eq!(conn.stream_encoding_for_probe(id), encoding);
    let first = conn
        .retained_error()
        .expect("same Root must retain authority failure");
    assert!(
        matches!(&first, ConnectionError::TransportError(e) if e.code == TransportErrorCode::INTERNAL_ERROR)
    );
    assert_eq!(conn.poll_timeout(), Some(deadline));
    conn.close(now, 0u32.into(), Bytes::new());
    assert_eq!(conn.retained_error(), Some(first));
    drop(pair);
    retained.assert_quarantined(&budget);
    assert!(budget.quarantined());
}
