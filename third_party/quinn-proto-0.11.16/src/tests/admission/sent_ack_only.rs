//! F0/D46 tracer: an honest peer ACK delivered behind its own data burst
//! (local receive backlog, whole datagrams, no edit/drop) must not let the
//! client's ACK-only packets pin cold Data-space sent history to Full.
use super::*;

#[test]
fn ack_only_records_do_not_pin_cold_history_behind_reordered_peer_ack() {
    const SLOTS: usize = 4;
    const REPLY: usize = 64 * 1024;
    let budget = cold_fixture_budget(1);
    let mut cold = admission_endpoint_config();
    cold.receive_backing_meter(bytes::BackingMeter::with_budget(&budget));
    cold.receive_datagram_limit(1472).unwrap();
    cold.receive_sent_history_limits(SLOTS, SLOTS).unwrap();
    let mut server_cfg = server_config();
    let mut frequency = AckFrequencyConfig::default();
    frequency.ack_eliciting_threshold(0u32.into());
    let mut transport = TransportConfig::default();
    transport
        .deterministic_packet_numbers(true)
        .mtu_discovery_config(None)
        .ack_frequency_config(Some(frequency));
    server_cfg.transport = Arc::new(transport);
    let mut client_cfg = client_config_with_deterministic_pns();
    Arc::get_mut(&mut client_cfg.transport)
        .unwrap()
        .mtu_discovery_config(None);
    // Only the client owns the cold history under test; the peer is legacy.
    let client = Endpoint::new(Arc::new(cold), None, true, None);
    let server = Endpoint::new(
        Arc::new(admission_endpoint_config()),
        Some(Arc::new(server_cfg)),
        true,
        None,
    );
    let mut pair = Pair::new_from_endpoint(client, server);
    let (ch, peer) = pair.connect_with(client_cfg);
    assert!(!pair.drive_bounded());

    // One ack-eliciting client packet P; the peer's ACK of P is held.
    let up = pair.client_conn_mut(ch).streams().open(Dir::Uni).unwrap();
    pair.client_send(ch, up).write(b"p").unwrap();
    pair.client.drive_outgoing(pair.time);
    let (_, with_p) = pair.client_conn_mut(ch).sent_history_data_state_for_probe();
    assert!(with_p >= 1);
    pair.client.outbound.drain(..).for_each(|(_, bytes)| {
        pair.server.inbound.push_back((pair.time, None, BytesMut::from(bytes.as_ref())));
    });
    pair.server.drive_incoming(pair.time, pair.client.addr);
    pair.server.process_connection_events(peer);
    pair.time += Duration::from_millis(30);
    pair.server.drive(pair.time, pair.client.addr);
    let held: Vec<_> = pair.server.outbound.drain(..).map(|(_, b)| b).collect();
    assert!(!held.is_empty(), "peer must have produced its ACK of P");

    // Peer data burst; every client packet reaches the peer normally.
    let down = pair.server_conn_mut(peer).streams().open(Dir::Uni).unwrap();
    let payload = vec![0x5a; REPLY];
    assert_eq!(pair.server_send(peer, down).write(&payload).unwrap(), REPLY);
    pair.server_send(peer, down).finish().unwrap();
    let frames_before = pair.client_conn_mut(ch).stats().frame_tx;
    let acks_before = frames_before.acks;
    let mut peak = with_p;
    let mut delivered = 0usize;
    for _ in 0..1000 {
        pair.server.drive_outgoing(pair.time);
        let burst: Vec<_> = pair.server.outbound.drain(..).map(|(_, b)| b).collect();
        if burst.is_empty() {
            break;
        }
        for bytes in burst {
            delivered += 1;
            pair.client.inbound.push_back((pair.time, None, BytesMut::from(bytes.as_ref())));
            pair.client.drive_incoming(pair.time, pair.server.addr);
            pair.client.process_connection_events(ch);
            pair.client.drive_outgoing(pair.time);
            peak = peak.max(pair.client_conn_mut(ch).sent_history_data_state_for_probe().1);
            if let Some(error) = pair.client_conn_mut(ch).retained_error() {
                panic!("honest reordered ACK closed the client: {error} peak={peak} delivered={delivered}");
            }
            pair.client.outbound.drain(..).for_each(|(_, b)| {
                pair.server.inbound.push_back((pair.time, None, BytesMut::from(b.as_ref())));
            });
            pair.server.drive_incoming(pair.time, pair.client.addr);
            pair.server.process_connection_events(peer);
        }
    }
    let frames = pair.client_conn_mut(ch).stats().frame_tx;
    let acks = frames.acks - acks_before;
    // Upper bound on ack-eliciting client packets: one per ae frame sent.
    let ae = |f: &crate::FrameStats| {
        f.ack_frequency + f.crypto + f.data_blocked + f.datagram + u64::from(f.handshake_done)
            + f.immediate_ack + f.max_data + f.max_stream_data + f.max_streams_bidi
            + f.max_streams_uni + f.new_connection_id + f.new_token + f.path_challenge
            + f.path_response + f.ping + f.reset_stream + f.retire_connection_id
            + f.stream_data_blocked + f.streams_blocked_bidi + f.streams_blocked_uni
            + f.stop_sending + f.stream
    };
    let ae_sent = (ae(&frames) - ae(&frames_before)) as usize;
    eprintln!("ack-only-history: slots={} delivered={delivered} client_acks={acks} ae_sent={ae_sent} peak={peak} with_p={with_p}", 2 * SLOTS);
    assert!(acks as usize > 4 * SLOTS, "ACK-only pressure must exceed the whole ring");
    assert!(ae_sent < SLOTS, "ae traffic alone stays inside the ordinary class");
    assert!(peak <= with_p + ae_sent, "only ack-eliciting records may occupy cold history");

    // The held peer ACK arrives last and releases P.
    for bytes in held {
        pair.client.inbound.push_back((pair.time, None, BytesMut::from(bytes.as_ref())));
    }
    pair.client.drive_incoming(pair.time, pair.server.addr);
    pair.client.process_connection_events(ch);
    assert!(!pair.drive_bounded());
    let mut recv = pair.client_recv(ch, down);
    let mut chunks = recv.read(true).unwrap();
    let mut actual = Vec::new();
    while let Some(chunk) = chunks.next(REPLY).unwrap() {
        actual.extend_from_slice(&chunk.bytes);
    }
    let _ = chunks.finalize();
    assert_eq!(actual, payload);
    assert!(pair.client_conn_mut(ch).sent_history_data_state_for_probe().1 < with_p);
    assert!(pair.client_conn_mut(ch).retained_error().is_none());
    assert!(pair.server_conn_mut(peer).retained_error().is_none());
    assert!(!budget.quarantined());
    control_records::stop(&mut pair, &[ch]);
    drop(pair);
    assert_eq!(budget.used(), 0);
}
