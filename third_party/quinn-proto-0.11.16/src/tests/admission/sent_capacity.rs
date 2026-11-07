//! Approved A capacity failure through original encrypted QUIC and whole-datagram loss.
//! D47: exhausted by lost ack-eliciting records (ACK-only records are not retained).
use super::*;
fn move_wire(from: &mut TestEndpoint, to: &mut TestEndpoint, now: Instant) -> usize {
    let mut n = 0;
    for (_, bytes) in from.outbound.drain(..) {
        to.inbound
            .push_back((now, None, BytesMut::from(bytes.as_ref())));
        n += 1;
    }
    n
}
#[test]
fn history_capacity_failure_is_connection_local_and_retains_first_error() {
    const AE_PER_STEP: usize = 64;
    let budget = cold_fixture_budget_for_endpoints(4, 2);
    let meter = bytes::BackingMeter::with_budget(&budget);
    let mut endpoint = admission_endpoint_config();
    endpoint.receive_backing_meter(meter.clone());
    endpoint.receive_datagram_limit(1472).unwrap();
    let mut server = server_config();
    let mut frequency = AckFrequencyConfig::default();
    frequency.ack_eliciting_threshold(0u32.into());
    let mut transport = TransportConfig::default();
    transport
        .deterministic_packet_numbers(true)
        .max_idle_timeout(Some(Duration::from_secs(15).try_into().unwrap()))
        .mtu_discovery_config(None)
        .ack_frequency_config(Some(frequency));
    server.transport = Arc::new(transport);
    let mut client = client_config_with_deterministic_pns();
    // D47: lost ae records are congestion-controlled; the window must admit
    // the whole 2048-record ring (as the Service pressure test's 8MiB Cubic).
    let mut cubic = crate::congestion::CubicConfig::default();
    cubic.initial_window(8 * 1024 * 1024);
    Arc::get_mut(&mut client.transport)
        .unwrap()
        .max_idle_timeout(Some(Duration::from_secs(15).try_into().unwrap()))
        .keep_alive_interval(Some(Duration::from_secs(5)))
        .mtu_discovery_config(None)
        .congestion_controller_factory(Arc::new(cubic));
    let config = Arc::new(endpoint);
    let mut other = Pair::new(config.clone(), server.clone());
    let (other_ch, other_peer) = other.connect_with(client.clone());
    let mut pair = Pair::new(config, server);
    let (ch, peer) = pair.connect_with(client);
    let mut flows = Vec::new();
    for payload in [
        b"first-unacknowledged-flow".as_slice(),
        b"second-unacknowledged-flow".as_slice(),
    ] {
        let stream = pair.client_conn_mut(ch).streams().open(Dir::Uni).unwrap();
        pair.client_send(ch, stream).write(payload).unwrap();
        flows.push(stream);
    }
    pair.client.drive_outgoing(pair.time);
    let mut dropped_client = pair.client.outbound.len();
    assert!(dropped_client > 0);
    pair.client.outbound.clear(); // Real network loss after original encoding.
    let acks = pair.client_conn_mut(ch).stats().frame_rx.acks;
    let lost = pair.client_conn_mut(ch).stats().path.lost_packets;
    let start = pair.time;
    let initial = pair
        .client_conn_mut(ch)
        .sent_history_len_for_capacity_probe();
    let mut dropped_peer_acks = 0;
    let mut accepted_ping = 0;
    let mut previous = initial;
    let mut terminal = None;
    'pressure: for round in 0..20 {
        let deadline = pair.client_conn_mut(ch).loss_detection_deadline().unwrap();
        while pair.time + Duration::from_secs(1) < deadline {
            pair.time += Duration::from_secs(1);
            // D47 amendment: ACK-only packets are no longer retained (D46), so
            // the history is exhausted by actual ack-eliciting client records
            // whose whole datagrams are lost, while peer ACKs stay dropped.
            for _ in 0..AE_PER_STEP {
                pair.client_conn_mut(ch).ping();
                pair.client.drive_outgoing(pair.time);
                if let Some(error) = pair.client_conn_mut(ch).retained_error() {
                    // Keep the encoded prefix/Close for the link restore below.
                    terminal = Some(error);
                    break 'pressure;
                }
                dropped_client += pair.client.outbound.len();
                pair.client.outbound.clear();
            }
            let before_ack = pair.server_conn_mut(peer).stats().frame_tx.acks;
            pair.server_conn_mut(peer).ping();
            pair.server.drive_outgoing(pair.time);
            if pair.server_conn_mut(peer).stats().frame_tx.acks > before_ack {
                // Drop the entire original datagram(s), never edit ciphertext.
                // This may drop a PING coalesced with ACK; the next PING still arrives.
                dropped_peer_acks += pair.server.outbound.len();
                pair.server.outbound.clear();
            } else {
                accepted_ping += move_wire(&mut pair.server, &mut pair.client, pair.time);
            }
            let before = pair.client_conn_mut(ch).stats().frame_tx;
            pair.client.drive(pair.time, pair.server.addr);
            if let Some(error) = pair.client_conn_mut(ch).retained_error() {
                terminal = Some(error);
                break 'pressure;
            }
            let after = pair.client_conn_mut(ch).stats().frame_tx;
            assert_eq!(after.stream, before.stream, "only ACK traffic is forwarded");
            assert_eq!(after.ping, before.ping, "original PTO is driven separately");
            assert_eq!(after.new_connection_id, before.new_connection_id);
            assert_eq!(after.immediate_ack, before.immediate_ack);
            move_wire(&mut pair.client, &mut pair.server, pair.time);
            pair.server.drive_incoming(pair.time, pair.client.addr);
            pair.server.process_connection_events(peer);
            assert!(pair.client_conn_mut(ch).retained_error().is_none());
            assert!(pair.server_conn_mut(peer).retained_error().is_none());
            assert_eq!(pair.client_conn_mut(ch).stats().frame_rx.acks, acks);
        }
        // The lost ae records re-armed the original loss timer; fire that one.
        let deadline = pair.client_conn_mut(ch).loss_detection_deadline().unwrap();
        pair.time = deadline;
        pair.client.drive_outgoing(pair.time);
        if let Some(error) = pair.client_conn_mut(ch).retained_error() {
            terminal = Some(error);
            break;
        }
        dropped_client += pair.client.outbound.len();
        assert!(!pair.client.outbound.is_empty(), "original PTO must send");
        pair.client.outbound.clear();
        let count = pair
            .client_conn_mut(ch)
            .sent_history_len_for_capacity_probe();
        assert!(count > previous);
        assert_eq!(pair.client_conn_mut(ch).stats().path.lost_packets, lost);
        assert_eq!(pair.client_conn_mut(ch).stats().frame_rx.acks, acks);
        assert!(pair.client_conn_mut(ch).retained_error().is_none());
        assert!(pair.server_conn_mut(peer).retained_error().is_none());
        eprintln!(
            "sent-history: pto_round={} sent={} elapsed={:?} ack_delta=0 lost_delta=0",
            round + 1,
            count,
            pair.time - start
        );
        previous = count;
    }
    assert!(pair.time - start > Duration::from_secs(15));
    assert!(dropped_peer_acks > 0 && accepted_ping > 0);
    eprintln!("sent-history-final: initial={initial} final={previous} dropped_client={dropped_client} dropped_peer_ack_datagrams={dropped_peer_acks} accepted_peer_ping={accepted_ping} Q={}",budget.quarantined());
    let error =
        terminal.expect("actual precharged history capacity must terminate this connection");
    assert!(matches!(&error, ConnectionError::TransportError(error)
        if error.code == TransportErrorCode::INTERNAL_ERROR
        && error.reason == "sent history control capacity exhausted"));
    let (space, before, after, occupied) = pair
        .client_conn_mut(ch)
        .sent_history_rejection_for_probe()
        .unwrap();
    assert_eq!(space, crate::packet::SpaceId::Data);
    assert_eq!(before, after, "capacity refusal cannot assign a PN");
    assert_eq!(occupied, 2048);
    eprintln!(
        "history-refusal: space={space:?} pn_before={before} pn_after={after} occupied={occupied}"
    );
    assert!(pair.client_conn_mut(ch).is_closed());
    for stream in flows {
        assert!(pair
            .client_send(ch, stream)
            .write(b"after-failure")
            .is_err());
    }
    assert!(
        !budget.quarantined(),
        "known capacity failure must not quarantine another connection"
    );
    let stream = other
        .client_conn_mut(other_ch)
        .streams()
        .open(Dir::Uni)
        .unwrap();
    other
        .client_send(other_ch, stream)
        .write(b"unrelated-connection-still-progresses")
        .unwrap();
    assert!(!other.drive_bounded());
    let mut recv = other.server_recv(other_peer, stream);
    let mut chunks = recv.read(true).unwrap();
    assert_eq!(
        chunks.next(4096).unwrap().unwrap().bytes.as_ref(),
        b"unrelated-connection-still-progresses"
    );
    chunks.finalize();
    assert!(other.client_conn_mut(other_ch).retained_error().is_none());
    assert!(other.server_conn_mut(other_peer).retained_error().is_none());
    let first = error.to_string();
    // Restore the original link after the terminal decision. Deliver any
    // already-encoded prefix/Close, then drive the original close/drain clock.
    move_wire(&mut pair.client, &mut pair.server, pair.time);
    assert!(!pair.drive_bounded());
    assert!(pair.client_conn_mut(ch).is_drained());
    assert!(pair.server_conn_mut(peer).is_drained());
    assert_eq!(
        pair.client_conn_mut(ch)
            .retained_error()
            .unwrap()
            .to_string(),
        first
    );
    assert_eq!(pair.client.endpoint.open_connections(), 0);
    assert_eq!(pair.server.endpoint.open_connections(), 0);
    eprintln!("history-capacity: same_conn_flows=2 other_connection_progress=true first_retained=true maps=0 Q=false");
    control_records::stop(&mut other, &[other_ch]);
    drop(pair);
    drop(other);
    assert_eq!(budget.used(), 0);
}

#[test]
fn ordinary_history_full_keeps_original_pto_then_ack_resumes_two_streams() {
    let budget = cold_fixture_budget(2);
    let mut config = admission_endpoint_config();
    config.receive_backing_meter(bytes::BackingMeter::with_budget(&budget));
    config.receive_datagram_limit(1472).unwrap();
    config.receive_sent_history_limits(4, 1024).unwrap();
    let mut server = server_config();
    Arc::get_mut(&mut server.transport)
        .unwrap()
        .mtu_discovery_config(None);
    let mut client = client_config_with_deterministic_pns();
    Arc::get_mut(&mut client.transport)
        .unwrap()
        .mtu_discovery_config(None)
        .pad_to_mtu(true);
    let mut pair = Pair::new(Arc::new(config), server);
    let (ch, peer) = pair.connect_with(client);
    let payloads = [vec![0x61; 20 * 1024], vec![0x62; 20 * 1024]];
    let mut streams = Vec::new();
    for payload in &payloads {
        let stream = pair.client_conn_mut(ch).streams().open(Dir::Uni).unwrap();
        assert_eq!(
            pair.client_send(ch, stream).write(payload).unwrap(),
            payload.len()
        );
        pair.client_send(ch, stream).finish().unwrap();
        streams.push(stream);
    }
    pair.client.drive_outgoing(pair.time);
    let (pn_full, occupied) = pair.client_conn_mut(ch).sent_history_data_state_for_probe();
    assert_eq!(occupied, 4);
    assert!(move_wire(&mut pair.client, &mut pair.server, pair.time) > 0);
    pair.server.drive_incoming(pair.time, pair.client.addr);
    pair.server.process_connection_events(peer);
    let before = pair.client_conn_mut(ch).stats().frame_tx;
    pair.client.drive_outgoing(pair.time);
    assert!(pair.client.outbound.is_empty());
    assert_eq!(
        pair.client_conn_mut(ch).sent_history_data_state_for_probe(),
        (pn_full, 4)
    );
    let deadline = pair.client_conn_mut(ch).loss_detection_deadline().unwrap();
    pair.time = deadline;
    pair.client.drive_outgoing(pair.time);
    let after = pair.client_conn_mut(ch).stats().frame_tx;
    assert_eq!(
        after.stream, before.stream,
        "control credit cannot encode fresh or retransmitted STREAM data"
    );
    assert!(after.ping > before.ping || after.immediate_ack > before.immediate_ack);
    assert!(move_wire(&mut pair.client, &mut pair.server, pair.time) > 0);
    pair.server.drive_incoming(pair.time, pair.client.addr);
    pair.server.process_connection_events(peer);
    // Drive original peer timers/ACK queue; do not inject ACK or edit ciphertext.
    pair.server.drive_outgoing(pair.time);
    pair.server.drive_outgoing(pair.time);
    assert!(move_wire(&mut pair.server, &mut pair.client, pair.time) > 0);
    pair.client.drive_incoming(pair.time, pair.server.addr);
    pair.client.process_connection_events(ch);
    assert!(
        pair.client_conn_mut(ch)
            .sent_history_data_state_for_probe()
            .1
            < 4
    );
    assert!(!pair.drive_bounded());
    assert!(pair.client_conn_mut(ch).stats().frame_tx.stream > after.stream);
    for (stream, payload) in streams.into_iter().zip(payloads) {
        let mut recv = pair.server_recv(peer, stream);
        let mut chunks = recv.read(true).unwrap();
        let mut actual = Vec::new();
        while let Some(chunk) = chunks.next(64 * 1024).unwrap() {
            actual.extend_from_slice(&chunk.bytes);
        }
        chunks.finalize();
        assert_eq!(actual, payload);
    }
    assert!(pair.client_conn_mut(ch).retained_error().is_none());
    assert!(pair.server_conn_mut(peer).retained_error().is_none());
    eprintln!("ordinary-full: slots=4 two_stream_bytes=40960 original_pto={deadline:?} control_without_STREAM=true actual_ACK_resumes=true Q=false");
    assert!(!budget.quarantined());
    control_records::stop(&mut pair, &[ch]);
    drop(pair);
    assert_eq!(budget.used(), 0);
}
