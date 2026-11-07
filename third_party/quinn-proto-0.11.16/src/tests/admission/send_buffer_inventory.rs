//! Original authenticated STREAM write admission and physical send inventory.
use super::*;

pub(super) struct RetainedLayouts {
    pub(super) charged: usize,
    meter: bytes::BackingMeter,
    allocations: usize,
    payload_bytes: usize,
    physical_bytes: usize,
}
impl RetainedLayouts {
    pub(super) fn assert_quarantined(&self, budget: &bytes::BackingBudget) {
        assert!(budget.quarantined());
        assert_eq!(budget.used(), self.charged);
        let actual = self.meter.snapshot();
        assert_eq!((actual.allocations, actual.payload_bytes, actual.physical_bytes),
            (self.allocations, self.payload_bytes, self.physical_bytes));
    }
}
// Exact for these ByteSlice -> BytesMut Shared fixtures. Metadata belongs to
// each original field; only actual payload allocation identities are deduped.
// This is not an oracle for the distinct metered-inline BytesArray layout.
pub(super) fn retained_layouts(pair: &Pair) -> RetainedLayouts {
    let mut unique = std::collections::HashMap::new();
    let mut metadata = 0;
    let meter = pair.client.endpoint.config().receive_backing_meter.clone().unwrap();
    assert!(meter.same_authority(pair.server.endpoint.config().receive_backing_meter.as_ref().unwrap()));
    for connection in pair.client.connections.values().chain(pair.server.connections.values()) {
        let layouts = connection.stream_layouts_for_probe();
        assert_eq!(connection.retained_stream_metadata_bytes(),
            Some(layouts.iter().filter(|(id, _)| id.is_none()).map(|(_, n)| n).sum()));
        for (identity, size) in layouts {
            if let Some(identity) = identity {
                if let Some(previous) = unique.insert(identity, size) { assert_eq!(previous, size); }
            } else { metadata += size; }
        }
    }
    let physical_bytes = unique.values().sum::<usize>();
    RetainedLayouts { charged: (metadata + physical_bytes).max(1), meter,
        allocations: unique.len(), payload_bytes: unique.keys().map(|id| id.capacity).sum(), physical_bytes }
}

#[test]
fn actual_native_stream_write_full_does_not_consume_or_copy_application_input() {
    let budget = cold_fixture_budget(2);
    let meter = bytes::BackingMeter::with_budget(&budget);
    let mut config = admission_endpoint_config();
    config.receive_backing_meter(meter.clone());
    config.receive_datagram_limit(1472).unwrap();
    let mut pair = Pair::new(Arc::new(config), server_config());
    let client = pair.begin_connect(client_config());
    assert!(!pair.drive_bounded());
    let stream = pair.client_conn_mut(client).streams().open(Dir::Uni).unwrap();
    let header = bytes::BytesMut::metered_copy_layout_for_len(1).unwrap() - 1;
    let pressure = bytes::BytesMut::try_zeroed_metered(budget.limit() - budget.used() - header, &meter).unwrap();
    assert_eq!(budget.used(), budget.limit());
    let result = pair.client_send(client, stream).write(&[0x47; 145]);
    let accepted = result.as_ref().copied().unwrap_or(0);
    eprintln!("[N1-native-stream-write-full] used={} limit={} accepted={accepted} result={result:?}", budget.used(), budget.limit());
    drop(pressure);
    let now = pair.time;
    pair.client_conn_mut(client).close(now, 0u32.into(), bytes::Bytes::new());
    assert!(!pair.drive_bounded());
    assert!(pair.client_conn_mut(client).is_drained());
    assert_eq!(pair.client.endpoint.open_connections(), 0);
    assert_eq!(pair.server.endpoint.open_connections(), 0);
    drop(pair);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
    assert_eq!(accepted, 0, "original ByteSlice and segment allocation must be admitted before consuming application input");
}

fn native_pair() -> (Pair, bytes::BackingBudget, bytes::BackingMeter, ConnectionHandle, ConnectionHandle) {
    let budget = cold_fixture_budget(2);
    let meter = bytes::BackingMeter::with_budget(&budget);
    let mut config = admission_endpoint_config();
    config.receive_backing_meter(meter.clone());
    config.receive_datagram_limit(1472).unwrap();
    let mut pair = Pair::new(Arc::new(config), server_config());
    let (client, server) = pair.connect();
    (pair, budget, meter, client, server)
}

#[test]
fn actual_native_whole_chunk_call_refuses_foreign_tail_before_consuming_prefix() {
    let (mut pair, budget, meter, client, _) = native_pair();
    let other = bytes::BackingBudget::new(4096).unwrap();
    let foreign = bytes::BackingMeter::with_budget(&other);
    let stream = pair.client_conn_mut(client).streams().open(Dir::Uni).unwrap();
    let mut chunks = [
        BytesMut::try_copy_from_slice_metered(b"prefix", &meter).unwrap().freeze(),
        BytesMut::try_copy_from_slice_metered(b"foreign", &foreign).unwrap().freeze(),
    ];
    let identities = chunks.each_ref().map(|b| b.allocation_identity());
    assert!(matches!(pair.client_send(client, stream).write_chunks(&mut chunks), Err(crate::WriteError::Admission(bytes::MeteredCopyError::Quarantined))));
    assert_eq!(chunks.each_ref().map(|b| b.allocation_identity()), identities);
    assert_eq!(&chunks[0][..], b"prefix");
    assert_eq!(&chunks[1][..], b"foreign");
    assert_eq!(pair.client_send(client, stream).progress().unwrap().written_bytes, 0);
    assert!(budget.quarantined() && other.quarantined());
}

#[test]
fn actual_encrypted_key_update_ack_full_refuses_and_peer_recovers() {
    encrypted_recovery_full(false, true, false, false);
}
#[test]
fn actual_encrypted_key_update_loss_full_refuses_and_peer_recovers() {
    encrypted_recovery_full(true, true, false, false);
}
#[test]
fn actual_encrypted_ack_full_refuses_before_selecting_and_peer_recovers() {
    encrypted_recovery_full(false, false, false, false);
}
#[test]
fn actual_encrypted_loss_full_refuses_before_selecting_and_peer_recovers() {
    encrypted_recovery_full(true, false, false, false);
}
#[test]
fn actual_encrypted_ack_does_not_fund_loss_for_already_acked_records() {
    encrypted_recovery_full(false, false, true, false);
}
#[test]
fn key_ack_and_stop_sending_full_preserves_packet_and_recovers() {
    encrypted_recovery_full(false, true, false, true);
}
#[test]
fn key_loss_and_stop_sending_full_preserves_packet_and_recovers() {
    encrypted_recovery_full(true, true, false, true);
}
fn encrypted_recovery_full(loss: bool, key_update: bool, inspect_bound: bool, with_stop: bool) {
    let (mut pair, budget, meter, client, server) = native_pair();
    // Random initial key lifetimes start at 10 packets; the loss rounds below
    // can exhaust a short one, leaving prev_crypto set so the server's forced
    // update is legally ignored. Settle one real rotation first, exactly as
    // packet_pressure does, rather than altering key lifetimes or timers.
    pair.client_conn_mut(client).force_key_update();
    pair.client_conn_mut(client).ping();
    assert!(!pair.drive_bounded());
    let retire = pair.client_conn_mut(client).packet_key_discard_for_probe().unwrap()
        .max(pair.server_conn_mut(server).packet_key_discard_for_probe().unwrap());
    pair.time = retire;
    pair.client_conn_mut(client).handle_timeout(retire);
    pair.server_conn_mut(server).handle_timeout(retire);
    assert!(!pair.drive_bounded());
    let mut streams = Vec::new();
    for byte in [31, 52] {
        let id = pair.client_conn_mut(client).streams().open(Dir::Uni).unwrap();
        pair.client_send(client, id).write(&[byte; 64]).unwrap();
        pair.client_send(client, id).finish().unwrap();
        if loss && byte == 31 {
            let reset = pair.client_conn_mut(client).streams().open(Dir::Uni).unwrap();
            pair.client_send(client, reset).reset(17u32.into()).unwrap();
        }
        pair.client.drive_outgoing(pair.time);
        streams.push(id);
    }
    let before = pair.client_conn_mut(client).stream_packets_for_probe();
    assert_eq!(before.len(), 2);
    if loss {
        assert_eq!(pair.client_conn_mut(client).stats().frame_tx.reset_stream, 1);
        for _ in 0..4 {
            pair.client.outbound.clear(); // Actual whole datagram loss, including STREAM + RESET.
            pair.client_conn_mut(client).ping();
            pair.client.drive_outgoing(pair.time);
        }
    }
    for (_, packet) in pair.client.outbound.drain(..) {
        pair.server.inbound.push_back((pair.time, None, BytesMut::from(&packet[..])));
    }
    pair.server.drive_incoming(pair.time, pair.client.addr);
    pair.server.process_connection_events(server);
    pair.time += Duration::from_millis(30);
    let key_before = pair.client_conn_mut(client).key_phase_for_packet_probe();
    if key_update {
        let before = pair.server_conn_mut(server).key_phase_for_packet_probe();
        pair.server_conn_mut(server).force_key_update();
        assert_ne!(pair.server_conn_mut(server).key_phase_for_packet_probe(), before);
    }
    if with_stop {
        // The actual STOP now shares whole-packet admission with key and ACK/loss.
        pair.server_recv(server, streams[0]).stop(0u32.into()).unwrap();
    }
    pair.server.drive_outgoing(pair.time);
    assert!(!pair.server.outbound.is_empty());
    for (_, packet) in pair.server.outbound.drain(..) {
        pair.client.inbound.push_back((pair.time, None, BytesMut::from(&packet[..])));
    }
    let protocol_before = pair.client_conn_mut(client).packet_admission_snapshot_for_probe();
    let refused_before = pair.client_conn_mut(client).stats().udp_rx.admission_refused;
    pair.client.capture_inbound_packets = inspect_bound || with_stop;
    pair.client.drive_incoming(pair.time, pair.server.addr);
    if with_stop {
        let wire = Bytes::copy_from_slice(pair.client.captured_packets.last().unwrap());
        assert!(frame::Iter::new(wire).unwrap().any(|f| matches!(f, Ok(frame::Frame::StopSending(_)))));
    }
    if inspect_bound {
        let wire = Bytes::copy_from_slice(pair.client.captured_packets.last().unwrap());
        assert_eq!(pair.client_conn_mut(client).recovery_requirements_for_packet_probe(&wire), (2, 0),
            "records covered by the first ACK leave History before loss detection");
    }
    let header = BytesMut::metered_copy_layout_for_len(1).unwrap() - 1;
    let pressure = BytesMut::try_zeroed_metered(budget.limit() - budget.used() - header, &meter).unwrap();
    assert_eq!(budget.used(), budget.limit());
    pair.client.process_connection_events(client);
    {
        assert_eq!(pair.client_conn_mut(client).packet_admission_snapshot_for_probe(), protocol_before);
        assert!(pair.client_conn_mut(client).stats().udp_rx.admission_refused > refused_before);
        assert_eq!(pair.client_conn_mut(client).stream_packets_for_probe(), before);
        for &stream in &streams {
            assert_eq!(pair.client_send(client, stream).progress().unwrap().acknowledged_bytes, 0);
        }
        drop(pressure);
        assert!(!pair.drive_bounded());
        if key_update { assert_ne!(pair.client_conn_mut(client).key_phase_for_packet_probe(), key_before); }
        for (index, stream) in streams.into_iter().enumerate() {
            if with_stop && index == 0 { continue; } // The peer deliberately stopped this receive half.
            let mut recv = pair.server_recv(server, stream);
            let mut chunks = recv.read(true).unwrap();
            let mut delivered = Vec::new();
            while let Some(chunk) = chunks.next(1024).unwrap() { delivered.extend_from_slice(&chunk.bytes); }
            let _ = chunks.finalize();
            assert_eq!(delivered, vec![[31, 52][index]; 64]);
        }
        assert!(pair.client_conn_mut(client).retained_error().is_none());
        control_records::stop(&mut pair, &[client]);
        drop(pair);
        assert_eq!(budget.used(), 0);
        assert!(!budget.quarantined());
        return;
    }

}
