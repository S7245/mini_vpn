//! Original ACK packet-threshold loss and actual NEW_CONNECTION_ID in one wire.
use super::*;
fn fixture(lost: bool, after_cids: usize) -> (bytes::BackingBudget, Pair, ConnectionHandle, Bytes) {
    let budget = cold_fixture_budget(2);
    let mut config = admission_endpoint_config();
    config.receive_datagram_limit(1472).unwrap();
    config.receive_backing_meter(bytes::BackingMeter::with_budget(&budget));
    let mut server_config = server_config();
    let mut transport = TransportConfig::default();
    transport
        .deterministic_packet_numbers(true)
        .mtu_discovery_config(None);
    server_config.transport = Arc::new(transport);
    let mut client_config = client_config_with_deterministic_pns();
    Arc::get_mut(&mut client_config.transport)
        .unwrap()
        .mtu_discovery_config(None);
    let mut pair = Pair::new(Arc::new(config), server_config);
    let client = pair.begin_connect(client_config);
    assert!(!pair.drive_bounded());
    let server = pair.server.assert_accept();
    pair.finish_connect(client, server);
    // Use original, linearly owned retirement nodes. Repeated retired IDs are
    // legal and are emitted/retransmitted by the original writer.
    let now = pair.time;
    assert!(pair.client_conn_mut(client).try_local_address_changed(now));
    pair.client_conn_mut(client)
        .queue_repeated_retirement_for_capacity_probe(0, 19);
    pair.client_conn_mut(client).ping();
    pair.client.drive_outgoing(pair.time);
    let outgoing: Vec<_> = pair.client.outbound.drain(..).collect();
    assert_eq!(outgoing.len(), 1);
    if !lost {
        for (_, p) in outgoing {
            pair.server
                .inbound
                .push_back((pair.time, None, BytesMut::from(p.as_ref())));
        }
    }
    assert_eq!(pair.client_conn_mut(client).retire_stock_for_probe().4, 20);
    for _ in 0..3 {
        pair.client_conn_mut(client).ping();
        pair.client.drive_outgoing(pair.time);
        for (_, p) in pair.client.outbound.drain(..) {
            pair.server
                .inbound
                .push_back((pair.time, None, BytesMut::from(p.as_ref())));
        }
    }
    pair.server.drive_incoming(pair.time, pair.client.addr);
    pair.server.process_connection_events(server);
    let now = pair.time;
    pair.server_conn_mut(server).rotate_local_cid(5, now);
    let conn = pair.server.connections.get_mut(&server).unwrap();
    while let Some(event) = conn.poll_endpoint_events() {
        pair.server
            .endpoint
            .handle_connection_event(server, event, conn);
    }
    pair.server.drive_outgoing(pair.time);
    let output: Vec<_> = pair.server.outbound.drain(..).collect();
    let packet = output
        .into_iter()
        .map(|(_, p)| p)
        .find(|p| {
            let frames = pair.client_conn_mut(client).packet_frames_for_probe(p);
            frames.iter().any(|f| matches!(f, frame::Frame::Ack(_)))
                && frames
                    .iter()
                    .filter(|f| matches!(f, frame::Frame::NewConnectionId(_)))
                    .count()
                    > 1
        })
        .expect("actual encrypted ACK plus multiple CID frames");
    let mut candidate = pair.client_conn_mut(client).remote_cid_queue_for_probe();
    let mut added = 0;
    for f in pair
        .client_conn_mut(client)
        .packet_frames_for_probe(&packet)
    {
        if let frame::Frame::NewConnectionId(f) = f {
            match candidate.insert(f) {
                Ok(Some((range, _))) => added += (range.end - range.start) as usize,
                Ok(None) => {}
                Err(crate::cid_queue::InsertError::Retired) => added += 1,
                Err(_) => panic!("actual CID window invalid"),
            }
        }
    }
    assert!(added > 0);
    pair.client_conn_mut(client)
        .queue_repeated_retirement_for_capacity_probe(0, after_cids - added);
    (budget, pair, client, packet)
}
#[test]
fn ack_loss_then_multiple_cids_reject_limit_before_any_protocol_commit() {
    for full in [false, true] {
        let (budget, mut pair, client, packet) = fixture(true, 31);
        let pending = pair.client_conn_mut(client).retire_stock_for_probe();
        assert_eq!(pending.4, 20);
        let before = pair
            .client_conn_mut(client)
            .packet_spaces_snapshot_for_probe();
        let cids = pair.client_conn_mut(client).remote_cid_state_for_probe();
        pair.client
            .inbound
            .push_back((pair.time, None, BytesMut::from(packet.as_ref())));
        pair.client.drive_incoming(pair.time, pair.server.addr);
        let held = full.then(|| budget.try_reserve(budget.limit() - budget.used()).unwrap());
        pair.client.process_connection_events(client);
        let conn = pair.client_conn_mut(client);
        assert!(
            matches!(conn.retained_error(), Some(ConnectionError::TransportError(e))
        if e.code == TransportErrorCode::CONNECTION_ID_LIMIT_ERROR)
        );
        assert!(
            conn.packet_spaces_snapshot_for_probe() == before,
            "ACK/loss/PN must remain uncommitted on an already predictable CID limit error"
        );
        assert_eq!(conn.remote_cid_state_for_probe(), cids);
        assert_eq!(conn.retire_stock_for_probe(), pending);
        assert!(!budget.quarantined());
        drop(held);
        control_records::stop(&mut pair, &[client]);
        drop(pair);
        assert_eq!(budget.used(), 0);
    }
}
#[test]
fn cid_limit_exact_boundary_and_confirmed_retirements_are_not_false_loss() {
    for lost in [false, true] {
        let (budget, mut pair, client, packet) = fixture(lost, if lost { 30 } else { 50 });
        let before = pair.client_conn_mut(client).stats().path.lost_packets;
        pair.client
            .inbound
            .push_back((pair.time, None, BytesMut::from(packet.as_ref())));
        pair.client.drive_incoming(pair.time, pair.server.addr);
        let held = budget.try_reserve(budget.limit() - budget.used()).unwrap();
        pair.client.process_connection_events(client);
        let conn = pair.client_conn_mut(client);
        assert!(
            conn.retained_error().is_none(),
            "exact boundary must remain valid at Full: {:?}",
            conn.retained_error()
        );
        assert_eq!(conn.retire_stock_for_probe().3.len(), 50);
        assert_eq!(conn.stats().path.lost_packets - before, u64::from(lost));
        assert!(!budget.quarantined());
        drop(held);
        control_records::stop(&mut pair, &[client]);
        drop(pair);
        assert_eq!(budget.used(), 0);
    }
}

#[test]
fn cid_replay_matches_original_ack_loss_across_duplicate_time_frequency_and_early_space() {
    for case in 0..6 {
        let (budget, mut pair, client, packet) = fixture(true, 30);
        let actual = pair
            .client_conn_mut(client)
            .packet_frames_for_probe(&packet)
            .into_iter()
            .find_map(|f| match f {
                frame::Frame::Ack(ack) => Some(ack),
                _ => None,
            })
            .unwrap();
        let now = pair.time;
        pair.client_conn_mut(client)
            .assert_cid_replay_for_probe(now, actual, case);
        assert!(!budget.quarantined());
        control_records::stop(&mut pair, &[client]);
        drop(pair);
        assert_eq!(budget.used(), 0);
    }
}

#[test]
fn cids_before_ack_preserve_original_wire_order_limit_at_full() {
    let (budget, mut pair, client, packet) = fixture(true, 31);
    let frames = pair
        .client_conn_mut(client)
        .packet_frames_for_probe(&packet);
    let held = budget.try_reserve(budget.limit() - budget.used()).unwrap();
    let now = pair.time;
    pair.client_conn_mut(client)
        .assert_cids_before_ack_for_probe(now, frames);
    assert!(!budget.quarantined());
    drop(held);
    control_records::stop(&mut pair, &[client]);
    drop(pair);
    assert_eq!(budget.used(), 0);
}
