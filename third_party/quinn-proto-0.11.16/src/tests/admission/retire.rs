//! Original encrypted CID rotation and Connection retirement settlement.
use super::*;

fn pair(capacity: usize) -> (Pair, bytes::BackingBudget) {
    let budget = cold_fixture_budget(4);
    let mut config = admission_endpoint_config();
    config.receive_datagram_limit(1472).unwrap();
    config.receive_control_record_limit(capacity).unwrap();
    config.receive_backing_meter(bytes::BackingMeter::with_budget(&budget));
    (Pair::new(Arc::new(config), server_config()), budget)
}

#[test]
fn retire_full_rejects_entire_remote_cid_rotation_before_active_token_or_pn() {
    let (mut pair, budget) = pair(1);
    let (client, server) = control_records::connect(&mut pair);
    let before = pair.client_conn_mut(client).remote_cid_state_for_probe();
    let queue = pair.client_conn_mut(client).remote_cid_queue_for_probe();
    let pn = pair
        .client_conn_mut(client)
        .next_data_number_for_record_probe();
    let now = pair.time;
    let deadline = pair
        .client_conn_mut(client)
        .original_close_deadline_for_stream_probe(now);
    pair.server_conn_mut(server).rotate_local_cid(5, now);
    pair.drive_server(); // Real encrypted NEW_CONNECTION_ID, not a local direct insert.
    assert!(!pair.client.inbound.is_empty());
    pair.client.drive_incoming(now, pair.server.addr);
    pair.client.process_connection_events(client);
    let conn = pair.client_conn_mut(client);
    assert_eq!(
        conn.remote_cid_state_for_probe(),
        before,
        "all remote CID and reset-token mutation must follow retirement admission"
    );
    assert_eq!(conn.remote_cid_queue_for_probe(), queue);
    assert_eq!(conn.retire_stock_for_probe(), (1, 1, 0, vec![], 0));
    assert_eq!(conn.next_data_number_for_record_probe(), pn);
    assert_eq!(conn.poll_timeout(), Some(deadline));
    let first = conn.retained_error().unwrap();
    assert!(matches!(&first, ConnectionError::TransportError(e)
        if e.code == TransportErrorCode::INTERNAL_ERROR));
    conn.close(now, 0u32.into(), Bytes::new());
    assert_eq!(conn.retained_error(), Some(first));
    assert!(!pair.drive_bounded());
    assert_eq!(pair.client.endpoint.open_connections(), 0);
    assert_eq!(pair.client.endpoint.known_cids(), 0);
    drop(pair);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn retire_rebind_full_preserves_original_state_and_other_connection_progress() {
    let (mut pair, budget) = pair(1);
    let (client, _) = control_records::connect(&mut pair);
    let (other, peer) = control_records::connect(&mut pair);
    let now = pair.time;
    assert!(pair.client_conn_mut(client).try_local_address_changed(now));
    let conn = pair.client_conn_mut(client);
    let before = conn.remote_cid_state_for_probe();
    let queue = conn.remote_cid_queue_for_probe();
    let stock = conn.retire_stock_for_probe();
    assert_eq!(stock, (1, 0, 1, vec![0], 0));
    let pn = conn.next_data_number_for_record_probe();
    let tx = conn.stats().udp_tx;
    let deadline = conn.original_close_deadline_for_stream_probe(now);
    assert!(!conn.try_local_address_changed(now));
    assert_eq!(conn.remote_cid_state_for_probe(), before);
    assert_eq!(conn.remote_cid_queue_for_probe(), queue);
    assert_eq!(conn.retire_stock_for_probe(), stock);
    assert_eq!(conn.next_data_number_for_record_probe(), pn);
    let after_tx = conn.stats().udp_tx;
    assert_eq!(
        (after_tx.datagrams, after_tx.bytes, after_tx.ios),
        (tx.datagrams, tx.bytes, tx.ios)
    );
    assert_eq!(conn.poll_timeout(), Some(deadline));
    let first = conn.retained_error().unwrap();
    assert!(
        matches!(&first, ConnectionError::TransportError(e) if e.code == TransportErrorCode::INTERNAL_ERROR)
    );
    conn.local_address_changed(now);
    assert_eq!(conn.retained_error(), Some(first));
    let id = pair
        .client_conn_mut(other)
        .streams()
        .open(Dir::Uni)
        .unwrap();
    pair.client_send(other, id)
        .write(b"other-connection-still-live")
        .unwrap();
    pair.client_send(other, id).finish().unwrap();
    assert!(!pair.drive_bounded());
    let mut recv = pair.server_recv(peer, id);
    let mut chunks = recv.read(true).unwrap();
    assert_eq!(
        chunks.next(100).unwrap().unwrap().bytes,
        b"other-connection-still-live"[..]
    );
    assert!(chunks.next(100).unwrap().is_none());
    chunks.finalize();
    control_records::stop(&mut pair, &[other]);
    drop(pair);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn retire_original_lifo_ciphertext_loss_pto_ack_and_pending_close_settle_same_nodes() {
    for close_pending in [false, true] {
        let (mut pair, budget) = pair(3);
        let (client, server) = control_records::connect(&mut pair);
        let now = pair.time;
        assert!(pair.client_conn_mut(client).try_local_address_changed(now));
        assert!(pair.client_conn_mut(client).try_local_address_changed(now));
        let stock = pair.client_conn_mut(client).retire_stock_for_probe();
        assert_eq!(stock, (3, 1, 2, vec![0, 1], 0));
        if !close_pending {
            pair.client.drive_outgoing(now);
            assert!(!pair.client.outbound.is_empty());
            let stock = pair.client_conn_mut(client).retire_stock_for_probe();
            assert_eq!(stock, (3, 1, 2, vec![], 2));
            pair.client.outbound.clear(); // Actual encrypted RETIRE frames lost.
            let before = pair
                .client_conn_mut(client)
                .stats()
                .frame_tx
                .retire_connection_id;
            pair.time = pair
                .client_conn_mut(client)
                .loss_detection_deadline()
                .unwrap();
            pair.client.drive_outgoing(pair.time);
            assert!(
                pair.client_conn_mut(client)
                    .stats()
                    .frame_tx
                    .retire_connection_id
                    > before
            );
            assert_eq!(pair.client_conn_mut(client).retire_stock_for_probe(), stock);
            pair.server.capture_inbound_packets = true;
            pair.drive_client();
            pair.server.drive_incoming(pair.time, pair.client.addr);
            let sequences: Vec<Vec<_>> = pair
                .server
                .captured_packets
                .iter()
                .map(|bytes| {
                    frame::Iter::new(Bytes::copy_from_slice(bytes))
                        .unwrap()
                        .filter_map(|f| match f.unwrap() {
                            frame::Frame::RetireConnectionId { sequence } => Some(sequence),
                            _ => None,
                        })
                        .collect()
                })
                .filter(|seqs: &Vec<u64>| !seqs.is_empty())
                .collect();
            // Both original PTO probes move the same two nodes: packet one
            // reverses original sent order, packet two reverses it again.
            assert_eq!(sequences, [vec![0, 1], vec![1, 0]]);
            assert!(!pair.drive_bounded());
            assert!(
                pair.server_conn_mut(server)
                    .stats()
                    .frame_rx
                    .retire_connection_id
                    >= 2
            );
            assert_eq!(
                pair.client_conn_mut(client).retire_stock_for_probe(),
                (3, 3, 0, vec![], 0)
            );
        }
        control_records::stop(&mut pair, &[client]);
        drop(pair);
        assert_eq!(budget.used(), 0);
        assert!(!budget.quarantined());
    }
}

#[test]
fn retire_reordered_original_peer_new_cid_takes_retired_branch_and_hard_full() {
    for capacity in [5, 7] {
        let (mut pair, budget) = pair(capacity);
        let (client, server) = control_records::connect(&mut pair);
        let now = pair.time;
        pair.server_conn_mut(server).rotate_local_cid(2, now);
        pair.drive_server();
        let held = std::mem::take(&mut pair.client.inbound); // Original seq5,6/prior2.
        assert!(!held.is_empty());
        pair.server_conn_mut(server).rotate_local_cid(7, now);
        pair.drive_server(); // New seq>=7/prior7 arrives first.
        pair.client.drive_incoming(now, pair.server.addr);
        pair.client.process_connection_events(client);
        assert_eq!(pair.client_conn_mut(client).active_rem_cid_seq(), 7);
        assert_eq!(
            pair.client_conn_mut(client).retire_stock_for_probe(),
            (capacity, capacity - 5, 5, vec![0, 1, 2, 3, 4], 0)
        );
        let before = pair.client_conn_mut(client).remote_cid_state_for_probe();
        let queue = pair.client_conn_mut(client).remote_cid_queue_for_probe();
        let pn = pair
            .client_conn_mut(client)
            .next_data_number_for_record_probe();
        let deadline = pair
            .client_conn_mut(client)
            .original_close_deadline_for_stream_probe(now);
        pair.client.inbound.extend(held);
        pair.client.drive_incoming(now, pair.server.addr);
        pair.client.process_connection_events(client);
        let conn = pair.client_conn_mut(client);
        assert_eq!(conn.remote_cid_state_for_probe(), before);
        assert_eq!(conn.remote_cid_queue_for_probe(), queue);
        assert_eq!(conn.next_data_number_for_record_probe(), pn);
        let stock = conn.retire_stock_for_probe();
        if capacity == 5 {
            assert_eq!(stock, (5, 0, 5, vec![0, 1, 2, 3, 4], 0));
            assert!(
                matches!(conn.retained_error(), Some(ConnectionError::TransportError(e)) if e.code == TransportErrorCode::INTERNAL_ERROR)
            );
            assert_eq!(conn.poll_timeout(), Some(deadline));
        } else {
            assert!(conn.retained_error().is_none());
            assert_eq!(stock.0, stock.2);
            assert_eq!(stock.1, 0);
            let mut sequences = stock.3;
            sequences.sort_unstable();
            assert_eq!(sequences, [0, 1, 2, 3, 4, 5, 6]);
        }
        control_records::stop(&mut pair, &[client]);
        drop(pair);
        assert_eq!(budget.used(), 0);
        assert!(!budget.quarantined());
    }
}

#[test]
fn retire_migration_full_prepares_before_path_rtt_and_congestion_mutation() {
    let (mut pair, budget) = pair(1);
    let (client, server) = control_records::connect(&mut pair);
    let now = pair.time;
    assert!(pair.server_conn_mut(server).try_local_address_changed(now));
    let conn = pair.server_conn_mut(server);
    let before = conn.remote_cid_state_for_probe();
    let queue = conn.remote_cid_queue_for_probe();
    let path = conn.remote_path_for_probe();
    let stock = conn.retire_stock_for_probe();
    let pn = conn.next_data_number_for_record_probe();
    let deadline = conn.original_close_deadline_for_stream_probe(now);
    pair.client.addr.set_port(pair.client.addr.port() + 1);
    pair.client_conn_mut(client).ping();
    pair.drive_client();
    assert!(!pair.server.inbound.is_empty());
    pair.server.drive_incoming(now, pair.client.addr);
    pair.server.process_connection_events(server);
    let conn = pair.server_conn_mut(server);
    assert_eq!(conn.remote_cid_state_for_probe(), before);
    assert_eq!(conn.remote_cid_queue_for_probe(), queue);
    assert_eq!(conn.remote_path_for_probe(), path);
    assert_eq!(conn.retire_stock_for_probe(), stock);
    assert_eq!(conn.next_data_number_for_record_probe(), pn);
    assert_eq!(conn.poll_timeout(), Some(deadline));
    assert!(
        matches!(conn.retained_error(), Some(ConnectionError::TransportError(e)) if e.code == TransportErrorCode::INTERNAL_ERROR)
    );
    // Restore only the harness address so original Close can reach its peer.
    pair.client.addr.set_port(pair.client.addr.port() - 1);
    assert!(!pair.drive_bounded());
    assert_eq!(pair.client.endpoint.open_connections(), 0);
    assert_eq!(pair.server.endpoint.open_connections(), 0);
    drop(pair);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn retire_foreign_loss_preflight_retains_first_and_quarantine_without_half_transfer() {
    let (mut pair, budget) = pair(2);
    let (client, _) = control_records::connect(&mut pair);
    let (other, _) = control_records::connect(&mut pair);
    let now = pair.time;
    for ch in [client, other] {
        assert!(pair.client_conn_mut(ch).try_local_address_changed(now));
    }
    pair.client.drive_outgoing(now);
    pair.client.outbound.clear();
    let [Some(left), Some(right)] = pair.client.connections.get_disjoint_mut([&client, &other])
    else {
        panic!("original owners live");
    };
    assert!(left.swap_sent_retire_for_probe(right)); // Explicit foreign-custody fault.
    let now = left.loss_detection_deadline().unwrap();
    let pn = left.next_data_number_for_record_probe();
    let flight = left.bytes_in_flight();
    let tx = left.stats().frame_tx.retire_connection_id;
    left.handle_timeout(now);
    let deadline = left.original_close_deadline_for_stream_probe(now);
    let mut wire = Vec::new();
    assert!(left.poll_transmit(now, 1, &mut wire).is_none());
    assert!(wire.is_empty());
    assert_eq!(left.next_data_number_for_record_probe(), pn);
    assert_eq!(left.bytes_in_flight(), flight);
    assert_eq!(left.stats().frame_tx.retire_connection_id, tx);
    assert!(left.retire_stock_for_probe().3.is_empty());
    let first = left.retained_error().unwrap();
    assert!(
        matches!(&first, ConnectionError::TransportError(e) if e.reason == "foreign retirement custody")
    );
    assert_eq!(left.poll_timeout(), Some(deadline));
    left.close(now, 0u32.into(), Bytes::new());
    assert_eq!(left.retained_error(), Some(first));
    drop(pair);
    assert_eq!(budget.used(), 1);
    assert!(budget.quarantined());
}
