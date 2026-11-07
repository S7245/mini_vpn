//! Actual receive credit: read preparation, original MAX_STREAM_DATA, ACK/loss.
use super::{
    control_records::{connect, stop},
    *,
};

fn pair(
    capacity: usize,
    connections: usize,
) -> (
    Pair,
    bytes::BackingBudget,
    ConnectionHandle,
    ConnectionHandle,
) {
    pair_window(capacity, connections, 64)
}
fn pair_window(
    capacity: usize,
    connections: usize,
    window: u32,
) -> (
    Pair,
    bytes::BackingBudget,
    ConnectionHandle,
    ConnectionHandle,
) {
    let mut config = admission_endpoint_config();
    config.receive_control_record_limit(capacity).unwrap();
    config.receive_datagram_limit(1472).unwrap();
    let layout = crate::connection::ColdResources::layout_bytes(&config, None).unwrap();
    // Each client_config() owns its actual default cache independently of its
    // two Connection cold owners. Keep the original packet/name headroom too.
    let cache_layout = crate::TokenMemoryCache::default()
        .fixed_layout_bytes()
        .unwrap();
    let budget = bytes::BackingBudget::new(
        layout * connections * 2
            + cache_layout * connections
            + Endpoint::native_registration_layout_bytes()
            + 64 * 1024,
    )
    .unwrap();
    config.receive_backing_meter(bytes::BackingMeter::with_budget(&budget));
    let mut server = server_config();
    Arc::get_mut(&mut server.transport)
        .unwrap()
        .stream_receive_window(window.into())
        .mtu_discovery_config(None);
    let mut pair = Pair::new(Arc::new(config), server);
    let (client, server) = connect(&mut pair);
    (pair, budget, client, server)
}

#[test]
fn real_read_and_credit_packet_transfer_without_late_metadata_allocation() {
    let (mut pair, budget, client, server) = pair(2, 1);
    let id = pair
        .client_conn_mut(client)
        .streams()
        .open(Dir::Uni)
        .unwrap();
    pair.client_send(client, id).write(&[0x73; 32]).unwrap();
    assert!(!pair.drive_bounded());
    let mut actual = None;
    let read_allocations = allocations_in(|| {
        let mut recv = pair.server_recv(server, id);
        let mut chunks = recv.read(true).unwrap();
        actual = Some(chunks.next(8).unwrap().unwrap());
        assert!(chunks.finalize().should_transmit());
    });
    assert_eq!(actual.as_ref().unwrap().bytes, [0x73; 8][..]);
    let mut wire = Vec::with_capacity(64 * 1472);
    let mut tx = None;
    let now = pair.time;
    let before = pair
        .server_conn_mut(server)
        .stats()
        .frame_tx
        .max_stream_data;
    let packet_allocations = allocations_in(|| {
        tx = pair
            .server_conn_mut(server)
            .poll_transmit(now, 1, &mut wire);
    });
    assert_eq!(
        pair.server_conn_mut(server)
            .stats()
            .frame_tx
            .max_stream_data
            - before,
        1
    );
    pair.server
        .outbound
        .push_back((tx.unwrap(), Bytes::from(wire)));
    assert!(!pair.drive_bounded());
    assert!(
        pair.client_conn_mut(client)
            .stats()
            .frame_rx
            .max_stream_data
            > 0
    );
    drop(actual);
    stop(&mut pair, &[client]);
    drop(pair);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
    assert_eq!(
        (read_allocations, packet_allocations),
        (0, 0),
        "read and packet must transfer cold credit, not allocate HashSets"
    );
}

fn read(pair: &mut Pair, server: ConnectionHandle, id: StreamId, count: usize) -> Bytes {
    let mut recv = pair.server_recv(server, id);
    let mut chunks = recv.read(true).unwrap();
    let bytes = chunks.next(count).unwrap().unwrap().bytes;
    let _ = chunks.finalize();
    bytes
}

#[test]
fn tiny_window_retains_original_zero_read_and_no_next_drop_advertisements() {
    let (mut pair, budget, client, server) = pair_window(2, 1, 4);
    let id = pair
        .client_conn_mut(client)
        .streams()
        .open(Dir::Uni)
        .unwrap();
    pair.client_send(client, id).write(b"x").unwrap();
    assert!(!pair.drive_bounded());
    {
        let mut recv = pair.server_recv(server, id);
        let mut chunks = recv.read(true).unwrap();
        assert!(chunks.next(0).unwrap().unwrap().bytes.is_empty());
        assert!(chunks.finalize().should_transmit());
    }
    assert_eq!(
        pair.server_recv(server, id).progress().unwrap().read_offset,
        0
    );
    assert_eq!(
        pair.server_conn_mut(server).flow_credit_stock_for_probe(),
        (2, 1, 1, 1, 0)
    );
    pair.server.drive_outgoing(pair.time);
    assert_eq!(
        pair.server_conn_mut(server)
            .stats()
            .frame_tx
            .max_stream_data,
        1
    );
    assert!(!pair.server.outbound.is_empty());
    pair.server.outbound.clear(); // Prior same-valued advertisement really lost.
    {
        let mut recv = pair.server_recv(server, id);
        drop(recv.read(true).unwrap()); // Original Drop owes the same zero-diff frame.
    }
    assert_eq!(
        pair.server_conn_mut(server).flow_credit_stock_for_probe(),
        (2, 0, 2, 1, 1)
    );
    assert!(!pair.drive_bounded());
    assert!(
        pair.client_conn_mut(client)
            .stats()
            .frame_rx
            .max_stream_data
            > 0
    );
    assert!(pair.server_conn_mut(server).retained_error().is_none());
    stop(&mut pair, &[client]);
    drop(pair);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn tiny_window_full_retains_already_owed_frame_and_refuses_before_removing_recv() {
    let (mut pair, budget, client, server) = pair_window(1, 1, 4);
    let mut ids = Vec::new();
    for _ in 0..2 {
        let id = pair
            .client_conn_mut(client)
            .streams()
            .open(Dir::Uni)
            .unwrap();
        pair.client_send(client, id).write(b"x").unwrap();
        ids.push(id);
    }
    assert!(!pair.drive_bounded());
    assert!(read(&mut pair, server, ids[0], 0).is_empty());
    assert_eq!(read(&mut pair, server, ids[0], 1), b"x"[..]);
    {
        let mut recv = pair.server_recv(server, ids[0]);
        let mut chunks = recv.read(true).unwrap();
        assert!(matches!(chunks.next(4), Err(crate::ReadError::Blocked)));
    }
    assert_eq!(
        pair.server_conn_mut(server).flow_credit_stock_for_probe(),
        (1, 0, 1, 1, 0)
    );
    assert!(pair.server_conn_mut(server).retained_error().is_none());
    let before = pair.server_recv(server, ids[1]).progress().unwrap();
    let used = budget.used();
    let now = pair.time;
    let deadline = pair
        .server_conn_mut(server)
        .original_close_deadline_for_stream_probe(now);
    assert!(matches!(
        pair.server_recv(server, ids[1]).read(true),
        Err(crate::ReadableError::BackingAuthorityLost)
    ));
    assert_eq!(pair.server_recv(server, ids[1]).progress().unwrap(), before);
    assert_eq!(budget.used(), used);
    assert_eq!(pair.server_conn_mut(server).poll_timeout(), Some(deadline));
    assert!(
        matches!(pair.server_conn_mut(server).retained_error(), Some(ConnectionError::TransportError(e)) if e.code == TransportErrorCode::INTERNAL_ERROR)
    );
    stop(&mut pair, &[client]);
    drop(pair);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn rejected_ordering_cancels_prepared_credit_without_losing_original_recv() {
    let (mut pair, budget, client, server) = pair_window(2, 1, 4);
    let id = pair
        .client_conn_mut(client)
        .streams()
        .open(Dir::Uni)
        .unwrap();
    pair.client_send(client, id).write(b"xyz").unwrap();
    assert!(!pair.drive_bounded());
    {
        let mut recv = pair.server_recv(server, id);
        let mut chunks = recv.read(false).unwrap();
        assert_eq!(chunks.next(1).unwrap().unwrap().bytes, b"x"[..]);
    }
    assert!(!pair.drive_bounded());
    let stock = pair.server_conn_mut(server).flow_credit_stock_for_probe();
    assert_eq!(stock, (2, 2, 0, 0, 0));
    let before = pair.server_recv(server, id).progress().unwrap();
    let used = budget.used();
    assert!(matches!(
        pair.server_recv(server, id).read(true),
        Err(crate::ReadableError::IllegalOrderedRead)
    ));
    assert_eq!(pair.server_recv(server, id).progress().unwrap(), before);
    assert_eq!(
        pair.server_conn_mut(server).flow_credit_stock_for_probe(),
        stock
    );
    assert_eq!(budget.used(), used);
    assert!(pair.server_conn_mut(server).retained_error().is_none());
    stop(&mut pair, &[client]);
    drop(pair);
    assert_eq!(budget.used(), 0);
}

#[test]
fn illegal_ordered_read_precedes_full_control_inventory_without_first_error() {
    let (mut pair, budget, client, server) = pair_window(1, 1, 4);
    let id = pair
        .client_conn_mut(client)
        .streams()
        .open(Dir::Uni)
        .unwrap();
    pair.client_send(client, id).write(b"x").unwrap();
    assert!(!pair.drive_bounded());
    assert_eq!(read(&mut pair, server, id, 1), b"x"[..]);
    assert!(!pair.drive_bounded());
    {
        let mut recv = pair.server_recv(server, id);
        // The real stream is drained of buffered bytes, so changing to unordered
        // does not need the independently blocked defrag-copy path.
        let mut chunks = recv.read(false).unwrap();
        assert!(matches!(chunks.next(4), Err(crate::ReadError::Blocked)));
    }
    assert!(!pair.drive_bounded());
    let reset = pair
        .server_conn_mut(server)
        .streams()
        .open(Dir::Uni)
        .unwrap();
    pair.server_send(server, reset).reset(9u32.into()).unwrap();
    let before = pair.server_recv(server, id).progress().unwrap();
    let stock = pair.server_conn_mut(server).flow_credit_stock_for_probe();
    assert_eq!(stock, (1, 0, 1, 0, 0)); // Sole control slot actually owned by RESET.
    assert!(matches!(
        pair.server_recv(server, id).read(true),
        Err(crate::ReadableError::IllegalOrderedRead)
    ));
    assert_eq!(pair.server_recv(server, id).progress().unwrap(), before);
    assert_eq!(
        pair.server_conn_mut(server).flow_credit_stock_for_probe(),
        stock
    );
    assert!(pair.server_conn_mut(server).retained_error().is_none());
    stop(&mut pair, &[client]);
    drop(pair);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn pending_credit_closes_clean_but_quarantined_read_settlement_keeps_unknown_charge() {
    for quarantine in [false, true] {
        let (mut pair, budget, client, server) = pair(1, 1);
        let id = pair
            .client_conn_mut(client)
            .streams()
            .open(Dir::Uni)
            .unwrap();
        pair.client_send(client, id).write(&[0x29; 32]).unwrap();
        assert!(!pair.drive_bounded());
        let meter = pair
            .server
            .endpoint
            .config()
            .receive_backing_meter
            .clone()
            .unwrap();
        let now = pair.time;
        let deadline = pair
            .server_conn_mut(server)
            .original_close_deadline_for_stream_probe(now);
        let retained = send_buffer_inventory::retained_layouts(&pair);
        {
            let mut recv = pair.server_recv(server, id);
            let mut chunks = recv.read(true).unwrap();
            assert_eq!(chunks.next(8).unwrap().unwrap().bytes, [0x29; 8][..]);
            if quarantine {
                meter.quarantine();
            } // Explicit fault after actual read, before Drop.
        }
        assert_eq!(
            pair.server_conn_mut(server).flow_credit_stock_for_probe(),
            (1, 0, 1, 1, 0)
        );
        if quarantine {
            let first = pair.server_conn_mut(server).retained_error().unwrap();
            assert!(
                matches!(&first, ConnectionError::TransportError(e) if e.code == TransportErrorCode::INTERNAL_ERROR)
            );
            assert_eq!(pair.server_conn_mut(server).poll_timeout(), Some(deadline));
            pair.server_conn_mut(server)
                .close(now, 0u32.into(), Bytes::new());
            assert_eq!(pair.server_conn_mut(server).retained_error(), Some(first));
        } else {
            assert!(pair.server_conn_mut(server).retained_error().is_none());
            stop(&mut pair, &[client]); // Queued credit still owned at original close.
        }
        drop(pair);
        if quarantine { retained.assert_quarantined(&budget); } else { assert_eq!(budget.used(), 0); }
        assert_eq!(budget.quarantined(), quarantine);
    }
}

#[test]
fn natural_full_refuses_only_new_positive_credit_and_keeps_other_connection_live() {
    let (mut pair, budget, client, server) = pair(1, 2);
    let (other, other_peer) = connect(&mut pair);
    let mut ids = Vec::new();
    for (length, fin) in [(32, false), (32, false), (1, false), (1, true)] {
        let id = pair
            .client_conn_mut(client)
            .streams()
            .open(Dir::Uni)
            .unwrap();
        pair.client_send(client, id)
            .write(&vec![0x31; length])
            .unwrap();
        if fin {
            pair.client_send(client, id).finish().unwrap();
        }
        ids.push(id);
    }
    assert!(!pair.drive_bounded());
    assert_eq!(read(&mut pair, server, ids[0], 8), [0x31; 8][..]);
    assert_eq!(
        pair.server_conn_mut(server).flow_credit_stock_for_probe(),
        (1, 0, 1, 1, 0)
    );
    // Existing same-key pending owns future updates; no second node is needed.
    assert_eq!(read(&mut pair, server, ids[0], 8), [0x31; 8][..]);
    assert!(read(&mut pair, server, ids[1], 0).is_empty());
    assert_eq!(read(&mut pair, server, ids[1], 1), [0x31][..]); // Still below threshold.
    assert_eq!(read(&mut pair, server, ids[2], 1), [0x31][..]);
    {
        let mut recv = pair.server_recv(server, ids[2]);
        let mut chunks = recv.read(true).unwrap();
        assert!(matches!(chunks.next(64), Err(crate::ReadError::Blocked)));
    }
    {
        let mut recv = pair.server_recv(server, ids[3]);
        let mut chunks = recv.read(true).unwrap();
        assert_eq!(chunks.next(64).unwrap().unwrap().bytes, [0x31][..]);
        assert!(chunks.next(64).unwrap().is_none()); // FIN needs no window advertisement.
    }
    assert!(pair.server_conn_mut(server).retained_error().is_none());
    let before = pair.server_recv(server, ids[1]).progress().unwrap();
    let used = budget.used();
    let now = pair.time;
    let deadline = pair
        .server_conn_mut(server)
        .original_close_deadline_for_stream_probe(now);
    {
        let mut recv = pair.server_recv(server, ids[1]);
        let mut chunks = recv.read(true).unwrap();
        assert!(matches!(
            chunks.next(7),
            Err(crate::ReadError::BackingAuthorityLost)
        ));
    }
    assert_eq!(pair.server_recv(server, ids[1]).progress().unwrap(), before);
    assert_eq!(budget.used(), used);
    assert_eq!(pair.server_conn_mut(server).poll_timeout(), Some(deadline));
    let first = pair.server_conn_mut(server).retained_error().unwrap();
    assert!(
        matches!(&first, ConnectionError::TransportError(e) if e.code == TransportErrorCode::INTERNAL_ERROR)
    );
    pair.server_conn_mut(server)
        .close(now, 0u32.into(), Bytes::new());
    assert_eq!(pair.server_conn_mut(server).retained_error(), Some(first));
    let live = pair
        .client_conn_mut(other)
        .streams()
        .open(Dir::Uni)
        .unwrap();
    pair.client_send(other, live)
        .write(b"same Endpoint survives")
        .unwrap();
    pair.client_send(other, live).finish().unwrap();
    assert!(!pair.drive_bounded());
    assert_eq!(
        read(&mut pair, other_peer, live, 64),
        b"same Endpoint survives"[..]
    );
    assert!(pair.server_conn_mut(other_peer).retained_error().is_none());
    stop(&mut pair, &[client, other]);
    drop(pair);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined()); // Known capacity failure, not an unknown release.
}

#[test]
fn real_pto_and_ack_loss_preserve_same_stream_credit_generations() {
    for force_loss_before_transmit in [false, true] {
        let (mut pair, budget, client, server) = pair(2, 1);
        let id = pair
            .client_conn_mut(client)
            .streams()
            .open(Dir::Uni)
            .unwrap();
        pair.client_send(client, id).write(&[0x63; 32]).unwrap();
        assert!(!pair.drive_bounded());
        assert_eq!(read(&mut pair, server, id, 8), [0x63; 8][..]);
        pair.server.drive_outgoing(pair.time);
        assert!(!pair.server.outbound.is_empty());
        pair.server.outbound.clear(); // Lose the original encrypted MAX_STREAM_DATA.
        assert_eq!(
            pair.server_conn_mut(server).flow_credit_stock_for_probe(),
            (2, 1, 1, 0, 1)
        );
        if force_loss_before_transmit {
            for _ in 0..3 {
                pair.server_conn_mut(server).ping();
                pair.server.drive_outgoing(pair.time);
            }
            assert_eq!(pair.server.outbound.len(), 3);
            pair.drive_server();
            pair.time += pair.latency;
            pair.drive_client(); // Original ACK covers three later encrypted PNs.
            pair.time += pair.latency;
            assert!(!pair.server.inbound.is_empty());
        }
        {
            let mut recv = pair.server_recv(server, id);
            let mut chunks = recv.read(true).unwrap();
            assert_eq!(chunks.next(8).unwrap().unwrap().bytes, [0x63; 8][..]);
            // Exercise the real Chunks Drop settlement instead of explicit finalize.
        }
        assert_eq!(
            pair.server_conn_mut(server).flow_credit_stock_for_probe(),
            (2, 0, 2, 1, 1)
        );
        if force_loss_before_transmit {
            let lost = pair.server_conn_mut(server).stats().path.lost_packets;
            pair.server.drive_incoming(pair.time, pair.client.addr);
            pair.server.process_connection_events(server); // No new poll_transmit yet.
            assert!(pair.server_conn_mut(server).stats().path.lost_packets > lost);
            assert_eq!(
                pair.server_conn_mut(server).flow_credit_stock_for_probe(),
                (2, 1, 1, 1, 0),
                "real loss merges into the existing pending key and returns only the duplicate"
            );
        } else {
            let before = pair
                .server_conn_mut(server)
                .stats()
                .frame_tx
                .max_stream_data;
            pair.time = pair
                .server_conn_mut(server)
                .loss_detection_deadline()
                .unwrap();
            pair.server.drive_outgoing(pair.time);
            assert!(
                pair.server_conn_mut(server)
                    .stats()
                    .frame_tx
                    .max_stream_data
                    > before
            );
        }
        let stock = pair.server_conn_mut(server).flow_credit_stock_for_probe();
        assert_eq!(stock.0, stock.1 + stock.2);
        assert_eq!(stock.2, stock.3 + stock.4);
        assert!(!pair.drive_bounded());
        assert!(
            pair.client_conn_mut(client)
                .stats()
                .frame_rx
                .max_stream_data
                > 0
        );
        assert_eq!(
            pair.server_conn_mut(server).flow_credit_stock_for_probe(),
            (2, 2, 0, 0, 0)
        );
        stop(&mut pair, &[client]);
        drop(pair);
        assert_eq!(budget.used(), 0);
        assert!(!budget.quarantined());
    }
}

#[test]
fn real_unordered_backing_full_cancels_credit_preserves_recv_and_uses_original_close() {
    let (mut pair, budget, client, server) = pair_window(2, 1, 4);
    let id = pair
        .client_conn_mut(client)
        .streams()
        .open(Dir::Uni)
        .unwrap();
    pair.client_send(client, id).write(b"xyz").unwrap();
    assert!(!pair.drive_bounded());
    let before = pair.server_recv(server, id).progress().unwrap();
    let stock = pair.server_conn_mut(server).flow_credit_stock_for_probe();
    let held = budget.try_reserve(budget.limit() - budget.used()).unwrap();
    let used = budget.used();
    let now = pair.time;
    let deadline = pair
        .server_conn_mut(server)
        .original_close_deadline_for_stream_probe(now);
    {
        let mut recv = pair.server_recv(server, id);
        assert!(matches!(
            recv.read(false),
            Err(crate::ReadableError::BackingAuthorityLost)
        ));
    }
    assert_eq!(pair.server_recv(server, id).progress().unwrap(), before);
    assert_eq!(
        pair.server_conn_mut(server).flow_credit_stock_for_probe(),
        stock
    );
    assert_eq!(budget.used(), used);
    assert!(!budget.quarantined());
    assert_eq!(pair.server_conn_mut(server).poll_timeout(), Some(deadline));
    let first = pair.server_conn_mut(server).retained_error().unwrap();
    assert!(
        matches!(&first, ConnectionError::TransportError(e) if e.code == TransportErrorCode::INTERNAL_ERROR)
    );
    pair.server_conn_mut(server)
        .close(now, 0u32.into(), Bytes::new());
    assert_eq!(pair.server_conn_mut(server).retained_error(), Some(first));
    drop(held);
    stop(&mut pair, &[client]);
    drop(pair);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}
