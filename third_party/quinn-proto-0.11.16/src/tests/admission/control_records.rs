//! Actual local controls, encrypted packets, original loss/PTO and terminal drain.
use super::*;

fn pair(capacity: usize, connections: usize) -> (Pair, bytes::BackingBudget) {
    let budget = cold_fixture_budget(connections * 2);
    let mut config = admission_endpoint_config();
    config.receive_datagram_limit(1472).unwrap();
    config.receive_control_record_limit(capacity).unwrap();
    config.receive_backing_meter(bytes::BackingMeter::with_budget(&budget));
    (Pair::new(Arc::new(config), server_config()), budget)
}
pub(super) fn connect(pair: &mut Pair) -> (ConnectionHandle, ConnectionHandle) {
    let client = pair.begin_connect(client_config());
    assert!(!pair.drive_bounded(), "bounded original handshake driver");
    let server = pair.server.assert_accept();
    pair.finish_connect(client, server);
    assert!(pair.client_conn_mut(client).retained_error().is_none());
    assert!(pair.server_conn_mut(server).retained_error().is_none());
    (client, server)
}
fn stock(connection: &Connection, capacity: usize, free: usize, pending: usize, sent: usize) {
    assert_eq!(
        connection.control_record_stock_for_probe(),
        (capacity, free, pending, sent, pending + sent)
    );
    assert_eq!(capacity, free + pending + sent);
}
pub(super) fn stop(pair: &mut Pair, clients: &[ConnectionHandle]) {
    for &client in clients {
        let now = pair.time;
        pair.client_conn_mut(client)
            .close(now, 0u32.into(), Bytes::new());
    }
    assert!(!pair.drive_bounded());
    assert_eq!(pair.client.endpoint.open_connections(), 0);
    assert_eq!(pair.server.endpoint.open_connections(), 0);
    assert_eq!(pair.client.endpoint.known_cids(), 0);
    assert_eq!(pair.server.endpoint.known_cids(), 0);
}
#[test]
fn reset_record_survives_original_pto_and_real_ack_then_reuses_same_capacity() {
    let (mut pair, budget) = pair(1, 1);
    let (client, server) = connect(&mut pair);
    let id = pair
        .client_conn_mut(client)
        .streams()
        .open(Dir::Uni)
        .unwrap();
    pair.client_send(client, id).reset(41u32.into()).unwrap();
    assert!(pair.client_send(client, id).reset(42u32.into()).is_err());
    assert!(pair.client_conn_mut(client).retained_error().is_none());
    stock(pair.client_conn_mut(client), 1, 0, 1, 0);
    pair.client.drive_outgoing(pair.time);
    assert!(!pair.client.outbound.is_empty());
    pair.client.outbound.clear(); // Real encrypted loss; do not change ciphertext.
    stock(pair.client_conn_mut(client), 1, 0, 0, 1);
    let before = pair.client_conn_mut(client).stats().frame_tx.reset_stream;
    pair.time = pair
        .client_conn_mut(client)
        .loss_detection_deadline()
        .unwrap();
    pair.client.drive_outgoing(pair.time);
    assert!(pair.client_conn_mut(client).stats().frame_tx.reset_stream > before);
    stock(pair.client_conn_mut(client), 1, 0, 0, 1);
    assert!(!pair.drive_bounded());
    assert!(pair.server_conn_mut(server).stats().frame_rx.reset_stream > 0);
    stock(pair.client_conn_mut(client), 1, 1, 0, 0);
    let next = pair
        .client_conn_mut(client)
        .streams()
        .open(Dir::Uni)
        .unwrap();
    pair.client_send(client, next).reset(43u32.into()).unwrap();
    assert!(!pair.drive_bounded());
    stock(pair.client_conn_mut(client), 1, 1, 0, 0);
    stop(&mut pair, &[client]);
    drop(pair);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}
#[test]
fn full_control_record_refuses_before_stream_or_pn_change_and_other_connection_survives() {
    let (mut pair, budget) = pair(1, 2);
    let (client, _) = connect(&mut pair);
    let (other, peer) = connect(&mut pair); // Same original endpoints, independent Conn.
    let first = pair
        .client_conn_mut(client)
        .streams()
        .open(Dir::Uni)
        .unwrap();
    let second = pair
        .client_conn_mut(client)
        .streams()
        .open(Dir::Uni)
        .unwrap();
    pair.client_send(client, second).write(b"retained").unwrap();
    let progress = pair.client_send(client, second).progress().unwrap();
    pair.client_send(client, first).reset(1u32.into()).unwrap();
    let now = pair.time;
    let connection = pair.client_conn_mut(client);
    let pn = connection.next_data_number_for_record_probe();
    let deadline = connection.original_close_deadline_for_stream_probe(now);
    assert!(connection
        .send_stream(second, now)
        .reset(2u32.into())
        .is_err());
    assert_eq!(
        connection.send_stream(second, now).progress().unwrap(),
        progress
    );
    assert_eq!(connection.next_data_number_for_record_probe(), pn);
    assert_eq!(connection.stats().frame_tx.reset_stream, 0);
    assert_eq!(connection.poll_timeout(), Some(deadline));
    assert!(connection.is_closed() && !connection.is_drained());
    let first_error = connection.retained_error().unwrap();
    assert!(
        matches!(&first_error, ConnectionError::TransportError(e) if e.code == TransportErrorCode::INTERNAL_ERROR)
    );
    assert!(connection
        .send_stream(second, now)
        .reset(3u32.into())
        .is_err());
    assert_eq!(connection.retained_error(), Some(first_error));
    stock(connection, 1, 0, 1, 0);
    let live = pair
        .client_conn_mut(other)
        .streams()
        .open(Dir::Uni)
        .unwrap();
    pair.client_send(other, live)
        .write(b"other continues")
        .unwrap();
    pair.client_send(other, live).finish().unwrap();
    assert!(!pair.drive_bounded());
    let mut recv = pair.server_recv(peer, live);
    let mut chunks = recv.read(true).unwrap();
    assert_eq!(
        &chunks.next(64).unwrap().unwrap().bytes[..],
        b"other continues"
    );
    assert!(chunks.next(64).unwrap().is_none());
    chunks.finalize();
    assert!(pair.client_conn_mut(other).retained_error().is_none());
    stop(&mut pair, &[client, other]);
    drop(pair);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}
#[test]
fn stop_uses_same_pool_and_redundant_stop_does_not_consume_a_full_pool() {
    let (mut pair, budget) = pair(1, 1);
    let (client, server) = connect(&mut pair);
    let id = pair
        .client_conn_mut(client)
        .streams()
        .open(Dir::Bi)
        .unwrap();
    pair.client_send(client, id).write(b"request").unwrap();
    assert!(!pair.drive_bounded());
    pair.server_send(server, id).write(b"response").unwrap();
    assert!(!pair.drive_bounded());
    pair.client_recv(client, id).stop(9u32.into()).unwrap();
    stock(pair.client_conn_mut(client), 1, 0, 1, 0);
    assert!(pair.client_recv(client, id).stop(10u32.into()).is_err());
    assert!(pair.client_conn_mut(client).retained_error().is_none());
    assert!(!pair.drive_bounded());
    assert!(pair.server_conn_mut(server).stats().frame_rx.stop_sending > 0);
    stock(pair.client_conn_mut(client), 1, 1, 0, 0);
    stop(&mut pair, &[client]);
    drop(pair);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn foreign_sent_record_at_real_pto_fails_before_new_pn_and_keeps_quarantine() {
    let (mut pair, budget) = pair(1, 2);
    let (client, _) = connect(&mut pair);
    let (other, _) = connect(&mut pair);
    for ch in [client, other] {
        let id = pair.client_conn_mut(ch).streams().open(Dir::Uni).unwrap();
        pair.client_send(ch, id).reset(51u32.into()).unwrap();
    }
    pair.client.drive_outgoing(pair.time);
    assert!(!pair.client.outbound.is_empty());
    pair.client.outbound.clear(); // Lose actual encrypted packets before peer ACK.
    stock(pair.client_conn_mut(client), 1, 0, 0, 1);
    stock(pair.client_conn_mut(other), 1, 0, 0, 1);
    let [Some(left), Some(right)] = pair.client.connections.get_disjoint_mut([&client, &other])
    else {
        panic!("both original connections must remain live");
    };
    // Explicit custody fault, never presented as a naturally reachable Full.
    assert!(left.swap_sent_reset_records_for_probe(right));
    let now = left.loss_detection_deadline().unwrap();
    let pn = left.next_data_number_for_record_probe();
    let frames = left.stats().frame_tx;
    left.handle_timeout(now);
    let close_deadline = left.original_close_deadline_for_stream_probe(now);
    let mut wire = Vec::new();
    assert!(left.poll_transmit(now, 1, &mut wire).is_none());
    assert!(wire.is_empty());
    assert_eq!(left.next_data_number_for_record_probe(), pn);
    assert_eq!(left.stats().frame_tx.stream, frames.stream);
    assert_eq!(left.stats().frame_tx.reset_stream, frames.reset_stream);
    let first = left.retained_error().unwrap();
    assert!(matches!(&first, ConnectionError::TransportError(e)
        if e.code == TransportErrorCode::INTERNAL_ERROR
        && e.reason.contains("foreign control record custody")));
    assert!(left.is_closed() && !left.is_drained());
    assert_eq!(left.poll_timeout(), Some(close_deadline));
    left.close(now, 0u32.into(), Bytes::new());
    assert_eq!(left.retained_error(), Some(first));
    assert!(budget.quarantined() && budget.used() > 0);
    // Probe may have taken old retransmits/changed ACK flags before detecting
    // the fault. Known destruction is not rollback or a clean drain receipt.
    drop(pair);
    assert!(budget.quarantined());
    assert_eq!(budget.used(), 1);
}

#[test]
fn terminal_receive_stop_needs_no_record_even_when_shared_control_pool_is_full() {
    let (mut pair, budget) = pair(1, 1);
    let (client, server) = connect(&mut pair);
    let recv = pair
        .server_conn_mut(server)
        .streams()
        .open(Dir::Uni)
        .unwrap();
    pair.server_send(server, recv).reset(61u32.into()).unwrap();
    assert!(!pair.drive_bounded());
    let send = pair
        .client_conn_mut(client)
        .streams()
        .open(Dir::Uni)
        .unwrap();
    pair.client_send(client, send).reset(62u32.into()).unwrap();
    stock(pair.client_conn_mut(client), 1, 0, 1, 0);
    pair.client_recv(client, recv).stop(63u32.into()).unwrap();
    stock(pair.client_conn_mut(client), 1, 0, 1, 0);
    assert!(pair.client_conn_mut(client).retained_error().is_none());
    stop(&mut pair, &[client]);
    drop(pair);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}
