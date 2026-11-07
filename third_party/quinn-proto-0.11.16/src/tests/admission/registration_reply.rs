//! Root and the actual receiver install are one Native ownership transaction.
use super::*;

fn config(budget: &bytes::BackingBudget) -> Arc<EndpointConfig> {
    let mut config = admission_endpoint_config();
    config.receive_datagram_limit(1472).unwrap();
    config.receive_backing_meter(bytes::BackingMeter::with_budget(budget));
    Arc::new(config)
}
fn apply_control(
    endpoint: &mut Endpoint,
    ch: ConnectionHandle,
    event: EndpointEvent,
    connection: &mut Connection,
) -> crate::EndpointEventOutcome {
    endpoint.handle_connection_event(ch, event, connection)
}

#[test]
fn initial_native_identifier_install_is_zero_allocation_through_actual_receiver() {
    let budget = cold_fixture_budget(3);
    let mut endpoint = Endpoint::try_new(config(&budget), None, true, None).unwrap();
    let now = Instant::now();
    let (ch, mut conn) = endpoint
        .connect(
            now,
            client_config(),
            "[::1]:4433".parse().unwrap(),
            "localhost",
        )
        .unwrap();
    let mut accepted = false;
    let allocations = allocations_in(|| {
        let event = EndpointEvent(
            crate::shared::EndpointEventInner::NeedIdentifiers(now, 7),
            conn.registration_key(),
        );
        accepted = apply_control(&mut endpoint, ch, event, &mut conn).disposition
            == crate::EndpointEventDisposition::Accepted;
    });
    let stock = conn.active_local_cid_seq();
    let root_stock = endpoint.assert_registration_inventory_for_probe();
    conn.abandon();
    endpoint.handle_event(ch, conn.take_drained_event().unwrap());
    drop(conn);
    drop(endpoint);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
    assert!(accepted);
    assert_eq!(stock, (0, 7));
    assert_eq!(root_stock, (1, 8, 0));
    // Owner maximum only: a cold bootstrap has not negotiated eight with a peer.
    assert_eq!(
        allocations, 0,
        "Root publication and actual Native CID state install must stay within cold owners"
    );
}

#[test]
fn encrypted_retire_rejects_foreign_actual_receiver_pool_before_root_publication() {
    let budget = cold_fixture_budget(6);
    let mut pair = Pair::new(config(&budget), server_config());
    let (client, peer) = control_records::connect(&mut pair);
    let (other, _) = control_records::connect(&mut pair);
    let (_, retire) = pair.client_conn_mut(client).active_local_cid_seq();
    let next = pair
        .client
        .endpoint
        .next_identifier_sequence_for_probe(client);
    let before = (
        next,
        pair.client
            .endpoint
            .routes_local_cid_for_probe(client, retire),
        pair.client
            .endpoint
            .routes_local_cid_for_probe(client, next),
    );
    let now = pair.time;
    pair.server_conn_mut(peer)
        .queue_repeated_retirement_for_capacity_probe(retire, 1);
    pair.server.drive_outgoing(now);
    for (_, wire) in pair.server.outbound.drain(..) {
        pair.client
            .inbound
            .push_back((now, None, BytesMut::from(wire.as_ref())));
    }
    pair.client.drive_incoming(now, pair.server.addr);
    pair.client.process_connection_events(client);
    let event = pair.client_conn_mut(client).poll_endpoint_events().unwrap();
    assert!(matches!(
        event.0,
        crate::shared::EndpointEventInner::RetireConnectionId(..)
    ));
    // Explicit inventory corruption after a genuine encrypted peer retirement.
    let held_original_pool = pair
        .client
        .endpoint
        .replace_identifier_pool_for_probe(client, other);
    let result = apply_control(
        &mut pair.client.endpoint,
        client,
        event,
        pair.client.connections.get_mut(&client).unwrap(),
    );
    let after = (
        pair.client
            .endpoint
            .next_identifier_sequence_for_probe(client),
        pair.client
            .endpoint
            .routes_local_cid_for_probe(client, retire),
        pair.client
            .endpoint
            .routes_local_cid_for_probe(client, next),
    );
    let rejected = result.disposition == crate::EndpointEventDisposition::Rejected;
    assert!(pair.client_conn_mut(other).retained_error().is_none());
    drop(result);
    drop(held_original_pool);
    drop(pair);
    assert!(budget.quarantined());
    assert_eq!(budget.used(), 1);
    assert_eq!(
        after, before,
        "foreign install authority cannot leave a Root route/sequence prefix"
    );
    assert!(rejected);
}

#[test]
fn stale_control_cannot_close_the_actual_same_slot_successor() {
    let budget = cold_fixture_budget(3);
    let mut endpoint = Endpoint::try_new(config(&budget), None, true, None).unwrap();
    let now = Instant::now();
    let (ch, mut first) = endpoint.connect(now, client_config(), "[::1]:4433".parse().unwrap(), "localhost").unwrap();
    let delayed = EndpointEvent(crate::shared::EndpointEventInner::NeedIdentifiers(now, 1), first.registration_key());
    first.abandon();
    endpoint.handle_event(ch, first.take_drained_event().unwrap());
    drop(first);
    let (reused, mut successor) = endpoint.connect(now, client_config(), "[::1]:4433".parse().unwrap(), "localhost").unwrap();
    assert_eq!(ch, reused);
    let before = endpoint.assert_registration_inventory_for_probe();
    let outcome = endpoint.handle_connection_event(reused, delayed, &mut successor);
    let disposition = outcome.disposition;
    let still_open = !successor.is_closed();
    let after = endpoint.assert_registration_inventory_for_probe();
    successor.abandon();
    endpoint.handle_event(reused, successor.take_drained_event().unwrap());
    drop((outcome, successor, endpoint));
    assert_eq!(disposition, crate::EndpointEventDisposition::Stale);
    assert!(still_open);
    assert_eq!(after, before);
    assert!(!budget.quarantined());
    assert_eq!(budget.used(), 0);
}

#[test]
fn old_native_control_entry_cannot_consume_a_control_cleanly() {
    let budget = cold_fixture_budget(3);
    let mut endpoint = Endpoint::try_new(config(&budget), None, true, None).unwrap();
    let now = Instant::now();
    let (ch, conn) = endpoint.connect(now, client_config(), "[::1]:4433".parse().unwrap(), "localhost").unwrap();
    let before = endpoint.assert_registration_inventory_for_probe();
    let reply = endpoint.handle_event(ch, EndpointEvent(crate::shared::EndpointEventInner::NeedIdentifiers(now, 1), conn.registration_key()));
    let after = endpoint.assert_registration_inventory_for_probe();
    let immediate_q = budget.quarantined();
    drop((reply, conn, endpoint));
    assert_eq!(after, before);
    assert!(immediate_q, "a consumed Native control without its receiver cannot disappear cleanly");
    assert_eq!(budget.used(), 1);
}
