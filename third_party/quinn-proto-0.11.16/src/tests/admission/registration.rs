//! Original Endpoint rejection must not publish an ownerless CID route.
use super::*;

#[test]
fn tls_prepare_rejection_preserves_registration_and_original_inventory() {
    let budget = cold_fixture_budget(2);
    let mut config = admission_endpoint_config();
    config.receive_datagram_limit(1472).unwrap();
    config.receive_backing_meter(bytes::BackingMeter::with_budget(&budget));
    let mut endpoint = Endpoint::new(Arc::new(config), None, true, None);
    let baseline = (
        endpoint.open_connections(),
        endpoint.known_cids(),
        budget.used(),
    );
    for attempt in 0..3 {
        let result = endpoint.connect(
            Instant::now(),
            client_config(),
            "[::1]:4433".parse().unwrap(),
            "bad server name",
        );
        assert!(matches!(result, Err(ConnectError::InvalidServerName(_))));
        let after = (
            endpoint.open_connections(),
            endpoint.known_cids(),
            budget.used(),
        );
        eprintln!(
            "registration-reject: attempt={attempt} before={baseline:?} after={after:?} Q={}",
            budget.quarantined()
        );
        assert_eq!(
            after, baseline,
            "TLS rejection cannot publish a route without its Meta owner"
        );
        assert!(!budget.quarantined());
    }
    let now = Instant::now();
    let (handle, mut conn) = endpoint
        .connect(
            now,
            client_config(),
            "[::1]:4433".parse().unwrap(),
            "localhost",
        )
        .unwrap();
    let live = (
        endpoint.open_connections(),
        endpoint.known_cids(),
        budget.used(),
    );
    assert_eq!((live.0, live.1), (1, 1));
    for attempt in 0..3 {
        let result = endpoint.connect(
            now,
            client_config(),
            "[::1]:4433".parse().unwrap(),
            "bad server name",
        );
        assert!(matches!(result, Err(ConnectError::InvalidServerName(_))));
        assert_eq!(
            (
                endpoint.open_connections(),
                endpoint.known_cids(),
                budget.used()
            ),
            live
        );
        assert!(
            !conn.is_closed(),
            "rejected preparation cannot close the existing connection"
        );
        eprintln!(
            "registration-live-reject: attempt={attempt} live={live:?} Q={}",
            budget.quarantined()
        );
    }
    let held = budget.try_reserve(budget.limit() - budget.used()).unwrap();
    let full = endpoint.connect(
        now,
        client_config(),
        "[::1]:4433".parse().unwrap(),
        "localhost",
    );
    assert!(matches!(full, Err(ConnectError::EventCapacityUnavailable)));
    assert_eq!((endpoint.open_connections(), endpoint.known_cids()), (1, 1));
    assert_eq!(budget.used(), budget.limit());
    drop(held);
    assert_eq!(budget.used(), live.2);
    let mut wire = Vec::new();
    assert!(
        conn.poll_transmit(now, 1, &mut wire).is_some(),
        "the original unpublished connection must still encode its actual Initial"
    );
    assert!(!wire.is_empty());
    // This owner was never published. Use its existing bootstrap abandon,
    // then consume the original Endpoint Drained event; never fabricate one.
    conn.abandon();
    assert!(conn.is_drained());
    let mut drained = 0;
    while let Some(event) = conn.poll_endpoint_events() {
        if matches!(event.0, crate::shared::EndpointEventInner::Drained) {
            drained += 1;
        }
        if let Some(reply) = endpoint.handle_event(handle, event) {
            conn.handle_event(reply);
        }
    }
    assert_eq!(drained, 1);
    assert_eq!((endpoint.open_connections(), endpoint.known_cids()), (0, 0));
    assert!(
        budget.used() > 0,
        "destroying route is not destroying Connection inventory"
    );
    drop(conn);
    assert_eq!(budget.used(), baseline.2);
    assert!(!budget.quarantined());
    eprintln!("registration: existing_Initial=true TLS_reject_preserves=true Full_preserves=true actual_unpublished_Drained=1 routes=0 last_ref_budget=0 Q=false");
    drop(endpoint);
    assert_eq!(budget.used(), 0);
}

#[test]
fn tls_rejection_preserves_established_original_peer_routing() {
    let budget = cold_fixture_budget(3);
    let mut config = admission_endpoint_config();
    config.receive_datagram_limit(1472).unwrap();
    config.receive_backing_meter(bytes::BackingMeter::with_budget(&budget));
    let mut pair = Pair::new(Arc::new(config), server_config());
    let (client, server) = pair.connect();
    let baseline = (
        pair.client.endpoint.open_connections(),
        pair.client.endpoint.known_cids(),
        budget.used(),
    );
    for _ in 0..3 {
        let result = pair.client.endpoint.connect(
            pair.time,
            client_config(),
            pair.server.addr,
            "bad server name",
        );
        assert!(matches!(result, Err(ConnectError::InvalidServerName(_))));
        assert_eq!(
            (
                pair.client.endpoint.open_connections(),
                pair.client.endpoint.known_cids(),
                budget.used()
            ),
            baseline
        );
    }
    let payload: Vec<u8> = (0..8192)
        .map(|n| ((n * 37 + n / 253) % 251) as u8)
        .collect();
    let incoming = pair
        .server_conn_mut(server)
        .streams()
        .open(Dir::Uni)
        .unwrap();
    assert_eq!(
        pair.server_send(server, incoming).write(&payload).unwrap(),
        payload.len()
    );
    pair.server_send(server, incoming).finish().unwrap();
    assert!(!pair.drive_bounded());
    let mut receive = pair.client_recv(client, incoming);
    let mut chunks = receive.read(true).unwrap();
    let mut actual = Vec::new();
    while let Some(chunk) = chunks.next(16 * 1024).unwrap() {
        actual.extend_from_slice(&chunk.bytes);
    }
    chunks.finalize();
    assert_eq!(actual, payload);
    assert!(pair.client_conn_mut(client).retained_error().is_none());
    assert!(pair.server_conn_mut(server).retained_error().is_none());
    let now = pair.time;
    pair.client_conn_mut(client)
        .close(now, VarInt::from_u32(0), Bytes::new());
    assert!(!pair.drive_bounded());
    assert!(pair.client_conn_mut(client).is_drained());
    assert!(pair.server_conn_mut(server).is_drained());
    assert_eq!(pair.client.endpoint.open_connections(), 0);
    assert_eq!(pair.client.endpoint.known_cids(), 0);
    assert_eq!(pair.server.endpoint.open_connections(), 0);
    assert_eq!(pair.server.endpoint.known_cids(), 0);
    drop(pair);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
    eprintln!("registration-established: original_peer_encrypted_bytes=8192 exact=true TLS_rejections=3 original_close_drains=2 meta_routes=0 final_budget=0 Q=false");
}

#[test]
fn real_two_connection_registration_is_bidirectional_through_rejection_and_drain() {
    let budget = cold_fixture_budget(6);
    let mut config = admission_endpoint_config();
    config.receive_datagram_limit(1472).unwrap();
    config.receive_backing_meter(bytes::BackingMeter::with_budget(&budget));
    let mut pair = Pair::new(Arc::new(config), server_config());
    let (first, _) = control_records::connect(&mut pair);
    let single = pair
        .client
        .endpoint
        .assert_registration_inventory_for_probe();
    assert_eq!((single.0, single.2), (1, 1));
    assert!((1..=crate::LOC_CID_COUNT as usize).contains(&single.1));
    let (second, _) = control_records::connect(&mut pair);
    let full = pair
        .client
        .endpoint
        .assert_registration_inventory_for_probe();
    assert_eq!(full, (2, single.1 * 2, 2));
    let refused = pair.client.endpoint.connect(
        pair.time,
        client_config(),
        pair.server.addr,
        "bad server name",
    );
    assert!(matches!(refused, Err(ConnectError::InvalidServerName(_))));
    assert_eq!(
        pair.client
            .endpoint
            .assert_registration_inventory_for_probe(),
        full
    );
    let now = pair.time;
    pair.client_conn_mut(first)
        .close(now, 0u32.into(), Bytes::new());
    assert!(!pair.drive_bounded());
    assert_eq!(
        pair.client
            .endpoint
            .assert_registration_inventory_for_probe(),
        single
    );
    control_records::stop(&mut pair, &[second]);
    assert_eq!(
        pair.client
            .endpoint
            .assert_registration_inventory_for_probe(),
        (0, 0, 0)
    );
    drop(pair);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn replayed_old_terminal_event_cannot_remove_a_reused_registration() {
    // Explicit caller replay fault. The original terminal event comes from a
    // real unpublished Connection abandon; this is not natural packet loss.
    let budget = cold_fixture_budget(3);
    let mut config = admission_endpoint_config();
    config.receive_datagram_limit(1472).unwrap();
    config.receive_backing_meter(bytes::BackingMeter::with_budget(&budget));
    let mut endpoint = Endpoint::new(Arc::new(config), None, true, None);
    let now = Instant::now();
    let remote = "[::1]:4433".parse().unwrap();
    let (old, mut first) = endpoint
        .connect(now, client_config(), remote, "localhost")
        .unwrap();
    first.abandon();
    let mut replay = None;
    while let Some(event) = first.poll_endpoint_events() {
        if event.is_drained() {
            replay = Some(crate::shared::EndpointEvent(event.0.clone(), event.1));
        }
        endpoint.handle_event(old, event);
    }
    assert_eq!(endpoint.open_connections(), 0);
    let (current, mut successor) = endpoint
        .connect(now, client_config(), remote, "localhost")
        .unwrap();
    assert_eq!(old, current, "the old physical slot was actually reused");
    let before = endpoint.assert_registration_inventory_for_probe();
    assert_eq!(before, (1, 1, 0));
    endpoint.handle_event(old, replay.unwrap());
    assert_eq!(
        endpoint.assert_registration_inventory_for_probe(),
        before,
        "old registration identity must not remove successor Meta/routes"
    );
    successor.abandon();
    while let Some(event) = successor.poll_endpoint_events() {
        endpoint.handle_event(current, event);
    }
    drop(first);
    drop(successor);
    drop(endpoint);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn paced_registration_first_path_migration_uses_a_separate_generation_dimension() {
    let budget = cold_fixture_budget(4);
    let mut config = admission_endpoint_config();
    config.receive_datagram_limit(1472).unwrap();
    config.receive_backing_meter(bytes::BackingMeter::with_budget(&budget));
    config.endpoint_pacing_service(Some(EndpointPacingServiceConfig::endpoint_window_v1()));
    let mut pair = Pair::new(Arc::new(config), server_config());
    let (client, server) = control_records::connect(&mut pair);
    let before = pair.server_conn_mut(server).stats().path.rtt;
    let new_port = pair.client.addr.port().checked_add(1).unwrap();
    pair.client.addr.set_port(new_port); // true simulated NAT rebind, original encrypted packets
    pair.client_conn_mut(client).ping();
    assert!(!pair.drive_bounded());
    assert!(pair.server_conn_mut(server).retained_error().is_none());
    assert!(pair.client_conn_mut(client).retained_error().is_none());
    assert_eq!(
        pair.server_conn_mut(server).remote_address(),
        pair.client.addr
    );
    assert!(pair.server_conn_mut(server).stats().path.rtt >= before.min(pair.latency));
    control_records::stop(&mut pair, &[client]);
    drop(pair);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn proto_retirement_alone_is_not_host_settlement() {
    let budget = cold_fixture_budget(2);
    let mut config = admission_endpoint_config();
    config.receive_datagram_limit(1472).unwrap();
    config.receive_backing_meter(bytes::BackingMeter::with_budget(&budget));
    let mut endpoint = Endpoint::new(Arc::new(config), None, true, None);
    let shared_client = client_config();
    let (handle, mut conn) = endpoint
        .connect(
            Instant::now(),
            shared_client.clone(),
            "[::1]:4433".parse().unwrap(),
            "localhost",
        )
        .unwrap();
    let key = conn.registration_key();
    conn.abandon();
    let event = conn.take_drained_event().unwrap();
    let observer = endpoint.registration_observer().unwrap();
    let mut outcome = endpoint.handle_event_checked(handle, event);
    assert!(outcome.terminal);
    assert!(
        !endpoint.registration_was_settled(key),
        "Proto retirement precedes Host catalog/key disposal and cannot ACK it"
    );
    assert!(!observer.was_settled(key));
    let (other_handle, mut other) = endpoint
        .connect(
            Instant::now(),
            shared_client.clone(),
            "[::1]:4433".parse().unwrap(),
            "localhost",
        )
        .unwrap();
    assert_ne!(
        other_handle, handle,
        "only the unresolved slot is blocked; unrelated free slots progress"
    );
    assert!(!observer.was_settled(key));
    other.abandon();
    endpoint.handle_event(other_handle, other.take_drained_event().unwrap());
    drop(other);
    outcome.finish_settlement();
    assert!(observer.was_settled(key));
    let (reused, mut successor) = endpoint
        .connect(
            Instant::now(),
            shared_client,
            "[::1]:4433".parse().unwrap(),
            "localhost",
        )
        .unwrap();
    assert_eq!(
        reused, handle,
        "the original physical slot is reusable only after Host completion"
    );
    successor.abandon();
    endpoint.handle_event(reused, successor.take_drained_event().unwrap());
    drop(successor);
    drop(endpoint);
    drop(conn);
    assert!(
        budget.used() > 0,
        "observer owns the fixed ledger after Endpoint destruction"
    );
    assert!(observer.was_settled(key));
    drop(outcome);
    drop(observer);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn lost_host_settlement_retains_quarantine_through_last_owner() {
    let budget = cold_fixture_budget(2);
    let mut config = admission_endpoint_config();
    config.receive_datagram_limit(1472).unwrap();
    config.receive_backing_meter(bytes::BackingMeter::with_budget(&budget));
    let mut endpoint = Endpoint::try_new(Arc::new(config), None, true, None).unwrap();
    let (handle, mut conn) = endpoint
        .connect(
            Instant::now(),
            client_config(),
            "[::1]:4433".parse().unwrap(),
            "localhost",
        )
        .unwrap();
    let key = conn.registration_key();
    let observer = endpoint.registration_observer().unwrap();
    conn.abandon();
    let outcome = endpoint.handle_event_checked(handle, conn.take_drained_event().unwrap());
    assert!(outcome.terminal);
    assert!(!observer.was_settled(key));
    drop(outcome); // The Host did not dispose its catalog or publish completion.
    assert!(budget.quarantined() && budget.used() > 0);
    assert!(!observer.was_settled(key));
    drop(endpoint);
    drop(conn);
    assert!(
        budget.used() > 1,
        "the physical ledger is still retained by its observer"
    );
    drop(observer);
    assert_eq!(budget.used(), 1);
    assert!(budget.quarantined());
}

#[test]
fn native_constructor_refuses_cold_layout_without_downgrading_profile() {
    let budget = bytes::BackingBudget::new(1).unwrap();
    let mut config = admission_endpoint_config();
    config.receive_backing_meter(bytes::BackingMeter::with_budget(&budget));
    let config = Arc::new(config);
    assert!(Endpoint::try_new(config.clone(), None, true, None).is_err());
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
    // The infallible compatibility API retains a fail-closed Native profile.
    let mut endpoint = Endpoint::new(config, None, true, None);
    assert!(endpoint.native_registration());
    assert!(endpoint
        .try_set_server_config(Some(Arc::new(server_config())))
        .is_err());
    assert!(matches!(
        endpoint.connect(
            Instant::now(),
            client_config(),
            "[::1]:4433".parse().unwrap(),
            "localhost"
        ),
        Err(ConnectError::EventCapacityUnavailable)
    ));
    assert_eq!(endpoint.open_connections(), 0);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn endpoint_root_loss_quarantines_before_observer_or_connection_drop() {
    let budget = cold_fixture_budget(2);
    let mut config = admission_endpoint_config();
    config.receive_datagram_limit(1472).unwrap();
    config.receive_backing_meter(bytes::BackingMeter::with_budget(&budget));
    let endpoint_config = Arc::new(config);
    let mut endpoint = Endpoint::try_new(endpoint_config, None, true, None).unwrap();
    let (_, conn) = endpoint
        .connect(
            Instant::now(),
            client_config(),
            "[::1]:4433".parse().unwrap(),
            "localhost",
        )
        .unwrap();
    let observer = endpoint.registration_observer().unwrap();
    let live_connection_bytes = budget.used() - Endpoint::native_registration_layout_bytes();
    assert!(!budget.quarantined());
    drop(endpoint); // Root is lost while the original Connection and observer still live.
    assert!(
        budget.quarantined(),
        "Root loss is the uncertainty boundary, not last observer destruction"
    );
    assert!(
        budget.used()>live_connection_bytes,
        "Root table destruction refunds only that actual Box; Connection and observer remain charged"
    );
    drop(conn);
    assert!(budget.used() > 1);
    drop(observer);
    assert_eq!(budget.used(), 1);
}
