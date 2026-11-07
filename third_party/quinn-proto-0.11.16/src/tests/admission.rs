use super::*;
mod send_buffer_inventory;
mod receive_settlement;
mod packet_pressure;

/// Fixed endpoint RNG seed for every admission fixture (D21). Unseeded, each
/// connection draws from SysRng: random initial key lifetimes (10..1000) and
/// optimistic-ACK PN skips then intermittently change the packet shape under
/// test. A fixed seed keeps each case reproducible; RNG coverage stays with
/// the upstream suites.
const ADMISSION_RNG_SEED: [u8; 32] = *b"mini_vpn-n1-admission-fixture-01";
fn admission_endpoint_config() -> EndpointConfig {
    let mut config = EndpointConfig::default();
    config.rng_seed(Some(ADMISSION_RNG_SEED));
    config
}

// Actual cold owner layouts plus bounded test encrypted in-flight headroom.
// Fixed owner headers/Arc instances and harness Vecs are not Slot layout bytes.
fn cold_fixture_budget(connections: usize) -> bytes::BackingBudget {
    cold_fixture_budget_for_endpoints(connections, 1)
}
fn cold_fixture_budget_for_endpoints(connections: usize, endpoints: usize) -> bytes::BackingBudget {
    let mut config = admission_endpoint_config();
    config.receive_datagram_limit(1472).unwrap();
    let per = crate::connection::ColdResources::layout_bytes(&config, None).unwrap();
    bytes::BackingBudget::new(
        per * connections + Endpoint::native_registration_layout_bytes() * endpoints + 64 * 1024,
    )
    .unwrap()
}

#[test]
fn cold_mixed_wire_frames_consume_actual_operation_credit_without_quarantine() {
    for key_update in [false, true] {
        let budget = cold_fixture_budget(2);
        let meter = bytes::BackingMeter::with_budget(&budget);
        let mut config = admission_endpoint_config();
        config.receive_backing_meter(meter.clone());
        config.receive_datagram_limit(1472).unwrap();
        let mut pair = Pair::new(Arc::new(config), server_config());
        let client_ch = pair.begin_connect(client_config());
        assert!(
            !pair.drive_bounded(),
            "cold handshake must settle using the receive authority"
        );
        let server_ch = pair.server.assert_accept();
        assert!(!meter.snapshot().quarantined);
        let (low, high) = pair.client_conn_mut(client_ch).active_local_cid_seq();
        let now = pair.time;
        pair.server_conn_mut(server_ch).rotate_local_cid(1, now);
        while let Some(event) = pair.server_conn_mut(server_ch).poll_endpoint_events() {
            if let Some(reply) = pair.server.endpoint.handle_event(server_ch, event) {
                pair.server_conn_mut(server_ch).handle_event(reply);
            }
        }
        pair.server_conn_mut(server_ch)
            .queue_repeated_retirement_for_capacity_probe(low, 10);
        pair.server_conn_mut(server_ch)
            .queue_repeated_retirement_for_capacity_probe(high, 10);
        for _ in 0..3 {
            let stream = pair
                .server_conn_mut(server_ch)
                .streams()
                .open(Dir::Uni)
                .unwrap();
            pair.server_send(server_ch, stream)
                .write(&[0x5a; 96])
                .unwrap();
        }
        if key_update {
            pair.server_conn_mut(server_ch).force_key_update();
        }
        pair.server.drive_outgoing(pair.time);
        let (transmit, packet) = pair.server.outbound.pop_front().unwrap();
        let packet = packet.slice(..transmit.segment_size.unwrap_or(transmit.size));
        let mut response = Vec::new();
        let Some(DatagramEvent::ConnectionEvent(handle, event)) = pair.client.endpoint.handle(
            pair.time,
            pair.server.addr,
            None,
            None,
            BytesMut::try_copy_from_slice_metered(&packet, &meter).unwrap(),
            &mut response,
        ) else {
            panic!("mixed encrypted packet did not route")
        };
        assert_eq!(handle, client_ch);
        let conn = pair.client_conn_mut(client_ch);
        let before = conn.stats().frame_rx;
        conn.handle_event_with_admission(event, |_| Some(()))
            .unwrap();
        let after = conn.stats().frame_rx;
        assert!(after.new_connection_id > before.new_connection_id);
        assert_eq!(after.retire_connection_id - before.retire_connection_id, 20);
        assert!(after.stream > before.stream);
        let mut events = Vec::new();
        while let Some(event) = conn.poll_endpoint_events() {
            events.push(event);
        }
        eprintln!(
            "cold-mixed: L={} ordinary={} new_cid={} retire={} stream={} key_update={}",
            packet.len(),
            events.len(),
            after.new_connection_id - before.new_connection_id,
            after.retire_connection_id - before.retire_connection_id,
            after.stream - before.stream,
            key_update
        );
        assert!(events.len() >= 2);
        assert!(events.len() <= packet.len() + 8);
        assert!(!conn.event_inventory_quarantined());
        assert!(!conn.is_closed());
        for event in events {
            pair.client.endpoint.handle_connection_event(client_ch, event,
                pair.client.connections.get_mut(&client_ch).unwrap());
        }
        assert!(!budget.quarantined());
        control_records::stop(&mut pair, &[client_ch]);
        drop(pair);
        assert_eq!(budget.used(), 0);
    }
}

#[test]
fn cold_authenticated_migration_keeps_event_credit_and_releases_stock() {
    let budget = cold_fixture_budget(2);
    let meter = bytes::BackingMeter::with_budget(&budget);
    let mut config = admission_endpoint_config();
    config.receive_backing_meter(meter.clone());
    config.receive_datagram_limit(1472).unwrap();
    let mut pair = Pair::new(Arc::new(config), server_config());
    pair.begin_connect(client_config());
    assert!(!pair.drive_bounded());
    let server_ch = pair.server.assert_accept();
    let client_ch = *pair.client.connections.keys().next().unwrap();
    let generation = pair.server_conn_mut(server_ch).current_path_generation();
    pair.client
        .addr
        .set_port(pair.client.addr.port().checked_add(10_000).unwrap());
    pair.client_conn_mut(client_ch).ping();
    assert!(!pair.drive_bounded());
    assert!(pair.server_conn_mut(server_ch).current_path_generation() > generation);
    assert!(!pair.server_conn_mut(server_ch).is_closed());
    assert!(!pair.client_conn_mut(client_ch).is_closed());
    assert!(!meter.snapshot().quarantined);
    control_records::stop(&mut pair, &[client_ch]);
    drop(pair);
    assert_eq!(budget.used(), 0);
}

#[test]
fn buffered_accept_replay_bound_mismatch_rejects_unpublished_connection() {
    let mut pair = Pair::new(Arc::new(admission_endpoint_config()), server_config());
    pair.begin_connect(client_config());
    pair.client.drive_outgoing(pair.time);
    let (transmit, bytes) = pair.client.outbound.pop_front().unwrap();
    let initial = bytes.slice(..transmit.segment_size.unwrap_or(transmit.size));
    let budget = bytes::BackingBudget::new(512 * 1024).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let mut config = admission_endpoint_config();
    config.receive_backing_meter(meter.clone());
    config.receive_datagram_limit(initial.len()).unwrap();
    pair.server.endpoint = Endpoint::new(
        Arc::new(config),
        Some(Arc::new(server_config())),
        true,
        None,
    );
    let mut response = Vec::new();
    let Some(DatagramEvent::NewConnection(incoming)) = pair.server.endpoint.handle(
        pair.time,
        pair.client.addr,
        None,
        None,
        BytesMut::try_copy_from_slice_metered(&initial, &meter).unwrap(),
        &mut response,
    ) else {
        panic!("real client Initial did not create incoming")
    };
    // Deliberately violate the declared I/O bound with two original encrypted
    // Initial packets in one buffered datagram. This is an adversarial rejection
    // tracer, not a claim that the Native client profile reaches this branch.
    let mut coalesced = initial.to_vec();
    coalesced.extend_from_slice(&initial);
    assert!(pair
        .server
        .endpoint
        .handle(
            pair.time,
            pair.client.addr,
            None,
            None,
            BytesMut::try_copy_from_slice_metered(&coalesced, &meter).unwrap(),
            &mut response
        )
        .is_none());
    assert!(budget.used() > 0);
    let result = pair
        .server
        .endpoint
        .accept(incoming, pair.time, &mut response, None);
    assert!(
        result.is_err(),
        "rejected buffered event must not publish a successful accept"
    );
    assert_eq!(pair.server.endpoint.open_connections(), 0);
    assert_eq!(
        budget.used(),
        0,
        "unpublished connection and input stock must actually drop"
    );
    assert!(
        !meter.snapshot().quarantined,
        "complete unpublished rollback is proved here"
    );
}

#[test]
fn cold_event_layout_is_admitted_before_endpoint_cid_registration() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct CountCids {
        count: Arc<AtomicUsize>,
        inner: RandomConnectionIdGenerator,
    }
    impl ConnectionIdGenerator for CountCids {
        fn generate_cid(&mut self) -> ConnectionId {
            self.count.fetch_add(1, Ordering::Relaxed);
            self.inner.generate_cid()
        }
        fn cid_len(&self) -> usize {
            self.inner.cid_len()
        }
        fn cid_lifetime(&self) -> Option<Duration> {
            self.inner.cid_lifetime()
        }
    }
    let layout =
        crate::connection::ColdResources::layout_bytes(&admission_endpoint_config(), None).unwrap();
    for (limit, held) in [(1, 0), (layout - 1, 0), (layout, 1), (4 * 1024 * 1024, 0)] {
        let budget = bytes::BackingBudget::new(limit).unwrap();
        let held_credit = budget.try_reserve(held).unwrap();
        let meter = bytes::BackingMeter::with_budget(&budget);
        let count = Arc::new(AtomicUsize::new(0));
        let counter = count.clone();
        let mut config = admission_endpoint_config();
        config.receive_backing_meter(meter);
        config.connection_id_generator_factory = Arc::new(move || {
            Box::new(CountCids {
                count: counter.clone(),
                inner: RandomConnectionIdGenerator::new(8),
            })
        });
        let mut endpoint = Endpoint::new(Arc::new(config), None, true, None);
        let endpoint_layout = budget.used() - held;
        let before = count.load(Ordering::Relaxed);
        let result = endpoint.connect(
            Instant::now(),
            client_config(),
            "[::1]:4433".parse().unwrap(),
            "localhost",
        );
        if limit - held - endpoint_layout < layout {
            assert!(matches!(
                result,
                Err(ConnectError::EventCapacityUnavailable)
            ));
            assert_eq!(count.load(Ordering::Relaxed), before);
            assert_eq!(endpoint.open_connections(), 0);
            assert_eq!(budget.used(), held + endpoint_layout);
        } else {
            let (handle, mut connection) = result.unwrap();
            assert!(count.load(Ordering::Relaxed) > before);
            assert_eq!(budget.used(), layout + endpoint_layout);
            connection.abandon();
            let terminal = connection.take_drained_event().unwrap();
            endpoint.handle_event(handle, terminal);
            drop(connection);
            assert_eq!(endpoint.open_connections(), 0);
            assert_eq!(budget.used(), held + endpoint_layout);
        }
        drop(endpoint);
        drop(held_credit);
        assert_eq!(budget.used(), 0);
        assert!(!budget.quarantined());
    }
}

#[test]
fn connection_rejection_returns_original_encrypted_event_before_key_or_ack_commit() {
    for key_update in [false, true] {
        let mut pair = Pair::new(Arc::new(admission_endpoint_config()), server_config());
        let (client_ch, server_ch) = pair.connect();
        if key_update {
            pair.server_conn_mut(server_ch).force_key_update();
        }
        pair.server_conn_mut(server_ch).ping();
        pair.server.drive_outgoing(pair.time);
        let (_, packet) = pair.server.outbound.pop_front().unwrap();
        let original_len = packet.len();
        let mut response = Vec::new();
        let Some(DatagramEvent::ConnectionEvent(handle, event)) = pair.client.endpoint.handle(
            pair.time,
            pair.server.addr,
            None,
            None,
            BytesMut::from(packet.as_ref()),
            &mut response,
        ) else {
            panic!("encrypted PING did not route")
        };
        assert_eq!(handle, client_ch);
        assert_eq!(event.datagram_len(), Some(original_len));
        let conn = pair.client_conn_mut(client_ch);
        let before_stats = format!("{:?}", conn.stats());
        let before_auth = conn.authenticated_packets();
        let before_timeout = conn.poll_timeout();
        let rejected = conn
            .handle_event_with_admission(event, |event| {
                assert_eq!(event.datagram_len(), Some(original_len));
                None::<u8>
            })
            .expect_err("admission must return the owned event");
        assert_eq!(format!("{:?}", conn.stats()), before_stats);
        assert_eq!(conn.authenticated_packets(), before_auth);
        assert_eq!(conn.poll_timeout(), before_timeout);
        assert!(conn.poll_endpoint_events().is_none());
        assert_eq!(
            conn.handle_event_with_admission(rejected, |_| Some(7u8))
                .unwrap(),
            7
        );
        assert_eq!(
            conn.authenticated_packets(),
            before_auth + 1,
            "same encrypted event must authenticate on first accepted commit"
        );
    }
}

#[test]
fn coalesced_handshake_rejection_precedes_both_packets_and_tls_commit() {
    let mut pair = Pair::new(Arc::new(admission_endpoint_config()), server_config());
    let client_ch = pair.begin_connect(client_config());
    pair.drive_client();
    pair.server.drive(pair.time, pair.client.addr);
    let (transmit, bytes) = pair.server.outbound.pop_front().unwrap();
    let udp = bytes.slice(..transmit.segment_size.unwrap_or(transmit.size));
    let mut response = Vec::new();
    let Some(DatagramEvent::ConnectionEvent(handle, event)) = pair.client.endpoint.handle(
        pair.time,
        pair.server.addr,
        None,
        None,
        BytesMut::from(udp.as_ref()),
        &mut response,
    ) else {
        panic!("server handshake did not route")
    };
    assert_eq!(handle, client_ch);
    assert!(
        matches!(&event.0, crate::shared::ConnectionEventInner::Datagram(datagram) if datagram.remaining.is_some()),
        "fixture must use real coalesced handshake packets"
    );
    assert_eq!(event.datagram_len(), Some(udp.len()));
    let conn = pair.client_conn_mut(client_ch);
    let before = (
        format!("{:?}", conn.stats()),
        conn.authenticated_packets(),
        conn.poll_timeout(),
    );
    let event = conn
        .handle_event_with_admission(event, |_| None::<()>)
        .unwrap_err();
    assert_eq!(
        (
            format!("{:?}", conn.stats()),
            conn.authenticated_packets(),
            conn.poll_timeout()
        ),
        before
    );
    assert!(conn.poll().is_none());
    conn.handle_event_with_admission(event, |_| Some(()))
        .unwrap();
    assert!(
        conn.authenticated_packets() >= before.1 + 2,
        "both original packets must authenticate after retry"
    );
}

#[test]
fn duplicate_retirement_frames_share_one_endpoint_settlement() {
    let mut pair = Pair::new(Arc::new(admission_endpoint_config()), server_config());
    let (client_ch, server_ch) = pair.connect();
    assert!(pair
        .client_conn_mut(client_ch)
        .poll_endpoint_events()
        .is_none());
    let sequence = pair.client_conn_mut(client_ch).active_local_cid_seq().1;
    pair.server_conn_mut(server_ch)
        .queue_repeated_retirement_for_capacity_probe(sequence, 96);
    pair.server.drive_outgoing(pair.time);
    let (_, packet) = pair
        .server
        .outbound
        .pop_front()
        .expect("encrypted retirement packet");
    eprintln!(
        "event-capacity: encrypted_datagram_bytes={} endpoint_event_layout={} repeated_retirements=96",
        packet.len(),
        std::mem::size_of::<crate::EndpointEvent>(),
    );
    let mut response = Vec::new();
    let event = pair.client.endpoint.handle(
        pair.time,
        pair.server.addr,
        None,
        None,
        BytesMut::from(packet.as_ref()),
        &mut response,
    );
    let Some(DatagramEvent::ConnectionEvent(handle, event)) = event else {
        panic!("retirement packet did not route to its live connection");
    };
    assert_eq!(handle, client_ch);
    let conn = pair.client_conn_mut(client_ch);
    conn.handle_event(event);
    let mut events = Vec::new();
    let mut retired = 0;
    while let Some(event) = conn.poll_endpoint_events() {
        if matches!(
            event.0,
            crate::shared::EndpointEventInner::RetireConnectionId(..)
        ) {
            retired += 1;
        }
        events.push(event);
    }
    let mut replies = 0;
    for event in events {
        if let Some(reply) = pair.client.endpoint.handle_event(client_ch, event) {
            replies += 1;
            pair.client_conn_mut(client_ch).handle_event(reply);
        }
    }
    assert_eq!(
        replies, 1,
        "original Endpoint retires and replenishes this CID exactly once"
    );
    assert_eq!(
        retired, 1,
        "duplicate frames must share the one effective retirement settlement"
    );
}

#[test]
fn mixed_retirement_duplicates_keep_first_effective_order_and_two_replies() {
    let mut pair = Pair::new(Arc::new(admission_endpoint_config()), server_config());
    let (client_ch, server_ch) = pair.connect();
    let (low, high) = pair.client_conn_mut(client_ch).active_local_cid_seq();
    assert_ne!(low, high);
    pair.server_conn_mut(server_ch)
        .queue_repeated_retirement_for_capacity_probe(low, 48);
    pair.server_conn_mut(server_ch)
        .queue_repeated_retirement_for_capacity_probe(high, 48);
    pair.server.drive_outgoing(pair.time);
    let (_, packet) = pair.server.outbound.pop_front().unwrap();
    let mut response = Vec::new();
    let Some(DatagramEvent::ConnectionEvent(handle, event)) = pair.client.endpoint.handle(
        pair.time,
        pair.server.addr,
        None,
        None,
        BytesMut::from(packet.as_ref()),
        &mut response,
    ) else {
        panic!("encrypted retirement packet did not route")
    };
    assert_eq!(handle, client_ch);
    pair.client_conn_mut(client_ch).handle_event(event);
    let mut events = Vec::new();
    let mut retired = Vec::new();
    while let Some(event) = pair.client_conn_mut(client_ch).poll_endpoint_events() {
        if let crate::shared::EndpointEventInner::RetireConnectionId(_, seq, _) = &event.0 {
            retired.push(*seq);
        }
        events.push(event);
    }
    assert_eq!(
        retired,
        [high, low],
        "original writer pops retirement frames from its stack"
    );
    let mut replies = 0;
    for event in events {
        if let Some(reply) = pair.client.endpoint.handle_event(client_ch, event) {
            replies += 1;
            pair.client_conn_mut(client_ch).handle_event(reply);
        }
    }
    assert_eq!(replies, 2);
    assert!(pair
        .client_conn_mut(client_ch)
        .poll_endpoint_events()
        .is_none());
}

#[test]
fn existing_connection_admission_rejects_before_event_custody() {
    let mut pair = Pair::new(Arc::new(admission_endpoint_config()), server_config());
    let (client_ch, server_ch) = pair.connect();

    pair.server_conn_mut(server_ch).ping();
    pair.server.drive_outgoing(pair.time);
    let (_, packet) = pair
        .server
        .outbound
        .pop_front()
        .expect("server emitted a real encrypted PING packet");
    let mut response = Vec::new();
    let mut routed = None;
    let event = pair.client.endpoint.handle_with_admission(
        pair.time,
        pair.server.addr,
        None,
        None,
        BytesMut::from(packet.as_ref()),
        &mut response,
        |handle| {
            routed = Some(handle);
            None::<u8>
        },
    );

    assert_eq!(routed, Some(client_ch));
    assert!(event.is_none());
    assert!(response.is_empty());
    assert!(pair.client_conn_mut(client_ch).poll().is_none());

    let accepted = pair.client.endpoint.handle_with_admission(
        pair.time,
        pair.server.addr,
        None,
        None,
        BytesMut::from(packet.as_ref()),
        &mut response,
        |handle| (handle == client_ch).then_some(7u8),
    );
    assert!(matches!(
        accepted,
        Some(AdmittedDatagram::Connection(handle, _, 7)) if handle == client_ch
    ));
}

mod cid;

mod cid_pressure;

mod sent_capacity;

mod sent_ack_only;

mod sent_gso;

mod registration;
mod registration_fixed;

mod recovery;
pub(crate) use recovery::{allocations_in, allocator_failure_in, freed_bytes_in};

mod metadata;

mod tls_output;

mod read_transaction;

mod stream_control;

mod control_records;

mod stream_records;

mod flow_credit;

mod crypto;

mod crypto_faults;

mod retire;

mod token_cache;

mod registration_prepared;

mod registration_prepared_unwind;
mod registration_reply;

mod endpoint_pacing;

mod datagram_inventory;
