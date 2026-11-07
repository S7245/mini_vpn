//! Actual original encrypted ACK/loss processing; test-only thread allocation scope.
use super::*;
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
};

struct ScopedAllocator;
thread_local! {
    static TRACK: Cell<bool> = const { Cell::new(false) };
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
    static FREED_BYTES: Cell<usize> = const { Cell::new(0) };
    static FAIL_AT: Cell<Option<usize>> = const { Cell::new(None) };
}
#[global_allocator]
static ALLOCATOR: ScopedAllocator = ScopedAllocator;
unsafe impl GlobalAlloc for ScopedAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if count_allocation() {
            return std::ptr::null_mut();
        }
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if count_allocation() {
            return std::ptr::null_mut();
        }
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        if count_allocation() {
            return std::ptr::null_mut();
        }
        unsafe { System.realloc(ptr, layout, size) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        let _ = TRACK.try_with(|track| {
            if track.get() {
                let _ = FREED_BYTES.try_with(|n| n.set(n.get() + layout.size()));
            }
        });
        unsafe { System.dealloc(ptr, layout) }
    }
}
fn count_allocation() -> bool {
    TRACK
        .try_with(|track| {
            if track.get() {
                return ALLOCATIONS
                    .try_with(|n| {
                        let call = n.get() + 1;
                        n.set(call);
                        FAIL_AT
                            .try_with(|fail| fail.get() == Some(call))
                            .unwrap_or(false)
                    })
                    .unwrap_or(false);
            }
            false
        })
        .unwrap_or(false)
}
pub(crate) fn allocator_failure_in(call: usize, f: impl FnOnce()) {
    FAIL_AT.with(|n| n.set(Some(call)));
    allocations_in(f);
}
pub(crate) fn allocations_in(f: impl FnOnce()) -> usize {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            TRACK.with(|t| t.set(false));
            FAIL_AT.with(|n| n.set(None));
        }
    }
    ALLOCATIONS.with(|n| n.set(0));
    FREED_BYTES.with(|n| n.set(0));
    TRACK.with(|t| t.set(true));
    let reset = Reset;
    f();
    drop(reset);
    ALLOCATIONS.with(Cell::get)
}
pub(crate) fn freed_bytes_in(f: impl FnOnce()) -> usize {
    allocations_in(f);
    FREED_BYTES.with(Cell::get)
}
fn deliver(from: &mut TestEndpoint, to: &mut TestEndpoint, now: Instant) -> usize {
    let n = from.outbound.len();
    for (_, bytes) in from.outbound.drain(..) {
        to.inbound
            .push_back((now, None, BytesMut::from(bytes.as_ref())));
    }
    n
}

#[test]
fn encrypted_ack_loss_selection_uses_only_preowned_inventory() {
    let budget = cold_fixture_budget(2);
    let mut config = admission_endpoint_config();
    config.receive_datagram_limit(1472).unwrap();
    config.receive_backing_meter(bytes::BackingMeter::with_budget(&budget));
    let mut server = server_config();
    let mut transport = TransportConfig::default();
    transport
        .deterministic_packet_numbers(true)
        .mtu_discovery_config(None);
    server.transport = Arc::new(transport);
    let mut client = client_config_with_deterministic_pns();
    Arc::get_mut(&mut client.transport)
        .unwrap()
        .mtu_discovery_config(None);
    let mut pair = Pair::new(Arc::new(config), server);
    let (ch, peer) = pair.connect_with(client);
    let before = pair.client_conn_mut(ch).stats();
    for ordinal in 0..4 {
        pair.client_conn_mut(ch).ping();
        pair.client.drive_outgoing(pair.time);
        assert!(!pair.client.outbound.is_empty());
        if ordinal == 0 {
            pair.client.outbound.clear(); // Entire original first PING datagram lost.
        } else {
            assert!(deliver(&mut pair.client, &mut pair.server, pair.time) > 0);
        }
    }
    pair.server.drive_incoming(pair.time, pair.client.addr);
    pair.server.process_connection_events(peer);
    pair.server.drive_outgoing(pair.time);
    assert!(deliver(&mut pair.server, &mut pair.client, pair.time) > 0);
    pair.client.drive_incoming(pair.time, pair.server.addr);
    let count = allocations_in(|| pair.client.process_connection_events(ch));
    let after = pair.client_conn_mut(ch).stats();
    assert!(after.frame_rx.acks > before.frame_rx.acks);
    assert_eq!(after.path.lost_packets - before.path.lost_packets, 1);
    assert!(pair.client_conn_mut(ch).retained_error().is_none());
    eprintln!(
        "recovery-allocation: actual_ACK=true actual_lost_PING=1 alloc_or_realloc={count} Q={}",
        budget.quarantined()
    );
    assert_eq!(
        count, 0,
        "original encrypted ACK/loss must use preowned recovery inventory"
    );
    assert!(!budget.quarantined());
    control_records::stop(&mut pair, &[ch]);
    drop(pair);
    assert_eq!(budget.used(), 0);
}

#[test]
fn sparse_original_ping_receive_keeps_65_range_peak_cold() {
    let budget = cold_fixture_budget(2);
    let mut config = admission_endpoint_config();
    config.receive_datagram_limit(1472).unwrap();
    config.receive_backing_meter(bytes::BackingMeter::with_budget(&budget));
    let mut server = server_config();
    let mut transport = TransportConfig::default();
    transport
        .deterministic_packet_numbers(true)
        .mtu_discovery_config(None);
    server.transport = Arc::new(transport);
    let mut client = client_config_with_deterministic_pns();
    Arc::get_mut(&mut client.transport)
        .unwrap()
        .mtu_discovery_config(None);
    let mut pair = Pair::new(Arc::new(config), server);
    let (ch, peer) = pair.connect_with(client);
    // The first production key phase is randomly 10..1000 packets. Complete a
    // real update before measuring ranges: TLS key objects have their separate
    // instance/configuration contract, not a zero-allocation contract here.
    let prior_phase = pair.server_conn_mut(peer).key_epoch_for_recovery_probe().0;
    pair.server_conn_mut(peer).force_key_update();
    let send_epoch = pair.server_conn_mut(peer).key_epoch_for_recovery_probe();
    assert_ne!(send_epoch.0, prior_phase, "original update must execute");
    let before_update_ping = pair.client_conn_mut(ch).stats().frame_rx.ping;
    pair.server_conn_mut(peer).ping();
    assert!(!pair.drive_bounded());
    assert_eq!(
        pair.client_conn_mut(ch).stats().frame_rx.ping,
        before_update_ping + 1
    );
    assert!(pair.client_conn_mut(ch).retained_error().is_none());
    assert!(pair.server_conn_mut(peer).retained_error().is_none());
    let receive_epoch = pair.client_conn_mut(ch).key_epoch_for_recovery_probe();
    assert_eq!(
        receive_epoch.0, send_epoch.0,
        "peer must authenticate new keys"
    );
    assert!(send_epoch.1 > 130 && receive_epoch.1 > 130);
    let before = pair.client_conn_mut(ch).stats().frame_rx.ping;
    let mut datagrams = 0;
    for ordinal in 0..130 {
        pair.server_conn_mut(peer).ping();
        pair.server.drive_outgoing(pair.time);
        assert!(!pair.server.outbound.is_empty());
        if ordinal % 2 == 0 {
            datagrams += deliver(&mut pair.server, &mut pair.client, pair.time);
        } else {
            pair.server.outbound.clear();
        }
    }
    assert_eq!(datagrams, 65);
    pair.client.drive_incoming(pair.time, pair.server.addr);
    let count = allocations_in(|| pair.client.process_connection_events(ch));
    assert_eq!(
        pair.server_conn_mut(peer).key_epoch_for_recovery_probe(),
        send_epoch
    );
    assert_eq!(
        pair.client_conn_mut(ch).key_epoch_for_recovery_probe(),
        receive_epoch
    );
    assert_eq!(pair.client_conn_mut(ch).stats().frame_rx.ping - before, 65);
    assert_eq!(
        pair.client_conn_mut(ch)
            .pending_ack_ranges_for_recovery_probe(crate::packet::SpaceId::Data),
        (64, Some(65), 65)
    );
    assert!(pair.client_conn_mut(ch).retained_error().is_none());
    eprintln!(
        "pending-ACK-range: actual_sparse_PING=65 final_ranges=64 alloc_or_realloc={count} Q={}",
        budget.quarantined()
    );
    assert_eq!(
        count, 0,
        "persistent ACK range storage must be admitted cold before packets"
    );
    assert!(!budget.quarantined());
    control_records::stop(&mut pair, &[ch]);
    drop(pair);
    assert_eq!(budget.used(), 0);
}

#[test]
fn actual_retry_preserves_all_three_cold_ack_range_owners() {
    let budget = cold_fixture_budget(2);
    let mut config = admission_endpoint_config();
    config.receive_datagram_limit(1472).unwrap();
    config.receive_backing_meter(bytes::BackingMeter::with_budget(&budget));
    let mut pair = Pair::new(Arc::new(config), server_config());
    let retries = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed = retries.clone();
    pair.server.handle_incoming = Box::new(move |incoming| {
        if incoming.remote_address_validated() {
            IncomingConnectionBehavior::Accept
        } else {
            observed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            IncomingConnectionBehavior::Retry
        }
    });
    let client = pair.begin_connect(client_config());
    for space in crate::packet::SpaceId::iter() {
        assert_eq!(
            pair.client_conn_mut(client)
                .pending_ack_ranges_for_recovery_probe(space)
                .1,
            Some(65)
        );
    }
    assert!(!pair.drive_bounded());
    let server = pair.server.assert_accept();
    assert_eq!(retries.load(std::sync::atomic::Ordering::Relaxed), 1);
    assert!(pair.client_conn_mut(client).retained_error().is_none());
    for space in crate::packet::SpaceId::iter() {
        assert_eq!(
            pair.client_conn_mut(client)
                .pending_ack_ranges_for_recovery_probe(space)
                .1,
            Some(65)
        );
        assert_eq!(
            pair.server_conn_mut(server)
                .pending_ack_ranges_for_recovery_probe(space)
                .1,
            Some(65)
        );
    }
    let now = pair.time;
    pair.client_conn_mut(client)
        .close(now, VarInt::from_u32(0), Bytes::new());
    assert!(!pair.drive_bounded());
    assert!(pair.client_conn_mut(client).is_drained());
    assert!(pair.server_conn_mut(server).is_drained());
    assert_eq!(pair.client.endpoint.open_connections(), 0);
    assert_eq!(pair.server.endpoint.open_connections(), 0);
    drop(pair);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
    eprintln!("cold-ACK-Retry: actual_Retry=1 client_three_spaces=65 server_three_spaces=65 original_close_drain=true final_budget=0 Q=false");
}

/// Fault injection in physical inventory metadata, followed by real encrypted
/// packets. This is not a natural pressure or whole-Service acceptance claim.
#[test]
fn cold_range_shape_fault_stops_current_and_already_owned_stream_packets() {
    let budget = cold_fixture_budget(2);
    let mut config = admission_endpoint_config();
    config.receive_datagram_limit(1472).unwrap();
    config.receive_backing_meter(bytes::BackingMeter::with_budget(&budget));
    let mut pair = Pair::new(Arc::new(config), server_config());
    let (ch, peer) = pair.connect();
    assert!(pair
        .client_conn_mut(ch)
        .corrupt_ack_range_shape_for_recovery_probe());
    let before = pair.client_conn_mut(ch).stats().frame_rx.stream;
    pair.server_conn_mut(peer).ping();
    pair.server.drive_outgoing(pair.time);
    assert!(!pair.server.outbound.is_empty());
    pair.server.outbound.clear(); // Original whole datagram loss leaves a gap.
    for byte in [0x51, 0xa7] {
        let stream = pair.server_conn_mut(peer).streams().open(Dir::Uni).unwrap();
        assert_eq!(
            pair.server_send(peer, stream).write(&[byte; 128]).unwrap(),
            128
        );
        pair.server.drive_outgoing(pair.time);
        assert!(deliver(&mut pair.server, &mut pair.client, pair.time) > 0);
    }
    // Both ciphertext packets enter original Endpoint custody before the first
    // fault. No unowned post-Q copy/packet is manufactured or refunded.
    pair.client.drive_incoming(pair.time, pair.server.addr);
    let retained = send_buffer_inventory::retained_layouts(&pair);
    assert!(pair.client.process_one_connection_event(ch));
    let first = pair.client_conn_mut(ch).retained_error();
    let actual = pair.client_conn_mut(ch).stats().frame_rx.stream - before;
    eprintln!(
        "cold-range-fault-first: stream_frames_applied={actual} first={first:?} Q={}",
        budget.quarantined()
    );
    assert_eq!(
        actual, 0,
        "physical ACK inventory fault must stop before DATA frame processing"
    );
    let first = first.expect("ACK inventory failure must retain its typed error");
    assert!(matches!(&first, ConnectionError::TransportError(error)
        if error.code == TransportErrorCode::INTERNAL_ERROR
        && error.reason == "ACK range custody invariant failed"));
    assert!(pair.client.process_one_connection_event(ch));
    pair.client.process_connection_events(ch);
    assert_eq!(pair.client_conn_mut(ch).stats().frame_rx.stream - before, 0);
    assert_eq!(
        pair.client_conn_mut(ch)
            .retained_error()
            .unwrap()
            .to_string(),
        first.to_string()
    );
    assert!(budget.quarantined());
    assert!(budget.used() > 1);
    drop(pair);
    retained.assert_quarantined(&budget);
    let charged = retained.charged;
    eprintln!("cold-range-fault: first_and_owned_tail_STREAM=0 first_value_retained=true final_exact_native_charged_Q={charged}");
}

#[test]
fn original_close_error_survives_actual_peer_stateless_reset() {
    let budget = cold_fixture_budget(2);
    let mut config = admission_endpoint_config();
    config.receive_datagram_limit(1472).unwrap();
    config.receive_backing_meter(bytes::BackingMeter::with_budget(&budget));
    // An original Endpoint restart with the same persisted CID/reset keys.
    config.cid_generator(|| Box::new(HashedConnectionIdGenerator::from_key(0)));
    let config = Arc::new(config);
    let mut pair = Pair::new(config.clone(), server_config());
    let (ch, peer) = pair.connect();
    let now = pair.time;
    pair.server_conn_mut(peer)
        .close(now, VarInt::from_u32(42), Bytes::from(vec![0xab; 128]));
    pair.server.drive_outgoing(now);
    assert!(deliver(&mut pair.server, &mut pair.client, now) > 0);
    pair.client.drive_incoming(now, pair.server.addr);
    pair.client.process_connection_events(ch);
    let first = pair.client_conn_mut(ch).retained_error().unwrap();
    assert!(
        matches!(&first, ConnectionError::ApplicationClosed(error) if error.error_code == VarInt::from_u32(42))
    );
    assert!(pair.client_conn_mut(ch).is_closed());
    assert!(!pair.client_conn_mut(ch).is_drained());
    // Only the remote Endpoint is restarted. Keep the client's real original
    // connection and its first error; no client state/error/token is injected.
    pair.server.endpoint = Endpoint::new(config, Some(Arc::new(server_config())), true, None);
    pair.client.drive_outgoing(now);
    assert!(deliver(&mut pair.client, &mut pair.server, now) > 0);
    pair.server.drive_incoming(now, pair.client.addr);
    assert_eq!(
        pair.server.outbound.len(),
        1,
        "original fresh Endpoint must emit the reset response"
    );
    assert_eq!(deliver(&mut pair.server, &mut pair.client, now), 1);
    pair.client.drive_incoming(now, pair.server.addr);
    pair.client.process_connection_events(ch);
    let after = pair.client_conn_mut(ch).retained_error().unwrap();
    assert!(
        pair.client_conn_mut(ch).is_drained(),
        "actual stateless reset still advances transport disposal"
    );
    assert_eq!(
        pair.time, now,
        "no timer expiry can substitute for the reset witness"
    );
    eprintln!("packet-first-error: first={first:?} after_original_Endpoint_reset={after:?} reset_drained=true same_clock=true Q={}",budget.quarantined());
    assert_eq!(
        after.to_string(),
        first.to_string(),
        "later reset must not overwrite original Close cause"
    );
    assert!(!budget.quarantined());
    // Apply the actual reset Drained event before destroying the Endpoint owner.
    pair.client.drive_outgoing(now);
    drop(pair);
    let retained = budget.used();
    assert!(
        retained > 0,
        "owned first/error observations still hold original reason backing"
    );
    drop(after);
    assert_eq!(
        budget.used(),
        retained,
        "both error observations share one original backing"
    );
    drop(first);
    assert_eq!(
        budget.used(),
        0,
        "last original reason reference actually releases backing"
    );
    eprintln!("first-error-backing: after_Pair={retained} after_one_snapshot={retained} last_snapshot=0 Q=false");
}
