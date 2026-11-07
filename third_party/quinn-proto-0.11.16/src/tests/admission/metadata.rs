//! Characterizes real control and STREAM metadata allocations before replacement.
//! A passing characterization preserves evidence of the open hard-budget gap.
use super::{recovery::allocations_in, *};

#[test]
fn original_tls_output_can_grow_beyond_pre_reserved_vec_capacity() {
    // Direct provider API counterexample: this is deliberately not represented
    // as an admitted Connection or a completed hard-budget implementation.
    let crypto = client_crypto();
    let mut tls = rustls::quic::ClientConnection::new(
        crypto.inner,
        rustls::quic::Version::V1,
        rustls::pki_types::ServerName::try_from("localhost").unwrap(),
        Vec::new(),
    )
    .unwrap();
    let budget = bytes::BackingBudget::new(1).unwrap();
    let reservation = budget.try_reserve(1).unwrap();
    let mut output = Vec::with_capacity(1);
    assert_eq!(output.capacity(), 1);
    let mut keys = None;
    let allocations = allocations_in(|| keys = tls.write_hs(&mut output));
    assert!(keys.is_none());
    assert!(!output.is_empty());
    assert!(output.capacity() > 1);
    assert_eq!(budget.used(), 1);
    assert!(allocations > 0);
    let first_len = output.len();
    let actual_capacity = output.capacity();
    let mut second = Vec::new();
    assert!(tls.write_hs(&mut second).is_none());
    assert!(
        second.is_empty(),
        "first call consumed the queued ClientHello"
    );
    drop(output);
    drop(reservation);
    assert_eq!(budget.used(), 0);
    eprintln!("tls-output-interface-counterexample: pre_reserved=1 actual_capacity={actual_capacity} output_len={first_len} alloc_or_realloc={allocations} second_output=0; no_fallible_limit_parameter=true; not_a_budgeted_Connection=true");
}

#[test]
fn original_reset_and_two_stream_packet_metadata_characterization() {
    let budget = cold_fixture_budget(2);
    let mut config = admission_endpoint_config();
    config.receive_datagram_limit(1472).unwrap();
    config.receive_backing_meter(bytes::BackingMeter::with_budget(&budget));
    let mut server = server_config();
    Arc::get_mut(&mut server.transport)
        .unwrap()
        .mtu_discovery_config(None);
    let mut client = client_config_with_deterministic_pns();
    Arc::get_mut(&mut client.transport)
        .unwrap()
        .mtu_discovery_config(None);
    let mut pair = Pair::new(Arc::new(config), server);
    let (ch, peer) = pair.connect_with(client);
    let reset = pair.client_conn_mut(ch).streams().open(Dir::Uni).unwrap();
    // Materialize the original per-stream instance outside metadata counting.
    pair.client_send(ch, reset).set_priority(0).unwrap();
    let mut result = None;
    let producer_allocations = allocations_in(|| {
        result = Some(pair.client_send(ch, reset).reset(42u32.into()));
    });
    result.unwrap().unwrap();
    let mut ids = Vec::new();
    for byte in [0x5a, 0xa5] {
        let id = pair.client_conn_mut(ch).streams().open(Dir::Uni).unwrap();
        assert_eq!(pair.client_send(ch, id).write(&[byte; 128]).unwrap(), 128);
        pair.client_send(ch, id).finish().unwrap();
        ids.push((id, byte));
    }
    // The original caller's wire capacity is outside the measured metadata scope.
    let mut wire = Vec::with_capacity(64 * 1472);
    let before = pair.client_conn_mut(ch).stats().frame_tx;
    let now = pair.time;
    let mut transmit = None;
    let packet_allocations = allocations_in(|| {
        transmit = pair.client_conn_mut(ch).poll_transmit(now, 1, &mut wire);
    });
    let transmit = transmit.expect("original mixed encrypted packet");
    assert_eq!(transmit.size, wire.len());
    let after = pair.client_conn_mut(ch).stats().frame_tx;
    assert_eq!(after.reset_stream - before.reset_stream, 1);
    assert_eq!(after.stream - before.stream, 2);
    let before_rx = pair.server_conn_mut(peer).stats().frame_rx;
    pair.client
        .outbound
        .push_back((transmit, Bytes::from(wire)));
    assert!(!pair.drive_bounded());
    let after_rx = pair.server_conn_mut(peer).stats().frame_rx;
    assert_eq!(after_rx.reset_stream - before_rx.reset_stream, 1);
    assert_eq!(after_rx.stream - before_rx.stream, 2);
    for (id, byte) in ids {
        let mut receive = pair.server_recv(peer, id);
        let mut chunks = receive.read(true).unwrap();
        let mut actual = Vec::new();
        while let Some(chunk) = chunks.next(1024).unwrap() {
            actual.extend_from_slice(&chunk.bytes);
        }
        chunks.finalize();
        assert_eq!(actual, [byte; 128]);
    }
    assert!(pair.client_conn_mut(ch).retained_error().is_none());
    assert!(pair.server_conn_mut(peer).retained_error().is_none());
    // These are explicit old-behavior observations, not the eventual zero-allocation gate.

    assert_eq!(
        packet_allocations, 0,
        "cold mixed metadata must use preowned records"
    );
    let now = pair.time;
    pair.client_conn_mut(ch)
        .close(now, VarInt::from_u32(0), Bytes::new());
    assert!(!pair.drive_bounded());
    assert!(pair.client_conn_mut(ch).is_drained());
    assert!(pair.server_conn_mut(peer).is_drained());
    assert_eq!(pair.client.endpoint.open_connections(), 0);
    assert_eq!(pair.client.endpoint.known_cids(), 0);
    assert_eq!(pair.server.endpoint.open_connections(), 0);
    assert_eq!(pair.server.endpoint.known_cids(), 0);
    drop(pair);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
    assert_eq!(
        producer_allocations, 0,
        "existing Send RESET metadata must use cold records"
    );
    eprintln!("metadata-characterization: real_RESET=1 real_STREAM=2 exact_payloads=2 producer_allocations={producer_allocations} packet_allocations={packet_allocations} original_close_drains=2 endpoint_routes=0 final_budget=0 Q=false; complete_hard_inventory=OPEN");
}
