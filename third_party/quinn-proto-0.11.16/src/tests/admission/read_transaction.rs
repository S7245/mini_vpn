//! Actual encrypted stream custody across a rejected read preparation.
use super::*;

#[test]
fn rejected_read_preparation_keeps_quarantined_backing_in_original_owner() {
    let budget = cold_fixture_budget(2);
    let meter = bytes::BackingMeter::with_budget(&budget);
    let mut config = admission_endpoint_config();
    config.receive_datagram_limit(1472).unwrap();
    config.receive_backing_meter(meter.clone());
    let mut pair = Pair::new(Arc::new(config), server_config());
    let (client, server) = pair.connect();
    let stream = pair
        .client_conn_mut(client)
        .streams()
        .open(Dir::Uni)
        .unwrap();
    assert_eq!(pair.client_send(client, stream).write(b"x").unwrap(), 1);
    assert!(!pair.drive_bounded());
    let before = pair.server_recv(server, stream).progress().unwrap();
    assert_eq!(before.buffered_bytes, 1);
    // Explicit fault after actual encrypted receive. This is not natural Full,
    // a successful drain, or permission to recover a quarantined authority.
    let retained = send_buffer_inventory::retained_layouts(&pair);
    meter.quarantine();
    let used = budget.used();
    assert!(matches!(
        pair.server_recv(server, stream).read(false),
        Err(crate::ReadableError::BackingAuthorityLost)
    ));
    let after = pair.server_recv(server, stream).progress();
    assert!(
        after.is_ok(),
        "failed preparation dropped quarantined receive inventory: {after:?}"
    );
    let after = after.unwrap();
    assert_eq!(after.read_offset, before.read_offset);
    assert_eq!(after.buffered_bytes, before.buffered_bytes);
    assert_eq!(after.next_received_offset, before.next_received_offset);
    assert_eq!(
        after.highest_received_offset,
        before.highest_received_offset
    );
    assert_eq!(budget.used(), used);
    assert!(budget.quarantined());
    drop(pair);
    retained.assert_quarantined(&budget);
    assert!(budget.quarantined());
    let charged = retained.charged;
    eprintln!("read-prepare-Q: actual_STREAM=1 injected_authority_Q=true same_recv_retained=true buffered=1 known_drop_then_exact_native_retained={charged}; not_a_clean_join=true");
}

#[test]
fn rejected_ordering_keeps_original_receive_inventory_and_can_finish() {
    let budget = cold_fixture_budget(2);
    let mut config = admission_endpoint_config();
    config.receive_datagram_limit(1472).unwrap();
    config.receive_backing_meter(bytes::BackingMeter::with_budget(&budget));
    let mut pair = Pair::new(Arc::new(config), server_config());
    let (client, server) = pair.connect();
    let stream = pair
        .client_conn_mut(client)
        .streams()
        .open(Dir::Uni)
        .unwrap();
    let payload: [u8; 1024] = std::array::from_fn(|i| ((i * 37 + i / 256) % 251) as u8);
    assert_eq!(
        pair.client_send(client, stream).write(&payload).unwrap(),
        payload.len()
    );
    pair.client_send(client, stream).finish().unwrap();
    assert!(!pair.drive_bounded());
    let mut received = Vec::new();
    {
        let mut recv = pair.server_recv(server, stream);
        let mut chunks = recv.read(false).unwrap();
        let first = chunks.next(128).unwrap().unwrap();
        assert_eq!(first.offset, 0);
        received.extend_from_slice(&first.bytes);
        assert_eq!(received.len(), 128);
        let _ = chunks.finalize();
    }
    let before = pair.server_recv(server, stream).progress().unwrap();
    let used = budget.used();
    for _ in 0..2 {
        assert!(matches!(
            pair.server_recv(server, stream).read(true),
            Err(crate::ReadableError::IllegalOrderedRead)
        ));
        let after = pair.server_recv(server, stream).progress();
        assert!(
            after.is_ok(),
            "rejected preparation removed the live receive owner: {after:?}"
        );
        let after = after.unwrap();
        assert_eq!(after.read_offset, before.read_offset);
        assert_eq!(after.next_received_offset, before.next_received_offset);
        assert_eq!(
            after.highest_received_offset,
            before.highest_received_offset
        );
        assert_eq!(after.buffered_bytes, before.buffered_bytes);
        assert_eq!(after.ordered_gap_bytes, before.ordered_gap_bytes);
        assert_eq!(
            budget.used(),
            used,
            "rejection must not release retained backing"
        );
        assert!(!budget.quarantined());
    }
    {
        let mut recv = pair.server_recv(server, stream);
        let mut chunks = recv.read(false).unwrap();
        while let Some(chunk) = chunks.next(1024).unwrap() {
            assert_eq!(chunk.offset, received.len() as u64);
            received.extend_from_slice(&chunk.bytes);
        }
        let _ = chunks.finalize();
    }
    assert_eq!(received, payload);
    assert!(pair.client_conn_mut(client).retained_error().is_none());
    assert!(pair.server_conn_mut(server).retained_error().is_none());
    let now = pair.time;
    pair.client_conn_mut(client)
        .close(now, VarInt::from_u32(0), Bytes::new());
    assert!(!pair.drive_bounded());
    assert!(pair.client_conn_mut(client).is_drained());
    assert!(pair.server_conn_mut(server).is_drained());
    assert_eq!(pair.client.endpoint.open_connections(), 0);
    assert_eq!(pair.server.endpoint.open_connections(), 0);
    assert_eq!(pair.client.endpoint.known_cids(), 0);
    assert_eq!(pair.server.endpoint.known_cids(), 0);
    drop(pair);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
    eprintln!("read-prepare: real_STREAM=1024 unordered_prefix=128 rejects=2 remaining=896 exact_delivery=1024 original_drains=2 maps=0 final_budget=0 Q=false; full_metadata=OPEN");
}
