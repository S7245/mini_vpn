//! CID wire exposure, record custody and actual ACK phase scenarios.
use super::*;

#[test]
fn peer_retirement_releases_route_before_original_cid_frame_ack() {
    for request_immediate in [false, true] {
        let budget = cold_fixture_budget(2);
        let meter = bytes::BackingMeter::with_budget(&budget);
        let mut config = admission_endpoint_config();
        config.receive_backing_meter(meter.clone());
        config.receive_datagram_limit(1472).unwrap();
        let mut pair = Pair::new(Arc::new(config), server_config());
        let (client_ch, server_ch) = pair.connect();
        let now = pair.time;
        // Retire an actually used peer CID through the original owner, so the
        // replacement respects the peer limit rather than issuing an extra CID.
        assert!(pair
            .server_conn_mut(server_ch)
            .try_local_address_changed(now));
        pair.server.drive_outgoing(now);
        let (tx, old_retirement) = pair.server.outbound.pop_front().unwrap();
        let old_retirement = old_retirement.slice(..tx.segment_size.unwrap_or(tx.size));
        let mut response = Vec::new();
        let Some(DatagramEvent::ConnectionEvent(handle, event)) = pair.client.endpoint.handle(
            now,
            pair.server.addr,
            None,
            None,
            BytesMut::try_copy_from_slice_metered(old_retirement.as_ref(), &meter).unwrap(),
            &mut response,
        ) else {
            panic!("original old CID retirement did not route")
        };
        assert_eq!(handle, client_ch);
        pair.client_conn_mut(client_ch).handle_event(event);
        while let Some(event) = pair.client_conn_mut(client_ch).poll_endpoint_events() {
            pair.client.endpoint.handle_connection_event(client_ch, event, pair.client.connections.get_mut(&client_ch).unwrap());
        }
        let sequence = pair.client_conn_mut(client_ch).active_local_cid_seq().1;
        assert_eq!(
            pair.client_conn_mut(client_ch)
                .cid_frame_stock_for_probe(sequence),
            (1, 0)
        );

        // Pre-encode the peer stimulus before it can ACK the new CID packet.
        // Deliver it only after the peer actually receives that original packet.
        // This separates RETIRE from ACK without editing authenticated payloads.
        pair.server_conn_mut(server_ch)
            .queue_repeated_retirement_for_capacity_probe(sequence, 1);
        pair.server.drive_outgoing(now);
        let (tx, retirement) = pair.server.outbound.pop_front().unwrap();
        let retirement = retirement.slice(..tx.segment_size.unwrap_or(tx.size));
        if request_immediate {
            pair.client_conn_mut(client_ch).immediate_ack();
        }
        pair.client.drive_outgoing(now);
        assert_eq!(
            pair.client_conn_mut(client_ch)
                .cid_frame_stock_for_probe(sequence),
            (0, 1)
        );
        assert_eq!(
            pair.client_conn_mut(client_ch)
                .cid_general_boxes_for_probe(sequence),
            0,
            "CID-only send must not allocate the general retransmission box"
        );
        let (tx, issued) = pair.client.outbound.pop_front().unwrap();
        let issued = issued.slice(..tx.segment_size.unwrap_or(tx.size));
        let Some(DatagramEvent::ConnectionEvent(handle, event)) = pair.server.endpoint.handle(
            now,
            pair.client.addr,
            None,
            None,
            BytesMut::try_copy_from_slice_metered(issued.as_ref(), &meter).unwrap(),
            &mut response,
        ) else {
            panic!("original CID packet did not route")
        };
        assert_eq!(handle, server_ch);
        let before = pair
            .server_conn_mut(server_ch)
            .stats()
            .frame_rx
            .new_connection_id;
        pair.server_conn_mut(server_ch).handle_event(event);
        assert!(
            pair.server_conn_mut(server_ch)
                .stats()
                .frame_rx
                .new_connection_id
                > before
        );
        let Some(DatagramEvent::ConnectionEvent(handle, event)) = pair.client.endpoint.handle(
            now,
            pair.server.addr,
            None,
            None,
            BytesMut::try_copy_from_slice_metered(retirement.as_ref(), &meter).unwrap(),
            &mut response,
        ) else {
            panic!("original retirement packet did not route")
        };
        assert_eq!(handle, client_ch);
        pair.client_conn_mut(client_ch).handle_event(event);
        let mut retired = false;
        let mut unpublished_replies = Vec::new();
        while let Some(event) = pair.client_conn_mut(client_ch).poll_endpoint_events() {
            let is_target = matches!(event.0,
            crate::shared::EndpointEventInner::RetireConnectionId(_, seq, _) if seq == sequence);
            let duplicate = is_target.then(|| EndpointEvent(event.0.clone(), event.1));
            pair.client.endpoint.handle_connection_event(client_ch, event, pair.client.connections.get_mut(&client_ch).unwrap());
            if is_target {
                // Explicit custody fault after the real receiver installation:
                // retain that exact replacement F rather than transmit another
                // synthetic peer retirement's CID back to the unmodified peer.
                unpublished_replies.push(pair.client_conn_mut(client_ch).take_pending_cid_reply_for_probe(now).unwrap());
            }
            if let Some(duplicate) = duplicate {
                assert!(
                    pair.client.endpoint.handle_connection_event(client_ch, duplicate,
                        pair.client.connections.get_mut(&client_ch).unwrap()).response.is_none(),
                    "the original Endpoint must have actually removed the route"
                );
                retired = true;
            }
        }
        assert!(retired);
        assert_eq!(
            pair.client_conn_mut(client_ch)
                .cid_frame_stock_for_probe(sequence),
            (0, 1),
            "route retirement is not proof that the sent CID frame storage was released"
        );
        assert_eq!(unpublished_replies.len(), 1);
        assert!(
            pair.server.inbound.is_empty(),
            "only the original peer timer may advance this phase"
        );
        let before_ack = pair.client_conn_mut(client_ch).stats();
        pair.server.drive_outgoing(now);
        assert!(pair.client_conn_mut(client_ch).retained_error().is_none());
        assert!(pair.server_conn_mut(server_ch).retained_error().is_none());
        // Consume already-generated ACKs before consulting a later timer. Only
        // this Pair clock advances; the client never transmits the held reply.
        let loss_deadline = pair
            .client_conn_mut(client_ch)
            .loss_detection_deadline()
            .unwrap();
        for _ in 0..100 {
            // The existing Pair harness observation guard, not a protocol bound.
            assert!(pair.time < loss_deadline);
            while let Some((tx, packet)) = pair.server.outbound.pop_front() {
                let packet = packet.slice(..tx.segment_size.unwrap_or(tx.size));
                let Some(DatagramEvent::ConnectionEvent(handle, event)) =
                    pair.client.endpoint.handle(
                        pair.time,
                        pair.server.addr,
                        None,
                        None,
                        BytesMut::try_copy_from_slice_metered(packet.as_ref(), &meter).unwrap(),
                        &mut response,
                    )
                else {
                    panic!("original peer ACK did not route")
                };
                assert_eq!(handle, client_ch);
                pair.client_conn_mut(client_ch).handle_event(event);
            }

            if pair.client_conn_mut(client_ch).stats().frame_rx.acks > before_ack.frame_rx.acks {
                break;
            }
            assert!(pair.client_conn_mut(client_ch).retained_error().is_none());
            assert!(pair.server_conn_mut(server_ch).retained_error().is_none());
            let wakeup = pair
                .server
                .next_wakeup()
                .expect("original peer wakeup before ACK");
            let previous = pair.time;
            pair.time = pair.time.max(wakeup);
            assert!(
                pair.time < loss_deadline,
                "original loss boundary precedes peer ACK"
            );
            pair.server.drive_outgoing(pair.time);
            assert!(
                pair.time > previous
                    || !pair.server.outbound.is_empty()
                    || pair.server.next_wakeup() != Some(wakeup),
                "peer wakeup made no progress"
            );
        }
        let after_ack = pair.client_conn_mut(client_ch).stats();
        assert!(after_ack.frame_rx.acks > before_ack.frame_rx.acks);
        assert_eq!(after_ack.path.lost_packets, before_ack.path.lost_packets);
        assert!(!pair.client_conn_mut(client_ch).is_closed());
        assert!(!pair.server_conn_mut(server_ch).is_closed());
        assert!(pair.client_conn_mut(client_ch).retained_error().is_none());
        assert!(pair.server_conn_mut(server_ch).retained_error().is_none());
        assert_eq!(
            pair.client_conn_mut(client_ch)
                .cid_frame_stock_for_probe(sequence),
            (0, 0),
            "the original peer ACK must actually release the retained frame"
        );
        eprintln!("cid-stock: seq={sequence} route_removed=true frame_before_ack=1 frame_after_ack=0 new_acks={} new_losses=0",
        after_ack.frame_rx.acks - before_ack.frame_rx.acks);
        control_records::stop(&mut pair, &[client_ch]);
        drop(pair);
        assert!(
            budget.used() > 0,
            "the original unpublished reply still owns F backing"
        );
        drop(unpublished_replies);
        assert_eq!(budget.used(), 0);
        assert!(!budget.quarantined());
    }
}

#[test]
fn encrypted_retirement_of_unencoded_cid_preserves_registration_and_frame_stock() {
    let mut pair = Pair::new(Arc::new(admission_endpoint_config()), server_config());
    let (client_ch, server_ch) = pair.connect();
    let now = pair.time;
    assert!(pair
        .server_conn_mut(server_ch)
        .try_local_address_changed(now));
    pair.server.drive_outgoing(now);
    let (tx, packet) = pair.server.outbound.pop_front().unwrap();
    let packet = packet.slice(..tx.segment_size.unwrap_or(tx.size));
    let mut response = Vec::new();
    let Some(DatagramEvent::ConnectionEvent(handle, event)) = pair.client.endpoint.handle(
        now,
        pair.server.addr,
        None,
        None,
        BytesMut::from(packet.as_ref()),
        &mut response,
    ) else {
        panic!("original retirement did not route")
    };
    assert_eq!(handle, client_ch);
    pair.client_conn_mut(client_ch).handle_event(event);
    while let Some(event) = pair.client_conn_mut(client_ch).poll_endpoint_events() {
        pair.client.endpoint.handle_connection_event(client_ch, event, pair.client.connections.get_mut(&client_ch).unwrap());
    }
    // The original Endpoint has registered the replacement, but the client
    // writer has not run. The peer guesses its monotonically increasing seq.
    let sequence = pair.client_conn_mut(client_ch).active_local_cid_seq().1;
    assert!(pair
        .client
        .endpoint
        .routes_local_cid_for_probe(client_ch, sequence));
    assert_eq!(
        pair.client_conn_mut(client_ch)
            .cid_frame_stock_for_probe(sequence),
        (1, 0)
    );
    let active_before = pair.client_conn_mut(client_ch).active_local_cid_seq();
    pair.server_conn_mut(server_ch)
        .queue_repeated_retirement_for_capacity_probe(sequence, 1);
    pair.server.drive_outgoing(now);
    let (tx, packet) = pair.server.outbound.pop_front().unwrap();
    let packet = packet.slice(..tx.segment_size.unwrap_or(tx.size));
    let Some(DatagramEvent::ConnectionEvent(handle, event)) = pair.client.endpoint.handle(
        now,
        pair.server.addr,
        None,
        None,
        BytesMut::from(packet.as_ref()),
        &mut response,
    ) else {
        panic!("encrypted guessed retirement did not route")
    };
    assert_eq!(handle, client_ch);
    pair.client_conn_mut(client_ch).handle_event(event);
    assert!(
        matches!(pair.client_conn_mut(client_ch).retained_error(),
        Some(ConnectionError::TransportError(error)) if error.code == TransportErrorCode::PROTOCOL_VIOLATION),
        "never-encoded future sequence must fail before retiring registered CID state"
    );
    assert!(pair
        .client_conn_mut(client_ch)
        .poll_endpoint_events()
        .is_none());
    assert_eq!(
        pair.client_conn_mut(client_ch).active_local_cid_seq(),
        active_before
    );
    assert!(pair
        .client
        .endpoint
        .routes_local_cid_for_probe(client_ch, sequence));
    assert_eq!(
        pair.client_conn_mut(client_ch)
            .cid_frame_stock_for_probe(sequence),
        (1, 0)
    );
}

#[test]
fn foreign_cold_reply_is_rejected_before_protocol_state_changes() {
    let mut owners = Vec::new();
    for _ in 0..2 {
        let budget = cold_fixture_budget(2);
        let meter = bytes::BackingMeter::with_budget(&budget);
        let mut config = admission_endpoint_config();
        config.receive_backing_meter(meter);
        config.receive_datagram_limit(1472).unwrap();
        let mut pair = Pair::new(Arc::new(config), server_config());
        let (client, _) = pair.connect();
        owners.push((budget, pair, client));
    }
    let (left, right) = owners.split_at_mut(1);
    let (left_budget, receiver, receiver_ch) = &mut left[0];
    let (right_budget, issuer, issuer_ch) = &mut right[0];
    let before = receiver
        .client_conn_mut(*receiver_ch)
        .active_local_cid_seq();
    let issuer_key = issuer.client_conn_mut(*issuer_ch).registration_key();
    issuer.client.endpoint.handle_connection_event(
        *issuer_ch,
        EndpointEvent(crate::shared::EndpointEventInner::NeedIdentifiers(issuer.time, 1), issuer_key),
        issuer.client.connections.get_mut(issuer_ch).unwrap(),
    );
    // Deliberately move the actual installed F to the foreign receiver. This
    // retains the old negative ownership assertion without a production bypass.
    let issued_at = issuer.time;
    let reply = issuer.client_conn_mut(*issuer_ch).take_pending_cid_reply_for_probe(issued_at).unwrap();
    let sequence = before.1 + 1;
    assert!(!receiver
        .client
        .endpoint
        .routes_local_cid_for_probe(*receiver_ch, sequence));
    assert!(issuer
        .client
        .endpoint
        .routes_local_cid_for_probe(*issuer_ch, sequence));
    receiver.client_conn_mut(*receiver_ch).handle_event(reply);
    assert_eq!(
        receiver
            .client_conn_mut(*receiver_ch)
            .active_local_cid_seq(),
        before
    );
    assert!(!receiver
        .client
        .endpoint
        .routes_local_cid_for_probe(*receiver_ch, sequence));
    assert!(issuer
        .client
        .endpoint
        .routes_local_cid_for_probe(*issuer_ch, sequence));
    assert_eq!(
        receiver
            .client_conn_mut(*receiver_ch)
            .cid_frame_stock_for_probe(sequence),
        (0, 0)
    );
    assert!(
        matches!(receiver.client_conn_mut(*receiver_ch).retained_error(),
        Some(ConnectionError::TransportError(error)) if error.reason == "CID frame authority mismatch")
    );
    assert!(left_budget.quarantined() && right_budget.quarantined());
    assert!(left_budget.used() > 1 && right_budget.used() > 1);
    for (budget, pair, _) in owners {
        drop(pair);
        assert_eq!(budget.used(), 1);
    }
}
