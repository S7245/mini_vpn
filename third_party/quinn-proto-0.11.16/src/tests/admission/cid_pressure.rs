//! Full CID records through honest encrypted rotations, original timers and ACKs.
use super::*;

// Match the real budgeted Quinn driver: process authenticated input, settle
// original Endpoint events in FIFO order, then poll the original transmitter.
fn settle_client_before_transmit(pair: &mut Pair, ch: ConnectionHandle) {
    pair.client.drive_incoming(pair.time, pair.server.addr);
    pair.client.process_connection_events(ch);
    let mut settled = 0;
    while let Some(event) = pair.client_conn_mut(ch).poll_endpoint_events() {
        pair.client.endpoint.handle_connection_event(ch, event, pair.client.connections.get_mut(&ch).unwrap());
        settled += 1;
        assert!(settled <= 1480, "original operation's event bound");
    }
}

#[test]
fn cold_cid_full_pauses_new_wire_but_old_probe_ack_and_data_progress() {
    for close_while_full in [false, true] {
        let budget = cold_fixture_budget(2);
        let meter = bytes::BackingMeter::with_budget(&budget);
        let mut config = admission_endpoint_config();
        config.receive_backing_meter(meter.clone());
        config.receive_datagram_limit(1472).unwrap();
        let mut server = server_config();
        let mut server_transport = TransportConfig::default();
        server_transport
            .deterministic_packet_numbers(true)
            .mtu_discovery_config(None);
        server.transport = Arc::new(server_transport);
        let mut client = client_config_with_deterministic_pns();
        let mut frequency = AckFrequencyConfig::default();
        frequency
            .ack_eliciting_threshold(256u32.into())
            .reordering_threshold(0u32.into());
        Arc::get_mut(&mut client.transport)
            .unwrap()
            .ack_frequency_config(Some(frequency))
            .mtu_discovery_config(None);
        let mut pair = Pair::new(Arc::new(config), server);
        let (client_ch, server_ch) = pair.connect_with(client);
        assert!(
            pair.client_conn_mut(client_ch)
                .stats()
                .frame_tx
                .ack_frequency
                > 0
        );
        assert!(
            pair.server_conn_mut(server_ch)
                .stats()
                .frame_rx
                .ack_frequency
                > 0
        );
        let now = pair.time;
        let ack_base = pair.client_conn_mut(client_ch).stats().frame_rx.acks;
        let mut rotations = 0;
        let mut withheld = Vec::new();
        for _ in 0..80 {
            // Finite test guard, not a protocol inventory bound.
            let before = pair.server_conn_mut(server_ch).active_rem_cid_seq();
            assert!(pair
                .server_conn_mut(server_ch)
                .try_local_address_changed(now));
            let after = pair.server_conn_mut(server_ch).active_rem_cid_seq();
            if before == after {
                break;
            }
            let tx_before = pair
                .server_conn_mut(server_ch)
                .stats()
                .frame_tx
                .retire_connection_id;
            let rx_before = pair
                .client_conn_mut(client_ch)
                .stats()
                .frame_rx
                .retire_connection_id;
            let stock_before = pair.client_conn_mut(client_ch).cid_pool_stock_for_probe();
            let new_tx_before = pair
                .client_conn_mut(client_ch)
                .stats()
                .frame_tx
                .new_connection_id;
            assert!(pair
                .client
                .endpoint
                .routes_local_cid_for_probe(client_ch, before));
            pair.drive_server();
            settle_client_before_transmit(&mut pair, client_ch);
            assert!(!pair
                .client
                .endpoint
                .routes_local_cid_for_probe(client_ch, before));
            let prepared = pair.client_conn_mut(client_ch).cid_pool_stock_for_probe();
            assert_eq!(prepared.0 + 1, stock_before.0, "exactly one replacement F");
            pair.drive_client();
            pair.drive_server();
            assert!(
                pair.server_conn_mut(server_ch)
                    .stats()
                    .frame_tx
                    .retire_connection_id
                    > tx_before
            );
            assert!(
                pair.client_conn_mut(client_ch)
                    .stats()
                    .frame_rx
                    .retire_connection_id
                    > rx_before
            );
            assert_eq!(
                pair.client_conn_mut(client_ch).stats().frame_rx.acks,
                ack_base,
                "ACK freed F before the intended Full phase"
            );
            assert!(pair.client_conn_mut(client_ch).retained_error().is_none());
            assert!(pair.server_conn_mut(server_ch).retained_error().is_none());
            let stock = pair.client_conn_mut(client_ch).cid_pool_stock_for_probe();
            let issued = pair.client_conn_mut(client_ch).active_local_cid_seq().1;
            let should_encode = prepared.0 >= 8;
            assert_eq!(
                pair.client_conn_mut(client_ch)
                    .stats()
                    .frame_tx
                    .new_connection_id,
                new_tx_before + u64::from(should_encode)
            );
            assert_eq!(
                pair.client_conn_mut(client_ch)
                    .cid_frame_stock_for_probe(issued),
                if should_encode { (0, 1) } else { (1, 0) }
            );
            if !should_encode {
                withheld.push(issued);
            }
            if stock.0 <= 9 {
                eprintln!("cid-full: rotation={rotations} seq={after} stock={stock:?}");
            }
            assert_eq!(stock.0 + stock.1 + stock.3, 72);
            rotations += 1;
        }
        let full = pair.client_conn_mut(client_ch).cid_pool_stock_for_probe();
        assert!(
            full.0 < 8 && full.1 > 0 && full.2 == 0 && full.3 > 0,
            "stock={full:?}"
        );
        assert!(rotations >= 64, "rotations={rotations}");
        assert_eq!(pair.time, now);
        assert!(!budget.quarantined());
        let tx = pair
            .client_conn_mut(client_ch)
            .stats()
            .frame_tx
            .new_connection_id;
        for payload in [
            b"first-full-flow".as_slice(),
            b"second-full-flow".as_slice(),
        ] {
            let stream = pair
                .client_conn_mut(client_ch)
                .streams()
                .open(Dir::Uni)
                .unwrap();
            pair.client_send(client_ch, stream).write(payload).unwrap();
            pair.drive_client();
            pair.drive_server();
            let mut recv = pair.server_recv(server_ch, stream);
            let mut chunks = recv.read(true).unwrap();
            let chunk = chunks.next(4096).unwrap().unwrap();
            assert_eq!(chunk.bytes.as_ref(), payload);
            chunks.finalize();
        }
        assert_eq!(
            pair.client_conn_mut(client_ch)
                .stats()
                .frame_tx
                .new_connection_id,
            tx
        );
        assert_eq!(
            pair.client_conn_mut(client_ch).cid_pool_stock_for_probe(),
            full
        );
        assert_eq!(
            pair.client_conn_mut(client_ch).stats().frame_rx.acks,
            ack_base
        );
        if close_while_full {
            let before = pair
                .server_conn_mut(server_ch)
                .stats()
                .frame_rx
                .connection_close;
            pair.client_conn_mut(client_ch)
                .close(now, VarInt::from_u32(42), Bytes::new());
            pair.drive_client();
            pair.drive_server();
            assert!(
                pair.server_conn_mut(server_ch)
                    .stats()
                    .frame_rx
                    .connection_close
                    > before
            );
            assert_eq!(
                pair.client_conn_mut(client_ch)
                    .stats()
                    .frame_tx
                    .new_connection_id,
                tx
            );
            assert!(matches!(
                pair.server_conn_mut(server_ch).retained_error(),
                Some(ConnectionError::ApplicationClosed(_))
            ));
            assert!(!pair.drive_bounded(), "original close/drain must settle");
            assert!(pair.client_conn_mut(client_ch).is_drained());
            assert!(pair.server_conn_mut(server_ch).is_drained());
            assert_eq!(pair.client.endpoint.open_connections(), 0);
            assert_eq!(pair.server.endpoint.open_connections(), 0);
            assert!(!budget.quarantined());
            eprintln!("cid-full-close: stock_before={full:?} peer_close=true maps=0 Q=false");
            drop(pair);
            assert_eq!(budget.used(), 0);
            continue;
        }
        // Only the original client loss clock is driven, while the peer's original
        // ACK deadline remains pending. This is controlled scheduling, not new PTO.
        pair.time = pair
            .client_conn_mut(client_ch)
            .loss_detection_deadline()
            .unwrap();
        pair.client.drive_outgoing(pair.time);
        let probe = pair
            .client_conn_mut(client_ch)
            .stats()
            .frame_tx
            .new_connection_id;
        assert!(
            probe > tx,
            "original encoded F must be probeable while Full"
        );
        assert_eq!(
            pair.client_conn_mut(client_ch).cid_pool_stock_for_probe(),
            full
        );
        pair.drive_client();
        pair.drive_server();
        pair.drive_client();
        let resumed = pair.client_conn_mut(client_ch).cid_pool_stock_for_probe();
        assert!(pair.client_conn_mut(client_ch).stats().frame_rx.acks > ack_base);
        assert!(
            resumed.0 >= 8,
            "actual peer ACK must free records: {resumed:?}"
        );
        assert_eq!(resumed.1, 0, "withheld first publication must resume");
        assert!(
            pair.client_conn_mut(client_ch)
                .stats()
                .frame_tx
                .new_connection_id
                > probe
        );
        for sequence in &withheld {
            assert_eq!(
                pair.client_conn_mut(client_ch)
                    .cid_frame_stock_for_probe(*sequence),
                (0, 1),
                "the same withheld record must now be sent"
            );
        }
        assert!(pair.client_conn_mut(client_ch).retained_error().is_none());
        assert!(pair.server_conn_mut(server_ch).retained_error().is_none());
        assert!(!budget.quarantined());
        eprintln!(
            "cid-full-recovery: full={full:?} probe_newcid_delta={} after_ack={resumed:?}",
            probe - tx
        );
        assert!(!pair.drive_bounded(), "original ACK/recovery must settle");
        assert_eq!(
            pair.server_conn_mut(server_ch)
                .stats()
                .frame_rx
                .new_connection_id,
            pair.client_conn_mut(client_ch)
                .stats()
                .frame_tx
                .new_connection_id,
            "the peer actually receives all original CID frames, including withheld ones"
        );
        assert!(pair.client_conn_mut(client_ch).retained_error().is_none());
        assert!(pair.server_conn_mut(server_ch).retained_error().is_none());
        control_records::stop(&mut pair, &[client_ch]);
        drop(pair);
        assert_eq!(budget.used(), 0);
    }
}
