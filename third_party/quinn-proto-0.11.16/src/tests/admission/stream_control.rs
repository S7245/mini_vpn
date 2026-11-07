//! Real stream operations under an injected authority fault; not natural Full.
use super::*;

#[test]
fn local_stream_fault_uses_original_root_clock_and_retains_inventory() {
    for action in 0..4 {
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
            .open(Dir::Bi)
            .unwrap();
        assert_eq!(
            pair.client_send(client, stream).write(b"retained").unwrap(),
            8
        );
        assert!(!pair.drive_bounded());
        let retained = send_buffer_inventory::retained_layouts(&pair);
        let (handle, on_client) = if action == 0 {
            (client, true)
        } else {
            (server, false)
        };
        let now = pair.time;
        let connection = if on_client {
            pair.client_conn_mut(handle)
        } else {
            pair.server_conn_mut(handle)
        };
        let deadline = connection.original_close_deadline_for_stream_probe(now);
        let before_rx = connection.recv_stream(stream, now).progress();
        let before_tx = connection.send_stream(stream, now).progress();
        let used;
        if action == 3 {
            let mut recv = connection.recv_stream(stream, now);
            let mut chunks = recv.read(true).unwrap();
            meter.quarantine();
            used = budget.used();
            assert!(
                chunks.next(1).is_err(),
                "an already-held read must not consume after Q"
            );
            drop(chunks);
        } else {
            meter.quarantine();
            used = budget.used();
            match action {
                0 => assert!(connection
                    .send_stream(stream, now)
                    .reset(42u32.into())
                    .is_err()),
                1 => assert!(connection
                    .recv_stream(stream, now)
                    .stop(42u32.into())
                    .is_err()),
                2 => assert!(connection.recv_stream(stream, now).read(true).is_err()),
                _ => unreachable!(),
            }
        }
        assert!(
            connection.is_closed(),
            "action {action} must fail in the same Root call"
        );
        assert!(
            !connection.is_drained(),
            "Q must not replace original Close with immediate Drained"
        );
        assert_eq!(connection.poll_timeout(), Some(deadline));
        let first = connection
            .retained_error()
            .expect("original typed first failure");
        assert!(matches!(&first, ConnectionError::TransportError(error)
            if error.code == TransportErrorCode::INTERNAL_ERROR));
        assert!(connection
            .recv_stream(stream, now)
            .stop(43u32.into())
            .is_err());
        assert_eq!(connection.retained_error(), Some(first));
        assert_eq!(connection.poll_timeout(), Some(deadline));
        if let Ok(before) = before_rx {
            let after = connection.recv_stream(stream, now).progress().unwrap();
            assert_eq!(after.read_offset, before.read_offset);
            assert_eq!(after.buffered_bytes, before.buffered_bytes);
            assert_eq!(
                after.highest_received_offset,
                before.highest_received_offset
            );
        }
        if let Ok(before) = before_tx {
            assert_eq!(
                connection.send_stream(stream, now).progress().unwrap(),
                before
            );
        }
        assert_eq!(budget.used(), used);
        drop(pair);
        retained.assert_quarantined(&budget);
        assert!(budget.quarantined());
        let charged = retained.charged;
        eprintln!("local-stream-Q: action={action} original_Close_deadline=true first_retained=true same_inventory=true known_drop_exact_native_retained={charged}; natural_Full=false actual_join=false");
    }
}
