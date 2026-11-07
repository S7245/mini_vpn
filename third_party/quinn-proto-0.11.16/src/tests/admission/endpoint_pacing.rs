//! Actual Native Endpoint construction must fund optional pacing before publication.
use super::*;

#[test]
fn native_pacing_cannot_construct_with_only_registration_layout_budget() {
    let budget = bytes::BackingBudget::new(Endpoint::native_registration_layout_bytes()).unwrap();
    let mut config = admission_endpoint_config();
    config.receive_backing_meter(bytes::BackingMeter::with_budget(&budget));
    config.endpoint_pacing_service(Some(
        crate::EndpointPacingServiceConfig::endpoint_window_v1(),
    ));
    let result = Endpoint::try_new(Arc::new(config), None, true, Some([0x45; 32]));
    let rejected = result.is_err();
    drop(result);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
    assert!(
        rejected,
        "Native pacing records and waiters need their own cold layout before Endpoint publication"
    );
}

#[test]
fn native_pacing_real_drained_connections_reuse_slots_beyond_nineteen() {
    let budget = cold_fixture_budget(2);
    let mut config = admission_endpoint_config();
    config.receive_datagram_limit(1472).unwrap();
    config.receive_backing_meter(bytes::BackingMeter::with_budget(&budget));
    config.endpoint_pacing_service(Some(
        crate::EndpointPacingServiceConfig::endpoint_window_v1(),
    ));
    let mut endpoint = Endpoint::try_new(Arc::new(config), None, true, Some([0x46; 32])).unwrap();
    let baseline = budget.used();
    for _ in 0..40 {
        let (handle, mut connection) = endpoint
            .connect(
                Instant::now(),
                client_config(),
                "[::1]:4433".parse().unwrap(),
                "localhost",
            )
            .unwrap();
        assert_eq!(
            endpoint
                .endpoint_pacing_snapshot()
                .unwrap()
                .connection_records,
            1
        );
        connection.abandon();
        let event = connection.take_drained_event().unwrap();
        endpoint.handle_event(handle, event);
        drop(connection);
        let records = endpoint
            .endpoint_pacing_snapshot()
            .unwrap()
            .connection_records;
        if records != 0 {
            drop(endpoint);
            assert_eq!(
                records, 0,
                "actual Drained/drop must retire its original pacing record"
            );
            return;
        }
        assert_eq!(budget.used(), baseline);
    }
    drop(endpoint);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn native_established_connection_cannot_disable_pacing_through_detach_facade() {
    let budget = cold_fixture_budget(2);
    let mut config = admission_endpoint_config();
    config.receive_datagram_limit(1472).unwrap();
    config.receive_backing_meter(bytes::BackingMeter::with_budget(&budget));
    config.endpoint_pacing_service(Some(
        crate::EndpointPacingServiceConfig::endpoint_window_v1(),
    ));
    let mut pair = Pair::new(Arc::new(config), server_config());
    let client = pair.begin_connect(client_config());
    assert!(!pair.drive_bounded());
    let server = pair.server.assert_accept();
    let now = pair.time;
    assert!(!pair.client_conn_mut(client).is_handshaking());
    let detached = pair.client_conn_mut(client).endpoint_pacing_detach(now);
    let active = pair.client_conn_mut(client).endpoint_pacing_active();
    pair.client_conn_mut(client).abandon();
    pair.server_conn_mut(server).abandon();
    pair.drive_bounded();
    drop(pair);
    assert!(
        !detached && active,
        "live Native pacing must not become the unpaced None branch"
    );
}
