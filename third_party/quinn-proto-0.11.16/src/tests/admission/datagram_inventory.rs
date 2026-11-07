//! Actual authenticated Connection DATAGRAM metadata under the original meter.
use super::*;
#[test]
fn native_zero_length_datagram_requires_real_descriptor_before_publication() {
    let budget = cold_fixture_budget(2);
    let meter = bytes::BackingMeter::with_budget(&budget);
    let mut config = admission_endpoint_config();
    config.receive_backing_meter(meter.clone());
    config.receive_datagram_limit(1472).unwrap();
    let mut pair = Pair::new(Arc::new(config), server_config());
    let client = pair.begin_connect(client_config());
    assert!(!pair.drive_bounded());
    let header = bytes::BytesMut::metered_copy_layout_for_len(1).unwrap() - 1;
    let pressure = bytes::BytesMut::try_zeroed_metered(budget.limit() - budget.used() - header, &meter).unwrap();
    let before = budget.used();
    let result = pair.client_datagrams(client).send(bytes::Bytes::new(), true);
    let refused = result.is_err();
    eprintln!("[N1-native-datagram-descriptor] before={before} after={} limit={} zero_length_refused={refused} result={result:?}", budget.used(), budget.limit());
    assert_eq!(budget.used(), before);
    drop(pressure);
    let now = pair.time;
    pair.client_conn_mut(client).close(now, 0u32.into(), bytes::Bytes::new());
    assert!(!pair.drive_bounded());
    assert!(pair.client_conn_mut(client).is_drained());
    assert_eq!(pair.client.endpoint.open_connections(), 0);
    assert_eq!(pair.server.endpoint.open_connections(), 0);
    drop(pair);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
    assert!(refused, "even an empty legal DATAGRAM consumes one physical outgoing descriptor");
}
