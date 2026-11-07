//! Actual original-loop inventory, taken once at each cold control milestone.
use super::*;

pub(super) async fn sample(service: &mut impl TunService, owner: &Arc<SessionControl>, phase: &str) {
    let bridge = wire::drive(service, Duration::from_secs(5), async {
        loop {
            if let Some(bridge) = owner.relay_event_test_bridge() { break bridge; }
            tokio::task::yield_now().await;
        }
    }).await;
    bridge.request_capacity();
    let actual = wire::drive(service, Duration::from_secs(5), async {
        loop {
            if let Some(actual) = bridge.take_capacity() { break actual; }
            tokio::task::yield_now().await;
        }
    }).await;
    assert!(actual.sockets <= 4352);
    assert!(actual.storage_capacity >= actual.storage_len && actual.storage_len >= actual.sockets);
    assert_eq!(actual.sockets, actual.registry_handles);
    assert_eq!(actual.rx_bytes + actual.tx_bytes, actual.sockets * 2 * 65535);
    assert!(actual.context_capacity >= actual.contexts);
    assert_eq!(actual.registry.handles, actual.sockets);
    assert_eq!(actual.registry.ports, actual.ports);
    assert!(actual.registry.ports <= 64 && actual.registry.max_total == 4352);
    assert!(actual.registry.port_capacity >= actual.registry.ports);
    assert!(actual.registry.handle_capacity >= actual.registry.handles);
    assert!(actual.registry.largest_handle_capacity <= actual.registry.handle_capacity);
    assert_eq!(actual.registry.handle_bytes, actual.registry.handle_capacity * std::mem::size_of::<SocketHandle>());
    assert!(actual.context_pair.0 > 0 && actual.context_pair.1.is_power_of_two());
    assert!(actual.registry.pair_size > 0 && actual.registry.pair_align.is_power_of_two());
    assert_eq!(actual.registry.pool_size, 1);
    let root = owner.root_layout_for_test();
    assert_eq!(root.slot_len, 4224);
    assert!(root.slot_capacity >= root.slot_len && root.rows <= root.slot_len);
    assert!(root.waiting <= 128 && root.published <= 4096 && root.provider_boxes <= root.rows);
    eprintln!("[N1-actual-instance-geometry] phase={phase} actual={actual:?} root={root:?} root_owned_slots={} row_boxes={} provider_outer={} ports={} config_socket_payload_bound={} hash_bucket_layout_requires_current_toolchain_source=true actor_TLS_OS_partition_separate=true",
        root.slot_bytes, root.row_bytes, root.provider_outer_bytes, actual.ports, 4352 * 2 * 65535);
}
