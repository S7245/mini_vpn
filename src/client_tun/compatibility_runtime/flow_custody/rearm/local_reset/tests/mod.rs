//! Locks the actual poll/flush sequencing relied on by no-F reset settlement.

#[cfg(feature = "harness")]
mod outcomes;

#[test]
fn every_original_iface_poll_awaits_common_flush_before_later_gateway_work() {
    let root = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/src/client_tun.rs"));
    let driver = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/client_tun/legacy_event_loop/driver.rs"
    ));
    let payload = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/client_tun/legacy_event_loop/remote_payload.rs"
    ));
    let ingress = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/client_tun/legacy_event_loop/ingress.rs"
    ));
    assert_eq!(
        ingress.matches("iface.poll_with_egress_targets(").count(),
        1
    );
    let local_egress = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/client_tun/legacy_event_loop/local_egress.rs"
    ));
    assert_eq!(root.matches("iface.poll(").count(), 0);
    assert_eq!(local_egress.matches("iface.poll(").count(), 1);
    assert_eq!(payload.matches("iface.poll(").count(), 1);
    assert_eq!(driver.matches("iface.poll(").count(), 1);
    let sources = [root, driver, payload, ingress, local_egress];
    let polls: Vec<_> = sources
        .iter()
        .flat_map(|source| {
            ["iface.poll(", "iface.poll_with_egress_targets("]
                .into_iter()
                .flat_map(|name| source.match_indices(name).map(|(i, _)| &source[i..]))
        })
        .collect();
    assert_eq!(
        polls.len(),
        4,
        "audit new production poll edges before promotion"
    );
    for edge in polls {
        let flush = edge.find("flush_tx_and_release_downlink_permits(").unwrap();
        assert!(flush < 400, "the same poll turn must immediately flush");
        let between = &edge[..flush];
        for forbidden in [
            ".await",
            "reap_dead_slots",
            "legacy_bare_rearm",
            "legacy_reasoned_rearm",
            "process_dirty_relay",
        ] {
            assert!(
                !between.contains(forbidden),
                "new intervening effect: {forbidden}"
            );
        }
        assert!(edge[flush..].find(".await").unwrap() < 260);
    }
    let dirty = &root[root.find("async fn process_dirty_relay").unwrap()..];
    assert!(
        dirty[..dirty.find("\nfn relay_allows_remote_payload").unwrap()]
            .contains("c.local_reset.is_some()")
    );
}
