//! Locks the shared physical poll/flush sequencing and no-F reset settlement.

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
    let owner = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/client_tun/compatibility_runtime/flow_custody/rearm/local_reset.rs"
    ));
    let local_egress = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/client_tun/legacy_event_loop/local_egress.rs"
    ));
    assert_eq!(root.matches("iface.poll(").count(), 0);
    assert_eq!(owner.matches("iface.poll_with_egress_targets(").count(), 1);
    let sources = [root, driver, payload, ingress, local_egress, owner];
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
        1,
        "the original physical owner must hold the only production poll edge"
    );
    for edge in polls {
        let flush = edge.find("device.flush_tx().await").unwrap();
        let between = &edge[..flush];
        // This provenance gate freezes the permitted synchronous transaction,
        // not its formatting length. Runtime outcome tests exercise the owner.
        let permitted = r#"iface.poll_with_egress_targets(
            smoltcp::time::Instant::now(), device, sockets,
            targets.map(|set| set.iter().copied()),
        );
        #[cfg(all(test, feature = "harness"))]
        if let Some(bridge) = test_bridge {
            bridge.inject_foreign_egress(
                crate::client_tun::legacy_event_loop::relay_events::ForeignEgressPhase::Flush,
                stage, sockets, socket_ctxs, None,
            );
        }
        if !crate::client_tun::downlink_custody::prepare_poll_egress_flush(
            &mut captured, sockets, socket_ctxs,
        ) {
            return (
                Err(capture_refusal(device, crate::client_tun::downlink_custody::poll_capture_refusal(&captured).unwrap_or(crate::client_tun::downlink_custody::CaptureRefusal::Inventory))),
                TunFlushPermitRelease::default(),
            );
        }
        let result = LocalReset::retain_flush_failure(socket_ctxs, "#;
        assert_eq!(between.split_whitespace().collect::<String>(),
            permitted.split_whitespace().collect::<String>(),
            "only the original synchronous capture preflight may precede flush");
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
        assert!(edge[flush..].find("finish_poll_egress_flush(").unwrap() < 450);
    }
    let dirty = &root[root.find("async fn process_dirty_relay").unwrap()..];
    assert!(
        dirty[..dirty.find("\nfn relay_allows_remote_payload").unwrap()]
            .contains("c.local_reset.is_some()")
    );
}

#[test]
fn all_four_poll_edges_use_one_physical_egress_cycle() {
    let sources = [
        include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/client_tun/legacy_event_loop/ingress.rs"
        )),
        include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/client_tun/legacy_event_loop/local_egress.rs"
        )),
        include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/client_tun/legacy_event_loop/remote_payload.rs"
        )),
        include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/client_tun/legacy_event_loop/driver.rs"
        )),
    ];
    for source in sources {
        assert_eq!(source.matches("poll_egress_cycle(").count(), 1);
        assert_eq!(source.matches("iface.poll(").count(), 0);
        assert_eq!(source.matches("iface.poll_with_egress_targets(").count(), 0);
    }
}

// Tests invoke the original private prepare, never construct a reset marker.
impl super::LocalReset {
    pub(in crate::client_tun) fn prepare_wire_test(
        ctx: &mut crate::client_tun::SocketCtx,
        socket: &mut crate::client_tun::TcpSocket<'_>,
    ) -> bool {
        Self::prepare(ctx, socket, None)
    }
    pub(in crate::client_tun) fn wire_deadline_test(&self) -> tokio::time::Instant {
        self.deadline
    }
}
