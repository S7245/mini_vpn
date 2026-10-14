//! Original local-egress stop and drain-only characterizers.
use super::*;

#[tokio::test]
async fn local_egress_service_stops_on_no_work_hard_pause_and_no_progress() {
    let upstream = Arc::new(LegacyOwnedUpstream::new(Arc::new(NoopTestUpstream)));
    let metrics = Metrics::new();
    let mut device = DnsInjectRecorder::default();
    let mut iface = Interface::new(
        SmolConfig::new(smoltcp::wire::HardwareAddress::Ip),
        &mut device,
        smoltcp::time::Instant::now(),
    );
    let mut assoc_table = AssocTable::new();
    let mut fake_pool = FakeIpPool::new();
    let mut registry = ListenerRegistry::new(1);
    let mut sockets = SocketSet::new(vec![]);
    let mut socket_ctxs = HashMap::new();
    let mut dirty = HashSet::new();
    let (handshake_done_tx, _handshake_done_rx) = OpenDispatch::channel(1, None);
    let (global_tx, _global_rx) = mpsc::channel(1);
    let (owned_action_tx, _owned_action_rx) = mpsc::channel(1);
    let mut sink = NoopSink;
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 100,
        low_bytes: 40,
    };
    let mut drop_debt = DownlinkEgressDropDebt::default();
    let mut tcp_loop_flush_tx_calls = 0;
    let mut tcp_loop_flush_tx_failures = 0;
    let mut tun_rx_diag = TunRxDrainDiag::default();
    let mut local_diag = LocalEgressServiceDiag::default();
    let service_cfg = LocalEgressServiceConfig {
        max_cycles: 2,
        target_bytes_per_window: 128,
        tun_rx_packets_per_cycle: 1,
    };

    let no_work = service_local_egress_until(
        &mut device,
        &mut assoc_table,
        &mut fake_pool,
        &upstream,
        0,
        &metrics,
        &mut registry,
        &mut sockets,
        &mut socket_ctxs,
        &mut iface,
        &mut dirty,
        &handshake_done_tx,
        &global_tx,
        &owned_action_tx,
        &mut sink,
        DEFAULT_DOWNLINK_FLUSH_MAX_BYTES,
        cfg,
        1200,
        &mut drop_debt,
        &mut tcp_loop_flush_tx_calls,
        &mut tcp_loop_flush_tx_failures,
        &mut tun_rx_diag,
        &mut local_diag,
        false,
        false,
        BufferedDownlinkConfig::disabled(cfg, 1),
        service_cfg,
        false,
    )
    .await
    .unwrap();
    assert_eq!(no_work.stop_reason, LocalEgressServiceStopReason::NoWork);
    assert_eq!(local_diag.no_work, 1);

    let handle = sockets.add(build_listener_socket(&ListenerSpec { local_port: 443 }));
    socket_ctxs.insert(handle, SocketCtx::new(443));
    dirty.insert(handle);
    let hard_pause = service_local_egress_until(
        &mut device,
        &mut assoc_table,
        &mut fake_pool,
        &upstream,
        0,
        &metrics,
        &mut registry,
        &mut sockets,
        &mut socket_ctxs,
        &mut iface,
        &mut dirty,
        &handshake_done_tx,
        &global_tx,
        &owned_action_tx,
        &mut sink,
        DEFAULT_DOWNLINK_FLUSH_MAX_BYTES,
        cfg,
        1200,
        &mut drop_debt,
        &mut tcp_loop_flush_tx_calls,
        &mut tcp_loop_flush_tx_failures,
        &mut tun_rx_diag,
        &mut local_diag,
        true,
        true,
        BufferedDownlinkConfig::disabled(cfg, 1),
        service_cfg,
        false,
    )
    .await
    .unwrap();
    assert_eq!(
        hard_pause.stop_reason,
        LocalEgressServiceStopReason::HardPause
    );
    assert_eq!(hard_pause.cycles, 0);
    assert_eq!(local_diag.hard_pause, 1);

    let read_pause_only = service_local_egress_until(
        &mut device,
        &mut assoc_table,
        &mut fake_pool,
        &upstream,
        0,
        &metrics,
        &mut registry,
        &mut sockets,
        &mut socket_ctxs,
        &mut iface,
        &mut dirty,
        &handshake_done_tx,
        &global_tx,
        &owned_action_tx,
        &mut sink,
        DEFAULT_DOWNLINK_FLUSH_MAX_BYTES,
        cfg,
        1200,
        &mut drop_debt,
        &mut tcp_loop_flush_tx_calls,
        &mut tcp_loop_flush_tx_failures,
        &mut tun_rx_diag,
        &mut local_diag,
        true,
        false,
        BufferedDownlinkConfig::disabled(cfg, 1),
        service_cfg,
        false,
    )
    .await
    .unwrap();
    assert_ne!(
        read_pause_only.stop_reason,
        LocalEgressServiceStopReason::HardPause
    );
    assert_eq!(
        read_pause_only.cycles, 1,
        "read-side pause must not suppress local egress service cycles"
    );
    assert_eq!(local_diag.hard_pause, 1);

    dirty.insert(handle);
    let no_progress = service_local_egress_until(
        &mut device,
        &mut assoc_table,
        &mut fake_pool,
        &upstream,
        0,
        &metrics,
        &mut registry,
        &mut sockets,
        &mut socket_ctxs,
        &mut iface,
        &mut dirty,
        &handshake_done_tx,
        &global_tx,
        &owned_action_tx,
        &mut sink,
        DEFAULT_DOWNLINK_FLUSH_MAX_BYTES,
        cfg,
        1200,
        &mut drop_debt,
        &mut tcp_loop_flush_tx_calls,
        &mut tcp_loop_flush_tx_failures,
        &mut tun_rx_diag,
        &mut local_diag,
        false,
        false,
        BufferedDownlinkConfig::disabled(cfg, 1),
        service_cfg,
        false,
    )
    .await
    .unwrap();
    assert_ne!(
        no_progress.stop_reason,
        LocalEgressServiceStopReason::HardPause
    );
    assert_eq!(no_progress.cycles, 1);
}

#[tokio::test]
async fn d16_drain_only_keeps_poll_and_flush_progress() {
    let upstream = Arc::new(LegacyOwnedUpstream::new(Arc::new(NoopTestUpstream)));
    let metrics = Metrics::new();
    let mut device = DnsInjectRecorder::default();
    let mut iface = Interface::new(
        SmolConfig::new(smoltcp::wire::HardwareAddress::Ip),
        &mut device,
        smoltcp::time::Instant::now(),
    );
    let mut assoc_table = AssocTable::new();
    let mut fake_pool = FakeIpPool::new();
    let mut registry = ListenerRegistry::new(1);
    let mut sockets = SocketSet::new(vec![]);
    let handle = sockets.add(build_listener_socket(&ListenerSpec { local_port: 443 }));
    let mut ctx = SocketCtx::new(443);
    ctx.d16_downlink_queue = Some(
        AsyncLeasedByteFlowQueue::new_with_release_mode(
            512 * 1024,
            DownstreamPermitReleaseMode::OnEgressDrain,
        )
        .unwrap(),
    );
    ctx.downlink_pending.resize(4096, 1);
    let mut socket_ctxs = HashMap::from([(handle, ctx)]);
    let mut dirty = HashSet::from([handle]);
    let (handshake_done_tx, _handshake_done_rx) = OpenDispatch::channel(1, None);
    let (global_tx, _global_rx) = mpsc::channel(1);
    let (owned_action_tx, _owned_action_rx) = mpsc::channel(1);
    let mut sink = NoopSink;
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 100,
        low_bytes: 40,
    };
    let mut drop_debt = DownlinkEgressDropDebt::default();
    let mut tcp_loop_flush_tx_calls = 0;
    let mut tcp_loop_flush_tx_failures = 0;
    let mut tun_rx_diag = TunRxDrainDiag::default();
    let mut local_diag = LocalEgressServiceDiag::default();

    let progress = service_local_egress_until(
        &mut device,
        &mut assoc_table,
        &mut fake_pool,
        &upstream,
        0,
        &metrics,
        &mut registry,
        &mut sockets,
        &mut socket_ctxs,
        &mut iface,
        &mut dirty,
        &handshake_done_tx,
        &global_tx,
        &owned_action_tx,
        &mut sink,
        DEFAULT_DOWNLINK_FLUSH_MAX_BYTES,
        cfg,
        1200,
        &mut drop_debt,
        &mut tcp_loop_flush_tx_calls,
        &mut tcp_loop_flush_tx_failures,
        &mut tun_rx_diag,
        &mut local_diag,
        true,
        true,
        BufferedDownlinkConfig::disabled(cfg, 1),
        LocalEgressServiceConfig {
            max_cycles: 1,
            target_bytes_per_window: 128,
            tun_rx_packets_per_cycle: 1,
        },
        true,
    )
    .await
    .unwrap();

    assert_eq!(progress.cycles, 1);
    assert_eq!(progress.flush_tx_calls, 1);
    assert_eq!(tcp_loop_flush_tx_calls, 1);
    let ctx = socket_ctxs.get(&handle).unwrap();
    assert_eq!(ctx.downlink_pending.len(), 4096);
    assert_eq!(ctx.downlink_diag.flush_attempts, 0);
    assert_eq!(ctx.downlink_diag.send_slice_calls, 0);
    assert_eq!(ctx.d16_egress_phase, EgressPhase::DrainOnly);
    assert_eq!(ctx.downlink_diag.d16_egress_phase_transitions, 1);
    assert_eq!(ctx.downlink_diag.d16_drain_only_cycles, 1);
    assert_eq!(ctx.downlink_diag.d16_drain_only_drain_bytes, 0);
    let aggregate = tcp_downlink_aggregate(socket_ctxs.values());
    let line = format_tcp_downlink_flush_diag(&aggregate, dirty.len());
    assert!(line.contains("d16_running_flows=0"), "{line}");
    assert!(line.contains("d16_drain_only_flows=1"), "{line}");
    assert!(line.contains("d16_recovery_flows=0"), "{line}");
    assert!(line.contains("d16_egress_phase_transitions=1"), "{line}");
    assert!(line.contains("d16_drain_only_cycles=1"), "{line}");
}
