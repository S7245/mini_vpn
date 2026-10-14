//! Original ingress transaction: classify/stage, poll, flush and dirty service.
//! The driver retains device, sockets, registry and flow authority; this child
//! borrows them and owns no queue or task. Every poll immediately awaits the
//! common flush. Entry: process_ready_tun_rx_packet and drain_ready_tun_rx.
//! Tests: compatibility_lifecycle and the local_reset poll-edge characterizer.

use super::super::*;
use listener_registry::InboundTcpCoverage;

async fn prepare_ready_tun_rx_packet<D, U>(
    device: &mut D,
    assoc_table: &mut AssocTable,
    fake_pool: &mut FakeIpPool,
    upstream: &Arc<U>,
    now_secs: u64,
    metrics_handle: &Metrics,
    registry: &mut ListenerRegistry,
    sockets: &mut SocketSet<'static>,
    socket_ctxs: &mut HashMap<SocketHandle, SocketCtx>,
    dirty: &mut HashSet<SocketHandle>,
    flow_mode: bool,
) -> (TunRxPacketKind, InboundTcpCoverage)
where
    D: TunIo,
    U: OwnedUpstream + 'static,
{
    // rx 分流（stage-12 D1 + 刀5）：任意 :53 → 裸包 DNS 劫持；其它 UDP → 裸 relay；
    // 非 UDP → 既有 smoltcp 路径。前两类 take 走、不进 iface.poll。
    let class = device.rx_peek().map(classify_inbound);
    if class == Some(Inbound::UdpRelay) {
        if let Some(pkt) = device.rx_take() {
            handle_tuic_udp_uplink(&pkt, assoc_table, fake_pool, upstream.as_ref(), now_secs).await;
        }
        return (TunRxPacketKind::Udp, InboundTcpCoverage::FullFallback);
    }
    if class == Some(Inbound::Dns) {
        if let Some(pkt) = device.rx_take() {
            handle_dns_hijack(&pkt, fake_pool, device, now_secs, metrics_handle).await;
        }
        return (TunRxPacketKind::Dns, InboundTcpCoverage::FullFallback);
    }

    let single_packet = device.can_stage_single_iface_rx();
    let mut coverage = if flow_mode {
        registry.prepare_inbound_tcp_capacity(device.rx_peek(), sockets, socket_ctxs)
    } else {
        registry.prepare_inbound_tcp(device.rx_peek(), sockets, socket_ctxs, dirty)
    };

    if !device.stage_rx_for_iface() {
        coverage = InboundTcpCoverage::FullFallback;
        tcp_diag_log!("🔎 tcp-tun-rx-stage-missing");
    }

    if !single_packet {
        coverage = InboundTcpCoverage::FullFallback;
    }
    (TunRxPacketKind::Tcp, coverage)
}

async fn poll_staged_tun_rx<D, M>(
    device: &mut D,
    sockets: &mut SocketSet<'static>,
    socket_ctxs: &mut HashMap<SocketHandle, SocketCtx>,
    iface: &mut Interface,
    metrics: &mut M,
    tcp_loop_flush_tx_calls: &mut u64,
    tcp_loop_flush_tx_failures: &mut u64,
    poll_stage: &str,
    targets: Option<&HashSet<SocketHandle>>,
) where
    D: TunIo,
    M: MetricsSink,
{
    metrics.enter_poll();
    iface.poll_with_egress_targets(
        smoltcp::time::Instant::now(),
        device,
        sockets,
        targets.map(|set| set.iter().copied()),
    );
    *tcp_loop_flush_tx_calls = tcp_loop_flush_tx_calls.saturating_add(1);
    let (flush_result, _) =
        flush_tx_and_release_downlink_permits(device, sockets, socket_ctxs, poll_stage).await;
    if let Err(e) = flush_result {
        *tcp_loop_flush_tx_failures = tcp_loop_flush_tx_failures.saturating_add(1);
        tcp_diag_log!(
            "🔎 tcp-loop-flush-tx-fail stage={} calls={} failures={} err={e}",
            poll_stage,
            *tcp_loop_flush_tx_calls,
            *tcp_loop_flush_tx_failures
        );
    }
    metrics.leave_poll();
}

pub(in crate::client_tun) async fn process_ready_tun_rx_packet<D, U, M>(
    device: &mut D,
    assoc_table: &mut AssocTable,
    fake_pool: &mut FakeIpPool,
    upstream: &Arc<U>,
    now_secs: u64,
    metrics_handle: &Metrics,
    registry: &mut ListenerRegistry,
    sockets: &mut SocketSet<'static>,
    socket_ctxs: &mut HashMap<SocketHandle, SocketCtx>,
    iface: &mut Interface,
    dirty: &mut HashSet<SocketHandle>,
    handshake_done_tx: &OpenDispatch,
    global_tx: &mpsc::Sender<(SocketHandle, RelayEvent)>,
    owned_action_tx: &mpsc::Sender<OwnedActionEvent>,
    metrics: &mut M,
    downlink_flush_max_bytes: usize,
    downlink_backpressure: DownlinkBackpressureConfig,
    tun_mtu: usize,
    downlink_egress_drop_debt: &mut DownlinkEgressDropDebt,
    tcp_loop_flush_tx_calls: &mut u64,
    tcp_loop_flush_tx_failures: &mut u64,
    relay_read_hard_pause: bool,
    buffered_downlink: BufferedDownlinkConfig,
    poll_stage: &str,
) -> TunRxPacketKind
where
    D: TunIo,
    U: OwnedUpstream + 'static,
    M: MetricsSink,
{
    let (packet_kind, coverage) = prepare_ready_tun_rx_packet(
        device,
        assoc_table,
        fake_pool,
        upstream,
        now_secs,
        metrics_handle,
        registry,
        sockets,
        socket_ctxs,
        dirty,
        handshake_done_tx.flow_mode(),
    )
    .await;
    if packet_kind != TunRxPacketKind::Tcp {
        return packet_kind;
    }

    poll_staged_tun_rx(
        device,
        sockets,
        socket_ctxs,
        iface,
        metrics,
        tcp_loop_flush_tx_calls,
        tcp_loop_flush_tx_failures,
        poll_stage,
        (handshake_done_tx.flow_mode() && coverage == InboundTcpCoverage::CompleteTcp)
            .then_some(&*dirty),
    )
    .await;

    super::ingress_effects::collect(iface, sockets, dirty, handshake_done_tx.flow_mode());
    let close_egress_guard = downlink_egress_drop_debt.has_active_drop_debt();
    let relay_read_recovery_active = downlink_egress_drop_debt.has_active_drop_debt();
    process_dirty_relay(
        dirty,
        sockets,
        socket_ctxs,
        upstream,
        handshake_done_tx,
        global_tx,
        owned_action_tx,
        fake_pool,
        now_secs,
        metrics_handle,
        metrics,
        downlink_flush_max_bytes,
        downlink_backpressure,
        tun_mtu,
        downlink_egress_drop_debt,
        close_egress_guard,
        relay_read_recovery_active,
        relay_read_hard_pause,
        buffered_downlink,
        DownlinkAdmissionOrigin::ControlOnly,
    )
    .await;

    TunRxPacketKind::Tcp
}

pub(in crate::client_tun) async fn drain_ready_tun_rx<D, U, M>(
    device: &mut D,
    assoc_table: &mut AssocTable,
    fake_pool: &mut FakeIpPool,
    upstream: &Arc<U>,
    now_secs: u64,
    metrics_handle: &Metrics,
    registry: &mut ListenerRegistry,
    sockets: &mut SocketSet<'static>,
    socket_ctxs: &mut HashMap<SocketHandle, SocketCtx>,
    iface: &mut Interface,
    dirty: &mut HashSet<SocketHandle>,
    handshake_done_tx: &OpenDispatch,
    global_tx: &mpsc::Sender<(SocketHandle, RelayEvent)>,
    owned_action_tx: &mpsc::Sender<OwnedActionEvent>,
    metrics: &mut M,
    downlink_flush_max_bytes: usize,
    downlink_backpressure: DownlinkBackpressureConfig,
    tun_mtu: usize,
    downlink_egress_drop_debt: &mut DownlinkEgressDropDebt,
    tcp_loop_flush_tx_calls: &mut u64,
    tcp_loop_flush_tx_failures: &mut u64,
    diag: &mut TunRxDrainDiag,
    budget: usize,
    source: &str,
    relay_read_hard_pause: bool,
    buffered_downlink: BufferedDownlinkConfig,
) -> std::io::Result<usize>
where
    D: TunIo,
    U: OwnedUpstream + 'static,
    M: MetricsSink,
{
    if budget == 0 || handshake_done_tx.flow_draining() {
        return Ok(0);
    }

    let flow_mode = handshake_done_tx.flow_mode();
    diag.note_attempt(source);
    let guard_before = diag.backlog_guard();
    let mut drained = 0usize;
    let mut tcp_packets_in_batch = 0usize;
    let emergency_budget = pressure_tun_rx_drain_budget(downlink_backpressure, tun_mtu);
    let mut drain_limit = budget;
    let mut emergency_extended = false;
    'drain: loop {
        while drained < drain_limit {
            if handshake_done_tx.flow_draining() {
                break 'drain;
            }
            match device.try_recv_rx() {
                Ok(true) => {
                    let kind = if flow_mode {
                        process_ready_tun_rx_packet(
                            device,
                            assoc_table,
                            fake_pool,
                            upstream,
                            now_secs,
                            metrics_handle,
                            registry,
                            sockets,
                            socket_ctxs,
                            iface,
                            dirty,
                            handshake_done_tx,
                            global_tx,
                            owned_action_tx,
                            metrics,
                            downlink_flush_max_bytes,
                            downlink_backpressure,
                            tun_mtu,
                            downlink_egress_drop_debt,
                            tcp_loop_flush_tx_calls,
                            tcp_loop_flush_tx_failures,
                            relay_read_hard_pause || diag.backlog_guard().is_active(),
                            buffered_downlink,
                            "tun_rx_drain",
                        )
                        .await
                    } else {
                        prepare_ready_tun_rx_packet(
                            device,
                            assoc_table,
                            fake_pool,
                            upstream,
                            now_secs,
                            metrics_handle,
                            registry,
                            sockets,
                            socket_ctxs,
                            dirty,
                            false,
                        )
                        .await
                        .0
                    };
                    if kind == TunRxPacketKind::Tcp {
                        if flow_mode {
                            diag.note_tcp_batch_poll_flush(1);
                            diag.note_tcp_batch_dirty_relay_pass(1);
                        } else {
                            tcp_packets_in_batch = tcp_packets_in_batch.saturating_add(1);
                        }
                    }
                    diag.note_packet(kind);
                    drained += 1;
                }
                Ok(false) => {
                    diag.note_would_block();
                    break 'drain;
                }
                Err(e) => {
                    diag.note_error();
                    if flow_mode {
                        return Err(e);
                    }
                    tcp_diag_log!(
                        "🔎 tcp-tun-rx-drain-error source={} drained={} err={e}",
                        source,
                        drained
                    );
                    break 'drain;
                }
            }
        }
        if handshake_done_tx.flow_draining() {
            break;
        }
        match device.try_recv_rx() {
            Ok(true) => {
                diag.note_budget_exhausted();
                let extended_limit = drain_limit.max(emergency_budget);
                if !emergency_extended && extended_limit > drain_limit {
                    emergency_extended = true;
                    drain_limit = extended_limit;
                    continue;
                }
                break;
            }
            Ok(false) => {
                diag.note_would_block();
                break;
            }
            Err(e) => {
                diag.note_error();
                if flow_mode {
                    return Err(e);
                }
                tcp_diag_log!(
                    "🔎 tcp-tun-rx-drain-probe-error source={} drained={} err={e}",
                    source,
                    drained
                );
                break;
            }
        }
    }
    if tcp_packets_in_batch > 0 {
        poll_staged_tun_rx(
            device,
            sockets,
            socket_ctxs,
            iface,
            metrics,
            tcp_loop_flush_tx_calls,
            tcp_loop_flush_tx_failures,
            "tun_rx_drain",
            None,
        )
        .await;
        super::ingress_effects::collect(iface, sockets, dirty, handshake_done_tx.flow_mode());
        diag.note_tcp_batch_poll_flush(tcp_packets_in_batch);
    }
    let guard_after = diag.backlog_guard();
    match tun_rx_backlog_action(guard_before, guard_after) {
        TunRxBacklogAction::ForceDrainOnly => {
            force_all_d16_flows_to_drain_only_for_tun_rx_backlog(
                dirty,
                sockets,
                socket_ctxs,
                downlink_backpressure,
                downlink_egress_drop_debt.has_active_drop_debt(),
            );
        }
        TunRxBacklogAction::Recover => {
            recover_all_eligible_d16_flows_after_clean_tun_rx_edge(
                dirty,
                sockets,
                socket_ctxs,
                downlink_backpressure,
                downlink_egress_drop_debt.has_active_drop_debt(),
            );
        }
        TunRxBacklogAction::None => {}
    }
    if tcp_packets_in_batch > 0 {
        let close_egress_guard = downlink_egress_drop_debt.has_active_drop_debt();
        let relay_read_recovery_active = downlink_egress_drop_debt.has_active_drop_debt();
        process_dirty_relay(
            dirty,
            sockets,
            socket_ctxs,
            upstream,
            handshake_done_tx,
            global_tx,
            owned_action_tx,
            fake_pool,
            now_secs,
            metrics_handle,
            metrics,
            downlink_flush_max_bytes,
            downlink_backpressure,
            tun_mtu,
            downlink_egress_drop_debt,
            close_egress_guard,
            relay_read_recovery_active,
            relay_read_hard_pause || guard_after.is_active(),
            buffered_downlink,
            DownlinkAdmissionOrigin::ControlOnly,
        )
        .await;
        diag.note_tcp_batch_dirty_relay_pass(tcp_packets_in_batch);
    }
    diag.pump = device.tun_ingress_pump_snapshot();
    metrics.note_tun_rx_backlog_guard(
        guard_after.is_active(),
        diag.budget_exhausted,
        guard_after.pause_edges(),
        guard_after.resume_edges(),
    );
    metrics.note_tun_rx_batch_service(
        diag.tcp_packets,
        diag.tcp_batches,
        diag.batch_dirty_relay_passes,
        diag.batch_iface_polls,
        diag.batch_flushes,
        diag.tcp_batch_packets_high_water,
        diag.avoided_dirty_relay_passes(),
        diag.avoided_iface_polls(),
        diag.pump.packets,
        diag.pump.queue_high_water,
        diag.pump.capacity_packets,
        diag.pump.full_waits,
        diag.pump.read_errors,
    );
    Ok(drained)
}

#[cfg(all(test, feature = "harness"))]
mod tests;
