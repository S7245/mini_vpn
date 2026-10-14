//! Original local egress service window, borrowing the driver's physical state.
//! Owns no task, registry or resource ledger. Every poll awaits the common flush;
//! ingress and dirty service retain their existing order and budgets.
//! Entry: service_local_egress_until. Tests: client_tun/tests local_egress_service
//! and d16_drain_only, plus the local_reset poll/flush provenance gate.
use super::super::*;

pub(in crate::client_tun) async fn service_local_egress_until<D, U, M>(
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
    tun_rx_drain_diag: &mut TunRxDrainDiag,
    local_egress_diag: &mut LocalEgressServiceDiag,
    relay_read_hard_pause: bool,
    local_egress_hard_pause: bool,
    buffered_downlink: BufferedDownlinkConfig,
    config: LocalEgressServiceConfig,
    egress_actor: bool,
) -> std::io::Result<LocalEgressServiceProgress>
where
    D: TunIo,
    U: OwnedUpstream + 'static,
    M: MetricsSink,
{
    let mut total = LocalEgressServiceProgress::default();
    if dirty.is_empty() {
        total.stop_reason = LocalEgressServiceStopReason::NoWork;
        record_local_egress_service_window(
            metrics,
            socket_ctxs,
            local_egress_diag,
            total,
            egress_actor,
        );
        return Ok(total);
    }
    let drop_debt_active = downlink_egress_drop_debt.has_active_drop_debt();
    let has_d16_flow = dirty_contains_d16_flow(dirty, socket_ctxs);
    if (local_egress_hard_pause || drop_debt_active) && !has_d16_flow {
        total.stop_reason = LocalEgressServiceStopReason::HardPause;
        record_local_egress_service_window(
            metrics,
            socket_ctxs,
            local_egress_diag,
            total,
            egress_actor,
        );
        return Ok(total);
    }
    transition_dirty_d16_egress_phases(
        dirty,
        sockets,
        socket_ctxs,
        downlink_backpressure,
        local_egress_hard_pause,
        tun_rx_drain_diag.backlog_guard().is_active(),
        drop_debt_active,
        None,
    );

    for _ in 0..config.max_cycles {
        let accepted_before =
            tcp_downlink_aggregate(socket_ctxs.values()).send_slice_accepted_bytes;
        let pressure_before_cycle =
            LocalEgressPressureSnapshot::capture(device, dirty, socket_ctxs, sockets);
        let send_queue_before_cycle = capture_dirty_send_queues(dirty, sockets);
        let mut cycle = LocalEgressServiceProgress {
            cycles: 1,
            ..LocalEgressServiceProgress::default()
        };

        let backlog_resume_edges_before = tun_rx_drain_diag.backlog_guard().resume_edges();
        let drained = drain_ready_tun_rx(
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
            tun_rx_drain_diag,
            config.tun_rx_packets_per_cycle,
            TUN_RX_DRAIN_SOURCE_LOCAL_EGRESS_SERVICE,
            relay_read_hard_pause || tun_rx_drain_diag.backlog_guard().is_active(),
            buffered_downlink,
        )
        .await?;
        cycle.tun_rx_packets = drained;
        let backlog_resumed_this_cycle =
            tun_rx_drain_diag.backlog_guard().resume_edges() > backlog_resume_edges_before;

        metrics.enter_poll();
        let timestamp = smoltcp::time::Instant::now();
        iface.poll(timestamp, device, sockets);
        super::ingress_effects::collect(iface, sockets, dirty, handshake_done_tx.flow_mode());
        *tcp_loop_flush_tx_calls = tcp_loop_flush_tx_calls.saturating_add(1);
        cycle.flush_tx_calls = 1;
        let (flush_result, flush_release) = flush_tx_and_release_downlink_permits(
            device,
            sockets,
            socket_ctxs,
            "local_egress_service",
        )
        .await;
        let pressure_after_cycle =
            LocalEgressPressureSnapshot::capture(device, dirty, socket_ctxs, sockets);
        if let Err(e) = flush_result {
            *tcp_loop_flush_tx_failures = tcp_loop_flush_tx_failures.saturating_add(1);
            cycle.flush_tx_failures = 1;
            tcp_diag_log!(
                "🔎 tcp-loop-flush-tx-fail stage=local_egress_service calls={} failures={} err={e}",
                *tcp_loop_flush_tx_calls,
                *tcp_loop_flush_tx_failures
            );
        } else {
            cycle.egress_drain_bytes = pressure_before_cycle
                .drain_bytes_to(pressure_after_cycle)
                .max(flush_release.flushed_tcp_payload_bytes)
                .max(flush_release.released_inflight_permit_bytes);
        }
        if flush_release.flushed_tcp_payload_bytes == 0 {
            let released_inflight = release_dirty_downlink_inflight_permits_for_send_queue_drain(
                &send_queue_before_cycle,
                sockets,
                socket_ctxs,
            );
            cycle.egress_drain_bytes = cycle.egress_drain_bytes.max(released_inflight);
        }
        metrics.leave_poll();

        let close_egress_guard = downlink_egress_drop_debt.has_active_drop_debt();
        let relay_read_recovery_active = downlink_egress_drop_debt.has_active_drop_debt();
        let tun_rx_backlog_active_for_control_epoch = tun_rx_drain_diag.backlog_guard().is_active();
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
            relay_read_hard_pause || tun_rx_drain_diag.backlog_guard().is_active(),
            buffered_downlink,
            if backlog_resumed_this_cycle || tun_rx_backlog_active_for_control_epoch {
                DownlinkAdmissionOrigin::ControlOnly
            } else {
                DownlinkAdmissionOrigin::EgressActor
            },
        )
        .await;
        cycle.dirty_passes = 1;

        if backlog_resumed_this_cycle {
            recover_all_eligible_d16_flows_after_clean_tun_rx_edge(
                dirty,
                sockets,
                socket_ctxs,
                downlink_backpressure,
                downlink_egress_drop_debt.has_active_drop_debt(),
            );
        }

        let accepted_after = tcp_downlink_aggregate(socket_ctxs.values()).send_slice_accepted_bytes;
        cycle.accepted_bytes = accepted_after.saturating_sub(accepted_before) as usize;
        if tun_rx_backlog_active_for_control_epoch
            && cycle.flush_tx_failures == 0
            && cycle.accepted_bytes == 0
        {
            tun_rx_drain_diag.note_control_epoch_completed();
        }
        transition_dirty_d16_egress_phases(
            dirty,
            sockets,
            socket_ctxs,
            downlink_backpressure,
            local_egress_hard_pause,
            tun_rx_drain_diag.backlog_guard().is_active(),
            drop_debt_active,
            (cycle.flush_tx_failures == 0).then_some(cycle.egress_drain_bytes),
        );

        total.cycles = total.cycles.saturating_add(cycle.cycles);
        total.accepted_bytes = total.accepted_bytes.saturating_add(cycle.accepted_bytes);
        total.egress_drain_bytes = total
            .egress_drain_bytes
            .saturating_add(cycle.egress_drain_bytes);
        total.tun_rx_packets = total.tun_rx_packets.saturating_add(cycle.tun_rx_packets);
        total.flush_tx_calls = total.flush_tx_calls.saturating_add(cycle.flush_tx_calls);
        total.flush_tx_failures = total
            .flush_tx_failures
            .saturating_add(cycle.flush_tx_failures);
        total.dirty_passes = total.dirty_passes.saturating_add(cycle.dirty_passes);

        if total.accepted_bytes >= config.target_bytes_per_window {
            total.stop_reason = LocalEgressServiceStopReason::TargetReached;
            record_local_egress_service_window(
                metrics,
                socket_ctxs,
                local_egress_diag,
                total,
                egress_actor,
            );
            return Ok(total);
        }
        if !cycle.has_progress() {
            total.stop_reason = LocalEgressServiceStopReason::NoProgress;
            record_local_egress_service_window(
                metrics,
                socket_ctxs,
                local_egress_diag,
                total,
                egress_actor,
            );
            return Ok(total);
        }
        if dirty.is_empty() {
            total.stop_reason = LocalEgressServiceStopReason::NoWork;
            record_local_egress_service_window(
                metrics,
                socket_ctxs,
                local_egress_diag,
                total,
                egress_actor,
            );
            return Ok(total);
        }
        if local_egress_hard_pause || downlink_egress_drop_debt.has_active_drop_debt() {
            total.stop_reason = LocalEgressServiceStopReason::HardPause;
            record_local_egress_service_window(
                metrics,
                socket_ctxs,
                local_egress_diag,
                total,
                egress_actor,
            );
            return Ok(total);
        }
    }

    total.stop_reason = LocalEgressServiceStopReason::CycleBudget;
    record_local_egress_service_window(
        metrics,
        socket_ctxs,
        local_egress_diag,
        total,
        egress_actor,
    );
    Ok(total)
}
