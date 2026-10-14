//! Read-credit publication for the original event-loop sockets.
//!
//! Owns credit selection and watch publication for initial, current and projected
//! local storage pressure. Borrows physical socket/context state; owns no flow,
//! queue, task or session authority. Entries preserve the original root functions;
//! characterization tests remain in client_tun::tests.

use crate::client_tun::*;

pub(in crate::client_tun) fn d16_relay_read_credit(
    ctx: &SocketCtx,
    tun_rx_backlog_active: bool,
) -> Option<RelayReadCredit> {
    let queue = ctx.d16_downlink_queue.as_ref()?;
    let permissions = EgressPermissions::for_phase(ctx.d16_egress_phase);
    if tun_rx_backlog_active || !permissions.allow_read {
        return Some(RelayReadCredit {
            paused: true,
            max_batch_bytes: 0,
        });
    }
    let snapshot = queue.snapshot_now();
    // A pending D16 Quinn read already owns `reserved_bytes`. Treating that
    // reservation as unavailable feedback makes a full-capacity read pause and
    // cancel itself before remote readability can wake it. The reservation is
    // still part of the exact byte ledger; adding it back here only describes
    // the read opportunity already armed by this flow.
    let read_opportunity_bytes = snapshot
        .available_bytes()
        .saturating_add(snapshot.reserved_bytes);
    let max_batch_bytes = read_opportunity_bytes.min(permissions.max_read_quantum_bytes);
    Some(RelayReadCredit {
        paused: max_batch_bytes == 0,
        max_batch_bytes,
    })
}

pub(in crate::client_tun) fn initial_relay_read_credit(hard_pause_active: bool) -> RelayReadCredit {
    if hard_pause_active {
        RelayReadCredit {
            paused: true,
            max_batch_bytes: 0,
        }
    } else {
        RelayReadCredit::initial()
    }
}

pub(in crate::client_tun) fn publish_relay_read_credit_for_handle(
    handle: SocketHandle,
    sockets: &SocketSet<'_>,
    socket_ctxs: &mut HashMap<SocketHandle, SocketCtx>,
    cfg: DownlinkBackpressureConfig,
    tun_mtu: usize,
    downlink_flush_max_bytes: usize,
    egress_recovery_active: bool,
    hard_pause_active: bool,
    buffered_downlink: BufferedDownlinkConfig,
    global_pending_bytes: usize,
) {
    let snapshot = {
        let socket = sockets.get::<TcpSocket>(handle);
        SocketCloseSnapshot::from_socket(socket)
    };
    let Some(ctx) = socket_ctxs.get_mut(&handle) else {
        return;
    };
    if let Some(mirror) = &mut ctx.flow_mirror {
        mirror.publish_window(ctx.downlink_pending.len());
        let credit = mirror.window_credit();
        if let Some(tx) = &ctx.relay_read_credit_tx {
            if *tx.borrow() != credit {
                let _ = tx.send(credit);
            }
        }
        return;
    }
    let Some(tx) = ctx.relay_read_credit_tx.as_ref() else {
        return;
    };
    if let Some(credit) = d16_relay_read_credit(ctx, hard_pause_active) {
        if *tx.borrow() != credit {
            let _ = tx.send(credit);
        }
        return;
    }
    let pending_bytes = ctx.downlink_pending.len();
    if let Some(credit) =
        relay_read_credit_for_terminal_no_recv_window(snapshot, pending_bytes, cfg, tun_mtu)
    {
        if *tx.borrow() != credit {
            let _ = tx.send(credit);
        }
        return;
    }
    if buffered_downlink.enabled {
        let global_pending_bytes = global_pending_bytes.max(pending_bytes);
        let decision = buffered_downlink_read_credit(
            tx.borrow().paused,
            pending_bytes,
            global_pending_bytes,
            false,
            hard_pause_active,
            buffered_downlink,
        );
        if *tx.borrow() != decision.credit {
            let _ = tx.send(decision.credit);
            tcp_diag_log!(
                "{}",
                format_buffered_downlink_read_credit_diag(
                    handle,
                    ctx.conn_epoch,
                    decision,
                    pending_bytes,
                    global_pending_bytes,
                    buffered_downlink,
                )
            );
        }
        return;
    }
    let local_pressure_bytes = snapshot.send_queue.saturating_add(pending_bytes);
    let pressure_ack_drain_bytes = ctx
        .downlink_credit_controller
        .consume_ack_cadence_boost_for_publish(
            local_pressure_bytes,
            hard_pause_active,
            cfg,
            tun_mtu,
        );
    let credit = relay_read_credit_for_local_egress_with_ack_drain_floor(
        snapshot.send_queue,
        pending_bytes,
        egress_recovery_active,
        hard_pause_active,
        cfg,
        tun_mtu,
        pressure_ack_drain_bytes,
        downlink_flush_max_bytes,
    );
    let credit = ctx
        .downlink_credit_controller
        .read_credit_for_local_pressure(credit, pending_bytes, local_pressure_bytes, cfg, tun_mtu);
    let progress_generation = ctx.downlink_credit_controller.egress_progress_generation;
    let progress_wake = progress_generation != ctx.relay_read_credit_progress_generation_sent
        && !credit.paused
        && credit.max_batch_bytes > 0;
    let changed = *tx.borrow() != credit;
    if changed || progress_wake {
        match tx.send(credit) {
            Ok(()) if progress_wake => {
                ctx.relay_read_credit_progress_generation_sent = progress_generation;
            }
            _ => {}
        }
    }
}

pub(in crate::client_tun) fn publish_projected_relay_read_credit_for_payload(
    handle: SocketHandle,
    epoch: u64,
    incoming_bytes: usize,
    sockets: &SocketSet<'_>,
    socket_ctxs: &mut HashMap<SocketHandle, SocketCtx>,
    cfg: DownlinkBackpressureConfig,
    tun_mtu: usize,
    downlink_flush_max_bytes: usize,
    egress_recovery_active: bool,
    hard_pause_active: bool,
) {
    if incoming_bytes == 0 {
        return;
    }
    let snapshot = {
        let socket = sockets.get::<TcpSocket>(handle);
        SocketCloseSnapshot::from_socket(socket)
    };
    let Some(ctx) = socket_ctxs.get_mut(&handle) else {
        return;
    };
    if ctx.conn_epoch != epoch
        || remote_payload_disposition(ctx, snapshot) != RemotePayloadDisposition::Accept
    {
        return;
    }
    if let Some(mirror) = &mut ctx.flow_mirror {
        mirror.publish_window(ctx.downlink_pending.len());
        let credit = mirror.window_credit();
        if let Some(tx) = &ctx.relay_read_credit_tx {
            if *tx.borrow() != credit {
                let _ = tx.send(credit);
            }
        }
        return;
    }
    let Some(tx) = ctx.relay_read_credit_tx.as_ref() else {
        return;
    };
    if let Some(credit) = d16_relay_read_credit(ctx, hard_pause_active) {
        if *tx.borrow() != credit {
            let _ = tx.send(credit);
        }
        return;
    }
    let pending_bytes = ctx.downlink_pending.len();
    let projected_pending = pending_bytes.saturating_add(incoming_bytes);
    if let Some(credit) =
        relay_read_credit_for_terminal_no_recv_window(snapshot, projected_pending, cfg, tun_mtu)
    {
        if *tx.borrow() != credit {
            let _ = tx.send(credit);
        }
        return;
    }
    let projected_pressure_bytes = snapshot
        .send_queue
        .saturating_add(pending_bytes)
        .saturating_add(incoming_bytes);
    let pressure_ack_drain_bytes = ctx.downlink_credit_controller.adaptive_ack_drain_floor_for(
        projected_pressure_bytes,
        cfg,
        tun_mtu,
    );
    ctx.downlink_credit_controller
        .note_payload_read_for_projected_pressure(incoming_bytes, projected_pressure_bytes, cfg);
    let credit = relay_read_credit_for_projected_local_egress_with_ack_drain_floor(
        snapshot.send_queue,
        pending_bytes,
        incoming_bytes,
        egress_recovery_active,
        hard_pause_active,
        cfg,
        tun_mtu,
        pressure_ack_drain_bytes,
        downlink_flush_max_bytes,
    );
    let credit = ctx
        .downlink_credit_controller
        .read_credit_for_local_pressure(
            credit,
            projected_pending,
            projected_pressure_bytes,
            cfg,
            tun_mtu,
        );
    let progress_generation = ctx.downlink_credit_controller.egress_progress_generation;
    let progress_wake = progress_generation != ctx.relay_read_credit_progress_generation_sent
        && !credit.paused
        && credit.max_batch_bytes > 0;
    if *tx.borrow() != credit || progress_wake {
        match tx.send(credit) {
            Ok(()) if progress_wake => {
                ctx.relay_read_credit_progress_generation_sent = progress_generation;
            }
            _ => {}
        }
    }
}

pub(in crate::client_tun) fn publish_relay_read_credit_for_dirty_handles(
    dirty: &HashSet<SocketHandle>,
    sockets: &SocketSet<'_>,
    socket_ctxs: &mut HashMap<SocketHandle, SocketCtx>,
    cfg: DownlinkBackpressureConfig,
    tun_mtu: usize,
    downlink_flush_max_bytes: usize,
    egress_recovery_active: bool,
    hard_pause_active: bool,
    buffered_downlink: BufferedDownlinkConfig,
    global_pending_bytes: usize,
) {
    let handles: Vec<SocketHandle> = dirty.iter().copied().collect();
    for handle in handles {
        publish_relay_read_credit_for_handle(
            handle,
            sockets,
            socket_ctxs,
            cfg,
            tun_mtu,
            downlink_flush_max_bytes,
            egress_recovery_active,
            hard_pause_active,
            buffered_downlink,
            global_pending_bytes,
        );
    }
}

pub(in crate::client_tun) fn global_pending_for_buffered_credit(
    buffered_credit_enabled: bool,
    scan_pending: impl FnOnce() -> usize,
) -> usize {
    if buffered_credit_enabled {
        scan_pending()
    } else {
        0
    }
}

pub(in crate::client_tun) fn dirty_downlink_pending_total(
    dirty: &HashSet<SocketHandle>,
    socket_ctxs: &HashMap<SocketHandle, SocketCtx>,
) -> usize {
    dirty
        .iter()
        .filter_map(|handle| socket_ctxs.get(handle))
        .fold(0usize, |total, ctx| {
            total.saturating_add(ctx.pending_downlink_bytes())
        })
}
