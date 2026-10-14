//! Original remote-payload physical transaction, including its final flush.
//! Borrows loop socket/device/fake-IP state and enters the existing rearm gate.
//! No separate flow authority; entry is handle_remote_payload_from_loop.
//! Direct characterization remains in client_tun::tests, panic tests in the
//! saved compatibility session. Preserve payload admission before poll/flush.

use crate::client_tun::*;

pub(in crate::client_tun) async fn handle_remote_payload_from_loop<D: TunIo>(
    dispatch: &OpenDispatch,
    handle: SocketHandle,
    epoch: u64,
    payload: &[u8],
    request: RemotePayloadRequest<'_, '_, D>,
) -> std::io::Result<usize> {
    handle_remote_payload_with_gate(
        RearmGate::original_loop(dispatch),
        handle,
        epoch,
        payload,
        request,
    )
    .await
}

pub(in crate::client_tun) async fn handle_remote_payload_with_gate<D: TunIo>(
    gate: RearmGate<'_>,
    handle: SocketHandle,
    epoch: u64,
    payload: &[u8],
    request: RemotePayloadRequest<'_, '_, D>,
) -> std::io::Result<usize> {
    let RemotePayloadRequest {
        sockets,
        socket_ctxs,
        iface,
        device,
        fake_pool,
        now_secs,
        downlink_flush_max_bytes,
        downlink_egress_pacer,
        downlink_egress_drop_debt,
        downlink_backpressure,
        tun_mtu,
        buffered_downlink_enabled,
        downstream_permit,
        d3_egress_actor,
        d3_egress_actor_adaptive_credit,
        d3_egress_actor_legacy_credit,
        d5_capacity_backpressure,
    } = request;

    let tcp_socket = sockets.get_mut::<TcpSocket>(handle);
    let Some(ctx) = socket_ctxs.get_mut(&handle) else {
        return Ok(0);
    };
    if ctx.conn_epoch != epoch {
        trace_log!(
            "🗑️ handle {:?} 丢弃旧 relay data(epoch {epoch}≠{}) {}B",
            handle,
            ctx.conn_epoch,
            payload.len()
        );
        return Ok(0);
    }

    if payload.is_empty() {
        trace_log!("🔄 handle {:?} entering {:?}", handle, SocketState::Closing);
        legacy_reasoned_rearm_with_gate(
            gate,
            handle,
            tcp_socket,
            ctx,
            fake_pool,
            now_secs,
            "remote_to_local",
            "remote_eof",
        );
        return Ok(0);
    }

    let before_payload_snapshot = SocketCloseSnapshot::from_socket(tcp_socket);
    match remote_payload_disposition(ctx, before_payload_snapshot) {
        RemotePayloadDisposition::Accept => {}
        RemotePayloadDisposition::StaleRelayReset => {
            trace_log!(
                "🗑️ handle {:?} 已复位，丢弃旧连接迟到回程 {} 字节",
                handle,
                payload.len()
            );
            return Ok(0);
        }
        RemotePayloadDisposition::TerminalNoSend => {
            if !gate.terminal_payload(handle, ctx, before_payload_snapshot, payload.len()) {
                return Ok(0);
            }
            ctx.downlink_diag
                .note_terminal_late_remote_payload(payload.len());
            tcp_diag_log!(
                "🔎 tcp-terminal-remote-payload handle={:?} bytes={} total_bytes={} events={} pending={} tcp_state={:?} active={} can_send={} can_recv={} may_send={} may_recv={} send_capacity={} send_queue={} recv_queue={}",
                handle,
                payload.len(),
                ctx.downlink_diag.terminal_late_remote_payload_bytes,
                ctx.downlink_diag.terminal_late_remote_payload_events,
                ctx.downlink_pending.len(),
                before_payload_snapshot.tcp_state,
                before_payload_snapshot.active,
                before_payload_snapshot.can_send,
                before_payload_snapshot.can_recv,
                before_payload_snapshot.may_send,
                before_payload_snapshot.may_recv,
                before_payload_snapshot.send_capacity,
                before_payload_snapshot.send_queue,
                before_payload_snapshot.recv_queue
            );
            legacy_reasoned_rearm_with_snapshot_and_gate(
                gate,
                handle,
                tcp_socket,
                ctx,
                fake_pool,
                now_secs,
                "remote_to_local",
                "terminal_remote_payload",
                Some(before_payload_snapshot),
            );
            return Ok(0);
        }
    }

    // 不直接 send_slice（会丢写不下的字节）：先入下行 pending，再尽量 flush；
    // 剩余字节由主循环每轮 poll 持续推进（TCP ACK 释放 buffer 后继续）。
    compatibility_runtime::FlowMirror::accept_data(ctx, payload, downstream_permit.take());
    #[cfg(all(test, feature = "harness"))]
    if let RearmGate::OriginalLoop(dispatch) = gate {
        dispatch.observe_payload_transfer(payload);
    }
    ctx.downlink_diag
        .note_remote_payload(payload.len(), ctx.downlink_pending.len());
    note_local_finish_remote_progress(ctx, now_secs);
    if d3_egress_actor {
        ctx.downlink_d3_actor_flow = true;
        ctx.downlink_d3_actor_adaptive_credit = d3_egress_actor_adaptive_credit;
        ctx.downlink_d3_actor_legacy_credit = d3_egress_actor_legacy_credit;
        ctx.downlink_d5_capacity_backpressure = d5_capacity_backpressure;
        note_downlink_pending_progress(ctx, now_secs, 0);
        ctx.state = SocketState::Relaying;
        let snapshot = SocketCloseSnapshot::from_socket(tcp_socket);
        log_tcp_lifecycle_observation(handle, ctx, snapshot, now_secs, "remote_payload_actor");
        if should_log_tcp_reverse_window(ctx, snapshot, now_secs) {
            tcp_diag_log!(
                "{}",
                format_tcp_reverse_window_diag(
                    handle,
                    TcpReverseWindowDiag {
                        ctx_state: ctx.state,
                        payload_bytes: payload.len(),
                        accepted_bytes: 0,
                        pending: ctx.downlink_pending.len(),
                        remote_to_global_rx_bytes: ctx.downlink_diag.remote_to_global_rx_bytes,
                        local_fin_sent: ctx.local_fin_sent,
                        snapshot,
                    },
                )
            );
        }
        return Ok(0);
    }
    let flush_outcome = admit_downlink_for_handle(
        handle,
        tcp_socket,
        ctx,
        downlink_flush_max_bytes,
        downlink_backpressure,
        tun_mtu,
        downlink_egress_drop_debt,
        DownlinkAdmissionOrigin::ControlOnly,
    );
    let accepted_bytes = flush_outcome.accepted_bytes;
    let snapshot = SocketCloseSnapshot::from_socket(tcp_socket);
    let post_flush_projected_pressure =
        downlink_projected_local_pressure_bytes(snapshot.send_queue, ctx.downlink_pending.len());
    let installed_projected_debt = if buffered_downlink_enabled {
        0
    } else {
        downlink_egress_drop_debt.note_post_flush_projected_egress_pressure(
            snapshot.send_queue,
            ctx.downlink_pending.len(),
            accepted_bytes,
            downlink_backpressure,
        )
    };
    if installed_projected_debt > 0 {
        tcp_diag_log!(
            "{}",
            format_downlink_egress_credit_debt_diag(
                "post_flush_projected_payload_credit_edge",
                installed_projected_debt,
                DownlinkPressureStats {
                    max_pending: ctx.downlink_pending.len(),
                    total_pending: ctx.downlink_pending.len(),
                    max_tx_queue: post_flush_projected_pressure,
                    total_tx_queue: post_flush_projected_pressure,
                },
                downlink_backpressure,
                *downlink_egress_drop_debt,
            )
        );
    }
    note_downlink_pending_progress(ctx, now_secs, accepted_bytes);
    ctx.state = SocketState::Relaying;
    log_tcp_lifecycle_observation(handle, ctx, snapshot, now_secs, "remote_payload");
    if should_log_tcp_reverse_window(ctx, snapshot, now_secs) {
        tcp_diag_log!(
            "{}",
            format_tcp_reverse_window_diag(
                handle,
                TcpReverseWindowDiag {
                    ctx_state: ctx.state,
                    payload_bytes: payload.len(),
                    accepted_bytes,
                    pending: ctx.downlink_pending.len(),
                    remote_to_global_rx_bytes: ctx.downlink_diag.remote_to_global_rx_bytes,
                    local_fin_sent: ctx.local_fin_sent,
                    snapshot,
                },
            )
        );
    }

    let allow_immediate_flush = if flush_outcome.headroom_limited && accepted_bytes > 0 {
        downlink_egress_pacer.allow_headroom_limited_flush(accepted_bytes)
    } else {
        downlink_egress_pacer.allow_remote_payload_flush(
            accepted_bytes,
            ctx.downlink_pending.len(),
            snapshot.send_queue,
            downlink_backpressure,
        )
    };
    if !allow_immediate_flush {
        if accepted_bytes > 0 {
            ctx.downlink_diag.note_tun_flush_deferred();
        }
        return Ok(accepted_bytes);
    }

    let timestamp = smoltcp::time::Instant::now();
    iface.poll(timestamp, device, sockets);
    let (result, _) =
        flush_tx_and_release_downlink_permits(device, sockets, socket_ctxs, "remote_payload").await;
    let ok = result.is_ok();
    if let Some(ctx) = socket_ctxs.get_mut(&handle) {
        ctx.downlink_diag.note_tun_flush(ok);
    }
    if let Err(e) = &result {
        tcp_diag_log!(
            "🔎 tcp-tun-flush-fail handle={:?} stage=remote_payload err={e}",
            handle
        );
    }
    // Even if this flush fails, accepted bytes are already queued in smoltcp and
    // must keep the handle dirty for tx-queue-aware backpressure.
    Ok(accepted_bytes)
}
