//! Listener activity and handshake completion for the original event loop.
//!
//! This Module borrows root-owned sockets, contexts and fake-IP state. It owns
//! the existing downlink/terminal, open-admission and local-uplink decisions;
//! the root keeps their lifetimes and the rearm gateway keeps physical reset.
//! ListenerActivity::process and HandshakeCompletion own each complete transaction.
//! The activity helpers preserve
//! the distinct Legacy and Resumable receive order. No state escapes the turn.
//! Characterization lives in client_tun/tests compatibility flow/open/rearm leaves
//! and harness::tests::knife16_resumable_adapter; this Move adds no F custody.

use super::super::*;

pub(in crate::client_tun) struct ListenerActivity<'a, 'storage, U> {
    pub(in crate::client_tun) sockets: &'a mut SocketSet<'storage>,
    pub(in crate::client_tun) socket_ctxs: &'a mut HashMap<SocketHandle, SocketCtx>,
    pub(in crate::client_tun) upstream: &'a Arc<U>,
    pub(in crate::client_tun) handshake_done_tx: &'a OpenDispatch,
    pub(in crate::client_tun) global_tx: &'a mpsc::Sender<(SocketHandle, RelayEvent)>,
    pub(in crate::client_tun) owned_action_tx: &'a mpsc::Sender<OwnedActionEvent>,
    pub(in crate::client_tun) fake_pool: &'a mut FakeIpPool,
    pub(in crate::client_tun) now_secs: u64,
    pub(in crate::client_tun) metrics_handle: &'a Metrics,
    pub(in crate::client_tun) downlink_flush_max_bytes: usize,
    pub(in crate::client_tun) downlink_backpressure: DownlinkBackpressureConfig,
    pub(in crate::client_tun) tun_mtu: usize,
    pub(in crate::client_tun) downlink_egress_drop_debt: &'a mut DownlinkEgressDropDebt,
    pub(in crate::client_tun) close_egress_drop_guard: bool,
    pub(in crate::client_tun) relay_read_hard_pause: bool,
    pub(in crate::client_tun) admission_origin: DownlinkAdmissionOrigin,
}

impl<U: OwnedUpstream + 'static> ListenerActivity<'_, '_, U> {
    pub(in crate::client_tun) async fn process(
        &mut self,
        handle: SocketHandle,
    ) -> Result<(), ClientError> {
        if let Some(ctx) = self
            .socket_ctxs
            .get_mut(&handle)
            .filter(|ctx| ctx.local_reset.is_some())
        {
            legacy_bare_rearm_from_loop(
                self.handshake_done_tx,
                handle,
                self.sockets.get_mut::<TcpSocket>(handle),
                ctx,
                self.fake_pool,
                self.now_secs,
            );
            return Ok(());
        }
        // 每轮先推进该 handle 的下行 pending：TCP ACK 释放 tx buffer 空间后继续写，
        // 直到把上一轮没写完的回程字节全部交付，绝不丢字节（修 bad decrypt 的另一半）。
        {
            let tcp_socket = self.sockets.get_mut::<TcpSocket>(handle);
            if let Some(ctx) = self.socket_ctxs.get_mut(&handle) {
                let graceful_owned_terminal = matches!(
                    ctx.resumable_terminal,
                    Some(TerminalAction::FlowFinished(FlowFinishReason::Graceful))
                );
                if graceful_owned_terminal && !tcp_socket.is_active() {
                    // Cross the no-F gateway before clearing terminal capability.
                    let intent = RearmIntent::bare_ctx(handle, ctx);
                    if !gate_legacy_rearm(RearmGate::original_loop(self.handshake_done_tx), intent)
                    {
                        return Ok(());
                    }
                    ctx.resumable_terminal = None;
                    ctx.resumable_action_bridge = None;
                    ctx.resumable_uplink = None;
                    legacy_bare_rearm_from_loop(
                        self.handshake_done_tx,
                        handle,
                        tcp_socket,
                        ctx,
                        self.fake_pool,
                        self.now_secs,
                    );
                    return Ok(());
                }
                let flush_outcome = admit_downlink_for_handle(
                    handle,
                    tcp_socket,
                    ctx,
                    self.downlink_flush_max_bytes,
                    self.downlink_backpressure,
                    self.tun_mtu,
                    self.downlink_egress_drop_debt,
                    self.admission_origin,
                );
                note_downlink_pending_progress(ctx, self.now_secs, flush_outcome.accepted_bytes);
                let terminal_disposition_pending = ctx.resumable_half_close.is_some()
                    || (ctx.pending_relay_close.is_none()
                        && d16_remote_eof_queue_state(ctx)
                            .is_some_and(|(closed, empty)| closed && empty));
                if terminal_disposition_pending {
                    if !gate_legacy_rearm(
                        RearmGate::original_loop(self.handshake_done_tx),
                        RearmIntent::reasoned_ctx(
                            handle,
                            ctx,
                            "remote_to_local",
                            "listener_terminal_disposition",
                            None,
                        ),
                    ) {
                        return Ok(());
                    }
                }
                let _ = apply_resumable_half_close(tcp_socket, ctx);
                if install_d16_remote_eof_if_ready(ctx, self.now_secs) {
                    tcp_diag_log!(
                        "🔎 tcp-d16-remote-eof-observed handle={:?} epoch={} pending={} inflight_permit_bytes={}",
                        handle,
                        ctx.conn_epoch,
                        ctx.downlink_pending.len(),
                        ctx.downlink_inflight_permit_bytes()
                    );
                }
                if finish_deferred_relay_close_from_loop(
                    self.handshake_done_tx,
                    handle,
                    tcp_socket,
                    ctx,
                    self.fake_pool,
                    self.now_secs,
                    self.downlink_backpressure,
                    self.close_egress_drop_guard || ctx_has_close_egress_pressure_evidence(ctx),
                ) {
                    return Ok(());
                }
            }
        }

        if self.socket_ctxs.get(&handle).is_some_and(|ctx| {
            ctx.flow_mirror
                .as_ref()
                .is_some_and(|mirror| mirror.producer_stopped || mirror.quarantined)
        }) {
            return Ok(());
        }

        // Knife16 Task 3: protocol-owned flows get first claim on uplink service.
        // The pump reserves exact replay ownership before dequeuing smoltcp bytes.
        let resumable_uplink = {
            let Some(ctx) = self.socket_ctxs.get_mut(&handle) else {
                return Ok(());
            };
            let tcp_socket = self.sockets.get_mut::<TcpSocket>(handle);
            ctx.downlink_diag
                .note_uplink_recv_queue(tcp_socket.recv_queue());
            let snapshot = SocketCloseSnapshot::from_socket(tcp_socket);
            pump_resumable_uplink(ctx, tcp_socket, snapshot)
        };
        match resumable_uplink {
            EstablishedUplink::Handled => return Ok(()),
            EstablishedUplink::Closed => {
                let tcp_socket = self.sockets.get_mut::<TcpSocket>(handle);
                if let Some(ctx) = self.socket_ctxs.get_mut(&handle) {
                    legacy_reasoned_rearm_from_loop(
                        self.handshake_done_tx,
                        handle,
                        tcp_socket,
                        ctx,
                        self.fake_pool,
                        self.now_secs,
                        "local_to_owner",
                        "owned_uplink_closed",
                    );
                }
                return Ok(());
            }
            EstablishedUplink::NotEstablished => {}
        }

        // 刀13 ② / 刀14s：已建立 relay 的上行必须非阻塞。先抢 mpsc permit，再从 smoltcp 取字节；
        // Full 时不 recv、不分配、不打印，让字节留在 smoltcp rx buffer → TCP 窗口自然背压 app。
        // 14s 起每个 dirty pass 抽一批 payload，避免退化成“每 5ms timer 只转发一个 MSS”。
        let established_uplink = {
            let Some(ctx) = self.socket_ctxs.get_mut(&handle) else {
                return Ok(());
            };
            let tcp_socket = self.sockets.get_mut::<TcpSocket>(handle);
            ctx.downlink_diag
                .note_uplink_recv_queue(tcp_socket.recv_queue());
            let tcp_state = tcp_socket.state();
            pump_established_uplink(ctx, tcp_state, self.now_secs, || {
                extract_socket_payload(tcp_socket)
            })
        };
        match established_uplink {
            EstablishedUplink::Handled => return Ok(()),
            EstablishedUplink::Closed => {
                let tcp_socket = self.sockets.get_mut::<TcpSocket>(handle);
                if let Some(ctx) = self.socket_ctxs.get_mut(&handle) {
                    if ctx.local_fin_sent {
                        ctx.uplink_tx = None;
                        return Ok(());
                    }
                    let snapshot = SocketCloseSnapshot::from_socket(tcp_socket);
                    let close = RelayClose {
                        epoch: ctx.conn_epoch,
                        direction: "local_to_remote",
                        reason: "uplink_channel_closed",
                    };
                    if !gate_legacy_rearm(
                        RearmGate::original_loop(self.handshake_done_tx),
                        RearmIntent::reasoned_ctx(handle, ctx, close.direction, close.reason, None),
                    ) {
                        return Ok(());
                    }
                    if start_deferred_close_for_pending_downlink(ctx, close, self.now_secs) {
                        tcp_diag_log!(
                            "🔎 tcp-deferred-close-pending handle={:?} direction={} reason={} pending={} started_secs={}",
                            handle,
                            close.direction,
                            close.reason,
                            ctx.downlink_pending.len(),
                            self.now_secs
                        );
                        return Ok(());
                    }
                    if start_deferred_close_for_egress_if_needed(
                        ctx,
                        close,
                        snapshot,
                        self.downlink_backpressure,
                        self.now_secs,
                    ) {
                        tcp_diag_log!(
                            "🔎 tcp-deferred-close-egress handle={:?} direction={} reason={} send_queue={} high={} low={} started_secs={}",
                            handle,
                            close.direction,
                            close.reason,
                            snapshot.send_queue,
                            self.downlink_backpressure.high_bytes,
                            self.downlink_backpressure.low_bytes,
                            self.now_secs
                        );
                        return Ok(());
                    }
                    legacy_reasoned_rearm_from_loop(
                        self.handshake_done_tx,
                        handle,
                        tcp_socket,
                        ctx,
                        self.fake_pool,
                        self.now_secs,
                        "local_to_remote",
                        "uplink_channel_closed",
                    );
                }
                return Ok(());
            }
            EstablishedUplink::NotEstablished => {}
        }

        if self.upstream.tcp_ownership_mode() == TcpOwnershipMode::Resumable {
            return self.open_resumable_listener_if_ready(handle).await;
        }

        // A local peer may complete SYN then immediately FIN without sending a
        // payload. Legacy mode has nothing to open upstream for, but the listener
        // incarnation still must be reclaimed instead of remaining forever in a
        // logical Listening state over a CloseWait/Closed smoltcp socket.
        let legacy_empty_close = {
            let socket = self.sockets.get::<TcpSocket>(handle);
            socket.recv_queue() == 0
                && matches!(
                    socket.state(),
                    TcpState::CloseWait
                        | TcpState::Closing
                        | TcpState::TimeWait
                        | TcpState::LastAck
                        | TcpState::Closed
                )
        };
        if legacy_empty_close {
            let socket = self.sockets.get_mut::<TcpSocket>(handle);
            if let Some(ctx) = self.socket_ctxs.get_mut(&handle) {
                legacy_reasoned_rearm_from_loop(
                    self.handshake_done_tx,
                    handle,
                    socket,
                    ctx,
                    self.fake_pool,
                    self.now_secs,
                    "local",
                    "empty_local_close",
                );
            }
            return Ok(());
        }

        let Some(endpoint) = self.sockets.get::<TcpSocket>(handle).local_endpoint() else {
            return Ok(());
        };
        if !self.sockets.get::<TcpSocket>(handle).can_recv() {
            return Ok(());
        }

        // Stage 11：fake-IP → 查表换域名（DomainPort）；非 fake → IpPort；fake 无映射 → 拒绝。
        let (target, fake_ip) = match resolve_target(endpoint, self.fake_pool) {
            TargetResolve::Direct { target, fake_ip } => (target, fake_ip),
            TargetResolve::Refuse => {
                trace_log!(
                    "🚫 fake-IP {} 无映射，拒绝连接（请重新解析）",
                    endpoint.addr
                );
                let tcp_socket = self.sockets.get_mut::<TcpSocket>(handle);
                if let Some(ctx) = self.socket_ctxs.get_mut(&handle) {
                    legacy_reasoned_rearm_from_loop(
                        self.handshake_done_tx,
                        handle,
                        tcp_socket,
                        ctx,
                        self.fake_pool,
                        self.now_secs,
                        "target",
                        "fake_ip_refuse",
                    );
                }
                return Ok(());
            }
            // 刀4：加密 DNS（DoT :853 / DoH :443）→ RST（rearm），逼应用回落明文 DNS。
            TargetResolve::Block => {
                // 解析 fake-IP 回域名供日志（low-rate TCP block 路径，便于核对命中端点 / 调 DoH 名单）；
                // :853/DoH-IP（非 fake-IP）则显示 IP。
                let who = match std::net::IpAddr::from(endpoint.addr) {
                    std::net::IpAddr::V4(v4) => self
                        .fake_pool
                        .resolve(v4)
                        .unwrap_or_else(|| endpoint.addr.to_string()),
                    _ => endpoint.addr.to_string(),
                };
                println!(
                    "🛡️ 阻断加密 DNS {who} (@{}:{})（→ RST，逼回落明文 DNS）",
                    endpoint.addr, endpoint.port
                );
                let tcp_socket = self.sockets.get_mut::<TcpSocket>(handle);
                if let Some(ctx) = self.socket_ctxs.get_mut(&handle) {
                    legacy_reasoned_rearm_from_loop(
                        self.handshake_done_tx,
                        handle,
                        tcp_socket,
                        ctx,
                        self.fake_pool,
                        self.now_secs,
                        "policy",
                        "encrypted_dns_block",
                    );
                }
                return Ok(());
            }
        };

        if self.handshake_done_tx.flow_mode() {
            if let Some(ctx) = self.socket_ctxs.get_mut(&handle) {
                self.handshake_done_tx.admit_flow(
                    self.upstream.clone(),
                    compatibility_runtime::FlowAdmission {
                        handle,
                        socket: self.sockets.get_mut::<TcpSocket>(handle),
                        ctx,
                        fake_pool: self.fake_pool,
                        fake_ip,
                        target,
                        now: self.now_secs,
                    },
                );
            }
            return Ok(());
        }
        let Some(ctx) = self.socket_ctxs.get(&handle) else {
            return Ok(());
        };
        let Some(payload) =
            self.handshake_done_tx
                .extract(handle, ctx, self.sockets.get_mut::<TcpSocket>(handle))
        else {
            return Ok(());
        };
        self.handle_local_payload(handle, payload, Some(target), fake_ip)
            .await
    }

    async fn open_resumable_listener_if_ready(
        &mut self,
        handle: SocketHandle,
    ) -> Result<(), ClientError> {
        let Some(ctx) = self.socket_ctxs.get_mut(&handle) else {
            return Ok(());
        };
        if ctx.state == SocketState::HandshakePending {
            return Ok(());
        }
        if ctx.resumable_uplink.is_some() || ctx.uplink_tx.is_some() {
            return Err(ClientError::Io(std::io::Error::other(
                "attempted a second upstream install for one socket incarnation",
            )));
        }

        let endpoint = {
            let socket = self.sockets.get_mut::<TcpSocket>(handle);
            let snapshot = SocketCloseSnapshot::from_socket(socket);
            if !socket.can_recv() && !resumable_local_source_eof(snapshot) {
                return Ok(());
            }
            socket.local_endpoint()
        };
        let Some(endpoint) = endpoint else {
            return Ok(());
        };
        let (target, fake_ip) = match resolve_target(endpoint, self.fake_pool) {
            TargetResolve::Direct { target, fake_ip } => (target, fake_ip),
            TargetResolve::Refuse => {
                let socket = self.sockets.get_mut::<TcpSocket>(handle);
                legacy_reasoned_rearm_from_loop(
                    self.handshake_done_tx,
                    handle,
                    socket,
                    ctx,
                    self.fake_pool,
                    self.now_secs,
                    "target",
                    "fake_ip_refuse",
                );
                return Ok(());
            }
            TargetResolve::Block => {
                let socket = self.sockets.get_mut::<TcpSocket>(handle);
                legacy_reasoned_rearm_from_loop(
                    self.handshake_done_tx,
                    handle,
                    socket,
                    ctx,
                    self.fake_pool,
                    self.now_secs,
                    "policy",
                    "encrypted_dns_block",
                );
                return Ok(());
            }
        };

        if self.upstream.open_is_cheap() {
            ctx.state = SocketState::OpeningRemote;
            let opened = with_tcp_relay_open_diag(
                TcpRelayOpenDiag {
                    handle: format!("{handle:?}"),
                    epoch: ctx.conn_epoch,
                },
                self.upstream.open_tcp_flow(&target),
            )
            .await;
            match opened {
                Ok(OpenedOwnedTcp::Resumable(flow)) => {
                    if let Some(ip) = fake_ip {
                        self.fake_pool.acquire(ip, self.now_secs);
                        ctx.fake_ip = Some(ip);
                    }
                    install_resumable_tcp_flow(
                        handle,
                        ctx.conn_epoch,
                        ctx,
                        flow,
                        self.owned_action_tx,
                        self.relay_read_hard_pause,
                    );
                    Ok(())
                }
                Ok(OpenedOwnedTcp::Legacy(_)) => {
                    let socket = self.sockets.get_mut::<TcpSocket>(handle);
                    legacy_reasoned_rearm_from_loop(
                        self.handshake_done_tx,
                        handle,
                        socket,
                        ctx,
                        self.fake_pool,
                        self.now_secs,
                        "remote_open",
                        "ownership_mode_mismatch",
                    );
                    Ok(())
                }
                Err(error) => {
                    let socket = self.sockets.get_mut::<TcpSocket>(handle);
                    legacy_reasoned_rearm_from_loop(
                        self.handshake_done_tx,
                        handle,
                        socket,
                        ctx,
                        self.fake_pool,
                        self.now_secs,
                        "remote_open",
                        "resumable_open_failed",
                    );
                    Err(error)
                }
            }
        } else {
            ctx.state = SocketState::HandshakePending;
            ctx.pending_open_mode = Some(TcpOwnershipMode::Resumable);
            ctx.conn_epoch = ctx.conn_epoch.wrapping_add(1);
            let epoch = ctx.conn_epoch;
            if let Some(ip) = fake_ip {
                self.fake_pool.acquire(ip, self.now_secs);
                ctx.fake_ip = Some(ip);
            }
            // Unlike the legacy path, no local DATA is extracted or copied while
            // this open is in flight. `uplink_buffer` must remain empty.
            debug_assert!(ctx.uplink_buffer.is_empty());
            self.handshake_done_tx
                .spawn(Arc::clone(self.upstream), target, handle, epoch);
            Ok(())
        }
    }

    async fn handle_local_payload(
        &mut self,
        handle: SocketHandle,
        payload: Vec<u8>,
        target: Option<TargetAddr>,
        fake_ip: Option<Ipv4Addr>,
    ) -> Result<(), ClientError> {
        if self.upstream.tcp_ownership_mode() != TcpOwnershipMode::LegacyRelay {
            return Err(ClientError::Io(std::io::Error::other(
                "legacy payload path selected for a resumable upstream",
            )));
        }
        let Some(ctx) = self.socket_ctxs.get_mut(&handle) else {
            return Ok(());
        };

        // Async remote-open 在飞中 → 后续上行包入 buffer（防双开 + 保序），不再开第二次 open。
        if ctx.state == SocketState::HandshakePending {
            if !ctx.buffer_uplink(&payload) {
                trace_log!(
                    "⚠️ handle {:?} remote open 在飞、上行缓存超上限({}KB)，丢弃 {}B（应用 TCP 背压自愈）",
                    handle,
                    MAX_UPLINK_BUFFER / 1024,
                    payload.len()
                );
            }
            return Ok(());
        }

        // 首次开远端必须有提取出的 Target；理论上首包时连接已 Established，local_endpoint 不应为 None。
        // 中文要点：缺 Target 时记录并跳过，绝不 panic、绝不退回写死地址。
        let Some(target) = target else {
            trace_log!("⚠️ handle {:?} 无 local_endpoint，跳过开远端", handle);
            return Ok(());
        };

        if self.upstream.open_is_cheap() && self.handshake_done_tx.inline_allowed() {
            // —— 明确廉价的上游：inline 开远端（主要用于测试/mock 轻路径）。——
            ctx.state = SocketState::OpeningRemote;
            trace_log!(
                "🎯 handle {:?} extracted target {}",
                handle,
                target.to_wire_string()
            );
            trace_log!("🔄 handle {:?} entering {:?}", handle, ctx.state);
            let stream = with_tcp_relay_open_diag(
                TcpRelayOpenDiag {
                    handle: format!("{handle:?}"),
                    epoch: ctx.conn_epoch,
                },
                open_legacy_owned_tcp(self.upstream.as_ref(), &target),
            )
            .await?;
            trace_log!("🚪 handle {:?} remote session opened", handle);

            let (tx, rx) = tokio::sync::mpsc::channel(RELAY_CHANNEL_CAPACITY);
            if let Err(e) = tx.try_send(RelayCommand::Data(payload)) {
                trace_log!(
                    "❌ handle {:?} 新建中继通道写入失败({e}) → rearm（不静默丢首包）",
                    handle
                );
                let tcp_socket = self.sockets.get_mut::<TcpSocket>(handle);
                legacy_reasoned_rearm_from_loop(
                    self.handshake_done_tx,
                    handle,
                    tcp_socket,
                    ctx,
                    self.fake_pool,
                    self.now_secs,
                    "local_to_remote",
                    "initial_uplink_send_failed",
                );
                return Ok(());
            }
            ctx.uplink_tx = Some(tx);
            ctx.state = SocketState::Relaying;
            // 刀2 引用计数：inline 路径开远端成功后才 acquire（开失败走 `?` 早返回，不泄漏 refcount）。
            if let Some(ip) = fake_ip {
                self.fake_pool.acquire(ip, self.now_secs);
                ctx.fake_ip = Some(ip);
            }
            let spawned = spawn_remote_relay(
                self.handshake_done_tx.relay_policy(),
                handle,
                ctx.conn_epoch,
                stream,
                rx,
                self.global_tx.clone(),
                self.metrics_handle,
                self.downlink_backpressure,
                initial_relay_read_credit(self.relay_read_hard_pause),
            );
            install_spawned_remote_relay(ctx, spawned, self.relay_read_hard_pause);
            Ok(())
        } else {
            // —— 不廉价上游：把远端 TCP open spawn 出主循环并发化（刀9 M3 / 刀14d）——
            // 主循环立即返回处理其它 flow，不被这条慢 open stall。完成后经 handshake_done channel 回灌。
            ctx.state = SocketState::HandshakePending;
            ctx.pending_open_mode = Some(TcpOwnershipMode::LegacyRelay);
            ctx.conn_epoch = ctx.conn_epoch.wrapping_add(1); // 新 remote-open 代次
            let epoch = ctx.conn_epoch;
            // fake-IP 在 spawn 时 acquire（与 inline「成功后才 acquire」不同——spawn 路径无早返回，
            // 由 rearm 在 open 失败/复位时 release，平衡）。首包入 buffer，open 成功后按序 flush。
            if let Some(ip) = fake_ip {
                self.fake_pool.acquire(ip, self.now_secs);
                ctx.fake_ip = Some(ip);
            }
            if !ctx.buffer_uplink(&payload) {
                // 理论不可达（spawn 入口 buffer 必空、首包 << 256KB）；防御性对称上面 HandshakePending 分支。
                trace_log!(
                    "⚠️ handle {:?} spawn 入口缓存首包失败（{}B），丢弃",
                    handle,
                    payload.len()
                );
            }
            trace_log!(
                "🎯 handle {:?} target {} → spawn remote open（并发化，不 stall 主循环）",
                handle,
                target.to_wire_string()
            );

            self.handshake_done_tx
                .spawn(Arc::clone(self.upstream), target, handle, epoch);
            Ok(())
        }
    }
}

pub(in crate::client_tun) struct HandshakeCompletion<'a, 'storage> {
    pub(in crate::client_tun) sockets: &'a mut SocketSet<'storage>,
    pub(in crate::client_tun) socket_ctxs: &'a mut HashMap<SocketHandle, SocketCtx>,
    pub(in crate::client_tun) global_tx: &'a mpsc::Sender<(SocketHandle, RelayEvent)>,
    pub(in crate::client_tun) owned_action_tx: &'a mpsc::Sender<OwnedActionEvent>,
    pub(in crate::client_tun) fake_pool: &'a mut FakeIpPool,
    pub(in crate::client_tun) now_secs: u64,
    pub(in crate::client_tun) metrics_handle: &'a Metrics,
    pub(in crate::client_tun) downlink_backpressure: DownlinkBackpressureConfig,
    pub(in crate::client_tun) relay_read_hard_pause: bool,
}

impl HandshakeCompletion<'_, '_> {
    pub(in crate::client_tun) async fn from_loop_turn(
        dispatch: &OpenDispatch,
        event: OpenEvent,
        mut completion: Self,
        dirty: &mut std::collections::HashSet<SocketHandle>,
    ) {
        let mut id = match event {
            OpenEvent::Flow(id) if dispatch.flow_draining() => id,
            event => {
                if let Some(handle) = Self::from_loop(dispatch, event, completion) {
                    dirty.insert(handle);
                }
                return;
            }
        };
        // Borrow the original task context without awaiting another event. Each
        // additional action spends only this turn's remaining control budget.
        std::future::poll_fn(|cx| {
            loop {
                if dispatch.service_flow(id, &mut completion) {
                    dirty.insert(id.handle);
                }
                let std::task::Poll::Ready(Some(OpenEvent::Flow(next))) = dispatch.poll_flows(cx)
                else {
                    break;
                };
                id = next;
            }
            std::task::Poll::Ready(())
        })
        .await;
    }

    pub(in crate::client_tun) fn from_loop(
        dispatch: &OpenDispatch,
        event: OpenEvent,
        mut completion: Self,
    ) -> Option<SocketHandle> {
        match event {
            OpenEvent::Yield => None,
            OpenEvent::Flow(id) => dispatch
                .service_flow(id, &mut completion)
                .then_some(id.handle),
            OpenEvent::Completion(done, custody) => {
                if dispatch.flow_mode() {
                    return None;
                }
                let handle = done.handle;
                let dirty = completion.apply(
                    RearmGate::original_loop(dispatch),
                    dispatch.relay_policy(),
                    done,
                );
                dispatch.consumed(custody);
                dirty.then_some(handle)
            }
        }
    }

    #[cfg(test)]
    pub(in crate::client_tun) fn legacy(self, done: HandshakeDone) -> bool {
        self.apply(
            RearmGate::legacy_facade(),
            &RelayFamilyPolicy::default(),
            done,
        )
    }

    fn apply(
        self,
        gate: RearmGate<'_>,
        relay_policy: &RelayFamilyPolicy,
        done: HandshakeDone,
    ) -> bool {
        let Self {
            sockets,
            socket_ctxs,
            global_tx,
            owned_action_tx,
            fake_pool,
            now_secs,
            metrics_handle,
            downlink_backpressure,
            relay_read_hard_pause,
        } = self;

        let HandshakeDone {
            handle,
            epoch,
            result,
        } = done;
        let Some(ctx) = socket_ctxs.get_mut(&handle) else {
            if let Ok(opened) = result {
                abandon_uninstalled_owned_tcp(opened, "socket_missing");
            }
            return false;
        };
        // 防串话（V2 patch 3）：epoch 比较置于状态检查**之前**。不匹配 = 本槽已 rearm/换代/复位 →
        // 迟到的 open 结果绝不装到新一代 socket；protocol-owned 结果还必须显式留下
        // LocalAbandon，不能把 endpoint drop 误当成 session flow 已终止。
        if ctx.conn_epoch != epoch {
            let completed = result.is_ok();
            if completed {
                trace_log!(
                    "🗑️ handle {:?} 迟到 open 结果(epoch {epoch}≠{}) 丢弃，不装到新代 socket",
                    handle,
                    ctx.conn_epoch
                );
            }
            if let Ok(opened) = result {
                abandon_uninstalled_owned_tcp(opened, "stale_socket_epoch");
            }
            return false;
        }
        // 二次防御：epoch 匹配但状态已非 HandshakePending（理论不该发生）→ 不装。
        if ctx.state != SocketState::HandshakePending {
            if let Ok(opened) = result {
                abandon_uninstalled_owned_tcp(opened, "socket_state_changed");
            }
            return false;
        }
        // Cross the no-F gateway before completion mutates open custody.
        if !gate_legacy_rearm(
            gate,
            RearmIntent::reasoned_ctx(handle, ctx, "remote_open", "handshake_completion", None),
        ) {
            return false;
        }
        let expected_mode = ctx.pending_open_mode.take();
        match (expected_mode, result) {
            (Some(TcpOwnershipMode::LegacyRelay), Ok(OpenedOwnedTcp::Legacy(stream))) => {
                let (tx, rx) = tokio::sync::mpsc::channel(RELAY_CHANNEL_CAPACITY);
                // 按序 flush open 期间缓存的上行字节（首包 + 在飞期到达的后续包，FIFO）。新 channel 容量充足、
                // rx 未 drop、仅 1 条消息 → try_send 必成（避免 await-with-borrow）。防御性：万一失败（未来
                // 容量被改小等），**不静默丢缓存**——log + rearm 撤掉本槽，让应用 TCP 重建（Finding 2）。
                if !ctx.uplink_buffer.is_empty() {
                    let buffered = std::mem::take(&mut ctx.uplink_buffer);
                    if let Err(e) = tx.try_send(RelayCommand::Data(buffered)) {
                        trace_log!(
                            "❌ handle {:?} flush open 缓存失败({e}) → rearm（不静默丢字节）",
                            handle
                        );
                        let socket = sockets.get_mut::<TcpSocket>(handle);
                        legacy_reasoned_rearm_with_gate(
                            gate,
                            handle,
                            socket,
                            ctx,
                            fake_pool,
                            now_secs,
                            "local_to_remote",
                            "handshake_buffer_flush_failed",
                        );
                        return false; // stream 随作用域 drop 干净关闭
                    }
                }
                ctx.uplink_tx = Some(tx);
                ctx.state = SocketState::Relaying;
                trace_log!(
                    "🚪 handle {:?} remote session opened（spawn open 成功）",
                    handle
                );
                let spawned = spawn_remote_relay(
                    relay_policy,
                    handle,
                    epoch,
                    stream,
                    rx,
                    global_tx.clone(),
                    metrics_handle,
                    downlink_backpressure,
                    initial_relay_read_credit(relay_read_hard_pause),
                );
                install_spawned_remote_relay(ctx, spawned, relay_read_hard_pause);
                // Preserve the legacy scheduler exactly: its spawned relay/global
                // event is the wakeup authority, as before Knife16.
                false
            }
            (Some(TcpOwnershipMode::Resumable), Ok(OpenedOwnedTcp::Resumable(flow))) => {
                if !ctx.uplink_buffer.is_empty() {
                    // Resumable open must never have consumed/buffered local bytes
                    // before its replay port was installed.
                    let socket = sockets.get_mut::<TcpSocket>(handle);
                    legacy_reasoned_rearm_with_gate(
                        gate,
                        handle,
                        socket,
                        ctx,
                        fake_pool,
                        now_secs,
                        "local_to_owner",
                        "resumable_open_buffer_invariant",
                    );
                    abandon_uninstalled_owned_tcp(
                        OpenedOwnedTcp::Resumable(flow),
                        "buffered_before_resumable_install",
                    );
                    return false;
                }
                install_resumable_tcp_flow(
                    handle,
                    epoch,
                    ctx,
                    flow,
                    owned_action_tx,
                    relay_read_hard_pause,
                );
                trace_log!(
                    "🚪 handle {:?} protocol-owned TCP flow opened (epoch={epoch})",
                    handle
                );
                true
            }
            (Some(expected), Ok(opened)) => {
                let actual = match &opened {
                    OpenedOwnedTcp::Legacy(_) => TcpOwnershipMode::LegacyRelay,
                    OpenedOwnedTcp::Resumable(_) => TcpOwnershipMode::Resumable,
                };
                println!(
                    "❌ handle {:?} upstream ownership mismatch: expected={expected:?} actual={actual:?} → rearm",
                    handle
                );
                let socket = sockets.get_mut::<TcpSocket>(handle);
                legacy_reasoned_rearm_with_gate(
                    gate,
                    handle,
                    socket,
                    ctx,
                    fake_pool,
                    now_secs,
                    "remote_open",
                    "ownership_mode_mismatch",
                );
                abandon_uninstalled_owned_tcp(opened, "ownership_mode_mismatch");
                false
            }
            (None, Ok(opened)) => {
                let socket = sockets.get_mut::<TcpSocket>(handle);
                legacy_reasoned_rearm_with_gate(
                    gate,
                    handle,
                    socket,
                    ctx,
                    fake_pool,
                    now_secs,
                    "remote_open",
                    "missing_open_ownership_mode",
                );
                abandon_uninstalled_owned_tcp(opened, "missing_open_ownership_mode");
                false
            }
            (_, Err(e)) => {
                println!(
                    "❌ handle {:?} spawned remote open failed: {e} → rearm",
                    handle
                );
                let socket = sockets.get_mut::<TcpSocket>(handle);
                legacy_reasoned_rearm_with_gate(
                    gate,
                    handle,
                    socket,
                    ctx,
                    fake_pool,
                    now_secs,
                    "remote_open",
                    "handshake_failed",
                ); // 释放 spawn 时 acquire 的 fake-IP（平衡）
                false
            }
        }
    }
}
