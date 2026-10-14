//! Original DATA-loop owner and its complete scheduling/physical transaction.
//!
//! Owns sockets, contexts, fake-IP, receiver lanes and timers; borrows the device.
//! Control custody stays in SessionControl. Borrowed execution may unwind while
//! this owner retains physical state and drives fatal settlement. The sole entry is
//! run_borrowed_event_loop; real saved-loop tests live in client_tun::tests and
//! compatibility_runtime::tests. Preserve all poll/flush and settlement ordering.

use crate::client_tun::compatibility_runtime::{FatalPhysicalTurn, FatalStep};
use crate::client_tun::*;
use crate::shared::errors::FailurePhase;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::task::Poll;

/// Original DATA engine; its persistent caller retains the borrowed device on unwind.
pub(in crate::client_tun) async fn run_borrowed_event_loop<D, U, M>(
    device: &mut D,
    upstream: Arc<U>,
    mut udp_downlink: UdpDownlinkSource,
    runtime_config: TunRuntimeConfig,
    metrics_handle: Arc<Metrics>,
    mut metrics: M,
    open_owner: Option<Arc<SessionControl>>,
) -> std::io::Result<()>
where
    D: TunIo,
    U: OwnedUpstream + 'static,
    M: MetricsSink,
{
    let panic_owner = open_owner.clone();
    let pool_size = runtime_config.listener.pool_size;
    let listener_capacity_profile = runtime_config.listener_capacity_profile;
    let socket_buffers = runtime_config.tcp_socket_buffers;
    let max_total = match listener_capacity_profile.startup_max_total(socket_buffers) {
        Ok(max_total) => max_total,
        Err(rejection) => {
            #[cfg(test)]
            if let Some(owner) = open_owner.as_ref() {
                owner.record_startup_profile_observation(StartupProfileObservation {
                    profile: listener_capacity_profile,
                    effective_rx_bytes: socket_buffers.rx_bytes,
                    effective_tx_bytes: socket_buffers.tx_bytes,
                    disposition: StartupProfileDisposition::Rejected(rejection),
                    listener_owned: ListenerOwnedCounts {
                        sockets: 0,
                        socket_ctxs: 0,
                        ports: 0,
                        total: 0,
                    },
                });
            }
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("listener capacity profile rejected: {rejection:?}"),
            ));
        }
    };

    // 全局回信通道（TCP relay 通用回程）：发送端 global_tx 克隆给每个后台车厢。
    let (global_tx, global_rx) =
        tokio::sync::mpsc::channel::<(SocketHandle, RelayEvent)>(RELAY_CHANNEL_CAPACITY);
    let mut relay_event_lane = RelayEventLane::new(global_rx);
    let (owned_action_tx, mut owned_action_rx) =
        tokio::sync::mpsc::channel::<OwnedActionEvent>(OWNED_ACTION_CHANNEL_CAPACITY);

    // 刀9 M3 / 刀14d：spawn 出主循环的 remote-open 完成后经此回灌主循环安装 relay。
    // 明确 cheap 的测试/mock 路径仍可 inline，不走此通道。
    let (handshake_done_tx, mut handshake_done_rx) =
        OpenDispatch::channel(HANDSHAKE_DONE_CAPACITY, open_owner);
    let handshake_done_tx =
        handshake_done_tx.with_relay_policy(Arc::clone(&runtime_config.relay_policy));
    #[cfg(all(test, feature = "harness"))]
    handshake_done_tx.install_relay_event_test_bridge(&global_tx);

    // =========== 初始化 smoltcp 酒店和路由器 ===========
    let mut sockets = SocketSet::new(vec![]);

    // Stage 9: 监听端口不再固定，由 SYN inspector 在 rx 热路径按需注册。
    // 中文要点：启动时 registry 是空的；第一条到任意端口的 SYN 会触发该端口建池。
    let mut registry =
        ListenerRegistry::with_socket_buffers_and_max_total(pool_size, socket_buffers, max_total);
    let mut socket_ctxs: HashMap<SocketHandle, SocketCtx> = HashMap::new();

    // #1 脏集合驱动：只有「本 tick 有活动」的 listener handle 进集合，relay 段仅处理它们，
    // 替代每 tick O(总槽) 全量 sweep。入集 = inbound TCP 包标脏其端口 pool / 回程残留下行 pending；
    // 出集 = 处理后无 pending、不再 can_recv、且没有待发送本地 FIN（见 process_dirty_relay）。
    let mut dirty: HashSet<SocketHandle> = HashSet::new();

    // 刀5: fake-IP DNS 不再用 smoltcp socket。任意 resolver 的明文 :53 在 rx 热路径被
    // classify_inbound 判 Dns → handle_dns_hijack 裸包伪造 fake-IP 回包（绕过 smoltcp，
    // 源 = 被查询的 resolver，故不依赖系统 DNS 指向 198.18.0.1，见 ADR-0007）。
    let mut fake_pool = FakeIpPool::new();

    // 3. 初始化 smoltcp 的“虚拟路由器”
    let config = SmolConfig::new(smoltcp::wire::HardwareAddress::Ip);
    // 这里传入了包装好的 device
    let mut iface = Interface::new(config, device, smoltcp::time::Instant::now());

    // 给虚拟路由器配置 IP 地址 (10.0.0.1/24)
    iface.update_ip_addrs(|ip_addrs| {
        ip_addrs
            .push(IpCidr::new(IpAddress::v4(10, 0, 0, 2), 24))
            .unwrap();
        // 刀5：不再把 198.18.0.1/32 配成接口 IP——DNS 回包改由 handle_dns_hijack 裸包注入
        // （src = 被查询的 resolver），不经 smoltcp 选源，故无需本接口持有 resolver 地址。
    });

    // AnyIP：接收目的 IP 不是本接口自身地址的包（即被拦截连接真正想去的 Target）。
    // 中文要点：默认路由的网关填本接口自己的 IP 10.0.0.2 是 smoltcp AnyIP 接收判定的
    // 硬性要求（routes.lookup(dst) 必须命中一个本接口 IP 才放行），不是笔误。
    iface.set_any_ip(true);
    iface
        .routes_mut()
        .add_default_ipv4_route(smoltcp::wire::Ipv4Address::new(10, 0, 0, 2))
        .unwrap();

    // 初始化定时器 (每 5 毫秒触发一次)
    let mut timer = tokio::time::interval(std::time::Duration::from_millis(5));

    // UDP flow ownership remains in the event loop. Wire decoding/reassembly
    // belongs to the selected downlink source, so this layer never interprets
    // TUIC records or a future resumable UDP envelope.
    let mut assoc_table = AssocTable::new();
    let udp_clock = std::time::Instant::now();
    let mut udp_sweep = tokio::time::interval(std::time::Duration::from_secs(1));
    // review #7：fake-IP 池回收单独走低频 tick（TTL=300s，无需每秒全表扫）。
    let mut fake_ip_sweep = tokio::time::interval(std::time::Duration::from_secs(60));
    // 刀11：数据面可观测性快照 tick（30s）。在此**单写者** task 重算 loop-owned gauge
    // （active_relays / fake-IP 用量 / failover 当班腿）→ 发布进 Arc<Metrics> → 打统一 📊 快照行。
    // 与 TuicUpstream::start_udp 的 UDP-path 📊 行（RTT/cwnd/背压）是两条独立日志、各司其职（见 ADR-0012 §5）。
    let mut metrics_tick =
        tokio::time::interval(std::time::Duration::from_secs(runtime_config.metrics_secs));
    let mut tun_egress_feedback_tick = tokio::time::interval(std::time::Duration::from_secs(
        TUN_EGRESS_FEEDBACK_SAMPLE_SECS,
    ));
    let mut tcp_loop_flush_tx_calls: u64 = 0;
    let mut tcp_loop_flush_tx_failures: u64 = 0;
    let downlink_backpressure = runtime_config.downlink_backpressure;
    let downlink_flush_max_bytes = runtime_config.downlink_flush_max_bytes;
    let tun_rx_drain_budget = runtime_config.tun_rx_drain_budget;
    let tun_rx_active_flow_timer_ms = runtime_config.tun_rx_active_flow_timer_ms;
    let tun_rx_active_flow_timer_duration = (tun_rx_active_flow_timer_ms > 0)
        .then(|| std::time::Duration::from_millis(tun_rx_active_flow_timer_ms));
    let bounded_global_rx_receive_window = runtime_config.bounded_global_rx_receive_window;
    let buffered_downlink = runtime_config.buffered_downlink;
    let mut downlink_egress_pacer =
        DownlinkEgressPacer::new(runtime_config.downlink_egress_immediate_bytes);
    let mut downlink_egress_drop_debt = DownlinkEgressDropDebt::default();
    let mut d16_global_drop_episode = D16GlobalDropEpisode::default();
    let mut active_flow_tun_rx_drain_until: Option<std::time::Instant> = None;
    let mut tun_egress_drop_sampler = TunEgressDropSampler::new(device.interface_name());
    let mut tun_egress_feedback = TunEgressFeedbackState::default();
    let mut tun_rx_drain_diag = TunRxDrainDiag::default();
    let local_egress_service_config = LocalEgressServiceConfig::default();
    let mut local_egress_service_diag = LocalEgressServiceDiag::default();
    let deferred_ack_drain_sleep = tokio::time::sleep(std::time::Duration::from_secs(60 * 60));
    tokio::pin!(deferred_ack_drain_sleep);
    let mut deferred_ack_drain = DeferredAckDrainState::default();
    let relay_gap_ack_drain_sleep = tokio::time::sleep(std::time::Duration::from_secs(60 * 60));
    tokio::pin!(relay_gap_ack_drain_sleep);
    let d3_egress_actor_self_wake_sleep =
        tokio::time::sleep(std::time::Duration::from_secs(60 * 60));
    tokio::pin!(d3_egress_actor_self_wake_sleep);
    let mut d3_egress_actor_self_wake_armed = false;
    let mut downlink_rx_paused = false;

    let mut selected_relay = None;
    let mut selected_action = None;
    let flow_mode = handshake_done_tx.flow_mode();
    let mut shutdown_physical_cursor = 0;
    let mut execution = Box::pin(async {
        loop {
            handshake_done_tx.begin_turn().await;
            handshake_done_tx.drain_shutdown_physical(
                &mut sockets,
                &mut socket_ctxs,
                &mut fake_pool,
                &mut shutdown_physical_cursor,
                udp_clock.elapsed().as_secs(),
            );
            if handshake_done_tx.flow_shutdown_complete(&sockets, &mut socket_ctxs, &fake_pool) {
                return Ok(());
            }
            let downlink_stats_raw = downlink_pressure_stats(&dirty, &socket_ctxs, &sockets);
            let pressure_now = std::time::Instant::now();
            let downlink_stats = tun_egress_feedback
                .effective_downlink_pressure_at(downlink_stats_raw, pressure_now);
            tun_egress_feedback.observe_pressure_at(
                downlink_stats_raw,
                downlink_backpressure,
                pressure_now,
            );
            let capacity_backpressure = d5_capacity_backpressure_enabled(&runtime_config);
            let next_downlink_rx_paused_without_debt = next_downlink_backpressure_for_runtime(
                capacity_backpressure,
                downlink_rx_paused,
                downlink_stats,
                downlink_backpressure,
                false,
            );
            let previous_downlink_rx_paused = downlink_rx_paused;
            if should_install_downlink_pressure_credit_debt_for_runtime(
                runtime_config.d3_egress_actor,
                runtime_config.d3_egress_actor_legacy_credit,
                buffered_downlink.enabled,
                previous_downlink_rx_paused,
                next_downlink_rx_paused_without_debt,
                downlink_stats_raw,
                downlink_backpressure,
            ) {
                let installed_bytes = downlink_egress_drop_debt
                    .note_egress_pressure(downlink_stats_raw, downlink_backpressure);
                if installed_bytes > 0 {
                    tcp_diag_log!(
                        "{}",
                        format_downlink_egress_credit_debt_diag(
                            if next_downlink_rx_paused_without_debt {
                                "pressure_pause_edge"
                            } else {
                                "pressure_credit_edge"
                            },
                            installed_bytes,
                            downlink_stats_raw,
                            downlink_backpressure,
                            downlink_egress_drop_debt,
                        )
                    );
                }
            }
            let next_downlink_rx_paused = next_downlink_backpressure_for_runtime(
                capacity_backpressure,
                downlink_rx_paused,
                downlink_stats,
                downlink_backpressure,
                downlink_egress_drop_debt.has_active_drop_debt(),
            );
            if next_downlink_rx_paused != previous_downlink_rx_paused {
                downlink_rx_paused = next_downlink_rx_paused;
                tcp_diag_log!(
                    "{}",
                    format_tcp_downlink_backpressure_diag(
                        downlink_rx_paused,
                        downlink_stats_raw,
                        downlink_stats,
                        downlink_backpressure,
                    )
                );
            }
            let global_rx_paused = if flow_mode {
                // Each F reader has an absolute per-flow window. Drain its DATA
                // independently of a sibling's local TCP receive window.
                false
            } else if buffered_downlink.enabled {
                let previous_global_rx_receive_paused = relay_event_lane.receive_paused();
                let next_global_rx_receive_paused = next_buffered_global_rx_receive_backpressure(
                    previous_global_rx_receive_paused,
                    downlink_stats_raw,
                    buffered_downlink,
                );
                if next_global_rx_receive_paused != previous_global_rx_receive_paused {
                    relay_event_lane.set_receive_paused(next_global_rx_receive_paused);
                    tcp_diag_log!(
                        "{}",
                        format_tcp_global_rx_backpressure_diag(
                            relay_event_lane.receive_paused(),
                            downlink_stats_raw,
                            downlink_backpressure,
                            false,
                            tun_egress_feedback.is_paused(),
                        )
                    );
                }
                relay_event_lane.receive_paused()
            } else if bounded_global_rx_receive_window {
                let previous_global_rx_receive_paused = relay_event_lane.receive_paused();
                let next_global_rx_receive_paused = next_global_rx_receive_backpressure(
                    previous_global_rx_receive_paused,
                    downlink_stats_raw,
                    downlink_backpressure,
                );
                if next_global_rx_receive_paused != previous_global_rx_receive_paused {
                    relay_event_lane.set_receive_paused(next_global_rx_receive_paused);
                    tcp_diag_log!(
                        "{}",
                        format_tcp_global_rx_backpressure_diag(
                            relay_event_lane.receive_paused(),
                            downlink_stats_raw,
                            downlink_backpressure,
                            downlink_rx_paused,
                            tun_egress_feedback.is_paused(),
                        )
                    );
                }
                relay_event_lane.receive_paused()
            } else {
                downlink_rx_paused || tun_egress_feedback.is_paused()
            };
            let allow_relay_receive = !global_rx_paused || handshake_done_tx.flow_draining();
            #[cfg(test)]
            let allow_relay_receive = allow_relay_receive && !handshake_done_tx.test_lane_held();
            publish_relay_read_credit_for_dirty_handles(
                &dirty,
                &sockets,
                &mut socket_ctxs,
                downlink_backpressure,
                runtime_config.tun_mtu,
                downlink_flush_max_bytes,
                downlink_egress_drop_debt.has_active_drop_debt(),
                tun_rx_drain_diag.backlog_guard().is_active()
                    || if buffered_downlink.enabled {
                        tun_egress_feedback.is_paused()
                    } else {
                        downlink_rx_paused || tun_egress_feedback.is_paused()
                    },
                buffered_downlink,
                downlink_stats_raw.total_pending,
            );
            macro_rules! relay_event_turn {
                () => {
                    RelayEventTurn {
                        socket: RelayEventSocketState {
                            sockets: &mut sockets,
                            socket_ctxs: &mut socket_ctxs,
                            dirty: &mut dirty,
                        },
                        ingress: RelayEventIngressState {
                            device: &mut *device,
                            assoc_table: &mut assoc_table,
                            fake_pool: &mut fake_pool,
                            upstream: &upstream,
                            udp_clock,
                            metrics_handle: metrics_handle.as_ref(),
                            registry: &mut registry,
                            iface: &mut iface,
                            handshake_done_tx: &handshake_done_tx,
                            global_tx: &global_tx,
                            owned_action_tx: &owned_action_tx,
                            metrics: &mut metrics,
                            runtime_config: &runtime_config,
                            downlink_backpressure,
                            downlink_flush_max_bytes,
                            tun_rx_drain_budget,
                            tun_rx_active_flow_timer_duration,
                            capacity_backpressure,
                            buffered_downlink,
                            downlink_rx_paused,
                            tun_egress_feedback: &tun_egress_feedback,
                            downlink_egress_pacer: &mut downlink_egress_pacer,
                            downlink_egress_drop_debt: &mut downlink_egress_drop_debt,
                            tcp_loop_flush_tx_calls: &mut tcp_loop_flush_tx_calls,
                            tcp_loop_flush_tx_failures: &mut tcp_loop_flush_tx_failures,
                            tun_rx_drain_diag: &mut tun_rx_drain_diag,
                            local_egress_service_config,
                            local_egress_service_diag: &mut local_egress_service_diag,
                            active_flow_tun_rx_drain_until: &mut active_flow_tun_rx_drain_until,
                            deferred_ack_drain: &mut deferred_ack_drain,
                            deferred_ack_drain_sleep: deferred_ack_drain_sleep.as_mut(),
                            d3_egress_actor_self_wake_armed: &mut d3_egress_actor_self_wake_armed,
                            d3_egress_actor_self_wake_sleep: d3_egress_actor_self_wake_sleep
                                .as_mut(),
                            relay_gap_ack_drain_sleep: relay_gap_ack_drain_sleep.as_mut(),
                        },
                    }
                };
            }

            let relay_gap_ack_drain_armed = relay_event_lane.relay_gap_ack_drain_is_armed();
            tokio::select! {
                // Protocol-owned application acceptance/FIN/terminal actions use
                // an independent bounded lane. They must remain serviceable when
                // legacy bulk DATA reception is paused by local egress pressure.
                Some(()) = async {
                    selected_action = owned_action_rx.recv().await;
                    selected_action.as_ref().map(|_| ())
                } => {
                    metrics.loop_park_end();
                    if flow_mode {
                        // The admitted Generic path has no owned-protocol action.
                        // Keep its capability and fail closed instead of dropping it.
                        return Err(std::io::Error::other("unexpected F owned action"));
                    }
                    let Some(OwnedActionEvent { handle, epoch, action }) = selected_action.take() else {
                        continue;
                    };
                    let active = match action {
                        OwnedAction::Tun(action) => handle_owned_tun_action_from_loop(
                            &handshake_done_tx,
                            handle,
                            epoch,
                            action,
                            &mut sockets,
                            &mut socket_ctxs,
                            &mut fake_pool,
                            udp_clock.elapsed().as_secs(),
                        ),
                        OwnedAction::PortClosed { flow } => handle_owned_port_closed_from_loop(
                            &handshake_done_tx,
                            handle,
                            epoch,
                            flow,
                            &mut sockets,
                            &mut socket_ctxs,
                            &mut fake_pool,
                            udp_clock.elapsed().as_secs(),
                        ),
                    };
                    if active {
                        dirty.insert(handle);
                    } else if socket_ctxs
                        .get(&handle)
                        .is_none_or(|ctx| ctx.state == SocketState::Listening)
                    {
                        dirty.remove(&handle);
                    }
                }
                // TCP relay 回程：后台车厢把远端回传字节送回主循环 → 注入对应 smoltcp socket。
                // TUIC 自重连（live_conn），不需要 legacy 的 disconnect/复位分支。
                Some(()) = async {
                    selected_relay = relay_event_lane.recv().await;
                    selected_relay.as_ref().map(|_| ())
                }, if allow_relay_receive => {
                    #[cfg(all(test, feature = "harness"))]
                    handshake_done_tx.observe_selected_relay(&selected_relay);
                    metrics.loop_park_end();
                    if let Some((handle, event)) = selected_relay.as_mut() {
                        relay_event_lane
                            .handle_relay_event(*handle, event, relay_event_turn!())
                            .await?;
                    }
                    selected_relay = None;
                    #[cfg(all(test, feature = "harness"))]
                    handshake_done_tx.observe_selected_relay(&selected_relay);
                }
                _ = &mut relay_gap_ack_drain_sleep,
                    if relay_gap_ack_drain_armed =>
                {
                    metrics.loop_park_end();
                    relay_event_lane
                        .handle_relay_gap_followup(relay_event_turn!())
                        .await?;
                }
                _ = &mut deferred_ack_drain_sleep, if deferred_ack_drain.is_armed() => {
                    metrics.loop_park_end();
                    if let Some(mode) = deferred_ack_drain.take_due() {
                        let tun_rx_budget = tun_rx_drain_budget_for_deferred_ack_drain(
                            mode,
                            downlink_pressure_stats(&dirty, &socket_ctxs, &sockets),
                            tun_egress_feedback.is_paused(),
                            tun_rx_drain_budget,
                            downlink_backpressure,
                            runtime_config.tun_mtu,
                            capacity_backpressure,
                        );
                        if tun_rx_budget > 0 {
                            let drained = drain_ready_tun_rx(
                                device,
                                &mut assoc_table,
                                &mut fake_pool,
                                &upstream,
                                udp_clock.elapsed().as_secs(),
                                &metrics_handle,
                                &mut registry,
                                &mut sockets,
                                &mut socket_ctxs,
                                &mut iface,
                                &mut dirty,
                                &handshake_done_tx,
                                &global_tx,
                                &owned_action_tx,
                                &mut metrics,
                                downlink_flush_max_bytes,
                                downlink_backpressure,
                                runtime_config.tun_mtu,
                                &mut downlink_egress_drop_debt,
                                &mut tcp_loop_flush_tx_calls,
                                &mut tcp_loop_flush_tx_failures,
                                &mut tun_rx_drain_diag,
                                tun_rx_budget,
                                mode.source(),
                                if buffered_downlink.enabled {
                                    tun_egress_feedback.is_paused()
                                } else {
                                    downlink_rx_paused || tun_egress_feedback.is_paused()
                                },
                                buffered_downlink,
                            )
                            .await?;
                            if drained >= tun_rx_budget
                                && deferred_ack_drain.arm_after_budget_exhausted()
                            {
                                deferred_ack_drain_sleep.as_mut().reset(
                                    tokio::time::Instant::now()
                                        + std::time::Duration::from_millis(DEFERRED_ACK_DRAIN_DELAY_MS),
                                );
                            } else if matches!(mode, DeferredAckDrainMode::ActiveFlow)
                                && deferred_ack_drain.arm_after_delayed_ack_probe()
                            {
                                deferred_ack_drain_sleep.as_mut().reset(
                                    tokio::time::Instant::now()
                                        + std::time::Duration::from_millis(
                                            DEFERRED_ACK_DRAIN_DELAYED_PROBE_MS,
                                        ),
                                );
                            }
                        }
                    }
                }
                _ = &mut d3_egress_actor_self_wake_sleep, if d3_egress_actor_self_wake_armed => {
                    metrics.loop_park_end();
                    d3_egress_actor_self_wake_armed = false;
                    if !dirty.is_empty() {
                        let relay_read_hard_pause = tun_rx_drain_diag
                            .backlog_guard()
                            .is_active()
                            || if buffered_downlink.enabled {
                            tun_egress_feedback.is_paused()
                        } else {
                            downlink_rx_paused || tun_egress_feedback.is_paused()
                        };
                        let egress_actor =
                            local_egress_actor_enabled(&runtime_config, &dirty, &socket_ctxs);
                        let local_progress = service_local_egress_until(
                            device,
                            &mut assoc_table,
                            &mut fake_pool,
                            &upstream,
                            udp_clock.elapsed().as_secs(),
                            &metrics_handle,
                            &mut registry,
                            &mut sockets,
                            &mut socket_ctxs,
                            &mut iface,
                            &mut dirty,
                            &handshake_done_tx,
                            &global_tx,
                            &owned_action_tx,
                            &mut metrics,
                            downlink_flush_max_bytes,
                            downlink_backpressure,
                            runtime_config.tun_mtu,
                            &mut downlink_egress_drop_debt,
                            &mut tcp_loop_flush_tx_calls,
                            &mut tcp_loop_flush_tx_failures,
                            &mut tun_rx_drain_diag,
                            &mut local_egress_service_diag,
                            relay_read_hard_pause,
                            tun_egress_feedback.is_paused(),
                            buffered_downlink,
                            local_egress_service_config,
                            egress_actor,
                        )
                        .await?;
                        if should_rearm_d3_egress_actor_self_wake(
                            d3_egress_actor_self_wake_enabled(&runtime_config),
                            !dirty.is_empty(),
                            local_progress,
                        ) {
                            local_egress_service_diag.note_egress_actor_self_wake();
                            d3_egress_actor_self_wake_armed = true;
                            d3_egress_actor_self_wake_sleep
                                .as_mut()
                                .reset(tokio::time::Instant::now());
                        }
                    }
                }
                // 分支 1: 全局回信通道接收到了新数据包
                // 分支 1: 物理网卡接收到了新数据包
                res = device.wait_for_rx(), if !handshake_done_tx.flow_draining() =>{
                    metrics.loop_park_end();
                    if res.is_ok(){
                        if handshake_done_tx.flow_draining() { continue; }
                        let relay_read_hard_pause = tun_rx_drain_diag
                            .backlog_guard()
                            .is_active()
                            || if buffered_downlink.enabled {
                                tun_egress_feedback.is_paused()
                            } else {
                                downlink_rx_paused || tun_egress_feedback.is_paused()
                            };
                        let tcp_packet_ready = if runtime_config.relay_policy.d16_byte_owned() {
                            let tcp_packets_before = tun_rx_drain_diag.tcp_packets;
                            let _ = drain_ready_tun_rx(
                                device,
                                &mut assoc_table,
                                &mut fake_pool,
                                &upstream,
                                udp_clock.elapsed().as_secs(),
                                &metrics_handle,
                                &mut registry,
                                &mut sockets,
                                &mut socket_ctxs,
                                &mut iface,
                                &mut dirty,
                                &handshake_done_tx,
                                &global_tx,
                                &owned_action_tx,
                                &mut metrics,
                                downlink_flush_max_bytes,
                                downlink_backpressure,
                                runtime_config.tun_mtu,
                                &mut downlink_egress_drop_debt,
                                &mut tcp_loop_flush_tx_calls,
                                &mut tcp_loop_flush_tx_failures,
                                &mut tun_rx_drain_diag,
                                local_egress_service_config.tun_rx_packets_per_cycle,
                                "inbound_ready_batch",
                                relay_read_hard_pause,
                                buffered_downlink,
                            )
                            .await?;
                            tun_rx_drain_diag.tcp_packets > tcp_packets_before
                        } else {
                            process_ready_tun_rx_packet(
                                device,
                                &mut assoc_table,
                                &mut fake_pool,
                                &upstream,
                                udp_clock.elapsed().as_secs(),
                                &metrics_handle,
                                &mut registry,
                                &mut sockets,
                                &mut socket_ctxs,
                                &mut iface,
                                &mut dirty,
                                &handshake_done_tx,
                                &global_tx,
                                &owned_action_tx,
                                &mut metrics,
                                downlink_flush_max_bytes,
                                downlink_backpressure,
                                runtime_config.tun_mtu,
                                &mut downlink_egress_drop_debt,
                                &mut tcp_loop_flush_tx_calls,
                                &mut tcp_loop_flush_tx_failures,
                                relay_read_hard_pause,
                                buffered_downlink,
                                "inbound_poll",
                            )
                            .await
                                == TunRxPacketKind::Tcp
                        };
                        if tcp_packet_ready
                            && dirty_contains_d16_flow(&dirty, &socket_ctxs)
                        {
                            metrics.note_d16_tun_rx_actor_wake();
                            local_egress_service_diag.note_egress_actor_immediate_wake();
                            let relay_read_hard_pause = tun_rx_drain_diag
                                .backlog_guard()
                                .is_active()
                                || if buffered_downlink.enabled {
                                    tun_egress_feedback.is_paused()
                                } else {
                                    downlink_rx_paused || tun_egress_feedback.is_paused()
                                };
                            let egress_actor = local_egress_actor_enabled(
                                &runtime_config,
                                &dirty,
                                &socket_ctxs,
                            );
                            let _ = service_local_egress_until(
                                device,
                                &mut assoc_table,
                                &mut fake_pool,
                                &upstream,
                                udp_clock.elapsed().as_secs(),
                                &metrics_handle,
                                &mut registry,
                                &mut sockets,
                                &mut socket_ctxs,
                                &mut iface,
                                &mut dirty,
                                &handshake_done_tx,
                                &global_tx,
                                &owned_action_tx,
                                &mut metrics,
                                downlink_flush_max_bytes,
                                downlink_backpressure,
                                runtime_config.tun_mtu,
                                &mut downlink_egress_drop_debt,
                                &mut tcp_loop_flush_tx_calls,
                                &mut tcp_loop_flush_tx_failures,
                                &mut tun_rx_drain_diag,
                                &mut local_egress_service_diag,
                                relay_read_hard_pause,
                                tun_egress_feedback.is_paused(),
                                buffered_downlink,
                                local_egress_service_config,
                                egress_actor,
                            )
                            .await?;
                        }
                    } else if let Err(error) = res {
                        return Err(error);
                    }
                }
                // The original loop acknowledges legacy completion or the exact root-owned F step.
                Some(event) = handshake_done_tx.recv(&mut handshake_done_rx) => {
                    metrics.loop_park_end();
                    let relay_read_hard_pause = tun_rx_drain_diag.backlog_guard().is_active()
                        || tun_egress_feedback.is_paused()
                        || (!buffered_downlink.enabled && downlink_rx_paused);
                    HandshakeCompletion::from_loop_turn(
                        &handshake_done_tx,
                        event,
                        HandshakeCompletion {
                            sockets: &mut sockets,
                            socket_ctxs: &mut socket_ctxs,
                            global_tx: &global_tx,
                            owned_action_tx: &owned_action_tx,
                            fake_pool: &mut fake_pool,
                            now_secs: udp_clock.elapsed().as_secs(),
                            metrics_handle: &metrics_handle,
                            downlink_backpressure,
                            relay_read_hard_pause,
                        },
                        &mut dirty,
                    ).await;
                }
                // 刀16 Task 3：上游中性 UDP 下行事件 → AssocTable 解路由 →
                // 造回程 IP/UDP 注入 TUN。Legacy source 在 facade 内保留 TUIC 解码/分片重组；
                // 每个 raw item 只产生一次 select 调度，不在 source 内循环改变公平性。
                Some(event) = udp_downlink.recv(&udp_clock) => {
                    metrics.loop_park_end();
                    if let UdpDownlinkEvent::Deliver(downlink) = event {
                        let (association, payload) = downlink.into_parts();
                        let assoc_id = association.get();
                        // 先取出路由信息(Copy),释放 assoc_table 借用后再 touch。
                        let routed = assoc_table
                            .resolve(assoc_id)
                            .map(|e| (e.target_src(), e.app_endpoint()));
                        let packet = routed.and_then(|(src, dst)| {
                            let packet = build_udp_ip_packet(src, dst, &payload);
                            if packet.is_none() {
                                metrics_handle.inc_udp_drops_down();
                            }
                            packet
                        });
                        if let Some(pkt) = packet {
                            device.inject_ip_packet(&pkt);
                            assoc_table.touch(assoc_id, udp_clock.elapsed().as_secs());
                            let (flush_result, _) = flush_tx_and_release_downlink_permits(
                                device,
                                &sockets,
                                &mut socket_ctxs,
                                "udp_downlink",
                            )
                            .await;
                            if let Err(e) = flush_result {
                                trace_log!("UDP 下行 flush 失败: {e}");
                            }
                        } else {
                            // 未知 association 或无法编码的 payload 不进入 TUN。
                            trace_log!("🗑️ UDP↓ assoc={assoc_id} 无映射或报文无效，丢弃 {}B", payload.len());
                        }
                    }
                }
                // Stage 13b: 周期回收空闲 UDP assoc。刀2：同时回收 fake-IP 引用 + sweep fake-IP 池。
                _ = udp_sweep.tick() => {
                    metrics.loop_park_end();
                    let now = udp_clock.elapsed().as_secs();
                    // 被回收的 UDP assoc → release 其占用的 fake-IP（引用计数归零，进可回收候选）。
                    for ip in assoc_table.sweep(now, UDP_FLOW_IDLE_SECS) {
                        fake_pool.release(ip, now);
                    }
                    // 刀16 Task 3：Legacy source 保留原 TUIC 未集齐分片的 TTL 回收；
                    // owned UDP source 在这个中性边界不持有 TUIC fragment state。
                    udp_downlink.sweep(now, crate::tuic::FRAG_REASSEMBLY_TTL_SECS);
                    // review #1/#2：回收已死/卡住的 TCP listener 槽（本地关闭/开远端失败的 teardown 缺口），
                    // 释放其 fake-IP refcount 并让槽回 Listen 复用，防 refcount 泄漏 + 槽数涨到 Capped。
                    reap_dead_slots_from_loop(
                        &handshake_done_tx,
                        &registry,
                        &mut sockets,
                        &mut socket_ctxs,
                        &mut fake_pool,
                        now,
                    );
                }
                // review #7：低频回收 idle 且 refcount==0 超 TTL 的 fake-IP 映射（长稳防泄漏）。
                _ = fake_ip_sweep.tick() => {
                    metrics.loop_park_end();
                    fake_pool.sweep(udp_clock.elapsed().as_secs(), FAKE_IP_TTL);
                }
                // 刀11：数据面可观测性快照（30s）。在此单写者 task 重算 loop-owned gauge → 发布进
                // Arc<Metrics> → snapshot 读出（上行计数经 upstream trait 访问器）→ **无门控**打统一 📊 行
                // （TCP/DNS/failover 在 UDP 空闲时也有意义；UDP-path 行另由 start_udp 发，见 ADR-0012 §5）。
                _ = metrics_tick.tick() => {
                    metrics.loop_park_end();
                    publish_gauges(
                        &metrics_handle,
                        socket_ctxs.values(),
                        &fake_pool,
                        upstream.failover_leg_u8(),
                    );
                    let snap = metrics_handle
                        .snapshot(upstream.udp_drops_up(), upstream.udp_stream_fallbacks());
                    println!("{}", crate::metrics::format_metrics_snapshot(&snap));
                    tcp_diag_log!(
                        "🔎 tcp-loop-flush-tx calls={} failures={}",
                        tcp_loop_flush_tx_calls, tcp_loop_flush_tx_failures
                    );
                    let tcp_downlink = tcp_downlink_aggregate(socket_ctxs.values());
                    tcp_diag_log!(
                        "{}",
                        format_tcp_downlink_flush_diag(&tcp_downlink, dirty.len())
                    );
                    tcp_diag_log!("{}", format_tun_rx_drain_diag(&tun_rx_drain_diag));
                    tcp_diag_log!(
                        "{}",
                        format_local_egress_service_diag(
                            &local_egress_service_diag,
                            tcp_downlink.actor_bypass_admitted_bytes,
                        )
                    );
                    // 刀12：紧挨 📊 行打 🔬 主循环归因行（profiler 关闭时 NoopSink::report 空、零开销）。
                    metrics.report();
                }
                // Knife14bf：TUN egress drop feedback。诊断日志仍受 MINI_VPN_TCP_DIAG 门控，
                // 但反馈状态每秒更新，避免 30s probe 结束后才发现 clean-window qdisc drops。
                _ = tun_egress_feedback_tick.tick() => {
                    metrics.loop_park_end();
                    let downlink_stats_raw = downlink_pressure_stats(&dirty, &socket_ctxs, &sockets);
                    let pressure_now = std::time::Instant::now();
                    tun_egress_feedback.expire_egress_pressure_hold(pressure_now);
                    let tcp_downlink = tcp_downlink_aggregate(socket_ctxs.values());
                    let tun_sample = tun_egress_drop_sampler.sample();
                    let feedback_event =
                        tun_egress_feedback.update(&tun_sample, downlink_stats_raw, downlink_backpressure);
                    let d16_recovery_evidence = apply_d16_global_tun_feedback(
                        &mut d16_global_drop_episode,
                        feedback_event,
                        downlink_backpressure,
                        &mut downlink_egress_drop_debt,
                    );
                    let _ = apply_d16_global_recovery_evidence(
                        &mut dirty,
                        &sockets,
                        &mut socket_ctxs,
                        downlink_backpressure,
                        d16_recovery_evidence,
                    );
                    tun_egress_feedback.observe_pressure_at(
                        downlink_stats_raw,
                        downlink_backpressure,
                        pressure_now,
                    );
                    let sampled_global_rx_paused =
                        if buffered_downlink.enabled || bounded_global_rx_receive_window {
                        relay_event_lane.receive_paused()
                    } else {
                        downlink_rx_paused || tun_egress_feedback.is_paused()
                    };
                    if tcp_diag_enabled() {
                        println!(
                            "{}",
                            format_tun_egress_diag(
                                tun_egress_drop_sampler.interface_label(),
                                &tun_sample,
                                &tcp_downlink,
                                dirty.len(),
                                sampled_global_rx_paused,
                            )
                        );
                        if let Some(event) = feedback_event {
                            println!(
                                "{}",
                                format_tun_egress_feedback_diag(
                                    event,
                                    &tun_egress_feedback,
                                    downlink_backpressure,
                                    downlink_egress_drop_debt,
                                )
                            );
                        }
                    }
                }
                // 分支 2: 时钟滴答，处理超时重传等后台任务
                _ = timer.tick() =>{
                    metrics.loop_park_end();
                    downlink_egress_pacer.on_timer_tick();
                    metrics.enter_poll();
                    let timestamp = smoltcp::time::Instant::now();
                    iface.poll(timestamp, device, &mut sockets);
                    super::ingress_effects::collect(&mut iface, &sockets, &mut dirty, handshake_done_tx.flow_mode());
                    tcp_loop_flush_tx_calls += 1;
                    let (flush_result, _) = flush_tx_and_release_downlink_permits(
                        device,
                        &sockets,
                        &mut socket_ctxs,
                        "timer_poll",
                    )
                    .await;
                    if let Err(e) = flush_result {
                        tcp_loop_flush_tx_failures += 1;
                        tcp_diag_log!(
                            "🔎 tcp-loop-flush-tx-fail stage=timer_poll calls={} failures={} err={e}",
                            tcp_loop_flush_tx_calls, tcp_loop_flush_tx_failures
                        );
                    }
                    metrics.leave_poll();

                    let timer_now = std::time::Instant::now();
                    let has_recent_active_flow = match active_flow_tun_rx_drain_until {
                        Some(deadline) if timer_now <= deadline => true,
                        Some(_) => {
                            active_flow_tun_rx_drain_until = None;
                            false
                        }
                        None => false,
                    };
                    let has_stalled_read_service = d4_stalled_read_service_enabled(&runtime_config)
                        && relay_event_lane.has_stalled_read_service(timer_now);
                    let dirty_downlink_stats = downlink_pressure_stats(&dirty, &socket_ctxs, &sockets);
                    let dirty_close_drain =
                        has_dirty_close_drain(&dirty, &socket_ctxs, downlink_backpressure);
                    let maintenance_tun_rx_budget = tun_rx_drain_budget_for_dirty_pressure(
                        !dirty.is_empty(),
                        dirty_close_drain,
                        dirty_downlink_stats,
                        tun_rx_drain_budget,
                        downlink_backpressure,
                        runtime_config.tun_mtu,
                        capacity_backpressure,
                    );
                    let (timer_tun_rx_budget, timer_tun_rx_source) = if maintenance_tun_rx_budget > 0 {
                        (
                            maintenance_tun_rx_budget,
                            TUN_RX_DRAIN_SOURCE_TIMER_PRESSURE,
                        )
                    } else if has_stalled_read_service {
                        (
                            tun_rx_drain_budget_for_recent_active_flow(
                                true,
                                tun_rx_active_flow_timer_ms.max(1),
                                dirty_downlink_stats,
                                tun_egress_feedback.is_paused(),
                                tun_rx_drain_budget,
                                downlink_backpressure,
                                runtime_config.tun_mtu,
                                capacity_backpressure,
                            ),
                            TUN_RX_DRAIN_SOURCE_TIMER_STALLED_READ,
                        )
                    } else {
                        (
                            tun_rx_drain_budget_for_recent_active_flow(
                                has_recent_active_flow,
                                tun_rx_active_flow_timer_ms,
                                dirty_downlink_stats,
                                tun_egress_feedback.is_paused(),
                                tun_rx_drain_budget,
                                downlink_backpressure,
                                runtime_config.tun_mtu,
                                capacity_backpressure,
                            ),
                            TUN_RX_DRAIN_SOURCE_TIMER_ACTIVE_FLOW,
                        )
                    };
                    if timer_tun_rx_budget > 0 {
                        drain_ready_tun_rx(
                            device,
                            &mut assoc_table,
                            &mut fake_pool,
                            &upstream,
                            udp_clock.elapsed().as_secs(),
                            &metrics_handle,
                            &mut registry,
                            &mut sockets,
                            &mut socket_ctxs,
                            &mut iface,
                            &mut dirty,
                            &handshake_done_tx,
                            &global_tx,
                            &owned_action_tx,
                            &mut metrics,
                            downlink_flush_max_bytes,
                            downlink_backpressure,
                            runtime_config.tun_mtu,
                            &mut downlink_egress_drop_debt,
                            &mut tcp_loop_flush_tx_calls,
                            &mut tcp_loop_flush_tx_failures,
                            &mut tun_rx_drain_diag,
                            timer_tun_rx_budget,
                            timer_tun_rx_source,
                            if buffered_downlink.enabled {
                                tun_egress_feedback.is_paused()
                            } else {
                                downlink_rx_paused || tun_egress_feedback.is_paused()
                            },
                            buffered_downlink,
                        )
                        .await?;
                    }

                    // #1：timer tick 无新 inbound 包，只续推进脏集合（主要是下行 pending flush +
                    // smoltcp 超时重传释放 tx buffer 后继续写）。不再全量 sweep。
                    let close_egress_guard = tun_egress_feedback.is_paused()
                        || downlink_egress_drop_debt.has_active_drop_debt();
                    let relay_read_recovery_active = downlink_egress_drop_debt.has_active_drop_debt();
                    let relay_read_hard_pause = tun_rx_drain_diag
                        .backlog_guard()
                        .is_active()
                        || if buffered_downlink.enabled {
                        tun_egress_feedback.is_paused()
                    } else {
                        downlink_rx_paused || tun_egress_feedback.is_paused()
                    };
                    process_dirty_relay(
                        &mut dirty,
                        &mut sockets,
                        &mut socket_ctxs,
                        &upstream,
                        &handshake_done_tx,
                        &global_tx,
                        &owned_action_tx,
                        &mut fake_pool,
                        udp_clock.elapsed().as_secs(),
                        &metrics_handle,
                        &mut metrics,
                        downlink_flush_max_bytes,
                        downlink_backpressure,
                        runtime_config.tun_mtu,
                        &mut downlink_egress_drop_debt,
                        close_egress_guard,
                        relay_read_recovery_active,
                        relay_read_hard_pause,
                        buffered_downlink,
                        DownlinkAdmissionOrigin::ControlOnly,
                    )
                    .await;
                    if !dirty.is_empty() {
                        let egress_actor =
                            local_egress_actor_enabled(&runtime_config, &dirty, &socket_ctxs);
                        let local_progress = service_local_egress_until(
                            device,
                            &mut assoc_table,
                            &mut fake_pool,
                            &upstream,
                            udp_clock.elapsed().as_secs(),
                            &metrics_handle,
                            &mut registry,
                            &mut sockets,
                            &mut socket_ctxs,
                            &mut iface,
                            &mut dirty,
                            &handshake_done_tx,
                            &global_tx,
                            &owned_action_tx,
                            &mut metrics,
                            downlink_flush_max_bytes,
                            downlink_backpressure,
                            runtime_config.tun_mtu,
                            &mut downlink_egress_drop_debt,
                            &mut tcp_loop_flush_tx_calls,
                            &mut tcp_loop_flush_tx_failures,
                            &mut tun_rx_drain_diag,
                            &mut local_egress_service_diag,
                            relay_read_hard_pause,
                            tun_egress_feedback.is_paused(),
                            buffered_downlink,
                            local_egress_service_config,
                            egress_actor,
                        )
                        .await?;
                        if should_rearm_d3_egress_actor_self_wake(
                            d3_egress_actor_self_wake_enabled(&runtime_config),
                            !dirty.is_empty(),
                            local_progress,
                        ) {
                            local_egress_service_diag.note_egress_actor_self_wake();
                            d3_egress_actor_self_wake_armed = true;
                            d3_egress_actor_self_wake_sleep
                                .as_mut()
                                .reset(tokio::time::Instant::now());
                        }
                    }
                }
            }
            // 刀12：循环底部——即将停在 select! 空等下一个事件，标记 park 开始
            //（NoopSink::loop_park_begin 空、零开销；仅 LoopProfiler 采时钟）。
            metrics.loop_park_begin();
        }
    });
    let outcome = std::future::poll_fn(|cx| {
        match catch_unwind(AssertUnwindSafe(|| execution.as_mut().poll(cx))) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(result)) => Poll::Ready(Ok(result)),
            Err(payload) => Poll::Ready(Err(payload)),
        }
    })
    .await;
    if !flow_mode {
        drop(execution);
        return match outcome {
            Ok(result) => result,
            Err(payload) => std::panic::resume_unwind(payload),
        };
    }
    let Some(owner) = panic_owner else {
        drop(execution);
        return match outcome {
            Ok(result) => result,
            Err(payload) => std::panic::resume_unwind(payload),
        };
    };
    let mut payloads: [Option<Box<dyn std::any::Any + Send>>; 2] = [None, None];
    let mut fatal = false;
    match &outcome {
        Err(_) => {
            owner.note_loop_panic(FailurePhase::SessionLoop);
            fatal = true;
        }
        Ok(Err(error)) => {
            owner.note_loop_error(error);
            fatal = true;
        }
        Ok(Ok(())) => {}
    }
    // Never repoll poisoned execution. Unknown destructors have separate guards.
    if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(execution))) {
        owner.note_loop_panic(FailurePhase::SessionLoopDrop);
        payloads[0] = Some(payload);
        fatal = true;
    }
    match outcome {
        Err(payload) => {
            if let Err(secondary) = catch_unwind(AssertUnwindSafe(|| drop(payload))) {
                owner.note_loop_panic(FailurePhase::SessionLoopDrop);
                payloads[1] = Some(secondary);
            }
        }
        Ok(Err(error)) => {
            if let Err(secondary) = catch_unwind(AssertUnwindSafe(|| drop(error))) {
                owner.note_loop_panic(FailurePhase::SessionLoopDrop);
                payloads[1] = Some(secondary);
            }
        }
        Ok(Ok(())) => {}
    }
    if !fatal {
        return Ok(());
    }
    let mut physical = FatalPhysicalTurn::new(
        &sockets,
        &mut socket_ctxs,
        &fake_pool,
        &mut relay_event_lane,
        &mut selected_relay,
    );
    loop {
        handshake_done_tx.begin_turn().await;
        let step = tokio::select! {
            step = std::future::poll_fn(|cx| handshake_done_tx.poll_fatal(cx, &mut physical)) => step,
            _ = timer.tick() => FatalStep::Waiting,
        };
        match step {
            FatalStep::Retained => std::future::pending::<()>().await,
            FatalStep::Yield => tokio::task::yield_now().await,
            FatalStep::Waiting => {}
        }
    }
}
