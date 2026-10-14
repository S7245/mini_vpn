//! Production-ingress rearm characterizations for G0-T.
//! This private child owns no fixture or production authority. Its tests enter
//! the reaper or passive original-loop bridge; they never call a sink or gateway.

use super::super::super::super::{
    AsyncLeasedByteFlowQueue, D16_PER_FLOW_BYTE_CAPACITY, DownlinkCreditController,
    DownlinkEgressClock, DownstreamPermitReleaseMode, EgressPhase, FakeIpPool, ListenerRegistry,
    SocketCtx, SocketState, TcpDownlinkDiag, TcpSocket, d16_data_ready_matches_ctx,
    reap_dead_slots,
};
use bytes::Bytes;
use smoltcp::iface::SocketSet;
use std::collections::{HashMap, VecDeque};
#[cfg(feature = "harness")]
use std::sync::Arc;
#[cfg(feature = "harness")]
use std::time::Duration;
#[cfg(feature = "harness")]
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;

#[cfg(feature = "harness")]
use super::super::super::super::{NoopSink, TunRuntimeConfig, run_event_loop};
#[cfg(feature = "harness")]
use super::{
    FlowRoleApps, FlowRoleUpstream, drive_original_turn, flow_role_fixture, wait_for_worker,
};
#[cfg(feature = "harness")]
use crate::client_tun::legacy_event_loop::relay_events::RelayEventTestKind;
#[cfg(feature = "harness")]
use crate::client_tun::{RelayClose, RelayEvent};
#[cfg(feature = "harness")]
use crate::harness::{LoopbackTunDevice, PacketLink};
#[cfg(feature = "harness")]
use crate::metrics::Metrics;

#[test]
fn g0_physical_rearm_is_private_to_the_gateway() {
    let root = include_str!("../../client_tun.rs");
    let driver = include_str!("../legacy_event_loop/driver.rs");
    assert!(!driver.contains("fn rearm_socket("));
    assert!(!driver.contains("rearm_socket_with_reason"));
    let relay_events = include_str!("../legacy_event_loop/relay_events.rs");
    let rearm = include_str!("../compatibility_runtime/flow_custody/rearm.rs");

    assert!(
        !root.contains("fn rearm_socket("),
        "the root must not retain the physical rearm sink"
    );
    assert!(
        !root.contains("rearm_socket_with_reason"),
        "every root reasoned rearm origin must enter the private gateway adapters"
    );
    assert!(
        !relay_events.contains("rearm_socket_with_reason"),
        "RelayEvent::Closed must enter the gateway instead of a raw rearm wrapper"
    );
    assert!(
        rearm.contains("fn request_close_or_rearm(")
            && rearm.contains("struct LegacyRearmAuthorization")
            && rearm.contains("fn physical_rearm("),
        "only rearm.rs may hold the gateway, authorization, and physical sink"
    );
    assert!(
        !rearm.contains("pub(in crate::client_tun) fn request_close_or_rearm")
            && !rearm.contains("pub(in crate::client_tun) struct RearmRequest")
            && !rearm.contains("pub(in crate::client_tun) enum RearmDisposition"),
        "the gateway request and physical request types must not escape rearm.rs"
    );
}

#[cfg(feature = "harness")]
#[tokio::test(flavor = "current_thread")]
async fn current_closed_event_rearms_through_original_loop_gateway() {
    let mut fixture = flow_role_fixture();
    let (handle, epoch) = wait_for_worker(&mut fixture)
        .await
        .expect("a real current worker must reach the original loop");
    let bridge = fixture
        .session
        .control
        .relay_event_test_bridge()
        .expect("owner-backed original loop must expose its passive bridge");
    let before_generation = bridge.snapshot().map_or(0, |snapshot| snapshot.generation);

    assert!(
        bridge
            .try_send(
                handle,
                RelayEvent::Closed(RelayClose {
                    epoch,
                    direction: "remote_to_local",
                    reason: "remote_eof",
                }),
            )
            .is_accepted(),
        "the passive bridge must enqueue one matching current Close"
    );

    let observed = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            drive_original_turn(&mut fixture).await;
            if let Some(snapshot) = bridge.snapshot()
                && snapshot.generation > before_generation
                && snapshot.handle == handle
                && snapshot.kind == RelayEventTestKind::Closed
                && snapshot.event_epoch == epoch
            {
                return snapshot;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the original relay-event lane must observe the matching Close");

    assert!(
        !observed.rejected,
        "matching current Close must enter its branch"
    );
    let context = observed
        .context
        .expect("matching Close must retain a listener context after rearm");
    assert_eq!(context.epoch, epoch.wrapping_add(1));
    assert_eq!(context.state, SocketState::Listening);
    assert!(!context.has_uplink_tx);
    assert_eq!(context.pending_downlink_bytes, 0);
    assert!(!observed.dirty, "rearmed listener must leave no dirty flow");

    assert!(
        bridge
            .try_send(handle, RelayEvent::DataReady { epoch })
            .is_accepted(),
        "the passive bridge must enqueue one old-epoch readiness probe"
    );
    let stale = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            drive_original_turn(&mut fixture).await;
            if let Some(snapshot) = bridge.snapshot()
                && snapshot.generation > observed.generation
                && snapshot.handle == handle
                && snapshot.kind == RelayEventTestKind::DataReady
                && snapshot.event_epoch == epoch
            {
                return snapshot;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the original lane must reject the old readiness event");
    assert!(
        stale.rejected,
        "old epoch must be rejected before its branch"
    );
    let stale_context = stale
        .context
        .expect("old epoch rejection must preserve the rearmed listener context");
    assert_eq!(stale_context.epoch, epoch.wrapping_add(1));
    assert_eq!(stale_context.state, SocketState::Listening);
    assert!(!stale_context.has_uplink_tx);
    assert_eq!(stale_context.pending_downlink_bytes, 0);
    assert!(!stale.dirty, "old readiness must not restore dirty work");

    fixture.session.request_stop();
    let _ = tokio::time::timeout(Duration::from_secs(5), fixture.session.observe_shutdown())
        .await
        .expect("current Close fixture must settle its session");
    drop(bridge);
    assert!(fixture.session.control.relay_event_test_bridge().is_none());
}

#[cfg(feature = "harness")]
#[tokio::test(flavor = "current_thread")]
async fn generic_launcher_public_loop_dispatches_then_closes() {
    let inbound = PacketLink::bounded(64);
    let outbound = PacketLink::bounded(64);
    let mut apps = FlowRoleApps::new(outbound.clone(), inbound.clone());
    let upstream = Arc::new(FlowRoleUpstream::new());
    let metrics = Arc::new(Metrics::new());
    let (_downlink, downlink_rx) = mpsc::channel(1);
    let mut config = TunRuntimeConfig::from_sources(Some("1")).expect("single launcher pool");
    config.tcp_socket_buffers.rx_bytes = 4096;
    config.tcp_socket_buffers.tx_bytes = 4096;
    let loop_task = tokio::spawn(run_event_loop(
        LoopbackTunDevice::new(inbound, outbound),
        Arc::clone(&upstream),
        downlink_rx,
        config,
        Arc::clone(&metrics),
        NoopSink,
    ));

    let opened = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            apps.turn().await;
            if upstream.entered.load(std::sync::atomic::Ordering::SeqCst) == 1 && apps.established()
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok();
    assert!(opened, "public legacy loop must start one Generic open");

    upstream.release.notify_one();
    let mut peer = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            apps.turn().await;
            if metrics.snapshot(0, 0).relays_spawned == 1
                && let Some(peer) = upstream.peer.lock().expect("launcher peer mutex").take()
            {
                return peer;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("Generic launcher must install one relay");
    assert_eq!(metrics.snapshot(0, 0).relays_spawned, 1);

    let mut uplink = [0; b"f0-retained".len()];
    tokio::time::timeout(Duration::from_secs(5), peer.read_exact(&mut uplink))
        .await
        .expect("relay writer must reach the Generic peer")
        .expect("Generic peer read");
    assert_eq!(&uplink, b"f0-retained");

    peer.shutdown().await.expect("controlled peer EOF");
    let mut eof = [0; 1];
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), peer.read(&mut eof))
            .await
            .expect("relay cleanup must finish")
            .expect("Generic peer EOF read"),
        0,
        "remote EOF closes the detached legacy relay after its uplink"
    );
    loop_task.abort();
    assert!(
        loop_task.await.is_err(),
        "test must join its cancelled loop task"
    );
}

#[tokio::test]
async fn d16_terminal_socket_rearm_preserves_local_terminal_cause() {
    let queue = AsyncLeasedByteFlowQueue::new_with_release_mode(
        D16_PER_FLOW_BYTE_CAPACITY,
        DownstreamPermitReleaseMode::OnEgressDrain,
    )
    .unwrap();
    let reservation = queue
        .reserve_read_up_to(D16_PER_FLOW_BYTE_CAPACITY)
        .await
        .unwrap();
    reservation
        .commit(Bytes::from(vec![9; D16_PER_FLOW_BYTE_CAPACITY]))
        .unwrap();

    let mut sockets = SocketSet::new(vec![]);
    let mut ctxs = HashMap::new();
    let mut registry = ListenerRegistry::new(1);
    registry.ensure_port(443, &mut sockets, &mut ctxs).unwrap();
    let handle = registry.handles_for_port(443)[0];
    sockets.get_mut::<TcpSocket>(handle).abort();
    let ctx = ctxs.get_mut(&handle).unwrap();
    ctx.state = SocketState::Relaying;
    ctx.d16_downlink_queue = Some(queue.clone());
    let mut pool = FakeIpPool::new();

    assert_eq!(
        reap_dead_slots(&registry, &mut sockets, &mut ctxs, &mut pool, 20),
        1,
        "the real reaper must take the terminal listener incarnation"
    );

    let queue_snapshot = queue.snapshot_now();
    assert_eq!(
        queue_snapshot.closure.terminal_cause(),
        Some(crate::tcp_downlink_pump::LeasedByteQueueTerminalCause {
            direction: "local_to_remote",
            reason: "local_socket_terminal",
        })
    );
    assert_eq!(queue_snapshot.queued_bytes, 0);
    assert_eq!(
        ctxs.get(&handle)
            .unwrap()
            .downlink_diag
            .permit_terminal_drop_bytes,
        0
    );
    assert!(ctxs.get(&handle).unwrap().d16_downlink_queue.is_none());
}
#[tokio::test]
async fn d16_rearm_detaches_old_epoch_queue_and_ignores_late_wake() {
    let queue = AsyncLeasedByteFlowQueue::new_with_release_mode(
        512 * 1024,
        DownstreamPermitReleaseMode::OnEgressDrain,
    )
    .unwrap();
    let reservation = queue.reserve_read_up_to(16 * 1024).await.unwrap();
    reservation.commit(Bytes::from(vec![8; 16 * 1024])).unwrap();

    let mut sockets = SocketSet::new(vec![]);
    let mut ctxs = HashMap::new();
    let mut registry = ListenerRegistry::new(1);
    registry.ensure_port(443, &mut sockets, &mut ctxs).unwrap();
    let handle = registry.handles_for_port(443)[0];
    let ctx = ctxs.get_mut(&handle).unwrap();
    ctx.state = SocketState::Relaying;
    ctx.conn_epoch = 21;
    ctx.d16_downlink_queue = Some(queue.clone());
    assert!(d16_data_ready_matches_ctx(ctx, 21));
    sockets.get_mut::<TcpSocket>(handle).abort();
    let mut pool = FakeIpPool::new();

    assert_eq!(
        reap_dead_slots(&registry, &mut sockets, &mut ctxs, &mut pool, 1),
        1,
        "the real reaper must reset the closed incarnation"
    );

    let ctx = ctxs.get(&handle).unwrap();
    assert_eq!(ctx.conn_epoch, 22);
    assert!(ctx.d16_downlink_queue.is_none());
    assert!(!ctx.d16_tun_rx_ack_barrier);
    assert!(!d16_data_ready_matches_ctx(ctx, 21));
    assert!(!d16_data_ready_matches_ctx(ctx, 22));
    let snapshot = queue.snapshot_now();
    assert!(snapshot.closure.is_closed());
    assert_eq!(snapshot.queued_bytes, 0);
    assert_eq!(snapshot.leased_bytes, 0);
    assert_eq!(snapshot.reserved_bytes, 0);
}
#[test]
fn reaper_restores_listening_state_and_releases_fake_ip() {
    let (tx, _rx) = mpsc::channel(1);
    let mut sockets = SocketSet::new(vec![]);
    let mut ctxs = HashMap::new();
    let mut registry = ListenerRegistry::new(1);
    registry.ensure_port(80, &mut sockets, &mut ctxs).unwrap();
    let handle = registry.handles_for_port(80)[0];
    let socket = sockets.get_mut::<TcpSocket>(handle);
    socket.set_nagle_enabled(true);
    socket.set_ack_delay(Some(smoltcp::time::Duration::from_millis(10)));

    let mut pool = FakeIpPool::new();
    let ip = pool.alloc("x.com", 0).expect("fake-IP capacity");
    pool.acquire(ip, 0); // 模拟本 flow 已 acquire
    let mut downlink_diag = TcpDownlinkDiag::default();
    downlink_diag.note_remote_payload(128, 128);
    *ctxs.get_mut(&handle).unwrap() = SocketCtx {
        local_port: 80,
        state: SocketState::Relaying,
        uplink_tx: Some(tx),
        resumable_uplink: None,
        resumable_sink: None,
        resumable_half_close: None,
        resumable_terminal: None,
        resumable_action_bridge: None,
        pending_open_mode: None,
        relay_read_credit_tx: None,
        d16_downlink_queue: None,
        d16_egress_phase: EgressPhase::Running,
        d16_tun_rx_ack_barrier: true,
        relay_read_credit_progress_generation_sent: 0,
        local_fin_sent: true,
        local_eof_sent: true,
        owned_terminal_pending: false,
        owned_terminal_wait_since_secs: None,
        local_fin_pending_since_secs: Some(3),
        local_fin_last_remote_progress_secs: Some(4),
        downlink_pending: Vec::new(),
        downlink_pending_permits: VecDeque::new(),
        downlink_inflight_permits: VecDeque::new(),
        downlink_delay_permit_release_until_egress: false,
        pending_relay_close: None,
        pending_relay_close_since_secs: None,
        pending_relay_close_last_progress_secs: None,
        pending_relay_close_last_pending_bytes: 0,
        pending_relay_close_last_egress_progress_secs: Some(5),
        pending_relay_close_last_egress_queue_bytes: 12_345,
        downlink_pending_last_progress_secs: Some(9),
        downlink_pending_last_pending_bytes: 42,
        fake_ip: Some(ip),
        flow_mirror: None,
        local_reset: None,
        conn_epoch: 7,
        uplink_buffer: vec![1, 2, 3],
        downlink_diag,
        downlink_egress_clock: DownlinkEgressClock {
            last_send_queue: Some(12_345),
            drain_credit_bytes: 1024,
            drop_debt_generation_seen: 3,
        },
        downlink_credit_controller: DownlinkCreditController {
            read_credit_ceiling_bytes: 32 * 1024,
            adaptive_ack_drain_bytes: 4 * 1024,
            ack_cadence_boost_bytes: 4 * 1024,
            egress_payload_credit_bytes: 0,
            egress_progress_generation: 0,
            headroom_debt_bytes: 4096,
            no_egress_progress_streak: 3,
        },
        downlink_d3_actor_flow: true,
        downlink_d3_actor_adaptive_credit: true,
        downlink_d3_actor_legacy_credit: true,
        downlink_d5_capacity_backpressure: true,
        last_tcp_lifecycle: None,
        reverse_window_last_log_secs: Some(12),
    };
    sockets.get_mut::<TcpSocket>(handle).abort();

    assert_eq!(
        reap_dead_slots(&registry, &mut sockets, &mut ctxs, &mut pool, 1),
        1,
        "the real reaper must restore the listener"
    );

    let ctx = ctxs.get(&handle).unwrap();
    let socket = sockets.get::<TcpSocket>(handle);
    assert_eq!(ctx.state, SocketState::Listening);
    assert!(ctx.uplink_tx.is_none());
    assert!(ctx.relay_read_credit_tx.is_none());
    assert!(ctx.d16_downlink_queue.is_none());
    assert!(!ctx.d16_tun_rx_ack_barrier);
    assert!(!ctx.local_fin_sent, "rearm 应清空本地 FIN 发送状态");
    assert!(!ctx.local_eof_sent, "rearm 应清空本地 EOF 发送状态");
    assert!(
        ctx.local_fin_pending_since_secs.is_none(),
        "rearm 应清空 deferred local FIN 状态"
    );
    assert!(
        ctx.local_fin_last_remote_progress_secs.is_none(),
        "rearm 应清空 deferred local FIN 进展时间"
    );
    assert!(
        ctx.pending_relay_close_last_egress_progress_secs.is_none(),
        "rearm 应清空 deferred close egress 进展时间"
    );
    assert_eq!(
        ctx.pending_relay_close_last_egress_queue_bytes, 0,
        "rearm 应清空 deferred close egress 队列状态"
    );
    assert!(ctx.fake_ip.is_none(), "rearm 应清空 fake_ip");
    assert!(
        ctx.uplink_buffer.is_empty(),
        "rearm 应清空 uplink_buffer（M3 patch）"
    );
    assert_eq!(
        ctx.downlink_diag.downlink_pending_high_water, 0,
        "rearm 应清空当前 flow 的 downlink diagnostics"
    );
    assert_eq!(
        ctx.downlink_egress_clock,
        DownlinkEgressClock::default(),
        "rearm 应清空当前 flow 的 egress progress clock"
    );
    assert_eq!(
        ctx.downlink_credit_controller,
        DownlinkCreditController::default(),
        "rearm 应清空当前 flow 的 local credit controller"
    );
    assert!(
        !ctx.downlink_d3_actor_flow,
        "rearm 应清空当前 flow 的 D3 actor egress pacing 状态"
    );
    assert!(
        !ctx.downlink_d3_actor_legacy_credit,
        "rearm must clear diagnostic D3 legacy credit state"
    );
    assert!(
        !ctx.downlink_d5_capacity_backpressure,
        "rearm must clear diagnostic D5 capacity backpressure state"
    );
    assert!(
        ctx.reverse_window_last_log_secs.is_none(),
        "rearm 应清空 reverse-window diagnostics 限流状态"
    );
    assert_eq!(
        ctx.conn_epoch, 8,
        "rearm 应 bump conn_epoch（让在飞 open 失效，M3）"
    );
    assert!(
        !socket.nagle_enabled(),
        "rearm 应恢复本地 virtual-link TCP no-Nagle 策略"
    );
    assert!(
        socket.ack_delay().is_none(),
        "rearm 应恢复本地 virtual-link TCP no-delayed-ACK 策略"
    );
    // refcount 已归零 → idle 超 TTL 可回收（证明 rearm 走了 release）。
    assert_eq!(pool.sweep(1000, 300), 1);
}
