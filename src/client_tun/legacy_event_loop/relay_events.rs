//! Original-loop relay-event receiver and dispositions.
//!
//! The root retains devices, sockets, timers and session authority. Each turn
//! borrows those resources. F DATA is explicitly transferred or discarded under
//! its local physical marker; unknown disposition cannot certify settlement.
//! Tests: external client_tun compatibility leaves through the passive bridge.

use super::super::*;

/// Terminal reason reported by one background TCP relay task.
#[derive(Debug, Clone, Copy)]
pub(in crate::client_tun) struct RelayClose {
    pub(in crate::client_tun) epoch: u64,
    pub(in crate::client_tun) direction: &'static str,
    pub(in crate::client_tun) reason: &'static str,
}

/// Event sent from one background TCP relay task back into the single owner
/// main loop. Data events preserve the existing downlink path; Closed makes
/// every relay terminal reason visible to the socket state machine.
#[derive(Debug)]
pub(in crate::client_tun) enum RelayEvent {
    Data {
        epoch: u64,
        bytes: Vec<u8>,
        stream_service_window: StreamServiceWindow,
        downstream_permit: Option<DownstreamBytePermit>,
    },
    DataReady {
        epoch: u64,
    },
    AckDrainHint {
        epoch: u64,
        gap_ms: u128,
        remote_to_global_rx_bytes: u64,
    },
    Closed(RelayClose),
}

impl RelayEvent {
    fn epoch(&self) -> u64 {
        match self {
            Self::Data { epoch, .. }
            | Self::DataReady { epoch }
            | Self::AckDrainHint { epoch, .. } => *epoch,
            Self::Closed(close) => close.epoch,
        }
    }
}

#[cfg(all(test, feature = "harness"))]
mod test_bridge;

#[cfg(all(test, feature = "harness"))]
pub(in crate::client_tun) use test_bridge::{
    RelayEventTestBridge, RelayEventTestContext, RelayEventTestKind, RelayEventTestSendResult,
    RelayEventTestSnapshot,
};

pub(in crate::client_tun) fn remote_payload_left_local_egress_work(
    ctx: Option<&SocketCtx>,
    accepted_bytes: usize,
) -> bool {
    ctx.map(|context| !context.downlink_pending.is_empty() || accepted_bytes > 0)
        .unwrap_or(false)
}

pub(in crate::client_tun) fn should_drain_tun_rx_after_remote_payload(
    ctx: Option<&SocketCtx>,
    accepted_bytes: usize,
) -> bool {
    accepted_bytes > 0
        || ctx
            .map(|context| !context.downlink_pending.is_empty())
            .unwrap_or(false)
}

pub(in crate::client_tun) fn tun_rx_drain_budget_before_remote_payload(
    ctx: Option<&SocketCtx>,
    snapshot: SocketCloseSnapshot,
    epoch: u64,
    payload_len: usize,
    configured_budget: usize,
    cfg: DownlinkBackpressureConfig,
    tun_mtu: usize,
    capacity_backpressure: bool,
) -> usize {
    let Some(context) = ctx else {
        return 0;
    };
    if payload_len == 0 || context.conn_epoch != epoch {
        return 0;
    }
    if remote_payload_disposition(context, snapshot) != RemotePayloadDisposition::Accept {
        return 0;
    }

    tun_rx_drain_budget_after_remote_payload(
        true,
        snapshot.send_queue,
        configured_budget,
        cfg,
        tun_mtu,
        capacity_backpressure,
    )
}

pub(in crate::client_tun) fn note_ack_cadence_gap_hint_for_handle(
    handle: SocketHandle,
    epoch: u64,
    sockets: &SocketSet<'_>,
    socket_ctxs: &mut HashMap<SocketHandle, SocketCtx>,
    cfg: DownlinkBackpressureConfig,
    tun_mtu: usize,
    hard_pause_active: bool,
) -> Option<usize> {
    let snapshot = {
        let socket = sockets.get::<TcpSocket>(handle);
        SocketCloseSnapshot::from_socket(socket)
    };
    let ctx = socket_ctxs.get_mut(&handle)?;
    if ctx.conn_epoch != epoch || !matches!(ctx.state, SocketState::Relaying | SocketState::Closing)
    {
        return None;
    }
    let local_pressure_bytes = snapshot
        .send_queue
        .saturating_add(ctx.downlink_pending.len());
    ctx.downlink_credit_controller.note_ack_cadence_gap_hint(
        local_pressure_bytes,
        hard_pause_active,
        cfg,
        tun_mtu,
    );
    Some(ctx.downlink_credit_controller.adaptive_ack_drain_floor_for(
        local_pressure_bytes,
        cfg,
        tun_mtu,
    ))
}

pub(in crate::client_tun) fn d4_stalled_read_service_deadline(
    now: std::time::Instant,
) -> std::time::Instant {
    now + std::time::Duration::from_millis(D4_STALLED_READ_SERVICE_HOLD_MS)
}

pub(in crate::client_tun) fn arm_d4_stalled_read_service(
    enabled: bool,
    stalled_read_service_until: &mut HashMap<SocketHandle, std::time::Instant>,
    handle: SocketHandle,
    now: std::time::Instant,
) -> bool {
    if !enabled {
        return false;
    }
    stalled_read_service_until.insert(handle, d4_stalled_read_service_deadline(now));
    true
}

pub(in crate::client_tun) fn clear_d4_stalled_read_service(
    stalled_read_service_until: &mut HashMap<SocketHandle, std::time::Instant>,
    handle: SocketHandle,
) -> bool {
    stalled_read_service_until.remove(&handle).is_some()
}

pub(in crate::client_tun) fn has_d4_stalled_read_service(
    stalled_read_service_until: &mut HashMap<SocketHandle, std::time::Instant>,
    now: std::time::Instant,
) -> bool {
    stalled_read_service_until.retain(|_, deadline| *deadline >= now);
    !stalled_read_service_until.is_empty()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::client_tun) enum RelayGapAckDrainFollowup {
    BudgetExhausted,
    Probe,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(in crate::client_tun) struct RelayGapAckDrainState {
    armed: Option<RelayGapAckDrainFollowup>,
    probe_followups_remaining: u8,
}

impl RelayGapAckDrainState {
    pub(in crate::client_tun) fn is_armed(&self) -> bool {
        self.armed.is_some()
    }

    pub(in crate::client_tun) fn arm_after_budget_exhausted(&mut self) -> bool {
        if self.armed.is_some() {
            return false;
        }
        self.armed = Some(RelayGapAckDrainFollowup::BudgetExhausted);
        self.probe_followups_remaining = 0;
        true
    }

    pub(in crate::client_tun) fn arm_after_hint_drain(
        &mut self,
        drained: usize,
        budget: usize,
    ) -> bool {
        if budget == 0 || self.armed.is_some() {
            return false;
        }
        if drained >= budget {
            return self.arm_after_budget_exhausted();
        }
        self.probe_followups_remaining = if drained == 0 {
            RELAY_GAP_ACK_DRAIN_EMPTY_FOLLOWUPS
        } else {
            RELAY_GAP_ACK_DRAIN_PROGRESS_FOLLOWUPS
        };
        self.armed = Some(RelayGapAckDrainFollowup::Probe);
        true
    }

    pub(in crate::client_tun) fn arm_after_followup_drain(
        &mut self,
        mode: RelayGapAckDrainFollowup,
        drained: usize,
        budget: usize,
    ) -> bool {
        if budget == 0 || self.armed.is_some() {
            return false;
        }
        if drained >= budget {
            return self.arm_after_budget_exhausted();
        }
        match mode {
            RelayGapAckDrainFollowup::BudgetExhausted => false,
            RelayGapAckDrainFollowup::Probe => {
                if drained > 0 {
                    self.probe_followups_remaining = RELAY_GAP_ACK_DRAIN_PROGRESS_FOLLOWUPS;
                } else if self.probe_followups_remaining > 0 {
                    self.probe_followups_remaining -= 1;
                } else {
                    return false;
                }
                self.armed = Some(RelayGapAckDrainFollowup::Probe);
                true
            }
        }
    }

    pub(in crate::client_tun) fn take_due(&mut self) -> Option<RelayGapAckDrainFollowup> {
        let mode = self.armed?;
        self.armed = None;
        Some(mode)
    }
}

pub(in crate::client_tun) fn tun_rx_drain_budget_for_relay_gap_hint(
    ctx: Option<&SocketCtx>,
    epoch: u64,
    stats: DownlinkPressureStats,
    tun_feedback_paused: bool,
    configured_budget: usize,
    cfg: DownlinkBackpressureConfig,
    tun_mtu: usize,
    capacity_backpressure: bool,
) -> usize {
    let Some(ctx) = ctx else {
        return 0;
    };
    if ctx.conn_epoch != epoch || !matches!(ctx.state, SocketState::Relaying | SocketState::Closing)
    {
        return 0;
    }
    let max_budget = if configured_budget > 0 {
        configured_budget
    } else {
        pressure_tun_rx_drain_budget(cfg, tun_mtu)
    };
    active_ack_drain_budget_for_egress_headroom(
        stats,
        cfg,
        tun_feedback_paused,
        max_budget,
        capacity_backpressure,
    )
}

pub(in crate::client_tun) fn tun_rx_drain_budget_for_relay_gap_followup(
    stats: DownlinkPressureStats,
    tun_feedback_paused: bool,
    configured_budget: usize,
    cfg: DownlinkBackpressureConfig,
    tun_mtu: usize,
    capacity_backpressure: bool,
) -> usize {
    let max_budget = if configured_budget > 0 {
        configured_budget
    } else {
        pressure_tun_rx_drain_budget(cfg, tun_mtu)
    };
    active_ack_drain_budget_for_egress_headroom(
        stats,
        cfg,
        tun_feedback_paused,
        max_budget,
        capacity_backpressure,
    )
}

/// Receiver-local state. It never owns any root resource or physical timer.
pub(in crate::client_tun) struct RelayEventLane {
    global_rx: tokio::sync::mpsc::Receiver<(SocketHandle, RelayEvent)>,
    stalled_read_service_until: HashMap<SocketHandle, std::time::Instant>,
    relay_gap_ack_drain: RelayGapAckDrainState,
    global_rx_receive_paused: bool,
}

impl RelayEventLane {
    pub(in crate::client_tun) fn new(
        global_rx: tokio::sync::mpsc::Receiver<(SocketHandle, RelayEvent)>,
    ) -> Self {
        Self {
            global_rx,
            stalled_read_service_until: HashMap::new(),
            relay_gap_ack_drain: RelayGapAckDrainState::default(),
            global_rx_receive_paused: false,
        }
    }

    pub(in crate::client_tun) fn recv(
        &mut self,
    ) -> impl std::future::Future<Output = Option<(SocketHandle, RelayEvent)>> + '_ {
        self.global_rx.recv()
    }

    pub(in crate::client_tun) fn try_recv_retained(
        &mut self,
    ) -> Result<(SocketHandle, RelayEvent), tokio::sync::mpsc::error::TryRecvError> {
        self.global_rx.try_recv()
    }

    pub(in crate::client_tun) fn receive_paused(&self) -> bool {
        self.global_rx_receive_paused
    }

    pub(in crate::client_tun) fn set_receive_paused(&mut self, paused: bool) {
        self.global_rx_receive_paused = paused;
    }

    pub(in crate::client_tun) fn relay_gap_ack_drain_is_armed(&self) -> bool {
        self.relay_gap_ack_drain.is_armed()
    }

    pub(in crate::client_tun) fn has_stalled_read_service(
        &mut self,
        now: std::time::Instant,
    ) -> bool {
        has_d4_stalled_read_service(&mut self.stalled_read_service_until, now)
    }
}

impl RelayEventLane {
    pub(in crate::client_tun) async fn handle_relay_gap_followup<D, U, M>(
        &mut self,
        turn: RelayEventTurn<'_, D, U, M>,
    ) -> std::io::Result<()>
    where
        D: TunIo,
        U: OwnedUpstream + 'static,
        M: MetricsSink,
    {
        let Some(mode) = self.relay_gap_ack_drain.take_due() else {
            return Ok(());
        };
        let RelayEventTurn {
            socket:
                RelayEventSocketState {
                    sockets,
                    socket_ctxs,
                    dirty,
                },
            ingress:
                RelayEventIngressState {
                    device,
                    assoc_table,
                    fake_pool,
                    upstream,
                    udp_clock,
                    metrics_handle,
                    registry,
                    iface,
                    handshake_done_tx,
                    global_tx,
                    owned_action_tx,
                    metrics,
                    runtime_config,
                    downlink_backpressure,
                    downlink_flush_max_bytes,
                    tun_rx_drain_budget,
                    capacity_backpressure,
                    buffered_downlink,
                    downlink_rx_paused,
                    tun_egress_feedback,
                    downlink_egress_drop_debt,
                    tcp_loop_flush_tx_calls,
                    tcp_loop_flush_tx_failures,
                    tun_rx_drain_diag,
                    mut relay_gap_ack_drain_sleep,
                    ..
                },
        } = turn;

        let tun_rx_budget = tun_rx_drain_budget_for_relay_gap_followup(
            downlink_pressure_stats(dirty, socket_ctxs, sockets),
            tun_egress_feedback.is_paused(),
            tun_rx_drain_budget,
            downlink_backpressure,
            runtime_config.tun_mtu,
            capacity_backpressure,
        );
        if tun_rx_budget > 0 {
            let drained = drain_ready_tun_rx(
                device,
                assoc_table,
                fake_pool,
                upstream,
                udp_clock.elapsed().as_secs(),
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
                runtime_config.tun_mtu,
                downlink_egress_drop_debt,
                tcp_loop_flush_tx_calls,
                tcp_loop_flush_tx_failures,
                tun_rx_drain_diag,
                tun_rx_budget,
                TUN_RX_DRAIN_SOURCE_RELAY_GAP_HINT_FOLLOWUP,
                if buffered_downlink.enabled {
                    tun_egress_feedback.is_paused()
                } else {
                    downlink_rx_paused || tun_egress_feedback.is_paused()
                },
                buffered_downlink,
            )
            .await?;
            if self
                .relay_gap_ack_drain
                .arm_after_followup_drain(mode, drained, tun_rx_budget)
            {
                relay_gap_ack_drain_sleep.as_mut().reset(
                    tokio::time::Instant::now()
                        + std::time::Duration::from_millis(RELAY_GAP_ACK_DRAIN_FOLLOWUP_DELAY_MS),
                );
            }
        }
        Ok(())
    }
}

/// The exact root-owned socket group borrowed for one selected relay event.
pub(in crate::client_tun) struct RelayEventSocketState<'turn> {
    pub(in crate::client_tun) sockets: &'turn mut SocketSet<'static>,
    pub(in crate::client_tun) socket_ctxs: &'turn mut HashMap<SocketHandle, SocketCtx>,
    pub(in crate::client_tun) dirty: &'turn mut HashSet<SocketHandle>,
}

/// The exact root-owned ingress and loop group borrowed for one selected event.
pub(in crate::client_tun) struct RelayEventIngressState<'turn, D, U, M> {
    pub(in crate::client_tun) device: &'turn mut D,
    pub(in crate::client_tun) assoc_table: &'turn mut AssocTable,
    pub(in crate::client_tun) fake_pool: &'turn mut FakeIpPool,
    pub(in crate::client_tun) upstream: &'turn Arc<U>,
    pub(in crate::client_tun) udp_clock: std::time::Instant,
    pub(in crate::client_tun) metrics_handle: &'turn Metrics,
    pub(in crate::client_tun) registry: &'turn mut ListenerRegistry,
    pub(in crate::client_tun) iface: &'turn mut Interface,
    pub(in crate::client_tun) handshake_done_tx: &'turn OpenDispatch,
    pub(in crate::client_tun) global_tx:
        &'turn tokio::sync::mpsc::Sender<(SocketHandle, RelayEvent)>,
    pub(in crate::client_tun) owned_action_tx: &'turn tokio::sync::mpsc::Sender<OwnedActionEvent>,
    pub(in crate::client_tun) metrics: &'turn mut M,
    pub(in crate::client_tun) runtime_config: &'turn TunRuntimeConfig,
    pub(in crate::client_tun) downlink_backpressure: DownlinkBackpressureConfig,
    pub(in crate::client_tun) downlink_flush_max_bytes: usize,
    pub(in crate::client_tun) tun_rx_drain_budget: usize,
    pub(in crate::client_tun) tun_rx_active_flow_timer_duration: Option<std::time::Duration>,
    pub(in crate::client_tun) capacity_backpressure: bool,
    pub(in crate::client_tun) buffered_downlink: BufferedDownlinkConfig,
    pub(in crate::client_tun) downlink_rx_paused: bool,
    pub(in crate::client_tun) tun_egress_feedback: &'turn TunEgressFeedbackState,
    pub(in crate::client_tun) downlink_egress_pacer: &'turn mut DownlinkEgressPacer,
    pub(in crate::client_tun) downlink_egress_drop_debt: &'turn mut DownlinkEgressDropDebt,
    pub(in crate::client_tun) tcp_loop_flush_tx_calls: &'turn mut u64,
    pub(in crate::client_tun) tcp_loop_flush_tx_failures: &'turn mut u64,
    pub(in crate::client_tun) tun_rx_drain_diag: &'turn mut TunRxDrainDiag,
    pub(in crate::client_tun) local_egress_service_config: LocalEgressServiceConfig,
    pub(in crate::client_tun) local_egress_service_diag: &'turn mut LocalEgressServiceDiag,
    pub(in crate::client_tun) active_flow_tun_rx_drain_until: &'turn mut Option<std::time::Instant>,
    pub(in crate::client_tun) deferred_ack_drain: &'turn mut DeferredAckDrainState,
    pub(in crate::client_tun) deferred_ack_drain_sleep:
        std::pin::Pin<&'turn mut tokio::time::Sleep>,
    pub(in crate::client_tun) d3_egress_actor_self_wake_armed: &'turn mut bool,
    pub(in crate::client_tun) d3_egress_actor_self_wake_sleep:
        std::pin::Pin<&'turn mut tokio::time::Sleep>,
    pub(in crate::client_tun) relay_gap_ack_drain_sleep:
        std::pin::Pin<&'turn mut tokio::time::Sleep>,
}

/// A nonescaping two-group borrow for one relay-event disposition.
pub(in crate::client_tun) struct RelayEventTurn<'turn, D, U, M> {
    pub(in crate::client_tun) socket: RelayEventSocketState<'turn>,
    pub(in crate::client_tun) ingress: RelayEventIngressState<'turn, D, U, M>,
}

#[cfg(test)]
pub(in crate::client_tun) fn handle_relay_closed(
    handle: SocketHandle,
    close: RelayClose,
    sockets: &mut SocketSet<'_>,
    socket_ctxs: &mut HashMap<SocketHandle, SocketCtx>,
    fake_pool: &mut FakeIpPool,
    now_secs: u64,
    downlink_backpressure: DownlinkBackpressureConfig,
) -> bool {
    handle_relay_closed_with_gate(
        RearmGate::legacy_facade(),
        handle,
        close,
        sockets,
        socket_ctxs,
        fake_pool,
        now_secs,
        downlink_backpressure,
    )
}

pub(in crate::client_tun) fn handle_relay_closed_from_loop(
    dispatch: &OpenDispatch,
    handle: SocketHandle,
    close: RelayClose,
    sockets: &mut SocketSet<'_>,
    socket_ctxs: &mut HashMap<SocketHandle, SocketCtx>,
    fake_pool: &mut FakeIpPool,
    now_secs: u64,
    downlink_backpressure: DownlinkBackpressureConfig,
) -> bool {
    handle_relay_closed_with_gate(
        RearmGate::original_loop(dispatch),
        handle,
        close,
        sockets,
        socket_ctxs,
        fake_pool,
        now_secs,
        downlink_backpressure,
    )
}

fn handle_relay_closed_with_gate(
    gate: RearmGate<'_>,
    handle: SocketHandle,
    close: RelayClose,
    sockets: &mut SocketSet<'_>,
    socket_ctxs: &mut HashMap<SocketHandle, SocketCtx>,
    fake_pool: &mut FakeIpPool,
    now_secs: u64,
    downlink_backpressure: DownlinkBackpressureConfig,
) -> bool {
    let Some(ctx) = socket_ctxs.get_mut(&handle) else {
        return false;
    };
    if ctx.conn_epoch != close.epoch {
        trace_log!(
            "🗑️ handle {:?} relay close({}/{}) epoch {}≠{}; ignored",
            handle,
            close.direction,
            close.reason,
            close.epoch,
            ctx.conn_epoch
        );
        return false;
    }
    if ctx.state == SocketState::Listening && ctx.uplink_tx.is_none() {
        trace_log!(
            "🗑️ handle {:?} relay close({}/{}) arrived after rearm; ignored",
            handle,
            close.direction,
            close.reason
        );
        return false;
    }
    if !gate_legacy_rearm(
        gate,
        RearmIntent::reasoned_ctx(handle, ctx, close.direction, close.reason, None),
    ) {
        return true;
    }
    if let Some(pending) = ctx.pending_relay_close {
        ctx.state = SocketState::Closing;
        ctx.uplink_tx = None;
        if pending.reason == "remote_eof" && close.reason != "remote_eof" {
            ctx.pending_relay_close = Some(close);
        }
        trace_log!(
            "⏳ handle {:?} relay close({}/{}) coalesced with pending close({}/{})",
            handle,
            close.direction,
            close.reason,
            pending.direction,
            pending.reason
        );
        return true;
    }
    let snapshot = {
        let tcp_socket = sockets.get::<TcpSocket>(handle);
        SocketCloseSnapshot::from_socket(tcp_socket)
    };
    log_tcp_lifecycle_observation(handle, ctx, snapshot, now_secs, "relay_close");
    if start_deferred_close_for_pending_downlink(ctx, close, now_secs) {
        trace_log!(
            "⏳ handle {:?} relay close({}/{}) waits for {} pending downlink bytes",
            handle,
            close.direction,
            close.reason,
            ctx.downlink_pending.len()
        );
        tcp_diag_log!(
            "🔎 tcp-deferred-close-pending handle={:?} direction={} reason={} pending={} started_secs={}",
            handle,
            close.direction,
            close.reason,
            ctx.downlink_pending.len(),
            now_secs
        );
        return true;
    }

    if start_deferred_close_for_egress_if_needed(
        ctx,
        close,
        snapshot,
        downlink_backpressure,
        now_secs,
    ) {
        tcp_diag_log!(
            "🔎 tcp-deferred-close-egress handle={:?} direction={} reason={} send_queue={} high={} low={} started_secs={}",
            handle,
            close.direction,
            close.reason,
            snapshot.send_queue,
            downlink_backpressure.high_bytes,
            downlink_backpressure.low_bytes,
            now_secs
        );
        return true;
    }

    let tcp_socket = sockets.get_mut::<TcpSocket>(handle);
    legacy_reasoned_rearm_with_gate(
        gate,
        handle,
        tcp_socket,
        ctx,
        fake_pool,
        now_secs,
        close.direction,
        close.reason,
    );
    false
}

impl RelayEventLane {
    pub(in crate::client_tun) async fn handle_relay_event<D, U, M>(
        &mut self,
        handle: SocketHandle,
        event: &mut RelayEvent,
        turn: RelayEventTurn<'_, D, U, M>,
    ) -> std::io::Result<()>
    where
        D: TunIo,
        U: OwnedUpstream + 'static,
        M: MetricsSink,
    {
        let RelayEventTurn {
            socket:
                RelayEventSocketState {
                    sockets,
                    socket_ctxs,
                    dirty,
                },
            ingress:
                RelayEventIngressState {
                    device,
                    assoc_table,
                    fake_pool,
                    upstream,
                    udp_clock,
                    metrics_handle,
                    registry,
                    iface,
                    handshake_done_tx,
                    global_tx,
                    owned_action_tx,
                    metrics,
                    runtime_config,
                    downlink_backpressure,
                    downlink_flush_max_bytes,
                    tun_rx_drain_budget,
                    tun_rx_active_flow_timer_duration,
                    capacity_backpressure,
                    buffered_downlink,
                    downlink_rx_paused,
                    tun_egress_feedback,
                    downlink_egress_pacer,
                    downlink_egress_drop_debt,
                    tcp_loop_flush_tx_calls,
                    tcp_loop_flush_tx_failures,
                    tun_rx_drain_diag,
                    local_egress_service_config,
                    local_egress_service_diag,
                    active_flow_tun_rx_drain_until,
                    deferred_ack_drain,
                    mut deferred_ack_drain_sleep,
                    d3_egress_actor_self_wake_armed,
                    mut d3_egress_actor_self_wake_sleep,
                    mut relay_gap_ack_drain_sleep,
                },
        } = turn;

        let event_epoch = event.epoch();

        #[cfg(all(test, feature = "harness"))]
        let (test_event_kind, test_event_epoch) = RelayEventTestKind::from_event(&event);

        let f_event = if let Some(mirror) = socket_ctxs
            .get_mut(&handle)
            .and_then(|ctx| ctx.flow_mirror.as_mut())
        {
            match &event {
                RelayEvent::Data { bytes, .. } => {
                    Some(mirror.begin_event(event_epoch, Some(bytes.len())))
                }
                RelayEvent::AckDrainHint { .. } => Some(mirror.begin_event(event_epoch, None)),
                _ => {
                    mirror.quarantined = true;
                    Some(false)
                }
            }
        } else {
            None
        };
        if f_event == Some(false) {
            return Ok(());
        }

        if f_event == Some(true) && handshake_done_tx.flow_draining() {
            if let Some(mirror) = socket_ctxs
                .get_mut(&handle)
                .and_then(|ctx| ctx.flow_mirror.as_mut())
            {
                if let RelayEvent::Data { bytes, .. } = &event {
                    mirror.discard_data(bytes.len());
                }
                mirror.finish_event();
            }
            return Ok(());
        }

        if !socket_ctxs
            .get(&handle)
            .is_some_and(|context| context.conn_epoch == event_epoch)
        {
            #[cfg(all(test, feature = "harness"))]
            handshake_done_tx.observe_relay_event_test(self.relay_event_test_snapshot(
                handle,
                test_event_kind,
                test_event_epoch,
                true,
                socket_ctxs,
                dirty,
            ));
            return Ok(());
        }

        match event {
            RelayEvent::Data {
                epoch,
                bytes: payload,
                stream_service_window,
                downstream_permit,
            } => {
                if d4_stalled_read_service_enabled(runtime_config) {
                    clear_d4_stalled_read_service(&mut self.stalled_read_service_until, handle);
                }
                let epoch = *epoch;
                let mut stream_service_window = *stream_service_window;
                trace_log!(
                    "📬 从大邮筒收到 {} 字节数据，准备送往房间 {:?}",
                    payload.len(),
                    handle
                );
                let pre_payload_tun_rx_budget = if payload.is_empty() {
                    0
                } else if let Some(ctx) = socket_ctxs.get(&handle) {
                    let snapshot = {
                        let socket = sockets.get::<TcpSocket>(handle);
                        SocketCloseSnapshot::from_socket(socket)
                    };
                    tun_rx_drain_budget_before_remote_payload(
                        Some(ctx),
                        snapshot,
                        epoch,
                        payload.len(),
                        tun_rx_drain_budget,
                        downlink_backpressure,
                        runtime_config.tun_mtu,
                        capacity_backpressure,
                    )
                } else {
                    0
                };
                if pre_payload_tun_rx_budget > 0 {
                    drain_ready_tun_rx(
                        device,
                        assoc_table,
                        fake_pool,
                        upstream,
                        udp_clock.elapsed().as_secs(),
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
                        runtime_config.tun_mtu,
                        downlink_egress_drop_debt,
                        tcp_loop_flush_tx_calls,
                        tcp_loop_flush_tx_failures,
                        tun_rx_drain_diag,
                        pre_payload_tun_rx_budget,
                        TUN_RX_DRAIN_SOURCE_REMOTE_PAYLOAD_PRE,
                        if buffered_downlink.enabled {
                            tun_egress_feedback.is_paused()
                        } else {
                            downlink_rx_paused || tun_egress_feedback.is_paused()
                        },
                        buffered_downlink,
                    )
                    .await?;
                }
                if !buffered_downlink.enabled {
                    publish_projected_relay_read_credit_for_payload(
                        handle,
                        epoch,
                        payload.len(),
                        sockets,
                        socket_ctxs,
                        downlink_backpressure,
                        runtime_config.tun_mtu,
                        downlink_flush_max_bytes,
                        downlink_egress_drop_debt.has_active_drop_debt(),
                        tun_rx_drain_diag.backlog_guard().is_active()
                            || downlink_rx_paused
                            || tun_egress_feedback.is_paused(),
                    );
                }
                let accepted_bytes = match handle_remote_payload_from_loop(
                    handshake_done_tx,
                    handle,
                    epoch,
                    payload,
                    RemotePayloadRequest {
                        sockets,
                        socket_ctxs,
                        iface,
                        device,
                        fake_pool,
                        now_secs: udp_clock.elapsed().as_secs(),
                        downlink_flush_max_bytes,
                        downlink_egress_pacer,
                        downlink_egress_drop_debt,
                        downlink_backpressure,
                        tun_mtu: runtime_config.tun_mtu,
                        buffered_downlink_enabled: buffered_downlink.enabled,
                        downstream_permit,
                        d3_egress_actor: runtime_config.d3_egress_actor,
                        d3_egress_actor_adaptive_credit: runtime_config
                            .d3_egress_actor_adaptive_credit,
                        d3_egress_actor_legacy_credit: runtime_config.d3_egress_actor_legacy_credit,
                        d5_capacity_backpressure: d5_capacity_backpressure_enabled(runtime_config),
                    },
                )
                .await
                {
                    Ok(accepted_bytes) => accepted_bytes,
                    Err(e) => {
                        trace_log!("处理回程数据失败: {e}");
                        0
                    }
                };
                super::ingress_effects::collect(
                    iface,
                    sockets,
                    dirty,
                    handshake_done_tx.flow_mode(),
                );
                stream_service_window.note_local_admission(
                    LocalAdmissionProgress {
                        accepted_bytes,
                        ..LocalAdmissionProgress::default()
                    },
                    if accepted_bytes > 0 {
                        StreamServiceBlockedReason::None
                    } else {
                        StreamServiceBlockedReason::LocalAdmissionNoWork
                    },
                );
                // #1：回程下行可能留在 app pending，也可能已进入 smoltcp tx queue。
                // 两者都需要继续标脏，直到本地 TCP/TUN egress 压力退到 low watermark。
                if remote_payload_left_local_egress_work(socket_ctxs.get(&handle), accepted_bytes) {
                    dirty.insert(handle);
                }
                let current_global_pending =
                    global_pending_for_buffered_credit(buffered_downlink.enabled, || {
                        dirty_downlink_pending_total(dirty, socket_ctxs)
                    });
                publish_relay_read_credit_for_handle(
                    handle,
                    sockets,
                    socket_ctxs,
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
                    current_global_pending,
                );
                let has_downlink_work = should_drain_tun_rx_after_remote_payload(
                    socket_ctxs.get(&handle),
                    accepted_bytes,
                );
                if has_downlink_work && let Some(duration) = tun_rx_active_flow_timer_duration {
                    *active_flow_tun_rx_drain_until = Some(std::time::Instant::now() + duration);
                }
                let tun_rx_budget = if has_downlink_work {
                    let send_queue_bytes = sockets.get::<TcpSocket>(handle).send_queue();
                    tun_rx_drain_budget_after_remote_payload(
                        has_downlink_work,
                        send_queue_bytes,
                        tun_rx_drain_budget,
                        downlink_backpressure,
                        runtime_config.tun_mtu,
                        capacity_backpressure,
                    )
                } else {
                    0
                };
                if tun_rx_budget > 0 {
                    drain_ready_tun_rx(
                        device,
                        assoc_table,
                        fake_pool,
                        upstream,
                        udp_clock.elapsed().as_secs(),
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
                        runtime_config.tun_mtu,
                        downlink_egress_drop_debt,
                        tcp_loop_flush_tx_calls,
                        tcp_loop_flush_tx_failures,
                        tun_rx_drain_diag,
                        tun_rx_budget,
                        TUN_RX_DRAIN_SOURCE_REMOTE_PAYLOAD,
                        if buffered_downlink.enabled {
                            tun_egress_feedback.is_paused()
                        } else {
                            downlink_rx_paused || tun_egress_feedback.is_paused()
                        },
                        buffered_downlink,
                    )
                    .await?;
                }
                if has_downlink_work {
                    let relay_read_hard_pause = tun_rx_drain_diag.backlog_guard().is_active()
                        || if buffered_downlink.enabled {
                            tun_egress_feedback.is_paused()
                        } else {
                            downlink_rx_paused || tun_egress_feedback.is_paused()
                        };
                    let egress_actor =
                        local_egress_actor_enabled(runtime_config, dirty, socket_ctxs);
                    if runtime_config.d3_egress_actor {
                        local_egress_service_diag.note_egress_actor_immediate_wake();
                    }
                    let local_progress = service_local_egress_until(
                        device,
                        assoc_table,
                        fake_pool,
                        upstream,
                        udp_clock.elapsed().as_secs(),
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
                        runtime_config.tun_mtu,
                        downlink_egress_drop_debt,
                        tcp_loop_flush_tx_calls,
                        tcp_loop_flush_tx_failures,
                        tun_rx_drain_diag,
                        local_egress_service_diag,
                        relay_read_hard_pause,
                        tun_egress_feedback.is_paused(),
                        buffered_downlink,
                        local_egress_service_config,
                        egress_actor,
                    )
                    .await?;
                    stream_service_window.note_local_admission(
                        LocalAdmissionProgress {
                            accepted_bytes: local_progress.accepted_bytes,
                            egress_drain_bytes: local_progress.egress_drain_bytes,
                            tun_rx_packets: local_progress.tun_rx_packets,
                            flush_tx_calls: local_progress.flush_tx_calls,
                            flush_tx_failures: local_progress.flush_tx_failures,
                            dirty_passes: local_progress.dirty_passes,
                        },
                        stream_service_blocked_reason_for_local_egress(local_progress.stop_reason),
                    );
                    if should_rearm_d3_egress_actor_self_wake(
                        d3_egress_actor_self_wake_enabled(runtime_config),
                        !dirty.is_empty(),
                        local_progress,
                    ) {
                        local_egress_service_diag.note_egress_actor_self_wake();
                        *d3_egress_actor_self_wake_armed = true;
                        d3_egress_actor_self_wake_sleep
                            .as_mut()
                            .reset(tokio::time::Instant::now());
                    }
                }
                tcp_diag_log!(
                    "{}",
                    format_stream_service_window_diag(handle, epoch, &stream_service_window)
                );
                if has_downlink_work && deferred_ack_drain.arm_after_remote_payload() {
                    deferred_ack_drain_sleep.as_mut().reset(
                        tokio::time::Instant::now()
                            + std::time::Duration::from_millis(DEFERRED_ACK_DRAIN_DELAY_MS),
                    );
                }
            }
            RelayEvent::DataReady { epoch } => {
                let epoch = *epoch;
                if socket_ctxs
                    .get(&handle)
                    .is_some_and(|ctx| d16_data_ready_matches_ctx(ctx, epoch))
                {
                    dirty.insert(handle);
                    local_egress_service_diag.note_egress_actor_immediate_wake();
                    let relay_read_hard_pause = tun_rx_drain_diag.backlog_guard().is_active()
                        || if buffered_downlink.enabled {
                            tun_egress_feedback.is_paused()
                        } else {
                            downlink_rx_paused || tun_egress_feedback.is_paused()
                        };
                    let local_progress = service_local_egress_until(
                        device,
                        assoc_table,
                        fake_pool,
                        upstream,
                        udp_clock.elapsed().as_secs(),
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
                        runtime_config.tun_mtu,
                        downlink_egress_drop_debt,
                        tcp_loop_flush_tx_calls,
                        tcp_loop_flush_tx_failures,
                        tun_rx_drain_diag,
                        local_egress_service_diag,
                        relay_read_hard_pause,
                        tun_egress_feedback.is_paused(),
                        buffered_downlink,
                        local_egress_service_config,
                        true,
                    )
                    .await?;
                    if should_drain_tun_rx_after_remote_payload(
                        socket_ctxs.get(&handle),
                        local_progress.accepted_bytes,
                    ) && let Some(duration) = tun_rx_active_flow_timer_duration
                    {
                        *active_flow_tun_rx_drain_until =
                            Some(std::time::Instant::now() + duration);
                    }
                }
            }
            RelayEvent::AckDrainHint {
                epoch,
                gap_ms,
                remote_to_global_rx_bytes,
            } => {
                let (epoch, gap_ms, remote_to_global_rx_bytes) =
                    (*epoch, *gap_ms, *remote_to_global_rx_bytes);
                let relay_read_hard_pause = tun_rx_drain_diag.backlog_guard().is_active()
                    || if buffered_downlink.enabled {
                        tun_egress_feedback.is_paused()
                    } else {
                        downlink_rx_paused || tun_egress_feedback.is_paused()
                    };
                let cadence_floor = note_ack_cadence_gap_hint_for_handle(
                    handle,
                    epoch,
                    sockets,
                    socket_ctxs,
                    downlink_backpressure,
                    runtime_config.tun_mtu,
                    relay_read_hard_pause,
                )
                .unwrap_or(0);
                if arm_d4_stalled_read_service(
                    d4_stalled_read_service_enabled(runtime_config),
                    &mut self.stalled_read_service_until,
                    handle,
                    std::time::Instant::now(),
                ) {
                    tcp_diag_log!(
                        "🔎 tcp-d4-stalled-read-service handle={:?} epoch={} gap_ms={} remote_to_global_rx_bytes={} hold_ms={}",
                        handle,
                        epoch,
                        gap_ms,
                        remote_to_global_rx_bytes,
                        D4_STALLED_READ_SERVICE_HOLD_MS
                    );
                }
                let current_downlink_stats = downlink_pressure_stats(dirty, socket_ctxs, sockets);
                let current_global_pending = current_downlink_stats.total_pending;
                publish_relay_read_credit_for_handle(
                    handle,
                    sockets,
                    socket_ctxs,
                    downlink_backpressure,
                    runtime_config.tun_mtu,
                    downlink_flush_max_bytes,
                    downlink_egress_drop_debt.has_active_drop_debt(),
                    relay_read_hard_pause,
                    buffered_downlink,
                    current_global_pending,
                );
                let tun_rx_budget = tun_rx_drain_budget_for_relay_gap_hint(
                    socket_ctxs.get(&handle),
                    epoch,
                    current_downlink_stats,
                    tun_egress_feedback.is_paused(),
                    tun_rx_drain_budget,
                    downlink_backpressure,
                    runtime_config.tun_mtu,
                    capacity_backpressure,
                );
                if tun_rx_budget > 0 {
                    tcp_diag_log!(
                        "🔎 tcp-relay-ack-drain-hint handle={:?} epoch={} gap_ms={} remote_to_global_rx_bytes={} budget={} cadence_floor={}",
                        handle,
                        epoch,
                        gap_ms,
                        remote_to_global_rx_bytes,
                        tun_rx_budget,
                        cadence_floor
                    );
                    let drained = drain_ready_tun_rx(
                        device,
                        assoc_table,
                        fake_pool,
                        upstream,
                        udp_clock.elapsed().as_secs(),
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
                        runtime_config.tun_mtu,
                        downlink_egress_drop_debt,
                        tcp_loop_flush_tx_calls,
                        tcp_loop_flush_tx_failures,
                        tun_rx_drain_diag,
                        tun_rx_budget,
                        TUN_RX_DRAIN_SOURCE_RELAY_GAP_HINT,
                        if buffered_downlink.enabled {
                            tun_egress_feedback.is_paused()
                        } else {
                            downlink_rx_paused || tun_egress_feedback.is_paused()
                        },
                        buffered_downlink,
                    )
                    .await?;
                    if self
                        .relay_gap_ack_drain
                        .arm_after_hint_drain(drained, tun_rx_budget)
                    {
                        relay_gap_ack_drain_sleep.as_mut().reset(
                            tokio::time::Instant::now()
                                + std::time::Duration::from_millis(
                                    RELAY_GAP_ACK_DRAIN_FOLLOWUP_DELAY_MS,
                                ),
                        );
                    }
                }
            }
            RelayEvent::Closed(close) => {
                if d4_stalled_read_service_enabled(runtime_config) {
                    clear_d4_stalled_read_service(&mut self.stalled_read_service_until, handle);
                }
                if handle_relay_closed_from_loop(
                    handshake_done_tx,
                    handle,
                    *close,
                    sockets,
                    socket_ctxs,
                    fake_pool,
                    udp_clock.elapsed().as_secs(),
                    downlink_backpressure,
                ) {
                    dirty.insert(handle);
                } else {
                    dirty.remove(&handle);
                }
            }
        }

        if f_event == Some(true) {
            if let Some(ctx) = socket_ctxs.get_mut(&handle) {
                if let Some(mirror) = &mut ctx.flow_mirror {
                    mirror.finish_event();
                    mirror.publish_window(ctx.downlink_pending.len());
                }
            }
        }

        #[cfg(all(test, feature = "harness"))]
        handshake_done_tx.observe_relay_event_test(self.relay_event_test_snapshot(
            handle,
            test_event_kind,
            test_event_epoch,
            false,
            socket_ctxs,
            dirty,
        ));
        Ok(())
    }

    #[cfg(all(test, feature = "harness"))]
    fn relay_event_test_snapshot(
        &self,
        handle: SocketHandle,
        kind: RelayEventTestKind,
        event_epoch: u64,
        rejected: bool,
        socket_ctxs: &HashMap<SocketHandle, SocketCtx>,
        dirty: &HashSet<SocketHandle>,
    ) -> RelayEventTestSnapshot {
        RelayEventTestSnapshot {
            generation: 0,
            handle,
            kind,
            event_epoch,
            rejected,
            context: socket_ctxs
                .get(&handle)
                .map(|context| RelayEventTestContext {
                    epoch: context.conn_epoch,
                    state: context.state,
                    has_uplink_tx: context.uplink_tx.is_some(),
                    pending_downlink_bytes: context.downlink_pending.len(),
                    read_credit: context.downlink_credit_controller,
                }),
            dirty: dirty.contains(&handle),
            d4_deadline: self.stalled_read_service_until.get(&handle).copied(),
            relay_gap_ack_drain: self.relay_gap_ack_drain,
        }
    }
}
