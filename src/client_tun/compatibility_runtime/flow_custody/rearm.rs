//! Original-loop physical half of admission, publication and listener rearm.
//!
//! SocketCtx holds the exact F marker, fake-IP capability and DATA dispositions;
//! transaction owns the sole W/F slot ledger and actual role joins. Fallible
//! physical work precedes its retained receipt. Legacy no-F calls use the same
//! physical sink. Tests: this module's tests and compatibility_f_flow.rs.

use super::super::super::*;
use super::super::OpenDispatch;

/// Identifies the composition edge that reached the no-F gateway. Neither
/// source grants authorization; the gateway alone decides its disposition.
#[derive(Clone, Copy)]
pub(in crate::client_tun) enum RearmGate<'a> {
    #[cfg(test)]
    LegacyFacade,
    OriginalLoop(&'a OpenDispatch),
}

impl<'a> RearmGate<'a> {
    #[cfg(test)]
    pub(in crate::client_tun) fn legacy_facade() -> Self {
        Self::LegacyFacade
    }

    pub(in crate::client_tun) fn original_loop(dispatch: &'a OpenDispatch) -> Self {
        Self::OriginalLoop(dispatch)
    }

    pub(in crate::client_tun) fn terminal_payload(
        self,
        handle: SocketHandle,
        ctx: &mut SocketCtx,
        snapshot: SocketCloseSnapshot,
        bytes: usize,
    ) -> bool {
        let intent = RearmIntent::reasoned_ctx(
            handle,
            ctx,
            "remote_to_local",
            "terminal_remote_payload",
            Some(snapshot),
        );
        let legacy = gate_legacy_rearm(self, intent);
        if !legacy {
            if let Some(mirror) = &mut ctx.flow_mirror {
                mirror.discard_data(bytes);
            }
        }
        legacy
    }

    fn settle_no_f(self) {
        match self {
            #[cfg(test)]
            Self::LegacyFacade => {}
            Self::OriginalLoop(dispatch) => {
                // G0 deliberately does not consult `owner`: `None` is not an
                // authorization path. The borrowed composition edge is retained
                // for the later F-owned disposition without creating state here.
                let _ = dispatch;
            }
        }
    }
}

#[derive(Clone, Copy)]
enum RearmKind {
    BarePhysical,
    Reasoned {
        direction: &'static str,
        reason: &'static str,
        snapshot: Option<SocketCloseSnapshot>,
    },
}

/// Value-only rearm identity captured before a terminal mutation.
#[derive(Clone, Copy)]
pub(in crate::client_tun) struct RearmIntent {
    handle: SocketHandle,
    epoch: u64,
    kind: RearmKind,
    flow: Option<FlowIdentity>,
}

impl RearmIntent {
    fn bind(mut self, ctx: Option<&SocketCtx>) -> Self {
        self.flow = ctx
            .and_then(|ctx| ctx.flow_mirror.as_ref())
            .map(|mirror| mirror.id);
        self
    }
    pub(in crate::client_tun) fn reasoned_ctx(
        handle: SocketHandle,
        ctx: &SocketCtx,
        direction: &'static str,
        reason: &'static str,
        snapshot: Option<SocketCloseSnapshot>,
    ) -> Self {
        Self::reasoned(handle, ctx.conn_epoch, direction, reason, snapshot).bind(Some(ctx))
    }
    pub(in crate::client_tun) fn bare_ctx(handle: SocketHandle, ctx: &SocketCtx) -> Self {
        Self::bare_physical(handle, ctx.conn_epoch).bind(Some(ctx))
    }

    pub(in crate::client_tun) fn bare_physical(handle: SocketHandle, epoch: u64) -> Self {
        Self {
            handle,
            epoch,
            kind: RearmKind::BarePhysical,
            flow: None,
        }
    }

    pub(in crate::client_tun) fn reasoned(
        handle: SocketHandle,
        epoch: u64,
        direction: &'static str,
        reason: &'static str,
        snapshot: Option<SocketCloseSnapshot>,
    ) -> Self {
        Self {
            handle,
            epoch,
            flow: None,
            kind: RearmKind::Reasoned {
                direction,
                reason,
                snapshot,
            },
        }
    }
}

/// Concrete event-loop resources needed only after the gateway has accepted a
/// legacy disposition. It has no constructor outside this private crate scope.
struct RearmRequest<'socket, 'storage, 'ctx, 'pool> {
    intent: RearmIntent,
    socket: &'socket mut TcpSocket<'storage>,
    ctx: &'ctx mut SocketCtx,
    fake_pool: &'pool mut FakeIpPool,
    now_secs: u64,
}

impl<'socket, 'storage, 'ctx, 'pool> RearmRequest<'socket, 'storage, 'ctx, 'pool> {
    fn new(
        intent: RearmIntent,
        socket: &'socket mut TcpSocket<'storage>,
        ctx: &'ctx mut SocketCtx,
        fake_pool: &'pool mut FakeIpPool,
        now_secs: u64,
    ) -> Self {
        Self {
            intent: intent.bind(Some(ctx)),
            socket,
            ctx,
            fake_pool,
            now_secs,
        }
    }
}

/// A no-F gateway can either authorize an existing legacy lifecycle branch to
/// continue, or execute its final physical listener reset.
enum RearmDisposition<'socket, 'storage, 'ctx, 'pool> {
    Gate(RearmIntent),
    Execute(RearmRequest<'socket, 'storage, 'ctx, 'pool>),
}

/// Unconstructible, non-copy authorization for the sole physical sink.
struct LegacyRearmAuthorization(());

/// The only G0 authorization gateway. It has no await and no persistent state.
fn request_close_or_rearm(gate: RearmGate<'_>, mut disposition: RearmDisposition<'_, '_, '_, '_>) {
    let intent = match &disposition {
        RearmDisposition::Gate(intent) => *intent,
        RearmDisposition::Execute(request) => request.intent,
    };
    if let RearmGate::OriginalLoop(dispatch) = gate {
        if dispatch.request_flow_close(intent.handle, intent.epoch, intent.flow) {
            return;
        }
    }
    let flow_mode = matches!(gate, RearmGate::OriginalLoop(dispatch) if dispatch.flow_mode());
    let deadline = match gate {
        RearmGate::OriginalLoop(dispatch) if dispatch.flow_draining() => dispatch
            .owner
            .as_ref()
            .and_then(|owner| owner.ledger().shutdown_deadline),
        _ => None,
    };
    if let RearmDisposition::Execute(request) = &mut disposition {
        if (flow_mode || request.ctx.local_reset.is_some())
            && !LocalReset::prepare(request.ctx, request.socket, deadline)
        {
            return;
        }
    }
    gate.settle_no_f();
    match disposition {
        RearmDisposition::Gate(intent) => {
            let _ = (intent.handle, intent.epoch, intent.kind);
        }
        RearmDisposition::Execute(request) => {
            let authorization = LegacyRearmAuthorization(());
            execute_legacy_physical_rearm(request, authorization);
        }
    }
}

fn execute_legacy_physical_rearm(
    request: RearmRequest<'_, '_, '_, '_>,
    _authorization: LegacyRearmAuthorization,
) {
    let RearmRequest {
        intent,
        socket,
        ctx,
        fake_pool,
        now_secs,
    } = request;
    debug_assert_eq!(intent.epoch, ctx.conn_epoch);
    match intent.kind {
        RearmKind::BarePhysical => physical_rearm(socket, ctx, fake_pool, now_secs),
        RearmKind::Reasoned {
            direction,
            reason,
            snapshot,
        } => {
            let snapshot = snapshot.unwrap_or_else(|| SocketCloseSnapshot::from_socket(socket));
            log_tcp_lifecycle_observation(intent.handle, ctx, snapshot, now_secs, reason);
            let close_pending = close_pending_accounting(ctx, snapshot);
            let close_egress = close_egress_accounting(snapshot);
            if let Some((direction, reason)) =
                d16_terminal_cause_for_rearm(ctx, snapshot, direction, reason)
            {
                let _ = drop_d16_owned_for_terminal(ctx, direction, reason);
            }
            tcp_diag_log!(
                "🔎 tcp-handle-close handle={:?} direction={} reason={} state={:?} pending={} pending_high={} remote_to_global_rx_bytes={} terminal_late_remote_payload_bytes={} terminal_late_remote_payload_events={} flush_attempts={} no_send_capacity={} send_window_samples={} send_capacity_min={} send_capacity_max={} send_queue_max={} recv_queue_max={} may_send_false={} may_recv_false={} no_send_capacity_streak_max={} no_send_capacity_pending_max={} send_slice_calls={} send_slice_accepted={} actor_admitted_bytes={} actor_bypass_admitted_bytes={} permit_terminal_drop_bytes={} permit_terminal_drop_events={} send_slice_zero={} send_slice_errors={} budget_limited_calls={} headroom_limited_calls={} headroom_deferred_bytes={} drain_credit_granted_bytes={} drain_credit_planned_bytes={} drain_credit_used_bytes={} drop_credit_debt_bytes={} drop_credit_debt_paid_bytes={} drop_credit_blocked_bytes={} pressure_credit_debt_bytes={} pressure_credit_debt_paid_bytes={} pressure_credit_blocked_bytes={} hard_edge_guard_bytes={} hard_edge_guard_limited_calls={} hard_edge_guard_deferred_bytes={} send_slice_max_accepted={} tun_flush_tx_calls={} tun_flush_tx_failures={} tun_flush_deferred={} close_pending_class={} close_pending_bytes={} terminal_pending_reap_bytes={} close_egress_class={} close_egress_bytes={} close_egress_drain_candidate={} tcp_state={:?} active={} can_send={} can_recv={} may_send={} may_recv={} send_capacity={} send_queue={} recv_queue={}",
                intent.handle,
                direction,
                reason,
                ctx.state,
                close_pending.pending_bytes,
                ctx.downlink_diag.downlink_pending_high_water,
                ctx.downlink_diag.remote_to_global_rx_bytes,
                ctx.downlink_diag.terminal_late_remote_payload_bytes,
                ctx.downlink_diag.terminal_late_remote_payload_events,
                ctx.downlink_diag.flush_attempts,
                ctx.downlink_diag.send_slice_no_capacity,
                ctx.downlink_diag.send_window_samples,
                ctx.downlink_diag.send_capacity_min,
                ctx.downlink_diag.send_capacity_max,
                ctx.downlink_diag.send_queue_max,
                ctx.downlink_diag.recv_queue_max,
                ctx.downlink_diag.may_send_false,
                ctx.downlink_diag.may_recv_false,
                ctx.downlink_diag.no_send_capacity_streak_max,
                ctx.downlink_diag.no_send_capacity_pending_max,
                ctx.downlink_diag.send_slice_calls,
                ctx.downlink_diag.send_slice_accepted_bytes,
                ctx.downlink_diag.actor_admitted_bytes,
                ctx.downlink_diag.actor_bypass_admitted_bytes,
                ctx.downlink_diag.permit_terminal_drop_bytes,
                ctx.downlink_diag.permit_terminal_drop_events,
                ctx.downlink_diag.send_slice_zero,
                ctx.downlink_diag.send_slice_errors,
                ctx.downlink_diag.send_slice_budget_limited,
                ctx.downlink_diag.send_slice_headroom_limited,
                ctx.downlink_diag.send_slice_headroom_deferred_bytes,
                ctx.downlink_diag.send_slice_drain_credit_granted_bytes,
                ctx.downlink_diag.send_slice_drain_credit_planned_bytes,
                ctx.downlink_diag.send_slice_drain_credit_used_bytes,
                ctx.downlink_diag.send_slice_drop_credit_debt_bytes,
                ctx.downlink_diag.send_slice_drop_credit_debt_paid_bytes,
                ctx.downlink_diag.send_slice_drop_credit_blocked_bytes,
                ctx.downlink_diag.send_slice_pressure_credit_debt_bytes,
                ctx.downlink_diag.send_slice_pressure_credit_debt_paid_bytes,
                ctx.downlink_diag.send_slice_pressure_credit_blocked_bytes,
                ctx.downlink_diag.send_slice_hard_edge_guard_bytes,
                ctx.downlink_diag.send_slice_hard_edge_guard_limited,
                ctx.downlink_diag.send_slice_hard_edge_guard_deferred_bytes,
                ctx.downlink_diag.send_slice_max_accepted_bytes,
                ctx.downlink_diag.tun_flush_tx_calls,
                ctx.downlink_diag.tun_flush_tx_failures,
                ctx.downlink_diag.tun_flush_deferred,
                close_pending.class.as_str(),
                close_pending.pending_bytes,
                close_pending.terminal_pending_reap_bytes,
                close_egress.class.as_str(),
                close_egress.bytes,
                close_egress.drain_candidate,
                snapshot.tcp_state,
                snapshot.active,
                snapshot.can_send,
                snapshot.can_recv,
                snapshot.may_send,
                snapshot.may_recv,
                snapshot.send_capacity,
                snapshot.send_queue,
                snapshot.recv_queue
            );
            physical_rearm(socket, ctx, fake_pool, now_secs);
        }
    }
}

/// Reset a slot back into listening after the current legacy relay ends.
fn physical_rearm(
    socket: &mut TcpSocket<'_>,
    ctx: &mut SocketCtx,
    fake_pool: &mut FakeIpPool,
    now_secs: u64,
) {
    ctx.state = SocketState::Closing;
    socket.abort();
    ctx.uplink_tx = None;
    ctx.resumable_action_bridge = None;
    if let Some(sink) = ctx.resumable_sink.take() {
        sink.abandon();
    }
    if let Some(half_close) = ctx.resumable_half_close.take() {
        half_close.fail();
    }
    let had_reducer_terminal = ctx.resumable_terminal.take().is_some();
    if let Some(uplink) = ctx.resumable_uplink.take()
        && !had_reducer_terminal
    {
        match uplink.try_send_reset(ResetReason::LocalAbandon) {
            Ok(()) | Err(FlowPortError::TerminalLaneFull) => {}
            Err(FlowPortError::Closed) => {}
            Err(error) => {
                tcp_diag_log!("🔎 owned-uplink-reset-error error={error}");
            }
        }
    }
    ctx.pending_open_mode = None;
    ctx.relay_read_credit_tx = None;
    ctx.d16_downlink_queue = None;
    ctx.d16_egress_phase = EgressPhase::Running;
    ctx.d16_tun_rx_ack_barrier = false;
    ctx.local_fin_sent = false;
    ctx.local_eof_sent = false;
    ctx.owned_terminal_pending = false;
    ctx.owned_terminal_wait_since_secs = None;
    ctx.local_fin_pending_since_secs = None;
    ctx.local_fin_last_remote_progress_secs = None;
    ctx.clear_downlink_pending();
    ctx.pending_relay_close = None;
    ctx.pending_relay_close_since_secs = None;
    ctx.pending_relay_close_last_progress_secs = None;
    ctx.pending_relay_close_last_pending_bytes = 0;
    ctx.pending_relay_close_last_egress_progress_secs = None;
    ctx.pending_relay_close_last_egress_queue_bytes = 0;
    ctx.downlink_pending_last_progress_secs = None;
    ctx.downlink_pending_last_pending_bytes = 0;
    ctx.downlink_diag = TcpDownlinkDiag::default();
    ctx.downlink_egress_clock = DownlinkEgressClock::default();
    ctx.downlink_credit_controller = DownlinkCreditController::default();
    ctx.downlink_d3_actor_flow = false;
    ctx.downlink_d3_actor_adaptive_credit = false;
    ctx.downlink_d3_actor_legacy_credit = false;
    ctx.downlink_d5_capacity_backpressure = false;
    ctx.last_tcp_lifecycle = None;
    ctx.reverse_window_last_log_secs = None;
    ctx.uplink_buffer.clear();
    ctx.conn_epoch = ctx.conn_epoch.wrapping_add(1);
    if let Some(ip) = ctx.fake_ip.take() {
        fake_pool.release(ip, now_secs);
    }
    ctx.state = SocketState::Rearming;
    configure_local_tcp_socket(socket);
    socket.listen(ctx.local_port).unwrap();
    ctx.local_reset = None;
    ctx.state = SocketState::Listening;
    trace_log!("♻️ handle slot rearmed on local port {}", ctx.local_port);
}

// Value-only reaper planning stays with the gateway so a reaping disposition
// crosses the no-F boundary before it applies legacy lifecycle bookkeeping.
#[cfg(test)]
pub(in crate::client_tun) fn reap_dead_slots(
    registry: &ListenerRegistry,
    sockets: &mut SocketSet<'_>,
    socket_ctxs: &mut HashMap<SocketHandle, SocketCtx>,
    fake_pool: &mut FakeIpPool,
    now_secs: u64,
) -> usize {
    reap_dead_slots_with_gate(
        RearmGate::legacy_facade(),
        registry,
        sockets,
        socket_ctxs,
        fake_pool,
        now_secs,
    )
}

pub(in crate::client_tun) fn reap_dead_slots_from_loop(
    dispatch: &OpenDispatch,
    registry: &ListenerRegistry,
    sockets: &mut SocketSet<'_>,
    socket_ctxs: &mut HashMap<SocketHandle, SocketCtx>,
    fake_pool: &mut FakeIpPool,
    now_secs: u64,
) -> usize {
    reap_dead_slots_with_gate(
        RearmGate::original_loop(dispatch),
        registry,
        sockets,
        socket_ctxs,
        fake_pool,
        now_secs,
    )
}

fn reap_dead_slots_with_gate(
    gate: RearmGate<'_>,
    registry: &ListenerRegistry,
    sockets: &mut SocketSet<'_>,
    socket_ctxs: &mut HashMap<SocketHandle, SocketCtx>,
    fake_pool: &mut FakeIpPool,
    now_secs: u64,
) -> usize {
    let handles: Vec<SocketHandle> = registry.all_handles().collect();
    let mut reaped = 0;
    for handle in handles {
        if let Some(ctx) = socket_ctxs
            .get_mut(&handle)
            .filter(|ctx| ctx.local_reset.is_some())
        {
            let intent = RearmIntent::bare_ctx(handle, ctx);
            request_close_or_rearm(
                gate,
                RearmDisposition::Execute(RearmRequest::new(
                    intent,
                    sockets.get_mut::<TcpSocket>(handle),
                    ctx,
                    fake_pool,
                    now_secs,
                )),
            );
            continue;
        }
        let snapshot = SocketCloseSnapshot::from_socket(sockets.get::<TcpSocket>(handle));
        let Some((epoch, plan)) = socket_ctxs
            .get(&handle)
            .map(|ctx| (ctx.conn_epoch, ReapPlan::inspect(ctx, snapshot, now_secs)))
        else {
            continue;
        };
        let intent =
            RearmIntent::reasoned(handle, epoch, "local", "dead_slot_reap", Some(snapshot));
        let intent = intent.bind(socket_ctxs.get(&handle));
        if plan.should_reap && !gate_legacy_rearm(gate, intent) {
            continue;
        }
        let dead = if let Some(ctx) = socket_ctxs.get_mut(&handle) {
            plan.apply(ctx);
            let source = if plan.should_reap {
                "dead_slot_reap"
            } else {
                "dead_slot_scan"
            };
            log_tcp_lifecycle_observation(handle, ctx, snapshot, now_secs, source);
            plan.should_reap
        } else {
            false
        };
        if dead {
            let socket = sockets.get_mut::<TcpSocket>(handle);
            if let Some(ctx) = socket_ctxs.get_mut(&handle) {
                request_close_or_rearm(
                    gate,
                    RearmDisposition::Execute(RearmRequest::new(
                        intent, socket, ctx, fake_pool, now_secs,
                    )),
                );
                reaped += 1;
            }
        }
    }
    reaped
}

#[derive(Clone, Copy)]
enum ReapEgressWrite {
    Keep,
    Set {
        last_progress_secs: Option<u64>,
        last_queue_bytes: usize,
    },
}

#[derive(Clone, Copy)]
struct ReapPlan {
    should_reap: bool,
    wait_write: Option<u64>,
    egress_write: ReapEgressWrite,
}

impl ReapPlan {
    fn inspect(ctx: &SocketCtx, snapshot: SocketCloseSnapshot, now_secs: u64) -> Self {
        let exact_graceful_terminal_ready = matches!(
            ctx.resumable_terminal,
            Some(TerminalAction::FlowFinished(FlowFinishReason::Graceful))
        );
        if exact_graceful_terminal_ready {
            let started = ctx.owned_terminal_wait_since_secs.unwrap_or(now_secs);
            let should_reap = !snapshot.active
                || now_secs.saturating_sub(started) >= OWNED_TERMINAL_WAIT_GRACE_SECS;
            return Self {
                should_reap,
                wait_write: if should_reap { None } else { Some(started) },
                egress_write: ReapEgressWrite::Keep,
            };
        }
        if ctx.owned_terminal_pending {
            let started = ctx.owned_terminal_wait_since_secs.unwrap_or(now_secs);
            let should_reap = now_secs.saturating_sub(started) >= OWNED_TERMINAL_WAIT_GRACE_SECS;
            return Self {
                should_reap,
                wait_write: if should_reap { None } else { Some(started) },
                egress_write: ReapEgressWrite::Keep,
            };
        }

        let (defer, wait_write) = plan_owned_terminal_wait(
            ctx.awaits_owned_graceful_terminal(),
            snapshot.active,
            ctx.owned_terminal_wait_since_secs,
            now_secs,
        );
        if defer {
            return Self {
                should_reap: false,
                wait_write,
                egress_write: ReapEgressWrite::Keep,
            };
        }
        if ctx.pending_relay_close.is_some() && snapshot.active && snapshot.can_send {
            return Self {
                should_reap: false,
                wait_write,
                egress_write: plan_deferred_close_egress_progress(
                    ctx.pending_relay_close_last_egress_progress_secs,
                    ctx.pending_relay_close_last_egress_queue_bytes,
                    snapshot.send_queue,
                    now_secs,
                ),
            };
        }

        Self {
            should_reap: should_reap_slot(
                ctx,
                snapshot.tcp_state,
                snapshot.active,
                snapshot.can_send,
                now_secs,
            ),
            wait_write,
            egress_write: ReapEgressWrite::Keep,
        }
    }

    fn apply(self, ctx: &mut SocketCtx) {
        ctx.owned_terminal_wait_since_secs = self.wait_write;
        match self.egress_write {
            ReapEgressWrite::Keep => {}
            ReapEgressWrite::Set {
                last_progress_secs,
                last_queue_bytes,
            } => {
                ctx.pending_relay_close_last_egress_progress_secs = last_progress_secs;
                ctx.pending_relay_close_last_egress_queue_bytes = last_queue_bytes;
            }
        }
    }
}

fn plan_owned_terminal_wait(
    awaits_terminal: bool,
    socket_active: bool,
    current: Option<u64>,
    now_secs: u64,
) -> (bool, Option<u64>) {
    if awaits_terminal && !socket_active {
        let started = current.unwrap_or(now_secs);
        return (
            now_secs.saturating_sub(started) < OWNED_TERMINAL_WAIT_GRACE_SECS,
            Some(started),
        );
    }
    (false, None)
}

fn plan_deferred_close_egress_progress(
    last_progress_secs: Option<u64>,
    last_queue_bytes: usize,
    observed_queue_bytes: usize,
    now_secs: u64,
) -> ReapEgressWrite {
    if observed_queue_bytes == 0 {
        ReapEgressWrite::Set {
            last_progress_secs: None,
            last_queue_bytes: 0,
        }
    } else if last_queue_bytes == 0 || observed_queue_bytes < last_queue_bytes {
        ReapEgressWrite::Set {
            last_progress_secs: Some(now_secs),
            last_queue_bytes: observed_queue_bytes,
        }
    } else if observed_queue_bytes > last_queue_bytes {
        ReapEgressWrite::Set {
            last_progress_secs,
            last_queue_bytes: observed_queue_bytes,
        }
    } else {
        ReapEgressWrite::Keep
    }
}

#[cfg(test)]
pub(in crate::client_tun) fn should_reap_slot_with_snapshot(
    ctx: &mut SocketCtx,
    snapshot: SocketCloseSnapshot,
    now_secs: u64,
) -> bool {
    let plan = ReapPlan::inspect(ctx, snapshot, now_secs);
    let should_reap = plan.should_reap;
    plan.apply(ctx);
    should_reap
}

#[cfg(test)]
pub(in crate::client_tun) fn should_defer_owned_terminal_reap(
    awaits_terminal: bool,
    socket_active: bool,
    wait_started_secs: &mut Option<u64>,
    now_secs: u64,
) -> bool {
    let (should_defer, wait_write) =
        plan_owned_terminal_wait(awaits_terminal, socket_active, *wait_started_secs, now_secs);
    *wait_started_secs = wait_write;
    should_defer
}

// Gateway-only adapters keep concrete socket borrows outside the authorization
// decision while preserving the original loop's borrowed composition edge.
/// The original loop reaches the same gateway through its borrowed dispatch.
pub(in crate::client_tun) fn legacy_reasoned_rearm_from_loop(
    dispatch: &OpenDispatch,
    handle: SocketHandle,
    socket: &mut TcpSocket<'_>,
    ctx: &mut SocketCtx,
    fake_pool: &mut FakeIpPool,
    now_secs: u64,
    close_direction: &'static str,
    close_reason: &'static str,
) {
    legacy_reasoned_rearm_with_gate(
        RearmGate::original_loop(dispatch),
        handle,
        socket,
        ctx,
        fake_pool,
        now_secs,
        close_direction,
        close_reason,
    );
}

pub(in crate::client_tun) fn legacy_reasoned_rearm_with_gate(
    gate: RearmGate<'_>,
    handle: SocketHandle,
    socket: &mut TcpSocket<'_>,
    ctx: &mut SocketCtx,
    fake_pool: &mut FakeIpPool,
    now_secs: u64,
    close_direction: &'static str,
    close_reason: &'static str,
) {
    legacy_reasoned_rearm_with_snapshot_and_gate(
        gate,
        handle,
        socket,
        ctx,
        fake_pool,
        now_secs,
        close_direction,
        close_reason,
        None,
    );
}

pub(in crate::client_tun) fn legacy_reasoned_rearm_with_snapshot_and_gate(
    gate: RearmGate<'_>,
    handle: SocketHandle,
    socket: &mut TcpSocket<'_>,
    ctx: &mut SocketCtx,
    fake_pool: &mut FakeIpPool,
    now_secs: u64,
    close_direction: &'static str,
    close_reason: &'static str,
    close_snapshot: Option<SocketCloseSnapshot>,
) {
    let intent =
        RearmIntent::reasoned_ctx(handle, ctx, close_direction, close_reason, close_snapshot);
    request_close_or_rearm(
        gate,
        RearmDisposition::Execute(RearmRequest::new(intent, socket, ctx, fake_pool, now_secs)),
    );
}

pub(in crate::client_tun) fn legacy_bare_rearm_from_loop(
    dispatch: &OpenDispatch,
    handle: SocketHandle,
    socket: &mut TcpSocket<'_>,
    ctx: &mut SocketCtx,
    fake_pool: &mut FakeIpPool,
    now_secs: u64,
) {
    let intent = RearmIntent::bare_ctx(handle, ctx);
    request_close_or_rearm(
        RearmGate::original_loop(dispatch),
        RearmDisposition::Execute(RearmRequest::new(intent, socket, ctx, fake_pool, now_secs)),
    );
}

pub(in crate::client_tun) fn gate_legacy_rearm(gate: RearmGate<'_>, intent: RearmIntent) -> bool {
    if let RearmGate::OriginalLoop(dispatch) = gate {
        if dispatch.request_flow_close(intent.handle, intent.epoch, intent.flow) {
            return false;
        }
    }
    true
}

// Stack-only argument bundle for the original relay-event ingress. It carries
// borrows for one call and owns no custody state.
pub(in crate::client_tun) struct RemotePayloadRequest<'turn, 'storage, D: TunIo> {
    pub(in crate::client_tun) sockets: &'turn mut SocketSet<'storage>,
    pub(in crate::client_tun) socket_ctxs: &'turn mut HashMap<SocketHandle, SocketCtx>,
    pub(in crate::client_tun) iface: &'turn mut Interface,
    pub(in crate::client_tun) device: &'turn mut D,
    pub(in crate::client_tun) fake_pool: &'turn mut FakeIpPool,
    pub(in crate::client_tun) now_secs: u64,
    pub(in crate::client_tun) downlink_flush_max_bytes: usize,
    pub(in crate::client_tun) downlink_egress_pacer: &'turn mut DownlinkEgressPacer,
    pub(in crate::client_tun) downlink_egress_drop_debt: &'turn mut DownlinkEgressDropDebt,
    pub(in crate::client_tun) downlink_backpressure: DownlinkBackpressureConfig,
    pub(in crate::client_tun) tun_mtu: usize,
    pub(in crate::client_tun) buffered_downlink_enabled: bool,
    pub(in crate::client_tun) downstream_permit: &'turn mut Option<DownstreamBytePermit>,
    pub(in crate::client_tun) d3_egress_actor: bool,
    pub(in crate::client_tun) d3_egress_actor_adaptive_credit: bool,
    pub(in crate::client_tun) d3_egress_actor_legacy_credit: bool,
    pub(in crate::client_tun) d5_capacity_backpressure: bool,
}

/// 回收「已用过但已死/卡住」的 listener 槽（review #1/#2 修复）：本地 FIN/RST 关闭、双向关闭完成、
/// 或开远端失败卡住的槽，热路径的 rearm 只在「远端 EOF / Refuse」触发，覆盖不到这些路径——
/// 不回收则 ① 它持有的 fake-IP refcount 永不归零 → 映射永不被 sweep 回收（泄漏）；
/// ② 槽停在非 Listen，`ensure_spare_listeners` 不断新建 → `total_handles` 涨到 `MAX_TOTAL_LISTENERS`
/// → Capped → #2 修好的热门端口 stall 又回来。低频（1s tick）调用，非每包热路径。
///
/// 死槽判定（仅对「被用过」的槽，即 `ctx.state != Listening`；空闲 Listen 槽 ctx.state==Listening 永不命中）：
/// - `!is_active()`：Closed / TimeWait（RST、双向关闭完成）；**但**若仍有
///   `downlink_pending`，先按最近一次 pending 观察/接受进展给 dirty flush 一个短 grace 窗口，
///   无进展过期才 hard reap。此例外覆盖 Knife14n/14s 看到的 `state=Closing/Relaying pending>0`
///   被 sweep 抢先清尾包，且仍保持 bounded lifecycle。也覆盖 `HandshakePending` 槽
///   在本地已关闭时**（remote-open 在飞但 app 已断）——回收时 rearm bump epoch，迟到的 `HandshakeDone` 被丢；
/// - `CloseWait` 且无 pending downlink、无 live relay：被拦截应用已结束发送半边，且远端 relay
///   也已卸下 → teardown；Resumable HandshakePending 保留到 open 完成后发送 LocalClose。
///   若 relay 仍活着，即使当前 pending 为空，也可能还有 reverse/downlink 字节稍后到达；
///   若仍有 `downlink_pending`，说明远端尾包已被接受但尚未全部写入 smoltcp，必须先让 dirty flush 排空；
/// - `OpeningRemote && uplink_tx.is_none()`：**inline** `open_tcp` 失败后状态卡在 OpeningRemote。
///   中文要点：此第三判据**仅匹配 `OpeningRemote`**（inline 路径），**不**匹配 `HandshakePending`
///   ——后者是 async remote-open 的**正常在飞态**（由上游超时兜底，`HandshakeDone` 必回灌解决），绝不能在飞就回收。
pub(in crate::client_tun) fn should_reap_slot(
    ctx: &SocketCtx,
    tcp_state: TcpState,
    active: bool,
    can_send: bool,
    now_secs: u64,
) -> bool {
    if ctx.state == SocketState::Listening && tcp_state == TcpState::Listen {
        return false;
    }
    if ctx.pending_downlink_bytes() > 0 {
        if active {
            return false;
        }
        if !can_send {
            return true;
        }
        let Some(deadline_base) = ctx
            .pending_relay_close_since_secs
            .max(ctx.pending_relay_close_last_progress_secs)
            .max(ctx.downlink_pending_last_progress_secs)
        else {
            return true;
        };
        return now_secs.saturating_sub(deadline_base) >= DEFERRED_CLOSE_PENDING_GRACE_SECS;
    }
    if ctx.pending_relay_close.is_some() && active && can_send {
        return false;
    }
    if !active {
        return true;
    }
    if tcp_state == TcpState::CloseWait {
        let pending_resumable = ctx.state == SocketState::HandshakePending
            && ctx.pending_open_mode == Some(TcpOwnershipMode::Resumable);
        return !pending_resumable && !ctx.has_live_uplink() && !ctx.local_fin_sent;
    }
    ctx.state == SocketState::OpeningRemote && !ctx.has_live_uplink()
}

#[cfg(test)]
pub(super) mod tests;

use super::transaction::FlowIdentity;

pub(super) mod physical_flow;
pub(super) use physical_flow::FlowPhysicalReceipt;
pub(in crate::client_tun) use physical_flow::{
    FatalPhysicalTurn, FatalStep, FlowAdmission, FlowMirror,
};

mod local_reset;
pub(in crate::client_tun) use local_reset::{LocalReset, flush_tx_and_release_downlink_permits};

mod shutdown;
