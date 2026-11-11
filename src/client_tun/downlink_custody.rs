//! Original downstream capability transfer and settlement implementation.
//! The existing SocketCtx alone holds pending/inflight permits; this Module
//! owns their admission, actual-flush/observed-drain release and terminal clear.
//! The original capability FIFO and real socket admission share one range ledger.
//! Entries: original SocketCtx methods, flush_downlink and batch release helpers.
//! The private harness model has no socket proof and keeps its old scalar semantics.
//! Tests: client_tun::tests d6_*, local_egress_service::ordered_flush, harness::tests.
use super::*;
mod capture;
pub(super) use capture::*;
mod ledger;
pub(in crate::client_tun) mod storage;
use super::compatibility_runtime::ResetProof;
use ledger::Binding;
pub(super) use ledger::EgressPermitLedger;
#[cfg(all(test, feature = "harness"))]
pub(in crate::client_tun) use ledger::{
    EgressLedgerEvent, EgressLedgerEventKind, EgressLedgerSnapshot, EgressRefundSource,
};
pub(in crate::client_tun) use storage::{PendingBytes, PermitStock};

#[cfg(feature = "harness")]
mod model;
#[cfg(feature = "harness")]
pub(crate) use model::{D16HarnessCycle, D16HarnessFlow, D16HarnessFlowSnapshot};

pub(super) fn drain_d16_owned_queue_into_pending(
    ctx: &mut SocketCtx,
    max_bytes: usize,
) -> D16OwnedQueueDrain {
    let Some(queue) = ctx.d16_downlink_queue.clone() else {
        return D16OwnedQueueDrain::default();
    };
    let max_bytes = max_bytes.min(ctx.downlink_pending.available());
    let mut outcome = D16OwnedQueueDrain::default();
    while outcome.bytes < max_bytes {
        match queue.try_recv_up_to(max_bytes.saturating_sub(outcome.bytes)) {
            LeasedQueuePoll::Data(leased) => {
                let (bytes, permit) = leased.into_parts();
                if !ctx.append_downlink_pending_with_permit(&bytes, &mut Some(permit)) {
                    break;
                }
                outcome.bytes = outcome.bytes.saturating_add(bytes.len());
                if let Some(mirror) = &mut ctx.flow_mirror {
                    if mirror.d16_bound() {
                        mirror.note_d16_queue_to_pending(bytes.len());
                    }
                }
            }
            LeasedQueuePoll::Empty => break,
            LeasedQueuePoll::Closed => {
                outcome.queue_closed = true;
                break;
            }
        }
    }
    if !outcome.queue_closed && matches!(queue.try_recv_up_to(0), LeasedQueuePoll::Closed) {
        outcome.queue_closed = true;
    }
    outcome
}

impl SocketCtx {
    pub(super) fn append_downlink_pending_with_permit(
        &mut self,
        payload: &[u8],
        permit: &mut Option<DownstreamBytePermit>,
    ) -> bool {
        let delayed = permit.as_ref().is_some_and(|permit| {
            permit.release_mode() == DownstreamPermitReleaseMode::OnEgressDrain
        });
        let Some(outcome) = storage::append(
            &mut self.downlink_pending,
            &mut self.downlink_pending_permits,
            payload,
            permit,
        ) else {
            self.downlink_pending.quarantine();
            self.downlink_inflight_permits
                .fail("pending storage admission refused");
            return false;
        };
        self.downlink_delay_permit_release_until_egress |= delayed;
        if outcome.retained_lease_bytes > 0 {
            self.downlink_diag
                .note_downstream_permit(outcome.retained_lease_bytes, outcome.leased_bytes_after);
        }
        true
    }

    fn admit_pending(&mut self, bytes: usize) -> usize {
        let before = self.downlink_pending.len();
        let released = if self.downlink_delay_permit_release_until_egress {
            self.move_downlink_pending_prefix_to_inflight(bytes)
        } else {
            self.release_downlink_permits(bytes)
        };
        if let Some(mirror) = &mut self.flow_mirror {
            mirror.admit_pending(
                before,
                self.downlink_pending.len(),
                bytes,
                self.d16_downlink_queue.is_none()
                    && !self.downlink_delay_permit_release_until_egress,
            );
        }
        released
    }

    fn move_downlink_pending_prefix_to_inflight(&mut self, bytes: usize) -> usize {
        #[cfg(all(test, feature = "harness"))]
        let prior_count = self.downlink_inflight_permits.permits.len();
        #[cfg(all(test, feature = "harness"))]
        let prior_bytes = self.downlink_inflight_permit_bytes();
        let Some(outcome) = storage::transfer(
            &mut self.downlink_pending,
            &mut self.downlink_pending_permits,
            &mut self.downlink_inflight_permits.permits,
            bytes,
        ) else {
            self.downlink_pending.quarantine();
            self.downlink_inflight_permits
                .fail("inflight capability transfer mismatch");
            return 0;
        };
        #[cfg(all(test, feature = "harness"))]
        self.downlink_inflight_permits.record_admitted_caps(
            prior_count,
            prior_bytes,
            outcome.transferred_lease_bytes,
            self.conn_epoch,
        );
        if outcome.transferred_lease_bytes > 0 {
            self.downlink_diag.note_downstream_permit_inflight(
                outcome.transferred_lease_bytes,
                outcome.inflight_leased_bytes_after,
            );
        }
        outcome.transferred_lease_bytes
    }

    fn release_downlink_permits(&mut self, bytes: usize) -> usize {
        let outcome = storage::consume(
            &mut self.downlink_pending,
            &mut self.downlink_pending_permits,
            bytes,
        );
        if outcome.released_lease_bytes > 0 {
            self.downlink_diag.note_downstream_permit_release(
                outcome.released_lease_bytes,
                outcome.leased_bytes_after,
            );
        }
        outcome.released_lease_bytes
    }

    fn release_inflight_prefix(&mut self, mut bytes: usize) -> usize {
        let mut released = 0usize;
        while bytes > 0 {
            let Some(front) = self.downlink_inflight_permits.permits.front_mut() else {
                break;
            };
            let n = front.release(bytes);
            if n == 0 {
                break;
            }
            released = released.saturating_add(n);
            bytes = bytes.saturating_sub(n);
            if front.remaining_bytes() == 0 {
                self.downlink_inflight_permits.permits.pop_front();
            }
        }
        if released > 0 {
            self.downlink_diag
                .note_downstream_permit_release(released, self.downlink_inflight_permit_bytes());
        }
        released
    }

    pub(super) fn downlink_inflight_permit_bytes(&self) -> usize {
        self.downlink_inflight_permits.remaining_bytes()
    }

    #[cfg(test)]
    pub(super) fn consume_downlink_pending_prefix(&mut self, bytes: usize) {
        self.release_downlink_permits(bytes);
    }

    pub(super) fn preflight_downlink_clear(&mut self) -> bool {
        if self.downlink_inflight_permits.quarantined() {
            return false;
        }
        if self.downlink_delay_permit_release_until_egress
            && !self
                .downlink_inflight_permits
                .validate_caps(&self.downlink_pending_permits, self.downlink_pending.len())
        {
            return false;
        }
        if !self
            .downlink_inflight_permits
            .validate_inflight(self.conn_epoch)
        {
            return false;
        }
        if !self
            .downlink_inflight_permits
            .validate_source(self.d16_downlink_queue.as_ref())
        {
            return false;
        }
        true
    }

    fn begin_downlink_clear(&mut self) -> bool {
        if !self.preflight_downlink_clear() {
            return false;
        }
        self.downlink_inflight_permits.busy = true;
        true
    }

    pub(super) fn clear_downlink_pending(&mut self) {
        // A no-Flow context with no physical stock, source or ledger history
        // has nothing to retire. Do not invent a terminal ledger that the next
        // Native Main cold preflight would correctly reject as non-pristine.
        if self.d16_downlink_queue.is_none()
            && self.flow_mirror.is_none()
            && !self.downlink_delay_permit_release_until_egress
            && !storage::any_native(self)
            && storage::pristine(self)
        {
            return;
        }
        if self.begin_downlink_clear() {
            self.finish_downlink_clear();
        }
    }

    pub(super) fn discard_ordered_for_reset(&mut self) -> bool {
        let pending = self.downlink_pending.len();
        let Some(abandoned) = self
            .flow_mirror
            .as_mut()
            .and_then(|mirror| mirror.begin_ordered_clear(self.conn_epoch, pending))
        else {
            return false;
        };
        self.clear_downlink_pending();
        let complete = self.downlink_pending.is_empty()
            && self.downlink_pending_permits.is_empty()
            && self.downlink_inflight_permits.is_empty();
        self.flow_mirror
            .as_mut()
            .is_some_and(|mirror| mirror.finish_ordered_clear(abandoned, complete))
    }

    fn finish_downlink_clear(&mut self) {
        let mut released = 0usize;
        // Keep payload and the front capability in their original custody until
        // the original release (including Notify) actually returns.
        let mut mismatch = false;
        'dispose: for caps in [
            &mut self.downlink_pending_permits,
            &mut self.downlink_inflight_permits.permits,
        ] {
            while let Some(front) = caps.front_mut() {
                let n = front.remaining_bytes();
                if front.release(n) != n || front.remaining_bytes() != 0 {
                    mismatch = true;
                    break 'dispose;
                }
                released += n;
                caps.pop_front();
            }
        }
        if mismatch {
            self.downlink_inflight_permits
                .fail("terminal capability disposal mismatch");
            return;
        }
        self.downlink_pending.clear();
        self.downlink_inflight_permits.clear_committed();
        self.downlink_delay_permit_release_until_egress = false;
        self.downlink_diag
            .note_downstream_permit_release(released, 0);
    }
}

pub(super) fn flush_downlink(
    handle: SocketHandle,
    tcp_socket: &mut TcpSocket,
    ctx: &mut SocketCtx,
    max_bytes_per_flush: usize,
    downlink_backpressure: DownlinkBackpressureConfig,
    tun_mtu: usize,
    downlink_egress_drop_debt: &mut DownlinkEgressDropDebt,
) -> DownlinkFlushOutcome {
    if ctx.downlink_pending.is_empty() {
        return DownlinkFlushOutcome {
            accepted_bytes: 0,
            headroom_limited: false,
        };
    }
    let send_window = TcpSendWindowSnapshot::from_socket(tcp_socket);
    ctx.downlink_diag
        .note_send_window(send_window, ctx.downlink_pending.len());
    if !send_window.can_send {
        ctx.downlink_diag
            .note_flush_attempt(ctx.downlink_pending.len(), max_bytes_per_flush);
        ctx.downlink_diag
            .note_no_send_capacity(ctx.downlink_pending.len());
        return DownlinkFlushOutcome {
            accepted_bytes: 0,
            headroom_limited: false,
        };
    }
    let controlled_max_bytes_per_flush = ctx.downlink_credit_controller.flush_budget(
        max_bytes_per_flush,
        downlink_backpressure,
        tun_mtu,
    );
    let flush_policy = downlink_flush_policy_for_ctx(ctx);
    let mut flush_limit = bounded_downlink_flush_limit_for_window_with_clock_drop_and_policy(
        ctx.downlink_pending.len(),
        controlled_max_bytes_per_flush,
        send_window,
        downlink_backpressure,
        &mut ctx.downlink_egress_clock,
        downlink_egress_drop_debt,
        flush_policy,
    );
    if ctx.pending_relay_close.is_some()
        && !ctx.local_eof_sent
        && flush_policy.allows_close_drain_expansion()
    {
        let expanded = close_drain_terminal_pending_flush_limit(
            ctx.downlink_pending.len(),
            max_bytes_per_flush,
            send_window,
            downlink_backpressure,
            flush_limit,
        );
        if expanded.len > flush_limit.len {
            tcp_diag_log!(
                "🔎 tcp-close-drain-flush-credit handle={:?} pending={} send_queue={} len={} clean_high={} target_high={} credit_high={} pause_high={} target_deferred={}",
                handle,
                ctx.downlink_pending.len(),
                send_window.send_queue,
                expanded.len,
                tx_queue_flush_threshold(downlink_backpressure),
                tx_queue_egress_target_threshold(downlink_backpressure),
                tx_queue_credit_spend_threshold(downlink_backpressure),
                tx_queue_pause_threshold(downlink_backpressure),
                expanded.hard_edge_guard_deferred_bytes
            );
            flush_limit = expanded;
        }
    }
    ctx.downlink_diag.note_flush_limit(
        ctx.downlink_pending.len(),
        controlled_max_bytes_per_flush,
        flush_limit,
    );
    if flush_limit.len == 0 {
        let used =
            ctx.downlink_egress_clock
                .note_flush_result(send_window.send_queue, 0, flush_limit);
        ctx.downlink_diag.note_drain_credit_used(used);
        ctx.downlink_credit_controller
            .note_flush_feedback_with_accepted(flush_limit, 0, downlink_backpressure, tun_mtu);
        return DownlinkFlushOutcome {
            accepted_bytes: 0,
            headroom_limited: flush_limit.headroom_limited,
        };
    }
    let flush_len = flush_limit.len;
    let generic =
        ctx.d16_downlink_queue.is_none() && !ctx.downlink_delay_permit_release_until_egress;
    if let Some(mirror) = &mut ctx.flow_mirror {
        if !mirror.preflight_pending_admission(
            ctx.conn_epoch,
            ctx.downlink_pending.len(),
            flush_len,
            generic,
        ) {
            ctx.downlink_inflight_permits
                .fail("receive-window admission preflight refused");
            return DownlinkFlushOutcome::NO_ADMISSION;
        }
    }
    if ctx.downlink_delay_permit_release_until_egress {
        if !ctx
            .downlink_inflight_permits
            .permits
            .can_accept(ctx.downlink_pending_permits.front())
        {
            ctx.downlink_inflight_permits
                .fail("inflight fixed storage preflight refused");
            return DownlinkFlushOutcome::NO_ADMISSION;
        }
        if !ctx.downlink_inflight_permits.prepare(
            ctx.conn_epoch,
            tcp_socket,
            flush_len,
            ctx.downlink_pending_permits.len(),
        ) || !ctx
            .downlink_inflight_permits
            .validate_caps(&ctx.downlink_pending_permits, ctx.downlink_pending.len())
            || !ctx
                .downlink_inflight_permits
                .validate_source(ctx.d16_downlink_queue.as_ref())
        {
            return DownlinkFlushOutcome::NO_ADMISSION;
        }
    } else {
        if ctx.downlink_inflight_permits.quarantined() {
            return DownlinkFlushOutcome::NO_ADMISSION;
        }
        ctx.downlink_inflight_permits.busy = true;
    }
    match tcp_socket.send_slice(&ctx.downlink_pending[..flush_len]) {
        Ok(0) => {
            ctx.downlink_inflight_permits.busy = false;
            let used =
                ctx.downlink_egress_clock
                    .note_flush_result(send_window.send_queue, 0, flush_limit);
            ctx.downlink_diag.note_drain_credit_used(used);
            ctx.downlink_diag
                .note_send_slice_ok(0, ctx.downlink_pending.len());
            ctx.downlink_credit_controller
                .note_flush_feedback_with_accepted(flush_limit, 0, downlink_backpressure, tun_mtu);
            DownlinkFlushOutcome {
                accepted_bytes: 0,
                headroom_limited: flush_limit.headroom_limited,
            }
        }
        Ok(n) => {
            if ctx.downlink_delay_permit_release_until_egress
                && !ctx
                    .downlink_inflight_permits
                    .accepted(ctx.conn_epoch, tcp_socket, n)
            {
                return DownlinkFlushOutcome {
                    accepted_bytes: n,
                    headroom_limited: flush_limit.headroom_limited,
                };
            }
            let used =
                ctx.downlink_egress_clock
                    .note_flush_result(send_window.send_queue, n, flush_limit);
            ctx.downlink_diag.note_drain_credit_used(used);
            ctx.admit_pending(n);
            ctx.downlink_inflight_permits.busy = false;
            ctx.downlink_diag
                .note_send_slice_ok(n, ctx.downlink_pending.len());
            ctx.downlink_credit_controller
                .note_flush_feedback_with_accepted(flush_limit, n, downlink_backpressure, tun_mtu);
            DownlinkFlushOutcome {
                accepted_bytes: n,
                headroom_limited: flush_limit.headroom_limited,
            }
        }
        Err(_) => {
            ctx.downlink_inflight_permits.busy = false;
            let used =
                ctx.downlink_egress_clock
                    .note_flush_result(send_window.send_queue, 0, flush_limit);
            ctx.downlink_diag.note_drain_credit_used(used);
            tcp_diag_log!(
                "🔎 tcp-send-slice-error handle={:?} pending={} state={:?} → clear",
                handle,
                ctx.downlink_pending.len(),
                ctx.state
            );
            ctx.downlink_diag.note_send_slice_error();
            // socket 不可发（已关闭/复位）：丢弃残留，避免无限堆积。
            ctx.clear_downlink_pending();
            ctx.downlink_diag.note_pending(ctx.downlink_pending.len());
            ctx.downlink_credit_controller
                .note_flush_feedback_with_accepted(flush_limit, 0, downlink_backpressure, tun_mtu);
            DownlinkFlushOutcome {
                accepted_bytes: 0,
                headroom_limited: flush_limit.headroom_limited,
            }
        }
    }
}

#[cfg(test)]
mod tests;
#[cfg(test)]
pub(in crate::client_tun) use tests::OrderedCustodySnapshot;

pub(super) fn drop_d16_owned_for_terminal(
    ctx: &mut SocketCtx,
    direction: &'static str,
    reason: &'static str,
) -> usize {
    let Some(queue) = ctx.d16_downlink_queue.clone() else {
        return 0;
    };
    if !ctx.begin_downlink_clear() {
        return 0;
    }
    let before = queue.snapshot_now();
    let dropped_queued_bytes = queue.close_and_drop_queued_for_terminal(direction, reason);
    ctx.finish_downlink_clear();
    let after = queue.snapshot_now();
    let released_leased_bytes = before.leased_bytes.saturating_sub(after.leased_bytes);
    let dropped = dropped_queued_bytes.saturating_add(released_leased_bytes);
    if dropped > 0 {
        ctx.downlink_diag.permit_terminal_drop_bytes = ctx
            .downlink_diag
            .permit_terminal_drop_bytes
            .saturating_add(dropped as u64);
        ctx.downlink_diag.permit_terminal_drop_events = ctx
            .downlink_diag
            .permit_terminal_drop_events
            .saturating_add(1);
    }
    dropped
}
