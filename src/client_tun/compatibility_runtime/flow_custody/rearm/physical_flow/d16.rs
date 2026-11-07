//! Physical binding for the original D16 queue under F. Owns no second FIFO,
//! budget, permit or actor; it proves the original Root roles and SocketCtx
//! name the same queue and process-wide budget. Entry `FlowMirror` D16 methods;
//! true endpoint tests live in client_tun/tests/compatibility_f_flow.rs.

use super::*;
use crate::tcp_downlink_pump::{AsyncLeasedByteFlowQueue, D16GlobalByteBudget};

#[derive(Debug)]
pub(super) struct Binding {
    queue: AsyncLeasedByteFlowQueue,
    budget: D16GlobalByteBudget,
    backing_meter: bytes::BackingMeter,
    ready_events: usize,
    closed_events: usize,
    pub(super) transferred_bytes: usize,
    pub(super) discarded_queue_bytes: usize,
    pub(super) terminal_preclear_inflight: Option<usize>,
    terminal_queue_disposed: bool,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct Receipt {
    pub(super) read_bytes: usize,
    pub(super) queued_bytes: usize,
    pub(super) transferred_bytes: usize,
    pub(super) discarded_queue_bytes: usize,
    pub(super) ready_events: usize,
    pub(super) closed_events: usize,
}

#[cfg(test)]
impl FlowPhysicalReceipt {
    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn d16_disposition(
        self,
    ) -> Option<(usize, usize, usize)> {
        self.d16.map(|receipt| {
            (
                receipt.queued_bytes,
                receipt.transferred_bytes,
                receipt.discarded_queue_bytes,
            )
        })
    }
}

impl FlowMirror {
    pub(super) fn bind_d16_queue(&mut self, roles: &FlowRoles) -> bool {
        if self.d16_queue.is_some() || self.permit_queue.is_some() || !roles.is_d16() {
            self.quarantined = true;
            return false;
        }
        let (Some(queue), Some(budget), Some(backing_meter)) = (
            roles.d16_queue(),
            roles.d16_budget(),
            roles.d16_backing_meter(),
        ) else {
            self.quarantined = true;
            return false;
        };
        if !queue.d16_uses_budget(budget) {
            self.quarantined = true;
            return false;
        }
        self.d16_queue = Some(Binding {
            queue: queue.clone(),
            budget: budget.clone(),
            backing_meter: backing_meter.clone(),
            ready_events: 0,
            closed_events: 0,
            transferred_bytes: 0,
            discarded_queue_bytes: 0,
            terminal_preclear_inflight: None,
            terminal_queue_disposed: false,
        });
        true
    }

    pub(in crate::client_tun) fn begin_d16_event(&mut self, epoch: u64, ready: bool) -> bool {
        if self.quarantined
            || self.event_in_flight
            || self.receipt.is_some()
            || self.id.epoch != epoch
        {
            self.quarantined = true;
            return false;
        }
        let Some(binding) = &mut self.d16_queue else {
            self.quarantined = true;
            return false;
        };
        let count = if ready {
            &mut binding.ready_events
        } else {
            &mut binding.closed_events
        };
        let Some(next) = count.checked_add(1) else {
            self.quarantined = true;
            return false;
        };
        *count = next;
        self.event_in_flight = true;
        true
    }

    pub(super) fn d16_event_counts(&self) -> Option<(usize, usize)> {
        self.d16_queue
            .as_ref()
            .map(|b| (b.ready_events, b.closed_events))
    }

    pub(in crate::client_tun) fn note_d16_queue_to_pending(&mut self, bytes: usize) {
        if self.quarantined || self.receipt.is_some() || bytes == 0 {
            self.quarantined = true;
            return;
        }
        let Some(binding) = &mut self.d16_queue else {
            self.quarantined = true;
            return;
        };
        match binding.transferred_bytes.checked_add(bytes) {
            Some(next) => binding.transferred_bytes = next,
            None => self.quarantined = true,
        }
    }

    pub(super) fn window_transferred_bytes(&self) -> Option<usize> {
        self.transferred
            .1
            .checked_add(self.d16_queue.as_ref().map_or(0, |b| b.transferred_bytes))
    }

    pub(in crate::client_tun) fn d16_bound(&self) -> bool {
        self.d16_queue.is_some()
    }

    pub(in crate::client_tun) fn d16_queue(&self) -> Option<&AsyncLeasedByteFlowQueue> {
        self.d16_queue.as_ref().map(|binding| &binding.queue)
    }

    pub(in crate::client_tun) fn d16_binding_matches(
        &self,
        ctx: &SocketCtx,
        roles: Option<&FlowRoles>,
    ) -> bool {
        match (&self.d16_queue, roles) {
            (None, None) => ctx.d16_downlink_queue.is_none(),
            (Some(binding), Some(roles)) => {
                roles.is_d16()
                    && roles
                        .d16_queue()
                        .is_some_and(|queue| queue.same_queue(&binding.queue))
                    && roles
                        .d16_budget()
                        .is_some_and(|budget| binding.queue.d16_uses_budget(budget))
                    && binding.queue.d16_uses_budget(&binding.budget)
                    && roles
                        .d16_backing_meter()
                        .is_some_and(|meter| meter.same_authority(&binding.backing_meter))
                    && ctx
                        .d16_downlink_queue
                        .as_ref()
                        .is_some_and(|queue| queue.same_queue(&binding.queue))
            }
            _ => false,
        }
    }

    pub(in crate::client_tun) fn can_preclear_d16_terminal(
        &self,
        ctx: &SocketCtx,
        roles: &FlowRoles,
    ) -> bool {
        self.d16_binding_matches(ctx, Some(roles))
            && self.d16_queue.as_ref().is_some_and(|binding| {
                binding.terminal_preclear_inflight.is_none() && !binding.terminal_queue_disposed
            })
            && !self.quarantined
            && !self.event_in_flight
            && self.pending_permits_match(ctx.downlink_pending.len(), &ctx.downlink_pending_permits)
    }

    pub(in crate::client_tun) fn note_d16_terminal_preclear(&mut self, inflight: usize) -> bool {
        let Some(binding) = &mut self.d16_queue else {
            self.quarantined = true;
            return false;
        };
        if binding
            .terminal_preclear_inflight
            .replace(inflight)
            .is_some()
        {
            self.quarantined = true;
            return false;
        }
        true
    }

    pub(in crate::client_tun) fn d16_terminal_preclear_inflight(&self) -> Option<usize> {
        self.d16_queue
            .as_ref()
            .and_then(|binding| binding.terminal_preclear_inflight)
    }

    pub(super) fn discard_d16_queue_after_joins(
        &mut self,
        roles: &FlowRoles,
        reason: RelayCloseReason,
    ) -> bool {
        let Some(binding) = &mut self.d16_queue else {
            return false;
        };
        if binding.terminal_preclear_inflight.is_none() {
            return true;
        }
        if binding.terminal_queue_disposed {
            return true;
        }
        let before = binding.queue.snapshot_now();
        if roles.join_count() != 3
            || !roles
                .d16_queue()
                .is_some_and(|queue| queue.same_queue(&binding.queue))
            || before.leased_bytes != 0
            || before.reserved_bytes != 0
        {
            self.quarantined = true;
            return false;
        }
        let Some(total) = binding
            .discarded_queue_bytes
            .checked_add(before.queued_bytes)
        else {
            self.quarantined = true;
            return false;
        };
        let dropped = binding
            .queue
            .close_and_drop_queued_for_terminal(reason.0, reason.1);
        if dropped != before.queued_bytes || binding.queue.snapshot_now().queued_bytes != 0 {
            self.quarantined = true;
            return false;
        }
        binding.discarded_queue_bytes = total;
        binding.terminal_queue_disposed = true;
        true
    }

    pub(super) fn d16_terminal_quota_zero(&self) -> bool {
        self.d16_queue
            .as_ref()
            .is_some_and(|binding| binding.queue.d16_terminal_quota_zero(&binding.budget))
    }

    pub(super) fn d16_reader_disposition_matches(
        &self,
        roles: &FlowRoles,
        pending_bytes: usize,
        pending_permits_empty: bool,
        inflight_empty: bool,
    ) -> bool {
        let Some(binding) = &self.d16_queue else {
            return false;
        };
        let Some((queued, ready, closed)) = roles.d16_reader_ledger() else {
            return false;
        };
        self.d16_terminal_quota_zero()
            && !self.quarantined
            && !self.event_in_flight
            && pending_bytes == 0
            && pending_permits_empty
            && inflight_empty
            && binding
                .transferred_bytes
                .checked_add(binding.discarded_queue_bytes)
                == Some(queued)
            && roles.read_total() == Some(queued)
            && ready == binding.ready_events
            && closed == binding.closed_events
    }

    pub(super) fn d16_receipt(
        &self,
        ctx_queue: Option<&AsyncLeasedByteFlowQueue>,
        pending_bytes: usize,
        pending_permits_empty: bool,
        inflight_empty: bool,
        roles: &FlowRoles,
    ) -> Option<Receipt> {
        let binding = self.d16_queue.as_ref()?;
        let (queued_bytes, ready_events, closed_events) = roles.d16_reader_ledger()?;
        let read_bytes = roles.read_total()?;
        if !roles.is_d16()
            || !roles
                .d16_queue()
                .is_some_and(|queue| queue.same_queue(&binding.queue))
            || !roles
                .d16_budget()
                .is_some_and(|budget| binding.queue.d16_uses_budget(budget))
            || !ctx_queue.is_some_and(|queue| queue.same_queue(&binding.queue))
            || !self.d16_terminal_quota_zero()
            || self.quarantined
            || self.event_in_flight
            || pending_bytes != 0
            || !pending_permits_empty
            || !inflight_empty
            || read_bytes != queued_bytes
            || binding
                .transferred_bytes
                .checked_add(binding.discarded_queue_bytes)
                != Some(queued_bytes)
            || ready_events != binding.ready_events
            || closed_events != binding.closed_events
        {
            return None;
        }
        Some(Receipt {
            read_bytes,
            queued_bytes,
            transferred_bytes: binding.transferred_bytes,
            discarded_queue_bytes: binding.discarded_queue_bytes,
            ready_events,
            closed_events,
        })
    }
}
