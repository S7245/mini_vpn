//! Physical half of the original Permit queue's capability ledger.
//! Owns no queue/actor/registry of its own: Mirror binds one original roles Arc.
//! DATA borrows identity before take; lifecycle sums and guarded disposal precede
//! reset/publication. Entries are FlowMirror methods; tests: tests/permit.rs.
use super::*;
use crate::tcp_downlink_pump::AsyncLeasedByteFlowQueue;

#[derive(Debug)]
pub(super) struct Binding {
    queue: AsyncLeasedByteFlowQueue,
    mode: DownstreamPermitReleaseMode,
}

impl FlowMirror {
    /// Value-only identity witness: index, physical handle, epoch, generation.
    pub(in crate::client_tun) fn capture_identity(&self) -> (usize, SocketHandle, u64, u64) {
        (
            self.id.index,
            self.id.handle,
            self.id.epoch,
            self.id.generation,
        )
    }

    pub(in crate::client_tun) fn capture_identity_matches(
        &self,
        handle: SocketHandle,
        epoch: u64,
    ) -> bool {
        self.id.handle == handle && self.id.epoch == epoch
    }
    pub(in crate::client_tun) fn admission_cap_matches(
        &self,
        cap: &crate::tcp_downlink_pump::DownstreamBytePermit,
    ) -> bool {
        !self.quarantined
            && self.permit_queue.as_ref().is_some_and(|bound| {
                bound.mode == DownstreamPermitReleaseMode::OnAdmission
                    && cap.release_mode() == bound.mode
                    && cap.queue_identity().matches_queue(&bound.queue)
            })
    }

    pub(super) fn bind_permit_queue(&mut self, roles: &FlowRoles, engine: TcpRelayEngine) -> bool {
        let expected = match engine {
            TcpRelayEngine::PermitPump => Some(DownstreamPermitReleaseMode::OnAdmission),
            TcpRelayEngine::OrderedEgressPermitPump => {
                Some(DownstreamPermitReleaseMode::OnEgressDrain)
            }
            _ => None,
        };
        if self.permit_queue.is_some() || roles.permit_mode() != expected {
            self.quarantined = true;
            return false;
        }
        self.permit_queue = match (roles.permit_queue(), expected) {
            (None, None) => None,
            (Some(queue), Some(mode)) if queue.custodied_release_mode() == Some(mode) => {
                Some(Binding {
                    queue: queue.clone(),
                    mode,
                })
            }
            _ => {
                self.quarantined = true;
                return false;
            }
        };
        true
    }
    pub(in crate::client_tun) fn egress_bound(&self) -> bool {
        if self.d16_bound() {
            return true;
        }
        self.permit_queue
            .as_ref()
            .is_some_and(|b| b.mode == DownstreamPermitReleaseMode::OnEgressDrain)
    }
    pub(in crate::client_tun) fn bound_egress_queue(&self) -> Option<&AsyncLeasedByteFlowQueue> {
        self.permit_queue
            .as_ref()
            .filter(|binding| binding.mode == DownstreamPermitReleaseMode::OnEgressDrain)
            .map(|binding| &binding.queue)
    }
    pub(in crate::client_tun) fn live_dead_socket_binding(
        &self,
        handle: SocketHandle,
        epoch: u64,
    ) -> bool {
        self.id.handle == handle
            && self.id.epoch == epoch
            && !self.quarantined
            && self.receipt.is_none()
    }
    pub(super) fn permit_binding_matches(&self, roles: Option<&FlowRoles>) -> bool {
        match (&self.permit_queue, roles.and_then(FlowRoles::permit_queue)) {
            (None, None) => true,
            (Some(binding), Some(queue)) => {
                binding.queue.same_queue(queue)
                    && queue.custodied_release_mode() == Some(binding.mode)
                    && roles.and_then(FlowRoles::permit_mode) == Some(binding.mode)
            }
            _ => false,
        }
    }
    pub(super) fn validate_ordered_custody(
        ctx: &mut SocketCtx,
        roles: Option<&FlowRoles>,
        published: bool,
    ) -> bool {
        // After physical rearm the mirror still owns the old receipt/queue
        // while actual stock and callbacks retire. The new epoch must stay
        // pristine: live-source validation would bind the old queue into it.
        if let Some(mirror) = ctx.flow_mirror.as_ref().filter(|m| m.receipt.is_some() && !m.d16_bound()) {
            return mirror.matches_epoch(ctx.conn_epoch)
                && mirror.rearm_started && mirror.producer_stopped
                && !mirror.quarantined && !mirror.event_in_flight
                && ctx.d16_downlink_queue.is_none()
                && !ctx.downlink_delay_permit_release_until_egress
                && crate::client_tun::downlink_custody::storage::pristine(ctx)
                && match &mirror.permit_queue {
                    Some(binding) => binding.mode == DownstreamPermitReleaseMode::OnEgressDrain,
                    None => roles.is_none() && !published && mirror.window.is_none() && mirror.published_bytes.is_none(),
                }
                && mirror.permit_physical_zero(roles);
        }
        if !ctx.preflight_downlink_clear() {
            return false;
        }
        let Some(mirror) = &ctx.flow_mirror else {
            return false;
        };
        if mirror.d16_bound() {
            return mirror.d16_binding_matches(ctx, roles);
        }
        if !mirror.permit_binding_matches(roles) {
            return false;
        }
        match &mirror.permit_queue {
            None => {
                roles.is_none()
                    && !published
                    && mirror.window.is_none()
                    && mirror.published_bytes.is_none()
                    && ctx.downlink_pending.is_empty()
                    && ctx.downlink_pending_permits.is_empty()
                    && ctx.downlink_inflight_permits.is_empty()
                    && !ctx.downlink_delay_permit_release_until_egress
            }
            Some(binding) => {
                binding.mode == DownstreamPermitReleaseMode::OnEgressDrain
                    && ctx
                        .downlink_inflight_permits
                        .validate_source(Some(&binding.queue))
            }
        }
    }
    pub(in crate::client_tun) fn begin_ordered_clear(
        &mut self,
        epoch: u64,
        pending: usize,
    ) -> Option<usize> {
        if self.quarantined || self.id.epoch != epoch || !self.egress_bound() {
            return None;
        }
        self.quarantined = true;
        self.abandoned_pending.checked_add(pending)
    }
    pub(in crate::client_tun) fn finish_ordered_clear(
        &mut self,
        abandoned: usize,
        complete: bool,
    ) -> bool {
        if !complete {
            return false;
        }
        self.abandoned_pending = abandoned;
        let transferred = self.window_transferred_bytes();
        if let Some(window) = &mut self.window {
            match transferred {
                Some(transferred) => window.audit_storage(transferred, 0, abandoned),
                None => window.fail("D16 transfer accounting overflow"),
            }
            if window.failed() {
                return false;
            }
        }
        self.quarantined = false;
        true
    }

    pub(in crate::client_tun) fn validate_data_permit(
        &self,
        epoch: u64,
        bytes: usize,
        permit: &Option<DownstreamBytePermit>,
    ) -> bool {
        match (&self.permit_queue, permit) {
            // Preserve the original None-family stale/close classification.
            (None, None) => true,
            (Some(queue), Some(permit)) => {
                !self.quarantined
                    && self.receipt.is_none()
                    && epoch == self.id.epoch
                    && queue.queue.owns_permit(permit, bytes)
            }
            _ => false,
        }
    }

    // Normal draining remains inside the original execution unwind boundary.
    // Fatal selected Some is deliberately retained, even if apparently valid.
    pub(in crate::client_tun) fn discard_data_permit(
        &mut self,
        bytes: usize,
        permit: &mut Option<DownstreamBytePermit>,
    ) -> bool {
        if let Some(permit) = permit {
            self.quarantined = true;
            if permit.release(bytes) != bytes || permit.remaining_bytes() != 0 {
                return false;
            }
            self.quarantined = false;
        }
        self.discard_data(bytes);
        !self.quarantined
    }

    /// D191 seam: DATA that reaches a relay already closed by a terminal or
    /// reset (late DATA) settles the in-flight event as discarded, so every
    /// begun event ends in exactly one disposition (hard and non-hard alike).
    pub(in crate::client_tun) fn discard_late_data(
        ctx: &mut SocketCtx,
        bytes: usize,
        permit: &mut Option<DownstreamBytePermit>,
    ) -> bool {
        match &mut ctx.flow_mirror {
            Some(mirror) if mirror.event_in_flight => mirror.discard_data_permit(bytes, permit),
            _ => true,
        }
    }

    pub(super) fn pending_permits_match(
        &self,
        bytes: usize,
        permits: &crate::client_tun::downlink_custody::PermitStock,
    ) -> bool {
        if let Some(queue) = self.d16_queue() {
            return permits.iter().try_fold(0usize, |total, permit| {
                let n = permit.remaining_bytes();
                queue
                    .d16_owns_permit(permit, n)
                    .then(|| total.checked_add(n))
                    .flatten()
            }) == Some(bytes);
        }
        match &self.permit_queue {
            None => permits.is_empty(),
            Some(queue) => {
                permits.iter().try_fold(0usize, |total, permit| {
                    let n = permit.remaining_bytes();
                    queue
                        .queue
                        .owns_permit(permit, n)
                        .then(|| total.checked_add(n))
                        .flatten()
                }) == Some(bytes)
            }
        }
    }

    pub(super) fn dispose_pending(
        &mut self,
        pending: &mut crate::client_tun::downlink_custody::PendingBytes,
        permits: &mut crate::client_tun::downlink_custody::PermitStock,
    ) -> bool {
        if self.quarantined || !self.pending_permits_match(pending.len(), permits) {
            self.quarantined = true;
            return false;
        }
        let bytes = pending.len();
        let Some(abandoned) = self.abandoned_pending.checked_add(bytes) else {
            self.quarantined = true;
            return false;
        };
        // An actual release/Notify unwind leaves this same owner sticky Q.
        self.quarantined = true;
        let receipt = crate::client_tun::downlink_custody::storage::clear(pending, permits);
        let released = if self.permit_queue.is_some() {
            bytes
        } else {
            0
        };
        if receipt.consumed_bytes != bytes
            || receipt.released_lease_bytes != released
            || !pending.is_empty()
            || !permits.is_empty()
        {
            return false;
        }
        self.abandoned_pending = abandoned;
        self.quarantined = false;
        let transferred = self.window_transferred_bytes();
        if let Some(window) = &mut self.window {
            match transferred {
                Some(transferred) => window.audit_storage(transferred, pending.len(), abandoned),
                None => window.fail("D16 transfer accounting overflow"),
            }
            if window.failed() {
                self.quarantined = true;
                return false;
            }
        }
        true
    }

    pub(super) fn permit_physical_zero(&self, roles: Option<&FlowRoles>) -> bool {
        if self.d16_bound() {
            return roles.is_some_and(FlowRoles::is_d16) && self.d16_terminal_quota_zero();
        }
        self.permit_binding_matches(roles)
            && self
                .permit_queue
                .as_ref()
                .is_none_or(|binding| binding.queue.physical_zero())
    }
}

#[cfg(test)]
mod tests;
