//! Original D16 scalar harness projection, isolated from real socket custody.
//! Owns its existing SocketCtx/queue fixture and cycles; no TCP socket or flush proof.
//! Old length-based admission/drain behavior is unchanged and is not N1 acceptance.
//! Entries: D16HarnessFlow and its two snapshots; tests: harness::tests d16_*.
use super::*;
use crate::tcp_downlink_pump::ByteQueueConfigError;

#[cfg(feature = "harness")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct D16HarnessCycle {
    pub admitted_bytes: usize,
    pub drained_bytes: usize,
    pub queue_to_pending_bytes: usize,
    pub phase: EgressPhase,
    pub allow_read: bool,
    pub allow_admission: bool,
}

#[cfg(feature = "harness")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct D16HarnessFlowSnapshot {
    pub queue: crate::tcp_downlink_pump::LeasedByteQueueSnapshot,
    pub pending_bytes: usize,
    pub inflight_bytes: usize,
    pub actor_admitted_bytes: u64,
    pub actor_bypass_admitted_bytes: u64,
    pub committed_tun_bytes: u64,
    pub phase: EgressPhase,
    pub eof_ready: bool,
}

#[cfg(feature = "harness")]
pub(crate) struct D16HarnessFlow {
    ctx: SocketCtx,
    queue: AsyncLeasedByteFlowQueue,
    committed_tun_bytes: u64,
}

#[cfg(feature = "harness")]
impl D16HarnessFlow {
    pub(crate) fn new(global_budget: D16GlobalByteBudget) -> Result<Self, ByteQueueConfigError> {
        let queue = AsyncLeasedByteFlowQueue::new_with_global_budget(
            512 * 1024,
            DownstreamPermitReleaseMode::OnEgressDrain,
            global_budget,
        )?;
        let mut ctx = SocketCtx::new(443);
        ctx.state = SocketState::Relaying;
        ctx.d16_downlink_queue = Some(queue.clone());
        Ok(Self {
            ctx,
            queue,
            committed_tun_bytes: 0,
        })
    }

    pub(crate) fn permissions(&self) -> EgressPermissions {
        EgressPermissions::for_phase(self.ctx.d16_egress_phase)
    }

    pub(crate) async fn enqueue_remote_chunk(
        &self,
        bytes: usize,
    ) -> Result<bool, ByteQueuePushError> {
        let reservation = self
            .queue
            .reserve_read_up_to(bytes)
            .await
            .map_err(|_| ByteQueuePushError::Closed)?;
        if reservation.max_len() != bytes {
            return Err(ByteQueuePushError::Full {
                len: bytes,
                available: reservation.max_len(),
            });
        }
        reservation.commit(Bytes::from(vec![0; bytes]))
    }

    pub(crate) async fn close_remote(&self) -> bool {
        self.queue.close_for_remote_eof_and_mark_ready().await
    }

    pub(crate) fn cycle(
        &mut self,
        hard_pressure: bool,
        drop_debt: bool,
        tun_drain_bytes: usize,
    ) -> D16HarnessCycle {
        let drained_bytes = self.ctx.release_inflight_prefix(tun_drain_bytes);
        self.committed_tun_bytes = self
            .committed_tun_bytes
            .saturating_add(drained_bytes as u64);

        let previous = self.ctx.d16_egress_phase;
        let before_permissions = EgressPermissions::for_phase(previous);
        let before = self.queue.snapshot_now();
        let below_low = self.ctx.downlink_inflight_permit_bytes() <= 128 * 1024;
        let phase = transition_egress_phase(previous, hard_pressure, drop_debt, below_low, false);
        self.ctx
            .downlink_diag
            .note_d16_egress_phase(previous, phase, None);
        self.ctx.d16_egress_phase = phase;
        let permissions = EgressPermissions::for_phase(phase);

        let mut queue_to_pending_bytes = 0usize;
        let mut admitted_bytes = 0usize;
        if permissions.allow_admission {
            let drained = drain_d16_owned_queue_into_pending(
                &mut self.ctx,
                permissions.max_admit_quantum_bytes,
            );
            queue_to_pending_bytes = drained.bytes;
            let outcome = with_downlink_admission(
                &mut self.ctx,
                DownlinkAdmissionOrigin::EgressActor,
                |ctx, _| {
                    let accepted = ctx
                        .downlink_pending
                        .len()
                        .min(permissions.max_admit_quantum_bytes);
                    ctx.admit_pending(accepted);
                    if accepted > 0 {
                        ctx.downlink_diag
                            .note_send_slice_ok(accepted, ctx.downlink_pending.len());
                    }
                    DownlinkFlushOutcome {
                        accepted_bytes: accepted,
                        headroom_limited: false,
                    }
                },
            );
            admitted_bytes = outcome.accepted_bytes;
        }
        let _ = install_d16_remote_eof_if_ready(&mut self.ctx, 0);

        let previous = self.ctx.d16_egress_phase;
        let next = transition_egress_phase(previous, hard_pressure, drop_debt, below_low, true);
        self.ctx
            .downlink_diag
            .note_d16_egress_phase(previous, next, Some(drained_bytes));
        self.ctx.d16_egress_phase = next;
        debug_assert!(before.owned_bytes() <= 512 * 1024);
        debug_assert!(before_permissions.allow_drain);

        D16HarnessCycle {
            admitted_bytes,
            drained_bytes,
            queue_to_pending_bytes,
            phase: next,
            allow_read: permissions.allow_read,
            allow_admission: permissions.allow_admission,
        }
    }

    pub(crate) fn snapshot(&self) -> D16HarnessFlowSnapshot {
        let queue = self.queue.snapshot_now();
        let (queue_closed, queue_empty) = d16_remote_eof_queue_state(&self.ctx).unwrap_or_default();
        D16HarnessFlowSnapshot {
            queue,
            pending_bytes: self.ctx.downlink_pending.len(),
            inflight_bytes: self.ctx.downlink_inflight_permit_bytes(),
            actor_admitted_bytes: self.ctx.downlink_diag.actor_admitted_bytes,
            actor_bypass_admitted_bytes: self.ctx.downlink_diag.actor_bypass_admitted_bytes,
            committed_tun_bytes: self.committed_tun_bytes,
            phase: self.ctx.d16_egress_phase,
            eof_ready: remote_eof_ready_for_local_close(
                queue_closed,
                queue_empty,
                self.ctx.downlink_pending.len(),
                self.ctx.downlink_inflight_permit_bytes(),
            ),
        }
    }
}
