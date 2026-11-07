//! Original-loop egress and terminal reset boundary.
//! Owns one reset kernel and raw flush failures in the actual physical owner.
//! The gateway alone commits rearm; transaction owns W/F. All four production
//! iface.poll edges await this flush before a later gateway turn. Normal DATA
//! adds no new scan/lock/allocation beyond the original flush capture scan.
//! Tests: local_reset/tests and compatibility_f_flow.

use super::*;

/// One physical poll, actual device flush and same-turn ACK disposition.
/// The sole pre-poll capture Vec replaces the old post-poll flush capture Vec.
pub(in crate::client_tun) async fn poll_egress_cycle<D: TunIo>(
    device: &mut D,
    iface: &mut Interface,
    sockets: &mut SocketSet<'_>,
    socket_ctxs: &mut HashMap<SocketHandle, SocketCtx>,
    targets: Option<&HashSet<SocketHandle>>,
    stage: &str,
    #[cfg(all(test, feature = "harness"))] test_bridge: Option<
        &Arc<crate::client_tun::legacy_event_loop::relay_events::RelayEventTestBridge>,
    >,
) -> (std::io::Result<()>, TunFlushPermitRelease) {
    #[cfg(all(test, feature = "harness"))]
    if let Some(bridge) = test_bridge {
        bridge.observe_egress_probe(
            crate::client_tun::legacy_event_loop::relay_events::EgressProbePhase::BeforePoll,
            socket_ctxs,
        );
    }
    let mut captured = match crate::client_tun::downlink_custody::capture_poll_egress(
        sockets,
        socket_ctxs,
        device.main_capture_meter(),
    ) {
        Ok(captured) => captured,
        Err(error) => return (
            Err(capture_refusal(device, error)),
            TunFlushPermitRelease::default(),
        ),
    };
    iface.poll_with_egress_targets(
        smoltcp::time::Instant::now(),
        device,
        sockets,
        targets.map(|set| set.iter().copied()),
    );
    #[cfg(all(test, feature = "harness"))]
    if let Some(bridge) = test_bridge {
        bridge.inject_foreign_egress(
            crate::client_tun::legacy_event_loop::relay_events::ForeignEgressPhase::Flush,
            stage,
            sockets,
            socket_ctxs,
            None,
        );
    }
    if !crate::client_tun::downlink_custody::prepare_poll_egress_flush(
        &mut captured,
        sockets,
        socket_ctxs,
    ) {
        return (
            Err(capture_refusal(device, crate::client_tun::downlink_custody::poll_capture_refusal(&captured).unwrap_or(crate::client_tun::downlink_custody::CaptureRefusal::Inventory))),
            TunFlushPermitRelease::default(),
        );
    }
    let result = LocalReset::retain_flush_failure(socket_ctxs, device.flush_tx().await);
    let mut release = crate::client_tun::downlink_custody::finish_poll_egress_flush(
        device.take_flushed_tcp_packets(),
        sockets,
        socket_ctxs,
        &mut captured,
        result.is_ok(),
    );
    #[cfg(all(test, feature = "harness"))]
    if let Some(bridge) = test_bridge {
        bridge.observe_egress_probe(
            crate::client_tun::legacy_event_loop::relay_events::EgressProbePhase::AfterFlush,
            socket_ctxs,
        );
    }
    #[cfg(all(test, feature = "harness"))]
    if let Some(bridge) = test_bridge {
        bridge.inject_foreign_egress(
            crate::client_tun::legacy_event_loop::relay_events::ForeignEgressPhase::Ack,
            stage,
            sockets,
            socket_ctxs,
            Some(&captured),
        );
    }
    release.released_inflight_permit_bytes = release.released_inflight_permit_bytes.saturating_add(
        crate::client_tun::downlink_custody::release_poll_egress_ack(
            &mut captured,
            sockets,
            socket_ctxs,
        ),
    );
    #[cfg(all(test, feature = "harness"))]
    if let Some(bridge) = test_bridge {
        bridge.observe_egress_probe(
            crate::client_tun::legacy_event_loop::relay_events::EgressProbePhase::AfterAck,
            socket_ctxs,
        );
        bridge.observe_pending_probe(sockets, socket_ctxs);
        bridge.observe_stock_receipt(socket_ctxs);
    }
    if release.released_inflight_permit_bytes > 0 {
        trace_log!(
            "🔎 tcp-d6-egress-permit-release stage={} flushed_tcp_payload={} released_inflight_permits={}",
            stage,
            release.flushed_tcp_payload_bytes,
            release.released_inflight_permit_bytes
        );
    }
    release.refusal = crate::client_tun::downlink_custody::poll_capture_refusal(&captured);
    (capture_outcome(device, result, release.refusal), release)
}

pub(in crate::client_tun) async fn flush_tx_and_release_downlink_permits<D: TunIo>(
    device: &mut D,
    sockets: &SocketSet<'_>,
    socket_ctxs: &mut HashMap<SocketHandle, SocketCtx>,
    stage: &str,
) -> (std::io::Result<()>, TunFlushPermitRelease) {
    let captured = match crate::client_tun::downlink_custody::begin_flush_batch(
        sockets,
        socket_ctxs,
        device.main_capture_meter(),
    ) {
        Ok(captured) => captured,
        Err(error) => return (
            Err(capture_refusal(device, error)),
            TunFlushPermitRelease::default(),
        ),
    };
    let result = device.flush_tx().await;
    let result = LocalReset::retain_flush_failure(socket_ctxs, result);
    let release = crate::client_tun::downlink_custody::finish_flush_batch(
        device.take_flushed_tcp_packets(),
        sockets,
        socket_ctxs,
        captured,
        result.is_ok(),
    );
    if release.released_inflight_permit_bytes > 0 {
        // This fires once per flushed batch and dominated long-run logs. The
        // 30s TCP aggregate retains the same byte accounting; keep individual
        // batches behind the explicit full-trace gate.
        trace_log!(
            "🔎 tcp-d6-egress-permit-release stage={} flushed_tcp_payload={} released_inflight_permits={}",
            stage,
            release.flushed_tcp_payload_bytes,
            release.released_inflight_permit_bytes
        );
    }
    (capture_outcome(device, result, release.refusal), release)
}

// Both real entry points report one capture outcome after all proof/ACK work.
// The Native adapter retains first/stop; Legacy refusal keeps its old policy.
fn capture_refusal<D: TunIo>(device: &mut D, error: crate::client_tun::downlink_custody::CaptureRefusal) -> std::io::Error {
    if device.main_capture_meter().is_some() {
        device.main_capture_failure(Some(error.into_io()))
    } else {
        std::io::Error::other("capture capacity/identity unavailable")
    }
}
fn capture_outcome<D: TunIo>(device: &mut D, result: std::io::Result<()>, refusal: Option<crate::client_tun::downlink_custody::CaptureRefusal>) -> std::io::Result<()> {
    match (refusal, result) {
        (Some(crate::client_tun::downlink_custody::CaptureRefusal::Inventory), Err(original)) => {
            // A new failed proof is distinct from the actual IO already kept
            // by its owner. Publish the proof once, return the original IO.
            drop(device.main_capture_failure(Some(crate::client_tun::downlink_custody::CaptureRefusal::Inventory.into_io())));
            Err(original)
        }
        (Some(refusal), result) => Err(device.main_capture_failure(Some(result.err().unwrap_or_else(|| refusal.into_io())))),
        (None, result) => result,
    }
}

/// One original-loop reset, retained by the no-F context or the F mirror. The socket tuple remains physical custody until
/// smoltcp dispatch and the immediately awaited common flush finish. No flow slot,
/// task, timer, queue or retry authority is added here.
#[derive(Debug)]
pub(in crate::client_tun) struct LocalReset {
    epoch: u64,
    tuple: Option<(smoltcp::wire::IpEndpoint, smoltcp::wire::IpEndpoint)>,
    deadline: tokio::time::Instant,
    committing: bool,
    invalid: Option<ResetInvalid>,
    first_io: Option<Arc<std::io::Error>>,
    flush_failures: u64,
}

/// Value-only proof issued by the original reset owner, never inferred from
/// a missing socket tuple. It grants no ACK, admission, rearm or new deadline.
#[derive(Debug)]
pub(in crate::client_tun) struct ResetProof {
    owner: ResetOwner,
    epoch: u64,
    tuple: (IpEndpoint, IpEndpoint),
    deadline: tokio::time::Instant,
}
#[derive(Debug)]
enum ResetOwner {
    Local,
    Flow,
}
impl ResetProof {
    pub(in crate::client_tun) fn capture(
        ctx: &SocketCtx,
        socket: &TcpSocket<'_>,
    ) -> Result<Option<Self>, ()> {
        let (reset, owner) = match (&ctx.local_reset, &ctx.flow_mirror) {
            (Some(_), Some(_)) => return Err(()),
            (Some(reset), None) => (reset, ResetOwner::Local),
            (None, Some(mirror)) => match &mirror.reset {
                Some(reset) => (reset, ResetOwner::Flow),
                None => return Ok(None),
            },
            (None, None) => return Ok(None),
        };
        if reset.quarantined() {
            return Err(());
        }
        let proof = Self {
            owner,
            epoch: reset.epoch,
            tuple: reset.tuple.ok_or(())?,
            deadline: reset.deadline,
        };
        if !proof.matches(ctx, socket) {
            return Err(());
        }
        Ok(Some(proof))
    }
    pub(in crate::client_tun) fn identity(&self) -> (u64, IpEndpoint, IpEndpoint) {
        (self.epoch, self.tuple.0, self.tuple.1)
    }
    pub(in crate::client_tun) fn matches(&self, ctx: &SocketCtx, socket: &TcpSocket<'_>) -> bool {
        let reset = match self.owner {
            ResetOwner::Local if ctx.flow_mirror.is_none() => ctx.local_reset.as_ref(),
            ResetOwner::Flow if ctx.local_reset.is_none() => {
                ctx.flow_mirror.as_ref().and_then(|m| m.reset.as_ref())
            }
            _ => None,
        };
        // A newly retained IO failure is allowed only to account proven packets
        // from this batch. The original IO stays retained and end_batch stays Q.
        reset.is_some_and(|r| {
            !r.committing
                && r.invalid.is_none()
                && r.epoch == self.epoch
                && r.tuple == Some(self.tuple)
                && r.deadline == self.deadline
        }) && ctx.conn_epoch == self.epoch
            && ctx.local_port == self.tuple.0.port
            && socket.state() == TcpState::Closed
            && socket
                .local_endpoint()
                .zip(socket.remote_endpoint())
                .is_none_or(|t| t == self.tuple)
            && tokio::time::Instant::now() < self.deadline
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ResetInvalid {
    Identity,
    Deadline,
    Interrupted,
    EpochExhausted,
}

impl LocalReset {
    /// Called only by the physical gateway after proving that no W/F owns it.
    pub(super) fn prepare(
        ctx: &mut SocketCtx,
        socket: &mut TcpSocket<'_>,
        deadline: Option<tokio::time::Instant>,
    ) -> bool {
        Self::prepare_slot(
            &mut ctx.local_reset,
            ctx.conn_epoch,
            ctx.local_port,
            &mut ctx.state,
            socket,
            deadline.unwrap_or_else(|| {
                tokio::time::Instant::now() + std::time::Duration::from_secs(15)
            }),
        )
    }

    fn prepare_slot(
        slot: &mut Option<Self>,
        epoch: u64,
        local_port: u16,
        state: &mut SocketState,
        socket: &mut TcpSocket<'_>,
        deadline: tokio::time::Instant,
    ) -> bool {
        let tuple = socket.local_endpoint().zip(socket.remote_endpoint());
        if slot.is_none() {
            let invalid = if epoch.checked_add(1).is_none() {
                Some(ResetInvalid::EpochExhausted)
            } else if tokio::time::Instant::now() >= deadline {
                Some(ResetInvalid::Deadline)
            } else {
                None
            };
            // TimeWait has already completed FIN through the preceding common
            // flush. Only a fresh reset may reuse that graceful disposition.
            if (tuple.is_none() || socket.state() == TcpState::TimeWait) && invalid.is_none() {
                return true;
            }
            *slot = Some(Self {
                epoch,
                tuple,
                deadline,
                committing: false,
                invalid,
                first_io: None,
                flush_failures: 0,
            });
            if invalid.is_some() {
                return false;
            }
            *state = SocketState::Closing;
            socket.abort();
        }
        let Some(reset) = slot else {
            return false;
        };
        reset.deadline = reset.deadline.min(deadline);
        let invalid = if reset.committing {
            Some(ResetInvalid::Interrupted)
        } else if reset.epoch != epoch
            || reset
                .tuple
                .is_some_and(|(local, _)| local.port != local_port)
            || socket.state() != TcpState::Closed
            || tuple.is_some_and(|tuple| Some(tuple) != reset.tuple)
        {
            Some(ResetInvalid::Identity)
        } else if reset.epoch.checked_add(1).is_none() {
            Some(ResetInvalid::EpochExhausted)
        } else if tokio::time::Instant::now() >= reset.deadline {
            Some(ResetInvalid::Deadline)
        } else {
            None
        };
        if reset.invalid.is_none() {
            reset.invalid = invalid;
        }
        if reset.quarantined() {
            return false;
        }
        if tuple.is_some() {
            return false;
        }
        // No await occurs until the caller's listen succeeds and clears this
        // marker. Any interruption must not become a fresh reset attempt.
        reset.committing = true;
        true
    }

    pub(in crate::client_tun) fn quarantined(&self) -> bool {
        self.invalid.is_some() || self.first_io.is_some()
    }

    fn retain_flush_failure(
        contexts: &mut HashMap<SocketHandle, SocketCtx>,
        result: std::io::Result<()>,
    ) -> std::io::Result<()> {
        let Err(error) = result else {
            return result;
        };
        // Exception path only. Normal DATA performs no additional scan or lock.
        if !contexts.values().any(|ctx| {
            ctx.local_reset.is_some()
                || ctx
                    .flow_mirror
                    .as_ref()
                    .is_some_and(|mirror| mirror.reset.is_some())
        }) {
            return Err(error);
        }
        let (source, error) =
            crate::client_tun::compatibility_runtime::egress::retain_reset_error(error);
        for ctx in contexts.values_mut() {
            // A conflicting pair stays intact. The gateway quarantines it; both
            // actual owners retain the source instead of selecting one to erase.
            if let Some(reset) = &mut ctx.local_reset {
                reset.retain_source(&source);
            }
            if let Some(mirror) = &mut ctx.flow_mirror {
                if let Some(reset) = &mut mirror.reset {
                    reset.retain_source(&source);
                    mirror.quarantined = true;
                }
            }
        }
        // Existing call sites log the category; the original source remains
        // held by the physical owner even if the device already popped its queue.
        Err(error)
    }
    fn retain_source(&mut self, source: &Arc<std::io::Error>) {
        if self.first_io.is_none() {
            self.first_io = Some(source.clone());
        }
        self.flush_failures = self.flush_failures.saturating_add(1);
    }
}

impl FlowMirror {
    pub(super) fn prepare_reset(
        &mut self,
        epoch: u64,
        local_port: u16,
        state: &mut SocketState,
        socket: &mut TcpSocket<'_>,
        deadline: tokio::time::Instant,
    ) -> bool {
        let ready =
            LocalReset::prepare_slot(&mut self.reset, epoch, local_port, state, socket, deadline);
        if self.reset.as_ref().is_some_and(LocalReset::quarantined) {
            self.quarantined = true;
        }
        ready
    }
}

#[cfg(test)]
mod tests;
