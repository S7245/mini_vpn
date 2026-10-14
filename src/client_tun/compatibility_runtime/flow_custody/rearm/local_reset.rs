//! Original-loop egress and terminal reset boundary.
//! Owns one reset kernel and raw flush failures in the actual physical owner.
//! The gateway alone commits rearm; transaction owns W/F. All four production
//! iface.poll edges await this flush before a later gateway turn. Normal DATA
//! adds no scan/lock/allocation. Tests: local_reset/tests and compatibility_f_flow.

use super::*;

pub(in crate::client_tun) async fn flush_tx_and_release_downlink_permits<D: TunIo>(
    device: &mut D,
    sockets: &SocketSet<'_>,
    socket_ctxs: &mut HashMap<SocketHandle, SocketCtx>,
    stage: &str,
) -> (std::io::Result<()>, TunFlushPermitRelease) {
    let result = device.flush_tx().await;
    let result = LocalReset::retain_flush_failure(socket_ctxs, result);
    let release = release_downlink_inflight_permits_for_flushed_tcp_packets(
        device.take_flushed_tcp_packets(),
        sockets,
        socket_ctxs,
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
    (result, release)
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
        let kind = error.kind();
        let source = crate::client_tun::compatibility_runtime::egress::original_source(error);
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
        Err(kind.into())
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
