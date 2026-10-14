//! Shutdown census and bounded no-F reset at the original loop composition seam.
//! Borrows the actual sockets and root ledger; owns no second registry or timers.
//! The driver retains only a scan cursor. Existing gateway/poll/flush commits
//! resets under the original deadline; DATA never enters this stop-only scan.
//! Tests: local_reset/transaction children and compatibility_unadmitted_shutdown.

use super::*;

const PHYSICAL_SCAN_PER_TURN: usize = 8;
const MAX_PHYSICAL_SOCKETS: usize = 4352;

// A bare physical reset must never erase an unaccounted relay or owned resource.
fn no_flow_resources_empty(ctx: &SocketCtx) -> bool {
    (ctx.state == SocketState::Listening
        || (ctx.state == SocketState::Closing && ctx.local_reset.is_some()))
        && ctx.uplink_tx.is_none()
        && ctx.resumable_uplink.is_none()
        && ctx.resumable_sink.is_none()
        && ctx.resumable_half_close.is_none()
        && ctx.resumable_terminal.is_none()
        && ctx.resumable_action_bridge.is_none()
        && ctx.pending_open_mode.is_none()
        && ctx.relay_read_credit_tx.is_none()
        && ctx.d16_downlink_queue.is_none()
        && !ctx.owned_terminal_pending
        && ctx.owned_terminal_wait_since_secs.is_none()
        && ctx.d16_egress_phase == EgressPhase::Running
        && !ctx.d16_tun_rx_ack_barrier
        && !ctx.local_fin_sent
        && !ctx.local_eof_sent
        && ctx.local_fin_pending_since_secs.is_none()
        && ctx.downlink_pending.is_empty()
        && ctx.downlink_pending_permits.is_empty()
        && ctx.downlink_inflight_permits.is_empty()
        && ctx.pending_relay_close.is_none()
        && ctx.pending_relay_close_since_secs.is_none()
        && ctx.fake_ip.is_none()
        && ctx.uplink_buffer.is_empty()
}

fn physical_work(socket: &TcpSocket<'_>) -> bool {
    // Closed can still own a tuple/RST awaiting actual dispatch and flush.
    socket.remote_endpoint().is_some()
        || !matches!(socket.state(), TcpState::Listen | TcpState::Closed)
        || socket.send_queue() != 0
        || socket.recv_queue() != 0
}

fn physical_identity(
    handle: SocketHandle,
    socket: &TcpSocket<'_>,
    ctx: &SocketCtx,
    opens: &super::super::transaction::OpenCustody,
) -> bool {
    if ctx.local_port == 0
        || socket
            .local_endpoint()
            .is_some_and(|local| local.port != ctx.local_port)
    {
        return false;
    }
    match &ctx.flow_mirror {
        Some(mirror) => {
            ctx.local_reset.is_none()
                && mirror.id.handle == handle
                && mirror.matches_epoch(ctx.conn_epoch)
                && mirror.matches_root(opens)
        }
        None => no_flow_resources_empty(ctx),
    }
}

impl OpenDispatch {
    pub(in crate::client_tun) fn drain_shutdown_physical(
        &self,
        sockets: &mut SocketSet<'_>,
        contexts: &mut HashMap<SocketHandle, SocketCtx>,
        pool: &mut FakeIpPool,
        cursor: &mut usize,
        now_secs: u64,
    ) {
        if !self.flow_draining() {
            return;
        }
        let Some(owner) = &self.owner else { return };
        let mut entries = sockets.iter_mut().skip(*cursor);
        for _ in 0..PHYSICAL_SCAN_PER_TURN {
            let Some((handle, socket)) = entries.next() else {
                *cursor = 0;
                break;
            };
            if !owner.take_control() {
                break;
            }
            *cursor += 1;
            let smoltcp::socket::Socket::Tcp(socket) = socket else {
                owner.ledger().quarantined = true;
                continue;
            };
            let Some(ctx) = contexts.get_mut(&handle) else {
                owner.ledger().quarantined = true;
                continue;
            };
            // Release the root lock before entering the existing gateway.
            let consistent = {
                let opens = owner.opens();
                physical_identity(handle, socket, ctx, &opens)
                    && (ctx.flow_mirror.is_some()
                        || !opens
                            .slots
                            .iter()
                            .flatten()
                            .any(|slot| slot.handle == handle))
            };
            if !consistent {
                if let Some(mirror) = &mut ctx.flow_mirror {
                    mirror.quarantined = true;
                }
                owner.ledger().quarantined = true;
                continue;
            }
            if ctx.flow_mirror.is_none() && (ctx.local_reset.is_some() || physical_work(socket)) {
                legacy_bare_rearm_from_loop(self, handle, socket, ctx, pool, now_secs);
            }
        }
    }

    pub(in crate::client_tun) fn flow_shutdown_complete(
        &self,
        sockets: &SocketSet<'_>,
        contexts: &mut HashMap<SocketHandle, SocketCtx>,
        pool: &FakeIpPool,
    ) -> bool {
        let Some(owner) = &self.owner else {
            return false;
        };
        if !self.flow_draining() {
            return false;
        }
        let mut opens = owner.opens();
        let leases = pool.audit_checked_leases(
            contexts
                .values()
                .filter_map(|ctx| ctx.flow_mirror.as_ref())
                .filter_map(|mirror| mirror.live_lease()),
        );
        let mut tcp_count = 0;
        let mut consistent = true;
        let mut pending = false;
        for (handle, socket) in sockets.iter() {
            let smoltcp::socket::Socket::Tcp(socket) = socket else {
                consistent = false;
                continue;
            };
            tcp_count += 1;
            let Some(ctx) = contexts.get_mut(&handle) else {
                consistent = false;
                continue;
            };
            if !physical_identity(handle, socket, ctx, &opens) {
                consistent = false;
                if let Some(mirror) = &mut ctx.flow_mirror {
                    mirror.quarantined = true;
                }
            }
            if let Some(mirror) = &mut ctx.flow_mirror {
                if mirror
                    .live_lease()
                    .is_some_and(|lease| !leases.accepts(lease))
                {
                    mirror.quarantined = true;
                    consistent = false;
                }
            }
            if let Some(reset) = &ctx.local_reset {
                pending = true;
                consistent &= !reset.quarantined();
            }
            pending |= ctx.flow_mirror.is_none() && physical_work(socket);
        }
        consistent &= tcp_count == contexts.len() && tcp_count <= MAX_PHYSICAL_SOCKETS;
        if !consistent {
            // Error-only reverse physical lookup; never allocate this on DATA
            // or the consistent shutdown path. F4096 admits at most 4352 sockets.
            let mut handles = std::collections::HashSet::new();
            let mut bounded = true;
            for (handle, socket) in sockets.iter() {
                if matches!(socket, smoltcp::socket::Socket::Tcp(_)) {
                    if handles.len() == MAX_PHYSICAL_SOCKETS {
                        bounded = false;
                        break;
                    }
                    handles.insert(handle);
                }
            }
            for (handle, ctx) in contexts.iter_mut() {
                // An overflowing inventory is uncertain, never clean zero.
                if !bounded || !handles.contains(handle) {
                    if let Some(mirror) = &mut ctx.flow_mirror {
                        mirror.quarantined = true;
                    }
                }
            }
        }
        // Validate every actual row even after an earlier physical failure.
        // A corrupt inner handle is used only to inspect, never to mutate a peer.
        for (index, slot) in opens.slots.iter_mut().enumerate() {
            let Some(slot) = slot else { continue };
            let valid = match &slot.phase {
                super::super::transaction::OpenPhase::Flow(flow) => {
                    flow.id.index == index
                        && slot.matches_physical(flow.id)
                        && contexts.get_mut(&flow.id.handle).is_some_and(|ctx| {
                            let Some(mirror) = &mut ctx.flow_mirror else {
                                return false;
                            };
                            if !mirror.matches_slot(slot) {
                                return false;
                            }
                            // Only a proven retained receipt permits advanced
                            // epoch and no live lease. Original epoch needs the
                            // original row's exact acquisition capability.
                            let valid = mirror.matches_epoch(ctx.conn_epoch)
                                && !mirror.quarantined
                                && (ctx.conn_epoch != flow.id.epoch
                                    || flow.fake_ip
                                        == mirror.live_lease().map(|lease| lease.receipt()));
                            if !valid {
                                mirror.quarantined = true;
                            }
                            valid
                        })
                }
                _ => false,
            };
            if !valid {
                consistent = false;
                if let super::super::transaction::OpenPhase::Flow(flow) = &mut slot.phase {
                    flow.stage = super::super::transaction::FlowStage::Quarantined;
                }
            }
        }
        let physical = leases.complete() && consistent;
        if !physical {
            owner.ledger().quarantined = true;
        }
        opens
            .flow
            .as_ref()
            .is_some_and(|a| a.identity_fault.is_none())
            && physical
            && !pending
            && opens.live == 0
            && opens.audit_flows()
            && !owner.ledger().quarantined
    }
}

#[cfg(all(test, feature = "harness"))]
mod tests;
