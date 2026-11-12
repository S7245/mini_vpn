//! Shutdown census and bounded no-F reset at the original loop composition seam.
//! Borrows the actual sockets and root ledger; owns no second registry or timers.
//! The driver retains only a scan cursor. Existing gateway/poll/flush commits
//! resets under the original deadline; DATA never enters this stop-only scan.
//! Tests: local_reset/transaction children and compatibility_unadmitted_shutdown.

use super::super::transaction::FLOW_SLOT_LIMIT;
use super::*;

const PHYSICAL_SCAN_PER_TURN: usize = 8;
const MAX_PHYSICAL_SOCKETS: usize = 4352;
const ROOT_SEEN_WORDS: usize = FLOW_SLOT_LIMIT.div_ceil(u64::BITS as usize);

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

fn physical_endpoint(socket: &TcpSocket<'_>, ctx: &SocketCtx) -> bool {
    ctx.local_port != 0
        && socket.local_endpoint().is_none_or(|local| local.port == ctx.local_port)
}

fn physical_identity(
    handle: SocketHandle,
    socket: &TcpSocket<'_>,
    ctx: &SocketCtx,
    opens: &super::super::transaction::OpenCustody,
) -> bool {
    if !physical_endpoint(socket, ctx) { return false; }
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

/// D64: periodic full census cadence while live flows remain (was 5 ms, which
/// cost ≈0.09 of a debug core on a held drain).
pub(in crate::client_tun) const SHUTDOWN_CENSUS_CADENCE: std::time::Duration =
    std::time::Duration::from_millis(50);

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
                owner.quarantine_quiet();
                continue;
            };
            let Some(ctx) = contexts.get_mut(&handle) else {
                owner.quarantine_quiet();
                continue;
            };
            // Release the root lock before entering the existing gateway.
            let consistent = {
                let opens = owner.opens();
                physical_identity(handle, socket, ctx, &opens)
                    && (ctx.flow_mirror.is_some()
                        || !opens.slots.occupied().any(|slot| slot.handle == handle))
            };
            if !consistent {
                if let Some(mirror) = &mut ctx.flow_mirror {
                    mirror.quarantined = true;
                }
                owner.quarantine_quiet();
                continue;
            }
            if ctx.flow_mirror.is_none() && (ctx.local_reset.is_some() || physical_work(socket)) {
                legacy_bare_rearm_from_loop(self, handle, socket, ctx, pool, now_secs);
            }
        }
    }

    /// Schedule the original full bidirectional check at stop, every
    /// `SHUTDOWN_CENSUS_CADENCE` while flows are live (D64: a live flow cannot
    /// complete, so the cadence only bounds detection latency), and every turn
    /// once the live count is zero.
    pub(in crate::client_tun) fn flow_shutdown_census_candidate(
        &self,
        last: Option<tokio::time::Instant>,
        now: tokio::time::Instant,
    ) -> bool {
        if !self.flow_draining() {
            return false;
        }
        last.is_none_or(|previous| {
            now.saturating_duration_since(previous) >= SHUTDOWN_CENSUS_CADENCE
        }) || self
            .owner
            .as_ref()
            .is_some_and(|owner| owner.opens().live == 0)
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
        #[cfg(all(test, feature = "harness"))]
        if let Some(bridge) = &self.relay_event_test_bridge {
            bridge.observe_egress_shutdown(contexts);
        }
        #[cfg(test)]
        let sampled = owner
            .shutdown_census_runs
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            % 64
            == 0;
        #[cfg(test)]
        let started = sampled.then(std::time::Instant::now);
        let mut opens = owner.opens();
        #[cfg(test)]
        let locked = started.map(|_| std::time::Instant::now());
        let leases = pool.audit_checked_leases(
            contexts
                .values()
                .filter_map(|ctx| ctx.flow_mirror.as_ref())
                .filter_map(|mirror| mirror.live_lease()),
        );
        #[cfg(test)]
        let leases_done = started.map(|_| std::time::Instant::now());
        // Count the complete physical set independently. Every context below
        // must inject into its own checked TCP slot; equal cardinality proves
        // reverse coverage, including extra sockets with no context.
        let (tcp_count, all_tcp) = sockets.iter().fold((0usize, true), |(count, valid), (_, socket)| {
            match socket {
                smoltcp::socket::Socket::Tcp(_) => (count + 1, valid),
                _ => (count, false),
            }
        });
        let mut consistent = opens.slots.len() == FLOW_SLOT_LIMIT && all_tcp;
        let mut pending = false;
        // One-call scratch, never a cached certificate. The original root lock
        // and exclusive contexts borrow cover both directions of this census.
        let mut root_seen = [0u64; ROOT_SEEN_WORDS];
        for (&handle, ctx) in contexts.iter_mut() {
            let Some(socket) = sockets.try_get::<TcpSocket>(handle) else {
                // Preserve the original error-only reverse pass and its Q
                // effects below; no unqualified context is reset or released.
                consistent = false;
                continue;
            };
            let endpoint_matches = physical_endpoint(socket, ctx);
            if let Some(mirror) = &mut ctx.flow_mirror {
                // One immutable Root borrow and one receipt/epoch check feed
                // both directions of this same census. matches_physical(id)
                // proves outer handle/epoch plus the entire inner Flow id;
                // repeating it with that already-equal inner id adds no fact.
                let row = opens.slots.get(mirror.id.index).and_then(Option::as_ref)
                    .filter(|slot| slot.matches_physical(mirror.id))
                    .and_then(|slot| match &slot.phase {
                        super::super::transaction::OpenPhase::Flow(flow) => Some(flow),
                        _ => None,
                    });
                let epoch_matches = mirror.matches_epoch(ctx.conn_epoch);
                if !endpoint_matches || ctx.local_reset.is_some()
                    || mirror.id.handle != handle || !epoch_matches || row.is_none()
                {
                    consistent = false;
                    mirror.quarantined = true;
                }
                if mirror.live_lease().is_some_and(|lease| !leases.accepts(lease)) {
                    mirror.quarantined = true;
                    consistent = false;
                }
                let valid = row.is_some_and(|flow| {
                    // D210: a registered Flow-Q row is held custody, not a
                    // census inconsistency; identity and lease checks still apply.
                    let valid = epoch_matches && (!mirror.quarantined || flow.flow_q_registered())
                        && (ctx.conn_epoch != flow.id.epoch
                            || flow.fake_ip == mirror.live_lease().map(|lease| lease.receipt()));
                    if !valid { mirror.quarantined = true; }
                    valid
                });
                if valid {
                    let bit = 1u64 << (mirror.id.index % u64::BITS as usize);
                    match root_seen.get_mut(mirror.id.index / u64::BITS as usize) {
                        Some(word) if *word & bit == 0 => *word |= bit,
                        _ => consistent = false,
                    }
                }
            } else {
                consistent &= endpoint_matches && no_flow_resources_empty(ctx);
            }
            if let Some(reset) = &ctx.local_reset {
                pending = true;
                consistent &= !reset.quarantined();
            }
            pending |= ctx.flow_mirror.is_none() && physical_work(socket);
        }
        #[cfg(test)]
        let sockets_done = started.map(|_| std::time::Instant::now());
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
                if !bounded || !handles.contains(handle) || sockets.try_get::<TcpSocket>(*handle).is_none() {
                    if let Some(mirror) = &mut ctx.flow_mirror {
                        mirror.quarantined = true;
                    }
                }
            }
        }
        // Freeze the physical result before root checks can add a failure. The
        // error-only reverse pass above may have newly quarantined mirrors, so
        // that path MUST re-run the original context check, not trust old bits.
        let physical_consistent = consistent;
        // Validate every actual row even after an earlier physical failure.
        // A corrupt inner handle is used only to inspect, never to mutate a peer.
        // D62: the occupancy index is part of the census, both directions.
        if !opens.slots.occupancy_consistent() {
            consistent = false;
        }
        for (index, slot) in opens.slots.occupied_mut() {
            let valid = if physical_consistent {
                root_seen
                    .get(index / u64::BITS as usize)
                    .is_some_and(|word| *word & (1u64 << (index % u64::BITS as usize)) != 0)
            } else {
                match &slot.phase {
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
                }
            };
            if !valid {
                consistent = false;
                if let super::super::transaction::OpenPhase::Flow(flow) = &mut slot.phase {
                    flow.stage = super::super::transaction::FlowStage::Quarantined;
                }
            }
        }
        #[cfg(test)]
        let reverse_done = started.map(|_| std::time::Instant::now());
        let physical = leases.complete() && consistent;
        let root_consistent = opens.audit_flows();
        #[cfg(test)]
        let cost = if let (
            Some(started),
            Some(locked),
            Some(leases_done),
            Some(sockets_done),
            Some(reverse_done),
        ) = (started, locked, leases_done, sockets_done, reverse_done)
        {
            Some((
                locked.duration_since(started),
                leases_done.duration_since(locked),
                sockets_done.duration_since(leases_done),
                reverse_done.duration_since(sockets_done),
                reverse_done.elapsed(),
            ))
        } else {
            None
        };
        // D210: at stop, once only registered Flow-Q rows remain (none can
        // release), any Q becomes Session scope: a bounded Incomplete end.
        let q_only = opens.live == opens.flow_q_rows() && owner.ledger().quarantined();
        if !physical || !root_consistent || q_only {
            owner.quarantine_quiet();
        }
        let complete = opens
            .flow
            .as_ref()
            .is_some_and(|a| a.identity_fault.is_none())
            && physical
            && root_consistent
            && !pending
            && opens.live == 0
            && !owner.ledger().quarantined();
        #[cfg(test)]
        {
            drop(opens);
            if let Some((lock, lease, sockets, reverse, root)) = cost {
                println!(
                    "B826 CensusCost session={} run={} lock={lock:?} lease={lease:?} sockets={sockets:?} reverse={reverse:?} root={root:?}",
                    Arc::as_ptr(owner) as usize,
                    owner
                        .shutdown_census_runs
                        .load(std::sync::atomic::Ordering::Relaxed),
                );
            }
        }
        complete
    }
}

#[cfg(all(test, feature = "harness"))]
mod tests;
