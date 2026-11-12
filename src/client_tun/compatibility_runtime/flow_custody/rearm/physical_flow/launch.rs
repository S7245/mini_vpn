//! Complete original Ready-to-publication transaction. Flow/opens retain sole
//! admission, W/F, epoch and receipt authority. Native cold preparation uses
//! the same Flow exclusive turn; publication retains the original lock gate.
use super::*;
use super::super::super::{supervision, transaction};

impl OpenDispatch {
    pub(super) fn prepare_ready(
        &self,
        owner: &Arc<crate::client_tun::compatibility_runtime::SessionControl>,
        mut opens: transaction::custody::Guard<'_>,
        id: FlowIdentity,
        completion: &mut crate::client_tun::HandshakeCompletion<'_, '_>,
    ) -> bool {
        // These are the same values validated immediately before this call.
        let Some(flow) = opens.flow_mut(id) else { return false; };
        let Some(ctx) = completion.socket_ctxs.get_mut(&id.handle) else { return false; };
        if !matches!(
            self.relay_policy().generic_engine(),
            TcpRelayEngine::Legacy
                | TcpRelayEngine::ThinStaging
                | TcpRelayEngine::ContinuousPump
                | TcpRelayEngine::PermitPump
                | TcpRelayEngine::OrderedEgressPermitPump
        ) {
            flow.close(("policy", "generic_engine_not_yet_custodied"));
            return true;
        }
        let Some(mut turn) = cold::Turn::begin(owner, &mut opens, id) else {
            drop(opens); owner.quarantine_quiet(); return false;
        };
        drop(opens);
        let mut result = turn.prepare(self, ctx, &completion.global_tx, completion.downlink_backpressure);
        #[cfg(test)]
        if tests::cold::lose_turn(owner) { drop(turn); return false; }
        let mut opens = owner.opens();
        let prepared = match turn.restore(&mut opens, &mut result, ctx) {
            cold::Restore::Ready(prepared) => prepared,
            cold::Restore::Dispose => {
                drop(opens);
                turn.dispose_unpublished(&mut result);
                let mut opens = owner.opens();
                let _ = turn.restore(&mut opens, &mut result, ctx);
                return false;
            }
            cold::Restore::End => return false,
        };
        let cold::Prepared { channels: native_channels } = prepared;
        let Some(flow) = opens.flow_mut(id) else { return false; };
        let Some(mirror) = &mut ctx.flow_mirror else { return false; };
        if mirror.id != id || !mirror.matches_epoch(ctx.conn_epoch) || mirror.published_bytes.is_some() {
            flow.stage = FlowStage::Quarantined;
            owner.quarantine_quiet(); // D216: identity/publication disagreement ⇒ Session.
            return false;
        }
        if native_channels.is_none() && !matches!(
            flow.carrier,
            Some(OpenedOwnedTcp::Legacy(
                OpenedTcpRelay::Generic(_)
                    | OpenedTcpRelay::Native(_)
                    | OpenedTcpRelay::NativeByteOwned(_)
            ))
        ) {
            flow.quarantine_flow(owner);
            return false;
        }
        let (tx, credit_tx, permit_engine, native, d16) = if let Some((tx, credit_tx)) = native_channels {
            let engine = match self.relay_policy().native_engine() {
                crate::upstream::NativeTcpRelayEngine::ChunkPump => TcpRelayEngine::ContinuousPump,
                crate::upstream::NativeTcpRelayEngine::PermitPump => TcpRelayEngine::OrderedEgressPermitPump,
            };
            (tx, credit_tx, engine, true, false)
        } else {
        let (tx, rx) = crate::client_tun::uplink::channel(RELAY_CHANNEL_CAPACITY);
        let (credit_tx, credit_rx) = watch::channel(initial_relay_read_credit(false));
        let (roles, permit_engine, native, d16) = match flow.carrier.take() {
            Some(OpenedOwnedTcp::Legacy(OpenedTcpRelay::Generic(stream))) => (
                FlowRoles::new(supervision::GenericLaunchRequest {
                    engine: self.relay_policy().generic_engine(),
                    handle: id.handle,
                    epoch: id.epoch,
                    stream,
                    rx,
                    back_tx: completion.global_tx.clone(),
                    read_credit_rx: credit_rx,
                    downlink_backpressure: completion.downlink_backpressure,
                }),
                self.relay_policy().generic_engine(),
                false,
                false,
            ),
            Some(OpenedOwnedTcp::Legacy(OpenedTcpRelay::NativeByteOwned(native))) => {
                let budget = crate::client_tun::d16_process_global_budget();
                let queue = match crate::tcp_downlink_pump::AsyncLeasedByteFlowQueue::new_with_global_budget(
                    crate::client_tun::D16_PER_FLOW_BYTE_CAPACITY,
                    DownstreamPermitReleaseMode::OnEgressDrain,
                    budget.clone(),
                ) {
                    Ok(queue) => queue,
                    Err(_) => {
                        flow.carrier = Some(OpenedOwnedTcp::Legacy(OpenedTcpRelay::NativeByteOwned(native)));
                        flow.quarantine_flow(owner);
                        return false;
                    }
                };
                let request = supervision::D16LaunchRequest {
                    handle: id.handle,
                    epoch: id.epoch,
                    native,
                    queue,
                    budget,
                    rx,
                    back_tx: completion.global_tx.clone(),
                    read_credit_rx: credit_rx,
                };
                let roles = match FlowRoles::new_d16(request) {
                    Ok(roles) => roles,
                    Err(request) => {
                        flow.carrier = Some(OpenedOwnedTcp::Legacy(
                            OpenedTcpRelay::NativeByteOwned(request.native),
                        ));
                        flow.quarantine_flow(owner);
                        return false;
                    }
                };
                (roles, TcpRelayEngine::Legacy, false, true)
            }
            other => {
                flow.carrier = other;
                flow.quarantine_flow(owner);
                return false;
            }
        };
        flow.roles = Some(roles);
        (tx, credit_tx, permit_engine, native, d16)
        };
        let Some(roles) = flow.roles.as_mut() else {
            return false;
        };
        if mirror.window.is_some() {
            flow.stage = FlowStage::Quarantined;
            return false;
        }
        mirror.window = Some(ReceiveWindow::new());
        let Some(window) = mirror.window.as_mut().and_then(ReceiveWindow::take_reader) else {
            flow.stage = FlowStage::Quarantined;
            return false;
        };
        roles.prepare(window);
        #[cfg(test)]
        let missing_start_gate = flow.missing_start_gate.take();
        #[cfg(test)]
        if !native {
            if let Some(index) = missing_start_gate {
                roles.drop_start_gate_for_test(index);
            }
        }
        if roles.quarantined() {
            flow.quarantine_flow(owner);
            return false;
        }
        if !mirror.bind_permit_queue(roles, permit_engine) {
            flow.quarantine_flow(owner);
            return false;
        }
        if d16 && !mirror.bind_d16_queue(roles) {
            flow.quarantine_flow(owner);
            return false;
        }
        if native && !mirror.bind_native_backing(roles) {
            flow.quarantine_flow(owner);
            return false;
        }
        if ctx.uplink_tx.is_some()
            || ctx.relay_read_credit_tx.is_some()
            || !ctx.uplink_buffer.is_empty()
            || mirror.published_bytes.is_some()
        {
            flow.stage = FlowStage::Quarantined;
            owner.quarantine_quiet(); // D216: a second producer on this socket ⇒ Session.
            return false;
        }
        // The fresh channel has a single original producer and closed worker
        // gates. Keep escrow until the synchronous physical ACK is committed.
        match tx.try_reserve() {
            Ok(permit) => permit.send(flow.escrow.clone().into_command()),
            Err(crate::shared::bounded_channel::ReserveError::Capacity) => {
                flow.close(("native_uplink", "capacity_refused")); return false;
            }
            Err(crate::shared::bounded_channel::ReserveError::Closed) => {
                flow.close(("native_uplink", "channel_closed")); return false;
            }
            Err(crate::shared::bounded_channel::ReserveError::Quarantined) => {
                flow.stage = FlowStage::Quarantined; return false;
            }
        }
        if native && !roles.commit_native() {
            flow.quarantine_flow(owner);
            return false;
        }
        if d16 && !roles.commit_d16() {
            flow.quarantine_flow(owner);
            return false;
        }
        #[cfg(test)]
        if native {
            if let Some(index) = missing_start_gate {
                roles.drop_native_start_gate_for_test(index);
            }
        }
        ctx.uplink_tx = Some(tx);
        if d16 {
            ctx.d16_downlink_queue = roles.d16_queue().cloned();
        }
        ctx.relay_read_credit_tx = Some(credit_tx);
        ctx.state = SocketState::Relaying;
        ctx.pending_open_mode = None;
        mirror.published_bytes = Some(flow.escrow.len());
        flow.published = true;
        flow.stage = FlowStage::Published;
        let reserved = flow.reserved;
        let escrow_bytes = flow.escrow_bytes;
        flow.escrow = Vec::new().into();
        flow.escrow_bytes = 0;
        flow.reserved = 0;
        flow.w_held = false;
        if let Some(accounting) = &mut opens.flow {
            accounting.w -= 1;
            accounting.bytes -= reserved;
            accounting.escrow_bytes -= escrow_bytes;
            accounting.published += 1;
        }
        completion.metrics_handle.inc_relays_spawned();
        let start = opens.flow_mut(id).and_then(|flow| {
            let roles = flow.roles.as_mut()?;
            if native { roles.take_native_start(owner, id) }
            else { roles.start(); None }
        });
        drop(opens);
        if let Some(start) = start { start.dispose(owner, &mut std::task::Context::from_waker(std::task::Waker::noop())); }
        return true;
    }
}
