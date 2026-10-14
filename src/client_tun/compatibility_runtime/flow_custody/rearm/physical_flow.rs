//! Physical F transaction in the original borrowed loop.
//! Owns its mirror, checked admission/publication and final physical receipt.
//! Root transaction owns W/F/roles; this module owns no second registry.
//! DATA disposition stays local; tests: physical_flow/tests and compatibility_f_flow.

use super::*;

// Original-loop physical half of the F transaction. It owns no second flow map.
// The marker is stored in the actual SocketCtx, with its exact fake-IP capability.
use super::super::receive_window::ReceiveWindow;
use super::super::supervision::FlowRoles;
use super::super::transaction::{FlowIdentity, FlowStage};
use crate::fake_ip::CheckedFakeIpLease;

#[derive(Debug)]
pub(in crate::client_tun) struct FlowMirror {
    pub(super) id: FlowIdentity,
    lease: Option<CheckedFakeIpLease>,
    published_bytes: Option<usize>,
    pub(in crate::client_tun) producer_stopped: bool,
    data_messages: usize,
    data_bytes: usize,
    transferred: (usize, usize),
    discarded: (usize, usize),
    abandoned_pending: usize,
    hints: usize,
    event_in_flight: bool,
    pub(in crate::client_tun) quarantined: bool,
    rearm_started: bool,
    pub(super) reset: Option<LocalReset>,
    receipt: Option<FlowPhysicalReceipt>,
    window: Option<ReceiveWindow>,
}

#[derive(Clone, Copy, Debug)]
pub(in crate::client_tun::compatibility_runtime::flow_custody) struct FlowPhysicalReceipt {
    id: FlowIdentity,
    next_epoch: u64,
    data_messages: usize,
    data_bytes: usize,
    transferred: (usize, usize),
    discarded: (usize, usize),
    discarded_escrow: usize,
    discarded_downlink: usize,
    discarded_uplink: usize,
    discarded_socket_rx: usize,
    discarded_socket_tx: usize,
    fake_release: Option<crate::fake_ip::FakeIpLeaseReceipt>,
}
impl FlowPhysicalReceipt {
    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn data_disposition(
        self,
    ) -> (usize, usize, usize, usize, usize) {
        (
            self.data_messages,
            self.data_bytes,
            self.transferred.1,
            self.discarded.1,
            self.discarded_downlink,
        )
    }
    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn matches_flow(
        self,
        flow: &super::super::transaction::FlowTransaction,
    ) -> bool {
        let published = flow.roles.as_ref().and_then(FlowRoles::published);
        let (messages, bytes) = published.map_or((0, 0), |value| (value.messages, value.bytes));
        let fake_matches = match (flow.fake_ip, self.fake_release) {
            (None, None) => true,
            (Some(acquired), Some(released)) => {
                acquired.ip == released.ip
                    && acquired.incarnation == released.incarnation
                    && acquired.acquisition == released.acquisition
                    && released.after.checked_add(1) == Some(released.before)
            }
            _ => false,
        };
        self.id == flow.id
            && flow.id.epoch.checked_add(1) == Some(self.next_epoch)
            && self.data_messages == messages
            && self.data_bytes == bytes
            && self.transferred.0.checked_add(self.discarded.0) == Some(self.data_messages)
            && self.transferred.1.checked_add(self.discarded.1) == Some(self.data_bytes)
            && self.discarded_escrow == flow.escrow_bytes
            && flow.escrow_bytes == flow.escrow.len()
            && self.discarded_uplink == 0
            && self.discarded_downlink <= self.transferred.1
            && fake_matches
            && self
                .discarded_downlink
                .checked_add(self.discarded_socket_rx)
                .and_then(|n| n.checked_add(self.discarded_socket_tx))
                .is_some()
    }
    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn identity(
        self,
    ) -> FlowIdentity {
        self.id
    }
}

impl FlowMirror {
    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn live_lease(
        &self,
    ) -> Option<&CheckedFakeIpLease> {
        if self.receipt.is_some() {
            None
        } else {
            self.lease.as_ref()
        }
    }
    pub(super) fn matches_epoch(&self, epoch: u64) -> bool {
        self.receipt.map_or(epoch == self.id.epoch, |receipt| {
            receipt.id == self.id
                && self.id.epoch.checked_add(1) == Some(receipt.next_epoch)
                && epoch == receipt.next_epoch
        })
    }
    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn matches_slot(
        &self,
        slot: &super::super::transaction::OpenSlot,
    ) -> bool {
        slot.matches_physical(self.id)
    }
    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn matches_root(
        &self,
        opens: &super::super::transaction::OpenCustody,
    ) -> bool {
        opens
            .slots
            .get(self.id.index)
            .and_then(Option::as_ref)
            .is_some_and(|slot| self.matches_slot(slot))
    }

    pub(in crate::client_tun) fn admit_pending(
        &mut self,
        before: usize,
        after: usize,
        n: usize,
        generic: bool,
    ) {
        if let Some(window) = &mut self.window {
            if generic {
                window.admit(before, after, n);
            } else {
                window.fail("non-Generic receive-window admission");
            }
        }
    }

    pub(in crate::client_tun) fn publish_window(&mut self, pending: usize) {
        if self.event_in_flight || self.quarantined || self.producer_stopped {
            return;
        }
        if let Some(window) = &mut self.window {
            window.audit_storage(self.transferred.1, pending, self.abandoned_pending);
            window.publish();
        }
    }

    pub(in crate::client_tun) fn window_credit(&self) -> RelayReadCredit {
        let paused = self.producer_stopped
            || self.quarantined
            || self.window.as_ref().is_none_or(ReceiveWindow::failed);
        RelayReadCredit {
            paused,
            max_batch_bytes: if paused { 0 } else { 65_536 },
        }
    }

    pub(in crate::client_tun) fn begin_event(&mut self, epoch: u64, bytes: Option<usize>) -> bool {
        if self.quarantined
            || self.event_in_flight
            || self.id.epoch != epoch
            || self.receipt.is_some()
        {
            self.quarantined = true;
            return false;
        }
        let totals = match bytes {
            Some(bytes) => self
                .data_messages
                .checked_add(1)
                .zip(self.data_bytes.checked_add(bytes)),
            None => self.hints.checked_add(1).map(|hints| (hints, 0)),
        };
        let Some((messages, bytes_total)) = totals else {
            self.quarantined = true;
            return false;
        };
        if bytes.is_some() {
            self.data_messages = messages;
            self.data_bytes = bytes_total;
            #[cfg(test)]
            if let Some(window) = &self.window {
                window.observe_consumed(self.data_bytes);
            }
        } else {
            self.hints = messages;
        }
        self.event_in_flight = true;
        true
    }

    pub(in crate::client_tun) fn accept_data(
        ctx: &mut SocketCtx,
        payload: &[u8],
        permit: Option<DownstreamBytePermit>,
    ) {
        ctx.append_downlink_pending_with_permit(payload, permit);
        if let Some(mirror) = &mut ctx.flow_mirror {
            mirror.record_disposition(payload.len(), false);
        }
    }

    pub(in crate::client_tun) fn discard_data(&mut self, bytes: usize) {
        self.record_disposition(bytes, true);
    }

    fn record_disposition(&mut self, bytes: usize, discarded: bool) {
        let count = if discarded {
            &mut self.discarded
        } else {
            &mut self.transferred
        };
        if let Some(next) = count.0.checked_add(1).zip(count.1.checked_add(bytes)) {
            *count = next;
        } else {
            self.quarantined = true;
        }
        if !self.event_in_flight
            || self.transferred.0.checked_add(self.discarded.0) != Some(self.data_messages)
            || self.transferred.1.checked_add(self.discarded.1) != Some(self.data_bytes)
        {
            self.quarantined = true;
        }
    }

    pub(in crate::client_tun) fn finish_event(&mut self) {
        if !self.event_in_flight
            || self.transferred.0.checked_add(self.discarded.0) != Some(self.data_messages)
            || self.transferred.1.checked_add(self.discarded.1) != Some(self.data_bytes)
        {
            self.quarantined = true;
        }
        self.event_in_flight = false;
    }
}

pub(in crate::client_tun) struct FlowAdmission<'a, 'storage> {
    pub(in crate::client_tun) handle: SocketHandle,
    pub(in crate::client_tun) socket: &'a mut TcpSocket<'storage>,
    pub(in crate::client_tun) ctx: &'a mut SocketCtx,
    pub(in crate::client_tun) fake_pool: &'a mut FakeIpPool,
    pub(in crate::client_tun) fake_ip: Option<std::net::Ipv4Addr>,
    pub(in crate::client_tun) target: TargetAddr,
    pub(in crate::client_tun) now: u64,
}

impl OpenDispatch {
    pub(in crate::client_tun) fn admit_flow<U: OwnedUpstream + 'static>(
        &self,
        upstream: Arc<U>,
        request: FlowAdmission<'_, '_>,
    ) {
        let FlowAdmission {
            handle,
            socket,
            ctx,
            fake_pool,
            fake_ip,
            target,
            now,
        } = request;
        if ctx.flow_mirror.is_some()
            || ctx.local_reset.is_some()
            || !socket.can_recv()
            || ctx.state != SocketState::Listening
        {
            return;
        }
        let Some(id) = self.reserve_flow(upstream, target, handle, ctx.conn_epoch) else {
            return;
        };
        // resolve_target used this same exclusive pool borrow without an await.
        // An acquire error is an integrity failure, not retryable W pressure.
        let acquired = fake_ip
            .map(|ip| fake_pool.acquire_checked(ip, now))
            .transpose();
        let failure = acquired.as_ref().err().copied();
        let lease = acquired.ok().flatten();
        let Some(owner) = &self.owner else {
            return;
        };
        let mut opens = owner.opens();
        let Some(flow) = opens.flow_mut(id) else {
            owner.ledger().quarantined = true;
            return;
        };
        flow.fake_ip = lease.as_ref().map(CheckedFakeIpLease::receipt);
        flow.fake_ip_failure = fake_ip.zip(failure);
        // Store physical custody before the recv callback can unwind.
        ctx.flow_mirror = Some(FlowMirror {
            id,
            lease,
            published_bytes: None,
            producer_stopped: false,
            data_messages: 0,
            data_bytes: 0,
            transferred: (0, 0),
            discarded: (0, 0),
            abandoned_pending: 0,
            hints: 0,
            event_in_flight: false,
            quarantined: failure.is_some(),
            rearm_started: false,
            reset: None,
            receipt: None,
            window: None,
        });
        flow.physical = true;
        ctx.conn_epoch = id.epoch;
        ctx.state = SocketState::HandshakePending;
        ctx.pending_open_mode = Some(TcpOwnershipMode::LegacyRelay);
        if failure.is_some() {
            flow.stage = FlowStage::Quarantined;
            owner.ledger().quarantined = true;
            owner.record(crate::shared::errors::RetainedFailure::from_io(
                super::super::transaction::open_origin(
                    id.index,
                    id.epoch,
                    crate::shared::errors::FailurePhase::TcpOpen,
                ),
                std::io::ErrorKind::InvalidData.into(),
            ));
            return;
        }
        let result = socket.recv(|bytes| {
            let len = bytes.len().min(flow.reserved);
            flow.escrow.extend_from_slice(&bytes[..len]);
            flow.escrow_bytes = flow.escrow.len();
            (len, ())
        });
        ctx.fake_ip = fake_ip;
        if result.is_err() {
            flow.close(("local", "recv_failed"));
        } else {
            flow.stage = FlowStage::Waiting;
        }
        let bytes = flow.escrow_bytes;
        if let Some(accounting) = &mut opens.flow {
            accounting.escrow_bytes += bytes;
        }
    }

    pub(in crate::client_tun) fn service_flow(
        &self,
        id: FlowIdentity,
        completion: &mut crate::client_tun::HandshakeCompletion<'_, '_>,
    ) -> bool {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.service_flow_inner(id, completion)
        })) {
            Ok(result) => {
                if let Some(owner) = &self.owner {
                    if let Some(flow) = owner.opens().flow_mut(id) {
                        if flow.stage == FlowStage::Quarantined {
                            if let Some(roles) = &mut flow.roles {
                                roles.stop();
                            }
                        }
                    }
                }
                result
            }
            Err(panic) => {
                if let Some(owner) = &self.owner {
                    if let Some(flow) = owner.opens().flow_mut(id) {
                        flow.panic = Some(panic);
                        flow.stage = FlowStage::Quarantined;
                        if let Some(roles) = &mut flow.roles {
                            roles.stop();
                        }
                    }
                    owner.ledger().quarantined = true;
                }
                false
            }
        }
    }

    fn service_flow_inner(
        &self,
        id: FlowIdentity,
        completion: &mut crate::client_tun::legacy_event_loop::activity::HandshakeCompletion<
            '_,
            '_,
        >,
    ) -> bool {
        let Some(owner) = &self.owner else {
            return false;
        };
        let mut opens = owner.opens();
        #[cfg(test)]
        if tests::window::control_probe(&mut opens, id, completion) {
            return false;
        }
        // Validate the original physical donor before any control mutation or ACK.
        // A corrupt identity never selects another row or physical container.
        if !opens
            .slots
            .get(id.index)
            .and_then(Option::as_ref)
            .is_some_and(|slot| slot.matches_physical(id))
        {
            if let Some(flow) = opens.flow_mut(id) {
                flow.stage = FlowStage::Quarantined;
            }
            if let Some(mirror) = completion
                .socket_ctxs
                .get_mut(&id.handle)
                .and_then(|ctx| ctx.flow_mirror.as_mut())
                .filter(|mirror| mirror.id == id)
            {
                mirror.quarantined = true;
            }
            owner.ledger().quarantined = true;
            return false;
        }
        let Some(flow) = opens.flow_mut(id) else {
            return false;
        };
        let Some(ctx) = completion.socket_ctxs.get_mut(&id.handle) else {
            flow.stage = FlowStage::Quarantined;
            owner.ledger().quarantined = true;
            return false;
        };
        // Generic has no protocol-owned/native permits to discard. Reject any
        // unexpected physical authority instead of invoking its unknown Drop.
        if ctx.local_reset.is_some()
            || ctx.resumable_uplink.is_some()
            || ctx.resumable_sink.is_some()
            || ctx.resumable_half_close.is_some()
            || ctx.resumable_terminal.is_some()
            || ctx.resumable_action_bridge.is_some()
            || ctx.d16_downlink_queue.is_some()
            || !ctx.downlink_pending_permits.is_empty()
            || !ctx.downlink_inflight_permits.is_empty()
        {
            flow.stage = FlowStage::Quarantined;
            owner.ledger().quarantined = true;
            return false;
        }
        let Some(mirror) = &mut ctx.flow_mirror else {
            flow.stage = FlowStage::Quarantined;
            owner.ledger().quarantined = true;
            return false;
        };
        if mirror.id != id || !mirror.matches_epoch(ctx.conn_epoch) {
            flow.stage = FlowStage::Quarantined;
            owner.ledger().quarantined = true;
            return false;
        }
        if let Some(window) = &mut mirror.window {
            let pending = mirror.receipt.map_or_else(
                || {
                    ctx.downlink_pending
                        .len()
                        .checked_add(mirror.abandoned_pending)
                },
                |receipt| Some(receipt.discarded_downlink),
            );
            match pending {
                Some(pending) => window.audit_storage(mirror.transferred.1, pending, 0),
                None => window.fail("receive-window pending overflow"),
            }
            if let Some(error) = window.take_error() {
                owner.record(crate::shared::errors::RetainedFailure::from_io(
                    super::super::transaction::open_origin(
                        id.index,
                        id.epoch,
                        crate::shared::errors::FailurePhase::TcpOpen,
                    ),
                    error,
                ));
                flow.close(("root", "receive_window_failed"));
            }
        }
        if mirror.quarantined
            || mirror.event_in_flight
            || (mirror.rearm_started && mirror.receipt.is_none())
        {
            flow.stage = FlowStage::Quarantined;
        }
        if flow.stage == FlowStage::Quarantined {
            mirror.producer_stopped = true;
            ctx.uplink_tx = None;
            ctx.relay_read_credit_tx = None;
            flow.producer_stopped = true;
            if let Some(roles) = &mut flow.roles {
                roles.stop();
            }
            owner.ledger().quarantined = true;
            return false;
        }
        if let Some(receipt) = mirror.receipt {
            if receipt.identity() != id
                || owner.ledger().first_egress_io.is_some()
                || mirror.window.as_ref().is_some_and(ReceiveWindow::failed)
            {
                mirror.quarantined = true;
                flow.stage = FlowStage::Quarantined;
                owner.ledger().quarantined = true;
                return false;
            }
            drop(opens);
            if self.release_flow(receipt) {
                ctx.flow_mirror = None;
                return true;
            }
            return false;
        }
        if matches!(flow.stage, FlowStage::Closing | FlowStage::Rearming)
            && !completion.sockets.get::<TcpSocket>(id.handle).is_active()
        {
            let Some(discarded) = mirror
                .abandoned_pending
                .checked_add(ctx.downlink_pending.len())
            else {
                flow.stage = FlowStage::Quarantined;
                return false;
            };
            mirror.abandoned_pending = discarded;
            ctx.downlink_pending = Vec::new();
        }
        if flow.stage == FlowStage::Ready {
            if self.relay_policy().generic_engine() != TcpRelayEngine::Legacy {
                flow.close(("policy", "generic_engine_not_yet_custodied"));
                return true;
            }
            let Some(attempt) = &mut flow.attempt else {
                return false;
            };
            if flow.carrier.is_none() {
                flow.carrier = attempt.take_carrier(tokio::time::Instant::now().into_std());
            }
            if flow.carrier.is_none() {
                match attempt.view().phase {
                    crate::upstream::TcpOpenPhase::Ready => return false,
                    crate::upstream::TcpOpenPhase::Retiring
                    | crate::upstream::TcpOpenPhase::Settled => {
                        flow.close(("remote_open", "carrier_failed"));
                        return true;
                    }
                    _ => {
                        flow.stage = FlowStage::Quarantined;
                        return false;
                    }
                }
            }
            if !matches!(
                flow.carrier,
                Some(OpenedOwnedTcp::Legacy(OpenedTcpRelay::Generic(_)))
            ) {
                flow.stage = FlowStage::Quarantined;
                owner.ledger().quarantined = true;
                return false;
            }
            let (tx, rx) = mpsc::channel(RELAY_CHANNEL_CAPACITY);
            let (credit_tx, credit_rx) = watch::channel(initial_relay_read_credit(false));
            let Some(OpenedOwnedTcp::Legacy(OpenedTcpRelay::Generic(stream))) = flow.carrier.take()
            else {
                return false;
            };
            flow.roles = Some(FlowRoles::new(
                super::super::supervision::GenericLaunchRequest {
                    engine: TcpRelayEngine::Legacy,
                    handle: id.handle,
                    epoch: id.epoch,
                    stream,
                    rx,
                    back_tx: completion.global_tx.clone(),
                    read_credit_rx: credit_rx,
                    downlink_backpressure: completion.downlink_backpressure,
                },
            ));
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
            if let Some(index) = flow.missing_start_gate.take() {
                roles.drop_start_gate_for_test(index);
            }
            if roles.quarantined() {
                flow.stage = FlowStage::Quarantined;
                owner.ledger().quarantined = true;
                return false;
            }
            if ctx.uplink_tx.is_some()
                || ctx.relay_read_credit_tx.is_some()
                || !ctx.uplink_buffer.is_empty()
                || mirror.published_bytes.is_some()
            {
                flow.stage = FlowStage::Quarantined;
                return false;
            }
            // The fresh channel has a single original producer and closed worker
            // gates. Keep escrow until the synchronous physical ACK is committed.
            if tx
                .try_send(RelayCommand::Data(flow.escrow.clone()))
                .is_err()
            {
                flow.stage = FlowStage::Quarantined;
                return false;
            }
            ctx.uplink_tx = Some(tx);
            ctx.relay_read_credit_tx = Some(credit_tx);
            ctx.state = SocketState::Relaying;
            ctx.pending_open_mode = None;
            mirror.published_bytes = Some(flow.escrow.len());
            flow.published = true;
            flow.stage = FlowStage::Published;
            let reserved = flow.reserved;
            let escrow_bytes = flow.escrow_bytes;
            flow.escrow = Vec::new();
            flow.escrow_bytes = 0;
            flow.reserved = 0;
            flow.w_held = false;
            if let Some(accounting) = &mut opens.flow {
                accounting.w -= 1;
                accounting.bytes -= reserved;
                accounting.escrow_bytes -= escrow_bytes;
                accounting.published += 1;
            }
            if let Some(flow) = opens.flow_mut(id) {
                if let Some(roles) = &mut flow.roles {
                    roles.start();
                }
            }
            return true;
        }
        if flow.stage == FlowStage::Closing {
            mirror.producer_stopped = true;
            ctx.uplink_tx = None;
            ctx.relay_read_credit_tx = None;
            flow.producer_stopped = true;
            if let Some(roles) = &mut flow.roles {
                roles.stop();
            }
            return true;
        }
        if flow.stage != FlowStage::Rearming {
            return false;
        }
        let (messages, bytes) = flow
            .roles
            .as_ref()
            .and_then(FlowRoles::published)
            .map_or((0, 0), |receipt| (receipt.messages, receipt.bytes));
        let hints = flow
            .roles
            .as_ref()
            .and_then(FlowRoles::published_hints)
            .unwrap_or(0);
        if mirror.data_messages != messages || mirror.data_bytes != bytes || mirror.hints != hints {
            return true;
        }
        if let Some(window) = &mut mirror.window {
            match flow.roles.as_ref().and_then(FlowRoles::read_total) {
                Some(read) => window.audit_reader(read),
                None => window.fail("receive-window joined reader missing"),
            }
            if window.failed() {
                if let Some(error) = window.take_error() {
                    owner.record(crate::shared::errors::RetainedFailure::from_io(
                        super::super::transaction::open_origin(
                            id.index,
                            id.epoch,
                            crate::shared::errors::FailurePhase::TcpOpen,
                        ),
                        error,
                    ));
                }
                mirror.quarantined = true;
                flow.stage = FlowStage::Quarantined;
                owner.ledger().quarantined = true;
                return false;
            }
        }
        // A shared device write has uncertain physical disposition. Only seal
        // quarantine after actual role/provider retirement and DATA accounting;
        // early Quarantined would skip the provider's Closing settlement.
        if owner.ledger().first_egress_io.is_some() {
            mirror.quarantined = true;
            flow.stage = FlowStage::Quarantined;
            owner.ledger().quarantined = true;
            return false;
        }
        let socket = completion.sockets.get_mut::<TcpSocket>(id.handle);
        let shutdown = owner
            .stop
            .stopping
            .load(std::sync::atomic::Ordering::Acquire);
        if !shutdown && flow.close_reason == ("remote_to_local", "remote_eof") {
            if !ctx.downlink_pending.is_empty() {
                return true;
            }
            if !ctx.local_eof_sent {
                socket.close();
                ctx.local_eof_sent = true;
            }
            if socket.is_active() {
                return true;
            }
        }
        if !shutdown
            && socket.is_active()
            && (!ctx.downlink_pending.is_empty() || socket.send_queue() != 0)
        {
            return true;
        }
        let Some(next_epoch) = ctx.conn_epoch.checked_add(1) else {
            mirror.quarantined = true;
            return false;
        };
        let Some(deadline) = flow.stop_deadline else {
            mirror.quarantined = true;
            flow.stage = FlowStage::Quarantined;
            owner.ledger().quarantined = true;
            return false;
        };
        if !mirror.prepare_reset(
            ctx.conn_epoch,
            ctx.local_port,
            &mut ctx.state,
            socket,
            tokio::time::Instant::from_std(deadline),
        ) {
            if mirror.quarantined {
                flow.stage = FlowStage::Quarantined;
                owner.ledger().quarantined = true;
            }
            return true;
        }
        let release = match mirror
            .lease
            .as_ref()
            .map(|lease| {
                completion
                    .fake_pool
                    .prepare_checked_release(lease, completion.now_secs)
            })
            .transpose()
        {
            Ok(release) => release,
            Err(_) => {
                mirror.quarantined = true;
                owner.ledger().quarantined = true;
                return false;
            }
        };
        let Some(discarded_downlink) = mirror
            .abandoned_pending
            .checked_add(ctx.downlink_pending.len())
        else {
            mirror.quarantined = true;
            return false;
        };
        let mut receipt = FlowPhysicalReceipt {
            id,
            next_epoch,
            data_messages: messages,
            data_bytes: bytes,
            transferred: mirror.transferred,
            discarded: mirror.discarded,
            discarded_escrow: flow.escrow_bytes,
            discarded_downlink,
            discarded_uplink: ctx.uplink_buffer.len(),
            discarded_socket_rx: socket.recv_queue(),
            discarded_socket_tx: socket.send_queue(),
            fake_release: None,
        };
        mirror.rearm_started = true;
        socket.abort();
        configure_local_tcp_socket(socket);
        if socket.listen(ctx.local_port).is_err() {
            mirror.quarantined = true;
            owner.ledger().quarantined = true;
            return false;
        }
        receipt.fake_release = release.map(|release| release.commit());
        mirror.reset = None;
        mirror.receipt = Some(receipt);
        // Retain the physical receipt until root acknowledges the exact old ID.
        let retained_mirror = ctx.flow_mirror.take();
        let mut next = SocketCtx::new(ctx.local_port);
        next.conn_epoch = next_epoch;
        next.flow_mirror = retained_mirror;
        *ctx = next;
        drop(opens);
        #[cfg(test)]
        if tests::defer_receipt(owner, id) {
            return false;
        }
        if self.release_flow(receipt) {
            #[cfg(test)]
            if let Some(accounting) = &mut owner.opens().flow {
                // Observe only actual new-mint ACKs; never drive settlement.
                accounting.last_fake_release = receipt
                    .fake_release
                    .map(|release| (id, release, completion.fake_pool.usage()));
            }
            ctx.flow_mirror = None;
            return true;
        }
        false
    }
}

/// Fatal control only: borrows the original physical inventory, never replaces
/// it. A poisoned DATA turn may retain one selected event plus the bounded lane.
/// No receipt, socket reset, fake-IP release, retry credit or publication occurs.
pub(in crate::client_tun) struct FatalPhysicalTurn<'a, 'storage> {
    sockets: &'a SocketSet<'storage>,
    contexts: &'a mut HashMap<SocketHandle, SocketCtx>,
    fake_pool: &'a FakeIpPool,
    lane: &'a mut RelayEventLane,
    selected: &'a mut Option<(SocketHandle, RelayEvent)>,
    scanned: usize,
    pending: bool,
    uncertain: bool,
    lane_blocked: bool,
}

pub(in crate::client_tun) enum FatalStep {
    Yield,
    Waiting,
    Retained,
}

impl<'a, 'storage> FatalPhysicalTurn<'a, 'storage> {
    pub(in crate::client_tun) fn new(
        sockets: &'a SocketSet<'storage>,
        contexts: &'a mut HashMap<SocketHandle, SocketCtx>,
        fake_pool: &'a FakeIpPool,
        lane: &'a mut RelayEventLane,
        selected: &'a mut Option<(SocketHandle, RelayEvent)>,
    ) -> Self {
        Self {
            sockets,
            contexts,
            fake_pool,
            lane,
            selected,
            scanned: 0,
            pending: false,
            uncertain: false,
            lane_blocked: false,
        }
    }

    // Only a proven, unstarted Generic event can be discarded. In-flight or
    // unexpected capability ownership is retained without guessing disposition.
    fn discard_selected(&mut self, opens: &super::super::transaction::OpenCustody) -> bool {
        let Some((handle, event)) = self.selected.as_ref() else {
            return true;
        };
        let Some(ctx) = self.contexts.get_mut(handle) else {
            return false;
        };
        let Some(mirror) = &mut ctx.flow_mirror else {
            return false;
        };
        let bytes = match event {
            RelayEvent::Data {
                epoch,
                bytes,
                downstream_permit: None,
                ..
            } if *epoch == ctx.conn_epoch => Some(bytes.len()),
            RelayEvent::AckDrainHint { epoch, .. } if *epoch == ctx.conn_epoch => None,
            _ => return false,
        };
        let bound_generic = opens
            .slots
            .get(mirror.id.index)
            .and_then(Option::as_ref)
            .is_some_and(|slot| {
                slot.handle == *handle
                    && slot.epoch == ctx.conn_epoch
                    && matches!(&slot.phase, super::super::transaction::OpenPhase::Flow(flow)
                    if flow.id == mirror.id && flow.physical && flow.published
                        && flow.roles.is_some() && flow.carrier.is_none())
            });
        if !bound_generic
            || !mirror.matches_root(opens)
            || mirror.id.handle != *handle
            || mirror.quarantined
            || mirror.event_in_flight
            || mirror.receipt.is_some()
            || !mirror.begin_event(ctx.conn_epoch, bytes)
        {
            return false;
        }
        if let Some(bytes) = bytes {
            mirror.discard_data(bytes);
        }
        mirror.finish_event();
        if mirror.quarantined {
            return false;
        }
        *self.selected = None;
        true
    }
}

impl OpenDispatch {
    pub(in crate::client_tun) fn poll_fatal(
        &self,
        cx: &mut std::task::Context<'_>,
        physical: &mut FatalPhysicalTurn<'_, '_>,
    ) -> std::task::Poll<FatalStep> {
        use super::super::transaction::OpenPhase;
        use crate::upstream::TcpOpenPhase;
        use std::panic::{AssertUnwindSafe, catch_unwind};
        use std::task::Poll;
        let Some(owner) = &self.owner else {
            return Poll::Pending;
        };
        owner.request_stop();
        let mut opens = owner.opens();
        let len = opens.slots.len();
        // Stop/join gets half each turn, even when the DATA lane is full.
        for _ in 0..16 {
            if physical.scanned == len || !owner.take_control() {
                break;
            }
            let index = opens.cursor;
            opens.cursor = (index + 1) % len;
            physical.scanned += 1;
            let Some(slot) = &mut opens.slots[index] else {
                continue;
            };
            let OpenPhase::Flow(flow) = &mut slot.phase else {
                physical.uncertain = true;
                continue;
            };
            // Never enter a provider again after an unknown unwind. Every
            // callback in a fresh row, including view and endpoint drop, shares
            // this guard; its payload remains in that same original flow slot.
            if flow.panic.is_some() {
                if let Some(roles) = &mut flow.roles {
                    roles.poll_fatal_joins(cx);
                }
                physical.pending = true;
                physical.uncertain = true;
                continue;
            }
            let settled = catch_unwind(AssertUnwindSafe(|| {
                flow.close(("root", "loop_panic"));
                if let Some(ctx) = physical.contexts.get_mut(&flow.id.handle) {
                    // Execution has ended permanently. Remove the original producer
                    // endpoints before marking it stopped, even if its mirror is bad.
                    ctx.uplink_tx = None;
                    ctx.relay_read_credit_tx = None;
                    if let Some(mirror) = &mut ctx.flow_mirror {
                        mirror.producer_stopped = true;
                        physical.uncertain |= mirror.id != flow.id
                            || mirror.quarantined
                            || mirror.event_in_flight
                            || ctx.conn_epoch != flow.id.epoch;
                    } else {
                        physical.uncertain = true;
                    }
                } else {
                    physical.uncertain = true;
                }
                flow.producer_stopped = true;
                if let Some(roles) = &mut flow.roles {
                    roles.stop();
                }

                flow.drive(cx);
                let roles_settled = flow.roles.as_ref().is_none_or(FlowRoles::settled);
                let provider_settled = flow.attempt.as_ref().is_none_or(|attempt| {
                    let view = attempt.view();
                    view.phase == TcpOpenPhase::Settled
                        && !view.lease_held
                        && !view.retirement_pending
                });
                roles_settled && provider_settled && flow.carrier.is_none()
            }));
            match settled {
                Ok(settled) => physical.pending |= !settled,
                Err(payload) => {
                    flow.panic = Some(payload);
                    flow.stage = FlowStage::Quarantined;
                    physical.pending = true;
                    physical.uncertain = true;
                }
            }
            physical.uncertain |= flow.stage == FlowStage::Quarantined;
        }
        // Keep rows in the original slots. Unknown selected DATA blocks further
        // receives, so there is no second quarantine queue or unbounded capture.
        let mut empty = false;
        for _ in 0..16 {
            if physical.lane_blocked || !owner.take_control() {
                break;
            }
            if physical.selected.is_none() {
                match physical.lane.try_recv_retained() {
                    Ok(event) => *physical.selected = Some(event),
                    Err(_) => {
                        empty = true;
                        break;
                    }
                }
            }
            if !physical.discard_selected(&opens) {
                physical.uncertain = true;
                physical.lane_blocked = true;
                break;
            }
        }
        if physical.scanned != len {
            return Poll::Ready(FatalStep::Yield);
        }
        let expired = owner
            .ledger()
            .shutdown_deadline
            .is_some_and(|deadline| tokio::time::Instant::now() >= deadline);
        if !expired && (physical.pending || (!empty && !physical.lane_blocked)) {
            physical.scanned = 0;
            physical.pending = false;
            return Poll::Pending;
        }
        // A once-only cold control census: both directions of the original
        // physical/ledger relation, independently summed DATA and actual leases.
        physical.uncertain |= !opens.audit_flows()
            || physical
                .contexts
                .values()
                .filter_map(|ctx| ctx.flow_mirror.as_ref())
                .any(|mirror| !mirror.matches_root(&opens))
            || !physical.fake_pool.validate_checked_leases(
                physical
                    .contexts
                    .values()
                    .filter_map(|ctx| ctx.flow_mirror.as_ref())
                    .filter_map(FlowMirror::live_lease),
            );
        #[cfg(all(test, feature = "harness"))]
        let mut census = [(0usize, 0usize); 3];
        #[cfg(all(test, feature = "harness"))]
        let mut in_flight = 0usize;
        for slot in opens.slots.iter().flatten() {
            let OpenPhase::Flow(flow) = &slot.phase else {
                continue;
            };
            let mirror = physical
                .contexts
                .get(&flow.id.handle)
                .and_then(|ctx| ctx.flow_mirror.as_ref());
            physical.uncertain |= !mirror.is_some_and(|m| m.matches_slot(slot));
            if let Some(mirror) = mirror {
                let published = flow.roles.as_ref().and_then(FlowRoles::published);
                let (messages, bytes) = published.map_or((0, 0), |v| (v.messages, v.bytes));
                #[cfg(all(test, feature = "harness"))]
                {
                    in_flight += usize::from(mirror.event_in_flight);
                }
                #[cfg(all(test, feature = "harness"))]
                for (total, value) in
                    census
                        .iter_mut()
                        .zip([(messages, bytes), mirror.transferred, mirror.discarded])
                {
                    total.0 = total.0.saturating_add(value.0);
                    total.1 = total.1.saturating_add(value.1);
                }
                let hints = flow
                    .roles
                    .as_ref()
                    .and_then(FlowRoles::published_hints)
                    .unwrap_or(0);
                physical.uncertain |= mirror.data_messages != messages
                    || mirror.data_bytes != bytes
                    || mirror.hints != hints
                    || mirror.event_in_flight
                    || mirror.transferred.0.checked_add(mirror.discarded.0)
                        != Some(mirror.data_messages)
                    || mirror.transferred.1.checked_add(mirror.discarded.1)
                        != Some(mirror.data_bytes);
            }
        }
        let physical_flows = physical
            .sockets
            .iter()
            .filter(|(handle, _)| {
                physical
                    .contexts
                    .get(handle)
                    .is_some_and(|ctx| ctx.flow_mirror.is_some())
            })
            .count();
        let mirrors = physical
            .contexts
            .values()
            .filter(|ctx| ctx.flow_mirror.is_some())
            .count();
        physical.uncertain |= physical_flows != mirrors;
        // Panic invalidates the physical receipt even if all known work joined.
        // Keep every slot/mirror/payload/permit/lease and the original device.
        for slot in opens.slots.iter_mut().flatten() {
            if let OpenPhase::Flow(flow) = &mut slot.phase {
                flow.stage = FlowStage::Quarantined;
            }
        }
        #[cfg(all(test, feature = "harness"))]
        {
            self.observe_selected_relay(physical.selected);
            if let Some(bridge) = &self.relay_event_test_bridge {
                bridge.observe_fatal_census(census, in_flight);
            }
        }
        owner.ledger().quarantined = true;
        Poll::Ready(FatalStep::Retained)
    }
}

#[cfg(test)]
pub(in crate::client_tun::compatibility_runtime::flow_custody) mod tests;
