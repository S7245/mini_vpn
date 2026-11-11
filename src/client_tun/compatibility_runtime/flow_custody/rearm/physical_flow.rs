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
use crate::shared::errors::{FailurePhase, RetainedFailure};

mod d16;
mod launch;
pub(in crate::client_tun::compatibility_runtime::flow_custody) mod cold;
mod native;
mod permit;
mod receipt;

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
    permit_queue: Option<permit::Binding>,
    d16_queue: Option<d16::Binding>,
    native_backing: Option<bytes::BackingMeter>,
    native_stock: Option<crate::tcp_downlink_pump::StockKey>,
}
impl FlowMirror {
    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn producer_admission_matches(ctx: &SocketCtx, id: FlowIdentity) -> bool {
        ctx.conn_epoch == id.epoch && ctx.flow_mirror.as_ref().is_some_and(|mirror| mirror.id == id && !mirror.producer_stopped)
    }
    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn producer_return_matches(ctx: &SocketCtx, id: FlowIdentity) -> bool {
        Self::producer_admission_matches(ctx, id) && ctx.uplink_tx.is_none() && ctx.relay_read_credit_tx.is_none()
    }
    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn commit_producer_return(ctx: &mut SocketCtx, id: FlowIdentity) -> bool {
        if !Self::producer_return_matches(ctx, id) { return false; }
        if let Some(mirror) = &mut ctx.flow_mirror { mirror.producer_stopped = true; }
        true
    }
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
    discarded_inflight: usize,
    discarded_uplink: usize,
    discarded_socket_rx: usize,
    discarded_socket_tx: usize,
    #[cfg(all(test, feature = "harness"))]
    permit_reset: Option<PermitResetObservation>,
    fake_release: Option<crate::fake_ip::FakeIpLeaseReceipt>,
    d16: Option<d16::Receipt>,
    native_pump: Option<crate::upstream::NativePumpReceipt>,
    native_direct_received: Option<usize>,
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
        self.receipt.as_ref().map_or(epoch == self.id.epoch, |receipt| {
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

    pub(in crate::client_tun) fn preflight_pending_admission(
        &mut self,
        epoch: u64,
        pending: usize,
        limit: usize,
        generic: bool,
    ) -> bool {
        let generic = generic || self.egress_bound();
        if self.quarantined
            || !self.matches_epoch(epoch)
            || self
                .window
                .as_ref()
                .is_some_and(|w| !generic || !w.can_admit(pending, limit))
        {
            self.quarantined = true;
            return false;
        }
        true
    }

    pub(in crate::client_tun) fn admit_pending(
        &mut self,
        before: usize,
        after: usize,
        n: usize,
        generic: bool,
    ) {
        let generic = generic || self.egress_bound();
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
            let transferred = self.transferred.1.checked_add(
                self.d16_queue
                    .as_ref()
                    .map_or(0, |binding| binding.transferred_bytes),
            );
            match transferred {
                Some(transferred) => {
                    window.audit_storage(transferred, pending, self.abandoned_pending)
                }
                None => window.fail("D16 transfer accounting overflow"),
            }
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
        permit: &mut Option<DownstreamBytePermit>,
    ) -> bool {
        if !ctx.append_downlink_pending_with_permit(payload, permit) {
            return false;
        }
        if let Some(mirror) = &mut ctx.flow_mirror {
            mirror.record_disposition(payload.len(), false);
        }
        true
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
        let Some(id) = self.reserve_flow(upstream, target, handle, ctx.conn_epoch) else { return; };
        let Some(owner) = &self.owner else { return; };
        let mut preparation_error = None;
        let mut prepared = if let Some(meter) = owner.app_meter.as_ref().filter(|m| m.budget_limit().is_some()) {
            let copied = match socket.peek(MAX_UPLINK_BUFFER) {
                Ok(bytes) => {
                    #[cfg(test)]
                    if let Some(error) = tests::cold::initial_copy_refusal(owner, id) {
                        Err(error)
                    } else { crate::client_tun::uplink::Payload::copy_native(bytes, meter) }
                    #[cfg(not(test))]
                    crate::client_tun::uplink::Payload::copy_native(bytes, meter)
                },
                Err(_) => { let _ = self.rollback_unreceived(id); return; }
            };
            match copied {
                Ok(value) => Some(value),
                Err(bytes::MeteredCopyError::BudgetFull) => { let _ = self.rollback_unreceived(id); return; }
                Err(error) => { preparation_error = Some(error); None }
            }
        } else { None };
        let parent_deadline = crate::shared::errors::QuietFailureSink::deadline_quiet(&**owner)
            .map(tokio::time::Instant::into_std);
        // The actual source is the typed value. This temporary diagnostic carrier
        // is created and destroyed outside opens; the original cause stays in F.
        let preparation_fact = preparation_error.filter(|error| *error != bytes::MeteredCopyError::Quarantined).map(|error| {
            RetainedFailure::from_io(
                super::super::transaction::open_origin(id.index, id.epoch, FailurePhase::TcpOpen),
                crate::client_tun::uplink::admission_error(error),
            )
        });
        // resolve_target used this same exclusive pool borrow without an await.
        // An acquire error is an integrity failure, not retryable W pressure.
        let acquired = if preparation_error.is_some() { Ok(None) } else {
            fake_ip.map(|ip| fake_pool.acquire_checked(ip, now)).transpose()
        };
        let failure = acquired.as_ref().err().copied();
        let lease = acquired.ok().flatten();
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
            permit_queue: None,
            d16_queue: None,
            native_backing: None, native_stock: None,
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
        if let Some(error) = preparation_error {
            if error == bytes::MeteredCopyError::Quarantined {
                flow.stage = FlowStage::Quarantined;
                drop(opens);
                owner.quarantine_quiet();
                let deadline = crate::shared::errors::QuietFailureSink::deadline_quiet(&**owner)
                    .map(tokio::time::Instant::into_std);
                let mut opens = owner.opens();
                if !opens.slots.get(id.index).and_then(Option::as_ref).is_some_and(|slot| slot.matches_physical(id)) {
                    return; // Already quiet/Q; a stale physical row cannot be updated.
                }
                if let Some(flow) = opens.flow_mut(id) {
                    if let Some(deadline) = deadline {
                        flow.stop_deadline.limit(deadline);
                    }
                }
                return;
            }
            flow.close(("native_uplink", "escrow_admission_failed"));
            if let Some(parent) = parent_deadline {
                flow.stop_deadline.limit(parent);
            }
            if let Some(fact) = preparation_fact {
                owner.record(fact);
                flow.rejection = Some(super::super::transaction::FlowRejection::ReportedNativeUplink(error));
            }
            drop(opens);
            return;
        }
        let result = socket.recv(|bytes| {
            let len = bytes.len().min(flow.reserved);
            if let Some(value) = prepared.take() {
                // No await or socket mutation occurred since the exact peek.
                flow.escrow = value;
            } else if let crate::client_tun::uplink::Payload::Legacy(value) = &mut flow.escrow {
                value.extend_from_slice(&bytes[..len]);
            }
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
                        if flow.stage == FlowStage::Quarantined && flow.panic.is_none() {
                            if let Some(roles) = &mut flow.roles {
                                roles.stop();
                            }
                        }
                    }
                }
                result
            }
            Err(panic) => {
                let mut panic = Some(panic);
                if let Some(owner) = &self.owner {
                    if let Some(flow) = owner.opens().flow_mut(id) {
                        if let Some(payload) = panic.take() { flow.retain_unwind(payload); }
                    }
                    owner.note_flow_unwind(id);
                }
                std::mem::forget(panic); // Lost identity retains its original captured Box as unknown custody.
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
        flow.clear_app_tail_wait();
        if flow.preparing || flow.roles.as_ref().is_some_and(FlowRoles::disposition_in_flight) { return false; }
        if flow.panic.is_some() && owner.app_meter.as_ref().is_some_and(|meter| meter.budget_limit().is_some()) {
            return false; // Hard opaque work is permanently closed; original fatal turns still JOIN.
        }
        let Some(ctx) = completion.socket_ctxs.get_mut(&id.handle) else {
            flow.stage = FlowStage::Quarantined;
            owner.ledger().quarantined = true;
            return false;
        };
        if flow.panic.is_some() && flow.roles.is_none()
            && (ctx.uplink_tx.is_some() || ctx.relay_read_credit_tx.is_some()
                || ctx.flow_mirror.as_ref().is_some_and(|mirror| mirror.window.is_some())) {
            drop(opens); owner.quarantine_quiet(); return false;
        }
        let native_selected = flow.roles.as_ref().is_some_and(FlowRoles::is_native)
            || matches!(
                flow.carrier,
                Some(OpenedOwnedTcp::Legacy(OpenedTcpRelay::Native(_)))
            );
        let d16_selected = flow.roles.as_ref().is_some_and(FlowRoles::is_d16)
            || matches!(
                flow.carrier,
                Some(OpenedOwnedTcp::Legacy(OpenedTcpRelay::NativeByteOwned(_)))
            )
            || (flow.stage == FlowStage::Ready
                && self.relay_policy().tuic_native_override()
                    == crate::upstream::TuicNativeOverride::D16DirectOrdered);
        let ordered = if d16_selected {
            true
        } else if native_selected {
            flow.roles.as_ref().and_then(FlowRoles::permit_mode)
                == Some(DownstreamPermitReleaseMode::OnEgressDrain)
                || (flow.roles.is_none()
                    && self.relay_policy().native_engine()
                        == crate::upstream::NativeTcpRelayEngine::PermitPump)
        } else {
            self.relay_policy().generic_engine() == TcpRelayEngine::OrderedEgressPermitPump
        };
        // Protocol/native authority remains forbidden. Ordered custody is bound
        // bidirectionally to the same four-role queue before any mutation.
        if ctx.local_reset.is_some()
            || ctx.resumable_uplink.is_some()
            || ctx.resumable_sink.is_some()
            || ctx.resumable_half_close.is_some()
            || ctx.resumable_terminal.is_some()
            || ctx.resumable_action_bridge.is_some()
            || (ctx.d16_downlink_queue.is_some() && !d16_selected)
            || (flow.stage == FlowStage::Ready
                && !FlowMirror::validate_ordered_custody(ctx, flow.roles.as_ref(), flow.published))
            || (!ordered
                && (!ctx.downlink_inflight_permits.is_empty()
                    || ctx.downlink_delay_permit_release_until_egress))
            || (ordered
                && !FlowMirror::validate_ordered_custody(ctx, flow.roles.as_ref(), flow.published))
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
        if mirror.id != id
            || !mirror.matches_epoch(ctx.conn_epoch)
            || !mirror.permit_binding_matches(flow.roles.as_ref())
            || !mirror
                .pending_permits_match(ctx.downlink_pending.len(), &ctx.downlink_pending_permits)
        {
            flow.stage = FlowStage::Quarantined;
            owner.ledger().quarantined = true;
            return false;
        }
        let window_transferred = mirror.window_transferred_bytes();
        if let Some(window) = &mut mirror.window {
            let pending = mirror.receipt.as_ref().map_or_else(
                || {
                    ctx.downlink_pending
                        .len()
                        .checked_add(mirror.abandoned_pending)
                },
                |receipt| Some(receipt.discarded_downlink),
            );
            match pending {
                Some(pending) => match window_transferred {
                    Some(transferred) => window.audit_storage(transferred, pending, 0),
                    None => window.fail("D16 transfer accounting overflow"),
                },
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
        // D72/D75: a hard Native reader terminal (EOF or read failure/RESET)
        // arrives on the monitor/signal path and can overtake DATA the dispatcher
        // already queued in the relay lane. Keep the producers (so
        // relay_allows_remote_payload) until the mirror has begun every published
        // DATA message, the exact witness Rearming uses below. This waits on the
        // App draining (D147 close tail: progress re-arms, idle expiry aborts
        // the tail and discards it below); stop bypasses.
        if flow.stage == FlowStage::Closing
            && !flow.producer_stopped
            && !flow.tail.aborted()
            && matches!(flow.close_reason, ("remote_to_local", "remote_eof" | "remote_read_failed"))
            && !owner.stop.stopping.load(std::sync::atomic::Ordering::Acquire)
            && flow.roles.as_ref().is_some_and(|roles| roles.hard_source_producers()
                && roles.published().is_none_or(|sent| mirror.data_messages < sent.messages))
        {
            let unbegun = flow
                .roles
                .as_ref()
                .and_then(FlowRoles::published)
                .map_or(0, |sent| sent.bytes.saturating_sub(mirror.data_bytes));
            let socket = completion.sockets.get::<TcpSocket>(id.handle).send_queue();
            flow.app_tail_wait(
                unbegun
                    .saturating_add(ctx.downlink_pending.len())
                    .saturating_add(ctx.downlink_inflight_permits.remaining_bytes())
                    .saturating_add(socket),
            );
            return true;
        }
        if matches!(flow.stage, FlowStage::Quarantined | FlowStage::Closing)
            && !flow.producer_stopped && flow.roles.as_ref().is_some_and(FlowRoles::hard_source_producers)
        {
            let turn = flow.roles.as_mut().and_then(|roles| roles.take_native_producers(owner, id, ctx));
            drop(opens);
            if let Some(turn) = turn {
                turn.dispose_producers(owner, &mut std::task::Context::from_waker(std::task::Waker::noop()), ctx);
            } else { owner.quarantine_quiet(); }
            return false;
        }
        if flow.producer_stopped && flow.roles.as_ref().is_some_and(FlowRoles::hard_source_producers)
            && (ctx.uplink_tx.is_some() || ctx.relay_read_credit_tx.is_some()) {
            flow.stage = FlowStage::Quarantined;
            drop(opens); owner.quarantine_quiet(); return false;
        }
        if flow.stage == FlowStage::Quarantined {
            mirror.producer_stopped = true;
            drop(ctx.uplink_tx.take());
            drop(ctx.relay_read_credit_tx.take());
            flow.producer_stopped = true;
            if let Some(roles) = flow.roles.as_mut().filter(|_| flow.panic.is_none()) {
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
            let retired = flow.roles.as_ref().is_none_or(FlowRoles::settled);
            drop(opens);
            if retired && self.release_flow(receipt) {
                ctx.flow_mirror = None;
                return true;
            }
            return false;
        }
        if !ordered
            && matches!(flow.stage, FlowStage::Closing | FlowStage::Rearming)
            && !completion.sockets.get::<TcpSocket>(id.handle).is_active()
        {
            if !mirror.dispose_pending(&mut ctx.downlink_pending, &mut ctx.downlink_pending_permits)
            {
                flow.stage = FlowStage::Quarantined;
                owner.ledger().quarantined = true;
                return false;
            }
        }

        if flow.stage == FlowStage::Ready {
            return self.prepare_ready(owner, opens, id, completion);
        }
        if flow.stage == FlowStage::Closing {
            mirror.producer_stopped = true;
            ctx.uplink_tx = None;
            ctx.relay_read_credit_tx = None;
            flow.producer_stopped = true;
            if let Some(roles) = &mut flow.roles {
                roles.stop();
                if d16_selected
                    && roles.join_count() == 3
                    && !mirror.discard_d16_queue_after_joins(roles, flow.close_reason)
                {
                    flow.stage = FlowStage::Quarantined;
                    owner.ledger().quarantined = true;
                    return false;
                }
                if d16_selected
                    && roles.join_count() == 3
                    && mirror.d16_reader_disposition_matches(
                        roles,
                        ctx.downlink_pending.len(),
                        ctx.downlink_pending_permits.is_empty(),
                        ctx.downlink_inflight_permits.is_empty(),
                    )
                    && !roles.dispose_d16_reader_after_physical_proof()
                {
                    flow.stage = FlowStage::Quarantined;
                    owner.ledger().quarantined = true;
                    return false;
                }
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
        // D147: these waits are App-only (custody already settled above); an
        // aborted tail skips them and the reset below RSTs the App socket.
        let tail_open = !shutdown && !flow.tail.aborted();
        let outstanding = ctx
            .downlink_pending
            .len()
            .saturating_add(ctx.downlink_inflight_permits.remaining_bytes())
            .saturating_add(socket.send_queue());
        if tail_open && flow.close_reason == ("remote_to_local", "remote_eof") {
            if !ctx.downlink_pending.is_empty() {
                flow.app_tail_wait(outstanding);
                return true;
            }
            if !ctx.local_eof_sent {
                socket.close();
                ctx.local_eof_sent = true;
            }
            if socket.is_active() {
                flow.app_tail_wait(outstanding);
                return true;
            }
        }
        if tail_open
            && socket.is_active()
            && (!ctx.downlink_pending.is_empty() || socket.send_queue() != 0)
        {
            flow.app_tail_wait(outstanding);
            return true;
        }
        if !ordered
            && (!mirror
                .dispose_pending(&mut ctx.downlink_pending, &mut ctx.downlink_pending_permits)
                || !ctx.downlink_inflight_permits.is_empty()
                || !mirror.permit_physical_zero(flow.roles.as_ref()))
        {
            mirror.quarantined = true;
            flow.stage = FlowStage::Quarantined;
            owner.ledger().quarantined = true;
            return false;
        }
        if !mirror.native_backing_matches(flow.roles.as_ref()) {
            mirror.quarantined = true;
            owner.ledger().quarantined = true;
            return false;
        }
        let Some(next_epoch) = ctx.conn_epoch.checked_add(1) else {
            mirror.quarantined = true;
            return false;
        };
        let Some(deadline) = *flow.stop_deadline else {
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
        // Ordered external caps cannot settle until this original reset has
        // actually dispatched/flushed. The same marker remains in this ctx.
        // A legal pre-publication W has no queue/roles/window or DATA. The
        // header proves that complete empty state; only bound Egress owns caps.
        let precleared_d16_inflight = mirror
            .d16_queue
            .as_ref()
            .and_then(|binding| binding.terminal_preclear_inflight);
        #[cfg(all(test, feature = "harness"))]
        let permit_reset_pre = tests::permit_reset::capture_pre(
            &ctx.downlink_inflight_permits,
            ctx.downlink_pending.len(),
            mirror,
        );
        let discarded_inflight = if let Some(bytes) = precleared_d16_inflight {
            bytes
        } else if ordered && mirror.egress_bound() {
            let bytes = ctx.downlink_inflight_permit_bytes();
            if !ctx.discard_ordered_for_reset() {
                flow.stage = FlowStage::Quarantined;
                owner.ledger().quarantined = true;
                return false;
            }
            bytes
        } else {
            0
        };
        let Some(mirror) = &mut ctx.flow_mirror else {
            return false;
        };
        #[cfg(all(test, feature = "harness"))]
        let permit_reset = tests::permit_reset::capture_post(
            &ctx.downlink_inflight_permits,
            mirror,
            permit_reset_pre,
            discarded_inflight,
        );
        if ordered
            && (!ctx.downlink_inflight_permits.is_empty()
                || !mirror.permit_physical_zero(flow.roles.as_ref()))
        {
            mirror.quarantined = true;
            flow.stage = FlowStage::Quarantined;
            owner.ledger().quarantined = true;
            return false;
        }
        let d16_receipt = if d16_selected {
            let Some(receipt) = flow.roles.as_ref().and_then(|roles| {
                mirror.d16_receipt(
                    ctx.d16_downlink_queue.as_ref(),
                    ctx.downlink_pending.len(),
                    ctx.downlink_pending_permits.is_empty(),
                    ctx.downlink_inflight_permits.is_empty(),
                    roles,
                )
            }) else {
                mirror.quarantined = true;
                flow.stage = FlowStage::Quarantined;
                owner.ledger().quarantined = true;
                return false;
            };
            Some(receipt)
        } else {
            None
        };
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
            discarded_inflight,
            discarded_uplink: ctx.uplink_buffer.len(),
            discarded_socket_rx: socket.recv_queue(),
            discarded_socket_tx: socket.send_queue(),
            #[cfg(all(test, feature = "harness"))]
            permit_reset,
            fake_release: None,
            d16: d16_receipt,
            native_pump: flow.roles.as_ref().and_then(FlowRoles::native_pump_receipt),
            native_direct_received: flow
                .roles
                .as_ref()
                .and_then(FlowRoles::native_direct_source_received_bytes),
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
        let retired = flow.roles.as_ref().is_none_or(FlowRoles::settled);
        drop(opens);
        #[cfg(test)]
        if tests::defer_receipt(owner, id) {
            return false;
        }
        if retired && self.release_flow(receipt) {
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
        #[cfg(test)]
        self.observe_ordered_physical(physical.contexts, physical.sockets);
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
            let Some(slot) = opens.slots.get_mut(index) else {
                continue;
            };
            let envelope = (slot.handle, slot.epoch);
            let OpenPhase::Flow(flow) = &mut slot.phase else {
                physical.uncertain = true;
                continue;
            };
            if flow.roles.as_ref().is_some_and(FlowRoles::disposition_in_flight) {
                physical.pending = true;
                continue;
            }
            // Never enter a provider again after an unknown unwind. Every
            // callback in a fresh row, including view and endpoint drop, shares
            // this guard; its payload remains in that same original flow slot.
            if flow.panic.is_some() {
                if let Some(roles) = &mut flow.roles {
                    roles.poll_fatal_joins(cx);
                }
                physical.pending = true;
                physical.uncertain = true;
                let disposal = flow.roles.as_mut().and_then(|roles| {
                    roles.all_original_roles_joined().then(|| roles.take_original_disposal(owner, flow.id)).flatten()
                        .or_else(|| roles.take_native_disposal(owner, flow.id, flow.producer_stopped))
                });
                if let Some(disposal) = disposal {
                    drop(opens);
                    disposal.dispose(owner, cx);
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
                continue;
            }
            if !flow.producer_stopped && flow.roles.as_ref().is_some_and(FlowRoles::hard_source_producers) {
                if !flow.physical || envelope != (flow.id.handle, flow.id.epoch) {
                    flow.stage = FlowStage::Quarantined;
                    drop(opens); owner.quarantine_quiet(); return Poll::Pending;
                }
                flow.close(("root", "loop_panic"));
                let Some(ctx) = physical.contexts.get_mut(&flow.id.handle) else {
                    flow.stage = FlowStage::Quarantined;
                    drop(opens); owner.quarantine_quiet(); return Poll::Pending;
                };
                let turn = flow.roles.as_mut().and_then(|roles| roles.take_native_producers(owner, flow.id, ctx));
                drop(opens);
                if let Some(turn) = turn { turn.dispose_producers(owner, cx, ctx); }
                else { owner.quarantine_quiet(); }
                physical.pending = true;
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            let settled = catch_unwind(AssertUnwindSafe(|| {
                flow.close(("root", "loop_panic"));
                if let Some(ctx) = physical.contexts.get_mut(&flow.id.handle) {
                    // Execution has ended permanently. Remove the original producer
                    // endpoints before marking it stopped, even if its mirror is bad.
                    if !flow.producer_stopped {
                        ctx.uplink_tx = None;
                        ctx.relay_read_credit_tx = None;
                    }
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
                if flow.preparing || flow.cold_retained.is_some() { return false; }
                let roles_settled = flow.roles.as_ref().is_none_or(FlowRoles::settled);
                let provider_settled = flow.attempt.as_ref().is_none_or(|attempt| {
                    let view = attempt.view();
                    view.phase == TcpOpenPhase::Settled
                        && !view.lease_held
                        && !view.retirement_pending
                });
                !flow.preparing && roles_settled && provider_settled && flow.carrier.is_none()
            }));
            match settled {
                Ok(settled) => physical.pending |= !settled,
                Err(payload) => {
                    flow.retain_unwind(payload);
                    physical.pending = true;
                    physical.uncertain = true;
                    let id = flow.id;
                    drop(opens);
                    owner.note_flow_unwind(id);
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
            }
            let disposal = flow.roles.as_mut().and_then(|roles| {
                roles.all_original_roles_joined().then(|| roles.take_original_disposal(owner, flow.id)).flatten()
                    .or_else(|| roles.take_native_disposal(owner, flow.id, flow.producer_stopped))
            });
            if let Some(disposal) = disposal {
                physical.pending = true;
                drop(opens);
                disposal.dispose(owner, cx);
                cx.waker().wake_by_ref();
                return Poll::Pending;
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
        #[cfg(all(test, feature = "harness"))]
        let mut census_complete = true;
        for slot in opens.slots.iter().flatten() {
            let OpenPhase::Flow(flow) = &slot.phase else {
                continue;
            };
            if flow.roles.as_ref().is_some_and(FlowRoles::disposition_in_flight) {
                physical.uncertain = true; // Actual outcome is retained in its original turn, never absent/zero.
                #[cfg(all(test, feature = "harness"))]
                { census_complete = false; }
                continue;
            }
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
        for (_, slot) in opens.slots.occupied_mut() {
            if let OpenPhase::Flow(flow) = &mut slot.phase {
                flow.stage = FlowStage::Quarantined;
            }
        }
        #[cfg(all(test, feature = "harness"))]
        {
            self.observe_selected_relay(physical.selected);
            if let Some(bridge) = &self.relay_event_test_bridge {
                if census_complete { bridge.observe_fatal_census(census, in_flight); }
            }
        }
        owner.ledger().quarantined = true;
        Poll::Ready(FatalStep::Retained)
    }
}

#[cfg(test)]
pub(in crate::client_tun::compatibility_runtime::flow_custody) mod tests;
#[cfg(all(test, feature = "harness"))]
pub(in crate::client_tun::compatibility_runtime::flow_custody) use tests::permit_reset::PermitResetObservation;

// Main calls this outside opens after a typed admission refusal. The original
// physical Flow settles its own cause; no Session-wide Device error is invented.
impl OpenDispatch {
    pub(in crate::client_tun) fn refuse_native_uplink(&self, ctx: &mut SocketCtx, error: bytes::MeteredCopyError) {
        let Some(owner) = &self.owner else { return; };
        let Some(id) = ctx.flow_mirror.as_ref().map(|mirror| mirror.id) else { owner.quarantine_quiet(); return; };
        let Some(tx) = ctx.uplink_tx.as_ref() else { owner.quarantine_quiet(); return; };
        let Some((_, clock)) = tx.native_context() else { owner.quarantine_quiet(); return; };
        let clock = clock.clone();
        tx.close_native_admission();
        let mut opens = owner.opens();
        if !opens.slots.get(id.index).and_then(Option::as_ref).is_some_and(|slot| slot.matches_physical(id))
            || !FlowMirror::producer_admission_matches(ctx, id) {
            drop(opens); clock.quarantine(); return;
        }
        let Some(flow) = opens.flow_mut(id) else { drop(opens); clock.quarantine(); return; };
        if error != bytes::MeteredCopyError::Quarantined && flow.stage != FlowStage::Published { return; }
        if error == bytes::MeteredCopyError::Quarantined { flow.stage = FlowStage::Quarantined; }
        else { flow.close(("native_uplink", "admission_failed")); }
        drop(opens);
        if error == bytes::MeteredCopyError::Quarantined { clock.quarantine(); }
        else {
            let source = crate::client_tun::uplink::admission_error(error);
            clock.io(&source);
            drop(source);
        }
        let deadline = crate::shared::errors::QuietFailureSink::deadline_quiet(&**owner)
            .into_iter().chain(clock.parent_deadline_for_stop()).min().map(tokio::time::Instant::into_std);
        let mut opens = owner.opens();
        if !opens.slots.get(id.index).and_then(Option::as_ref).is_some_and(|slot| slot.matches_physical(id)) { return; }
        if let Some(flow) = opens.flow_mut(id) {
            if error != bytes::MeteredCopyError::Quarantined && flow.rejection.is_none() {
                flow.rejection = Some(super::super::transaction::FlowRejection::ReportedNativeUplink(error));
            }
            if let Some(deadline) = deadline { flow.stop_deadline.limit(deadline); }
        }
    }
}
