//! Harness-only relay-event injection and passive observation.
//!
//! Owns test-only weak-sender access, one Copy truncate fault and snapshots; no production
//! sender, task, timer, SocketCtx, session/flow ledger, or physical authority.
//! Entries: `RelayEventTestKind::from_event` and `RelayEventTestBridge::{install_sender,
//! try_send, snapshot, observe}`, through the parent's restricted re-export.
//! Tests: `client_tun::compatibility_runtime::tests::open_custody::events` and
//! `client_tun::compatibility_runtime::tests::flow_custody::rearm_custody`.

mod stock_alias;
mod capacity;

use super::{
    DownlinkCreditController, RelayEvent, RelayGapAckDrainState, SocketHandle, SocketState,
};
use crate::client_tun::SocketCtx;
use crate::client_tun::downlink_custody::{
    EgressLedgerSnapshot, PollBatch, test_poll_lost_binding,
};
use crate::tcp_downlink_pump::{
    AsyncLeasedByteFlowQueue, DownstreamBytePermit, PermitQueueReleaseAudit,
};
use smoltcp::iface::SocketSet;
use std::collections::HashMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::client_tun) enum ForeignEgressPhase {
    Flush,
    Ack,
}

struct ForeignEgressFault {
    handle: SocketHandle,
    epoch: u64,
    phase: ForeignEgressPhase,
    cap: DownstreamBytePermit,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::client_tun) struct ForeignEgressFired {
    pub(in crate::client_tun) phase: ForeignEgressPhase,
    pub(in crate::client_tun) stage: &'static str,
    pub(in crate::client_tun) same_cycle_dead_transition: bool,
    pub(in crate::client_tun) original_bytes: usize,
    pub(in crate::client_tun) foreign_bytes: usize,
}

#[derive(Clone, Copy, Debug, Default)]
pub(in crate::client_tun) struct ForeignEgressDiag {
    pub(in crate::client_tun) visits: usize,
    pub(in crate::client_tun) visits_with_inflight: usize,
    pub(in crate::client_tun) visits_dead_with_inflight: usize,
    pub(in crate::client_tun) last_stage: Option<&'static str>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::client_tun) enum EgressProbePhase {
    BeforePoll,
    AfterFlush,
    AfterAck,
    BeforeUdpFlush,
    AfterUdpFlush,
    ShutdownCensus,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::client_tun) struct EgressProbeSample {
    pub handle: SocketHandle,
    pub phase: EgressProbePhase,
    pub ledger: EgressLedgerSnapshot,
    pub physical_caps: Option<(usize, usize)>,
    pub mirror_receipt: Option<bool>,
}

#[derive(Clone, Copy, Debug)]
pub(in crate::client_tun) struct PendingPermitCensus {
    pub bytes: usize,
    pub permit_bytes: usize,
    pub count: usize,
    pub same_queue: bool,
}

struct EgressProbe {
    queue: AsyncLeasedByteFlowQueue,
    samples: Vec<EgressProbeSample>,
    distinct_flush_only: bool,
    overflow: bool,
    ambiguous: bool,
    shutdown: Option<EgressProbeSample>,
    shutdown_audit: Option<PermitQueueReleaseAudit>,
    shutdown_pending: Option<PendingPermitCensus>,
    shutdown_visits: usize,
    shutdown_ambiguous: bool,
}

#[cfg(all(test, feature = "harness"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::client_tun) enum RelayEventTestKind {
    Data,
    DataReady,
    AckDrainHint,
    Closed,
}

#[cfg(all(test, feature = "harness"))]
impl RelayEventTestKind {
    pub(super) fn from_event(event: &RelayEvent) -> (Self, u64) {
        match event {
            RelayEvent::Data { epoch, .. } => (Self::Data, *epoch),
            RelayEvent::DataReady { epoch } => (Self::DataReady, *epoch),
            RelayEvent::AckDrainHint { epoch, .. } => (Self::AckDrainHint, *epoch),
            RelayEvent::Closed(close) => (Self::Closed, close.epoch),
        }
    }
}

#[cfg(all(test, feature = "harness"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::client_tun) struct RelayEventTestContext {
    pub(in crate::client_tun) epoch: u64,
    pub(in crate::client_tun) state: SocketState,
    pub(in crate::client_tun) has_uplink_tx: bool,
    pub(in crate::client_tun) pending_downlink_bytes: usize,
    pub(in crate::client_tun) read_credit: DownlinkCreditController,
}

#[cfg(all(test, feature = "harness"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::client_tun) struct RelayEventTestSnapshot {
    pub(in crate::client_tun) generation: u64,
    pub(in crate::client_tun) handle: SocketHandle,
    pub(in crate::client_tun) kind: RelayEventTestKind,
    pub(in crate::client_tun) event_epoch: u64,
    pub(in crate::client_tun) rejected: bool,
    pub(in crate::client_tun) context: Option<RelayEventTestContext>,
    pub(in crate::client_tun) dirty: bool,
    pub(in crate::client_tun) d4_deadline: Option<std::time::Instant>,
    pub(in crate::client_tun) relay_gap_ack_drain: RelayGapAckDrainState,
}

#[cfg(all(test, feature = "harness"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::client_tun) enum RelayEventTestSendResult {
    Accepted,
    NotInstalled,
    Closed,
    Full,
}

#[cfg(all(test, feature = "harness"))]
impl RelayEventTestSendResult {
    pub(in crate::client_tun) fn is_accepted(self) -> bool {
        matches!(self, Self::Accepted)
    }
}

#[derive(Clone, Copy)]
struct SelectedData {
    handle: SocketHandle,
    queue_matches_probe: Option<bool>,
    epoch: u64,
    len: usize,
    pointer: usize,
    transferred: usize,
    capacity: usize,
    permit: Option<(usize, crate::tcp_downlink_pump::DownstreamPermitReleaseMode)>,
}

#[derive(Default)]
struct SelectedFault {
    target: Option<(SocketHandle, u64)>,
    fired: Option<(usize, usize, usize, usize, usize)>,
}

/// Harness-only access to the existing bounded channel and one targeted fault.
/// It owns only a weak sender and one overwrite-only value snapshot; the
/// event-loop-local OpenDispatch is its sole strong owner.
#[cfg(all(test, feature = "harness"))]
#[derive(Default)]
pub(in crate::client_tun) struct RelayEventTestBridge {
    sender: std::sync::Mutex<Option<crate::client_tun::relay_channel::WeakSender<(SocketHandle, RelayEvent)>>>,
    snapshot: std::sync::Mutex<Option<RelayEventTestSnapshot>>,
    generation: std::sync::atomic::AtomicU64,
    selected_data: std::sync::Mutex<Option<SelectedData>>,
    selected_fault: std::sync::Mutex<SelectedFault>,
    fatal_census: std::sync::Mutex<Option<([(usize, usize); 3], usize)>>,
    foreign_egress: std::sync::Mutex<Option<ForeignEgressFault>>,
    foreign_fired: std::sync::Mutex<Option<ForeignEgressFired>>,
    foreign_diag: std::sync::Mutex<ForeignEgressDiag>,
    egress_probe: std::sync::Mutex<Option<EgressProbe>>,
    // Fixed passive sample, never a custody or disposal authority.
    pending_probe: std::sync::Mutex<Option<(SocketHandle, u64, Option<(usize, bool)>)>>,
    stock_alias: std::sync::Mutex<stock_alias::Capture>,
    timer_probe: std::sync::Mutex<Option<(tokio::time::Instant, usize, usize)>>,
    capacity_probe: std::sync::Mutex<Option<Option<capacity::Snapshot>>>,
    // D72 tracer: withholds only the original selected receive; never drops an event.
    relay_hold: std::sync::atomic::AtomicBool,
}

#[cfg(all(test, feature = "harness"))]
impl RelayEventTestBridge {
    // Passive, bounded counts of actual original timer selections. No tick is
    // created, consumed, or rescheduled by this observer.
    pub(in crate::client_tun) fn arm_timer_probe(&self, resumed: tokio::time::Instant) {
        let mut probe = self.timer_probe.lock().unwrap();
        assert!(probe.is_none());
        *probe = Some((resumed, 0, 0));
    }
    pub(in crate::client_tun) fn observe_timer_tick(&self, scheduled: tokio::time::Instant) {
        if let Some((resumed, total, stale)) = self.timer_probe.lock().unwrap().as_mut() {
            *total = total.saturating_add(1);
            *stale = stale.saturating_add(usize::from(scheduled < *resumed));
        }
    }
    pub(in crate::client_tun) fn take_timer_probe(&self) -> (usize, usize) {
        let (_, total, stale) = self.timer_probe.lock().unwrap().take().unwrap();
        (total, stale)
    }

    pub(in crate::client_tun) fn arm_pending_probe(&self, handle: SocketHandle, epoch: u64) {
        let mut probe = self.pending_probe.lock().unwrap();
        assert!(probe.is_none());
        *probe = Some((handle, epoch, None));
    }

    pub(in crate::client_tun) fn pending_probe(&self) -> Option<(usize, bool)> {
        self.pending_probe.lock().unwrap().as_ref().and_then(|probe| probe.2)
    }

    pub(in crate::client_tun) fn observe_pending_probe(
        &self,
        sockets: &SocketSet<'_>,
        contexts: &HashMap<SocketHandle, SocketCtx>,
    ) {
        let mut probe = self.pending_probe.lock().unwrap();
        let Some((handle, epoch, sample)) = probe.as_mut() else { return; };
        // Overwrite even on absence: a previous turn cannot prove current DATA.
        *sample = contexts.get(handle).filter(|ctx| ctx.conn_epoch == *epoch).and_then(|ctx| {
            let socket = sockets.get::<smoltcp::socket::tcp::Socket>(*handle);
            (socket.is_active() && socket.may_send()).then_some((ctx.downlink_pending.len(), socket.can_send()))
        });
    }

    pub(in crate::client_tun) fn arm_egress_probe(&self, queue: AsyncLeasedByteFlowQueue) {
        self.arm_probe(queue, false);
    }
    fn arm_probe(&self, queue: AsyncLeasedByteFlowQueue, distinct_flush_only: bool) {
        let mut probe = self
            .egress_probe
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(probe.is_none());
        *probe = Some(EgressProbe {
            queue,
            samples: Vec::with_capacity(512),
            distinct_flush_only,
            overflow: false,
            ambiguous: false,
            shutdown: None,
            shutdown_audit: None,
            shutdown_pending: None,
            shutdown_visits: 0,
            shutdown_ambiguous: false,
        });
    }

    /// The partial-IO test needs each distinct actual AfterFlush state. Other
    /// observers retain full phase adjacency and replay checkpoints unchanged.
    pub(in crate::client_tun) fn arm_distinct_flush_probe(&self, queue: AsyncLeasedByteFlowQueue) {
        self.arm_probe(queue, true);
    }

    /// Remove only the test observer after required original facts were checked
    /// and copied. This removal itself is never a physical Flow receipt.
    /// Original queue/receipt authority stays in Root; Drop is outside probe lock.
    pub(in crate::client_tun) fn retire_egress_probe_for_test(&self) {
        let retired = {
            self.egress_probe.lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner).take()
        };
        assert!(retired.is_some());
        drop(retired);
    }

    // The original Root shutdown census owns this one bounded terminal view.
    // Replacing a Copy snapshot cannot grow the 512 poll-history buffer.
    pub(in crate::client_tun) fn observe_egress_shutdown(
        &self,
        contexts: &HashMap<SocketHandle, SocketCtx>,
    ) {
        let mut guard = self
            .egress_probe
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(probe) = guard.as_mut() else { return };
        probe.shutdown_visits = probe.shutdown_visits.saturating_add(1);
        let mut found = None;
        let mut pending = None;
        for (handle, ctx) in contexts {
            if let Some(ledger) = ctx
                .downlink_inflight_permits
                .snapshot_for_queue(&probe.queue)
            {
                let sample = EgressProbeSample {
                    handle: *handle,
                    phase: EgressProbePhase::ShutdownCensus,
                    ledger,
                    physical_caps: ctx.downlink_inflight_permits.actual_inflight_caps_for_test(&probe.queue),
                    mirror_receipt: ctx
                        .flow_mirror
                        .as_ref()
                        .map(|mirror| mirror.receipt_present_for_test()),
                };
                if found.replace(sample).is_some() {
                    probe.shutdown_ambiguous = true;
                }
                let permit_bytes = ctx
                    .downlink_pending_permits
                    .iter()
                    .try_fold(0usize, |total, permit| {
                        total.checked_add(permit.remaining_bytes())
                    });
                pending = Some(PendingPermitCensus {
                    bytes: ctx.downlink_pending.len(),
                    permit_bytes: permit_bytes.unwrap_or(0),
                    count: ctx.downlink_pending_permits.len(),
                    same_queue: permit_bytes.is_some()
                        && ctx.downlink_pending_permits.iter().all(|permit| {
                            probe.queue.owns_permit(permit, permit.remaining_bytes())
                        }),
                });
            }
        }
        probe.shutdown = found;
        probe.shutdown_pending = pending;
        probe.shutdown_audit = probe.queue.permit_release_audit_for_test();
    }

    pub(in crate::client_tun) fn egress_shutdown_snapshot(
        &self,
    ) -> (
        Option<EgressProbeSample>,
        Option<PermitQueueReleaseAudit>,
        Option<PendingPermitCensus>,
        usize,
        bool,
    ) {
        let guard = self
            .egress_probe
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let probe = guard.as_ref().expect("egress probe armed");
        (
            probe.shutdown,
            probe.shutdown_audit,
            probe.shutdown_pending,
            probe.shutdown_visits,
            probe.shutdown_ambiguous,
        )
    }

    pub(in crate::client_tun) fn egress_probe_samples(
        &self,
    ) -> (Vec<EgressProbeSample>, bool, bool) {
        let probe = self
            .egress_probe
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let probe = probe.as_ref().expect("egress probe armed");
        (probe.samples.clone(), probe.overflow, probe.ambiguous)
    }

    pub(in crate::client_tun) fn observe_egress_probe(
        &self,
        phase: EgressProbePhase,
        contexts: &HashMap<SocketHandle, SocketCtx>,
    ) {
        let mut guard = self
            .egress_probe
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(probe) = guard.as_mut() else {
            return;
        };
        let mut found = None;
        for (handle, ctx) in contexts {
            if let Some(ledger) = ctx
                .downlink_inflight_permits
                .snapshot_for_queue(&probe.queue)
            {
                if found
                    .replace(EgressProbeSample {
                        handle: *handle,
                        phase,
                        ledger,
                        physical_caps: ctx.downlink_inflight_permits.actual_inflight_caps_for_test(&probe.queue),
                        mirror_receipt: ctx
                            .flow_mirror
                            .as_ref()
                            .map(|mirror| mirror.receipt_present_for_test()),
                    })
                    .is_some()
                {
                    probe.ambiguous = true;
                }
            }
        }
        if probe.distinct_flush_only && phase != EgressProbePhase::AfterFlush { return; }
        if let Some(sample) = found {
            if probe.distinct_flush_only && probe.samples.last() == Some(&sample) { return; }
            if probe.samples.len() < probe.samples.capacity() {
                probe.samples.push(sample);
            } else {
                probe.overflow = true;
            }
        }
    }

    pub(in crate::client_tun) fn arm_foreign_egress(
        &self,
        handle: SocketHandle,
        epoch: u64,
        phase: ForeignEgressPhase,
        cap: DownstreamBytePermit,
    ) {
        let mut armed = self
            .foreign_egress
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(armed.is_none() && self.foreign_fired().is_none());
        *armed = Some(ForeignEgressFault {
            handle,
            epoch,
            phase,
            cap,
        });
    }

    pub(in crate::client_tun) fn foreign_fired(&self) -> Option<ForeignEgressFired> {
        *self
            .foreign_fired
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(in crate::client_tun) fn foreign_diag(&self) -> ForeignEgressDiag {
        *self
            .foreign_diag
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(in crate::client_tun) fn inject_foreign_egress(
        &self,
        phase: ForeignEgressPhase,
        stage: &str,
        sockets: &SocketSet<'_>,
        contexts: &mut HashMap<SocketHandle, SocketCtx>,
        captures: Option<&PollBatch>,
    ) {
        let mut armed = self
            .foreign_egress
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(fault) = armed.as_ref() else {
            return;
        };
        if fault.phase != phase {
            return;
        }
        let handle = fault.handle;
        let epoch = fault.epoch;
        let same_cycle_dead_transition = captures
            .is_some_and(|captures| test_poll_lost_binding(captures, handle, epoch, sockets, contexts));
        if phase == ForeignEgressPhase::Ack && !same_cycle_dead_transition {
            return;
        }
        let stage = match stage {
            "inbound_poll" => "inbound_poll",
            "tun_rx_drain" => "tun_rx_drain",
            "local_egress_service" => "local_egress_service",
            "remote_payload" => "remote_payload",
            "timer_poll" => "timer_poll",
            _ => "other",
        };
        let mut diag = self
            .foreign_diag
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        diag.visits += 1;
        diag.last_stage = Some(stage);
        let Some(ctx) = contexts.get_mut(&handle) else {
            return;
        };
        if ctx.downlink_inflight_permit_bytes() > 0 {
            diag.visits_with_inflight += 1;
            let socket = sockets.get::<smoltcp::socket::tcp::Socket>(handle);
            if !socket.is_active() && !socket.can_send() {
                diag.visits_dead_with_inflight += 1;
            }
        }
        drop(diag);
        if ctx.conn_epoch != epoch || ctx.downlink_inflight_permit_bytes() == 0 {
            return;
        }
        let socket = sockets.get::<smoltcp::socket::tcp::Socket>(handle);
        let fault = armed.take().expect("armed fault still present");
        match ctx.test_inject_foreign_dead_inflight(handle, socket, fault.cap) {
            Ok((original_bytes, foreign_bytes)) => {
                *self
                    .foreign_fired
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) =
                    Some(ForeignEgressFired {
                        phase,
                        stage,
                        same_cycle_dead_transition,
                        original_bytes,
                        foreign_bytes,
                    });
            }
            Err(cap) => *armed = Some(ForeignEgressFault { cap, ..fault }),
        }
    }

    pub(in crate::client_tun) fn arm_short_data(&self, handle: SocketHandle, epoch: u64) {
        let mut fault = self
            .selected_fault
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(fault.target.is_none() && fault.fired.is_none());
        fault.target = Some((handle, epoch));
    }

    pub(in crate::client_tun) fn selected_fault(
        &self,
    ) -> Option<(usize, usize, usize, usize, usize)> {
        self.selected_fault
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .fired
    }

    pub(in crate::client_tun) fn fault_selected(
        &self,
        selected: &mut Option<(SocketHandle, RelayEvent)>,
    ) {
        let mut fault = self
            .selected_fault
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some((
            handle,
            RelayEvent::Data {
                epoch,
                bytes,
                downstream_permit: Some(permit),
                ..
            },
        )) = selected
        else {
            return;
        };
        if fault.target != Some((*handle, *epoch)) || bytes.len() < 2 {
            return;
        }
        let before = bytes.len();
        let facts = (
            before,
            before - 1,
            bytes.as_ptr() as usize,
            bytes.capacity(),
            permit.remaining_bytes(),
        );
        match bytes {
            crate::tcp_downlink_pump::OwnedPayload::Legacy(v) => v.truncate(before - 1),
            crate::tcp_downlink_pump::OwnedPayload::Native(v) => v.truncate(before - 1),
        } // Original capability and original backing stay untouched.
        fault.target = None;
        fault.fired = Some(facts);
    }

    pub(in crate::client_tun) fn install_sender(
        &self,
        sender: crate::client_tun::relay_channel::WeakSender<(SocketHandle, RelayEvent)>,
    ) {
        *self
            .sender
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(sender);
    }

    pub(in crate::client_tun) fn queued_len(&self) -> usize {
        self.sender
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .and_then(|sender| sender.upgrade())
            .map_or(0, |sender| sender.max_capacity() - sender.capacity())
    }

    pub(in crate::client_tun) fn hold_relay_receive(&self, held: bool) {
        self.relay_hold.store(held, std::sync::atomic::Ordering::SeqCst);
    }
    pub(in crate::client_tun) fn relay_receive_held(&self) -> bool {
        self.relay_hold.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub(in crate::client_tun) fn receiver_is_open(&self) -> bool {
        self.sender
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .and_then(|sender| sender.upgrade())
            .is_some_and(|sender| !sender.is_closed())
    }

    pub(in crate::client_tun) fn fatal_census(&self) -> Option<([(usize, usize); 3], usize)> {
        *self
            .fatal_census
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(in crate::client_tun) fn observe_fatal_census(
        &self,
        census: [(usize, usize); 3],
        in_flight: usize,
    ) {
        *self
            .fatal_census
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some((census, in_flight));
    }

    pub(in crate::client_tun) fn selected_data(&self) -> Option<(u64, usize, usize, usize)> {
        self.selected_data
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .map(|v| (v.epoch, v.len, v.pointer, v.transferred))
    }

    pub(in crate::client_tun) fn selected_capability(
        &self,
    ) -> Option<(
        usize,
        Option<(usize, crate::tcp_downlink_pump::DownstreamPermitReleaseMode)>,
    )> {
        self.selected_data
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .map(|v| (v.capacity, v.permit))
    }

    pub(in crate::client_tun) fn selected_probe_custody(&self) -> Option<(SocketHandle, u64, usize, bool)> {
        self.selected_data.lock().unwrap_or_else(std::sync::PoisonError::into_inner).as_ref().map(|value| {
            (value.handle, value.epoch, value.permit.map_or(0, |p| p.0), value.queue_matches_probe == Some(true))
        })
    }

    pub(in crate::client_tun) fn observe_selected(
        &self,
        selected: &Option<(SocketHandle, RelayEvent)>,
    ) {
        self.capture_stock_alias(selected);
        let mut value = self
            .selected_data
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let probe = self.egress_probe.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let next = selected.as_ref().and_then(|(handle, event)| match event {
            RelayEvent::Data {
                epoch,
                bytes,
                downstream_permit,
                ..
            } => Some(SelectedData {
                handle: *handle,
                queue_matches_probe: downstream_permit.as_ref().and_then(|permit| probe.as_ref().map(|probe| {
                    crate::tcp_downlink_pump::PermitQueueIdentity::from_queue(&probe.queue)
                        .matches(permit, crate::tcp_downlink_pump::DownstreamPermitReleaseMode::OnEgressDrain)
                })),
                epoch: *epoch,
                len: bytes.len(),
                pointer: bytes.as_ptr() as usize,
                transferred: 0,
                capacity: bytes.capacity(),
                permit: downstream_permit
                    .as_ref()
                    .map(|p| (p.remaining_bytes(), p.release_mode())),
            }),
            _ => None,
        });
        if next.map(|v| (v.epoch, v.len, v.pointer)) != value.map(|v| (v.epoch, v.len, v.pointer)) {
            *value = next;
        } else if let (Some(value), Some(next)) = (value.as_mut(), next) {
            value.handle = next.handle;
            value.queue_matches_probe = next.queue_matches_probe;
            value.capacity = next.capacity;
            value.permit = next.permit;
        }
    }

    pub(in crate::client_tun) fn observe_payload_transfer(&self, payload: &[u8]) {
        let mut value = self
            .selected_data
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(value) = value.as_mut() {
            if value.len == payload.len() && value.pointer == payload.as_ptr() as usize {
                value.transferred = payload.len();
            }
        }
    }

    pub(in crate::client_tun) fn try_send(
        &self,
        handle: SocketHandle,
        event: RelayEvent,
    ) -> RelayEventTestSendResult {
        let sender = self
            .sender
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let Some(sender) = sender else {
            return RelayEventTestSendResult::NotInstalled;
        };
        let Some(sender) = sender.upgrade() else {
            return RelayEventTestSendResult::Closed;
        };
        match sender.try_send((handle, event)) {
            Ok(()) => RelayEventTestSendResult::Accepted,
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => RelayEventTestSendResult::Full,
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                RelayEventTestSendResult::Closed
            }
        }
    }

    pub(in crate::client_tun) fn snapshot(&self) -> Option<RelayEventTestSnapshot> {
        *self
            .snapshot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(in crate::client_tun) fn observe(&self, mut snapshot: RelayEventTestSnapshot) {
        snapshot.generation = self
            .generation
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            .saturating_add(1);
        *self
            .snapshot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(snapshot);
    }
}
