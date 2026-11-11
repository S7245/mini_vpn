//! Session-owned Legacy W and F control transactions in one fixed slot table.
//!
//! Owns admission, escrow, staged provider, direct role custody and final receipts.
//! Physical sockets/fake-IP remain in the original loop through rearm; DATA uses
//! that loop's local marker without a root lock. W and F overlap and are audited
//! independently. Tests: client_tun/tests/compatibility_* and role test children.

#[cfg(all(test, feature = "harness"))]
use super::super::super::RelayEvent;
#[cfg(all(test, feature = "harness"))]
use super::super::super::legacy_event_loop::{RelayEventTestBridge, RelayEventTestSnapshot};
use super::super::super::{
    HandshakeDone, MAX_UPLINK_BUFFER, SocketCtx, SocketHandle, SocketState, TcpSocket,
    extract_socket_payload, publish_handshake_done,
};
#[cfg(all(test, feature = "harness"))]
use super::super::tests as runtime_tests;
use super::super::{SessionControl, SessionStop};
mod release;
mod slot_table;
mod stop_deadline;
pub(in crate::client_tun::compatibility_runtime) use slot_table::SlotTable;
pub(in crate::client_tun::compatibility_runtime) mod custody;
use crate::owned_upstream::OwnedUpstream;
use crate::shared::errors::{FailureOrigin, FailurePhase, FailureResource, RetainedFailure};
use crate::upstream::RelayFamilyPolicy;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Poll;
use tokio::sync::mpsc;
pub(in crate::client_tun) enum HandshakeNotice {
    Legacy(HandshakeDone),
    Custodied(usize),
}

pub(in crate::client_tun) enum OpenEvent {
    Completion(HandshakeDone, Option<usize>),
    Yield,
    Flow(FlowIdentity),
}

pub(in crate::client_tun::compatibility_runtime) struct OpenSlot {
    pub(in crate::client_tun::compatibility_runtime) handle: SocketHandle,
    pub(in crate::client_tun::compatibility_runtime) epoch: u64,
    pub(in crate::client_tun::compatibility_runtime) deadline: tokio::time::Instant,
    pub(in crate::client_tun::compatibility_runtime) phase: OpenPhase,
}

impl OpenSlot {
    pub(super) fn matches_physical(&self, id: FlowIdentity) -> bool {
        self.handle == id.handle
            && self.epoch == id.epoch
            && matches!(&self.phase, OpenPhase::Flow(flow) if flow.id == id && flow.physical)
    }
}

pub(in crate::client_tun::compatibility_runtime) enum OpenPhase {
    Flow(Box<FlowTransaction>),
    Reserved,
    Worker(tokio::task::JoinHandle<OpenOutcome>),
    Retained(OpenOutcome),
    AwaitingDisposition(OpenOutcome),
}

#[derive(Default)]
pub(in crate::client_tun::compatibility_runtime) struct OpenOutcome {
    pub(in crate::client_tun::compatibility_runtime) opened:
        Option<crate::owned_upstream::OpenedOwnedTcp>,
    pub(in crate::client_tun::compatibility_runtime) failure: Option<RetainedFailure>,
    pub(in crate::client_tun::compatibility_runtime) additional: u64,
}

pub(in crate::client_tun::compatibility_runtime) struct OpenCustody {
    pub(super) flow: Option<FlowAccounting>,
    pub(in crate::client_tun::compatibility_runtime) slots: SlotTable,
    pub(in crate::client_tun::compatibility_runtime) live: usize,
    pub(in crate::client_tun::compatibility_runtime) cursor: usize,
    #[cfg(test)]
    pub(in crate::client_tun::compatibility_runtime) blocked:
        Option<(SocketHandle, u64, usize, u64)>,
    #[cfg(all(test, feature = "harness"))]
    pub(in crate::client_tun::compatibility_runtime) pressure: runtime_tests::PressureProbe,
}

impl Default for OpenCustody {
    fn default() -> Self {
        Self {
            flow: None,
            slots: SlotTable::with_len(128),
            live: 0,
            cursor: 0,
            #[cfg(test)]
            blocked: None,
            #[cfg(all(test, feature = "harness"))]
            pressure: runtime_tests::PressureProbe::default(),
        }
    }
}

/// The original completion channel also carries IDs for session-owned workers.
pub(in crate::client_tun) struct OpenDispatch {
    relay_policy: Arc<RelayFamilyPolicy>,
    pub(in crate::client_tun::compatibility_runtime) sender: mpsc::Sender<HandshakeNotice>,
    pub(in crate::client_tun::compatibility_runtime) owner: Option<Arc<SessionControl>>,
    #[cfg(all(test, feature = "harness"))]
    pub(in crate::client_tun::compatibility_runtime) relay_event_test_bridge:
        Option<Arc<RelayEventTestBridge>>,
}

impl OpenDispatch {
    #[cfg(all(test, feature = "harness"))]
    pub(in crate::client_tun) fn test_egress_bridge(&self) -> Option<&Arc<RelayEventTestBridge>> {
        self.relay_event_test_bridge.as_ref()
    }

    pub(in crate::client_tun) fn with_relay_policy(
        mut self,
        policy: Arc<RelayFamilyPolicy>,
    ) -> Self {
        self.relay_policy = policy;
        self
    }

    pub(in crate::client_tun) fn relay_policy(&self) -> &Arc<RelayFamilyPolicy> {
        &self.relay_policy
    }

    pub(in crate::client_tun) fn channel(
        capacity: usize,
        owner: Option<Arc<SessionControl>>,
    ) -> (Self, mpsc::Receiver<HandshakeNotice>) {
        let (sender, receiver) = mpsc::channel(capacity);
        #[cfg(all(test, feature = "harness"))]
        let relay_event_test_bridge = owner.as_ref().map(|session_control| {
            let bridge = Arc::new(RelayEventTestBridge::default());
            session_control.install_relay_event_test_bridge(Arc::downgrade(&bridge));
            bridge
        });
        (
            Self {
                relay_policy: Arc::new(RelayFamilyPolicy::default()),
                sender,
                owner,
                #[cfg(all(test, feature = "harness"))]
                relay_event_test_bridge,
            },
            receiver,
        )
    }

    #[cfg(all(test, feature = "harness"))]
    pub(in crate::client_tun) fn install_relay_event_test_bridge(
        &self,
        global_tx: &crate::client_tun::relay_channel::Sender<(SocketHandle, RelayEvent)>,
    ) {
        if let Some(bridge) = &self.relay_event_test_bridge {
            bridge.install_sender(global_tx.downgrade());
        }
    }

    #[cfg(all(test, feature = "harness"))]
    pub(in crate::client_tun) fn observe_relay_event_test(&self, snapshot: RelayEventTestSnapshot) {
        if let Some(bridge) = &self.relay_event_test_bridge {
            bridge.observe(snapshot);
        }
    }

    #[cfg(all(test, feature = "harness"))]
    pub(in crate::client_tun) fn fault_selected_relay(
        &self,
        selected: &mut Option<(SocketHandle, RelayEvent)>,
    ) {
        if let Some(bridge) = &self.relay_event_test_bridge {
            bridge.fault_selected(selected);
        }
    }

    #[cfg(all(test, feature = "harness"))]
    pub(in crate::client_tun) fn observe_selected_relay(
        &self,
        selected: &Option<(SocketHandle, RelayEvent)>,
    ) {
        if let Some(bridge) = &self.relay_event_test_bridge {
            bridge.observe_selected(selected);
        }
    }

    #[cfg(all(test, feature = "harness"))]
    pub(in crate::client_tun) fn observe_payload_transfer(&self, payload: &[u8]) {
        if let Some(bridge) = &self.relay_event_test_bridge {
            bridge.observe_payload_transfer(payload);
        }
    }

    pub(in crate::client_tun) async fn begin_turn(&self) {
        if let Some(owner) = &self.owner {
            let remaining = owner.control_remaining.swap(32, Ordering::Relaxed);
            if owner.open_started.swap(false, Ordering::Relaxed) || remaining == 0 {
                tokio::task::yield_now().await;
            }
        }
    }

    pub(in crate::client_tun) fn inline_allowed(&self) -> bool {
        self.owner.is_none()
    }

    pub(in crate::client_tun) fn extract(
        &self,
        handle: SocketHandle,
        ctx: &SocketCtx,
        socket: &mut TcpSocket<'_>,
    ) -> Option<Vec<u8>> {
        if self.flow_mode() {
            return None;
        }
        let Some(owner) = &self.owner else {
            return extract_socket_payload(socket);
        };
        if !socket.can_recv() || owner.stop.stopping.load(Ordering::Acquire) {
            return None;
        }
        let limit = MAX_UPLINK_BUFFER.saturating_sub(ctx.uplink_buffer.len());
        let pending = ctx.state == SocketState::HandshakePending;
        if !pending {
            if owner.open_started.load(Ordering::Relaxed) || !owner.take_control() {
                return None;
            }
            let mut opens = owner.opens();
            let free = opens.slots.iter().position(Option::is_none);
            if free.is_none() {
                #[cfg(all(test, feature = "harness"))]
                opens.observe_blocked(handle, ctx.conn_epoch, socket);
                return None;
            }
            if let Some(index) = free {
                // `index` is a free row of this locked table; Err is unreachable.
                if opens.slots.put(index, OpenSlot {
                    handle,
                    epoch: ctx.conn_epoch.wrapping_add(1),
                    deadline: tokio::time::Instant::now() + std::time::Duration::from_secs(20),
                    phase: OpenPhase::Reserved,
                }).is_err() {
                    owner.ledger().quarantined = true;
                    return None;
                }
                opens.live += 1;
                owner.open_started.store(true, Ordering::Relaxed);
            }
        }
        if limit == 0 {
            #[cfg(all(test, feature = "harness"))]
            self.observe_full(handle, ctx, socket);
            return None;
        }
        #[cfg(all(test, feature = "harness"))]
        let queued_before = socket.recv_queue();
        let payload = socket
            .recv(|bytes| {
                let len = bytes.len().min(limit);
                #[cfg(all(test, feature = "harness"))]
                self.observe_recv(handle, ctx, (queued_before, bytes.len(), limit, len));
                (len, bytes[..len].to_vec())
            })
            .ok();
        #[cfg(all(test, feature = "harness"))]
        self.observe_recv_after(handle, ctx, socket.recv_queue());
        payload
    }

    pub(in crate::client_tun) fn spawn<U: OwnedUpstream + 'static>(
        &self,
        upstream: Arc<U>,
        target: crate::shared::TargetAddr,
        handle: SocketHandle,
        epoch: u64,
    ) {
        if self.flow_mode() {
            return;
        }
        let diag = crate::upstream::TcpRelayOpenDiag {
            handle: format!("{handle:?}"),
            epoch,
        };
        let sender = self.sender.clone();
        if let Some(owner) = &self.owner {
            let mut opens = owner.opens();
            let Some((index, slot)) =
                opens.slots.occupied_mut().find(|(_, slot)| {
                    slot.handle == handle
                        && slot.epoch == epoch
                        && matches!(slot.phase, OpenPhase::Reserved)
                })
            else {
                owner.ledger().quarantined = true;
                owner.request_stop();
                return;
            };
            let stop = Arc::clone(&owner.stop);
            let deadline = slot.deadline;
            slot.phase = OpenPhase::Worker(tokio::spawn(async move {
                let result = guard_open(
                    run_open(upstream, target, diag, Arc::clone(&stop), deadline, index),
                    index,
                    epoch,
                )
                .await;
                tokio::select! {
                    biased;
                    _ = stop.stopped() => {},
                    _ = sender.send(HandshakeNotice::Custodied(index)) => {},
                }
                result
            }));
        } else {
            tokio::spawn(async move {
                let result = crate::upstream::with_tcp_relay_open_diag(
                    diag,
                    upstream.open_tcp_flow(&target),
                )
                .await;
                publish_handshake_done(
                    &sender,
                    HandshakeDone {
                        handle,
                        epoch,
                        result,
                    },
                )
                .await;
            });
        }
    }

    pub(in crate::client_tun) async fn recv(
        &self,
        receiver: &mut mpsc::Receiver<HandshakeNotice>,
    ) -> Option<OpenEvent> {
        if self.flow_mode() {
            return std::future::poll_fn(|cx| self.poll_flows(cx)).await;
        }
        std::future::poll_fn(|cx| {
            let mut visited = 0u128;
            loop {
                if let Some(owner) = &self.owner {
                    if !owner.take_control() {
                        return Poll::Ready(Some(OpenEvent::Yield));
                    }
                    let announced = u128::from(owner.announced[0].load(Ordering::Acquire))
                        | (u128::from(owner.announced[1].load(Ordering::Acquire)) << 64);
                    let available = announced & !visited;
                    if available != 0 {
                        let mut opens = owner.opens();
                        let index = (available.rotate_right(opens.cursor as u32).trailing_zeros()
                            as usize
                            + opens.cursor)
                            % 128;
                        opens.cursor = (index + 1) % 128;
                        visited |= 1u128 << index;
                        if let Some(slot) = opens.slots.get_mut(index) {
                            if let Some(outcome) = poll_open_slot(slot, index, cx) {
                                owner.announced[index / 64]
                                    .fetch_and(!(1u64 << (index % 64)), Ordering::Release);
                                if outcome.opened.is_some() {
                                    slot.phase = OpenPhase::Retained(outcome);
                                } else {
                                    let kind = outcome
                                        .failure
                                        .as_ref()
                                        .and_then(|failure| failure.facts().io_kind)
                                        .unwrap_or(std::io::ErrorKind::Other);
                                    slot.phase = OpenPhase::AwaitingDisposition(outcome);
                                    return Poll::Ready(Some(OpenEvent::Completion(
                                        HandshakeDone {
                                            handle: slot.handle,
                                            epoch: slot.epoch,
                                            result: Err(crate::shared::ClientError::Io(
                                                kind.into(),
                                            )),
                                        },
                                        Some(index),
                                    )));
                                }
                            }
                        } else {
                            owner.ledger().quarantined = true;
                            owner.request_stop();
                        }
                        continue;
                    }
                }
                match receiver.poll_recv(cx) {
                    Poll::Ready(Some(HandshakeNotice::Legacy(done))) => {
                        return Poll::Ready(Some(OpenEvent::Completion(done, None)));
                    }
                    Poll::Ready(Some(HandshakeNotice::Custodied(index))) => {
                        if let Some(owner) = &self.owner {
                            owner.announced[index / 64]
                                .fetch_or(1u64 << (index % 64), Ordering::Release);
                        }
                    }
                    Poll::Ready(None) => return Poll::Ready(None),
                    Poll::Pending => return Poll::Pending,
                }
            }
        })
        .await
    }

    pub(in crate::client_tun) fn consumed(&self, index: Option<usize>) {
        if self.flow_mode() {
            return;
        }
        if let (Some(owner), Some(index)) = (&self.owner, index) {
            let mut opens = owner.opens();
            if matches!(
                opens.slots[index].as_ref().map(|slot| &slot.phase),
                Some(OpenPhase::AwaitingDisposition(_))
            ) {
                if let Some(OpenSlot {
                    phase: OpenPhase::AwaitingDisposition(outcome),
                    ..
                }) = opens.slots.take(index)
                {
                    opens.live -= 1;
                    owner.record_open(outcome);
                }
            }
        }
    }
}

pub(in crate::client_tun::compatibility_runtime) fn open_origin(
    index: usize,
    epoch: u64,
    phase: FailurePhase,
) -> FailureOrigin {
    FailureOrigin {
        phase,
        resource: FailureResource::TcpOpen,
        resource_id: index as u64,
        attempt_id: Some(epoch),
    }
}

// Conversion may itself run an unknown error/panic-payload destructor. Keep that
// boundary inside the worker; no raw panic result may bypass its one notice.
async fn guard_open(
    work: impl Future<Output = OpenOutcome>,
    index: usize,
    epoch: u64,
) -> OpenOutcome {
    let mut work = Box::pin(work);
    let result = std::future::poll_fn(|cx| {
        match catch_unwind(AssertUnwindSafe(|| work.as_mut().poll(cx))) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(outcome)) => Poll::Ready(Ok(outcome)),
            Err(panic) => Poll::Ready(Err(panic)),
        }
    })
    .await;
    let mut outcome = result.unwrap_or_else(|panic| OpenOutcome {
        failure: Some(RetainedFailure::from_panic(
            open_origin(index, epoch, FailurePhase::TcpOpen),
            panic,
        )),
        ..OpenOutcome::default()
    });
    if let Err(panic) = catch_unwind(AssertUnwindSafe(|| drop(work))) {
        let failure = RetainedFailure::from_panic(
            open_origin(index, epoch, FailurePhase::TcpOpenDrop),
            panic,
        );
        if outcome.failure.is_none() {
            outcome.failure = Some(failure);
        } else {
            outcome.additional = outcome.additional.saturating_add(1);
        }
    }
    outcome
}

async fn run_open<U: OwnedUpstream + 'static>(
    upstream: Arc<U>,
    target: crate::shared::TargetAddr,
    diag: crate::upstream::TcpRelayOpenDiag,
    stop: Arc<SessionStop>,
    deadline: tokio::time::Instant,
    index: usize,
) -> OpenOutcome {
    let epoch = diag.epoch;
    let mut inner = Box::pin(crate::upstream::with_tcp_relay_open_diag(
        diag,
        async move { upstream.open_tcp_flow(&target).await },
    ));
    let result = tokio::select! {
        biased;
        _ = stop.stopped() => None,
        _ = tokio::time::sleep_until(deadline) => Some(Ok(Err(crate::shared::ClientError::Io(std::io::ErrorKind::TimedOut.into())))),
        result = std::future::poll_fn(|cx| match catch_unwind(AssertUnwindSafe(|| inner.as_mut().poll(cx))) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(result)) => Poll::Ready(Ok(result)),
            Err(panic) => Poll::Ready(Err(panic)),
        }) => Some(result),
    };
    let mut outcome = OpenOutcome::default();
    match result {
        Some(Ok(Ok(opened))) => outcome.opened = Some(opened),
        Some(Ok(Err(error))) => {
            outcome.failure = Some(RetainedFailure::from_client(
                open_origin(index, epoch, FailurePhase::TcpOpen),
                error,
            ))
        }
        Some(Err(panic)) => {
            outcome.failure = Some(RetainedFailure::from_panic(
                open_origin(index, epoch, FailurePhase::TcpOpen),
                panic,
            ))
        }
        None => {}
    }
    if let Err(panic) = catch_unwind(AssertUnwindSafe(|| drop(inner))) {
        let failure = RetainedFailure::from_panic(
            open_origin(index, epoch, FailurePhase::TcpOpenDrop),
            panic,
        );
        if outcome.failure.is_none() {
            outcome.failure = Some(failure);
        } else {
            outcome.additional = 1;
        }
    }
    outcome
}

fn poll_open_slot(
    slot: &mut OpenSlot,
    index: usize,
    cx: &mut std::task::Context<'_>,
) -> Option<OpenOutcome> {
    let OpenPhase::Worker(worker) = &mut slot.phase else {
        return None;
    };
    match Pin::new(worker).poll(cx) {
        Poll::Pending => None,
        Poll::Ready(Ok(outcome)) => Some(outcome),
        Poll::Ready(Err(error)) => Some(OpenOutcome {
            failure: Some(RetainedFailure::from_join(
                open_origin(index, slot.epoch, FailurePhase::TcpOpenJoin),
                error,
            )),
            ..OpenOutcome::default()
        }),
    }
}

// One stop-local signal distinguishes child retries from bounded-scan self-wakes.
// It owns no handles; observer replacement never recreates the settlement future.
#[derive(Default)]
struct OpenJoinWake {
    retry: AtomicBool,
    outer: Mutex<Option<std::task::Waker>>,
}

impl OpenJoinWake {
    fn register(&self, observer: &std::task::Waker) {
        let next = observer.clone();
        let previous = {
            let mut outer = self
                .outer
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            outer.replace(next)
        };
        drop(previous);
    }
}

impl std::task::Wake for OpenJoinWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.retry.store(true, Ordering::Release);
        let observer = {
            let mut outer = self
                .outer
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            outer.take()
        };
        if let Some(observer) = observer {
            observer.wake();
        }
    }
}

mod settlement;

// F control transactions share the original slot table. W and F overlap in one
// row during open/publication, so their counts are audited independently.
use super::supervision::FlowRoles;
use crate::owned_upstream::OpenedOwnedTcp;
use crate::upstream::{TcpOpenAttempt, TcpOpenCancel, TcpOpenPhase, TcpOpenRequest};

pub(super) const FLOW_LIMIT: usize = 4096;
const OPEN_LIMIT: usize = 128;
pub(super) const FLOW_SLOT_LIMIT: usize = FLOW_LIMIT + OPEN_LIMIT;
/// Configured Root geometry for the §5.3 instance bound (D25): F, F + W slots,
/// one boxed row, one slot, and the fixed occupancy index (D66). Each Root
/// slot Vec is preallocated at F + W.
pub(in crate::client_tun::compatibility_runtime) const ROOT_GEOMETRY: (usize, usize, usize, usize, usize) = (
    FLOW_LIMIT,
    FLOW_SLOT_LIMIT,
    std::mem::size_of::<FlowTransaction>(),
    std::mem::size_of::<Option<OpenSlot>>(),
    slot_table::INDEX_BYTES,
);
const ESCROW_LIMIT: usize = OPEN_LIMIT * MAX_UPLINK_BUFFER;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::client_tun) struct FlowIdentity {
    pub(in crate::client_tun) index: usize,
    pub(in crate::client_tun) handle: SocketHandle,
    pub(in crate::client_tun) epoch: u64,
    pub(in crate::client_tun) generation: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum FlowStage {
    Receiving,
    Waiting,
    Opening,
    Ready,
    Published,
    Closing,
    Rearming,
    Quarantined,
}

pub(in crate::client_tun::compatibility_runtime) struct FlowTransaction {
    pub(super) id: FlowIdentity,
    pub(super) stage: FlowStage,
    pub(super) escrow: crate::client_tun::uplink::Payload,
    pub(super) escrow_bytes: usize,
    pub(super) fake_ip: Option<crate::fake_ip::FakeIpLeaseReceipt>,
    pub(super) fake_ip_failure: Option<(std::net::Ipv4Addr, crate::fake_ip::CheckedFakeIpError)>,
    pub(super) reserved: usize,
    pub(super) w_held: bool,
    pub(super) f_held: bool,
    pub(super) physical: bool,
    pub(super) published: bool,
    pub(super) producer_stopped: bool,
    pub(super) cold_retained: Option<super::rearm::physical_flow::cold::RetainedCarrier>,
    pub(super) preparing: bool,
    release_escrow: bool,
    pub(super) attempt: Option<Box<dyn TcpOpenAttempt<OpenedOwnedTcp>>>,
    pub(super) rejection: Option<FlowRejection>,
    pub(super) carrier: Option<OpenedOwnedTcp>,
    pub(super) roles: Option<FlowRoles>,
    pub(super) close_reason: (&'static str, &'static str),
    pub(super) stop_deadline: stop_deadline::StopDeadline,
    pub(super) tail: CloseTail,
    pub(super) panic: Option<Box<dyn std::any::Any + Send>>,
    upstream: Option<Arc<dyn OwnedUpstream>>,
    target: crate::shared::TargetAddr,
    open_deadline: std::time::Instant,
    waited: bool,
    provider_cancelled: bool,
    #[cfg(test)]
    open_steps: usize,
    #[cfg(test)]
    pub(super) missing_start_gate: Option<usize>,
    physical_check: Option<(FlowStage, tokio::time::Instant)>,
}

const CLOSE_TAIL: std::time::Duration = std::time::Duration::from_secs(15);
/// D152 (A1), value D181: the provider's own bounded loss settlement — peer idle
/// then own idle (one-way loss re-arms ours, RFC 9000 §10.1), then the Node's
/// release of the dead connection after two bounded reconnect handshakes, + 1 s.
const PROVIDER_LOSS_BOUND: std::time::Duration =
    std::time::Duration::from_secs(2 * crate::quic::QUIC_MAX_IDLE_SECS + 1)
        .saturating_add(crate::tuic::TUIC_RECONNECT_TIMEOUT.saturating_mul(2));

/// D147 (A') close tail. `stop_deadline` runs 15s from the later of the close
/// and the last App progress (a strict drop of the flow's undelivered tail
/// below its minimum since close, reported by the physical pass). Expiry is a
/// local reset of this flow only when the latest physical pass of the same
/// stage waited on nothing but the App socket (custody certain). D152 (A1):
/// an expiry whose latest drive proved the only wait is the cancelled
/// provider's own retirement gets one extension to the provider deadline
/// (fixed at cancel as tail + PROVIDER_LOSS_BOUND). Every other expiry, and any
/// second expiry, stays sticky Q.
#[derive(Default)]
pub(super) struct CloseTail {
    wait: Option<FlowStage>,
    min: Option<usize>,
    aborted: bool,
    provider_wait: bool,
    provider_deadline: Option<std::time::Instant>,
    extended: bool,
}

impl CloseTail {
    pub(super) fn aborted(&self) -> bool {
        self.aborted
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum AdmissionIdentityFault {
    EpochExhausted {
        handle: SocketHandle,
        epoch: u64,
    },
    GenerationExhausted {
        handle: SocketHandle,
        generation: u64,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum FlowRejection {
    ReportedNativeUplink(bytes::MeteredCopyError),
    Unavailable(crate::upstream::TcpOpenUnavailable),
    Deadline,
}

#[derive(Default)]
pub(super) struct FlowAccounting {
    #[cfg(test)]
    pub(super) cold_probe: Option<Arc<super::rearm::physical_flow::tests::cold::Probe>>,
    #[cfg(test)]
    receipt_gate: Option<tests::receipt_gate::ReceiptGate>,
    #[cfg(test)]
    pub(super) defer_receipt: Option<FlowIdentity>,
    #[cfg(test)]
    pub(super) window_fault: Option<FlowIdentity>,
    #[cfg(test)]
    pub(super) held_lane: Option<FlowIdentity>,
    #[cfg(test)]
    pub(super) lane_observation: Option<(FlowIdentity, bool, usize)>,
    #[cfg(test)]
    pub(super) active_close_probe: Option<FlowIdentity>,
    #[cfg(test)]
    pub(super) active_close_observation: Option<(bool, usize, usize, usize, usize, Option<usize>)>,
    #[cfg(test)]
    pub(super) last_d16_disposition: Option<(usize, usize, usize)>,
    #[cfg(test)]
    pub(super) ordered_watch: Option<FlowIdentity>,
    // One overwritten physical snapshot: id/epoch/pending/caps/inflight/socket/d16/reset.
    #[cfg(test)]
    pub(super) ordered_physical: Option<(
        (FlowIdentity, u64, usize, usize, usize, usize, bool, bool),
        crate::client_tun::downlink_custody::OrderedCustodySnapshot,
    )>,
    #[cfg(test)]
    factory_started: u64,
    #[cfg(test)]
    pub(super) last_fake_release: Option<(
        FlowIdentity,
        crate::fake_ip::FakeIpLeaseReceipt,
        (usize, usize),
    )>,
    pub(super) w: usize,
    pub(super) f: usize,
    pub(super) bytes: usize,
    pub(super) escrow_bytes: usize,
    generation: u64,
    pub(super) identity_fault: Option<AdmissionIdentityFault>,
    retry: usize,
    pub(super) first_rejection: Option<FlowRejection>,
    pub(super) published: u64,
    pub(super) released: u64,
    pub(super) joined: u64,
    last_data_disposition: Option<(usize, usize, usize, usize, usize)>,
    #[cfg(all(test, feature = "harness"))]
    last_permit_reset: Option<super::rearm::physical_flow::PermitResetObservation>,
    // A settled provider can still own the original error. Retain only the first,
    // after its physical resource view is proved empty; this is not a flow ledger.
    pub(super) first_provider_failure: Option<Box<dyn TcpOpenAttempt<OpenedOwnedTcp>>>,
}

impl OpenCustody {
    pub(in crate::client_tun::compatibility_runtime) fn with_flows() -> Self {
        Self {
            slots: SlotTable::with_len(FLOW_SLOT_LIMIT),
            flow: Some(FlowAccounting::default()),
            ..Self::default()
        }
    }

    pub(super) fn audit_flows(&self) -> bool {
        let Some(accounting) = &self.flow else {
            return true;
        };
        let (mut live, mut w, mut f, mut bytes, mut escrow) =
            (0usize, 0usize, 0usize, 0usize, 0usize);
        for (index, slot) in self.slots.iter().enumerate() {
            let Some(slot) = slot else {
                continue;
            };
            let OpenPhase::Flow(flow) = &slot.phase else {
                return false;
            };
            if flow.id.index != index
                || flow.id.handle != slot.handle
                || flow.id.epoch != slot.epoch
                || flow.id.generation == 0
                || flow.id.generation > accounting.generation
                || (flow.upstream.is_none() && !flow.preparing)
                || flow.reserved > MAX_UPLINK_BUFFER
                || if flow.release_escrow {
                    !flow.preparing || flow.upstream.is_some() || !flow.escrow.is_empty()
                } else { flow.escrow.len() != flow.escrow_bytes }
                || flow.escrow.len() > flow.reserved
            {
                return false;
            }
            live += 1;
            w += usize::from(flow.w_held);
            f += usize::from(flow.f_held);
            let Some(sum) = bytes.checked_add(flow.reserved) else {
                return false;
            };
            bytes = sum;
            let Some(sum) = escrow.checked_add(flow.escrow_bytes) else {
                return false;
            };
            escrow = sum;
        }
        self.slots.len() == FLOW_SLOT_LIMIT
            && live == self.live
            && w == accounting.w
            && f == accounting.f
            && bytes == accounting.bytes
            && escrow == accounting.escrow_bytes
            && w <= OPEN_LIMIT
            && f <= FLOW_LIMIT
            && bytes <= ESCROW_LIMIT
    }

    pub(super) fn flow_mut(&mut self, id: FlowIdentity) -> Option<&mut FlowTransaction> {
        match &mut self.slots.get_mut(id.index)?.phase {
            OpenPhase::Flow(flow) if flow.id == id => Some(flow),
            _ => None,
        }
    }
}

impl FlowTransaction {
    pub(super) fn retain_unwind(&mut self, payload: Box<dyn std::any::Any + Send>) {
        if self.panic.is_none() { self.panic = Some(payload); }
        else if let Some(roles) = &mut self.roles { roles.retain_control_unwind(payload); }
        else { std::mem::forget(payload); } // No provider/actor callback can restart this poisoned row.
        self.stage = FlowStage::Quarantined;
    }
    pub(super) fn close(&mut self, reason: (&'static str, &'static str)) {
        if self.stop_deadline.is_some() {
            return;
        }
        self.close_reason = reason;
        self.stop_deadline.open(tokio::time::Instant::now().into_std() + CLOSE_TAIL);
        if self.stage != FlowStage::Quarantined {
            self.stage = FlowStage::Closing;
        }
    }

    /// Physical pass start: no App-only wait is proven for this pass yet.
    pub(super) fn clear_app_tail_wait(&mut self) {
        self.tail.wait = None;
    }

    /// The physical pass ends waiting only on the App socket, `outstanding`
    /// tail bytes not yet acknowledged by it. App progress re-arms the tail.
    pub(super) fn app_tail_wait(&mut self, outstanding: usize) {
        self.tail.wait = Some(self.stage);
        if self.tail.min.is_some_and(|min| outstanding < min) {
            self.stop_deadline.rearm(tokio::time::Instant::now().into_std() + CLOSE_TAIL);
        }
        self.tail.min = Some(
            self.tail
                .min
                .map_or(outstanding, |min| min.min(outstanding)),
        );
    }

    pub(super) fn drive(&mut self, cx: &mut std::task::Context<'_>) {
        if self.panic.is_some() { return; } // Only the original fatal JOIN boundary may proceed.
        let now = tokio::time::Instant::now().into_std();
        let provider_wait = std::mem::take(&mut self.tail.provider_wait);
        if self.stop_deadline.is_some_and(|deadline| now >= deadline) {
            let certain = !self.tail.aborted
                && !self.tail.extended
                && !self.preparing
                && self.cold_retained.is_none()
                && !self
                    .roles
                    .as_ref()
                    .is_some_and(FlowRoles::disposition_in_flight);
            let app_only = certain
                && matches!(self.stage, FlowStage::Closing | FlowStage::Rearming)
                && self.tail.wait == Some(self.stage);
            let provider_only = certain && provider_wait && self.stage == FlowStage::Closing;
            if app_only {
                // Certain custody: abandon only the App tail; the normal path
                // resets the App socket and releases within one fresh tail.
                self.tail.aborted = true;
                self.stop_deadline.grant(now + CLOSE_TAIL);
            } else if let Some(deadline) = self.tail.provider_deadline.filter(|_| provider_only) {
                // D152 (A1): only the provider's bounded loss settlement remains.
                self.tail.extended = true;
                self.stop_deadline.grant(deadline);
            } else {
                self.stage = FlowStage::Quarantined;
            }
        }
        if self.preparing || self.cold_retained.is_some() || self.roles.as_ref().is_some_and(FlowRoles::disposition_in_flight) { return; }
        if let Some(roles) = &mut self.roles {
            if self.stage == FlowStage::Quarantined {
                roles.stop();
            }
            roles.poll(cx, self.producer_stopped);
            if let Some(reason) = roles.closed() {
                if self.stage == FlowStage::Published {
                    self.close(reason);
                }
            }
        }
        if self.roles.as_ref().is_some_and(FlowRoles::quarantined) {
            self.stage = FlowStage::Quarantined;
            if let Some(roles) = &mut self.roles {
                roles.stop();
            }
            return;
        }
        if self.stage == FlowStage::Opening {
            let Some(attempt) = &mut self.attempt else {
                self.stage = FlowStage::Quarantined;
                return;
            };
            #[cfg(test)]
            {
                self.open_steps += 1;
            }
            attempt.step(cx, now);
            match attempt.view().phase {
                TcpOpenPhase::Ready => self.stage = FlowStage::Ready,
                TcpOpenPhase::Settled | TcpOpenPhase::Retiring => {
                    self.close(("remote_open", "open_failed"))
                }
                TcpOpenPhase::Quarantined => self.stage = FlowStage::Quarantined,
                _ => {}
            }
        }
        if self.stage == FlowStage::Closing && self.producer_stopped {
            if let Some(roles) = &mut self.roles {
                roles.stop();
                if !roles.data_disposed() {
                    return;
                }
            }
            if let Some(attempt) = &mut self.attempt {
                let Some(deadline) = *self.stop_deadline else {
                    return;
                };
                // Fixed on first use: the provider bound covers one A1 extension.
                let deadline = *self
                    .tail
                    .provider_deadline
                    .get_or_insert(deadline + PROVIDER_LOSS_BOUND);
                if !self.provider_cancelled {
                    if !matches!(
                        attempt.view().phase,
                        TcpOpenPhase::Transferred | TcpOpenPhase::Settled
                    ) {
                        attempt.cancel(TcpOpenCancel::Cancelled, deadline);
                    }
                    self.provider_cancelled = true;
                }
                attempt.settle(cx, now, deadline);
                let view = attempt.view();
                if view.phase == TcpOpenPhase::Quarantined {
                    self.stage = FlowStage::Quarantined;
                    return;
                }
                if view.phase != TcpOpenPhase::Settled || view.lease_held || view.retirement_pending
                {
                    self.tail.provider_wait = view.phase == TcpOpenPhase::Retiring
                        && (view.lease_held || view.retirement_pending);
                    return;
                }
            }
            self.stage = FlowStage::Rearming;
        }
    }
}

impl OpenDispatch {
    pub(in crate::client_tun) fn flow_draining(&self) -> bool {
        self.owner
            .as_ref()
            .is_some_and(|owner| owner.stop.stopping.load(Ordering::Acquire) && owner.flow_mode())
    }

    pub(in crate::client_tun) fn flow_mode(&self) -> bool {
        self.owner.as_ref().is_some_and(|owner| owner.flow_mode())
    }

    pub(super) fn reserve_flow<U: OwnedUpstream + 'static>(
        &self,
        upstream: Arc<U>,
        target: crate::shared::TargetAddr,
        handle: SocketHandle,
        epoch: u64,
    ) -> Option<FlowIdentity> {
        let owner = self.owner.as_ref()?;
        if owner.stop.stopping.load(Ordering::Acquire) || !owner.take_control() {
            return None;
        }
        let mut opens = owner.opens();
        if opens.flow.as_ref()?.identity_fault.is_some() {
            return None;
        }
        if !opens.audit_flows() {
            owner.ledger().quarantined = true;
            return None;
        }
        let index = opens.slots.iter().position(Option::is_none)?;
        if opens
            .slots
            .iter()
            .flatten()
            .any(|slot| slot.handle == handle)
        {
            return None;
        }
        let accounting = opens.flow.as_mut()?;
        // Admission and the final physical receipt each advance the epoch.
        let final_epoch = epoch.checked_add(2);
        let next = final_epoch
            .map(|final_epoch| final_epoch - 1)
            .zip(accounting.generation.checked_add(1));
        let Some((epoch, generation)) = next else {
            accounting.identity_fault = Some(if final_epoch.is_none() {
                AdmissionIdentityFault::EpochExhausted { handle, epoch }
            } else {
                AdmissionIdentityFault::GenerationExhausted {
                    handle,
                    generation: accounting.generation,
                }
            });
            owner.ledger().quarantined = true;
            owner.record(RetainedFailure::from_io(
                SessionControl::origin(FailurePhase::TcpOpen),
                std::io::ErrorKind::InvalidData.into(),
            ));
            return None;
        };
        let bytes = accounting.bytes.checked_add(MAX_UPLINK_BUFFER)?;
        if accounting.w >= OPEN_LIMIT || bytes > ESCROW_LIMIT {
            return None;
        }
        accounting.generation = generation;
        accounting.bytes = bytes;
        accounting.w += 1;
        let id = FlowIdentity {
            index,
            handle,
            epoch,
            generation,
        };
        let deadline = tokio::time::Instant::now().into_std() + std::time::Duration::from_secs(20);
        let flow = FlowTransaction {
            id,
            stage: FlowStage::Receiving,
            escrow: Vec::new().into(),
            escrow_bytes: 0,
            fake_ip: None,
            fake_ip_failure: None,
            reserved: MAX_UPLINK_BUFFER,
            w_held: true,
            f_held: false,
            physical: false,
            published: false,
            producer_stopped: false,
            cold_retained: None,
            preparing: false,
            release_escrow: false,
            attempt: None,
            rejection: None,
            carrier: None,
            roles: None,
            close_reason: ("root", "opening"),
            stop_deadline: Default::default(),
            tail: CloseTail::default(),
            panic: None,
            upstream: Some(upstream),
            target,
            open_deadline: deadline,
            waited: false,
            provider_cancelled: false,
            #[cfg(test)]
            open_steps: 0,
            #[cfg(test)]
            missing_start_gate: None,
            physical_check: None,
        };
        // `index` is a free row of this locked table; Err is unreachable and
        // would retain (never destroy) the uncertain row under sticky Q.
        if let Err(row) = opens.slots.put(index, OpenSlot {
            handle,
            epoch,
            deadline: deadline.into(),
            phase: OpenPhase::Flow(Box::new(flow)),
        }) {
            std::mem::forget(row);
            owner.ledger().quarantined = true;
            return None;
        }
        opens.live += 1;
        Some(id)
    }

    /// Inverse of the original prephysical Receiving reservation only. No
    /// producer/provider or physical slot has been published; generation is
    /// deliberately monotonic. The removed original Box drops outside opens.
    pub(super) fn rollback_unreceived(&self, id: FlowIdentity) -> bool {
        let Some(owner) = &self.owner else { return false; };
        let mut opens = owner.opens();
        let valid = opens.audit_flows() && opens.flow_mut(id).is_some_and(|flow| {
            flow.stage == FlowStage::Receiving && flow.w_held && !flow.f_held
                && !flow.physical && !flow.published && !flow.producer_stopped
                && !flow.preparing && flow.cold_retained.is_none()
                && flow.escrow.is_empty() && flow.escrow_bytes == 0
                && flow.reserved == MAX_UPLINK_BUFFER && flow.fake_ip.is_none()
                && flow.fake_ip_failure.is_none() && flow.attempt.is_none()
                && flow.rejection.is_none() && flow.carrier.is_none() && flow.roles.is_none()
                && flow.stop_deadline.is_none() && flow.panic.is_none()
                && !flow.waited && !flow.provider_cancelled && flow.physical_check.is_none()
        }) && opens.live > 0 && opens.flow.as_ref().is_some_and(|a| {
            a.w > 0 && a.bytes >= MAX_UPLINK_BUFFER
        });
        if !valid {
            drop(opens);
            owner.quarantine_quiet();
            return false;
        }
        let removed = opens.slots.take(id.index);
        opens.live -= 1;
        if let Some(accounting) = &mut opens.flow {
            accounting.w -= 1;
            accounting.bytes -= MAX_UPLINK_BUFFER;
        }
        let valid = opens.audit_flows();
        drop(opens);
        if !valid { owner.quarantine_quiet(); std::mem::forget(removed); return false; }
        drop(removed);
        true
    }

    pub(in crate::client_tun) fn request_flow_close(
        &self,
        handle: SocketHandle,
        epoch: u64,
        physical: Option<FlowIdentity>,
        d16_reason: Option<(&'static str, &'static str)>,
        terminal_ctx: Option<(&mut SocketCtx, (&'static str, &'static str))>,
    ) -> bool {
        let Some(owner) = &self.owner else {
            return false;
        };
        let mut opens = owner.opens();
        if opens.flow.is_none() {
            return false;
        }
        for (_, slot) in opens.slots.occupied_mut() {
            if slot.handle == handle {
                if slot.epoch != epoch
                    || physical.is_some_and(
                        |id| !matches!(&slot.phase, OpenPhase::Flow(flow) if flow.id == id),
                    )
                {
                    owner.ledger().quarantined = true;
                    return true;
                }
                if let OpenPhase::Flow(flow) = &mut slot.phase {
                    if flow.roles.as_ref().is_some_and(FlowRoles::is_d16) {
                        if let Some((ctx, _cause)) = terminal_ctx {
                            if ctx
                                .flow_mirror
                                .as_ref()
                                .and_then(|mirror| mirror.d16_terminal_preclear_inflight())
                                .is_none()
                            {
                                let prepared = flow.roles.as_ref().is_some_and(|roles| {
                                    ctx.flow_mirror.as_ref().is_some_and(|mirror| {
                                        mirror.can_preclear_d16_terminal(ctx, roles)
                                    })
                                }) && ctx.preflight_downlink_clear();
                                if !prepared {
                                    flow.stage = FlowStage::Quarantined;
                                    owner.ledger().quarantined = true;
                                    return true;
                                }
                                let inflight = ctx.downlink_inflight_permit_bytes();
                                let disposed = ctx.discard_ordered_for_reset();
                                if !disposed
                                    || !ctx.flow_mirror.as_mut().is_some_and(|mirror| {
                                        mirror.note_d16_terminal_preclear(inflight)
                                    })
                                {
                                    flow.stage = FlowStage::Quarantined;
                                    owner.ledger().quarantined = true;
                                    return true;
                                }
                            }
                        }
                    }
                    let reason = if flow.roles.as_ref().is_some_and(FlowRoles::is_d16) {
                        d16_reason.unwrap_or(("local", "close_requested"))
                    } else {
                        ("local", "close_requested")
                    };
                    flow.close(reason);
                }
                return true;
            }
        }
        if physical.is_some() {
            owner.ledger().quarantined = true;
            return true;
        }
        false
    }

    pub(in crate::client_tun) fn poll_flows(
        &self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<OpenEvent>> {
        let Some(owner) = &self.owner else {
            return Poll::Pending;
        };
        let mut opens = owner.opens();
        let stopping = owner.stop.stopping.load(Ordering::Acquire);
        let mut wrapped = false;
        for _ in 0..32 {
            // D61/D62 R-a: visit occupied rows only; an empty row spends no
            // control. R-b: passing the table end ends this pass with Pending
            // and no self-wake; the original loop's 5ms timer starts the next.
            if wrapped {
                break;
            }
            let Some(index) = opens.slots.next_occupied(opens.cursor) else {
                opens.cursor = 0;
                wrapped = true;
                break;
            };
            if !owner.take_control() {
                opens.cursor = index;
                return Poll::Ready(Some(OpenEvent::Yield));
            }
            opens.cursor = (index + 1) % opens.slots.len();
            wrapped |= opens.cursor == 0;
            // Split disjoint fields; no transaction is removed from root custody.
            let OpenCustody {
                slots,
                flow: accounting,
                ..
            } = &mut *opens;
            let (Some(slot), Some(accounting)) = (slots.get_mut(index), accounting) else {
                continue;
            };
            let OpenPhase::Flow(flow) = &mut slot.phase else {
                owner.ledger().quarantined = true;
                continue;
            };
            #[cfg(test)]
            if accounting.window_fault == Some(flow.id) {
                return Poll::Ready(Some(OpenEvent::Flow(flow.id)));
            }
            if stopping && flow.stage != FlowStage::Rearming {
                flow.close(("root", "session_stop"));
            }
            if flow.preparing {
                if flow.stop_deadline.is_some_and(|deadline| tokio::time::Instant::now().into_std() >= deadline) {
                    flow.stage = FlowStage::Quarantined;
                }
                continue;
            }
            if flow.stage == FlowStage::Waiting
                && tokio::time::Instant::now().into_std() >= flow.open_deadline
            {
                flow.rejection = Some(FlowRejection::Deadline);
                flow.close(("root", "open_deadline"));
            }
            if flow.stage == FlowStage::Waiting {
                if accounting.f == FLOW_LIMIT || (flow.waited && accounting.retry == 0) {
                    flow.waited = true;
                    continue;
                }
                if flow.waited {
                    accounting.retry -= 1;
                }
                accounting.f += 1;
                flow.f_held = true;
                flow.stage = FlowStage::Opening;
                let Some(upstream) = flow.upstream.as_ref().cloned() else {
                    flow.stage = FlowStage::Quarantined;
                    drop(opens);
                    owner.quarantine_quiet();
                    opens = owner.opens(); opens.cursor = index;
                    continue;
                };
                match catch_unwind(AssertUnwindSafe(|| {
                    #[cfg(test)]
                    {
                        accounting.factory_started += 1;
                    }
                    upstream.staged_tcp_open(TcpOpenRequest {
                        target: flow.target.clone(),
                        deadline: flow.open_deadline,
                    })
                })) {
                    Ok(Ok(attempt)) => flow.attempt = Some(attempt),
                    Ok(Err(error)) => {
                        flow.rejection = Some(FlowRejection::Unavailable(error));
                        flow.close(("remote_open", "staged_open_unavailable"));
                    }
                    Err(panic) => {
                        flow.retain_unwind(panic);
                        let id = flow.id;
                        drop(opens);
                        owner.note_flow_unwind(id);
                        opens = owner.opens(); opens.cursor = index;
                        continue; // Original Q event selection; the poisoned provider is never re-entered.
                    }
                }
            }
            let mut continue_next_turn = false;
            if let Err(panic) = catch_unwind(AssertUnwindSafe(|| {
                use TcpOpenPhase as P;
                loop {
                    let before = (flow.stage == FlowStage::Opening)
                        .then(|| flow.attempt.as_ref().map(|a| a.view().phase))
                        .flatten();
                    flow.drive(cx);
                    let progressed = flow.stage == FlowStage::Opening
                        && matches!(
                            before.zip(flow.attempt.as_ref().map(|a| a.view().phase)),
                            Some(
                                (P::Unstarted, P::Acquiring)
                                    | (P::Acquiring, P::Reserved)
                                    | (P::Reserved, P::Opening)
                                    | (P::Opening, P::Connecting)
                                    | (P::Connecting, P::Preparing)
                            )
                        );
                    // Continue only a forward phase; pressure/partial writes stop.
                    // Every extra step spends the same shared control budget.
                    if !progressed {
                        break;
                    }
                    if !owner.take_control() {
                        continue_next_turn = true;
                        break;
                    }
                }
            })) {
                flow.retain_unwind(panic);
                let id = flow.id;
                drop(opens);
                owner.note_flow_unwind(id);
                opens = owner.opens(); opens.cursor = index;
                continue;
            }
            if continue_next_turn {
                opens.cursor = index;
                return Poll::Ready(Some(OpenEvent::Yield));
            }
            if let Some(roles) = &mut flow.roles {
                if roles.all_original_roles_joined() {
                    if let Some(disposal) = roles.take_original_disposal(owner, flow.id) {
                        drop(opens);
                        if disposal.dispose(owner, cx) {
                            opens = owner.opens(); opens.cursor = index;
                            continue; // Actual progress spends the same bounded original control turn.
                        }
                        return Poll::Ready(Some(OpenEvent::Yield));
                    }
                }
            }
            if let Some(disposal) = flow.roles.as_mut().and_then(|roles|
                roles.take_native_disposal(owner, flow.id, flow.producer_stopped)) {
                let stock_wait = disposal.is_stock_retirement();
                let id = flow.id;
                drop(opens);
                if disposal.dispose(owner, cx) {
                    opens = owner.opens(); opens.cursor = index;
                    continue;
                }
                if stock_wait {
                    opens = owner.opens();
                    let Some(flow) = opens.flow_mut(id) else { return Poll::Pending; };
                    let now = tokio::time::Instant::now();
                    // A waiting inventory cannot prevent Main from releasing
                    // its actual loan. Reuse the original physical-control timer.
                    if flow.physical_check.is_some_and(|(stage, next)| stage == flow.stage && now < next) { continue; }
                    flow.physical_check = Some((flow.stage, now + std::time::Duration::from_millis(5)));
                    return Poll::Ready(Some(OpenEvent::Flow(id)));
                }
                return Poll::Ready(Some(OpenEvent::Yield));
            }
            if flow.stage == FlowStage::Quarantined {
                owner.ledger().quarantined = true;
            }
            if matches!(
                flow.stage,
                FlowStage::Ready | FlowStage::Closing | FlowStage::Rearming
            ) || (flow.stage == FlowStage::Quarantined && !flow.producer_stopped)
            {
                let now = tokio::time::Instant::now();
                if flow
                    .physical_check
                    .is_some_and(|(stage, next)| stage == flow.stage && now < next)
                {
                    continue;
                }
                // First/new-stage action is immediate. Repeated physical probes
                // use the original loop's existing 5ms timer, not a new timer/task.
                flow.physical_check = Some((flow.stage, now + std::time::Duration::from_millis(5)));
                owner.open_started.store(true, Ordering::Relaxed);
                return Poll::Ready(Some(OpenEvent::Flow(flow.id)));
            }
        }
        if !wrapped {
            cx.waker().wake_by_ref();
        }
        Poll::Pending
    }


}

impl SessionControl {
    // One cold owner entry publishes the original flow-scoped opaque fault.
    // Retained payload custody remains at the original call site; quiet/Q and
    // the earliest stop deadline are published through the complete Session API.
    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn note_flow_unwind(
        &self,
        id: FlowIdentity,
    ) {
        // Every caller has retained the original Box and released opens before
        // entering this complete quiet/Q publication boundary.
        let failure = RetainedFailure::opaque_panic(open_origin(
            id.index,
            id.epoch,
            FailurePhase::TcpOpen,
        ));
        if self.app_meter.as_ref().is_some_and(|meter| meter.budget_limit().is_some()) {
            crate::shared::errors::QuietFailureSink::record_quiet(self, failure);
        } else {
            self.record(failure);
            self.ledger().quarantined = true;
        }
    }

    pub(in crate::client_tun::compatibility_runtime) fn flow_mode(&self) -> bool {
        self.opens().flow.is_some()
    }
}

#[cfg(test)]
mod tests;
