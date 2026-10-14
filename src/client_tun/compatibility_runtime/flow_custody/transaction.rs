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
    pub(in crate::client_tun::compatibility_runtime) slots: Vec<Option<OpenSlot>>,
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
            slots: std::iter::repeat_with(|| None).take(128).collect(),
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
        global_tx: &mpsc::Sender<(SocketHandle, RelayEvent)>,
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
                opens.slots[index] = Some(OpenSlot {
                    handle,
                    epoch: ctx.conn_epoch.wrapping_add(1),
                    deadline: tokio::time::Instant::now() + std::time::Duration::from_secs(20),
                    phase: OpenPhase::Reserved,
                });
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
                opens
                    .slots
                    .iter_mut()
                    .enumerate()
                    .find_map(|(index, slot)| {
                        slot.as_mut()
                            .filter(|slot| {
                                slot.handle == handle
                                    && slot.epoch == epoch
                                    && matches!(slot.phase, OpenPhase::Reserved)
                            })
                            .map(|slot| (index, slot))
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
                        if let Some(slot) = &mut opens.slots[index] {
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
                }) = opens.slots[index].take()
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

impl SessionControl {
    pub(in crate::client_tun::compatibility_runtime) fn opens(
        &self,
    ) -> std::sync::MutexGuard<'_, OpenCustody> {
        self.opens
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(in crate::client_tun::compatibility_runtime) fn take_control(&self) -> bool {
        self.control_remaining
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
    }

    pub(in crate::client_tun::compatibility_runtime) fn record_open(
        &self,
        mut outcome: OpenOutcome,
    ) {
        if let Some(failure) = outcome.failure.take() {
            self.record(failure);
        }
        let mut ledger = self.ledger();
        ledger.additional_failures = ledger
            .additional_failures
            .saturating_add(outcome.additional);
    }

    pub(in crate::client_tun::compatibility_runtime) async fn settle_opens(&self) {
        if self.flow_mode() {
            let opens = self.opens();
            if opens.live != 0
                || !opens.audit_flows()
                || opens
                    .flow
                    .as_ref()
                    .is_some_and(|a| a.identity_fault.is_some())
            {
                self.ledger().quarantined = true;
            }
            return;
        }
        {
            let opens = self.opens();
            let mut actual = 0;
            let mut workers = [0u64; 2];
            for (index, slot) in opens.slots.iter().enumerate() {
                actual += usize::from(slot.is_some());
                if matches!(
                    slot.as_ref().map(|slot| &slot.phase),
                    Some(OpenPhase::Worker(_))
                ) {
                    workers[index / 64] |= 1u64 << (index % 64);
                }
            }
            let orphan = self
                .announced
                .iter()
                .zip(workers)
                .any(|(announced, workers)| announced.load(Ordering::Acquire) & !workers != 0);
            if actual != opens.live || orphan {
                self.ledger().quarantined = true;
            }
            if actual == 0 {
                return;
            }
        }
        let mut cursor = 0;
        let mut pending = false;
        let signal = Arc::new(OpenJoinWake::default());
        let child_waker = std::task::Waker::from(Arc::clone(&signal));
        std::future::poll_fn(|cx| {
            signal.register(cx.waker());
            if cursor == 0 {
                // Earlier notifications are covered by this complete bounded scan.
                signal.retry.store(false, Ordering::Release);
            }
            let mut child_cx = std::task::Context::from_waker(&child_waker);
            let mut opens = self.opens();
            for index in cursor..cursor + 32 {
                let Some(slot) = &mut opens.slots[index] else {
                    continue;
                };
                if matches!(slot.phase, OpenPhase::Worker(_)) {
                    let Some(outcome) = poll_open_slot(slot, index, &mut child_cx) else {
                        pending = true;
                        continue;
                    };
                    slot.phase = OpenPhase::Retained(outcome);
                    self.announced[index / 64]
                        .fetch_and(!(1u64 << (index % 64)), Ordering::Release);
                }
                if matches!(&slot.phase, OpenPhase::Retained(outcome) if outcome.opened.is_some()) {
                    self.ledger().quarantined = true;
                    continue;
                }
                let outcome = opens.slots[index].take().and_then(|slot| match slot.phase {
                    OpenPhase::Retained(outcome) | OpenPhase::AwaitingDisposition(outcome) => {
                        Some(outcome)
                    }
                    _ => None,
                });
                opens.live = opens.live.saturating_sub(1);
                if let Some(outcome) = outcome {
                    self.record_open(outcome);
                }
            }
            cursor = (cursor + 32) % 128;
            if cursor == 0 {
                if !pending {
                    return Poll::Ready(());
                }
                pending = false;
                if signal.retry.load(Ordering::Acquire) {
                    cx.waker().wake_by_ref();
                }
            } else {
                cx.waker().wake_by_ref();
            }
            Poll::Pending
        })
        .await;
    }
}

// F control transactions share the original slot table. W and F overlap in one
// row during open/publication, so their counts are audited independently.
use super::supervision::FlowRoles;
use crate::owned_upstream::OpenedOwnedTcp;
use crate::upstream::{TcpOpenAttempt, TcpOpenCancel, TcpOpenPhase, TcpOpenRequest};

pub(super) const FLOW_LIMIT: usize = 4096;
const OPEN_LIMIT: usize = 128;
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
    pub(super) escrow: Vec<u8>,
    pub(super) escrow_bytes: usize,
    pub(super) fake_ip: Option<crate::fake_ip::FakeIpLeaseReceipt>,
    pub(super) fake_ip_failure: Option<(std::net::Ipv4Addr, crate::fake_ip::CheckedFakeIpError)>,
    pub(super) reserved: usize,
    pub(super) w_held: bool,
    pub(super) f_held: bool,
    pub(super) physical: bool,
    pub(super) published: bool,
    pub(super) producer_stopped: bool,
    pub(super) attempt: Option<Box<dyn TcpOpenAttempt<OpenedOwnedTcp>>>,
    rejection: Option<FlowRejection>,
    pub(super) carrier: Option<OpenedOwnedTcp>,
    pub(super) roles: Option<FlowRoles>,
    pub(super) close_reason: (&'static str, &'static str),
    pub(super) stop_deadline: Option<std::time::Instant>,
    pub(super) panic: Option<Box<dyn std::any::Any + Send>>,
    upstream: Arc<dyn OwnedUpstream>,
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
    Unavailable(crate::upstream::TcpOpenUnavailable),
    Deadline,
}

#[derive(Default)]
pub(super) struct FlowAccounting {
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
    // A settled provider can still own the original error. Retain only the first,
    // after its physical resource view is proved empty; this is not a flow ledger.
    pub(super) first_provider_failure: Option<Box<dyn TcpOpenAttempt<OpenedOwnedTcp>>>,
}

impl OpenCustody {
    pub(in crate::client_tun::compatibility_runtime) fn with_flows() -> Self {
        Self {
            slots: std::iter::repeat_with(|| None)
                .take(FLOW_LIMIT + OPEN_LIMIT)
                .collect(),
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
                || flow.reserved > MAX_UPLINK_BUFFER
                || flow.escrow.len() != flow.escrow_bytes
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
            let Some(sum) = escrow.checked_add(flow.escrow.len()) else {
                return false;
            };
            escrow = sum;
        }
        self.slots.len() == FLOW_LIMIT + OPEN_LIMIT
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
        match &mut self.slots.get_mut(id.index)?.as_mut()?.phase {
            OpenPhase::Flow(flow) if flow.id == id => Some(flow),
            _ => None,
        }
    }
}

impl FlowTransaction {
    pub(super) fn close(&mut self, reason: (&'static str, &'static str)) {
        if self.stop_deadline.is_some() {
            return;
        }
        self.close_reason = reason;
        self.stop_deadline =
            Some(tokio::time::Instant::now().into_std() + std::time::Duration::from_secs(15));
        if self.stage != FlowStage::Quarantined {
            self.stage = FlowStage::Closing;
        }
    }

    pub(super) fn drive(&mut self, cx: &mut std::task::Context<'_>) {
        let now = tokio::time::Instant::now().into_std();
        if self.stop_deadline.is_some_and(|deadline| now >= deadline) {
            self.stage = FlowStage::Quarantined;
        }
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
                if !roles.settled() {
                    return;
                }
            }
            if let Some(attempt) = &mut self.attempt {
                let Some(deadline) = self.stop_deadline else {
                    return;
                };
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
            escrow: Vec::new(),
            escrow_bytes: 0,
            fake_ip: None,
            fake_ip_failure: None,
            reserved: MAX_UPLINK_BUFFER,
            w_held: true,
            f_held: false,
            physical: false,
            published: false,
            producer_stopped: false,
            attempt: None,
            rejection: None,
            carrier: None,
            roles: None,
            close_reason: ("root", "opening"),
            stop_deadline: None,
            panic: None,
            upstream,
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
        opens.slots[index] = Some(OpenSlot {
            handle,
            epoch,
            deadline: deadline.into(),
            phase: OpenPhase::Flow(Box::new(flow)),
        });
        opens.live += 1;
        Some(id)
    }

    pub(in crate::client_tun) fn request_flow_close(
        &self,
        handle: SocketHandle,
        epoch: u64,
        physical: Option<FlowIdentity>,
    ) -> bool {
        let Some(owner) = &self.owner else {
            return false;
        };
        let mut opens = owner.opens();
        if opens.flow.is_none() {
            return false;
        }
        for slot in opens.slots.iter_mut().flatten() {
            if slot.handle == handle {
                if slot.epoch != epoch
                    || physical.is_some_and(
                        |id| !matches!(&slot.phase, OpenPhase::Flow(flow) if flow.id == id),
                    )
                {
                    owner.ledger().quarantined = true;
                }
                if let OpenPhase::Flow(flow) = &mut slot.phase {
                    flow.close(("local", "close_requested"));
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
            if !owner.take_control() {
                return Poll::Ready(Some(OpenEvent::Yield));
            }
            let index = opens.cursor;
            opens.cursor = (index + 1) % opens.slots.len();
            wrapped |= opens.cursor == 0;
            // Split disjoint fields; no transaction is removed from root custody.
            let OpenCustody {
                slots,
                flow: accounting,
                ..
            } = &mut *opens;
            let (Some(slot), Some(accounting)) = (&mut slots[index], accounting) else {
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
                match catch_unwind(AssertUnwindSafe(|| {
                    #[cfg(test)]
                    {
                        accounting.factory_started += 1;
                    }
                    flow.upstream.clone().staged_tcp_open(TcpOpenRequest {
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
                        flow.panic = Some(panic);
                        flow.stage = FlowStage::Quarantined;
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
                flow.panic = Some(panic);
                flow.stage = FlowStage::Quarantined;
            }
            if continue_next_turn {
                opens.cursor = index;
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

    pub(super) fn release_flow(&self, receipt: super::rearm::FlowPhysicalReceipt) -> bool {
        let id = receipt.identity();
        let Some(owner) = &self.owner else {
            return false;
        };
        let mut opens = owner.opens();
        #[cfg(test)]
        let Some(receipt) = tests::receipt_gate::intercept(&mut opens, receipt) else {
            return false;
        };
        if !opens.audit_flows() {
            if let Some(flow) = opens.flow_mut(id) {
                flow.stage = FlowStage::Quarantined;
            }
            owner.ledger().quarantined = true;
            return false;
        }
        let Some(flow) = opens.flow_mut(id) else {
            owner.ledger().quarantined = true;
            return false;
        };
        if !receipt.matches_flow(flow)
            || flow.stage != FlowStage::Rearming
            || !flow.producer_stopped
            || !flow.physical
            || flow.carrier.is_some()
            || flow.roles.as_ref().is_some_and(|roles| !roles.settled())
        {
            flow.stage = FlowStage::Quarantined;
            owner.ledger().quarantined = true;
            return false;
        }
        if let Some(roles) = &mut flow.roles {
            while let Some(error) = roles.take_error() {
                owner.record(RetainedFailure::from_io(
                    open_origin(id.index, id.epoch, FailurePhase::TcpOpen),
                    error,
                ));
            }
        }
        let rejection = flow.rejection;
        if let Some(rejection) = rejection {
            let kind = match rejection {
                FlowRejection::Unavailable(crate::upstream::TcpOpenUnavailable::Unsupported) => {
                    std::io::ErrorKind::Unsupported
                }
                FlowRejection::Unavailable(crate::upstream::TcpOpenUnavailable::InvalidTarget) => {
                    std::io::ErrorKind::InvalidInput
                }
                FlowRejection::Deadline => std::io::ErrorKind::TimedOut,
            };
            owner.record(RetainedFailure::from_io(
                open_origin(id.index, id.epoch, FailurePhase::TcpOpen),
                kind.into(),
            ));
        } else if flow
            .attempt
            .as_ref()
            .is_some_and(|attempt| attempt.failure().is_some())
        {
            // The original source stays in the settled attempt below. This record
            // exposes failed settlement to the session without fabricating success.
            owner.record(RetainedFailure::from_io(
                open_origin(id.index, id.epoch, FailurePhase::TcpOpen),
                std::io::ErrorKind::Other.into(),
            ));
        }
        let discarded_escrow = flow.escrow_bytes;
        flow.escrow = Vec::new();
        flow.escrow_bytes = 0;
        let (w, f, bytes, published, joined) = (
            flow.w_held,
            flow.f_held,
            flow.reserved,
            flow.published,
            flow.roles.as_ref().map_or(0, FlowRoles::join_count),
        );
        let failed_attempt = if flow
            .attempt
            .as_ref()
            .is_some_and(|attempt| attempt.failure().is_some())
        {
            flow.attempt.take()
        } else {
            None
        };
        let Some(accounting) = &mut opens.flow else {
            return false;
        };
        if accounting.first_rejection.is_none() {
            accounting.first_rejection = rejection;
        }
        accounting.w -= usize::from(w);
        accounting.f -= usize::from(f);
        accounting.bytes -= bytes;
        accounting.escrow_bytes -= discarded_escrow;
        accounting.last_data_disposition = Some(receipt.data_disposition());
        accounting.released += 1;
        accounting.joined += joined as u64;
        if published {
            accounting.retry = (accounting.retry + 1).min(OPEN_LIMIT);
        }
        if accounting.first_provider_failure.is_none() {
            accounting.first_provider_failure = failed_attempt;
        }
        opens.slots[id.index] = None;
        opens.live -= 1;
        true
    }
}

impl SessionControl {
    pub(in crate::client_tun::compatibility_runtime) fn flow_mode(&self) -> bool {
        self.opens().flow.is_some()
    }
}

#[cfg(test)]
mod tests;
