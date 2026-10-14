//! Bounded, priority-aware ingress for one resumable session owner.
//!
//! Owns two channel lanes and one prefetch cell per lane. Capacity observation
//! owns only progress metadata, never commands; one observer covers both lanes.
//! Dequeue commits custody and its checked epoch before invoking a user Waker.
//! Close ends admission, not command settlement or whole-session ownership.
//! Entry is bounded/try_send/recv_next; progress tests live in session/tests/.

use super::tcp::{ClientControllerDriverInput, DriverInput};
use crate::resumable::{Record, SessionEvent};
use std::fmt;
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::task::{Context, Poll};
use thiserror::Error;
use tokio::sync::futures::OwnedNotified;
use tokio::sync::{Notify, mpsc};

/// Ordinary control keeps low latency without allowing a continuously
/// replenished ACK/completion stream to starve already-owned source bytes.
const CONTROL_BURST_LIMIT: usize = 8;
const _: () = assert!(CONTROL_BURST_LIMIT > 0);

pub(crate) struct SessionOwnerMailbox;

impl SessionOwnerMailbox {
    pub(crate) fn bounded(
        control_capacity: usize,
        data_capacity: usize,
    ) -> Result<(SessionOwnerIngress, SessionOwnerInbox), SessionMailboxConfigError> {
        if control_capacity == 0 {
            return Err(SessionMailboxConfigError::ZeroControlCapacity);
        }
        if data_capacity == 0 {
            return Err(SessionMailboxConfigError::ZeroDataCapacity);
        }
        let max = tokio::sync::Semaphore::MAX_PERMITS;
        if control_capacity > max {
            return Err(SessionMailboxConfigError::ControlCapacityTooLarge {
                requested: control_capacity,
                max,
            });
        }
        if data_capacity > max {
            return Err(SessionMailboxConfigError::DataCapacityTooLarge {
                requested: data_capacity,
                max,
            });
        }
        let (control_tx, control_rx) = mpsc::channel(control_capacity);
        let (data_tx, data_rx) = mpsc::channel(data_capacity);
        let progress = Arc::new(SessionMailboxProgress::default());
        Ok((
            SessionOwnerIngress {
                control_tx,
                data_tx,
                progress: Arc::clone(&progress),
            },
            SessionOwnerInbox {
                control_rx,
                data_rx,
                control_streak: 0,
                prefetch: [None, None],
                progress,
            },
        ))
    }
}

#[derive(Clone)]
pub(crate) struct SessionOwnerIngress {
    control_tx: mpsc::Sender<SessionOwnerCommand>,
    data_tx: mpsc::Sender<SessionOwnerCommand>,
    progress: Arc<SessionMailboxProgress>,
}

impl SessionOwnerIngress {
    /// Acquire before trying either lane. Progress is a retry hint, not a
    /// reservation. One persistent observer can survive cancelled waits.
    pub(crate) fn observe_capacity(
        &self,
    ) -> Result<SessionMailboxCapacityObservation, SessionMailboxObserverBusy> {
        self.progress
            .observer_held
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| SessionMailboxObserverBusy)?;
        // Admit before allocating: rejected contenders own no cache. Local
        // reverse-drop order unlinks a constructed registration before lease.
        let lease = SessionMailboxObserverLease {
            progress: Arc::clone(&self.progress),
        };
        let mut registration = Box::pin(Arc::clone(&lease.progress.notify).notified_owned());
        let seen = lease.progress.epochs();
        // The notification baseline must not predate the reported epoch pair.
        registration
            .as_mut()
            .set(Arc::clone(&lease.progress.notify).notified_owned());
        Ok(SessionMailboxCapacityObservation {
            registration,
            lease,
            seen,
        })
    }

    pub(crate) fn try_send_event(
        &self,
        event: SessionEvent,
    ) -> Result<(), SessionMailboxTrySendError> {
        self.try_send_command(SessionOwnerCommand::Event(event))
    }

    pub(crate) fn try_send_driver(
        &self,
        input: DriverInput,
    ) -> Result<(), SessionMailboxTrySendError> {
        self.try_send_command(SessionOwnerCommand::Driver(input))
    }

    fn try_send_command(
        &self,
        command: SessionOwnerCommand,
    ) -> Result<(), SessionMailboxTrySendError> {
        if self.progress.terminal.load(Ordering::Acquire) != MAILBOX_OPEN {
            return Err(SessionMailboxTrySendError::Closed(Box::new(command)));
        }
        let sender = if command.is_ordered_source() {
            &self.data_tx
        } else {
            &self.control_tx
        };
        sender.try_send(command).map_err(|error| match error {
            mpsc::error::TrySendError::Full(command) => {
                SessionMailboxTrySendError::Full(Box::new(command))
            }
            mpsc::error::TrySendError::Closed(command) => {
                SessionMailboxTrySendError::Closed(Box::new(command))
            }
        })
    }
}

impl fmt::Debug for SessionOwnerIngress {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SessionOwnerIngress([REDACTED])")
    }
}

pub(crate) struct SessionOwnerInbox {
    control_rx: mpsc::Receiver<SessionOwnerCommand>,
    data_rx: mpsc::Receiver<SessionOwnerCommand>,
    control_streak: usize,
    prefetch: [Option<SessionOwnerCommand>; 2],
    progress: Arc<SessionMailboxProgress>,
}

const MAILBOX_OPEN: u8 = 0;
const MAILBOX_CLOSED: u8 = 1;
const MAILBOX_EPOCH_EXHAUSTED: u8 = 2;

#[derive(Default)]
struct SessionMailboxProgress {
    epochs: [AtomicU64; 2],
    terminal: AtomicU8,
    observer_held: AtomicBool,
    notify: Arc<Notify>,
}

impl SessionMailboxProgress {
    fn epochs(&self) -> [u64; 2] {
        self.epochs
            .each_ref()
            .map(|epoch| epoch.load(Ordering::Acquire))
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct SessionMailboxObserverBusy;

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum SessionMailboxCapacityChange {
    Control,
    OrderedSource,
    Both,
    Closed,
    EpochExhausted,
}

/// Non-Clone lease on the mailbox's sole capacity observer. It retains only
/// metadata, not a sender, receiver, command, task, or capacity reservation.
pub(crate) struct SessionMailboxCapacityObservation {
    // Declaration order is the destruction contract: remove the cached Waker
    // before another observer may acquire the exclusive lease.
    registration: Pin<Box<OwnedNotified>>,
    lease: SessionMailboxObserverLease,
    seen: [u64; 2],
}

impl SessionMailboxCapacityObservation {
    pub(crate) async fn changed(&mut self) -> SessionMailboxCapacityChange {
        poll_fn(|cx| self.poll_changed(cx)).await
    }

    fn poll_changed(&mut self, cx: &mut Context<'_>) -> Poll<SessionMailboxCapacityChange> {
        // This closed graph has one Inbox writer and broadcast-only progress.
        // At most one already-observed broadcast can arrive late: <=2 cached
        // polls and <=3 state checks; no self-wake or synthetic Pending. This
        // bounds calls, not dependency lock latency or a process-wide park.
        loop {
            let terminal = self.lease.progress.terminal.load(Ordering::Acquire);
            if terminal == MAILBOX_EPOCH_EXHAUSTED {
                self.reset_registration();
                return Poll::Ready(SessionMailboxCapacityChange::EpochExhausted);
            }
            if terminal == MAILBOX_CLOSED {
                self.reset_registration();
                return Poll::Ready(SessionMailboxCapacityChange::Closed);
            }
            let epochs = self.lease.progress.epochs();
            let changed = match (epochs[0] != self.seen[0], epochs[1] != self.seen[1]) {
                (true, true) => Some(SessionMailboxCapacityChange::Both),
                (true, false) => Some(SessionMailboxCapacityChange::Control),
                (false, true) => Some(SessionMailboxCapacityChange::OrderedSource),
                (false, false) => None,
            };
            if let Some(changed) = changed {
                // Reset can drop a user Waker. Do not consume epochs until
                // that operation finishes, so unwind permits exact re-entry.
                self.reset_registration();
                self.seen = epochs;
                return Poll::Ready(changed);
            }
            // OwnedNotified captures broadcast count at creation. Poll either
            // observes a raced broadcast or installs the persistent Waker;
            // cancellation of changed() no longer unlinks that registration.
            if self.registration.as_mut().poll(cx).is_pending() {
                return Poll::Pending;
            }
            self.reset_registration();
        }
    }

    fn reset_registration(&mut self) {
        // Reuse the sole pinned allocation, including after a fused Ready.
        self.registration
            .as_mut()
            .set(Arc::clone(&self.lease.progress.notify).notified_owned());
    }
}

struct SessionMailboxObserverLease {
    progress: Arc<SessionMailboxProgress>,
}

impl Drop for SessionMailboxObserverLease {
    fn drop(&mut self) {
        self.progress.observer_held.store(false, Ordering::Release);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum SessionMailboxLane {
    Control,
    OrderedSource,
}

impl SessionMailboxLane {
    fn index(self) -> usize {
        match self {
            Self::Control => 0,
            Self::OrderedSource => 1,
        }
    }
}

impl SessionOwnerInbox {
    pub(crate) async fn recv_next(&mut self) -> Option<SessionOwnerCommand> {
        std::future::poll_fn(|cx| self.poll_next(cx)).await
    }

    /// One Owner-selected lane only. The checked Controller stores the returned
    /// command immediately in Held before validating or reducing it. A capacity
    /// callback unwind leaves this exact command in the original prefetch.
    /// Bound callers never mix this operation with the legacy mixed receive.
    pub(super) fn poll_selected(
        &mut self,
        lane: SessionMailboxLane,
        cx: &mut Context<'_>,
    ) -> Poll<Option<SessionOwnerCommand>> {
        if let Some(command) = self.prefetch[lane.index()].take() {
            return Poll::Ready(Some(command));
        }
        // The wrapper contains exactly one raw receive, not prefetch, capacity
        // notification or caller work. Only private try-send producers exist;
        // returning its permit cannot invoke an async sender's user Waker.
        let received = {
            let receiver = match lane {
                SessionMailboxLane::Control => &mut self.control_rx,
                SessionMailboxLane::OrderedSource => &mut self.data_rx,
            };
            let mut receive = std::pin::pin!(tokio::task::unconstrained(poll_fn(|cx| {
                receiver.poll_recv(cx)
            })));
            receive.as_mut().poll(cx)
        };
        match received {
            Poll::Ready(Some(command)) => {
                self.prefetch[lane.index()] = Some(command);
                self.control_streak = match lane {
                    SessionMailboxLane::Control => self.control_streak.saturating_add(1),
                    SessionMailboxLane::OrderedSource => 0,
                };
                self.publish_capacity(lane);
                Poll::Ready(self.prefetch[lane.index()].take())
            }
            // None closes only this lane. It is neither the other lane's EOF
            // nor proof that either prefetch or the complete mailbox is empty.
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<SessionOwnerCommand>> {
        // Only the selected lane is prefetched in one poll. If its notification
        // unwound, deliver that selection before touching either channel again.
        for held in &mut self.prefetch {
            if let Some(command) = held.take() {
                return Poll::Ready(Some(command));
            }
        }
        let order = if self.control_streak >= CONTROL_BURST_LIMIT {
            [
                SessionMailboxLane::OrderedSource,
                SessionMailboxLane::Control,
            ]
        } else {
            [
                SessionMailboxLane::Control,
                SessionMailboxLane::OrderedSource,
            ]
        };
        let mut all_closed = true;
        // At most two channel polls. Unlike try_recv's Busy path, this does
        // not park the owner thread waiting for an in-flight producer push.
        // The private try-only ports cannot register semaphore waiters, so
        // permit return cannot invoke an async sender's user Waker here.
        for lane in order {
            let received = match lane {
                SessionMailboxLane::Control => self.control_rx.poll_recv(cx),
                SessionMailboxLane::OrderedSource => self.data_rx.poll_recv(cx),
            };
            match received {
                Poll::Ready(Some(command)) => {
                    self.prefetch[lane.index()] = Some(command);
                    self.control_streak = match lane {
                        SessionMailboxLane::Control => self.control_streak.saturating_add(1),
                        SessionMailboxLane::OrderedSource => 0,
                    };
                    self.publish_capacity(lane);
                    return Poll::Ready(self.prefetch[lane.index()].take());
                }
                Poll::Ready(None) => {}
                Poll::Pending => all_closed = false,
            }
        }
        if all_closed {
            self.close();
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }

    /// Prevents new sends while retaining every already-queued event for the
    /// sole owner to drain through [`Self::recv_next`].
    pub(crate) fn close(&mut self) {
        // One monotonic terminal word ends both logical lanes before any wake.
        // A send that passed its precheck already still uses its actual result.
        let previous = self
            .progress
            .terminal
            .fetch_max(MAILBOX_CLOSED, Ordering::AcqRel);
        self.close_channels();
        if previous == MAILBOX_OPEN {
            self.progress.notify.notify_waiters();
        }
    }

    fn close_channels(&mut self) {
        // Raw sender ports cannot install semaphore/closed waiters. Both
        // physical closes therefore precede our sole capacity observer wake.
        self.control_rx.close();
        self.data_rx.close();
    }

    fn publish_capacity(&mut self, lane: SessionMailboxLane) {
        if self.progress.terminal.load(Ordering::Acquire) == MAILBOX_EPOCH_EXHAUSTED {
            return;
        }
        let epoch = &self.progress.epochs[lane.index()];
        match epoch.load(Ordering::Relaxed).checked_add(1) {
            Some(next) => epoch.store(next, Ordering::Release),
            None => {
                self.progress
                    .terminal
                    .store(MAILBOX_EPOCH_EXHAUSTED, Ordering::Release);
                self.close_channels();
            }
        }
        // A single exclusive observer + mutable changed() borrow bounds this
        // Notify to one waiter. Panic cannot skip another observer's Waker.
        // The actual command and streak already belong to the inbox above.
        self.progress.notify.notify_waiters();
    }
}

impl Drop for SessionOwnerInbox {
    fn drop(&mut self) {
        // Normal destruction must terminate an observation that outlives us.
        // This is no drain/clean-zero claim; double-panic abort is not recovery.
        self.close();
    }
}

impl fmt::Debug for SessionOwnerInbox {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SessionOwnerInbox([REDACTED])")
    }
}

pub(crate) enum SessionOwnerCommand {
    Event(SessionEvent),
    Driver(DriverInput),
    ClientDriver(ClientControllerDriverInput),
}

impl SessionOwnerCommand {
    pub(crate) fn is_ordered_source(&self) -> bool {
        match self {
            Self::Event(event) => is_ordered_source_event(event),
            Self::Driver(DriverInput::Data(_)) => true,
            Self::Driver(DriverInput::Control(event)) => is_ordered_source_event(event),
            Self::ClientDriver(input) => match input.as_inner() {
                DriverInput::Data(_) => true,
                DriverInput::Control(event) => is_ordered_source_event(event),
            },
        }
    }
}

impl fmt::Debug for SessionOwnerCommand {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Event(event) => formatter.debug_tuple("Event").field(event).finish(),
            Self::Driver(input) => formatter.debug_tuple("Driver").field(input).finish(),
            Self::ClientDriver(input) => {
                formatter.debug_tuple("ClientDriver").field(input).finish()
            }
        }
    }
}

fn is_ordered_source_event(event: &SessionEvent) -> bool {
    // CLOSE freezes the final byte offset, so it must share FIFO ownership
    // with preceding DATA from the same source. RESET remains urgent control:
    // it abandons queued source bytes instead of assigning a final offset.
    matches!(
        event,
        SessionEvent::LocalData { .. } | SessionEvent::LocalClose { .. }
    ) || matches!(
        event,
        SessionEvent::PeerFrame { frame, .. }
            if matches!(frame.record(), Record::Data { .. } | Record::Close { .. })
    )
}

pub(crate) enum SessionMailboxTrySendError {
    Full(Box<SessionOwnerCommand>),
    Closed(Box<SessionOwnerCommand>),
}

impl SessionMailboxTrySendError {
    pub(crate) fn into_command(self) -> SessionOwnerCommand {
        match self {
            Self::Full(command) | Self::Closed(command) => *command,
        }
    }
}

impl fmt::Debug for SessionMailboxTrySendError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full(_) => formatter.write_str("SessionMailboxTrySendError::Full([REDACTED])"),
            Self::Closed(_) => {
                formatter.write_str("SessionMailboxTrySendError::Closed([REDACTED])")
            }
        }
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub(crate) enum SessionMailboxConfigError {
    #[error("session owner control lane capacity must be non-zero")]
    ZeroControlCapacity,
    #[error("session owner data lane capacity must be non-zero")]
    ZeroDataCapacity,
    #[error("session owner control lane capacity {requested} exceeds Tokio's maximum {max}")]
    ControlCapacityTooLarge { requested: usize, max: usize },
    #[error("session owner data lane capacity {requested} exceeds Tokio's maximum {max}")]
    DataCapacityTooLarge { requested: usize, max: usize },
}

#[cfg(test)]
mod tests {
    mod progress;

    use super::*;
    use crate::resumable::{
        AttachAlpn, AttachCredentials, AttachNonce, AttachPolicy, AttachRequest,
        AttachTransportBinding, ByteOffset, DevicePrincipal, DeviceSecret, Direction, FeatureOffer,
        FeatureSet, Frame, LegGeneration, OwnerIdentity, ReceiveBudgetLimits, Record,
        ReplayBudgetLimits, ResetReason, ResumeSecret, SESSION_PROTOCOL_VERSION, SessionConfig,
        SessionEffect, SessionFlowId, SessionId, SessionModel, SessionRole, TcpWindowLimits,
        TlsExporterBinding, VersionRange,
    };
    use crate::shared::TargetAddr;
    use bytes::Bytes;
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

    fn credentials() -> AttachCredentials {
        AttachCredentials::new(
            DeviceSecret::new([0xde; 32]).unwrap(),
            ResumeSecret::new([0xad; 32]).unwrap(),
        )
        .unwrap()
    }

    fn committed_leg() -> crate::resumable::CommittedLeg {
        let session_id = SessionId::new([0x11; 16]).unwrap();
        let owner = OwnerIdentity::new([0x31; 32]).unwrap();
        let alpn = AttachAlpn::new(b"mini-vpn-owned/1").unwrap();
        let principal = DevicePrincipal::new([0x53; 16]).unwrap();
        let binding = AttachTransportBinding::new(
            owner,
            alpn,
            TlsExporterBinding::new([0x42; 32]).unwrap(),
            principal,
        );
        let request = AttachRequest::new(
            session_id,
            LegGeneration::new(2).unwrap(),
            AttachNonce::new([0x22; 16]).unwrap(),
            VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION).unwrap(),
            FeatureOffer::new(0b111, 0b001).unwrap(),
        );
        let proof = credentials().prove(&request, &binding).unwrap();
        crate::resumable::AttachAuthority::new(
            session_id,
            LegGeneration::new(1).unwrap(),
            credentials(),
            AttachPolicy::new(owner, alpn, principal, SESSION_PROTOCOL_VERSION, 0b1111).unwrap(),
        )
        .verify_and_commit(&request, &binding, &proof)
        .unwrap()
    }

    fn peer_frame(record: Record) -> SessionEvent {
        let leg = committed_leg();
        SessionEvent::PeerFrame {
            leg,
            frame: Frame::try_new(leg.generation(), record).unwrap(),
        }
    }

    fn peer_data(secret: &'static [u8]) -> SessionEvent {
        peer_frame(Record::Data {
            flow_id: SessionFlowId::new(1).unwrap(),
            direction: Direction::TargetToClient,
            offset: ByteOffset::new(0),
            payload: Bytes::from_static(secret),
        })
    }

    fn peer_ack() -> SessionEvent {
        peer_frame(Record::Ack {
            flow_id: SessionFlowId::new(1).unwrap(),
            direction: Direction::ClientToTarget,
            next_accepted: ByteOffset::new(1),
            final_accepted: false,
        })
    }

    fn config() -> SessionConfig {
        SessionConfig::new(
            4,
            4,
            TcpWindowLimits::new(64, 8).unwrap(),
            TcpWindowLimits::new(64, 8).unwrap(),
            ReplayBudgetLimits::new(64, 8).unwrap(),
            ReplayBudgetLimits::new(64, 8).unwrap(),
            ReceiveBudgetLimits::new(128, 16).unwrap(),
            64,
        )
        .unwrap()
    }

    fn local_flow_and_offer() -> (crate::resumable::LocalFlow, crate::resumable::SinkOffer) {
        let leg = committed_leg();
        let mut model = SessionModel::new(SessionRole::Client, config(), leg);
        let effects = model
            .reduce(SessionEvent::LocalOpen {
                leg,
                target: TargetAddr::IpPort(SocketAddr::V4(SocketAddrV4::new(
                    Ipv4Addr::LOCALHOST,
                    443,
                ))),
            })
            .unwrap();
        let flow = effects
            .iter()
            .find_map(|effect| match effect {
                SessionEffect::LocalFlowOpened { flow } => Some(*flow),
                _ => None,
            })
            .unwrap();
        model
            .reduce(SessionEvent::PeerFrame {
                leg,
                frame: Frame::try_new(
                    leg.generation(),
                    Record::OpenResult {
                        flow_id: flow.flow_id(),
                        result: crate::resumable::OpenResultCode::Opened,
                    },
                )
                .unwrap(),
            })
            .unwrap();
        let effects = model
            .reduce(SessionEvent::PeerFrame {
                leg,
                frame: Frame::try_new(
                    leg.generation(),
                    Record::Data {
                        flow_id: flow.flow_id(),
                        direction: Direction::TargetToClient,
                        offset: ByteOffset::new(0),
                        payload: Bytes::from_static(b"offer"),
                    },
                )
                .unwrap(),
            })
            .unwrap();
        let offer = effects
            .iter()
            .find_map(|effect| match effect {
                SessionEffect::OfferToSink { offer, .. } => Some(*offer),
                _ => None,
            })
            .unwrap();
        (flow, offer)
    }

    fn peer_attach_accepted() -> SessionEvent {
        peer_frame(Record::AttachAccepted {
            session_id: SessionId::new([0x11; 16]).unwrap(),
            nonce: AttachNonce::new([0x22; 16]).unwrap(),
            selected_version: SESSION_PROTOCOL_VERSION,
            features: FeatureSet::new(0b111),
        })
    }

    fn peer_close() -> SessionEvent {
        peer_frame(Record::Close {
            flow_id: SessionFlowId::new(1).unwrap(),
            direction: Direction::TargetToClient,
            final_offset: ByteOffset::new(1),
        })
    }

    fn peer_reset() -> SessionEvent {
        peer_frame(Record::Reset {
            flow_id: SessionFlowId::new(1).unwrap(),
            reason: ResetReason::TargetFailure,
        })
    }

    async fn recv_event(inbox: &mut SessionOwnerInbox) -> Option<SessionEvent> {
        match inbox.recv_next().await {
            Some(SessionOwnerCommand::Event(event))
            | Some(SessionOwnerCommand::Driver(DriverInput::Control(event))) => Some(event),
            Some(SessionOwnerCommand::Driver(DriverInput::Data(_))) => {
                panic!("expected an event command, got owned DATA")
            }
            Some(SessionOwnerCommand::ClientDriver(_)) => {
                panic!("session mailbox cannot receive a controller-only typed driver command")
            }
            None => None,
        }
    }

    #[test]
    fn invalid_lane_capacities_are_rejected_without_entering_tokio_panics() {
        assert!(matches!(
            SessionOwnerMailbox::bounded(0, 1),
            Err(SessionMailboxConfigError::ZeroControlCapacity)
        ));
        assert!(matches!(
            SessionOwnerMailbox::bounded(1, 0),
            Err(SessionMailboxConfigError::ZeroDataCapacity)
        ));
        assert!(matches!(
            SessionOwnerMailbox::bounded(usize::MAX, 1),
            Err(SessionMailboxConfigError::ControlCapacityTooLarge { .. })
        ));
        assert!(matches!(
            SessionOwnerMailbox::bounded(1, usize::MAX),
            Err(SessionMailboxConfigError::DataCapacityTooLarge { .. })
        ));
    }

    #[tokio::test]
    async fn a_full_data_lane_does_not_block_control_and_control_is_received_first() {
        let (ingress, mut inbox) = SessionOwnerMailbox::bounded(2, 1).unwrap();
        ingress.try_send_event(peer_data(b"first-secret")).unwrap();
        assert!(matches!(
            ingress.try_send_event(peer_data(b"second-secret")),
            Err(SessionMailboxTrySendError::Full(_))
        ));
        ingress.try_send_event(peer_ack()).unwrap();

        assert!(matches!(
            recv_event(&mut inbox).await,
            Some(SessionEvent::PeerFrame { frame, .. })
                if matches!(frame.record(), Record::Ack { .. })
        ));
        assert!(matches!(
            recv_event(&mut inbox).await,
            Some(SessionEvent::PeerFrame { frame, .. })
                if matches!(frame.record(), Record::Data { .. })
        ));
    }

    #[tokio::test]
    async fn event_classification_is_internal_and_covers_data_and_control_families() {
        let (ingress, mut inbox) = SessionOwnerMailbox::bounded(5, 1).unwrap();
        let (flow, offer) = local_flow_and_offer();
        ingress
            .try_send_event(peer_data(b"fill-data-lane"))
            .unwrap();
        assert!(matches!(
            ingress.try_send_event(SessionEvent::LocalData {
                flow,
                payload: Bytes::from_static(b"local-secret"),
            }),
            Err(SessionMailboxTrySendError::Full(_))
        ));

        ingress.try_send_event(peer_ack()).unwrap();
        ingress.try_send_event(peer_attach_accepted()).unwrap();
        ingress.try_send_event(peer_reset()).unwrap();
        ingress
            .try_send_event(SessionEvent::SinkAccepted { offer, bytes: 1 })
            .unwrap();

        assert!(matches!(
            recv_event(&mut inbox).await,
            Some(SessionEvent::PeerFrame { frame, .. }) if matches!(frame.record(), Record::Ack { .. })
        ));
        assert!(matches!(
            recv_event(&mut inbox).await,
            Some(SessionEvent::PeerFrame { frame, .. }) if matches!(frame.record(), Record::AttachAccepted { .. })
        ));
        assert!(matches!(
            recv_event(&mut inbox).await,
            Some(SessionEvent::PeerFrame { frame, .. }) if matches!(frame.record(), Record::Reset { .. })
        ));
        assert!(matches!(
            recv_event(&mut inbox).await,
            Some(SessionEvent::SinkAccepted { .. })
        ));
        assert!(matches!(
            recv_event(&mut inbox).await,
            Some(SessionEvent::PeerFrame { frame, .. }) if matches!(frame.record(), Record::Data { .. })
        ));
    }

    #[tokio::test]
    async fn close_shares_fifo_with_preceding_data_in_both_directions() {
        let (local_ingress, mut local_inbox) = SessionOwnerMailbox::bounded(1, 2).unwrap();
        let (flow, _) = local_flow_and_offer();
        local_ingress
            .try_send_event(SessionEvent::LocalData {
                flow,
                payload: Bytes::from_static(b"local-before-close"),
            })
            .unwrap();
        local_ingress
            .try_send_event(SessionEvent::LocalClose { flow })
            .unwrap();

        assert!(matches!(
            recv_event(&mut local_inbox).await,
            Some(SessionEvent::LocalData { .. })
        ));
        assert!(matches!(
            recv_event(&mut local_inbox).await,
            Some(SessionEvent::LocalClose { .. })
        ));

        let (peer_ingress, mut peer_inbox) = SessionOwnerMailbox::bounded(1, 2).unwrap();
        peer_ingress
            .try_send_event(peer_data(b"peer-before-close"))
            .unwrap();
        peer_ingress.try_send_event(peer_close()).unwrap();

        assert!(matches!(
            recv_event(&mut peer_inbox).await,
            Some(SessionEvent::PeerFrame { frame, .. })
                if matches!(frame.record(), Record::Data { .. })
        ));
        assert!(matches!(
            recv_event(&mut peer_inbox).await,
            Some(SessionEvent::PeerFrame { frame, .. })
                if matches!(frame.record(), Record::Close { .. })
        ));
    }

    #[tokio::test]
    async fn continuous_control_cannot_starve_an_already_queued_source_event() {
        let (ingress, mut inbox) = SessionOwnerMailbox::bounded(2, 1).unwrap();
        ingress.try_send_event(peer_data(b"must-progress")).unwrap();
        ingress.try_send_event(peer_ack()).unwrap();

        let mut controls_seen = 0usize;
        loop {
            match recv_event(&mut inbox).await {
                Some(SessionEvent::PeerFrame { frame, .. })
                    if matches!(frame.record(), Record::Data { .. }) =>
                {
                    break;
                }
                Some(_) => {
                    controls_seen = controls_seen.saturating_add(1);
                    ingress.try_send_event(peer_ack()).unwrap();
                }
                None => panic!("mailbox closed before queued source progress"),
            }
            assert!(
                controls_seen <= CONTROL_BURST_LIMIT,
                "ordinary control replenishment starved source ownership"
            );
        }
        assert_eq!(controls_seen, CONTROL_BURST_LIMIT);
    }

    #[tokio::test]
    async fn queued_control_remains_low_latency_under_source_pressure() {
        let (ingress, mut inbox) = SessionOwnerMailbox::bounded(1, 2).unwrap();
        ingress.try_send_event(peer_data(b"bulk-one")).unwrap();
        ingress.try_send_event(peer_data(b"bulk-two")).unwrap();
        ingress.try_send_event(peer_ack()).unwrap();

        assert!(matches!(
            recv_event(&mut inbox).await,
            Some(SessionEvent::PeerFrame { frame, .. })
                if matches!(frame.record(), Record::Ack { .. })
        ));
    }

    #[tokio::test]
    async fn close_rejects_new_ingress_and_drains_both_lanes_without_loss() {
        let (ingress, mut inbox) = SessionOwnerMailbox::bounded(2, 2).unwrap();
        let cloned_ingress = ingress.clone();
        ingress.try_send_event(peer_data(b"queued-data")).unwrap();
        ingress.try_send_event(peer_ack()).unwrap();

        inbox.close();
        let error = cloned_ingress.try_send_event(peer_ack()).unwrap_err();
        assert!(matches!(error, SessionMailboxTrySendError::Closed(_)));
        assert!(matches!(
            error.into_command(),
            SessionOwnerCommand::Event(SessionEvent::PeerFrame { frame, .. })
                if matches!(frame.record(), Record::Ack { .. })
        ));

        assert!(matches!(
            recv_event(&mut inbox).await,
            Some(SessionEvent::PeerFrame { frame, .. }) if matches!(frame.record(), Record::Ack { .. })
        ));
        assert!(matches!(
            recv_event(&mut inbox).await,
            Some(SessionEvent::PeerFrame { frame, .. }) if matches!(frame.record(), Record::Data { .. })
        ));
        assert!(recv_event(&mut inbox).await.is_none());
    }

    #[tokio::test]
    async fn mailbox_debug_and_send_errors_never_expose_frame_or_payload() {
        const SECRET: &[u8] = b"MAILBOX_PAYLOAD_MUST_NOT_LEAK";
        let (ingress, mut inbox) = SessionOwnerMailbox::bounded(1, 1).unwrap();
        assert_eq!(format!("{ingress:?}"), "SessionOwnerIngress([REDACTED])");
        assert_eq!(format!("{inbox:?}"), "SessionOwnerInbox([REDACTED])");

        ingress.try_send_event(peer_data(b"fill")).unwrap();
        let full = ingress.try_send_event(peer_data(SECRET)).unwrap_err();
        assert_eq!(
            format!("{full:?}"),
            "SessionMailboxTrySendError::Full([REDACTED])"
        );
        assert!(!format!("{full:?}").contains("MAILBOX_PAYLOAD"));
        let _recovered = full.into_command();

        inbox.close();
        let closed = ingress.try_send_event(peer_data(SECRET)).unwrap_err();
        assert_eq!(
            format!("{closed:?}"),
            "SessionMailboxTrySendError::Closed([REDACTED])"
        );
        assert!(!format!("{closed:?}").contains("MAILBOX_PAYLOAD"));
        let _recovered = closed.into_command();
    }

    // The sole receiver must stay non-cloneable even though ingress is
    // intentionally fan-out cloneable. This becomes ambiguous at compile time
    // if a future change implements Clone for SessionOwnerInbox.
    trait AmbiguousIfClone<Marker> {
        fn marker() {}
    }

    impl<T: ?Sized> AmbiguousIfClone<()> for T {}
    impl<T: Clone> AmbiguousIfClone<u8> for T {}

    const _: fn() = || {
        let _ = <SessionOwnerInbox as AmbiguousIfClone<_>>::marker;
    };
}
