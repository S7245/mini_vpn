//! Bounded, priority-aware ingress for one resumable session owner.

use crate::resumable::{Record, SessionEvent};
use std::fmt;
use thiserror::Error;
use tokio::sync::mpsc;

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
        Ok((
            SessionOwnerIngress {
                control_tx,
                data_tx,
            },
            SessionOwnerInbox {
                control_rx,
                data_rx,
                control_streak: 0,
            },
        ))
    }
}

#[derive(Clone)]
pub(crate) struct SessionOwnerIngress {
    control_tx: mpsc::Sender<SessionEvent>,
    data_tx: mpsc::Sender<SessionEvent>,
}

impl SessionOwnerIngress {
    pub(crate) async fn send(&self, event: SessionEvent) -> Result<(), SessionMailboxSendError> {
        let sender = if is_ordered_source_event(&event) {
            &self.data_tx
        } else {
            &self.control_tx
        };
        sender
            .send(event)
            .await
            .map_err(|error| SessionMailboxSendError {
                event: Box::new(error.0),
            })
    }

    pub(crate) fn try_send(&self, event: SessionEvent) -> Result<(), SessionMailboxTrySendError> {
        let sender = if is_ordered_source_event(&event) {
            &self.data_tx
        } else {
            &self.control_tx
        };
        sender.try_send(event).map_err(|error| match error {
            mpsc::error::TrySendError::Full(event) => {
                SessionMailboxTrySendError::Full(Box::new(event))
            }
            mpsc::error::TrySendError::Closed(event) => {
                SessionMailboxTrySendError::Closed(Box::new(event))
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
    control_rx: mpsc::Receiver<SessionEvent>,
    data_rx: mpsc::Receiver<SessionEvent>,
    control_streak: usize,
}

impl SessionOwnerInbox {
    pub(crate) async fn recv_next(&mut self) -> Option<SessionEvent> {
        let mut control_closed = false;
        let mut data_closed = false;
        loop {
            let source_due = self.control_streak >= CONTROL_BURST_LIMIT;
            if source_due {
                match self.data_rx.try_recv() {
                    Ok(event) => {
                        self.control_streak = 0;
                        return Some(event);
                    }
                    Err(mpsc::error::TryRecvError::Disconnected) => data_closed = true,
                    Err(mpsc::error::TryRecvError::Empty) => {}
                }
            }
            if !control_closed {
                match self.control_rx.try_recv() {
                    Ok(event) => {
                        self.control_streak = self.control_streak.saturating_add(1);
                        return Some(event);
                    }
                    Err(mpsc::error::TryRecvError::Disconnected) => control_closed = true,
                    Err(mpsc::error::TryRecvError::Empty) => {}
                }
            }
            if !data_closed {
                match self.data_rx.try_recv() {
                    Ok(event) => {
                        self.control_streak = 0;
                        return Some(event);
                    }
                    Err(mpsc::error::TryRecvError::Disconnected) => data_closed = true,
                    Err(mpsc::error::TryRecvError::Empty) => {}
                }
            }
            if control_closed && data_closed {
                return None;
            }

            if source_due && !data_closed {
                tokio::select! {
                    biased;
                    event = self.data_rx.recv() => {
                        match event {
                            Some(event) => {
                                self.control_streak = 0;
                                return Some(event);
                            }
                            None => data_closed = true,
                        }
                    }
                    event = self.control_rx.recv(), if !control_closed => {
                        match event {
                            Some(event) => {
                                self.control_streak = self.control_streak.saturating_add(1);
                                return Some(event);
                            }
                            None => control_closed = true,
                        }
                    }
                    else => return None,
                }
            } else {
                tokio::select! {
                    biased;
                    event = self.control_rx.recv(), if !control_closed => {
                        match event {
                            Some(event) => {
                                self.control_streak = self.control_streak.saturating_add(1);
                                return Some(event);
                            }
                            None => control_closed = true,
                        }
                    }
                    event = self.data_rx.recv(), if !data_closed => {
                        match event {
                            Some(event) => {
                                self.control_streak = 0;
                                return Some(event);
                            }
                            None => data_closed = true,
                        }
                    }
                    else => return None,
                }
            }
        }
    }

    /// Prevents new sends while retaining every already-queued event for the
    /// sole owner to drain through [`Self::recv_next`].
    pub(crate) fn close(&mut self) {
        self.control_rx.close();
        self.data_rx.close();
    }
}

impl fmt::Debug for SessionOwnerInbox {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SessionOwnerInbox([REDACTED])")
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

pub(crate) struct SessionMailboxSendError {
    event: Box<SessionEvent>,
}

impl SessionMailboxSendError {
    pub(crate) fn into_event(self) -> SessionEvent {
        *self.event
    }
}

impl fmt::Debug for SessionMailboxSendError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SessionMailboxSendError([REDACTED])")
    }
}

impl fmt::Display for SessionMailboxSendError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("session owner mailbox is closed")
    }
}

impl std::error::Error for SessionMailboxSendError {}

pub(crate) enum SessionMailboxTrySendError {
    Full(Box<SessionEvent>),
    Closed(Box<SessionEvent>),
}

impl SessionMailboxTrySendError {
    pub(crate) fn into_event(self) -> SessionEvent {
        match self {
            Self::Full(event) | Self::Closed(event) => *event,
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
        ingress.try_send(peer_data(b"first-secret")).unwrap();
        assert!(matches!(
            ingress.try_send(peer_data(b"second-secret")),
            Err(SessionMailboxTrySendError::Full(_))
        ));
        ingress.try_send(peer_ack()).unwrap();

        assert!(matches!(
            inbox.recv_next().await,
            Some(SessionEvent::PeerFrame { frame, .. })
                if matches!(frame.record(), Record::Ack { .. })
        ));
        assert!(matches!(
            inbox.recv_next().await,
            Some(SessionEvent::PeerFrame { frame, .. })
                if matches!(frame.record(), Record::Data { .. })
        ));
    }

    #[tokio::test]
    async fn event_classification_is_internal_and_covers_data_and_control_families() {
        let (ingress, mut inbox) = SessionOwnerMailbox::bounded(5, 1).unwrap();
        let (flow, offer) = local_flow_and_offer();
        ingress.try_send(peer_data(b"fill-data-lane")).unwrap();
        assert!(matches!(
            ingress.try_send(SessionEvent::LocalData {
                flow,
                payload: Bytes::from_static(b"local-secret"),
            }),
            Err(SessionMailboxTrySendError::Full(_))
        ));

        ingress.try_send(peer_ack()).unwrap();
        ingress.try_send(peer_attach_accepted()).unwrap();
        ingress.try_send(peer_reset()).unwrap();
        ingress
            .try_send(SessionEvent::SinkAccepted { offer, bytes: 1 })
            .unwrap();

        assert!(matches!(
            inbox.recv_next().await,
            Some(SessionEvent::PeerFrame { frame, .. }) if matches!(frame.record(), Record::Ack { .. })
        ));
        assert!(matches!(
            inbox.recv_next().await,
            Some(SessionEvent::PeerFrame { frame, .. }) if matches!(frame.record(), Record::AttachAccepted { .. })
        ));
        assert!(matches!(
            inbox.recv_next().await,
            Some(SessionEvent::PeerFrame { frame, .. }) if matches!(frame.record(), Record::Reset { .. })
        ));
        assert!(matches!(
            inbox.recv_next().await,
            Some(SessionEvent::SinkAccepted { .. })
        ));
        assert!(matches!(
            inbox.recv_next().await,
            Some(SessionEvent::PeerFrame { frame, .. }) if matches!(frame.record(), Record::Data { .. })
        ));
    }

    #[tokio::test]
    async fn close_shares_fifo_with_preceding_data_in_both_directions() {
        let (local_ingress, mut local_inbox) = SessionOwnerMailbox::bounded(1, 2).unwrap();
        let (flow, _) = local_flow_and_offer();
        local_ingress
            .try_send(SessionEvent::LocalData {
                flow,
                payload: Bytes::from_static(b"local-before-close"),
            })
            .unwrap();
        local_ingress
            .try_send(SessionEvent::LocalClose { flow })
            .unwrap();

        assert!(matches!(
            local_inbox.recv_next().await,
            Some(SessionEvent::LocalData { .. })
        ));
        assert!(matches!(
            local_inbox.recv_next().await,
            Some(SessionEvent::LocalClose { .. })
        ));

        let (peer_ingress, mut peer_inbox) = SessionOwnerMailbox::bounded(1, 2).unwrap();
        peer_ingress
            .try_send(peer_data(b"peer-before-close"))
            .unwrap();
        peer_ingress.try_send(peer_close()).unwrap();

        assert!(matches!(
            peer_inbox.recv_next().await,
            Some(SessionEvent::PeerFrame { frame, .. })
                if matches!(frame.record(), Record::Data { .. })
        ));
        assert!(matches!(
            peer_inbox.recv_next().await,
            Some(SessionEvent::PeerFrame { frame, .. })
                if matches!(frame.record(), Record::Close { .. })
        ));
    }

    #[tokio::test]
    async fn continuous_control_cannot_starve_an_already_queued_source_event() {
        let (ingress, mut inbox) = SessionOwnerMailbox::bounded(2, 1).unwrap();
        ingress.try_send(peer_data(b"must-progress")).unwrap();
        ingress.try_send(peer_ack()).unwrap();

        let mut controls_seen = 0usize;
        loop {
            match inbox.recv_next().await {
                Some(SessionEvent::PeerFrame { frame, .. })
                    if matches!(frame.record(), Record::Data { .. }) =>
                {
                    break;
                }
                Some(_) => {
                    controls_seen = controls_seen.saturating_add(1);
                    ingress.try_send(peer_ack()).unwrap();
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
        ingress.try_send(peer_data(b"bulk-one")).unwrap();
        ingress.try_send(peer_data(b"bulk-two")).unwrap();
        ingress.try_send(peer_ack()).unwrap();

        assert!(matches!(
            inbox.recv_next().await,
            Some(SessionEvent::PeerFrame { frame, .. })
                if matches!(frame.record(), Record::Ack { .. })
        ));
    }

    #[tokio::test]
    async fn close_rejects_new_ingress_and_drains_both_lanes_without_loss() {
        let (ingress, mut inbox) = SessionOwnerMailbox::bounded(2, 2).unwrap();
        let cloned_ingress = ingress.clone();
        ingress.try_send(peer_data(b"queued-data")).unwrap();
        ingress.try_send(peer_ack()).unwrap();

        inbox.close();
        let error = cloned_ingress.try_send(peer_ack()).unwrap_err();
        assert!(matches!(error, SessionMailboxTrySendError::Closed(_)));
        assert!(matches!(
            error.into_event(),
            SessionEvent::PeerFrame { frame, .. } if matches!(frame.record(), Record::Ack { .. })
        ));

        assert!(matches!(
            inbox.recv_next().await,
            Some(SessionEvent::PeerFrame { frame, .. }) if matches!(frame.record(), Record::Ack { .. })
        ));
        assert!(matches!(
            inbox.recv_next().await,
            Some(SessionEvent::PeerFrame { frame, .. }) if matches!(frame.record(), Record::Data { .. })
        ));
        assert!(inbox.recv_next().await.is_none());
    }

    #[tokio::test]
    async fn mailbox_debug_and_send_errors_never_expose_frame_or_payload() {
        const SECRET: &[u8] = b"MAILBOX_PAYLOAD_MUST_NOT_LEAK";
        let (ingress, mut inbox) = SessionOwnerMailbox::bounded(1, 1).unwrap();
        assert_eq!(format!("{ingress:?}"), "SessionOwnerIngress([REDACTED])");
        assert_eq!(format!("{inbox:?}"), "SessionOwnerInbox([REDACTED])");

        ingress.try_send(peer_data(b"fill")).unwrap();
        let full = ingress.try_send(peer_data(SECRET)).unwrap_err();
        assert_eq!(
            format!("{full:?}"),
            "SessionMailboxTrySendError::Full([REDACTED])"
        );
        assert!(!format!("{full:?}").contains("MAILBOX_PAYLOAD"));
        let _recovered = full.into_event();

        inbox.close();
        let closed = ingress.send(peer_data(SECRET)).await.unwrap_err();
        assert_eq!(format!("{closed:?}"), "SessionMailboxSendError([REDACTED])");
        assert!(!format!("{closed:?}").contains("MAILBOX_PAYLOAD"));
        let _recovered = closed.into_event();
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
