//! Owns one bounded Endpoint/Connection event lane and its exact slot layout.
//! It does not own QUIC protocol state or datagram backing. A terminal channel
//! reserves one extra slot for the Drained witness, unavailable to ordinary events.
//! A noncopying Permit reserves one slot before a producer mutates Proto; a send or
//! Drop settles it once. The receiver owns the slot storage until both handles drop.
//! Focused tests live in `budgeted_events/tests/mod.rs`.

use bytes::BackingMeter;
use proto::owned_events::{
    control_channel, terminal_channel, ControlKind, ControlPermit, Permit, Receiver, ReserveError,
    Sender,
};
use std::{
    io,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::sync::mpsc;

use crate::ConnectionEvent;

#[derive(Debug)]
enum SourceAuthority {
    Legacy,
    AwaitingPublication,
    Bound(proto::BoundRegistrationObserver),
}

/// Budgeted ordinary control events settle at their original Endpoint owner.
/// Drained keeps the original deferred removal/ACK path; it never locks the
/// Endpoint from Connection destruction. Its terminal slot is reserved cold.
#[derive(Debug)]
pub(crate) struct EndpointEventSender {
    deferred: mpsc::UnboundedSender<(proto::ConnectionHandle, crate::ConnectionToEndpointEvent)>,
    owner: Option<std::sync::Weak<super::EndpointInner>>,
    meter: Option<BackingMeter>,
    terminal: Option<Sender<crate::ConnectionToEndpointEvent>>,
    authority: SourceAuthority,
    pub(crate) ready: Option<(Arc<TerminalReady>, usize)>,
}

impl EndpointEventSender {
    pub(crate) fn is_synchronous(&self) -> bool {
        self.owner.is_some()
    }
    pub(crate) fn new(
        deferred: mpsc::UnboundedSender<(
            proto::ConnectionHandle,
            crate::ConnectionToEndpointEvent,
        )>,
        owner: Option<std::sync::Weak<super::EndpointInner>>,
        meter: Option<BackingMeter>,
        terminal: Option<Sender<crate::ConnectionToEndpointEvent>>,
    ) -> Self {
        Self {
            deferred,
            owner,
            meter,
            terminal,
            ready: None,
            authority: SourceAuthority::Legacy,
        }
    }

    pub(crate) fn prepare_registration(&mut self) {
        self.authority = SourceAuthority::AwaitingPublication;
    }
    pub(crate) fn bind_registration(&mut self, observer: proto::BoundRegistrationObserver) {
        self.authority = SourceAuthority::Bound(observer);
    }
    pub(crate) fn is_native(&self) -> bool {
        !matches!(self.authority, SourceAuthority::Legacy)
    }

    fn origin(
        &self,
        handle: proto::ConnectionHandle,
        key: Option<proto::RegistrationKey>,
    ) -> Result<Option<&proto::BoundRegistrationObserver>, ()> {
        match &self.authority {
            SourceAuthority::Legacy => Ok(None),
            SourceAuthority::Bound(observer) if observer.matches(handle, key) => Ok(Some(observer)),
            SourceAuthority::AwaitingPublication | SourceAuthority::Bound(_) => {
                self.quarantine();
                Err(())
            }
        }
    }

    fn quarantine(&self) {
        if let Some(meter) = &self.meter {
            meter.quarantine();
        }
    }

    /// The caller holds the actual Connection lock through Root publication and
    /// reply installation. Native ordinary controls have no deferred fallback.
    pub(crate) fn send_protocol(
        &self,
        handle: proto::ConnectionHandle,
        event: proto::EndpointEvent,
        connection: &mut proto::Connection,
    ) -> Result<(), ()> {
        if event.is_drained() || self.origin(handle, event.registration_key())?.is_none() {
            self.quarantine();
            return Err(());
        }
        let result = self
            .owner
            .as_ref()
            .and_then(|owner| owner.upgrade())
            .ok_or(())
            .and_then(|owner| owner.settle_connection_protocol(handle, event, connection));
        if result.is_err() {
            self.quarantine();
        }
        result
    }

    pub(crate) fn send(
        &self,
        mut message: (proto::ConnectionHandle, crate::ConnectionToEndpointEvent),
    ) -> Result<Option<proto::ConnectionEvent>, ()> {
        let key = match &message.1 {
            crate::ConnectionToEndpointEvent::Proto { event, .. } => event.registration_key(),
            crate::ConnectionToEndpointEvent::AuthenticatedCurrentSocketRx(key, _) => *key,
        };
        let observer = self.origin(message.0, key)?;
        let terminal = matches!(&message.1, crate::ConnectionToEndpointEvent::Proto { event, .. } if event.is_drained());
        if terminal {
            if let crate::ConnectionToEndpointEvent::Proto {
                endpoint_released, ..
            } = &mut message.1
            {
                if observer.is_some_and(|observer| observer.was_settled(message.0, key)) {
                    if let Some(ack) = endpoint_released.take() {
                        let _ = ack.send(());
                    }
                    return Ok(None);
                }
            }
        }
        let result = if terminal && self.owner.is_some() {
            self.terminal
                .as_ref()
                .ok_or(())
                .and_then(|slot| slot.send_terminal(message.1).map(|_| None).map_err(|_| ()))
        } else if let Some(owner) = self.owner.as_ref() {
            owner
                .upgrade()
                .ok_or(())
                .and_then(|owner| owner.settle_connection_event(message.0, message.1))
        } else {
            self.deferred.send(message).map(|_| None).map_err(|_| ())
        };
        if result.is_err() {
            if let Some(meter) = &self.meter {
                meter.quarantine();
            }
        }
        if terminal && result.is_ok() {
            if let Some((ready, index)) = &self.ready {
                ready.mark(*index);
            }
        }
        result
    }
}

/// One bounded Endpoint notification authority. Only terminal sends set bits;
/// healthy DATA polls never scan per-connection terminal receivers.
#[derive(Debug, Default)]
pub(crate) struct TerminalReady {
    bits: std::sync::atomic::AtomicU32,
    pub(crate) notify: Arc<tokio::sync::Notify>,
}

impl TerminalReady {
    fn mark(&self, index: usize) {
        self.bits
            .fetch_or(1 << index, std::sync::atomic::Ordering::Release);
        self.notify.notify_one();
    }

    pub(crate) fn take(&self) -> Option<usize> {
        use std::sync::atomic::Ordering;
        let mut bits = self.bits.load(Ordering::Acquire);
        while bits != 0 {
            let index = bits.trailing_zeros() as usize;
            match self.bits.compare_exchange_weak(
                bits,
                bits & !(1 << index),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Some(index),
                Err(current) => bits = current,
            }
        }
        None
    }
}

impl From<mpsc::UnboundedSender<(proto::ConnectionHandle, crate::ConnectionToEndpointEvent)>>
    for EndpointEventSender
{
    fn from(
        deferred: mpsc::UnboundedSender<(
            proto::ConnectionHandle,
            crate::ConnectionToEndpointEvent,
        )>,
    ) -> Self {
        Self::new(deferred, None, None, None)
    }
}

#[cfg(test)]
#[path = "budgeted_events/tests/mod.rs"]
mod tests;

/// The original transport path remains available when no shared hard budget is
/// installed. Budgeted endpoints use the owned fixed slot lane directly.
#[derive(Debug)]
pub(crate) enum ForwardSender {
    Legacy(mpsc::UnboundedSender<ConnectionEvent>),
    Budgeted(Sender<ConnectionEvent>),
}

#[derive(Debug)]
pub(crate) enum ForwardReceiver {
    Legacy(mpsc::UnboundedReceiver<ConnectionEvent>),
    Budgeted(Receiver<ConnectionEvent>),
}

pub(crate) enum ForwardPermit {
    Legacy,
    Budgeted(Permit<ConnectionEvent>),
}

pub(crate) enum ForwardControlPermit {
    Legacy,
    Budgeted(ControlPermit<ConnectionEvent>),
}

pub(crate) fn forward_channel(
    meter: Option<&BackingMeter>,
    ordinary_capacity: usize,
) -> io::Result<(ForwardSender, ForwardReceiver)> {
    if let Some(meter) = meter.filter(|meter| meter.budget_limit().is_some()) {
        let (sender, receiver) = control_channel(ordinary_capacity, meter)?;
        Ok((
            ForwardSender::Budgeted(sender),
            ForwardReceiver::Budgeted(receiver),
        ))
    } else {
        let (sender, receiver) = mpsc::unbounded_channel();
        Ok((
            ForwardSender::Legacy(sender),
            ForwardReceiver::Legacy(receiver),
        ))
    }
}

pub(crate) struct ConnectionChannels {
    pub(crate) forward: (ForwardSender, ForwardReceiver),
    pub(crate) terminal: Option<(
        Sender<crate::ConnectionToEndpointEvent>,
        Receiver<crate::ConnectionToEndpointEvent>,
    )>,
}

/// Both lane layouts are admitted before Proto creates connection/CID state.
pub(crate) fn connection_channels(
    meter: Option<&BackingMeter>,
    capacity: usize,
) -> io::Result<ConnectionChannels> {
    let forward = forward_channel(meter, capacity)?;
    let terminal = meter
        .filter(|meter| meter.budget_limit().is_some())
        .map(|meter| terminal_channel(0, meter))
        .transpose()?;
    Ok(ConnectionChannels { forward, terminal })
}

impl ForwardSender {
    pub(crate) fn poll_reserve_proto(
        &self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<ForwardPermit, ReserveError>> {
        match self {
            Self::Legacy(_) => Poll::Ready(Ok(ForwardPermit::Legacy)),
            Self::Budgeted(sender) => sender
                .poll_reserve(cx)
                .map(|r| r.map(ForwardPermit::Budgeted)),
        }
    }
    pub(crate) fn reserve_proto(&self) -> Result<ForwardPermit, ReserveError> {
        match self {
            Self::Legacy(_) => Ok(ForwardPermit::Legacy),
            Self::Budgeted(sender) => sender.try_reserve().map(ForwardPermit::Budgeted),
        }
    }

    pub(crate) fn reserve_control(
        &self,
        kind: ControlKind,
    ) -> Result<ForwardControlPermit, ReserveError> {
        match self {
            Self::Legacy(_) => Ok(ForwardControlPermit::Legacy),
            Self::Budgeted(sender) => sender
                .try_reserve_control(kind)
                .map(ForwardControlPermit::Budgeted),
        }
    }

    pub(crate) fn send_proto(
        &self,
        permit: ForwardPermit,
        event: ConnectionEvent,
    ) -> Result<(), ConnectionEvent> {
        match (self, permit) {
            (Self::Legacy(sender), ForwardPermit::Legacy) => sender.send(event).map_err(|e| e.0),
            (Self::Budgeted(_), ForwardPermit::Budgeted(permit)) => permit.send(event),
            _ => Err(event),
        }
    }

    pub(crate) fn send_control(
        &self,
        permit: ForwardControlPermit,
        event: ConnectionEvent,
    ) -> Result<(), ConnectionEvent> {
        match (self, permit) {
            (Self::Legacy(sender), ForwardControlPermit::Legacy) => {
                sender.send(event).map_err(|e| e.0)
            }
            (Self::Budgeted(sender), ForwardControlPermit::Budgeted(permit)) => {
                let kind = match &event {
                    ConnectionEvent::Rebind(_) => ControlKind::Rebind,
                    ConnectionEvent::Close { .. } => ControlKind::Close,
                    _ => return Err(event),
                };
                if !permit.belongs_to(sender, kind) {
                    return Err(event);
                }
                permit.send(event)
            }
            _ => Err(event),
        }
    }

    pub(crate) fn is_budgeted(&self) -> bool {
        matches!(self, Self::Budgeted(_))
    }
}

impl ForwardReceiver {
    pub(crate) fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<ConnectionEvent>> {
        match self {
            Self::Legacy(receiver) => receiver.poll_recv(cx),
            Self::Budgeted(receiver) => receiver.poll_recv(cx),
        }
    }
}
