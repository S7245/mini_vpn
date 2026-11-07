//! Original Connection application-event inventory. Owns its actual queue and
//! layout; preparation never publishes an event. Active successor turns and
//! blocked DATAGRAM senders retain one physical terminal slot each, derived
//! from their original owners rather than a second reservation ledger.
//! Entry: Queue::prepare/commit/push_reserved. Tests: application_events/tests.rs.
use super::storage::Slab;
use super::{Connection, Event, SuccessorServiceTurnOutcome, TransportError};
use bytes::{BackingBudgetReservation, BackingMeter, MeteredCopyError};
use std::{
    alloc::Layout,
    collections::VecDeque,
    mem::{self, ManuallyDrop},
};

/// Only these five variants are physically queued by Connection. Stream and
/// ConnectionLost events remain in their original owners and are polled there.
#[derive(Debug)]
pub(super) enum QueuedEvent {
    HandshakeDataReady,
    Connected,
    DatagramReceived,
    DatagramsUnblocked,
    SuccessorServiceTurn {
        outcome: SuccessorServiceTurnOutcome,
    },
}
impl From<QueuedEvent> for Event {
    fn from(value: QueuedEvent) -> Self {
        match value {
            QueuedEvent::HandshakeDataReady => Self::HandshakeDataReady,
            QueuedEvent::Connected => Self::Connected,
            QueuedEvent::DatagramReceived => Self::DatagramReceived,
            QueuedEvent::DatagramsUnblocked => Self::DatagramsUnblocked,
            QueuedEvent::SuccessorServiceTurn { outcome } => Self::SuccessorServiceTurn { outcome },
        }
    }
}

pub(super) struct Queue {
    values: ManuallyDrop<VecDeque<QueuedEvent>>,
    allowance: Option<BackingBudgetReservation>,
    meter: Option<BackingMeter>,
}
pub(super) struct Prepared {
    owner: usize,
    shape: (usize, usize),
    meter: Option<BackingMeter>,
    replacement: Option<Slab<QueuedEvent>>,
}
impl Queue {
    pub(super) fn new(meter: Option<BackingMeter>) -> Self {
        Self {
            values: ManuallyDrop::new(VecDeque::new()),
            allowance: None,
            meter: meter.filter(|m| m.budget_limit().is_some()),
        }
    }
    fn layout_matches(&self) -> bool {
        self.meter.as_ref().is_none_or(|meter| {
            Layout::array::<QueuedEvent>(self.values.capacity()).is_ok_and(|layout| {
                meter.cold_layout_matches(self.allowance.as_ref(), layout.size())
            })
        })
    }
    fn qualify(&self) -> Result<(), MeteredCopyError> {
        let Some(meter) = &self.meter else {
            return Ok(());
        };
        meter.admit_precharged()?;
        if !self.layout_matches() {
            meter.quarantine();
            return Err(MeteredCopyError::Quarantined);
        }
        Ok(())
    }
    pub(super) fn prepare(
        &self,
        additional: usize,
        held: usize,
    ) -> Result<Prepared, MeteredCopyError> {
        self.qualify()?;
        let needed = self
            .values
            .len()
            .checked_add(additional)
            .and_then(|n| n.checked_add(held))
            .ok_or(MeteredCopyError::TooLarge)?;
        let replacement = if self.meter.is_some() && needed > self.values.capacity() {
            let capacity = needed
                .max(
                    self.values
                        .capacity()
                        .checked_mul(2)
                        .ok_or(MeteredCopyError::TooLarge)?,
                )
                .max(4);
            Some(Slab::prepare(capacity, self.meter.as_ref())?)
        } else {
            None
        };
        Ok(Prepared {
            owner: self as *const Self as usize,
            shape: (self.values.len(), self.values.capacity()),
            meter: self.meter.clone(),
            replacement,
        })
    }
    pub(super) fn commit(&mut self, prepared: Prepared) -> Result<(), MeteredCopyError> {
        self.qualify()?;
        if prepared.owner != self as *const Self as usize
            || prepared.shape != (self.values.len(), self.values.capacity())
            || match (&self.meter, &prepared.meter) {
                (Some(a), Some(b)) => !a.same_authority(b),
                (None, None) => false,
                _ => true,
            }
        {
            if let Some(meter) = &self.meter {
                meter.quarantine();
            }
            if let Some(meter) = &prepared.meter {
                meter.quarantine();
            }
            return Err(MeteredCopyError::Quarantined);
        }
        if let Some(replacement) = prepared.replacement {
            let mut values = VecDeque::from(replacement.values);
            values.extend(self.values.drain(..));
            let old = mem::replace(&mut *self.values, values);
            let allowance = mem::replace(&mut self.allowance, replacement.allowance);
            drop(old);
            drop(allowance);
        }
        Ok(())
    }
    pub(super) fn reserve(
        &mut self,
        additional: usize,
        held: usize,
    ) -> Result<(), MeteredCopyError> {
        let prepared = self.prepare(additional, held)?;
        self.commit(prepared)
    }
    pub(super) fn funded(&self, additional: usize, held: usize) -> bool {
        self.qualify().is_ok()
            && (self.meter.is_none()
                || self
                    .values
                    .len()
                    .checked_add(additional)
                    .and_then(|n| n.checked_add(held))
                    .is_some_and(|n| n <= self.values.capacity()))
    }
    pub(super) fn push_reserved(
        &mut self,
        event: QueuedEvent,
        held: usize,
    ) -> Result<(), QueuedEvent> {
        if !self.funded(1, held) {
            if let Some(meter) = &self.meter {
                meter.quarantine();
            }
            return Err(event);
        }
        self.values.push_back(event);
        Ok(())
    }
    pub(super) fn pop_front(&mut self) -> Option<Event> {
        if self.qualify().is_err() {
            return None;
        }
        self.values.pop_front().map(Event::from)
    }
    #[cfg(test)]
    pub(super) fn snapshot(&self) -> String {
        format!(
            "{:?}",
            (self.values.len(), self.values.capacity(), &*self.values)
        )
    }
}
impl Drop for Queue {
    fn drop(&mut self) {
        if let Some(meter) = &self.meter {
            if std::thread::panicking() {
                meter.quarantine();
            }
            if !self.layout_matches() {
                meter.quarantine();
                mem::forget(self.allowance.take());
                return;
            }
        }
        // QueuedEvent contains only the five value-only variants. No opaque
        // destructor or external actor can interrupt this physical retirement.
        // A global Q forbids admission, not destruction of a qualified local
        // layout; the original budget keeps its non-zero sticky Q witness.
        unsafe {
            ManuallyDrop::drop(&mut self.values);
        }
    }
}
impl Connection {
    pub(super) fn application_event_holds(&self) -> usize {
        usize::from(self.successor_service_turn.is_some())
            + usize::from(self.datagrams.send_blocked)
    }
    pub(super) fn prepare_application_events(
        &self,
        additional: usize,
    ) -> Result<Prepared, MeteredCopyError> {
        self.events
            .prepare(additional, self.application_event_holds())
    }
    pub(super) fn push_application_event(
        &mut self,
        event: QueuedEvent,
    ) -> Result<(), TransportError> {
        let held = self.application_event_holds();
        self.events.reserve(1, held).map_err(event_error)?;
        self.events
            .push_reserved(event, held)
            .map_err(|_| event_error(MeteredCopyError::Quarantined))
    }
}
pub(super) fn event_error(error: MeteredCopyError) -> TransportError {
    TransportError::INTERNAL_ERROR(match error {
        MeteredCopyError::BudgetFull => "application event inventory budget full",
        MeteredCopyError::TooLarge => "application event layout too large",
        _ => "application event inventory mismatch",
    })
}
#[cfg(test)]
#[path = "application_events/tests.rs"]
mod tests;

impl Drop for Connection {
    fn drop(&mut self) {
        if !self.events.funded(0, self.application_event_holds()) {
            if let Some(meter) = &self.endpoint_config.receive_backing_meter {
                meter.quarantine();
            }
        }
    }
}
