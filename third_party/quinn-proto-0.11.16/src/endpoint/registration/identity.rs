//! Cold registration identity, independent of a reusable physical slot.
//! IDs are minted only here; no DATA-path global lock or allocation is added.
//! Physical fixed storage and host settlement remain the registration Root's job.
use super::*;
use std::sync::atomic::{AtomicU64, Ordering};
static NEXT_AUTHORITY: AtomicU64 = AtomicU64::new(1);

/// Opaque identity for one registration at one Endpoint. A raw slot is not authority.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct RegistrationKey {
    pub(super) authority: u64,
    pub(super) generation: u64,
    pub(super) slot: ConnectionHandle,
}
impl RegistrationKey {
    /// Physical slot used by the original Endpoint/Connection transport.
    pub fn handle(self) -> ConnectionHandle {
        self.slot
    }
    pub(crate) fn generation(self) -> u64 {
        self.generation
    }
}

#[derive(Debug)]
pub(super) struct RegistrationIdentity {
    authority: Option<u64>,
    next_generation: u64,
    native: bool,
    observer: Option<super::ledger::RegistrationObserver>,
    available: bool,
}
impl RegistrationIdentity {
    pub(super) fn new(native: bool, meter: Option<&bytes::BackingMeter>) -> Self {
        let authority = NEXT_AUTHORITY
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_add(1)
            })
            .ok();
        let observer = if native {
            authority.zip(meter).and_then(|(authority, meter)| {
                super::ledger::RegistrationObserver::prepare(authority, meter).ok()
            })
        } else {
            None
        };
        let available = authority.is_some() && (!native || observer.is_some());
        Self {
            authority,
            next_generation: 0,
            native,
            observer,
            available,
        }
    }
    pub(super) fn inventory_matches(
        &self,
        meta: impl Fn(ConnectionHandle) -> Option<RegistrationKey>,
        prepared: impl Fn(ConnectionHandle) -> Option<RegistrationKey>,
    ) -> bool {
        if !self.native {
            return true;
        }
        self.observer
            .as_ref()
            .is_some_and(|observer| observer.inventory_matches(meta, prepared))
    }
    pub(super) fn slot_available(&self, slot: ConnectionHandle) -> bool {
        self.available
            && self
                .observer
                .as_ref()
                .is_none_or(|observer| observer.can_prepare(slot))
    }
    pub(super) fn ready(&self) -> bool {
        self.available
    }
    pub(super) fn observer(&self) -> Option<super::ledger::RegistrationObserver> {
        self.observer.clone()
    }
    pub(super) fn publish(&self, key: RegistrationKey) {
        if let Some(observer) = &self.observer {
            observer.publish(key);
        }
    }
    pub(super) fn retire(
        &self,
        key: RegistrationKey,
    ) -> Option<super::ledger::PreparedRegistrationSettlement> {
        self.observer.as_ref().map(|observer| observer.retire(key))
    }
    pub(super) fn was_settled(&self, key: RegistrationKey) -> bool {
        self.observer
            .as_ref()
            .is_some_and(|observer| observer.was_settled(Some(key)))
    }
    pub(super) fn native(&self) -> bool {
        self.native
    }
    pub(super) fn prepare(
        &mut self,
        slot: ConnectionHandle,
    ) -> Result<RegistrationKey, TransportError> {
        if !self.available
            || self
                .observer
                .as_ref()
                .is_some_and(|observer| !observer.can_prepare(slot))
        {
            return Err(TransportError::INTERNAL_ERROR(
                "registration unavailable or prior Host settlement pending",
            ));
        }
        let authority = self
            .authority
            .ok_or_else(|| TransportError::INTERNAL_ERROR("registration authority exhausted"))?;
        let generation = self
            .next_generation
            .checked_add(1)
            .ok_or_else(|| TransportError::INTERNAL_ERROR("registration generation exhausted"))?;
        self.next_generation = generation;
        Ok(RegistrationKey {
            authority,
            generation,
            slot,
        })
    }
}

impl Drop for RegistrationIdentity {
    fn drop(&mut self) {
        if let Some(observer) = &self.observer {
            observer.root_dropped();
        }
    }
}

/// Whether an original event was accepted by its actual registration owner.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EndpointEventDisposition {
    Accepted,
    Stale,
    Rejected,
}

/// A disposition precedes host settlement; an absent response is not a Drained ACK.
#[doc(hidden)]
#[derive(Debug)]
pub struct EndpointEventOutcome {
    /// Exact disposition of the incoming identity.
    pub disposition: EndpointEventDisposition,
    /// Registration whose transition was accepted, including terminal removal.
    pub registration: Option<RegistrationKey>,
    /// True only after matching real terminal removal.
    pub terminal: bool,
    /// Linear protocol reply for the original Connection owner.
    pub response: Option<ConnectionEvent>,
    pub(super) settlement: Option<super::ledger::PreparedRegistrationSettlement>,
}
impl EndpointEventOutcome {
    /// Publish final settlement after Host catalog and terminal key disposal.
    pub fn finish_settlement(&mut self) {
        if let Some(settlement) = self.settlement.take() {
            settlement.complete();
        }
    }

    pub(super) fn refused(disposition: EndpointEventDisposition) -> Self {
        Self {
            disposition,
            registration: None,
            terminal: false,
            response: None,
            settlement: None,
        }
    }
}
