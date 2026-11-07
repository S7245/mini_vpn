//! One precharged fixed Endpoint settlement ledger, retained by terminal senders.
//! Proto retirement mints a linear completion; Host consumes it after catalog disposal.
//! Observers survive Endpoint destruction without locking it. Routing remains in registration.
//! Tests: tests/admission/registration and the Quinn terminal lifecycle harness.
use super::identity::RegistrationKey;
use bytes::{BackingBudgetReservation, BackingMeter};
use std::{
    alloc::{alloc, Layout},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

#[derive(Debug, Default)]
struct Slot {
    prepared: AtomicU64,
    disposed: AtomicU64,
    published: AtomicU64,
    retired: AtomicU64,
    settled: AtomicU64,
}
#[derive(Debug)]
struct Storage {
    slots: Box<[Slot]>,
    _allowance: BackingBudgetReservation,
}
#[derive(Debug)]
struct Ledger {
    authority: u64,
    storage: Storage,
    meter: BackingMeter,
}

/// Read-only terminal proof retained by the original Connection sender.
#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct RegistrationObserver(Arc<Ledger>);
impl RegistrationObserver {
    pub(super) fn root_dropped(&self) {
        self.0.quarantine_unsettled();
    }

    pub(in crate::endpoint) const fn layout_bytes() -> usize {
        Layout::new::<[Slot; crate::BUDGETED_CONNECTION_LIMIT]>().size()
    }
    /// Bind the original sender to one immutable registration identity.
    pub fn bind(&self, key: RegistrationKey) -> BoundRegistrationObserver {
        BoundRegistrationObserver {
            observer: self.clone(),
            key,
            published: self.slot(key).is_some_and(|slot| {
                slot.published.load(Ordering::Acquire) == key.generation
                    && slot.retired.load(Ordering::Acquire) < key.generation
            }),
        }
    }

    pub(super) fn prepare(authority: u64, meter: &BackingMeter) -> Result<Self, ()> {
        let count = crate::BUDGETED_CONNECTION_LIMIT;
        let layout = Layout::new::<[Slot; crate::BUDGETED_CONNECTION_LIMIT]>();
        let allowance = meter
            .reserve_cold_layout(layout.size())
            .map_err(|_| ())?
            .ok_or(())?;
        let ptr = unsafe { alloc(layout).cast::<Slot>() };
        if ptr.is_null() {
            return Err(());
        }
        for index in 0..count {
            unsafe {
                ptr.add(index).write(Slot::default());
            }
        }
        let slots = unsafe { Box::from_raw(std::ptr::slice_from_raw_parts_mut(ptr, count)) };
        Ok(Self(Arc::new(Ledger {
            authority,
            storage: Storage {
                slots,
                _allowance: allowance,
            },
            meter: meter.clone(),
        })))
    }
    fn slot(&self, key: RegistrationKey) -> Option<&Slot> {
        (self.0.authority == key.authority).then_some(())?;
        self.0.storage.slots.get(key.slot.0)
    }
    /// True only after original Proto retirement and Host disposal both completed.
    pub fn was_settled(&self, key: Option<RegistrationKey>) -> bool {
        key.and_then(|key| {
            self.slot(key)
                .map(|slot| slot.settled.load(Ordering::Acquire) >= key.generation)
        })
        .unwrap_or(false)
    }
    pub(super) fn inventory_matches(
        &self,
        meta: impl Fn(crate::ConnectionHandle) -> Option<RegistrationKey>,
        prepared: impl Fn(crate::ConnectionHandle) -> Option<RegistrationKey>,
    ) -> bool {
        self.0
            .storage
            .slots
            .iter()
            .enumerate()
            .all(|(index, slot)| {
                let published = slot.published.load(Ordering::Acquire);
                let retired = slot.retired.load(Ordering::Acquire);
                let settled = slot.settled.load(Ordering::Acquire);
                let staged = slot.prepared.load(Ordering::Acquire);
                let disposed = slot.disposed.load(Ordering::Acquire);
                let ch = crate::ConnectionHandle(index);
                match prepared(ch) {
                    Some(key)
                        if key.authority == self.0.authority
                            && key.slot == ch
                            && key.generation == staged
                            && staged > published
                            && published == retired
                            && retired == settled
                            && (disposed == 0 || disposed == staged) =>
                    {
                        return meta(ch).is_none();
                    }
                    None if staged == 0 && disposed == 0 => {}
                    _ => return false,
                }
                match meta(ch) {
                    Some(key) => {
                        key.authority == self.0.authority
                            && key.slot.0 == index
                            && key.generation == published
                            && published > retired
                            && retired == settled
                    }
                    None => published == retired && retired >= settled,
                }
            })
    }
    pub(super) fn can_prepare(&self, slot: crate::ConnectionHandle) -> bool {
        self.0.storage.slots.get(slot.0).is_some_and(|slot| {
            let published = slot.published.load(Ordering::Acquire);
            slot.prepared.load(Ordering::Acquire) == 0
                && slot.disposed.load(Ordering::Acquire) == 0
                && published == slot.retired.load(Ordering::Acquire)
                && published == slot.settled.load(Ordering::Acquire)
        }) && self.0.meter.admit_precharged().is_ok()
    }
    pub(super) fn publish(&self, key: RegistrationKey) {
        // Preparation proved the previous generation settled under &mut Endpoint.
        if let Some(slot) = self.slot(key) {
            slot.published.store(key.generation, Ordering::Release);
            slot.prepared.store(0, Ordering::Release);
            slot.disposed.store(0, Ordering::Release);
        } else {
            self.0.meter.quarantine();
        }
    }
    pub(in crate::endpoint::registration) fn quarantine(&self) {
        self.0.meter.quarantine();
    }
    pub(in crate::endpoint::registration) fn reserve(&self, key: RegistrationKey) {
        if let Some(slot) = self.slot(key) {
            slot.prepared.store(key.generation, Ordering::Release);
        } else {
            self.quarantine();
        }
    }
    pub(in crate::endpoint::registration) fn is_prepared(&self, key: RegistrationKey) -> bool {
        self.slot(key)
            .is_some_and(|slot| slot.prepared.load(Ordering::Acquire) == key.generation)
    }
    pub(in crate::endpoint::registration) fn can_publish(&self, key: RegistrationKey) -> bool {
        self.slot(key).is_some_and(|slot| {
            slot.prepared.load(Ordering::Acquire) == key.generation
                && slot.disposed.load(Ordering::Acquire) == 0
        }) && self.0.meter.admit_precharged().is_ok()
    }
    pub(in crate::endpoint::registration) fn connection_disposed(&self, key: RegistrationKey) {
        if let Some(slot) = self.slot(key) {
            if slot.prepared.load(Ordering::Acquire) == key.generation {
                slot.disposed.store(key.generation, Ordering::Release);
            }
        } else {
            self.quarantine();
        }
    }
    pub(in crate::endpoint::registration) fn was_disposed(&self, key: RegistrationKey) -> bool {
        self.slot(key).is_some_and(|slot| {
            slot.prepared.load(Ordering::Acquire) == key.generation
                && slot.disposed.load(Ordering::Acquire) == key.generation
        })
    }
    pub(in crate::endpoint::registration) fn cancel(&self, key: RegistrationKey) {
        if let Some(slot) = self.slot(key) {
            slot.prepared.store(0, Ordering::Release);
            slot.disposed.store(0, Ordering::Release);
        } else {
            self.quarantine();
        }
    }
    pub(super) fn retire(&self, key: RegistrationKey) -> PreparedRegistrationSettlement {
        if let Some(slot) = self.slot(key) {
            slot.retired.store(key.generation, Ordering::Release);
        } else {
            self.0.meter.quarantine();
        }
        PreparedRegistrationSettlement {
            observer: self.clone(),
            key,
            armed: true,
        }
    }
}

/// Terminal proof scoped to one original sender, never another slot or successor.
#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct BoundRegistrationObserver {
    observer: RegistrationObserver,
    key: RegistrationKey,
    published: bool,
}
impl BoundRegistrationObserver {
    /// Check both physical route and sealed registration before accepting a message.
    pub fn matches(&self, handle: crate::ConnectionHandle, key: Option<RegistrationKey>) -> bool {
        self.published
            && self.key.handle() == handle
            && key == Some(self.key)
            && self.observer.slot(self.key).is_some()
    }
    /// Final settlement for the same original sender only.
    pub fn was_settled(
        &self,
        handle: crate::ConnectionHandle,
        key: Option<RegistrationKey>,
    ) -> bool {
        self.matches(handle, key) && self.observer.was_settled(key)
    }
}

/// Linear terminal completion, minted only by matching Proto retirement.
/// Losing it before Host settlement preserves sticky charged quarantine.
#[doc(hidden)]
#[derive(Debug)]
pub(super) struct PreparedRegistrationSettlement {
    observer: RegistrationObserver,
    key: RegistrationKey,
    armed: bool,
}
impl PreparedRegistrationSettlement {
    pub(super) fn complete(mut self) {
        // No fallible work remains. Slot reuse is excluded until publication.
        if let Some(slot) = self.observer.slot(self.key) {
            slot.settled.store(self.key.generation, Ordering::Release);
            self.armed = false;
        }
    }
}
impl Drop for PreparedRegistrationSettlement {
    fn drop(&mut self) {
        if self.armed {
            self.observer.0.meter.quarantine();
        }
    }
}

impl Ledger {
    fn quarantine_unsettled(&self) {
        if self.storage.slots.iter().any(|slot| {
            let published = slot.published.load(Ordering::Acquire);
            slot.prepared.load(Ordering::Acquire) != 0
                || slot.disposed.load(Ordering::Acquire) != 0
                || published != slot.retired.load(Ordering::Acquire)
                || published != slot.settled.load(Ordering::Acquire)
        }) {
            self.meter.quarantine();
        }
    }
}

impl Drop for Ledger {
    fn drop(&mut self) {
        self.quarantine_unsettled();
    }
}
