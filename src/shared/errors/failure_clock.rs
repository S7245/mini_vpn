//! Original failure ordering and quiet publication into its primary owner.
//! Native UDP may cold-bind one parent Session; delivery is Node then Session
//! under one ordinal. Each owner ledger releases before the next is entered.
//! DATA never enters this owner. Legacy keeps ordinal-only atomic state; Native
//! cold state is one actual Box charged to the original App meter until its last
//! observer drops. Role IO/panic custody and JOIN stay with their owners; only
//! two fixed final-owner Drop panic slots live here. No wake runs in this owner.
use super::{FailureOrigin, RetainedFailure};
use std::sync::{
    Arc, Mutex, OnceLock, Weak,
    atomic::{AtomicUsize, Ordering},
};

pub(crate) type FailureClock = Arc<FailureOrder>;
/// Internal capability implemented by the original Session. It must not notify,
/// enter the role registry or re-enter this clock.
pub(crate) trait QuietFailureSink: Send + Sync {
    fn record_quiet(&self, failure: RetainedFailure);
    fn quarantine_quiet(&self) {}
    fn deadline_quiet(&self) -> Option<tokio::time::Instant> {
        None
    }
}
#[derive(Default)]
struct State {
    next: usize,
    reported: usize,
    lost: bool,
}
struct NativeState {
    state: Mutex<State>,
    owner: Weak<dyn QuietFailureSink>,
    parent: Option<Weak<dyn QuietFailureSink>>,
    origin: FailureOrigin,
    meter: Option<bytes::BackingMeter>,
    owner_drop: Mutex<[Option<std::mem::ManuallyDrop<Box<dyn std::any::Any + Send>>>; 2]>,
}
struct Cold {
    // Declaration order: actual allocation is destroyed before its allowance.
    value: Box<NativeState>,
    _allowance: Option<bytes::BackingBudgetReservation>,
}
pub(crate) struct Publication<'a> {
    clock: &'a FailureOrder,
    pub(crate) order: usize,
    // Returned to a caller holding its own fault lock; caller unlocks before
    // dropping this possibly last Session reference.
    _owners: [Option<Arc<dyn QuietFailureSink>>; 2],
}
impl Drop for Publication<'_> {
    fn drop(&mut self) {
        // Drop primary while parent can still accept its failure. If parent
        // then fails, upgrade only the original primary once and guard that
        // final release. Each actual owner destructor runs at most once.
        for index in [0, 1, 0] {
            let Some(owner) = self._owners[index].take() else {
                continue;
            };
            if let Err(payload) =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(owner)))
            {
                let Some(native) = self.clock.native.get() else {
                    std::mem::forget(payload);
                    continue;
                };
                let mut slots = native
                    .value
                    .owner_drop
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if slots[index].is_none() {
                    slots[index] = Some(std::mem::ManuallyDrop::new(payload));
                } else {
                    std::mem::forget(payload);
                }
                drop(slots);
                if let Some(meter) = &native.value.meter {
                    meter.quarantine();
                }
                if index == 1 {
                    self._owners[0] = native.value.owner.upgrade();
                }
                let mut state = self.clock.lock(&native.value);
                let order = state.next;
                state.next = state.next.saturating_add(1);
                let continuous_primary =
                    !state.lost && state.reported == order && order != usize::MAX;
                // Box/Q precede the distinct failure publication. Only a real,
                // consecutive delivery to the original Node advances reported.
                if let Some(survivor) = self._owners[1 - index].as_ref() {
                    survivor
                        .record_quiet(RetainedFailure::opaque_panic(native.value.origin.clone()));
                    if index == 1 && continuous_primary {
                        state.reported = state.next;
                    }
                }
                state.lost = true;
            }
        }
    }
}

#[derive(Default)]
pub(crate) struct FailureOrder {
    legacy: AtomicUsize,
    native: OnceLock<Cold>,
}
impl FailureOrder {
    #[cfg(test)]
    pub(crate) fn owner_drop_payload_is<T: std::any::Any>(&self) -> bool {
        self.native.get().is_some_and(|native| {
            native.value.owner_drop.lock().unwrap()[0]
                .as_ref()
                .is_some_and(|payload| payload.is::<T>())
        })
    }
    pub(crate) fn layout_bytes() -> usize {
        std::mem::size_of::<NativeState>()
    }
    pub(crate) fn bind(
        &self,
        owner: Weak<dyn QuietFailureSink>,
        origin: FailureOrigin,
        meter: Option<bytes::BackingMeter>,
    ) -> Result<(), bytes::MeteredCopyError> {
        self.bind_with_parent(owner, None, origin, meter)
    }
    /// Cold-only fixed Node -> Session bridge. No nested owner ledger locks or
    /// observer upgrade/drop inside another owner are required by callers.
    pub(crate) fn bind_with_parent(
        &self,
        owner: Weak<dyn QuietFailureSink>,
        parent: Option<Weak<dyn QuietFailureSink>>,
        origin: FailureOrigin,
        meter: Option<bytes::BackingMeter>,
    ) -> Result<(), bytes::MeteredCopyError> {
        if self.legacy.load(Ordering::Acquire) != 0
            || self.native.get().is_some()
            || parent
                .as_ref()
                .is_some_and(|parent| Weak::ptr_eq(&owner, parent))
        {
            if let Some(meter) = &meter {
                meter.quarantine();
            }
            return Err(bytes::MeteredCopyError::Quarantined);
        }
        let allowance = match &meter {
            Some(meter) => meter.reserve_cold_layout(Self::layout_bytes())?,
            None => None,
        };
        let layout = std::alloc::Layout::new::<NativeState>();
        // The exact layout is reserved before this fallible allocation. Box
        // uses the same global allocator/layout and destroys it before refund.
        let pointer = unsafe { std::alloc::alloc(layout) }.cast::<NativeState>();
        if pointer.is_null() {
            return Err(bytes::MeteredCopyError::Allocation);
        }
        let value = unsafe {
            pointer.write(NativeState {
                state: Mutex::new(State::default()),
                owner,
                parent,
                origin,
                meter,
                owner_drop: Mutex::new([None, None]),
            });
            Box::from_raw(pointer)
        };
        self.native
            .set(Cold {
                value,
                _allowance: allowance,
            })
            .map_err(|_| bytes::MeteredCopyError::Quarantined)
    }
    fn lock<'a>(&self, native: &'a NativeState) -> std::sync::MutexGuard<'a, State> {
        match native.state.lock() {
            Ok(state) => state,
            Err(poison) => {
                let mut state = poison.into_inner();
                state.lost = true;
                if let Some(meter) = &native.meter {
                    meter.quarantine();
                }
                state
            }
        }
    }
    pub(crate) fn quarantine(&self) {
        if let Some(native) = self.native.get() {
            let owners = [
                native.value.owner.upgrade(),
                native.value.parent.as_ref().and_then(Weak::upgrade),
            ];
            for owner in owners.iter().flatten() {
                owner.quarantine_quiet();
            }
            drop(Publication {
                clock: self,
                order: usize::MAX,
                _owners: owners,
            });
        }
        if let Some(meter) = self
            .native
            .get()
            .and_then(|native| native.value.meter.as_ref())
        {
            meter.quarantine();
        }
    }
    pub(crate) fn work_allowed(&self) -> bool {
        self.native.get().is_none_or(|native| {
            native
                .value
                .meter
                .as_ref()
                .is_none_or(|meter| !meter.is_quarantined())
        })
    }
    /// Read before acquiring Node/UdpState locks. No new deadline is created;
    /// losing a bound parent is distinct from a parent with no deadline yet.
    pub(crate) fn parent_deadline(&self) -> Result<Option<tokio::time::Instant>, ()> {
        if !self.work_allowed() {
            return Err(());
        }
        let Some(native) = self.native.get() else {
            return Ok(None);
        };
        let Some(parent) = native.value.parent.as_ref() else {
            return Ok(None);
        };
        let Some(parent) = parent.upgrade() else {
            self.quarantine();
            return Err(());
        };
        let publication = Publication {
            clock: self,
            order: usize::MAX,
            _owners: [None, Some(parent)],
        };
        let deadline = publication._owners[1]
            .as_ref()
            .and_then(|owner| owner.deadline_quiet());
        drop(publication); // This may be the original parent's last strong reference.
        if self.work_allowed() {
            Ok(deadline)
        } else {
            self.quarantine();
            Err(())
        }
    }
    /// Shutdown reads the original parent's known limit even after Q. This
    /// grants no work/admission and never normalizes a lost owner to success.
    pub(crate) fn parent_deadline_for_stop(&self) -> Option<tokio::time::Instant> {
        let native = self.native.get()?;
        let parent = native.value.parent.as_ref()?;
        let Some(parent) = parent.upgrade() else {
            self.quarantine();
            return None;
        };
        let publication = Publication {
            clock: self,
            order: usize::MAX,
            _owners: [None, Some(parent)],
        };
        let deadline = publication._owners[1]
            .as_ref()
            .and_then(|owner| owner.deadline_quiet());
        drop(publication);
        deadline
    }
    /// Cold composition check; binding to another meter on the same budget is
    /// not binding to this owner. No DATA-path clock locking is introduced.
    pub(crate) fn bound_to_meter(&self, meter: &bytes::BackingMeter) -> bool {
        self.native
            .get()
            .and_then(|native| native.value.meter.as_ref())
            .is_some_and(|actual| actual.same_authority(meter))
    }
    /// Queue Node and original Session App use distinct authorities on one
    /// physical budget. This cold check never qualifies another budget.
    pub(crate) fn bound_to_budget(&self, meter: &bytes::BackingMeter) -> bool {
        self.native
            .get()
            .and_then(|n| n.value.meter.as_ref())
            .is_some_and(|actual| actual.same_budget(meter))
    }
    pub(crate) fn is_bound(&self) -> bool {
        self.native.get().is_some()
    }
    pub(crate) fn stamp(&self) -> usize {
        self.publish(|| None).order
    }
    pub(crate) fn io(&self, error: &std::io::Error) -> usize {
        self.io_deferred(error).order
    }
    pub(crate) fn io_deferred(&self, error: &std::io::Error) -> Publication<'_> {
        self.publish(|| {
            self.native
                .get()
                .map(|native| RetainedFailure::from_io_ref(native.value.origin.clone(), error))
        })
    }
    pub(crate) fn retained(&self, failure: &RetainedFailure) -> usize {
        self.publish(|| Some(failure.clone())).order
    }
    pub(crate) fn panic(&self) -> usize {
        self.panic_deferred().order
    }
    pub(crate) fn panic_deferred(&self) -> Publication<'_> {
        self.publish(|| {
            self.native
                .get()
                .map(|native| RetainedFailure::opaque_panic(native.value.origin.clone()))
        })
    }
    fn publish(&self, failure: impl Fn() -> Option<RetainedFailure>) -> Publication<'_> {
        let Some(native) = self.native.get().map(|cold| &*cold.value) else {
            let order = self
                .legacy
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_add(1))
                .unwrap_or(usize::MAX);
            return Publication {
                clock: self,
                order,
                _owners: [None, None],
            };
        };
        let owner = native.owner.upgrade();
        let parent = native.parent.as_ref().and_then(Weak::upgrade);
        let mut state = self.lock(native);
        let order = state.next;
        state.next = state.next.saturating_add(1);
        if !state.lost && order != usize::MAX {
            if let (Some(owner), Some(fact)) = (owner.as_ref(), failure()) {
                owner.record_quiet(fact);
                // This watermark means actual primary delivery, even if a
                // subsequently missing parent forces the whole clock to Q.
                state.reported = state.next;
                if native.parent.is_some() {
                    if let (Some(parent), Some(fact)) = (parent.as_ref(), failure()) {
                        parent.record_quiet(fact);
                    } else {
                        state.lost = true;
                    }
                }
            } else {
                state.lost = true;
            }
        }
        if state.lost || order == usize::MAX {
            if let Some(meter) = &native.meter {
                meter.quarantine();
            }
        }
        drop(state);
        Publication {
            clock: self,
            order,
            _owners: [owner, parent],
        }
    }
    pub(crate) fn reported(&self, order: usize) -> bool {
        self.native.get().is_some_and(|native| {
            let state = self.lock(&native.value);
            order < state.reported
        })
    }
    pub(crate) fn load(&self, ordering: Ordering) -> usize {
        match self.native.get() {
            Some(native) => {
                let state = self.lock(&native.value);
                if state.lost { usize::MAX } else { state.next }
            }
            None => self.legacy.load(ordering),
        }
    }
}

#[cfg(test)]
mod tests;
