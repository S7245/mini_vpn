//! Optional Node meter for an existing shared receive allocation.
//! One Shared header owns one charge; clones and slices do not register again.

use crate::loom::sync::atomic::{AtomicUsize, Ordering};
use alloc::sync::Arc;

/// One physical allocation-layout allowance shared by all participating Nodes.
/// A reservation is obtained before allocation and returned only by its owner.
#[derive(Debug, Clone)]
pub struct BackingBudget {
    inner: Arc<BackingBudgetInner>,
}

const QUARANTINED_BIT: usize = 1usize << (usize::BITS - 1);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum MeteredCopyError {
    BudgetFull,
    TooLarge,
    Quarantined,
    Allocation,
}

#[derive(Debug)]
struct BackingBudgetInner {
    limit: usize,
    /// Headroom only protocol-tier meters may use: app-tier reservations fail
    /// at `limit - protocol_reserve`. Zero preserves the single-tier budget.
    protocol_reserve: usize,
    /// High bit is sticky Q; remaining bits are exact known reservations.
    state: AtomicUsize,
}

/// An in-flight allowance. Dropping an uncommitted reservation returns it.
#[derive(Debug)]
#[must_use]
pub struct BackingBudgetReservation {
    inner: Arc<BackingBudgetInner>,
    bytes: usize,
}

impl BackingBudget {
    pub fn new(limit: usize) -> Option<Self> {
        Self::with_protocol_reserve(limit, 0)
    }

    /// One budget identity with a protocol-only top tier. App-tier meters
    /// (`BackingMeter::with_budget`) are admitted up to `limit - reserve`;
    /// protocol-tier meters (`with_protocol_budget`) up to `limit`. The app
    /// tier must be non-empty.
    pub fn with_protocol_reserve(limit: usize, reserve: usize) -> Option<Self> {
        (limit != 0 && limit < QUARANTINED_BIT && reserve < limit).then(|| Self {
            inner: Arc::new(BackingBudgetInner {
                limit,
                protocol_reserve: reserve,
                state: AtomicUsize::new(0),
            }),
        })
    }

    /// Headroom reserved for protocol-tier meters.
    pub fn protocol_reserve(&self) -> usize {
        self.inner.protocol_reserve
    }

    fn ceiling(&self, tier: Tier) -> usize {
        match tier {
            Tier::App => self.inner.limit - self.inner.protocol_reserve,
            Tier::Protocol => self.inner.limit,
        }
    }

    /// Known layouts, or one sticky unknown-debt sentinel after quarantine.
    pub fn used(&self) -> usize {
        let state = self.inner.state.load(Ordering::Acquire);
        (state & !QUARANTINED_BIT).max(usize::from(state & QUARANTINED_BIT != 0))
    }

    pub fn limit(&self) -> usize {
        self.inner.limit
    }

    pub fn quarantined(&self) -> bool {
        self.inner.state.load(Ordering::Acquire) & QUARANTINED_BIT != 0
    }

    /// App-tier reservation (fails at `limit - protocol_reserve`).
    pub fn try_reserve(&self, bytes: usize) -> Option<BackingBudgetReservation> {
        self.reserve_detailed(bytes, Tier::App).ok()
    }

    fn reserve_detailed(
        &self,
        bytes: usize,
        tier: Tier,
    ) -> Result<BackingBudgetReservation, MeteredCopyError> {
        let ceiling = self.ceiling(tier);
        if bytes > ceiling {
            return Err(MeteredCopyError::TooLarge);
        }
        match self
            .inner
            .state
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |state| {
                (state & QUARANTINED_BIT == 0)
                    .then_some(state)?
                    .checked_add(bytes)
                    .filter(|next| *next <= ceiling)
            }) {
            Ok(_) => Ok(BackingBudgetReservation {
                inner: self.inner.clone(),
                bytes,
            }),
            Err(state) if state & QUARANTINED_BIT != 0 => Err(MeteredCopyError::Quarantined),
            Err(_) => Err(MeteredCopyError::BudgetFull),
        }
    }

    fn release_owned(&self, bytes: usize) -> bool {
        self.inner.release(bytes)
    }
}

impl BackingBudgetInner {
    fn quarantine(&self) {
        self.state.fetch_or(QUARANTINED_BIT, Ordering::AcqRel);
    }

    fn release(&self, bytes: usize) -> bool {
        if self
            .state
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |state| {
                let used = state & !QUARANTINED_BIT;
                used.checked_sub(bytes)
                    .map(|next| next | (state & QUARANTINED_BIT))
            })
            .is_err()
        {
            self.quarantine();
            return false;
        }
        true
    }
}

impl BackingBudgetReservation {
    fn belongs_to(&self, budget: &BackingBudget, bytes: usize) -> bool {
        Arc::ptr_eq(&self.inner, &budget.inner) && self.bytes == bytes
    }

    fn commit(mut self) {
        self.bytes = 0;
    }
}

impl Drop for BackingBudgetReservation {
    fn drop(&mut self) {
        if self.bytes != 0 {
            self.inner.release(self.bytes);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackingMeterSnapshot {
    pub allocations: usize,
    pub payload_bytes: usize,
    /// Sum of requested allocation layouts, including inline/shared headers.
    pub physical_bytes: usize,
    pub quarantined: bool,
    pub registered_total: usize,
}

#[derive(Debug, Clone)]
pub struct BackingMeter {
    pub(crate) inner: Arc<BackingMeterInner>,
}

/// Admission tier of one meter within its shared budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Tier {
    App,
    Protocol,
}

#[derive(Debug)]
pub(crate) struct BackingMeterInner {
    budget: Option<BackingBudget>,
    tier: Tier,
    /// Set only on a tier view: every counter, flag and identity belongs to
    /// this core; the view contributes nothing but its admission ceiling.
    core: Option<Arc<BackingMeterInner>>,
    allocations: AtomicUsize,
    payload_bytes: AtomicUsize,
    physical_bytes: AtomicUsize,
    quarantined: AtomicUsize,
    registered_total: AtomicUsize,
}

impl Default for BackingMeter {
    fn default() -> Self {
        Self::new()
    }
}

impl BackingMeter {
    pub fn new() -> Self {
        Self::new_with_budget(None, Tier::App)
    }

    /// Attach one shared hard allowance before this meter receives allocations.
    /// App tier: admitted up to `limit - protocol_reserve`.
    pub fn with_budget(budget: &BackingBudget) -> Self {
        Self::new_with_budget(Some(budget.clone()), Tier::App)
    }

    /// Protocol-tier meter on the same budget identity (`same_budget` holds
    /// with app-tier meters); admitted up to the full hard limit.
    pub fn with_protocol_budget(budget: &BackingBudget) -> Self {
        Self::new_with_budget(Some(budget.clone()), Tier::Protocol)
    }

    /// App-tier admission handle on this meter's exact authority. Counters,
    /// quarantine, Shared-header identity and `same_authority` remain the
    /// original meter's; only new reservations stop at the App ceiling. An
    /// App-tier or unbudgeted meter returns itself.
    pub fn app_tier_view(&self) -> Self {
        if self.inner.tier == Tier::App || self.inner.budget.is_none() {
            return self.clone();
        }
        let core = self.inner.core.clone().unwrap_or_else(|| self.inner.clone());
        Self {
            inner: Arc::new(BackingMeterInner {
                budget: core.budget.clone(),
                tier: Tier::App,
                core: Some(core),
                allocations: AtomicUsize::new(0),
                payload_bytes: AtomicUsize::new(0),
                physical_bytes: AtomicUsize::new(0),
                quarantined: AtomicUsize::new(0),
                registered_total: AtomicUsize::new(0),
            }),
        }
    }

    /// Shared hard limit, when this meter participates in one.
    #[doc(hidden)]
    pub fn budget_limit(&self) -> Option<usize> {
        self.inner.budget.as_ref().map(BackingBudget::limit)
    }

    /// Read-only pressure hint for an owner's idle-inventory control turn.
    /// This grants no allocation permission and registers no wake or refund.
    #[doc(hidden)]
    pub fn budget_headroom(&self) -> Result<Option<usize>, MeteredCopyError> {
        if self.inner.core().quarantined.load(Ordering::Acquire) != 0 {
            return Err(MeteredCopyError::Quarantined);
        }
        self.inner.budget.as_ref().map(|budget| {
            let state = budget.inner.state.load(Ordering::Acquire);
            if state & QUARANTINED_BIT != 0 { return Err(MeteredCopyError::Quarantined); }
            // Below this meter's tier ceiling; an app tier already invaded by
            // protocol bytes reports zero, never a quarantine.
            Ok(budget.ceiling(self.inner.tier).saturating_sub(state))
        }).transpose()
    }

    /// Reserve a cold owner layout before it allocates. The owner retains the
    /// token until its exact backing has been dropped.
    #[doc(hidden)]
    pub fn reserve_cold_layout(
        &self,
        bytes: usize,
    ) -> Result<Option<BackingBudgetReservation>, MeteredCopyError> {
        self.inner.reserve_layout(bytes)
    }

    /// Cold, read-only validation of the exact original layout capability.
    /// Does not transfer, consume, or refund the reservation.
    #[doc(hidden)]
    pub fn cold_layout_matches(
        &self,
        token: Option<&BackingBudgetReservation>,
        bytes: usize,
    ) -> bool {
        match (self.inner.budget.as_ref(), token) {
            (Some(budget), Some(token)) => token.belongs_to(budget, bytes),
            (Some(_), None) => bytes == 0,
            (None, None) => true,
            (None, Some(_)) => false,
        }
    }

    /// Linearize one operation on resources whose layout was already charged.
    /// This admits no bytes and allocates nothing. A successful common-budget
    /// RMW orders this operation before a concurrent quarantine; an operation
    /// already in flight remains charged and must settle or quarantine. Every
    /// later operation must obtain its own fence. This is not a refund or a
    /// promise that asynchronous I/O has completed before quarantine.
    #[doc(hidden)]
    pub fn admit_precharged(&self) -> Result<(), MeteredCopyError> {
        if self.inner.core().quarantined.load(Ordering::Acquire) != 0 {
            return Err(MeteredCopyError::Quarantined);
        }
        if let Some(budget) = &self.inner.budget {
            budget
                .inner
                .state
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |state| {
                    (state & QUARANTINED_BIT == 0).then_some(state)
                })
                .map(|_| ())
                .map_err(|_| MeteredCopyError::Quarantined)
        } else {
            Ok(())
        }
    }

    fn new_with_budget(budget: Option<BackingBudget>, tier: Tier) -> Self {
        Self {
            inner: Arc::new(BackingMeterInner {
                budget,
                tier,
                core: None,
                allocations: AtomicUsize::new(0),
                payload_bytes: AtomicUsize::new(0),
                physical_bytes: AtomicUsize::new(0),
                quarantined: AtomicUsize::new(0),
                registered_total: AtomicUsize::new(0),
            }),
        }
    }

    /// Read only the local/shared sticky isolation flags, without sampling
    /// allocation counters. In-flight admission still uses admit_precharged.
    #[doc(hidden)]
    pub fn is_quarantined(&self) -> bool {
        self.inner.core().quarantined.load(Ordering::Acquire) != 0
            || self
                .inner
                .budget
                .as_ref()
                .is_some_and(BackingBudget::quarantined)
    }

    /// Exact after all tracked owners are quiescent. Concurrent producers may
    /// make the two atomics a non-atomic live observation.
    pub fn snapshot(&self) -> BackingMeterSnapshot {
        let core = self.inner.core();
        BackingMeterSnapshot {
            allocations: core.allocations.load(Ordering::Acquire),
            payload_bytes: core.payload_bytes.load(Ordering::Acquire),
            physical_bytes: core.physical_bytes.load(Ordering::Acquire),
            quarantined: self.is_quarantined(),
            registered_total: core.registered_total.load(Ordering::Acquire),
        }
    }

    /// Whether two meters charge the very same bounded budget authority.
    /// Unbudgeted meters do not establish this cross-owner Native proof.
    pub fn same_budget(&self, other: &Self) -> bool {
        match (&self.inner.budget, &other.inner.budget) {
            (Some(a), Some(b)) => Arc::ptr_eq(&a.inner, &b.inner),
            _ => false,
        }
    }

    #[doc(hidden)]
    pub fn same_authority(&self, other: &Self) -> bool {
        self.inner.same_core(&other.inner)
    }

    #[doc(hidden)]
    pub fn quarantine(&self) {
        self.inner.quarantine();
    }
}

impl BackingMeterInner {
    /// The authority that owns counters and quarantine (itself unless a view).
    #[inline]
    pub(crate) fn core(&self) -> &BackingMeterInner {
        match &self.core {
            Some(core) => core,
            None => self,
        }
    }

    /// Exact authority identity, independent of the admission tier handle.
    #[inline]
    pub(crate) fn same_core(&self, other: &BackingMeterInner) -> bool {
        core::ptr::eq(self.core(), other.core())
    }

    pub(crate) fn quarantine(&self) {
        let this = self.core();
        // The common atomic is the Q linearization point for budgeted
        // owners. Publish it before the local flag so no observer can see
        // local Q while a common precharged admission still succeeds.
        if let Some(budget) = &this.budget {
            budget.inner.quarantine();
        }
        this.quarantined.store(1, Ordering::Release);
    }

    pub(crate) fn register(&self, capacity: usize, physical_bytes: usize) -> bool {
        let this = self.core();
        if this.budget.is_some() {
            // A budgeted allocation must obtain its allowance before allocating.
            self.quarantine();
            return false;
        }
        this.register_counters(capacity, physical_bytes)
    }

    pub(crate) fn reserve_layout(
        &self,
        physical_bytes: usize,
    ) -> Result<Option<BackingBudgetReservation>, MeteredCopyError> {
        if self.core().quarantined.load(Ordering::Acquire) != 0 {
            return Err(MeteredCopyError::Quarantined);
        }
        // Admission ceiling is this handle's tier; the budget is the core's.
        self.budget
            .as_ref()
            .map(|budget| budget.reserve_detailed(physical_bytes, self.tier).map(Some))
            .unwrap_or(Ok(None))
    }

    pub(crate) fn register_preallocated(
        &self,
        capacity: usize,
        physical_bytes: usize,
        reservation: Option<BackingBudgetReservation>,
    ) -> bool {
        match (&self.budget, &reservation) {
            (Some(budget), Some(token)) if token.belongs_to(budget, physical_bytes) => {}
            (None, None) => {}
            _ => {
                self.quarantine();
                return false;
            }
        }
        if !self.core().register_counters(capacity, physical_bytes) {
            return false;
        }
        if let Some(token) = reservation {
            token.commit();
        }
        true
    }

    fn register_counters(&self, capacity: usize, physical_bytes: usize) -> bool {
        if self
            .payload_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                n.checked_add(capacity)
            })
            .is_err()
        {
            self.quarantine();
            return false;
        }
        if self
            .physical_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                n.checked_add(physical_bytes)
            })
            .is_err()
        {
            let _ = self.payload_bytes.fetch_sub(capacity, Ordering::AcqRel);
            self.quarantine();
            return false;
        }
        if self
            .allocations
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_add(1))
            .is_err()
        {
            let _ = self
                .physical_bytes
                .fetch_sub(physical_bytes, Ordering::AcqRel);
            let _ = self.payload_bytes.fetch_sub(capacity, Ordering::AcqRel);
            self.quarantine();
            return false;
        }
        if self
            .registered_total
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_add(1))
            .is_err()
        {
            let _ = self.allocations.fetch_sub(1, Ordering::AcqRel);
            let _ = self
                .physical_bytes
                .fetch_sub(physical_bytes, Ordering::AcqRel);
            let _ = self.payload_bytes.fetch_sub(capacity, Ordering::AcqRel);
            self.quarantine();
            return false;
        }
        true
    }

    pub(crate) fn release(&self, capacity: usize, physical_bytes: usize) {
        let this = self.core();
        let settled = this
            .payload_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                n.checked_sub(capacity)
            })
            .is_ok()
            && this
                .physical_bytes
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                    n.checked_sub(physical_bytes)
                })
                .is_ok()
            && this
                .allocations
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_sub(1))
                .is_ok();
        if !settled {
            self.quarantine();
            return;
        }
        if this
            .budget
            .as_ref()
            .is_some_and(|budget| !budget.release_owned(physical_bytes))
        {
            self.quarantine();
        }
    }
}
