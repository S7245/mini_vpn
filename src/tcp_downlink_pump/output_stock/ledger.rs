//! Sealed owner of actual cold slabs, one write operation and typed loans.
//! Owns complete reserve/copy/publish/return/retire settlement; no FIFO, epoch,
//! actor JOIN or refund authority is introduced. Adapters cannot enter State or
//! mark Q; external rejection carries its original quiet publication capability.
//! Entry: cold prepare/producer spaces/physical retirement. Tests: ledger/tests.
//! NativeRoles/read-port/Main production wiring remains a later part of A1.

use crate::tcp_downlink_pump::CustodiedBytes;
use bytes::{
    AllocationIdentity, BackingBudgetReservation, BackingMeter, Bytes, BytesMut, MeteredCopyError,
};
use std::{
    alloc::{Layout, alloc},
    mem::ManuallyDrop,
    sync::{Arc, Mutex, MutexGuard, Weak},
};

use super::notify::QueueReturn;
use crate::tcp_downlink_pump::native_wait;

#[derive(Clone, Copy, Debug)]
pub(crate) struct StockLayout {
    slab: usize,
    count: usize,
    bootstrap: Option<usize>,
}
impl StockLayout {
    pub(in crate::tcp_downlink_pump) fn checked(
        queue: usize,
        slab: usize,
        window: usize,
    ) -> Result<Self, MeteredCopyError> {
        if queue == 0 || slab == 0 || window == 0 {
            return Err(MeteredCopyError::TooLarge);
        }
        let published = window.checked_mul(2).ok_or(MeteredCopyError::TooLarge)?;
        let count = queue
            .div_ceil(slab)
            .checked_add(published.div_ceil(slab))
            .and_then(|n| n.checked_add(4))
            .filter(|n| *n <= 64)
            .ok_or(MeteredCopyError::TooLarge)?;
        Ok(Self {
            slab,
            count,
            bootstrap: None,
        })
    }
    pub(in crate::tcp_downlink_pump) fn progressive(
        queue: usize,
        slab: usize,
        window: usize,
    ) -> Result<Self, MeteredCopyError> {
        let mut shape = Self::checked(queue, slab, window)?;
        if window > slab {
            return Err(MeteredCopyError::TooLarge);
        }
        shape.count = shape
            .count
            .checked_add(1)
            .filter(|n| *n <= 64)
            .ok_or(MeteredCopyError::TooLarge)?;
        shape.bootstrap = Some(window);
        Ok(shape)
    }
    fn capacity_at(&self, index: usize) -> usize {
        if index == 0 {
            self.bootstrap.unwrap_or(self.slab)
        } else {
            self.slab
        }
    }
}

enum Tail {
    Mutable(BytesMut),
    Quarantined(Bytes),
}
struct Slab {
    tail: Option<Tail>,
    identity: AllocationIdentity,
    generation: u64,
    views: usize,
    view_bytes: usize,
}
// Unfunded metadata is explicit; absence of a funded tail still means damage.
enum SlabRecord {
    Unfunded,
    Funded(Slab),
}
impl SlabRecord {
    fn funded(&self) -> Option<&Slab> {
        match self {
            Self::Funded(slab) => Some(slab),
            Self::Unfunded => None,
        }
    }
    fn funded_mut(&mut self) -> Option<&mut Slab> {
        match self {
            Self::Funded(slab) => Some(slab),
            Self::Unfunded => None,
        }
    }
}
#[derive(Clone, Copy)]
struct Operation {
    id: u64,
    index: usize,
    max: usize,
    written: usize,
}
struct State {
    slabs: Vec<SlabRecord>,
    funded: usize,
    funded_capacity: usize,
    busy: usize,
    _slab_layout: Option<BackingBudgetReservation>,
    active: Option<usize>,
    ready: u64,
    operation: Option<Operation>,
    next_operation: u64,
    writer: u8, // 0: never issued; 1: unique lease present; 2: lease actually dropped
    views: usize,
    view_bytes: usize,
    closed: bool,
    retiring: bool, // Exclusive online turn still owns a physical tail outside the lock.
}
impl State {
    fn slab(&self, index: usize) -> Option<&Slab> {
        self.slabs.get(index).and_then(SlabRecord::funded)
    }
    fn slab_mut(&mut self, index: usize) -> Option<&mut Slab> {
        self.slabs.get_mut(index).and_then(SlabRecord::funded_mut)
    }
}
struct Core {
    state: Mutex<State>,
    clock: crate::shared::errors::FailureClock,
}
pub(in crate::tcp_downlink_pump) struct Root {
    body: ManuallyDrop<Box<Core>>,
    allowance: ManuallyDrop<Option<BackingBudgetReservation>>,
    meter: BackingMeter,
    notify: QueueReturn,
    shape: StockLayout,
    retired: bool,
}
impl Root {
    pub(in crate::tcp_downlink_pump) fn returned(&self) -> Result<bool, StockWaitError> {
        let _quiet = self.quiet_scope();
        self.meter.admit_precharged().map_err(StockWaitError::Admission)?;
        let state = self.lock().map_err(StockWaitError::Admission)?;
        if state.retiring { return Err(StockWaitError::Pressure); }
        if !self.inventory_matches(&state) {
            return Err(StockWaitError::Admission(self.invalid()));
        }
        if state.writer == 1 { return Err(StockWaitError::Pressure); }
        Ok(state.operation.is_none() && state.views == 0 && state.view_bytes == 0)
    }
    fn quiet_scope(&self) -> native_wait::QuietScope {
        native_wait::QuietScope::new(self.meter.clone(), Some(self.body.clock.clone()))
    }
    fn lock(&self) -> Result<MutexGuard<'_, State>, MeteredCopyError> {
        self.body.state.lock().map_err(|_| {
            self.meter.quarantine();
            MeteredCopyError::Quarantined
        })
    }
    fn invalid(&self) -> MeteredCopyError {
        self.meter.quarantine();
        MeteredCopyError::Quarantined
    }
    /// Full inventory belongs to cold/control retirement, never each DATA view.
    fn inventory_matches(&self, state: &State) -> bool {
        let Ok(layout) = Layout::array::<SlabRecord>(self.shape.count) else {
            return false;
        };
        if self.shape.count == 0
            || self.shape.count > 64
            || state.funded == 0
            || state.funded > self.shape.count
            || (self.shape.bootstrap.is_none() && state.funded != self.shape.count)
            || state.slabs.len() != self.shape.count
            || state.slabs.capacity() != self.shape.count
            || !self
                .meter
                .cold_layout_matches(state._slab_layout.as_ref(), layout.size())
            || !self
                .meter
                .cold_layout_matches(self.allowance.as_ref(), Layout::new::<Core>().size())
            || state.writer > 2
            || state.active.is_some_and(|i| i >= state.funded)
            || (state.funded < 64 && state.ready >> state.funded != 0)
        {
            return false;
        }
        let (mut funded, mut capacity, mut busy, mut views, mut bytes) =
            (0usize, 0usize, 0usize, 0usize, 0usize);
        for (index, record) in state.slabs.iter().enumerate() {
            let Some(slab) = record.funded() else {
                if index < state.funded || self.shape.bootstrap.is_none() {
                    return false;
                }
                continue;
            };
            if index >= state.funded {
                return false;
            }
            let expected = self.shape.capacity_at(index);
            let Some(end) = slab.identity.base.checked_add(expected) else {
                return false;
            };
            if slab.identity.capacity != expected
                || slab.identity.base == 0
                || (slab.views == 0 && slab.view_bytes != 0)
                || (state.ready & (1u64 << index) != 0 && slab.views != 0)
                || (slab.views == 0
                    && state.ready & (1u64 << index) == 0
                    && state.active != Some(index))
            {
                return false;
            }
            let Some(Tail::Mutable(tail)) = &slab.tail else {
                return false;
            };
            let pointer = tail.as_ptr() as usize;
            if !tail.precharged_backing_matches(&self.meter)
                || pointer < slab.identity.base
                || pointer.checked_add(tail.capacity()).is_none_or(|n| n > end)
                || tail.len() > tail.capacity()
                || (state.active == Some(index) && tail.is_empty())
            {
                return false;
            }
            if state.slabs[..index]
                .iter()
                .filter_map(SlabRecord::funded)
                .any(|other| {
                    other
                        .identity
                        .base
                        .checked_add(other.identity.capacity)
                        .is_none_or(|other_end| {
                            slab.identity.base < other_end && other.identity.base < end
                        })
                })
            {
                return false;
            }
            let Some(totals) = funded
                .checked_add(1)
                .zip(capacity.checked_add(expected))
                .zip(busy.checked_add(usize::from(slab.views != 0)))
                .zip(
                    views
                        .checked_add(slab.views)
                        .zip(bytes.checked_add(slab.view_bytes)),
                )
            else {
                return false;
            };
            (((funded, capacity), busy), (views, bytes)) = totals;
        }
        funded == state.funded
            && capacity == state.funded_capacity
            && busy == state.busy
            && views == state.views
            && bytes == state.view_bytes
    }
    pub(in crate::tcp_downlink_pump) fn reserve(
        self: &Arc<Self>,
        limit: usize,
        allow_growth: bool,
    ) -> Result<Option<OutputSpace>, MeteredCopyError> {
        let _quiet = self.quiet_scope();
        self.meter.admit_precharged()?;
        let mut state = self.lock()?;
        if state.closed || state.retiring || state.writer != 1 || limit == 0 || state.operation.is_some() {
            return Err(self.invalid());
        }
        if allow_growth && state.active.is_none()
            && state.ready == 0
            && self.shape.bootstrap.is_some()
            && state.funded < self.shape.count
        {
            let index = state.funded;
            if !matches!(state.slabs.get(index), Some(SlabRecord::Unfunded)) {
                return Err(self.invalid());
            }
            let capacity = self.shape.capacity_at(index);
            let total = state
                .funded_capacity
                .checked_add(capacity)
                .ok_or_else(|| self.invalid())?;
            match BytesMut::try_zeroed_metered(capacity, &self.meter) {
                Ok(tail) => {
                    let identity = AllocationIdentity {
                        base: tail.as_ptr() as usize,
                        capacity: tail.capacity(),
                    };
                    state.slabs[index] = SlabRecord::Funded(Slab {
                        tail: Some(Tail::Mutable(tail)),
                        identity,
                        generation: 0,
                        views: 0,
                        view_bytes: 0,
                    });
                    state.funded += 1;
                    state.funded_capacity = total;
                    state.ready |= 1u64 << index;
                }
                Err(MeteredCopyError::BudgetFull) => {
                    // A global refund has no producer wake. Waiting is legal
                    // only because each actual funded slab is locally loaned.
                    if state.busy != state.funded {
                        return Err(self.invalid());
                    }
                }
                Err(error) => return Err(error),
            }
        }
        if state.active.is_none() {
            while state.ready != 0 {
                let index = state.ready.trailing_zeros() as usize;
                state.ready &= !(1u64 << index);
                let slab = state.slab_mut(index).ok_or_else(|| self.invalid())?;
                if slab.views != 0 {
                    return Err(self.invalid());
                }
                // Move, never clone, the actual owner into a uniqueness test.
                // try_reclaim alone can short-circuit on spare capacity.
                let Some(Tail::Mutable(tail)) = slab.tail.take() else {
                    return Err(self.invalid());
                };
                match tail.freeze().try_into_mut() {
                    Ok(mut tail) => {
                        tail.clear();
                        if !tail.try_reclaim(self.shape.capacity_at(index))
                            || tail.capacity() != self.shape.capacity_at(index)
                        {
                            slab.tail = Some(Tail::Mutable(tail));
                            return Err(self.invalid());
                        }
                        // Cold preparation initialized the whole allocation;
                        // reuse never reallocates or exposes uninitialized bytes.
                        unsafe {
                            tail.set_len(self.shape.capacity_at(index));
                        }
                        slab.tail = Some(Tail::Mutable(tail));
                    }
                    Err(original) => {
                        slab.tail = Some(Tail::Quarantined(original));
                        return Err(self.invalid());
                    }
                }
                slab.generation = slab
                    .generation
                    .checked_add(1)
                    .ok_or_else(|| self.invalid())?;
                state.active = Some(index);
                break;
            }
        }
        let Some(index) = state.active else {
            if state.funded == 0 || state.busy != state.funded {
                return Err(self.invalid());
            }
            return Ok(None);
        };
        let Some(Slab {
            tail: Some(Tail::Mutable(tail)),
            ..
        }) = state.slab(index)
        else {
            return Err(self.invalid());
        };
        let max = tail.len().min(limit);
        if max == 0 {
            return Err(self.invalid());
        }
        let id = state.next_operation;
        state.next_operation = id.checked_add(1).ok_or_else(|| self.invalid())?;
        state.operation = Some(Operation {
            id,
            index,
            max,
            written: 0,
        });
        Ok(Some(OutputSpace {
            root: self.clone(),
            id,
            finished: false,
        }))
    }
    fn mark_quarantined(&self) {
        self.meter.quarantine();
    }
    pub(in crate::tcp_downlink_pump) fn reject(&self) {
        let quiet = self.quiet_scope();
        quiet.mark_quarantined();
    }
}
impl Drop for Root {
    fn drop(&mut self) {
        if !self.retired {
            self.reject();
            return;
        }
        // The unique owner retired actual buffers before this flag. Metadata
        // allocation is destroyed before returning its exact allowance.
        unsafe {
            ManuallyDrop::drop(&mut self.body);
            ManuallyDrop::drop(&mut self.allowance);
        }
    }
}

/// Sole Root owner, separate from its actor's exclusive write lease.
pub(crate) struct OutputStock {
    root: Option<Arc<Root>>,
}
#[derive(Clone, Debug)]
pub(crate) struct StockKey(Weak<Root>);
impl StockKey {
    /// Complete refusal at the borrowed Main boundary (no owner lock held).
    /// Collect both original scopes, mark all budgets, then publish on Drop.
    pub(crate) fn reject_payload(key: Option<&Self>, payload: Option<&CustodiedBytes>) {
        let receiver = key.and_then(|key| key.0.upgrade());
        let expected = receiver.as_ref().map(|root| root.quiet_scope());
        let actual = payload.and_then(CustodiedBytes::quiet_scope);
        if let Some(scope) = &expected { scope.mark_quarantined(); }
        if let Some(scope) = &actual { scope.mark_quarantined(); }
        if let Some(meter) = payload.and_then(CustodiedBytes::backing_meter) { meter.quarantine(); }
        drop(actual);
        drop(expected);
        drop(receiver);
    }
    pub(crate) fn accepts_payload(key: Option<&Self>, payload: &CustodiedBytes) -> bool {
        match key { Some(key) => key.matches_payload(payload), None => !payload.has_stock() }
    }
    pub(crate) fn same_stock(&self, other: &Self) -> bool { Weak::ptr_eq(&self.0, &other.0) }
    pub(in crate::tcp_downlink_pump) fn matches_producer(&self, producer: &StockProducer) -> bool {
        self.0.as_ptr() == Arc::as_ptr(&producer.root)
    }
    pub(crate) fn matches_payload(&self, payload: &CustodiedBytes) -> bool {
        payload.stock_matches(self.0.as_ptr())
    }
}
impl OutputStock {
    pub(in crate::tcp_downlink_pump) fn prepare(
        meter: &BackingMeter,
        shape: StockLayout,
        notify: QueueReturn,
        clock: crate::shared::errors::FailureClock,
    ) -> Result<Self, MeteredCopyError> {
        meter.admit_precharged()?;
        if meter.budget_limit().is_none() {
            return Err(MeteredCopyError::TooLarge);
        }
        if !clock.bound_to_budget(meter) {
            meter.quarantine();
            return Err(MeteredCopyError::Quarantined);
        }
        let layout =
            Layout::array::<SlabRecord>(shape.count).map_err(|_| MeteredCopyError::TooLarge)?;
        let slab_layout = meter.reserve_cold_layout(layout.size())?;
        let ptr = unsafe { alloc(layout) }.cast::<SlabRecord>();
        if ptr.is_null() {
            return Err(MeteredCopyError::Allocation);
        }
        let mut slabs = unsafe { Vec::from_raw_parts(ptr, 0, shape.count) };
        let funded = if shape.bootstrap.is_some() {
            1
        } else {
            shape.count
        };
        let mut funded_capacity = 0usize;
        for index in 0..shape.count {
            if index >= funded {
                slabs.push(SlabRecord::Unfunded);
                continue;
            }
            let capacity = shape.capacity_at(index);
            funded_capacity = funded_capacity
                .checked_add(capacity)
                .ok_or(MeteredCopyError::TooLarge)?;
            let tail = BytesMut::try_zeroed_metered(capacity, meter)?;
            let identity = AllocationIdentity {
                base: tail.as_ptr() as usize,
                capacity: tail.capacity(),
            };
            slabs.push(SlabRecord::Funded(Slab {
                tail: Some(Tail::Mutable(tail)),
                identity,
                generation: 0,
                views: 0,
                view_bytes: 0,
            }));
        }
        let state = State {
            slabs,
            funded,
            funded_capacity,
            busy: 0,
            _slab_layout: slab_layout,
            active: None,
            ready: if funded == 64 {
                u64::MAX
            } else {
                (1u64 << funded) - 1
            },
            operation: None,
            next_operation: 1,
            writer: 0,
            views: 0,
            view_bytes: 0,
            closed: false,
            retiring: false,
        };
        let (body, allowance) = native_wait::cold(
            meter,
            Core {
                state: Mutex::new(state),
                clock,
            },
        )?;
        let root = Arc::new(Root {
            body: ManuallyDrop::new(body),
            allowance: ManuallyDrop::new(allowance),
            meter: meter.clone(),
            notify,
            shape,
            retired: false,
        });
        // First lock is part of cold preparation, including platform mutex state.
        drop(root.lock()?);
        Ok(Self { root: Some(root) })
    }
    pub(crate) fn key(&self) -> Option<StockKey> {
        self.root
            .as_ref()
            .map(|root| StockKey(Arc::downgrade(root)))
    }
    pub(crate) fn take_producer(&mut self) -> Result<StockProducer, MeteredCopyError> {
        let root = self.root.as_ref().ok_or(MeteredCopyError::Quarantined)?;
        let _quiet = root.quiet_scope();
        root.meter.admit_precharged()?;
        let mut state = root.lock()?;
        if state.writer != 0 || state.closed || !root.inventory_matches(&state) {
            return Err(root.invalid());
        }
        state.writer = 1;
        Ok(StockProducer { root: root.clone() })
    }
    pub(crate) fn try_retire(&mut self) -> bool {
        let Some(root) = self.root.as_ref() else {
            return true;
        };
        let _quiet = root.quiet_scope();
        if root.meter.is_quarantined() || Arc::strong_count(root) != 1 {
            return false;
        }
        {
            let Ok(mut state) = root.lock() else {
                return false;
            };
            if state.writer == 1 || state.retiring || state.operation.is_some() || !root.inventory_matches(&state) {
                root.mark_quarantined();
                return false;
            }
            let count = state
                .slabs
                .iter()
                .filter_map(SlabRecord::funded)
                .try_fold(0usize, |n, slab| n.checked_add(slab.views));
            let bytes = state
                .slabs
                .iter()
                .filter_map(SlabRecord::funded)
                .try_fold(0usize, |n, slab| n.checked_add(slab.view_bytes));
            if count != Some(state.views) || bytes != Some(state.view_bytes) {
                root.mark_quarantined();
                return false;
            }
            if state.views != 0 || state.view_bytes != 0 {
                root.mark_quarantined();
                return false;
            }
            for slab in state.slabs.iter_mut().filter_map(SlabRecord::funded_mut) {
                let Some(Tail::Mutable(tail)) = slab.tail.take() else {
                    root.mark_quarantined();
                    return false;
                };
                let original = tail.freeze();
                if original.allocation_identity() != Some(slab.identity) {
                    slab.tail = Some(Tail::Quarantined(original));
                    root.mark_quarantined();
                    return false;
                }
                match original.try_into_mut() {
                    Ok(tail) => slab.tail = Some(Tail::Mutable(tail)),
                    Err(original) => {
                        slab.tail = Some(Tail::Quarantined(original));
                        root.mark_quarantined();
                        return false;
                    }
                }
            }
            state.closed = true;
        }
        let Some(root) = self.root.take() else {
            return false;
        };
        match Arc::try_unwrap(root) {
            Ok(mut root) => {
                // The actual slab Vec and all Shared headers are dropped here.
                // Native Root receipts may only follow this physical retirement.
                root.retired = true;
                drop(root);
                true
            }
            Err(root) => {
                self.root = Some(root);
                false
            }
        }
    }

    /// Borrowing the stock prevents physical retirement until this future and
    /// its original Producer work have actually been destroyed.
    pub(crate) async fn wait_returned(&mut self) -> Result<(), StockWaitError> {
        let Some(root) = self.root.as_ref() else { return Ok(()); };
        root.notify.wait_returned(root).await
    }
}
impl Drop for OutputStock {
    fn drop(&mut self) {
        if self.try_retire() {
            return;
        }
        if let Some(root) = &self.root {
            let _quiet = root.quiet_scope();
            root.mark_quarantined();
        }
        // Root::drop retains the actual body/allowance after uncertainty.
    }
}

pub(crate) struct StockProducer {
    root: Arc<Root>,
}
#[derive(Debug)]
pub(crate) enum StockWaitError {
    /// Complete or cancel the existing grant before asking this port again.
    Pressure,
    Closed,
    Admission(MeteredCopyError),
}
impl StockProducer {
    pub(super) fn validate_space_owner(&self, space: &OutputSpace) -> Result<(), MeteredCopyError> {
        if Arc::ptr_eq(&self.root, &space.root) { return Ok(()); }
        let expected = self.root.quiet_scope();
        let actual = space.root.quiet_scope();
        expected.mark_quarantined();
        actual.mark_quarantined();
        // Both original clocks publish only after all owners were marked; no
        // owner lock is held, and the caller still owns the original space.
        Err(MeteredCopyError::Quarantined)
    }

    // O(1) hint after a real read Pending. Full inventory/physical identity is
    // checked by the actual retirement transaction, not on each DATA read.
    pub(super) fn idle_trim_candidate(&self, space: &OutputSpace) -> Result<bool, StockWaitError> {
        let root = &self.root;
        let _quiet = root.quiet_scope();
        self.validate_space_owner(space).map_err(StockWaitError::Admission)?;
        let spare = root.meter.budget_headroom().map_err(StockWaitError::Admission)?;
        let state = root.lock().map_err(StockWaitError::Admission)?;
        if state.closed { return Err(StockWaitError::Closed); }
        if state.retiring { return Err(StockWaitError::Pressure); }
        let Some(op) = state.operation.filter(|op| op.id == space.id && op.written == 0 && op.max > 0) else {
            return Err(StockWaitError::Admission(root.invalid()));
        };
        if state.writer != 1 || state.funded == 0 || state.funded > root.shape.count || state.funded > 64 {
            return Err(StockWaitError::Admission(root.invalid()));
        }
        if root.shape.bootstrap.is_none() || state.funded <= 1 { return Ok(false); }
        let last = state.funded - 1;
        let cost = BytesMut::metered_copy_layout_for_len(root.shape.capacity_at(last))
            .map_err(StockWaitError::Admission)?;
        if spare.is_none_or(|spare| spare >= cost) { return Ok(false); }
        let Some(slab) = state.slab(last) else { return Err(StockWaitError::Admission(root.invalid())); };
        // A different already-funded allocation must survive the retirement.
        // Rebinding cannot grow even when bootstrap still has a live alias.
        let active_survives = op.index != last && state.active == Some(op.index)
            && state.slab(op.index).is_some_and(|slab| {
                matches!(&slab.tail, Some(Tail::Mutable(tail)) if tail.len() >= op.max)
            });
        Ok(slab.views == 0 && (state.ready & !(1u64 << last) != 0 || active_survives))
    }

    pub(super) fn reserve_existing(&mut self, max: usize) -> Result<Option<OutputSpace>, MeteredCopyError> {
        self.root.reserve(max, false)
    }
    /// Explicit control transaction, never a Loan return. Only the last large
    /// funded record can leave the prefix; bootstrap and all metadata remain.
    pub(super) fn try_retire_idle_output(&mut self) -> Result<bool, StockWaitError> {
        let root = &self.root;
        let _quiet = root.quiet_scope();
        root.meter.admit_precharged().map_err(StockWaitError::Admission)?;
        let mut state = root.lock().map_err(StockWaitError::Admission)?;
        if state.retiring || state.operation.is_some() { return Err(StockWaitError::Pressure); }
        if state.closed { return Err(StockWaitError::Closed); }
        if state.writer != 1 || !root.inventory_matches(&state) {
            return Err(StockWaitError::Admission(root.invalid()));
        }
        if root.shape.bootstrap.is_none() || state.funded == 1 {
            return Err(StockWaitError::Admission(MeteredCopyError::BudgetFull));
        }
        let index = state.funded - 1;
        let slab = state.slab_mut(index).ok_or_else(|| StockWaitError::Admission(root.invalid()))?;
        if slab.views != 0 { return Ok(false); } // Only this actual local loan can wake us.
        let Some(Tail::Mutable(tail)) = slab.tail.take() else {
            return Err(StockWaitError::Admission(root.invalid()));
        };
        let original = tail.freeze();
        if original.allocation_identity() != Some(slab.identity) {
            slab.tail = Some(Tail::Quarantined(original));
            return Err(StockWaitError::Admission(root.invalid()));
        }
        let tail = match original.try_into_mut() {
            Ok(tail) => tail,
            Err(original) => {
                slab.tail = Some(Tail::Quarantined(original));
                return Err(StockWaitError::Admission(root.invalid()));
            }
        };
        // From here to actual Drop there is no await or opaque callback. The
        // guard owns the original value before changing any ledger marker.
        let turn = RetiringTail { root, tail: ManuallyDrop::new(tail), complete: false };
        state.retiring = true;
        state.slabs[index] = SlabRecord::Unfunded;
        state.funded -= 1;
        state.funded_capacity -= root.shape.capacity_at(index);
        state.ready &= !(1u64 << index);
        if state.active == Some(index) { state.active = None; }
        drop(state);
        turn.finish()?;
        Ok(true)
    }
    pub(super) fn queue_return(&self) -> QueueReturn { self.root.notify.clone() }
    pub(super) fn meter(&self) -> &BackingMeter { &self.root.meter }
    pub(super) fn stock_key(&self) -> StockKey { StockKey(Arc::downgrade(&self.root)) }
    pub(super) fn reserve_next(&mut self, max: usize) -> Result<Option<OutputSpace>, MeteredCopyError> { self.root.reserve(max, true) }
    pub(super) fn rejection_scope(&self) -> native_wait::QuietScope {
        self.root.quiet_scope()
    }
    #[cfg(test)]
    pub(in crate::tcp_downlink_pump) fn try_prepare(
        &mut self,
        max: usize,
    ) -> Result<Option<OutputSpace>, MeteredCopyError> {
        self.root.reserve(max, true)
    }
    pub(in crate::tcp_downlink_pump) async fn wait_prepare(&mut self, max: usize) -> Result<OutputSpace, StockWaitError> {
        let _quiet = self.root.quiet_scope();
        self.root.notify.wait_prepare(self.root.clone(), max).await
    }
}

/// Cancellation/unknown unwinding cannot refund the original in-flight value.
/// The buffer destructor has no opaque user fields; callback retirement is
/// separately owned by the original FIFO Producer Work enclosing this turn.
struct RetiringTail<'a> {
    root: &'a Root,
    tail: ManuallyDrop<BytesMut>,
    complete: bool,
}
impl RetiringTail<'_> {
    fn finish(mut self) -> Result<(), StockWaitError> {
        // SAFETY: this is the only physical owner and the sole disposal attempt.
        unsafe { ManuallyDrop::drop(&mut self.tail); }
        let mut state = self.root.lock().map_err(StockWaitError::Admission)?;
        if !state.retiring || !self.root.inventory_matches(&state) {
            return Err(StockWaitError::Admission(self.root.invalid()));
        }
        state.retiring = false;
        self.complete = true;
        drop(state);
        self.root.meter.admit_precharged().map_err(StockWaitError::Admission)
    }
}
impl Drop for RetiringTail<'_> {
    fn drop(&mut self) {
        if !self.complete { self.root.reject(); }
    }
}
impl Drop for StockProducer {
    fn drop(&mut self) {
        let _quiet = self.root.quiet_scope();
        let Ok(mut state) = self.root.lock() else {
            return;
        };
        if state.writer != 1 || state.operation.is_some() {
            self.root.mark_quarantined();
            return;
        }
        state.writer = 2;
    }
}

pub(crate) struct OutputSpace {
    root: Arc<Root>,
    id: u64,
    finished: bool,
}
impl OutputSpace {
    pub(crate) fn validate_meter(&self, expected: Option<&BackingMeter>) -> Result<(), MeteredCopyError> {
        let _quiet = self.root.quiet_scope();
        if !expected.is_some_and(|expected| self.root.meter.same_authority(expected)) {
            // Mark both actual owners before this owner's lock-free publication.
            if let Some(expected) = expected { expected.quarantine(); }
            return Err(self.root.invalid());
        }
        self.root.meter.admit_precharged()?;
        let state = self.root.lock()?;
        if !state.operation.is_some_and(|op| op.id == self.id && op.max != 0 && op.written == 0) {
            return Err(self.root.invalid());
        }
        Ok(())
    }
    pub(crate) fn capacity(&self) -> usize {
        let _quiet = self.root.quiet_scope();
        self.root
            .lock()
            .ok()
            .and_then(|s| s.operation.filter(|op| op.id == self.id))
            .map_or(0, |op| op.max)
    }
    pub(crate) fn write(&mut self, bytes: &[u8]) -> Result<(), MeteredCopyError> {
        let _quiet = self.root.quiet_scope();
        self.root.meter.admit_precharged()?;
        let mut state = self.root.lock()?;
        let op = state
            .operation
            .filter(|op| op.id == self.id)
            .ok_or_else(|| self.root.invalid())?;
        let end = op
            .written
            .checked_add(bytes.len())
            .filter(|n| *n <= op.max)
            .ok_or_else(|| self.root.invalid())?;
        let Some(Slab {
            tail: Some(Tail::Mutable(tail)),
            ..
        }) = state.slab_mut(op.index)
        else {
            return Err(self.root.invalid());
        };
        let output = tail
            .get_mut(op.written..end)
            .ok_or_else(|| self.root.invalid())?;
        output.copy_from_slice(bytes);
        state.operation = Some(Operation { written: end, ..op });
        Ok(())
    }
    pub(crate) fn finish(mut self) -> Result<CustodiedBytes, MeteredCopyError> {
        self.finish_inner()
    }
    /// A borrowed caller's original grant survives every refusal. Only a real
    /// typed output permits removal of the completed, now inert operation.
    pub(crate) fn finish_retained(slot: &mut Option<Self>) -> Result<CustodiedBytes, MeteredCopyError> {
        let output = slot.as_mut().ok_or(MeteredCopyError::Quarantined)?.finish_inner()?;
        drop(slot.take());
        Ok(output)
    }
    fn finish_inner(&mut self) -> Result<CustodiedBytes, MeteredCopyError> {
        let _quiet = self.root.quiet_scope();
        self.root.meter.admit_precharged()?;
        let mut state = self.root.lock()?;
        let op = state
            .operation
            .filter(|op| op.id == self.id && op.written != 0)
            .ok_or_else(|| self.root.invalid())?;
        let total = state
            .views
            .checked_add(1)
            .ok_or_else(|| self.root.invalid())?;
        let total_bytes = state
            .view_bytes
            .checked_add(op.written)
            .ok_or_else(|| self.root.invalid())?;
        let old_busy = state.busy;
        let slab = state
            .slab_mut(op.index)
            .ok_or_else(|| self.root.invalid())?;
        let busy = if slab.views == 0 {
            old_busy.checked_add(1).ok_or_else(|| self.root.invalid())?
        } else {
            old_busy
        };
        let count = slab
            .views
            .checked_add(1)
            .ok_or_else(|| self.root.invalid())?;
        let view_bytes = slab
            .view_bytes
            .checked_add(op.written)
            .ok_or_else(|| self.root.invalid())?;
        let Some(Tail::Mutable(tail)) = slab.tail.as_mut() else {
            return Err(self.root.invalid());
        };
        if op.written > tail.len() || !tail.precharged_backing_matches(&self.root.meter) {
            return Err(self.root.invalid());
        }
        let bytes = tail.split_to(op.written).freeze();
        let loan = Loan {
            root: self.root.clone(),
            index: op.index,
            generation: slab.generation,
            len: op.written,
            tracked: true,
        };
        slab.views = count;
        slab.view_bytes = view_bytes;
        let exhausted = tail.is_empty();
        state.views = total;
        state.view_bytes = total_bytes;
        state.busy = busy;
        state.ready &= !(1u64 << op.index);
        if exhausted {
            state.active = None;
        }
        state.operation = None;
        self.finished = true;
        drop(state);
        Ok(CustodiedBytes::from_stock(bytes, loan))
    }
}
impl Drop for OutputSpace {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        let _quiet = self.root.quiet_scope();
        let Ok(mut state) = self.root.lock() else {
            return;
        };
        if state.operation.is_some_and(|op| op.id == self.id) {
            state.operation = None;
        } else {
            self.root.mark_quarantined();
        }
    }
}

pub(in crate::tcp_downlink_pump) struct Loan {
    root: Arc<Root>,
    index: usize,
    generation: u64,
    len: usize,
    tracked: bool,
}
impl std::fmt::Debug for Loan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StockLoan")
            .field("index", &self.index)
            .field("generation", &self.generation)
            .field("len", &self.len)
            .field("tracked", &self.tracked)
            .finish()
    }
}
impl Loan {
    pub(in crate::tcp_downlink_pump) fn quiet_scope(&self) -> native_wait::QuietScope {
        self.root.quiet_scope()
    }
    pub(in crate::tcp_downlink_pump) fn quarantine(&self) {
        self.root.reject();
    }
    pub(in crate::tcp_downlink_pump) fn matches(&self, root: *const Root) -> bool {
        Arc::as_ptr(&self.root) == root
    }
    pub(in crate::tcp_downlink_pump) fn valid(&self, bytes: &Bytes) -> bool {
        let Ok(state) = self.root.lock() else {
            return false;
        };
        self.tracked
            && self.len == bytes.len()
            && state.slab(self.index).is_some_and(|slab| {
                slab.generation == self.generation
                    && slab.views != 0
                    && bytes.allocation_identity() == Some(slab.identity)
                    && bytes
                        .backing_meter()
                        .is_some_and(|meter| meter.same_authority(&self.root.meter))
            })
    }
    pub(in crate::tcp_downlink_pump) fn clone_for(&self, bytes: &Bytes) -> Option<Self> {
        if bytes.is_empty() && bytes.allocation_identity().is_none() {
            return None;
        }
        let mut next = Self {
            root: self.root.clone(),
            index: self.index,
            generation: self.generation,
            len: bytes.len(),
            tracked: false,
        };
        let Ok(mut state) = self.root.lock() else {
            return Some(next);
        };
        let Some(slab) = state.slab(self.index) else {
            self.root.mark_quarantined();
            return Some(next);
        };
        if !self.tracked
            || slab.views == 0
            || slab.generation != self.generation
            || bytes.allocation_identity() != Some(slab.identity)
        {
            self.root.mark_quarantined();
            return Some(next);
        }
        let counts = slab
            .views
            .checked_add(1)
            .zip(slab.view_bytes.checked_add(bytes.len()))
            .zip(
                state
                    .views
                    .checked_add(1)
                    .zip(state.view_bytes.checked_add(bytes.len())),
            );
        let Some(((views, view_bytes), (total, total_bytes))) = counts else {
            self.root.mark_quarantined();
            return Some(next);
        };
        let Some(slab) = state.slab_mut(self.index) else {
            self.root.mark_quarantined();
            return Some(next);
        };
        slab.views = views;
        slab.view_bytes = view_bytes;
        state.views = total;
        state.view_bytes = total_bytes;
        state.ready &= !(1u64 << self.index);
        next.tracked = true;
        Some(next)
    }
    pub(in crate::tcp_downlink_pump) fn truncate(&mut self, len: usize) {
        let Ok(mut state) = self.root.lock() else {
            return;
        };
        let Some(slab) = state.slab(self.index) else {
            self.root.mark_quarantined();
            return;
        };
        if !self.tracked || len > self.len || slab.generation != self.generation {
            self.root.mark_quarantined();
            return;
        }
        let delta = self.len - len;
        let Some((local, total)) = slab
            .view_bytes
            .checked_sub(delta)
            .zip(state.view_bytes.checked_sub(delta))
        else {
            self.root.mark_quarantined();
            return;
        };
        let Some(slab) = state.slab_mut(self.index) else {
            self.root.mark_quarantined();
            return;
        };
        slab.view_bytes = local;
        state.view_bytes = total;
        self.len = len;
    }
}
impl Drop for Loan {
    fn drop(&mut self) {
        let _quiet = self.root.quiet_scope();
        if !self.tracked {
            self.root.mark_quarantined();
            return;
        }
        let notify = {
            let Ok(mut state) = self.root.lock() else {
                return;
            };
            let Some(slab) = state.slab(self.index) else {
                self.root.mark_quarantined();
                return;
            };
            if slab.generation != self.generation {
                self.root.mark_quarantined();
                return;
            }
            let counts = slab
                .views
                .checked_sub(1)
                .zip(slab.view_bytes.checked_sub(self.len))
                .zip(
                    state
                        .views
                        .checked_sub(1)
                        .zip(state.view_bytes.checked_sub(self.len)),
                );
            let Some(((views, view_bytes), (total, total_bytes))) = counts else {
                self.root.mark_quarantined();
                return;
            };
            let busy = if views == 0 {
                state.busy.checked_sub(1)
            } else {
                Some(state.busy)
            };
            let Some(busy) = busy else {
                self.root.mark_quarantined();
                return;
            };
            let Some(slab) = state.slab_mut(self.index) else {
                self.root.mark_quarantined();
                return;
            };
            slab.views = views;
            slab.view_bytes = view_bytes;
            state.busy = busy;
            state.views = total;
            state.view_bytes = total_bytes;
            if views == 0 {
                state.ready |= 1u64 << self.index;
            }
            (views == 0 && !state.closed, state.writer)
        };
        if notify.0 && !self.root.meter.is_quarantined() && !std::thread::panicking() {
            // A joined writer may have a Roles retirement waiter. An absent
            // original queue is legal only after that writer actually exited.
            if !self.root.notify.notify() && notify.1 == 1 {
                self.root.mark_quarantined();
            }
        }
    }
}

#[cfg(test)]
mod tests;
