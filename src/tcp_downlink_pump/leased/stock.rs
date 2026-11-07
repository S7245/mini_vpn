//! Original Leased FIFO's progressive cold inventory and read-credit transfer.
//! Owns no second DATA queue or refund authority. The shared stock ledger owns
//! slabs; this owner alone converts reserved -> queued -> leased -> released.
//! Source retains one common read claim. Disposal and permit ACK remain separate.
//! Tests: leased/tests/stock.rs and real Service native_complete/leased_stock.rs.
use super::*;
use crate::tcp_downlink_pump::{output_stock::{OutputStock, StockLayout, ReadAdmission, StockKey, StockWaitError},
    native_wait::{QuietScope, Waiters}, ByteCustodyError, OwnedPayload, MeteredMutation};
use bytes::{BackingMeter, MeteredCopyError};

impl AsyncLeasedByteFlowQueue {
    pub(crate) fn new_with_progressive_output_stock(
        capacity: usize, mode: DownstreamPermitReleaseMode, meter: &BackingMeter,
        clock: crate::shared::errors::FailureClock, slab: usize, window: usize,
    ) -> Result<(Self, OutputStock), MeteredCopyError> {
        let shape = StockLayout::progressive(capacity, slab, window)?;
        let queue = Self::new_custodied_with_mode(capacity, mode).map_err(|_| MeteredCopyError::TooLarge)?;
        // This fresh queue has no published handle or registered waiter. The
        // existing cold binding consumes Start exactly once; no Legacy upgrade.
        queue.bind_native_meter_typed(Some(meter.clone()))?;
        if !queue.bind_native_failures(clock.clone())
            || !queue.inner.waiters.get().is_some_and(Waiters::stock_bound) {
            meter.quarantine(); return Err(MeteredCopyError::Quarantined);
        }
        queue.inner.lock_state().chunks.prepare(Some(meter.clone()))?;
        let stock = OutputStock::prepare(meter, shape,
            super::super::output_stock::QueueReturn::Leased(StockReturn(Arc::downgrade(&queue.inner))), clock)?;
        let key = stock.key().ok_or(MeteredCopyError::Quarantined)?;
        if queue.inner.stock.set(key).is_err() { meter.quarantine(); return Err(MeteredCopyError::Quarantined); }
        Ok((queue, stock))
    }
    pub(crate) fn has_output_stock(&self) -> bool { self.inner.stock.get().is_some() }
}

#[derive(Clone)]
pub(in crate::tcp_downlink_pump) struct StockQueue(Arc<AsyncLeasedByteFlowQueueInner>);
impl StockReturn {
    pub(in crate::tcp_downlink_pump) fn upgrade(&self) -> Option<StockQueue> { self.0.upgrade().map(StockQueue) }
}
impl StockQueue {
    pub(in crate::tcp_downlink_pump) fn same(&self, other: &Self) -> bool { Arc::ptr_eq(&self.0, &other.0) }
    pub(in crate::tcp_downlink_pump) fn read(&self) -> &ReadAdmission { &self.0.read }
    pub(in crate::tcp_downlink_pump) fn waiters(&self) -> Option<&Waiters> { self.0.waiters.get() }
    pub(in crate::tcp_downlink_pump) fn stock(&self) -> Option<&StockKey> { self.0.stock.get() }
    pub(in crate::tcp_downlink_pump) fn not_empty(&self) -> &Notify { &self.0.not_empty }
    pub(in crate::tcp_downlink_pump) fn not_full(&self) -> &Notify { &self.0.not_full }
    pub(in crate::tcp_downlink_pump) fn begin(&self) { self.0.start.begin(); }
    fn valid(&self, state: &LeasedByteFlowQueueState) -> bool {
        state.custodied && !state.custody.interrupted && !state.custody.frozen
            && self.0.global_budget.is_none() && state.owned_bytes() <= state.capacity_bytes
    }
    pub(in crate::tcp_downlink_pump) fn open(&self, quiet: &QuietScope) -> Result<(), StockWaitError> {
        let state = self.0.lock_state();
        if state.closure.is_closed() || self.waiters().is_none_or(Waiters::is_closed) { return Err(StockWaitError::Closed); }
        if !self.valid(&state) { quiet.mark_quarantined(); return Err(StockWaitError::Admission(MeteredCopyError::Quarantined)); }
        if self.read().is_active() { return Err(StockWaitError::Pressure); }
        Ok(())
    }
    pub(in crate::tcp_downlink_pump) fn prepare(&self, max: usize, meter: &BackingMeter, quiet: &QuietScope)
        -> Result<Option<(u64, usize)>, StockWaitError> {
        let mut state = self.0.lock_state();
        if state.closure.is_closed() || self.waiters().is_none_or(Waiters::is_closed) { return Err(StockWaitError::Closed); }
        if !self.valid(&state) || !state.chunks.meter().is_some_and(|m| m.same_authority(meter)) {
            quiet.mark_quarantined(); return Err(StockWaitError::Admission(MeteredCopyError::Quarantined));
        }
        if self.read().is_active() { return Err(StockWaitError::Pressure); }
        if state.reserved_bytes != 0 { quiet.mark_quarantined(); return Err(StockWaitError::Admission(MeteredCopyError::Quarantined)); }
        let available = max.min(state.available_bytes());
        if available == 0 { return Ok(None); }
        match state.chunks.prepare(Some(meter.clone())) {
            Ok(()) => {},
            Err(MeteredCopyError::BudgetFull) if !state.chunks.is_empty() => return Ok(None),
            Err(error) => return Err(StockWaitError::Admission(error)),
        }
        let Some(id) = self.read().arm(available) else {
            quiet.mark_quarantined(); return Err(StockWaitError::Admission(MeteredCopyError::Quarantined));
        };
        state.reserved_bytes = available;
        state.note_high_water();
        Ok(Some((id, available)))
    }
    pub(in crate::tcp_downlink_pump) fn resize(&self, id: u64, old: usize, next: usize) -> bool {
        let mut state = self.0.lock_state();
        if !self.valid(&state) || state.reserved_bytes != old || !self.read().resize(id, old, next) { return false; }
        state.reserved_bytes = next;
        true
    }
    pub(in crate::tcp_downlink_pump) fn release(&self, id: u64, max: usize, committed: bool) -> bool {
        let mut state = self.0.lock_state();
        if !self.valid(&state) || state.reserved_bytes != if committed { 0 } else { max }
            || !self.read().release(id, max) { return false; }
        state.reserved_bytes = 0;
        true
    }
    pub(in crate::tcp_downlink_pump) fn publish(
        &self, pending: &mut Option<OwnedPayload>, grant: (u64, usize),
        meter: &BackingMeter, quiet: &QuietScope, committed: &mut bool,
    ) -> Result<(), ByteCustodyError> {
        let mut state = self.0.lock_state();
        let (id, max) = grant;
        let Some(payload) = pending.as_ref() else { quiet.mark_quarantined(); return Err(ByteCustodyError::Invalid); };
        // Closure prevents new admission but cannot revoke this real grant.
        if !self.valid(&state) || *committed || !self.read().matches(id, max)
            || state.reserved_bytes != max || payload.is_empty() || payload.len() > max
            || state.chunks.len() >= state.chunks.capacity()
            || !state.chunks.meter().is_some_and(|m| m.same_authority(meter)) {
            quiet.mark_quarantined(); return Err(ByteCustodyError::Invalid);
        }
        let Some(sources) = state.custody.sources.checked_add(1) else { quiet.mark_quarantined(); return Err(ByteCustodyError::Invalid); };
        let Some(emitted) = state.custody.emitted.checked_add(payload.len()) else { quiet.mark_quarantined(); return Err(ByteCustodyError::Invalid); };
        let mutation = MeteredMutation::begin(Some(meter)).map_err(ByteCustodyError::Capacity)?;
        state.custody.interrupted = true;
        let payload = pending.take().ok_or(ByteCustodyError::Invalid)?;
        let allocation = payload.capacity();
        state.reserved_bytes = 0;
        state.queued_bytes += payload.len();
        state.chunks.push_back(ByteChunk { bytes: payload.into_custodied_bytes(), allocation: Some(allocation) });
        state.custody.sources = sources; state.custody.emitted = emitted;
        state.note_high_water();
        *committed = true;
        state.custody.interrupted = false;
        drop(state);
        self.0.not_empty.notify_one();
        mutation.commit();
        Ok(())
    }
}
