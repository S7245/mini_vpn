//! Original StreamsState operation inventories: pending heap, events and blocked
//! IDs. Keeps their actual layouts under this Connection's meter. Preparation
//! returns unpublished replacements; one commit precedes protocol mutation.
//! Maps and Send/Recv instances retain the separately specified configuration
//! partition. There is no new queue, clock, task or refund authority.
use super::*;
use crate::connection::storage::Slab;
use bytes::{BackingBudgetReservation, BackingMeter, MeteredCopyError};
use std::{alloc::Layout, collections::BinaryHeap};
use super::super::PendingStream;
#[cfg(test)]
#[path = "tests/inventory.rs"]
mod tests;

pub(super) struct Inventory {
    meter: Option<BackingMeter>,
    pending: Option<BackingBudgetReservation>,
    events: Option<BackingBudgetReservation>,
    blocked: Option<BackingBudgetReservation>,
}
impl Inventory {
    pub(super) fn new(meter: Option<BackingMeter>) -> Self {
        Self { meter: meter.filter(|m| m.budget_limit().is_some()), pending: None, events: None, blocked: None }
    }
}
pub(in crate::connection) struct Prepared {
    owner: usize,
    shape: [usize; 6],
    meter: Option<BackingMeter>,
    pending: Option<Slab<PendingStream>>,
    events: Option<Slab<StreamEvent>>,
    blocked: Option<Slab<StreamId>>,
}
fn prepare<T>(len: usize, capacity: usize, add: usize, meter: Option<&BackingMeter>) -> Result<Option<Slab<T>>, MeteredCopyError> {
    let Some(meter) = meter else { return Ok(None); };
    let needed = len.checked_add(add).ok_or(MeteredCopyError::TooLarge)?;
    if needed <= capacity { return Ok(None); }
    let capacity = needed.max(capacity.checked_mul(2).ok_or(MeteredCopyError::TooLarge)?).max(4);
    Slab::prepare(capacity, Some(meter)).map(Some)
}
impl StreamsState {
    pub(in crate::connection) fn native_meter(&self) -> Option<&BackingMeter> { self.inventory.meter.as_ref() }
    fn operation_shape(&self) -> [usize; 6] {
        [self.pending.streams.len(), self.pending.streams.capacity(), self.events.len(), self.events.capacity(),
            self.connection_blocked.len(), self.connection_blocked.capacity()]
    }
    fn operation_layouts_match(&self, meter: &BackingMeter) -> bool {
        fn valid<T>(cap: usize, token: Option<&BackingBudgetReservation>, meter: &BackingMeter) -> bool {
            Layout::array::<T>(cap).is_ok_and(|layout| meter.cold_layout_matches(token, layout.size()))
        }
        valid::<PendingStream>(self.pending.streams.capacity(), self.inventory.pending.as_ref(), meter)
            && valid::<StreamEvent>(self.events.capacity(), self.inventory.events.as_ref(), meter)
            && valid::<StreamId>(self.connection_blocked.capacity(), self.inventory.blocked.as_ref(), meter)
    }
    pub(in crate::connection) fn prepare_operations(&self, events: usize, pending: usize, blocked: usize) -> Result<Prepared, MeteredCopyError> {
        let meter = self.native_meter();
        if let Some(meter) = meter {
            meter.admit_precharged()?;
            if !self.operation_layouts_match(meter) { meter.quarantine(); return Err(MeteredCopyError::Quarantined); }
        }
        Ok(Prepared {
            owner: self as *const Self as usize, shape: self.operation_shape(), meter: meter.cloned(),
            pending: prepare(self.pending.streams.len(), self.pending.streams.capacity(), pending, meter)?,
            events: prepare(self.events.len(), self.events.capacity(), events, meter)?,
            blocked: prepare(self.connection_blocked.len(), self.connection_blocked.capacity(), blocked, meter)?,
        })
    }
    pub(in crate::connection) fn commit_operations(&mut self, plan: Prepared) -> Result<(), MeteredCopyError> {
        if plan.owner != self as *const Self as usize || plan.shape != self.operation_shape()
            || match (self.native_meter(), plan.meter.as_ref()) {
                (Some(a), Some(b)) => !a.same_authority(b), (None, None) => false, _ => true,
            } {
            if let Some(meter) = self.native_meter() { meter.quarantine(); }
            if let Some(meter) = &plan.meter { meter.quarantine(); }
            return Err(MeteredCopyError::Quarantined);
        }
        if let Some(mut new) = plan.pending {
            new.values.extend(self.pending.streams.drain());
            let old = mem::replace(&mut self.pending.streams, BinaryHeap::from(new.values));
            let token = mem::replace(&mut self.inventory.pending, new.allowance);
            drop(old); drop(token);
        }
        if let Some(new) = plan.events {
            let mut values = VecDeque::from(new.values);
            values.extend(self.events.drain(..));
            let old = mem::replace(&mut self.events, values);
            let token = mem::replace(&mut self.inventory.events, new.allowance);
            drop(old); drop(token);
        }
        if let Some(mut new) = plan.blocked {
            new.values.append(&mut self.connection_blocked);
            let old = mem::replace(&mut self.connection_blocked, new.values);
            let token = mem::replace(&mut self.inventory.blocked, new.allowance);
            drop(old); drop(token);
        }
        Ok(())
    }
    pub(in crate::connection) fn reserve_operations(&mut self, events: usize, pending: usize, blocked: usize) -> Result<(), MeteredCopyError> {
        let plan = self.prepare_operations(events, pending, blocked)?;
        self.commit_operations(plan)
    }
    pub(super) fn readable_event_needed(&self, id: StreamId) -> usize {
        usize::from(id.initiator() == self.side || id.index() < self.next_remote[id.dir() as usize])
    }
}
impl Drop for StreamsState {
    fn drop(&mut self) {
        let Some(meter) = self.native_meter() else { return; };
        if std::thread::panicking() || !self.operation_layouts_match(meter) { meter.quarantine(); }
        if meter.admit_precharged().is_ok() { return; }
        // Keep the original actual allocations and matching tokens on unknown
        // interruption. Queue clear/empty does not prove physical retirement.
        mem::forget(mem::take(&mut self.pending.streams));
        mem::forget(mem::take(&mut self.events));
        mem::forget(mem::take(&mut self.connection_blocked));
        mem::forget(self.inventory.pending.take());
        mem::forget(self.inventory.events.take());
        mem::forget(self.inventory.blocked.take());
    }
}

pub(in crate::connection) fn inventory_reason(error: MeteredCopyError) -> &'static str {
    match error {
        MeteredCopyError::BudgetFull => "stream physical inventory budget full",
        MeteredCopyError::TooLarge => "stream physical inventory layout too large",
        MeteredCopyError::Allocation => "stream physical inventory allocation failed",
        MeteredCopyError::Quarantined => "stream physical inventory authority lost",
    }
}

pub(super) fn transport(error: MeteredCopyError) -> TransportError {
    tracing::trace!(?error, "stream operation inventory refused");
    TransportError::INTERNAL_ERROR(inventory_reason(error))
}

impl crate::Connection {
    /// Cold, read-only measurement of original Native stream metadata retained
    /// on unknown interruption. Validates actual container layouts and their
    /// tokens even in Q; does not admit, mutate, allocate or certify shutdown.
    /// Excludes payload backing, maps/instance headers, Recv storage and other
    /// Connection owners. `None` means Legacy or an invalid physical inventory.
    pub fn retained_stream_metadata_bytes(&self) -> Option<usize> {
        let state = &self.streams;
        let meter = state.native_meter()?;
        state.operation_layouts_match(meter).then_some(())?;
        let mut total = Layout::array::<PendingStream>(state.pending.streams.capacity()).ok()?.size()
            .checked_add(Layout::array::<StreamEvent>(state.events.capacity()).ok()?.size())?
            .checked_add(Layout::array::<StreamId>(state.connection_blocked.capacity()).ok()?.size())?;
        for stream in state.send.values().flatten() {
            total = total.checked_add(stream.pending.retained_metadata_bytes(meter)?)?;
        }
        Some(total)
    }
}
