//! Physical DATAGRAM descriptors belong to the original DatagramState queues.
//! Growth prepares the complete replacement before eviction/publication; old
//! and new layouts coexist until actual old storage Drop. No payload refund,
//! new notification authority or independent message limit lives here.
use super::*;
use bytes::{BackingBudgetReservation, BackingMeter, MeteredCopyError};
use std::{
    alloc::{Layout, alloc},
    ops::Deref,
};

#[derive(Default)]
pub(in crate::connection) struct Queue {
    values: VecDeque<Datagram>,
    allowance: Option<BackingBudgetReservation>,
}
impl Deref for Queue {
    type Target = VecDeque<Datagram>;
    fn deref(&self) -> &Self::Target {
        &self.values
    }
}
impl Queue {
    pub(super) fn layout_matches(&self, meter: &BackingMeter) -> bool {
        Layout::array::<Datagram>(self.values.capacity())
            .is_ok_and(|layout| meter.cold_layout_matches(self.allowance.as_ref(), layout.size()))
    }
    pub(super) fn prepare(
        &self,
        needed: usize,
        meter: Option<&BackingMeter>,
    ) -> Result<Option<Self>, MeteredCopyError> {
        let Some(meter) = meter else {
            return Ok(None);
        };
        meter.admit_precharged()?;
        if !self.layout_matches(meter) {
            meter.quarantine();
            return Err(MeteredCopyError::Quarantined);
        }
        if needed <= self.values.capacity() {
            return Ok(None);
        }
        let count = needed
            .max(
                self.values
                    .capacity()
                    .checked_mul(2)
                    .ok_or(MeteredCopyError::TooLarge)?,
            )
            .max(4);
        let layout = Layout::array::<Datagram>(count).map_err(|_| MeteredCopyError::TooLarge)?;
        let allowance = meter.reserve_cold_layout(layout.size())?;
        let ptr = unsafe { alloc(layout).cast::<Datagram>() };
        if ptr.is_null() {
            return Err(MeteredCopyError::Allocation);
        }
        let values = VecDeque::from(unsafe { Vec::from_raw_parts(ptr, 0, count) });
        Ok(Some(Self { values, allowance }))
    }
    pub(super) fn funded(&self, needed: usize, meter: Option<&BackingMeter>) -> bool {
        meter.is_none_or(|m| self.layout_matches(m) && needed <= self.values.capacity())
    }
    pub(super) fn install(&mut self, replacement: Option<Self>) {
        if let Some(mut new) = replacement {
            new.values.extend(self.values.drain(..));
            *self = new;
        }
    }
    pub(super) fn append(&mut self, replacement: Option<Self>, evict: usize, value: Datagram) {
        // All capacity and arithmetic checks precede this infallible commit.
        for _ in 0..evict {
            drop(self.values.pop_front());
        }
        self.install(replacement);
        self.values.push_back(value);
    }
    pub(super) fn pop_front(&mut self) -> Option<Datagram> {
        self.values.pop_front()
    }
    pub(super) fn push_front(&mut self, value: Datagram) {
        self.values.push_front(value);
    }
    pub(super) fn retain(&mut self, f: impl FnMut(&Datagram) -> bool) {
        self.values.retain(f);
    }
}
