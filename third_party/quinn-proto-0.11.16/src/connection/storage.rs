//! Exact-layout storage algorithms shared by original connection inventories.
//! Each caller owns its actual slab, token and retirement; this module adds no
//! queue, meter, clock or refund authority. Growth holds old and new layouts.
//! Entries: Slab and intervals::Intervals. Tests: storage/tests/intervals.rs.
use bytes::{BackingBudgetReservation, BackingMeter, MeteredCopyError};
use std::alloc::{alloc, Layout};
pub(super) mod intervals;
pub(super) mod stock;

#[derive(Debug)]
pub(super) struct Slab<T> {
    pub(super) values: Vec<T>,
    // Fields drop in declaration order: layout before allowance.
    pub(super) allowance: Option<BackingBudgetReservation>,
}
impl<T> Default for Slab<T> {
    fn default() -> Self {
        Self {
            values: Vec::new(),
            allowance: None,
        }
    }
}
impl<T> Slab<T> {
    pub(super) fn layout_matches(&self, meter: &BackingMeter) -> bool {
        Layout::array::<T>(self.values.capacity()).is_ok_and(|layout|
            meter.cold_layout_matches(self.allowance.as_ref(), layout.size()))
    }
    pub(super) fn prepare(
        capacity: usize,
        meter: Option<&BackingMeter>,
    ) -> Result<Self, MeteredCopyError> {
        if capacity == 0 {
            return Ok(Self::default());
        }
        let layout = Layout::array::<T>(capacity)
            .map_err(|_| MeteredCopyError::TooLarge)?;
        // These slabs contain non-ZST Buffer/Node values only.
        debug_assert_ne!(layout.size(), 0);
        let allowance = meter
            .map(|m| m.reserve_cold_layout(layout.size()))
            .transpose()?
            .flatten();
        let ptr = unsafe { alloc(layout).cast::<T>() };
        if ptr.is_null() {
            return Err(MeteredCopyError::Allocation);
        }
        Ok(Self {
            values: unsafe { Vec::from_raw_parts(ptr, 0, capacity) },
            allowance,
        })
    }
    pub(super) fn growth(
        &self,
        additional: usize,
        meter: Option<&BackingMeter>,
    ) -> Result<Option<Self>, MeteredCopyError> {
        let needed = self
            .values
            .len()
            .checked_add(additional)
            .ok_or(MeteredCopyError::TooLarge)?;
        if needed <= self.values.capacity() {
            return Ok(None);
        }
        Self::prepare(
            needed.max(self.values.capacity().saturating_mul(2)).max(4),
            meter,
        )
        .map(Some)
    }
    pub(super) fn install(&mut self, replacement: Option<Self>) {
        if let Some(mut new) = replacement {
            debug_assert!(new.values.capacity() >= self.values.len());
            new.values.append(&mut self.values);
            *self = new;
        }
    }
}

