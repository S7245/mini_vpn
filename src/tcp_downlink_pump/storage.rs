//! Original FIFO descriptor storage. Prepares the actual VecDeque allocation
//! under the payload authority before transfer, keeping old/new charges together.
//! No logical-credit or permit ledger lives here. Tests: tests/native_storage.rs.
use super::ByteChunk;
use bytes::{BackingBudgetReservation, BackingMeter, MeteredCopyError};
use std::{
    alloc::{Layout, alloc},
    collections::VecDeque,
    ops::Deref,
};

#[derive(Debug, Default)]
pub(super) struct Chunks {
    values: VecDeque<ByteChunk>,
    _allowance: Option<BackingBudgetReservation>,
    meter: Option<BackingMeter>,
}
impl Deref for Chunks {
    type Target = VecDeque<ByteChunk>;
    fn deref(&self) -> &Self::Target {
        &self.values
    }
}
impl Chunks {
    pub(super) fn bind(&mut self, meter: Option<BackingMeter>) -> Option<()> {
        if self.values.capacity() != 0 || self.meter.is_some() {
            return None;
        }
        if meter.as_ref().is_some_and(BackingMeter::is_quarantined) {
            return None;
        }
        self.meter = meter;
        Some(())
    }
    pub(super) fn prepare(&mut self, actual: Option<BackingMeter>) -> Result<(), MeteredCopyError> {
        match (&self.meter, &actual) {
            (Some(expected), Some(actual)) if expected.same_authority(actual) => {
                expected.admit_precharged()?
            }
            (None, None) => {
                self.values
                    .try_reserve(1)
                    .map_err(|_| MeteredCopyError::Allocation)?;
                return Ok(());
            }
            (None, Some(_)) if self.values.capacity() == 0 => {}
            _ => {
                if let Some(m) = &self.meter {
                    m.quarantine();
                }
                if let Some(m) = &actual {
                    m.quarantine();
                }
                return Err(MeteredCopyError::Quarantined);
            }
        }
        if self.values.len() < self.values.capacity() {
            return Ok(());
        }
        let capacity = self
            .values
            .capacity()
            .checked_mul(2)
            .ok_or(MeteredCopyError::TooLarge)?
            .max(4);
        let layout =
            Layout::array::<ByteChunk>(capacity).map_err(|_| MeteredCopyError::TooLarge)?;
        let allowance = actual
            .as_ref()
            .map(|m| m.reserve_cold_layout(layout.size()))
            .transpose()?
            .flatten();
        // VecDeque::from(Vec) transfers its allocation (no element copy or reallocation).
        let ptr = unsafe { alloc(layout).cast::<ByteChunk>() };
        if ptr.is_null() {
            return Err(MeteredCopyError::Allocation);
        }
        let vec = unsafe { Vec::from_raw_parts(ptr, 0, capacity) };
        let mut next = Self {
            values: VecDeque::from(vec),
            _allowance: allowance,
            meter: actual,
        };
        while let Some(chunk) = self.values.pop_front() {
            next.values.push_back(chunk);
        }
        *self = next;
        Ok(())
    }
    pub(super) fn meter(&self) -> Option<&BackingMeter> {
        self.meter.as_ref()
    }
    pub(super) fn push_back(&mut self, chunk: ByteChunk) {
        assert!(
            self.values.len() < self.values.capacity(),
            "missing FIFO admission"
        );
        self.values.push_back(chunk);
    }
    pub(super) fn push_front(&mut self, chunk: ByteChunk) {
        assert!(
            self.values.len() < self.values.capacity(),
            "missing FIFO remainder slot"
        );
        self.values.push_front(chunk);
    }
    pub(super) fn pop_front(&mut self) -> Option<ByteChunk> {
        self.values.pop_front()
    }
    pub(super) fn clear(&mut self) {
        self.values.clear();
    }
    pub(super) fn next_len(&self, limit: usize, queued: usize) -> usize {
        if self.meter.is_some() {
            self.front().map_or(0, |c| c.bytes.len().min(limit))
        } else {
            limit.min(queued)
        }
    }
    pub(super) fn pop_native(&mut self, limit: usize) -> Option<(super::CustodiedBytes, usize)> {
        if self.meter.is_none() || limit == 0 {
            return None;
        }
        let mut chunk = self.pop_front()?;
        let n = limit.min(chunk.bytes.len());
        let capacity = chunk.allocation
            .or_else(|| chunk.bytes.allocation_identity().map(|id| id.capacity)).unwrap_or(0);
        if n == chunk.bytes.len() {
            // Move the final actual view, never Drop an empty loan in FIFO lock.
            return Some((chunk.bytes, capacity));
        }
        let bytes = chunk.bytes.split_to_queued(n);
        self.push_front(chunk);
        Some((bytes, capacity))
    }
}
