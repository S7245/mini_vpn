use bytes::{BackingBudgetReservation, BackingMeter};
use std::{
    alloc::{alloc, Layout},
    io,
    ops::Range,
};

use tinyvec::TinyVec;

/// A set of u64 values optimized for long runs and random insert/delete/contains
///
/// `ArrayRangeSet` uses an array representation, where each array entry represents
/// a range.
///
/// The array-based RangeSet provides 2 benefits:
/// - There exists an inline representation, which avoids the need of heap
///   allocating ACK ranges for SentFrames for small ranges.
/// - Iterating over ranges should usually be faster since there is only
///   a single cache-friendly contiguous range.
///
/// `ArrayRangeSet` is especially useful for tracking ACK ranges where the amount
/// of ranges is usually very low (since ACK numbers are in consecutive fashion
/// unless reordering or packet loss occur).
#[derive(Debug, Default)]
pub struct ArrayRangeSet(
    TinyVec<[Range<u64>; ARRAY_RANGE_SET_INLINE_CAPACITY]>,
    Option<ColdRangeOwner>,
    #[cfg(test)] usize,
);

#[derive(Debug)]
struct ColdRangeOwner {
    capacity: usize,
    meter: BackingMeter,
    // The exact Vec layout is destroyed before this last field refunds it.
    _reservation: BackingBudgetReservation,
}
impl Drop for ColdRangeOwner {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.meter.quarantine();
        }
    }
}

/// The capacity of elements directly stored in [`ArrayRangeSet`]
///
/// An inline capacity of 2 is chosen to keep `SentFrame` below 128 bytes.
const ARRAY_RANGE_SET_INLINE_CAPACITY: usize = 2;

#[derive(Debug)]
pub(crate) struct ColdRangeInvariant;

impl ArrayRangeSet {
    /// Only PendingAcks uses cold storage. Its insert-then-trim peak is 65;
    /// prefix removal never splits a range. No Clone can escape this owner.
    pub(crate) fn cold_layout(capacity: usize) -> io::Result<Layout> {
        Layout::array::<Range<u64>>(capacity).map_err(io::Error::other)
    }
    pub(crate) fn cold(meter: &BackingMeter, capacity: usize) -> io::Result<Self> {
        if capacity == 0 {
            return Err(io::Error::other("empty cold ACK range capacity"));
        }
        let layout = Self::cold_layout(capacity)?;
        let reservation = meter
            .reserve_cold_layout(layout.size())
            .map_err(|e| io::Error::other(format!("{e:?}")))?
            .ok_or_else(|| io::Error::other("cold ACK ranges require hard budget"))?;
        let ptr = unsafe { alloc(layout) as *mut Range<u64> };
        if ptr.is_null() {
            return Err(io::Error::other("cold ACK range allocation failed"));
        }
        // Exact requested element capacity; Vec initializes only live ranges.
        let ranges = unsafe { Vec::from_raw_parts(ptr, 0, capacity) };
        let mut result = Self::default();
        result.0 = TinyVec::Heap(ranges);
        result.1 = Some(ColdRangeOwner {
            capacity,
            meter: meter.clone(),
            _reservation: reservation,
        });
        Ok(result)
    }
    fn can_add_range(&self) -> bool {
        if let Some(owner) = &self.1 {
            let valid_layout =
                matches!(&self.0, TinyVec::Heap(ranges) if ranges.capacity() == owner.capacity);
            if !valid_layout || self.0.len() >= owner.capacity {
                owner.meter.quarantine();
                return false;
            }
        }
        true
    }
    fn note_growth(&mut self) {
        #[cfg(test)]
        {
            self.2 = self.2.max(self.0.len());
        }
    }
    pub(crate) fn clear(&mut self) {
        self.0.clear();
    }
    #[cfg(test)]
    pub(crate) fn cold_inventory(&self) -> (Option<usize>, usize) {
        (self.1.as_ref().map(|_| self.0.capacity()), self.2)
    }
    #[cfg(test)]
    pub(crate) fn corrupt_cold_shape_for_recovery_probe(&mut self) -> bool {
        let Some(owner) = &mut self.1 else {
            return false;
        };
        owner.capacity = owner.capacity.saturating_sub(1);
        true
    }
    pub fn new() -> Self {
        Default::default()
    }

    pub fn iter(&self) -> impl DoubleEndedIterator<Item = Range<u64>> + '_ {
        self.0.iter().cloned()
    }

    pub fn elts(&self) -> impl Iterator<Item = u64> + '_ {
        self.iter().flatten()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn contains(&self, x: u64) -> bool {
        for range in self.0.iter() {
            if range.start > x {
                // We only get here if there was no prior range that contained x
                return false;
            } else if range.contains(&x) {
                return true;
            }
        }
        false
    }

    pub fn subtract(&mut self, other: &Self) {
        // TODO: This can potentially be made more efficient, since the we know
        // individual ranges are not overlapping, and the next range must start
        // after the last one finished
        for range in &other.0 {
            self.remove(range.clone());
        }
    }

    fn validate_cold(&self) -> Result<(), ColdRangeInvariant> {
        if let Some(owner) = &self.1 {
            if owner.meter.is_quarantined() {
                return Err(ColdRangeInvariant);
            }
            if !matches!(&self.0, TinyVec::Heap(ranges) if ranges.capacity() == owner.capacity) {
                owner.meter.quarantine();
                return Err(ColdRangeInvariant);
            }
        }
        Ok(())
    }
    /// Cold PendingAcks has one spare physical slot before its bounded trim.
    pub(crate) fn prepare_cold_insert(&self, meter: &BackingMeter) -> Result<(), ColdRangeInvariant> {
        self.validate_cold()?;
        let valid = self.1.as_ref().is_some_and(|owner|
            owner.meter.same_authority(meter) && self.0.len() < owner.capacity);
        if !valid {
            meter.quarantine();
            if let Some(owner) = &self.1 { owner.meter.quarantine(); }
            return Err(ColdRangeInvariant);
        }
        Ok(())
    }

    pub(crate) fn try_insert_one(&mut self, x: u64) -> Result<bool, ColdRangeInvariant> {
        self.validate_cold()?;
        let inserted = self.insert_one(x);
        self.validate_cold()?;
        Ok(inserted)
    }

    pub fn insert_one(&mut self, x: u64) -> bool {
        self.insert(x..x + 1)
    }

    pub fn insert(&mut self, x: Range<u64>) -> bool {
        // Includes in-place range extension: Q is never a duplicate/no-op.
        if self.validate_cold().is_err() {
            return false;
        }
        let mut result = false;

        if x.is_empty() {
            // Don't try to deal with ranges where x.end <= x.start
            return false;
        }

        let mut idx = 0;
        while idx != self.0.len() {
            let range = &mut self.0[idx];

            if range.start > x.end {
                // The range is fully before this range and therefore not extensible.
                // Add a new range to the left
                if !self.can_add_range() {
                    return false;
                }
                self.0.insert(idx, x);
                self.note_growth();
                return true;
            } else if range.start > x.start {
                // The new range starts before this range but overlaps.
                // Extend the current range to the left
                // Note that we don't have to merge a potential left range, since
                // this case would have been captured by merging the right range
                // in the previous loop iteration
                result = true;
                range.start = x.start;
            }

            // At this point we have handled all parts of the new range which
            // are in front of the current range. Now we handle everything from
            // the start of the current range

            if x.end <= range.end {
                // Fully contained
                return result;
            } else if x.start <= range.end {
                // Extend the current range to the end of the new range.
                // Since it's not contained it must be bigger
                range.end = x.end;

                // Merge all follow-up ranges which overlap
                while idx != self.0.len() - 1 {
                    let curr = self.0[idx].clone();
                    let next = self.0[idx + 1].clone();
                    if curr.end >= next.start {
                        self.0[idx].end = next.end.max(curr.end);
                        self.0.remove(idx + 1);
                    } else {
                        break;
                    }
                }

                return true;
            }

            idx += 1;
        }

        // Insert a range at the end
        if !self.can_add_range() {
            return false;
        }
        self.0.push(x);
        self.note_growth();
        true
    }

    pub fn remove(&mut self, x: Range<u64>) -> bool {
        let mut result = false;

        if x.is_empty() {
            // Don't try to deal with ranges where x.end <= x.start
            return false;
        }

        let mut idx = 0;
        while idx != self.0.len() && x.start != x.end {
            let range = self.0[idx].clone();

            if x.end <= range.start {
                // The range is fully before this range
                return result;
            } else if x.start >= range.end {
                // The range is fully after this range
                idx += 1;
                continue;
            }

            // The range overlaps with this range
            result = true;

            let left = range.start..x.start;
            let right = x.end..range.end;
            if left.is_empty() && right.is_empty() {
                self.0.remove(idx);
            } else if left.is_empty() {
                self.0[idx] = right;
                idx += 1;
            } else if right.is_empty() {
                self.0[idx] = left;
                idx += 1;
            } else {
                if !self.can_add_range() {
                    return false;
                }
                self.0[idx] = right;
                self.0.insert(idx, left);
                self.note_growth();
                idx += 2;
            }
        }

        result
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn pop_min(&mut self) -> Option<Range<u64>> {
        if !self.0.is_empty() {
            Some(self.0.remove(0))
        } else {
            None
        }
    }

    pub fn min(&self) -> Option<u64> {
        self.iter().next().map(|x| x.start)
    }

    pub fn max(&self) -> Option<u64> {
        self.iter().next_back().map(|x| x.end - 1)
    }
}

#[cfg(test)]
mod tests;
