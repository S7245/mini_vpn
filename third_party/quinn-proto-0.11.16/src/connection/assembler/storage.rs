//! Original Assembler descriptor heap. Small heaps retain the original contiguous
//! layout; large heaps own bounded descriptor pages and their charged directory.
//! Growth prepares every real layout before install; no queue, meter or refund
//! authority is added. Clear retains inventory; Drop releases layouts before
//! their allowances. Tests: storage/tests and assembler/tests/inventory.rs.
use super::{Buffer, InsertError};
use bytes::{BackingBudgetReservation, BackingMeter, MeteredCopyError};
use std::{alloc::{alloc, Layout}, collections::BinaryHeap, mem, cmp::Reverse};
use super::super::storage::Slab;

// A descriptor-block bound, not an assumed operating-system page size. Dense
// power-of-two indexing keeps sift operations local without division by a
// variable or a linked-page walk. Layout changes recalculate the geometry.
const BLOCK_BYTES: usize = 4096;
const PAGE_RECORDS: usize = 1usize << (usize::BITS - 1
    - (BLOCK_BYTES / mem::size_of::<Buffer>()).leading_zeros());

#[derive(Debug)]
pub(super) enum Heap {
    Small { heap: BinaryHeap<Buffer>, allowance: Option<BackingBudgetReservation> },
    Large { body: Box<Paged>, allowance: Option<BackingBudgetReservation> },
}
impl Default for Heap {
    fn default() -> Self { Self::Small { heap: BinaryHeap::new(), allowance: None } }
}
#[derive(Debug)]
pub(super) struct Paged {
    pages: Slab<Slab<Buffer>>,
    len: usize,
    funded: usize,
    meter: BackingMeter,
}
pub(super) enum Growth {
    Replace(Heap),
    Pages { directory: Option<Slab<Slab<Buffer>>>, prepared: Slab<Slab<Buffer>> },
}
impl Heap {
    pub(super) fn has_native_layout(&self) -> bool {
        match self { Self::Small { allowance, .. } => allowance.is_some(), Self::Large { .. } => true }
    }
    pub(super) fn quarantine_layout_owner(&self) {
        if let Self::Large { body, .. } = self { body.meter.quarantine(); }
    }
    pub(super) fn layout_matches(&self, meter: &BackingMeter) -> bool {
        match self {
            Self::Small { heap, allowance } =>
                Layout::array::<Buffer>(heap.capacity()).is_ok_and(|layout|
                    meter.cold_layout_matches(allowance.as_ref(), layout.size()))
                    && (meter.budget_limit().is_none() || heap.capacity() <= PAGE_RECORDS),
            Self::Large { body, allowance } =>
                meter.cold_layout_matches(allowance.as_ref(), Layout::new::<Paged>().size())
                    && body.layout_matches(meter),
        }
    }
    pub(super) fn prepare(capacity: usize, meter: Option<&BackingMeter>) -> Result<Self, InsertError> {
        if capacity <= PAGE_RECORDS || !meter.is_some_and(|m| m.budget_limit().is_some()) {
            let slab = Slab::prepare(capacity, meter)?;
            return Ok(Self::Small { heap: BinaryHeap::from(slab.values), allowance: slab.allowance });
        }
        let count = capacity.div_ceil(PAGE_RECORDS);
        count.checked_mul(PAGE_RECORDS).ok_or(MeteredCopyError::TooLarge)?;
        let owner = meter.cloned().ok_or(MeteredCopyError::Quarantined)?;
        let pages = prepare_pages(count, meter)?;
        let layout = Layout::new::<Paged>();
        let allowance = meter.map(|m| m.reserve_cold_layout(layout.size())).transpose()?.flatten();
        let pointer = unsafe { alloc(layout).cast::<Paged>() };
        if pointer.is_null() { return Err(MeteredCopyError::Allocation.into()); }
        // All fallible allocations precede initialization. The Box, its page
        // directory and each page own exactly the layouts funded above.
        unsafe { pointer.write(Paged { pages, len: 0, funded: count, meter: owner }); }
        Ok(Self::Large { body: unsafe { Box::from_raw(pointer) }, allowance })
    }
    pub(super) fn growth(&self, additional: usize, meter: Option<&BackingMeter>) -> Result<Option<Growth>, InsertError> {
        if !self.qualify() { return Err(MeteredCopyError::Quarantined.into()); }
        let needed = self.len().checked_add(additional).ok_or(MeteredCopyError::TooLarge)?;
        if let Self::Large { body, .. } = self { body.prepare_pushes(needed)?; }
        if needed <= self.capacity() { return Ok(None); }
        match self {
            Self::Small { heap, .. } => {
                let mut target = needed.max(heap.capacity().saturating_mul(2)).max(4);
                if needed <= PAGE_RECORDS && meter.is_some_and(|m| m.budget_limit().is_some()) {
                    target = target.min(PAGE_RECORDS);
                }
                Self::prepare(target, meter).map(|heap| Some(Growth::Replace(heap)))
            },
            Self::Large { body, .. } => {
                let count = needed.div_ceil(PAGE_RECORDS);
                count.checked_mul(PAGE_RECORDS).ok_or(MeteredCopyError::TooLarge)?;
                let extra = count - body.pages.values.len();
                let directory = body.pages.growth(extra, meter)?;
                let prepared = prepare_pages(extra, meter)?;
                Ok(Some(Growth::Pages { directory, prepared }))
            }
        }
    }
    pub(super) fn install(&mut self, growth: Option<Growth>) {
        match growth {
            None => {},
            Some(Growth::Replace(mut replacement)) => {
                // Promotion moves only the bounded old small heap. Its real
                // array remains charged until every original Buffer has moved.
                if let Self::Small { heap, .. } = self {
                    while let Some(value) = heap.pop() { replacement.push(value); }
                    *self = replacement;
                } else { unreachable!("replacement preparation requires original small heap"); }
            }
            Some(Growth::Pages { directory, mut prepared }) => {
                if let Self::Large { body, .. } = self {
                    body.pages.install(directory);
                    body.pages.values.append(&mut prepared.values);
                    body.funded = body.pages.values.len();
                } else {
                    // The only caller holds exclusive Assembler custody from
                    // growth through install; no representation can change.
                    unreachable!("paged preparation on a small descriptor heap");
                }
            }
        }
    }
    pub(super) fn qualify(&self) -> bool {
        match self { Self::Small { .. } => true, Self::Large { body, .. } => body.qualify() }
    }
    pub(super) fn len(&self) -> usize {
        match self { Self::Small { heap, .. } => heap.len(), Self::Large { body, .. } => body.len }
    }
    pub(super) fn capacity(&self) -> usize {
        match self { Self::Small { heap, .. } => heap.capacity(), Self::Large { body, .. } => body.pages.values.len() * PAGE_RECORDS }
    }
    pub(super) fn is_empty(&self) -> bool { self.len() == 0 }
    pub(super) fn push(&mut self, value: Buffer) {
        debug_assert!(self.len() < self.capacity(), "missing descriptor admission");
        match self {
            Self::Small { heap, .. } => heap.push(value),
            Self::Large { body, .. } => body.push(value),
        }
    }
    pub(super) fn peek(&self) -> Option<&Buffer> {
        match self {
            Self::Small { heap, .. } => heap.peek(),
            Self::Large { body, .. } => body.pages.values.first()?.values.first(),
        }
    }
    pub(super) fn pop_checked(&mut self) -> Result<Option<Buffer>, MeteredCopyError> {
        match self { Self::Small { heap, .. } => Ok(heap.pop()), Self::Large { body, .. } => body.pop_checked() }
    }
    pub(super) fn read_head(&mut self, skip: usize, max: usize) -> Result<Option<Taken>, MeteredCopyError> {
        match self {
            Self::Small { heap, .. } => {
                let Some(mut head) = heap.peek_mut() else { return Ok(None); };
                let remain = head.bytes.len().checked_sub(skip).ok_or(MeteredCopyError::Quarantined)?;
                let take = remain.min(max);
                let offset = head.offset.checked_add(skip as u64).ok_or(MeteredCopyError::Quarantined)?;
                let next = offset.checked_add(take as u64).ok_or(MeteredCopyError::Quarantined)?;
                if take == remain {
                    let mut original = std::collections::binary_heap::PeekMut::pop(head);
                    bytes::Buf::advance(&mut original.bytes, skip);
                    Ok(Some(Taken { offset, bytes: original.bytes, discarded: skip, allocation_removed: original.allocation_size }))
                } else {
                    bytes::Buf::advance(&mut head.bytes, skip);
                    let bytes = head.bytes.split_to(take);
                    head.offset = next;
                    Ok(Some(Taken { offset, bytes, discarded: skip, allocation_removed: 0 }))
                }
            }
            Self::Large { body, .. } => body.read_head(skip, max),
        }
    }
    pub(super) fn iter(&self) -> impl Iterator<Item = &Buffer> {
        let (small, large) = match self {
            Self::Small { heap, .. } => (Some(heap.iter()), None),
            Self::Large { body, .. } => (None, Some(body.pages.values.iter())),
        };
        small.into_iter().flatten().chain(large.into_iter().flatten().flat_map(|page| page.values.iter()))
    }
    pub(super) fn clear(&mut self) {
        match self {
            Self::Small { heap, .. } => heap.clear(),
            Self::Large { body, .. } => {
                for page in &mut body.pages.values { page.values.clear(); }
                body.len = 0;
            }
        }
    }
}
fn prepare_pages(count: usize, meter: Option<&BackingMeter>) -> Result<Slab<Slab<Buffer>>, InsertError> {
    let mut pages = Slab::prepare(count, meter)?;
    for _ in 0..count { pages.values.push(Slab::prepare(PAGE_RECORDS, meter)?); }
    Ok(pages)
}
impl Paged {
    // Only local frontier checks on DATA. Complete page inventory is checked
    // separately at cold/quiescent boundaries; no packet-wide audit is added.
    fn qualify(&self) -> bool {
        let valid = self.funded >= 2 && self.funded == self.pages.values.len()
            && self.funded.checked_mul(PAGE_RECORDS).is_some_and(|capacity| self.len <= capacity)
            && self.pages.values.first().is_some_and(|page| page.values.len() == self.len.min(PAGE_RECORDS))
            && (self.len == 0 || self.pages.values.get((self.len - 1) / PAGE_RECORDS)
                .is_some_and(|page| page.values.len() == (self.len - 1) % PAGE_RECORDS + 1));
        if !valid { self.meter.quarantine(); }
        valid
    }
    fn at(&self, index: usize) -> &Buffer {
        &self.pages.values[index / PAGE_RECORDS].values[index % PAGE_RECORDS]
    }
    fn swap(&mut self, first: usize, second: usize) {
        let (low, high) = if first <= second { (first, second) } else { (second, first) };
        let (a, b) = (low / PAGE_RECORDS, high / PAGE_RECORDS);
        if a == b { self.pages.values[a].values.swap(low % PAGE_RECORDS, high % PAGE_RECORDS); }
        else {
            let (left, right) = self.pages.values.split_at_mut(b);
            mem::swap(&mut left[a].values[low % PAGE_RECORDS], &mut right[0].values[high % PAGE_RECORDS]);
        }
    }
    fn push(&mut self, value: Buffer) {
        let mut index = self.len;
        self.pages.values[index / PAGE_RECORDS].values.push(value);
        self.len += 1;
        while index != 0 {
            let parent = (index - 1) / 2;
            if self.at(parent) >= self.at(index) { break; }
            self.swap(parent, index);
            index = parent;
        }
    }
    fn fault(&self) -> MeteredCopyError { self.meter.quarantine(); MeteredCopyError::Quarantined }
    fn page_checked(&self, index: usize) -> Result<&Slab<Buffer>, MeteredCopyError> {
        let page = self.pages.values.get(index).ok_or_else(|| self.fault())?;
        let expected = self.len.saturating_sub(index * PAGE_RECORDS).min(PAGE_RECORDS);
        if page.values.capacity() != PAGE_RECORDS || page.values.len() != expected
            || !page.layout_matches(&self.meter) { return Err(self.fault()); }
        Ok(page)
    }
    fn at_checked(&self, index: usize) -> Result<&Buffer, MeteredCopyError> {
        if index >= self.len { return Err(self.fault()); }
        self.page_checked(index / PAGE_RECORDS)?.values.get(index % PAGE_RECORDS).ok_or_else(|| self.fault())
    }
    fn prepare_pushes(&self, needed: usize) -> Result<(), MeteredCopyError> {
        for index in self.len..needed {
            if index / PAGE_RECORDS < self.funded { self.page_checked(index / PAGE_RECORDS)?; }
            let mut ancestor = index;
            while ancestor != 0 {
                ancestor = (ancestor - 1) / 2;
                if ancestor < self.len { self.at_checked(ancestor)?; }
            }
        }
        Ok(())
    }
    fn plan_down(&self, key: (Reverse<u64>, usize), effective_len: usize) -> Result<Sift, MeteredCopyError> {
        let mut plan = Sift { path: [0; usize::BITS as usize], count: 0 };
        let mut index = 0;
        while index < effective_len / 2 {
            let left = index * 2 + 1;
            let right = left + 1;
            let left_value = self.at_checked(left)?;
            let next = if right < effective_len && self.at_checked(right)? > left_value { right } else { left };
            let value = self.at_checked(next)?;
            if key >= (Reverse(value.offset), value.bytes.len()) { break; }
            *plan.path.get_mut(plan.count).ok_or_else(|| self.fault())? = next;
            plan.count += 1;
            index = next;
        }
        Ok(plan)
    }
    fn apply_down(&mut self, plan: Sift) {
        let mut parent = 0;
        for &next in &plan.path[..plan.count] { self.swap(parent, next); parent = next; }
    }
    fn pop_checked(&mut self) -> Result<Option<Buffer>, MeteredCopyError> {
        if !self.qualify() { return Err(self.fault()); }
        if self.len == 0 { return Ok(None); }
        self.at_checked(0)?;
        let last_index = self.len - 1;
        let last = self.at_checked(last_index)?;
        let plan = self.plan_down((Reverse(last.offset), last.bytes.len()), last_index)?;
        // No budget check, allocation, user callback or fallible operation
        // remains after this exclusive, fully qualified move begins.
        let last = self.pages.values[last_index / PAGE_RECORDS].values.pop().ok_or_else(|| self.fault())?;
        self.len = last_index;
        if last_index == 0 { return Ok(Some(last)); }
        let first = mem::replace(&mut self.pages.values[0].values[0], last);
        self.apply_down(plan);
        Ok(Some(first))
    }
    fn read_head(&mut self, skip: usize, max: usize) -> Result<Option<Taken>, MeteredCopyError> {
        if !self.qualify() { return Err(self.fault()); }
        if self.len == 0 { return Ok(None); }
        let original = self.at_checked(0)?;
        let remain = original.bytes.len().checked_sub(skip).ok_or_else(|| self.fault())?;
        let take = remain.min(max);
        let offset = original.offset.checked_add(skip as u64).ok_or_else(|| self.fault())?;
        let next = offset.checked_add(take as u64).ok_or_else(|| self.fault())?;
        if take == remain {
            let mut actual = self.pop_checked()?.ok_or_else(|| self.fault())?;
            bytes::Buf::advance(&mut actual.bytes, skip);
            Ok(Some(Taken { offset, bytes: actual.bytes, discarded: skip, allocation_removed: actual.allocation_size }))
        } else {
            let plan = self.plan_down((Reverse(next), remain - take), self.len)?;
            let original = &mut self.pages.values[0].values[0];
            bytes::Buf::advance(&mut original.bytes, skip);
            let bytes = original.bytes.split_to(take);
            original.offset = next;
            self.apply_down(plan);
            Ok(Some(Taken { offset, bytes, discarded: skip, allocation_removed: 0 }))
        }
    }
    fn layout_matches(&self, meter: &BackingMeter) -> bool {
        if !self.meter.same_authority(meter) || self.funded < 2
            || self.funded != self.pages.values.len() || !self.pages.layout_matches(meter) { return false; }
        let Some(capacity) = self.pages.values.len().checked_mul(PAGE_RECORDS) else { return false; };
        if self.len > capacity { return false; }
        let mut remaining = self.len;
        for page in &self.pages.values {
            if !page.layout_matches(meter) || page.values.capacity() != PAGE_RECORDS
                || page.values.len() != remaining.min(PAGE_RECORDS) { return false; }
            remaining = remaining.saturating_sub(PAGE_RECORDS);
        }
        remaining == 0 && (1..self.len).all(|index| self.at((index - 1) / 2) >= self.at(index))
    }
}

struct Sift { path: [usize; usize::BITS as usize], count: usize }
pub(super) struct Taken {
    pub(super) offset: u64,
    pub(super) bytes: bytes::Bytes,
    pub(super) discarded: usize,
    pub(super) allocation_removed: usize,
}

/// There are no fallible operations inside an admitted mutation. An unwinding
/// transaction is nevertheless unknown, and must never become a clean account.
pub(super) struct Mutation(Option<BackingMeter>);
impl Mutation {
    pub(super) fn begin(meter: Option<&BackingMeter>) -> Result<Self, InsertError> {
        if let Some(m) = meter {
            m.admit_precharged()?;
        }
        Ok(Self(meter.cloned()))
    }
    pub(super) fn commit(mut self) {
        self.0 = None;
    }
}
impl Drop for Mutation {
    fn drop(&mut self) {
        if let Some(m) = &self.0 {
            m.quarantine();
        }
    }
}

#[cfg(test)]
mod tests;
