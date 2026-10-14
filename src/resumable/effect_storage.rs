//! Original effect-container backing, without protocol or lifecycle authority.
//!
//! Owns checked allocation/growth and the first overflowing effect. Callers
//! prepare before mutation and propagate the first append error immediately.
//! No allocator replacement, pointee accounting or whole-root-zero claim.
//! Entry: exact_vec, prepare_vec/deque, SessionEffectBatch; tests live in tests/.

use super::SessionEffect;
use std::alloc::{Layout, alloc};
use std::collections::VecDeque;

#[cfg(test)]
mod tests;
#[cfg(test)]
std::thread_local! {
    static FAIL_NEXT_ALLOCATION: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// One same-thread allocation refusal for a real owning integration path.
#[cfg(test)]
pub(crate) fn fail_next_effect_allocation_for_test() {
    assert!(!FAIL_NEXT_ALLOCATION.replace(true));
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum EffectStorageError {
    Layout,
    Reserve,
    Bound,
    Overflow,
}

impl std::fmt::Display for EffectStorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "effect storage {self:?}")
    }
}
impl std::error::Error for EffectStorageError {}

/// Exact requested backing; allocator-internal metadata/RSS is not represented.
pub(crate) fn exact_vec<T>(capacity: usize) -> Result<Vec<T>, EffectStorageError> {
    let layout = Layout::array::<T>(capacity).map_err(|_| EffectStorageError::Layout)?;
    if layout.size() == 0 {
        // ZST Vec capacity is usize::MAX, but its physical allocation is zero.
        return Ok(Vec::new());
    }
    #[cfg(test)]
    if FAIL_NEXT_ALLOCATION.replace(false) {
        return Err(EffectStorageError::Reserve);
    }
    allocate_exact_vec(capacity)
}

/// Shared backing primitive; caller fault domains are deliberately separate.
/// Rechecks Layout at this entry; no effect-specific allocation hook is consumed.
pub(crate) fn allocate_exact_vec<T>(capacity: usize) -> Result<Vec<T>, EffectStorageError> {
    let layout = Layout::array::<T>(capacity).map_err(|_| EffectStorageError::Layout)?;
    if layout.size() == 0 {
        return Ok(Vec::new());
    }
    // SAFETY: layout is nonzero, aligned for T, and checked against isize::MAX.
    // The global allocator matches Vec's allocator. No initialized element is
    // claimed. A non-null allocation is transferred exactly once, immediately,
    // with its exact capacity; Vec subsequently owns initialization and Drop.
    unsafe {
        let pointer = alloc(layout).cast::<T>();
        if pointer.is_null() {
            return Err(EffectStorageError::Reserve);
        }
        Ok(Vec::from_raw_parts(pointer, 0, capacity))
    }
}

pub(crate) fn next_capacity(
    current: usize,
    need: usize,
    first: usize,
    maximum: usize,
) -> Result<usize, EffectStorageError> {
    if first == 0 || maximum < first || need > maximum || current > maximum {
        return Err(EffectStorageError::Bound);
    }
    if need <= current {
        return Ok(current);
    }
    let mut next = first;
    while next < need {
        next = if next > maximum - next {
            maximum
        } else {
            next * 2
        };
    }
    Ok(next)
}

/// Checks both actual simultaneous allocations before moving a single item.
pub(crate) fn growth_peak<T>(old: usize, new: usize) -> Result<usize, EffectStorageError> {
    let old = Layout::array::<T>(old).map_err(|_| EffectStorageError::Layout)?;
    let new = Layout::array::<T>(new).map_err(|_| EffectStorageError::Layout)?;
    old.size()
        .checked_add(new.size())
        .ok_or(EffectStorageError::Layout)
}

pub(crate) fn prepare_vec<T>(
    values: &mut Vec<T>,
    need: usize,
    first: usize,
    maximum: usize,
) -> Result<(), EffectStorageError> {
    if values.len() > need || first == 0 || maximum < first {
        return Err(EffectStorageError::Bound);
    }
    if size_of::<T>() == 0 {
        return (need <= maximum)
            .then_some(())
            .ok_or(EffectStorageError::Bound);
    }
    let next = next_capacity(values.capacity(), need, first, maximum)?;
    if next == values.capacity() {
        return Ok(());
    }
    growth_peak::<T>(values.capacity(), next)?;
    let mut prepared = exact_vec(next)?;
    // next >= need >= live length at every admitted call. No callback, Clone,
    // element Drop or fallible reserve occurs in this single-owning transfer.
    for value in values.drain(..) {
        prepared.push(value);
    }
    *values = prepared;
    Ok(())
}

pub(crate) fn prepare_deque<T>(
    values: &mut VecDeque<T>,
    need: usize,
    first: usize,
    maximum: usize,
) -> Result<(), EffectStorageError> {
    if values.len() > need || first == 0 || maximum < first {
        return Err(EffectStorageError::Bound);
    }
    if size_of::<T>() == 0 {
        return (need <= maximum)
            .then_some(())
            .ok_or(EffectStorageError::Bound);
    }
    let next = next_capacity(values.capacity(), need, first, maximum)?;
    if next == values.capacity() {
        return Ok(());
    }
    growth_peak::<T>(values.capacity(), next)?;
    let mut prepared = exact_vec(next)?;
    while let Some(value) = values.pop_front() {
        prepared.push(value);
    }
    // Same T: Vec -> VecDeque moves the allocation, not elements into a second backing.
    *values = prepared.into();
    Ok(())
}

/// Writable original producer journal. An overflow stays with its full prefix.
/// A failed journal is never returned to an idle pool or appended to again.
#[derive(Debug)]
pub(crate) struct SessionEffectBatch {
    values: Vec<SessionEffect>,
    overflow: Option<SessionEffect>,
    mutation_started: bool,
}

impl SessionEffectBatch {
    pub(crate) const fn empty() -> Self {
        Self {
            values: Vec::new(),
            overflow: None,
            mutation_started: false,
        }
    }

    pub(crate) fn prepared(capacity: usize) -> Result<Self, EffectStorageError> {
        Ok(Self {
            values: exact_vec(capacity)?,
            overflow: None,
            mutation_started: false,
        })
    }

    pub(crate) fn prepare_ordinary(&mut self) -> Result<(), EffectStorageError> {
        if self.overflow.is_some() {
            return Err(EffectStorageError::Overflow);
        }
        if self.values.capacity() == 0 {
            self.values = exact_vec(3)?;
        }
        Ok(())
    }

    pub(crate) fn mark_mutation(&mut self) {
        self.mutation_started = true;
    }
    pub(crate) fn mutation_started(&self) -> bool {
        self.mutation_started
    }
    pub(crate) fn rearm_after_transfer(&mut self) -> Result<(), EffectStorageError> {
        if !self.is_empty() {
            return Err(EffectStorageError::Overflow);
        }
        self.mutation_started = false;
        Ok(())
    }
    pub(crate) fn install_prepared(&mut self, values: Vec<SessionEffect>) {
        self.values = values;
    }

    pub(crate) fn push(&mut self, value: SessionEffect) -> Result<(), EffectStorageError> {
        if self.values.len() < self.values.capacity() && self.overflow.is_none() {
            self.values.push(value);
            Ok(())
        } else {
            // Every reducer append propagates this first error with `?`; the
            // public wrapper or original Supervisor then quarantines the batch.
            // Never replace the first retained effect even on a caller defect.
            self.overflow.get_or_insert(value);
            Err(EffectStorageError::Overflow)
        }
    }

    #[cfg(test)]
    pub(crate) fn as_ptr(&self) -> *const SessionEffect {
        self.values.as_ptr()
    }
    pub(crate) fn capacity(&self) -> usize {
        self.values.capacity()
    }
    pub(crate) fn len(&self) -> usize {
        self.values.len() + usize::from(self.overflow.is_some())
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.values.is_empty() && self.overflow.is_none()
    }
    pub(crate) fn has_overflow(&self) -> bool {
        self.overflow.is_some()
    }
    pub(crate) fn iter(&self) -> impl Iterator<Item = &SessionEffect> {
        self.values.iter().chain(self.overflow.iter())
    }
    pub(crate) fn get(&self, index: usize) -> Option<&SessionEffect> {
        self.values.get(index)
    }
    pub(crate) fn remove(&mut self, index: usize) -> SessionEffect {
        self.values.remove(index)
    }
    pub(crate) fn retain_mut(&mut self, keep: impl FnMut(&mut SessionEffect) -> bool) {
        self.values.retain_mut(keep);
    }
    pub(crate) fn drain(&mut self) -> std::vec::Drain<'_, SessionEffect> {
        self.values.drain(..)
    }
    pub(crate) fn into_parts(self) -> (Vec<SessionEffect>, Option<SessionEffect>) {
        (self.values, self.overflow)
    }
    pub(crate) fn from_vec(values: Vec<SessionEffect>) -> Self {
        Self {
            values,
            overflow: None,
            mutation_started: true,
        }
    }
}
