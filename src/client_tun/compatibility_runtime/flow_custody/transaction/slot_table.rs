//! Root slot table with an occupied-slot index (D61/D62 R-a).
//!
//! Owns: the fixed row storage of `OpenCustody` and one occupancy bit per row,
//! kept equal to `row.is_some()` by construction — every occupancy change goes
//! through `put`/`take` under the existing root lock; reads deref to the row
//! slice and in-place row edits use `get_mut`/`occupied_mut`, which cannot
//! change occupancy. Does not own: transaction semantics, accounting, cursors.
//! Invariant: `bits[i] == rows[i].is_some()` for every row, rows ≤ `CAPACITY`;
//! `occupancy_consistent` re-proves it in both directions on the cold shutdown
//! census only. Entry symbols: `with_len`, `put`, `take`, `next_occupied`.
//! The fixed index is `WORDS * 8` bytes of cold root layout. Tests:
//! `transaction/slot_table/tests/mod.rs`.
use super::{FLOW_SLOT_LIMIT, OpenSlot};

const WORDS: usize = FLOW_SLOT_LIMIT.div_ceil(u64::BITS as usize);
/// Largest row count the fixed index covers.
pub(in crate::client_tun::compatibility_runtime) const CAPACITY: usize =
    WORDS * u64::BITS as usize;
/// Fixed Root layout of the occupancy index (§5.3 `I_cfg`, D66).
pub(in crate::client_tun::compatibility_runtime) const INDEX_BYTES: usize =
    std::mem::size_of::<[u64; WORDS]>();

pub(in crate::client_tun::compatibility_runtime) struct SlotTable {
    rows: Vec<Option<OpenSlot>>,
    bits: [u64; WORDS],
}

impl Default for SlotTable {
    fn default() -> Self {
        Self { rows: Vec::new(), bits: [0; WORDS] }
    }
}

impl std::ops::Deref for SlotTable {
    type Target = [Option<OpenSlot>];

    fn deref(&self) -> &Self::Target {
        &self.rows
    }
}

const fn bit(index: usize) -> (usize, u64) {
    (index / u64::BITS as usize, 1u64 << (index % u64::BITS as usize))
}

impl SlotTable {
    /// Empty table of `len` rows; `len` above `CAPACITY` is clamped (callers
    /// pass the fixed W or F + W geometry, both within it).
    pub(in crate::client_tun::compatibility_runtime) fn with_len(len: usize) -> Self {
        Self {
            rows: std::iter::repeat_with(|| None).take(len.min(CAPACITY)).collect(),
            bits: [0; WORDS],
        }
    }

    /// Occupy `index`, returning any previous row. Out of range drops nothing:
    /// the row is handed back unchanged.
    pub(in crate::client_tun::compatibility_runtime) fn put(
        &mut self,
        index: usize,
        slot: OpenSlot,
    ) -> Result<Option<OpenSlot>, OpenSlot> {
        let Some(row) = self.rows.get_mut(index).filter(|_| index < CAPACITY) else {
            return Err(slot);
        };
        let (word, mask) = bit(index);
        self.bits[word] |= mask;
        Ok(row.replace(slot))
    }

    pub(in crate::client_tun::compatibility_runtime) fn take(
        &mut self,
        index: usize,
    ) -> Option<OpenSlot> {
        let row = self.rows.get_mut(index)?.take();
        let (word, mask) = bit(index);
        self.bits[word] &= !mask;
        row
    }

    /// In-place edit of an occupied row; occupancy cannot change through it.
    pub(in crate::client_tun::compatibility_runtime) fn get_mut(
        &mut self,
        index: usize,
    ) -> Option<&mut OpenSlot> {
        self.rows.get_mut(index)?.as_mut()
    }

    pub(in crate::client_tun::compatibility_runtime) fn occupied_mut(
        &mut self,
    ) -> impl Iterator<Item = (usize, &mut OpenSlot)> {
        self.rows
            .iter_mut()
            .enumerate()
            .filter_map(|(index, row)| row.as_mut().map(|slot| (index, slot)))
    }

    /// First occupied row at or after `from`, scanning index words only.
    pub(in crate::client_tun::compatibility_runtime) fn next_occupied(
        &self,
        from: usize,
    ) -> Option<usize> {
        if from >= self.rows.len() {
            return None;
        }
        let (mut word, _) = bit(from);
        let mut current = self.bits[word] & (u64::MAX << (from % u64::BITS as usize));
        loop {
            if current != 0 {
                let index = word * u64::BITS as usize + current.trailing_zeros() as usize;
                return (index < self.rows.len()).then_some(index);
            }
            word += 1;
            current = *self.bits.get(word)?;
        }
    }

    /// Occupied rows in index order through the index alone.
    pub(in crate::client_tun::compatibility_runtime) fn occupied(
        &self,
    ) -> impl Iterator<Item = &OpenSlot> {
        let mut next = self.next_occupied(0);
        std::iter::from_fn(move || {
            let index = next?;
            next = self.next_occupied(index + 1);
            self.rows[index].as_ref()
        })
    }

    /// Cold bidirectional census: every set bit names an occupied row and
    /// every occupied row has its bit; no bit beyond the rows.
    pub(in crate::client_tun::compatibility_runtime) fn occupancy_consistent(&self) -> bool {
        self.rows.len() <= CAPACITY
            && (0..CAPACITY).all(|index| {
                let (word, mask) = bit(index);
                (self.bits[word] & mask != 0)
                    == self.rows.get(index).is_some_and(Option::is_some)
            })
    }

    #[cfg(test)]
    pub(in crate::client_tun::compatibility_runtime) fn capacity(&self) -> usize {
        self.rows.capacity()
    }

    /// Geometry faults for the census tracers: the row Vec changes length,
    /// the index keeps tracking (a popped row's bit is cleared).
    #[cfg(test)]
    pub(in crate::client_tun::compatibility_runtime) fn pop_row_for_test(
        &mut self,
    ) -> Option<Option<OpenSlot>> {
        let row = self.rows.pop()?;
        if let Some((word, mask)) = Some(bit(self.rows.len())).filter(|(word, _)| *word < WORDS) {
            self.bits[word] &= !mask;
        }
        Some(row)
    }

    #[cfg(test)]
    pub(in crate::client_tun::compatibility_runtime) fn push_empty_row_for_test(&mut self) {
        self.rows.push(None);
    }

    #[cfg(test)]
    pub(in crate::client_tun::compatibility_runtime) fn corrupt_bit_for_test(&mut self, index: usize) {
        let (word, mask) = bit(index);
        self.bits[word] ^= mask;
    }
}

#[cfg(test)]
mod tests;
