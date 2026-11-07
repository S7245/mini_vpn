//! Physical pacing inventory owned by the original service mutex.
//! Native records, hash index and ordered wait queues live inside one precharged
//! State allocation. Detached positive leases occupy the same record inventory.
//! Legacy retains its map/deque semantics. No per-record allocation or new lock.
//! Entry: StateStorage/Records/WaitQueue; tests: endpoint_pacing/tests/inventory.
use super::*;
use bytes::{BackingBudgetReservation, BackingMeter};
use std::{
    alloc::{alloc, Layout},
    hash::{Hash, Hasher},
    io,
    ops::{Deref, DerefMut},
};

pub(super) const CAPACITY: usize = crate::BUDGETED_CONNECTION_LIMIT;
const BUCKETS: usize = 64;
type Key = EndpointPacingConnectionKey;
type Record = (Key, ConnectionAccounting);

#[derive(Debug)]
pub(super) struct StateStorage {
    state: Box<EndpointPacingState>,
    // Physical state is destroyed before returning the original layout credit.
    _reservation: Option<BackingBudgetReservation>,
}
impl StateStorage {
    pub(super) const fn layout_bytes() -> usize {
        Layout::new::<EndpointPacingState>().size()
    }
    pub(super) fn new(
        state: EndpointPacingState,
        meter: Option<&BackingMeter>,
    ) -> io::Result<Self> {
        if meter.is_none() {
            return Ok(Self {
                state: Box::new(state),
                _reservation: None,
            });
        }
        let layout = Layout::new::<EndpointPacingState>();
        let reservation = meter
            .map(|m| m.reserve_cold_layout(layout.size()))
            .transpose()
            .map_err(|e| io::Error::other(format!("pacing inventory admission {e:?}")))?
            .flatten();
        let raw = unsafe { alloc(layout) }.cast::<EndpointPacingState>();
        if raw.is_null() {
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "pacing inventory allocation",
            ));
        }
        // SAFETY: exact State layout, initialized once, uniquely owned by Box.
        unsafe {
            raw.write(state);
        }
        Ok(Self {
            state: unsafe { Box::from_raw(raw) },
            _reservation: reservation,
        })
    }
}
impl Deref for StateStorage {
    type Target = EndpointPacingState;
    fn deref(&self) -> &Self::Target {
        &self.state
    }
}
impl DerefMut for StateStorage {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.state
    }
}

#[derive(Debug)]
pub(super) struct FixedRecords {
    records: [Option<Record>; CAPACITY],
    buckets: [Option<usize>; BUCKETS],
    len: usize,
}
impl FixedRecords {
    fn home(key: &Key) -> usize {
        let mut hash = rustc_hash::FxHasher::default();
        key.hash(&mut hash);
        (hash.finish() as usize) % BUCKETS
    }
    fn find(&self, key: &Key) -> Option<usize> {
        let home = Self::home(key);
        for step in 0..BUCKETS {
            let record = self.buckets[(home + step) % BUCKETS]?;
            if self.records.get(record)?.as_ref()?.0 == *key {
                return Some(record);
            }
        }
        None
    }
    fn index(&mut self, record: usize, key: &Key) {
        let home = Self::home(key);
        for step in 0..BUCKETS {
            let cell = &mut self.buckets[(home + step) % BUCKETS];
            if cell.is_none() {
                *cell = Some(record);
                return;
            }
        }
    }
    fn insert(&mut self, key: Key, value: ConnectionAccounting) -> bool {
        if self.find(&key).is_some() {
            return false;
        }
        let Some(index) = self.records.iter().position(Option::is_none) else {
            return false;
        };
        self.records[index] = Some((key, value));
        self.index(index, &key);
        self.len += 1;
        true
    }
    fn remove(&mut self, key: &Key) -> Option<ConnectionAccounting> {
        let record = self.find(key)?;
        let (_, value) = self.records[record].take()?;
        self.len -= 1;
        // Lifecycle-only deletion rebuilds a bounded index; ordinary lookup does
        // not scan records and never accumulates tombstones or allocates.
        self.buckets.fill(None);
        for i in 0..CAPACITY {
            if let Some((key, _)) = &self.records[i] {
                let key = *key;
                self.index(i, &key);
            }
        }
        Some(value)
    }
}
#[derive(Debug)]
pub(super) enum Records {
    Legacy(FxHashMap<Key, ConnectionAccounting>),
    Native(FixedRecords),
}
impl Records {
    pub(super) fn new(native: bool) -> Self {
        if native {
            Self::Native(FixedRecords {
                records: std::array::from_fn(|_| None),
                buckets: [None; BUCKETS],
                len: 0,
            })
        } else {
            Self::Legacy(FxHashMap::default())
        }
    }
    pub(super) fn len(&self) -> usize {
        match self {
            Self::Legacy(v) => v.len(),
            Self::Native(v) => v.len,
        }
    }
    pub(super) fn has_capacity(&self) -> bool {
        match self {
            Self::Legacy(_) => true,
            Self::Native(v) => v.len < CAPACITY,
        }
    }
    pub(super) fn contains_key(&self, key: &Key) -> bool {
        self.get(key).is_some()
    }
    pub(super) fn get(&self, key: &Key) -> Option<&ConnectionAccounting> {
        match self {
            Self::Legacy(v) => v.get(key),
            Self::Native(v) => Some(&v.records[v.find(key)?].as_ref()?.1),
        }
    }
    pub(super) fn get_mut(&mut self, key: &Key) -> Option<&mut ConnectionAccounting> {
        match self {
            Self::Legacy(v) => v.get_mut(key),
            Self::Native(v) => {
                let i = v.find(key)?;
                Some(&mut v.records[i].as_mut()?.1)
            }
        }
    }
    pub(super) fn insert(&mut self, key: Key, value: ConnectionAccounting) -> bool {
        match self {
            Self::Legacy(v) => match v.entry(key) {
                std::collections::hash_map::Entry::Vacant(slot) => {
                    slot.insert(value);
                    true
                }
                _ => false,
            },
            Self::Native(v) => v.insert(key, value),
        }
    }
    pub(super) fn remove(&mut self, key: &Key) -> Option<ConnectionAccounting> {
        match self {
            Self::Legacy(v) => v.remove(key),
            Self::Native(v) => v.remove(key),
        }
    }
    pub(super) fn valid(&self) -> bool {
        let Self::Native(f) = self else { return true };
        if f.records.iter().flatten().count() != f.len
            || f.buckets.iter().flatten().count() != f.len
        {
            return false;
        }
        let mut seen = [false; CAPACITY];
        for &index in f.buckets.iter().flatten() {
            if index >= CAPACITY || seen[index] {
                return false;
            }
            seen[index] = true;
            let Some((key, _)) = &f.records[index] else {
                return false;
            };
            if f.find(key) != Some(index) {
                return false;
            }
        }
        f.records
            .iter()
            .enumerate()
            .all(|(i, r)| r.is_some() == seen[i])
    }
}
pub(super) enum RecordIter<'a> {
    Legacy(std::collections::hash_map::Iter<'a, Key, ConnectionAccounting>),
    Native(std::slice::Iter<'a, Option<Record>>),
}
impl<'a> Iterator for RecordIter<'a> {
    type Item = (&'a Key, &'a ConnectionAccounting);
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Legacy(v) => v.next(),
            Self::Native(v) => v.find_map(|r| r.as_ref().map(|(k, v)| (k, v))),
        }
    }
}
impl<'a> IntoIterator for &'a Records {
    type Item = (&'a Key, &'a ConnectionAccounting);
    type IntoIter = RecordIter<'a>;
    fn into_iter(self) -> Self::IntoIter {
        match self {
            Records::Legacy(v) => RecordIter::Legacy(v.iter()),
            Records::Native(v) => RecordIter::Native(v.records.iter()),
        }
    }
}

#[derive(Debug)]
pub(super) enum WaitQueue {
    Legacy(VecDeque<Key>),
    Native {
        slots: [Option<Key>; CAPACITY],
        len: usize,
    },
}
impl WaitQueue {
    pub(super) fn new(native: bool) -> Self {
        if native {
            Self::Native {
                slots: [None; CAPACITY],
                len: 0,
            }
        } else {
            Self::Legacy(VecDeque::new())
        }
    }
    pub(super) fn len(&self) -> usize {
        match self {
            Self::Legacy(v) => v.len(),
            Self::Native { len, .. } => *len,
        }
    }
    pub(super) fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub(super) fn iter(&self) -> impl Iterator<Item = &Key> {
        let (legacy, fixed) = match self {
            Self::Legacy(v) => (Some(v), None),
            Self::Native { slots, .. } => (None, Some(slots)),
        };
        legacy
            .into_iter()
            .flat_map(|v| v.iter())
            .chain(fixed.into_iter().flat_map(|v| v.iter().flatten()))
    }
    pub(super) fn contains(&self, key: &Key) -> bool {
        self.iter().any(|k| k == key)
    }
    pub(super) fn push_back(&mut self, key: Key) -> bool {
        match self {
            Self::Legacy(v) => {
                v.push_back(key);
                true
            }
            Self::Native { slots, len } if *len < CAPACITY => {
                slots[*len] = Some(key);
                *len += 1;
                true
            }
            _ => false,
        }
    }
    pub(super) fn retain(&mut self, mut keep: impl FnMut(&Key) -> bool) {
        match self {
            Self::Legacy(v) => v.retain(keep),
            Self::Native { slots, len } => {
                let mut write = 0;
                for read in 0..*len {
                    if let Some(key) = slots[read].take() {
                        if keep(&key) {
                            slots[write] = Some(key);
                            write += 1;
                        }
                    }
                }
                *len = write;
            }
        }
    }
    pub(super) fn valid(&self) -> bool {
        let Self::Native { slots, len } = self else {
            return true;
        };
        *len <= CAPACITY
            && slots
                .iter()
                .enumerate()
                .all(|(i, k)| k.is_some() == (i < *len))
            && slots[..*len]
                .iter()
                .enumerate()
                .all(|(i, k)| !slots[..i].contains(k))
    }
}
