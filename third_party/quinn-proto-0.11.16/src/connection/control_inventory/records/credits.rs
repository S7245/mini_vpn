//! MAX_STREAM_DATA custody and bounded deduplication in the Connection Pool.
//! One cold bucket table indexes the same control nodes by queue generation/id.
//! Node.prev is the hash-chain link, Node.next the owning FIFO link. No second
//! metadata allocation or node owner exists. Pending keys are stable per space;
//! packet keys change on descriptor reuse. Chunks prepares before useful reads,
//! transfers at finalize, and packet/loss/ACK move or dispose the same ticket.
//! Tests: records/tests/credits and tests/admission/flow_credit.
use super::*;
use rustc_hash::{FxHashSet, FxHasher};
use std::hash::{Hash, Hasher};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub(in crate::connection) struct CreditKey {
    slot: usize,
    generation: u64,
}
impl CreditKey {
    pub(in crate::connection) fn pending(space: crate::packet::SpaceId) -> Self {
        Self {
            slot: space as usize,
            generation: 0,
        }
    }
    pub(super) fn packet(slot: usize, generation: u64) -> Self {
        Self {
            slot: slot + 3,
            generation,
        }
    }
}
#[derive(Clone, Copy)]
pub(super) struct CreditValue {
    key: CreditKey,
    id: StreamId,
}
impl Item for StreamId {
    fn pack(self) -> Value {
        Value::Credit(CreditValue {
            key: CreditKey::default(),
            id: self,
        })
    }
    fn unpack(value: Value) -> Option<Self> {
        if let Value::Credit(value) = value {
            Some(value.id)
        } else {
            None
        }
    }
}

pub(super) struct Index(Box<[u32]>);
impl Index {
    pub(super) fn layout(capacity: usize) -> io::Result<Layout> {
        let count = capacity
            .checked_mul(2)
            .and_then(usize::checked_next_power_of_two)
            .ok_or_else(|| io::Error::other("credit index capacity overflow"))?;
        Layout::array::<u32>(count).map_err(io::Error::other)
    }
    pub(super) fn prepare(capacity: usize) -> io::Result<Self> {
        let layout = Self::layout(capacity)?;
        let count = layout.size() / std::mem::size_of::<u32>();
        let ptr = unsafe { alloc(layout).cast::<u32>() };
        if ptr.is_null() {
            return Err(io::Error::other("credit index allocation failed"));
        }
        for i in 0..count {
            unsafe {
                ptr.add(i).write(NONE);
            }
        }
        Ok(Self(unsafe {
            Box::from_raw(std::ptr::slice_from_raw_parts_mut(ptr, count))
        }))
    }
    fn bucket(&self, key: CreditKey, id: StreamId) -> usize {
        let mut hash = FxHasher::default();
        (key, id).hash(&mut hash);
        hash.finish() as usize & (self.0.len() - 1)
    }
    fn find(&self, nodes: &[Node], key: CreditKey, id: StreamId) -> Option<u32> {
        let mut next = self.0[self.bucket(key, id)];
        while next != NONE {
            let node = &nodes[next as usize];
            if matches!(&node.value, Some(Value::Credit(v)) if v.key == key && v.id == id) {
                return Some(next);
            }
            next = node.prev;
        }
        None
    }
    fn remove(&mut self, nodes: &mut [Node], key: CreditKey, id: StreamId, slot: u32) -> bool {
        let bucket = self.bucket(key, id);
        let mut previous = NONE;
        let mut next = self.0[bucket];
        while next != NONE {
            if next == slot {
                if previous == NONE {
                    self.0[bucket] = nodes[next as usize].prev;
                } else {
                    nodes[previous as usize].prev = nodes[next as usize].prev;
                }
                return true;
            }
            previous = next;
            next = nodes[next as usize].prev;
        }
        false
    }
}

pub(in crate::connection) struct Credits(Mode);
enum Mode {
    Legacy(FxHashSet<StreamId>),
    Cold { chain: Chain, key: CreditKey },
}
impl Default for Credits {
    fn default() -> Self {
        Self(Mode::Legacy(FxHashSet::default()))
    }
}
impl fmt::Debug for Credits {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamCredits")
            .field("len", &self.len())
            .finish()
    }
}
impl Credits {
    pub(in crate::connection) fn new(pool: Option<&Pool>, key: CreditKey) -> Self {
        match pool {
            None => Self::default(),
            Some(pool) => Self(Mode::Cold {
                chain: Chain {
                    pool: pool.clone(),
                    head: NONE,
                    tail: NONE,
                    len: 0,
                },
                key,
            }),
        }
    }
    pub(in crate::connection) fn empty_like(&self) -> Self {
        match &self.0 {
            Mode::Legacy(_) => Self::default(),
            Mode::Cold { chain, key } => Self::new(Some(&chain.pool), *key),
        }
    }
    pub(in crate::connection) fn accepts(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            (Mode::Legacy(_), Mode::Legacy(_)) => true,
            (Mode::Cold { chain: a, .. }, Mode::Cold { chain: b, .. }) => a.pool.same(&b.pool),
            (Mode::Cold { .. }, Mode::Legacy(v)) => v.is_empty() && v.capacity() == 0,
            _ => false,
        }
    }
    pub(in crate::connection) fn quarantine(&self) {
        if let Mode::Cold { chain, .. } = &self.0 {
            chain.pool.quarantine();
        }
    }
    pub(in crate::connection) fn prepare(
        &self,
        id: StreamId,
    ) -> Result<Option<Entry<StreamId>>, ()> {
        let slot = match &self.0 {
            Mode::Legacy(values) => {
                if values.contains(&id) {
                    return Ok(None);
                }
                None
            }
            Mode::Cold { chain, key } => {
                chain.pool.0.meter.admit_precharged().map_err(|_| ())?;
                {
                    let state = chain.pool.lock();
                    if state.credit_index.find(&state.nodes, *key, id).is_some() {
                        return Ok(None);
                    }
                }
                Some(chain.pool.reserve(0)?)
            }
        };
        Ok(Some(Entry { value: id, slot }))
    }
    pub(in crate::connection) fn insert(&mut self, mut entry: Entry<StreamId>) -> Result<(), ()> {
        match (&mut self.0, entry.slot.as_mut()) {
            (Mode::Legacy(values), None) => {
                values.insert(entry.value);
            }
            (Mode::Cold { chain, key }, Some(slot)) if chain.pool.same(&slot.pool) => {
                let mut state = chain.pool.lock();
                if state
                    .credit_index
                    .find(&state.nodes, *key, entry.value)
                    .is_some()
                {
                    slot.phase = Phase::Prepared; // Original set merge consumes the duplicate.
                    return Ok(());
                }
                let bucket = state.credit_index.bucket(*key, entry.value);
                let hash_next = state.credit_index.0[bucket];
                state.nodes[slot.id as usize] = Node {
                    value: Some(Value::Credit(CreditValue {
                        key: *key,
                        id: entry.value,
                    })),
                    prev: hash_next,
                    next: NONE,
                };
                state.credit_index.0[bucket] = slot.id;
                if chain.tail == NONE {
                    chain.head = slot.id;
                } else {
                    state.nodes[chain.tail as usize].next = slot.id;
                }
                chain.tail = slot.id;
                chain.len += 1;
                slot.phase = Phase::Transferred;
            }
            _ => {
                self.quarantine();
                if let Some(slot) = &entry.slot {
                    slot.pool.quarantine();
                }
                return Err(());
            }
        }
        Ok(())
    }
    pub(in crate::connection) fn pop(&mut self) -> Option<Entry<StreamId>> {
        match &mut self.0 {
            Mode::Legacy(values) => {
                let id = *values.iter().next()?;
                values.remove(&id);
                Some(Entry {
                    value: id,
                    slot: None,
                })
            }
            Mode::Cold { chain, key } => {
                if chain.head == NONE {
                    return None;
                }
                let slot = chain.head;
                let mut state = chain.pool.lock();
                let Some(Value::Credit(value)) = state.nodes[slot as usize].value.as_ref() else {
                    chain.pool.quarantine();
                    return None;
                };
                let value = *value;
                let State {
                    nodes,
                    credit_index,
                    ..
                } = &mut *state;
                if value.key != *key || !credit_index.remove(nodes, *key, value.id, slot) {
                    chain.pool.quarantine();
                    return None;
                }
                chain.head = nodes[slot as usize].next;
                if chain.head == NONE {
                    chain.tail = NONE;
                }
                nodes[slot as usize].value = None;
                nodes[slot as usize].prev = NONE;
                nodes[slot as usize].next = NONE;
                chain.len -= 1;
                Some(Entry {
                    value: value.id,
                    slot: Some(Slot {
                        pool: chain.pool.clone(),
                        id: slot,
                        phase: Phase::Prepared,
                    }),
                })
            }
        }
    }
    pub(in crate::connection) fn append(&mut self, mut other: Self) {
        if !self.accepts(&other) {
            self.quarantine();
            other.quarantine();
            return;
        }
        while let Some(entry) = other.pop() {
            if self.insert(entry).is_err() {
                return;
            }
        }
    }
    pub(in crate::connection) fn len(&self) -> usize {
        match &self.0 {
            Mode::Legacy(v) => v.len(),
            Mode::Cold { chain, .. } => chain.len,
        }
    }
    pub(in crate::connection) fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub(in crate::connection) fn iter(&self) -> CreditIter<'_> {
        match &self.0 {
            Mode::Legacy(v) => CreditIter::Legacy(v.iter()),
            Mode::Cold { chain, .. } => CreditIter::Cold(Iter::Cold {
                pool: &chain.pool,
                next: chain.head,
                marker: PhantomData,
            }),
        }
    }
}
impl Drop for Credits {
    fn drop(&mut self) {
        while let Some(entry) = self.pop() {
            drop(entry);
        }
    }
}
pub(in crate::connection) enum CreditIter<'a> {
    Legacy(std::collections::hash_set::Iter<'a, StreamId>),
    Cold(Iter<'a, StreamId>),
}
impl Iterator for CreditIter<'_> {
    type Item = StreamId;
    fn next(&mut self) -> Option<StreamId> {
        match self {
            Self::Legacy(i) => i.next().copied(),
            Self::Cold(i) => i.next(),
        }
    }
}

#[cfg(test)]
#[path = "tests/credits.rs"]
mod tests;
