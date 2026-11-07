//! Owns precharged CID frame records from issuance through ACK or disposal.
//! ColdResources in cold_inventory prepares this pool before CID/TLS mutation.
//! A record moves
//! between reply, pending, packet and retransmit lists without new allocation.
//! Native's no-lifetime profile reserves ordinary stock plus LOC control slots;
//! only first encoding is gated, never retransmission, ACK or terminal cleanup.
//! This does not budget Endpoint routes or generic sent/ACK storage. Tests are
//! in cid_frames/tests; original encrypted-path tests are in tests/admission.

use crate::{shared::IssuedCid, LOC_CID_COUNT};
use bytes::{BackingBudgetReservation, BackingMeter};
use std::{
    alloc::{alloc, Layout, LayoutError},
    fmt, io,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex, MutexGuard,
    },
};

mod reply;

const NONE: u32 = u32::MAX;
const ORDINARY: usize = 64;

struct Node {
    value: Option<IssuedCid>,
    next: u32,
    encoded: bool,
}
struct State {
    nodes: Box<[Node]>,
    _reservation: BackingBudgetReservation,
    free_head: u32,
}
struct Inner {
    state: Mutex<State>,
    free: AtomicUsize,
    meter: BackingMeter,
}
#[derive(Clone)]
pub(crate) struct Pool(Arc<Inner>);
impl fmt::Debug for Pool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CidPool")
            .field("free", &self.0.free.load(Ordering::Acquire))
            .finish()
    }
}
impl Pool {
    fn lock(&self) -> MutexGuard<'_, State> {
        match self.0.state.lock() {
            Ok(state) => state,
            Err(error) => {
                self.quarantine();
                error.into_inner()
            }
        }
    }
    pub(crate) fn layout() -> Result<Layout, LayoutError> {
        Layout::array::<Node>(ORDINARY + LOC_CID_COUNT as usize)
    }
    pub(super) fn new(meter: &BackingMeter) -> io::Result<Self> {
        let layout = Self::layout().map_err(io::Error::other)?;
        let count = layout.size() / std::mem::size_of::<Node>();
        let reservation = meter
            .reserve_cold_layout(layout.size())
            .map_err(|error| io::Error::other(format!("{error:?}")))?
            .ok_or_else(|| io::Error::other("CID pool requires hard allowance"))?;
        // The exact layout is admitted before allocation; every element is
        // initialized before constructing the Box, which precedes the credit.
        let ptr = unsafe { alloc(layout) as *mut Node };
        if ptr.is_null() {
            return Err(io::Error::other("CID pool allocation failed"));
        }
        for index in 0..count {
            unsafe {
                ptr.add(index).write(Node {
                    value: None,
                    next: if index + 1 < count {
                        index as u32 + 1
                    } else {
                        NONE
                    },
                    encoded: false,
                });
            }
        }
        let nodes = unsafe { Box::from_raw(std::ptr::slice_from_raw_parts_mut(ptr, count)) };
        let pool = Self(Arc::new(Inner {
            state: Mutex::new(State {
                nodes,
                _reservation: reservation,
                free_head: 0,
            }),
            free: AtomicUsize::new(count),
            meter: meter.clone(),
        }));
        // Warm the actual platform mutex before connection publication.
        drop(pool.lock());
        Ok(pool)
    }
    pub(crate) fn quarantine(&self) {
        self.0.meter.quarantine();
    }
    fn first_encoding_ready(&self) -> bool {
        !self.0.meter.snapshot().quarantined
            && self.0.free.load(Ordering::Acquire) >= LOC_CID_COUNT as usize
    }
    pub(crate) fn prepare(&self, count: usize) -> Option<Prepared> {
        let mut state = self.lock();
        if self.0.meter.snapshot().quarantined {
            return None;
        }
        let free = self.0.free.load(Ordering::Relaxed);
        // This is bounded CID-control preparation, never per-DATA accounting.
        // Validate physical free links before the first node can be detached;
        // an audit after finish/reverse would be too late for a corrupt cycle.
        if reply::free_inventory(&state, free).is_none() {
            self.quarantine();
            return None;
        }
        if count > free {
            return None;
        }
        let mut chain = Chain::empty(self.clone());
        for _ in 0..count {
            let index = state.free_head;
            let node = &mut state.nodes[index as usize];
            state.free_head = node.next;
            state.nodes[index as usize].next = chain.head;
            chain.head = index;
            if chain.tail == NONE {
                chain.tail = index;
            }
            chain.len += 1;
        }
        self.0.free.store(free - count, Ordering::Release);
        let cursor = chain.head;
        Some(Prepared::Cold {
            frames: Frames::Cold(chain),
            cursor,
            dirty: false,
        })
    }
    fn release(&self, index: u32) {
        let mut state = self.lock();
        let head = state.free_head;
        state.nodes[index as usize] = Node {
            value: None,
            next: head,
            encoded: false,
        };
        state.free_head = index;
        self.0.free.fetch_add(1, Ordering::Release);
    }
}

#[derive(Debug)]
pub(crate) struct Chain {
    pool: Pool,
    head: u32,
    tail: u32,
    len: u32,
    encoded: u32,
}
impl Chain {
    fn empty(pool: Pool) -> Self {
        Self {
            pool,
            head: NONE,
            tail: NONE,
            len: 0,
            encoded: 0,
        }
    }
}
#[derive(Debug)]
pub(crate) enum Frames {
    Legacy(Vec<IssuedCid>),
    Cold(Chain),
}
impl Default for Frames {
    fn default() -> Self {
        Self::Legacy(Vec::new())
    }
}
impl Frames {
    pub(crate) fn empty(pool: Option<&Pool>) -> Self {
        pool.map_or_else(Self::default, |pool| Self::Cold(Chain::empty(pool.clone())))
    }
    pub(crate) fn cold_pool(&self) -> Option<&Pool> {
        match self { Self::Cold(chain) => Some(&chain.pool), Self::Legacy(_) => None }
    }
    pub(crate) fn is_empty(&self) -> bool {
        match self {
            Self::Legacy(items) => items.is_empty(),
            Self::Cold(chain) => chain.len == 0,
        }
    }
    pub(crate) fn ready(&self) -> bool {
        match self {
            Self::Legacy(items) => !items.is_empty(),
            Self::Cold(chain) => {
                chain.len != 0
                    && !chain.pool.0.meter.snapshot().quarantined
                    && (chain.encoded != 0 || chain.pool.first_encoding_ready())
            }
        }
    }
    pub(crate) fn iter(&self) -> Values<'_> {
        Values {
            frames: self,
            position: 0,
            next: match self {
                Self::Cold(chain) => chain.head,
                _ => NONE,
            },
        }
    }
    pub(crate) fn reverse(&mut self) {
        match self {
            Self::Legacy(items) => items.reverse(),
            Self::Cold(chain) => {
                let mut state = chain.pool.lock();
                let mut current = chain.head;
                let mut previous = NONE;
                chain.tail = chain.head;
                while current != NONE {
                    let next = state.nodes[current as usize].next;
                    state.nodes[current as usize].next = previous;
                    previous = current;
                    current = next;
                }
                chain.head = previous;
            }
        }
    }
    pub(crate) fn pop_sendable(&mut self) -> Option<Frame> {
        self.pop_inner(true)
    }
    fn pop_inner(&mut self, sendable: bool) -> Option<Frame> {
        match self {
            Self::Legacy(items) => items.pop().map(|value| Frame { value, owner: None }),
            Self::Cold(chain) => {
                let mut state = chain.pool.lock();
                if chain.pool.0.meter.snapshot().quarantined {
                    return None;
                }
                let allow_new = !sendable || chain.pool.first_encoding_ready();
                let mut index = chain.head;
                let mut previous = NONE;
                while index != NONE && !allow_new && !state.nodes[index as usize].encoded {
                    previous = index;
                    index = state.nodes[index as usize].next;
                }
                if index == NONE {
                    return None;
                }
                let node = &state.nodes[index as usize];
                let Some(value) = node.value else {
                    chain.pool.quarantine();
                    return None;
                };
                let next = node.next;
                let encoded = node.encoded;
                if previous == NONE {
                    chain.head = next;
                } else {
                    state.nodes[previous as usize].next = next;
                }
                if chain.tail == index {
                    chain.tail = previous;
                }
                state.nodes[index as usize].next = NONE;
                chain.len -= 1;
                chain.encoded -= u32::from(encoded);
                Some(Frame {
                    value,
                    owner: Some((chain.pool.clone(), index)),
                })
            }
        }
    }
    fn accepts(&self, pool: &Pool) -> bool {
        match self {
            Self::Cold(chain) => Arc::ptr_eq(&chain.pool.0, &pool.0),
            Self::Legacy(items) => items.is_empty(),
        }
    }
    fn reject(&self, pool: &Pool) {
        if let Self::Cold(chain) = self {
            chain.pool.quarantine();
        }
        pool.quarantine();
    }
    pub(crate) fn discard(&mut self) {
        let replacement = match self {
            Self::Cold(chain) => Self::Cold(Chain::empty(chain.pool.clone())),
            Self::Legacy(_) => Self::default(),
        };
        *self = replacement;
    }
    pub(crate) fn push(&mut self, mut frame: Frame) {
        if let Some((pool, _)) = &frame.owner {
            if !self.accepts(pool) {
                self.reject(pool);
                return;
            }
        }
        let Some((pool, index)) = frame.owner.take() else {
            if let Self::Legacy(items) = self {
                items.push(frame.value);
            } else if let Self::Cold(chain) = self {
                chain.pool.quarantine();
            }
            return;
        };
        if matches!(self, Self::Legacy(items) if items.is_empty()) {
            *self = Self::Cold(Chain::empty(pool.clone()));
        }
        match self {
            Self::Cold(chain) if Arc::ptr_eq(&chain.pool.0, &pool.0) => {
                let mut state = pool.lock();
                state.nodes[index as usize].next = chain.head;
                chain.encoded += u32::from(state.nodes[index as usize].encoded);
                chain.head = index;
                if chain.tail == NONE {
                    chain.tail = index;
                }
                chain.len += 1;
            }
            _ => {
                pool.quarantine();
                pool.release(index);
            }
        }
    }
    pub(crate) fn validate_append(&self, other: &Self) -> bool {
        match (self, other) {
            (_, Self::Cold(right)) if !self.accepts(&right.pool) => {
                self.reject(&right.pool);
                false
            }
            (Self::Cold(left), Self::Legacy(right)) if !right.is_empty() => {
                left.pool.quarantine();
                false
            }
            _ => true,
        }
    }
    pub(crate) fn append(&mut self, other: &mut Self) {
        if !self.validate_append(other) {
            return;
        }
        if other.is_empty() {
            return;
        }
        if self.is_empty() {
            std::mem::swap(self, other);
            return;
        }
        match (self, other) {
            (Self::Legacy(left), Self::Legacy(right)) => left.append(right),
            (Self::Cold(left), Self::Cold(right)) if Arc::ptr_eq(&left.pool.0, &right.pool.0) => {
                let mut state = left.pool.lock();
                state.nodes[right.tail as usize].next = left.head;
                left.head = right.head;
                left.len += right.len;
                left.encoded += right.encoded;
                right.head = NONE;
                right.tail = NONE;
                right.len = 0;
                right.encoded = 0;
            }
            (Self::Cold(left), _) => left.pool.quarantine(),
            (_, Self::Cold(right)) => right.pool.quarantine(),
        }
    }
}
impl Drop for Frames {
    fn drop(&mut self) {
        if let Self::Cold(chain) = self {
            if std::thread::panicking() {
                chain.pool.quarantine();
            }
            let mut index = chain.head;
            while index != NONE {
                let next = chain.pool.lock().nodes[index as usize].next;
                chain.pool.release(index);
                index = next;
            }
        }
    }
}
pub(crate) struct Values<'a> {
    frames: &'a Frames,
    position: usize,
    next: u32,
}
impl Iterator for Values<'_> {
    type Item = IssuedCid;
    fn next(&mut self) -> Option<IssuedCid> {
        match self.frames {
            Frames::Legacy(items) => {
                let value = items.get(self.position).copied();
                self.position += 1;
                value
            }
            Frames::Cold(chain) => {
                if self.next == NONE {
                    return None;
                }
                let state = chain.pool.lock();
                let node = &state.nodes[self.next as usize];
                self.next = node.next;
                node.value
            }
        }
    }
}
pub(crate) struct Frame {
    value: IssuedCid,
    owner: Option<(Pool, u32)>,
}
impl Frame {
    pub(crate) fn value(&self) -> IssuedCid {
        self.value
    }
    pub(crate) fn mark_encoded(&self) {
        if let Some((pool, index)) = &self.owner {
            pool.lock().nodes[*index as usize].encoded = true;
        }
    }
}
impl Drop for Frame {
    fn drop(&mut self) {
        if let Some((pool, index)) = self.owner.take() {
            if std::thread::panicking() {
                pool.quarantine();
            }
            pool.release(index);
        }
    }
}

pub(crate) enum Prepared {
    Legacy(Vec<IssuedCid>),
    Cold {
        frames: Frames,
        cursor: u32,
        dirty: bool,
    },
}
impl Prepared {
    pub(crate) fn legacy() -> Self {
        Self::Legacy(Vec::new())
    }
    pub(crate) fn mark_mutated(&mut self) {
        if let Self::Cold { dirty, .. } = self {
            *dirty = true;
        }
    }
    pub(crate) fn push(&mut self, value: IssuedCid) {
        match self {
            Self::Legacy(items) => items.push(value),
            Self::Cold {
                frames: Frames::Cold(chain),
                cursor,
                ..
            } => {
                if *cursor == NONE {
                    chain.pool.quarantine();
                    return;
                }
                let mut state = chain.pool.lock();
                let node = &mut state.nodes[*cursor as usize];
                node.value = Some(value);
                *cursor = node.next;
            }
            _ => {}
        }
    }
    pub(crate) fn finish(mut self) -> Frames {
        match &mut self {
            Self::Legacy(items) => Frames::Legacy(std::mem::take(items)),
            Self::Cold {
                frames,
                cursor,
                dirty,
            } => {
                if *cursor != NONE {
                    if let Frames::Cold(chain) = frames {
                        chain.pool.quarantine();
                    }
                }
                *dirty = false;
                frames.reverse();
                std::mem::take(frames)
            }
        }
    }
}
impl Drop for Prepared {
    fn drop(&mut self) {
        if let Self::Cold {
            frames: Frames::Cold(chain),
            dirty: true,
            ..
        } = self
        {
            chain.pool.quarantine();
        }
    }
}

#[derive(Debug)]
pub(crate) struct CapacityFailure(pub(crate) Pool);
impl Drop for CapacityFailure {
    fn drop(&mut self) {
        self.0.quarantine();
    }
}

impl<'a> IntoIterator for &'a Frames {
    type Item = IssuedCid;
    type IntoIter = Values<'a>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[cfg(test)]
mod tests;
