//! One Connection's precharged control records. Pending/packet/recovery lists
//! move the same nodes, without allocation. A prepared Entry is linear; dirty
//! interruption quarantines the shared authority. Known disposal returns slots,
//! while only the final Pool reference releases the physical cold layout.
//! This migration covers RESET/STOP, STREAM and MAX_STREAM_DATA; other metadata remains
//! legacy inventory in the parent. Entry points: Pool::prepare, Queue/Entry.
use crate::{frame, EndpointConfig, StreamId, VarInt};
use bytes::{BackingBudgetReservation, BackingMeter};
use std::{
    alloc::{alloc, Layout},
    fmt, io,
    marker::PhantomData,
    sync::{Arc, Mutex, MutexGuard},
};

const NONE: u32 = u32::MAX;
mod crypto;
pub(in crate::connection) use crypto::{CryptoEntry, CryptoFrames};
mod credits;
pub(in crate::connection) use credits::{CreditKey, Credits};
mod packet;
pub(in crate::connection) use packet::PacketRecords;
mod streams;
pub(in crate::connection) use streams::{Disposition as StreamDisposition, PreparedStreams, StreamFrames, StreamRecord};
#[derive(Clone)]
pub(in crate::connection) enum Value {
    Crypto(frame::Crypto),
    Retire(u64),
    Reset((StreamId, VarInt)),
    Stop(frame::StopSending),
    Stream(StreamRecord),
    Credit(credits::CreditValue),
}
pub(in crate::connection) trait Item: Copy {
    const CLASS: usize = 0;
    fn pack(self) -> Value;
    fn unpack(value: Value) -> Option<Self>;
}
impl Item for u64 {
    fn pack(self) -> Value {
        Value::Retire(self)
    }
    fn unpack(value: Value) -> Option<Self> {
        if let Value::Retire(v) = value {
            Some(v)
        } else {
            None
        }
    }
}
impl Item for (StreamId, VarInt) {
    fn pack(self) -> Value {
        Value::Reset(self)
    }
    fn unpack(value: Value) -> Option<Self> {
        if let Value::Reset(v) = value {
            Some(v)
        } else {
            None
        }
    }
}
impl Item for frame::StopSending {
    fn pack(self) -> Value {
        Value::Stop(self)
    }
    fn unpack(value: Value) -> Option<Self> {
        if let Value::Stop(v) = value {
            Some(v)
        } else {
            None
        }
    }
}
struct Node {
    value: Option<Value>,
    prev: u32,
    next: u32,
}
struct State {
    nodes: Box<[Node]>,
    packets: packet::Stock,
    credit_index: credits::Index,
    // Drop actual storage before returning its allowance.
    _reservation: BackingBudgetReservation,
    free_head: [u32; 3],
    free: [usize; 3],
    control_capacity: usize,
    crypto_start: usize,
}
struct Inner {
    state: Mutex<State>,
    meter: BackingMeter,
}
/// One pre-PN packet admission, including all possible CRYPTO split nodes.
#[derive(Default)]
pub(in crate::connection) struct PreparedPacket {
    pub(in crate::connection) records: PacketRecords,
    pub(in crate::connection) streams: PreparedStreams,
    pub(in crate::connection) crypto: CryptoFrames,
}
#[derive(Clone)]
pub(crate) struct Pool(Arc<Inner>);
impl Pool {
    fn node_count(config: &EndpointConfig) -> io::Result<usize> {
        config
            .control_record_capacity
            .checked_add(config.stream_record_capacity)
            .and_then(|n| n.checked_add(config.crypto_record_capacity))
            .filter(|n| *n < u32::MAX as usize)
            .ok_or_else(|| io::Error::other("record capacity overflow"))
    }
    pub(crate) fn layout_bytes(config: &EndpointConfig) -> io::Result<usize> {
        Layout::array::<Node>(Self::node_count(config)?)
            .map_err(io::Error::other)?
            .size()
            .checked_add(credits::Index::layout(config.control_record_capacity)?.size())
            .and_then(|size| {
                size.checked_add(
                    packet::Stock::layout_bytes(
                        super::super::sent_history::History::descriptor_capacity(config).ok()?,
                    )
                    .ok()?,
                )
            })
            .ok_or_else(|| io::Error::other("control inventory layout overflow"))
    }
    pub(crate) fn prepare(config: &EndpointConfig) -> io::Result<Option<Self>> {
        let Some(meter) = config
            .receive_backing_meter
            .as_ref()
            .filter(|m| m.budget_limit().is_some())
        else {
            return Ok(None);
        };
        let count = Self::node_count(config)?;
        let layout = Layout::array::<Node>(count).map_err(io::Error::other)?;
        let reservation = meter
            .reserve_cold_layout(Self::layout_bytes(config)?)
            .map_err(|error| io::Error::other(format!("{error:?}")))?
            .ok_or_else(|| io::Error::other("control records require hard allowance"))?;
        let ptr = unsafe { alloc(layout).cast::<Node>() };
        if ptr.is_null() {
            return Err(io::Error::other("control record allocation failed"));
        }
        for i in 0..count {
            unsafe {
                ptr.add(i).write(Node {
                    value: None,
                    prev: NONE,
                    next: if i + 1 < count
                        && i + 1 != config.control_record_capacity
                        && i + 1 != config.control_record_capacity + config.stream_record_capacity
                    {
                        i as u32 + 1
                    } else {
                        NONE
                    },
                });
            }
        }
        let nodes = unsafe { Box::from_raw(std::ptr::slice_from_raw_parts_mut(ptr, count)) };
        let credit_index = credits::Index::prepare(config.control_record_capacity)?;
        let packets = packet::Stock::prepare(
            super::super::sent_history::History::descriptor_capacity(config)?,
        )?;
        let pool = Self(Arc::new(Inner {
            state: Mutex::new(State {
                nodes,
                packets,
                credit_index,
                _reservation: reservation,
                free_head: [
                    0,
                    config.control_record_capacity as u32,
                    (config.control_record_capacity + config.stream_record_capacity) as u32,
                ],
                free: [
                    config.control_record_capacity,
                    config.stream_record_capacity,
                    config.crypto_record_capacity,
                ],
                control_capacity: config.control_record_capacity,
                crypto_start: config.control_record_capacity + config.stream_record_capacity,
            }),
            meter: meter.clone(),
        }));
        // Some platforms lazily allocate their one runtime mutex object. Do
        // that in the cold factory, before publication or any stream operation.
        // The mutex/Arc are fixed instance inventory, separate from Node layout.
        drop(pool.lock());
        Ok(Some(pool))
    }
    fn lock(&self) -> MutexGuard<'_, State> {
        match self.0.state.lock() {
            Ok(state) => state,
            Err(error) => {
                self.quarantine();
                error.into_inner()
            }
        }
    }
    fn quarantine(&self) {
        self.0.meter.quarantine();
    }
    fn same(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
    fn available(&self, class: usize) -> bool {
        self.0.meter.admit_precharged().is_ok() && self.lock().free[class] != 0
    }
    fn reserve(&self, class: usize) -> Result<Slot, ()> {
        self.0.meter.admit_precharged().map_err(|_| ())?;
        let mut state = self.lock();
        self.0.meter.admit_precharged().map_err(|_| ())?;
        if state.free[class] == 0 {
            return Err(());
        }
        let id = state.free_head[class];
        let next = state.nodes[id as usize].next;
        state.nodes[id as usize].next = NONE;
        state.free_head[class] = next;
        state.free[class] -= 1;
        Ok(Slot {
            pool: self.clone(),
            id,
            phase: Phase::Prepared,
        })
    }
    fn release(&self, id: u32) {
        let mut state = self.lock();
        let class = if id as usize >= state.crypto_start {
            2
        } else {
            usize::from(id as usize >= state.control_capacity)
        };
        let value = state.nodes[id as usize].value.take();
        let next = state.free_head[class];
        state.nodes[id as usize] = Node {
            value: None,
            prev: NONE,
            next,
        };
        state.free_head[class] = id;
        state.free[class] += 1;
        drop(state);
        drop(value);
    }
}
enum Phase {
    Prepared,
    Dirty,
    Transferred,
}
struct Slot {
    pool: Pool,
    id: u32,
    phase: Phase,
}
impl Drop for Slot {
    fn drop(&mut self) {
        match self.phase {
            Phase::Prepared => self.pool.release(self.id),
            Phase::Dirty => {
                self.pool.quarantine();
                self.pool.release(self.id);
            }
            Phase::Transferred => {}
        }
    }
}
/// Value and its exact reserved node. It cannot be cloned into a second owner.
pub(in crate::connection) struct Entry<T: Item> {
    value: T,
    slot: Option<Slot>,
}
impl<T: Item> Entry<T> {
    pub(in crate::connection) fn value(&self) -> T {
        self.value
    }
    pub(in crate::connection) fn discard_proved(mut self) {
        if let Some(slot) = &mut self.slot {
            slot.phase = Phase::Prepared;
        }
    }
    pub(in crate::connection) fn begin_mutation(&mut self) {
        if let Some(slot) = &mut self.slot {
            slot.phase = Phase::Dirty;
        }
    }
}
struct Chain {
    pool: Pool,
    head: u32,
    tail: u32,
    len: usize,
}
pub(in crate::connection) struct Queue<T: Item> {
    mode: Mode<T>,
}
enum Mode<T: Item> {
    Legacy(Vec<T>),
    Cold(Chain, PhantomData<T>),
}
impl<T: Item> Default for Queue<T> {
    fn default() -> Self {
        Self {
            mode: Mode::Legacy(Vec::new()),
        }
    }
}
impl<T: Item> fmt::Debug for Queue<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ControlQueue")
            .field("len", &self.len())
            .finish()
    }
}
impl<T: Item> Queue<T> {
    pub(in crate::connection) fn new(pool: Option<&Pool>) -> Self {
        match pool {
            None => Self::default(),
            Some(pool) => Self {
                mode: Mode::Cold(
                    Chain {
                        pool: pool.clone(),
                        head: NONE,
                        tail: NONE,
                        len: 0,
                    },
                    PhantomData,
                ),
            },
        }
    }
    pub(in crate::connection) fn empty_like(&self) -> Self {
        match &self.mode {
            Mode::Legacy(_) => Self::default(),
            Mode::Cold(chain, _) => Self::new(Some(&chain.pool)),
        }
    }
    pub(in crate::connection) fn authority_ready(&self) -> bool {
        match &self.mode {
            Mode::Legacy(_) => true,
            Mode::Cold(c, _) => c.pool.0.meter.admit_precharged().is_ok(),
        }
    }
    pub(in crate::connection) fn accepts(&self, other: &Self) -> bool {
        match (&self.mode, &other.mode) {
            (Mode::Legacy(_), Mode::Legacy(_)) => true,
            (Mode::Cold(a, _), Mode::Cold(b, _)) => a.pool.same(&b.pool),
            (Mode::Cold(_, _), Mode::Legacy(v)) => v.is_empty() && v.capacity() == 0,
            _ => false,
        }
    }
    pub(in crate::connection) fn quarantine(&self) {
        if let Mode::Cold(c, _) = &self.mode {
            c.pool.quarantine();
        }
    }
    #[cfg(test)]
    pub(in crate::connection) fn cold_stock(&self) -> Option<(usize, usize, usize)> {
        self.class_stock(T::CLASS)
    }
    #[cfg(test)]
    pub(in crate::connection) fn class_stock(&self, class: usize) -> Option<(usize, usize, usize)> {
        let Mode::Cold(c, _) = &self.mode else {
            return None;
        };
        let state = c.pool.lock();
        let range = if class == 0 {
            0..state.control_capacity
        } else if class == 1 {
            state.control_capacity..state.crypto_start
        } else {
            state.crypto_start..state.nodes.len()
        };
        Some((
            range.len(),
            state.free[class],
            state.nodes[range]
                .iter()
                .filter(|n| n.value.is_some())
                .count(),
        ))
    }
    pub(in crate::connection) fn prepare(&self, value: T) -> Result<Entry<T>, ()> {
        let slot = match &self.mode {
            Mode::Legacy(_) => None,
            Mode::Cold(chain, _) => Some(chain.pool.reserve(T::CLASS)?),
        };
        Ok(Entry { value, slot })
    }
    pub(in crate::connection) fn len(&self) -> usize {
        match &self.mode {
            Mode::Legacy(v) => v.len(),
            Mode::Cold(c, _) => c.len,
        }
    }
    pub(in crate::connection) fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub(in crate::connection) fn push(&mut self, mut entry: Entry<T>) {
        if let (Mode::Legacy(v), Some(slot)) = (&self.mode, &entry.slot) {
            if v.is_empty() && v.capacity() == 0 {
                *self = Self::new(Some(&slot.pool));
            }
        }
        match (&mut self.mode, entry.slot.take()) {
            (Mode::Legacy(v), None) => v.push(entry.value),
            (Mode::Cold(c, _), Some(mut slot)) if c.pool.same(&slot.pool) => {
                let mut state = c.pool.lock();
                state.nodes[slot.id as usize] = Node {
                    value: Some(entry.value.pack()),
                    prev: c.tail,
                    next: NONE,
                };
                if c.tail != NONE {
                    state.nodes[c.tail as usize].next = slot.id;
                } else {
                    c.head = slot.id;
                }
                c.tail = slot.id;
                c.len += 1;
                slot.phase = Phase::Transferred;
            }
            (Mode::Cold(c, _), slot) => {
                c.pool.quarantine();
                if let Some(slot) = slot {
                    slot.pool.quarantine();
                }
            }
            (Mode::Legacy(_), Some(slot)) => {
                slot.pool.quarantine();
            }
        }
    }
    pub(in crate::connection) fn pop(&mut self) -> Option<Entry<T>> {
        match &mut self.mode {
            Mode::Legacy(v) => v.pop().map(|value| Entry { value, slot: None }),
            Mode::Cold(c, _) => {
                if c.tail == NONE {
                    return None;
                }
                let id = c.tail;
                let mut state = c.pool.lock();
                let Some(value) = state.nodes[id as usize].value.take().and_then(T::unpack) else {
                    c.pool.quarantine();
                    return None;
                };
                c.tail = state.nodes[id as usize].prev;
                if c.tail == NONE {
                    c.head = NONE;
                } else {
                    state.nodes[c.tail as usize].next = NONE;
                }
                state.nodes[id as usize].prev = NONE;
                c.len -= 1;
                Some(Entry {
                    value,
                    slot: Some(Slot {
                        pool: c.pool.clone(),
                        id,
                        phase: Phase::Prepared,
                    }),
                })
            }
        }
    }
    fn pop_front(&mut self) -> Option<Entry<T>> {
        let Mode::Cold(c, _) = &mut self.mode else {
            return None;
        };
        if c.head == NONE {
            return None;
        }
        let id = c.head;
        let mut state = c.pool.lock();
        let Some(value) = state.nodes[id as usize].value.take().and_then(T::unpack) else {
            c.pool.quarantine();
            return None;
        };
        c.head = state.nodes[id as usize].next;
        if c.head == NONE {
            c.tail = NONE;
        } else {
            state.nodes[c.head as usize].prev = NONE;
        }
        state.nodes[id as usize].next = NONE;
        c.len -= 1;
        Some(Entry {
            value,
            slot: Some(Slot {
                pool: c.pool.clone(),
                id,
                phase: Phase::Prepared,
            }),
        })
    }
    pub(in crate::connection) fn append(&mut self, mut other: Self) {
        if let (Mode::Legacy(v), Mode::Cold(c, _)) = (&self.mode, &other.mode) {
            if v.is_empty() && v.capacity() == 0 {
                *self = Self::new(Some(&c.pool));
            }
        }
        match (&mut self.mode, &mut other.mode) {
            (Mode::Legacy(a), Mode::Legacy(b)) => a.append(b),
            (Mode::Cold(a, _), Mode::Cold(b, _)) if a.pool.same(&b.pool) => {
                if b.len == 0 {
                    return;
                }
                let mut state = a.pool.lock();
                if a.tail == NONE {
                    a.head = b.head;
                } else {
                    state.nodes[a.tail as usize].next = b.head;
                    state.nodes[b.head as usize].prev = a.tail;
                }
                a.tail = b.tail;
                a.len += b.len;
                b.head = NONE;
                b.tail = NONE;
                b.len = 0;
            }
            (Mode::Cold(_, _), Mode::Legacy(v)) if v.is_empty() => {}
            (Mode::Cold(a, _), Mode::Cold(b, _)) => {
                a.pool.quarantine();
                b.pool.quarantine();
            }
            (Mode::Cold(a, _), _) => a.pool.quarantine(),
            (_, Mode::Cold(b, _)) => b.pool.quarantine(),
        }
    }
    pub(in crate::connection) fn iter(&self) -> Iter<'_, T> {
        match &self.mode {
            Mode::Legacy(v) => Iter::Legacy(v.iter()),
            Mode::Cold(c, _) => Iter::Cold {
                pool: &c.pool,
                next: c.head,
                marker: PhantomData,
            },
        }
    }
}
impl<T: Item> Drop for Queue<T> {
    fn drop(&mut self) {
        while let Some(entry) = self.pop() {
            drop(entry);
        }
    }
}
pub(in crate::connection) enum Iter<'a, T: Item> {
    Legacy(std::slice::Iter<'a, T>),
    Cold {
        pool: &'a Pool,
        next: u32,
        marker: PhantomData<T>,
    },
}
impl<T: Item> Iterator for Iter<'_, T> {
    type Item = T;
    fn next(&mut self) -> Option<T> {
        match self {
            Self::Legacy(iter) => iter.next().copied(),
            Self::Cold { pool, next, .. } => {
                if *next == NONE {
                    return None;
                }
                let state = pool.lock();
                let node = &state.nodes[*next as usize];
                let Some(value) = node.value.clone().and_then(T::unpack) else {
                    pool.quarantine();
                    *next = NONE;
                    return None;
                };
                *next = node.next;
                Some(value)
            }
        }
    }
}

#[cfg(test)]
mod tests;
