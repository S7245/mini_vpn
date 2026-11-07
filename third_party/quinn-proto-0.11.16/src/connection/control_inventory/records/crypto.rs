//! CRYPTO owns non-Copy payloads in the original Pool's third free list. One
//! Entry moves from producer to pending/sent/recovery; splitting needs a second
//! pre-PN Entry. FIFO prepend preserves original loss/Retry order. Backing is
//! dropped before its slot returns. No separate refund or allocation authority.
//! Tests: records/tests/crypto and tests/admission/crypto.
use super::*;
use std::collections::VecDeque;

pub(in crate::connection) struct CryptoEntry {
    pub(in crate::connection) frame: frame::Crypto,
    slot: Option<Slot>,
}
impl CryptoEntry {
    pub(in crate::connection) fn begin_mutation(&mut self) {
        if let Some(slot) = &mut self.slot {
            slot.phase = Phase::Dirty;
        }
    }
    pub(in crate::connection) fn proved(&mut self) {
        if let Some(slot) = &mut self.slot {
            slot.phase = Phase::Prepared;
        }
    }
}
impl fmt::Debug for CryptoEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.frame.fmt(f)
    }
}
#[derive(Default)]
pub(in crate::connection) struct CryptoFrames {
    mode: CryptoMode,
}
enum CryptoMode {
    Legacy(VecDeque<frame::Crypto>),
    Cold(Chain),
}
impl Default for CryptoMode {
    fn default() -> Self {
        Self::Legacy(VecDeque::new())
    }
}
impl fmt::Debug for CryptoFrames {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CryptoFrames")
            .field("len", &self.len())
            .finish()
    }
}
impl CryptoFrames {
    pub(in crate::connection) fn new(pool: Option<&Pool>) -> Self {
        Self {
            mode: match pool {
                None => CryptoMode::default(),
                Some(pool) => CryptoMode::Cold(Chain {
                    pool: pool.clone(),
                    head: NONE,
                    tail: NONE,
                    len: 0,
                }),
            },
        }
    }
    pub(in crate::connection) fn empty_like(&self) -> Self {
        match &self.mode {
            CryptoMode::Legacy(_) => Self::default(),
            CryptoMode::Cold(c) => Self::new(Some(&c.pool)),
        }
    }
    pub(in crate::connection) fn budgeted(&self) -> bool {
        matches!(self.mode, CryptoMode::Cold(_))
    }
    pub(in crate::connection) fn prepare(&self) -> Result<CryptoEntry, ()> {
        let slot = match &self.mode {
            CryptoMode::Legacy(_) => None,
            CryptoMode::Cold(c) => Some(c.pool.reserve(2)?),
        };
        Ok(CryptoEntry {
            frame: frame::Crypto {
                offset: 0,
                data: bytes::Bytes::new(),
            },
            slot,
        })
    }
    /// At most one tail split plus one split per full 2^14-1 byte fragment
    /// fits this packet. Hold all such nodes before PacketBuilder changes PN.
    pub(in crate::connection) fn prepare_packet(&self, packet_limit: usize) -> Result<Self, ()> {
        let mut prepared = self.empty_like();
        if self.budgeted() && !self.is_empty() {
            for _ in 0..=packet_limit / ((1 << 14) - 1) {
                prepared.push_back(self.prepare()?);
            }
        }
        Ok(prepared)
    }
    pub(in crate::connection) fn split_ticket(&mut self) -> Option<CryptoEntry> {
        if self.budgeted() {
            self.pop_front()
        } else {
            self.prepare().ok()
        }
    }
    pub(in crate::connection) fn len(&self) -> usize {
        match &self.mode {
            CryptoMode::Legacy(v) => v.len(),
            CryptoMode::Cold(c) => c.len,
        }
    }
    pub(in crate::connection) fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub(in crate::connection) fn accepts(&self, other: &Self) -> bool {
        match (&self.mode, &other.mode) {
            (CryptoMode::Legacy(_), CryptoMode::Legacy(_)) => true,
            (CryptoMode::Cold(a), CryptoMode::Cold(b)) => a.pool.same(&b.pool),
            (CryptoMode::Cold(_), CryptoMode::Legacy(v)) => v.is_empty() && v.capacity() == 0,
            _ => false,
        }
    }
    pub(in crate::connection) fn quarantine(&self) {
        if let CryptoMode::Cold(c) = &self.mode {
            c.pool.quarantine();
        }
    }
    pub(in crate::connection) fn push_back(&mut self, entry: CryptoEntry) {
        self.push(entry, false);
    }
    pub(in crate::connection) fn push_front(&mut self, entry: CryptoEntry) {
        self.push(entry, true);
    }
    fn push(&mut self, mut entry: CryptoEntry, front: bool) {
        if let (CryptoMode::Legacy(v), Some(slot)) = (&self.mode, &entry.slot) {
            if v.is_empty() && v.capacity() == 0 {
                *self = Self::new(Some(&slot.pool));
            }
        }
        match (&mut self.mode, entry.slot.take()) {
            (CryptoMode::Legacy(v), None) => {
                if front {
                    v.push_front(entry.frame);
                } else {
                    v.push_back(entry.frame);
                }
            }
            (CryptoMode::Cold(c), Some(mut slot)) if c.pool.same(&slot.pool) => {
                let mut state = c.pool.lock();
                let (prev, next) = if front {
                    (NONE, c.head)
                } else {
                    (c.tail, NONE)
                };
                state.nodes[slot.id as usize] = Node {
                    value: Some(Value::Crypto(entry.frame)),
                    prev,
                    next,
                };
                if prev != NONE {
                    state.nodes[prev as usize].next = slot.id;
                } else {
                    c.head = slot.id;
                }
                if next != NONE {
                    state.nodes[next as usize].prev = slot.id;
                } else {
                    c.tail = slot.id;
                }
                c.len += 1;
                slot.phase = Phase::Transferred;
            }
            (CryptoMode::Cold(c), slot) => {
                c.pool.quarantine();
                if let Some(slot) = slot {
                    slot.pool.quarantine();
                }
            }
            (_, Some(slot)) => slot.pool.quarantine(),
        }
    }
    pub(in crate::connection) fn pop_front(&mut self) -> Option<CryptoEntry> {
        match &mut self.mode {
            CryptoMode::Legacy(v) => v.pop_front().map(|frame| CryptoEntry { frame, slot: None }),
            CryptoMode::Cold(c) => {
                if c.head == NONE {
                    return None;
                }
                let id = c.head;
                let mut state = c.pool.lock();
                let Some(Value::Crypto(frame)) = state.nodes[id as usize].value.take() else {
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
                Some(CryptoEntry {
                    frame,
                    slot: Some(Slot {
                        pool: c.pool.clone(),
                        id,
                        phase: Phase::Prepared,
                    }),
                })
            }
        }
    }
    pub(in crate::connection) fn prepend(&mut self, mut other: Self) {
        if let (CryptoMode::Legacy(v), CryptoMode::Cold(c)) = (&self.mode, &other.mode) {
            if v.is_empty() && v.capacity() == 0 {
                *self = Self::new(Some(&c.pool));
            }
        }
        match (&mut self.mode, &mut other.mode) {
            (CryptoMode::Legacy(a), CryptoMode::Legacy(b)) => {
                while let Some(frame) = b.pop_back() {
                    a.push_front(frame);
                }
            }
            (CryptoMode::Cold(a), CryptoMode::Cold(b)) if a.pool.same(&b.pool) => {
                if b.len == 0 {
                    return;
                }
                let mut state = a.pool.lock();
                if a.head == NONE {
                    a.tail = b.tail;
                } else {
                    state.nodes[a.head as usize].prev = b.tail;
                    state.nodes[b.tail as usize].next = a.head;
                }
                a.head = b.head;
                a.len += b.len;
                b.head = NONE;
                b.tail = NONE;
                b.len = 0;
            }
            (CryptoMode::Cold(_), CryptoMode::Legacy(v)) if v.is_empty() => {}
            (CryptoMode::Cold(a), CryptoMode::Cold(b)) => {
                a.pool.quarantine();
                b.pool.quarantine();
            }
            (CryptoMode::Cold(a), _) => a.pool.quarantine(),
            (_, CryptoMode::Cold(b)) => b.pool.quarantine(),
        }
    }
    #[cfg(test)]
    pub(in crate::connection) fn frames(&self) -> Vec<frame::Crypto> {
        match &self.mode {
            CryptoMode::Legacy(v) => v.iter().cloned().collect(),
            CryptoMode::Cold(c) => {
                let state = c.pool.lock();
                let mut next = c.head;
                let mut frames = Vec::new();
                while next != NONE {
                    let node = &state.nodes[next as usize];
                    if let Some(Value::Crypto(frame)) = &node.value {
                        frames.push(frame.clone());
                    }
                    next = node.next;
                }
                frames
            }
        }
    }
}
impl Drop for CryptoFrames {
    fn drop(&mut self) {
        while let Some(entry) = self.pop_front() {
            drop(entry);
        }
    }
}
