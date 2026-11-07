//! Original generation's Native fragment transaction. Owns fixed partials,
//! descriptors and key index; it owns no epoch, callback, queue or refund actor.
//! All metadata is admitted before allocation. Output backing is admitted before
//! replacement/completion commits; pressure returns the original input unchanged.
//! DATA uses key lookup and one packet's descriptors, never a global inventory.
//! Control sweep/retirement audits both index directions and independent sums.
//! Entry: prepare/accept/sweep/retire_generation. Tests live in native_fragments/tests.
use crate::tuic::decode_packet_meta;
use crate::udp_relay::MAX_IPV4_UDP_PAYLOAD_BYTES;
use bytes::{BackingBudgetReservation, BackingMeter, Bytes, BytesMut, MeteredCopyError};
use std::{
    alloc::{Layout, alloc, dealloc},
    mem::ManuallyDrop,
};
const CAPACITY: usize = crate::tuic::FRAG_REASSEMBLY_CAP;
const FRAGMENTS: usize = u8::MAX as usize;
type Key = (u16, u16);
#[derive(Clone, Copy, PartialEq, Eq)]
struct Entry {
    key: Key,
    slot: usize,
}
const EMPTY: Entry = Entry {
    key: (0, 0),
    slot: usize::MAX,
};
struct Partial {
    key: Option<Key>,
    total: usize,
    received: usize,
    bytes: usize,
    first_seen: u64,
    fragments: [Option<Bytes>; FRAGMENTS],
}
impl Partial {
    fn empty() -> Self {
        Self {
            key: None,
            total: 0,
            received: 0,
            bytes: 0,
            first_seen: 0,
            fragments: std::array::from_fn(|_| None),
        }
    }
    fn clear(&mut self) {
        for fragment in &mut self.fragments {
            *fragment = None;
        }
        self.key = None;
        self.total = 0;
        self.received = 0;
        self.bytes = 0;
        self.first_seen = 0;
    }
}
#[derive(Clone, Copy)]
struct Selection {
    slot: usize,
    at: usize,
}
struct InsertPlan {
    slot: usize,
    remove: Option<usize>,
    at: usize,
}
struct Root {
    slots: Vec<Partial>,
    index: [Entry; CAPACITY],
    len: usize,
    meter: BackingMeter,
}
pub(super) struct Fragments {
    root: ManuallyDrop<Box<Root>>,
    stock: ManuallyDrop<Option<BackingBudgetReservation>>,
}
#[derive(Debug)]
pub(super) struct Rejected {
    pub(super) frame: Bytes,
    pub(super) error: MeteredCopyError,
}
impl Fragments {
    pub(super) fn layout_bytes() -> Result<usize, MeteredCopyError> {
        Layout::array::<Partial>(CAPACITY)
            .map_err(|_| MeteredCopyError::TooLarge)?
            .size()
            .checked_add(Layout::new::<Root>().size())
            .ok_or(MeteredCopyError::TooLarge)
    }
    pub(super) fn prepare(meter: &BackingMeter) -> Result<Self, MeteredCopyError> {
        if meter.budget_limit().is_none() {
            meter.quarantine();
            return Err(MeteredCopyError::Quarantined);
        }
        let stock = meter.reserve_cold_layout(Self::layout_bytes()?)?;
        let root_layout = Layout::new::<Root>();
        let slots_layout =
            Layout::array::<Partial>(CAPACITY).map_err(|_| MeteredCopyError::TooLarge)?;
        let pointer = unsafe { alloc(root_layout).cast::<Root>() };
        if pointer.is_null() {
            return Err(MeteredCopyError::Allocation);
        }
        let slots_pointer = unsafe { alloc(slots_layout).cast::<Partial>() };
        if slots_pointer.is_null() {
            unsafe {
                dealloc(pointer.cast(), root_layout);
            }
            return Err(MeteredCopyError::Allocation);
        }
        // Exact nonzero layouts were admitted together. All following writes
        // initialize known fields without allocation or external callbacks.
        let mut slots = unsafe { Vec::from_raw_parts(slots_pointer, 0, CAPACITY) };
        for _ in 0..CAPACITY {
            slots.push(Partial::empty());
        }
        unsafe {
            pointer.write(Root {
                slots,
                index: [EMPTY; CAPACITY],
                len: 0,
                meter: meter.clone(),
            });
        }
        Ok(Self {
            root: ManuallyDrop::new(unsafe { Box::from_raw(pointer) }),
            stock: ManuallyDrop::new(stock),
        })
    }
    fn reject(&self, frame: Bytes, error: MeteredCopyError) -> Rejected {
        Rejected { frame, error }
    }
    fn invalid(&self, frame: Bytes) -> Rejected {
        self.root.meter.quarantine();
        self.reject(frame, MeteredCopyError::Quarantined)
    }
    pub(super) fn accept(
        &mut self,
        frame: Bytes,
        now: u64,
    ) -> Result<Option<(u16, Bytes)>, Rejected> {
        if let Err(error) = self.root.meter.admit_precharged() {
            return Err(self.reject(frame, error));
        }
        if !self.root.valid_bytes(&frame) {
            return Err(self.invalid(frame));
        }
        let Some(meta) = decode_packet_meta(&frame) else {
            return Ok(None);
        };
        if meta.frag_total == 0 || meta.frag_id >= meta.frag_total {
            return Ok(None);
        }
        let key = (meta.assoc_id, meta.pkt_id);
        let total = usize::from(meta.frag_total);
        let id = usize::from(meta.frag_id);
        let data = frame.slice_ref(meta.data);
        // Original single-packet passthrough never reads or mutates a partial.
        // Oversized input still validates and retires its own existing key.
        if total == 1 && data.len() <= MAX_IPV4_UDP_PAYLOAD_BYTES {
            return Ok(Some((key.0, data)));
        }
        // One selected-packet preflight covers every replacement, completion
        // and removal. Unknown backing cannot be erased by a later mutation.
        let selection = match self.root.select(key) {
            Ok(selection) => selection,
            Err(()) => return Err(self.invalid(frame)),
        };
        let slot = selection.map(|selected| selected.slot);
        if data.len() > MAX_IPV4_UDP_PAYLOAD_BYTES {
            if let Some(selected) = selection {
                self.root.remove_at(selected.at);
            }
            return Ok(None);
        }
        let old = slot.filter(|&slot| self.root.slots[slot].total == total);
        let (bytes, received) = if let Some(slot) = old {
            let partial = &self.root.slots[slot];
            let replaced = partial.fragments[id].as_ref().map_or(0, Bytes::len);
            let Some(bytes) = partial
                .bytes
                .checked_sub(replaced)
                .and_then(|n| n.checked_add(data.len()))
            else {
                return Err(self.invalid(frame));
            };
            let Some(received) = partial
                .received
                .checked_add(usize::from(partial.fragments[id].is_none()))
            else {
                return Err(self.invalid(frame));
            };
            (bytes, received)
        } else {
            (data.len(), 1)
        };
        if bytes > MAX_IPV4_UDP_PAYLOAD_BYTES {
            if let Some(selected) = selection {
                self.root.remove_at(selected.at);
            }
            return Ok(None);
        }
        if received > total {
            return Err(self.invalid(frame));
        }
        if received == total {
            // No old slot, key, LRU timestamp or accounting changes until the
            // complete output has its real allowance and every copy succeeds.
            let removal = selection.map(|selected| selected.at);
            let output = match self.root.assemble(old, total, id, &data, bytes) {
                Ok(output) => output,
                Err(error) => return Err(self.reject(frame, error)),
            };
            if let Some(at) = removal {
                self.root.remove_at(at);
            }
            return Ok(Some((key.0, output)));
        }
        let slot = if let Some(slot) = old {
            slot
        } else {
            let plan = match self.root.plan_insert(key, selection) {
                Ok(plan) => plan,
                Err(()) => return Err(self.invalid(frame)),
            };
            self.root.insert(plan, key, total, now)
        };
        let partial = &mut self.root.slots[slot];
        partial.fragments[id] = Some(data);
        partial.bytes = bytes;
        partial.received = received;
        Ok(None)
    }
    fn audit(&self) -> bool {
        let valid = Self::layout_bytes().is_ok_and(|size| {
            self.root
                .meter
                .cold_layout_matches(self.stock.as_ref(), size)
        }) && self.root.audit();
        if !valid {
            self.root.meter.quarantine();
        }
        valid
    }
    pub(super) fn retire_generation(&mut self) -> bool {
        if self.root.meter.is_quarantined() || !self.audit() {
            return false;
        }
        for slot in &mut self.root.slots {
            slot.clear();
        }
        self.root.index.fill(EMPTY);
        self.root.len = 0;
        true
    }
    pub(super) fn sweep(&mut self, now: u64, ttl: u64) -> bool {
        if self.root.meter.is_quarantined() || !self.audit() {
            return false;
        }
        for slot in 0..CAPACITY {
            let partial = &self.root.slots[slot];
            if partial.key.is_some() && now.saturating_sub(partial.first_seen) > ttl {
                if !self.root.remove(slot) {
                    return false;
                }
            }
        }
        true
    }
}
impl Root {
    fn find(&self, key: Key) -> Result<Option<usize>, ()> {
        let index = self.index.get(..self.len).ok_or(())?;
        match index.binary_search_by_key(&key, |entry| entry.key) {
            Ok(at) => {
                let slot = index[at].slot;
                if self
                    .slots
                    .get(slot)
                    .is_none_or(|partial| partial.key != Some(key))
                {
                    return Err(());
                }
                Ok(Some(slot))
            }
            Err(_) => Ok(None),
        }
    }
    fn select(&self, key: Key) -> Result<Option<Selection>, ()> {
        self.find(key)?
            .map(|slot| self.removal(slot).map(|at| Selection { slot, at }))
            .transpose()
    }
    fn removal(&self, slot: usize) -> Result<usize, ()> {
        let partial = self.slots.get(slot).ok_or(())?;
        if !self.valid_partial(partial) {
            return Err(());
        }
        let key = partial.key.ok_or(())?;
        let index = self.index.get(..self.len).ok_or(())?;
        let at = index
            .binary_search_by_key(&key, |entry| entry.key)
            .map_err(|_| ())?;
        if index[at].slot != slot || index.iter().filter(|entry| entry.slot == slot).count() != 1 {
            return Err(());
        }
        Ok(at)
    }
    fn remove_at(&mut self, at: usize) {
        let slot = self.index[at].slot;
        self.index.copy_within(at + 1..self.len, at);
        self.len -= 1;
        self.index[self.len] = EMPTY;
        self.slots[slot].clear();
    }
    fn remove(&mut self, slot: usize) -> bool {
        let Ok(at) = self.removal(slot) else {
            self.meter.quarantine();
            return false;
        };
        self.remove_at(at);
        true
    }
    fn plan_insert(&self, key: Key, old: Option<Selection>) -> Result<InsertPlan, ()> {
        let index = self.index.get(..self.len).ok_or(())?;
        let (slot, remove) = if let Some(selected) = old {
            (selected.slot, Some(selected.at))
        } else if self.len == CAPACITY {
            let mut victim = None;
            for entry in index {
                let partial = self.slots.get(entry.slot).ok_or(())?;
                if partial.key != Some(entry.key) {
                    return Err(());
                }
                let candidate = (partial.first_seen, entry.key, entry.slot);
                if victim.is_none_or(|current| candidate < current) {
                    victim = Some(candidate);
                }
            }
            let slot = victim.ok_or(())?.2;
            (slot, Some(self.removal(slot)?))
        } else {
            let slot = self
                .slots
                .iter()
                .position(|partial| partial.key.is_none())
                .ok_or(())?;
            let partial = &self.slots[slot];
            if !self.valid_partial(partial) || index.iter().any(|entry| entry.slot == slot) {
                return Err(());
            }
            (slot, None)
        };
        let at = match index.binary_search_by_key(&key, |entry| entry.key) {
            Ok(at) if remove == Some(at) => at,
            Ok(_) => return Err(()),
            Err(at) => at - usize::from(remove.is_some_and(|removed| removed < at)),
        };
        let remaining = self
            .len
            .checked_sub(usize::from(remove.is_some()))
            .ok_or(())?;
        if remaining >= CAPACITY || at > remaining {
            return Err(());
        }
        Ok(InsertPlan { slot, remove, at })
    }
    fn insert(&mut self, plan: InsertPlan, key: Key, total: usize, now: u64) -> usize {
        // Unique owner, no callbacks/await: every index/slot check precedes this
        // fixed commit. There is no fallible insertion after an old value drops.
        if let Some(at) = plan.remove {
            self.remove_at(at);
        }
        self.index.copy_within(plan.at..self.len, plan.at + 1);
        self.index[plan.at] = Entry {
            key,
            slot: plan.slot,
        };
        self.len += 1;
        let partial = &mut self.slots[plan.slot];
        partial.key = Some(key);
        partial.total = total;
        partial.first_seen = now;
        plan.slot
    }
    fn assemble(
        &self,
        slot: Option<usize>,
        total: usize,
        incoming: usize,
        data: &Bytes,
        bytes: usize,
    ) -> Result<Bytes, MeteredCopyError> {
        let fragment = |id: usize| {
            if id == incoming {
                Some(data)
            } else {
                slot.and_then(|slot| self.slots.get(slot))
                    .and_then(|partial| partial.fragments.get(id))
                    .and_then(Option::as_ref)
            }
        };
        let mut actual = 0usize;
        for id in 0..total {
            let Some(value) = fragment(id) else {
                self.meter.quarantine();
                return Err(MeteredCopyError::Quarantined);
            };
            let Some(sum) = actual.checked_add(value.len()) else {
                self.meter.quarantine();
                return Err(MeteredCopyError::Quarantined);
            };
            actual = sum;
        }
        if actual != bytes {
            self.meter.quarantine();
            return Err(MeteredCopyError::Quarantined);
        }
        if bytes == 0 {
            return Ok(Bytes::new());
        }
        let mut output = BytesMut::try_zeroed_metered(bytes, &self.meter)?;
        let mut cursor = 0;
        for id in 0..total {
            let Some(value) = fragment(id) else {
                self.meter.quarantine();
                return Err(MeteredCopyError::Quarantined);
            };
            // Immutable descriptors and the same input were just checked;
            // every nonnegative prefix is within the verified whole length.
            let end = cursor + value.len();
            output[cursor..end].copy_from_slice(value);
            cursor = end;
        }
        Ok(output.freeze())
    }
    fn valid_bytes(&self, value: &Bytes) -> bool {
        value.is_empty()
            || value
                .backing_meter()
                .is_some_and(|meter| meter.same_authority(&self.meter))
    }
    fn valid_partial(&self, partial: &Partial) -> bool {
        if partial.key.is_none() {
            return partial.total == 0
                && partial.received == 0
                && partial.bytes == 0
                && partial.first_seen == 0
                && partial.fragments.iter().all(Option::is_none);
        }
        if !(2..=FRAGMENTS).contains(&partial.total)
            || !(1..partial.total).contains(&partial.received)
        {
            return false;
        }
        let mut bytes = 0usize;
        let mut count = 0usize;
        for (id, fragment) in partial.fragments.iter().enumerate() {
            if let Some(fragment) = fragment {
                if id >= partial.total || !self.valid_bytes(fragment) {
                    return false;
                }
                let Some(sum) = bytes.checked_add(fragment.len()) else {
                    return false;
                };
                bytes = sum;
                count += 1;
            }
        }
        count == partial.received && bytes == partial.bytes && bytes <= MAX_IPV4_UDP_PAYLOAD_BYTES
    }
    fn audit(&self) -> bool {
        if self.len > CAPACITY || self.slots.len() != CAPACITY || self.slots.capacity() != CAPACITY
        {
            return false;
        }
        let mut indexed = [false; CAPACITY];
        for (at, entry) in self.index.iter().enumerate() {
            if at >= self.len {
                if *entry != EMPTY {
                    return false;
                }
                continue;
            }
            if entry.slot >= CAPACITY
                || indexed[entry.slot]
                || (at > 0 && self.index[at - 1].key >= entry.key)
                || self.slots[entry.slot].key != Some(entry.key)
            {
                return false;
            }
            indexed[entry.slot] = true;
        }
        for (slot, partial) in self.slots.iter().enumerate() {
            if indexed[slot] != partial.key.is_some() || !self.valid_partial(partial) {
                return false;
            }
        }
        true
    }
}
impl Drop for Fragments {
    fn drop(&mut self) {
        if self.root.meter.is_quarantined() || !self.audit() {
            return;
        }
        unsafe {
            ManuallyDrop::drop(&mut self.root);
            ManuallyDrop::drop(&mut self.stock);
        }
    }
}
#[cfg(test)]
mod tests;
