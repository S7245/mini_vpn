//! Physical registration catalog, uniquely owned by RegistrationRoot.
//! Native storage is one exact precharged allocation: 19 Meta slots, 8 local CIDs
//! per Meta, fixed CID/reset hash tables. Legacy keeps its original containers.
//! Routing is bounded hash lookup; only cold/terminal paths enumerate inventory.
//! Entry: Store; original protocol tests and endpoint/tests own acceptance.
use super::*;
use bytes::{BackingBudgetReservation, BackingMeter};
use std::{
    alloc::{alloc, Layout},
    collections::hash_map::RandomState,
    hash::{BuildHasher, Hash},
};

const LOCAL_CIDS: usize = crate::LOC_CID_COUNT as usize;
const CID_BUCKETS: usize = 512;
const RESET_BUCKETS: usize = 64;

#[derive(Debug)]
pub(super) enum LocalCids {
    Legacy(FxHashMap<u64, ConnectionId>),
    Native([Option<(u64, ConnectionId)>; LOCAL_CIDS]),
}
impl LocalCids {
    pub(super) fn new(native: bool) -> Self {
        if native {
            Self::Native([None; LOCAL_CIDS])
        } else {
            Self::Legacy(FxHashMap::default())
        }
    }
    pub(super) fn len(&self) -> usize {
        match self {
            Self::Legacy(ids) => ids.len(),
            Self::Native(ids) => ids.iter().flatten().count(),
        }
    }
    pub(super) fn contains_key(&self, seq: &u64) -> bool {
        match self {
            Self::Legacy(ids) => ids.contains_key(seq),
            Self::Native(ids) => ids.iter().flatten().any(|(s, _)| s == seq),
        }
    }
    pub(super) fn insert(&mut self, seq: u64, cid: ConnectionId) -> bool {
        match self {
            Self::Legacy(ids) => {
                ids.insert(seq, cid);
                true
            }
            Self::Native(ids) => {
                if let Some(slot) = ids
                    .iter_mut()
                    .find(|slot| slot.is_some_and(|(s, _)| s == seq))
                {
                    *slot = Some((seq, cid));
                    return true;
                }
                if let Some(slot) = ids.iter_mut().find(|slot| slot.is_none()) {
                    *slot = Some((seq, cid));
                    true
                } else {
                    false
                }
            }
        }
    }
    pub(super) fn remove(&mut self, seq: &u64) -> Option<ConnectionId> {
        match self {
            Self::Legacy(ids) => ids.remove(seq),
            Self::Native(ids) => ids
                .iter_mut()
                .find(|slot| slot.is_some_and(|(s, _)| s == *seq))?
                .take()
                .map(|(_, cid)| cid),
        }
    }
    pub(super) fn values(&self) -> impl Iterator<Item = &ConnectionId> {
        let (legacy, native) = match self {
            Self::Legacy(ids) => (Some(ids), None),
            Self::Native(ids) => (None, Some(ids)),
        };
        legacy.into_iter().flat_map(|ids| ids.values()).chain(
            native
                .into_iter()
                .flat_map(|ids| ids.iter().flatten().map(|(_, cid)| cid)),
        )
    }
}

/// Deletion rehashes the following cluster; no tombstone growth or DATA allocation.
#[derive(Clone, Copy)]
struct PreparedCell {
    index: usize,
}
struct Table<K, V, const N: usize, S> {
    cells: [Option<(K, V)>; N],
    hasher: S,
    len: usize,
}
impl<K: Copy + Eq + Hash, V: Copy, const N: usize, S: BuildHasher> Table<K, V, N, S> {
    fn new(hasher: S) -> Self {
        Self {
            cells: [None; N],
            hasher,
            len: 0,
        }
    }
    fn home(&self, key: &K) -> usize {
        (self.hasher.hash_one(key) as usize) % N
    }
    fn find(&self, key: &K) -> Option<usize> {
        let home = self.home(key);
        for distance in 0..N {
            let index = (home + distance) % N;
            match self.cells[index] {
                Some((found, _)) if found == *key => return Some(index),
                None => return None,
                _ => {}
            }
        }
        None
    }
    fn get(&self, key: &K) -> Option<&V> {
        self.find(key)
            .and_then(|index| self.cells[index].as_ref().map(|(_, value)| value))
    }
    fn prepare(&self, key: &K, reserved: &[PreparedCell]) -> Option<PreparedCell> {
        let home = self.home(key);
        for distance in 0..N {
            let index = (home + distance) % N;
            match self.cells[index] {
                Some((found, _)) if found == *key => return None,
                None if !reserved.iter().any(|cell| cell.index == index) => {
                    return Some(PreparedCell { index })
                }
                _ => {}
            }
        }
        None
    }
    fn commit(&mut self, cell: PreparedCell, key: K, value: V) {
        self.cells[cell.index] = Some((key, value));
        self.len += 1;
    }
    fn remove(&mut self, key: &K) -> Option<V> {
        let index = self.find(key)?;
        let (_, value) = self.cells[index].take()?;
        self.len -= 1;
        let mut hole = index;
        for distance in 1..N {
            let scan = (index + distance) % N;
            let Some((key, _)) = self.cells[scan] else {
                break;
            };
            let home = self.home(&key);
            if (hole + N - home) % N < (scan + N - home) % N {
                self.cells[hole] = self.cells[scan].take();
                hole = scan;
            }
        }
        Some(value)
    }
    fn iter(&self) -> impl Iterator<Item = &(K, V)> {
        self.cells.iter().flatten()
    }
}

#[derive(Clone, Copy)]
struct Reserved {
    key: RegistrationKey,
    cid: ConnectionId,
}

struct Native {
    reserved: [Option<Reserved>; crate::BUDGETED_CONNECTION_LIMIT],
    slots: [Option<ConnectionMeta>; crate::BUDGETED_CONNECTION_LIMIT],
    cids: Table<ConnectionId, ConnectionHandle, CID_BUCKETS, rustc_hash::FxBuildHasher>,
    resets: Table<(SocketAddr, ResetToken), ConnectionHandle, RESET_BUCKETS, RandomState>,
    len: usize,
    signature: [u8; 64],
}
pub(super) struct Fixed {
    data: Box<Native>,
    _allowance: BackingBudgetReservation,
    meter: BackingMeter,
}
impl fmt::Debug for Fixed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FixedRegistry")
            .field("connections", &self.data.len)
            .field("cids", &self.data.cids.len)
            .field("resets", &self.data.resets.len)
            .finish()
    }
}
impl Fixed {
    fn prepare(meter: &BackingMeter) -> Result<Self, ()> {
        let layout = Layout::new::<Native>();
        let allowance = meter
            .reserve_cold_layout(layout.size())
            .map_err(|_| ())?
            .ok_or(())?;
        let reset_hasher = RandomState::new();
        let ptr = unsafe { alloc(layout).cast::<Native>() };
        if ptr.is_null() {
            return Err(());
        }
        unsafe {
            ptr.write(Native {
                reserved: [None; crate::BUDGETED_CONNECTION_LIMIT],
                slots: std::array::from_fn(|_| None),
                cids: Table::new(rustc_hash::FxBuildHasher),
                resets: Table::new(reset_hasher),
                len: 0,
                signature: [0; 64],
            });
        }
        Ok(Self {
            data: unsafe { Box::from_raw(ptr) },
            _allowance: allowance,
            meter: meter.clone(),
        })
    }
}

#[derive(Debug)]
pub(super) enum Store {
    Legacy {
        index: ConnectionIndex,
        connections: Slab<ConnectionMeta>,
    },
    Native(Fixed),
    Unavailable,
}
impl Store {
    pub(super) const fn native_layout_bytes() -> usize {
        Layout::new::<Native>().size()
    }
    pub(super) fn new(native: bool, meter: Option<&BackingMeter>) -> Self {
        if !native {
            return Self::Legacy {
                index: ConnectionIndex::default(),
                connections: Slab::new(),
            };
        }
        match meter.and_then(|meter| Fixed::prepare(meter).ok()) {
            Some(fixed) => Self::Native(fixed),
            None => Self::Unavailable,
        }
    }
    pub(super) fn reset_token(
        &mut self,
        key: &dyn crypto::HmacKey,
        id: ConnectionId,
    ) -> Option<ResetToken> {
        let Self::Native(f) = self else {
            return None;
        };
        if f.meter.admit_precharged().is_err() {
            return None;
        }
        let guard = RegistrationGuard {
            meter: Some(f.meter.clone()),
            armed: true,
        };
        let token = ResetToken::new_bounded(key, id, &mut f.data.signature);
        guard.finish();
        token
    }
    pub(super) fn ready(&self) -> bool {
        !matches!(self, Self::Unavailable)
    }
    pub(super) fn len(&self) -> usize {
        match self {
            Self::Legacy { connections, .. } => connections.len(),
            Self::Native(f) => f.data.len,
            Self::Unavailable => 0,
        }
    }
    pub(super) fn occupied_len(&self) -> usize {
        match self {
            Self::Native(f) => f.data.len + f.data.reserved.iter().flatten().count(),
            _ => self.len(),
        }
    }
    pub(super) fn get(&self, slot: usize) -> Option<&ConnectionMeta> {
        match self {
            Self::Legacy { connections, .. } => connections.get(slot),
            Self::Native(f) => f.data.slots.get(slot)?.as_ref(),
            Self::Unavailable => None,
        }
    }
    pub(super) fn get_mut(&mut self, slot: usize) -> Option<&mut ConnectionMeta> {
        match self {
            Self::Legacy { connections, .. } => connections.get_mut(slot),
            Self::Native(f) => f.data.slots.get_mut(slot)?.as_mut(),
            Self::Unavailable => None,
        }
    }
    pub(super) fn vacant_key(&self, available: impl Fn(ConnectionHandle) -> bool) -> Option<usize> {
        match self {
            Self::Legacy { connections, .. } => Some(connections.vacant_key()),
            Self::Native(f) => f
                .data
                .slots
                .iter()
                .enumerate()
                .find(|(index, meta)| {
                    meta.is_none()
                        && f.data.reserved[*index].is_none()
                        && available(ConnectionHandle(*index))
                })
                .map(|(index, _)| index),
            Self::Unavailable => None,
        }
    }
    pub(super) fn prepared_key(&self, ch: ConnectionHandle) -> Option<RegistrationKey> {
        let Self::Native(f) = self else {
            return None;
        };
        f.data.reserved.get(ch.0)?.as_ref().map(|item| item.key)
    }
    pub(super) fn reserve(&mut self, key: RegistrationKey, cid: ConnectionId) -> bool {
        if self.contains_cid(&cid) || cid.len() != 8 {
            return false;
        }
        let Self::Native(f) = self else {
            return false;
        };
        let Some(slot) = f.data.reserved.get_mut(key.handle().0) else {
            return false;
        };
        if slot.is_some() || f.data.slots[key.handle().0].is_some() {
            return false;
        }
        *slot = Some(Reserved { key, cid });
        true
    }
    pub(super) fn reserved(&self, key: RegistrationKey, cid: ConnectionId) -> bool {
        let Self::Native(f) = self else {
            return false;
        };
        f.data
            .reserved
            .get(key.handle().0)
            .is_some_and(|slot| slot.is_some_and(|item| item.key == key && item.cid == cid))
    }
    pub(super) fn cancel_reserved(&mut self, key: RegistrationKey) {
        if let Self::Native(f) = self {
            f.data.reserved[key.handle().0] = None;
        }
    }
    pub(super) fn publish_prepared(
        &mut self,
        key: RegistrationKey,
        cid: ConnectionId,
        meta: &mut Option<ConnectionMeta>,
    ) -> bool {
        if !self.reserved(key, cid) {
            return false;
        }
        let Self::Native(f) = self else {
            return false;
        };
        let Some(value) = meta.as_ref() else {
            return false;
        };
        if f.data.slots[key.handle().0].is_some()
            || value.registration_key != Some(key)
            || value.side != Side::Client
            || value.reset_token.is_some()
            || value.loc_cids.len() != 1
            || !value.loc_cids.contains_key(&0)
            || !value.loc_cids.values().any(|value| *value == cid)
        {
            return false;
        }
        let Some(cell) = f.data.cids.prepare(&cid, &[]) else {
            return false;
        };
        f.data.cids.commit(cell, cid, key.handle());
        f.data.slots[key.handle().0] = meta.take();
        f.data.reserved[key.handle().0] = None;
        f.data.len += 1;
        true
    }
    pub(super) fn contains_cid(&self, cid: &ConnectionId) -> bool {
        self.cid_owner(cid).is_some()
            || matches!(self, Self::Native(f)
            if f.data.reserved.iter().flatten().any(|item| &item.cid == cid))
    }
    pub(super) fn cid_owner(&self, cid: &ConnectionId) -> Option<&ConnectionHandle> {
        match self {
            Self::Legacy { index, .. } => index.connection_ids.get(cid),
            Self::Native(f) => f.data.cids.get(cid),
            Self::Unavailable => None,
        }
    }
    pub(super) fn insert_cid(&mut self, cid: ConnectionId, ch: ConnectionHandle) -> bool {
        match self {
            Self::Legacy { index, .. } => {
                index.connection_ids.insert(cid, ch);
                true
            }
            Self::Native(_) => false,
            Self::Unavailable => false,
        }
    }
    pub(super) fn retire_cid(&mut self, cid: ConnectionId) {
        match self {
            Self::Legacy { index, .. } => index.retire(cid),
            Self::Native(f) => {
                f.data.cids.remove(&cid);
            }
            Self::Unavailable => {}
        }
    }
    pub(super) fn reset_owner(
        &self,
        remote: SocketAddr,
        token: &[u8],
    ) -> Option<&ConnectionHandle> {
        match self {
            Self::Legacy { index, .. } => index.connection_reset_tokens.get(remote, token),
            Self::Native(f) => f.data.resets.get(&(
                remote,
                ResetToken::from(<[u8; RESET_TOKEN_SIZE]>::try_from(token).ok()?),
            )),
            Self::Unavailable => None,
        }
    }
    pub(super) fn replace_reset(
        &mut self,
        ch: ConnectionHandle,
        remote: SocketAddr,
        token: ResetToken,
    ) -> bool {
        match self {
            Self::Legacy { index, connections } => {
                let Some(meta) = connections.get_mut(ch.0) else {
                    return false;
                };
                if let Some(old) = meta.reset_token.replace((remote, token)) {
                    index.connection_reset_tokens.remove(old.0, old.1);
                }
                index.connection_reset_tokens.insert(remote, token, ch);
                true
            }
            Self::Native(f) => {
                let Some(meta) = f.data.slots.get_mut(ch.0).and_then(Option::as_mut) else {
                    return false;
                };
                let new = (remote, token);
                if meta.reset_token == Some(new) {
                    return true;
                }
                let Some(cell) = f.data.resets.prepare(&new, &[]) else {
                    return false;
                };
                f.data.resets.commit(cell, new, ch);
                if let Some(old) = meta.reset_token {
                    f.data.resets.remove(&old);
                }
                meta.reset_token = Some(new);
                true
            }
            Self::Unavailable => false,
        }
    }
    pub(super) fn insert(
        &mut self,
        ch: ConnectionHandle,
        meta: ConnectionMeta,
        loc_cid: ConnectionId,
    ) -> bool {
        match self {
            Self::Legacy { index, connections } => {
                let addresses = meta.addresses;
                let side = meta.side;
                let id = connections.insert(meta);
                index.insert_conn(addresses, loc_cid, ch, side);
                id == ch.0
            }
            Self::Native(f) => {
                let Some(slot) = f.data.slots.get_mut(ch.0) else {
                    return false;
                };
                let LocalCids::Native(local) = &meta.loc_cids else {
                    return false;
                };
                if slot.is_some()
                    || meta.side != Side::Client
                    || meta.loc_cids.len() != 1
                    || !meta.loc_cids.values().any(|cid| *cid == loc_cid)
                    || !meta.registration_key.is_some_and(|key| key.handle() == ch)
                {
                    return false;
                }
                let mut cells = [PreparedCell { index: 0 }; LOCAL_CIDS];
                let mut count = 0;
                for (_, cid) in local.iter().flatten() {
                    if cid.len() != 8 {
                        return false;
                    }
                    let Some(cell) = f.data.cids.prepare(cid, &cells[..count]) else {
                        return false;
                    };
                    cells[count] = cell;
                    count += 1;
                }
                for ((_, cid), cell) in local.iter().flatten().zip(cells) {
                    f.data.cids.commit(cell, *cid, ch);
                }
                *slot = Some(meta);
                f.data.len += 1;
                true
            }
            Self::Unavailable => false,
        }
    }
    pub(super) fn commit_identifiers(
        &mut self,
        ch: ConnectionHandle,
        retire: Option<u64>,
        sequence: u64,
        ids: &[IssuedCid],
    ) -> bool {
        let Self::Native(f) = self else {
            return false;
        };
        let Some(meta) = f.data.slots.get_mut(ch.0).and_then(Option::as_mut) else {
            return false;
        };
        let LocalCids::Native(local) = &mut meta.loc_cids else {
            return false;
        };
        let retiring = retire.and_then(|seq| {
            local
                .iter()
                .position(|slot| slot.is_some_and(|(old, _)| old == seq))
        });
        let count = local.iter().flatten().count();
        if meta.cids_issued != sequence
            || ids.len() > LOCAL_CIDS
            || count - usize::from(retiring.is_some()) + ids.len() > LOCAL_CIDS
            || ids.iter().enumerate().any(|(index, cid)| {
                cid.id.len() != 8
                    || cid.sequence != sequence + index as u64
                    || ids[..index].iter().any(|prior| prior.id == cid.id)
            })
        {
            return false;
        }
        let mut cells = [PreparedCell { index: 0 }; LOCAL_CIDS];
        let mut local_cells = [0; LOCAL_CIDS];
        let mut available = 0;
        for (index, slot) in local.iter().enumerate() {
            if slot.is_none() || retiring == Some(index) {
                local_cells[available] = index;
                available += 1;
            }
        }
        for (index, cid) in ids.iter().enumerate() {
            let Some(cell) = f.data.cids.prepare(&cid.id, &cells[..index]) else {
                return false;
            };
            cells[index] = cell;
        }
        // No allocation, provider call, Pool lock, or fallible capacity decision follows.
        for (cid, cell) in ids.iter().zip(cells) {
            f.data.cids.commit(cell, cid.id, ch);
        }
        if let Some(index) = retiring {
            if let Some((_, cid)) = local[index].take() {
                f.data.cids.remove(&cid);
            }
        }
        for (cid, index) in ids.iter().zip(local_cells) {
            local[index] = Some((cid.sequence, cid.id));
        }
        meta.cids_issued = sequence + ids.len() as u64;
        true
    }
    pub(super) fn remove(&mut self, ch: ConnectionHandle) -> Option<ConnectionMeta> {
        match self {
            Self::Legacy { index, connections } => {
                let meta = connections.try_remove(ch.0)?;
                index.remove(&meta);
                Some(meta)
            }
            Self::Native(f) => {
                let meta = f.data.slots.get_mut(ch.0)?.take()?;
                for cid in meta.loc_cids.values() {
                    f.data.cids.remove(cid);
                }
                if let Some(reset) = meta.reset_token {
                    f.data.resets.remove(&reset);
                }
                f.data.len -= 1;
                Some(meta)
            }
            Self::Unavailable => None,
        }
    }
    pub(super) fn route(
        &self,
        addresses: &FourTuple,
        packet: &PartialDecode,
    ) -> Option<RouteDatagramTo> {
        match self {
            Self::Legacy { index, .. } => index.get(addresses, packet),
            Self::Native(f) => {
                if let Some(ch) = f.data.cids.get(packet.dst_cid()) {
                    return Some(RouteDatagramTo::Connection(*ch));
                }
                let data = packet.data();
                if data.len() < RESET_TOKEN_SIZE {
                    return None;
                }
                self.reset_owner(addresses.remote, &data[data.len() - RESET_TOKEN_SIZE..])
                    .copied()
                    .map(RouteDatagramTo::Connection)
            }
            Self::Unavailable => None,
        }
    }
    pub(super) fn initial_incoming(&mut self, cid: ConnectionId, key: usize) {
        if let Self::Legacy { index, .. } = self {
            index.insert_initial_incoming(cid, key);
        }
    }
    pub(super) fn remove_initial(&mut self, cid: ConnectionId) {
        if let Self::Legacy { index, .. } = self {
            index.remove_initial(cid);
        }
    }
    pub(super) fn insert_initial(&mut self, cid: ConnectionId, ch: ConnectionHandle) {
        if let Self::Legacy { index, .. } = self {
            index.insert_initial(cid, ch);
        }
    }
    pub(super) fn cid_count(&self) -> usize {
        match self {
            Self::Legacy { index, .. } => index.connection_ids.len(),
            Self::Native(f) => f.data.cids.len,
            Self::Unavailable => 0,
        }
    }
    pub(super) fn iter(&self) -> impl Iterator<Item = (usize, &ConnectionMeta)> {
        let (legacy, native) = match self {
            Self::Legacy { connections, .. } => (Some(connections), None),
            Self::Native(f) => (None, Some(&f.data.slots)),
            Self::Unavailable => (None, None),
        };
        legacy
            .into_iter()
            .flat_map(|connections| connections.iter())
            .chain(native.into_iter().flat_map(|slots| {
                slots
                    .iter()
                    .enumerate()
                    .filter_map(|(slot, meta)| meta.as_ref().map(|meta| (slot, meta)))
            }))
    }
    pub(super) fn inventory_valid(&self) -> bool {
        let Self::Native(f) = self else {
            return !matches!(self, Self::Unavailable);
        };
        for (index, reserved) in f.data.reserved.iter().enumerate() {
            if let Some(item) = reserved {
                if item.key.handle().0 != index
                    || item.cid.len() != 8
                    || f.data.slots[index].is_some()
                    || f.data.cids.get(&item.cid).is_some()
                    || f.data.reserved[..index]
                        .iter()
                        .flatten()
                        .any(|other| other.cid == item.cid)
                {
                    return false;
                }
            }
        }
        let occupied = f.data.slots.iter().flatten().count();
        if occupied != f.data.len
            || f.data.cids.iter().count() != f.data.cids.len
            || f.data.resets.iter().count() != f.data.resets.len
        {
            return false;
        }
        let mut cid_sum = 0;
        let mut reset_sum = 0;
        for (slot, meta) in f
            .data
            .slots
            .iter()
            .enumerate()
            .filter_map(|(slot, meta)| meta.as_ref().map(|meta| (slot, meta)))
        {
            let ch = ConnectionHandle(slot);
            let LocalCids::Native(local) = &meta.loc_cids else {
                return false;
            };
            if meta.side != Side::Client
                || meta.cids_issued == 0
                || meta.cids_issued > u64::from(crate::VarInt::MAX) + 1
                || meta.cid_frames.is_none()
                || !meta.registration_key.is_some_and(|key| key.handle() == ch)
            {
                return false;
            }
            for (index, entry) in local.iter().enumerate() {
                if let Some((seq, cid)) = entry {
                    cid_sum += 1;
                    if cid.len() != 8
                        || *seq >= meta.cids_issued
                        || *seq > u64::from(crate::VarInt::MAX)
                        || f.data.cids.get(cid) != Some(&ch)
                        || local[..index]
                            .iter()
                            .flatten()
                            .any(|(old_seq, old_cid)| old_seq == seq || old_cid == cid)
                    {
                        return false;
                    }
                }
            }
            if let Some(reset) = meta.reset_token {
                reset_sum += 1;
                if f.data.resets.get(&reset) != Some(&ch) {
                    return false;
                }
            }
        }
        if cid_sum != f.data.cids.len
            || reset_sum != f.data.resets.len
            || reset_sum > occupied
            || cid_sum > occupied * LOCAL_CIDS
        {
            return false;
        }
        if f.data.cids.iter().any(|(cid, ch)| {
            !self
                .get(ch.0)
                .is_some_and(|meta| meta.loc_cids.values().any(|value| value == cid))
        }) {
            return false;
        }
        if f.data.resets.iter().any(|(reset, ch)| {
            !self
                .get(ch.0)
                .is_some_and(|meta| meta.reset_token == Some(*reset))
        }) {
            return false;
        }
        true
    }
}

#[cfg(test)]
mod tests;
