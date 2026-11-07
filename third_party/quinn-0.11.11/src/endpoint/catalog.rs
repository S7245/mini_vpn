//! Host half of Endpoint registration: sender, terminal receiver and sealed key.
//! Hard profiles own one exact precharged 19-slot allocation, with mutually
//! exclusive vacant/prepared/live states. DATA lookup is direct by physical slot;
//! only control and lifecycle paths enumerate. Proto owns CID/reset routing.
//! Entry: Catalog reservation/publication/removal; tests: construction and terminal19.
use super::*;
use std::alloc::alloc;

type Terminal = proto::owned_events::Receiver<ConnectionToEndpointEvent>;
#[derive(Debug)]
pub(super) struct Entry {
    pub(super) handle: ConnectionHandle,
    pub(super) key: Option<proto::RegistrationKey>,
    pub(super) sender: ForwardSender,
    pub(super) terminal: Option<Terminal>,
    pub(super) previous_socket: Option<std::num::NonZeroU64>,
}
#[derive(Debug)]
enum Slot {
    Vacant,
    Prepared(proto::RegistrationKey),
    Live(Entry),
}
#[derive(Debug)]
pub(super) struct Fixed {
    slots: Box<[Slot; BUDGETED_CONNECTION_LIMIT]>,
    _allowance: BackingBudgetReservation,
    meter: BackingMeter,
}
impl Drop for Fixed {
    fn drop(&mut self) {
        if self.slots.iter().any(|slot| !matches!(slot, Slot::Vacant)) {
            self.meter.quarantine();
        }
    }
}
#[derive(Debug)]
pub(super) enum Catalog {
    Legacy(FxHashMap<ConnectionHandle, Entry>),
    Native(Fixed),
}
impl Catalog {
    pub(super) fn prepare(meter: Option<&BackingMeter>) -> io::Result<Self> {
        let Some(meter) = meter.filter(|meter| meter.budget_limit().is_some()) else {
            return Ok(Self::Legacy(FxHashMap::default()));
        };
        let layout = Layout::new::<[Slot; BUDGETED_CONNECTION_LIMIT]>();
        let allowance = meter
            .reserve_cold_layout(layout.size())
            .map_err(|error| io::Error::other(format!("host catalog admission {error:?}")))?
            .ok_or_else(|| io::Error::other("host catalog requires hard authority"))?;
        let ptr = unsafe { alloc(layout).cast::<Slot>() };
        if ptr.is_null() {
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "host catalog allocation",
            ));
        }
        for index in 0..BUDGETED_CONNECTION_LIMIT {
            unsafe {
                ptr.add(index).write(Slot::Vacant);
            }
        }
        Ok(Self::Native(Fixed {
            slots: unsafe { Box::from_raw(ptr.cast::<[Slot; BUDGETED_CONNECTION_LIMIT]>()) },
            _allowance: allowance,
            meter: meter.clone(),
        }))
    }
    pub(super) fn entries(&self) -> impl Iterator<Item = &Entry> {
        let (legacy, fixed) = match self {
            Self::Legacy(map) => (Some(map), None),
            Self::Native(fixed) => (None, Some(&fixed.slots)),
        };
        legacy
            .into_iter()
            .flat_map(|map| map.values())
            .chain(fixed.into_iter().flat_map(|slots| {
                slots.iter().filter_map(|slot| match slot {
                    Slot::Live(entry) => Some(entry),
                    _ => None,
                })
            }))
    }
    pub(super) fn len(&self) -> usize {
        self.entries().count()
    }
    pub(super) fn occupied_len(&self) -> usize {
        match self {
            Self::Legacy(map) => map.len(),
            Self::Native(f) => f
                .slots
                .iter()
                .filter(|slot| !matches!(slot, Slot::Vacant))
                .count(),
        }
    }
    pub(super) fn is_empty(&self) -> bool {
        self.occupied_len() == 0
    }
    pub(super) fn get(&self, ch: &ConnectionHandle) -> Option<&ForwardSender> {
        match self {
            Self::Legacy(map) => map.get(ch).map(|entry| &entry.sender),
            Self::Native(f) => match f.slots.get(ch.0)? {
                Slot::Live(entry) if entry.handle == *ch => Some(&entry.sender),
                _ => None,
            },
        }
    }
    pub(super) fn contains_key(&self, ch: &ConnectionHandle) -> bool {
        self.get(ch).is_some()
    }
    pub(super) fn values(&self) -> impl Iterator<Item = &ForwardSender> {
        self.entries().map(|entry| &entry.sender)
    }
    pub(super) fn keys(&self) -> impl Iterator<Item = &ConnectionHandle> {
        self.entries().map(|entry| &entry.handle)
    }
    pub(super) fn reserve(&mut self, key: proto::RegistrationKey) -> bool {
        let Self::Native(f) = self else {
            return false;
        };
        if f.meter.admit_precharged().is_err() {
            return false;
        }
        let Some(slot) = f.slots.get_mut(key.handle().0) else {
            return false;
        };
        if !matches!(slot, Slot::Vacant) {
            return false;
        }
        *slot = Slot::Prepared(key);
        true
    }
    pub(super) fn can_publish(&self, entry: &Entry) -> bool {
        match self {
            Self::Legacy(map) => !map.contains_key(&entry.handle),
            Self::Native(f) => {
                f.slots.get(entry.handle.0).is_some_and(
                    |slot| matches!(slot, Slot::Prepared(key) if Some(*key) == entry.key),
                ) && entry.terminal.is_some()
                    && f.meter.admit_precharged().is_ok()
            }
        }
    }
    /// Caller has validated this exact sealed Entry before Proto publication.
    pub(super) fn publish(&mut self, entry: Entry) {
        match self {
            Self::Legacy(map) => {
                map.insert(entry.handle, entry);
            }
            Self::Native(f) => {
                let index = entry.handle.0;
                f.slots[index] = Slot::Live(entry);
            }
        }
    }
    pub(super) fn cancel(&mut self, key: proto::RegistrationKey) -> bool {
        let Self::Native(f) = self else {
            return false;
        };
        let Some(slot) = f.slots.get_mut(key.handle().0) else {
            return false;
        };
        if !matches!(slot, Slot::Prepared(current) if *current == key) {
            return false;
        }
        *slot = Slot::Vacant;
        true
    }
    pub(super) fn remove(
        &mut self,
        ch: &ConnectionHandle,
        key: Option<proto::RegistrationKey>,
    ) -> bool {
        match self {
            Self::Legacy(map) => map.remove(ch).is_some(),
            Self::Native(f) => {
                let Some(slot) = f.slots.get_mut(ch.0) else {
                    return false;
                };
                if !matches!(slot, Slot::Live(entry) if entry.handle == *ch && entry.key == key) {
                    return false;
                }
                // Both actual channel owners are destroyed before ledger completion/ACK.
                *slot = Slot::Vacant;
                true
            }
        }
    }
    pub(super) fn poll_terminal(
        &mut self,
        index: usize,
        cx: &mut Context<'_>,
    ) -> Option<(ConnectionHandle, ConnectionToEndpointEvent)> {
        let Self::Native(f) = self else {
            return None;
        };
        let Slot::Live(entry) = f.slots.get_mut(index)? else {
            return None;
        };
        let Poll::Ready(Some(event)) = entry.terminal.as_mut()?.poll_recv(cx) else {
            return None;
        };
        Some((entry.handle, event))
    }
    pub(super) fn matches(&self, endpoint: &proto::Endpoint) -> bool {
        if !endpoint.native_registration() {
            return true;
        }
        let Self::Native(f) = self else {
            return false;
        };
        endpoint.registration_catalog_matches(|ch, live, prepared| match &f.slots[ch.0] {
            Slot::Vacant => live.is_none() && prepared.is_none(),
            Slot::Prepared(key) => live.is_none() && prepared == Some(*key) && key.handle() == ch,
            Slot::Live(entry) => {
                prepared.is_none()
                    && live == entry.key
                    && live.is_some()
                    && entry.handle == ch
                    && entry.terminal.is_some()
            }
        })
    }
    pub(super) fn is_native(&self) -> bool {
        matches!(self, Self::Native(_))
    }
    pub(super) fn previous_inventory_valid(&self, generation: u64) -> bool {
        self.entries().all(|entry| {
            entry
                .previous_socket
                .is_none_or(|pending| pending.get() == generation && entry.key.is_some())
        })
    }
    pub(super) fn has_previous_pending(&self) -> bool {
        self.entries().any(|entry| entry.previous_socket.is_some())
    }
    pub(super) fn begin_rebind(&mut self, generation: std::num::NonZeroU64) {
        if let Self::Native(f) = self {
            for slot in f.slots.iter_mut() {
                if let Slot::Live(entry) = slot {
                    entry.previous_socket = Some(generation);
                }
            }
        }
    }
    pub(super) fn clear_previous_pending(&mut self) {
        if let Self::Native(f) = self {
            for slot in f.slots.iter_mut() {
                if let Slot::Live(entry) = slot {
                    entry.previous_socket = None;
                }
            }
        }
    }
    pub(super) fn note_authenticated_socket(
        &mut self,
        ch: ConnectionHandle,
        key: Option<proto::RegistrationKey>,
        generation: u64,
    ) {
        if let Self::Native(f) = self {
            if let Some(Slot::Live(entry)) = f.slots.get_mut(ch.0) {
                if entry.handle == ch
                    && entry.key == key
                    && entry
                        .previous_socket
                        .is_some_and(|pending| pending.get() == generation)
                {
                    entry.previous_socket = None;
                }
            }
        }
    }
    pub(super) fn clear(&mut self) {
        match self {
            Self::Legacy(map) => map.clear(),
            Self::Native(f) => {
                for slot in f.slots.iter_mut() {
                    *slot = Slot::Vacant;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
