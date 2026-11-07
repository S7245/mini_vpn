//! Unpublished storage for the original stream tables and Recv cache. Retiring
//! a receiver moves its original Box through the original free/replenish path.
//! Reserve a packet-sized insertion margin, including tombstones: no rehash or
//! cache allocation is allowed during that path. These tables remain in the
//! existing bounded proto instance partition, not the backing byte partition.
use super::*;

pub(super) struct Prepared {
    owner: usize,
    meter: Option<BackingMeter>,
    shape: [usize; 6],
    recv: Option<FxHashMap<StreamId, Option<StreamRecv>>>,
    send: Option<FxHashMap<StreamId, Option<Box<Send>>>>,
    cache: Option<Vec<StreamRecv>>,
}
fn map<V>(old: &FxHashMap<StreamId, V>, add: usize) -> Result<Option<FxHashMap<StreamId, V>>, MeteredCopyError> {
    let needed = old.len().checked_add(add).ok_or(MeteredCopyError::TooLarge)?;
    if needed <= old.capacity() { return Ok(None); }
    let mut new = FxHashMap::default();
    new.try_reserve(needed).map_err(|_| MeteredCopyError::Allocation)?;
    Ok(Some(new))
}
fn shape(s: &StreamsState) -> [usize; 6] {
    [s.recv.len(), s.recv.capacity(), s.send.len(), s.send.capacity(), s.free_recv.len(), s.free_recv.capacity()]
}
impl Prepared {
    pub(super) fn prepare(s: &StreamsState, retirements: usize) -> Result<Option<Self>, MeteredCopyError> {
        if retirements == 0 { return Ok(None); }
        let needed = s.free_recv.len().checked_add(retirements).ok_or(MeteredCopyError::TooLarge)?;
        let cache = if needed > s.free_recv.capacity() {
            let mut cache = Vec::new();
            cache.try_reserve_exact(needed).map_err(|_| MeteredCopyError::Allocation)?;
            Some(cache)
        } else { None };
        Ok(Some(Self { owner: s as *const StreamsState as usize, shape: shape(s), meter: s.native_meter().cloned(),
            recv: map(&s.recv, retirements)?, send: map(&s.send, retirements)?, cache }))
    }
    pub(super) fn install(mut self, s: &mut StreamsState) -> Result<(), TransportError> {
        if self.owner != s as *const StreamsState as usize || self.shape != shape(s) {
            if let Some(meter) = s.native_meter() { meter.quarantine(); }
            if let Some(meter) = &self.meter { meter.quarantine(); }
            return Err(Receiver::error());
        }
        if let Some(mut new) = self.recv.take() { new.extend(s.recv.drain()); s.recv = new; }
        if let Some(mut new) = self.send.take() { new.extend(s.send.drain()); s.send = new; }
        if let Some(mut new) = self.cache.take() { new.append(&mut s.free_recv); s.free_recv = new; }
        Ok(())
    }
}
