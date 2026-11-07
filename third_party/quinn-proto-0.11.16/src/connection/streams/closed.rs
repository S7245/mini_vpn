//! Physical half retirement for the original Native Connection after known
//! protocol close. Quinn calls only from the actual unique half's Drop. This
//! owner validates original maps/buffers/counters, drops the actual allocation,
//! then removes its key. No ACK, MAX credit, replacement map or free_recv cache.
//! Q/unwind keeps the map present; witnesses continue to inspect actual maps.
//! Tests use real Quinn half Drop and the original staged CONNECT caller.
use super::*;
use bytes::{BackingMeter, MeteredCopyError};

impl StreamOwner<'_> {
    fn closed_meter(&mut self) -> Result<Option<BackingMeter>, MeteredCopyError> {
        let Self::Connection { connection, .. } = self else { return Ok(None); };
        let Some(meter) = connection.streams.native_meter().cloned() else { return Ok(None); };
        if std::thread::panicking() { meter.quarantine(); }
        meter.admit_precharged()?;
        Ok(connection.state.is_closed().then_some(meter))
    }
    fn retire_closed(&mut self, id: StreamId, half: StreamHalf) -> Result<bool, MeteredCopyError> {
        let meter = match self.closed_meter() {
            Ok(Some(meter)) => meter,
            Ok(None) => return Ok(false),
            Err(error) => { self.fail("stream backing authority is quarantined"); return Err(error); }
        };
        let result = self.parts().0.retire_closed_half(id, half, &meter);
        if result.is_err() {
            meter.quarantine();
            self.fail("closed stream physical inventory invalid");
        }
        result.map(|()| true)
    }
}
impl SendStream<'_> {
    /// Fork-only physical disposition, called by Quinn's actual SendStream Drop.
    /// `false` preserves the existing open/Legacy behavior. Q never grants absence.
    #[doc(hidden)]
    pub fn retire_closed(&mut self) -> Result<bool, MeteredCopyError> {
        self.owner.retire_closed(self.id, StreamHalf::Send)
    }
}
impl RecvStream<'_> {
    /// Fork-only physical disposition, called by Quinn's actual RecvStream Drop.
    #[doc(hidden)]
    pub fn retire_closed(&mut self) -> Result<bool, MeteredCopyError> {
        self.owner.retire_closed(self.id, StreamHalf::Recv)
    }
}
impl StreamsState {
    fn closed_inventory_matches(&self, meter: &BackingMeter) -> bool {
        if self.prepare_operations(0, 0, 0).is_err() { return false; }
        let opened_send = self.send.keys().filter(|id|
            id.initiator() == self.side || id.index() < self.next_reported_remote[id.dir() as usize]).count();
        let demand = self.send.values().flatten().filter(|s| s.transport_write_demand_through.is_some()).count();
        let unacked = self.send.values().flatten().try_fold(0u64, |n, s|
            n.checked_add(if matches!(s.state, SendState::ResetSent) { 0 } else { s.pending.unacked() }));
        let mut remote = [0u64; 2];
        for id in self.recv.keys().chain(self.send.keys().filter(|id| !self.recv.contains_key(id))) {
            if id.initiator() != self.side {
                let Some(next) = remote[id.dir() as usize].checked_add(1) else { return false; };
                remote[id.dir() as usize] = next;
            }
        }
        opened_send == self.send_streams && demand == self.transport_write_demand_streams
            && unacked == Some(self.unacked_data) && remote == self.allocated_remote_count
            && self.send.values().flatten().all(|s| s.pending.closed_inventory_matches(meter))
            && self.recv.values().flatten().chain(self.free_recv.iter()).all(|r| {
                let (StreamRecv::Free(r) | StreamRecv::Open(r)) = r;
                r.assembler.closed_inventory_matches(meter)
            })
    }
    fn retire_closed_half(&mut self, id: StreamId, half: StreamHalf, meter: &BackingMeter) -> Result<(), MeteredCopyError> {
        if !self.closed_inventory_matches(meter) { return Err(MeteredCopyError::Quarantined); }
        let present = match half { StreamHalf::Send => self.send.contains_key(&id), StreamHalf::Recv => self.recv.contains_key(&id) };
        if !present { return Ok(()); }
        // An actual application half cannot own a peer stream not yet opened.
        if id.initiator() != self.side && id.index() >= self.next_reported_remote[id.dir() as usize] {
            return Err(MeteredCopyError::Quarantined);
        }
        let last_remote = id.initiator() != self.side && match half {
            StreamHalf::Send => !self.recv.contains_key(&id), StreamHalf::Recv => !self.send.contains_key(&id),
        };
        let remote = if last_remote { self.allocated_remote_count[id.dir() as usize].checked_sub(1) }
            else { Some(self.allocated_remote_count[id.dir() as usize]) };
        let Some(remote) = remote else { return Err(MeteredCopyError::Quarantined); };
        match half {
            StreamHalf::Send => {
                let (unacked, demand) = self.send.get(&id).and_then(Option::as_ref).map_or((0, 0), |s| (
                    if matches!(s.state, SendState::ResetSent) { 0 } else { s.pending.unacked() },
                    usize::from(s.transport_write_demand_through.is_some()),
                ));
                let counts = self.send_streams.checked_sub(1).zip(self.unacked_data.checked_sub(unacked))
                    .zip(self.transport_write_demand_streams.checked_sub(demand));
                let Some(((streams, unacked), demand)) = counts else { return Err(MeteredCopyError::Quarantined); };
                // None remains a present tombstone until real Drop and final
                // admission. A concurrent Q cannot mint a false absent witness.
                let value = self.send.get_mut(&id).and_then(Option::take);
                drop(value);
                meter.admit_precharged()?;
                self.send.remove(&id);
                self.send_streams = streams;
                self.unacked_data = unacked;
                self.transport_write_demand_streams = demand;
            }
            StreamHalf::Recv => {
                let value = self.recv.get_mut(&id).and_then(Option::take);
                drop(value);
                meter.admit_precharged()?;
                self.recv.remove(&id);
            }
        }
        self.allocated_remote_count[id.dir() as usize] = remote;
        Ok(())
    }
}
