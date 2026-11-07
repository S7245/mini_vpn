//! Device-owned packet egress: queued packets, selected write and flush proofs.
//! No Flow, ACK, session-stop or budget authority is created here. Virtual TUN
//! owns this inventory; Native Loopback uses the same packet owner. One cold
//! protected TX packet stays charged until actual Egress retirement; RX progress
//! under general backing pressure remains a separate unfinished A1 obligation.
//! Entries: token/inject/flush/summaries; tests live in egress/tests/mod.rs.
use super::*;
use bytes::{AllocationIdentity, BackingBudgetReservation, BackingMeter, Bytes, MeteredCopyError};
use std::alloc::Layout;
use tokio::io::AsyncWrite;

use super::callback;
pub(crate) use callback::{
    Claim as CallbackClaim, Custody as CallbackCustody, Report as CallbackReport,
    Status as CallbackStatus, SourceGuard as CallbackSourceGuard, Tail as CallbackTail, original_io as callback_original_io,
    report as callback_report,
};

// The one protected allocation never leaves this Egress owner. It moves through
// prepared/queue/selected, and returns only after an actual known disposition.
// Idle stock is physical storage, not a logical packet or a budget refund.
enum ProtectedState {
    Idle(BytesMut),
    InFlight,
    Uncertain(Bytes),
}
struct ProtectedPacket {
    identity: AllocationIdentity,
    state: ProtectedState,
}
impl ProtectedPacket {
    fn new(len: usize, meter: &BackingMeter) -> std::io::Result<Self> {
        let bytes = BytesMut::try_zeroed_metered(len, meter)
            .map_err(quinn::owned_events::AdmissionError)?
            .freeze();
        let identity = bytes.allocation_identity().ok_or_else(device_closed)?;
        let packet = bytes.try_into_mut().map_err(|_| device_closed())?;
        Ok(Self {
            identity,
            state: ProtectedState::Idle(packet),
        })
    }
    fn overlaps(&self, packet: &BytesMut) -> bool {
        let start = packet.as_ptr() as usize;
        let end = start.checked_add(packet.capacity());
        let own_end = self.identity.base.checked_add(self.identity.capacity);
        match (end, own_end) {
            (Some(end), Some(own_end)) => start < own_end && self.identity.base < end,
            _ => true,
        }
    }
    fn matches(&self, packet: &BytesMut, meter: &BackingMeter) -> bool {
        packet.as_ptr() as usize == self.identity.base
            && packet.capacity() == self.identity.capacity
            && packet.len() <= self.identity.capacity
            && packet.precharged_backing_matches(meter)
    }
    fn take(&mut self, meter: &BackingMeter) -> Result<Option<BytesMut>, ()> {
        match &self.state {
            ProtectedState::Idle(packet)
                if !self.matches(packet, meter) || packet.len() != self.identity.capacity =>
            {
                return Err(());
            }
            ProtectedState::InFlight => return Ok(None),
            ProtectedState::Uncertain(_) => return Err(()),
            _ => {}
        }
        match std::mem::replace(&mut self.state, ProtectedState::InFlight) {
            ProtectedState::Idle(packet) => Ok(Some(packet)),
            original => {
                self.state = original;
                Err(())
            }
        }
    }
    // A failed validation leaves the original packet in its original field;
    // an unexpected alias leaves its original immutable backing here instead.
    fn return_packet(&mut self, slot: &mut Option<BytesMut>, meter: &BackingMeter) -> bool {
        let Some(packet) = slot.as_ref() else {
            return true;
        };
        if !self.overlaps(packet) {
            drop(slot.take());
            return true;
        }
        if !matches!(self.state, ProtectedState::InFlight) || !self.matches(packet, meter) {
            return false;
        }
        let Some(packet) = slot.take() else {
            return false;
        };
        let bytes = packet.freeze();
        if bytes.allocation_identity() != Some(self.identity) {
            self.state = ProtectedState::Uncertain(bytes);
            return false;
        }
        match bytes.try_into_mut() {
            Ok(mut packet) => {
                // The exact capacity was validated before mutation; no growth.
                packet.resize(self.identity.capacity, 0);
                packet.fill(0);
                self.state = ProtectedState::Idle(packet);
                true
            }
            Err(original) => {
                self.state = ProtectedState::Uncertain(original);
                false
            }
        }
    }
    fn verify_idle_unique(&mut self, meter: &BackingMeter) -> bool {
        match std::mem::replace(&mut self.state, ProtectedState::InFlight) {
            ProtectedState::Idle(packet) => self.return_packet(&mut Some(packet), meter),
            original => {
                self.state = original;
                false
            }
        }
    }
    fn verify_live_unique(&mut self, packet: &mut BytesMut, meter: &BackingMeter) -> bool {
        if !self.overlaps(packet) {
            return true;
        }
        if !self.matches(packet, meter) {
            return false;
        }
        let original = std::mem::replace(packet, BytesMut::new()).freeze();
        if original.allocation_identity() != Some(self.identity) {
            self.state = ProtectedState::Uncertain(original);
            return false;
        }
        match original.try_into_mut() {
            Ok(original) => {
                *packet = original;
                true
            }
            Err(original) => {
                self.state = ProtectedState::Uncertain(original);
                false
            }
        }
    }
    fn uncertain_bytes(&self) -> usize {
        match &self.state {
            ProtectedState::Uncertain(bytes) => bytes.len(),
            _ => 0,
        }
    }
    fn inventory_matches<'a>(
        &self,
        packets: impl Iterator<Item = &'a BytesMut>,
        meter: &BackingMeter,
    ) -> bool {
        let mut found = 0usize;
        for packet in packets {
            if self.overlaps(packet) {
                if !self.matches(packet, meter) {
                    return false;
                }
                found += 1;
            }
        }
        match &self.state {
            ProtectedState::Idle(packet) => {
                found == 0 && self.matches(packet, meter) && packet.len() == self.identity.capacity
            }
            ProtectedState::InFlight => found == 1,
            ProtectedState::Uncertain(_) => false,
        }
    }
}

struct Cold {
    meter: BackingMeter,
    slots: usize,
    mtu: usize,
    prefix: usize,
    layout: BackingBudgetReservation,
    callbacks: Option<CallbackCustody>,
    protected: ProtectedPacket,
}
struct UnwindBudget(Option<BackingMeter>);
impl Drop for UnwindBudget {
    fn drop(&mut self) {
        if std::thread::panicking() {
            if let Some(meter) = &self.0 {
                meter.quarantine();
            }
        }
    }
}

#[derive(Default)]
pub(crate) struct Egress {
    pub(crate) queue: VecDeque<BytesMut>,
    pub(crate) summaries: Vec<TunFlushedTcpPacket>,
    selected: Option<BytesMut>,
    failed: bool,
    prepared: Option<BytesMut>,
    cold: Option<Cold>,
}
impl Egress {
    pub(crate) fn bind(
        &mut self,
        meter: &BackingMeter,
        slots: usize,
        mtu: usize,
        prefix: usize,
    ) -> std::io::Result<()> {
        let cold = self
            .cold
            .as_ref()
            .ok_or_else(|| std::io::Error::from(ErrorKind::Unsupported))?;
        let original = cold.meter.clone();
        let profile = (cold.slots, cold.mtu, cold.prefix);
        if !self.ready() {
            return Err(self.unavailable());
        }
        let valid = self
            .callback_custody()
            .is_some_and(|c| c.matches(&original))
            && self
                .queue
                .iter()
                .chain(self.selected.iter())
                .chain(self.prepared.iter())
                .all(|p| p.precharged_backing_matches(&original))
            && self.verify_protected_inventory();
        if !valid {
            self.quarantine();
            return Err(self.unavailable());
        }
        if !original.same_authority(meter) || profile != (slots, mtu, prefix) {
            return Err(ErrorKind::InvalidInput.into());
        }
        Ok(())
    }
    pub(crate) fn native(
        slots: usize,
        mtu: usize,
        prefix: usize,
        meter: &BackingMeter,
    ) -> std::io::Result<Self> {
        Self::native_with_callbacks(slots, mtu, prefix, meter, CallbackCustody::new(meter)?)
    }
    pub(crate) fn native_with_callbacks(
        slots: usize, mtu: usize, prefix: usize, meter: &BackingMeter, callbacks: CallbackCustody,
    ) -> std::io::Result<Self> {
        if !callbacks.matches(meter) { return Err(ErrorKind::InvalidInput.into()); }
        if slots == 0 || !(576..=9000).contains(&mtu) || !matches!(prefix, 0 | 4) {
            return Err(ErrorKind::InvalidInput.into());
        }
        let bytes = Self::layout_bytes(slots)
            .ok_or_else(|| std::io::Error::from(ErrorKind::InvalidInput))?;
        let reservation = meter
            .reserve_cold_layout(bytes)
            .map_err(|e| std::io::Error::from(quinn::owned_events::AdmissionError(e)))?
            .ok_or_else(|| std::io::Error::from(ErrorKind::Unsupported))?;
        let mut owner = Self::default();
        owner.cold = Some(Cold {
            meter: meter.clone(),
            slots,
            mtu,
            prefix,
            layout: reservation,
            callbacks: Some(callbacks),
            protected: ProtectedPacket::new(mtu + prefix, meter)?,
        });
        owner.queue = VecDeque::from(exact_vec(slots)?);
        owner.summaries = exact_vec(slots)?;
        Ok(owner)
    }
    /// Cold bytes `native_with_callbacks` reserves: descriptor arrays plus the
    /// protected packet's metered allocation of `mtu + prefix`.
    pub(super) fn cold_bytes(slots: usize, mtu: usize, prefix: usize) -> Option<usize> {
        let protected = BytesMut::metered_copy_layout_for_len(mtu.checked_add(prefix)?).ok()?;
        Self::layout_bytes(slots)?.checked_add(protected)
    }
    fn layout_bytes(slots: usize) -> Option<usize> {
        Layout::array::<BytesMut>(slots).ok().and_then(|p| {
            Layout::array::<TunFlushedTcpPacket>(slots)
                .ok()
                .and_then(|s| p.size().checked_add(s.size()))
        })
    }
    fn ready(&mut self) -> bool {
        if self.failed {
            return false;
        }
        let valid = self.cold.as_ref().is_none_or(|cold| {
            cold.meter.admit_precharged().is_ok()
                && cold.callbacks.is_some()
                && self.queue.capacity() == cold.slots
                && self.summaries.capacity() == cold.slots
                && self.occupied() <= cold.slots
                && Self::layout_bytes(cold.slots)
                    .is_some_and(|n| cold.meter.cold_layout_matches(Some(&cold.layout), n))
        });
        if !valid {
            self.quarantine();
        }
        valid
    }
    fn protected_inventory_matches(&self) -> bool {
        self.cold.as_ref().is_none_or(|cold| {
            cold.protected.inventory_matches(
                self.queue
                    .iter()
                    .chain(self.selected.iter())
                    .chain(self.prepared.iter()),
                &cold.meter,
            )
        })
    }
    // Whole physical checks belong to bind/cut/retirement, never each packet.
    fn verify_protected_inventory(&mut self) -> bool {
        if !self.protected_inventory_matches() {
            return false;
        }
        let Some(cold) = &mut self.cold else {
            return true;
        };
        if matches!(cold.protected.state, ProtectedState::Idle(_)) {
            return cold.protected.verify_idle_unique(&cold.meter);
        }
        self.queue
            .iter_mut()
            .chain(self.selected.iter_mut())
            .chain(self.prepared.iter_mut())
            .all(|packet| cold.protected.verify_live_unique(packet, &cold.meter))
    }
    fn retire_prepared(&mut self) {
        let valid = match &mut self.cold {
            Some(cold) => cold
                .protected
                .return_packet(&mut self.prepared, &cold.meter),
            None => {
                drop(self.prepared.take());
                true
            }
        };
        if !valid {
            self.quarantine();
        }
    }
    fn retire_selected(&mut self) -> bool {
        let valid = match &mut self.cold {
            Some(cold) => cold
                .protected
                .return_packet(&mut self.selected, &cold.meter),
            None => {
                drop(self.selected.take());
                true
            }
        };
        if !valid {
            self.quarantine();
        }
        valid
    }
    fn occupied(&self) -> usize {
        self.queue.len()
            + self.summaries.len()
            + usize::from(self.selected.is_some())
            + usize::from(self.prepared.is_some())
    }
    fn quarantine(&mut self) {
        self.failed = true;
        if let Some(cold) = &self.cold {
            cold.meter.quarantine();
        }
    }
    pub(crate) fn unavailable(&self) -> std::io::Error {
        self.callback_custody()
            .and_then(|callbacks| callbacks.finish().err())
            .unwrap_or_else(device_closed)
    }
    pub(crate) fn token(&mut self, mtu: usize, prefix: usize) -> Option<PacketToken<'_>> {
        if !self.ready() || self.prepared.is_some() {
            return None;
        }
        if let Some(cold) = &self.cold {
            if cold.mtu != mtu || cold.prefix != prefix {
                self.quarantine();
                return None;
            }
            if self.occupied() >= cold.slots {
                return None;
            }
            let payload = match BytesMut::try_zeroed_metered(mtu + prefix, &cold.meter) {
                Ok(packet) => Some(packet),
                Err(MeteredCopyError::BudgetFull) => {
                    let cold = self.cold.as_mut()?;
                    match cold.protected.take(&cold.meter) {
                        Ok(packet) => packet,
                        Err(()) => {
                            self.quarantine();
                            return None;
                        }
                    }
                }
                Err(_) => None,
            }?;
            self.prepared = Some(payload);
        }
        Some(PacketToken {
            owner: self,
            prefix,
        })
    }

    pub(crate) fn inject(
        &mut self,
        max_len: usize,
        prefix: usize,
        encode: impl FnOnce(&mut [u8]) -> std::io::Result<usize>,
    ) -> std::io::Result<()> {
        if max_len == 0 || max_len > u16::MAX as usize || !matches!(prefix, 0 | 4) {
            return Err(ErrorKind::InvalidInput.into());
        }
        if !self.ready() || self.prepared.is_some() {
            return Err(self.unavailable());
        }
        let _unwind = UnwindBudget(self.cold.as_ref().map(|cold| cold.meter.clone()));
        let allocation = match &self.cold {
            Some(cold) => {
                if cold.prefix != prefix {
                    self.quarantine();
                    return Err(self.unavailable());
                }
                cold.meter
                    .admit_precharged()
                    .map_err(quinn::owned_events::AdmissionError)?;
                if self.occupied() >= cold.slots {
                    return Err(ErrorKind::WouldBlock.into());
                }
                BytesMut::try_zeroed_metered(max_len + prefix, &cold.meter)
                    .map_err(quinn::owned_events::AdmissionError)?
            }
            None => BytesMut::zeroed(max_len + prefix),
        };
        self.prepared = Some(allocation);
        let packet = self.prepared.as_mut().ok_or_else(device_closed)?;
        if prefix == 4 {
            packet[..4].copy_from_slice(&[0, 0, 0, 2]);
        }
        let result = encode(&mut packet[prefix..]);
        if self
            .cold
            .as_ref()
            .is_some_and(|c| c.meter.admit_precharged().is_err())
        {
            self.quarantine();
            return match result {
                Err(original) => {
                    if let Some(callbacks) = self.callback_custody().filter(|c| c.has_failure()) {
                        callbacks.note_write(Err(original));
                        Err(self.unavailable())
                    } else {
                        Err(original)
                    }
                }
                Ok(_) => Err(self.unavailable()),
            };
        }
        match result {
            Ok(len) if len > 0 && len <= max_len => {
                if let Some(mut packet) = self.prepared.take() {
                    packet.truncate(len + prefix);
                    self.queue.push_back(packet);
                }
                Ok(())
            }
            Ok(_) => {
                self.quarantine();
                Err(ErrorKind::InvalidData.into())
            }
            Err(error) => {
                self.prepared.take();
                Err(error)
            }
        }
    }
    pub(crate) fn raw_injection_allowed(&mut self) -> bool {
        if self.cold.is_some() {
            self.quarantine();
            false
        } else {
            !self.failed
        }
    }

    pub(crate) fn summaries(&mut self) -> FlushedTcpBatch<'_> {
        match &self.cold {
            Some(cold) => FlushedTcpBatch {
                inner: BatchStorage::Native(self.summaries.drain(..)),
                meter: Some(cold.meter.clone()),
                committed: false,
            },
            None => std::mem::take(&mut self.summaries).into(),
        }
    }

    #[cfg(test)]
    pub(crate) fn packets_for_probe(&self) -> impl Iterator<Item = &BytesMut> {
        self.selected.iter().chain(self.queue.iter())
    }
    pub(crate) fn shutdown_tail(&mut self, stock: Option<rx_packet::Stock>) -> Option<CallbackTail> {
        self.cold.as_mut()?.callbacks.take().map(|c| CallbackTail::with_stock(c, stock))
    }
    pub(crate) fn callback_custody(&self) -> Option<CallbackCustody> {
        self.cold.as_ref().and_then(|cold| cold.callbacks.clone())
    }
    pub(crate) fn native_meter(&self) -> Option<&BackingMeter> {
        self.cold.as_ref().map(|cold| &cold.meter)
    }

    pub(crate) fn owned_bytes(&self) -> usize {
        self.queue.iter().map(BytesMut::len).sum::<usize>()
            + self.selected.as_ref().map_or(0, BytesMut::len)
            + self.prepared.as_ref().map_or(0, BytesMut::len)
            + self
                .cold
                .as_ref()
                .map_or(0, |cold| cold.protected.uncertain_bytes())
    }
    pub(crate) async fn flush<W: AsyncWrite + Unpin>(
        &mut self,
        writer: &mut W,
    ) -> std::io::Result<()> {
        let _unwind = UnwindBudget(self.cold.as_ref().map(|cold| cold.meter.clone()));
        loop {
            if !self.ready() {
                return Err(self.unavailable());
            }
            if self.selected.is_none() {
                self.selected = self.queue.pop_front();
            }
            let Some(packet) = self.selected.as_ref() else {
                return Ok(());
            };
            let expected = packet.len();
            let result = writer.write(packet).await;
            let error = match result {
                Ok(n) if n == expected => {
                    let prefix = self
                        .cold
                        .as_ref()
                        .map_or_else(tun_packet_header_len, |cold| cold.prefix);
                    if let Some(summary) = packet
                        .get(prefix..)
                        .and_then(summarize_flushed_ip_tcp_packet)
                    {
                        self.summaries.push(summary);
                    }
                    if !self.retire_selected() {
                        return Err(self.unavailable());
                    }
                    continue;
                }
                Ok(0) => {
                    self.quarantine();
                    std::io::Error::new(ErrorKind::WriteZero, "TUN packet write returned zero")
                }
                Ok(_) => {
                    self.quarantine();
                    std::io::Error::other("short TUN packet write; no suffix retry or flush proof")
                }
                Err(error) if error.kind() == ErrorKind::Interrupted => continue,
                Err(error) => error,
            };
            self.failed = true;
            return Err(error);
        }
    }
    pub(crate) fn discard(&mut self, totals: &mut (u64, u64)) {
        if !self.verify_protected_inventory() {
            self.quarantine();
        }
        // Totals describe disposed packets, not refunded cold allocations.
        if let Some(packet) = &self.prepared {
            totals.0 = totals.0.saturating_add(1);
            totals.1 = totals.1.saturating_add(packet.capacity() as u64);
        }
        self.retire_prepared();
        if let Some(packet) = &self.selected {
            totals.0 = totals.0.saturating_add(1);
            totals.1 = totals.1.saturating_add(packet.capacity() as u64);
        }
        self.retire_selected();
        while let Some(packet) = self.queue.pop_front() {
            totals.0 = totals.0.saturating_add(1);
            totals.1 = totals.1.saturating_add(packet.capacity() as u64);
            let mut slot = Some(packet);
            let valid = match &mut self.cold {
                Some(cold) => cold.protected.return_packet(&mut slot, &cold.meter),
                None => {
                    drop(slot.take());
                    true
                }
            };
            if !valid {
                self.quarantine();
                // Preserve an invalid original in its queue field on Q.
                if let Some(packet) = slot {
                    self.queue.push_front(packet);
                }
                break;
            }
        }
        self.summaries.clear();
        if !self.protected_inventory_matches() {
            self.quarantine();
        }
    }
}

#[cfg(test)]
mod tests;

impl Drop for Egress {
    fn drop(&mut self) {
        if let Some(callbacks) = self.cold.as_ref().and_then(|cold| cold.callbacks.as_ref()) {
            callbacks.close();
            if callbacks.pending() {
                self.quarantine();
            }
        }
        if self.cold.is_some() && (self.occupied() != 0 || !self.verify_protected_inventory()) {
            self.quarantine();
        }
    }
}

/// A real slot plus maximum packet backing is prepared before smoltcp receives
/// this token. An unused token cancels only its known unpublished preparation.
pub struct PacketToken<'a> {
    owner: &'a mut Egress,
    prefix: usize,
}
impl TxToken for PacketToken<'_> {
    fn consume<R, F: FnOnce(&mut [u8]) -> R>(self, len: usize, f: F) -> R {
        if let Some(cold) = &self.owner.cold {
            if len > cold.mtu || self.owner.failed || cold.meter.admit_precharged().is_err() {
                self.owner.quarantine();
                panic!("invalidated Native TUN TxToken contract");
            }
            let Some(packet) = self.owner.prepared.as_mut() else {
                self.owner.quarantine();
                panic!("Native TUN TxToken lost prepared backing");
            };
            packet.truncate(len + self.prefix);
            if self.prefix == 4 {
                packet[..4].copy_from_slice(&[0, 0, 0, 2]);
            }
            let result = f(&mut packet[self.prefix..]);
            if self
                .owner
                .cold
                .as_ref()
                .is_some_and(|c| c.meter.admit_precharged().is_err())
            {
                self.owner.quarantine();
                // Dispose R before starting our own unwind; if its Drop panics,
                // that original payload goes directly to the Service guard.
                drop(result);
                panic!("Native TUN TxToken invalidated before publication");
            }
            if let Some(packet) = self.owner.prepared.take() {
                self.owner.queue.push_back(packet);
            }
            result
        } else {
            let mut packet = BytesMut::zeroed(len + self.prefix);
            if self.prefix == 4 {
                packet[..4].copy_from_slice(&[0, 0, 0, 2]);
            }
            let result = f(&mut packet[self.prefix..]);
            self.owner.queue.push_back(packet);
            result
        }
    }
}
impl Drop for PacketToken<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.owner.quarantine();
        } else if !self.owner.failed {
            self.owner.retire_prepared();
        }
    }
}

/// A Device-borrowed proof batch. Native descriptor backing remains with the
/// Device until shutdown; abandoning a proof transaction quarantines its meter.
pub struct FlushedTcpBatch<'a> {
    inner: BatchStorage<'a>,
    meter: Option<BackingMeter>,
    committed: bool,
}
enum BatchStorage<'a> {
    Legacy(std::vec::IntoIter<TunFlushedTcpPacket>),
    Native(std::vec::Drain<'a, TunFlushedTcpPacket>),
}
impl<'a> From<Vec<TunFlushedTcpPacket>> for FlushedTcpBatch<'a> {
    fn from(packets: Vec<TunFlushedTcpPacket>) -> Self {
        Self {
            inner: BatchStorage::Legacy(packets.into_iter()),
            meter: None,
            committed: false,
        }
    }
}
impl Iterator for FlushedTcpBatch<'_> {
    type Item = TunFlushedTcpPacket;
    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.inner {
            BatchStorage::Legacy(i) => i.next(),
            BatchStorage::Native(i) => i.next(),
        }
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        match &self.inner {
            BatchStorage::Legacy(i) => i.size_hint(),
            BatchStorage::Native(i) => i.size_hint(),
        }
    }
}
impl ExactSizeIterator for FlushedTcpBatch<'_> {}
impl FlushedTcpBatch<'_> {
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub(crate) fn quarantine(&self) {
        if let Some(meter) = &self.meter {
            meter.quarantine();
        }
    }
    pub(crate) fn commit(mut self) {
        if !self.is_empty() {
            self.quarantine();
        }
        self.committed = true;
    }
}
impl Drop for FlushedTcpBatch<'_> {
    fn drop(&mut self) {
        if !self.committed {
            self.quarantine();
        }
    }
}
