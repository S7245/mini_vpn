//! Owns ordered sent records, linear pre-encoding credit, and original tail policy.
//! Hard-budget profiles cold-admit Slot layout before CID/TLS; legacy profiles
//! retain BTreeMap semantics. Interior holes stay charged until head advancement.
//! An untouched permit cancels; dirty interruption or foreign custody is sticky Q.
//! ACK/loss selections use these original records without heap scratch;
//! space-discard keeps original counters. Non-CID frame storage is still an
//! outstanding budget obligation. Credits
//! headers are fixed three instances/connection, not included in Slot layout.
//! Zero-size non-ACK-eliciting records settle their credit and are never
//! retained in cold history (N1 D46); ACK-eliciting Full stays connection-local.
//! Entries: History::{prepare,reserve,release_untracked}, PacketSpace::{take,sent}.
//! Tests: sent_history/tests and original encrypted tests/admission.

use super::{control_inventory::ThinRetransmits, spaces::PacketSpace};
use crate::Instant;
use std::ops::Bound;

/// Represents one or more packets subject to retransmission
#[derive(Debug)]
pub(super) struct SentPacket {
    pub(super) recovery_selection: u8,
    /// [`PathData::generation`](super::PathData::generation) of the path on which this packet was sent
    pub(super) path_generation: u64,
    /// The time the packet was sent.
    pub(super) time_sent: Instant,
    /// The number of bytes sent in the packet, not including UDP or IP overhead, but including QUIC
    /// framing overhead. Zero if this packet is not counted towards congestion control, i.e. not an
    /// "in flight" packet.
    pub(super) size: u16,
    /// Whether an acknowledgement is expected directly in response to this packet.
    pub(super) ack_eliciting: bool,
    /// Whether this packet carries bytes owned by the bounded successor service turn.
    pub(super) successor_service_turn: bool,
    /// Whether this packet carries STREAM bytes for a transport-blocked application writer.
    pub(super) transport_write_demand: bool,
    /// The largest packet number acknowledged by this packet
    pub(super) largest_acked: Option<u64>,
    /// Data which needs to be retransmitted in case the packet is lost.
    /// The data is boxed to minimize `SentPacket` size for the typical case of
    /// packets only containing ACKs and STREAM frames.
    pub(super) retransmits: ThinRetransmits,
    /// Metadata for stream frames in a packet
    ///
    /// The actual application data is stored with the stream state.
    pub(super) stream_frames: super::control_inventory::records::StreamFrames,
}

impl PacketSpace {
    /// Stop tracking sent packet `number`, and return what we knew about it
    pub(super) fn take(&mut self, number: u64) -> Option<SentPacket> {
        let packet = self.sent_packets.remove(&number)?;
        if !packet.ack_eliciting && number > self.largest_ack_eliciting_sent {
            self.unacked_non_ack_eliciting_tail =
                self.unacked_non_ack_eliciting_tail.checked_sub(1).unwrap();
        }
        Some(packet)
    }

    /// May return a packet that should be forgotten
    pub(super) fn sent(
        &mut self,
        number: u64,
        packet: SentPacket,
        permit: PacketPermit,
    ) -> Result<Option<SentPacket>, (SentPacket, PacketPermit)> {
        // Retain state for at most this many non-ACK-eliciting packets sent after the most recently
        // sent ACK-eliciting packet. We're never guaranteed to receive an ACK for those, and we
        // can't judge them as lost without an ACK, so to limit memory in applications which receive
        // packets but don't send ACK-eliciting data for long periods use we must eventually start
        // forgetting about them, although it might also be reasonable to just kill the connection
        // due to weird peer behavior.
        const MAX_UNACKED_NON_ACK_ELICTING_TAIL: u64 = 1_000;

        // Cold profile only: a zero-size non-ACK-eliciting packet (ACK/PADDING/
        // CLOSE only, RFC 9000 §13.2) owns no in-flight bytes and no
        // retransmittable state, so it is never retained (upstream already
        // forgets such tails). Retaining it lets an honest peer ACK queued
        // behind a local receive backlog pin the fixed ring to Full and close
        // the whole connection (N1 D46). Its exact credit settles cleanly.
        if !packet.ack_eliciting
            && packet.size == 0
            && packet.stream_frames.is_empty()
            && packet.retransmits.record_len() == 0
            && self.sent_packets.is_cold()
        {
            return self.sent_packets.release_untracked(packet, permit);
        }
        let mut forgotten = None;
        if packet.ack_eliciting {
            self.unacked_non_ack_eliciting_tail = 0;
            self.largest_ack_eliciting_sent = number;
        } else if self.unacked_non_ack_eliciting_tail > MAX_UNACKED_NON_ACK_ELICTING_TAIL {
            let oldest_after_ack_eliciting = *self
                .sent_packets
                .range((
                    Bound::Excluded(self.largest_ack_eliciting_sent),
                    Bound::Unbounded,
                ))
                .next()
                .unwrap()
                .0;
            // Per https://www.rfc-editor.org/rfc/rfc9000.html#name-frames-and-frame-types,
            // non-ACK-eliciting packets must only contain PADDING, ACK, and CONNECTION_CLOSE
            // frames, which require no special handling on ACK or loss beyond removal from
            // in-flight counters if padded.
            let packet = self
                .sent_packets
                .remove(&oldest_after_ack_eliciting)
                .unwrap();
            debug_assert!(!packet.ack_eliciting);
            forgotten = Some(packet);
        } else {
            self.unacked_non_ack_eliciting_tail += 1;
        }

        self.sent_packets.insert_prepared(number, packet, permit)?;
        Ok(forgotten)
    }
}

#[cfg(test)]
mod tests;
#[cfg(feature = "sent-history-observe")]
pub mod observe;

use bytes::{BackingBudgetReservation, BackingMeter};
use std::{
    alloc::{alloc, Layout, LayoutError},
    collections::{btree_map, BTreeMap},
    io,
    ops::{Index, RangeBounds},
    slice,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

/// Ordered records owned by one packet space. Cold positions, including
/// tombstones, and outstanding preparations all consume the same fixed quota.
pub(super) struct History {
    mode: Mode,
}
enum Mode {
    Legacy(BTreeMap<u64, SentPacket>),
    Cold(Ring),
}
struct Slot {
    number: u64,
    packet: Option<SentPacket>,
}
struct Credits {
    recovery_active: AtomicUsize,
    used: AtomicUsize,
    ordinary: usize,
    capacity: usize,
    meter: BackingMeter,
    #[cfg(feature = "sent-history-observe")]
    observed: bool,
    _reservation: BackingBudgetReservation,
}
struct Ring {
    // Actual records drop before the last credit can refund their layout.
    slots: Box<[Slot]>,
    owner: Arc<Credits>,
    head: usize,
    positions: usize,
    live: usize,
}

/// Linear preparation of one future tracked packet, before PN/frame mutation.
/// A cancelled untouched preparation returns its slot. An interrupted dirty
/// transaction retains a charged unknown rather than declaring clean rollback.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum PacketClass {
    Ordinary,
    Control,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ReserveError {
    Full,
    Quarantined,
}
pub(super) struct PacketPermit {
    owner: Option<Arc<Credits>>,
    class: PacketClass,
    dirty: bool,
    settled: bool,
}
impl PacketPermit {
    pub(super) fn allows_data(&self) -> bool {
        self.class == PacketClass::Ordinary
    }
    pub(super) fn begin_encoding(&mut self) -> Result<(), ReserveError> {
        if let Some(owner) = &self.owner {
            owner
                .meter
                .admit_precharged()
                .map_err(|_| ReserveError::Quarantined)?;
        }
        self.dirty = true;
        Ok(())
    }
}
impl Drop for PacketPermit {
    fn drop(&mut self) {
        if self.settled {
            return;
        }
        if let Some(owner) = &self.owner {
            if self.dirty {
                owner.meter.quarantine();
            } else {
                owner.used.fetch_sub(1, Ordering::AcqRel);
            }
        }
    }
}
impl Default for History {
    fn default() -> Self {
        Self {
            mode: Mode::Legacy(BTreeMap::new()),
        }
    }
}
impl History {
    /// Every retained packet may own metadata, even without RESET/STOP.
    /// The original poll finishes the previous builder before preparing one
    /// successor, so exactly one additional descriptor covers construction.
    pub(super) fn descriptor_capacity(config: &crate::EndpointConfig) -> io::Result<usize> {
        Self::limits(config)
            .into_iter()
            .try_fold(1usize, |sum, (a, b)| {
                sum.checked_add(a)
                    .and_then(|n| n.checked_add(b))
                    .ok_or_else(|| io::Error::other("descriptor capacity overflow"))
            })
    }
    fn limits(config: &crate::EndpointConfig) -> [(usize, usize); 3] {
        let small = (
            config.sent_history_ordinary.min(64),
            config.sent_history_control.min(64),
        );
        [
            small,
            small,
            (config.sent_history_ordinary, config.sent_history_control),
        ]
    }
    pub(super) fn layout_bytes(config: &crate::EndpointConfig) -> io::Result<usize> {
        Self::limits(config)
            .into_iter()
            .try_fold(0usize, |sum, (ordinary, control)| {
                let count = ordinary
                    .checked_add(control)
                    .ok_or_else(|| io::Error::other("sent capacity overflow"))?;
                sum.checked_add(Self::layout(count).map_err(io::Error::other)?.size())
                    .ok_or_else(|| io::Error::other("sent layouts overflow"))
            })
    }
    pub(super) fn prepare(config: &crate::EndpointConfig) -> io::Result<[Self; 3]> {
        let Some(meter) = config
            .receive_backing_meter
            .as_ref()
            .filter(|m| m.budget_limit().is_some())
        else {
            return Ok(std::array::from_fn(|_| Self::default()));
        };
        let [a, b, c] = Self::limits(config);
        let data = Self::cold(meter, c.0, c.1)?;
        #[cfg(feature = "sent-history-observe")]
        let data = observe::data(data);
        Ok([
            Self::cold(meter, a.0, a.1)?,
            Self::cold(meter, b.0, b.1)?,
            data,
        ])
    }
    pub(super) fn layout(capacity: usize) -> Result<Layout, LayoutError> {
        Layout::array::<Slot>(capacity)
    }
    pub(super) fn cold(meter: &BackingMeter, ordinary: usize, control: usize) -> io::Result<Self> {
        let capacity = ordinary
            .checked_add(control)
            .filter(|&n| n != 0)
            .ok_or_else(|| io::Error::other("sent layout capacity overflow"))?;
        let layout = Self::layout(capacity).map_err(io::Error::other)?;
        let reservation = meter
            .reserve_cold_layout(layout.size())
            .map_err(|error| io::Error::other(format!("{error:?}")))?
            .ok_or_else(|| io::Error::other("sent history requires hard allowance"))?;
        // The same exact requested Layout is admitted before allocating and
        // initializing every slot. No allocation occurs when sending a record.
        let ptr = unsafe { alloc(layout) as *mut Slot };
        if ptr.is_null() {
            return Err(io::Error::other("sent history allocation failed"));
        }
        for index in 0..capacity {
            unsafe {
                ptr.add(index).write(Slot {
                    number: 0,
                    packet: None,
                });
            }
        }
        let slots = unsafe { Box::from_raw(std::ptr::slice_from_raw_parts_mut(ptr, capacity)) };
        Ok(Self {
            mode: Mode::Cold(Ring {
                slots,
                owner: Arc::new(Credits {
                    recovery_active: AtomicUsize::new(0),
                    used: AtomicUsize::new(0),
                    ordinary,
                    capacity,
                    meter: meter.clone(),
                    #[cfg(feature = "sent-history-observe")]
                    observed: false,
                    _reservation: reservation,
                }),
                head: 0,
                positions: 0,
                live: 0,
            }),
        })
    }
    pub(super) fn ordinary_ready(&self) -> bool {
        match &self.mode {
            Mode::Legacy(_) => true,
            Mode::Cold(ring) => {
                let ready = !ring.owner.meter.snapshot().quarantined
                    && ring.owner.used.load(Ordering::Acquire) < ring.owner.ordinary;
                #[cfg(feature = "sent-history-observe")]
                observe::ready(&ring.owner, ready);
                ready
            }
        }
    }
    pub(super) fn reserve(&self, class: PacketClass) -> Result<PacketPermit, ReserveError> {
        match &self.mode {
            Mode::Legacy(_) => Ok(PacketPermit {
                owner: None,
                class,
                dirty: false,
                settled: false,
            }),
            Mode::Cold(ring) => {
                if ring.owner.meter.snapshot().quarantined {
                    return Err(ReserveError::Quarantined);
                }
                let limit = match class {
                    PacketClass::Ordinary => ring.owner.ordinary,
                    PacketClass::Control => ring.owner.capacity,
                };
                let _previous = ring
                    .owner
                    .used
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                        (used < limit).then(|| used + 1)
                    })
                    .map_err(|_| ReserveError::Full)?;
                #[cfg(feature = "sent-history-observe")]
                observe::used(&ring.owner, _previous + 1, class);
                if ring.owner.meter.admit_precharged().is_err() {
                    ring.owner.used.fetch_sub(1, Ordering::AcqRel);
                    return Err(ReserveError::Quarantined);
                }
                Ok(PacketPermit {
                    owner: Some(ring.owner.clone()),
                    class,
                    dirty: false,
                    settled: false,
                })
            }
        }
    }
    pub(super) fn insert_prepared(
        &mut self,
        number: u64,
        packet: SentPacket,
        mut permit: PacketPermit,
    ) -> Result<(), (SentPacket, PacketPermit)> {
        // This is a new commit fence, not a second byte charge. Q ordered
        // before it refuses this record; Q after it observes an already
        // admitted in-flight transaction whose actual resources stay charged.
        if permit
            .owner
            .as_ref()
            .is_some_and(|owner| owner.meter.admit_precharged().is_err())
        {
            return Err((packet, permit));
        }
        match (&mut self.mode, &permit.owner) {
            (Mode::Legacy(map), None) => {
                map.insert(number, packet);
            }
            (Mode::Cold(ring), Some(owner))
                if Arc::ptr_eq(&ring.owner, owner)
                    && ring.positions < ring.slots.len()
                    && (ring.positions == 0 || ring.number(ring.positions - 1) < number) =>
            {
                let index = (ring.head + ring.positions) % ring.slots.len();
                ring.slots[index] = Slot {
                    number,
                    packet: Some(packet),
                };
                ring.positions += 1;
                ring.live += 1;
            }
            _ => {
                if let Mode::Cold(ring) = &self.mode {
                    ring.owner.meter.quarantine();
                }
                if let Some(owner) = &permit.owner {
                    owner.meter.quarantine();
                }
                return Err((packet, permit));
            }
        }
        permit.settled = true;
        Ok(())
    }
    pub(super) fn is_cold(&self) -> bool {
        matches!(self.mode, Mode::Cold(_))
    }
    /// Settle one encoded record that is intentionally not retained. Same
    /// commit fence as [`Self::insert_prepared`]; the returned packet is the
    /// caller's "forgotten" record (in-flight removal, then drop).
    pub(super) fn release_untracked(
        &mut self,
        packet: SentPacket,
        mut permit: PacketPermit,
    ) -> Result<Option<SentPacket>, (SentPacket, PacketPermit)> {
        let owned = match (&self.mode, &permit.owner) {
            (Mode::Cold(ring), Some(owner)) => Arc::ptr_eq(&ring.owner, owner),
            _ => false,
        };
        if !owned {
            if let Mode::Cold(ring) = &self.mode {
                ring.owner.meter.quarantine();
            }
            if let Some(owner) = &permit.owner {
                owner.meter.quarantine();
            }
            return Err((packet, permit));
        }
        if permit
                .owner
                .as_ref()
                .is_some_and(|owner| owner.meter.admit_precharged().is_err())
        {
            return Err((packet, permit));
        }
        if let Some(owner) = &permit.owner {
            owner.used.fetch_sub(1, Ordering::AcqRel);
        }
        permit.settled = true;
        Ok(Some(packet))
    }
    pub(super) fn len(&self) -> usize {
        match &self.mode {
            Mode::Legacy(map) => map.len(),
            Mode::Cold(ring) => ring.live,
        }
    }
    pub(super) fn occupied(&self) -> usize {
        match &self.mode {
            Mode::Legacy(map) => map.len(),
            Mode::Cold(ring) => ring.owner.used.load(Ordering::Acquire),
        }
    }
    pub(super) fn get(&self, number: &u64) -> Option<&SentPacket> {
        match &self.mode {
            Mode::Legacy(map) => map.get(number),
            Mode::Cold(ring) => {
                let pos = ring.lower(*number, false);
                if pos == ring.positions || ring.number(pos) != *number {
                    return None;
                }
                ring.slots[(ring.head + pos) % ring.slots.len()]
                    .packet
                    .as_ref()
            }
        }
    }
    pub(super) fn remove(&mut self, number: &u64) -> Option<SentPacket> {
        match &mut self.mode {
            Mode::Legacy(map) => map.remove(number),
            Mode::Cold(ring) => {
                let pos = ring.lower(*number, false);
                if pos == ring.positions || ring.number(pos) != *number {
                    return None;
                }
                let index = (ring.head + pos) % ring.slots.len();
                let packet = ring.slots[index].packet.take()?;
                ring.live -= 1;
                ring.trim();
                Some(packet)
            }
        }
    }
    /// Remove one original record without replacing the store's authority.
    /// Used by Retry/0RTT/space discard, whose original counter policy remains
    /// in PacketSpace rather than being interpreted as an ACK here.
    pub(super) fn drain_next(&mut self) -> Option<SentPacket> {
        match &mut self.mode {
            Mode::Legacy(map) => map.pop_first().map(|(_, packet)| packet),
            Mode::Cold(ring) => {
                if ring.positions == 0 {
                    return None;
                }
                let packet = ring.slots[ring.head].packet.take()?;
                ring.live -= 1;
                ring.trim();
                Some(packet)
            }
        }
    }
    pub(super) fn iter(&self) -> Iter<'_> {
        match &self.mode {
            Mode::Legacy(map) => Iter::All(map.iter()),
            Mode::Cold(ring) => ring.iter_range(0, ring.positions),
        }
    }
    pub(super) fn range(&self, range: impl RangeBounds<u64>) -> Iter<'_> {
        match &self.mode {
            Mode::Legacy(map) => Iter::Range(map.range(range)),
            Mode::Cold(ring) => {
                let start = match range.start_bound() {
                    Bound::Included(&n) => ring.lower(n, false),
                    Bound::Excluded(&n) => ring.lower(n, true),
                    Bound::Unbounded => 0,
                };
                let end = match range.end_bound() {
                    Bound::Included(&n) => ring.lower(n, true),
                    Bound::Excluded(&n) => ring.lower(n, false),
                    Bound::Unbounded => ring.positions,
                };
                ring.iter_range(start, end.max(start))
            }
        }
    }
    pub(super) fn values(&self) -> Values<'_> {
        Values(self.iter())
    }
    pub(super) fn values_mut(&mut self) -> ValuesMut<'_> {
        ValuesMut(self.iter_mut())
    }
    pub(super) fn iter_mut(&mut self) -> IterMut<'_> {
        match &mut self.mode {
            Mode::Legacy(map) => IterMut::Legacy(map.iter_mut()),
            Mode::Cold(ring) => {
                let (before, after) = ring.slots.split_at_mut(ring.head);
                let first = ring.positions.min(after.len());
                IterMut::Cold(
                    after[..first].iter_mut(),
                    before[..ring.positions - first].iter_mut(),
                )
            }
        }
    }
}
impl Ring {
    fn number(&self, logical: usize) -> u64 {
        self.slots[(self.head + logical) % self.slots.len()].number
    }
    fn lower(&self, number: u64, exclude: bool) -> usize {
        let (mut low, mut high) = (0, self.positions);
        while low < high {
            let mid = low + (high - low) / 2;
            let n = self.number(mid);
            if n < number || (exclude && n == number) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        low
    }
    fn trim(&mut self) {
        while self.positions != 0 && self.slots[self.head].packet.is_none() {
            self.head = (self.head + 1) % self.slots.len();
            self.positions -= 1;
            self.owner.used.fetch_sub(1, Ordering::AcqRel);
        }
    }
    fn iter_range(&self, start: usize, end: usize) -> Iter<'_> {
        let index = (self.head + start) % self.slots.len();
        let len = end - start;
        let first = len.min(self.slots.len() - index);
        Iter::Cold(
            self.slots[index..index + first].iter(),
            self.slots[..len - first].iter(),
        )
    }
}
impl Drop for Ring {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.owner.meter.quarantine();
        }
        self.owner.used.fetch_sub(self.positions, Ordering::AcqRel);
    }
}

pub(super) enum Iter<'a> {
    All(btree_map::Iter<'a, u64, SentPacket>),
    Range(btree_map::Range<'a, u64, SentPacket>),
    Cold(slice::Iter<'a, Slot>, slice::Iter<'a, Slot>),
}
impl<'a> Iterator for Iter<'a> {
    type Item = (&'a u64, &'a SentPacket);
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::All(iter) => iter.next(),
            Self::Range(iter) => iter.next(),
            Self::Cold(first, second) => loop {
                let slot = first.next().or_else(|| second.next())?;
                if let Some(packet) = &slot.packet {
                    return Some((&slot.number, packet));
                }
            },
        }
    }
}
pub(super) enum IterMut<'a> {
    Range(btree_map::RangeMut<'a, u64, SentPacket>),
    Legacy(btree_map::IterMut<'a, u64, SentPacket>),
    Cold(slice::IterMut<'a, Slot>, slice::IterMut<'a, Slot>),
}
impl<'a> Iterator for IterMut<'a> {
    type Item = (&'a u64, &'a mut SentPacket);
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Legacy(iter) => iter.next(),
            Self::Range(iter) => iter.next(),
            Self::Cold(first, second) => loop {
                let slot = first.next().or_else(|| second.next())?;
                if let Some(packet) = &mut slot.packet {
                    return Some((&slot.number, packet));
                }
            },
        }
    }
}
pub(super) struct Values<'a>(Iter<'a>);
impl<'a> Iterator for Values<'a> {
    type Item = &'a SentPacket;
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|(_, p)| p)
    }
}
pub(super) struct ValuesMut<'a>(IterMut<'a>);
impl<'a> Iterator for ValuesMut<'a> {
    type Item = &'a mut SentPacket;
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|(_, p)| p)
    }
}
impl<'a> IntoIterator for &'a History {
    type Item = (&'a u64, &'a SentPacket);
    type IntoIter = Iter<'a>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
impl<'a> IntoIterator for &'a mut History {
    type Item = (&'a u64, &'a mut SentPacket);
    type IntoIter = IterMut<'a>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter_mut()
    }
}
impl Index<&u64> for History {
    type Output = SentPacket;
    fn index(&self, number: &u64) -> &Self::Output {
        // The sole production index caller first collects this same key from
        // an immutable loss-range scan, without an intervening mutation/await.
        self.get(number)
            .expect("sent history indexed by an existing loss key")
    }
}

impl super::Connection {
    pub(super) fn reserve_sent_packet(
        &self,
        space: crate::packet::SpaceId,
    ) -> Result<PacketPermit, ReserveError> {
        let history = &self.spaces[space].sent_packets;
        #[cfg(feature = "sent-history-observe")]
        observe::cwnd(history, self.path.congestion.window());
        if history.ordinary_ready() {
            match history.reserve(PacketClass::Ordinary) {
                Err(ReserveError::Full) => {}
                result => return result,
            }
        }
        history.reserve(PacketClass::Control)
    }
    pub(super) fn fail_sent_history_capacity(
        &mut self,
        now: Instant,
        space: crate::packet::SpaceId,
    ) {
        if self.state.is_closed() {
            return;
        }
        #[cfg(test)]
        let before = self.spaces[space].next_packet_number;
        self.fail_control_inventory(now, "sent history control capacity exhausted");
        #[cfg(test)]
        self.note_sent_history_rejection(space, before);
    }
}

mod recovery;
pub(super) use recovery::{RecoveryKind, packet_is_lost, Projection};
