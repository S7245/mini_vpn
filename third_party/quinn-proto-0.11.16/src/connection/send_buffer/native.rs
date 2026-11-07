//! Original SendBuffer's Native physical inventory and explicit retirement.
//! Owns the actual segment deque and two slab AVL trees under its Connection
//! meter. Preparation precedes input consumption; commit allocates nothing.
//! ACK/loss/suffix handling shares this owner. Unknown interruption retains all
//! actual fields, not just a completion marker. Tests: send_buffer/tests/native.rs.
use super::*;
use crate::connection::storage::{intervals::{Intervals, Node}, Slab};
use bytes::{BackingBudgetReservation, BackingMeter, MeteredCopyError};
use std::{alloc::Layout, mem::ManuallyDrop};

#[derive(Debug, Default)]
pub(super) struct Segments {
    values: VecDeque<Bytes>,
    allowance: Option<BackingBudgetReservation>,
}
impl Segments {
    fn layout_matches(&self, meter: &BackingMeter) -> bool {
        Layout::array::<Bytes>(self.values.capacity()).is_ok_and(|layout|
            meter.cold_layout_matches(self.allowance.as_ref(), layout.size()))
    }
    fn prepare(&self, additional: usize, meter: &BackingMeter) -> Result<Option<Self>, MeteredCopyError> {
        let needed = self.values.len().checked_add(additional).ok_or(MeteredCopyError::TooLarge)?;
        if needed <= self.values.capacity() { return Ok(None); }
        let capacity = needed.max(self.values.capacity().checked_mul(2).ok_or(MeteredCopyError::TooLarge)?).max(4);
        let slab = Slab::prepare(capacity, Some(meter))?;
        Ok(Some(Self { values: VecDeque::from(slab.values), allowance: slab.allowance }))
    }
    fn install(&mut self, replacement: Option<Self>) {
        if let Some(mut new) = replacement {
            new.values.extend(self.values.drain(..));
            *self = new;
        }
    }
}

#[derive(Debug)]
pub(super) struct Native {
    pub(super) meter: BackingMeter,
    revision: u64,
    segments: ManuallyDrop<Segments>,
    acks: ManuallyDrop<Intervals>,
    retransmits: ManuallyDrop<Intervals>,
}
impl Native {
    pub(super) fn new(meter: BackingMeter) -> Self {
        Self { meter, revision: 0, segments: ManuallyDrop::new(Segments::default()),
            acks: ManuallyDrop::new(Intervals::default()), retransmits: ManuallyDrop::new(Intervals::default()) }
    }
    pub(super) fn stamp(&self) -> Result<Stamp, MeteredCopyError> {
        self.admit()?;
        if self.revision == u64::MAX { self.meter.quarantine(); return Err(MeteredCopyError::Quarantined); }
        Ok(Stamp { address: self as *const Self as usize, revision: self.revision })
    }
    fn validate(&self, stamp: Stamp, owner: &BackingMeter) -> Result<(), MeteredCopyError> {
        if self.meter.same_authority(owner) && stamp.address == self as *const Self as usize
            && stamp.revision == self.revision && self.revision != u64::MAX {
            self.meter.admit_precharged()
        } else {
            self.meter.quarantine(); owner.quarantine(); Err(MeteredCopyError::Quarantined)
        }
    }
    pub(super) fn changed(&mut self) {
        self.revision = self.revision.saturating_add(1);
    }
    fn admit(&self) -> Result<(), MeteredCopyError> {
        self.meter.admit_precharged()?;
        if !self.segments.layout_matches(&self.meter) || !self.acks.layout_matches(&self.meter)
            || !self.retransmits.layout_matches(&self.meter) {
            self.meter.quarantine();
            return Err(MeteredCopyError::Quarantined);
        }
        Ok(())
    }
    pub(super) fn prepare(&self, chunks: usize) -> Result<Option<Segments>, MeteredCopyError> {
        self.admit()?;
        self.segments.prepare(chunks, &self.meter)
    }
    pub(super) fn install(&mut self, replacement: Option<Segments>, stamp: Stamp, owner: &BackingMeter) -> Result<(), MeteredCopyError> {
        self.validate(stamp, owner)?;
        self.changed();
        self.segments.install(replacement);
        Ok(())
    }
    pub(super) fn push(&mut self, value: Bytes) { self.changed(); self.segments.values.push_back(value); }
    pub(super) fn get(&self, offsets: Range<u64>, base: u64) -> &[u8] {
        let mut start = base;
        for segment in &self.segments.values {
            let end = start + segment.len() as u64;
            if offsets.start >= start && offsets.start < end {
                return &segment[(offsets.start - start) as usize..((offsets.end - start) as usize).min(segment.len())];
            }
            start = end;
        }
        &[]
    }
    pub(super) fn prepare_packet_capacity(&self, acks: usize, losses: usize) -> Result<PacketCapacity, MeteredCopyError> {
        let stamp = self.stamp()?;
        Ok(PacketCapacity {
            stamp, owner: self.meter.clone(),
            acks: self.acks.prepare_insertions(acks, &self.meter)?,
            losses: self.retransmits.prepare_insertions(losses, &self.meter)?,
        })
    }
    pub(super) fn install_packet_capacity(&mut self, prepared: PacketCapacity) -> Result<(), MeteredCopyError> {
        self.validate(prepared.stamp, &prepared.owner)?;
        self.acks.install(prepared.acks);
        self.retransmits.install(prepared.losses);
        self.changed();
        Ok(())
    }
    pub(super) fn prepare_ack(&self, range: Range<u64>, offset: u64, unacked: usize) -> Result<Ack, MeteredCopyError> {
        self.admit()?;
        if range.end > offset || unacked as u64 > offset {
            self.meter.quarantine(); return Err(MeteredCopyError::Quarantined);
        }
        let base = offset - unacked as u64;
        let range = range.start.max(base)..range.end.max(base);
        let stamp = self.stamp()?;
        let replacement = self.acks.prepare(range.clone(), Some(&self.meter))?;
        Ok(Ack { range, replacement, owner: self.meter.clone(), stamp, kind: Kind::Ack })
    }
    pub(super) fn ack(&mut self, plan: Ack, offset: u64, unacked: &mut usize) -> Result<(), MeteredCopyError> {
        self.validate(plan.stamp, &plan.owner)?;
        if plan.kind != Kind::Ack { self.meter.quarantine(); return Err(MeteredCopyError::Quarantined); }
        self.changed();
        let Ack { range, replacement, .. } = plan;
        self.acks.install(replacement);
        self.acks.insert(range);
        while self.acks.min() == Some(offset - *unacked as u64) {
            let Some(prefix) = self.acks.pop_min() else { self.meter.quarantine(); return Err(MeteredCopyError::Quarantined); };
            let mut remaining = (prefix.end - prefix.start) as usize;
            if remaining > *unacked { self.meter.quarantine(); return Err(MeteredCopyError::Quarantined); }
            *unacked -= remaining;
            while remaining > 0 {
                let Some(front) = self.segments.values.front_mut() else { self.meter.quarantine(); return Err(MeteredCopyError::Quarantined); };
                if front.len() <= remaining {
                    remaining -= front.len();
                    drop(self.segments.values.pop_front());
                } else {
                    front.advance(remaining);
                    remaining = 0;
                }
            }
        }
        Ok(())
    }
    pub(super) fn prepare_retransmit(&self, range: Range<u64>) -> Result<Ack, MeteredCopyError> {
        self.admit()?;
        let stamp = self.stamp()?;
        let replacement = self.retransmits.prepare(range.clone(), Some(&self.meter))?;
        Ok(Ack { range, replacement, owner: self.meter.clone(), stamp, kind: Kind::Loss })
    }
    pub(super) fn retransmit(&mut self, plan: Ack) -> Result<(), MeteredCopyError> {
        self.validate(plan.stamp, &plan.owner)?;
        if plan.kind != Kind::Loss { self.meter.quarantine(); return Err(MeteredCopyError::Quarantined); }
        self.changed();
        self.retransmits.install(plan.replacement);
        self.retransmits.insert(plan.range);
        Ok(())
    }
    pub(super) fn ack_covers(&self, range: Range<u64>, base: u64, end: u64) -> bool {
        super::covers(range, base, end, self.acks.overlaps(base..end))
    }
    pub(super) fn pop_retransmit(&mut self) -> Option<Range<u64>> { self.changed(); self.retransmits.pop_min() }
    pub(super) fn put_suffix(&mut self, range: Range<u64>) {
        // pop_min returned the actual node to this tree; the suffix consumes
        // that same free slot. No allocation or new admission is possible here.
        self.changed();
        self.retransmits.insert(range);
    }
    pub(super) fn metadata_bytes(&self) -> Option<usize> {
        self.segments.layout_matches(&self.meter).then_some(())?;
        Layout::array::<Bytes>(self.segments.values.capacity()).ok()?.size()
            .checked_add(self.acks.metadata_bytes(&self.meter)?)?
            .checked_add(self.retransmits.metadata_bytes(&self.meter)?)
    }
    pub(super) fn has_retransmits(&self) -> bool { self.retransmits.min().is_some() }
    pub(super) fn acknowledged(&self) -> u64 { self.acks.bytes() }
    pub(super) fn inventory_matches(&self, unacked: usize) -> bool {
        self.segments.layout_matches(&self.meter)
            && self.segments.values.iter().try_fold(0usize, |n, v| n.checked_add(v.len())) == Some(unacked)
            && self.segments.values.iter().all(|v| v.backing_meter().is_some_and(|m| m.same_budget(&self.meter)))
            && self.acks.inventory_matches(&self.meter) && self.retransmits.inventory_matches(&self.meter)
    }
}
impl Drop for Native {
    fn drop(&mut self) {
        if std::thread::panicking() { self.meter.quarantine(); }
        if self.meter.admit_precharged().is_err() { return; }
        // These fields contain only our own slabs and metered Shared Bytes;
        // arbitrary owner-backed Bytes never enter the Native commit path.
        unsafe {
            ManuallyDrop::drop(&mut self.segments);
            ManuallyDrop::drop(&mut self.acks);
            ManuallyDrop::drop(&mut self.retransmits);
        }
    }
}

#[derive(Clone, Copy)]
pub(super) struct Stamp { address: usize, revision: u64 }
#[derive(PartialEq)]
enum Kind { Ack, Loss }
pub(super) struct Ack {
    range: Range<u64>, replacement: Option<Slab<Node>>, pub(super) owner: BackingMeter,
    stamp: Stamp, kind: Kind,
}

#[cfg(test)]
#[path = "tests/inventory.rs"]
mod tests;

// Actual replacements stay unpublished until the Connection has prepared both
// sending recovery and receiving data. No protocol offsets change here.
pub(super) struct PacketCapacity {
    stamp: Stamp,
    owner: BackingMeter,
    acks: Option<Slab<Node>>,
    losses: Option<Slab<Node>>,
}
