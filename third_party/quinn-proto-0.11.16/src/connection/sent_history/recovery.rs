//! Owns allocation-free recovery selections in the original sent inventory.
//! Per-record bits are part of the cold Slot layout. ACK/loss may nest with
//! distinct bits; the same kind cannot reenter. No callback runs under a borrow
//! of History. A committing guard interrupted by unwind keeps shared sticky Q.
//! Entries: History::{begin_recovery,select_recovery}, PacketSpace::take_recovery.
//! Tests: original encrypted admission/recovery and sent_history/tests/recovery.
use super::*;
use crate::TransportError;

#[derive(Clone, Copy)]
pub(in crate::connection) enum RecoveryKind {
    Ack = 1,
    Loss = 2,
    Projection = 4,
}

pub(in crate::connection) struct Selection {
    owner: Option<Arc<Credits>>,
    kind: RecoveryKind,
    remaining: usize,
    committing: bool,
    settled: bool,
}
impl Selection {
    fn bit(&self) -> u8 {
        self.kind as u8
    }
    pub(in crate::connection) fn select(&mut self, info: &mut SentPacket) {
        if info.recovery_selection & self.bit() == 0 {
            info.recovery_selection |= self.bit();
            self.remaining += 1;
        }
    }
    pub(in crate::connection) fn is_empty(&self) -> bool {
        self.remaining == 0
    }
    pub(in crate::connection) fn begin_commit(&mut self) -> Result<(), TransportError> {
        if let Some(owner) = &self.owner {
            owner
                .meter
                .admit_precharged()
                .map_err(|_| custody_error())?;
        }
        self.committing = true;
        Ok(())
    }
    fn poison(&self) {
        if let Some(owner) = &self.owner {
            owner.meter.quarantine();
        }
    }
    pub(in crate::connection) fn finish(mut self) -> Result<(), TransportError> {
        if self.remaining != 0 {
            self.poison();
            return Err(custody_error());
        }
        if let Some(owner) = &self.owner {
            let previous = owner
                .recovery_active
                .fetch_and(!(self.bit() as usize), Ordering::AcqRel);
            if previous & self.bit() as usize == 0 {
                self.poison();
                return Err(custody_error());
            }
        }
        self.settled = true;
        Ok(())
    }
}
impl Drop for Selection {
    fn drop(&mut self) {
        if !self.settled {
            self.poison();
        }
    }
}
fn custody_error() -> TransportError {
    TransportError::INTERNAL_ERROR("sent recovery custody lost")
}
impl History {
    pub(in crate::connection) fn begin_recovery(
        &self,
        kind: RecoveryKind,
    ) -> Result<Selection, TransportError> {
        let owner = match &self.mode {
            Mode::Legacy(_) => None,
            Mode::Cold(ring) => {
                ring.owner
                    .meter
                    .admit_precharged()
                    .map_err(|_| custody_error())?;
                let bit = kind as usize;
                ring.owner
                    .recovery_active
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |state| {
                        (if bit == RecoveryKind::Projection as usize { state == 0 }
                        else { state & (bit | RecoveryKind::Projection as usize) == 0 }).then_some(state | bit)
                    })
                    .map_err(|_| {
                        ring.owner.meter.quarantine();
                        custody_error()
                    })?;
                Some(ring.owner.clone())
            }
        };
        Ok(Selection {
            owner,
            kind,
            remaining: 0,
            committing: false,
            settled: false,
        })
    }
    fn owns_selection(&self, selection: &Selection) -> bool {
        match (&self.mode, &selection.owner) {
            (Mode::Legacy(_), None) => true,
            (Mode::Cold(ring), Some(owner)) if Arc::ptr_eq(&ring.owner, owner) => true,
            (Mode::Cold(ring), _) => {
                ring.owner.meter.quarantine();
                selection.poison();
                false
            }
            (_, _) => {
                selection.poison();
                false
            }
        }
    }
    pub(in crate::connection) fn select_recovery(
        &mut self,
        selection: &mut Selection,
        range: impl RangeBounds<u64>,
    ) -> Result<(), TransportError> {
        if !self.owns_selection(selection) || selection.committing {
            return Err(custody_error());
        }
        for (_, info) in self.recovery_range_mut(range) {
            selection.select(info);
        }
        Ok(())
    }
    pub(in crate::connection) fn recovery_ranges(
        &self,
        selection: &Selection,
    ) -> Result<usize, TransportError> {
        if !self.owns_selection(selection) {
            return Err(custody_error());
        }
        let mut previous = None;
        let mut ranges = 0;
        let mut count = 0;
        for (&pn, info) in self.iter() {
            if info.recovery_selection & selection.bit() == 0 {
                continue;
            }
            if previous.is_none() || previous != pn.checked_sub(1) {
                ranges += 1;
            }
            previous = Some(pn);
            count += 1;
        }
        if count != selection.remaining {
            selection.poison();
            return Err(custody_error());
        }
        Ok(ranges)
    }
    pub(in crate::connection) fn cancel_recovery(
        &mut self,
        mut selection: Selection,
    ) -> Result<(), TransportError> {
        if !self.owns_selection(&selection) || selection.committing {
            selection.poison();
            return Err(custody_error());
        }
        for (_, info) in self.iter_mut() {
            if info.recovery_selection & selection.bit() != 0 {
                if selection.remaining == 0 {
                    selection.poison();
                    return Err(custody_error());
                }
                info.recovery_selection &= !selection.bit();
                selection.remaining -= 1;
            }
        }
        selection.finish()
    }
    /// The original Connection has explicitly closed. Retire only this
    /// committing selection's qualification; remaining packets stay in History
    /// with their actual slots, metadata and in-flight debt until close disposal.
    pub(in crate::connection) fn close_recovery(
        &mut self,
        mut selection: Selection,
        state: &super::super::State,
    ) -> Result<(), TransportError> {
        if !state.is_closed() || !self.owns_selection(&selection) || !selection.committing
            || selection.owner.as_ref().is_some_and(|owner|
                owner.recovery_active.load(Ordering::Acquire) & selection.bit() as usize == 0)
        {
            selection.poison();
            return Err(custody_error());
        }
        let actual = self.iter().filter(|(_, info)| info.recovery_selection & selection.bit() != 0).count();
        if actual != selection.remaining {
            selection.poison();
            return Err(custody_error());
        }
        for (_, info) in self.iter_mut() {
            if info.recovery_selection & selection.bit() != 0 {
                info.recovery_selection &= !selection.bit();
                selection.remaining -= 1;
            }
        }
        selection.finish()
    }
    pub(in crate::connection) fn recovery_range_mut(
        &mut self,
        range: impl RangeBounds<u64>,
    ) -> IterMut<'_> {
        match &mut self.mode {
            Mode::Legacy(map) => IterMut::Range(map.range_mut(range)),
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
                let count = end.saturating_sub(start);
                let index = (ring.head + start) % ring.slots.len();
                let (before, after) = ring.slots.split_at_mut(index);
                let first = count.min(after.len());
                IterMut::Cold(
                    after[..first].iter_mut(),
                    before[..count - first].iter_mut(),
                )
            }
        }
    }
}
impl PacketSpace {
    pub(in crate::connection) fn take_recovery(
        &mut self,
        selection: &mut Selection,
        cursor: u64,
    ) -> Result<Option<(u64, SentPacket)>, TransportError> {
        if !selection.committing || !self.sent_packets.owns_selection(selection) {
            return Err(custody_error());
        }
        let next = self
            .sent_packets
            .range(cursor..)
            .find_map(|(&pn, info)| (info.recovery_selection & selection.bit() != 0).then_some(pn));
        let Some(pn) = next else {
            if selection.remaining != 0 {
                selection.poison();
                return Err(custody_error());
            }
            return Ok(None);
        };
        let Some(mut info) = self.take(pn) else {
            selection.poison();
            return Err(custody_error());
        };
        if selection.remaining == 0 {
            selection.poison();
            return Err(custody_error());
        }
        selection.remaining -= 1;
        info.recovery_selection &= !selection.bit();
        Ok(Some((pn, info)))
    }
}

/// Original packet/time-threshold test, shared with unpublished packet replay.
pub(in crate::connection) fn packet_is_lost(now: Instant, sent: Instant, packet: u64,
    largest: u64, delay: crate::Duration, threshold: u64) -> bool {
    now.saturating_duration_since(sent) >= delay || largest >= packet + threshold
}

/// Exclusive, unpublished use of the original cold selection bits. Never takes
/// a packet or a control node; Drop cancels all marks, including protocol errors.
pub(in crate::connection) struct Projection<'a> {
    history: &'a mut History,
    selection: Option<Selection>,
}
impl History {
    pub(in crate::connection) fn project_recovery(&mut self) -> Result<Projection<'_>, TransportError> {
        let selection = self.begin_recovery(RecoveryKind::Projection)?;
        if self.iter().any(|(_, p)| p.recovery_selection != 0) {
            selection.poison();
            return Err(custody_error());
        }
        Ok(Projection { history: self, selection: Some(selection) })
    }
}
impl Projection<'_> {
    pub(in crate::connection) fn get(&self, pn: u64) -> Option<&SentPacket> {
        self.history.get(&pn).filter(|p| p.recovery_selection & RecoveryKind::Projection as u8 == 0)
    }
    pub(in crate::connection) fn mark_where(&mut self, range: impl RangeBounds<u64>,
        mut select: impl FnMut(u64, &SentPacket) -> bool) {
        let Some(selection) = self.selection.as_mut() else { return; };
        for (&pn, info) in self.history.recovery_range_mut(range) {
            if info.recovery_selection & selection.bit() == 0 && select(pn, info) {
                selection.select(info);
            }
        }
    }
}
impl Drop for Projection<'_> {
    fn drop(&mut self) {
        if let Some(selection) = self.selection.take() {
            if std::thread::panicking() { selection.poison(); }
            // Cancellation checks exact count and original authority. Failure
            // retains quarantine; a known protocol rejection restores all bits.
            let _ = self.history.cancel_recovery(selection);
        }
    }
}
