//! Cold ordinary-lane composition checks. The original Inner remains the slot,
//! sequence and reservation authority; a Weak identity only links its physical
//! sender to its receiver. O(C) validation is never called by DATA operations.
//! Entries: Sender::channel_identity, Receiver::validate_ordinary. Tests:
//! owned_events/tests/inventory.rs (wrap/gaps, identity and physical corruption).
use super::*;

/// Read-only physical channel identity. It owns no values, permits or allowance.
pub struct ChannelIdentity<T>(std::sync::Weak<Inner<T>>);

impl<T> Sender<T> {
    pub fn matches_identity(&self, identity: &ChannelIdentity<T>) -> bool {
        identity.0.as_ptr() == Arc::as_ptr(&self.0)
    }
    pub fn channel_identity(&self) -> ChannelIdentity<T> {
        ChannelIdentity(Arc::downgrade(&self.0))
    }
}
impl<T> Receiver<T> {
    /// Validate the actual ordinary ring at cold construction or Device bind.
    /// `fresh` additionally requires the original unpolled, unused channel.
    /// Expected profile mismatch is a rejection; damaged physical inventory or
    /// a substituted same-meter channel seals the original authority into Q.
    pub fn validate_ordinary(
        &self,
        meter: &BackingMeter,
        capacity: usize,
        sender: &ChannelIdentity<T>,
        fresh: bool,
    ) -> io::Result<()> {
        if capacity == 0 || !self.0.meter.same_authority(meter) {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        meter.admit_precharged().map_err(AdmissionError)?;
        let mut state = self.0.lock();
        let actual_capacity = state.ordinary_capacity;
        let ordinary = state.control_indices.is_none() && state.terminal_index.is_none();
        let valid = sender.0.as_ptr() == Arc::as_ptr(&self.0)
            && !state.failed
            && state.cold_shape_matches(meter)
            && (!ordinary
                || (actual_capacity > 0
                    && state.control_reserved == [None; 2]
                    && !state.terminal_sent
                    && state.head < actual_capacity
                    && state.len <= actual_capacity
                    && state.ordinary_ring_valid()));
        if !valid {
            state.failed = true;
            state.sender_alive = false;
            state.receiver_alive = false;
            self.0.meter.quarantine();
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "ordinary lane physical inventory",
            ));
        }
        // Nonordinary profiles are not certified by this API. Verify only their
        // constructor layout above, then reject without auditing their protocol.
        if !ordinary {
            return Err(io::ErrorKind::Unsupported.into());
        }
        if actual_capacity != capacity
            || (fresh
                && (state.head != 0
                    || state.len != 0
                    || state.next_seq != 0
                    || state.reserved_seq.is_some()
                    || state.producer_waker.is_some()
                    || state.receiver_waker.is_some()))
        {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        if !state.sender_alive || !state.receiver_alive {
            return Err(io::ErrorKind::BrokenPipe.into());
        }
        Ok(())
    }
}
impl<T> State<T> {
    fn cold_shape_matches(&self, meter: &BackingMeter) -> bool {
        if self.ordinary_capacity == 0 && self.terminal_index.is_none() {
            return false;
        }
        let controls = usize::from(self.control_indices.is_some());
        let terminal = usize::from(self.terminal_index.is_some());
        let Some(count) = self
            .ordinary_capacity
            .checked_add(2 * controls)
            .and_then(|n| n.checked_add(terminal))
        else {
            return false;
        };
        if count == 0 || self.slots.len() != count {
            return false;
        }
        if self
            .control_indices
            .is_some_and(|indices| indices != [self.ordinary_capacity, self.ordinary_capacity + 1])
            || self
                .terminal_index
                .is_some_and(|index| index != self.ordinary_capacity + 2 * controls)
        {
            return false;
        }
        slot_layout::<T>(count).ok().is_some_and(|layout| {
            meter.cold_layout_matches(Some(&self._reservation), layout.size())
        })
    }
    // Called only after shape/range validation, so every slot access is bounded.
    fn ordinary_ring_valid(&self) -> bool {
        let mut physical = 0usize;
        for (index, slot) in self.slots.iter().enumerate() {
            let offset = if index >= self.head {
                index - self.head
            } else {
                self.ordinary_capacity - (self.head - index)
            };
            if slot.value.is_some() != (offset < self.len) {
                return false;
            }
            physical += usize::from(slot.value.is_some());
        }
        if physical != self.len {
            return false;
        }
        let mut previous = None;
        for offset in 0..self.len {
            // Avoid overflowing head+offset even for an extreme valid capacity.
            let tail = self.ordinary_capacity - self.head;
            let index = if offset < tail {
                self.head + offset
            } else {
                offset - tail
            };
            let seq = self.slots[index].seq;
            if seq >= self.next_seq || previous.is_some_and(|old| old >= seq) {
                return false;
            }
            previous = Some(seq);
        }
        self.reserved_seq.is_none_or(|reserved| {
            reserved < self.next_seq
                && previous.is_none_or(|last| last < reserved)
                && u64::try_from(self.ordinary_capacity - self.len)
                    .ok()
                    .is_some_and(|free| self.next_seq - reserved <= free)
        })
    }
}
