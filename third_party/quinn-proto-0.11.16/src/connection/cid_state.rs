//! Maintain the state of local connection IDs
#[cfg(test)]
mod tests;
use std::collections::VecDeque;

use rustc_hash::FxHashSet;
use tracing::{debug, trace};

use crate::{shared::IssuedCid, Duration, Instant, TransportError};

/// Local connection ID management
pub(super) struct CidState {
    /// Timestamp when issued cids should be retired
    retire_timestamp: VecDeque<CidTimestamp>,
    /// Next generated sequence, including handshake IDs; not proof of exposure.
    issued: u64,
    /// Highest sequence potentially exposed in handshake metadata or encoded
    /// by the original frame writer. This is not a UDP delivery witness.
    largest_encoded_seq: u64,
    /// Sequence numbers of local connection IDs not yet retired by the peer
    active_seq: Active,
    /// Sequence number the peer has already retired all CIDs below at our request via `retire_prior_to`
    prev_retire_seq: u64,
    /// Sequence number to set in retire_prior_to field in NEW_CONNECTION_ID frame
    retire_seq: u64,
    /// cid length used to decode short packet
    cid_len: usize,
    //// cid lifetime
    cid_lifetime: Option<Duration>,
}

impl CidState {
    pub(crate) fn new(
        cid_len: usize,
        cid_lifetime: Option<Duration>,
        now: Instant,
        issued: u64,
    ) -> Self {
        let mut active_seq = Active::Legacy(FxHashSet::default());
        for seq in 0..issued {
            active_seq.insert(seq);
        }
        Self::with_active(cid_len, cid_lifetime, now, issued, active_seq)
    }

    pub(crate) fn new_native(
        cid_len: usize,
        now: Instant,
        issued: u64,
    ) -> Result<Self, TransportError> {
        if issued > crate::LOC_CID_COUNT {
            return Err(cid_capacity());
        }
        let mut active_seq = Active::Native([None; crate::LOC_CID_COUNT as usize]);
        for seq in 0..issued {
            if !active_seq.insert(seq) {
                return Err(cid_capacity());
            }
        }
        Ok(Self::with_active(cid_len, None, now, issued, active_seq))
    }
    fn with_active(
        cid_len: usize,
        cid_lifetime: Option<Duration>,
        now: Instant,
        issued: u64,
        active_seq: Active,
    ) -> Self {
        let mut this = Self {
            retire_timestamp: VecDeque::new(),
            issued,
            largest_encoded_seq: issued.saturating_sub(1),
            active_seq,
            prev_retire_seq: 0,
            retire_seq: 0,
            cid_len,
            cid_lifetime,
        };
        // Track lifetime of CIDs used in handshaking
        for seq in 0..issued {
            this.track_lifetime(seq, now);
        }
        this
    }

    /// Find the next timestamp when previously issued CID should be retired
    pub(crate) fn next_timeout(&mut self) -> Option<Instant> {
        self.retire_timestamp.front().map(|nc| {
            trace!("CID {} will expire at {:?}", nc.sequence, nc.timestamp);
            nc.timestamp
        })
    }

    /// Track the lifetime of issued cids in `retire_timestamp`
    fn track_lifetime(&mut self, new_cid_seq: u64, now: Instant) {
        let lifetime = match self.cid_lifetime {
            Some(lifetime) => lifetime,
            None => return,
        };

        let expire_timestamp = now.checked_add(lifetime);
        let expire_at = match expire_timestamp {
            Some(expire_at) => expire_at,
            None => return,
        };

        let last_record = self.retire_timestamp.back_mut();
        if let Some(last) = last_record {
            // Compare the timestamp with the last inserted record
            // Combine into a single batch if timestamp of current cid is same as the last record
            if expire_at == last.timestamp {
                debug_assert!(new_cid_seq > last.sequence);
                last.sequence = new_cid_seq;
                return;
            }
        }

        self.retire_timestamp.push_back(CidTimestamp {
            sequence: new_cid_seq,
            timestamp: expire_at,
        });
    }

    /// Update local CID state when previously issued CID is retired
    ///
    /// Return whether a new CID needs to be pushed that notifies remote peer to respond `RETIRE_CONNECTION_ID`
    pub(crate) fn on_cid_timeout(&mut self) -> bool {
        // Whether the peer hasn't retired all the CIDs we asked it to yet
        let unretired_ids_found =
            (self.prev_retire_seq..self.retire_seq).any(|seq| self.active_seq.contains(&seq));

        let current_retire_prior_to = self.retire_seq;
        let next_retire_sequence = self
            .retire_timestamp
            .pop_front()
            .map(|seq| seq.sequence + 1);

        // According to RFC:
        // Endpoints SHOULD NOT issue updates of the Retire Prior To field
        // before receiving RETIRE_CONNECTION_ID frames that retire all
        // connection IDs indicated by the previous Retire Prior To value.
        // https://tools.ietf.org/html/draft-ietf-quic-transport-29#section-5.1.2
        if !unretired_ids_found {
            // All Cids are retired, `prev_retire_cid_seq` can be assigned to `retire_cid_seq`
            self.prev_retire_seq = self.retire_seq;
            // Advance `retire_seq` if next cid that needs to be retired exists
            if let Some(next_retire_prior_to) = next_retire_sequence {
                self.retire_seq = next_retire_prior_to;
            }
        }

        // Check if retirement of all CIDs that reach their lifetime is still needed
        // According to RFC:
        // An endpoint MUST NOT
        // provide more connection IDs than the peer's limit.  An endpoint MAY
        // send connection IDs that temporarily exceed a peer's limit if the
        // NEW_CONNECTION_ID frame also requires the retirement of any excess,
        // by including a sufficiently large value in the Retire Prior To field.
        //
        // If yes (return true), a new CID must be pushed with updated `retire_prior_to` field to remote peer.
        // If no (return false), it means CIDs that reach the end of lifetime have been retired already. Do not push a new CID in order to avoid violating above RFC.
        (current_retire_prior_to..self.retire_seq).any(|seq| self.active_seq.contains(&seq))
    }

    /// Update cid state when `NewIdentifiers` event is received
    pub(crate) fn new_cids(
        &mut self,
        ids: impl IntoIterator<Item = IssuedCid>,
        now: Instant,
    ) -> Result<(), TransportError> {
        if matches!(self.active_seq, Active::Native(_)) {
            let mut count = 0u64;
            let mut seen = 0u16;
            for frame in ids {
                let offset = frame
                    .sequence
                    .checked_sub(self.issued)
                    .ok_or_else(cid_capacity)?;
                if offset >= crate::LOC_CID_COUNT || seen & (1u16 << offset) != 0 {
                    return Err(cid_capacity());
                }
                seen |= 1u16 << offset;
                count += 1;
            }
            if seen != (1u16 << count) - 1 {
                return Err(cid_capacity());
            }
            let prepared = self.prepare_native(self.issued, count)?;
            self.install_native(prepared);
            return Ok(());
        }
        let mut last = None;
        for frame in ids {
            self.issued += 1;
            self.active_seq.insert(frame.sequence);
            last = Some(last.map_or(frame.sequence, |old: u64| old.max(frame.sequence)));
        }
        if let Some(sequence) = last {
            self.track_lifetime(sequence, now);
        }
        Ok(())
    }
    pub(crate) fn prepare_native(
        &self,
        sequence: u64,
        count: u64,
    ) -> Result<PreparedNative, TransportError> {
        let Active::Native(active) = &self.active_seq else {
            return Err(cid_capacity());
        };
        if sequence != self.issued || count > crate::LOC_CID_COUNT {
            return Err(cid_capacity());
        }
        let mut prepared = PreparedNative {
            active: *active,
            issued: self.issued,
        };
        for _ in 0..count {
            prepared.push(prepared.issued)?;
        }
        Ok(prepared)
    }
    pub(crate) fn install_native(&mut self, prepared: PreparedNative) {
        self.active_seq = Active::Native(prepared.active);
        self.issued = prepared.issued;
    }

    /// Record the original writer's completed frame encoding, before that
    /// same record moves into SentFrames. Retransmission cannot lower this.
    pub(crate) fn mark_encoded(&mut self, sequence: u64) {
        self.largest_encoded_seq = self.largest_encoded_seq.max(sequence);
    }

    /// Update CidState for receipt of a `RETIRE_CONNECTION_ID` frame
    ///
    /// Returns a new retirement and whether a CID can be issued. A duplicate
    /// has no Endpoint effect and returns None after the original validation.
    pub(crate) fn on_cid_retirement(
        &mut self,
        sequence: u64,
        limit: u64,
    ) -> Result<Option<bool>, TransportError> {
        self.check_retirement(sequence)?;
        if !self.active_seq.remove(&sequence) {
            return Ok(None);
        }
        // Consider a scenario where peer A has active remote cid 0,1,2.
        // Peer B first send a NEW_CONNECTION_ID with cid 3 and retire_prior_to set to 1.
        // Peer A processes this NEW_CONNECTION_ID frame; update remote cid to 1,2,3
        // and meanwhile send a RETIRE_CONNECTION_ID to retire cid 0 to peer B.
        // If peer B doesn't check the cid limit here and send a new cid again, peer A will then face CONNECTION_ID_LIMIT_ERROR
        Ok(Some(limit > self.active_seq.len() as u64))
    }

    pub(super) fn check_retirement(&self, sequence: u64) -> Result<(), TransportError> {
        if self.cid_len == 0 {
            return Err(TransportError::PROTOCOL_VIOLATION(
                "RETIRE_CONNECTION_ID when CIDs aren't in use",
            ));
        }
        if sequence > self.largest_encoded_seq {
            debug!(
                sequence,
                "got RETIRE_CONNECTION_ID for unissued sequence number"
            );
            return Err(TransportError::PROTOCOL_VIOLATION(
                "RETIRE_CONNECTION_ID for unissued sequence number",
            ));
        }
        Ok(())
    }

    /// Length of local Connection IDs
    pub(crate) fn cid_len(&self) -> usize {
        self.cid_len
    }

    /// The value for `retire_prior_to` field in `NEW_CONNECTION_ID` frame
    pub(crate) fn retire_prior_to(&self) -> u64 {
        self.retire_seq
    }

    #[cfg(test)]
    pub(crate) fn active_seq(&self) -> (u64, u64) {
        let mut min = u64::MAX;
        let mut max = u64::MIN;
        for n in self.active_seq.iter() {
            if n < &min {
                min = *n;
            }
            if n > &max {
                max = *n;
            }
        }
        (min, max)
    }

    #[cfg(test)]
    pub(crate) fn assign_retire_seq(&mut self, v: u64) -> u64 {
        // Cannot retire more CIDs than what have been issued
        debug_assert!(v <= *self.active_seq.iter().max().unwrap() + 1);
        let n = v.checked_sub(self.retire_seq).unwrap();
        self.retire_seq = v;
        n
    }
}

/// Data structure that records when issued cids should be retired
#[derive(Copy, Clone, Eq, PartialEq)]
struct CidTimestamp {
    /// Highest cid sequence number created in a batch
    sequence: u64,
    /// Timestamp when cid needs to be retired
    timestamp: Instant,
}

// Native's no-lifetime profile owns exactly eight inline active sequences. The
// Legacy set and lifetime behavior retain their original semantics; no heap or
// new refund authority is introduced for the fixed instance state.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Active {
    Legacy(FxHashSet<u64>),
    Native([Option<u64>; crate::LOC_CID_COUNT as usize]),
}
impl Active {
    fn len(&self) -> usize {
        match self {
            Self::Legacy(set) => set.len(),
            Self::Native(slots) => slots.iter().flatten().count(),
        }
    }
    fn contains(&self, sequence: &u64) -> bool {
        match self {
            Self::Legacy(set) => set.contains(sequence),
            Self::Native(slots) => slots.contains(&Some(*sequence)),
        }
    }
    fn insert(&mut self, sequence: u64) -> bool {
        match self {
            Self::Legacy(set) => set.insert(sequence),
            Self::Native(slots) => {
                if slots.contains(&Some(sequence)) {
                    return false;
                }
                let Some(slot) = slots.iter_mut().find(|slot| slot.is_none()) else {
                    return false;
                };
                *slot = Some(sequence);
                true
            }
        }
    }
    fn remove(&mut self, sequence: &u64) -> bool {
        match self {
            Self::Legacy(set) => set.remove(sequence),
            Self::Native(slots) => {
                let Some(slot) = slots.iter_mut().find(|slot| **slot == Some(*sequence)) else {
                    return false;
                };
                *slot = None;
                true
            }
        }
    }
    fn iter(&self) -> impl Iterator<Item = &u64> {
        let (legacy, native) = match self {
            Self::Legacy(set) => (Some(set), None),
            Self::Native(slots) => (None, Some(slots)),
        };
        legacy
            .into_iter()
            .flat_map(|set| set.iter())
            .chain(native.into_iter().flat_map(|slots| slots.iter().flatten()))
    }
}
pub(crate) struct PreparedNative {
    active: [Option<u64>; crate::LOC_CID_COUNT as usize],
    issued: u64,
}
impl PreparedNative {
    fn push(&mut self, sequence: u64) -> Result<(), TransportError> {
        if sequence != self.issued
            || sequence > u64::from(crate::VarInt::MAX)
            || self.active.contains(&Some(sequence))
        {
            return Err(cid_capacity());
        }
        let next = self.issued.checked_add(1).ok_or_else(cid_capacity)?;
        let slot = self
            .active
            .iter_mut()
            .find(|slot| slot.is_none())
            .ok_or_else(cid_capacity)?;
        *slot = Some(sequence);
        self.issued = next;
        Ok(())
    }
}
fn cid_capacity() -> TransportError {
    TransportError::INTERNAL_ERROR("native CID sequence or active inventory unavailable")
}
