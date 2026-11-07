//! Test-only peer stimulus and explicitly labelled negative ownership faults.
//! This owns no receive path or admission substitute.

use super::{Connection, SpaceId};

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct CryptoSnapshot {
    pub(crate) capacity: usize,
    pub(crate) free: usize,
    pub(crate) stored: usize,
    pub(crate) pending: usize,
    pub(crate) sent: usize,
    pub(crate) hello: Option<(usize, usize)>,
    pub(crate) initial: Option<(u64, usize, usize)>,
    pub(crate) barriers: [usize; 3],
    pub(crate) offsets: [u64; 3],
    pub(crate) pn: [u64; 3],
}

impl Connection {
    pub(crate) fn stream_layouts_for_probe(&self) -> Vec<(Option<bytes::AllocationIdentity>, usize)> {
        self.streams.actual_layouts_for_probe()
    }
    pub(crate) fn stream_packets_for_probe(&self) -> Vec<(u64, usize, u8)> {
        self.spaces[SpaceId::Data].sent_packets.iter()
            .filter(|(_, packet)| !packet.stream_frames.is_empty())
            .map(|(&pn, packet)| (pn, packet.stream_frames.len(), packet.recovery_selection))
            .collect()
    }
    /// Explicit custody fault: retain the actual already-installed replacement
    /// Frames without running the writer. Not an alternative admission path.
    pub(crate) fn take_pending_cid_reply_for_probe(&mut self, now: crate::Instant) -> Option<crate::ConnectionEvent> {
        let frames = &mut self.spaces[SpaceId::Data].pending_cids;
        if frames.is_empty() { return None; }
        let empty = super::cid_frames::Frames::empty(frames.cold_pool());
        let held = std::mem::replace(frames, empty);
        Some(crate::ConnectionEvent(crate::shared::ConnectionEventInner::NewIdentifiers(held, now)))
    }
    /// Controlled Root maximum-boundary stimulus, not an authenticated peer negotiation.
    pub(crate) fn retire_local_for_root_boundary(
        &mut self,
        now: crate::Instant,
        sequence: u64,
        limit: u64,
    ) -> Result<(), crate::TransportError> {
        if !self.endpoint_events.begin_control(1) {
            return Err(crate::TransportError::INTERNAL_ERROR(
                "test control admission",
            ));
        }
        let guard = self.endpoint_events.guard();
        let result = self.local_cid_state.on_cid_retirement(sequence, limit);
        if let Ok(Some(allow_more)) = result {
            self.endpoint_events
                .push_back(crate::shared::EndpointEventInner::RetireConnectionId(
                    now, sequence, allow_more,
                ));
        }
        self.endpoint_events.finish();
        guard.finish();
        result.map(|_| ())
    }
    pub(crate) fn validation_token_for_cache_probe(&self) -> Option<&bytes::Bytes> {
        match &self.side {
            super::ConnectionSide::Client { token, .. } => Some(token),
            _ => None,
        }
    }

    pub(crate) fn remote_path_for_probe(
        &self,
    ) -> (std::net::SocketAddr, u64, std::time::Duration, u64) {
        (
            self.path.remote,
            self.path_counter,
            self.path.rtt.get(),
            self.path.congestion.window(),
        )
    }
    pub(crate) fn swap_sent_retire_for_probe(&mut self, other: &mut Self) -> bool {
        let left = self.spaces[SpaceId::Data]
            .sent_packets
            .values_mut()
            .find(|p| {
                p.retransmits
                    .get()
                    .is_some_and(|m| !m.retire_cids.is_empty())
            });
        let right = other.spaces[SpaceId::Data]
            .sent_packets
            .values_mut()
            .find(|p| {
                p.retransmits
                    .get()
                    .is_some_and(|m| !m.retire_cids.is_empty())
            });
        match (left, right) {
            (Some(a), Some(b)) => {
                std::mem::swap(
                    &mut a.retransmits.get_or_create().retire_cids,
                    &mut b.retransmits.get_or_create().retire_cids,
                );
                true
            }
            _ => false,
        }
    }
    pub(crate) fn remote_cid_queue_for_probe(&self) -> crate::cid_queue::CidQueue {
        self.rem_cids.clone()
    }
    pub(crate) fn retire_stock_for_probe(&self) -> (usize, usize, usize, Vec<u64>, usize) {
        let space = &self.spaces[SpaceId::Data];
        let (capacity, free, stored) = space.pending.retire_cids.cold_stock().unwrap();
        let sent = space
            .sent_packets
            .values()
            .map(|p| p.retransmits.get().map_or(0, |m| m.retire_cids.len()))
            .sum();
        (
            capacity,
            free,
            stored,
            space.pending.retire_cids.iter().collect(),
            sent,
        )
    }

    pub(crate) fn remote_cid_state_for_probe(
        &self,
    ) -> (crate::ConnectionId, u64, Option<crate::ResetToken>) {
        (
            self.rem_cids.active(),
            self.rem_cids.active_seq(),
            self.peer_params.stateless_reset_token,
        )
    }

    pub(crate) fn swap_sent_crypto_for_probe(&mut self, other: &mut Self) -> bool {
        let left = self.spaces[SpaceId::Initial]
            .sent_packets
            .values_mut()
            .find(|p| p.retransmits.get().is_some_and(|r| !r.crypto.is_empty()));
        let right = other.spaces[SpaceId::Initial]
            .sent_packets
            .values_mut()
            .find(|p| p.retransmits.get().is_some_and(|r| !r.crypto.is_empty()));
        match (left, right) {
            (Some(left), Some(right)) => {
                std::mem::swap(
                    &mut left.retransmits.get_or_create().crypto,
                    &mut right.retransmits.get_or_create().crypto,
                );
                true
            }
            _ => false,
        }
    }

    pub(crate) fn crypto_snapshot_for_probe(&self) -> CryptoSnapshot {
        let (capacity, free, stored) = self.spaces[SpaceId::Initial]
            .pending
            .reset_stream
            .class_stock(2)
            .unwrap();
        let hello = if let super::State::Handshake(state) = &self.state {
            state
                .client_hello
                .as_ref()
                .map(|h| (h.frame.data.as_ptr() as usize, h.frame.data.len()))
        } else {
            None
        };
        CryptoSnapshot {
            capacity,
            free,
            stored,
            hello,
            initial: self.spaces[SpaceId::Initial]
                .pending
                .crypto
                .frames()
                .first()
                .map(|f| (f.offset, f.data.as_ptr() as usize, f.data.len())),
            pending: self.spaces.iter().map(|s| s.pending.crypto.len()).sum(),
            sent: self
                .spaces
                .iter()
                .flat_map(|s| s.sent_packets.values())
                .map(|p| p.retransmits.get().map_or(0, |m| m.crypto.len()))
                .sum(),
            barriers: self.crypto_barriers,
            offsets: std::array::from_fn(|i| self.spaces[i].crypto_offset),
            pn: std::array::from_fn(|i| self.spaces[i].next_packet_number),
        }
    }
    pub(crate) fn crypto_pending_backings_for_probe(&self) -> (usize, usize, usize) {
        let mut result = (0, 0, 0);
        for space in self.spaces.iter() {
            for frame in space.pending.crypto.frames() {
                result.0 += 1;
                result.1 += usize::from(frame.data.backing_meter().is_some());
                result.2 += frame.data.len();
            }
        }
        result
    }

    pub(crate) fn flow_credit_stock_for_probe(&self) -> (usize, usize, usize, usize, usize) {
        let (capacity, free, values) = self.spaces[SpaceId::Data]
            .pending
            .reset_stream
            .cold_stock()
            .unwrap();
        let pending = self
            .spaces
            .iter()
            .map(|s| s.pending.max_stream_data.len())
            .sum();
        let sent = self
            .spaces
            .iter()
            .flat_map(|s| s.sent_packets.values())
            .map(|p| p.retransmits.get().map_or(0, |m| m.max_stream_data.len()))
            .sum();
        (capacity, free, values, pending, sent)
    }
    pub(crate) fn stream_record_stock_for_probe(&self) -> (usize, usize, usize, usize) {
        let (capacity, free, values) = self.spaces[SpaceId::Data]
            .pending
            .reset_stream
            .class_stock(1)
            .unwrap();
        let sent = self
            .spaces
            .iter()
            .flat_map(|s| s.sent_packets.values())
            .map(|p| p.stream_frames.len())
            .sum();
        (capacity, free, values, sent)
    }
    pub(crate) fn stream_encoding_for_probe(&self, id: crate::StreamId) -> Option<(u64, bool)> {
        self.streams.encoding_for_probe(id)
    }

    pub(crate) fn control_record_stock_for_probe(&self) -> (usize, usize, usize, usize, usize) {
        let (capacity, free, values) = self.spaces[SpaceId::Data]
            .pending
            .reset_stream
            .cold_stock()
            .unwrap();
        let pending = self
            .spaces
            .iter()
            .map(|s| s.pending.reset_stream.len() + s.pending.stop_sending.len())
            .sum();
        let sent = self
            .spaces
            .iter()
            .flat_map(|s| s.sent_packets.values())
            .map(|p| p.retransmits.record_len())
            .sum();
        (capacity, free, pending, sent, values)
    }
    pub(crate) fn next_data_number_for_record_probe(&self) -> u64 {
        self.spaces[SpaceId::Data].next_packet_number
    }
    pub(crate) fn swap_sent_reset_records_for_probe(&mut self, other: &mut Self) -> bool {
        let left = self.spaces[SpaceId::Data]
            .sent_packets
            .values_mut()
            .find(|p| p.retransmits.reset_iter().next().is_some());
        let right = other.spaces[SpaceId::Data]
            .sent_packets
            .values_mut()
            .find(|p| p.retransmits.reset_iter().next().is_some());
        match (left, right) {
            (Some(a), Some(b)) => {
                std::mem::swap(&mut a.retransmits.records, &mut b.retransmits.records);
                true
            }
            _ => false,
        }
    }

    pub(crate) fn original_close_deadline_for_stream_probe(
        &self,
        now: crate::Instant,
    ) -> crate::Instant {
        now + 3 * self.pto(self.highest_space)
    }

    pub(crate) fn queue_repeated_retirement_for_capacity_probe(
        &mut self,
        sequence: u64,
        count: usize,
    ) {
        let pending = &mut self.spaces[SpaceId::Data].pending.retire_cids;
        for _ in 0..count {
            pending.push(
                pending
                    .prepare(sequence)
                    .expect("test retirement stimulus credit"),
            );
        }
    }

    pub(crate) fn cid_frame_stock_for_probe(&self, sequence: u64) -> (usize, usize) {
        let space = &self.spaces[SpaceId::Data];
        let pending = space
            .pending_cids
            .iter()
            .filter(|cid| cid.sequence == sequence)
            .count();
        let sent = space
            .sent_packets
            .values()
            .flat_map(|packet| &packet.retransmits.new_cids)
            .filter(|cid| cid.sequence == sequence)
            .count();
        (pending, sent)
    }

    pub(crate) fn cid_general_boxes_for_probe(&self, sequence: u64) -> usize {
        self.spaces[SpaceId::Data]
            .sent_packets
            .values()
            .filter(|packet| {
                packet
                    .retransmits
                    .new_cids
                    .iter()
                    .any(|cid| cid.sequence == sequence)
                    && packet.retransmits.get().is_some()
            })
            .count()
    }
    pub(crate) fn cid_pool_stock_for_probe(&self) -> (usize, usize, usize, usize) {
        let space = &self.spaces[SpaceId::Data];
        let (free, pending, encoded) = space.pending_cids.cold_stock_for_probe().unwrap();
        let sent = space
            .sent_packets
            .values()
            .map(|packet| packet.retransmits.new_cids.iter().count())
            .sum();
        (free, pending, encoded, sent)
    }
    pub(crate) fn sent_history_len_for_capacity_probe(&self) -> usize {
        self.spaces
            .iter()
            .map(|space| space.sent_packets.len())
            .sum()
    }
    pub(super) fn note_sent_history_rejection(&mut self, space: SpaceId, before: u64) {
        self.history_rejection = Some((
            space,
            before,
            self.spaces[space].next_packet_number,
            self.spaces[space].sent_packets.occupied(),
        ));
    }
    pub(crate) fn sent_history_rejection_for_probe(&self) -> Option<(SpaceId, u64, u64, usize)> {
        self.history_rejection
    }
    pub(crate) fn sent_history_data_state_for_probe(&self) -> (u64, usize) {
        let space = &self.spaces[SpaceId::Data];
        (space.next_packet_number, space.sent_packets.occupied())
    }
    pub(crate) fn pending_ack_ranges_for_recovery_probe(
        &self,
        space: SpaceId,
    ) -> (usize, Option<usize>, usize) {
        let ranges = self.spaces[space].pending_acks.ranges();
        let (capacity, peak) = ranges.cold_inventory();
        (ranges.len(), capacity, peak)
    }
    pub(crate) fn key_epoch_for_recovery_probe(&self) -> (bool, u64) {
        (self.key_phase, self.key_phase_size)
    }
    pub(crate) fn corrupt_ack_range_shape_for_recovery_probe(&mut self) -> bool {
        self.spaces[SpaceId::Data]
            .pending_acks
            .ranges
            .corrupt_cold_shape_for_recovery_probe()
    }
}
