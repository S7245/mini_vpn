//! Owns existing pending/sent control metadata and its loss/retry transfer.
//! The Connection cold root composes resources; packet/stream producers keep
//! their original call chain. The cold owner admits RESET/STOP/MAX_STREAM_DATA nodes and one
//! descriptor per possible sent record before publication/encoding. Remaining
//! variable containers are not yet hard-budget accepted; no second authority.
//! Requeue composes CID append before general metadata; CID slots/budget
//! remain owned by cid_frames, not by this Module.
//! Entry: Retransmits, ThinRetransmits, PacketSpace::requeue.
//! Tests: tests/admission/{metadata,recovery,sent_gso} and existing QUIC suite.
pub(super) mod records;
use records::{CreditKey, Credits, CryptoFrames, PacketRecords, Pool, Queue};

use super::{cid_frames::Frames, spaces::PacketSpace, streams::StreamsState};
use crate::{frame, Dir, StreamId, VarInt};
use std::net::SocketAddr;

impl super::Connection {
    /// One original Connection owns the first error and timed transport close.
    /// A capacity/authority failure must not borrow a later driver's timestamp.
    pub(super) fn fail_control_inventory(&mut self, now: crate::Instant, reason: &'static str) {
        if self.state.is_closed() {
            return;
        }
        self.record_first_error(crate::TransportError::INTERNAL_ERROR(reason).into());
        self.close_inner(
            now,
            frame::Close::Connection(frame::ConnectionClose {
                error_code: crate::TransportErrorCode::INTERNAL_ERROR,
                frame_type: None,
                reason: bytes::Bytes::from_static(reason.as_bytes()),
            }),
        );
    }
    pub(super) fn requeue_control_records(
        &mut self,
        now: crate::Instant,
        space: crate::packet::SpaceId,
        frames: ThinRetransmits,
    ) -> Result<(), crate::TransportError> {
        if let Err((error, frames)) = self.spaces[space].requeue(frames) {
            self.record_first_error(error.clone().into());
            self.fail_control_inventory(now, "control inventory requeue failed");
            // Original Root has now committed failstop. Dispose known rejected
            // metadata here, not implicitly before first-error/close custody.
            drop(frames);
            return Err(error);
        }
        Ok(())
    }
}

/// Retransmittable data queue
#[allow(unreachable_pub)] // fuzzing only
#[derive(Debug, Default)]
pub struct Retransmits {
    pub(super) max_data: bool,
    pub(super) max_stream_id: [bool; 2],
    pub(super) reset_stream: Queue<(StreamId, VarInt)>,
    pub(super) stop_sending: Queue<frame::StopSending>,
    pub(super) max_stream_data: Credits,
    pub(super) crypto: CryptoFrames,
    pub(super) retire_cids: Queue<u64>,
    pub(super) ack_frequency: bool,
    pub(super) handshake_done: bool,
    /// For each enqueued NEW_TOKEN frame, a copy of the path's remote address
    ///
    /// There are 2 reasons this is unusual:
    ///
    /// - If the path changes, NEW_TOKEN frames bound for the old path are not retransmitted on the
    ///   new path. That is why this field stores the remote address: so that ones for old paths
    ///   can be filtered out.
    /// - If a token is lost, a new randomly generated token is re-transmitted, rather than the
    ///   original. This is so that if both transmissions are received, the client won't risk
    ///   sending the same token twice. That is why this field does _not_ store any actual token.
    ///
    /// It is true that a QUIC endpoint will only want to effectively have NEW_TOKEN frames
    /// enqueued for its current path at a given point in time. Based on that, we could conceivably
    /// change this from a vector to an `Option<(SocketAddr, usize)>` or just a `usize` or
    /// something. However, due to the architecture of Quinn, it is considerably simpler to not do
    /// that; consider what such a change would mean for implementing `BitOrAssign` on Self.
    pub(super) new_tokens: Vec<SocketAddr>,
}

impl Retransmits {
    pub(super) fn with_records(pool: Option<&Pool>, key: CreditKey) -> Self {
        Self {
            reset_stream: Queue::new(pool),
            stop_sending: Queue::new(pool),
            max_stream_data: Credits::new(pool, key),
            crypto: CryptoFrames::new(pool),
            retire_cids: Queue::new(pool),
            ..Self::default()
        }
    }
    pub(super) fn empty_like(&self) -> Self {
        Self {
            reset_stream: self.reset_stream.empty_like(),
            stop_sending: self.stop_sending.empty_like(),
            max_stream_data: self.max_stream_data.empty_like(),
            crypto: self.crypto.empty_like(),
            retire_cids: self.retire_cids.empty_like(),
            ..Self::default()
        }
    }

    pub(super) fn is_empty(&self, streams: &StreamsState) -> bool {
        !self.max_data
            && !self.max_stream_id.into_iter().any(|x| x)
            && self.reset_stream.is_empty()
            && self.stop_sending.is_empty()
            && self
                .max_stream_data
                .iter()
                .all(|id| !streams.can_send_flow_control(id))
            && self.crypto.is_empty()
            && self.retire_cids.is_empty()
            && !self.ack_frequency
            && !self.handshake_done
            && self.new_tokens.is_empty()
    }
}

impl ::std::ops::BitOrAssign for Retransmits {
    fn bitor_assign(&mut self, rhs: Self) {
        // We reduce in-stream head-of-line blocking by queueing retransmits before other data for
        // STREAM and CRYPTO frames.
        self.max_data |= rhs.max_data;
        for dir in Dir::iter() {
            self.max_stream_id[dir as usize] |= rhs.max_stream_id[dir as usize];
        }
        self.reset_stream.append(rhs.reset_stream);
        self.stop_sending.append(rhs.stop_sending);
        self.max_stream_data.append(rhs.max_stream_data);
        self.crypto.prepend(rhs.crypto);
        self.retire_cids.append(rhs.retire_cids);
        self.ack_frequency |= rhs.ack_frequency;
        self.handshake_done |= rhs.handshake_done;
        self.new_tokens.extend_from_slice(&rhs.new_tokens);
    }
}

impl ::std::iter::FromIterator<Self> for Retransmits {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = Self>,
    {
        let mut result = Self::default();
        for packet in iter {
            result |= packet;
        }
        result
    }
}

/// A variant of `Retransmits` which only allocates storage when required
#[derive(Debug, Default)]
pub(super) struct ThinRetransmits {
    pub(super) records: PacketRecords,
    pub(super) new_cids: Frames,
}

impl ThinRetransmits {
    pub(super) fn with_records(records: PacketRecords) -> Self {
        Self {
            records,
            ..Self::default()
        }
    }
    pub(super) fn reset_iter(&self) -> impl Iterator<Item = (StreamId, VarInt)> + '_ {
        self.record_queues()
            .into_iter()
            .flat_map(|(reset, _)| reset.iter())
    }
    pub(super) fn record_len(&self) -> usize {
        self.record_queues()
            .map_or(0, |(reset, stop)| reset.len() + stop.len())
    }
    fn record_queues(&self) -> Option<(&Queue<(StreamId, VarInt)>, &Queue<frame::StopSending>)> {
        self.records.queues()
    }
    pub(super) fn push_reset(&mut self, record: records::Entry<(StreamId, VarInt)>) {
        if let Some((reset, _)) = self.records.queues_mut() {
            reset.push(record);
        } else {
            self.get_or_create().reset_stream.push(record);
        }
    }
    pub(super) fn push_stop(&mut self, record: records::Entry<frame::StopSending>) {
        if let Some((_, stop)) = self.records.queues_mut() {
            stop.push(record);
        } else {
            self.get_or_create().stop_sending.push(record);
        }
    }

    /// Returns `true` if no retransmits are necessary
    pub(super) fn is_empty(&self, streams: &StreamsState) -> bool {
        self.new_cids.is_empty()
            && self.records.len() == 0
            && self
                .get()
                .is_none_or(|retransmits| retransmits.is_empty(streams))
    }

    /// Returns a reference to the retransmits stored in this box
    pub(super) fn get(&self) -> Option<&Retransmits> {
        self.records.get()
    }

    /// Returns a mutable reference to the stored retransmits
    ///
    /// This function will allocate a backing storage if required.
    pub(super) fn get_or_create(&mut self) -> &mut Retransmits {
        self.records.get_or_create()
    }
}

impl PacketSpace {
    pub(super) fn admit_requeue(&self, frames: &ThinRetransmits) -> Result<(), crate::TransportError> {
        if !self.pending_cids.validate_append(&frames.new_cids) {
            return Err(crate::TransportError::INTERNAL_ERROR("foreign CID custody"));
        }
        if let Some((reset, stop)) = frames.record_queues() {
            if !self.pending.reset_stream.accepts(reset)
                || !self.pending.stop_sending.accepts(stop)
                || !self.pending.reset_stream.authority_ready()
                || !self.pending.stop_sending.authority_ready()
                || !reset.authority_ready()
                || !stop.authority_ready()
            {
                self.pending.reset_stream.quarantine();
                self.pending.stop_sending.quarantine();
                reset.quarantine();
                stop.quarantine();
                return Err(crate::TransportError::INTERNAL_ERROR(
                    "foreign control record custody",
                ));
            }
        }
        if let Some(metadata) = frames.get() {
            if !self.pending.retire_cids.accepts(&metadata.retire_cids)
                || !self.pending.retire_cids.authority_ready()
                || !metadata.retire_cids.authority_ready()
            {
                self.pending.retire_cids.quarantine();
                metadata.retire_cids.quarantine();
                return Err(crate::TransportError::INTERNAL_ERROR(
                    "foreign retirement custody",
                ));
            }
            if !self.pending.crypto.accepts(&metadata.crypto) {
                self.pending.crypto.quarantine();
                metadata.crypto.quarantine();
                return Err(crate::TransportError::INTERNAL_ERROR(
                    "foreign CRYPTO custody",
                ));
            }
            if !self
                .pending
                .max_stream_data
                .accepts(&metadata.max_stream_data)
            {
                self.pending.max_stream_data.quarantine();
                metadata.max_stream_data.quarantine();
                return Err(crate::TransportError::INTERNAL_ERROR(
                    "foreign stream credit custody",
                ));
            }
        }
        if !self.pending.reset_stream.authority_ready() {
            return Err(crate::TransportError::INTERNAL_ERROR(
                "control inventory authority lost",
            ));
        }
        Ok(())
    }
    pub(super) fn requeue(
        &mut self,
        mut frames: ThinRetransmits,
    ) -> Result<(), (crate::TransportError, ThinRetransmits)> {
        if let Err(error) = self.admit_requeue(&frames) {
            return Err((error, frames));
        }
        // Every fallible check precedes this settlement. A concurrent Q after
        // admission does not abandon half of the already-owned transfer.
        self.pending_cids.append(&mut frames.new_cids);
        if let Some(other) = frames.records.take_metadata() {
            self.pending |= other;
        }
        Ok(())
    }
}
