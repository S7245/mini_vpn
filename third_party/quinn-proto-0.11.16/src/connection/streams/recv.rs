use std::collections::hash_map::Entry;
use std::mem;

use thiserror::Error;
use tracing::debug;

use super::state::get_or_insert_recv;
use super::{ClosedStream, ShouldTransmit, StreamId, StreamOwner};
use crate::connection::assembler::{Assembler, Chunk, InsertError};
use crate::connection::control_inventory::records::Entry as CreditEntry;
use crate::connection::streams::state::StreamRecv;
use crate::{frame, TransportError, VarInt};

#[derive(Debug, Default)]
pub(super) struct Recv {
    // NB: when adding or removing fields, remember to update `reinit`.
    state: RecvState,
    pub(super) assembler: Assembler,
    sent_max_stream_data: u64,
    pub(super) end: u64,
    pub(super) stopped: bool,
}

impl Recv {
    pub(super) fn new(initial_max_data: u64) -> Box<Self> {
        Box::new(Self::initial(initial_max_data))
    }

    pub(super) fn initial(initial_max_data: u64) -> Self {
        Self {
            state: RecvState::default(),
            assembler: Assembler::new(),
            sent_max_stream_data: initial_max_data,
            end: 0,
            stopped: false,
        }
    }

    /// Prepare the original proto instance without publishing a stream. The
    /// instance remains in the existing proto/count partition; its Assembler
    /// backing and operation queues are separately admitted by the packet root.
    pub(super) fn try_new(initial_max_data: u64) -> Result<Box<Self>, bytes::MeteredCopyError> {
        let pointer = unsafe { std::alloc::alloc(std::alloc::Layout::new::<Self>()).cast::<Self>() };
        if pointer.is_null() { return Err(bytes::MeteredCopyError::Allocation); }
        // The non-zero layout and initialization exactly match the original Box.
        unsafe {
            pointer.write(Self::initial(initial_max_data));
            Ok(Box::from_raw(pointer))
        }
    }

    /// Reset to the initial state
    pub(super) fn reinit(&mut self, initial_max_data: u64) -> Result<(), TransportError> {
        self.check_inventory(None)?;
        self.assembler.reinit_checked().map_err(|_| Self::inventory_error())?;
        self.state = RecvState::default();
        self.sent_max_stream_data = initial_max_data;
        self.end = 0;
        self.stopped = false;
        Ok(())
    }
    pub(super) fn check_inventory(&self, expected: Option<&bytes::BackingMeter>) -> Result<(), TransportError> {
        self.assembler.check_inventory_with_end(self.end, expected).map_err(|_| Self::inventory_error())
    }
    fn inventory_error() -> TransportError {
        TransportError::INTERNAL_ERROR("receive inventory mismatch before settlement")
    }

    /// Process a STREAM frame
    ///
    /// Return value is `(number_of_new_bytes_ingested, stream_is_closed)`
    #[cfg(test)]
    pub(super) fn ingest(
        &mut self,
        frame: frame::Stream,
        payload_len: usize,
        received: u64,
        max_data: u64,
    ) -> Result<(u64, bool), TransportError> {
        self.ingest_checked(frame, payload_len, received, max_data, None)
    }

    pub(super) fn validate_ingest(&self, offset: u64, len: usize, fin: bool, received: u64, max_data: u64, expected: Option<&bytes::BackingMeter>) -> Result<(u64, u64), TransportError> {
        let (end, new_bytes) = self.packet_progress().validate(offset, len, fin, received, max_data)?;
        if self.stopped && fin { self.check_inventory(expected).inspect_err(|_| {
            if let Some(meter) = expected { meter.quarantine(); }
        })?; }
        Ok((end, new_bytes))

    }

    pub(super) fn packet_progress(&self) -> ReceiveProgress {
        ReceiveProgress { end: self.end, final_offset: self.final_offset(), max_stream: self.sent_max_stream_data, reset: !self.is_receiving(), stopped: self.stopped, bytes_read: self.assembler.bytes_read(), closed: false }
    }

    pub(super) fn ingest_checked(
        &mut self,
        frame: frame::Stream,
        payload_len: usize,
        received: u64,
        max_data: u64,
        expected_meter: Option<&bytes::BackingMeter>,
    ) -> Result<(u64, bool), TransportError> {
        let (end, new_bytes) = self.validate_ingest(frame.offset, frame.data.len(), frame.fin, received, max_data, expected_meter)?;

        // Don't bother storing data or releasing stream-level flow control credit if the stream's
        // already stopped
        if !self.stopped {
            self.assembler
                .insert_checked(frame.offset, frame.data, payload_len, expected_meter)
                .map_err(|error| match error {
                    InsertError::TooManyChunks => {
                        TransportError::INTERNAL_ERROR("too many gaps in stream buffer")
                    }
                    InsertError::Capacity(error) => {
                        tracing::trace!(?error, "receive inventory admission refused");
                        TransportError::INTERNAL_ERROR("receive inventory admission refused")
                    }
                    InsertError::AuthorityMismatch => {
                        TransportError::INTERNAL_ERROR("receive backing authority mismatch")
                    }
                })?;
        }

        self.commit_received_metadata(end, frame.fin);

        Ok((new_bytes, frame.fin && self.stopped))
    }

    pub(super) fn commit_received_metadata(&mut self, end: u64, fin: bool) {
        if fin && !self.stopped {
            if let RecvState::Recv { ref mut size } = self.state {
                *size = Some(end);
            }
        }
        self.end = self.end.max(end);
    }

    pub(super) fn stop(&mut self) -> Result<(u64, ShouldTransmit), ClosedStream> {
        if self.stopped {
            return Err(ClosedStream { _private: () });
        }

        self.check_inventory(None).map_err(|_| ClosedStream { _private: () })?;
        // Qualify the exact credit before disposing any actual backing.
        let read_credits = self.end.checked_sub(self.assembler.bytes_read())
            .ok_or(ClosedStream { _private: () })?;
        self.assembler.clear_checked().map_err(|_| ClosedStream { _private: () })?;
        self.stopped = true;
        // This may send a spurious STOP_SENDING if we've already received all data, but it's a bit
        // fiddly to distinguish that from the case where we've received a FIN but are missing some
        // data that the peer might still be trying to retransmit, in which case a STOP_SENDING is
        // still useful.
        Ok((read_credits, ShouldTransmit(self.is_receiving())))
    }

    /// Returns the window that should be advertised in a `MAX_STREAM_DATA` frame
    ///
    /// The method returns a tuple which consists of the window that should be
    /// announced, as well as a boolean parameter which indicates if a new
    /// transmission of the value is recommended. If the boolean value is
    /// `false` the new window should only be transmitted if a previous transmission
    /// had failed.
    pub(super) fn max_stream_data(&mut self, stream_receive_window: u64) -> (u64, ShouldTransmit) {
        let max_stream_data = self.assembler.bytes_read() + stream_receive_window;

        // Only announce a window update if it's significant enough
        // to make it worthwhile sending a MAX_STREAM_DATA frame.
        // We use here a fraction of the configured stream receive window to make
        // the decision, and accommodate for streams using bigger windows requiring
        // less updates. A fixed size would also work - but it would need to be
        // smaller than `stream_receive_window` in order to make sure the stream
        // does not get stuck.
        let diff = max_stream_data - self.sent_max_stream_data;
        let transmit = self.can_send_flow_control() && diff >= (stream_receive_window / 8);
        (max_stream_data, ShouldTransmit(transmit))
    }

    /// Records that a `MAX_STREAM_DATA` announcing a certain window was sent
    ///
    /// This will suppress enqueuing further `MAX_STREAM_DATA` frames unless
    /// either the previous transmission was not acknowledged or the window
    /// further increased.
    pub(super) fn record_sent_max_stream_data(&mut self, sent_value: u64) {
        if sent_value > self.sent_max_stream_data {
            self.sent_max_stream_data = sent_value;
        }
    }

    /// Whether the total amount of data that the peer will send on this stream is unknown
    ///
    /// True until we've received either a reset or the final frame.
    ///
    /// Implies that the sender might benefit from stream-level flow control updates, and we might
    /// need to issue connection-level flow control updates due to flow control budget use by this
    /// stream in the future, even if it's been stopped.
    pub(super) fn final_offset_unknown(&self) -> bool {
        matches!(self.state, RecvState::Recv { size: None })
    }

    /// Whether stream-level flow control updates should be sent for this stream
    pub(super) fn can_send_flow_control(&self) -> bool {
        // Stream-level flow control is redundant if the sender has already sent the whole stream,
        // and moot if we no longer want data on this stream.
        self.final_offset_unknown() && !self.stopped
    }

    /// Whether data is still being accepted from the peer
    pub(super) fn is_receiving(&self) -> bool {
        matches!(self.state, RecvState::Recv { .. })
    }

    fn final_offset(&self) -> Option<u64> {
        match self.state {
            RecvState::Recv { size } => size,
            RecvState::ResetRecvd { size, .. } => Some(size),
        }
    }

    pub(super) fn validate_reset(&self, final_offset: VarInt, received: u64, max_data: u64, expected: Option<&bytes::BackingMeter>) -> Result<bool, TransportError> {
        if !self.packet_progress().validate_reset(final_offset, received, max_data)? {
            return Ok(false);
        }
        self.check_inventory(expected).inspect_err(|_| {
            if let Some(meter) = expected { meter.quarantine(); }
        })?;
        Ok(true)
    }

    /// Returns `false` iff the reset was redundant
    pub(super) fn reset(
        &mut self,
        error_code: VarInt,
        final_offset: VarInt,
        received: u64,
        max_data: u64,
        expected: Option<&bytes::BackingMeter>,
    ) -> Result<bool, TransportError> {
        if !self.validate_reset(final_offset, received, max_data, expected)? { return Ok(false); }
        self.assembler.clear_checked().map_err(|_| Self::inventory_error())?;
        self.state = RecvState::ResetRecvd {
            size: final_offset.into(),
            error_code,
        };
        // Nuke buffers so that future reads fail immediately, which ensures future reads don't
        // issue flow control credit redundant to that already issued. We could instead special-case
        // reset streams during read, but it's unclear if there's any benefit to retaining data for
        // reset streams.
        Ok(true)
    }

    pub(super) fn reset_code(&self) -> Option<VarInt> {
        match self.state {
            RecvState::ResetRecvd { error_code, .. } => Some(error_code),
            _ => None,
        }
    }


}

/// Value-only protocol progression; never owns a receive inventory.
pub(super) struct ReceiveProgress {
    end: u64,
    final_offset: Option<u64>,
    max_stream: u64,
    reset: bool,
    pub(super) stopped: bool,
    bytes_read: u64,
    pub(super) closed: bool,
}
impl ReceiveProgress {
    pub(super) fn receiving(&self) -> bool { !self.closed && !self.reset }
    fn validate_reset(&mut self, final_offset: VarInt, received: u64, max_data: u64) -> Result<bool, TransportError> {
        let offset = final_offset.into_inner();
        if self.final_offset.is_some_and(|size| size != offset) {
            return Err(TransportError::FINAL_SIZE_ERROR("inconsistent value"));
        }
        if self.final_offset.is_none() && self.end > offset {
            return Err(TransportError::FINAL_SIZE_ERROR("lower than high water mark"));
        }
        if offset > self.max_stream || received.checked_add(offset.saturating_sub(self.end)).is_none_or(|n| n > max_data) {
            return Err(TransportError::FLOW_CONTROL_ERROR(""));
        }
        if self.reset { return Ok(false); }
        self.reset = true;
        self.final_offset = Some(offset);
        Ok(true)
    }
    /// Original receive metadata and credit effects, without any physical owner.
    pub(super) fn apply(&mut self, frame: &frame::Frame, received: u64, max_data: u64)
        -> Result<(u64, u64, bool, bool), TransportError>
    {
        if self.closed { return Ok((0, 0, false, false)); }
        match frame {
            frame::Frame::Stream(s) if !self.reset => {
                let (_, credit) = self.validate(s.offset, s.data.len(), s.fin, received, max_data)?;
                if self.stopped && s.fin { self.closed = true; }
                Ok((credit, if self.stopped { credit } else { 0 }, !self.stopped, !self.stopped))
            }
            frame::Frame::ResetStream(s) => {
                if !self.validate_reset(s.final_offset, received, max_data)? { return Ok((0, 0, false, false)); }
                self.closed = self.stopped;
                let final_offset = s.final_offset.into_inner();
                let (credit, refund) = if self.bytes_read == final_offset { (0, 0) }
                    else { (final_offset - self.end, final_offset - self.bytes_read) };
                Ok((credit, refund, !self.stopped, true))
            }
            _ => Ok((0, 0, false, false)),
        }
    }
    pub(super) fn validate(&mut self, offset: u64, len: usize, fin: bool, received: u64, max_data: u64)
        -> Result<(u64, u64), TransportError>
    {
        let end = offset.checked_add(len as u64).filter(|end| *end < 1 << 62)
            .ok_or_else(|| TransportError::FLOW_CONTROL_ERROR("maximum stream offset too large"))?;
        if (fin && end < self.end) || self.final_offset.is_some_and(|size| end > size || (fin && end != size)) {
            return Err(TransportError::FINAL_SIZE_ERROR(""));
        }
        let credit = end.saturating_sub(self.end);
        if end > self.max_stream || received.checked_add(credit).is_none_or(|n| n > max_data) {
            return Err(TransportError::FLOW_CONTROL_ERROR(""));
        }
        self.end = self.end.max(end);
        if fin && !self.stopped { self.final_offset = Some(end); }
        Ok((end, credit))
    }
}

/// Chunks returned from [`RecvStream::read()`][crate::RecvStream::read].
///
/// ### Note: Finalization Needed
/// Bytes read from the stream are not released from the congestion window until
/// either [`Self::finalize()`] is called, or this type is dropped.
///
/// It is recommended that you call [`Self::finalize()`] because it returns a flag
/// telling you whether reading from the stream has resulted in the need to transmit a packet.
///
/// If this type is leaked, the stream will remain blocked on the remote peer until
/// another read from the stream is done.
pub struct Chunks<'a> {
    id: StreamId,
    ordered: bool,
    owner: StreamOwner<'a>,
    state: ChunksState,
    read: u64,
    credit: Option<CreditEntry<StreamId>>,
}

impl<'a> Chunks<'a> {
    pub(super) fn new(
        id: StreamId,
        ordered: bool,
        mut owner: StreamOwner<'a>,
    ) -> Result<Self, ReadableError> {
        if !owner.admit() {
            return Err(ReadableError::BackingAuthorityLost);
        }
        let prepared = (|| {
            let (streams, pending, _conn_state) = owner.parts();
            let expected = streams.native_meter().cloned();
            let mut entry = match streams.recv.entry(id) {
                Entry::Occupied(entry) => entry,
                Entry::Vacant(_) => return Err(ReadableError::ClosedStream),
            };

            // Prepare inside the original map owner. Ordering/defrag may reject;
            // removing first would drop the entire receive inventory on that Err.
            let recv = get_or_insert_recv(streams.stream_receive_window)(entry.get_mut());
            if recv.stopped {
                return Err(ReadableError::ClosedStream);
            }
            recv.assembler.check_ordering(ordered)?;
            recv.assembler.check_authority(expected.as_ref()).map_err(|_| ReadableError::BackingAuthorityLost)?;
            // Preserve an already-owed advertisement, including the original
            // window/8 == 0 case and a caller that only drops Chunks. Reserve
            // before ordering or removal; a later preparation Err cancels it.
            let credit = if recv.max_stream_data(streams.stream_receive_window).1 .0 {
                pending
                    .max_stream_data
                    .prepare(id)
                    .map_err(|()| ReadableError::BackingAuthorityLost)?
            } else {
                None
            };
            recv.assembler.ensure_ordering(ordered)?;
            // Preparation materialized this occupied entry. Transfer only after all
            // fallible work; Chunks now owns its return/finalize transaction.
            Ok((entry.remove().unwrap().into_inner(), credit))
        })();
        let (recv, credit) = match prepared {
            Ok(recv) => recv,
            Err(error) => {
                if matches!(error, ReadableError::BackingAuthorityLost) {
                    let unknown = owner.streams().recv.get(&id).and_then(Option::as_ref)
                        .is_some_and(|recv| match recv {
                            StreamRecv::Open(recv) | StreamRecv::Free(recv) => recv.check_inventory(owner.streams().native_meter()).is_err(),
                        });
                    if unknown { owner.fail_receive_inventory("receive inventory mismatch during read preparation"); }
                    else { owner.fail("receive backing authority lost during read preparation"); }
                }
                return Err(error);
            }
        };
        Ok(Self {
            id,
            ordered,
            owner,
            state: ChunksState::Readable(recv),
            read: 0,
            credit,
        })
    }

    /// Read only a chunk beginning at `expected_offset`, without consuming a
    /// different chunk. A mismatch is `Blocked` and releases no read credit.
    /// Ordered reads retain the original prefix trimming rule; unordered reads
    /// gate the actual lowest unread chunk. Call `finalize()` as for `next()`.
    /// Zero length retains `next()` semantics and consumes no bytes.
    pub fn next_at(
        &mut self,
        expected_offset: u64,
        max_length: usize,
    ) -> Result<Option<Chunk>, ReadError> {
        if let ChunksState::Readable(ref rs) = self.state {
            // Offset mismatch must not consume/trim anything, but it cannot
            // hide lost original inventory as ordinary read pressure.
            if rs.assembler.qualify_read(self.owner.streams().native_meter()).is_err() {
                self.owner.fail_receive_inventory("receive descriptor inventory lost before offset read");
                return Err(ReadError::BackingAuthorityLost);
            }
            if let Some(offset) = rs.assembler.progress().next_received_offset {
                let next = if self.ordered {
                    rs.assembler.bytes_read()
                } else {
                    offset
                };
                if next != expected_offset {
                    return Err(if self.owner.admit() {
                        ReadError::Blocked
                    } else {
                        ReadError::BackingAuthorityLost
                    });
                }
            }
        }
        // No buffered data is not EOF: the original transaction distinguishes
        // Blocked, RESET and FIN, and owns all credit/finalize bookkeeping.
        self.next(max_length)
    }

    /// Next
    ///
    /// Should call finalize() when done calling this.
    pub fn next(&mut self, max_length: usize) -> Result<Option<Chunk>, ReadError> {
        if !self.owner.admit() {
            return Err(ReadError::BackingAuthorityLost);
        }
        let (streams, pending, _conn_state) = self.owner.parts();
        let rs = match self.state {
            ChunksState::Readable(ref mut rs) => rs,
            ChunksState::Reset(error_code) => {
                return Err(ReadError::Reset(error_code));
            }
            ChunksState::Finished => {
                return Ok(None);
            }
            ChunksState::Finalized => panic!("must not call next() after finalize()"),
        };

        if rs.assembler.check_authority(streams.native_meter()).is_err() {
            self.owner.fail_receive_inventory("receive descriptor inventory authority lost before read");
            return Err(ReadError::BackingAuthorityLost);
        }
        let positive = match rs.assembler.prepare_read(max_length, self.ordered) {
            Ok(length) => length.unwrap_or(0),
            Err(_) => {
                self.owner.fail_receive_inventory("receive descriptor inventory lost before read");
                return Err(ReadError::BackingAuthorityLost);
            }
        };
        if positive != 0
            && self.credit.is_none()
            && rs.can_send_flow_control()
            && rs.assembler.bytes_read() + positive as u64 + streams.stream_receive_window
                - rs.sent_max_stream_data
                >= streams.stream_receive_window / 8
        {
            match pending.max_stream_data.prepare(self.id) {
                Ok(credit) => self.credit = credit,
                Err(()) => {
                    self.owner
                        .fail("MAX_STREAM_DATA capacity unavailable before read");
                    return Err(ReadError::BackingAuthorityLost);
                }
            }
        }
        if let Some(credit) = &mut self.credit {
            credit.begin_mutation();
        }
        match rs.assembler.read_checked(max_length, self.ordered) {
            Ok(Some(chunk)) => {
                self.read += chunk.bytes.len() as u64;
                return Ok(Some(chunk));
            }
            Ok(None) => {},
            Err(_) => {
                self.owner.fail_receive_inventory("receive descriptor inventory lost during read");
                return Err(ReadError::BackingAuthorityLost);
            }
        }

        match rs.state {
            RecvState::ResetRecvd { error_code, .. } => {
                if rs.check_inventory(streams.native_meter()).is_err() {
                    self.owner.fail_receive_inventory("receive inventory mismatch before reset retirement");
                    return Err(ReadError::BackingAuthorityLost);
                }
                debug_assert_eq!(self.read, 0, "reset streams have empty buffers");
                let state = mem::replace(&mut self.state, ChunksState::Reset(error_code));
                // At this point if we have `rs` self.state must be `ChunksState::Readable`
                let recv = match state {
                    ChunksState::Readable(recv) => StreamRecv::Open(recv),
                    _ => unreachable!("state must be ChunkState::Readable"),
                };
                if streams.stream_recv_freed(self.id, recv).is_err() {
                    self.owner.fail_receive_inventory("receive inventory mismatch during reset retirement");
                    return Err(ReadError::BackingAuthorityLost);
                }
                Err(ReadError::Reset(error_code))
            }
            RecvState::Recv { size } => {
                if size == Some(rs.end) && rs.assembler.bytes_read() == rs.end {
                    if rs.check_inventory(streams.native_meter()).is_err() {
                        self.owner.fail_receive_inventory("receive inventory mismatch before FIN retirement");
                        return Err(ReadError::BackingAuthorityLost);
                    }
                    let state = mem::replace(&mut self.state, ChunksState::Finished);
                    // At this point if we have `rs` self.state must be `ChunksState::Readable`
                    let recv = match state {
                        ChunksState::Readable(recv) => StreamRecv::Open(recv),
                        _ => unreachable!("state must be ChunkState::Readable"),
                    };
                    if streams.stream_recv_freed(self.id, recv).is_err() {
                        self.owner.fail_receive_inventory("receive inventory mismatch during FIN retirement");
                        return Err(ReadError::BackingAuthorityLost);
                    }
                    Ok(None)
                } else {
                    // We don't need a distinct `ChunksState` variant for a blocked stream because
                    // retrying a read harmlessly re-traces our steps back to returning
                    // `Err(Blocked)` again. The buffers can't refill and the stream's own state
                    // can't change so long as this `Chunks` exists.
                    Err(ReadError::Blocked)
                }
            }
        }
    }

    /// Mark the read data as consumed from the stream.
    ///
    /// The number of read bytes will be released from the congestion window,
    /// allowing the remote peer to send more data if it was previously blocked.
    ///
    /// If [`ShouldTransmit::should_transmit()`] returns `true`,
    /// a packet needs to be sent to the peer informing them that the stream is unblocked.
    /// This means that you should call [`Connection::poll_transmit()`][crate::Connection::poll_transmit]
    /// and send the returned packet as soon as is reasonable, to unblock the remote peer.
    pub fn finalize(mut self) -> ShouldTransmit {
        self.finalize_inner()
    }

    fn finalize_inner(&mut self) -> ShouldTransmit {
        let (streams, pending, _conn_state) = self.owner.parts();
        let state = mem::replace(&mut self.state, ChunksState::Finalized);
        if let ChunksState::Finalized = state {
            // Noop on repeated calls
            return ShouldTransmit(false);
        }

        // We issue additional stream ID credit after the application is notified that a previously
        // open stream has finished or been reset and we've therefore disposed of its state, as
        // recorded by `stream_freed` calls in `next`.
        let mut should_transmit = streams.queue_max_stream_id(pending);

        // If the stream hasn't finished, we may need to issue stream-level flow control credit
        if let ChunksState::Readable(mut rs) = state {
            let (_, max_stream_data) = rs.max_stream_data(streams.stream_receive_window);
            should_transmit |= max_stream_data.0;
            if max_stream_data.0 {
                if let Some(credit) = self.credit.take() {
                    // Known read credit settles even if the shared authority was
                    // quarantined later. No new allocation or reservation here.
                    if pending.max_stream_data.insert(credit).is_err() {
                        pending.max_stream_data.quarantine();
                    }
                }
            }
            // Return the stream to storage for future use
            streams.recv.insert(self.id, Some(StreamRecv::Open(rs)));
        }

        // Issue connection-level flow control credit for any data we read regardless of state
        let max_data = streams.add_read_credits(self.read);
        pending.max_data |= max_data.0;
        should_transmit |= max_data.0;
        if let Some(credit) = self.credit.take() {
            credit.discard_proved();
        }
        if !pending.reset_stream.authority_ready() {
            self.owner.fail("MAX_STREAM_DATA settlement authority lost");
        }
        ShouldTransmit(should_transmit)
    }
}

impl Drop for Chunks<'_> {
    fn drop(&mut self) {
        let _ = self.finalize_inner();
    }
}

enum ChunksState {
    Readable(Box<Recv>),
    Reset(VarInt),
    Finished,
    Finalized,
}

/// Errors triggered when reading from a recv stream
#[derive(Debug, Error, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ReadError {
    /// The original Connection rejected consumption after backing quarantine.
    #[error("receive backing authority lost")]
    BackingAuthorityLost,
    /// No more data is currently available on this stream.
    ///
    /// If more data on this stream is received from the peer, an `Event::StreamReadable` will be
    /// generated for this stream, indicating that retrying the read might succeed.
    #[error("blocked")]
    Blocked,
    /// The peer abandoned transmitting data on this stream.
    ///
    /// Carries an application-defined error code.
    #[error("reset by peer: code {0}")]
    Reset(VarInt),
}

/// Errors triggered when opening a recv stream for reading
#[derive(Debug, Error, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ReadableError {
    /// The stream has not been opened or was already stopped, finished, or reset
    #[error("closed stream")]
    ClosedStream,
    /// Attempted an ordered read following an unordered read
    ///
    /// Performing an unordered read allows discontinuities to arise in the receive buffer of a
    /// stream which cannot be recovered, making further ordered reads impossible.
    #[error("ordered read after unordered read")]
    IllegalOrderedRead,
    /// A receive backing lost its Node authority during buffered read setup.
    #[error("receive backing authority lost")]
    BackingAuthorityLost,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum RecvState {
    Recv { size: Option<u64> },
    ResetRecvd { size: u64, error_code: VarInt },
}

impl Default for RecvState {
    fn default() -> Self {
        Self::Recv { size: None }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use crate::{Dir, Side};

    use super::*;

    #[test]
    fn reordered_frames_while_stopped() {
        const INITIAL_BYTES: u64 = 3;
        const INITIAL_OFFSET: u64 = 3;
        const RECV_WINDOW: u64 = 8;
        let mut s = Recv::new(RECV_WINDOW);
        let mut data_recvd = 0;
        // Receive bytes 3..6
        let (new_bytes, is_closed) = s
            .ingest(
                frame::Stream {
                    id: StreamId::new(Side::Client, Dir::Uni, 0),
                    offset: INITIAL_OFFSET,
                    fin: false,
                    data: Bytes::from_static(&[0; INITIAL_BYTES as usize]),
                },
                123,
                data_recvd,
                data_recvd + 1024,
            )
            .unwrap();
        data_recvd += new_bytes;
        assert_eq!(new_bytes, INITIAL_OFFSET + INITIAL_BYTES);
        assert!(!is_closed);

        let (credits, transmit) = s.stop().unwrap();
        assert!(transmit.should_transmit());
        assert_eq!(
            credits,
            INITIAL_OFFSET + INITIAL_BYTES,
            "full connection flow control credit is issued by stop"
        );

        let (max_stream_data, transmit) = s.max_stream_data(RECV_WINDOW);
        assert!(!transmit.should_transmit());
        assert_eq!(
            max_stream_data, RECV_WINDOW,
            "stream flow control credit isn't issued by stop"
        );

        // Receive byte 7
        let (new_bytes, is_closed) = s
            .ingest(
                frame::Stream {
                    id: StreamId::new(Side::Client, Dir::Uni, 0),
                    offset: RECV_WINDOW - 1,
                    fin: false,
                    data: Bytes::from_static(&[0; 1]),
                },
                123,
                data_recvd,
                data_recvd + 1024,
            )
            .unwrap();
        data_recvd += new_bytes;
        assert_eq!(new_bytes, RECV_WINDOW - (INITIAL_OFFSET + INITIAL_BYTES));
        assert!(!is_closed);

        let (max_stream_data, transmit) = s.max_stream_data(RECV_WINDOW);
        assert!(!transmit.should_transmit());
        assert_eq!(
            max_stream_data, RECV_WINDOW,
            "stream flow control credit isn't issued after stop"
        );

        // Receive bytes 0..3
        let (new_bytes, is_closed) = s
            .ingest(
                frame::Stream {
                    id: StreamId::new(Side::Client, Dir::Uni, 0),
                    offset: 0,
                    fin: false,
                    data: Bytes::from_static(&[0; INITIAL_OFFSET as usize]),
                },
                123,
                data_recvd,
                data_recvd + 1024,
            )
            .unwrap();
        assert_eq!(
            new_bytes, 0,
            "reordered frames don't issue connection-level flow control for stopped streams"
        );
        assert!(!is_closed);

        let (max_stream_data, transmit) = s.max_stream_data(RECV_WINDOW);
        assert!(!transmit.should_transmit());
        assert_eq!(
            max_stream_data, RECV_WINDOW,
            "stream flow control credit isn't issued after stop"
        );
    }
}

#[cfg(test)]
#[path = "recv/packet_progress_tests.rs"]
mod packet_progress_tests;
