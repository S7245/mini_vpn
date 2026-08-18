//! Pure TCP ownership windows for resumable sessions.
//!
//! These state machines deliberately know nothing about transports, sockets,
//! Tokio, or scheduling.  The sender retains exactly the application-owned
//! replay interval `[peer_acked, next_sent)`.  The receiver advances its
//! application ACK only when its caller reports bytes accepted by the sink;
//! receiving a DATA record alone never transfers ownership.
//!
//! `bytes::Bytes` cannot reveal the capacity of its backing allocation. A
//! small slice may therefore keep an arbitrarily large allocation alive. At
//! each ownership boundary this module copies newly-owned bytes into a
//! length-bounded allocation and records that backing length. Later partial
//! ACK/accept operations normally keep a zero-copy suffix, but compact it when
//! the retained suffix is at most one quarter of the recorded backing. The
//! comparison never forgets the original backing after repeated small
//! advances. Consequently every persistent ownership window retains strictly
//! less than four backing bytes per live payload byte, while geometric
//! compaction avoids quadratic copying. Replay and contiguous peek views
//! clone/slice these normalized `Bytes` values and are zero-copy.

use std::collections::{BTreeMap, VecDeque};
use std::fmt;

use bytes::Bytes;
use thiserror::Error;

use super::protocol::{ByteOffset, MAX_DATA_PAYLOAD_BYTES};

pub(super) const RETAINED_SLICE_COMPACT_RATIO: usize = 4;

#[derive(Clone)]
struct OwnedPayload {
    bytes: Bytes,
    backing_bytes: usize,
}

impl fmt::Debug for OwnedPayload {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OwnedPayload")
            .field("payload_len", &self.bytes.len())
            .field("backing_bytes", &self.backing_bytes)
            .finish()
    }
}

impl OwnedPayload {
    fn new(payload: &Bytes) -> Self {
        Self {
            bytes: Bytes::copy_from_slice(payload),
            backing_bytes: payload.len(),
        }
    }

    fn len(&self) -> usize {
        self.bytes.len()
    }

    fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    fn slice(&self, range: std::ops::Range<usize>) -> Self {
        Self {
            bytes: self.bytes.slice(range),
            backing_bytes: self.backing_bytes,
        }
    }

    fn retained_suffix(&self, start: usize) -> (Self, usize) {
        let suffix = self.bytes.slice(start..);
        if suffix.len() <= self.backing_bytes / RETAINED_SLICE_COMPACT_RATIO {
            let compacted = Self::new(&suffix);
            let copied = compacted.len();
            (compacted, copied)
        } else {
            (
                Self {
                    bytes: suffix,
                    backing_bytes: self.backing_bytes,
                },
                0,
            )
        }
    }

    fn backing_is_bounded(&self) -> bool {
        self.backing_bytes / RETAINED_SLICE_COMPACT_RATIO < self.len()
    }
}

/// Per-flow bound used by one directional TCP ownership window.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TcpWindowLimits {
    max_bytes: usize,
    max_segments: usize,
}

impl TcpWindowLimits {
    pub fn new(max_bytes: usize, max_segments: usize) -> Result<Self, TcpOwnershipError> {
        if max_bytes == 0 {
            return Err(TcpOwnershipError::ZeroByteCapacity);
        }
        if max_segments == 0 {
            return Err(TcpOwnershipError::ZeroSegmentCapacity);
        }
        Ok(Self {
            max_bytes,
            max_segments,
        })
    }

    pub const fn max_bytes(self) -> usize {
        self.max_bytes
    }

    /// Maximum retained sender segments or buffered receiver ranges.
    pub const fn max_segments(self) -> usize {
        self.max_segments
    }
}

/// One offset-addressed DATA extent.
#[derive(Clone)]
pub struct TcpDataSegment {
    offset: ByteOffset,
    end_offset: ByteOffset,
    payload: OwnedPayload,
}

impl PartialEq for TcpDataSegment {
    fn eq(&self, other: &Self) -> bool {
        self.offset == other.offset
            && self.end_offset == other.end_offset
            && self.payload.bytes == other.payload.bytes
    }
}

impl Eq for TcpDataSegment {}

impl TcpDataSegment {
    pub const fn offset(&self) -> ByteOffset {
        self.offset
    }

    pub fn payload(&self) -> &Bytes {
        &self.payload.bytes
    }

    pub fn len(&self) -> usize {
        self.payload.len()
    }

    pub fn is_empty(&self) -> bool {
        self.payload.is_empty()
    }

    pub const fn end_offset(&self) -> ByteOffset {
        self.end_offset
    }

    /// Logical length of the allocation retained by this payload view.
    pub const fn backing_bytes(&self) -> usize {
        self.payload.backing_bytes
    }
}

impl fmt::Debug for TcpDataSegment {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TcpDataSegment")
            .field("offset", &self.offset)
            .field("end_offset", &self.end_offset)
            .field("payload_len", &self.payload.len())
            .field("backing_bytes", &self.payload.backing_bytes)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TcpSendSnapshot {
    peer_acked: ByteOffset,
    next_sent: ByteOffset,
    retained_bytes: usize,
    retained_backing_bytes: usize,
    segment_count: usize,
    final_offset: Option<ByteOffset>,
    final_accepted: bool,
}

impl TcpSendSnapshot {
    pub const fn peer_acked(self) -> ByteOffset {
        self.peer_acked
    }

    pub const fn next_sent(self) -> ByteOffset {
        self.next_sent
    }

    pub const fn retained_bytes(self) -> usize {
        self.retained_bytes
    }

    pub const fn retained_backing_bytes(self) -> usize {
        self.retained_backing_bytes
    }

    pub const fn segment_count(self) -> usize {
        self.segment_count
    }

    pub const fn final_offset(self) -> Option<ByteOffset> {
        self.final_offset
    }

    pub const fn final_accepted(self) -> bool {
        self.final_accepted
    }
}

/// Positive ownership delta returned after a successful append.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TcpSendAppend {
    offset: ByteOffset,
    end_offset: ByteOffset,
    retained_bytes_added: usize,
    retained_backing_bytes_added: usize,
    ownership_bytes_copied: usize,
    segments_added: usize,
}

impl TcpSendAppend {
    pub const fn offset(self) -> ByteOffset {
        self.offset
    }

    pub const fn end_offset(self) -> ByteOffset {
        self.end_offset
    }

    pub const fn retained_bytes_added(self) -> usize {
        self.retained_bytes_added
    }

    pub const fn retained_backing_bytes_added(self) -> usize {
        self.retained_backing_bytes_added
    }

    pub const fn ownership_bytes_copied(self) -> usize {
        self.ownership_bytes_copied
    }

    pub const fn segments_added(self) -> usize {
        self.segments_added
    }
}

/// Negative ownership delta returned after a valid application ACK.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TcpSendAcknowledge {
    previous_peer_acked: ByteOffset,
    peer_acked: ByteOffset,
    retained_bytes_released: usize,
    retained_backing_bytes_released: usize,
    compaction_bytes_copied: usize,
    segments_released: usize,
    final_accepted_transition: bool,
}

impl TcpSendAcknowledge {
    pub const fn previous_peer_acked(self) -> ByteOffset {
        self.previous_peer_acked
    }

    pub const fn peer_acked(self) -> ByteOffset {
        self.peer_acked
    }

    pub const fn retained_bytes_released(self) -> usize {
        self.retained_bytes_released
    }

    pub const fn retained_backing_bytes_released(self) -> usize {
        self.retained_backing_bytes_released
    }

    pub const fn compaction_bytes_copied(self) -> usize {
        self.compaction_bytes_copied
    }

    pub const fn segments_released(self) -> usize {
        self.segments_released
    }

    pub const fn final_accepted_transition(self) -> bool {
        self.final_accepted_transition
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TcpSendClose {
    final_offset: ByteOffset,
    newly_recorded: bool,
    final_accepted: bool,
}

impl TcpSendClose {
    pub const fn final_offset(self) -> ByteOffset {
        self.final_offset
    }

    pub const fn newly_recorded(self) -> bool {
        self.newly_recorded
    }

    pub const fn final_accepted(self) -> bool {
        self.final_accepted
    }
}

/// Sender-side replay ownership for one TCP flow and one direction.
#[derive(Debug)]
pub struct TcpSendWindow {
    limits: TcpWindowLimits,
    segments: VecDeque<TcpDataSegment>,
    peer_acked: ByteOffset,
    next_sent: ByteOffset,
    retained_bytes: usize,
    retained_backing_bytes: usize,
    final_offset: Option<ByteOffset>,
    final_accepted: bool,
}

impl TcpSendWindow {
    pub fn new(limits: TcpWindowLimits) -> Self {
        Self {
            limits,
            segments: VecDeque::new(),
            peer_acked: ByteOffset::new(0),
            next_sent: ByteOffset::new(0),
            retained_bytes: 0,
            retained_backing_bytes: 0,
            final_offset: None,
            final_accepted: false,
        }
    }

    pub const fn limits(&self) -> TcpWindowLimits {
        self.limits
    }

    pub fn snapshot(&self) -> TcpSendSnapshot {
        TcpSendSnapshot {
            peer_acked: self.peer_acked,
            next_sent: self.next_sent,
            retained_bytes: self.retained_bytes,
            retained_backing_bytes: self.retained_backing_bytes,
            segment_count: self.segments.len(),
            final_offset: self.final_offset,
            final_accepted: self.final_accepted,
        }
    }

    pub const fn peer_acked(&self) -> ByteOffset {
        self.peer_acked
    }

    pub const fn next_sent(&self) -> ByteOffset {
        self.next_sent
    }

    pub const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    pub const fn retained_backing_bytes(&self) -> usize {
        self.retained_backing_bytes
    }

    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }

    pub const fn final_offset(&self) -> Option<ByteOffset> {
        self.final_offset
    }

    pub const fn final_accepted(&self) -> bool {
        self.final_accepted
    }

    /// Reserves both capacity dimensions before taking ownership of `payload`.
    pub fn append(
        &mut self,
        offset: ByteOffset,
        payload: Bytes,
    ) -> Result<TcpSendAppend, TcpOwnershipError> {
        if payload.is_empty() {
            return Err(TcpOwnershipError::EmptyData);
        }
        if payload.len() > MAX_DATA_PAYLOAD_BYTES {
            return Err(TcpOwnershipError::DataTooLarge {
                actual: payload.len(),
                max: MAX_DATA_PAYLOAD_BYTES,
            });
        }
        if let Some(final_offset) = self.final_offset {
            return Err(TcpOwnershipError::SendAfterClose { final_offset });
        }
        if offset != self.next_sent {
            return Err(TcpOwnershipError::UnexpectedSendOffset {
                expected: self.next_sent,
                actual: offset,
            });
        }
        let end_offset = checked_advance(offset, payload.len())?;
        let retained_after = self.retained_bytes.checked_add(payload.len()).ok_or(
            TcpOwnershipError::ByteCapacityExceeded {
                attempted: usize::MAX,
                max: self.limits.max_bytes,
            },
        )?;
        if retained_after > self.limits.max_bytes {
            return Err(TcpOwnershipError::ByteCapacityExceeded {
                attempted: retained_after,
                max: self.limits.max_bytes,
            });
        }
        let segments_after = self.segments.len().checked_add(1).ok_or(
            TcpOwnershipError::SegmentCapacityExceeded {
                attempted: usize::MAX,
                max: self.limits.max_segments,
            },
        )?;
        if segments_after > self.limits.max_segments {
            return Err(TcpOwnershipError::SegmentCapacityExceeded {
                attempted: segments_after,
                max: self.limits.max_segments,
            });
        }

        let retained_bytes_added = payload.len();
        let payload = OwnedPayload::new(&payload);
        let retained_backing_bytes_added = payload.backing_bytes;
        let retained_backing_after = self
            .retained_backing_bytes
            .checked_add(retained_backing_bytes_added)
            .ok_or(TcpOwnershipError::InvariantViolation(
                "sender retained backing byte accounting overflow",
            ))?;
        self.segments.push_back(TcpDataSegment {
            offset,
            end_offset,
            payload,
        });
        self.retained_bytes = retained_after;
        self.retained_backing_bytes = retained_backing_after;
        self.next_sent = end_offset;
        debug_assert!(self.backing_is_bounded());

        Ok(TcpSendAppend {
            offset,
            end_offset,
            retained_bytes_added,
            retained_backing_bytes_added,
            ownership_bytes_copied: retained_bytes_added,
            segments_added: 1,
        })
    }

    /// Applies an application ACK and releases the acknowledged replay prefix.
    ///
    /// `final_accepted` is the FIN/application-close acknowledgement carried by
    /// the wire ACK record.  It is monotonic and is accepted only after an
    /// exact local close and an ACK through that close's final offset.
    pub fn acknowledge(
        &mut self,
        next_accepted: ByteOffset,
        final_accepted: bool,
    ) -> Result<TcpSendAcknowledge, TcpOwnershipError> {
        if next_accepted < self.peer_acked {
            return Err(TcpOwnershipError::AckRegression {
                peer_acked: self.peer_acked,
                attempted: next_accepted,
            });
        }
        if next_accepted > self.next_sent {
            return Err(TcpOwnershipError::AckBeyondSent {
                next_sent: self.next_sent,
                attempted: next_accepted,
            });
        }
        if final_accepted {
            let final_offset = self
                .final_offset
                .ok_or(TcpOwnershipError::FinalAckBeforeClose)?;
            if next_accepted != final_offset {
                return Err(TcpOwnershipError::FinalAckOffsetMismatch {
                    expected: final_offset,
                    actual: next_accepted,
                });
            }
        }

        let previous_peer_acked = self.peer_acked;
        let released = offset_distance(previous_peer_acked, next_accepted)?;
        let mut released_segments = 0usize;
        let mut released_backing_bytes = 0usize;
        let mut compaction_bytes_copied = 0usize;

        while let Some(front) = self.segments.front() {
            if front.end_offset() <= next_accepted {
                released_backing_bytes = released_backing_bytes
                    .checked_add(front.backing_bytes())
                    .ok_or(TcpOwnershipError::InvariantViolation(
                        "sender released backing byte accounting overflow",
                    ))?;
                self.segments.pop_front();
                released_segments += 1;
                continue;
            }
            if front.offset() < next_accepted {
                let prefix = offset_distance(front.offset(), next_accepted)?;
                let previous_backing_bytes = front.backing_bytes();
                let (remainder, copied) = front.payload.retained_suffix(prefix);
                released_backing_bytes = released_backing_bytes
                    .checked_add(previous_backing_bytes - remainder.backing_bytes)
                    .ok_or(TcpOwnershipError::InvariantViolation(
                        "sender released backing byte accounting overflow",
                    ))?;
                compaction_bytes_copied = compaction_bytes_copied.checked_add(copied).ok_or(
                    TcpOwnershipError::InvariantViolation(
                        "sender compaction copy byte accounting overflow",
                    ),
                )?;
                let front =
                    self.segments
                        .front_mut()
                        .ok_or(TcpOwnershipError::InvariantViolation(
                            "sender front disappeared during partial ACK",
                        ))?;
                front.offset = next_accepted;
                front.payload = remainder;
            }
            break;
        }

        self.retained_bytes = self.retained_bytes.checked_sub(released).ok_or(
            TcpOwnershipError::InvariantViolation("sender retained byte accounting underflow"),
        )?;
        self.retained_backing_bytes = self
            .retained_backing_bytes
            .checked_sub(released_backing_bytes)
            .ok_or(TcpOwnershipError::InvariantViolation(
                "sender retained backing byte accounting underflow",
            ))?;
        self.peer_acked = next_accepted;
        let final_accepted_transition = final_accepted && !self.final_accepted;
        if final_accepted {
            self.final_accepted = true;
        }

        debug_assert_eq!(
            self.retained_bytes as u64,
            self.next_sent.get() - self.peer_acked.get()
        );
        debug_assert!(self.backing_is_bounded());

        Ok(TcpSendAcknowledge {
            previous_peer_acked,
            peer_acked: next_accepted,
            retained_bytes_released: released,
            retained_backing_bytes_released: released_backing_bytes,
            compaction_bytes_copied,
            segments_released: released_segments,
            final_accepted_transition,
        })
    }

    /// Records the exact final byte offset. Repeating the same CLOSE is
    /// idempotent; a different final offset fails closed.
    pub fn close(&mut self, final_offset: ByteOffset) -> Result<TcpSendClose, TcpOwnershipError> {
        if let Some(recorded) = self.final_offset {
            if recorded != final_offset {
                return Err(TcpOwnershipError::ConflictingFinalOffset {
                    recorded,
                    attempted: final_offset,
                });
            }
            return Ok(TcpSendClose {
                final_offset,
                newly_recorded: false,
                final_accepted: self.final_accepted,
            });
        }
        if final_offset != self.next_sent {
            return Err(TcpOwnershipError::CloseOffsetMismatch {
                expected: self.next_sent,
                actual: final_offset,
            });
        }
        self.final_offset = Some(final_offset);
        Ok(TcpSendClose {
            final_offset,
            newly_recorded: true,
            final_accepted: false,
        })
    }

    /// Zero-copy replay view of exactly `[peer_acked, next_sent)`.
    pub fn replay_view(&self) -> impl ExactSizeIterator<Item = TcpDataSegment> + '_ {
        self.segments.iter().cloned()
    }

    fn backing_is_bounded(&self) -> bool {
        let exact_backing = self.segments.iter().try_fold(0usize, |total, segment| {
            total.checked_add(segment.backing_bytes())
        });
        self.segments
            .iter()
            .all(|segment| segment.payload.backing_is_bounded())
            && exact_backing == Some(self.retained_backing_bytes)
            && ((self.retained_bytes == 0 && self.retained_backing_bytes == 0)
                || (self.retained_bytes != 0
                    && self.retained_backing_bytes / RETAINED_SLICE_COMPACT_RATIO
                        < self.retained_bytes))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TcpReceiveSnapshot {
    accepted: ByteOffset,
    contiguous_received: ByteOffset,
    highest_received: ByteOffset,
    buffered_bytes: usize,
    buffered_backing_bytes: usize,
    range_count: usize,
    final_offset: Option<ByteOffset>,
    half_close_ready: bool,
    abandoned: bool,
}

impl TcpReceiveSnapshot {
    pub const fn accepted(self) -> ByteOffset {
        self.accepted
    }

    pub const fn contiguous_received(self) -> ByteOffset {
        self.contiguous_received
    }

    pub const fn highest_received(self) -> ByteOffset {
        self.highest_received
    }

    pub const fn buffered_bytes(self) -> usize {
        self.buffered_bytes
    }

    pub const fn buffered_backing_bytes(self) -> usize {
        self.buffered_backing_bytes
    }

    pub const fn range_count(self) -> usize {
        self.range_count
    }

    pub const fn final_offset(self) -> Option<ByteOffset> {
        self.final_offset
    }

    pub const fn half_close_ready(self) -> bool {
        self.half_close_ready
    }

    pub const fn abandoned(self) -> bool {
        self.abandoned
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TcpReceiveDisposition {
    /// At least one previously unowned byte was buffered.
    Buffered,
    /// Every byte at or above `accepted` was already buffered identically.
    Duplicate,
    /// The complete DATA extent was below `accepted` and is no longer
    /// authoritative. It is ignored without retaining accepted history.
    StaleDuplicate,
}

/// Receiver ownership delta. DATA receipt never contains an application ACK.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TcpReceiveData {
    disposition: TcpReceiveDisposition,
    original_offset: ByteOffset,
    end_offset: ByteOffset,
    buffered_bytes_added: usize,
    buffered_backing_bytes_added: usize,
    ownership_bytes_copied: usize,
    ranges_added: usize,
}

impl TcpReceiveData {
    pub const fn disposition(self) -> TcpReceiveDisposition {
        self.disposition
    }

    pub const fn original_offset(self) -> ByteOffset {
        self.original_offset
    }

    pub const fn end_offset(self) -> ByteOffset {
        self.end_offset
    }

    pub const fn buffered_bytes_added(self) -> usize {
        self.buffered_bytes_added
    }

    pub const fn buffered_backing_bytes_added(self) -> usize {
        self.buffered_backing_bytes_added
    }

    pub const fn ownership_bytes_copied(self) -> usize {
        self.ownership_bytes_copied
    }

    pub const fn ranges_added(self) -> usize {
        self.ranges_added
    }
}

/// Receiver ownership release after the local application sink accepted data.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TcpReceiveAccept {
    previous_accepted: ByteOffset,
    accepted: ByteOffset,
    accepted_bytes: usize,
    buffered_backing_bytes_released: usize,
    compaction_bytes_copied: usize,
    ranges_released: usize,
    half_close_ready_transition: bool,
}

impl TcpReceiveAccept {
    pub const fn previous_accepted(self) -> ByteOffset {
        self.previous_accepted
    }

    /// New application ACK offset, present only when the sink accepted bytes.
    pub const fn application_ack(self) -> Option<ByteOffset> {
        if self.accepted_bytes == 0 {
            None
        } else {
            Some(self.accepted)
        }
    }

    pub const fn accepted(self) -> ByteOffset {
        self.accepted
    }

    pub const fn accepted_bytes(self) -> usize {
        self.accepted_bytes
    }

    /// Exact negative delta for the receiver's buffered-byte accounting.
    pub const fn buffered_bytes_released(self) -> usize {
        self.accepted_bytes
    }

    pub const fn buffered_backing_bytes_released(self) -> usize {
        self.buffered_backing_bytes_released
    }

    pub const fn compaction_bytes_copied(self) -> usize {
        self.compaction_bytes_copied
    }

    pub const fn ranges_released(self) -> usize {
        self.ranges_released
    }

    pub const fn half_close_ready_transition(self) -> bool {
        self.half_close_ready_transition
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TcpReceiveAcceptPreview {
    accepted: ByteOffset,
    accepted_bytes: usize,
    ranges_released: usize,
}

impl TcpReceiveAcceptPreview {
    pub(crate) const fn accepted(self) -> ByteOffset {
        self.accepted
    }

    pub(crate) const fn accepted_bytes(self) -> usize {
        self.accepted_bytes
    }

    pub(crate) const fn ranges_released(self) -> usize {
        self.ranges_released
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TcpReceiveClose {
    final_offset: ByteOffset,
    newly_recorded: bool,
    half_close_ready: bool,
    half_close_ready_transition: bool,
}

impl TcpReceiveClose {
    pub const fn final_offset(self) -> ByteOffset {
        self.final_offset
    }

    pub const fn newly_recorded(self) -> bool {
        self.newly_recorded
    }

    pub const fn half_close_ready(self) -> bool {
        self.half_close_ready
    }

    pub const fn half_close_ready_transition(self) -> bool {
        self.half_close_ready_transition
    }
}

/// Terminal receiver release caused by local abandonment.
///
/// Unlike [`TcpReceiveAccept`], this type intentionally has no application ACK
/// accessor: discarded bytes never become peer-authoritative accepted bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TcpReceiveAbandon {
    accepted: ByteOffset,
    buffered_bytes_dropped: usize,
    buffered_backing_bytes_dropped: usize,
    ranges_dropped: usize,
    newly_abandoned: bool,
}

/// Crate-private descriptor for one already-validated receive extent.
///
/// The reservation binds the exact immutable input bytes and the receiver
/// epoch observed by [`TcpReceiveWindow::preview_receive`]. Its crate-private
/// surface exposes only the ownership deltas needed by the session's
/// aggregate-budget owner. It is deliberately not exported as a cross-window
/// capability: the session previews and commits it synchronously against the
/// same flow. Constructing a reservation never changes the receive window.
pub(crate) struct TcpReceiveReservation {
    limits: TcpWindowLimits,
    epoch: u64,
    accepted: ByteOffset,
    final_offset: Option<ByteOffset>,
    abandoned: bool,
    original_offset: ByteOffset,
    end_offset: ByteOffset,
    payload: Bytes,
    overlapping_ranges: Vec<(u64, Bytes)>,
    uncovered: Vec<(u64, u64)>,
    buffered_bytes_before: usize,
    buffered_backing_bytes_before: usize,
    ranges_before: usize,
    buffered_bytes_after: usize,
    buffered_backing_bytes_after: usize,
    ranges_after: usize,
}

impl TcpReceiveReservation {
    pub const fn buffered_bytes_added(&self) -> usize {
        self.buffered_bytes_after - self.buffered_bytes_before
    }

    pub const fn buffered_backing_bytes_added(&self) -> usize {
        self.buffered_backing_bytes_after - self.buffered_backing_bytes_before
    }

    pub const fn ranges_added(&self) -> usize {
        self.ranges_after - self.ranges_before
    }

    pub fn disposition(&self) -> TcpReceiveDisposition {
        if self.end_offset <= self.accepted {
            TcpReceiveDisposition::StaleDuplicate
        } else if self.buffered_bytes_added() == 0 {
            TcpReceiveDisposition::Duplicate
        } else {
            TcpReceiveDisposition::Buffered
        }
    }
}

impl fmt::Debug for TcpReceiveReservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TcpReceiveReservation")
            .field("epoch", &self.epoch)
            .field("accepted", &self.accepted)
            .field("original_offset", &self.original_offset)
            .field("end_offset", &self.end_offset)
            .field("payload_len", &self.payload.len())
            .field("buffered_bytes_added", &self.buffered_bytes_added())
            .field("ranges_added", &self.ranges_added())
            .finish()
    }
}

impl TcpReceiveAbandon {
    pub const fn accepted(self) -> ByteOffset {
        self.accepted
    }

    pub const fn buffered_bytes_dropped(self) -> usize {
        self.buffered_bytes_dropped
    }

    pub const fn buffered_backing_bytes_dropped(self) -> usize {
        self.buffered_backing_bytes_dropped
    }

    pub const fn ranges_dropped(self) -> usize {
        self.ranges_dropped
    }

    pub const fn newly_abandoned(self) -> bool {
        self.newly_abandoned
    }
}

/// Receiver-side reorder/dedup ownership for one TCP flow and one direction.
#[derive(Debug)]
pub struct TcpReceiveWindow {
    limits: TcpWindowLimits,
    ranges: BTreeMap<u64, OwnedPayload>,
    epoch: u64,
    accepted: ByteOffset,
    buffered_bytes: usize,
    buffered_backing_bytes: usize,
    final_offset: Option<ByteOffset>,
    abandoned: bool,
}

impl TcpReceiveWindow {
    pub fn new(limits: TcpWindowLimits) -> Self {
        Self {
            limits,
            ranges: BTreeMap::new(),
            epoch: 0,
            accepted: ByteOffset::new(0),
            buffered_bytes: 0,
            buffered_backing_bytes: 0,
            final_offset: None,
            abandoned: false,
        }
    }

    pub const fn limits(&self) -> TcpWindowLimits {
        self.limits
    }

    pub const fn accepted(&self) -> ByteOffset {
        self.accepted
    }

    pub const fn buffered_bytes(&self) -> usize {
        self.buffered_bytes
    }

    pub const fn buffered_backing_bytes(&self) -> usize {
        self.buffered_backing_bytes
    }

    pub fn range_count(&self) -> usize {
        self.ranges.len()
    }

    pub const fn final_offset(&self) -> Option<ByteOffset> {
        self.final_offset
    }

    pub const fn is_abandoned(&self) -> bool {
        self.abandoned
    }

    pub fn half_close_ready(&self) -> bool {
        !self.abandoned && self.final_offset == Some(self.accepted)
    }

    pub fn snapshot(&self) -> TcpReceiveSnapshot {
        TcpReceiveSnapshot {
            accepted: self.accepted,
            contiguous_received: self.contiguous_received(),
            highest_received: self.highest_received(),
            buffered_bytes: self.buffered_bytes,
            buffered_backing_bytes: self.buffered_backing_bytes,
            range_count: self.ranges.len(),
            final_offset: self.final_offset,
            half_close_ready: self.half_close_ready(),
            abandoned: self.abandoned,
        }
    }

    /// Validates one DATA extent and returns its exact ownership delta without
    /// mutating the window.
    ///
    /// Ranges wholly below `accepted` are stale duplicates.  For a partial
    /// stale overlap, the accepted prefix is ignored and only the still-live
    /// suffix participates in buffered overlap validation.  This explicitly
    /// makes `accepted` the authority boundary; the model does not retain an
    /// unbounded history merely to compare already-acknowledged bytes.
    pub(crate) fn preview_receive(
        &self,
        offset: ByteOffset,
        payload: &Bytes,
    ) -> Result<TcpReceiveReservation, TcpOwnershipError> {
        if self.abandoned {
            return Err(TcpOwnershipError::ReceiveAfterAbandon);
        }
        if payload.is_empty() {
            return Err(TcpOwnershipError::EmptyData);
        }
        if payload.len() > MAX_DATA_PAYLOAD_BYTES {
            return Err(TcpOwnershipError::DataTooLarge {
                actual: payload.len(),
                max: MAX_DATA_PAYLOAD_BYTES,
            });
        }
        let original_offset = offset;
        let end_offset = checked_advance(offset, payload.len())?;

        if end_offset <= self.accepted {
            return Ok(TcpReceiveReservation {
                limits: self.limits,
                epoch: self.epoch,
                accepted: self.accepted,
                final_offset: self.final_offset,
                abandoned: self.abandoned,
                original_offset,
                end_offset,
                payload: payload.clone(),
                overlapping_ranges: Vec::new(),
                uncovered: Vec::new(),
                buffered_bytes_before: self.buffered_bytes,
                buffered_backing_bytes_before: self.buffered_backing_bytes,
                ranges_before: self.ranges.len(),
                buffered_bytes_after: self.buffered_bytes,
                buffered_backing_bytes_after: self.buffered_backing_bytes,
                ranges_after: self.ranges.len(),
            });
        }

        if let Some(final_offset) = self.final_offset
            && end_offset > final_offset
        {
            return Err(TcpOwnershipError::DataBeyondFinalOffset {
                final_offset,
                attempted_end: end_offset,
            });
        }

        let window_end = self
            .accepted
            .get()
            .saturating_add(usize_to_u64(self.limits.max_bytes)?);
        if end_offset.get() > window_end {
            return Err(TcpOwnershipError::ReceiveWindowExceeded {
                accepted: self.accepted,
                attempted_end: end_offset,
                max_end: ByteOffset::new(window_end),
            });
        }

        let live_start = offset.get().max(self.accepted.get());
        let live_end = end_offset.get();

        // First pass only validates overlaps. No state is mutated until every
        // byte, range, and capacity check succeeds.
        let mut overlapping_ranges = Vec::new();
        for (&range_start, range_payload) in self.ranges.range(..live_end) {
            let range_end = checked_end_raw(range_start, range_payload.len())?;
            if range_end <= live_start {
                continue;
            }
            overlapping_ranges.push((range_start, range_payload.bytes.clone()));
            let overlap_start = range_start.max(live_start);
            let overlap_end = range_end.min(live_end);
            if overlap_start >= overlap_end {
                continue;
            }
            let existing_start = offset_distance_raw(range_start, overlap_start)?;
            let candidate_start = offset_distance_raw(offset.get(), overlap_start)?;
            let overlap_len = offset_distance_raw(overlap_start, overlap_end)?;
            if range_payload
                .bytes
                .slice(existing_start..existing_start + overlap_len)
                != payload.slice(candidate_start..candidate_start + overlap_len)
            {
                let mismatch = first_mismatch(
                    &range_payload.bytes[existing_start..existing_start + overlap_len],
                    &payload[candidate_start..candidate_start + overlap_len],
                );
                let conflict_offset = overlap_start.checked_add(usize_to_u64(mismatch)?).ok_or(
                    TcpOwnershipError::OffsetOverflow {
                        offset: ByteOffset::new(overlap_start),
                        bytes: mismatch,
                    },
                )?;
                return Err(TcpOwnershipError::ConflictingOverlap {
                    offset: ByteOffset::new(conflict_offset),
                });
            }
        }

        let mut uncovered = Vec::new();
        let mut cursor = live_start;
        for (&range_start, range_payload) in self.ranges.range(..live_end) {
            let range_end = checked_end_raw(range_start, range_payload.len())?;
            if range_end <= cursor {
                continue;
            }
            if range_start > cursor {
                uncovered.push((cursor, range_start.min(live_end)));
            }
            cursor = cursor.max(range_end.min(live_end));
            if cursor == live_end {
                break;
            }
        }
        if cursor < live_end {
            uncovered.push((cursor, live_end));
        }

        let mut added_bytes = 0usize;
        for (start, end) in &uncovered {
            added_bytes = added_bytes
                .checked_add(offset_distance_raw(*start, *end)?)
                .ok_or(TcpOwnershipError::ByteCapacityExceeded {
                    attempted: usize::MAX,
                    max: self.limits.max_bytes,
                })?;
        }
        let buffered_after = self.buffered_bytes.checked_add(added_bytes).ok_or(
            TcpOwnershipError::ByteCapacityExceeded {
                attempted: usize::MAX,
                max: self.limits.max_bytes,
            },
        )?;
        if buffered_after > self.limits.max_bytes {
            return Err(TcpOwnershipError::ByteCapacityExceeded {
                attempted: buffered_after,
                max: self.limits.max_bytes,
            });
        }
        let ranges_added = uncovered.len();
        let ranges_after = self.ranges.len().checked_add(ranges_added).ok_or(
            TcpOwnershipError::SegmentCapacityExceeded {
                attempted: usize::MAX,
                max: self.limits.max_segments,
            },
        )?;
        if ranges_after > self.limits.max_segments {
            return Err(TcpOwnershipError::SegmentCapacityExceeded {
                attempted: ranges_after,
                max: self.limits.max_segments,
            });
        }

        // Each uncovered byte is copied exactly once into a length-bounded
        // allocation, so physical backing added is identical to logical
        // ownership added. Keep this calculation ahead of mutation.
        let added_backing_bytes = added_bytes;
        let buffered_backing_after = self
            .buffered_backing_bytes
            .checked_add(added_backing_bytes)
            .ok_or(TcpOwnershipError::InvariantViolation(
                "receiver backing byte accounting overflow",
            ))?;
        Ok(TcpReceiveReservation {
            limits: self.limits,
            epoch: self.epoch,
            accepted: self.accepted,
            final_offset: self.final_offset,
            abandoned: self.abandoned,
            original_offset,
            end_offset,
            payload: payload.clone(),
            overlapping_ranges,
            uncovered,
            buffered_bytes_before: self.buffered_bytes,
            buffered_backing_bytes_before: self.buffered_backing_bytes,
            ranges_before: self.ranges.len(),
            buffered_bytes_after: buffered_after,
            buffered_backing_bytes_after: buffered_backing_after,
            ranges_after,
        })
    }

    /// Commits an exact receive reservation. Every fallible validation and
    /// allocation completes before the first window mutation.
    pub(crate) fn commit_receive(
        &mut self,
        reservation: TcpReceiveReservation,
        offset: ByteOffset,
        payload: Bytes,
    ) -> Result<TcpReceiveData, TcpOwnershipError> {
        let attempted_end = checked_advance(offset, payload.len())?;
        if offset != reservation.original_offset || attempted_end != reservation.end_offset {
            return Err(TcpOwnershipError::ReceiveReservationExtentMismatch {
                reserved_offset: reservation.original_offset,
                reserved_end: reservation.end_offset,
                attempted_offset: offset,
                attempted_end,
            });
        }
        if payload != reservation.payload {
            return Err(TcpOwnershipError::ReceiveReservationPayloadMismatch);
        }
        if self.limits != reservation.limits
            || self.epoch != reservation.epoch
            || self.accepted != reservation.accepted
            || self.final_offset != reservation.final_offset
            || self.abandoned != reservation.abandoned
            || self.buffered_bytes != reservation.buffered_bytes_before
            || self.buffered_backing_bytes != reservation.buffered_backing_bytes_before
            || self.ranges.len() != reservation.ranges_before
        {
            return Err(TcpOwnershipError::StaleReceiveReservation {
                reserved_epoch: reservation.epoch,
                current_epoch: self.epoch,
            });
        }
        let live_start = reservation
            .original_offset
            .get()
            .max(reservation.accepted.get());
        let live_end = reservation.end_offset.get();
        let mut observed = reservation.overlapping_ranges.iter();
        for (&start, current) in self.ranges.range(..live_end) {
            let current_end = checked_end_raw(start, current.len())?;
            if current_end <= live_start {
                continue;
            }
            let Some((reserved_start, reserved_payload)) = observed.next() else {
                return Err(TcpOwnershipError::StaleReceiveReservation {
                    reserved_epoch: reservation.epoch,
                    current_epoch: self.epoch,
                });
            };
            if start != *reserved_start || current.bytes != *reserved_payload {
                return Err(TcpOwnershipError::StaleReceiveReservation {
                    reserved_epoch: reservation.epoch,
                    current_epoch: self.epoch,
                });
            }
        }
        if observed.next().is_some() {
            return Err(TcpOwnershipError::StaleReceiveReservation {
                reserved_epoch: reservation.epoch,
                current_epoch: self.epoch,
            });
        }
        if reservation
            .uncovered
            .iter()
            .any(|(start, _)| self.ranges.contains_key(start))
        {
            return Err(TcpOwnershipError::StaleReceiveReservation {
                reserved_epoch: reservation.epoch,
                current_epoch: self.epoch,
            });
        }

        let mut owned_ranges = Vec::with_capacity(reservation.uncovered.len());
        for &(start, end) in &reservation.uncovered {
            let payload_start = offset_distance_raw(offset.get(), start)?;
            let payload_len = offset_distance_raw(start, end)?;
            let owned =
                OwnedPayload::new(&payload.slice(payload_start..payload_start + payload_len));
            debug_assert_eq!(owned.backing_bytes, payload_len);
            owned_ranges.push((start, owned));
        }
        let next_epoch = if owned_ranges.is_empty() {
            self.epoch
        } else {
            self.epoch
                .checked_add(1)
                .ok_or(TcpOwnershipError::ReceiveEpochExhausted)?
        };

        for (start, owned) in owned_ranges {
            let replaced = self.ranges.insert(start, owned);
            debug_assert!(replaced.is_none());
        }
        self.buffered_bytes = reservation.buffered_bytes_after;
        self.buffered_backing_bytes = reservation.buffered_backing_bytes_after;
        self.epoch = next_epoch;
        debug_assert!(self.backing_is_bounded());

        Ok(TcpReceiveData {
            disposition: reservation.disposition(),
            original_offset: reservation.original_offset,
            end_offset: reservation.end_offset,
            buffered_bytes_added: reservation.buffered_bytes_added(),
            buffered_backing_bytes_added: reservation.buffered_backing_bytes_added(),
            ownership_bytes_copied: reservation.buffered_bytes_added(),
            ranges_added: reservation.ranges_added(),
        })
    }

    /// Convenience path for callers that do not own an aggregate budget.
    pub fn receive(
        &mut self,
        offset: ByteOffset,
        payload: Bytes,
    ) -> Result<TcpReceiveData, TcpOwnershipError> {
        let reservation = self.preview_receive(offset, &payload)?;
        self.commit_receive(reservation, offset, payload)
    }

    /// Returns up to `max_bytes` contiguous bytes starting at `accepted`.
    /// Returned `Bytes` handles share normalized backing storage with the
    /// window; no payload bytes are copied.
    pub fn peek_contiguous(&self, max_bytes: usize) -> Vec<TcpDataSegment> {
        if max_bytes == 0 || self.abandoned {
            return Vec::new();
        }
        let mut result = Vec::new();
        let mut cursor = self.accepted.get();
        let mut remaining = max_bytes;
        while remaining > 0 {
            let Some(payload) = self.ranges.get(&cursor) else {
                break;
            };
            let take = remaining.min(payload.len());
            let Ok(segment_end) = checked_end_raw(cursor, take) else {
                break;
            };
            result.push(TcpDataSegment {
                offset: ByteOffset::new(cursor),
                end_offset: ByteOffset::new(segment_end),
                payload: payload.slice(0..take),
            });
            remaining -= take;
            if take < payload.len() {
                break;
            }
            let Ok(next) = checked_end_raw(cursor, payload.len()) else {
                break;
            };
            cursor = next;
        }
        result
    }

    pub(crate) fn preview_accept(
        &self,
        max_bytes: usize,
    ) -> Result<TcpReceiveAcceptPreview, TcpOwnershipError> {
        if self.abandoned {
            return Err(TcpOwnershipError::AcceptAfterAbandon);
        }
        let mut cursor = self.accepted;
        let mut remaining = max_bytes;
        let mut accepted_bytes = 0usize;
        let mut ranges_released = 0usize;
        while remaining > 0 {
            let Some(payload) = self.ranges.get(&cursor.get()) else {
                break;
            };
            let take = remaining.min(payload.len());
            cursor = checked_advance(cursor, take)?;
            accepted_bytes =
                accepted_bytes
                    .checked_add(take)
                    .ok_or(TcpOwnershipError::InvariantViolation(
                        "receiver accepted byte overflow",
                    ))?;
            remaining -= take;
            if take == payload.len() {
                ranges_released += 1;
            } else {
                break;
            }
        }
        Ok(TcpReceiveAcceptPreview {
            accepted: cursor,
            accepted_bytes,
            ranges_released,
        })
    }

    /// Transfers up to `max_bytes` of the contiguous prefix to the application
    /// sink. The caller must pass only bytes its sink actually accepted.
    pub fn accept(&mut self, max_bytes: usize) -> Result<TcpReceiveAccept, TcpOwnershipError> {
        let preview = self.preview_accept(max_bytes)?;
        let next_epoch = if preview.accepted_bytes > 0 {
            Some(
                self.epoch
                    .checked_add(1)
                    .ok_or(TcpOwnershipError::ReceiveEpochExhausted)?,
            )
        } else {
            None
        };
        let previous_accepted = self.accepted;
        let was_half_close_ready = self.half_close_ready();
        let mut remaining = max_bytes;
        let mut accepted_bytes = 0usize;
        let mut released_backing_bytes = 0usize;
        let mut compaction_bytes_copied = 0usize;
        let mut ranges_released = 0usize;

        while remaining > 0 {
            let key = self.accepted.get();
            let Some(payload) = self.ranges.remove(&key) else {
                break;
            };
            let take = remaining.min(payload.len());
            let next = checked_advance(self.accepted, take)?;
            if take == payload.len() {
                released_backing_bytes = released_backing_bytes
                    .checked_add(payload.backing_bytes)
                    .ok_or(TcpOwnershipError::InvariantViolation(
                        "receiver released backing byte accounting overflow",
                    ))?;
                ranges_released += 1;
            } else {
                let previous_backing_bytes = payload.backing_bytes;
                let (suffix, copied) = payload.retained_suffix(take);
                released_backing_bytes = released_backing_bytes
                    .checked_add(previous_backing_bytes - suffix.backing_bytes)
                    .ok_or(TcpOwnershipError::InvariantViolation(
                        "receiver released backing byte accounting overflow",
                    ))?;
                compaction_bytes_copied = compaction_bytes_copied.checked_add(copied).ok_or(
                    TcpOwnershipError::InvariantViolation(
                        "receiver compaction copy byte accounting overflow",
                    ),
                )?;
                if self.ranges.insert(next.get(), suffix).is_some() {
                    return Err(TcpOwnershipError::InvariantViolation(
                        "receiver partial accept collided with a range start",
                    ));
                }
            }
            self.accepted = next;
            remaining -= take;
            accepted_bytes =
                accepted_bytes
                    .checked_add(take)
                    .ok_or(TcpOwnershipError::InvariantViolation(
                        "receiver accepted byte overflow",
                    ))?;
            if take < payload.len() {
                break;
            }
        }

        self.buffered_bytes = self.buffered_bytes.checked_sub(accepted_bytes).ok_or(
            TcpOwnershipError::InvariantViolation("receiver buffered byte accounting underflow"),
        )?;
        self.buffered_backing_bytes = self
            .buffered_backing_bytes
            .checked_sub(released_backing_bytes)
            .ok_or(TcpOwnershipError::InvariantViolation(
                "receiver buffered backing byte accounting underflow",
            ))?;
        if let Some(next_epoch) = next_epoch {
            self.epoch = next_epoch;
        }
        let half_close_ready = self.half_close_ready();
        debug_assert!(self.backing_is_bounded());
        debug_assert_eq!(self.accepted, preview.accepted);
        debug_assert_eq!(accepted_bytes, preview.accepted_bytes);
        debug_assert_eq!(ranges_released, preview.ranges_released);

        Ok(TcpReceiveAccept {
            previous_accepted,
            accepted: self.accepted,
            accepted_bytes,
            buffered_backing_bytes_released: released_backing_bytes,
            compaction_bytes_copied,
            ranges_released,
            half_close_ready_transition: !was_half_close_ready && half_close_ready,
        })
    }

    /// Records an early or on-time CLOSE. The final offset may be ahead of a
    /// gap, but never behind accepted or already-buffered bytes.
    pub fn receive_close(
        &mut self,
        final_offset: ByteOffset,
    ) -> Result<TcpReceiveClose, TcpOwnershipError> {
        if self.abandoned {
            return Err(TcpOwnershipError::CloseAfterAbandon);
        }
        if let Some(recorded) = self.final_offset {
            if recorded != final_offset {
                return Err(TcpOwnershipError::ConflictingFinalOffset {
                    recorded,
                    attempted: final_offset,
                });
            }
            return Ok(TcpReceiveClose {
                final_offset,
                newly_recorded: false,
                half_close_ready: self.half_close_ready(),
                half_close_ready_transition: false,
            });
        }
        if final_offset < self.accepted {
            return Err(TcpOwnershipError::CloseBeforeAccepted {
                accepted: self.accepted,
                attempted: final_offset,
            });
        }
        let highest_received = self.highest_received();
        if final_offset < highest_received {
            return Err(TcpOwnershipError::CloseBeforeBufferedEnd {
                highest_received,
                attempted: final_offset,
            });
        }
        let next_epoch = self
            .epoch
            .checked_add(1)
            .ok_or(TcpOwnershipError::ReceiveEpochExhausted)?;
        self.final_offset = Some(final_offset);
        self.epoch = next_epoch;
        let half_close_ready = self.half_close_ready();
        Ok(TcpReceiveClose {
            final_offset,
            newly_recorded: true,
            half_close_ready,
            half_close_ready_transition: half_close_ready,
        })
    }

    /// Drops local ownership without advancing `accepted` or manufacturing an
    /// application ACK. Repeated abandonment is idempotent.
    pub fn abandon(&mut self) -> TcpReceiveAbandon {
        if self.abandoned {
            return TcpReceiveAbandon {
                accepted: self.accepted,
                buffered_bytes_dropped: 0,
                buffered_backing_bytes_dropped: 0,
                ranges_dropped: 0,
                newly_abandoned: false,
            };
        }
        let buffered_bytes_dropped = self.buffered_bytes;
        let buffered_backing_bytes_dropped = self.buffered_backing_bytes;
        let ranges_dropped = self.ranges.len();
        self.ranges.clear();
        self.buffered_bytes = 0;
        self.buffered_backing_bytes = 0;
        self.abandoned = true;
        self.epoch = self.epoch.saturating_add(1);
        TcpReceiveAbandon {
            accepted: self.accepted,
            buffered_bytes_dropped,
            buffered_backing_bytes_dropped,
            ranges_dropped,
            newly_abandoned: true,
        }
    }

    fn contiguous_received(&self) -> ByteOffset {
        let mut cursor = self.accepted.get();
        while let Some(payload) = self.ranges.get(&cursor) {
            let Ok(next) = checked_end_raw(cursor, payload.len()) else {
                return ByteOffset::new(u64::MAX);
            };
            cursor = next;
        }
        ByteOffset::new(cursor)
    }

    fn highest_received(&self) -> ByteOffset {
        let Some((&start, payload)) = self.ranges.last_key_value() else {
            return self.accepted;
        };
        match checked_end_raw(start, payload.len()) {
            Ok(end) => ByteOffset::new(end),
            Err(_) => ByteOffset::new(u64::MAX),
        }
    }

    fn backing_is_bounded(&self) -> bool {
        let exact_backing = self.ranges.values().try_fold(0usize, |total, payload| {
            total.checked_add(payload.backing_bytes)
        });
        self.ranges.values().all(OwnedPayload::backing_is_bounded)
            && exact_backing == Some(self.buffered_backing_bytes)
            && ((self.buffered_bytes == 0 && self.buffered_backing_bytes == 0)
                || (self.buffered_bytes != 0
                    && self.buffered_backing_bytes / RETAINED_SLICE_COMPACT_RATIO
                        < self.buffered_bytes))
    }
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum TcpOwnershipError {
    #[error("TCP ownership byte capacity must be non-zero")]
    ZeroByteCapacity,
    #[error("TCP ownership segment/range capacity must be non-zero")]
    ZeroSegmentCapacity,
    #[error("TCP DATA payload must not be empty")]
    EmptyData,
    #[error("TCP DATA payload is {actual} bytes; wire maximum is {max}")]
    DataTooLarge { actual: usize, max: usize },
    #[error("TCP offset {offset:?} cannot advance by {bytes} bytes")]
    OffsetOverflow { offset: ByteOffset, bytes: usize },
    #[error("expected sender offset {expected:?}, got {actual:?}")]
    UnexpectedSendOffset {
        expected: ByteOffset,
        actual: ByteOffset,
    },
    #[error("cannot append TCP DATA after CLOSE at {final_offset:?}")]
    SendAfterClose { final_offset: ByteOffset },
    #[error("TCP byte capacity exceeded: attempted {attempted}, max {max}")]
    ByteCapacityExceeded { attempted: usize, max: usize },
    #[error("TCP segment/range capacity exceeded: attempted {attempted}, max {max}")]
    SegmentCapacityExceeded { attempted: usize, max: usize },
    #[error("ACK regressed from {peer_acked:?} to {attempted:?}")]
    AckRegression {
        peer_acked: ByteOffset,
        attempted: ByteOffset,
    },
    #[error("ACK {attempted:?} exceeds next sent offset {next_sent:?}")]
    AckBeyondSent {
        next_sent: ByteOffset,
        attempted: ByteOffset,
    },
    #[error("final-accepted ACK arrived before local CLOSE")]
    FinalAckBeforeClose,
    #[error("final-accepted ACK offset {actual:?} does not equal final offset {expected:?}")]
    FinalAckOffsetMismatch {
        expected: ByteOffset,
        actual: ByteOffset,
    },
    #[error("CLOSE final offset {actual:?} does not equal next sent offset {expected:?}")]
    CloseOffsetMismatch {
        expected: ByteOffset,
        actual: ByteOffset,
    },
    #[error("conflicting CLOSE final offset: recorded {recorded:?}, got {attempted:?}")]
    ConflictingFinalOffset {
        recorded: ByteOffset,
        attempted: ByteOffset,
    },
    #[error("cannot receive DATA after local abandonment")]
    ReceiveAfterAbandon,
    #[error("cannot accept DATA after local abandonment")]
    AcceptAfterAbandon,
    #[error("cannot receive CLOSE after local abandonment")]
    CloseAfterAbandon,
    #[error("DATA ending at {attempted_end:?} exceeds final offset {final_offset:?}")]
    DataBeyondFinalOffset {
        final_offset: ByteOffset,
        attempted_end: ByteOffset,
    },
    #[error("DATA ending at {attempted_end:?} exceeds receive window [{accepted:?}, {max_end:?}]")]
    ReceiveWindowExceeded {
        accepted: ByteOffset,
        attempted_end: ByteOffset,
        max_end: ByteOffset,
    },
    #[error("conflicting TCP overlap at offset {offset:?}")]
    ConflictingOverlap { offset: ByteOffset },
    #[error(
        "stale TCP receive reservation: reserved epoch {reserved_epoch}, current epoch {current_epoch}"
    )]
    StaleReceiveReservation {
        reserved_epoch: u64,
        current_epoch: u64,
    },
    #[error(
        "TCP receive reservation extent [{reserved_offset:?}, {reserved_end:?}) does not match attempted extent [{attempted_offset:?}, {attempted_end:?})"
    )]
    ReceiveReservationExtentMismatch {
        reserved_offset: ByteOffset,
        reserved_end: ByteOffset,
        attempted_offset: ByteOffset,
        attempted_end: ByteOffset,
    },
    #[error("TCP receive reservation payload does not match the validated payload")]
    ReceiveReservationPayloadMismatch,
    #[error("TCP receive mutation epoch exhausted")]
    ReceiveEpochExhausted,
    #[error("CLOSE {attempted:?} is before accepted offset {accepted:?}")]
    CloseBeforeAccepted {
        accepted: ByteOffset,
        attempted: ByteOffset,
    },
    #[error("CLOSE {attempted:?} is before buffered end {highest_received:?}")]
    CloseBeforeBufferedEnd {
        highest_received: ByteOffset,
        attempted: ByteOffset,
    },
    #[error("TCP ownership invariant violated: {0}")]
    InvariantViolation(&'static str),
}

fn checked_advance(offset: ByteOffset, bytes: usize) -> Result<ByteOffset, TcpOwnershipError> {
    let bytes_u64 = usize_to_u64(bytes)?;
    offset
        .get()
        .checked_add(bytes_u64)
        .map(ByteOffset::new)
        .ok_or(TcpOwnershipError::OffsetOverflow { offset, bytes })
}

fn checked_end_raw(offset: u64, bytes: usize) -> Result<u64, TcpOwnershipError> {
    offset
        .checked_add(usize_to_u64(bytes)?)
        .ok_or(TcpOwnershipError::OffsetOverflow {
            offset: ByteOffset::new(offset),
            bytes,
        })
}

fn usize_to_u64(value: usize) -> Result<u64, TcpOwnershipError> {
    u64::try_from(value).map_err(|_| TcpOwnershipError::OffsetOverflow {
        offset: ByteOffset::new(0),
        bytes: value,
    })
}

fn offset_distance(start: ByteOffset, end: ByteOffset) -> Result<usize, TcpOwnershipError> {
    offset_distance_raw(start.get(), end.get())
}

fn offset_distance_raw(start: u64, end: u64) -> Result<usize, TcpOwnershipError> {
    let distance = end
        .checked_sub(start)
        .ok_or(TcpOwnershipError::InvariantViolation(
            "offset distance regressed",
        ))?;
    usize::try_from(distance).map_err(|_| TcpOwnershipError::OffsetOverflow {
        offset: ByteOffset::new(start),
        bytes: usize::MAX,
    })
}

fn first_mismatch(left: &[u8], right: &[u8]) -> usize {
    left.iter()
        .zip(right)
        .position(|(left, right)| left != right)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits(max_bytes: usize, max_segments: usize) -> TcpWindowLimits {
        TcpWindowLimits::new(max_bytes, max_segments).unwrap()
    }

    fn data(value: &'static str) -> Bytes {
        Bytes::from_static(value.as_bytes())
    }

    #[test]
    fn limits_reject_zero_in_each_dimension() {
        assert_eq!(
            TcpWindowLimits::new(0, 1),
            Err(TcpOwnershipError::ZeroByteCapacity)
        );
        assert_eq!(
            TcpWindowLimits::new(1, 0),
            Err(TcpOwnershipError::ZeroSegmentCapacity)
        );
    }

    #[test]
    fn sender_replay_is_exact_and_partial_ack_releases_prefix() {
        let mut sender = TcpSendWindow::new(limits(32, 4));
        let first = sender.append(ByteOffset::new(0), data("abcdef")).unwrap();
        let second = sender.append(ByteOffset::new(6), data("ghij")).unwrap();

        assert_eq!(first.offset(), ByteOffset::new(0));
        assert_eq!(first.end_offset(), ByteOffset::new(6));
        assert_eq!(first.retained_bytes_added(), 6);
        assert_eq!(first.retained_backing_bytes_added(), 6);
        assert_eq!(first.ownership_bytes_copied(), 6);
        assert_eq!(first.segments_added(), 1);
        assert_eq!(second.end_offset(), ByteOffset::new(10));

        let first_view = sender.replay_view().collect::<Vec<_>>();
        let second_view = sender.replay_view().collect::<Vec<_>>();
        assert_eq!(first_view, second_view);
        assert_eq!(
            first_view[0].payload().as_ptr(),
            second_view[0].payload().as_ptr()
        );
        assert_eq!(
            first_view[1].payload().as_ptr(),
            second_view[1].payload().as_ptr()
        );

        let partial = sender.acknowledge(ByteOffset::new(3), false).unwrap();
        assert_eq!(partial.previous_peer_acked(), ByteOffset::new(0));
        assert_eq!(partial.peer_acked(), ByteOffset::new(3));
        assert_eq!(partial.retained_bytes_released(), 3);
        assert_eq!(partial.retained_backing_bytes_released(), 0);
        assert_eq!(partial.compaction_bytes_copied(), 0);
        assert_eq!(partial.segments_released(), 0);
        assert!(!partial.final_accepted_transition());
        assert_eq!(
            sender.snapshot(),
            TcpSendSnapshot {
                peer_acked: ByteOffset::new(3),
                next_sent: ByteOffset::new(10),
                retained_bytes: 7,
                retained_backing_bytes: 10,
                segment_count: 2,
                final_offset: None,
                final_accepted: false,
            }
        );
        let replay = sender.replay_view().collect::<Vec<_>>();
        assert_eq!(replay[0].offset(), ByteOffset::new(3));
        assert_eq!(replay[0].end_offset(), ByteOffset::new(6));
        assert_eq!(replay[0].payload(), &data("def"));
        assert_eq!(replay[1].offset(), ByteOffset::new(6));
        assert_eq!(replay[1].payload(), &data("ghij"));

        let boundary = sender.acknowledge(ByteOffset::new(6), false).unwrap();
        assert_eq!(boundary.retained_bytes_released(), 3);
        assert_eq!(boundary.retained_backing_bytes_released(), 6);
        assert_eq!(boundary.compaction_bytes_copied(), 0);
        assert_eq!(boundary.segments_released(), 1);
        assert_eq!(sender.retained_bytes(), 4);
        assert_eq!(sender.retained_backing_bytes(), 4);
        assert_eq!(sender.segment_count(), 1);
    }

    #[test]
    fn sender_checks_offset_and_both_caps_before_ownership() {
        let mut sender = TcpSendWindow::new(limits(5, 1));
        let initial = sender.snapshot();
        assert_eq!(
            sender.append(ByteOffset::new(1), data("a")),
            Err(TcpOwnershipError::UnexpectedSendOffset {
                expected: ByteOffset::new(0),
                actual: ByteOffset::new(1),
            })
        );
        assert_eq!(sender.snapshot(), initial);

        sender.append(ByteOffset::new(0), data("abc")).unwrap();
        let one_segment = sender.snapshot();
        assert_eq!(
            sender.append(ByteOffset::new(3), data("def")),
            Err(TcpOwnershipError::ByteCapacityExceeded {
                attempted: 6,
                max: 5,
            })
        );
        assert_eq!(sender.snapshot(), one_segment);
        assert_eq!(
            sender.append(ByteOffset::new(3), data("d")),
            Err(TcpOwnershipError::SegmentCapacityExceeded {
                attempted: 2,
                max: 1,
            })
        );
        assert_eq!(sender.snapshot(), one_segment);
    }

    #[test]
    fn sender_rejects_unencodable_segment_before_capacity_or_copy() {
        let mut sender = TcpSendWindow::new(limits(MAX_DATA_PAYLOAD_BYTES + 1, 1));
        let oversized = Bytes::from(vec![7; MAX_DATA_PAYLOAD_BYTES + 1]);
        assert_eq!(
            sender.append(ByteOffset::new(0), oversized),
            Err(TcpOwnershipError::DataTooLarge {
                actual: MAX_DATA_PAYLOAD_BYTES + 1,
                max: MAX_DATA_PAYLOAD_BYTES,
            })
        );
        assert_eq!(sender.snapshot().retained_bytes(), 0);
        assert_eq!(sender.snapshot().segment_count(), 0);
    }

    #[test]
    fn sender_rejects_ack_regression_and_ack_beyond_sent_transactionally() {
        let mut sender = TcpSendWindow::new(limits(16, 2));
        sender.append(ByteOffset::new(0), data("abcdef")).unwrap();
        sender.acknowledge(ByteOffset::new(2), false).unwrap();
        let before = sender.snapshot();

        assert_eq!(
            sender.acknowledge(ByteOffset::new(1), false),
            Err(TcpOwnershipError::AckRegression {
                peer_acked: ByteOffset::new(2),
                attempted: ByteOffset::new(1),
            })
        );
        assert_eq!(sender.snapshot(), before);
        assert_eq!(
            sender.acknowledge(ByteOffset::new(7), false),
            Err(TcpOwnershipError::AckBeyondSent {
                next_sent: ByteOffset::new(6),
                attempted: ByteOffset::new(7),
            })
        );
        assert_eq!(sender.snapshot(), before);
    }

    #[test]
    fn sender_close_and_final_ack_are_exact_monotonic_state() {
        let mut sender = TcpSendWindow::new(limits(16, 2));
        sender.append(ByteOffset::new(0), data("abc")).unwrap();
        assert_eq!(
            sender.close(ByteOffset::new(2)),
            Err(TcpOwnershipError::CloseOffsetMismatch {
                expected: ByteOffset::new(3),
                actual: ByteOffset::new(2),
            })
        );

        let close = sender.close(ByteOffset::new(3)).unwrap();
        assert!(close.newly_recorded());
        assert!(!close.final_accepted());
        assert!(!sender.close(ByteOffset::new(3)).unwrap().newly_recorded());
        assert_eq!(
            sender.append(ByteOffset::new(3), data("d")),
            Err(TcpOwnershipError::SendAfterClose {
                final_offset: ByteOffset::new(3),
            })
        );
        assert_eq!(
            sender.acknowledge(ByteOffset::new(2), true),
            Err(TcpOwnershipError::FinalAckOffsetMismatch {
                expected: ByteOffset::new(3),
                actual: ByteOffset::new(2),
            })
        );

        let accepted = sender.acknowledge(ByteOffset::new(3), true).unwrap();
        assert_eq!(accepted.retained_bytes_released(), 3);
        assert_eq!(accepted.segments_released(), 1);
        assert!(accepted.final_accepted_transition());
        assert!(sender.final_accepted());
        assert!(
            !sender
                .acknowledge(ByteOffset::new(3), true)
                .unwrap()
                .final_accepted_transition()
        );
        assert!(
            !sender
                .acknowledge(ByteOffset::new(3), false)
                .unwrap()
                .final_accepted_transition()
        );
        assert!(sender.final_accepted());
    }

    #[test]
    fn sender_final_ack_requires_close() {
        let mut sender = TcpSendWindow::new(limits(8, 1));
        assert_eq!(
            sender.acknowledge(ByteOffset::new(0), true),
            Err(TcpOwnershipError::FinalAckBeforeClose)
        );
    }

    #[test]
    fn sender_normalizes_new_and_tiny_retained_backing() {
        let mut sender = TcpSendWindow::new(limits(32, 1));
        let input = Bytes::from(vec![9; 16]);
        let input_pointer = input.as_ptr();
        sender.append(ByteOffset::new(0), input.clone()).unwrap();
        let before = sender.replay_view().next().unwrap().payload().clone();
        assert_ne!(before.as_ptr(), input_pointer);

        sender.acknowledge(ByteOffset::new(15), false).unwrap();
        let after = sender.replay_view().next().unwrap().payload().clone();
        assert_eq!(after.len(), 1);
        assert_ne!(after.as_ptr(), before.as_ptr().wrapping_add(15));
    }

    #[test]
    fn sender_repeated_small_acks_compact_against_the_original_backing() {
        let mut sender = TcpSendWindow::new(limits(32, 1));
        let append = sender
            .append(ByteOffset::new(0), Bytes::from(vec![9; 16]))
            .unwrap();
        assert_eq!(append.retained_backing_bytes_added(), 16);
        assert_eq!(append.ownership_bytes_copied(), 16);
        let original = sender.replay_view().next().unwrap().payload().clone();

        let mut compaction_bytes_copied = 0;
        for accepted in 1..=15 {
            let delta = sender
                .acknowledge(ByteOffset::new(accepted), false)
                .unwrap();
            compaction_bytes_copied += delta.compaction_bytes_copied();
            let snapshot = sender.snapshot();
            assert_eq!(snapshot.retained_bytes(), 16 - accepted as usize);
            assert!(snapshot.retained_backing_bytes() < 4 * snapshot.retained_bytes());
        }

        let retained = sender.replay_view().next().unwrap().payload().clone();
        assert_eq!(retained.len(), 1);
        assert_ne!(retained.as_ptr(), original.as_ptr().wrapping_add(15));
        assert_eq!(sender.snapshot().retained_backing_bytes(), 1);
        assert_eq!(compaction_bytes_copied, 5);
        assert!(compaction_bytes_copied < 16);
    }

    #[test]
    fn receiver_buffers_gap_without_ack_then_accepts_only_sink_progress() {
        let mut receiver = TcpReceiveWindow::new(limits(32, 4));
        let gap = receiver.receive(ByteOffset::new(4), data("ef")).unwrap();
        assert_eq!(gap.disposition(), TcpReceiveDisposition::Buffered);
        assert_eq!(gap.buffered_bytes_added(), 2);
        assert_eq!(gap.ranges_added(), 1);
        assert_eq!(receiver.accepted(), ByteOffset::new(0));
        assert_eq!(
            receiver.snapshot().contiguous_received(),
            ByteOffset::new(0)
        );
        assert!(receiver.peek_contiguous(32).is_empty());

        let prefix = receiver.receive(ByteOffset::new(0), data("abcd")).unwrap();
        assert_eq!(prefix.buffered_bytes_added(), 4);
        assert_eq!(prefix.ranges_added(), 1);
        assert_eq!(receiver.accepted(), ByteOffset::new(0));
        assert_eq!(
            receiver.snapshot().contiguous_received(),
            ByteOffset::new(6)
        );
        let peek = receiver.peek_contiguous(5);
        assert_eq!(peek.len(), 2);
        assert_eq!(peek[0].payload(), &data("abcd"));
        assert_eq!(peek[1].payload(), &data("e"));

        let first_accept = receiver.accept(3).unwrap();
        assert_eq!(first_accept.accepted_bytes(), 3);
        assert_eq!(first_accept.application_ack(), Some(ByteOffset::new(3)));
        assert_eq!(first_accept.buffered_backing_bytes_released(), 3);
        assert_eq!(first_accept.compaction_bytes_copied(), 1);
        assert_eq!(first_accept.ranges_released(), 0);
        assert_eq!(receiver.buffered_bytes(), 3);
        assert_eq!(receiver.buffered_backing_bytes(), 3);
        assert_eq!(
            receiver
                .peek_contiguous(32)
                .into_iter()
                .map(|segment| segment.payload().clone())
                .collect::<Vec<_>>(),
            vec![data("d"), data("ef")]
        );

        let rest = receiver.accept(99).unwrap();
        assert_eq!(rest.accepted_bytes(), 3);
        assert_eq!(rest.application_ack(), Some(ByteOffset::new(6)));
        assert_eq!(rest.ranges_released(), 2);
        assert_eq!(receiver.buffered_bytes(), 0);
        assert_eq!(receiver.range_count(), 0);
        assert_eq!(receiver.accept(0).unwrap().application_ack(), None);
        assert_eq!(receiver.accept(4).unwrap().application_ack(), None);
    }

    #[test]
    fn receiver_reservation_previews_exact_delta_without_taking_ownership() {
        let mut receiver = TcpReceiveWindow::new(limits(32, 4));
        receiver.receive(ByteOffset::new(2), data("cd")).unwrap();
        let before = receiver.snapshot();
        let payload = data("abcdef");

        let reservation = receiver
            .preview_receive(ByteOffset::new(0), &payload)
            .unwrap();

        assert_eq!(reservation.original_offset, ByteOffset::new(0));
        assert_eq!(reservation.end_offset, ByteOffset::new(6));
        assert_eq!(reservation.buffered_bytes_added(), 4);
        assert_eq!(reservation.ranges_added(), 2);
        assert_eq!(receiver.snapshot(), before);

        let received = receiver
            .commit_receive(reservation, ByteOffset::new(0), payload)
            .unwrap();
        assert_eq!(received.disposition(), TcpReceiveDisposition::Buffered);
        assert_eq!(received.buffered_bytes_added(), 4);
        assert_eq!(received.ranges_added(), 2);
        assert_eq!(receiver.buffered_bytes(), 6);
        assert_eq!(receiver.range_count(), 3);
    }

    #[test]
    fn receiver_rejects_stale_reservation_without_mutation() {
        let mut receiver = TcpReceiveWindow::new(limits(32, 4));
        let payload = data("ab");
        let reservation = receiver
            .preview_receive(ByteOffset::new(0), &payload)
            .unwrap();
        receiver.receive(ByteOffset::new(2), data("cd")).unwrap();
        let before = receiver.snapshot();

        assert!(matches!(
            receiver.commit_receive(reservation, ByteOffset::new(0), payload),
            Err(TcpOwnershipError::StaleReceiveReservation { .. })
        ));
        assert_eq!(receiver.snapshot(), before);
    }

    #[test]
    fn receiver_reservation_rejects_altered_payload_or_extent_without_mutation() {
        let mut receiver = TcpReceiveWindow::new(limits(32, 4));
        let original = data("abcd");
        let payload_reservation = receiver
            .preview_receive(ByteOffset::new(2), &original)
            .unwrap();
        let before = receiver.snapshot();
        assert_eq!(
            receiver.commit_receive(payload_reservation, ByteOffset::new(2), data("abXd"),),
            Err(TcpOwnershipError::ReceiveReservationPayloadMismatch)
        );
        assert_eq!(receiver.snapshot(), before);

        let extent_reservation = receiver
            .preview_receive(ByteOffset::new(2), &original)
            .unwrap();
        assert!(matches!(
            receiver.commit_receive(extent_reservation, ByteOffset::new(3), original),
            Err(TcpOwnershipError::ReceiveReservationExtentMismatch { .. })
        ));
        assert_eq!(receiver.snapshot(), before);
    }

    #[test]
    fn receiver_accept_and_close_invalidate_prior_reservations() {
        let mut accepted = TcpReceiveWindow::new(limits(32, 4));
        accepted.receive(ByteOffset::new(0), data("ab")).unwrap();
        let payload = data("cd");
        let reservation = accepted
            .preview_receive(ByteOffset::new(2), &payload)
            .unwrap();
        accepted.accept(1).unwrap();
        let before = accepted.snapshot();
        assert!(matches!(
            accepted.commit_receive(reservation, ByteOffset::new(2), payload),
            Err(TcpOwnershipError::StaleReceiveReservation { .. })
        ));
        assert_eq!(accepted.snapshot(), before);

        let mut closed = TcpReceiveWindow::new(limits(32, 4));
        let payload = data("ab");
        let reservation = closed
            .preview_receive(ByteOffset::new(0), &payload)
            .unwrap();
        closed.receive_close(ByteOffset::new(2)).unwrap();
        let before = closed.snapshot();
        assert!(matches!(
            closed.commit_receive(reservation, ByteOffset::new(0), payload),
            Err(TcpOwnershipError::StaleReceiveReservation { .. })
        ));
        assert_eq!(closed.snapshot(), before);
    }

    #[test]
    fn receiver_rejects_reservation_from_different_equal_count_window() {
        let mut source = TcpReceiveWindow::new(limits(32, 4));
        source.receive(ByteOffset::new(2), data("cd")).unwrap();
        let payload = data("abcd");
        let reservation = source
            .preview_receive(ByteOffset::new(0), &payload)
            .unwrap();

        let mut destination = TcpReceiveWindow::new(limits(32, 4));
        destination.receive(ByteOffset::new(1), data("bc")).unwrap();
        let before = destination.snapshot();
        assert!(matches!(
            destination.commit_receive(reservation, ByteOffset::new(0), payload),
            Err(TcpOwnershipError::StaleReceiveReservation { .. })
        ));
        assert_eq!(destination.snapshot(), before);
    }

    #[test]
    fn receiver_identical_overlap_is_idempotent_and_conflict_is_transactional() {
        let mut receiver = TcpReceiveWindow::new(limits(32, 4));
        receiver
            .receive(ByteOffset::new(0), data("abcdef"))
            .unwrap();
        let before_duplicate = receiver.snapshot();
        let duplicate = receiver.receive(ByteOffset::new(1), data("bcd")).unwrap();
        assert_eq!(duplicate.disposition(), TcpReceiveDisposition::Duplicate);
        assert_eq!(duplicate.buffered_bytes_added(), 0);
        assert_eq!(duplicate.ranges_added(), 0);
        assert_eq!(receiver.snapshot(), before_duplicate);

        assert_eq!(
            receiver.receive(ByteOffset::new(2), data("cX")),
            Err(TcpOwnershipError::ConflictingOverlap {
                offset: ByteOffset::new(3),
            })
        );
        assert_eq!(receiver.snapshot(), before_duplicate);
    }

    #[test]
    fn receiver_bridge_adds_only_uncovered_bytes_and_exact_ranges() {
        let mut receiver = TcpReceiveWindow::new(limits(32, 4));
        receiver.receive(ByteOffset::new(2), data("cd")).unwrap();
        let bridge = receiver
            .receive(ByteOffset::new(0), data("abcdef"))
            .unwrap();
        assert_eq!(bridge.disposition(), TcpReceiveDisposition::Buffered);
        assert_eq!(bridge.buffered_bytes_added(), 4);
        assert_eq!(bridge.ranges_added(), 2);
        assert_eq!(receiver.buffered_bytes(), 6);
        assert_eq!(receiver.range_count(), 3);
        let payload = receiver
            .peek_contiguous(32)
            .into_iter()
            .flat_map(|segment| segment.payload().to_vec())
            .collect::<Vec<_>>();
        assert_eq!(payload, b"abcdef");
    }

    #[test]
    fn receiver_stale_authority_boundary_ignores_accepted_prefix() {
        let mut receiver = TcpReceiveWindow::new(limits(32, 4));
        receiver
            .receive(ByteOffset::new(0), data("abcdef"))
            .unwrap();
        receiver.accept(3).unwrap();

        let stale = receiver.receive(ByteOffset::new(0), data("XXX")).unwrap();
        assert_eq!(stale.disposition(), TcpReceiveDisposition::StaleDuplicate);
        assert_eq!(receiver.accepted(), ByteOffset::new(3));

        let partial_stale = receiver.receive(ByteOffset::new(1), data("XXde")).unwrap();
        assert_eq!(
            partial_stale.disposition(),
            TcpReceiveDisposition::Duplicate
        );
        assert_eq!(partial_stale.buffered_bytes_added(), 0);
    }

    #[test]
    fn receiver_caps_and_window_fail_before_state_changes() {
        let mut receiver = TcpReceiveWindow::new(limits(5, 2));
        assert_eq!(
            receiver.receive(ByteOffset::new(5), data("x")),
            Err(TcpOwnershipError::ReceiveWindowExceeded {
                accepted: ByteOffset::new(0),
                attempted_end: ByteOffset::new(6),
                max_end: ByteOffset::new(5),
            })
        );
        assert_eq!(receiver.snapshot().buffered_bytes(), 0);

        receiver.receive(ByteOffset::new(4), data("e")).unwrap();
        receiver.receive(ByteOffset::new(0), data("a")).unwrap();
        let before_third_range = receiver.snapshot();
        assert_eq!(
            receiver.receive(ByteOffset::new(2), data("c")),
            Err(TcpOwnershipError::SegmentCapacityExceeded {
                attempted: 3,
                max: 2,
            })
        );
        assert_eq!(receiver.snapshot(), before_third_range);
    }

    #[test]
    fn receiver_early_close_becomes_ready_only_at_exact_final_acceptance() {
        let mut receiver = TcpReceiveWindow::new(limits(16, 4));
        receiver.receive(ByteOffset::new(2), data("cd")).unwrap();
        let close = receiver.receive_close(ByteOffset::new(4)).unwrap();
        assert!(close.newly_recorded());
        assert!(!close.half_close_ready());
        assert!(!close.half_close_ready_transition());
        assert!(!receiver.half_close_ready());
        assert!(
            !receiver
                .receive_close(ByteOffset::new(4))
                .unwrap()
                .newly_recorded()
        );
        assert_eq!(
            receiver.receive_close(ByteOffset::new(5)),
            Err(TcpOwnershipError::ConflictingFinalOffset {
                recorded: ByteOffset::new(4),
                attempted: ByteOffset::new(5),
            })
        );
        assert_eq!(
            receiver.receive(ByteOffset::new(4), data("e")),
            Err(TcpOwnershipError::DataBeyondFinalOffset {
                final_offset: ByteOffset::new(4),
                attempted_end: ByteOffset::new(5),
            })
        );

        let data_result = receiver.receive(ByteOffset::new(0), data("ab")).unwrap();
        assert_eq!(data_result.buffered_bytes_added(), 2);
        assert_eq!(receiver.accepted(), ByteOffset::new(0));
        assert!(!receiver.half_close_ready());
        let accepted = receiver.accept(4).unwrap();
        assert_eq!(accepted.application_ack(), Some(ByteOffset::new(4)));
        assert!(accepted.half_close_ready_transition());
        assert!(receiver.half_close_ready());
    }

    #[test]
    fn receiver_rejects_close_behind_accepted_or_buffered_data() {
        let mut accepted = TcpReceiveWindow::new(limits(16, 2));
        accepted.receive(ByteOffset::new(0), data("ab")).unwrap();
        accepted.accept(2).unwrap();
        assert_eq!(
            accepted.receive_close(ByteOffset::new(1)),
            Err(TcpOwnershipError::CloseBeforeAccepted {
                accepted: ByteOffset::new(2),
                attempted: ByteOffset::new(1),
            })
        );

        let mut buffered = TcpReceiveWindow::new(limits(16, 2));
        buffered.receive(ByteOffset::new(3), data("de")).unwrap();
        assert_eq!(
            buffered.receive_close(ByteOffset::new(4)),
            Err(TcpOwnershipError::CloseBeforeBufferedEnd {
                highest_received: ByteOffset::new(5),
                attempted: ByteOffset::new(4),
            })
        );
    }

    #[test]
    fn abandon_releases_without_ack_and_is_terminal() {
        let mut receiver = TcpReceiveWindow::new(limits(16, 2));
        receiver.receive(ByteOffset::new(0), data("abc")).unwrap();
        let abandoned = receiver.abandon();
        assert_eq!(abandoned.accepted(), ByteOffset::new(0));
        assert_eq!(abandoned.buffered_bytes_dropped(), 3);
        assert_eq!(abandoned.buffered_backing_bytes_dropped(), 3);
        assert_eq!(abandoned.ranges_dropped(), 1);
        assert!(abandoned.newly_abandoned());
        assert_eq!(receiver.accepted(), ByteOffset::new(0));
        assert_eq!(receiver.buffered_bytes(), 0);
        assert_eq!(receiver.buffered_backing_bytes(), 0);
        assert!(receiver.is_abandoned());
        assert_eq!(
            receiver.receive(ByteOffset::new(0), data("abc")),
            Err(TcpOwnershipError::ReceiveAfterAbandon)
        );
        assert_eq!(
            receiver.accept(1),
            Err(TcpOwnershipError::AcceptAfterAbandon)
        );
        assert_eq!(
            receiver.receive_close(ByteOffset::new(0)),
            Err(TcpOwnershipError::CloseAfterAbandon)
        );
        let duplicate = receiver.abandon();
        assert!(!duplicate.newly_abandoned());
        assert_eq!(duplicate.buffered_bytes_dropped(), 0);
        assert_eq!(duplicate.ranges_dropped(), 0);
    }

    #[test]
    fn receiver_rejects_oversized_and_overflowing_data_transactionally() {
        let mut receiver = TcpReceiveWindow::new(limits(MAX_DATA_PAYLOAD_BYTES + 1, 2));
        let oversized = Bytes::from(vec![1; MAX_DATA_PAYLOAD_BYTES + 1]);
        assert_eq!(
            receiver.receive(ByteOffset::new(0), oversized),
            Err(TcpOwnershipError::DataTooLarge {
                actual: MAX_DATA_PAYLOAD_BYTES + 1,
                max: MAX_DATA_PAYLOAD_BYTES,
            })
        );
        assert_eq!(receiver.snapshot().buffered_bytes(), 0);
        assert_eq!(
            receiver.receive(ByteOffset::new(u64::MAX), data("x")),
            Err(TcpOwnershipError::OffsetOverflow {
                offset: ByteOffset::new(u64::MAX),
                bytes: 1,
            })
        );
        assert_eq!(receiver.snapshot().buffered_bytes(), 0);
    }

    #[test]
    fn receiver_normalizes_new_and_tiny_retained_backing() {
        let mut receiver = TcpReceiveWindow::new(limits(32, 2));
        let input = Bytes::from(vec![5; 16]);
        let input_pointer = input.as_ptr();
        receiver.receive(ByteOffset::new(0), input.clone()).unwrap();
        let before = receiver.peek_contiguous(16)[0].payload().clone();
        assert_ne!(before.as_ptr(), input_pointer);

        receiver.accept(15).unwrap();
        let after = receiver.peek_contiguous(16)[0].payload().clone();
        assert_eq!(after.len(), 1);
        assert_ne!(after.as_ptr(), before.as_ptr().wrapping_add(15));
    }

    #[test]
    fn receiver_repeated_small_accepts_compact_against_the_original_backing() {
        let mut receiver = TcpReceiveWindow::new(limits(32, 1));
        let received = receiver
            .receive(ByteOffset::new(0), Bytes::from(vec![5; 16]))
            .unwrap();
        assert_eq!(received.buffered_backing_bytes_added(), 16);
        assert_eq!(received.ownership_bytes_copied(), 16);
        let original = receiver.peek_contiguous(16)[0].payload().clone();
        let repeated_view = receiver.peek_contiguous(16);
        assert_eq!(repeated_view[0].payload().as_ptr(), original.as_ptr());

        let mut compaction_bytes_copied = 0;
        for _ in 0..15 {
            let delta = receiver.accept(1).unwrap();
            compaction_bytes_copied += delta.compaction_bytes_copied();
            let snapshot = receiver.snapshot();
            assert!(snapshot.buffered_backing_bytes() < 4 * snapshot.buffered_bytes());
        }

        let retained = receiver.peek_contiguous(16)[0].payload().clone();
        assert_eq!(retained.len(), 1);
        assert_ne!(retained.as_ptr(), original.as_ptr().wrapping_add(15));
        assert_eq!(receiver.snapshot().buffered_backing_bytes(), 1);
        assert_eq!(compaction_bytes_copied, 5);
        assert!(compaction_bytes_copied < 16);
    }

    #[test]
    fn data_segment_debug_redacts_payload_bytes() {
        let mut sender = TcpSendWindow::new(limits(64, 1));
        sender
            .append(ByteOffset::new(0), data("secret-payload-sentinel"))
            .unwrap();

        let debug = format!("{:?}", sender.replay_view().next().unwrap());
        assert!(debug.contains("payload_len"));
        assert!(debug.contains("backing_bytes"));
        assert!(!debug.contains("secret-payload-sentinel"));
    }

    #[test]
    fn receive_reservation_debug_redacts_payload_bytes() {
        let receiver = TcpReceiveWindow::new(limits(64, 1));
        let payload = data("secret-reservation-sentinel");
        let reservation = receiver
            .preview_receive(ByteOffset::new(0), &payload)
            .unwrap();

        let debug = format!("{reservation:?}");
        assert!(debug.contains("payload_len"));
        assert!(!debug.contains("secret-reservation-sentinel"));
    }
}
