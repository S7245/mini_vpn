//! Original receive ownership transactions, local to the TCP window.
//! Owns mutation, exact byte/backing accounting and protocol dispositions.
//! No transport, scheduler, Session policy or second resource owner.
//! Entry: existing window methods; characterization: parent tests Module.

use super::receive_storage::{FreeNodes, storage_error};
use super::*;
use crate::resumable::effect_storage::allocate_exact_vec;

struct ReceiveShape {
    reservation: TcpReceiveReservation,
    overlaps: usize,
}

fn receive_vec<T>(count: usize) -> Result<Vec<T>, TcpOwnershipError> {
    std::alloc::Layout::array::<T>(count).map_err(|_| TcpOwnershipError::ReceiveStorageLayout)?;
    #[cfg(test)]
    if count > 0 && FAIL_RESERVATION.replace(false) {
        return Err(TcpOwnershipError::ReceiveStorageReserve);
    }
    allocate_exact_vec(count).map_err(storage_error)
}

/// Borrowed projection over the same existing and prepared normalized ranges.
/// Exact lookups preserve the former O(log R) per contiguous range; no heap stack.
struct ReceiveProjection<'a> {
    window: &'a TcpReceiveWindow,
    added: &'a [(u64, OwnedPayload)],
    partial: Option<&'a OwnedPayload>,
    start: u64,
}
impl ReceiveProjection<'_> {
    fn pieces(&self, max: usize) -> impl Iterator<Item = (u64, &OwnedPayload, usize)> {
        let mut cursor = self.start;
        let mut remaining = if self.window.abandoned { 0 } else { max };
        std::iter::from_fn(move || {
            if remaining == 0 {
                return None;
            }
            let payload = if cursor == self.start && self.partial.is_some() {
                self.partial
            } else {
                self.added
                    .binary_search_by_key(&cursor, |(k, _)| *k)
                    .ok()
                    .map(|i| &self.added[i].1)
                    .or_else(|| self.window.ranges.get(&cursor))
            }?;
            let take = remaining.min(payload.len());
            let start = cursor;
            cursor = checked_end_raw(cursor, take).ok()?;
            remaining -= take;
            Some((start, payload, take))
        })
    }
    fn prepare(&self, max: usize) -> Result<Vec<TcpDataSegment>, TcpOwnershipError> {
        let count = self.pieces(max).count();
        std::alloc::Layout::array::<TcpDataSegment>(count)
            .map_err(|_| TcpOwnershipError::ReceiveStorageLayout)?;
        #[cfg(test)]
        if count > 0 && FAIL_OFFER.replace(false) {
            return Err(TcpOwnershipError::ReceiveStorageReserve);
        }
        let mut segments = allocate_exact_vec(count).map_err(storage_error)?;
        for (start, payload, take) in self.pieces(max) {
            segments.push(TcpDataSegment {
                offset: ByteOffset::new(start),
                end_offset: ByteOffset::new(start + take as u64),
                payload: payload.slice(0..take),
            });
        }
        Ok(segments)
    }
}

/// Retains both possible custom input owners until the calling Session's final
/// global/pending-Offer/effect publication. Public TCP callers finish locally.
pub(in crate::resumable) struct CommittedTcpReceiveData {
    result: TcpReceiveData,
    _original_payload: Bytes,
    _reservation_payload: Bytes,
}
impl CommittedTcpReceiveData {
    pub(in crate::resumable) fn finish(self) -> TcpReceiveData {
        self.result
    }
}

impl TcpReceiveWindow {
    #[cfg(test)]
    pub(in crate::resumable) fn fail_next_receive_offer_allocation_for_test() {
        FAIL_OFFER.set(true);
    }
    #[cfg(test)]
    pub(in crate::resumable) fn fail_next_receive_reservation_allocation_for_test() {
        FAIL_RESERVATION.set(true);
    }
    /// Returns shared normalized slices. This compatibility API keeps its
    /// original allocation-abort contract; production Session uses preparation.
    pub fn peek_contiguous(&self, max_bytes: usize) -> Vec<TcpDataSegment> {
        let projection = ReceiveProjection {
            window: self,
            added: &[],
            partial: None,
            start: self.accepted.get(),
        };
        let mut segments = Vec::with_capacity(projection.pieces(max_bytes).count());
        for (start, payload, take) in projection.pieces(max_bytes) {
            segments.push(TcpDataSegment {
                offset: ByteOffset::new(start),
                end_offset: ByteOffset::new(start + take as u64),
                payload: payload.slice(0..take),
            });
        }
        segments
    }

    pub(in crate::resumable) fn contiguous_len(&self, max: usize) -> usize {
        ReceiveProjection {
            window: self,
            added: &[],
            partial: None,
            start: self.accepted.get(),
        }
        .pieces(max)
        .map(|(_, _, take)| take)
        .sum()
    }

    pub(in crate::resumable) fn prepare_contiguous_offer(
        &self,
        max: usize,
    ) -> Result<Vec<TcpDataSegment>, TcpOwnershipError> {
        ReceiveProjection {
            window: self,
            added: &[],
            partial: None,
            start: self.accepted.get(),
        }
        .prepare(max)
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
        let ReceiveShape {
            mut reservation,
            overlaps,
        } = self.receive_shape(offset, payload)?;
        // In Session this materialization follows global budget and ID checks.
        // A recoverable array Reserve must not hide the original epoch failure.
        if reservation.ranges_added() > 0 {
            self.epoch
                .checked_add(1)
                .ok_or(TcpOwnershipError::ReceiveEpochExhausted)?;
        }
        reservation.overlapping_ranges = receive_vec(overlaps)?;
        reservation.uncovered = receive_vec(reservation.ranges_added())?;
        if reservation.end_offset > reservation.accepted {
            let start = offset.get().max(self.accepted.get());
            let end = reservation.end_offset.get();
            for (&key, owned) in self.ranges.range(..end) {
                if checked_end_raw(key, owned.len())? > start {
                    reservation
                        .overlapping_ranges
                        .push((key, owned.bytes.clone()));
                }
            }
            for range in self.uncovered_ranges(start, end) {
                reservation.uncovered.push(range);
            }
        }
        Ok(reservation)
    }

    /// Same logical validation, without materializing reservation arrays. Session
    /// checks its original global budget/IDs before any new recoverable reserve.
    pub(in crate::resumable) fn preview_receive_delta(
        &self,
        offset: ByteOffset,
        payload: &Bytes,
    ) -> Result<(usize, usize), TcpOwnershipError> {
        let shape = self.receive_shape(offset, payload)?;
        Ok((
            shape.reservation.buffered_bytes_added(),
            shape.reservation.ranges_added(),
        ))
    }

    fn receive_shape(
        &self,
        offset: ByteOffset,
        payload: &Bytes,
    ) -> Result<ReceiveShape, TcpOwnershipError> {
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
            return Ok(ReceiveShape {
                overlaps: 0,
                reservation: TcpReceiveReservation {
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
                },
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
        let mut overlaps = 0usize;
        for (&range_start, range_payload) in self.ranges.range(..live_end) {
            let range_end = checked_end_raw(range_start, range_payload.len())?;
            if range_end <= live_start {
                continue;
            }
            overlaps += 1;
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

        let mut ranges_added = 0usize;
        let mut added_bytes = 0usize;
        for (start, end) in self.uncovered_ranges(live_start, live_end) {
            ranges_added += 1;
            added_bytes = added_bytes
                .checked_add(offset_distance_raw(start, end)?)
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
        Ok(ReceiveShape {
            overlaps,
            reservation: TcpReceiveReservation {
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
                buffered_bytes_after: buffered_after,
                buffered_backing_bytes_after: buffered_backing_after,
                ranges_after,
            },
        })
    }

    fn uncovered_ranges(&self, start: u64, end: u64) -> impl Iterator<Item = (u64, u64)> {
        let mut ranges = self.ranges.range(..end);
        let mut cursor = start;
        std::iter::from_fn(move || {
            if cursor >= end {
                return None;
            }
            for (&key, payload) in ranges.by_ref() {
                // All existing ranges passed checked_end_raw at admission.
                let next = key + payload.len() as u64;
                if next <= cursor {
                    continue;
                }
                let gap = (key > cursor).then_some((cursor, key.min(end)));
                cursor = cursor.max(next.min(end));
                if gap.is_some() {
                    return gap;
                }
                if cursor == end {
                    return None;
                }
            }
            let tail = (cursor < end).then_some((cursor, end));
            cursor = end;
            tail
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
        Ok(self
            .prepare_receive_commit(reservation, offset, payload)?
            .commit()
            .finish())
    }

    pub(in crate::resumable) fn prepare_receive_commit(
        &mut self,
        reservation: TcpReceiveReservation,
        offset: ByteOffset,
        payload: Bytes,
    ) -> Result<PreparedTcpReceiveData<'_>, TcpOwnershipError> {
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

        let next_epoch = if reservation.uncovered.is_empty() {
            self.epoch
        } else {
            self.epoch
                .checked_add(1)
                .ok_or(TcpOwnershipError::ReceiveEpochExhausted)?
        };
        for &(start, end) in &reservation.uncovered {
            offset_distance_raw(offset.get(), start)?;
            offset_distance_raw(start, end)?;
        }
        let mut owned_ranges = receive_vec(reservation.uncovered.len())?;
        for &(start, end) in &reservation.uncovered {
            let payload_start = offset_distance_raw(offset.get(), start)?;
            let payload_len = offset_distance_raw(start, end)?;
            let owned =
                OwnedPayload::new(&payload.slice(payload_start..payload_start + payload_len));
            debug_assert_eq!(owned.backing_bytes, payload_len);
            owned_ranges.push((start, owned));
        }
        let nodes = self
            .ranges
            .prepare(owned_ranges.len(), self.limits.max_segments)?;
        Ok(PreparedTcpReceiveData {
            window: self,
            reservation,
            owned_ranges,
            next_epoch,
            original_payload: payload,
            nodes,
        })
    }

    /// Transfers up to `max_bytes` of the contiguous prefix to the application
    /// sink. The caller must pass only bytes its sink actually accepted.
    pub fn accept(&mut self, max_bytes: usize) -> Result<TcpReceiveAccept, TcpOwnershipError> {
        Ok(self.prepare_accept(max_bytes)?.commit())
    }

    pub(in crate::resumable) fn prepare_accept(
        &mut self,
        max_bytes: usize,
    ) -> Result<PreparedTcpReceiveAccept<'_>, TcpOwnershipError> {
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
        let mut cursor = previous_accepted;
        let mut partial = None;

        while remaining > 0 {
            let key = cursor.get();
            let Some(payload) = self.ranges.get(&key) else {
                break;
            };
            let take = remaining.min(payload.len());
            let next = checked_advance(cursor, take)?;
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
                if self.ranges.contains_key(&next.get()) {
                    return Err(TcpOwnershipError::InvariantViolation(
                        "receiver partial accept collided with a range start",
                    ));
                }
                partial = Some(suffix);
            }
            cursor = next;
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

        let buffered_bytes = self.buffered_bytes.checked_sub(accepted_bytes).ok_or(
            TcpOwnershipError::InvariantViolation("receiver buffered byte accounting underflow"),
        )?;
        let buffered_backing_bytes = self
            .buffered_backing_bytes
            .checked_sub(released_backing_bytes)
            .ok_or(TcpOwnershipError::InvariantViolation(
                "receiver buffered backing byte accounting underflow",
            ))?;
        let next_epoch = next_epoch.unwrap_or(self.epoch);
        let half_close_ready = !self.abandoned && self.final_offset == Some(cursor);
        debug_assert_eq!(cursor, preview.accepted);
        debug_assert_eq!(accepted_bytes, preview.accepted_bytes);
        debug_assert_eq!(ranges_released, preview.ranges_released);

        let result = TcpReceiveAccept {
            previous_accepted,
            accepted: cursor,
            accepted_bytes,
            buffered_backing_bytes_released: released_backing_bytes,
            compaction_bytes_copied,
            ranges_released,
            half_close_ready_transition: !was_half_close_ready && half_close_ready,
        };
        Ok(PreparedTcpReceiveAccept {
            window: self,
            partial,
            buffered_bytes,
            buffered_backing_bytes,
            next_epoch,
            result,
        })
    }

    /// Records an early or on-time CLOSE. The final offset may be ahead of a
    /// gap, but never behind accepted or already-buffered bytes.
    pub fn receive_close(
        &mut self,
        final_offset: ByteOffset,
    ) -> Result<TcpReceiveClose, TcpOwnershipError> {
        Ok(self.prepare_receive_close(final_offset)?.commit())
    }

    pub(in crate::resumable) fn prepare_receive_close(
        &mut self,
        final_offset: ByteOffset,
    ) -> Result<PreparedTcpReceiveClose<'_>, TcpOwnershipError> {
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
            let result = TcpReceiveClose {
                final_offset,
                newly_recorded: false,
                half_close_ready: self.half_close_ready(),
                half_close_ready_transition: false,
            };
            return Ok(PreparedTcpReceiveClose {
                next_epoch: self.epoch,
                window: self,
                result,
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
        let half_close_ready = !self.abandoned && final_offset == self.accepted;
        let result = TcpReceiveClose {
            final_offset,
            newly_recorded: true,
            half_close_ready,
            half_close_ready_transition: half_close_ready,
        };
        Ok(PreparedTcpReceiveClose {
            window: self,
            next_epoch,
            result,
        })
    }
}

#[cfg(test)]
std::thread_local! {
    static FAIL_OFFER: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static FAIL_RESERVATION: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Exact reservation and normalized uncovered ranges, before any insertion.
pub(in crate::resumable) struct PreparedTcpReceiveData<'window> {
    window: &'window mut TcpReceiveWindow,
    reservation: TcpReceiveReservation,
    owned_ranges: Vec<(u64, OwnedPayload)>,
    next_epoch: u64,
    original_payload: Bytes,
    nodes: FreeNodes,
}

impl PreparedTcpReceiveData<'_> {
    pub(in crate::resumable) fn prepare_offer(
        &self,
        max: usize,
    ) -> Result<Vec<TcpDataSegment>, TcpOwnershipError> {
        ReceiveProjection {
            window: self.window,
            added: &self.owned_ranges,
            partial: None,
            start: self.window.accepted.get(),
        }
        .prepare(max)
    }
    pub(in crate::resumable) fn commit(mut self) -> CommittedTcpReceiveData {
        for (start, owned) in self.owned_ranges {
            self.window
                .ranges
                .insert_prepared(&mut self.nodes, start, owned);
        }
        self.window.buffered_bytes = self.reservation.buffered_bytes_after;
        self.window.buffered_backing_bytes = self.reservation.buffered_backing_bytes_after;
        self.window.epoch = self.next_epoch;
        debug_assert!(self.window.backing_is_bounded());
        // Equal reservation/commit bytes may have distinct custom owners.
        // Neither original input disposal precedes normalized publication.
        let result = TcpReceiveData {
            disposition: self.reservation.disposition(),
            original_offset: self.reservation.original_offset,
            end_offset: self.reservation.end_offset,
            buffered_bytes_added: self.reservation.buffered_bytes_added(),
            buffered_backing_bytes_added: self.reservation.buffered_backing_bytes_added(),
            ownership_bytes_copied: self.reservation.buffered_bytes_added(),
            ranges_added: self.reservation.ranges_added(),
        };
        CommittedTcpReceiveData {
            result,
            _original_payload: self.original_payload,
            _reservation_payload: self.reservation.payload,
        }
    }
}

/// The complete contiguous prefix and possible partial suffix are validated
/// without removing an original range. No key list or second range map.
pub(in crate::resumable) struct PreparedTcpReceiveAccept<'window> {
    window: &'window mut TcpReceiveWindow,
    partial: Option<OwnedPayload>,
    buffered_bytes: usize,
    buffered_backing_bytes: usize,
    next_epoch: u64,
    result: TcpReceiveAccept,
}

impl PreparedTcpReceiveAccept<'_> {
    pub(in crate::resumable) fn prepare_offer(
        &self,
        max: usize,
    ) -> Result<Vec<TcpDataSegment>, TcpOwnershipError> {
        ReceiveProjection {
            window: self.window,
            added: &[],
            partial: self.partial.as_ref(),
            start: self.result.accepted.get(),
        }
        .prepare(max)
    }
    pub(in crate::resumable) fn outcome(&self) -> TcpReceiveAccept {
        self.result
    }

    pub(in crate::resumable) fn commit(self) -> TcpReceiveAccept {
        // Only the proven contiguous prefix is below this exclusive end;
        // preparation holds the same map's mutable borrow throughout.
        self.window
            .ranges
            .accept_prefix(self.result.accepted.get(), self.partial);
        self.window.accepted = self.result.accepted;
        self.window.buffered_bytes = self.buffered_bytes;
        self.window.buffered_backing_bytes = self.buffered_backing_bytes;
        self.window.epoch = self.next_epoch;
        debug_assert!(self.window.backing_is_bounded());
        self.result
    }
}

pub(in crate::resumable) struct PreparedTcpReceiveClose<'window> {
    window: &'window mut TcpReceiveWindow,
    next_epoch: u64,
    result: TcpReceiveClose,
}

impl PreparedTcpReceiveClose<'_> {
    pub(in crate::resumable) fn outcome(&self) -> TcpReceiveClose {
        self.result
    }

    pub(in crate::resumable) fn commit(self) -> TcpReceiveClose {
        if self.result.newly_recorded {
            self.window.final_offset = Some(self.result.final_offset);
            self.window.epoch = self.next_epoch;
        }
        self.result
    }
}
