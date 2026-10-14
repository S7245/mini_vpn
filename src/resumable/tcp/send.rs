//! Original send ownership transactions, local to the TCP window.
//! Owns mutation, exact byte/backing accounting and protocol dispositions.
//! No transport, scheduler, Session policy or second resource owner.
//! Entry: existing window methods; characterization: parent tests Module.

use super::*;
use crate::resumable::effect_storage::{
    EffectStorageError, allocate_exact_vec, growth_peak, next_capacity,
};

#[cfg(test)]
mod tests;

#[cfg(test)]
std::thread_local! {
    static FAIL_NEXT_DESCRIPTOR_ALLOCATION: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

impl TcpSendWindow {
    /// Reserves both capacity dimensions before taking ownership of `payload`.
    pub fn append(
        &mut self,
        offset: ByteOffset,
        payload: Bytes,
    ) -> Result<TcpSendAppend, TcpOwnershipError> {
        Ok(self.prepare_append(offset, payload)?.commit())
    }

    pub(in crate::resumable) fn prepare_append(
        &mut self,
        offset: ByteOffset,
        payload: Bytes,
    ) -> Result<PreparedTcpSendAppend<'_>, TcpOwnershipError> {
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
        // OwnedPayload copies exactly this length. Preserve the original pure
        // accounting rejection before any new fallible descriptor allocation.
        let retained_backing_bytes_added = payload.len();
        let retained_backing_after = self
            .retained_backing_bytes
            .checked_add(retained_backing_bytes_added)
            .ok_or(TcpOwnershipError::InvariantViolation(
                "sender retained backing byte accounting overflow",
            ))?;
        let descriptors = PreparedSendDescriptors::prepare(
            &self.segments,
            segments_after,
            self.limits.max_segments,
        )?;
        let original_payload = payload;
        let payload = OwnedPayload::new(&original_payload);
        let segment = TcpDataSegment {
            offset,
            end_offset,
            payload,
        };
        let result = TcpSendAppend {
            offset,
            end_offset,
            retained_bytes_added,
            retained_backing_bytes_added,
            ownership_bytes_copied: retained_bytes_added,
            segments_added: 1,
        };
        Ok(PreparedTcpSendAppend {
            window: self,
            segment,
            retained_after,
            retained_backing_after,
            result,
            original_payload,
            descriptors,
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
        Ok(self
            .prepare_acknowledge(next_accepted, final_accepted)?
            .commit())
    }

    pub(in crate::resumable) fn prepare_acknowledge(
        &mut self,
        next_accepted: ByteOffset,
        final_accepted: bool,
    ) -> Result<PreparedTcpSendAcknowledge<'_>, TcpOwnershipError> {
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
        let mut partial = None;

        for front in &self.segments {
            if front.end_offset() <= next_accepted {
                released_backing_bytes = released_backing_bytes
                    .checked_add(front.backing_bytes())
                    .ok_or(TcpOwnershipError::InvariantViolation(
                        "sender released backing byte accounting overflow",
                    ))?;
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
                partial = Some(remainder);
            }
            break;
        }

        let retained_bytes = self.retained_bytes.checked_sub(released).ok_or(
            TcpOwnershipError::InvariantViolation("sender retained byte accounting underflow"),
        )?;
        let retained_backing_bytes = self
            .retained_backing_bytes
            .checked_sub(released_backing_bytes)
            .ok_or(TcpOwnershipError::InvariantViolation(
                "sender retained backing byte accounting underflow",
            ))?;
        let final_accepted_transition = final_accepted && !self.final_accepted;
        let result = TcpSendAcknowledge {
            previous_peer_acked,
            peer_acked: next_accepted,
            retained_bytes_released: released,
            retained_backing_bytes_released: released_backing_bytes,
            compaction_bytes_copied,
            segments_released: released_segments,
            final_accepted_transition,
        };
        Ok(PreparedTcpSendAcknowledge {
            window: self,
            partial,
            retained_bytes,
            retained_backing_bytes,
            result,
        })
    }

    /// Records the exact final byte offset. Repeating the same CLOSE is
    /// idempotent; a different final offset fails closed.
    pub fn close(&mut self, final_offset: ByteOffset) -> Result<TcpSendClose, TcpOwnershipError> {
        Ok(self.prepare_close(final_offset)?.commit())
    }

    pub(in crate::resumable) fn prepare_close(
        &mut self,
        final_offset: ByteOffset,
    ) -> Result<PreparedTcpSendClose<'_>, TcpOwnershipError> {
        if let Some(recorded) = self.final_offset {
            if recorded != final_offset {
                return Err(TcpOwnershipError::ConflictingFinalOffset {
                    recorded,
                    attempted: final_offset,
                });
            }
            let result = TcpSendClose {
                final_offset,
                newly_recorded: false,
                final_accepted: self.final_accepted,
            };
            return Ok(PreparedTcpSendClose {
                window: self,
                result,
            });
        }
        if final_offset != self.next_sent {
            return Err(TcpOwnershipError::CloseOffsetMismatch {
                expected: self.next_sent,
                actual: final_offset,
            });
        }
        let result = TcpSendClose {
            final_offset,
            newly_recorded: true,
            final_accepted: false,
        };
        Ok(PreparedTcpSendClose {
            window: self,
            result,
        })
    }
}

/// Original backing stays untouched until the existing append commit point.
enum PreparedSendDescriptors {
    Reuse,
    Replace(Vec<TcpDataSegment>),
}

impl PreparedSendDescriptors {
    fn prepare(
        original: &VecDeque<TcpDataSegment>,
        need: usize,
        maximum: usize,
    ) -> Result<Self, TcpOwnershipError> {
        let old = original.capacity();
        let next = next_capacity(old, need, 1, maximum).map_err(descriptor_storage_error)?;
        if next == old {
            return Ok(Self::Reuse);
        }
        growth_peak::<TcpDataSegment>(old, next).map_err(descriptor_storage_error)?;
        #[cfg(test)]
        if FAIL_NEXT_DESCRIPTOR_ALLOCATION.replace(false) {
            return Err(TcpOwnershipError::DescriptorReserve);
        }
        Ok(Self::Replace(
            allocate_exact_vec(next).map_err(descriptor_storage_error)?,
        ))
    }
}

fn descriptor_storage_error(error: EffectStorageError) -> TcpOwnershipError {
    match error {
        EffectStorageError::Layout => TcpOwnershipError::DescriptorLayout,
        EffectStorageError::Reserve => TcpOwnershipError::DescriptorReserve,
        EffectStorageError::Bound | EffectStorageError::Overflow => {
            TcpOwnershipError::InvariantViolation("sender descriptor capacity is outside its bound")
        }
    }
}

/// Owns the exclusive window borrow, normalized segment and prepared backing.
/// Dropping before commit leaves every original window field unchanged.
pub(in crate::resumable) struct PreparedTcpSendAppend<'window> {
    window: &'window mut TcpSendWindow,
    segment: TcpDataSegment,
    retained_after: usize,
    retained_backing_after: usize,
    result: TcpSendAppend,
    original_payload: Bytes,
    descriptors: PreparedSendDescriptors,
}

impl PreparedTcpSendAppend<'_> {
    pub(in crate::resumable) fn commit(self) -> TcpSendAppend {
        match self.descriptors {
            PreparedSendDescriptors::Reuse => self.window.segments.push_back(self.segment),
            PreparedSendDescriptors::Replace(mut prepared) => {
                // Capacity covers every old descriptor plus this one. No Clone,
                // element Drop or allocation occurs in the original-owner move.
                while let Some(segment) = self.window.segments.pop_front() {
                    prepared.push(segment);
                }
                prepared.push(self.segment);
                self.window.segments = prepared.into();
            }
        }
        self.window.retained_bytes = self.retained_after;
        self.window.retained_backing_bytes = self.retained_backing_after;
        self.window.next_sent = self.result.end_offset;
        debug_assert!(self.window.backing_is_bounded());
        // Bytes::from_owner can run user Drop: keep the old public append
        // ordering, after normalized ownership/accounting are in the window.
        drop(self.original_payload);
        self.result
    }
}

/// The immutable prefix walk prepares at most one normalized partial suffix.
/// The same exclusive borrow prevents the prefix/partial position changing.
pub(in crate::resumable) struct PreparedTcpSendAcknowledge<'window> {
    window: &'window mut TcpSendWindow,
    partial: Option<OwnedPayload>,
    retained_bytes: usize,
    retained_backing_bytes: usize,
    result: TcpSendAcknowledge,
}

impl PreparedTcpSendAcknowledge<'_> {
    pub(in crate::resumable) fn outcome(&self) -> TcpSendAcknowledge {
        self.result
    }

    pub(in crate::resumable) fn commit(self) -> TcpSendAcknowledge {
        // The count was derived from this exact deque under its held borrow.
        drop(self.window.segments.drain(..self.result.segments_released));
        if let Some(remainder) = self.partial {
            // A prepared partial is necessarily the first non-released item.
            if let Some(front) = self.window.segments.front_mut() {
                front.offset = self.result.peer_acked;
                front.payload = remainder;
            }
        }
        self.window.retained_bytes = self.retained_bytes;
        self.window.retained_backing_bytes = self.retained_backing_bytes;
        self.window.peer_acked = self.result.peer_acked;
        self.window.final_accepted |= self.result.final_accepted_transition;
        debug_assert!(self.window.backing_is_bounded());
        self.result
    }
}

pub(in crate::resumable) struct PreparedTcpSendClose<'window> {
    window: &'window mut TcpSendWindow,
    result: TcpSendClose,
}

impl PreparedTcpSendClose<'_> {
    pub(in crate::resumable) fn outcome(&self) -> TcpSendClose {
        self.result
    }

    pub(in crate::resumable) fn commit(self) -> TcpSendClose {
        if self.result.newly_recorded {
            self.window.final_offset = Some(self.result.final_offset);
        }
        self.result
    }
}
