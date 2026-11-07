//! Unpublished receive insertion owned by the original Assembler. The mutable
//! borrow prevents interleaving while actual interval/heap growth is prepared.
//! Dropping a preparation releases its input and layouts without publication.
//! Connection-wide packet admission remains the caller's responsibility.
//! Tests live in insertion/tests.rs.
use super::super::storage::{intervals::Node, Slab};
use super::*;

pub(in crate::connection) struct PreparedInsertion<'a> {
    target: &'a mut Assembler,
    offset: u64,
    bytes: Bytes,
    allocation_size: usize,
    interval_growth: Option<Slab<Node>>,
    heap_growth: Option<storage::Growth>,
    meter: Option<BackingMeter>,
    replacement: Option<Replacement>,
    defrag_prepared: bool,
}

impl Assembler {
    pub(in crate::connection) fn check_input_authority(
        &self,
        bytes: &Bytes,
        expected_meter: Option<&BackingMeter>,
    ) -> Result<(), InsertError> {
        self.check_authority(expected_meter)?;
        if let Some(saved) = &self.backing_meter {
            if !expected_meter.is_some_and(|expected| saved.same_authority(expected)) {
                saved.quarantine();
                if let Some(expected) = expected_meter {
                    expected.quarantine();
                }
                return Err(InsertError::AuthorityMismatch);
            }
        }
        if !bytes.is_empty() {
            if let Some(expected) = expected_meter {
                match bytes.backing_meter() {
                    Some(actual) if expected.same_authority(&actual) => {}
                    Some(actual) => {
                        expected.quarantine();
                        actual.quarantine();
                        return Err(InsertError::AuthorityMismatch);
                    }
                    None => {
                        expected.quarantine();
                        return Err(InsertError::AuthorityMismatch);
                    }
                }
            }
        }
        Ok(())
    }

    pub(in crate::connection) fn prepare_insert(
        &mut self,
        mut offset: u64,
        mut bytes: Bytes,
        allocation_size: usize,
        expected_meter: Option<&BackingMeter>,
    ) -> Result<PreparedInsertion<'_>, InsertError> {
        self.check_input_authority(&bytes, expected_meter)?;
        debug_assert!(
            bytes.len() <= allocation_size,
            "allocation_size less than bytes.len(): {:?} < {:?}",
            allocation_size,
            bytes.len()
        );
        let meter = self.backing_meter.clone().or_else(|| {
            if bytes.is_empty() {
                None
            } else {
                expected_meter.cloned()
            }
        });
        // The contiguous received prefix is duplicate history, not aggregate
        // unordered bytes_read. It needs no sparse AVL node or payload copy.
        if let State::Unordered {
            received_prefix, ..
        } = &self.state
        {
            let duplicate = received_prefix
                .saturating_sub(offset)
                .min(bytes.len() as u64) as usize;
            bytes.advance(duplicate);
            offset += duplicate as u64;
        }
        let end = offset + bytes.len() as u64;
        let range = offset..end;
        let mut pieces = 0usize;
        let interval_growth = if let State::Unordered {
            recvd,
            received_prefix,
        } = &self.state
        {
            let mut cursor = offset;
            for duplicate in recvd.overlaps(range.clone()) {
                pieces += usize::from(duplicate.start > cursor);
                cursor = duplicate.end;
            }
            pieces += usize::from(cursor < end);
            if range.start == *received_prefix {
                None
            } else {
                recvd.prepare(range.clone(), meter.as_ref())?
            }
        } else {
            if offset < self.bytes_read {
                let consumed = (self.bytes_read - offset).min(bytes.len() as u64) as usize;
                offset += consumed as u64;
                bytes.advance(consumed);
            }
            pieces = usize::from(!bytes.is_empty());
            None
        };
        let heap_growth = self.data.growth(pieces, meter.as_ref())?;
        Ok(PreparedInsertion {
            target: self,
            offset,
            bytes,
            allocation_size,
            interval_growth,
            heap_growth,
            meter,
            replacement: None,
            defrag_prepared: false,
        })
    }
}

impl<'a> PreparedInsertion<'a> {
    /// Prepare the original post-insert defragmentation while the original
    /// heap and protocol state are still unpublished.
    pub(in crate::connection) fn prepare_defragmentation(mut self) -> Result<Self, InsertError> {
        let mut count = 0usize;
        let mut added = 0usize;
        let mut has_tail = false;
        self.for_each_piece(|buffer, tail| {
            count += 1;
            added += buffer.bytes.len();
            has_tail |= tail;
        });
        let buffered = self
            .target
            .buffered
            .checked_add(added)
            .ok_or(MeteredCopyError::TooLarge)?;
        let allocated = count
            .checked_mul(self.allocation_size)
            .and_then(|extra| self.target.allocated.checked_add(extra))
            .ok_or(MeteredCopyError::TooLarge)?;
        let end = self.target.end.max(self.offset + self.bytes.len() as u64);
        let unique = buffered.min((end - self.target.bytes_read) as usize);
        let excess = allocated
            .checked_sub(unique)
            .ok_or_else(|| self.target.inventory_failure())?;
        let threshold = unique.checked_mul(3).ok_or(MeteredCopyError::TooLarge)? / 2;
        if has_tail && excess > 32768.max(threshold) {
            let replacement =
                Replacement::prepare_insert(self.target, count, self.meter.as_ref(), |buffers| {
                    self.for_each_piece(|buffer, _| buffers.push(buffer))
                })?;
            if replacement.data.len() > 1024 {
                return Err(InsertError::TooManyChunks);
            }
            self.replacement = Some(replacement);
        }
        self.defrag_prepared = true;
        Ok(self)
    }

    fn for_each_piece(&self, mut visit: impl FnMut(Buffer, bool)) {
        let mut offset = self.offset;
        let end = offset + self.bytes.len() as u64;
        if let State::Unordered { recvd, .. } = &self.target.state {
            for duplicate in recvd.overlaps(offset..end) {
                if duplicate.start > offset {
                    let begin = (offset - self.offset) as usize;
                    let finish = (duplicate.start - self.offset) as usize;
                    visit(
                        Buffer::new(
                            offset,
                            self.bytes.slice(begin..finish),
                            self.allocation_size,
                        ),
                        false,
                    );
                }
                offset = duplicate.end;
            }
        }
        if offset < end {
            let begin = (offset - self.offset) as usize;
            visit(
                Buffer::new(offset, self.bytes.slice(begin..), self.allocation_size),
                true,
            );
        }
    }

    fn into_parts(self) -> (&'a mut Assembler, PacketInsertion) {
        let Self {
            target,
            offset,
            bytes,
            allocation_size,
            interval_growth,
            heap_growth,
            meter,
            replacement,
            defrag_prepared,
        } = self;
        let prepared = PacketInsertion {
            owner: target as *const Assembler as usize,
            shape: target.packet_shape(),
            offset,
            bytes,
            allocation_size,
            interval_growth,
            heap_growth,
            meter,
            replacement,
            defrag_prepared,
        };
        (target, prepared)
    }

    pub(in crate::connection) fn commit(self) -> Result<(), InsertError> {
        let (target, prepared) = self.into_parts();
        prepared.commit(target)
    }

    /// Used only while the original Connection retains exclusive synchronous
    /// packet custody. ACK/loss may change Send state, never this live Recv.
    pub(in crate::connection) fn for_packet(self) -> PacketInsertion {
        self.into_parts().1
    }
}

pub(in crate::connection) struct PacketInsertion {
    owner: usize,
    shape: (usize, usize, usize, u64, u64),
    offset: u64,
    bytes: Bytes,
    allocation_size: usize,
    interval_growth: Option<Slab<Node>>,
    heap_growth: Option<storage::Growth>,
    meter: Option<BackingMeter>,
    replacement: Option<Replacement>,
    defrag_prepared: bool,
}
impl Assembler {
    pub(super) fn packet_shape(&self) -> (usize, usize, usize, u64, u64) {
        (
            self.data.len(),
            self.buffered,
            self.allocated,
            self.bytes_read,
            self.end,
        )
    }
}
impl PacketInsertion {
    pub(super) fn reserved(
        target: &Assembler, mut offset: u64, mut bytes: Bytes,
        allocation_size: usize, meter: &BackingMeter, replacement: Option<Replacement>,
    ) -> Result<Self, InsertError> {
        let prefix = match &target.state {
            State::Unordered { received_prefix, .. } => *received_prefix,
            State::Ordered => target.bytes_read,
        };
        let consumed = prefix.saturating_sub(offset).min(bytes.len() as u64) as usize;
        bytes.advance(consumed);
        offset += consumed as u64;
        let end = offset + bytes.len() as u64;
        let mut pieces = 0;
        let mut cursor = offset;
        if let State::Unordered { recvd, received_prefix } = &target.state {
            if offset != *received_prefix && !recvd.insertion_funded(offset..end) {
                meter.quarantine(); return Err(target.inventory_failure());
            }
            for duplicate in recvd.overlaps(offset..end) {
                pieces += usize::from(duplicate.start > cursor); cursor = duplicate.end;
            }
        }
        pieces += usize::from(cursor < end);
        if pieces > target.data.capacity().saturating_sub(target.data.len()) {
            meter.quarantine(); return Err(target.inventory_failure());
        }
        Ok(Self { owner: target as *const Assembler as usize, shape: target.packet_shape(),
            offset, bytes, allocation_size, interval_growth: None, heap_growth: None,
            meter: Some(meter.clone()), replacement, defrag_prepared: true })
    }
    pub(in crate::connection) fn commit(self, target: &mut Assembler) -> Result<(), InsertError> {
        let Self {
            owner,
            shape,
            mut offset,
            mut bytes,
            allocation_size,
            interval_growth,
            heap_growth,
            meter,
            replacement,
            defrag_prepared,
        } = self;
        if owner != target as *const Assembler as usize || shape != target.packet_shape() {
            if let Some(meter) = &meter {
                meter.quarantine();
            }
            return Err(target.inventory_failure());
        }
        let end = offset + bytes.len() as u64;
        let range = offset..end;
        let mutation = Mutation::begin(meter.as_ref())?;
        target.backing_meter = meter;
        target.data.install(heap_growth);
        if let State::Unordered {
            recvd,
            received_prefix,
        } = &mut target.state
        {
            recvd.install(interval_growth);
            for duplicate in recvd.overlaps(range.clone()) {
                if duplicate.start > offset {
                    let buffer = Buffer::new(
                        offset,
                        bytes.split_to((duplicate.start - offset) as usize),
                        allocation_size,
                    );
                    target.buffered += buffer.bytes.len();
                    target.allocated += buffer.allocation_size;
                    target.data.push(buffer);
                    offset = duplicate.start;
                }
                bytes.advance((duplicate.end - offset) as usize);
                offset = duplicate.end;
            }
            record_received(recvd, received_prefix, range);
        }
        target.end = target.end.max(end);
        let has_tail = !bytes.is_empty();
        if has_tail {
            let buffer = Buffer::new(offset, bytes, allocation_size);
            target.buffered += buffer.bytes.len();
            target.allocated += buffer.allocation_size;
            target.data.push(buffer);
        }
        if let Some(replacement) = replacement {
            replacement.commit(target);
        }
        mutation.commit();
        if defrag_prepared || !has_tail {
            return Ok(());
        }
        // Insertion has now taken ownership. A later defrag refusal preserves
        // that admitted input too; the original Connection performs failstop.
        // `target.buffered` also counts duplicate bytes, therefore we use
        // `target.end - target.bytes_read` as an upper bound of buffered unique
        // bytes. This will cause a defragmentation if the amount of duplicate
        // bytes exceedes a proportion of the receive window size.
        let buffered = target
            .buffered
            .min((target.end - target.bytes_read) as usize);
        let over_allocation = target.allocated - buffered;
        // Rationale: on the one hand, we want to defragment rarely, ideally never
        // in non-pathological scenarios. However, a pathological or malicious
        // peer could send us one-byte frames, and since we use reference-counted
        // buffers in order to prevent copying, this could result in keeping a lot
        // of memory allocated. This limits over-allocation in proportion to the
        // buffered data. The constants are chosen somewhat arbitrarily and try to
        // balance between defragmentation overhead and over-allocation.
        let threshold = 32768.max(buffered * 3 / 2);
        if over_allocation > threshold {
            target.defragment()?;
            // ngtcp2 uses a threshold of 4000 -- try to be a little more conservative?
            if target.data.len() > 1024 {
                return Err(InsertError::TooManyChunks);
            }
        }

        Ok(())
    }
}
#[cfg(test)]
#[path = "insertion/tests.rs"]
mod tests;
