//! Packet-local CRYPTO insertion/drain schedule for the original ordered Assembler.
//! Owns unpublished heap growth and defrag replacements, never TLS or an Assembler.
//! Reads still run through the original owner after each wire-order insertion.
//! Tests: crypto/tests.rs and tests/admission/packet_pressure/crypto.rs.
use super::super::storage::{stock::Stock, Slab};
use super::*;
use insertion::PacketInsertion;
type Shape = (usize, usize, usize, u64, u64);
struct Step {
    offset: u64,
    bytes: Bytes,
    allocation: usize,
    replacement: Option<Replacement>,
    after: Shape,
}
pub(in crate::connection) struct CryptoInsertion {
    owner: usize,
    before: Shape,
    steps: Stock<Step>,
    cursor: usize,
    growth: Option<storage::Growth>,
    meter: BackingMeter,
    pub(in crate::connection) reads: usize,
}
impl CryptoInsertion {
    /// Validate the original per-frame buffer limit before any resource Full.
    /// Only this bounded control path scans ranges. The closure starts a fresh
    /// zero-allocation range iterator; no payload or protocol owner is cloned.
    pub(in crate::connection) fn check<I: Iterator<Item = (u64, usize)>>(
        target: &Assembler,
        ranges: impl Fn() -> I,
        limit: usize,
    ) -> Result<(), PrepareError> {
        let mut read = target.bytes_read;
        for (index, (offset, len)) in ranges().enumerate() {
            let end = offset
                .checked_add(len as u64)
                .ok_or(MeteredCopyError::TooLarge)?;
            if end.saturating_sub(read) > limit as u64 {
                return Err(PrepareError::BufferLimit);
            }
            // Union of original buffered ranges and this packet's consumed
            // prefix. Revisit only while the contiguous prefix strictly grows.
            loop {
                let before = read;
                for (start, len) in target
                    .data
                    .iter()
                    .map(|b| (b.offset, b.bytes.len()))
                    .chain(ranges().take(index + 1))
                {
                    if start <= read {
                        read = read.max(
                            start
                                .checked_add(len as u64)
                                .ok_or(MeteredCopyError::TooLarge)?,
                        );
                    }
                }
                if before == read {
                    break;
                }
            }
        }
        Ok(())
    }
    pub(in crate::connection) fn prepare(
        target: &Assembler,
        inputs: impl Iterator<Item = (u64, Bytes, usize)>,
        meter: &BackingMeter,
        limit: usize,
    ) -> Result<Self, PrepareError> {
        target.check_inventory()?;
        if !target.state.is_ordered() {
            return Err(InsertError::AuthorityMismatch.into());
        }
        let mut model = Stock::default();
        for buffer in target.data.iter() {
            model.push(buffer.clone(), meter)?;
        }
        let mut result = Self {
            owner: target as *const Assembler as usize,
            before: target.packet_shape(),
            steps: Stock::default(),
            cursor: 0,
            growth: None,
            meter: meter.clone(),
            reads: 0,
        };
        let (mut buffered, mut allocated, mut read, mut end) = (
            target.buffered,
            target.allocated,
            target.bytes_read,
            target.end,
        );
        let (mut segment, mut peak) = (None, target.data.len());
        for (offset, bytes, allocation) in inputs {
            target.check_input_authority(&bytes, Some(meter))?;
            if bytes.len() > allocation {
                return Err(MeteredCopyError::TooLarge.into());
            }
            let finish = offset
                .checked_add(bytes.len() as u64)
                .ok_or(MeteredCopyError::TooLarge)?;
            if finish.saturating_sub(read) > limit as u64 {
                return Err(PrepareError::BufferLimit);
            }
            let skip = read.saturating_sub(offset).min(bytes.len() as u64) as usize;
            let tail = skip < bytes.len();
            end = end.max(finish);
            if tail {
                let piece = bytes.slice(skip..);
                buffered = buffered
                    .checked_add(piece.len())
                    .ok_or(MeteredCopyError::TooLarge)?;
                allocated = allocated
                    .checked_add(allocation)
                    .ok_or(MeteredCopyError::TooLarge)?;
                model.push(Buffer::new(offset + skip as u64, piece, allocation), meter)?;
            }
            peak = peak.max(model.slice().len());
            let unique = buffered.min(end.saturating_sub(read) as usize);
            let excess = allocated
                .checked_sub(unique)
                .ok_or(InsertError::AuthorityMismatch)?;
            let mut replacement = None;
            if tail
                && excess > 32768.max(unique.checked_mul(3).ok_or(MeteredCopyError::TooLarge)? / 2)
            {
                result.fund(target, segment, peak)?;
                let mut buffers = Slab::prepare(model.slice().len(), Some(meter))?;
                buffers
                    .values
                    .extend(model.slice().iter().flatten().cloned());
                let prepared = Replacement::prepare_buffers(buffers, meter)?;
                if prepared.data.len() > 1024 {
                    return Err(InsertError::TooManyChunks.into());
                }
                model = Stock::default();
                for buffer in prepared.data.iter() {
                    model.push(buffer.clone(), meter)?;
                }
                buffered = prepared.buffered;
                allocated = buffered;
                peak = prepared.data.len();
                segment = Some(result.steps.slice().len());
                replacement = Some(prepared);
            }
            // The original max-length ordered read consumes one entire head,
            // including obsolete overlaps, and stops at the first gap.
            loop {
                let head = model
                    .slice()
                    .iter()
                    .enumerate()
                    .filter_map(|(i, b)| b.as_ref().map(|b| (i, b)))
                    .max_by(|a, b| a.1.cmp(b.1));
                let Some((index, head)) = head else { break };
                if head.offset > read {
                    break;
                }
                let finish = head.offset + head.bytes.len() as u64;
                buffered -= head.bytes.len();
                allocated -= head.allocation_size;
                if finish > read {
                    result.reads += 1;
                    read = finish;
                }
                model.slice_mut()[index] = None;
            }
            model.compact();
            result.steps.push(
                Step {
                    offset,
                    bytes,
                    allocation,
                    replacement,
                    after: (model.slice().len(), buffered, allocated, read, end),
                },
                meter,
            )?;
        }
        result.fund(target, segment, peak)?;
        Ok(result)
    }
    fn fund(
        &mut self,
        target: &Assembler,
        segment: Option<usize>,
        peak: usize,
    ) -> Result<(), InsertError> {
        if let Some(index) = segment {
            let replacement = self
                .steps
                .slice_mut()
                .get_mut(index)
                .and_then(Option::as_mut)
                .and_then(|s| s.replacement.as_mut())
                .ok_or(InsertError::AuthorityMismatch)?;
            let growth = replacement.data.growth(
                peak.saturating_sub(replacement.data.len()),
                Some(&self.meter),
            )?;
            replacement.data.install(growth);
        } else {
            self.growth = target
                .data
                .growth(peak.saturating_sub(target.data.len()), Some(&self.meter))?;
        }
        Ok(())
    }
    pub(in crate::connection) fn insert_next(
        &mut self,
        target: &mut Assembler,
    ) -> Result<(), InsertError> {
        if target as *const Assembler as usize != self.owner || target.packet_shape() != self.before
        {
            self.meter.quarantine();
            return Err(target.inventory_failure());
        }
        if self.cursor == 0 {
            target.data.install(self.growth.take());
        }
        let step = self
            .steps
            .slice_mut()
            .get_mut(self.cursor)
            .and_then(Option::take)
            .ok_or_else(|| {
                self.meter.quarantine();
                InsertError::AuthorityMismatch
            })?;
        PacketInsertion::reserved(
            target,
            step.offset,
            step.bytes,
            step.allocation,
            &self.meter,
            step.replacement,
        )?
        .commit(target)?;
        self.before = step.after;
        self.cursor += 1;
        Ok(())
    }
    pub(in crate::connection) fn check_drained(
        &self,
        target: &Assembler,
    ) -> Result<(), InsertError> {
        if target as *const Assembler as usize != self.owner || target.packet_shape() != self.before
        {
            self.meter.quarantine();
            return Err(target.inventory_failure());
        }
        Ok(())
    }
}
#[derive(Debug)]
pub(in crate::connection) enum PrepareError {
    Insert(InsertError),
    BufferLimit,
}
impl From<InsertError> for PrepareError {
    fn from(e: InsertError) -> Self {
        Self::Insert(e)
    }
}
impl From<MeteredCopyError> for PrepareError {
    fn from(e: MeteredCopyError) -> Self {
        Self::Insert(e.into())
    }
}

#[cfg(test)]
#[path = "crypto/tests.rs"]
mod tests;
