//! Ordered packet-local insertions into one original Assembler. Preparation
//! overlays only this packet's ranges on the original AVL history. Heap scans
//! occur only when the original per-frame defrag threshold requires them.
//! Each heap segment and replacement is physically funded before publication.
//! No receive owner, queue or timer is cloned. Tests live in batch/tests.rs.
use super::super::storage::{intervals::Node, stock::Stock, Slab};
use super::*;
use insertion::PacketInsertion;

struct Step {
    offset: u64,
    bytes: Bytes,
    allocation: usize,
    replacement: Option<Replacement>,
}
pub(in crate::connection) struct BatchInsertion {
    owner: usize,
    shape: (usize, usize, usize, u64, u64),
    steps: Stock<Step>,
    cursor: usize,
    heap: Option<storage::Growth>,
    intervals: Option<Slab<Node>>,
    meter: BackingMeter,
}
impl BatchInsertion {
    pub(in crate::connection) fn prepare(
        target: &Assembler,
        inputs: impl Iterator<Item = (u64, Bytes, usize)>,
        meter: &BackingMeter,
    ) -> Result<Self, InsertError> {
        let mut result = Self {
            owner: target as *const Assembler as usize,
            shape: target.packet_shape(),
            steps: Stock::default(),
            cursor: 0,
            heap: None,
            intervals: None,
            meter: meter.clone(),
        };
        let (mut buffered, mut allocated, mut end) =
            (target.buffered, target.allocated, target.end);
        let (mut segment, mut count, mut nodes) = (0, 0usize, 0usize);
        for (offset, bytes, allocation) in inputs {
            target.check_input_authority(&bytes, Some(meter))?;
            if bytes.len() > allocation {
                return Err(MeteredCopyError::TooLarge.into());
            }
            let step = Step {
                offset,
                bytes,
                allocation,
                replacement: None,
            };
            let index = result.steps.slice().len();
            result.steps.push(step, meter)?;
            let mut tail = false;
            visit(target, result.steps.slice(), index, |buffer, is_tail| {
                buffered += buffer.bytes.len();
                allocated += buffer.allocation_size;
                count += 1;
                tail |= is_tail;
            });
            let step = result.steps.slice()[index]
                .as_ref()
                .ok_or(InsertError::AuthorityMismatch)?;
            end = end.max(step.offset + step.bytes.len() as u64);
            if let State::Unordered {
                received_prefix,
                recvd,
            } = &target.state
            {
                let start = step.offset.max(*received_prefix);
                let finish = step.offset + step.bytes.len() as u64;
                // Only truly disjoint packet ranges can require a new AVL node.
                // Adjacent earlier ranges may have been absorbed into prefix;
                // that also makes this insertion allocation-free.
                let touches_packet = result.steps.slice()[..index].iter().flatten().any(|old| {
                    old.offset <= finish && old.offset + old.bytes.len() as u64 >= start
                });
                if start < finish
                    && start != *received_prefix
                    && !touches_packet
                    && recvd.needs_node(start..finish)
                {
                    nodes += 1;
                }
            }
            let unique = buffered.min(end.saturating_sub(target.bytes_read) as usize);
            let excess = allocated
                .checked_sub(unique)
                .ok_or_else(|| target.inventory_failure())?;
            if tail
                && excess > 32768.max(unique.checked_mul(3).ok_or(MeteredCopyError::TooLarge)? / 2)
            {
                target.check_inventory()?;
                result.fund_segment(target, segment, count)?;
                let source = if segment == 0 {
                    &target.data
                } else {
                    &result.steps.slice()[segment - 1]
                        .as_ref()
                        .and_then(|s| s.replacement.as_ref())
                        .ok_or(InsertError::AuthorityMismatch)?
                        .data
                };
                let replacement =
                    Replacement::prepare_heap(source, count, Some(meter), |buffers| {
                        for i in segment..=index {
                            visit(target, result.steps.slice(), i, |buffer, _| {
                                buffers.push(buffer)
                            });
                        }
                    })?;
                if replacement.data.len() > 1024 {
                    return Err(InsertError::TooManyChunks);
                }
                buffered = replacement.buffered;
                allocated = buffered;
                result.steps.slice_mut()[index]
                    .as_mut()
                    .ok_or(InsertError::AuthorityMismatch)?
                    .replacement = Some(replacement);
                segment = index + 1;
                count = 0;
            }
        }
        result.fund_segment(target, segment, count)?;
        if let State::Unordered { recvd, .. } = &target.state {
            result.intervals = recvd.prepare_insertions(nodes, meter)?;
        }
        Ok(result)
    }
    fn fund_segment(
        &mut self,
        target: &Assembler,
        segment: usize,
        count: usize,
    ) -> Result<(), InsertError> {
        if segment == 0 {
            self.heap = target.data.growth(count, Some(&self.meter))?;
        } else {
            let replacement = self.steps.slice_mut()[segment - 1]
                .as_mut()
                .and_then(|step| step.replacement.as_mut())
                .ok_or(InsertError::AuthorityMismatch)?;
            let growth = replacement.data.growth(count, Some(&self.meter))?;
            replacement.data.install(growth);
        }
        Ok(())
    }
    pub(in crate::connection) fn commit_next(
        &mut self,
        target: &mut Assembler,
    ) -> Result<(), InsertError> {
        if self.owner != target as *const Assembler as usize || self.shape != target.packet_shape()
        {
            self.meter.quarantine();
            return Err(target.inventory_failure());
        }
        if self.cursor == 0 {
            target.data.install(self.heap.take());
            if let State::Unordered { recvd, .. } = &mut target.state {
                recvd.install(self.intervals.take());
            }
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
        // Original frame insertion consumes only the already funded layouts.
        PacketInsertion::reserved(
            target,
            step.offset,
            step.bytes,
            step.allocation,
            &self.meter,
            step.replacement,
        )?
        .commit(target)?;
        self.cursor += 1;
        self.shape = target.packet_shape();
        Ok(())
    }
}

/// Subtract original history and earlier packet ranges without cloning either
/// inventory. Overlap queries seek only the frame's intersecting AVL ranges.
fn visit(
    target: &Assembler,
    steps: &[Option<Step>],
    index: usize,
    mut emit: impl FnMut(Buffer, bool),
) {
    let Some(step) = &steps[index] else { return };
    let end = step.offset + step.bytes.len() as u64;
    let prefix = match &target.state {
        State::Ordered => target.bytes_read,
        State::Unordered {
            received_prefix, ..
        } => *received_prefix,
    };
    let mut cursor = step.offset.max(prefix).min(end);
    while cursor < end {
        let duplicate = if let State::Unordered { recvd, .. } = &target.state {
            recvd
                .overlaps(cursor..end)
                .next()
                .into_iter()
                .chain(
                    steps[..index]
                        .iter()
                        .flatten()
                        .map(|s| s.offset..s.offset + s.bytes.len() as u64)
                        .filter(|r| r.start < end && r.end > cursor)
                        .map(|r| r.start.max(cursor)..r.end.min(end)),
                )
                .min_by_key(|r| r.start)
        } else {
            None
        };
        let next = duplicate.as_ref().map_or(end, |r| r.start);
        if cursor < next {
            emit(
                Buffer::new(
                    cursor,
                    step.bytes
                        .slice((cursor - step.offset) as usize..(next - step.offset) as usize),
                    step.allocation,
                ),
                next == end,
            );
        }
        cursor = duplicate.map_or(end, |r| r.end);
    }
}
#[cfg(test)]
#[path = "batch/tests.rs"]
mod tests;
