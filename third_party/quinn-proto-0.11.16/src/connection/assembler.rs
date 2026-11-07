use std::{cmp::Ordering, mem};

use super::streams::ReadableError;
use bytes::{BackingMeter, Buf, Bytes, BytesMut, MeteredCopyError};

mod replacement;
pub(in crate::connection) mod crypto;
pub(in crate::connection) mod batch;
pub(in crate::connection) mod insertion;
mod storage;
use super::storage::intervals::Intervals;
use replacement::Replacement;
use storage::{Heap, Mutation};

/// Helper to assemble unordered stream frames into an ordered stream
#[derive(Debug, Default)]
pub(super) struct Assembler {
    backing_meter: Option<BackingMeter>,
    state: State,
    data: Heap,
    /// Total number of buffered bytes, including duplicates in ordered mode.
    buffered: usize,
    /// Estimated number of allocated bytes, will never be less than `buffered`.
    allocated: usize,
    /// Number of bytes read by the application. When only ordered reads have been used, this is the
    /// length of the contiguous prefix of the stream which has been consumed by the application,
    /// aka the stream offset.
    bytes_read: u64,
    end: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct AssemblerProgress {
    pub(super) read_offset: u64,
    pub(super) next_received_offset: Option<u64>,
    pub(super) highest_received_offset: u64,
    pub(super) buffered_bytes: usize,
    pub(super) ordered_gap_bytes: u64,
}

impl Assembler {
    pub(super) fn closed_inventory_matches(&self, meter: &BackingMeter) -> bool {
        self.backing_meter.as_ref().map_or_else(|| !self.has_native_layout(), |saved| saved.same_authority(meter))
            && self.data.layout_matches(meter)
            && self.data.iter().try_fold(0usize, |n, v| n.checked_add(v.bytes.len())) == Some(self.buffered)
            && self.data.iter().try_fold(0usize, |n, v| n.checked_add(v.allocation_size)) == Some(self.allocated)
            && self.bytes_read <= self.end
            && self.data.iter().all(|v| v.allocation_size >= v.bytes.len()
                && v.offset.checked_add(v.bytes.len() as u64).is_some_and(|end| end <= self.end)
                && v.bytes.backing_meter().is_some_and(|actual| actual.same_authority(meter)))
            && match &self.state { State::Ordered => true, State::Unordered { recvd, received_prefix } =>
                recvd.inventory_within(meter, self.end)
                    && recvd.min().is_none_or(|first| first > *received_prefix)
                    && received_prefix.checked_add(recvd.bytes()).is_some_and(|unique|
                        unique <= self.end && self.bytes_read <= unique) }
    }

    pub(super) fn new() -> Self {
        Self::default()
    }

    fn has_native_layout(&self) -> bool {
        self.data.has_native_layout() || match &self.state {
            State::Ordered => false,
            State::Unordered { recvd, .. } => recvd.has_native_layout(),
        }
    }
    fn inventory_failure(&self) -> InsertError {
        if let Some(meter) = &self.backing_meter { meter.quarantine(); }
        self.data.quarantine_layout_owner();
        // Actual chunks keep every implicated authority alive during marking.
        for chunk in self.data.iter() {
            if let Some(actual) = chunk.bytes.backing_meter() { actual.quarantine(); }
        }
        InsertError::AuthorityMismatch
    }
    /// Constant local authority check. A missing root may bind only while no
    /// actual hard layout remains; an internally consistent foreign inventory
    /// is still foreign to its original Connection.
    pub(super) fn check_authority(&self, expected: Option<&BackingMeter>) -> Result<(), InsertError> {
        let valid = match &self.backing_meter {
            Some(saved) => expected.is_none_or(|meter| saved.same_authority(meter)),
            None => !self.has_native_layout(),
        };
        if !valid {
            if let Some(meter) = expected { meter.quarantine(); }
            return Err(self.inventory_failure());
        }
        Ok(())
    }
    pub(super) fn check_inventory(&self) -> Result<(), InsertError> {
        let valid = match &self.backing_meter {
            Some(meter) => self.closed_inventory_matches(meter),
            None => !self.has_native_layout()
                && self.data.iter().all(|chunk| chunk.bytes.backing_meter().is_none()),
        };
        if !valid { return Err(self.inventory_failure()); }
        Ok(())
    }
    pub(super) fn check_inventory_with_end(&self, owner_end: u64, expected: Option<&BackingMeter>) -> Result<(), InsertError> {
        self.check_authority(expected)?;
        self.check_inventory()?;
        if self.end > owner_end || self.bytes_read > owner_end {
            return Err(self.inventory_failure());
        }
        Ok(())
    }

    /// Reset only a physically qualified original inventory.
    pub(super) fn reinit_checked(&mut self) -> Result<(), InsertError> {
        self.check_inventory()?;
        let old_data = mem::take(&mut self.data);
        let meter = self.backing_meter.take();
        *self = Self::default();
        self.backing_meter = meter;
        self.data = old_data;
        self.data.clear();
        Ok(())
    }

    /// Preserve an already-determined API rejection before resource admission.
    pub(super) fn check_ordering(&self, ordered: bool) -> Result<(), ReadableError> {
        if ordered && !self.state.is_ordered() {
            return Err(ReadableError::IllegalOrderedRead);
        }
        Ok(())
    }

    pub(super) fn ensure_ordering(&mut self, ordered: bool) -> Result<(), ReadableError> {
        self.check_ordering(ordered)?;
        if !ordered && self.state.is_ordered() {
            self.check_inventory().map_err(|_| ReadableError::BackingAuthorityLost)?;
            // All replacement layouts and offset inventory precede publication.
            let replacement = if self.data.is_empty() {
                None
            } else {
                Some(
                    Replacement::prepare(self, freeze_metered)
                        .map_err(|_| ReadableError::BackingAuthorityLost)?,
                )
            };
            let mut recvd = Intervals::default();
            let meter = self.backing_meter.as_ref();
            let mut received_prefix = self.bytes_read;
            let data = replacement.as_ref().map_or(&self.data, |r| &r.data);
            for chunk in data.iter() {
                let range = chunk.offset.max(received_prefix)..chunk.offset + chunk.bytes.len() as u64;
                if range.start > received_prefix {
                    let prepare = recvd.prepare(range.clone(), meter)
                        .map_err(|_| ReadableError::BackingAuthorityLost)?;
                    recvd.install(prepare);
                }
                record_received(&mut recvd, &mut received_prefix, range);
            }
            let mutation =
                Mutation::begin(meter).map_err(|_| ReadableError::BackingAuthorityLost)?;
            if let Some(replacement) = replacement {
                replacement.commit(self);
            }
            self.state = State::Unordered { recvd, received_prefix };
            mutation.commit();
        }
        Ok(())
    }

    /// Prepare the original next read without consuming useful bytes. Obsolete
    /// ordered heads are disposed exactly as read() did; gaps/EOF/zero reads
    /// predict no positive consumption and therefore require no control credit.
    pub(super) fn qualify_read(&self, expected: Option<&BackingMeter>) -> Result<(), ReadableError> {
        self.check_authority(expected).map_err(|_| self.read_inventory_lost())?;
        if !self.data.qualify() { return Err(self.read_inventory_lost()); }
        Ok(())
    }
    pub(super) fn prepare_read(&mut self, max_length: usize, ordered: bool) -> Result<Option<usize>, ReadableError> {
        self.qualify_read(None)?;
        loop {
            let Some(chunk) = self.data.peek() else { return Ok(None); };
            let end = chunk.offset.checked_add(chunk.bytes.len() as u64)
                .ok_or_else(|| self.read_inventory_lost())?;
            let start = if ordered {
                if chunk.offset > self.bytes_read { return Ok(None); }
                if end <= self.bytes_read {
                    if self.buffered < chunk.bytes.len() || self.allocated < chunk.allocation_size {
                        return Err(self.read_inventory_lost());
                    }
                    let actual = self.data.pop_checked().map_err(|_| self.read_inventory_lost())?
                        .ok_or_else(|| self.read_inventory_lost())?;
                    self.buffered -= actual.bytes.len();
                    self.allocated -= actual.allocation_size;
                    continue;
                }
                (self.bytes_read - chunk.offset) as usize
            } else { 0 };
            return Ok(Some((chunk.bytes.len() - start).min(max_length)));
        }
    }
    fn read_inventory_lost(&self) -> ReadableError {
        self.inventory_failure();
        ReadableError::BackingAuthorityLost
    }
    pub(super) fn read_checked(&mut self, max_length: usize, ordered: bool) -> Result<Option<Chunk>, ReadableError> {
        let Some(take) = self.prepare_read(max_length, ordered)? else { return Ok(None); };
        let Some(head) = self.data.peek() else { return Err(self.read_inventory_lost()); };
        let skip = if ordered { (self.bytes_read - head.offset) as usize } else { 0 };
        let consumed = skip.checked_add(take).ok_or_else(|| self.read_inventory_lost())?;
        let removed = if consumed == head.bytes.len() { head.allocation_size } else { 0 };
        let buffered = self.buffered.checked_sub(consumed).ok_or_else(|| self.read_inventory_lost())?;
        let allocated = self.allocated.checked_sub(removed).ok_or_else(|| self.read_inventory_lost())?;
        let bytes_read = self.bytes_read.checked_add(take as u64).ok_or_else(|| self.read_inventory_lost())?;
        let actual = self.data.read_head(skip, max_length).map_err(|_| self.read_inventory_lost())?
            .ok_or_else(|| self.read_inventory_lost())?;
        debug_assert_eq!((actual.discarded, actual.bytes.len(), actual.allocation_removed), (skip, take, removed));
        self.buffered = buffered;
        self.allocated = allocated;
        self.bytes_read = bytes_read;
        Ok(Some(Chunk::new(actual.offset, actual.bytes)))
    }

    /// Copy fragmented chunk data to new chunks backed by a single buffer
    ///
    /// This makes sure we're not unnecessarily holding on to many larger allocations.
    /// We merge contiguous chunks in the process of doing so.
    fn defragment(&mut self) -> Result<(), InsertError> {
        self.defragment_with(freeze_metered)
    }

    fn defragment_with<F>(&mut self, mut freeze: F) -> Result<(), InsertError>
    where
        F: FnMut(&mut BytesMut, Option<&BackingMeter>) -> Result<Bytes, InsertError>,
    {
        let replacement = Replacement::prepare(self, &mut freeze)?;
        let mutation = Mutation::begin(self.backing_meter.as_ref())?;
        replacement.commit(self);
        mutation.commit();
        Ok(())
    }

    // Note: If a packet contains many frames from the same stream, the estimated over-allocation
    // will be much higher because we are counting the same allocation multiple times.
    #[cfg(test)]
    pub(super) fn insert(
        &mut self,
        offset: u64,
        bytes: Bytes,
        allocation_size: usize,
    ) -> Result<(), InsertError> {
        self.insert_checked(offset, bytes, allocation_size, None)
    }

    pub(super) fn insert_checked(
        &mut self,
        offset: u64,
        bytes: Bytes,
        allocation_size: usize,
        expected_meter: Option<&BackingMeter>,
    ) -> Result<(), InsertError> {
        self.prepare_insert(offset, bytes, allocation_size, expected_meter)?.commit()
    }

    /// Number of bytes consumed by the application
    pub(super) fn bytes_read(&self) -> u64 {
        self.bytes_read
    }

    pub(super) fn progress(&self) -> AssemblerProgress {
        let next_received_offset = self.data.peek().map(|chunk| chunk.offset);
        AssemblerProgress {
            read_offset: self.bytes_read,
            next_received_offset,
            highest_received_offset: self.end,
            buffered_bytes: self.buffered,
            ordered_gap_bytes: next_received_offset
                .unwrap_or(self.bytes_read)
                .saturating_sub(self.bytes_read),
        }
    }

    /// Discard all buffered data
    pub(super) fn clear_checked(&mut self) -> Result<(), InsertError> {
        self.check_inventory()?;
        self.data.clear();
        self.buffered = 0;
        self.allocated = 0;
        Ok(())
    }
}

fn freeze_metered(
    buffer: &mut BytesMut,
    meter: Option<&BackingMeter>,
) -> Result<Bytes, InsertError> {
    let split = buffer.split();
    if let Some(meter) = meter {
        if !split.charge_shared_backing(meter) {
            meter.quarantine();
            return Err(InsertError::AuthorityMismatch);
        }
    }
    Ok(split.freeze())
}

/// A chunk of data from the receive stream
#[derive(Debug, PartialEq, Eq)]
pub struct Chunk {
    /// The offset in the stream
    pub offset: u64,
    /// The contents of the chunk
    pub bytes: Bytes,
}

impl Chunk {
    fn new(offset: u64, bytes: Bytes) -> Self {
        Self { offset, bytes }
    }
}

#[derive(Debug, Clone, Eq)]
struct Buffer {
    offset: u64,
    bytes: Bytes,
    /// Size of the allocation behind `bytes`, if `defragmented == false`.
    /// Otherwise this will be set to `bytes.len()` by `try_mark_defragment`.
    /// Will never be less than `bytes.len()`.
    allocation_size: usize,
    defragmented: bool,
}

impl Buffer {
    /// Constructs a new fragmented Buffer
    fn new(offset: u64, bytes: Bytes, allocation_size: usize) -> Self {
        Self {
            offset,
            bytes,
            allocation_size,
            defragmented: false,
        }
    }

    /// Constructs a new defragmented Buffer
    fn new_defragmented(offset: u64, bytes: Bytes) -> Self {
        let allocation_size = bytes.len();
        Self {
            offset,
            bytes,
            allocation_size,
            defragmented: true,
        }
    }

    /// Discards data before `offset` and flags `self` as defragmented if it has good utilization
    fn try_mark_defragment(&mut self, offset: u64) {
        let duplicate = offset.saturating_sub(self.offset) as usize;
        self.offset = self.offset.max(offset);
        if duplicate >= self.bytes.len() {
            // All bytes are duplicate
            self.bytes = Bytes::new();
            self.defragmented = true;
            self.allocation_size = 0;
            return;
        }
        self.bytes.advance(duplicate);
        // Make sure that fragmented buffers with high utilization become defragmented and
        // defragmented buffers remain defragmented
        self.defragmented = self.defragmented || self.bytes.len() * 6 / 5 >= self.allocation_size;
        if self.defragmented {
            // Make sure that defragmented buffers do not contribute to over-allocation
            self.allocation_size = self.bytes.len();
        }
    }
}

impl Ord for Buffer {
    // Invert ordering based on offset (max-heap, min offset first),
    // prioritize longer chunks at the same offset.
    fn cmp(&self, other: &Self) -> Ordering {
        self.offset
            .cmp(&other.offset)
            .reverse()
            .then(self.bytes.len().cmp(&other.bytes.len()))
    }
}

impl PartialOrd for Buffer {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Buffer {
    fn eq(&self, other: &Self) -> bool {
        (self.offset, self.bytes.len()) == (other.offset, other.bytes.len())
    }
}

#[derive(Debug, Default)]
enum State {
    #[default]
    Ordered,
    Unordered {
        /// Duplicate history after the contiguous prefix, including unread data.
        /// Every sparse range starts strictly beyond received_prefix.
        recvd: Intervals,
        received_prefix: u64,
    },
}

// Same Assembler commit: contiguous receipt consumes no sparse slot. Folding
// bridges returns nodes to this original slab, never its global layout charge.
fn record_received(recvd: &mut Intervals, prefix: &mut u64, range: std::ops::Range<u64>) {
    if range.is_empty() { return; }
    if range.start > *prefix { recvd.insert(range); return; }
    *prefix = (*prefix).max(range.end);
    while recvd.min().is_some_and(|start| start <= *prefix) {
        let Some(next) = recvd.pop_min() else { break; };
        *prefix = (*prefix).max(next.end);
    }
}

impl State {
    fn is_ordered(&self) -> bool {
        matches!(self, Self::Ordered)
    }
}

#[derive(Debug)]
pub(crate) enum InsertError {
    TooManyChunks,
    AuthorityMismatch,
    Capacity(MeteredCopyError),
}

impl From<MeteredCopyError> for InsertError {
    fn from(error: MeteredCopyError) -> Self {
        Self::Capacity(error)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use assert_matches::assert_matches;

    #[test]
    fn assemble_ordered() {
        let mut x = Assembler::new();
        assert_matches!(next(&mut x, 32), None);
        x.insert(0, Bytes::from_static(b"123"), 3).unwrap();
        assert_matches!(next(&mut x, 1), Some(ref y) if &y[..] == b"1");
        assert_matches!(next(&mut x, 3), Some(ref y) if &y[..] == b"23");
        x.insert(3, Bytes::from_static(b"456"), 3).unwrap();
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"456");
        x.insert(6, Bytes::from_static(b"789"), 3).unwrap();
        x.insert(9, Bytes::from_static(b"10"), 2).unwrap();
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"789");
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"10");
        assert_matches!(next(&mut x, 32), None);
    }

    #[test]
    fn ordered_progress_distinguishes_buffered_gap_from_contiguous_data() {
        let mut assembler = Assembler::new();
        assert_eq!(
            assembler.progress(),
            AssemblerProgress {
                read_offset: 0,
                next_received_offset: None,
                highest_received_offset: 0,
                buffered_bytes: 0,
                ordered_gap_bytes: 0,
            },
            "an empty assembler is not an ordered gap"
        );

        assembler.insert(3, Bytes::from_static(b"def"), 3).unwrap();
        let gap = assembler.progress();
        assert_eq!(gap.read_offset, 0);
        assert_eq!(gap.next_received_offset, Some(3));
        assert_eq!(gap.highest_received_offset, 6);
        assert_eq!(gap.buffered_bytes, 3);
        assert_eq!(gap.ordered_gap_bytes, 3);

        assembler.insert(0, Bytes::from_static(b"abc"), 3).unwrap();
        let contiguous = assembler.progress();
        assert_eq!(contiguous.next_received_offset, Some(0));
        assert_eq!(contiguous.ordered_gap_bytes, 0);

        assert_eq!(
            assembler.read(usize::MAX, true).unwrap().bytes.as_ref(),
            b"abc"
        );
        assert_eq!(
            assembler.read(usize::MAX, true).unwrap().bytes.as_ref(),
            b"def"
        );
        let drained = assembler.progress();
        assert_eq!(drained.read_offset, 6);
        assert_eq!(drained.next_received_offset, None);
        assert_eq!(drained.highest_received_offset, 6);
        assert_eq!(drained.buffered_bytes, 0);
        assert_eq!(drained.ordered_gap_bytes, 0);
    }

    #[test]
    fn assemble_unordered() {
        let mut x = Assembler::new();
        x.ensure_ordering(false).unwrap();
        x.insert(3, Bytes::from_static(b"456"), 3).unwrap();
        assert_matches!(next(&mut x, 32), None);
        x.insert(0, Bytes::from_static(b"123"), 3).unwrap();
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"123");
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"456");
        assert_matches!(next(&mut x, 32), None);
    }

    #[test]
    fn assemble_duplicate() {
        let mut x = Assembler::new();
        x.insert(0, Bytes::from_static(b"123"), 3).unwrap();
        x.insert(0, Bytes::from_static(b"123"), 3).unwrap();
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"123");
        assert_matches!(next(&mut x, 32), None);
    }

    #[test]
    fn assemble_duplicate_compact() {
        let mut x = Assembler::new();
        x.insert(0, Bytes::from_static(b"123"), 3).unwrap();
        x.insert(0, Bytes::from_static(b"123"), 3).unwrap();
        x.defragment();
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"123");
        assert_matches!(next(&mut x, 32), None);
    }

    #[test]
    fn assemble_contained() {
        let mut x = Assembler::new();
        x.insert(0, Bytes::from_static(b"12345"), 5).unwrap();
        x.insert(1, Bytes::from_static(b"234"), 3).unwrap();
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"12345");
        assert_matches!(next(&mut x, 32), None);
    }

    #[test]
    fn assemble_contained_compact() {
        let mut x = Assembler::new();
        x.insert(0, Bytes::from_static(b"12345"), 5).unwrap();
        x.insert(1, Bytes::from_static(b"234"), 3).unwrap();
        x.defragment();
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"12345");
        assert_matches!(next(&mut x, 32), None);
    }

    #[test]
    fn assemble_contains() {
        let mut x = Assembler::new();
        x.insert(1, Bytes::from_static(b"234"), 3).unwrap();
        x.insert(0, Bytes::from_static(b"12345"), 5).unwrap();
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"12345");
        assert_matches!(next(&mut x, 32), None);
    }

    #[test]
    fn assemble_contains_compact() {
        let mut x = Assembler::new();
        x.insert(1, Bytes::from_static(b"234"), 3).unwrap();
        x.insert(0, Bytes::from_static(b"12345"), 5).unwrap();
        x.defragment();
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"12345");
        assert_matches!(next(&mut x, 32), None);
    }

    #[test]
    fn assemble_overlapping() {
        let mut x = Assembler::new();
        x.insert(0, Bytes::from_static(b"123"), 3).unwrap();
        x.insert(1, Bytes::from_static(b"234"), 3).unwrap();
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"123");
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"4");
        assert_matches!(next(&mut x, 32), None);
    }

    #[test]
    fn assemble_overlapping_compact() {
        let mut x = Assembler::new();
        x.insert(0, Bytes::from_static(b"123"), 4).unwrap();
        x.insert(1, Bytes::from_static(b"234"), 4).unwrap();
        x.defragment();
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"1234");
        assert_matches!(next(&mut x, 32), None);
    }

    #[test]
    fn assemble_complex() {
        let mut x = Assembler::new();
        x.insert(0, Bytes::from_static(b"1"), 1).unwrap();
        x.insert(2, Bytes::from_static(b"3"), 1).unwrap();
        x.insert(4, Bytes::from_static(b"5"), 1).unwrap();
        x.insert(0, Bytes::from_static(b"123456"), 6).unwrap();
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"123456");
        assert_matches!(next(&mut x, 32), None);
    }

    #[test]
    fn assemble_complex_compact() {
        let mut x = Assembler::new();
        x.insert(0, Bytes::from_static(b"1"), 1).unwrap();
        x.insert(2, Bytes::from_static(b"3"), 1).unwrap();
        x.insert(4, Bytes::from_static(b"5"), 1).unwrap();
        x.insert(0, Bytes::from_static(b"123456"), 6).unwrap();
        x.defragment();
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"123456");
        assert_matches!(next(&mut x, 32), None);
    }

    #[test]
    fn assemble_old() {
        let mut x = Assembler::new();
        x.insert(0, Bytes::from_static(b"1234"), 4).unwrap();
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"1234");
        x.insert(0, Bytes::from_static(b"1234"), 4).unwrap();
        assert_matches!(next(&mut x, 32), None);
    }

    #[test]
    fn compact() {
        let mut x = Assembler::new();
        x.insert(0, Bytes::from_static(b"abc"), 4).unwrap();
        x.insert(3, Bytes::from_static(b"def"), 4).unwrap();
        x.insert(9, Bytes::from_static(b"jkl"), 4).unwrap();
        x.insert(12, Bytes::from_static(b"mno"), 4).unwrap();
        x.defragment();
        assert_eq!(
            next_unordered(&mut x),
            Chunk::new(0, Bytes::from_static(b"abcdef"))
        );
        assert_eq!(
            next_unordered(&mut x),
            Chunk::new(9, Bytes::from_static(b"jklmno"))
        );
    }

    #[test]
    fn defrag_with_missing_prefix() {
        let mut x = Assembler::new();
        x.insert(3, Bytes::from_static(b"def"), 3).unwrap();
        x.defragment();
        assert_eq!(
            next_unordered(&mut x),
            Chunk::new(3, Bytes::from_static(b"def"))
        );
    }

    #[test]
    fn defrag_read_chunk() {
        let mut x = Assembler::new();
        x.insert(3, Bytes::from_static(b"def"), 4).unwrap();
        x.insert(0, Bytes::from_static(b"abc"), 4).unwrap();
        x.insert(7, Bytes::from_static(b"hij"), 4).unwrap();
        x.insert(11, Bytes::from_static(b"lmn"), 4).unwrap();
        x.defragment();
        assert_matches!(x.read(usize::MAX, true), Some(ref y) if &y.bytes[..] == b"abcdef");
        x.insert(5, Bytes::from_static(b"fghijklmn"), 9).unwrap();
        assert_matches!(x.read(usize::MAX, true), Some(ref y) if &y.bytes[..] == b"ghijklmn");
        x.insert(13, Bytes::from_static(b"nopq"), 4).unwrap();
        assert_matches!(x.read(usize::MAX, true), Some(ref y) if &y.bytes[..] == b"opq");
        x.insert(15, Bytes::from_static(b"pqrs"), 4).unwrap();
        assert_matches!(x.read(usize::MAX, true), Some(ref y) if &y.bytes[..] == b"rs");
        assert_matches!(x.read(usize::MAX, true), None);
    }

    #[test]
    fn unordered_happy_path() {
        let mut x = Assembler::new();
        x.ensure_ordering(false).unwrap();
        x.insert(0, Bytes::from_static(b"abc"), 3).unwrap();
        assert_eq!(
            next_unordered(&mut x),
            Chunk::new(0, Bytes::from_static(b"abc"))
        );
        assert_eq!(x.read(usize::MAX, false), None);
        x.insert(3, Bytes::from_static(b"def"), 3).unwrap();
        assert_eq!(
            next_unordered(&mut x),
            Chunk::new(3, Bytes::from_static(b"def"))
        );
        assert_eq!(x.read(usize::MAX, false), None);
    }

    #[test]
    fn unordered_dedup() {
        let mut x = Assembler::new();
        x.ensure_ordering(false).unwrap();
        x.insert(3, Bytes::from_static(b"def"), 3).unwrap();
        assert_eq!(
            next_unordered(&mut x),
            Chunk::new(3, Bytes::from_static(b"def"))
        );
        assert_eq!(x.read(usize::MAX, false), None);
        x.insert(0, Bytes::from_static(b"a"), 1).unwrap();
        x.insert(0, Bytes::from_static(b"abcdefghi"), 9).unwrap();
        x.insert(0, Bytes::from_static(b"abcd"), 4).unwrap();
        assert_eq!(
            next_unordered(&mut x),
            Chunk::new(0, Bytes::from_static(b"a"))
        );
        assert_eq!(
            next_unordered(&mut x),
            Chunk::new(1, Bytes::from_static(b"bc"))
        );
        assert_eq!(
            next_unordered(&mut x),
            Chunk::new(6, Bytes::from_static(b"ghi"))
        );
        assert_eq!(x.read(usize::MAX, false), None);
        x.insert(8, Bytes::from_static(b"ijkl"), 4).unwrap();
        assert_eq!(
            next_unordered(&mut x),
            Chunk::new(9, Bytes::from_static(b"jkl"))
        );
        assert_eq!(x.read(usize::MAX, false), None);
        x.insert(12, Bytes::from_static(b"mno"), 3).unwrap();
        assert_eq!(
            next_unordered(&mut x),
            Chunk::new(12, Bytes::from_static(b"mno"))
        );
        assert_eq!(x.read(usize::MAX, false), None);
        x.insert(2, Bytes::from_static(b"cde"), 3).unwrap();
        assert_eq!(x.read(usize::MAX, false), None);
    }

    #[test]
    fn chunks_dedup() {
        let mut x = Assembler::new();
        x.insert(3, Bytes::from_static(b"def"), 3).unwrap();
        assert_eq!(x.read(usize::MAX, true), None);
        x.insert(0, Bytes::from_static(b"a"), 1).unwrap();
        x.insert(1, Bytes::from_static(b"bcdefghi"), 9).unwrap();
        x.insert(0, Bytes::from_static(b"abcd"), 4).unwrap();
        assert_eq!(
            x.read(usize::MAX, true),
            Some(Chunk::new(0, Bytes::from_static(b"abcd")))
        );
        assert_eq!(
            x.read(usize::MAX, true),
            Some(Chunk::new(4, Bytes::from_static(b"efghi")))
        );
        assert_eq!(x.read(usize::MAX, true), None);
        x.insert(8, Bytes::from_static(b"ijkl"), 4).unwrap();
        assert_eq!(
            x.read(usize::MAX, true),
            Some(Chunk::new(9, Bytes::from_static(b"jkl")))
        );
        assert_eq!(x.read(usize::MAX, true), None);
        x.insert(12, Bytes::from_static(b"mno"), 3).unwrap();
        assert_eq!(
            x.read(usize::MAX, true),
            Some(Chunk::new(12, Bytes::from_static(b"mno")))
        );
        assert_eq!(x.read(usize::MAX, true), None);
        x.insert(2, Bytes::from_static(b"cde"), 3).unwrap();
        assert_eq!(x.read(usize::MAX, true), None);
    }

    #[test]
    fn ordered_eager_discard() {
        let mut x = Assembler::new();
        x.insert(0, Bytes::from_static(b"abc"), 3).unwrap();
        assert_eq!(x.data.len(), 1);
        assert_eq!(
            x.read(usize::MAX, true),
            Some(Chunk::new(0, Bytes::from_static(b"abc")))
        );
        x.insert(0, Bytes::from_static(b"ab"), 2).unwrap();
        assert_eq!(x.data.len(), 0);
        x.insert(2, Bytes::from_static(b"cd"), 2).unwrap();
        assert_eq!(
            x.data.peek(),
            Some(&Buffer::new(3, Bytes::from_static(b"d"), 2))
        );
    }

    #[test]
    fn ordered_insert_unordered_read() {
        let mut x = Assembler::new();
        x.insert(0, Bytes::from_static(b"abc"), 3).unwrap();
        x.insert(0, Bytes::from_static(b"abc"), 3).unwrap();
        x.ensure_ordering(false).unwrap();
        assert_eq!(
            x.read(3, false),
            Some(Chunk::new(0, Bytes::from_static(b"abc")))
        );
        assert_eq!(x.read(3, false), None);
    }

    fn next_unordered(x: &mut Assembler) -> Chunk {
        x.read(usize::MAX, false).unwrap()
    }

    fn next(x: &mut Assembler, size: usize) -> Option<Bytes> {
        x.read(size, true).map(|chunk| chunk.bytes)
    }
}

#[cfg(test)]
mod meter_tests;

#[cfg(test)]
#[path = "assembler/tests/inventory.rs"]
mod inventory_tests;

#[cfg(test)]
#[path = "assembler/tests/prefix.rs"]
mod prefix_tests;

#[cfg(test)]
#[path = "assembler/tests/legacy_read.rs"]
mod legacy_read;
