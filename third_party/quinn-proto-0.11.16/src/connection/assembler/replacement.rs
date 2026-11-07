//! Defragmentation preparation keeps the original heap/backings live until the
//! caller commits. Scratch, replacement metadata and payload all acquire the
//! same authority's layout allowance before allocation. No late adoption.
use super::{
    storage::Heap,
    Assembler, Buffer, InsertError,
};
use bytes::{BackingMeter, Bytes, BytesMut};
use super::super::storage::Slab;

pub(super) struct Replacement {
    pub(super) data: Heap,
    pub(super) buffered: usize,
}
impl Replacement {
    pub(super) fn prepare<F>(source: &Assembler, mut freeze: F) -> Result<Self, InsertError>
    where
        F: FnMut(&mut BytesMut, Option<&BackingMeter>) -> Result<Bytes, InsertError>,
    {
        source.check_inventory()?;
        let meter = source.backing_meter.as_ref();
        if let Some(owner) = meter {
            owner.admit_precharged()?;
            for chunk in source.data.iter() {
                match chunk.bytes.backing_meter() {
                    Some(other) if owner.same_authority(&other) => {}
                    other => {
                        owner.quarantine();
                        if let Some(other) = other {
                            other.quarantine();
                        }
                        return Err(InsertError::AuthorityMismatch);
                    }
                }
            }
        }
        let mut buffers = Slab::prepare(source.data.len(), meter)?;
        buffers.values.extend(source.data.iter().cloned());
        Self::from_buffers(buffers, meter, &mut freeze)
    }

    pub(super) fn prepare_insert(
        source: &Assembler,
        additional: usize,
        meter: Option<&BackingMeter>,
        append: impl FnOnce(&mut Vec<Buffer>),
    ) -> Result<Self, InsertError> {
        source.check_inventory()?;
        Self::prepare_heap(&source.data, additional, meter, append)
    }

    pub(super) fn prepare_heap(
        source: &Heap, additional: usize, meter: Option<&BackingMeter>,
        append: impl FnOnce(&mut Vec<Buffer>),
    ) -> Result<Self, InsertError> {
        let count = source.len().checked_add(additional)
            .ok_or(bytes::MeteredCopyError::TooLarge)?;
        let mut buffers = Slab::prepare(count, meter)?;
        buffers.values.extend(source.iter().cloned());
        append(&mut buffers.values);
        Self::from_buffers(buffers, meter, &mut super::freeze_metered)
    }

    pub(super) fn prepare_buffers(
        buffers: Slab<Buffer>, meter: &BackingMeter,
    ) -> Result<Self, InsertError> {
        Self::from_buffers(buffers, Some(meter), &mut super::freeze_metered)
    }

    fn from_buffers<F>(
        mut buffers: Slab<Buffer>,
        meter: Option<&BackingMeter>,
        freeze: &mut F,
    ) -> Result<Self, InsertError>
    where
        F: FnMut(&mut BytesMut, Option<&BackingMeter>) -> Result<Bytes, InsertError>,
    {
        let mut data = Heap::prepare(buffers.values.len(), meter)?;
        buffers.values.sort_unstable();
        let (mut buffered, mut fragmented, mut offset) = (0, 0, 0);
        for chunk in buffers.values.iter_mut().rev() {
            chunk.try_mark_defragment(offset);
            let size = chunk.bytes.len();
            offset = chunk.offset + size as u64;
            buffered += size;
            if !chunk.defragmented {
                fragmented += size;
            }
        }
        let mut buffer = if fragmented == 0 {
            BytesMut::new()
        } else if let Some(owner) = meter {
            BytesMut::try_zeroed_metered(fragmented, owner)?
        } else {
            BytesMut::zeroed(fragmented)
        };
        let (mut offset, mut used) = (0, 0);
        while let Some(chunk) = buffers.values.pop() {
            if chunk.defragmented {
                if !chunk.bytes.is_empty() {
                    data.push(chunk);
                }
                continue;
            }
            if chunk.offset != offset + used as u64 {
                if used != 0 {
                    let mut part = buffer.split_to(used);
                    data.push(Buffer::new_defragmented(offset, freeze(&mut part, meter)?));
                    used = 0;
                }
                offset = chunk.offset;
            }
            let next = used + chunk.bytes.len();
            buffer[used..next].copy_from_slice(&chunk.bytes);
            used = next;
        }
        if used != 0 {
            let mut part = buffer.split_to(used);
            data.push(Buffer::new_defragmented(offset, freeze(&mut part, meter)?));
        }
        Ok(Self { data, buffered })
    }
    pub(super) fn commit(self, target: &mut Assembler) {
        target.data = self.data;
        target.buffered = self.buffered;
        target.allocated = self.buffered;
    }
}
