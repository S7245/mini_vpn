//! A cold Box is a linear packet descriptor, not a runtime allocation. The
//! parent Pool owns its table and exact layouts. Idle boxes contain no Pool
//! references; checked-out boxes retain that same owner until all nodes return.
//! Entry: PacketRecords::prepare/Drop; tests live in records/tests.
use super::super::Retransmits;
use super::*;

pub(super) struct Descriptor {
    owner: Option<Pool>,
    slot: usize,
    generation: u64,
    metadata: Retransmits,
}
impl Default for Descriptor {
    fn default() -> Self {
        Self {
            owner: None,
            slot: 0,
            generation: 0,
            metadata: Retransmits::default(),
        }
    }
}
pub(super) struct Stock {
    boxes: Box<[Option<Box<Descriptor>>]>,
    free: usize,
}
impl Stock {
    pub(super) fn layout_bytes(count: usize) -> io::Result<usize> {
        let table = Layout::array::<Option<Box<Descriptor>>>(count).map_err(io::Error::other)?;
        Layout::new::<Descriptor>()
            .size()
            .checked_mul(count)
            .and_then(|n| n.checked_add(table.size()))
            .ok_or_else(|| io::Error::other("packet descriptor layout overflow"))
    }
    pub(super) fn prepare(count: usize) -> io::Result<Self> {
        let layout = Layout::array::<Option<Box<Descriptor>>>(count).map_err(io::Error::other)?;
        let ptr = unsafe { alloc(layout).cast::<Option<Box<Descriptor>>>() };
        if ptr.is_null() {
            return Err(io::Error::other("descriptor table allocation failed"));
        }
        for i in 0..count {
            unsafe {
                ptr.add(i).write(None);
            }
        }
        let mut boxes = unsafe { Box::from_raw(std::ptr::slice_from_raw_parts_mut(ptr, count)) };
        for (index, slot) in boxes.iter_mut().enumerate() {
            let ptr = unsafe { alloc(Layout::new::<Descriptor>()).cast::<Descriptor>() };
            if ptr.is_null() {
                return Err(io::Error::other("descriptor allocation failed"));
            }
            unsafe {
                ptr.write(Descriptor {
                    slot: index,
                    ..Descriptor::default()
                });
                *slot = Some(Box::from_raw(ptr));
            }
        }
        Ok(Self { boxes, free: count })
    }
    fn take(&mut self) -> Option<Box<Descriptor>> {
        let index = self.free.checked_sub(1)?;
        let generation = self.boxes[index].as_ref()?.generation.checked_add(1)?;
        let mut descriptor = self.boxes[index].take()?;
        descriptor.generation = generation;
        self.free = index;
        Some(descriptor)
    }
    fn put(&mut self, descriptor: Box<Descriptor>) -> Result<(), Box<Descriptor>> {
        let Some(slot) = self.boxes.get_mut(self.free).filter(|slot| slot.is_none()) else {
            return Err(descriptor);
        };
        *slot = Some(descriptor);
        self.free += 1;
        Ok(())
    }
}

#[derive(Default)]
pub(in crate::connection) struct PacketRecords(Option<Box<Descriptor>>);
impl fmt::Debug for PacketRecords {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PacketRecords")
            .field("len", &self.len())
            .finish()
    }
}
impl PacketRecords {
    pub(in crate::connection) fn prepare(
        reset: &Queue<(StreamId, VarInt)>,
        stop: &Queue<frame::StopSending>,
    ) -> Result<Self, ()> {
        let pool = match (&reset.mode, &stop.mode) {
            (Mode::Legacy(_), Mode::Legacy(_)) => return Ok(Self::default()),
            (Mode::Cold(a, _), Mode::Cold(b, _)) if a.pool.same(&b.pool) => &a.pool,
            _ => {
                reset.quarantine();
                stop.quarantine();
                return Err(());
            }
        };
        pool.0.meter.admit_precharged().map_err(|_| ())?;
        let mut descriptor = {
            let mut state = pool.lock();
            pool.0.meter.admit_precharged().map_err(|_| ())?;
            state.packets.take().ok_or(())?
        };
        descriptor.owner = Some(pool.clone());
        descriptor.metadata = Retransmits::with_records(
            Some(pool),
            CreditKey::packet(descriptor.slot, descriptor.generation),
        );
        Ok(Self(Some(descriptor)))
    }
    pub(in crate::connection) fn queues(
        &self,
    ) -> Option<(&Queue<(StreamId, VarInt)>, &Queue<frame::StopSending>)> {
        self.0
            .as_ref()
            .map(|d| (&d.metadata.reset_stream, &d.metadata.stop_sending))
    }
    pub(in crate::connection) fn queues_mut(
        &mut self,
    ) -> Option<(
        &mut Queue<(StreamId, VarInt)>,
        &mut Queue<frame::StopSending>,
    )> {
        self.0
            .as_mut()
            .map(|d| (&mut d.metadata.reset_stream, &mut d.metadata.stop_sending))
    }
    pub(in crate::connection) fn get(&self) -> Option<&Retransmits> {
        self.0.as_ref().map(|d| &d.metadata)
    }
    pub(in crate::connection) fn get_or_create(&mut self) -> &mut Retransmits {
        // Only the legacy path may allocate here. Every cold packet receives
        // this same Box from prepare before the Builder, including general-only.
        &mut self.0.get_or_insert_with(Box::default).metadata
    }
    pub(in crate::connection) fn take_metadata(&mut self) -> Option<Retransmits> {
        let metadata = self.0.as_mut().map(|d| std::mem::take(&mut d.metadata));
        *self = Self::default();
        metadata
    }
    pub(in crate::connection) fn len(&self) -> usize {
        self.queues()
            .map_or(0, |(reset, stop)| reset.len() + stop.len())
    }
    pub(in crate::connection) fn release_if_empty(&mut self) {
        if self.0.as_ref().is_some_and(|d| {
            let m = &d.metadata;
            m.reset_stream.is_empty()
                && m.stop_sending.is_empty()
                && !m.max_data
                && !m.max_stream_id.into_iter().any(|v| v)
                && m.max_stream_data.is_empty()
                && m.crypto.is_empty()
                && m.retire_cids.is_empty()
                && !m.ack_frequency
                && !m.handshake_done
                && m.new_tokens.is_empty()
        }) {
            *self = Self::default();
        }
    }
}
impl Drop for PacketRecords {
    fn drop(&mut self) {
        let Some(mut descriptor) = self.0.take() else {
            return;
        };
        let Some(pool) = descriptor.owner.take() else {
            return;
        };
        // Queue disposal locks this same Pool. Complete it before locking the
        // free descriptor stack, and remove every idle Arc to avoid a cycle.
        descriptor.metadata = Retransmits::default();
        if pool.lock().packets.put(descriptor).is_err() {
            pool.quarantine();
        }
    }
}
