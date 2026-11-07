//! Synchronous packet-work storage. Eight entries stay on the stack; overflow
//! owns an exact charged slab. This is preparation workspace, never a queue.
use crate::connection::storage::Slab;
use bytes::{BackingMeter, MeteredCopyError};

pub(in crate::connection) struct Stock<T> {
    inline: [Option<T>; 8],
    len: usize,
    spill: Slab<Option<T>>,
}
impl<T> Default for Stock<T> {
    fn default() -> Self {
        Self {
            inline: std::array::from_fn(|_| None),
            len: 0,
            spill: Slab::default(),
        }
    }
}
impl<T> Stock<T> {
    pub(in crate::connection) fn push(&mut self, item: T, meter: &BackingMeter) -> Result<(), MeteredCopyError> {
        if self.spill.values.capacity() == 0 && self.len < self.inline.len() {
            self.inline[self.len] = Some(item);
            self.len += 1;
            return Ok(());
        }
        if self.spill.values.capacity() == 0 {
            let mut spill = Slab::prepare(16, Some(meter))?;
            for entry in &mut self.inline {
                spill.values.push(entry.take());
            }
            self.len = 0;
            self.spill = spill;
        }
        let growth = self.spill.growth(1, Some(meter))?;
        self.spill.install(growth);
        self.spill.values.push(Some(item));
        Ok(())
    }
    pub(in crate::connection) fn slice(&self) -> &[Option<T>] {
        if self.spill.values.capacity() == 0 {
            &self.inline[..self.len]
        } else {
            &self.spill.values
        }
    }
    pub(in crate::connection) fn slice_mut(&mut self) -> &mut [Option<T>] {
        if self.spill.values.capacity() == 0 {
            &mut self.inline[..self.len]
        } else {
            &mut self.spill.values
        }
    }
    pub(in crate::connection) fn compact(&mut self) {
        let values = self.slice_mut();
        let mut end = 0;
        for index in 0..values.len() {
            if let Some(item) = values[index].take() {
                values[end] = Some(item);
                end += 1;
            }
        }
        if self.spill.values.capacity() == 0 {
            self.len = end;
        } else {
            self.spill.values.truncate(end);
        }
    }
}
