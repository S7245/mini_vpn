//! Owns the ordered source's 64 descriptors and original FIFO's 64 slots.
//! Cold preparation precedes stream/lease transfer; cancellation drops only
//! known unpublished physical owners. Custody still owns read/sent counters,
//! producer/reader pending values and actual JOIN. Tests: tests/cold.rs/source.rs.
use super::*;
use quinn::owned_events as lane;
use std::{
    alloc::{Layout, alloc},
    ops::{Deref, DerefMut},
    ptr,
};

pub(super) struct SourceSlots {
    slots: Box<[bytes::Bytes]>,
    _reservation: Option<bytes::BackingBudgetReservation>,
    meter: Option<bytes::BackingMeter>,
}
impl SourceSlots {
    pub(super) fn quarantined(&self) -> bool {
        self.meter
            .as_ref()
            .is_some_and(bytes::BackingMeter::is_quarantined)
    }
    pub(super) fn layout() -> Layout {
        Layout::new::<[bytes::Bytes; TUIC_TCP_NATIVE_ORDERED_PUMP_READ_CHUNKS]>()
    }
    pub(super) fn prepare(meter: Option<&bytes::BackingMeter>) -> io::Result<Self> {
        let layout = Self::layout();
        let reservation = meter
            .map(|meter| meter.reserve_cold_layout(layout.size()))
            .transpose()
            .map_err(|error| io::Error::from(lane::AdmissionError(error)))?
            .flatten();
        let raw = unsafe { alloc(layout) }.cast::<bytes::Bytes>();
        if raw.is_null() {
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "ordered source slots",
            ));
        }
        for i in 0..TUIC_TCP_NATIVE_ORDERED_PUMP_READ_CHUNKS {
            unsafe {
                ptr::write(raw.add(i), bytes::Bytes::new());
            }
        }
        // Exact array layout, all entries initialized; physical storage precedes refund.
        let slots = unsafe {
            Box::from_raw(ptr::slice_from_raw_parts_mut(
                raw,
                TUIC_TCP_NATIVE_ORDERED_PUMP_READ_CHUNKS,
            ))
        };
        Ok(Self {
            slots,
            _reservation: reservation,
            meter: meter.cloned(),
        })
    }
}
impl Deref for SourceSlots {
    type Target = [bytes::Bytes];
    fn deref(&self) -> &Self::Target {
        &self.slots
    }
}
impl DerefMut for SourceSlots {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.slots
    }
}
impl Drop for SourceSlots {
    fn drop(&mut self) {
        if std::thread::panicking() {
            if let Some(meter) = &self.meter {
                meter.quarantine();
            }
        }
    }
}

pub(super) enum Sender {
    Legacy(mpsc::Sender<custody::Item>),
    Native(lane::Sender<custody::Item>),
}
pub(super) enum Receiver {
    Legacy(mpsc::Receiver<custody::Item>),
    Native(lane::Receiver<custody::Item>),
}
pub(super) enum Permit<'a> {
    Legacy(mpsc::Permit<'a, custody::Item>),
    Native(lane::Permit<custody::Item>),
}
pub(super) fn channel(meter: Option<&bytes::BackingMeter>) -> io::Result<(Sender, Receiver)> {
    if let Some(meter) = meter.filter(|meter| meter.budget_limit().is_some()) {
        let (tx, rx) = lane::channel(TUIC_TCP_NATIVE_ORDERED_PUMP_CHANNEL_CHUNKS, meter)?;
        Ok((Sender::Native(tx), Receiver::Native(rx)))
    } else {
        let (tx, rx) = mpsc::channel(TUIC_TCP_NATIVE_ORDERED_PUMP_CHANNEL_CHUNKS);
        Ok((Sender::Legacy(tx), Receiver::Legacy(rx)))
    }
}
impl Sender {
    pub(super) async fn reserve(&self) -> Result<Permit<'_>, ()> {
        match self {
            Self::Legacy(tx) => tx.reserve().await.map(Permit::Legacy).map_err(|_| ()),
            Self::Native(tx) => std::future::poll_fn(|cx| tx.poll_reserve(cx))
                .await
                .map(Permit::Native)
                .map_err(|_| ()),
        }
    }
}
impl Permit<'_> {
    pub(super) fn send(self, item: custody::Item) -> Result<(), custody::Item> {
        match self {
            Self::Legacy(permit) => {
                permit.send(item);
                Ok(())
            }
            Self::Native(permit) => permit.send(item),
        }
    }
}
impl Receiver {
    pub(super) fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<custody::Item>> {
        match self {
            Self::Legacy(rx) => rx.poll_recv(cx),
            Self::Native(rx) => rx.poll_recv(cx),
        }
    }
    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        match self {
            Self::Legacy(rx) => rx.len(),
            Self::Native(rx) => rx.len(),
        }
    }
}
