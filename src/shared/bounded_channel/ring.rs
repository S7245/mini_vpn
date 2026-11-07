//! Native lane's actual fixed slot allocation and atomic FIFO publication.
//! Based on the stamp/lap algorithm in local Rust std sync/mpmc/array.rs
//! (Rust project, MIT OR Apache-2.0; Vyukov bounded queue). Unlike std's blocking
//! receive, an unfinished publication returns InFlight immediately. No slot
//! claim escapes try_push, and there is no opaque call between claim and publish.
//! Logical credits, callbacks, receiver lifetime and value disposal belong to
//! the enclosing lane. Unknown final stock is retained under the original Q.
//! Tests: tests/ring.rs, reached through the parent test index.
use bytes::{BackingBudgetReservation, BackingMeter, MeteredCopyError};
use std::{
    alloc::{Layout, alloc},
    cell::UnsafeCell,
    mem::{ManuallyDrop, MaybeUninit},
    sync::atomic::{AtomicUsize, Ordering, fence},
};

struct Slot<T> {
    stamp: AtomicUsize,
    value: UnsafeCell<MaybeUninit<T>>,
}
#[repr(align(128))]
struct Cursor(AtomicUsize);
pub(super) enum Pop<T> {
    Value(T),
    Empty,
    InFlight,
}
#[cfg(test)]
pub(super) struct PublicationPause {
    pub(super) entered: std::sync::Barrier,
    pub(super) resume: std::sync::Barrier,
}
pub(super) struct Ring<T> {
    #[cfg(test)]
    pub(super) pause: std::sync::Mutex<Option<std::sync::Arc<PublicationPause>>>,
    #[cfg(test)]
    pub(super) before_claim: std::sync::Mutex<Option<std::sync::Arc<PublicationPause>>>,
    head: Cursor,
    tail: Cursor,
    slots: ManuallyDrop<Box<[Slot<T>]>>,
    stock: ManuallyDrop<Option<BackingBudgetReservation>>,
    meter: BackingMeter,
    index_mask: usize,
    lap: usize,
}
// Slot ownership comes only from successful head/tail CAS. Release of a slot
// stamp follows the last read/write, and the next owner acquires that stamp.
// The value is only moved, never borrowed across threads or automatically dropped.
unsafe impl<T: Send> Send for Ring<T> {}
unsafe impl<T: Send> Sync for Ring<T> {}

impl<T> Ring<T> {
    pub(super) fn layout_bytes(capacity: usize) -> Result<usize, MeteredCopyError> {
        Self::layout(capacity).map(|layout| layout.size())
    }
    fn layout(capacity: usize) -> Result<Layout, MeteredCopyError> {
        if capacity == 0 || capacity > 1024 {
            return Err(MeteredCopyError::TooLarge);
        }
        Layout::array::<Slot<T>>(capacity).map_err(|_| MeteredCopyError::TooLarge)
    }
    pub(super) fn prepare(capacity: usize, meter: &BackingMeter) -> Result<Self, MeteredCopyError> {
        let layout = Self::layout(capacity)?;
        let stock = meter.reserve_cold_layout(layout.size())?;
        let raw = unsafe { alloc(layout) }.cast::<Slot<T>>();
        if raw.is_null() {
            return Err(MeteredCopyError::Allocation);
        }
        for i in 0..capacity {
            unsafe {
                raw.add(i).write(Slot {
                    stamp: AtomicUsize::new(i),
                    value: UnsafeCell::new(MaybeUninit::uninit()),
                });
            }
        }
        let slots = unsafe { Box::from_raw(std::ptr::slice_from_raw_parts_mut(raw, capacity)) };
        let mark = (capacity + 1).next_power_of_two(); // capacity checked <= 1024
        Ok(Self {
            #[cfg(test)]
            pause: std::sync::Mutex::new(None),
            #[cfg(test)]
            before_claim: std::sync::Mutex::new(None),
            head: Cursor(AtomicUsize::new(0)),
            tail: Cursor(AtomicUsize::new(0)),
            slots: ManuallyDrop::new(slots),
            stock: ManuallyDrop::new(stock),
            meter: meter.clone(),
            index_mask: mark - 1,
            lap: mark * 2,
        })
    }
    fn next(&self, stamp: usize, index: usize) -> usize {
        if index + 1 < self.slots.len() {
            stamp + 1
        } else {
            (stamp & !(self.lap - 1)).wrapping_add(self.lap)
        }
    }
    pub(super) fn try_push(&self, value: T) -> Result<(), T> {
        if self.meter.admit_precharged().is_err() {
            return Err(value);
        }
        let mut tail = self.tail.0.load(Ordering::Relaxed);
        #[cfg(test)]
        {
            let pause = self
                .before_claim
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
            if let Some(pause) = pause {
                pause.entered.wait();
                pause.resume.wait();
            }
        }
        loop {
            if self.meter.is_quarantined() {
                return Err(value);
            }
            let index = tail & self.index_mask;
            let slot = &self.slots[index];
            let stamp = slot.stamp.load(Ordering::Acquire);
            if stamp != tail {
                // A stamp mismatch alone is not Full: another producer may
                // already have advanced our cached tail. Only the matching
                // previous-lap stamp AND original head establish actual Full.
                if stamp.wrapping_add(self.lap) == tail + 1 {
                    fence(Ordering::SeqCst);
                    if self.head.0.load(Ordering::Relaxed).wrapping_add(self.lap) == tail {
                        return Err(value);
                    }
                }
                tail = self.tail.0.load(Ordering::Relaxed);
                std::hint::spin_loop();
                continue;
            }
            match self.tail.0.compare_exchange_weak(
                tail,
                self.next(tail, index),
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    #[cfg(test)]
                    {
                        let pause = self
                            .pause
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .take();
                        if let Some(pause) = pause {
                            pause.entered.wait();
                            pause.resume.wait();
                        }
                    }
                    // One uninterrupted, infallible publication. No exposed
                    // physical permit, allocation, destructor, callback or await.
                    unsafe {
                        (*slot.value.get()).write(value);
                    }
                    slot.stamp.store(tail + 1, Ordering::Release);
                    return Ok(());
                }
                Err(actual) => tail = actual,
            }
        }
    }
    /// Only the enclosing Native Core may invoke this after consuming its own
    /// admitted Unit. Unlike a fresh try_push this cannot reject an old Permit.
    pub(super) fn publish_admitted(&self, value: T) {
        let mut tail = self.tail.0.load(Ordering::Relaxed);
        loop {
            let index = tail & self.index_mask;
            let slot = &self.slots[index];
            if slot.stamp.load(Ordering::Acquire) == tail {
                if self
                    .tail
                    .0
                    .compare_exchange_weak(
                        tail,
                        self.next(tail, index),
                        Ordering::SeqCst,
                        Ordering::Relaxed,
                    )
                    .is_ok()
                {
                    #[cfg(test)]
                    {
                        let pause = self
                            .pause
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .take();
                        if let Some(pause) = pause {
                            pause.entered.wait();
                            pause.resume.wait();
                        }
                    }
                    unsafe {
                        (*slot.value.get()).write(value);
                    }
                    slot.stamp.store(tail + 1, Ordering::Release);
                    return;
                }
            }
            // An admitted unit proves room; read credit is released only after
            // the free stamp. Retry competing publishers, never wait for a reader.
            tail = self.tail.0.load(Ordering::Relaxed);
            std::hint::spin_loop();
        }
    }
    /// Caller must exclude every publisher/reader for the complete inspection.
    pub(super) unsafe fn inventory(&self, bytes: impl Fn(&T) -> usize) -> Option<(usize, usize)> {
        let head = self.head.0.load(Ordering::Acquire);
        let tail = self.tail.0.load(Ordering::Acquire);
        let mut cursor = head;
        let mut count = None;
        for n in 0..=self.slots.len() {
            if cursor == tail {
                count = Some(n);
                break;
            }
            let index = cursor & self.index_mask;
            if index >= self.slots.len() {
                return None;
            }
            cursor = self.next(cursor, index);
        }
        let count = count?;
        let mut total = 0usize;
        cursor = head;
        for n in 0..self.slots.len() {
            let index = cursor & self.index_mask;
            if index >= self.slots.len() {
                return None;
            }
            let slot = &self.slots[index];
            let expected = if n < count {
                cursor.wrapping_add(1)
            } else {
                cursor
            };
            if slot.stamp.load(Ordering::Acquire) != expected {
                return None;
            }
            if n < count {
                total =
                    total.checked_add(bytes(unsafe { (*slot.value.get()).assume_init_ref() }))?;
            }
            cursor = self.next(cursor, index);
        }
        Some((count, total))
    }
    pub(super) fn pop(&self) -> Pop<T> {
        let mut head = self.head.0.load(Ordering::Relaxed);
        loop {
            let index = head & self.index_mask;
            let slot = &self.slots[index];
            let stamp = slot.stamp.load(Ordering::Acquire);
            if stamp != head + 1 {
                // A publisher may have advanced tail without publishing head.
                // Never spin on that thread or treat the hole as terminal EOF.
                fence(Ordering::SeqCst);
                return if self.tail.0.load(Ordering::Relaxed) == head {
                    Pop::Empty
                } else {
                    Pop::InFlight
                };
            }
            match self.head.0.compare_exchange_weak(
                head,
                self.next(head, index),
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    let value = unsafe { (*slot.value.get()).assume_init_read() };
                    slot.stamp
                        .store(head.wrapping_add(self.lap), Ordering::Release);
                    return Pop::Value(value);
                }
                Err(actual) => head = actual,
            }
        }
    }
    fn empty_inventory(&mut self) -> bool {
        // Exclusive final owner: no concurrent publisher/reader. Check actual
        // stamps in both directions, not merely matching logical cursors.
        let head = *self.head.0.get_mut();
        if head != *self.tail.0.get_mut() {
            return false;
        }
        let index = head & self.index_mask;
        if index >= self.slots.len() {
            return false;
        }
        let lap = head & !(self.lap - 1);
        self.slots.iter_mut().enumerate().all(|(i, slot)| {
            let expected = if i < index {
                lap.wrapping_add(self.lap)
            } else {
                lap
            } + i;
            *slot.stamp.get_mut() == expected
        })
    }
}
impl<T> Drop for Ring<T> {
    fn drop(&mut self) {
        if self.empty_inventory() {
            // Slot<T> holds MaybeUninit, so this cannot call an opaque T Drop.
            unsafe {
                ManuallyDrop::drop(&mut self.slots);
                ManuallyDrop::drop(&mut self.stock);
            }
        } else {
            // Enclosing lane must perform explicit guarded value disposition.
            // If it did not, retain the actual allocation AND original charge.
            self.meter.quarantine();
        }
    }
}
