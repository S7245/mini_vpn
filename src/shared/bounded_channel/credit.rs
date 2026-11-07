//! Native lane's unit-credit FIFO and fixed callback custody. An acquire holds
//! a cold record until both its future and any detached callback have ended.
//! No future pointer escapes, no reserve allocates, and uncontended credit uses
//! atomics. This owner grants logical units only, never ring slots or Flow ACKs.
//! Tests: ../tests/credit.rs. The channel owner must return committed units only
//! after its actual ring read, and keep ordinary close distinct from quarantine.
use crate::shared::errors::FailureClock;
use bytes::{BackingBudgetReservation, BackingMeter, MeteredCopyError};
use std::{
    alloc::{Layout, alloc},
    any::Any,
    future::Future,
    mem::ManuallyDrop,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
};

const CLOSED: usize = 1 << (usize::BITS - 1);
const WAITERS: usize = CLOSED >> 1;
const COUNT: usize = WAITERS - 1;
const NONE: usize = usize::MAX;
type Payload = ManuallyDrop<Box<dyn Any + Send>>;

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ReserveError {
    Closed,
    Capacity,
    Quarantined,
}
impl ReserveError {
    pub(crate) fn reason(&self) -> &'static str {
        match self {
            Self::Closed => "global_rx_closed",
            Self::Capacity => "global_rx_capacity",
            Self::Quarantined => "global_rx_quarantined",
        }
    }
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Free,
    Registering,
    Queued,
    Granted,
    Closed,
    Transferred,
}
struct Record {
    phase: Phase,
    future: bool,
    callback: bool,
    next: usize,
    prev: usize,
    waker: Option<Waker>,
    // One future can clone/replace while the sole detached callback wakes/drops.
    // Q ends registration; six cells include final owner retirement as well.
    failures: [Option<Payload>; 6],
}
impl Record {
    fn empty(next: usize) -> Self {
        Self {
            phase: Phase::Free,
            future: false,
            callback: false,
            next,
            prev: NONE,
            waker: None,
            failures: std::array::from_fn(|_| None),
        }
    }
}
struct Queue {
    records: Box<[Record]>,
    free: usize,
    head: usize,
    tail: usize,
}
struct Core {
    units: AtomicUsize,
    driver: AtomicUsize,
    capacity: usize,
    queue: Mutex<Queue>,
    meter: BackingMeter,
    clock: FailureClock,
}
struct Inner {
    core: Box<Core>,
    // Both physical allocations precede both refund tokens in drop order.
    _core_stock: Option<BackingBudgetReservation>,
    _record_stock: Option<BackingBudgetReservation>,
}
pub(super) struct Credit(Arc<Inner>);
pub(super) struct Acquire<'a> {
    credit: &'a Credit,
    record: Option<usize>,
    done: bool,
}
pub(super) struct Unit<'a> {
    credit: &'a Credit,
    returned: bool,
}

impl Credit {
    pub(super) fn layout_bytes(records: usize) -> Result<usize, MeteredCopyError> {
        Layout::array::<Record>(records)
            .ok()
            .and_then(|layout| layout.size().checked_add(Layout::new::<Core>().size()))
            .ok_or(MeteredCopyError::TooLarge)
    }
    pub(super) fn prepare(
        capacity: usize,
        records: usize,
        meter: &BackingMeter,
        clock: FailureClock,
    ) -> Result<Self, MeteredCopyError> {
        if !clock.bound_to_meter(meter) {
            meter.quarantine();
            return Err(MeteredCopyError::Quarantined);
        }
        if capacity == 0 || capacity > COUNT || records == 0 || records > 4096 {
            return Err(MeteredCopyError::TooLarge);
        }
        let layout = Layout::array::<Record>(records).map_err(|_| MeteredCopyError::TooLarge)?;
        let record_stock = meter.reserve_cold_layout(layout.size())?;
        let core_stock = meter.reserve_cold_layout(Layout::new::<Core>().size())?;
        let raw = unsafe { alloc(layout) }.cast::<Record>();
        if raw.is_null() {
            return Err(MeteredCopyError::Allocation);
        }
        for i in 0..records {
            unsafe {
                raw.add(i)
                    .write(Record::empty(if i + 1 < records { i + 1 } else { NONE }));
            }
        }
        let records = unsafe { Box::from_raw(std::ptr::slice_from_raw_parts_mut(raw, records)) };
        let raw = unsafe { alloc(Layout::new::<Core>()) }.cast::<Core>();
        if raw.is_null() {
            return Err(MeteredCopyError::Allocation);
        }
        let core = unsafe {
            raw.write(Core {
                units: AtomicUsize::new(capacity),
                driver: AtomicUsize::new(0),
                capacity,
                queue: Mutex::new(Queue {
                    records,
                    free: 0,
                    head: NONE,
                    tail: NONE,
                }),
                meter: meter.clone(),
                clock,
            });
            Box::from_raw(raw)
        };
        // Initialize any platform mutex storage during cold preparation.
        drop(
            core.queue
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        Ok(Self(Arc::new(Inner {
            core,
            _core_stock: core_stock,
            _record_stock: record_stock,
        })))
    }
    pub(super) fn is_closed(&self) -> bool {
        self.0.core.units.load(Ordering::Acquire) & CLOSED != 0
    }
    pub(super) fn runner_idle(&self) -> bool {
        self.0.core.driver.load(Ordering::Acquire) == 0
    }
    pub(super) fn settled_inventory(&self) -> bool {
        let core = &self.0.core;
        let queue = core.lock();
        if core.units.load(Ordering::Acquire) & COUNT != core.capacity
            || core.driver.load(Ordering::Acquire) != 0
            || queue.head != NONE
            || queue.tail != NONE
        {
            return false;
        }
        let mut cursor = queue.free;
        let mut count = 0;
        while cursor != NONE {
            let Some(record) = queue.records.get(cursor) else {
                return false;
            };
            if record.phase != Phase::Free
                || record.future
                || record.callback
                || record.waker.is_some()
                || record.failures.iter().any(Option::is_some)
            {
                return false;
            }
            count += 1;
            if count > queue.records.len() {
                return false;
            }
            cursor = record.next;
        }
        count == queue.records.len()
    }
    pub(super) fn available(&self) -> usize {
        self.0.core.units.load(Ordering::Acquire) & COUNT
    }
    pub(super) fn try_acquire(&self) -> Result<Unit<'_>, ReserveError> {
        self.0.core.take_fast()?;
        if self.0.core.meter.admit_precharged().is_err() {
            self.0.release();
            return Err(ReserveError::Quarantined);
        }
        Ok(Unit {
            credit: self,
            returned: false,
        })
    }
    pub(super) fn acquire(&self) -> Acquire<'_> {
        Acquire {
            credit: self,
            record: None,
            done: false,
        }
    }
    pub(super) fn close(&self) {
        self.0
            .core
            .units
            .fetch_or(CLOSED | WAITERS, Ordering::AcqRel);
        self.0.drive();
    }
    pub(super) fn release(&self) {
        self.0.release();
    }
}
impl Core {
    fn lock(&self) -> MutexGuard<'_, Queue> {
        self.queue.lock().unwrap_or_else(|e| {
            self.meter.quarantine();
            e.into_inner()
        })
    }
    fn take_fast(&self) -> Result<(), ReserveError> {
        let mut value = self.units.load(Ordering::Acquire);
        loop {
            if self.meter.is_quarantined() {
                return Err(ReserveError::Quarantined);
            }
            if value & CLOSED != 0 {
                return Err(ReserveError::Closed);
            }
            if value & WAITERS != 0 || value & COUNT == 0 {
                return Err(ReserveError::Capacity);
            }
            match self.units.compare_exchange_weak(
                value,
                value - 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(()),
                Err(actual) => value = actual,
            }
        }
    }
    fn capture(&self, index: usize, payload: Box<dyn Any + Send>) {
        let mut queue = self.lock();
        if let Some(slot) = queue.records[index]
            .failures
            .iter_mut()
            .find(|slot| slot.is_none())
        {
            *slot = Some(ManuallyDrop::new(payload));
        } else {
            std::mem::forget(payload);
        }
        let publication = self.clock.panic_deferred();
        self.meter.quarantine();
        drop(queue);
        drop(publication);
    }
    fn dispose(&self, index: usize, waker: Option<Waker>) {
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(waker))) {
            self.capture(index, payload);
        }
    }
    fn clone_waker(&self, index: usize, waker: &Waker) -> Option<Waker> {
        match catch_unwind(AssertUnwindSafe(|| waker.clone())) {
            Ok(waker) => Some(waker),
            Err(payload) => {
                self.capture(index, payload);
                None
            }
        }
    }
    fn unlink(&self, queue: &mut Queue, index: usize) {
        let (prev, next) = (queue.records[index].prev, queue.records[index].next);
        if prev == NONE {
            queue.head = next;
        } else {
            queue.records[prev].next = next;
        }
        if next == NONE {
            queue.tail = prev;
        } else {
            queue.records[next].prev = prev;
        }
        queue.records[index].next = NONE;
        queue.records[index].prev = NONE;
        if queue.head == NONE {
            self.units.fetch_and(!WAITERS, Ordering::AcqRel);
        }
    }
    fn recycle(&self, queue: &mut Queue, index: usize) {
        let record = &mut queue.records[index];
        if !record.future
            && !record.callback
            && record.waker.is_none()
            && record.failures.iter().all(Option::is_none)
        {
            *record = Record::empty(queue.free);
            queue.free = index;
        }
    }
}
impl Inner {
    fn release(&self) {
        let old = self.core.units.fetch_add(1, Ordering::AcqRel);
        if old & COUNT >= self.core.capacity {
            self.core.meter.quarantine();
            return;
        }
        if old & WAITERS != 0 {
            self.drive();
        }
    }
    fn drive(&self) {
        const ACTIVE: usize = 1;
        const AGAIN: usize = 2;
        // One original owner runner: inline wake/Drop may return another unit,
        // but only request another pass, never recurse into another callback.
        if self.core.driver.fetch_or(ACTIVE | AGAIN, Ordering::AcqRel) & ACTIVE != 0 {
            return;
        }
        loop {
            self.core.driver.fetch_and(!AGAIN, Ordering::AcqRel);
            self.drive_ready();
            if self
                .core
                .driver
                .compare_exchange(ACTIVE, 0, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return;
            }
        }
    }
    fn drive_ready(&self) {
        loop {
            let (index, waker) = {
                let mut queue = self.core.lock();
                let units = self.core.units.load(Ordering::Acquire);
                if queue.head == NONE {
                    return;
                }
                let closed = units & CLOSED != 0 || self.core.meter.is_quarantined();
                if !closed && units & COUNT == 0 {
                    return;
                }
                let index = queue.head;
                if !closed {
                    self.core.units.fetch_sub(1, Ordering::AcqRel);
                }
                // Keep WAITERS set until the grant is committed under this lock.
                queue.records[index].phase = if closed {
                    Phase::Closed
                } else {
                    Phase::Granted
                };
                self.core.unlink(&mut queue, index);
                let record = &mut queue.records[index];
                record.callback = true;
                (index, record.waker.take())
            };
            // No pointer to the acquire future survives unlock. Its cancellation
            // may return its unit now, but cannot recycle this callback record.
            if let Some(waker) = waker.as_ref() {
                if !std::thread::panicking() {
                    if let Err(payload) = catch_unwind(AssertUnwindSafe(|| waker.wake_by_ref())) {
                        self.core.capture(index, payload);
                    }
                } else {
                    self.core.meter.quarantine();
                }
            }
            self.core.dispose(index, waker);
            let mut queue = self.core.lock();
            queue.records[index].callback = false;
            self.core.recycle(&mut queue, index);
        }
    }
}
impl<'a> Future for Acquire<'a> {
    type Output = Result<Unit<'a>, ReserveError>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let core = &this.credit.0.core;
        if this.done {
            return Poll::Ready(Err(ReserveError::Closed));
        }
        if core.meter.is_quarantined() {
            return this.quarantined();
        }
        if this.record.is_none() {
            match this.credit.try_acquire() {
                Ok(unit) => {
                    this.done = true;
                    return Poll::Ready(Ok(unit));
                }
                Err(ReserveError::Capacity) => {}
                Err(error) => {
                    this.done = true;
                    return Poll::Ready(Err(error));
                }
            }
            let mut queue = core.lock();
            // Freeze fast acquisitions before the final availability recheck.
            core.units.fetch_or(WAITERS, Ordering::AcqRel);
            let units = core.units.load(Ordering::Acquire);
            if queue.head == NONE
                && units & COUNT != 0
                && units & CLOSED == 0
                && !core.meter.is_quarantined()
            {
                core.units.fetch_sub(1, Ordering::AcqRel);
                core.units.fetch_and(!WAITERS, Ordering::AcqRel);
                drop(queue);
                if core.meter.admit_precharged().is_err() {
                    this.credit.0.release();
                    return this.quarantined();
                }
                this.done = true;
                return Poll::Ready(Ok(Unit {
                    credit: this.credit,
                    returned: false,
                }));
            }
            let index = queue.free;
            if index == NONE || units & CLOSED != 0 || core.meter.is_quarantined() {
                if queue.head == NONE {
                    core.units.fetch_and(!WAITERS, Ordering::AcqRel);
                }
                this.done = true;
                return Poll::Ready(Err(if core.meter.is_quarantined() {
                    ReserveError::Quarantined
                } else if units & CLOSED != 0 {
                    ReserveError::Closed
                } else {
                    ReserveError::Capacity
                }));
            }
            queue.free = queue.records[index].next;
            let record = &mut queue.records[index];
            record.next = NONE;
            record.phase = Phase::Registering;
            record.future = true;
            this.record = Some(index);
        }
        let index = this.record.unwrap_or(NONE);
        let old = {
            let mut queue = core.lock();
            if core.meter.is_quarantined() {
                drop(queue);
                return this.quarantined();
            }
            match queue.records[index].phase {
                Phase::Granted => {
                    if core.meter.admit_precharged().is_err() {
                        drop(queue);
                        return this.quarantined();
                    }
                    if core.meter.admit_precharged().is_err() {
                        drop(queue);
                        return this.quarantined();
                    }
                    queue.records[index].phase = Phase::Transferred;
                    this.done = true;
                    return Poll::Ready(Ok(Unit {
                        credit: this.credit,
                        returned: false,
                    }));
                }
                Phase::Closed => {
                    this.done = true;
                    return Poll::Ready(Err(if core.meter.is_quarantined() {
                        ReserveError::Quarantined
                    } else {
                        ReserveError::Closed
                    }));
                }
                Phase::Queued
                    if queue.records[index]
                        .waker
                        .as_ref()
                        .is_some_and(|w| w.will_wake(cx.waker())) =>
                {
                    return Poll::Pending;
                }
                _ => {}
            }
            queue.records[index].waker.take()
        };
        // Clone and displaced Drop are isolated. The future keeps this record
        // leased even if a release concurrently detaches its old callback.
        let candidate = core.clone_waker(index, cx.waker());
        core.dispose(index, old);
        let unused = {
            let mut queue = core.lock();
            if queue.records[index].phase == Phase::Registering {
                let tail = queue.tail;
                queue.records[index].prev = tail;
                queue.records[index].phase = Phase::Queued;
                if tail == NONE {
                    queue.head = index;
                } else {
                    queue.records[tail].next = index;
                }
                queue.tail = index;
                core.units.fetch_or(WAITERS, Ordering::AcqRel);
            }
            if queue.records[index].phase == Phase::Queued {
                queue.records[index].waker = candidate;
                None
            } else {
                candidate
            }
        };
        core.dispose(index, unused);
        this.credit.0.drive();
        // drive may synchronously grant/close us. Returning Ready avoids relying
        // on a callback whose clone failed; a real queued registration is Pending.
        let mut queue = core.lock();
        if core.meter.is_quarantined() {
            drop(queue);
            return this.quarantined();
        }
        match queue.records[index].phase {
            Phase::Granted => {
                queue.records[index].phase = Phase::Transferred;
                this.done = true;
                Poll::Ready(Ok(Unit {
                    credit: this.credit,
                    returned: false,
                }))
            }
            Phase::Closed => {
                this.done = true;
                Poll::Ready(Err(if core.meter.is_quarantined() {
                    ReserveError::Quarantined
                } else {
                    ReserveError::Closed
                }))
            }
            _ => Poll::Pending,
        }
    }
}
impl<'a> Acquire<'a> {
    fn quarantined(&mut self) -> Poll<Result<Unit<'a>, ReserveError>> {
        self.cancel();
        self.done = true;
        Poll::Ready(Err(ReserveError::Quarantined))
    }
    fn cancel(&mut self) {
        let Some(index) = self.record.take() else {
            return;
        };
        let core = &self.credit.0.core;
        let (release, waker) = {
            let mut queue = core.lock();
            let phase = queue.records[index].phase;
            if phase == Phase::Queued {
                core.unlink(&mut queue, index);
            } else if phase == Phase::Registering && queue.head == NONE {
                core.units.fetch_and(!WAITERS, Ordering::AcqRel);
            }
            queue.records[index].phase = Phase::Transferred;
            (phase == Phase::Granted, queue.records[index].waker.take())
        };
        core.dispose(index, waker);
        {
            let mut queue = core.lock();
            queue.records[index].future = false;
            core.recycle(&mut queue, index);
        }
        if release {
            self.credit.0.release();
        }
    }
}
impl Drop for Acquire<'_> {
    fn drop(&mut self) {
        self.cancel();
    }
}
impl Unit<'_> {
    pub(super) fn commit(mut self) {
        self.returned = true;
    }
}
impl Drop for Unit<'_> {
    fn drop(&mut self) {
        if !self.returned {
            self.credit.0.release();
        }
    }
}
impl Drop for Inner {
    fn drop(&mut self) {
        // No acquire or runner can outlive the owning Credit borrow. A nonempty
        // registry here is an interrupted transaction, never a clean refund.
        let core = &self.core;
        if core.units.load(Ordering::Acquire) & COUNT != core.capacity {
            core.meter.quarantine();
        }
        let len = core.lock().records.len();
        for index in 0..len {
            let waker = {
                let mut queue = core.lock();
                if queue.records[index].future || queue.records[index].callback {
                    core.meter.quarantine();
                }
                queue.records[index].waker.take()
            };
            core.dispose(index, waker);
        }
    }
}
