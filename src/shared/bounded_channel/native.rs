//! One Native global lane: cold physical stock, admitted publication, receiver
//! notification and final disposition. Flow ACK and Session deadlines stay with
//! their original owners. Only the unique receiver or closed-lane drain runner
//! touches held; a non-clone observer keeps the allocation through retirement.
//! Entry: prepare; private behavior tracers live in ../tests/native.rs.
use super::{
    Value,
    credit::{Credit, ReserveError, Unit},
    ring::{Pop, Ring},
};
use crate::shared::errors::FailureClock;
use bytes::{BackingBudgetReservation, BackingMeter, MeteredCopyError};
use std::{
    alloc::{Layout, alloc},
    any::Any,
    cell::UnsafeCell,
    mem::ManuallyDrop,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
};

const ACTIVE: usize = 1;
const AGAIN: usize = 2;
const REGISTERED: usize = 4;
type Payload = ManuallyDrop<Box<dyn Any + Send>>;
#[derive(Clone, Copy)]
pub(super) enum Site {
    Clone,
    Replace,
    Wake,
    WakeDrop,
    Retire,
    Value,
    ContextHead,
    ContextTail,
}
struct Callbacks {
    waiter: Option<Waker>,
    failures: [Option<Payload>; 8],
}
pub(crate) struct Fault<'a> {
    meter: &'a BackingMeter,
    clock: &'a FailureClock,
    callbacks: &'a Mutex<Callbacks>,
}
impl Fault<'_> {
    pub(crate) fn failed(&self) -> bool {
        self.meter.is_quarantined()
    }
    fn capture(&self, site: Site, payload: Box<dyn Any + Send>) {
        let mut callbacks = self.callbacks.lock().unwrap_or_else(|e| e.into_inner());
        let slot = &mut callbacks.failures[site as usize];
        if slot.is_none() {
            *slot = Some(ManuallyDrop::new(payload));
        } else {
            std::mem::forget(payload);
        }
        let publication = self.clock.panic_deferred();
        self.meter.quarantine();
        drop(callbacks);
        drop(publication);
    }
    pub(crate) fn dispose<T>(&self, value: T) {
        self.dispose_at(Site::Value, value);
    }
    pub(crate) fn dispose_context_head<T>(&self, value: T) {
        self.dispose_at(Site::ContextHead, value);
    }
    pub(crate) fn dispose_context_tail<T>(&self, value: T) {
        self.dispose_at(Site::ContextTail, value);
    }
    fn dispose_at<T>(&self, site: Site, value: T) {
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(value))) {
            self.capture(site, payload);
        }
    }
}
struct Core<T: Value> {
    // Drain only borrows; only final quiescence may take this original context.
    disposition: UnsafeCell<Option<ManuallyDrop<T::Disposition>>>,
    ring: Ring<T>,
    credit: Credit,
    capacity: usize,
    senders: AtomicUsize,
    receiver_closed: AtomicBool,
    operations: AtomicUsize,
    notification: AtomicUsize,
    drain: AtomicUsize,
    messages: AtomicUsize,
    bytes: AtomicUsize,
    held: UnsafeCell<Option<ManuallyDrop<T>>>,
    invalid: Mutex<Option<std::io::Error>>,
    callbacks: Mutex<Callbacks>,
    meter: BackingMeter,
    clock: FailureClock,
}
// The receiver is unique; after receiver_closed only the single drain runner
// accesses held. Inventory accesses it only at final quiescence. Ring provides
// its own slot ownership. No reference to a T escapes shared access.
unsafe impl<T: Value> Sync for Core<T> {}
struct Inner<T: Value> {
    core: ManuallyDrop<Box<Core<T>>>,
    stock: ManuallyDrop<Option<BackingBudgetReservation>>,
}
pub(super) struct Sender<T: Value>(Arc<Inner<T>>);
pub(super) struct Receiver<T: Value>(Arc<Inner<T>>);
pub(super) struct WeakSender<T: Value>(Weak<Inner<T>>);
pub(crate) struct Observer<T: Value>(Arc<Inner<T>>);
// Unit is dropped before notification. Returning the last old granted Unit
// must wake a receiver awaiting close EOF even while Sender remains alive.
pub(super) struct Permit<'a, T: Value> {
    unit: Unit<'a>,
    notify: ReturnNotify<'a, T>,
}
struct ReturnNotify<'a, T: Value>(&'a Sender<T>);
impl<T: Value> Drop for ReturnNotify<'_, T> {
    fn drop(&mut self) {
        let core = &self.0.0.core;
        let _op = core.operation();
        core.fire();
        core.drain_closed();
    }
}

struct Operation<'a>(&'a AtomicUsize);
impl Drop for Operation<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Release);
    }
}
impl<T: Value> Core<T> {
    fn operation(&self) -> Operation<'_> {
        self.operations.fetch_add(1, Ordering::AcqRel);
        Operation(&self.operations)
    }
    fn fault(&self) -> Fault<'_> {
        Fault {
            meter: &self.meter,
            clock: &self.clock,
            callbacks: &self.callbacks,
        }
    }
    fn callbacks(&self) -> std::sync::MutexGuard<'_, Callbacks> {
        self.callbacks.lock().unwrap_or_else(|e| {
            self.meter.quarantine();
            e.into_inner()
        })
    }
    fn fire(&self) {
        let previous = self.notification.fetch_or(AGAIN, Ordering::AcqRel);
        if previous & REGISTERED == 0 || previous & ACTIVE != 0 {
            return;
        }
        loop {
            let flags = self.notification.load(Ordering::Acquire);
            if flags & ACTIVE != 0 || flags & REGISTERED == 0 {
                return;
            }
            if self
                .notification
                .compare_exchange(flags, flags | ACTIVE, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                break;
            }
        }
        loop {
            self.notification.fetch_and(!AGAIN, Ordering::AcqRel);
            let waiter = {
                let mut state = self.callbacks();
                self.notification.fetch_and(!REGISTERED, Ordering::AcqRel);
                state.waiter.take()
            };
            if let Some(waiter) = waiter {
                if !self.meter.is_quarantined() && !std::thread::panicking() {
                    if let Err(payload) = catch_unwind(AssertUnwindSafe(|| waiter.wake_by_ref())) {
                        self.fault().capture(Site::Wake, payload);
                    }
                }
                self.fault().dispose_at(Site::WakeDrop, waiter);
            }
            let flags = self.notification.load(Ordering::Acquire);
            if flags & (AGAIN | REGISTERED) == AGAIN | REGISTERED && !self.meter.is_quarantined() {
                continue;
            }
            if self
                .notification
                .compare_exchange(flags, flags & !ACTIVE, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return;
            }
        }
    }
    fn register(&self, waker: &Waker) {
        if self.meter.is_quarantined() {
            return;
        }
        if self
            .callbacks()
            .waiter
            .as_ref()
            .is_some_and(|old| old.will_wake(waker))
        {
            return;
        }
        let candidate = match catch_unwind(AssertUnwindSafe(|| waker.clone())) {
            Ok(waker) => waker,
            Err(payload) => {
                self.fault().capture(Site::Clone, payload);
                return;
            }
        };
        let old = {
            let mut state = self.callbacks();
            if self.meter.is_quarantined() {
                Some(candidate)
            } else {
                let old = state.waiter.replace(candidate);
                self.notification.fetch_or(REGISTERED, Ordering::AcqRel);
                old
            }
        };
        self.fault().dispose_at(Site::Replace, old);
    }
    fn retire_waiter(&self) {
        let old = {
            let mut state = self.callbacks();
            self.notification.fetch_and(!REGISTERED, Ordering::AcqRel);
            state.waiter.take()
        };
        self.fault().dispose_at(Site::Retire, old);
    }
    // Only called by the unique Receiver, or by the post-close drain runner.
    fn take(&self) -> Pop<T> {
        if self.meter.is_quarantined() {
            return Pop::Empty;
        }
        let value = match self.ring.pop() {
            Pop::Value(value) => value,
            other => return other,
        };
        // Publish actual selected custody BEFORE credit release can call a Waker.
        let held = unsafe { &mut *self.held.get() };
        *held = Some(ManuallyDrop::new(value));
        if self.notification.load(Ordering::Acquire) & REGISTERED != 0 {
            self.retire_waiter();
        }
        self.credit.release(); // pop already stored the free stamp with Release
        if self.meter.is_quarantined() {
            return Pop::Empty;
        }
        let Some(value) = held.take() else {
            self.meter.quarantine();
            return Pop::Empty;
        };
        let value = ManuallyDrop::into_inner(value);
        self.messages.fetch_sub(1, Ordering::AcqRel);
        self.bytes.fetch_sub(value.lane_bytes(), Ordering::AcqRel);
        Pop::Value(value)
    }
    fn drain_closed(&self) {
        if !self.receiver_closed.load(Ordering::Acquire) {
            return;
        }
        if self.drain.fetch_or(ACTIVE | AGAIN, Ordering::AcqRel) & ACTIVE != 0 {
            return;
        }
        loop {
            self.drain.fetch_and(!AGAIN, Ordering::AcqRel);
            while !self.meter.is_quarantined() {
                let Pop::Value(value) = self.ring.pop() else {
                    break;
                };
                let size = value.lane_bytes();
                // The single drain runner excludes final context retirement.
                let context = unsafe { &*self.disposition.get() };
                let Some(context) = context.as_ref() else {
                    unsafe {
                        *self.held.get() = Some(ManuallyDrop::new(value));
                    }
                    self.meter.quarantine();
                    break;
                };
                let remainder = value.dispose(&self.fault(), context);
                if let Some(value) = remainder {
                    unsafe {
                        *self.held.get() = Some(ManuallyDrop::new(value));
                    }
                }
                if self.meter.is_quarantined() {
                    break;
                }
                self.messages.fetch_sub(1, Ordering::AcqRel);
                self.bytes.fetch_sub(size, Ordering::AcqRel);
                self.credit.release();
            }
            if self
                .drain
                .compare_exchange(ACTIVE, 0, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                break;
            }
        }
    }
    fn quiescent(&self) -> bool {
        self.receiver_closed.load(Ordering::Acquire)
            && self.senders.load(Ordering::Acquire) == 0
            && self.operations.load(Ordering::Acquire) == 0
            && self.notification.load(Ordering::Acquire) & ACTIVE == 0
            && self.drain.load(Ordering::Acquire) & ACTIVE == 0
    }
    fn retire_context(&self) {
        if !self.clean_inventory() {
            return;
        }
        // senders=0 is permanent: Weak upgrade cannot resurrect it. Receiver,
        // publication, drain and every callback are quiescent. Only the unique
        // observer (or final Inner::drop) can now touch the context.
        let context = unsafe { &mut *self.disposition.get() };
        if let Some(value) = context.take() {
            if let Some(remainder) =
                T::dispose_context(ManuallyDrop::into_inner(value), &self.fault())
            {
                *context = Some(ManuallyDrop::new(remainder));
                self.meter.quarantine();
            }
        }
    }
    fn clean_inventory(&self) -> bool {
        if !self.quiescent() || self.meter.is_quarantined() {
            return false;
        }
        // No Sender/Permit or receiver/runner can now access the ring/held.
        let stock = unsafe { self.ring.inventory(Value::lane_bytes) };
        let held_empty = unsafe { (&*self.held.get()).is_none() };
        let callbacks = self.callbacks();
        let clean = stock == Some((0, 0))
            && held_empty
            && self.messages.load(Ordering::Acquire) == 0
            && self.bytes.load(Ordering::Acquire) == 0
            && self.credit.settled_inventory()
            && callbacks.waiter.is_none()
            && callbacks.failures.iter().all(Option::is_none);
        if !clean {
            self.meter.quarantine();
        }
        clean
    }
}
pub(super) fn layout_bytes<T: Value>(
    capacity: usize,
    records: usize,
) -> Result<usize, MeteredCopyError> {
    Ring::<T>::layout_bytes(capacity)?
        .checked_add(Credit::layout_bytes(records)?)
        .and_then(|n| n.checked_add(Layout::new::<Core<T>>().size()))
        .ok_or(MeteredCopyError::TooLarge)
}
pub(super) fn prepare<T: Value<Disposition = ()>>(
    capacity: usize,
    records: usize,
    meter: &BackingMeter,
    clock: FailureClock,
) -> Result<(Sender<T>, Receiver<T>, Observer<T>), MeteredCopyError> {
    prepare_with(capacity, records, meter, clock, &mut Some(()))
}
pub(super) fn prepare_with<T: Value>(
    capacity: usize,
    records: usize,
    meter: &BackingMeter,
    clock: FailureClock,
    disposition: &mut Option<T::Disposition>,
) -> Result<(Sender<T>, Receiver<T>, Observer<T>), MeteredCopyError> {
    if disposition.is_none() || !clock.bound_to_meter(meter) {
        meter.quarantine();
        return Err(MeteredCopyError::Quarantined);
    }
    let stock = meter.reserve_cold_layout(Layout::new::<Core<T>>().size())?;
    let ring = Ring::prepare(capacity, meter)?;
    let credit = Credit::prepare(capacity, records, meter, clock.clone())?;
    let pointer = unsafe { alloc(Layout::new::<Core<T>>()) }.cast::<Core<T>>();
    if pointer.is_null() {
        return Err(MeteredCopyError::Allocation);
    }
    let core = unsafe {
        pointer.write(Core {
            disposition: UnsafeCell::new(disposition.take().map(ManuallyDrop::new)),
            ring,
            credit,
            capacity,
            senders: AtomicUsize::new(1),
            receiver_closed: AtomicBool::new(false),
            operations: AtomicUsize::new(0),
            notification: AtomicUsize::new(0),
            drain: AtomicUsize::new(0),
            messages: AtomicUsize::new(0),
            bytes: AtomicUsize::new(0),
            held: UnsafeCell::new(None),
            invalid: Mutex::new(None),
            callbacks: Mutex::new(Callbacks {
                waiter: None,
                failures: std::array::from_fn(|_| None),
            }),
            meter: meter.clone(),
            clock,
        });
        Box::from_raw(pointer)
    };
    drop(core.callbacks()); // initialize the platform mutex before publication
    let inner = Arc::new(Inner {
        core: ManuallyDrop::new(core),
        stock: ManuallyDrop::new(stock),
    });
    Ok((
        Sender(inner.clone()),
        Receiver(inner.clone()),
        Observer(inner),
    ))
}
impl<T: Value> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.0.core.senders.fetch_add(1, Ordering::AcqRel);
        Self(self.0.clone())
    }
}
impl<T: Value> Sender<T> {
    pub(super) fn close_admission(&self) { self.0.core.credit.close(); }
    pub(super) fn context(&self) -> (&BackingMeter, &FailureClock) {
        (&self.0.core.meter, &self.0.core.clock)
    }
    pub(super) fn capacity(&self) -> usize {
        self.0.core.credit.available()
    }
    pub(super) fn max_capacity(&self) -> usize {
        self.0.core.capacity
    }
    pub(super) fn is_closed(&self) -> bool {
        self.0.core.credit.is_closed() || self.0.core.meter.is_quarantined()
    }
    pub(super) fn downgrade(&self) -> WeakSender<T> {
        WeakSender(Arc::downgrade(&self.0))
    }
    pub(super) async fn reserve(&self) -> Result<Permit<'_, T>, ReserveError> {
        let unit = self.0.core.credit.acquire().await?;
        Ok(Permit {
            unit,
            notify: ReturnNotify(self),
        })
    }
    pub(super) fn try_reserve(&self) -> Result<Permit<'_, T>, ReserveError> {
        Ok(Permit {
            unit: self.0.core.credit.try_acquire()?,
            notify: ReturnNotify(self),
        })
    }
}
impl<T: Value> Drop for Sender<T> {
    fn drop(&mut self) {
        let _op = self.0.core.operation();
        if self.0.core.senders.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.0.core.fire();
        }
    }
}
impl<T: Value> Clone for WeakSender<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
impl<T: Value> WeakSender<T> {
    pub(super) fn upgrade(&self) -> Option<Sender<T>> {
        let inner = self.0.upgrade()?;
        inner
            .core
            .senders
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                (count != 0).then(|| count.checked_add(1)).flatten()
            })
            .ok()?;
        Some(Sender(inner))
    }
}
impl<T: Value> Permit<'_, T> {
    pub(super) fn send(self, value: T) {
        let core = &self.notify.0.0.core;
        let _op = core.operation();
        let size = value.lane_bytes();
        if !value.valid_for(&core.meter) {
            let mut invalid = core.invalid.lock().unwrap_or_else(|e| e.into_inner());
            if invalid.is_none() {
                let error = std::io::Error::from(std::io::ErrorKind::InvalidData);
                let publication = core.clock.io_deferred(&error);
                *invalid = Some(error);
                drop(invalid);
                drop(publication);
            } else {
                drop(invalid);
            }
            core.clock.quarantine();
            core.meter.quarantine();
        }
        // The original Unit is the capacity/admission proof; there is no new Q
        // fence or fallible operation after the caller committed its Flow debt.
        self.unit.commit();
        core.messages.fetch_add(1, Ordering::AcqRel);
        core.bytes.fetch_add(size, Ordering::AcqRel);
        core.ring.publish_admitted(value);
        // ReturnNotify runs after the original Unit has committed and the
        // publication is complete, including when this Permit is only dropped.
    }
}
struct Receiving<'a, T: Value>(&'a mut Receiver<T>);
impl<T: Value> Drop for Receiving<'_, T> {
    fn drop(&mut self) {
        self.0.0.core.retire_waiter();
    }
}
impl<T: Value> Receiver<T> {
    pub(super) fn max_capacity(&self) -> usize { self.0.core.capacity }
    pub(super) async fn recv(&mut self) -> Option<T> {
        let receiving = Receiving(self);
        std::future::poll_fn(|cx| receiving.0.poll_recv(cx)).await
    }
    pub(super) fn close(&mut self) {
        let core = &self.0.core;
        let _op = core.operation();
        core.credit.close();
    }
    /// Caller holds its original producer-sealed publication exclusion and
    /// exclusively owns this Receiver; no publication/read may begin here.
    pub(super) unsafe fn empty_under_producer_seal(&mut self) -> bool {
        let core = &self.0.core;
        if core.meter.is_quarantined()
            || core.operations.load(Ordering::Acquire) != 0
            || core.notification.load(Ordering::Acquire) & ACTIVE != 0
            || core.drain.load(Ordering::Acquire) & ACTIVE != 0
            || !core.credit.runner_idle()
        {
            return false;
        }
        let stock = unsafe { core.ring.inventory(Value::lane_bytes) };
        let held_empty = unsafe { (&*core.held.get()).is_none() };
        let expected = Some((
            core.messages.load(Ordering::Acquire),
            core.bytes.load(Ordering::Acquire),
        ));
        if stock.is_none() || stock != expected || !held_empty {
            core.meter.quarantine();
            return false;
        }
        stock == Some((0, 0))
            && core.credit.settled_inventory()
            && core.callbacks().waiter.is_none()
    }
    pub(super) fn producers_gone(&self) -> bool {
        self.0.core.senders.load(Ordering::Acquire) == 0
    }
    pub(super) fn try_recv(&mut self) -> Result<T, tokio::sync::mpsc::error::TryRecvError> {
        use tokio::sync::mpsc::error::TryRecvError;
        let core = &self.0.core;
        let _op = core.operation();
        match core.take() {
            Pop::Value(value) => Ok(value),
            Pop::Empty
                if core.meter.is_quarantined()
                    || core.senders.load(Ordering::Acquire) == 0
                    || (core.credit.is_closed() && core.credit.available() == core.capacity) =>
            {
                Err(TryRecvError::Disconnected)
            }
            _ => Err(TryRecvError::Empty),
        }
    }
    pub(super) fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        use tokio::sync::mpsc::error::TryRecvError;
        match self.try_recv() {
            Ok(value) => return Poll::Ready(Some(value)),
            Err(TryRecvError::Disconnected) => return Poll::Ready(None),
            _ => {}
        }
        self.0.core.register(cx.waker());
        // Registration must precede the second actual slot/EOF observation.
        match self.try_recv() {
            Ok(value) => Poll::Ready(Some(value)),
            Err(TryRecvError::Disconnected) => {
                self.0.core.retire_waiter();
                Poll::Ready(None)
            }
            _ => Poll::Pending,
        }
    }
    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.0.core.messages.load(Ordering::Acquire)
    }
    #[cfg(test)]
    pub(super) fn is_closed(&self) -> bool {
        self.0.core.senders.load(Ordering::Acquire) == 0 || self.0.core.meter.is_quarantined()
    }
}
impl<T: Value> Drop for Receiver<T> {
    fn drop(&mut self) {
        let core = &self.0.core;
        let _op = core.operation();
        core.receiver_closed.store(true, Ordering::Release);
        core.credit.close();
        core.retire_waiter();
        core.drain_closed();
    }
}
impl<T: Value> Observer<T> {
    pub(crate) fn settled(&self) -> bool {
        self.0.core.clean_inventory()
    }
    pub(crate) fn finish(self) -> Result<(), Self> {
        if !self.settled() {
            return Err(self);
        }
        self.0.core.retire_context();
        if !self.settled() {
            return Err(self);
        }
        match Arc::try_unwrap(self.0) {
            Ok(inner) => {
                drop(inner);
                Ok(())
            }
            Err(inner) => Err(Self(inner)),
        }
    }
}
impl<T: Value> Drop for Inner<T> {
    fn drop(&mut self) {
        self.core.retire_context();
        if !self.core.clean_inventory() {
            self.core.meter.quarantine(); // retain actual Core, all slots and original charge
            return;
        }
        unsafe {
            ManuallyDrop::drop(&mut self.core);
            ManuallyDrop::drop(&mut self.stock);
        }
    }
}

#[cfg(test)]
mod tests;
