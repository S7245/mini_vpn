//! Cold callback inventory: original UdpRuntime keeps its fixed 261-role catalog;
//! a Native TCP Source has one separately owned slot. This is physical custody,
//! not an epoch/stop/refund authority. Each actual Signal Box remains charged
//! through its last safe proxy. A role cannot be reused while an original clone,
//! wake or Drop is in flight, even after its original future/JoinSet entry ends.
//! Only safe Wakers enter Notify/watch/transport futures. Original callbacks and
//! whole-future poll/Drop are guarded separately, with fixed original Box slots.
//! Entry: prepare/acquire/Lease::run; tests live in callbacks/tests.
use crate::shared::errors::FailureClock;
use bytes::{BackingBudgetReservation, BackingMeter, MeteredCopyError};
use std::{
    alloc::{Layout, alloc},
    any::Any,
    future::{Future, poll_fn},
    mem::ManuallyDrop,
    ops::Deref,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Mutex},
    task::{Context, Poll, Wake, Waker},
};
const CHILDREN: usize = 256;
const ROLES: usize = CHILDREN + 5;
type Payload = ManuallyDrop<Box<dyn Any + Send>>;
#[derive(Clone, Copy)]
enum Site {
    Clone,
    Replace,
    Wake,
    WakeDrop,
    Retire,
    Poll,
    WorkDrop,
    Source0,
    Source1,
    Source2,
    Source3,
    Source4,
    Source5,
    Source6,
    Source7,
    Join,
    Notification,
}
#[derive(Clone, Copy)]
pub(crate) enum Role {
    Parent,
    Child,
    Monitor,
    Source,
    Recovery,
    Shutdown,
}
impl Role {
    fn range(self) -> std::ops::Range<usize> {
        match self {
            Self::Parent => 0..1,
            Self::Child => 1..CHILDREN + 1,
            Self::Monitor => CHILDREN + 1..CHILDREN + 2,
            Self::Source => CHILDREN + 2..CHILDREN + 3,
            Self::Recovery => CHILDREN + 3..CHILDREN + 4,
            Self::Shutdown => CHILDREN + 4..ROLES,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CallbackError {
    Capacity,
    Quarantined,
}
struct State {
    leased: bool,
    operations: usize,
    running: bool,
    again: bool,
    waiter: Option<Waker>,
    failures: [Option<Payload>; 17],
}
impl State {
    fn idle(&self) -> bool {
        !self.leased
            && self.operations == 0
            && !self.running
            && self.waiter.is_none()
            && self.failures.iter().all(Option::is_none)
    }
}
struct Core {
    state: Mutex<State>,
    meter: BackingMeter,
    clock: FailureClock,
    source_cancel_wake: bool,
}
struct Signal {
    core: ManuallyDrop<Box<Core>>,
    stock: ManuallyDrop<Option<BackingBudgetReservation>>,
}
impl Deref for Signal {
    type Target = Core;
    fn deref(&self) -> &Core {
        &self.core
    }
}
pub(crate) type Callbacks = Inventory<ROLES>;
pub(crate) type SourceCallbacks = Inventory<1>;
pub(crate) struct Inventory<const N: usize> {
    slots: ManuallyDrop<Box<[Option<Arc<Signal>>; N]>>,
    stock: ManuallyDrop<Option<BackingBudgetReservation>>,
    meter: BackingMeter,
}
// Field order also owns every partially prepared cold rollback.
struct ColdSlots<const N: usize> {
    slots: Box<[Option<Arc<Signal>>; N]>,
    stock: Option<BackingBudgetReservation>,
}
#[derive(Debug)]
pub(crate) struct CallbackFailure<T> {
    pub(crate) output: Option<T>,
    pub(crate) error: CallbackError,
}
pub(crate) struct Lease {
    signal: Arc<Signal>,
}
fn cold<T>(
    meter: &BackingMeter,
    value: T,
) -> Result<(Box<T>, Option<BackingBudgetReservation>), MeteredCopyError> {
    let layout = Layout::new::<T>();
    let stock = meter.reserve_cold_layout(layout.size())?;
    let ptr = unsafe { alloc(layout).cast::<T>() };
    if ptr.is_null() {
        return Err(MeteredCopyError::Allocation);
    }
    Ok((
        unsafe {
            ptr.write(value);
            Box::from_raw(ptr)
        },
        stock,
    ))
}
impl<const N: usize> Inventory<N> {
    pub(crate) fn layout_bytes() -> usize {
        Layout::new::<[Option<Arc<Signal>>; N]>().size() + N * Layout::new::<Core>().size()
    }
    fn prepare_bound(
        meter: &BackingMeter,
        clock: FailureClock,
        same_budget: bool,
    ) -> Result<Self, MeteredCopyError> {
        if N == 0 { return Err(MeteredCopyError::TooLarge); }
        let bound = if same_budget { clock.bound_to_budget(meter) } else { clock.bound_to_meter(meter) };
        if meter.budget_limit().is_none() || !bound {
            meter.quarantine();
            if same_budget { clock.quarantine(); }
            return Err(MeteredCopyError::Quarantined);
        }
        let (slots, stock) = cold(meter, std::array::from_fn(|_| None))?;
        let mut prepared = ColdSlots::<N> { slots, stock };
        for slot in prepared.slots.iter_mut() {
            let (core, allowance) = cold(
                meter,
                Core {
                    state: Mutex::new(State {
                        leased: false,
                        operations: 0,
                        running: false,
                        again: false,
                        waiter: None,
                        failures: std::array::from_fn(|_| None),
                    }),
                    meter: meter.clone(),
                    clock: clock.clone(),
                    source_cancel_wake: same_budget,
                },
            )?;
            let signal = Arc::new(Signal {
                core: ManuallyDrop::new(core),
                stock: ManuallyDrop::new(allowance),
            });
            drop(signal.lock());
            *slot = Some(signal);
        }
        Ok(Self {
            slots: ManuallyDrop::new(prepared.slots),
            stock: ManuallyDrop::new(prepared.stock),
            meter: meter.clone(),
        })
    }
    fn acquire_range(&self, range: std::ops::Range<usize>) -> Result<Lease, CallbackError> {
        if self.meter.is_quarantined() {
            return Err(CallbackError::Quarantined);
        }
        for index in range {
            let Some(signal) = self.slots[index].as_ref() else {
                self.meter.quarantine();
                return Err(CallbackError::Quarantined);
            };
            let mut state = signal.lock();
            if self.meter.is_quarantined() {
                return Err(CallbackError::Quarantined);
            }
            if state.idle() {
                state.leased = true;
                return Ok(Lease {
                    signal: signal.clone(),
                });
            }
        }
        Err(CallbackError::Capacity)
    }
    pub(crate) fn quiescent(&self) -> bool {
        !self.meter.is_quarantined()
            && self.slots.iter().all(|s| {
                s.as_ref()
                    .is_some_and(|s| Arc::strong_count(s) == 1 && s.lock().idle())
            })
    }
}
impl Callbacks {
    pub(crate) fn prepare(meter: &BackingMeter, clock: FailureClock) -> Result<Self, MeteredCopyError> {
        Self::prepare_bound(meter, clock, false)
    }
    pub(crate) fn acquire(&self, role: Role) -> Result<Lease, CallbackError> {
        self.acquire_range(role.range())
    }
}
impl SourceCallbacks {
    /// Source is owned by its Node; its original Session clock may use the App
    /// meter on that same physical budget. UDP keeps its stricter constructor.
    pub(crate) fn prepare_source(meter: &BackingMeter, clock: FailureClock) -> Result<Self, MeteredCopyError> {
        Self::prepare_bound(meter, clock, true)
    }
    pub(crate) fn acquire_source(&self) -> Result<Lease, CallbackError> {
        self.acquire_range(0..1)
    }
}
impl<const N: usize> Drop for Inventory<N> {
    fn drop(&mut self) {
        if self.quiescent() {
            unsafe {
                ManuallyDrop::drop(&mut self.slots);
                ManuallyDrop::drop(&mut self.stock);
            }
        } else {
            self.meter.quarantine();
        }
    }
}
impl Signal {
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| {
            self.meter.quarantine();
            e.into_inner()
        })
    }
    fn capture(&self, site: Site, payload: Box<dyn Any + Send>) {
        let mut state = self.lock();
        let slot = &mut state.failures[site as usize];
        if slot.is_none() {
            *slot = Some(ManuallyDrop::new(payload));
        } else {
            std::mem::forget(payload);
        }
        let publication = self.clock.panic_deferred();
        self.meter.quarantine();
        drop(state);
        drop(publication);
    }
    fn dispose(&self, site: Site, waiter: Option<Waker>) {
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(waiter))) {
            self.capture(site, payload);
        }
    }
    fn end_operation(&self) {
        self.lock().operations -= 1;
    }
    fn register(&self, original: &Waker) -> bool {
        {
            let mut state = self.lock();
            if !state.leased || self.meter.is_quarantined() {
                return false;
            }
            if state.waiter.as_ref().is_some_and(|w| w.will_wake(original)) {
                return true;
            }
            state.operations += 1;
        }
        let new = match catch_unwind(AssertUnwindSafe(|| original.clone())) {
            Ok(new) => new,
            Err(payload) => {
                self.capture(Site::Clone, payload);
                self.end_operation();
                return false;
            }
        };
        let old = {
            let mut state = self.lock();
            if state.leased && !self.meter.is_quarantined() {
                state.waiter.replace(new)
            } else {
                Some(new)
            }
        };
        self.dispose(Site::Replace, old);
        self.end_operation();
        !self.meter.is_quarantined()
    }
    fn retire(&self) {
        let waiter = {
            let mut state = self.lock();
            state.leased = false;
            state.operations += 1;
            state.waiter.take()
        };
        self.dispose(Site::Retire, waiter);
        self.end_operation();
    }
    fn fire(&self) {
        {
            let mut state = self.lock();
            if !state.leased {
                return;
            }
            if state.running {
                state.again = true;
                return;
            }
            state.running = true;
        }
        loop {
            let waiter = {
                let mut state = self.lock();
                state.again = false;
                state.waiter.take()
            };
            if let Some(waiter) = waiter {
                if std::thread::panicking() {
                    self.meter.quarantine();
                }
                // Source needs one final executor poll to drop Work and return
                // its actual Outcome. register still rejects Q before any DATA.
                if !self.meter.is_quarantined() || self.source_cancel_wake {
                    if let Err(payload) = catch_unwind(AssertUnwindSafe(|| waiter.wake_by_ref())) {
                        self.capture(Site::Wake, payload);
                    }
                }
                self.dispose(Site::WakeDrop, Some(waiter));
            }
            let mut state = self.lock();
            if state.again && state.leased && state.waiter.is_some() {
                continue;
            }
            state.running = false;
            state.again = false;
            return;
        }
    }
}
impl Wake for Signal {
    fn wake(self: Arc<Self>) {
        self.fire();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.fire();
    }
}
impl Drop for Signal {
    fn drop(&mut self) {
        if !self.meter.is_quarantined() && self.lock().idle() {
            unsafe {
                ManuallyDrop::drop(&mut self.core);
                ManuallyDrop::drop(&mut self.stock);
            }
        } else {
            self.meter.quarantine();
        }
    }
}
impl Lease {
    /// One actor's concrete fields are independently guarded before a future
    /// can poll or unwind. Custody keeps the role unavailable through actual Drop.
    pub(crate) fn guard(&self) -> Guard {
        self.signal.lock().operations += 1;
        Guard {
            signal: self.signal.clone(),
        }
    }
    /// Synchronous admission takes the original future only on success. The
    /// returned future already owns Work, so even zero-poll cancellation guards
    /// its destructor. Q leaves the caller's original Option untouched.
    pub(crate) fn run<'a, F: Future + 'a>(
        &'a mut self,
        future: &mut Option<F>,
    ) -> Result<
        impl Future<Output = Result<F::Output, CallbackFailure<F::Output>>> + 'a,
        CallbackError,
    > {
        if self.signal.meter.is_quarantined() {
            return Err(CallbackError::Quarantined);
        }
        let Some(future) = future.take() else {
            self.signal.clock.quarantine();
            return Err(CallbackError::Quarantined);
        };
        let signal = &self.signal;
        let work = Work {
            future: ManuallyDrop::new(future),
            signal,
        };
        Ok(async move {
            let result = {
                let mut work = std::pin::pin!(work);
                poll_fn(|cx| {
                    if !signal.register(cx.waker()) {
                        return Poll::Ready(Err(CallbackError::Quarantined));
                    }
                    let proxy = Waker::from(signal.clone());
                    let mut safe = Context::from_waker(&proxy);
                    // Work never moves after pinning; its future drops in place.
                    let pinned =
                        unsafe { work.as_mut().map_unchecked_mut(|work| &mut *work.future) };
                    match catch_unwind(AssertUnwindSafe(|| pinned.poll(&mut safe))) {
                        Ok(Poll::Pending) if !signal.meter.is_quarantined() => Poll::Pending,
                        Ok(Poll::Pending) => Poll::Ready(Err(CallbackError::Quarantined)),
                        Ok(Poll::Ready(output)) => Poll::Ready(Ok(output)),
                        Err(payload) => {
                            signal.capture(Site::Poll, payload);
                            Poll::Ready(Err(CallbackError::Quarantined))
                        }
                    }
                })
                .await
            }; // Actual Work Drop precedes final disposition; keep original output.
            match result {
                Ok(output) if !signal.meter.is_quarantined() => Ok(output),
                Ok(output) => Err(CallbackFailure {
                    output: Some(output),
                    error: CallbackError::Quarantined,
                }),
                Err(error) => Err(CallbackFailure {
                    output: None,
                    error,
                }),
            }
        })
    }
}

#[derive(Clone, Copy)]
pub(crate) enum Field {
    Source0,
    Source1,
    Source2,
    Source3,
    Source4,
    Source5,
    Source6,
    Source7,
    Join,
    Notification,
}
impl Field {
    fn site(self) -> Site {
        match self {
            Self::Source0 => Site::Source0,
            Self::Source1 => Site::Source1,
            Self::Source2 => Site::Source2,
            Self::Source3 => Site::Source3,
            Self::Source4 => Site::Source4,
            Self::Source5 => Site::Source5,
            Self::Source6 => Site::Source6,
            Self::Source7 => Site::Source7,
            Self::Join => Site::Join,
            Self::Notification => Site::Notification,
        }
    }
}
pub(crate) struct Guard {
    signal: Arc<Signal>,
}
impl Clone for Guard {
    fn clone(&self) -> Self {
        self.signal.lock().operations += 1;
        Self {
            signal: self.signal.clone(),
        }
    }
}
impl Guard {
    pub(crate) fn own<T>(&self, value: T, field: Field) -> Custody<T> {
        Custody {
            value: ManuallyDrop::new(value),
            guard: self.clone(),
            field,
        }
    }
    pub(crate) fn capture(&self, field: Field, payload: Box<dyn Any + Send>) {
        self.signal.capture(field.site(), payload);
    }
    pub(crate) fn dispose<T>(&self, value: T, field: Field) {
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(value))) {
            self.capture(field, payload);
        }
    }
    pub(crate) fn notify(&self, notify: &tokio::sync::Notify) {
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| notify.notify_waiters())) {
            self.capture(Field::Notification, payload);
        }
    }
    pub(crate) fn quarantine(&self) {
        self.signal.clock.quarantine();
        self.signal.meter.quarantine();
    }
    pub(crate) fn failed(&self) -> bool {
        self.signal.meter.is_quarantined()
    }
}
impl Drop for Guard {
    fn drop(&mut self) {
        self.signal.end_operation();
    }
}
pub(crate) struct Custody<T> {
    value: ManuallyDrop<T>,
    guard: Guard,
    field: Field,
}
impl<T> Deref for Custody<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.value
    }
}
impl<T> std::ops::DerefMut for Custody<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.value
    }
}
impl<T> Drop for Custody<T> {
    fn drop(&mut self) {
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| unsafe {
            ManuallyDrop::drop(&mut self.value)
        })) {
            self.guard.capture(self.field, payload);
        }
    }
}

struct Work<'a, F> {
    future: ManuallyDrop<F>,
    signal: &'a Signal,
}
impl<F> Drop for Work<'_, F> {
    fn drop(&mut self) {
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| unsafe {
            ManuallyDrop::drop(&mut self.future)
        })) {
            self.signal.capture(Site::WorkDrop, payload);
        }
    }
}
impl Drop for Lease {
    fn drop(&mut self) {
        self.signal.retire();
    }
}
#[cfg(test)]
mod tests;
