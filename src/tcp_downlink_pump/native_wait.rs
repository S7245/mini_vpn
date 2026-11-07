//! Native FIFO's two bounded waiting roles. Original Notify/Mutex receive safe
//! proxies; original callbacks are guarded individually. Each work role has one
//! independent retirement notice and one successor claim. Its ledger serializes
//! phase, callback operations and runner retirement. All application state
//! resides in actual cold Boxes; Arc carries only Box/stock lifetime pointers.
//! The original queue owns retirement references. JOIN attempts try_unwrap
//! outside its local inventory lock; no receipt precedes physical Box refund.
//! Operations lease a role under one same-flow mutex (no per-record allocation).
//! Tests: tcp_downlink_pump/tests/native_waiters.rs and actual RelayEvent permit.
use crate::shared::errors::FailureClock;
use bytes::{BackingBudgetReservation, BackingMeter, MeteredCopyError};
use std::{
    alloc::{Layout, alloc},
    any::Any,
    future::{Future, poll_fn},
    mem::ManuallyDrop,
    ops::Deref,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
};
#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase { Idle, Leased, Retiring, Closed }
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
    Disposal,
}
struct State {
    waiter: Option<Waker>,
    failures: [Option<Payload>; 8],
    phase: Phase,
    operations: usize,
    acquisition: bool,
    running: bool,
    again: bool,
}
struct ControlState {
    meter: BackingMeter,
    clock: OnceLock<FailureClock>,
    closing: AtomicBool,
}
struct Control {
    core: ManuallyDrop<Box<ControlState>>,
    stock: ManuallyDrop<Option<BackingBudgetReservation>>,
}
struct SignalState {
    callbacks: Mutex<State>,
    control: Arc<Control>,
    // One independent, cold notification role per original work role. Its
    // callback can schedule acquisition without being the work being retired.
    retirement: Option<Arc<Signal>>,
}
struct Signal {
    core: ManuallyDrop<Box<SignalState>>,
    stock: ManuallyDrop<Option<BackingBudgetReservation>>,
}
impl Deref for Control {
    type Target = ControlState;
    fn deref(&self) -> &Self::Target {
        &self.core
    }
}
impl Deref for Signal {
    type Target = SignalState;
    fn deref(&self) -> &Self::Target {
        &self.core
    }
}
struct Inventory {
    roles: [Option<Arc<Signal>>; 2],
    control: Option<Arc<Control>>,
}
pub(super) struct RootState {
    // The original clock outlives callback seal while this queue still owns
    // physical stock or an old permit. Signals retain the same Arc for panic.
    clock: OnceLock<FailureClock>,
    inventory: Mutex<Inventory>,
    finishing: AtomicBool,
    // 0: no transfer; 1: one actual chunk Drop in flight; 2: Drop completed.
    // FIFO interrupted stays set until state 2 is committed under its lock.
    disposal: AtomicUsize,
    disposal_turn: AtomicBool,
    meter: BackingMeter,
}
pub(super) struct Waiters {
    root: ManuallyDrop<Box<RootState>>,
    stock: ManuallyDrop<Option<BackingBudgetReservation>>,
}
impl Deref for Waiters {
    type Target = RootState;
    fn deref(&self) -> &Self::Target {
        &self.root
    }
}
impl Drop for Waiters {
    fn drop(&mut self) {
        if !self.meter.is_quarantined() {
            unsafe {
                ManuallyDrop::drop(&mut self.root);
                ManuallyDrop::drop(&mut self.stock);
            }
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RunError {
    Closed,
    Quarantined,
}
impl std::fmt::Debug for Waiters {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("NativeQueueWaiters")
    }
}
pub(super) enum Role {
    Producer,
    Consumer,
}
struct Lease {
    signal: Arc<Signal>,
}
impl Drop for Lease {
    fn drop(&mut self) {
        self.signal.retire();
    }
}
pub(super) fn cold<T>(
    meter: &BackingMeter,
    value: T,
) -> Result<(Box<T>, Option<BackingBudgetReservation>), MeteredCopyError> {
    let layout = Layout::new::<T>();
    let stock = meter.reserve_cold_layout(layout.size())?;
    let ptr = unsafe { alloc(layout) }.cast::<T>();
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
impl Signal {
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.callbacks.lock().unwrap_or_else(|e| {
            self.control.meter.quarantine();
            e.into_inner()
        })
    }
    fn capture(&self, site: Site, payload: Box<dyn Any + Send>) {
        let mut cold = self.lock();
        let slot = &mut cold.failures[site as usize];
        if slot.is_none() { *slot = Some(ManuallyDrop::new(payload)); }
        else { std::mem::forget(payload); }
        // Keep the original Box before publishing; no opaque callback is run
        // under this ledger. Original owner/parent publication is outside it.
        self.control.meter.quarantine();
        drop(cold);
        if let Some(clock) = self.control.clock.get() { clock.panic(); }
    }
    fn dispose(&self, site: Site, waker: Option<Waker>) {
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(waker))) { self.capture(site, payload); }
    }
    fn settle(&self, state: &mut State) -> bool {
        if state.phase == Phase::Retiring && state.operations == 0 && !state.running
            && state.waiter.is_none() && state.failures.iter().all(Option::is_none)
            && !self.control.meter.is_quarantined()
        {
            state.phase = Phase::Idle;
            true
        } else { false }
    }
    fn notify_retired(&self, retired: bool) {
        if retired { if let Some(signal) = &self.retirement { signal.fire(); } }
    }
    fn end_operation(&self) {
        let retired = {
            let mut state = self.lock();
            match state.operations.checked_sub(1) {
                Some(count) => state.operations = count,
                None => self.control.meter.quarantine(),
            }
            self.settle(&mut state)
        };
        self.notify_retired(retired);
    }
    fn register(&self, waker: &Waker) -> bool {
        {
            let mut state = self.lock();
            if state.phase == Phase::Closed || self.control.meter.is_quarantined() { return false; }
            if state.waiter.as_ref().is_some_and(|old| old.will_wake(waker)) { return true; }
            let Some(count) = state.operations.checked_add(1) else { self.control.meter.quarantine(); return false; };
            state.operations = count;
        }
        let new = match catch_unwind(AssertUnwindSafe(|| waker.clone())) {
            Ok(waker) => waker,
            Err(payload) => { self.capture(Site::Clone, payload); self.end_operation(); return false; }
        };
        let old = {
            let mut state = self.lock();
            if state.phase != Phase::Closed && !self.control.meter.is_quarantined() { state.waiter.replace(new) }
            else { Some(new) }
        };
        self.dispose(Site::Replace, old);
        self.end_operation();
        !self.control.meter.is_quarantined()
    }
    fn retire_waiter(&self, work: bool) {
        let waiter = {
            let mut state = self.lock();
            if work {
                if state.phase != Phase::Leased { self.control.meter.quarantine(); }
                state.phase = Phase::Retiring;
            }
            let Some(count) = state.operations.checked_add(1) else { self.control.meter.quarantine(); return; };
            state.operations = count;
            state.waiter.take()
        };
        self.dispose(Site::Retire, waiter);
        self.end_operation();
    }
    fn retire(&self) { self.retire_waiter(true); }
    fn fire(&self) {
        {
            let mut state = self.lock();
            if state.phase == Phase::Closed || (self.retirement.is_some() && state.phase != Phase::Leased) { return; }
            if state.running { state.again = true; return; }
            state.running = true;
        }
        loop {
            let waiter = {
                let mut state = self.lock();
                state.again = false;
                state.waiter.take()
            };
            if let Some(waiter) = waiter {
                if !self.control.meter.is_quarantined() && !std::thread::panicking() {
                    if let Err(payload) = catch_unwind(AssertUnwindSafe(|| waiter.wake_by_ref())) { self.capture(Site::Wake, payload); }
                } else if std::thread::panicking() { self.control.meter.quarantine(); }
                self.dispose(Site::WakeDrop, Some(waiter));
            }
            let retired = {
                let mut state = self.lock();
                if state.again && state.waiter.is_some() && !self.control.meter.is_quarantined() { continue; }
                state.running = false;
                state.again = false;
                self.settle(&mut state)
            };
            self.notify_retired(retired);
            return;
        }
    }
    fn ready(&self) -> bool {
        if self.control.meter.is_quarantined() { return false; }
        {
            let mut state = self.lock();
            if !matches!(state.phase, Phase::Idle | Phase::Closed) || state.acquisition || state.operations != 0 || state.running { return false; }
            if state.waiter.is_some() || state.failures.iter().any(Option::is_some) {
                self.control.meter.quarantine(); return false;
            }
            state.phase = Phase::Closed;
        }
        self.retirement.as_ref().is_none_or(|signal| Arc::strong_count(signal) == 1 && signal.ready())
    }
}
impl Wake for Signal {
    fn wake(self: Arc<Self>) { self.fire(); }
    fn wake_by_ref(self: &Arc<Self>) { self.fire(); }
}
impl Drop for Signal {
    fn drop(&mut self) {
        let state = self.lock();
        let clean = !self.control.meter.is_quarantined()
            && matches!(state.phase, Phase::Idle | Phase::Closed)
            && !state.acquisition && state.operations == 0 && !state.running && state.waiter.is_none()
            && state.failures.iter().all(Option::is_none);
        drop(state);
        if clean {
            unsafe { ManuallyDrop::drop(&mut self.core); ManuallyDrop::drop(&mut self.stock); }
        } else { self.control.meter.quarantine(); }
    }
}
// Original queue work contains only its borrowed pending/held value and safe
// Notify futures. This guard owns its actual poll/drop; it is not an actor-field
// guard for an arbitrary Legacy reader with several opaque destructors.
struct Work<'a, F> { future: ManuallyDrop<F>, signal: &'a Signal }
impl<F> Drop for Work<'_, F> {
    fn drop(&mut self) {
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| unsafe { ManuallyDrop::drop(&mut self.future); })) {
            self.signal.capture(Site::WorkDrop, payload);
        }
    }
}
struct Acquisition { signal: Arc<Signal>, notification: Arc<Signal>, ticket: bool }
impl Drop for Acquisition {
    fn drop(&mut self) {
        if !self.ticket { return; }
        self.notification.retire_waiter(false); // callback/last Waker outside locks
        let mut state = self.signal.lock();
        if !state.acquisition { self.signal.control.meter.quarantine(); }
        state.acquisition = false;
    }
}
pub(super) struct DisposalTurn<'a>(&'a Waiters);
impl Drop for DisposalTurn<'_> {
    fn drop(&mut self) { self.0.disposal_turn.store(false, Ordering::Release); }
}
impl Drop for Control {
    fn drop(&mut self) {
        if !self.meter.is_quarantined() {
            unsafe {
                ManuallyDrop::drop(&mut self.core);
                ManuallyDrop::drop(&mut self.stock);
            }
        }
    }
}
/// Serializes the one cold transition against the first operation, including a
/// legacy waiter that is cancelled before any DATA enters the queue.
#[derive(Debug, Default)]
pub(super) struct Start(Mutex<bool>);
impl Start {
    pub(super) fn begin(&self) {
        *self.0.lock().unwrap_or_else(|e| e.into_inner()) = true;
    }
    pub(super) fn cold(&self) -> Option<std::sync::MutexGuard<'_, bool>> {
        let mut used = self.0.lock().ok()?;
        if *used {
            return None;
        }
        *used = true;
        Some(used)
    }
}
/// Created at the external owner boundary before stock/FIFO guards. Atomic Q
/// is written inside ledgers; original quiet and final owner Arc Drop run here.
#[derive(Clone)]
pub(super) struct QuietScope { meter: BackingMeter, clock: Option<FailureClock> }
impl QuietScope {
    pub(super) fn mark_quarantined(&self) { self.meter.quarantine(); }
    pub(super) fn new(meter: BackingMeter, clock: Option<FailureClock>) -> Self { Self { meter, clock } }
}
impl Drop for QuietScope {
    fn drop(&mut self) {
        if self.meter.is_quarantined() { if let Some(clock) = &self.clock { clock.quarantine(); } }
    }
}
impl Waiters {
    pub(super) fn is_quarantined(&self) -> bool { self.meter.is_quarantined() }
    pub(super) fn dispose_output_space(&self, space: super::output_stock::OutputSpace) -> bool {
        // This one known field is not an arbitrary actor with opaque siblings.
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(space))) {
            let signal = self.lock().roles[0].clone();
            match signal {
                Some(signal) => signal.capture(Site::Disposal, payload),
                None => { self.meter.quarantine(); std::mem::forget(payload); }
            }
            return false;
        }
        !self.meter.is_quarantined()
    }
    pub(super) fn try_disposal_turn(&self) -> Option<DisposalTurn<'_>> {
        self.disposal_turn.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire).ok().map(|_| DisposalTurn(self))
    }
    pub(super) fn disposal_state(&self) -> usize { self.disposal.load(Ordering::Acquire) }
    pub(super) fn begin_disposal(&self) -> bool {
        if self.disposal.compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire).is_err() {
            self.meter.quarantine(); return false;
        }
        true
    }
    pub(super) fn dispose_chunk(&self, chunk: super::ByteChunk) -> bool {
        // ByteChunk has one independently guarded CustodiedBytes destructor;
        // its other field is a scalar. No multi-opaque actor is hidden here.
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(chunk))) {
            let signal = self.lock().roles[1].clone();
            match signal {
                Some(signal) => signal.capture(Site::Disposal, payload),
                None => { self.meter.quarantine(); std::mem::forget(payload); }
            }
            return false;
        }
        if self.meter.is_quarantined() { return false; }
        if self.disposal.compare_exchange(1, 2, Ordering::AcqRel, Ordering::Acquire).is_err() {
            self.meter.quarantine(); return false;
        }
        true
    }
    pub(super) fn commit_disposal(&self) -> bool {
        if self.meter.is_quarantined() || self.disposal.compare_exchange(2, 0, Ordering::AcqRel, Ordering::Acquire).is_err() {
            self.meter.quarantine(); return false;
        }
        true
    }
    pub(super) fn mark_quarantined(&self) { self.meter.quarantine(); }
    pub(super) fn bound_clock(&self) -> Option<FailureClock> { self.clock.get().cloned() }
    pub(super) fn quiet_scope(&self) -> QuietScope { QuietScope::new(self.meter.clone(), self.bound_clock()) }
    pub(super) fn stock_bound(&self) -> bool {
        self.lock().control.as_ref().is_some_and(|control| {
            !control.meter.is_quarantined() && control.clock.get().zip(self.clock.get())
                .is_some_and(|(a, b)| Arc::ptr_eq(a, b) && a.is_bound())
        })
    }
    fn lock(&self) -> std::sync::MutexGuard<'_, Inventory> {
        self.inventory.lock().unwrap_or_else(|e| {
            let inner = e.into_inner();
            if let Some(c) = &inner.control {
                c.meter.quarantine();
            }
            inner
        })
    }
    pub(super) fn layout_bytes() -> usize {
        Layout::new::<RootState>().size()
            + Layout::new::<SignalState>().size() * 4
            + Layout::new::<ControlState>().size()
    }
    #[cfg(test)]
    pub(super) fn retained_role_bytes() -> usize {
        Layout::new::<RootState>().size()
            + Layout::new::<SignalState>().size() * 2
            + Layout::new::<ControlState>().size()
    }
    #[cfg(test)]
    pub(super) fn retains_arc<T: Any>(&self, expected: &Arc<T>) -> bool {
        self.lock().roles.iter().flatten().any(|role| {
            role.lock().failures.iter().flatten().any(|p| {
                p.downcast_ref::<Arc<T>>()
                    .is_some_and(|a| Arc::ptr_eq(a, expected))
            })
        })
    }
    pub(super) fn prepare(meter: &BackingMeter) -> Result<Self, MeteredCopyError> {
        let (core, stock) = cold(
            meter,
            ControlState {
                meter: meter.clone(),
                clock: OnceLock::new(),
                closing: AtomicBool::new(false),
            },
        )?;
        let control = Arc::new(Control {
            core: ManuallyDrop::new(core),
            stock: ManuallyDrop::new(stock),
        });
        let make_signal = |retirement| {
            let (core, stock) = cold(
                meter,
                SignalState {
                    callbacks: Mutex::new(State {
                        waiter: None,
                        failures: std::array::from_fn(|_| None),
                        phase: Phase::Idle, operations: 0, acquisition: false, running: false, again: false,
                    }),
                    control: control.clone(),
                    retirement,
                },
            )?;
            let signal = Arc::new(Signal {
                core: ManuallyDrop::new(core),
                stock: ManuallyDrop::new(stock),
            });
            drop(signal.lock()); // platform mutex initialization belongs to cold preparation
            Ok::<_, MeteredCopyError>(signal)
        };
        let signal = || make_signal(Some(make_signal(None)?));
        let (root, stock) = cold(
            meter,
            RootState {
                clock: OnceLock::new(),
                inventory: Mutex::new(Inventory {
                    roles: [Some(signal()?), Some(signal()?)],
                    control: Some(control),
                }),
                finishing: AtomicBool::new(false),
                disposal: AtomicUsize::new(0),
                disposal_turn: AtomicBool::new(false),
                meter: meter.clone(),
            },
        )?;
        let waiters = Self {
            root: ManuallyDrop::new(root),
            stock: ManuallyDrop::new(stock),
        };
        drop(waiters.lock());
        Ok(waiters)
    }
    pub(super) fn bind(&self, clock: FailureClock) -> bool {
        let _quiet = self.quiet_scope();
        // Finish cannot remove Control between validation and the two handles
        // to this one original clock. Rejected Arc values retire after unlock.
        let outcome = {
            let inventory = self.lock();
            match inventory.control.as_ref() {
                Some(control) if clock.bound_to_budget(&control.meter)
                    && self.clock.get().is_none() && control.clock.get().is_none() =>
                    Some((self.clock.set(clock.clone()), control.clock.set(clock.clone()))),
                _ => None,
            }
        };
        let valid = outcome.as_ref().is_some_and(|(a, b)| a.is_ok() && b.is_ok());
        if !valid { self.meter.quarantine(); } // Rejected clock never became this queue's owner.
        drop(outcome);
        valid
    }
    /// The real cold root survives role retirement until the original queue's
    /// last reference; an old Main permit remains Native after Control is gone.
    pub(super) fn uses_budget(&self, meter: &BackingMeter) -> bool {
        self.meter.same_budget(meter)
    }
    pub(super) fn reject_raw(&self) {
        let quiet = self.quiet_scope();
        quiet.mark_quarantined();
    }
    pub(super) fn close(&self) {
        if let Some(control) = &self.lock().control {
            control.closing.store(true, Ordering::Release);
        }
    }
    pub(super) fn is_closed(&self) -> bool {
        self.lock()
            .control
            .as_ref()
            .is_none_or(|c| c.closing.load(Ordering::Acquire))
    }
    pub(super) fn finish(&self) -> bool {
        if self.finishing.swap(true, Ordering::AcqRel) {
            return false;
        }
        let done = self.finish_inner();
        self.finishing.store(false, Ordering::Release);
        done
    }
    fn finish_inner(&self) -> bool {
        if self.disposal_state() != 0 || self.disposal_turn.load(Ordering::Acquire) { return false; }
        for index in 0..2 {
            let role = self.lock().roles[index].take();
            if let Some(role) = role {
                if !role.ready() {
                    self.lock().roles[index] = Some(role);
                    continue;
                }
                match Arc::try_unwrap(role) {
                    Ok(role) => drop(role), // actual Box and stock, outside inventory lock
                    Err(role) => self.lock().roles[index] = Some(role),
                }
            }
        }
        let control = {
            let mut inventory = self.lock();
            if inventory.roles.iter().any(Option::is_some) {
                return false;
            }
            inventory.control.take()
        };
        if let Some(control) = control {
            match Arc::try_unwrap(control) {
                Ok(control) if !control.meter.is_quarantined() => drop(control),
                Ok(control) => {
                    self.lock().control = Some(Arc::new(control));
                    return false;
                }
                Err(control) => {
                    self.lock().control = Some(control);
                    return false;
                }
            }
        }
        true
    }
    pub(super) async fn run<F: Future>(&self, role: Role, work: F) -> Result<F::Output, RunError> {
        let _quiet = self.quiet_scope();
        let signal = self.lock().roles[match role {
            Role::Producer => 0,
            Role::Consumer => 1,
        }]
        .clone();
        let Some(signal) = signal else {
            return Err(if self.meter.is_quarantined() {
                RunError::Quarantined
            } else {
                RunError::Closed
            });
        };
        if signal.control.clock.get().is_none() {
            self.reject_raw();
            return Err(RunError::Quarantined);
        }
        if signal.control.meter.is_quarantined() {
            return Err(RunError::Quarantined);
        }
        let Some(notification) = signal.retirement.clone() else { self.reject_raw(); return Err(RunError::Quarantined); };
        let mut acquisition = Acquisition { signal: signal.clone(), notification, ticket: false };
        // Even a successor cancelled while waiting for the previous callback
        // owns a Work guard before its first poll. Only private queue futures
        // enter here; actor opaque fields have their independent guards.
        let work = Work { future: ManuallyDrop::new(work), signal: &signal };
        let admission = poll_fn(|cx| {
            loop {
                if signal.control.meter.is_quarantined() { return Poll::Ready(Err(RunError::Quarantined)); }
                let (phase, duplicate) = {
                    let mut state = signal.lock();
                    let phase = state.phase;
                    let duplicate = state.acquisition && !acquisition.ticket;
                    if !duplicate {
                        if phase == Phase::Retiring && !acquisition.ticket {
                            state.acquisition = true;
                            acquisition.ticket = true;
                        }
                        if phase == Phase::Idle { state.phase = Phase::Leased; }
                    }
                    (phase, duplicate)
                };
                if duplicate { self.reject_raw(); return Poll::Ready(Err(RunError::Quarantined)); }
                match phase {
                    Phase::Idle => return Poll::Ready(Ok(())),
                    Phase::Closed => return Poll::Ready(Err(RunError::Closed)),
                    Phase::Leased => { self.reject_raw(); return Poll::Ready(Err(RunError::Quarantined)); }
                    Phase::Retiring => {
                        if !acquisition.notification.register(cx.waker()) { return Poll::Ready(Err(RunError::Quarantined)); }
                        if signal.lock().phase == Phase::Retiring { return Poll::Pending; }
                    }
                }
            }
        }).await;
        drop(acquisition); // Original acquisition waiter Drop is outside owner locks.
        admission?;
        let lease = Lease { signal: signal.clone() };
        let proxy = Waker::from(signal.clone());
        let result = {
            let mut work = std::pin::pin!(work);
            poll_fn(|cx| {
                if signal.control.meter.is_quarantined() { return Poll::Ready(Err(RunError::Quarantined)); }
                if !signal.register(cx.waker()) { return Poll::Ready(Err(RunError::Quarantined)); }
                let mut safe = Context::from_waker(&proxy);
                let pinned = unsafe { work.as_mut().map_unchecked_mut(|work| &mut *work.future) };
                match catch_unwind(AssertUnwindSafe(|| pinned.poll(&mut safe))) {
                    Ok(poll) => poll.map(Ok),
                    Err(payload) => { signal.capture(Site::Poll, payload); Poll::Ready(Err(RunError::Quarantined)) }
                }
            }).await
        }; // Actual Work Drop precedes role retirement and final disposition.
        drop(proxy);
        drop(lease);
        if signal.control.meter.is_quarantined() {
            Err(RunError::Quarantined)
        } else {
            result
        }
    }
}

#[cfg(test)]
#[path = "tests/native_wait_observation.rs"]
mod tests;
