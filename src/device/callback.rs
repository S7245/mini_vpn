//! Device cold callback transaction. RX/TX own independent fixed roles and share
//! the original first-error/claim ledger. Each runner owns its original Waker;
//! one slot owns the next registration. Clone/replace/cancel remain on the unique
//! writer, all callbacks run unlocked, and failure seals further admission. Wake
//! borrows its Waker before separately disposing it, so two panics cannot unwind
//! through each other. Fixed failure sites retain original opaque Boxes. The
//! exact State allocation stays charged through the last in-flight runner Arc.
use bytes::{BackingBudgetReservation, BackingMeter};
use std::{
    alloc::{Layout, alloc},
    any::Any,
    io,
    mem::ManuallyDrop,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Mutex},
    task::{Wake, Waker},
};
type Payload = ManuallyDrop<Box<dyn Any + Send>>;
#[derive(Clone, Copy, Debug)]
#[repr(usize)]
enum Site {
    Clone,
    Replace,
    Wake,
    WakeDrop,
    Retire,
    NotifyDrop,
    IoDrop,
    LaterIoDrop,
}
const SITES: usize = 8;
#[derive(Clone)]
enum First {
    Io(usize, IoState),
    Panic(usize, Site),
}
#[derive(Clone)]
struct IoState {
    source: Option<Arc<io::Error>>,
    facts: crate::shared::errors::RetainedFailure,
}
fn source_phase(role: usize) -> crate::shared::errors::FailurePhase {
    if role == RX { crate::shared::errors::FailurePhase::DeviceRead }
    else { crate::shared::errors::FailurePhase::DeviceWrite }
}
const TX: usize = 0;
const RX: usize = 1;
const ROLES: usize = 2;
#[derive(Default)]
struct RoleState {
    waiter: Option<Waker>,
    active: bool,
    pending: bool,
    sealed: bool,
    retiring: bool,
    payloads: [Option<Payload>; SITES],
    later_io: Option<ManuallyDrop<io::Error>>,
}
struct State {
    roles: [RoleState; ROLES],
    first: Option<First>,
    reported: usize,
    io_count: usize,
    io_disposal_active: bool,
    terminal_claimed: bool,
}
impl Default for State {
    fn default() -> Self {
        let mut roles = std::array::from_fn(|_| RoleState::default());
        roles[RX].sealed = true; // Legacy construction has only its original TX role.
        Self { roles, first: None, reported: 0, io_count: 0, io_disposal_active: false, terminal_claimed: false }
    }
}
struct Core {
    role_count: usize,
    state: ManuallyDrop<Mutex<Box<State>>>,
    meter: BackingMeter,
    _layout: ManuallyDrop<BackingBudgetReservation>,
}
struct Inner {
    core: Arc<Core>,
    role: usize,
}
impl std::ops::Deref for Inner {
    type Target = Core;
    fn deref(&self) -> &Core { &self.core }
}
#[derive(Clone)]
pub(crate) struct Custody(Arc<Inner>);
struct CallbackFailure(Custody);
impl std::fmt::Debug for CallbackFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
impl std::fmt::Display for CallbackFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0.0.lock().first {
            Some(First::Panic(_, site)) => write!(f, "Native Device callback {site:?} failed"),
            _ => f.write_str("Native Device original write failure"),
        }
    }
}
impl std::error::Error for CallbackFailure {}
pub(crate) struct Report {
    pub(crate) failure: Option<crate::shared::errors::RetainedFailure>,
    pub(crate) source: Option<Arc<io::Error>>,
    pub(crate) additional: u64,
    pub(crate) source_phase: Option<crate::shared::errors::FailurePhase>,
    pub(crate) unknown: bool,
    pub(crate) source_guard: Option<SourceGuard>,
}
/// Facts claimed exactly once, before a Session supplies its diagnostic origin.
/// Public Device settlement retains this value privately in the same receipt.
pub(crate) struct Claim {
    first: Option<First>,
    source: Option<Arc<io::Error>>,
    additional: u64,
    phase: Option<crate::shared::errors::FailurePhase>,
    unknown: bool,
    custody: Custody,
}
impl Claim {
    pub(crate) fn report(self, mut origin: crate::shared::errors::FailureOrigin) -> Report {
        if let Some(First::Io(role, _) | First::Panic(role, _)) = &self.first {
            origin.phase = if *role == RX { crate::shared::errors::FailurePhase::DeviceRead }
                else { crate::shared::errors::FailurePhase::DeviceWrite };
        }
        use crate::shared::errors::RetainedFailure;
        let failure = match self.first {
            Some(First::Io(_, original)) => Some(original.facts.with_origin(origin)),
            Some(First::Panic(_, _)) => Some(RetainedFailure::opaque_panic(origin)),
            None => None,
        };
        let source_guard = self.source.as_ref().map(|source| SourceGuard {
            custody: self.custody.clone(), original: Arc::downgrade(source), phase: self.phase.clone(),
        });
        Report { failure, source: self.source, additional: self.additional,
            source_phase: self.phase, unknown: self.unknown, source_guard }

    }
}
pub(crate) fn original_io(error: &io::Error) -> Option<Arc<io::Error>> {
    let callback = error.get_ref()?.downcast_ref::<CallbackFailure>()?;
    let state = callback.0.0.lock();
    match &state.first {
        Some(First::Io(_, original)) => original.source.clone(),
        _ => None,
    }
}
pub(crate) fn report(
    error: &io::Error,
    origin: crate::shared::errors::FailureOrigin,
) -> Option<Report> {
    let Some(callback) = error.get_ref().and_then(|inner| inner.downcast_ref::<CallbackFailure>()) else {
        return super::failure::report(error);
    };
    Some(callback.0.claim(origin))
}
pub(super) fn layout_bytes() -> usize {
    layout_for(1)
}
/// Cold bytes `Custody::pair` reserves (one Core with both Device roles).
pub(super) fn pair_layout_bytes() -> usize {
    layout_for(ROLES)
}
fn layout_for(roles: usize) -> usize {
    Layout::new::<State>().size() + Layout::new::<Core>().size()
        + roles * Layout::new::<Inner>().size()
}
impl Core {
    fn lock(&self) -> std::sync::MutexGuard<'_, Box<State>> {
        self.state.lock().unwrap_or_else(|e| {
            self.meter.quarantine();
            e.into_inner()
        })
    }
}
impl Inner {
    fn capture(&self, site: Site, payload: Box<dyn Any + Send>) {
        let mut state = self.lock();
        let payload = ManuallyDrop::new(payload);
        // Each site can fail once: sealing prevents new writers/registrations;
        // only one runner, one registration and one cancellation can be in flight.
        if state.roles[self.role].payloads[site as usize].is_none() {
            state.roles[self.role].payloads[site as usize] = Some(payload);
        }
        if state.first.is_none() {
            state.first = Some(First::Panic(self.role, site));
        }
        for role in &mut state.roles { role.sealed = true; role.pending = false; }
        drop(state);
        self.meter.quarantine();
    }
    fn dispose(&self, site: Site, waiter: Option<Waker>) {
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(waiter))) {
            self.capture(site, payload);
        }
    }
    fn error(self: &Arc<Self>) -> io::Error {
        let kind = match &self.lock().first {
            Some(First::Io(_, original)) => original.facts.facts().io_kind.unwrap_or(io::ErrorKind::Other),
            Some(First::Panic(_, _)) => io::ErrorKind::Other,
            None if self.meter.admit_precharged().is_err() => io::ErrorKind::BrokenPipe,
            None => return crate::device::device_closed(),
        };
        io::Error::new(kind, CallbackFailure(Custody(self.clone())))
    }
    fn admit(self: &Arc<Self>) -> io::Result<()> {
        if self.meter.admit_precharged().is_err() || self.lock().roles[self.role].sealed {
            Err(self.error())
        } else {
            Ok(())
        }
    }
    fn fire(&self) {
        {
            let mut state = self.lock();
            if (state.roles[self.role].sealed && !state.roles[self.role].retiring) || self.meter.admit_precharged().is_err() {
                state.roles[self.role].sealed = true;
                state.roles[self.role].pending = false;
                return;
            }
            state.roles[self.role].pending = true;
            if state.roles[self.role].active {
                return;
            }
            state.roles[self.role].active = true;
        }
        loop {
            let waiter = {
                let mut state = self.lock();
                if (state.roles[self.role].sealed && !state.roles[self.role].retiring) || !state.roles[self.role].pending {
                    state.roles[self.role].active = false;
                    return;
                }
                state.roles[self.role].pending = false;
                state.roles[self.role].waiter.take()
            };
            if let Some(waiter) = waiter {
                if std::thread::panicking() {
                    self.meter.quarantine();
                } else if self.meter.admit_precharged().is_ok() {
                    if let Err(payload) = catch_unwind(AssertUnwindSafe(|| waiter.wake_by_ref())) {
                        self.capture(Site::Wake, payload);
                    }
                }
                // The original Waker is still owned outside the wake catch.
                self.dispose(Site::WakeDrop, Some(waiter));
            }
        }
    }
}
impl Wake for Inner {
    fn wake(self: Arc<Self>) {
        self.fire();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.fire();
    }
}
impl Custody {
    pub(super) fn rx_authority(&self, meter: &BackingMeter) -> bool {
        self.0.role == RX && self.0.role_count == ROLES && self.0.meter.same_authority(meter)
            && meter.cold_layout_matches(Some(&*self.0._layout), layout_for(ROLES))
    }
    pub(super) fn matches_rx(&self, meter: &BackingMeter) -> bool { self.rx_authority(meter) && self.admit().is_ok() }
    pub(super) fn matches(&self, meter: &BackingMeter) -> bool {
        self.0.meter.same_authority(meter)
            && meter.cold_layout_matches(Some(&*self.0._layout), layout_for(self.0.role_count))
            && self.admit().is_ok()
    }
    pub(super) fn new(meter: &BackingMeter) -> io::Result<Self> { Self::new_roles(meter, 1) }
    fn new_roles(meter: &BackingMeter, role_count: usize) -> io::Result<Self> {
        let layout = Layout::new::<State>();
        let reserved = meter
            .reserve_cold_layout(layout_for(role_count))
            .map_err(quinn::owned_events::AdmissionError)?
            .ok_or_else(|| io::Error::from(io::ErrorKind::Unsupported))?;
        let pointer = unsafe { alloc(layout).cast::<State>() };
        if pointer.is_null() {
            return Err(io::ErrorKind::OutOfMemory.into());
        }
        unsafe { pointer.write(State::default()) };
        let core = Arc::new(Core {
            role_count,
            state: ManuallyDrop::new(Mutex::new(unsafe { Box::from_raw(pointer) })),
            meter: meter.clone(),
            _layout: ManuallyDrop::new(reserved),
        });
        drop(core.lock());
        Ok(Self(Arc::new(Inner { core, role: TX })))
    }
    /// Called only by the original cold Device composition, before source creation.
    pub(super) fn pair(meter: &BackingMeter) -> io::Result<(Self, Self)> {
        let tx = Self::new_roles(meter, ROLES)?;
        tx.0.lock().roles[RX].sealed = false;
        let rx = Self(Arc::new(Inner { core: tx.0.core.clone(), role: RX }));
        Ok((tx, rx))
    }
    pub(super) fn notify(&self) { self.0.fire(); }
    fn close_all(&self) {
        let waiters: [Option<Waker>; ROLES] = {
            let mut state = self.0.lock();
            for role in &mut state.roles { role.sealed = true; role.retiring = false; role.pending = false; }
            std::array::from_fn(|i| state.roles[i].waiter.take())
        };
        for (index, waiter) in waiters.into_iter().enumerate() {
            let role = Inner { core: self.0.core.clone(), role: index };
            role.dispose(Site::Retire, waiter);
        }
    }
    pub(crate) fn admit(&self) -> io::Result<()> {
        self.0.admit()
    }
    pub(crate) fn register(&self, waker: &Waker) -> io::Result<Waker> { self.register_inner(waker, false) }
    pub(super) fn begin_retirement(&self) {
        let mut state = self.0.lock();
        if !state.terminal_claimed { state.roles[self.0.role].retiring = true; }
    }
    pub(super) fn register_retirement(&self, waker: &Waker) -> io::Result<Waker> { self.register_inner(waker, true) }
    fn admit_registration(&self, retirement: bool) -> io::Result<()> {
        if retirement {
            let valid = { let state = self.0.lock(); state.roles[self.0.role].retiring && !state.terminal_claimed };
            if valid && self.0.meter.admit_precharged().is_ok() { return Ok(()); }
        }
        self.admit()
    }
    fn register_inner(&self, waker: &Waker, retirement: bool) -> io::Result<Waker> {
        self.admit_registration(retirement)?;
        let same = self
            .0
            .lock()
            .roles[self.0.role].waiter
            .as_ref()
            .is_some_and(|old| old.will_wake(waker));
        if !same {
            let new = match catch_unwind(AssertUnwindSafe(|| waker.clone())) {
                Ok(new) => new,
                Err(payload) => {
                    self.0.capture(Site::Clone, payload);
                    return Err(self.0.error());
                }
            };
            let old = {
                let mut state = self.0.lock();
                if state.roles[self.0.role].sealed && !(retirement && state.roles[self.0.role].retiring) {
                    Some(new)
                } else {
                    state.roles[self.0.role].waiter.replace(new)
                }
            };
            self.0.dispose(Site::Replace, old);
        }
        self.admit_registration(retirement)?;
        Ok(Waker::from(self.0.clone()))
    }
    pub(crate) fn retire(&self) {
        let waiter = self.0.lock().roles[self.0.role].waiter.take();
        self.0.dispose(Site::Retire, waiter);
    }
    pub(crate) fn retain_secondary(&self, payload: Box<dyn Any + Send>) {
        self.0.capture(Site::NotifyDrop, payload);
    }
    pub(crate) fn note_write(&self, result: io::Result<()>) {
        let Err(error) = result else { return };
        // Our own derived carrier is already in this ledger; retaining it here
        // would create a self-cycle and invent a second failure.
        if error
            .get_ref()
            .and_then(|e| e.downcast_ref::<CallbackFailure>())
            .is_some_and(|e| Arc::ptr_eq(&e.0.0.core, &self.0.core))
        {
            return;
        }
        let mut state = self.0.lock();
        if state.first.is_none() {
            let facts = crate::shared::errors::RetainedFailure::from_io_ref(
                super::PumpControl::origin(source_phase(self.0.role)), &error,
            );
            state.first = Some(First::Io(self.0.role, IoState { source: Some(Arc::new(error)), facts }));
            state.io_count += 1;
        } else if state.roles[self.0.role].later_io.is_none() {
            state.roles[self.0.role].later_io = Some(ManuallyDrop::new(error));
            state.io_count += 1;
        }
        for role in &mut state.roles { role.sealed = true; role.pending = false; }
    }
    pub(crate) fn finish(&self) -> io::Result<()> {
        if self.0.lock().first.is_some() || self.0.meter.admit_precharged().is_err() {
            Err(self.0.error())
        } else {
            Ok(())
        }
    }
    pub(crate) fn has_failure(&self) -> bool {
        self.0.lock().first.is_some()
    }
    pub(crate) fn status(&self) -> Status {
        Status(self.clone())
    }
    pub(crate) fn claim(&self, origin: crate::shared::errors::FailureOrigin) -> Report {
        self.claim_inner(false).report(origin)
    }
    fn claim_inner(&self, terminal: bool) -> Claim {
        let mut state = self.0.lock();
        let count = state.roles.iter().map(|role|
            role.payloads.iter().filter(|p| p.is_some()).count()
        ).sum::<usize>() + state.io_count;
        let first_report = state.reported == 0;
        let source = match &state.first {
            Some(First::Io(_, original)) => original.source.clone(),
            _ => None,
        };
        let first = first_report.then(|| state.first.clone()).flatten();
        let additional = count
            .saturating_sub(state.reported)
            .saturating_sub(usize::from(first.is_some())) as u64;
        state.reported = count;
        if terminal {
            state.terminal_claimed = true;
        }
        let phase = state.first.as_ref().map(|first| match first {
            First::Io(role, _) | First::Panic(role, _) => source_phase(*role),
        });
        let unknown = self.0.meter.is_quarantined() || state.roles.iter().any(|role| role.payloads.iter().any(Option::is_some));
        Claim { first, source, additional, phase, unknown, custody: self.clone() }
    }
    pub(crate) fn pending(&self) -> bool {
        let state = self.0.lock();
        state.roles[self.0.role].active || state.roles[self.0.role].waiter.is_some()
    }
    async fn wait_closed(&self) {
        std::future::poll_fn(|cx| {
            if !self.0.lock().roles.iter().any(|r| r.active || r.waiter.is_some()) {
                return std::task::Poll::Ready(());
            }
            // Only the borrowed shutdown future schedules its next inventory
            // check. No peer-side completion callback can fail after claim.
            // Its caller catches this poll before the actual Device can unwind.
            cx.waker().wake_by_ref();
            std::task::Poll::Pending
        })
        .await
    }
    pub(crate) fn close(&self) {
        {
            let mut state = self.0.lock();
            state.roles[self.0.role].sealed = true;
            state.roles[self.0.role].pending = false;
        }
        self.retire();
    }
}
/// Read-only view of the same physical ledger; no close, claim or delta rights.
#[derive(Clone)]
pub(crate) struct Status(Custody);
impl Status {
    pub(crate) fn state(&self) -> crate::device::DeviceShutdownState {
        use crate::device::DeviceShutdownState;
        let state = self.0.0.lock();
        if !state.terminal_claimed {
            DeviceShutdownState::PendingCallbacks
        } else if state.first.is_some() || self.0.0.meter.admit_precharged().is_err() {
            DeviceShutdownState::FailedSettled
        } else {
            DeviceShutdownState::Settled
        }
    }
    pub(crate) fn error(&self) -> io::Error {
        self.0.0.error()
    }
}
/// Unique terminal observer transferred out of the original Egress. Its actual
/// cold State and failure ledger survive the Device while an old wake completes.
pub(crate) struct Tail {
    custody: Custody,
    stock: Option<super::rx_packet::Stock>,
    settled: bool,
}
impl Tail {
    pub(super) fn new(custody: Custody) -> Self {
        custody.close_all();
        Self { custody, stock: None, settled: false }
    }
    pub(super) fn with_stock(custody: Custody, stock: Option<super::rx_packet::Stock>) -> Self {
        let Some(stock) = stock else { return Self::new(custody) };
        custody.close(); // Stop TX DATA; RX stays available only to physical retirement.
        stock.begin_retirement();
        Self { custody, stock: Some(stock), settled: false }
    }
    pub(crate) async fn settle(&mut self) -> Claim {
        if let Some(stock) = &self.stock {
            std::future::poll_fn(|cx| stock.poll_retire(cx)).await;
            drop(self.stock.take());
            self.custody.close_all();
        }
        self.custody.wait_closed().await;
        let report = self.custody.claim_inner(true);
        self.settled = true;
        report
    }
}
impl Drop for Tail {
    fn drop(&mut self) {
        if !self.settled {
            self.custody.0.meter.quarantine();
            self.custody.close_all();
        }
    }
}

impl Drop for Core {
    fn drop(&mut self) {
        let state = self.state.get_mut().unwrap_or_else(|e| { self.meter.quarantine(); e.into_inner() });
        for role in &mut state.roles { role.sealed = true; role.pending = false; }
        for (index, role) in state.roles.iter_mut().enumerate() {
            let waiter = role.waiter.take();
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(waiter))) {
                role.payloads[Site::Retire as usize] = Some(ManuallyDrop::new(payload));
                if state.first.is_none() { state.first = Some(First::Panic(index, Site::Retire)); }
                self.meter.quarantine();
            }
        }
        if let Some(First::Io(role, original)) = &mut state.first {
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(original.source.take()))) {
                state.roles[*role].payloads[Site::IoDrop as usize] = Some(ManuallyDrop::new(payload));
                self.meter.quarantine();
            }
        }
        for role in &mut state.roles {
            if let Some(original) = role.later_io.take() {
                if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(ManuallyDrop::into_inner(original)))) {
                    role.payloads[Site::LaterIoDrop as usize] = Some(ManuallyDrop::new(payload));
                    self.meter.quarantine();
                }
            }
        }
        if state.roles.iter().any(|role| role.payloads.iter().any(Option::is_some)) {
            self.meter.quarantine();
            // The actual State Box, fixed original Box slots, and allowance stay
            // retained together. Arc headers remain separately instance bounded.
            return;
        }
        unsafe { ManuallyDrop::drop(&mut self.state); ManuallyDrop::drop(&mut self._layout); }
    }
}
/// The original Core keeps the strong first source while diagnostic holders
/// release their own Arcs. No second IO owner, failure ledger or allocation.
#[derive(Clone)]
pub(crate) struct SourceGuard {
    custody: Custody,
    original: std::sync::Weak<io::Error>,
    phase: Option<crate::shared::errors::FailurePhase>,
}
impl SourceGuard {
    pub(crate) fn matches(&self, source: &Arc<io::Error>, phase: Option<crate::shared::errors::FailurePhase>, meter: Option<&BackingMeter>) -> bool {
        let state = self.custody.0.lock();
        matches!(&state.first, Some(First::Io(role, original)) if
            original.source.as_ref().is_some_and(|s| Arc::ptr_eq(s, source))
            && phase == Some(source_phase(*role)) && phase == self.phase)
            && self.original.ptr_eq(&Arc::downgrade(source))
            && meter.is_none_or(|m| self.custody.0.meter.same_authority(m))
    }
    pub(crate) fn quarantine(&self) { self.custody.0.meter.quarantine(); }
    pub(crate) fn try_dispose(&self) -> bool {
        let original = {
            let mut state = self.custody.0.lock();
            if !state.terminal_claimed || state.io_disposal_active { return false; }
            state.io_disposal_active = true;
            match &mut state.first {
                Some(First::Io(role, io)) => io.source.take().map(|source| (*role, source)),
                _ => None,
            }
        };
        if let Some((role, original)) = original {
            let original = match Arc::try_unwrap(original) {
                Ok(original) => original,
                Err(original) => {
                    let mut state = self.custody.0.lock();
                    if let Some(First::Io(_, io)) = &mut state.first { io.source = Some(original); }
                    state.io_disposal_active = false;
                    return false;
                }
            };
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(original))) {
                Inner { core: self.custody.0.core.clone(), role }.capture(Site::IoDrop, payload);
            }
        }
        for role in 0..ROLES {
            let original = self.custody.0.lock().roles[role].later_io.take();
            if let Some(original) = original {
                if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(ManuallyDrop::into_inner(original)))) {
                    Inner { core: self.custody.0.core.clone(), role }.capture(Site::LaterIoDrop, payload);
                }
            }
        }
        self.custody.0.lock().io_disposal_active = false;
        true
    }
    pub(crate) fn claim(&self, origin: crate::shared::errors::FailureOrigin) -> Report { self.custody.claim(origin) }
}

#[cfg(test)]
mod tests;
