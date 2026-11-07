//! Holds original control reservations through a composition-root commit.
//! The lane owns slots and poison; the caller owns Root state and deferred wakes.
//! No callbacks run in a held commit. Cancellation returns the original ticket.
//! Entry: ControlPermit::hold; tests: owned_events/tests/control.rs.
use super::*;
use std::{
    ops::{Deref, DerefMut},
    sync::MutexGuard,
};

pub(super) struct LaneGuard<'a, T> {
    state: MutexGuard<'a, State<T>>,
    meter: &'a BackingMeter,
}
impl<T> Deref for LaneGuard<'_, T> {
    type Target = State<T>;
    fn deref(&self) -> &Self::Target {
        &self.state
    }
}
impl<T> DerefMut for LaneGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.state
    }
}
impl<T> Drop for LaneGuard<'_, T> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.meter.quarantine();
            self.state.failed = true;
            self.state.sender_alive = false;
            self.state.receiver_alive = false;
        }
    }
}
impl<T> Inner<T> {
    pub(super) fn lock(&self) -> LaneGuard<'_, T> {
        let mut state = self.state.lock().unwrap_or_else(|poison| {
            self.meter.quarantine();
            let mut state = poison.into_inner();
            state.failed = true;
            state.sender_alive = false;
            state.receiver_alive = false;
            state
        });
        if self.meter.is_quarantined() {
            state.failed = true;
            state.sender_alive = false;
            state.receiver_alive = false;
        }
        LaneGuard {
            state,
            meter: &self.meter,
        }
    }
    pub(super) fn wake(&self, waker: Waker) {
        wake(&self.meter, waker);
    }
}

/// Opaque callbacks are invoked only after the composition root unlocks.
/// During an existing unwind, disposal cannot replace the original panic.
pub struct ControlWake {
    waker: Option<Waker>,
    meter: BackingMeter,
}
fn wake(meter: &BackingMeter, waker: Waker) {
    let unwinding = std::thread::panicking();
    if unwinding {
        meter.quarantine();
    }
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if unwinding {
            drop(waker);
        } else {
            waker.wake();
        }
    }));
    if let Err(payload) = result {
        meter.quarantine();
        if unwinding {
            std::mem::forget(payload);
        } else {
            std::panic::resume_unwind(payload);
        }
    }
}
impl ControlWake {
    pub fn wake(mut self) {
        if let Some(waker) = self.waker.take() {
            wake(&self.meter, waker);
        }
    }
}
impl Drop for ControlWake {
    fn drop(&mut self) {
        let Some(waker) = self.waker.take() else {
            return;
        };
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(waker)));
        if let Err(payload) = result {
            self.meter.quarantine();
            if std::thread::panicking() {
                std::mem::forget(payload);
            } else {
                std::panic::resume_unwind(payload);
            }
        }
    }
}

/// Exclusive custody of a validated physical slot until commit or cancellation.
/// Borrows the original ticket; cancellation unlocks before that ticket drops.
pub struct HeldControl<'a, T> {
    state: LaneGuard<'a, T>,
    armed: &'a mut bool,
    kind: ControlKind,
    index: usize,
    seq: u64,
}
/// Published slot still locked until all peers in the batch are installed.
pub struct PublishedControl<'a, T> {
    _state: LaneGuard<'a, T>,
    wake: Option<ControlWake>,
}
impl<T> PublishedControl<'_, T> {
    pub fn unlock(self) -> Option<ControlWake> {
        self.wake
    }
}
impl<'a, T> HeldControl<'a, T> {
    /// Consumes the original permit before unlocking; no fallible work remains.
    pub fn commit(mut self, value: T) -> PublishedControl<'a, T> {
        self.state.slots[self.index].seq = self.seq;
        self.state.slots[self.index].value = Some(value);
        self.state.control_reserved[self.kind.index()] = None;
        *self.armed = false;
        let wake = self.state.receiver_waker.take().map(|waker| ControlWake {
            waker: Some(waker),
            meter: self.state.meter.clone(),
        });
        PublishedControl {
            _state: self.state,
            wake,
        }
    }
}
impl<T> ControlPermit<T> {
    pub fn same_lane(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
    pub fn belongs_to(&self, sender: &Sender<T>, kind: ControlKind) -> bool {
        self.armed && self.kind == kind && Arc::ptr_eq(&self.inner, &sender.0)
    }
    pub fn hold(&mut self) -> Result<HeldControl<'_, T>, ReserveError> {
        self.inner
            .meter
            .admit_precharged()
            .map_err(|_| ReserveError::Closed)?;
        let state = self.inner.lock();
        let index = state.control_indices.ok_or(ReserveError::Closed)?[self.kind.index()];
        if !self.armed
            || !state.sender_alive
            || !state.receiver_alive
            || state.control_reserved[self.kind.index()] != Some(self.seq)
            || state.slots[index].value.is_some()
        {
            return Err(ReserveError::Closed);
        }
        Ok(HeldControl {
            state,
            armed: &mut self.armed,
            kind: self.kind,
            index,
            seq: self.seq,
        })
    }
    pub fn send(mut self, value: T) -> Result<(), T> {
        let wake = match self.hold() {
            Ok(held) => held.commit(value).unlock(),
            Err(_) => return Err(value),
        };
        if let Some(wake) = wake {
            wake.wake();
        }
        Ok(())
    }
}
impl<T> Drop for ControlPermit<T> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let wake = {
            let mut state = self.inner.lock();
            if state.control_reserved[self.kind.index()] == Some(self.seq) {
                state.control_reserved[self.kind.index()] = None;
            } else {
                self.inner.meter.quarantine();
            }
            state.receiver_waker.take()
        };
        self.armed = false;
        if let Some(waker) = wake {
            self.inner.wake(waker);
        }
    }
}
