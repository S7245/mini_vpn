//! Lane-local custody for opaque waiting callbacks. Locks only move Wakers;
//! clone, wake and retirement run outside them. The first payload survives
//! remaining callback disposal, and every interruption marks the original meter.
//! This owns no protocol or Root settlement. Tests: tests/waiters.rs.
use super::*;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};

pub(super) struct Callbacks<'a> {
    pub(super) slots: [Option<Waker>; 2],
    wake: bool,
    meter: &'a BackingMeter,
}
impl<'a> Callbacks<'a> {
    pub(super) fn new(meter: &'a BackingMeter) -> Self {
        Self {
            slots: [None, None],
            wake: false,
            meter,
        }
    }
    pub(super) fn clone_waiter(&mut self, waker: &Waker) {
        match catch_unwind(AssertUnwindSafe(|| waker.clone())) {
            Ok(waker) => self.slots[0] = Some(waker),
            Err(payload) => {
                self.meter.quarantine();
                resume_unwind(payload);
            }
        }
    }
    pub(super) fn wake(meter: &'a BackingMeter, pair: (Option<Waker>, Option<Waker>)) {
        let callbacks = Self {
            slots: [pair.0, pair.1],
            wake: true,
            meter,
        };
        drop(callbacks);
    }
}
impl Drop for Callbacks<'_> {
    fn drop(&mut self) {
        let unwinding = std::thread::panicking();
        let mut first = None;
        for slot in &mut self.slots {
            let Some(waker) = slot.take() else { continue };
            if unwinding {
                self.meter.quarantine();
            }
            let result = catch_unwind(AssertUnwindSafe(|| {
                if self.wake && !unwinding && first.is_none() {
                    waker.wake();
                } else {
                    drop(waker);
                }
            }));
            if let Err(payload) = result {
                self.meter.quarantine();
                if unwinding || first.is_some() {
                    std::mem::forget(payload);
                } else {
                    first = Some(payload);
                }
            }
        }
        if let Some(payload) = first {
            resume_unwind(payload);
        }
    }
}

impl<T> Drop for Inner<T> {
    fn drop(&mut self) {
        let mut callbacks = Callbacks::new(&self.meter);
        let state = match self.state.get_mut() {
            Ok(state) => state,
            Err(poison) => {
                self.meter.quarantine();
                poison.into_inner()
            }
        };
        callbacks.slots = [state.receiver_waker.take(), state.producer_waker.take()];
        let unwinding = std::thread::panicking();
        if unwinding {
            self.meter.quarantine();
        }
        let mut first = None;
        for slot in state.slots.iter_mut() {
            let Some(value) = slot.value.take() else {
                continue;
            };
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(value))) {
                self.meter.quarantine();
                if unwinding || first.is_some() {
                    std::mem::forget(payload);
                } else {
                    first = Some(payload);
                }
            }
        }
        // Callback custody drops during this unwind and cannot replace first.
        if let Some(payload) = first {
            resume_unwind(payload);
        }
    }
}
