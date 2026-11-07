//! Original PumpControl's first-error ledger, fixed opaque terminal sites and
//! STOP/FAIL/PAUSE effects. No queue, task, budget or deadline authority is added.
//! Native payloads stay opaque and charged Q; TunIngressPump owns actual JOIN.
//! Tests: device/tests/pump_panics.rs and pump_native.rs.
use super::*;
use std::{any::Any, mem::ManuallyDrop, sync::atomic::AtomicUsize};

#[derive(Clone, Copy)]
#[repr(usize)]
pub(super) enum Site {
    ReadCreate,
    ReadPoll,
    ReadFutureDrop,
    SourceDrop,
    WorkPoll,
    WorkDrop,
    Notification,
    JoinPanic,
    ReadErrorDrop,
    Receive,
    ReceiverDrop,
    ReceiverDrain,
    StopDrop,
    PauseDrop,
}
impl Site {
    fn phase(self) -> FailurePhase {
        match self {
            Self::ReadCreate | Self::ReadPoll | Self::WorkPoll | Self::ReadErrorDrop => {
                FailurePhase::DeviceRead
            }
            _ => FailurePhase::DeviceJoin,
        }
    }
}
type Payload = ManuallyDrop<Box<dyn Any + Send>>;
pub(super) struct Failures {
    pub(super) first: PumpFailureSlot,
    payloads: [Option<Payload>; 14],
}
impl Default for Failures {
    fn default() -> Self {
        Self {
            first: PumpFailureSlot::Empty,
            payloads: std::array::from_fn(|_| None),
        }
    }
}

pub(super) const STOP: usize = 1;
pub(super) const FAILURE: usize = 2;
pub(super) const PAUSE: usize = 4;
const ACTIVE: usize = 8;
const SEALED: usize = 16;
#[derive(Default)]
pub(super) struct Notifications {
    state: AtomicUsize,
}

impl PumpControl {
    pub(super) fn lock_failures(&self) -> std::sync::MutexGuard<'_, Failures> {
        match self.first_failure.lock() {
            Ok(guard) => guard,
            Err(poison) => {
                if self.native_meter.is_some() {
                    self.quarantine_quiet();
                }
                poison.into_inner()
            }
        }
    }
    pub(super) fn native_layout_bytes() -> usize {
        std::mem::size_of::<Self>()
    }
    pub(super) fn native(meter: &bytes::BackingMeter) -> std::io::Result<Self> {
        let layout = meter.reserve_cold_layout(Self::native_layout_bytes())
            .map_err(quinn::owned_events::AdmissionError)?.ok_or(ErrorKind::Unsupported)?;
        let mut control = Self::new();
        control._native_layout = Some(layout);
        control.native_meter = Some(meter.clone());
        drop(
            control
                .first_failure
                .lock()
                .unwrap_or_else(|e| e.into_inner()),
        );
        Ok(control)
    }
    pub(super) fn native_quarantined(&self) -> bool {
        self.native_meter
            .as_ref()
            .is_some_and(bytes::BackingMeter::is_quarantined)
    }
    pub(super) fn quarantine_quiet(&self) {
        if let Some(meter) = &self.native_meter {
            meter.quarantine();
        }
        self.stop.store(true, Ordering::Release);
    }
    fn retain_first(&self, slots: &mut Failures, failure: RetainedFailure) {
        if matches!(slots.first, PumpFailureSlot::Empty) {
            slots.first = PumpFailureSlot::Retained(failure);
        } else {
            let _ =
                self.additional_failures
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                        Some(n.saturating_add(1))
                    });
        }
        self.failed.store(true, Ordering::Release);
    }
    pub(super) fn record_quiet(&self, failure: RetainedFailure) {
        let mut slots = self.lock_failures();
        self.retain_first(&mut slots, failure);
    }
    pub(super) fn capture(&self, site: Site, payload: Box<dyn Any + Send>) {
        if self.native_meter.is_none() {
            self.record(RetainedFailure::from_panic(
                Self::origin(site.phase()),
                payload,
            ));
            return;
        }
        self.capture_quiet(site, payload);
        if !std::thread::panicking() {
            self.notify(STOP | FAILURE);
        }
    }
    fn capture_quiet(&self, site: Site, payload: Box<dyn Any + Send>) {
        let failure = RetainedFailure::opaque_panic(Self::origin(site.phase()));
        let mut slots = self.lock_failures();
        // Each site is once-only: failed read ends the loop, each Drop consumes
        // one owner, notifications seal, and the original JOIN is consumed once.
        // A corrupted repeated caller must never destroy either unknown payload.
        let payload = ManuallyDrop::new(payload);
        if slots.payloads[site as usize].is_none() {
            slots.payloads[site as usize] = Some(payload);
        }
        self.retain_first(&mut slots, failure);
        drop(slots);
        // Original Box and first facts are retained under one lock before Q.
        self.quarantine_quiet();
    }
    pub(super) fn record_io(&self, error: std::io::Error) {
        if let Some(access) = &self.native_ingress {
            access.note_io(error);
            self.record_callback_failure();
            self.notify(FAILURE);
            return;
        }

        if self.native_meter.is_none() {
            self.record(RetainedFailure::from_io(
                Self::origin(FailurePhase::DeviceRead),
                error,
            ));
            return;
        }
        self.record_quiet(RetainedFailure::from_io_ref(
            Self::origin(FailurePhase::DeviceRead),
            &error,
        ));
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(error))) {
            self.capture_quiet(Site::ReadErrorDrop, payload);
        }
        self.notify(FAILURE);
    }
    pub(super) fn record_callback_failure(&self) -> bool {
        let Some(error) = self.native_ingress.as_ref().and_then(rx_packet::Access::failure) else { return false };
        let kind = error.kind();
        {
            let mut slots = self.lock_failures();
            if matches!(slots.first, PumpFailureSlot::Empty) { slots.first = PumpFailureSlot::Callback(kind); }
            self.failed.store(true, Ordering::Release);
        }
        drop(error); // A derived carrier never destroys its last Arc under Pump's lock.
        true
    }
    pub(super) fn failure_error(self: &Arc<Self>) -> std::io::Error {
        let callback = matches!(self.lock_failures().first, PumpFailureSlot::Empty | PumpFailureSlot::Callback(_) | PumpFailureSlot::CallbackTransferred(_));
        if callback {
            if let Some(error) = self.native_ingress.as_ref().and_then(rx_packet::Access::failure) { return error; }
        }
        if self.native_meter.is_some() { super::failure::error(self) }
        else { std::io::Error::from(self.failure_kind()) }
    }
    pub(super) fn dispose_interrupted(&self, error: std::io::Error) -> bool {
        if self.native_meter.is_none() {
            drop(error);
            return true;
        }
        match catch_unwind(AssertUnwindSafe(|| drop(error))) {
            Ok(()) => true,
            Err(payload) => {
                self.capture(Site::ReadErrorDrop, payload);
                false
            }
        }
    }
    pub(super) fn record_join(&self, error: tokio::task::JoinError) {
        if self.native_meter.is_none() {
            self.record(RetainedFailure::from_join(
                Self::origin(FailurePhase::DeviceJoin),
                error,
            ));
        } else {
            match error.try_into_panic() {
                Ok(payload) => self.capture(Site::JoinPanic, payload),
                Err(cancelled) => self.record(RetainedFailure::from_join(
                    Self::origin(FailurePhase::DeviceJoin),
                    cancelled,
                )),
            }
        }
    }
    fn effect(&self, effect: usize) {
        match effect {
            STOP => self.stop_wake.notify_waiters(),
            FAILURE => self.failure_wake.notify_waiters(),
            PAUSE => self.pause.changed.notify_waiters(),
            _ => (),
        }
    }
    pub(super) fn notify(&self, effects: usize) {
        if self.native_meter.is_some() && std::thread::panicking() {
            self.quarantine_quiet();
            return;
        }
        if self.native_meter.is_none() {
            for effect in [STOP, FAILURE, PAUSE] {
                if effects & effect != 0 {
                    self.effect(effect);
                }
            }
            return;
        }
        let state = &self.notifications.state;
        if state.fetch_or(effects | ACTIVE, Ordering::AcqRel) & (ACTIVE | SEALED) != 0 {
            return;
        }
        loop {
            let pending = state.fetch_and(ACTIVE, Ordering::AcqRel) & (STOP | FAILURE | PAUSE);
            for effect in [STOP, FAILURE, PAUSE] {
                if pending & effect == 0 {
                    continue;
                }
                if let Err(payload) = catch_unwind(AssertUnwindSafe(|| self.effect(effect))) {
                    self.capture_quiet(Site::Notification, payload);
                    state.store(SEALED, Ordering::Release);
                    return;
                }
            }
            if state
                .compare_exchange(ACTIVE, 0, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return;
            }
        }
    }
}
