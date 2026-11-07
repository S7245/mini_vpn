//! The existing pacing mutex is the only state authority. Native poison and
//! opaque callback failures share its original meter; no lease may clear Q.
//! State guards move bounded retired callbacks outside the lock before disposal.
//! Entry: Inner::lock/opaque and Guard; tests: endpoint_pacing/tests/authority.
use super::*;
use std::{
    ops::{Deref, DerefMut},
    sync::atomic::Ordering,
};

pub(super) struct Guard<'a> {
    state: Option<MutexGuard<'a, StateStorage>>,
    owner: &'a EndpointPacingServiceInner,
}
impl Deref for Guard<'_> {
    type Target = EndpointPacingState;
    fn deref(&self) -> &Self::Target {
        self.state.as_ref().expect("live pacing guard")
    }
}
impl DerefMut for Guard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.state.as_mut().expect("live pacing guard")
    }
}
impl Guard<'_> {
    pub(super) fn unavailable(&self) -> bool {
        self.owner.unavailable()
    }
    pub(super) fn validate_control(&self) -> bool {
        if self.unavailable() {
            return false;
        }
        if self.owner.meter.is_none() {
            return true;
        }
        let valid = self.inventory_valid(self.owner.config.burst_bytes);
        if !valid {
            self.owner.quarantine();
        }
        valid
    }
}
impl Drop for Guard<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() || self.callback_overflow {
            self.owner.quarantine();
        }
        let retired = std::mem::take(&mut self.retired_wakers);
        drop(self.state.take());
        self.owner.dispose_wakers(retired);
    }
}
pub(super) struct PreparedWaker<'a> {
    owner: &'a EndpointPacingServiceInner,
    value: Option<Waker>,
}
impl PreparedWaker<'_> {
    pub(super) fn take(&mut self) -> Option<Waker> {
        self.value.take()
    }
    pub(super) fn wake(mut self) {
        if let Some(waker) = self.value.take() {
            self.owner.wake(waker);
        }
    }
}
impl Drop for PreparedWaker<'_> {
    fn drop(&mut self) {
        self.owner.dispose_wakers([self.value.take()]);
    }
}
/// Owns opaque effects until the caller installs the corresponding path and key.
/// Cancellation after ledger commit is uncertain and retains the original charge.
pub(crate) struct MigrationEffects {
    owner: Arc<EndpointPacingServiceInner>,
    retired: [Option<Waker>; 2],
    wake: Option<Waker>,
    committed: bool,
}
impl MigrationEffects {
    pub(super) fn new(
        owner: &Arc<EndpointPacingServiceInner>,
        state: &mut Guard<'_>,
        wake: Option<Waker>,
    ) -> Self {
        Self {
            owner: owner.clone(),
            retired: std::mem::take(&mut state.retired_wakers),
            wake,
            committed: false,
        }
    }
    pub(crate) fn finish(mut self) {
        self.committed = true;
        self.owner.dispose_wakers(std::mem::take(&mut self.retired));
        if let Some(waker) = self.wake.take() {
            self.owner.wake(waker);
        }
    }
}
impl Drop for MigrationEffects {
    fn drop(&mut self) {
        if !self.committed || std::thread::panicking() {
            self.owner.quarantine();
        }
        let [a, b] = std::mem::take(&mut self.retired);
        self.owner.dispose_wakers([a, b, self.wake.take()]);
    }
}
impl EndpointPacingServiceInner {
    pub(super) fn hold_waker(&self, value: Option<Waker>) -> PreparedWaker<'_> {
        PreparedWaker { owner: self, value }
    }
    pub(super) fn prepare_waker(&self, waker: Option<&Waker>) -> PreparedWaker<'_> {
        PreparedWaker {
            owner: self,
            value: self.opaque(|| waker.cloned()),
        }
    }
    pub(super) fn quarantine(&self) {
        self.failed.store(true, Ordering::Release);
        if let Some(meter) = &self.meter {
            meter.quarantine();
        }
    }
    pub(super) fn unavailable(&self) -> bool {
        self.failed.load(Ordering::Acquire)
            || self
                .meter
                .as_ref()
                .is_some_and(bytes::BackingMeter::is_quarantined)
    }
    pub(super) fn lock(&self) -> Guard<'_> {
        let state = self.state.lock().unwrap_or_else(|poison| {
            self.quarantine();
            poison.into_inner()
        });
        Guard {
            state: Some(state),
            owner: self,
        }
    }
    pub(super) fn opaque<R>(&self, action: impl FnOnce() -> R) -> R {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(action)) {
            Ok(value) => value,
            Err(payload) => {
                self.quarantine();
                std::panic::resume_unwind(payload)
            }
        }
    }
    pub(super) fn wake(&self, waker: Waker) {
        if std::thread::panicking() {
            self.quarantine();
            self.dispose_wakers([Some(waker)]);
        } else {
            self.opaque(|| waker.wake());
        }
    }
    pub(super) fn dispose_wakers<const N: usize>(&self, wakers: [Option<Waker>; N]) {
        let mut first = None;
        for waker in wakers {
            if let Err(payload) =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(waker)))
            {
                self.quarantine();
                if !std::thread::panicking() && first.is_none() {
                    first = Some(payload);
                } else {
                    std::mem::forget(payload);
                }
            }
        }
        if let Some(payload) = first {
            std::panic::resume_unwind(payload);
        }
    }
}
impl EndpointPacingState {
    pub(super) fn retire_waker(&mut self, waker: Option<Waker>) {
        let Some(waker) = waker else { return };
        if let Some(slot) = self.retired_wakers.iter_mut().find(|slot| slot.is_none()) {
            *slot = Some(waker);
        } else {
            // At most replacement plus grant/cancellation can retire a callback
            // in one locked operation. Overflow is corruption, never a Drop in lock.
            self.callback_overflow = true;
            std::mem::forget(waker);
        }
    }
    fn inventory_valid(&self, burst: u64) -> bool {
        if self.callback_overflow
            || !self.connections.valid()
            || !self.control_waiters.valid()
            || !self.bulk_waiters.valid()
        {
            return false;
        }
        let mut live = 0u128;
        let mut outstanding = u128::from(self.stateless_outstanding_bytes);
        for (key, connection) in &self.connections {
            live += u128::from(connection.live_reservation_bytes);
            outstanding += u128::from(connection.outstanding_bytes);
            let control = self
                .control_waiters
                .iter()
                .filter(|queued| *queued == key)
                .count();
            let bulk = self
                .bulk_waiters
                .iter()
                .filter(|queued| *queued == key)
                .count();
            let queues_valid = match &connection.waiter {
                Some(waiter) if connection.attached => match waiter.class {
                    PacingTrafficClass::Control => control == 1 && bulk == 0,
                    PacingTrafficClass::Bulk => control == 0 && bulk == 1,
                },
                Some(_) => false,
                None => control == 0 && bulk == 0,
            };
            if !queues_valid
                || (!connection.attached
                    && connection.live_reservation_bytes == 0
                    && connection.outstanding_bytes == 0)
            {
                return false;
            }
        }
        for key in self.control_waiters.iter().chain(self.bulk_waiters.iter()) {
            if !self.connections.contains_key(key) {
                return false;
            }
        }
        for key in [self.control_turn, self.bulk_turn].into_iter().flatten() {
            if !self
                .connections
                .get(&key)
                .is_some_and(|connection| connection.attached)
            {
                return false;
            }
        }
        live == u128::from(self.live_reservation_bytes)
            && outstanding == u128::from(self.outstanding_bytes)
            && live + outstanding + u128::from(self.available_tokens) <= u128::from(burst)
    }
}

impl EndpointPacingService {
    /// Physical adapter destruction retires only known custody, without minting
    /// elapsed refill time or a protocol/driver acknowledgement.
    pub(super) fn dispose_attachment(
        &self,
        key: EndpointPacingConnectionKey,
        has_socket_batch: bool,
    ) {
        if std::thread::panicking() || self.0.unavailable() {
            self.0.quarantine();
            return;
        }
        let mut state = self.lock_state();
        if !state.validate_control() {
            return;
        }
        let Some(record) = state.connections.get(&key) else {
            return;
        };
        if !record.attached {
            return;
        }
        if has_socket_batch || record.outstanding_bytes != 0 || record.outstanding_datagrams != 0 {
            self.0.quarantine();
            return;
        }
        state.remove_waiter(key);
        if let Some(record) = state.connections.get_mut(&key) {
            record.attached = false;
            if record.live_reservation_bytes == 0 {
                state.connections.remove(&key);
            }
        }
        state.detaches = state.detaches.saturating_add(1);
    }
}
impl Drop for ConnectionEndpointPacing {
    fn drop(&mut self) {
        let driver = self.service.0.hold_waker(self.driver_waker.take());
        self.service
            .dispose_attachment(self.key, self.pending_socket_batch.is_some());
        drop(driver);
    }
}
impl Drop for EndpointPacingServiceInner {
    fn drop(&mut self) {
        let dirty = {
            let (state, poisoned) = match self.state.get_mut() {
                Ok(state) => (state, false),
                Err(error) => (error.into_inner(), true),
            };
            poisoned
                || state.connections.len() != 0
                || state.live_reservation_bytes != 0
                || state.outstanding_bytes != 0
        };
        if dirty && self.meter.is_some() {
            self.quarantine();
        }
        let mut first = None;
        loop {
            let mut retired: [Option<Waker>; inventory::CAPACITY + 2] =
                std::array::from_fn(|_| None);
            {
                let state = self
                    .state
                    .get_mut()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let keys: [Option<EndpointPacingConnectionKey>; inventory::CAPACITY] = {
                    let mut keys = [None; inventory::CAPACITY];
                    for (slot, (key, _)) in keys.iter_mut().zip(
                        (&state.connections).into_iter().filter(|(_, record)| {
                            record
                                .waiter
                                .as_ref()
                                .is_some_and(|waiter| waiter.waker.is_some())
                        }),
                    ) {
                        *slot = Some(*key);
                    }
                    keys
                };
                for (slot, key) in retired.iter_mut().zip(keys.into_iter().flatten()) {
                    *slot = state
                        .connections
                        .get_mut(&key)
                        .and_then(|record| record.waiter.take())
                        .and_then(|waiter| waiter.waker);
                }
                retired[inventory::CAPACITY..].swap_with_slice(&mut state.retired_wakers);
            }
            if retired.iter().all(Option::is_none) {
                break;
            }
            if let Err(payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                self.dispose_wakers(retired)
            })) {
                if !std::thread::panicking() && first.is_none() {
                    first = Some(payload);
                } else {
                    std::mem::forget(payload);
                }
            }
        }
        if let Some(payload) = first {
            std::panic::resume_unwind(payload);
        }
    }
}
