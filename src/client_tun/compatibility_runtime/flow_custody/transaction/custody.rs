//! The original Session open-table mutex and its exclusive mutable access.
//! Every mutable borrow invalidates prior read evidence, including whole-table
//! replacement. No second ledger, allocation, admission or release authority.
//! Poison and revision exhaustion disable reuse, never wrap or manufacture Q.
//! Entry: Owner::lock/get_mut; release alone consumes revision evidence.
//! Tests: custody/tests/mod.rs and real Service provider reentry tests.
use super::OpenCustody;
use std::ops::{Deref, DerefMut};
use std::sync::{LockResult, Mutex, MutexGuard, PoisonError};

#[derive(Default)]
struct State {
    value: OpenCustody,
    revision: u64,
}
impl State {
    fn invalidate(&mut self) { self.revision = self.revision.saturating_add(1); }
}
#[derive(Default)]
pub(in crate::client_tun::compatibility_runtime) struct Owner(Mutex<State>);
pub(in crate::client_tun::compatibility_runtime) struct Guard<'a> {
    inner: MutexGuard<'a, State>,
    reusable: bool,
}
impl Owner {
    pub(in crate::client_tun::compatibility_runtime) fn new(value: OpenCustody) -> Self {
        Self(Mutex::new(State { value, revision: 0 }))
    }
    pub(in crate::client_tun::compatibility_runtime) fn lock(&self) -> LockResult<Guard<'_>> {
        match self.0.lock() {
            Ok(inner) => Ok(Guard { inner, reusable: true }),
            Err(error) => Err(PoisonError::new(Guard { inner: error.into_inner(), reusable: false })),
        }
    }
    pub(in crate::client_tun::compatibility_runtime) fn get_mut(&mut self) -> LockResult<&mut OpenCustody> {
        match self.0.get_mut() {
            Ok(state) => { state.invalidate(); Ok(&mut state.value) }
            Err(error) => {
                let state = error.into_inner();
                state.invalidate();
                Err(PoisonError::new(&mut state.value))
            }
        }
    }
    #[cfg(test)]
    pub(in crate::client_tun::compatibility_runtime) fn try_lock(&self) -> std::sync::TryLockResult<Guard<'_>> {
        match self.0.try_lock() {
            Ok(inner) => Ok(Guard { inner, reusable: true }),
            Err(std::sync::TryLockError::WouldBlock) => Err(std::sync::TryLockError::WouldBlock),
            Err(std::sync::TryLockError::Poisoned(error)) => Err(std::sync::TryLockError::Poisoned(
                PoisonError::new(Guard { inner: error.into_inner(), reusable: false }),
            )),
        }
    }
}
impl Guard<'_> {
    pub(super) fn revision(&self) -> Option<u64> {
        (self.reusable && self.inner.revision != u64::MAX).then_some(self.inner.revision)
    }
}
impl Deref for Guard<'_> {
    type Target = OpenCustody;
    fn deref(&self) -> &OpenCustody { &self.inner.value }
}
impl DerefMut for Guard<'_> {
    fn deref_mut(&mut self) -> &mut OpenCustody {
        self.inner.invalidate();
        &mut self.inner.value
    }
}
#[cfg(test)]
mod tests;
