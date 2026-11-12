//! The actual Connection State lock owns sticky interruption disposition.
//! Poison is recovered solely to quarantine the original inventory and expose
//! its retained first error; it never authorizes normal protocol work or ACK.
//! No Root/Pool ownership or DATA coordination is added. Interruption is per
//! section (std Mutex rule): only a panic that starts while a guard is held
//! poisons; a guard taken during a foreign unwind (stream Drop) completes.
//! Entry: StateLock::lock; tests: connection/tests/{reply_poison, unwind_drop}.
use super::*;
use crate::mutex::MutexGuard;
use std::ops::{Deref, DerefMut};

#[derive(Debug)]
pub(crate) struct StateLock(Mutex<State>);
impl StateLock {
    pub(super) fn new(state: State) -> Self { Self(Mutex::new(state)) }
    pub(crate) fn lock(&self, purpose: &'static str) -> StateGuard<'_> {
        let (mut state, poisoned) = self.0.lock_recover(purpose);
        if poisoned || state.poisoned {
            state.poisoned = true;
            state.inner.quarantine_event_inventory();
            if !std::thread::panicking() {
                if state.error.is_none() {
                    state.error = Some(state.inner.retained_error().unwrap_or_else(endpoint_settlement_lost));
                }
                state.wake();
            }
        }
        StateGuard(state, std::thread::panicking())
    }
}
/// The flag records whether this section began during an unwind.
pub(crate) struct StateGuard<'a>(MutexGuard<'a, State>, bool);
impl Deref for StateGuard<'_> {
    type Target = State;
    fn deref(&self) -> &State { &self.0 }
}
impl DerefMut for StateGuard<'_> {
    fn deref_mut(&mut self) -> &mut State { &mut self.0 }
}
impl Drop for StateGuard<'_> {
    fn drop(&mut self) {
        if !self.1 && std::thread::panicking() {
            self.0.poisoned = true;
            self.0.inner.quarantine_event_inventory();
        }
    }
}
