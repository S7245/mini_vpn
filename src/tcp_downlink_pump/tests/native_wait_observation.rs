//! Read-only original Producer registration, used by the actual Service tracer.
use super::*;
impl Waiters {
    pub(in crate::tcp_downlink_pump) fn producer_waiter_matches_for_test(&self, waker: &Waker) -> bool {
        let inventory = self.lock();
        let Some(signal) = &inventory.roles[0] else { return false; };
        let state = signal.lock();
        state.phase == Phase::Leased && state.waiter.as_ref().is_some_and(|original| original.will_wake(waker))
    }
}
