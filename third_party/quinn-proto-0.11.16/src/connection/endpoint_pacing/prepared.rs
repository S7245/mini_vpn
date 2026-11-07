//! Known unpublished attachment rollback; no clock, refill or waiter wake is invented.
//! The same service proves no independently owned reservation, batch, waiter or turn
//! exists before deleting its record. Unknown inventory refuses the disposal witness.
//! Entry: EndpointPacingService::rollback_unpublished; tests: admission/registration_prepared.
use super::*;
impl EndpointPacingService {
    pub(crate) fn rollback_unpublished(&self, key: EndpointPacingConnectionKey) -> bool {
        let mut state = self.lock_state();
        if !state.validate_control() {
            return false;
        }
        let clean = state.connections.get(&key).is_none_or(|connection| {
            connection.live_reservation_bytes == 0
                && connection.outstanding_bytes == 0
                && connection.outstanding_datagrams == 0
                && connection.waiter.is_none()
        }) && !state.control_waiters.contains(&key)
            && !state.bulk_waiters.contains(&key)
            && state.control_turn != Some(key)
            && state.bulk_turn != Some(key);
        if clean {
            state.connections.remove(&key);
        }
        clean
    }
}

#[cfg(test)]
mod tests;
