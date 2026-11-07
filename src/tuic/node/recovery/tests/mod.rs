//! Test-only observation of the original clear transaction, outside its mutex.
//! No production state, fake resource or replacement settlement receipt.
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::Duration;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::tuic::node) struct HealthySnapshot {
    pub(in crate::tuic::node) ticket: super::Ticket,
    pub(in crate::tuic::node) deadline: tokio::time::Instant,
    pub(in crate::tuic::node) attempts: u8,
    pub(in crate::tuic::node) failed: bool,
    pub(in crate::tuic::node) settled: bool,
}
impl super::State {
    // Borrow the original demand; never supply a ticket or settlement receipt.
    pub(in crate::tuic::node) fn healthy_snapshot_for_test(
        &self,
        index: usize,
    ) -> Option<HealthySnapshot> {
        let demand = self.demands.get(index)?.as_ref()?;
        (demand.ticket.kind == super::Kind::Healthy).then_some(HealthySnapshot {
            ticket: demand.ticket,
            deadline: demand.ticket.deadline,
            attempts: demand.attempts,
            failed: demand.outcome == super::Outcome::Failed,
            settled: demand.settled,
        })
    }
}

#[derive(Default)]
pub(in crate::tuic::node) struct ClearGate {
    phase: AtomicU8,
    released: Mutex<u8>,
    changed: Condvar,
}
impl ClearGate {
    pub(super) fn observe(&self, phase: u8) {
        self.phase.store(phase, Ordering::Release);
        let released = self
            .released
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (released, timeout) = self
            .changed
            .wait_timeout_while(released, Duration::from_secs(3), |released| {
                *released < phase
            })
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(
            *released >= phase && !timeout.timed_out(),
            "original clear observation timed out"
        );
    }
    pub(in crate::tuic::node) fn arrived(&self, phase: u8) -> bool {
        self.phase.load(Ordering::Acquire) >= phase
    }
    pub(in crate::tuic::node) fn release(&self, phase: u8) {
        let mut released = self
            .released
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *released = (*released).max(phase);
        self.changed.notify_all();
    }
}
