//! Native committed-fault Wake and fixed metadata arrangement only.
//! The outer root scenario owns mailbox operations, catch scope and re-entry.

use super::super::super::super::ReadyShared;
use super::*;
use std::sync::Weak;
use std::sync::atomic::{AtomicUsize, Ordering};

pub(in crate::owned_upstream) struct PanicAfterCommittedFault {
    shared: Weak<ReadyShared>,
    calls: AtomicUsize,
}

impl Wake for PanicAfterCommittedFault {
    fn wake(self: Arc<Self>) {
        assert_eq!(self.calls.fetch_add(1, Ordering::SeqCst), 0);
        let shared = self
            .shared
            .upgrade()
            .expect("the original root remains alive");
        assert_eq!(shared.failure.check(), Err(ReadyFault::RevisionExhausted));
        let guard = shared.slots[0]
            .try_lock()
            .unwrap_or_else(|_| panic!("publisher must release its slot guard before Notify"));
        assert_eq!(guard.sources[0].revision, u64::MAX);
        assert!(matches!(guard.sources[0].phase, ReadyPhase::Quarantined));
        drop(guard);
        panic!("root Notify wake after committed publisher fault");
    }
}

impl PanicAfterCommittedFault {
    pub(in crate::owned_upstream) fn assert_not_called(&self) {
        assert_eq!(self.calls.load(Ordering::SeqCst), 0);
    }

    pub(in crate::owned_upstream) fn assert_called_once(&self) {
        assert_eq!(self.calls.load(Ordering::SeqCst), 1);
    }
}

impl ReadyIndex {
    pub(in crate::owned_upstream) fn notify_unwind_wake(&self) -> Arc<PanicAfterCommittedFault> {
        Arc::new(PanicAfterCommittedFault {
            shared: Arc::downgrade(&self.shared),
            calls: AtomicUsize::new(0),
        })
    }

    pub(in crate::owned_upstream) fn assert_committed_notify_fault_metadata(&self) {
        assert!(!self.shared.slots[0].is_poisoned());
        {
            let state = self.shared.slots[0].lock().unwrap();
            assert_eq!(state.sources[0].revision, u64::MAX);
            assert!(matches!(state.sources[0].phase, ReadyPhase::Quarantined));
            assert!(matches!(
                state.processes[SESSION_CLASS].phase,
                ReadyPhase::Published
            ));
            assert_eq!(state.local_word, 1);
            assert_eq!(state.capacity_mask, 1);
        }
        assert_eq!(self.shared.trees[SESSION_CLASS].first_at(0, 0), Ok(Some(0)));
    }

    pub(in crate::owned_upstream) fn assert_notify_slot_unpoisoned_after_close(&self) {
        assert!(!self.shared.slots[0].is_poisoned());
    }
}
