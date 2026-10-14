//! Fixed metadata arrangements and observations for outer-root dispatch tests.
//!
//! Each recipe short-borrows the actual index at its original observation point.
//! Real mailbox operations, catch scopes, command custody and cleanup stay in
//! the scenarios. Only expected-value observations and the original native Wake
//! Arc can leave a recipe; no guard, command or runtime authority escapes.

mod cold;
mod cold_storage;
mod flow;
mod ready_dispatch;

use super::{ReadyIndex, ReadyPhase, SESSION_CLASS};

impl ReadyIndex {
    pub(in crate::owned_upstream) fn arm_b_source_revision_exhaustion_after_waiting(&self) {
        let mut state = self.shared.slots[0].lock().unwrap();
        assert_eq!(state.capacity_mask, 1);
        assert!(matches!(state.sources[0].phase, ReadyPhase::Idle));
        assert!(matches!(
            state.processes[SESSION_CLASS].phase,
            ReadyPhase::Idle
        ));
        // Only an arithmetic boundary changes; actual notification owns the publish.
        state.sources[0].revision = u64::MAX;
    }

    pub(in crate::owned_upstream) fn assert_b_source_quarantined_after_dequeue(&self) {
        let state = self.shared.slots[0].lock().unwrap();
        assert_eq!(state.sources[0].revision, u64::MAX);
        assert!(matches!(state.sources[0].phase, ReadyPhase::Quarantined));
    }

    pub(in crate::owned_upstream) fn arm_b_process_revision_exhaustion_after_waiting(&self) {
        let mut state = self.shared.slots[0].lock().unwrap();
        assert_eq!(state.capacity_mask, 1);
        assert_eq!(state.local_word, 0);
        assert_eq!(state.sources[0].revision, 1);
        assert!(matches!(state.sources[0].phase, ReadyPhase::Idle));
        assert!(matches!(
            state.processes[SESSION_CLASS].phase,
            ReadyPhase::Idle
        ));
        // Leave the source's counter intact: only aggregate publication exhausts.
        state.processes[SESSION_CLASS].revision = u64::MAX;
    }

    pub(in crate::owned_upstream) fn assert_b_process_quarantined_after_dequeue(&self) {
        let state = self.shared.slots[0].lock().unwrap();
        assert_eq!(state.sources[0].revision, 2);
        assert!(matches!(state.sources[0].phase, ReadyPhase::Published));
        assert_eq!(state.processes[SESSION_CLASS].revision, u64::MAX);
        assert!(matches!(
            state.processes[SESSION_CLASS].phase,
            ReadyPhase::Quarantined
        ));
        assert_eq!(state.local_word, 1);
        assert_eq!(state.capacity_mask, 1);
    }

    pub(in crate::owned_upstream) fn assert_b_slot_unpoisoned_before_guarded_panic(&self) {
        assert!(!self.shared.slots[0].is_poisoned());
    }

    pub(in crate::owned_upstream) fn poison_b_slot_inside_publication_catch(&self) {
        // The guard must unwind inside catch_unwind to poison the real mutex.
        let _guard = self.shared.slots[0].lock().unwrap();
        panic!("publisher fixture slot poison");
    }

    pub(in crate::owned_upstream) fn assert_b_slot_poisoned_before_dequeue(&self) {
        assert!(self.shared.slots[0].is_poisoned());
    }

    pub(in crate::owned_upstream) fn assert_b_slot_still_poisoned_after_close(&self) {
        assert!(self.shared.slots[0].is_poisoned());
    }

    pub(in crate::owned_upstream) fn arm_a_process_exhaustion_after_both_waiting(&self) {
        for slot in &self.shared.slots {
            let state = slot.lock().unwrap();
            assert_eq!(state.capacity_mask, 1);
            assert_eq!(state.local_word, 0);
            assert_eq!(state.sources[0].revision, 1);
            assert!(matches!(state.sources[0].phase, ReadyPhase::Idle));
            assert!(matches!(
                state.processes[SESSION_CLASS].phase,
                ReadyPhase::Idle
            ));
        }
        {
            let mut state = self.shared.slots[1].lock().unwrap();
            state.processes[SESSION_CLASS].revision = u64::MAX;
        }
    }

    pub(in crate::owned_upstream) fn poison_b_before_either_publisher(&self) {
        let _guard = self.shared.slots[0].lock().unwrap();
        panic!("first-cause fixture poisons B before either publisher");
    }

    pub(in crate::owned_upstream) fn assert_a_still_unpublished_after_b_dequeue(&self) {
        let state = self.shared.slots[1].lock().unwrap();
        assert_eq!(state.sources[0].revision, 1);
        assert!(matches!(state.sources[0].phase, ReadyPhase::Idle));
        assert_eq!(state.processes[SESSION_CLASS].revision, u64::MAX);
        assert!(matches!(
            state.processes[SESSION_CLASS].phase,
            ReadyPhase::Idle
        ));
        assert_eq!(state.local_word, 0);
        assert_eq!(state.capacity_mask, 1);
    }

    pub(in crate::owned_upstream) fn assert_a_process_quarantined_after_a_dequeue(&self) {
        let state = self.shared.slots[1].lock().unwrap();
        assert_eq!(state.sources[0].revision, 2);
        assert!(matches!(state.sources[0].phase, ReadyPhase::Published));
        assert_eq!(state.processes[SESSION_CLASS].revision, u64::MAX);
        assert!(matches!(
            state.processes[SESSION_CLASS].phase,
            ReadyPhase::Quarantined
        ));
        assert_eq!(state.local_word, 1);
        assert_eq!(state.capacity_mask, 1);
    }
}
