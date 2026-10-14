//! Native healthy-source Wake and fixed metadata observations/claim assertions.
//! No command payload or root polling escapes into this metadata owner.

use super::super::super::super::ReadyShared;
use super::*;
use std::sync::Weak;
use std::sync::atomic::{AtomicUsize, Ordering};

pub(in crate::owned_upstream) struct PanicWithHealthyMetadata {
    shared: Weak<ReadyShared>,
    calls: AtomicUsize,
}

impl Wake for PanicWithHealthyMetadata {
    fn wake(self: Arc<Self>) {
        assert_eq!(self.calls.fetch_add(1, Ordering::SeqCst), 0);
        let shared = self
            .shared
            .upgrade()
            .expect("the original root remains alive");
        assert_eq!(shared.failure.check(), Ok(()));
        let guard = shared.slots[0]
            .try_lock()
            .unwrap_or_else(|_| panic!("source send must release the scheduler guard"));
        assert!(matches!(guard.sources[0].phase, ReadyPhase::RunningClean));
        assert!(matches!(
            guard.processes[SESSION_CLASS].phase,
            ReadyPhase::RunningClean
        ));
        drop(guard);
        panic!("actual enqueue wake leaves healthy source metadata");
    }
}

pub(in crate::owned_upstream) struct HealthySourceObservation([(ReadyKey, u64, u64); 2]);

impl PanicWithHealthyMetadata {
    pub(in crate::owned_upstream) fn assert_called_once(&self) {
        assert_eq!(self.calls.load(Ordering::SeqCst), 1);
    }
}

impl ReadyClaim {
    pub(in crate::owned_upstream) fn assert_healthy_source_metadata(
        &self,
        expected_metadata: &HealthySourceObservation,
    ) {
        assert!((self.key, self.source_revision, self.process_revision) == expected_metadata.0[0]);
    }
}

impl ReadyIndex {
    pub(in crate::owned_upstream) fn capture_healthy_source_metadata(
        &self,
    ) -> HealthySourceObservation {
        HealthySourceObservation([0, 1].map(|slot| {
            let state = self.shared.slots[slot].lock().unwrap();
            assert!(matches!(state.sources[0].phase, ReadyPhase::Published));
            assert!(matches!(
                state.processes[SESSION_CLASS].phase,
                ReadyPhase::Published
            ));
            (
                state.key,
                state.sources[0].revision,
                state.processes[SESSION_CLASS].revision,
            )
        }))
    }

    pub(in crate::owned_upstream) fn healthy_source_unwind_wake(
        &self,
    ) -> Arc<PanicWithHealthyMetadata> {
        Arc::new(PanicWithHealthyMetadata {
            shared: Arc::downgrade(&self.shared),
            calls: AtomicUsize::new(0),
        })
    }

    fn assert_healthy_source_slot(
        &self,
        expected_metadata: &HealthySourceObservation,
        slot: usize,
        recovered: bool,
    ) {
        let expected = &expected_metadata.0[slot];
        let local_quarantine = slot == 0 && recovered;
        assert!(!self.shared.slots[slot].is_poisoned());
        let state = self.shared.slots[slot].lock().unwrap();
        assert!(
            (
                state.key,
                state.sources[0].revision,
                state.processes[SESSION_CLASS].revision
            ) == *expected
        );
        let phase = if local_quarantine {
            ReadyPhase::Quarantined
        } else if slot == 0 {
            ReadyPhase::RunningClean
        } else {
            ReadyPhase::Published
        };
        assert!(state.sources[0].phase == phase);
        assert!(state.processes[SESSION_CLASS].phase == phase);
        assert!(
            state.sources[1..]
                .iter()
                .all(|cell| cell.phase == ReadyPhase::Dormant && cell.revision == 0)
        );
        assert!(
            state.processes[..SESSION_CLASS]
                .iter()
                .all(|cell| cell.phase == ReadyPhase::Dormant && cell.revision == 0)
        );
        assert_eq!(state.local_word, usize::from(!local_quarantine));
        assert_eq!(state.capacity_mask, 0);
        assert_eq!(state.source_cursor, usize::from(slot == 0));
    }

    pub(in crate::owned_upstream) fn assert_a_metadata_before_local_recovery(
        &self,
        expected: &HealthySourceObservation,
    ) {
        self.assert_healthy_source_slot(expected, 0, false);
    }

    pub(in crate::owned_upstream) fn assert_a_metadata_after_local_recovery(
        &self,
        expected: &HealthySourceObservation,
    ) {
        self.assert_healthy_source_slot(expected, 0, true);
    }

    pub(in crate::owned_upstream) fn assert_b_metadata_before_a_recovery(
        &self,
        expected: &HealthySourceObservation,
    ) {
        self.assert_healthy_source_slot(expected, 1, false);
    }

    pub(in crate::owned_upstream) fn assert_b_metadata_after_a_recovery(
        &self,
        expected: &HealthySourceObservation,
    ) {
        self.assert_healthy_source_slot(expected, 1, true);
    }

    pub(in crate::owned_upstream) fn assert_healthy_tree_before_local_recovery(&self) {
        assert_eq!(
            self.shared.trees[SESSION_CLASS].first_at(0, 0),
            Ok(Some(usize::from(false)))
        );
    }

    pub(in crate::owned_upstream) fn assert_healthy_tree_after_local_recovery(&self) {
        assert_eq!(
            self.shared.trees[SESSION_CLASS].first_at(0, 0),
            Ok(Some(usize::from(true)))
        );
    }

    pub(in crate::owned_upstream) fn assert_b_metadata_after_healthy_send(
        &self,
        expected_metadata: &HealthySourceObservation,
    ) {
        {
            let state = self.shared.slots[1].lock().unwrap();
            assert!(
                (
                    state.key,
                    state.sources[0].revision,
                    state.processes[SESSION_CLASS].revision
                ) == expected_metadata.0[1]
            );
            assert!(matches!(state.sources[0].phase, ReadyPhase::Idle));
            assert!(matches!(
                state.processes[SESSION_CLASS].phase,
                ReadyPhase::Idle
            ));
            assert_eq!(state.local_word, 0);
            assert_eq!(state.capacity_mask, 0);
        }
        assert_eq!(self.shared.trees[SESSION_CLASS].first_at(0, 0), Ok(None));
    }

    pub(in crate::owned_upstream) fn assert_healthy_slots_unpoisoned_after_close(&self) {
        assert!(self.shared.slots.iter().all(|slot| !slot.is_poisoned()));
    }
}
