//! Fixed publisher-fatal metadata recipes, native wakes and value observations.
//! Complete root polling, command custody and catch scopes live in outer tests.

mod notify_unwind;
mod park_fatal;
mod source_unknown;

use super::super::super::{
    ReadyClaim, ReadyFault, ReadyIndex, ReadyKey, ReadyPhase, SESSION_CLASS,
};
use super::*;

pub(in crate::owned_upstream) struct PoisonOnFirstReceiveWake {
    shared: std::sync::Weak<super::super::super::ReadyShared>,
    calls: std::sync::atomic::AtomicUsize,
}

impl Wake for PoisonOnFirstReceiveWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
            let shared = self
                .shared
                .upgrade()
                .expect("the actual root remains alive");
            let _guard = shared.slots[0]
                .try_lock()
                .unwrap_or_else(|_| panic!("send must not hold the scheduler guard"));
            panic!("actual enqueue wake poisons source recovery metadata");
        }
    }
}

pub(in crate::owned_upstream) struct PoisonedSourceObservation((ReadyKey, u64, u64));

impl PoisonOnFirstReceiveWake {
    pub(in crate::owned_upstream) fn assert_called_once(&self) {
        assert_eq!(self.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    }
}

impl ReadyClaim {
    pub(in crate::owned_upstream) fn assert_poisoned_source_metadata(
        &self,
        expected_metadata: &PoisonedSourceObservation,
    ) {
        assert!((self.key, self.source_revision, self.process_revision) == expected_metadata.0);
    }
}

impl ReadyIndex {
    pub(in crate::owned_upstream) fn capture_poisoned_source_metadata(
        &self,
    ) -> PoisonedSourceObservation {
        let state = self.shared.slots[0].lock().unwrap();
        PoisonedSourceObservation((
            state.key,
            state.sources[0].revision,
            state.processes[SESSION_CLASS].revision,
        ))
    }

    pub(in crate::owned_upstream) fn poison_on_first_receive_wake(
        &self,
    ) -> Arc<PoisonOnFirstReceiveWake> {
        Arc::new(PoisonOnFirstReceiveWake {
            shared: Arc::downgrade(&self.shared),
            calls: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    pub(in crate::owned_upstream) fn assert_poisoned_source_readback(
        &self,
        expected_metadata: &PoisonedSourceObservation,
    ) {
        assert!(self.shared.slots[0].is_poisoned());
        // Test-only readback; never clear poison or mutate recovered metadata.
        let state = match self.shared.slots[0].lock() {
            Err(poisoned) => poisoned.into_inner(),
            Ok(_) => panic!("the metadata mutex must remain poisoned"),
        };
        assert!(matches!(state.sources[0].phase, ReadyPhase::RunningClean));
        assert!(matches!(
            state.processes[SESSION_CLASS].phase,
            ReadyPhase::RunningClean
        ));
        assert!(
            (
                state.key,
                state.sources[0].revision,
                state.processes[SESSION_CLASS].revision
            ) == expected_metadata.0
        );
        assert_eq!(state.local_word, 1);
        assert_eq!(state.capacity_mask, 0);
        assert_eq!(state.source_cursor, 1);
    }
}

pub(in crate::owned_upstream) struct AdmittedCompletionObservation((ReadyKey, u64, u64));

impl ReadyIndex {
    pub(in crate::owned_upstream) fn capture_a_before_admitted_send(
        &self,
    ) -> AdmittedCompletionObservation {
        AdmittedCompletionObservation({
            let state = self.shared.slots[1].lock().unwrap();
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
        })
    }

    pub(in crate::owned_upstream) fn assert_a_idle_after_admitted_send(
        &self,
        initial_a: &AdmittedCompletionObservation,
    ) {
        let state = self.shared.slots[1].lock().unwrap();
        assert!(matches!(state.sources[0].phase, ReadyPhase::Idle));
        assert!(matches!(
            state.processes[SESSION_CLASS].phase,
            ReadyPhase::Idle
        ));
        assert!(
            (
                state.key,
                state.sources[0].revision,
                state.processes[SESSION_CLASS].revision
            ) == initial_a.0
        );
        assert_eq!(state.local_word, 0);
        assert_eq!(state.capacity_mask, 0);
    }

    pub(in crate::owned_upstream) fn assert_only_b_indexed_after_admitted_send(&self) {
        let tree = &self.shared.trees[SESSION_CLASS];
        assert_eq!(tree.first_at(0, 0), Ok(Some(0)));
        assert_eq!(tree.first_at(0, 1), Ok(None));
    }
}
