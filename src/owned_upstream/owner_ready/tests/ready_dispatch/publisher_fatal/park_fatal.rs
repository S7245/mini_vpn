//! Fixed before/after cached-registration epoch observations for outer tests.
//! Owns metadata only; the actual dequeue-on-Drop callback stays with its root.

use super::*;
use std::sync::atomic::Ordering;

pub(in crate::owned_upstream) struct BeforeCachedReplacementEpoch(u64);
pub(in crate::owned_upstream) struct DirtyCachedReplacementEpoch(u64);

impl ReadyIndex {
    pub(in crate::owned_upstream) fn capture_even_epoch_before_cached_replacement(
        &self,
    ) -> BeforeCachedReplacementEpoch {
        let even_epoch = self.shared.epoch.load(Ordering::Acquire);
        assert_eq!(even_epoch & 1, 0);
        assert_eq!(self.shared.trees[SESSION_CLASS].first_at(0, 0), Ok(None));
        BeforeCachedReplacementEpoch(even_epoch)
    }

    pub(in crate::owned_upstream) fn assert_fault_during_cached_replacement(
        &self,
        even_epoch: &BeforeCachedReplacementEpoch,
    ) -> DirtyCachedReplacementEpoch {
        let dirty_epoch = self.shared.epoch.load(Ordering::Acquire);
        assert_eq!(dirty_epoch, even_epoch.0 | 1);
        assert_eq!(dirty_epoch & 1, 1);
        assert_eq!(self.shared.trees[SESSION_CLASS].first_at(0, 0), Ok(Some(0)));
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
        DirtyCachedReplacementEpoch(dirty_epoch)
    }

    pub(in crate::owned_upstream) fn assert_replacement_epoch_stays_dirty(
        &self,
        dirty_epoch: &DirtyCachedReplacementEpoch,
    ) {
        assert_eq!(self.shared.epoch.load(Ordering::Acquire), dirty_epoch.0);
    }
}
