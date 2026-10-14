//! Atomic owner session/replay capacity provenance.
//!
//! A replay plan and its owner-role reducer config are derived together here.
//! No API accepts an independently assembled pair, so equal scalar limits
//! cannot forge a different recovery horizon or storage geometry.

use crate::resumable::{ReplayStoragePlan, SessionConfig, SessionConfigError, SessionRole};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct OwnerSessionCapacityPlan {
    replay_storage: ReplayStoragePlan,
    session: SessionConfig,
}

impl OwnerSessionCapacityPlan {
    pub(crate) fn derive(
        replay_storage: ReplayStoragePlan,
        max_terminal_tombstones: usize,
        max_sink_offer_bytes: usize,
    ) -> Result<Self, SessionConfigError> {
        let session = SessionConfig::from_storage_plan(
            SessionRole::Owner,
            max_terminal_tombstones,
            replay_storage,
            max_sink_offer_bytes,
        )?;
        Ok(Self {
            replay_storage,
            session,
        })
    }

    pub(crate) const fn replay_storage(self) -> ReplayStoragePlan {
        self.replay_storage
    }

    pub(crate) const fn session_config(self) -> SessionConfig {
        self.session
    }
}
