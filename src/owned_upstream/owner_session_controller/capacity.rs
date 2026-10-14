//! Transport-independent capacity authority for one owner session controller.
//!
//! This module deliberately stops below physical Quinn actors.  The future
//! owner physical root combines this witness with the independently checked A
//! and B actor witnesses; the controller never guesses transport storage.

use thiserror::Error;

use super::{OwnerOutputPageCapacity, OwnerOutputPageCapacityError};
pub(crate) use crate::owned_upstream::owner_session_capacity::OwnerSessionCapacityPlan;
use crate::owned_upstream::owner_target::{OwnerTargetConfig, OwnerTargetExecutor};
use crate::owned_upstream::reverse_switch::{OwnerProgressCapacity, OwnerProgressCapacityError};
use crate::owned_upstream::target::{TargetIo, TargetIoCapacityWitness};
use crate::owned_upstream::tcp::TcpPortFactoryCapacityWitness;
use crate::resumable::{Direction, ReplayStoragePlan, SessionConfig};

/// Exact checked capacity installed beside the sole owner executor.
///
/// Byte domains are intentionally independent.  Replay backing is the owner
/// Target-to-client direction, receive backing is client-to-Target, and the
/// quarantine staging slot is the controller's one bounded Target read.  A
/// future root must sum these values; none is a mirror of another.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct OwnerSessionCapacityWitness {
    replay_storage: ReplayStoragePlan,
    session: SessionConfig,
    target_config: OwnerTargetConfig,
    target: TargetIoCapacityWitness,
    source_factory: TcpPortFactoryCapacityWitness,
    source_session_channel_slots: usize,
    progress: OwnerProgressCapacity,
    output_page: OwnerOutputPageCapacity,
    recovery_effect_bound: usize,
    required_pending_effects: usize,
    replay_persistent_backing_bytes: usize,
    receive_persistent_backing_bytes: usize,
    quarantine_staging_bytes: usize,
}

impl OwnerSessionCapacityWitness {
    pub(super) fn derive<T: TargetIo>(
        executor: &OwnerTargetExecutor<T>,
        plan: OwnerSessionCapacityPlan,
    ) -> Result<Self, OwnerSessionCapacityError> {
        let installed_storage = executor
            .installed_replay_storage_plan()
            .ok_or(OwnerSessionCapacityError::MissingInstalledReplayStorageProvenance)?;
        if installed_storage != plan.replay_storage() {
            return Err(OwnerSessionCapacityError::ReplayStorageExpectationMismatch);
        }
        let installed = executor.installed_session_config();
        if installed != plan.session_config() {
            return Err(OwnerSessionCapacityError::InstalledSessionConfigProvenanceInvariant);
        }

        let source_factory = executor.source_factory_capacity_witness();
        if source_factory.bound_direction() != Some(Direction::TargetToClient) {
            return Err(OwnerSessionCapacityError::SourceFactoryDirectionMismatch {
                actual: source_factory.bound_direction(),
            });
        }
        let per_flow_storage = installed_storage.per_flow().target_to_client();
        let global_storage = installed_storage.global().target_to_client();
        let source_flow_bytes = source_factory.flow_port().uplink_byte_capacity();
        if source_flow_bytes > per_flow_storage.max_bytes() {
            return Err(
                OwnerSessionCapacityError::SourcePerFlowByteCapacityExceedsStorage {
                    requested: source_flow_bytes,
                    available: per_flow_storage.max_bytes(),
                },
            );
        }
        if source_factory.session_uplink_byte_capacity() != global_storage.max_bytes() {
            return Err(
                OwnerSessionCapacityError::SourceSessionByteCapacityMismatch {
                    storage: global_storage.max_bytes(),
                    factory: source_factory.session_uplink_byte_capacity(),
                },
            );
        }
        if source_factory.per_flow_uplink_segment_capacity() != per_flow_storage.max_segments() {
            return Err(
                OwnerSessionCapacityError::SourcePerFlowSegmentCapacityMismatch {
                    storage: per_flow_storage.max_segments(),
                    factory: source_factory.per_flow_uplink_segment_capacity(),
                },
            );
        }
        if source_factory.session_uplink_segment_capacity() != global_storage.max_segments() {
            return Err(
                OwnerSessionCapacityError::SourceSessionSegmentCapacityMismatch {
                    storage: global_storage.max_segments(),
                    factory: source_factory.session_uplink_segment_capacity(),
                },
            );
        }

        let target = executor.target().capacity_witness();
        if target.max_flows() != installed.max_flows() {
            return Err(OwnerSessionCapacityError::TargetFlowCapacityMismatch {
                session: installed.max_flows(),
                target: target.max_flows(),
            });
        }
        if target.max_terminal_tombstones() != installed.max_terminal_tombstones() {
            return Err(
                OwnerSessionCapacityError::TargetTerminalTombstoneCapacityMismatch {
                    session: installed.max_terminal_tombstones(),
                    target: target.max_terminal_tombstones(),
                },
            );
        }

        let progress = OwnerProgressCapacity::checked(
            installed.max_flows(),
            installed.max_terminal_tombstones(),
        )
        .map_err(OwnerSessionCapacityError::ProgressCapacity)?;
        let target_config = executor.config();
        if target_config.max_target_read_bytes() > source_factory.max_single_data_payload_bytes() {
            return Err(
                OwnerSessionCapacityError::TargetReadExceedsSourceSingleDataPayload {
                    requested: target_config.max_target_read_bytes(),
                    available: source_factory.max_single_data_payload_bytes(),
                },
            );
        }
        let source_session_channel_slots = installed
            .max_flows()
            .checked_mul(source_factory.per_flow_channel_slots())
            .ok_or(
                OwnerSessionCapacityError::SourceSessionChannelCapacityOverflow {
                    max_flows: installed.max_flows(),
                    per_flow_slots: source_factory.per_flow_channel_slots(),
                },
            )?;
        let output_page =
            OwnerOutputPageCapacity::checked_from_max_outputs(target_config.max_effect_steps())
                .map_err(OwnerSessionCapacityError::OutputPage)?;
        let recovery_effect_bound = installed
            .owner_recovery_effect_bound()
            .ok_or(OwnerSessionCapacityError::RecoveryEffectBoundOverflow)?;
        if executor.installed_recovery_effect_bound() != recovery_effect_bound {
            return Err(OwnerSessionCapacityError::RecoveryEffectCapacityMismatch {
                derived: recovery_effect_bound,
                installed: executor.installed_recovery_effect_bound(),
            });
        }
        let required_pending_effects = installed
            .owner_pending_work_bound()
            .ok_or(OwnerSessionCapacityError::PendingEffectBoundOverflow)?;
        if target_config.max_pending_effects() < required_pending_effects {
            return Err(OwnerSessionCapacityError::PendingEffectCapacityTooSmall {
                required: required_pending_effects,
                available: target_config.max_pending_effects(),
            });
        }

        let global = installed_storage.global();
        Ok(Self {
            replay_storage: installed_storage,
            session: installed,
            target_config,
            target,
            source_factory,
            source_session_channel_slots,
            progress,
            output_page,
            recovery_effect_bound,
            required_pending_effects,
            replay_persistent_backing_bytes: global
                .target_to_client()
                .max_persistent_backing_bytes(),
            receive_persistent_backing_bytes: global
                .client_to_target()
                .max_persistent_backing_bytes(),
            quarantine_staging_bytes: target_config.max_target_read_bytes(),
        })
    }

    pub(crate) const fn replay_storage(self) -> ReplayStoragePlan {
        self.replay_storage
    }

    pub(crate) const fn session_config(self) -> SessionConfig {
        self.session
    }

    pub(crate) const fn target_config(self) -> OwnerTargetConfig {
        self.target_config
    }

    pub(crate) const fn target(self) -> TargetIoCapacityWitness {
        self.target
    }

    pub(crate) const fn source_factory(self) -> TcpPortFactoryCapacityWitness {
        self.source_factory
    }

    pub(crate) const fn source_session_channel_slots(self) -> usize {
        self.source_session_channel_slots
    }

    pub(crate) const fn max_progress_entries(self) -> usize {
        self.progress.max_entries()
    }

    pub(super) const fn progress_capacity(self) -> OwnerProgressCapacity {
        self.progress
    }

    pub(crate) const fn output_page(self) -> OwnerOutputPageCapacity {
        self.output_page
    }

    pub(crate) const fn recovery_effect_bound(self) -> usize {
        self.recovery_effect_bound
    }

    pub(crate) const fn required_pending_effects(self) -> usize {
        self.required_pending_effects
    }

    pub(crate) const fn replay_persistent_backing_bytes(self) -> usize {
        self.replay_persistent_backing_bytes
    }

    pub(crate) const fn receive_persistent_backing_bytes(self) -> usize {
        self.receive_persistent_backing_bytes
    }

    pub(crate) const fn quarantine_staging_bytes(self) -> usize {
        self.quarantine_staging_bytes
    }
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub(crate) enum OwnerSessionCapacityError {
    #[error("owner executor is missing exact installed replay-storage provenance")]
    MissingInstalledReplayStorageProvenance,
    #[error("owner root replay-storage expectation differs from exact installed provenance")]
    ReplayStorageExpectationMismatch,
    #[error("installed owner SessionConfig differs from its atomic replay-storage plan")]
    InstalledSessionConfigProvenanceInvariant,
    #[error("owner Target flow capacity {target} does not equal session capacity {session}")]
    TargetFlowCapacityMismatch { session: usize, target: usize },
    #[error(
        "owner Target terminal-tombstone capacity {target} does not equal session capacity {session}"
    )]
    TargetTerminalTombstoneCapacityMismatch { session: usize, target: usize },
    #[error(
        "owner source factory direction {actual:?} is not the required TargetToClient direction"
    )]
    SourceFactoryDirectionMismatch { actual: Option<Direction> },
    #[error("owner source per-flow byte capacity {requested} exceeds replay storage {available}")]
    SourcePerFlowByteCapacityExceedsStorage { requested: usize, available: usize },
    #[error("owner source session byte capacity {factory} differs from replay storage {storage}")]
    SourceSessionByteCapacityMismatch { storage: usize, factory: usize },
    #[error(
        "owner source per-flow segment capacity {factory} differs from replay storage {storage}"
    )]
    SourcePerFlowSegmentCapacityMismatch { storage: usize, factory: usize },
    #[error(
        "owner source session segment capacity {factory} differs from replay storage {storage}"
    )]
    SourceSessionSegmentCapacityMismatch { storage: usize, factory: usize },
    #[error("owner Target read bound {requested} exceeds source single-DATA capacity {available}")]
    TargetReadExceedsSourceSingleDataPayload { requested: usize, available: usize },
    #[error(
        "owner source session channel envelope overflowed for {max_flows} flows and {per_flow_slots} slots per flow"
    )]
    SourceSessionChannelCapacityOverflow {
        max_flows: usize,
        per_flow_slots: usize,
    },
    #[error("owner progress capacity is invalid: {0}")]
    ProgressCapacity(OwnerProgressCapacityError),
    #[error("owner progress storage is invalid: {0}")]
    ProgressStorage(crate::owned_upstream::reverse_switch::OwnerProgressError),
    #[error("owner output page capacity is invalid: {0}")]
    OutputPage(OwnerOutputPageCapacityError),
    #[error("owner recovery-effect capacity derivation overflowed")]
    RecoveryEffectBoundOverflow,
    #[error(
        "installed owner recovery-effect capacity {installed} differs from derived capacity {derived}"
    )]
    RecoveryEffectCapacityMismatch { derived: usize, installed: usize },
    #[error("owner pending-effect capacity derivation overflowed")]
    PendingEffectBoundOverflow,
    #[error(
        "owner pending-effect capacity {available} is below the derived requirement {required}"
    )]
    PendingEffectCapacityTooSmall { required: usize, available: usize },
}
