//! Initial and legless construction within the sole Target executor authority.
//!
//! Owns validation, factory mint and exact bootstrap/Target returns on rejection.
//! Fields, effect execution, readiness and lifecycle remain in the parent.
//! Original input checks precede publication; no additional source is minted.
//! Entries: from_initial_bootstrap, new, new_legless_bootstrap.
//! Tests: parent tests, especially initial bootstrap and constructor rejection.

use super::{
    Arc, BTreeMap, FlowPortConfig, LeglessOwnerSessionBootstrap, MAX_DATA_PAYLOAD_BYTES,
    OwnerLeglessHighWater, OwnerSessionBootstrap, OwnerTargetConfig, OwnerTargetConfigError,
    OwnerTargetConstructionFailure, OwnerTargetExecutor, OwnerTargetExecutorIdentity,
    OwnerTargetInitialBootstrapFailure, OwnerTargetInitialBootstrapInstallGate,
    OwnerTargetInitialHandoff, OwnerTargetLeglessBootstrapFailure,
    OwnerTargetLeglessBootstrapInstallGate, SessionSupervisor, TargetIo, VecDeque,
};

impl<T: TargetIo> OwnerTargetExecutor<T> {
    /// Atomically turns the complete initial owner bootstrap into the sole
    /// Target executor and an executor-bound active handoff. Every typed
    /// construction failure reassembles and returns the exact bootstrap,
    /// including its still-live acceptance receipt, together with the Target.
    #[allow(
        clippy::result_large_err,
        reason = "construction failure must return the exact non-cloneable bootstrap and Target"
    )]
    pub(crate) fn from_initial_bootstrap(
        config: OwnerTargetConfig,
        bootstrap: OwnerSessionBootstrap,
        target: T,
        source_config: FlowPortConfig,
    ) -> Result<(Self, OwnerTargetInitialHandoff), OwnerTargetInitialBootstrapFailure<T>> {
        let gate = OwnerTargetInitialBootstrapInstallGate::mint();
        let (supervisor, attached, acceptance) = bootstrap.into_target_install_parts(&gate);
        match Self::new_in_phase_preserving(config, supervisor, target, source_config, false) {
            Ok(executor) => {
                let handoff = OwnerTargetInitialHandoff {
                    owner: Arc::downgrade(&executor.identity),
                    attached,
                    acceptance: Some(acceptance),
                    physical_actor: None,
                };
                Ok((executor, handoff))
            }
            Err(failure) => {
                let (source, supervisor, target) = failure.into_parts();
                Err(OwnerTargetInitialBootstrapFailure {
                    source,
                    config,
                    bootstrap: OwnerSessionBootstrap::from_target_install_parts(
                        supervisor, attached, acceptance, &gate,
                    ),
                    target,
                    source_config,
                })
            }
        }
    }

    pub(crate) fn new(
        config: OwnerTargetConfig,
        supervisor: SessionSupervisor,
        target: T,
        source_config: FlowPortConfig,
    ) -> Result<Self, OwnerTargetConfigError> {
        Self::new_in_phase(config, supervisor, target, source_config, false)
    }

    /// Constructs the sole Target executor from a committed initial attach
    /// whose exact transport died before acceptance publication. The returned
    /// high-water token is bound to this executor and owns the one resume
    /// grace expiry event; ordinary construction remains Active-only.
    pub(crate) fn new_legless_bootstrap(
        config: OwnerTargetConfig,
        bootstrap: LeglessOwnerSessionBootstrap,
        target: T,
        source_config: FlowPortConfig,
    ) -> Result<(Self, OwnerLeglessHighWater), OwnerTargetLeglessBootstrapFailure<T>> {
        let gate = OwnerTargetLeglessBootstrapInstallGate::mint();
        let (supervisor, pending_high_water) = bootstrap.into_legless_target_install_parts(&gate);
        let executor =
            match Self::new_in_phase_preserving(config, supervisor, target, source_config, true) {
                Ok(executor) => executor,
                Err(failure) => {
                    let (source, supervisor, target) = failure.into_parts();
                    return Err(OwnerTargetLeglessBootstrapFailure {
                        source,
                        config,
                        bootstrap: LeglessOwnerSessionBootstrap::from_legless_target_install_parts(
                            supervisor,
                            pending_high_water,
                            &gate,
                        ),
                        target,
                        source_config,
                    });
                }
            };
        let (generation, reason, resume_grace_expired) = pending_high_water.into_parts();
        let high_water = OwnerLeglessHighWater {
            owner: Arc::downgrade(&executor.identity),
            generation,
            reason,
            resume_grace_expired,
        };
        Ok((executor, high_water))
    }

    pub(super) fn new_in_phase(
        config: OwnerTargetConfig,
        supervisor: SessionSupervisor,
        target: T,
        source_config: FlowPortConfig,
        legless_bootstrap: bool,
    ) -> Result<Self, OwnerTargetConfigError> {
        Self::new_in_phase_preserving(config, supervisor, target, source_config, legless_bootstrap)
            .map_err(|failure| failure.source)
    }

    pub(super) fn new_in_phase_preserving(
        config: OwnerTargetConfig,
        mut supervisor: SessionSupervisor,
        target: T,
        source_config: FlowPortConfig,
        legless_bootstrap: bool,
    ) -> Result<Self, OwnerTargetConstructionFailure<T>> {
        macro_rules! reject {
            ($source:expr) => {
                return Err(OwnerTargetConstructionFailure {
                    source: $source,
                    supervisor,
                    target,
                })
            };
        }

        let Some(reducer_capacity) = supervisor.owner_target_replay_capacity() else {
            reject!(OwnerTargetConfigError::OwnerRoleRequired);
        };
        let Some(recovery_bound) = supervisor.owner_recovery_effect_bound() else {
            reject!(OwnerTargetConfigError::RecoveryEffectCapacityOverflow);
        };
        let Some(required_pending_effects) = supervisor.owner_pending_work_bound() else {
            reject!(OwnerTargetConfigError::RecoveryEffectCapacityOverflow);
        };
        if config.max_pending_effects < required_pending_effects {
            reject!(OwnerTargetConfigError::PendingEffectCapacityTooSmall {
                required: required_pending_effects,
                available: config.max_pending_effects,
            });
        }
        let flow_capacity = source_config.uplink_byte_capacity();
        if flow_capacity > reducer_capacity.per_flow_bytes {
            reject!(OwnerTargetConfigError::PerFlowBytesExceedReducer {
                requested: flow_capacity,
                available: reducer_capacity.per_flow_bytes,
            });
        }
        let session_capacity = reducer_capacity.session_bytes;
        if config.max_target_read_bytes > flow_capacity {
            reject!(OwnerTargetConfigError::TargetReadExceedsFlowCapacity {
                requested: config.max_target_read_bytes,
                available: flow_capacity,
            });
        }
        if config.max_target_read_bytes > session_capacity {
            reject!(OwnerTargetConfigError::TargetReadExceedsSessionCapacity {
                requested: config.max_target_read_bytes,
                available: session_capacity,
            });
        }
        let single_data_capacity = flow_capacity.min(MAX_DATA_PAYLOAD_BYTES);
        if config.max_target_read_bytes > single_data_capacity {
            reject!(OwnerTargetConfigError::TargetReadExceedsSingleDataPayload {
                requested: config.max_target_read_bytes,
                available: single_data_capacity,
            });
        }
        let (pending_effects, prepared_outputs) = match Self::prepare_cold_effect_storage(config) {
            Ok(storage) => storage,
            Err(source) => reject!(source.into()),
        };
        let ordinary = match crate::resumable::effect_storage::SessionEffectBatch::prepared(3) {
            Ok(batch) => batch,
            Err(source) => reject!(source.into()),
        };
        let source_factory = match if legless_bootstrap {
            supervisor.mint_tcp_port_factory_for_legless_bootstrap(source_config)
        } else {
            supervisor.mint_tcp_port_factory(source_config)
        } {
            Ok(factory) => factory,
            Err(source) => reject!(source.into()),
        };
        supervisor.install_ordinary_effect_storage(ordinary);
        Ok(Self {
            identity: Arc::new(OwnerTargetExecutorIdentity),
            config,
            supervisor,
            target,
            source_factory,
            flows: BTreeMap::new(),
            target_tombstones: BTreeMap::new(),
            pending_effects,
            prepared_outputs,
            outstanding_resume: None,
            next_resume_id: Some(1),
            max_recovery_effects: recovery_bound,
            aborted_effects: None,
            attach_barrier_pending: false,
            service: super::service::OwnerTargetServiceState::Unbound,
        })
    }
}
