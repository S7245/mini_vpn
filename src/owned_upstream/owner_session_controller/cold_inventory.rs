//! Original Controller cold audit and its value-only result predicates.
//! Borrows ordinary/recovery/error ownership; settlement remains in parent.
//! No mutation, allocation or resource transfer. Tests remain under parent tests/.

use super::*;
use crate::owned_upstream::owner_target::error_inventory::{
    ErrorColdInventory, TargetErrorColdAudit, audit_recovery_results,
};

impl OwnerSessionControllerColdInventory {
    pub(in crate::owned_upstream) const fn error_payload(self) -> TargetErrorColdAudit {
        self.retained_error_payload
    }
    pub(in crate::owned_upstream) const fn recovery_payload(self) -> ErrorColdInventory {
        self.retained_recovery_payload
    }
    /// Exact zero for the ownership domains this cold audit actually covers.
    /// Actors, FIFOs, timers and session authority remain separate root domains.
    pub(crate) const fn is_exact_zero(self) -> bool {
        self.retained_outputs == 0
            && self.retained_target_errors == 0
            && matches!(self.retained_error_payload, Ok(payload) if payload.is_exact_zero())
            && self.retained_recovery_payload.is_exact_zero()
            && self.independently_summed_frame_bytes == 0
            && self.stored_frame_bytes == 0
            && self.pending_continuations == 0
            && self.retained_continuations == 0
            && self.malformed_raw_continuations == 0
            && self.total_continuations == 0
            && self.actor_origin_witnesses == 0
            && self.local_origin_witnesses == 0
            && self.malformed_page_origin_witnesses == 0
            && self.malformed_page_arrival_witnesses == 0
            && self.retained_observations == 0
            && self.retained_application_ack_observations == 0
            && self.target_aborted_effect_guards == 0
            && self.target_aborted_effects == 0
            && self.target_retained_ordinary_outputs == 0
            && self.target_retained_ordinary_observations == 0
            && self.target_retained_recovery_outputs == 0
            && self.target_retained_recovery_observations == 0
            && self.controller_retained_recovery_turns == 0
            && self.target_retained_recovery_guards == 0
    }
    pub(crate) const fn retained_outputs(self) -> usize {
        self.retained_outputs
    }
    pub(crate) const fn independently_summed_frame_bytes(self) -> usize {
        self.independently_summed_frame_bytes
    }
    pub(crate) const fn stored_frame_bytes(self) -> usize {
        self.stored_frame_bytes
    }
    pub(crate) const fn pending_continuations(self) -> usize {
        self.pending_continuations
    }
    pub(crate) const fn retained_continuations(self) -> usize {
        self.retained_continuations
    }
    pub(crate) const fn malformed_raw_continuations(self) -> usize {
        self.malformed_raw_continuations
    }
    pub(crate) const fn total_continuations(self) -> usize {
        self.total_continuations
    }
    pub(crate) const fn actor_origin_witnesses(self) -> usize {
        self.actor_origin_witnesses
    }

    pub(crate) const fn local_origin_witnesses(self) -> usize {
        self.local_origin_witnesses
    }

    pub(crate) const fn malformed_page_origin_witnesses(self) -> usize {
        self.malformed_page_origin_witnesses
    }

    pub(crate) const fn malformed_page_arrival_witnesses(self) -> usize {
        self.malformed_page_arrival_witnesses
    }

    pub(crate) const fn retained_observations(self) -> usize {
        self.retained_observations
    }

    pub(crate) const fn retained_application_ack_observations(self) -> usize {
        self.retained_application_ack_observations
    }

    pub(crate) const fn target_aborted_effect_guards(self) -> usize {
        self.target_aborted_effect_guards
    }

    pub(crate) const fn target_aborted_effects(self) -> usize {
        self.target_aborted_effects
    }

    pub(crate) const fn target_retained_ordinary_outputs(self) -> usize {
        self.target_retained_ordinary_outputs
    }

    pub(crate) const fn target_retained_ordinary_observations(self) -> usize {
        self.target_retained_ordinary_observations
    }

    pub(crate) const fn target_retained_recovery_outputs(self) -> usize {
        self.target_retained_recovery_outputs
    }

    pub(crate) const fn target_retained_recovery_observations(self) -> usize {
        self.target_retained_recovery_observations
    }

    pub(crate) const fn controller_retained_recovery_turns(self) -> usize {
        self.controller_retained_recovery_turns
    }

    pub(crate) const fn target_retained_recovery_guards(self) -> usize {
        self.target_retained_recovery_guards
    }
}

impl<T: TargetIo, C> OwnerSessionControllerCore<T, C> {
    pub(crate) fn cold_inventory(
        &self,
    ) -> Result<OwnerSessionControllerColdInventory, OwnerSessionControllerColdInventoryError> {
        self.progress
            .cold_audit()
            .map_err(OwnerSessionControllerColdInventoryError::Progress)?;
        let target = self.executor.snapshot();
        let mut retained_outputs = 0_usize;
        let mut independently_summed_frame_bytes = 0_usize;
        let mut stored_frame_bytes = 0_usize;
        let mut retained_continuations = 0_usize;
        let mut malformed_raw_continuations = 0_usize;
        let mut actor_origin_witnesses = 0_usize;
        let mut local_origin_witnesses = 0_usize;
        let mut malformed_page_origin_witnesses = 0_usize;
        let mut malformed_page_arrival_witnesses = 0_usize;
        let mut retained_observations = self.terminal_grace.fact_count();
        let mut retained_application_ack_observations = 0_usize;
        let pending_continuations = usize::from(self.pending_continuation.is_some());
        if let Some(pending) = self.pending_continuation.as_ref() {
            Self::add_continuation_origin(
                pending.origin,
                &mut actor_origin_witnesses,
                &mut local_origin_witnesses,
            )?;
        }
        if let Some(turn) = self.aborted_turn.as_ref() {
            Self::add_continuation_origin(
                turn.page_origin,
                &mut actor_origin_witnesses,
                &mut local_origin_witnesses,
            )?;
            retained_outputs = turn.retained_output_count();
            if retained_outputs != turn.stored_output_count {
                return Err(
                    OwnerSessionControllerColdInventoryError::StoredOutputCountMismatch {
                        stored: turn.stored_output_count,
                        summed: retained_outputs,
                    },
                );
            }
            independently_summed_frame_bytes =
                AbortedOwnerControllerTurn::sum_frame_bytes(&turn.outputs)
                    .map_err(OwnerControllerFrameByteAuditError::into_cold_inventory_error)?;
            stored_frame_bytes = turn.stored_frame_bytes.cold_inventory_result()?;
            if independently_summed_frame_bytes != stored_frame_bytes {
                return Err(
                    OwnerSessionControllerColdInventoryError::StoredFrameByteMismatch {
                        stored: stored_frame_bytes,
                        summed: independently_summed_frame_bytes,
                    },
                );
            }
            if turn.continuation.is_some() {
                retained_continuations = Self::checked_cold_add(
                    retained_continuations,
                    1,
                    OwnerSessionControllerColdCount::TotalContinuations,
                )?;
            }
            malformed_raw_continuations = turn
                .outputs
                .iter()
                .filter(|output| matches!(output, OwnerTargetOutput::NeedsResume { .. }))
                .count();
            retained_continuations = Self::checked_cold_add(
                retained_continuations,
                malformed_raw_continuations,
                OwnerSessionControllerColdCount::TotalContinuations,
            )?;
            if malformed_raw_continuations > 0 {
                malformed_page_origin_witnesses = 1;
                malformed_page_arrival_witnesses = usize::from(matches!(
                    turn.page_origin,
                    OwnerContinuationOrigin::Actor(_)
                ));
            }
            retained_observations += turn.observations.fact_count();
            retained_application_ack_observations =
                usize::from(turn.observations.application_ack().is_some());
            if retained_application_ack_observations > 0
                && !matches!(turn.page_origin, OwnerContinuationOrigin::Actor(_))
            {
                return Err(
                    OwnerSessionControllerColdInventoryError::RetainedApplicationAckWithoutActorOrigin,
                );
            }
        }
        let total_continuations = Self::checked_cold_add(
            pending_continuations,
            retained_continuations,
            OwnerSessionControllerColdCount::TotalContinuations,
        )?;
        let controller_retained_recovery_turns = usize::from(self.aborted_recovery.is_some());
        if self
            .aborted_recovery
            .as_ref()
            .and_then(|turn| turn.observations.as_ref())
            .is_some_and(|observations| observations.application_ack().is_some())
        {
            return Err(
                OwnerSessionControllerColdInventoryError::RetainedRecoveryAckWithoutActorOrigin,
            );
        }
        Ok(OwnerSessionControllerColdInventory {
            retained_outputs,
            retained_recovery_payload: self
                .audit_recovery_payload()
                .map_err(OwnerSessionControllerColdInventoryError::RecoveryPayload)?,
            retained_error_payload: self
                .aborted_turn
                .as_ref()
                .and_then(|turn| turn.target_error.as_ref())
                .map_or(
                    Ok(ErrorColdInventory::empty()),
                    OwnerTargetError::cold_inventory,
                ),
            retained_target_errors: usize::from(
                self.aborted_turn
                    .as_ref()
                    .is_some_and(|turn| turn.target_error.is_some()),
            ),
            independently_summed_frame_bytes,
            stored_frame_bytes,
            pending_continuations,
            retained_continuations,
            malformed_raw_continuations,
            total_continuations,
            actor_origin_witnesses,
            local_origin_witnesses,
            malformed_page_origin_witnesses,
            malformed_page_arrival_witnesses,
            retained_observations,
            retained_application_ack_observations,
            target_aborted_effect_guards: target.aborted_effect_guards,
            target_aborted_effects: target.aborted_effects,
            target_retained_ordinary_outputs: target.aborted_ordinary_outputs,
            target_retained_ordinary_observations: target.aborted_ordinary_observations,
            target_retained_recovery_outputs: target.aborted_recovery_outputs,
            target_retained_recovery_observations: target.aborted_recovery_observations,
            controller_retained_recovery_turns,
            target_retained_recovery_guards: target.aborted_recovery_guards,
        })
    }
    fn audit_recovery_payload(&self) -> TargetErrorColdAudit {
        let mut inventory = ErrorColdInventory::empty();
        if let Some(pending) = &self.recovery {
            inventory = inventory.checked_add(audit_pending_recovery(pending)?)?;
        }
        if let Some(status) = &self.attach_status {
            let held = match status {
                PendingOwnerAttachStatus::Enqueue(value) => value.cold_inventory()?,
                PendingOwnerAttachStatus::Delivery(value) => value.cold_inventory(),
            };
            inventory = inventory.checked_add(held)?;
        }
        if let Some(pending) = &self.legless_high_water {
            inventory = inventory.checked_add(pending.high_water.cold_inventory()?)?;
        }
        if let Some(quarantined) = &self.terminal_quarantine {
            inventory = inventory.checked_add(quarantined.cold_inventory()?)?;
        }
        if let Some(turn) = &self.aborted_recovery {
            if let Some(pending) = &turn.pending {
                inventory = inventory.checked_add(audit_pending_recovery(pending)?)?;
            }
            inventory = inventory.checked_add(audit_recovery_results(&turn.outputs)?)?;
            if let Some(observations) = &turn.observations {
                inventory = inventory.checked_add(ErrorColdInventory {
                    observation_facts: observations.fact_count(),
                    ..ErrorColdInventory::empty()
                })?;
            }
        }
        Ok(inventory)
    }
    fn checked_cold_add(
        left: usize,
        right: usize,
        category: OwnerSessionControllerColdCount,
    ) -> Result<usize, OwnerSessionControllerColdInventoryError> {
        left.checked_add(right)
            .ok_or(OwnerSessionControllerColdInventoryError::CountOverflow { category })
    }
    fn add_continuation_origin(
        origin: OwnerContinuationOrigin,
        actor_origins: &mut usize,
        local_origins: &mut usize,
    ) -> Result<(), OwnerSessionControllerColdInventoryError> {
        let (slot, category) = match origin {
            OwnerContinuationOrigin::Actor(_) => {
                (actor_origins, OwnerSessionControllerColdCount::ActorOrigins)
            }
            OwnerContinuationOrigin::Local => {
                (local_origins, OwnerSessionControllerColdCount::LocalOrigins)
            }
        };
        *slot = Self::checked_cold_add(*slot, 1, category)?;
        Ok(())
    }
}

fn audit_pending_recovery(pending: &PendingOwnerAttachedRecovery) -> TargetErrorColdAudit {
    match pending {
        PendingOwnerAttachedRecovery::Acceptance(value) => value.cold_inventory(),
        PendingOwnerAttachedRecovery::Initial(value) => value.cold_inventory(),
        PendingOwnerAttachedRecovery::Execute(value) => Ok(ErrorColdInventory {
            recovery: value.cold_inventory(),
            ..ErrorColdInventory::empty()
        }),
        PendingOwnerAttachedRecovery::Publish(value) => value.cold_inventory(),
        PendingOwnerAttachedRecovery::HandoffReady(value) => Ok(ErrorColdInventory {
            recovery: value.cold_inventory(),
            ..ErrorColdInventory::empty()
        }),
        PendingOwnerAttachedRecovery::TerminalBound(value) => value.cold_inventory(),
    }
}

impl OwnerSessionControllerError {
    /// Audit the original returned payload; never include C/Executor-owned peers.
    pub(in crate::owned_upstream) fn cold_inventory(&self) -> TargetErrorColdAudit {
        use OwnerSessionControllerErrorKind::*;
        match &self.kind {
            Target(error) | TargetAndProgress { target: error, .. } => error.cold_inventory(),
            InactiveActorEvent { input, .. }
            | RejectedActorEvent { input, .. }
            | WrongActorClock { input }
            | ActorClock { input, .. } => {
                let OwnerSessionActorEvent { event, arrival: _ } = input.as_ref();
                let mut inventory = ErrorColdInventory::empty();
                inventory.owner.actor_events = 1;
                inventory.owner.arrival_witnesses = 1;
                inventory.payload =
                    crate::resumable::payload_inventory::SessionPayloadInventory::event(event)?;
                Ok(inventory)
            }
            WrongRecoveryOwner { recovery } | RejectedRecoveryStart { recovery } => {
                recovery.cold_inventory()
            }
            RecoveryAcceptanceQueueBlocked { kind }
            | RecoveryQueueBlocked { kind }
            | AttachStatusQueueBlocked { kind } => {
                crate::owned_upstream::owner_target::error_inventory::audit_queue_cause(kind)?;
                Ok(ErrorColdInventory::empty())
            }
            Clock(_)
            | ReadyReceipt(_)
            | ContinueRequired
            | NoPendingContinuation
            | RecoveryRequired
            | NoPendingRecovery
            | RegisteredRecoveryBlocked { .. }
            | RecoveryAcceptanceRequired
            | RecoveryExecuteRequired
            | RecoveryPublishRequired
            | RecoveryHandoffRequired
            | RecoveryHandoffOccupied
            | RecoveryHandoffWrongOwner
            | RecoveryHandoffWrongTransport
            | InitialRetirementRejected { .. }
            | RecoveryTerminalDiscardRequired
            | RejectedRecoveryTerminalDiscard { .. }
            | RecoveryTerminalQuarantined
            | RejectedRecoveryExecute { .. }
            | RejectedRecoveryPublish { .. }
            | RejectedAttachStatusArm { .. }
            | InvalidRecoveryPage
            | RejectedContinuation { .. }
            | InvalidContinuationPage
            | PostCommitProgress(_)
            | SessionExpired
            | Failed => Ok(ErrorColdInventory::empty()),
        }
    }
}
