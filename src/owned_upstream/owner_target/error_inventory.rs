//! Borrow-only cold audit of original error ownership, never a second ledger.
//! Traverses original observations, payloads, recovery/terminal tokens and causes;
//! checked count/Layout or invalid-frame failures never become clean zero.
//! No allocation, recursion, resource transfer or DATA-path work. Entry is
//! OwnerTargetError::cold_inventory; C returned_error tests own the first tracer.

mod payloads;
use super::{
    OwnerTargetError, OwnerTargetOutput, OwnerTargetRecoveryDrain, OwnerTargetRecoveryHandoff,
    OwnerTargetRecoveryOutput, OwnerTargetRecoveryResume, OwnerTargetResume,
    SessionSupervisorError, TargetIoError,
};
use crate::owned_upstream::leg_io::LegOutboundQueueErrorKind;
use crate::owned_upstream::tcp::cold_inventory::UplinkColdInventory;
use crate::resumable::payload_inventory::{PayloadAuditFailure, SessionPayloadInventory};
use payloads::OwnerPayloadInventory;
use std::alloc::Layout;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ErrorColdInventory {
    pub(crate) target_error_nodes: usize,
    pub(crate) observation_facts: usize,
    pub(crate) recovery: RecoveryPayloadInventory,
    pub(crate) payload: SessionPayloadInventory,
    pub(crate) owner: OwnerPayloadInventory,
    pub(in crate::owned_upstream) uplink: UplinkColdInventory,
}

/// Original token/value cardinalities, not transport correlation or physical
/// resource release. Array bytes exclude pointees, shared metadata and allocator
/// overhead; wire bytes are not a fabricated stored counter or memory peak.
/// frames/wire bytes are a recovery-only classification of canonical Session
/// payload counts; never add both views as if they were distinct owners.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct RecoveryPayloadInventory {
    pub(crate) cursors: usize,
    pub(crate) output_values: usize,
    pub(crate) pending_capacity: usize,
    pub(crate) ready_capacity: usize,
    pub(crate) container_bytes: usize,
    pub(crate) frames: usize,
    pub(crate) frame_encoded_bytes: usize,
    pub(crate) resumes: usize,
    pub(crate) attached_authorities: usize,
    pub(crate) acceptance_receipts: usize,
}

impl RecoveryPayloadInventory {
    pub(crate) const fn empty() -> Self {
        Self {
            cursors: 0,
            output_values: 0,
            pending_capacity: 0,
            ready_capacity: 0,
            container_bytes: 0,
            frames: 0,
            frame_encoded_bytes: 0,
            resumes: 0,
            attached_authorities: 0,
            acceptance_receipts: 0,
        }
    }

    pub(crate) const fn is_exact_zero(self) -> bool {
        self.cursors == 0
            && self.output_values == 0
            && self.pending_capacity == 0
            && self.ready_capacity == 0
            && self.container_bytes == 0
            && self.frames == 0
            && self.frame_encoded_bytes == 0
            && self.resumes == 0
            && self.attached_authorities == 0
            && self.acceptance_receipts == 0
    }

    pub(crate) fn checked_add(self, other: Self) -> Result<Self, ErrorAuditFailure> {
        let add = |left: usize, right| {
            left.checked_add(right)
                .ok_or(ErrorAuditFailure::CountOverflow)
        };
        Ok(Self {
            cursors: add(self.cursors, other.cursors)?,
            output_values: add(self.output_values, other.output_values)?,
            pending_capacity: add(self.pending_capacity, other.pending_capacity)?,
            ready_capacity: add(self.ready_capacity, other.ready_capacity)?,
            container_bytes: add(self.container_bytes, other.container_bytes)?,
            frames: add(self.frames, other.frames)?,
            frame_encoded_bytes: add(self.frame_encoded_bytes, other.frame_encoded_bytes)?,
            resumes: add(self.resumes, other.resumes)?,
            attached_authorities: add(self.attached_authorities, other.attached_authorities)?,
            acceptance_receipts: add(self.acceptance_receipts, other.acceptance_receipts)?,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ErrorAuditFailure {
    CountOverflow,
    Invariant,
    Layout,
    InvalidFrame,
}

pub(crate) type TargetErrorColdAudit = Result<ErrorColdInventory, ErrorAuditFailure>;

impl From<PayloadAuditFailure> for ErrorAuditFailure {
    fn from(error: PayloadAuditFailure) -> Self {
        match error {
            PayloadAuditFailure::CountOverflow => Self::CountOverflow,
            PayloadAuditFailure::Layout => Self::Layout,
            PayloadAuditFailure::InvalidFrame => Self::InvalidFrame,
        }
    }
}

impl ErrorColdInventory {
    pub(crate) const fn empty() -> Self {
        Self {
            target_error_nodes: 0,
            observation_facts: 0,
            recovery: RecoveryPayloadInventory::empty(),
            payload: SessionPayloadInventory::empty(),
            owner: OwnerPayloadInventory::empty(),
            uplink: UplinkColdInventory::empty(),
        }
    }

    pub(crate) const fn is_exact_zero(self) -> bool {
        self.target_error_nodes == 0
            && self.observation_facts == 0
            && self.recovery.is_exact_zero()
            && self.payload.is_exact_zero()
            && self.owner.is_exact_zero()
            && self.uplink.is_exact_zero()
    }

    pub(crate) fn checked_add(self, other: Self) -> TargetErrorColdAudit {
        Ok(Self {
            target_error_nodes: self
                .target_error_nodes
                .checked_add(other.target_error_nodes)
                .ok_or(ErrorAuditFailure::CountOverflow)?,
            recovery: self.recovery.checked_add(other.recovery)?,
            payload: self.payload.checked_add(other.payload)?,
            owner: self.owner.checked_add(other.owner)?,
            uplink: self
                .uplink
                .checked_add(other.uplink)
                .ok_or(ErrorAuditFailure::CountOverflow)?,
            observation_facts: self
                .observation_facts
                .checked_add(other.observation_facts)
                .ok_or(ErrorAuditFailure::CountOverflow)?,
        })
    }
}

impl OwnerTargetError {
    /// Audits original payloads or returns a typed structural/count failure.
    /// Error-node and fact counts are distinct from ordinary/Executor domains.
    pub(in crate::owned_upstream) fn cold_inventory(&self) -> TargetErrorColdAudit {
        use OwnerTargetError::*;
        let mut inventory = ErrorColdInventory::empty();
        let mut current = self;
        loop {
            inventory = inventory.checked_add(ErrorColdInventory {
                target_error_nodes: 1,
                observation_facts: 0,
                ..ErrorColdInventory::empty()
            })?;
            match current {
                PostCommitObservationRetentionInvariant {
                    error,
                    observations,
                    effects,
                } => {
                    inventory.payload = inventory
                        .payload
                        .checked_add(SessionPayloadInventory::batch(effects)?)?;
                    inventory = inventory.checked_add(ErrorColdInventory {
                        target_error_nodes: 0,
                        observation_facts: observations.fact_count(),
                        ..ErrorColdInventory::empty()
                    })?;
                    current = error.as_ref();
                }
                PostCommitObservationConflict { observations, .. } => {
                    return inventory.checked_add(ErrorColdInventory {
                        target_error_nodes: 0,
                        observation_facts: observations.fact_count(),
                        ..ErrorColdInventory::empty()
                    });
                }
                Supervisor(error) => {
                    return inventory.checked_add(audit_supervisor_cause(error)?);
                }
                Target(error) => {
                    audit_target_cause(error);
                    return Ok(inventory);
                }
                FlowPort(error) => {
                    require_value_only(error);
                    return Ok(inventory);
                }
                EffectStorage(_)
                | BoundSourceRequiresSelectedService
                | EffectStepOverflow
                | PendingEffectTurn
                | AttachBarrierPending
                | DuplicateEffectResume
                | EffectResumeIdExhausted
                | PendingEffectCapacityInvariant { .. }
                | GeneratedEffectBoundExceeded { .. }
                | AbortedEffectInvariant
                | RegisteredOwnerAttachReservationInvariant
                | OwnerAttachReservationInvariant
                | AttachedRecoveryObservationInvariant
                | DuplicateOrMismatchedPeerOpen { .. }
                | UnknownTargetFlow { .. }
                | NoPendingTargetWrite { .. }
                | DuplicatePendingTargetWrite { .. }
                | DuplicatePendingTargetCompletion { .. }
                | PendingTargetCompletion { .. }
                | MissingPendingTargetCompletion { .. }
                | CarriedTargetCompletionMismatch { .. }
                | UnexpectedTargetCompletionAdmission { .. }
                | HalfCloseBeforePendingWrite { .. }
                | EmptyTargetOffer { .. }
                | InvalidTargetWriteAcceptance { .. }
                | NoPendingTargetJoin { .. }
                | TargetJoinStillPending { .. }
                | UnexpectedPendingTargetJoin { .. }
                | DuplicateTargetTombstone { .. }
                | UnrecordedTargetTombstone { .. }
                | MissingTargetTombstone { .. }
                | UnexpectedTargetTombstone { .. }
                | TargetTombstoneKindMismatch { .. }
                | InternalReceiptEscapedSupervisor
                | ClientOnlyEffectAtOwner
                | MissingTargetReadSource { .. }
                | MissingTargetReadDriverInput { .. }
                | UnexpectedTargetReadDriverInput { .. }
                | MissingTargetFailureDriverInput { .. }
                | UnexpectedTargetFailureDriverInput { .. } => return Ok(inventory),
                RejectedEvent { block, event } => {
                    require_value_only(block);
                    inventory.payload = inventory
                        .payload
                        .checked_add(SessionPayloadInventory::event(event)?)?;
                    return Ok(inventory);
                }
                RejectedCommand { block, command } => {
                    require_value_only(block);
                    return inventory.checked_add(payloads::command(command)?);
                }
                RejectedOwnerAttach { block, received } => {
                    require_value_only(block);
                    return inventory.checked_add(payloads::received(received)?);
                }
                RejectedRegisteredOwnerAttach { source, received } => {
                    require_value_only(source);
                    return inventory.checked_add(payloads::received(received)?);
                }
                RejectedCatchUpOwnerAttach { source, received } => {
                    return inventory
                        .checked_add(payloads::received(received)?)?
                        .checked_add(audit_supervisor_cause(source)?);
                }
                RegisteredOwnerAttachTerminalTransition { source, received }
                | OwnerAttachTerminalTransition { source, received } => {
                    require_value_only(source);
                    return inventory.checked_add(payloads::received(received)?);
                }
                OwnerAttachEndpointLost { received } => {
                    return inventory.checked_add(payloads::received(received)?);
                }
                RejectedOwnerAttachQueueReservation { source, received } => {
                    require_value_only(source);
                    return inventory.checked_add(payloads::received(received)?);
                }
                RejectedOwnerStandbyAuthentication { block, inbound } => {
                    require_value_only(block);
                    inventory.owner.sealed_inbounds = inventory
                        .owner
                        .sealed_inbounds
                        .checked_add(1)
                        .ok_or(ErrorAuditFailure::CountOverflow)?;
                    inventory.payload = inventory.payload.checked_add(inbound.cold_payload()?)?;
                    return Ok(inventory);
                }
                RegisteredOwnerAttachRestoreFailure(failure) => {
                    require_value_only(&failure.source);
                    // Reservation is still in its standby slot, not this error.
                    return inventory.checked_add(audit_supervisor_cause(&failure.error)?);
                }
                CatchUpOwnerAttachReleaseFailure(failure) => {
                    return inventory
                        .checked_add(payloads::reservation(&failure.reservation))?
                        .checked_add(audit_supervisor_cause(&failure.error)?);
                }
                RejectedOwnerAttachStatusArm { block, publication }
                | RejectedEnqueuedOwnerAttachStatusAbandon { block, publication } => {
                    require_value_only(block);
                    return inventory.checked_add(publication.cold_inventory());
                }
                RejectedPendingOwnerAttachStatusAbandon { block, publication } => {
                    require_value_only(block);
                    return inventory.checked_add(publication.cold_inventory()?);
                }
                PendingOwnerAttachStatusAbandonRejected {
                    source,
                    publication,
                } => {
                    return inventory
                        .checked_add(publication.cold_inventory()?)?
                        .checked_add(audit_supervisor_cause(source)?);
                }
                EnqueuedOwnerAttachStatusAbandonRejected {
                    source,
                    publication,
                } => {
                    return inventory
                        .checked_add(publication.cold_inventory())?
                        .checked_add(audit_supervisor_cause(source)?);
                }
                PostCommitRecoveryRetentionInvariant { error, recovery } => {
                    inventory = inventory.checked_add(recovery.cold_inventory()?)?;
                    current = error.as_ref();
                }
                RejectedEffectResume { block, resume } => {
                    require_value_only(block);
                    inventory.recovery = inventory.recovery.checked_add(resume.cold_inventory())?;
                    return Ok(inventory);
                }
                RejectedAttachedRecoveryResume { block, recovery } => {
                    require_value_only(block);
                    inventory.recovery =
                        inventory.recovery.checked_add(recovery.cold_inventory())?;
                    return Ok(inventory);
                }
                RejectedAttachedRecoveryDrain { block, drain } => {
                    require_value_only(block);
                    inventory = inventory.checked_add(drain.cold_inventory()?)?;
                    return Ok(inventory);
                }
                AttachedRecoveryQueueBlocked { kind, drain } => {
                    audit_queue_cause(kind)?;
                    inventory = inventory.checked_add(drain.cold_inventory()?)?;
                    return Ok(inventory);
                }
                AttachedRecoveryOutputInvariant { drain } => {
                    inventory = inventory.checked_add(drain.cold_inventory()?)?;
                    return Ok(inventory);
                }
                RejectedAttachedRecovery { block, recovery } => {
                    require_value_only(block);
                    return inventory.checked_add(recovery.cold_inventory()?);
                }
                RejectedLeglessHighWater { high_water } => {
                    return inventory.checked_add(high_water.cold_inventory()?);
                }
                RejectedPendingAttachLoss {
                    block,
                    pending,
                    terminal,
                } => {
                    require_value_only(block);
                    return inventory
                        .checked_add(pending.cold_inventory()?)?
                        .checked_add(payloads::terminal(terminal));
                }
                RejectedAttachedRecoveryTerminalization {
                    block,
                    recovery,
                    terminal,
                } => {
                    require_value_only(block);
                    return inventory
                        .checked_add(recovery.cold_inventory()?)?
                        .checked_add(payloads::terminal(terminal));
                }
                RejectedAttachedRecoveryDrainTerminalization {
                    block,
                    drain,
                    terminal,
                } => {
                    require_value_only(block);
                    return inventory
                        .checked_add(drain.cold_inventory()?)?
                        .checked_add(payloads::terminal(terminal));
                }
                RejectedAttachedRecoveryResumeTerminalization {
                    block,
                    recovery,
                    terminal,
                } => {
                    require_value_only(block);
                    inventory.recovery =
                        inventory.recovery.checked_add(recovery.cold_inventory())?;
                    return inventory.checked_add(payloads::terminal(terminal));
                }
                RejectedTerminalAttachRecoveryDiscard { block, pending } => {
                    require_value_only(block);
                    return inventory.checked_add(pending.cold_inventory()?);
                }
                TerminalAttachRecoveryDiscardInvariant { source, pending } => {
                    return inventory
                        .checked_add(pending.cold_inventory()?)?
                        .checked_add(audit_supervisor_cause(source)?);
                }
                TerminalAttachRecoveryStorageInvariant {
                    source,
                    effects,
                    pending,
                } => {
                    inventory.payload = inventory
                        .payload
                        .checked_add(SessionPayloadInventory::batch(effects)?)?;
                    return inventory
                        .checked_add(pending.cold_inventory()?)?
                        .checked_add(audit_supervisor_cause(source)?);
                }
                PendingAttachLossEffectInvariant {
                    generation,
                    effects,
                    pending,
                } => {
                    require_value_only(generation);
                    inventory.payload = inventory
                        .payload
                        .checked_add(SessionPayloadInventory::effect_vec(effects)?)?;
                    return inventory.checked_add(pending.cold_inventory()?);
                }
            }
        }
    }
}

impl OwnerTargetResume {
    pub(in crate::owned_upstream) fn cold_inventory(&self) -> RecoveryPayloadInventory {
        let OwnerTargetResume { owner: _, id: _ } = self;
        RecoveryPayloadInventory {
            resumes: 1,
            ..RecoveryPayloadInventory::empty()
        }
    }
}

impl OwnerTargetRecoveryResume {
    pub(in crate::owned_upstream) fn cold_inventory(&self) -> RecoveryPayloadInventory {
        let Self {
            attached: _,
            acceptance: _,
            resume,
        } = self;
        RecoveryPayloadInventory {
            attached_authorities: 1,
            acceptance_receipts: 1,
            ..resume.cold_inventory()
        }
    }
}

impl OwnerTargetRecoveryDrain {
    pub(in crate::owned_upstream) fn cold_inventory(&self) -> TargetErrorColdAudit {
        let OwnerTargetRecoveryDrain {
            owner: _,
            attached,
            acceptance,
            pending,
            ready,
        } = self;
        let mut inventory = audit_recovery_outputs(pending, ready)?;
        inventory.recovery = inventory.recovery.checked_add(RecoveryPayloadInventory {
            cursors: 1,
            attached_authorities: usize::from(attached.is_some()),
            acceptance_receipts: usize::from(acceptance.is_some()),
            ..RecoveryPayloadInventory::empty()
        })?;
        Ok(inventory)
    }
}

// Vec references deliberately preserve the original arrays' retained capacities.
fn audit_recovery_outputs(
    pending: &std::collections::VecDeque<OwnerTargetOutput>,
    ready: &Vec<OwnerTargetRecoveryOutput>,
) -> TargetErrorColdAudit {
    let mut inventory = audit_recovery_results(ready)?;
    inventory.recovery = inventory.recovery.checked_add(RecoveryPayloadInventory {
        output_values: pending.len(),
        pending_capacity: pending.capacity(),
        container_bytes: Layout::array::<OwnerTargetOutput>(pending.capacity())
            .map_err(|_| ErrorAuditFailure::Layout)?
            .size(),
        ..RecoveryPayloadInventory::empty()
    })?;
    for output in pending {
        let recovery = match output {
            OwnerTargetOutput::Transmit(frame) => {
                let payload = SessionPayloadInventory::frame(frame)?;
                inventory.payload = inventory.payload.checked_add(payload)?;
                RecoveryPayloadInventory {
                    frames: 1,
                    frame_encoded_bytes: payload.session_encoded_bytes,
                    ..RecoveryPayloadInventory::empty()
                }
            }
            OwnerTargetOutput::NeedsResume { resume } => resume.cold_inventory(),
            OwnerTargetOutput::LegActivated { .. }
            | OwnerTargetOutput::ResumeGraceStarted { .. }
            | OwnerTargetOutput::TerminalGraceStarted { .. }
            | OwnerTargetOutput::SessionExpired => RecoveryPayloadInventory::empty(),
        };
        inventory.recovery = inventory.recovery.checked_add(recovery)?;
    }
    Ok(inventory)
}

// Original result backing can exist without a cursor; never invent a holder.
pub(in crate::owned_upstream) fn audit_recovery_results(
    ready: &Vec<OwnerTargetRecoveryOutput>,
) -> TargetErrorColdAudit {
    let mut inventory = ErrorColdInventory {
        recovery: RecoveryPayloadInventory {
            output_values: ready.len(),
            ready_capacity: ready.capacity(),
            container_bytes: Layout::array::<OwnerTargetRecoveryOutput>(ready.capacity())
                .map_err(|_| ErrorAuditFailure::Layout)?
                .size(),
            ..RecoveryPayloadInventory::empty()
        },
        ..ErrorColdInventory::empty()
    };
    for output in ready {
        let recovery = match output {
            OwnerTargetRecoveryOutput::NeedsResume { resume } => resume.cold_inventory(),
            OwnerTargetRecoveryOutput::RecoveryCompleted { handoff } => handoff.cold_inventory(),
            OwnerTargetRecoveryOutput::LegActivated { .. }
            | OwnerTargetRecoveryOutput::ResumeGraceStarted { .. }
            | OwnerTargetRecoveryOutput::TerminalGraceStarted { .. }
            | OwnerTargetRecoveryOutput::SessionExpired => RecoveryPayloadInventory::empty(),
        };
        inventory.recovery = inventory.recovery.checked_add(recovery)?;
    }
    Ok(inventory)
}

impl OwnerTargetRecoveryHandoff {
    pub(in crate::owned_upstream) fn cold_inventory(&self) -> RecoveryPayloadInventory {
        let Self {
            owner: _,
            attached: _,
        } = self;
        RecoveryPayloadInventory {
            attached_authorities: 1,
            ..RecoveryPayloadInventory::empty()
        }
    }
}

pub(in crate::owned_upstream) fn audit_queue_cause(
    cause: &LegOutboundQueueErrorKind,
) -> Result<(), ErrorAuditFailure> {
    use LegOutboundQueueErrorKind::*;
    match cause {
        InvalidFrame(error) => {
            payloads::protocol_cause(error);
            Ok(())
        }
        ByteCountOverflow
        | EnqueueOrdinalExhausted
        | EndpointLost
        | ResetDeliveryReceiptMismatch
        | NotAttachAcceptance
        | RegisteredAttachAdmissionRequired
        | AttachAcceptanceAdmissionRequired
        | AuthenticatedAttachStatusAdmissionRequired
        | AttachAcceptanceReserved
        | AttachRequestPending
        | AttachStatusPending
        | ClientReplacementRecoveryPending
        | ClientRecoveryTerminal
        | OwnerRecoveryTerminal
        | NotAttachRequest
        | NotAttachStatus
        | WrongLeg
        | WrongAttachAcceptanceReservation
        | MismatchedAttachRequest
        | MismatchedAttachAcceptance
        | AttachRequestNotFirst { .. }
        | AttachStatusNotFirst { .. }
        | AttachAcceptanceNotFirst { .. }
        | CapacityExceeded { .. } => Ok(()),
    }
}

// Copy is a structural promise that this cause cannot uniquely own a resource.
fn require_value_only<T: Copy>(_: &T) {}

fn audit_target_cause(cause: &TargetIoError) {
    use TargetIoError::*;
    match cause {
        ReadinessInvariant(_)
        | StorageLayout
        | StorageReserve
        | OutputPublicationUnknown { .. }
        | StaleFlowId { .. }
        | ConflictingTarget { .. }
        | UnknownFlow { .. }
        | EmptyWrite
        | EmptyReadFeed
        | ZeroDrainLimit
        | ZeroReadLimit
        | ZeroAcceptedWrite
        | WriteDirectiveLimit { .. }
        | CounterOverflow { .. }
        | BufferCapacityExceeded { .. }
        | ReadEofAlreadyQueued { .. }
        | ReadAfterEof { .. }
        | WriteAfterHalfClose { .. }
        | FlowTerminal { .. }
        | FlowJoined { .. }
        | JoinBeforeTerminal { .. } => {}
    }
}

fn audit_supervisor_cause(cause: &SessionSupervisorError) -> TargetErrorColdAudit {
    use SessionSupervisorError::*;
    match cause {
        UplinkStorageCapacity { input }
        | UplinkStorageLayout { input }
        | UplinkStorageReserve { input } => {
            let mut inventory = ErrorColdInventory::empty();
            inventory.owner.driver_inputs = 1;
            match input {
                crate::owned_upstream::tcp::DriverInput::Data(data) => {
                    inventory.uplink = data.cold_inventory()
                }
                crate::owned_upstream::tcp::DriverInput::Control(event) => {
                    inventory.payload = SessionPayloadInventory::event(event)?
                }
            }
            Ok(inventory)
        }
        RecoveryEffectStorage { source, received } => {
            payloads::session_cause(source);
            payloads::received(received)
        }
        RejectedEvent { block, event } => {
            require_value_only(block);
            Ok(ErrorColdInventory {
                payload: SessionPayloadInventory::event(event)?,
                ..ErrorColdInventory::empty()
            })
        }
        RejectedCommand { block, command } => {
            require_value_only(block);
            payloads::command(command)
        }
        Reducer(source) | ReducerReplayCapacityInvariant { source } => {
            payloads::session_cause(source);
            Ok(ErrorColdInventory::empty())
        }
        LegProvenance(source) => {
            match source {
                crate::owned_upstream::leg::LegProvenanceError::WrongLeg
                | crate::owned_upstream::leg::LegProvenanceError::Rejected => {}
            }
            Ok(ErrorColdInventory::empty())
        }
        FlowPort(error) => {
            require_value_only(error);
            Ok(ErrorColdInventory::empty())
        }
        #[cfg(test)]
        OwnerRoleRequired { .. }
        | AuthorityModelSessionMismatch
        | AuthorityModelSemanticsMismatch => Ok(ErrorColdInventory::empty()),
        PoisonedOwnershipInvariant
        | MissingAttachAuthority
        | OwnerStandbyAuthenticationRejected
        | OwnerRegisteredAttachGateRejected
        | OwnerCatchUpPermitMissing
        | OwnerCatchUpPermitNotArmed
        | OwnerCatchUpPermitPending
        | OwnerCatchUpArmRejected
        | OwnerCatchUpStatusRejected
        | OwnerCatchUpPermitConflict
        | OwnerCatchUpAttachGateRejected
        | OwnerBootstrapCompleted
        | AuthorityModelGenerationMismatch { .. }
        | UnownedLocalData
        | UplinkFactoryProvenanceMismatch
        | OwnerAttachTransactionRequired
        | ReplacementProvenanceRequired
        | ReplacementEndpointLost
        | MissingReplayStored
        | DataCustodyInvariant
        | DuplicateReplayStored
        | UnownedReplayStored
        | DuplicateReplayAcknowledgedObservation
        | DuplicateSinkAcceptedObservation
        | DuplicateReplayOutstandingObservation
        | DuplicateFlowTerminalObservation
        | DuplicateSessionExpiredObservation
        | InvalidReplayAcknowledgedDelta
        | DuplicateReplayExtent
        | ReplayExtentDisappeared => Ok(ErrorColdInventory::empty()),
    }
}
