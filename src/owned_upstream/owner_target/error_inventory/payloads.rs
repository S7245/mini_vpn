//! Complete payload traversal stays beside the original OwnerTarget error.
//! No new business Interface or owner; lower types expose value-only cold views.
//! Entry is the parent error audit. Tests are initial-bootstrap diagnostic leaves.
use super::*;
use crate::owned_upstream::session::SessionOwnerCommand;
use crate::owned_upstream::tcp::DriverInput;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct OwnerPayloadInventory {
    pub(crate) commands: usize,
    pub(crate) driver_inputs: usize,
    pub(crate) client_driver_inputs: usize,
    pub(crate) sealed_inbounds: usize,
    pub(crate) pending_status: usize,
    pub(crate) enqueued_status: usize,
    pub(crate) catch_up_arms: usize,
    pub(crate) status_receipts: usize,
    pub(crate) acceptance_reservations: usize,
    pub(crate) unexecuted_recoveries: usize,
    pub(crate) actor_events: usize,
    pub(crate) arrival_witnesses: usize,
    pub(crate) high_waters: usize,
    pub(crate) pending_publications: usize,
    pub(crate) pending_losses: usize,
    pub(crate) exact_terminals: usize,
}
impl OwnerPayloadInventory {
    pub(super) const fn empty() -> Self {
        Self {
            commands: 0,
            driver_inputs: 0,
            client_driver_inputs: 0,
            sealed_inbounds: 0,
            pending_status: 0,
            enqueued_status: 0,
            catch_up_arms: 0,
            status_receipts: 0,
            acceptance_reservations: 0,
            unexecuted_recoveries: 0,
            actor_events: 0,
            arrival_witnesses: 0,
            high_waters: 0,
            pending_publications: 0,
            pending_losses: 0,
            exact_terminals: 0,
        }
    }
    pub(super) const fn is_exact_zero(self) -> bool {
        self.commands == 0
            && self.driver_inputs == 0
            && self.client_driver_inputs == 0
            && self.sealed_inbounds == 0
            && self.pending_status == 0
            && self.enqueued_status == 0
            && self.catch_up_arms == 0
            && self.status_receipts == 0
            && self.acceptance_reservations == 0
            && self.unexecuted_recoveries == 0
            && self.actor_events == 0
            && self.arrival_witnesses == 0
            && self.high_waters == 0
            && self.pending_publications == 0
            && self.pending_losses == 0
            && self.exact_terminals == 0
    }
    pub(super) fn checked_add(self, other: Self) -> Result<Self, ErrorAuditFailure> {
        let add = |a: usize, b: usize| a.checked_add(b).ok_or(ErrorAuditFailure::CountOverflow);
        Ok(Self {
            commands: add(self.commands, other.commands)?,
            driver_inputs: add(self.driver_inputs, other.driver_inputs)?,
            client_driver_inputs: add(self.client_driver_inputs, other.client_driver_inputs)?,
            sealed_inbounds: add(self.sealed_inbounds, other.sealed_inbounds)?,
            pending_status: add(self.pending_status, other.pending_status)?,
            enqueued_status: add(self.enqueued_status, other.enqueued_status)?,
            catch_up_arms: add(self.catch_up_arms, other.catch_up_arms)?,
            status_receipts: add(self.status_receipts, other.status_receipts)?,
            acceptance_reservations: add(
                self.acceptance_reservations,
                other.acceptance_reservations,
            )?,
            unexecuted_recoveries: add(self.unexecuted_recoveries, other.unexecuted_recoveries)?,
            actor_events: add(self.actor_events, other.actor_events)?,
            arrival_witnesses: add(self.arrival_witnesses, other.arrival_witnesses)?,
            high_waters: add(self.high_waters, other.high_waters)?,
            pending_publications: add(self.pending_publications, other.pending_publications)?,
            pending_losses: add(self.pending_losses, other.pending_losses)?,
            exact_terminals: add(self.exact_terminals, other.exact_terminals)?,
        })
    }
}

pub(super) fn command(command: &SessionOwnerCommand) -> TargetErrorColdAudit {
    let mut inventory = ErrorColdInventory::empty();
    inventory.owner.commands = 1;
    let driver = match command {
        SessionOwnerCommand::Event(event) => {
            inventory.payload = SessionPayloadInventory::event(event)?;
            return Ok(inventory);
        }
        SessionOwnerCommand::Driver(driver) => driver,
        SessionOwnerCommand::ClientDriver(driver) => {
            inventory.owner.client_driver_inputs = 1;
            driver.as_inner()
        }
    };
    inventory.owner.driver_inputs = 1;
    match driver {
        DriverInput::Control(event) => inventory.payload = SessionPayloadInventory::event(event)?,
        DriverInput::Data(data) => inventory.uplink = data.cold_inventory(),
    }
    Ok(inventory)
}

pub(super) fn received(frame: &crate::owned_upstream::leg::LegBoundFrame) -> TargetErrorColdAudit {
    let mut inventory = ErrorColdInventory::empty();
    inventory.owner.sealed_inbounds = 1;
    inventory.payload = frame.cold_payload()?;
    Ok(inventory)
}

impl super::super::PendingOwnerAttachStatusPublication {
    pub(in crate::owned_upstream) fn cold_inventory(&self) -> TargetErrorColdAudit {
        let super::super::PendingOwnerAttachStatusPublication {
            owner: _,
            seal: _,
            endpoint: _,
            status,
            arm,
        } = self;
        let mut inventory = ErrorColdInventory::empty();
        inventory.owner.pending_status = 1;
        inventory.owner.catch_up_arms = usize::from(arm.is_some());
        inventory.payload = SessionPayloadInventory::frame(status)?;
        Ok(inventory)
    }
}

impl super::super::EnqueuedOwnerAttachStatus {
    pub(in crate::owned_upstream) fn cold_inventory(&self) -> ErrorColdInventory {
        let super::super::EnqueuedOwnerAttachStatus {
            owner: _,
            endpoint: _,
            receipt: _,
            arm,
        } = self;
        let mut inventory = ErrorColdInventory::empty();
        inventory.owner.enqueued_status = 1;
        inventory.owner.status_receipts = 1;
        inventory.owner.catch_up_arms = usize::from(arm.is_some());
        inventory
    }
}

pub(super) fn reservation(
    _: &crate::owned_upstream::leg_io::AttachAcceptanceReservation,
) -> ErrorColdInventory {
    let mut inventory = ErrorColdInventory::empty();
    inventory.owner.acceptance_reservations = 1;
    inventory
}

impl super::super::EnqueuedOwnerTargetAttach {
    pub(in crate::owned_upstream) fn cold_inventory(&self) -> TargetErrorColdAudit {
        let Self {
            owner: _,
            attached: _,
            recovery,
        } = self;
        let mut inventory = unexecuted(recovery)?;
        inventory.recovery.attached_authorities = 1;
        Ok(inventory)
    }
}

pub(super) fn terminal(_: &crate::owned_upstream::leg::ExactLegTerminal) -> ErrorColdInventory {
    let mut inventory = ErrorColdInventory::empty();
    inventory.owner.exact_terminals = 1;
    inventory
}

impl super::super::OwnerLeglessHighWater {
    pub(in crate::owned_upstream) fn cold_inventory(&self) -> TargetErrorColdAudit {
        let super::super::OwnerLeglessHighWater {
            owner: _,
            generation: _,
            reason: _,
            resume_grace_expired,
        } = self;
        let mut inventory = ErrorColdInventory::empty();
        inventory.owner.high_waters = 1;
        inventory.payload = SessionPayloadInventory::event(resume_grace_expired)?;
        Ok(inventory)
    }
}

fn admission(value: &super::super::OwnerAttachAcceptanceAdmission) -> ErrorColdInventory {
    match value {
        super::super::OwnerAttachAcceptanceAdmission::Reserved(value) => reservation(value),
        #[cfg(test)]
        super::super::OwnerAttachAcceptanceAdmission::UnreservedForTest => {
            ErrorColdInventory::empty()
        }
    }
}

impl super::super::PendingOwnerTargetAttachPublication {
    pub(in crate::owned_upstream) fn cold_inventory(&self) -> TargetErrorColdAudit {
        let super::super::PendingOwnerTargetAttachPublication {
            owner: _,
            endpoint: _,
            attached: _,
            acceptance,
            effects,
            admission: held,
        } = self;
        let mut inventory = admission(held);
        inventory.owner.pending_publications = 1;
        inventory.recovery.attached_authorities = 1;
        inventory.payload = SessionPayloadInventory::frame(acceptance)?
            .checked_add(SessionPayloadInventory::effect_vec(effects)?)?;
        Ok(inventory)
    }
}

fn unexecuted(value: &super::super::EnqueuedOwnerTargetRecovery) -> TargetErrorColdAudit {
    let super::super::EnqueuedOwnerTargetRecovery {
        acceptance: _,
        effects,
    } = value;
    let mut inventory = ErrorColdInventory::empty();
    inventory.owner.unexecuted_recoveries = 1;
    inventory.recovery.acceptance_receipts = 1;
    inventory.payload = SessionPayloadInventory::effect_vec(effects)?;
    Ok(inventory)
}

impl super::super::PendingOwnerAttachLoss {
    pub(in crate::owned_upstream) fn cold_inventory(&self) -> TargetErrorColdAudit {
        use super::super::TerminalOwnerAttachTail;
        let super::super::PendingOwnerAttachLoss {
            owner: _,
            generation: _,
            reason: _,
            resume_grace_expired,
            loss: _,
            tail,
            phase: _,
        } = self;
        let mut inventory = ErrorColdInventory::empty();
        inventory.owner.pending_losses = 1;
        // PendingLegLoss is the unique holder of this exact reported terminal.
        // This token is NOT a second physical endpoint/FIFO/session allocation.
        inventory.owner.exact_terminals = 1;
        inventory.payload = SessionPayloadInventory::event(resume_grace_expired)?;
        let held = match tail {
            TerminalOwnerAttachTail::Pending {
                endpoint: _,
                acceptance,
                effects,
                admission: held,
            } => {
                let mut value = admission(held);
                value.payload = SessionPayloadInventory::frame(acceptance)?
                    .checked_add(SessionPayloadInventory::effect_vec(effects)?)?;
                value
            }
            TerminalOwnerAttachTail::Enqueued(value) => unexecuted(value)?,
            TerminalOwnerAttachTail::Drain {
                acceptance,
                pending,
                ready,
            } => {
                let mut value = audit_recovery_outputs(pending, ready)?;
                value.recovery.acceptance_receipts = value
                    .recovery
                    .acceptance_receipts
                    .checked_add(usize::from(acceptance.is_some()))
                    .ok_or(ErrorAuditFailure::CountOverflow)?;
                value
            }
            TerminalOwnerAttachTail::Resume {
                acceptance: _,
                resume,
            } => ErrorColdInventory {
                recovery: RecoveryPayloadInventory {
                    acceptance_receipts: 1,
                    ..resume.cold_inventory()
                },
                ..ErrorColdInventory::empty()
            },
            TerminalOwnerAttachTail::HandoffReady => ErrorColdInventory::empty(),
        };
        inventory.checked_add(held)
    }
}

pub(super) fn session_cause(cause: &crate::resumable::SessionError) {
    use crate::resumable::SessionError::*;
    match cause {
        TcpOwnership { source, .. } => tcp_cause(source),
        InvalidProtocolRecord { source } => protocol_cause(source),
        FlowStorageLayout
        | FlowStorageReserve
        | EffectStorageLayout
        | EffectStorageReserve
        | EffectStorageOverflow
        | CrossSessionLeg { .. }
        | LegGenerationMismatch { .. }
        | LegCapabilityMismatch
        | FrameGenerationMismatch { .. }
        | ReplacementGenerationNotNext { .. }
        | CatchUpGenerationMismatch { .. }
        | CatchUpRequiresLegless { .. }
        | InvalidCatchUpCapability
        | LegGenerationExhausted
        | SessionExpired
        | RoleCannotAcceptOwnerAttach(_)
        | RoleCannotAcceptClientAttach(_)
        | ResumeGraceNotActive
        | PeerFrameWhileLegless
        | RoleCannotInitiateOpen(_)
        | PreparedLocalOpenRequiresActive { .. }
        | RoleCannotResolvePeerOpen(_)
        | UnexpectedPeerOpen(_)
        | UnexpectedPeerOpenResult(_)
        | FlowCapacityExceeded { .. }
        | FlowIdExhausted
        | LocalFlowAuthorityExhausted
        | LocalOpenEffectAllocationFailed
        | OpenRequestIdExhausted
        | PeerFlowIdNotMonotonic { .. }
        | RetiredFlow(_)
        | FlowTargetConflict { .. }
        | UnknownFlow(_)
        | FlowOriginMismatch { .. }
        | LocalSourceNotOpen { .. }
        | ConflictingOpenResult { .. }
        | UnexpectedDirection { .. }
        | ReplayByteBudgetExceeded { .. }
        | ReplaySegmentBudgetExceeded { .. }
        | ReceiveByteBudgetExceeded { .. }
        | ReceiveRangeBudgetExceeded { .. }
        | UnexpectedAttachRecord
        | ZeroSinkAcceptance
        | SinkAcceptanceTooLarge { .. }
        | SinkOfferMismatch
        | SinkOfferIdExhausted
        | HalfCloseCompletionIdExhausted
        | TerminalTombstoneIdExhausted
        | TerminalTombstoneCapacityExceeded { .. }
        | CrossSessionLocalFlow
        | LocalFlowAuthorityMismatch
        | CrossSessionOpenRequest
        | OpenRequestMismatch
        | CrossSessionSinkOffer
        | CrossSessionHalfClose
        | HalfCloseCompletionMismatch
        | CrossSessionTerminalGrace
        | UnknownTerminalGrace
        | InvariantViolation(_) => {}
    }
}

pub(super) fn tcp_cause(cause: &crate::resumable::TcpOwnershipError) {
    use crate::resumable::TcpOwnershipError::*;
    match cause {
        ZeroByteCapacity
        | ZeroSegmentCapacity
        | DescriptorLayout
        | DescriptorReserve
        | ReceiveStorageLayout
        | ReceiveStorageReserve
        | EmptyData
        | DataTooLarge { .. }
        | OffsetOverflow { .. }
        | UnexpectedSendOffset { .. }
        | SendAfterClose { .. }
        | ByteCapacityExceeded { .. }
        | SegmentCapacityExceeded { .. }
        | AckRegression { .. }
        | AckBeyondSent { .. }
        | FinalAckBeforeClose
        | FinalAckOffsetMismatch { .. }
        | CloseOffsetMismatch { .. }
        | ConflictingFinalOffset { .. }
        | ReceiveAfterAbandon
        | AcceptAfterAbandon
        | CloseAfterAbandon
        | DataBeyondFinalOffset { .. }
        | ReceiveWindowExceeded { .. }
        | ConflictingOverlap { .. }
        | StaleReceiveReservation { .. }
        | ReceiveReservationExtentMismatch { .. }
        | ReceiveReservationPayloadMismatch
        | ReceiveEpochExhausted
        | CloseBeforeAccepted { .. }
        | CloseBeforeBufferedEnd { .. }
        | InvariantViolation(_) => {}
    }
}

pub(super) fn protocol_cause(cause: &crate::resumable::ProtocolError) {
    use crate::resumable::ProtocolError::*;
    match cause {
        InvalidMagic
        | UnsupportedVersion(_)
        | UnknownRecordType(_)
        | NonZeroFlags(_)
        | Truncated { .. }
        | TrailingBytes { .. }
        | FrameTooLarge
        | InvalidRecordLength { .. }
        | ZeroFlowId
        | ZeroSessionId
        | ZeroAttachNonce
        | ZeroAttachProof
        | ZeroStandbyNonce
        | ZeroLegEpochNonce
        | ZeroProbeSequence
        | ProbeSequenceOverflow
        | ZeroHintSequence
        | HintSequenceOverflow
        | ZeroStandbyProof
        | ZeroLegGeneration
        | InvalidDirection(_)
        | EmptyData
        | DataTooLarge { .. }
        | OffsetOverflow
        | ZeroTargetPort
        | InvalidTargetType(_)
        | InvalidDomainLength { .. }
        | InvalidDomainEncoding
        | ScopedIpv6Target
        | InvalidVersionRange { .. }
        | RequiredFeaturesNotOffered { .. }
        | InvalidOpenResult(_)
        | InvalidResetReason(_)
        | InvalidSwitchHintCause(_)
        | InvalidSwitchHintDirection(_)
        | InvalidBoolean(_)
        | StandbyControlNotNegotiated { .. }
        | StandbyControlFeatureMissing { .. }
        | LegControlRequiresClassification => {}
    }
}

impl super::super::QuarantinedOwnerAttachLoss {
    pub(in crate::owned_upstream) fn cold_inventory(&self) -> TargetErrorColdAudit {
        use super::super::OwnerTerminalAttachDiscardQuarantineSource;
        let Self { pending, source } = self;
        let mut inventory = pending.cold_inventory()?;
        match source {
            OwnerTerminalAttachDiscardQuarantineSource::Supervisor(source) => {
                inventory = inventory.checked_add(audit_supervisor_cause(source)?)?;
            }
            OwnerTerminalAttachDiscardQuarantineSource::Storage { source, effects } => {
                inventory = inventory.checked_add(audit_supervisor_cause(source)?)?;
                inventory.payload = inventory
                    .payload
                    .checked_add(SessionPayloadInventory::batch(effects)?)?;
            }
            OwnerTerminalAttachDiscardQuarantineSource::Effects {
                generation: _,
                effects,
            } => {
                inventory.payload = inventory
                    .payload
                    .checked_add(SessionPayloadInventory::effect_vec(effects)?)?;
            }
        }
        Ok(inventory)
    }
}
