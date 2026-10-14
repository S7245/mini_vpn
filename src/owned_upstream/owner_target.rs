//! Sole Target owner; executes original capabilities once; Bound commands stay local.
mod binding;
mod commands;
mod completion;
mod construction;
mod effect_storage;
pub(in crate::owned_upstream) mod error_inventory;
mod observed_turn;
mod read_source;
mod service;
#[cfg(test)]
pub(super) struct OwnerCommandPrototype(commands::SessionCommands);
use super::leg::{
    AttachedLeg, BoundInbound, EstablishedLeg, ExactLegTerminal, LegBoundFrame, LegSeal,
    LegTerminalTransitionError, LegTransportEndpoint, LegTransportTerminalReason, PendingLegLoss,
    transport_endpoint_is_open,
};
use super::leg_io::{
    AttachAcceptanceEnqueued, AttachAcceptanceReservation, AttachAcceptanceReserveError,
    AttachStatusEnqueued, LegIo, LegOutboundQueue, LegOutboundQueueErrorKind,
    PhysicalLegActorBinding, PhysicalLegRuntimeIncarnation,
};
use super::session::SessionOwnerCommand;
use super::standby::{ExactAuthenticatedStandby, OwnerRegisteredAttachGateError, OwnerStandbySlot};
use super::supervisor::{
    AuthenticatedOwnerAttachStatus, LeglessOwnerSessionBootstrap, OwnerAttachPublication,
    OwnerCatchUpArm, OwnerSessionBootstrap, ReducerAdmissionBlock, ReducerApplicationAckAdvanced,
    ReducerFlowDirectionTerminal, ReducerOwnerObservations, ReducerReplayBecameOutstanding,
    ReducerSessionExpired, SessionSupervisor, SessionSupervisorError, SessionSupervisorSnapshot,
    TcpPortFactoryMintError,
};
use super::target::{
    TargetCancelCompletion, TargetHalfCloseCompletion, TargetIo, TargetIoCapacityWitness,
    TargetIoColdInventory, TargetIoColdInventoryError, TargetIoError, TargetJoinCompletion,
    TargetOpenCompletion, TargetOpenFailure, TargetResetCompletion, TargetTerminalKind,
    TargetTombstoneRetireCompletion, TargetWriteCompletion,
};
use super::tcp::{
    DriverInput, FlowPortConfig, FlowPortConfigError, FlowPortError, ResumableTcpDriver,
    ResumableTcpFlow, ResumableTcpPortFactory, TcpPortFactoryCapacityWitness,
};
use crate::resumable::{
    AttachNonce, ByteOffset, Direction, FlowFinishReason, Frame, LegGeneration,
    MAX_DATA_PAYLOAD_BYTES, OpenResultCode, PeerOpenRequest, Record, ResetReason, SessionEffect,
    SessionEvent, SessionFlowId, SessionId, SinkOffer, TcpDataSegment, TerminalGrace,
};
use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::sync::{Arc, Weak};
use thiserror::Error;
// Audited reducer fan-out for every non-recovery command the Target executor
// can issue: one Transmit, one HalfClose/FlowFinished, and one following offer.
const MAX_INCREMENTAL_EFFECT_BURST: usize = 3;
/// Closed work bound for one synchronous reducer/Target turn.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct OwnerTargetConfig {
    max_effect_steps: usize,
    max_pending_effects: usize,
    max_target_read_bytes: usize,
}
impl OwnerTargetConfig {
    pub(crate) fn new(
        max_effect_steps: usize,
        max_pending_effects: usize,
        max_target_read_bytes: usize,
    ) -> Result<Self, OwnerTargetConfigError> {
        if max_effect_steps == 0 {
            return Err(OwnerTargetConfigError::ZeroEffectSteps);
        }
        if max_target_read_bytes == 0 {
            return Err(OwnerTargetConfigError::ZeroTargetReadBytes);
        }
        if max_pending_effects == 0 {
            return Err(OwnerTargetConfigError::ZeroPendingEffects);
        }
        Ok(Self {
            max_effect_steps,
            max_pending_effects,
            max_target_read_bytes,
        })
    }

    pub(crate) const fn max_effect_steps(self) -> usize {
        self.max_effect_steps
    }

    pub(crate) const fn max_target_read_bytes(self) -> usize {
        self.max_target_read_bytes
    }

    pub(crate) const fn max_pending_effects(self) -> usize {
        self.max_pending_effects
    }
}
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub(crate) enum OwnerTargetConfigError {
    #[error("owner effect backing cannot be prepared: {0}")]
    EffectStorage(#[from] crate::resumable::effect_storage::EffectStorageError),
    #[error("owner Target effect-step bound must be non-zero")]
    ZeroEffectSteps,
    #[error("owner Target read bound must be non-zero")]
    ZeroTargetReadBytes,
    #[error("owner Target pending-effect bound must be non-zero")]
    ZeroPendingEffects,
    #[error("owner Target executor requires an Owner-role session supervisor")]
    OwnerRoleRequired,
    #[error("owner Target read bound {requested} exceeds per-flow source capacity {available}")]
    TargetReadExceedsFlowCapacity { requested: usize, available: usize },
    #[error("owner Target read bound {requested} exceeds session source capacity {available}")]
    TargetReadExceedsSessionCapacity { requested: usize, available: usize },
    #[error(
        "owner Target read bound {requested} exceeds one source DATA record capacity {available}"
    )]
    TargetReadExceedsSingleDataPayload { requested: usize, available: usize },
    #[error("owner recovery effect-capacity derivation overflowed")]
    RecoveryEffectCapacityOverflow,
    #[error(
        "owner pending-effect capacity {available} is below the derived requirement {required}"
    )]
    PendingEffectCapacityTooSmall { required: usize, available: usize },
    #[error(
        "owner Target per-flow byte capacity {requested} exceeds reducer send capacity {available}"
    )]
    PerFlowBytesExceedReducer { requested: usize, available: usize },
    #[error("owner session already minted its sole TCP source factory")]
    SourceFactoryAlreadyMinted,
    #[error("owner TCP source factory cannot represent the reducer limits: {0}")]
    InvalidSourceFactoryCapacity(FlowPortConfigError),
    #[error("owner TCP source factory cannot be minted from this supervisor state: {0}")]
    InvalidSourceFactoryState(TcpPortFactoryMintError),
}

impl From<TcpPortFactoryMintError> for OwnerTargetConfigError {
    fn from(error: TcpPortFactoryMintError) -> Self {
        match error {
            TcpPortFactoryMintError::AlreadyMinted => Self::SourceFactoryAlreadyMinted,
            TcpPortFactoryMintError::PerFlowBytesExceedReducer {
                requested,
                available,
            } => Self::PerFlowBytesExceedReducer {
                requested,
                available,
            },
            TcpPortFactoryMintError::InvalidDerivedCapacity(source) => {
                Self::InvalidSourceFactoryCapacity(source)
            }
            state @ (TcpPortFactoryMintError::FactoryAdmissionClosed
            | TcpPortFactoryMintError::PoisonedSupervisor
            | TcpPortFactoryMintError::SessionNotPristine { .. }) => {
                Self::InvalidSourceFactoryState(state)
            }
        }
    }
}
/// Transport and lifecycle facts that remain after Target effects are fully
/// consumed.  Only a reducer-produced `Frame` can become transport output.
#[derive(PartialEq, Eq)]
pub(crate) enum OwnerTargetOutput {
    Transmit(Frame),
    LegActivated { generation: LegGeneration },
    ResumeGraceStarted { generation: LegGeneration },
    TerminalGraceStarted { terminal: TerminalGrace },
    SessionExpired,
    NeedsResume { resume: OwnerTargetResume },
}
/// Reducer-committed positive application-ACK progress observed only through
/// the serialized owner Target executor. The reducer receipt and its replay
/// ownership never leave the supervisor.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct OwnerTargetApplicationAckAdvanced(ReducerApplicationAckAdvanced);

impl OwnerTargetApplicationAckAdvanced {
    pub(crate) const fn session_id(&self) -> crate::resumable::SessionId {
        self.0.session_id()
    }

    pub(crate) const fn flow_id(&self) -> SessionFlowId {
        self.0.flow_id()
    }

    pub(crate) const fn generation(&self) -> LegGeneration {
        self.0.generation()
    }

    pub(crate) const fn direction(&self) -> crate::resumable::Direction {
        self.0.direction()
    }

    pub(crate) const fn previous_accepted(&self) -> crate::resumable::ByteOffset {
        self.0.previous_accepted()
    }

    pub(crate) const fn next_accepted(&self) -> crate::resumable::ByteOffset {
        self.0.next_accepted()
    }

    pub(crate) const fn released_bytes(&self) -> usize {
        self.0.released_bytes()
    }

    pub(crate) const fn final_accepted(&self) -> bool {
        self.0.final_accepted()
    }

    /// Exact oldest byte still retained for this flow/direction after the
    /// whole reducer turn, including any same-turn terminal cleanup.
    pub(crate) const fn remaining_oldest(&self) -> Option<ByteOffset> {
        self.0.remaining_oldest()
    }
}
/// Reducer-proven first transition from no replay to outstanding replay for
/// one exact flow direction. No byte payload or replay authority escapes.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct OwnerTargetReplayBecameOutstanding(ReducerReplayBecameOutstanding);

impl OwnerTargetReplayBecameOutstanding {
    pub(crate) const fn session_id(&self) -> SessionId {
        self.0.session_id()
    }

    pub(crate) const fn flow_id(&self) -> SessionFlowId {
        self.0.flow_id()
    }

    pub(crate) const fn generation(&self) -> LegGeneration {
        self.0.generation()
    }

    pub(crate) const fn direction(&self) -> Direction {
        self.0.direction()
    }

    pub(crate) const fn start(&self) -> ByteOffset {
        self.0.start()
    }

    pub(crate) const fn end(&self) -> ByteOffset {
        self.0.end()
    }
}
/// Reducer-proven terminal transition for one exact flow direction.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct OwnerTargetFlowDirectionTerminal(ReducerFlowDirectionTerminal);
impl OwnerTargetFlowDirectionTerminal {
    pub(crate) const fn session_id(&self) -> SessionId {
        self.0.session_id()
    }
    pub(crate) const fn flow_id(&self) -> SessionFlowId {
        self.0.flow_id()
    }
    pub(crate) const fn generation(&self) -> LegGeneration {
        self.0.generation()
    }
    pub(crate) const fn direction(&self) -> Direction {
        self.0.direction()
    }

    pub(super) const fn terminal(&self) -> &TerminalGrace {
        self.0.terminal()
    }
}
/// Reducer-proven retirement of one complete flow after its exact terminal
/// grace token committed. This is stronger than a directional terminal fact
/// and is the only progress observation that authorizes releasing both
/// directional entries.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct OwnerTargetFlowRetired {
    session_id: SessionId,
    flow_id: SessionFlowId,
    generation: LegGeneration,
}

impl OwnerTargetFlowRetired {
    pub(crate) const fn session_id(&self) -> SessionId {
        self.session_id
    }

    pub(crate) const fn flow_id(&self) -> SessionFlowId {
        self.flow_id
    }

    pub(crate) const fn generation(&self) -> LegGeneration {
        self.generation
    }
}
/// Reducer-proven terminal transition for the whole session.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct OwnerTargetSessionExpired(ReducerSessionExpired);

impl OwnerTargetSessionExpired {
    pub(crate) const fn session_id(&self) -> SessionId {
        self.0.session_id()
    }

    pub(crate) const fn generation(&self) -> LegGeneration {
        self.0.generation()
    }
}
/// Fixed-size reducer facts from one serialized owner turn. Each optional
/// field is single-valued and stores neither replay bytes nor ACK history.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct OwnerTargetReducerObservations {
    replay_became_outstanding: Option<OwnerTargetReplayBecameOutstanding>,
    application_ack: Option<OwnerTargetApplicationAckAdvanced>,
    flow_terminal: Option<OwnerTargetFlowDirectionTerminal>,
    flow_retired: Option<OwnerTargetFlowRetired>,
    session_expired: Option<OwnerTargetSessionExpired>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OwnerTargetObservationKind {
    ReplayBecameOutstanding,
    ApplicationAck,
    FlowTerminal,
    FlowRetired,
    SessionExpired,
}

impl OwnerTargetReducerObservations {
    fn from_supervisor(observations: ReducerOwnerObservations) -> Self {
        let (replay_became_outstanding, application_ack, flow_terminal, session_expired) =
            observations.into_parts();
        Self {
            replay_became_outstanding: replay_became_outstanding
                .map(OwnerTargetReplayBecameOutstanding),
            application_ack: application_ack.map(OwnerTargetApplicationAckAdvanced),
            flow_terminal: flow_terminal.map(OwnerTargetFlowDirectionTerminal),
            flow_retired: None,
            session_expired: session_expired.map(OwnerTargetSessionExpired),
        }
    }
    pub(crate) const fn replay_became_outstanding(
        &self,
    ) -> Option<&OwnerTargetReplayBecameOutstanding> {
        self.replay_became_outstanding.as_ref()
    }
    pub(crate) const fn application_ack(&self) -> Option<&OwnerTargetApplicationAckAdvanced> {
        self.application_ack.as_ref()
    }
    pub(crate) const fn flow_terminal(&self) -> Option<&OwnerTargetFlowDirectionTerminal> {
        self.flow_terminal.as_ref()
    }

    pub(crate) const fn flow_retired(&self) -> Option<&OwnerTargetFlowRetired> {
        self.flow_retired.as_ref()
    }

    pub(crate) const fn session_expired(&self) -> Option<&OwnerTargetSessionExpired> {
        self.session_expired.as_ref()
    }

    pub(crate) const fn is_empty(&self) -> bool {
        self.replay_became_outstanding.is_none()
            && self.application_ack.is_none()
            && self.flow_terminal.is_none()
            && self.flow_retired.is_none()
            && self.session_expired.is_none()
    }

    /// Counts reducer facts, not the presence of the fixed-size container.
    /// Empty post-commit bundles therefore own zero observation facts.
    pub(super) fn fact_count(&self) -> usize {
        usize::from(self.replay_became_outstanding.is_some())
            + usize::from(self.application_ack.is_some())
            + usize::from(self.flow_terminal.is_some())
            + usize::from(self.flow_retired.is_some())
            + usize::from(self.session_expired.is_some())
    }

    pub(super) fn into_terminal_fact(self) -> Option<OwnerTargetFlowDirectionTerminal> {
        self.flow_terminal
    }
    fn into_application_ack(self) -> Option<OwnerTargetApplicationAckAdvanced> {
        self.application_ack
    }

    fn try_merge(&mut self, observations: Self) -> Result<(), OwnerTargetError> {
        let conflict = [
            (
                self.replay_became_outstanding.is_some()
                    && observations.replay_became_outstanding.is_some(),
                OwnerTargetObservationKind::ReplayBecameOutstanding,
            ),
            (
                self.application_ack.is_some() && observations.application_ack.is_some(),
                OwnerTargetObservationKind::ApplicationAck,
            ),
            (
                self.flow_terminal.is_some() && observations.flow_terminal.is_some(),
                OwnerTargetObservationKind::FlowTerminal,
            ),
            (
                self.flow_retired.is_some() && observations.flow_retired.is_some(),
                OwnerTargetObservationKind::FlowRetired,
            ),
            (
                self.session_expired.is_some() && observations.session_expired.is_some(),
                OwnerTargetObservationKind::SessionExpired,
            ),
        ]
        .into_iter()
        .find_map(|(conflict, kind)| conflict.then_some(kind));
        if let Some(kind) = conflict {
            return Err(OwnerTargetError::PostCommitObservationConflict {
                kind,
                observations: Box::new(observations),
            });
        }
        let Self {
            replay_became_outstanding,
            application_ack,
            flow_terminal,
            flow_retired,
            session_expired,
        } = observations;
        if replay_became_outstanding.is_some() {
            self.replay_became_outstanding = replay_became_outstanding;
        }
        if application_ack.is_some() {
            self.application_ack = application_ack;
        }
        if flow_terminal.is_some() {
            self.flow_terminal = flow_terminal;
        }
        if flow_retired.is_some() {
            self.flow_retired = flow_retired;
        }
        if session_expired.is_some() {
            self.session_expired = session_expired;
        }
        Ok(())
    }
}
/// One fully executed owner Target turn plus its fixed post-commit reducer
/// observations. Existing callers continue to use `apply_event` and receive
/// the exact same output vector; the legacy `into_parts` ACK shape is retained.
pub(crate) struct OwnerTargetObservedApply {
    outputs: Vec<OwnerTargetOutput>,
    observations: OwnerTargetReducerObservations,
}

impl OwnerTargetObservedApply {
    pub(crate) fn into_parts(
        self,
    ) -> (
        Vec<OwnerTargetOutput>,
        Option<OwnerTargetApplicationAckAdvanced>,
    ) {
        (self.outputs, self.observations.into_application_ack())
    }

    pub(crate) fn into_progress_parts(
        self,
    ) -> (Vec<OwnerTargetOutput>, OwnerTargetReducerObservations) {
        (self.outputs, self.observations)
    }

    fn into_outputs(self) -> Vec<OwnerTargetOutput> {
        self.outputs
    }
}
/// Post-attach recovery observations after every wire frame has crossed the
/// exact acceptance queue. This type deliberately has no `Transmit(Frame)`
/// variant, so replay bytes cannot escape the serialized FIFO boundary.
pub(crate) enum OwnerTargetRecoveryOutput {
    LegActivated {
        generation: LegGeneration,
    },
    ResumeGraceStarted {
        generation: LegGeneration,
    },
    TerminalGraceStarted {
        terminal: TerminalGrace,
    },
    SessionExpired,
    NeedsResume {
        resume: OwnerTargetRecoveryResume,
    },
    /// Initial recovery and every continuation are durably ordered behind the
    /// acceptance. Only this terminal observation releases receive authority.
    RecoveryCompleted {
        handoff: OwnerTargetRecoveryHandoff,
    },
}

struct OwnerTargetExecutorIdentity;
pub(super) struct OwnerTargetColdPreparationGate(());
/// Private gate allowing only this module to decompose an initial owner
/// bootstrap while constructing the executor-bound handoff in the same call.
/// No physical caller can obtain the intermediate bare `AttachedLeg`.
pub(super) struct OwnerTargetInitialBootstrapInstallGate {
    _private: (),
}

impl OwnerTargetInitialBootstrapInstallGate {
    fn mint() -> Self {
        Self { _private: () }
    }
}
/// Private gate allowing only this module to decompose and, on rejection,
/// exactly reassemble one legless bootstrap around a fallible executor build.
pub(super) struct OwnerTargetLeglessBootstrapInstallGate {
    _private: (),
}

impl OwnerTargetLeglessBootstrapInstallGate {
    fn mint() -> Self {
        Self { _private: () }
    }
}
/// Exact record-specific delivery/settlement state for the initial acceptance.
/// No variant is inferred from aggregate queue length or ordered-wait state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OwnerTargetInitialAcceptanceDelivery {
    AdmittedDeliveryPending,
    DeliveredAwaitingSettlement,
    Settled,
}
/// Borrow-only exact ownership inventory for the initial owner handoff.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct OwnerTargetInitialHandoffInventory {
    attached_authorities: usize,
    acceptance_receipts: usize,
    executor_identity_live: bool,
    physical_actor_identities: usize,
    physical_actor_identity_live: bool,
    acceptance_queue_live: bool,
    delivery: OwnerTargetInitialAcceptanceDelivery,
}

impl OwnerTargetInitialHandoffInventory {
    pub(crate) const fn attached_authorities(self) -> usize {
        self.attached_authorities
    }

    pub(crate) const fn acceptance_receipts(self) -> usize {
        self.acceptance_receipts
    }

    pub(crate) const fn executor_identity_live(self) -> bool {
        self.executor_identity_live
    }

    pub(crate) const fn physical_actor_identities(self) -> usize {
        self.physical_actor_identities
    }

    pub(crate) const fn physical_actor_identity_live(self) -> bool {
        self.physical_actor_identity_live
    }

    pub(crate) const fn acceptance_queue_live(self) -> bool {
        self.acceptance_queue_live
    }

    pub(crate) const fn delivery(self) -> OwnerTargetInitialAcceptanceDelivery {
        self.delivery
    }
}
/// Executor-bound initial owner authority. It keeps the initial AttachedLeg
/// transport seal and its exact acceptance FIFO receipt together; neither is
/// exposed as a naked production value.
#[must_use = "initial owner handoff must remain attached to its executor and acceptance receipt"]
pub(crate) struct OwnerTargetInitialHandoff {
    owner: Weak<OwnerTargetExecutorIdentity>,
    attached: AttachedLeg,
    acceptance: Option<AttachAcceptanceEnqueued>,
    physical_actor: Option<PhysicalLegActorBinding>,
}

impl OwnerTargetInitialHandoff {
    pub(crate) fn generation(&self) -> LegGeneration {
        self.attached.generation()
    }

    pub(crate) fn nonce(&self) -> crate::resumable::AttachNonce {
        self.attached.nonce()
    }

    pub(crate) const fn retained_acceptance_receipts(&self) -> usize {
        if self.acceptance.is_some() { 1 } else { 0 }
    }

    pub(crate) fn acceptance_queue_is_live(&self) -> bool {
        self.acceptance
            .as_ref()
            .is_some_and(AttachAcceptanceEnqueued::queue_is_live)
    }

    pub(crate) const fn physical_actor_identities(&self) -> usize {
        if self.physical_actor.is_some() { 1 } else { 0 }
    }

    pub(super) fn acceptance_belongs_to_queue(&self, queue: &LegOutboundQueue) -> bool {
        self.acceptance
            .as_ref()
            .is_some_and(|acceptance| acceptance.belongs_to_queue(queue))
            || self
                .physical_actor
                .as_ref()
                .is_some_and(|actor| actor.belongs_to_queue(queue))
    }

    /// Inventories the exact receipt without mutating the FIFO. Aggregate
    /// queue frames/waits cannot identify this receipt after later traffic or
    /// cleanup, so they are audited only for internal consistency and never
    /// promoted into a record-specific delivery claim.
    pub(super) fn inventory(
        &self,
        queue: &LegOutboundQueue,
    ) -> Result<OwnerTargetInitialHandoffInventory, OwnerTargetInputBlock> {
        if !self.acceptance_belongs_to_queue(queue) {
            return Err(OwnerTargetInputBlock::WrongAcceptanceQueue);
        }
        let _inventory = queue
            .cold_inventory()
            .ok_or(OwnerTargetInputBlock::InitialAcceptanceQueueInvariant)?;
        let delivery = match self.acceptance.as_ref() {
            Some(acceptance) if acceptance.was_delivered() => {
                OwnerTargetInitialAcceptanceDelivery::DeliveredAwaitingSettlement
            }
            Some(_) => OwnerTargetInitialAcceptanceDelivery::AdmittedDeliveryPending,
            None => OwnerTargetInitialAcceptanceDelivery::Settled,
        };
        Ok(OwnerTargetInitialHandoffInventory {
            attached_authorities: 1,
            acceptance_receipts: self.retained_acceptance_receipts(),
            executor_identity_live: self.owner.upgrade().is_some(),
            physical_actor_identities: self.physical_actor_identities(),
            physical_actor_identity_live: self
                .physical_actor
                .as_ref()
                .is_some_and(PhysicalLegActorBinding::actor_is_live),
            acceptance_queue_live: self.acceptance_queue_is_live(),
            delivery,
        })
    }

    pub(super) fn belongs_to_transport(&self, leg: &EstablishedLeg) -> bool {
        self.matches_transport_identity(leg) && leg.transport_is_open()
    }

    fn matches_transport_identity(&self, leg: &EstablishedLeg) -> bool {
        self.attached.belongs_to_transport(&leg.standby_seal())
    }

    pub(super) fn accept_frame(
        &self,
        received: LegBoundFrame,
    ) -> Result<SessionEvent, super::leg::LegProvenanceError> {
        self.attached.accept_frame(received)
    }
}

impl fmt::Debug for OwnerTargetInitialHandoff {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OwnerTargetInitialHandoff")
            .field("generation", &self.generation())
            .field("owner_live", &self.owner.upgrade().is_some())
            .field("physical_actor", &self.physical_actor.is_some())
            .field("acceptance", &self.acceptance)
            .finish_non_exhaustive()
    }
}
/// Initial A after the owner reducer has already committed B as its exact
/// replacement.  The embedded AttachedLeg is deliberately unreachable: this
/// type can only validate and consume A's joined terminal, never construct a
/// second data-plane event or report `LegLost` for the now-current B.
#[must_use = "retired initial owner handoff must settle its exact joined terminal"]
pub(crate) struct OwnerTargetRetiredInitialHandoff {
    owner: Weak<OwnerTargetExecutorIdentity>,
    attached: AttachedLeg,
    physical_actor: PhysicalLegActorBinding,
    replacement_generation: LegGeneration,
    replacement_nonce: AttachNonce,
}

impl OwnerTargetRetiredInitialHandoff {
    pub(crate) fn retired_generation(&self) -> LegGeneration {
        self.attached.generation()
    }

    pub(crate) const fn replacement_generation(&self) -> LegGeneration {
        self.replacement_generation
    }

    pub(crate) const fn replacement_nonce(&self) -> AttachNonce {
        self.replacement_nonce
    }
}

impl fmt::Debug for OwnerTargetRetiredInitialHandoff {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OwnerTargetRetiredInitialHandoff")
            .field("retired_generation", &self.retired_generation())
            .field("replacement_generation", &self.replacement_generation)
            .field("owner_live", &self.owner.upgrade().is_some())
            .finish_non_exhaustive()
    }
}
/// Fully checked A-retirement plan.  Preparation is the last fallible step;
/// after B installation commits, converting this value to the retired type is
/// allocation-free and infallible.
#[must_use = "prepared initial retirement must be restored or committed after B install"]
pub(super) struct PreparedOwnerInitialRetirement<'queue> {
    _a_queue: &'queue mut LegOutboundQueue,
    owner: Weak<OwnerTargetExecutorIdentity>,
    attached: AttachedLeg,
    physical_actor: PhysicalLegActorBinding,
    replacement_generation: LegGeneration,
    replacement_nonce: AttachNonce,
}

impl PreparedOwnerInitialRetirement<'_> {
    pub(super) fn restore_initial(self) -> OwnerTargetInitialHandoff {
        let Self {
            _a_queue: _,
            owner,
            attached,
            physical_actor,
            replacement_generation: _,
            replacement_nonce: _,
        } = self;
        OwnerTargetInitialHandoff {
            owner,
            attached,
            acceptance: None,
            physical_actor: Some(physical_actor),
        }
    }

    pub(super) fn commit_after_b_install(self) -> OwnerTargetRetiredInitialHandoff {
        let Self {
            _a_queue: _,
            owner,
            attached,
            physical_actor,
            replacement_generation,
            replacement_nonce,
        } = self;
        OwnerTargetRetiredInitialHandoff {
            owner,
            attached,
            physical_actor,
            replacement_generation,
            replacement_nonce,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OwnerTargetInitialRetirementErrorKind {
    WrongInitialOwner,
    WrongReplacementOwner,
    AcceptanceNotSettled,
    MissingPhysicalActor,
    WrongPhysicalActor,
    ReplacementNotNewer,
    InitialOutboundQueueInvariant,
    InitialOutboundNotPristine {
        was_active: bool,
        queued_frames: usize,
        summed_frame_bytes: usize,
        stored_owned_bytes: usize,
        ordered_delivery_wait: bool,
    },
}
/// Precommit retirement rejection.  B is borrowed and remains controller
/// owned; this value returns the exact initial handoff for an unchanged retry.
#[must_use = "rejected initial retirement returns the exact current-A handoff"]
pub(super) struct OwnerTargetInitialRetirementFailure {
    kind: OwnerTargetInitialRetirementErrorKind,
    initial: OwnerTargetInitialHandoff,
}

impl OwnerTargetInitialRetirementFailure {
    pub(super) const fn kind(&self) -> OwnerTargetInitialRetirementErrorKind {
        self.kind
    }

    pub(super) fn into_initial(self) -> OwnerTargetInitialHandoff {
        self.initial
    }
}

impl fmt::Debug for OwnerTargetInitialRetirementFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OwnerTargetInitialRetirementFailure")
            .field("kind", &self.kind)
            .field("initial", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OwnerTargetRetiredInitialSettlementErrorKind {
    WrongRetiredOwner,
    WrongActiveOwner,
    WrongReplacement,
    WrongJoinedActor,
    MissingTerminal,
    WrongTerminal,
}
/// Rejected retired-A settlement returns both exact linear owners.  The outer
/// joined-actor transaction restores `terminal` before the caller regains the
/// handoff, so a mismatch is fully retryable without reconstruction.
#[must_use = "rejected retired-A settlement retains handoff and terminal ownership"]
pub(super) struct OwnerTargetRetiredInitialSettlementFailure {
    kind: OwnerTargetRetiredInitialSettlementErrorKind,
    retired: OwnerTargetRetiredInitialHandoff,
    terminal: Option<ExactLegTerminal>,
}

impl OwnerTargetRetiredInitialSettlementFailure {
    pub(super) const fn kind(&self) -> OwnerTargetRetiredInitialSettlementErrorKind {
        self.kind
    }

    pub(super) fn into_parts(self) -> (OwnerTargetRetiredInitialHandoff, Option<ExactLegTerminal>) {
        (self.retired, self.terminal)
    }
}

impl fmt::Debug for OwnerTargetRetiredInitialSettlementFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OwnerTargetRetiredInitialSettlementFailure")
            .field("kind", &self.kind)
            .field("retired", &"[REDACTED]")
            .field("terminal", &"[REDACTED]")
            .finish()
    }
}
/// Historical value-only witness emitted only after the exact retired A
/// handoff, joined runtime, and terminal were consumed together.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct OwnerTargetRetiredInitialSettled {
    retired_generation: LegGeneration,
    replacement_generation: LegGeneration,
    reason: LegTransportTerminalReason,
}

impl OwnerTargetRetiredInitialSettled {
    pub(crate) const fn retired_generation(self) -> LegGeneration {
        self.retired_generation
    }

    pub(crate) const fn replacement_generation(self) -> LegGeneration {
        self.replacement_generation
    }

    pub(crate) const fn reason(self) -> LegTransportTerminalReason {
        self.reason
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OwnerTargetInitialAcceptanceSettlementDisposition {
    SettledNow,
    AlreadySettled,
}
/// Exact initial active handoff returned after every settlement attempt. A
/// repeated call is value-only and returns `AlreadySettled`; it cannot mint a
/// second receipt or AttachedLeg authority.
#[must_use = "initial acceptance settlement returns the still-live active handoff"]
pub(crate) struct OwnerTargetInitialAcceptanceSettlement {
    disposition: OwnerTargetInitialAcceptanceSettlementDisposition,
    handoff: OwnerTargetInitialHandoff,
}

impl OwnerTargetInitialAcceptanceSettlement {
    pub(crate) const fn disposition(&self) -> OwnerTargetInitialAcceptanceSettlementDisposition {
        self.disposition
    }

    pub(crate) fn into_handoff(self) -> OwnerTargetInitialHandoff {
        self.handoff
    }
}

impl fmt::Debug for OwnerTargetInitialAcceptanceSettlement {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OwnerTargetInitialAcceptanceSettlement")
            .field("disposition", &self.disposition)
            .field("handoff", &"[REDACTED]")
            .finish()
    }
}
/// Ownership-preserving whole-bootstrap construction failure.
#[must_use = "failed initial owner construction retains bootstrap and Target ownership"]
pub(crate) struct OwnerTargetInitialBootstrapFailure<T> {
    source: OwnerTargetConfigError,
    config: OwnerTargetConfig,
    bootstrap: OwnerSessionBootstrap,
    target: T,
    source_config: FlowPortConfig,
}

impl<T> OwnerTargetInitialBootstrapFailure<T> {
    pub(crate) const fn source(&self) -> OwnerTargetConfigError {
        self.source
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        OwnerTargetConfigError,
        OwnerTargetConfig,
        OwnerSessionBootstrap,
        T,
        FlowPortConfig,
    ) {
        (
            self.source,
            self.config,
            self.bootstrap,
            self.target,
            self.source_config,
        )
    }
}

impl<T> fmt::Debug for OwnerTargetInitialBootstrapFailure<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OwnerTargetInitialBootstrapFailure")
            .field("source", &self.source)
            .field("config", &self.config)
            .field("bootstrap", &self.bootstrap)
            .field("target", &"[REDACTED]")
            .field("source_config", &self.source_config)
            .finish()
    }
}

impl<T> fmt::Display for OwnerTargetInitialBootstrapFailure<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "initial owner Target construction failed: {}",
            self.source
        )
    }
}

impl<T> std::error::Error for OwnerTargetInitialBootstrapFailure<T> {}
/// Ownership-preserving legless construction failure. The committed
/// high-water timer remains nested in its bootstrap and can be retried with a
/// corrected configuration without reconstruction or oracle state.
#[must_use = "failed legless owner construction retains bootstrap and Target ownership"]
pub(crate) struct OwnerTargetLeglessBootstrapFailure<T> {
    source: OwnerTargetConfigError,
    config: OwnerTargetConfig,
    bootstrap: LeglessOwnerSessionBootstrap,
    target: T,
    source_config: FlowPortConfig,
}

impl<T> OwnerTargetLeglessBootstrapFailure<T> {
    pub(crate) const fn source(&self) -> OwnerTargetConfigError {
        self.source
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        OwnerTargetConfigError,
        OwnerTargetConfig,
        LeglessOwnerSessionBootstrap,
        T,
        FlowPortConfig,
    ) {
        (
            self.source,
            self.config,
            self.bootstrap,
            self.target,
            self.source_config,
        )
    }
}

impl<T> fmt::Debug for OwnerTargetLeglessBootstrapFailure<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OwnerTargetLeglessBootstrapFailure")
            .field("source", &self.source)
            .field("config", &self.config)
            .field("bootstrap", &self.bootstrap)
            .field("target", &"[REDACTED]")
            .field("source_config", &self.source_config)
            .finish()
    }
}

impl<T> fmt::Display for OwnerTargetLeglessBootstrapFailure<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "legless owner Target construction failed: {}",
            self.source
        )
    }
}

impl<T> std::error::Error for OwnerTargetLeglessBootstrapFailure<T> {}
/// Precommit rejection returning the exact initial handoff unchanged.
#[must_use = "rejected initial handoff retains its AttachedLeg and acceptance receipt"]
pub(crate) struct OwnerTargetInitialHandoffOwnerFailure {
    block: OwnerTargetInputBlock,
    handoff: OwnerTargetInitialHandoff,
}

impl OwnerTargetInitialHandoffOwnerFailure {
    pub(crate) const fn block(&self) -> OwnerTargetInputBlock {
        self.block
    }

    pub(crate) fn into_handoff(self) -> OwnerTargetInitialHandoff {
        self.handoff
    }
}

impl fmt::Debug for OwnerTargetInitialHandoffOwnerFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OwnerTargetInitialHandoffOwnerFailure")
            .field("block", &self.block)
            .field("handoff", &"[REDACTED]")
            .finish()
    }
}

impl fmt::Display for OwnerTargetInitialHandoffOwnerFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "initial owner handoff rejected: {:?}",
            self.block
        )
    }
}

impl std::error::Error for OwnerTargetInitialHandoffOwnerFailure {}
/// Executor-bound recovery completion. Keeping owner identity beside the
/// installed leg prevents a different controller from installing or
/// terminalizing a capability merely because it matches the transport.
pub(crate) struct OwnerTargetRecoveryHandoff {
    owner: Weak<OwnerTargetExecutorIdentity>,
    attached: AttachedLeg,
}

impl OwnerTargetRecoveryHandoff {
    pub(crate) fn generation(&self) -> LegGeneration {
        self.attached.generation()
    }

    pub(super) fn belongs_to_transport(&self, leg: &EstablishedLeg) -> bool {
        self.attached.belongs_to_transport(&leg.standby_seal()) && leg.transport_is_open()
    }

    pub(crate) fn nonce(&self) -> crate::resumable::AttachNonce {
        self.attached.nonce()
    }

    pub(super) fn accept_frame(
        &self,
        received: LegBoundFrame,
    ) -> Result<SessionEvent, super::leg::LegProvenanceError> {
        self.attached.accept_frame(received)
    }

    #[cfg(test)]
    pub(super) fn into_attached_for_test(self) -> AttachedLeg {
        self.attached
    }
}

impl fmt::Debug for OwnerTargetRecoveryHandoff {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OwnerTargetRecoveryHandoff")
            .field("generation", &self.generation())
            .field("owner_live", &self.owner.upgrade().is_some())
            .finish_non_exhaustive()
    }
}
/// Read-only reducer observation authority. The constructor is private to
/// this module, so only the sole owner Target executor can request the
/// supervisor's post-commit application-ACK fact.
pub(super) struct OwnerTargetObservedApplyGate {
    _private: (),
}

impl OwnerTargetObservedApplyGate {
    fn mint() -> Self {
        Self { _private: () }
    }
}
/// Non-cloneable authority to execute exactly one following bounded effect
/// turn. Transport must schedule this capability once and cannot manufacture
/// a second wakeup for the same pending work.
#[must_use = "pending owner Target effects require exactly one resumed turn"]
pub(crate) struct OwnerTargetResume {
    owner: Weak<OwnerTargetExecutorIdentity>,
    id: u64,
}

impl PartialEq for OwnerTargetResume {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && Weak::ptr_eq(&self.owner, &other.owner)
    }
}

impl Eq for OwnerTargetResume {}

impl fmt::Debug for OwnerTargetResume {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OwnerTargetResume([REDACTED])")
    }
}
/// Exact owner high-water after initial acceptance loss. The resume-grace
/// expiry event stays private and is bound to one constructed Target executor;
/// callers can observe only the generation and terminal reason.
#[must_use = "legless owner high-water must be recovered or explicitly expired"]
pub(crate) struct OwnerLeglessHighWater {
    owner: Weak<OwnerTargetExecutorIdentity>,
    generation: LegGeneration,
    reason: LegTransportTerminalReason,
    resume_grace_expired: SessionEvent,
}

impl OwnerLeglessHighWater {
    pub(crate) const fn generation(&self) -> LegGeneration {
        self.generation
    }

    pub(crate) const fn reason(&self) -> LegTransportTerminalReason {
        self.reason
    }
}

impl fmt::Debug for OwnerLeglessHighWater {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OwnerLeglessHighWater")
            .field("generation", &self.generation)
            .field("reason", &self.reason)
            .field("owner_live", &self.owner.upgrade().is_some())
            .finish_non_exhaustive()
    }
}

/// Terminal-bound installed attach whose unpublished acceptance/recovery tail
/// remains owned until the same executor explicitly discards it. This keeps
/// queue loss, terminal proof, reducer loss, and recovery-tail disposal as
/// separate irreversible steps.
#[must_use = "terminal attach recovery must be explicitly discarded or quarantined"]
pub(crate) struct PendingOwnerAttachLoss {
    owner: Weak<OwnerTargetExecutorIdentity>,
    generation: LegGeneration,
    reason: LegTransportTerminalReason,
    resume_grace_expired: SessionEvent,
    loss: PendingLegLoss,
    tail: TerminalOwnerAttachTail,
    phase: OwnerRecoveryLossPhase,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OwnerRecoveryLossPhase {
    BarrierStrict,
    BarrierPendingTurn,
    BarrierCleared,
}

/// Exact precommit rejection from binding one in-flight owner recovery to a
/// transport terminal. The generic input keeps this seam reusable across the
/// four recovery typestates while guaranteeing that neither authority can be
/// lost behind an opaque executor error.
pub(super) struct OwnerRecoveryTerminalizationFailure<I> {
    block: OwnerTargetInputBlock,
    input: I,
    terminal: ExactLegTerminal,
}

impl<I> OwnerRecoveryTerminalizationFailure<I> {
    pub(super) const fn block(&self) -> OwnerTargetInputBlock {
        self.block
    }

    pub(super) fn into_parts(self) -> (I, ExactLegTerminal) {
        (self.input, self.terminal)
    }
}

pub(super) enum OwnerTerminalAttachDiscardFailure {
    Blocked {
        block: OwnerTargetInputBlock,
        pending: PendingOwnerAttachLoss,
    },
    Quarantined(QuarantinedOwnerAttachLoss),
}

pub(super) struct QuarantinedOwnerAttachLoss {
    pending: PendingOwnerAttachLoss,
    source: OwnerTerminalAttachDiscardQuarantineSource,
}

enum OwnerTerminalAttachDiscardQuarantineSource {
    Supervisor(SessionSupervisorError),
    Storage {
        source: SessionSupervisorError,
        effects: crate::resumable::effect_storage::SessionEffectBatch,
    },
    Effects {
        generation: LegGeneration,
        effects: Vec<SessionEffect>,
    },
}

impl PendingOwnerAttachLoss {
    pub(crate) const fn generation(&self) -> LegGeneration {
        self.generation
    }

    pub(crate) const fn reason(&self) -> LegTransportTerminalReason {
        self.reason
    }
}

impl fmt::Debug for PendingOwnerAttachLoss {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PendingOwnerAttachLoss")
            .field("generation", &self.generation)
            .field("reason", &self.reason)
            .field("tail", &self.tail)
            .finish_non_exhaustive()
    }
}

enum TerminalOwnerAttachTail {
    Pending {
        endpoint: Weak<LegTransportEndpoint>,
        acceptance: Frame,
        effects: Vec<SessionEffect>,
        admission: OwnerAttachAcceptanceAdmission,
    },
    Enqueued(EnqueuedOwnerTargetRecovery),
    Drain {
        acceptance: Option<AttachAcceptanceEnqueued>,
        pending: VecDeque<OwnerTargetOutput>,
        ready: Vec<OwnerTargetRecoveryOutput>,
    },
    Resume {
        acceptance: AttachAcceptanceEnqueued,
        resume: OwnerTargetResume,
    },
    HandoffReady,
}

impl TerminalOwnerAttachTail {
    fn discard(self) {
        match self {
            Self::Pending {
                endpoint,
                acceptance,
                effects,
                admission,
            } => drop((endpoint, acceptance, effects, admission)),
            Self::Enqueued(recovery) => drop(recovery),
            Self::Drain {
                acceptance,
                pending,
                ready,
            } => drop((acceptance, pending, ready)),
            Self::Resume { acceptance, resume } => drop((acceptance, resume)),
            Self::HandoffReady => {}
        }
    }
}

impl fmt::Debug for TerminalOwnerAttachTail {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pending { effects, .. } => formatter
                .debug_struct("Pending")
                .field("effects", &effects.len())
                .finish_non_exhaustive(),
            Self::Enqueued(recovery) => formatter.debug_tuple("Enqueued").field(recovery).finish(),
            Self::Drain { pending, ready, .. } => formatter
                .debug_struct("Drain")
                .field("pending", &pending.len())
                .field("ready", &ready.len())
                .finish_non_exhaustive(),
            Self::Resume { .. } => formatter.write_str("Resume([REDACTED])"),
            Self::HandoffReady => formatter.write_str("HandoffReady"),
        }
    }
}

/// Exact continuation for one bounded post-attach recovery turn. It retains
/// the acceptance queue receipt, preventing the generic owner resume path
/// from publishing later replay on a different queue.
#[must_use = "post-attach recovery continuation must resume on its acceptance queue"]
pub(crate) struct OwnerTargetRecoveryResume {
    attached: AttachedLeg,
    acceptance: AttachAcceptanceEnqueued,
    resume: OwnerTargetResume,
}

impl fmt::Debug for OwnerTargetRecoveryResume {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OwnerTargetRecoveryResume")
            .field("generation", &self.attached.generation())
            .field("acceptance", &self.acceptance)
            .field("resume", &"[REDACTED]")
            .finish()
    }
}

/// Owner attach result whose recovery work remains deliberately unpublished.
///
/// On `Installed`, transport must consume `pending` through its bounded queue
/// admission method. Only that successful transition exposes the attached leg
/// and a recovery capability, preventing replay from overtaking the serialized
/// acceptance barrier.
#[must_use = "owner attach publication must send acceptance/status and retain recovery ordering"]
#[allow(
    clippy::large_enum_variant,
    reason = "the non-cloneable installed transaction stays inline so dropping or returning the enum preserves one exact ownership path"
)]
pub(crate) enum OwnerTargetAttachPublication {
    Installed {
        pending: PendingOwnerTargetAttachPublication,
    },
    Resynchronize {
        pending: PendingOwnerAttachStatusPublication,
    },
}

impl fmt::Debug for OwnerTargetAttachPublication {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Installed { pending } => formatter
                .debug_struct("Installed")
                .field("recovery_effects", &pending.effects.len())
                .finish_non_exhaustive(),
            Self::Resynchronize { .. } => formatter
                .debug_struct("Resynchronize")
                .finish_non_exhaustive(),
        }
    }
}

/// One installed owner attach whose correlated acceptance and recovery have
/// not yet crossed the same bounded FIFO admission boundary.
#[must_use = "owner attach acceptance must be queued before recovery can be dispatched"]
pub(crate) struct PendingOwnerTargetAttachPublication {
    owner: Weak<OwnerTargetExecutorIdentity>,
    endpoint: Weak<LegTransportEndpoint>,
    attached: AttachedLeg,
    acceptance: Frame,
    effects: Vec<SessionEffect>,
    admission: OwnerAttachAcceptanceAdmission,
}

enum OwnerAttachAcceptanceAdmission {
    Reserved(AttachAcceptanceReservation),
    #[cfg(test)]
    UnreservedForTest,
}

impl PendingOwnerTargetAttachPublication {
    pub(crate) fn acceptance_encoded_len(&self) -> Result<usize, crate::resumable::ProtocolError> {
        self.acceptance.encode().map(|encoded| encoded.len())
    }

    #[allow(
        clippy::result_large_err,
        reason = "queue pressure must return the exact non-cloneable attach publication without allocation"
    )]
    pub(crate) fn enqueue_acceptance(
        self,
        queue: &mut LegOutboundQueue,
    ) -> Result<EnqueuedOwnerTargetAttach, OwnerTargetAttachEnqueueError> {
        let Self {
            owner,
            endpoint,
            attached,
            acceptance,
            effects,
            admission,
        } = self;
        match admission {
            OwnerAttachAcceptanceAdmission::Reserved(reservation) => {
                match queue.push_reserved_attach_acceptance(reservation, &attached, acceptance) {
                    Ok(acceptance) => Ok(EnqueuedOwnerTargetAttach {
                        owner,
                        attached,
                        recovery: EnqueuedOwnerTargetRecovery {
                            acceptance,
                            effects,
                        },
                    }),
                    Err(error) => {
                        let kind = error.kind().clone();
                        let (reservation, acceptance) = error.into_parts();
                        Err(OwnerTargetAttachEnqueueError {
                            kind,
                            publication: Self {
                                owner,
                                endpoint,
                                attached,
                                acceptance,
                                effects,
                                admission: OwnerAttachAcceptanceAdmission::Reserved(reservation),
                            },
                        })
                    }
                }
            }
            #[cfg(test)]
            OwnerAttachAcceptanceAdmission::UnreservedForTest => {
                match queue.push_attach_acceptance(&attached, acceptance) {
                    Ok(acceptance) => Ok(EnqueuedOwnerTargetAttach {
                        owner,
                        attached,
                        recovery: EnqueuedOwnerTargetRecovery {
                            acceptance,
                            effects,
                        },
                    }),
                    Err(error) => {
                        let kind = error.kind().clone();
                        Err(OwnerTargetAttachEnqueueError {
                            kind,
                            publication: Self {
                                owner,
                                endpoint,
                                attached,
                                acceptance: error.into_frame(),
                                effects,
                                admission: OwnerAttachAcceptanceAdmission::UnreservedForTest,
                            },
                        })
                    }
                }
            }
        }
    }
}

impl fmt::Debug for PendingOwnerTargetAttachPublication {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PendingOwnerTargetAttachPublication")
            .field("effects", &self.effects.len())
            .finish_non_exhaustive()
    }
}

/// Successful admission of the exact acceptance plus the only recovery
/// capability that may follow it.
pub(crate) struct EnqueuedOwnerTargetAttach {
    owner: Weak<OwnerTargetExecutorIdentity>,
    attached: AttachedLeg,
    recovery: EnqueuedOwnerTargetRecovery,
}

impl EnqueuedOwnerTargetAttach {
    pub(crate) fn generation(&self) -> LegGeneration {
        self.attached.generation()
    }
}

impl fmt::Debug for EnqueuedOwnerTargetAttach {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EnqueuedOwnerTargetAttach")
            .field("generation", &self.attached.generation())
            .field("acceptance", &self.recovery.acceptance)
            .field("recovery_effects", &self.recovery.effects.len())
            .finish_non_exhaustive()
    }
}

/// Non-cloneable post-acceptance owner recovery work. Construction is private
/// to a successful reserved acceptance admission on the exact outbound queue.
#[must_use = "queued owner attach recovery must be dispatched or explicitly aborted"]
pub(crate) struct EnqueuedOwnerTargetRecovery {
    acceptance: AttachAcceptanceEnqueued,
    effects: Vec<SessionEffect>,
}

impl fmt::Debug for EnqueuedOwnerTargetRecovery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EnqueuedOwnerTargetRecovery")
            .field("acceptance", &self.acceptance)
            .field("effects", &self.effects.len())
            .finish()
    }
}

/// Exact post-reducer output cursor retained when the acceptance queue cannot
/// admit every recovery transmit. It owns the failed frame, all later work,
/// and any non-wire outputs already observed during the same effect turn.
#[must_use = "blocked owner recovery drain must retry on its exact acceptance queue"]
pub(crate) struct OwnerTargetRecoveryDrain {
    owner: Weak<OwnerTargetExecutorIdentity>,
    attached: Option<AttachedLeg>,
    acceptance: Option<AttachAcceptanceEnqueued>,
    pending: VecDeque<OwnerTargetOutput>,
    ready: Vec<OwnerTargetRecoveryOutput>,
}

/// The scheduling-facing name for an already-executed FIFO publication turn.
/// `OwnerTargetRecoveryDrain` remains the legacy adapter/error name.
pub(crate) type OwnerTargetRecoveryPublishCursor = OwnerTargetRecoveryDrain;

impl fmt::Debug for OwnerTargetRecoveryDrain {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OwnerTargetRecoveryDrain")
            .field(
                "generation",
                &self.attached.as_ref().map(AttachedLeg::generation),
            )
            .field("acceptance", &self.acceptance)
            .field("pending_outputs", &self.pending.len())
            .field("ready_outputs", &self.ready.len())
            .finish()
    }
}

impl OwnerTargetRecoveryDrain {
    /// Exact output values still owned by this unpublished recovery cursor.
    /// Wire frames remain in `pending`; already-classified lifecycle values
    /// remain in `ready` until the whole page commits.
    pub(super) fn retained_output_count(&self) -> usize {
        self.pending.len() + self.ready.len()
    }
}

/// One fully executed post-attach recovery page. Reducer observations are
/// returned before the caller mutates the acceptance FIFO; the cursor owns
/// every page output until that exact queue publishes it.
#[must_use = "observed owner recovery must be consumed before its page is published"]
pub(crate) struct OwnerTargetObservedRecoveryPage {
    publish: OwnerTargetRecoveryDrain,
    observations: OwnerTargetReducerObservations,
}

impl OwnerTargetObservedRecoveryPage {
    pub(crate) fn into_progress_parts(
        self,
    ) -> (
        OwnerTargetRecoveryPublishCursor,
        OwnerTargetReducerObservations,
    ) {
        (self.publish, self.observations)
    }
}

impl fmt::Debug for OwnerTargetObservedRecoveryPage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OwnerTargetObservedRecoveryPage")
            .field("publish", &self.publish)
            .field("observations_empty", &self.observations.is_empty())
            .finish()
    }
}

struct OwnerTargetRecoveryGuard {
    owner: Weak<OwnerTargetExecutorIdentity>,
    attached: AttachedLeg,
    acceptance: AttachAcceptanceEnqueued,
}

impl OwnerTargetRecoveryGuard {
    fn into_publish_cursor(self, outputs: Vec<OwnerTargetOutput>) -> OwnerTargetRecoveryDrain {
        OwnerTargetRecoveryDrain {
            owner: self.owner,
            attached: Some(self.attached),
            acceptance: Some(self.acceptance),
            pending: outputs.into(),
            ready: Vec::new(),
        }
    }
}

/// Queue-pressure rejection that returns the exact attach publication. The
/// acceptance frame and recovery effects remain owned for a later retry.
pub(crate) struct OwnerTargetAttachEnqueueError {
    kind: LegOutboundQueueErrorKind,
    publication: PendingOwnerTargetAttachPublication,
}

impl OwnerTargetAttachEnqueueError {
    pub(crate) const fn kind(&self) -> &LegOutboundQueueErrorKind {
        &self.kind
    }

    pub(crate) fn into_publication(self) -> PendingOwnerTargetAttachPublication {
        self.publication
    }
}

impl fmt::Debug for OwnerTargetAttachEnqueueError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OwnerTargetAttachEnqueueError")
            .field("kind", &self.kind)
            .field("publication", &"[REDACTED]")
            .finish()
    }
}

impl fmt::Display for OwnerTargetAttachEnqueueError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "owner attach acceptance queue admission failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for OwnerTargetAttachEnqueueError {}

/// Proof-authenticated generation status awaiting the exact status leg's
/// dedicated ordered queue admission. The optional catch-up arm remains
/// private and unusable until actual delivery is observed.
#[must_use = "owner generation status must cross its exact ordered queue"]
pub(crate) struct PendingOwnerAttachStatusPublication {
    owner: Weak<OwnerTargetExecutorIdentity>,
    seal: LegSeal,
    endpoint: Weak<LegTransportEndpoint>,
    status: Frame,
    arm: Option<OwnerCatchUpArm>,
}

impl PendingOwnerAttachStatusPublication {
    #[allow(
        clippy::result_large_err,
        reason = "status queue pressure returns the exact publication and arm authority"
    )]
    pub(crate) fn enqueue(
        self,
        queue: &mut LegOutboundQueue,
    ) -> Result<EnqueuedOwnerAttachStatus, OwnerAttachStatusEnqueueError> {
        let Self {
            owner,
            seal,
            endpoint,
            status,
            arm,
        } = self;
        match queue.push_authenticated_attach_status(&seal, status) {
            Ok(receipt) => Ok(EnqueuedOwnerAttachStatus {
                owner,
                endpoint,
                receipt,
                arm,
            }),
            Err(error) => {
                let kind = error.kind().clone();
                Err(OwnerAttachStatusEnqueueError {
                    kind,
                    publication: Self {
                        owner,
                        seal,
                        endpoint,
                        status: error.into_frame(),
                        arm,
                    },
                })
            }
        }
    }

    #[cfg(test)]
    fn status_for_test(&self) -> &Frame {
        &self.status
    }
}

impl fmt::Debug for PendingOwnerAttachStatusPublication {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PendingOwnerAttachStatusPublication")
            .field("endpoint_open", &transport_endpoint_is_open(&self.endpoint))
            .field("catch_up", &self.arm.is_some())
            .finish_non_exhaustive()
    }
}

/// Exact status queue receipt retaining the optional one-shot permit arm.
pub(crate) struct EnqueuedOwnerAttachStatus {
    owner: Weak<OwnerTargetExecutorIdentity>,
    endpoint: Weak<LegTransportEndpoint>,
    receipt: AttachStatusEnqueued,
    arm: Option<OwnerCatchUpArm>,
}

impl fmt::Debug for EnqueuedOwnerAttachStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EnqueuedOwnerAttachStatus")
            .field("receipt", &self.receipt)
            .field("catch_up", &self.arm.is_some())
            .finish()
    }
}

pub(crate) struct OwnerAttachStatusEnqueueError {
    kind: LegOutboundQueueErrorKind,
    publication: PendingOwnerAttachStatusPublication,
}

impl OwnerAttachStatusEnqueueError {
    pub(crate) const fn kind(&self) -> &LegOutboundQueueErrorKind {
        &self.kind
    }

    pub(crate) fn into_publication(self) -> PendingOwnerAttachStatusPublication {
        self.publication
    }
}

impl fmt::Debug for OwnerAttachStatusEnqueueError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OwnerAttachStatusEnqueueError")
            .field("kind", &self.kind)
            .field("publication", &"[REDACTED]")
            .finish()
    }
}

impl fmt::Display for OwnerAttachStatusEnqueueError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "owner status queue admission failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for OwnerAttachStatusEnqueueError {}

/// Fail-closed quarantine for exact work after an owner execution invariant.
/// Co-owns effects, outputs, observations and at most one recovery cursor.
#[derive(Default)]
pub(crate) struct AbortedOwnerTargetEffects {
    effects: VecDeque<OwnerTargetWork>,
    outputs: Vec<OwnerTargetOutput>,
    observations: Option<OwnerTargetReducerObservations>,
    recovery: Option<OwnerTargetRecoveryDrain>,
    incoming: Option<effect_storage::OwnerTargetIncomingReduction>,
    source: Option<TargetReadSource>,
    current: Option<OwnerTargetWork>,
    source_releasing: bool,
    open_outcome: Option<TargetOpenCompletion>,
}

impl fmt::Debug for AbortedOwnerTargetEffects {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AbortedOwnerTargetEffects")
            .field("effects", &self.effects.len())
            .field("outputs", &self.outputs.len())
            .field("observations", &usize::from(self.observations.is_some()))
            .field("recovery", &usize::from(self.recovery.is_some()))
            .field("incoming", &usize::from(self.incoming.is_some()))
            .field("source", &usize::from(self.source.is_some()))
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for OwnerTargetOutput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transmit(frame) => formatter
                .debug_struct("Transmit")
                .field("generation", &frame.leg_generation())
                .finish_non_exhaustive(),
            Self::LegActivated { generation } => formatter
                .debug_struct("LegActivated")
                .field("generation", generation)
                .finish(),
            Self::ResumeGraceStarted { generation } => formatter
                .debug_struct("ResumeGraceStarted")
                .field("generation", generation)
                .finish(),
            Self::TerminalGraceStarted { terminal } => formatter
                .debug_struct("TerminalGraceStarted")
                .field("terminal", terminal)
                .finish(),
            Self::SessionExpired => formatter.write_str("SessionExpired"),
            Self::NeedsResume { .. } => formatter.write_str("NeedsResume([REDACTED])"),
        }
    }
}

impl fmt::Debug for OwnerTargetRecoveryOutput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LegActivated { generation } => formatter
                .debug_struct("LegActivated")
                .field("generation", generation)
                .finish(),
            Self::ResumeGraceStarted { generation } => formatter
                .debug_struct("ResumeGraceStarted")
                .field("generation", generation)
                .finish(),
            Self::TerminalGraceStarted { terminal } => formatter
                .debug_struct("TerminalGraceStarted")
                .field("terminal", terminal)
                .finish(),
            Self::SessionExpired => formatter.write_str("SessionExpired"),
            Self::NeedsResume { .. } => formatter.write_str("NeedsResume([REDACTED])"),
            Self::RecoveryCompleted { handoff } => formatter
                .debug_struct("RecoveryCompleted")
                .field("generation", &handoff.generation())
                .finish(),
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct OwnerTargetSnapshot {
    pub(crate) session: SessionSupervisorSnapshot,
    pub(crate) target_flows: usize,
    pub(crate) target_tombstones: usize,
    pub(crate) pending_writes: usize,
    pub(crate) pending_joins: usize,
    pub(crate) target_read_owned_bytes: usize,
    pub(crate) target_read_owned_segments: usize,
    pub(crate) pending_effects: usize,
    pub(crate) needs_resume: bool,
    pub(crate) pending_target_completions: usize,
    pub(crate) aborted_effect_guards: usize,
    pub(crate) aborted_effects: usize,
    pub(crate) aborted_outputs: usize,
    pub(crate) aborted_observations: usize,
    pub(crate) aborted_ordinary_outputs: usize,
    pub(crate) aborted_ordinary_observations: usize,
    pub(crate) aborted_recovery_outputs: usize,
    pub(crate) aborted_recovery_observations: usize,
    pub(crate) aborted_recovery_guards: usize,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum OwnerReadyInstallError {
    AlreadyBound,
    NotBound,
    Handoff(OwnerTargetInputBlock),
    Source(super::supervisor::OwnerSourceIdentityError),
    Reservation(super::owner_ready::OwnerReadyReservationError),
    Mailbox(commands::OwnerMailboxPreparationError),
    Layout,
    Reserve,
    FlowState(SessionFlowId),
}
struct PendingTargetWrite {
    offer: SinkOffer,
    segments: Vec<TcpDataSegment>,
}
enum PendingTargetSink {
    Write(PendingTargetWrite),
    HalfClose(crate::resumable::SinkHalfClose),
}
struct TargetFlowOwner {
    target_present: bool,
    source: Option<TargetReadSource>,
    pending_sink: Option<PendingTargetSink>,
    terminal: Option<TargetTerminalKind>,
    join_pending: bool,
    retire_after_join: bool,
    pending_completion: Option<PendingTargetCompletion>,
    service_position: Option<usize>,
}
enum PendingTargetCompletion {
    Failed {
        work: OwnerTargetWork,
        cause: super::target::TargetFailure,
        feedback: completion::TargetFailureFeedback,
    },
    StagedReset(crate::resumable::LocalFlow),
    TerminalSettling(completion::TerminalFlowSettlement),
    Open(completion::OpenFlowSettlement),
    DriverReturned(completion::DriverCompletionState),
    Failure {
        event: SessionEvent,
    },
    HalfClose {
        event: SessionEvent,
    },
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PendingTargetCompletionKind {
    Open,
    Failure,
    HalfClose,
}
struct TargetReadSource {
    flow: ResumableTcpFlow,
    driver: ResumableTcpDriver,
}
enum TargetJoinAttempt {
    Joined,
    Pending(TargetJoinBlock),
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TargetJoinBlock {
    OwnedState,
    TerminalCapacity,
}
enum OwnerTargetWork {
    Effect(SessionEffect),
    ExpireNextTarget,
    TargetIoInvariant {
        flow_id: SessionFlowId,
        operation: TargetIoOperation,
    },
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TargetIoOperation {
    Read,
    ReadCompletion,
    WriteCompletion,
    OpenCompletion,
    HalfCloseCompletion,
    FailureCompletion,
    Join,
    TerminalRetirement,
}
/// Sole Target-effect executor for one owner-side session supervisor.
pub(crate) struct OwnerTargetExecutor<T> {
    identity: Arc<OwnerTargetExecutorIdentity>,
    config: OwnerTargetConfig,
    supervisor: SessionSupervisor,
    target: T,
    source_factory: ResumableTcpPortFactory,
    flows: BTreeMap<SessionFlowId, TargetFlowOwner>,
    target_tombstones: BTreeMap<SessionFlowId, TargetTerminalKind>,
    pending_effects: VecDeque<OwnerTargetWork>,
    prepared_outputs: Vec<OwnerTargetOutput>,
    outstanding_resume: Option<u64>,
    next_resume_id: Option<u64>,
    max_recovery_effects: usize,
    aborted_effects: Option<AbortedOwnerTargetEffects>,
    attach_barrier_pending: bool,
    service: service::OwnerTargetServiceState,
}
struct OwnerTargetConstructionFailure<T> {
    source: OwnerTargetConfigError,
    supervisor: SessionSupervisor,
    target: T,
}
impl<T> OwnerTargetConstructionFailure<T> {
    fn into_parts(self) -> (OwnerTargetConfigError, SessionSupervisor, T) {
        (self.source, self.supervisor, self.target)
    }
}
impl<T: TargetIo> OwnerTargetExecutor<T> {
    pub(crate) fn snapshot(&self) -> OwnerTargetSnapshot {
        let aborted_is_recovery = self
            .aborted_effects
            .as_ref()
            .is_some_and(|aborted| aborted.recovery.is_some());
        let aborted_outputs = self
            .aborted_effects
            .as_ref()
            .map_or(0, |aborted| aborted.outputs.len());
        let (aborted_effects, aborted_observations, aborted_observation_facts) = self
            .aborted_effects
            .as_ref()
            .map_or((0, 0, 0), AbortedOwnerTargetEffects::retained_inventory);
        let aborted_recovery_cursor_outputs = self
            .aborted_effects
            .as_ref()
            .and_then(|aborted| aborted.recovery.as_ref())
            .map_or(0, OwnerTargetRecoveryDrain::retained_output_count);
        OwnerTargetSnapshot {
            session: self.supervisor.snapshot(),
            target_flows: self.flows.len(),
            target_tombstones: self.target_tombstones.len(),
            pending_writes: self
                .flows
                .values()
                .filter(|flow| matches!(flow.pending_sink, Some(PendingTargetSink::Write(_))))
                .count(),
            pending_joins: self.flows.values().filter(|flow| flow.join_pending).count(),
            target_read_owned_bytes: self.source_factory.session_uplink_owned_bytes(),
            target_read_owned_segments: self.source_factory.session_uplink_owned_segments(),
            pending_effects: self.pending_effects.len(),
            needs_resume: self.outstanding_resume.is_some(),
            pending_target_completions: self
                .flows
                .values()
                .filter_map(|flow| flow.pending_completion.as_ref())
                .filter(|completion| completion.needs_feedback())
                .count(),
            aborted_effect_guards: usize::from(self.aborted_effects.is_some()),
            aborted_effects,
            aborted_outputs,
            aborted_observations,
            aborted_ordinary_outputs: if aborted_is_recovery {
                0
            } else {
                aborted_outputs
            },
            aborted_ordinary_observations: if aborted_is_recovery {
                0
            } else {
                aborted_observation_facts
            },
            aborted_recovery_outputs: if aborted_is_recovery {
                aborted_outputs + aborted_recovery_cursor_outputs
            } else {
                0
            },
            aborted_recovery_observations: if aborted_is_recovery {
                aborted_observation_facts
            } else {
                0
            },
            aborted_recovery_guards: self
                .aborted_effects
                .as_ref()
                .map_or(0, |aborted| usize::from(aborted.recovery.is_some())),
        }
    }

    pub(crate) const fn config(&self) -> OwnerTargetConfig {
        self.config
    }
    /// Exact reducer configuration already installed behind this executor.
    /// Capacity-sensitive composition may compare it with an injected storage
    /// plan, but it cannot acquire or mutate the supervisor itself.
    pub(super) fn installed_session_config(&self) -> crate::resumable::SessionConfig {
        self.supervisor.session_config()
    }
    /// Exact replay-storage provenance carried from the atomic initial owner
    /// bootstrap. Legacy reducer fixtures report `None` and cannot enter a
    /// checked physical owner controller.
    pub(super) fn installed_replay_storage_plan(
        &self,
    ) -> Option<crate::resumable::ReplayStoragePlan> {
        self.supervisor.installed_replay_storage_plan()
    }

    /// Immutable queue/ledger layout installed by the sole source factory.
    /// This is capacity provenance, not factory identity; ownership identity
    /// remains carried by the whole executor and its private origin token.
    pub(super) fn source_factory_capacity_witness(&self) -> TcpPortFactoryCapacityWitness {
        self.source_factory.capacity_witness()
    }

    /// Fixed recovery vector bound retained by the executor at construction.
    /// Exposing the scalar lets the controller reject internal plan drift
    /// without exposing executor queues or the supervisor.
    pub(super) const fn installed_recovery_effect_bound(&self) -> usize {
        self.max_recovery_effects
    }

    /// Exact reducer flow bound used by the production owner policy
    /// controller to derive its two-directions-per-flow progress capacity.
    pub(super) fn owner_progress_max_flows(&self) -> usize {
        self.supervisor.session_config().max_flows()
    }

    pub(crate) fn target(&self) -> &T {
        &self.target
    }

    /// Returns the sole fixed-size observation retained when reducer commit
    /// succeeded but later Target/effect execution failed. The executor stays
    /// fail-closed with its exact aborted work and outputs after this value is
    /// taken; no command can create a second retained observation.
    pub(crate) fn take_retained_observations_after_failure(
        &mut self,
    ) -> Option<OwnerTargetReducerObservations> {
        self.aborted_effects.as_mut()?.observations.take()
    }

    /// Test-only instrumentation for deterministic Target adapters. Production
    /// controllers must drive all Target work through barrier-aware executor
    /// entry points and cannot acquire raw mutable I/O authority.
    #[cfg(test)]
    pub(crate) fn target_mut(&mut self) -> &mut T {
        &mut self.target
    }

    #[cfg(test)]
    pub(super) fn exhaust_effect_resume_ids_for_test(&mut self) {
        self.next_resume_id = None;
    }

    /// Test-only bridge to the supervisor's exact post-model receipt
    /// correlation failpoint. Production callers cannot mutate replay
    /// ownership behind reducer keys.
    #[cfg(test)]
    pub(crate) fn swap_uplink_replay_ownership_for_test(
        &mut self,
        first_flow_id: SessionFlowId,
        second_flow_id: SessionFlowId,
        direction: Direction,
    ) -> bool {
        self.supervisor.swap_uplink_replay_ownership_for_test(
            first_flow_id,
            second_flow_id,
            direction,
        )
    }

    pub(crate) fn apply_command(
        &mut self,
        command: SessionOwnerCommand,
    ) -> Result<Vec<OwnerTargetOutput>, OwnerTargetError> {
        if let Some(block) = self.input_admission_block(command_flow_id(&command)) {
            return Err(OwnerTargetError::RejectedCommand {
                block,
                command: Box::new(command),
            });
        }
        if let Err(source) = self.prepare_effect_entry(3) {
            return Err(OwnerTargetError::RejectedCommand {
                block: OwnerTargetInputBlock::EffectStorage(source),
                command: Box::new(command),
            });
        }
        let effects = match self.supervisor.apply_command(command) {
            Ok(effects) => effects,
            Err(SessionSupervisorError::RejectedCommand { block, command }) => {
                return Err(OwnerTargetError::RejectedCommand {
                    block: OwnerTargetInputBlock::ReducerAdmission(block),
                    command,
                });
            }
            Err(error) => return Err(OwnerTargetError::Supervisor(error)),
        };
        self.execute_effects(effects)
    }

    pub(crate) fn apply_event(
        &mut self,
        event: SessionEvent,
    ) -> Result<Vec<OwnerTargetOutput>, OwnerTargetError> {
        self.apply_event_observed(event)
            .map(OwnerTargetObservedApply::into_outputs)
    }

    /// Consumes the one high-water timer only on the executor that was built
    /// from its legless bootstrap and only while that generation remains the
    /// current legless high-water. Recovered or wrong-owner sessions return
    /// the exact token unchanged.
    pub(crate) fn expire_legless_high_water(
        &mut self,
        high_water: OwnerLeglessHighWater,
    ) -> Result<Vec<OwnerTargetOutput>, OwnerTargetError> {
        let snapshot = self.supervisor.snapshot().session;
        let exact_owner = high_water
            .owner
            .upgrade()
            .is_some_and(|owner| Arc::ptr_eq(&owner, &self.identity));
        if !exact_owner
            || snapshot.phase() != crate::resumable::SessionPhase::Legless
            || snapshot.generation() != high_water.generation
            || self.input_admission_block(None).is_some()
        {
            return Err(OwnerTargetError::RejectedLeglessHighWater {
                high_water: Box::new(high_water),
            });
        }
        self.apply_event(high_water.resume_grace_expired)
    }

    /// Delegates one exact standby registration to the supervisor-owned
    /// authority only after all input and attach-ordering barriers are clear.
    /// A transient executor block returns the original classified wire fact
    /// intact so the controller can retry it without decoding or rebinding.
    pub(crate) fn authenticate_owner_standby(
        &self,
        active: &AttachedLeg,
        candidate: &EstablishedLeg,
        inbound: BoundInbound,
    ) -> Result<ExactAuthenticatedStandby, OwnerTargetError> {
        if let Some(block) = self.input_admission_block(None) {
            return Err(OwnerTargetError::RejectedOwnerStandbyAuthentication {
                block,
                inbound: Box::new(inbound),
            });
        }
        self.supervisor
            .authenticate_owner_standby(active, candidate, inbound)
            .map_err(OwnerTargetError::Supervisor)
    }

    /// Owner-physical-root entry for authenticating B without exposing the
    /// initial active `AttachedLeg`.  The complete executor-bound handoff must
    /// already represent a settled, still-live physical A; every precommit
    /// rejection returns the exact classified B registration unchanged.
    pub(super) fn authenticate_owner_standby_from_initial(
        &self,
        active: &OwnerTargetInitialHandoff,
        candidate: &EstablishedLeg,
        inbound: BoundInbound,
    ) -> Result<ExactAuthenticatedStandby, OwnerTargetError> {
        if let Some(block) = self.initial_standby_authentication_block(active) {
            return Err(OwnerTargetError::RejectedOwnerStandbyAuthentication {
                block,
                inbound: Box::new(inbound),
            });
        }
        self.authenticate_owner_standby(&active.attached, candidate, inbound)
    }

    /// Borrow-only preflight used before the owner actor moves the exact B
    /// registration envelope out of its inbound slot.
    pub(super) fn initial_standby_authentication_block(
        &self,
        active: &OwnerTargetInitialHandoff,
    ) -> Option<OwnerTargetInputBlock> {
        self.recovery_owner_block(&active.owner)
            .or_else(|| {
                active
                    .acceptance
                    .is_some()
                    .then_some(OwnerTargetInputBlock::InitialAcceptanceSettlementPending)
            })
            .or_else(|| {
                (!active
                    .physical_actor
                    .as_ref()
                    .is_some_and(PhysicalLegActorBinding::actor_is_live))
                .then_some(OwnerTargetInputBlock::InitialPhysicalActorNotBound)
            })
            .or_else(|| self.input_admission_block(None))
    }

    /// Authenticates and installs one exact owner attach without publishing
    /// recovery before its acceptance. The installed result keeps acceptance,
    /// attached-leg authority, and recovery indivisible until the acceptance
    /// enters a bounded [`LegOutboundQueue`].
    #[cfg(test)]
    pub(crate) fn accept_owner_attach_for_test(
        &mut self,
        leg: &EstablishedLeg,
        received: LegBoundFrame,
    ) -> Result<OwnerTargetAttachPublication, OwnerTargetError> {
        if let Some(block) = self.input_admission_block(None) {
            return Err(OwnerTargetError::RejectedOwnerAttach {
                block,
                received: Box::new(received),
            });
        }
        match self
            .supervisor
            .accept_owner_attach_for_test(leg, received)?
        {
            OwnerAttachPublication::Installed {
                attached,
                acceptance,
                recovery,
            } => {
                self.attach_barrier_pending = true;
                Ok(OwnerTargetAttachPublication::Installed {
                    pending: PendingOwnerTargetAttachPublication {
                        owner: Arc::downgrade(&self.identity),
                        endpoint: leg.endpoint_liveness(),
                        attached,
                        acceptance,
                        effects: recovery,
                        admission: OwnerAttachAcceptanceAdmission::UnreservedForTest,
                    },
                })
            }
            OwnerAttachPublication::Resynchronize { status } => {
                Ok(OwnerTargetAttachPublication::Resynchronize {
                    pending: self.pending_owner_attach_status(leg, status, None),
                })
            }
        }
    }

    /// Synchronously joins the exact registered-B slot gate, endpoint terminal
    /// transition, supervisor CAS/model install, slot consumption, and owner
    /// acceptance barrier. No await or external callback exists inside the
    /// terminal critical section.
    pub(crate) fn accept_registered_owner_attach(
        &mut self,
        slot: &mut OwnerStandbySlot,
        leg: &EstablishedLeg,
        received: LegBoundFrame,
        queue: &mut LegOutboundQueue,
    ) -> Result<OwnerTargetAttachPublication, OwnerTargetError> {
        if let Some(block) = self.input_admission_block(None) {
            return Err(OwnerTargetError::RejectedOwnerAttach {
                block,
                received: Box::new(received),
            });
        }
        let gate = match slot.begin_exact_next_attach(leg, &received, queue) {
            Ok(gate) => gate,
            Err(source) => {
                return Err(OwnerTargetError::RejectedRegisteredOwnerAttach {
                    source,
                    received: Box::new(received),
                });
            }
        };
        let _terminal_transition = match leg.lock_terminal_transition() {
            Ok(transition) => transition,
            Err(source) => {
                if gate.restore(slot, queue).is_err() {
                    return Err(OwnerTargetError::RegisteredOwnerAttachReservationInvariant);
                }
                return Err(OwnerTargetError::RegisteredOwnerAttachTerminalTransition {
                    source,
                    received: Box::new(received),
                });
            }
        };
        if !leg.transport_is_open() {
            if gate.restore(slot, queue).is_err() {
                return Err(OwnerTargetError::RegisteredOwnerAttachReservationInvariant);
            }
            return Err(OwnerTargetError::RejectedRegisteredOwnerAttach {
                source: OwnerRegisteredAttachGateError::Rejected,
                received: Box::new(received),
            });
        }
        let publication = match self
            .supervisor
            .accept_registered_owner_attach(&gate, leg, received)
        {
            Ok(publication) => publication,
            Err(error) => {
                if let Err(source) = gate.restore(slot, queue) {
                    return Err(OwnerTargetError::RegisteredOwnerAttachRestoreFailure(
                        Box::new(effect_storage::OwnerRegisteredRestoreFailure {
                            source,
                            error: Box::new(error),
                        }),
                    ));
                }
                return Err(OwnerTargetError::Supervisor(error));
            }
        };
        match publication {
            OwnerAttachPublication::Installed {
                attached,
                acceptance,
                recovery,
            } => {
                let reservation = gate.consume_after_install(&attached);
                self.attach_barrier_pending = true;
                Ok(OwnerTargetAttachPublication::Installed {
                    pending: PendingOwnerTargetAttachPublication {
                        owner: Arc::downgrade(&self.identity),
                        endpoint: leg.endpoint_liveness(),
                        attached,
                        acceptance,
                        effects: recovery,
                        admission: OwnerAttachAcceptanceAdmission::Reserved(reservation),
                    },
                })
            }
            OwnerAttachPublication::Resynchronize { status } => {
                if gate.restore(slot, queue).is_err() {
                    return Err(OwnerTargetError::RegisteredOwnerAttachReservationInvariant);
                }
                Ok(OwnerTargetAttachPublication::Resynchronize {
                    pending: self.pending_owner_attach_status(leg, status, None),
                })
            }
        }
    }

    /// Authenticates stale/exhausted ATTACH solely for a correlated status and
    /// bounded catch-up permit. Exact-next input is rejected by the authority
    /// without a generation CAS or reducer transition.
    pub(crate) fn resynchronize_owner_attach(
        &mut self,
        leg: &EstablishedLeg,
        received: LegBoundFrame,
    ) -> Result<PendingOwnerAttachStatusPublication, OwnerTargetError> {
        if let Some(block) = self.input_admission_block(None) {
            return Err(OwnerTargetError::RejectedOwnerAttach {
                block,
                received: Box::new(received),
            });
        }
        let _terminal_transition = match leg.lock_terminal_transition() {
            Ok(transition) => transition,
            Err(source) => {
                return Err(OwnerTargetError::OwnerAttachTerminalTransition {
                    source,
                    received: Box::new(received),
                });
            }
        };
        if !leg.transport_is_open() {
            return Err(OwnerTargetError::OwnerAttachEndpointLost {
                received: Box::new(received),
            });
        }
        let authenticated = self
            .supervisor
            .resynchronize_owner_attach(leg, received)
            .map_err(OwnerTargetError::Supervisor)?;
        let (status, arm) = authenticated.into_parts();
        Ok(self.pending_owner_attach_status(leg, status, arm))
    }

    /// Commits only the exact successor authorized by the supervisor's one
    /// bounded stale-request catch-up permit.
    pub(crate) fn accept_catch_up_owner_attach(
        &mut self,
        leg: &EstablishedLeg,
        received: LegBoundFrame,
        queue: &mut LegOutboundQueue,
    ) -> Result<OwnerTargetAttachPublication, OwnerTargetError> {
        if let Some(block) = self.input_admission_block(None) {
            return Err(OwnerTargetError::RejectedOwnerAttach {
                block,
                received: Box::new(received),
            });
        }
        if let Err(source) = self
            .supervisor
            .preflight_catch_up_owner_attach(leg, &received)
        {
            return Err(OwnerTargetError::RejectedCatchUpOwnerAttach {
                source,
                received: Box::new(received),
            });
        }
        let reservation = match queue.reserve_attach_acceptance(leg) {
            Ok(reservation) => reservation,
            Err(source) => {
                return Err(OwnerTargetError::RejectedOwnerAttachQueueReservation {
                    source,
                    received: Box::new(received),
                });
            }
        };
        let _terminal_transition = match leg.lock_terminal_transition() {
            Ok(transition) => transition,
            Err(source) => {
                if reservation.release(queue).is_err() {
                    return Err(OwnerTargetError::OwnerAttachReservationInvariant);
                }
                return Err(OwnerTargetError::OwnerAttachTerminalTransition {
                    source,
                    received: Box::new(received),
                });
            }
        };
        if !leg.transport_is_open() {
            if reservation.release(queue).is_err() {
                return Err(OwnerTargetError::OwnerAttachReservationInvariant);
            }
            return Err(OwnerTargetError::OwnerAttachEndpointLost {
                received: Box::new(received),
            });
        }
        let publication = match self.supervisor.accept_catch_up_owner_attach(leg, received) {
            Ok(publication) => publication,
            Err(error) => {
                if let Err(reservation) = reservation.release(queue) {
                    return Err(OwnerTargetError::CatchUpOwnerAttachReleaseFailure(
                        Box::new(effect_storage::OwnerCatchUpReleaseFailure {
                            error: Box::new(error),
                            reservation: Box::new(reservation),
                        }),
                    ));
                }
                return Err(OwnerTargetError::Supervisor(error));
            }
        };
        match publication {
            OwnerAttachPublication::Installed {
                attached,
                acceptance,
                recovery,
            } => {
                self.attach_barrier_pending = true;
                Ok(OwnerTargetAttachPublication::Installed {
                    pending: PendingOwnerTargetAttachPublication {
                        owner: Arc::downgrade(&self.identity),
                        endpoint: leg.endpoint_liveness(),
                        attached,
                        acceptance,
                        effects: recovery,
                        admission: OwnerAttachAcceptanceAdmission::Reserved(reservation),
                    },
                })
            }
            OwnerAttachPublication::Resynchronize { status } => {
                if reservation.release(queue).is_err() {
                    return Err(OwnerTargetError::OwnerAttachReservationInvariant);
                }
                Ok(OwnerTargetAttachPublication::Resynchronize {
                    pending: self.pending_owner_attach_status(leg, status, None),
                })
            }
        }
    }

    /// Consumes a status publication only after its exact queue reports actual
    /// ordered delivery. Wrong-owner, wrong-queue, and pre-delivery attempts
    /// return the complete non-cloneable publication unchanged.
    pub(crate) fn arm_owner_catch_up_after_status_delivery(
        &mut self,
        publication: EnqueuedOwnerAttachStatus,
        queue: &LegOutboundQueue,
    ) -> Result<(), OwnerTargetError> {
        if !publication
            .owner
            .upgrade()
            .is_some_and(|owner| Arc::ptr_eq(&owner, &self.identity))
        {
            return Err(OwnerTargetError::RejectedOwnerAttachStatusArm {
                block: OwnerTargetInputBlock::WrongOwnerTargetExecutor,
                publication: Box::new(publication),
            });
        }
        if !publication.receipt.belongs_to_queue(queue) {
            return Err(OwnerTargetError::RejectedOwnerAttachStatusArm {
                block: OwnerTargetInputBlock::WrongStatusQueue,
                publication: Box::new(publication),
            });
        }
        if !publication.receipt.was_delivered() {
            return Err(OwnerTargetError::RejectedOwnerAttachStatusArm {
                block: OwnerTargetInputBlock::StatusNotDelivered,
                publication: Box::new(publication),
            });
        }
        if let Some(arm) = publication.arm {
            self.supervisor.arm_owner_catch_up(arm)?;
        }
        Ok(())
    }

    /// Releases only an unarmed permit whose not-yet-enqueued exact status
    /// endpoint or already-claimed sole queue is provably gone. A live held
    /// publication is returned intact.
    pub(crate) fn abandon_lost_pending_owner_attach_status(
        &mut self,
        publication: PendingOwnerAttachStatusPublication,
    ) -> Result<(), OwnerTargetError> {
        if !publication
            .owner
            .upgrade()
            .is_some_and(|owner| Arc::ptr_eq(&owner, &self.identity))
        {
            return Err(OwnerTargetError::RejectedPendingOwnerAttachStatusAbandon {
                block: OwnerTargetInputBlock::WrongOwnerTargetExecutor,
                publication: Box::new(publication),
            });
        }
        let endpoint_lost = !transport_endpoint_is_open(&publication.endpoint);
        let queue_lost = publication.seal.outbound_queue_was_lost();
        if !endpoint_lost && !queue_lost {
            return Err(OwnerTargetError::RejectedPendingOwnerAttachStatusAbandon {
                block: OwnerTargetInputBlock::StatusPublicationStillLive,
                publication: Box::new(publication),
            });
        }
        if let Some(arm) = publication.arm.as_ref()
            && let Err(source) = self.supervisor.abandon_unarmed_owner_catch_up(arm)
        {
            return Err(OwnerTargetError::PendingOwnerAttachStatusAbandonRejected {
                source,
                publication: Box::new(publication),
            });
        }
        Ok(())
    }

    /// Releases only an unarmed permit whose exact admitted status queue or
    /// endpoint is gone before the actual-delivery arm is consumed.
    pub(crate) fn abandon_lost_enqueued_owner_attach_status(
        &mut self,
        publication: EnqueuedOwnerAttachStatus,
    ) -> Result<(), OwnerTargetError> {
        if !publication
            .owner
            .upgrade()
            .is_some_and(|owner| Arc::ptr_eq(&owner, &self.identity))
        {
            return Err(OwnerTargetError::RejectedEnqueuedOwnerAttachStatusAbandon {
                block: OwnerTargetInputBlock::WrongOwnerTargetExecutor,
                publication: Box::new(publication),
            });
        }
        let was_delivered = publication.receipt.was_delivered();
        let endpoint_lost = !transport_endpoint_is_open(&publication.endpoint);
        let queue_lost = !publication.receipt.queue_is_live();
        if was_delivered || (!endpoint_lost && !queue_lost) {
            return Err(OwnerTargetError::RejectedEnqueuedOwnerAttachStatusAbandon {
                block: OwnerTargetInputBlock::StatusPublicationStillLive,
                publication: Box::new(publication),
            });
        }
        if let Some(arm) = publication.arm.as_ref()
            && let Err(source) = self.supervisor.abandon_unarmed_owner_catch_up(arm)
        {
            return Err(OwnerTargetError::EnqueuedOwnerAttachStatusAbandonRejected {
                source,
                publication: Box::new(publication),
            });
        }
        Ok(())
    }

    fn pending_owner_attach_status(
        &self,
        leg: &EstablishedLeg,
        status: Frame,
        arm: Option<OwnerCatchUpArm>,
    ) -> PendingOwnerAttachStatusPublication {
        PendingOwnerAttachStatusPublication {
            owner: Arc::downgrade(&self.identity),
            seal: leg.standby_seal(),
            endpoint: leg.endpoint_liveness(),
            status,
            arm,
        }
    }

    pub(crate) fn execute_attached_recovery(
        &mut self,
        recovery: EnqueuedOwnerTargetAttach,
        queue: &mut LegOutboundQueue,
    ) -> Result<Vec<OwnerTargetRecoveryOutput>, OwnerTargetError> {
        let page = self.execute_attached_recovery_observed(recovery, queue)?;
        self.publish_legacy_attached_recovery_page(page, queue)
    }

    /// Retries only the bounded queue drain after pressure. Reducer and Target
    /// work were already consumed by the first turn and are never repeated.
    pub(crate) fn resume_attached_recovery_drain(
        &mut self,
        drain: OwnerTargetRecoveryDrain,
        queue: &mut LegOutboundQueue,
    ) -> Result<Vec<OwnerTargetRecoveryOutput>, OwnerTargetError> {
        self.publish_attached_recovery_page(drain, queue)
    }

    /// Publishes one already-executed recovery page through its exact
    /// acceptance FIFO. Queue pressure returns this cursor, so retry cannot
    /// repeat reducer or Target work.
    pub(crate) fn publish_attached_recovery_page(
        &mut self,
        drain: OwnerTargetRecoveryPublishCursor,
        queue: &mut LegOutboundQueue,
    ) -> Result<Vec<OwnerTargetRecoveryOutput>, OwnerTargetError> {
        if let Some(block) = self.recovery_owner_block(&drain.owner) {
            return Err(OwnerTargetError::RejectedAttachedRecoveryDrain {
                block,
                drain: Box::new(drain),
            });
        }
        if let Some(block) = self.recovery_admission_block_allow_pending_turn() {
            return Err(OwnerTargetError::RejectedAttachedRecoveryDrain {
                block,
                drain: Box::new(drain),
            });
        }
        let Some(acceptance) = drain.acceptance.as_ref() else {
            return Err(OwnerTargetError::AttachedRecoveryOutputInvariant {
                drain: Box::new(drain),
            });
        };
        if !acceptance.queue_is_live() {
            return Err(OwnerTargetError::RejectedAttachedRecoveryDrain {
                block: OwnerTargetInputBlock::AcceptanceQueueLost,
                drain: Box::new(drain),
            });
        }
        if !acceptance.belongs_to_queue(queue) {
            return Err(OwnerTargetError::RejectedAttachedRecoveryDrain {
                block: OwnerTargetInputBlock::WrongAcceptanceQueue,
                drain: Box::new(drain),
            });
        }
        self.drain_attached_recovery_outputs(drain, queue)
    }

    /// Consumes one exact recovery continuation and drains its following wire
    /// effects through the same queue. The generic owner resume capability is
    /// never exposed to transport/controller code on this path.
    pub(crate) fn resume_attached_recovery_effects(
        &mut self,
        recovery: OwnerTargetRecoveryResume,
        queue: &mut LegOutboundQueue,
    ) -> Result<Vec<OwnerTargetRecoveryOutput>, OwnerTargetError> {
        let page = self.resume_attached_recovery_effects_observed(recovery, queue)?;
        self.publish_legacy_attached_recovery_page(page, queue)
    }

    /// Binds the accepted initial active handoff to its exact terminal without
    /// reclassifying it as a recovery handoff.  Physical delivery is not the
    /// commit point: the record-specific acceptance receipt must already have
    /// been synchronously settled by the owner controller.
    #[allow(
        clippy::result_large_err,
        reason = "precommit rejection returns the exact initial handoff and terminal authority"
    )]
    fn terminalize_initial_handoff_exact(
        &mut self,
        handoff: OwnerTargetInitialHandoff,
        terminal: ExactLegTerminal,
    ) -> Result<
        PendingOwnerAttachLoss,
        OwnerRecoveryTerminalizationFailure<OwnerTargetInitialHandoff>,
    > {
        if let Some(block) = self.recovery_owner_block(&handoff.owner) {
            return Err(OwnerRecoveryTerminalizationFailure {
                block,
                input: handoff,
                terminal,
            });
        }
        if handoff.acceptance.is_some() {
            return Err(OwnerRecoveryTerminalizationFailure {
                block: OwnerTargetInputBlock::InitialAcceptanceSettlementPending,
                input: handoff,
                terminal,
            });
        }
        if handoff.physical_actor.is_none() {
            return Err(OwnerRecoveryTerminalizationFailure {
                block: OwnerTargetInputBlock::InitialPhysicalActorNotBound,
                input: handoff,
                terminal,
            });
        }
        if self.attach_barrier_pending {
            return Err(OwnerRecoveryTerminalizationFailure {
                block: OwnerTargetInputBlock::AttachBarrierPending,
                input: handoff,
                terminal,
            });
        }
        if let Some(block) = self.input_admission_block(None) {
            return Err(OwnerRecoveryTerminalizationFailure {
                block,
                input: handoff,
                terminal,
            });
        }

        let OwnerTargetInitialHandoff {
            owner,
            attached,
            acceptance,
            physical_actor,
        } = handoff;
        let generation = attached.generation();
        let resume_grace_expired = attached.resume_grace_expired_event();
        let loss = match attached.bind_terminal(terminal) {
            Ok(loss) => loss,
            Err(mismatch) => {
                let (attached, terminal) = mismatch.into_parts();
                return Err(OwnerRecoveryTerminalizationFailure {
                    block: OwnerTargetInputBlock::WrongLegTerminal,
                    input: OwnerTargetInitialHandoff {
                        owner,
                        attached,
                        acceptance,
                        physical_actor,
                    },
                    terminal,
                });
            }
        };
        let reason = loss.reason();
        drop((acceptance, physical_actor));
        Ok(PendingOwnerAttachLoss {
            owner,
            generation,
            reason,
            resume_grace_expired,
            loss,
            tail: TerminalOwnerAttachTail::HandoffReady,
            phase: OwnerRecoveryLossPhase::BarrierCleared,
        })
    }

    /// Physical wrapper for initial A terminalization.  The immutable runtime
    /// identity, policy leg and sole FIFO must still match the binding captured
    /// during initial adoption. Queue cleanup commits only after logical
    /// terminalization succeeds; every rejection leaves the FIFO untouched.
    #[allow(
        clippy::result_large_err,
        reason = "physical rejection returns the exact initial handoff and terminal authority"
    )]
    pub(super) fn terminalize_initial_handoff_physical_exact(
        &mut self,
        handoff: OwnerTargetInitialHandoff,
        terminal: ExactLegTerminal,
        runtime: &PhysicalLegRuntimeIncarnation,
        io: &LegIo,
        queue: &mut LegOutboundQueue,
    ) -> Result<
        PendingOwnerAttachLoss,
        OwnerRecoveryTerminalizationFailure<OwnerTargetInitialHandoff>,
    > {
        let Some(binding) = handoff.physical_actor.as_ref() else {
            return Err(OwnerRecoveryTerminalizationFailure {
                block: OwnerTargetInputBlock::InitialPhysicalActorNotBound,
                input: handoff,
                terminal,
            });
        };
        if !binding.matches(runtime, io, queue)
            || !handoff.matches_transport_identity(io.established_leg())
        {
            return Err(OwnerRecoveryTerminalizationFailure {
                block: OwnerTargetInputBlock::WrongInitialPhysicalActor,
                input: handoff,
                terminal,
            });
        }
        self.terminalize_with_physical_queue_exact(
            handoff,
            terminal,
            io.established_leg(),
            queue,
            Self::terminalize_initial_handoff_exact,
        )
    }

    fn terminalize_with_physical_queue_exact<I>(
        &mut self,
        input: I,
        terminal: ExactLegTerminal,
        leg: &EstablishedLeg,
        queue: &mut LegOutboundQueue,
        terminalize: impl FnOnce(
            &mut Self,
            I,
            ExactLegTerminal,
        ) -> Result<
            PendingOwnerAttachLoss,
            OwnerRecoveryTerminalizationFailure<I>,
        >,
    ) -> Result<PendingOwnerAttachLoss, OwnerRecoveryTerminalizationFailure<I>> {
        let Some(cleanup) = queue.begin_owner_terminal_cleanup(leg, &terminal) else {
            return Err(OwnerRecoveryTerminalizationFailure {
                block: OwnerTargetInputBlock::WrongTerminalQueue,
                input,
                terminal,
            });
        };
        match terminalize(self, input, terminal) {
            Ok(pending) => {
                let _discarded = cleanup.commit();
                Ok(pending)
            }
            Err(failure) => Err(failure),
        }
    }

    /// Binds an unpublished replacement and all of its unsent recovery tail
    /// to the exact terminal fact. The supervisor and attach barrier remain
    /// unchanged until [`Self::discard_terminal_attach_recovery`] explicitly
    /// consumes the returned loss.
    #[allow(
        clippy::result_large_err,
        reason = "every pre-transition rejection returns the exact publication and terminal authority"
    )]
    pub(super) fn terminalize_pending_attach_before_acceptance_exact(
        &mut self,
        pending: PendingOwnerTargetAttachPublication,
        terminal: ExactLegTerminal,
    ) -> Result<
        PendingOwnerAttachLoss,
        OwnerRecoveryTerminalizationFailure<PendingOwnerTargetAttachPublication>,
    > {
        if let Some(block) = self.recovery_owner_block(&pending.owner) {
            return Err(OwnerRecoveryTerminalizationFailure {
                block,
                input: pending,
                terminal,
            });
        }
        if let Some(block) = self.recovery_admission_block() {
            return Err(OwnerRecoveryTerminalizationFailure {
                block,
                input: pending,
                terminal,
            });
        }

        let PendingOwnerTargetAttachPublication {
            owner,
            endpoint,
            attached,
            acceptance,
            effects: recovery_effects,
            admission,
        } = pending;
        let generation = attached.generation();
        let resume_grace_expired = attached.resume_grace_expired_event();
        let loss = match attached.bind_terminal(terminal) {
            Ok(loss) => loss,
            Err(mismatch) => {
                let (attached, terminal) = mismatch.into_parts();
                return Err(OwnerRecoveryTerminalizationFailure {
                    block: OwnerTargetInputBlock::WrongLegTerminal,
                    input: PendingOwnerTargetAttachPublication {
                        owner,
                        endpoint,
                        attached,
                        acceptance,
                        effects: recovery_effects,
                        admission,
                    },
                    terminal,
                });
            }
        };
        let reason = loss.reason();
        Ok(PendingOwnerAttachLoss {
            owner,
            generation,
            reason,
            resume_grace_expired,
            loss,
            tail: TerminalOwnerAttachTail::Pending {
                endpoint,
                acceptance,
                effects: recovery_effects,
                admission,
            },
            phase: OwnerRecoveryLossPhase::BarrierStrict,
        })
    }

    pub(super) fn terminalize_pending_attach_before_acceptance_physical_exact(
        &mut self,
        pending: PendingOwnerTargetAttachPublication,
        terminal: ExactLegTerminal,
        leg: &EstablishedLeg,
        queue: &mut LegOutboundQueue,
    ) -> Result<
        PendingOwnerAttachLoss,
        OwnerRecoveryTerminalizationFailure<PendingOwnerTargetAttachPublication>,
    > {
        self.terminalize_with_physical_queue_exact(
            pending,
            terminal,
            leg,
            queue,
            Self::terminalize_pending_attach_before_acceptance_exact,
        )
    }

    pub(crate) fn terminalize_pending_attach_before_acceptance(
        &mut self,
        pending: PendingOwnerTargetAttachPublication,
        terminal: ExactLegTerminal,
    ) -> Result<PendingOwnerAttachLoss, OwnerTargetError> {
        self.terminalize_pending_attach_before_acceptance_exact(pending, terminal)
            .map_err(|failure| {
                let block = failure.block();
                let (pending, terminal) = failure.into_parts();
                OwnerTargetError::RejectedPendingAttachLoss {
                    block,
                    pending: Box::new(pending),
                    terminal: Box::new(terminal),
                }
            })
    }

    #[allow(
        clippy::result_large_err,
        reason = "terminal mismatch returns the exact enqueued recovery and terminal authority"
    )]
    pub(super) fn terminalize_enqueued_attach_recovery_exact(
        &mut self,
        recovery: EnqueuedOwnerTargetAttach,
        terminal: ExactLegTerminal,
    ) -> Result<
        PendingOwnerAttachLoss,
        OwnerRecoveryTerminalizationFailure<EnqueuedOwnerTargetAttach>,
    > {
        if let Some(block) = self.recovery_owner_block(&recovery.owner) {
            return Err(OwnerRecoveryTerminalizationFailure {
                block,
                input: recovery,
                terminal,
            });
        }
        if let Some(block) = self.recovery_admission_block() {
            return Err(OwnerRecoveryTerminalizationFailure {
                block,
                input: recovery,
                terminal,
            });
        }
        let EnqueuedOwnerTargetAttach {
            owner,
            attached,
            recovery,
        } = recovery;
        let generation = attached.generation();
        let resume_grace_expired = attached.resume_grace_expired_event();
        let loss = match attached.bind_terminal(terminal) {
            Ok(loss) => loss,
            Err(mismatch) => {
                let (attached, terminal) = mismatch.into_parts();
                return Err(OwnerRecoveryTerminalizationFailure {
                    block: OwnerTargetInputBlock::WrongLegTerminal,
                    input: EnqueuedOwnerTargetAttach {
                        owner,
                        attached,
                        recovery,
                    },
                    terminal,
                });
            }
        };
        Ok(PendingOwnerAttachLoss {
            owner,
            generation,
            reason: loss.reason(),
            resume_grace_expired,
            loss,
            tail: TerminalOwnerAttachTail::Enqueued(recovery),
            phase: OwnerRecoveryLossPhase::BarrierStrict,
        })
    }

    pub(super) fn terminalize_enqueued_attach_recovery_physical_exact(
        &mut self,
        recovery: EnqueuedOwnerTargetAttach,
        terminal: ExactLegTerminal,
        leg: &EstablishedLeg,
        queue: &mut LegOutboundQueue,
    ) -> Result<
        PendingOwnerAttachLoss,
        OwnerRecoveryTerminalizationFailure<EnqueuedOwnerTargetAttach>,
    > {
        self.terminalize_with_physical_queue_exact(
            recovery,
            terminal,
            leg,
            queue,
            Self::terminalize_enqueued_attach_recovery_exact,
        )
    }

    pub(crate) fn terminalize_enqueued_attach_recovery(
        &mut self,
        recovery: EnqueuedOwnerTargetAttach,
        terminal: ExactLegTerminal,
    ) -> Result<PendingOwnerAttachLoss, OwnerTargetError> {
        self.terminalize_enqueued_attach_recovery_exact(recovery, terminal)
            .map_err(|failure| {
                let block = failure.block();
                let (recovery, terminal) = failure.into_parts();
                OwnerTargetError::RejectedAttachedRecoveryTerminalization {
                    block,
                    recovery: Box::new(recovery),
                    terminal: Box::new(terminal),
                }
            })
    }

    #[allow(
        clippy::result_large_err,
        reason = "terminal mismatch returns the exact retained drain and terminal authority"
    )]
    pub(super) fn terminalize_attached_recovery_drain_exact(
        &mut self,
        drain: OwnerTargetRecoveryDrain,
        terminal: ExactLegTerminal,
    ) -> Result<PendingOwnerAttachLoss, OwnerRecoveryTerminalizationFailure<OwnerTargetRecoveryDrain>>
    {
        if let Some(block) = self.recovery_owner_block(&drain.owner) {
            return Err(OwnerRecoveryTerminalizationFailure {
                block,
                input: drain,
                terminal,
            });
        }
        if let Some(block) = self.recovery_admission_block_allow_pending_turn() {
            return Err(OwnerRecoveryTerminalizationFailure {
                block,
                input: drain,
                terminal,
            });
        }
        let OwnerTargetRecoveryDrain {
            owner,
            mut attached,
            acceptance,
            pending,
            ready,
        } = drain;
        let Some(attached) = attached.take() else {
            return Err(OwnerRecoveryTerminalizationFailure {
                block: OwnerTargetInputBlock::MissingAttachedRecovery,
                input: OwnerTargetRecoveryDrain {
                    owner,
                    attached,
                    acceptance,
                    pending,
                    ready,
                },
                terminal,
            });
        };
        let generation = attached.generation();
        let resume_grace_expired = attached.resume_grace_expired_event();
        let loss = match attached.bind_terminal(terminal) {
            Ok(loss) => loss,
            Err(mismatch) => {
                let (attached, terminal) = mismatch.into_parts();
                return Err(OwnerRecoveryTerminalizationFailure {
                    block: OwnerTargetInputBlock::WrongLegTerminal,
                    input: OwnerTargetRecoveryDrain {
                        owner,
                        attached: Some(attached),
                        acceptance,
                        pending,
                        ready,
                    },
                    terminal,
                });
            }
        };
        Ok(PendingOwnerAttachLoss {
            owner,
            generation,
            reason: loss.reason(),
            resume_grace_expired,
            loss,
            tail: TerminalOwnerAttachTail::Drain {
                acceptance,
                pending,
                ready,
            },
            phase: OwnerRecoveryLossPhase::BarrierPendingTurn,
        })
    }

    pub(super) fn terminalize_attached_recovery_drain_physical_exact(
        &mut self,
        drain: OwnerTargetRecoveryDrain,
        terminal: ExactLegTerminal,
        leg: &EstablishedLeg,
        queue: &mut LegOutboundQueue,
    ) -> Result<PendingOwnerAttachLoss, OwnerRecoveryTerminalizationFailure<OwnerTargetRecoveryDrain>>
    {
        self.terminalize_with_physical_queue_exact(
            drain,
            terminal,
            leg,
            queue,
            Self::terminalize_attached_recovery_drain_exact,
        )
    }

    pub(crate) fn terminalize_attached_recovery_drain(
        &mut self,
        drain: OwnerTargetRecoveryDrain,
        terminal: ExactLegTerminal,
    ) -> Result<PendingOwnerAttachLoss, OwnerTargetError> {
        self.terminalize_attached_recovery_drain_exact(drain, terminal)
            .map_err(|failure| {
                let block = failure.block();
                let (drain, terminal) = failure.into_parts();
                OwnerTargetError::RejectedAttachedRecoveryDrainTerminalization {
                    block,
                    drain: Box::new(drain),
                    terminal: Box::new(terminal),
                }
            })
    }

    #[allow(
        clippy::result_large_err,
        reason = "terminal mismatch returns the exact retained resume and terminal authority"
    )]
    pub(super) fn terminalize_attached_recovery_resume_exact(
        &mut self,
        recovery: OwnerTargetRecoveryResume,
        terminal: ExactLegTerminal,
    ) -> Result<
        PendingOwnerAttachLoss,
        OwnerRecoveryTerminalizationFailure<OwnerTargetRecoveryResume>,
    > {
        if let Some(block) = self.recovery_owner_block(&recovery.resume.owner) {
            return Err(OwnerRecoveryTerminalizationFailure {
                block,
                input: recovery,
                terminal,
            });
        }
        if let Some(block) = self.recovery_admission_block_allow_pending_turn() {
            return Err(OwnerRecoveryTerminalizationFailure {
                block,
                input: recovery,
                terminal,
            });
        }
        let OwnerTargetRecoveryResume {
            attached,
            acceptance,
            resume,
        } = recovery;
        let owner = resume.owner.clone();
        let generation = attached.generation();
        let resume_grace_expired = attached.resume_grace_expired_event();
        let loss = match attached.bind_terminal(terminal) {
            Ok(loss) => loss,
            Err(mismatch) => {
                let (attached, terminal) = mismatch.into_parts();
                return Err(OwnerRecoveryTerminalizationFailure {
                    block: OwnerTargetInputBlock::WrongLegTerminal,
                    input: OwnerTargetRecoveryResume {
                        attached,
                        acceptance,
                        resume,
                    },
                    terminal,
                });
            }
        };
        Ok(PendingOwnerAttachLoss {
            owner,
            generation,
            reason: loss.reason(),
            resume_grace_expired,
            loss,
            tail: TerminalOwnerAttachTail::Resume { acceptance, resume },
            phase: OwnerRecoveryLossPhase::BarrierPendingTurn,
        })
    }

    pub(super) fn terminalize_attached_recovery_resume_physical_exact(
        &mut self,
        recovery: OwnerTargetRecoveryResume,
        terminal: ExactLegTerminal,
        leg: &EstablishedLeg,
        queue: &mut LegOutboundQueue,
    ) -> Result<
        PendingOwnerAttachLoss,
        OwnerRecoveryTerminalizationFailure<OwnerTargetRecoveryResume>,
    > {
        self.terminalize_with_physical_queue_exact(
            recovery,
            terminal,
            leg,
            queue,
            Self::terminalize_attached_recovery_resume_exact,
        )
    }

    pub(crate) fn terminalize_attached_recovery_resume(
        &mut self,
        recovery: OwnerTargetRecoveryResume,
        terminal: ExactLegTerminal,
    ) -> Result<PendingOwnerAttachLoss, OwnerTargetError> {
        self.terminalize_attached_recovery_resume_exact(recovery, terminal)
            .map_err(|failure| {
                let block = failure.block();
                let (recovery, terminal) = failure.into_parts();
                OwnerTargetError::RejectedAttachedRecoveryResumeTerminalization {
                    block,
                    recovery: Box::new(recovery),
                    terminal: Box::new(terminal),
                }
            })
    }

    /// Binds a fully published recovery handoff after the attach barrier has
    /// cleared. Owner identity remains paired with the AttachedLeg, so a
    /// foreign controller or terminal mismatch returns both exact inputs.
    pub(super) fn terminalize_recovery_handoff_exact(
        &mut self,
        handoff: OwnerTargetRecoveryHandoff,
        terminal: ExactLegTerminal,
    ) -> Result<
        PendingOwnerAttachLoss,
        OwnerRecoveryTerminalizationFailure<OwnerTargetRecoveryHandoff>,
    > {
        if let Some(block) = self.recovery_owner_block(&handoff.owner) {
            return Err(OwnerRecoveryTerminalizationFailure {
                block,
                input: handoff,
                terminal,
            });
        }
        if self.attach_barrier_pending {
            return Err(OwnerRecoveryTerminalizationFailure {
                block: OwnerTargetInputBlock::AttachBarrierPending,
                input: handoff,
                terminal,
            });
        }
        if let Some(block) = self.input_admission_block(None) {
            return Err(OwnerRecoveryTerminalizationFailure {
                block,
                input: handoff,
                terminal,
            });
        }
        let OwnerTargetRecoveryHandoff { owner, attached } = handoff;
        let generation = attached.generation();
        let resume_grace_expired = attached.resume_grace_expired_event();
        let loss = match attached.bind_terminal(terminal) {
            Ok(loss) => loss,
            Err(mismatch) => {
                let (attached, terminal) = mismatch.into_parts();
                return Err(OwnerRecoveryTerminalizationFailure {
                    block: OwnerTargetInputBlock::WrongLegTerminal,
                    input: OwnerTargetRecoveryHandoff { owner, attached },
                    terminal,
                });
            }
        };
        Ok(PendingOwnerAttachLoss {
            owner,
            generation,
            reason: loss.reason(),
            resume_grace_expired,
            loss,
            tail: TerminalOwnerAttachTail::HandoffReady,
            phase: OwnerRecoveryLossPhase::BarrierCleared,
        })
    }

    pub(super) fn terminalize_recovery_handoff_physical_exact(
        &mut self,
        handoff: OwnerTargetRecoveryHandoff,
        terminal: ExactLegTerminal,
        leg: &EstablishedLeg,
        queue: &mut LegOutboundQueue,
    ) -> Result<
        PendingOwnerAttachLoss,
        OwnerRecoveryTerminalizationFailure<OwnerTargetRecoveryHandoff>,
    > {
        self.terminalize_with_physical_queue_exact(
            handoff,
            terminal,
            leg,
            queue,
            Self::terminalize_recovery_handoff_exact,
        )
    }

    /// Explicitly discards one terminal-bound unpublished recovery tail, then
    /// applies exactly one `LegLost` to the same supervisor and releases the
    /// attach barrier into a legless high-water state.
    #[allow(
        clippy::result_large_err,
        reason = "wrong executor or barrier state returns the complete terminal-bound recovery"
    )]
    pub(super) fn discard_terminal_attach_recovery_exact(
        &mut self,
        pending: PendingOwnerAttachLoss,
    ) -> Result<OwnerLeglessHighWater, OwnerTerminalAttachDiscardFailure> {
        if let Some(block) = self.recovery_owner_block(&pending.owner) {
            return Err(OwnerTerminalAttachDiscardFailure::Blocked { block, pending });
        }
        let admission = match pending.phase {
            OwnerRecoveryLossPhase::BarrierStrict => self.recovery_admission_block(),
            OwnerRecoveryLossPhase::BarrierPendingTurn => {
                self.recovery_admission_block_allow_pending_turn()
            }
            OwnerRecoveryLossPhase::BarrierCleared => {
                if self.attach_barrier_pending {
                    Some(OwnerTargetInputBlock::AttachBarrierPending)
                } else {
                    self.input_admission_block(None)
                }
            }
        };
        if let Some(block) = admission {
            return Err(OwnerTerminalAttachDiscardFailure::Blocked { block, pending });
        }
        let generation = pending.generation;
        let effects = match self.supervisor.apply_event(pending.loss.event()) {
            Ok(effects) => effects,
            Err(SessionSupervisorError::RejectedEvent { block, event }) => {
                drop(event);
                return Err(OwnerTerminalAttachDiscardFailure::Blocked {
                    block: OwnerTargetInputBlock::ReducerAdmission(block),
                    pending,
                });
            }
            Err(source) => {
                return Err(OwnerTerminalAttachDiscardFailure::Quarantined(
                    QuarantinedOwnerAttachLoss {
                        pending,
                        source: OwnerTerminalAttachDiscardQuarantineSource::Supervisor(source),
                    },
                ));
            }
        };
        if effects.as_slice() != [SessionEffect::ResumeGraceStarted { generation }] {
            return Err(OwnerTerminalAttachDiscardFailure::Quarantined(
                QuarantinedOwnerAttachLoss {
                    pending,
                    source: OwnerTerminalAttachDiscardQuarantineSource::Effects {
                        generation,
                        effects,
                    },
                },
            ));
        }
        let mut batch = crate::resumable::effect_storage::SessionEffectBatch::from_vec(effects);
        drop(batch.remove(0)); // The checked value-only ResumeGraceStarted is consumed here.
        if let Err(source) = self.supervisor.reclaim_effect_batch(&mut batch) {
            return Err(OwnerTerminalAttachDiscardFailure::Quarantined(
                QuarantinedOwnerAttachLoss {
                    pending,
                    source: OwnerTerminalAttachDiscardQuarantineSource::Storage {
                        source,
                        effects: batch,
                    },
                },
            ));
        }
        let PendingOwnerAttachLoss {
            owner: _,
            generation,
            reason,
            resume_grace_expired,
            loss,
            tail,
            phase: _,
        } = pending;
        tail.discard();
        drop(loss);
        self.finish_lost_attach_barrier();
        Ok(OwnerLeglessHighWater {
            owner: Arc::downgrade(&self.identity),
            generation,
            reason,
            resume_grace_expired,
        })
    }

    pub(crate) fn discard_terminal_attach_recovery(
        &mut self,
        pending: PendingOwnerAttachLoss,
    ) -> Result<OwnerLeglessHighWater, OwnerTargetError> {
        self.discard_terminal_attach_recovery_exact(pending)
            .map_err(|failure| match failure {
                OwnerTerminalAttachDiscardFailure::Blocked { block, pending } => {
                    OwnerTargetError::RejectedTerminalAttachRecoveryDiscard {
                        block,
                        pending: Box::new(pending),
                    }
                }
                OwnerTerminalAttachDiscardFailure::Quarantined(quarantine) => {
                    let QuarantinedOwnerAttachLoss { pending, source } = quarantine;
                    match source {
                        OwnerTerminalAttachDiscardQuarantineSource::Storage { source, effects } => {
                            OwnerTargetError::TerminalAttachRecoveryStorageInvariant {
                                source,
                                effects,
                                pending: Box::new(pending),
                            }
                        }
                        OwnerTerminalAttachDiscardQuarantineSource::Supervisor(source) => {
                            OwnerTargetError::TerminalAttachRecoveryDiscardInvariant {
                                source,
                                pending: Box::new(pending),
                            }
                        }
                        OwnerTerminalAttachDiscardQuarantineSource::Effects {
                            generation,
                            effects,
                        } => OwnerTargetError::PendingAttachLossEffectInvariant {
                            generation,
                            effects,
                            pending: Box::new(pending),
                        },
                    }
                }
            })
    }

    fn finish_lost_attach_barrier(&mut self) {
        self.pending_effects.clear();
        self.outstanding_resume = None;
        self.attach_barrier_pending = false;
    }

    fn drain_attached_recovery_outputs(
        &mut self,
        mut drain: OwnerTargetRecoveryDrain,
        queue: &mut LegOutboundQueue,
    ) -> Result<Vec<OwnerTargetRecoveryOutput>, OwnerTargetError> {
        while let Some(output) = drain.pending.pop_front() {
            match output {
                OwnerTargetOutput::Transmit(frame) => {
                    if let Err(error) = queue.push(frame) {
                        let kind = error.kind().clone();
                        drain
                            .pending
                            .push_front(OwnerTargetOutput::Transmit(error.into_frame()));
                        return Err(OwnerTargetError::AttachedRecoveryQueueBlocked {
                            kind,
                            drain: Box::new(drain),
                        });
                    }
                }
                OwnerTargetOutput::LegActivated { generation } => drain
                    .ready
                    .push(OwnerTargetRecoveryOutput::LegActivated { generation }),
                OwnerTargetOutput::ResumeGraceStarted { generation } => drain
                    .ready
                    .push(OwnerTargetRecoveryOutput::ResumeGraceStarted { generation }),
                OwnerTargetOutput::TerminalGraceStarted { terminal } => drain
                    .ready
                    .push(OwnerTargetRecoveryOutput::TerminalGraceStarted { terminal }),
                OwnerTargetOutput::SessionExpired => {
                    drain.ready.push(OwnerTargetRecoveryOutput::SessionExpired)
                }
                OwnerTargetOutput::NeedsResume { resume } => {
                    if !drain.pending.is_empty() {
                        drain
                            .pending
                            .push_front(OwnerTargetOutput::NeedsResume { resume });
                        return Err(OwnerTargetError::AttachedRecoveryOutputInvariant {
                            drain: Box::new(drain),
                        });
                    }
                    let Some(acceptance) = drain.acceptance.take() else {
                        drain
                            .pending
                            .push_front(OwnerTargetOutput::NeedsResume { resume });
                        return Err(OwnerTargetError::AttachedRecoveryOutputInvariant {
                            drain: Box::new(drain),
                        });
                    };
                    let Some(attached) = drain.attached.take() else {
                        drain.acceptance = Some(acceptance);
                        drain
                            .pending
                            .push_front(OwnerTargetOutput::NeedsResume { resume });
                        return Err(OwnerTargetError::AttachedRecoveryOutputInvariant {
                            drain: Box::new(drain),
                        });
                    };
                    drain.ready.push(OwnerTargetRecoveryOutput::NeedsResume {
                        resume: OwnerTargetRecoveryResume {
                            attached,
                            acceptance,
                            resume,
                        },
                    });
                    return Ok(drain.ready);
                }
            }
        }
        let Some(attached) = drain.attached.take() else {
            return Err(OwnerTargetError::AttachedRecoveryOutputInvariant {
                drain: Box::new(drain),
            });
        };
        if !self.attach_barrier_pending {
            drain.attached = Some(attached);
            return Err(OwnerTargetError::AttachedRecoveryOutputInvariant {
                drain: Box::new(drain),
            });
        }
        let Some(acceptance) = drain.acceptance.take() else {
            drain.attached = Some(attached);
            return Err(OwnerTargetError::AttachedRecoveryOutputInvariant {
                drain: Box::new(drain),
            });
        };
        if !queue.finish_attach_recovery(&acceptance) {
            drain.attached = Some(attached);
            drain.acceptance = Some(acceptance);
            return Err(OwnerTargetError::AttachedRecoveryOutputInvariant {
                drain: Box::new(drain),
            });
        }
        self.attach_barrier_pending = false;
        drain
            .ready
            .push(OwnerTargetRecoveryOutput::RecoveryCompleted {
                handoff: OwnerTargetRecoveryHandoff {
                    owner: Arc::downgrade(&self.identity),
                    attached,
                },
            });
        Ok(drain.ready)
    }

    fn publish_legacy_attached_recovery_page(
        &mut self,
        page: OwnerTargetObservedRecoveryPage,
        queue: &mut LegOutboundQueue,
    ) -> Result<Vec<OwnerTargetRecoveryOutput>, OwnerTargetError> {
        let (publish, observations) = page.into_progress_parts();
        if observations.is_empty() {
            return self.publish_attached_recovery_page(publish, queue);
        }
        let error = match self.retain_observations_on_failure::<()>(
            Err(OwnerTargetError::AttachedRecoveryObservationInvariant),
            observations,
        ) {
            Err(error) => error,
            Ok(()) => OwnerTargetError::AttachedRecoveryObservationInvariant,
        };
        Err(self.retain_failed_recovery_cursor(publish, error))
    }

    /// Retries one retained Target sink offer.  Zero and would-block retain the
    /// exact offer and produce no ACK.  A positive completion is returned to
    /// the reducer before any following offer can be attempted.
    pub(crate) fn retry_target_write(
        &mut self,
        flow_id: SessionFlowId,
    ) -> Result<Vec<OwnerTargetOutput>, OwnerTargetError> {
        self.retry_target_write_observed(flow_id)
            .map(OwnerTargetObservedApply::into_outputs)
    }

    /// Attempts to finish one Target join whose owned application buffers were
    /// not yet drainable when the reducer retired the flow.
    pub(crate) fn retry_target_join(
        &mut self,
        flow_id: SessionFlowId,
    ) -> Result<(), OwnerTargetError> {
        self.require_no_pending_effects()?;
        self.require_no_pending_target_completion(flow_id)?;
        let flow = self
            .flows
            .get(&flow_id)
            .ok_or(OwnerTargetError::UnknownTargetFlow { flow_id })?;
        if !flow.join_pending {
            return Err(OwnerTargetError::NoPendingTargetJoin { flow_id });
        }
        let attempt = match self.try_join_target_flow(flow_id) {
            Ok(attempt) => attempt,
            Err(error) => {
                return self.abort_direct_owner_error(flow_id, TargetIoOperation::Join, error);
            }
        };
        match attempt {
            TargetJoinAttempt::Joined => Ok(()),
            TargetJoinAttempt::Pending(_) => {
                Err(OwnerTargetError::TargetJoinStillPending { flow_id })
            }
        }
    }
    fn require_no_pending_effects(&self) -> Result<(), OwnerTargetError> {
        if self.aborted_effects.is_some() {
            Err(OwnerTargetError::AbortedEffectInvariant)
        } else if self.attach_barrier_pending {
            Err(OwnerTargetError::AttachBarrierPending)
        } else if self.pending_effects.is_empty() && self.outstanding_resume.is_none() {
            Ok(())
        } else {
            Err(OwnerTargetError::PendingEffectTurn)
        }
    }
    fn require_no_pending_target_completion(
        &self,
        flow_id: SessionFlowId,
    ) -> Result<(), OwnerTargetError> {
        let flow = self
            .flows
            .get(&flow_id)
            .ok_or(OwnerTargetError::UnknownTargetFlow { flow_id })?;
        if flow.completion_blocks_input(&self.service) {
            Err(OwnerTargetError::PendingTargetCompletion { flow_id })
        } else {
            Ok(())
        }
    }
    fn input_admission_block(
        &self,
        flow_id: Option<SessionFlowId>,
    ) -> Option<OwnerTargetInputBlock> {
        if self.aborted_effects.is_some() {
            return Some(OwnerTargetInputBlock::AbortedEffectInvariant);
        }
        if self.attach_barrier_pending {
            return Some(OwnerTargetInputBlock::AttachBarrierPending);
        }
        if !self.pending_effects.is_empty() || self.outstanding_resume.is_some() {
            return Some(OwnerTargetInputBlock::PendingEffectTurn);
        }
        flow_id.and_then(|flow_id| {
            self.flows
                .get(&flow_id)
                .is_some_and(|flow| flow.completion_blocks_input(&self.service))
                .then_some(OwnerTargetInputBlock::PendingTargetCompletion { flow_id })
        })
    }
    /// Borrow-only preflight used by the closed physical owner before it
    /// consumes an actor envelope. A blocked ATTACH therefore remains wholly
    /// actor-owned and can retry with its original arrival/ordinal evidence.
    pub(super) fn owner_attach_admission_block(&self) -> Option<OwnerTargetInputBlock> {
        self.input_admission_block(None)
    }
    fn recovery_owner_block(
        &self,
        owner: &Weak<OwnerTargetExecutorIdentity>,
    ) -> Option<OwnerTargetInputBlock> {
        match owner.upgrade() {
            None => Some(OwnerTargetInputBlock::OwnerTargetExecutorLost),
            Some(identity) if !Arc::ptr_eq(&identity, &self.identity) => {
                Some(OwnerTargetInputBlock::WrongOwnerTargetExecutor)
            }
            Some(_) => None,
        }
    }
    /// Precommit identity check for the controller handoff. The capability is
    /// only borrowed here, so a wrong controller can reject it without ever
    /// storing or consuming the exact recovery ownership.
    pub(super) fn owns_enqueued_attached_recovery(
        &self,
        recovery: &EnqueuedOwnerTargetAttach,
    ) -> bool {
        self.recovery_owner_block(&recovery.owner).is_none()
    }

    pub(super) fn owns_recovery_handoff(&self, handoff: &OwnerTargetRecoveryHandoff) -> bool {
        self.recovery_owner_block(&handoff.owner).is_none()
    }
    /// Performs every fallible check needed to turn the settled initial A
    /// handoff into a retired-only capability when B becomes current.  The
    /// returned plan is consumed only after the controller installs B; until
    /// then `restore_initial` reconstructs the exact original handoff.
    #[allow(
        clippy::result_large_err,
        reason = "precommit rejection must return the exact initial handoff"
    )]
    pub(super) fn prepare_initial_retirement<'queue>(
        &self,
        mut initial: OwnerTargetInitialHandoff,
        replacement: &OwnerTargetRecoveryHandoff,
        a_runtime: &PhysicalLegRuntimeIncarnation,
        a_io: &LegIo,
        a_queue: &'queue mut LegOutboundQueue,
    ) -> Result<PreparedOwnerInitialRetirement<'queue>, OwnerTargetInitialRetirementFailure> {
        if self.recovery_owner_block(&initial.owner).is_some() {
            return Err(OwnerTargetInitialRetirementFailure {
                kind: OwnerTargetInitialRetirementErrorKind::WrongInitialOwner,
                initial,
            });
        }
        if self.recovery_owner_block(&replacement.owner).is_some() {
            return Err(OwnerTargetInitialRetirementFailure {
                kind: OwnerTargetInitialRetirementErrorKind::WrongReplacementOwner,
                initial,
            });
        }
        if initial.acceptance.is_some() {
            return Err(OwnerTargetInitialRetirementFailure {
                kind: OwnerTargetInitialRetirementErrorKind::AcceptanceNotSettled,
                initial,
            });
        }
        let Some(binding) = initial.physical_actor.as_ref() else {
            return Err(OwnerTargetInitialRetirementFailure {
                kind: OwnerTargetInitialRetirementErrorKind::MissingPhysicalActor,
                initial,
            });
        };
        if !binding.matches(a_runtime, a_io, a_queue)
            || !initial.matches_transport_identity(a_io.established_leg())
        {
            return Err(OwnerTargetInitialRetirementFailure {
                kind: OwnerTargetInitialRetirementErrorKind::WrongPhysicalActor,
                initial,
            });
        }
        if replacement.generation().get() <= initial.generation().get() {
            return Err(OwnerTargetInitialRetirementFailure {
                kind: OwnerTargetInitialRetirementErrorKind::ReplacementNotNewer,
                initial,
            });
        }
        let Some(a_inventory) = a_queue.cold_inventory() else {
            return Err(OwnerTargetInitialRetirementFailure {
                kind: OwnerTargetInitialRetirementErrorKind::InitialOutboundQueueInvariant,
                initial,
            });
        };
        if !a_inventory.is_exact_active_zero() {
            return Err(OwnerTargetInitialRetirementFailure {
                kind: OwnerTargetInitialRetirementErrorKind::InitialOutboundNotPristine {
                    was_active: a_inventory.was_active(),
                    queued_frames: a_inventory.queued_frames(),
                    summed_frame_bytes: a_inventory.summed_frame_bytes(),
                    stored_owned_bytes: a_inventory.stored_owned_bytes(),
                    ordered_delivery_wait: a_inventory.ordered_delivery_wait(),
                },
                initial,
            });
        }

        let Some(physical_actor) = initial.physical_actor.take() else {
            return Err(OwnerTargetInitialRetirementFailure {
                kind: OwnerTargetInitialRetirementErrorKind::MissingPhysicalActor,
                initial,
            });
        };
        let OwnerTargetInitialHandoff {
            owner,
            attached,
            acceptance,
            physical_actor: remaining_physical_actor,
        } = initial;
        drop((acceptance, remaining_physical_actor));
        Ok(PreparedOwnerInitialRetirement {
            _a_queue: a_queue,
            owner,
            attached,
            physical_actor,
            replacement_generation: replacement.generation(),
            replacement_nonce: replacement.nonce(),
        })
    }
    /// Retired-only joined transaction.  It validates A's frozen actor and B
    /// replacement identities, then consumes the exact A terminal without
    /// creating a pending loss or applying another reducer event.
    #[allow(
        clippy::result_large_err,
        reason = "settlement rejection must return retired handoff and terminal ownership"
    )]
    pub(super) fn settle_retired_initial_terminal(
        &self,
        retired: OwnerTargetRetiredInitialHandoff,
        active_b: Option<&OwnerTargetRecoveryHandoff>,
        a_runtime: &PhysicalLegRuntimeIncarnation,
        terminal: Option<ExactLegTerminal>,
    ) -> Result<OwnerTargetRetiredInitialSettled, OwnerTargetRetiredInitialSettlementFailure> {
        if self.recovery_owner_block(&retired.owner).is_some() {
            return Err(OwnerTargetRetiredInitialSettlementFailure {
                kind: OwnerTargetRetiredInitialSettlementErrorKind::WrongRetiredOwner,
                retired,
                terminal,
            });
        }
        if active_b.is_some_and(|active_b| self.recovery_owner_block(&active_b.owner).is_some()) {
            return Err(OwnerTargetRetiredInitialSettlementFailure {
                kind: OwnerTargetRetiredInitialSettlementErrorKind::WrongActiveOwner,
                retired,
                terminal,
            });
        }
        let committed_generation = self.supervisor.snapshot().session.generation();
        if committed_generation != retired.replacement_generation
            || active_b.is_some_and(|active_b| {
                active_b.generation() != retired.replacement_generation
                    || active_b.nonce() != retired.replacement_nonce
            })
        {
            return Err(OwnerTargetRetiredInitialSettlementFailure {
                kind: OwnerTargetRetiredInitialSettlementErrorKind::WrongReplacement,
                retired,
                terminal,
            });
        }
        if !retired.physical_actor.matches_joined_actor(a_runtime) {
            return Err(OwnerTargetRetiredInitialSettlementFailure {
                kind: OwnerTargetRetiredInitialSettlementErrorKind::WrongJoinedActor,
                retired,
                terminal,
            });
        }
        let Some(terminal) = terminal else {
            return Err(OwnerTargetRetiredInitialSettlementFailure {
                kind: OwnerTargetRetiredInitialSettlementErrorKind::MissingTerminal,
                retired,
                terminal: None,
            });
        };
        if !retired.attached.matches_terminal(&terminal) {
            return Err(OwnerTargetRetiredInitialSettlementFailure {
                kind: OwnerTargetRetiredInitialSettlementErrorKind::WrongTerminal,
                retired,
                terminal: Some(terminal),
            });
        }

        let OwnerTargetRetiredInitialHandoff {
            owner,
            attached,
            physical_actor,
            replacement_generation,
            replacement_nonce,
        } = retired;
        let retired_generation = attached.generation();
        let reason = terminal.reason();
        drop((owner, attached, physical_actor, replacement_nonce, terminal));
        Ok(OwnerTargetRetiredInitialSettled {
            retired_generation,
            replacement_generation,
            reason,
        })
    }
    /// Consuming precommit validation for the initial physical controller.
    /// A wrong/lost executor returns the exact handoff, including both its
    /// AttachedLeg authority and acceptance receipt, without touching either.
    #[allow(
        clippy::result_large_err,
        reason = "wrong executor must return the exact non-cloneable handoff"
    )]
    pub(super) fn validate_initial_handoff_owner(
        &self,
        handoff: OwnerTargetInitialHandoff,
    ) -> Result<OwnerTargetInitialHandoff, OwnerTargetInitialHandoffOwnerFailure> {
        if let Some(block) = self.recovery_owner_block(&handoff.owner) {
            return Err(OwnerTargetInitialHandoffOwnerFailure { block, handoff });
        }
        Ok(handoff)
    }
    /// Binds the complete initial handoff to one pointer-identical physical
    /// runtime -> LegIo policy -> EstablishedLeg -> FIFO chain. Every mismatch
    /// returns the original non-cloneable handoff without mutation.
    #[allow(
        clippy::result_large_err,
        reason = "physical binding rejection must return the exact initial handoff"
    )]
    pub(super) fn bind_initial_handoff_to_physical_actor(
        &self,
        mut handoff: OwnerTargetInitialHandoff,
        runtime: &PhysicalLegRuntimeIncarnation,
        io: &LegIo,
        queue: &LegOutboundQueue,
    ) -> Result<OwnerTargetInitialHandoff, OwnerTargetInitialHandoffOwnerFailure> {
        if let Some(block) = self.recovery_owner_block(&handoff.owner) {
            return Err(OwnerTargetInitialHandoffOwnerFailure { block, handoff });
        }
        if handoff.physical_actor.is_some() {
            return Err(OwnerTargetInitialHandoffOwnerFailure {
                block: OwnerTargetInputBlock::InitialPhysicalActorAlreadyBound,
                handoff,
            });
        }
        let Some(binding) = PhysicalLegActorBinding::try_bind(runtime, io, queue) else {
            return Err(OwnerTargetInitialHandoffOwnerFailure {
                block: OwnerTargetInputBlock::WrongInitialPhysicalActor,
                handoff,
            });
        };
        if !handoff.matches_transport_identity(io.established_leg()) {
            return Err(OwnerTargetInitialHandoffOwnerFailure {
                block: OwnerTargetInputBlock::WrongInitialPhysicalActor,
                handoff,
            });
        }
        if !handoff
            .acceptance
            .as_ref()
            .is_some_and(|acceptance| acceptance.belongs_to_queue(queue))
        {
            return Err(OwnerTargetInitialHandoffOwnerFailure {
                block: OwnerTargetInputBlock::WrongAcceptanceQueue,
                handoff,
            });
        }
        handoff.physical_actor = Some(binding);
        Ok(handoff)
    }
    /// Consumes the exact acceptance receipt only after its own ordered record
    /// reports delivery through the same physical actor/FIFO binding. Pending
    /// or mismatched attempts return the original handoff; a repeat after the
    /// first settlement is an inert typed success.
    #[allow(
        clippy::result_large_err,
        reason = "settlement rejection must return exact AttachedLeg and receipt ownership"
    )]
    pub(super) fn settle_initial_acceptance_delivery(
        &self,
        mut handoff: OwnerTargetInitialHandoff,
        runtime: &PhysicalLegRuntimeIncarnation,
        io: &LegIo,
        queue: &LegOutboundQueue,
    ) -> Result<OwnerTargetInitialAcceptanceSettlement, OwnerTargetInitialHandoffOwnerFailure> {
        if let Some(block) = self.recovery_owner_block(&handoff.owner) {
            return Err(OwnerTargetInitialHandoffOwnerFailure { block, handoff });
        }
        let Some(binding) = handoff.physical_actor.as_ref() else {
            return Err(OwnerTargetInitialHandoffOwnerFailure {
                block: OwnerTargetInputBlock::InitialPhysicalActorNotBound,
                handoff,
            });
        };
        if !binding.matches(runtime, io, queue)
            || !handoff.matches_transport_identity(io.established_leg())
        {
            return Err(OwnerTargetInitialHandoffOwnerFailure {
                block: OwnerTargetInputBlock::WrongInitialPhysicalActor,
                handoff,
            });
        }
        let Some(acceptance) = handoff.acceptance.as_ref() else {
            return Ok(OwnerTargetInitialAcceptanceSettlement {
                disposition: OwnerTargetInitialAcceptanceSettlementDisposition::AlreadySettled,
                handoff,
            });
        };
        if !acceptance.belongs_to_queue(queue) {
            return Err(OwnerTargetInitialHandoffOwnerFailure {
                block: OwnerTargetInputBlock::WrongAcceptanceQueue,
                handoff,
            });
        }
        if !acceptance.was_delivered() {
            return Err(OwnerTargetInitialHandoffOwnerFailure {
                block: OwnerTargetInputBlock::InitialAcceptanceDeliveryPending,
                handoff,
            });
        }
        drop(handoff.acceptance.take());
        Ok(OwnerTargetInitialAcceptanceSettlement {
            disposition: OwnerTargetInitialAcceptanceSettlementDisposition::SettledNow,
            handoff,
        })
    }
    fn recovery_admission_block(&self) -> Option<OwnerTargetInputBlock> {
        if !self.attach_barrier_pending {
            return Some(OwnerTargetInputBlock::AttachBarrierNotPending);
        }
        if self.aborted_effects.is_some() {
            return Some(OwnerTargetInputBlock::AbortedEffectInvariant);
        }
        if !self.pending_effects.is_empty() || self.outstanding_resume.is_some() {
            return Some(OwnerTargetInputBlock::PendingEffectTurn);
        }
        None
    }
    fn recovery_admission_block_allow_pending_turn(&self) -> Option<OwnerTargetInputBlock> {
        if !self.attach_barrier_pending {
            return Some(OwnerTargetInputBlock::AttachBarrierNotPending);
        }
        self.aborted_effects
            .is_some()
            .then_some(OwnerTargetInputBlock::AbortedEffectInvariant)
    }
}

#[derive(Clone, Copy)]
enum TargetSourceFact {
    Data,
    Eof,
}

fn command_flow_id(command: &SessionOwnerCommand) -> Option<SessionFlowId> {
    match command {
        SessionOwnerCommand::Event(event)
        | SessionOwnerCommand::Driver(DriverInput::Control(event)) => event_flow_id(event),
        SessionOwnerCommand::Driver(DriverInput::Data(data)) => Some(data.flow().flow_id()),
        SessionOwnerCommand::ClientDriver(input) => match input.as_inner() {
            DriverInput::Control(event) => event_flow_id(event),
            DriverInput::Data(data) => Some(data.flow().flow_id()),
        },
    }
}

fn event_flow_id(event: &SessionEvent) -> Option<SessionFlowId> {
    match event {
        SessionEvent::LocalData { flow, .. }
        | SessionEvent::LocalClose { flow }
        | SessionEvent::LocalReset { flow, .. } => Some(flow.flow_id()),
        SessionEvent::PeerOpenResolved { request, .. } => Some(request.flow_id()),
        SessionEvent::PeerFrame { frame, .. } => match frame.record() {
            Record::Open { flow_id, .. }
            | Record::Data { flow_id, .. }
            | Record::OpenResult { flow_id, .. }
            | Record::Ack { flow_id, .. }
            | Record::Close { flow_id, .. }
            | Record::Reset { flow_id, .. } => Some(*flow_id),
            Record::Attach { .. }
            | Record::AttachAccepted { .. }
            | Record::AttachGenerationStatus { .. } => None,
        },
        SessionEvent::SinkAccepted { offer, .. } | SessionEvent::SinkAbandoned { offer } => {
            Some(offer.flow_id())
        }
        SessionEvent::SinkHalfClosed { completion }
        | SessionEvent::SinkHalfCloseFailed { completion } => Some(completion.flow_id()),
        SessionEvent::TerminalGraceExpired { terminal } => Some(terminal.flow_id()),
        SessionEvent::ReplacementAttached { .. }
        | SessionEvent::ReplacementCaughtUp { .. }
        | SessionEvent::LegLost { .. }
        | SessionEvent::ResumeGraceExpired { .. }
        | SessionEvent::LocalOpen { .. } => None,
    }
}

const fn incremental_effect_burst(effect: &SessionEffect) -> usize {
    match effect {
        SessionEffect::PeerOpenRequested { .. }
        | SessionEffect::OfferToSink { .. }
        | SessionEffect::HalfCloseSink { .. } => MAX_INCREMENTAL_EFFECT_BURST,
        SessionEffect::LegActivated { .. }
        | SessionEffect::ResumeGraceStarted { .. }
        | SessionEffect::SessionExpired
        | SessionEffect::ReplayStored { .. }
        | SessionEffect::ReplayAcknowledged { .. }
        | SessionEffect::Transmit(_)
        | SessionEffect::LocalFlowOpened { .. }
        | SessionEffect::LocalOpenResolved { .. }
        | SessionEffect::PeerReset { .. }
        | SessionEffect::FlowFinished { .. } => 0,
    }
}

impl<T> fmt::Debug for OwnerTargetExecutor<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OwnerTargetExecutor")
            .field("config", &self.config)
            .field("snapshot", &self.snapshot_without_supervisor())
            .finish_non_exhaustive()
    }
}

impl<T> OwnerTargetExecutor<T> {
    fn snapshot_without_supervisor(&self) -> (usize, usize, usize, usize) {
        (
            self.flows.len(),
            self.target_tombstones.len(),
            self.flows
                .values()
                .filter(|flow| matches!(flow.pending_sink, Some(PendingTargetSink::Write(_))))
                .count(),
            self.flows.values().filter(|flow| flow.join_pending).count(),
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OwnerTargetInputBlock {
    EffectStorage(crate::resumable::effect_storage::EffectStorageError),
    ReadyReceiptInvariant,
    PendingEffectTurn,
    NoPendingEffectTurn,
    InvalidEffectResume { expected: u64, actual: u64 },
    AbortedEffectInvariant,
    AttachBarrierPending,
    AttachBarrierNotPending,
    OwnerTargetExecutorLost,
    WrongOwnerTargetExecutor,
    InitialPhysicalActorAlreadyBound,
    InitialPhysicalActorNotBound,
    WrongInitialPhysicalActor,
    InitialAcceptanceDeliveryPending,
    InitialAcceptanceSettlementPending,
    WrongLegTerminal,
    WrongTerminalQueue,
    MissingAttachedRecovery,
    AcceptanceQueueLost,
    InitialAcceptanceQueueInvariant,
    AcceptanceQueueStillLive,
    WrongAcceptanceQueue,
    WrongStatusQueue,
    StatusNotDelivered,
    StatusPublicationStillLive,
    PendingTargetCompletion { flow_id: SessionFlowId },
    ReducerAdmission(ReducerAdmissionBlock),
}
#[derive(Debug, Error)]
pub(crate) enum OwnerTargetError {
    #[error("owner effect backing preparation failed: {0}")]
    EffectStorage(#[from] crate::resumable::effect_storage::EffectStorageError),
    #[error("Bound source work requires its selected ready-service transaction")]
    BoundSourceRequiresSelectedService,
    #[error("session supervisor rejected owner Target work: {0}")]
    Supervisor(#[from] SessionSupervisorError),
    #[error("Target I/O rejected owner work: {0}")]
    Target(#[from] TargetIoError),
    #[error("Target-read ownership port rejected owner work: {0}")]
    FlowPort(#[from] FlowPortError),
    #[error("owner Target effect step counter overflowed")]
    EffectStepOverflow,
    #[error("owner Target has a pending effect turn; consume its exact resume capability first")]
    PendingEffectTurn,
    #[error("owner Target attach acceptance/recovery barrier is still pending")]
    AttachBarrierPending,
    #[error("owner Target rejected an effect resume before consuming it: {block:?}")]
    RejectedEffectResume {
        block: OwnerTargetInputBlock,
        resume: Box<OwnerTargetResume>,
    },
    #[error("owner Target attempted to mint a second effect resume capability")]
    DuplicateEffectResume,
    #[error("owner Target effect resume ID space is exhausted")]
    EffectResumeIdExhausted,
    #[error("owner Target retained an effect-capacity invariant failure ({attempted} > {max})")]
    PendingEffectCapacityInvariant { max: usize, attempted: usize },
    #[error("owner Target retained a reducer effect burst {actual} above derived bound {max}")]
    GeneratedEffectBoundExceeded { max: usize, actual: usize },
    #[error("owner Target is fail-closed with retained aborted effects")]
    AbortedEffectInvariant,
    #[error("owner Target could not retain a second post-commit observation after effect failure")]
    PostCommitObservationRetentionInvariant {
        error: Box<OwnerTargetError>,
        observations: Box<OwnerTargetReducerObservations>,
        effects: crate::resumable::effect_storage::SessionEffectBatch,
    },
    #[error("owner Target could not retain a second exact attached-recovery cursor after failure")]
    PostCommitRecoveryRetentionInvariant {
        error: Box<OwnerTargetError>,
        recovery: Box<OwnerTargetRecoveryDrain>,
    },
    #[error("owner Target produced a second {kind:?} observation in one fixed-size turn")]
    PostCommitObservationConflict {
        kind: OwnerTargetObservationKind,
        observations: Box<OwnerTargetReducerObservations>,
    },
    #[error("owner Target rejected a command before consuming it: {block:?}")]
    RejectedCommand {
        block: OwnerTargetInputBlock,
        command: Box<SessionOwnerCommand>,
    },
    #[error("owner Target rejected an event before consuming it: {block:?}")]
    RejectedEvent {
        block: OwnerTargetInputBlock,
        event: Box<SessionEvent>,
    },
    #[error("owner Target rejected a legless high-water timer without consuming it")]
    RejectedLeglessHighWater {
        high_water: Box<OwnerLeglessHighWater>,
    },
    #[error("owner Target rejected an exact owner ATTACH before consuming it: {block:?}")]
    RejectedOwnerAttach {
        block: OwnerTargetInputBlock,
        received: Box<LegBoundFrame>,
    },
    #[error("owner Target rejected a registered standby ATTACH without consuming it: {source}")]
    RejectedRegisteredOwnerAttach {
        source: OwnerRegisteredAttachGateError,
        received: Box<LegBoundFrame>,
    },
    #[error("owner Target rejected a catch-up ATTACH without consuming it: {source}")]
    RejectedCatchUpOwnerAttach {
        source: SessionSupervisorError,
        received: Box<LegBoundFrame>,
    },
    #[error(
        "registered owner ATTACH terminal transition failed without consuming its slot: {source}"
    )]
    RegisteredOwnerAttachTerminalTransition {
        source: LegTerminalTransitionError,
        received: Box<LegBoundFrame>,
    },
    #[error(
        "registered owner ATTACH could not restore its exact queue reservation; state remains fail-closed"
    )]
    RegisteredOwnerAttachReservationInvariant,
    #[error("owner attach rejection retained with exact registered restore failure: {0:?}")]
    RegisteredOwnerAttachRestoreFailure(Box<effect_storage::OwnerRegisteredRestoreFailure>),
    #[error("owner ATTACH terminal transition failed: {source}")]
    OwnerAttachTerminalTransition {
        source: LegTerminalTransitionError,
        received: Box<LegBoundFrame>,
    },
    #[error("owner ATTACH arrived on a terminal transport endpoint")]
    OwnerAttachEndpointLost { received: Box<LegBoundFrame> },
    #[error("owner ATTACH could not reserve its exact awaiting acceptance queue: {source}")]
    RejectedOwnerAttachQueueReservation {
        source: AttachAcceptanceReserveError,
        received: Box<LegBoundFrame>,
    },
    #[error(
        "owner ATTACH could not restore its exact queue reservation; state remains fail-closed"
    )]
    OwnerAttachReservationInvariant,
    #[error("owner catch-up rejection retained with its unreleased acceptance reservation: {0:?}")]
    CatchUpOwnerAttachReleaseFailure(Box<effect_storage::OwnerCatchUpReleaseFailure>),
    #[error(
        "owner Target rejected catch-up arm before consuming its exact status receipt: {block:?}"
    )]
    RejectedOwnerAttachStatusArm {
        block: OwnerTargetInputBlock,
        publication: Box<EnqueuedOwnerAttachStatus>,
    },
    #[error("owner Target rejected live pending status abandonment: {block:?}")]
    RejectedPendingOwnerAttachStatusAbandon {
        block: OwnerTargetInputBlock,
        publication: Box<PendingOwnerAttachStatusPublication>,
    },
    #[error("owner Target rejected pending status permit abandonment: {source}")]
    PendingOwnerAttachStatusAbandonRejected {
        source: SessionSupervisorError,
        publication: Box<PendingOwnerAttachStatusPublication>,
    },
    #[error("owner Target rejected live enqueued status abandonment: {block:?}")]
    RejectedEnqueuedOwnerAttachStatusAbandon {
        block: OwnerTargetInputBlock,
        publication: Box<EnqueuedOwnerAttachStatus>,
    },
    #[error("owner Target rejected enqueued status permit abandonment: {source}")]
    EnqueuedOwnerAttachStatusAbandonRejected {
        source: SessionSupervisorError,
        publication: Box<EnqueuedOwnerAttachStatus>,
    },
    #[error("owner Target rejected exact standby authentication before consuming it: {block:?}")]
    RejectedOwnerStandbyAuthentication {
        block: OwnerTargetInputBlock,
        inbound: Box<BoundInbound>,
    },
    #[error(
        "owner Target rejected pending attach loss without consuming either authority: {block:?}"
    )]
    RejectedPendingAttachLoss {
        block: OwnerTargetInputBlock,
        pending: Box<PendingOwnerTargetAttachPublication>,
        terminal: Box<ExactLegTerminal>,
    },
    #[error(
        "owner Target rejected enqueued attach terminalization without consuming either authority: {block:?}"
    )]
    RejectedAttachedRecoveryTerminalization {
        block: OwnerTargetInputBlock,
        recovery: Box<EnqueuedOwnerTargetAttach>,
        terminal: Box<ExactLegTerminal>,
    },
    #[error(
        "owner Target rejected retained recovery-drain terminalization without consuming either authority: {block:?}"
    )]
    RejectedAttachedRecoveryDrainTerminalization {
        block: OwnerTargetInputBlock,
        drain: Box<OwnerTargetRecoveryDrain>,
        terminal: Box<ExactLegTerminal>,
    },
    #[error(
        "owner Target rejected recovery-resume terminalization without consuming either authority: {block:?}"
    )]
    RejectedAttachedRecoveryResumeTerminalization {
        block: OwnerTargetInputBlock,
        recovery: Box<OwnerTargetRecoveryResume>,
        terminal: Box<ExactLegTerminal>,
    },
    #[error("owner Target rejected terminal recovery discard without consuming it: {block:?}")]
    RejectedTerminalAttachRecoveryDiscard {
        block: OwnerTargetInputBlock,
        pending: Box<PendingOwnerAttachLoss>,
    },
    #[error(
        "owner Target terminal recovery cleanup failed with the exact pending loss quarantined: {source}"
    )]
    TerminalAttachRecoveryDiscardInvariant {
        source: SessionSupervisorError,
        pending: Box<PendingOwnerAttachLoss>,
    },
    #[error(
        "owner Target retained terminal-loss effect backing after reclamation failed: {source}"
    )]
    TerminalAttachRecoveryStorageInvariant {
        source: SessionSupervisorError,
        effects: crate::resumable::effect_storage::SessionEffectBatch,
        pending: Box<PendingOwnerAttachLoss>,
    },
    #[error(
        "owner Target pending attach loss emitted an invalid effect set for generation {generation:?}"
    )]
    PendingAttachLossEffectInvariant {
        generation: LegGeneration,
        effects: Vec<SessionEffect>,
        pending: Box<PendingOwnerAttachLoss>,
    },
    #[error("owner Target rejected post-acceptance recovery before consuming it: {block:?}")]
    RejectedAttachedRecovery {
        block: OwnerTargetInputBlock,
        recovery: Box<EnqueuedOwnerTargetAttach>,
    },
    #[error("owner Target rejected a retained recovery drain before consuming it: {block:?}")]
    RejectedAttachedRecoveryDrain {
        block: OwnerTargetInputBlock,
        drain: Box<OwnerTargetRecoveryDrain>,
    },
    #[error("owner Target rejected a post-attach recovery continuation: {block:?}")]
    RejectedAttachedRecoveryResume {
        block: OwnerTargetInputBlock,
        recovery: Box<OwnerTargetRecoveryResume>,
    },
    #[error("owner Target recovery queue is blocked: {kind:?}")]
    AttachedRecoveryQueueBlocked {
        kind: LegOutboundQueueErrorKind,
        drain: Box<OwnerTargetRecoveryDrain>,
    },
    #[error("owner Target recovery output ordering invariant failed")]
    AttachedRecoveryOutputInvariant {
        drain: Box<OwnerTargetRecoveryDrain>,
    },
    #[error("legacy attached-recovery adapter observed an unsupported reducer fact")]
    AttachedRecoveryObservationInvariant,
    #[error("peer OPEN effect for flow {flow_id:?} was duplicate or mismatched")]
    DuplicateOrMismatchedPeerOpen { flow_id: SessionFlowId },
    #[error("owner Target has no live flow {flow_id:?}")]
    UnknownTargetFlow { flow_id: SessionFlowId },
    #[error("owner Target flow {flow_id:?} has no pending write")]
    NoPendingTargetWrite { flow_id: SessionFlowId },
    #[error("owner Target flow {flow_id:?} already has a pending write")]
    DuplicatePendingTargetWrite { flow_id: SessionFlowId },
    #[error("owner Target flow {flow_id:?} already owns a pending Target completion")]
    DuplicatePendingTargetCompletion { flow_id: SessionFlowId },
    #[error("owner Target flow {flow_id:?} has a pending Target completion")]
    PendingTargetCompletion { flow_id: SessionFlowId },
    #[error("owner Target flow {flow_id:?} lost its pending Target completion")]
    MissingPendingTargetCompletion { flow_id: SessionFlowId },
    #[error(
        "session supervisor returned a different Target completion command for flow {flow_id:?}"
    )]
    CarriedTargetCompletionMismatch { flow_id: SessionFlowId },
    #[error("Target completion for flow {flow_id:?} hit unexpected reducer admission {block:?}")]
    UnexpectedTargetCompletionAdmission {
        flow_id: SessionFlowId,
        block: ReducerAdmissionBlock,
    },
    #[error("owner Target flow {flow_id:?} cannot half-close before its pending write")]
    HalfCloseBeforePendingWrite { flow_id: SessionFlowId },
    #[error("owner Target flow {flow_id:?} received an empty sink offer")]
    EmptyTargetOffer { flow_id: SessionFlowId },
    #[error(
        "owner Target flow {flow_id:?} reported {accepted} accepted bytes after only {exposed} bytes were exposed from a {offered}-byte offer"
    )]
    InvalidTargetWriteAcceptance {
        flow_id: SessionFlowId,
        accepted: usize,
        exposed: usize,
        offered: usize,
    },
    #[error("owner Target flow {flow_id:?} has no pending join")]
    NoPendingTargetJoin { flow_id: SessionFlowId },
    #[error("owner Target flow {flow_id:?} remains blocked on bounded join ownership")]
    TargetJoinStillPending { flow_id: SessionFlowId },
    #[error("owner Target flow {flow_id:?} produced an unclassified pending join")]
    UnexpectedPendingTargetJoin { flow_id: SessionFlowId },
    #[error("owner Target flow {flow_id:?} already has a recorded terminal tombstone")]
    DuplicateTargetTombstone { flow_id: SessionFlowId },
    #[error("owner Target flow {flow_id:?} has no recorded terminal tombstone")]
    UnrecordedTargetTombstone { flow_id: SessionFlowId },
    #[error("owner Target flow {flow_id:?} lost its recorded terminal tombstone")]
    MissingTargetTombstone { flow_id: SessionFlowId },
    #[error("owner Target flow {flow_id:?} retired an unrecorded {actual:?} tombstone")]
    UnexpectedTargetTombstone {
        flow_id: SessionFlowId,
        actual: TargetTerminalKind,
    },
    #[error("owner Target flow {flow_id:?} tombstone kind changed from {expected:?} to {actual:?}")]
    TargetTombstoneKindMismatch {
        flow_id: SessionFlowId,
        expected: TargetTerminalKind,
        actual: TargetTerminalKind,
    },
    #[error("an internal replay receipt escaped the session supervisor")]
    InternalReceiptEscapedSupervisor,
    #[error("a client-only session effect reached the owner Target executor")]
    ClientOnlyEffectAtOwner,
    #[error("owner Target flow {flow_id:?} has no byte-owned read source")]
    MissingTargetReadSource { flow_id: SessionFlowId },
    #[error("owner Target flow {flow_id:?} committed a read without a driver input")]
    MissingTargetReadDriverInput { flow_id: SessionFlowId },
    #[error("owner Target flow {flow_id:?} dequeued a different source fact after read")]
    UnexpectedTargetReadDriverInput { flow_id: SessionFlowId },
    #[error("owner Target flow {flow_id:?} committed a Target failure without a driver input")]
    MissingTargetFailureDriverInput { flow_id: SessionFlowId },
    #[error("owner Target flow {flow_id:?} dequeued a different driver input after Target failure")]
    UnexpectedTargetFailureDriverInput { flow_id: SessionFlowId },
}

const fn open_failure_result(failure: TargetOpenFailure) -> OpenResultCode {
    match failure {
        TargetOpenFailure::Refused => OpenResultCode::TargetRefused,
        TargetOpenFailure::Unreachable => OpenResultCode::TargetUnreachable,
        TargetOpenFailure::TimedOut => OpenResultCode::TimedOut,
        TargetOpenFailure::ResourceExhausted => OpenResultCode::ResourceExhausted,
        TargetOpenFailure::PolicyDenied => OpenResultCode::PolicyDenied,
        TargetOpenFailure::Internal => OpenResultCode::Internal,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::owned_upstream::leg::{
        BoundInbound, LegProvenanceError, LegTransportReporter, LegTransportTerminalReason,
    };
    use crate::owned_upstream::leg_io::{
        EncodedLegTransport, EncodedLegTransportError, LegEndpointRole, LegIo, LegIoConfigError,
        LegIoLimits, LegOutboundQueue, OrderedDeliveryToken,
    };
    use crate::owned_upstream::standby::{
        ExactAuthenticatedStandby, OwnerStandbyAdmission, OwnerStandbySlot,
    };
    use crate::owned_upstream::target::{
        MemoryTarget, MemoryTargetConfig, MemoryWriteDirective, TargetExpireCompletion,
        TargetFailure, TargetOpenFailure, TargetReadCompletion, TargetReadFeedCompletion,
        TargetReadiness,
    };
    use crate::owned_upstream::two_leg::{EncodedDelivery, LegId, SimTime, WireLane, WireRoute};
    use crate::resumable::{
        ATTACH_ACCEPTED_ENCODED_BYTES, AttachAlpn, AttachAuthority, AttachCredentials, AttachNonce,
        AttachPolicy, AttachRequest, AttachTransportBinding, ByteOffset, DevicePrincipal,
        DeviceSecret, Direction, FeatureOffer, FeatureSet, LegGeneration, OwnerIdentity,
        ReceiveBudgetLimits, Record, ReplayBudgetLimits, ResetReason, ResumeSecret,
        SESSION_PROTOCOL_VERSION, STANDBY_CONTROL_V1, SessionConfig, SessionId, SessionModel,
        SessionPhase, SessionRole, StandbyNonce, TcpWindowLimits, TlsExporterBinding, VersionRange,
    };
    use crate::shared::TargetAddr;
    use bytes::Bytes;
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

    const STANDBY_FEATURES: u64 = STANDBY_CONTROL_V1.bits() | 0b0111;
    mod owner_data_retention;
    mod r7_attached_recovery_pages;
    mod r7_initial_bootstrap_handoff;
    mod r7_nested_abort;
    mod r7_observed_pages;
    mod r7_owner_ack;
    mod target_overacceptance;

    #[test]
    fn source_provenance_closes_generic_owner_commit_and_raw_client_catch_up() {
        let owner_target_source = include_str!("owner_target.rs");
        let supervisor_source = include_str!("supervisor.rs");
        let leg_source = include_str!("leg.rs");
        let leg_io_source = include_str!("leg_io.rs");
        let standby_source = include_str!("standby.rs");
        let two_leg_harness_source = include_str!("two_leg_harness.rs");
        let owner_target_production = owner_target_source
            .split_once("#[cfg(test)]\nmod tests")
            .unwrap()
            .0;
        let supervisor_production = supervisor_source
            .split_once("#[cfg(test)]\nmod tests")
            .unwrap()
            .0;
        let leg_io_production = leg_io_source
            .split_once("#[cfg(test)]\nmod tests")
            .unwrap()
            .0;
        // `leg.rs` has no inline test module; production-only raw entries are
        // distinguished below by their exact visibility and `cfg(test)` seal.
        let leg_production = leg_source;
        let standby_production = standby_source
            .split_once("#[cfg(test)]\npub(super) mod tests")
            .unwrap()
            .0;
        let two_leg_harness_production = two_leg_harness_source
            .split_once("#[cfg(test)]\nmod tests")
            .unwrap()
            .0;

        let legacy_generic_entry = ["fn accept_owner_", "attach("].concat();
        assert!(!owner_target_source.contains(&legacy_generic_entry));
        assert!(!supervisor_source.contains(&legacy_generic_entry));
        assert!(
            owner_target_production
                .contains("#[cfg(test)]\n    pub(crate) fn accept_owner_attach_for_test")
        );
        assert!(
            supervisor_production
                .contains("#[cfg(test)]\n    pub(crate) fn accept_owner_attach_for_test")
        );
        assert_eq!(
            owner_target_production
                .matches(".accept_owner_attach_for_test(")
                .count(),
            1
        );
        assert_eq!(
            supervisor_production
                .matches(".transact_owner_attach_from_supervisor(")
                .count(),
            1
        );
        assert_eq!(
            owner_target_production
                .matches("transact_owner_attach_from_supervisor")
                .count(),
            0
        );
        assert_eq!(
            leg_source
                .matches("fn transact_owner_attach_from_supervisor(")
                .count(),
            1
        );
        assert!(leg_source.contains("#[cfg(test)]\n    pub(crate) fn begin_catch_up("));
        assert!(
            leg_source.contains("#[cfg(test)]\n    pub(crate) fn request(&self) -> AttachRequest")
        );
        assert!(leg_source.contains(
            "#[cfg(test)]\n    pub(crate) fn transport_binding(&self) -> AttachTransportBinding"
        ));
        assert!(leg_source.contains("#[cfg(test)]\n    pub(crate) fn begin_attach("));
        assert!(leg_source.contains("#[cfg(test)]\n    pub(crate) fn validate_response("));
        assert!(leg_source.contains("pub(crate) fn begin_initial_attach("));
        assert!(
            leg_source.contains(") -> Result<PendingInitialAttach, InitialAttachBeginFailure>")
        );
        assert!(!two_leg_harness_production.contains(".begin_attach("));
        assert!(
            supervisor_production.contains("#[cfg(test)]\n    pub(crate) fn install_attached_leg(")
        );
        assert!(
            leg_io_production.contains("#[cfg(test)]\n    pub(crate) fn push_attach_acceptance(")
        );
        assert!(standby_production.contains("struct RegisteredPendingAttachAuthority {"));
        assert!(leg_production.contains(
            "pub(super) fn for_registered_standby(authority: RegisteredPendingAttachAuthority)"
        ));
        assert!(!leg_production.contains("fn for_registered_standby(\n        seal: LegSeal,"));
        assert!(leg_production.contains("gate: RegisteredAttachResponseGate"));
        assert!(leg_production.contains("gate: RegisteredCatchUpBeginGate"));
        assert!(leg_production.contains("gate: RegisteredCatchUpAssemblyGate"));
        assert!(leg_production.contains("gate: RegisteredCatchUpResponseGate"));
        assert!(!leg_production.contains("pub(super) fn begin_signed_catch_up("));
        assert!(!leg_production.contains(
            "pub(super) fn into_parts(self) -> (LegSeal, PendingCatchUpAttach, AttachRequest, Frame)"
        ));
        assert!(leg_source.contains(
            "#[cfg(test)]\n    pub(crate) fn validate_response(\n        self,\n        response: LegBoundFrame,\n    ) -> Result<CaughtUpAttachedLeg"
        ));
        assert!(!leg_production.contains(
            "pub(super) fn validate_response_preserving(\n        self,\n        response: LegBoundFrame,\n    ) -> Result<CaughtUpAttachedLeg"
        ));
        assert!(
            supervisor_production
                .contains("#[cfg(test)]\n    pub(crate) fn install_caught_up_leg(")
        );
    }

    #[test]
    fn source_provenance_closes_split_initial_client_bootstrap() {
        let controller_source = include_str!("client_controller.rs");
        let leg_io_source = include_str!("leg_io.rs");
        let supervisor_source = include_str!("supervisor.rs");
        let controller_production = controller_source
            .split_once("#[cfg(test)]\nmod tests")
            .unwrap()
            .0;
        let supervisor_production = supervisor_source
            .split_once("#[cfg(test)]\nmod tests")
            .unwrap()
            .0;
        let leg_io_production = leg_io_source
            .split_once("#[cfg(test)]\nmod tests")
            .unwrap()
            .0;

        assert!(
            !supervisor_production.contains("pub(crate) fn bootstrap_client(")
                && !supervisor_production.contains("pub(crate) fn into_controller("),
            "borrowed bootstrap followed by into_controller leaves raw LegIo/queue authority with the caller between successful activation and controller publication"
        );
        assert_eq!(
            controller_production
                .matches("pub(crate) fn bootstrap_initial(")
                .count(),
            1
        );
        assert!(!controller_production.contains("fn from_initial_parts("));
        assert!(
            controller_production.contains(
                "pub(super) struct ClientControllerBootstrapGate {\n    _private: (),\n}"
            )
        );
        assert_eq!(
            controller_production
                .matches("ClientControllerBootstrapGate::mint()")
                .count(),
            1
        );
        assert!(!leg_io_production.contains("pub(super) fn activate_on_queue("));
        assert!(leg_io_production.contains("_gate: &ClientControllerBootstrapGate,"));
        assert!(supervisor_production.contains("_gate: &ClientControllerBootstrapGate,"));
    }

    #[test]
    fn source_provenance_closes_client_ack_observation_command_bypass() {
        let controller_source = include_str!("client_controller.rs");
        let supervisor_source = include_str!("supervisor.rs");
        let tcp_source = include_str!("tcp.rs");
        let harness_source = include_str!("two_leg_harness.rs");
        let controller_production = controller_source
            .split_once("#[cfg(test)]\nmod tests")
            .unwrap()
            .0;
        let supervisor_production = supervisor_source
            .split_once("#[cfg(test)]\nmod tests")
            .unwrap()
            .0;
        let tcp_production = tcp_source.split_once("#[cfg(test)]\nmod tests").unwrap().0;
        let legacy_driver_adapter = controller_production
            .split_once("pub(super) fn apply_driver_input(")
            .expect("legacy driver adapter remains production-reachable")
            .1
            .split_once("pub(super) fn apply_driver_input_observed(")
            .expect("observed driver adapter follows the legacy adapter")
            .0;
        let observed_driver_adapter = controller_production
            .split_once("pub(super) fn apply_driver_input_observed(")
            .expect("observed driver adapter remains production-reachable")
            .1
            .split_once("pub(crate) fn expire_terminal_grace(")
            .expect("terminal-grace API follows the observed adapter")
            .0;

        assert!(!controller_production.contains("fn apply_driver_command("));
        assert!(controller_production.contains("pub(super) fn apply_driver_input("));
        assert!(
            controller_production.contains("input: ClientControllerDriverInput,"),
            "the controller must accept only a driver-minted opaque input"
        );
        assert!(
            !controller_production.contains("input: DriverInput,"),
            "a sibling-forgeable DriverInput would restore the PeerFrame ACK bypass"
        );
        assert_eq!(
            controller_production
                .matches("let command = SessionOwnerCommand::ClientDriver(input);")
                .count(),
            1,
            "the controller must carry the opaque wrapper into the supervisor unchanged"
        );
        assert_eq!(
            legacy_driver_adapter
                .matches("SessionOwnerCommand::ClientDriver(input) =>")
                .count(),
            1,
            "the legacy adapter must return the same rejected opaque wrapper exactly once"
        );
        assert_eq!(
            observed_driver_adapter
                .matches("SessionOwnerCommand::ClientDriver(input) =>")
                .count(),
            1,
            "the observed adapter must return the same rejected opaque wrapper exactly once"
        );
        assert_eq!(
            controller_production.matches("input.into_inner").count(),
            0,
            "the controller must never expose a raw DriverInput while carrying typed provenance"
        );
        assert!(
            tcp_production
                .contains("pub(crate) struct ClientControllerDriverInput(Box<DriverInput>);")
        );
        assert!(tcp_production.contains("pub(super) fn try_recv_next_for_client_controller("));
        assert_eq!(
            tcp_production
                .matches(".map(ClientControllerDriverInput::from_driver_dequeue)")
                .count(),
            1,
            "only the real driver dequeue may mint controller input provenance"
        );
        assert_eq!(
            tcp_production
                .matches("fn from_driver_dequeue(input: DriverInput) -> Self")
                .count(),
            1,
            "the initial wrapper constructor remains private to the TCP driver module"
        );
        assert!(tcp_production.contains(
            "pub(super) fn from_rejected_driver(\n        input: DriverInput,\n        _gate: &ClientControllerDriverReducerGate,"
        ));
        assert!(supervisor_production.contains(
            "pub(super) struct ClientControllerDriverReducerGate {\n    _private: (),\n}"
        ));
        assert!(!supervisor_production.contains("pub(super) fn mint() -> Self"));
        assert_eq!(
            supervisor_production
                .matches("ClientControllerDriverReducerGate::mint()")
                .count(),
            1,
            "only the supervisor may mint the raw-driver extraction/rewrap gate"
        );
        assert_eq!(
            supervisor_production
                .matches("input.into_inner(&gate)")
                .count(),
            1,
            "only the gated supervisor command path may consume the opaque wrapper"
        );
        assert_eq!(
            supervisor_production
                .matches("ClientControllerDriverInput::from_rejected_driver(")
                .count(),
            1,
            "only the gated supervisor rejection path may rewrap raw driver ownership"
        );
        assert!(harness_source.contains(".try_recv_next_for_client_controller()"));
        assert_eq!(
            controller_production
                .matches(".apply_current_event_observed(")
                .count(),
            1
        );
        assert!(supervisor_production.contains("_gate: &ClientControllerObservedApplyGate,"));
    }

    #[derive(Default)]
    struct CapturingTransport {
        frames: Vec<Bytes>,
    }

    impl EncodedLegTransport for CapturingTransport {
        fn send_encoded(
            &mut self,
            _now: SimTime,
            _route: WireRoute,
            _lane: WireLane,
            bytes: Bytes,
        ) -> Result<(), EncodedLegTransportError> {
            self.frames.push(bytes);
            Ok(())
        }

        fn send_attach_ordered_encoded(
            &mut self,
            _now: SimTime,
            _route: WireRoute,
            _lane: WireLane,
            bytes: Bytes,
            completion: OrderedDeliveryToken,
        ) -> Result<(), EncodedLegTransportError> {
            self.frames.push(bytes);
            completion.mark_delivered();
            Ok(())
        }
    }

    fn credentials() -> AttachCredentials {
        AttachCredentials::new(
            DeviceSecret::new([0xde; 32]).unwrap(),
            ResumeSecret::new([0xad; 32]).unwrap(),
        )
        .unwrap()
    }

    fn binding(exporter: u8) -> AttachTransportBinding {
        AttachTransportBinding::new(
            OwnerIdentity::new([0x31; 32]).unwrap(),
            AttachAlpn::new(b"mini-vpn-owned/1").unwrap(),
            TlsExporterBinding::new([exporter; 32]).unwrap(),
            DevicePrincipal::new([0x53; 16]).unwrap(),
        )
    }

    fn standby_authority(current_generation: u64) -> AttachAuthority {
        AttachAuthority::new(
            SessionId::new([0x11; 16]).unwrap(),
            LegGeneration::new(current_generation).unwrap(),
            credentials(),
            AttachPolicy::new(
                OwnerIdentity::new([0x31; 32]).unwrap(),
                AttachAlpn::new(b"mini-vpn-owned/1").unwrap(),
                DevicePrincipal::new([0x53; 16]).unwrap(),
                SESSION_PROTOCOL_VERSION,
                STANDBY_FEATURES,
            )
            .unwrap(),
        )
    }

    fn standby_attach_request(generation: u64, nonce: u8) -> AttachRequest {
        AttachRequest::new(
            SessionId::new([0x11; 16]).unwrap(),
            LegGeneration::new(generation).unwrap(),
            AttachNonce::new([nonce; 16]).unwrap(),
            VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION).unwrap(),
            FeatureOffer::new(STANDBY_FEATURES, STANDBY_CONTROL_V1.bits()).unwrap(),
        )
    }

    fn bootstrap_standby_executor() -> (OwnerTargetExecutor<MemoryTarget>, AttachedLeg) {
        let transport = binding(0x42);
        let owner_leg = EstablishedLeg::for_authenticated_transport(transport);
        let request = standby_attach_request(2, 0x22);
        let proof = credentials().prove(&request, &transport).unwrap();
        let mut pending = SessionSupervisor::prepare_owner_unproven_for_test(
            session_config(),
            standby_authority(1),
        );
        let mut queue = LegOutboundQueue::for_leg(&owner_leg, 4, 4_096).unwrap();
        let (supervisor, active) = pending
            .accept(
                &owner_leg,
                owner_leg.bind_received_frame(request.to_attach_frame(proof)),
                &mut queue,
            )
            .unwrap()
            .enqueue_acceptance(&mut queue)
            .unwrap()
            .into_parts();
        let executor = OwnerTargetExecutor::new(
            OwnerTargetConfig::new(128, 1_024, 64).unwrap(),
            supervisor,
            MemoryTarget::new(MemoryTargetConfig::new(4, 256, 512, 128, 128, 16).unwrap()),
            source_config(),
        )
        .unwrap();
        (executor, active)
    }

    fn classified_standby_inbound(
        active: &AttachedLeg,
        candidate: &mut LegIo,
        nonce: u8,
    ) -> BoundInbound {
        let request = active
            .standby_registration_request(StandbyNonce::new([nonce; 16]).unwrap())
            .unwrap();
        let proof = credentials()
            .prove_standby_registration(
                &request,
                &candidate.established_leg().standby_transport_binding(),
            )
            .unwrap();
        let frame = request.to_frame(proof).unwrap();
        let delivery = EncodedDelivery::from_memory_ingress(
            candidate.inbound_route(),
            WireLane::Control,
            frame
                .encode(FeatureSet::new(STANDBY_FEATURES))
                .unwrap()
                .to_vec(),
            1,
            0,
        )
        .unwrap();
        candidate
            .receive_classified_delivery(delivery, FeatureSet::new(STANDBY_FEATURES))
            .unwrap()
    }

    fn owner_standby_leg(exporter: u8) -> LegIo {
        LegIo::for_authenticated_transport(
            LegId::B,
            LegEndpointRole::Owner,
            binding(exporter),
            LegIoLimits::new(4_096, 8, 32_768, 8, 32_768).unwrap(),
        )
    }

    fn registered_owner_slot(
        exporter: u8,
    ) -> (
        OwnerTargetExecutor<MemoryTarget>,
        LegIo,
        OwnerStandbySlot,
        LegOutboundQueue,
    ) {
        let (executor, active) = bootstrap_standby_executor();
        let mut candidate = owner_standby_leg(exporter);
        let inbound = classified_standby_inbound(&active, &mut candidate, 0x31);
        let authenticated = executor
            .authenticate_owner_standby(&active, candidate.established_leg(), inbound)
            .unwrap();
        let mut queue = LegOutboundQueue::for_leg(candidate.established_leg(), 8, 32_768).unwrap();
        let mut slot = OwnerStandbySlot::new();
        assert_eq!(
            slot.admit(authenticated, &mut queue).unwrap(),
            OwnerStandbyAdmission::Installed
        );
        let mut transport = CapturingTransport::default();
        queue
            .try_flush(&mut candidate, &mut transport, SimTime::ZERO)
            .unwrap()
            .unwrap();
        assert!(slot.registered_is_live());
        (executor, candidate, slot, queue)
    }

    fn registered_owner_slot_with_reporter(
        exporter: u8,
    ) -> (
        OwnerTargetExecutor<MemoryTarget>,
        LegIo,
        OwnerStandbySlot,
        LegOutboundQueue,
        LegTransportReporter,
    ) {
        let (executor, active) = bootstrap_standby_executor();
        let (mut candidate, reporter) = LegIo::for_authenticated_transport_with_reporter(
            LegId::B,
            LegEndpointRole::Owner,
            binding(exporter),
            LegIoLimits::new(4_096, 8, 32_768, 8, 32_768).unwrap(),
        );
        let inbound = classified_standby_inbound(&active, &mut candidate, 0x31);
        let authenticated = executor
            .authenticate_owner_standby(&active, candidate.established_leg(), inbound)
            .unwrap();
        let mut queue = LegOutboundQueue::for_leg(candidate.established_leg(), 8, 32_768).unwrap();
        let mut slot = OwnerStandbySlot::new();
        assert_eq!(
            slot.admit(authenticated, &mut queue).unwrap(),
            OwnerStandbyAdmission::Installed
        );
        let mut transport = CapturingTransport::default();
        queue
            .try_flush(&mut candidate, &mut transport, SimTime::ZERO)
            .unwrap()
            .unwrap();
        assert!(slot.registered_is_live());
        (executor, candidate, slot, queue, reporter)
    }

    fn registered_exact_request(generation: u64, nonce: u8) -> AttachRequest {
        AttachRequest::new(
            SessionId::new([0x11; 16]).unwrap(),
            LegGeneration::new(generation).unwrap(),
            AttachNonce::new([nonce; 16]).unwrap(),
            VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION).unwrap(),
            FeatureOffer::new(STANDBY_FEATURES, STANDBY_FEATURES).unwrap(),
        )
    }

    #[test]
    fn registered_owner_gate_keeps_wrong_contract_proof_and_transport_inert() {
        let (mut executor, candidate, mut slot, mut queue) = registered_owner_slot(0x91);
        let candidate_binding = candidate.established_leg().standby_transport_binding();

        let wrong_generation = registered_exact_request(4, 0x41);
        let wrong_generation_proof = credentials()
            .prove(&wrong_generation, &candidate_binding)
            .unwrap();
        let wrong_generation = candidate
            .established_leg()
            .bind_received_frame(wrong_generation.to_attach_frame(wrong_generation_proof));
        let before = executor.snapshot();
        assert!(matches!(
            executor.accept_registered_owner_attach(
                &mut slot,
                candidate.established_leg(),
                wrong_generation,
                &mut queue,
            ),
            Err(OwnerTargetError::RejectedRegisteredOwnerAttach { .. })
        ));
        assert_eq!(executor.snapshot(), before);
        assert!(slot.registered_is_live());

        let wrong_features = AttachRequest::new(
            SessionId::new([0x11; 16]).unwrap(),
            LegGeneration::new(3).unwrap(),
            AttachNonce::new([0x42; 16]).unwrap(),
            VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION).unwrap(),
            FeatureOffer::new(STANDBY_FEATURES, STANDBY_CONTROL_V1.bits()).unwrap(),
        );
        let wrong_features_proof = credentials()
            .prove(&wrong_features, &candidate_binding)
            .unwrap();
        let wrong_features = candidate
            .established_leg()
            .bind_received_frame(wrong_features.to_attach_frame(wrong_features_proof));
        assert!(matches!(
            executor.accept_registered_owner_attach(
                &mut slot,
                candidate.established_leg(),
                wrong_features,
                &mut queue,
            ),
            Err(OwnerTargetError::RejectedRegisteredOwnerAttach { .. })
        ));
        assert_eq!(executor.snapshot(), before);
        assert!(slot.registered_is_live());

        let wrong_session = AttachRequest::new(
            SessionId::new([0x12; 16]).unwrap(),
            LegGeneration::new(3).unwrap(),
            AttachNonce::new([0x43; 16]).unwrap(),
            VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION).unwrap(),
            FeatureOffer::new(STANDBY_FEATURES, STANDBY_FEATURES).unwrap(),
        );
        let wrong_session_proof = credentials()
            .prove(&wrong_session, &candidate_binding)
            .unwrap();
        let wrong_session = candidate
            .established_leg()
            .bind_received_frame(wrong_session.to_attach_frame(wrong_session_proof));
        assert!(matches!(
            executor.accept_registered_owner_attach(
                &mut slot,
                candidate.established_leg(),
                wrong_session,
                &mut queue,
            ),
            Err(OwnerTargetError::RejectedRegisteredOwnerAttach { .. })
        ));
        assert_eq!(executor.snapshot(), before);
        assert!(slot.registered_is_live());

        let wrong_leg = EstablishedLeg::for_authenticated_transport(binding(0x92));
        let exact = registered_exact_request(3, 0x44);
        let exact_proof = credentials().prove(&exact, &candidate_binding).unwrap();
        let wrong_seal = wrong_leg.bind_received_frame(exact.to_attach_frame(exact_proof));
        assert!(matches!(
            executor.accept_registered_owner_attach(
                &mut slot,
                candidate.established_leg(),
                wrong_seal,
                &mut queue,
            ),
            Err(OwnerTargetError::RejectedRegisteredOwnerAttach { .. })
        ));
        assert_eq!(executor.snapshot(), before);
        assert!(slot.registered_is_live());

        let carried = registered_exact_request(3, 0x45);
        let differently_signed = registered_exact_request(3, 0x46);
        let bad_proof = credentials()
            .prove(&differently_signed, &candidate_binding)
            .unwrap();
        let bad_proof = candidate
            .established_leg()
            .bind_received_frame(carried.to_attach_frame(bad_proof));
        assert!(matches!(
            executor.accept_registered_owner_attach(
                &mut slot,
                candidate.established_leg(),
                bad_proof,
                &mut queue,
            ),
            Err(OwnerTargetError::Supervisor(
                SessionSupervisorError::LegProvenance(LegProvenanceError::Rejected)
            ))
        ));
        assert_eq!(executor.snapshot(), before);
        assert!(slot.registered_is_live());

        assert!(queue.mark_active_for_test());
        let exact_but_active_queue = registered_exact_request(3, 0x47);
        let exact_but_active_proof = credentials()
            .prove(&exact_but_active_queue, &candidate_binding)
            .unwrap();
        let active_before = executor.snapshot();
        assert!(matches!(
            executor.accept_registered_owner_attach(
                &mut slot,
                candidate.established_leg(),
                candidate.established_leg().bind_received_frame(
                    exact_but_active_queue.to_attach_frame(exact_but_active_proof)
                ),
                &mut queue,
            ),
            Err(OwnerTargetError::RejectedRegisteredOwnerAttach {
                source: OwnerRegisteredAttachGateError::QueueReservation(
                    AttachAcceptanceReserveError::NotAwaiting { .. }
                ),
                ..
            })
        ));
        assert_eq!(executor.snapshot(), active_before);
        assert!(slot.registered_is_live());
    }

    #[test]
    fn poisoned_terminal_transition_restores_registered_slot_and_keeps_model_inert() {
        let (mut executor, candidate, mut slot, mut queue) = registered_owner_slot(0xb3);
        let request = registered_exact_request(3, 0x73);
        let proof = credentials()
            .prove(
                &request,
                &candidate.established_leg().standby_transport_binding(),
            )
            .unwrap();
        let received = candidate
            .established_leg()
            .bind_received_frame(request.to_attach_frame(proof));
        let before = executor.snapshot();
        let queue_before = (queue.len(), queue.owned_bytes());
        candidate
            .established_leg()
            .poison_terminal_transition_for_test();

        assert!(matches!(
            executor.accept_registered_owner_attach(
                &mut slot,
                candidate.established_leg(),
                received,
                &mut queue,
            ),
            Err(OwnerTargetError::RegisteredOwnerAttachTerminalTransition {
                source: LegTerminalTransitionError::Poisoned,
                ..
            })
        ));
        assert_eq!(executor.snapshot(), before);
        assert_eq!((queue.len(), queue.owned_bytes()), queue_before);
        assert!(!slot.registered_is_live());
        assert!(slot.retire_lost().is_some());
    }

    #[test]
    fn owner_expiry_invalidates_pending_catch_up_permit_and_rejects_new_status() {
        let active_binding = binding(0x42);
        let (owner_leg, reporter) =
            EstablishedLeg::for_authenticated_transport_with_reporter(active_binding);
        let initial_request = standby_attach_request(2, 0x22);
        let initial_proof = credentials()
            .prove(&initial_request, &active_binding)
            .unwrap();
        let mut bootstrap = SessionSupervisor::prepare_owner_unproven_for_test(
            session_config(),
            standby_authority(1),
        );
        let mut queue = LegOutboundQueue::for_leg(&owner_leg, 4, 4_096).unwrap();
        let (supervisor, active) = bootstrap
            .accept(
                &owner_leg,
                owner_leg.bind_received_frame(initial_request.to_attach_frame(initial_proof)),
                &mut queue,
            )
            .unwrap()
            .enqueue_acceptance(&mut queue)
            .unwrap()
            .into_parts();
        let mut executor = OwnerTargetExecutor::new(
            OwnerTargetConfig::new(128, 1_024, 64).unwrap(),
            supervisor,
            MemoryTarget::new(MemoryTargetConfig::new(4, 256, 512, 128, 128, 16).unwrap()),
            source_config(),
        )
        .unwrap();
        let stale = registered_exact_request(2, 0x74);
        let status_leg = EstablishedLeg::for_authenticated_transport(binding(0xb4));
        let stale_proof = credentials()
            .prove(&stale, &status_leg.standby_transport_binding())
            .unwrap();
        let pending_status = executor
            .resynchronize_owner_attach(
                &status_leg,
                status_leg.bind_received_frame(stale.to_attach_frame(stale_proof)),
            )
            .unwrap();
        assert!(executor.snapshot().session.owner_catch_up_permit);
        assert!(!executor.snapshot().session.owner_catch_up_permit_armed);

        let expiry = active.resume_grace_expired_event();
        let terminal = reporter
            .report(LegTransportTerminalReason::PeerClosed)
            .unwrap();
        executor
            .apply_event(active.bind_terminal(terminal).unwrap().into_event())
            .unwrap();
        executor.apply_event(expiry).unwrap();
        let expired = executor.snapshot();
        assert_eq!(expired.session.session.phase(), SessionPhase::Expired);
        assert!(!expired.session.owner_catch_up_permit);
        assert!(!expired.session.owner_catch_up_permit_armed);
        drop(pending_status);

        let retry_leg = EstablishedLeg::for_authenticated_transport(binding(0xb5));
        let retry_proof = credentials()
            .prove(&stale, &retry_leg.standby_transport_binding())
            .unwrap();
        assert!(matches!(
            executor.resynchronize_owner_attach(
                &retry_leg,
                retry_leg.bind_received_frame(stale.to_attach_frame(retry_proof)),
            ),
            Err(OwnerTargetError::Supervisor(
                SessionSupervisorError::OwnerCatchUpStatusRejected
            ))
        ));
        assert_eq!(executor.snapshot(), expired);
    }

    #[test]
    fn initial_acceptance_terminal_bootstrap_recovers_through_fresh_status_and_catch_up() {
        let active_binding = binding(0x42);
        let (owner_leg, reporter) =
            EstablishedLeg::for_authenticated_transport_with_reporter(active_binding);
        let initial_request = standby_attach_request(2, 0x22);
        let initial_proof = credentials()
            .prove(&initial_request, &active_binding)
            .unwrap();
        let mut bootstrap = SessionSupervisor::prepare_owner_unproven_for_test(
            session_config(),
            standby_authority(1),
        );
        let mut initial_queue = LegOutboundQueue::for_leg(&owner_leg, 4, 4_096).unwrap();
        let publication = bootstrap
            .accept(
                &owner_leg,
                owner_leg.bind_received_frame(initial_request.to_attach_frame(initial_proof)),
                &mut initial_queue,
            )
            .unwrap();

        let terminal = reporter
            .report(LegTransportTerminalReason::PeerClosed)
            .unwrap();
        let legless = publication.lose_before_acceptance(terminal).unwrap();
        assert_eq!(legless.snapshot().session.phase(), SessionPhase::Legless);
        assert_eq!(legless.high_water_generation().get(), 2);

        let (mut executor, high_water) = OwnerTargetExecutor::new_legless_bootstrap(
            OwnerTargetConfig::new(128, 1_024, 64).unwrap(),
            legless,
            MemoryTarget::new(MemoryTargetConfig::new(4, 256, 512, 128, 128, 16).unwrap()),
            source_config(),
        )
        .unwrap();
        assert_eq!(high_water.generation().get(), 2);
        assert_eq!(high_water.reason(), LegTransportTerminalReason::PeerClosed);

        let stale = registered_exact_request(2, 0x81);
        let mut status_leg = owner_standby_leg(0xa1);
        let mut status_queue =
            LegOutboundQueue::for_leg(status_leg.established_leg(), 4, 4_096).unwrap();
        let status_proof = credentials()
            .prove(
                &stale,
                &status_leg.established_leg().standby_transport_binding(),
            )
            .unwrap();
        let status = executor
            .resynchronize_owner_attach(
                status_leg.established_leg(),
                status_leg
                    .established_leg()
                    .bind_received_frame(stale.to_attach_frame(status_proof)),
            )
            .unwrap()
            .enqueue(&mut status_queue)
            .unwrap();
        let mut status_transport = CapturingTransport::default();
        status_queue
            .try_flush(&mut status_leg, &mut status_transport, SimTime::ZERO)
            .unwrap()
            .unwrap();
        executor
            .arm_owner_catch_up_after_status_delivery(status, &status_queue)
            .unwrap();

        let follow_up = registered_exact_request(3, 0x82);
        let follow_up_leg = owner_standby_leg(0xa2);
        let mut follow_up_queue =
            LegOutboundQueue::for_leg(follow_up_leg.established_leg(), 4, 4_096).unwrap();
        let follow_up_proof = credentials()
            .prove(
                &follow_up,
                &follow_up_leg.established_leg().standby_transport_binding(),
            )
            .unwrap();
        let OwnerTargetAttachPublication::Installed { pending } = executor
            .accept_catch_up_owner_attach(
                follow_up_leg.established_leg(),
                follow_up_leg
                    .established_leg()
                    .bind_received_frame(follow_up.to_attach_frame(follow_up_proof)),
                &mut follow_up_queue,
            )
            .unwrap()
        else {
            panic!("armed fresh follow-up must install the legless successor")
        };
        let recovery = pending.enqueue_acceptance(&mut follow_up_queue).unwrap();
        executor
            .execute_attached_recovery(recovery, &mut follow_up_queue)
            .unwrap();
        assert_eq!(
            executor.snapshot().session.session.phase(),
            SessionPhase::Active
        );
        assert_eq!(executor.snapshot().session.session.generation().get(), 3);

        let high_water = match executor.expire_legless_high_water(high_water) {
            Err(OwnerTargetError::RejectedLeglessHighWater { high_water }) => *high_water,
            other => panic!("recovered generation must retain the stale timer, got {other:?}"),
        };
        assert_eq!(high_water.generation().get(), 2);
        assert_eq!(executor.snapshot().session.session.generation().get(), 3);
    }

    #[test]
    fn duplicate_same_status_contract_cannot_publish_while_original_arm_is_pending() {
        let (mut executor, installed_b, mut slot, acceptance_queue, reporter) =
            registered_owner_slot_with_reporter(0xa0);
        let mut acceptance_queue = acceptance_queue;
        let request = registered_exact_request(3, 0x71);
        let proof = credentials()
            .prove(
                &request,
                &installed_b.established_leg().standby_transport_binding(),
            )
            .unwrap();
        let OwnerTargetAttachPublication::Installed { pending } = executor
            .accept_registered_owner_attach(
                &mut slot,
                installed_b.established_leg(),
                installed_b
                    .established_leg()
                    .bind_received_frame(request.to_attach_frame(proof)),
                &mut acceptance_queue,
            )
            .unwrap()
        else {
            panic!("registered B did not install generation 3");
        };
        let terminal = reporter
            .report(LegTransportTerminalReason::PeerClosed)
            .unwrap();
        let terminalized = executor
            .terminalize_pending_attach_before_acceptance(pending, terminal)
            .unwrap();
        let _high_water = executor
            .discard_terminal_attach_recovery(terminalized)
            .unwrap();
        drop(acceptance_queue);

        let stale = registered_exact_request(3, 0x72);
        let mut status_leg = owner_standby_leg(0xa1);
        let mut status_queue =
            LegOutboundQueue::for_leg(status_leg.established_leg(), 4, 4_096).unwrap();
        let proof = credentials()
            .prove(
                &stale,
                &status_leg.established_leg().standby_transport_binding(),
            )
            .unwrap();
        let original = executor
            .resynchronize_owner_attach(
                status_leg.established_leg(),
                status_leg
                    .established_leg()
                    .bind_received_frame(stale.to_attach_frame(proof)),
            )
            .unwrap();
        assert!(original.arm.is_some());
        let before_duplicate = executor.snapshot();
        let queue_before_duplicate = (status_queue.len(), status_queue.owned_bytes());

        let duplicate_proof = credentials()
            .prove(
                &stale,
                &status_leg.established_leg().standby_transport_binding(),
            )
            .unwrap();
        let duplicate = executor.resynchronize_owner_attach(
            status_leg.established_leg(),
            status_leg
                .established_leg()
                .bind_received_frame(stale.to_attach_frame(duplicate_proof)),
        );
        assert!(matches!(
            duplicate,
            Err(OwnerTargetError::Supervisor(
                SessionSupervisorError::OwnerCatchUpPermitPending
            ))
        ));
        assert_eq!(executor.snapshot(), before_duplicate);
        assert_eq!(
            (status_queue.len(), status_queue.owned_bytes()),
            queue_before_duplicate
        );

        let enqueued = original.enqueue(&mut status_queue).unwrap();
        let mut status_transport = CapturingTransport::default();
        status_queue
            .try_flush(&mut status_leg, &mut status_transport, SimTime::ZERO)
            .unwrap()
            .unwrap();
        executor
            .arm_owner_catch_up_after_status_delivery(enqueued, &status_queue)
            .unwrap();
        assert!(executor.snapshot().session.owner_catch_up_permit_armed);

        let follow_up = registered_exact_request(4, 0x73);
        let follow_up_leg = owner_standby_leg(0xa2);
        let mut follow_up_queue =
            LegOutboundQueue::for_leg(follow_up_leg.established_leg(), 4, 4_096).unwrap();
        let follow_up_proof = credentials()
            .prove(
                &follow_up,
                &follow_up_leg.established_leg().standby_transport_binding(),
            )
            .unwrap();
        assert!(matches!(
            executor
                .accept_catch_up_owner_attach(
                    follow_up_leg.established_leg(),
                    follow_up_leg
                        .established_leg()
                        .bind_received_frame(follow_up.to_attach_frame(follow_up_proof)),
                    &mut follow_up_queue,
                )
                .unwrap(),
            OwnerTargetAttachPublication::Installed { .. }
        ));
        assert_eq!(executor.snapshot().session.session.generation().get(), 4);
        assert!(!executor.snapshot().session.owner_catch_up_permit);
    }

    #[test]
    fn status_only_is_no_cas_and_one_permit_closes_lost_acceptance_catch_up() {
        let (mut executor, installed_b, mut slot, acceptance_queue, reporter) =
            registered_owner_slot_with_reporter(0x93);
        let mut acceptance_queue = acceptance_queue;
        let request = registered_exact_request(3, 0x51);
        let binding_b = installed_b.established_leg().standby_transport_binding();
        let proof = credentials().prove(&request, &binding_b).unwrap();
        let OwnerTargetAttachPublication::Installed { pending } = executor
            .accept_registered_owner_attach(
                &mut slot,
                installed_b.established_leg(),
                installed_b
                    .established_leg()
                    .bind_received_frame(request.to_attach_frame(proof)),
                &mut acceptance_queue,
            )
            .unwrap()
        else {
            panic!("registered B did not install generation 3");
        };
        let terminal = reporter
            .report(LegTransportTerminalReason::PeerClosed)
            .unwrap();
        let terminalized = executor
            .terminalize_pending_attach_before_acceptance(pending, terminal)
            .unwrap();
        let _high_water = executor
            .discard_terminal_attach_recovery(terminalized)
            .unwrap();
        drop(acceptance_queue);
        assert_eq!(executor.snapshot().session.session.generation().get(), 3);
        assert!(!executor.snapshot().session.owner_catch_up_permit);

        let drift = AttachRequest::new(
            SessionId::new([0x11; 16]).unwrap(),
            LegGeneration::new(3).unwrap(),
            AttachNonce::new([0x50; 16]).unwrap(),
            VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION).unwrap(),
            FeatureOffer::new(STANDBY_CONTROL_V1.bits(), STANDBY_CONTROL_V1.bits()).unwrap(),
        );
        let drift_leg = EstablishedLeg::for_authenticated_transport(binding(0x90));
        let drift_proof = credentials()
            .prove(&drift, &drift_leg.standby_transport_binding())
            .unwrap();
        let before_drift = executor.snapshot();
        assert!(matches!(
            executor.resynchronize_owner_attach(
                &drift_leg,
                drift_leg.bind_received_frame(drift.to_attach_frame(drift_proof)),
            ),
            Err(OwnerTargetError::Supervisor(
                SessionSupervisorError::OwnerCatchUpStatusRejected
            ))
        ));
        assert_eq!(executor.snapshot(), before_drift);

        let generation_one = registered_exact_request(1, 0x4f);
        let generation_one_leg = EstablishedLeg::for_authenticated_transport(binding(0x8f));
        let generation_one_proof = credentials()
            .prove(
                &generation_one,
                &generation_one_leg.standby_transport_binding(),
            )
            .unwrap();
        let generation_one_status = executor
            .resynchronize_owner_attach(
                &generation_one_leg,
                generation_one_leg
                    .bind_received_frame(generation_one.to_attach_frame(generation_one_proof)),
            )
            .unwrap();
        assert!(generation_one_status.arm.is_none());
        assert!(!executor.snapshot().session.owner_catch_up_permit);
        drop(generation_one_status);

        let stale = registered_exact_request(3, 0x52);
        let (status_leg, status_reporter) = LegIo::for_authenticated_transport_with_reporter(
            LegId::B,
            LegEndpointRole::Owner,
            binding(0x94),
            LegIoLimits::new(4_096, 8, 32_768, 8, 32_768).unwrap(),
        );
        let mut status_queue =
            LegOutboundQueue::for_leg(status_leg.established_leg(), 4, 4_096).unwrap();
        let stale_proof = credentials()
            .prove(
                &stale,
                &status_leg.established_leg().standby_transport_binding(),
            )
            .unwrap();
        let before_status = executor.snapshot();
        let pending_status = executor
            .resynchronize_owner_attach(
                status_leg.established_leg(),
                status_leg
                    .established_leg()
                    .bind_received_frame(stale.to_attach_frame(stale_proof)),
            )
            .unwrap();
        assert!(matches!(
            pending_status.status_for_test().record(),
            Record::AttachGenerationStatus {
                requested_generation,
                nonce,
                ..
            } if *requested_generation == stale.requested_generation()
                && *nonce == stale.nonce()
        ));
        assert_eq!(pending_status.status_for_test().leg_generation().get(), 3);
        assert_eq!(
            executor.snapshot().session.session,
            before_status.session.session
        );
        assert!(executor.snapshot().session.owner_catch_up_permit);
        assert!(!executor.snapshot().session.owner_catch_up_permit_armed);

        let same_stale_proof = credentials()
            .prove(
                &stale,
                &status_leg.established_leg().standby_transport_binding(),
            )
            .unwrap();
        let mut permit_before_retry = executor.snapshot();
        assert!(matches!(
            executor.resynchronize_owner_attach(
                status_leg.established_leg(),
                status_leg
                    .established_leg()
                    .bind_received_frame(stale.to_attach_frame(same_stale_proof)),
            ),
            Err(OwnerTargetError::Supervisor(
                SessionSupervisorError::OwnerCatchUpPermitPending
            ))
        ));
        assert_eq!(executor.snapshot(), permit_before_retry);

        let same_stale_leg = EstablishedLeg::for_authenticated_transport(binding(0x95));
        let same_stale_proof = credentials()
            .prove(&stale, &same_stale_leg.standby_transport_binding())
            .unwrap();
        assert!(matches!(
            executor.resynchronize_owner_attach(
                &same_stale_leg,
                same_stale_leg.bind_received_frame(stale.to_attach_frame(same_stale_proof)),
            ),
            Err(OwnerTargetError::Supervisor(
                SessionSupervisorError::OwnerCatchUpPermitConflict
            ))
        ));
        assert_eq!(executor.snapshot(), permit_before_retry);

        let conflict = registered_exact_request(3, 0x53);
        let conflict_leg = EstablishedLeg::for_authenticated_transport(binding(0x96));
        let conflict_proof = credentials()
            .prove(&conflict, &conflict_leg.standby_transport_binding())
            .unwrap();
        assert!(matches!(
            executor.resynchronize_owner_attach(
                &conflict_leg,
                conflict_leg.bind_received_frame(conflict.to_attach_frame(conflict_proof)),
            ),
            Err(OwnerTargetError::Supervisor(
                SessionSupervisorError::OwnerCatchUpPermitConflict
            ))
        ));
        assert_eq!(executor.snapshot(), permit_before_retry);

        let enqueued_status = pending_status.enqueue(&mut status_queue).unwrap();
        let wrong_status_leg = owner_standby_leg(0x96);
        let wrong_status_queue =
            LegOutboundQueue::for_leg(wrong_status_leg.established_leg(), 4, 4_096).unwrap();
        let enqueued_status = match executor
            .arm_owner_catch_up_after_status_delivery(enqueued_status, &wrong_status_queue)
        {
            Err(OwnerTargetError::RejectedOwnerAttachStatusArm {
                block: OwnerTargetInputBlock::WrongStatusQueue,
                publication,
            }) => *publication,
            other => panic!("wrong status queue must retain exact receipt, got {other:?}"),
        };
        let enqueued_status = match executor
            .arm_owner_catch_up_after_status_delivery(enqueued_status, &status_queue)
        {
            Err(OwnerTargetError::RejectedOwnerAttachStatusArm {
                block: OwnerTargetInputBlock::StatusNotDelivered,
                publication,
            }) => *publication,
            other => panic!("undelivered status must retain exact receipt, got {other:?}"),
        };
        let enqueued_status =
            match executor.abandon_lost_enqueued_owner_attach_status(enqueued_status) {
                Err(OwnerTargetError::RejectedEnqueuedOwnerAttachStatusAbandon {
                    block: OwnerTargetInputBlock::StatusPublicationStillLive,
                    publication,
                }) => *publication,
                other => panic!("live held status must not clear its permit, got {other:?}"),
            };
        let status_terminal = status_reporter
            .report(LegTransportTerminalReason::FatalIo)
            .unwrap();
        executor
            .abandon_lost_enqueued_owner_attach_status(enqueued_status)
            .unwrap();
        assert!(!executor.snapshot().session.owner_catch_up_permit);
        drop((status_queue, status_leg, status_terminal));

        let (mut status_leg, status_reporter) = LegIo::for_authenticated_transport_with_reporter(
            LegId::B,
            LegEndpointRole::Owner,
            binding(0x9a),
            LegIoLimits::new(4_096, 8, 32_768, 8, 32_768).unwrap(),
        );
        let mut status_queue =
            LegOutboundQueue::for_leg(status_leg.established_leg(), 4, 4_096).unwrap();
        let fresh_stale_proof = credentials()
            .prove(
                &stale,
                &status_leg.established_leg().standby_transport_binding(),
            )
            .unwrap();
        let pending_status = executor
            .resynchronize_owner_attach(
                status_leg.established_leg(),
                status_leg
                    .established_leg()
                    .bind_received_frame(stale.to_attach_frame(fresh_stale_proof)),
            )
            .unwrap();
        let enqueued_status = pending_status.enqueue(&mut status_queue).unwrap();
        permit_before_retry = executor.snapshot();
        assert!(permit_before_retry.session.owner_catch_up_permit);
        assert!(!permit_before_retry.session.owner_catch_up_permit_armed);

        let exact_next = registered_exact_request(4, 0x54);
        let exact_next_leg = EstablishedLeg::for_authenticated_transport(binding(0x97));
        let exact_next_proof = credentials()
            .prove(&exact_next, &exact_next_leg.standby_transport_binding())
            .unwrap();
        assert!(matches!(
            executor.resynchronize_owner_attach(
                &exact_next_leg,
                exact_next_leg.bind_received_frame(exact_next.to_attach_frame(exact_next_proof)),
            ),
            Err(OwnerTargetError::Supervisor(
                SessionSupervisorError::LegProvenance(LegProvenanceError::Rejected)
            ))
        ));
        assert_eq!(executor.snapshot(), permit_before_retry);

        let follow_up = registered_exact_request(4, 0x55);
        let (follow_up_leg, follow_up_reporter) = LegIo::for_authenticated_transport_with_reporter(
            LegId::B,
            LegEndpointRole::Owner,
            binding(0x98),
            LegIoLimits::new(4_096, 8, 32_768, 8, 32_768).unwrap(),
        );
        let mut follow_up_queue =
            LegOutboundQueue::for_leg(follow_up_leg.established_leg(), 4, 4_096).unwrap();
        let follow_up_proof = credentials()
            .prove(
                &follow_up,
                &follow_up_leg.established_leg().standby_transport_binding(),
            )
            .unwrap();
        let follow_up_received = follow_up_leg
            .established_leg()
            .bind_received_frame(follow_up.to_attach_frame(follow_up_proof));
        let follow_up_received = match executor.accept_catch_up_owner_attach(
            follow_up_leg.established_leg(),
            follow_up_received,
            &mut follow_up_queue,
        ) {
            Err(OwnerTargetError::RejectedCatchUpOwnerAttach {
                source: SessionSupervisorError::OwnerCatchUpPermitNotArmed,
                received,
            }) => *received,
            other => panic!("unarmed catch-up must return exact D input, got {other:?}"),
        };
        assert_eq!(executor.snapshot(), permit_before_retry);
        let reservation = follow_up_queue
            .reserve_attach_acceptance(follow_up_leg.established_leg())
            .unwrap();
        reservation.release(&mut follow_up_queue).unwrap();

        let mut status_transport = CapturingTransport::default();
        status_queue
            .try_flush(&mut status_leg, &mut status_transport, SimTime::ZERO)
            .unwrap()
            .unwrap();
        let status_terminal = status_reporter
            .report(LegTransportTerminalReason::FatalIo)
            .unwrap();
        let enqueued_status =
            match executor.abandon_lost_enqueued_owner_attach_status(enqueued_status) {
                Err(OwnerTargetError::RejectedEnqueuedOwnerAttachStatusAbandon {
                    block: OwnerTargetInputBlock::StatusPublicationStillLive,
                    publication,
                }) => *publication,
                other => panic!(
                    "delivered status must retain its sole arm after terminal, got {other:?}"
                ),
            };
        assert_eq!(executor.snapshot(), permit_before_retry);
        executor
            .arm_owner_catch_up_after_status_delivery(enqueued_status, &status_queue)
            .unwrap();
        drop(status_terminal);
        assert!(executor.snapshot().session.owner_catch_up_permit_armed);
        let armed = executor.snapshot();

        let same_c_follow_up = registered_exact_request(4, 0x57);
        let same_c_proof = credentials()
            .prove(
                &same_c_follow_up,
                &status_leg.established_leg().standby_transport_binding(),
            )
            .unwrap();
        assert!(matches!(
            executor.accept_catch_up_owner_attach(
                status_leg.established_leg(),
                status_leg
                    .established_leg()
                    .bind_received_frame(same_c_follow_up.to_attach_frame(same_c_proof)),
                &mut status_queue,
            ),
            Err(OwnerTargetError::RejectedCatchUpOwnerAttach {
                source: SessionSupervisorError::OwnerCatchUpAttachGateRejected,
                ..
            })
        ));
        assert_eq!(executor.snapshot(), armed);

        let same_binding_leg = EstablishedLeg::for_authenticated_transport(binding(0x9a));
        let mut same_binding_queue =
            LegOutboundQueue::for_leg(&same_binding_leg, 4, 4_096).unwrap();
        let same_binding_follow_up = registered_exact_request(4, 0x58);
        let same_binding_proof = credentials()
            .prove(
                &same_binding_follow_up,
                &same_binding_leg.standby_transport_binding(),
            )
            .unwrap();
        assert!(matches!(
            executor.accept_catch_up_owner_attach(
                &same_binding_leg,
                same_binding_leg.bind_received_frame(
                    same_binding_follow_up.to_attach_frame(same_binding_proof)
                ),
                &mut same_binding_queue,
            ),
            Err(OwnerTargetError::RejectedCatchUpOwnerAttach {
                source: SessionSupervisorError::OwnerCatchUpAttachGateRejected,
                ..
            })
        ));
        assert_eq!(executor.snapshot(), armed);

        let active_binding_leg = EstablishedLeg::for_authenticated_transport(binding(0x93));
        let mut active_binding_queue =
            LegOutboundQueue::for_leg(&active_binding_leg, 4, 4_096).unwrap();
        let active_binding_follow_up = registered_exact_request(4, 0x5a);
        let active_binding_proof = credentials()
            .prove(
                &active_binding_follow_up,
                &active_binding_leg.standby_transport_binding(),
            )
            .unwrap();
        assert!(matches!(
            executor.accept_catch_up_owner_attach(
                &active_binding_leg,
                active_binding_leg.bind_received_frame(
                    active_binding_follow_up.to_attach_frame(active_binding_proof)
                ),
                &mut active_binding_queue,
            ),
            Err(OwnerTargetError::RejectedCatchUpOwnerAttach {
                source: SessionSupervisorError::OwnerCatchUpAttachGateRejected,
                ..
            })
        ));
        assert_eq!(executor.snapshot(), armed);

        let active_nonce_leg = EstablishedLeg::for_authenticated_transport(binding(0x9b));
        let mut active_nonce_queue =
            LegOutboundQueue::for_leg(&active_nonce_leg, 4, 4_096).unwrap();
        let active_nonce_follow_up = registered_exact_request(4, 0x51);
        let active_nonce_proof = credentials()
            .prove(
                &active_nonce_follow_up,
                &active_nonce_leg.standby_transport_binding(),
            )
            .unwrap();
        assert!(matches!(
            executor.accept_catch_up_owner_attach(
                &active_nonce_leg,
                active_nonce_leg.bind_received_frame(
                    active_nonce_follow_up.to_attach_frame(active_nonce_proof)
                ),
                &mut active_nonce_queue,
            ),
            Err(OwnerTargetError::RejectedCatchUpOwnerAttach {
                source: SessionSupervisorError::OwnerCatchUpAttachGateRejected,
                ..
            })
        ));
        assert_eq!(executor.snapshot(), armed);

        let active_seal_follow_up = registered_exact_request(4, 0x5b);
        let active_seal_proof = credentials()
            .prove(&active_seal_follow_up, &binding_b)
            .unwrap();
        assert!(matches!(
            executor.supervisor.accept_catch_up_owner_attach(
                installed_b.established_leg(),
                installed_b
                    .established_leg()
                    .bind_received_frame(active_seal_follow_up.to_attach_frame(active_seal_proof)),
            ),
            Err(SessionSupervisorError::OwnerCatchUpAttachGateRejected)
        ));
        assert_eq!(executor.snapshot(), armed);

        let active_leg = owner_standby_leg(0x97);
        let mut active_queue =
            LegOutboundQueue::for_leg(active_leg.established_leg(), 4, 4_096).unwrap();
        assert!(active_queue.mark_active_for_test());
        let active_follow_up = registered_exact_request(4, 0x59);
        let active_proof = credentials()
            .prove(
                &active_follow_up,
                &active_leg.established_leg().standby_transport_binding(),
            )
            .unwrap();
        assert!(matches!(
            executor.accept_catch_up_owner_attach(
                active_leg.established_leg(),
                active_leg
                    .established_leg()
                    .bind_received_frame(active_follow_up.to_attach_frame(active_proof)),
                &mut active_queue,
            ),
            Err(OwnerTargetError::RejectedOwnerAttachQueueReservation { .. })
        ));
        assert_eq!(executor.snapshot(), armed);

        let wrong_follow_up = registered_exact_request(4, 0x56);
        let bad_follow_up_proof = credentials()
            .prove(
                &wrong_follow_up,
                &follow_up_leg.established_leg().standby_transport_binding(),
            )
            .unwrap();
        assert!(matches!(
            executor.accept_catch_up_owner_attach(
                follow_up_leg.established_leg(),
                follow_up_leg
                    .established_leg()
                    .bind_received_frame(follow_up.to_attach_frame(bad_follow_up_proof)),
                &mut follow_up_queue,
            ),
            Err(OwnerTargetError::Supervisor(
                SessionSupervisorError::LegProvenance(LegProvenanceError::Rejected)
            ))
        ));
        assert_eq!(executor.snapshot(), armed);

        let OwnerTargetAttachPublication::Installed { pending } = executor
            .accept_catch_up_owner_attach(
                follow_up_leg.established_leg(),
                follow_up_received,
                &mut follow_up_queue,
            )
            .unwrap()
        else {
            panic!("armed fresh D must install")
        };
        assert_eq!(executor.snapshot().session.session.generation().get(), 4);
        assert!(!executor.snapshot().session.owner_catch_up_permit);
        assert!(!executor.snapshot().session.owner_catch_up_permit_armed);
        let follow_up_terminal = follow_up_reporter
            .report(LegTransportTerminalReason::PeerClosed)
            .unwrap();
        let terminalized = executor
            .terminalize_pending_attach_before_acceptance(pending, follow_up_terminal)
            .unwrap();
        let _high_water = executor
            .discard_terminal_attach_recovery(terminalized)
            .unwrap();
        drop(follow_up_queue);

        let stale_four = registered_exact_request(4, 0x61);
        let mut status_four_leg = owner_standby_leg(0x9c);
        let mut status_four_queue =
            LegOutboundQueue::for_leg(status_four_leg.established_leg(), 4, 4_096).unwrap();
        let stale_four_proof = credentials()
            .prove(
                &stale_four,
                &status_four_leg
                    .established_leg()
                    .standby_transport_binding(),
            )
            .unwrap();
        let status_four = executor
            .resynchronize_owner_attach(
                status_four_leg.established_leg(),
                status_four_leg
                    .established_leg()
                    .bind_received_frame(stale_four.to_attach_frame(stale_four_proof)),
            )
            .unwrap()
            .enqueue(&mut status_four_queue)
            .unwrap();
        let mut status_four_transport = CapturingTransport::default();
        status_four_queue
            .try_flush(
                &mut status_four_leg,
                &mut status_four_transport,
                SimTime::ZERO,
            )
            .unwrap()
            .unwrap();
        executor
            .arm_owner_catch_up_after_status_delivery(status_four, &status_four_queue)
            .unwrap();

        let generation_five = registered_exact_request(5, 0x62);
        let generation_five_leg = owner_standby_leg(0x9d);
        let mut generation_five_queue =
            LegOutboundQueue::for_leg(generation_five_leg.established_leg(), 4, 4_096).unwrap();
        let generation_five_proof = credentials()
            .prove(
                &generation_five,
                &generation_five_leg
                    .established_leg()
                    .standby_transport_binding(),
            )
            .unwrap();
        let OwnerTargetAttachPublication::Installed {
            pending: generation_five_pending,
        } = executor
            .accept_catch_up_owner_attach(
                generation_five_leg.established_leg(),
                generation_five_leg
                    .established_leg()
                    .bind_received_frame(generation_five.to_attach_frame(generation_five_proof)),
                &mut generation_five_queue,
            )
            .unwrap()
        else {
            panic!("second lost-acceptance catch-up must install generation 5")
        };
        assert_eq!(executor.snapshot().session.session.generation().get(), 5);
        assert!(!executor.snapshot().session.owner_catch_up_permit);
        let generation_five_recovery = generation_five_pending
            .enqueue_acceptance(&mut generation_five_queue)
            .unwrap();
        executor
            .execute_attached_recovery(generation_five_recovery, &mut generation_five_queue)
            .unwrap();
        drop((
            wrong_status_queue,
            wrong_status_leg,
            active_queue,
            active_leg,
        ));
    }

    fn committed_leg(generation: u64, nonce: u8, exporter: u8) -> crate::resumable::CommittedLeg {
        let session_id = SessionId::new([0x11; 16]).unwrap();
        let transport = binding(exporter);
        let request = AttachRequest::new(
            session_id,
            LegGeneration::new(generation).unwrap(),
            AttachNonce::new([nonce; 16]).unwrap(),
            VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION).unwrap(),
            FeatureOffer::new(0b111, 0b001).unwrap(),
        );
        let proof = credentials().prove(&request, &transport).unwrap();
        AttachAuthority::new(
            session_id,
            LegGeneration::new(generation - 1).unwrap(),
            credentials(),
            AttachPolicy::new(
                OwnerIdentity::new([0x31; 32]).unwrap(),
                AttachAlpn::new(b"mini-vpn-owned/1").unwrap(),
                DevicePrincipal::new([0x53; 16]).unwrap(),
                SESSION_PROTOCOL_VERSION,
                0b1111,
            )
            .unwrap(),
        )
        .verify_and_commit(&request, &transport, &proof)
        .unwrap()
    }

    fn session_config() -> SessionConfig {
        SessionConfig::new(
            4,
            8,
            TcpWindowLimits::new(128, 16).unwrap(),
            TcpWindowLimits::new(128, 16).unwrap(),
            ReplayBudgetLimits::new(128, 16).unwrap(),
            ReplayBudgetLimits::new(128, 16).unwrap(),
            ReceiveBudgetLimits::new(256, 32).unwrap(),
            128,
        )
        .unwrap()
    }

    fn target(port: u16) -> TargetAddr {
        TargetAddr::IpPort(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port)))
    }

    fn source_config() -> FlowPortConfig {
        FlowPortConfig::new(128, 8, 8, 8).unwrap()
    }

    fn executor() -> (
        crate::resumable::CommittedLeg,
        OwnerTargetExecutor<MemoryTarget>,
    ) {
        let leg = committed_leg(2, 0x22, 0x42);
        let model = SessionModel::new(SessionRole::Owner, session_config(), leg);
        let supervisor = SessionSupervisor::new(model);
        let target = MemoryTarget::new(MemoryTargetConfig::new(4, 256, 512, 128, 128, 16).unwrap());
        (
            leg,
            OwnerTargetExecutor::new(
                OwnerTargetConfig::new(128, 1_024, 64).unwrap(),
                supervisor,
                target,
                source_config(),
            )
            .unwrap(),
        )
    }

    fn attach_executor() -> OwnerTargetExecutor<MemoryTarget> {
        let initial = committed_leg(2, 0x22, 0x42);
        let model = SessionModel::new(SessionRole::Owner, session_config(), initial);
        let authority = AttachAuthority::new(
            SessionId::new([0x11; 16]).unwrap(),
            LegGeneration::new(2).unwrap(),
            credentials(),
            AttachPolicy::new(
                OwnerIdentity::new([0x31; 32]).unwrap(),
                AttachAlpn::new(b"mini-vpn-owned/1").unwrap(),
                DevicePrincipal::new([0x53; 16]).unwrap(),
                SESSION_PROTOCOL_VERSION,
                0b1111,
            )
            .unwrap(),
        );
        let supervisor = SessionSupervisor::new_owner(model, authority).unwrap();
        OwnerTargetExecutor::new(
            OwnerTargetConfig::new(128, 1_024, 64).unwrap(),
            supervisor,
            MemoryTarget::new(MemoryTargetConfig::new(4, 256, 512, 128, 128, 16).unwrap()),
            source_config(),
        )
        .unwrap()
    }

    #[test]
    fn pending_effect_capacity_is_derived_from_full_recovery_and_work_dag() {
        let config = session_config();
        // 1 activation + 8 tombstones + 3*4 per-flow controls + 16+16
        // directional replay segments.
        assert_eq!(config.owner_recovery_effect_bound(), Some(53));
        // Recovery + two effects per 256 receive-owned bytes + five lifecycle
        // effects per flow + the audited incremental burst of three.
        assert_eq!(config.owner_pending_work_bound(), Some(588));

        let leg = committed_leg(2, 0x22, 0x42);
        let model = SessionModel::new(SessionRole::Owner, config, leg);
        let supervisor = SessionSupervisor::new(model);
        let target_io =
            MemoryTarget::new(MemoryTargetConfig::new(4, 256, 512, 128, 128, 16).unwrap());
        assert!(matches!(
            OwnerTargetExecutor::new(
                OwnerTargetConfig::new(1, 587, 64).unwrap(),
                supervisor,
                target_io,
                source_config(),
            ),
            Err(OwnerTargetConfigError::PendingEffectCapacityTooSmall {
                required: 588,
                available: 587,
            })
        ));

        let model = SessionModel::new(SessionRole::Owner, config, leg);
        let supervisor = SessionSupervisor::new(model);
        let target_io =
            MemoryTarget::new(MemoryTargetConfig::new(4, 256, 512, 128, 128, 16).unwrap());
        assert!(
            OwnerTargetExecutor::new(
                OwnerTargetConfig::new(1, 588, 64).unwrap(),
                supervisor,
                target_io,
                source_config(),
            )
            .is_ok()
        );
    }

    #[test]
    fn constructor_rejects_a_client_role_supervisor_before_factory_mint() {
        let leg = committed_leg(2, 0x22, 0x42);
        let model = SessionModel::new(SessionRole::Client, session_config(), leg);
        let supervisor = SessionSupervisor::new(model);
        let target_io =
            MemoryTarget::new(MemoryTargetConfig::new(4, 256, 512, 128, 128, 16).unwrap());

        assert!(matches!(
            OwnerTargetExecutor::new(
                OwnerTargetConfig::new(128, 1_024, 64).unwrap(),
                supervisor,
                target_io,
                source_config(),
            ),
            Err(OwnerTargetConfigError::OwnerRoleRequired)
        ));
    }

    #[test]
    fn constructor_rejects_per_flow_source_bytes_beyond_exact_reducer_limit() {
        fn construct(
            source_config: FlowPortConfig,
        ) -> Result<OwnerTargetExecutor<MemoryTarget>, OwnerTargetConfigError> {
            let leg = committed_leg(2, 0x22, 0x42);
            let model = SessionModel::new(SessionRole::Owner, session_config(), leg);
            OwnerTargetExecutor::new(
                OwnerTargetConfig::new(128, 1_024, 64).unwrap(),
                SessionSupervisor::new(model),
                MemoryTarget::new(MemoryTargetConfig::new(4, 256, 512, 128, 128, 16).unwrap()),
                source_config,
            )
        }

        assert!(matches!(
            construct(FlowPortConfig::new(129, 8, 8, 8).unwrap()),
            Err(OwnerTargetConfigError::PerFlowBytesExceedReducer {
                requested: 129,
                available: 128,
            })
        ));
    }

    #[test]
    fn owner_target_delegates_private_standby_auth_and_returns_exact_inbound_at_attach_barrier() {
        let (mut executor, active) = bootstrap_standby_executor();
        let mut candidate_b = owner_standby_leg(0x91);
        let inbound_b = classified_standby_inbound(&active, &mut candidate_b, 0x31);
        let before = executor.snapshot();
        let slot = OwnerStandbySlot::new();

        let authenticated: ExactAuthenticatedStandby = executor
            .authenticate_owner_standby(&active, candidate_b.established_leg(), inbound_b)
            .unwrap();
        assert_eq!(
            format!("{authenticated:?}"),
            "ExactAuthenticatedStandby([REDACTED])"
        );
        assert_eq!(executor.snapshot(), before);
        assert!(!slot.registered_is_live());
        drop(authenticated);

        let replacement_binding = binding(0x92);
        let replacement = EstablishedLeg::for_authenticated_transport(replacement_binding);
        let request = standby_attach_request(3, 0x23);
        let proof = credentials().prove(&request, &replacement_binding).unwrap();
        let OwnerTargetAttachPublication::Installed { pending } = executor
            .accept_owner_attach_for_test(
                &replacement,
                replacement.bind_received_frame(request.to_attach_frame(proof)),
            )
            .unwrap()
        else {
            panic!("exact-next attach unexpectedly returned status");
        };
        let mut candidate_c = owner_standby_leg(0x93);
        let inbound_c = classified_standby_inbound(&pending.attached, &mut candidate_c, 0x32);
        let barrier_before = executor.snapshot();
        let inbound_c = match executor.authenticate_owner_standby(
            &pending.attached,
            candidate_c.established_leg(),
            inbound_c,
        ) {
            Err(OwnerTargetError::RejectedOwnerStandbyAuthentication {
                block: OwnerTargetInputBlock::AttachBarrierPending,
                inbound,
            }) => inbound,
            other => panic!("attach barrier must return exact classified B input, got {other:?}"),
        };
        assert_eq!(executor.snapshot(), barrier_before);
        assert!(!slot.registered_is_live());

        let mut queue = LegOutboundQueue::for_leg(&replacement, 8, 32_768).unwrap();
        let recovery = pending.enqueue_acceptance(&mut queue).unwrap();
        let outputs = executor
            .execute_attached_recovery(recovery, &mut queue)
            .unwrap();
        let current_active = outputs
            .into_iter()
            .find_map(|output| match output {
                OwnerTargetRecoveryOutput::RecoveryCompleted { handoff } => Some(handoff.attached),
                _ => None,
            })
            .expect("bounded initial recovery must return the current active leg");

        let after_barrier = executor.snapshot();
        let _: ExactAuthenticatedStandby = executor
            .authenticate_owner_standby(&current_active, candidate_c.established_leg(), *inbound_c)
            .unwrap();
        assert_eq!(executor.snapshot(), after_barrier);
        assert!(!slot.registered_is_live());
    }

    fn peer_frame<T: TargetIo>(
        executor: &mut OwnerTargetExecutor<T>,
        leg: crate::resumable::CommittedLeg,
        record: Record,
    ) -> Result<Vec<OwnerTargetOutput>, OwnerTargetError> {
        executor.apply_event(SessionEvent::PeerFrame {
            leg,
            frame: Frame::try_new(leg.generation(), record).unwrap(),
        })
    }

    fn open_flow<T: TargetIo>(
        executor: &mut OwnerTargetExecutor<T>,
        leg: crate::resumable::CommittedLeg,
        flow_id: SessionFlowId,
        target: TargetAddr,
    ) -> Vec<OwnerTargetOutput> {
        peer_frame(executor, leg, Record::Open { flow_id, target }).unwrap()
    }

    fn drain_resumes<T: TargetIo>(
        executor: &mut OwnerTargetExecutor<T>,
        mut outputs: Vec<OwnerTargetOutput>,
    ) -> Vec<OwnerTargetOutput> {
        let mut turns = 0usize;
        while let Some(index) = outputs
            .iter()
            .position(|output| matches!(output, OwnerTargetOutput::NeedsResume { .. }))
        {
            turns += 1;
            assert!(
                turns <= 64,
                "owned effect continuation failed to make progress"
            );
            let OwnerTargetOutput::NeedsResume { resume } = outputs.remove(index) else {
                unreachable!()
            };
            outputs.extend(executor.resume_pending_effects(resume).unwrap());
        }
        outputs
    }

    fn transmitted(outputs: &[OwnerTargetOutput]) -> Vec<&crate::resumable::Record> {
        outputs
            .iter()
            .filter_map(|output| match output {
                OwnerTargetOutput::Transmit(frame) => Some(frame.record()),
                _ => None,
            })
            .collect()
    }

    fn terminal_grace(outputs: &[OwnerTargetOutput]) -> crate::resumable::TerminalGrace {
        outputs
            .iter()
            .find_map(|output| match output {
                OwnerTargetOutput::TerminalGraceStarted { terminal } => Some(*terminal),
                _ => None,
            })
            .expect("flow completion must publish its reducer-minted terminal capability")
    }

    #[test]
    fn replacement_replay_can_drain_only_through_its_acceptance_queue() {
        let leg2 = committed_leg(2, 0x22, 0x42);
        let model = SessionModel::new(SessionRole::Owner, session_config(), leg2);
        let authority = AttachAuthority::new(
            SessionId::new([0x11; 16]).unwrap(),
            LegGeneration::new(2).unwrap(),
            credentials(),
            AttachPolicy::new(
                OwnerIdentity::new([0x31; 32]).unwrap(),
                AttachAlpn::new(b"mini-vpn-owned/1").unwrap(),
                DevicePrincipal::new([0x53; 16]).unwrap(),
                SESSION_PROTOCOL_VERSION,
                0b1111,
            )
            .unwrap(),
        );
        let supervisor = SessionSupervisor::new_owner(model, authority).unwrap();
        let target_io =
            MemoryTarget::new(MemoryTargetConfig::new(4, 256, 512, 128, 128, 16).unwrap());
        let mut executor = OwnerTargetExecutor::new(
            OwnerTargetConfig::new(128, 1_024, 64).unwrap(),
            supervisor,
            target_io,
            source_config(),
        )
        .unwrap();
        let flow_id = SessionFlowId::new(1).unwrap();
        let target = target(443);

        let first = open_flow(&mut executor, leg2, flow_id, target.clone());
        assert!(matches!(
            transmitted(&first).as_slice(),
            [Record::OpenResult {
                flow_id: observed,
                result: OpenResultCode::Opened,
            }] if *observed == flow_id
        ));
        let duplicate = open_flow(&mut executor, leg2, flow_id, target);
        assert!(matches!(
            transmitted(&duplicate).as_slice(),
            [Record::OpenResult {
                result: OpenResultCode::Opened,
                ..
            }]
        ));
        assert_eq!(executor.target().snapshot().open_attempts, 1);

        executor
            .apply_event(SessionEvent::LegLost { leg: leg2 })
            .unwrap();
        let transport_binding = binding(0x43);
        let mut endpoint = LegIo::for_authenticated_transport(
            LegId::A,
            LegEndpointRole::Owner,
            transport_binding,
            LegIoLimits::new(1_024, 8, 8_192, 8, 8_192).unwrap(),
        );
        let established = endpoint.established_leg();
        let request = AttachRequest::new(
            SessionId::new([0x11; 16]).unwrap(),
            LegGeneration::new(3).unwrap(),
            AttachNonce::new([0x33; 16]).unwrap(),
            VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION).unwrap(),
            FeatureOffer::new(0b111, 0b001).unwrap(),
        );
        let proof = credentials().prove(&request, &transport_binding).unwrap();
        let publication = executor
            .accept_owner_attach_for_test(
                established,
                established.bind_received_frame(request.to_attach_frame(proof)),
            )
            .unwrap();
        let OwnerTargetAttachPublication::Installed { pending } = publication else {
            panic!("exact-next replacement unexpectedly requested resynchronization");
        };
        let acceptance_len = pending.acceptance_encoded_len().unwrap();
        let mut acceptance_queue = LegOutboundQueue::for_leg(established, 1, 1_024).unwrap();
        let enqueued = pending.enqueue_acceptance(&mut acceptance_queue).unwrap();
        assert_eq!(enqueued.generation().get(), 3);
        assert_eq!(acceptance_queue.len(), 1);

        let other_leg = EstablishedLeg::for_authenticated_transport(binding(0x44));
        let mut other_queue = LegOutboundQueue::for_leg(&other_leg, 4, acceptance_len * 4).unwrap();
        let recovery = match executor.execute_attached_recovery(enqueued, &mut other_queue) {
            Err(OwnerTargetError::RejectedAttachedRecovery {
                block: OwnerTargetInputBlock::WrongAcceptanceQueue,
                recovery,
            }) => recovery,
            other => panic!("queue B must not drain queue A recovery, got {other:?}"),
        };
        assert!(other_queue.is_empty());
        assert_eq!(executor.target().snapshot().open_attempts, 1);

        let drain = match executor.execute_attached_recovery(*recovery, &mut acceptance_queue) {
            Err(OwnerTargetError::AttachedRecoveryQueueBlocked {
                kind: LegOutboundQueueErrorKind::CapacityExceeded { .. },
                drain,
            }) => drain,
            other => panic!("full acceptance queue must retain replay drain, got {other:?}"),
        };
        assert_eq!(acceptance_queue.len(), 1);
        assert_eq!(executor.target().snapshot().open_attempts, 1);
        assert!(!format!("{drain:?}").contains("33333333"));

        let drain = match executor.resume_attached_recovery_drain(*drain, &mut other_queue) {
            Err(OwnerTargetError::RejectedAttachedRecoveryDrain {
                block: OwnerTargetInputBlock::WrongAcceptanceQueue,
                drain,
            }) => drain,
            other => panic!("queue B must not resume queue A drain, got {other:?}"),
        };
        let mut transport = CapturingTransport::default();
        acceptance_queue
            .try_flush(&mut endpoint, &mut transport, SimTime::ZERO)
            .unwrap()
            .unwrap();
        let outputs = executor
            .resume_attached_recovery_drain(*drain, &mut acceptance_queue)
            .unwrap();
        assert!(outputs.iter().any(|output| matches!(
            output,
            OwnerTargetRecoveryOutput::LegActivated { generation }
                if generation.get() == 3
        )));
        assert!(outputs.iter().any(|output| matches!(
            output,
            OwnerTargetRecoveryOutput::RecoveryCompleted { handoff }
                if handoff.generation().get() == 3
        )));
        assert_eq!(acceptance_queue.len(), 1);
        acceptance_queue
            .try_flush(&mut endpoint, &mut transport, SimTime::ZERO)
            .unwrap()
            .unwrap();
        assert_eq!(transport.frames.len(), 2);
        assert!(matches!(
            Frame::decode_owned_exact(Bytes::from(transport.frames[0].clone()))
                .unwrap()
                .record(),
            Record::AttachAccepted { .. }
        ));
        assert!(matches!(
            Frame::decode_owned_exact(Bytes::from(transport.frames[1].clone()))
                .unwrap()
                .record(),
            Record::OpenResult { flow_id: observed, result: OpenResultCode::Opened }
                if *observed == flow_id
        ));
        assert_eq!(executor.target().snapshot().open_attempts, 1);
        assert_eq!(executor.target().snapshot().opened_flows, 1);
    }

    #[test]
    fn terminalized_recovery_drain_retains_queue_bytes_until_explicit_discard() {
        let leg2 = committed_leg(2, 0x22, 0x42);
        let model = SessionModel::new(SessionRole::Owner, session_config(), leg2);
        let authority = AttachAuthority::new(
            SessionId::new([0x11; 16]).unwrap(),
            LegGeneration::new(2).unwrap(),
            credentials(),
            AttachPolicy::new(
                OwnerIdentity::new([0x31; 32]).unwrap(),
                AttachAlpn::new(b"mini-vpn-owned/1").unwrap(),
                DevicePrincipal::new([0x53; 16]).unwrap(),
                SESSION_PROTOCOL_VERSION,
                0b1111,
            )
            .unwrap(),
        );
        let supervisor = SessionSupervisor::new_owner(model, authority).unwrap();
        let mut executor = OwnerTargetExecutor::new(
            OwnerTargetConfig::new(128, 1_024, 64).unwrap(),
            supervisor,
            MemoryTarget::new(MemoryTargetConfig::new(4, 256, 512, 128, 128, 16).unwrap()),
            source_config(),
        )
        .unwrap();
        let flow_id = SessionFlowId::new(1).unwrap();
        open_flow(&mut executor, leg2, flow_id, target(443));
        executor
            .apply_event(SessionEvent::LegLost { leg: leg2 })
            .unwrap();

        let transport_binding = binding(0x43);
        let (endpoint, reporter) = LegIo::for_authenticated_transport_with_reporter(
            LegId::A,
            LegEndpointRole::Owner,
            transport_binding,
            LegIoLimits::new(1_024, 8, 8_192, 8, 8_192).unwrap(),
        );
        let established = endpoint.established_leg();
        let request = AttachRequest::new(
            SessionId::new([0x11; 16]).unwrap(),
            LegGeneration::new(3).unwrap(),
            AttachNonce::new([0x33; 16]).unwrap(),
            VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION).unwrap(),
            FeatureOffer::new(0b111, 0b001).unwrap(),
        );
        let proof = credentials().prove(&request, &transport_binding).unwrap();
        let OwnerTargetAttachPublication::Installed { pending } = executor
            .accept_owner_attach_for_test(
                established,
                established.bind_received_frame(request.to_attach_frame(proof)),
            )
            .unwrap()
        else {
            panic!("exact-next replacement must install")
        };
        let mut queue = LegOutboundQueue::for_leg(established, 1, 1_024).unwrap();
        let enqueued = pending.enqueue_acceptance(&mut queue).unwrap();
        let drain = match executor.execute_attached_recovery(enqueued, &mut queue) {
            Err(OwnerTargetError::AttachedRecoveryQueueBlocked {
                kind: LegOutboundQueueErrorKind::CapacityExceeded { .. },
                drain,
            }) => *drain,
            other => panic!("full acceptance queue must retain the recovery drain, got {other:?}"),
        };
        let queue_before = (queue.len(), queue.owned_bytes());
        let (wrong_leg, wrong_reporter) =
            EstablishedLeg::for_authenticated_transport_with_reporter(binding(0x44));
        let wrong_terminal = wrong_reporter
            .report(LegTransportTerminalReason::PeerClosed)
            .unwrap();
        let (drain, wrong_terminal) =
            match executor.terminalize_attached_recovery_drain(drain, wrong_terminal) {
                Err(OwnerTargetError::RejectedAttachedRecoveryDrainTerminalization {
                    block: OwnerTargetInputBlock::WrongLegTerminal,
                    drain,
                    terminal,
                }) => (*drain, *terminal),
                other => panic!("wrong terminal must return drain and terminal, got {other:?}"),
            };
        assert_eq!(
            wrong_terminal.reason(),
            LegTransportTerminalReason::PeerClosed
        );
        assert_eq!((queue.len(), queue.owned_bytes()), queue_before);
        assert_eq!(
            executor.snapshot().session.session.phase(),
            SessionPhase::Active
        );
        let terminal = reporter
            .report(LegTransportTerminalReason::FatalIo)
            .unwrap();
        let terminalized = executor
            .terminalize_attached_recovery_drain(drain, terminal)
            .unwrap();
        assert!(format!("{terminalized:?}").contains("Drain"));
        assert_eq!((queue.len(), queue.owned_bytes()), queue_before);
        assert_eq!(
            executor.snapshot().session.session.phase(),
            SessionPhase::Active
        );
        assert!(executor.attach_barrier_pending);
        let high_water = executor
            .discard_terminal_attach_recovery(terminalized)
            .unwrap();
        assert_eq!(high_water.generation().get(), 3);
        assert_eq!(
            executor.snapshot().session.session.phase(),
            SessionPhase::Legless
        );
        assert!(!executor.attach_barrier_pending);
        assert_eq!((queue.len(), queue.owned_bytes()), queue_before);
        drop((endpoint, wrong_leg, wrong_terminal));
    }

    #[test]
    fn owner_attach_recovery_requires_exact_acceptance_queue_admission() {
        let initial = committed_leg(2, 0x22, 0x42);
        let model = SessionModel::new(SessionRole::Owner, session_config(), initial);
        let authority = AttachAuthority::new(
            SessionId::new([0x11; 16]).unwrap(),
            LegGeneration::new(2).unwrap(),
            credentials(),
            AttachPolicy::new(
                OwnerIdentity::new([0x31; 32]).unwrap(),
                AttachAlpn::new(b"mini-vpn-owned/1").unwrap(),
                DevicePrincipal::new([0x53; 16]).unwrap(),
                SESSION_PROTOCOL_VERSION,
                0b1111,
            )
            .unwrap(),
        );
        let supervisor = SessionSupervisor::new_owner(model, authority).unwrap();
        let target_io =
            MemoryTarget::new(MemoryTargetConfig::new(4, 256, 512, 128, 128, 16).unwrap());
        let mut executor = OwnerTargetExecutor::new(
            OwnerTargetConfig::new(128, 1_024, 64).unwrap(),
            supervisor,
            target_io,
            source_config(),
        )
        .unwrap();

        let transport_binding = binding(0x43);
        let (established, reporter) =
            EstablishedLeg::for_authenticated_transport_with_reporter(transport_binding);
        let request = AttachRequest::new(
            SessionId::new([0x11; 16]).unwrap(),
            LegGeneration::new(3).unwrap(),
            AttachNonce::new([0x33; 16]).unwrap(),
            VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION).unwrap(),
            FeatureOffer::new(0b111, 0b001).unwrap(),
        );
        let proof = credentials().prove(&request, &transport_binding).unwrap();
        let publication = executor
            .accept_owner_attach_for_test(
                &established,
                established.bind_received_frame(request.to_attach_frame(proof)),
            )
            .unwrap();

        let OwnerTargetAttachPublication::Installed { pending } = publication else {
            panic!("exact-next attach unexpectedly requested resynchronization");
        };
        assert_eq!(executor.snapshot().session.session.generation().get(), 3);

        let acceptance_len = pending.acceptance_encoded_len().unwrap();
        let mut queue = LegOutboundQueue::for_leg(&established, 1, acceptance_len).unwrap();
        let enqueued = pending.enqueue_acceptance(&mut queue).unwrap();
        assert_eq!(enqueued.generation().get(), 3);
        assert_eq!(queue.len(), 1);

        let (_, mut wrong_executor) = self::executor();
        let enqueued = match wrong_executor.execute_attached_recovery(enqueued, &mut queue) {
            Err(OwnerTargetError::RejectedAttachedRecovery {
                block: OwnerTargetInputBlock::WrongOwnerTargetExecutor,
                recovery,
            }) => *recovery,
            other => panic!("another executor must return the exact recovery, got {other:?}"),
        };
        assert_eq!(wrong_executor.target().snapshot().open_attempts, 0);

        let endpoint_liveness = established.endpoint_liveness();
        let terminal = reporter
            .report(LegTransportTerminalReason::FatalIo)
            .unwrap();
        assert!(endpoint_liveness.upgrade().is_some());
        let recovery = match executor.execute_attached_recovery(enqueued, &mut queue) {
            Err(OwnerTargetError::RejectedAttachedRecovery {
                block: OwnerTargetInputBlock::AcceptanceQueueLost,
                recovery,
            }) => recovery,
            other => panic!("terminal endpoint must retain recovery, got {other:?}"),
        };
        assert!(format!("{recovery:?}").contains("queue_live: false"));
        let (recovery, terminal) =
            match wrong_executor.terminalize_enqueued_attach_recovery(*recovery, terminal) {
                Err(OwnerTargetError::RejectedAttachedRecoveryTerminalization {
                    block: OwnerTargetInputBlock::WrongOwnerTargetExecutor,
                    recovery,
                    terminal,
                }) => (*recovery, *terminal),
                other => panic!("wrong executor must return recovery and terminal, got {other:?}"),
            };
        let terminalized = executor
            .terminalize_enqueued_attach_recovery(recovery, terminal)
            .unwrap();
        assert_eq!(terminalized.generation().get(), 3);
        assert_eq!(terminalized.reason(), LegTransportTerminalReason::FatalIo);
        assert_eq!(
            executor.snapshot().session.session.phase(),
            SessionPhase::Active
        );
        assert!(executor.attach_barrier_pending);
        let high_water = executor
            .discard_terminal_attach_recovery(terminalized)
            .unwrap();
        assert!(!executor.attach_barrier_pending);
        assert_eq!(
            executor.snapshot().session.session.phase(),
            SessionPhase::Legless
        );
        assert_eq!(high_water.generation().get(), 3);
        drop((queue, established));
    }

    #[test]
    fn endpoint_loss_without_terminal_retains_the_failed_acceptance_publication() {
        let mut executor = attach_executor();
        let transport_binding = binding(0x63);
        let established = EstablishedLeg::for_authenticated_transport(transport_binding);
        let request = AttachRequest::new(
            SessionId::new([0x11; 16]).unwrap(),
            LegGeneration::new(3).unwrap(),
            AttachNonce::new([0x63; 16]).unwrap(),
            VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION).unwrap(),
            FeatureOffer::new(0b111, 0b001).unwrap(),
        );
        let proof = credentials().prove(&request, &transport_binding).unwrap();
        let OwnerTargetAttachPublication::Installed { pending } = executor
            .accept_owner_attach_for_test(
                &established,
                established.bind_received_frame(request.to_attach_frame(proof)),
            )
            .unwrap()
        else {
            panic!("exact-next attach unexpectedly requested resynchronization")
        };
        let mut queue = LegOutboundQueue::for_leg(&established, 4, 4_096).unwrap();
        drop(established);
        let enqueue_error = pending.enqueue_acceptance(&mut queue).unwrap_err();
        assert_eq!(
            enqueue_error.kind(),
            &LegOutboundQueueErrorKind::EndpointLost
        );
        let pending = enqueue_error.into_publication();
        assert_eq!(executor.snapshot().session.session.generation().get(), 3);
        assert_eq!(
            executor.snapshot().session.session.phase(),
            SessionPhase::Active
        );
        assert!(executor.attach_barrier_pending);
        assert_eq!(
            pending.acceptance_encoded_len().unwrap(),
            ATTACH_ACCEPTED_ENCODED_BYTES
        );
    }

    #[test]
    fn endpoint_handle_loss_without_terminal_keeps_the_bounded_publication() {
        let mut executor = attach_executor();
        let transport_binding = binding(0x73);
        let established = EstablishedLeg::for_authenticated_transport(transport_binding);
        let request = AttachRequest::new(
            SessionId::new([0x11; 16]).unwrap(),
            LegGeneration::new(3).unwrap(),
            AttachNonce::new([0x73; 16]).unwrap(),
            VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION).unwrap(),
            FeatureOffer::new(0b111, 0b001).unwrap(),
        );
        let proof = credentials().prove(&request, &transport_binding).unwrap();
        let OwnerTargetAttachPublication::Installed { pending } = executor
            .accept_owner_attach_for_test(
                &established,
                established.bind_received_frame(request.to_attach_frame(proof)),
            )
            .unwrap()
        else {
            panic!("exact-next attach unexpectedly requested resynchronization")
        };

        drop(established);
        assert_eq!(executor.snapshot().session.session.generation().get(), 3);
        assert_eq!(
            executor.snapshot().session.session.phase(),
            SessionPhase::Active
        );
        assert!(executor.attach_barrier_pending);
        assert_eq!(
            pending.acceptance_encoded_len().unwrap(),
            ATTACH_ACCEPTED_ENCODED_BYTES
        );
    }

    #[test]
    fn pending_registered_acceptance_requires_exact_terminal_then_recovers_from_legless() {
        let (mut executor, active) = bootstrap_standby_executor();
        let transport_binding = binding(0x76);
        let (mut endpoint, reporter) = LegIo::for_authenticated_transport_with_reporter(
            LegId::B,
            LegEndpointRole::Owner,
            transport_binding,
            LegIoLimits::new(4_096, 8, 32_768, 8, 32_768).unwrap(),
        );
        let inbound = classified_standby_inbound(&active, &mut endpoint, 0x76);
        let authenticated = executor
            .authenticate_owner_standby(&active, endpoint.established_leg(), inbound)
            .unwrap();
        let mut acceptance_queue =
            LegOutboundQueue::for_leg(endpoint.established_leg(), 8, 32_768).unwrap();
        let mut slot = OwnerStandbySlot::new();
        assert_eq!(
            slot.admit(authenticated, &mut acceptance_queue).unwrap(),
            OwnerStandbyAdmission::Installed
        );
        let mut standby_transport = CapturingTransport::default();
        acceptance_queue
            .try_flush(&mut endpoint, &mut standby_transport, SimTime::ZERO)
            .unwrap()
            .unwrap();

        let request = registered_exact_request(3, 0x76);
        let proof = credentials().prove(&request, &transport_binding).unwrap();
        let OwnerTargetAttachPublication::Installed { pending } = executor
            .accept_registered_owner_attach(
                &mut slot,
                endpoint.established_leg(),
                endpoint
                    .established_leg()
                    .bind_received_frame(request.to_attach_frame(proof)),
                &mut acceptance_queue,
            )
            .unwrap()
        else {
            panic!("registered exact-next attach unexpectedly requested resynchronization")
        };

        let terminal = reporter
            .report(LegTransportTerminalReason::PeerClosed)
            .unwrap();
        assert_eq!(terminal.reason(), LegTransportTerminalReason::PeerClosed);
        let (_, mut wrong_executor) = self::executor();
        let (pending, terminal) = match wrong_executor
            .terminalize_pending_attach_before_acceptance(pending, terminal)
        {
            Err(OwnerTargetError::RejectedPendingAttachLoss {
                block: OwnerTargetInputBlock::WrongOwnerTargetExecutor,
                pending,
                terminal,
            }) => (*pending, *terminal),
            other => panic!("wrong executor must return publication and terminal, got {other:?}"),
        };
        let terminalized = executor
            .terminalize_pending_attach_before_acceptance(pending, terminal)
            .unwrap();
        assert_eq!(terminalized.generation().get(), 3);
        assert_eq!(
            terminalized.reason(),
            LegTransportTerminalReason::PeerClosed
        );
        assert_eq!(
            executor.snapshot().session.session.phase(),
            SessionPhase::Active
        );
        assert!(executor.attach_barrier_pending);
        let high_water = executor
            .discard_terminal_attach_recovery(terminalized)
            .unwrap();
        assert_eq!(executor.snapshot().session.session.generation().get(), 3);
        assert_eq!(
            executor.snapshot().session.session.phase(),
            SessionPhase::Legless
        );
        assert_eq!(high_water.generation().get(), 3);
        assert_eq!(high_water.reason(), LegTransportTerminalReason::PeerClosed);
        assert!(!executor.attach_barrier_pending);

        let status_request = registered_exact_request(3, 0x77);
        let mut status_leg = owner_standby_leg(0x77);
        let mut status_queue =
            LegOutboundQueue::for_leg(status_leg.established_leg(), 4, 4_096).unwrap();
        let status_proof = credentials()
            .prove(
                &status_request,
                &status_leg.established_leg().standby_transport_binding(),
            )
            .unwrap();
        let status = executor
            .resynchronize_owner_attach(
                status_leg.established_leg(),
                status_leg
                    .established_leg()
                    .bind_received_frame(status_request.to_attach_frame(status_proof)),
            )
            .unwrap()
            .enqueue(&mut status_queue)
            .unwrap();
        let mut status_transport = CapturingTransport::default();
        status_queue
            .try_flush(&mut status_leg, &mut status_transport, SimTime::ZERO)
            .unwrap()
            .unwrap();
        executor
            .arm_owner_catch_up_after_status_delivery(status, &status_queue)
            .unwrap();

        let next_request = registered_exact_request(4, 0x78);
        let next_leg = owner_standby_leg(0x78);
        let mut next_queue =
            LegOutboundQueue::for_leg(next_leg.established_leg(), 4, 4_096).unwrap();
        let next_proof = credentials()
            .prove(
                &next_request,
                &next_leg.established_leg().standby_transport_binding(),
            )
            .unwrap();
        let OwnerTargetAttachPublication::Installed { pending } = executor
            .accept_catch_up_owner_attach(
                next_leg.established_leg(),
                next_leg
                    .established_leg()
                    .bind_received_frame(next_request.to_attach_frame(next_proof)),
                &mut next_queue,
            )
            .unwrap()
        else {
            panic!("authenticated high-water successor must install")
        };
        let recovery = pending.enqueue_acceptance(&mut next_queue).unwrap();
        executor
            .execute_attached_recovery(recovery, &mut next_queue)
            .unwrap();
        assert_eq!(executor.snapshot().session.session.generation().get(), 4);
        assert_eq!(
            executor.snapshot().session.session.phase(),
            SessionPhase::Active
        );
        let high_water = match executor.expire_legless_high_water(high_water) {
            Err(OwnerTargetError::RejectedLeglessHighWater { high_water }) => *high_water,
            other => panic!("recovered owner must return the stale high-water, got {other:?}"),
        };
        assert_eq!(high_water.generation().get(), 3);
    }

    #[test]
    fn sole_queue_loss_without_exact_terminal_retains_bounded_publication() {
        let mut executor = attach_executor();
        let transport_binding = binding(0x66);
        let established = EstablishedLeg::for_authenticated_transport(transport_binding);
        let request = AttachRequest::new(
            SessionId::new([0x11; 16]).unwrap(),
            LegGeneration::new(3).unwrap(),
            AttachNonce::new([0x66; 16]).unwrap(),
            VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION).unwrap(),
            FeatureOffer::new(0b111, 0b001).unwrap(),
        );
        let proof = credentials().prove(&request, &transport_binding).unwrap();
        let OwnerTargetAttachPublication::Installed { pending } = executor
            .accept_owner_attach_for_test(
                &established,
                established.bind_received_frame(request.to_attach_frame(proof)),
            )
            .unwrap()
        else {
            panic!("exact-next attach unexpectedly requested resynchronization")
        };
        let queue = LegOutboundQueue::for_leg(&established, 4, 4_096).unwrap();

        drop(queue);
        let (wrong_leg, wrong_reporter) =
            EstablishedLeg::for_authenticated_transport_with_reporter(binding(0x67));
        let wrong_terminal = wrong_reporter
            .report(LegTransportTerminalReason::FatalIo)
            .unwrap();
        let before = executor.snapshot();
        let (pending, wrong_terminal) = match executor
            .terminalize_pending_attach_before_acceptance(pending, wrong_terminal)
        {
            Err(OwnerTargetError::RejectedPendingAttachLoss {
                block: OwnerTargetInputBlock::WrongLegTerminal,
                pending,
                terminal,
            }) => (*pending, *terminal),
            other => panic!("queue loss must not replace exact terminal authority, got {other:?}"),
        };
        assert_eq!(wrong_terminal.reason(), LegTransportTerminalReason::FatalIo);
        assert_eq!(executor.snapshot(), before);
        assert_eq!(executor.snapshot().session.session.generation().get(), 3);
        assert_eq!(
            executor.snapshot().session.session.phase(),
            SessionPhase::Active
        );
        assert!(executor.attach_barrier_pending);
        assert_eq!(
            pending.acceptance_encoded_len().unwrap(),
            ATTACH_ACCEPTED_ENCODED_BYTES
        );
        drop((wrong_leg, established));
    }

    #[test]
    fn owner_attach_acceptance_rejects_a_queue_minted_by_another_leg() {
        let initial = committed_leg(2, 0x22, 0x42);
        let model = SessionModel::new(SessionRole::Owner, session_config(), initial);
        let authority = AttachAuthority::new(
            SessionId::new([0x11; 16]).unwrap(),
            LegGeneration::new(2).unwrap(),
            credentials(),
            AttachPolicy::new(
                OwnerIdentity::new([0x31; 32]).unwrap(),
                AttachAlpn::new(b"mini-vpn-owned/1").unwrap(),
                DevicePrincipal::new([0x53; 16]).unwrap(),
                SESSION_PROTOCOL_VERSION,
                0b1111,
            )
            .unwrap(),
        );
        let supervisor = SessionSupervisor::new_owner(model, authority).unwrap();
        let mut executor = OwnerTargetExecutor::new(
            OwnerTargetConfig::new(128, 1_024, 64).unwrap(),
            supervisor,
            MemoryTarget::new(MemoryTargetConfig::new(4, 256, 512, 128, 128, 16).unwrap()),
            source_config(),
        )
        .unwrap();

        let accepted_binding = binding(0x43);
        let accepted_leg = EstablishedLeg::for_authenticated_transport(accepted_binding);
        let wrong_leg = EstablishedLeg::for_authenticated_transport(binding(0x44));
        let request = AttachRequest::new(
            SessionId::new([0x11; 16]).unwrap(),
            LegGeneration::new(3).unwrap(),
            AttachNonce::new([0x33; 16]).unwrap(),
            VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION).unwrap(),
            FeatureOffer::new(0b111, 0b001).unwrap(),
        );
        let proof = credentials().prove(&request, &accepted_binding).unwrap();
        let OwnerTargetAttachPublication::Installed { pending } = executor
            .accept_owner_attach_for_test(
                &accepted_leg,
                accepted_leg.bind_received_frame(request.to_attach_frame(proof)),
            )
            .unwrap()
        else {
            panic!("exact-next attach unexpectedly requested resynchronization")
        };

        let acceptance_len = pending.acceptance_encoded_len().unwrap();
        let mut wrong_queue = LegOutboundQueue::for_leg(&wrong_leg, 2, acceptance_len * 2).unwrap();
        let error = pending.enqueue_acceptance(&mut wrong_queue).unwrap_err();

        assert_eq!(error.kind(), &LegOutboundQueueErrorKind::WrongLeg);
        assert!(wrong_queue.is_empty());
        let pending = error.into_publication();
        assert_eq!(pending.acceptance_encoded_len().unwrap(), acceptance_len);
        assert!(!format!("{pending:?}").contains("33333333"));
    }

    #[test]
    fn pending_turn_returns_exact_attach_and_post_acceptance_recovery_ownership() {
        let initial = committed_leg(2, 0x22, 0x42);
        let model = SessionModel::new(SessionRole::Owner, session_config(), initial);
        let authority = AttachAuthority::new(
            SessionId::new([0x11; 16]).unwrap(),
            LegGeneration::new(2).unwrap(),
            credentials(),
            AttachPolicy::new(
                OwnerIdentity::new([0x31; 32]).unwrap(),
                AttachAlpn::new(b"mini-vpn-owned/1").unwrap(),
                DevicePrincipal::new([0x53; 16]).unwrap(),
                SESSION_PROTOCOL_VERSION,
                0b1111,
            )
            .unwrap(),
        );
        let supervisor = SessionSupervisor::new_owner(model, authority).unwrap();
        let mut executor = OwnerTargetExecutor::new(
            OwnerTargetConfig::new(1, 1_024, 64).unwrap(),
            supervisor,
            MemoryTarget::new(MemoryTargetConfig::new(4, 256, 512, 128, 128, 16).unwrap()),
            source_config(),
        )
        .unwrap();
        let flow_id = SessionFlowId::new(1).unwrap();

        let mut open = peer_frame(
            &mut executor,
            initial,
            Record::Open {
                flow_id,
                target: target(443),
            },
        )
        .unwrap();
        let OwnerTargetOutput::NeedsResume {
            resume: open_resume,
        } = open.pop().unwrap()
        else {
            panic!("one-step OPEN must retain its generated result")
        };

        let transport_binding = binding(0x43);
        let established = EstablishedLeg::for_authenticated_transport(transport_binding);
        let request = AttachRequest::new(
            SessionId::new([0x11; 16]).unwrap(),
            LegGeneration::new(3).unwrap(),
            AttachNonce::new([0x33; 16]).unwrap(),
            VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION).unwrap(),
            FeatureOffer::new(0b111, 0b001).unwrap(),
        );
        let proof = credentials().prove(&request, &transport_binding).unwrap();
        let received = established.bind_received_frame(request.to_attach_frame(proof));
        let received = match executor.accept_owner_attach_for_test(&established, received) {
            Err(OwnerTargetError::RejectedOwnerAttach {
                block: OwnerTargetInputBlock::PendingEffectTurn,
                received,
            }) => received,
            other => panic!("pending OPEN must return the exact ATTACH, got {other:?}"),
        };
        assert!(
            transmitted(&executor.resume_pending_effects(open_resume).unwrap())
                .iter()
                .any(|record| matches!(
                    record,
                    Record::OpenResult { flow_id: observed, .. } if *observed == flow_id
                ))
        );

        let publication = executor
            .accept_owner_attach_for_test(&established, *received)
            .unwrap();
        let OwnerTargetAttachPublication::Installed { pending } = publication else {
            panic!("retained exact-next ATTACH must install")
        };
        let acceptance_len = pending.acceptance_encoded_len().unwrap();
        let mut outbound = LegOutboundQueue::for_leg(&established, 4, acceptance_len * 4).unwrap();
        let enqueued = pending.enqueue_acceptance(&mut outbound).unwrap();
        assert!(matches!(
            executor.apply_event(SessionEvent::LegLost { leg: initial }),
            Err(OwnerTargetError::RejectedEvent {
                block: OwnerTargetInputBlock::AttachBarrierPending,
                ..
            })
        ));
        assert!(matches!(
            executor.try_read_target(flow_id),
            Err(OwnerTargetError::AttachBarrierPending)
        ));

        let other_leg = EstablishedLeg::for_authenticated_transport(binding(0x44));
        let mut other_queue = LegOutboundQueue::for_leg(&other_leg, 4, acceptance_len * 4).unwrap();
        let mut recovered = executor
            .execute_attached_recovery(enqueued, &mut outbound)
            .unwrap();
        let mut activated = None;
        let mut attached = None;
        loop {
            let mut resume = None;
            for output in recovered {
                match output {
                    OwnerTargetRecoveryOutput::LegActivated { generation } => {
                        activated = Some(generation)
                    }
                    OwnerTargetRecoveryOutput::NeedsResume { resume: next } => {
                        assert!(resume.replace(next).is_none());
                    }
                    OwnerTargetRecoveryOutput::RecoveryCompleted { handoff } => {
                        assert!(attached.replace(handoff.attached).is_none());
                    }
                    OwnerTargetRecoveryOutput::ResumeGraceStarted { .. }
                    | OwnerTargetRecoveryOutput::TerminalGraceStarted { .. }
                    | OwnerTargetRecoveryOutput::SessionExpired => {}
                }
            }
            let Some(resume) = resume else {
                break;
            };
            let resume = match executor.resume_attached_recovery_effects(resume, &mut other_queue) {
                Err(OwnerTargetError::RejectedAttachedRecoveryResume {
                    block: OwnerTargetInputBlock::WrongAcceptanceQueue,
                    recovery,
                }) => *recovery,
                other => panic!("recovery continuation escaped to queue B: {other:?}"),
            };
            recovered = executor
                .resume_attached_recovery_effects(resume, &mut outbound)
                .unwrap();
        }
        let attached = attached.expect("recovery completion must release attached authority");
        assert_eq!(activated, Some(attached.generation()));
        assert_eq!(attached.nonce(), request.nonce());
        assert_eq!(
            LegOutboundQueue::for_leg(&established, 4, acceptance_len * 4).unwrap_err(),
            LegIoConfigError::OutboundQueueAlreadyMinted
        );

        let data_event = attached
            .accept_frame(
                established.bind_received_frame(
                    Frame::try_new(
                        attached.generation(),
                        Record::Data {
                            flow_id,
                            direction: Direction::ClientToTarget,
                            offset: ByteOffset::new(0),
                            payload: Bytes::from_static(b"x"),
                        },
                    )
                    .unwrap(),
                ),
            )
            .unwrap();
        let mut data = executor.apply_event(data_event).unwrap();
        let index = data
            .iter()
            .position(|output| matches!(output, OwnerTargetOutput::NeedsResume { .. }))
            .expect("one-step DATA must retain its generated ACK");
        let OwnerTargetOutput::NeedsResume {
            resume: data_resume,
        } = data.remove(index)
        else {
            unreachable!()
        };
        assert!(
            transmitted(&executor.resume_pending_effects(data_resume).unwrap())
                .iter()
                .any(|record| matches!(
                    record,
                    Record::Ack { flow_id: observed, .. } if *observed == flow_id
                ))
        );
    }

    #[test]
    fn terminalized_recovery_resume_retains_pending_turn_until_explicit_discard() {
        let initial = committed_leg(2, 0x22, 0x42);
        let model = SessionModel::new(SessionRole::Owner, session_config(), initial);
        let authority = AttachAuthority::new(
            SessionId::new([0x11; 16]).unwrap(),
            LegGeneration::new(2).unwrap(),
            credentials(),
            AttachPolicy::new(
                OwnerIdentity::new([0x31; 32]).unwrap(),
                AttachAlpn::new(b"mini-vpn-owned/1").unwrap(),
                DevicePrincipal::new([0x53; 16]).unwrap(),
                SESSION_PROTOCOL_VERSION,
                0b1111,
            )
            .unwrap(),
        );
        let supervisor = SessionSupervisor::new_owner(model, authority).unwrap();
        let mut executor = OwnerTargetExecutor::new(
            OwnerTargetConfig::new(1, 1_024, 64).unwrap(),
            supervisor,
            MemoryTarget::new(MemoryTargetConfig::new(4, 256, 512, 128, 128, 16).unwrap()),
            source_config(),
        )
        .unwrap();
        let flow_id = SessionFlowId::new(1).unwrap();
        let mut open = peer_frame(
            &mut executor,
            initial,
            Record::Open {
                flow_id,
                target: target(443),
            },
        )
        .unwrap();
        let OwnerTargetOutput::NeedsResume {
            resume: open_resume,
        } = open.pop().unwrap()
        else {
            panic!("one-step OPEN must retain its generated result")
        };
        executor.resume_pending_effects(open_resume).unwrap();

        let transport_binding = binding(0x43);
        let (endpoint, reporter) = LegIo::for_authenticated_transport_with_reporter(
            LegId::A,
            LegEndpointRole::Owner,
            transport_binding,
            LegIoLimits::new(1_024, 8, 8_192, 8, 8_192).unwrap(),
        );
        let established = endpoint.established_leg();
        let request = AttachRequest::new(
            SessionId::new([0x11; 16]).unwrap(),
            LegGeneration::new(3).unwrap(),
            AttachNonce::new([0x33; 16]).unwrap(),
            VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION).unwrap(),
            FeatureOffer::new(0b111, 0b001).unwrap(),
        );
        let proof = credentials().prove(&request, &transport_binding).unwrap();
        let OwnerTargetAttachPublication::Installed { pending } = executor
            .accept_owner_attach_for_test(
                established,
                established.bind_received_frame(request.to_attach_frame(proof)),
            )
            .unwrap()
        else {
            panic!("exact-next replacement must install")
        };
        let mut queue = LegOutboundQueue::for_leg(established, 4, 4_096).unwrap();
        let enqueued = pending.enqueue_acceptance(&mut queue).unwrap();
        let recovered = executor
            .execute_attached_recovery(enqueued, &mut queue)
            .unwrap();
        let resume = recovered
            .into_iter()
            .find_map(|output| match output {
                OwnerTargetRecoveryOutput::NeedsResume { resume } => Some(resume),
                OwnerTargetRecoveryOutput::LegActivated { .. }
                | OwnerTargetRecoveryOutput::ResumeGraceStarted { .. }
                | OwnerTargetRecoveryOutput::TerminalGraceStarted { .. }
                | OwnerTargetRecoveryOutput::SessionExpired
                | OwnerTargetRecoveryOutput::RecoveryCompleted { .. } => None,
            })
            .expect("one-step recovery must retain its next effect turn");
        let queue_before = (queue.len(), queue.owned_bytes());
        let terminal = reporter
            .report(LegTransportTerminalReason::PeerClosed)
            .unwrap();
        let terminalized = executor
            .terminalize_attached_recovery_resume(resume, terminal)
            .unwrap();
        assert!(format!("{terminalized:?}").contains("Resume"));
        assert_eq!((queue.len(), queue.owned_bytes()), queue_before);
        assert_eq!(
            executor.snapshot().session.session.phase(),
            SessionPhase::Active
        );
        assert!(executor.snapshot().needs_resume);
        assert!(executor.attach_barrier_pending);

        let (_, mut wrong_executor) = self::executor();
        let terminalized = match wrong_executor.discard_terminal_attach_recovery(terminalized) {
            Err(OwnerTargetError::RejectedTerminalAttachRecoveryDiscard {
                block: OwnerTargetInputBlock::WrongOwnerTargetExecutor,
                pending,
            }) => *pending,
            other => panic!("wrong executor must return terminal recovery, got {other:?}"),
        };
        let high_water = executor
            .discard_terminal_attach_recovery(terminalized)
            .unwrap();
        assert_eq!(high_water.generation().get(), 3);
        assert_eq!(
            executor.snapshot().session.session.phase(),
            SessionPhase::Legless
        );
        assert!(!executor.snapshot().needs_resume);
        assert!(!executor.attach_barrier_pending);
        assert_eq!((queue.len(), queue.owned_bytes()), queue_before);
        drop(endpoint);
    }

    #[test]
    fn target_open_capacity_failure_resolves_and_retires_without_fake_target_ownership() {
        let leg = committed_leg(2, 0x22, 0x42);
        let model = SessionModel::new(SessionRole::Owner, session_config(), leg);
        let supervisor = SessionSupervisor::new(model);
        let target_io =
            MemoryTarget::new(MemoryTargetConfig::new(1, 256, 512, 128, 128, 16).unwrap());
        let mut executor = OwnerTargetExecutor::new(
            OwnerTargetConfig::new(128, 1_024, 64).unwrap(),
            supervisor,
            target_io,
            source_config(),
        )
        .unwrap();
        open_flow(
            &mut executor,
            leg,
            SessionFlowId::new(1).unwrap(),
            target(443),
        );

        let rejected = open_flow(
            &mut executor,
            leg,
            SessionFlowId::new(2).unwrap(),
            target(8443),
        );
        assert!(transmitted(&rejected).iter().any(|record| matches!(
            record,
            Record::OpenResult {
                flow_id,
                result: OpenResultCode::ResourceExhausted,
            } if *flow_id == SessionFlowId::new(2).unwrap()
        )));
        assert_eq!(executor.snapshot().target_flows, 1);
        assert_eq!(executor.target().snapshot().open_attempts, 2);
        assert_eq!(executor.target().snapshot().opened_flows, 1);
    }

    #[test]
    fn target_already_terminal_open_is_adopted_and_retires_its_exact_tombstone() {
        let leg = committed_leg(2, 0x22, 0x42);
        let model = SessionModel::new(SessionRole::Owner, session_config(), leg);
        let supervisor = SessionSupervisor::new(model);
        let flow_id = SessionFlowId::new(1).unwrap();
        let target_addr = target(443);
        let mut target_io =
            MemoryTarget::new(MemoryTargetConfig::new(4, 256, 512, 128, 128, 16).unwrap());
        target_io.open(flow_id, &target_addr).unwrap();
        target_io.reset(flow_id).unwrap();
        assert_eq!(
            target_io.join(flow_id),
            Ok(TargetJoinCompletion::Joined(TargetTerminalKind::Reset))
        );
        let mut executor = OwnerTargetExecutor::new(
            OwnerTargetConfig::new(128, 1_024, 64).unwrap(),
            supervisor,
            target_io,
            source_config(),
        )
        .unwrap();

        let outputs = open_flow(&mut executor, leg, flow_id, target_addr);
        assert!(transmitted(&outputs).iter().any(|record| matches!(
            record,
            Record::OpenResult {
                flow_id: observed,
                result: OpenResultCode::Internal,
            } if *observed == flow_id
        )));
        let terminal = terminal_grace(&outputs);
        assert_eq!(executor.snapshot().target_flows, 0);
        assert_eq!(executor.snapshot().target_tombstones, 1);
        assert_eq!(executor.target().snapshot().joined_flows, 1);

        executor
            .apply_event(SessionEvent::TerminalGraceExpired { terminal })
            .unwrap();
        assert_eq!(executor.snapshot().target_tombstones, 0);
        assert_eq!(executor.target().snapshot().joined_flows, 0);
    }

    #[test]
    fn effect_step_budget_returns_one_owned_resume_without_losing_committed_open() {
        let leg = committed_leg(2, 0x22, 0x42);
        let model = SessionModel::new(SessionRole::Owner, session_config(), leg);
        let supervisor = SessionSupervisor::new(model);
        let target_io =
            MemoryTarget::new(MemoryTargetConfig::new(4, 256, 512, 128, 128, 16).unwrap());
        let mut executor = OwnerTargetExecutor::new(
            OwnerTargetConfig::new(1, 1_024, 64).unwrap(),
            supervisor,
            target_io,
            source_config(),
        )
        .unwrap();
        let flow_id = SessionFlowId::new(1).unwrap();

        let mut first = peer_frame(
            &mut executor,
            leg,
            Record::Open {
                flow_id,
                target: target(443),
            },
        )
        .unwrap();
        assert_eq!(executor.target().snapshot().open_attempts, 1);
        assert_eq!(executor.snapshot().target_flows, 1);
        assert_eq!(first.len(), 1);
        let OwnerTargetOutput::NeedsResume { resume } = first.pop().unwrap() else {
            panic!("a committed bounded turn must return one resume capability")
        };

        match executor.apply_event(SessionEvent::LegLost { leg }) {
            Err(OwnerTargetError::RejectedEvent {
                block: OwnerTargetInputBlock::PendingEffectTurn,
                event,
            }) => assert_eq!(*event, SessionEvent::LegLost { leg }),
            other => panic!("pending turn must return exact rejected event, got {other:?}"),
        }
        match executor.apply_command(SessionOwnerCommand::Event(SessionEvent::LegLost { leg })) {
            Err(OwnerTargetError::RejectedCommand {
                block: OwnerTargetInputBlock::PendingEffectTurn,
                command,
            }) => assert!(matches!(
                *command,
                SessionOwnerCommand::Event(SessionEvent::LegLost { leg: observed })
                    if observed == leg
            )),
            other => panic!("pending turn must return exact rejected command, got {other:?}"),
        }
        assert_eq!(
            executor.snapshot().session.session.phase(),
            crate::resumable::SessionPhase::Active
        );

        let resumed = executor.resume_pending_effects(resume).unwrap();
        assert!(transmitted(&resumed).iter().any(|record| matches!(
            record,
            Record::OpenResult {
                flow_id: observed,
                result: OpenResultCode::Opened,
            } if *observed == flow_id
        )));
        assert!(
            !resumed
                .iter()
                .any(|output| matches!(output, OwnerTargetOutput::NeedsResume { .. }))
        );
        assert_eq!(executor.target().snapshot().open_attempts, 1);
    }

    #[test]
    fn impossible_near_capacity_fanout_retains_tail_and_outputs_then_permanently_fails_closed() {
        let (leg, mut executor) = executor();
        let flow_id = SessionFlowId::new(1).unwrap();
        let mut generated = executor
            .supervisor
            .apply_event(SessionEvent::PeerFrame {
                leg,
                frame: Frame::try_new(
                    leg.generation(),
                    Record::Open {
                        flow_id,
                        target: target(443),
                    },
                )
                .unwrap(),
            })
            .unwrap();
        assert_eq!(generated.len(), 1);
        let peer_open = generated.pop().unwrap();

        // Constructor validation makes this state unreachable in production.
        // Shrinking the bound inside the module reproduces the exact historical
        // near-capacity head fan-out: retrying it on every NeedsResume would
        // make no progress forever.
        executor.config.max_pending_effects = 3;
        let mut queue = VecDeque::from([
            OwnerTargetWork::Effect(SessionEffect::LegActivated {
                generation: leg.generation(),
            }),
            OwnerTargetWork::Effect(peer_open),
            OwnerTargetWork::Effect(SessionEffect::ResumeGraceStarted {
                generation: leg.generation(),
            }),
        ]);
        let result = executor.execute_queue(std::mem::take(&mut queue));
        assert!(matches!(
            result,
            Err(OwnerTargetError::PendingEffectCapacityInvariant {
                max: 3,
                attempted: 5,
            })
        ));
        assert_eq!(executor.target().snapshot().open_attempts, 0);
        assert_eq!(executor.snapshot().aborted_effects, 2);
        assert_eq!(executor.snapshot().aborted_outputs, 1);
        assert!(!executor.snapshot().needs_resume);
        match executor.apply_event(SessionEvent::LegLost { leg }) {
            Err(OwnerTargetError::RejectedEvent {
                block: OwnerTargetInputBlock::AbortedEffectInvariant,
                event,
            }) => assert_eq!(*event, SessionEvent::LegLost { leg }),
            other => panic!("aborted owner must return exact rejected event, got {other:?}"),
        }
        assert_eq!(executor.target().snapshot().open_attempts, 0);
        assert_eq!(
            executor.snapshot().session.session.phase(),
            crate::resumable::SessionPhase::Active
        );
    }

    #[test]
    fn one_step_turn_drains_data_close_chain_without_repeating_target_io() {
        let leg = committed_leg(2, 0x22, 0x42);
        let model = SessionModel::new(SessionRole::Owner, session_config(), leg);
        let supervisor = SessionSupervisor::new(model);
        let target_io =
            MemoryTarget::new(MemoryTargetConfig::new(4, 256, 512, 128, 1, 16).unwrap());
        let mut executor = OwnerTargetExecutor::new(
            OwnerTargetConfig::new(1, 1_024, 64).unwrap(),
            supervisor,
            target_io,
            source_config(),
        )
        .unwrap();
        let flow_id = SessionFlowId::new(1).unwrap();

        let opened = peer_frame(
            &mut executor,
            leg,
            Record::Open {
                flow_id,
                target: target(443),
            },
        )
        .unwrap();
        let opened = drain_resumes(&mut executor, opened);
        assert!(transmitted(&opened).iter().any(|record| matches!(
            record,
            Record::OpenResult { flow_id: observed, result: OpenResultCode::Opened }
                if *observed == flow_id
        )));

        let data = peer_frame(
            &mut executor,
            leg,
            Record::Data {
                flow_id,
                direction: Direction::ClientToTarget,
                offset: ByteOffset::new(0),
                payload: Bytes::from_static(b"x"),
            },
        )
        .unwrap();
        let data = drain_resumes(&mut executor, data);
        assert!(transmitted(&data).iter().any(|record| matches!(
            record,
            Record::Ack { flow_id: observed, next_accepted, .. }
                if *observed == flow_id && *next_accepted == ByteOffset::new(1)
        )));
        assert_eq!(
            executor.target_mut().take_written(flow_id, 1).unwrap(),
            Bytes::from_static(b"x")
        );

        let close = peer_frame(
            &mut executor,
            leg,
            Record::Close {
                flow_id,
                direction: Direction::ClientToTarget,
                final_offset: ByteOffset::new(1),
            },
        )
        .unwrap();
        let close = drain_resumes(&mut executor, close);
        assert_eq!(
            close
                .iter()
                .filter(|output| matches!(output, OwnerTargetOutput::TerminalGraceStarted { .. }))
                .count(),
            0,
            "one-sided CLOSE must not finish before Target-to-client EOF"
        );
        assert_eq!(executor.target().snapshot().write_calls, 1);
        assert_eq!(executor.target().snapshot().write_accepted_bytes, 1);
        assert_eq!(executor.target().snapshot().write_half_closes, 1);

        executor.target_mut().finish_read(flow_id).unwrap();
        let eof = executor.try_read_target(flow_id).unwrap();
        let eof = drain_resumes(&mut executor, eof);
        let final_offset = transmitted(&eof)
            .iter()
            .find_map(|record| match record {
                Record::Close {
                    flow_id: observed,
                    direction: Direction::TargetToClient,
                    final_offset,
                } if *observed == flow_id => Some(*final_offset),
                _ => None,
            })
            .unwrap();
        let final_ack = peer_frame(
            &mut executor,
            leg,
            Record::Ack {
                flow_id,
                direction: Direction::TargetToClient,
                next_accepted: final_offset,
                final_accepted: true,
            },
        )
        .unwrap();
        let final_ack = drain_resumes(&mut executor, final_ack);
        assert_eq!(
            final_ack
                .iter()
                .filter(|output| matches!(output, OwnerTargetOutput::TerminalGraceStarted { .. }))
                .count(),
            1
        );
        assert_eq!(executor.target().snapshot().write_calls, 1);
        assert_eq!(executor.target().snapshot().write_half_closes, 1);
        assert_eq!(executor.snapshot().target_flows, 0);
    }

    #[test]
    fn one_step_session_expiry_removes_one_target_owner_per_resume_and_emits_once() {
        let leg = committed_leg(2, 0x22, 0x42);
        let model = SessionModel::new(SessionRole::Owner, session_config(), leg);
        let supervisor = SessionSupervisor::new(model);
        let mut executor = OwnerTargetExecutor::new(
            OwnerTargetConfig::new(1, 1_024, 64).unwrap(),
            supervisor,
            MemoryTarget::new(MemoryTargetConfig::new(4, 256, 512, 128, 128, 16).unwrap()),
            source_config(),
        )
        .unwrap();
        for value in 1..=3 {
            let flow_id = SessionFlowId::new(value).unwrap();
            let opened = peer_frame(
                &mut executor,
                leg,
                Record::Open {
                    flow_id,
                    target: target(400 + value as u16),
                },
            )
            .unwrap();
            let _ = drain_resumes(&mut executor, opened);
        }
        let terminal_outputs = peer_frame(
            &mut executor,
            leg,
            Record::Reset {
                flow_id: SessionFlowId::new(1).unwrap(),
                reason: ResetReason::TargetFailure,
            },
        )
        .unwrap();
        let _ = drain_resumes(&mut executor, terminal_outputs);
        assert_eq!(executor.snapshot().target_flows, 2);
        assert_eq!(executor.snapshot().target_tombstones, 1);

        executor.apply_event(SessionEvent::LegLost { leg }).unwrap();
        let mut outputs = executor
            .apply_event(SessionEvent::ResumeGraceExpired { leg })
            .unwrap();
        let mut previous = 3usize;
        let mut session_expired = 0usize;
        let mut turns = 0usize;
        loop {
            session_expired += outputs
                .iter()
                .filter(|output| matches!(output, OwnerTargetOutput::SessionExpired))
                .count();
            let Some(index) = outputs
                .iter()
                .position(|output| matches!(output, OwnerTargetOutput::NeedsResume { .. }))
            else {
                break;
            };
            let OwnerTargetOutput::NeedsResume { resume } = outputs.remove(index) else {
                unreachable!()
            };
            outputs = executor.resume_pending_effects(resume).unwrap();
            turns += 1;
            assert!(turns <= 8, "expiry marker failed to make bounded progress");
            let current = executor.snapshot().target_flows + executor.snapshot().target_tombstones;
            if previous != 0 {
                assert!(
                    current < previous,
                    "expiry resume did not release ownership"
                );
            }
            previous = current;
        }
        assert_eq!(session_expired, 1);
        let snapshot = executor.snapshot();
        assert_eq!(snapshot.target_flows, 0);
        assert_eq!(snapshot.target_tombstones, 0);
        assert_eq!(snapshot.target_read_owned_bytes, 0);
        assert_eq!(snapshot.target_read_owned_segments, 0);
        assert_eq!(executor.target().snapshot().buffered_bytes, 0);
        assert_eq!(executor.target().snapshot().queued_write_directives, 0);
    }

    #[test]
    fn zero_and_would_block_never_ack_while_positive_partial_write_advances_exactly() {
        let (leg, mut executor) = executor();
        let flow_id = SessionFlowId::new(1).unwrap();
        open_flow(&mut executor, leg, flow_id, target(443));
        executor
            .target_mut()
            .push_write_directive(flow_id, MemoryWriteDirective::WouldBlock)
            .unwrap();
        executor
            .target_mut()
            .push_write_directive(flow_id, MemoryWriteDirective::Zero)
            .unwrap();
        executor
            .target_mut()
            .push_write_directive(flow_id, MemoryWriteDirective::accept_at_most(3).unwrap())
            .unwrap();
        executor
            .target_mut()
            .push_write_directive(flow_id, MemoryWriteDirective::WouldBlock)
            .unwrap();

        let initial = peer_frame(
            &mut executor,
            leg,
            Record::Data {
                flow_id,
                direction: Direction::ClientToTarget,
                offset: ByteOffset::new(0),
                payload: Bytes::from_static(b"abcdef"),
            },
        )
        .unwrap();
        assert!(transmitted(&initial).is_empty());
        assert_eq!(executor.snapshot().pending_writes, 1);

        let zero = executor.retry_target_write(flow_id).unwrap();
        assert!(transmitted(&zero).is_empty());
        assert_eq!(executor.snapshot().pending_writes, 1);

        let partial = executor.retry_target_write(flow_id).unwrap();
        assert!(transmitted(&partial).iter().any(|record| matches!(
            record,
            Record::Ack { next_accepted, final_accepted: false, .. }
                if *next_accepted == ByteOffset::new(3)
        )));
        assert_eq!(executor.snapshot().pending_writes, 1);
        assert_eq!(
            &executor.target_mut().take_written(flow_id, 8).unwrap()[..],
            b"abc"
        );

        let final_part = executor.retry_target_write(flow_id).unwrap();
        assert!(transmitted(&final_part).iter().any(|record| matches!(
            record,
            Record::Ack { next_accepted, final_accepted: false, .. }
                if *next_accepted == ByteOffset::new(6)
        )));
        assert_eq!(executor.snapshot().pending_writes, 0);
        assert_eq!(
            &executor.target_mut().take_written(flow_id, 8).unwrap()[..],
            b"def"
        );
    }

    #[test]
    fn close_half_closes_target_only_after_every_byte_is_accepted() {
        let (leg, mut executor) = executor();
        let flow_id = SessionFlowId::new(1).unwrap();
        open_flow(&mut executor, leg, flow_id, target(443));
        peer_frame(
            &mut executor,
            leg,
            Record::Data {
                flow_id,
                direction: Direction::ClientToTarget,
                offset: ByteOffset::new(0),
                payload: Bytes::from_static(b"abcdef"),
            },
        )
        .unwrap();

        let close = peer_frame(
            &mut executor,
            leg,
            Record::Close {
                flow_id,
                direction: Direction::ClientToTarget,
                final_offset: ByteOffset::new(6),
            },
        )
        .unwrap();
        let records = transmitted(&close);
        assert!(records.iter().any(|record| matches!(
            record,
            Record::Ack { next_accepted, final_accepted: true, .. }
                if *next_accepted == ByteOffset::new(6)
        )));
        assert_eq!(executor.target().snapshot().write_accepted_bytes, 6);
        assert_eq!(executor.target().snapshot().write_half_closes, 1);
        assert!(matches!(
            executor.target_mut().write(flow_id, b"late"),
            Err(TargetIoError::WriteAfterHalfClose { .. })
        ));
    }

    #[test]
    fn peer_reset_isolated_to_one_flow_and_joins_all_of_its_target_ownership() {
        let (leg, mut executor) = executor();
        let first = SessionFlowId::new(1).unwrap();
        let second = SessionFlowId::new(2).unwrap();
        open_flow(&mut executor, leg, first, target(443));
        open_flow(&mut executor, leg, second, target(8443));
        for (flow_id, bytes) in [(first, b"first".as_slice()), (second, b"other".as_slice())] {
            peer_frame(
                &mut executor,
                leg,
                Record::Data {
                    flow_id,
                    direction: Direction::ClientToTarget,
                    offset: ByteOffset::new(0),
                    payload: Bytes::copy_from_slice(bytes),
                },
            )
            .unwrap();
        }

        peer_frame(
            &mut executor,
            leg,
            Record::Reset {
                flow_id: first,
                reason: ResetReason::LocalAbandon,
            },
        )
        .unwrap();
        assert_eq!(executor.target().snapshot().reset_flows, 1);
        assert_eq!(executor.target().snapshot().joined_flows, 1);
        assert_eq!(executor.snapshot().target_flows, 1);
        assert_eq!(
            &executor.target_mut().take_written(second, 16).unwrap()[..],
            b"other"
        );

        let continued = peer_frame(
            &mut executor,
            leg,
            Record::Data {
                flow_id: second,
                direction: Direction::ClientToTarget,
                offset: ByteOffset::new(5),
                payload: Bytes::from_static(b"-live"),
            },
        )
        .unwrap();
        assert!(transmitted(&continued).iter().any(|record| matches!(
            record,
            Record::Ack { flow_id, next_accepted, .. }
                if *flow_id == second && *next_accepted == ByteOffset::new(10)
        )));
    }

    #[test]
    fn target_read_reserves_before_consumption_and_eof_follows_owned_data() {
        let (leg, mut executor) = executor();
        let flow_id = SessionFlowId::new(1).unwrap();
        open_flow(&mut executor, leg, flow_id, target(443));
        assert!(executor.try_read_target(flow_id).unwrap().is_empty());
        assert_eq!(executor.snapshot().target_read_owned_bytes, 0);
        assert_eq!(executor.snapshot().target_read_owned_segments, 0);
        assert_eq!(
            executor
                .target_mut()
                .feed_read(flow_id, Bytes::from_static(b"reply")),
            Ok(TargetReadFeedCompletion::Queued { bytes: 5 })
        );
        let before = executor.target().snapshot();

        let data = executor.try_read_target(flow_id).unwrap();
        assert!(transmitted(&data).iter().any(|record| matches!(
            record,
            Record::Data {
                flow_id: observed,
                direction: Direction::TargetToClient,
                offset,
                payload,
            } if *observed == flow_id
                && *offset == ByteOffset::new(0)
                && payload.as_ref() == b"reply"
        )));
        let after = executor.target().snapshot();
        assert_eq!(after.read_calls, before.read_calls + 1);
        assert_eq!(after.buffered_bytes, 0);
        assert_eq!(executor.snapshot().target_read_owned_bytes, 5);
        assert_eq!(executor.snapshot().target_read_owned_segments, 1);
        assert_eq!(executor.snapshot().session.uplink_replay_owned_bytes, 5);

        peer_frame(
            &mut executor,
            leg,
            Record::Ack {
                flow_id,
                direction: Direction::TargetToClient,
                next_accepted: ByteOffset::new(5),
                final_accepted: false,
            },
        )
        .unwrap();
        assert_eq!(executor.snapshot().target_read_owned_bytes, 0);
        assert_eq!(executor.snapshot().target_read_owned_segments, 0);
        assert_eq!(executor.snapshot().session.uplink_replay_owned_bytes, 0);

        executor.target_mut().finish_read(flow_id).unwrap();
        let eof = executor.try_read_target(flow_id).unwrap();
        assert!(transmitted(&eof).iter().any(|record| matches!(
            record,
            Record::Close {
                flow_id: observed,
                direction: Direction::TargetToClient,
                final_offset,
            } if *observed == flow_id && *final_offset == ByteOffset::new(5)
        )));
        assert_eq!(executor.snapshot().target_read_owned_bytes, 0);
        assert_eq!(executor.snapshot().target_read_owned_segments, 0);
    }

    #[test]
    fn exhausted_session_read_budget_never_consumes_target_bytes() {
        let leg = committed_leg(2, 0x22, 0x42);
        let config = SessionConfig::new(
            4,
            8,
            TcpWindowLimits::new(128, 16).unwrap(),
            TcpWindowLimits::new(128, 16).unwrap(),
            ReplayBudgetLimits::new(128, 16).unwrap(),
            ReplayBudgetLimits::new(64, 16).unwrap(),
            ReceiveBudgetLimits::new(256, 32).unwrap(),
            128,
        )
        .unwrap();
        let model = SessionModel::new(SessionRole::Owner, config, leg);
        let supervisor = SessionSupervisor::new(model);
        let target_io =
            MemoryTarget::new(MemoryTargetConfig::new(4, 256, 512, 128, 128, 16).unwrap());
        let source_config = FlowPortConfig::new(64, 2, 2, 2).unwrap();
        let mut executor = OwnerTargetExecutor::new(
            OwnerTargetConfig::new(128, 1_024, 64).unwrap(),
            supervisor,
            target_io,
            source_config,
        )
        .unwrap();
        let first = SessionFlowId::new(1).unwrap();
        let second = SessionFlowId::new(2).unwrap();
        open_flow(&mut executor, leg, first, target(443));
        open_flow(&mut executor, leg, second, target(8443));
        executor
            .target_mut()
            .feed_read(first, Bytes::from(vec![0x41; 64]))
            .unwrap();
        executor.try_read_target(first).unwrap();
        assert_eq!(executor.snapshot().target_read_owned_bytes, 64);
        assert_eq!(executor.snapshot().target_read_owned_segments, 1);

        executor
            .target_mut()
            .feed_read(second, Bytes::from_static(b"z"))
            .unwrap();
        let before = executor.target().snapshot();
        assert!(matches!(
            executor.try_read_target(second),
            Err(OwnerTargetError::FlowPort(
                FlowPortError::UplinkGlobalByteBudgetExhausted {
                    requested: 64,
                    available: 0,
                }
            ))
        ));
        let after = executor.target().snapshot();
        assert_eq!(after.read_calls, before.read_calls);
        assert_eq!(after.buffered_bytes, before.buffered_bytes);
        assert_eq!(after.buffered_bytes, 1);
    }

    #[test]
    fn replay_segment_credit_backpressures_before_third_tiny_target_read_after_lane_reuse() {
        let leg = committed_leg(2, 0x22, 0x42);
        let config = SessionConfig::new(
            4,
            8,
            TcpWindowLimits::new(128, 2).unwrap(),
            TcpWindowLimits::new(128, 16).unwrap(),
            ReplayBudgetLimits::new(128, 16).unwrap(),
            ReplayBudgetLimits::new(128, 2).unwrap(),
            ReceiveBudgetLimits::new(256, 32).unwrap(),
            128,
        )
        .unwrap();
        let model = SessionModel::new(SessionRole::Owner, config, leg);
        let supervisor = SessionSupervisor::new(model);
        let target_io =
            MemoryTarget::new(MemoryTargetConfig::new(4, 256, 512, 128, 1, 16).unwrap());
        // The physical one-message lane is dequeued and reused, while replay
        // segment ownership remains held until exact cumulative ACK.
        let source_config = FlowPortConfig::new(128, 1, 2, 2).unwrap();
        let mut executor = OwnerTargetExecutor::new(
            OwnerTargetConfig::new(128, 1_024, 1).unwrap(),
            supervisor,
            target_io,
            source_config,
        )
        .unwrap();
        let flow_id = SessionFlowId::new(1).unwrap();
        open_flow(&mut executor, leg, flow_id, target(443));
        executor
            .target_mut()
            .feed_read(flow_id, Bytes::from_static(b"abc"))
            .unwrap();

        executor.try_read_target(flow_id).unwrap();
        executor.try_read_target(flow_id).unwrap();
        assert_eq!(executor.snapshot().target_read_owned_bytes, 2);
        assert_eq!(executor.snapshot().target_read_owned_segments, 2);
        let before = executor.target().snapshot();
        assert!(matches!(
            executor.try_read_target(flow_id),
            Err(OwnerTargetError::FlowPort(
                FlowPortError::UplinkSegmentBudgetExhausted { available: 0 }
            ))
        ));
        let after = executor.target().snapshot();
        assert_eq!(after.read_calls, before.read_calls);
        assert_eq!(after.buffered_bytes, before.buffered_bytes);
        assert_eq!(after.buffered_bytes, 1);

        peer_frame(
            &mut executor,
            leg,
            Record::Ack {
                flow_id,
                direction: Direction::TargetToClient,
                next_accepted: ByteOffset::new(2),
                final_accepted: false,
            },
        )
        .unwrap();
        assert_eq!(executor.snapshot().target_read_owned_segments, 0);
        executor.try_read_target(flow_id).unwrap();
        assert_eq!(executor.target().snapshot().buffered_bytes, 0);
        assert_eq!(executor.snapshot().target_read_owned_segments, 1);
        peer_frame(
            &mut executor,
            leg,
            Record::Ack {
                flow_id,
                direction: Direction::TargetToClient,
                next_accepted: ByteOffset::new(3),
                final_accepted: false,
            },
        )
        .unwrap();
        assert_eq!(executor.snapshot().target_read_owned_bytes, 0);
        assert_eq!(executor.snapshot().target_read_owned_segments, 0);
    }

    #[test]
    fn session_expiry_cancels_and_joins_every_target_flow_with_bounded_cleanup() {
        let (leg, mut executor) = executor();
        let first = SessionFlowId::new(1).unwrap();
        let second = SessionFlowId::new(2).unwrap();
        open_flow(&mut executor, leg, first, target(443));
        open_flow(&mut executor, leg, second, target(8443));
        executor
            .target_mut()
            .feed_read(first, Bytes::from_static(b"owned"))
            .unwrap();
        executor
            .target_mut()
            .feed_read(second, Bytes::from_static(b"bytes"))
            .unwrap();

        executor.apply_event(SessionEvent::LegLost { leg }).unwrap();
        let expired = executor
            .apply_event(SessionEvent::ResumeGraceExpired { leg })
            .unwrap();
        assert_eq!(expired, vec![OwnerTargetOutput::SessionExpired]);
        assert_eq!(executor.snapshot().target_flows, 0);
        assert_eq!(executor.snapshot().target_tombstones, 0);
        assert_eq!(executor.snapshot().target_read_owned_bytes, 0);
        assert_eq!(executor.snapshot().target_read_owned_segments, 0);
        assert_eq!(executor.target().snapshot().live_flows, 0);
        assert_eq!(executor.target().snapshot().joined_flows, 0);
        assert_eq!(executor.target().snapshot().buffered_bytes, 0);
    }

    #[test]
    fn session_expiry_force_discards_buffers_from_an_already_graceful_target() {
        let (leg, mut executor) = executor();
        let flow_id = SessionFlowId::new(1).unwrap();
        open_flow(&mut executor, leg, flow_id, target(443));
        peer_frame(
            &mut executor,
            leg,
            Record::Data {
                flow_id,
                direction: Direction::ClientToTarget,
                offset: ByteOffset::new(0),
                payload: Bytes::from_static(b"owned"),
            },
        )
        .unwrap();
        peer_frame(
            &mut executor,
            leg,
            Record::Close {
                flow_id,
                direction: Direction::ClientToTarget,
                final_offset: ByteOffset::new(5),
            },
        )
        .unwrap();

        executor.target_mut().finish_read(flow_id).unwrap();
        executor.try_read_target(flow_id).unwrap();
        assert_eq!(
            executor.target().readiness(flow_id).unwrap().terminal,
            Some(TargetTerminalKind::Graceful)
        );
        assert_eq!(executor.target().snapshot().buffered_bytes, 5);

        executor.apply_event(SessionEvent::LegLost { leg }).unwrap();
        assert_eq!(
            executor
                .apply_event(SessionEvent::ResumeGraceExpired { leg })
                .unwrap(),
            vec![OwnerTargetOutput::SessionExpired]
        );
        assert_eq!(executor.snapshot().target_flows, 0);
        assert_eq!(executor.snapshot().target_tombstones, 0);
        assert_eq!(executor.snapshot().target_read_owned_bytes, 0);
        assert_eq!(executor.snapshot().target_read_owned_segments, 0);
        assert_eq!(executor.target().snapshot().live_flows, 0);
        assert_eq!(executor.target().snapshot().joined_flows, 0);
        assert_eq!(executor.target().snapshot().buffered_bytes, 0);
        assert_eq!(executor.target().snapshot().queued_write_directives, 0);
    }

    #[test]
    fn session_expiry_retires_each_join_before_reusing_tombstone_capacity() {
        let leg = committed_leg(2, 0x22, 0x42);
        let model = SessionModel::new(SessionRole::Owner, session_config(), leg);
        let supervisor = SessionSupervisor::new(model);
        let target_io = MemoryTarget::new(
            MemoryTargetConfig::new(2, 256, 512, 128, 128, 16)
                .unwrap()
                .with_max_terminal_tombstones(1)
                .unwrap(),
        );
        let mut executor = OwnerTargetExecutor::new(
            OwnerTargetConfig::new(128, 1_024, 64).unwrap(),
            supervisor,
            target_io,
            source_config(),
        )
        .unwrap();
        open_flow(
            &mut executor,
            leg,
            SessionFlowId::new(1).unwrap(),
            target(443),
        );
        open_flow(
            &mut executor,
            leg,
            SessionFlowId::new(2).unwrap(),
            target(8443),
        );

        executor.apply_event(SessionEvent::LegLost { leg }).unwrap();
        assert_eq!(
            executor
                .apply_event(SessionEvent::ResumeGraceExpired { leg })
                .unwrap(),
            vec![OwnerTargetOutput::SessionExpired]
        );
        assert_eq!(executor.snapshot().target_flows, 0);
        assert_eq!(executor.snapshot().target_tombstones, 0);
        assert_eq!(executor.snapshot().pending_joins, 0);
        assert_eq!(executor.target().snapshot().live_flows, 0);
        assert_eq!(executor.target().snapshot().joined_flows, 0);
        assert_eq!(executor.target().snapshot().buffered_bytes, 0);
    }

    #[test]
    fn terminal_grace_retires_target_tombstones_and_reuses_one_live_slot() {
        let leg = committed_leg(2, 0x22, 0x42);
        let model = SessionModel::new(SessionRole::Owner, session_config(), leg);
        let supervisor = SessionSupervisor::new(model);
        let target_io = MemoryTarget::new(
            MemoryTargetConfig::new(1, 256, 512, 128, 128, 16)
                .unwrap()
                .with_max_terminal_tombstones(1)
                .unwrap(),
        );
        let mut executor = OwnerTargetExecutor::new(
            OwnerTargetConfig::new(128, 1_024, 64).unwrap(),
            supervisor,
            target_io,
            source_config(),
        )
        .unwrap();

        for value in 1..=3 {
            let flow_id = SessionFlowId::new(value).unwrap();
            let opened = open_flow(&mut executor, leg, flow_id, target(400 + value as u16));
            assert!(transmitted(&opened).iter().any(|record| matches!(
                record,
                Record::OpenResult {
                    flow_id: observed,
                    result: OpenResultCode::Opened,
                } if *observed == flow_id
            )));
            let finished = peer_frame(
                &mut executor,
                leg,
                Record::Reset {
                    flow_id,
                    reason: ResetReason::LocalAbandon,
                },
            )
            .unwrap();
            let terminal = terminal_grace(&finished);
            assert_eq!(executor.target().snapshot().live_flows, 0);
            assert_eq!(executor.target().snapshot().joined_flows, 1);

            executor
                .apply_event(SessionEvent::TerminalGraceExpired { terminal })
                .unwrap();
            assert_eq!(executor.target().snapshot().live_flows, 0);
            assert_eq!(executor.target().snapshot().joined_flows, 0);
        }
        assert_eq!(executor.snapshot().target_flows, 0);
    }

    #[test]
    fn failed_open_terminal_retirement_is_a_safe_typed_noop() {
        let leg = committed_leg(2, 0x22, 0x42);
        let model = SessionModel::new(SessionRole::Owner, session_config(), leg);
        let supervisor = SessionSupervisor::new(model);
        let target_io = MemoryTarget::new(
            MemoryTargetConfig::new(1, 256, 512, 128, 128, 16)
                .unwrap()
                .with_max_terminal_tombstones(1)
                .unwrap(),
        );
        let mut executor = OwnerTargetExecutor::new(
            OwnerTargetConfig::new(128, 1_024, 64).unwrap(),
            supervisor,
            target_io,
            source_config(),
        )
        .unwrap();
        open_flow(
            &mut executor,
            leg,
            SessionFlowId::new(1).unwrap(),
            target(443),
        );

        let rejected = open_flow(
            &mut executor,
            leg,
            SessionFlowId::new(2).unwrap(),
            target(8443),
        );
        let terminal = terminal_grace(&rejected);
        assert!(transmitted(&rejected).iter().any(|record| matches!(
            record,
            Record::OpenResult {
                flow_id,
                result: OpenResultCode::ResourceExhausted,
            } if *flow_id == SessionFlowId::new(2).unwrap()
        )));

        assert!(
            executor
                .apply_event(SessionEvent::TerminalGraceExpired { terminal })
                .unwrap()
                .is_empty()
        );
        assert_eq!(executor.target().snapshot().live_flows, 1);
        assert_eq!(executor.target().snapshot().joined_flows, 0);
    }

    #[test]
    fn grace_expiry_before_a_capacity_blocked_join_retires_after_retry() {
        let leg = committed_leg(2, 0x22, 0x42);
        let model = SessionModel::new(SessionRole::Owner, session_config(), leg);
        let supervisor = SessionSupervisor::new(model);
        let target_io = MemoryTarget::new(
            MemoryTargetConfig::new(2, 256, 512, 128, 128, 16)
                .unwrap()
                .with_max_terminal_tombstones(1)
                .unwrap(),
        );
        let mut executor = OwnerTargetExecutor::new(
            OwnerTargetConfig::new(128, 1_024, 64).unwrap(),
            supervisor,
            target_io,
            source_config(),
        )
        .unwrap();
        let first = SessionFlowId::new(1).unwrap();
        let second = SessionFlowId::new(2).unwrap();
        open_flow(&mut executor, leg, first, target(443));
        let first_terminal = terminal_grace(
            &peer_frame(
                &mut executor,
                leg,
                Record::Reset {
                    flow_id: first,
                    reason: ResetReason::LocalAbandon,
                },
            )
            .unwrap(),
        );
        open_flow(&mut executor, leg, second, target(8443));
        let second_terminal = terminal_grace(
            &peer_frame(
                &mut executor,
                leg,
                Record::Reset {
                    flow_id: second,
                    reason: ResetReason::LocalAbandon,
                },
            )
            .unwrap(),
        );
        assert_eq!(executor.snapshot().pending_joins, 1);
        assert_eq!(executor.target().snapshot().live_flows, 1);
        assert_eq!(executor.target().snapshot().joined_flows, 1);

        // The second reducer tombstone can expire while its Target terminal
        // handle is still waiting for tombstone capacity. The exact expiry
        // must remain owned until that later join succeeds.
        executor
            .apply_event(SessionEvent::TerminalGraceExpired {
                terminal: second_terminal,
            })
            .unwrap();
        executor
            .apply_event(SessionEvent::TerminalGraceExpired {
                terminal: first_terminal,
            })
            .unwrap();
        assert_eq!(executor.snapshot().target_flows, 0);
        assert_eq!(executor.snapshot().pending_joins, 0);
        assert_eq!(executor.target().snapshot().live_flows, 0);
        assert_eq!(executor.target().snapshot().joined_flows, 0);
    }

    trait AmbiguousIfClone<Marker> {
        fn marker() {}
    }
    impl<T: ?Sized> AmbiguousIfClone<()> for T {}
    impl<T: Clone> AmbiguousIfClone<u8> for T {}
    const _: fn() = || {
        let _ = <PendingOwnerTargetAttachPublication as AmbiguousIfClone<_>>::marker;
        let _ = <EnqueuedOwnerTargetAttach as AmbiguousIfClone<_>>::marker;
        let _ = <EnqueuedOwnerTargetRecovery as AmbiguousIfClone<_>>::marker;
        let _ = <OwnerTargetRecoveryDrain as AmbiguousIfClone<_>>::marker;
        let _ = <OwnerTargetRecoveryResume as AmbiguousIfClone<_>>::marker;
        let _ = <OwnerTargetRecoveryOutput as AmbiguousIfClone<_>>::marker;
    };
}
