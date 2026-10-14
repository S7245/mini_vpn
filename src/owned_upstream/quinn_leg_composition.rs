//! Closed composition root for one Quinn-backed client session.
//!
//! This module owns the exact bridge between physical Quinn actor
//! lifetimes and the transport-independent client controller.  Sibling
//! modules must never be able to pair an arbitrary actor terminal with an
//! unrelated retired controller slot.

mod owner;
mod owner_initial;

#[cfg(test)]
mod tests;

pub(in crate::owned_upstream) use owner::QuinnOwnerContinuationPermit;
pub(crate) use owner::{
    QuinnOwnerOutputRootInventory, QuinnOwnerOutputServiceDrive, QuinnOwnerOutputStopDrive,
    QuinnOwnerStandbyAdoptFailure, QuinnOwnerStandbyInboundDrive, QuinnOwnerStandbyProbeArmDrive,
    QuinnOwnerStandbyProbeArmError, QuinnOwnerStandbyProbeDrive, QuinnOwnerStandbyProbeDriveError,
    QuinnOwnerStandbyProbeExpiryDrive, QuinnOwnerStandbyProbeExpiryError,
    QuinnOwnerStandbySessionComposition, QuinnOwnerStandbyShutdownTerminalDrive,
    QuinnOwnerStandbyShutdownTerminalError,
};
#[cfg(test)]
pub(crate) use owner::{
    QuinnOwnerRecoveryAdoptErrorKind, QuinnOwnerRecoveryAdoptFailure,
    QuinnOwnerRecoveryInboundDrive, QuinnOwnerRecoveryServiceDrive,
    QuinnOwnerRecoverySessionComposition, QuinnOwnerRecoveryTerminalDrive,
    QuinnOwnerRecoveryTerminalError,
};
pub(crate) use owner_initial::{
    QuinnOwnerInitialAcceptanceDrive, QuinnOwnerInitialActorJoined,
    QuinnOwnerInitialAdoptErrorKind, QuinnOwnerInitialSessionComposition,
    QuinnOwnerInitialShutdownError,
};

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

/// Unforgeable sibling authority for the closed controller/actor bridge.
/// Only this module may mint it; transport modules may require a borrow before
/// exposing any physical-incarnation comparison or ownership handoff.
pub(super) struct QuinnLegCompositionGate(());

impl QuinnLegCompositionGate {
    const fn mint() -> Self {
        Self(())
    }

    #[cfg(test)]
    pub(super) const fn mint_for_test() -> Self {
        Self::mint()
    }
}

/// One-shot shutdown-only authority minted only after the exact current actor
/// has already committed a hard failure while its recovery tail is active.
/// It adds no queue or byte capacity; the composition retains one O(1) token.
pub(super) struct QuinnFatalRecoveryTailShutdownGate(());

impl QuinnFatalRecoveryTailShutdownGate {
    const fn mint() -> Self {
        Self(())
    }
}

use crate::owned_upstream::client_controller::{
    ClientCommittedLocalOpen, ClientControllerBootstrapErrorKind, ClientControllerDriverApply,
    ClientControllerObservationSettlement, ClientControllerObservationSettlementErrorKind,
    ClientControllerObservedDriverApply, ClientControllerOwnershipLedger,
    ClientControllerRequiredObservationSettlementFailure, ClientCurrentFrameEnqueueFailure,
    ClientCurrentInboundAdmission, ClientCurrentProbeAckReconcileErrorKind,
    ClientCurrentProbeAckSettlement, ClientCurrentProbeArmError, ClientCurrentProbeArmFailure,
    ClientCurrentProbeBeginErrorKind, ClientCurrentProbeDeadline, ClientCurrentProbeDeadlineStatus,
    ClientCurrentProbeQueued, ClientCurrentProbeSchedulerAuthority,
    ClientCurrentShutdownClearErrorKind, ClientCurrentShutdownCleared,
    ClientCurrentTerminalBindErrorKind, ClientCurrentTerminalBound,
    ClientCurrentTerminalDiscardErrorKind, ClientCurrentTerminalDiscarded,
    ClientInboundDisposition, ClientInboundObservation, ClientPreparedLocalOpenBlock,
    ClientPreparedLocalOpenFailure, ClientRecoveryBeginErrorKind, ClientRecoveryDrive,
    ClientRecoveryStarted, ClientRecoveryTailOwnership, ClientRecoveryTailSettlement,
    ClientRecoveryTailSettlementBlock, ClientRegisteredRecoveryFailureClass,
    ClientSessionController, RetiredClientClearErrorKind,
};
use crate::owned_upstream::leg::PendingInitialAttach;
use crate::owned_upstream::leg::{
    BoundInbound, LegTransportTerminalReason, RetiredClientInboundKind,
};
#[cfg(test)]
use crate::owned_upstream::leg_io::ClientRecoveryTailInventory;
use crate::owned_upstream::leg_io::{
    AcceptedInitialAttach, AwaitingInitialAttach, InitialAttachResponseErrorKind, LegIoCounters,
};
use crate::owned_upstream::leg_io::{
    LegControlQueueErrorKind, LegIoError, LegOutboundQueueErrorKind, OrderedResetDeadlineCommit,
    OrderedResetDeliveryReceipt,
};
use crate::owned_upstream::quinn_client_root::RootStandbyBRecoveryKey;
use crate::owned_upstream::quinn_leg_actor::{
    ActorTerminalStampWitness, QuinnLegActorArrivalDeadline,
    QuinnLegActorCompositionInboundDecision, QuinnLegActorCompositionInboundPoll,
    QuinnLegActorEncodedInbound, QuinnLegActorError, QuinnLegActorHeldInbound,
    QuinnLegActorInboundDeadlinePoll, QuinnLegActorInboundDecision, QuinnLegActorInboundPoll,
    QuinnLegActorInboundTimerPoll, QuinnLegActorJoined, QuinnLegActorOwnerProgressCursor,
    QuinnLegActorReportedTerminal, QuinnLegActorReportedTerminalPoll, QuinnLegActorSemanticTimer,
    QuinnLegActorShutdownWaitDrive, QuinnLegActorSnapshot, QuinnLegActorTimerDecision,
    StartedQuinnLegActor,
};
#[cfg(test)]
use crate::owned_upstream::quinn_leg_actor::{
    QuinnLegActorDataWriterBlock, QuinnLegActorDiagnosticObservation, QuinnLegActorEnqueueError,
    QuinnLegActorInboundPublicationBlock, QuinnLegActorJoinBlock,
    QuinnLegActorPostTerminalCleanupBlock, QuinnLegActorProbeDeadlineCommit,
};
use crate::owned_upstream::reverse_switch::{
    ClientSwitchArbiter, CommittedSwitch, CorrelatedAttachAccepted, PendingPhysicalAttachPermit,
    PhysicalPendingAttachSettlement, PhysicalPendingAttachStage, StandbyActorSeal,
    SwitchArbiterError, SwitchDiscardAudit, SwitchShutdownDisposition,
};
use crate::owned_upstream::session_clock::SessionMonotonicClock;
use crate::owned_upstream::standby::{
    AcceptedClientRegisteredAttach, AcceptedClientRegisteredAttachShutdownDiscard,
    ArmedClientRegisteredStandbyProbe, AwaitingClientRegisteredStandbyProbeAck,
    AwaitingRegisteredAttach, AwaitingStandbyAccepted, ClientRegisteredStandby,
    ExactLegProbeResponder, ExactLegProbeResponse, FreshClientRegisteredStandby,
    FreshRegisteredAttachBeginErrorKind, FreshStandbySwitchHintEvidenceErrorKind,
    PendingClientRegisteredStandbyProbe, PendingExactLegProbeAck, PendingRegisteredAttach,
    PendingStandbyRegistration, RegisteredAttachBeginErrorKind, RegisteredAttachGenerationStatus,
    RegisteredAttachResponse, RegisteredAttachResponseErrorKind,
    RegisteredStandbyProbeAckErrorKind, RegisteredStandbyProbeArmErrorKind,
    RegisteredStandbyProbeBeginErrorKind, StandbyAcceptanceErrorKind, StandbyEnqueueFailure,
    StandbyRegistrationError,
};
use crate::owned_upstream::supervisor::{
    ReducerAdmissionBlock, SessionSupervisorError, SessionSupervisorSnapshot,
    TcpPortFactoryMintError,
};
use crate::owned_upstream::tcp::{
    ClientControllerDriverInput, FlowPortConfig, FlowPortError, ResumableTcpDriver,
    ResumableTcpFlow, ResumableTcpPortFactory,
};
#[cfg(test)]
use crate::owned_upstream::two_leg::EncodedDelivery;
use crate::owned_upstream::two_leg::{LegId, SimTime, WireDirection, WireRoute};
use crate::resumable::LegControlFrame;
use crate::resumable::{
    AttachConfigError, AttachCredentials, AttachNonce, AttachRequest, ByteOffset, Direction,
    FeatureSet, Frame, HintSequence, LegControlRecord, LegGeneration, ProbeSequence, ProtocolError,
    SessionConfig, SessionEffect, SessionFlowId, SessionId, StandbyNonce, SwitchHintCause,
    TerminalGrace,
};
use crate::shared::TargetAddr;

/// Deep owner for one controller and the physical actor supplying its exact
/// current transport. Raw transport pieces and incarnation authority never
/// escape after a successful composition transaction.
pub(crate) struct QuinnClientSessionComposition {
    session_clock: SessionMonotonicClock,
    controller: ClientSessionController,
    current: StartedQuinnLegActor,
    current_joined: Option<QuinnLegActorJoined>,
    current_terminal: QuinnCurrentTerminalState,
    current_terminal_stamp: Option<ActorTerminalStampWitness>,
    retired: Option<RetiredQuinnLegOwner>,
    standby: Option<QuinnRegisteredStandbyOwner>,
    physical_attach: Option<QuinnPhysicalAttachOwner>,
    accepted_physical_shutdown_discard: Option<QuinnAcceptedPhysicalShutdownDiscard>,
    current_loss_source: QuinnCurrentLossSource,
    fatal_recovery_tail_shutdown: Option<QuinnFatalRecoveryTailShutdown>,
    shutdown_probe_scheduler: Option<QuinnCurrentProbeSchedulerAuthority>,
    phase: QuinnClientCompositionPhase,
}

struct QuinnFatalRecoveryTailShutdown {
    cause: QuinnFatalRecoveryTailCause,
    gate: QuinnFatalRecoveryTailShutdownGate,
}

enum QuinnFatalRecoveryTailCause {
    RootFailed(Arc<QuinnLegActorError>),
    TaskJoin(Arc<str>),
}

impl QuinnFatalRecoveryTailShutdown {
    fn from_actor_error(error: QuinnLegActorError) -> Option<Self> {
        let cause = match error {
            QuinnLegActorError::RootFailed(cause) => QuinnFatalRecoveryTailCause::RootFailed(cause),
            QuinnLegActorError::TaskJoin(cause) => {
                QuinnFatalRecoveryTailCause::TaskJoin(Arc::from(cause))
            }
            _ => return None,
        };
        Some(Self {
            cause,
            gate: QuinnFatalRecoveryTailShutdownGate::mint(),
        })
    }

    fn join_error(&self) -> QuinnCurrentJoinError {
        let actor = match &self.cause {
            QuinnFatalRecoveryTailCause::RootFailed(cause) => {
                QuinnLegActorError::RootFailed(Arc::clone(cause))
            }
            QuinnFatalRecoveryTailCause::TaskJoin(cause) => {
                QuinnLegActorError::TaskJoin(cause.to_string())
            }
        };
        QuinnCurrentJoinError::Actor(actor)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum QuinnCurrentLossSource {
    None,
    TransportTerminal,
    ProbeDeadline {
        generation: crate::resumable::LegGeneration,
        probe_sequence: ProbeSequence,
    },
}

struct QuinnRegisteredStandbyOwner {
    actor: Option<QuinnStandbyActorOwner>,
    state: Option<QuinnRegisteredStandbyState>,
    admitted_generation: crate::resumable::LegGeneration,
    registration_nonce: StandbyNonce,
    next_probe_sequence: Option<ProbeSequence>,
    current_probe_sequence: Option<ProbeSequence>,
    liveness: QuinnStandbyLivenessConfig,
    features: FeatureSet,
    failure: Option<QuinnStandbyFailureKind>,
    probe_responder: Option<ExactLegProbeResponder>,
    pending_control: Option<QuinnStandbyPendingControl>,
    hint_actor_seal: StandbyActorSeal,
    shutdown_control_discard: Option<QuinnStandbyShutdownControlDiscard>,
    shutdown_terminal: Option<QuinnStandbyShutdownTerminalSettlement>,
    shutdown_retired_state: Option<QuinnStandbyShutdownRetiredState>,
}

enum QuinnPhysicalAttachAuthority {
    Pending(PendingPhysicalAttachPermit),
    Accepted(QuinnLegActorHeldInbound<QuinnStandbyHeldControl>),
    Shutdown(PhysicalPendingAttachSettlement),
}

struct QuinnPhysicalAttachOwner {
    stage: PhysicalPendingAttachStage,
    authority: QuinnPhysicalAttachAuthority,
    stored_pending_frame_bytes: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QuinnPhysicalAttachAuthorityKind {
    Pending,
    Accepted,
    Shutdown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct QuinnPhysicalAttachInventory {
    stage: Option<PhysicalPendingAttachStage>,
    authority_kind: Option<QuinnPhysicalAttachAuthorityKind>,
    live_authorities: usize,
    pending_frames: usize,
    summed_pending_frame_bytes: usize,
    stored_pending_frame_bytes: usize,
    accepted_pending_commit: Option<QuinnAcceptedPendingCommitObservation>,
}

/// Copy-only capacity and retry fingerprint for one actor-held physical
/// `ATTACH_ACCEPTED`. The exact actor record, typed acceptance, policy identity,
/// queue permits, and DELIVERY capability remain private to the composition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct QuinnAcceptedPendingCommitObservation {
    accepted_generation: LegGeneration,
    marker_generation: LegGeneration,
    held_records: usize,
    held_encoded_bytes: usize,
    arrived_at: SimTime,
    publication_epoch: u64,
    send_ordinal: u64,
    queued_inbound_markers: usize,
    composition_message_permits: usize,
    reserved_delivery_acks: usize,
    reserved_delivery_ack_bytes: usize,
}

/// Historical, copy-only witness installed only after shutdown consumes one
/// actor-held physical acceptance without publishing DELIVERY. The typed
/// acceptance and exact policy settlement are counted separately so root
/// aggregation cannot hide either ownership class.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct QuinnAcceptedPhysicalShutdownDiscard {
    generation: LegGeneration,
    held_records: usize,
    held_encoded_bytes: usize,
    queued_inbound_markers: usize,
    composition_message_permits: usize,
    reserved_delivery_acks_discarded: usize,
    reserved_delivery_ack_bytes_discarded: usize,
    delivery_acks_emitted: usize,
    accepted_authorities: usize,
    request_receipts: usize,
    policy_settlements: usize,
    arrived_at: SimTime,
    publication_epoch: u64,
    send_ordinal: u64,
}

impl QuinnAcceptedPhysicalShutdownDiscard {
    pub(crate) const fn generation(self) -> LegGeneration {
        self.generation
    }

    pub(crate) const fn held_records(self) -> usize {
        self.held_records
    }

    pub(crate) const fn held_encoded_bytes(self) -> usize {
        self.held_encoded_bytes
    }

    pub(crate) const fn queued_inbound_markers(self) -> usize {
        self.queued_inbound_markers
    }

    pub(crate) const fn composition_message_permits(self) -> usize {
        self.composition_message_permits
    }

    pub(crate) const fn reserved_delivery_acks_discarded(self) -> usize {
        self.reserved_delivery_acks_discarded
    }

    pub(crate) const fn reserved_delivery_ack_bytes_discarded(self) -> usize {
        self.reserved_delivery_ack_bytes_discarded
    }

    pub(crate) const fn delivery_acks_emitted(self) -> usize {
        self.delivery_acks_emitted
    }

    pub(crate) const fn accepted_authorities(self) -> usize {
        self.accepted_authorities
    }

    pub(crate) const fn request_receipts(self) -> usize {
        self.request_receipts
    }

    pub(crate) const fn policy_settlements(self) -> usize {
        self.policy_settlements
    }

    pub(crate) const fn arrived_at(self) -> SimTime {
        self.arrived_at
    }

    pub(crate) const fn publication_epoch(self) -> u64 {
        self.publication_epoch
    }

    pub(crate) const fn send_ordinal(self) -> u64 {
        self.send_ordinal
    }
}

impl QuinnAcceptedPendingCommitObservation {
    pub(crate) const fn accepted_generation(self) -> LegGeneration {
        self.accepted_generation
    }

    pub(crate) const fn marker_generation(self) -> LegGeneration {
        self.marker_generation
    }

    pub(crate) const fn held_records(self) -> usize {
        self.held_records
    }

    pub(crate) const fn held_encoded_bytes(self) -> usize {
        self.held_encoded_bytes
    }

    pub(crate) const fn arrived_at(self) -> SimTime {
        self.arrived_at
    }

    pub(crate) const fn publication_epoch(self) -> u64 {
        self.publication_epoch
    }

    pub(crate) const fn send_ordinal(self) -> u64 {
        self.send_ordinal
    }

    pub(crate) const fn queued_inbound_markers(self) -> usize {
        self.queued_inbound_markers
    }

    pub(crate) const fn composition_message_permits(self) -> usize {
        self.composition_message_permits
    }

    pub(crate) const fn reserved_delivery_acks(self) -> usize {
        self.reserved_delivery_acks
    }

    pub(crate) const fn reserved_delivery_ack_bytes(self) -> usize {
        self.reserved_delivery_ack_bytes
    }
}

impl QuinnPhysicalAttachInventory {
    pub(crate) const fn is_empty(self) -> bool {
        self.live_authorities == 0
            && self.pending_frames == 0
            && self.summed_pending_frame_bytes == 0
            && self.stored_pending_frame_bytes == 0
            && self.accepted_pending_commit.is_none()
    }

    pub(super) const fn stage(self) -> Option<PhysicalPendingAttachStage> {
        self.stage
    }

    pub(crate) const fn authority_kind(self) -> Option<QuinnPhysicalAttachAuthorityKind> {
        self.authority_kind
    }

    pub(crate) const fn live_authorities(self) -> usize {
        self.live_authorities
    }

    pub(crate) const fn pending_frames(self) -> usize {
        self.pending_frames
    }

    pub(crate) const fn pending_frame_bytes(self) -> usize {
        self.summed_pending_frame_bytes
    }

    pub(crate) const fn summed_pending_frame_bytes(self) -> usize {
        self.summed_pending_frame_bytes
    }

    pub(crate) const fn stored_pending_frame_bytes(self) -> usize {
        self.stored_pending_frame_bytes
    }

    pub(crate) const fn accepted_pending_commit(
        self,
    ) -> Option<QuinnAcceptedPendingCommitObservation> {
        self.accepted_pending_commit
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum QuinnPhysicalAttachAuditError {
    PendingFrameMissing,
    PendingFrameUnexpected,
    AcceptedMarkerWithoutAuthority,
    AcceptedOwnershipMismatch,
    AcceptedPolicyMismatch,
    PendingFrameEncoding(ProtocolError),
    PendingFrameBytesMismatch { summed: usize, stored: usize },
    UnexpectedStoredPendingFrameBytes { stored: usize },
}

fn audit_physical_pending_frame_bytes(
    encoded_len: Result<usize, ProtocolError>,
    stored: usize,
) -> Result<usize, QuinnPhysicalAttachAuditError> {
    let summed = encoded_len.map_err(QuinnPhysicalAttachAuditError::PendingFrameEncoding)?;
    if summed != stored {
        return Err(QuinnPhysicalAttachAuditError::PendingFrameBytesMismatch { summed, stored });
    }
    Ok(summed)
}

fn advance_pending_physical_attach_stage(
    physical_attach: &mut Option<QuinnPhysicalAttachOwner>,
    next: PhysicalPendingAttachStage,
) -> Result<(), ()> {
    let Some(physical) = physical_attach.take() else {
        // The legacy scalar attach path intentionally remains reachable until
        // the permit-driven install transaction replaces it end to end.
        return Ok(());
    };
    let QuinnPhysicalAttachOwner {
        stage,
        authority,
        stored_pending_frame_bytes,
    } = physical;
    if stage != PhysicalPendingAttachStage::AwaitingAttach {
        *physical_attach = Some(QuinnPhysicalAttachOwner {
            stage,
            authority,
            stored_pending_frame_bytes,
        });
        return Err(());
    }
    let authority = match (next, authority) {
        (
            PhysicalPendingAttachStage::GenerationStatus,
            authority @ QuinnPhysicalAttachAuthority::Pending(_),
        ) => authority,
        (_, authority) => {
            *physical_attach = Some(QuinnPhysicalAttachOwner {
                stage,
                authority,
                stored_pending_frame_bytes,
            });
            return Err(());
        }
    };
    *physical_attach = Some(QuinnPhysicalAttachOwner {
        stage: next,
        authority,
        stored_pending_frame_bytes: 0,
    });
    Ok(())
}

#[cfg(test)]
pub(super) fn audit_physical_pending_frame_bytes_for_test(
    encoded_len: Result<usize, ProtocolError>,
    stored: usize,
) -> Result<usize, QuinnPhysicalAttachAuditError> {
    audit_physical_pending_frame_bytes(encoded_len, stored)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QuinnPhysicalAttachAdmissionErrorKind {
    SessionShuttingDown,
    SlotOccupied,
    MissingStandby,
    WrongStandbyNonce,
    WrongGeneration,
    WrongState,
    Arbiter(SwitchArbiterError),
}

pub(crate) struct QuinnPhysicalAttachAdmissionFailure {
    kind: QuinnPhysicalAttachAdmissionErrorKind,
    permit: PendingPhysicalAttachPermit,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum QuinnPhysicalAttachShutdownError {
    MissingPhysicalAuthority,
    ArbiterNotPending(SwitchShutdownDisposition),
    AcceptedInboundPending,
    Audit(QuinnPhysicalAttachAuditError),
    Arbiter(SwitchArbiterError),
}

impl QuinnPhysicalAttachAdmissionFailure {
    pub(crate) const fn kind(&self) -> QuinnPhysicalAttachAdmissionErrorKind {
        self.kind
    }

    pub(super) fn into_permit(self) -> PendingPhysicalAttachPermit {
        self.permit
    }
}

impl fmt::Debug for QuinnPhysicalAttachAdmissionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QuinnPhysicalAttachAdmissionFailure")
            .field("kind", &self.kind)
            .field("permit", &"[REDACTED]")
            .finish()
    }
}

enum QuinnStandbyHeldControl {
    ProbeAck(PendingExactLegProbeAck),
    SwitchHint(BoundInbound),
    AttachAccepted {
        accepted: AcceptedClientRegisteredAttach,
        policy: CorrelatedAttachAccepted,
    },
}

enum QuinnStandbyPendingControl {
    ProbeAck(QuinnLegActorHeldInbound<QuinnStandbyHeldControl>),
    SwitchHint(QuinnLegActorHeldInbound<QuinnStandbyHeldControl>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QuinnStandbyPendingControlKind {
    ProbeAck,
    SwitchHint,
}

/// Copy-only audit of one actor-held B control record explicitly discarded
/// after the composition published its lifecycle cutoff.  The exact actor
/// record is committed before this value is installed, so the audit never
/// becomes a second owner of the decoded frame or its tracked bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct QuinnStandbyShutdownControlDiscard {
    kind: QuinnStandbyPendingControlKind,
    messages: usize,
    bytes: usize,
    arrived_at: SimTime,
    publication_epoch: u64,
    send_ordinal: u64,
}

impl QuinnStandbyShutdownControlDiscard {
    pub(crate) const fn kind(self) -> QuinnStandbyPendingControlKind {
        self.kind
    }

    pub(crate) const fn messages(self) -> usize {
        self.messages
    }

    pub(crate) const fn bytes(self) -> usize {
        self.bytes
    }

    pub(crate) const fn arrived_at(self) -> SimTime {
        self.arrived_at
    }

    pub(crate) const fn publication_epoch(self) -> u64 {
        self.publication_epoch
    }

    pub(crate) const fn send_ordinal(self) -> u64 {
        self.send_ordinal
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QuinnStandbyShutdownStateKind {
    RegistrationPending,
    AwaitingRegistration,
    Registered,
    ProbePending,
    AwaitingProbe,
    ArmedProbe,
    Fresh,
    AttachPending,
    AwaitingAttach,
    Accepted,
    AcceptedPendingCommit,
    GenerationStatus,
}

/// Exact, countable protocol authorities retired beside a physically joined
/// B.  Pending wire frames are intentionally not estimated: only typestates
/// whose complete zero-frame inventory is known may mint this witness.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct QuinnStandbyShutdownStateInventory {
    kind: QuinnStandbyShutdownStateKind,
    authorities: usize,
    pending_frames: usize,
    pending_frame_bytes: usize,
    receipts: usize,
    timers: usize,
    freshness: usize,
}

impl QuinnStandbyShutdownStateInventory {
    pub(crate) const fn kind(self) -> QuinnStandbyShutdownStateKind {
        self.kind
    }

    pub(crate) const fn authorities(self) -> usize {
        self.authorities
    }

    pub(crate) const fn pending_frames(self) -> usize {
        self.pending_frames
    }

    pub(crate) const fn pending_frame_bytes(self) -> usize {
        self.pending_frame_bytes
    }

    pub(crate) const fn receipts(self) -> usize {
        self.receipts
    }

    pub(crate) const fn timers(self) -> usize {
        self.timers
    }

    pub(crate) const fn freshness(self) -> usize {
        self.freshness
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct QuinnStandbyShutdownTerminalSettlement {
    reason: LegTransportTerminalReason,
}

/// Exact cold FIFO inventory observed at the shutdown boundary.  Pending
/// delivery is ownership just like a queued frame; cancellation may retire it
/// only after the actor publishes the terminal for this exact transport.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct QuinnStandbyShutdownOutboundObservation {
    queued_frames: usize,
    queued_bytes: usize,
    ordered_delivery_wait: bool,
}

fn observe_standby_shutdown_outbound(
    drive: QuinnLegActorShutdownWaitDrive,
) -> QuinnStandbyShutdownOutboundObservation {
    match drive {
        QuinnLegActorShutdownWaitDrive::Clean
        | QuinnLegActorShutdownWaitDrive::RetiredDelivered => {
            QuinnStandbyShutdownOutboundObservation::default()
        }
        QuinnLegActorShutdownWaitDrive::Pending => QuinnStandbyShutdownOutboundObservation {
            queued_frames: 0,
            queued_bytes: 0,
            ordered_delivery_wait: true,
        },
        QuinnLegActorShutdownWaitDrive::Residual {
            queued_frames,
            queued_bytes,
            ordered_delivery_wait,
        } => QuinnStandbyShutdownOutboundObservation {
            queued_frames,
            queued_bytes,
            ordered_delivery_wait,
        },
    }
}

fn standby_terminal_is_failure(reason: LegTransportTerminalReason) -> bool {
    reason != LegTransportTerminalReason::LocalCancelled
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct QuinnStandbyShutdownTerminalRejection {
    reason: Option<LegTransportTerminalReason>,
    terminal_failure: bool,
}

/// Copy-only record of the exact sole-FIFO ownership discarded by B's
/// authenticated physical terminal.  Summed frame bytes and the queue's
/// stored byte counter remain separate so a corrupted accounting path cannot
/// manufacture a clean-zero witness.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct QuinnStandbyShutdownOutboundDiscard {
    queued_frames: usize,
    summed_frame_bytes: usize,
    stored_owned_bytes: usize,
    ordered_delivery_wait: bool,
    was_active: bool,
}

impl QuinnStandbyShutdownOutboundDiscard {
    pub(crate) const fn queued_frames(self) -> usize {
        self.queued_frames
    }

    pub(crate) const fn summed_frame_bytes(self) -> usize {
        self.summed_frame_bytes
    }

    pub(crate) const fn stored_owned_bytes(self) -> usize {
        self.stored_owned_bytes
    }

    pub(crate) const fn ordered_delivery_wait(self) -> bool {
        self.ordered_delivery_wait
    }

    pub(crate) const fn was_active(self) -> bool {
        self.was_active
    }
}

/// Copy-only audit installed only after the exact former B typestate has been
/// synchronously retired under the shutdown cutoff. Normally the physical
/// actor is already joined.  If its first join reports FIFO residual, its root
/// has still completed and published the exact terminal; that terminal first
/// authorizes the sole-FIFO discard, then this audit is installed before the
/// retained actor is spawned into its one bounded structural retry.
struct QuinnStandbyShutdownRetiredState {
    inventory: QuinnStandbyShutdownStateInventory,
    outbound_discard: Option<QuinnStandbyShutdownOutboundDiscard>,
    terminal_rejection: Option<QuinnStandbyShutdownTerminalRejection>,
    residual_terminal_stamp: Option<ActorTerminalStampWitness>,
}

fn pending_standby_control_inventory(
    pending: &QuinnStandbyPendingControl,
) -> (QuinnStandbyPendingControlKind, usize, usize) {
    match pending {
        QuinnStandbyPendingControl::ProbeAck(held) => (
            QuinnStandbyPendingControlKind::ProbeAck,
            1,
            held.encoded_bytes(),
        ),
        QuinnStandbyPendingControl::SwitchHint(held) => (
            QuinnStandbyPendingControlKind::SwitchHint,
            1,
            held.encoded_bytes(),
        ),
    }
}

/// Immutable identity of one exact held physical B arrival. These values are
/// observations only: the non-cloneable actor record stays inside the
/// composition until a synchronous transaction settles or explicitly retains
/// it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct QuinnStandbySwitchHintMetadata {
    arrived_at: SimTime,
    publication_epoch: u64,
    send_ordinal: u64,
    encoded_bytes: usize,
}

impl QuinnStandbySwitchHintMetadata {
    pub(crate) const fn arrived_at(self) -> SimTime {
        self.arrived_at
    }

    pub(crate) const fn publication_epoch(self) -> u64 {
        self.publication_epoch
    }

    pub(crate) const fn send_ordinal(self) -> u64 {
        self.send_ordinal
    }

    pub(crate) const fn encoded_bytes(self) -> usize {
        self.encoded_bytes
    }
}

/// Non-constructible, non-cloneable physical authentication proof for one
/// exact actor-held B `SWITCH_HINT`. The frame remains borrowed from the held
/// actor record, and the process-local actor identity remains borrowed from
/// the same composition slot. The private gate prevents the proof from
/// escaping this synchronous transaction.
pub(crate) struct PhysicalAuthenticatedBHint<'gate> {
    frame: &'gate LegControlFrame,
    session_id: SessionId,
    generation: LegGeneration,
    standby_nonce: StandbyNonce,
    hint_sequence: HintSequence,
    flow_id: SessionFlowId,
    direction: Direction,
    oldest_unacknowledged: ByteOffset,
    cause: SwitchHintCause,
    arrived_at: SimTime,
    actor_seal: &'gate StandbyActorSeal,
    _composition_gate: &'gate QuinnLegCompositionGate,
}

pub(crate) type QuinnStandbyPhysicalSwitchHint<'gate> = PhysicalAuthenticatedBHint<'gate>;

impl PhysicalAuthenticatedBHint<'_> {
    pub(crate) const fn frame(&self) -> &LegControlFrame {
        self.frame
    }

    pub(crate) const fn session_id(&self) -> SessionId {
        self.session_id
    }

    pub(crate) const fn generation(&self) -> LegGeneration {
        self.generation
    }

    pub(crate) const fn standby_nonce(&self) -> StandbyNonce {
        self.standby_nonce
    }

    pub(crate) const fn hint_sequence(&self) -> HintSequence {
        self.hint_sequence
    }

    pub(crate) const fn flow_id(&self) -> SessionFlowId {
        self.flow_id
    }

    pub(crate) const fn direction(&self) -> Direction {
        self.direction
    }

    pub(crate) const fn oldest_unacknowledged(&self) -> ByteOffset {
        self.oldest_unacknowledged
    }

    pub(crate) const fn cause(&self) -> SwitchHintCause {
        self.cause
    }

    pub(crate) const fn arrived_at(&self) -> SimTime {
        self.arrived_at
    }

    /// Process-local identity of the exact B actor whose held inbound record
    /// passed the physical freshness/correlation gate. It is borrowed from
    /// the composition-owned B slot and cannot outlive this transaction.
    pub(crate) const fn actor_seal(&self) -> &StandbyActorSeal {
        self.actor_seal
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QuinnStandbyPhysicalSwitchHintReject {
    ProbeNotFresh,
    WrongActor,
    FeatureNotNegotiated,
    WrongGeneration,
    WrongRecordKind,
    WrongSession,
    WrongStandbyNonce,
    Stale,
    EndpointOrQueueLost,
}

impl From<FreshStandbySwitchHintEvidenceErrorKind> for QuinnStandbyPhysicalSwitchHintReject {
    fn from(kind: FreshStandbySwitchHintEvidenceErrorKind) -> Self {
        match kind {
            FreshStandbySwitchHintEvidenceErrorKind::WrongActor => Self::WrongActor,
            FreshStandbySwitchHintEvidenceErrorKind::FeatureNotNegotiated => {
                Self::FeatureNotNegotiated
            }
            FreshStandbySwitchHintEvidenceErrorKind::WrongGeneration => Self::WrongGeneration,
            FreshStandbySwitchHintEvidenceErrorKind::WrongRecordKind => Self::WrongRecordKind,
            FreshStandbySwitchHintEvidenceErrorKind::WrongSession => Self::WrongSession,
            FreshStandbySwitchHintEvidenceErrorKind::WrongStandbyNonce => Self::WrongStandbyNonce,
            FreshStandbySwitchHintEvidenceErrorKind::Stale => Self::Stale,
            FreshStandbySwitchHintEvidenceErrorKind::EndpointOrQueueLost => {
                Self::EndpointOrQueueLost
            }
        }
    }
}

/// Root outcome for one synchronous physical-hint transaction. `Settled`
/// covers both accepted hints and typed semantic rejections: either is an
/// irrevocable fact and commits the exact actor record. `Retain` is reserved
/// for snapshot-inert pressure/arbiter blocking and restores that same record.
pub(crate) enum QuinnStandbySwitchHintDecision<S, B> {
    Settled(S),
    Retain(B),
}

pub(crate) enum QuinnStandbySwitchHintTransaction<S, B> {
    SessionShuttingDown,
    MissingStandby,
    StandbyFailed,
    NoPendingHint,
    ProbeAckPending,
    PhysicalRejected {
        kind: QuinnStandbyPhysicalSwitchHintReject,
        metadata: QuinnStandbySwitchHintMetadata,
    },
    Settled {
        outcome: S,
        metadata: QuinnStandbySwitchHintMetadata,
    },
    Retained {
        blocker: B,
        metadata: QuinnStandbySwitchHintMetadata,
    },
    CompositionInvariant,
}

fn held_switch_hint_metadata(
    held: &QuinnLegActorHeldInbound<QuinnStandbyHeldControl>,
) -> QuinnStandbySwitchHintMetadata {
    QuinnStandbySwitchHintMetadata {
        arrived_at: held.arrived_at(),
        publication_epoch: held.publication_epoch(),
        send_ordinal: held.send_ordinal(),
        encoded_bytes: held.encoded_bytes(),
    }
}

enum QuinnStandbyProbeAckCommitError {
    Queue(LegControlQueueErrorKind),
    Actor(QuinnLegActorError),
    WrongControl,
}

fn commit_held_standby_probe_ack(
    actor: &mut StartedQuinnLegActor,
    gate: &QuinnLegCompositionGate,
    held: QuinnLegActorHeldInbound<QuinnStandbyHeldControl>,
) -> Result<
    ProbeSequence,
    (
        QuinnLegActorHeldInbound<QuinnStandbyHeldControl>,
        QuinnStandbyProbeAckCommitError,
    ),
> {
    match actor.with_controller_parts_mut_input(gate, held, |held, _io, queue| {
        held.try_commit(|control| match control {
            QuinnStandbyHeldControl::ProbeAck(pending) => {
                let probe_sequence = pending.probe_sequence();
                match pending.enqueue(queue) {
                    Ok(enqueued) => {
                        debug_assert_eq!(enqueued.probe_sequence(), probe_sequence);
                        Ok(probe_sequence)
                    }
                    Err(failure) => {
                        let kind = failure.kind().clone();
                        Err((
                            QuinnStandbyHeldControl::ProbeAck(failure.into_pending()),
                            QuinnStandbyProbeAckCommitError::Queue(kind),
                        ))
                    }
                }
            }
            QuinnStandbyHeldControl::SwitchHint(inbound) => Err((
                QuinnStandbyHeldControl::SwitchHint(inbound),
                QuinnStandbyProbeAckCommitError::WrongControl,
            )),
            QuinnStandbyHeldControl::AttachAccepted { accepted, policy } => Err((
                QuinnStandbyHeldControl::AttachAccepted { accepted, policy },
                QuinnStandbyProbeAckCommitError::WrongControl,
            )),
        })
    }) {
        Ok(result) => result,
        Err((held, error)) => Err((held, QuinnStandbyProbeAckCommitError::Actor(error))),
    }
}

enum QuinnStandbyActorOwner {
    Running(Box<StartedQuinnLegActor>),
    Joining(tokio::task::JoinHandle<QuinnStandbyLegJoinOutcome>),
    Joined(QuinnLegActorJoined),
    TaskLost(Arc<str>),
}

struct QuinnStandbyLegJoinOutcome {
    actor: StartedQuinnLegActor,
    joined: Result<QuinnLegActorJoined, QuinnLegActorError>,
    runtime: tokio::runtime::Handle,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum QuinnStandbyFailureKind {
    ActorTransport,
    QueueOrEndpointLost,
    ProtocolInvariant,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QuinnStandbyQueueFailureKind {
    EndpointLost,
    Invariant,
}

fn classify_leg_control_enqueue(
    kind: &LegControlQueueErrorKind,
) -> Result<(), QuinnStandbyQueueFailureKind> {
    match kind {
        LegControlQueueErrorKind::CapacityExceeded { .. } => Ok(()),
        LegControlQueueErrorKind::EndpointLost => Err(QuinnStandbyQueueFailureKind::EndpointLost),
        LegControlQueueErrorKind::InvalidFrame(_)
        | LegControlQueueErrorKind::ByteCountOverflow
        | LegControlQueueErrorKind::EnqueueOrdinalExhausted
        | LegControlQueueErrorKind::WrongLeg
        | LegControlQueueErrorKind::AttachAcceptanceReserved
        | LegControlQueueErrorKind::AttachRequestPending
        | LegControlQueueErrorKind::AttachStatusPending
        | LegControlQueueErrorKind::AttachRecoveryPending
        | LegControlQueueErrorKind::ClientReplacementRecoveryPending
        | LegControlQueueErrorKind::ClientRecoveryTerminal
        | LegControlQueueErrorKind::OwnerRecoveryTerminal => {
            Err(QuinnStandbyQueueFailureKind::Invariant)
        }
    }
}

fn classify_registered_attach_enqueue(
    kind: &LegOutboundQueueErrorKind,
) -> Result<(), QuinnStandbyQueueFailureKind> {
    match kind {
        LegOutboundQueueErrorKind::CapacityExceeded { .. }
        | LegOutboundQueueErrorKind::AttachRequestNotFirst { .. } => Ok(()),
        LegOutboundQueueErrorKind::EndpointLost => Err(QuinnStandbyQueueFailureKind::EndpointLost),
        LegOutboundQueueErrorKind::InvalidFrame(_)
        | LegOutboundQueueErrorKind::ByteCountOverflow
        | LegOutboundQueueErrorKind::EnqueueOrdinalExhausted
        | LegOutboundQueueErrorKind::ResetDeliveryReceiptMismatch
        | LegOutboundQueueErrorKind::NotAttachAcceptance
        | LegOutboundQueueErrorKind::RegisteredAttachAdmissionRequired
        | LegOutboundQueueErrorKind::AttachAcceptanceAdmissionRequired
        | LegOutboundQueueErrorKind::AuthenticatedAttachStatusAdmissionRequired
        | LegOutboundQueueErrorKind::AttachAcceptanceReserved
        | LegOutboundQueueErrorKind::AttachRequestPending
        | LegOutboundQueueErrorKind::AttachStatusPending
        | LegOutboundQueueErrorKind::ClientReplacementRecoveryPending
        | LegOutboundQueueErrorKind::ClientRecoveryTerminal
        | LegOutboundQueueErrorKind::OwnerRecoveryTerminal
        | LegOutboundQueueErrorKind::NotAttachRequest
        | LegOutboundQueueErrorKind::NotAttachStatus
        | LegOutboundQueueErrorKind::WrongLeg
        | LegOutboundQueueErrorKind::WrongAttachAcceptanceReservation
        | LegOutboundQueueErrorKind::MismatchedAttachRequest
        | LegOutboundQueueErrorKind::MismatchedAttachAcceptance
        | LegOutboundQueueErrorKind::AttachStatusNotFirst { .. }
        | LegOutboundQueueErrorKind::AttachAcceptanceNotFirst { .. } => {
            Err(QuinnStandbyQueueFailureKind::Invariant)
        }
    }
}

fn standby_failure_for_queue(kind: QuinnStandbyQueueFailureKind) -> QuinnStandbyFailureKind {
    match kind {
        QuinnStandbyQueueFailureKind::EndpointLost => QuinnStandbyFailureKind::QueueOrEndpointLost,
        QuinnStandbyQueueFailureKind::Invariant => QuinnStandbyFailureKind::ProtocolInvariant,
    }
}

fn spawn_standby_join(
    runtime: &tokio::runtime::Handle,
    mut actor: StartedQuinnLegActor,
) -> tokio::task::JoinHandle<QuinnStandbyLegJoinOutcome> {
    let retained_runtime = runtime.clone();
    runtime.spawn(async move {
        let joined = actor.join().await;
        QuinnStandbyLegJoinOutcome {
            actor,
            joined,
            runtime: retained_runtime,
        }
    })
}

enum QuinnRegisteredStandbyState {
    RegistrationPending(PendingStandbyRegistration),
    AwaitingRegistration(AwaitingStandbyAccepted),
    Registered(ClientRegisteredStandby),
    ProbePending(PendingClientRegisteredStandbyProbe),
    AwaitingProbe(AwaitingClientRegisteredStandbyProbeAck),
    ArmedProbe(QuinnArmedRegisteredStandbyProbe),
    Fresh(FreshClientRegisteredStandby),
    AttachPending(PendingRegisteredAttach),
    AwaitingAttach(AwaitingRegisteredAttach),
    Accepted(AcceptedClientRegisteredAttach),
    AcceptedPendingCommit { generation: LegGeneration },
    GenerationStatus(RegisteredAttachGenerationStatus),
}

/// Exact armed-B deadline plus the frozen actor publication high-water used
/// by receive-before-expiry arbitration. Both values move together across
/// retry so root scheduling delay cannot replace or extend the deadline.
struct QuinnArmedRegisteredStandbyProbe {
    armed: ArmedClientRegisteredStandbyProbe,
    recovery_key: RootStandbyBRecoveryKey,
    inbound_publication_cutoff: Option<u64>,
}

impl QuinnArmedRegisteredStandbyProbe {
    fn new(
        armed: ArmedClientRegisteredStandbyProbe,
        recovery_key: RootStandbyBRecoveryKey,
    ) -> Self {
        Self {
            armed,
            recovery_key,
            inbound_publication_cutoff: None,
        }
    }

    const fn deadline(&self) -> SimTime {
        self.armed.deadline()
    }

    const fn recovery_key(&self) -> &RootStandbyBRecoveryKey {
        &self.recovery_key
    }
}

impl QuinnLegActorArrivalDeadline for QuinnArmedRegisteredStandbyProbe {
    fn arrival_deadline(&self) -> SimTime {
        self.deadline()
    }
}

impl QuinnLegActorSemanticTimer for QuinnArmedRegisteredStandbyProbe {
    fn capture_inbound_publication_cutoff(&mut self, high_water: u64) -> u64 {
        *self.inbound_publication_cutoff.get_or_insert(high_water)
    }
}

fn standby_shutdown_state_kind(
    state: &QuinnRegisteredStandbyState,
) -> QuinnStandbyShutdownStateKind {
    match state {
        QuinnRegisteredStandbyState::RegistrationPending(_) => {
            QuinnStandbyShutdownStateKind::RegistrationPending
        }
        QuinnRegisteredStandbyState::AwaitingRegistration(_) => {
            QuinnStandbyShutdownStateKind::AwaitingRegistration
        }
        QuinnRegisteredStandbyState::Registered(_) => QuinnStandbyShutdownStateKind::Registered,
        QuinnRegisteredStandbyState::ProbePending(_) => QuinnStandbyShutdownStateKind::ProbePending,
        QuinnRegisteredStandbyState::AwaitingProbe(_) => {
            QuinnStandbyShutdownStateKind::AwaitingProbe
        }
        QuinnRegisteredStandbyState::ArmedProbe(_) => QuinnStandbyShutdownStateKind::ArmedProbe,
        QuinnRegisteredStandbyState::Fresh(_) => QuinnStandbyShutdownStateKind::Fresh,
        QuinnRegisteredStandbyState::AttachPending(_) => {
            QuinnStandbyShutdownStateKind::AttachPending
        }
        QuinnRegisteredStandbyState::AwaitingAttach(_) => {
            QuinnStandbyShutdownStateKind::AwaitingAttach
        }
        QuinnRegisteredStandbyState::Accepted(_) => QuinnStandbyShutdownStateKind::Accepted,
        QuinnRegisteredStandbyState::AcceptedPendingCommit { .. } => {
            QuinnStandbyShutdownStateKind::AcceptedPendingCommit
        }
        QuinnRegisteredStandbyState::GenerationStatus(_) => {
            QuinnStandbyShutdownStateKind::GenerationStatus
        }
    }
}

/// Preflights the complete exact B typestate before terminal consumption.
/// Pending frame bytes are encoded from the retained original frame; no
/// estimate or reconstructed protocol value is accepted as shutdown proof.
fn preflight_standby_shutdown_state(
    state: &QuinnRegisteredStandbyState,
) -> Result<QuinnStandbyShutdownStateInventory, QuinnStandbyShutdownStateKind> {
    let kind = standby_shutdown_state_kind(state);
    let (authorities, pending_frames, pending_frame_bytes, receipts, timers, freshness) =
        match state {
            QuinnRegisteredStandbyState::RegistrationPending(pending) => (
                1,
                1,
                pending.shutdown_pending_frame_bytes().map_err(|_| kind)?,
                0,
                0,
                0,
            ),
            QuinnRegisteredStandbyState::AwaitingRegistration(awaiting) => (
                1,
                1,
                awaiting.shutdown_retry_frame_bytes().map_err(|_| kind)?,
                1,
                0,
                0,
            ),
            QuinnRegisteredStandbyState::Registered(_) => (1, 0, 0, 1, 0, 0),
            QuinnRegisteredStandbyState::ProbePending(pending) => (
                1,
                1,
                pending.shutdown_pending_frame_bytes().map_err(|_| kind)?,
                1,
                0,
                0,
            ),
            QuinnRegisteredStandbyState::AwaitingProbe(_) => (1, 0, 0, 2, 0, 0),
            QuinnRegisteredStandbyState::ArmedProbe(_) => (1, 0, 0, 2, 1, 0),
            QuinnRegisteredStandbyState::Fresh(_) => (1, 0, 0, 2, 0, 1),
            QuinnRegisteredStandbyState::AttachPending(pending) => {
                (1, 1, pending.encoded_len().map_err(|_| kind)?, 1, 0, 0)
            }
            QuinnRegisteredStandbyState::AwaitingAttach(_) => (1, 0, 0, 1, 0, 0),
            QuinnRegisteredStandbyState::Accepted(_) => (1, 0, 0, 1, 0, 0),
            // The exact typed acceptance and policy live in the physical slot;
            // this generation-only marker is correlation state, not a second
            // authority. After no-ACK discard it remains only as shutdown history.
            QuinnRegisteredStandbyState::AcceptedPendingCommit { .. } => (0, 0, 0, 0, 0, 0),
            QuinnRegisteredStandbyState::GenerationStatus(_) => (1, 0, 0, 1, 0, 0),
        };
    Ok(QuinnStandbyShutdownStateInventory {
        kind,
        authorities,
        pending_frames,
        pending_frame_bytes,
        receipts,
        timers,
        freshness,
    })
}

fn discard_pending_standby_control_for_shutdown(
    owner: &mut QuinnRegisteredStandbyOwner,
    gate: &QuinnLegCompositionGate,
) -> Result<(), QuinnCurrentJoinError> {
    let Some(pending) = owner.pending_control.take() else {
        return Ok(());
    };
    let (kind, messages, bytes) = pending_standby_control_inventory(&pending);
    if owner.shutdown_control_discard.is_some() {
        owner.pending_control = Some(pending);
        return Err(QuinnCurrentJoinError::StandbyPendingControl {
            kind,
            messages,
            bytes,
        });
    }
    let (arrived_at, publication_epoch, send_ordinal) = match &pending {
        QuinnStandbyPendingControl::ProbeAck(held)
        | QuinnStandbyPendingControl::SwitchHint(held) => (
            held.arrived_at(),
            held.publication_epoch(),
            held.send_ordinal(),
        ),
    };
    let audit = QuinnStandbyShutdownControlDiscard {
        kind,
        messages,
        bytes,
        arrived_at,
        publication_epoch,
        send_ordinal,
    };
    let committed: Result<(), (QuinnStandbyPendingControl, ())> = match pending {
        QuinnStandbyPendingControl::ProbeAck(held) => {
            match held.try_shutdown_discard(gate, |control| match control {
                QuinnStandbyHeldControl::ProbeAck(pending_ack) => Ok(pending_ack),
                other => Err((other, ())),
            }) {
                Ok(discarded) => {
                    drop(discarded.into_value());
                    Ok(())
                }
                Err((held, ())) => Err((QuinnStandbyPendingControl::ProbeAck(held), ())),
            }
        }
        QuinnStandbyPendingControl::SwitchHint(held) => {
            match held.try_shutdown_discard(gate, |control| match control {
                QuinnStandbyHeldControl::SwitchHint(inbound) => Ok(inbound),
                other => Err((other, ())),
            }) {
                Ok(discarded) => {
                    drop(discarded.into_value());
                    Ok(())
                }
                Err((held, ())) => Err((QuinnStandbyPendingControl::SwitchHint(held), ())),
            }
        }
    };
    match committed {
        Ok(()) => {
            // The actor record and exact decoded value are gone before this
            // copy-only audit becomes visible.
            owner.shutdown_control_discard = Some(audit);
            Ok(())
        }
        Err((pending, ())) => {
            owner.pending_control = Some(pending);
            Err(QuinnCurrentJoinError::StandbyPendingControl {
                kind,
                messages,
                bytes,
            })
        }
    }
}

/// After the first structured join has awaited B's real actor root, a local
/// shutdown terminal is available even when that join rejected the still-live
/// FIFO inventory.  This synchronous transaction binds the exact terminal,
/// established leg, and sole FIFO before discarding anything.  It also moves
/// the retained B protocol typestate into its explicit shutdown inventory, so
/// cancellation between the following retry spawn and await cannot lose an
/// authority or byte.
fn settle_standby_fifo_after_join_residual(
    owner: &mut QuinnRegisteredStandbyOwner,
    gate: &QuinnLegCompositionGate,
) -> Result<(), QuinnCurrentJoinError> {
    if owner.shutdown_terminal.is_some() || owner.shutdown_retired_state.is_some() {
        return Err(QuinnCurrentJoinError::CompositionInvariant);
    }
    let state_inventory = preflight_standby_shutdown_state(
        owner
            .state
            .as_ref()
            .ok_or(QuinnCurrentJoinError::CompositionInvariant)?,
    )
    .map_err(|kind| QuinnCurrentJoinError::StandbyStateRetirementPending { kind })?;
    let actor = match owner.actor.as_mut() {
        Some(QuinnStandbyActorOwner::Running(actor)) => actor.as_mut(),
        _ => return Err(QuinnCurrentJoinError::CompositionInvariant),
    };
    let settled = actor
        .try_transact_reported_terminal_with_parts(gate, |_runtime, io, queue, reported| {
            let (terminal, terminal_stamp) = reported.into_parts();
            let reason = terminal.reason();
            // A first-failure clock error deliberately carries no timestamp,
            // but it is still fatal.  The actor root derives the authenticated
            // terminal reason from its first-failure barrier, so only the
            // explicit local-cancel reason is a clean shutdown.
            let terminal_failure = standby_terminal_is_failure(reason);
            let Some(cleanup) = queue.begin_owner_terminal_cleanup(io.established_leg(), &terminal)
            else {
                return Err((
                    QuinnLegActorReportedTerminal::restore(terminal, terminal_stamp),
                    QuinnCurrentJoinError::CompositionInvariant,
                ));
            };
            let discarded = cleanup.commit();
            let outbound_discard = QuinnStandbyShutdownOutboundDiscard {
                queued_frames: discarded.queued_frames(),
                summed_frame_bytes: discarded.summed_frame_bytes(),
                stored_owned_bytes: discarded.stored_owned_bytes(),
                ordered_delivery_wait: discarded.ordered_delivery_wait(),
                was_active: discarded.was_active(),
            };
            drop(terminal);
            Ok((reason, terminal_failure, outbound_discard, terminal_stamp))
        })
        .map_err(QuinnCurrentJoinError::StandbyActor)?;
    let (reason, terminal_failure, outbound_discard, terminal_stamp) = match settled {
        QuinnLegActorReportedTerminalPoll::Applied(settled) => settled,
        QuinnLegActorReportedTerminalPoll::Rejected(error) => return Err(error),
        QuinnLegActorReportedTerminalPoll::Pending
        | QuinnLegActorReportedTerminalPoll::AlreadyConsumed => {
            return Err(QuinnCurrentJoinError::CompositionInvariant);
        }
    };

    // Everything below is infallible after the preflight and exact terminal
    // commit above.  No await can expose a half-retired protocol state.
    let state = owner
        .state
        .take()
        .ok_or(QuinnCurrentJoinError::CompositionInvariant)?;
    drop(state);
    let terminal_rejection =
        if reason == LegTransportTerminalReason::LocalCancelled && !terminal_failure {
            owner.shutdown_terminal = Some(QuinnStandbyShutdownTerminalSettlement { reason });
            None
        } else {
            Some(QuinnStandbyShutdownTerminalRejection {
                reason: Some(reason),
                terminal_failure,
            })
        };
    owner.shutdown_retired_state = Some(QuinnStandbyShutdownRetiredState {
        inventory: state_inventory,
        outbound_discard: Some(outbound_discard),
        terminal_rejection,
        residual_terminal_stamp: Some(terminal_stamp),
    });
    Ok(())
}

fn settle_joined_standby_for_shutdown(
    owner: &mut QuinnRegisteredStandbyOwner,
    gate: &QuinnLegCompositionGate,
) -> Result<(), QuinnCurrentJoinError> {
    let Some(QuinnStandbyActorOwner::Joined(joined)) = owner.actor.as_mut() else {
        return if let Some(QuinnStandbyActorOwner::TaskLost(cause)) = owner.actor.as_ref() {
            Err(QuinnCurrentJoinError::StandbyJoinTask(Arc::clone(cause)))
        } else {
            Err(QuinnCurrentJoinError::CompositionInvariant)
        };
    };
    let owned_messages = joined.owned_messages();
    let owned_bytes = joined.owned_bytes();
    if owned_messages != 0 || owned_bytes != 0 {
        return Err(QuinnCurrentJoinError::StandbyResidualOwnership {
            messages: owned_messages,
            bytes: owned_bytes,
            terminals: joined.owned_terminals(),
            terminal_failure: joined.terminal_failure().is_some(),
        });
    }

    if let Some(state) = owner.shutdown_retired_state.as_mut()
        && let Some(handoff_stamp) = state.residual_terminal_stamp.as_ref()
    {
        let expected_terminal_failure = state
            .terminal_rejection
            .is_some_and(|rejection| rejection.terminal_failure);
        let same_source = joined
            .terminal_stamp()
            .is_some_and(|root_stamp| handoff_stamp.same_source(root_stamp));
        if !same_source || expected_terminal_failure != joined.terminal_failure().is_some() {
            return Err(QuinnCurrentJoinError::CompositionInvariant);
        }
        // Consume the linear handoff witness only after both correlations
        // succeed.  A mismatched root must remain sticky across repeated
        // shutdown observation instead of turning into an unchecked retry.
        drop(
            state
                .residual_terminal_stamp
                .take()
                .ok_or(QuinnCurrentJoinError::CompositionInvariant)?,
        );
    }

    if let Some(rejection) = owner
        .shutdown_retired_state
        .as_ref()
        .and_then(|state| state.terminal_rejection)
    {
        return Err(QuinnCurrentJoinError::StandbyTerminalRejected {
            reason: rejection.reason,
            terminal_failure: rejection.terminal_failure,
        });
    }

    if owner.shutdown_retired_state.is_some() {
        return if owner.state.is_none() && owner.shutdown_terminal.is_some() {
            Ok(())
        } else {
            Err(QuinnCurrentJoinError::CompositionInvariant)
        };
    }
    if owner.shutdown_terminal.is_some() {
        return Err(QuinnCurrentJoinError::CompositionInvariant);
    }

    // Preflight the exact retained B typestate before consuming any joined
    // terminal.  Fatal is a sticky copy-only fact, not terminal ownership: a
    // structurally joined actor must still consume its terminal and retire the
    // exact typestate so the final live ledger is zero.
    let inventory = preflight_standby_shutdown_state(
        owner
            .state
            .as_ref()
            .ok_or(QuinnCurrentJoinError::CompositionInvariant)?,
    )
    .map_err(|kind| QuinnCurrentJoinError::StandbyStateRetirementPending { kind })?;
    let terminal_failure = joined.terminal_failure().is_some();
    let reason =
        joined.transact_terminal::<_, QuinnCurrentJoinError>(gate, |_runtime, terminal| {
            let reason = terminal.as_ref().map(|terminal| terminal.reason());
            drop(terminal);
            Ok(reason)
        })?;
    let state = owner
        .state
        .take()
        .ok_or(QuinnCurrentJoinError::CompositionInvariant)?;
    drop(state);
    let terminal_rejection =
        if reason == Some(LegTransportTerminalReason::LocalCancelled) && !terminal_failure {
            owner.shutdown_terminal = Some(QuinnStandbyShutdownTerminalSettlement {
                reason: LegTransportTerminalReason::LocalCancelled,
            });
            None
        } else {
            Some(QuinnStandbyShutdownTerminalRejection {
                reason,
                terminal_failure,
            })
        };
    owner.shutdown_retired_state = Some(QuinnStandbyShutdownRetiredState {
        inventory,
        outbound_discard: None,
        terminal_rejection,
        residual_terminal_stamp: None,
    });
    if let Some(rejection) = terminal_rejection {
        Err(QuinnCurrentJoinError::StandbyTerminalRejected {
            reason: rejection.reason,
            terminal_failure: rejection.terminal_failure,
        })
    } else {
        Ok(())
    }
}

const MAX_STANDBY_LIVENESS_DURATION: Duration = Duration::from_secs(60);

/// Checked local policy for one adopted registered B. Callers can select a
/// bounded duration but cannot supply a freshness boolean or an absolute
/// deadline; both clocks are derived inside the composition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct QuinnStandbyLivenessConfig {
    probe_timeout: Duration,
    freshness_window: Duration,
}

impl QuinnStandbyLivenessConfig {
    pub(crate) fn new(
        probe_timeout: Duration,
        freshness_window: Duration,
    ) -> Result<Self, QuinnStandbyLivenessConfigError> {
        if probe_timeout.is_zero() || freshness_window.is_zero() {
            return Err(QuinnStandbyLivenessConfigError::ZeroDuration);
        }
        if probe_timeout > MAX_STANDBY_LIVENESS_DURATION
            || freshness_window > MAX_STANDBY_LIVENESS_DURATION
        {
            return Err(QuinnStandbyLivenessConfigError::DurationTooLarge);
        }
        Ok(Self {
            probe_timeout,
            freshness_window,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QuinnStandbyLivenessConfigError {
    ZeroDuration,
    DurationTooLarge,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum QuinnClientCompositionPhase {
    Running,
    ShuttingDown,
    Joined,
}

/// Snapshot cursor for all physical actor sources currently owned by one
/// composition. The root captures it *before* its final bounded ready-work
/// scan, then parks with that cursor only when the scan reports no progress.
/// Each running cursor carries opaque source identity as well as an epoch, so
/// an actor replacement cannot impersonate old progress with an equal number.
#[derive(Clone)]
pub(crate) struct QuinnCompositionProgressCursor {
    current: QuinnLegActorOwnerProgressCursor,
    standby: QuinnOwnedProgressCursor,
    retired: QuinnOwnedProgressCursor,
}

#[derive(Clone)]
enum QuinnOwnedProgressCursor {
    Running(QuinnLegActorOwnerProgressCursor),
    Joining,
    Dormant,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QuinnCompositionProgressSource {
    Current,
    Standby,
    Retired,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QuinnCompositionProgress {
    Current { epoch: u64 },
    Standby { epoch: u64 },
    Retired { epoch: u64 },
    StandbyJoin,
    RetiredJoin,
}

#[derive(Debug)]
pub(crate) struct QuinnCompositionProgressError {
    source: QuinnCompositionProgressSource,
    kind: QuinnCompositionProgressErrorKind,
}

#[derive(Debug)]
enum QuinnCompositionProgressErrorKind {
    TopologyChanged,
    Actor(QuinnLegActorError),
}

impl QuinnCompositionProgressError {
    pub(crate) const fn source(&self) -> QuinnCompositionProgressSource {
        self.source
    }

    pub(crate) const fn actor(&self) -> Option<&QuinnLegActorError> {
        match &self.kind {
            QuinnCompositionProgressErrorKind::Actor(actor) => Some(actor),
            QuinnCompositionProgressErrorKind::TopologyChanged => None,
        }
    }

    pub(crate) const fn topology_changed(&self) -> bool {
        matches!(
            self.kind,
            QuinnCompositionProgressErrorKind::TopologyChanged
        )
    }
}

enum QuinnOwnedProgressWait<'a, T> {
    Running {
        actor: &'a StartedQuinnLegActor,
        cursor: QuinnLegActorOwnerProgressCursor,
    },
    Joining(&'a mut tokio::task::JoinHandle<T>),
    Dormant,
}

enum QuinnOwnedProgressWaitOutcome<T> {
    Actor(Result<u64, QuinnLegActorError>),
    Join(Result<T, tokio::task::JoinError>),
}

async fn wait_owned_progress<T>(
    source: QuinnOwnedProgressWait<'_, T>,
) -> QuinnOwnedProgressWaitOutcome<T> {
    match source {
        QuinnOwnedProgressWait::Running { actor, cursor } => {
            QuinnOwnedProgressWaitOutcome::Actor(actor.wait_for_owner_progress(cursor).await)
        }
        QuinnOwnedProgressWait::Joining(task) => QuinnOwnedProgressWaitOutcome::Join(task.await),
        QuinnOwnedProgressWait::Dormant => std::future::pending().await,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum QuinnCurrentTerminalState {
    Unbound,
    ControllerBound {
        generation: crate::resumable::LegGeneration,
        reason: LegTransportTerminalReason,
    },
    Applied {
        generation: crate::resumable::LegGeneration,
        reason: LegTransportTerminalReason,
    },
    ConsumedFailed {
        generation: crate::resumable::LegGeneration,
        reason: LegTransportTerminalReason,
        failure: QuinnCurrentTerminalConsumedFailure,
    },
    ShutdownCleared {
        generation: crate::resumable::LegGeneration,
        reason: LegTransportTerminalReason,
    },
}

struct QuinnCurrentTerminalBoundAt {
    bound: ClientCurrentTerminalBound,
    terminal_stamp: ActorTerminalStampWitness,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum QuinnCurrentTerminalConsumedFailure {
    ControllerDiscard(ClientCurrentTerminalDiscardErrorKind),
    AppliedIdentityInvariant,
}

impl QuinnCurrentTerminalConsumedFailure {
    const fn into_drive_error(self) -> QuinnCurrentTerminalError {
        match self {
            Self::ControllerDiscard(kind) => QuinnCurrentTerminalError::ControllerDiscard(kind),
            Self::AppliedIdentityInvariant => QuinnCurrentTerminalError::CompositionInvariant,
        }
    }

    const fn into_join_error(self) -> QuinnCurrentJoinError {
        match self {
            Self::ControllerDiscard(kind) => {
                QuinnCurrentJoinError::CurrentTerminalDiscardFailed(kind)
            }
            Self::AppliedIdentityInvariant => {
                QuinnCurrentJoinError::CurrentTerminalAppliedIdentityMismatch
            }
        }
    }
}

enum RetiredQuinnLegOwner {
    Running(Box<StartedQuinnLegActor>),
    Joining(tokio::task::JoinHandle<RetiredQuinnLegJoinOutcome>),
    Joined(QuinnLegActorJoined),
    TaskLost(Arc<str>),
}

struct RetiredQuinnLegJoinOutcome {
    actor: StartedQuinnLegActor,
    joined: Result<QuinnLegActorJoined, QuinnLegActorError>,
}

fn spawn_retired_join(
    runtime: &tokio::runtime::Handle,
    mut actor: StartedQuinnLegActor,
) -> tokio::task::JoinHandle<RetiredQuinnLegJoinOutcome> {
    runtime.spawn(async move {
        let joined = actor.join().await;
        RetiredQuinnLegJoinOutcome { actor, joined }
    })
}

impl RetiredQuinnLegOwner {
    fn physical_incarnation<'a>(
        &'a self,
        gate: &QuinnLegCompositionGate,
    ) -> Option<&'a crate::owned_upstream::leg_io::PhysicalLegRuntimeIncarnation> {
        match self {
            Self::Running(actor) => actor.physical_incarnation(gate),
            Self::Joined(joined) => Some(joined.physical_incarnation(gate)),
            Self::Joining(_) | Self::TaskLost(_) => None,
        }
    }
}

struct QuinnCompositionNoopWake;

impl Wake for QuinnCompositionNoopWake {
    fn wake(self: Arc<Self>) {}
}

fn poll_finished_join<T>(
    task: &mut tokio::task::JoinHandle<T>,
) -> Poll<Result<T, tokio::task::JoinError>> {
    if !task.is_finished() {
        return Poll::Pending;
    }
    let waker = Waker::from(Arc::new(QuinnCompositionNoopWake));
    let mut context = Context::from_waker(&waker);
    Future::poll(Pin::new(task), &mut context)
}

fn standby_progress_wait(
    standby: &mut Option<QuinnRegisteredStandbyOwner>,
    cursor: QuinnOwnedProgressCursor,
) -> Result<QuinnOwnedProgressWait<'_, QuinnStandbyLegJoinOutcome>, ()> {
    let actor = standby.as_mut().and_then(|owner| owner.actor.as_mut());
    match (actor, cursor) {
        (
            Some(QuinnStandbyActorOwner::Running(actor)),
            QuinnOwnedProgressCursor::Running(cursor),
        ) => {
            if !actor.matches_owner_progress_cursor(&cursor) {
                return Err(());
            }
            Ok(QuinnOwnedProgressWait::Running { actor, cursor })
        }
        (Some(QuinnStandbyActorOwner::Joining(task)), QuinnOwnedProgressCursor::Joining) => {
            Ok(QuinnOwnedProgressWait::Joining(task))
        }
        (
            Some(QuinnStandbyActorOwner::Joined(_) | QuinnStandbyActorOwner::TaskLost(_)) | None,
            QuinnOwnedProgressCursor::Dormant,
        ) => Ok(QuinnOwnedProgressWait::Dormant),
        _ => Err(()),
    }
}

fn retired_progress_wait(
    retired: &mut Option<RetiredQuinnLegOwner>,
    cursor: QuinnOwnedProgressCursor,
) -> Result<QuinnOwnedProgressWait<'_, RetiredQuinnLegJoinOutcome>, ()> {
    match (retired.as_mut(), cursor) {
        (Some(RetiredQuinnLegOwner::Running(actor)), QuinnOwnedProgressCursor::Running(cursor)) => {
            if !actor.matches_owner_progress_cursor(&cursor) {
                return Err(());
            }
            Ok(QuinnOwnedProgressWait::Running { actor, cursor })
        }
        (Some(RetiredQuinnLegOwner::Joining(task)), QuinnOwnedProgressCursor::Joining) => {
            Ok(QuinnOwnedProgressWait::Joining(task))
        }
        (
            Some(RetiredQuinnLegOwner::Joined(_) | RetiredQuinnLegOwner::TaskLost(_)) | None,
            QuinnOwnedProgressCursor::Dormant,
        ) => Ok(QuinnOwnedProgressWait::Dormant),
        _ => Err(()),
    }
}

enum QuinnCompositionProgressWaitOutcome {
    Current(Result<u64, QuinnLegActorError>),
    Standby(QuinnOwnedProgressWaitOutcome<QuinnStandbyLegJoinOutcome>),
    Retired(QuinnOwnedProgressWaitOutcome<RetiredQuinnLegJoinOutcome>),
}

/// Initial client-A ATTACH owner. The request typestate, fixed actor, private
/// LegIo, and sole FIFO never exist as independently movable production
/// handles while authentication and bootstrap are in progress.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct QuinnClientInitialAttachConfig(SessionConfig);

impl QuinnClientInitialAttachConfig {
    /// Runs the controller's checked capacity arithmetic before a caller
    /// creates a physical actor. The ATTACH transaction accepts only this
    /// witness, so the same overflow cannot be deferred until after startup.
    pub(crate) fn preflight(
        config: SessionConfig,
    ) -> Result<Self, QuinnClientInitialAttachConfigError> {
        config
            .client_recovery_effect_bound()
            .ok_or(QuinnClientInitialAttachConfigError::RecoveryEffectBoundOverflow)?;
        Ok(Self(config))
    }

    const fn session_config(self) -> SessionConfig {
        self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QuinnClientInitialAttachConfigError {
    RecoveryEffectBoundOverflow,
}

pub(crate) struct QuinnClientInitialAttach {
    config: QuinnClientInitialAttachConfig,
    actor: StartedQuinnLegActor,
    awaiting: AwaitingInitialAttach,
}

#[derive(Clone)]
pub(crate) struct QuinnClientInitialAttachProgressCursor(QuinnLegActorOwnerProgressCursor);

#[derive(Debug)]
pub(crate) enum QuinnClientInitialAttachWaitError {
    ActorChanged,
    Actor(QuinnLegActorError),
}

#[derive(Debug)]
pub(crate) enum QuinnClientInitialAttachBeginErrorKind {
    WrongActorRoute,
    Authentication(AttachConfigError),
    InitialFrameInvariant,
    Enqueue(LegOutboundQueueErrorKind),
    Actor(QuinnLegActorError),
}

pub(crate) enum QuinnClientInitialAttachBeginFailureOwnership {
    Request {
        config: QuinnClientInitialAttachConfig,
        request: AttachRequest,
        actor: Box<StartedQuinnLegActor>,
    },
    Signed {
        config: QuinnClientInitialAttachConfig,
        request: AttachRequest,
        frame: Frame,
        actor: Box<StartedQuinnLegActor>,
    },
    Pending {
        config: QuinnClientInitialAttachConfig,
        pending: PendingInitialAttach,
        actor: Box<StartedQuinnLegActor>,
    },
}

pub(crate) struct QuinnClientInitialAttachBeginFailure {
    kind: QuinnClientInitialAttachBeginErrorKind,
    ownership: Box<QuinnClientInitialAttachBeginFailureOwnership>,
}

impl QuinnClientInitialAttachBeginFailure {
    pub(crate) const fn kind(&self) -> &QuinnClientInitialAttachBeginErrorKind {
        &self.kind
    }

    pub(crate) fn into_ownership(self) -> QuinnClientInitialAttachBeginFailureOwnership {
        *self.ownership
    }

    pub(crate) fn into_cancelled_actor(self) -> StartedQuinnLegActor {
        let mut actor = match *self.ownership {
            QuinnClientInitialAttachBeginFailureOwnership::Request { actor, .. }
            | QuinnClientInitialAttachBeginFailureOwnership::Signed { actor, .. }
            | QuinnClientInitialAttachBeginFailureOwnership::Pending { actor, .. } => *actor,
        };
        actor.request_cancel();
        actor
    }
}

impl std::fmt::Debug for QuinnClientInitialAttachBeginFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("QuinnClientInitialAttachBeginFailure")
            .field("kind", &self.kind)
            .field("ownership", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QuinnClientInitialAttachProgress {
    Submitted,
    OrderedDeliveryPending,
    AwaitingResponse,
}

pub(crate) enum QuinnClientInitialAttachDrive {
    InProgress {
        owner: Box<QuinnClientInitialAttach>,
        progress: QuinnClientInitialAttachProgress,
    },
    Ready(Box<QuinnClientSessionComposition>),
}

#[derive(Debug)]
pub(crate) enum QuinnClientInitialAttachDriveErrorKind {
    Actor(QuinnLegActorError),
    Response(InitialAttachResponseErrorKind),
    CompositionInvariant,
    Bootstrap(QuinnClientSessionBootstrapErrorKind),
}

pub(crate) enum QuinnClientInitialAttachDriveFailureOwnership {
    Awaiting(Box<QuinnClientInitialAttach>),
    Actor {
        config: QuinnClientInitialAttachConfig,
        actor: Box<StartedQuinnLegActor>,
    },
    Bootstrap(Box<QuinnClientSessionBootstrapFailure>),
}

pub(crate) struct QuinnClientInitialAttachDriveFailure {
    kind: QuinnClientInitialAttachDriveErrorKind,
    ownership: Box<QuinnClientInitialAttachDriveFailureOwnership>,
}

impl QuinnClientInitialAttachDriveFailure {
    pub(crate) const fn kind(&self) -> &QuinnClientInitialAttachDriveErrorKind {
        &self.kind
    }

    pub(crate) fn into_ownership(self) -> QuinnClientInitialAttachDriveFailureOwnership {
        *self.ownership
    }

    pub(crate) fn into_cancelled_actor(self) -> StartedQuinnLegActor {
        match *self.ownership {
            QuinnClientInitialAttachDriveFailureOwnership::Awaiting(owner) => {
                (*owner).into_cancelled_actor()
            }
            QuinnClientInitialAttachDriveFailureOwnership::Actor { config: _, actor } => {
                let mut actor = *actor;
                actor.request_cancel();
                actor
            }
            QuinnClientInitialAttachDriveFailureOwnership::Bootstrap(failure) => {
                let (_config, _accepted, mut actor) = (*failure).into_parts();
                actor.request_cancel();
                actor
            }
        }
    }
}

impl std::fmt::Debug for QuinnClientInitialAttachDriveFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("QuinnClientInitialAttachDriveFailure")
            .field("kind", &self.kind)
            .field("ownership", &"[REDACTED]")
            .finish()
    }
}

impl QuinnClientInitialAttach {
    pub(crate) fn begin(
        config: QuinnClientInitialAttachConfig,
        mut actor: StartedQuinnLegActor,
        request: AttachRequest,
        credentials: &AttachCredentials,
    ) -> Result<Self, QuinnClientInitialAttachBeginFailure> {
        enum PrepareFailure {
            Request {
                request: AttachRequest,
                kind: QuinnClientInitialAttachBeginErrorKind,
            },
            Signed {
                request: AttachRequest,
                frame: Frame,
                kind: QuinnClientInitialAttachBeginErrorKind,
            },
            Pending {
                pending: PendingInitialAttach,
                kind: QuinnClientInitialAttachBeginErrorKind,
            },
        }

        let gate = QuinnLegCompositionGate::mint();
        let transaction = actor.with_controller_parts_mut_input(
            &gate,
            (config, request),
            |(config, request), io, queue| {
                let prepared = (|| {
                    if io.outbound_route() != WireRoute::new(LegId::A, WireDirection::ClientToOwner)
                    {
                        return Err(PrepareFailure::Request {
                            request,
                            kind: QuinnClientInitialAttachBeginErrorKind::WrongActorRoute,
                        });
                    }
                    let binding = io.established_leg().standby_transport_binding();
                    let proof = match credentials.prove(&request, &binding) {
                        Ok(proof) => proof,
                        Err(error) => {
                            return Err(PrepareFailure::Request {
                                request,
                                kind: QuinnClientInitialAttachBeginErrorKind::Authentication(error),
                            });
                        }
                    };
                    let frame = request.to_attach_frame(proof);
                    let pending = match io.established_leg().begin_initial_attach(request, frame) {
                        Ok(pending) => pending,
                        Err(failure) => {
                            let (request, frame) = failure.into_parts();
                            return Err(PrepareFailure::Signed {
                                request,
                                frame,
                                kind: QuinnClientInitialAttachBeginErrorKind::InitialFrameInvariant,
                            });
                        }
                    };
                    match pending.enqueue(queue) {
                        Ok(awaiting) => Ok(awaiting),
                        Err(failure) => {
                            let kind = QuinnClientInitialAttachBeginErrorKind::Enqueue(
                                failure.kind().clone(),
                            );
                            Err(PrepareFailure::Pending {
                                pending: failure.into_pending(),
                                kind,
                            })
                        }
                    }
                })();
                (config, prepared)
            },
        );
        match transaction {
            Ok((config, Ok(awaiting))) => Ok(Self {
                config,
                actor,
                awaiting,
            }),
            Ok((config, Err(PrepareFailure::Request { request, kind }))) => {
                Err(QuinnClientInitialAttachBeginFailure {
                    kind,
                    ownership: Box::new(QuinnClientInitialAttachBeginFailureOwnership::Request {
                        config,
                        request,
                        actor: Box::new(actor),
                    }),
                })
            }
            Ok((
                config,
                Err(PrepareFailure::Signed {
                    request,
                    frame,
                    kind,
                }),
            )) => Err(QuinnClientInitialAttachBeginFailure {
                kind,
                ownership: Box::new(QuinnClientInitialAttachBeginFailureOwnership::Signed {
                    config,
                    request,
                    frame,
                    actor: Box::new(actor),
                }),
            }),
            Ok((config, Err(PrepareFailure::Pending { pending, kind }))) => {
                Err(QuinnClientInitialAttachBeginFailure {
                    kind,
                    ownership: Box::new(QuinnClientInitialAttachBeginFailureOwnership::Pending {
                        config,
                        pending,
                        actor: Box::new(actor),
                    }),
                })
            }
            Err(((config, request), actor_error)) => Err(QuinnClientInitialAttachBeginFailure {
                kind: QuinnClientInitialAttachBeginErrorKind::Actor(actor_error),
                ownership: Box::new(QuinnClientInitialAttachBeginFailureOwnership::Request {
                    config,
                    request,
                    actor: Box::new(actor),
                }),
            }),
        }
    }

    pub(crate) fn progress_cursor(&self) -> QuinnClientInitialAttachProgressCursor {
        QuinnClientInitialAttachProgressCursor(self.actor.owner_progress_cursor())
    }

    #[cfg(test)]
    pub(crate) fn actor_snapshot_for_test(
        &self,
    ) -> crate::owned_upstream::quinn_leg_actor::QuinnLegActorSnapshot {
        self.actor.actor_snapshot()
    }

    pub(crate) async fn wait_for_progress(
        &self,
        cursor: QuinnClientInitialAttachProgressCursor,
    ) -> Result<u64, QuinnClientInitialAttachWaitError> {
        if !self.actor.matches_owner_progress_cursor(&cursor.0) {
            return Err(QuinnClientInitialAttachWaitError::ActorChanged);
        }
        self.actor
            .wait_for_owner_progress(cursor.0)
            .await
            .map_err(QuinnClientInitialAttachWaitError::Actor)
    }

    pub(crate) fn into_cancelled_actor(mut self) -> StartedQuinnLegActor {
        self.actor.request_cancel();
        self.actor
    }

    pub(crate) fn drive_one(
        self,
        now: SimTime,
    ) -> Result<QuinnClientInitialAttachDrive, QuinnClientInitialAttachDriveFailure> {
        let Self {
            config,
            mut actor,
            awaiting,
        } = self;
        let progress = match actor.try_flush(now) {
            Ok(crate::owned_upstream::quinn_leg_actor::QuinnLegActorFlushDisposition::Submitted) => {
                Some(QuinnClientInitialAttachProgress::Submitted)
            }
            Ok(
                crate::owned_upstream::quinn_leg_actor::QuinnLegActorFlushDisposition::OrderedDeliveryPending,
            ) => Some(QuinnClientInitialAttachProgress::OrderedDeliveryPending),
            Ok(crate::owned_upstream::quinn_leg_actor::QuinnLegActorFlushDisposition::Idle) => None,
            Err(actor_error) => {
                return Err(QuinnClientInitialAttachDriveFailure {
                    kind: QuinnClientInitialAttachDriveErrorKind::Actor(actor_error),
                    ownership: Box::new(
                        QuinnClientInitialAttachDriveFailureOwnership::Awaiting(Box::new(Self {
                            config,
                            actor,
                            awaiting,
                        })),
                    ),
                });
            }
        };
        if let Some(progress) = progress {
            return Ok(QuinnClientInitialAttachDrive::InProgress {
                owner: Box::new(Self {
                    config,
                    actor,
                    awaiting,
                }),
                progress,
            });
        }

        let gate = QuinnLegCompositionGate::mint();
        let mut awaiting = Some(awaiting);
        let mut response_error = None;
        let transaction = actor.try_transact_composition_inbound(
            &gate,
            FeatureSet::default(),
            |inbound, _arrived_at, queue| {
                let BoundInbound::Session(response) = inbound else {
                    drop(inbound);
                    return Err(QuinnLegActorError::Protocol(
                        "initial client received non-session traffic before ATTACH acceptance",
                    ));
                };
                let Some(pending) = awaiting.take() else {
                    drop(response);
                    return Err(QuinnLegActorError::Protocol(
                        "initial client ATTACH typestate was already consumed",
                    ));
                };
                match pending.validate_response(queue, response) {
                    Ok(accepted) => Ok(accepted),
                    Err(failure) => {
                        let kind = failure.kind();
                        let (pending, response) = failure.into_parts();
                        awaiting = Some(pending);
                        response_error = Some(kind);
                        drop(response);
                        Err(QuinnLegActorError::Protocol(
                            "initial client ATTACH response was rejected",
                        ))
                    }
                }
            },
        );
        match transaction {
            Ok(QuinnLegActorInboundPoll::Idle) => {
                let Some(awaiting) = awaiting else {
                    return Err(QuinnClientInitialAttachDriveFailure {
                        kind: QuinnClientInitialAttachDriveErrorKind::CompositionInvariant,
                        ownership: Box::new(QuinnClientInitialAttachDriveFailureOwnership::Actor {
                            config,
                            actor: Box::new(actor),
                        }),
                    });
                };
                Ok(QuinnClientInitialAttachDrive::InProgress {
                    owner: Box::new(Self {
                        config,
                        actor,
                        awaiting,
                    }),
                    progress: QuinnClientInitialAttachProgress::AwaitingResponse,
                })
            }
            Ok(QuinnLegActorInboundPoll::Classified(accepted)) => {
                match QuinnClientSessionComposition::bootstrap_initial(
                    config.session_config(),
                    accepted,
                    actor,
                ) {
                    Ok(composition) => {
                        Ok(QuinnClientInitialAttachDrive::Ready(Box::new(composition)))
                    }
                    Err(failure) => Err(QuinnClientInitialAttachDriveFailure {
                        kind: QuinnClientInitialAttachDriveErrorKind::Bootstrap(failure.kind()),
                        ownership: Box::new(
                            QuinnClientInitialAttachDriveFailureOwnership::Bootstrap(Box::new(
                                failure,
                            )),
                        ),
                    }),
                }
            }
            Ok(QuinnLegActorInboundPoll::Blocked) => {
                let ownership = match awaiting {
                    Some(awaiting) => {
                        QuinnClientInitialAttachDriveFailureOwnership::Awaiting(Box::new(Self {
                            config,
                            actor,
                            awaiting,
                        }))
                    }
                    None => QuinnClientInitialAttachDriveFailureOwnership::Actor {
                        config,
                        actor: Box::new(actor),
                    },
                };
                Err(QuinnClientInitialAttachDriveFailure {
                    kind: QuinnClientInitialAttachDriveErrorKind::CompositionInvariant,
                    ownership: Box::new(ownership),
                })
            }
            Err(actor_error) => {
                let kind = response_error.map_or_else(
                    || QuinnClientInitialAttachDriveErrorKind::Actor(actor_error),
                    QuinnClientInitialAttachDriveErrorKind::Response,
                );
                let ownership = match awaiting {
                    Some(awaiting) => {
                        QuinnClientInitialAttachDriveFailureOwnership::Awaiting(Box::new(Self {
                            config,
                            actor,
                            awaiting,
                        }))
                    }
                    None => QuinnClientInitialAttachDriveFailureOwnership::Actor {
                        config,
                        actor: Box::new(actor),
                    },
                };
                Err(QuinnClientInitialAttachDriveFailure {
                    kind,
                    ownership: Box::new(ownership),
                })
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QuinnClientSessionBootstrapErrorKind {
    ActorTransportUnavailable,
    Controller(ClientControllerBootstrapErrorKind),
}

/// Ownership-preserving bootstrap rejection. The accepted capability and
/// whole actor remain paired for an exact retry or terminal path.
pub(crate) struct QuinnClientSessionBootstrapFailure {
    ownership: Box<QuinnClientSessionBootstrapOwnership>,
    kind: QuinnClientSessionBootstrapErrorKind,
}

struct QuinnClientSessionBootstrapOwnership {
    config: SessionConfig,
    accepted: AcceptedInitialAttach,
    actor: StartedQuinnLegActor,
}

impl QuinnClientSessionBootstrapFailure {
    pub(crate) const fn kind(&self) -> QuinnClientSessionBootstrapErrorKind {
        self.kind
    }

    pub(crate) fn into_parts(self) -> (SessionConfig, AcceptedInitialAttach, StartedQuinnLegActor) {
        let QuinnClientSessionBootstrapOwnership {
            config,
            accepted,
            actor,
        } = *self.ownership;
        (config, accepted, actor)
    }
}

impl std::fmt::Debug for QuinnClientSessionBootstrapFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("QuinnClientSessionBootstrapFailure")
            .field("kind", &self.kind)
            .field("ownership", &"[REDACTED]")
            .finish()
    }
}

impl QuinnClientSessionComposition {
    pub(crate) fn session_clock(&self) -> SessionMonotonicClock {
        self.session_clock.clone()
    }

    pub(crate) fn physical_attach_inventory(
        &self,
    ) -> Result<QuinnPhysicalAttachInventory, QuinnPhysicalAttachAuditError> {
        let accepted_marker_generation =
            match self.standby.as_ref().and_then(|owner| owner.state.as_ref()) {
                Some(QuinnRegisteredStandbyState::AcceptedPendingCommit { generation }) => {
                    Some(*generation)
                }
                _ => None,
            };
        let accepted_shutdown_generation = self
            .accepted_physical_shutdown_discard
            .map(QuinnAcceptedPhysicalShutdownDiscard::generation);
        let Some(physical) = self.physical_attach.as_ref() else {
            if accepted_marker_generation.is_some()
                && accepted_marker_generation != accepted_shutdown_generation
            {
                return Err(QuinnPhysicalAttachAuditError::AcceptedMarkerWithoutAuthority);
            }
            return Ok(QuinnPhysicalAttachInventory {
                stage: None,
                authority_kind: None,
                live_authorities: 0,
                pending_frames: 0,
                summed_pending_frame_bytes: 0,
                stored_pending_frame_bytes: 0,
                accepted_pending_commit: None,
            });
        };
        let (authority_kind, accepted_pending_commit) = match &physical.authority {
            QuinnPhysicalAttachAuthority::Pending(_) => {
                if physical.stage == PhysicalPendingAttachStage::Accepted
                    || accepted_marker_generation.is_some()
                    || accepted_shutdown_generation.is_some()
                {
                    return Err(QuinnPhysicalAttachAuditError::AcceptedOwnershipMismatch);
                }
                (QuinnPhysicalAttachAuthorityKind::Pending, None)
            }
            QuinnPhysicalAttachAuthority::Accepted(held) => {
                if accepted_shutdown_generation.is_some() {
                    return Err(QuinnPhysicalAttachAuditError::AcceptedOwnershipMismatch);
                }
                let marker_generation = accepted_marker_generation
                    .ok_or(QuinnPhysicalAttachAuditError::AcceptedOwnershipMismatch)?;
                let (accepted_generation, policy_generation) = match held.value() {
                    QuinnStandbyHeldControl::AttachAccepted { accepted, policy } => {
                        (accepted.generation(), policy.generation())
                    }
                    QuinnStandbyHeldControl::ProbeAck(_)
                    | QuinnStandbyHeldControl::SwitchHint(_) => {
                        return Err(QuinnPhysicalAttachAuditError::AcceptedOwnershipMismatch);
                    }
                };
                if policy_generation != accepted_generation {
                    return Err(QuinnPhysicalAttachAuditError::AcceptedPolicyMismatch);
                }
                if physical.stage != PhysicalPendingAttachStage::Accepted
                    || physical.stored_pending_frame_bytes != 0
                    || marker_generation != accepted_generation
                {
                    return Err(QuinnPhysicalAttachAuditError::AcceptedOwnershipMismatch);
                }
                (
                    QuinnPhysicalAttachAuthorityKind::Accepted,
                    Some(QuinnAcceptedPendingCommitObservation {
                        accepted_generation,
                        marker_generation,
                        held_records: held.held_records(),
                        held_encoded_bytes: held.encoded_bytes(),
                        arrived_at: held.arrived_at(),
                        publication_epoch: held.publication_epoch(),
                        send_ordinal: held.send_ordinal(),
                        queued_inbound_markers: held.queued_inbound_markers(),
                        composition_message_permits: held.composition_message_permits(),
                        reserved_delivery_acks: held.reserved_delivery_acks(),
                        reserved_delivery_ack_bytes: held.reserved_delivery_ack_bytes(),
                    }),
                )
            }
            QuinnPhysicalAttachAuthority::Shutdown(_) => {
                // After the raw accepted record is discarded, a wrong-arbiter
                // rejection retains only the exact policy settlement. The
                // marker and copy-only discard audit remain paired until B's
                // structural shutdown retires its typestate.
                let accepted_shutdown_layout = physical.stage
                    == PhysicalPendingAttachStage::Accepted
                    && accepted_marker_generation.is_some()
                    && accepted_marker_generation == accepted_shutdown_generation;
                let non_accepted_shutdown_layout = physical.stage
                    != PhysicalPendingAttachStage::Accepted
                    && accepted_marker_generation.is_none()
                    && accepted_shutdown_generation.is_none();
                if !accepted_shutdown_layout && !non_accepted_shutdown_layout {
                    return Err(QuinnPhysicalAttachAuditError::AcceptedOwnershipMismatch);
                }
                (QuinnPhysicalAttachAuthorityKind::Shutdown, None)
            }
        };
        let (pending_frames, summed_pending_frame_bytes, stored_pending_frame_bytes) =
            if physical.stage == PhysicalPendingAttachStage::AttachPending {
                match self.standby.as_ref().and_then(|owner| owner.state.as_ref()) {
                    Some(QuinnRegisteredStandbyState::AttachPending(pending)) => {
                        let stored = physical.stored_pending_frame_bytes;
                        let summed =
                            audit_physical_pending_frame_bytes(pending.encoded_len(), stored)?;
                        (1, summed, stored)
                    }
                    _ => return Err(QuinnPhysicalAttachAuditError::PendingFrameMissing),
                }
            } else {
                if physical.stored_pending_frame_bytes != 0 {
                    return Err(
                        QuinnPhysicalAttachAuditError::UnexpectedStoredPendingFrameBytes {
                            stored: physical.stored_pending_frame_bytes,
                        },
                    );
                }
                if matches!(
                    self.standby.as_ref().and_then(|owner| owner.state.as_ref()),
                    Some(QuinnRegisteredStandbyState::AttachPending(_))
                ) {
                    return Err(QuinnPhysicalAttachAuditError::PendingFrameUnexpected);
                }
                (0, 0, 0)
            };
        Ok(QuinnPhysicalAttachInventory {
            stage: Some(physical.stage),
            authority_kind: Some(authority_kind),
            live_authorities: 1,
            pending_frames,
            summed_pending_frame_bytes,
            stored_pending_frame_bytes,
            accepted_pending_commit,
        })
    }

    /// Exercises the marker-to-slot direction of the accepted layout audit
    /// without exposing or destroying the retained physical authority.
    #[cfg(test)]
    pub(super) fn physical_attach_inventory_with_slot_omitted_for_test(
        &mut self,
    ) -> Result<QuinnPhysicalAttachInventory, QuinnPhysicalAttachAuditError> {
        let physical = self.physical_attach.take();
        let audit = self.physical_attach_inventory();
        self.physical_attach = physical;
        audit
    }

    pub(crate) const fn accepted_physical_shutdown_discard(
        &self,
    ) -> Option<QuinnAcceptedPhysicalShutdownDiscard> {
        self.accepted_physical_shutdown_discard
    }

    pub(super) fn admit_pending_physical_attach(
        &mut self,
        arbiter: &ClientSwitchArbiter,
        permit: PendingPhysicalAttachPermit,
    ) -> Result<(), QuinnPhysicalAttachAdmissionFailure> {
        let reject = |permit, kind| QuinnPhysicalAttachAdmissionFailure { kind, permit };
        if self.phase != QuinnClientCompositionPhase::Running {
            return Err(reject(
                permit,
                QuinnPhysicalAttachAdmissionErrorKind::SessionShuttingDown,
            ));
        }
        if self.physical_attach.is_some() {
            return Err(reject(
                permit,
                QuinnPhysicalAttachAdmissionErrorKind::SlotOccupied,
            ));
        }
        let intent = match arbiter.preflight_physical_permit(&permit) {
            Ok(intent) => intent,
            Err(kind) => {
                return Err(reject(
                    permit,
                    QuinnPhysicalAttachAdmissionErrorKind::Arbiter(kind),
                ));
            }
        };
        let Some(owner) = self.standby.as_ref() else {
            return Err(reject(
                permit,
                QuinnPhysicalAttachAdmissionErrorKind::MissingStandby,
            ));
        };
        if owner.registration_nonce != intent.standby_nonce() {
            return Err(reject(
                permit,
                QuinnPhysicalAttachAdmissionErrorKind::WrongStandbyNonce,
            ));
        }
        let current_generation = self.controller.generation();
        let requested_generation = current_generation
            .get()
            .checked_add(1)
            .and_then(|value| LegGeneration::new(value).ok());
        if owner.admitted_generation != current_generation
            || intent.current_generation() != current_generation
            || requested_generation != Some(intent.requested_generation())
        {
            return Err(reject(
                permit,
                QuinnPhysicalAttachAdmissionErrorKind::WrongGeneration,
            ));
        }
        if matches!(
            owner.state,
            Some(
                QuinnRegisteredStandbyState::AttachPending(_)
                    | QuinnRegisteredStandbyState::AwaitingAttach(_)
                    | QuinnRegisteredStandbyState::Accepted(_)
                    | QuinnRegisteredStandbyState::AcceptedPendingCommit { .. }
                    | QuinnRegisteredStandbyState::GenerationStatus(_)
            )
        ) {
            return Err(reject(
                permit,
                QuinnPhysicalAttachAdmissionErrorKind::WrongState,
            ));
        }
        self.physical_attach = Some(QuinnPhysicalAttachOwner {
            stage: PhysicalPendingAttachStage::Unbegun,
            authority: QuinnPhysicalAttachAuthority::Pending(permit),
            stored_pending_frame_bytes: 0,
        });
        Ok(())
    }

    /// Closes arbiter admission and consumes the exact retained physical
    /// authority synchronously, before structural actor shutdown can await.
    /// A rejected settlement is restored in the capacity-one slot, so a
    /// caller can repair correlation and retry without minting ownership.
    pub(super) fn settle_pending_physical_attach_for_shutdown(
        &mut self,
        arbiter: &mut ClientSwitchArbiter,
    ) -> Result<Option<SwitchDiscardAudit>, QuinnPhysicalAttachShutdownError> {
        let disposition = arbiter.close_switch_admission_for_shutdown();
        let Some(_) = self.physical_attach.as_ref() else {
            if arbiter.inventory().pending_physical_settlements() != 0 {
                return Err(QuinnPhysicalAttachShutdownError::MissingPhysicalAuthority);
            }
            return Ok(arbiter.discard_audit());
        };
        if arbiter.inventory().pending_physical_settlements() != 1 {
            return Err(QuinnPhysicalAttachShutdownError::ArbiterNotPending(
                disposition,
            ));
        }
        let inventory = self
            .physical_attach_inventory()
            .map_err(QuinnPhysicalAttachShutdownError::Audit)?;
        let physical = self
            .physical_attach
            .take()
            .ok_or(QuinnPhysicalAttachShutdownError::MissingPhysicalAuthority)?;
        let QuinnPhysicalAttachOwner {
            stage,
            authority,
            stored_pending_frame_bytes,
        } = physical;
        let settlement = match authority {
            QuinnPhysicalAttachAuthority::Pending(permit) => permit.into_shutdown_settlement(stage),
            QuinnPhysicalAttachAuthority::Accepted(held) => {
                let Some(observation) = inventory.accepted_pending_commit() else {
                    self.physical_attach = Some(QuinnPhysicalAttachOwner {
                        stage,
                        authority: QuinnPhysicalAttachAuthority::Accepted(held),
                        stored_pending_frame_bytes,
                    });
                    return Err(QuinnPhysicalAttachShutdownError::AcceptedInboundPending);
                };
                let gate = QuinnLegCompositionGate::mint();
                let discarded = match held.try_shutdown_discard(&gate, |control| match control {
                    QuinnStandbyHeldControl::AttachAccepted { accepted, policy } => Ok((
                        accepted,
                        policy.into_shutdown_settlement(PhysicalPendingAttachStage::Accepted),
                    )),
                    other => Err((other, ())),
                }) {
                    Ok(discarded) => discarded,
                    Err((held, ())) => {
                        self.physical_attach = Some(QuinnPhysicalAttachOwner {
                            stage,
                            authority: QuinnPhysicalAttachAuthority::Accepted(held),
                            stored_pending_frame_bytes,
                        });
                        return Err(QuinnPhysicalAttachShutdownError::AcceptedInboundPending);
                    }
                };
                let (accepted, settlement) = discarded.into_value();
                let accepted_discard = accepted.discard_for_shutdown();
                self.accepted_physical_shutdown_discard =
                    Some(QuinnAcceptedPhysicalShutdownDiscard {
                        generation: accepted_discard.generation(),
                        held_records: observation.held_records(),
                        held_encoded_bytes: observation.held_encoded_bytes(),
                        queued_inbound_markers: observation.queued_inbound_markers(),
                        composition_message_permits: observation.composition_message_permits(),
                        reserved_delivery_acks_discarded: observation.reserved_delivery_acks(),
                        reserved_delivery_ack_bytes_discarded: observation
                            .reserved_delivery_ack_bytes(),
                        delivery_acks_emitted: 0,
                        accepted_authorities: accepted_discard.accepted_authorities(),
                        request_receipts: accepted_discard.request_receipts(),
                        policy_settlements: 1,
                        arrived_at: observation.arrived_at(),
                        publication_epoch: observation.publication_epoch(),
                        send_ordinal: observation.send_ordinal(),
                    });
                settlement
            }
            QuinnPhysicalAttachAuthority::Shutdown(settlement) => settlement,
        };
        match arbiter.settle_shutdown_pending(settlement) {
            Ok(audit) => Ok(Some(audit)),
            Err(failure) => {
                let kind = failure.kind();
                self.physical_attach = Some(QuinnPhysicalAttachOwner {
                    stage,
                    authority: QuinnPhysicalAttachAuthority::Shutdown(failure.into_settlement()),
                    stored_pending_frame_bytes,
                });
                Err(QuinnPhysicalAttachShutdownError::Arbiter(kind))
            }
        }
    }

    pub(crate) fn drive_owned_standby_physical_attach(
        &mut self,
        now: SimTime,
        credentials: &AttachCredentials,
    ) -> Result<QuinnStandbyAttachDrive, QuinnStandbyAttachError> {
        let intent = match self.physical_attach.as_ref() {
            Some(QuinnPhysicalAttachOwner {
                authority: QuinnPhysicalAttachAuthority::Pending(permit),
                ..
            }) => permit.intent(),
            _ => return Err(QuinnStandbyAttachError::MissingPhysicalPermit),
        };
        if self.phase != QuinnClientCompositionPhase::Running {
            return Err(QuinnStandbyAttachError::SessionShuttingDown);
        }
        let (generation, fresh_required) = if intent.is_post_loss() {
            self.post_loss_attach_context()?
        } else if self.current_policy_is_active() {
            (intent.current_generation(), false)
        } else {
            return Err(QuinnStandbyAttachError::TerminalNotApplied);
        };
        if generation != intent.current_generation() {
            return Err(QuinnStandbyAttachError::WrongSwitchEpoch);
        }
        let outcome = self.drive_owned_standby_attach_core(
            now,
            intent.attach_nonce(),
            credentials,
            generation,
            fresh_required,
        )?;
        let stage = match outcome.disposition {
            QuinnStandbyAttachDrive::AwaitingResponse => PhysicalPendingAttachStage::AwaitingAttach,
            QuinnStandbyAttachDrive::Pressure => PhysicalPendingAttachStage::AttachPending,
        };
        let Some(QuinnPhysicalAttachOwner {
            stage: owned_stage,
            authority: QuinnPhysicalAttachAuthority::Pending(_),
            stored_pending_frame_bytes,
        }) = self.physical_attach.as_mut()
        else {
            return Err(QuinnStandbyAttachError::CompositionInvariant);
        };
        *owned_stage = stage;
        *stored_pending_frame_bytes = outcome.pending_frame_bytes;
        Ok(outcome.disposition)
    }

    pub(super) fn current_exact_leg_probe_responder(&self) -> Option<ExactLegProbeResponder> {
        self.controller.current_exact_leg_probe_responder()
    }

    /// Immutable actor-captured hard-terminal time retained across controller
    /// bind, logical loss application, and delayed physical cleanup. This is
    /// the future R7 arbiter's hard-trigger evidence; local cancellation has no
    /// failure timestamp.
    pub(crate) fn current_terminal_failure_observed_at(&self) -> Option<SimTime> {
        self.current_terminal_stamp
            .as_ref()
            .and_then(ActorTerminalStampWitness::observed_at)
    }

    #[cfg(test)]
    pub(super) fn current_terminal_stamp_for_test(&self) -> Option<&ActorTerminalStampWitness> {
        self.current_terminal_stamp.as_ref()
    }

    #[cfg(test)]
    pub(super) fn current_joined_terminal_stamp_for_test(
        &self,
    ) -> Option<&ActorTerminalStampWitness> {
        self.current_joined
            .as_ref()
            .and_then(QuinnLegActorJoined::terminal_stamp)
    }

    fn current_policy_is_active(&self) -> bool {
        self.phase == QuinnClientCompositionPhase::Running
            && self.current_terminal == QuinnCurrentTerminalState::Unbound
            && self.current_terminal_stamp.is_none()
    }

    /// Consumes the whole actor and installs only that actor's private
    /// `LegIo/FIFO`. Every controller rejection restores the two pieces before
    /// the still-whole actor is returned.
    pub(crate) fn bootstrap_initial(
        config: SessionConfig,
        accepted: AcceptedInitialAttach,
        mut actor: StartedQuinnLegActor,
    ) -> Result<Self, QuinnClientSessionBootstrapFailure> {
        let gate = QuinnLegCompositionGate::mint();
        let session_clock = actor.session_clock(&gate);
        let controller = actor.transact_controller_parts(
            &gate,
            (config, accepted),
            |(config, accepted), io, queue| match ClientSessionController::bootstrap_initial(
                config, accepted, io, queue,
            ) {
                Ok(controller) => Ok(controller),
                Err(failure) => {
                    let kind = failure.kind();
                    let (config, accepted, io, queue) = failure.into_parts();
                    Err(Box::new(((config, accepted), io, queue, kind)))
                }
            },
        );
        match controller {
            Ok(controller) => Ok(Self {
                session_clock,
                controller,
                current: actor,
                current_joined: None,
                current_terminal: QuinnCurrentTerminalState::Unbound,
                current_terminal_stamp: None,
                retired: None,
                standby: None,
                physical_attach: None,
                accepted_physical_shutdown_discard: None,
                current_loss_source: QuinnCurrentLossSource::None,
                fatal_recovery_tail_shutdown: None,
                shutdown_probe_scheduler: None,
                phase: QuinnClientCompositionPhase::Running,
            }),
            Err(((config, accepted), kind)) => Err(QuinnClientSessionBootstrapFailure {
                ownership: Box::new(QuinnClientSessionBootstrapOwnership {
                    config,
                    accepted,
                    actor,
                }),
                kind: kind.map_or(
                    QuinnClientSessionBootstrapErrorKind::ActorTransportUnavailable,
                    QuinnClientSessionBootstrapErrorKind::Controller,
                ),
            }),
        }
    }

    /// Queues a standby registration on the exact candidate actor while the
    /// installed current authority remains private inside the controller.
    /// This compatibility-only test seam predates composition-owned B and is
    /// deliberately absent from production builds.
    #[cfg(test)]
    pub(super) fn begin_standby_registration(
        &mut self,
        candidate: &mut StartedQuinnLegActor,
        nonce: StandbyNonce,
        credentials: &AttachCredentials,
    ) -> Result<AwaitingStandbyAccepted, QuinnStandbyRegistrationFailure> {
        if !self.current_policy_is_active() {
            return Err(QuinnStandbyRegistrationFailure::SessionShuttingDown);
        }
        if self.standby.is_some() {
            return Err(QuinnStandbyRegistrationFailure::CompositionInvariant);
        }
        let gate = QuinnLegCompositionGate::mint();
        let controller = &mut self.controller;
        self.current
            .transact_controller_policy(&gate, |runtime| {
                if !controller.current_physical_actor_matches(runtime, &gate) {
                    return Err(QuinnStandbyRegistrationFailure::CompositionInvariant);
                }
                candidate
                    .with_controller_parts_mut(&gate, |io, queue| {
                        controller
                            .begin_current_standby_registration(
                                io.established_leg(),
                                nonce,
                                credentials,
                            )
                            .map_err(QuinnStandbyRegistrationFailure::Begin)?
                            .enqueue(queue)
                            .map_err(|error| {
                                QuinnStandbyRegistrationFailure::Enqueue(Box::new(error))
                            })
                    })
                    .ok_or(QuinnStandbyRegistrationFailure::ActorTransportUnavailable)?
            })
            .map_err(|_| QuinnStandbyRegistrationFailure::ActorTransportUnavailable)?
    }

    /// Begins the production composition-owned B lifecycle. The whole actor
    /// enters the sole standby slot before any registration authority can
    /// escape; queue pressure retains the exact pending frame internally.
    pub(crate) fn begin_owned_standby_registration(
        &mut self,
        actor: StartedQuinnLegActor,
        nonce: StandbyNonce,
        credentials: &AttachCredentials,
        liveness: QuinnStandbyLivenessConfig,
    ) -> Result<QuinnOwnedStandbyRegistrationDrive, QuinnOwnedStandbyRegistrationFailure> {
        let mut actor = actor;
        let reject = |actor: StartedQuinnLegActor, kind| QuinnOwnedStandbyRegistrationFailure {
            actor: Box::new(actor),
            kind,
        };
        if !self.current_policy_is_active() {
            return Err(reject(
                actor,
                QuinnOwnedStandbyRegistrationErrorKind::SessionShuttingDown,
            ));
        }
        if self.standby.is_some() {
            return Err(reject(
                actor,
                QuinnOwnedStandbyRegistrationErrorKind::StandbyOccupied,
            ));
        }
        let gate = QuinnLegCompositionGate::mint();
        if !self.session_clock.same_source(&actor.session_clock(&gate)) {
            return Err(reject(
                actor,
                QuinnOwnedStandbyRegistrationErrorKind::ClockSourceMismatch,
            ));
        }
        let controller = &mut self.controller;
        let transaction = self.current.transact_controller_policy(&gate, |runtime| {
            if !controller.current_physical_actor_matches(runtime, &gate) {
                return Err(QuinnOwnedStandbyRegistrationErrorKind::CompositionInvariant);
            }
            actor
                .with_controller_parts_mut(&gate, |io, queue| {
                    let pending = controller
                        .begin_current_standby_registration(
                            io.established_leg(),
                            nonce,
                            credentials,
                        )
                        .map_err(QuinnOwnedStandbyRegistrationErrorKind::Begin)?;
                    let installed_features = pending.installed_features();
                    match pending.enqueue(queue) {
                        Ok(awaiting) => Ok((
                            QuinnOwnedStandbyRegistrationDrive::Queued,
                            QuinnRegisteredStandbyState::AwaitingRegistration(awaiting),
                            installed_features,
                            None,
                        )),
                        Err(failure) => {
                            let classification = classify_leg_control_enqueue(failure.kind());
                            let state = QuinnRegisteredStandbyState::RegistrationPending(
                                failure.into_pending(),
                            );
                            let (drive, owned_failure) = match classification {
                                Ok(()) => (QuinnOwnedStandbyRegistrationDrive::Pressure, None),
                                Err(kind) => (
                                    QuinnOwnedStandbyRegistrationDrive::Failed(kind),
                                    Some(standby_failure_for_queue(kind)),
                                ),
                            };
                            Ok((drive, state, installed_features, owned_failure))
                        }
                    }
                })
                .ok_or(QuinnOwnedStandbyRegistrationErrorKind::ActorTransportUnavailable)?
        });
        let (drive, owned_state, installed_features, owned_failure) = match transaction {
            Ok(Ok(ownership)) => ownership,
            Ok(Err(kind)) => return Err(reject(actor, kind)),
            Err(_) => {
                return Err(reject(
                    actor,
                    QuinnOwnedStandbyRegistrationErrorKind::ActorTransportUnavailable,
                ));
            }
        };
        self.standby = Some(QuinnRegisteredStandbyOwner {
            actor: Some(QuinnStandbyActorOwner::Running(Box::new(actor))),
            state: Some(owned_state),
            admitted_generation: self.controller.generation(),
            registration_nonce: nonce,
            next_probe_sequence: ProbeSequence::new(1).ok(),
            current_probe_sequence: None,
            liveness,
            features: installed_features,
            failure: owned_failure,
            probe_responder: None,
            pending_control: None,
            hint_actor_seal: StandbyActorSeal::mint_for_exact_b(&gate),
            shutdown_control_discard: None,
            shutdown_terminal: None,
            shutdown_retired_state: None,
        });
        Ok(drive)
    }

    pub(crate) fn drive_owned_standby_registration(
        &mut self,
    ) -> Result<QuinnOwnedStandbyRegistrationDrive, QuinnOwnedStandbyRegistrationErrorKind> {
        if !self.current_policy_is_active() {
            return Err(QuinnOwnedStandbyRegistrationErrorKind::SessionShuttingDown);
        }
        let owner = self
            .standby
            .as_mut()
            .ok_or(QuinnOwnedStandbyRegistrationErrorKind::MissingStandby)?;
        if owner.failure.is_some() {
            return Err(QuinnOwnedStandbyRegistrationErrorKind::StandbyFailed);
        }
        let Some(state) = owner.state.take() else {
            owner
                .failure
                .get_or_insert(QuinnStandbyFailureKind::ProtocolInvariant);
            return Err(QuinnOwnedStandbyRegistrationErrorKind::CompositionInvariant);
        };
        let QuinnRegisteredStandbyState::RegistrationPending(pending) = state else {
            owner.state = Some(state);
            return if matches!(
                owner.state,
                Some(QuinnRegisteredStandbyState::AwaitingRegistration(_))
            ) {
                Ok(QuinnOwnedStandbyRegistrationDrive::AlreadyQueued)
            } else {
                Err(QuinnOwnedStandbyRegistrationErrorKind::WrongState)
            };
        };
        let gate = QuinnLegCompositionGate::mint();
        let Some(QuinnStandbyActorOwner::Running(actor)) = owner.actor.as_mut() else {
            owner.state = Some(QuinnRegisteredStandbyState::RegistrationPending(pending));
            return Err(QuinnOwnedStandbyRegistrationErrorKind::StandbyNotRunning);
        };
        match actor.with_controller_parts_mut_input(&gate, pending, |pending, _io, queue| {
            pending.enqueue(queue).map_err(Box::new)
        }) {
            Ok(Ok(awaiting)) => {
                owner.state = Some(QuinnRegisteredStandbyState::AwaitingRegistration(awaiting));
                Ok(QuinnOwnedStandbyRegistrationDrive::Queued)
            }
            Ok(Err(failure)) => {
                let failure = *failure;
                let classification = classify_leg_control_enqueue(failure.kind());
                owner.state = Some(QuinnRegisteredStandbyState::RegistrationPending(
                    failure.into_pending(),
                ));
                match classification {
                    Ok(()) => Ok(QuinnOwnedStandbyRegistrationDrive::Pressure),
                    Err(kind) => {
                        owner.failure.get_or_insert(standby_failure_for_queue(kind));
                        Err(QuinnOwnedStandbyRegistrationErrorKind::Enqueue(kind))
                    }
                }
            }
            Err((pending, _error)) => {
                owner.state = Some(QuinnRegisteredStandbyState::RegistrationPending(pending));
                owner
                    .failure
                    .get_or_insert(QuinnStandbyFailureKind::ActorTransport);
                Err(QuinnOwnedStandbyRegistrationErrorKind::ActorTransportUnavailable)
            }
        }
    }

    /// Begins or retries the one exact registered-B probe. Queue pressure
    /// retains the pre-encoded probe and B capability inside the composition.
    pub(crate) fn drive_registered_standby_probe(
        &mut self,
        now: SimTime,
    ) -> Result<QuinnStandbyProbeDrive, QuinnStandbyProbeError> {
        if !self.current_policy_is_active() {
            return Err(QuinnStandbyProbeError::SessionShuttingDown);
        }
        let owner = self
            .standby
            .as_mut()
            .ok_or(QuinnStandbyProbeError::MissingStandby)?;
        if owner.failure.is_some() {
            return Err(QuinnStandbyProbeError::StandbyFailed);
        }
        if !matches!(
            owner.actor.as_ref(),
            Some(QuinnStandbyActorOwner::Running(_))
        ) {
            return Err(QuinnStandbyProbeError::WrongState);
        }
        let Some(state) = owner.state.take() else {
            owner
                .failure
                .get_or_insert(QuinnStandbyFailureKind::ProtocolInvariant);
            return Err(QuinnStandbyProbeError::CompositionInvariant);
        };
        let state = match state {
            QuinnRegisteredStandbyState::Fresh(fresh) if !fresh.is_fresh_at(now) => {
                QuinnRegisteredStandbyState::Registered(fresh.into_registered())
            }
            QuinnRegisteredStandbyState::Fresh(fresh) => {
                owner.state = Some(QuinnRegisteredStandbyState::Fresh(fresh));
                return Ok(QuinnStandbyProbeDrive::AlreadyFresh);
            }
            state => state,
        };
        let pending = match state {
            QuinnRegisteredStandbyState::Registered(registered) => {
                let sequence = match owner.next_probe_sequence {
                    Some(sequence) => sequence,
                    None => {
                        owner.state = Some(QuinnRegisteredStandbyState::Registered(registered));
                        owner
                            .failure
                            .get_or_insert(QuinnStandbyFailureKind::ProtocolInvariant);
                        return Err(QuinnStandbyProbeError::SequenceExhausted);
                    }
                };
                match registered.begin_exact_probe(sequence) {
                    Ok(pending) => {
                        owner.next_probe_sequence = sequence.checked_next().ok();
                        owner.current_probe_sequence = Some(sequence);
                        pending
                    }
                    Err(failure) => {
                        let kind = failure.kind();
                        owner.state = Some(QuinnRegisteredStandbyState::Registered(
                            failure.into_registered(),
                        ));
                        owner.failure.get_or_insert(match kind {
                            RegisteredStandbyProbeBeginErrorKind::EndpointOrQueueLost => {
                                QuinnStandbyFailureKind::QueueOrEndpointLost
                            }
                            RegisteredStandbyProbeBeginErrorKind::Rejected => {
                                QuinnStandbyFailureKind::ProtocolInvariant
                            }
                        });
                        return Err(QuinnStandbyProbeError::Begin(kind));
                    }
                }
            }
            QuinnRegisteredStandbyState::ProbePending(pending) => pending,
            state => {
                owner.state = Some(state);
                return Err(QuinnStandbyProbeError::WrongState);
            }
        };
        let gate = QuinnLegCompositionGate::mint();
        let Some(QuinnStandbyActorOwner::Running(actor)) = owner.actor.as_mut() else {
            owner.state = Some(QuinnRegisteredStandbyState::ProbePending(pending));
            return Err(QuinnStandbyProbeError::WrongState);
        };
        match actor.with_controller_parts_mut_input(&gate, pending, |pending, _io, queue| {
            pending.enqueue(queue).map_err(Box::new)
        }) {
            Ok(Ok(awaiting)) => {
                owner.state = Some(QuinnRegisteredStandbyState::AwaitingProbe(awaiting));
                Ok(QuinnStandbyProbeDrive::Queued)
            }
            Ok(Err(failure)) => {
                let failure = *failure;
                let classification = classify_leg_control_enqueue(failure.kind());
                owner.state = Some(QuinnRegisteredStandbyState::ProbePending(
                    failure.into_pending(),
                ));
                match classification {
                    Ok(()) => Ok(QuinnStandbyProbeDrive::Pressure),
                    Err(kind) => {
                        owner.failure.get_or_insert(standby_failure_for_queue(kind));
                        Err(QuinnStandbyProbeError::Enqueue(kind))
                    }
                }
            }
            Err((pending, error)) => {
                owner.state = Some(QuinnRegisteredStandbyState::ProbePending(pending));
                owner
                    .failure
                    .get_or_insert(QuinnStandbyFailureKind::ActorTransport);
                Err(QuinnStandbyProbeError::Actor(error))
            }
        }
    }

    pub(crate) fn try_flush_registered_standby(
        &mut self,
        now: SimTime,
    ) -> Result<
        crate::owned_upstream::quinn_leg_actor::QuinnLegActorFlushDisposition,
        QuinnStandbyProbeError,
    > {
        if self.phase != QuinnClientCompositionPhase::Running {
            return Err(QuinnStandbyProbeError::SessionShuttingDown);
        }
        let owner = self
            .standby
            .as_mut()
            .ok_or(QuinnStandbyProbeError::MissingStandby)?;
        if owner.failure.is_some() {
            return Err(QuinnStandbyProbeError::StandbyFailed);
        }
        let Some(QuinnStandbyActorOwner::Running(actor)) = owner.actor.as_mut() else {
            return Err(QuinnStandbyProbeError::WrongState);
        };
        match actor.try_flush(now) {
            Ok(disposition) => Ok(disposition),
            Err(error) => {
                owner
                    .failure
                    .get_or_insert(QuinnStandbyFailureKind::ActorTransport);
                Err(QuinnStandbyProbeError::Actor(error))
            }
        }
    }

    /// Starts B's liveness clock only from the exact positive writer receipt.
    /// The timeout duration and resulting absolute deadline are both owned by
    /// the checked composition policy.
    pub(crate) fn arm_registered_standby_probe(
        &mut self,
        now: SimTime,
    ) -> Result<QuinnStandbyProbeArmDrive, QuinnStandbyProbeArmError> {
        if self.phase != QuinnClientCompositionPhase::Running {
            return Err(QuinnStandbyProbeArmError::SessionShuttingDown);
        }
        let owner = self
            .standby
            .as_mut()
            .ok_or(QuinnStandbyProbeArmError::MissingStandby)?;
        if owner.failure.is_some() {
            return Err(QuinnStandbyProbeArmError::StandbyFailed);
        }
        let Some(state) = owner.state.take() else {
            owner
                .failure
                .get_or_insert(QuinnStandbyFailureKind::ProtocolInvariant);
            return Err(QuinnStandbyProbeArmError::CompositionInvariant);
        };
        match state {
            QuinnRegisteredStandbyState::AwaitingProbe(awaiting) => {
                let Some(sequence) = owner.current_probe_sequence else {
                    owner.state = Some(QuinnRegisteredStandbyState::AwaitingProbe(awaiting));
                    owner
                        .failure
                        .get_or_insert(QuinnStandbyFailureKind::ProtocolInvariant);
                    return Err(QuinnStandbyProbeArmError::CompositionInvariant);
                };
                match awaiting.arm_after_delivery(now, owner.liveness.probe_timeout) {
                    Ok(armed) => {
                        let deadline = armed.deadline();
                        let recovery_key =
                            RootStandbyBRecoveryKey::mint(owner.admitted_generation, sequence);
                        owner.state = Some(QuinnRegisteredStandbyState::ArmedProbe(
                            QuinnArmedRegisteredStandbyProbe::new(armed, recovery_key.clone()),
                        ));
                        Ok(QuinnStandbyProbeArmDrive::Armed {
                            deadline,
                            recovery_key,
                        })
                    }
                    Err(failure) => {
                        let kind = failure.kind();
                        owner.state = Some(QuinnRegisteredStandbyState::AwaitingProbe(
                            failure.into_awaiting(),
                        ));
                        match kind {
                            RegisteredStandbyProbeArmErrorKind::NotDelivered => {
                                Ok(QuinnStandbyProbeArmDrive::NotDelivered)
                            }
                            RegisteredStandbyProbeArmErrorKind::Deadline(_) => {
                                Err(QuinnStandbyProbeArmError::Arm(kind))
                            }
                            RegisteredStandbyProbeArmErrorKind::QueueOrEndpointLost => {
                                owner
                                    .failure
                                    .get_or_insert(QuinnStandbyFailureKind::QueueOrEndpointLost);
                                Err(QuinnStandbyProbeArmError::Arm(kind))
                            }
                            RegisteredStandbyProbeArmErrorKind::InvalidTimeout => {
                                owner
                                    .failure
                                    .get_or_insert(QuinnStandbyFailureKind::ProtocolInvariant);
                                Err(QuinnStandbyProbeArmError::Arm(kind))
                            }
                        }
                    }
                }
            }
            QuinnRegisteredStandbyState::ArmedProbe(armed) => {
                let deadline = armed.deadline();
                owner.state = Some(QuinnRegisteredStandbyState::ArmedProbe(armed));
                Ok(QuinnStandbyProbeArmDrive::AlreadyArmed { deadline })
            }
            state => {
                owner.state = Some(state);
                Err(QuinnStandbyProbeArmError::WrongState)
            }
        }
    }

    /// Arbitrates one internally retained armed-B expiry against the actor's
    /// immutable arrival boundary. A queued or in-flight record admitted at
    /// or before the deadline is restored for normal B classification first;
    /// post-deadline traffic cannot extend the absolute timer.
    pub(crate) fn try_expire_registered_standby_probe(
        &mut self,
        now: SimTime,
        recovery_key: &RootStandbyBRecoveryKey,
    ) -> Result<QuinnStandbyProbeExpiryDrive, QuinnStandbyProbeExpiryError> {
        if self.phase != QuinnClientCompositionPhase::Running {
            return Err(QuinnStandbyProbeExpiryError::SessionShuttingDown);
        }
        let owner = self
            .standby
            .as_mut()
            .ok_or(QuinnStandbyProbeExpiryError::MissingStandby)?;
        if owner.failure.is_some() {
            return Err(QuinnStandbyProbeExpiryError::StandbyFailed);
        }
        if let Some(QuinnRegisteredStandbyState::ArmedProbe(armed)) = owner.state.as_ref()
            && armed.recovery_key() != recovery_key
        {
            return Err(QuinnStandbyProbeExpiryError::RecoveryKeyMismatch);
        }
        let Some(state) = owner.state.take() else {
            owner
                .failure
                .get_or_insert(QuinnStandbyFailureKind::ProtocolInvariant);
            return Err(QuinnStandbyProbeExpiryError::CompositionInvariant);
        };
        let QuinnRegisteredStandbyState::ArmedProbe(armed) = state else {
            owner.state = Some(state);
            return Ok(QuinnStandbyProbeExpiryDrive::Inert);
        };
        let deadline = armed.deadline();
        if now < deadline {
            owner.state = Some(QuinnRegisteredStandbyState::ArmedProbe(armed));
            return Ok(QuinnStandbyProbeExpiryDrive::Pending { deadline });
        }
        let Some(QuinnStandbyActorOwner::Running(actor)) = owner.actor.as_mut() else {
            owner.state = Some(QuinnRegisteredStandbyState::ArmedProbe(armed));
            owner
                .failure
                .get_or_insert(QuinnStandbyFailureKind::ActorTransport);
            return Err(QuinnStandbyProbeExpiryError::StandbyFailed);
        };
        let gate = QuinnLegCompositionGate::mint();
        let arbitration = actor.try_transact_inbound_before_semantic_timer(
            &gate,
            armed,
            &mut (),
            |_context, _runtime, envelope, timer| {
                Ok((QuinnLegActorInboundDecision::Blocked(envelope), timer))
            },
            |_context,
             _runtime,
             timer|
             -> QuinnLegActorTimerDecision<
                QuinnArmedRegisteredStandbyProbe,
                ClientRegisteredStandby,
                std::convert::Infallible,
            > {
                let QuinnArmedRegisteredStandbyProbe {
                    armed,
                    recovery_key,
                    inbound_publication_cutoff,
                } = timer;
                match armed.expire_if_due(now) {
                    Ok(registered) => QuinnLegActorTimerDecision::Committed(registered),
                    Err(armed) => {
                        QuinnLegActorTimerDecision::Rejected(QuinnArmedRegisteredStandbyProbe {
                            armed,
                            recovery_key,
                            inbound_publication_cutoff,
                        })
                    }
                }
            },
        );
        match arbitration {
            QuinnLegActorInboundTimerPoll::Inbound { poll, timer } => {
                owner.state = Some(QuinnRegisteredStandbyState::ArmedProbe(timer));
                match poll {
                    QuinnLegActorInboundPoll::Blocked => {
                        Ok(QuinnStandbyProbeExpiryDrive::InboundPending { deadline })
                    }
                    QuinnLegActorInboundPoll::Idle | QuinnLegActorInboundPoll::Classified(()) => {
                        owner
                            .failure
                            .get_or_insert(QuinnStandbyFailureKind::ProtocolInvariant);
                        Err(QuinnStandbyProbeExpiryError::CompositionInvariant)
                    }
                }
            }
            QuinnLegActorInboundTimerPoll::PublicationPending(timer) => {
                owner.state = Some(QuinnRegisteredStandbyState::ArmedProbe(timer));
                Ok(QuinnStandbyProbeExpiryDrive::PublicationPending { deadline })
            }
            QuinnLegActorInboundTimerPoll::TimerCommitted(registered) => {
                owner.state = Some(QuinnRegisteredStandbyState::Registered(registered));
                Ok(QuinnStandbyProbeExpiryDrive::Expired { deadline })
            }
            QuinnLegActorInboundTimerPoll::TimerRejected(timer) => {
                owner.state = Some(QuinnRegisteredStandbyState::ArmedProbe(timer));
                Ok(QuinnStandbyProbeExpiryDrive::Pending { deadline })
            }
            QuinnLegActorInboundTimerPoll::TimerInvariant { error, .. } => match error {},
            QuinnLegActorInboundTimerPoll::Suppressed { timer, .. }
            | QuinnLegActorInboundTimerPoll::Failed { timer, .. }
            | QuinnLegActorInboundTimerPoll::LocalShutdown(timer) => {
                owner.state = Some(QuinnRegisteredStandbyState::ArmedProbe(timer));
                owner
                    .failure
                    .get_or_insert(QuinnStandbyFailureKind::ActorTransport);
                Err(QuinnStandbyProbeExpiryError::StandbyFailed)
            }
        }
    }

    /// Consumes at most one B record without awaiting or exposing actor,
    /// decoded-inbound, or tracked byte ownership. Classification and the B
    /// typestate commit happen under the actor's exact first-failure lock.
    pub(crate) fn try_receive_registered_standby(&mut self) -> QuinnStandbyInboundDrive {
        if self.phase != QuinnClientCompositionPhase::Running {
            return QuinnStandbyInboundDrive::SessionShuttingDown;
        }
        let physical_attach = &mut self.physical_attach;
        let Some(owner) = self.standby.as_mut() else {
            return QuinnStandbyInboundDrive::MissingStandby;
        };
        if owner.failure.is_some() {
            return QuinnStandbyInboundDrive::StandbyFailed;
        }
        let gate = QuinnLegCompositionGate::mint();
        let QuinnRegisteredStandbyOwner {
            actor,
            state,
            liveness,
            features,
            failure: failure_authority,
            probe_responder,
            pending_control,
            ..
        } = owner;
        let Some(QuinnStandbyActorOwner::Running(actor)) = actor.as_mut() else {
            return QuinnStandbyInboundDrive::WrongState;
        };
        if let Some(QuinnRegisteredStandbyState::AcceptedPendingCommit { generation }) =
            state.as_ref()
        {
            let exact_pending = matches!(
                physical_attach.as_ref(),
                Some(QuinnPhysicalAttachOwner {
                    stage: PhysicalPendingAttachStage::Accepted,
                    authority: QuinnPhysicalAttachAuthority::Accepted(held),
                    stored_pending_frame_bytes: 0,
                }) if matches!(
                    held.value(),
                    QuinnStandbyHeldControl::AttachAccepted { accepted, .. }
                        if accepted.generation() == *generation
                )
            );
            if exact_pending {
                return QuinnStandbyInboundDrive::AttachAcceptedPendingCommit {
                    generation: *generation,
                };
            }
            failure_authority.get_or_insert(QuinnStandbyFailureKind::ProtocolInvariant);
            return QuinnStandbyInboundDrive::CompositionInvariant;
        }

        if let Some(pending) = pending_control.take() {
            match pending {
                QuinnStandbyPendingControl::SwitchHint(held) => {
                    let metadata = held_switch_hint_metadata(&held);
                    *pending_control = Some(QuinnStandbyPendingControl::SwitchHint(held));
                    return QuinnStandbyInboundDrive::SwitchHintPending {
                        arrived_at: metadata.arrived_at(),
                        publication_epoch: metadata.publication_epoch(),
                        send_ordinal: metadata.send_ordinal(),
                        encoded_bytes: metadata.encoded_bytes(),
                    };
                }
                QuinnStandbyPendingControl::ProbeAck(held) => {
                    return match commit_held_standby_probe_ack(actor, &gate, held) {
                        Ok(probe_sequence) => {
                            QuinnStandbyInboundDrive::ProbeAckQueued { probe_sequence }
                        }
                        Err((held, QuinnStandbyProbeAckCommitError::Queue(kind))) => {
                            let probe_sequence = match held.value() {
                                QuinnStandbyHeldControl::ProbeAck(pending) => {
                                    pending.probe_sequence()
                                }
                                QuinnStandbyHeldControl::SwitchHint(_)
                                | QuinnStandbyHeldControl::AttachAccepted { .. } => {
                                    failure_authority
                                        .get_or_insert(QuinnStandbyFailureKind::ProtocolInvariant);
                                    *pending_control =
                                        Some(QuinnStandbyPendingControl::ProbeAck(held));
                                    return QuinnStandbyInboundDrive::CompositionInvariant;
                                }
                            };
                            *pending_control = Some(QuinnStandbyPendingControl::ProbeAck(held));
                            if matches!(kind, LegControlQueueErrorKind::CapacityExceeded { .. }) {
                                QuinnStandbyInboundDrive::ProbeAckPressure { probe_sequence }
                            } else {
                                if let Err(failure) = classify_leg_control_enqueue(&kind) {
                                    failure_authority
                                        .get_or_insert(standby_failure_for_queue(failure));
                                } else {
                                    failure_authority
                                        .get_or_insert(QuinnStandbyFailureKind::ProtocolInvariant);
                                }
                                QuinnStandbyInboundDrive::ProbeAckRejected(kind)
                            }
                        }
                        Err((held, QuinnStandbyProbeAckCommitError::Actor(error))) => {
                            *pending_control = Some(QuinnStandbyPendingControl::ProbeAck(held));
                            failure_authority
                                .get_or_insert(QuinnStandbyFailureKind::ActorTransport);
                            QuinnStandbyInboundDrive::Actor(error)
                        }
                        Err((held, QuinnStandbyProbeAckCommitError::WrongControl)) => {
                            *pending_control = Some(QuinnStandbyPendingControl::ProbeAck(held));
                            failure_authority
                                .get_or_insert(QuinnStandbyFailureKind::ProtocolInvariant);
                            QuinnStandbyInboundDrive::CompositionInvariant
                        }
                    };
                }
            }
        }
        // Move the exact physical permit beside this one actor receive turn.
        // Any outcome other than the correlated acceptance/status restores it
        // below before returning; no await or external callback can observe
        // the capacity-one slot empty.
        let mut physical_accept_permit = if matches!(
            state.as_ref(),
            Some(QuinnRegisteredStandbyState::AwaitingAttach(_))
        ) {
            match physical_attach.as_ref() {
                None => None,
                Some(QuinnPhysicalAttachOwner {
                    stage: PhysicalPendingAttachStage::AwaitingAttach,
                    authority: QuinnPhysicalAttachAuthority::Pending(_),
                    stored_pending_frame_bytes: 0,
                }) => match physical_attach.take() {
                    Some(QuinnPhysicalAttachOwner {
                        stage: PhysicalPendingAttachStage::AwaitingAttach,
                        authority: QuinnPhysicalAttachAuthority::Pending(permit),
                        stored_pending_frame_bytes: 0,
                    }) => Some(permit),
                    other => {
                        *physical_attach = other;
                        failure_authority.get_or_insert(QuinnStandbyFailureKind::ProtocolInvariant);
                        return QuinnStandbyInboundDrive::CompositionInvariant;
                    }
                },
                Some(_) => {
                    failure_authority.get_or_insert(QuinnStandbyFailureKind::ProtocolInvariant);
                    return QuinnStandbyInboundDrive::CompositionInvariant;
                }
            }
        } else {
            None
        };
        let transaction = actor.try_classify_or_hold_composition_inbound(
            &gate,
            *features,
            |inbound, arrived_at, _queue| {
                let inbound = if let Some(responder) = probe_responder.as_ref() {
                    match responder.classify(inbound) {
                        ExactLegProbeResponse::Pending(pending) => {
                            return Ok(QuinnLegActorCompositionInboundDecision::Hold(
                                QuinnStandbyHeldControl::ProbeAck(pending),
                            ));
                        }
                        ExactLegProbeResponse::Unhandled(inbound) => inbound,
                    }
                } else {
                    inbound
                };
                if probe_responder.is_some()
                    && matches!(
                        &inbound,
                        BoundInbound::LegControl(bound)
                            if matches!(bound.parts().1.record(), LegControlRecord::SwitchHint { .. })
                    )
                {
                    return Ok(QuinnLegActorCompositionInboundDecision::Hold(
                        QuinnStandbyHeldControl::SwitchHint(inbound),
                    ));
                }
                let Some(owned_state) = state.take() else {
                    failure_authority.get_or_insert(QuinnStandbyFailureKind::ProtocolInvariant);
                    drop(inbound);
                    return Ok(QuinnLegActorCompositionInboundDecision::Classified(
                        QuinnStandbyInboundDrive::CompositionInvariant,
                    ));
                };
                let outcome = match owned_state {
                    QuinnRegisteredStandbyState::AwaitingRegistration(awaiting) => {
                        match awaiting.validate(inbound) {
                            Ok(registered) => {
                                if registered.installed_features() != *features {
                                    failure_authority
                                        .get_or_insert(QuinnStandbyFailureKind::ProtocolInvariant);
                                    *state =
                                        Some(QuinnRegisteredStandbyState::Registered(registered));
                                    return Ok(QuinnLegActorCompositionInboundDecision::Classified(
                                        QuinnStandbyInboundDrive::CompositionInvariant,
                                    ));
                                }
                                *probe_responder =
                                    Some(registered.exact_leg_probe_responder());
                                *state = Some(QuinnRegisteredStandbyState::Registered(registered));
                                QuinnStandbyInboundDrive::RegistrationAccepted
                            }
                            Err(failure) => {
                                let kind = failure.kind();
                                *state = Some(QuinnRegisteredStandbyState::AwaitingRegistration(
                                    failure.into_awaiting(),
                                ));
                                if kind == StandbyAcceptanceErrorKind::QueueLost {
                                    failure_authority.get_or_insert(
                                        QuinnStandbyFailureKind::QueueOrEndpointLost,
                                    );
                                }
                                QuinnStandbyInboundDrive::RejectedRegistration(kind)
                            }
                        }
                    }
                    QuinnRegisteredStandbyState::AwaitingProbe(awaiting) => {
                        let fresh_until = match arrived_at.checked_add(liveness.freshness_window) {
                            Ok(fresh_until) => fresh_until,
                            Err(error) => {
                                *state = Some(QuinnRegisteredStandbyState::AwaitingProbe(awaiting));
                                drop(inbound);
                                return Ok(QuinnLegActorCompositionInboundDecision::Classified(
                                    QuinnStandbyInboundDrive::Time(error),
                                ));
                            }
                        };
                        match awaiting.validate_exact_ack(inbound, arrived_at, fresh_until) {
                            Ok(fresh) => {
                                *state = Some(QuinnRegisteredStandbyState::Fresh(fresh));
                                QuinnStandbyInboundDrive::ProbeFresh {
                                    fresh_until,
                                    recovery_key: None,
                                }
                            }
                            Err(failure) => {
                                let kind = failure.kind();
                                let (awaiting, inbound) = failure.into_parts();
                                *state = Some(QuinnRegisteredStandbyState::AwaitingProbe(awaiting));
                                if kind == RegisteredStandbyProbeAckErrorKind::QueueOrEndpointLost {
                                    failure_authority.get_or_insert(
                                        QuinnStandbyFailureKind::QueueOrEndpointLost,
                                    );
                                }
                                drop(inbound);
                                QuinnStandbyInboundDrive::RejectedProbe(kind)
                            }
                        }
                    }
                    QuinnRegisteredStandbyState::ArmedProbe(armed) => {
                        let QuinnArmedRegisteredStandbyProbe {
                            armed,
                            recovery_key,
                            inbound_publication_cutoff,
                        } = armed;
                        let fresh_until = match arrived_at.checked_add(liveness.freshness_window) {
                            Ok(fresh_until) => fresh_until,
                            Err(error) => {
                                *state = Some(QuinnRegisteredStandbyState::ArmedProbe(
                                    QuinnArmedRegisteredStandbyProbe {
                                        armed,
                                        recovery_key,
                                        inbound_publication_cutoff,
                                    },
                                ));
                                drop(inbound);
                                return Ok(QuinnLegActorCompositionInboundDecision::Classified(
                                    QuinnStandbyInboundDrive::Time(error),
                                ));
                            }
                        };
                        match armed.validate_exact_ack(inbound, arrived_at, fresh_until) {
                            Ok(fresh) => {
                                *state = Some(QuinnRegisteredStandbyState::Fresh(fresh));
                                QuinnStandbyInboundDrive::ProbeFresh {
                                    fresh_until,
                                    recovery_key: Some(recovery_key),
                                }
                            }
                            Err(failure) => {
                                let kind = failure.kind();
                                let (armed, inbound) = failure.into_parts();
                                if kind == RegisteredStandbyProbeAckErrorKind::DeadlineExpired {
                                    match armed.expire_if_due(arrived_at) {
                                        Ok(registered) => {
                                            *state = Some(QuinnRegisteredStandbyState::Registered(
                                                registered,
                                            ));
                                        }
                                        Err(armed) => {
                                            *state = Some(QuinnRegisteredStandbyState::ArmedProbe(
                                                QuinnArmedRegisteredStandbyProbe {
                                                    armed,
                                                    recovery_key,
                                                    inbound_publication_cutoff,
                                                },
                                            ));
                                            failure_authority.get_or_insert(
                                                QuinnStandbyFailureKind::ProtocolInvariant,
                                            );
                                            drop(inbound);
                                            return Ok(
                                                QuinnLegActorCompositionInboundDecision::Classified(
                                                    QuinnStandbyInboundDrive::CompositionInvariant,
                                                ),
                                            );
                                        }
                                    }
                                } else {
                                    *state = Some(QuinnRegisteredStandbyState::ArmedProbe(
                                        QuinnArmedRegisteredStandbyProbe {
                                            armed,
                                            recovery_key,
                                            inbound_publication_cutoff,
                                        },
                                    ));
                                    if kind
                                        == RegisteredStandbyProbeAckErrorKind::QueueOrEndpointLost
                                    {
                                        failure_authority.get_or_insert(
                                            QuinnStandbyFailureKind::QueueOrEndpointLost,
                                        );
                                    }
                                }
                                drop(inbound);
                                QuinnStandbyInboundDrive::RejectedProbe(kind)
                            }
                        }
                    }
                    QuinnRegisteredStandbyState::AwaitingAttach(awaiting) => {
                        match awaiting.validate_response(inbound) {
                            Ok(RegisteredAttachResponse::Accepted(accepted)) => {
                                let generation = accepted.generation();
                                if let Some(permit) = physical_accept_permit.take() {
                                    let policy = permit.into_correlated_accepted();
                                    *state = Some(
                                        QuinnRegisteredStandbyState::AcceptedPendingCommit {
                                            generation,
                                        },
                                    );
                                    return Ok(QuinnLegActorCompositionInboundDecision::Hold(
                                        QuinnStandbyHeldControl::AttachAccepted {
                                            accepted,
                                            policy,
                                        },
                                    ));
                                }
                                *state = Some(QuinnRegisteredStandbyState::Accepted(accepted));
                                QuinnStandbyInboundDrive::AttachAccepted { generation }
                            }
                            Ok(RegisteredAttachResponse::GenerationStatus(status)) => {
                                *state =
                                    Some(QuinnRegisteredStandbyState::GenerationStatus(status));
                                if let Some(permit) = physical_accept_permit.take() {
                                    *physical_attach = Some(QuinnPhysicalAttachOwner {
                                        stage: PhysicalPendingAttachStage::GenerationStatus,
                                        authority: QuinnPhysicalAttachAuthority::Pending(permit),
                                        stored_pending_frame_bytes: 0,
                                    });
                                } else if advance_pending_physical_attach_stage(
                                    physical_attach,
                                    PhysicalPendingAttachStage::GenerationStatus,
                                )
                                .is_err()
                                {
                                    failure_authority
                                        .get_or_insert(QuinnStandbyFailureKind::ProtocolInvariant);
                                    return Ok(
                                        QuinnLegActorCompositionInboundDecision::Classified(
                                            QuinnStandbyInboundDrive::CompositionInvariant,
                                        ),
                                    );
                                }
                                QuinnStandbyInboundDrive::AttachGenerationStatus
                            }
                            Err(failure) => {
                                let kind = failure.kind();
                                let (awaiting, inbound) = failure.into_parts();
                                *state =
                                    Some(QuinnRegisteredStandbyState::AwaitingAttach(awaiting));
                                if kind == RegisteredAttachResponseErrorKind::QueueLost {
                                    failure_authority.get_or_insert(
                                        QuinnStandbyFailureKind::QueueOrEndpointLost,
                                    );
                                }
                                drop(inbound);
                                QuinnStandbyInboundDrive::RejectedAttach(kind)
                            }
                        }
                    }
                    owned_state => {
                        *state = Some(owned_state);
                        drop(inbound);
                        QuinnStandbyInboundDrive::WrongState
                    }
                };
                Ok(QuinnLegActorCompositionInboundDecision::Classified(outcome))
            },
        );
        if let Some(permit) = physical_accept_permit.take() {
            *physical_attach = Some(QuinnPhysicalAttachOwner {
                stage: PhysicalPendingAttachStage::AwaitingAttach,
                authority: QuinnPhysicalAttachAuthority::Pending(permit),
                stored_pending_frame_bytes: 0,
            });
        }
        match transaction {
            Ok(QuinnLegActorCompositionInboundPoll::Idle) => QuinnStandbyInboundDrive::Idle,
            Ok(QuinnLegActorCompositionInboundPoll::Classified(outcome)) => outcome,
            Ok(QuinnLegActorCompositionInboundPoll::Held(held)) => match held.value() {
                QuinnStandbyHeldControl::ProbeAck(pending) => {
                    let probe_sequence = pending.probe_sequence();
                    *pending_control = Some(QuinnStandbyPendingControl::ProbeAck(held));
                    match pending_control.take() {
                        Some(QuinnStandbyPendingControl::ProbeAck(held)) => {
                            match commit_held_standby_probe_ack(actor, &gate, held) {
                                Ok(probe_sequence) => {
                                    QuinnStandbyInboundDrive::ProbeAckQueued { probe_sequence }
                                }
                                Err((held, QuinnStandbyProbeAckCommitError::Queue(kind))) => {
                                    *pending_control =
                                        Some(QuinnStandbyPendingControl::ProbeAck(held));
                                    if matches!(
                                        kind,
                                        LegControlQueueErrorKind::CapacityExceeded { .. }
                                    ) {
                                        QuinnStandbyInboundDrive::ProbeAckPressure {
                                            probe_sequence,
                                        }
                                    } else {
                                        if let Err(failure) = classify_leg_control_enqueue(&kind) {
                                            failure_authority
                                                .get_or_insert(standby_failure_for_queue(failure));
                                        } else {
                                            failure_authority.get_or_insert(
                                                QuinnStandbyFailureKind::ProtocolInvariant,
                                            );
                                        }
                                        QuinnStandbyInboundDrive::ProbeAckRejected(kind)
                                    }
                                }
                                Err((held, QuinnStandbyProbeAckCommitError::Actor(error))) => {
                                    *pending_control =
                                        Some(QuinnStandbyPendingControl::ProbeAck(held));
                                    failure_authority
                                        .get_or_insert(QuinnStandbyFailureKind::ActorTransport);
                                    QuinnStandbyInboundDrive::Actor(error)
                                }
                                Err((held, QuinnStandbyProbeAckCommitError::WrongControl)) => {
                                    *pending_control =
                                        Some(QuinnStandbyPendingControl::ProbeAck(held));
                                    failure_authority
                                        .get_or_insert(QuinnStandbyFailureKind::ProtocolInvariant);
                                    QuinnStandbyInboundDrive::CompositionInvariant
                                }
                            }
                        }
                        _ => {
                            failure_authority
                                .get_or_insert(QuinnStandbyFailureKind::ProtocolInvariant);
                            QuinnStandbyInboundDrive::CompositionInvariant
                        }
                    }
                }
                QuinnStandbyHeldControl::SwitchHint(_) => {
                    let metadata = held_switch_hint_metadata(&held);
                    *pending_control = Some(QuinnStandbyPendingControl::SwitchHint(held));
                    QuinnStandbyInboundDrive::SwitchHintPending {
                        arrived_at: metadata.arrived_at(),
                        publication_epoch: metadata.publication_epoch(),
                        send_ordinal: metadata.send_ordinal(),
                        encoded_bytes: metadata.encoded_bytes(),
                    }
                }
                QuinnStandbyHeldControl::AttachAccepted { accepted, .. } => {
                    let generation = accepted.generation();
                    *physical_attach = Some(QuinnPhysicalAttachOwner {
                        stage: PhysicalPendingAttachStage::Accepted,
                        authority: QuinnPhysicalAttachAuthority::Accepted(held),
                        stored_pending_frame_bytes: 0,
                    });
                    QuinnStandbyInboundDrive::AttachAcceptedPendingCommit { generation }
                }
            },
            Err(error) => {
                owner
                    .failure
                    .get_or_insert(QuinnStandbyFailureKind::ActorTransport);
                QuinnStandbyInboundDrive::Actor(error)
            }
        }
    }

    /// Synchronously presents the one held physical B hint without releasing
    /// its raw actor record. A validator/arbiter root must return `Settled`
    /// for either acceptance or a final typed rejection; only retryable
    /// pressure/blocking may return `Retain`. The latter restores the exact
    /// same arrival, publication epoch, ordinal, bytes, and delivery ACK.
    pub(crate) fn try_transact_pending_switch_hint<S, B>(
        &mut self,
        transact: impl FnOnce(
            &QuinnStandbyPhysicalSwitchHint<'_>,
        ) -> QuinnStandbySwitchHintDecision<S, B>,
    ) -> QuinnStandbySwitchHintTransaction<S, B> {
        if self.phase != QuinnClientCompositionPhase::Running {
            return QuinnStandbySwitchHintTransaction::SessionShuttingDown;
        }
        let Some(owner) = self.standby.as_mut() else {
            return QuinnStandbySwitchHintTransaction::MissingStandby;
        };
        if owner.failure.is_some() {
            return QuinnStandbySwitchHintTransaction::StandbyFailed;
        }
        let Some(pending) = owner.pending_control.take() else {
            return QuinnStandbySwitchHintTransaction::NoPendingHint;
        };
        let held = match pending {
            QuinnStandbyPendingControl::ProbeAck(held) => {
                owner.pending_control = Some(QuinnStandbyPendingControl::ProbeAck(held));
                return QuinnStandbySwitchHintTransaction::ProbeAckPending;
            }
            QuinnStandbyPendingControl::SwitchHint(held) => held,
        };
        let metadata = held_switch_hint_metadata(&held);
        let gate = QuinnLegCompositionGate::mint();
        let bound = match held.value() {
            QuinnStandbyHeldControl::SwitchHint(BoundInbound::LegControl(bound)) => bound,
            QuinnStandbyHeldControl::ProbeAck(_)
            | QuinnStandbyHeldControl::SwitchHint(BoundInbound::Session(_)) => {
                drop(held.commit());
                return QuinnStandbySwitchHintTransaction::PhysicalRejected {
                    kind: QuinnStandbyPhysicalSwitchHintReject::WrongRecordKind,
                    metadata,
                };
            }
            QuinnStandbyHeldControl::AttachAccepted { .. } => {
                owner.pending_control = Some(QuinnStandbyPendingControl::SwitchHint(held));
                owner
                    .failure
                    .get_or_insert(QuinnStandbyFailureKind::ProtocolInvariant);
                return QuinnStandbySwitchHintTransaction::CompositionInvariant;
            }
        };
        let physical_evidence = match owner.state.as_ref() {
            Some(QuinnRegisteredStandbyState::Fresh(fresh)) => fresh
                .validate_exact_switch_hint(
                    bound,
                    metadata.arrived_at(),
                    owner.admitted_generation,
                    owner.features,
                )
                .map_err(QuinnStandbyPhysicalSwitchHintReject::from),
            _ => Err(QuinnStandbyPhysicalSwitchHintReject::ProbeNotFresh),
        };
        if let Err(kind) = physical_evidence {
            drop(held.commit());
            return QuinnStandbySwitchHintTransaction::PhysicalRejected { kind, metadata };
        }
        let LegControlRecord::SwitchHint {
            session_id,
            standby_nonce,
            hint_sequence,
            flow_id,
            direction,
            oldest_unacknowledged,
            cause,
        } = bound.parts().1.record()
        else {
            drop(held.commit());
            return QuinnStandbySwitchHintTransaction::PhysicalRejected {
                kind: QuinnStandbyPhysicalSwitchHintReject::WrongRecordKind,
                metadata,
            };
        };
        let view = PhysicalAuthenticatedBHint {
            frame: bound.parts().1,
            session_id: *session_id,
            generation: bound.parts().1.leg_generation(),
            standby_nonce: *standby_nonce,
            hint_sequence: *hint_sequence,
            flow_id: *flow_id,
            direction: *direction,
            oldest_unacknowledged: *oldest_unacknowledged,
            cause: *cause,
            arrived_at: metadata.arrived_at(),
            actor_seal: &owner.hint_actor_seal,
            _composition_gate: &gate,
        };
        match transact(&view) {
            QuinnStandbySwitchHintDecision::Settled(outcome) => {
                // `view` proved this exact held value is a SWITCH_HINT. The
                // raw record is committed only after the synchronous root has
                // returned an irrevocable accepted/rejected fact.
                drop(held.commit());
                QuinnStandbySwitchHintTransaction::Settled { outcome, metadata }
            }
            QuinnStandbySwitchHintDecision::Retain(blocker) => {
                owner.pending_control = Some(QuinnStandbyPendingControl::SwitchHint(held));
                QuinnStandbySwitchHintTransaction::Retained { blocker, metadata }
            }
        }
    }

    /// Moves only an independently failed B into a composition-owned cleanup
    /// task. Healthy current A remains synchronously driveable while B's close
    /// tail runs; dropping a caller loses no actor or join authority.
    pub(crate) fn begin_failed_standby_join(
        &mut self,
    ) -> Result<QuinnStandbyJoinStartDisposition, QuinnStandbyClearError> {
        if self.phase != QuinnClientCompositionPhase::Running {
            return Err(QuinnStandbyClearError::SessionShuttingDown);
        }
        let owner = self
            .standby
            .as_mut()
            .ok_or(QuinnStandbyClearError::MissingStandby)?;
        if owner.failure.is_none() {
            return Err(QuinnStandbyClearError::FailureNotObserved);
        }
        if let Some(pending) = owner.pending_control.as_ref() {
            let (kind, messages, bytes) = pending_standby_control_inventory(pending);
            return Err(QuinnStandbyClearError::PendingControl {
                kind,
                messages,
                bytes,
            });
        }
        let actor = owner
            .actor
            .as_ref()
            .ok_or(QuinnStandbyClearError::CompositionInvariant)?;
        match actor {
            QuinnStandbyActorOwner::Joining(_) => {
                return Ok(QuinnStandbyJoinStartDisposition::AlreadyJoining);
            }
            QuinnStandbyActorOwner::Joined(_) => {
                return Ok(QuinnStandbyJoinStartDisposition::AlreadyJoined);
            }
            QuinnStandbyActorOwner::TaskLost(cause) => {
                return Err(QuinnStandbyClearError::JoinTask(Arc::clone(cause)));
            }
            QuinnStandbyActorOwner::Running(_) => {}
        }
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| QuinnStandbyClearError::RuntimeUnavailable)?;
        let Some(QuinnStandbyActorOwner::Running(actor)) = owner.actor.take() else {
            return Err(QuinnStandbyClearError::CompositionInvariant);
        };
        let mut actor = *actor;
        actor.request_cancel();
        owner.actor = Some(QuinnStandbyActorOwner::Joining(spawn_standby_join(
            &runtime, actor,
        )));
        Ok(QuinnStandbyJoinStartDisposition::Started)
    }

    /// Non-blocking failed-B cleanup drive. The exact task and protocol state
    /// remain in the slot until structural join proves zero ownership.
    pub(crate) fn try_finish_failed_standby_join(
        &mut self,
    ) -> Result<QuinnStandbyJoinDrive, QuinnStandbyClearError> {
        if self.phase != QuinnClientCompositionPhase::Running {
            return Err(QuinnStandbyClearError::SessionShuttingDown);
        }
        let owner = self
            .standby
            .as_mut()
            .ok_or(QuinnStandbyClearError::MissingStandby)?;
        if owner.failure.is_none() {
            return Err(QuinnStandbyClearError::FailureNotObserved);
        }
        let actor = owner
            .actor
            .take()
            .ok_or(QuinnStandbyClearError::CompositionInvariant)?;
        match actor {
            QuinnStandbyActorOwner::Running(actor) => {
                owner.actor = Some(QuinnStandbyActorOwner::Running(actor));
                Err(QuinnStandbyClearError::NotStarted)
            }
            QuinnStandbyActorOwner::Joining(mut task) => match poll_finished_join(&mut task) {
                Poll::Pending => {
                    owner.actor = Some(QuinnStandbyActorOwner::Joining(task));
                    Ok(QuinnStandbyJoinDrive::Pending)
                }
                Poll::Ready(Err(error)) => {
                    let cause: Arc<str> = Arc::from(error.to_string());
                    owner.actor = Some(QuinnStandbyActorOwner::TaskLost(Arc::clone(&cause)));
                    Err(QuinnStandbyClearError::JoinTask(cause))
                }
                Poll::Ready(Ok(QuinnStandbyLegJoinOutcome {
                    actor,
                    joined,
                    runtime: _,
                })) => match joined {
                    Ok(joined) => {
                        drop(actor);
                        owner.actor = Some(QuinnStandbyActorOwner::Joined(joined));
                        self.commit_joined_failed_standby()
                    }
                    Err(error) => {
                        owner.actor = Some(QuinnStandbyActorOwner::Running(Box::new(actor)));
                        Err(QuinnStandbyClearError::Actor(error))
                    }
                },
            },
            QuinnStandbyActorOwner::Joined(joined) => {
                owner.actor = Some(QuinnStandbyActorOwner::Joined(joined));
                self.commit_joined_failed_standby()
            }
            QuinnStandbyActorOwner::TaskLost(cause) => {
                owner.actor = Some(QuinnStandbyActorOwner::TaskLost(Arc::clone(&cause)));
                Err(QuinnStandbyClearError::JoinTask(cause))
            }
        }
    }

    fn commit_joined_failed_standby(
        &mut self,
    ) -> Result<QuinnStandbyJoinDrive, QuinnStandbyClearError> {
        let owner = self
            .standby
            .as_mut()
            .ok_or(QuinnStandbyClearError::MissingStandby)?;
        if let Some(pending) = owner.pending_control.as_ref() {
            let (kind, messages, bytes) = pending_standby_control_inventory(pending);
            return Err(QuinnStandbyClearError::PendingControl {
                kind,
                messages,
                bytes,
            });
        }
        let Some(QuinnStandbyActorOwner::Joined(joined)) = owner.actor.as_ref() else {
            return Err(QuinnStandbyClearError::JoinedWitnessMissing);
        };
        let inventory = QuinnActorJoinedInventory::from_actor(joined);
        if !inventory.is_clean() {
            return Err(QuinnStandbyClearError::ResidualOwnership {
                messages: inventory.owned_messages(),
                bytes: inventory.owned_bytes(),
                terminals: inventory.owned_terminals(),
                terminal_failure: inventory.terminal_failure(),
            });
        }
        let cleared = QuinnStandbyActorCleared {
            owned_messages: inventory.owned_messages(),
            owned_bytes: inventory.owned_bytes(),
            owned_terminals: inventory.owned_terminals(),
            terminal_failure: inventory.terminal_failure(),
        };
        drop(owner.state.take());
        drop(self.standby.take());
        Ok(QuinnStandbyJoinDrive::Cleared(cleared))
    }

    /// Begins or retries the exact composition-owned B ATTACH after current A
    /// loss. Silent probe expiry requires the non-forgeable Fresh wrapper;
    /// an exact transport terminal may use ordinary Registered or Fresh B.
    /// The actor and every retryable attach owner remain inside the B slot
    /// until installation commits.
    pub(crate) fn drive_owned_standby_post_loss_attach(
        &mut self,
        now: SimTime,
        nonce: AttachNonce,
        credentials: &AttachCredentials,
    ) -> Result<QuinnStandbyAttachDrive, QuinnStandbyAttachError> {
        if self.phase != QuinnClientCompositionPhase::Running {
            return Err(QuinnStandbyAttachError::SessionShuttingDown);
        }
        let (loss_generation, fresh_required) = self.post_loss_attach_context()?;
        self.drive_owned_standby_attach_core(
            now,
            nonce,
            credentials,
            loss_generation,
            fresh_required,
        )
        .map(|outcome| outcome.disposition)
    }

    fn post_loss_attach_context(&self) -> Result<(LegGeneration, bool), QuinnStandbyAttachError> {
        match (self.current_loss_source, self.current_terminal) {
            (
                QuinnCurrentLossSource::ProbeDeadline { generation, .. },
                QuinnCurrentTerminalState::Applied {
                    generation: terminal_generation,
                    ..
                },
            ) if generation == terminal_generation => Ok((generation, true)),
            (
                QuinnCurrentLossSource::TransportTerminal,
                QuinnCurrentTerminalState::Applied { generation, .. },
            ) => Ok((generation, false)),
            (QuinnCurrentLossSource::None, _) => Err(QuinnStandbyAttachError::LossNotReady),
            (
                QuinnCurrentLossSource::ProbeDeadline { .. }
                | QuinnCurrentLossSource::TransportTerminal,
                _,
            ) => Err(QuinnStandbyAttachError::TerminalNotApplied),
        }
    }

    fn drive_owned_standby_attach_core(
        &mut self,
        now: SimTime,
        nonce: AttachNonce,
        credentials: &AttachCredentials,
        loss_generation: LegGeneration,
        fresh_required: bool,
    ) -> Result<QuinnStandbyAttachCoreDrive, QuinnStandbyAttachError> {
        let owner = self
            .standby
            .as_mut()
            .ok_or(QuinnStandbyAttachError::MissingStandby)?;
        if owner.failure.is_some() {
            return Err(QuinnStandbyAttachError::StandbyFailed);
        }
        if owner.admitted_generation != loss_generation
            || self.controller.generation() != loss_generation
        {
            owner
                .failure
                .get_or_insert(QuinnStandbyFailureKind::ProtocolInvariant);
            return Err(QuinnStandbyAttachError::WrongSwitchEpoch);
        }
        let Some(state) = owner.state.take() else {
            owner
                .failure
                .get_or_insert(QuinnStandbyFailureKind::ProtocolInvariant);
            return Err(QuinnStandbyAttachError::CompositionInvariant);
        };
        let pending = match (fresh_required, state) {
            (true, QuinnRegisteredStandbyState::Fresh(fresh)) => {
                match fresh.begin_exact_next_attach_for_silent_loss(now, nonce, credentials) {
                    Ok(pending) => pending,
                    Err(failure) => {
                        let kind = failure.kind();
                        owner.state =
                            Some(QuinnRegisteredStandbyState::Fresh(failure.into_fresh()));
                        match kind {
                            FreshRegisteredAttachBeginErrorKind::Stale => {}
                            FreshRegisteredAttachBeginErrorKind::EndpointOrQueueLost
                            | FreshRegisteredAttachBeginErrorKind::Registered(
                                RegisteredAttachBeginErrorKind::EndpointOrQueueLost,
                            ) => {
                                owner.failure = Some(QuinnStandbyFailureKind::QueueOrEndpointLost);
                            }
                            FreshRegisteredAttachBeginErrorKind::Registered(
                                RegisteredAttachBeginErrorKind::GenerationExhausted
                                | RegisteredAttachBeginErrorKind::Rejected,
                            ) => {
                                owner
                                    .failure
                                    .get_or_insert(QuinnStandbyFailureKind::ProtocolInvariant);
                            }
                        }
                        return Err(QuinnStandbyAttachError::Begin(kind));
                    }
                }
            }
            (true, QuinnRegisteredStandbyState::AttachPending(pending))
            | (false, QuinnRegisteredStandbyState::AttachPending(pending)) => pending,
            (true, state) => {
                owner.state = Some(state);
                return Err(QuinnStandbyAttachError::FreshStandbyRequired);
            }
            (false, state) => {
                let registered = match state {
                    QuinnRegisteredStandbyState::Registered(registered) => registered,
                    QuinnRegisteredStandbyState::Fresh(fresh) => fresh.into_registered(),
                    state => {
                        owner.state = Some(state);
                        return Err(QuinnStandbyAttachError::EligibleStandbyRequired);
                    }
                };
                match registered.begin_exact_next_attach(nonce, credentials) {
                    Ok(pending) => pending,
                    Err(failure) => {
                        let kind = failure.kind();
                        owner.state = Some(QuinnRegisteredStandbyState::Registered(
                            failure.into_registered(),
                        ));
                        match kind {
                            RegisteredAttachBeginErrorKind::EndpointOrQueueLost => {
                                owner.failure = Some(QuinnStandbyFailureKind::QueueOrEndpointLost);
                            }
                            RegisteredAttachBeginErrorKind::GenerationExhausted
                            | RegisteredAttachBeginErrorKind::Rejected => {
                                owner
                                    .failure
                                    .get_or_insert(QuinnStandbyFailureKind::ProtocolInvariant);
                            }
                        }
                        return Err(QuinnStandbyAttachError::BeginRegistered(kind));
                    }
                }
            }
        };
        let pending_frame_bytes = match pending.encoded_len() {
            Ok(encoded_len) => encoded_len,
            Err(cause) => {
                owner.state = Some(QuinnRegisteredStandbyState::AttachPending(pending));
                owner
                    .failure
                    .get_or_insert(QuinnStandbyFailureKind::ProtocolInvariant);
                return Err(QuinnStandbyAttachError::PendingFrameEncoding(cause));
            }
        };
        let gate = QuinnLegCompositionGate::mint();
        let Some(QuinnStandbyActorOwner::Running(actor)) = owner.actor.as_mut() else {
            owner.state = Some(QuinnRegisteredStandbyState::AttachPending(pending));
            return Err(QuinnStandbyAttachError::StandbyNotRunning);
        };
        match actor.with_controller_parts_mut_input(&gate, pending, |pending, _io, queue| {
            pending.enqueue(queue).map_err(Box::new)
        }) {
            Ok(Ok(awaiting)) => {
                owner.state = Some(QuinnRegisteredStandbyState::AwaitingAttach(awaiting));
                Ok(QuinnStandbyAttachCoreDrive {
                    disposition: QuinnStandbyAttachDrive::AwaitingResponse,
                    pending_frame_bytes: 0,
                })
            }
            Ok(Err(failure)) => {
                let failure = *failure;
                let classification = classify_registered_attach_enqueue(failure.kind());
                owner.state = Some(QuinnRegisteredStandbyState::AttachPending(
                    failure.into_pending(),
                ));
                match classification {
                    Ok(()) => Ok(QuinnStandbyAttachCoreDrive {
                        disposition: QuinnStandbyAttachDrive::Pressure,
                        pending_frame_bytes,
                    }),
                    Err(kind) => {
                        owner.failure.get_or_insert(standby_failure_for_queue(kind));
                        Err(QuinnStandbyAttachError::Enqueue(kind))
                    }
                }
            }
            Err((pending, error)) => {
                owner.state = Some(QuinnRegisteredStandbyState::AttachPending(pending));
                owner
                    .failure
                    .get_or_insert(QuinnStandbyFailureKind::ActorTransport);
                Err(QuinnStandbyAttachError::Actor(error))
            }
        }
    }

    /// Commits one actor-held physical `ATTACH_ACCEPTED` as a single local
    /// transaction. Controller and actor ownership move first, the policy
    /// arbiter finalizes second, and the raw actor record publishes DELIVERY
    /// only as the infallible last step of `try_commit`.
    pub(crate) fn commit_owned_standby_physical_replacement(
        &mut self,
        arbiter: &mut ClientSwitchArbiter,
    ) -> Result<CommittedSwitch, QuinnPhysicalReplacementCommitError> {
        if self.phase != QuinnClientCompositionPhase::Running {
            return Err(QuinnPhysicalReplacementCommitError::SessionShuttingDown);
        }
        self.physical_attach_inventory()
            .map_err(|_| QuinnPhysicalReplacementCommitError::CompositionInvariant)?;
        let physical_generation = match self.physical_attach.as_ref() {
            Some(QuinnPhysicalAttachOwner {
                stage: PhysicalPendingAttachStage::Accepted,
                authority: QuinnPhysicalAttachAuthority::Accepted(held),
                stored_pending_frame_bytes: 0,
            }) => match held.value() {
                QuinnStandbyHeldControl::AttachAccepted { accepted, .. } => accepted.generation(),
                QuinnStandbyHeldControl::ProbeAck(_) | QuinnStandbyHeldControl::SwitchHint(_) => {
                    return Err(QuinnPhysicalReplacementCommitError::CompositionInvariant);
                }
            },
            None => {
                return Err(QuinnPhysicalReplacementCommitError::MissingPhysicalAuthority);
            }
            Some(_) => return Err(QuinnPhysicalReplacementCommitError::WrongState),
        };
        let Some(owner) = self.standby.as_ref() else {
            return Err(QuinnPhysicalReplacementCommitError::MissingStandby);
        };
        if let Some(failure) = owner.failure {
            let action = match failure {
                QuinnStandbyFailureKind::ActorTransport
                | QuinnStandbyFailureKind::QueueOrEndpointLost => {
                    QuinnOwnedStandbyInstallAction::StandbyTerminal
                }
                QuinnStandbyFailureKind::ProtocolInvariant => {
                    QuinnOwnedStandbyInstallAction::Invariant
                }
            };
            return Err(QuinnPhysicalReplacementCommitError::StandbyFailed { action });
        }
        let owner_ready = owner.pending_control.is_none()
            && owner.shutdown_control_discard.is_none()
            && owner.shutdown_terminal.is_none()
            && owner.shutdown_retired_state.is_none()
            && matches!(
                owner.actor.as_ref(),
                Some(QuinnStandbyActorOwner::Running(_))
            )
            && matches!(
                owner.state.as_ref(),
                Some(QuinnRegisteredStandbyState::AcceptedPendingCommit { generation })
                    if *generation == physical_generation
            );
        if !owner_ready {
            return Err(QuinnPhysicalReplacementCommitError::WrongState);
        }

        let physical = self
            .physical_attach
            .take()
            .ok_or(QuinnPhysicalReplacementCommitError::MissingPhysicalAuthority)?;
        let held = match physical {
            QuinnPhysicalAttachOwner {
                stage: PhysicalPendingAttachStage::Accepted,
                authority: QuinnPhysicalAttachAuthority::Accepted(held),
                stored_pending_frame_bytes: 0,
            } => held,
            physical => {
                self.physical_attach = Some(physical);
                return Err(QuinnPhysicalReplacementCommitError::WrongState);
            }
        };
        let mut owner = match self.standby.take() {
            Some(owner) => owner,
            None => {
                self.physical_attach = Some(QuinnPhysicalAttachOwner {
                    stage: PhysicalPendingAttachStage::Accepted,
                    authority: QuinnPhysicalAttachAuthority::Accepted(held),
                    stored_pending_frame_bytes: 0,
                });
                return Err(QuinnPhysicalReplacementCommitError::MissingStandby);
            }
        };
        let actor = match owner.actor.take() {
            Some(QuinnStandbyActorOwner::Running(actor)) => *actor,
            actor => {
                owner.actor = actor;
                self.standby = Some(owner);
                self.physical_attach = Some(QuinnPhysicalAttachOwner {
                    stage: PhysicalPendingAttachStage::Accepted,
                    authority: QuinnPhysicalAttachAuthority::Accepted(held),
                    stored_pending_frame_bytes: 0,
                });
                return Err(QuinnPhysicalReplacementCommitError::WrongState);
            }
        };
        let state = owner.state.take();
        if !matches!(
            state,
            Some(QuinnRegisteredStandbyState::AcceptedPendingCommit { generation })
                if generation == physical_generation
        ) {
            owner.state = state;
            owner.actor = Some(QuinnStandbyActorOwner::Running(Box::new(actor)));
            self.standby = Some(owner);
            self.physical_attach = Some(QuinnPhysicalAttachOwner {
                stage: PhysicalPendingAttachStage::Accepted,
                authority: QuinnPhysicalAttachAuthority::Accepted(held),
                stored_pending_frame_bytes: 0,
            });
            return Err(QuinnPhysicalReplacementCommitError::WrongState);
        }

        let committed = held.try_commit(|control| match control {
            QuinnStandbyHeldControl::AttachAccepted { accepted, policy } => {
                let prepared = match arbiter.preflight_commit(&policy) {
                    Ok(prepared) => prepared,
                    Err(kind) => {
                        return Err((
                            QuinnStandbyHeldControl::AttachAccepted { accepted, policy },
                            QuinnPhysicalReplacementOperationFailure {
                                actor,
                                error: QuinnPhysicalReplacementCommitError::Arbiter(kind),
                            },
                        ));
                    }
                };
                let post_loss = prepared.plan().is_post_loss();
                let replacement = if post_loss {
                    self.begin_registered_post_loss_replacement(accepted, actor)
                } else {
                    self.begin_registered_replacement_core(accepted, actor)
                };
                match replacement {
                    Ok(_started) => {
                        let committed = prepared.finalize();
                        drop(policy);
                        Ok(committed)
                    }
                    Err(failure) => {
                        drop(prepared);
                        let kind = failure.kind();
                        let action = failure.failure_class();
                        let (accepted, actor) = failure.into_parts();
                        Err((
                            QuinnStandbyHeldControl::AttachAccepted { accepted, policy },
                            QuinnPhysicalReplacementOperationFailure {
                                actor,
                                error: QuinnPhysicalReplacementCommitError::Replacement {
                                    kind,
                                    action,
                                },
                            },
                        ))
                    }
                }
            }
            control => Err((
                control,
                QuinnPhysicalReplacementOperationFailure {
                    actor,
                    error: QuinnPhysicalReplacementCommitError::CompositionInvariant,
                },
            )),
        });
        match committed {
            Ok(committed) => {
                drop(owner);
                Ok(committed)
            }
            Err((held, failure)) => {
                owner.actor = Some(QuinnStandbyActorOwner::Running(Box::new(failure.actor)));
                owner.state = Some(QuinnRegisteredStandbyState::AcceptedPendingCommit {
                    generation: physical_generation,
                });
                self.standby = Some(owner);
                self.physical_attach = Some(QuinnPhysicalAttachOwner {
                    stage: PhysicalPendingAttachStage::Accepted,
                    authority: QuinnPhysicalAttachAuthority::Accepted(held),
                    stored_pending_frame_bytes: 0,
                });
                Err(failure.error)
            }
        }
    }

    pub(crate) fn commit_owned_standby_post_loss_replacement(
        &mut self,
    ) -> Result<ClientRecoveryStarted, QuinnOwnedStandbyInstallError> {
        if self.phase != QuinnClientCompositionPhase::Running {
            return Err(QuinnOwnedStandbyInstallError::SessionShuttingDown);
        }
        let owner = self
            .standby
            .take()
            .ok_or(QuinnOwnedStandbyInstallError::MissingStandby)?;
        if let Some(failure) = owner.failure {
            let action = match failure {
                QuinnStandbyFailureKind::ActorTransport
                | QuinnStandbyFailureKind::QueueOrEndpointLost => {
                    QuinnOwnedStandbyInstallAction::StandbyTerminal
                }
                QuinnStandbyFailureKind::ProtocolInvariant => {
                    QuinnOwnedStandbyInstallAction::Invariant
                }
            };
            self.standby = Some(owner);
            return Err(QuinnOwnedStandbyInstallError::StandbyFailed { action });
        }
        if owner.pending_control.is_some() {
            // A physical probe response or hint still owns its exact actor
            // arrival. Replacement cannot silently discard that transaction
            // while moving B into the current slot.
            self.standby = Some(owner);
            return Err(QuinnOwnedStandbyInstallError::WrongState);
        }
        let QuinnRegisteredStandbyOwner {
            mut actor,
            state,
            admitted_generation,
            registration_nonce,
            next_probe_sequence,
            current_probe_sequence,
            liveness,
            features,
            failure: prior_failure,
            probe_responder,
            pending_control,
            hint_actor_seal,
            shutdown_control_discard,
            shutdown_terminal,
            shutdown_retired_state,
        } = owner;
        let Some(QuinnStandbyActorOwner::Running(actor)) = actor.take() else {
            self.standby = Some(QuinnRegisteredStandbyOwner {
                actor,
                state,
                admitted_generation,
                registration_nonce,
                next_probe_sequence,
                current_probe_sequence,
                liveness,
                features,
                failure: prior_failure,
                probe_responder,
                pending_control,
                hint_actor_seal,
                shutdown_control_discard,
                shutdown_terminal,
                shutdown_retired_state,
            });
            return Err(QuinnOwnedStandbyInstallError::WrongState);
        };
        let Some(QuinnRegisteredStandbyState::Accepted(accepted)) = state else {
            self.standby = Some(QuinnRegisteredStandbyOwner {
                actor: Some(QuinnStandbyActorOwner::Running(actor)),
                state,
                admitted_generation,
                registration_nonce,
                next_probe_sequence,
                current_probe_sequence,
                liveness,
                features,
                failure: prior_failure,
                probe_responder,
                pending_control,
                hint_actor_seal,
                shutdown_control_discard,
                shutdown_terminal,
                shutdown_retired_state,
            });
            return Err(QuinnOwnedStandbyInstallError::WrongState);
        };
        match self.begin_registered_post_loss_replacement_core(accepted, *actor) {
            Ok(started) => Ok(started),
            Err(replacement_failure) => {
                let kind = replacement_failure.kind();
                let action = replacement_failure.failure_class();
                let (accepted, actor) = replacement_failure.into_parts();
                let failure = match action {
                    QuinnOwnedStandbyInstallAction::Retryable
                    | QuinnOwnedStandbyInstallAction::SessionFatal => prior_failure,
                    QuinnOwnedStandbyInstallAction::StandbyTerminal => {
                        if kind == QuinnRegisteredReplacementErrorKind::ActorTransportUnavailable {
                            Some(QuinnStandbyFailureKind::ActorTransport)
                        } else {
                            Some(QuinnStandbyFailureKind::QueueOrEndpointLost)
                        }
                    }
                    QuinnOwnedStandbyInstallAction::Invariant => {
                        Some(QuinnStandbyFailureKind::ProtocolInvariant)
                    }
                };
                self.standby = Some(QuinnRegisteredStandbyOwner {
                    actor: Some(QuinnStandbyActorOwner::Running(Box::new(actor))),
                    state: Some(QuinnRegisteredStandbyState::Accepted(accepted)),
                    admitted_generation,
                    registration_nonce,
                    next_probe_sequence,
                    current_probe_sequence,
                    liveness,
                    features,
                    failure,
                    probe_responder,
                    pending_control,
                    hint_actor_seal,
                    shutdown_control_discard,
                    shutdown_terminal,
                    shutdown_retired_state,
                });
                Err(QuinnOwnedStandbyInstallError::Replacement { kind, action })
            }
        }
    }

    /// Synchronously commits controller A→retired/B→current and then performs
    /// the infallible actor-owner mirror move. A failed controller transaction
    /// restores B's exact policy pieces before returning the whole actor.
    #[cfg(test)]
    pub(super) fn begin_registered_replacement(
        &mut self,
        accepted: AcceptedClientRegisteredAttach,
        actor: StartedQuinnLegActor,
    ) -> Result<ClientRecoveryStarted, QuinnRegisteredReplacementFailure> {
        self.begin_registered_replacement_core(accepted, actor)
    }

    fn begin_registered_replacement_core(
        &mut self,
        accepted: AcceptedClientRegisteredAttach,
        actor: StartedQuinnLegActor,
    ) -> Result<ClientRecoveryStarted, QuinnRegisteredReplacementFailure> {
        if !self.current_policy_is_active() {
            return Err(QuinnRegisteredReplacementFailure {
                ownership: Box::new(QuinnRegisteredReplacementOwnership { accepted, actor }),
                kind: QuinnRegisteredReplacementErrorKind::SessionShuttingDown,
                class: QuinnOwnedStandbyInstallAction::SessionFatal,
            });
        }
        if self.standby.is_some() {
            return Err(QuinnRegisteredReplacementFailure {
                ownership: Box::new(QuinnRegisteredReplacementOwnership { accepted, actor }),
                kind: QuinnRegisteredReplacementErrorKind::OwnedStandbyOccupied,
                class: QuinnOwnedStandbyInstallAction::Retryable,
            });
        }
        let gate = QuinnLegCompositionGate::mint();
        let retired_is_empty = self.retired.is_none();
        let current_join_is_empty = self.current_joined.is_none();
        let controller = &mut self.controller;
        let ownership = QuinnRegisteredReplacementOwnership { accepted, actor };
        let started = self.current.transact_controller_policy_with_input_result(
            &gate,
            ownership,
            |runtime, ownership| {
                let layout_matches = retired_is_empty
                    && current_join_is_empty
                    && controller.physical_actor_layout_matches(runtime, None, &gate);
                if !layout_matches {
                    return Err((
                        ownership,
                        QuinnRegisteredReplacementErrorKind::CompositionInvariant,
                        QuinnOwnedStandbyInstallAction::Invariant,
                    ));
                }

                let QuinnRegisteredReplacementOwnership {
                    accepted,
                    actor: mut candidate,
                } = ownership;
                let mut failure_class = QuinnOwnedStandbyInstallAction::Retryable;
                match candidate.transact_controller_parts(&gate, accepted, |accepted, io, queue| {
                    match controller.begin_registered_replacement(accepted, io, queue) {
                        Ok(started) => Ok(started),
                        Err(failure) => {
                            failure_class =
                                map_client_recovery_failure_class(failure.failure_class());
                            let kind = failure.kind();
                            let (accepted, io, queue) = failure.into_parts();
                            Err(Box::new((accepted, io, queue, kind)))
                        }
                    }
                }) {
                    Ok(started) => Ok((started, candidate)),
                    Err((accepted, kind)) => {
                        let actor_transport = kind.is_none();
                        Err((
                            QuinnRegisteredReplacementOwnership {
                                accepted,
                                actor: candidate,
                            },
                            kind.map_or(
                                QuinnRegisteredReplacementErrorKind::ActorTransportUnavailable,
                                QuinnRegisteredReplacementErrorKind::Controller,
                            ),
                            if actor_transport {
                                QuinnOwnedStandbyInstallAction::StandbyTerminal
                            } else {
                                failure_class
                            },
                        ))
                    }
                }
            },
        );
        match started {
            Ok(Ok((started, candidate))) => {
                let old = std::mem::replace(&mut self.current, candidate);
                self.retired = Some(RetiredQuinnLegOwner::Running(Box::new(old)));
                Ok(started)
            }
            Ok(Err((ownership, kind, class))) => Err(QuinnRegisteredReplacementFailure {
                ownership: Box::new(ownership),
                kind,
                class,
            }),
            Err((ownership, _current_error)) => Err(QuinnRegisteredReplacementFailure {
                ownership: Box::new(ownership),
                kind: QuinnRegisteredReplacementErrorKind::ActorTransportUnavailable,
                class: QuinnOwnedStandbyInstallAction::StandbyTerminal,
            }),
        }
    }

    /// Commits terminal-applied A→retired/B→current without asking the fatal
    /// A actor to reopen its policy mutex. Candidate B's own first-failure
    /// mutex still spans endpoint extraction and the complete controller
    /// transaction; only infallible composition owner moves follow success.
    pub(super) fn begin_registered_post_loss_replacement(
        &mut self,
        accepted: AcceptedClientRegisteredAttach,
        actor: StartedQuinnLegActor,
    ) -> Result<ClientRecoveryStarted, QuinnRegisteredReplacementFailure> {
        if self.standby.is_some() {
            return Err(QuinnRegisteredReplacementFailure {
                ownership: Box::new(QuinnRegisteredReplacementOwnership { accepted, actor }),
                kind: QuinnRegisteredReplacementErrorKind::OwnedStandbyOccupied,
                class: QuinnOwnedStandbyInstallAction::Retryable,
            });
        }
        if matches!(
            self.current_loss_source,
            QuinnCurrentLossSource::ProbeDeadline { .. }
        ) {
            return Err(QuinnRegisteredReplacementFailure {
                ownership: Box::new(QuinnRegisteredReplacementOwnership { accepted, actor }),
                kind: QuinnRegisteredReplacementErrorKind::FreshStandbyRequired,
                class: QuinnOwnedStandbyInstallAction::Invariant,
            });
        }
        self.begin_registered_post_loss_replacement_core(accepted, actor)
    }

    fn begin_registered_post_loss_replacement_core(
        &mut self,
        accepted: AcceptedClientRegisteredAttach,
        actor: StartedQuinnLegActor,
    ) -> Result<ClientRecoveryStarted, QuinnRegisteredReplacementFailure> {
        if self.phase != QuinnClientCompositionPhase::Running {
            return Err(QuinnRegisteredReplacementFailure {
                ownership: Box::new(QuinnRegisteredReplacementOwnership { accepted, actor }),
                kind: QuinnRegisteredReplacementErrorKind::SessionShuttingDown,
                class: QuinnOwnedStandbyInstallAction::SessionFatal,
            });
        }
        if !matches!(
            self.current_terminal,
            QuinnCurrentTerminalState::Applied { .. }
        ) {
            return Err(QuinnRegisteredReplacementFailure {
                ownership: Box::new(QuinnRegisteredReplacementOwnership { accepted, actor }),
                kind: QuinnRegisteredReplacementErrorKind::PostLossNotReady,
                class: QuinnOwnedStandbyInstallAction::Retryable,
            });
        }

        let gate = QuinnLegCompositionGate::mint();
        let layout_matches = self.retired.is_none()
            && self.current_joined.is_none()
            && self
                .current
                .physical_incarnation(&gate)
                .is_some_and(|runtime| {
                    self.controller
                        .terminating_current_physical_actor_matches(runtime, &gate)
                });
        if !layout_matches {
            return Err(QuinnRegisteredReplacementFailure {
                ownership: Box::new(QuinnRegisteredReplacementOwnership { accepted, actor }),
                kind: QuinnRegisteredReplacementErrorKind::CompositionInvariant,
                class: QuinnOwnedStandbyInstallAction::Invariant,
            });
        }

        let mut candidate = actor;
        let controller = &mut self.controller;
        let mut failure_class = QuinnOwnedStandbyInstallAction::Retryable;
        let started =
            candidate.transact_controller_parts(&gate, accepted, |accepted, io, queue| {
                match controller.begin_registered_post_loss_replacement(accepted, io, queue) {
                    Ok(started) => Ok(started),
                    Err(failure) => {
                        failure_class = map_client_recovery_failure_class(failure.failure_class());
                        let kind = failure.kind();
                        let (accepted, io, queue) = failure.into_parts();
                        Err(Box::new((accepted, io, queue, kind)))
                    }
                }
            });
        match started {
            Ok(started) => {
                let old = std::mem::replace(&mut self.current, candidate);
                self.retired = Some(RetiredQuinnLegOwner::Running(Box::new(old)));
                self.current_terminal = QuinnCurrentTerminalState::Unbound;
                self.current_terminal_stamp = None;
                self.current_loss_source = QuinnCurrentLossSource::None;
                Ok(started)
            }
            Err((accepted, kind)) => {
                let actor_transport = kind.is_none();
                Err(QuinnRegisteredReplacementFailure {
                    ownership: Box::new(QuinnRegisteredReplacementOwnership {
                        accepted,
                        actor: candidate,
                    }),
                    kind: kind.map_or(
                        QuinnRegisteredReplacementErrorKind::ActorTransportUnavailable,
                        QuinnRegisteredReplacementErrorKind::Controller,
                    ),
                    class: if actor_transport {
                        QuinnOwnedStandbyInstallAction::StandbyTerminal
                    } else {
                        failure_class
                    },
                })
            }
        }
    }

    pub(crate) fn drive_recovery_one(&mut self) -> ClientRecoveryDrive {
        if !self.current_policy_is_active() {
            return ClientRecoveryDrive::NotRecovering;
        }
        let gate = QuinnLegCompositionGate::mint();
        let controller = &mut self.controller;
        self.current
            .transact_controller_policy(&gate, |runtime| {
                if !controller.current_physical_actor_matches(runtime, &gate) {
                    return ClientRecoveryDrive::InternalMismatch;
                }
                controller.drive_recovery_one()
            })
            .unwrap_or(ClientRecoveryDrive::NotRecovering)
    }

    /// Consumes one completed recovery tail only inside the exact current
    /// actor's first-failure/policy gate. An incomplete tail remains in the
    /// controller unchanged, while a repeated successful call is inert.
    pub(crate) fn settle_completed_recovery_tail(
        &mut self,
    ) -> Result<ClientRecoveryTailSettlement, QuinnRecoveryTailSettlementError> {
        match self.controller.recovery_tail_ownership() {
            ClientRecoveryTailOwnership::Absent => {
                return Ok(ClientRecoveryTailSettlement::AlreadySettled);
            }
            ClientRecoveryTailOwnership::TerminalOwned => {
                return Err(QuinnRecoveryTailSettlementError::SessionShuttingDown);
            }
            ClientRecoveryTailOwnership::Active => {}
        }
        if !self.current_policy_is_active() {
            return Err(QuinnRecoveryTailSettlementError::SessionShuttingDown);
        }
        let gate = QuinnLegCompositionGate::mint();
        let controller = &mut self.controller;
        self.current
            .transact_controller_policy(&gate, |runtime| {
                controller
                    .settle_completed_recovery_tail(runtime, &gate)
                    .map_err(QuinnRecoveryTailSettlementError::Controller)
            })
            .map_err(QuinnRecoveryTailSettlementError::Actor)?
    }

    /// Resolves only an active physical recovery tail before shutdown changes
    /// phase or requests local cancellation. A terminal-owned tail stays with
    /// the exact terminal bind/discard transaction. If a hard actor failure
    /// wins the policy gate, drive that existing transaction once and proceed
    /// to structural join without relabelling the failure LocalCancelled.
    fn settle_recovery_tail_before_shutdown(&mut self) -> Result<(), QuinnCurrentJoinError> {
        if self.fatal_recovery_tail_shutdown.is_some() {
            return Ok(());
        }
        match self.controller.recovery_tail_ownership() {
            ClientRecoveryTailOwnership::Absent | ClientRecoveryTailOwnership::TerminalOwned => {
                return Ok(());
            }
            ClientRecoveryTailOwnership::Active => {}
        }
        match self.settle_completed_recovery_tail() {
            Ok(
                ClientRecoveryTailSettlement::Settled { .. }
                | ClientRecoveryTailSettlement::AlreadySettled,
            ) => Ok(()),
            Err(QuinnRecoveryTailSettlementError::Actor(
                error @ (QuinnLegActorError::RootFailed(_) | QuinnLegActorError::TaskJoin(_)),
            )) => {
                // Pending publication is still hard-failure owned. Structural
                // join will await the same actor root; an already published
                // terminal moves the tail under the existing bind/discard path.
                self.fatal_recovery_tail_shutdown =
                    QuinnFatalRecoveryTailShutdown::from_actor_error(error);
                let _terminal_drive = self.try_drive_current_terminal();
                Ok(())
            }
            Err(error) => Err(QuinnCurrentJoinError::RecoveryTailSettlement(error)),
        }
    }

    pub(crate) fn normal_replacement_blocker(&mut self) -> Option<ClientRecoveryBeginErrorKind> {
        if !self.current_policy_is_active() {
            return Some(ClientRecoveryBeginErrorKind::NotActive);
        }
        let gate = QuinnLegCompositionGate::mint();
        let controller = &self.controller;
        self.current
            .transact_controller_policy(&gate, |runtime| {
                if !controller.current_physical_actor_matches(runtime, &gate) {
                    return Some(ClientRecoveryBeginErrorKind::NotActive);
                }
                controller.normal_replacement_blocker()
            })
            .unwrap_or(Some(ClientRecoveryBeginErrorKind::NotActive))
    }

    pub(crate) fn snapshot(&self) -> SessionSupervisorSnapshot {
        self.controller.snapshot()
    }

    /// Exact reducer configuration installed during initial ATTACH. The
    /// production root uses this read-only witness before its sole factory
    /// mint; no reducer or transport authority escapes with it.
    pub(super) fn session_config(&self) -> SessionConfig {
        self.controller.session_config()
    }

    /// Exact start gate for the first root slice. A future root with a real
    /// monotonic clock and full standby/recovery scheduler may deliberately
    /// widen this contract; the current owner must not reinterpret an already
    /// armed deadline through `SimTime::ZERO`.
    pub(super) fn root_start_is_quiescent(&self) -> bool {
        self.phase == QuinnClientCompositionPhase::Running
            && self.current_terminal == QuinnCurrentTerminalState::Unbound
            && self.current_loss_source == QuinnCurrentLossSource::None
            && self.current_joined.is_none()
            && self.standby.is_none()
            && self.retired.is_none()
            && self.controller.root_start_is_quiescent()
    }

    /// Seals every clone of the sole root-owned TCP factory while the
    /// composition remains Running so existing flows can still drive their
    /// typed LocalAbandon transactions before physical actor shutdown.
    pub(super) fn close_root_flow_admission(&mut self) -> bool {
        self.controller.close_tcp_factory_admission()
    }

    /// Read-only completion gate for logical flow shutdown. Physical actor
    /// cancellation must not begin until reducer flow/tombstone ownership and
    /// the controller FIFO (including an ordered-delivery wait) are empty.
    pub(super) fn root_flow_shutdown_is_quiescent(&self) -> bool {
        self.phase == QuinnClientCompositionPhase::Running
            && self.current_terminal == QuinnCurrentTerminalState::Unbound
            && self.current_loss_source == QuinnCurrentLossSource::None
            && self.current_joined.is_none()
            && self.standby.is_none()
            && self.retired.is_none()
            && self.controller.root_flow_shutdown_is_quiescent()
    }

    pub(super) fn root_shutdown_transport_is_pristine(&self) -> bool {
        self.phase == QuinnClientCompositionPhase::Running
            && self.current_terminal == QuinnCurrentTerminalState::Unbound
            && self.current_joined.is_none()
            && self.controller.root_shutdown_transport_is_pristine()
    }

    /// Publishes the outer production root's Ready state at the exact current
    /// actor first-failure linearization point. A child fatal that wins first
    /// suppresses publication; a successful publication completes before a
    /// later fatal can become observable.
    pub(super) fn transact_root_ready_publication<T>(
        &mut self,
        publication: impl FnOnce() -> T,
    ) -> Result<Option<T>, QuinnLegActorError> {
        if !self.current_policy_is_active() {
            return Ok(None);
        }
        let gate = QuinnLegCompositionGate::mint();
        let controller = &self.controller;
        self.current.transact_controller_policy(&gate, |runtime| {
            controller
                .current_physical_actor_matches(runtime, &gate)
                .then(publication)
        })
    }

    /// Mints the session's sole typed TCP source factory only while the exact
    /// current actor can still linearize controller policy against its first
    /// failure. This is the production-shared ingress a future session root
    /// must drive; it does not expose raw reducer or transport authority.
    pub(super) fn mint_tcp_port_factory(
        &mut self,
        config: FlowPortConfig,
    ) -> Option<Result<ResumableTcpPortFactory, TcpPortFactoryMintError>> {
        if !self.current_policy_is_active() {
            return None;
        }
        let gate = QuinnLegCompositionGate::mint();
        let controller = &mut self.controller;
        self.current
            .transact_controller_policy(&gate, |runtime| {
                if !controller.current_physical_actor_matches(runtime, &gate) {
                    return None;
                }
                Some(controller.mint_tcp_port_factory(config))
            })
            .ok()
            .flatten()
    }

    /// Applies one opaque local source fact through the same actor/controller
    /// policy lock. Actor loss before the commit returns the exact input so a
    /// session root can retain or retry its byte ownership.
    pub(super) fn apply_driver_input(
        &mut self,
        input: ClientControllerDriverInput,
    ) -> Result<ClientControllerDriverApply, QuinnClientDriverInputRejected> {
        if !self.current_policy_is_active() {
            return Err(QuinnClientDriverInputRejected { input });
        }
        let gate = QuinnLegCompositionGate::mint();
        let controller = &mut self.controller;
        match self
            .current
            .transact_controller_policy_with_input(&gate, input, |runtime, input| {
                if !controller.current_physical_actor_matches(runtime, &gate) {
                    return Err(input);
                }
                Ok(controller.apply_driver_input(input))
            }) {
            Ok(Ok(applied)) => Ok(applied),
            Ok(Err(input)) | Err(input) => Err(QuinnClientDriverInputRejected { input }),
        }
    }

    /// Observation-preserving production source transaction. Reducer effects
    /// and the fixed O(1) replay/frontier/terminal facts stay in one returned
    /// value, so the outer physical root cannot accidentally drive the legacy
    /// effects-only seam and discard policy evidence.
    pub(super) fn apply_driver_input_observed(
        &mut self,
        input: ClientControllerDriverInput,
    ) -> Result<ClientControllerObservedDriverApply, QuinnClientDriverInputRejected> {
        if !self.current_policy_is_active() {
            return Err(QuinnClientDriverInputRejected { input });
        }
        let gate = QuinnLegCompositionGate::mint();
        let controller = &mut self.controller;
        match self
            .current
            .transact_controller_policy_with_input(&gate, input, |runtime, input| {
                if !controller.current_physical_actor_matches(runtime, &gate) {
                    return Err(input);
                }
                Ok(controller.apply_driver_input_observed(input))
            }) {
            Ok(Ok(applied)) => Ok(applied),
            Ok(Err(input)) | Err(input) => Err(QuinnClientDriverInputRejected { input }),
        }
    }

    /// Shutdown-only semantic path. Command/factory admission is already
    /// sealed and the root is the sole controller caller, so a physical actor
    /// failure must not prevent an exact local reset from releasing reducer
    /// flow/replay ownership. This deliberately bypasses actor policy while
    /// retaining the same typed driver provenance and reducer backpressure.
    pub(super) fn apply_root_shutdown_driver_input(
        &mut self,
        input: ClientControllerDriverInput,
    ) -> Result<ClientControllerDriverApply, QuinnClientDriverInputRejected> {
        if !self.controller.external_flow_admission_is_closed() {
            return Err(QuinnClientDriverInputRejected { input });
        }
        Ok(self.controller.apply_driver_input(input))
    }

    pub(crate) fn open_local(
        &mut self,
        target: TargetAddr,
    ) -> Option<Result<Vec<SessionEffect>, SessionSupervisorError>> {
        if !self.current_policy_is_active() || self.controller.external_flow_admission_is_closed() {
            return None;
        }
        let gate = QuinnLegCompositionGate::mint();
        let controller = &mut self.controller;
        self.current
            .transact_controller_policy(&gate, |runtime| {
                if !controller.current_physical_actor_matches(runtime, &gate) {
                    return None;
                }
                controller.open_local(target)
            })
            .unwrap_or(None)
    }

    /// Atomically admits one production TCP OPEN while the current actor's
    /// first-failure barrier is held. Queue reservation precedes factory
    /// construction and reducer commit; the exact OPEN reaches the sole FIFO
    /// before this method returns the endpoint/driver pair.
    pub(super) fn open_prepared_tcp(
        &mut self,
        target: TargetAddr,
        factory: &ResumableTcpPortFactory,
    ) -> Result<QuinnCommittedLocalTcpOpen, QuinnPreparedLocalTcpOpenFailure> {
        if !self.current_policy_is_active() {
            return Err(QuinnPreparedLocalTcpOpenFailure::SessionShuttingDown(
                target,
            ));
        }
        let gate = QuinnLegCompositionGate::mint();
        let controller = &mut self.controller;
        match self.current.transact_controller_policy_with_input_result(
            &gate,
            target,
            |runtime, target| {
                if !controller.current_physical_actor_matches(runtime, &gate) {
                    return Err(QuinnPreparedLocalTcpOpenFailure::PhysicalActorMismatch(
                        target,
                    ));
                }
                controller
                    .install_prepared_local_open(target, |local_flow| factory.open_flow(local_flow))
                    .map_err(QuinnPreparedLocalTcpOpenFailure::Controller)
            },
        ) {
            Ok(Ok(committed)) => Ok(QuinnCommittedLocalTcpOpen::from_controller(committed)),
            Ok(Err(failure)) => Err(failure),
            Err((target, actor)) => Err(
                QuinnPreparedLocalTcpOpenFailure::ActorTransportUnavailable {
                    target,
                    kind: QuinnCurrentActorEnqueueUnavailableKind::from_actor_error(&actor),
                },
            ),
        }
    }

    pub(crate) fn current_actor_snapshot(&self) -> QuinnLegActorSnapshot {
        self.current.actor_snapshot()
    }

    pub(crate) fn retired_actor_snapshot(&self) -> Option<QuinnLegActorSnapshot> {
        match self.retired.as_ref()? {
            RetiredQuinnLegOwner::Running(actor) => Some(actor.actor_snapshot()),
            RetiredQuinnLegOwner::Joining(_)
            | RetiredQuinnLegOwner::Joined(_)
            | RetiredQuinnLegOwner::TaskLost(_) => None,
        }
    }

    /// Captures every currently waitable physical actor without exposing an
    /// actor, queue, transport, or cancellation authority to the root.
    pub(crate) fn progress_cursor(&self) -> QuinnCompositionProgressCursor {
        let standby = self
            .standby
            .as_ref()
            .map_or(QuinnOwnedProgressCursor::Dormant, |owner| {
                match owner.actor.as_ref() {
                    Some(QuinnStandbyActorOwner::Running(actor)) => {
                        QuinnOwnedProgressCursor::Running(actor.owner_progress_cursor())
                    }
                    Some(QuinnStandbyActorOwner::Joining(_)) => QuinnOwnedProgressCursor::Joining,
                    Some(
                        QuinnStandbyActorOwner::Joined(_) | QuinnStandbyActorOwner::TaskLost(_),
                    )
                    | None => QuinnOwnedProgressCursor::Dormant,
                }
            });
        let retired = self
            .retired
            .as_ref()
            .map_or(QuinnOwnedProgressCursor::Dormant, |owner| match owner {
                RetiredQuinnLegOwner::Running(actor) => {
                    QuinnOwnedProgressCursor::Running(actor.owner_progress_cursor())
                }
                RetiredQuinnLegOwner::Joining(_) => QuinnOwnedProgressCursor::Joining,
                RetiredQuinnLegOwner::Joined(_) | RetiredQuinnLegOwner::TaskLost(_) => {
                    QuinnOwnedProgressCursor::Dormant
                }
            });
        QuinnCompositionProgressCursor {
            current: self.current.owner_progress_cursor(),
            standby,
            retired,
        }
    }

    /// Waits without polling for one actor-owned progress publication. The
    /// result is only a hint: the sole root must perform a bounded scan of all
    /// transport sources before it commits any timer expiry.
    pub(crate) async fn wait_for_actor_progress(
        &mut self,
        cursor: QuinnCompositionProgressCursor,
    ) -> Result<QuinnCompositionProgress, QuinnCompositionProgressError> {
        let QuinnCompositionProgressCursor {
            current,
            standby,
            retired,
        } = cursor;
        if !self.current.matches_owner_progress_cursor(&current) {
            return Err(QuinnCompositionProgressError {
                source: QuinnCompositionProgressSource::Current,
                kind: QuinnCompositionProgressErrorKind::TopologyChanged,
            });
        }
        let waited = {
            let standby = standby_progress_wait(&mut self.standby, standby).map_err(|()| {
                QuinnCompositionProgressError {
                    source: QuinnCompositionProgressSource::Standby,
                    kind: QuinnCompositionProgressErrorKind::TopologyChanged,
                }
            })?;
            let retired = retired_progress_wait(&mut self.retired, retired).map_err(|()| {
                QuinnCompositionProgressError {
                    source: QuinnCompositionProgressSource::Retired,
                    kind: QuinnCompositionProgressErrorKind::TopologyChanged,
                }
            })?;
            tokio::select! {
                biased;
                result = self.current.wait_for_owner_progress(current) => {
                    QuinnCompositionProgressWaitOutcome::Current(result)
                }
                result = wait_owned_progress(standby) => {
                    QuinnCompositionProgressWaitOutcome::Standby(result)
                }
                result = wait_owned_progress(retired) => {
                    QuinnCompositionProgressWaitOutcome::Retired(result)
                }
            }
        };
        match waited {
            QuinnCompositionProgressWaitOutcome::Current(result) => result
                .map(|epoch| QuinnCompositionProgress::Current { epoch })
                .map_err(|actor| QuinnCompositionProgressError {
                    source: QuinnCompositionProgressSource::Current,
                    kind: QuinnCompositionProgressErrorKind::Actor(actor),
                }),
            QuinnCompositionProgressWaitOutcome::Standby(QuinnOwnedProgressWaitOutcome::Actor(
                result,
            )) => result
                .map(|epoch| QuinnCompositionProgress::Standby { epoch })
                .map_err(|actor| QuinnCompositionProgressError {
                    source: QuinnCompositionProgressSource::Standby,
                    kind: QuinnCompositionProgressErrorKind::Actor(actor),
                }),
            QuinnCompositionProgressWaitOutcome::Retired(QuinnOwnedProgressWaitOutcome::Actor(
                result,
            )) => result
                .map(|epoch| QuinnCompositionProgress::Retired { epoch })
                .map_err(|actor| QuinnCompositionProgressError {
                    source: QuinnCompositionProgressSource::Retired,
                    kind: QuinnCompositionProgressErrorKind::Actor(actor),
                }),
            QuinnCompositionProgressWaitOutcome::Standby(QuinnOwnedProgressWaitOutcome::Join(
                result,
            )) => self.finish_waited_standby_join(result),
            QuinnCompositionProgressWaitOutcome::Retired(QuinnOwnedProgressWaitOutcome::Join(
                result,
            )) => self.finish_waited_retired_join(result),
        }
    }

    fn finish_waited_standby_join(
        &mut self,
        result: Result<QuinnStandbyLegJoinOutcome, tokio::task::JoinError>,
    ) -> Result<QuinnCompositionProgress, QuinnCompositionProgressError> {
        let owner = self.standby.as_mut().ok_or(QuinnCompositionProgressError {
            source: QuinnCompositionProgressSource::Standby,
            kind: QuinnCompositionProgressErrorKind::TopologyChanged,
        })?;
        let slot = owner.actor.take();
        let Some(QuinnStandbyActorOwner::Joining(task)) = slot else {
            owner.actor = slot;
            return Err(QuinnCompositionProgressError {
                source: QuinnCompositionProgressSource::Standby,
                kind: QuinnCompositionProgressErrorKind::TopologyChanged,
            });
        };
        drop(task);
        match result {
            Ok(QuinnStandbyLegJoinOutcome {
                actor,
                joined,
                runtime: _,
            }) => match joined {
                Ok(joined) => {
                    drop(actor);
                    owner.actor = Some(QuinnStandbyActorOwner::Joined(joined));
                    Ok(QuinnCompositionProgress::StandbyJoin)
                }
                Err(actor_error) => {
                    owner.actor = Some(QuinnStandbyActorOwner::Running(Box::new(actor)));
                    Err(QuinnCompositionProgressError {
                        source: QuinnCompositionProgressSource::Standby,
                        kind: QuinnCompositionProgressErrorKind::Actor(actor_error),
                    })
                }
            },
            Err(error) => {
                let cause: Arc<str> = Arc::from(error.to_string());
                owner.actor = Some(QuinnStandbyActorOwner::TaskLost(Arc::clone(&cause)));
                Err(QuinnCompositionProgressError {
                    source: QuinnCompositionProgressSource::Standby,
                    kind: QuinnCompositionProgressErrorKind::Actor(QuinnLegActorError::TaskJoin(
                        cause.to_string(),
                    )),
                })
            }
        }
    }

    fn finish_waited_retired_join(
        &mut self,
        result: Result<RetiredQuinnLegJoinOutcome, tokio::task::JoinError>,
    ) -> Result<QuinnCompositionProgress, QuinnCompositionProgressError> {
        let slot = self.retired.take();
        let Some(RetiredQuinnLegOwner::Joining(task)) = slot else {
            self.retired = slot;
            return Err(QuinnCompositionProgressError {
                source: QuinnCompositionProgressSource::Retired,
                kind: QuinnCompositionProgressErrorKind::TopologyChanged,
            });
        };
        drop(task);
        match result {
            Ok(RetiredQuinnLegJoinOutcome { actor, joined }) => match joined {
                Ok(joined) => {
                    drop(actor);
                    self.retired = Some(RetiredQuinnLegOwner::Joined(joined));
                    Ok(QuinnCompositionProgress::RetiredJoin)
                }
                Err(actor_error) => {
                    self.retired = Some(RetiredQuinnLegOwner::Running(Box::new(actor)));
                    Err(QuinnCompositionProgressError {
                        source: QuinnCompositionProgressSource::Retired,
                        kind: QuinnCompositionProgressErrorKind::Actor(actor_error),
                    })
                }
            },
            Err(error) => {
                let cause: Arc<str> = Arc::from(error.to_string());
                self.retired = Some(RetiredQuinnLegOwner::TaskLost(Arc::clone(&cause)));
                Err(QuinnCompositionProgressError {
                    source: QuinnCompositionProgressSource::Retired,
                    kind: QuinnCompositionProgressErrorKind::Actor(QuinnLegActorError::TaskJoin(
                        cause.to_string(),
                    )),
                })
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn current_leg_io_counters_for_test(&self) -> Option<LegIoCounters> {
        self.controller.current_counters()
    }

    #[cfg(test)]
    pub(crate) fn current_outbound_inventory_for_test(&self) -> Option<(usize, usize)> {
        Some((
            self.controller.current_queued_frames()?,
            self.controller.current_queued_bytes()?,
        ))
    }

    #[cfg(test)]
    pub(crate) fn force_submitted_reset_root_delivered_before_attempt_for_test(&self) -> bool {
        self.controller
            .force_submitted_reset_root_delivered_before_attempt_for_test()
    }

    #[cfg(test)]
    pub(crate) fn retired_supersession_for_test(
        &self,
    ) -> Option<(LegGeneration, LegGeneration, usize, usize)> {
        self.controller.retired_supersession_for_test()
    }

    #[cfg(test)]
    pub(crate) fn retired_supersession_detail_for_test(
        &self,
    ) -> Option<(bool, &'static str, bool)> {
        self.controller.retired_supersession_detail_for_test()
    }

    #[cfg(test)]
    pub(crate) fn force_supersession_history_replacements_for_test(&mut self, replacements: usize) {
        self.controller
            .force_supersession_history_replacements_for_test(replacements);
    }

    #[cfg(test)]
    pub(crate) const fn shutdown_phase_is_joined_for_test(&self) -> bool {
        matches!(self.phase, QuinnClientCompositionPhase::Joined)
    }

    #[cfg(test)]
    pub(crate) fn controller_shutdown_settlement_for_test(
        &self,
    ) -> Option<crate::owned_upstream::client_controller::ClientControllerShutdownSettlement> {
        self.controller.shutdown_settlement()
    }

    #[cfg(test)]
    pub(crate) fn recovery_tail_inventory_for_test(&self) -> Option<ClientRecoveryTailInventory> {
        self.controller.recovery_tail_inventory_for_test()
    }

    #[cfg(test)]
    pub(super) fn recovery_tail_ownership_for_test(&self) -> ClientRecoveryTailOwnership {
        self.controller.recovery_tail_ownership()
    }

    #[cfg(test)]
    pub(crate) fn current_actor_readiness_epoch_for_test(&self) -> u64 {
        self.current.readiness_epoch()
    }

    #[cfg(test)]
    pub(crate) fn current_probe_writer_completed_for_test(&self) -> bool {
        self.controller.current_probe_writer_completed_for_test()
    }

    /// Deliberately completes the controller half of one real actor terminal
    /// transaction without publishing its result into the composition state.
    /// The next production drive must therefore observe `AlreadyDiscarded`
    /// while still in `ControllerBound` and make that consumed failure sticky.
    #[cfg(test)]
    pub(super) fn bind_and_discard_current_terminal_controller_only_for_test(
        &mut self,
    ) -> Result<Option<ClientCurrentTerminalDiscarded>, QuinnCurrentTerminalError> {
        if self.phase != QuinnClientCompositionPhase::Running
            || self.current_terminal != QuinnCurrentTerminalState::Unbound
            || self.current_terminal_stamp.is_some()
        {
            return Err(QuinnCurrentTerminalError::CompositionInvariant);
        }
        let gate = QuinnLegCompositionGate::mint();
        let current_matches = self
            .current
            .physical_incarnation(&gate)
            .is_some_and(|runtime| {
                self.controller
                    .current_physical_actor_matches(runtime, &gate)
            });
        if !current_matches {
            return Err(QuinnCurrentTerminalError::CompositionInvariant);
        }

        let terminal = self.try_bind_current_actor_terminal(&gate);
        match terminal {
            Ok(QuinnLegActorReportedTerminalPoll::Pending) => Ok(None),
            Ok(QuinnLegActorReportedTerminalPoll::Rejected(error)) => Err(error),
            Ok(QuinnLegActorReportedTerminalPoll::AlreadyConsumed) => {
                Err(QuinnCurrentTerminalError::CompositionInvariant)
            }
            Ok(QuinnLegActorReportedTerminalPoll::Applied(bound_at)) => {
                self.current_terminal_stamp = Some(bound_at.terminal_stamp);
                self.current_terminal = QuinnCurrentTerminalState::ControllerBound {
                    generation: bound_at.bound.generation(),
                    reason: bound_at.bound.reason(),
                };
                self.controller
                    .discard_terminating_current_recovery()
                    .map(Some)
                    .map_err(QuinnCurrentTerminalError::ControllerDiscard)
            }
            Err(error) => Err(QuinnCurrentTerminalError::Actor(error)),
        }
    }

    /// Rejects one ready exact actor terminal before controller bind. The actor
    /// must restore the same non-cloneable fact so the next production drive
    /// can bind and apply it exactly once.
    #[cfg(test)]
    pub(super) fn reject_current_terminal_before_bind_for_test(
        &mut self,
    ) -> Result<Option<()>, QuinnCurrentTerminalError> {
        if self.phase != QuinnClientCompositionPhase::Running
            || self.current_terminal != QuinnCurrentTerminalState::Unbound
        {
            return Err(QuinnCurrentTerminalError::CompositionInvariant);
        }
        let gate = QuinnLegCompositionGate::mint();
        let current_matches = self
            .current
            .physical_incarnation(&gate)
            .is_some_and(|runtime| {
                self.controller
                    .current_physical_actor_matches(runtime, &gate)
            });
        if !current_matches {
            return Err(QuinnCurrentTerminalError::CompositionInvariant);
        }
        match self
            .current
            .try_transact_reported_terminal(&gate, |_runtime, terminal| Err((terminal, ())))
        {
            Ok(QuinnLegActorReportedTerminalPoll::Pending) => Ok(None),
            Ok(QuinnLegActorReportedTerminalPoll::Rejected(())) => Ok(Some(())),
            Ok(QuinnLegActorReportedTerminalPoll::Applied(()))
            | Ok(QuinnLegActorReportedTerminalPoll::AlreadyConsumed) => {
                Err(QuinnCurrentTerminalError::CompositionInvariant)
            }
            Err(error) => Err(QuinnCurrentTerminalError::Actor(error)),
        }
    }

    #[cfg(test)]
    pub(super) fn current_joined_ownership_for_test(&self) -> Option<(usize, usize)> {
        self.current_joined
            .as_ref()
            .map(|joined| (joined.owned_messages(), joined.owned_bytes()))
    }

    #[cfg(test)]
    pub(super) const fn retired_owner_is_absent_for_test(&self) -> bool {
        self.retired.is_none()
    }

    #[cfg(test)]
    pub(crate) async fn wait_for_current_actor_progress(
        &self,
        previous: u64,
    ) -> Result<u64, QuinnLegActorError> {
        self.current.wait_for_progress(previous).await
    }

    /// Moves one exact spontaneous current terminal into the matching
    /// controller slot, then consumes at most one logical `LegLost`. Physical
    /// connection/Endpoint cleanup remains owned by the actor's later join.
    pub(crate) fn try_drive_current_terminal(
        &mut self,
    ) -> Result<QuinnCurrentTerminalDrive, QuinnCurrentTerminalError> {
        if self.phase != QuinnClientCompositionPhase::Running {
            return Err(QuinnCurrentTerminalError::SessionShuttingDown);
        }
        match self.current_terminal {
            QuinnCurrentTerminalState::Applied {
                generation, reason, ..
            } => {
                return Ok(QuinnCurrentTerminalDrive::AlreadyApplied { generation, reason });
            }
            QuinnCurrentTerminalState::ConsumedFailed { failure, .. } => {
                return Err(failure.into_drive_error());
            }
            QuinnCurrentTerminalState::ShutdownCleared { .. } => {
                return Err(QuinnCurrentTerminalError::SessionShuttingDown);
            }
            QuinnCurrentTerminalState::ControllerBound { .. } => {
                return self.finish_bound_current_terminal();
            }
            QuinnCurrentTerminalState::Unbound => {}
        }
        if self.current_terminal_stamp.is_some() {
            return Err(QuinnCurrentTerminalError::CompositionInvariant);
        }

        let gate = QuinnLegCompositionGate::mint();
        let current_matches = self
            .current
            .physical_incarnation(&gate)
            .is_some_and(|runtime| {
                self.controller
                    .current_physical_actor_matches(runtime, &gate)
            });
        if !current_matches {
            return Err(QuinnCurrentTerminalError::CompositionInvariant);
        }

        let terminal = self.try_bind_current_actor_terminal(&gate);
        match terminal {
            Ok(QuinnLegActorReportedTerminalPoll::Pending) => {
                Ok(QuinnCurrentTerminalDrive::Pending)
            }
            Ok(QuinnLegActorReportedTerminalPoll::Rejected(error)) => Err(error),
            Ok(QuinnLegActorReportedTerminalPoll::AlreadyConsumed) => {
                Err(QuinnCurrentTerminalError::CompositionInvariant)
            }
            Ok(QuinnLegActorReportedTerminalPoll::Applied(bound_at)) => {
                self.current_terminal_stamp = Some(bound_at.terminal_stamp);
                self.current_terminal = QuinnCurrentTerminalState::ControllerBound {
                    generation: bound_at.bound.generation(),
                    reason: bound_at.bound.reason(),
                };
                self.finish_bound_current_terminal()
            }
            Err(error) => Err(QuinnCurrentTerminalError::Actor(error)),
        }
    }

    fn try_bind_current_actor_terminal(
        &mut self,
        gate: &QuinnLegCompositionGate,
    ) -> Result<
        QuinnLegActorReportedTerminalPoll<QuinnCurrentTerminalBoundAt, QuinnCurrentTerminalError>,
        QuinnLegActorError,
    > {
        let controller = &mut self.controller;
        self.current
            .try_transact_reported_terminal(gate, |runtime, reported| {
                if reported.reason() == LegTransportTerminalReason::LocalCancelled {
                    return Err((
                        reported,
                        QuinnCurrentTerminalError::UnexpectedLocalCancellation,
                    ));
                }
                if !controller.current_physical_actor_matches(runtime, gate) {
                    return Err((reported, QuinnCurrentTerminalError::CompositionInvariant));
                }
                if reported.failure_observed_at().is_none() {
                    return Err((
                        reported,
                        QuinnCurrentTerminalError::MissingTerminalFailureTimestamp,
                    ));
                }
                let (terminal, terminal_stamp) = reported.into_parts();
                match controller.bind_current_terminal(terminal) {
                    Ok(bound) => Ok(QuinnCurrentTerminalBoundAt {
                        bound,
                        terminal_stamp,
                    }),
                    Err(failure) => {
                        let kind = failure.kind();
                        Err((
                            QuinnLegActorReportedTerminal::restore(
                                failure.into_terminal(),
                                terminal_stamp,
                            ),
                            QuinnCurrentTerminalError::ControllerBind(kind),
                        ))
                    }
                }
            })
    }

    fn finish_bound_current_terminal(
        &mut self,
    ) -> Result<QuinnCurrentTerminalDrive, QuinnCurrentTerminalError> {
        let QuinnCurrentTerminalState::ControllerBound { generation, reason } =
            self.current_terminal
        else {
            return Err(QuinnCurrentTerminalError::CompositionInvariant);
        };
        if self.current_terminal_stamp.is_none() {
            return Err(QuinnCurrentTerminalError::CompositionInvariant);
        }
        match self.controller.discard_terminating_current_recovery() {
            Ok(discarded) => {
                let applied_generation = discarded.generation();
                let applied_reason = discarded.reason();
                if applied_generation != generation || applied_reason != reason {
                    self.current_terminal = QuinnCurrentTerminalState::ConsumedFailed {
                        generation,
                        reason,
                        failure: QuinnCurrentTerminalConsumedFailure::AppliedIdentityInvariant,
                    };
                    return Err(QuinnCurrentTerminalError::CompositionInvariant);
                }
                self.current_terminal = QuinnCurrentTerminalState::Applied {
                    generation: applied_generation,
                    reason: applied_reason,
                };
                if self.current_loss_source == QuinnCurrentLossSource::None {
                    self.current_loss_source = QuinnCurrentLossSource::TransportTerminal;
                }
                Ok(QuinnCurrentTerminalDrive::Applied(discarded))
            }
            Err(kind) => {
                if kind != ClientCurrentTerminalDiscardErrorKind::RecoveryCleanupRejected {
                    self.current_terminal = QuinnCurrentTerminalState::ConsumedFailed {
                        generation,
                        reason,
                        failure: QuinnCurrentTerminalConsumedFailure::ControllerDiscard(kind),
                    };
                }
                Err(QuinnCurrentTerminalError::ControllerDiscard(kind))
            }
        }
    }

    /// After physical join the actor's terminal may still be in the joined
    /// witness because fatal publication won before the live handoff, or
    /// because explicit whole-session cancellation produced LocalCancelled.
    /// Move that exact optional capability through one shutdown-only
    /// controller transaction; rejection restores it inside the joined actor.
    fn clear_joined_current_terminal_for_shutdown(
        &mut self,
        gate: &QuinnLegCompositionGate,
    ) -> Result<
        (ClientCurrentShutdownCleared, ActorTerminalStampWitness),
        ClientCurrentShutdownClearErrorKind,
    > {
        let joined = self
            .current_joined
            .as_mut()
            .ok_or(ClientCurrentShutdownClearErrorKind::MissingTerminal)?;
        let terminal_stamp = joined
            .take_terminal_stamp()
            .ok_or(ClientCurrentShutdownClearErrorKind::MissingTerminal)?;
        let controller = &mut self.controller;
        let cleared = joined.transact_terminal(gate, |runtime, terminal| {
            match controller.clear_current_after_shutdown_join(
                runtime,
                terminal,
                self.fatal_recovery_tail_shutdown
                    .as_ref()
                    .map(|shutdown| &shutdown.gate),
                gate,
            ) {
                Ok(cleared) => Ok(cleared),
                Err(failure) => {
                    let kind = failure.kind();
                    Err((failure.into_terminal(), kind))
                }
            }
        });
        match cleared {
            Ok(cleared) => Ok((cleared, terminal_stamp)),
            Err(kind) => {
                if !joined.restore_terminal_stamp(terminal_stamp) {
                    return Err(ClientCurrentShutdownClearErrorKind::MissingTerminal);
                }
                Err(kind)
            }
        }
    }

    /// Drives at most one real current-actor inbound record through the exact
    /// controller policy slot. The recovery barrier is checked before actor
    /// dequeue; after dequeue the actor transaction is entirely synchronous.
    pub(crate) fn try_receive_current(&mut self) -> QuinnCurrentInboundDrive {
        if !self.current_policy_is_active() {
            return QuinnCurrentInboundDrive::SessionShuttingDown;
        }
        match self.controller.current_inbound_admission() {
            ClientCurrentInboundAdmission::PolicyBlocked => {
                return QuinnCurrentInboundDrive::PolicyBlocked;
            }
            ClientCurrentInboundAdmission::MissingCurrent => {
                return QuinnCurrentInboundDrive::MissingCurrent;
            }
            ClientCurrentInboundAdmission::Ready => {}
        }
        let gate = QuinnLegCompositionGate::mint();
        let current_matches = self
            .current
            .physical_incarnation(&gate)
            .is_some_and(|runtime| {
                self.controller
                    .current_physical_actor_matches(runtime, &gate)
            });
        if !current_matches {
            return QuinnCurrentInboundDrive::CompositionInvariant;
        }
        let controller = &mut self.controller;
        let transaction = self
            .current
            .try_transact_controller_inbound(&gate, |envelope| {
                let arrived_at = envelope.arrived_at();
                let (delivery, _) = envelope.into_parts();
                match controller
                    .receive_current_from_quinn(delivery, &gate, arrived_at)
                    .map_err(QuinnLegActorError::LegIo)?
                {
                    ClientInboundDisposition::Blocked(delivery) => {
                        Ok(QuinnLegActorInboundDecision::Blocked(
                            QuinnLegActorEncodedInbound::restore_delivery(delivery, arrived_at),
                        ))
                    }
                    disposition @ (ClientInboundDisposition::Applied { .. }
                    | ClientInboundDisposition::ProbeAcked { .. }
                    | ClientInboundDisposition::Rejected
                    | ClientInboundDisposition::ReducerRejected(_)) => {
                        Ok(QuinnLegActorInboundDecision::Classified(disposition))
                    }
                    ClientInboundDisposition::Retired(_) => Err(QuinnLegActorError::Protocol(
                        "current controller returned a retired disposition",
                    )),
                }
            });
        match transaction {
            Ok(QuinnLegActorInboundPoll::Idle) => QuinnCurrentInboundDrive::Idle,
            Ok(QuinnLegActorInboundPoll::Blocked) => QuinnCurrentInboundDrive::PolicyBlocked,
            Ok(QuinnLegActorInboundPoll::Classified(disposition)) => match disposition {
                ClientInboundDisposition::Applied {
                    effects,
                    observation,
                } => QuinnCurrentInboundDrive::Applied {
                    effects,
                    observation,
                },
                ClientInboundDisposition::ProbeAcked {
                    generation,
                    probe_sequence,
                } => QuinnCurrentInboundDrive::ProbeAcked {
                    generation,
                    probe_sequence,
                },
                ClientInboundDisposition::Rejected => QuinnCurrentInboundDrive::Rejected,
                ClientInboundDisposition::Retired(_) => {
                    QuinnCurrentInboundDrive::CompositionInvariant
                }
                ClientInboundDisposition::ReducerRejected(error) => {
                    QuinnCurrentInboundDrive::ReducerRejected(error)
                }
                ClientInboundDisposition::Blocked(_) => {
                    QuinnCurrentInboundDrive::CompositionInvariant
                }
            },
            Err(error) => QuinnCurrentInboundDrive::ActorFailed(error),
        }
    }

    /// Deterministically exercises the defensive post-dequeue blocked
    /// restoration. Production reaches the same branch only if a future
    /// controller predicate changes between the composition precheck and its
    /// synchronous receive result.
    #[cfg(test)]
    pub(super) fn force_current_raw_block_after_dequeue_for_test(
        &mut self,
    ) -> QuinnCurrentInboundDrive {
        if !self.current_policy_is_active() {
            return QuinnCurrentInboundDrive::SessionShuttingDown;
        }
        let gate = QuinnLegCompositionGate::mint();
        let current_matches = self
            .current
            .physical_incarnation(&gate)
            .is_some_and(|runtime| {
                self.controller
                    .current_physical_actor_matches(runtime, &gate)
            });
        if !current_matches {
            return QuinnCurrentInboundDrive::CompositionInvariant;
        }
        match self.current.try_transact_controller_inbound(
            &gate,
            |envelope| -> Result<_, QuinnLegActorError> {
                Ok(QuinnLegActorInboundDecision::<()>::Blocked(envelope))
            },
        ) {
            Ok(QuinnLegActorInboundPoll::Blocked) => QuinnCurrentInboundDrive::PolicyBlocked,
            Ok(QuinnLegActorInboundPoll::Idle) => QuinnCurrentInboundDrive::Idle,
            Ok(QuinnLegActorInboundPoll::Classified(_)) => {
                QuinnCurrentInboundDrive::CompositionInvariant
            }
            Err(error) => QuinnCurrentInboundDrive::ActorFailed(error),
        }
    }

    /// Injects one wrong physical route only after a real Quinn record has
    /// left the actor queue. This test-only seam exercises the production
    /// controller-owned LegIo error, first-failure, ACK, and cleanup path.
    #[cfg(test)]
    pub(super) fn force_current_wrong_route_after_dequeue_for_test(
        &mut self,
    ) -> QuinnCurrentInboundDrive {
        if !self.current_policy_is_active() {
            return QuinnCurrentInboundDrive::SessionShuttingDown;
        }
        match self.controller.current_inbound_admission() {
            ClientCurrentInboundAdmission::PolicyBlocked => {
                return QuinnCurrentInboundDrive::PolicyBlocked;
            }
            ClientCurrentInboundAdmission::MissingCurrent => {
                return QuinnCurrentInboundDrive::MissingCurrent;
            }
            ClientCurrentInboundAdmission::Ready => {}
        }
        let gate = QuinnLegCompositionGate::mint();
        let current_matches = self
            .current
            .physical_incarnation(&gate)
            .is_some_and(|runtime| {
                self.controller
                    .current_physical_actor_matches(runtime, &gate)
            });
        if !current_matches {
            return QuinnCurrentInboundDrive::CompositionInvariant;
        }
        let controller = &mut self.controller;
        let transaction = self.current.try_transact_controller_inbound(
            &gate,
            |envelope| -> Result<QuinnLegActorInboundDecision<()>, QuinnLegActorError> {
                let (delivery, _arrived_at) = envelope.into_parts();
                let route = delivery.route();
                let wrong_leg = match route.leg() {
                    LegId::A => LegId::B,
                    LegId::B => LegId::A,
                };
                let wrong_route = WireRoute::new(wrong_leg, route.direction());
                let lane = delivery.lane();
                let ordinal = delivery.send_ordinal();
                let wrong_delivery = EncodedDelivery::from_memory_ingress(
                    wrong_route,
                    lane,
                    delivery.into_bytes(),
                    ordinal,
                    0,
                )
                .map_err(|_| QuinnLegActorError::Protocol("test wrong-route rebuild failed"))?;
                match controller.receive_current_from_quinn(wrong_delivery, &gate, SimTime::ZERO) {
                    Err(error) => Err(QuinnLegActorError::LegIo(error)),
                    Ok(_disposition) => Err(QuinnLegActorError::Protocol(
                        "test wrong-route delivery unexpectedly classified",
                    )),
                }
            },
        );
        match transaction {
            Err(error) => QuinnCurrentInboundDrive::ActorFailed(error),
            Ok(QuinnLegActorInboundPoll::Idle) => QuinnCurrentInboundDrive::Idle,
            Ok(QuinnLegActorInboundPoll::Blocked) | Ok(QuinnLegActorInboundPoll::Classified(_)) => {
                QuinnCurrentInboundDrive::CompositionInvariant
            }
        }
    }

    /// Drives one still-running retired actor through the controller's exact
    /// inert late-inbound slot. Once retired join starts, this entry refuses
    /// to dequeue; cancellation/join owns the remaining actor queue instead.
    pub(crate) fn try_receive_retired(&mut self) -> QuinnRetiredInboundDrive {
        if self.phase != QuinnClientCompositionPhase::Running {
            return QuinnRetiredInboundDrive::SessionShuttingDown;
        }
        let gate = QuinnLegCompositionGate::mint();
        let Some(RetiredQuinnLegOwner::Running(retired)) = self.retired.as_mut() else {
            return if self.retired.is_none() {
                QuinnRetiredInboundDrive::MissingRetired
            } else {
                QuinnRetiredInboundDrive::NotRunning
            };
        };
        let retired_matches = retired.physical_incarnation(&gate).is_some_and(|runtime| {
            self.controller
                .retired_physical_actor_matches(runtime, &gate)
        });
        if !retired_matches {
            return QuinnRetiredInboundDrive::CompositionInvariant;
        }
        let controller = &mut self.controller;
        let transaction = retired.try_transact_controller_inbound(&gate, |envelope| {
            let (delivery, _arrived_at) = envelope.into_parts();
            match controller
                .receive_retired_from_quinn(delivery, &gate)
                .map_err(QuinnLegActorError::LegIo)?
            {
                disposition @ (ClientInboundDisposition::Retired(_)
                | ClientInboundDisposition::Rejected) => {
                    Ok(QuinnLegActorInboundDecision::Classified(disposition))
                }
                ClientInboundDisposition::Blocked(_)
                | ClientInboundDisposition::ProbeAcked { .. }
                | ClientInboundDisposition::Applied { .. }
                | ClientInboundDisposition::ReducerRejected(_) => {
                    Err(QuinnLegActorError::Protocol(
                        "retired controller returned an active disposition",
                    ))
                }
            }
        });
        match transaction {
            Ok(QuinnLegActorInboundPoll::Idle) => QuinnRetiredInboundDrive::Idle,
            Ok(QuinnLegActorInboundPoll::Blocked) => QuinnRetiredInboundDrive::CompositionInvariant,
            Ok(QuinnLegActorInboundPoll::Classified(disposition)) => match disposition {
                ClientInboundDisposition::Retired(kind) => QuinnRetiredInboundDrive::Retired(kind),
                ClientInboundDisposition::Rejected => QuinnRetiredInboundDrive::Rejected,
                ClientInboundDisposition::Blocked(_)
                | ClientInboundDisposition::ProbeAcked { .. }
                | ClientInboundDisposition::Applied { .. }
                | ClientInboundDisposition::ReducerRejected(_) => {
                    QuinnRetiredInboundDrive::CompositionInvariant
                }
            },
            Err(error) => QuinnRetiredInboundDrive::ActorFailed(error),
        }
    }

    pub(crate) fn current_routes(&self) -> Option<(WireRoute, WireRoute)> {
        Some((
            self.controller.current_inbound_route()?,
            self.controller.current_outbound_route()?,
        ))
    }

    pub(crate) fn retired_inbound_route(&self) -> Option<WireRoute> {
        self.controller.retired_inbound_route()
    }

    pub(crate) fn enqueue_current_frame(
        &mut self,
        frame: Frame,
    ) -> Result<(), QuinnCurrentEnqueueError> {
        if !self.current_policy_is_active() {
            return Err(QuinnCurrentEnqueueError::new(
                QuinnCurrentEnqueueErrorKind::SessionShuttingDown,
                frame,
            ));
        }
        let gate = QuinnLegCompositionGate::mint();
        let controller = &mut self.controller;
        match self.current.transact_controller_policy_with_input_result(
            &gate,
            frame,
            |_runtime, frame| controller.enqueue_current_frame_preserving(frame),
        ) {
            Ok(Ok(())) => Ok(()),
            Ok(Err(ClientCurrentFrameEnqueueFailure::MissingCurrent(frame))) => Err(
                QuinnCurrentEnqueueError::new(QuinnCurrentEnqueueErrorKind::MissingCurrent, frame),
            ),
            Ok(Err(ClientCurrentFrameEnqueueFailure::Queue(error))) => {
                let kind = Box::new(error.kind().clone());
                let frame = error.into_frame();
                Err(QuinnCurrentEnqueueError::new(
                    QuinnCurrentEnqueueErrorKind::Queue(kind),
                    frame,
                ))
            }
            Err((frame, actor)) => Err(QuinnCurrentEnqueueError::new(
                QuinnCurrentEnqueueErrorKind::ActorTransportUnavailable(
                    QuinnCurrentActorEnqueueUnavailableKind::from_actor_error(&actor),
                ),
                frame,
            )),
        }
    }

    pub(super) fn enqueue_current_reset_frame(
        &mut self,
        frame: Frame,
        reset_delivery: OrderedResetDeliveryReceipt,
    ) -> Result<(), QuinnCurrentEnqueueError> {
        if !self.current_policy_is_active() {
            return Err(QuinnCurrentEnqueueError::new(
                QuinnCurrentEnqueueErrorKind::SessionShuttingDown,
                frame,
            ));
        }
        let gate = QuinnLegCompositionGate::mint();
        let controller = &mut self.controller;
        match self.current.transact_controller_policy_with_input_result(
            &gate,
            (frame, reset_delivery),
            |_runtime, (frame, reset_delivery)| {
                controller.enqueue_current_reset_frame_preserving(frame, reset_delivery)
            },
        ) {
            Ok(Ok(())) => Ok(()),
            Ok(Err(ClientCurrentFrameEnqueueFailure::MissingCurrent(frame))) => Err(
                QuinnCurrentEnqueueError::new(QuinnCurrentEnqueueErrorKind::MissingCurrent, frame),
            ),
            Ok(Err(ClientCurrentFrameEnqueueFailure::Queue(error))) => {
                let kind = Box::new(error.kind().clone());
                let frame = error.into_frame();
                Err(QuinnCurrentEnqueueError::new(
                    QuinnCurrentEnqueueErrorKind::Queue(kind),
                    frame,
                ))
            }
            Err(((frame, _reset_delivery), actor)) => Err(QuinnCurrentEnqueueError::new(
                QuinnCurrentEnqueueErrorKind::ActorTransportUnavailable(
                    QuinnCurrentActorEnqueueUnavailableKind::from_actor_error(&actor),
                ),
                frame,
            )),
        }
    }

    /// Admits one genuinely ordered current-leg control record through the
    /// controller-owned FIFO while linearized against actor shutdown/failure.
    /// The opaque receipt exposes positive transport-delivery completion but
    /// no queue, seal, actor sender, or raw encoded-submission authority.
    pub(crate) fn begin_current_probe(
        &mut self,
    ) -> Result<QuinnCurrentProbeQueued, QuinnCurrentProbeBeginError> {
        if !self.current_policy_is_active() {
            return Err(QuinnCurrentProbeBeginError::SessionShuttingDown);
        }
        let gate = QuinnLegCompositionGate::mint();
        let current_matches = self
            .current
            .physical_incarnation(&gate)
            .is_some_and(|runtime| {
                self.controller
                    .current_physical_actor_matches(runtime, &gate)
            });
        if !current_matches {
            return Err(QuinnCurrentProbeBeginError::CompositionInvariant);
        }
        let controller = &mut self.controller;
        let queued = self
            .current
            .with_encoded_transport_mut(&gate, |_transport| controller.begin_current_probe(&gate))
            .ok_or(QuinnCurrentProbeBeginError::ActorTransportUnavailable)?
            .map_err(|error| QuinnCurrentProbeBeginError::Controller(error.kind()))?;
        Ok(QuinnCurrentProbeQueued(queued))
    }

    pub(crate) fn arm_current_probe(
        &mut self,
        queued: QuinnCurrentProbeQueued,
        now: SimTime,
        timeout: Duration,
    ) -> Result<QuinnCurrentProbeDeadline, QuinnCurrentProbeArmError> {
        if !self.current_policy_is_active() {
            return Err(QuinnCurrentProbeArmError::SessionShuttingDown(queued));
        }
        let gate = QuinnLegCompositionGate::mint();
        let controller = &mut self.controller;
        let armed = match self.current.transact_controller_policy_with_input_result(
            &gate,
            queued.0,
            |runtime, queued| {
                if !controller.current_physical_actor_matches(runtime, &gate) {
                    return Err(QuinnCurrentProbeArmError::CompositionInvariant(
                        QuinnCurrentProbeQueued(queued),
                    ));
                }
                controller
                    .arm_current_probe(queued, now, timeout)
                    .map_err(QuinnCurrentProbeArmError::from_controller)
            },
        ) {
            Ok(armed) => armed?,
            Err((queued, error)) => {
                return Err(QuinnCurrentProbeArmError::Actor {
                    queued: QuinnCurrentProbeQueued(queued),
                    error,
                });
            }
        };
        Ok(QuinnCurrentProbeDeadline(armed))
    }

    /// Synchronously moves the root scheduler's exact queued/armed probe
    /// owner into the composition before any shutdown await can be created.
    /// A wrong, stale, duplicate, or phase-mismatched owner is returned
    /// untouched; successful retention is bounded to one slot.
    pub(crate) fn retain_current_probe_scheduler_for_shutdown(
        &mut self,
        authority: QuinnCurrentProbeSchedulerAuthority,
    ) -> Result<(), QuinnCurrentProbeSchedulerAuthority> {
        if self.shutdown_probe_scheduler.is_some() {
            return Err(authority);
        }
        let matches = match &authority {
            QuinnCurrentProbeSchedulerAuthority::Queued(queued) => self
                .controller
                .current_or_pending_probe_queued_matches(&queued.0),
            QuinnCurrentProbeSchedulerAuthority::Armed(deadline) => self
                .controller
                .current_or_pending_probe_deadline_matches(&deadline.0),
        };
        if !matches {
            return Err(authority);
        }
        self.shutdown_probe_scheduler = Some(authority);
        Ok(())
    }

    /// Reconciles one ACK winner only after the scheduler returns the exact
    /// queued/armed authority it retained outside the controller. Failure
    /// preserves that complete authority for retry or structured shutdown.
    pub(crate) fn reconcile_current_probe_ack(
        &mut self,
        scheduler: QuinnCurrentProbeSchedulerAuthority,
        generation: crate::resumable::LegGeneration,
        probe_sequence: crate::resumable::ProbeSequence,
    ) -> Result<ClientCurrentProbeAckSettlement, QuinnCurrentProbeAckReconcileFailure> {
        self.controller
            .reconcile_current_probe_ack(scheduler.into_controller(), generation, probe_sequence)
            .map_err(|failure| {
                let kind = failure.kind();
                let scheduler =
                    QuinnCurrentProbeSchedulerAuthority::from_controller(failure.into_scheduler());
                QuinnCurrentProbeAckReconcileFailure { scheduler, kind }
            })
    }

    /// Copy-only selector for the exact root scheduler authority retained by a
    /// terminal observation.  The returned correlation cannot settle or
    /// recreate the non-cloneable queued/deadline owner.
    pub(crate) fn pending_current_observation_probe_key(
        &self,
    ) -> Option<(
        crate::resumable::LegGeneration,
        crate::resumable::ProbeSequence,
    )> {
        self.controller.pending_current_observation_probe_key()
    }

    /// Completes a live terminal observation only when the root returns the
    /// exact scheduler authority selected above.  Every rejection returns that
    /// full authority, preserving queued/deadline identity for retry or the
    /// later structured-shutdown handoff.
    pub(crate) fn settle_pending_current_observation(
        &mut self,
        scheduler: QuinnCurrentProbeSchedulerAuthority,
    ) -> Result<ClientControllerObservationSettlement, QuinnCurrentObservationSettlementFailure>
    {
        self.controller
            .settle_pending_current_observation_with_scheduler(scheduler.into_controller())
            .map_err(QuinnCurrentObservationSettlementFailure::from_controller)
    }

    /// Performs one bounded receive-before-expiry arbitration for a due
    /// current probe. The actor first consumes a record already published at
    /// its inbound boundary. If a complete record is still entering that
    /// boundary, this returns the exact token and relies on the paired
    /// readiness publication for the owner's next scan. Only an observed
    /// Empty boundary with no such producer can commit the deadline.
    ///
    /// `received_at` is root scheduling evidence used only to decide that the
    /// timer is due. A complete record beats the timer only when its immutable
    /// actor-captured arrival is at or before the token's absolute deadline;
    /// publication epochs close the queue-Empty/in-flight race.
    pub(crate) fn try_receive_current_before_probe_deadline(
        &mut self,
        token: QuinnCurrentProbeDeadline,
        received_at: SimTime,
    ) -> QuinnCurrentProbeDeadlineDrive {
        let QuinnCurrentProbeDeadline(token) = token;
        match self
            .controller
            .current_probe_deadline_status(&token, received_at)
        {
            ClientCurrentProbeDeadlineStatus::Stale => {
                return QuinnCurrentProbeDeadlineDrive::Stale(QuinnCurrentProbeDeadline(token));
            }
            ClientCurrentProbeDeadlineStatus::AckWon {
                generation,
                probe_sequence,
            } => {
                return QuinnCurrentProbeDeadlineDrive::ProbeAcked {
                    deadline: QuinnCurrentProbeDeadline(token),
                    generation,
                    probe_sequence,
                };
            }
            ClientCurrentProbeDeadlineStatus::NotDue => {
                return QuinnCurrentProbeDeadlineDrive::NotDue(QuinnCurrentProbeDeadline(token));
            }
            ClientCurrentProbeDeadlineStatus::Due => {}
        }
        if !self.current_policy_is_active() {
            return QuinnCurrentProbeDeadlineDrive::SessionShuttingDown(QuinnCurrentProbeDeadline(
                token,
            ));
        }
        match self.controller.current_inbound_admission() {
            ClientCurrentInboundAdmission::PolicyBlocked => {
                return QuinnCurrentProbeDeadlineDrive::Receive {
                    outcome: QuinnCurrentInboundDrive::PolicyBlocked,
                    deadline: QuinnCurrentProbeDeadline(token),
                };
            }
            ClientCurrentInboundAdmission::MissingCurrent => {
                return QuinnCurrentProbeDeadlineDrive::CompositionInvariant(
                    QuinnCurrentProbeDeadline(token),
                );
            }
            ClientCurrentInboundAdmission::Ready => {}
        }

        let gate = QuinnLegCompositionGate::mint();
        let current_matches = self
            .current
            .physical_incarnation(&gate)
            .is_some_and(|runtime| {
                self.controller
                    .current_physical_actor_matches(runtime, &gate)
            });
        if !current_matches {
            return QuinnCurrentProbeDeadlineDrive::CompositionInvariant(
                QuinnCurrentProbeDeadline(token),
            );
        }

        let controller = &mut self.controller;
        let arbitration = self.current.try_transact_inbound_before_probe_deadline(
            &gate,
            token,
            controller,
            |controller, runtime, envelope, deadline| {
                if !controller.current_physical_actor_matches(runtime, &gate) {
                    return Ok((QuinnLegActorInboundDecision::Blocked(envelope), deadline));
                }
                let arrived_at = envelope.arrived_at();
                let (delivery, _) = envelope.into_parts();
                let decision =
                    match controller.receive_current_from_quinn(delivery, &gate, arrived_at) {
                        Ok(ClientInboundDisposition::Blocked(delivery)) => {
                            QuinnLegActorInboundDecision::Blocked(
                                QuinnLegActorEncodedInbound::restore_delivery(delivery, arrived_at),
                            )
                        }
                        Ok(
                            disposition @ (ClientInboundDisposition::Applied { .. }
                            | ClientInboundDisposition::ProbeAcked { .. }
                            | ClientInboundDisposition::Rejected
                            | ClientInboundDisposition::ReducerRejected(_)),
                        ) => QuinnLegActorInboundDecision::Classified(disposition),
                        Ok(ClientInboundDisposition::Retired(_)) => {
                            return Err((
                                deadline,
                                QuinnLegActorError::Protocol(
                                    "current controller returned a retired disposition",
                                ),
                            ));
                        }
                        Err(error) => return Err((deadline, QuinnLegActorError::LegIo(error))),
                    };
                Ok((decision, deadline))
            },
            |controller, runtime, deadline| {
                if !controller.current_physical_actor_matches(runtime, &gate) {
                    return Err(deadline);
                }
                controller.commit_due_current_probe(deadline, received_at)
            },
        );

        match arbitration {
            QuinnLegActorInboundDeadlinePoll::Inbound { poll, timer } => {
                let deadline = QuinnCurrentProbeDeadline(timer);
                match poll {
                    QuinnLegActorInboundPoll::Idle => {
                        QuinnCurrentProbeDeadlineDrive::CompositionInvariant(deadline)
                    }
                    QuinnLegActorInboundPoll::Blocked => QuinnCurrentProbeDeadlineDrive::Receive {
                        outcome: QuinnCurrentInboundDrive::PolicyBlocked,
                        deadline,
                    },
                    QuinnLegActorInboundPoll::Classified(disposition) => match disposition {
                        ClientInboundDisposition::ProbeAcked {
                            generation,
                            probe_sequence,
                        } => QuinnCurrentProbeDeadlineDrive::ProbeAcked {
                            deadline,
                            generation,
                            probe_sequence,
                        },
                        ClientInboundDisposition::Applied {
                            effects,
                            observation,
                        } => QuinnCurrentProbeDeadlineDrive::Receive {
                            outcome: QuinnCurrentInboundDrive::Applied {
                                effects,
                                observation,
                            },
                            deadline,
                        },
                        ClientInboundDisposition::Rejected => {
                            QuinnCurrentProbeDeadlineDrive::Receive {
                                outcome: QuinnCurrentInboundDrive::Rejected,
                                deadline,
                            }
                        }
                        ClientInboundDisposition::ReducerRejected(error) => {
                            QuinnCurrentProbeDeadlineDrive::Receive {
                                outcome: QuinnCurrentInboundDrive::ReducerRejected(error),
                                deadline,
                            }
                        }
                        ClientInboundDisposition::Retired(_)
                        | ClientInboundDisposition::Blocked(_) => {
                            QuinnCurrentProbeDeadlineDrive::CompositionInvariant(deadline)
                        }
                    },
                }
            }
            QuinnLegActorInboundDeadlinePoll::PublicationPending(deadline) => {
                QuinnCurrentProbeDeadlineDrive::PublicationPending(QuinnCurrentProbeDeadline(
                    deadline,
                ))
            }
            QuinnLegActorInboundDeadlinePoll::TimerCommitted(expired) => {
                let generation = expired.generation();
                let probe_sequence = expired.probe_sequence();
                self.current_loss_source = QuinnCurrentLossSource::ProbeDeadline {
                    generation,
                    probe_sequence,
                };
                QuinnCurrentProbeDeadlineDrive::Committed {
                    generation,
                    probe_sequence,
                }
            }
            QuinnLegActorInboundDeadlinePoll::TimerRejected(deadline) => {
                QuinnCurrentProbeDeadlineDrive::CompositionInvariant(QuinnCurrentProbeDeadline(
                    deadline,
                ))
            }
            QuinnLegActorInboundDeadlinePoll::Suppressed { timer, cause }
            | QuinnLegActorInboundDeadlinePoll::Failed { timer, cause } => {
                QuinnCurrentProbeDeadlineDrive::ActorFailureWon {
                    deadline: QuinnCurrentProbeDeadline(timer),
                    cause,
                }
            }
            QuinnLegActorInboundDeadlinePoll::TimerInvariant { error, .. } => match error {},
            QuinnLegActorInboundDeadlinePoll::LocalShutdown(deadline) => {
                QuinnCurrentProbeDeadlineDrive::SessionShuttingDown(QuinnCurrentProbeDeadline(
                    deadline,
                ))
            }
        }
    }

    /// Performs one bounded current-inbound-before-terminal-grace turn once
    /// root scheduling observes the timer due. A complete record wins only
    /// when its immutable actor-captured arrival is at or before the absolute
    /// grace deadline; publication epochs close the queue-Empty/in-flight
    /// race. Successful expiry is session semantics and neither fails nor
    /// cancels the physical actor.
    pub(crate) fn try_receive_current_before_terminal_grace(
        &mut self,
        timer: QuinnTerminalGraceTimer,
        observed_at: SimTime,
    ) -> QuinnTerminalGraceTimerDrive {
        if observed_at < timer.deadline {
            return QuinnTerminalGraceTimerDrive::NotDue(timer);
        }
        let mut timer = timer;
        self.current
            .capture_semantic_timer_inbound_cutoff(&mut timer);
        if !self.current_policy_is_active() {
            return QuinnTerminalGraceTimerDrive::SessionShuttingDown(timer);
        }
        match self.controller.current_inbound_admission() {
            ClientCurrentInboundAdmission::PolicyBlocked => {
                return QuinnTerminalGraceTimerDrive::Receive {
                    outcome: QuinnCurrentInboundDrive::PolicyBlocked,
                    timer,
                };
            }
            ClientCurrentInboundAdmission::MissingCurrent => {
                return QuinnTerminalGraceTimerDrive::CompositionInvariant(timer);
            }
            ClientCurrentInboundAdmission::Ready => {}
        }

        let gate = QuinnLegCompositionGate::mint();
        let current_matches = self
            .current
            .physical_incarnation(&gate)
            .is_some_and(|runtime| {
                self.controller
                    .current_physical_actor_matches(runtime, &gate)
            });
        if !current_matches {
            return QuinnTerminalGraceTimerDrive::CompositionInvariant(timer);
        }

        let controller = &mut self.controller;
        let arbitration = self.current.try_transact_inbound_before_semantic_timer(
            &gate,
            timer,
            controller,
            |controller, runtime, envelope, timer| {
                if !controller.current_physical_actor_matches(runtime, &gate) {
                    return Ok((QuinnLegActorInboundDecision::Blocked(envelope), timer));
                }
                let arrived_at = envelope.arrived_at();
                let (delivery, _) = envelope.into_parts();
                let decision =
                    match controller.receive_current_from_quinn(delivery, &gate, arrived_at) {
                        Ok(ClientInboundDisposition::Blocked(delivery)) => {
                            QuinnLegActorInboundDecision::Blocked(
                                QuinnLegActorEncodedInbound::restore_delivery(delivery, arrived_at),
                            )
                        }
                        Ok(
                            disposition @ (ClientInboundDisposition::Applied { .. }
                            | ClientInboundDisposition::ProbeAcked { .. }
                            | ClientInboundDisposition::Rejected
                            | ClientInboundDisposition::ReducerRejected(_)),
                        ) => QuinnLegActorInboundDecision::Classified(disposition),
                        Ok(ClientInboundDisposition::Retired(_)) => {
                            return Err((
                                timer,
                                QuinnLegActorError::Protocol(
                                    "current controller returned a retired disposition",
                                ),
                            ));
                        }
                        Err(error) => return Err((timer, QuinnLegActorError::LegIo(error))),
                    };
                Ok((decision, timer))
            },
            |controller, runtime, timer| {
                if !controller.current_physical_actor_matches(runtime, &gate) {
                    return QuinnLegActorTimerDecision::Rejected(timer);
                }
                if timer.try_commit_reset_delivery_deadline()
                    == OrderedResetDeadlineCommit::DeadlineWon
                {
                    return QuinnLegActorTimerDecision::Rejected(timer);
                }
                match timer.expire(controller) {
                    Ok(effects) => QuinnLegActorTimerDecision::Committed(effects),
                    Err((timer, error)) => QuinnLegActorTimerDecision::Invariant { timer, error },
                }
            },
        );

        match arbitration {
            QuinnLegActorInboundTimerPoll::Inbound { poll, timer } => match poll {
                QuinnLegActorInboundPoll::Idle => {
                    QuinnTerminalGraceTimerDrive::CompositionInvariant(timer)
                }
                QuinnLegActorInboundPoll::Blocked => QuinnTerminalGraceTimerDrive::Receive {
                    outcome: QuinnCurrentInboundDrive::PolicyBlocked,
                    timer,
                },
                QuinnLegActorInboundPoll::Classified(disposition) => {
                    let outcome = match disposition {
                        ClientInboundDisposition::Applied {
                            effects,
                            observation,
                        } => QuinnCurrentInboundDrive::Applied {
                            effects,
                            observation,
                        },
                        ClientInboundDisposition::ProbeAcked {
                            generation,
                            probe_sequence,
                        } => QuinnCurrentInboundDrive::ProbeAcked {
                            generation,
                            probe_sequence,
                        },
                        ClientInboundDisposition::Rejected => QuinnCurrentInboundDrive::Rejected,
                        ClientInboundDisposition::ReducerRejected(error) => {
                            QuinnCurrentInboundDrive::ReducerRejected(error)
                        }
                        ClientInboundDisposition::Retired(_)
                        | ClientInboundDisposition::Blocked(_) => {
                            return QuinnTerminalGraceTimerDrive::CompositionInvariant(timer);
                        }
                    };
                    QuinnTerminalGraceTimerDrive::Receive { outcome, timer }
                }
            },
            QuinnLegActorInboundTimerPoll::PublicationPending(timer) => {
                QuinnTerminalGraceTimerDrive::PublicationPending(timer)
            }
            QuinnLegActorInboundTimerPoll::TimerCommitted(effects) => {
                QuinnTerminalGraceTimerDrive::Expired { effects }
            }
            QuinnLegActorInboundTimerPoll::TimerRejected(timer) => {
                if timer.reset_delivery_deadline_won() {
                    QuinnTerminalGraceTimerDrive::ResetDeliveryDeadlineWon(timer)
                } else {
                    QuinnTerminalGraceTimerDrive::CompositionInvariant(timer)
                }
            }
            QuinnLegActorInboundTimerPoll::TimerInvariant { timer, error } => {
                QuinnTerminalGraceTimerDrive::ReducerInvariant { timer, error }
            }
            QuinnLegActorInboundTimerPoll::Suppressed { timer, cause }
            | QuinnLegActorInboundTimerPoll::Failed { timer, cause } => {
                QuinnTerminalGraceTimerDrive::ActorFailure { timer, cause }
            }
            QuinnLegActorInboundTimerPoll::LocalShutdown(timer) => {
                QuinnTerminalGraceTimerDrive::SessionShuttingDown(timer)
            }
        }
    }

    /// During whole-session shutdown no future replacement can use a terminal
    /// tombstone. Consume the exact capability immediately rather than making
    /// actor cleanup wait for an arbitrarily long configured grace duration.
    /// This remains ownership-preserving on reducer invariant failure.
    pub(super) fn force_expire_terminal_grace_for_root_shutdown(
        &mut self,
        timer: QuinnTerminalGraceTimer,
    ) -> QuinnTerminalGraceTimerDrive {
        if !self.controller.external_flow_admission_is_closed() {
            return QuinnTerminalGraceTimerDrive::CompositionInvariant(timer);
        }
        match timer.expire(&mut self.controller) {
            Ok(effects) => QuinnTerminalGraceTimerDrive::Expired { effects },
            Err((timer, error)) => QuinnTerminalGraceTimerDrive::ReducerInvariant { timer, error },
        }
    }

    #[cfg(test)]
    pub(crate) fn try_expire_current_probe(
        &mut self,
        token: QuinnCurrentProbeDeadline,
        now: SimTime,
    ) -> QuinnCurrentProbeExpiry {
        let QuinnCurrentProbeDeadline(token) = token;
        match self.controller.current_probe_deadline_status(&token, now) {
            ClientCurrentProbeDeadlineStatus::Stale => {
                return QuinnCurrentProbeExpiry::Stale(QuinnCurrentProbeDeadline(token));
            }
            ClientCurrentProbeDeadlineStatus::AckWon {
                generation,
                probe_sequence,
            } => {
                return QuinnCurrentProbeExpiry::ProbeAcked {
                    deadline: QuinnCurrentProbeDeadline(token),
                    generation,
                    probe_sequence,
                };
            }
            ClientCurrentProbeDeadlineStatus::NotDue => {
                return QuinnCurrentProbeExpiry::NotDue(QuinnCurrentProbeDeadline(token));
            }
            ClientCurrentProbeDeadlineStatus::Due => {}
        }
        if !self.current_policy_is_active() {
            return QuinnCurrentProbeExpiry::SessionShuttingDown(QuinnCurrentProbeDeadline(token));
        }
        let gate = QuinnLegCompositionGate::mint();
        let controller = &mut self.controller;
        match self
            .current
            .commit_current_probe_deadline(&gate, token, |runtime, token| {
                if !controller.current_physical_actor_matches(runtime, &gate) {
                    return Err(token);
                }
                controller.commit_due_current_probe(token, now)
            }) {
            QuinnLegActorProbeDeadlineCommit::Committed(expired) => {
                let generation = expired.generation();
                let probe_sequence = expired.probe_sequence();
                self.current_loss_source = QuinnCurrentLossSource::ProbeDeadline {
                    generation,
                    probe_sequence,
                };
                QuinnCurrentProbeExpiry::Committed {
                    generation,
                    probe_sequence,
                }
            }
            QuinnLegActorProbeDeadlineCommit::Rejected(token) => {
                QuinnCurrentProbeExpiry::CompositionInvariant(QuinnCurrentProbeDeadline(token))
            }
            QuinnLegActorProbeDeadlineCommit::Suppressed { input, cause } => {
                QuinnCurrentProbeExpiry::ActorFailureWon {
                    deadline: QuinnCurrentProbeDeadline(input),
                    cause,
                }
            }
        }
    }

    #[cfg(test)]
    pub(super) fn enqueue_current_ordered_leg_control(
        &mut self,
        frame: LegControlFrame,
        features: FeatureSet,
    ) -> Result<QuinnCurrentOrderedControlEnqueued, QuinnCurrentControlEnqueueError> {
        if !self.current_policy_is_active() {
            return Err(QuinnCurrentControlEnqueueError::SessionShuttingDown);
        }
        let gate = QuinnLegCompositionGate::mint();
        let admitted = self
            .current
            .with_encoded_transport_mut(&gate, |_transport| {
                self.controller
                    .enqueue_current_ordered_leg_control(frame, features, &gate)
            });
        let admitted =
            admitted.ok_or(QuinnCurrentControlEnqueueError::ActorTransportUnavailable)?;
        let admitted = admitted.ok_or(QuinnCurrentControlEnqueueError::MissingCurrent)?;
        admitted
            .map(QuinnCurrentOrderedControlEnqueued)
            .map_err(|error| QuinnCurrentControlEnqueueError::Queue(Box::new(error)))
    }

    pub(crate) fn try_flush_current(
        &mut self,
        now: SimTime,
    ) -> Result<QuinnCurrentFlushDisposition, QuinnCurrentFlushError> {
        if !self.current_policy_is_active() {
            return Err(QuinnCurrentFlushError::SessionShuttingDown);
        }
        let gate = QuinnLegCompositionGate::mint();
        let result = self
            .current
            .with_encoded_transport_mut(&gate, |transport| {
                self.controller.try_flush_current(transport, now)
            })
            .ok_or(QuinnCurrentFlushError::ActorTransportUnavailable)?;
        match result {
            Ok(Some(_submission)) => Ok(QuinnCurrentFlushDisposition::Submitted),
            Ok(None) => Ok(QuinnCurrentFlushDisposition::Idle),
            Err(LegIoError::OrderedDeliveryPending) => {
                Ok(QuinnCurrentFlushDisposition::OrderedDeliveryPending)
            }
            Err(error) => Err(QuinnCurrentFlushError::LegIo(error)),
        }
    }

    /// Checks both actor/controller bindings before cancellation, then moves
    /// only retired A into one composition-owned cleanup task. The current B
    /// actor and controller stay in place and remain driveable while the
    /// bounded close tail runs.
    pub(crate) fn begin_retired_join(
        &mut self,
    ) -> Result<QuinnRetiredJoinStartDisposition, QuinnRetiredJoinClearError> {
        if self.phase != QuinnClientCompositionPhase::Running {
            return Err(QuinnRetiredJoinClearError::SessionShuttingDown);
        }
        let gate = QuinnLegCompositionGate::mint();
        let Some(retired) = self.retired.as_ref() else {
            return Err(QuinnRetiredJoinClearError::MissingRetired);
        };
        match retired {
            RetiredQuinnLegOwner::Joining(_) => {
                return Ok(QuinnRetiredJoinStartDisposition::AlreadyJoining);
            }
            RetiredQuinnLegOwner::Joined(_) => {
                return Ok(QuinnRetiredJoinStartDisposition::AlreadyJoined);
            }
            RetiredQuinnLegOwner::TaskLost(cause) => {
                return Err(QuinnRetiredJoinClearError::JoinTask(Arc::clone(cause)));
            }
            RetiredQuinnLegOwner::Running(_) => {}
        }
        if !self.layout_matches_controller(&gate) {
            return Err(QuinnRetiredJoinClearError::CompositionInvariant);
        }
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| QuinnRetiredJoinClearError::RuntimeUnavailable)?;
        let Some(RetiredQuinnLegOwner::Running(actor)) = self.retired.take() else {
            return Err(QuinnRetiredJoinClearError::CompositionInvariant);
        };
        let task = spawn_retired_join(&runtime, *actor);
        self.retired = Some(RetiredQuinnLegOwner::Joining(task));
        Ok(QuinnRetiredJoinStartDisposition::Started)
    }

    /// Non-blocking join drive. `Pending` retains the structured cleanup task
    /// inside the composition, so dropping a caller-side polling future cannot
    /// lose A or release capacity. Once the task is ready, joined persistence
    /// and controller clear run synchronously in this call.
    pub(crate) fn try_finish_retired_join(
        &mut self,
    ) -> Result<QuinnRetiredJoinDrive, QuinnRetiredJoinClearError> {
        if self.phase != QuinnClientCompositionPhase::Running {
            return Err(QuinnRetiredJoinClearError::SessionShuttingDown);
        }
        let Some(retired) = self.retired.take() else {
            return Err(QuinnRetiredJoinClearError::MissingRetired);
        };
        match retired {
            RetiredQuinnLegOwner::Running(actor) => {
                self.retired = Some(RetiredQuinnLegOwner::Running(actor));
                Err(QuinnRetiredJoinClearError::NotStarted)
            }
            RetiredQuinnLegOwner::Joining(mut task) => match poll_finished_join(&mut task) {
                Poll::Pending => {
                    self.retired = Some(RetiredQuinnLegOwner::Joining(task));
                    Ok(QuinnRetiredJoinDrive::Pending)
                }
                Poll::Ready(Err(error)) => {
                    let cause: Arc<str> = Arc::from(error.to_string());
                    self.retired = Some(RetiredQuinnLegOwner::TaskLost(Arc::clone(&cause)));
                    Err(QuinnRetiredJoinClearError::JoinTask(cause))
                }
                Poll::Ready(Ok(RetiredQuinnLegJoinOutcome { actor, joined })) => match joined {
                    Ok(joined) => {
                        drop(actor);
                        self.retired = Some(RetiredQuinnLegOwner::Joined(joined));
                        self.commit_joined_retired()
                    }
                    Err(error) => {
                        self.retired = Some(RetiredQuinnLegOwner::Running(Box::new(actor)));
                        Err(QuinnRetiredJoinClearError::Actor(error))
                    }
                },
            },
            RetiredQuinnLegOwner::Joined(joined) => {
                self.retired = Some(RetiredQuinnLegOwner::Joined(joined));
                self.commit_joined_retired()
            }
            RetiredQuinnLegOwner::TaskLost(cause) => {
                self.retired = Some(RetiredQuinnLegOwner::TaskLost(Arc::clone(&cause)));
                Err(QuinnRetiredJoinClearError::JoinTask(cause))
            }
        }
    }

    fn commit_joined_retired(
        &mut self,
    ) -> Result<QuinnRetiredJoinDrive, QuinnRetiredJoinClearError> {
        let gate = QuinnLegCompositionGate::mint();
        if !self.layout_matches_controller(&gate) {
            return Err(QuinnRetiredJoinClearError::CompositionInvariant);
        }
        let Some(RetiredQuinnLegOwner::Joined(joined)) = self.retired.as_mut() else {
            return Err(QuinnRetiredJoinClearError::MissingRetired);
        };
        let owned_messages = joined.owned_messages();
        let owned_bytes = joined.owned_bytes();
        let terminal_failure = joined.terminal_failure().is_some();
        let cleared = joined.transact_terminal(&gate, |runtime, terminal| {
            match self
                .controller
                .clear_retired_after_physical_join(runtime, terminal, &gate)
            {
                Ok(cleared) => Ok(cleared),
                Err(failure) => {
                    let kind = failure.kind();
                    Err((failure.into_terminal(), kind))
                }
            }
        });
        let cleared = match cleared {
            Ok(cleared) => cleared,
            Err(kind) => {
                return Err(QuinnRetiredJoinClearError::Controller(kind.into()));
            }
        };
        let joined = self.retired.take();
        match joined {
            Some(RetiredQuinnLegOwner::Joined(joined)) => drop(joined),
            Some(other) => {
                self.retired = Some(other);
                return Err(QuinnRetiredJoinClearError::CompositionInvariant);
            }
            None => return Err(QuinnRetiredJoinClearError::MissingRetired),
        }
        Ok(QuinnRetiredJoinDrive::Cleared(RetiredQuinnActorCleared {
            generation: cleared.generation(),
            owned_messages,
            owned_bytes,
            terminal_failure,
        }))
    }

    /// Starts all independently owned cleanup first, then structurally joins
    /// current, standby, and retired actors without letting one permanent
    /// cleanup error short-circuit the others. The current joined witness stays
    /// inside this composition beside the controller until the root is dropped.
    pub(crate) async fn join_current_for_shutdown(
        &mut self,
    ) -> Result<CurrentQuinnActorJoined, QuinnCurrentJoinError> {
        // Preserve the fail-fast composition contract used by direct
        // controller tests and recovery code. The production root calls the
        // join-all variant below after its stricter quiescent start gate.
        let _closed_now = self.controller.close_tcp_factory_admission();
        self.settle_recovery_tail_before_shutdown()?;
        if matches!(
            self.current_terminal,
            QuinnCurrentTerminalState::ControllerBound { .. }
        ) {
            return Err(QuinnCurrentJoinError::CurrentTerminalDiscardPending);
        }
        let gate = QuinnLegCompositionGate::mint();
        if !self.shutdown_layout_matches_controller(&gate) {
            return Err(QuinnCurrentJoinError::CompositionInvariant);
        }
        let retired_runtime = match self.retired.as_ref() {
            Some(RetiredQuinnLegOwner::Running(_)) => Some(
                tokio::runtime::Handle::try_current()
                    .map_err(|_| QuinnCurrentJoinError::RuntimeUnavailable)?,
            ),
            Some(RetiredQuinnLegOwner::Joining(_))
            | Some(RetiredQuinnLegOwner::Joined(_))
            | Some(RetiredQuinnLegOwner::TaskLost(_))
            | None => None,
        };
        let standby_runtime = match self.standby.as_ref().and_then(|owner| owner.actor.as_ref()) {
            Some(QuinnStandbyActorOwner::Running(_)) => Some(
                tokio::runtime::Handle::try_current()
                    .map_err(|_| QuinnCurrentJoinError::RuntimeUnavailable)?,
            ),
            Some(QuinnStandbyActorOwner::Joining(_))
            | Some(QuinnStandbyActorOwner::Joined(_))
            | Some(QuinnStandbyActorOwner::TaskLost(_))
            | None => None,
        };
        if self.phase == QuinnClientCompositionPhase::Running {
            match self.current_terminal {
                QuinnCurrentTerminalState::Unbound => {
                    let phase = &mut self.phase;
                    let controller = &self.controller;
                    self.current
                        .transact_controller_shutdown(&gate, |runtime| {
                            if !controller.current_physical_actor_matches(runtime, &gate) {
                                return None;
                            }
                            *phase = QuinnClientCompositionPhase::ShuttingDown;
                            Some(())
                        })
                        .map_err(QuinnCurrentJoinError::Actor)?
                        .ok_or(QuinnCurrentJoinError::CompositionInvariant)?;
                }
                QuinnCurrentTerminalState::Applied { .. }
                | QuinnCurrentTerminalState::ConsumedFailed { .. }
                | QuinnCurrentTerminalState::ShutdownCleared { .. } => {
                    self.phase = QuinnClientCompositionPhase::ShuttingDown;
                }
                QuinnCurrentTerminalState::ControllerBound { .. } => {
                    return Err(QuinnCurrentJoinError::CurrentTerminalDiscardPending);
                }
            }
        }
        let mut cleanup_error = self
            .fatal_recovery_tail_shutdown
            .as_ref()
            .map(QuinnFatalRecoveryTailShutdown::join_error);
        if let Err(error) = self.start_retired_join_for_shutdown(&gate, retired_runtime.as_ref()) {
            cleanup_error = Some(error);
        }
        if let Err(error) = self.start_standby_join_for_shutdown(&gate, standby_runtime.as_ref()) {
            cleanup_error.get_or_insert(error);
        }
        if self.current_joined.is_none() {
            self.current.request_cancel();
            match self.current.join().await {
                Ok(joined) => self.current_joined = Some(joined),
                Err(error) => {
                    cleanup_error.get_or_insert(QuinnCurrentJoinError::Actor(error));
                }
            }
        }
        // This legacy/direct API is a structural actor join. Controller-owned
        // recovery tails, probe receipts, pending application ACK authority,
        // and the exact joined terminal stay inside the composition until its
        // owner drops it. Only the root-facing join-all transaction below may
        // require a pristine controller and consume that terminal into a
        // zero-ownership production witness.
        if let Err(error) = self.join_standby_for_shutdown(&gate).await {
            cleanup_error.get_or_insert(error);
        }
        if let Err(error) = self.join_retired_for_shutdown(&gate).await {
            cleanup_error.get_or_insert(error);
        }
        if let Some(error) = cleanup_error {
            return Err(error);
        }
        if !self.standby_shutdown_is_clean() {
            return Err(QuinnCurrentJoinError::StandbyJoinedWitnessMissing);
        }
        self.phase = QuinnClientCompositionPhase::Joined;
        if !self.layout_matches_controller(&gate) {
            return Err(QuinnCurrentJoinError::CompositionInvariant);
        }
        let joined = self
            .current_joined
            .as_ref()
            .ok_or(QuinnCurrentJoinError::JoinedWitnessMissing)?;
        if let QuinnCurrentTerminalState::ConsumedFailed { failure, .. } = self.current_terminal {
            return Err(failure.into_join_error());
        }
        Ok(CurrentQuinnActorJoined {
            owned_messages: joined.owned_messages(),
            owned_bytes: joined.owned_bytes(),
            owned_terminals: joined.owned_terminals(),
            terminal_failure: joined.terminal_failure().is_some(),
        })
    }

    /// Root-facing shutdown transaction. Unlike the legacy result wrapper,
    /// this preserves a zero-ownership current witness beside the first sticky
    /// semantic/cleanup error. Actor fatal, controller-bound terminal cleanup,
    /// and layout rejection therefore cannot short-circuit structural joins.
    fn controller_ownership_ledger(&self) -> ClientControllerOwnershipLedger {
        let scheduler_authorities = usize::from(self.shutdown_probe_scheduler.is_some());
        let scheduler_deadlines = usize::from(
            self.shutdown_probe_scheduler
                .as_ref()
                .is_some_and(QuinnCurrentProbeSchedulerAuthority::is_deadline),
        );
        self.controller
            .ownership_ledger(scheduler_authorities, scheduler_deadlines)
    }

    fn settle_pending_controller_observation_for_shutdown(
        &mut self,
    ) -> Result<(), QuinnCurrentJoinError> {
        if !self.controller.observation_settlement_pending() {
            return if self.shutdown_probe_scheduler.is_none() {
                Ok(())
            } else {
                Err(QuinnCurrentJoinError::CurrentObservationSettlement(
                    ClientControllerObservationSettlementErrorKind::MissingPendingSettlement,
                ))
            };
        }
        let scheduler = self
            .shutdown_probe_scheduler
            .take()
            .map(QuinnCurrentProbeSchedulerAuthority::into_controller);
        match self
            .controller
            .settle_pending_current_observation(scheduler)
        {
            Ok(_settlement) => Ok(()),
            Err(failure) => {
                let kind = failure.kind();
                self.shutdown_probe_scheduler = failure
                    .into_scheduler()
                    .map(QuinnCurrentProbeSchedulerAuthority::from_controller);
                Err(QuinnCurrentJoinError::CurrentObservationSettlement(kind))
            }
        }
    }

    /// Drops the controller's terminal-bound authority and retired current
    /// transport only after the exact physical actor has joined and every
    /// probe/application observation has settled.  The composition state is
    /// advanced only after the controller transaction succeeds, so rejection
    /// leaves both sides retryable and identity-exact.
    fn clear_terminal_bound_current_for_shutdown(
        &mut self,
        gate: &QuinnLegCompositionGate,
    ) -> Result<(), QuinnCurrentJoinError> {
        let (expected_generation, expected_reason) = match self.current_terminal {
            QuinnCurrentTerminalState::Applied { generation, reason }
            | QuinnCurrentTerminalState::ConsumedFailed {
                generation, reason, ..
            } => (generation, reason),
            QuinnCurrentTerminalState::Unbound
            | QuinnCurrentTerminalState::ControllerBound { .. }
            | QuinnCurrentTerminalState::ShutdownCleared { .. } => {
                return Err(QuinnCurrentJoinError::CompositionInvariant);
            }
        };
        let current_joined = self
            .current_joined
            .as_ref()
            .ok_or(QuinnCurrentJoinError::JoinedWitnessMissing)?;
        let runtime = current_joined.physical_incarnation(gate);
        let cleared = self
            .controller
            .clear_terminal_bound_current_after_shutdown_join(runtime, gate)
            .map_err(QuinnCurrentJoinError::CurrentShutdownClearFailed)?;
        let generation = cleared.generation();
        let reason = cleared.reason();
        self.current_terminal = QuinnCurrentTerminalState::ShutdownCleared { generation, reason };
        if generation != expected_generation || reason != expected_reason {
            return Err(QuinnCurrentJoinError::CompositionInvariant);
        }
        Ok(())
    }

    pub(super) async fn join_all_for_shutdown(&mut self) -> QuinnCompositionShutdownOutcome {
        // Admission closes before every possible early return and before the
        // first await. A rejected or caller-cancelled shutdown observation can
        // never reopen a stale factory clone.
        let _closed_now = self.controller.close_tcp_factory_admission();
        if let Err(error) = self.settle_recovery_tail_before_shutdown() {
            return QuinnCompositionShutdownOutcome {
                current: None,
                controller_ownership: self.controller_ownership_ledger(),
                first_error: Some(error),
            };
        }
        self.controller
            .capture_current_transport_shutdown_residual();
        let gate = QuinnLegCompositionGate::mint();
        let mut cleanup_error = self
            .fatal_recovery_tail_shutdown
            .as_ref()
            .map(QuinnFatalRecoveryTailShutdown::join_error);
        if !self.shutdown_layout_matches_controller(&gate) {
            cleanup_error = Some(QuinnCurrentJoinError::CompositionInvariant);
        }
        if self.current_terminal != QuinnCurrentTerminalState::Unbound
            && let Err(error) = self.settle_pending_controller_observation_for_shutdown()
        {
            cleanup_error.get_or_insert(error);
        }
        let retired_runtime = match self.retired.as_ref() {
            Some(RetiredQuinnLegOwner::Running(_)) => match tokio::runtime::Handle::try_current() {
                Ok(runtime) => Some(runtime),
                Err(_) => {
                    cleanup_error.get_or_insert(QuinnCurrentJoinError::RuntimeUnavailable);
                    None
                }
            },
            Some(RetiredQuinnLegOwner::Joining(_))
            | Some(RetiredQuinnLegOwner::Joined(_))
            | Some(RetiredQuinnLegOwner::TaskLost(_))
            | None => None,
        };
        let standby_runtime = match self.standby.as_ref().and_then(|owner| owner.actor.as_ref()) {
            Some(QuinnStandbyActorOwner::Running(_)) => {
                match tokio::runtime::Handle::try_current() {
                    Ok(runtime) => Some(runtime),
                    Err(_) => {
                        cleanup_error.get_or_insert(QuinnCurrentJoinError::RuntimeUnavailable);
                        None
                    }
                }
            }
            Some(QuinnStandbyActorOwner::Joining(_))
            | Some(QuinnStandbyActorOwner::Joined(_))
            | Some(QuinnStandbyActorOwner::TaskLost(_))
            | None => None,
        };
        if self.phase == QuinnClientCompositionPhase::Running {
            match self.current_terminal {
                QuinnCurrentTerminalState::Unbound => {
                    let phase = &mut self.phase;
                    let controller = &self.controller;
                    self.current
                        .transact_controller_shutdown(&gate, |runtime| {
                            if !controller.current_physical_actor_matches(runtime, &gate) {
                                return None;
                            }
                            // This publication shares the actor's exact
                            // first-failure mutex with actor Closing/cancel.
                            // Fatal-first therefore cannot be reclassified as
                            // local shutdown, while shutdown-first closes
                            // admission before a later child fatal can publish.
                            *phase = QuinnClientCompositionPhase::ShuttingDown;
                            Some(())
                        })
                        .map_err(QuinnCurrentJoinError::Actor)
                        .and_then(|published| {
                            published.ok_or(QuinnCurrentJoinError::CompositionInvariant)
                        })
                        .unwrap_or_else(|error| {
                            cleanup_error.get_or_insert(error);
                        });
                    // Fatal-first cannot be relabelled local shutdown because
                    // its actor error is retained above. The composition must
                    // nevertheless enter structural cleanup and join the
                    // exact failed actor instead of falling back to Drop.
                    self.phase = QuinnClientCompositionPhase::ShuttingDown;
                }
                QuinnCurrentTerminalState::Applied { .. }
                | QuinnCurrentTerminalState::ConsumedFailed { .. }
                | QuinnCurrentTerminalState::ControllerBound { .. }
                | QuinnCurrentTerminalState::ShutdownCleared { .. } => {
                    self.phase = QuinnClientCompositionPhase::ShuttingDown;
                }
            }
        }
        if let Err(error) = self.start_retired_join_for_shutdown(&gate, retired_runtime.as_ref()) {
            cleanup_error.get_or_insert(error);
        }
        if let Err(error) = self.start_standby_join_for_shutdown(&gate, standby_runtime.as_ref()) {
            cleanup_error.get_or_insert(error);
        }
        if self.current_joined.is_none() {
            // Unbound shutdown already published Closing in the exact barrier
            // transaction; terminal-led shutdown reaches the same idempotent
            // path here. Cancelling the caller at the following await loses
            // only observation, never cancellation ownership.
            self.current.request_cancel();
            match self.current.join().await {
                Ok(joined) => self.current_joined = Some(joined),
                Err(error) => {
                    cleanup_error.get_or_insert(QuinnCurrentJoinError::Actor(error));
                }
            }
        }
        if self.current_terminal == QuinnCurrentTerminalState::Unbound
            && self.current_joined.is_some()
        {
            match self.clear_joined_current_terminal_for_shutdown(&gate) {
                Ok((cleared, terminal_stamp)) => {
                    self.current_terminal_stamp = Some(terminal_stamp);
                    self.current_terminal = QuinnCurrentTerminalState::ShutdownCleared {
                        generation: cleared.generation(),
                        reason: cleared.reason(),
                    };
                }
                Err(kind) => {
                    cleanup_error
                        .get_or_insert(QuinnCurrentJoinError::CurrentShutdownClearFailed(kind));
                }
            }
        }
        // A controller-bound terminal may have been waiting for an actor-owned
        // delivery/recovery receipt. Physical join releases that tail; retry
        // the exact second phase now, retaining any remaining logical failure
        // without sacrificing the joined witness.
        if matches!(
            self.current_terminal,
            QuinnCurrentTerminalState::ControllerBound { .. }
        ) && let Err(error) = self.finish_bound_current_terminal()
        {
            cleanup_error.get_or_insert(current_terminal_error_for_join(error));
        }
        if let Err(error) = self.settle_pending_controller_observation_for_shutdown() {
            cleanup_error.get_or_insert(error);
        }
        if let QuinnCurrentTerminalState::ConsumedFailed { failure, .. } = self.current_terminal {
            // Preserve the logical failure before the following structural
            // clear advances the state to ShutdownCleared.
            cleanup_error.get_or_insert(failure.into_join_error());
        }
        if matches!(
            self.current_terminal,
            QuinnCurrentTerminalState::Applied { .. }
                | QuinnCurrentTerminalState::ConsumedFailed { .. }
        ) && let Err(error) = self.clear_terminal_bound_current_for_shutdown(&gate)
        {
            cleanup_error.get_or_insert(error);
        }
        if let Err(error) = self.join_standby_for_shutdown(&gate).await {
            cleanup_error.get_or_insert(error);
        }
        if let Err(error) = self.join_retired_for_shutdown(&gate).await {
            cleanup_error.get_or_insert(error);
        }
        if !self.standby_shutdown_is_clean() {
            cleanup_error.get_or_insert(QuinnCurrentJoinError::StandbyJoinedWitnessMissing);
        }
        if let Some(residual) = self.controller.current_transport_shutdown_residual() {
            cleanup_error.get_or_insert(QuinnCurrentJoinError::CurrentControllerResidual {
                active: residual.active(),
                queued_frames: residual.queued_frames(),
                owned_bytes: residual.owned_bytes(),
                ordered_delivery_wait: residual.ordered_delivery_wait(),
            });
        }
        let standby_control_pending = self
            .standby
            .as_ref()
            .is_some_and(|owner| owner.pending_control.is_some());
        let standby_structurally_joined = self.standby.as_ref().is_none_or(|owner| {
            matches!(
                owner.actor.as_ref(),
                Some(QuinnStandbyActorOwner::Joined(_))
            )
        });
        if self.current_joined.is_some()
            && !standby_control_pending
            && standby_structurally_joined
            && self.retired.is_none()
        {
            self.phase = QuinnClientCompositionPhase::Joined;
            if !self.layout_matches_controller(&gate) {
                cleanup_error.get_or_insert(QuinnCurrentJoinError::CompositionInvariant);
            }
        }
        let joined = self
            .shutdown_actor_inventory()
            .map(CurrentQuinnActorJoined::from_inventory);
        if joined.is_none() {
            cleanup_error.get_or_insert(QuinnCurrentJoinError::JoinedWitnessMissing);
        }
        if let QuinnCurrentTerminalState::ConsumedFailed { failure, .. } = self.current_terminal {
            cleanup_error.get_or_insert(failure.into_join_error());
        }
        let controller_ownership = self.controller_ownership_ledger();
        if !controller_ownership.live_is_zero() {
            cleanup_error.get_or_insert(QuinnCurrentJoinError::CurrentControllerOwnership);
        }
        QuinnCompositionShutdownOutcome {
            current: joined,
            controller_ownership,
            first_error: cleanup_error,
        }
    }

    /// Starts or adopts the sole B cleanup task before shutdown's first await.
    /// An independently failed B may already be Joining; shutdown consumes the
    /// same task instead of creating a second owner or touching the actor.
    fn start_standby_join_for_shutdown(
        &mut self,
        gate: &QuinnLegCompositionGate,
        runtime: Option<&tokio::runtime::Handle>,
    ) -> Result<(), QuinnCurrentJoinError> {
        if self.phase == QuinnClientCompositionPhase::Running {
            return Err(QuinnCurrentJoinError::CompositionInvariant);
        }
        let Some(owner) = self.standby.as_mut() else {
            return Ok(());
        };
        discard_pending_standby_control_for_shutdown(owner, gate)?;
        let actor = owner
            .actor
            .as_mut()
            .ok_or(QuinnCurrentJoinError::CompositionInvariant)?;
        match actor {
            QuinnStandbyActorOwner::Joining(_) | QuinnStandbyActorOwner::Joined(_) => {
                return Ok(());
            }
            QuinnStandbyActorOwner::TaskLost(cause) => {
                return Err(QuinnCurrentJoinError::StandbyJoinTask(Arc::clone(cause)));
            }
            QuinnStandbyActorOwner::Running(actor) => {
                let _outbound_at_cutoff = observe_standby_shutdown_outbound(
                    actor
                        .try_retire_delivered_wait_for_shutdown(gate)
                        .map_err(QuinnCurrentJoinError::StandbyActor)?,
                );
                // Pending/queued FIFO state is an exact observation, not a
                // structural-join gate.  Closing the actor publishes the one
                // terminal that can authorize its synchronous discard.  The
                // join task retains the actor across caller cancellation; if
                // its first inventory check reports residual ownership, the
                // continuation below consumes that exact terminal+FIFO and
                // retries the actor join once without polling.
            }
        }
        let runtime = runtime.ok_or(QuinnCurrentJoinError::RuntimeUnavailable)?;
        let Some(QuinnStandbyActorOwner::Running(actor)) = owner.actor.take() else {
            return Err(QuinnCurrentJoinError::CompositionInvariant);
        };
        let mut actor = *actor;
        actor.request_cancel();
        owner.actor = Some(QuinnStandbyActorOwner::Joining(spawn_standby_join(
            runtime, actor,
        )));
        Ok(())
    }

    async fn join_standby_for_shutdown(
        &mut self,
        gate: &QuinnLegCompositionGate,
    ) -> Result<(), QuinnCurrentJoinError> {
        let Some(owner) = self.standby.as_mut() else {
            return Ok(());
        };
        discard_pending_standby_control_for_shutdown(owner, gate)?;
        // `shutdown_retired_state` is installed synchronously before the sole
        // retry task.  A caller cancelled while awaiting that task may enter
        // here again, so derive the retry budget from retained composition
        // state instead of resetting it per observation.
        let mut retried_after_exact_fifo_discard = owner.shutdown_retired_state.is_some();
        while matches!(
            owner.actor.as_ref(),
            Some(QuinnStandbyActorOwner::Joining(_))
        ) {
            let outcome = {
                let Some(QuinnStandbyActorOwner::Joining(task)) = owner.actor.as_mut() else {
                    return Err(QuinnCurrentJoinError::CompositionInvariant);
                };
                task.await
            };
            let actor = owner.actor.take();
            if !matches!(actor, Some(QuinnStandbyActorOwner::Joining(_))) {
                owner.actor = actor;
                return Err(QuinnCurrentJoinError::CompositionInvariant);
            }
            match outcome {
                Ok(QuinnStandbyLegJoinOutcome {
                    actor,
                    joined,
                    runtime,
                }) => match joined {
                    Ok(joined) => {
                        drop(actor);
                        owner.actor = Some(QuinnStandbyActorOwner::Joined(joined));
                    }
                    Err(error @ QuinnLegActorError::ResidualOwnership { .. })
                        if !retried_after_exact_fifo_discard =>
                    {
                        owner.actor = Some(QuinnStandbyActorOwner::Running(Box::new(actor)));
                        settle_standby_fifo_after_join_residual(owner, gate)?;
                        let Some(QuinnStandbyActorOwner::Running(actor)) = owner.actor.take()
                        else {
                            return Err(QuinnCurrentJoinError::CompositionInvariant);
                        };
                        owner.actor = Some(QuinnStandbyActorOwner::Joining(spawn_standby_join(
                            &runtime, *actor,
                        )));
                        retried_after_exact_fifo_discard = true;
                        drop(error);
                        continue;
                    }
                    Err(error) => {
                        owner.actor = Some(QuinnStandbyActorOwner::Running(Box::new(actor)));
                        return Err(QuinnCurrentJoinError::StandbyActor(error));
                    }
                },
                Err(error) => {
                    let cause: Arc<str> = Arc::from(error.to_string());
                    owner.actor = Some(QuinnStandbyActorOwner::TaskLost(Arc::clone(&cause)));
                    return Err(QuinnCurrentJoinError::StandbyJoinTask(cause));
                }
            }
        }
        settle_joined_standby_for_shutdown(owner, gate)
    }

    fn standby_shutdown_is_clean(&self) -> bool {
        self.standby.as_ref().is_none_or(|owner| {
            owner.state.is_none()
                && owner.pending_control.is_none()
                && owner.shutdown_terminal.is_some()
                && owner.shutdown_retired_state.is_some()
                && matches!(
                    owner.actor.as_ref(),
                    Some(QuinnStandbyActorOwner::Joined(joined))
                        if QuinnActorJoinedInventory::from_actor(joined).is_clean()
                )
        })
    }

    pub(crate) fn standby_shutdown_control_discard(
        &self,
    ) -> Option<QuinnStandbyShutdownControlDiscard> {
        self.standby
            .as_ref()
            .and_then(|owner| owner.shutdown_control_discard)
    }

    pub(crate) fn standby_shutdown_state_inventory(
        &self,
    ) -> Option<QuinnStandbyShutdownStateInventory> {
        self.standby
            .as_ref()
            .and_then(|owner| owner.shutdown_retired_state.as_ref())
            .map(|retired| retired.inventory)
    }

    pub(crate) fn standby_shutdown_terminal_reason(&self) -> Option<LegTransportTerminalReason> {
        self.standby
            .as_ref()
            .and_then(|owner| owner.shutdown_terminal)
            .map(|settled| settled.reason)
    }

    /// Checked composition-wide actor inventory for the root-facing shutdown
    /// witness. Joined standby and retired actors remain visible even when
    /// controller cleanup failed, so terminal-only ownership can never be
    /// hidden behind the current actor's local zero.
    fn shutdown_actor_inventory(&self) -> Option<QuinnActorJoinedInventory> {
        let current = QuinnActorJoinedInventory::from_actor(self.current_joined.as_ref()?);
        let standby = match self.standby.as_ref() {
            None => QuinnActorJoinedInventory::ZERO,
            Some(owner) => match owner.actor.as_ref()? {
                QuinnStandbyActorOwner::Joined(joined) => {
                    QuinnActorJoinedInventory::from_actor(joined)
                }
                QuinnStandbyActorOwner::Running(_)
                | QuinnStandbyActorOwner::Joining(_)
                | QuinnStandbyActorOwner::TaskLost(_) => return None,
            },
        };
        let retired = match self.retired.as_ref() {
            None => QuinnActorJoinedInventory::ZERO,
            Some(RetiredQuinnLegOwner::Joined(joined)) => {
                QuinnActorJoinedInventory::from_actor(joined)
            }
            Some(
                RetiredQuinnLegOwner::Running(_)
                | RetiredQuinnLegOwner::Joining(_)
                | RetiredQuinnLegOwner::TaskLost(_),
            ) => return None,
        };
        current.checked_add(standby)?.checked_add(retired)
    }

    /// Starts retired cleanup while keeping its task handle in the
    /// composition. Cancelling the outer shutdown future therefore loses only
    /// observation; the actor and its eventual joined witness stay owned here.
    fn start_retired_join_for_shutdown(
        &mut self,
        gate: &QuinnLegCompositionGate,
        runtime: Option<&tokio::runtime::Handle>,
    ) -> Result<(), QuinnCurrentJoinError> {
        let Some(retired) = self.retired.as_ref() else {
            return Ok(());
        };
        match retired {
            RetiredQuinnLegOwner::Running(_) => {
                if !self.layout_matches_controller(gate) {
                    return Err(QuinnCurrentJoinError::CompositionInvariant);
                }
            }
            RetiredQuinnLegOwner::Joining(_) | RetiredQuinnLegOwner::Joined(_) => return Ok(()),
            RetiredQuinnLegOwner::TaskLost(_) => return Ok(()),
        }
        let runtime = runtime.ok_or(QuinnCurrentJoinError::RuntimeUnavailable)?;
        let Some(RetiredQuinnLegOwner::Running(actor)) = self.retired.take() else {
            return Err(QuinnCurrentJoinError::CompositionInvariant);
        };
        let mut actor = *actor;
        actor.request_cancel();
        let task = spawn_retired_join(runtime, actor);
        self.retired = Some(RetiredQuinnLegOwner::Joining(task));
        Ok(())
    }

    async fn join_retired_for_shutdown(
        &mut self,
        gate: &QuinnLegCompositionGate,
    ) -> Result<(), QuinnCurrentJoinError> {
        if matches!(self.retired, Some(RetiredQuinnLegOwner::Joining(_))) {
            let outcome = {
                let Some(RetiredQuinnLegOwner::Joining(task)) = self.retired.as_mut() else {
                    return Err(QuinnCurrentJoinError::CompositionInvariant);
                };
                task.await
            };
            let retired = self.retired.take();
            if !matches!(retired, Some(RetiredQuinnLegOwner::Joining(_))) {
                self.retired = retired;
                return Err(QuinnCurrentJoinError::CompositionInvariant);
            }
            match outcome {
                Ok(RetiredQuinnLegJoinOutcome { actor, joined }) => match joined {
                    Ok(joined) => {
                        drop(actor);
                        self.retired = Some(RetiredQuinnLegOwner::Joined(joined));
                    }
                    Err(error) => {
                        self.retired = Some(RetiredQuinnLegOwner::Running(Box::new(actor)));
                        return Err(QuinnCurrentJoinError::RetiredActor(error));
                    }
                },
                Err(error) => {
                    let cause: Arc<str> = Arc::from(error.to_string());
                    self.retired = Some(RetiredQuinnLegOwner::TaskLost(Arc::clone(&cause)));
                    return Err(QuinnCurrentJoinError::RetiredJoinTask(cause));
                }
            }
        }
        let Some(RetiredQuinnLegOwner::Joined(joined)) = self.retired.as_mut() else {
            return if let Some(RetiredQuinnLegOwner::TaskLost(cause)) = self.retired.as_ref() {
                Err(QuinnCurrentJoinError::RetiredJoinTask(Arc::clone(cause)))
            } else if self.retired.is_none() {
                Ok(())
            } else {
                Err(QuinnCurrentJoinError::CompositionInvariant)
            };
        };
        let cleared = joined.transact_terminal(gate, |runtime, terminal| {
            match self
                .controller
                .clear_retired_after_shutdown_join(runtime, terminal, gate)
            {
                Ok(cleared) => Ok(cleared),
                Err(failure) => {
                    let kind = failure.kind();
                    Err((failure.into_terminal(), kind))
                }
            }
        });
        if let Err(kind) = cleared {
            return Err(QuinnCurrentJoinError::RetiredController(kind.into()));
        }
        let retired = self.retired.take();
        match retired {
            Some(RetiredQuinnLegOwner::Joined(joined)) => drop(joined),
            Some(other) => {
                self.retired = Some(other);
                return Err(QuinnCurrentJoinError::CompositionInvariant);
            }
            None => return Err(QuinnCurrentJoinError::CompositionInvariant),
        }
        Ok(())
    }

    fn current_physical_incarnation<'a>(
        &'a self,
        gate: &QuinnLegCompositionGate,
    ) -> Option<&'a crate::owned_upstream::leg_io::PhysicalLegRuntimeIncarnation> {
        self.current_joined
            .as_ref()
            .map(|joined| joined.physical_incarnation(gate))
            .or_else(|| self.current.physical_incarnation(gate))
    }

    fn layout_matches_controller(&self, gate: &QuinnLegCompositionGate) -> bool {
        if !self.current_layout_matches_controller(gate) {
            return false;
        }
        let retired = self
            .retired
            .as_ref()
            .and_then(|retired| retired.physical_incarnation(gate));
        self.controller
            .retired_physical_actor_layout_matches(retired, gate)
    }

    /// A running or joined retired actor still exposes its exact incarnation
    /// and must match before shutdown publishes or cancels anything. A
    /// joining/task-lost owner no longer exposes that borrow; it is accepted
    /// only when the controller still owns the retired slot whose exact match
    /// was checked before the structured join task was installed.
    fn shutdown_layout_matches_controller(&self, gate: &QuinnLegCompositionGate) -> bool {
        if !self.current_layout_matches_controller(gate) {
            return false;
        }
        match self.retired.as_ref() {
            None => self
                .controller
                .retired_physical_actor_layout_matches(None, gate),
            Some(
                retired @ (RetiredQuinnLegOwner::Running(_) | RetiredQuinnLegOwner::Joined(_)),
            ) => self
                .controller
                .retired_physical_actor_layout_matches(retired.physical_incarnation(gate), gate),
            Some(RetiredQuinnLegOwner::Joining(_) | RetiredQuinnLegOwner::TaskLost(_)) => {
                self.controller.retired_slot_is_present()
            }
        }
    }

    fn current_layout_matches_controller(&self, gate: &QuinnLegCompositionGate) -> bool {
        let Some(current) = self.current_physical_incarnation(gate) else {
            return false;
        };
        match self.current_terminal {
            QuinnCurrentTerminalState::Unbound => self
                .controller
                .current_physical_actor_matches(current, gate),
            QuinnCurrentTerminalState::ControllerBound { .. }
            | QuinnCurrentTerminalState::Applied { .. }
            | QuinnCurrentTerminalState::ConsumedFailed { .. } => self
                .controller
                .terminating_current_physical_actor_matches(current, gate),
            QuinnCurrentTerminalState::ShutdownCleared { .. } => {
                self.controller.current_is_cleared_for_shutdown()
            }
        }
    }

    /// Corrupts only the current runtime half in tests, leaving retired A
    /// exact, so the two pre-cancel layout conjuncts are independently locked.
    #[cfg(test)]
    pub(super) fn swap_current_with_actor_for_test(
        &mut self,
        actor: &mut StartedQuinnLegActor,
    ) -> bool {
        if self.current_joined.is_some() {
            return false;
        }
        std::mem::swap(&mut self.current, actor);
        true
    }

    /// Corrupts only the retired runtime half in tests, leaving current B
    /// exact, so route/LegId equality cannot stand in for physical identity.
    #[cfg(test)]
    pub(super) fn swap_retired_with_actor_for_test(
        &mut self,
        actor: &mut StartedQuinnLegActor,
    ) -> bool {
        let Some(RetiredQuinnLegOwner::Running(retired)) = self.retired.as_mut() else {
            return false;
        };
        std::mem::swap(retired.as_mut(), actor);
        true
    }

    /// Arms a deterministic pause inside the retired actor root after
    /// cancellation has begun but before structural join has completed.
    #[cfg(test)]
    pub(super) fn arm_retired_join_block_for_test(&self) -> Option<QuinnLegActorJoinBlock> {
        match self.retired.as_ref()? {
            RetiredQuinnLegOwner::Running(actor) => Some(actor.arm_join_block_for_test()),
            RetiredQuinnLegOwner::Joining(_)
            | RetiredQuinnLegOwner::Joined(_)
            | RetiredQuinnLegOwner::TaskLost(_) => None,
        }
    }

    #[cfg(test)]
    pub(super) fn arm_current_join_block_for_test(&self) -> QuinnLegActorJoinBlock {
        self.current.arm_join_block_for_test()
    }

    #[cfg(test)]
    pub(super) fn arm_current_data_writer_block_for_test(&self) -> QuinnLegActorDataWriterBlock {
        self.current.arm_data_writer_block_for_test()
    }

    #[cfg(test)]
    pub(super) fn arm_standby_join_block_for_test(&self) -> Option<QuinnLegActorJoinBlock> {
        let owner = self.standby.as_ref()?;
        match owner.actor.as_ref()? {
            QuinnStandbyActorOwner::Running(actor) => Some(actor.arm_join_block_for_test()),
            QuinnStandbyActorOwner::Joining(_)
            | QuinnStandbyActorOwner::Joined(_)
            | QuinnStandbyActorOwner::TaskLost(_) => None,
        }
    }

    /// Observes the installed client B actor without exposing transport, FIFO,
    /// cancellation, or settlement authority to the test.
    #[cfg(test)]
    pub(super) fn registered_standby_actor_diagnostic_for_test(
        &self,
    ) -> Option<QuinnLegActorDiagnosticObservation> {
        let owner = self.standby.as_ref()?;
        match owner.actor.as_ref()? {
            QuinnStandbyActorOwner::Running(actor) => Some(actor.diagnostic_observation_for_test()),
            QuinnStandbyActorOwner::Joining(_)
            | QuinnStandbyActorOwner::Joined(_)
            | QuinnStandbyActorOwner::TaskLost(_) => None,
        }
    }

    /// Copy-only actor counters for rollback/capacity tracers. This exposes no
    /// transport, FIFO, cancellation, record, or settlement authority.
    #[cfg(test)]
    pub(super) fn registered_standby_actor_snapshot_for_test(
        &self,
    ) -> Option<QuinnLegActorSnapshot> {
        let owner = self.standby.as_ref()?;
        match owner.actor.as_ref()? {
            QuinnStandbyActorOwner::Running(actor) => Some(actor.actor_snapshot()),
            QuinnStandbyActorOwner::Joining(_)
            | QuinnStandbyActorOwner::Joined(_)
            | QuinnStandbyActorOwner::TaskLost(_) => None,
        }
    }

    #[cfg(test)]
    pub(super) fn standby_shutdown_retry_is_retained_for_test(&self) -> bool {
        self.phase == QuinnClientCompositionPhase::ShuttingDown
            && self.standby.as_ref().is_some_and(|owner| {
                matches!(
                    owner.actor.as_ref(),
                    Some(QuinnStandbyActorOwner::Running(_))
                )
            })
    }

    /// Historical discard is not live ownership, but it is part of the final
    /// checked audit and therefore remains observable beside the joined-zero
    /// actor witness instead of being silently overwritten.
    pub(crate) fn standby_shutdown_outbound_discard(
        &self,
    ) -> Option<QuinnStandbyShutdownOutboundDiscard> {
        self.standby
            .as_ref()?
            .shutdown_retired_state
            .as_ref()?
            .outbound_discard
    }

    /// Fills only the already-installed B actor's real bounded FIFO.  The
    /// exact enqueue error retains its frame, allowing a dispatcher test to
    /// create ACK pressure without exposing B's queue or transport pieces.
    #[cfg(test)]
    pub(super) fn enqueue_registered_standby_control_for_test(
        &mut self,
        frame: LegControlFrame,
    ) -> Result<(), QuinnLegActorEnqueueError> {
        let Some(owner) = self.standby.as_mut() else {
            return Err(QuinnLegActorEnqueueError::Closing { frame });
        };
        let Some(QuinnStandbyActorOwner::Running(actor)) = owner.actor.as_mut() else {
            return Err(QuinnLegActorEnqueueError::Closing { frame });
        };
        actor
            .enqueue_ordered_leg_control(frame, owner.features)
            .map(drop)
    }

    /// Aborts only the already-installed structured B cleanup task. Tests use
    /// the real `JoinError -> TaskLost` reducer path instead of forging the
    /// sticky state or discarding a still-running actor directly.
    #[cfg(test)]
    pub(super) fn abort_standby_join_task_for_test(&self) -> bool {
        let Some(QuinnStandbyActorOwner::Joining(task)) =
            self.standby.as_ref().and_then(|owner| owner.actor.as_ref())
        else {
            return false;
        };
        task.abort();
        true
    }

    #[cfg(test)]
    pub(super) fn arm_current_post_terminal_cleanup_block_for_test(
        &self,
    ) -> QuinnLegActorPostTerminalCleanupBlock {
        self.current.arm_post_terminal_cleanup_block_for_test()
    }

    #[cfg(test)]
    pub(super) fn arm_current_inbound_publication_block_for_test(
        &self,
    ) -> QuinnLegActorInboundPublicationBlock {
        self.current.arm_inbound_publication_block_for_test()
    }
}

impl Drop for QuinnClientSessionComposition {
    fn drop(&mut self) {
        if let Some(RetiredQuinnLegOwner::Joining(task)) = self.retired.as_mut() {
            // This is deliberately only an unclean, fail-closed fallback.
            // Tokio abort completion is asynchronous, so production
            // composition must drive the explicit retired/current join path
            // and must not claim structural cleanup from Drop.
            task.abort();
        }
        if let Some(QuinnStandbyActorOwner::Joining(task)) =
            self.standby.as_mut().and_then(|owner| owner.actor.as_mut())
        {
            task.abort();
        }
    }
}

pub(crate) enum QuinnStandbyRegistrationFailure {
    SessionShuttingDown,
    CompositionInvariant,
    ActorTransportUnavailable,
    Begin(StandbyRegistrationError),
    Enqueue(Box<StandbyEnqueueFailure>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QuinnOwnedStandbyRegistrationDrive {
    Pressure,
    Queued,
    AlreadyQueued,
    Failed(QuinnStandbyQueueFailureKind),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QuinnOwnedStandbyRegistrationErrorKind {
    SessionShuttingDown,
    StandbyOccupied,
    ClockSourceMismatch,
    MissingStandby,
    WrongState,
    StandbyNotRunning,
    StandbyFailed,
    CompositionInvariant,
    ActorTransportUnavailable,
    Begin(StandbyRegistrationError),
    Enqueue(QuinnStandbyQueueFailureKind),
}

pub(crate) struct QuinnOwnedStandbyRegistrationFailure {
    actor: Box<StartedQuinnLegActor>,
    kind: QuinnOwnedStandbyRegistrationErrorKind,
}

impl QuinnOwnedStandbyRegistrationFailure {
    pub(crate) const fn kind(&self) -> QuinnOwnedStandbyRegistrationErrorKind {
        self.kind
    }

    pub(crate) fn into_actor(self) -> StartedQuinnLegActor {
        *self.actor
    }
}

impl std::fmt::Debug for QuinnOwnedStandbyRegistrationFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("QuinnOwnedStandbyRegistrationFailure")
            .field("kind", &self.kind)
            .field("actor", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QuinnStandbyProbeDrive {
    Queued,
    Pressure,
    AlreadyFresh,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum QuinnStandbyProbeArmDrive {
    NotDelivered,
    Armed {
        deadline: SimTime,
        recovery_key: RootStandbyBRecoveryKey,
    },
    AlreadyArmed {
        deadline: SimTime,
    },
}

#[derive(Debug)]
pub(crate) enum QuinnStandbyProbeArmError {
    SessionShuttingDown,
    MissingStandby,
    StandbyFailed,
    WrongState,
    CompositionInvariant,
    Arm(RegisteredStandbyProbeArmErrorKind),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QuinnStandbyProbeExpiryDrive {
    Inert,
    Pending { deadline: SimTime },
    InboundPending { deadline: SimTime },
    PublicationPending { deadline: SimTime },
    Expired { deadline: SimTime },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QuinnStandbyProbeExpiryError {
    SessionShuttingDown,
    MissingStandby,
    StandbyFailed,
    CompositionInvariant,
    RecoveryKeyMismatch,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QuinnStandbyJoinStartDisposition {
    Started,
    AlreadyJoining,
    AlreadyJoined,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QuinnStandbyJoinDrive {
    Pending,
    Cleared(QuinnStandbyActorCleared),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct QuinnStandbyActorCleared {
    owned_messages: usize,
    owned_bytes: usize,
    owned_terminals: usize,
    terminal_failure: bool,
}

impl QuinnStandbyActorCleared {
    pub(crate) const fn owned_messages(self) -> usize {
        self.owned_messages
    }

    pub(crate) const fn owned_bytes(self) -> usize {
        self.owned_bytes
    }

    pub(crate) const fn owned_terminals(self) -> usize {
        self.owned_terminals
    }

    pub(crate) const fn terminal_failure(self) -> bool {
        self.terminal_failure
    }
}

#[derive(Debug)]
pub(crate) enum QuinnStandbyClearError {
    SessionShuttingDown,
    MissingStandby,
    FailureNotObserved,
    NotStarted,
    RuntimeUnavailable,
    JoinTask(Arc<str>),
    CompositionInvariant,
    Actor(QuinnLegActorError),
    PendingControl {
        kind: QuinnStandbyPendingControlKind,
        messages: usize,
        bytes: usize,
    },
    ResidualOwnership {
        messages: usize,
        bytes: usize,
        terminals: usize,
        terminal_failure: bool,
    },
    JoinedWitnessMissing,
}

#[derive(Debug)]
pub(crate) enum QuinnStandbyProbeError {
    SessionShuttingDown,
    MissingStandby,
    StandbyFailed,
    WrongState,
    CompositionInvariant,
    SequenceExhausted,
    Begin(RegisteredStandbyProbeBeginErrorKind),
    Enqueue(QuinnStandbyQueueFailureKind),
    Actor(QuinnLegActorError),
}

#[derive(Debug)]
pub(crate) enum QuinnStandbyInboundDrive {
    SessionShuttingDown,
    MissingStandby,
    StandbyFailed,
    Idle,
    Time(crate::owned_upstream::two_leg::SimTimeError),
    Actor(QuinnLegActorError),
    CompositionInvariant,
    RegistrationAccepted,
    RejectedRegistration(StandbyAcceptanceErrorKind),
    ProbeFresh {
        fresh_until: SimTime,
        recovery_key: Option<RootStandbyBRecoveryKey>,
    },
    RejectedProbe(RegisteredStandbyProbeAckErrorKind),
    /// The exact peer probe was classified ahead of B typestate policy and
    /// its contract-derived ACK entered the physical B FIFO.
    ProbeAckQueued {
        probe_sequence: ProbeSequence,
    },
    ProbeAckPressure {
        probe_sequence: ProbeSequence,
    },
    ProbeAckRejected(LegControlQueueErrorKind),
    SwitchHintPending {
        arrived_at: SimTime,
        publication_epoch: u64,
        send_ordinal: u64,
        encoded_bytes: usize,
    },
    AttachAccepted {
        generation: crate::resumable::LegGeneration,
    },
    /// The exact physical `ATTACH_ACCEPTED` record and its reserved DELIVERY
    /// remain held until controller/actor/arbiter replacement commits.
    AttachAcceptedPendingCommit {
        generation: crate::resumable::LegGeneration,
    },
    AttachGenerationStatus,
    RejectedAttach(RegisteredAttachResponseErrorKind),
    WrongState,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QuinnStandbyAttachDrive {
    AwaitingResponse,
    Pressure,
}

struct QuinnStandbyAttachCoreDrive {
    disposition: QuinnStandbyAttachDrive,
    pending_frame_bytes: usize,
}

#[derive(Debug)]
pub(crate) enum QuinnStandbyAttachError {
    SessionShuttingDown,
    MissingPhysicalPermit,
    LossNotReady,
    TerminalNotApplied,
    MissingStandby,
    StandbyFailed,
    StandbyNotRunning,
    WrongSwitchEpoch,
    CompositionInvariant,
    FreshStandbyRequired,
    EligibleStandbyRequired,
    Begin(FreshRegisteredAttachBeginErrorKind),
    BeginRegistered(RegisteredAttachBeginErrorKind),
    PendingFrameEncoding(ProtocolError),
    Enqueue(QuinnStandbyQueueFailureKind),
    Actor(QuinnLegActorError),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QuinnOwnedStandbyInstallAction {
    Retryable,
    SessionFatal,
    StandbyTerminal,
    Invariant,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QuinnOwnedStandbyInstallError {
    SessionShuttingDown,
    MissingStandby,
    StandbyFailed {
        action: QuinnOwnedStandbyInstallAction,
    },
    WrongState,
    Replacement {
        kind: QuinnRegisteredReplacementErrorKind,
        action: QuinnOwnedStandbyInstallAction,
    },
}

/// Copy-only rejection from the physical acceptance transaction. Exact B
/// actor, raw inbound, acceptance, and policy ownership are always restored
/// inside the composition before one of these values escapes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QuinnPhysicalReplacementCommitError {
    SessionShuttingDown,
    MissingPhysicalAuthority,
    MissingStandby,
    StandbyFailed {
        action: QuinnOwnedStandbyInstallAction,
    },
    WrongState,
    CompositionInvariant,
    Arbiter(SwitchArbiterError),
    Replacement {
        kind: QuinnRegisteredReplacementErrorKind,
        action: QuinnOwnedStandbyInstallAction,
    },
}

impl QuinnOwnedStandbyInstallError {
    pub(crate) const fn action(self) -> QuinnOwnedStandbyInstallAction {
        match self {
            Self::SessionShuttingDown => QuinnOwnedStandbyInstallAction::SessionFatal,
            Self::MissingStandby | Self::WrongState => QuinnOwnedStandbyInstallAction::Invariant,
            Self::StandbyFailed { action } | Self::Replacement { action, .. } => action,
        }
    }
}

#[derive(Debug)]
pub(crate) enum QuinnCurrentTerminalDrive {
    Pending,
    Applied(ClientCurrentTerminalDiscarded),
    AlreadyApplied {
        generation: crate::resumable::LegGeneration,
        reason: LegTransportTerminalReason,
    },
}

#[derive(Debug)]
pub(crate) enum QuinnCurrentTerminalError {
    SessionShuttingDown,
    CompositionInvariant,
    UnexpectedLocalCancellation,
    MissingTerminalFailureTimestamp,
    Actor(QuinnLegActorError),
    ControllerBind(ClientCurrentTerminalBindErrorKind),
    ControllerDiscard(ClientCurrentTerminalDiscardErrorKind),
}

impl std::fmt::Debug for QuinnStandbyRegistrationFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SessionShuttingDown => {
                formatter.write_str("QuinnStandbyRegistrationFailure::SessionShuttingDown")
            }
            Self::CompositionInvariant => {
                formatter.write_str("QuinnStandbyRegistrationFailure::CompositionInvariant")
            }
            Self::ActorTransportUnavailable => {
                formatter.write_str("QuinnStandbyRegistrationFailure::ActorTransportUnavailable")
            }
            Self::Begin(error) => formatter.debug_tuple("Begin").field(error).finish(),
            Self::Enqueue(error) => formatter.debug_tuple("Enqueue").field(error).finish(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QuinnRegisteredReplacementErrorKind {
    SessionShuttingDown,
    PostLossNotReady,
    FreshStandbyRequired,
    OwnedStandbyOccupied,
    CompositionInvariant,
    ActorTransportUnavailable,
    Controller(ClientRecoveryBeginErrorKind),
}

const fn map_client_recovery_failure_class(
    class: ClientRegisteredRecoveryFailureClass,
) -> QuinnOwnedStandbyInstallAction {
    match class {
        ClientRegisteredRecoveryFailureClass::Retryable => {
            QuinnOwnedStandbyInstallAction::Retryable
        }
        ClientRegisteredRecoveryFailureClass::SessionFatal => {
            QuinnOwnedStandbyInstallAction::SessionFatal
        }
        ClientRegisteredRecoveryFailureClass::StandbyTerminal => {
            QuinnOwnedStandbyInstallAction::StandbyTerminal
        }
        ClientRegisteredRecoveryFailureClass::Invariant => {
            QuinnOwnedStandbyInstallAction::Invariant
        }
    }
}

pub(crate) struct QuinnRegisteredReplacementFailure {
    ownership: Box<QuinnRegisteredReplacementOwnership>,
    kind: QuinnRegisteredReplacementErrorKind,
    class: QuinnOwnedStandbyInstallAction,
}

struct QuinnRegisteredReplacementOwnership {
    accepted: AcceptedClientRegisteredAttach,
    actor: StartedQuinnLegActor,
}

struct QuinnPhysicalReplacementOperationFailure {
    actor: StartedQuinnLegActor,
    error: QuinnPhysicalReplacementCommitError,
}

impl QuinnRegisteredReplacementFailure {
    pub(crate) const fn kind(&self) -> QuinnRegisteredReplacementErrorKind {
        self.kind
    }

    const fn failure_class(&self) -> QuinnOwnedStandbyInstallAction {
        self.class
    }

    pub(crate) fn into_parts(self) -> (AcceptedClientRegisteredAttach, StartedQuinnLegActor) {
        let QuinnRegisteredReplacementOwnership { accepted, actor } = *self.ownership;
        (accepted, actor)
    }
}

impl std::fmt::Debug for QuinnRegisteredReplacementFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("QuinnRegisteredReplacementFailure")
            .field("kind", &self.kind)
            .field("ownership", &"[REDACTED]")
            .finish()
    }
}

#[derive(Debug)]
pub(crate) enum QuinnRetiredJoinClearError {
    SessionShuttingDown,
    CompositionInvariant,
    MissingRetired,
    NotStarted,
    RuntimeUnavailable,
    JoinTask(Arc<str>),
    Actor(QuinnLegActorError),
    Controller(QuinnRetiredControllerClearErrorKind),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QuinnRetiredJoinStartDisposition {
    Started,
    AlreadyJoining,
    AlreadyJoined,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QuinnRetiredJoinDrive {
    Pending,
    Cleared(RetiredQuinnActorCleared),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QuinnRetiredControllerClearErrorKind {
    MissingRetired,
    WrongPhysicalActor,
    WrongTerminal,
    ReplacementApplicationAckPending,
    SupersessionHistoryOverflow,
}

impl From<RetiredClientClearErrorKind> for QuinnRetiredControllerClearErrorKind {
    fn from(kind: RetiredClientClearErrorKind) -> Self {
        match kind {
            RetiredClientClearErrorKind::MissingRetired => Self::MissingRetired,
            RetiredClientClearErrorKind::WrongPhysicalActor => Self::WrongPhysicalActor,
            RetiredClientClearErrorKind::WrongTerminal => Self::WrongTerminal,
            RetiredClientClearErrorKind::ReplacementApplicationAckPending => {
                Self::ReplacementApplicationAckPending
            }
            RetiredClientClearErrorKind::SupersessionHistoryOverflow => {
                Self::SupersessionHistoryOverflow
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RetiredQuinnActorCleared {
    generation: crate::resumable::LegGeneration,
    owned_messages: usize,
    owned_bytes: usize,
    terminal_failure: bool,
}

impl RetiredQuinnActorCleared {
    pub(crate) const fn generation(self) -> crate::resumable::LegGeneration {
        self.generation
    }

    pub(crate) const fn owned_messages(self) -> usize {
        self.owned_messages
    }

    pub(crate) const fn owned_bytes(self) -> usize {
        self.owned_bytes
    }

    pub(crate) const fn terminal_failure(self) -> bool {
        self.terminal_failure
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QuinnCurrentActorEnqueueUnavailableKind {
    FirstFailure,
    Closing,
    Unavailable,
}

/// Fully committed local TCP pair. The exact OPEN is already in the current
/// actor's sole FIFO at `enqueue_ordinal`; no caller can observe the pair at a
/// hybrid model-without-wire point.
pub(super) struct QuinnCommittedLocalTcpOpen {
    flow: ResumableTcpFlow,
    driver: ResumableTcpDriver,
    local_flow: crate::resumable::LocalFlow,
    enqueue_ordinal: u64,
}

impl QuinnCommittedLocalTcpOpen {
    fn from_controller(
        committed: ClientCommittedLocalOpen<(ResumableTcpFlow, ResumableTcpDriver)>,
    ) -> Self {
        let ((flow, driver), local_flow, enqueue_ordinal) = committed.into_parts();
        Self {
            flow,
            driver,
            local_flow,
            enqueue_ordinal,
        }
    }

    pub(super) fn into_parts(
        self,
    ) -> (
        ResumableTcpFlow,
        ResumableTcpDriver,
        crate::resumable::LocalFlow,
        u64,
    ) {
        (
            self.flow,
            self.driver,
            self.local_flow,
            self.enqueue_ordinal,
        )
    }
}

impl std::fmt::Debug for QuinnCommittedLocalTcpOpen {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("QuinnCommittedLocalTcpOpen")
            .field("flow", &"[REDACTED]")
            .field("driver", &"[REDACTED]")
            .field("local_flow", &self.local_flow)
            .field("enqueue_ordinal", &self.enqueue_ordinal)
            .finish()
    }
}

pub(super) enum QuinnPreparedLocalTcpOpenFailure {
    SessionShuttingDown(TargetAddr),
    ActorTransportUnavailable {
        target: TargetAddr,
        kind: QuinnCurrentActorEnqueueUnavailableKind,
    },
    PhysicalActorMismatch(TargetAddr),
    Controller(ClientPreparedLocalOpenFailure<FlowPortError>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum QuinnPreparedLocalTcpOpenFailureKind {
    SessionShuttingDown,
    ActorTransportUnavailable(QuinnCurrentActorEnqueueUnavailableKind),
    PhysicalActorMismatch,
    ControllerBlocked(ClientPreparedLocalOpenBlock),
    QueueCapacity,
    QueueEndpointLost,
    QueueInvariant,
    Factory(FlowPortError),
    ReducerCapacity(ReducerAdmissionBlock),
    ReducerOrSupervisor,
}

impl QuinnPreparedLocalTcpOpenFailure {
    pub(super) fn kind(&self) -> QuinnPreparedLocalTcpOpenFailureKind {
        match self {
            Self::SessionShuttingDown(_) => {
                QuinnPreparedLocalTcpOpenFailureKind::SessionShuttingDown
            }
            Self::ActorTransportUnavailable { kind, .. } => {
                QuinnPreparedLocalTcpOpenFailureKind::ActorTransportUnavailable(*kind)
            }
            Self::PhysicalActorMismatch(_) => {
                QuinnPreparedLocalTcpOpenFailureKind::PhysicalActorMismatch
            }
            Self::Controller(failure) => match failure {
                ClientPreparedLocalOpenFailure::Blocked { block, .. } => {
                    QuinnPreparedLocalTcpOpenFailureKind::ControllerBlocked(*block)
                }
                ClientPreparedLocalOpenFailure::Queue { kind, .. } => match kind {
                    LegOutboundQueueErrorKind::CapacityExceeded { .. } => {
                        QuinnPreparedLocalTcpOpenFailureKind::QueueCapacity
                    }
                    LegOutboundQueueErrorKind::EndpointLost => {
                        QuinnPreparedLocalTcpOpenFailureKind::QueueEndpointLost
                    }
                    _ => QuinnPreparedLocalTcpOpenFailureKind::QueueInvariant,
                },
                ClientPreparedLocalOpenFailure::Adapter { error, .. } => {
                    QuinnPreparedLocalTcpOpenFailureKind::Factory(*error)
                }
                ClientPreparedLocalOpenFailure::Supervisor(failure) => failure.block().map_or(
                    QuinnPreparedLocalTcpOpenFailureKind::ReducerOrSupervisor,
                    QuinnPreparedLocalTcpOpenFailureKind::ReducerCapacity,
                ),
            },
        }
    }

    pub(super) const fn target(&self) -> &TargetAddr {
        match self {
            Self::SessionShuttingDown(target) | Self::PhysicalActorMismatch(target) => target,
            Self::ActorTransportUnavailable { target, .. } => target,
            Self::Controller(failure) => failure.target(),
        }
    }

    pub(super) fn into_target(self) -> TargetAddr {
        match self {
            Self::SessionShuttingDown(target) | Self::PhysicalActorMismatch(target) => target,
            Self::ActorTransportUnavailable { target, .. } => target,
            Self::Controller(failure) => failure.into_target(),
        }
    }
}

impl std::fmt::Debug for QuinnPreparedLocalTcpOpenFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SessionShuttingDown(_) => formatter
                .debug_tuple("SessionShuttingDown")
                .field(&"[REDACTED]")
                .finish(),
            Self::ActorTransportUnavailable { kind, .. } => formatter
                .debug_struct("ActorTransportUnavailable")
                .field("target", &"[REDACTED]")
                .field("kind", kind)
                .finish(),
            Self::PhysicalActorMismatch(_) => formatter
                .debug_tuple("PhysicalActorMismatch")
                .field(&"[REDACTED]")
                .finish(),
            Self::Controller(failure) => {
                formatter.debug_tuple("Controller").field(failure).finish()
            }
        }
    }
}

impl QuinnCurrentActorEnqueueUnavailableKind {
    const fn from_actor_error(error: &QuinnLegActorError) -> Self {
        match error {
            QuinnLegActorError::Closing => Self::Closing,
            QuinnLegActorError::AlreadyJoined | QuinnLegActorError::MissingPhysicalIncarnation => {
                Self::Unavailable
            }
            _ => Self::FirstFailure,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum QuinnCurrentEnqueueErrorKind {
    SessionShuttingDown,
    ActorTransportUnavailable(QuinnCurrentActorEnqueueUnavailableKind),
    MissingCurrent,
    Queue(Box<LegOutboundQueueErrorKind>),
}

pub(crate) struct QuinnCurrentEnqueueError {
    kind: QuinnCurrentEnqueueErrorKind,
    frame: Frame,
}

impl QuinnCurrentEnqueueError {
    fn new(kind: QuinnCurrentEnqueueErrorKind, frame: Frame) -> Self {
        Self { kind, frame }
    }

    pub(crate) const fn kind(&self) -> &QuinnCurrentEnqueueErrorKind {
        &self.kind
    }

    pub(crate) fn into_frame(self) -> Frame {
        self.frame
    }
}

pub(crate) struct QuinnCurrentProbeQueued(ClientCurrentProbeQueued);

impl QuinnCurrentProbeQueued {
    pub(crate) fn generation(&self) -> crate::resumable::LegGeneration {
        self.0.generation()
    }

    pub(crate) fn probe_sequence(&self) -> crate::resumable::ProbeSequence {
        self.0.probe_sequence()
    }
}

impl std::fmt::Debug for QuinnCurrentProbeQueued {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("QuinnCurrentProbeQueued([REDACTED])")
    }
}

#[derive(Debug)]
pub(crate) enum QuinnCurrentProbeBeginError {
    SessionShuttingDown,
    ActorTransportUnavailable,
    CompositionInvariant,
    Controller(ClientCurrentProbeBeginErrorKind),
}

#[derive(Debug)]
pub(crate) enum QuinnCurrentProbeArmError {
    SessionShuttingDown(QuinnCurrentProbeQueued),
    CompositionInvariant(QuinnCurrentProbeQueued),
    Stale(QuinnCurrentProbeQueued),
    NotDelivered(QuinnCurrentProbeQueued),
    InvalidTimeout(QuinnCurrentProbeQueued),
    Deadline {
        queued: QuinnCurrentProbeQueued,
        error: crate::owned_upstream::two_leg::SimTimeError,
    },
    Actor {
        queued: QuinnCurrentProbeQueued,
        error: QuinnLegActorError,
    },
}

impl QuinnCurrentProbeArmError {
    fn from_controller(failure: ClientCurrentProbeArmFailure) -> Self {
        let (queued, error) = failure.into_parts();
        let queued = QuinnCurrentProbeQueued(queued);
        match error {
            ClientCurrentProbeArmError::Stale => Self::Stale(queued),
            ClientCurrentProbeArmError::NotDelivered => Self::NotDelivered(queued),
            ClientCurrentProbeArmError::InvalidTimeout => Self::InvalidTimeout(queued),
            ClientCurrentProbeArmError::Deadline(error) => Self::Deadline { queued, error },
        }
    }
}

/// Opaque scheduler-owned token. Its private inner controller authority is
/// neither cloneable nor reconstructable from time or wire correlations.
pub(crate) struct QuinnCurrentProbeDeadline(ClientCurrentProbeDeadline);

impl QuinnCurrentProbeDeadline {
    /// Opaque scheduler deadline value. This exposes no probe correlation,
    /// identity, generation, or authority and cannot recreate the token.
    pub(crate) const fn instant(&self) -> SimTime {
        self.0.deadline()
    }
}

impl QuinnLegActorArrivalDeadline for QuinnCurrentProbeDeadline {
    fn arrival_deadline(&self) -> SimTime {
        self.instant()
    }
}

impl QuinnLegActorArrivalDeadline for ClientCurrentProbeDeadline {
    fn arrival_deadline(&self) -> SimTime {
        self.deadline()
    }
}

impl std::fmt::Debug for QuinnCurrentProbeDeadline {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("QuinnCurrentProbeDeadline([REDACTED])")
    }
}

pub(crate) enum QuinnCurrentProbeSchedulerAuthority {
    Queued(QuinnCurrentProbeQueued),
    Armed(QuinnCurrentProbeDeadline),
}

impl QuinnCurrentProbeSchedulerAuthority {
    fn into_controller(self) -> ClientCurrentProbeSchedulerAuthority {
        match self {
            Self::Queued(queued) => ClientCurrentProbeSchedulerAuthority::Queued(queued.0),
            Self::Armed(deadline) => ClientCurrentProbeSchedulerAuthority::Armed(deadline.0),
        }
    }

    fn from_controller(authority: ClientCurrentProbeSchedulerAuthority) -> Self {
        match authority {
            ClientCurrentProbeSchedulerAuthority::Queued(queued) => {
                Self::Queued(QuinnCurrentProbeQueued(queued))
            }
            ClientCurrentProbeSchedulerAuthority::Armed(deadline) => {
                Self::Armed(QuinnCurrentProbeDeadline(deadline))
            }
        }
    }

    const fn is_deadline(&self) -> bool {
        matches!(self, Self::Armed(_))
    }
}

impl std::fmt::Debug for QuinnCurrentProbeSchedulerAuthority {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("QuinnCurrentProbeSchedulerAuthority([REDACTED])")
    }
}

pub(crate) struct QuinnCurrentProbeAckReconcileFailure {
    scheduler: QuinnCurrentProbeSchedulerAuthority,
    kind: ClientCurrentProbeAckReconcileErrorKind,
}

impl QuinnCurrentProbeAckReconcileFailure {
    pub(crate) const fn kind(&self) -> ClientCurrentProbeAckReconcileErrorKind {
        self.kind
    }

    pub(crate) fn into_scheduler(self) -> QuinnCurrentProbeSchedulerAuthority {
        self.scheduler
    }
}

impl std::fmt::Debug for QuinnCurrentProbeAckReconcileFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("QuinnCurrentProbeAckReconcileFailure")
            .field("kind", &self.kind)
            .field("scheduler", &"[REDACTED]")
            .finish()
    }
}

pub(crate) struct QuinnCurrentObservationSettlementFailure {
    scheduler: QuinnCurrentProbeSchedulerAuthority,
    kind: ClientControllerObservationSettlementErrorKind,
}

impl QuinnCurrentObservationSettlementFailure {
    fn from_controller(failure: ClientControllerRequiredObservationSettlementFailure) -> Self {
        let kind = failure.kind();
        Self {
            scheduler: QuinnCurrentProbeSchedulerAuthority::from_controller(
                failure.into_scheduler(),
            ),
            kind,
        }
    }

    pub(crate) const fn kind(&self) -> ClientControllerObservationSettlementErrorKind {
        self.kind
    }

    pub(crate) fn into_scheduler(self) -> QuinnCurrentProbeSchedulerAuthority {
        self.scheduler
    }
}

impl std::fmt::Debug for QuinnCurrentObservationSettlementFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("QuinnCurrentObservationSettlementFailure")
            .field("kind", &self.kind)
            .field("scheduler", &"[REDACTED]")
            .finish()
    }
}

/// Opaque, non-cloneable root timer for one exact reducer terminal tombstone.
/// The absolute deadline is stamped once when `FlowFinished` is first
/// scheduled; every retry moves this value and therefore cannot extend it.
pub(crate) struct QuinnTerminalGraceTimer {
    terminal: TerminalGrace,
    deadline: SimTime,
    inbound_publication_cutoff: Option<u64>,
    reset_delivery: Option<OrderedResetDeliveryReceipt>,
}

impl QuinnTerminalGraceTimer {
    pub(crate) const fn new(terminal: TerminalGrace, deadline: SimTime) -> Self {
        Self {
            terminal,
            deadline,
            inbound_publication_cutoff: None,
            reset_delivery: None,
        }
    }

    pub(crate) fn new_with_ordered_reset_delivery(
        terminal: TerminalGrace,
        deadline: SimTime,
    ) -> Self {
        Self {
            terminal,
            deadline,
            inbound_publication_cutoff: None,
            reset_delivery: Some(OrderedResetDeliveryReceipt::pending(terminal.flow_id())),
        }
    }

    pub(crate) const fn instant(&self) -> SimTime {
        self.deadline
    }

    pub(crate) const fn flow_id(&self) -> SessionFlowId {
        self.terminal.flow_id()
    }

    pub(super) fn reset_delivery_for_queue(
        &self,
        flow_id: SessionFlowId,
    ) -> Option<OrderedResetDeliveryReceipt> {
        self.reset_delivery
            .as_ref()
            .filter(|receipt| receipt.flow_id() == flow_id)
            .map(OrderedResetDeliveryReceipt::clone_for_queue)
    }

    pub(super) fn try_commit_reset_delivery_deadline(&self) -> OrderedResetDeadlineCommit {
        self.reset_delivery.as_ref().map_or(
            OrderedResetDeadlineCommit::Delivered,
            OrderedResetDeliveryReceipt::try_commit_deadline,
        )
    }

    fn reset_delivery_deadline_won(&self) -> bool {
        self.reset_delivery
            .as_ref()
            .is_some_and(OrderedResetDeliveryReceipt::deadline_won)
    }

    pub(crate) fn reset_delivery_pending(&self) -> bool {
        self.reset_delivery
            .as_ref()
            .is_some_and(OrderedResetDeliveryReceipt::is_pending)
    }

    fn expire(
        self,
        controller: &mut ClientSessionController,
    ) -> Result<Vec<SessionEffect>, (Self, SessionSupervisorError)> {
        let Self {
            terminal,
            deadline,
            inbound_publication_cutoff,
            reset_delivery,
        } = self;
        controller
            .expire_terminal_grace_preserving(terminal)
            .map_err(|(terminal, error)| {
                (
                    Self {
                        terminal,
                        deadline,
                        inbound_publication_cutoff,
                        reset_delivery,
                    },
                    error,
                )
            })
    }
}

impl QuinnLegActorArrivalDeadline for QuinnTerminalGraceTimer {
    fn arrival_deadline(&self) -> SimTime {
        self.instant()
    }
}

impl QuinnLegActorSemanticTimer for QuinnTerminalGraceTimer {
    fn capture_inbound_publication_cutoff(&mut self, high_water: u64) -> u64 {
        *self.inbound_publication_cutoff.get_or_insert(high_water)
    }
}

impl std::fmt::Debug for QuinnTerminalGraceTimer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("QuinnTerminalGraceTimer([REDACTED])")
    }
}

pub(crate) enum QuinnTerminalGraceTimerDrive {
    NotDue(QuinnTerminalGraceTimer),
    PublicationPending(QuinnTerminalGraceTimer),
    Receive {
        outcome: QuinnCurrentInboundDrive,
        timer: QuinnTerminalGraceTimer,
    },
    Expired {
        effects: Vec<SessionEffect>,
    },
    ResetDeliveryDeadlineWon(QuinnTerminalGraceTimer),
    /// Unknown, cross-session, or duplicate terminal authority. This is not
    /// normal reducer pressure: the root must make the error sticky while
    /// retaining the exact timer for structural cleanup accounting.
    ReducerInvariant {
        timer: QuinnTerminalGraceTimer,
        error: SessionSupervisorError,
    },
    ActorFailure {
        timer: QuinnTerminalGraceTimer,
        cause: Arc<QuinnLegActorError>,
    },
    SessionShuttingDown(QuinnTerminalGraceTimer),
    CompositionInvariant(QuinnTerminalGraceTimer),
}

#[cfg(test)]
#[derive(Debug)]
pub(crate) enum QuinnCurrentProbeExpiry {
    Stale(QuinnCurrentProbeDeadline),
    NotDue(QuinnCurrentProbeDeadline),
    ProbeAcked {
        deadline: QuinnCurrentProbeDeadline,
        generation: crate::resumable::LegGeneration,
        probe_sequence: crate::resumable::ProbeSequence,
    },
    Committed {
        generation: crate::resumable::LegGeneration,
        probe_sequence: crate::resumable::ProbeSequence,
    },
    ActorFailureWon {
        deadline: QuinnCurrentProbeDeadline,
        cause: Arc<QuinnLegActorError>,
    },
    SessionShuttingDown(QuinnCurrentProbeDeadline),
    CompositionInvariant(QuinnCurrentProbeDeadline),
}

pub(crate) enum QuinnCurrentProbeDeadlineDrive {
    Stale(QuinnCurrentProbeDeadline),
    NotDue(QuinnCurrentProbeDeadline),
    /// One complete actor record is still between its bounded read and queue
    /// publication. The exact token is returned for the post-wake rescan.
    PublicationPending(QuinnCurrentProbeDeadline),
    /// One bounded receive attempt won this turn. Expiry authority remains
    /// with the returned exact token for root-slot reconciliation.
    Receive {
        outcome: QuinnCurrentInboundDrive,
        deadline: QuinnCurrentProbeDeadline,
    },
    ProbeAcked {
        deadline: QuinnCurrentProbeDeadline,
        generation: crate::resumable::LegGeneration,
        probe_sequence: crate::resumable::ProbeSequence,
    },
    Committed {
        generation: crate::resumable::LegGeneration,
        probe_sequence: crate::resumable::ProbeSequence,
    },
    ActorFailureWon {
        deadline: QuinnCurrentProbeDeadline,
        cause: Arc<QuinnLegActorError>,
    },
    SessionShuttingDown(QuinnCurrentProbeDeadline),
    CompositionInvariant(QuinnCurrentProbeDeadline),
}

pub(crate) enum QuinnCurrentInboundDrive {
    Idle,
    PolicyBlocked,
    Applied {
        effects: Vec<SessionEffect>,
        observation: Option<ClientInboundObservation>,
    },
    ProbeAcked {
        generation: crate::resumable::LegGeneration,
        probe_sequence: crate::resumable::ProbeSequence,
    },
    Rejected,
    ReducerRejected(SessionSupervisorError),
    SessionShuttingDown,
    MissingCurrent,
    CompositionInvariant,
    ActorFailed(QuinnLegActorError),
}

pub(super) struct QuinnClientDriverInputRejected {
    input: ClientControllerDriverInput,
}

impl QuinnClientDriverInputRejected {
    pub(super) fn into_input(self) -> ClientControllerDriverInput {
        self.input
    }
}

impl std::fmt::Debug for QuinnClientDriverInputRejected {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("QuinnClientDriverInputRejected([REDACTED])")
    }
}

pub(crate) enum QuinnRetiredInboundDrive {
    Idle,
    Retired(RetiredClientInboundKind),
    Rejected,
    SessionShuttingDown,
    MissingRetired,
    NotRunning,
    CompositionInvariant,
    ActorFailed(QuinnLegActorError),
}

impl std::fmt::Debug for QuinnCurrentEnqueueError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("QuinnCurrentEnqueueError")
            .field("kind", &self.kind)
            .field("frame", &"[REDACTED]")
            .finish()
    }
}

impl std::fmt::Display for QuinnCurrentEnqueueError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.kind {
            QuinnCurrentEnqueueErrorKind::SessionShuttingDown => {
                formatter.write_str("current frame enqueue rejected: session is shutting down")
            }
            QuinnCurrentEnqueueErrorKind::ActorTransportUnavailable(_) => formatter
                .write_str("current frame enqueue rejected: actor transport is unavailable"),
            QuinnCurrentEnqueueErrorKind::MissingCurrent => {
                formatter.write_str("current frame enqueue rejected: controller has no current leg")
            }
            QuinnCurrentEnqueueErrorKind::Queue(_) => {
                formatter.write_str("current frame enqueue rejected by the bounded leg queue")
            }
        }
    }
}

#[cfg(test)]
pub(super) struct QuinnCurrentOrderedControlEnqueued(
    crate::owned_upstream::leg_io::LegControlEnqueued,
);

#[cfg(test)]
impl QuinnCurrentOrderedControlEnqueued {
    pub(super) fn was_delivered(&self) -> bool {
        self.0.was_delivered()
    }
}

#[cfg(test)]
pub(super) enum QuinnCurrentControlEnqueueError {
    SessionShuttingDown,
    ActorTransportUnavailable,
    MissingCurrent,
    Queue(Box<crate::owned_upstream::leg_io::LegControlQueueError>),
}

#[cfg(test)]
impl std::fmt::Debug for QuinnCurrentControlEnqueueError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SessionShuttingDown => {
                formatter.write_str("QuinnCurrentControlEnqueueError::SessionShuttingDown")
            }
            Self::ActorTransportUnavailable => {
                formatter.write_str("QuinnCurrentControlEnqueueError::ActorTransportUnavailable")
            }
            Self::MissingCurrent => {
                formatter.write_str("QuinnCurrentControlEnqueueError::MissingCurrent")
            }
            Self::Queue(error) => formatter.debug_tuple("Queue").field(error).finish(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QuinnCurrentFlushDisposition {
    Idle,
    Submitted,
    OrderedDeliveryPending,
}

#[derive(Debug)]
pub(crate) enum QuinnCurrentFlushError {
    SessionShuttingDown,
    ActorTransportUnavailable,
    LegIo(LegIoError),
}

#[derive(Debug)]
pub(crate) enum QuinnRecoveryTailSettlementError {
    SessionShuttingDown,
    Actor(QuinnLegActorError),
    Controller(ClientRecoveryTailSettlementBlock),
}

#[derive(Debug)]
pub(crate) enum QuinnCurrentJoinError {
    CompositionInvariant,
    RuntimeUnavailable,
    RetiredJoinTask(Arc<str>),
    RetiredActor(QuinnLegActorError),
    RetiredController(QuinnRetiredControllerClearErrorKind),
    StandbyJoinTask(Arc<str>),
    StandbyActor(QuinnLegActorError),
    StandbyPendingControl {
        kind: QuinnStandbyPendingControlKind,
        messages: usize,
        bytes: usize,
    },
    StandbyOutboundRetirementPending {
        queued_frames: usize,
        queued_bytes: usize,
        ordered_delivery_wait: bool,
    },
    StandbyResidualOwnership {
        messages: usize,
        bytes: usize,
        terminals: usize,
        terminal_failure: bool,
    },
    StandbyTerminalRejected {
        reason: Option<LegTransportTerminalReason>,
        terminal_failure: bool,
    },
    StandbyStateRetirementPending {
        kind: QuinnStandbyShutdownStateKind,
    },
    StandbyJoinedWitnessMissing,
    CurrentTerminalDiscardPending,
    CurrentTerminalDiscardFailed(ClientCurrentTerminalDiscardErrorKind),
    CurrentTerminalAppliedIdentityMismatch,
    CurrentShutdownClearFailed(ClientCurrentShutdownClearErrorKind),
    CurrentObservationSettlement(ClientControllerObservationSettlementErrorKind),
    CurrentControllerOwnership,
    RecoveryTailSettlement(QuinnRecoveryTailSettlementError),
    Actor(QuinnLegActorError),
    JoinedWitnessMissing,
    CurrentControllerResidual {
        active: bool,
        queued_frames: usize,
        owned_bytes: usize,
        ordered_delivery_wait: bool,
    },
}

fn current_terminal_error_for_join(error: QuinnCurrentTerminalError) -> QuinnCurrentJoinError {
    match error {
        QuinnCurrentTerminalError::Actor(error) => QuinnCurrentJoinError::Actor(error),
        QuinnCurrentTerminalError::ControllerDiscard(kind) => {
            QuinnCurrentJoinError::CurrentTerminalDiscardFailed(kind)
        }
        QuinnCurrentTerminalError::SessionShuttingDown
        | QuinnCurrentTerminalError::CompositionInvariant
        | QuinnCurrentTerminalError::UnexpectedLocalCancellation
        | QuinnCurrentTerminalError::MissingTerminalFailureTimestamp
        | QuinnCurrentTerminalError::ControllerBind(_) => {
            QuinnCurrentJoinError::CompositionInvariant
        }
    }
}

pub(super) struct QuinnCompositionShutdownOutcome {
    current: Option<CurrentQuinnActorJoined>,
    controller_ownership: ClientControllerOwnershipLedger,
    first_error: Option<QuinnCurrentJoinError>,
}

/// Exact live ownership retained by one or more physically joined Quinn
/// actors. The root-facing aggregate uses checked arithmetic; actor fatal is a
/// sticky fact and therefore combines with logical OR rather than disappearing
/// when the terminal capability itself is absent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct QuinnActorJoinedInventory {
    owned_messages: usize,
    owned_bytes: usize,
    owned_terminals: usize,
    terminal_failure: bool,
}

impl QuinnActorJoinedInventory {
    const ZERO: Self = Self {
        owned_messages: 0,
        owned_bytes: 0,
        owned_terminals: 0,
        terminal_failure: false,
    };

    fn from_actor(joined: &QuinnLegActorJoined) -> Self {
        Self {
            owned_messages: joined.owned_messages(),
            owned_bytes: joined.owned_bytes(),
            owned_terminals: joined.owned_terminals(),
            terminal_failure: joined.terminal_failure().is_some(),
        }
    }

    const fn checked_add(self, other: Self) -> Option<Self> {
        let Some(owned_messages) = self.owned_messages.checked_add(other.owned_messages) else {
            return None;
        };
        let Some(owned_bytes) = self.owned_bytes.checked_add(other.owned_bytes) else {
            return None;
        };
        let Some(owned_terminals) = self.owned_terminals.checked_add(other.owned_terminals) else {
            return None;
        };
        Some(Self {
            owned_messages,
            owned_bytes,
            owned_terminals,
            terminal_failure: self.terminal_failure || other.terminal_failure,
        })
    }

    const fn is_clean(self) -> bool {
        self.owned_messages == 0
            && self.owned_bytes == 0
            && self.owned_terminals == 0
            && !self.terminal_failure
    }

    const fn owned_messages(self) -> usize {
        self.owned_messages
    }

    const fn owned_bytes(self) -> usize {
        self.owned_bytes
    }

    const fn owned_terminals(self) -> usize {
        self.owned_terminals
    }

    const fn terminal_failure(self) -> bool {
        self.terminal_failure
    }

    #[cfg(test)]
    const fn for_test(
        owned_messages: usize,
        owned_bytes: usize,
        owned_terminals: usize,
        terminal_failure: bool,
    ) -> Self {
        Self {
            owned_messages,
            owned_bytes,
            owned_terminals,
            terminal_failure,
        }
    }
}

impl QuinnCompositionShutdownOutcome {
    pub(crate) const fn controller_ownership_ledger(&self) -> ClientControllerOwnershipLedger {
        self.controller_ownership
    }

    pub(super) fn into_parts(
        self,
    ) -> (
        Option<CurrentQuinnActorJoined>,
        Option<QuinnCurrentJoinError>,
    ) {
        (self.current, self.first_error)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct CurrentQuinnActorJoined {
    owned_messages: usize,
    owned_bytes: usize,
    owned_terminals: usize,
    terminal_failure: bool,
}

impl CurrentQuinnActorJoined {
    const fn from_inventory(inventory: QuinnActorJoinedInventory) -> Self {
        Self {
            owned_messages: inventory.owned_messages(),
            owned_bytes: inventory.owned_bytes(),
            owned_terminals: inventory.owned_terminals(),
            terminal_failure: inventory.terminal_failure(),
        }
    }

    pub(crate) const fn owned_messages(self) -> usize {
        self.owned_messages
    }

    pub(crate) const fn owned_bytes(self) -> usize {
        self.owned_bytes
    }

    pub(crate) const fn owned_terminals(self) -> usize {
        self.owned_terminals
    }

    pub(crate) const fn terminal_failure(self) -> bool {
        self.terminal_failure
    }
}

#[cfg(test)]
mod queue_classifier_tests {
    use super::{
        QuinnOwnedStandbyInstallAction, QuinnOwnedStandbyInstallError,
        QuinnRegisteredReplacementErrorKind, QuinnStandbyQueueFailureKind,
        classify_leg_control_enqueue, classify_registered_attach_enqueue,
    };
    use crate::owned_upstream::leg_io::{LegControlQueueErrorKind, LegOutboundQueueErrorKind};
    use crate::resumable::ProtocolError;

    #[test]
    fn owned_install_root_action_matrix_exposes_one_exact_action() {
        assert_eq!(
            QuinnOwnedStandbyInstallError::SessionShuttingDown.action(),
            QuinnOwnedStandbyInstallAction::SessionFatal,
        );
        assert_eq!(
            QuinnOwnedStandbyInstallError::MissingStandby.action(),
            QuinnOwnedStandbyInstallAction::Invariant,
        );
        assert_eq!(
            QuinnOwnedStandbyInstallError::StandbyFailed {
                action: QuinnOwnedStandbyInstallAction::StandbyTerminal,
            }
            .action(),
            QuinnOwnedStandbyInstallAction::StandbyTerminal,
        );
        assert_eq!(
            QuinnOwnedStandbyInstallError::Replacement {
                kind: QuinnRegisteredReplacementErrorKind::Controller(
                    crate::owned_upstream::client_controller::ClientRecoveryBeginErrorKind::InstallRejected,
                ),
                action: QuinnOwnedStandbyInstallAction::Retryable,
            }
            .action(),
            QuinnOwnedStandbyInstallAction::Retryable,
        );
    }

    #[test]
    fn registered_b_queue_failure_matrix_has_only_explicit_retryable_cases() {
        assert_eq!(
            classify_leg_control_enqueue(&LegControlQueueErrorKind::CapacityExceeded {
                queued_frames: 1,
                queued_bytes: 2,
                incoming_bytes: 3,
            }),
            Ok(()),
        );
        assert_eq!(
            classify_leg_control_enqueue(&LegControlQueueErrorKind::EndpointLost),
            Err(QuinnStandbyQueueFailureKind::EndpointLost),
        );
        let control_invariants = [
            LegControlQueueErrorKind::InvalidFrame(ProtocolError::InvalidMagic),
            LegControlQueueErrorKind::ByteCountOverflow,
            LegControlQueueErrorKind::EnqueueOrdinalExhausted,
            LegControlQueueErrorKind::WrongLeg,
            LegControlQueueErrorKind::AttachAcceptanceReserved,
            LegControlQueueErrorKind::AttachRequestPending,
            LegControlQueueErrorKind::AttachStatusPending,
            LegControlQueueErrorKind::AttachRecoveryPending,
            LegControlQueueErrorKind::ClientReplacementRecoveryPending,
            LegControlQueueErrorKind::ClientRecoveryTerminal,
        ];
        for kind in &control_invariants {
            assert_eq!(
                classify_leg_control_enqueue(kind),
                Err(QuinnStandbyQueueFailureKind::Invariant),
                "control queue invariant was accidentally made retryable: {kind:?}",
            );
        }

        for retryable in [
            LegOutboundQueueErrorKind::CapacityExceeded {
                queued_frames: 1,
                queued_bytes: 2,
                incoming_bytes: 3,
            },
            LegOutboundQueueErrorKind::AttachRequestNotFirst {
                next_ordinal: 4,
                queued_frames: 1,
                queued_bytes: 2,
            },
        ] {
            assert_eq!(classify_registered_attach_enqueue(&retryable), Ok(()));
        }
        assert_eq!(
            classify_registered_attach_enqueue(&LegOutboundQueueErrorKind::EndpointLost),
            Err(QuinnStandbyQueueFailureKind::EndpointLost),
        );
        let attach_invariants = [
            LegOutboundQueueErrorKind::InvalidFrame(ProtocolError::InvalidMagic),
            LegOutboundQueueErrorKind::ByteCountOverflow,
            LegOutboundQueueErrorKind::EnqueueOrdinalExhausted,
            LegOutboundQueueErrorKind::NotAttachAcceptance,
            LegOutboundQueueErrorKind::RegisteredAttachAdmissionRequired,
            LegOutboundQueueErrorKind::AttachAcceptanceAdmissionRequired,
            LegOutboundQueueErrorKind::AuthenticatedAttachStatusAdmissionRequired,
            LegOutboundQueueErrorKind::AttachAcceptanceReserved,
            LegOutboundQueueErrorKind::AttachRequestPending,
            LegOutboundQueueErrorKind::AttachStatusPending,
            LegOutboundQueueErrorKind::ClientReplacementRecoveryPending,
            LegOutboundQueueErrorKind::ClientRecoveryTerminal,
            LegOutboundQueueErrorKind::NotAttachRequest,
            LegOutboundQueueErrorKind::NotAttachStatus,
            LegOutboundQueueErrorKind::WrongLeg,
            LegOutboundQueueErrorKind::WrongAttachAcceptanceReservation,
            LegOutboundQueueErrorKind::MismatchedAttachRequest,
            LegOutboundQueueErrorKind::MismatchedAttachAcceptance,
            LegOutboundQueueErrorKind::AttachStatusNotFirst {
                next_ordinal: 4,
                queued_frames: 1,
                queued_bytes: 2,
            },
            LegOutboundQueueErrorKind::AttachAcceptanceNotFirst {
                next_ordinal: 4,
                queued_frames: 1,
                queued_bytes: 2,
            },
        ];
        for kind in &attach_invariants {
            assert_eq!(
                classify_registered_attach_enqueue(kind),
                Err(QuinnStandbyQueueFailureKind::Invariant),
                "registered ATTACH invariant was accidentally made retryable: {kind:?}",
            );
        }
    }
}
