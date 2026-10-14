//! Single-owner client replacement and recovery transaction.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use super::leg::{
    BoundInbound, ClientActiveLeg, EstablishedLeg, ExactLegTerminal, LegSeal,
    LegTransportTerminalReason, PendingClientLegLoss, RetiredClientInboundKind, RetiredClientLeg,
    TerminalBoundRetiredClientLeg,
};
use super::leg_io::{
    AcceptedInitialAttach, AcceptedInitialAttachActivationFailure, ClientLegTransport,
    ClientLegTransportBuildFailure, ClientLegTransportResidual,
    ClientOutboundQueueSupersessionAudit, ClientRecoveryBudgetSnapshot,
    ClientRecoveryEnqueueErrorKind, ClientRecoverySupersessionManifest, ClientRecoveryTail,
    ClientRecoveryTailSettlementErrorKind, EncodedLegTransport, InitialAttachActivationError,
    InstalledClientRecoveryReservation, LegControlEnqueued, LegControlQueueError, LegIo,
    LegIoError, LegOutboundQueue, LegOutboundQueueError, LegOutboundQueueErrorKind,
    OrderedResetDeliveryReceipt, OutboundSubmission, PhysicalLegRuntimeIncarnation,
    PreparedClientRecoveryEffect, PreparedClientReplayRanges, PreparedRetiredClientTransport,
    RetiredClientTransport,
};
#[cfg(test)]
use super::leg_io::{ClientRecoveryTailInventory, LegIoCounters};
use super::quinn_leg_composition::{QuinnFatalRecoveryTailShutdownGate, QuinnLegCompositionGate};
use super::session::SessionOwnerCommand;
use super::standby::{
    AcceptedClientInstallErrorKind, AcceptedClientRegisteredAttach,
    AcceptedClientRegisteredCatchUp, AcceptedClientRegisteredCatchUpInstallFailure,
    AcceptedClientRegisteredInstallFailure, ExactLegProbeResponder, PendingStandbyRegistration,
    StandbyRegistrationError,
};
use super::supervisor::{
    PreparedClientLocalOpenSupervisorFailure, ReducerAdmissionBlock, ReducerClientObservations,
    ReducerFlowDirectionTerminal, ReducerReplayBecameOutstanding, ReducerSessionExpired,
    ReducerSinkAcceptedFrontier, SessionSupervisor, SessionSupervisorError,
    SessionSupervisorSnapshot, TcpPortFactoryMintError,
};
use super::tcp::{ClientControllerDriverInput, FlowPortConfig, ResumableTcpPortFactory};
use super::two_leg::{EncodedDelivery, SimTime, SimTimeError, WireRoute};
use crate::resumable::{
    AttachCredentials, ByteOffset, Direction, FeatureSet, Frame, LegControlFrame, LegControlRecord,
    LegEpochNonce, LegGeneration, LocalFlow, PreparedLocalOpenInput, ProbeSequence, ProtocolError,
    SessionConfig, SessionEffect, SessionEvent, SessionFlowId, SessionId, StandbyNonce,
    TerminalGrace,
};
use crate::shared::TargetAddr;

/// One-shot module authority preventing accepted registered wrappers from
/// exposing any raw attached leg or reducer candidate to sibling adapters.
pub(super) struct ClientControllerInstallGate {
    _private: (),
}

impl ClientControllerInstallGate {
    fn mint() -> Self {
        Self { _private: () }
    }
}

/// Constructor authority for the controller-owned endpoint/FIFO bundle.
pub(super) struct ClientControllerTransportGate {
    _private: (),
}

impl ClientControllerTransportGate {
    fn mint() -> Self {
        Self { _private: () }
    }
}

/// Non-forgeable authority for the one initial client publication. Sibling
/// modules may require this type but cannot construct its private value.
pub(super) struct ClientControllerBootstrapGate {
    _private: (),
}

impl ClientControllerBootstrapGate {
    fn mint() -> Self {
        Self { _private: () }
    }
}

/// Read-only reducer observation authority. Only the controller may request
/// application-ACK progress after the supervisor has consumed its receipt.
pub(super) struct ClientControllerObservedApplyGate {
    _private: (),
}

impl ClientControllerObservedApplyGate {
    fn mint() -> Self {
        Self { _private: () }
    }
}

/// Minted only after the controller consumes the current leg's exact terminal
/// fact. Queue code requires this before releasing post-commit ownership.
pub(super) struct ClientRecoveryTerminalGate {
    _private: (),
}

impl ClientRecoveryTerminalGate {
    fn mint() -> Self {
        Self { _private: () }
    }
}

struct ActiveClientTransport {
    authority: ClientActiveLeg,
    transport: ClientLegTransport,
}

pub(super) enum ClientCurrentFrameEnqueueFailure {
    MissingCurrent(Frame),
    Queue(LegOutboundQueueError),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ClientPreparedLocalOpenBlock {
    FactoryAdmissionClosed,
    MissingCurrent,
    RecoveryPending,
    RecoveryTailPending,
    Terminating,
    Legless,
}

/// Every prepared-OPEN rejection retains the exact caller Target. Adapter
/// ownership remains nested in its private failure and is never formatted.
pub(super) enum ClientPreparedLocalOpenFailure<E> {
    Blocked {
        target: TargetAddr,
        block: ClientPreparedLocalOpenBlock,
    },
    Queue {
        target: TargetAddr,
        frame: Box<Frame>,
        kind: LegOutboundQueueErrorKind,
    },
    Adapter {
        target: TargetAddr,
        frame: Box<Frame>,
        error: E,
    },
    Supervisor(PreparedClientLocalOpenSupervisorFailure),
}

impl<E> fmt::Debug for ClientPreparedLocalOpenFailure<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Blocked { block, .. } => formatter
                .debug_struct("ClientPreparedLocalOpenFailure::Blocked")
                .field("target", &"[REDACTED]")
                .field("block", block)
                .finish(),
            Self::Queue { kind, .. } => formatter
                .debug_struct("ClientPreparedLocalOpenFailure::Queue")
                .field("target", &"[REDACTED]")
                .field("kind", kind)
                .finish(),
            Self::Adapter { .. } => formatter
                .debug_struct("ClientPreparedLocalOpenFailure::Adapter")
                .field("target", &"[REDACTED]")
                .field("error", &"[REDACTED]")
                .finish(),
            Self::Supervisor(failure) => formatter
                .debug_tuple("ClientPreparedLocalOpenFailure::Supervisor")
                .field(failure)
                .finish(),
        }
    }
}

impl<E> ClientPreparedLocalOpenFailure<E> {
    pub(super) const fn target(&self) -> &TargetAddr {
        match self {
            Self::Blocked { target, .. }
            | Self::Queue { target, .. }
            | Self::Adapter { target, .. } => target,
            Self::Supervisor(failure) => failure.target(),
        }
    }

    pub(super) fn into_target(self) -> TargetAddr {
        match self {
            Self::Blocked { target, .. }
            | Self::Queue { target, .. }
            | Self::Adapter { target, .. } => target,
            Self::Supervisor(failure) => failure.into_parts().0,
        }
    }
}

enum ClientLocalOpenAdapterPrepareError<E> {
    Queue(LegOutboundQueueErrorKind),
    Adapter(E),
}

/// One reducer flow, one TCP adapter object, and the exact OPEN's committed
/// position in the current leg FIFO. Construction is private so callers
/// cannot observe a reducer/factory pair before its OPEN is ordered.
pub(super) struct ClientCommittedLocalOpen<T> {
    prepared_adapter: T,
    local_flow: LocalFlow,
    enqueue_ordinal: u64,
}

impl<T> ClientCommittedLocalOpen<T> {
    pub(super) fn into_parts(self) -> (T, LocalFlow, u64) {
        (self.prepared_adapter, self.local_flow, self.enqueue_ordinal)
    }
}

impl<T> fmt::Debug for ClientCommittedLocalOpen<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClientCommittedLocalOpen")
            .field("prepared_adapter", &"[REDACTED]")
            .field("local_flow", &self.local_flow)
            .field("enqueue_ordinal", &self.enqueue_ordinal)
            .finish()
    }
}

/// Ownership-preserving result for the production typed source seam. Reducer
/// pressure is retryable only because the exact non-cloneable input is
/// returned; invariant/protocol failures remain ordinary fatal errors.
pub(super) enum ClientControllerDriverApply {
    Applied(Vec<SessionEffect>),
    Deferred {
        block: ReducerAdmissionBlock,
        input: ClientControllerDriverInput,
    },
    Failed(SessionSupervisorError),
}

/// Client-facing form of the reducer's exact committed local sink frontier.
/// The wrapper is intentionally not `Clone`; it can only leave one observed
/// controller turn by ownership.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct ClientSinkAcceptedFrontier(ReducerSinkAcceptedFrontier);

impl ClientSinkAcceptedFrontier {
    pub(super) const fn session_id(&self) -> SessionId {
        self.0.session_id()
    }

    pub(super) const fn flow_id(&self) -> SessionFlowId {
        self.0.flow_id()
    }

    pub(super) const fn generation(&self) -> LegGeneration {
        self.0.generation()
    }

    pub(super) const fn direction(&self) -> Direction {
        self.0.direction()
    }

    pub(super) const fn previous_accepted(&self) -> ByteOffset {
        self.0.previous_accepted()
    }

    pub(super) const fn next_accepted(&self) -> ByteOffset {
        self.0.next_accepted()
    }
}

/// Reducer-minted proof that one exact client replay direction became
/// outstanding. `session_became_nonempty` distinguishes the sole aggregate
/// local-stall episode start from an additional flow becoming live while the
/// same bounded episode is already active.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct ClientReplayBecameOutstanding(ReducerReplayBecameOutstanding);

impl ClientReplayBecameOutstanding {
    pub(super) const fn session_id(&self) -> SessionId {
        self.0.session_id()
    }

    pub(super) const fn flow_id(&self) -> SessionFlowId {
        self.0.flow_id()
    }

    pub(super) const fn generation(&self) -> LegGeneration {
        self.0.generation()
    }

    pub(super) const fn direction(&self) -> Direction {
        self.0.direction()
    }

    pub(super) const fn start(&self) -> ByteOffset {
        self.0.start()
    }

    pub(super) const fn end(&self) -> ByteOffset {
        self.0.end()
    }

    pub(super) const fn session_became_nonempty(&self) -> bool {
        self.0.session_became_nonempty()
    }
}

/// Exact reducer terminal for one client-owned flow direction. This remains a
/// move-only scalar observation; the matching terminal-grace capability stays
/// in the visible effect batch and cannot be reconstructed from this fact.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct ClientFlowDirectionTerminal(ReducerFlowDirectionTerminal);

impl ClientFlowDirectionTerminal {
    pub(super) const fn session_id(&self) -> SessionId {
        self.0.session_id()
    }

    pub(super) const fn flow_id(&self) -> SessionFlowId {
        self.0.flow_id()
    }

    pub(super) const fn generation(&self) -> LegGeneration {
        self.0.generation()
    }

    pub(super) const fn direction(&self) -> Direction {
        self.0.direction()
    }
}

/// Exact reducer proof that the client session reached its expired terminal
/// state. It carries no flow, replay, transport, or timer ownership.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct ClientSessionExpired(ReducerSessionExpired);

impl ClientSessionExpired {
    pub(super) const fn session_id(&self) -> SessionId {
        self.0.session_id()
    }

    pub(super) const fn generation(&self) -> LegGeneration {
        self.0.generation()
    }
}

/// Fixed O(1) facts from one committed client reducer turn. This tracer slice
/// contains only the exact local sink frontier and stores no DATA, sink offer,
/// ACK history, or transport-delivery evidence.
#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct ClientControllerReducerObservations {
    replay_became_outstanding: Option<ClientReplayBecameOutstanding>,
    sink_accepted_frontier: Option<ClientSinkAcceptedFrontier>,
    flow_direction_terminal: Option<ClientFlowDirectionTerminal>,
    session_expired: Option<ClientSessionExpired>,
    session_has_outstanding_replay: bool,
}

impl ClientControllerReducerObservations {
    fn from_supervisor(mut observations: ReducerClientObservations) -> Self {
        let session_has_outstanding_replay = observations.session_has_outstanding_replay();
        let replay_became_outstanding = observations
            .into_replay_became_outstanding()
            .map(ClientReplayBecameOutstanding);
        let sink_accepted_frontier = observations
            .into_sink_accepted_frontier()
            .map(ClientSinkAcceptedFrontier);
        let flow_direction_terminal = observations
            .into_flow_terminal()
            .map(ClientFlowDirectionTerminal);
        let session_expired = observations
            .into_session_expired()
            .map(ClientSessionExpired);
        Self {
            replay_became_outstanding,
            sink_accepted_frontier,
            flow_direction_terminal,
            session_expired,
            session_has_outstanding_replay,
        }
    }

    pub(super) const fn replay_became_outstanding(&self) -> Option<&ClientReplayBecameOutstanding> {
        self.replay_became_outstanding.as_ref()
    }

    pub(super) const fn sink_accepted_frontier(&self) -> Option<&ClientSinkAcceptedFrontier> {
        self.sink_accepted_frontier.as_ref()
    }

    pub(super) const fn flow_direction_terminal(&self) -> Option<&ClientFlowDirectionTerminal> {
        self.flow_direction_terminal.as_ref()
    }

    pub(super) const fn session_expired(&self) -> Option<&ClientSessionExpired> {
        self.session_expired.as_ref()
    }

    pub(super) const fn session_has_outstanding_replay(&self) -> bool {
        self.session_has_outstanding_replay
    }
}

/// One reducer-committed client driver turn. Effects and the fixed observation
/// batch move together so an outer dispatcher can retain both under pressure.
pub(super) struct ClientControllerObservedApply {
    effects: Vec<SessionEffect>,
    observations: ClientControllerReducerObservations,
}

impl ClientControllerObservedApply {
    pub(super) fn into_parts(self) -> (Vec<SessionEffect>, ClientControllerReducerObservations) {
        (self.effects, self.observations)
    }
}

/// Observation-preserving counterpart to `ClientControllerDriverApply`.
pub(super) enum ClientControllerObservedDriverApply {
    Applied(ClientControllerObservedApply),
    Deferred {
        block: ReducerAdmissionBlock,
        input: ClientControllerDriverInput,
    },
    Failed(SessionSupervisorError),
}

struct RetiredClientTransportSlot {
    authority: RetiredClientAuthority,
    transport: RetiredClientTransport,
    supersession: Option<ClientReplacementSupersessionAudit>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ClientSupersededProbePhase {
    Idle,
    Queued,
    Armed,
    AckWonQueued,
    AckWonArmed,
    DeadlineWon,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ClientCurrentLivenessSupersessionAudit {
    phase: ClientSupersededProbePhase,
    generation: LegGeneration,
    switch_epoch: u64,
    probe_sequence: Option<ProbeSequence>,
    ordered_receipt: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ClientReplacementSupersessionAudit {
    retired_generation: LegGeneration,
    replacement_generation: LegGeneration,
    queue: ClientOutboundQueueSupersessionAudit,
    liveness: ClientCurrentLivenessSupersessionAudit,
    recovery_frames: usize,
    recovery_bytes: usize,
    recovery_replay_bytes: usize,
    rebound_reset_deliveries: usize,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct ClientSupersessionHistory {
    replacements: usize,
    superseded_frames: usize,
    superseded_bytes: usize,
    superseded_ordered_waits: usize,
    superseded_delivery_states: usize,
    superseded_tracked_resets: usize,
    superseded_probes: usize,
    recovery_frames: usize,
    recovery_bytes: usize,
    recovery_replay_bytes: usize,
    rebound_reset_deliveries: usize,
    last: Option<ClientReplacementSupersessionAudit>,
}

impl ClientSupersessionHistory {
    fn checked_record(self, audit: ClientReplacementSupersessionAudit) -> Option<Self> {
        Some(Self {
            replacements: self.replacements.checked_add(1)?,
            superseded_frames: self
                .superseded_frames
                .checked_add(audit.queue.queued_frames())?,
            superseded_bytes: self
                .superseded_bytes
                .checked_add(audit.queue.owned_bytes())?,
            superseded_ordered_waits: self
                .superseded_ordered_waits
                .checked_add(usize::from(audit.queue.ordered_delivery_wait()))?,
            superseded_delivery_states: self
                .superseded_delivery_states
                .checked_add(audit.queue.queued_delivery_states())?,
            superseded_tracked_resets: self
                .superseded_tracked_resets
                .checked_add(audit.queue.tracked_reset_deliveries())?,
            superseded_probes: self.superseded_probes.checked_add(usize::from(!matches!(
                audit.liveness.phase,
                ClientSupersededProbePhase::Idle
            )))?,
            recovery_frames: self.recovery_frames.checked_add(audit.recovery_frames)?,
            recovery_bytes: self.recovery_bytes.checked_add(audit.recovery_bytes)?,
            recovery_replay_bytes: self
                .recovery_replay_bytes
                .checked_add(audit.recovery_replay_bytes)?,
            rebound_reset_deliveries: self
                .rebound_reset_deliveries
                .checked_add(audit.rebound_reset_deliveries)?,
            last: Some(audit),
        })
    }
}

struct PreparedNormalClientSupersession {
    authority: ClientActiveLeg,
    transport: PreparedRetiredClientTransport,
    liveness: ClientCurrentLiveness,
    liveness_audit: ClientCurrentLivenessSupersessionAudit,
}

impl PreparedNormalClientSupersession {
    fn prepare_recovery_reset_rebinds(
        &self,
        recovery: &mut super::leg_io::PreparedClientRecovery,
    ) -> Result<(), super::leg_io::ClientRecoveryPlanError> {
        self.transport
            .rebind_queued_reset_deliveries(recovery)
            .map_err(super::leg_io::ClientRecoveryPlanError::from)
    }

    fn restore(
        self,
        current: &mut Option<ActiveClientTransport>,
        current_liveness: &mut ClientCurrentLiveness,
    ) {
        *current = Some(ActiveClientTransport {
            authority: self.authority,
            transport: self.transport.restore(),
        });
        *current_liveness = self.liveness;
    }

    fn into_retired_slot(
        self,
        recovery: ClientRecoverySupersessionManifest,
    ) -> RetiredClientTransportSlot {
        let retired_generation = self.authority.generation();
        let (transport, queue) = self.transport.commit();
        let supersession = ClientReplacementSupersessionAudit {
            retired_generation,
            replacement_generation: recovery.generation(),
            queue,
            liveness: self.liveness_audit,
            recovery_frames: recovery.frame_count(),
            recovery_bytes: recovery.encoded_bytes(),
            recovery_replay_bytes: recovery.client_local_replay_bytes(),
            rebound_reset_deliveries: recovery.rebound_reset_deliveries(),
        };
        drop(self.liveness);
        RetiredClientTransportSlot {
            authority: RetiredClientAuthority::AwaitingTerminal(self.authority.retire()),
            transport,
            supersession: Some(supersession),
        }
    }
}

enum RetiredClientAuthority {
    AwaitingTerminal(RetiredClientLeg),
    TerminalBound(TerminalBoundRetiredClientLeg),
}

impl RetiredClientAuthority {
    fn generation(&self) -> LegGeneration {
        match self {
            Self::AwaitingTerminal(authority) => authority.generation(),
            Self::TerminalBound(authority) => authority.generation(),
        }
    }

    fn discard_late_inbound(
        &self,
        inbound: BoundInbound,
    ) -> Result<RetiredClientInboundKind, BoundInbound> {
        match self {
            Self::AwaitingTerminal(authority) => authority.discard_late_inbound(inbound),
            Self::TerminalBound(authority) => authority.discard_late_inbound(inbound),
        }
    }

    fn clear_joined_terminal(
        &self,
        terminal: Option<ExactLegTerminal>,
    ) -> Result<(), Option<ExactLegTerminal>> {
        match (self, terminal) {
            (Self::AwaitingTerminal(authority), Some(terminal)) => {
                authority.discard_terminal(terminal).map_err(Some)
            }
            (Self::TerminalBound(_), None) => Ok(()),
            (Self::AwaitingTerminal(_), None) => Err(None),
            (Self::TerminalBound(_), Some(terminal)) => Err(Some(terminal)),
        }
    }

    const fn is_terminal_bound(&self) -> bool {
        matches!(self, Self::TerminalBound(_))
    }
}

struct ClientRecoveryState {
    generation: LegGeneration,
    effects: std::vec::IntoIter<PreparedClientRecoveryEffect>,
    pending: Option<PreparedClientRecoveryEffect>,
    reservation: InstalledClientRecoveryReservation,
    drive_ready: bool,
}

struct PendingReplacementApplicationAck {
    session_id: SessionId,
    generation: LegGeneration,
    replay: PreparedClientReplayRanges,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ClientControllerShutdownProbePhase {
    Idle,
    Queued,
    Armed,
    AckWonQueued,
    AckWonArmed,
    DeadlineWon,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ClientControllerObservationSettlementSource {
    CurrentTerminal,
    JoinedShutdown,
}

/// Copy-only fact installed only after one terminal/shutdown transaction has
/// consumed the exact liveness, scheduler-deadline, and application-ACK
/// observation owners. Replay DATA remains reducer-owned and is independently
/// counted by the root session ledger; this witness never pretends that
/// abandoning an observation acknowledged those bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ClientControllerObservationSettlement {
    source: ClientControllerObservationSettlementSource,
    probe_phase: ClientControllerShutdownProbePhase,
    probe_generation: LegGeneration,
    probe_switch_epoch: u64,
    probe_sequence: Option<ProbeSequence>,
    probe_receipt: bool,
    probe_receipt_delivered: bool,
    application_ack_generation: Option<LegGeneration>,
    application_ack_replay_ranges: usize,
    application_ack_replay_bytes: usize,
}

impl ClientControllerObservationSettlement {
    pub(crate) const fn source(self) -> ClientControllerObservationSettlementSource {
        self.source
    }

    pub(crate) const fn probe_phase(self) -> ClientControllerShutdownProbePhase {
        self.probe_phase
    }

    pub(crate) const fn probe_generation(self) -> LegGeneration {
        self.probe_generation
    }

    pub(crate) const fn probe_switch_epoch(self) -> u64 {
        self.probe_switch_epoch
    }

    pub(crate) const fn probe_sequence(self) -> Option<ProbeSequence> {
        self.probe_sequence
    }

    pub(crate) const fn probe_receipt(self) -> bool {
        self.probe_receipt
    }

    pub(crate) const fn probe_receipt_delivered(self) -> bool {
        self.probe_receipt_delivered
    }

    pub(crate) const fn application_ack(self) -> bool {
        self.application_ack_generation.is_some()
    }

    pub(crate) const fn application_ack_generation(self) -> Option<LegGeneration> {
        self.application_ack_generation
    }

    pub(crate) const fn application_ack_replay_ranges(self) -> usize {
        self.application_ack_replay_ranges
    }

    pub(crate) const fn application_ack_replay_bytes(self) -> usize {
        self.application_ack_replay_bytes
    }
}

pub(crate) type ClientControllerShutdownSettlement = ClientControllerObservationSettlement;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct ClientControllerObservationSettlementHistory {
    observations: usize,
    current_terminal_observations: usize,
    joined_shutdown_observations: usize,
    probe_authorities: usize,
    probe_receipts: usize,
    probe_scheduler_authorities: usize,
    probe_deadlines: usize,
    application_ack_observations: usize,
    application_ack_replay_ranges: usize,
    application_ack_replay_bytes: usize,
    last: Option<ClientControllerObservationSettlement>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ClientCurrentProbeAckSettlementPhase {
    Queued,
    Armed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ClientCurrentProbeAckSettlement {
    phase: ClientCurrentProbeAckSettlementPhase,
    generation: LegGeneration,
    probe_sequence: ProbeSequence,
}

impl ClientCurrentProbeAckSettlement {
    pub(crate) const fn phase(self) -> ClientCurrentProbeAckSettlementPhase {
        self.phase
    }

    pub(crate) const fn generation(self) -> LegGeneration {
        self.generation
    }

    pub(crate) const fn probe_sequence(self) -> ProbeSequence {
        self.probe_sequence
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct ClientCurrentProbeAckSettlementHistory {
    settlements: usize,
    queued: usize,
    armed: usize,
    last: Option<ClientCurrentProbeAckSettlement>,
}

impl ClientCurrentProbeAckSettlementHistory {
    fn checked_record(self, settlement: ClientCurrentProbeAckSettlement) -> Option<Self> {
        let queued = usize::from(matches!(
            settlement.phase,
            ClientCurrentProbeAckSettlementPhase::Queued
        ));
        let armed = usize::from(matches!(
            settlement.phase,
            ClientCurrentProbeAckSettlementPhase::Armed
        ));
        Some(Self {
            settlements: self.settlements.checked_add(1)?,
            queued: self.queued.checked_add(queued)?,
            armed: self.armed.checked_add(armed)?,
            last: Some(settlement),
        })
    }
}

impl ClientControllerObservationSettlementHistory {
    fn checked_record(self, settlement: ClientControllerObservationSettlement) -> Option<Self> {
        let current_terminal = usize::from(matches!(
            settlement.source,
            ClientControllerObservationSettlementSource::CurrentTerminal
        ));
        let joined_shutdown = usize::from(matches!(
            settlement.source,
            ClientControllerObservationSettlementSource::JoinedShutdown
        ));
        let probe_authority = usize::from(!matches!(
            settlement.probe_phase,
            ClientControllerShutdownProbePhase::Idle
        ));
        let probe_deadline = usize::from(matches!(
            settlement.probe_phase,
            ClientControllerShutdownProbePhase::Armed
                | ClientControllerShutdownProbePhase::AckWonArmed
        ));
        let probe_scheduler_authority = usize::from(matches!(
            settlement.probe_phase,
            ClientControllerShutdownProbePhase::Queued
                | ClientControllerShutdownProbePhase::Armed
                | ClientControllerShutdownProbePhase::AckWonQueued
                | ClientControllerShutdownProbePhase::AckWonArmed
        ));
        Some(Self {
            observations: self.observations.checked_add(1)?,
            current_terminal_observations: self
                .current_terminal_observations
                .checked_add(current_terminal)?,
            joined_shutdown_observations: self
                .joined_shutdown_observations
                .checked_add(joined_shutdown)?,
            probe_authorities: self.probe_authorities.checked_add(probe_authority)?,
            probe_receipts: self
                .probe_receipts
                .checked_add(usize::from(settlement.probe_receipt))?,
            probe_scheduler_authorities: self
                .probe_scheduler_authorities
                .checked_add(probe_scheduler_authority)?,
            probe_deadlines: self.probe_deadlines.checked_add(probe_deadline)?,
            application_ack_observations: self
                .application_ack_observations
                .checked_add(usize::from(settlement.application_ack()))?,
            application_ack_replay_ranges: self
                .application_ack_replay_ranges
                .checked_add(settlement.application_ack_replay_ranges)?,
            application_ack_replay_bytes: self
                .application_ack_replay_bytes
                .checked_add(settlement.application_ack_replay_bytes)?,
            last: Some(settlement),
        })
    }
}

struct PendingClientControllerObservationSettlement {
    settlement: ClientControllerObservationSettlement,
    next_history: ClientControllerObservationSettlementHistory,
    probe: ClientCurrentProbePhase,
    application_ack: Option<PendingReplacementApplicationAck>,
}

struct PreparedClientControllerObservationSettlement {
    settlement: ClientControllerObservationSettlement,
    next_history: ClientControllerObservationSettlementHistory,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ClientControllerObservationSettlementPrepareErrorKind {
    PendingSettlement,
    LivenessMismatch,
    ApplicationAckMismatch,
    HistoryOverflow,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ClientControllerObservationSettlementErrorKind {
    MissingPendingSettlement,
    MissingProbeSchedulerAuthority,
    UnexpectedProbeSchedulerAuthority,
    WrongProbeSchedulerAuthority,
}

pub(super) struct ClientControllerObservationSettlementFailure {
    scheduler: Option<ClientCurrentProbeSchedulerAuthority>,
    kind: ClientControllerObservationSettlementErrorKind,
}

impl ClientControllerObservationSettlementFailure {
    pub(super) const fn kind(&self) -> ClientControllerObservationSettlementErrorKind {
        self.kind
    }

    pub(super) fn into_scheduler(self) -> Option<ClientCurrentProbeSchedulerAuthority> {
        self.scheduler
    }
}

pub(super) struct ClientControllerRequiredObservationSettlementFailure {
    scheduler: ClientCurrentProbeSchedulerAuthority,
    kind: ClientControllerObservationSettlementErrorKind,
}

impl ClientControllerRequiredObservationSettlementFailure {
    pub(super) const fn kind(&self) -> ClientControllerObservationSettlementErrorKind {
        self.kind
    }

    pub(super) fn into_scheduler(self) -> ClientCurrentProbeSchedulerAuthority {
        self.scheduler
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ClientControllerOwnershipLedger {
    live_current_transport_owners: usize,
    live_retired_transport_owners: usize,
    live_active_leg_authorities: usize,
    live_terminal_bound_authorities: usize,
    live_pending_loss_authorities: usize,
    live_recovery_transactions: usize,
    live_probe_authorities: usize,
    live_probe_receipts: usize,
    live_probe_scheduler_authorities: usize,
    live_scheduler_probe_deadlines: usize,
    live_application_ack_observations: usize,
    live_application_ack_replay_ranges: usize,
    live_application_ack_replay_bytes: usize,
    live_pending_settlements: usize,
    settlement_history: ClientControllerObservationSettlementHistory,
    probe_ack_settlement_history: ClientCurrentProbeAckSettlementHistory,
    overflowed: bool,
}

impl ClientControllerOwnershipLedger {
    pub(crate) const fn overflowed(self) -> bool {
        self.overflowed
    }

    pub(crate) const fn live_probe_authorities(self) -> usize {
        self.live_probe_authorities
    }

    pub(crate) const fn live_current_transport_owners(self) -> usize {
        self.live_current_transport_owners
    }

    pub(crate) const fn live_retired_transport_owners(self) -> usize {
        self.live_retired_transport_owners
    }

    pub(crate) const fn live_active_leg_authorities(self) -> usize {
        self.live_active_leg_authorities
    }

    pub(crate) const fn live_terminal_bound_authorities(self) -> usize {
        self.live_terminal_bound_authorities
    }

    pub(crate) const fn live_pending_loss_authorities(self) -> usize {
        self.live_pending_loss_authorities
    }

    pub(crate) const fn live_recovery_transactions(self) -> usize {
        self.live_recovery_transactions
    }

    pub(crate) const fn live_probe_receipts(self) -> usize {
        self.live_probe_receipts
    }

    pub(crate) const fn live_scheduler_probe_deadlines(self) -> usize {
        self.live_scheduler_probe_deadlines
    }

    pub(crate) const fn live_probe_scheduler_authorities(self) -> usize {
        self.live_probe_scheduler_authorities
    }

    pub(crate) const fn live_application_ack_observations(self) -> usize {
        self.live_application_ack_observations
    }

    pub(crate) const fn live_application_ack_replay_ranges(self) -> usize {
        self.live_application_ack_replay_ranges
    }

    pub(crate) const fn live_application_ack_replay_bytes(self) -> usize {
        self.live_application_ack_replay_bytes
    }

    pub(crate) const fn live_pending_settlements(self) -> usize {
        self.live_pending_settlements
    }

    pub(crate) const fn settled_observations(self) -> usize {
        self.settlement_history.observations
    }

    pub(crate) const fn settled_current_terminals(self) -> usize {
        self.settlement_history.current_terminal_observations
    }

    pub(crate) const fn settled_joined_shutdowns(self) -> usize {
        self.settlement_history.joined_shutdown_observations
    }

    pub(crate) const fn settled_probe_authorities(self) -> usize {
        self.settlement_history.probe_authorities
    }

    pub(crate) const fn settled_probe_receipts(self) -> usize {
        self.settlement_history.probe_receipts
    }

    pub(crate) const fn settled_probe_scheduler_authorities(self) -> usize {
        self.settlement_history.probe_scheduler_authorities
    }

    pub(crate) const fn settled_probe_deadlines(self) -> usize {
        self.settlement_history.probe_deadlines
    }

    pub(crate) const fn settled_application_ack_observations(self) -> usize {
        self.settlement_history.application_ack_observations
    }

    pub(crate) const fn settled_application_ack_replay_ranges(self) -> usize {
        self.settlement_history.application_ack_replay_ranges
    }

    pub(crate) const fn settled_application_ack_replay_bytes(self) -> usize {
        self.settlement_history.application_ack_replay_bytes
    }

    pub(crate) const fn last_settlement(self) -> Option<ClientControllerObservationSettlement> {
        self.settlement_history.last
    }

    pub(crate) const fn settled_probe_acks(self) -> usize {
        self.probe_ack_settlement_history.settlements
    }

    pub(crate) const fn settled_queued_probe_acks(self) -> usize {
        self.probe_ack_settlement_history.queued
    }

    pub(crate) const fn settled_armed_probe_acks(self) -> usize {
        self.probe_ack_settlement_history.armed
    }

    pub(crate) const fn last_probe_ack_settlement(self) -> Option<ClientCurrentProbeAckSettlement> {
        self.probe_ack_settlement_history.last
    }

    pub(crate) const fn live_is_zero(self) -> bool {
        !self.overflowed
            && self.live_current_transport_owners == 0
            && self.live_retired_transport_owners == 0
            && self.live_active_leg_authorities == 0
            && self.live_terminal_bound_authorities == 0
            && self.live_pending_loss_authorities == 0
            && self.live_recovery_transactions == 0
            && self.live_probe_authorities == 0
            && self.live_probe_receipts == 0
            && self.live_probe_scheduler_authorities == 0
            && self.live_scheduler_probe_deadlines == 0
            && self.live_application_ack_observations == 0
            && self.live_application_ack_replay_ranges == 0
            && self.live_application_ack_replay_bytes == 0
            && self.live_pending_settlements == 0
    }

    const fn overflow(
        settlement_history: ClientControllerObservationSettlementHistory,
        probe_ack_settlement_history: ClientCurrentProbeAckSettlementHistory,
    ) -> Self {
        Self {
            live_current_transport_owners: usize::MAX,
            live_retired_transport_owners: usize::MAX,
            live_active_leg_authorities: usize::MAX,
            live_terminal_bound_authorities: usize::MAX,
            live_pending_loss_authorities: usize::MAX,
            live_recovery_transactions: usize::MAX,
            live_probe_authorities: usize::MAX,
            live_probe_receipts: usize::MAX,
            live_probe_scheduler_authorities: usize::MAX,
            live_scheduler_probe_deadlines: usize::MAX,
            live_application_ack_observations: usize::MAX,
            live_application_ack_replay_ranges: usize::MAX,
            live_application_ack_replay_bytes: usize::MAX,
            live_pending_settlements: usize::MAX,
            settlement_history,
            probe_ack_settlement_history,
            overflowed: true,
        }
    }
}

/// Independent B lifecycle slot. It never consumes the sole retired-A slot:
/// the exact current terminal first retains all recovery ownership here, and
/// explicit discard advances only this slot to its inert transport form.
#[allow(
    clippy::large_enum_variant,
    reason = "exact non-cloneable terminal ownership stays inline instead of adding boxing to the terminal path"
)]
enum TerminatingCurrentClientTransport {
    RecoveryPending {
        authority: TerminalBoundRetiredClientLeg,
        transport: ClientLegTransport,
        pending_loss: PendingClientLegLoss,
        recovery: TerminatingCurrentRecoveryOwnership,
    },
    RecoveryDiscarded {
        authority: TerminalBoundRetiredClientLeg,
        transport: RetiredClientTransport,
        generation: LegGeneration,
        reason: LegTransportTerminalReason,
    },
}

/// Non-cloneable controller-side half of the catch-up transaction. It moves
/// out only after every controller gate has passed, is returned by every
/// standby/reducer rejection, and is consumed by the installed publication.
struct PostLossClientRetirementSource {
    authority: TerminalBoundRetiredClientLeg,
    transport: RetiredClientTransport,
    generation: LegGeneration,
    reason: LegTransportTerminalReason,
}

impl PostLossClientRetirementSource {
    fn into_phase(self) -> ClientControllerPhase {
        ClientControllerPhase::TerminatingCurrent(
            TerminatingCurrentClientTransport::RecoveryDiscarded {
                authority: self.authority,
                transport: self.transport,
                generation: self.generation,
                reason: self.reason,
            },
        )
    }

    fn into_retired_slot(self) -> RetiredClientTransportSlot {
        RetiredClientTransportSlot {
            authority: RetiredClientAuthority::TerminalBound(self.authority),
            transport: self.transport,
            supersession: None,
        }
    }
}

#[allow(
    clippy::large_enum_variant,
    reason = "exact non-cloneable recovery ownership stays inline instead of adding boxing to the terminal path"
)]
enum TerminatingCurrentRecoveryOwnership {
    Installed(ClientRecoveryState),
    Tail(ClientRecoveryTail),
    NoRecovery,
}

impl TerminatingCurrentRecoveryOwnership {
    fn into_controller_phase(self) -> ClientControllerPhase {
        match self {
            Self::Installed(recovery) => ClientControllerPhase::Recovering(recovery),
            Self::Tail(tail) => ClientControllerPhase::Active {
                recovery_tail: Some(tail),
            },
            Self::NoRecovery => ClientControllerPhase::Active {
                recovery_tail: None,
            },
        }
    }
}

#[allow(
    clippy::large_enum_variant,
    reason = "exact non-cloneable phase ownership stays inline instead of adding boxing to the terminal path"
)]
enum ClientControllerPhase {
    Active {
        recovery_tail: Option<ClientRecoveryTail>,
    },
    Recovering(ClientRecoveryState),
    TerminatingCurrent(TerminatingCurrentClientTransport),
    /// Fail-closed terminal/transition state. Queue loss alone never enters it.
    Legless,
}

/// Production-shared sole mutator for one client reducer and its exact active
/// endpoint/FIFO. Once installed, no sibling owns a raw `LegIo` or queue.
pub(crate) struct ClientSessionController {
    supervisor: SessionSupervisor,
    current: Option<ActiveClientTransport>,
    retired: Option<RetiredClientTransportSlot>,
    phase: ClientControllerPhase,
    installed_features: FeatureSet,
    recovery_effect_bound: usize,
    pending_replacement_application_ack: Option<PendingReplacementApplicationAck>,
    current_liveness: ClientCurrentLiveness,
    shutdown_current_transport_residual: Option<ClientLegTransportResidual>,
    pending_observation_settlement: Option<PendingClientControllerObservationSettlement>,
    observation_settlement_history: ClientControllerObservationSettlementHistory,
    probe_ack_settlement_history: ClientCurrentProbeAckSettlementHistory,
    supersession_history: ClientSupersessionHistory,
}

struct ClientCurrentLiveness {
    phase: ClientCurrentProbePhase,
    next_sequence: Option<ProbeSequence>,
    switch_epoch: u64,
}

enum ClientCurrentProbePhase {
    Idle,
    Queued {
        identity: Arc<ClientCurrentProbeIdentity>,
        receipt: LegControlEnqueued,
    },
    Armed {
        identity: Arc<ClientCurrentProbeIdentity>,
        deadline: SimTime,
    },
    AckWonQueued {
        identity: Arc<ClientCurrentProbeIdentity>,
    },
    AckWonArmed {
        identity: Arc<ClientCurrentProbeIdentity>,
        deadline: SimTime,
    },
    DeadlineWon {
        identity: Arc<ClientCurrentProbeIdentity>,
    },
}

struct ClientCurrentProbeIdentity {
    correlation: ClientCurrentProbeCorrelation,
}

struct ClientCurrentProbeCorrelation {
    seal: LegSeal,
    session_id: SessionId,
    generation: LegGeneration,
    epoch_nonce: LegEpochNonce,
    sequence: ProbeSequence,
    switch_epoch: u64,
}

impl ClientCurrentProbeCorrelation {
    fn same_wire_ack(
        &self,
        seal: &LegSeal,
        frame: &LegControlFrame,
        current_switch_epoch: u64,
    ) -> bool {
        if self.switch_epoch != current_switch_epoch
            || !self.seal.same_connection(seal)
            || frame.leg_generation() != self.generation
        {
            return false;
        }
        matches!(
            frame.record(),
            LegControlRecord::LegProbeAck {
                session_id,
                leg_epoch_nonce,
                probe_sequence,
            } if *session_id == self.session_id
                && *leg_epoch_nonce == self.epoch_nonce
                && *probe_sequence == self.sequence
        )
    }
}

impl ClientCurrentLiveness {
    fn for_generation(generation: LegGeneration) -> Self {
        Self {
            phase: ClientCurrentProbePhase::Idle,
            next_sequence: ProbeSequence::new(1).ok(),
            switch_epoch: generation.get(),
        }
    }

    fn reset_for_replacement(&mut self, generation: LegGeneration) {
        self.switch_epoch = generation.get();
        self.phase = ClientCurrentProbePhase::Idle;
    }

    fn supersession_audit(
        &self,
        authority: &ClientActiveLeg,
    ) -> Option<ClientCurrentLivenessSupersessionAudit> {
        let generation = authority.generation();
        if self.switch_epoch != generation.get() {
            return None;
        }
        let authority_seal = authority.share_probe_seal();
        let (phase, correlation, ordered_receipt) = match &self.phase {
            ClientCurrentProbePhase::Idle => (ClientSupersededProbePhase::Idle, None, false),
            ClientCurrentProbePhase::Queued { identity, .. } => (
                ClientSupersededProbePhase::Queued,
                Some(&identity.correlation),
                true,
            ),
            ClientCurrentProbePhase::Armed { identity, .. } => (
                ClientSupersededProbePhase::Armed,
                Some(&identity.correlation),
                false,
            ),
            ClientCurrentProbePhase::AckWonQueued { identity } => (
                ClientSupersededProbePhase::AckWonQueued,
                Some(&identity.correlation),
                false,
            ),
            ClientCurrentProbePhase::AckWonArmed { identity, .. } => (
                ClientSupersededProbePhase::AckWonArmed,
                Some(&identity.correlation),
                false,
            ),
            ClientCurrentProbePhase::DeadlineWon { identity } => (
                ClientSupersededProbePhase::DeadlineWon,
                Some(&identity.correlation),
                false,
            ),
        };
        let probe_sequence = match correlation {
            Some(correlation)
                if correlation.generation == generation
                    && correlation.switch_epoch == self.switch_epoch
                    && authority_seal.same_connection(&correlation.seal) =>
            {
                Some(correlation.sequence)
            }
            Some(_) => return None,
            None => None,
        };
        Some(ClientCurrentLivenessSupersessionAudit {
            phase,
            generation,
            switch_epoch: self.switch_epoch,
            probe_sequence,
            ordered_receipt,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ClientCurrentInboundAdmission {
    Ready,
    PolicyBlocked,
    MissingCurrent,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ClientControllerBootstrapErrorKind {
    RecoveryEffectBoundOverflow,
    WrongTransport,
    WrongQueue,
    EndpointLost,
    RequestNotDelivered,
    NotAwaitingInitialAttach,
}

impl ClientSessionController {
    fn prepare_current_observation_settlement(
        &self,
        authority: &ClientActiveLeg,
        source: ClientControllerObservationSettlementSource,
    ) -> Result<
        PreparedClientControllerObservationSettlement,
        ClientControllerObservationSettlementPrepareErrorKind,
    > {
        if self.pending_observation_settlement.is_some() {
            return Err(ClientControllerObservationSettlementPrepareErrorKind::PendingSettlement);
        }
        let liveness = self
            .current_liveness
            .supersession_audit(authority)
            .ok_or(ClientControllerObservationSettlementPrepareErrorKind::LivenessMismatch)?;
        let (probe_phase, probe_receipt_delivered) = match &self.current_liveness.phase {
            ClientCurrentProbePhase::Idle => (ClientControllerShutdownProbePhase::Idle, false),
            ClientCurrentProbePhase::Queued { receipt, .. } => (
                ClientControllerShutdownProbePhase::Queued,
                receipt.was_delivered(),
            ),
            ClientCurrentProbePhase::Armed { .. } => {
                (ClientControllerShutdownProbePhase::Armed, false)
            }
            ClientCurrentProbePhase::AckWonQueued { .. } => {
                (ClientControllerShutdownProbePhase::AckWonQueued, false)
            }
            ClientCurrentProbePhase::AckWonArmed { .. } => {
                (ClientControllerShutdownProbePhase::AckWonArmed, false)
            }
            ClientCurrentProbePhase::DeadlineWon { .. } => {
                (ClientControllerShutdownProbePhase::DeadlineWon, false)
            }
        };
        let (
            application_ack_generation,
            application_ack_replay_ranges,
            application_ack_replay_bytes,
        ) = match self.pending_replacement_application_ack.as_ref() {
            Some(pending)
                if pending.session_id
                    == self.supervisor.session_id_for_client_controller(
                        &ClientControllerObservedApplyGate::mint(),
                    )
                    && pending.generation == authority.generation()
                    && pending.replay.range_count() > 0
                    && pending.replay.data_bytes() > 0 =>
            {
                (
                    Some(pending.generation),
                    pending.replay.range_count(),
                    pending.replay.data_bytes(),
                )
            }
            Some(_) => {
                return Err(
                    ClientControllerObservationSettlementPrepareErrorKind::ApplicationAckMismatch,
                );
            }
            None => (None, 0, 0),
        };
        let settlement = ClientControllerObservationSettlement {
            source,
            probe_phase,
            probe_generation: liveness.generation,
            probe_switch_epoch: liveness.switch_epoch,
            probe_sequence: liveness.probe_sequence,
            probe_receipt: liveness.ordered_receipt,
            probe_receipt_delivered,
            application_ack_generation,
            application_ack_replay_ranges,
            application_ack_replay_bytes,
        };
        let next_history = self
            .observation_settlement_history
            .checked_record(settlement)
            .ok_or(ClientControllerObservationSettlementPrepareErrorKind::HistoryOverflow)?;
        Ok(PreparedClientControllerObservationSettlement {
            settlement,
            next_history,
        })
    }

    fn begin_current_observation_settlement(
        &mut self,
        prepared: PreparedClientControllerObservationSettlement,
    ) {
        let probe = std::mem::replace(
            &mut self.current_liveness.phase,
            ClientCurrentProbePhase::Idle,
        );
        let application_ack = self.pending_replacement_application_ack.take();
        let pending = PendingClientControllerObservationSettlement {
            settlement: prepared.settlement,
            next_history: prepared.next_history,
            probe,
            application_ack,
        };
        if matches!(
            pending.probe,
            ClientCurrentProbePhase::Queued { .. }
                | ClientCurrentProbePhase::Armed { .. }
                | ClientCurrentProbePhase::AckWonQueued { .. }
                | ClientCurrentProbePhase::AckWonArmed { .. }
        ) {
            self.pending_observation_settlement = Some(pending);
        } else {
            drop((pending.probe, pending.application_ack));
            self.observation_settlement_history = pending.next_history;
        }
    }

    pub(super) fn current_or_pending_probe_scheduler_matches(
        &self,
        scheduler: &ClientCurrentProbeSchedulerAuthority,
    ) -> bool {
        let phase = self
            .pending_observation_settlement
            .as_ref()
            .map(|pending| &pending.probe)
            .unwrap_or(&self.current_liveness.phase);
        match (phase, scheduler) {
            (
                ClientCurrentProbePhase::Queued { identity, .. },
                ClientCurrentProbeSchedulerAuthority::Queued(token),
            ) => Arc::ptr_eq(identity, &token.identity),
            (
                ClientCurrentProbePhase::Armed { identity, deadline },
                ClientCurrentProbeSchedulerAuthority::Armed(token),
            ) => Arc::ptr_eq(identity, &token.identity) && *deadline == token.deadline,
            (
                ClientCurrentProbePhase::AckWonQueued { identity },
                ClientCurrentProbeSchedulerAuthority::Queued(token),
            ) => Arc::ptr_eq(identity, &token.identity),
            (
                ClientCurrentProbePhase::AckWonArmed { identity, deadline },
                ClientCurrentProbeSchedulerAuthority::Armed(token),
            ) => Arc::ptr_eq(identity, &token.identity) && *deadline == token.deadline,
            _ => false,
        }
    }

    pub(super) const fn observation_settlement_pending(&self) -> bool {
        self.pending_observation_settlement.is_some()
    }

    /// Returns the value correlation for the one exact scheduler authority
    /// that must be returned before a terminal-bound observation can settle.
    /// The non-cloneable queued/deadline owner remains outside this controller;
    /// this key is selection evidence only and grants no settlement authority.
    pub(super) fn pending_current_observation_probe_key(
        &self,
    ) -> Option<(LegGeneration, ProbeSequence)> {
        let pending = self.pending_observation_settlement.as_ref()?;
        match pending.probe {
            ClientCurrentProbePhase::Queued { .. }
            | ClientCurrentProbePhase::Armed { .. }
            | ClientCurrentProbePhase::AckWonQueued { .. }
            | ClientCurrentProbePhase::AckWonArmed { .. } => pending
                .settlement
                .probe_sequence
                .map(|sequence| (pending.settlement.probe_generation, sequence)),
            ClientCurrentProbePhase::Idle | ClientCurrentProbePhase::DeadlineWon { .. } => None,
        }
    }

    pub(super) fn current_or_pending_probe_queued_matches(
        &self,
        token: &ClientCurrentProbeQueued,
    ) -> bool {
        let phase = self
            .pending_observation_settlement
            .as_ref()
            .map(|pending| &pending.probe)
            .unwrap_or(&self.current_liveness.phase);
        matches!(
            phase,
            ClientCurrentProbePhase::Queued { identity, .. }
                | ClientCurrentProbePhase::AckWonQueued { identity }
                if Arc::ptr_eq(identity, &token.identity)
        )
    }

    pub(super) fn current_or_pending_probe_deadline_matches(
        &self,
        token: &ClientCurrentProbeDeadline,
    ) -> bool {
        let phase = self
            .pending_observation_settlement
            .as_ref()
            .map(|pending| &pending.probe)
            .unwrap_or(&self.current_liveness.phase);
        matches!(
            phase,
            ClientCurrentProbePhase::Armed { identity, deadline }
                | ClientCurrentProbePhase::AckWonArmed { identity, deadline }
                if Arc::ptr_eq(identity, &token.identity) && *deadline == token.deadline
        )
    }

    pub(super) fn settle_pending_current_observation(
        &mut self,
        scheduler: Option<ClientCurrentProbeSchedulerAuthority>,
    ) -> Result<ClientControllerObservationSettlement, ClientControllerObservationSettlementFailure>
    {
        let Some(pending) = self.pending_observation_settlement.as_ref() else {
            return Err(ClientControllerObservationSettlementFailure {
                scheduler,
                kind: ClientControllerObservationSettlementErrorKind::MissingPendingSettlement,
            });
        };
        let validation = Self::validate_pending_current_observation_scheduler(
            &pending.probe,
            scheduler.as_ref(),
        );
        if let Err(kind) = validation {
            return Err(ClientControllerObservationSettlementFailure { scheduler, kind });
        }
        let Some(pending) = self.pending_observation_settlement.take() else {
            return Err(ClientControllerObservationSettlementFailure {
                scheduler,
                kind: ClientControllerObservationSettlementErrorKind::MissingPendingSettlement,
            });
        };
        let settlement = pending.settlement;
        drop((pending.probe, pending.application_ack, scheduler));
        self.observation_settlement_history = pending.next_history;
        Ok(settlement)
    }

    /// Required-authority live variant. Unlike the shutdown-facing optional
    /// API, every failure contains the exact queued/deadline owner supplied by
    /// the root, so a fixed scheduler slot can be restored without an
    /// impossible `None` branch or reconstructed capability.
    pub(super) fn settle_pending_current_observation_with_scheduler(
        &mut self,
        scheduler: ClientCurrentProbeSchedulerAuthority,
    ) -> Result<
        ClientControllerObservationSettlement,
        ClientControllerRequiredObservationSettlementFailure,
    > {
        let Some(pending) = self.pending_observation_settlement.as_ref() else {
            return Err(ClientControllerRequiredObservationSettlementFailure {
                scheduler,
                kind: ClientControllerObservationSettlementErrorKind::MissingPendingSettlement,
            });
        };
        if let Err(kind) =
            Self::validate_pending_current_observation_scheduler(&pending.probe, Some(&scheduler))
        {
            return Err(ClientControllerRequiredObservationSettlementFailure { scheduler, kind });
        }
        let Some(pending) = self.pending_observation_settlement.take() else {
            return Err(ClientControllerRequiredObservationSettlementFailure {
                scheduler,
                kind: ClientControllerObservationSettlementErrorKind::MissingPendingSettlement,
            });
        };
        let settlement = pending.settlement;
        drop((pending.probe, pending.application_ack, scheduler));
        self.observation_settlement_history = pending.next_history;
        Ok(settlement)
    }

    fn validate_pending_current_observation_scheduler(
        probe: &ClientCurrentProbePhase,
        scheduler: Option<&ClientCurrentProbeSchedulerAuthority>,
    ) -> Result<(), ClientControllerObservationSettlementErrorKind> {
        match (probe, scheduler) {
            (ClientCurrentProbePhase::Queued { .. }, None)
            | (ClientCurrentProbePhase::Armed { .. }, None)
            | (ClientCurrentProbePhase::AckWonQueued { .. }, None)
            | (ClientCurrentProbePhase::AckWonArmed { .. }, None) => {
                Err(ClientControllerObservationSettlementErrorKind::MissingProbeSchedulerAuthority)
            }
            (
                ClientCurrentProbePhase::Queued { identity, .. },
                Some(ClientCurrentProbeSchedulerAuthority::Queued(token)),
            ) if Arc::ptr_eq(identity, &token.identity) => Ok(()),
            (
                ClientCurrentProbePhase::Armed { identity, deadline },
                Some(ClientCurrentProbeSchedulerAuthority::Armed(token)),
            ) if Arc::ptr_eq(identity, &token.identity) && *deadline == token.deadline => Ok(()),
            (
                ClientCurrentProbePhase::AckWonQueued { identity },
                Some(ClientCurrentProbeSchedulerAuthority::Queued(token)),
            ) if Arc::ptr_eq(identity, &token.identity) => Ok(()),
            (
                ClientCurrentProbePhase::AckWonArmed { identity, deadline },
                Some(ClientCurrentProbeSchedulerAuthority::Armed(token)),
            ) if Arc::ptr_eq(identity, &token.identity) && *deadline == token.deadline => Ok(()),
            (
                ClientCurrentProbePhase::Queued { .. }
                | ClientCurrentProbePhase::Armed { .. }
                | ClientCurrentProbePhase::AckWonQueued { .. }
                | ClientCurrentProbePhase::AckWonArmed { .. },
                Some(_),
            ) => Err(ClientControllerObservationSettlementErrorKind::WrongProbeSchedulerAuthority),
            (
                ClientCurrentProbePhase::Idle | ClientCurrentProbePhase::DeadlineWon { .. },
                Some(_),
            ) => Err(
                ClientControllerObservationSettlementErrorKind::UnexpectedProbeSchedulerAuthority,
            ),
            (ClientCurrentProbePhase::Idle | ClientCurrentProbePhase::DeadlineWon { .. }, None) => {
                Ok(())
            }
        }
    }

    pub(super) fn current_physical_actor_matches(
        &self,
        current: &PhysicalLegRuntimeIncarnation,
        gate: &QuinnLegCompositionGate,
    ) -> bool {
        self.current
            .as_ref()
            .is_some_and(|slot| slot.transport.belongs_to_physical_actor(current, gate))
    }

    pub(super) fn terminating_current_physical_actor_matches(
        &self,
        current: &PhysicalLegRuntimeIncarnation,
        gate: &QuinnLegCompositionGate,
    ) -> bool {
        match &self.phase {
            ClientControllerPhase::TerminatingCurrent(
                TerminatingCurrentClientTransport::RecoveryPending { transport, .. },
            ) => transport.belongs_to_physical_actor(current, gate),
            ClientControllerPhase::TerminatingCurrent(
                TerminatingCurrentClientTransport::RecoveryDiscarded { transport, .. },
            ) => transport.belongs_to_physical_actor(current, gate),
            ClientControllerPhase::Active { .. }
            | ClientControllerPhase::Recovering(_)
            | ClientControllerPhase::Legless => false,
        }
    }

    pub(super) fn current_is_cleared_for_shutdown(&self) -> bool {
        self.external_flow_admission_is_closed()
            && self.current.is_none()
            && matches!(self.phase, ClientControllerPhase::Legless)
            && matches!(self.current_liveness.phase, ClientCurrentProbePhase::Idle)
            && self.pending_replacement_application_ack.is_none()
            && self.pending_observation_settlement.is_none()
    }

    pub(super) fn retired_physical_actor_matches(
        &self,
        retired: &PhysicalLegRuntimeIncarnation,
        gate: &QuinnLegCompositionGate,
    ) -> bool {
        self.retired
            .as_ref()
            .is_some_and(|slot| slot.transport.belongs_to_physical_actor(retired, gate))
    }

    pub(super) fn retired_physical_actor_layout_matches(
        &self,
        retired: Option<&PhysicalLegRuntimeIncarnation>,
        gate: &QuinnLegCompositionGate,
    ) -> bool {
        match (self.retired.as_ref(), retired) {
            (None, None) => true,
            (Some(_), Some(retired)) => self.retired_physical_actor_matches(retired, gate),
            (None, Some(_)) | (Some(_), None) => false,
        }
    }

    pub(super) fn retired_slot_is_present(&self) -> bool {
        self.retired.is_some()
    }

    pub(super) fn physical_actor_layout_matches(
        &self,
        current: &PhysicalLegRuntimeIncarnation,
        retired: Option<&PhysicalLegRuntimeIncarnation>,
        gate: &QuinnLegCompositionGate,
    ) -> bool {
        self.current_physical_actor_matches(current, gate)
            && self.retired_physical_actor_layout_matches(retired, gate)
    }

    /// Consumes the initial acceptance and both raw transport pieces before
    /// activating their exact queue. Every fallible preflight returns all
    /// inputs; successful activation is followed only by infallible closed
    /// controller construction.
    #[allow(
        clippy::result_large_err,
        reason = "pre-commit rejection returns config, acceptance, endpoint, and sole queue"
    )]
    pub(crate) fn bootstrap_initial(
        config: SessionConfig,
        accepted: AcceptedInitialAttach,
        io: LegIo,
        queue: LegOutboundQueue,
    ) -> Result<Self, ClientControllerBootstrapFailure> {
        let Some(recovery_effect_bound) = config.client_recovery_effect_bound() else {
            return Err(ClientControllerBootstrapFailure::raw(
                config,
                accepted,
                io,
                queue,
                ClientControllerBootstrapErrorKind::RecoveryEffectBoundOverflow,
            ));
        };
        let gate = ClientControllerBootstrapGate::mint();
        let transport = match ClientLegTransport::from_parts(
            io,
            queue,
            ClientControllerTransportGate::mint(),
        ) {
            Ok(transport) => transport,
            Err(transport) => {
                return Err(ClientControllerBootstrapFailure::transport(
                    config, accepted, transport,
                ));
            }
        };
        let mut transport = transport;
        let attached = match transport.activate_initial_client(accepted, &gate) {
            Ok(attached) => attached,
            Err(failure) => {
                return Err(ClientControllerBootstrapFailure::activation(
                    config, failure, transport,
                ));
            }
        };
        let supervisor = SessionSupervisor::from_initial_client(config, &attached, &gate);
        let installed_features = FeatureSet::new(attached.negotiated_features());
        let authority =
            ClientActiveLeg::from_initial(attached, ClientControllerInstallGate::mint());
        let current_liveness = ClientCurrentLiveness::for_generation(authority.generation());
        Ok(Self {
            supervisor,
            current: Some(ActiveClientTransport {
                authority,
                transport,
            }),
            retired: None,
            phase: ClientControllerPhase::Active {
                recovery_tail: None,
            },
            installed_features,
            recovery_effect_bound,
            pending_replacement_application_ack: None,
            current_liveness,
            shutdown_current_transport_residual: None,
            pending_observation_settlement: None,
            observation_settlement_history: ClientControllerObservationSettlementHistory::default(),
            probe_ack_settlement_history: ClientCurrentProbeAckSettlementHistory::default(),
            supersession_history: ClientSupersessionHistory::default(),
        })
    }

    pub(crate) fn snapshot(&self) -> SessionSupervisorSnapshot {
        self.supervisor.snapshot()
    }

    pub(super) fn session_config(&self) -> SessionConfig {
        self.supervisor.session_config()
    }

    /// Admission witness for the first production root slice. Until the root
    /// owns a same-domain clock and recovery scheduler, it may adopt only the
    /// exact post-initial-ATTACH state: no recovery tail, retired transport,
    /// replacement ACK, queued/armed/deadline-won probe authority, live flow,
    /// replay/receive ownership, sink offer, or terminal tombstone. The latter
    /// cannot be reconstructed from a read-only snapshot because its private
    /// `TerminalGrace` token must move into the root's typed timer registry.
    pub(super) fn root_start_is_quiescent(&self) -> bool {
        self.root_owned_data_plane_is_quiescent()
    }

    /// Exact pre-join gate for the production root after it has closed the
    /// shared TCP factory. Unlike a snapshot-only check, this includes the
    /// controller-owned current FIFO and ordered-delivery waiter.
    pub(super) fn root_flow_shutdown_is_quiescent(&self) -> bool {
        self.external_flow_admission_is_closed() && self.root_owned_data_plane_is_quiescent()
    }

    /// Shutdown may release terminal tombstones before reducer quiescence,
    /// but only after every earlier RESET has left the controller FIFO and any
    /// ordered physical-delivery receipt has retired.
    pub(super) fn root_shutdown_transport_is_pristine(&self) -> bool {
        self.external_flow_admission_is_closed()
            && matches!(
                self.phase,
                ClientControllerPhase::Active {
                    recovery_tail: None
                }
            )
            && self
                .current
                .as_ref()
                .is_some_and(|current| current.transport.root_start_is_pristine())
    }

    fn root_owned_data_plane_is_quiescent(&self) -> bool {
        matches!(
            self.phase,
            ClientControllerPhase::Active {
                recovery_tail: None
            }
        ) && self
            .current
            .as_ref()
            .is_some_and(|current| current.transport.root_start_is_pristine())
            && self.retired.is_none()
            && self.pending_replacement_application_ack.is_none()
            && self.pending_observation_settlement.is_none()
            && matches!(self.current_liveness.phase, ClientCurrentProbePhase::Idle)
            && self.shutdown_current_transport_residual.is_none()
            && self.reducer_root_start_is_pristine()
    }

    fn reducer_root_start_is_pristine(&self) -> bool {
        let snapshot = self.supervisor.snapshot();
        let session = snapshot.session;
        let client_replay = session.replay_usage(Direction::ClientToTarget);
        let target_replay = session.replay_usage(Direction::TargetToClient);
        session.flow_count() == 0
            && client_replay.bytes() == 0
            && client_replay.segments() == 0
            && target_replay.bytes() == 0
            && target_replay.segments() == 0
            && session.receive_owned_bytes() == 0
            && session.receive_ranges() == 0
            && session.outstanding_sink_offers() == 0
            && session.terminal_tombstones() == 0
            && snapshot.uplink_replay_extents == 0
            && snapshot.uplink_replay_owned_bytes == 0
            && snapshot.quarantined_uplink_extents == 0
            && snapshot.quarantined_uplink_owned_bytes == 0
            && !snapshot.owner_catch_up_permit
            && !snapshot.owner_catch_up_permit_armed
            && !snapshot.poisoned
    }

    /// Actor join counters cannot see the controller's sole outbound FIFO.
    /// The root-facing join-all path captures this before its first await, so
    /// terminal retirement and caller cancellation cannot erase the witness.
    pub(super) fn capture_current_transport_shutdown_residual(&mut self) {
        if self.shutdown_current_transport_residual.is_none() {
            self.shutdown_current_transport_residual = self.live_current_transport_residual();
        }
    }

    /// Returns either the pre-await sticky witness or residual ownership that
    /// remains live in the current/terminal-bound transport.
    pub(super) fn current_transport_shutdown_residual(&self) -> Option<ClientLegTransportResidual> {
        self.shutdown_current_transport_residual
            .or_else(|| self.live_current_transport_residual())
    }

    pub(super) const fn shutdown_settlement(&self) -> Option<ClientControllerShutdownSettlement> {
        self.observation_settlement_history.last
    }

    pub(super) fn ownership_ledger(
        &self,
        live_probe_scheduler_authorities: usize,
        live_scheduler_probe_deadlines: usize,
    ) -> ClientControllerOwnershipLedger {
        fn probe_inventory(phase: &ClientCurrentProbePhase) -> (usize, usize) {
            match phase {
                ClientCurrentProbePhase::Idle => (0, 0),
                ClientCurrentProbePhase::Queued { .. } => (1, 1),
                ClientCurrentProbePhase::Armed { .. }
                | ClientCurrentProbePhase::AckWonQueued { .. }
                | ClientCurrentProbePhase::AckWonArmed { .. }
                | ClientCurrentProbePhase::DeadlineWon { .. } => (1, 0),
            }
        }

        fn application_ack_inventory(
            ack: Option<&PendingReplacementApplicationAck>,
        ) -> (usize, usize, usize) {
            ack.map_or((0, 0, 0), |ack| {
                (1, ack.replay.range_count(), ack.replay.data_bytes())
            })
        }

        let (current_probe_authorities, current_probe_receipts) =
            probe_inventory(&self.current_liveness.phase);
        let (current_acks, current_ack_ranges, current_ack_bytes) =
            application_ack_inventory(self.pending_replacement_application_ack.as_ref());
        let (
            pending_probe_authorities,
            pending_probe_receipts,
            pending_acks,
            pending_ack_ranges,
            pending_ack_bytes,
        ) = self
            .pending_observation_settlement
            .as_ref()
            .map_or((0, 0, 0, 0, 0), |pending| {
                let (probe_authorities, probe_receipts) = probe_inventory(&pending.probe);
                let (acks, ack_ranges, ack_bytes) =
                    application_ack_inventory(pending.application_ack.as_ref());
                (
                    probe_authorities,
                    probe_receipts,
                    acks,
                    ack_ranges,
                    ack_bytes,
                )
            });
        let Some(live_probe_authorities) =
            current_probe_authorities.checked_add(pending_probe_authorities)
        else {
            return ClientControllerOwnershipLedger::overflow(
                self.observation_settlement_history,
                self.probe_ack_settlement_history,
            );
        };
        let Some(live_probe_receipts) = current_probe_receipts.checked_add(pending_probe_receipts)
        else {
            return ClientControllerOwnershipLedger::overflow(
                self.observation_settlement_history,
                self.probe_ack_settlement_history,
            );
        };
        let Some(live_application_ack_observations) = current_acks.checked_add(pending_acks) else {
            return ClientControllerOwnershipLedger::overflow(
                self.observation_settlement_history,
                self.probe_ack_settlement_history,
            );
        };
        let Some(live_application_ack_replay_ranges) =
            current_ack_ranges.checked_add(pending_ack_ranges)
        else {
            return ClientControllerOwnershipLedger::overflow(
                self.observation_settlement_history,
                self.probe_ack_settlement_history,
            );
        };
        let Some(live_application_ack_replay_bytes) =
            current_ack_bytes.checked_add(pending_ack_bytes)
        else {
            return ClientControllerOwnershipLedger::overflow(
                self.observation_settlement_history,
                self.probe_ack_settlement_history,
            );
        };
        let terminating_current = usize::from(matches!(
            self.phase,
            ClientControllerPhase::TerminatingCurrent(_)
        ));
        let Some(live_current_transport_owners) =
            usize::from(self.current.is_some()).checked_add(terminating_current)
        else {
            return ClientControllerOwnershipLedger::overflow(
                self.observation_settlement_history,
                self.probe_ack_settlement_history,
            );
        };
        let live_pending_loss_authorities = usize::from(matches!(
            self.phase,
            ClientControllerPhase::TerminatingCurrent(
                TerminatingCurrentClientTransport::RecoveryPending { .. }
            )
        ));
        let live_recovery_transactions = match &self.phase {
            ClientControllerPhase::Recovering(_) => 1,
            ClientControllerPhase::Active {
                recovery_tail: Some(_),
            } => 1,
            ClientControllerPhase::TerminatingCurrent(
                TerminatingCurrentClientTransport::RecoveryPending { recovery, .. },
            ) => usize::from(!matches!(
                recovery,
                TerminatingCurrentRecoveryOwnership::NoRecovery
            )),
            ClientControllerPhase::Active {
                recovery_tail: None,
            }
            | ClientControllerPhase::TerminatingCurrent(
                TerminatingCurrentClientTransport::RecoveryDiscarded { .. },
            )
            | ClientControllerPhase::Legless => 0,
        };
        ClientControllerOwnershipLedger {
            live_current_transport_owners,
            live_retired_transport_owners: usize::from(self.retired.is_some()),
            live_active_leg_authorities: usize::from(self.current.is_some()),
            live_terminal_bound_authorities: terminating_current,
            live_pending_loss_authorities,
            live_recovery_transactions,
            live_probe_authorities,
            live_probe_receipts,
            live_probe_scheduler_authorities,
            live_scheduler_probe_deadlines,
            live_application_ack_observations,
            live_application_ack_replay_ranges,
            live_application_ack_replay_bytes,
            live_pending_settlements: usize::from(self.pending_observation_settlement.is_some()),
            settlement_history: self.observation_settlement_history,
            probe_ack_settlement_history: self.probe_ack_settlement_history,
            overflowed: false,
        }
    }

    fn live_current_transport_residual(&self) -> Option<ClientLegTransportResidual> {
        self.current
            .as_ref()
            .and_then(|current| current.transport.shutdown_residual())
            .or_else(|| match &self.phase {
                ClientControllerPhase::TerminatingCurrent(
                    TerminatingCurrentClientTransport::RecoveryPending { transport, .. },
                ) => transport.shutdown_residual(),
                ClientControllerPhase::Active { .. }
                | ClientControllerPhase::Recovering(_)
                | ClientControllerPhase::TerminatingCurrent(
                    TerminatingCurrentClientTransport::RecoveryDiscarded { .. },
                )
                | ClientControllerPhase::Legless => None,
            })
    }

    pub(crate) fn mint_tcp_port_factory(
        &mut self,
        config: FlowPortConfig,
    ) -> Result<ResumableTcpPortFactory, TcpPortFactoryMintError> {
        self.supervisor.mint_tcp_port_factory(config)
    }

    /// Seals all clones of the sole TCP factory before session shutdown can
    /// await actor cleanup. Returns whether this call performed the close.
    pub(super) fn close_tcp_factory_admission(&mut self) -> bool {
        matches!(
            self.supervisor.close_tcp_factory_admission(),
            super::tcp::TcpFactoryAdmissionClose::ClosedNow
        )
    }

    pub(super) fn external_flow_admission_is_closed(&self) -> bool {
        self.supervisor.tcp_factory_admission_is_closed()
    }

    pub(super) fn apply_driver_input(
        &mut self,
        input: ClientControllerDriverInput,
    ) -> ClientControllerDriverApply {
        let command = SessionOwnerCommand::ClientDriver(input);
        match self.supervisor.apply_command(command) {
            Ok(effects) => ClientControllerDriverApply::Applied(effects),
            Err(SessionSupervisorError::RejectedCommand { block, command }) => match *command {
                SessionOwnerCommand::ClientDriver(input) => {
                    ClientControllerDriverApply::Deferred { block, input }
                }
                command => {
                    ClientControllerDriverApply::Failed(SessionSupervisorError::RejectedCommand {
                        block,
                        command: Box::new(command),
                    })
                }
            },
            Err(error) => ClientControllerDriverApply::Failed(error),
        }
    }

    pub(super) fn apply_driver_input_observed(
        &mut self,
        input: ClientControllerDriverInput,
    ) -> ClientControllerObservedDriverApply {
        let observation_gate = ClientControllerObservedApplyGate::mint();
        match self
            .supervisor
            .apply_client_driver_input_observed(input, &observation_gate)
        {
            Ok(observed) => {
                let (effects, observations) = observed.into_client_parts();
                ClientControllerObservedDriverApply::Applied(ClientControllerObservedApply {
                    effects,
                    observations: ClientControllerReducerObservations::from_supervisor(
                        observations,
                    ),
                })
            }
            Err(SessionSupervisorError::RejectedCommand { block, command }) => match *command {
                SessionOwnerCommand::ClientDriver(input) => {
                    ClientControllerObservedDriverApply::Deferred { block, input }
                }
                command => ClientControllerObservedDriverApply::Failed(
                    SessionSupervisorError::RejectedCommand {
                        block,
                        command: Box::new(command),
                    },
                ),
            },
            Err(error) => ClientControllerObservedDriverApply::Failed(error),
        }
    }

    pub(crate) fn expire_terminal_grace(
        &mut self,
        terminal: TerminalGrace,
    ) -> Result<Vec<SessionEffect>, SessionSupervisorError> {
        self.expire_terminal_grace_preserving(terminal)
            .map_err(|(_terminal, error)| error)
    }

    /// Ownership-preserving terminal-timer reducer seam. A cross-session,
    /// unknown, or already-consumed capability is an invariant failure, not
    /// retryable reducer pressure, but the timer owner still needs the exact
    /// authority for sticky failure accounting and structured shutdown.
    pub(super) fn expire_terminal_grace_preserving(
        &mut self,
        terminal: TerminalGrace,
    ) -> Result<Vec<SessionEffect>, (TerminalGrace, SessionSupervisorError)> {
        self.supervisor
            .apply_event(SessionEvent::TerminalGraceExpired { terminal })
            .map_err(|error| (terminal, error))
    }

    pub(crate) fn open_local(
        &mut self,
        target: TargetAddr,
    ) -> Option<Result<Vec<SessionEffect>, SessionSupervisorError>> {
        let event = self.current.as_ref()?.authority.local_open_event(target);
        Some(self.supervisor.apply_event(event))
    }

    #[allow(
        clippy::result_large_err,
        reason = "prepared OPEN rejection returns exact Target, Frame, and adapter ownership without cloning"
    )]
    pub(super) fn install_prepared_local_open<T, E>(
        &mut self,
        target: TargetAddr,
        prepare: impl FnOnce(LocalFlow) -> Result<T, E>,
    ) -> Result<ClientCommittedLocalOpen<T>, ClientPreparedLocalOpenFailure<E>> {
        if self.external_flow_admission_is_closed() {
            return Err(ClientPreparedLocalOpenFailure::Blocked {
                target,
                block: ClientPreparedLocalOpenBlock::FactoryAdmissionClosed,
            });
        }
        match &self.phase {
            ClientControllerPhase::Active {
                recovery_tail: None,
            } => {}
            ClientControllerPhase::Active {
                recovery_tail: Some(_),
            } => {
                return Err(ClientPreparedLocalOpenFailure::Blocked {
                    target,
                    block: ClientPreparedLocalOpenBlock::RecoveryTailPending,
                });
            }
            ClientControllerPhase::Recovering(_) => {
                return Err(ClientPreparedLocalOpenFailure::Blocked {
                    target,
                    block: ClientPreparedLocalOpenBlock::RecoveryPending,
                });
            }
            ClientControllerPhase::TerminatingCurrent(_) => {
                return Err(ClientPreparedLocalOpenFailure::Blocked {
                    target,
                    block: ClientPreparedLocalOpenBlock::Terminating,
                });
            }
            ClientControllerPhase::Legless => {
                return Err(ClientPreparedLocalOpenFailure::Blocked {
                    target,
                    block: ClientPreparedLocalOpenBlock::Legless,
                });
            }
        }
        let Some(current) = self.current.as_mut() else {
            return Err(ClientPreparedLocalOpenFailure::Blocked {
                target,
                block: ClientPreparedLocalOpenBlock::MissingCurrent,
            });
        };
        let leg = current.authority.committed_leg();
        let supervisor = &mut self.supervisor;
        match supervisor.install_prepared_client_local_open(leg, target, |input| {
            let (local_flow, open_frame) = input.into_parts();
            let reservation = match current.transport.prepare_ordinary_frame(open_frame) {
                Ok(reservation) => reservation,
                Err(error) => {
                    let kind = error.kind().clone();
                    let input = PreparedLocalOpenInput::from_parts(local_flow, error.into_frame());
                    return Err(input.reject(ClientLocalOpenAdapterPrepareError::Queue(kind)));
                }
            };
            let prepared_adapter = match prepare(local_flow) {
                Ok(prepared_adapter) => prepared_adapter,
                Err(error) => {
                    let input =
                        PreparedLocalOpenInput::from_parts(local_flow, reservation.into_frame());
                    return Err(input.reject(ClientLocalOpenAdapterPrepareError::Adapter(error)));
                }
            };
            Ok((prepared_adapter, reservation))
        }) {
            Ok(Ok(committed)) => {
                let ((prepared_adapter, reservation), local_flow) = committed.into_parts();
                let enqueue_ordinal = reservation.commit();
                Ok(ClientCommittedLocalOpen {
                    prepared_adapter,
                    local_flow,
                    enqueue_ordinal,
                })
            }
            Ok(Err(failure)) => {
                let (target, input, error) = failure.into_parts();
                let (_local_flow, frame) = input.into_parts();
                match error {
                    ClientLocalOpenAdapterPrepareError::Queue(kind) => {
                        Err(ClientPreparedLocalOpenFailure::Queue {
                            target,
                            frame: Box::new(frame),
                            kind,
                        })
                    }
                    ClientLocalOpenAdapterPrepareError::Adapter(error) => {
                        Err(ClientPreparedLocalOpenFailure::Adapter {
                            target,
                            frame: Box::new(frame),
                            error,
                        })
                    }
                }
            }
            Err(failure) => Err(ClientPreparedLocalOpenFailure::Supervisor(failure)),
        }
    }

    pub(crate) fn enqueue_current_frame(
        &mut self,
        frame: Frame,
    ) -> Option<Result<(), LegOutboundQueueError>> {
        match self.enqueue_current_frame_preserving(frame) {
            Ok(()) => Some(Ok(())),
            Err(ClientCurrentFrameEnqueueFailure::MissingCurrent(_frame)) => None,
            Err(ClientCurrentFrameEnqueueFailure::Queue(error)) => Some(Err(error)),
        }
    }

    #[allow(
        clippy::result_large_err,
        reason = "every enqueue rejection returns the exact unconsumed frame ownership"
    )]
    pub(super) fn enqueue_current_frame_preserving(
        &mut self,
        frame: Frame,
    ) -> Result<(), ClientCurrentFrameEnqueueFailure> {
        let Some(current) = self.current.as_mut() else {
            return Err(ClientCurrentFrameEnqueueFailure::MissingCurrent(frame));
        };
        current
            .transport
            .push_frame(frame)
            .map_err(ClientCurrentFrameEnqueueFailure::Queue)
    }

    pub(super) fn enqueue_current_reset_frame_preserving(
        &mut self,
        frame: Frame,
        reset_delivery: OrderedResetDeliveryReceipt,
    ) -> Result<(), ClientCurrentFrameEnqueueFailure> {
        let Some(current) = self.current.as_mut() else {
            return Err(ClientCurrentFrameEnqueueFailure::MissingCurrent(frame));
        };
        current
            .transport
            .push_frame_with_reset_delivery(frame, reset_delivery)
            .map_err(ClientCurrentFrameEnqueueFailure::Queue)
    }

    #[allow(
        clippy::result_large_err,
        reason = "the error must return the exact unconsumed control frame"
    )]
    pub(super) fn enqueue_current_ordered_leg_control(
        &mut self,
        frame: LegControlFrame,
        features: FeatureSet,
        gate: &QuinnLegCompositionGate,
    ) -> Option<Result<LegControlEnqueued, LegControlQueueError>> {
        Some(
            self.current
                .as_mut()?
                .transport
                .push_ordered_leg_control(frame, features, gate),
        )
    }

    /// Queues one exact current-leg liveness probe. The controller retains
    /// the non-cloneable FIFO receipt; no deadline exists until the actor's
    /// ordered writer positively completes this record.
    pub(super) fn begin_current_probe(
        &mut self,
        gate: &QuinnLegCompositionGate,
    ) -> Result<ClientCurrentProbeQueued, ClientCurrentProbeBeginError> {
        if !matches!(self.phase, ClientControllerPhase::Active { .. }) {
            return Err(ClientCurrentProbeBeginError::PolicyBlocked);
        }
        if !matches!(self.current_liveness.phase, ClientCurrentProbePhase::Idle) {
            return Err(ClientCurrentProbeBeginError::OutstandingProbe);
        }
        let sequence = self
            .current_liveness
            .next_sequence
            .ok_or(ClientCurrentProbeBeginError::SequenceExhausted)?;
        let current = self
            .current
            .as_mut()
            .ok_or(ClientCurrentProbeBeginError::MissingCurrent)?;
        let correlation = ClientCurrentProbeCorrelation {
            seal: current.authority.share_probe_seal(),
            session_id: current.authority.probe_session_id(),
            generation: current.authority.generation(),
            epoch_nonce: current.authority.probe_epoch_nonce(),
            sequence,
            switch_epoch: self.current_liveness.switch_epoch,
        };
        let frame = LegControlFrame::try_new(
            correlation.generation,
            LegControlRecord::LegProbe {
                session_id: correlation.session_id,
                leg_epoch_nonce: correlation.epoch_nonce,
                probe_sequence: correlation.sequence,
            },
            self.installed_features,
        )
        .map_err(ClientCurrentProbeBeginError::Protocol)?;
        let receipt = current
            .transport
            .push_ordered_leg_control(frame, self.installed_features, gate)
            .map_err(|error| ClientCurrentProbeBeginError::Queue(Box::new(error)))?;
        self.current_liveness.next_sequence = sequence.checked_next().ok();
        let identity = Arc::new(ClientCurrentProbeIdentity { correlation });
        let queued = ClientCurrentProbeQueued {
            identity: Arc::clone(&identity),
        };
        self.current_liveness.phase = ClientCurrentProbePhase::Queued { identity, receipt };
        Ok(queued)
    }

    pub(super) fn arm_current_probe(
        &mut self,
        queued: ClientCurrentProbeQueued,
        now: SimTime,
        timeout: Duration,
    ) -> Result<ClientCurrentProbeDeadline, ClientCurrentProbeArmFailure> {
        let phase = std::mem::replace(
            &mut self.current_liveness.phase,
            ClientCurrentProbePhase::Idle,
        );
        match phase {
            ClientCurrentProbePhase::Queued { identity, receipt } => {
                if !Arc::ptr_eq(&identity, &queued.identity) {
                    self.current_liveness.phase =
                        ClientCurrentProbePhase::Queued { identity, receipt };
                    return Err(ClientCurrentProbeArmFailure::new(
                        queued,
                        ClientCurrentProbeArmError::Stale,
                    ));
                }
                if !receipt.was_delivered() {
                    self.current_liveness.phase =
                        ClientCurrentProbePhase::Queued { identity, receipt };
                    return Err(ClientCurrentProbeArmFailure::new(
                        queued,
                        ClientCurrentProbeArmError::NotDelivered,
                    ));
                }
                if timeout.is_zero() {
                    self.current_liveness.phase =
                        ClientCurrentProbePhase::Queued { identity, receipt };
                    return Err(ClientCurrentProbeArmFailure::new(
                        queued,
                        ClientCurrentProbeArmError::InvalidTimeout,
                    ));
                }
                let deadline = match now.checked_add(timeout) {
                    Ok(deadline) => deadline,
                    Err(error) => {
                        self.current_liveness.phase =
                            ClientCurrentProbePhase::Queued { identity, receipt };
                        return Err(ClientCurrentProbeArmFailure::new(
                            queued,
                            ClientCurrentProbeArmError::Deadline(error),
                        ));
                    }
                };
                self.current_liveness.phase = ClientCurrentProbePhase::Armed {
                    identity: Arc::clone(&identity),
                    deadline,
                };
                Ok(ClientCurrentProbeDeadline { identity, deadline })
            }
            phase => {
                self.current_liveness.phase = phase;
                Err(ClientCurrentProbeArmFailure::new(
                    queued,
                    ClientCurrentProbeArmError::Stale,
                ))
            }
        }
    }

    fn accept_current_probe_ack(
        &mut self,
        inbound: super::leg::LegBoundControlFrame,
        received_at: SimTime,
    ) -> Option<ClientCurrentProbeAcked> {
        let (seal, frame) = inbound.into_parts();
        let phase = std::mem::replace(
            &mut self.current_liveness.phase,
            ClientCurrentProbePhase::Idle,
        );
        let current_switch_epoch = self.current_liveness.switch_epoch;
        match phase {
            ClientCurrentProbePhase::Queued {
                identity,
                receipt: _receipt,
            } if identity
                .correlation
                .same_wire_ack(&seal, &frame, current_switch_epoch) =>
            {
                let acked = ClientCurrentProbeAcked {
                    generation: identity.correlation.generation,
                    probe_sequence: identity.correlation.sequence,
                };
                // A valid peer ACK causally proves the exact probe crossed the
                // writer and peer decoder even if the independent DELIVERED
                // lane is delayed. The local receipt is now redundant, but
                // controller liveness stays in AckWon until root returns the
                // pointer-identical queued scheduler authority.
                self.current_liveness.phase = ClientCurrentProbePhase::AckWonQueued { identity };
                Some(acked)
            }
            ClientCurrentProbePhase::Armed { identity, deadline }
                if received_at <= deadline
                    && identity
                        .correlation
                        .same_wire_ack(&seal, &frame, current_switch_epoch) =>
            {
                let acked = ClientCurrentProbeAcked {
                    generation: identity.correlation.generation,
                    probe_sequence: identity.correlation.sequence,
                };
                self.current_liveness.phase =
                    ClientCurrentProbePhase::AckWonArmed { identity, deadline };
                Some(acked)
            }
            phase => {
                self.current_liveness.phase = phase;
                None
            }
        }
    }

    pub(super) fn current_probe_deadline_status(
        &self,
        token: &ClientCurrentProbeDeadline,
        now: SimTime,
    ) -> ClientCurrentProbeDeadlineStatus {
        match &self.current_liveness.phase {
            ClientCurrentProbePhase::Armed { identity, deadline }
                if Arc::ptr_eq(identity, &token.identity) && *deadline == token.deadline =>
            {
                if now < *deadline {
                    ClientCurrentProbeDeadlineStatus::NotDue
                } else {
                    ClientCurrentProbeDeadlineStatus::Due
                }
            }
            ClientCurrentProbePhase::AckWonArmed { identity, deadline }
                if Arc::ptr_eq(identity, &token.identity) && *deadline == token.deadline =>
            {
                ClientCurrentProbeDeadlineStatus::AckWon {
                    generation: identity.correlation.generation,
                    probe_sequence: identity.correlation.sequence,
                }
            }
            ClientCurrentProbePhase::Idle
            | ClientCurrentProbePhase::Queued { .. }
            | ClientCurrentProbePhase::Armed { .. }
            | ClientCurrentProbePhase::AckWonQueued { .. }
            | ClientCurrentProbePhase::AckWonArmed { .. }
            | ClientCurrentProbePhase::DeadlineWon { .. } => {
                ClientCurrentProbeDeadlineStatus::Stale
            }
        }
    }

    pub(super) fn reconcile_current_probe_ack(
        &mut self,
        scheduler: ClientCurrentProbeSchedulerAuthority,
        generation: LegGeneration,
        probe_sequence: ProbeSequence,
    ) -> Result<ClientCurrentProbeAckSettlement, ClientCurrentProbeAckReconcileFailure> {
        let settlement = match &self.current_liveness.phase {
            ClientCurrentProbePhase::AckWonQueued { identity } => {
                if identity.correlation.generation != generation
                    || identity.correlation.sequence != probe_sequence
                {
                    return Err(ClientCurrentProbeAckReconcileFailure::new(
                        scheduler,
                        ClientCurrentProbeAckReconcileErrorKind::WrongAcknowledgement,
                    ));
                }
                let ClientCurrentProbeSchedulerAuthority::Queued(token) = &scheduler else {
                    return Err(ClientCurrentProbeAckReconcileFailure::new(
                        scheduler,
                        ClientCurrentProbeAckReconcileErrorKind::WrongSchedulerAuthority,
                    ));
                };
                if !Arc::ptr_eq(identity, &token.identity) {
                    return Err(ClientCurrentProbeAckReconcileFailure::new(
                        scheduler,
                        ClientCurrentProbeAckReconcileErrorKind::WrongSchedulerAuthority,
                    ));
                }
                ClientCurrentProbeAckSettlement {
                    phase: ClientCurrentProbeAckSettlementPhase::Queued,
                    generation,
                    probe_sequence,
                }
            }
            ClientCurrentProbePhase::AckWonArmed { identity, deadline } => {
                if identity.correlation.generation != generation
                    || identity.correlation.sequence != probe_sequence
                {
                    return Err(ClientCurrentProbeAckReconcileFailure::new(
                        scheduler,
                        ClientCurrentProbeAckReconcileErrorKind::WrongAcknowledgement,
                    ));
                }
                let ClientCurrentProbeSchedulerAuthority::Armed(token) = &scheduler else {
                    return Err(ClientCurrentProbeAckReconcileFailure::new(
                        scheduler,
                        ClientCurrentProbeAckReconcileErrorKind::WrongSchedulerAuthority,
                    ));
                };
                if !Arc::ptr_eq(identity, &token.identity) || *deadline != token.deadline {
                    return Err(ClientCurrentProbeAckReconcileFailure::new(
                        scheduler,
                        ClientCurrentProbeAckReconcileErrorKind::WrongSchedulerAuthority,
                    ));
                }
                ClientCurrentProbeAckSettlement {
                    phase: ClientCurrentProbeAckSettlementPhase::Armed,
                    generation,
                    probe_sequence,
                }
            }
            ClientCurrentProbePhase::Idle
            | ClientCurrentProbePhase::Queued { .. }
            | ClientCurrentProbePhase::Armed { .. }
            | ClientCurrentProbePhase::DeadlineWon { .. } => {
                return Err(ClientCurrentProbeAckReconcileFailure::new(
                    scheduler,
                    ClientCurrentProbeAckReconcileErrorKind::NotAckWon,
                ));
            }
        };
        let Some(next_history) = self.probe_ack_settlement_history.checked_record(settlement)
        else {
            return Err(ClientCurrentProbeAckReconcileFailure::new(
                scheduler,
                ClientCurrentProbeAckReconcileErrorKind::HistoryOverflow,
            ));
        };
        let won = std::mem::replace(
            &mut self.current_liveness.phase,
            ClientCurrentProbePhase::Idle,
        );
        drop((won, scheduler));
        self.probe_ack_settlement_history = next_history;
        Ok(settlement)
    }

    #[cfg(test)]
    pub(super) fn current_probe_writer_completed_for_test(&self) -> bool {
        matches!(
            &self.current_liveness.phase,
            ClientCurrentProbePhase::Queued { receipt, .. } if receipt.was_delivered()
        )
    }

    pub(super) fn commit_due_current_probe(
        &mut self,
        token: ClientCurrentProbeDeadline,
        now: SimTime,
    ) -> Result<ClientCurrentProbeExpired, ClientCurrentProbeDeadline> {
        if self.current_probe_deadline_status(&token, now) != ClientCurrentProbeDeadlineStatus::Due
        {
            return Err(token);
        }
        let phase = std::mem::replace(
            &mut self.current_liveness.phase,
            ClientCurrentProbePhase::Idle,
        );
        let ClientCurrentProbePhase::Armed { identity, deadline } = phase else {
            self.current_liveness.phase = phase;
            return Err(token);
        };
        if !Arc::ptr_eq(&identity, &token.identity) || deadline != token.deadline {
            self.current_liveness.phase = ClientCurrentProbePhase::Armed { identity, deadline };
            return Err(token);
        }
        let expired = ClientCurrentProbeExpired {
            generation: identity.correlation.generation,
            probe_sequence: identity.correlation.sequence,
        };
        self.current_liveness.phase = ClientCurrentProbePhase::DeadlineWon { identity };
        Ok(expired)
    }

    pub(super) fn begin_current_standby_registration(
        &self,
        candidate: &EstablishedLeg,
        nonce: StandbyNonce,
        credentials: &AttachCredentials,
    ) -> Result<PendingStandbyRegistration, StandbyRegistrationError> {
        let current = self
            .current
            .as_ref()
            .ok_or(StandbyRegistrationError::Rejected)?;
        PendingStandbyRegistration::begin_from_client_active(
            &current.authority,
            candidate,
            nonce,
            credentials,
        )
    }

    fn recovery_owned_current_transport(&self) -> Option<&ClientLegTransport> {
        self.current
            .as_ref()
            .map(|current| &current.transport)
            .or(match &self.phase {
                ClientControllerPhase::TerminatingCurrent(
                    TerminatingCurrentClientTransport::RecoveryPending { transport, .. },
                ) => Some(transport),
                ClientControllerPhase::Active { .. }
                | ClientControllerPhase::Recovering(_)
                | ClientControllerPhase::TerminatingCurrent(
                    TerminatingCurrentClientTransport::RecoveryDiscarded { .. },
                )
                | ClientControllerPhase::Legless => None,
            })
    }

    #[cfg(test)]
    pub(crate) fn current_queue_is_empty(&self) -> Option<bool> {
        Some(self.recovery_owned_current_transport()?.queue_is_empty())
    }

    #[cfg(test)]
    pub(crate) fn current_queued_frames(&self) -> Option<usize> {
        Some(self.recovery_owned_current_transport()?.queued_frames())
    }

    #[cfg(test)]
    pub(crate) fn current_queued_bytes(&self) -> Option<usize> {
        Some(self.recovery_owned_current_transport()?.queued_bytes())
    }

    #[cfg(test)]
    pub(crate) fn force_submitted_reset_root_delivered_before_attempt_for_test(&self) -> bool {
        self.recovery_owned_current_transport()
            .is_some_and(|transport| {
                transport.force_submitted_reset_root_delivered_before_attempt_for_test()
            })
    }

    #[cfg(test)]
    pub(crate) fn retired_supersession_for_test(
        &self,
    ) -> Option<(LegGeneration, LegGeneration, usize, usize)> {
        let audit = self
            .retired
            .as_ref()
            .and_then(|retired| retired.supersession)
            .or(self.supersession_history.last)?;
        Some((
            audit.retired_generation,
            audit.replacement_generation,
            audit.queue.queued_frames(),
            audit.queue.owned_bytes(),
        ))
    }

    #[cfg(test)]
    pub(crate) fn retired_supersession_detail_for_test(
        &self,
    ) -> Option<(bool, &'static str, bool)> {
        let audit = self
            .retired
            .as_ref()
            .and_then(|retired| retired.supersession)
            .or(self.supersession_history.last)?;
        let phase = match audit.liveness.phase {
            ClientSupersededProbePhase::Idle => "Idle",
            ClientSupersededProbePhase::Queued => "Queued",
            ClientSupersededProbePhase::Armed => "Armed",
            ClientSupersededProbePhase::AckWonQueued => "AckWonQueued",
            ClientSupersededProbePhase::AckWonArmed => "AckWonArmed",
            ClientSupersededProbePhase::DeadlineWon => "DeadlineWon",
        };
        Some((
            audit.queue.ordered_delivery_wait(),
            phase,
            audit.liveness.ordered_receipt,
        ))
    }

    #[cfg(test)]
    pub(crate) fn force_supersession_history_replacements_for_test(&mut self, replacements: usize) {
        self.supersession_history.replacements = replacements;
    }

    #[cfg(test)]
    pub(crate) fn current_counters(&self) -> Option<LegIoCounters> {
        Some(self.recovery_owned_current_transport()?.counters())
    }

    #[cfg(test)]
    pub(crate) fn recovery_tail_inventory_for_test(&self) -> Option<ClientRecoveryTailInventory> {
        match &self.phase {
            ClientControllerPhase::Active {
                recovery_tail: None,
            } => Some(ClientRecoveryTailInventory::default()),
            ClientControllerPhase::Active {
                recovery_tail: Some(tail),
            } => Some(
                self.current
                    .as_ref()?
                    .transport
                    .recovery_tail_inventory(tail),
            ),
            ClientControllerPhase::Recovering(_)
            | ClientControllerPhase::TerminatingCurrent(_)
            | ClientControllerPhase::Legless => None,
        }
    }

    /// Classifies only the physical recovery-tail capability. An installed
    /// recovery reservation is not a tail, while a tail already moved under
    /// the exact terminal transaction must remain distinguishable from a
    /// successfully settled/absent tail.
    pub(super) fn recovery_tail_ownership(&self) -> ClientRecoveryTailOwnership {
        match &self.phase {
            ClientControllerPhase::Active {
                recovery_tail: Some(_),
            } => ClientRecoveryTailOwnership::Active,
            ClientControllerPhase::TerminatingCurrent(
                TerminatingCurrentClientTransport::RecoveryPending {
                    recovery: TerminatingCurrentRecoveryOwnership::Tail(_),
                    ..
                },
            ) => ClientRecoveryTailOwnership::TerminalOwned,
            ClientControllerPhase::Active {
                recovery_tail: None,
            }
            | ClientControllerPhase::Recovering(_)
            | ClientControllerPhase::TerminatingCurrent(_)
            | ClientControllerPhase::Legless => ClientRecoveryTailOwnership::Absent,
        }
    }

    pub(crate) fn generation(&self) -> LegGeneration {
        self.supervisor.snapshot().session.generation()
    }

    pub(crate) fn replacement_barrier_is_active(&self) -> bool {
        matches!(
            self.phase,
            ClientControllerPhase::Recovering(_)
                | ClientControllerPhase::TerminatingCurrent(
                    TerminatingCurrentClientTransport::RecoveryPending { .. }
                )
        )
    }

    pub(super) fn normal_replacement_blocker(&self) -> Option<ClientRecoveryBeginErrorKind> {
        let ClientControllerPhase::Active { recovery_tail } = &self.phase else {
            return Some(ClientRecoveryBeginErrorKind::NotActive);
        };
        if let Some(tail) = recovery_tail {
            return Some(if tail.actual_delivery_complete() {
                ClientRecoveryBeginErrorKind::PriorRecoveryTailUnsettled
            } else {
                ClientRecoveryBeginErrorKind::PriorRecoveryDeliveryPending
            });
        }
        if self.current.is_none() {
            return Some(ClientRecoveryBeginErrorKind::NotActive);
        }
        if self.retired.is_some() {
            return Some(ClientRecoveryBeginErrorKind::RetiredCapacityExceeded);
        }
        if self.pending_replacement_application_ack.is_some() {
            return Some(ClientRecoveryBeginErrorKind::PriorReplacementApplicationAckPending);
        }
        if self.pending_observation_settlement.is_some() {
            return Some(ClientRecoveryBeginErrorKind::ObservationSettlementPending);
        }
        if !matches!(self.current_liveness.phase, ClientCurrentProbePhase::Idle) {
            return Some(ClientRecoveryBeginErrorKind::CurrentProbeSettlementPending);
        }
        None
    }

    fn prepare_normal_begin(&mut self) -> Result<(), ClientRecoveryBeginErrorKind> {
        if let Some(blocker) = self.normal_replacement_blocker() {
            return Err(blocker);
        }
        Ok(())
    }

    fn take_normal_supersession_source(
        &mut self,
    ) -> Result<PreparedNormalClientSupersession, ClientRecoveryBeginErrorKind> {
        self.prepare_normal_begin()?;
        let Some(current) = self.current.take() else {
            return Err(ClientRecoveryBeginErrorKind::NotActive);
        };
        let Some(liveness_audit) = self.current_liveness.supersession_audit(&current.authority)
        else {
            self.current = Some(current);
            return Err(ClientRecoveryBeginErrorKind::SupersessionInvariant);
        };
        let ActiveClientTransport {
            authority,
            transport,
        } = current;
        let transport = match transport.prepare_normal_supersession() {
            Ok(transport) => transport,
            Err(transport) => {
                self.current = Some(ActiveClientTransport {
                    authority,
                    transport,
                });
                return Err(ClientRecoveryBeginErrorKind::SupersessionInventoryInvalid);
            }
        };
        let placeholder = ClientCurrentLiveness::for_generation(authority.generation());
        let liveness = std::mem::replace(&mut self.current_liveness, placeholder);
        Ok(PreparedNormalClientSupersession {
            authority,
            transport,
            liveness,
            liveness_audit,
        })
    }

    /// Settles the sole active recovery tail after the composition proves the
    /// exact current physical actor. Every rejection precedes mutation or
    /// restores the same non-cloneable tail into the active slot.
    pub(super) fn settle_completed_recovery_tail(
        &mut self,
        runtime: &PhysicalLegRuntimeIncarnation,
        gate: &QuinnLegCompositionGate,
    ) -> Result<ClientRecoveryTailSettlement, ClientRecoveryTailSettlementBlock> {
        let ClientControllerPhase::Active { recovery_tail } = &self.phase else {
            return Err(ClientRecoveryTailSettlementBlock::NotActive);
        };
        if recovery_tail.is_none() {
            return Ok(ClientRecoveryTailSettlement::AlreadySettled);
        }
        let Some(current) = self.current.as_ref() else {
            return Err(ClientRecoveryTailSettlementBlock::MissingCurrent);
        };
        if !current.transport.belongs_to_physical_actor(runtime, gate) {
            return Err(ClientRecoveryTailSettlementBlock::WrongPhysicalActor);
        }
        self.settle_completed_recovery_tail_after_actor_preflight()
    }

    fn settle_completed_recovery_tail_after_actor_preflight(
        &mut self,
    ) -> Result<ClientRecoveryTailSettlement, ClientRecoveryTailSettlementBlock> {
        let (current, recovery_tail) = match (&mut self.current, &mut self.phase) {
            (Some(current), ClientControllerPhase::Active { recovery_tail }) => {
                (current, recovery_tail)
            }
            (None, ClientControllerPhase::Active { .. }) => {
                return Err(ClientRecoveryTailSettlementBlock::MissingCurrent);
            }
            (_, _) => return Err(ClientRecoveryTailSettlementBlock::NotActive),
        };
        let Some(tail) = recovery_tail.take() else {
            return Ok(ClientRecoveryTailSettlement::AlreadySettled);
        };
        let generation = current.authority.generation();
        match current
            .transport
            .settle_delivered_client_recovery_tail(tail, generation)
        {
            Ok(settled) => {
                debug_assert_eq!(settled.generation(), generation);
                Ok(ClientRecoveryTailSettlement::Settled { generation })
            }
            Err(failure) => {
                let kind = ClientRecoveryTailSettlementBlock::from_leg_io(failure.kind());
                let tail = failure.into_tail();
                debug_assert!(recovery_tail.is_none());
                *recovery_tail = Some(tail);
                Err(kind)
            }
        }
    }

    #[cfg(test)]
    fn settle_completed_recovery_tail_after_actor_preflight_for_test(
        &mut self,
    ) -> Result<ClientRecoveryTailSettlement, ClientRecoveryTailSettlementBlock> {
        self.settle_completed_recovery_tail_after_actor_preflight()
    }

    fn prepare_post_loss_begin(&self) -> Result<(), ClientRecoveryBeginErrorKind> {
        let ClientControllerPhase::TerminatingCurrent(
            TerminatingCurrentClientTransport::RecoveryDiscarded {
                authority,
                generation,
                ..
            },
        ) = &self.phase
        else {
            return Err(ClientRecoveryBeginErrorKind::CatchUpRequiresDiscardedCurrent);
        };
        let snapshot = self.supervisor.snapshot();
        if self.current.is_some()
            || snapshot.session.phase() != crate::resumable::SessionPhase::Legless
            || snapshot.session.generation() != *generation
            || authority.generation() != *generation
        {
            return Err(ClientRecoveryBeginErrorKind::CatchUpRequiresDiscardedCurrent);
        }
        if self.retired.is_some() {
            return Err(ClientRecoveryBeginErrorKind::RetiredCapacityExceeded);
        }
        if self.pending_observation_settlement.is_some() {
            return Err(ClientRecoveryBeginErrorKind::ObservationSettlementPending);
        }
        Ok(())
    }

    fn take_post_loss_retirement_source(
        &mut self,
    ) -> Result<PostLossClientRetirementSource, ClientRecoveryBeginErrorKind> {
        self.prepare_post_loss_begin()?;
        let phase = std::mem::replace(&mut self.phase, ClientControllerPhase::Legless);
        match phase {
            ClientControllerPhase::TerminatingCurrent(
                TerminatingCurrentClientTransport::RecoveryDiscarded {
                    authority,
                    transport,
                    generation,
                    reason,
                },
            ) => Ok(PostLossClientRetirementSource {
                authority,
                transport,
                generation,
                reason,
            }),
            unexpected => {
                self.phase = unexpected;
                Err(ClientRecoveryBeginErrorKind::CatchUpRequiresDiscardedCurrent)
            }
        }
    }

    fn publish_replacement_recovery(
        current: &mut Option<ActiveClientTransport>,
        phase: &mut ClientControllerPhase,
        current_liveness: &mut ClientCurrentLiveness,
        pending_replacement_application_ack: &mut Option<PendingReplacementApplicationAck>,
        session_id: SessionId,
        authority: ClientActiveLeg,
        transport: ClientLegTransport,
        recovery: super::leg_io::PreparedClientRecovery,
        reservation: super::leg_io::ClientRecoveryReservation,
    ) -> ClientRecoveryStarted {
        let generation = authority.generation();
        current_liveness.reset_for_replacement(generation);
        let (recovery_effects, client_local_replay) =
            recovery.into_effects_and_client_local_replay();
        let pending_application_ack =
            (client_local_replay.data_bytes() > 0).then_some(PendingReplacementApplicationAck {
                session_id,
                generation,
                replay: client_local_replay,
            });
        *current = Some(ActiveClientTransport {
            authority,
            transport,
        });
        *phase = ClientControllerPhase::Recovering(ClientRecoveryState {
            generation,
            effects: recovery_effects,
            pending: None,
            reservation: reservation.into_installed(),
            drive_ready: true,
        });
        *pending_replacement_application_ack = pending_application_ack;
        ClientRecoveryStarted { generation }
    }

    /// Consumes opaque accepted B plus caller-owned transport pieces. The
    /// terminal mutex spans reservation, reducer commit, old-A retirement,
    /// and publication of the internal recovery cursor.
    #[allow(
        clippy::result_large_err,
        reason = "pre-commit rejection returns accepted B plus exact endpoint and sole queue"
    )]
    pub(crate) fn begin_registered_replacement(
        &mut self,
        accepted: AcceptedClientRegisteredAttach,
        io: LegIo,
        queue: LegOutboundQueue,
    ) -> Result<ClientRecoveryStarted, ClientRegisteredRecoveryBeginFailure> {
        let transport = match ClientLegTransport::from_parts(
            io,
            queue,
            ClientControllerTransportGate::mint(),
        ) {
            Ok(transport) => transport,
            Err(transport) => {
                return Err(ClientRegisteredRecoveryBeginFailure::transport(
                    accepted, transport,
                ));
            }
        };
        let source = match self.take_normal_supersession_source() {
            Ok(source) => source,
            Err(kind) => {
                return Err(ClientRegisteredRecoveryBeginFailure::controller(
                    accepted, transport, kind,
                ));
            }
        };

        let effect_bound = self.recovery_effect_bound;
        let current = &mut self.current;
        let retired = &mut self.retired;
        let phase = &mut self.phase;
        let current_liveness = &mut self.current_liveness;
        let session_id = self
            .supervisor
            .session_id_for_client_controller(&ClientControllerObservedApplyGate::mint());
        let pending_replacement_application_ack = &mut self.pending_replacement_application_ack;
        match accepted.install_with_controller_source(
            &mut self.supervisor,
            transport,
            effect_bound,
            ClientControllerInstallGate::mint(),
            source,
            PreparedNormalClientSupersession::prepare_recovery_reset_rebinds,
            |source, authority, transport, recovery, reservation| {
                let supersession_manifest = recovery.supersession_manifest();
                *retired = Some(source.into_retired_slot(supersession_manifest));
                Self::publish_replacement_recovery(
                    current,
                    phase,
                    current_liveness,
                    pending_replacement_application_ack,
                    session_id,
                    authority,
                    transport,
                    recovery,
                    reservation,
                )
            },
        ) {
            Ok(started) => Ok(started),
            Err((failure, source)) => {
                source.restore(current, current_liveness);
                Err(ClientRegisteredRecoveryBeginFailure::install(failure))
            }
        }
    }

    /// Installs ordinary exact-next registered B only after the exact current
    /// terminal has made the reducer Legless. Unlike generation catch-up, B
    /// carries a normal `ATTACH_ACCEPTED`; the terminal-bound predecessor is
    /// nevertheless moved through the same one-way retirement source.
    #[allow(
        clippy::result_large_err,
        reason = "pre-commit rejection returns accepted B plus exact endpoint and sole queue"
    )]
    pub(crate) fn begin_registered_post_loss_replacement(
        &mut self,
        accepted: AcceptedClientRegisteredAttach,
        io: LegIo,
        queue: LegOutboundQueue,
    ) -> Result<ClientRecoveryStarted, ClientRegisteredRecoveryBeginFailure> {
        let transport = match ClientLegTransport::from_parts(
            io,
            queue,
            ClientControllerTransportGate::mint(),
        ) {
            Ok(transport) => transport,
            Err(transport) => {
                return Err(ClientRegisteredRecoveryBeginFailure::transport(
                    accepted, transport,
                ));
            }
        };
        let source = match self.take_post_loss_retirement_source() {
            Ok(source) => source,
            Err(kind) => {
                return Err(ClientRegisteredRecoveryBeginFailure::controller(
                    accepted, transport, kind,
                ));
            }
        };

        let effect_bound = self.recovery_effect_bound;
        let current = &mut self.current;
        let retired = &mut self.retired;
        let phase = &mut self.phase;
        let current_liveness = &mut self.current_liveness;
        let session_id = self
            .supervisor
            .session_id_for_client_controller(&ClientControllerObservedApplyGate::mint());
        let pending_replacement_application_ack = &mut self.pending_replacement_application_ack;
        match accepted.install_with_controller_source(
            &mut self.supervisor,
            transport,
            effect_bound,
            ClientControllerInstallGate::mint(),
            source,
            |_, _| Ok(()),
            |source, authority, transport, recovery, reservation| {
                *retired = Some(source.into_retired_slot());
                Self::publish_replacement_recovery(
                    current,
                    phase,
                    current_liveness,
                    pending_replacement_application_ack,
                    session_id,
                    authority,
                    transport,
                    recovery,
                    reservation,
                )
            },
        ) {
            Ok(started) => Ok(started),
            Err((failure, source)) => {
                *phase = source.into_phase();
                Err(ClientRegisteredRecoveryBeginFailure::install(failure))
            }
        }
    }

    /// Installs an opaque catch-up D only after the exact terminal/discard of
    /// current A has made the reducer Legless. A remains controller-owned in
    /// `RecoveryDiscarded` until D commits, then moves once into the retired
    /// receive slot while D enters the normal recovery cursor.
    #[allow(
        clippy::result_large_err,
        reason = "pre-commit rejection returns accepted D plus exact endpoint and sole queue"
    )]
    pub(crate) fn begin_registered_catch_up_replacement(
        &mut self,
        accepted: AcceptedClientRegisteredCatchUp,
        io: LegIo,
        queue: LegOutboundQueue,
    ) -> Result<ClientRecoveryStarted, ClientRegisteredCatchUpRecoveryBeginFailure> {
        let transport = match ClientLegTransport::from_parts(
            io,
            queue,
            ClientControllerTransportGate::mint(),
        ) {
            Ok(transport) => transport,
            Err(transport) => {
                return Err(ClientRegisteredCatchUpRecoveryBeginFailure::transport(
                    accepted, transport,
                ));
            }
        };
        let source = match self.take_post_loss_retirement_source() {
            Ok(source) => source,
            Err(kind) => {
                return Err(ClientRegisteredCatchUpRecoveryBeginFailure::controller(
                    accepted, transport, kind,
                ));
            }
        };

        let effect_bound = self.recovery_effect_bound;
        let current = &mut self.current;
        let retired = &mut self.retired;
        let phase = &mut self.phase;
        let current_liveness = &mut self.current_liveness;
        let session_id = self
            .supervisor
            .session_id_for_client_controller(&ClientControllerObservedApplyGate::mint());
        let pending_replacement_application_ack = &mut self.pending_replacement_application_ack;
        match accepted.install_with_controller(
            &mut self.supervisor,
            transport,
            effect_bound,
            ClientControllerInstallGate::mint(),
            source,
            |source, authority, transport, recovery, reservation| {
                *retired = Some(source.into_retired_slot());
                Self::publish_replacement_recovery(
                    current,
                    phase,
                    current_liveness,
                    pending_replacement_application_ack,
                    session_id,
                    authority,
                    transport,
                    recovery,
                    reservation,
                )
            },
        ) {
            Ok(started) => Ok(started),
            Err((failure, source)) => {
                *phase = source.into_phase();
                Err(ClientRegisteredCatchUpRecoveryBeginFailure::install(
                    failure,
                ))
            }
        }
    }

    /// Binds an exact current terminal only while B recovery ownership is
    /// installed or retained as an Active delivery tail. Wrong facts return
    /// untouched, and even an exact fact does not discard ownership or mutate
    /// the reducer yet.
    pub(crate) fn bind_current_terminal(
        &mut self,
        terminal: ExactLegTerminal,
    ) -> Result<ClientCurrentTerminalBound, ClientCurrentTerminalBindFailure> {
        let phase = std::mem::replace(&mut self.phase, ClientControllerPhase::Legless);
        let recovery = match phase {
            ClientControllerPhase::Recovering(recovery) => {
                TerminatingCurrentRecoveryOwnership::Installed(recovery)
            }
            ClientControllerPhase::Active {
                recovery_tail: Some(tail),
            } => TerminatingCurrentRecoveryOwnership::Tail(tail),
            ClientControllerPhase::Active {
                recovery_tail: None,
            } => TerminatingCurrentRecoveryOwnership::NoRecovery,
            phase => {
                self.phase = phase;
                return Err(ClientCurrentTerminalBindFailure::new(
                    terminal,
                    ClientCurrentTerminalBindErrorKind::NoCurrentRecovery,
                ));
            }
        };
        let Some(current) = self.current.take() else {
            self.phase = recovery.into_controller_phase();
            return Err(ClientCurrentTerminalBindFailure::new(
                terminal,
                ClientCurrentTerminalBindErrorKind::MissingCurrent,
            ));
        };
        let ActiveClientTransport {
            authority,
            transport,
        } = current;
        let generation = authority.generation();
        let observation_settlement = match self.prepare_current_observation_settlement(
            &authority,
            ClientControllerObservationSettlementSource::CurrentTerminal,
        ) {
            Ok(settlement) => settlement,
            Err(kind) => {
                self.current = Some(ActiveClientTransport {
                    authority,
                    transport,
                });
                self.phase = recovery.into_controller_phase();
                return Err(ClientCurrentTerminalBindFailure::new(
                    terminal,
                    ClientCurrentTerminalBindErrorKind::from_observation_settlement_prepare(kind),
                ));
            }
        };
        let (authority, pending_loss) = match authority.bind_terminal_retaining_retired(terminal) {
            Ok(bound) => bound,
            Err(mismatch) => {
                let (authority, terminal) = mismatch.into_parts();
                self.current = Some(ActiveClientTransport {
                    authority,
                    transport,
                });
                self.phase = recovery.into_controller_phase();
                return Err(ClientCurrentTerminalBindFailure::new(
                    terminal,
                    ClientCurrentTerminalBindErrorKind::WrongLegTerminal,
                ));
            }
        };
        let reason = authority.reason();
        debug_assert_eq!(pending_loss.reason(), reason);
        self.begin_current_observation_settlement(observation_settlement);
        self.phase = ClientControllerPhase::TerminatingCurrent(
            TerminatingCurrentClientTransport::RecoveryPending {
                authority,
                transport,
                pending_loss,
                recovery,
            },
        );
        Ok(ClientCurrentTerminalBound { generation, reason })
    }

    /// Explicit second phase for a terminal B. Its installed reservation or
    /// Active delivery tail is discarded first; only then is the single
    /// PendingClientLegLoss consumed by this controller's supervisor.
    pub(crate) fn discard_terminating_current_recovery(
        &mut self,
    ) -> Result<ClientCurrentTerminalDiscarded, ClientCurrentTerminalDiscardErrorKind> {
        let phase = std::mem::replace(&mut self.phase, ClientControllerPhase::Legless);
        let ClientControllerPhase::TerminatingCurrent(terminating) = phase else {
            self.phase = phase;
            return Err(ClientCurrentTerminalDiscardErrorKind::NotTerminatingCurrent);
        };
        let TerminatingCurrentClientTransport::RecoveryPending {
            authority,
            mut transport,
            pending_loss,
            recovery,
        } = terminating
        else {
            self.phase = ClientControllerPhase::TerminatingCurrent(terminating);
            return Err(ClientCurrentTerminalDiscardErrorKind::AlreadyDiscarded);
        };
        let generation = authority.generation();
        match recovery {
            TerminatingCurrentRecoveryOwnership::Installed(recovery) => {
                let ClientRecoveryState {
                    generation: recovery_generation,
                    effects,
                    pending,
                    reservation,
                    drive_ready,
                } = recovery;
                if let Err(reservation) = transport
                    .discard_installed_client_recovery_after_terminal(
                        reservation,
                        &ClientRecoveryTerminalGate::mint(),
                    )
                {
                    self.phase = ClientControllerPhase::TerminatingCurrent(
                        TerminatingCurrentClientTransport::RecoveryPending {
                            authority,
                            transport,
                            pending_loss,
                            recovery: TerminatingCurrentRecoveryOwnership::Installed(
                                ClientRecoveryState {
                                    generation: recovery_generation,
                                    effects,
                                    pending,
                                    reservation,
                                    drive_ready,
                                },
                            ),
                        },
                    );
                    return Err(ClientCurrentTerminalDiscardErrorKind::RecoveryCleanupRejected);
                }
            }
            TerminatingCurrentRecoveryOwnership::Tail(tail) => {
                if let Err(tail) = transport.discard_client_recovery_tail_after_terminal(
                    tail,
                    &ClientRecoveryTerminalGate::mint(),
                ) {
                    self.phase = ClientControllerPhase::TerminatingCurrent(
                        TerminatingCurrentClientTransport::RecoveryPending {
                            authority,
                            transport,
                            pending_loss,
                            recovery: TerminatingCurrentRecoveryOwnership::Tail(tail),
                        },
                    );
                    return Err(ClientCurrentTerminalDiscardErrorKind::RecoveryCleanupRejected);
                }
            }
            TerminatingCurrentRecoveryOwnership::NoRecovery => {}
        }

        let reason = authority.reason();
        debug_assert_eq!(pending_loss.reason(), reason);
        let transport = transport.into_retired();
        let applied = self.supervisor.apply_event(pending_loss.into_event());
        self.phase = ClientControllerPhase::TerminatingCurrent(
            TerminatingCurrentClientTransport::RecoveryDiscarded {
                authority,
                transport,
                generation,
                reason,
            },
        );
        let effects = match applied {
            Ok(effects) => effects,
            Err(_error) => {
                return Err(ClientCurrentTerminalDiscardErrorKind::SupervisorRejected);
            }
        };
        if effects.as_slice() != [SessionEffect::ResumeGraceStarted { generation }] {
            return Err(ClientCurrentTerminalDiscardErrorKind::EffectInvariant);
        }
        Ok(ClientCurrentTerminalDiscarded {
            generation,
            reason,
            effects,
        })
    }

    /// Whole-session shutdown consumes the joined current actor's exact
    /// terminal without synthesizing a recoverable `LegLost`. Factory
    /// admission is already sealed and no replacement can observe the pending
    /// loss, so the terminal, pending-loss authority, transport, and active
    /// controller slot are retired as one synchronous transaction.
    pub(super) fn clear_current_after_shutdown_join(
        &mut self,
        runtime: &PhysicalLegRuntimeIncarnation,
        terminal: Option<ExactLegTerminal>,
        fatal_tail: Option<&QuinnFatalRecoveryTailShutdownGate>,
        gate: &QuinnLegCompositionGate,
    ) -> Result<ClientCurrentShutdownCleared, ClientCurrentShutdownClearFailure> {
        if !self.external_flow_admission_is_closed() {
            return Err(ClientCurrentShutdownClearFailure::new(
                terminal,
                ClientCurrentShutdownClearErrorKind::FactoryAdmissionOpen,
            ));
        }
        let recovery_tail_pending = match &self.phase {
            ClientControllerPhase::Active { recovery_tail } => recovery_tail.is_some(),
            ClientControllerPhase::Recovering(_)
            | ClientControllerPhase::TerminatingCurrent(_)
            | ClientControllerPhase::Legless => {
                return Err(ClientCurrentShutdownClearFailure::new(
                    terminal,
                    ClientCurrentShutdownClearErrorKind::ControllerNotPristine,
                ));
            }
        };
        if recovery_tail_pending && fatal_tail.is_none() {
            return Err(ClientCurrentShutdownClearFailure::new(
                terminal,
                ClientCurrentShutdownClearErrorKind::ControllerNotPristine,
            ));
        }
        let Some(current) = self.current.take() else {
            return Err(ClientCurrentShutdownClearFailure::new(
                terminal,
                ClientCurrentShutdownClearErrorKind::MissingCurrent,
            ));
        };
        let ActiveClientTransport {
            authority,
            mut transport,
        } = current;
        if !transport.belongs_to_physical_actor(runtime, gate) {
            self.current = Some(ActiveClientTransport {
                authority,
                transport,
            });
            return Err(ClientCurrentShutdownClearFailure::new(
                terminal,
                ClientCurrentShutdownClearErrorKind::WrongPhysicalActor,
            ));
        }
        let Some(terminal) = terminal else {
            self.current = Some(ActiveClientTransport {
                authority,
                transport,
            });
            return Err(ClientCurrentShutdownClearFailure::new(
                None,
                ClientCurrentShutdownClearErrorKind::MissingTerminal,
            ));
        };
        if !authority.matches_terminal(&terminal) {
            self.current = Some(ActiveClientTransport {
                authority,
                transport,
            });
            return Err(ClientCurrentShutdownClearFailure::new(
                Some(terminal),
                ClientCurrentShutdownClearErrorKind::WrongTerminal,
            ));
        }
        let observation_settlement = match self.prepare_current_observation_settlement(
            &authority,
            ClientControllerObservationSettlementSource::JoinedShutdown,
        ) {
            Ok(settlement) => settlement,
            Err(kind) => {
                self.current = Some(ActiveClientTransport {
                    authority,
                    transport,
                });
                return Err(ClientCurrentShutdownClearFailure::new(
                    Some(terminal),
                    ClientCurrentShutdownClearErrorKind::from_observation_settlement_prepare(kind),
                ));
            }
        };
        let recovery_tail = match &mut self.phase {
            ClientControllerPhase::Active { recovery_tail } => recovery_tail.take(),
            ClientControllerPhase::Recovering(_)
            | ClientControllerPhase::TerminatingCurrent(_)
            | ClientControllerPhase::Legless => None,
        };
        if let Some(recovery_tail) = recovery_tail
            && let Err(recovery_tail) = transport.discard_client_recovery_tail_after_terminal(
                recovery_tail,
                &ClientRecoveryTerminalGate::mint(),
            )
        {
            let ClientControllerPhase::Active {
                recovery_tail: slot,
            } = &mut self.phase
            else {
                return Err(ClientCurrentShutdownClearFailure::new(
                    Some(terminal),
                    ClientCurrentShutdownClearErrorKind::ControllerNotPristine,
                ));
            };
            *slot = Some(recovery_tail);
            self.current = Some(ActiveClientTransport {
                authority,
                transport,
            });
            return Err(ClientCurrentShutdownClearFailure::new(
                Some(terminal),
                ClientCurrentShutdownClearErrorKind::RecoveryCleanupRejected,
            ));
        }
        let (terminal_bound, pending_loss) =
            match authority.bind_terminal_retaining_retired(terminal) {
                Ok(bound) => bound,
                Err(mismatch) => {
                    let (authority, terminal) = mismatch.into_parts();
                    self.current = Some(ActiveClientTransport {
                        authority,
                        transport,
                    });
                    return Err(ClientCurrentShutdownClearFailure::new(
                        Some(terminal),
                        ClientCurrentShutdownClearErrorKind::WrongTerminal,
                    ));
                }
            };
        let generation = terminal_bound.generation();
        let reason = terminal_bound.reason();
        debug_assert_eq!(pending_loss.reason(), reason);
        self.begin_current_observation_settlement(observation_settlement);
        drop((terminal_bound, pending_loss, transport));
        self.phase = ClientControllerPhase::Legless;
        Ok(ClientCurrentShutdownCleared { generation, reason })
    }

    /// Shutdown-only structural retirement for a current leg whose exact
    /// hard terminal was already bound and applied before physical join. The
    /// controller keeps the terminal-bound authority and retired transport in
    /// `RecoveryDiscarded` until the joined actor proves the same physical
    /// incarnation and every probe/application observation has settled.
    pub(super) fn clear_terminal_bound_current_after_shutdown_join(
        &mut self,
        runtime: &PhysicalLegRuntimeIncarnation,
        gate: &QuinnLegCompositionGate,
    ) -> Result<ClientCurrentShutdownCleared, ClientCurrentShutdownClearErrorKind> {
        if !self.external_flow_admission_is_closed() {
            return Err(ClientCurrentShutdownClearErrorKind::FactoryAdmissionOpen);
        }
        if self.pending_observation_settlement.is_some() {
            return Err(ClientCurrentShutdownClearErrorKind::ObservationSettlementPending);
        }
        if self.current.is_some()
            || self.pending_replacement_application_ack.is_some()
            || !matches!(self.current_liveness.phase, ClientCurrentProbePhase::Idle)
        {
            return Err(ClientCurrentShutdownClearErrorKind::ControllerNotPristine);
        }
        let snapshot = self.supervisor.snapshot();
        let phase = std::mem::replace(&mut self.phase, ClientControllerPhase::Legless);
        let ClientControllerPhase::TerminatingCurrent(
            TerminatingCurrentClientTransport::RecoveryDiscarded {
                authority,
                transport,
                generation,
                reason,
            },
        ) = phase
        else {
            self.phase = phase;
            return Err(ClientCurrentShutdownClearErrorKind::MissingTerminatingCurrent);
        };
        if !transport.belongs_to_physical_actor(runtime, gate) {
            self.phase = ClientControllerPhase::TerminatingCurrent(
                TerminatingCurrentClientTransport::RecoveryDiscarded {
                    authority,
                    transport,
                    generation,
                    reason,
                },
            );
            return Err(ClientCurrentShutdownClearErrorKind::WrongPhysicalActor);
        }
        if authority.generation() != generation
            || snapshot.session.phase() != crate::resumable::SessionPhase::Legless
            || snapshot.session.generation() != generation
        {
            self.phase = ClientControllerPhase::TerminatingCurrent(
                TerminatingCurrentClientTransport::RecoveryDiscarded {
                    authority,
                    transport,
                    generation,
                    reason,
                },
            );
            return Err(ClientCurrentShutdownClearErrorKind::SessionNotLegless);
        }
        drop((authority, transport));
        Ok(ClientCurrentShutdownCleared { generation, reason })
    }

    /// Consumes at most one recovery effect. No queue argument exists: the
    /// cursor and exact B/D FIFO are both owned by this controller phase.
    pub(crate) fn drive_recovery_one(&mut self) -> ClientRecoveryDrive {
        let phase = std::mem::replace(&mut self.phase, ClientControllerPhase::Legless);
        let ClientControllerPhase::Recovering(mut recovery) = phase else {
            self.phase = phase;
            return ClientRecoveryDrive::NotRecovering;
        };
        if !recovery.drive_ready {
            self.phase = ClientControllerPhase::Recovering(recovery);
            return ClientRecoveryDrive::AwaitingFlush;
        }
        let Some(current) = self.current.as_mut() else {
            self.phase = ClientControllerPhase::Recovering(recovery);
            return ClientRecoveryDrive::InternalMismatch;
        };
        if !current
            .transport
            .installed_recovery_belongs_here(&recovery.reservation)
        {
            self.phase = ClientControllerPhase::Recovering(recovery);
            return ClientRecoveryDrive::InternalMismatch;
        }

        let effect = recovery.pending.take().or_else(|| recovery.effects.next());
        match effect {
            Some(PreparedClientRecoveryEffect::LegActivated { generation }) => {
                if generation != recovery.generation {
                    recovery.pending =
                        Some(PreparedClientRecoveryEffect::LegActivated { generation });
                    self.phase = ClientControllerPhase::Recovering(recovery);
                    return ClientRecoveryDrive::InternalMismatch;
                }
                self.phase = ClientControllerPhase::Recovering(recovery);
                ClientRecoveryDrive::Progress(ClientRecoveryObservation::LegActivated {
                    generation,
                })
            }
            Some(PreparedClientRecoveryEffect::Transmit(frame)) => {
                match current
                    .transport
                    .push_client_recovery_frame(recovery.reservation, frame)
                {
                    Ok(reservation) => {
                        recovery.reservation = reservation;
                        if recovery.effects.len() == 0 && recovery.pending.is_none() {
                            let generation = recovery.generation;
                            match current
                                .transport
                                .finish_client_recovery_admission(recovery.reservation)
                            {
                                Ok(tail) => {
                                    self.phase = ClientControllerPhase::Active {
                                        recovery_tail: Some(tail),
                                    };
                                    ClientRecoveryDrive::AdmissionCompleted { generation }
                                }
                                Err(failure) => {
                                    recovery.reservation = failure.into_reservation();
                                    self.phase = ClientControllerPhase::Recovering(recovery);
                                    ClientRecoveryDrive::InternalMismatch
                                }
                            }
                        } else {
                            self.phase = ClientControllerPhase::Recovering(recovery);
                            ClientRecoveryDrive::Progress(ClientRecoveryObservation::FrameAdmitted)
                        }
                    }
                    Err(failure) => {
                        let kind = failure.kind();
                        let (reservation, frame) = failure.into_parts();
                        recovery.reservation = reservation;
                        recovery.pending = Some(PreparedClientRecoveryEffect::Transmit(frame));
                        if kind == ClientRecoveryEnqueueErrorKind::Pressure {
                            recovery.drive_ready = false;
                        }
                        self.phase = ClientControllerPhase::Recovering(recovery);
                        match kind {
                            ClientRecoveryEnqueueErrorKind::Pressure => {
                                ClientRecoveryDrive::Pressure
                            }
                            ClientRecoveryEnqueueErrorKind::EndpointOrQueueLost => {
                                ClientRecoveryDrive::EndpointOrQueueLost
                            }
                            ClientRecoveryEnqueueErrorKind::WrongQueue
                            | ClientRecoveryEnqueueErrorKind::GenerationMismatch
                            | ClientRecoveryEnqueueErrorKind::CapacityMismatch => {
                                ClientRecoveryDrive::InternalMismatch
                            }
                        }
                    }
                }
            }
            None => {
                let generation = recovery.generation;
                match current
                    .transport
                    .finish_client_recovery_admission(recovery.reservation)
                {
                    Ok(tail) => {
                        self.phase = ClientControllerPhase::Active {
                            recovery_tail: Some(tail),
                        };
                        ClientRecoveryDrive::AdmissionCompleted { generation }
                    }
                    Err(failure) => {
                        recovery.reservation = failure.into_reservation();
                        self.phase = ClientControllerPhase::Recovering(recovery);
                        ClientRecoveryDrive::InternalMismatch
                    }
                }
            }
        }
    }

    /// Sole outbound progress seam. A successful queue pop or completion of
    /// the preceding ordered delivery rearms one recovery turn after FIFO
    /// pressure; callers still observe only physical submissions.
    pub(crate) fn try_flush_current<T: EncodedLegTransport + ?Sized>(
        &mut self,
        transport: &mut T,
        now: SimTime,
    ) -> Result<Option<OutboundSubmission>, LegIoError> {
        let current = self
            .current
            .as_mut()
            .ok_or(LegIoError::OutboundQueueEndpointLost)?;
        let progress = current.transport.try_flush(transport, now)?;
        if progress.recovery_admission_ready()
            && let ClientControllerPhase::Recovering(recovery) = &mut self.phase
        {
            recovery.drive_ready = true;
        }
        Ok(progress.into_submission())
    }

    #[cfg(test)]
    pub(crate) fn receive_current(
        &mut self,
        delivery: EncodedDelivery,
        installed_features: FeatureSet,
    ) -> Result<ClientInboundDisposition, LegIoError> {
        self.receive_current_with_features(delivery, installed_features, SimTime::ZERO)
    }

    pub(super) fn receive_current_from_quinn(
        &mut self,
        delivery: EncodedDelivery,
        _gate: &QuinnLegCompositionGate,
        received_at: SimTime,
    ) -> Result<ClientInboundDisposition, LegIoError> {
        self.receive_current_with_features(delivery, self.installed_features, received_at)
    }

    fn receive_current_with_features(
        &mut self,
        delivery: EncodedDelivery,
        installed_features: FeatureSet,
        received_at: SimTime,
    ) -> Result<ClientInboundDisposition, LegIoError> {
        if matches!(
            self.phase,
            ClientControllerPhase::Recovering(_) | ClientControllerPhase::TerminatingCurrent(_)
        ) {
            return Ok(ClientInboundDisposition::Blocked(delivery));
        }
        let inbound = self
            .current
            .as_mut()
            .ok_or(LegIoError::OutboundQueueEndpointLost)?
            .transport
            .receive_classified_delivery(delivery, installed_features)?;
        let inbound = match inbound {
            BoundInbound::LegControl(inbound) => {
                return Ok(self.accept_current_probe_ack(inbound, received_at).map_or(
                    ClientInboundDisposition::Rejected,
                    |acked| ClientInboundDisposition::ProbeAcked {
                        generation: acked.generation,
                        probe_sequence: acked.probe_sequence,
                    },
                ));
            }
            inbound @ BoundInbound::Session(_) => inbound,
        };
        let current = self
            .current
            .as_mut()
            .ok_or(LegIoError::OutboundQueueEndpointLost)?;
        let event = match current.authority.accept_inbound(inbound) {
            Ok(event) => event,
            Err(_failure) => return Ok(ClientInboundDisposition::Rejected),
        };
        let current_generation = current.authority.generation();
        let observation_gate = ClientControllerObservedApplyGate::mint();
        Ok(
            match self
                .supervisor
                .apply_current_event_observed(event, &observation_gate)
            {
                Ok(observed) => {
                    let (effects, application_ack) = observed.into_parts();
                    let observation = application_ack.and_then(|application_ack| {
                        let supervisor_snapshot = self.supervisor.snapshot();
                        let supervisor_generation = supervisor_snapshot.session.generation();
                        let supervisor_session_id = self
                            .supervisor
                            .session_id_for_client_controller(&observation_gate);
                        let valid_current_ack = application_ack.session_id()
                            == supervisor_session_id
                            && application_ack.generation() == current_generation
                            && supervisor_generation == current_generation
                            && application_ack.previous_accepted()
                                < application_ack.next_accepted()
                            && application_ack.released_bytes() > 0;
                        if !valid_current_ack {
                            return None;
                        }
                        let pending_matches = self
                            .pending_replacement_application_ack
                            .as_ref()
                            .is_some_and(|pending| {
                                pending.session_id == application_ack.session_id()
                                    && pending.session_id == supervisor_session_id
                                    && pending.generation == current_generation
                                    && pending.replay.intersects_ack_delta(
                                        application_ack.flow_id(),
                                        application_ack.direction(),
                                        application_ack.previous_accepted(),
                                        application_ack.next_accepted(),
                                    )
                            });
                        if pending_matches {
                            self.pending_replacement_application_ack.take();
                            Some(ClientInboundObservation::ReplacementRecovered {
                                arrived_at: received_at,
                                session_id: application_ack.session_id(),
                                flow_id: application_ack.flow_id(),
                                generation: current_generation,
                                direction: application_ack.direction(),
                                previous_accepted: application_ack.previous_accepted(),
                                next_accepted: application_ack.next_accepted(),
                                released_bytes: application_ack.released_bytes(),
                                final_accepted: application_ack.final_accepted(),
                                session_has_outstanding_replay: application_ack
                                    .session_has_outstanding_replay(),
                            })
                        } else {
                            Some(ClientInboundObservation::ApplicationAckAdvanced {
                                arrived_at: received_at,
                                session_id: application_ack.session_id(),
                                flow_id: application_ack.flow_id(),
                                generation: current_generation,
                                direction: application_ack.direction(),
                                previous_accepted: application_ack.previous_accepted(),
                                next_accepted: application_ack.next_accepted(),
                                released_bytes: application_ack.released_bytes(),
                                final_accepted: application_ack.final_accepted(),
                                remaining_oldest: application_ack.remaining_oldest(),
                                session_has_outstanding_replay: application_ack
                                    .session_has_outstanding_replay(),
                            })
                        }
                    });
                    ClientInboundDisposition::Applied {
                        effects,
                        observation,
                    }
                }
                Err(error) => ClientInboundDisposition::ReducerRejected(error),
            },
        )
    }

    #[cfg(test)]
    pub(crate) fn receive_retired(
        &mut self,
        delivery: EncodedDelivery,
        installed_features: FeatureSet,
    ) -> Result<ClientInboundDisposition, LegIoError> {
        self.receive_retired_with_features(delivery, installed_features)
    }

    pub(super) fn receive_retired_from_quinn(
        &mut self,
        delivery: EncodedDelivery,
        _gate: &QuinnLegCompositionGate,
    ) -> Result<ClientInboundDisposition, LegIoError> {
        self.receive_retired_with_features(delivery, self.installed_features)
    }

    fn receive_retired_with_features(
        &mut self,
        delivery: EncodedDelivery,
        installed_features: FeatureSet,
    ) -> Result<ClientInboundDisposition, LegIoError> {
        let Some(retired) = self.retired.as_mut() else {
            return Ok(ClientInboundDisposition::Rejected);
        };
        let inbound = retired
            .transport
            .receive_classified_delivery(delivery, installed_features)?;
        Ok(match retired.authority.discard_late_inbound(inbound) {
            Ok(kind) => ClientInboundDisposition::Retired(kind),
            Err(_inbound) => ClientInboundDisposition::Rejected,
        })
    }

    pub(super) fn current_inbound_admission(&self) -> ClientCurrentInboundAdmission {
        if matches!(
            self.phase,
            ClientControllerPhase::Recovering(_) | ClientControllerPhase::TerminatingCurrent(_)
        ) {
            ClientCurrentInboundAdmission::PolicyBlocked
        } else if self.current.is_none() {
            ClientCurrentInboundAdmission::MissingCurrent
        } else {
            ClientCurrentInboundAdmission::Ready
        }
    }

    /// Read-only current-A adapter into the production exact-leg responder.
    /// The source is minted by the installed client authority; no caller can
    /// supply session, generation, epoch, features, seal, or endpoint values.
    pub(super) fn current_exact_leg_probe_responder(&self) -> Option<ExactLegProbeResponder> {
        self.current.as_ref().map(|current| {
            ExactLegProbeResponder::from_installed(current.authority.installed_leg_probe_source())
        })
    }

    /// Synchronous retired-only commit. It neither binds a pending leg loss
    /// nor calls the reducer, so a clean physical join cannot synthesize
    /// `LegLost` or disturb the current transport/recovery phase.
    pub(super) fn clear_retired_after_physical_join(
        &mut self,
        runtime: &PhysicalLegRuntimeIncarnation,
        terminal: Option<ExactLegTerminal>,
        gate: &QuinnLegCompositionGate,
    ) -> Result<RetiredClientCleared, RetiredClientClearFailure> {
        self.clear_retired_after_join(runtime, terminal, gate)
    }

    /// Shutdown-only counterpart to normal retired clear. The current joined
    /// transaction must first settle any replacement application-ACK observer;
    /// retired-A cleanup may never silently consume that independent owner.
    pub(super) fn clear_retired_after_shutdown_join(
        &mut self,
        runtime: &PhysicalLegRuntimeIncarnation,
        terminal: Option<ExactLegTerminal>,
        gate: &QuinnLegCompositionGate,
    ) -> Result<RetiredClientCleared, RetiredClientClearFailure> {
        self.clear_retired_after_join(runtime, terminal, gate)
    }

    fn clear_retired_after_join(
        &mut self,
        runtime: &PhysicalLegRuntimeIncarnation,
        terminal: Option<ExactLegTerminal>,
        gate: &QuinnLegCompositionGate,
    ) -> Result<RetiredClientCleared, RetiredClientClearFailure> {
        let Some(slot) = self.retired.take() else {
            return Err(RetiredClientClearFailure {
                terminal,
                kind: RetiredClientClearErrorKind::MissingRetired,
            });
        };
        if !slot.transport.belongs_to_physical_actor(runtime, gate) {
            self.retired = Some(slot);
            return Err(RetiredClientClearFailure {
                terminal,
                kind: RetiredClientClearErrorKind::WrongPhysicalActor,
            });
        }
        let next_supersession_history = match slot.supersession {
            Some(audit) => self.supersession_history.checked_record(audit).ok_or(()),
            None => Ok(self.supersession_history),
        };
        let Ok(next_supersession_history) = next_supersession_history else {
            self.retired = Some(slot);
            return Err(RetiredClientClearFailure {
                terminal,
                kind: RetiredClientClearErrorKind::SupersessionHistoryOverflow,
            });
        };
        let generation = slot.authority.generation();
        let terminal_bound = slot.authority.is_terminal_bound();
        if let Err(terminal) = slot.authority.clear_joined_terminal(terminal) {
            self.retired = Some(slot);
            return Err(RetiredClientClearFailure {
                terminal,
                kind: RetiredClientClearErrorKind::WrongTerminal,
            });
        }
        if terminal_bound && self.pending_replacement_application_ack.is_some() {
            self.retired = Some(slot);
            return Err(RetiredClientClearFailure {
                terminal: None,
                kind: RetiredClientClearErrorKind::ReplacementApplicationAckPending,
            });
        }
        self.supersession_history = next_supersession_history;
        drop(slot);
        Ok(RetiredClientCleared { generation })
    }

    pub(crate) fn current_inbound_route(&self) -> Option<WireRoute> {
        self.current
            .as_ref()
            .map(|current| current.transport.inbound_route())
    }

    pub(crate) fn current_outbound_route(&self) -> Option<WireRoute> {
        self.current
            .as_ref()
            .map(|current| current.transport.outbound_route())
    }

    pub(crate) fn retired_inbound_route(&self) -> Option<WireRoute> {
        self.retired
            .as_ref()
            .map(|retired| retired.transport.inbound_route())
    }

    pub(crate) fn recovery_budget_snapshot(&self) -> Option<ClientRecoveryBudgetSnapshot> {
        self.recovery_owned_current_transport()
            .map(ClientLegTransport::recovery_budget_snapshot)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum RetiredClientClearErrorKind {
    MissingRetired,
    WrongPhysicalActor,
    WrongTerminal,
    ReplacementApplicationAckPending,
    SupersessionHistoryOverflow,
}

pub(super) struct RetiredClientClearFailure {
    terminal: Option<ExactLegTerminal>,
    kind: RetiredClientClearErrorKind,
}

impl RetiredClientClearFailure {
    pub(super) const fn kind(&self) -> RetiredClientClearErrorKind {
        self.kind
    }

    pub(super) fn into_terminal(self) -> Option<ExactLegTerminal> {
        self.terminal
    }
}

impl fmt::Debug for RetiredClientClearFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RetiredClientClearFailure")
            .field("kind", &self.kind)
            .field("terminal", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct RetiredClientCleared {
    generation: LegGeneration,
}

impl RetiredClientCleared {
    pub(super) const fn generation(self) -> LegGeneration {
        self.generation
    }
}

enum ClientControllerBootstrapTransport {
    Raw { io: LegIo, queue: LegOutboundQueue },
    Checked(ClientLegTransport),
}

/// Ownership-preserving initial bootstrap rejection. No error can be
/// produced after the accepted queue transitions to Active.
pub(crate) struct ClientControllerBootstrapFailure {
    config: SessionConfig,
    accepted: AcceptedInitialAttach,
    transport: ClientControllerBootstrapTransport,
    kind: ClientControllerBootstrapErrorKind,
}

impl ClientControllerBootstrapFailure {
    fn raw(
        config: SessionConfig,
        accepted: AcceptedInitialAttach,
        io: LegIo,
        queue: LegOutboundQueue,
        kind: ClientControllerBootstrapErrorKind,
    ) -> Self {
        Self {
            config,
            accepted,
            transport: ClientControllerBootstrapTransport::Raw { io, queue },
            kind,
        }
    }

    fn transport(
        config: SessionConfig,
        accepted: AcceptedInitialAttach,
        failure: ClientLegTransportBuildFailure,
    ) -> Self {
        let (io, queue) = failure.into_parts();
        Self::raw(
            config,
            accepted,
            io,
            queue,
            ClientControllerBootstrapErrorKind::WrongTransport,
        )
    }

    fn activation(
        config: SessionConfig,
        failure: AcceptedInitialAttachActivationFailure,
        transport: ClientLegTransport,
    ) -> Self {
        let kind = match failure.kind() {
            InitialAttachActivationError::WrongQueue => {
                ClientControllerBootstrapErrorKind::WrongQueue
            }
            InitialAttachActivationError::EndpointOrQueueLost => {
                ClientControllerBootstrapErrorKind::EndpointLost
            }
            InitialAttachActivationError::RequestNotDelivered => {
                ClientControllerBootstrapErrorKind::RequestNotDelivered
            }
            InitialAttachActivationError::NotAwaitingInitialAttach => {
                ClientControllerBootstrapErrorKind::NotAwaitingInitialAttach
            }
        };
        Self {
            config,
            accepted: failure.into_accepted(),
            transport: ClientControllerBootstrapTransport::Checked(transport),
            kind,
        }
    }

    pub(crate) const fn kind(&self) -> ClientControllerBootstrapErrorKind {
        self.kind
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        SessionConfig,
        AcceptedInitialAttach,
        LegIo,
        LegOutboundQueue,
    ) {
        let (io, queue) = match self.transport {
            ClientControllerBootstrapTransport::Raw { io, queue } => (io, queue),
            ClientControllerBootstrapTransport::Checked(transport) => {
                transport.into_parts(ClientControllerTransportGate::mint())
            }
        };
        (self.config, self.accepted, io, queue)
    }
}

impl fmt::Debug for ClientControllerBootstrapFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ClientControllerBootstrapFailure([REDACTED])")
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ClientRecoveryStarted {
    pub(crate) generation: LegGeneration,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ClientRecoveryTailSettlement {
    Settled { generation: LegGeneration },
    AlreadySettled,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ClientRecoveryTailOwnership {
    Absent,
    Active,
    TerminalOwned,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ClientRecoveryTailSettlementBlock {
    NotActive,
    MissingCurrent,
    WrongPhysicalActor,
    QueueNotActive,
    WrongQueue,
    WrongEndpoint,
    WrongGeneration,
    EndpointLost,
    DeliveryPending,
    ReservationMismatch,
}

impl ClientRecoveryTailSettlementBlock {
    const fn from_leg_io(kind: ClientRecoveryTailSettlementErrorKind) -> Self {
        match kind {
            ClientRecoveryTailSettlementErrorKind::QueueNotActive => Self::QueueNotActive,
            ClientRecoveryTailSettlementErrorKind::WrongQueue => Self::WrongQueue,
            ClientRecoveryTailSettlementErrorKind::WrongEndpoint => Self::WrongEndpoint,
            ClientRecoveryTailSettlementErrorKind::WrongGeneration => Self::WrongGeneration,
            ClientRecoveryTailSettlementErrorKind::EndpointLost => Self::EndpointLost,
            ClientRecoveryTailSettlementErrorKind::DeliveryPending => Self::DeliveryPending,
            ClientRecoveryTailSettlementErrorKind::ReservationMismatch => Self::ReservationMismatch,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ClientRecoveryBeginErrorKind {
    NotActive,
    CatchUpRequiresDiscardedCurrent,
    PriorRecoveryDeliveryPending,
    PriorRecoveryTailUnsettled,
    RetiredCapacityExceeded,
    PriorReplacementApplicationAckPending,
    ObservationSettlementPending,
    CurrentProbeSettlementPending,
    SupersessionInventoryInvalid,
    SupersessionInvariant,
    WrongTransport,
    InstallRejected,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ClientRegisteredRecoveryFailureClass {
    Retryable,
    SessionFatal,
    StandbyTerminal,
    Invariant,
}

const fn classify_client_recovery_begin_error(
    kind: ClientRecoveryBeginErrorKind,
) -> ClientRegisteredRecoveryFailureClass {
    match kind {
        ClientRecoveryBeginErrorKind::PriorRecoveryDeliveryPending
        | ClientRecoveryBeginErrorKind::PriorRecoveryTailUnsettled
        | ClientRecoveryBeginErrorKind::RetiredCapacityExceeded
        | ClientRecoveryBeginErrorKind::PriorReplacementApplicationAckPending
        | ClientRecoveryBeginErrorKind::ObservationSettlementPending
        | ClientRecoveryBeginErrorKind::CurrentProbeSettlementPending => {
            ClientRegisteredRecoveryFailureClass::Retryable
        }
        ClientRecoveryBeginErrorKind::NotActive
        | ClientRecoveryBeginErrorKind::CatchUpRequiresDiscardedCurrent
        | ClientRecoveryBeginErrorKind::SupersessionInventoryInvalid
        | ClientRecoveryBeginErrorKind::SupersessionInvariant
        | ClientRecoveryBeginErrorKind::WrongTransport
        | ClientRecoveryBeginErrorKind::InstallRejected => {
            ClientRegisteredRecoveryFailureClass::Invariant
        }
    }
}

const fn classify_client_install_error(
    kind: AcceptedClientInstallErrorKind,
) -> ClientRegisteredRecoveryFailureClass {
    match kind {
        AcceptedClientInstallErrorKind::EndpointLost
        | AcceptedClientInstallErrorKind::RecoveryReservationTerminal => {
            ClientRegisteredRecoveryFailureClass::StandbyTerminal
        }
        AcceptedClientInstallErrorKind::SupervisorRejected => {
            ClientRegisteredRecoveryFailureClass::SessionFatal
        }
        AcceptedClientInstallErrorKind::RecoveryPlanRetryable
        | AcceptedClientInstallErrorKind::RecoveryReservationRetryable => {
            ClientRegisteredRecoveryFailureClass::Retryable
        }
        AcceptedClientInstallErrorKind::TerminalTransitionPoisoned
        | AcceptedClientInstallErrorKind::RecoveryPlanInvariant
        | AcceptedClientInstallErrorKind::RecoveryReservationInvariant => {
            ClientRegisteredRecoveryFailureClass::Invariant
        }
    }
}

pub(crate) struct ClientRegisteredRecoveryBeginFailure {
    ownership: ClientRegisteredRecoveryBeginOwnership,
    kind: ClientRecoveryBeginErrorKind,
}

enum ClientRegisteredRecoveryBeginOwnership {
    Transport {
        accepted: AcceptedClientRegisteredAttach,
        transport: ClientLegTransportBuildFailure,
    },
    Controller {
        accepted: AcceptedClientRegisteredAttach,
        transport: ClientLegTransport,
    },
    Install(AcceptedClientRegisteredInstallFailure),
}

impl ClientRegisteredRecoveryBeginFailure {
    fn transport(
        accepted: AcceptedClientRegisteredAttach,
        transport: ClientLegTransportBuildFailure,
    ) -> Self {
        Self {
            ownership: ClientRegisteredRecoveryBeginOwnership::Transport {
                accepted,
                transport,
            },
            kind: ClientRecoveryBeginErrorKind::WrongTransport,
        }
    }

    fn controller(
        accepted: AcceptedClientRegisteredAttach,
        transport: ClientLegTransport,
        kind: ClientRecoveryBeginErrorKind,
    ) -> Self {
        Self {
            ownership: ClientRegisteredRecoveryBeginOwnership::Controller {
                accepted,
                transport,
            },
            kind,
        }
    }

    fn install(failure: AcceptedClientRegisteredInstallFailure) -> Self {
        Self {
            ownership: ClientRegisteredRecoveryBeginOwnership::Install(failure),
            kind: ClientRecoveryBeginErrorKind::InstallRejected,
        }
    }

    pub(crate) const fn kind(&self) -> ClientRecoveryBeginErrorKind {
        self.kind
    }

    pub(crate) fn failure_class(&self) -> ClientRegisteredRecoveryFailureClass {
        match &self.ownership {
            ClientRegisteredRecoveryBeginOwnership::Transport { .. } => {
                ClientRegisteredRecoveryFailureClass::Invariant
            }
            ClientRegisteredRecoveryBeginOwnership::Controller { .. } => {
                classify_client_recovery_begin_error(self.kind)
            }
            ClientRegisteredRecoveryBeginOwnership::Install(failure) => {
                classify_client_install_error(failure.kind())
            }
        }
    }

    pub(crate) fn into_parts(self) -> (AcceptedClientRegisteredAttach, LegIo, LegOutboundQueue) {
        match self.ownership {
            ClientRegisteredRecoveryBeginOwnership::Transport {
                accepted,
                transport,
            } => {
                let (io, queue) = transport.into_parts();
                (accepted, io, queue)
            }
            ClientRegisteredRecoveryBeginOwnership::Controller {
                accepted,
                transport,
            } => {
                let (io, queue) = transport.into_parts(ClientControllerTransportGate::mint());
                (accepted, io, queue)
            }
            ClientRegisteredRecoveryBeginOwnership::Install(failure) => {
                let (accepted, transport) = failure.into_parts();
                let (io, queue) = transport.into_parts(ClientControllerTransportGate::mint());
                (accepted, io, queue)
            }
        }
    }
}

impl fmt::Debug for ClientRegisteredRecoveryBeginFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ClientRegisteredRecoveryBeginFailure([REDACTED])")
    }
}

pub(crate) struct ClientRegisteredCatchUpRecoveryBeginFailure {
    ownership: ClientRegisteredCatchUpRecoveryBeginOwnership,
    kind: ClientRecoveryBeginErrorKind,
}

enum ClientRegisteredCatchUpRecoveryBeginOwnership {
    Transport {
        accepted: AcceptedClientRegisteredCatchUp,
        transport: ClientLegTransportBuildFailure,
    },
    Controller {
        accepted: AcceptedClientRegisteredCatchUp,
        transport: ClientLegTransport,
    },
    Install(AcceptedClientRegisteredCatchUpInstallFailure),
}

impl ClientRegisteredCatchUpRecoveryBeginFailure {
    fn transport(
        accepted: AcceptedClientRegisteredCatchUp,
        transport: ClientLegTransportBuildFailure,
    ) -> Self {
        Self {
            ownership: ClientRegisteredCatchUpRecoveryBeginOwnership::Transport {
                accepted,
                transport,
            },
            kind: ClientRecoveryBeginErrorKind::WrongTransport,
        }
    }

    fn controller(
        accepted: AcceptedClientRegisteredCatchUp,
        transport: ClientLegTransport,
        kind: ClientRecoveryBeginErrorKind,
    ) -> Self {
        Self {
            ownership: ClientRegisteredCatchUpRecoveryBeginOwnership::Controller {
                accepted,
                transport,
            },
            kind,
        }
    }

    fn install(failure: AcceptedClientRegisteredCatchUpInstallFailure) -> Self {
        Self {
            ownership: ClientRegisteredCatchUpRecoveryBeginOwnership::Install(failure),
            kind: ClientRecoveryBeginErrorKind::InstallRejected,
        }
    }

    pub(crate) const fn kind(&self) -> ClientRecoveryBeginErrorKind {
        self.kind
    }

    pub(crate) fn into_parts(self) -> (AcceptedClientRegisteredCatchUp, LegIo, LegOutboundQueue) {
        match self.ownership {
            ClientRegisteredCatchUpRecoveryBeginOwnership::Transport {
                accepted,
                transport,
            } => {
                let (io, queue) = transport.into_parts();
                (accepted, io, queue)
            }
            ClientRegisteredCatchUpRecoveryBeginOwnership::Controller {
                accepted,
                transport,
            } => {
                let (io, queue) = transport.into_parts(ClientControllerTransportGate::mint());
                (accepted, io, queue)
            }
            ClientRegisteredCatchUpRecoveryBeginOwnership::Install(failure) => {
                let (accepted, transport) = failure.into_parts();
                let (io, queue) = transport.into_parts(ClientControllerTransportGate::mint());
                (accepted, io, queue)
            }
        }
    }
}

impl fmt::Debug for ClientRegisteredCatchUpRecoveryBeginFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ClientRegisteredCatchUpRecoveryBeginFailure([REDACTED])")
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ClientCurrentTerminalBound {
    generation: LegGeneration,
    reason: LegTransportTerminalReason,
}

impl ClientCurrentTerminalBound {
    pub(crate) const fn generation(&self) -> LegGeneration {
        self.generation
    }

    pub(crate) const fn reason(&self) -> LegTransportTerminalReason {
        self.reason
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ClientCurrentTerminalBindErrorKind {
    NoCurrentRecovery,
    MissingCurrent,
    WrongLegTerminal,
    ObservationSettlementPending,
    ObservationLivenessMismatch,
    ObservationApplicationAckMismatch,
    ObservationHistoryOverflow,
}

impl ClientCurrentTerminalBindErrorKind {
    const fn from_observation_settlement_prepare(
        kind: ClientControllerObservationSettlementPrepareErrorKind,
    ) -> Self {
        match kind {
            ClientControllerObservationSettlementPrepareErrorKind::PendingSettlement => {
                Self::ObservationSettlementPending
            }
            ClientControllerObservationSettlementPrepareErrorKind::LivenessMismatch => {
                Self::ObservationLivenessMismatch
            }
            ClientControllerObservationSettlementPrepareErrorKind::ApplicationAckMismatch => {
                Self::ObservationApplicationAckMismatch
            }
            ClientControllerObservationSettlementPrepareErrorKind::HistoryOverflow => {
                Self::ObservationHistoryOverflow
            }
        }
    }
}

pub(crate) struct ClientCurrentTerminalBindFailure {
    terminal: ExactLegTerminal,
    kind: ClientCurrentTerminalBindErrorKind,
}

impl ClientCurrentTerminalBindFailure {
    fn new(terminal: ExactLegTerminal, kind: ClientCurrentTerminalBindErrorKind) -> Self {
        Self { terminal, kind }
    }

    pub(crate) const fn kind(&self) -> ClientCurrentTerminalBindErrorKind {
        self.kind
    }

    pub(crate) fn into_terminal(self) -> ExactLegTerminal {
        self.terminal
    }
}

impl fmt::Debug for ClientCurrentTerminalBindFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ClientCurrentTerminalBindFailure([REDACTED])")
    }
}

#[derive(Debug)]
pub(crate) struct ClientCurrentTerminalDiscarded {
    generation: LegGeneration,
    reason: LegTransportTerminalReason,
    effects: Vec<SessionEffect>,
}

impl ClientCurrentTerminalDiscarded {
    pub(crate) const fn generation(&self) -> LegGeneration {
        self.generation
    }

    pub(crate) const fn reason(&self) -> LegTransportTerminalReason {
        self.reason
    }

    pub(crate) fn effects(&self) -> &[SessionEffect] {
        &self.effects
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ClientCurrentTerminalDiscardErrorKind {
    NotTerminatingCurrent,
    AlreadyDiscarded,
    RecoveryCleanupRejected,
    SupervisorRejected,
    EffectInvariant,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct ClientCurrentShutdownCleared {
    generation: LegGeneration,
    reason: LegTransportTerminalReason,
}

impl ClientCurrentShutdownCleared {
    pub(super) const fn generation(self) -> LegGeneration {
        self.generation
    }

    pub(super) const fn reason(self) -> LegTransportTerminalReason {
        self.reason
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ClientCurrentShutdownClearErrorKind {
    FactoryAdmissionOpen,
    ControllerNotPristine,
    MissingCurrent,
    WrongPhysicalActor,
    MissingTerminal,
    WrongTerminal,
    RecoveryCleanupRejected,
    ObservationSettlementPending,
    ObservationLivenessMismatch,
    ObservationApplicationAckMismatch,
    ObservationHistoryOverflow,
    MissingTerminatingCurrent,
    SessionNotLegless,
}

impl ClientCurrentShutdownClearErrorKind {
    const fn from_observation_settlement_prepare(
        kind: ClientControllerObservationSettlementPrepareErrorKind,
    ) -> Self {
        match kind {
            ClientControllerObservationSettlementPrepareErrorKind::PendingSettlement => {
                Self::ObservationSettlementPending
            }
            ClientControllerObservationSettlementPrepareErrorKind::LivenessMismatch => {
                Self::ObservationLivenessMismatch
            }
            ClientControllerObservationSettlementPrepareErrorKind::ApplicationAckMismatch => {
                Self::ObservationApplicationAckMismatch
            }
            ClientControllerObservationSettlementPrepareErrorKind::HistoryOverflow => {
                Self::ObservationHistoryOverflow
            }
        }
    }
}

pub(super) struct ClientCurrentShutdownClearFailure {
    terminal: Option<ExactLegTerminal>,
    kind: ClientCurrentShutdownClearErrorKind,
}

impl ClientCurrentShutdownClearFailure {
    fn new(terminal: Option<ExactLegTerminal>, kind: ClientCurrentShutdownClearErrorKind) -> Self {
        Self { terminal, kind }
    }

    pub(super) const fn kind(&self) -> ClientCurrentShutdownClearErrorKind {
        self.kind
    }

    pub(super) fn into_terminal(self) -> Option<ExactLegTerminal> {
        self.terminal
    }
}

impl fmt::Debug for ClientCurrentShutdownClearFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClientCurrentShutdownClearFailure")
            .field("kind", &self.kind)
            .field("terminal", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ClientRecoveryObservation {
    LegActivated { generation: LegGeneration },
    FrameAdmitted,
}

#[derive(Debug)]
pub(crate) enum ClientRecoveryDrive {
    Progress(ClientRecoveryObservation),
    AdmissionCompleted { generation: LegGeneration },
    Pressure,
    AwaitingFlush,
    EndpointOrQueueLost,
    InternalMismatch,
    NotRecovering,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ClientInboundObservation {
    ReplacementRecovered {
        arrived_at: SimTime,
        session_id: SessionId,
        flow_id: SessionFlowId,
        generation: LegGeneration,
        direction: Direction,
        previous_accepted: ByteOffset,
        next_accepted: ByteOffset,
        released_bytes: usize,
        final_accepted: bool,
        session_has_outstanding_replay: bool,
    },
    ApplicationAckAdvanced {
        arrived_at: SimTime,
        session_id: SessionId,
        flow_id: SessionFlowId,
        generation: LegGeneration,
        direction: Direction,
        previous_accepted: ByteOffset,
        next_accepted: ByteOffset,
        released_bytes: usize,
        final_accepted: bool,
        remaining_oldest: Option<ByteOffset>,
        session_has_outstanding_replay: bool,
    },
}

pub(crate) enum ClientInboundDisposition {
    Retired(RetiredClientInboundKind),
    Blocked(EncodedDelivery),
    ProbeAcked {
        generation: LegGeneration,
        probe_sequence: ProbeSequence,
    },
    Applied {
        effects: Vec<SessionEffect>,
        observation: Option<ClientInboundObservation>,
    },
    Rejected,
    ReducerRejected(SessionSupervisorError),
}

pub(super) struct ClientCurrentProbeQueued {
    identity: Arc<ClientCurrentProbeIdentity>,
}

impl ClientCurrentProbeQueued {
    pub(super) fn generation(&self) -> LegGeneration {
        self.identity.correlation.generation
    }

    pub(super) fn probe_sequence(&self) -> ProbeSequence {
        self.identity.correlation.sequence
    }
}

impl fmt::Debug for ClientCurrentProbeQueued {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ClientCurrentProbeQueued([REDACTED])")
    }
}

pub(super) enum ClientCurrentProbeBeginError {
    PolicyBlocked,
    OutstandingProbe,
    MissingCurrent,
    SequenceExhausted,
    Protocol(ProtocolError),
    Queue(Box<LegControlQueueError>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ClientCurrentProbeBeginErrorKind {
    PolicyBlocked,
    OutstandingProbe,
    MissingCurrent,
    SequenceExhausted,
    Protocol,
    Queue,
}

impl ClientCurrentProbeBeginError {
    pub(super) const fn kind(&self) -> ClientCurrentProbeBeginErrorKind {
        match self {
            Self::PolicyBlocked => ClientCurrentProbeBeginErrorKind::PolicyBlocked,
            Self::OutstandingProbe => ClientCurrentProbeBeginErrorKind::OutstandingProbe,
            Self::MissingCurrent => ClientCurrentProbeBeginErrorKind::MissingCurrent,
            Self::SequenceExhausted => ClientCurrentProbeBeginErrorKind::SequenceExhausted,
            Self::Protocol(_) => ClientCurrentProbeBeginErrorKind::Protocol,
            Self::Queue(_) => ClientCurrentProbeBeginErrorKind::Queue,
        }
    }
}

impl fmt::Debug for ClientCurrentProbeBeginError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PolicyBlocked => {
                formatter.write_str("ClientCurrentProbeBeginError::PolicyBlocked")
            }
            Self::OutstandingProbe => {
                formatter.write_str("ClientCurrentProbeBeginError::OutstandingProbe")
            }
            Self::MissingCurrent => {
                formatter.write_str("ClientCurrentProbeBeginError::MissingCurrent")
            }
            Self::SequenceExhausted => {
                formatter.write_str("ClientCurrentProbeBeginError::SequenceExhausted")
            }
            Self::Protocol(error) => formatter
                .debug_tuple("ClientCurrentProbeBeginError::Protocol")
                .field(error)
                .finish(),
            Self::Queue(error) => formatter
                .debug_tuple("ClientCurrentProbeBeginError::Queue")
                .field(error)
                .finish(),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum ClientCurrentProbeArmError {
    Stale,
    NotDelivered,
    InvalidTimeout,
    Deadline(SimTimeError),
}

impl fmt::Debug for ClientCurrentProbeArmError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stale => formatter.write_str("ClientCurrentProbeArmError::Stale"),
            Self::NotDelivered => formatter.write_str("ClientCurrentProbeArmError::NotDelivered"),
            Self::InvalidTimeout => {
                formatter.write_str("ClientCurrentProbeArmError::InvalidTimeout")
            }
            Self::Deadline(error) => formatter
                .debug_tuple("ClientCurrentProbeArmError::Deadline")
                .field(error)
                .finish(),
        }
    }
}

pub(super) struct ClientCurrentProbeArmFailure {
    queued: ClientCurrentProbeQueued,
    error: ClientCurrentProbeArmError,
}

impl ClientCurrentProbeArmFailure {
    fn new(queued: ClientCurrentProbeQueued, error: ClientCurrentProbeArmError) -> Self {
        Self { queued, error }
    }

    pub(super) fn into_parts(self) -> (ClientCurrentProbeQueued, ClientCurrentProbeArmError) {
        (self.queued, self.error)
    }
}

impl fmt::Debug for ClientCurrentProbeArmFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClientCurrentProbeArmFailure")
            .field("queued", &"[REDACTED]")
            .field("error", &self.error)
            .finish()
    }
}

/// Opaque, non-cloneable scheduler input for one exact current probe.
pub(super) struct ClientCurrentProbeDeadline {
    identity: Arc<ClientCurrentProbeIdentity>,
    deadline: SimTime,
}

impl ClientCurrentProbeDeadline {
    pub(super) const fn deadline(&self) -> SimTime {
        self.deadline
    }
}

impl fmt::Debug for ClientCurrentProbeDeadline {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ClientCurrentProbeDeadline([REDACTED])")
    }
}

pub(super) enum ClientCurrentProbeSchedulerAuthority {
    Queued(ClientCurrentProbeQueued),
    Armed(ClientCurrentProbeDeadline),
}

impl fmt::Debug for ClientCurrentProbeSchedulerAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ClientCurrentProbeSchedulerAuthority([REDACTED])")
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ClientCurrentProbeAckReconcileErrorKind {
    NotAckWon,
    WrongAcknowledgement,
    WrongSchedulerAuthority,
    HistoryOverflow,
}

pub(super) struct ClientCurrentProbeAckReconcileFailure {
    scheduler: ClientCurrentProbeSchedulerAuthority,
    kind: ClientCurrentProbeAckReconcileErrorKind,
}

impl ClientCurrentProbeAckReconcileFailure {
    fn new(
        scheduler: ClientCurrentProbeSchedulerAuthority,
        kind: ClientCurrentProbeAckReconcileErrorKind,
    ) -> Self {
        Self { scheduler, kind }
    }

    pub(super) const fn kind(&self) -> ClientCurrentProbeAckReconcileErrorKind {
        self.kind
    }

    pub(super) fn into_scheduler(self) -> ClientCurrentProbeSchedulerAuthority {
        self.scheduler
    }
}

impl fmt::Debug for ClientCurrentProbeAckReconcileFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClientCurrentProbeAckReconcileFailure")
            .field("kind", &self.kind)
            .field("scheduler", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ClientCurrentProbeDeadlineStatus {
    Stale,
    NotDue,
    Due,
    AckWon {
        generation: LegGeneration,
        probe_sequence: ProbeSequence,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct ClientCurrentProbeExpired {
    generation: LegGeneration,
    probe_sequence: ProbeSequence,
}

impl ClientCurrentProbeExpired {
    pub(super) const fn generation(self) -> LegGeneration {
        self.generation
    }

    pub(super) const fn probe_sequence(self) -> ProbeSequence {
        self.probe_sequence
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ClientCurrentProbeAcked {
    generation: LegGeneration,
    probe_sequence: ProbeSequence,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::owned_upstream::FlowPortConfig;
    use crate::owned_upstream::leg::{LegTransportReporter, LegTransportTerminalReason};
    use crate::owned_upstream::leg_io::{
        LegEndpointRole, LegIoLimits, MemoryFaultScript, MemoryLegTransport,
    };
    use crate::owned_upstream::session::SessionOwnerCommand;
    use crate::owned_upstream::standby::tests::{
        AcceptedRegisteredCatchUpClientTransportFixture, AcceptedRegisteredClientTransportFixture,
        accepted_registered_catch_up_client_transport_fixture,
        accepted_registered_client_transport_fixture,
    };
    use crate::owned_upstream::two_leg::{
        FaultAction, LegId, TwoLegWire, WireBounds, WireCapacity, WireCapacitySpec, WireLane,
    };
    use crate::resumable::{
        AttachAlpn, AttachAuthority, AttachCredentials, AttachNonce, AttachPolicy, AttachRequest,
        AttachTransportBinding, ByteOffset, DevicePrincipal, DeviceSecret, Direction, FeatureOffer,
        Frame, OpenResultCode, OwnerIdentity, ReceiveBudgetLimits, Record, ReplayBudgetLimits,
        ResetReason, ResumeSecret, SESSION_PROTOCOL_VERSION, STANDBY_CONTROL_V1, SessionConfig,
        SessionError, SessionFlowId, SessionId, SessionPhase, TcpWindowLimits, TlsExporterBinding,
        VersionRange,
    };
    use crate::shared::TargetAddr;
    use bytes::Bytes;
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
    use std::time::Duration;

    #[test]
    fn registered_recovery_action_matrix_is_root_actionable() {
        for retryable in [
            ClientRecoveryBeginErrorKind::PriorRecoveryDeliveryPending,
            ClientRecoveryBeginErrorKind::PriorRecoveryTailUnsettled,
            ClientRecoveryBeginErrorKind::RetiredCapacityExceeded,
            ClientRecoveryBeginErrorKind::PriorReplacementApplicationAckPending,
            ClientRecoveryBeginErrorKind::ObservationSettlementPending,
            ClientRecoveryBeginErrorKind::CurrentProbeSettlementPending,
        ] {
            assert_eq!(
                classify_client_recovery_begin_error(retryable),
                ClientRegisteredRecoveryFailureClass::Retryable,
            );
        }
        for invariant in [
            ClientRecoveryBeginErrorKind::NotActive,
            ClientRecoveryBeginErrorKind::CatchUpRequiresDiscardedCurrent,
            ClientRecoveryBeginErrorKind::WrongTransport,
            ClientRecoveryBeginErrorKind::InstallRejected,
        ] {
            assert_eq!(
                classify_client_recovery_begin_error(invariant),
                ClientRegisteredRecoveryFailureClass::Invariant,
            );
        }

        for retryable in [
            AcceptedClientInstallErrorKind::RecoveryPlanRetryable,
            AcceptedClientInstallErrorKind::RecoveryReservationRetryable,
        ] {
            assert_eq!(
                classify_client_install_error(retryable),
                ClientRegisteredRecoveryFailureClass::Retryable,
            );
        }
        assert_eq!(
            classify_client_install_error(AcceptedClientInstallErrorKind::SupervisorRejected),
            ClientRegisteredRecoveryFailureClass::SessionFatal,
        );
        for terminal in [
            AcceptedClientInstallErrorKind::EndpointLost,
            AcceptedClientInstallErrorKind::RecoveryReservationTerminal,
        ] {
            assert_eq!(
                classify_client_install_error(terminal),
                ClientRegisteredRecoveryFailureClass::StandbyTerminal,
            );
        }
        for invariant in [
            AcceptedClientInstallErrorKind::TerminalTransitionPoisoned,
            AcceptedClientInstallErrorKind::RecoveryPlanInvariant,
            AcceptedClientInstallErrorKind::RecoveryReservationInvariant,
        ] {
            assert_eq!(
                classify_client_install_error(invariant),
                ClientRegisteredRecoveryFailureClass::Invariant,
            );
        }
    }

    const FEATURES: u64 = STANDBY_CONTROL_V1.bits() | 0b0111;

    mod r7_client_progress;
    mod r7_observation_settlement;
    mod r7_replacement_probe_gate;

    trait AmbiguousIfClone<Marker> {
        const MARKER: ();
    }

    impl<T: ?Sized> AmbiguousIfClone<()> for T {
        const MARKER: () = ();
    }

    impl<T: Clone> AmbiguousIfClone<u8> for T {
        const MARKER: () = ();
    }

    #[test]
    fn current_probe_scheduler_authorities_are_not_clone() {
        let _ = <ClientCurrentProbeQueued as AmbiguousIfClone<_>>::MARKER;
        let _ = <crate::owned_upstream::quinn_leg_composition::QuinnCurrentProbeQueued as AmbiguousIfClone<_>>::MARKER;
        let _ = <crate::owned_upstream::quinn_leg_composition::QuinnTerminalGraceTimer as AmbiguousIfClone<_>>::MARKER;
    }

    fn expect_driver_effects(
        controller: &mut ClientSessionController,
        input: ClientControllerDriverInput,
    ) -> Vec<SessionEffect> {
        match controller.apply_driver_input(input) {
            ClientControllerDriverApply::Applied(effects) => effects,
            ClientControllerDriverApply::Deferred { .. } => {
                panic!("test driver input unexpectedly met reducer pressure")
            }
            ClientControllerDriverApply::Failed(error) => {
                panic!("test driver input unexpectedly failed: {error}")
            }
        }
    }

    #[test]
    fn matching_probe_ack_is_delivery_proof_even_before_local_receipt_publication() {
        let mut controller = initial_controller();
        let (session_id, generation, epoch_nonce) = {
            let authority = &controller
                .current
                .as_ref()
                .expect("initial current transport exists")
                .authority;
            (
                authority.probe_session_id(),
                authority.generation(),
                authority.probe_epoch_nonce(),
            )
        };
        let probe_sequence = ProbeSequence::new(1).unwrap();
        let features = FeatureSet::new(FEATURES);

        let receipt_io = LegIo::for_authenticated_transport(
            LegId::B,
            LegEndpointRole::Client,
            binding(0x9a),
            limits(),
        );
        let receipt_seal = receipt_io.established_leg().standby_seal();
        let mut receipt_queue =
            LegOutboundQueue::for_leg(receipt_io.established_leg(), 1, 1_024).unwrap();
        let receipt = receipt_queue
            .push_leg_control(
                &receipt_seal,
                LegControlFrame::try_new(
                    generation,
                    LegControlRecord::LegProbe {
                        session_id,
                        leg_epoch_nonce: epoch_nonce,
                        probe_sequence,
                    },
                    features,
                )
                .unwrap(),
                features,
            )
            .unwrap();
        assert!(!receipt.was_delivered());
        let identity = Arc::new(ClientCurrentProbeIdentity {
            correlation: ClientCurrentProbeCorrelation {
                seal: receipt_seal,
                session_id,
                generation,
                epoch_nonce,
                sequence: probe_sequence,
                switch_epoch: controller.current_liveness.switch_epoch,
            },
        });
        let queued = ClientCurrentProbeQueued {
            identity: Arc::clone(&identity),
        };
        controller.current_liveness.phase = ClientCurrentProbePhase::Queued { identity, receipt };

        let ack = LegControlFrame::try_new(
            generation,
            LegControlRecord::LegProbeAck {
                session_id,
                leg_epoch_nonce: epoch_nonce,
                probe_sequence,
            },
            features,
        )
        .unwrap();
        let acked = controller
            .accept_current_probe_ack(
                receipt_io
                    .established_leg()
                    .bind_received_control_frame(ack),
                SimTime::ZERO,
            )
            .expect("the exact ACK is observed despite delayed local receipt publication");
        assert_eq!(acked.generation, generation);
        assert_eq!(acked.probe_sequence, probe_sequence);
        let (queued, error) = controller
            .arm_current_probe(queued, SimTime::from_nanos(10), Duration::from_millis(1))
            .expect_err("ACK-completed queued authority is stale")
            .into_parts();
        assert!(
            matches!(error, ClientCurrentProbeArmError::Stale),
            "the peer ACK wins without waiting for the redundant receipt",
        );
        assert!(!controller.current_probe_writer_completed_for_test());
        let before_reconcile = controller.ownership_ledger(1, 0);
        assert_eq!(before_reconcile.live_probe_authorities(), 1);
        assert_eq!(before_reconcile.live_probe_scheduler_authorities(), 1);
        let settlement = controller
            .reconcile_current_probe_ack(
                ClientCurrentProbeSchedulerAuthority::Queued(queued),
                acked.generation,
                acked.probe_sequence,
            )
            .expect("root returns the pointer-identical queued scheduler authority");
        assert_eq!(
            settlement.phase(),
            ClientCurrentProbeAckSettlementPhase::Queued
        );
        assert_eq!(settlement.generation(), generation);
        assert_eq!(settlement.probe_sequence(), probe_sequence);
        let after_reconcile = controller.ownership_ledger(0, 0);
        assert_eq!(after_reconcile.live_probe_authorities(), 0);
        assert_eq!(after_reconcile.live_probe_scheduler_authorities(), 0);
        assert_eq!(after_reconcile.settled_probe_acks(), 1);
        assert_eq!(after_reconcile.settled_queued_probe_acks(), 1);
        assert_eq!(after_reconcile.settled_armed_probe_acks(), 0);
        assert_eq!(
            after_reconcile.last_probe_ack_settlement(),
            Some(settlement)
        );
        drop(receipt_queue);
    }

    #[test]
    fn stale_probe_arm_cannot_target_next_probe_and_deadline_starts_at_arm_time() {
        let mut controller = initial_controller();
        let (session_id, generation, epoch_nonce) = {
            let authority = &controller
                .current
                .as_ref()
                .expect("initial current transport exists")
                .authority;
            (
                authority.probe_session_id(),
                authority.generation(),
                authority.probe_epoch_nonce(),
            )
        };
        let features = FeatureSet::new(FEATURES);
        let mut receipt_io = LegIo::for_authenticated_transport(
            LegId::B,
            LegEndpointRole::Client,
            binding(0x9b),
            limits(),
        );
        let route = receipt_io.outbound_route();
        let live_seal = receipt_io.established_leg().standby_seal();
        let stale_seal = receipt_io.established_leg().standby_seal();
        let mut receipt_queue =
            LegOutboundQueue::for_leg(receipt_io.established_leg(), 1, 1_024).unwrap();
        let live_sequence = ProbeSequence::new(2).unwrap();
        let receipt = receipt_queue
            .push_leg_control(
                &live_seal,
                LegControlFrame::try_new(
                    generation,
                    LegControlRecord::LegProbe {
                        session_id,
                        leg_epoch_nonce: epoch_nonce,
                        probe_sequence: live_sequence,
                    },
                    features,
                )
                .unwrap(),
                features,
            )
            .unwrap();
        let mut wire = memory_transport(MemoryFaultScript::new(0));
        receipt_queue
            .try_flush(
                &mut receipt_io,
                &mut wire.controller_sender(),
                SimTime::ZERO,
            )
            .unwrap()
            .unwrap();
        release_due(&mut wire);
        wire.try_recv_next(route)
            .unwrap()
            .expect("live probe receives its exact positive delivery");
        assert!(receipt.was_delivered());

        let live_identity = Arc::new(ClientCurrentProbeIdentity {
            correlation: ClientCurrentProbeCorrelation {
                seal: live_seal,
                session_id,
                generation,
                epoch_nonce,
                sequence: live_sequence,
                switch_epoch: controller.current_liveness.switch_epoch,
            },
        });
        let live_queued = ClientCurrentProbeQueued {
            identity: Arc::clone(&live_identity),
        };
        controller.current_liveness.phase = ClientCurrentProbePhase::Queued {
            identity: Arc::clone(&live_identity),
            receipt,
        };

        let stale_identity = Arc::new(ClientCurrentProbeIdentity {
            correlation: ClientCurrentProbeCorrelation {
                seal: stale_seal,
                session_id,
                generation,
                epoch_nonce,
                sequence: ProbeSequence::new(1).unwrap(),
                switch_epoch: controller.current_liveness.switch_epoch,
            },
        });
        let stale_queued = ClientCurrentProbeQueued {
            identity: Arc::clone(&stale_identity),
        };
        let (returned_stale, error) = controller
            .arm_current_probe(
                stale_queued,
                SimTime::from_nanos(1_000),
                Duration::from_nanos(50),
            )
            .expect_err("probe N cannot arm already-live probe N+1")
            .into_parts();
        assert!(matches!(error, ClientCurrentProbeArmError::Stale));
        assert!(Arc::ptr_eq(&returned_stale.identity, &stale_identity));
        assert!(matches!(
            &controller.current_liveness.phase,
            ClientCurrentProbePhase::Queued { identity, .. }
                if Arc::ptr_eq(identity, &live_identity)
        ));

        let deadline = controller
            .arm_current_probe(
                live_queued,
                SimTime::from_nanos(1_000),
                Duration::from_nanos(50),
            )
            .expect("the exact delivered N+1 authority arms once");
        assert_eq!(deadline.deadline, SimTime::from_nanos(1_050));
        assert_eq!(
            controller.current_probe_deadline_status(&deadline, SimTime::from_nanos(1_049)),
            ClientCurrentProbeDeadlineStatus::NotDue,
        );
        assert_eq!(
            controller.current_probe_deadline_status(&deadline, SimTime::from_nanos(1_050)),
            ClientCurrentProbeDeadlineStatus::Due,
        );
        let late_ack = LegControlFrame::try_new(
            generation,
            LegControlRecord::LegProbeAck {
                session_id,
                leg_epoch_nonce: epoch_nonce,
                probe_sequence: live_sequence,
            },
            features,
        )
        .unwrap();
        assert!(
            controller
                .accept_current_probe_ack(
                    receipt_io
                        .established_leg()
                        .bind_received_control_frame(late_ack),
                    SimTime::from_nanos(1_051),
                )
                .is_none(),
            "an exact ACK received after the armed deadline cannot cancel silent-loss recovery",
        );
        assert_eq!(
            controller.current_probe_deadline_status(&deadline, SimTime::from_nanos(1_051)),
            ClientCurrentProbeDeadlineStatus::Due,
        );
        drop((receipt_queue, returned_stale));
    }

    #[test]
    fn registered_post_loss_b_enters_the_controller_replacement_transaction() {
        let (mut controller, reporter_a) = initial_controller_with_reporter();
        let generation_a = controller.generation();
        let route_a = controller
            .current_inbound_route()
            .expect("initial A owns its inbound route");
        let before_loss = controller.snapshot();
        let terminal_a = reporter_a
            .report(LegTransportTerminalReason::PeerClosed)
            .expect("A reporter mints one exact terminal");
        controller
            .bind_current_terminal(terminal_a)
            .expect("exact A terminal binds to current");
        controller
            .discard_terminating_current_recovery()
            .expect("A loss enters the reducer once");
        assert_eq!(controller.snapshot().session.phase(), SessionPhase::Legless);
        assert_ne!(controller.snapshot(), before_loss);
        assert!(controller.current.is_none());
        assert!(controller.retired.is_none());

        let AcceptedRegisteredClientTransportFixture {
            accepted,
            io,
            queue,
            reporter: reporter_b,
        } = accepted_registered_client_transport_fixture(1);
        let started = controller
            .begin_registered_post_loss_replacement(accepted, io, queue)
            .expect("exact next registered B installs after A loss");

        assert_eq!(started.generation.get(), generation_a.get() + 1);
        assert_eq!(controller.generation(), started.generation);
        assert_eq!(controller.snapshot().session.phase(), SessionPhase::Active);
        assert!(controller.current.is_some());
        assert_eq!(
            controller
                .retired
                .as_ref()
                .expect("terminal-bound A moves into the sole retired slot")
                .authority
                .generation(),
            generation_a,
        );
        assert_eq!(controller.retired_inbound_route(), Some(route_a));
        assert!(controller.replacement_barrier_is_active());
        drop(reporter_b);
    }

    #[test]
    fn registered_post_loss_b_prepare_rejection_restores_b_and_terminal_bound_a() {
        let (mut controller, reporter_a) = initial_controller_with_reporter();
        let factory = controller
            .mint_tcp_port_factory(FlowPortConfig::new(64, 8, 8, 8).unwrap())
            .unwrap();
        let local_flow = controller
            .open_local(target())
            .unwrap()
            .unwrap()
            .into_iter()
            .find_map(|effect| match effect {
                SessionEffect::LocalFlowOpened { flow } => Some(flow),
                _ => None,
            })
            .unwrap();
        let (flow, mut driver) = factory.open_flow(local_flow).unwrap();
        flow.try_send_uplink_with(1, || Bytes::from_static(b"r"))
            .unwrap();
        assert!(matches!(
            controller.apply_driver_input(
                driver
                    .try_recv_next_for_client_controller()
                    .unwrap()
                    .unwrap(),
            ),
            ClientControllerDriverApply::Applied(_)
        ));
        assert_eq!(controller.snapshot().uplink_replay_owned_bytes, 1);

        let generation_a = controller.generation();
        let route_a = controller.current_inbound_route().unwrap();
        let terminal_a = reporter_a
            .report(LegTransportTerminalReason::PeerClosed)
            .unwrap();
        controller.bind_current_terminal(terminal_a).unwrap();
        controller.discard_terminating_current_recovery().unwrap();

        let AcceptedRegisteredClientTransportFixture {
            accepted,
            io,
            queue,
            reporter: reporter_b,
        } = accepted_registered_client_transport_fixture(1);
        let full_effect_bound = controller.recovery_effect_bound;
        controller.recovery_effect_bound = 1;
        let before = controller.snapshot();
        let discarded_before = match &controller.phase {
            ClientControllerPhase::TerminatingCurrent(
                TerminatingCurrentClientTransport::RecoveryDiscarded {
                    authority,
                    transport,
                    generation,
                    reason,
                },
            ) => (
                authority.generation(),
                transport.inbound_route(),
                *generation,
                *reason,
            ),
            _ => panic!("A terminal-bound retirement source is missing"),
        };
        let failure = controller
            .begin_registered_post_loss_replacement(accepted, io, queue)
            .expect_err("bounded prepare rejects B without consuming either side");
        assert_eq!(
            failure.kind(),
            ClientRecoveryBeginErrorKind::InstallRejected
        );
        let (accepted, io, queue) = failure.into_parts();
        assert_eq!(controller.snapshot(), before);
        assert!(controller.current.is_none());
        assert!(controller.retired.is_none());
        let discarded_after = match &controller.phase {
            ClientControllerPhase::TerminatingCurrent(
                TerminatingCurrentClientTransport::RecoveryDiscarded {
                    authority,
                    transport,
                    generation,
                    reason,
                },
            ) => (
                authority.generation(),
                transport.inbound_route(),
                *generation,
                *reason,
            ),
            _ => panic!("post-loss rejection did not restore exact A"),
        };
        assert_eq!(discarded_after, discarded_before);

        controller.recovery_effect_bound = full_effect_bound;
        let started = controller
            .begin_registered_post_loss_replacement(accepted, io, queue)
            .expect("the same B ownership succeeds after the bound is restored");
        assert_eq!(started.generation.get(), generation_a.get() + 1);
        assert_eq!(controller.retired_inbound_route(), Some(route_a));
        assert_eq!(controller.snapshot().uplink_replay_owned_bytes, 1);
        drop((flow, reporter_b));
    }

    #[test]
    fn typed_driver_pressure_returns_the_exact_input_for_one_retry() {
        let (mut controller, _reporter) =
            initial_controller_with_reporter_config(terminal_pressure_session_config());
        let factory = controller
            .mint_tcp_port_factory(FlowPortConfig::new(64, 8, 8, 8).unwrap())
            .unwrap();
        let mut ports = Vec::new();
        for ordinal in 1..=2_u64 {
            let opened = controller.open_local(target()).unwrap().unwrap();
            let local_flow = opened
                .into_iter()
                .find_map(|effect| match effect {
                    SessionEffect::LocalFlowOpened { flow } => Some(flow),
                    _ => None,
                })
                .unwrap();
            assert!(matches!(
                controller.receive_current(
                    EncodedDelivery::from_memory_ingress(
                        controller.current_inbound_route().unwrap(),
                        WireLane::Control,
                        Frame::try_new(
                            controller.generation(),
                            Record::OpenResult {
                                flow_id: local_flow.flow_id(),
                                result: OpenResultCode::Opened,
                            },
                        )
                        .unwrap()
                        .encode()
                        .unwrap()
                        .to_vec(),
                        ordinal,
                        0,
                    )
                    .unwrap(),
                    FeatureSet::new(FEATURES),
                ),
                Ok(ClientInboundDisposition::Applied { .. })
            ));
            ports.push(factory.open_flow(local_flow).unwrap());
        }
        let (first_flow, mut first_driver) = ports.remove(0);
        let (second_flow, mut second_driver) = ports.remove(0);

        first_flow
            .try_send_reset(ResetReason::LocalAbandon)
            .unwrap();
        let first = first_driver
            .try_recv_next_for_client_controller()
            .unwrap()
            .unwrap();
        let first_effects = match controller.apply_driver_input(first) {
            ClientControllerDriverApply::Applied(effects) => effects,
            _ => panic!("first reset must fill the sole terminal tombstone"),
        };
        let first_terminal = first_effects
            .into_iter()
            .find_map(|effect| match effect {
                SessionEffect::FlowFinished { terminal, .. } => Some(terminal),
                _ => None,
            })
            .expect("first reset mints the sole terminal grace capability");

        second_flow
            .try_send_reset(ResetReason::LocalAbandon)
            .unwrap();
        let second = second_driver
            .try_recv_next_for_client_controller()
            .unwrap()
            .unwrap();
        let before_pressure = controller.snapshot();
        let deferred = match controller.apply_driver_input(second) {
            ClientControllerDriverApply::Deferred {
                block:
                    crate::owned_upstream::supervisor::ReducerAdmissionBlock::TerminalTombstoneCapacityExceeded {
                        max: 1,
                    },
                input,
            } => input,
            _ => panic!("second reset must return its exact typed ownership under pressure"),
        };
        assert_eq!(controller.snapshot(), before_pressure);

        controller.expire_terminal_grace(first_terminal).unwrap();
        let retried = match controller.apply_driver_input(deferred) {
            ClientControllerDriverApply::Applied(effects) => effects,
            _ => panic!("the returned typed input must commit after pressure clears"),
        };
        assert_eq!(
            retried
                .iter()
                .filter(|effect| matches!(effect, SessionEffect::FlowFinished { .. }))
                .count(),
            1,
        );
        assert_eq!(controller.snapshot().session.terminal_tombstones(), 1);
    }

    #[test]
    fn registered_catch_up_d_enters_the_controller_replacement_transaction() {
        let (mut controller, reporter) = initial_controller_with_reporter();
        let original_generation = controller.generation();
        let original_route = controller.current_inbound_route().unwrap();
        let factory = controller
            .mint_tcp_port_factory(FlowPortConfig::new(64, 8, 8, 8).unwrap())
            .unwrap();
        let opened = controller.open_local(target()).unwrap().unwrap();
        let local_flow = opened
            .into_iter()
            .find_map(|effect| match effect {
                SessionEffect::LocalFlowOpened { flow } => Some(flow),
                _ => None,
            })
            .unwrap();
        let open_result = EncodedDelivery::from_memory_ingress(
            original_route,
            WireLane::Control,
            Frame::try_new(
                original_generation,
                Record::OpenResult {
                    flow_id: local_flow.flow_id(),
                    result: OpenResultCode::Opened,
                },
            )
            .unwrap()
            .encode()
            .unwrap()
            .to_vec(),
            301,
            0,
        )
        .unwrap();
        assert!(matches!(
            controller.receive_current(open_result, FeatureSet::new(FEATURES)),
            Ok(ClientInboundDisposition::Applied { .. })
        ));
        let (flow, mut driver) = factory.open_flow(local_flow).unwrap();
        for byte in [b"a".as_slice(), b"b".as_slice()] {
            flow.try_send_uplink_with(1, || Bytes::copy_from_slice(byte))
                .unwrap();
            expect_driver_effects(
                &mut controller,
                driver
                    .try_recv_next_for_client_controller()
                    .unwrap()
                    .unwrap(),
            );
        }
        assert_eq!(controller.snapshot().uplink_replay_extents, 2);

        let AcceptedRegisteredCatchUpClientTransportFixture {
            accepted,
            io,
            queue,
            reporter: reporter_d,
        } = accepted_registered_catch_up_client_transport_fixture(1);
        let (wrong_io, wrong_reporter) = LegIo::for_authenticated_transport_with_reporter(
            LegId::B,
            LegEndpointRole::Client,
            binding(0xdd),
            limits(),
        );
        let before_rejection = controller.snapshot();
        let failure = controller
            .begin_registered_catch_up_replacement(accepted, wrong_io, queue)
            .unwrap_err();
        assert_eq!(failure.kind(), ClientRecoveryBeginErrorKind::WrongTransport);
        let (accepted, wrong_io, queue) = failure.into_parts();
        drop((wrong_io, wrong_reporter));
        assert_eq!(controller.snapshot(), before_rejection);

        let failure = controller
            .begin_registered_catch_up_replacement(accepted, io, queue)
            .unwrap_err();
        assert_eq!(
            failure.kind(),
            ClientRecoveryBeginErrorKind::CatchUpRequiresDiscardedCurrent
        );
        let (accepted, io, queue) = failure.into_parts();
        assert_eq!(controller.snapshot(), before_rejection);

        let terminal = reporter
            .report(LegTransportTerminalReason::FatalIo)
            .unwrap();
        controller.bind_current_terminal(terminal).unwrap();
        controller.discard_terminating_current_recovery().unwrap();
        assert_eq!(controller.snapshot().session.phase(), SessionPhase::Legless);
        assert!(controller.current.is_none());
        assert!(controller.retired.is_none());
        assert!(matches!(
            controller.phase,
            ClientControllerPhase::TerminatingCurrent(
                TerminatingCurrentClientTransport::RecoveryDiscarded { .. }
            )
        ));
        assert_eq!(accepted.generation().get(), 4);

        let full_effect_bound = controller.recovery_effect_bound;
        controller.recovery_effect_bound = 1;
        let before_prepare_rejection = controller.snapshot();
        let discarded_before = match &controller.phase {
            ClientControllerPhase::TerminatingCurrent(
                TerminatingCurrentClientTransport::RecoveryDiscarded {
                    authority,
                    transport,
                    generation,
                    reason,
                },
            ) => (
                authority.generation(),
                transport.inbound_route(),
                *generation,
                *reason,
            ),
            _ => panic!("catch-up source was not retained before prepare rejection"),
        };
        let failure = controller
            .begin_registered_catch_up_replacement(accepted, io, queue)
            .unwrap_err();
        assert_eq!(
            failure.kind(),
            ClientRecoveryBeginErrorKind::InstallRejected
        );
        let (accepted, io, queue) = failure.into_parts();
        assert_eq!(accepted.generation().get(), 4);
        assert_eq!(controller.snapshot(), before_prepare_rejection);
        assert!(controller.current.is_none());
        assert!(controller.retired.is_none());
        let discarded_after = match &controller.phase {
            ClientControllerPhase::TerminatingCurrent(
                TerminatingCurrentClientTransport::RecoveryDiscarded {
                    authority,
                    transport,
                    generation,
                    reason,
                },
            ) => (
                authority.generation(),
                transport.inbound_route(),
                *generation,
                *reason,
            ),
            _ => panic!("prepare rejection did not restore the catch-up source"),
        };
        assert_eq!(discarded_after, discarded_before);
        controller.recovery_effect_bound = full_effect_bound;

        let started = controller
            .begin_registered_catch_up_replacement(accepted, io, queue)
            .unwrap();

        assert_eq!(started.generation.get(), 4);
        assert_eq!(controller.generation(), started.generation);
        assert_eq!(
            controller.retired.as_ref().unwrap().authority.generation(),
            original_generation
        );
        assert_eq!(controller.retired_inbound_route(), Some(original_route));
        assert!(controller.replacement_barrier_is_active());

        let blocked = ack_delivery(
            controller.current_inbound_route().unwrap(),
            started.generation,
            local_flow.flow_id(),
            0,
            302,
        );
        let expected_blocked = blocked.clone();
        let before_blocked = controller.current_counters().unwrap();
        let blocked = match controller.receive_current(blocked, FeatureSet::new(FEATURES)) {
            Ok(ClientInboundDisposition::Blocked(blocked)) => blocked,
            _ => panic!("catch-up recovery did not preserve the inbound barrier"),
        };
        assert_eq!(blocked, expected_blocked);
        assert_eq!(controller.current_counters(), Some(before_blocked));

        assert!(matches!(
            controller.drive_recovery_one(),
            ClientRecoveryDrive::Progress(ClientRecoveryObservation::LegActivated {
                generation
            }) if generation == started.generation
        ));
        assert!(matches!(
            controller.drive_recovery_one(),
            ClientRecoveryDrive::Progress(ClientRecoveryObservation::FrameAdmitted)
        ));
        assert!(matches!(
            controller.drive_recovery_one(),
            ClientRecoveryDrive::Pressure
        ));

        let route_d = controller.current_outbound_route().unwrap();
        let mut wire_d = memory_transport(MemoryFaultScript::new(0));
        assert!(
            controller
                .try_flush_current(&mut wire_d.controller_sender(), SimTime::ZERO)
                .unwrap()
                .is_some()
        );
        assert!(matches!(
            controller.drive_recovery_one(),
            ClientRecoveryDrive::Pressure
        ));
        assert!(matches!(
            controller.try_flush_current(&mut wire_d.controller_sender(), SimTime::ZERO),
            Err(LegIoError::OrderedDeliveryPending)
        ));
        release_due(&mut wire_d);
        assert_data_delivery(wire_d.try_recv_next(route_d).unwrap().unwrap(), 0, b"a");
        assert!(
            controller
                .try_flush_current(&mut wire_d.controller_sender(), SimTime::ZERO)
                .unwrap()
                .is_none(),
            "exact first delivery must rearm recovery without submitting the pending frame"
        );
        assert!(matches!(
            controller.drive_recovery_one(),
            ClientRecoveryDrive::AdmissionCompleted { generation }
                if generation == started.generation
        ));
        assert!(
            controller
                .try_flush_current(&mut wire_d.controller_sender(), SimTime::ZERO)
                .unwrap()
                .is_some()
        );
        let ClientControllerPhase::Active {
            recovery_tail: Some(tail),
        } = &controller.phase
        else {
            panic!("catch-up D did not retain its final ordered-delivery tail");
        };
        assert!(!tail.actual_delivery_complete());
        release_due(&mut wire_d);
        assert_data_delivery(wire_d.try_recv_next(route_d).unwrap().unwrap(), 1, b"b");
        let ClientControllerPhase::Active {
            recovery_tail: Some(tail),
        } = &controller.phase
        else {
            panic!("catch-up D lost its ordered-delivery tail");
        };
        assert!(tail.actual_delivery_complete());

        assert!(matches!(
            controller.receive_current(blocked, FeatureSet::new(FEATURES)),
            Ok(ClientInboundDisposition::Applied {
                observation: None,
                ..
            })
        ));
        assert!(matches!(
            controller.receive_current(
                ack_delivery(
                    controller.current_inbound_route().unwrap(),
                    started.generation,
                    local_flow.flow_id(),
                    1,
                    303,
                ),
                FeatureSet::new(FEATURES),
            ),
            Ok(ClientInboundDisposition::Applied {
                observation: Some(ClientInboundObservation::ReplacementRecovered {
                    flow_id,
                    generation,
                    direction: Direction::ClientToTarget,
                    previous_accepted,
                    next_accepted,
                    released_bytes: 1,
                    final_accepted: false,
                    ..
                }),
                ..
            }) if flow_id == local_flow.flow_id()
                && generation == started.generation
                && previous_accepted == ByteOffset::new(0)
                && next_accepted == ByteOffset::new(1)
        ));
        assert!(matches!(
            controller.receive_current(
                ack_delivery(
                    controller.current_inbound_route().unwrap(),
                    started.generation,
                    local_flow.flow_id(),
                    2,
                    304,
                ),
                FeatureSet::new(FEATURES),
            ),
            Ok(ClientInboundDisposition::Applied {
                observation: Some(ClientInboundObservation::ApplicationAckAdvanced {
                    flow_id,
                    generation,
                    direction: Direction::ClientToTarget,
                    previous_accepted,
                    next_accepted,
                    ..
                }),
                ..
            }) if flow_id == local_flow.flow_id()
                && generation == started.generation
                && previous_accepted == ByteOffset::new(1)
                && next_accepted == ByteOffset::new(2)
        ));
        assert_eq!(controller.snapshot().uplink_replay_extents, 0);
        drop((flow, factory, reporter_d, wire_d));
    }

    fn binding(exporter: u8) -> AttachTransportBinding {
        AttachTransportBinding::new(
            OwnerIdentity::new([0x31; 32]).unwrap(),
            AttachAlpn::new(b"mini-vpn-owned/1").unwrap(),
            TlsExporterBinding::new([exporter; 32]).unwrap(),
            DevicePrincipal::new([0x53; 16]).unwrap(),
        )
    }

    fn credentials() -> AttachCredentials {
        AttachCredentials::new(
            DeviceSecret::new([0x71; 32]).unwrap(),
            ResumeSecret::new([0x72; 32]).unwrap(),
        )
        .unwrap()
    }

    fn session_config() -> SessionConfig {
        SessionConfig::new(
            4,
            4,
            TcpWindowLimits::new(64, 8).unwrap(),
            TcpWindowLimits::new(64, 8).unwrap(),
            ReplayBudgetLimits::new(64, 8).unwrap(),
            ReplayBudgetLimits::new(64, 8).unwrap(),
            ReceiveBudgetLimits::new(128, 16).unwrap(),
            64,
        )
        .unwrap()
    }

    fn terminal_pressure_session_config() -> SessionConfig {
        SessionConfig::new(
            4,
            1,
            TcpWindowLimits::new(64, 8).unwrap(),
            TcpWindowLimits::new(64, 8).unwrap(),
            ReplayBudgetLimits::new(64, 8).unwrap(),
            ReplayBudgetLimits::new(64, 8).unwrap(),
            ReceiveBudgetLimits::new(128, 16).unwrap(),
            64,
        )
        .unwrap()
    }

    fn authority_for(session_id: SessionId) -> AttachAuthority {
        AttachAuthority::new(
            session_id,
            LegGeneration::new(1).unwrap(),
            credentials(),
            AttachPolicy::new(
                binding(0x42).owner_identity(),
                binding(0x42).alpn(),
                binding(0x42).device_principal(),
                SESSION_PROTOCOL_VERSION,
                FEATURES,
            )
            .unwrap(),
        )
    }

    fn capacity() -> WireCapacity {
        WireCapacitySpec::new(80_000, Duration::from_secs(1), 256, 128, 1)
            .unwrap()
            .with_fault_copies(1, 1)
            .unwrap()
            .derive()
            .unwrap()
    }

    fn limits() -> LegIoLimits {
        LegIoLimits::from_wire_capacity(capacity(), 1_024).unwrap()
    }

    fn memory_transport(faults: MemoryFaultScript) -> MemoryLegTransport {
        MemoryLegTransport::new(
            TwoLegWire::new(WireBounds::new(capacity(), 64, 32_000, 4, 4).unwrap()),
            faults,
        )
    }

    fn release_due(transport: &mut MemoryLegTransport) {
        while transport.advance_one_due(SimTime::ZERO).unwrap() {}
        transport.advance_idle_to(SimTime::ZERO).unwrap();
    }

    fn initial_controller_with_reporter_config(
        config: SessionConfig,
    ) -> (ClientSessionController, LegTransportReporter) {
        initial_controller_with_reporter_config_for_session(
            config,
            SessionId::new([0x11; 16]).unwrap(),
        )
    }

    fn initial_controller_with_reporter_config_for_session(
        config: SessionConfig,
        session_id: SessionId,
    ) -> (ClientSessionController, LegTransportReporter) {
        let transport_binding = binding(0x42);
        let (mut client, reporter) = LegIo::for_authenticated_transport_with_reporter(
            LegId::A,
            LegEndpointRole::Client,
            transport_binding,
            limits(),
        );
        let mut owner = LegIo::for_authenticated_transport(
            LegId::A,
            LegEndpointRole::Owner,
            transport_binding,
            limits(),
        );
        let request = AttachRequest::new(
            session_id,
            LegGeneration::new(2).unwrap(),
            AttachNonce::new([0x22; 16]).unwrap(),
            VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION).unwrap(),
            FeatureOffer::new(FEATURES, STANDBY_CONTROL_V1.bits()).unwrap(),
        );
        let proof = credentials().prove(&request, &transport_binding).unwrap();
        let pending = client
            .established_leg()
            .begin_attach(request)
            .bind_initial_frame(request.to_attach_frame(proof))
            .unwrap();
        let mut client_queue =
            LegOutboundQueue::for_leg(client.established_leg(), 4, 4_096).unwrap();
        let awaiting = pending.enqueue(&mut client_queue).unwrap();
        let mut owner_queue = LegOutboundQueue::for_leg(owner.established_leg(), 4, 4_096).unwrap();
        let mut wire = memory_transport(MemoryFaultScript::new(0));

        client_queue
            .try_flush(&mut client, &mut wire.controller_sender(), SimTime::ZERO)
            .unwrap()
            .unwrap();
        release_due(&mut wire);
        let request_delivery = wire.try_recv_next(owner.inbound_route()).unwrap().unwrap();
        let received = owner.receive_delivery(request_delivery).unwrap();
        let mut pending_owner =
            SessionSupervisor::prepare_owner_unproven_for_test(config, authority_for(session_id));
        let owner_bootstrap = pending_owner
            .accept(owner.established_leg(), received, &mut owner_queue)
            .unwrap()
            .enqueue_acceptance(&mut owner_queue)
            .unwrap();

        owner_queue
            .try_flush(&mut owner, &mut wire.controller_sender(), SimTime::ZERO)
            .unwrap()
            .unwrap();
        release_due(&mut wire);
        let acceptance_delivery = wire.try_recv_next(client.inbound_route()).unwrap().unwrap();
        let response = client.receive_delivery(acceptance_delivery).unwrap();
        let accepted = awaiting.validate_response(&client_queue, response).unwrap();
        drop(owner_bootstrap);
        (
            ClientSessionController::bootstrap_initial(config, accepted, client, client_queue)
                .unwrap(),
            reporter,
        )
    }

    fn initial_controller_with_reporter() -> (ClientSessionController, LegTransportReporter) {
        initial_controller_with_reporter_config(session_config())
    }

    fn initial_controller() -> ClientSessionController {
        initial_controller_with_reporter().0
    }

    #[test]
    fn prepared_local_open_controller_commits_factory_model_and_fifo_as_one_result() {
        let mut controller = initial_controller();
        let factory = controller
            .mint_tcp_port_factory(FlowPortConfig::new(64, 8, 8, 8).unwrap())
            .unwrap();
        let before_flow_count = controller.snapshot().session.flow_count();
        let before_queue_count = controller.current_queued_frames().unwrap();

        let committed = controller
            .install_prepared_local_open(target(), |local_flow| factory.open_flow(local_flow))
            .expect("active current leg atomically accepts one local OPEN");
        let ((flow, driver), local_flow, enqueue_ordinal) = committed.into_parts();

        assert_eq!(local_flow, flow.local_flow());
        assert_eq!(
            controller.snapshot().session.flow_count(),
            before_flow_count + 1
        );
        assert_eq!(
            controller.current_queued_frames(),
            Some(before_queue_count + 1)
        );
        assert!(enqueue_ordinal > 0);
        drop((flow, driver));
    }

    #[test]
    fn prepared_local_open_controller_adapter_rejection_changes_no_model_or_fifo_state() {
        let mut controller = initial_controller();
        let before = controller.snapshot();
        let before_queue_count = controller.current_queued_frames();
        let before_queue_bytes = controller.current_queued_bytes();
        let ownership = Bytes::from(vec![0x7d; 23]);
        let ownership_ptr = ownership.as_ptr();

        let failure = controller
            .install_prepared_local_open(target(), |_local_flow| Err::<(), _>(ownership))
            .expect_err("adapter rejection remains precommit");
        let (returned_target, returned_frame, returned_ownership) = match failure {
            ClientPreparedLocalOpenFailure::Adapter {
                target,
                frame,
                error,
            } => (target, frame, error),
            other => panic!("unexpected prepared OPEN rejection: {other:?}"),
        };

        assert_eq!(returned_target, target());
        assert!(matches!(
            returned_frame.record(),
            Record::Open { flow_id, target: opened_target }
                if *flow_id == SessionFlowId::new(1).unwrap()
                    && opened_target == &target()
        ));
        assert_eq!(returned_ownership.as_ptr(), ownership_ptr);
        assert_eq!(controller.snapshot(), before);
        assert_eq!(controller.current_queued_frames(), before_queue_count);
        assert_eq!(controller.current_queued_bytes(), before_queue_bytes);
    }

    #[test]
    fn prepared_local_open_controller_queue_pressure_precedes_factory_and_retries_same_id() {
        let mut controller = initial_controller();
        let factory = controller
            .mint_tcp_port_factory(FlowPortConfig::new(64, 8, 8, 8).unwrap())
            .unwrap();
        for offset in 0..4_u64 {
            let frame = Frame::try_new(
                controller.generation(),
                Record::Data {
                    flow_id: SessionFlowId::new(77).unwrap(),
                    direction: Direction::ClientToTarget,
                    offset: ByteOffset::new(offset),
                    payload: Bytes::from_static(b"q"),
                },
            )
            .unwrap();
            controller
                .enqueue_current_frame(frame)
                .expect("current leg exists")
                .expect("fixture fills one bounded FIFO slot");
        }
        let before = controller.snapshot();
        let before_queue_count = controller.current_queued_frames();
        let factory_called = std::cell::Cell::new(false);

        let failure = controller
            .install_prepared_local_open(target(), |local_flow| {
                factory_called.set(true);
                factory.open_flow(local_flow)
            })
            .expect_err("full FIFO rejects before factory admission");
        let returned_frame = match failure {
            ClientPreparedLocalOpenFailure::Queue {
                frame,
                kind: LegOutboundQueueErrorKind::CapacityExceeded { .. },
                ..
            } => frame,
            other => panic!("unexpected prepared OPEN rejection: {other:?}"),
        };
        assert!(matches!(
            returned_frame.record(),
            Record::Open { flow_id, target: opened_target }
                if *flow_id == SessionFlowId::new(1).unwrap()
                    && opened_target == &target()
        ));
        assert!(!factory_called.get());
        assert_eq!(controller.snapshot(), before);
        assert_eq!(controller.current_queued_frames(), before_queue_count);

        let mut transport = memory_transport(MemoryFaultScript::new(0));
        assert!(
            controller
                .try_flush_current(&mut transport.controller_sender(), SimTime::ZERO)
                .unwrap()
                .is_some()
        );
        let committed = controller
            .install_prepared_local_open(target(), |local_flow| factory.open_flow(local_flow))
            .expect("released FIFO capacity admits the same unadvanced reducer/factory id");
        let ((flow, driver), local_flow, _ordinal) = committed.into_parts();
        assert_eq!(local_flow.flow_id(), SessionFlowId::new(1).unwrap());
        assert_eq!(flow.local_flow(), local_flow);
        drop((flow, driver));
    }

    #[test]
    fn prepared_local_open_controller_legless_gate_returns_target_without_callback() {
        let mut controller = initial_controller();
        controller.phase = ClientControllerPhase::Legless;
        let before = controller.snapshot();
        let called = std::cell::Cell::new(false);

        let failure = controller
            .install_prepared_local_open(target(), |_local_flow| {
                called.set(true);
                Ok::<(), ()>(())
            })
            .expect_err("legless controller blocks local OPEN");
        assert!(matches!(
            failure,
            ClientPreparedLocalOpenFailure::Blocked {
                target: returned,
                block: ClientPreparedLocalOpenBlock::Legless,
            } if returned == target()
        ));
        assert!(!called.get());
        assert_eq!(controller.snapshot(), before);
    }

    #[test]
    fn root_start_requires_empty_flow_and_terminal_tombstone_ownership() {
        let mut controller = initial_controller();
        assert!(controller.root_start_is_quiescent());

        let opened = controller
            .open_local(target())
            .expect("initial controller retains current policy")
            .expect("one local flow is admitted");
        let flow = opened
            .iter()
            .find_map(|effect| match effect {
                SessionEffect::LocalFlowOpened { flow } => Some(*flow),
                _ => None,
            })
            .expect("local open mints one exact flow authority");
        assert_eq!(controller.snapshot().session.flow_count(), 1);
        assert!(!controller.root_start_is_quiescent());

        let terminal = controller
            .supervisor
            .apply_event(SessionEvent::LocalReset {
                flow,
                reason: ResetReason::LocalAbandon,
            })
            .expect("local reset terminalizes the pre-root flow");
        assert!(
            terminal
                .iter()
                .any(|effect| matches!(effect, SessionEffect::FlowFinished { .. }))
        );
        assert_eq!(controller.snapshot().session.flow_count(), 0);
        assert_eq!(controller.snapshot().session.terminal_tombstones(), 1);
        assert!(!controller.root_start_is_quiescent());
    }

    #[test]
    fn terminal_grace_rejection_returns_the_exact_authority_without_mutation() {
        let mut owner = initial_controller();
        let mut unknown = initial_controller();
        let mut foreign = initial_controller_with_reporter_config_for_session(
            session_config(),
            SessionId::new([0x12; 16]).unwrap(),
        )
        .0;
        let opened = owner
            .open_local(target())
            .expect("owner retains current policy")
            .expect("one local flow is admitted");
        let flow = opened
            .into_iter()
            .find_map(|effect| match effect {
                SessionEffect::LocalFlowOpened { flow } => Some(flow),
                _ => None,
            })
            .expect("local open mints one exact flow authority");
        let terminal = owner
            .supervisor
            .apply_event(SessionEvent::LocalReset {
                flow,
                reason: ResetReason::LocalAbandon,
            })
            .expect("local reset terminalizes the flow")
            .into_iter()
            .find_map(|effect| match effect {
                SessionEffect::FlowFinished { terminal, .. } => Some(terminal),
                _ => None,
            })
            .expect("terminal flow mints one grace authority");

        let unknown_before = unknown.snapshot();
        let (returned, error) = unknown
            .expire_terminal_grace_preserving(terminal)
            .expect_err("unknown terminal authority is invariant failure");
        assert_eq!(returned, terminal);
        assert!(matches!(
            error,
            SessionSupervisorError::Reducer(crate::resumable::SessionError::UnknownTerminalGrace)
        ));
        assert_eq!(unknown.snapshot(), unknown_before);

        let foreign_before = foreign.snapshot();
        let (returned, error) = foreign
            .expire_terminal_grace_preserving(returned)
            .expect_err("cross-session terminal authority is invariant failure");
        assert_eq!(returned, terminal);
        assert!(matches!(
            error,
            SessionSupervisorError::Reducer(
                crate::resumable::SessionError::CrossSessionTerminalGrace
            )
        ));
        assert_eq!(foreign.snapshot(), foreign_before);

        owner
            .expire_terminal_grace_preserving(returned)
            .expect("the exact authority remains valid in its owner session");
        let owner_after_expiry = owner.snapshot();
        let (returned, error) = owner
            .expire_terminal_grace_preserving(returned)
            .expect_err("duplicate expiry is a sticky invariant failure");
        assert_eq!(returned, terminal);
        assert!(matches!(
            error,
            SessionSupervisorError::Reducer(crate::resumable::SessionError::UnknownTerminalGrace)
        ));
        assert_eq!(owner.snapshot(), owner_after_expiry);
    }

    #[test]
    fn shutdown_capture_retains_current_fifo_witness_after_transport_moves_out() {
        let mut controller = initial_controller();
        let generation = controller.generation();
        controller
            .enqueue_current_frame(
                Frame::try_new(
                    generation,
                    Record::Reset {
                        flow_id: SessionFlowId::new(92).unwrap(),
                        reason: ResetReason::LocalAbandon,
                    },
                )
                .unwrap(),
            )
            .unwrap()
            .unwrap();

        controller.capture_current_transport_shutdown_residual();
        let current = controller.current.take().unwrap();
        drop(current.transport.into_retired());

        let residual = controller
            .current_transport_shutdown_residual()
            .expect("shutdown retains the pre-await FIFO witness after terminal retirement");
        assert!(residual.active());
        assert_eq!(residual.queued_frames(), 1);
        assert!(residual.owned_bytes() > 0);
        assert!(!residual.ordered_delivery_wait());
    }

    fn target() -> TargetAddr {
        TargetAddr::IpPort(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 443)))
    }

    fn assert_data_delivery(delivery: EncodedDelivery, expected_offset: u64, expected: &[u8]) {
        let frame = Frame::decode_owned_exact(Bytes::from(delivery.into_bytes())).unwrap();
        assert!(matches!(
            frame.record(),
            Record::Data {
                offset,
                payload,
                ..
            } if offset.get() == expected_offset && payload.as_ref() == expected
        ));
    }

    fn ack_delivery(
        route: WireRoute,
        generation: LegGeneration,
        flow_id: SessionFlowId,
        next_accepted: u64,
        send_ordinal: u64,
    ) -> EncodedDelivery {
        EncodedDelivery::from_memory_ingress(
            route,
            WireLane::Control,
            Frame::try_new(
                generation,
                Record::Ack {
                    flow_id,
                    direction: Direction::ClientToTarget,
                    next_accepted: ByteOffset::new(next_accepted),
                    final_accepted: false,
                },
            )
            .unwrap()
            .encode()
            .unwrap()
            .to_vec(),
            send_ordinal,
            0,
        )
        .unwrap()
    }

    fn classified_session_delivery(
        route: WireRoute,
        generation: LegGeneration,
        record: Record,
        send_ordinal: u64,
    ) -> EncodedDelivery {
        let lane = match &record {
            Record::Data { .. } | Record::Close { .. } => WireLane::Data,
            _ => WireLane::Control,
        };
        EncodedDelivery::from_memory_ingress(
            route,
            lane,
            Frame::try_new(generation, record)
                .unwrap()
                .encode()
                .unwrap()
                .to_vec(),
            send_ordinal,
            0,
        )
        .unwrap()
    }

    #[derive(Default)]
    struct RejectOnceRetainingRecoveryTransport {
        rejected: Option<Bytes>,
        accepted: Option<Bytes>,
    }

    impl EncodedLegTransport for RejectOnceRetainingRecoveryTransport {
        fn send_encoded(
            &mut self,
            _now: SimTime,
            _route: WireRoute,
            _lane: WireLane,
            _bytes: Bytes,
        ) -> Result<(), super::super::leg_io::EncodedLegTransportError> {
            panic!("client recovery submissions must retain ordered delivery authority")
        }

        fn send_attach_ordered_encoded(
            &mut self,
            _now: SimTime,
            _route: WireRoute,
            _lane: WireLane,
            bytes: Bytes,
            completion: super::super::leg_io::OrderedDeliveryToken,
        ) -> Result<(), super::super::leg_io::EncodedLegTransportError> {
            if self.rejected.is_none() {
                self.rejected = Some(bytes);
                return Err(super::super::leg_io::EncodedLegTransportError::Wire(
                    super::super::two_leg::WireError::LiveMessageCapacityExceeded,
                ));
            }
            self.accepted = Some(bytes);
            completion.mark_delivered();
            Ok(())
        }
    }

    #[test]
    fn registered_b_transport_pressure_retries_the_same_preencoded_backing() {
        let mut controller = initial_controller();
        let factory = controller
            .supervisor
            .mint_tcp_port_factory(FlowPortConfig::new(64, 8, 8, 8).unwrap())
            .unwrap();
        let open = controller
            .current
            .as_ref()
            .unwrap()
            .authority
            .local_open_event(target());
        let opened = controller.supervisor.apply_event(open).unwrap();
        let local_flow = opened
            .into_iter()
            .find_map(|effect| match effect {
                SessionEffect::LocalFlowOpened { flow } => Some(flow),
                _ => None,
            })
            .unwrap();
        let open_result = Frame::try_new(
            controller.generation(),
            Record::OpenResult {
                flow_id: local_flow.flow_id(),
                result: OpenResultCode::Opened,
            },
        )
        .unwrap()
        .encode()
        .unwrap()
        .to_vec();
        let open_result = EncodedDelivery::from_memory_ingress(
            controller.current_inbound_route().unwrap(),
            WireLane::Control,
            open_result,
            1,
            0,
        )
        .unwrap();
        assert!(matches!(
            controller.receive_current(open_result, FeatureSet::new(FEATURES)),
            Ok(ClientInboundDisposition::Applied { .. })
        ));
        let (flow, mut driver) = factory.open_flow(local_flow).unwrap();
        flow.try_send_uplink_with(1, || Bytes::from_static(b"a"))
            .unwrap();
        controller
            .supervisor
            .apply_command(SessionOwnerCommand::Driver(
                driver.try_recv_next().unwrap().unwrap(),
            ))
            .unwrap();
        let AcceptedRegisteredClientTransportFixture {
            accepted,
            io,
            queue,
            reporter: _reporter,
        } = accepted_registered_client_transport_fixture(1);
        controller
            .begin_registered_replacement(accepted, io, queue)
            .unwrap();
        assert!(matches!(
            controller.drive_recovery_one(),
            ClientRecoveryDrive::Progress(ClientRecoveryObservation::LegActivated { .. })
        ));
        assert!(matches!(
            controller.drive_recovery_one(),
            ClientRecoveryDrive::AdmissionCompleted { .. }
        ));

        let mut transport = RejectOnceRetainingRecoveryTransport::default();
        assert!(matches!(
            controller.try_flush_current(&mut transport, SimTime::ZERO),
            Err(LegIoError::Transport(
                super::super::leg_io::EncodedLegTransportError::Wire(
                    super::super::two_leg::WireError::LiveMessageCapacityExceeded
                )
            ))
        ));
        assert!(
            controller
                .try_flush_current(&mut transport, SimTime::ZERO)
                .unwrap()
                .is_some()
        );

        let rejected = transport.rejected.as_ref().unwrap();
        let accepted = transport.accepted.as_ref().unwrap();
        assert_eq!(rejected, accepted);
        assert_eq!(
            rejected.as_ptr(),
            accepted.as_ptr(),
            "retry must retain the exact preflight-encoded backing without a post-commit copy"
        );
        drop((flow, factory));
    }

    #[test]
    fn registered_b_submission_is_not_actual_recovery_delivery() {
        let mut controller = initial_controller();
        let factory = controller
            .supervisor
            .mint_tcp_port_factory(FlowPortConfig::new(64, 8, 8, 8).unwrap())
            .unwrap();
        let open = controller
            .current
            .as_ref()
            .unwrap()
            .authority
            .local_open_event(target());
        let opened = controller.supervisor.apply_event(open).unwrap();
        let local_flow = opened
            .into_iter()
            .find_map(|effect| match effect {
                SessionEffect::LocalFlowOpened { flow } => Some(flow),
                _ => None,
            })
            .unwrap();
        let open_result = Frame::try_new(
            controller.generation(),
            Record::OpenResult {
                flow_id: local_flow.flow_id(),
                result: OpenResultCode::Opened,
            },
        )
        .unwrap()
        .encode()
        .unwrap()
        .to_vec();
        let open_result = EncodedDelivery::from_memory_ingress(
            controller.current_inbound_route().unwrap(),
            WireLane::Control,
            open_result,
            1,
            0,
        )
        .unwrap();
        assert!(matches!(
            controller.receive_current(open_result, FeatureSet::new(FEATURES)),
            Ok(ClientInboundDisposition::Applied { .. })
        ));
        let (flow, mut driver) = factory.open_flow(local_flow).unwrap();
        flow.try_send_uplink_with(1, || Bytes::from_static(b"a"))
            .unwrap();
        controller
            .supervisor
            .apply_command(SessionOwnerCommand::Driver(
                driver.try_recv_next().unwrap().unwrap(),
            ))
            .unwrap();
        assert_eq!(controller.snapshot().uplink_replay_extents, 1);

        let AcceptedRegisteredClientTransportFixture {
            accepted,
            io,
            queue,
            reporter: _reporter,
        } = accepted_registered_client_transport_fixture(1);
        let started = controller
            .begin_registered_replacement(accepted, io, queue)
            .unwrap();
        assert!(matches!(
            controller.drive_recovery_one(),
            ClientRecoveryDrive::Progress(ClientRecoveryObservation::LegActivated { .. })
        ));
        assert!(matches!(
            controller.drive_recovery_one(),
            ClientRecoveryDrive::AdmissionCompleted { .. }
        ));

        let route_b = controller.current_outbound_route().unwrap();
        let mut faults = MemoryFaultScript::new(1);
        faults
            .insert(0, route_b, WireLane::Data, FaultAction::Hold { token: 72 })
            .unwrap();
        let mut wire_b = memory_transport(faults);
        assert!(
            controller
                .try_flush_current(&mut wire_b.controller_sender(), SimTime::ZERO)
                .unwrap()
                .is_some()
        );

        let ClientControllerPhase::Active {
            recovery_tail: Some(tail),
        } = &controller.phase
        else {
            panic!("completed recovery admission must retain its delivery tail");
        };
        assert!(
            !tail.actual_delivery_complete(),
            "transport submission is not exact ordered delivery"
        );

        let AcceptedRegisteredClientTransportFixture {
            accepted,
            io,
            queue,
            reporter: _next_reporter,
        } = accepted_registered_client_transport_fixture(1);
        let failure = controller
            .begin_registered_replacement(accepted, io, queue)
            .unwrap_err();
        assert_eq!(
            failure.kind(),
            ClientRecoveryBeginErrorKind::PriorRecoveryDeliveryPending
        );
        drop(failure.into_parts());

        wire_b.release_hold(72, SimTime::ZERO).unwrap();
        release_due(&mut wire_b);
        let delivered = wire_b.try_recv_next(route_b).unwrap().unwrap();
        assert_data_delivery(delivered, 0, b"a");

        let ClientControllerPhase::Active {
            recovery_tail: Some(tail),
        } = &controller.phase
        else {
            panic!("exact delivery tail must remain until explicit settlement");
        };
        assert!(tail.actual_delivery_complete());
        assert_eq!(
            controller.prepare_normal_begin(),
            Err(ClientRecoveryBeginErrorKind::PriorRecoveryTailUnsettled)
        );
        assert_eq!(
            controller
                .settle_completed_recovery_tail_after_actor_preflight_for_test()
                .unwrap(),
            ClientRecoveryTailSettlement::Settled {
                generation: started.generation,
            }
        );
        assert_eq!(
            controller
                .settle_completed_recovery_tail_after_actor_preflight_for_test()
                .unwrap(),
            ClientRecoveryTailSettlement::AlreadySettled
        );
        assert_eq!(
            controller.prepare_normal_begin(),
            Err(ClientRecoveryBeginErrorKind::RetiredCapacityExceeded)
        );
        assert!(matches!(
            controller.phase,
            ClientControllerPhase::Active {
                recovery_tail: None
            }
        ));
        drop((flow, factory));
    }

    #[test]
    fn recovering_current_inbound_exact_returns_before_decode_or_accounting() {
        let mut controller = initial_controller();
        let open = controller
            .current
            .as_ref()
            .unwrap()
            .authority
            .local_open_event(target());
        let opened = controller.supervisor.apply_event(open).unwrap();
        let local_flow = opened
            .into_iter()
            .find_map(|effect| match effect {
                SessionEffect::LocalFlowOpened { flow } => Some(flow),
                _ => None,
            })
            .unwrap();
        let AcceptedRegisteredClientTransportFixture {
            accepted,
            io,
            queue,
            reporter: _reporter,
        } = accepted_registered_client_transport_fixture(4);
        let started = controller
            .begin_registered_replacement(accepted, io, queue)
            .unwrap();
        let delivery = EncodedDelivery::from_memory_ingress(
            controller.current_inbound_route().unwrap(),
            WireLane::Control,
            Frame::try_new(
                started.generation,
                Record::OpenResult {
                    flow_id: local_flow.flow_id(),
                    result: OpenResultCode::Opened,
                },
            )
            .unwrap()
            .encode()
            .unwrap()
            .to_vec(),
            91,
            0,
        )
        .unwrap();
        let expected = delivery.clone();
        let before = controller.current.as_ref().unwrap().transport.counters();

        let returned = match controller.receive_current(delivery, FeatureSet::new(FEATURES)) {
            Ok(ClientInboundDisposition::Blocked(returned)) => returned,
            _ => panic!("recovering current inbound was not returned to its caller"),
        };
        assert_eq!(returned, expected);
        assert_eq!(
            controller.current.as_ref().unwrap().transport.counters(),
            before,
            "the recovery barrier must precede all LegIo decode and accounting"
        );

        assert!(matches!(
            controller.drive_recovery_one(),
            ClientRecoveryDrive::Progress(ClientRecoveryObservation::LegActivated {
                generation
            }) if generation == started.generation
        ));
        assert!(matches!(
            controller.drive_recovery_one(),
            ClientRecoveryDrive::AdmissionCompleted { generation }
                if generation == started.generation
        ));
        assert!(matches!(
            controller.receive_current(returned, FeatureSet::new(FEATURES)),
            Ok(ClientInboundDisposition::Applied { .. })
        ));
        let after = controller.current.as_ref().unwrap().transport.counters();
        assert_eq!(after.inbound_messages, before.inbound_messages + 1);
        assert!(after.inbound_bytes > before.inbound_bytes);
        assert_eq!(after.bound_messages, before.bound_messages + 1);
        assert!(after.bound_bytes > before.bound_bytes);
        assert_eq!(
            after.bound_session_messages,
            before.bound_session_messages + 1
        );
        assert_eq!(
            after.bound_leg_control_messages,
            before.bound_leg_control_messages
        );
    }

    #[test]
    fn discarded_current_retains_terminal_cleanup_authority_without_replaying_leg_loss() {
        let (mut controller, reporter) = initial_controller_with_reporter();
        let generation = controller.generation();
        let terminal = reporter
            .report(LegTransportTerminalReason::PeerClosed)
            .unwrap();

        let bound = controller.bind_current_terminal(terminal).unwrap();
        assert_eq!(bound.generation(), generation);
        assert_eq!(bound.reason(), LegTransportTerminalReason::PeerClosed);

        let discarded = controller.discard_terminating_current_recovery().unwrap();
        assert_eq!(discarded.generation(), generation);
        assert_eq!(discarded.reason(), LegTransportTerminalReason::PeerClosed);
        assert_eq!(
            discarded.effects(),
            &[SessionEffect::ResumeGraceStarted { generation }]
        );
        let snapshot_after_loss = controller.snapshot();
        let ClientControllerPhase::TerminatingCurrent(
            TerminatingCurrentClientTransport::RecoveryDiscarded { authority, .. },
        ) = &controller.phase
        else {
            panic!("discard must retain the terminal-bound current cleanup authority");
        };
        assert_eq!(authority.generation(), generation);
        assert_eq!(authority.reason(), LegTransportTerminalReason::PeerClosed);

        assert_eq!(
            controller
                .discard_terminating_current_recovery()
                .unwrap_err(),
            ClientCurrentTerminalDiscardErrorKind::AlreadyDiscarded
        );
        assert_eq!(controller.snapshot(), snapshot_after_loss);
        let ClientControllerPhase::TerminatingCurrent(
            TerminatingCurrentClientTransport::RecoveryDiscarded { authority, .. },
        ) = &controller.phase
        else {
            panic!("replay rejection must preserve terminal cleanup authority");
        };
        assert_eq!(authority.generation(), generation);
        assert_eq!(authority.reason(), LegTransportTerminalReason::PeerClosed);
    }

    #[test]
    fn recovering_b_exact_terminal_requires_explicit_recovery_discard_before_one_leg_loss() {
        let mut controller = initial_controller();
        let factory = controller
            .supervisor
            .mint_tcp_port_factory(FlowPortConfig::new(64, 8, 8, 8).unwrap())
            .unwrap();
        let opened = controller.open_local(target()).unwrap().unwrap();
        let local_flow = opened
            .into_iter()
            .find_map(|effect| match effect {
                SessionEffect::LocalFlowOpened { flow } => Some(flow),
                _ => None,
            })
            .unwrap();
        let open_result = EncodedDelivery::from_memory_ingress(
            controller.current_inbound_route().unwrap(),
            WireLane::Control,
            Frame::try_new(
                controller.generation(),
                Record::OpenResult {
                    flow_id: local_flow.flow_id(),
                    result: OpenResultCode::Opened,
                },
            )
            .unwrap()
            .encode()
            .unwrap()
            .to_vec(),
            401,
            0,
        )
        .unwrap();
        assert!(matches!(
            controller.receive_current(open_result, FeatureSet::new(FEATURES)),
            Ok(ClientInboundDisposition::Applied { .. })
        ));
        let (flow, mut driver) = factory.open_flow(local_flow).unwrap();
        for payload in [b"a".as_slice(), b"b".as_slice(), b"c".as_slice()] {
            flow.try_send_uplink_with(payload.len(), || Bytes::copy_from_slice(payload))
                .unwrap();
            controller
                .supervisor
                .apply_command(SessionOwnerCommand::Driver(
                    driver.try_recv_next().unwrap().unwrap(),
                ))
                .unwrap();
        }

        let generation_a = controller.generation();
        let route_a = controller.current_inbound_route().unwrap();
        let AcceptedRegisteredClientTransportFixture {
            accepted,
            io,
            queue,
            reporter: reporter_b,
        } = accepted_registered_client_transport_fixture(2);
        let started = controller
            .begin_registered_replacement(accepted, io, queue)
            .unwrap();
        assert!(matches!(
            controller.drive_recovery_one(),
            ClientRecoveryDrive::Progress(ClientRecoveryObservation::LegActivated {
                generation
            }) if generation == started.generation
        ));
        assert!(matches!(
            controller.drive_recovery_one(),
            ClientRecoveryDrive::Progress(ClientRecoveryObservation::FrameAdmitted)
        ));
        assert!(matches!(
            controller.drive_recovery_one(),
            ClientRecoveryDrive::Progress(ClientRecoveryObservation::FrameAdmitted)
        ));

        let route_b = controller.current_outbound_route().unwrap();
        let mut faults = MemoryFaultScript::new(1);
        faults
            .insert(0, route_b, WireLane::Data, FaultAction::Hold { token: 81 })
            .unwrap();
        let mut wire_b = memory_transport(faults);
        assert!(
            controller
                .try_flush_current(&mut wire_b.controller_sender(), SimTime::ZERO)
                .unwrap()
                .is_some()
        );
        assert!(matches!(
            controller.drive_recovery_one(),
            ClientRecoveryDrive::Pressure
        ));
        assert!(matches!(
            controller.drive_recovery_one(),
            ClientRecoveryDrive::AwaitingFlush
        ));
        assert_eq!(
            controller.snapshot().session.phase(),
            SessionPhase::Active,
            "queue pressure cannot mint terminal authority or apply LegLost"
        );

        let cursor_before = match &controller.phase {
            ClientControllerPhase::Recovering(recovery) => (
                recovery.generation,
                recovery.effects.len(),
                recovery.pending.is_some(),
                recovery.drive_ready,
            ),
            _ => panic!("B must still own its pressure-blocked recovery cursor"),
        };
        let snapshot_before = controller.snapshot();
        let budget_before = controller.recovery_budget_snapshot().unwrap();
        let queue_before = (
            controller.current_queued_frames().unwrap(),
            controller.current_queued_bytes().unwrap(),
        );
        let counters_before = controller.current_counters().unwrap();
        let retired_before = (
            controller.retired_inbound_route(),
            controller.retired.as_ref().unwrap().authority.generation(),
        );
        assert!(budget_before.reserved_outbound_messages > 0);
        assert!(queue_before.0 > 0 && queue_before.1 > 0);

        let (wrong_io, wrong_reporter) = LegIo::for_authenticated_transport_with_reporter(
            LegId::A,
            LegEndpointRole::Client,
            binding(0x59),
            limits(),
        );
        let wrong_terminal = wrong_reporter
            .report(LegTransportTerminalReason::PeerClosed)
            .unwrap();
        let failure = controller
            .bind_current_terminal(wrong_terminal)
            .unwrap_err();
        assert_eq!(
            failure.kind(),
            ClientCurrentTerminalBindErrorKind::WrongLegTerminal
        );
        let wrong_terminal = failure.into_terminal();
        assert_eq!(
            wrong_terminal.reason(),
            LegTransportTerminalReason::PeerClosed
        );
        assert_eq!(controller.snapshot(), snapshot_before);
        assert_eq!(controller.recovery_budget_snapshot(), Some(budget_before));
        assert_eq!(
            (
                controller.current_queued_frames(),
                controller.current_queued_bytes(),
            ),
            (Some(queue_before.0), Some(queue_before.1))
        );
        assert_eq!(controller.current_counters(), Some(counters_before));
        assert_eq!(
            (
                controller.retired_inbound_route(),
                controller.retired.as_ref().unwrap().authority.generation(),
            ),
            retired_before
        );
        assert_eq!(
            match &controller.phase {
                ClientControllerPhase::Recovering(recovery) => (
                    recovery.generation,
                    recovery.effects.len(),
                    recovery.pending.is_some(),
                    recovery.drive_ready,
                ),
                _ => panic!("wrong terminal must preserve the exact recovery cursor"),
            },
            cursor_before
        );

        let exact_terminal = reporter_b
            .report(LegTransportTerminalReason::FatalIo)
            .unwrap();
        let terminalized = controller.bind_current_terminal(exact_terminal).unwrap();
        assert_eq!(terminalized.generation(), started.generation);
        assert_eq!(terminalized.reason(), LegTransportTerminalReason::FatalIo);
        assert_eq!(controller.snapshot(), snapshot_before);
        assert_eq!(controller.recovery_budget_snapshot(), Some(budget_before));
        assert_eq!(
            (
                controller.current_queued_frames(),
                controller.current_queued_bytes(),
            ),
            (Some(queue_before.0), Some(queue_before.1)),
            "terminal binding must not discard queued recovery ownership"
        );
        assert_eq!(
            (
                controller.retired_inbound_route(),
                controller.retired.as_ref().unwrap().authority.generation(),
            ),
            retired_before
        );
        assert!(matches!(
            controller.phase,
            ClientControllerPhase::TerminatingCurrent(
                TerminatingCurrentClientTransport::RecoveryPending {
                    recovery: TerminatingCurrentRecoveryOwnership::Installed(_),
                    ..
                }
            )
        ));
        assert!(matches!(
            controller.receive_retired(
                ack_delivery(route_a, generation_a, local_flow.flow_id(), 0, 402),
                FeatureSet::new(FEATURES),
            ),
            Ok(ClientInboundDisposition::Retired(
                RetiredClientInboundKind::Ack
            ))
        ));

        let discarded = controller.discard_terminating_current_recovery().unwrap();
        assert_eq!(discarded.generation(), started.generation);
        assert_eq!(discarded.reason(), LegTransportTerminalReason::FatalIo);
        assert_eq!(
            discarded.effects(),
            &[SessionEffect::ResumeGraceStarted {
                generation: started.generation,
            }]
        );
        assert_eq!(controller.snapshot().session.phase(), SessionPhase::Legless);
        assert_eq!(controller.recovery_budget_snapshot(), None);
        assert_eq!(controller.current_queued_frames(), None);
        assert_eq!(controller.current_queued_bytes(), None);
        assert_eq!(
            (
                controller.retired_inbound_route(),
                controller.retired.as_ref().unwrap().authority.generation(),
            ),
            (Some(route_a), generation_a),
            "terminal B must not overwrite the sole retired-A slot"
        );
        assert!(matches!(
            controller.phase,
            ClientControllerPhase::TerminatingCurrent(
                TerminatingCurrentClientTransport::RecoveryDiscarded { .. }
            )
        ));
        assert!(matches!(
            controller.receive_retired(
                ack_delivery(route_a, generation_a, local_flow.flow_id(), 0, 403),
                FeatureSet::new(FEATURES),
            ),
            Ok(ClientInboundDisposition::Retired(
                RetiredClientInboundKind::Ack
            ))
        ));
        assert_eq!(
            controller
                .discard_terminating_current_recovery()
                .unwrap_err(),
            ClientCurrentTerminalDiscardErrorKind::AlreadyDiscarded,
            "the same PendingClientLegLoss cannot be applied twice"
        );
        assert_eq!(controller.snapshot().session.phase(), SessionPhase::Legless);
        drop((flow, factory, wrong_io, wrong_terminal, wire_b));
    }

    #[test]
    fn active_b_held_recovery_tail_requires_explicit_discard_before_one_leg_loss() {
        let mut controller = initial_controller();
        let factory = controller
            .supervisor
            .mint_tcp_port_factory(FlowPortConfig::new(64, 8, 8, 8).unwrap())
            .unwrap();
        let opened = controller.open_local(target()).unwrap().unwrap();
        let local_flow = opened
            .into_iter()
            .find_map(|effect| match effect {
                SessionEffect::LocalFlowOpened { flow } => Some(flow),
                _ => None,
            })
            .unwrap();
        let open_result = EncodedDelivery::from_memory_ingress(
            controller.current_inbound_route().unwrap(),
            WireLane::Control,
            Frame::try_new(
                controller.generation(),
                Record::OpenResult {
                    flow_id: local_flow.flow_id(),
                    result: OpenResultCode::Opened,
                },
            )
            .unwrap()
            .encode()
            .unwrap()
            .to_vec(),
            411,
            0,
        )
        .unwrap();
        assert!(matches!(
            controller.receive_current(open_result, FeatureSet::new(FEATURES)),
            Ok(ClientInboundDisposition::Applied { .. })
        ));
        let (flow, mut driver) = factory.open_flow(local_flow).unwrap();
        flow.try_send_uplink_with(1, || Bytes::from_static(b"t"))
            .unwrap();
        controller
            .supervisor
            .apply_command(SessionOwnerCommand::Driver(
                driver.try_recv_next().unwrap().unwrap(),
            ))
            .unwrap();

        let generation_a = controller.generation();
        let route_a = controller.current_inbound_route().unwrap();
        let AcceptedRegisteredClientTransportFixture {
            accepted,
            io,
            queue,
            reporter: reporter_b,
        } = accepted_registered_client_transport_fixture(1);
        let started = controller
            .begin_registered_replacement(accepted, io, queue)
            .unwrap();
        assert!(matches!(
            controller.drive_recovery_one(),
            ClientRecoveryDrive::Progress(ClientRecoveryObservation::LegActivated {
                generation
            }) if generation == started.generation
        ));
        assert!(matches!(
            controller.drive_recovery_one(),
            ClientRecoveryDrive::AdmissionCompleted { generation }
                if generation == started.generation
        ));

        let route_b = controller.current_outbound_route().unwrap();
        let mut faults = MemoryFaultScript::new(1);
        faults
            .insert(0, route_b, WireLane::Data, FaultAction::Hold { token: 82 })
            .unwrap();
        let mut wire_b = memory_transport(faults);
        assert!(
            controller
                .try_flush_current(&mut wire_b.controller_sender(), SimTime::ZERO)
                .unwrap()
                .is_some()
        );
        let ClientControllerPhase::Active {
            recovery_tail: Some(tail),
        } = &controller.phase
        else {
            panic!("B must retain its admitted recovery tail");
        };
        assert!(!tail.actual_delivery_complete());

        let snapshot_before = controller.snapshot();
        let budget_before = controller.recovery_budget_snapshot().unwrap();
        let queue_before = (
            controller.current_queued_frames().unwrap(),
            controller.current_queued_bytes().unwrap(),
        );
        let retired_before = (
            controller.retired_inbound_route(),
            controller.retired.as_ref().unwrap().authority.generation(),
        );
        assert_eq!(budget_before, ClientRecoveryBudgetSnapshot::default());
        assert_eq!(
            controller.prepare_normal_begin(),
            Err(ClientRecoveryBeginErrorKind::PriorRecoveryDeliveryPending),
            "zero submit budget is not ordered recovery delivery"
        );

        let (wrong_io, wrong_reporter) = LegIo::for_authenticated_transport_with_reporter(
            LegId::A,
            LegEndpointRole::Client,
            binding(0x5a),
            limits(),
        );
        let wrong_terminal = wrong_reporter
            .report(LegTransportTerminalReason::Reset)
            .unwrap();
        let failure = controller
            .bind_current_terminal(wrong_terminal)
            .unwrap_err();
        assert_eq!(
            failure.kind(),
            ClientCurrentTerminalBindErrorKind::WrongLegTerminal
        );
        let wrong_terminal = failure.into_terminal();
        assert_eq!(wrong_terminal.reason(), LegTransportTerminalReason::Reset);
        assert_eq!(controller.snapshot(), snapshot_before);
        assert_eq!(controller.recovery_budget_snapshot(), Some(budget_before));
        assert_eq!(
            (
                controller.current_queued_frames(),
                controller.current_queued_bytes(),
            ),
            (Some(queue_before.0), Some(queue_before.1))
        );
        assert_eq!(
            (
                controller.retired_inbound_route(),
                controller.retired.as_ref().unwrap().authority.generation(),
            ),
            retired_before
        );
        let ClientControllerPhase::Active {
            recovery_tail: Some(tail),
        } = &controller.phase
        else {
            panic!("wrong terminal must restore the same active recovery tail");
        };
        assert!(!tail.actual_delivery_complete());

        let exact_terminal = reporter_b
            .report(LegTransportTerminalReason::PeerClosed)
            .unwrap();
        let terminalized = controller.bind_current_terminal(exact_terminal).unwrap();
        assert_eq!(terminalized.generation(), started.generation);
        assert_eq!(
            terminalized.reason(),
            LegTransportTerminalReason::PeerClosed
        );
        assert_eq!(controller.snapshot(), snapshot_before);
        assert_eq!(controller.recovery_budget_snapshot(), Some(budget_before));
        assert_eq!(
            (
                controller.current_queued_frames(),
                controller.current_queued_bytes(),
            ),
            (Some(queue_before.0), Some(queue_before.1))
        );
        assert_eq!(
            (
                controller.retired_inbound_route(),
                controller.retired.as_ref().unwrap().authority.generation(),
            ),
            retired_before
        );
        let ClientControllerPhase::TerminatingCurrent(
            TerminatingCurrentClientTransport::RecoveryPending {
                recovery: TerminatingCurrentRecoveryOwnership::Tail(tail),
                ..
            },
        ) = &controller.phase
        else {
            panic!("exact active terminal must retain the held tail in TerminatingCurrent");
        };
        assert!(!tail.actual_delivery_complete());

        let discarded = controller.discard_terminating_current_recovery().unwrap();
        assert_eq!(discarded.generation(), started.generation);
        assert_eq!(
            discarded.effects(),
            &[SessionEffect::ResumeGraceStarted {
                generation: started.generation,
            }]
        );
        assert_eq!(controller.snapshot().session.phase(), SessionPhase::Legless);
        assert_eq!(controller.recovery_budget_snapshot(), None);
        assert_eq!(
            (
                controller.retired_inbound_route(),
                controller.retired.as_ref().unwrap().authority.generation(),
            ),
            (Some(route_a), generation_a)
        );
        assert_eq!(
            controller
                .discard_terminating_current_recovery()
                .unwrap_err(),
            ClientCurrentTerminalDiscardErrorKind::AlreadyDiscarded
        );
        assert_eq!(controller.snapshot().session.phase(), SessionPhase::Legless);
        drop((flow, factory, wrong_io, wrong_terminal, wire_b));
    }

    #[test]
    fn active_b_without_recovery_ownership_requires_explicit_discard_before_one_leg_loss() {
        let mut controller = initial_controller();
        let generation_a = controller.generation();
        let route_a = controller.current_inbound_route().unwrap();
        let AcceptedRegisteredClientTransportFixture {
            accepted,
            io,
            queue,
            reporter: reporter_b,
        } = accepted_registered_client_transport_fixture(2);
        let started = controller
            .begin_registered_replacement(accepted, io, queue)
            .unwrap();
        assert!(matches!(
            controller.drive_recovery_one(),
            ClientRecoveryDrive::Progress(ClientRecoveryObservation::LegActivated {
                generation
            }) if generation == started.generation
        ));
        assert!(matches!(
            controller.drive_recovery_one(),
            ClientRecoveryDrive::AdmissionCompleted { generation }
                if generation == started.generation
        ));
        let ClientControllerPhase::Active {
            recovery_tail: Some(tail),
        } = &controller.phase
        else {
            panic!("idle B must first publish its completed recovery tail");
        };
        assert!(tail.actual_delivery_complete());
        assert_eq!(
            controller.prepare_normal_begin(),
            Err(ClientRecoveryBeginErrorKind::PriorRecoveryTailUnsettled)
        );
        assert_eq!(
            controller
                .settle_completed_recovery_tail_after_actor_preflight_for_test()
                .unwrap(),
            ClientRecoveryTailSettlement::Settled {
                generation: started.generation,
            }
        );
        assert_eq!(
            controller
                .settle_completed_recovery_tail_after_actor_preflight_for_test()
                .unwrap(),
            ClientRecoveryTailSettlement::AlreadySettled
        );
        assert_eq!(
            controller.prepare_normal_begin(),
            Err(ClientRecoveryBeginErrorKind::RetiredCapacityExceeded)
        );
        assert!(matches!(
            controller.phase,
            ClientControllerPhase::Active {
                recovery_tail: None
            }
        ));

        controller
            .enqueue_current_frame(
                Frame::try_new(
                    started.generation,
                    Record::Reset {
                        flow_id: SessionFlowId::new(91).unwrap(),
                        reason: ResetReason::LocalAbandon,
                    },
                )
                .unwrap(),
            )
            .unwrap()
            .unwrap();
        let snapshot_before = controller.snapshot();
        let queue_before = (
            controller.current_queued_frames().unwrap(),
            controller.current_queued_bytes().unwrap(),
        );
        let counters_before = controller.current_counters().unwrap();
        let budget_before = controller.recovery_budget_snapshot().unwrap();
        let retired_before = (
            controller.retired_inbound_route(),
            controller.retired.as_ref().unwrap().authority.generation(),
        );
        assert_eq!(queue_before.0, 1);
        assert!(queue_before.1 > 0);
        assert_eq!(budget_before, ClientRecoveryBudgetSnapshot::default());

        let (wrong_io, wrong_reporter) = LegIo::for_authenticated_transport_with_reporter(
            LegId::A,
            LegEndpointRole::Client,
            binding(0x5b),
            limits(),
        );
        let wrong_terminal = wrong_reporter
            .report(LegTransportTerminalReason::FatalIo)
            .unwrap();
        let failure = controller
            .bind_current_terminal(wrong_terminal)
            .unwrap_err();
        assert_eq!(
            failure.kind(),
            ClientCurrentTerminalBindErrorKind::WrongLegTerminal
        );
        let wrong_terminal = failure.into_terminal();
        assert_eq!(wrong_terminal.reason(), LegTransportTerminalReason::FatalIo);
        assert_eq!(controller.snapshot(), snapshot_before);
        assert_eq!(controller.current_counters(), Some(counters_before));
        assert_eq!(controller.recovery_budget_snapshot(), Some(budget_before));
        assert_eq!(
            (
                controller.current_queued_frames(),
                controller.current_queued_bytes(),
            ),
            (Some(queue_before.0), Some(queue_before.1))
        );
        assert_eq!(
            (
                controller.retired_inbound_route(),
                controller.retired.as_ref().unwrap().authority.generation(),
            ),
            retired_before
        );
        assert!(matches!(
            controller.phase,
            ClientControllerPhase::Active {
                recovery_tail: None
            }
        ));

        let exact_terminal = reporter_b
            .report(LegTransportTerminalReason::Reset)
            .unwrap();
        let terminalized = controller.bind_current_terminal(exact_terminal).unwrap();
        assert_eq!(terminalized.generation(), started.generation);
        assert_eq!(terminalized.reason(), LegTransportTerminalReason::Reset);
        assert_eq!(controller.snapshot(), snapshot_before);
        assert_eq!(controller.current_counters(), Some(counters_before));
        assert_eq!(controller.recovery_budget_snapshot(), Some(budget_before));
        assert_eq!(
            (
                controller.current_queued_frames(),
                controller.current_queued_bytes(),
            ),
            (Some(queue_before.0), Some(queue_before.1)),
            "terminal binding must retain the ordinary queue until explicit discard"
        );
        assert_eq!(
            (
                controller.retired_inbound_route(),
                controller.retired.as_ref().unwrap().authority.generation(),
            ),
            retired_before
        );
        assert!(matches!(
            controller.phase,
            ClientControllerPhase::TerminatingCurrent(
                TerminatingCurrentClientTransport::RecoveryPending {
                    recovery: TerminatingCurrentRecoveryOwnership::NoRecovery,
                    ..
                }
            )
        ));

        let discarded = controller.discard_terminating_current_recovery().unwrap();
        assert_eq!(discarded.generation(), started.generation);
        assert_eq!(discarded.reason(), LegTransportTerminalReason::Reset);
        assert_eq!(
            discarded.effects(),
            &[SessionEffect::ResumeGraceStarted {
                generation: started.generation,
            }]
        );
        assert_eq!(controller.snapshot().session.phase(), SessionPhase::Legless);
        assert_eq!(controller.current_queued_frames(), None);
        assert_eq!(controller.current_queued_bytes(), None);
        assert_eq!(controller.recovery_budget_snapshot(), None);
        assert_eq!(
            (
                controller.retired_inbound_route(),
                controller.retired.as_ref().unwrap().authority.generation(),
            ),
            (Some(route_a), generation_a)
        );
        assert_eq!(
            controller
                .discard_terminating_current_recovery()
                .unwrap_err(),
            ClientCurrentTerminalDiscardErrorKind::AlreadyDiscarded
        );
        assert_eq!(controller.snapshot().session.phase(), SessionPhase::Legless);
        drop((wrong_io, wrong_terminal));
    }

    #[test]
    fn registered_b_pressure_recovers_once_on_its_first_positive_application_ack() {
        let mut controller = initial_controller();
        let factory = controller
            .supervisor
            .mint_tcp_port_factory(FlowPortConfig::new(64, 8, 8, 8).unwrap())
            .unwrap();
        let open = controller
            .current
            .as_ref()
            .unwrap()
            .authority
            .local_open_event(target());
        let opened = controller.supervisor.apply_event(open).unwrap();
        let local_flow = opened
            .into_iter()
            .find_map(|effect| match effect {
                SessionEffect::LocalFlowOpened { flow } => Some(flow),
                _ => None,
            })
            .unwrap();
        let open_result = Frame::try_new(
            controller.generation(),
            Record::OpenResult {
                flow_id: local_flow.flow_id(),
                result: OpenResultCode::Opened,
            },
        )
        .unwrap()
        .encode()
        .unwrap()
        .to_vec();
        let open_result = EncodedDelivery::from_memory_ingress(
            controller.current_inbound_route().unwrap(),
            WireLane::Control,
            open_result,
            1,
            0,
        )
        .unwrap();
        assert!(matches!(
            controller.receive_current(open_result, FeatureSet::new(FEATURES)),
            Ok(ClientInboundDisposition::Applied { .. })
        ));
        let (flow, mut driver) = factory.open_flow(local_flow).unwrap();
        for payload in [b"a".as_slice(), b"b".as_slice()] {
            flow.try_send_uplink_with(payload.len(), || Bytes::copy_from_slice(payload))
                .unwrap();
            controller
                .supervisor
                .apply_command(SessionOwnerCommand::Driver(
                    driver.try_recv_next().unwrap().unwrap(),
                ))
                .unwrap();
        }
        assert_eq!(controller.snapshot().uplink_replay_extents, 2);

        let generation_a = controller.generation();
        let route_a = controller.current_inbound_route().unwrap();
        let AcceptedRegisteredClientTransportFixture {
            accepted,
            io,
            queue,
            reporter: _reporter,
        } = accepted_registered_client_transport_fixture(1);
        let started = controller
            .begin_registered_replacement(accepted, io, queue)
            .unwrap();

        assert_eq!(generation_a.get(), 2);
        assert_eq!(started.generation.get(), 3);
        assert_eq!(controller.generation(), started.generation);
        assert_eq!(controller.retired_inbound_route(), Some(route_a));
        assert_eq!(
            controller.retired.as_ref().unwrap().authority.generation(),
            generation_a
        );
        assert!(controller.replacement_barrier_is_active());
        assert!(matches!(
            controller.drive_recovery_one(),
            ClientRecoveryDrive::Progress(ClientRecoveryObservation::LegActivated {
                generation
            }) if generation == started.generation
        ));
        assert!(matches!(
            controller.drive_recovery_one(),
            ClientRecoveryDrive::Progress(ClientRecoveryObservation::FrameAdmitted)
        ));

        let route_b_outbound = controller.current_outbound_route().unwrap();
        let route_b_inbound = controller.current_inbound_route().unwrap();
        let mut faults = MemoryFaultScript::new(1);
        faults
            .insert(
                0,
                route_b_outbound,
                WireLane::Data,
                FaultAction::Hold { token: 71 },
            )
            .unwrap();
        let mut wire_b = memory_transport(faults);
        assert!(
            controller
                .try_flush_current(&mut wire_b.controller_sender(), SimTime::ZERO)
                .unwrap()
                .is_some()
        );
        assert!(matches!(
            controller.drive_recovery_one(),
            ClientRecoveryDrive::Pressure
        ));
        assert!(matches!(
            controller.drive_recovery_one(),
            ClientRecoveryDrive::AwaitingFlush
        ));
        assert_eq!(controller.generation(), started.generation);
        assert!(controller.replacement_barrier_is_active());

        wire_b.release_hold(71, SimTime::ZERO).unwrap();
        release_due(&mut wire_b);
        let first = wire_b.try_recv_next(route_b_outbound).unwrap().unwrap();
        assert_data_delivery(first, 0, b"a");
        assert!(
            controller
                .try_flush_current(&mut wire_b.controller_sender(), SimTime::ZERO)
                .unwrap()
                .is_none(),
            "ordered F1 delivery may rearm recovery without another submission"
        );

        assert!(matches!(
            controller.drive_recovery_one(),
            ClientRecoveryDrive::AdmissionCompleted { generation }
                if generation == started.generation
        ));
        assert!(!controller.replacement_barrier_is_active());
        assert_eq!(controller.generation(), started.generation);
        assert!(
            controller
                .try_flush_current(&mut wire_b.controller_sender(), SimTime::ZERO)
                .unwrap()
                .is_some()
        );
        release_due(&mut wire_b);
        let second = wire_b.try_recv_next(route_b_outbound).unwrap().unwrap();
        assert_data_delivery(second, 1, b"b");
        assert_eq!(wire_b.remaining_fault_directives(), 0);
        assert_eq!(controller.generation(), started.generation);

        let ClientControllerPhase::Active {
            recovery_tail: Some(tail),
        } = &controller.phase
        else {
            panic!("recovery admission must retain the independent delivery fact");
        };
        assert!(tail.actual_delivery_complete());
        assert_eq!(controller.snapshot().uplink_replay_extents, 2);

        assert!(matches!(
            controller.receive_retired(
                ack_delivery(route_a, generation_a, local_flow.flow_id(), 1, 101),
                FeatureSet::new(FEATURES),
            ),
            Ok(ClientInboundDisposition::Retired(
                RetiredClientInboundKind::Ack
            ))
        ));
        assert_eq!(controller.snapshot().uplink_replay_extents, 2);

        assert!(matches!(
            controller.receive_current(
                ack_delivery(route_b_inbound, generation_a, local_flow.flow_id(), 1, 102),
                FeatureSet::new(FEATURES),
            ),
            Ok(ClientInboundDisposition::ReducerRejected(_))
        ));
        assert_eq!(controller.snapshot().uplink_replay_extents, 2);

        assert!(matches!(
            controller.receive_current(
                ack_delivery(
                    route_b_inbound,
                    started.generation,
                    local_flow.flow_id(),
                    0,
                    103,
                ),
                FeatureSet::new(FEATURES),
            ),
            Ok(ClientInboundDisposition::Applied {
                observation: None,
                ..
            })
        ));
        assert_eq!(controller.snapshot().uplink_replay_extents, 2);

        let recovered = controller
            .receive_current_with_features(
                ack_delivery(
                    route_b_inbound,
                    started.generation,
                    local_flow.flow_id(),
                    1,
                    104,
                ),
                FeatureSet::new(FEATURES),
                SimTime::from_nanos(777),
            )
            .unwrap();
        let ClientInboundDisposition::Applied {
            effects,
            observation:
                Some(ClientInboundObservation::ReplacementRecovered {
                    arrived_at,
                    session_id,
                    flow_id,
                    generation,
                    direction,
                    previous_accepted,
                    next_accepted,
                    released_bytes,
                    final_accepted,
                    ..
                }),
        } = recovered
        else {
            panic!("the first positive exact-B ACK did not complete replacement recovery");
        };
        assert_eq!(arrived_at, SimTime::from_nanos(777));
        assert!(
            !effects
                .iter()
                .any(|effect| matches!(effect, SessionEffect::ReplayAcknowledged { .. })),
            "the opaque reducer receipt must not escape through Applied"
        );
        assert_eq!(session_id, local_flow.session_id());
        assert_eq!(flow_id, local_flow.flow_id());
        assert_eq!(generation, started.generation);
        assert_eq!(direction, Direction::ClientToTarget);
        assert_eq!(previous_accepted, ByteOffset::new(0));
        assert_eq!(next_accepted, ByteOffset::new(1));
        assert_eq!(released_bytes, 1);
        assert!(!final_accepted);
        assert_eq!(controller.snapshot().uplink_replay_extents, 1);

        assert!(matches!(
            controller.receive_current(
                ack_delivery(
                    route_b_inbound,
                    started.generation,
                    local_flow.flow_id(),
                    1,
                    105,
                ),
                FeatureSet::new(FEATURES),
            ),
            Ok(ClientInboundDisposition::Applied {
                observation: None,
                ..
            })
        ));
        assert!(matches!(
            controller.receive_current(
                ack_delivery(
                    route_b_inbound,
                    started.generation,
                    local_flow.flow_id(),
                    2,
                    106,
                ),
                FeatureSet::new(FEATURES),
            ),
            Ok(ClientInboundDisposition::Applied {
                observation: Some(ClientInboundObservation::ApplicationAckAdvanced {
                    flow_id,
                    generation,
                    direction: Direction::ClientToTarget,
                    previous_accepted,
                    next_accepted,
                    ..
                }),
                ..
            }) if flow_id == local_flow.flow_id()
                && generation == started.generation
                && previous_accepted == ByteOffset::new(1)
                && next_accepted == ByteOffset::new(2)
        ));
        assert_eq!(controller.snapshot().uplink_replay_extents, 0);
        drop((flow, factory));
    }

    #[test]
    fn retired_a_and_stale_current_b_late_record_matrix_is_inert() {
        let features = FeatureSet::new(FEATURES);
        let mut controller = initial_controller();
        let factory = controller
            .mint_tcp_port_factory(FlowPortConfig::new(64, 8, 8, 8).unwrap())
            .unwrap();
        let opened = controller.open_local(target()).unwrap().unwrap();
        let local_flow = opened
            .into_iter()
            .find_map(|effect| match effect {
                SessionEffect::LocalFlowOpened { flow } => Some(flow),
                _ => None,
            })
            .unwrap();
        let generation_a = controller.generation();
        let route_a = controller.current_inbound_route().unwrap();
        assert!(matches!(
            controller.receive_current(
                classified_session_delivery(
                    route_a,
                    generation_a,
                    Record::OpenResult {
                        flow_id: local_flow.flow_id(),
                        result: OpenResultCode::Opened,
                    },
                    1,
                ),
                features,
            ),
            Ok(ClientInboundDisposition::Applied { .. })
        ));
        let (flow, mut driver) = factory.open_flow(local_flow).unwrap();
        flow.try_send_uplink_with(1, || Bytes::from_static(b"r"))
            .unwrap();
        let input = driver
            .try_recv_next_for_client_controller()
            .unwrap()
            .unwrap();
        expect_driver_effects(&mut controller, input);
        assert_eq!(controller.snapshot().uplink_replay_extents, 1);
        assert_eq!(controller.snapshot().uplink_replay_owned_bytes, 1);

        let AcceptedRegisteredClientTransportFixture {
            accepted,
            io,
            queue,
            reporter: _reporter,
        } = accepted_registered_client_transport_fixture(1);
        let started_b = controller
            .begin_registered_replacement(accepted, io, queue)
            .unwrap();
        let generation_b = started_b.generation;
        let route_b_inbound = controller.current_inbound_route().unwrap();
        let route_b_outbound = controller.current_outbound_route().unwrap();
        assert_eq!(controller.retired_inbound_route(), Some(route_a));
        assert!(controller.replacement_barrier_is_active());
        assert!(matches!(
            controller.drive_recovery_one(),
            ClientRecoveryDrive::Progress(ClientRecoveryObservation::LegActivated {
                generation
            }) if generation == generation_b
        ));
        assert!(matches!(
            controller.drive_recovery_one(),
            ClientRecoveryDrive::AdmissionCompleted { generation }
                if generation == generation_b
        ));
        assert!(!controller.replacement_barrier_is_active());

        let stable_snapshot = controller.snapshot();
        let stable_replay_extents = stable_snapshot.uplink_replay_extents;
        let stable_replay_bytes = stable_snapshot.uplink_replay_owned_bytes;
        let stable_recovery_budget = controller.recovery_budget_snapshot();
        let stable_queued_frames = controller.current_queued_frames();
        let stable_queued_bytes = controller.current_queued_bytes();
        let current_counters_before_retired = controller.current_counters().unwrap();
        let data_plane_records = [
            (
                Record::Data {
                    flow_id: local_flow.flow_id(),
                    direction: Direction::TargetToClient,
                    offset: ByteOffset::new(0),
                    payload: Bytes::from_static(b"late"),
                },
                RetiredClientInboundKind::Data,
            ),
            (
                Record::Ack {
                    flow_id: local_flow.flow_id(),
                    direction: Direction::ClientToTarget,
                    next_accepted: ByteOffset::new(1),
                    final_accepted: false,
                },
                RetiredClientInboundKind::Ack,
            ),
            (
                Record::Close {
                    flow_id: local_flow.flow_id(),
                    direction: Direction::TargetToClient,
                    final_offset: ByteOffset::new(0),
                },
                RetiredClientInboundKind::Close,
            ),
            (
                Record::Reset {
                    flow_id: local_flow.flow_id(),
                    reason: ResetReason::TargetFailure,
                },
                RetiredClientInboundKind::Reset,
            ),
        ];

        let mut send_ordinal = 100;
        for (record, expected_kind) in &data_plane_records {
            for generation in [generation_a, generation_b] {
                assert!(matches!(
                    controller.receive_retired(
                        classified_session_delivery(
                            route_a,
                            generation,
                            record.clone(),
                            send_ordinal,
                        ),
                        features,
                    ),
                    Ok(ClientInboundDisposition::Retired(kind)) if kind == *expected_kind
                ));
                send_ordinal += 1;
                assert_eq!(controller.snapshot(), stable_snapshot);
                assert_eq!(controller.current_inbound_route(), Some(route_b_inbound));
                assert_eq!(controller.current_outbound_route(), Some(route_b_outbound));
                assert_eq!(controller.retired_inbound_route(), Some(route_a));
                assert!(!controller.replacement_barrier_is_active());
            }
        }
        assert_eq!(
            controller.current_counters(),
            Some(current_counters_before_retired),
            "retired-A classification cannot consume current-B counters",
        );

        let current_counters_before_stale = controller.current_counters().unwrap();
        for (record, _expected_kind) in &data_plane_records {
            assert!(matches!(
                controller.receive_current(
                    classified_session_delivery(
                        route_b_inbound,
                        generation_a,
                        record.clone(),
                        send_ordinal,
                    ),
                    features,
                ),
                Ok(ClientInboundDisposition::ReducerRejected(
                    SessionSupervisorError::Reducer(SessionError::FrameGenerationMismatch {
                        current,
                        actual,
                    })
                )) if current == generation_b.get() && actual == generation_a.get()
            ));
            send_ordinal += 1;
            assert_eq!(controller.snapshot(), stable_snapshot);
            assert!(!controller.replacement_barrier_is_active());
        }

        let attach_nonce = AttachNonce::new([0x71; 16]).unwrap();
        for (generation, record) in [
            (
                generation_b,
                Record::AttachAccepted {
                    session_id: local_flow.session_id(),
                    nonce: attach_nonce,
                    selected_version: SESSION_PROTOCOL_VERSION,
                    features,
                },
            ),
            (
                generation_a,
                Record::AttachGenerationStatus {
                    session_id: local_flow.session_id(),
                    requested_generation: generation_b,
                    nonce: attach_nonce,
                },
            ),
        ] {
            assert!(matches!(
                controller.receive_retired(
                    classified_session_delivery(route_a, generation, record.clone(), send_ordinal),
                    features,
                ),
                Ok(ClientInboundDisposition::Rejected)
            ));
            send_ordinal += 1;
            assert!(matches!(
                controller.receive_current(
                    classified_session_delivery(route_b_inbound, generation, record, send_ordinal),
                    features,
                ),
                Ok(ClientInboundDisposition::Rejected)
            ));
            send_ordinal += 1;
            assert_eq!(controller.snapshot(), stable_snapshot);
            assert!(!controller.replacement_barrier_is_active());
        }

        let stale_probe_ack = LegControlFrame::try_new(
            generation_a,
            LegControlRecord::LegProbeAck {
                session_id: local_flow.session_id(),
                leg_epoch_nonce: LegEpochNonce::new([0x72; 16]).unwrap(),
                probe_sequence: ProbeSequence::new(1).unwrap(),
            },
            features,
        )
        .unwrap()
        .encode(features)
        .unwrap();
        assert!(matches!(
            controller.receive_retired(
                EncodedDelivery::from_memory_ingress(
                    route_a,
                    WireLane::Control,
                    stale_probe_ack.to_vec(),
                    send_ordinal,
                    0,
                )
                .unwrap(),
                features,
            ),
            Ok(ClientInboundDisposition::Rejected)
        ));
        send_ordinal += 1;
        assert!(matches!(
            controller.receive_current(
                EncodedDelivery::from_memory_ingress(
                    route_b_inbound,
                    WireLane::Control,
                    stale_probe_ack.to_vec(),
                    send_ordinal,
                    0,
                )
                .unwrap(),
                features,
            ),
            Ok(ClientInboundDisposition::Rejected)
        ));
        send_ordinal += 1;

        assert_eq!(controller.snapshot(), stable_snapshot);
        assert_eq!(
            controller.snapshot().uplink_replay_extents,
            stable_replay_extents
        );
        assert_eq!(
            controller.snapshot().uplink_replay_owned_bytes,
            stable_replay_bytes
        );
        assert_eq!(
            controller.recovery_budget_snapshot(),
            stable_recovery_budget
        );
        assert_eq!(controller.current_queued_frames(), stable_queued_frames);
        assert_eq!(controller.current_queued_bytes(), stable_queued_bytes);
        assert_eq!(controller.current_inbound_route(), Some(route_b_inbound));
        assert_eq!(controller.current_outbound_route(), Some(route_b_outbound));
        assert_eq!(controller.retired_inbound_route(), Some(route_a));
        assert!(!controller.replacement_barrier_is_active());

        let current_counters_after_stale = controller.current_counters().unwrap();
        assert_eq!(
            current_counters_after_stale.inbound_messages,
            current_counters_before_stale.inbound_messages + 7,
        );
        assert_eq!(
            current_counters_after_stale.bound_messages,
            current_counters_before_stale.bound_messages + 7,
        );
        assert_eq!(
            current_counters_after_stale.bound_session_messages,
            current_counters_before_stale.bound_session_messages + 6,
        );
        assert_eq!(
            current_counters_after_stale.bound_leg_control_messages,
            current_counters_before_stale.bound_leg_control_messages + 1,
        );
        assert_eq!(
            current_counters_after_stale.inbound_bytes
                - current_counters_before_stale.inbound_bytes,
            current_counters_after_stale.bound_bytes - current_counters_before_stale.bound_bytes,
        );
        assert!(
            current_counters_after_stale.inbound_bytes
                > current_counters_before_stale.inbound_bytes
        );
        assert_eq!(
            current_counters_after_stale.wrong_route_rejections,
            current_counters_before_stale.wrong_route_rejections
        );
        assert_eq!(
            current_counters_after_stale.wrong_lane_rejections,
            current_counters_before_stale.wrong_lane_rejections
        );
        assert_eq!(
            current_counters_after_stale.decode_rejections,
            current_counters_before_stale.decode_rejections
        );
        assert_eq!(
            current_counters_after_stale.outbound_messages,
            current_counters_before_stale.outbound_messages
        );
        assert_eq!(
            current_counters_after_stale.outbound_bytes,
            current_counters_before_stale.outbound_bytes
        );
        assert_eq!(
            current_counters_after_stale.outbound_control_messages,
            current_counters_before_stale.outbound_control_messages
        );
        assert_eq!(
            current_counters_after_stale.outbound_data_messages,
            current_counters_before_stale.outbound_data_messages
        );

        // `ReplacementRecovered` is the observable proof that none of the
        // rejected late records consumed the opaque pending-ACK witness.
        let recovered = controller
            .receive_current(
                classified_session_delivery(
                    route_b_inbound,
                    generation_b,
                    Record::Ack {
                        flow_id: local_flow.flow_id(),
                        direction: Direction::ClientToTarget,
                        next_accepted: ByteOffset::new(1),
                        final_accepted: false,
                    },
                    send_ordinal,
                ),
                features,
            )
            .unwrap();
        assert!(matches!(
            recovered,
            ClientInboundDisposition::Applied {
                observation: Some(ClientInboundObservation::ReplacementRecovered {
                    generation,
                    previous_accepted,
                    next_accepted,
                    released_bytes: 1,
                    ..
                }),
                ..
            } if generation == generation_b
                && previous_accepted == ByteOffset::new(0)
                && next_accepted == ByteOffset::new(1)
        ));
        assert_eq!(controller.snapshot().uplink_replay_extents, 0);
        assert_eq!(controller.snapshot().uplink_replay_owned_bytes, 0);
        drop((flow, factory));
    }

    #[test]
    fn idle_b_replacement_does_not_arm_a_future_new_flow_ack_as_recovery() {
        let mut controller = initial_controller();
        let factory = controller
            .supervisor
            .mint_tcp_port_factory(FlowPortConfig::new(64, 8, 8, 8).unwrap())
            .unwrap();
        assert_eq!(controller.snapshot().uplink_replay_extents, 0);

        let AcceptedRegisteredClientTransportFixture {
            accepted,
            io,
            queue,
            reporter: _reporter,
        } = accepted_registered_client_transport_fixture(1);
        let started = controller
            .begin_registered_replacement(accepted, io, queue)
            .unwrap();
        assert!(matches!(
            controller.drive_recovery_one(),
            ClientRecoveryDrive::Progress(ClientRecoveryObservation::LegActivated {
                generation
            }) if generation == started.generation
        ));
        assert!(matches!(
            controller.drive_recovery_one(),
            ClientRecoveryDrive::AdmissionCompleted { generation }
                if generation == started.generation
        ));

        let opened = controller.open_local(target()).unwrap().unwrap();
        let local_flow = opened
            .into_iter()
            .find_map(|effect| match effect {
                SessionEffect::LocalFlowOpened { flow } => Some(flow),
                _ => None,
            })
            .unwrap();
        let open_result = EncodedDelivery::from_memory_ingress(
            controller.current_inbound_route().unwrap(),
            WireLane::Control,
            Frame::try_new(
                started.generation,
                Record::OpenResult {
                    flow_id: local_flow.flow_id(),
                    result: OpenResultCode::Opened,
                },
            )
            .unwrap()
            .encode()
            .unwrap()
            .to_vec(),
            201,
            0,
        )
        .unwrap();
        assert!(matches!(
            controller.receive_current(open_result, FeatureSet::new(FEATURES)),
            Ok(ClientInboundDisposition::Applied {
                observation: None,
                ..
            })
        ));

        let (flow, mut driver) = factory.open_flow(local_flow).unwrap();
        flow.try_send_uplink_with(1, || Bytes::from_static(b"n"))
            .unwrap();
        controller
            .supervisor
            .apply_command(SessionOwnerCommand::Driver(
                driver.try_recv_next().unwrap().unwrap(),
            ))
            .unwrap();
        assert_eq!(controller.snapshot().uplink_replay_extents, 1);

        assert!(matches!(
            controller.receive_current(
                ack_delivery(
                    controller.current_inbound_route().unwrap(),
                    started.generation,
                    local_flow.flow_id(),
                    1,
                    202,
                ),
                FeatureSet::new(FEATURES),
            ),
            Ok(ClientInboundDisposition::Applied {
                observation: Some(ClientInboundObservation::ApplicationAckAdvanced {
                    flow_id,
                    generation,
                    direction: Direction::ClientToTarget,
                    previous_accepted,
                    next_accepted,
                    ..
                }),
                ..
            }) if flow_id == local_flow.flow_id()
                && generation == started.generation
                && previous_accepted == ByteOffset::new(0)
                && next_accepted == ByteOffset::new(1)
        ));
        assert_eq!(controller.snapshot().uplink_replay_extents, 0);
        drop((flow, factory));
    }

    #[test]
    fn new_flow_ack_cannot_complete_an_old_flow_recovery_range() {
        let mut controller = initial_controller();
        let factory = controller
            .supervisor
            .mint_tcp_port_factory(FlowPortConfig::new(64, 8, 8, 8).unwrap())
            .unwrap();

        let old_opened = controller.open_local(target()).unwrap().unwrap();
        let old_local_flow = old_opened
            .into_iter()
            .find_map(|effect| match effect {
                SessionEffect::LocalFlowOpened { flow } => Some(flow),
                _ => None,
            })
            .unwrap();
        let old_open_result = EncodedDelivery::from_memory_ingress(
            controller.current_inbound_route().unwrap(),
            WireLane::Control,
            Frame::try_new(
                controller.generation(),
                Record::OpenResult {
                    flow_id: old_local_flow.flow_id(),
                    result: OpenResultCode::Opened,
                },
            )
            .unwrap()
            .encode()
            .unwrap()
            .to_vec(),
            211,
            0,
        )
        .unwrap();
        assert!(matches!(
            controller.receive_current(old_open_result, FeatureSet::new(FEATURES)),
            Ok(ClientInboundDisposition::Applied { .. })
        ));
        let (old_flow, mut old_driver) = factory.open_flow(old_local_flow).unwrap();
        old_flow
            .try_send_uplink_with(1, || Bytes::from_static(b"o"))
            .unwrap();
        expect_driver_effects(
            &mut controller,
            old_driver
                .try_recv_next_for_client_controller()
                .unwrap()
                .unwrap(),
        );

        let AcceptedRegisteredClientTransportFixture {
            accepted,
            io,
            queue,
            reporter: _reporter,
        } = accepted_registered_client_transport_fixture(1);
        let started = controller
            .begin_registered_replacement(accepted, io, queue)
            .unwrap();
        assert!(matches!(
            controller.drive_recovery_one(),
            ClientRecoveryDrive::Progress(ClientRecoveryObservation::LegActivated {
                generation
            }) if generation == started.generation
        ));
        assert!(matches!(
            controller.drive_recovery_one(),
            ClientRecoveryDrive::AdmissionCompleted { generation }
                if generation == started.generation
        ));

        let route_b_outbound = controller.current_outbound_route().unwrap();
        let route_b_inbound = controller.current_inbound_route().unwrap();
        let mut wire_b = memory_transport(MemoryFaultScript::new(0));
        assert!(
            controller
                .try_flush_current(&mut wire_b.controller_sender(), SimTime::ZERO)
                .unwrap()
                .is_some()
        );
        release_due(&mut wire_b);
        assert_data_delivery(
            wire_b.try_recv_next(route_b_outbound).unwrap().unwrap(),
            0,
            b"o",
        );

        let new_opened = controller.open_local(target()).unwrap().unwrap();
        let new_local_flow = new_opened
            .into_iter()
            .find_map(|effect| match effect {
                SessionEffect::LocalFlowOpened { flow } => Some(flow),
                _ => None,
            })
            .unwrap();
        let new_open_result = EncodedDelivery::from_memory_ingress(
            route_b_inbound,
            WireLane::Control,
            Frame::try_new(
                started.generation,
                Record::OpenResult {
                    flow_id: new_local_flow.flow_id(),
                    result: OpenResultCode::Opened,
                },
            )
            .unwrap()
            .encode()
            .unwrap()
            .to_vec(),
            212,
            0,
        )
        .unwrap();
        assert!(matches!(
            controller.receive_current(new_open_result, FeatureSet::new(FEATURES)),
            Ok(ClientInboundDisposition::Applied { .. })
        ));
        let (new_flow, mut new_driver) = factory.open_flow(new_local_flow).unwrap();
        new_flow
            .try_send_uplink_with(1, || Bytes::from_static(b"n"))
            .unwrap();
        let new_data_effects = expect_driver_effects(
            &mut controller,
            new_driver
                .try_recv_next_for_client_controller()
                .unwrap()
                .unwrap(),
        );
        for effect in new_data_effects {
            let SessionEffect::Transmit(frame) = effect else {
                panic!("new B flow DATA produced a non-wire effect");
            };
            controller.enqueue_current_frame(frame).unwrap().unwrap();
        }
        assert!(
            controller
                .try_flush_current(&mut wire_b.controller_sender(), SimTime::ZERO)
                .unwrap()
                .is_some()
        );
        release_due(&mut wire_b);
        assert_data_delivery(
            wire_b.try_recv_next(route_b_outbound).unwrap().unwrap(),
            0,
            b"n",
        );
        assert_eq!(controller.snapshot().uplink_replay_extents, 2);

        assert!(matches!(
            controller.receive_current(
                ack_delivery(
                    route_b_inbound,
                    started.generation,
                    new_local_flow.flow_id(),
                    1,
                    213,
                ),
                FeatureSet::new(FEATURES),
            ),
            Ok(ClientInboundDisposition::Applied {
                observation: Some(ClientInboundObservation::ApplicationAckAdvanced {
                    flow_id,
                    generation,
                    direction: Direction::ClientToTarget,
                    previous_accepted,
                    next_accepted,
                    ..
                }),
                ..
            }) if flow_id == new_local_flow.flow_id()
                && generation == started.generation
                && previous_accepted == ByteOffset::new(0)
                && next_accepted == ByteOffset::new(1)
        ));
        assert_eq!(controller.snapshot().uplink_replay_extents, 1);

        let recovered = controller
            .receive_current(
                ack_delivery(
                    route_b_inbound,
                    started.generation,
                    old_local_flow.flow_id(),
                    1,
                    214,
                ),
                FeatureSet::new(FEATURES),
            )
            .unwrap();
        assert!(matches!(
            recovered,
            ClientInboundDisposition::Applied {
                observation: Some(ClientInboundObservation::ReplacementRecovered {
                    flow_id,
                    previous_accepted,
                    next_accepted,
                    ..
                }),
                ..
            } if flow_id == old_local_flow.flow_id()
                && previous_accepted == ByteOffset::new(0)
                && next_accepted == ByteOffset::new(1)
        ));
        assert_eq!(controller.snapshot().uplink_replay_extents, 0);
        drop((old_flow, old_driver, new_flow, new_driver, factory));
    }
}
