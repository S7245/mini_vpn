//! Single-owner transaction boundary for one resumable session.
//!
//! The supervisor is the only adapter layer allowed to split an admitted
//! [`super::tcp::UplinkData`] into a reducer event and its byte ownership. It consumes the
//! reducer's opaque replay receipts before returning effects to transport or
//! TUN adapters, so staging permits cannot be released by a merely well-formed
//! wire ACK.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use thiserror::Error;

use crate::resumable::{
    AttachAuthority, AttachNonce, AttachTransportBinding, AuthenticatedAttachStatus, ByteOffset,
    Direction, Frame, LegGeneration, SessionConfig, SessionEffect, SessionError, SessionEvent,
    SessionFlowId, SessionModel, SessionPhase, SessionRole, SessionSnapshot,
};

#[cfg(test)]
use super::leg::CaughtUpAttachedLeg;
use super::leg::{
    AttachedLeg, BoundInbound, EstablishedLeg, ExactLegTerminal, LegBoundFrame, LegProvenanceError,
    LegSeal, LegTerminalTransitionError, LegTransportTerminalReason, OwnerAttachTransaction,
};
use super::leg_io::{
    AcceptedInitialAttach, AcceptedInitialAttachActivationFailure, AttachAcceptanceEnqueued,
    AttachAcceptanceReservation, AttachAcceptanceReserveError, InitialAttachActivationError,
    LegOutboundQueue, LegOutboundQueueErrorKind,
};
use super::session::SessionOwnerCommand;
use super::standby::{ExactAuthenticatedStandby, OwnerRegisteredAttachGate};
use super::tcp::{
    DriverInput, FlowPortConfig, FlowPortConfigError, FlowPortError, ResumableTcpPortFactory,
    UplinkOwnership, UplinkReplayOwnership,
};

struct TcpFactoryOriginSeal;

/// Opaque identity shared only by the one factory minted for this supervisor.
/// The seal is private to this module, so sibling adapters can carry and
/// compare this capability but cannot manufacture another valid origin.
#[derive(Clone)]
pub(super) struct TcpFactoryOrigin(Arc<TcpFactoryOriginSeal>);

impl TcpFactoryOrigin {
    fn mint() -> Self {
        Self(Arc::new(TcpFactoryOriginSeal))
    }

    pub(super) fn matches(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct ReplayExtentKey {
    flow_id: SessionFlowId,
    direction: Direction,
    start: ByteOffset,
    end: ByteOffset,
}

impl ReplayExtentKey {
    fn of(ownership: &UplinkReplayOwnership) -> Self {
        Self {
            flow_id: ownership.flow_id(),
            direction: ownership.direction(),
            start: ownership.start(),
            end: ownership.end(),
        }
    }
}

/// Read-only proof that reducer replay and adapter byte ownership agree.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SessionSupervisorSnapshot {
    pub(crate) session: SessionSnapshot,
    pub(crate) uplink_replay_extents: usize,
    pub(crate) uplink_replay_owned_bytes: usize,
    pub(crate) poisoned: bool,
    pub(crate) quarantined_uplink_extents: usize,
    pub(crate) quarantined_uplink_owned_bytes: usize,
    pub(crate) owner_catch_up_permit: bool,
    pub(crate) owner_catch_up_permit_armed: bool,
}

enum QuarantinedUplinkOwnership {
    Staging(UplinkOwnership),
    Replay(UplinkReplayOwnership),
}

struct OwnerActiveContract {
    seal: LegSeal,
    generation: LegGeneration,
    nonce: AttachNonce,
    binding: AttachTransportBinding,
    selected_version: u16,
    negotiated_features: u64,
}

struct OwnerCatchUpPermitIdentity;

struct OwnerCatchUpPermit {
    identity: Arc<OwnerCatchUpPermitIdentity>,
    authenticated: AuthenticatedAttachStatus,
    status_seal: LegSeal,
    status_binding: AttachTransportBinding,
    armed: bool,
}

impl OwnerCatchUpPermit {
    fn same_contract(
        &self,
        authenticated: &AuthenticatedAttachStatus,
        leg: &EstablishedLeg,
    ) -> bool {
        self.authenticated.same_catch_up_contract(authenticated)
            && leg.belongs_to_transport(&self.status_seal)
            && leg.standby_transport_binding() == self.status_binding
    }

    fn permits(
        &self,
        active: &OwnerActiveContract,
        leg: &EstablishedLeg,
        received: &LegBoundFrame,
    ) -> bool {
        self.armed
            && !leg.belongs_to_transport(&self.status_seal)
            && leg.standby_transport_binding() != self.status_binding
            && !leg.belongs_to_transport(&active.seal)
            && leg.standby_transport_binding() != active.binding
            && received.belongs_to_transport(leg)
            && received.attach_request().is_ok_and(|request| {
                request.nonce() != active.nonce
                    && self
                        .authenticated
                        .permits_follow_up(&request, &leg.standby_transport_binding())
            })
    }

    fn matches_current(&self, snapshot: SessionSnapshot) -> bool {
        snapshot.phase() != SessionPhase::Expired
            && snapshot.generation() == self.authenticated.current_generation()
    }
}

/// Non-cloneable authority to arm exactly the bounded stale correlation that
/// produced a status response. It has no commit or frame construction API.
pub(super) struct OwnerCatchUpArm {
    identity: Arc<OwnerCatchUpPermitIdentity>,
}

/// Authenticated status-only publication. OwnerTarget must route its frame
/// through the exact status leg's dedicated ordered admission before the
/// optional catch-up arm can be consumed.
pub(super) struct AuthenticatedOwnerAttachStatus {
    status: Frame,
    arm: Option<OwnerCatchUpArm>,
}

impl AuthenticatedOwnerAttachStatus {
    pub(super) fn into_parts(self) -> (Frame, Option<OwnerCatchUpArm>) {
        (self.status, self.arm)
    }
}

impl fmt::Debug for AuthenticatedOwnerAttachStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthenticatedOwnerAttachStatus")
            .field("catch_up", &self.arm.is_some())
            .finish_non_exhaustive()
    }
}

impl OwnerActiveContract {
    fn from_attached(active: &AttachedLeg) -> Self {
        Self {
            seal: active.active_transport_seal(),
            generation: active.generation(),
            nonce: active.nonce(),
            binding: active.transport_binding(),
            selected_version: active.session_protocol_version(),
            negotiated_features: active.negotiated_features(),
        }
    }

    fn matches(&self, active: &AttachedLeg) -> bool {
        active.belongs_to_transport(&self.seal)
            && self.generation == active.generation()
            && self.nonce == active.nonce()
            && self.binding == active.transport_binding()
            && self.selected_version == active.session_protocol_version()
            && self.negotiated_features == active.negotiated_features()
    }
}

impl QuarantinedUplinkOwnership {
    fn bytes(&self) -> usize {
        match self {
            Self::Staging(ownership) => ownership.bytes(),
            Self::Replay(ownership) => ownership.bytes(),
        }
    }
}

/// Exact reducer byte capacities that bound owner Target reads before the sole
/// source factory is minted. Segment and session-global factory capacities are
/// always derived directly inside [`SessionSupervisor::mint_tcp_port_factory`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct OwnerTargetReplayCapacity {
    pub(crate) per_flow_bytes: usize,
    pub(crate) session_bytes: usize,
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub(crate) enum TcpPortFactoryMintError {
    #[error("this session supervisor already minted its TCP port factory")]
    AlreadyMinted,
    #[error("a poisoned session supervisor cannot mint a TCP port factory")]
    PoisonedSupervisor,
    #[error(
        "TCP port factory must be minted while the session is pristine: phase={phase:?}, flows={flow_count}, replay_bytes={replay_bytes}, replay_segments={replay_segments}, tombstones={terminal_tombstones}, next_local_flow={next_local_flow_id:?}, highest_peer_flow={highest_peer_flow_id}"
    )]
    SessionNotPristine {
        phase: SessionPhase,
        flow_count: usize,
        replay_bytes: usize,
        replay_segments: usize,
        terminal_tombstones: usize,
        next_local_flow_id: Option<u64>,
        highest_peer_flow_id: u64,
    },
    #[error(
        "TCP port per-flow byte capacity {requested} exceeds reducer send capacity {available}"
    )]
    PerFlowBytesExceedReducer { requested: usize, available: usize },
    #[error("TCP port factory capacity cannot represent the reducer limits: {0}")]
    InvalidDerivedCapacity(#[from] FlowPortConfigError),
}

/// Reducer-wide admission pressure for which retrying the exact same input is
/// both necessary and safe. Protocol violations and per-flow ownership errors
/// deliberately remain ordinary terminal errors.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ReducerAdmissionBlock {
    FlowCapacityExceeded { max: usize },
    ReceiveByteBudgetExceeded { attempted: usize, max: usize },
    ReceiveRangeBudgetExceeded { attempted: usize, max: usize },
    TerminalTombstoneCapacityExceeded { max: usize },
}

impl ReducerAdmissionBlock {
    fn from_error(error: &SessionError) -> Option<Self> {
        match error {
            SessionError::FlowCapacityExceeded { max } => {
                Some(Self::FlowCapacityExceeded { max: *max })
            }
            SessionError::ReceiveByteBudgetExceeded { attempted, max } => {
                Some(Self::ReceiveByteBudgetExceeded {
                    attempted: *attempted,
                    max: *max,
                })
            }
            SessionError::ReceiveRangeBudgetExceeded { attempted, max } => {
                Some(Self::ReceiveRangeBudgetExceeded {
                    attempted: *attempted,
                    max: *max,
                })
            }
            SessionError::TerminalTombstoneCapacityExceeded { max } => {
                Some(Self::TerminalTombstoneCapacityExceeded { max: *max })
            }
            _ => None,
        }
    }
}

/// Sole mutable owner of the pure reducer and its adapter-side byte permits.
pub(crate) struct SessionSupervisor {
    model: SessionModel,
    attach_authority: Option<AttachAuthority>,
    owner_active_contract: Option<OwnerActiveContract>,
    owner_catch_up_permit: Option<OwnerCatchUpPermit>,
    tcp_factory_origin: Option<TcpFactoryOrigin>,
    uplink_replay: BTreeMap<ReplayExtentKey, UplinkReplayOwnership>,
    poisoned: bool,
    quarantined_uplink: Vec<QuarantinedUplinkOwnership>,
}

impl SessionSupervisor {
    /// Isolated reducer-fixture constructor. Production adapters must enter
    /// through exact-leg [`Self::bootstrap_client`] or
    /// [`Self::prepare_owner`].
    #[cfg(test)]
    pub(crate) fn new(model: SessionModel) -> Self {
        Self {
            model,
            attach_authority: None,
            owner_active_contract: None,
            owner_catch_up_permit: None,
            tcp_factory_origin: None,
            uplink_replay: BTreeMap::new(),
            poisoned: false,
            quarantined_uplink: Vec::new(),
        }
    }

    /// Creates reusable pre-session state for exact-leg owner bootstrap.
    /// Rejected attempts retain this same authority and config for retry.
    pub(crate) fn prepare_owner(
        config: SessionConfig,
        authority: AttachAuthority,
    ) -> PendingOwnerBootstrap {
        PendingOwnerBootstrap {
            config,
            authority: Some(authority),
        }
    }

    /// Constructs the initial client model only after one queue-bound,
    /// actually delivered ATTACH has received its exact correlated
    /// acceptance. The same sole queue transitions to Active without an
    /// allocation or another pressure-sensitive admission.
    #[allow(
        clippy::result_large_err,
        reason = "bootstrap rejection returns the exact queue-bound acceptance capability"
    )]
    pub(crate) fn bootstrap_client(
        config: SessionConfig,
        accepted: AcceptedInitialAttach,
        queue: &mut LegOutboundQueue,
    ) -> Result<ClientSessionBootstrap, ClientBootstrapError> {
        let attached = accepted
            .activate_on_queue(queue)
            .map_err(ClientBootstrapError::activation)?;
        let model = attached.initial_client_model(config);
        Ok(ClientSessionBootstrap {
            supervisor: Self {
                model,
                attach_authority: None,
                owner_active_contract: None,
                owner_catch_up_permit: None,
                tcp_factory_origin: None,
                uplink_replay: BTreeMap::new(),
                poisoned: false,
                quarantined_uplink: Vec::new(),
            },
            attached,
        })
    }

    /// Isolated constructor-validation seam. Production owner sessions are
    /// created only by [`Self::prepare_owner`] from an exact-leg ATTACH.
    #[cfg(test)]
    pub(crate) fn new_owner(
        model: SessionModel,
        attach_authority: AttachAuthority,
    ) -> Result<Self, SessionSupervisorError> {
        if model.role() != SessionRole::Owner {
            return Err(SessionSupervisorError::OwnerRoleRequired {
                actual: model.role(),
            });
        }
        if model.session_id() != attach_authority.session_id() {
            return Err(SessionSupervisorError::AuthorityModelSessionMismatch);
        }
        let model_generation = model.snapshot().generation().get();
        let authority_generation = attach_authority.current_generation();
        if model_generation != authority_generation {
            return Err(SessionSupervisorError::AuthorityModelGenerationMismatch {
                model: model_generation,
                authority: authority_generation,
            });
        }
        if !attach_authority.matches_existing_leg_semantics(&model.current_leg()) {
            return Err(SessionSupervisorError::AuthorityModelSemanticsMismatch);
        }
        Ok(Self {
            model,
            attach_authority: Some(attach_authority),
            // This test-only bare-model seam has no exact process-local
            // AttachedLeg seal and therefore cannot authorize standby.
            owner_active_contract: None,
            owner_catch_up_permit: None,
            tcp_factory_origin: None,
            uplink_replay: BTreeMap::new(),
            poisoned: false,
            quarantined_uplink: Vec::new(),
        })
    }

    /// Test-only bridge for an already-created exact owner `AttachedLeg`.
    /// Production still enters exclusively through `PendingOwnerBootstrap`.
    #[cfg(test)]
    pub(crate) fn new_owner_with_active(
        model: SessionModel,
        attach_authority: AttachAuthority,
        active: &AttachedLeg,
    ) -> Result<Self, SessionSupervisorError> {
        let model_leg = model.current_leg();
        if active.generation() != model_leg.generation()
            || active.nonce() != model_leg.nonce()
            || active.transport_binding() != model_leg.transport_binding()
        {
            return Err(SessionSupervisorError::OwnerStandbyAuthenticationRejected);
        }
        let mut supervisor = Self::new_owner(model, attach_authority)?;
        supervisor.owner_active_contract = Some(OwnerActiveContract::from_attached(active));
        Ok(supervisor)
    }

    pub(crate) fn snapshot(&self) -> SessionSupervisorSnapshot {
        SessionSupervisorSnapshot {
            session: self.model.snapshot(),
            uplink_replay_extents: self.uplink_replay.len(),
            uplink_replay_owned_bytes: self
                .uplink_replay
                .values()
                .map(UplinkReplayOwnership::bytes)
                .sum(),
            poisoned: self.poisoned,
            quarantined_uplink_extents: self.quarantined_uplink.len(),
            quarantined_uplink_owned_bytes: self
                .quarantined_uplink
                .iter()
                .map(QuarantinedUplinkOwnership::bytes)
                .sum(),
            owner_catch_up_permit: self.owner_catch_up_permit.is_some(),
            owner_catch_up_permit_armed: self
                .owner_catch_up_permit
                .as_ref()
                .is_some_and(|permit| permit.armed),
        }
    }

    pub(crate) fn owner_recovery_effect_bound(&self) -> Option<usize> {
        self.model.config().owner_recovery_effect_bound()
    }

    pub(crate) fn owner_pending_work_bound(&self) -> Option<usize> {
        self.model.config().owner_pending_work_bound()
    }

    pub(crate) fn owner_target_replay_capacity(&self) -> Option<OwnerTargetReplayCapacity> {
        if self.model.role() != SessionRole::Owner {
            return None;
        }
        let config = self.model.config();
        let per_flow = config.send_window();
        let session = config.replay_limit(Direction::TargetToClient);
        Some(OwnerTargetReplayCapacity {
            per_flow_bytes: per_flow.max_bytes(),
            session_bytes: session.max_bytes(),
        })
    }

    /// Mints the only production TCP source factory for this session. The
    /// session-global byte/segment bounds and per-flow segment bound are
    /// derived from the reducer itself; callers may select only a smaller
    /// per-flow byte/channel shape. A rejected construction leaves the one-shot
    /// claim available for a corrected configuration.
    pub(crate) fn mint_tcp_port_factory(
        &mut self,
        config: FlowPortConfig,
    ) -> Result<ResumableTcpPortFactory, TcpPortFactoryMintError> {
        self.mint_tcp_port_factory_in_phase(config, SessionPhase::Active)
    }

    /// Initial acceptance loss constructs OwnerTarget only after the exact
    /// leg has already moved this pristine session to Legless. This narrow
    /// entry preserves every ordinary factory invariant while avoiding a
    /// global relaxation of Active-only construction.
    pub(super) fn mint_tcp_port_factory_for_legless_bootstrap(
        &mut self,
        config: FlowPortConfig,
    ) -> Result<ResumableTcpPortFactory, TcpPortFactoryMintError> {
        self.mint_tcp_port_factory_in_phase(config, SessionPhase::Legless)
    }

    fn mint_tcp_port_factory_in_phase(
        &mut self,
        config: FlowPortConfig,
        required_phase: SessionPhase,
    ) -> Result<ResumableTcpPortFactory, TcpPortFactoryMintError> {
        if self.tcp_factory_origin.is_some() {
            return Err(TcpPortFactoryMintError::AlreadyMinted);
        }
        if self.poisoned {
            return Err(TcpPortFactoryMintError::PoisonedSupervisor);
        }
        let snapshot = self.model.snapshot();
        let client_replay = snapshot.replay_usage(Direction::ClientToTarget);
        let target_replay = snapshot.replay_usage(Direction::TargetToClient);
        let replay_bytes = client_replay.bytes().saturating_add(target_replay.bytes());
        let replay_segments = client_replay
            .segments()
            .saturating_add(target_replay.segments());
        if snapshot.phase() != required_phase
            || snapshot.flow_count() != 0
            || replay_bytes != 0
            || replay_segments != 0
            || snapshot.terminal_tombstones() != 0
            || snapshot.next_local_flow_id() != Some(1)
            || snapshot.highest_peer_flow_id() != 0
        {
            return Err(TcpPortFactoryMintError::SessionNotPristine {
                phase: snapshot.phase(),
                flow_count: snapshot.flow_count(),
                replay_bytes,
                replay_segments,
                terminal_tombstones: snapshot.terminal_tombstones(),
                next_local_flow_id: snapshot.next_local_flow_id(),
                highest_peer_flow_id: snapshot.highest_peer_flow_id(),
            });
        }
        let direction = self.model.role().local_send_direction();
        let per_flow = self.model.config().send_window();
        if config.uplink_byte_capacity() > per_flow.max_bytes() {
            return Err(TcpPortFactoryMintError::PerFlowBytesExceedReducer {
                requested: config.uplink_byte_capacity(),
                available: per_flow.max_bytes(),
            });
        }
        let session = self.model.config().replay_limit(direction);
        let origin = TcpFactoryOrigin::mint();
        let factory = ResumableTcpPortFactory::new_bound(
            config,
            self.model.session_id(),
            direction,
            session.max_bytes(),
            per_flow.max_segments(),
            session.max_segments(),
            origin.clone(),
        )?;
        self.tcp_factory_origin = Some(origin);
        Ok(factory)
    }

    pub(crate) fn apply_command(
        &mut self,
        command: SessionOwnerCommand,
    ) -> Result<Vec<SessionEffect>, SessionSupervisorError> {
        self.require_healthy()?;
        match command {
            SessionOwnerCommand::Event(event) => {
                self.apply_carried_event_command(event, SessionOwnerCommand::Event)
            }
            SessionOwnerCommand::Driver(DriverInput::Control(event)) => self
                .apply_carried_event_command(event, |event| {
                    SessionOwnerCommand::Driver(DriverInput::Control(event))
                }),
            SessionOwnerCommand::Driver(input @ DriverInput::Data(_)) => {
                self.apply_driver_input(input)
            }
        }
    }

    fn apply_carried_event_command(
        &mut self,
        event: SessionEvent,
        rebuild: impl FnOnce(SessionEvent) -> SessionOwnerCommand,
    ) -> Result<Vec<SessionEffect>, SessionSupervisorError> {
        match self.apply_event(event) {
            Err(SessionSupervisorError::RejectedEvent { block, event }) => {
                Err(SessionSupervisorError::RejectedCommand {
                    block,
                    command: Box::new(rebuild(*event)),
                })
            }
            result => result,
        }
    }

    pub(crate) fn apply_event(
        &mut self,
        event: SessionEvent,
    ) -> Result<Vec<SessionEffect>, SessionSupervisorError> {
        self.require_healthy()?;
        if matches!(event, SessionEvent::LocalData { .. }) {
            return Err(SessionSupervisorError::UnownedLocalData);
        }
        if matches!(
            event,
            SessionEvent::ReplacementAttached { .. } | SessionEvent::ReplacementCaughtUp { .. }
        ) {
            return Err(SessionSupervisorError::ReplacementProvenanceRequired);
        }
        let retry = event.clone();
        let effects = match self.model.reduce(event) {
            Ok(effects) => effects,
            Err(error) => {
                if let Some(block) = ReducerAdmissionBlock::from_error(&error) {
                    return Err(SessionSupervisorError::RejectedEvent {
                        block,
                        event: Box::new(retry),
                    });
                }
                return Err(SessionSupervisorError::Reducer(error));
            }
        };
        self.consume_after_model_commit(effects)
    }

    /// Authenticates one exact classified standby registration while keeping
    /// the owner's attach authority private to this supervisor.
    ///
    /// This step is deliberately read-only: it neither installs the standby
    /// slot nor authorizes an ATTACH. Every wire/authentication mismatch has
    /// one public rejection shape, while an ownership-poisoned supervisor and
    /// an internal authority/model generation split remain fail-closed
    /// invariants.
    pub(crate) fn authenticate_owner_standby(
        &self,
        active: &AttachedLeg,
        candidate: &EstablishedLeg,
        inbound: BoundInbound,
    ) -> Result<ExactAuthenticatedStandby, SessionSupervisorError> {
        self.require_healthy()?;
        let snapshot = self.model.snapshot();
        let Some(authority) = self.attach_authority.as_ref() else {
            return Err(SessionSupervisorError::OwnerStandbyAuthenticationRejected);
        };
        if self.model.role() != SessionRole::Owner || snapshot.phase() != SessionPhase::Active {
            return Err(SessionSupervisorError::OwnerStandbyAuthenticationRejected);
        }
        let model_generation = snapshot.generation().get();
        let authority_generation = authority.current_generation();
        if model_generation != authority_generation {
            return Err(SessionSupervisorError::AuthorityModelGenerationMismatch {
                model: model_generation,
                authority: authority_generation,
            });
        }
        let active_matches = self
            .owner_active_contract
            .as_ref()
            .is_some_and(|installed| {
                installed.generation.get() == model_generation && installed.matches(active)
            });
        if !active_matches {
            return Err(SessionSupervisorError::OwnerStandbyAuthenticationRejected);
        }
        ExactAuthenticatedStandby::authenticate(active, candidate, inbound, authority)
            .map_err(|_| SessionSupervisorError::OwnerStandbyAuthenticationRejected)
    }

    /// Serializes owner authentication commit, reducer installation, and
    /// response publication ownership in one non-awaiting turn.
    ///
    /// A committed publication is returned only after the owner reducer has
    /// installed the exact same-leg capability. The caller must queue the
    /// acceptance on that leg before dispatching the returned recovery effects.
    #[cfg(test)]
    pub(crate) fn accept_owner_attach_for_test(
        &mut self,
        leg: &EstablishedLeg,
        received: LegBoundFrame,
    ) -> Result<OwnerAttachPublication, SessionSupervisorError> {
        if self.owner_catch_up_permit.is_some() {
            return Err(SessionSupervisorError::OwnerCatchUpPermitConflict);
        }
        self.accept_owner_attach_inner(leg, received)
    }

    /// Only a gate removed from the exact registered B slot may authorize a
    /// normal exact-next owner commit in production.
    pub(super) fn accept_registered_owner_attach(
        &mut self,
        gate: &OwnerRegisteredAttachGate,
        leg: &EstablishedLeg,
        received: LegBoundFrame,
    ) -> Result<OwnerAttachPublication, SessionSupervisorError> {
        let model_generation = self.model.snapshot().generation();
        if gate.current_generation() != model_generation
            || !gate.matches_transaction(leg, &received)
        {
            return Err(SessionSupervisorError::OwnerRegisteredAttachGateRejected);
        }
        let publication = self.accept_owner_attach_inner(leg, received)?;
        if matches!(publication, OwnerAttachPublication::Installed { .. }) {
            self.owner_catch_up_permit = None;
        }
        Ok(publication)
    }

    /// Authenticates a stale/exhausted request and publishes only its
    /// correlated generation status. A single catch-up permit is installed
    /// idempotently for the same stale correlation; conflicts are inert.
    pub(super) fn resynchronize_owner_attach(
        &mut self,
        leg: &EstablishedLeg,
        received: LegBoundFrame,
    ) -> Result<AuthenticatedOwnerAttachStatus, SessionSupervisorError> {
        self.require_healthy()?;
        self.invalidate_owner_catch_up_permit();
        let snapshot = self.model.snapshot();
        if self.model.role() != SessionRole::Owner || snapshot.phase() == SessionPhase::Expired {
            return Err(SessionSupervisorError::OwnerCatchUpStatusRejected);
        }
        let authority = self
            .attach_authority
            .as_ref()
            .ok_or(SessionSupervisorError::MissingAttachAuthority)?;
        let model_generation = snapshot.generation().get();
        let authority_generation = authority.current_generation();
        if model_generation != authority_generation {
            return Err(SessionSupervisorError::AuthorityModelGenerationMismatch {
                model: model_generation,
                authority: authority_generation,
            });
        }
        let authenticated = leg.authenticate_owner_resynchronization(received, authority)?;
        if authenticated.current_generation().get() != model_generation {
            return Err(SessionSupervisorError::AuthorityModelGenerationMismatch {
                model: model_generation,
                authority: authenticated.current_generation().get(),
            });
        }
        let Some(active) = self.owner_active_contract.as_ref() else {
            return Err(SessionSupervisorError::OwnerCatchUpStatusRejected);
        };
        if active.generation.get() != model_generation
            || !authenticated
                .matches_active_negotiation(active.selected_version, active.negotiated_features)
        {
            return Err(SessionSupervisorError::OwnerCatchUpStatusRejected);
        }
        let status = authenticated.status_frame();
        let arm = if authenticated.can_catch_up() {
            match self.owner_catch_up_permit.as_ref() {
                None => {
                    let identity = Arc::new(OwnerCatchUpPermitIdentity);
                    let arm = OwnerCatchUpArm {
                        identity: Arc::clone(&identity),
                    };
                    self.owner_catch_up_permit = Some(OwnerCatchUpPermit {
                        identity,
                        status_binding: authenticated.transport_binding(),
                        status_seal: leg.standby_seal(),
                        authenticated,
                        armed: false,
                    });
                    Some(arm)
                }
                // The original pending publication remains the sole arm
                // authority. Before it is delivered, a same-contract status
                // publication without that arm could occupy the exact C FIFO
                // and permanently strand the permit. Once armed, status-only
                // authentication remains state-idempotent without promising
                // that a controller will republish it on the wire.
                Some(existing)
                    if existing.same_contract(&authenticated, leg) && !existing.armed =>
                {
                    return Err(SessionSupervisorError::OwnerCatchUpPermitPending);
                }
                Some(existing) if existing.same_contract(&authenticated, leg) => None,
                Some(_) => return Err(SessionSupervisorError::OwnerCatchUpPermitConflict),
            }
        } else {
            None
        };
        Ok(AuthenticatedOwnerAttachStatus { status, arm })
    }

    /// Arms one reserved stale correlation only after OwnerTarget has consumed
    /// the exact queue receipt proving ordered status delivery.
    pub(super) fn arm_owner_catch_up(
        &mut self,
        arm: OwnerCatchUpArm,
    ) -> Result<(), SessionSupervisorError> {
        self.require_healthy()?;
        self.invalidate_owner_catch_up_permit();
        let Some(permit) = self.owner_catch_up_permit.as_mut() else {
            return Err(SessionSupervisorError::OwnerCatchUpPermitMissing);
        };
        if !Arc::ptr_eq(&permit.identity, &arm.identity) {
            return Err(SessionSupervisorError::OwnerCatchUpArmRejected);
        }
        permit.armed = true;
        Ok(())
    }

    /// Clears only the exact still-unarmed permit whose status publication is
    /// proven lost by OwnerTarget. Armed or mismatched authority is inert.
    pub(super) fn abandon_unarmed_owner_catch_up(
        &mut self,
        arm: &OwnerCatchUpArm,
    ) -> Result<(), SessionSupervisorError> {
        self.invalidate_owner_catch_up_permit();
        let Some(permit) = self.owner_catch_up_permit.as_ref() else {
            return Err(SessionSupervisorError::OwnerCatchUpPermitMissing);
        };
        if permit.armed || !Arc::ptr_eq(&permit.identity, &arm.identity) {
            return Err(SessionSupervisorError::OwnerCatchUpArmRejected);
        }
        self.owner_catch_up_permit = None;
        Ok(())
    }

    /// The only non-registered production commit path consumes a bounded
    /// permit derived from an earlier proof-valid stale request. Failed auth,
    /// model rejection, or resynchronization retains the exact permit.
    pub(super) fn preflight_catch_up_owner_attach(
        &self,
        leg: &EstablishedLeg,
        received: &LegBoundFrame,
    ) -> Result<(), SessionSupervisorError> {
        self.require_healthy()?;
        let authority = self
            .attach_authority
            .as_ref()
            .ok_or(SessionSupervisorError::MissingAttachAuthority)?;
        let snapshot = self.model.snapshot();
        let model_generation = snapshot.generation().get();
        let authority_generation = authority.current_generation();
        if model_generation != authority_generation {
            return Err(SessionSupervisorError::AuthorityModelGenerationMismatch {
                model: model_generation,
                authority: authority_generation,
            });
        }
        let permit = self
            .owner_catch_up_permit
            .as_ref()
            .ok_or(SessionSupervisorError::OwnerCatchUpPermitMissing)?;
        if !permit.matches_current(snapshot) {
            return Err(SessionSupervisorError::OwnerCatchUpAttachGateRejected);
        }
        if !permit.armed {
            return Err(SessionSupervisorError::OwnerCatchUpPermitNotArmed);
        }
        let Some(active) = self.owner_active_contract.as_ref() else {
            return Err(SessionSupervisorError::OwnerCatchUpAttachGateRejected);
        };
        if !permit.permits(active, leg, received) {
            return Err(SessionSupervisorError::OwnerCatchUpAttachGateRejected);
        }
        Ok(())
    }

    pub(super) fn accept_catch_up_owner_attach(
        &mut self,
        leg: &EstablishedLeg,
        received: LegBoundFrame,
    ) -> Result<OwnerAttachPublication, SessionSupervisorError> {
        self.preflight_catch_up_owner_attach(leg, &received)?;
        let publication = self.accept_owner_attach_inner(leg, received)?;
        if matches!(publication, OwnerAttachPublication::Installed { .. }) {
            self.owner_catch_up_permit = None;
        }
        Ok(publication)
    }

    fn accept_owner_attach_inner(
        &mut self,
        leg: &EstablishedLeg,
        received: LegBoundFrame,
    ) -> Result<OwnerAttachPublication, SessionSupervisorError> {
        self.require_healthy()?;
        let authority = self
            .attach_authority
            .as_ref()
            .ok_or(SessionSupervisorError::MissingAttachAuthority)?;
        let model_generation = self.model.snapshot().generation().get();
        let authority_generation = authority.current_generation();
        if model_generation != authority_generation {
            return Err(SessionSupervisorError::AuthorityModelGenerationMismatch {
                model: model_generation,
                authority: authority_generation,
            });
        }

        let outcome = leg
            .transact_owner_attach_from_supervisor(received, authority, &mut self.model)?
            .map_err(SessionSupervisorError::Reducer)?;
        match outcome {
            OwnerAttachTransaction::Installed {
                attached,
                acceptance,
                recovery,
            } => {
                // `PreflightedReplacement::install` constructs only visible
                // activation/replay effects.  Keep the post-CAS half of this
                // transaction infallible: slot and queue authority must never
                // be restored after the generation/model commit succeeded.
                self.invalidate_owner_catch_up_permit();
                self.owner_active_contract = Some(OwnerActiveContract::from_attached(&attached));
                Ok(OwnerAttachPublication::Installed {
                    attached,
                    acceptance,
                    recovery,
                })
            }
            OwnerAttachTransaction::Resynchronize { status } => {
                Ok(OwnerAttachPublication::Resynchronize { status })
            }
        }
    }

    /// Installs an exact status-derived client replacement after both the
    /// status response and follow-up acceptance crossed their own live-leg
    /// provenance gates.
    #[cfg(test)]
    pub(crate) fn install_caught_up_leg(
        &mut self,
        leg: &CaughtUpAttachedLeg,
    ) -> Result<Vec<SessionEffect>, SessionSupervisorError> {
        if !leg.transport_is_open() {
            return Err(SessionSupervisorError::ReplacementEndpointLost);
        }
        self.install_client_replacement(leg.replacement_caught_up_event())
    }

    /// Installs a normal client replacement only from the exact live-leg
    /// capability minted by its correlated ATTACH_ACCEPTED response.
    #[cfg(test)]
    pub(crate) fn install_attached_leg(
        &mut self,
        leg: &AttachedLeg,
    ) -> Result<Vec<SessionEffect>, SessionSupervisorError> {
        if !leg.transport_is_open() {
            return Err(SessionSupervisorError::ReplacementEndpointLost);
        }
        self.install_client_replacement(leg.replacement_attached_event())
    }

    fn install_client_replacement(
        &mut self,
        event: SessionEvent,
    ) -> Result<Vec<SessionEffect>, SessionSupervisorError> {
        self.require_healthy()?;
        if self.attach_authority.is_some() {
            return Err(SessionSupervisorError::OwnerAttachTransactionRequired);
        }
        let effects = self.model.reduce(event)?;
        self.consume_after_model_commit(effects)
    }

    fn apply_driver_input(
        &mut self,
        input: DriverInput,
    ) -> Result<Vec<SessionEffect>, SessionSupervisorError> {
        match input {
            DriverInput::Control(event) => self.apply_event(event),
            DriverInput::Data(data) => {
                let (event, mut staging) = data.into_event_and_ownership();
                let provenance_matches = self
                    .tcp_factory_origin
                    .as_ref()
                    .is_some_and(|expected| staging.matches_factory_origin(expected));
                if !provenance_matches {
                    self.poisoned = true;
                    self.quarantined_uplink
                        .push(QuarantinedUplinkOwnership::Staging(staging));
                    return Err(SessionSupervisorError::UplinkFactoryProvenanceMismatch);
                }
                let effects = match self.model.reduce(event) {
                    Ok(effects) => effects,
                    Err(
                        source @ (SessionError::ReplayByteBudgetExceeded { .. }
                        | SessionError::ReplaySegmentBudgetExceeded { .. }),
                    ) => {
                        self.poisoned = true;
                        self.quarantined_uplink
                            .push(QuarantinedUplinkOwnership::Staging(staging));
                        return Err(SessionSupervisorError::ReducerReplayCapacityInvariant {
                            source,
                        });
                    }
                    Err(error) => return Err(SessionSupervisorError::Reducer(error)),
                };
                let mut stored = None;
                let mut visible = Vec::with_capacity(effects.len());
                for effect in effects {
                    match effect {
                        SessionEffect::ReplayStored { receipt } => {
                            if stored.replace(receipt).is_some() {
                                self.poisoned = true;
                                self.quarantined_uplink
                                    .push(QuarantinedUplinkOwnership::Staging(staging));
                                return Err(SessionSupervisorError::DuplicateReplayStored);
                            }
                        }
                        effect => visible.push(effect),
                    }
                }
                let Some(stored) = stored else {
                    self.poisoned = true;
                    self.quarantined_uplink
                        .push(QuarantinedUplinkOwnership::Staging(staging));
                    return Err(SessionSupervisorError::MissingReplayStored);
                };
                let ownership = match staging.bind_replay(stored) {
                    Ok(ownership) => ownership,
                    Err(error) => {
                        self.poisoned = true;
                        self.quarantined_uplink
                            .push(QuarantinedUplinkOwnership::Staging(staging));
                        return Err(SessionSupervisorError::FlowPort(error));
                    }
                };
                let provenance_matches = self
                    .tcp_factory_origin
                    .as_ref()
                    .is_some_and(|expected| ownership.matches_factory_origin(expected));
                if !provenance_matches {
                    self.poisoned = true;
                    self.quarantined_uplink
                        .push(QuarantinedUplinkOwnership::Replay(ownership));
                    return Err(SessionSupervisorError::UplinkFactoryProvenanceMismatch);
                }
                let key = ReplayExtentKey::of(&ownership);
                if self.uplink_replay.contains_key(&key) {
                    self.poisoned = true;
                    self.quarantined_uplink
                        .push(QuarantinedUplinkOwnership::Replay(ownership));
                    return Err(SessionSupervisorError::DuplicateReplayExtent);
                }
                let previous = self.uplink_replay.insert(key, ownership);
                debug_assert!(previous.is_none());
                self.consume_after_model_commit(visible)
            }
        }
    }

    fn require_healthy(&self) -> Result<(), SessionSupervisorError> {
        if self.poisoned {
            return Err(SessionSupervisorError::PoisonedOwnershipInvariant);
        }
        Ok(())
    }

    fn consume_after_model_commit(
        &mut self,
        effects: Vec<SessionEffect>,
    ) -> Result<Vec<SessionEffect>, SessionSupervisorError> {
        match self.consume_internal_receipts(effects) {
            Ok(visible) => {
                self.invalidate_owner_catch_up_permit();
                Ok(visible)
            }
            Err(error) => {
                self.poisoned = true;
                self.owner_catch_up_permit = None;
                Err(error)
            }
        }
    }

    fn invalidate_owner_catch_up_permit(&mut self) {
        let snapshot = self.model.snapshot();
        if self
            .owner_catch_up_permit
            .as_ref()
            .is_some_and(|permit| !permit.matches_current(snapshot))
        {
            self.owner_catch_up_permit = None;
        }
    }

    fn consume_internal_receipts(
        &mut self,
        effects: Vec<SessionEffect>,
    ) -> Result<Vec<SessionEffect>, SessionSupervisorError> {
        let mut visible = Vec::with_capacity(effects.len());
        for effect in effects {
            match effect {
                SessionEffect::ReplayStored { .. } => {
                    return Err(SessionSupervisorError::UnownedReplayStored);
                }
                SessionEffect::ReplayAcknowledged { receipt } => {
                    let candidates = self
                        .uplink_replay
                        .iter_mut()
                        .filter(|(key, _)| {
                            key.flow_id == receipt.flow_id() && key.direction == receipt.direction()
                        })
                        .map(|(key, ownership)| {
                            ownership
                                .release_after_reducer_ack(&receipt)
                                .map(|released| (*key, released))
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    for (key, released) in candidates {
                        if released {
                            self.uplink_replay.remove(&key);
                        }
                    }
                }
                SessionEffect::SessionExpired => {
                    // This effect is emitted only after this exact reducer
                    // terminally clears every flow and replay window. Dropping
                    // the matching adapter ownership releases both per-flow
                    // and session-global byte permits in the same owner turn.
                    self.uplink_replay.clear();
                    visible.push(SessionEffect::SessionExpired);
                }
                effect @ SessionEffect::FlowFinished { flow_id, .. } => {
                    let candidates = self
                        .uplink_replay
                        .keys()
                        .copied()
                        .filter(|key| key.flow_id == flow_id)
                        .collect::<Vec<_>>();
                    for key in candidates {
                        let ownership = self
                            .uplink_replay
                            .get_mut(&key)
                            .ok_or(SessionSupervisorError::ReplayExtentDisappeared)?;
                        ownership.release_after_reducer_terminal(&effect)?;
                        self.uplink_replay.remove(&key);
                    }
                    visible.push(effect);
                }
                effect => visible.push(effect),
            }
        }
        Ok(visible)
    }
}

/// Retry-safe owner state before the first exact ATTACH has installed a
/// session model.
///
/// Authentication failures restore the same authority object before
/// returning. A successful CAS is followed only by infallible model and
/// supervisor construction, after which this value is permanently consumed.
pub(crate) struct PendingOwnerBootstrap {
    config: SessionConfig,
    authority: Option<AttachAuthority>,
}

impl PendingOwnerBootstrap {
    #[allow(
        clippy::result_large_err,
        reason = "bootstrap rejection returns the exact inbound capability and any unreleased queue reservation"
    )]
    pub(crate) fn accept(
        &mut self,
        leg: &EstablishedLeg,
        received: LegBoundFrame,
        queue: &mut LegOutboundQueue,
    ) -> Result<PendingInitialOwnerBootstrapPublication, InitialOwnerBootstrapAcceptFailure> {
        if self.authority.is_none() {
            return Err(InitialOwnerBootstrapAcceptFailure::new(
                received,
                InitialOwnerBootstrapAcceptErrorKind::AlreadyCompleted,
            ));
        }
        let reservation = match queue.reserve_attach_acceptance(leg) {
            Ok(reservation) => reservation,
            Err(source) => {
                return Err(InitialOwnerBootstrapAcceptFailure::new(
                    received,
                    InitialOwnerBootstrapAcceptErrorKind::QueueReservation(source),
                ));
            }
        };
        let _terminal_transition = match leg.lock_terminal_transition() {
            Ok(transition) => transition,
            Err(source) => {
                return Err(Self::release_failed_reservation(
                    queue,
                    reservation,
                    received,
                    InitialOwnerBootstrapAcceptErrorKind::TerminalTransition(source),
                ));
            }
        };
        if !leg.transport_is_open() {
            return Err(Self::release_failed_reservation(
                queue,
                reservation,
                received,
                InitialOwnerBootstrapAcceptErrorKind::EndpointLost,
            ));
        }
        let Some(authority) = self.authority.take() else {
            return Err(Self::release_failed_reservation(
                queue,
                reservation,
                received,
                InitialOwnerBootstrapAcceptErrorKind::AlreadyCompleted,
            ));
        };
        let authenticated = match leg
            .authenticate_initial_owner_attach_preserving(received, &authority)
        {
            Ok(authenticated) => authenticated,
            Err(failure) => {
                let (received, source) = failure.into_parts();
                self.authority = Some(authority);
                let kind = match source {
                    LegProvenanceError::WrongLeg => InitialOwnerBootstrapAcceptErrorKind::WrongLeg,
                    LegProvenanceError::Rejected => InitialOwnerBootstrapAcceptErrorKind::Rejected,
                };
                return Err(Self::release_failed_reservation(
                    queue,
                    reservation,
                    received,
                    kind,
                ));
            }
        };
        let (model, attached, acceptance) = authenticated.into_owner_parts(self.config);
        let owner_active_contract = OwnerActiveContract::from_attached(&attached);
        let supervisor = SessionSupervisor {
            model,
            attach_authority: Some(authority),
            owner_active_contract: Some(owner_active_contract),
            owner_catch_up_permit: None,
            tcp_factory_origin: None,
            uplink_replay: BTreeMap::new(),
            poisoned: false,
            quarantined_uplink: Vec::new(),
        };
        Ok(PendingInitialOwnerBootstrapPublication {
            supervisor,
            attached,
            acceptance,
            reservation,
        })
    }

    fn release_failed_reservation(
        queue: &mut LegOutboundQueue,
        reservation: AttachAcceptanceReservation,
        received: LegBoundFrame,
        kind: InitialOwnerBootstrapAcceptErrorKind,
    ) -> InitialOwnerBootstrapAcceptFailure {
        match reservation.release(queue) {
            Ok(()) => InitialOwnerBootstrapAcceptFailure::new(received, kind),
            Err(reservation) => InitialOwnerBootstrapAcceptFailure {
                received,
                kind: InitialOwnerBootstrapAcceptErrorKind::ReservationInvariant,
                reservation: Some(reservation),
            },
        }
    }
}

impl fmt::Debug for PendingOwnerBootstrap {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PendingOwnerBootstrap")
            .field("completed", &self.authority.is_none())
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub(crate) enum InitialOwnerBootstrapAcceptErrorKind {
    #[error("initial owner bootstrap already completed")]
    AlreadyCompleted,
    #[error("initial owner queue reservation failed: {0}")]
    QueueReservation(AttachAcceptanceReserveError),
    #[error("initial owner terminal transition failed: {0}")]
    TerminalTransition(LegTerminalTransitionError),
    #[error("initial owner endpoint was lost before authentication")]
    EndpointLost,
    #[error("initial owner ATTACH belongs to another leg")]
    WrongLeg,
    #[error("initial owner ATTACH authentication was rejected")]
    Rejected,
    #[error("initial owner queue reservation could not be restored")]
    ReservationInvariant,
}

/// Retry-safe initial-owner rejection. Authentication authority remains in
/// `PendingOwnerBootstrap`; an exceptional reservation release mismatch is
/// returned here rather than silently losing the exact queue claim.
pub(crate) struct InitialOwnerBootstrapAcceptFailure {
    received: LegBoundFrame,
    kind: InitialOwnerBootstrapAcceptErrorKind,
    reservation: Option<AttachAcceptanceReservation>,
}

impl InitialOwnerBootstrapAcceptFailure {
    fn new(received: LegBoundFrame, kind: InitialOwnerBootstrapAcceptErrorKind) -> Self {
        Self {
            received,
            kind,
            reservation: None,
        }
    }

    pub(crate) const fn kind(&self) -> InitialOwnerBootstrapAcceptErrorKind {
        self.kind
    }

    pub(super) fn into_parts(self) -> (LegBoundFrame, Option<AttachAcceptanceReservation>) {
        (self.received, self.reservation)
    }
}

impl fmt::Debug for InitialOwnerBootstrapAcceptFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InitialOwnerBootstrapAcceptFailure")
            .field("kind", &self.kind)
            .field("reservation_retained", &self.reservation.is_some())
            .field("received", &"[REDACTED]")
            .finish()
    }
}

impl fmt::Display for InitialOwnerBootstrapAcceptFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.kind.fmt(formatter)
    }
}

impl std::error::Error for InitialOwnerBootstrapAcceptFailure {}

/// Committed initial owner session whose exact queue reservation still owns
/// the sole acceptance publication path.
#[must_use = "initial owner acceptance must enter its reserved exact queue"]
pub(crate) struct PendingInitialOwnerBootstrapPublication {
    supervisor: SessionSupervisor,
    attached: AttachedLeg,
    acceptance: Frame,
    reservation: AttachAcceptanceReservation,
}

impl PendingInitialOwnerBootstrapPublication {
    #[allow(
        clippy::result_large_err,
        reason = "queue rejection returns the exact committed publication for fail-closed handling"
    )]
    pub(crate) fn enqueue_acceptance(
        self,
        queue: &mut LegOutboundQueue,
    ) -> Result<OwnerSessionBootstrap, InitialOwnerBootstrapEnqueueFailure> {
        let Self {
            supervisor,
            attached,
            acceptance,
            reservation,
        } = self;
        match queue.push_reserved_initial_attach_acceptance(reservation, &attached, acceptance) {
            Ok(acceptance) => Ok(OwnerSessionBootstrap {
                supervisor,
                attached,
                acceptance,
            }),
            Err(error) => {
                let kind = error.kind().clone();
                let (reservation, acceptance) = error.into_parts();
                Err(InitialOwnerBootstrapEnqueueFailure {
                    publication: Self {
                        supervisor,
                        attached,
                        acceptance,
                        reservation,
                    },
                    kind,
                })
            }
        }
    }

    /// Consumes a committed-but-unpublished initial attach only for the exact
    /// transport terminal that owns its installed leg. The same supervisor
    /// applies `LegLost` once and moves to a typed legless high-water
    /// bootstrap; queue loss alone has no conversion API and therefore keeps
    /// this bounded publication waiting for terminal authority.
    #[allow(
        clippy::result_large_err,
        reason = "wrong terminal returns the exact committed publication and terminal fact"
    )]
    pub(crate) fn lose_before_acceptance(
        self,
        terminal: ExactLegTerminal,
    ) -> Result<LeglessOwnerSessionBootstrap, InitialOwnerBootstrapTerminalFailure> {
        let Self {
            mut supervisor,
            attached,
            acceptance,
            reservation,
        } = self;
        let high_water_generation = attached.generation();
        let resume_grace_expired = attached.resume_grace_expired_event();
        let loss = match attached.bind_terminal(terminal) {
            Ok(loss) => loss,
            Err(mismatch) => {
                let (attached, terminal) = mismatch.into_parts();
                return Err(InitialOwnerBootstrapTerminalFailure::WrongTerminal {
                    publication: Self {
                        supervisor,
                        attached,
                        acceptance,
                        reservation,
                    },
                    terminal,
                });
            }
        };
        let reason = loss.reason();
        let effects = match supervisor.apply_event(loss.into_event()) {
            Ok(effects) => effects,
            Err(source) => {
                return Err(InitialOwnerBootstrapTerminalFailure::SupervisorInvariant {
                    supervisor,
                    source,
                });
            }
        };
        if effects.as_slice()
            != [SessionEffect::ResumeGraceStarted {
                generation: high_water_generation,
            }]
        {
            return Err(InitialOwnerBootstrapTerminalFailure::EffectInvariant {
                supervisor,
                effects,
            });
        }
        drop((acceptance, reservation, effects));
        Ok(LeglessOwnerSessionBootstrap {
            supervisor,
            high_water: PendingOwnerHighWater {
                generation: high_water_generation,
                reason,
                resume_grace_expired,
            },
        })
    }
}

impl fmt::Debug for PendingInitialOwnerBootstrapPublication {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PendingInitialOwnerBootstrapPublication")
            .field("snapshot", &self.supervisor.snapshot())
            .field("acceptance", &"[REDACTED]")
            .finish_non_exhaustive()
    }
}

/// Queue rejection after the owner generation CAS. The exact committed
/// publication retains both its reserved queue claim and acceptance frame, so
/// the actor can retry admission without authenticating or committing again.
pub(crate) struct InitialOwnerBootstrapEnqueueFailure {
    publication: PendingInitialOwnerBootstrapPublication,
    kind: LegOutboundQueueErrorKind,
}

impl InitialOwnerBootstrapEnqueueFailure {
    pub(crate) const fn kind(&self) -> &LegOutboundQueueErrorKind {
        &self.kind
    }

    pub(crate) fn into_publication(self) -> PendingInitialOwnerBootstrapPublication {
        self.publication
    }
}

impl fmt::Debug for InitialOwnerBootstrapEnqueueFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InitialOwnerBootstrapEnqueueFailure")
            .field("kind", &self.kind)
            .field("publication", &"[REDACTED]")
            .finish()
    }
}

impl fmt::Display for InitialOwnerBootstrapEnqueueFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "initial owner acceptance queue rejected: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for InitialOwnerBootstrapEnqueueFailure {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum InitialOwnerBootstrapTerminalErrorKind {
    WrongTerminal,
    SupervisorInvariant,
    EffectInvariant,
}

#[allow(
    clippy::large_enum_variant,
    reason = "terminal mismatch must return the exact committed publication and terminal fact"
)]
pub(crate) enum InitialOwnerBootstrapTerminalFailure {
    WrongTerminal {
        publication: PendingInitialOwnerBootstrapPublication,
        terminal: ExactLegTerminal,
    },
    SupervisorInvariant {
        supervisor: SessionSupervisor,
        source: SessionSupervisorError,
    },
    EffectInvariant {
        supervisor: SessionSupervisor,
        effects: Vec<SessionEffect>,
    },
}

impl InitialOwnerBootstrapTerminalFailure {
    pub(crate) const fn kind(&self) -> InitialOwnerBootstrapTerminalErrorKind {
        match self {
            Self::WrongTerminal { .. } => InitialOwnerBootstrapTerminalErrorKind::WrongTerminal,
            Self::SupervisorInvariant { .. } => {
                InitialOwnerBootstrapTerminalErrorKind::SupervisorInvariant
            }
            Self::EffectInvariant { .. } => InitialOwnerBootstrapTerminalErrorKind::EffectInvariant,
        }
    }

    #[allow(
        clippy::result_large_err,
        reason = "non-mismatch failures retain their exact supervisor ownership"
    )]
    pub(crate) fn into_exact_parts(
        self,
    ) -> Result<
        (PendingInitialOwnerBootstrapPublication, ExactLegTerminal),
        InitialOwnerBootstrapTerminalFailure,
    > {
        match self {
            Self::WrongTerminal {
                publication,
                terminal,
            } => Ok((publication, terminal)),
            other => Err(other),
        }
    }
}

impl fmt::Debug for InitialOwnerBootstrapTerminalFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = formatter.debug_struct("InitialOwnerBootstrapTerminalFailure");
        debug.field("kind", &self.kind());
        match self {
            Self::WrongTerminal { .. } => debug.field("ownership", &"[REDACTED]").finish(),
            Self::SupervisorInvariant { supervisor, source } => debug
                .field("snapshot", &supervisor.snapshot())
                .field("source", source)
                .finish(),
            Self::EffectInvariant {
                supervisor,
                effects,
            } => debug
                .field("snapshot", &supervisor.snapshot())
                .field("effects", &effects.len())
                .finish(),
        }
    }
}

/// Committed owner state after the initial leg died before its acceptance
/// could be published. It owns the same supervisor and a single high-water
/// timer authority suitable for construction of a legless OwnerTarget.
pub(crate) struct LeglessOwnerSessionBootstrap {
    supervisor: SessionSupervisor,
    high_water: PendingOwnerHighWater,
}

impl LeglessOwnerSessionBootstrap {
    pub(crate) fn snapshot(&self) -> SessionSupervisorSnapshot {
        self.supervisor.snapshot()
    }

    pub(crate) const fn high_water_generation(&self) -> LegGeneration {
        self.high_water.generation
    }

    pub(super) fn into_parts(self) -> (SessionSupervisor, PendingOwnerHighWater) {
        (self.supervisor, self.high_water)
    }
}

impl fmt::Debug for LeglessOwnerSessionBootstrap {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LeglessOwnerSessionBootstrap")
            .field("snapshot", &self.supervisor.snapshot())
            .field("high_water_generation", &self.high_water.generation)
            .finish_non_exhaustive()
    }
}

/// Unbound exact high-water authority. OwnerTarget construction binds it to
/// one executor identity before exposing its one-shot resume-grace expiry.
pub(super) struct PendingOwnerHighWater {
    generation: LegGeneration,
    reason: LegTransportTerminalReason,
    resume_grace_expired: SessionEvent,
}

impl PendingOwnerHighWater {
    pub(super) fn into_parts(self) -> (LegGeneration, LegTransportTerminalReason, SessionEvent) {
        (self.generation, self.reason, self.resume_grace_expired)
    }
}

/// Fully installed initial owner session whose zero-recovery acceptance has
/// already crossed the exact reserved queue admission boundary.
pub(crate) struct OwnerSessionBootstrap {
    supervisor: SessionSupervisor,
    attached: AttachedLeg,
    acceptance: AttachAcceptanceEnqueued,
}

impl OwnerSessionBootstrap {
    pub(crate) fn into_parts(self) -> (SessionSupervisor, AttachedLeg) {
        let Self {
            supervisor,
            attached,
            acceptance,
        } = self;
        drop(acceptance);
        (supervisor, attached)
    }
}

impl fmt::Debug for OwnerSessionBootstrap {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OwnerSessionBootstrap")
            .field("snapshot", &self.supervisor.snapshot())
            .field("acceptance", &self.acceptance)
            .finish_non_exhaustive()
    }
}

/// Fully installed initial client session retaining the exact accepted leg.
pub(crate) struct ClientSessionBootstrap {
    supervisor: SessionSupervisor,
    attached: AttachedLeg,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ClientBootstrapErrorKind {
    WrongQueue,
    EndpointLost,
    RequestNotDelivered,
    NotAwaitingInitialAttach,
}

/// Typed bootstrap rejection that returns the exact non-cloneable accepted
/// leg so the outer transport actor can pair it with its terminal fact.
#[derive(Error)]
#[error("client bootstrap rejected a terminal transport endpoint")]
pub(crate) struct ClientBootstrapError {
    kind: ClientBootstrapErrorKind,
    accepted: AcceptedInitialAttach,
}

impl ClientBootstrapError {
    fn activation(failure: AcceptedInitialAttachActivationFailure) -> Self {
        let kind = match failure.kind() {
            InitialAttachActivationError::WrongQueue => ClientBootstrapErrorKind::WrongQueue,
            InitialAttachActivationError::EndpointOrQueueLost => {
                ClientBootstrapErrorKind::EndpointLost
            }
            InitialAttachActivationError::RequestNotDelivered => {
                ClientBootstrapErrorKind::RequestNotDelivered
            }
            InitialAttachActivationError::NotAwaitingInitialAttach => {
                ClientBootstrapErrorKind::NotAwaitingInitialAttach
            }
        };
        Self {
            kind,
            accepted: failure.into_accepted(),
        }
    }

    pub(crate) const fn kind(&self) -> ClientBootstrapErrorKind {
        self.kind
    }

    pub(crate) fn into_accepted(self) -> AcceptedInitialAttach {
        self.accepted
    }
}

impl fmt::Debug for ClientBootstrapError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ClientBootstrapError([REDACTED])")
    }
}

impl ClientSessionBootstrap {
    pub(crate) fn into_parts(self) -> (SessionSupervisor, AttachedLeg) {
        (self.supervisor, self.attached)
    }
}

impl fmt::Debug for ClientSessionBootstrap {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClientSessionBootstrap")
            .field("snapshot", &self.supervisor.snapshot())
            .finish_non_exhaustive()
    }
}

/// Ordered publication produced by one owner attach transaction.
///
/// For `Installed`, the model is already on the attached generation. Transport
/// code must publish `acceptance` first, retain `attached` as the receive seal,
/// and only then dispatch `recovery` on that same leg.
#[allow(
    clippy::large_enum_variant,
    reason = "exact non-cloneable publication ownership stays inline without adding allocation to the successful control path"
)]
pub(crate) enum OwnerAttachPublication {
    Installed {
        attached: AttachedLeg,
        acceptance: Frame,
        recovery: Vec<SessionEffect>,
    },
    Resynchronize {
        status: Frame,
    },
}

impl fmt::Debug for OwnerAttachPublication {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Installed { recovery, .. } => formatter
                .debug_struct("OwnerAttachPublication::Installed")
                .field("recovery_effects", &recovery.len())
                .finish_non_exhaustive(),
            Self::Resynchronize { .. } => formatter
                .debug_struct("OwnerAttachPublication::Resynchronize")
                .finish_non_exhaustive(),
        }
    }
}

impl fmt::Debug for SessionSupervisor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SessionSupervisor")
            .field("snapshot", &self.snapshot())
            .finish()
    }
}

#[derive(Debug, Error)]
pub(crate) enum SessionSupervisorError {
    #[error("session supervisor is fail-closed after a post-model ownership invariant failure")]
    PoisonedOwnershipInvariant,
    #[error("session reducer rejected an owned command: {0}")]
    Reducer(#[from] SessionError),
    #[error("session reducer deferred an exact event because of {block:?}")]
    RejectedEvent {
        block: ReducerAdmissionBlock,
        event: Box<SessionEvent>,
    },
    #[error("session reducer deferred an exact command because of {block:?}")]
    RejectedCommand {
        block: ReducerAdmissionBlock,
        command: Box<SessionOwnerCommand>,
    },
    #[error("TCP ownership port rejected a reducer receipt: {0}")]
    FlowPort(#[from] FlowPortError),
    #[error("transport leg provenance rejected an owner attach: {0}")]
    LegProvenance(#[from] LegProvenanceError),
    #[error("this session supervisor has no owner attach authority")]
    MissingAttachAuthority,
    #[error("owner standby authentication rejected")]
    OwnerStandbyAuthenticationRejected,
    #[error("owner registered standby gate rejected the exact-next ATTACH")]
    OwnerRegisteredAttachGateRejected,
    #[error("owner catch-up permit is absent")]
    OwnerCatchUpPermitMissing,
    #[error("owner catch-up permit is not armed by actual status delivery")]
    OwnerCatchUpPermitNotArmed,
    #[error("the exact owner catch-up status publication is still pending delivery")]
    OwnerCatchUpPermitPending,
    #[error("owner catch-up arm does not match the bounded stale correlation")]
    OwnerCatchUpArmRejected,
    #[error("owner catch-up status is incompatible with the current installed session")]
    OwnerCatchUpStatusRejected,
    #[error("a conflicting stale ATTACH cannot replace the bounded owner catch-up permit")]
    OwnerCatchUpPermitConflict,
    #[error("owner catch-up permit rejected the follow-up ATTACH")]
    OwnerCatchUpAttachGateRejected,
    #[error("initial owner bootstrap has already completed")]
    OwnerBootstrapCompleted,
    #[cfg(test)]
    #[error("an owner attach supervisor requires the Owner role, got {actual:?}")]
    OwnerRoleRequired { actual: SessionRole },
    #[cfg(test)]
    #[error("attach authority session does not match the owner model session")]
    AuthorityModelSessionMismatch,
    #[error(
        "attach authority generation {authority} does not match owner model generation {model}"
    )]
    AuthorityModelGenerationMismatch { model: u64, authority: u64 },
    #[cfg(test)]
    #[error("attach authority policy does not match the owner model's installed leg semantics")]
    AuthorityModelSemanticsMismatch,
    #[error("raw LocalData has no adapter byte ownership")]
    UnownedLocalData,
    #[error("owned LocalData did not originate from this supervisor's sole TCP factory")]
    UplinkFactoryProvenanceMismatch,
    #[error("correctly provenanced LocalData exceeded reducer replay capacity: {source}")]
    ReducerReplayCapacityInvariant { source: SessionError },
    #[error("owner replacement must pass through the serialized attach transaction")]
    OwnerAttachTransactionRequired,
    #[error("replacement installation requires an exact attached-leg provenance capability")]
    ReplacementProvenanceRequired,
    #[error("replacement installation rejected a terminal transport endpoint")]
    ReplacementEndpointLost,
    #[error("LocalData did not emit its exact ReplayStored receipt")]
    MissingReplayStored,
    #[error("LocalData emitted more than one ReplayStored receipt")]
    DuplicateReplayStored,
    #[error("a raw reducer path emitted ReplayStored outside owned DATA handling")]
    UnownedReplayStored,
    #[error("two owned DATA commands produced the same replay extent")]
    DuplicateReplayExtent,
    #[error("a replay extent disappeared during terminal cleanup")]
    ReplayExtentDisappeared,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::owned_upstream::leg::{
        AttachResponse, BoundInbound, LegTransportReporter, LegTransportTerminalReason,
    };
    use crate::owned_upstream::leg_io::{
        LegEndpointRole, LegIo, LegIoLimits, MemoryFaultScript, MemoryLegTransport,
    };
    use crate::owned_upstream::standby::{ExactAuthenticatedStandby, OwnerStandbySlot};
    use crate::owned_upstream::two_leg::{
        EncodedDelivery, LegId, SimTime, TwoLegWire, WireBounds, WireCapacitySpec, WireLane,
    };
    use crate::owned_upstream::{FlowPortConfig, ResumableTcpPortFactory};
    use crate::resumable::{
        AttachAlpn, AttachCredentials, AttachNonce, AttachPolicy, AttachRequest,
        AttachTransportBinding, DevicePrincipal, DeviceSecret, FeatureOffer, FeatureSet, Frame,
        LegGeneration, OpenResultCode, OwnerIdentity, ReceiveBudgetLimits, Record,
        ReplayBudgetLimits, ResetReason, ResumeSecret, SESSION_PROTOCOL_VERSION,
        STANDBY_CONTROL_V1, SessionConfig, SessionId, SessionRole, StandbyNonce, TcpWindowLimits,
        TlsExporterBinding, VersionRange,
    };
    use crate::shared::TargetAddr;
    use bytes::Bytes;
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
    use std::time::Duration;

    const STANDBY_FEATURES: u64 = STANDBY_CONTROL_V1.bits() | 0b0111;

    trait AmbiguousIfClone<Marker> {
        fn marker() {}
    }

    impl<T: ?Sized> AmbiguousIfClone<()> for T {}
    impl<T: Clone> AmbiguousIfClone<u8> for T {}

    #[test]
    fn initial_bootstrap_authorities_are_nonclone_capabilities() {
        let _ = <ClientBootstrapError as AmbiguousIfClone<_>>::marker;
        let _ = <PendingOwnerBootstrap as AmbiguousIfClone<_>>::marker;
        let _ = <PendingInitialOwnerBootstrapPublication as AmbiguousIfClone<_>>::marker;
        let _ = <InitialOwnerBootstrapAcceptFailure as AmbiguousIfClone<_>>::marker;
        let _ = <InitialOwnerBootstrapEnqueueFailure as AmbiguousIfClone<_>>::marker;
        let _ = <OwnerSessionBootstrap as AmbiguousIfClone<_>>::marker;
    }

    fn credentials() -> AttachCredentials {
        AttachCredentials::new(
            DeviceSecret::new([0xde; 32]).unwrap(),
            ResumeSecret::new([0xad; 32]).unwrap(),
        )
        .unwrap()
    }

    fn committed_leg() -> crate::resumable::CommittedLeg {
        committed_leg_with(0x11, 0x31, 0x53, b"mini-vpn-owned/1", 0b111)
    }

    fn committed_leg_with(
        session_byte: u8,
        owner_byte: u8,
        principal_byte: u8,
        alpn_bytes: &[u8],
        offered_features: u64,
    ) -> crate::resumable::CommittedLeg {
        committed_leg_with_policy(
            session_byte,
            owner_byte,
            principal_byte,
            alpn_bytes,
            offered_features,
            0b1111,
        )
    }

    fn committed_leg_with_policy(
        session_byte: u8,
        owner_byte: u8,
        principal_byte: u8,
        alpn_bytes: &[u8],
        offered_features: u64,
        supported_features: u64,
    ) -> crate::resumable::CommittedLeg {
        let session_id = SessionId::new([session_byte; 16]).unwrap();
        let owner = OwnerIdentity::new([owner_byte; 32]).unwrap();
        let alpn = AttachAlpn::new(alpn_bytes).unwrap();
        let principal = DevicePrincipal::new([principal_byte; 16]).unwrap();
        let binding = AttachTransportBinding::new(
            owner,
            alpn,
            TlsExporterBinding::new([0x42; 32]).unwrap(),
            principal,
        );
        let request = AttachRequest::new(
            session_id,
            LegGeneration::new(2).unwrap(),
            AttachNonce::new([0x22; 16]).unwrap(),
            VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION).unwrap(),
            FeatureOffer::new(offered_features, 0b001).unwrap(),
        );
        let proof = credentials().prove(&request, &binding).unwrap();
        crate::resumable::AttachAuthority::new(
            session_id,
            LegGeneration::new(1).unwrap(),
            credentials(),
            AttachPolicy::new(
                owner,
                alpn,
                principal,
                SESSION_PROTOCOL_VERSION,
                supported_features,
            )
            .unwrap(),
        )
        .verify_and_commit(&request, &binding, &proof)
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

    fn authority(current_generation: u64) -> AttachAuthority {
        AttachAuthority::new(
            SessionId::new([0x11; 16]).unwrap(),
            LegGeneration::new(current_generation).unwrap(),
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

    fn attach_request(generation: u64, nonce: u8) -> AttachRequest {
        AttachRequest::new(
            SessionId::new([0x11; 16]).unwrap(),
            LegGeneration::new(generation).unwrap(),
            AttachNonce::new([nonce; 16]).unwrap(),
            VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION).unwrap(),
            FeatureOffer::new(0b111, 0b001).unwrap(),
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

    fn bootstrap_owner_and_client() -> (
        SessionSupervisor,
        AttachedLeg,
        SessionSupervisor,
        AttachedLeg,
    ) {
        let (owner, owner_active, client, client_active, _reporter) =
            bootstrap_owner_and_client_with_reporter();
        (owner, owner_active, client, client_active)
    }

    fn bootstrap_owner_and_client_with_reporter() -> (
        SessionSupervisor,
        AttachedLeg,
        SessionSupervisor,
        AttachedLeg,
        LegTransportReporter,
    ) {
        let transport = binding(0x42);
        let limits = LegIoLimits::new(4_096, 8, 32_768, 8, 32_768).unwrap();
        let mut client_leg = LegIo::for_authenticated_transport(
            LegId::A,
            LegEndpointRole::Client,
            transport,
            limits,
        );
        let (mut owner_leg, reporter) = LegIo::for_authenticated_transport_with_reporter(
            LegId::A,
            LegEndpointRole::Owner,
            transport,
            limits,
        );
        let request = standby_attach_request(2, 0x22);
        let proof = credentials().prove(&request, &transport).unwrap();
        let pending_client = client_leg
            .established_leg()
            .begin_attach(request)
            .bind_initial_frame(request.to_attach_frame(proof))
            .unwrap();
        let mut client_queue =
            LegOutboundQueue::for_leg(client_leg.established_leg(), 4, 4_096).unwrap();
        let awaiting = pending_client.enqueue(&mut client_queue).unwrap();
        let mut owner_queue =
            LegOutboundQueue::for_leg(owner_leg.established_leg(), 4, 4_096).unwrap();
        let capacity = WireCapacitySpec::new(1_000_000, Duration::from_secs(1), 4_096, 128, 1)
            .unwrap()
            .derive()
            .unwrap();
        let wire = TwoLegWire::new(WireBounds::new(capacity, 16, 32_768, 2, 2).unwrap());
        let mut wire = MemoryLegTransport::new(wire, MemoryFaultScript::new(0));
        client_queue
            .try_flush(
                &mut client_leg,
                &mut wire.controller_sender(),
                SimTime::ZERO,
            )
            .unwrap()
            .unwrap();
        while wire.advance_one_due(SimTime::ZERO).unwrap() {}
        wire.advance_idle_to(SimTime::ZERO).unwrap();
        let request_delivery = wire
            .try_recv_next(owner_leg.inbound_route())
            .unwrap()
            .unwrap();
        let received = owner_leg.receive_delivery(request_delivery).unwrap();
        let mut pending_owner =
            SessionSupervisor::prepare_owner(session_config(), standby_authority(1));
        let (owner, owner_active) = pending_owner
            .accept(owner_leg.established_leg(), received, &mut owner_queue)
            .unwrap()
            .enqueue_acceptance(&mut owner_queue)
            .unwrap()
            .into_parts();
        owner_queue
            .try_flush(&mut owner_leg, &mut wire.controller_sender(), SimTime::ZERO)
            .unwrap()
            .unwrap();
        while wire.advance_one_due(SimTime::ZERO).unwrap() {}
        wire.advance_idle_to(SimTime::ZERO).unwrap();
        let acceptance_delivery = wire
            .try_recv_next(client_leg.inbound_route())
            .unwrap()
            .unwrap();
        let response = client_leg.receive_delivery(acceptance_delivery).unwrap();
        let accepted = awaiting.validate_response(&client_queue, response).unwrap();
        let (client, client_active) =
            SessionSupervisor::bootstrap_client(session_config(), accepted, &mut client_queue)
                .unwrap()
                .into_parts();
        (owner, owner_active, client, client_active, reporter)
    }

    fn classified_standby_inbound(
        active: &AttachedLeg,
        candidate: &mut LegIo,
        proof_binding: AttachTransportBinding,
        signing_credentials: &AttachCredentials,
        nonce: u8,
    ) -> BoundInbound {
        let request = active
            .standby_registration_request(StandbyNonce::new([nonce; 16]).unwrap())
            .unwrap();
        let proof = signing_credentials
            .prove_standby_registration(&request, &proof_binding)
            .unwrap();
        let frame = request.to_frame(proof).unwrap();
        let bytes = frame
            .encode(FeatureSet::new(STANDBY_FEATURES))
            .unwrap()
            .to_vec();
        let delivery = EncodedDelivery::from_memory_ingress(
            candidate.inbound_route(),
            WireLane::Control,
            bytes,
            1,
            0,
        )
        .unwrap();
        candidate
            .receive_classified_delivery(delivery, FeatureSet::new(STANDBY_FEATURES))
            .unwrap()
    }

    fn standby_leg(exporter: u8) -> LegIo {
        LegIo::for_authenticated_transport(
            LegId::B,
            LegEndpointRole::Owner,
            binding(exporter),
            LegIoLimits::new(4_096, 8, 32_768, 8, 32_768).unwrap(),
        )
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

    fn port_factory(supervisor: &mut SessionSupervisor) -> ResumableTcpPortFactory {
        supervisor
            .mint_tcp_port_factory(FlowPortConfig::new(8, 2, 2, 2).unwrap())
            .unwrap()
    }

    fn owner_receive_pressure_config() -> SessionConfig {
        SessionConfig::new(
            2,
            2,
            TcpWindowLimits::new(8, 2).unwrap(),
            TcpWindowLimits::new(4, 2).unwrap(),
            ReplayBudgetLimits::new(16, 4).unwrap(),
            ReplayBudgetLimits::new(16, 4).unwrap(),
            ReceiveBudgetLimits::new(4, 2).unwrap(),
            4,
        )
        .unwrap()
    }

    fn owner_flow_pressure_config() -> SessionConfig {
        SessionConfig::new(
            1,
            2,
            TcpWindowLimits::new(8, 2).unwrap(),
            TcpWindowLimits::new(8, 2).unwrap(),
            ReplayBudgetLimits::new(16, 4).unwrap(),
            ReplayBudgetLimits::new(16, 4).unwrap(),
            ReceiveBudgetLimits::new(16, 4).unwrap(),
            8,
        )
        .unwrap()
    }

    fn owner_terminal_pressure_config() -> SessionConfig {
        SessionConfig::new(
            2,
            1,
            TcpWindowLimits::new(8, 2).unwrap(),
            TcpWindowLimits::new(8, 2).unwrap(),
            ReplayBudgetLimits::new(16, 4).unwrap(),
            ReplayBudgetLimits::new(16, 4).unwrap(),
            ReceiveBudgetLimits::new(16, 4).unwrap(),
            8,
        )
        .unwrap()
    }

    fn owner_receive_range_pressure_config() -> SessionConfig {
        SessionConfig::new(
            2,
            2,
            TcpWindowLimits::new(8, 2).unwrap(),
            TcpWindowLimits::new(8, 2).unwrap(),
            ReplayBudgetLimits::new(16, 4).unwrap(),
            ReplayBudgetLimits::new(16, 4).unwrap(),
            ReceiveBudgetLimits::new(16, 1).unwrap(),
            8,
        )
        .unwrap()
    }

    fn open_owner_flow(
        supervisor: &mut SessionSupervisor,
        leg: crate::resumable::CommittedLeg,
        raw_flow_id: u64,
    ) -> crate::resumable::LocalFlow {
        let flow_id = SessionFlowId::new(raw_flow_id).unwrap();
        let effects = supervisor
            .apply_event(SessionEvent::PeerFrame {
                leg,
                frame: Frame::try_new(
                    leg.generation(),
                    Record::Open {
                        flow_id,
                        target: target(),
                    },
                )
                .unwrap(),
            })
            .unwrap();
        let (request, flow) = effects
            .into_iter()
            .find_map(|effect| match effect {
                SessionEffect::PeerOpenRequested { request, flow, .. } => Some((request, flow)),
                _ => None,
            })
            .unwrap();
        supervisor
            .apply_event(SessionEvent::PeerOpenResolved {
                request,
                result: OpenResultCode::Opened,
            })
            .unwrap();
        flow
    }

    #[test]
    fn global_receive_pressure_returns_exact_peer_data_for_retry_after_capacity_release() {
        let leg = committed_leg();
        let model = SessionModel::new(SessionRole::Owner, owner_receive_pressure_config(), leg);
        let mut supervisor = SessionSupervisor::new(model);
        let first = open_owner_flow(&mut supervisor, leg, 1);
        let second = open_owner_flow(&mut supervisor, leg, 2);

        let first_effects = supervisor
            .apply_event(SessionEvent::PeerFrame {
                leg,
                frame: Frame::try_new(
                    leg.generation(),
                    Record::Data {
                        flow_id: first.flow_id(),
                        direction: Direction::ClientToTarget,
                        offset: ByteOffset::new(0),
                        payload: Bytes::from_static(b"full"),
                    },
                )
                .unwrap(),
            })
            .unwrap();
        let offer = first_effects
            .into_iter()
            .find_map(|effect| match effect {
                SessionEffect::OfferToSink { offer, .. } => Some(offer),
                _ => None,
            })
            .unwrap();

        let blocked = SessionEvent::PeerFrame {
            leg,
            frame: Frame::try_new(
                leg.generation(),
                Record::Data {
                    flow_id: second.flow_id(),
                    direction: Direction::ClientToTarget,
                    offset: ByteOffset::new(0),
                    payload: Bytes::from(vec![0x5a]),
                },
            )
            .unwrap(),
        };
        let payload_ptr = match &blocked {
            SessionEvent::PeerFrame { frame, .. } => match frame.record() {
                Record::Data { payload, .. } => payload.as_ptr(),
                _ => unreachable!("constructed exact DATA frame"),
            },
            _ => unreachable!("constructed exact peer frame event"),
        };
        let returned = match supervisor.apply_event(blocked) {
            Err(SessionSupervisorError::RejectedEvent {
                block:
                    ReducerAdmissionBlock::ReceiveByteBudgetExceeded {
                        attempted: 5,
                        max: 4,
                    },
                event,
            }) => *event,
            result => panic!("expected exact receive-pressure carrying rejection, got {result:?}"),
        };
        assert!(matches!(
            &returned,
            SessionEvent::PeerFrame { frame, .. }
                if matches!(frame.record(), Record::Data { payload, .. }
                    if payload.as_ptr() == payload_ptr && payload.as_ref() == [0x5a])
        ));

        supervisor
            .apply_event(SessionEvent::SinkAccepted { offer, bytes: 4 })
            .unwrap();
        let retried = supervisor.apply_event(returned).unwrap();
        assert!(retried.into_iter().any(|effect| matches!(
            effect,
            SessionEffect::OfferToSink { offer, .. } if offer.flow_id() == second.flow_id()
        )));
        assert_eq!(supervisor.snapshot().session.receive_owned_bytes(), 1);
    }

    #[test]
    fn global_receive_range_pressure_returns_exact_peer_data() {
        let leg = committed_leg();
        let model = SessionModel::new(
            SessionRole::Owner,
            owner_receive_range_pressure_config(),
            leg,
        );
        let mut supervisor = SessionSupervisor::new(model);
        let first = open_owner_flow(&mut supervisor, leg, 1);
        let second = open_owner_flow(&mut supervisor, leg, 2);
        supervisor
            .apply_event(SessionEvent::PeerFrame {
                leg,
                frame: Frame::try_new(
                    leg.generation(),
                    Record::Data {
                        flow_id: first.flow_id(),
                        direction: Direction::ClientToTarget,
                        offset: ByteOffset::new(1),
                        payload: Bytes::from_static(b"a"),
                    },
                )
                .unwrap(),
            })
            .unwrap();

        let blocked = SessionEvent::PeerFrame {
            leg,
            frame: Frame::try_new(
                leg.generation(),
                Record::Data {
                    flow_id: second.flow_id(),
                    direction: Direction::ClientToTarget,
                    offset: ByteOffset::new(1),
                    payload: Bytes::from_static(b"b"),
                },
            )
            .unwrap(),
        };
        let expected = blocked.clone();
        let returned = match supervisor.apply_event(blocked) {
            Err(SessionSupervisorError::RejectedEvent {
                block:
                    ReducerAdmissionBlock::ReceiveRangeBudgetExceeded {
                        attempted: 2,
                        max: 1,
                    },
                event,
            }) => *event,
            result => panic!("expected receive-range carrying rejection, got {result:?}"),
        };
        assert_eq!(returned, expected);

        supervisor
            .apply_event(SessionEvent::PeerFrame {
                leg,
                frame: Frame::try_new(
                    leg.generation(),
                    Record::Reset {
                        flow_id: first.flow_id(),
                        reason: ResetReason::ResourceExhausted,
                    },
                )
                .unwrap(),
            })
            .unwrap();
        supervisor.apply_event(returned).unwrap();
        assert_eq!(supervisor.snapshot().session.receive_ranges(), 1);
    }

    #[test]
    fn command_carrying_rejection_preserves_event_and_driver_control_lanes() {
        let leg = committed_leg();
        let model = SessionModel::new(SessionRole::Owner, owner_flow_pressure_config(), leg);
        let mut supervisor = SessionSupervisor::new(model);
        let first = open_owner_flow(&mut supervisor, leg, 1);
        let blocked = SessionEvent::PeerFrame {
            leg,
            frame: Frame::try_new(
                leg.generation(),
                Record::Open {
                    flow_id: SessionFlowId::new(2).unwrap(),
                    target: target(),
                },
            )
            .unwrap(),
        };

        let returned_event =
            match supervisor.apply_command(SessionOwnerCommand::Event(blocked.clone())) {
                Err(SessionSupervisorError::RejectedCommand {
                    block: ReducerAdmissionBlock::FlowCapacityExceeded { max: 1 },
                    command,
                }) => match *command {
                    SessionOwnerCommand::Event(event) => event,
                    other => panic!("event command changed lane: {other:?}"),
                },
                result => panic!("expected event-lane carrying rejection, got {result:?}"),
            };
        assert_eq!(returned_event, blocked);

        let returned_control = match supervisor
            .apply_command(SessionOwnerCommand::Driver(DriverInput::Control(blocked)))
        {
            Err(SessionSupervisorError::RejectedCommand {
                block: ReducerAdmissionBlock::FlowCapacityExceeded { max: 1 },
                command,
            }) => match *command {
                SessionOwnerCommand::Driver(DriverInput::Control(event)) => event,
                other => panic!("driver control command changed lane: {other:?}"),
            },
            result => panic!("expected control-lane carrying rejection, got {result:?}"),
        };
        assert_eq!(returned_control, returned_event);

        supervisor
            .apply_event(SessionEvent::PeerFrame {
                leg,
                frame: Frame::try_new(
                    leg.generation(),
                    Record::Reset {
                        flow_id: first.flow_id(),
                        reason: ResetReason::Unspecified,
                    },
                )
                .unwrap(),
            })
            .unwrap();
        assert!(
            supervisor
                .apply_command(SessionOwnerCommand::Event(returned_event))
                .unwrap()
                .into_iter()
                .any(|effect| matches!(effect, SessionEffect::PeerOpenRequested { .. }))
        );
    }

    #[test]
    fn terminal_pressure_returns_exact_peer_reset_until_tombstone_expires() {
        let leg = committed_leg();
        let model = SessionModel::new(SessionRole::Owner, owner_terminal_pressure_config(), leg);
        let mut supervisor = SessionSupervisor::new(model);
        let first = open_owner_flow(&mut supervisor, leg, 1);
        let second = open_owner_flow(&mut supervisor, leg, 2);
        let first_terminal = supervisor
            .apply_event(SessionEvent::PeerFrame {
                leg,
                frame: Frame::try_new(
                    leg.generation(),
                    Record::Reset {
                        flow_id: first.flow_id(),
                        reason: ResetReason::Unspecified,
                    },
                )
                .unwrap(),
            })
            .unwrap()
            .into_iter()
            .find_map(|effect| match effect {
                SessionEffect::FlowFinished { terminal, .. } => Some(terminal),
                _ => None,
            })
            .unwrap();

        let blocked = SessionEvent::PeerFrame {
            leg,
            frame: Frame::try_new(
                leg.generation(),
                Record::Reset {
                    flow_id: second.flow_id(),
                    reason: ResetReason::ResourceExhausted,
                },
            )
            .unwrap(),
        };
        let expected = blocked.clone();
        let returned = match supervisor.apply_event(blocked) {
            Err(SessionSupervisorError::RejectedEvent {
                block: ReducerAdmissionBlock::TerminalTombstoneCapacityExceeded { max: 1 },
                event,
            }) => *event,
            result => panic!("expected terminal carrying rejection, got {result:?}"),
        };
        assert_eq!(returned, expected);

        supervisor
            .apply_event(SessionEvent::TerminalGraceExpired {
                terminal: first_terminal,
            })
            .unwrap();
        let retried = supervisor.apply_event(returned).unwrap();
        assert_eq!(
            retried
                .into_iter()
                .filter(|effect| matches!(effect, SessionEffect::FlowFinished { .. }))
                .count(),
            1
        );
    }

    #[test]
    fn terminal_pressure_returns_exact_final_ack_until_tombstone_expires() {
        let leg = committed_leg();
        let model = SessionModel::new(SessionRole::Owner, owner_terminal_pressure_config(), leg);
        let mut supervisor = SessionSupervisor::new(model);
        let first = open_owner_flow(&mut supervisor, leg, 1);
        let second = open_owner_flow(&mut supervisor, leg, 2);
        let first_terminal = supervisor
            .apply_event(SessionEvent::PeerFrame {
                leg,
                frame: Frame::try_new(
                    leg.generation(),
                    Record::Reset {
                        flow_id: first.flow_id(),
                        reason: ResetReason::Unspecified,
                    },
                )
                .unwrap(),
            })
            .unwrap()
            .into_iter()
            .find_map(|effect| match effect {
                SessionEffect::FlowFinished { terminal, .. } => Some(terminal),
                _ => None,
            })
            .unwrap();

        let half_close = supervisor
            .apply_event(SessionEvent::PeerFrame {
                leg,
                frame: Frame::try_new(
                    leg.generation(),
                    Record::Close {
                        flow_id: second.flow_id(),
                        direction: Direction::ClientToTarget,
                        final_offset: ByteOffset::new(0),
                    },
                )
                .unwrap(),
            })
            .unwrap()
            .into_iter()
            .find_map(|effect| match effect {
                SessionEffect::HalfCloseSink { completion } => Some(completion),
                _ => None,
            })
            .unwrap();
        supervisor
            .apply_event(SessionEvent::SinkHalfClosed {
                completion: half_close,
            })
            .unwrap();
        supervisor
            .apply_event(SessionEvent::LocalClose { flow: second })
            .unwrap();

        let blocked = SessionEvent::PeerFrame {
            leg,
            frame: Frame::try_new(
                leg.generation(),
                Record::Ack {
                    flow_id: second.flow_id(),
                    direction: Direction::TargetToClient,
                    next_accepted: ByteOffset::new(0),
                    final_accepted: true,
                },
            )
            .unwrap(),
        };
        let expected = blocked.clone();
        let returned = match supervisor.apply_event(blocked) {
            Err(SessionSupervisorError::RejectedEvent {
                block: ReducerAdmissionBlock::TerminalTombstoneCapacityExceeded { max: 1 },
                event,
            }) => *event,
            result => panic!("expected final-ACK carrying rejection, got {result:?}"),
        };
        assert_eq!(returned, expected);

        supervisor
            .apply_event(SessionEvent::TerminalGraceExpired {
                terminal: first_terminal,
            })
            .unwrap();
        let retried = supervisor.apply_event(returned).unwrap();
        assert_eq!(
            retried
                .into_iter()
                .filter(|effect| matches!(effect, SessionEffect::FlowFinished { .. }))
                .count(),
            1
        );
    }

    fn target() -> TargetAddr {
        TargetAddr::IpPort(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 443)))
    }

    fn opened_flow(
        supervisor: &mut SessionSupervisor,
        leg: crate::resumable::CommittedLeg,
    ) -> crate::resumable::LocalFlow {
        let effects = supervisor
            .apply_event(SessionEvent::LocalOpen {
                leg,
                target: target(),
            })
            .unwrap();
        let flow = effects
            .iter()
            .find_map(|effect| match effect {
                SessionEffect::LocalFlowOpened { flow } => Some(*flow),
                _ => None,
            })
            .unwrap();
        supervisor
            .apply_event(SessionEvent::PeerFrame {
                leg,
                frame: Frame::try_new(
                    leg.generation(),
                    Record::OpenResult {
                        flow_id: flow.flow_id(),
                        result: OpenResultCode::Opened,
                    },
                )
                .unwrap(),
            })
            .unwrap();
        flow
    }

    #[test]
    fn supervisor_mints_one_exact_factory_and_failed_validation_does_not_consume_the_claim() {
        let leg = committed_leg();
        let model = SessionModel::new(SessionRole::Client, session_config(), leg);
        let mut supervisor = SessionSupervisor::new(model);

        assert_eq!(
            supervisor
                .mint_tcp_port_factory(FlowPortConfig::new(65, 2, 2, 2).unwrap())
                .unwrap_err(),
            TcpPortFactoryMintError::PerFlowBytesExceedReducer {
                requested: 65,
                available: 64,
            }
        );

        let factory = supervisor
            .mint_tcp_port_factory(FlowPortConfig::new(8, 2, 2, 2).unwrap())
            .unwrap();
        assert_eq!(factory.session_uplink_byte_capacity(), 64);
        assert_eq!(factory.per_flow_uplink_segment_capacity(), 8);
        assert_eq!(factory.session_uplink_segment_capacity(), 8);
        assert_eq!(
            supervisor
                .mint_tcp_port_factory(FlowPortConfig::new(8, 2, 2, 2).unwrap())
                .unwrap_err(),
            TcpPortFactoryMintError::AlreadyMinted
        );

        let clone = factory.clone();
        let first = opened_flow(&mut supervisor, leg);
        let second = opened_flow(&mut supervisor, leg);
        drop(factory.open_flow(first).unwrap());
        assert!(matches!(
            clone.open_flow(first),
            Err(FlowPortError::LocalFlowAuthorityNotMonotonic {
                highest: 1,
                attempted: 1,
            })
        ));
        drop(clone.open_flow(second).unwrap());
    }

    #[test]
    fn derived_factory_capacity_failure_does_not_consume_the_one_shot_claim() {
        let leg = committed_leg();
        let too_many_segments = tokio::sync::Semaphore::MAX_PERMITS.checked_add(1).unwrap();
        let config = SessionConfig::new(
            1,
            1,
            TcpWindowLimits::new(8, too_many_segments).unwrap(),
            TcpWindowLimits::new(8, 1).unwrap(),
            ReplayBudgetLimits::new(8, too_many_segments).unwrap(),
            ReplayBudgetLimits::new(8, 1).unwrap(),
            ReceiveBudgetLimits::new(8, 1).unwrap(),
            8,
        )
        .unwrap();
        let model = SessionModel::new(SessionRole::Client, config, leg);
        let mut supervisor = SessionSupervisor::new(model);

        for _ in 0..2 {
            assert!(matches!(
                supervisor.mint_tcp_port_factory(FlowPortConfig::new(8, 1, 1, 1).unwrap()),
                Err(TcpPortFactoryMintError::InvalidDerivedCapacity(
                    FlowPortConfigError::SegmentCapacityTooLarge {
                        scope: "per-flow",
                        value,
                        max,
                    }
                )) if value == too_many_segments && max == tokio::sync::Semaphore::MAX_PERMITS
            ));
            assert!(supervisor.tcp_factory_origin.is_none());
        }
    }

    #[test]
    fn minted_factory_is_prebound_to_the_supervisor_session_and_direction() {
        let leg = committed_leg();
        let model = SessionModel::new(SessionRole::Client, session_config(), leg);
        let mut client = SessionSupervisor::new(model);
        let factory = client
            .mint_tcp_port_factory(FlowPortConfig::new(8, 2, 2, 2).unwrap())
            .unwrap();

        let owner_model = SessionModel::new(SessionRole::Owner, session_config(), leg);
        let mut owner = SessionSupervisor::new(owner_model);
        let owner_flow = open_owner_flow(&mut owner, leg, 1);
        assert!(matches!(
            factory.open_flow(owner_flow),
            Err(FlowPortError::FlowFactoryDirectionMismatch {
                expected: Direction::ClientToTarget,
                actual: Direction::TargetToClient,
            })
        ));

        let other_leg = committed_leg_with(0x12, 0x31, 0x53, b"mini-vpn-owned/1", 0b111);
        let other_model = SessionModel::new(SessionRole::Client, session_config(), other_leg);
        let mut other = SessionSupervisor::new(other_model);
        let other_flow = opened_flow(&mut other, other_leg);
        assert!(matches!(
            factory.open_flow(other_flow),
            Err(FlowPortError::CrossSessionFlowFactory)
        ));
    }

    #[test]
    fn factory_mint_requires_a_healthy_pristine_active_supervisor() {
        let leg = committed_leg();
        let model = SessionModel::new(SessionRole::Client, session_config(), leg);
        let mut with_live_flow = SessionSupervisor::new(model);
        opened_flow(&mut with_live_flow, leg);
        assert!(matches!(
            with_live_flow.mint_tcp_port_factory(FlowPortConfig::new(8, 2, 2, 2).unwrap()),
            Err(TcpPortFactoryMintError::SessionNotPristine {
                phase: crate::resumable::SessionPhase::Active,
                flow_count: 1,
                terminal_tombstones: 0,
                ..
            })
        ));

        let model = SessionModel::new(SessionRole::Client, session_config(), leg);
        let mut expired = SessionSupervisor::new(model);
        expired.apply_event(SessionEvent::LegLost { leg }).unwrap();
        expired
            .apply_event(SessionEvent::ResumeGraceExpired { leg })
            .unwrap();
        assert!(matches!(
            expired.mint_tcp_port_factory(FlowPortConfig::new(8, 2, 2, 2).unwrap()),
            Err(TcpPortFactoryMintError::SessionNotPristine {
                phase: crate::resumable::SessionPhase::Expired,
                flow_count: 0,
                terminal_tombstones: 0,
                ..
            })
        ));

        let model = SessionModel::new(SessionRole::Client, session_config(), leg);
        let mut historically_used = SessionSupervisor::new(model);
        let flow = opened_flow(&mut historically_used, leg);
        let terminal = historically_used
            .apply_event(SessionEvent::LocalReset {
                flow,
                reason: ResetReason::LocalAbandon,
            })
            .unwrap()
            .into_iter()
            .find_map(|effect| match effect {
                SessionEffect::FlowFinished { terminal, .. } => Some(terminal),
                _ => None,
            })
            .unwrap();
        historically_used
            .apply_event(SessionEvent::TerminalGraceExpired { terminal })
            .unwrap();
        let retired = historically_used.snapshot().session;
        assert_eq!(retired.phase(), crate::resumable::SessionPhase::Active);
        assert_eq!(retired.flow_count(), 0);
        assert_eq!(retired.terminal_tombstones(), 0);
        assert!(matches!(
            historically_used.mint_tcp_port_factory(FlowPortConfig::new(8, 2, 2, 2).unwrap()),
            Err(TcpPortFactoryMintError::SessionNotPristine { .. })
        ));
    }

    #[test]
    fn data_from_unbound_test_factory_is_quarantined_before_reducer_mutation() {
        let leg = committed_leg();
        let model = SessionModel::new(SessionRole::Client, session_config(), leg);
        let mut supervisor = SessionSupervisor::new(model);
        let local_flow = opened_flow(&mut supervisor, leg);
        let raw = ResumableTcpPortFactory::new_unbound_for_test(
            FlowPortConfig::new(8, 2, 2, 2).unwrap(),
            64,
            8,
            8,
        )
        .unwrap();
        let (flow, mut driver) = raw.open_flow(local_flow).unwrap();
        flow.try_send_uplink_with(1, || Bytes::from_static(b"x"))
            .unwrap();
        let before = supervisor.snapshot().session;

        assert!(matches!(
            supervisor.apply_command(SessionOwnerCommand::Driver(
                driver.try_recv_next().unwrap().unwrap(),
            )),
            Err(SessionSupervisorError::UplinkFactoryProvenanceMismatch)
        ));
        let poisoned = supervisor.snapshot();
        assert_eq!(poisoned.session, before);
        assert!(poisoned.poisoned);
        assert_eq!(poisoned.uplink_replay_extents, 0);
        assert_eq!(poisoned.quarantined_uplink_extents, 1);
        assert_eq!(poisoned.quarantined_uplink_owned_bytes, 1);
        assert_eq!(raw.session_uplink_owned_bytes(), 1);
        assert_eq!(raw.session_uplink_owned_segments(), 1);
        assert_eq!(
            supervisor
                .mint_tcp_port_factory(FlowPortConfig::new(8, 2, 2, 2).unwrap())
                .unwrap_err(),
            TcpPortFactoryMintError::PoisonedSupervisor
        );

        drop(supervisor);
        assert_eq!(raw.session_uplink_owned_bytes(), 0);
        assert_eq!(raw.session_uplink_owned_segments(), 0);
    }

    #[test]
    fn correctly_provenanced_replay_segment_overrun_is_quarantined_and_poisons() {
        let leg = committed_leg();
        let config = SessionConfig::new(
            2,
            2,
            TcpWindowLimits::new(8, 2).unwrap(),
            TcpWindowLimits::new(8, 2).unwrap(),
            ReplayBudgetLimits::new(8, 1).unwrap(),
            ReplayBudgetLimits::new(8, 1).unwrap(),
            ReceiveBudgetLimits::new(16, 4).unwrap(),
            8,
        )
        .unwrap();
        let model = SessionModel::new(SessionRole::Client, config, leg);
        let mut supervisor = SessionSupervisor::new(model);
        let first_factory = supervisor
            .mint_tcp_port_factory(FlowPortConfig::new(8, 2, 2, 2).unwrap())
            .unwrap();
        let second_factory = first_factory
            .fork_with_independent_ledgers_for_test()
            .unwrap();
        let first_flow = opened_flow(&mut supervisor, leg);
        let second_flow = opened_flow(&mut supervisor, leg);
        let (first, mut first_driver) = first_factory.open_flow(first_flow).unwrap();
        let (second, mut second_driver) = second_factory.open_flow(second_flow).unwrap();

        first
            .try_send_uplink_with(1, || Bytes::from_static(b"a"))
            .unwrap();
        supervisor
            .apply_command(SessionOwnerCommand::Driver(
                first_driver.try_recv_next().unwrap().unwrap(),
            ))
            .unwrap();
        second
            .try_send_uplink_with(1, || Bytes::from_static(b"b"))
            .unwrap();

        assert!(matches!(
            supervisor.apply_command(SessionOwnerCommand::Driver(
                second_driver.try_recv_next().unwrap().unwrap(),
            )),
            Err(SessionSupervisorError::ReducerReplayCapacityInvariant {
                source: SessionError::ReplaySegmentBudgetExceeded {
                    direction: Direction::ClientToTarget,
                    attempted: 2,
                    max: 1,
                }
            })
        ));
        let poisoned = supervisor.snapshot();
        assert!(poisoned.poisoned);
        assert_eq!(poisoned.uplink_replay_extents, 1);
        assert_eq!(poisoned.uplink_replay_owned_bytes, 1);
        assert_eq!(poisoned.quarantined_uplink_extents, 1);
        assert_eq!(poisoned.quarantined_uplink_owned_bytes, 1);
        assert_eq!(first_factory.session_uplink_owned_segments(), 1);
        assert_eq!(second_factory.session_uplink_owned_segments(), 1);

        drop(supervisor);
        assert_eq!(first_factory.session_uplink_owned_segments(), 0);
        assert_eq!(second_factory.session_uplink_owned_segments(), 0);
    }

    #[test]
    fn correctly_provenanced_replay_byte_overrun_is_quarantined_and_poisons() {
        let leg = committed_leg();
        let config = SessionConfig::new(
            2,
            2,
            TcpWindowLimits::new(8, 2).unwrap(),
            TcpWindowLimits::new(8, 2).unwrap(),
            ReplayBudgetLimits::new(1, 2).unwrap(),
            ReplayBudgetLimits::new(1, 2).unwrap(),
            ReceiveBudgetLimits::new(16, 4).unwrap(),
            8,
        )
        .unwrap();
        let model = SessionModel::new(SessionRole::Client, config, leg);
        let mut supervisor = SessionSupervisor::new(model);
        let first_factory = supervisor
            .mint_tcp_port_factory(FlowPortConfig::new(1, 2, 2, 2).unwrap())
            .unwrap();
        let second_factory = first_factory
            .fork_with_independent_ledgers_for_test()
            .unwrap();
        let first_flow = opened_flow(&mut supervisor, leg);
        let second_flow = opened_flow(&mut supervisor, leg);
        let (first, mut first_driver) = first_factory.open_flow(first_flow).unwrap();
        let (second, mut second_driver) = second_factory.open_flow(second_flow).unwrap();

        first
            .try_send_uplink_with(1, || Bytes::from_static(b"a"))
            .unwrap();
        supervisor
            .apply_command(SessionOwnerCommand::Driver(
                first_driver.try_recv_next().unwrap().unwrap(),
            ))
            .unwrap();
        second
            .try_send_uplink_with(1, || Bytes::from_static(b"b"))
            .unwrap();

        assert!(matches!(
            supervisor.apply_command(SessionOwnerCommand::Driver(
                second_driver.try_recv_next().unwrap().unwrap(),
            )),
            Err(SessionSupervisorError::ReducerReplayCapacityInvariant {
                source: SessionError::ReplayByteBudgetExceeded {
                    direction: Direction::ClientToTarget,
                    attempted: 2,
                    max: 1,
                }
            })
        ));
        let poisoned = supervisor.snapshot();
        assert!(poisoned.poisoned);
        assert_eq!(poisoned.uplink_replay_owned_bytes, 1);
        assert_eq!(poisoned.quarantined_uplink_owned_bytes, 1);
        assert_eq!(first_factory.session_uplink_owned_bytes(), 1);
        assert_eq!(second_factory.session_uplink_owned_bytes(), 1);

        drop(supervisor);
        assert_eq!(first_factory.session_uplink_owned_bytes(), 0);
        assert_eq!(second_factory.session_uplink_owned_bytes(), 0);
    }

    #[test]
    fn terminal_superseded_queued_data_releases_staging_without_poisoning() {
        let leg = committed_leg();
        let model = SessionModel::new(SessionRole::Client, session_config(), leg);
        let mut supervisor = SessionSupervisor::new(model);
        let factory = port_factory(&mut supervisor);
        let local_flow = opened_flow(&mut supervisor, leg);
        let (flow, mut driver) = factory.open_flow(local_flow).unwrap();
        flow.try_send_uplink_with(1, || Bytes::from_static(b"s"))
            .unwrap();
        let queued = driver.try_recv_next().unwrap().unwrap();
        supervisor
            .apply_event(SessionEvent::LocalReset {
                flow: local_flow,
                reason: ResetReason::LocalAbandon,
            })
            .unwrap();

        assert!(matches!(
            supervisor.apply_command(SessionOwnerCommand::Driver(queued)),
            Err(SessionSupervisorError::Reducer(SessionError::RetiredFlow(
                flow_id
            ))) if flow_id == local_flow.flow_id()
        ));
        let snapshot = supervisor.snapshot();
        assert!(!snapshot.poisoned);
        assert_eq!(snapshot.uplink_replay_extents, 0);
        assert_eq!(snapshot.quarantined_uplink_extents, 0);
        assert_eq!(factory.session_uplink_owned_bytes(), 0);
        assert_eq!(factory.session_uplink_owned_segments(), 0);
    }

    #[test]
    fn dequeued_uplink_ownership_survives_until_reducer_application_ack() {
        let leg = committed_leg();
        let model = SessionModel::new(SessionRole::Client, session_config(), leg);
        let mut supervisor = SessionSupervisor::new(model);
        let factory = port_factory(&mut supervisor);
        let local_flow = opened_flow(&mut supervisor, leg);
        let (flow, mut driver) = factory.open_flow(local_flow).unwrap();
        let probe = flow.probe();

        flow.try_send_uplink_with(4, || Bytes::from_static(b"data"))
            .unwrap();
        let input = driver.try_recv_next().unwrap().unwrap();
        assert_eq!(probe.snapshot().uplink_owned_bytes(), 4);
        assert_eq!(factory.session_uplink_owned_bytes(), 4);

        let effects = supervisor
            .apply_command(SessionOwnerCommand::Driver(input))
            .unwrap();
        assert!(effects.iter().any(|effect| matches!(
            effect,
            SessionEffect::Transmit(frame)
                if matches!(frame.record(), Record::Data { payload, .. } if payload.as_ref() == b"data")
        )));
        assert_eq!(supervisor.snapshot().uplink_replay_extents, 1);
        assert_eq!(supervisor.snapshot().uplink_replay_owned_bytes, 4);
        assert_eq!(probe.snapshot().uplink_owned_bytes(), 4);
        assert_eq!(factory.session_uplink_owned_bytes(), 4);

        let visible = supervisor
            .apply_event(SessionEvent::PeerFrame {
                leg,
                frame: Frame::try_new(
                    leg.generation(),
                    Record::Ack {
                        flow_id: local_flow.flow_id(),
                        direction: Direction::ClientToTarget,
                        next_accepted: ByteOffset::new(4),
                        final_accepted: false,
                    },
                )
                .unwrap(),
            })
            .unwrap();
        assert!(visible.is_empty());
        assert_eq!(supervisor.snapshot().uplink_replay_extents, 0);
        assert_eq!(supervisor.snapshot().uplink_replay_owned_bytes, 0);
        assert_eq!(probe.snapshot().uplink_owned_bytes(), 0);
        assert_eq!(factory.session_uplink_owned_bytes(), 0);
    }

    #[test]
    fn post_reduce_receipt_mismatch_quarantines_ownership_and_poisons_every_mutator() {
        let leg = committed_leg();
        let model = SessionModel::new(SessionRole::Client, session_config(), leg);
        let mut supervisor = SessionSupervisor::new(model);
        let factory = port_factory(&mut supervisor);
        let duplicate_factory = factory.fork_with_independent_ledgers_for_test().unwrap();
        let local_flow = opened_flow(&mut supervisor, leg);
        let (first, mut first_driver) = factory.open_flow(local_flow).unwrap();
        let (second, mut second_driver) = duplicate_factory.open_flow(local_flow).unwrap();

        first
            .try_send_uplink_with(1, || Bytes::from_static(b"a"))
            .unwrap();
        supervisor
            .apply_command(SessionOwnerCommand::Driver(
                first_driver.try_recv_next().unwrap().unwrap(),
            ))
            .unwrap();

        // A second endpoint for the same capability starts its local source
        // offset at zero. The reducer has already retained this byte at offset
        // one before the adapter can detect the mismatched storage receipt.
        second
            .try_send_uplink_with(1, || Bytes::from_static(b"b"))
            .unwrap();
        assert!(matches!(
            supervisor.apply_command(SessionOwnerCommand::Driver(
                second_driver.try_recv_next().unwrap().unwrap(),
            )),
            Err(SessionSupervisorError::FlowPort(
                FlowPortError::UplinkReplayReceiptMismatch
            ))
        ));

        let poisoned = supervisor.snapshot();
        assert!(poisoned.poisoned);
        assert_eq!(poisoned.uplink_replay_extents, 1);
        assert_eq!(poisoned.uplink_replay_owned_bytes, 1);
        assert_eq!(poisoned.quarantined_uplink_extents, 1);
        assert_eq!(poisoned.quarantined_uplink_owned_bytes, 1);
        assert_eq!(
            poisoned
                .session
                .replay_usage(Direction::ClientToTarget)
                .bytes(),
            2
        );
        assert_eq!(first.probe().snapshot().uplink_owned_segments(), 1);
        assert_eq!(second.probe().snapshot().uplink_owned_segments(), 1);
        assert_eq!(factory.session_uplink_owned_bytes(), 1);
        assert_eq!(factory.session_uplink_owned_segments(), 1);
        assert_eq!(duplicate_factory.session_uplink_owned_bytes(), 1);
        assert_eq!(duplicate_factory.session_uplink_owned_segments(), 1);

        assert!(matches!(
            supervisor.apply_event(SessionEvent::LegLost { leg }),
            Err(SessionSupervisorError::PoisonedOwnershipInvariant)
        ));
        assert!(matches!(
            supervisor.apply_command(SessionOwnerCommand::Event(SessionEvent::LegLost { leg })),
            Err(SessionSupervisorError::PoisonedOwnershipInvariant)
        ));

        let transport = binding(0x71);
        let established = EstablishedLeg::for_authenticated_transport(transport);
        let request = attach_request(3, 0x71);
        let pending = established.begin_attach(request);
        let proof = credentials().prove(&request, &transport).unwrap();
        let committed = authority(2)
            .verify_and_commit(&request, &transport, &proof)
            .unwrap();
        let AttachResponse::Accepted(attached) = pending
            .validate_response(established.bind_received_frame(committed.attach_accepted_frame()))
            .unwrap()
        else {
            panic!("the exact correlated acceptance changed response kind");
        };
        assert!(matches!(
            supervisor.install_attached_leg(&attached),
            Err(SessionSupervisorError::PoisonedOwnershipInvariant)
        ));

        let owner_transport = binding(0x72);
        let owner_leg = EstablishedLeg::for_authenticated_transport(owner_transport);
        let owner_request = attach_request(3, 0x72);
        let owner_proof = credentials()
            .prove(&owner_request, &owner_transport)
            .unwrap();
        assert!(matches!(
            supervisor.accept_owner_attach_for_test(
                &owner_leg,
                owner_leg.bind_received_frame(owner_request.to_attach_frame(owner_proof)),
            ),
            Err(SessionSupervisorError::PoisonedOwnershipInvariant)
        ));

        assert_eq!(supervisor.snapshot(), poisoned);
        drop(supervisor);
        assert_eq!(factory.session_uplink_owned_bytes(), 0);
        assert_eq!(factory.session_uplink_owned_segments(), 0);
        assert_eq!(duplicate_factory.session_uplink_owned_bytes(), 0);
        assert_eq!(duplicate_factory.session_uplink_owned_segments(), 0);
    }

    #[test]
    fn duplicate_replay_extent_keeps_the_original_ownership_and_quarantines_the_new_one() {
        let leg = committed_leg();
        let model = SessionModel::new(SessionRole::Client, session_config(), leg);
        let mut supervisor = SessionSupervisor::new(model);
        let factory = port_factory(&mut supervisor);
        let local_flow = opened_flow(&mut supervisor, leg);
        let (flow, mut driver) = factory.open_flow(local_flow).unwrap();

        flow.try_send_uplink_with(1, || Bytes::from_static(b"a"))
            .unwrap();
        supervisor
            .apply_command(SessionOwnerCommand::Driver(
                driver.try_recv_next().unwrap().unwrap(),
            ))
            .unwrap();

        // Fault-inject the adapter index only: the ownership itself remains
        // the reducer's first 0..1 extent. The next valid DATA produces 1..2,
        // exercising the duplicate-key branch without constructing permits
        // outside the factory.
        let first_key = ReplayExtentKey {
            flow_id: local_flow.flow_id(),
            direction: Direction::ClientToTarget,
            start: ByteOffset::new(0),
            end: ByteOffset::new(1),
        };
        let original = supervisor.uplink_replay.remove(&first_key).unwrap();
        let duplicate_key = ReplayExtentKey {
            start: ByteOffset::new(1),
            end: ByteOffset::new(2),
            ..first_key
        };
        assert!(
            supervisor
                .uplink_replay
                .insert(duplicate_key, original)
                .is_none()
        );

        flow.try_send_uplink_with(1, || Bytes::from_static(b"b"))
            .unwrap();
        assert!(matches!(
            supervisor.apply_command(SessionOwnerCommand::Driver(
                driver.try_recv_next().unwrap().unwrap(),
            )),
            Err(SessionSupervisorError::DuplicateReplayExtent)
        ));

        let retained = supervisor.uplink_replay.get(&duplicate_key).unwrap();
        assert_eq!(retained.start(), ByteOffset::new(0));
        assert_eq!(retained.end(), ByteOffset::new(1));
        let poisoned = supervisor.snapshot();
        assert!(poisoned.poisoned);
        assert_eq!(poisoned.uplink_replay_extents, 1);
        assert_eq!(poisoned.quarantined_uplink_extents, 1);
        assert_eq!(poisoned.uplink_replay_owned_bytes, 1);
        assert_eq!(poisoned.quarantined_uplink_owned_bytes, 1);
        assert_eq!(factory.session_uplink_owned_bytes(), 2);
        assert_eq!(factory.session_uplink_owned_segments(), 2);

        drop(supervisor);
        assert_eq!(factory.session_uplink_owned_bytes(), 0);
        assert_eq!(factory.session_uplink_owned_segments(), 0);
    }

    #[test]
    fn raw_local_data_cannot_bypass_the_owned_driver_command() {
        let leg = committed_leg();
        let model = SessionModel::new(SessionRole::Client, session_config(), leg);
        let mut supervisor = SessionSupervisor::new(model);
        let flow = opened_flow(&mut supervisor, leg);
        let before = supervisor.snapshot();

        assert!(matches!(
            supervisor.apply_event(SessionEvent::LocalData {
                flow,
                payload: Bytes::from_static(b"unowned"),
            }),
            Err(SessionSupervisorError::UnownedLocalData)
        ));
        assert_eq!(supervisor.snapshot(), before);
    }

    #[test]
    fn client_replacement_requires_an_exact_attached_leg_capability() {
        let initial = committed_leg();
        let model = SessionModel::new(SessionRole::Client, session_config(), initial);
        let mut supervisor = SessionSupervisor::new(model);
        let transport = binding(0x71);
        let established = EstablishedLeg::for_authenticated_transport(transport);
        let request = attach_request(3, 0x71);
        let pending = established.begin_attach(request);
        let proof = credentials().prove(&request, &transport).unwrap();
        let committed = authority(2)
            .verify_and_commit(&request, &transport, &proof)
            .unwrap();
        let AttachResponse::Accepted(attached) = pending
            .validate_response(established.bind_received_frame(committed.attach_accepted_frame()))
            .unwrap()
        else {
            panic!("the exact correlated acceptance changed response kind");
        };
        let before = supervisor.snapshot();

        assert!(matches!(
            supervisor.apply_event(attached.replacement_attached_event()),
            Err(SessionSupervisorError::ReplacementProvenanceRequired)
        ));
        assert_eq!(supervisor.snapshot(), before);

        let recovery = supervisor.install_attached_leg(&attached).unwrap();
        assert_eq!(supervisor.snapshot().session.generation().get(), 3);
        assert!(matches!(
            recovery.first(),
            Some(SessionEffect::LegActivated { generation }) if generation.get() == 3
        ));
    }

    #[test]
    fn terminal_normal_replacement_is_inert_before_client_install() {
        let initial = committed_leg();
        let model = SessionModel::new(SessionRole::Client, session_config(), initial);
        let mut supervisor = SessionSupervisor::new(model);
        let transport = binding(0x72);
        let (established, reporter) =
            EstablishedLeg::for_authenticated_transport_with_reporter(transport);
        let request = attach_request(3, 0x72);
        let pending = established.begin_attach(request);
        let proof = credentials().prove(&request, &transport).unwrap();
        let committed = authority(2)
            .verify_and_commit(&request, &transport, &proof)
            .unwrap();
        let AttachResponse::Accepted(attached) = pending
            .validate_response(established.bind_received_frame(committed.attach_accepted_frame()))
            .unwrap()
        else {
            panic!("the exact correlated acceptance changed response kind");
        };
        let before = supervisor.snapshot();
        let terminal = reporter
            .report(LegTransportTerminalReason::PeerClosed)
            .unwrap();

        assert!(matches!(
            supervisor.install_attached_leg(&attached),
            Err(SessionSupervisorError::ReplacementEndpointLost)
        ));
        assert_eq!(supervisor.snapshot(), before);

        // Both strong endpoint owners remain alive, so only the terminal bit
        // can reject installation; allocation destruction is not this proof.
        drop((attached, established, terminal));
    }

    #[test]
    fn terminal_initial_client_leg_is_rejected_before_bootstrap() {
        let transport = binding(0x73);
        let limits = LegIoLimits::new(4_096, 8, 32_768, 8, 32_768).unwrap();
        let (mut client, reporter) = LegIo::for_authenticated_transport_with_reporter(
            LegId::A,
            LegEndpointRole::Client,
            transport,
            limits,
        );
        let mut owner =
            LegIo::for_authenticated_transport(LegId::A, LegEndpointRole::Owner, transport, limits);
        let request = attach_request(2, 0x73);
        let proof = credentials().prove(&request, &transport).unwrap();
        let pending = client
            .established_leg()
            .begin_attach(request)
            .bind_initial_frame(request.to_attach_frame(proof))
            .unwrap();
        let mut client_queue =
            LegOutboundQueue::for_leg(client.established_leg(), 4, 4_096).unwrap();
        let awaiting = pending.enqueue(&mut client_queue).unwrap();
        let mut owner_queue = LegOutboundQueue::for_leg(owner.established_leg(), 4, 4_096).unwrap();
        let capacity = WireCapacitySpec::new(1_000_000, Duration::from_secs(1), 4_096, 128, 1)
            .unwrap()
            .derive()
            .unwrap();
        let wire = TwoLegWire::new(WireBounds::new(capacity, 16, 32_768, 2, 2).unwrap());
        let mut wire = MemoryLegTransport::new(wire, MemoryFaultScript::new(0));
        client_queue
            .try_flush(&mut client, &mut wire.controller_sender(), SimTime::ZERO)
            .unwrap()
            .unwrap();
        while wire.advance_one_due(SimTime::ZERO).unwrap() {}
        wire.advance_idle_to(SimTime::ZERO).unwrap();
        let delivery = wire.try_recv_next(owner.inbound_route()).unwrap().unwrap();
        let received = owner.receive_delivery(delivery).unwrap();
        let mut pending_owner = SessionSupervisor::prepare_owner(session_config(), authority(1));
        let _owner_bootstrap = pending_owner
            .accept(owner.established_leg(), received, &mut owner_queue)
            .unwrap()
            .enqueue_acceptance(&mut owner_queue)
            .unwrap();
        owner_queue
            .try_flush(&mut owner, &mut wire.controller_sender(), SimTime::ZERO)
            .unwrap()
            .unwrap();
        while wire.advance_one_due(SimTime::ZERO).unwrap() {}
        wire.advance_idle_to(SimTime::ZERO).unwrap();
        let delivery = wire.try_recv_next(client.inbound_route()).unwrap().unwrap();
        let response = client.receive_delivery(delivery).unwrap();
        let accepted = awaiting.validate_response(&client_queue, response).unwrap();
        let terminal = reporter.report(LegTransportTerminalReason::Reset).unwrap();

        let error = match SessionSupervisor::bootstrap_client(
            session_config(),
            accepted,
            &mut client_queue,
        ) {
            Ok(_) => panic!("terminal initial leg incorrectly bootstrapped an active client"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), ClientBootstrapErrorKind::EndpointLost);
        assert_eq!(format!("{error:?}"), "ClientBootstrapError([REDACTED])");
        let accepted = error.into_accepted();
        assert!(!accepted.queue_is_live());

        // The rejected bootstrap returns its exact non-cloneable capability,
        // while the independent terminal fact remains linear and fail-closed.
        drop((client, owner, accepted, terminal));
    }

    #[test]
    fn session_expiry_releases_every_adapter_owned_replay_extent() {
        let leg = committed_leg();
        let model = SessionModel::new(SessionRole::Client, session_config(), leg);
        let mut supervisor = SessionSupervisor::new(model);
        let factory = port_factory(&mut supervisor);
        let local_flow = opened_flow(&mut supervisor, leg);
        let (flow, mut driver) = factory.open_flow(local_flow).unwrap();
        let probe = flow.probe();
        flow.try_send_uplink_with(4, || Bytes::from_static(b"held"))
            .unwrap();
        supervisor
            .apply_command(SessionOwnerCommand::Driver(
                driver.try_recv_next().unwrap().unwrap(),
            ))
            .unwrap();
        assert_eq!(supervisor.snapshot().uplink_replay_owned_bytes, 4);
        assert_eq!(factory.session_uplink_owned_bytes(), 4);

        supervisor
            .apply_event(SessionEvent::LegLost { leg })
            .unwrap();
        let effects = supervisor
            .apply_event(SessionEvent::ResumeGraceExpired { leg })
            .unwrap();
        assert!(matches!(
            effects.as_slice(),
            [SessionEffect::SessionExpired]
        ));
        assert_eq!(supervisor.snapshot().uplink_replay_extents, 0);
        assert_eq!(supervisor.snapshot().uplink_replay_owned_bytes, 0);
        assert_eq!(probe.snapshot().uplink_owned_bytes(), 0);
        assert_eq!(factory.session_uplink_owned_bytes(), 0);
    }

    #[tokio::test]
    async fn mailbox_transports_nonclone_driver_data_without_losing_byte_ownership() {
        let leg = committed_leg();
        let model = SessionModel::new(SessionRole::Client, session_config(), leg);
        let mut supervisor = SessionSupervisor::new(model);
        let factory = port_factory(&mut supervisor);
        let local_flow = opened_flow(&mut supervisor, leg);
        let (flow, mut driver) = factory.open_flow(local_flow).unwrap();
        let probe = flow.probe();
        flow.try_send_uplink_with(4, || Bytes::from_static(b"mail"))
            .unwrap();
        let input = driver.try_recv_next().unwrap().unwrap();

        let (ingress, mut inbox) =
            super::super::session::SessionOwnerMailbox::bounded(1, 1).unwrap();
        ingress.try_send_driver(input).unwrap();
        let command = inbox.recv_next().await.unwrap();
        assert_eq!(probe.snapshot().uplink_owned_bytes(), 4);
        supervisor.apply_command(command).unwrap();
        assert_eq!(probe.snapshot().uplink_owned_bytes(), 4);
        assert_eq!(supervisor.snapshot().uplink_replay_owned_bytes, 4);
    }

    #[test]
    fn owner_bootstrap_privately_authenticates_exact_b_without_mutating_model_or_slot() {
        let (owner, owner_active, _client, _client_active) = bootstrap_owner_and_client();
        let mut candidate = standby_leg(0x99);
        let inbound = classified_standby_inbound(
            &owner_active,
            &mut candidate,
            binding(0x99),
            &credentials(),
            0x33,
        );
        let before = owner.snapshot();
        let authority_generation = owner
            .attach_authority
            .as_ref()
            .unwrap()
            .current_generation();
        let slot = OwnerStandbySlot::new();

        let authenticated: ExactAuthenticatedStandby = owner
            .authenticate_owner_standby(&owner_active, candidate.established_leg(), inbound)
            .unwrap();

        assert_eq!(
            format!("{authenticated:?}"),
            "ExactAuthenticatedStandby([REDACTED])"
        );
        assert_eq!(owner.snapshot(), before);
        assert_eq!(
            owner
                .attach_authority
                .as_ref()
                .unwrap()
                .current_generation(),
            authority_generation
        );
        assert!(!slot.registered_is_live());
    }

    #[test]
    fn same_semantic_different_seal_active_cannot_authenticate_owner_standby() {
        let (owner, exact_active, _client, same_contract_wrong_seal) = bootstrap_owner_and_client();
        assert_eq!(
            exact_active.generation(),
            same_contract_wrong_seal.generation()
        );
        assert_eq!(exact_active.nonce(), same_contract_wrong_seal.nonce());
        assert_eq!(
            exact_active.transport_binding(),
            same_contract_wrong_seal.transport_binding()
        );
        let registration_nonce = StandbyNonce::new([0x39; 16]).unwrap();
        assert_eq!(
            exact_active.standby_registration_request(registration_nonce),
            same_contract_wrong_seal.standby_registration_request(registration_nonce)
        );
        let wrong_seal = same_contract_wrong_seal.active_transport_seal();
        assert!(!exact_active.belongs_to_transport(&wrong_seal));

        let mut candidate = standby_leg(0x96);
        let inbound = classified_standby_inbound(
            &same_contract_wrong_seal,
            &mut candidate,
            binding(0x96),
            &credentials(),
            0x39,
        );
        let before = owner.snapshot();
        let authority_generation = owner
            .attach_authority
            .as_ref()
            .unwrap()
            .current_generation();
        let slot = OwnerStandbySlot::new();

        assert!(matches!(
            owner.authenticate_owner_standby(
                &same_contract_wrong_seal,
                candidate.established_leg(),
                inbound,
            ),
            Err(SessionSupervisorError::OwnerStandbyAuthenticationRejected)
        ));
        assert_eq!(owner.snapshot(), before);
        assert_eq!(
            owner
                .attach_authority
                .as_ref()
                .unwrap()
                .current_generation(),
            authority_generation
        );
        assert!(!slot.registered_is_live());

        let exact_inbound = classified_standby_inbound(
            &exact_active,
            &mut candidate,
            binding(0x96),
            &credentials(),
            0x3a,
        );
        let _: ExactAuthenticatedStandby = owner
            .authenticate_owner_standby(&exact_active, candidate.established_leg(), exact_inbound)
            .unwrap();
        assert_eq!(owner.snapshot(), before);
        assert!(!slot.registered_is_live());
    }

    #[test]
    fn standby_authentication_failures_are_uniform_and_leave_owner_and_client_unchanged() {
        let (mut owner, owner_active, client, client_active) = bootstrap_owner_and_client();
        let owner_before = owner.snapshot();
        let client_before = client.snapshot();
        let authority_generation = owner
            .attach_authority
            .as_ref()
            .unwrap()
            .current_generation();
        let slot = OwnerStandbySlot::new();

        let mut client_candidate = standby_leg(0x91);
        let client_inbound = classified_standby_inbound(
            &client_active,
            &mut client_candidate,
            binding(0x91),
            &credentials(),
            0x31,
        );
        assert!(matches!(
            client.authenticate_owner_standby(
                &client_active,
                client_candidate.established_leg(),
                client_inbound,
            ),
            Err(SessionSupervisorError::OwnerStandbyAuthenticationRejected)
        ));

        let mut candidate = standby_leg(0x92);
        let mut wrong_seal = standby_leg(0x93);
        let wrong_seal_inbound = classified_standby_inbound(
            &owner_active,
            &mut wrong_seal,
            binding(0x92),
            &credentials(),
            0x32,
        );
        assert!(matches!(
            owner.authenticate_owner_standby(
                &owner_active,
                candidate.established_leg(),
                wrong_seal_inbound,
            ),
            Err(SessionSupervisorError::OwnerStandbyAuthenticationRejected)
        ));

        let wrong_credentials = AttachCredentials::new(
            DeviceSecret::new([0xba; 32]).unwrap(),
            ResumeSecret::new([0xdb; 32]).unwrap(),
        )
        .unwrap();
        let wrong_proof_inbound = classified_standby_inbound(
            &owner_active,
            &mut candidate,
            binding(0x92),
            &wrong_credentials,
            0x34,
        );
        assert!(matches!(
            owner.authenticate_owner_standby(
                &owner_active,
                candidate.established_leg(),
                wrong_proof_inbound,
            ),
            Err(SessionSupervisorError::OwnerStandbyAuthenticationRejected)
        ));

        assert_eq!(owner.snapshot(), owner_before);
        assert_eq!(client.snapshot(), client_before);
        assert_eq!(
            owner
                .attach_authority
                .as_ref()
                .unwrap()
                .current_generation(),
            authority_generation
        );
        assert!(!slot.registered_is_live());

        owner.poisoned = true;
        let poisoned_before = owner.snapshot();
        let mut poisoned_candidate = standby_leg(0x94);
        let poisoned_inbound = classified_standby_inbound(
            &owner_active,
            &mut poisoned_candidate,
            binding(0x94),
            &credentials(),
            0x35,
        );
        assert!(matches!(
            owner.authenticate_owner_standby(
                &owner_active,
                poisoned_candidate.established_leg(),
                poisoned_inbound,
            ),
            Err(SessionSupervisorError::PoisonedOwnershipInvariant)
        ));
        assert_eq!(owner.snapshot(), poisoned_before);
    }

    #[test]
    fn terminally_legless_owner_rejects_same_contract_standby_authentication_without_mutation() {
        let (mut owner, owner_active, _client, same_contract_active, reporter) =
            bootstrap_owner_and_client_with_reporter();
        let loss = owner_active
            .bind_terminal(
                reporter
                    .report(LegTransportTerminalReason::PeerClosed)
                    .unwrap(),
            )
            .unwrap();
        owner.apply_event(loss.into_event()).unwrap();
        assert_eq!(owner.snapshot().session.phase(), SessionPhase::Legless);

        let mut candidate = standby_leg(0x95);
        let inbound = classified_standby_inbound(
            &same_contract_active,
            &mut candidate,
            binding(0x95),
            &credentials(),
            0x38,
        );
        let before = owner.snapshot();
        let authority_generation = owner
            .attach_authority
            .as_ref()
            .unwrap()
            .current_generation();
        let slot = OwnerStandbySlot::new();

        assert!(matches!(
            owner.authenticate_owner_standby(
                &same_contract_active,
                candidate.established_leg(),
                inbound,
            ),
            Err(SessionSupervisorError::OwnerStandbyAuthenticationRejected)
        ));
        assert_eq!(owner.snapshot(), before);
        assert_eq!(
            owner
                .attach_authority
                .as_ref()
                .unwrap()
                .current_generation(),
            authority_generation
        );
        assert!(!slot.registered_is_live());
    }

    #[test]
    fn stale_active_standby_authentication_is_uniform_and_cannot_advance_authority() {
        let (mut owner, stale_active, _client, _client_active) = bootstrap_owner_and_client();
        let replacement_binding = binding(0x61);
        let replacement = EstablishedLeg::for_authenticated_transport(replacement_binding);
        let request = standby_attach_request(3, 0x23);
        let proof = credentials().prove(&request, &replacement_binding).unwrap();
        let OwnerAttachPublication::Installed {
            attached: current_active,
            ..
        } = owner
            .accept_owner_attach_for_test(
                &replacement,
                replacement.bind_received_frame(request.to_attach_frame(proof)),
            )
            .unwrap()
        else {
            panic!("exact-next replacement unexpectedly returned status");
        };
        let before = owner.snapshot();
        let authority_generation = owner
            .attach_authority
            .as_ref()
            .unwrap()
            .current_generation();
        let slot = OwnerStandbySlot::new();
        let mut candidate = standby_leg(0x99);
        let stale_inbound = classified_standby_inbound(
            &stale_active,
            &mut candidate,
            binding(0x99),
            &credentials(),
            0x36,
        );

        assert!(matches!(
            owner.authenticate_owner_standby(
                &stale_active,
                candidate.established_leg(),
                stale_inbound,
            ),
            Err(SessionSupervisorError::OwnerStandbyAuthenticationRejected)
        ));
        assert_eq!(owner.snapshot(), before);
        assert_eq!(
            owner
                .attach_authority
                .as_ref()
                .unwrap()
                .current_generation(),
            authority_generation
        );
        assert!(!slot.registered_is_live());

        let current_inbound = classified_standby_inbound(
            &current_active,
            &mut candidate,
            binding(0x99),
            &credentials(),
            0x37,
        );
        let _: ExactAuthenticatedStandby = owner
            .authenticate_owner_standby(
                &current_active,
                candidate.established_leg(),
                current_inbound,
            )
            .unwrap();
        assert_eq!(owner.snapshot(), before);
    }

    #[test]
    fn owner_attach_installs_model_before_publishing_acceptance_and_recovery() {
        let initial = committed_leg();
        let model = SessionModel::new(SessionRole::Owner, session_config(), initial);
        let mut supervisor = SessionSupervisor::new_owner(model, authority(2)).unwrap();
        let transport = binding(0x44);
        let leg = EstablishedLeg::for_authenticated_transport(transport);
        let request = attach_request(3, 0x33);
        let proof = credentials().prove(&request, &transport).unwrap();
        let received = leg.bind_received_frame(request.to_attach_frame(proof));

        let publication = supervisor
            .accept_owner_attach_for_test(&leg, received)
            .unwrap();
        assert_eq!(supervisor.snapshot().session.generation().get(), 3);
        let OwnerAttachPublication::Installed {
            attached,
            acceptance,
            recovery,
        } = publication
        else {
            panic!("exact next owner attach unexpectedly returned status");
        };
        assert_eq!(attached.generation().get(), 3);
        assert!(matches!(
            acceptance.record(),
            Record::AttachAccepted { session_id, nonce, .. }
                if *session_id == request.session_id() && *nonce == request.nonce()
        ));

        assert!(matches!(
            recovery.first(),
            Some(SessionEffect::LegActivated { generation }) if generation.get() == 3
        ));
    }

    #[test]
    fn initial_owner_reserves_the_exact_empty_queue_before_generation_commit() {
        let transport = binding(0x40);
        let leg = EstablishedLeg::for_authenticated_transport(transport);
        let mut queue = LegOutboundQueue::for_leg(&leg, 4, 4_096).unwrap();
        queue
            .push(
                Frame::try_new(
                    LegGeneration::new(2).unwrap(),
                    Record::Data {
                        flow_id: SessionFlowId::new(1).unwrap(),
                        direction: Direction::ClientToTarget,
                        offset: ByteOffset::new(0),
                        payload: Bytes::from_static(b"preexisting"),
                    },
                )
                .unwrap(),
            )
            .unwrap();
        let request = attach_request(2, 0x20);
        let proof = credentials().prove(&request, &transport).unwrap();
        let received = leg.bind_received_frame(request.to_attach_frame(proof));
        let mut pending = SessionSupervisor::prepare_owner(session_config(), authority(1));

        let error = pending.accept(&leg, received, &mut queue).unwrap_err();
        assert!(matches!(
            error.kind(),
            InitialOwnerBootstrapAcceptErrorKind::QueueReservation(
                AttachAcceptanceReserveError::NotAwaiting { .. }
            )
        ));
        assert_eq!(pending.authority.as_ref().unwrap().current_generation(), 1);
        assert_eq!(queue.len(), 1);
        let debug = format!("{error:?}");
        assert!(debug.contains("QueueReservation(NotAwaiting"));
        assert!(debug.contains("received: \"[REDACTED]\""));
        assert!(!debug.contains("preexisting"));
        let (_received, reservation) = error.into_parts();
        assert!(reservation.is_none());
    }

    #[test]
    fn initial_owner_rejects_unpublishable_acceptance_capacity_before_generation_commit() {
        let transport = binding(0x40);
        let leg = EstablishedLeg::for_authenticated_transport(transport);
        let mut queue =
            LegOutboundQueue::for_leg(&leg, 1, crate::resumable::ATTACH_ACCEPTED_ENCODED_BYTES - 1)
                .unwrap();
        let request = attach_request(2, 0x20);
        let proof = credentials().prove(&request, &transport).unwrap();
        let received = leg.bind_received_frame(request.to_attach_frame(proof));
        let mut pending = SessionSupervisor::prepare_owner(session_config(), authority(1));
        let error = pending.accept(&leg, received, &mut queue).unwrap_err();
        assert!(matches!(
            error.kind(),
            InitialOwnerBootstrapAcceptErrorKind::QueueReservation(
                AttachAcceptanceReserveError::CapacityExceeded {
                    queued_frames: 0,
                    queued_bytes: 0,
                    incoming_bytes: crate::resumable::ATTACH_ACCEPTED_ENCODED_BYTES,
                }
            )
        ));
        let (received, reservation) = error.into_parts();
        assert!(reservation.is_none());
        assert!(received.belongs_to_transport(&leg));
        assert_eq!(received.attach_request().unwrap(), request);
        assert_eq!(pending.authority.as_ref().unwrap().current_generation(), 1);
        assert!(matches!(
            queue.reserve_attach_acceptance(&leg),
            Err(AttachAcceptanceReserveError::CapacityExceeded { .. })
        ));

        let fresh_leg = EstablishedLeg::for_authenticated_transport(transport);
        let mut exact_queue = LegOutboundQueue::for_leg(
            &fresh_leg,
            1,
            crate::resumable::ATTACH_ACCEPTED_ENCODED_BYTES,
        )
        .unwrap();
        let retry_proof = credentials().prove(&request, &transport).unwrap();
        let publication = pending
            .accept(
                &fresh_leg,
                fresh_leg.bind_received_frame(request.to_attach_frame(retry_proof)),
                &mut exact_queue,
            )
            .unwrap();
        assert!(pending.authority.is_none());
        let bootstrap = publication.enqueue_acceptance(&mut exact_queue).unwrap();
        let (_supervisor, attached) = bootstrap.into_parts();
        assert_eq!(attached.generation().get(), 2);
        assert_eq!(
            exact_queue.owned_bytes(),
            crate::resumable::ATTACH_ACCEPTED_ENCODED_BYTES
        );
    }

    #[test]
    fn initial_owner_exact_terminal_after_commit_yields_legless_high_water_bootstrap() {
        let transport = binding(0x40);
        let (leg, reporter) = EstablishedLeg::for_authenticated_transport_with_reporter(transport);
        let mut queue =
            LegOutboundQueue::for_leg(&leg, 1, crate::resumable::ATTACH_ACCEPTED_ENCODED_BYTES)
                .unwrap();
        let request = attach_request(2, 0x20);
        let proof = credentials().prove(&request, &transport).unwrap();
        let mut pending = SessionSupervisor::prepare_owner(session_config(), authority(1));
        let publication = pending
            .accept(
                &leg,
                leg.bind_received_frame(request.to_attach_frame(proof)),
                &mut queue,
            )
            .unwrap();
        assert!(pending.authority.is_none());

        let (_wrong_leg, wrong_reporter) =
            EstablishedLeg::for_authenticated_transport_with_reporter(binding(0x41));
        let wrong_terminal = wrong_reporter
            .report(LegTransportTerminalReason::FatalIo)
            .unwrap();
        let failure = publication
            .lose_before_acceptance(wrong_terminal)
            .unwrap_err();
        assert_eq!(
            failure.kind(),
            InitialOwnerBootstrapTerminalErrorKind::WrongTerminal
        );
        let (publication, _wrong_terminal) = failure.into_exact_parts().unwrap();
        let terminal = reporter
            .report(LegTransportTerminalReason::FatalIo)
            .unwrap();
        let legless = publication.lose_before_acceptance(terminal).unwrap();
        assert_eq!(legless.snapshot().session.phase(), SessionPhase::Legless);
        assert_eq!(legless.snapshot().session.generation().get(), 2);
        assert_eq!(legless.high_water_generation().get(), 2);
        assert_eq!(queue.len(), 0);
        assert_eq!(queue.owned_bytes(), 0);
    }

    #[test]
    fn rejected_initial_owner_attempts_preserve_the_same_generation_one_authority_for_retry() {
        let mut pending = SessionSupervisor::prepare_owner(session_config(), authority(1));
        let transport = binding(0x40);
        let request = attach_request(2, 0x20);
        let valid_proof = credentials().prove(&request, &transport).unwrap();
        let leg_a = EstablishedLeg::for_authenticated_transport(transport);
        let leg_b = EstablishedLeg::for_authenticated_transport(transport);
        let mut queue_a = LegOutboundQueue::for_leg(&leg_a, 4, 4_096).unwrap();
        let mut queue_b = LegOutboundQueue::for_leg(&leg_b, 4, 4_096).unwrap();

        let wrong_seal = leg_a.bind_received_frame(request.to_attach_frame(valid_proof));
        let error = pending
            .accept(&leg_b, wrong_seal, &mut queue_b)
            .unwrap_err();
        assert_eq!(error.kind(), InitialOwnerBootstrapAcceptErrorKind::WrongLeg);
        let (_received, reservation) = error.into_parts();
        assert!(reservation.is_none());
        assert_eq!(pending.authority.as_ref().unwrap().current_generation(), 1);

        let wrong_credentials = AttachCredentials::new(
            DeviceSecret::new([0xba; 32]).unwrap(),
            ResumeSecret::new([0xdb; 32]).unwrap(),
        )
        .unwrap();
        let bad_proof = wrong_credentials.prove(&request, &transport).unwrap();
        let bad_auth = leg_a.bind_received_frame(request.to_attach_frame(bad_proof));
        let error = pending.accept(&leg_a, bad_auth, &mut queue_a).unwrap_err();
        assert_eq!(error.kind(), InitialOwnerBootstrapAcceptErrorKind::Rejected);
        let (_received, reservation) = error.into_parts();
        assert!(reservation.is_none());
        assert_eq!(pending.authority.as_ref().unwrap().current_generation(), 1);

        let valid = leg_a.bind_received_frame(request.to_attach_frame(valid_proof));
        let (supervisor, attached) = pending
            .accept(&leg_a, valid, &mut queue_a)
            .unwrap()
            .enqueue_acceptance(&mut queue_a)
            .unwrap()
            .into_parts();
        assert!(pending.authority.is_none());
        assert_eq!(supervisor.snapshot().session.generation().get(), 2);
        assert_eq!(attached.generation().get(), 2);
        assert_eq!(queue_a.len(), 1);
        assert!(queue_a.owned_bytes() > 0);

        let retry = attach_request(3, 0x21);
        let retry_proof = credentials().prove(&retry, &transport).unwrap();
        assert!(matches!(
            pending.accept(
                &leg_a,
                leg_a.bind_received_frame(retry.to_attach_frame(retry_proof)),
                &mut queue_a,
            ),
            Err(error) if error.kind() == InitialOwnerBootstrapAcceptErrorKind::AlreadyCompleted
        ));
    }

    #[test]
    fn expired_owner_rejects_attach_before_authority_or_model_advance() {
        let initial = committed_leg();
        let model = SessionModel::new(SessionRole::Owner, session_config(), initial);
        let mut supervisor = SessionSupervisor::new_owner(model, authority(2)).unwrap();
        supervisor
            .apply_event(SessionEvent::LegLost { leg: initial })
            .unwrap();
        supervisor
            .apply_event(SessionEvent::ResumeGraceExpired { leg: initial })
            .unwrap();

        let transport = binding(0x44);
        let leg = EstablishedLeg::for_authenticated_transport(transport);
        let request = attach_request(3, 0x33);
        let proof = credentials().prove(&request, &transport).unwrap();
        let received = leg.bind_received_frame(request.to_attach_frame(proof));
        let before = supervisor.snapshot();

        assert!(matches!(
            supervisor.accept_owner_attach_for_test(&leg, received),
            Err(SessionSupervisorError::Reducer(
                SessionError::SessionExpired
            ))
        ));
        assert_eq!(supervisor.snapshot(), before);
        assert_eq!(
            supervisor
                .attach_authority
                .as_ref()
                .unwrap()
                .current_generation(),
            2
        );
    }

    #[test]
    fn simultaneous_owner_attaches_serialize_commit_install_and_status() {
        let initial = committed_leg();
        let model = SessionModel::new(SessionRole::Owner, session_config(), initial);
        let mut supervisor = SessionSupervisor::new_owner(model, authority(2)).unwrap();

        let transport_a = binding(0x44);
        let leg_a = EstablishedLeg::for_authenticated_transport(transport_a);
        let request_a = attach_request(3, 0x33);
        let proof_a = credentials().prove(&request_a, &transport_a).unwrap();
        let received_a = leg_a.bind_received_frame(request_a.to_attach_frame(proof_a));
        assert!(matches!(
            supervisor
                .accept_owner_attach_for_test(&leg_a, received_a)
                .unwrap(),
            OwnerAttachPublication::Installed { .. }
        ));

        let transport_b = binding(0x45);
        let leg_b = EstablishedLeg::for_authenticated_transport(transport_b);
        let request_b = attach_request(3, 0x34);
        let proof_b = credentials().prove(&request_b, &transport_b).unwrap();
        let received_b = leg_b.bind_received_frame(request_b.to_attach_frame(proof_b));
        let before = supervisor.snapshot();
        let OwnerAttachPublication::Resynchronize { status } = supervisor
            .accept_owner_attach_for_test(&leg_b, received_b)
            .unwrap()
        else {
            panic!("second same-generation owner attach minted a second capability");
        };
        assert!(matches!(
            status.record(),
            Record::AttachGenerationStatus {
                session_id,
                requested_generation,
                nonce,
            } if *session_id == request_b.session_id()
                && *requested_generation == request_b.requested_generation()
                && *nonce == request_b.nonce()
        ));
        assert_eq!(status.leg_generation().get(), 3);
        assert_eq!(supervisor.snapshot(), before);
    }

    fn caught_up_client_with_reporter() -> (
        SessionSupervisor,
        CaughtUpAttachedLeg,
        LegIo,
        LegTransportReporter,
    ) {
        let initial = committed_leg();
        let mut client = SessionSupervisor::new(SessionModel::new(
            SessionRole::Client,
            session_config(),
            initial,
        ));
        client
            .apply_event(SessionEvent::LegLost { leg: initial })
            .unwrap();
        let mut owner = SessionSupervisor::new_owner(
            SessionModel::new(SessionRole::Owner, session_config(), initial),
            authority(2),
        )
        .unwrap();

        let lost_binding = binding(0x71);
        let lost_leg = EstablishedLeg::for_authenticated_transport(lost_binding);
        let lost_request = attach_request(3, 0x71);
        let lost_proof = credentials().prove(&lost_request, &lost_binding).unwrap();
        let OwnerAttachPublication::Installed { .. } = owner
            .accept_owner_attach_for_test(
                &lost_leg,
                lost_leg.bind_received_frame(lost_request.to_attach_frame(lost_proof)),
            )
            .unwrap()
        else {
            panic!("generation 3 setup attach did not commit")
        };

        let status_binding = binding(0x72);
        let status_leg = EstablishedLeg::for_authenticated_transport(status_binding);
        let retry = attach_request(3, 0x72);
        let retry_pending = status_leg.begin_attach(retry);
        let retry_proof = credentials().prove(&retry, &status_binding).unwrap();
        let OwnerAttachPublication::Resynchronize { status } = owner
            .accept_owner_attach_for_test(
                &status_leg,
                status_leg.bind_received_frame(retry.to_attach_frame(retry_proof)),
            )
            .unwrap()
        else {
            panic!("same-generation retry did not return status")
        };
        let AttachResponse::GenerationStatus(status) = retry_pending
            .validate_response(status_leg.bind_received_frame(status))
            .unwrap()
        else {
            panic!("status response minted data-plane authority")
        };

        let follow_up_binding = binding(0x73);
        let (follow_up_endpoint, reporter) = LegIo::for_authenticated_transport_with_reporter(
            LegId::B,
            LegEndpointRole::Client,
            follow_up_binding,
            LegIoLimits::new(4_096, 8, 32_768, 8, 32_768).unwrap(),
        );
        let pending = status
            .begin_catch_up(
                follow_up_endpoint.established_leg(),
                AttachNonce::new([0x73; 16]).unwrap(),
            )
            .unwrap();
        let follow_up = pending.request();
        let proof = credentials()
            .prove(&follow_up, &pending.transport_binding())
            .unwrap();
        let OwnerAttachPublication::Installed { acceptance, .. } = owner
            .accept_owner_attach_for_test(
                follow_up_endpoint.established_leg(),
                follow_up_endpoint
                    .established_leg()
                    .bind_received_frame(follow_up.to_attach_frame(proof)),
            )
            .unwrap()
        else {
            panic!("status-derived generation 4 attach did not commit")
        };
        let caught_up = pending
            .validate_response(
                follow_up_endpoint
                    .established_leg()
                    .bind_received_frame(acceptance),
            )
            .unwrap();
        (client, caught_up, follow_up_endpoint, reporter)
    }

    #[test]
    fn terminal_caught_up_leg_is_inert_before_client_install() {
        let (mut client, caught_up, endpoint, reporter) = caught_up_client_with_reporter();
        let before = client.snapshot();
        let terminal = reporter.report(LegTransportTerminalReason::PeerClosed);

        assert!(matches!(
            client.install_caught_up_leg(&caught_up),
            Err(SessionSupervisorError::ReplacementEndpointLost)
        ));
        assert_eq!(client.snapshot(), before);

        // Keep both endpoint owners alive so only the terminal bit can reject
        // installation; Weak destruction is not part of this proof.
        drop((caught_up, endpoint, terminal));
    }

    #[test]
    fn installed_caught_up_leg_consumes_only_its_exact_terminal_into_one_leg_loss() {
        let (mut client_a, caught_up_a, endpoint_a, reporter_a) = caught_up_client_with_reporter();
        let (mut client_b, caught_up_b, endpoint_b, reporter_b) = caught_up_client_with_reporter();
        client_a.install_caught_up_leg(&caught_up_a).unwrap();
        client_b.install_caught_up_leg(&caught_up_b).unwrap();
        assert_eq!(client_a.snapshot().session.phase(), SessionPhase::Active);
        assert_eq!(client_b.snapshot().session.phase(), SessionPhase::Active);

        let terminal_b = reporter_b
            .report(LegTransportTerminalReason::Reset)
            .unwrap();
        let mismatch = caught_up_a.bind_terminal(terminal_b).unwrap_err();
        assert_eq!(
            format!("{mismatch:?}"),
            "CaughtUpLegTerminalMismatch([REDACTED])"
        );
        let (caught_up_a, terminal_b) = mismatch.into_parts();

        let loss_b = caught_up_b.bind_terminal(terminal_b).unwrap();
        assert_eq!(loss_b.reason(), LegTransportTerminalReason::Reset);
        client_b.apply_event(loss_b.into_event()).unwrap();
        assert_eq!(client_b.snapshot().session.phase(), SessionPhase::Legless);

        let loss_a = caught_up_a
            .bind_terminal(
                reporter_a
                    .report(LegTransportTerminalReason::FatalIo)
                    .unwrap(),
            )
            .unwrap();
        assert_eq!(loss_a.reason(), LegTransportTerminalReason::FatalIo);
        client_a.apply_event(loss_a.into_event()).unwrap();
        assert_eq!(client_a.snapshot().session.phase(), SessionPhase::Legless);

        drop((endpoint_a, endpoint_b));
    }

    #[test]
    fn lost_owner_acceptance_catches_up_the_live_client_through_exact_leg_seals() {
        let initial = committed_leg();
        let mut client = SessionSupervisor::new(SessionModel::new(
            SessionRole::Client,
            session_config(),
            initial,
        ));
        let factory = port_factory(&mut client);
        let local_flow = opened_flow(&mut client, initial);
        let (flow, mut driver) = factory.open_flow(local_flow).unwrap();
        flow.try_send_uplink_with(4, || Bytes::from_static(b"held"))
            .unwrap();
        client
            .apply_command(SessionOwnerCommand::Driver(
                driver.try_recv_next().unwrap().unwrap(),
            ))
            .unwrap();
        client
            .apply_event(SessionEvent::LegLost { leg: initial })
            .unwrap();
        assert_eq!(client.snapshot().uplink_replay_owned_bytes, 4);

        let mut owner = SessionSupervisor::new_owner(
            SessionModel::new(SessionRole::Owner, session_config(), initial),
            authority(2),
        )
        .unwrap();

        // Generation 3 commits on the owner, but its correlated acceptance is
        // lost before the client can install it.
        let lost_binding = binding(0x61);
        let lost_leg = EstablishedLeg::for_authenticated_transport(lost_binding);
        let lost_request = attach_request(3, 0x61);
        let lost_proof = credentials().prove(&lost_request, &lost_binding).unwrap();
        let lost_pending = lost_leg.begin_attach(lost_request);
        let OwnerAttachPublication::Installed { .. } = owner
            .accept_owner_attach_for_test(
                &lost_leg,
                lost_leg.bind_received_frame(lost_request.to_attach_frame(lost_proof)),
            )
            .unwrap()
        else {
            panic!("exact generation 3 attach did not commit");
        };
        drop(lost_pending);
        assert_eq!(owner.snapshot().session.generation().get(), 3);
        assert_eq!(client.snapshot().session.generation().get(), 2);

        // A fresh exact leg retries generation 3 and receives only the
        // authenticated owner high-water status.
        let status_binding = binding(0x62);
        let status_leg = EstablishedLeg::for_authenticated_transport(status_binding);
        let retry = attach_request(3, 0x62);
        let retry_pending = status_leg.begin_attach(retry);
        let retry_proof = credentials().prove(&retry, &status_binding).unwrap();
        let OwnerAttachPublication::Resynchronize { status } = owner
            .accept_owner_attach_for_test(
                &status_leg,
                status_leg.bind_received_frame(retry.to_attach_frame(retry_proof)),
            )
            .unwrap()
        else {
            panic!("stale generation 3 retry did not return status");
        };
        let AttachResponse::GenerationStatus(status) = retry_pending
            .validate_response(status_leg.bind_received_frame(status))
            .unwrap()
        else {
            panic!("status response minted data-plane authority");
        };

        // The status capability transfers to a distinct authenticated leg,
        // which alone may validate generation 4 and install the client.
        let follow_up_leg = EstablishedLeg::for_authenticated_transport(binding(0x63));
        let pending = status
            .begin_catch_up(&follow_up_leg, AttachNonce::new([0x63; 16]).unwrap())
            .unwrap();
        let follow_up = pending.request();
        let proof = credentials()
            .prove(&follow_up, &pending.transport_binding())
            .unwrap();
        let OwnerAttachPublication::Installed { acceptance, .. } = owner
            .accept_owner_attach_for_test(
                &follow_up_leg,
                follow_up_leg.bind_received_frame(follow_up.to_attach_frame(proof)),
            )
            .unwrap()
        else {
            panic!("status-derived generation 4 attach did not commit");
        };
        let caught_up = pending
            .validate_response(follow_up_leg.bind_received_frame(acceptance))
            .unwrap();
        let recovery = client.install_caught_up_leg(&caught_up).unwrap();

        assert_eq!(owner.snapshot().session.generation().get(), 4);
        assert_eq!(client.snapshot().session.generation().get(), 4);
        assert_eq!(client.snapshot().session.flow_count(), 1);
        assert_eq!(client.snapshot().uplink_replay_extents, 1);
        assert_eq!(client.snapshot().uplink_replay_owned_bytes, 4);
        assert!(recovery.iter().any(|effect| matches!(
            effect,
            SessionEffect::Transmit(frame)
                if matches!(frame.record(), Record::Data { payload, .. } if payload.as_ref() == b"held")
        )));
    }

    #[test]
    fn mismatched_owner_authority_is_rejected_before_any_attach_commit() {
        let initial = committed_leg();
        let model = SessionModel::new(SessionRole::Owner, session_config(), initial);
        assert!(matches!(
            SessionSupervisor::new_owner(model, authority(1)),
            Err(SessionSupervisorError::AuthorityModelGenerationMismatch {
                model: 2,
                authority: 1,
            })
        ));
    }

    #[test]
    fn owner_supervisor_rejects_client_role_before_exposing_attach_authority() {
        let initial = committed_leg();
        let model = SessionModel::new(SessionRole::Client, session_config(), initial);

        assert!(matches!(
            SessionSupervisor::new_owner(model, authority(2)),
            Err(SessionSupervisorError::OwnerRoleRequired {
                actual: SessionRole::Client,
            })
        ));
    }

    #[test]
    fn owner_supervisor_rejects_cross_session_same_generation_authority() {
        let other_session = committed_leg_with(0x12, 0x31, 0x53, b"mini-vpn-owned/1", 0b111);
        let model = SessionModel::new(SessionRole::Owner, session_config(), other_session);

        assert!(matches!(
            SessionSupervisor::new_owner(model, authority(2)),
            Err(SessionSupervisorError::AuthorityModelSessionMismatch)
        ));
    }

    #[test]
    fn owner_supervisor_rejects_stable_or_negotiated_policy_drift_at_construction() {
        let mismatches = [
            committed_leg_with(0x11, 0x32, 0x53, b"mini-vpn-owned/1", 0b111),
            committed_leg_with(0x11, 0x31, 0x54, b"mini-vpn-owned/1", 0b111),
            committed_leg_with(0x11, 0x31, 0x53, b"mini-vpn-other/1", 0b111),
            committed_leg_with_policy(0x11, 0x31, 0x53, b"mini-vpn-owned/1", 0b1_0001, 0b1_1111),
        ];

        for mismatch in mismatches {
            let model = SessionModel::new(SessionRole::Owner, session_config(), mismatch);
            assert!(matches!(
                SessionSupervisor::new_owner(model, authority(2)),
                Err(SessionSupervisorError::AuthorityModelSemanticsMismatch)
            ));
        }
    }

    #[test]
    fn attach_semantic_mismatch_cannot_advance_authority_or_model() {
        let initial = committed_leg();
        let model = SessionModel::new(SessionRole::Owner, session_config(), initial);
        let mut supervisor = SessionSupervisor::new_owner(model, authority(2)).unwrap();
        let before = supervisor.snapshot();

        let cross_session_transport = binding(0x43);
        let cross_session_leg =
            EstablishedLeg::for_authenticated_transport(cross_session_transport);
        let cross_session_request = AttachRequest::new(
            SessionId::new([0x12; 16]).unwrap(),
            LegGeneration::new(3).unwrap(),
            AttachNonce::new([0x32; 16]).unwrap(),
            VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION).unwrap(),
            FeatureOffer::new(0b111, 0b001).unwrap(),
        );
        let cross_session_proof = credentials()
            .prove(&cross_session_request, &cross_session_transport)
            .unwrap();
        assert!(matches!(
            supervisor.accept_owner_attach_for_test(
                &cross_session_leg,
                cross_session_leg.bind_received_frame(
                    cross_session_request.to_attach_frame(cross_session_proof),
                ),
            ),
            Err(SessionSupervisorError::LegProvenance(
                LegProvenanceError::Rejected
            ))
        ));
        assert_eq!(supervisor.snapshot(), before);
        assert_eq!(
            supervisor
                .attach_authority
                .as_ref()
                .unwrap()
                .current_generation(),
            2
        );

        let wrong_identity = AttachTransportBinding::new(
            OwnerIdentity::new([0x32; 32]).unwrap(),
            AttachAlpn::new(b"mini-vpn-owned/1").unwrap(),
            TlsExporterBinding::new([0x44; 32]).unwrap(),
            DevicePrincipal::new([0x53; 16]).unwrap(),
        );
        let wrong_leg = EstablishedLeg::for_authenticated_transport(wrong_identity);
        let request = attach_request(3, 0x33);
        let proof = credentials().prove(&request, &wrong_identity).unwrap();
        assert!(matches!(
            supervisor.accept_owner_attach_for_test(
                &wrong_leg,
                wrong_leg.bind_received_frame(request.to_attach_frame(proof)),
            ),
            Err(SessionSupervisorError::LegProvenance(
                LegProvenanceError::Rejected
            ))
        ));
        assert_eq!(supervisor.snapshot(), before);

        let transport = binding(0x45);
        let feature_leg = EstablishedLeg::for_authenticated_transport(transport);
        let feature_request = AttachRequest::new(
            SessionId::new([0x11; 16]).unwrap(),
            LegGeneration::new(3).unwrap(),
            AttachNonce::new([0x34; 16]).unwrap(),
            VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION).unwrap(),
            FeatureOffer::new(0b011, 0b001).unwrap(),
        );
        let feature_proof = credentials().prove(&feature_request, &transport).unwrap();
        assert!(matches!(
            supervisor.accept_owner_attach_for_test(
                &feature_leg,
                feature_leg.bind_received_frame(feature_request.to_attach_frame(feature_proof)),
            ),
            Err(SessionSupervisorError::Reducer(
                SessionError::LegCapabilityMismatch
            ))
        ));
        assert_eq!(supervisor.snapshot(), before);
        assert_eq!(
            supervisor
                .attach_authority
                .as_ref()
                .unwrap()
                .current_generation(),
            2
        );
    }

    #[test]
    fn raw_owner_replacement_cannot_bypass_the_attach_transaction() {
        let initial = committed_leg();
        let model = SessionModel::new(SessionRole::Owner, session_config(), initial);
        let mut supervisor = SessionSupervisor::new_owner(model, authority(2)).unwrap();
        let transport = binding(0x70);
        let request = attach_request(3, 0x70);
        let proof = credentials().prove(&request, &transport).unwrap();
        let candidate = authority(2)
            .verify_and_commit(&request, &transport, &proof)
            .unwrap();
        let before = supervisor.snapshot();

        assert!(matches!(
            supervisor.apply_event(SessionEvent::ReplacementAttached { leg: candidate }),
            Err(SessionSupervisorError::ReplacementProvenanceRequired)
        ));
        assert_eq!(supervisor.snapshot(), before);
        assert_eq!(
            supervisor
                .attach_authority
                .as_ref()
                .unwrap()
                .current_generation(),
            2
        );
    }
}
