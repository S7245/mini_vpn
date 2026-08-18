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
    AttachAuthority, ByteOffset, Direction, Frame, SessionConfig, SessionEffect, SessionError,
    SessionEvent, SessionFlowId, SessionModel, SessionPhase, SessionRole, SessionSnapshot,
};

use super::leg::{
    AttachedLeg, CaughtUpAttachedLeg, EstablishedLeg, LegBoundFrame, LegProvenanceError,
    OwnerAttachTransaction,
};
use super::session::SessionOwnerCommand;
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
}

enum QuarantinedUplinkOwnership {
    Staging(UplinkOwnership),
    Replay(UplinkReplayOwnership),
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

    /// Constructs the initial client model from one exact, correlated
    /// ATTACH_ACCEPTED capability while retaining its live transport seal.
    pub(crate) fn bootstrap_client(
        config: SessionConfig,
        attached: AttachedLeg,
    ) -> ClientSessionBootstrap {
        let model = attached.initial_client_model(config);
        ClientSessionBootstrap {
            supervisor: Self {
                model,
                attach_authority: None,
                tcp_factory_origin: None,
                uplink_replay: BTreeMap::new(),
                poisoned: false,
                quarantined_uplink: Vec::new(),
            },
            attached,
        }
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
            tcp_factory_origin: None,
            uplink_replay: BTreeMap::new(),
            poisoned: false,
            quarantined_uplink: Vec::new(),
        })
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
        if snapshot.phase() != SessionPhase::Active
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

    /// Serializes owner authentication commit, reducer installation, and
    /// response publication ownership in one non-awaiting turn.
    ///
    /// A committed publication is returned only after the owner reducer has
    /// installed the exact same-leg capability. The caller must queue the
    /// acceptance on that leg before dispatching the returned recovery effects.
    pub(crate) fn accept_owner_attach(
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
            .transact_owner_attach(received, authority, &mut self.model)?
            .map_err(SessionSupervisorError::Reducer)?;
        match outcome {
            OwnerAttachTransaction::Installed {
                attached,
                acceptance,
                recovery,
            } => {
                let recovery = self.consume_after_model_commit(recovery)?;
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
    pub(crate) fn install_caught_up_leg(
        &mut self,
        leg: &CaughtUpAttachedLeg,
    ) -> Result<Vec<SessionEffect>, SessionSupervisorError> {
        self.install_client_replacement(leg.replacement_caught_up_event())
    }

    /// Installs a normal client replacement only from the exact live-leg
    /// capability minted by its correlated ATTACH_ACCEPTED response.
    pub(crate) fn install_attached_leg(
        &mut self,
        leg: &AttachedLeg,
    ) -> Result<Vec<SessionEffect>, SessionSupervisorError> {
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
            Ok(visible) => Ok(visible),
            Err(error) => {
                self.poisoned = true;
                Err(error)
            }
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
    pub(crate) fn accept(
        &mut self,
        leg: &EstablishedLeg,
        received: LegBoundFrame,
    ) -> Result<OwnerSessionBootstrap, SessionSupervisorError> {
        let authority = self
            .authority
            .take()
            .ok_or(SessionSupervisorError::OwnerBootstrapCompleted)?;
        let authenticated = match leg.authenticate_initial_owner_attach(received, &authority) {
            Ok(authenticated) => authenticated,
            Err(error) => {
                self.authority = Some(authority);
                return Err(SessionSupervisorError::LegProvenance(error));
            }
        };
        let (model, attached, acceptance) = authenticated.into_owner_parts(self.config);
        let supervisor = SessionSupervisor {
            model,
            attach_authority: Some(authority),
            tcp_factory_origin: None,
            uplink_replay: BTreeMap::new(),
            poisoned: false,
            quarantined_uplink: Vec::new(),
        };
        Ok(OwnerSessionBootstrap {
            supervisor,
            attached,
            acceptance,
        })
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

/// Fully installed initial owner session. Acceptance becomes accessible only
/// together with the matching supervisor and exact live-leg capability.
pub(crate) struct OwnerSessionBootstrap {
    supervisor: SessionSupervisor,
    attached: AttachedLeg,
    acceptance: Frame,
}

impl OwnerSessionBootstrap {
    pub(crate) fn into_parts(self) -> (SessionSupervisor, AttachedLeg, Frame) {
        (self.supervisor, self.attached, self.acceptance)
    }
}

impl fmt::Debug for OwnerSessionBootstrap {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OwnerSessionBootstrap")
            .field("snapshot", &self.supervisor.snapshot())
            .finish_non_exhaustive()
    }
}

/// Fully installed initial client session retaining the exact accepted leg.
pub(crate) struct ClientSessionBootstrap {
    supervisor: SessionSupervisor,
    attached: AttachedLeg,
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
    use crate::owned_upstream::leg::AttachResponse;
    use crate::owned_upstream::{FlowPortConfig, ResumableTcpPortFactory};
    use crate::resumable::{
        AttachAlpn, AttachCredentials, AttachNonce, AttachPolicy, AttachRequest,
        AttachTransportBinding, DevicePrincipal, DeviceSecret, FeatureOffer, Frame, LegGeneration,
        OpenResultCode, OwnerIdentity, ReceiveBudgetLimits, Record, ReplayBudgetLimits,
        ResetReason, ResumeSecret, SESSION_PROTOCOL_VERSION, SessionConfig, SessionId, SessionRole,
        TcpWindowLimits, TlsExporterBinding, VersionRange,
    };
    use crate::shared::TargetAddr;
    use bytes::Bytes;
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

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

    fn attach_request(generation: u64, nonce: u8) -> AttachRequest {
        AttachRequest::new(
            SessionId::new([0x11; 16]).unwrap(),
            LegGeneration::new(generation).unwrap(),
            AttachNonce::new([nonce; 16]).unwrap(),
            VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION).unwrap(),
            FeatureOffer::new(0b111, 0b001).unwrap(),
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
            supervisor.accept_owner_attach(
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
    fn owner_attach_installs_model_before_publishing_acceptance_and_recovery() {
        let initial = committed_leg();
        let model = SessionModel::new(SessionRole::Owner, session_config(), initial);
        let mut supervisor = SessionSupervisor::new_owner(model, authority(2)).unwrap();
        let transport = binding(0x44);
        let leg = EstablishedLeg::for_authenticated_transport(transport);
        let request = attach_request(3, 0x33);
        let proof = credentials().prove(&request, &transport).unwrap();
        let received = leg.bind_received_frame(request.to_attach_frame(proof));

        let publication = supervisor.accept_owner_attach(&leg, received).unwrap();
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
    fn rejected_initial_owner_attempts_preserve_the_same_generation_one_authority_for_retry() {
        let mut pending = SessionSupervisor::prepare_owner(session_config(), authority(1));
        let transport = binding(0x40);
        let request = attach_request(2, 0x20);
        let valid_proof = credentials().prove(&request, &transport).unwrap();
        let leg_a = EstablishedLeg::for_authenticated_transport(transport);
        let leg_b = EstablishedLeg::for_authenticated_transport(transport);

        let wrong_seal = leg_a.bind_received_frame(request.to_attach_frame(valid_proof));
        assert!(matches!(
            pending.accept(&leg_b, wrong_seal),
            Err(SessionSupervisorError::LegProvenance(
                LegProvenanceError::WrongLeg
            ))
        ));
        assert_eq!(pending.authority.as_ref().unwrap().current_generation(), 1);

        let wrong_credentials = AttachCredentials::new(
            DeviceSecret::new([0xba; 32]).unwrap(),
            ResumeSecret::new([0xdb; 32]).unwrap(),
        )
        .unwrap();
        let bad_proof = wrong_credentials.prove(&request, &transport).unwrap();
        let bad_auth = leg_a.bind_received_frame(request.to_attach_frame(bad_proof));
        assert!(matches!(
            pending.accept(&leg_a, bad_auth),
            Err(SessionSupervisorError::LegProvenance(
                LegProvenanceError::Rejected
            ))
        ));
        assert_eq!(pending.authority.as_ref().unwrap().current_generation(), 1);

        let valid = leg_a.bind_received_frame(request.to_attach_frame(valid_proof));
        let (supervisor, attached, acceptance) =
            pending.accept(&leg_a, valid).unwrap().into_parts();
        assert!(pending.authority.is_none());
        assert_eq!(supervisor.snapshot().session.generation().get(), 2);
        assert_eq!(attached.generation().get(), 2);
        assert!(matches!(
            acceptance.record(),
            Record::AttachAccepted { session_id, nonce, .. }
                if *session_id == request.session_id() && *nonce == request.nonce()
        ));

        let retry = attach_request(3, 0x21);
        let retry_proof = credentials().prove(&retry, &transport).unwrap();
        assert!(matches!(
            pending.accept(
                &leg_a,
                leg_a.bind_received_frame(retry.to_attach_frame(retry_proof)),
            ),
            Err(SessionSupervisorError::OwnerBootstrapCompleted)
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
            supervisor.accept_owner_attach(&leg, received),
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
            supervisor.accept_owner_attach(&leg_a, received_a).unwrap(),
            OwnerAttachPublication::Installed { .. }
        ));

        let transport_b = binding(0x45);
        let leg_b = EstablishedLeg::for_authenticated_transport(transport_b);
        let request_b = attach_request(3, 0x34);
        let proof_b = credentials().prove(&request_b, &transport_b).unwrap();
        let received_b = leg_b.bind_received_frame(request_b.to_attach_frame(proof_b));
        let before = supervisor.snapshot();
        let OwnerAttachPublication::Resynchronize { status } =
            supervisor.accept_owner_attach(&leg_b, received_b).unwrap()
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
            .accept_owner_attach(
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
            .accept_owner_attach(
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
            .accept_owner_attach(
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
            supervisor.accept_owner_attach(
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
            supervisor.accept_owner_attach(
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
            supervisor.accept_owner_attach(
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
