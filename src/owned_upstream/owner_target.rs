//! Serialized owner-side Target effect execution for one resumable session.
//!
//! This is the production-shared boundary between the pure session reducer and
//! a concrete [`TargetIo`].  It consumes reducer-issued capabilities, performs
//! the corresponding Target operation exactly once, and feeds only the exact
//! completion back through [`SessionSupervisor`].  It never manufactures an
//! ACK, replay receipt, terminal capability, or raw `LocalData` event.

use std::collections::{BTreeMap, VecDeque};
use std::fmt;

use thiserror::Error;

use crate::resumable::{
    FlowFinishReason, Frame, LegGeneration, OpenResultCode, PeerOpenRequest, Record, ResetReason,
    SessionEffect, SessionEvent, SessionFlowId, SinkOffer, TcpDataSegment, TerminalGrace,
};

use super::leg::{AttachedLeg, EstablishedLeg, LegBoundFrame};
use super::session::SessionOwnerCommand;
use super::supervisor::{
    OwnerAttachPublication, ReducerAdmissionBlock, SessionSupervisor, SessionSupervisorError,
    SessionSupervisorSnapshot, TcpPortFactoryMintError,
};
use super::target::{
    TargetCancelCompletion, TargetHalfCloseCompletion, TargetIo, TargetIoError,
    TargetJoinCompletion, TargetOpenCompletion, TargetOpenFailure, TargetResetCompletion,
    TargetTerminalKind, TargetTombstoneRetireCompletion, TargetWriteCompletion,
};
use super::tcp::{
    DriverInput, FlowPortConfig, FlowPortConfigError, FlowPortError, ResumableTcpDriver,
    ResumableTcpFlow, ResumableTcpPortFactory,
};

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
            state @ (TcpPortFactoryMintError::PoisonedSupervisor
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

/// Non-cloneable authority to execute exactly one following bounded effect
/// turn. Transport must schedule this capability once and cannot manufacture
/// a second wakeup for the same pending work.
#[must_use = "pending owner Target effects require exactly one resumed turn"]
#[derive(PartialEq, Eq)]
pub(crate) struct OwnerTargetResume {
    id: u64,
}

impl fmt::Debug for OwnerTargetResume {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OwnerTargetResume([REDACTED])")
    }
}

/// Owner attach result whose recovery work remains deliberately unpublished.
///
/// On `Installed`, transport must enqueue `acceptance` on the exact attached
/// leg before passing `recovery` back to [`OwnerTargetExecutor`].  Keeping the
/// reducer effects opaque prevents a caller from dispatching replay before the
/// serialized attach barrier has published its acceptance.
#[must_use = "owner attach publication must send acceptance/status and retain recovery ordering"]
pub(crate) enum OwnerTargetAttachPublication {
    Installed {
        attached: AttachedLeg,
        acceptance: Frame,
        recovery: PendingOwnerTargetRecovery,
    },
    Resynchronize {
        status: Frame,
    },
}

impl fmt::Debug for OwnerTargetAttachPublication {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Installed { recovery, .. } => formatter
                .debug_struct("Installed")
                .field("recovery_effects", &recovery.effects.len())
                .finish_non_exhaustive(),
            Self::Resynchronize { .. } => formatter
                .debug_struct("Resynchronize")
                .finish_non_exhaustive(),
        }
    }
}

/// Non-cloneable post-acceptance owner recovery work.
#[must_use = "owner attach recovery must be dispatched after acceptance or explicitly aborted"]
pub(crate) struct PendingOwnerTargetRecovery {
    effects: Vec<SessionEffect>,
}

impl fmt::Debug for PendingOwnerTargetRecovery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PendingOwnerTargetRecovery")
            .field("effects", &self.effects.len())
            .finish()
    }
}

/// Fail-closed ownership returned only if the audited reducer fan-out bound is
/// violated. Keeping the generated effects inside the error prevents a
/// post-mutation drop from masquerading as successful continuation.
pub(crate) struct AbortedOwnerTargetEffects {
    effects: VecDeque<OwnerTargetWork>,
    outputs: Vec<OwnerTargetOutput>,
}

impl fmt::Debug for AbortedOwnerTargetEffects {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AbortedOwnerTargetEffects")
            .field("effects", &self.effects.len())
            .field("outputs", &self.outputs.len())
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
    pub(crate) aborted_effects: usize,
    pub(crate) aborted_outputs: usize,
}

struct PendingTargetWrite {
    offer: SinkOffer,
    segments: Vec<TcpDataSegment>,
}

struct TargetFlowOwner {
    target_present: bool,
    source: Option<TargetReadSource>,
    pending_write: Option<PendingTargetWrite>,
    terminal: Option<TargetTerminalKind>,
    join_pending: bool,
    retire_after_join: bool,
    pending_completion: Option<PendingTargetCompletion>,
}

enum PendingTargetCompletion {
    Open {
        request: PeerOpenRequest,
        result: OpenResultCode,
    },
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

impl PendingTargetCompletion {
    const fn kind(&self) -> PendingTargetCompletionKind {
        match self {
            Self::Open { .. } => PendingTargetCompletionKind::Open,
            Self::Failure { .. } => PendingTargetCompletionKind::Failure,
            Self::HalfClose { .. } => PendingTargetCompletionKind::HalfClose,
        }
    }

    fn into_command(self) -> SessionOwnerCommand {
        match self {
            Self::Open { request, result } => {
                SessionOwnerCommand::Event(SessionEvent::PeerOpenResolved { request, result })
            }
            Self::Failure { event } => SessionOwnerCommand::Driver(DriverInput::Control(event)),
            Self::HalfClose { event } => SessionOwnerCommand::Event(event),
        }
    }

    fn from_carried_command(
        expected: PendingTargetCompletionKind,
        flow_id: SessionFlowId,
        command: SessionOwnerCommand,
    ) -> Result<Self, OwnerTargetError> {
        match (expected, command) {
            (
                PendingTargetCompletionKind::Open,
                SessionOwnerCommand::Event(SessionEvent::PeerOpenResolved { request, result }),
            ) if request.flow_id() == flow_id => Ok(Self::Open { request, result }),
            (
                PendingTargetCompletionKind::Failure,
                SessionOwnerCommand::Driver(DriverInput::Control(
                    event @ SessionEvent::LocalReset {
                        flow,
                        reason: ResetReason::TargetFailure,
                    },
                )),
            ) if flow.flow_id() == flow_id => Ok(Self::Failure { event }),
            (
                PendingTargetCompletionKind::HalfClose,
                SessionOwnerCommand::Event(event @ SessionEvent::SinkHalfClosed { completion }),
            ) if completion.flow_id() == flow_id => Ok(Self::HalfClose { event }),
            _ => Err(OwnerTargetError::CarriedTargetCompletionMismatch { flow_id }),
        }
    }
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
    OpenCompletion,
    HalfCloseCompletion,
    FailureCompletion,
    Join,
    TerminalRetirement,
}

/// Sole Target-effect executor for one owner-side session supervisor.
pub(crate) struct OwnerTargetExecutor<T> {
    config: OwnerTargetConfig,
    supervisor: SessionSupervisor,
    target: T,
    source_factory: ResumableTcpPortFactory,
    flows: BTreeMap<SessionFlowId, TargetFlowOwner>,
    target_tombstones: BTreeMap<SessionFlowId, TargetTerminalKind>,
    pending_effects: VecDeque<OwnerTargetWork>,
    outstanding_resume: Option<u64>,
    next_resume_id: Option<u64>,
    max_recovery_effects: usize,
    aborted_effects: Option<AbortedOwnerTargetEffects>,
}

impl<T: TargetIo> OwnerTargetExecutor<T> {
    pub(crate) fn new(
        config: OwnerTargetConfig,
        mut supervisor: SessionSupervisor,
        target: T,
        source_config: FlowPortConfig,
    ) -> Result<Self, OwnerTargetConfigError> {
        let reducer_capacity = supervisor
            .owner_target_replay_capacity()
            .ok_or(OwnerTargetConfigError::OwnerRoleRequired)?;
        let recovery_bound = supervisor
            .owner_recovery_effect_bound()
            .ok_or(OwnerTargetConfigError::RecoveryEffectCapacityOverflow)?;
        let required_pending_effects = supervisor
            .owner_pending_work_bound()
            .ok_or(OwnerTargetConfigError::RecoveryEffectCapacityOverflow)?;
        if config.max_pending_effects < required_pending_effects {
            return Err(OwnerTargetConfigError::PendingEffectCapacityTooSmall {
                required: required_pending_effects,
                available: config.max_pending_effects,
            });
        }
        let flow_capacity = source_config.uplink_byte_capacity();
        if flow_capacity > reducer_capacity.per_flow_bytes {
            return Err(OwnerTargetConfigError::PerFlowBytesExceedReducer {
                requested: flow_capacity,
                available: reducer_capacity.per_flow_bytes,
            });
        }
        let session_capacity = reducer_capacity.session_bytes;
        if config.max_target_read_bytes > flow_capacity {
            return Err(OwnerTargetConfigError::TargetReadExceedsFlowCapacity {
                requested: config.max_target_read_bytes,
                available: flow_capacity,
            });
        }
        if config.max_target_read_bytes > session_capacity {
            return Err(OwnerTargetConfigError::TargetReadExceedsSessionCapacity {
                requested: config.max_target_read_bytes,
                available: session_capacity,
            });
        }
        let source_factory = supervisor.mint_tcp_port_factory(source_config)?;
        Ok(Self {
            config,
            supervisor,
            target,
            source_factory,
            flows: BTreeMap::new(),
            target_tombstones: BTreeMap::new(),
            pending_effects: VecDeque::new(),
            outstanding_resume: None,
            next_resume_id: Some(1),
            max_recovery_effects: recovery_bound,
            aborted_effects: None,
        })
    }

    pub(crate) fn snapshot(&self) -> OwnerTargetSnapshot {
        OwnerTargetSnapshot {
            session: self.supervisor.snapshot(),
            target_flows: self.flows.len(),
            target_tombstones: self.target_tombstones.len(),
            pending_writes: self
                .flows
                .values()
                .filter(|flow| flow.pending_write.is_some())
                .count(),
            pending_joins: self.flows.values().filter(|flow| flow.join_pending).count(),
            target_read_owned_bytes: self.source_factory.session_uplink_owned_bytes(),
            target_read_owned_segments: self.source_factory.session_uplink_owned_segments(),
            pending_effects: self.pending_effects.len(),
            needs_resume: self.outstanding_resume.is_some(),
            pending_target_completions: self
                .flows
                .values()
                .filter(|flow| flow.pending_completion.is_some())
                .count(),
            aborted_effects: self
                .aborted_effects
                .as_ref()
                .map_or(0, |aborted| aborted.effects.len()),
            aborted_outputs: self
                .aborted_effects
                .as_ref()
                .map_or(0, |aborted| aborted.outputs.len()),
        }
    }

    pub(crate) const fn config(&self) -> OwnerTargetConfig {
        self.config
    }

    pub(crate) fn target(&self) -> &T {
        &self.target
    }

    pub(crate) fn target_mut(&mut self) -> &mut T {
        &mut self.target
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
        if let Some(block) = self.input_admission_block(event_flow_id(&event)) {
            return Err(OwnerTargetError::RejectedEvent {
                block,
                event: Box::new(event),
            });
        }
        let terminal_retirement = match &event {
            SessionEvent::TerminalGraceExpired { terminal } => Some(terminal.flow_id()),
            _ => None,
        };
        let mut effects = match self.supervisor.apply_event(event) {
            Ok(effects) => effects,
            Err(SessionSupervisorError::RejectedEvent { block, event }) => {
                return Err(OwnerTargetError::RejectedEvent {
                    block: OwnerTargetInputBlock::ReducerAdmission(block),
                    event,
                });
            }
            Err(error) => return Err(OwnerTargetError::Supervisor(error)),
        };
        if let Some(flow_id) = terminal_retirement {
            if let Err(error) = self.retire_target_tombstone(flow_id) {
                let mut retained = effects
                    .into_iter()
                    .map(OwnerTargetWork::Effect)
                    .collect::<VecDeque<_>>();
                let mut outputs = Vec::new();
                return self.abort_owner_error(
                    OwnerTargetWork::TargetIoInvariant {
                        flow_id,
                        operation: TargetIoOperation::TerminalRetirement,
                    },
                    &mut retained,
                    &mut outputs,
                    error,
                );
            }
            match self.retry_one_pending_target_completion() {
                Ok(retried) => effects.extend(retried),
                Err(error) => {
                    let mut retained = effects
                        .into_iter()
                        .map(OwnerTargetWork::Effect)
                        .collect::<VecDeque<_>>();
                    let mut outputs = Vec::new();
                    return self.abort_owner_error(
                        OwnerTargetWork::TargetIoInvariant {
                            flow_id,
                            operation: TargetIoOperation::FailureCompletion,
                        },
                        &mut retained,
                        &mut outputs,
                        error,
                    );
                }
            }
        }
        self.execute_effects(effects)
    }

    /// Authenticates and installs one exact owner attach without publishing
    /// recovery before its acceptance.  The returned recovery capability must
    /// be consumed only after transport has queued `acceptance` on that same
    /// exact leg.
    pub(crate) fn accept_owner_attach(
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
        match self.supervisor.accept_owner_attach(leg, received)? {
            OwnerAttachPublication::Installed {
                attached,
                acceptance,
                recovery,
            } => Ok(OwnerTargetAttachPublication::Installed {
                attached,
                acceptance,
                recovery: PendingOwnerTargetRecovery { effects: recovery },
            }),
            OwnerAttachPublication::Resynchronize { status } => {
                Ok(OwnerTargetAttachPublication::Resynchronize { status })
            }
        }
    }

    pub(crate) fn execute_attached_recovery(
        &mut self,
        recovery: PendingOwnerTargetRecovery,
    ) -> Result<Vec<OwnerTargetOutput>, OwnerTargetError> {
        if let Some(block) = self.input_admission_block(None) {
            return Err(OwnerTargetError::RejectedAttachedRecovery {
                block,
                recovery: Box::new(recovery),
            });
        }
        let effects = recovery
            .effects
            .into_iter()
            .map(OwnerTargetWork::Effect)
            .collect::<VecDeque<_>>();
        if effects.len() > self.max_recovery_effects {
            let actual = effects.len();
            self.aborted_effects = Some(AbortedOwnerTargetEffects {
                effects,
                outputs: Vec::new(),
            });
            return Err(OwnerTargetError::GeneratedEffectBoundExceeded {
                max: self.max_recovery_effects,
                actual,
            });
        }
        self.execute_queue(effects)
    }

    /// Consumes the exact wakeup minted by the previous bounded turn.
    pub(crate) fn resume_pending_effects(
        &mut self,
        resume: OwnerTargetResume,
    ) -> Result<Vec<OwnerTargetOutput>, OwnerTargetError> {
        let Some(expected) = self.outstanding_resume else {
            return Err(OwnerTargetError::NoPendingEffectTurn);
        };
        if resume.id != expected {
            return Err(OwnerTargetError::InvalidEffectResume {
                expected,
                actual: resume.id,
            });
        }
        self.outstanding_resume = None;
        let effects = std::mem::take(&mut self.pending_effects);
        self.execute_queue(effects)
    }

    /// Retries one retained Target sink offer.  Zero and would-block retain the
    /// exact offer and produce no ACK.  A positive completion is returned to
    /// the reducer before any following offer can be attempted.
    pub(crate) fn retry_target_write(
        &mut self,
        flow_id: SessionFlowId,
    ) -> Result<Vec<OwnerTargetOutput>, OwnerTargetError> {
        self.require_no_pending_effects()?;
        self.require_no_pending_target_completion(flow_id)?;
        let pending = self
            .flows
            .get_mut(&flow_id)
            .ok_or(OwnerTargetError::UnknownTargetFlow { flow_id })?
            .pending_write
            .take()
            .ok_or(OwnerTargetError::NoPendingTargetWrite { flow_id })?;
        let mut effects = VecDeque::new();
        let mut outputs = Vec::new();
        self.attempt_target_write(pending, &mut effects, &mut outputs)?;
        outputs.extend(self.execute_queue(effects)?);
        Ok(outputs)
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

    /// Reserves the source FIFO slot and maximum byte permits before touching
    /// `TargetIo`.  A positive read shrinks that reservation to the exact
    /// bytes and reaches the reducer only through the non-cloneable driver
    /// command. EOF releases the read reservation before entering the same
    /// ordered source lane as DATA.
    pub(crate) fn try_read_target(
        &mut self,
        flow_id: SessionFlowId,
    ) -> Result<Vec<OwnerTargetOutput>, OwnerTargetError> {
        self.require_no_pending_effects()?;
        self.require_no_pending_target_completion(flow_id)?;
        let reservation = self
            .flows
            .get(&flow_id)
            .ok_or(OwnerTargetError::UnknownTargetFlow { flow_id })?
            .source
            .as_ref()
            .ok_or(OwnerTargetError::MissingTargetReadSource { flow_id })?
            .flow
            .try_reserve_uplink_source(self.config.max_target_read_bytes)?;
        let read = match self.target.read(flow_id, self.config.max_target_read_bytes) {
            Ok(read) => read,
            Err(error) => {
                drop(reservation);
                return self.abort_direct_target_io(flow_id, TargetIoOperation::Read, error);
            }
        };
        let source_fact = match read {
            super::target::TargetReadCompletion::Data(bytes) => {
                if let Err(error) = reservation.commit(bytes) {
                    return self.abort_direct_owner_error(
                        flow_id,
                        TargetIoOperation::ReadCompletion,
                        OwnerTargetError::FlowPort(error),
                    );
                }
                TargetSourceFact::Data
            }
            super::target::TargetReadCompletion::Eof => {
                if let Err(error) = reservation.commit_eof() {
                    return self.abort_direct_owner_error(
                        flow_id,
                        TargetIoOperation::ReadCompletion,
                        OwnerTargetError::FlowPort(error),
                    );
                }
                TargetSourceFact::Eof
            }
            super::target::TargetReadCompletion::WouldBlock => {
                drop(reservation);
                return Ok(Vec::new());
            }
            super::target::TargetReadCompletion::Failed(_) => {
                drop(reservation);
                let mut effects = VecDeque::new();
                let mut outputs = Vec::new();
                if let Err(error) = self.apply_target_failure(flow_id, &mut effects, &mut outputs) {
                    return self.abort_direct_owner_error(
                        flow_id,
                        TargetIoOperation::FailureCompletion,
                        error,
                    );
                }
                outputs.extend(self.execute_queue(effects)?);
                return Ok(outputs);
            }
        };
        let input_result = match self.flows.get_mut(&flow_id) {
            Some(flow) => match flow.source.as_mut() {
                Some(source) => source.driver.try_recv_next(),
                None => {
                    return self.abort_direct_owner_error(
                        flow_id,
                        TargetIoOperation::ReadCompletion,
                        OwnerTargetError::MissingTargetReadSource { flow_id },
                    );
                }
            },
            None => {
                return self.abort_direct_owner_error(
                    flow_id,
                    TargetIoOperation::ReadCompletion,
                    OwnerTargetError::UnknownTargetFlow { flow_id },
                );
            }
        };
        let input = match input_result {
            Ok(Some(input)) => input,
            Ok(None) => {
                return self.abort_direct_owner_error(
                    flow_id,
                    TargetIoOperation::ReadCompletion,
                    OwnerTargetError::MissingTargetReadDriverInput { flow_id },
                );
            }
            Err(error) => {
                return self.abort_direct_owner_error(
                    flow_id,
                    TargetIoOperation::ReadCompletion,
                    OwnerTargetError::FlowPort(error),
                );
            }
        };
        let exact_source_fact = match (&input, source_fact) {
            (DriverInput::Data(data), TargetSourceFact::Data) => data.flow().flow_id() == flow_id,
            (DriverInput::Control(SessionEvent::LocalClose { flow }), TargetSourceFact::Eof) => {
                flow.flow_id() == flow_id
            }
            _ => false,
        };
        if !exact_source_fact {
            return self.abort_direct_owner_error(
                flow_id,
                TargetIoOperation::ReadCompletion,
                OwnerTargetError::UnexpectedTargetReadDriverInput { flow_id },
            );
        }
        let effects = match self
            .supervisor
            .apply_command(SessionOwnerCommand::Driver(input))
        {
            Ok(effects) => effects,
            Err(error) => {
                return self.abort_direct_owner_error(
                    flow_id,
                    TargetIoOperation::ReadCompletion,
                    OwnerTargetError::Supervisor(error),
                );
            }
        };
        self.execute_effects(effects)
    }

    fn execute_effects(
        &mut self,
        effects: Vec<SessionEffect>,
    ) -> Result<Vec<OwnerTargetOutput>, OwnerTargetError> {
        self.require_no_pending_effects()?;
        let effects = effects
            .into_iter()
            .map(OwnerTargetWork::Effect)
            .collect::<VecDeque<_>>();
        if effects.len() > MAX_INCREMENTAL_EFFECT_BURST {
            let actual = effects.len();
            self.aborted_effects = Some(AbortedOwnerTargetEffects {
                effects,
                outputs: Vec::new(),
            });
            return Err(OwnerTargetError::GeneratedEffectBoundExceeded {
                max: MAX_INCREMENTAL_EFFECT_BURST,
                actual,
            });
        }
        self.execute_queue(effects)
    }

    fn execute_queue(
        &mut self,
        mut effects: VecDeque<OwnerTargetWork>,
    ) -> Result<Vec<OwnerTargetOutput>, OwnerTargetError> {
        let mut outputs = Vec::new();
        let mut steps = 0usize;
        while steps < self.config.max_effect_steps {
            let Some(work) = effects.pop_front() else {
                return Ok(outputs);
            };
            steps = match steps.checked_add(1) {
                Some(steps) => steps,
                None => {
                    return self.abort_owner_error(
                        work,
                        &mut effects,
                        &mut outputs,
                        OwnerTargetError::EffectStepOverflow,
                    );
                }
            };
            let effect = match work {
                OwnerTargetWork::Effect(effect) => effect,
                OwnerTargetWork::ExpireNextTarget => {
                    match self.expire_next_target() {
                        Ok(true) => effects.push_front(OwnerTargetWork::ExpireNextTarget),
                        Ok(false) => outputs.push(OwnerTargetOutput::SessionExpired),
                        Err(error) => {
                            return self.abort_owner_error(
                                OwnerTargetWork::ExpireNextTarget,
                                &mut effects,
                                &mut outputs,
                                error,
                            );
                        }
                    }
                    continue;
                }
                current @ OwnerTargetWork::TargetIoInvariant { .. } => {
                    return self.abort_owner_error(
                        current,
                        &mut effects,
                        &mut outputs,
                        OwnerTargetError::AbortedEffectInvariant,
                    );
                }
            };
            let reserved = incremental_effect_burst(&effect);
            let admitted = effects
                .len()
                .checked_add(reserved)
                .is_some_and(|attempted| attempted <= self.config.max_pending_effects);
            if !admitted {
                effects.push_front(OwnerTargetWork::Effect(effect));
                let attempted = effects.len().saturating_add(reserved);
                self.aborted_effects = Some(AbortedOwnerTargetEffects { effects, outputs });
                return Err(OwnerTargetError::PendingEffectCapacityInvariant {
                    max: self.config.max_pending_effects,
                    attempted,
                });
            }
            match effect {
                SessionEffect::Transmit(frame) => {
                    outputs.push(OwnerTargetOutput::Transmit(frame));
                }
                SessionEffect::LegActivated { generation } => {
                    outputs.push(OwnerTargetOutput::LegActivated { generation });
                }
                SessionEffect::ResumeGraceStarted { generation } => {
                    outputs.push(OwnerTargetOutput::ResumeGraceStarted { generation });
                }
                SessionEffect::SessionExpired => {
                    effects.push_front(OwnerTargetWork::ExpireNextTarget);
                }
                SessionEffect::PeerOpenRequested {
                    request,
                    flow,
                    target,
                } => {
                    let flow_id = request.flow_id();
                    if flow.flow_id() != flow_id || self.flows.contains_key(&flow_id) {
                        return self.abort_owner_error(
                            OwnerTargetWork::Effect(SessionEffect::PeerOpenRequested {
                                request,
                                flow,
                                target,
                            }),
                            &mut effects,
                            &mut outputs,
                            OwnerTargetError::DuplicateOrMismatchedPeerOpen { flow_id },
                        );
                    }
                    // Consume the one-shot LocalFlow authority before Target
                    // admission. A factory invariant failure must not leave an
                    // opened Target that was never entered in `self.flows`.
                    let source = match self.source_factory.open_flow(flow) {
                        Ok((flow, driver)) => TargetReadSource { flow, driver },
                        Err(error) => {
                            return self.abort_owner_error(
                                OwnerTargetWork::Effect(SessionEffect::PeerOpenRequested {
                                    request,
                                    flow,
                                    target,
                                }),
                                &mut effects,
                                &mut outputs,
                                OwnerTargetError::FlowPort(error),
                            );
                        }
                    };
                    let (result, target_present, retain_source, terminal) =
                        match self.target.open(flow_id, &target) {
                            Ok(
                                TargetOpenCompletion::Opened | TargetOpenCompletion::AlreadyOpen,
                            ) => (OpenResultCode::Opened, true, true, None),
                            Ok(TargetOpenCompletion::AlreadyTerminal(kind)) => {
                                (OpenResultCode::Internal, true, false, Some(kind))
                            }
                            Ok(TargetOpenCompletion::Failed(failure)) => {
                                (open_failure_result(failure), false, false, None)
                            }
                            Err(error) => {
                                return self.abort_target_io(
                                    OwnerTargetWork::Effect(SessionEffect::PeerOpenRequested {
                                        request,
                                        flow,
                                        target,
                                    }),
                                    &mut effects,
                                    &mut outputs,
                                    error,
                                );
                            }
                        };
                    // Retain the reducer flow even when Target admission
                    // failed: `PeerOpenResolved` will emit `FlowFinished`,
                    // which must retire this owner record without pretending
                    // that a Target resource exists.
                    let source = retain_source.then_some(source);
                    self.flows.insert(
                        flow_id,
                        TargetFlowOwner {
                            target_present,
                            source,
                            pending_write: None,
                            terminal,
                            join_pending: false,
                            retire_after_join: false,
                            pending_completion: None,
                        },
                    );
                    let generated = match self.submit_target_completion(
                        flow_id,
                        PendingTargetCompletion::Open { request, result },
                    ) {
                        Ok(generated) => generated,
                        Err(error) => {
                            return self.abort_owner_error(
                                OwnerTargetWork::TargetIoInvariant {
                                    flow_id,
                                    operation: TargetIoOperation::OpenCompletion,
                                },
                                &mut effects,
                                &mut outputs,
                                error,
                            );
                        }
                    };
                    self.extend_effects(&mut effects, &mut outputs, generated)?;
                }
                SessionEffect::OfferToSink { offer, segments } => {
                    let flow_id = offer.flow_id();
                    let pending = PendingTargetWrite { offer, segments };
                    let flow = match self.flows.get(&flow_id) {
                        Some(flow) => flow,
                        None => {
                            return self.abort_owner_error(
                                OwnerTargetWork::Effect(SessionEffect::OfferToSink {
                                    offer: pending.offer,
                                    segments: pending.segments,
                                }),
                                &mut effects,
                                &mut outputs,
                                OwnerTargetError::UnknownTargetFlow { flow_id },
                            );
                        }
                    };
                    if flow.pending_write.is_some() {
                        return self.abort_owner_error(
                            OwnerTargetWork::Effect(SessionEffect::OfferToSink {
                                offer: pending.offer,
                                segments: pending.segments,
                            }),
                            &mut effects,
                            &mut outputs,
                            OwnerTargetError::DuplicatePendingTargetWrite { flow_id },
                        );
                    }
                    self.attempt_target_write(pending, &mut effects, &mut outputs)?;
                }
                SessionEffect::HalfCloseSink { completion } => {
                    let flow_id = completion.flow_id();
                    let flow = match self.flows.get(&flow_id) {
                        Some(flow) => flow,
                        None => {
                            return self.abort_owner_error(
                                OwnerTargetWork::Effect(SessionEffect::HalfCloseSink {
                                    completion,
                                }),
                                &mut effects,
                                &mut outputs,
                                OwnerTargetError::UnknownTargetFlow { flow_id },
                            );
                        }
                    };
                    if flow.pending_write.is_some() {
                        return self.abort_owner_error(
                            OwnerTargetWork::Effect(SessionEffect::HalfCloseSink { completion }),
                            &mut effects,
                            &mut outputs,
                            OwnerTargetError::HalfCloseBeforePendingWrite { flow_id },
                        );
                    }
                    let completion_event = match self.target.half_close_write(flow_id) {
                        Ok(
                            TargetHalfCloseCompletion::Closed
                            | TargetHalfCloseCompletion::AlreadyClosed,
                        ) => SessionEvent::SinkHalfClosed { completion },
                        Ok(TargetHalfCloseCompletion::Failed(_)) => {
                            if let Err(error) =
                                self.apply_target_failure(flow_id, &mut effects, &mut outputs)
                            {
                                return self.abort_owner_error(
                                    OwnerTargetWork::Effect(SessionEffect::HalfCloseSink {
                                        completion,
                                    }),
                                    &mut effects,
                                    &mut outputs,
                                    error,
                                );
                            }
                            continue;
                        }
                        Err(error) => {
                            return self.abort_target_io(
                                OwnerTargetWork::Effect(SessionEffect::HalfCloseSink {
                                    completion,
                                }),
                                &mut effects,
                                &mut outputs,
                                error,
                            );
                        }
                    };
                    let generated = match self.submit_target_completion(
                        flow_id,
                        PendingTargetCompletion::HalfClose {
                            event: completion_event,
                        },
                    ) {
                        Ok(generated) => generated,
                        Err(error) => {
                            return self.abort_owner_error(
                                OwnerTargetWork::TargetIoInvariant {
                                    flow_id,
                                    operation: TargetIoOperation::HalfCloseCompletion,
                                },
                                &mut effects,
                                &mut outputs,
                                error,
                            );
                        }
                    };
                    self.extend_effects(&mut effects, &mut outputs, generated)?;
                }
                SessionEffect::PeerReset { flow_id, reason } => {
                    if !self.flows.contains_key(&flow_id) {
                        return self.abort_owner_error(
                            OwnerTargetWork::Effect(SessionEffect::PeerReset { flow_id, reason }),
                            &mut effects,
                            &mut outputs,
                            OwnerTargetError::UnknownTargetFlow { flow_id },
                        );
                    }
                    let terminal = match self.target.reset(flow_id) {
                        Err(error) => {
                            return self.abort_target_io(
                                OwnerTargetWork::Effect(SessionEffect::PeerReset {
                                    flow_id,
                                    reason,
                                }),
                                &mut effects,
                                &mut outputs,
                                error,
                            );
                        }
                        Ok(completion) => match completion {
                            TargetResetCompletion::Reset { .. }
                            | TargetResetCompletion::AlreadyReset => TargetTerminalKind::Reset,
                            TargetResetCompletion::AlreadyTerminal(kind) => kind,
                        },
                    };
                    if let Some(flow) = self.flows.get_mut(&flow_id) {
                        flow.pending_write = None;
                        flow.terminal = Some(terminal);
                    }
                }
                SessionEffect::FlowFinished {
                    flow_id,
                    reason,
                    terminal,
                } => {
                    if let Err(error) = self.finish_target_flow(flow_id, reason) {
                        return self.abort_owner_error(
                            OwnerTargetWork::Effect(SessionEffect::FlowFinished {
                                flow_id,
                                reason,
                                terminal,
                            }),
                            &mut effects,
                            &mut outputs,
                            error,
                        );
                    }
                    outputs.push(OwnerTargetOutput::TerminalGraceStarted { terminal });
                }
                unexpected @ (SessionEffect::ReplayStored { .. }
                | SessionEffect::ReplayAcknowledged { .. }) => {
                    return self.abort_owner_error(
                        OwnerTargetWork::Effect(unexpected),
                        &mut effects,
                        &mut outputs,
                        OwnerTargetError::InternalReceiptEscapedSupervisor,
                    );
                }
                unexpected @ (SessionEffect::LocalFlowOpened { .. }
                | SessionEffect::LocalOpenResolved { .. }) => {
                    return self.abort_owner_error(
                        OwnerTargetWork::Effect(unexpected),
                        &mut effects,
                        &mut outputs,
                        OwnerTargetError::ClientOnlyEffectAtOwner,
                    );
                }
            }
        }
        if effects.is_empty() {
            return Ok(outputs);
        }
        if effects.len() > self.config.max_pending_effects {
            let attempted = effects.len();
            self.aborted_effects = Some(AbortedOwnerTargetEffects { effects, outputs });
            return Err(OwnerTargetError::PendingEffectCapacityInvariant {
                max: self.config.max_pending_effects,
                attempted,
            });
        }
        self.pending_effects = effects;
        let resume = match self.issue_resume() {
            Ok(resume) => resume,
            Err(error) => {
                self.aborted_effects = Some(AbortedOwnerTargetEffects {
                    effects: std::mem::take(&mut self.pending_effects),
                    outputs,
                });
                return Err(error);
            }
        };
        outputs.push(OwnerTargetOutput::NeedsResume { resume });
        Ok(outputs)
    }

    fn attempt_target_write(
        &mut self,
        pending: PendingTargetWrite,
        effects: &mut VecDeque<OwnerTargetWork>,
        outputs: &mut Vec<OwnerTargetOutput>,
    ) -> Result<(), OwnerTargetError> {
        let flow_id = pending.offer.flow_id();
        let Some(first) = pending.segments.first() else {
            return self.abort_owner_error(
                OwnerTargetWork::Effect(SessionEffect::OfferToSink {
                    offer: pending.offer,
                    segments: pending.segments,
                }),
                effects,
                outputs,
                OwnerTargetError::EmptyTargetOffer { flow_id },
            );
        };
        match self.target.write(flow_id, first.payload()) {
            Ok(TargetWriteCompletion::Accepted(bytes)) => {
                let accepted = bytes.get();
                let exposed = first.len();
                let offered = pending.offer.len();
                if accepted > exposed || accepted > offered {
                    return self.abort_owner_error(
                        OwnerTargetWork::Effect(SessionEffect::OfferToSink {
                            offer: pending.offer,
                            segments: pending.segments,
                        }),
                        effects,
                        outputs,
                        OwnerTargetError::InvalidTargetWriteAcceptance {
                            flow_id,
                            accepted,
                            exposed,
                            offered,
                        },
                    );
                }
                let generated = match self.supervisor.apply_event(SessionEvent::SinkAccepted {
                    offer: pending.offer,
                    bytes: accepted,
                }) {
                    Ok(generated) => generated,
                    Err(error) => {
                        return self.abort_owner_error(
                            OwnerTargetWork::Effect(SessionEffect::OfferToSink {
                                offer: pending.offer,
                                segments: pending.segments,
                            }),
                            effects,
                            outputs,
                            OwnerTargetError::Supervisor(error),
                        );
                    }
                };
                self.extend_effects(effects, outputs, generated)?;
            }
            Ok(TargetWriteCompletion::Zero | TargetWriteCompletion::WouldBlock) => {
                let flow = match self.flows.get_mut(&flow_id) {
                    Some(flow) => flow,
                    None => {
                        return self.abort_owner_error(
                            OwnerTargetWork::Effect(SessionEffect::OfferToSink {
                                offer: pending.offer,
                                segments: pending.segments,
                            }),
                            effects,
                            outputs,
                            OwnerTargetError::UnknownTargetFlow { flow_id },
                        );
                    }
                };
                if flow.pending_write.is_some() {
                    return self.abort_owner_error(
                        OwnerTargetWork::Effect(SessionEffect::OfferToSink {
                            offer: pending.offer,
                            segments: pending.segments,
                        }),
                        effects,
                        outputs,
                        OwnerTargetError::DuplicatePendingTargetWrite { flow_id },
                    );
                }
                flow.pending_write = Some(pending);
            }
            Ok(TargetWriteCompletion::Failed(_)) => {
                if let Err(error) = self.apply_target_failure(flow_id, effects, outputs) {
                    return self.abort_owner_error(
                        OwnerTargetWork::Effect(SessionEffect::OfferToSink {
                            offer: pending.offer,
                            segments: pending.segments,
                        }),
                        effects,
                        outputs,
                        error,
                    );
                }
            }
            Err(error) => {
                return self.abort_target_io(
                    OwnerTargetWork::Effect(SessionEffect::OfferToSink {
                        offer: pending.offer,
                        segments: pending.segments,
                    }),
                    effects,
                    outputs,
                    error,
                );
            }
        }
        Ok(())
    }

    fn apply_target_failure(
        &mut self,
        flow_id: SessionFlowId,
        effects: &mut VecDeque<OwnerTargetWork>,
        outputs: &mut Vec<OwnerTargetOutput>,
    ) -> Result<(), OwnerTargetError> {
        let source = self
            .flows
            .get(&flow_id)
            .ok_or(OwnerTargetError::UnknownTargetFlow { flow_id })?
            .source
            .as_ref()
            .ok_or(OwnerTargetError::MissingTargetReadSource { flow_id })?;
        source.flow.try_send_reset(ResetReason::TargetFailure)?;
        let input = self
            .flows
            .get_mut(&flow_id)
            .ok_or(OwnerTargetError::UnknownTargetFlow { flow_id })?
            .source
            .as_mut()
            .ok_or(OwnerTargetError::MissingTargetReadSource { flow_id })?
            .driver
            .try_recv_next()?
            .ok_or(OwnerTargetError::MissingTargetFailureDriverInput { flow_id })?;
        let event = match input {
            DriverInput::Control(
                event @ SessionEvent::LocalReset {
                    flow,
                    reason: ResetReason::TargetFailure,
                },
            ) if flow.flow_id() == flow_id => event,
            _ => return Err(OwnerTargetError::UnexpectedTargetFailureDriverInput { flow_id }),
        };
        let generated = match self
            .submit_target_completion(flow_id, PendingTargetCompletion::Failure { event })
        {
            Ok(generated) => generated,
            Err(error) => {
                return self.abort_owner_error(
                    OwnerTargetWork::TargetIoInvariant {
                        flow_id,
                        operation: TargetIoOperation::FailureCompletion,
                    },
                    effects,
                    outputs,
                    error,
                );
            }
        };
        self.extend_effects(effects, outputs, generated)
    }

    fn submit_target_completion(
        &mut self,
        flow_id: SessionFlowId,
        completion: PendingTargetCompletion,
    ) -> Result<Vec<SessionEffect>, OwnerTargetError> {
        if self
            .flows
            .get(&flow_id)
            .ok_or(OwnerTargetError::UnknownTargetFlow { flow_id })?
            .pending_completion
            .is_some()
        {
            return Err(OwnerTargetError::DuplicatePendingTargetCompletion { flow_id });
        }
        let expected = completion.kind();
        match self.supervisor.apply_command(completion.into_command()) {
            Ok(effects) => Ok(effects),
            Err(SessionSupervisorError::RejectedCommand {
                block: ReducerAdmissionBlock::TerminalTombstoneCapacityExceeded { .. },
                command,
            }) => {
                let completion =
                    PendingTargetCompletion::from_carried_command(expected, flow_id, *command)?;
                self.flows
                    .get_mut(&flow_id)
                    .ok_or(OwnerTargetError::UnknownTargetFlow { flow_id })?
                    .pending_completion = Some(completion);
                Ok(Vec::new())
            }
            Err(SessionSupervisorError::RejectedCommand { block, command }) => {
                let completion =
                    PendingTargetCompletion::from_carried_command(expected, flow_id, *command)?;
                self.flows
                    .get_mut(&flow_id)
                    .ok_or(OwnerTargetError::UnknownTargetFlow { flow_id })?
                    .pending_completion = Some(completion);
                Err(OwnerTargetError::UnexpectedTargetCompletionAdmission { flow_id, block })
            }
            Err(error) => Err(OwnerTargetError::Supervisor(error)),
        }
    }

    fn retry_one_pending_target_completion(
        &mut self,
    ) -> Result<Vec<SessionEffect>, OwnerTargetError> {
        let Some(flow_id) = self
            .flows
            .iter()
            .find_map(|(flow_id, flow)| flow.pending_completion.is_some().then_some(*flow_id))
        else {
            return Ok(Vec::new());
        };
        let completion = self
            .flows
            .get_mut(&flow_id)
            .ok_or(OwnerTargetError::UnknownTargetFlow { flow_id })?
            .pending_completion
            .take()
            .ok_or(OwnerTargetError::MissingPendingTargetCompletion { flow_id })?;
        let expected = completion.kind();
        match self.supervisor.apply_command(completion.into_command()) {
            Ok(effects) => Ok(effects),
            Err(SessionSupervisorError::RejectedCommand {
                block: ReducerAdmissionBlock::TerminalTombstoneCapacityExceeded { .. },
                command,
            }) => {
                let completion =
                    PendingTargetCompletion::from_carried_command(expected, flow_id, *command)?;
                self.flows
                    .get_mut(&flow_id)
                    .ok_or(OwnerTargetError::UnknownTargetFlow { flow_id })?
                    .pending_completion = Some(completion);
                Ok(Vec::new())
            }
            Err(SessionSupervisorError::RejectedCommand { block, command }) => {
                let completion =
                    PendingTargetCompletion::from_carried_command(expected, flow_id, *command)?;
                self.flows
                    .get_mut(&flow_id)
                    .ok_or(OwnerTargetError::UnknownTargetFlow { flow_id })?
                    .pending_completion = Some(completion);
                Err(OwnerTargetError::UnexpectedTargetCompletionAdmission { flow_id, block })
            }
            Err(error) => Err(OwnerTargetError::Supervisor(error)),
        }
    }

    fn finish_target_flow(
        &mut self,
        flow_id: SessionFlowId,
        reason: FlowFinishReason,
    ) -> Result<(), OwnerTargetError> {
        let target_present = self
            .flows
            .get(&flow_id)
            .ok_or(OwnerTargetError::UnknownTargetFlow { flow_id })?
            .target_present;
        if !target_present {
            self.flows.remove(&flow_id);
            return Ok(());
        }
        match reason {
            FlowFinishReason::PeerReset(_) => {
                if self
                    .flows
                    .get(&flow_id)
                    .is_some_and(|flow| flow.terminal.is_none())
                {
                    let terminal = match self.target.reset(flow_id)? {
                        TargetResetCompletion::Reset { .. }
                        | TargetResetCompletion::AlreadyReset => TargetTerminalKind::Reset,
                        TargetResetCompletion::AlreadyTerminal(kind) => kind,
                    };
                    self.flows
                        .get_mut(&flow_id)
                        .ok_or(OwnerTargetError::UnknownTargetFlow { flow_id })?
                        .terminal = Some(terminal);
                }
            }
            FlowFinishReason::LocalReset(_) | FlowFinishReason::OpenFailed(_) => {
                if self
                    .flows
                    .get(&flow_id)
                    .is_some_and(|flow| flow.terminal.is_none())
                {
                    let terminal = match self.target.cancel(flow_id)? {
                        TargetCancelCompletion::Cancelled { .. }
                        | TargetCancelCompletion::AlreadyCancelled => TargetTerminalKind::Cancelled,
                        TargetCancelCompletion::AlreadyTerminal(kind) => kind,
                    };
                    self.flows
                        .get_mut(&flow_id)
                        .ok_or(OwnerTargetError::UnknownTargetFlow { flow_id })?
                        .terminal = Some(terminal);
                }
            }
            FlowFinishReason::Graceful => {
                // The Target adapter owns the graceful terminal transition:
                // all reads reached EOF and the write side was half-closed.
            }
        }
        self.flows
            .get_mut(&flow_id)
            .ok_or(OwnerTargetError::UnknownTargetFlow { flow_id })?
            .pending_write = None;
        let _ = self.try_join_target_flow(flow_id)?;
        Ok(())
    }

    fn retire_target_tombstone(&mut self, flow_id: SessionFlowId) -> Result<(), OwnerTargetError> {
        if self.target_tombstones.contains_key(&flow_id) {
            self.retire_recorded_target_tombstone(flow_id)?;
        } else {
            match self.target.retire_terminal_tombstone(flow_id)? {
                TargetTombstoneRetireCompletion::Retired(actual) => {
                    return Err(OwnerTargetError::UnexpectedTargetTombstone { flow_id, actual });
                }
                TargetTombstoneRetireCompletion::Absent => {
                    if let Some(flow) = self
                        .flows
                        .get_mut(&flow_id)
                        .filter(|flow| flow.target_present && flow.join_pending)
                    {
                        // Terminal grace can expire while the Target still
                        // owns application buffers or while the bounded
                        // tombstone table blocks join. Preserve that exact
                        // retirement fact and consume it immediately after
                        // a later successful join.
                        flow.retire_after_join = true;
                    }
                }
            }
        }
        self.drain_pending_target_joins()
    }

    fn retire_recorded_target_tombstone(
        &mut self,
        flow_id: SessionFlowId,
    ) -> Result<(), OwnerTargetError> {
        let expected = self
            .target_tombstones
            .get(&flow_id)
            .copied()
            .ok_or(OwnerTargetError::UnrecordedTargetTombstone { flow_id })?;
        match self.target.retire_terminal_tombstone(flow_id)? {
            TargetTombstoneRetireCompletion::Retired(actual) if actual == expected => {
                self.target_tombstones.remove(&flow_id);
                Ok(())
            }
            TargetTombstoneRetireCompletion::Retired(actual) => {
                Err(OwnerTargetError::TargetTombstoneKindMismatch {
                    flow_id,
                    expected,
                    actual,
                })
            }
            TargetTombstoneRetireCompletion::Absent => {
                Err(OwnerTargetError::MissingTargetTombstone { flow_id })
            }
        }
    }

    fn try_join_target_flow(
        &mut self,
        flow_id: SessionFlowId,
    ) -> Result<TargetJoinAttempt, OwnerTargetError> {
        let retire_after_join = self
            .flows
            .get(&flow_id)
            .ok_or(OwnerTargetError::UnknownTargetFlow { flow_id })?
            .retire_after_join;
        if !retire_after_join && self.target_tombstones.contains_key(&flow_id) {
            return Err(OwnerTargetError::DuplicateTargetTombstone { flow_id });
        }
        let terminal = match self.target.join(flow_id)? {
            TargetJoinCompletion::Joined(terminal)
            | TargetJoinCompletion::AlreadyJoined(terminal) => terminal,
            TargetJoinCompletion::PendingOwnedState { .. } => {
                self.flows
                    .get_mut(&flow_id)
                    .ok_or(OwnerTargetError::UnknownTargetFlow { flow_id })?
                    .join_pending = true;
                return Ok(TargetJoinAttempt::Pending(TargetJoinBlock::OwnedState));
            }
            TargetJoinCompletion::PendingTerminalCapacity { .. } => {
                self.flows
                    .get_mut(&flow_id)
                    .ok_or(OwnerTargetError::UnknownTargetFlow { flow_id })?
                    .join_pending = true;
                return Ok(TargetJoinAttempt::Pending(
                    TargetJoinBlock::TerminalCapacity,
                ));
            }
        };
        if retire_after_join {
            self.retire_just_joined_tombstone(flow_id, terminal)?;
        } else {
            let previous = self.target_tombstones.insert(flow_id, terminal);
            debug_assert!(previous.is_none());
        }
        self.flows.remove(&flow_id);
        Ok(TargetJoinAttempt::Joined)
    }

    fn retire_just_joined_tombstone(
        &mut self,
        flow_id: SessionFlowId,
        expected: TargetTerminalKind,
    ) -> Result<(), OwnerTargetError> {
        match self.target.retire_terminal_tombstone(flow_id)? {
            TargetTombstoneRetireCompletion::Retired(actual) if actual == expected => Ok(()),
            TargetTombstoneRetireCompletion::Retired(actual) => {
                Err(OwnerTargetError::TargetTombstoneKindMismatch {
                    flow_id,
                    expected,
                    actual,
                })
            }
            TargetTombstoneRetireCompletion::Absent => {
                Err(OwnerTargetError::MissingTargetTombstone { flow_id })
            }
        }
    }

    fn drain_pending_target_joins(&mut self) -> Result<(), OwnerTargetError> {
        let pending = self
            .flows
            .iter()
            .filter_map(|(flow_id, flow)| flow.join_pending.then_some(*flow_id))
            .collect::<Vec<_>>();
        for flow_id in pending {
            match self.try_join_target_flow(flow_id)? {
                TargetJoinAttempt::Joined
                | TargetJoinAttempt::Pending(TargetJoinBlock::OwnedState) => {}
                TargetJoinAttempt::Pending(TargetJoinBlock::TerminalCapacity) => {
                    // The table is session-scoped: once one join observes it
                    // full, no later pending flow can make progress until the
                    // next exact retirement. Avoid an O(pending) tail of
                    // equivalent failed Target calls.
                    break;
                }
            }
        }
        Ok(())
    }

    /// Performs at most one Target retirement/expiry transaction. Returning
    /// `true` asks the bounded executor to schedule another exact cleanup
    /// step; `false` proves all Target ownership is gone.
    fn expire_next_target(&mut self) -> Result<bool, OwnerTargetError> {
        // Session expiry invalidates every reducer terminal capability. Retire
        // already-joined Target tombstones first so the finite table cannot
        // block cancellation/join of a live flow.
        if let Some(flow_id) = self.target_tombstones.keys().next().copied() {
            self.retire_recorded_target_tombstone(flow_id)?;
            return Ok(true);
        }
        let Some(flow_id) = self.flows.keys().next().copied() else {
            return Ok(false);
        };
        if !self
            .flows
            .get(&flow_id)
            .is_some_and(|flow| flow.target_present)
        {
            self.flows.remove(&flow_id);
            return Ok(true);
        }
        let terminal = self.target.expire_session_flow(flow_id)?.terminal;
        if let Some(flow) = self.flows.get_mut(&flow_id) {
            flow.pending_write = None;
            flow.terminal = Some(terminal);
        }
        let joined_terminal = match self.target.join(flow_id)? {
            TargetJoinCompletion::Joined(terminal)
            | TargetJoinCompletion::AlreadyJoined(terminal) => terminal,
            TargetJoinCompletion::PendingOwnedState { .. }
            | TargetJoinCompletion::PendingTerminalCapacity { .. } => {
                return Err(OwnerTargetError::UnexpectedPendingTargetJoin { flow_id });
            }
        };
        // Do not fill the bounded tombstone table during session-wide
        // teardown. Join and retire this exact flow before moving to the next.
        self.retire_just_joined_tombstone(flow_id, joined_terminal)?;
        self.flows.remove(&flow_id);
        Ok(true)
    }

    fn require_no_pending_effects(&self) -> Result<(), OwnerTargetError> {
        if self.aborted_effects.is_some() {
            Err(OwnerTargetError::AbortedEffectInvariant)
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
        if flow.pending_completion.is_some() {
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
        if !self.pending_effects.is_empty() || self.outstanding_resume.is_some() {
            return Some(OwnerTargetInputBlock::PendingEffectTurn);
        }
        flow_id.and_then(|flow_id| {
            self.flows
                .get(&flow_id)
                .is_some_and(|flow| flow.pending_completion.is_some())
                .then_some(OwnerTargetInputBlock::PendingTargetCompletion { flow_id })
        })
    }

    fn extend_effects(
        &mut self,
        pending: &mut VecDeque<OwnerTargetWork>,
        outputs: &mut Vec<OwnerTargetOutput>,
        generated: Vec<SessionEffect>,
    ) -> Result<(), OwnerTargetError> {
        let generated_len = generated.len();
        let attempted = pending.len().saturating_add(generated_len);
        pending.extend(generated.into_iter().map(OwnerTargetWork::Effect));
        if generated_len > MAX_INCREMENTAL_EFFECT_BURST {
            self.aborted_effects = Some(AbortedOwnerTargetEffects {
                effects: std::mem::take(pending),
                outputs: std::mem::take(outputs),
            });
            Err(OwnerTargetError::GeneratedEffectBoundExceeded {
                max: MAX_INCREMENTAL_EFFECT_BURST,
                actual: generated_len,
            })
        } else if attempted > self.config.max_pending_effects {
            self.aborted_effects = Some(AbortedOwnerTargetEffects {
                effects: std::mem::take(pending),
                outputs: std::mem::take(outputs),
            });
            Err(OwnerTargetError::PendingEffectCapacityInvariant {
                max: self.config.max_pending_effects,
                attempted,
            })
        } else {
            Ok(())
        }
    }

    /// Retains the exact work item and every already-generated output before
    /// surfacing a fatal owner/Target invariant. The executor is permanently
    /// poisoned after this point: no retry can accidentally repeat I/O whose
    /// commit status is no longer trusted.
    fn abort_owner_error<R>(
        &mut self,
        current: OwnerTargetWork,
        pending: &mut VecDeque<OwnerTargetWork>,
        outputs: &mut Vec<OwnerTargetOutput>,
        error: OwnerTargetError,
    ) -> Result<R, OwnerTargetError> {
        if self.aborted_effects.is_some() {
            return Err(error);
        }
        pending.push_front(current);
        self.aborted_effects = Some(AbortedOwnerTargetEffects {
            effects: std::mem::take(pending),
            outputs: std::mem::take(outputs),
        });
        Err(error)
    }

    fn abort_target_io<R>(
        &mut self,
        current: OwnerTargetWork,
        pending: &mut VecDeque<OwnerTargetWork>,
        outputs: &mut Vec<OwnerTargetOutput>,
        error: TargetIoError,
    ) -> Result<R, OwnerTargetError> {
        self.abort_owner_error(current, pending, outputs, OwnerTargetError::Target(error))
    }

    fn abort_direct_target_io<R>(
        &mut self,
        flow_id: SessionFlowId,
        operation: TargetIoOperation,
        error: TargetIoError,
    ) -> Result<R, OwnerTargetError> {
        self.abort_direct_owner_error(flow_id, operation, OwnerTargetError::Target(error))
    }

    fn abort_direct_owner_error<R>(
        &mut self,
        flow_id: SessionFlowId,
        operation: TargetIoOperation,
        error: OwnerTargetError,
    ) -> Result<R, OwnerTargetError> {
        let mut pending = VecDeque::new();
        let mut outputs = Vec::new();
        self.abort_owner_error(
            OwnerTargetWork::TargetIoInvariant { flow_id, operation },
            &mut pending,
            &mut outputs,
            error,
        )
    }

    fn issue_resume(&mut self) -> Result<OwnerTargetResume, OwnerTargetError> {
        if self.outstanding_resume.is_some() {
            return Err(OwnerTargetError::DuplicateEffectResume);
        }
        let id = self
            .next_resume_id
            .ok_or(OwnerTargetError::EffectResumeIdExhausted)?;
        self.next_resume_id = id.checked_add(1);
        self.outstanding_resume = Some(id);
        Ok(OwnerTargetResume { id })
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
                .filter(|flow| flow.pending_write.is_some())
                .count(),
            self.flows.values().filter(|flow| flow.join_pending).count(),
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OwnerTargetInputBlock {
    PendingEffectTurn,
    AbortedEffectInvariant,
    PendingTargetCompletion { flow_id: SessionFlowId },
    ReducerAdmission(ReducerAdmissionBlock),
}

#[derive(Debug, Error)]
pub(crate) enum OwnerTargetError {
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
    #[error("owner Target has no pending effect turn")]
    NoPendingEffectTurn,
    #[error("owner Target effect resume {actual} does not match outstanding resume {expected}")]
    InvalidEffectResume { expected: u64, actual: u64 },
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
    #[error("owner Target rejected an exact owner ATTACH before consuming it: {block:?}")]
    RejectedOwnerAttach {
        block: OwnerTargetInputBlock,
        received: Box<LegBoundFrame>,
    },
    #[error("owner Target rejected post-acceptance recovery before consuming it: {block:?}")]
    RejectedAttachedRecovery {
        block: OwnerTargetInputBlock,
        recovery: Box<PendingOwnerTargetRecovery>,
    },
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
    use crate::owned_upstream::target::{
        MemoryTarget, MemoryTargetConfig, MemoryWriteDirective, TargetExpireCompletion,
        TargetFailure, TargetOpenFailure, TargetReadCompletion, TargetReadFeedCompletion,
        TargetReadiness,
    };
    use crate::resumable::{
        AttachAlpn, AttachAuthority, AttachCredentials, AttachNonce, AttachPolicy, AttachRequest,
        AttachTransportBinding, ByteOffset, DevicePrincipal, DeviceSecret, Direction, FeatureOffer,
        LegGeneration, OwnerIdentity, ReceiveBudgetLimits, Record, ReplayBudgetLimits, ResetReason,
        ResumeSecret, SESSION_PROTOCOL_VERSION, SessionConfig, SessionId, SessionModel,
        SessionRole, TcpWindowLimits, TlsExporterBinding, VersionRange,
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

    fn binding(exporter: u8) -> AttachTransportBinding {
        AttachTransportBinding::new(
            OwnerIdentity::new([0x31; 32]).unwrap(),
            AttachAlpn::new(b"mini-vpn-owned/1").unwrap(),
            TlsExporterBinding::new([exporter; 32]).unwrap(),
            DevicePrincipal::new([0x53; 16]).unwrap(),
        )
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

    struct OverAcceptingTarget {
        inner: MemoryTarget,
    }

    struct FailOnceTarget {
        inner: MemoryTarget,
        open: Option<TargetOpenFailure>,
        write: Option<TargetFailure>,
        write_invariant: Option<TargetIoError>,
        read: Option<TargetFailure>,
        half_close: Option<TargetFailure>,
        half_close_invariant: Option<TargetIoError>,
        open_calls: usize,
        write_calls: usize,
        read_calls: usize,
        half_close_calls: usize,
    }

    impl FailOnceTarget {
        fn new() -> Self {
            Self {
                inner: MemoryTarget::new(
                    MemoryTargetConfig::new(4, 256, 512, 128, 128, 16).unwrap(),
                ),
                open: None,
                write: None,
                write_invariant: None,
                read: None,
                half_close: None,
                half_close_invariant: None,
                open_calls: 0,
                write_calls: 0,
                read_calls: 0,
                half_close_calls: 0,
            }
        }
    }

    impl TargetIo for FailOnceTarget {
        fn open(
            &mut self,
            flow_id: SessionFlowId,
            target: &TargetAddr,
        ) -> Result<TargetOpenCompletion, TargetIoError> {
            self.open_calls += 1;
            if let Some(failure) = self.open.take() {
                return Ok(TargetOpenCompletion::Failed(failure));
            }
            self.inner.open(flow_id, target)
        }

        fn write(
            &mut self,
            flow_id: SessionFlowId,
            bytes: &[u8],
        ) -> Result<TargetWriteCompletion, TargetIoError> {
            self.write_calls += 1;
            if let Some(error) = self.write_invariant.take() {
                return Err(error);
            }
            if let Some(failure) = self.write.take() {
                return Ok(TargetWriteCompletion::Failed(failure));
            }
            self.inner.write(flow_id, bytes)
        }

        fn read(
            &mut self,
            flow_id: SessionFlowId,
            max_bytes: usize,
        ) -> Result<TargetReadCompletion, TargetIoError> {
            self.read_calls += 1;
            if let Some(failure) = self.read.take() {
                return Ok(TargetReadCompletion::Failed(failure));
            }
            self.inner.read(flow_id, max_bytes)
        }

        fn half_close_write(
            &mut self,
            flow_id: SessionFlowId,
        ) -> Result<TargetHalfCloseCompletion, TargetIoError> {
            self.half_close_calls += 1;
            if let Some(error) = self.half_close_invariant.take() {
                return Err(error);
            }
            if let Some(failure) = self.half_close.take() {
                return Ok(TargetHalfCloseCompletion::Failed(failure));
            }
            self.inner.half_close_write(flow_id)
        }

        fn reset(
            &mut self,
            flow_id: SessionFlowId,
        ) -> Result<TargetResetCompletion, TargetIoError> {
            self.inner.reset(flow_id)
        }

        fn cancel(
            &mut self,
            flow_id: SessionFlowId,
        ) -> Result<TargetCancelCompletion, TargetIoError> {
            self.inner.cancel(flow_id)
        }

        fn expire_session_flow(
            &mut self,
            flow_id: SessionFlowId,
        ) -> Result<TargetExpireCompletion, TargetIoError> {
            self.inner.expire_session_flow(flow_id)
        }

        fn readiness(&self, flow_id: SessionFlowId) -> Result<TargetReadiness, TargetIoError> {
            self.inner.readiness(flow_id)
        }

        fn join(&mut self, flow_id: SessionFlowId) -> Result<TargetJoinCompletion, TargetIoError> {
            self.inner.join(flow_id)
        }

        fn retire_terminal_tombstone(
            &mut self,
            flow_id: SessionFlowId,
        ) -> Result<TargetTombstoneRetireCompletion, TargetIoError> {
            self.inner.retire_terminal_tombstone(flow_id)
        }
    }

    impl TargetIo for OverAcceptingTarget {
        fn open(
            &mut self,
            flow_id: SessionFlowId,
            target: &TargetAddr,
        ) -> Result<TargetOpenCompletion, TargetIoError> {
            self.inner.open(flow_id, target)
        }

        fn write(
            &mut self,
            flow_id: SessionFlowId,
            bytes: &[u8],
        ) -> Result<TargetWriteCompletion, TargetIoError> {
            self.inner.readiness(flow_id)?;
            TargetWriteCompletion::accepted(bytes.len() + 1)
        }

        fn read(
            &mut self,
            flow_id: SessionFlowId,
            max_bytes: usize,
        ) -> Result<TargetReadCompletion, TargetIoError> {
            self.inner.read(flow_id, max_bytes)
        }

        fn half_close_write(
            &mut self,
            flow_id: SessionFlowId,
        ) -> Result<TargetHalfCloseCompletion, TargetIoError> {
            self.inner.half_close_write(flow_id)
        }

        fn reset(
            &mut self,
            flow_id: SessionFlowId,
        ) -> Result<TargetResetCompletion, TargetIoError> {
            self.inner.reset(flow_id)
        }

        fn cancel(
            &mut self,
            flow_id: SessionFlowId,
        ) -> Result<TargetCancelCompletion, TargetIoError> {
            self.inner.cancel(flow_id)
        }

        fn expire_session_flow(
            &mut self,
            flow_id: SessionFlowId,
        ) -> Result<super::super::target::TargetExpireCompletion, TargetIoError> {
            self.inner.expire_session_flow(flow_id)
        }

        fn readiness(&self, flow_id: SessionFlowId) -> Result<TargetReadiness, TargetIoError> {
            self.inner.readiness(flow_id)
        }

        fn join(&mut self, flow_id: SessionFlowId) -> Result<TargetJoinCompletion, TargetIoError> {
            self.inner.join(flow_id)
        }

        fn retire_terminal_tombstone(
            &mut self,
            flow_id: SessionFlowId,
        ) -> Result<TargetTombstoneRetireCompletion, TargetIoError> {
            self.inner.retire_terminal_tombstone(flow_id)
        }
    }

    #[test]
    fn target_open_is_exactly_once_across_duplicate_open_and_replacement_replay() {
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
        let established = EstablishedLeg::for_authenticated_transport(transport_binding);
        let request = AttachRequest::new(
            SessionId::new([0x11; 16]).unwrap(),
            LegGeneration::new(3).unwrap(),
            AttachNonce::new([0x33; 16]).unwrap(),
            VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION).unwrap(),
            FeatureOffer::new(0b111, 0b001).unwrap(),
        );
        let proof = credentials().prove(&request, &transport_binding).unwrap();
        let publication = executor
            .accept_owner_attach(
                &established,
                established.bind_received_frame(request.to_attach_frame(proof)),
            )
            .unwrap();
        let OwnerTargetAttachPublication::Installed {
            attached,
            acceptance,
            recovery,
        } = publication
        else {
            panic!("exact-next replacement unexpectedly requested resynchronization");
        };
        assert_eq!(attached.generation().get(), 3);
        assert!(matches!(acceptance.record(), Record::AttachAccepted { .. }));
        let recovery = executor.execute_attached_recovery(recovery).unwrap();
        assert!(transmitted(&recovery).iter().any(|record| matches!(
            record,
            Record::OpenResult { flow_id: observed, result: OpenResultCode::Opened }
                if *observed == flow_id
        )));
        assert_eq!(executor.target().snapshot().open_attempts, 1);
        assert_eq!(executor.target().snapshot().opened_flows, 1);
    }

    #[test]
    fn owner_attach_keeps_recovery_opaque_until_acceptance_is_ready_to_publish() {
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
        let established = EstablishedLeg::for_authenticated_transport(transport_binding);
        let request = AttachRequest::new(
            SessionId::new([0x11; 16]).unwrap(),
            LegGeneration::new(3).unwrap(),
            AttachNonce::new([0x33; 16]).unwrap(),
            VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION).unwrap(),
            FeatureOffer::new(0b111, 0b001).unwrap(),
        );
        let proof = credentials().prove(&request, &transport_binding).unwrap();
        let publication = executor
            .accept_owner_attach(
                &established,
                established.bind_received_frame(request.to_attach_frame(proof)),
            )
            .unwrap();

        let OwnerTargetAttachPublication::Installed {
            attached,
            acceptance,
            recovery,
        } = publication
        else {
            panic!("exact-next attach unexpectedly requested resynchronization");
        };
        assert_eq!(attached.generation().get(), 3);
        assert!(matches!(
            acceptance.record(),
            Record::AttachAccepted { nonce, .. } if *nonce == request.nonce()
        ));
        assert_eq!(executor.snapshot().session.session.generation().get(), 3);

        // The caller can publish `acceptance` before explicitly consuming the
        // non-cloneable recovery token.
        let recovered = executor.execute_attached_recovery(recovery).unwrap();
        assert_eq!(
            recovered,
            vec![OwnerTargetOutput::LegActivated {
                generation: LegGeneration::new(3).unwrap(),
            }]
        );
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
        let received = match executor.accept_owner_attach(&established, received) {
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
            .accept_owner_attach(&established, *received)
            .unwrap();
        let OwnerTargetAttachPublication::Installed {
            attached, recovery, ..
        } = publication
        else {
            panic!("retained exact-next ATTACH must install")
        };

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
        let data_resume = data
            .iter()
            .position(|output| matches!(output, OwnerTargetOutput::NeedsResume { .. }))
            .map(|index| data.remove(index));
        let Some(OwnerTargetOutput::NeedsResume {
            resume: data_resume,
        }) = data_resume
        else {
            panic!("one-step DATA must retain its generated ACK")
        };

        let recovery = match executor.execute_attached_recovery(recovery) {
            Err(OwnerTargetError::RejectedAttachedRecovery {
                block: OwnerTargetInputBlock::PendingEffectTurn,
                recovery,
            }) => recovery,
            other => panic!("pending DATA must return exact recovery ownership, got {other:?}"),
        };
        assert!(
            transmitted(&executor.resume_pending_effects(data_resume).unwrap())
                .iter()
                .any(|record| matches!(
                    record,
                    Record::Ack { flow_id: observed, .. } if *observed == flow_id
                ))
        );

        let mut recovered = executor.execute_attached_recovery(*recovery).unwrap();
        while let Some(index) = recovered
            .iter()
            .position(|output| matches!(output, OwnerTargetOutput::NeedsResume { .. }))
        {
            let OwnerTargetOutput::NeedsResume { resume } = recovered.remove(index) else {
                unreachable!()
            };
            recovered.extend(executor.resume_pending_effects(resume).unwrap());
        }
        assert!(recovered.iter().any(|output| matches!(
            output,
            OwnerTargetOutput::LegActivated { generation }
                if *generation == attached.generation()
        )));
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
    fn operational_open_failures_map_exactly_once_without_target_ownership() {
        let cases = [
            (TargetOpenFailure::Refused, OpenResultCode::TargetRefused),
            (
                TargetOpenFailure::Unreachable,
                OpenResultCode::TargetUnreachable,
            ),
            (TargetOpenFailure::TimedOut, OpenResultCode::TimedOut),
            (
                TargetOpenFailure::ResourceExhausted,
                OpenResultCode::ResourceExhausted,
            ),
            (
                TargetOpenFailure::PolicyDenied,
                OpenResultCode::PolicyDenied,
            ),
            (TargetOpenFailure::Internal, OpenResultCode::Internal),
        ];
        for (index, (failure, expected)) in cases.into_iter().enumerate() {
            let leg = committed_leg(2, 0x22, 0x42);
            let model = SessionModel::new(SessionRole::Owner, session_config(), leg);
            let supervisor = SessionSupervisor::new(model);
            let mut target_io = FailOnceTarget::new();
            target_io.open = Some(failure);
            let mut executor = OwnerTargetExecutor::new(
                OwnerTargetConfig::new(128, 1_024, 64).unwrap(),
                supervisor,
                target_io,
                source_config(),
            )
            .unwrap();
            let flow_id = SessionFlowId::new((index + 1) as u64).unwrap();

            let outputs = open_flow(&mut executor, leg, flow_id, target(443));
            assert_eq!(
                transmitted(&outputs)
                    .iter()
                    .filter(|record| matches!(
                        record,
                        Record::OpenResult {
                            flow_id: observed,
                            result,
                        } if *observed == flow_id && *result == expected
                    ))
                    .count(),
                1
            );
            assert_eq!(executor.target().open_calls, 1);
            assert_eq!(executor.snapshot().target_flows, 0);
            assert_eq!(executor.snapshot().target_read_owned_bytes, 0);
            assert_eq!(executor.snapshot().target_read_owned_segments, 0);
            assert_eq!(executor.target().inner.snapshot().live_flows, 0);
        }
    }

    #[test]
    fn operational_read_failure_is_one_urgent_target_failure_reset_and_other_flow_progresses() {
        let leg = committed_leg(2, 0x22, 0x42);
        let model = SessionModel::new(SessionRole::Owner, session_config(), leg);
        let supervisor = SessionSupervisor::new(model);
        let mut target_io = FailOnceTarget::new();
        target_io.read = Some(TargetFailure::ConnectionLost);
        let mut executor = OwnerTargetExecutor::new(
            OwnerTargetConfig::new(128, 1_024, 64).unwrap(),
            supervisor,
            target_io,
            source_config(),
        )
        .unwrap();
        let failed = SessionFlowId::new(1).unwrap();
        let live = SessionFlowId::new(2).unwrap();
        open_flow(&mut executor, leg, failed, target(443));
        open_flow(&mut executor, leg, live, target(8443));

        let outputs = executor.try_read_target(failed).unwrap();
        assert_eq!(executor.target().read_calls, 1);
        assert!(transmitted(&outputs).iter().any(|record| matches!(
            record,
            Record::Reset {
                flow_id,
                reason: ResetReason::TargetFailure,
            } if *flow_id == failed
        )));
        assert_eq!(executor.snapshot().target_flows, 1);
        assert_eq!(executor.target().inner.snapshot().live_flows, 1);
        assert!(matches!(
            executor.try_read_target(failed),
            Err(OwnerTargetError::UnknownTargetFlow { flow_id }) if flow_id == failed
        ));
        assert_eq!(executor.target().read_calls, 1);

        executor
            .target_mut()
            .inner
            .feed_read(live, Bytes::from_static(b"ok"))
            .unwrap();
        let progressed = executor.try_read_target(live).unwrap();
        assert!(transmitted(&progressed).iter().any(|record| matches!(
            record,
            Record::Data { flow_id, payload, .. }
                if *flow_id == live && payload.as_ref() == b"ok"
        )));
    }

    #[test]
    fn operational_write_and_half_close_failures_reset_but_invariant_errors_stay_fatal() {
        let leg = committed_leg(2, 0x22, 0x42);
        let model = SessionModel::new(SessionRole::Owner, session_config(), leg);
        let supervisor = SessionSupervisor::new(model);
        let mut target_io = FailOnceTarget::new();
        target_io.write = Some(TargetFailure::ResourceExhausted);
        target_io.half_close = Some(TargetFailure::ConnectionLost);
        let mut executor = OwnerTargetExecutor::new(
            OwnerTargetConfig::new(128, 1_024, 64).unwrap(),
            supervisor,
            target_io,
            source_config(),
        )
        .unwrap();
        let write_failed = SessionFlowId::new(1).unwrap();
        let close_failed = SessionFlowId::new(2).unwrap();
        open_flow(&mut executor, leg, write_failed, target(443));
        open_flow(&mut executor, leg, close_failed, target(8443));

        let write = peer_frame(
            &mut executor,
            leg,
            Record::Data {
                flow_id: write_failed,
                direction: Direction::ClientToTarget,
                offset: ByteOffset::new(0),
                payload: Bytes::from_static(b"owned"),
            },
        )
        .unwrap();
        assert!(transmitted(&write).iter().any(|record| matches!(
            record,
            Record::Reset { flow_id, reason: ResetReason::TargetFailure }
                if *flow_id == write_failed
        )));
        assert_eq!(executor.target().write_calls, 1);

        let close = peer_frame(
            &mut executor,
            leg,
            Record::Close {
                flow_id: close_failed,
                direction: Direction::ClientToTarget,
                final_offset: ByteOffset::new(0),
            },
        )
        .unwrap();
        assert!(transmitted(&close).iter().any(|record| matches!(
            record,
            Record::Reset { flow_id, reason: ResetReason::TargetFailure }
                if *flow_id == close_failed
        )));
        assert_eq!(executor.target().half_close_calls, 1);
        assert_eq!(executor.snapshot().target_flows, 0);

        let model = SessionModel::new(SessionRole::Owner, session_config(), leg);
        let supervisor = SessionSupervisor::new(model);
        let mut target_io = FailOnceTarget::new();
        target_io.write_invariant = Some(TargetIoError::CounterOverflow {
            counter: "injected_write_invariant",
        });
        let mut invariant_executor = OwnerTargetExecutor::new(
            OwnerTargetConfig::new(128, 1_024, 64).unwrap(),
            supervisor,
            target_io,
            source_config(),
        )
        .unwrap();
        let flow_id = SessionFlowId::new(1).unwrap();
        open_flow(&mut invariant_executor, leg, flow_id, target(443));
        assert!(matches!(
            peer_frame(
                &mut invariant_executor,
                leg,
                Record::Data {
                    flow_id,
                    direction: Direction::ClientToTarget,
                    offset: ByteOffset::new(0),
                    payload: Bytes::from_static(b"fatal"),
                },
            ),
            Err(OwnerTargetError::Target(TargetIoError::CounterOverflow {
                counter: "injected_write_invariant"
            }))
        ));
        assert_eq!(invariant_executor.target().write_calls, 1);
        assert_eq!(invariant_executor.snapshot().aborted_effects, 1);
        assert_eq!(
            invariant_executor
                .snapshot()
                .session
                .session
                .outstanding_sink_offers(),
            1
        );
        match invariant_executor.apply_event(SessionEvent::LegLost { leg }) {
            Err(OwnerTargetError::RejectedEvent {
                block: OwnerTargetInputBlock::AbortedEffectInvariant,
                event,
            }) => assert_eq!(*event, SessionEvent::LegLost { leg }),
            other => panic!("poisoned owner must return exact rejected event, got {other:?}"),
        }
        assert_eq!(invariant_executor.target().write_calls, 1);
        assert_eq!(
            invariant_executor.snapshot().session.session.phase(),
            crate::resumable::SessionPhase::Active
        );
    }

    #[test]
    fn terminal_capacity_retains_failed_read_completion_without_repeating_target_io() {
        let leg = committed_leg(2, 0x22, 0x42);
        let config = SessionConfig::new(
            4,
            1,
            TcpWindowLimits::new(128, 16).unwrap(),
            TcpWindowLimits::new(128, 16).unwrap(),
            ReplayBudgetLimits::new(128, 16).unwrap(),
            ReplayBudgetLimits::new(128, 16).unwrap(),
            ReceiveBudgetLimits::new(256, 32).unwrap(),
            128,
        )
        .unwrap();
        let model = SessionModel::new(SessionRole::Owner, config, leg);
        let supervisor = SessionSupervisor::new(model);
        let mut target_io = FailOnceTarget::new();
        target_io.read = Some(TargetFailure::TimedOut);
        let mut executor = OwnerTargetExecutor::new(
            OwnerTargetConfig::new(128, 1_024, 64).unwrap(),
            supervisor,
            target_io,
            source_config(),
        )
        .unwrap();
        let terminal_flow = SessionFlowId::new(1).unwrap();
        let failed = SessionFlowId::new(2).unwrap();
        let live = SessionFlowId::new(3).unwrap();
        open_flow(&mut executor, leg, terminal_flow, target(443));
        let terminal_outputs = peer_frame(
            &mut executor,
            leg,
            Record::Reset {
                flow_id: terminal_flow,
                reason: ResetReason::TargetFailure,
            },
        )
        .unwrap();
        let terminal = terminal_grace(&terminal_outputs);
        open_flow(&mut executor, leg, failed, target(8443));

        assert!(executor.try_read_target(failed).unwrap().is_empty());
        assert_eq!(executor.target().read_calls, 1);
        assert_eq!(executor.snapshot().pending_target_completions, 1);
        assert!(matches!(
            executor.try_read_target(failed),
            Err(OwnerTargetError::PendingTargetCompletion { flow_id }) if flow_id == failed
        ));
        assert_eq!(executor.target().read_calls, 1);

        let live_open = open_flow(&mut executor, leg, live, target(9443));
        assert!(transmitted(&live_open).iter().any(|record| matches!(
            record,
            Record::OpenResult { flow_id, result: OpenResultCode::Opened }
                if *flow_id == live
        )));
        let retried = executor
            .apply_event(SessionEvent::TerminalGraceExpired { terminal })
            .unwrap();
        assert!(transmitted(&retried).iter().any(|record| matches!(
            record,
            Record::Reset { flow_id, reason: ResetReason::TargetFailure }
                if *flow_id == failed
        )));
        assert_eq!(executor.target().read_calls, 1);
        assert_eq!(executor.snapshot().pending_target_completions, 0);
        assert_eq!(executor.snapshot().target_flows, 1);
        assert_eq!(executor.target().inner.snapshot().live_flows, 1);
    }

    #[test]
    fn successive_terminal_retirements_advance_pending_completions_in_flow_order() {
        let leg = committed_leg(2, 0x22, 0x42);
        let config = SessionConfig::new(
            4,
            1,
            TcpWindowLimits::new(128, 16).unwrap(),
            TcpWindowLimits::new(128, 16).unwrap(),
            ReplayBudgetLimits::new(128, 16).unwrap(),
            ReplayBudgetLimits::new(128, 16).unwrap(),
            ReceiveBudgetLimits::new(256, 32).unwrap(),
            128,
        )
        .unwrap();
        let model = SessionModel::new(SessionRole::Owner, config, leg);
        let supervisor = SessionSupervisor::new(model);
        let mut executor = OwnerTargetExecutor::new(
            OwnerTargetConfig::new(128, 1_024, 64).unwrap(),
            supervisor,
            FailOnceTarget::new(),
            source_config(),
        )
        .unwrap();
        let occupying = SessionFlowId::new(1).unwrap();
        let first = SessionFlowId::new(2).unwrap();
        let second = SessionFlowId::new(3).unwrap();
        open_flow(&mut executor, leg, occupying, target(443));
        open_flow(&mut executor, leg, first, target(8443));
        open_flow(&mut executor, leg, second, target(9443));
        let occupying_terminal = terminal_grace(
            &peer_frame(
                &mut executor,
                leg,
                Record::Reset {
                    flow_id: occupying,
                    reason: ResetReason::TargetFailure,
                },
            )
            .unwrap(),
        );

        executor.target_mut().read = Some(TargetFailure::TimedOut);
        assert!(executor.try_read_target(first).unwrap().is_empty());
        executor.target_mut().read = Some(TargetFailure::ConnectionLost);
        assert!(executor.try_read_target(second).unwrap().is_empty());
        assert_eq!(executor.snapshot().pending_target_completions, 2);
        assert_eq!(executor.target().read_calls, 2);

        let first_retry = executor
            .apply_event(SessionEvent::TerminalGraceExpired {
                terminal: occupying_terminal,
            })
            .unwrap();
        let first_terminal = terminal_grace(&first_retry);
        assert_eq!(first_terminal.flow_id(), first);
        assert_eq!(executor.snapshot().pending_target_completions, 1);
        assert_eq!(executor.target().read_calls, 2);

        let second_retry = executor
            .apply_event(SessionEvent::TerminalGraceExpired {
                terminal: first_terminal,
            })
            .unwrap();
        let second_terminal = terminal_grace(&second_retry);
        assert_eq!(second_terminal.flow_id(), second);
        assert_eq!(executor.snapshot().pending_target_completions, 0);
        assert_eq!(executor.target().read_calls, 2);

        executor
            .apply_event(SessionEvent::TerminalGraceExpired {
                terminal: second_terminal,
            })
            .unwrap();
        assert_eq!(executor.snapshot().target_flows, 0);
        assert_eq!(executor.snapshot().target_tombstones, 0);
        assert_eq!(executor.target().inner.snapshot().joined_flows, 0);
    }

    #[test]
    fn terminal_capacity_retains_successful_half_close_without_repeating_target_io() {
        let leg = committed_leg(2, 0x22, 0x42);
        let config = SessionConfig::new(
            4,
            1,
            TcpWindowLimits::new(128, 16).unwrap(),
            TcpWindowLimits::new(128, 16).unwrap(),
            ReplayBudgetLimits::new(128, 16).unwrap(),
            ReplayBudgetLimits::new(128, 16).unwrap(),
            ReceiveBudgetLimits::new(256, 32).unwrap(),
            128,
        )
        .unwrap();
        let model = SessionModel::new(SessionRole::Owner, config, leg);
        let supervisor = SessionSupervisor::new(model);
        let mut executor = OwnerTargetExecutor::new(
            OwnerTargetConfig::new(128, 1_024, 64).unwrap(),
            supervisor,
            FailOnceTarget::new(),
            source_config(),
        )
        .unwrap();
        let occupying = SessionFlowId::new(1).unwrap();
        let blocked = SessionFlowId::new(2).unwrap();
        open_flow(&mut executor, leg, occupying, target(443));
        open_flow(&mut executor, leg, blocked, target(8443));

        let occupying_outputs = peer_frame(
            &mut executor,
            leg,
            Record::Reset {
                flow_id: occupying,
                reason: ResetReason::TargetFailure,
            },
        )
        .unwrap();
        let occupying_terminal = terminal_grace(&occupying_outputs);

        executor.target_mut().inner.finish_read(blocked).unwrap();
        let eof = executor.try_read_target(blocked).unwrap();
        assert!(transmitted(&eof).iter().any(|record| matches!(
            record,
            Record::Close {
                flow_id,
                direction: Direction::TargetToClient,
                final_offset,
            } if *flow_id == blocked && *final_offset == ByteOffset::new(0)
        )));
        peer_frame(
            &mut executor,
            leg,
            Record::Ack {
                flow_id: blocked,
                direction: Direction::TargetToClient,
                next_accepted: ByteOffset::new(0),
                final_accepted: true,
            },
        )
        .unwrap();

        let blocked_completion = peer_frame(
            &mut executor,
            leg,
            Record::Close {
                flow_id: blocked,
                direction: Direction::ClientToTarget,
                final_offset: ByteOffset::new(0),
            },
        )
        .unwrap();
        assert!(
            !blocked_completion
                .iter()
                .any(|output| matches!(output, OwnerTargetOutput::TerminalGraceStarted { .. }))
        );
        assert_eq!(executor.target().half_close_calls, 1);
        assert_eq!(executor.snapshot().pending_target_completions, 1);

        let deferred_event = SessionEvent::PeerFrame {
            leg,
            frame: Frame::try_new(
                leg.generation(),
                Record::Close {
                    flow_id: blocked,
                    direction: Direction::ClientToTarget,
                    final_offset: ByteOffset::new(0),
                },
            )
            .unwrap(),
        };
        let retained_event = match executor.apply_event(deferred_event) {
            Err(OwnerTargetError::RejectedEvent {
                block: OwnerTargetInputBlock::PendingTargetCompletion { flow_id },
                event,
            }) if flow_id == blocked => event,
            other => panic!("blocked completion must return exact same-flow input, got {other:?}"),
        };
        assert_eq!(event_flow_id(&retained_event), Some(blocked));
        assert_eq!(executor.target().half_close_calls, 1);

        let retried = executor
            .apply_event(SessionEvent::TerminalGraceExpired {
                terminal: occupying_terminal,
            })
            .unwrap();
        let blocked_terminal = terminal_grace(&retried);
        assert_eq!(blocked_terminal.flow_id(), blocked);
        assert_eq!(executor.target().half_close_calls, 1);
        assert_eq!(executor.snapshot().pending_target_completions, 0);
        assert_eq!(executor.snapshot().target_flows, 0);

        let duplicate = executor.apply_event(*retained_event).unwrap();
        assert!(transmitted(&duplicate).iter().any(|record| matches!(
            record,
            Record::Ack {
                flow_id,
                direction: Direction::ClientToTarget,
                final_accepted: true,
                ..
            } if *flow_id == blocked
        )));
        assert_eq!(executor.target().half_close_calls, 1);

        executor
            .apply_event(SessionEvent::TerminalGraceExpired {
                terminal: blocked_terminal,
            })
            .unwrap();
        assert_eq!(executor.snapshot().target_tombstones, 0);
        assert_eq!(executor.target().inner.snapshot().joined_flows, 0);
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
    fn target_cannot_ack_more_bytes_than_the_slice_it_was_given() {
        let leg = committed_leg(2, 0x22, 0x42);
        let model = SessionModel::new(SessionRole::Owner, session_config(), leg);
        let supervisor = SessionSupervisor::new(model);
        let target_io = OverAcceptingTarget {
            inner: MemoryTarget::new(MemoryTargetConfig::new(4, 256, 512, 128, 128, 16).unwrap()),
        };
        let mut executor = OwnerTargetExecutor::new(
            OwnerTargetConfig::new(128, 1_024, 64).unwrap(),
            supervisor,
            target_io,
            source_config(),
        )
        .unwrap();
        let flow_id = SessionFlowId::new(1).unwrap();
        open_flow(&mut executor, leg, flow_id, target(443));

        let result = peer_frame(
            &mut executor,
            leg,
            Record::Data {
                flow_id,
                direction: Direction::ClientToTarget,
                offset: ByteOffset::new(0),
                payload: Bytes::from_static(b"abcdef"),
            },
        );

        assert!(matches!(
            result,
            Err(OwnerTargetError::InvalidTargetWriteAcceptance {
                flow_id: observed,
                accepted: 7,
                exposed: 6,
                offered: 6,
            }) if observed == flow_id
        ));
        assert_eq!(
            executor
                .snapshot()
                .session
                .session
                .outstanding_sink_offers(),
            1
        );
        assert_eq!(executor.target().inner.snapshot().write_accepted_bytes, 0);
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
}
