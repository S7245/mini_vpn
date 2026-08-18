//! Pure session reducer for the Knife16 resumable TCP ownership protocol.
//!
//! Transport-facing lifecycle events and peer records carry the exact
//! [`CommittedLeg`] capability and frame generation that authorize them.
//! Local socket completions instead carry opaque session/flow/request/offer
//! capabilities so an in-flight local operation remains valid across an
//! authenticated leg replacement. The reducer owns no sockets, clocks, tasks,
//! or I/O: it returns typed effects for an adapter to execute.
//!
//! Receive ownership has an explicit role-local global byte/range budget. The
//! TCP window returns an opaque, exact receive reservation before it takes
//! ownership; the reducer checks the aggregate budget and then commits that
//! same reservation. The TCP window's normalized-slice contract makes retained
//! backing storage strictly less than four times live payload;
//! [`SessionConfig`] exposes a conservative four-times bound. A decoded input
//! DATA frame is transient and is not part of that bound.
//! Finished flows leave only a count-bounded compact tombstone until the
//! adapter returns its [`TerminalGrace`] expiry capability; live-flow capacity
//! is reclaimed immediately while monotonically increasing IDs prevent reuse.

use std::collections::BTreeMap;
use std::fmt;

use bytes::Bytes;
use thiserror::Error;

use crate::shared::TargetAddr;

use super::auth::CommittedLeg;
use super::capacity::{DirectionalReplayStorageLimits, ReplayStorageLimit, ReplayStoragePlan};
use super::protocol::{
    ByteOffset, Direction, Frame, LegGeneration, OpenResultCode, ProtocolError, Record,
    ResetReason, SessionFlowId, SessionId, validate_record,
};
use super::tcp::{
    TcpDataSegment, TcpOwnershipError, TcpReceiveSnapshot, TcpReceiveWindow, TcpSendSnapshot,
    TcpSendWindow, TcpWindowLimits,
};

const NORMALIZED_BACKING_FACTOR: usize = 4;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionRole {
    Client,
    Owner,
}

impl SessionRole {
    pub const fn local_send_direction(self) -> Direction {
        match self {
            Self::Client => Direction::ClientToTarget,
            Self::Owner => Direction::TargetToClient,
        }
    }

    pub const fn peer_receive_direction(self) -> Direction {
        match self {
            Self::Client => Direction::TargetToClient,
            Self::Owner => Direction::ClientToTarget,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplayBudgetLimits {
    max_bytes: usize,
    max_segments: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReceiveBudgetLimits {
    max_bytes: usize,
    max_ranges: usize,
}

impl ReceiveBudgetLimits {
    pub fn new(max_bytes: usize, max_ranges: usize) -> Result<Self, SessionConfigError> {
        if max_bytes == 0 {
            return Err(SessionConfigError::ZeroReceiveByteCapacity);
        }
        if max_ranges == 0 {
            return Err(SessionConfigError::ZeroReceiveRangeCapacity);
        }
        Ok(Self {
            max_bytes,
            max_ranges,
        })
    }

    pub const fn max_bytes(self) -> usize {
        self.max_bytes
    }

    pub const fn max_ranges(self) -> usize {
        self.max_ranges
    }
}

impl ReplayBudgetLimits {
    pub fn new(max_bytes: usize, max_segments: usize) -> Result<Self, SessionConfigError> {
        if max_bytes == 0 {
            return Err(SessionConfigError::ZeroReplayByteCapacity);
        }
        if max_segments == 0 {
            return Err(SessionConfigError::ZeroReplaySegmentCapacity);
        }
        Ok(Self {
            max_bytes,
            max_segments,
        })
    }

    pub const fn max_bytes(self) -> usize {
        self.max_bytes
    }

    pub const fn max_segments(self) -> usize {
        self.max_segments
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SessionConfig {
    max_flows: usize,
    max_terminal_tombstones: usize,
    send_window: TcpWindowLimits,
    receive_window: TcpWindowLimits,
    client_to_target_replay: ReplayBudgetLimits,
    target_to_client_replay: ReplayBudgetLimits,
    receive_budget: ReceiveBudgetLimits,
    max_sink_offer_bytes: usize,
    max_receive_owned_bytes: usize,
    max_receive_ranges: usize,
    max_receive_normalized_backing_bytes: usize,
}

impl SessionConfig {
    /// Builds the reducer's exact ownership limits from the checked capacity
    /// plan. The local sender takes the per-flow limit for its wire direction;
    /// the peer receiver takes the opposite per-flow limit. Global replay is
    /// preserved for both directions, while this role's receive accounting is
    /// bounded by the plan's aggregate peer direction.
    pub fn from_storage_plan(
        role: SessionRole,
        max_terminal_tombstones: usize,
        storage: ReplayStoragePlan,
        max_sink_offer_bytes: usize,
    ) -> Result<Self, SessionConfigError> {
        let local_direction = role.local_send_direction();
        let peer_direction = role.peer_receive_direction();
        let per_flow = storage.per_flow();
        let global = storage.global();
        let local_send = storage_limit(per_flow, local_direction);
        let peer_receive = storage_limit(per_flow, peer_direction);
        let client_to_target = global.client_to_target();
        let target_to_client = global.target_to_client();
        let global_receive = storage_limit(global, peer_direction);

        let send_window = TcpWindowLimits::new(local_send.max_bytes(), local_send.max_segments())
            .map_err(|source| SessionConfigError::InvalidDerivedTcpWindow {
            direction: local_direction,
            source,
        })?;
        let receive_window =
            TcpWindowLimits::new(peer_receive.max_bytes(), peer_receive.max_segments()).map_err(
                |source| SessionConfigError::InvalidDerivedTcpWindow {
                    direction: peer_direction,
                    source,
                },
            )?;
        let client_to_target_replay = ReplayBudgetLimits::new(
            client_to_target.max_bytes(),
            client_to_target.max_segments(),
        )?;
        let target_to_client_replay = ReplayBudgetLimits::new(
            target_to_client.max_bytes(),
            target_to_client.max_segments(),
        )?;
        let receive_budget =
            ReceiveBudgetLimits::new(global_receive.max_bytes(), global_receive.max_segments())?;

        Self::new(
            storage.geometry().max_flows(),
            max_terminal_tombstones,
            send_window,
            receive_window,
            client_to_target_replay,
            target_to_client_replay,
            receive_budget,
            max_sink_offer_bytes,
        )
    }

    pub fn new(
        max_flows: usize,
        max_terminal_tombstones: usize,
        send_window: TcpWindowLimits,
        receive_window: TcpWindowLimits,
        client_to_target_replay: ReplayBudgetLimits,
        target_to_client_replay: ReplayBudgetLimits,
        receive_budget: ReceiveBudgetLimits,
        max_sink_offer_bytes: usize,
    ) -> Result<Self, SessionConfigError> {
        if max_flows == 0 {
            return Err(SessionConfigError::ZeroFlowCapacity);
        }
        if max_sink_offer_bytes == 0 {
            return Err(SessionConfigError::ZeroSinkOfferCapacity);
        }
        if max_terminal_tombstones == 0 {
            return Err(SessionConfigError::ZeroTerminalTombstoneCapacity);
        }
        let max_receive_owned_bytes = receive_budget.max_bytes();
        let max_receive_ranges = receive_budget.max_ranges();
        let max_receive_normalized_backing_bytes = receive_budget
            .max_bytes()
            .checked_mul(NORMALIZED_BACKING_FACTOR)
            .ok_or(SessionConfigError::ReceiveBackingBoundOverflow)?;
        Ok(Self {
            max_flows,
            max_terminal_tombstones,
            send_window,
            receive_window,
            client_to_target_replay,
            target_to_client_replay,
            receive_budget,
            max_sink_offer_bytes,
            max_receive_owned_bytes,
            max_receive_ranges,
            max_receive_normalized_backing_bytes,
        })
    }

    pub const fn max_flows(self) -> usize {
        self.max_flows
    }

    pub const fn max_terminal_tombstones(self) -> usize {
        self.max_terminal_tombstones
    }

    pub const fn send_window(self) -> TcpWindowLimits {
        self.send_window
    }

    pub const fn receive_window(self) -> TcpWindowLimits {
        self.receive_window
    }

    pub const fn replay_limit(self, direction: Direction) -> ReplayBudgetLimits {
        match direction {
            Direction::ClientToTarget => self.client_to_target_replay,
            Direction::TargetToClient => self.target_to_client_replay,
        }
    }

    pub const fn max_sink_offer_bytes(self) -> usize {
        self.max_sink_offer_bytes
    }

    pub const fn receive_budget(self) -> ReceiveBudgetLimits {
        self.receive_budget
    }

    /// Exact maximum live payload ownership across all receive windows.
    pub const fn max_receive_owned_bytes(self) -> usize {
        self.max_receive_owned_bytes
    }

    /// Exact maximum number of live receive ranges across all flows.
    pub const fn max_receive_ranges(self) -> usize {
        self.max_receive_ranges
    }

    /// Conservative bound implied by the TCP window's `< 4x` slice policy.
    pub const fn max_receive_normalized_backing_bytes(self) -> usize {
        self.max_receive_normalized_backing_bytes
    }
}

const fn storage_limit(
    limits: DirectionalReplayStorageLimits,
    direction: Direction,
) -> ReplayStorageLimit {
    match direction {
        Direction::ClientToTarget => limits.client_to_target(),
        Direction::TargetToClient => limits.target_to_client(),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionPhase {
    Active,
    Legless,
    Expired,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReplayBudgetUsage {
    bytes: usize,
    segments: usize,
}

impl ReplayBudgetUsage {
    pub const fn bytes(self) -> usize {
        self.bytes
    }

    pub const fn segments(self) -> usize {
        self.segments
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SessionSnapshot {
    phase: SessionPhase,
    generation: LegGeneration,
    flow_count: usize,
    client_to_target_replay: ReplayBudgetUsage,
    target_to_client_replay: ReplayBudgetUsage,
    receive_owned_bytes: usize,
    receive_ranges: usize,
    outstanding_sink_offers: usize,
    terminal_tombstones: usize,
    next_local_flow_id: Option<u64>,
    highest_peer_flow_id: u64,
}

impl SessionSnapshot {
    pub const fn phase(self) -> SessionPhase {
        self.phase
    }

    pub const fn generation(self) -> LegGeneration {
        self.generation
    }

    pub const fn flow_count(self) -> usize {
        self.flow_count
    }

    pub const fn replay_usage(self, direction: Direction) -> ReplayBudgetUsage {
        match direction {
            Direction::ClientToTarget => self.client_to_target_replay,
            Direction::TargetToClient => self.target_to_client_replay,
        }
    }

    pub const fn receive_owned_bytes(self) -> usize {
        self.receive_owned_bytes
    }

    pub const fn receive_ranges(self) -> usize {
        self.receive_ranges
    }

    pub const fn outstanding_sink_offers(self) -> usize {
        self.outstanding_sink_offers
    }

    pub const fn terminal_tombstones(self) -> usize {
        self.terminal_tombstones
    }

    pub const fn next_local_flow_id(self) -> Option<u64> {
        self.next_local_flow_id
    }

    pub const fn highest_peer_flow_id(self) -> u64 {
        self.highest_peer_flow_id
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SinkOffer {
    session_id: SessionId,
    id: u64,
    flow_id: SessionFlowId,
    direction: Direction,
    offset: ByteOffset,
    len: usize,
}

impl SinkOffer {
    pub const fn session_id(self) -> SessionId {
        self.session_id
    }

    pub const fn id(self) -> u64 {
        self.id
    }

    pub const fn flow_id(self) -> SessionFlowId {
        self.flow_id
    }

    pub const fn direction(self) -> Direction {
        self.direction
    }

    pub const fn offset(self) -> ByteOffset {
        self.offset
    }

    pub const fn len(self) -> usize {
        self.len
    }

    pub const fn is_empty(self) -> bool {
        self.len == 0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct LocalFlow {
    session_id: SessionId,
    flow_id: SessionFlowId,
    authority_id: u64,
}

impl LocalFlow {
    pub const fn session_id(self) -> SessionId {
        self.session_id
    }

    pub const fn flow_id(self) -> SessionFlowId {
        self.flow_id
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct PeerOpenRequest {
    session_id: SessionId,
    flow_id: SessionFlowId,
    request_id: u64,
}

impl PeerOpenRequest {
    pub const fn session_id(self) -> SessionId {
        self.session_id
    }

    pub const fn flow_id(self) -> SessionFlowId {
        self.flow_id
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SinkHalfClose {
    session_id: SessionId,
    flow_id: SessionFlowId,
    direction: Direction,
    final_offset: ByteOffset,
    completion_id: u64,
}

impl SinkHalfClose {
    pub const fn session_id(self) -> SessionId {
        self.session_id
    }

    pub const fn flow_id(self) -> SessionFlowId {
        self.flow_id
    }

    pub const fn direction(self) -> Direction {
        self.direction
    }

    pub const fn final_offset(self) -> ByteOffset {
        self.final_offset
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TerminalGrace {
    session_id: SessionId,
    flow_id: SessionFlowId,
    tombstone_id: u64,
}

impl TerminalGrace {
    pub const fn session_id(self) -> SessionId {
        self.session_id
    }

    pub const fn flow_id(self) -> SessionFlowId {
        self.flow_id
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FlowFinishReason {
    Graceful,
    PeerReset(ResetReason),
    LocalReset(ResetReason),
    OpenFailed(OpenResultCode),
}

#[derive(Clone, PartialEq, Eq)]
pub enum SessionEvent {
    ReplacementAttached {
        leg: CommittedLeg,
    },
    LegLost {
        leg: CommittedLeg,
    },
    ResumeGraceExpired {
        leg: CommittedLeg,
    },
    LocalOpen {
        leg: CommittedLeg,
        target: TargetAddr,
    },
    LocalData {
        flow: LocalFlow,
        payload: Bytes,
    },
    LocalClose {
        flow: LocalFlow,
    },
    LocalReset {
        flow: LocalFlow,
        reason: ResetReason,
    },
    PeerOpenResolved {
        request: PeerOpenRequest,
        result: OpenResultCode,
    },
    PeerFrame {
        leg: CommittedLeg,
        frame: Frame,
    },
    SinkAccepted {
        offer: SinkOffer,
        bytes: usize,
    },
    SinkAbandoned {
        offer: SinkOffer,
    },
    SinkHalfClosed {
        completion: SinkHalfClose,
    },
    SinkHalfCloseFailed {
        completion: SinkHalfClose,
    },
    TerminalGraceExpired {
        terminal: TerminalGrace,
    },
}

#[derive(Clone, PartialEq, Eq)]
pub enum SessionEffect {
    LegActivated {
        generation: LegGeneration,
    },
    ResumeGraceStarted {
        generation: LegGeneration,
    },
    SessionExpired,
    Transmit(Frame),
    LocalFlowOpened {
        flow: LocalFlow,
    },
    PeerOpenRequested {
        request: PeerOpenRequest,
        flow: LocalFlow,
        target: TargetAddr,
    },
    LocalOpenResolved {
        flow_id: SessionFlowId,
        result: OpenResultCode,
    },
    OfferToSink {
        offer: SinkOffer,
        segments: Vec<TcpDataSegment>,
    },
    HalfCloseSink {
        completion: SinkHalfClose,
    },
    PeerReset {
        flow_id: SessionFlowId,
        reason: ResetReason,
    },
    FlowFinished {
        flow_id: SessionFlowId,
        reason: FlowFinishReason,
        terminal: TerminalGrace,
    },
}

impl fmt::Debug for SessionEvent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReplacementAttached { leg } => formatter
                .debug_struct("ReplacementAttached")
                .field("generation", &leg.generation())
                .finish(),
            Self::LegLost { leg } => formatter
                .debug_struct("LegLost")
                .field("generation", &leg.generation())
                .finish(),
            Self::ResumeGraceExpired { leg } => formatter
                .debug_struct("ResumeGraceExpired")
                .field("generation", &leg.generation())
                .finish(),
            Self::LocalOpen { leg, .. } => formatter
                .debug_struct("LocalOpen")
                .field("generation", &leg.generation())
                .field("target", &Redacted)
                .finish(),
            Self::LocalData { flow, payload } => formatter
                .debug_struct("LocalData")
                .field("flow", flow)
                .field("payload_len", &payload.len())
                .finish(),
            Self::LocalClose { flow } => formatter
                .debug_struct("LocalClose")
                .field("flow", flow)
                .finish(),
            Self::LocalReset { flow, reason } => formatter
                .debug_struct("LocalReset")
                .field("flow", flow)
                .field("reason", reason)
                .finish(),
            Self::PeerOpenResolved { request, result } => formatter
                .debug_struct("PeerOpenResolved")
                .field("request", request)
                .field("result", result)
                .finish(),
            Self::PeerFrame { leg, frame } => formatter
                .debug_struct("PeerFrame")
                .field("capability_generation", &leg.generation())
                .field("frame_generation", &frame.leg_generation())
                .field("record", &RedactedRecord(frame.record()))
                .finish(),
            Self::SinkAccepted { offer, bytes } => formatter
                .debug_struct("SinkAccepted")
                .field("offer", offer)
                .field("bytes", bytes)
                .finish(),
            Self::SinkAbandoned { offer } => formatter
                .debug_struct("SinkAbandoned")
                .field("offer", offer)
                .finish(),
            Self::SinkHalfClosed { completion } => formatter
                .debug_struct("SinkHalfClosed")
                .field("completion", completion)
                .finish(),
            Self::SinkHalfCloseFailed { completion } => formatter
                .debug_struct("SinkHalfCloseFailed")
                .field("completion", completion)
                .finish(),
            Self::TerminalGraceExpired { terminal } => formatter
                .debug_struct("TerminalGraceExpired")
                .field("terminal", terminal)
                .finish(),
        }
    }
}

impl fmt::Debug for SessionEffect {
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
            Self::SessionExpired => formatter.write_str("SessionExpired"),
            Self::Transmit(frame) => formatter
                .debug_struct("Transmit")
                .field("generation", &frame.leg_generation())
                .field("record", &RedactedRecord(frame.record()))
                .finish(),
            Self::LocalFlowOpened { flow } => formatter
                .debug_struct("LocalFlowOpened")
                .field("flow", flow)
                .finish(),
            Self::PeerOpenRequested { request, flow, .. } => formatter
                .debug_struct("PeerOpenRequested")
                .field("request", request)
                .field("flow", flow)
                .field("target", &Redacted)
                .finish(),
            Self::LocalOpenResolved { flow_id, result } => formatter
                .debug_struct("LocalOpenResolved")
                .field("flow_id", flow_id)
                .field("result", result)
                .finish(),
            Self::OfferToSink { offer, segments } => {
                let bytes = segments.iter().map(TcpDataSegment::len).sum::<usize>();
                formatter
                    .debug_struct("OfferToSink")
                    .field("offer", offer)
                    .field("segment_count", &segments.len())
                    .field("payload_bytes", &bytes)
                    .finish()
            }
            Self::HalfCloseSink { completion } => formatter
                .debug_struct("HalfCloseSink")
                .field("completion", completion)
                .finish(),
            Self::PeerReset { flow_id, reason } => formatter
                .debug_struct("PeerReset")
                .field("flow_id", flow_id)
                .field("reason", reason)
                .finish(),
            Self::FlowFinished {
                flow_id,
                reason,
                terminal,
            } => formatter
                .debug_struct("FlowFinished")
                .field("flow_id", flow_id)
                .field("reason", reason)
                .field("terminal", terminal)
                .finish(),
        }
    }
}

struct Redacted;

impl fmt::Debug for Redacted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("[REDACTED]")
    }
}

struct RedactedRecord<'a>(&'a Record);

impl fmt::Debug for RedactedRecord<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            Record::Attach { .. } => formatter.write_str("Attach([REDACTED])"),
            Record::AttachAccepted { .. } => formatter.write_str("AttachAccepted"),
            Record::AttachGenerationStatus { .. } => {
                formatter.write_str("AttachGenerationStatus([REDACTED])")
            }
            Record::Open { flow_id, .. } => formatter
                .debug_struct("Open")
                .field("flow_id", flow_id)
                .field("target", &Redacted)
                .finish(),
            Record::OpenResult { flow_id, result } => formatter
                .debug_struct("OpenResult")
                .field("flow_id", flow_id)
                .field("result", result)
                .finish(),
            Record::Data {
                flow_id,
                direction,
                offset,
                payload,
            } => formatter
                .debug_struct("Data")
                .field("flow_id", flow_id)
                .field("direction", direction)
                .field("offset", offset)
                .field("payload_len", &payload.len())
                .finish(),
            Record::Ack {
                flow_id,
                direction,
                next_accepted,
                final_accepted,
            } => formatter
                .debug_struct("Ack")
                .field("flow_id", flow_id)
                .field("direction", direction)
                .field("next_accepted", next_accepted)
                .field("final_accepted", final_accepted)
                .finish(),
            Record::Close {
                flow_id,
                direction,
                final_offset,
            } => formatter
                .debug_struct("Close")
                .field("flow_id", flow_id)
                .field("direction", direction)
                .field("final_offset", final_offset)
                .finish(),
            Record::Reset { flow_id, reason } => formatter
                .debug_struct("Reset")
                .field("flow_id", flow_id)
                .field("reason", reason)
                .finish(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FlowOrigin {
    Local,
    Peer,
}

#[derive(Debug)]
struct FlowState {
    target: TargetAddr,
    origin: FlowOrigin,
    send: TcpSendWindow,
    receive: TcpReceiveWindow,
    local_flow: Option<LocalFlow>,
    peer_open_request: Option<PeerOpenRequest>,
    pending_sink_offer: Option<SinkOffer>,
    pending_half_close: Option<SinkHalfClose>,
    receive_final_accepted: bool,
    local_open_pending: bool,
    local_open_result: Option<OpenResultCode>,
    peer_open_result: Option<OpenResultCode>,
}

impl FlowState {
    fn new(target: TargetAddr, origin: FlowOrigin, config: SessionConfig) -> Self {
        Self {
            target,
            origin,
            send: TcpSendWindow::new(config.send_window()),
            receive: TcpReceiveWindow::new(config.receive_window()),
            local_flow: None,
            peer_open_request: None,
            pending_sink_offer: None,
            pending_half_close: None,
            receive_final_accepted: false,
            local_open_pending: origin == FlowOrigin::Local,
            local_open_result: None,
            peer_open_result: None,
        }
    }

    fn sink_is_open(&self) -> bool {
        match self.origin {
            FlowOrigin::Local => self.local_open_result == Some(OpenResultCode::Opened),
            FlowOrigin::Peer => self.peer_open_result == Some(OpenResultCode::Opened),
        }
    }
}

#[derive(Debug)]
struct TerminalTombstone {
    terminal: TerminalGrace,
    target: TargetAddr,
    replay_control: Option<Record>,
}

pub struct SessionModel {
    session_id: SessionId,
    role: SessionRole,
    config: SessionConfig,
    phase: SessionPhase,
    current_leg: CommittedLeg,
    flows: BTreeMap<SessionFlowId, FlowState>,
    terminal_tombstones: BTreeMap<SessionFlowId, TerminalTombstone>,
    next_local_flow_id: Option<u64>,
    highest_peer_flow_id: u64,
    next_local_authority_id: Option<u64>,
    next_open_request_id: Option<u64>,
    next_sink_offer_id: Option<u64>,
    next_half_close_id: Option<u64>,
    next_tombstone_id: Option<u64>,
    client_to_target_replay: ReplayBudgetUsage,
    target_to_client_replay: ReplayBudgetUsage,
    receive_usage: ReplayBudgetUsage,
}

impl SessionModel {
    pub fn new(role: SessionRole, config: SessionConfig, initial_leg: CommittedLeg) -> Self {
        Self {
            session_id: initial_leg.session_id(),
            role,
            config,
            phase: SessionPhase::Active,
            current_leg: initial_leg,
            flows: BTreeMap::new(),
            terminal_tombstones: BTreeMap::new(),
            next_local_flow_id: Some(1),
            highest_peer_flow_id: 0,
            next_local_authority_id: Some(1),
            next_open_request_id: Some(1),
            next_sink_offer_id: Some(1),
            next_half_close_id: Some(1),
            next_tombstone_id: Some(1),
            client_to_target_replay: ReplayBudgetUsage::default(),
            target_to_client_replay: ReplayBudgetUsage::default(),
            receive_usage: ReplayBudgetUsage::default(),
        }
    }

    pub const fn session_id(&self) -> SessionId {
        self.session_id
    }

    pub const fn role(&self) -> SessionRole {
        self.role
    }

    pub const fn config(&self) -> SessionConfig {
        self.config
    }

    pub fn snapshot(&self) -> SessionSnapshot {
        let mut outstanding_sink_offers = 0usize;
        for flow in self.flows.values() {
            outstanding_sink_offers += usize::from(flow.pending_sink_offer.is_some());
        }
        SessionSnapshot {
            phase: self.phase,
            generation: self.current_leg.generation(),
            flow_count: self.flows.len(),
            client_to_target_replay: self.client_to_target_replay,
            target_to_client_replay: self.target_to_client_replay,
            receive_owned_bytes: self.receive_usage.bytes,
            receive_ranges: self.receive_usage.segments,
            outstanding_sink_offers,
            terminal_tombstones: self.terminal_tombstones.len(),
            next_local_flow_id: self.next_local_flow_id,
            highest_peer_flow_id: self.highest_peer_flow_id,
        }
    }

    pub fn flow_snapshot(&self, flow_id: SessionFlowId) -> Option<SessionFlowSnapshot> {
        self.flows.get(&flow_id).map(|flow| SessionFlowSnapshot {
            send: flow.send.snapshot(),
            receive: flow.receive.snapshot(),
            has_outstanding_sink_offer: flow.pending_sink_offer.is_some(),
            has_pending_half_close: flow.pending_half_close.is_some(),
        })
    }

    pub fn reduce(&mut self, event: SessionEvent) -> Result<Vec<SessionEffect>, SessionError> {
        match event {
            SessionEvent::ReplacementAttached { leg } => self.attach_replacement(leg),
            SessionEvent::LegLost { leg } => self.lose_leg(leg),
            SessionEvent::ResumeGraceExpired { leg } => self.expire_resume_grace(leg),
            SessionEvent::LocalOpen { leg, target } => self.local_open(leg, target),
            SessionEvent::LocalData { flow, payload } => self.local_data(flow, payload),
            SessionEvent::LocalClose { flow } => self.local_close(flow),
            SessionEvent::LocalReset { flow, reason } => self.local_reset(flow, reason),
            SessionEvent::PeerOpenResolved { request, result } => {
                self.peer_open_resolved(request, result)
            }
            SessionEvent::PeerFrame { leg, frame } => self.peer_frame(leg, frame),
            SessionEvent::SinkAccepted { offer, bytes } => self.sink_accepted(offer, bytes),
            SessionEvent::SinkAbandoned { offer } => self.sink_abandoned(offer),
            SessionEvent::SinkHalfClosed { completion } => self.sink_half_closed(completion),
            SessionEvent::SinkHalfCloseFailed { completion } => {
                self.sink_half_close_failed(completion)
            }
            SessionEvent::TerminalGraceExpired { terminal } => self.expire_terminal_grace(terminal),
        }
    }

    fn attach_replacement(
        &mut self,
        leg: CommittedLeg,
    ) -> Result<Vec<SessionEffect>, SessionError> {
        if leg.session_id() != self.session_id {
            return Err(SessionError::CrossSessionLeg {
                expected: self.session_id,
                actual: leg.session_id(),
            });
        }
        if self.phase == SessionPhase::Expired {
            return Err(SessionError::SessionExpired);
        }
        if !same_session_semantics(&self.current_leg, &leg) {
            return Err(SessionError::LegCapabilityMismatch);
        }
        let current = self.current_leg.generation().get();
        let expected = current
            .checked_add(1)
            .ok_or(SessionError::LegGenerationExhausted)?;
        if leg.generation().get() != expected {
            return Err(SessionError::ReplacementGenerationNotNext {
                current,
                requested: leg.generation().get(),
            });
        }

        self.current_leg = leg;
        self.phase = SessionPhase::Active;
        Ok(self.recovery_effects())
    }

    fn lose_leg(&mut self, leg: CommittedLeg) -> Result<Vec<SessionEffect>, SessionError> {
        self.validate_current_capability(&leg)?;
        if self.phase == SessionPhase::Legless {
            return Ok(Vec::new());
        }
        self.phase = SessionPhase::Legless;
        Ok(vec![SessionEffect::ResumeGraceStarted {
            generation: leg.generation(),
        }])
    }

    fn expire_resume_grace(
        &mut self,
        leg: CommittedLeg,
    ) -> Result<Vec<SessionEffect>, SessionError> {
        self.validate_current_capability(&leg)?;
        if self.phase != SessionPhase::Legless {
            return Err(SessionError::ResumeGraceNotActive);
        }
        self.flows.clear();
        self.terminal_tombstones.clear();
        self.client_to_target_replay = ReplayBudgetUsage::default();
        self.target_to_client_replay = ReplayBudgetUsage::default();
        self.receive_usage = ReplayBudgetUsage::default();
        self.phase = SessionPhase::Expired;
        Ok(vec![SessionEffect::SessionExpired])
    }

    fn local_open(
        &mut self,
        leg: CommittedLeg,
        target: TargetAddr,
    ) -> Result<Vec<SessionEffect>, SessionError> {
        self.validate_current_capability(&leg)?;
        if self.role != SessionRole::Client {
            return Err(SessionError::RoleCannotInitiateOpen(self.role));
        }
        self.precheck_flow_capacity()?;
        let raw = self
            .next_local_flow_id
            .ok_or(SessionError::FlowIdExhausted)?;
        let flow_id = SessionFlowId::new(raw).map_err(|_| SessionError::FlowIdExhausted)?;
        let authority_id = self
            .next_local_authority_id
            .ok_or(SessionError::LocalFlowAuthorityExhausted)?;
        if self.flows.contains_key(&flow_id) || self.terminal_tombstones.contains_key(&flow_id) {
            return Err(SessionError::InvariantViolation(
                "fresh local flow id already existed",
            ));
        }
        let open_frame = Frame::try_new(
            self.current_leg.generation(),
            Record::Open { flow_id, target },
        )
        .map_err(|source| SessionError::InvalidProtocolRecord { source })?;
        let target = match open_frame.record() {
            Record::Open { target, .. } => target.clone(),
            _ => {
                return Err(SessionError::InvariantViolation(
                    "local OPEN record construction changed variant",
                ));
            }
        };
        let next = raw.checked_add(1);
        let local_flow = LocalFlow {
            session_id: self.session_id,
            flow_id,
            authority_id,
        };
        let mut flow = FlowState::new(target, FlowOrigin::Local, self.config);
        flow.local_flow = Some(local_flow);
        self.flows.insert(flow_id, flow);
        self.next_local_flow_id = next;
        self.next_local_authority_id = authority_id.checked_add(1);

        let mut effects = vec![SessionEffect::LocalFlowOpened { flow: local_flow }];
        if self.phase == SessionPhase::Active {
            effects.push(SessionEffect::Transmit(open_frame));
        }
        Ok(effects)
    }

    fn local_data(
        &mut self,
        local_flow: LocalFlow,
        payload: Bytes,
    ) -> Result<Vec<SessionEffect>, SessionError> {
        let flow_id = self.validate_local_flow(local_flow)?;
        self.precheck_flow_live(flow_id)?;
        self.precheck_local_source_open(flow_id)?;
        let direction = self.role.local_send_direction();
        self.precheck_replay_budget(direction, payload.len(), 1)?;

        let (append, owned_payload) = {
            let flow = self.flow_mut(flow_id)?;
            let offset = flow.send.next_sent();
            let append = flow
                .send
                .append(offset, payload)
                .map_err(|source| SessionError::TcpOwnership { flow_id, source })?;
            let owned_payload = flow
                .send
                .replay_view()
                .last()
                .filter(|segment| {
                    segment.offset() == append.offset()
                        && segment.end_offset() == append.end_offset()
                })
                .map(|segment| segment.payload().clone())
                .ok_or(SessionError::InvariantViolation(
                    "new sender ownership has no replay view",
                ))?;
            (append, owned_payload)
        };
        self.add_replay_usage(
            direction,
            append.retained_bytes_added(),
            append.segments_added(),
        )?;

        Ok(self.transmit_if_active(Record::Data {
            flow_id,
            direction,
            offset: append.offset(),
            payload: owned_payload,
        }))
    }

    fn local_close(&mut self, local_flow: LocalFlow) -> Result<Vec<SessionEffect>, SessionError> {
        let flow_id = self.validate_local_flow(local_flow)?;
        self.precheck_flow_live(flow_id)?;
        self.precheck_local_source_open(flow_id)?;
        let direction = self.role.local_send_direction();
        let close = {
            let flow = self.flow_mut(flow_id)?;
            let final_offset = flow.send.next_sent();
            flow.send
                .close(final_offset)
                .map_err(|source| SessionError::TcpOwnership { flow_id, source })?
        };
        if close.final_accepted() {
            return Ok(Vec::new());
        }
        Ok(self.transmit_if_active(Record::Close {
            flow_id,
            direction,
            final_offset: close.final_offset(),
        }))
    }

    fn local_reset(
        &mut self,
        local_flow: LocalFlow,
        reason: ResetReason,
    ) -> Result<Vec<SessionEffect>, SessionError> {
        let flow_id = self.validate_local_flow(local_flow)?;
        self.precheck_flow_live(flow_id)?;
        self.precheck_terminal_capacity()?;
        let control = Record::Reset { flow_id, reason };
        let mut effects = self.transmit_if_active(control.clone());
        effects.extend(self.finish_flow(
            flow_id,
            FlowFinishReason::LocalReset(reason),
            Some(control),
        )?);
        Ok(effects)
    }

    fn peer_open_resolved(
        &mut self,
        request: PeerOpenRequest,
        result: OpenResultCode,
    ) -> Result<Vec<SessionEffect>, SessionError> {
        let flow_id = self.validate_peer_open_request(request)?;
        if self.role != SessionRole::Owner {
            return Err(SessionError::RoleCannotResolvePeerOpen(self.role));
        }
        self.precheck_flow_live(flow_id)?;
        if result != OpenResultCode::Opened {
            self.precheck_terminal_capacity()?;
        } else {
            self.precheck_sink_activation_after_open(flow_id)?;
        }
        {
            let flow = self.flow_mut(flow_id)?;
            if flow.origin != FlowOrigin::Peer {
                return Err(SessionError::FlowOriginMismatch { flow_id });
            }
            if let Some(recorded) = flow.peer_open_result {
                if recorded != result {
                    return Err(SessionError::ConflictingOpenResult {
                        flow_id,
                        recorded,
                        attempted: result,
                    });
                }
            } else {
                flow.peer_open_result = Some(result);
            }
        }
        let control = Record::OpenResult { flow_id, result };
        let mut effects = self.transmit_if_active(control.clone());
        if result == OpenResultCode::Opened {
            effects.extend(self.activate_sink_after_open(flow_id)?);
        } else {
            effects.extend(self.finish_flow(
                flow_id,
                FlowFinishReason::OpenFailed(result),
                Some(control),
            )?);
        }
        Ok(effects)
    }

    fn peer_frame(
        &mut self,
        leg: CommittedLeg,
        frame: Frame,
    ) -> Result<Vec<SessionEffect>, SessionError> {
        self.validate_current_capability(&leg)?;
        if self.phase != SessionPhase::Active {
            return Err(SessionError::PeerFrameWhileLegless);
        }
        if frame.leg_generation() != self.current_leg.generation() {
            return Err(SessionError::FrameGenerationMismatch {
                current: self.current_leg.generation().get(),
                actual: frame.leg_generation().get(),
            });
        }
        validate_record(frame.record())
            .map_err(|source| SessionError::InvalidProtocolRecord { source })?;
        match frame.record().clone() {
            Record::Open { flow_id, target } => self.receive_open(flow_id, target),
            Record::OpenResult { flow_id, result } => self.receive_open_result(flow_id, result),
            Record::Data {
                flow_id,
                direction,
                offset,
                payload,
            } => self.receive_data(flow_id, direction, offset, payload),
            Record::Ack {
                flow_id,
                direction,
                next_accepted,
                final_accepted,
            } => self.receive_ack(flow_id, direction, next_accepted, final_accepted),
            Record::Close {
                flow_id,
                direction,
                final_offset,
            } => self.receive_close(flow_id, direction, final_offset),
            Record::Reset { flow_id, reason } => self.receive_reset(flow_id, reason),
            Record::Attach { .. }
            | Record::AttachAccepted { .. }
            | Record::AttachGenerationStatus { .. } => Err(SessionError::UnexpectedAttachRecord),
        }
    }

    fn receive_open(
        &mut self,
        flow_id: SessionFlowId,
        target: TargetAddr,
    ) -> Result<Vec<SessionEffect>, SessionError> {
        if self.role != SessionRole::Owner {
            return Err(SessionError::UnexpectedPeerOpen(self.role));
        }
        if let Some(tombstone) = self.terminal_tombstones.get(&flow_id) {
            if tombstone.target != target {
                return Err(SessionError::FlowTargetConflict { flow_id });
            }
            return Ok(self.replay_terminal_control(tombstone));
        }
        if let Some(flow) = self.flows.get(&flow_id) {
            if flow.target != target {
                return Err(SessionError::FlowTargetConflict { flow_id });
            }
            return Ok(match flow.peer_open_result {
                Some(result) => self.transmit_if_active(Record::OpenResult { flow_id, result }),
                None => Vec::new(),
            });
        }
        if flow_id.get() <= self.highest_peer_flow_id {
            return Err(SessionError::PeerFlowIdNotMonotonic {
                highest: self.highest_peer_flow_id,
                attempted: flow_id.get(),
            });
        }
        self.precheck_flow_capacity()?;
        let request_id = self
            .next_open_request_id
            .ok_or(SessionError::OpenRequestIdExhausted)?;
        let authority_id = self
            .next_local_authority_id
            .ok_or(SessionError::LocalFlowAuthorityExhausted)?;
        let request = PeerOpenRequest {
            session_id: self.session_id,
            flow_id,
            request_id,
        };
        let local_flow = LocalFlow {
            session_id: self.session_id,
            flow_id,
            authority_id,
        };
        let mut flow = FlowState::new(target.clone(), FlowOrigin::Peer, self.config);
        flow.peer_open_request = Some(request);
        flow.local_flow = Some(local_flow);
        self.flows.insert(flow_id, flow);
        self.highest_peer_flow_id = flow_id.get();
        self.next_open_request_id = request_id.checked_add(1);
        self.next_local_authority_id = authority_id.checked_add(1);
        Ok(vec![SessionEffect::PeerOpenRequested {
            request,
            flow: local_flow,
            target,
        }])
    }

    fn receive_open_result(
        &mut self,
        flow_id: SessionFlowId,
        result: OpenResultCode,
    ) -> Result<Vec<SessionEffect>, SessionError> {
        if self.role != SessionRole::Client {
            return Err(SessionError::UnexpectedPeerOpenResult(self.role));
        }
        if let Some(tombstone) = self.terminal_tombstones.get(&flow_id) {
            return Ok(self.replay_terminal_control(tombstone));
        }
        self.precheck_flow_live(flow_id)?;
        if result != OpenResultCode::Opened {
            self.precheck_terminal_capacity()?;
        } else {
            self.precheck_sink_activation_after_open(flow_id)?;
        }
        {
            let flow = self.flow_mut(flow_id)?;
            if flow.origin != FlowOrigin::Local {
                return Err(SessionError::FlowOriginMismatch { flow_id });
            }
            if let Some(recorded) = flow.local_open_result {
                if recorded != result {
                    return Err(SessionError::ConflictingOpenResult {
                        flow_id,
                        recorded,
                        attempted: result,
                    });
                }
                return Ok(Vec::new());
            }
            flow.local_open_result = Some(result);
            flow.local_open_pending = false;
        }
        let mut effects = vec![SessionEffect::LocalOpenResolved { flow_id, result }];
        if result == OpenResultCode::Opened {
            effects.extend(self.activate_sink_after_open(flow_id)?);
        } else {
            effects.extend(self.finish_flow(
                flow_id,
                FlowFinishReason::OpenFailed(result),
                None,
            )?);
        }
        Ok(effects)
    }

    fn receive_data(
        &mut self,
        flow_id: SessionFlowId,
        direction: Direction,
        offset: ByteOffset,
        payload: Bytes,
    ) -> Result<Vec<SessionEffect>, SessionError> {
        self.require_peer_receive_direction(direction)?;
        if let Some(tombstone) = self.terminal_tombstones.get(&flow_id) {
            return Ok(self.replay_terminal_control(tombstone));
        }
        self.precheck_flow_live(flow_id)?;
        let (reservation, may_offer) = {
            let flow = self
                .flows
                .get(&flow_id)
                .ok_or(SessionError::UnknownFlow(flow_id))?;
            (
                flow.receive
                    .preview_receive(offset, &payload)
                    .map_err(|source| SessionError::TcpOwnership { flow_id, source })?,
                flow.sink_is_open() && flow.pending_sink_offer.is_none(),
            )
        };
        self.precheck_receive_budget(
            reservation.buffered_bytes_added(),
            reservation.ranges_added(),
        )?;
        if may_offer && self.next_sink_offer_id.is_none() {
            return Err(SessionError::SinkOfferIdExhausted);
        }
        let expected_bytes = reservation.buffered_bytes_added();
        let expected_ranges = reservation.ranges_added();
        let should_offer = {
            let flow = self.flow_mut(flow_id)?;
            flow.receive
                .commit_receive(reservation, offset, payload)
                .map_err(|source| SessionError::TcpOwnership { flow_id, source })?;
            flow.pending_sink_offer.is_none() && flow.sink_is_open()
        };
        self.add_receive_usage(expected_bytes, expected_ranges)?;
        if should_offer {
            self.offer_contiguous(flow_id)
        } else {
            Ok(Vec::new())
        }
    }

    fn receive_ack(
        &mut self,
        flow_id: SessionFlowId,
        direction: Direction,
        next_accepted: ByteOffset,
        final_accepted: bool,
    ) -> Result<Vec<SessionEffect>, SessionError> {
        self.require_local_send_direction(direction)?;
        if let Some(tombstone) = self.terminal_tombstones.get(&flow_id) {
            return Ok(self.replay_terminal_control(tombstone));
        }
        self.precheck_flow_live(flow_id)?;
        let may_finish = final_accepted
            && self
                .flows
                .get(&flow_id)
                .ok_or(SessionError::UnknownFlow(flow_id))?
                .receive_final_accepted;
        if may_finish {
            self.precheck_terminal_capacity()?;
        }
        let acknowledged = {
            let flow = self.flow_mut(flow_id)?;
            flow.send
                .acknowledge(next_accepted, final_accepted)
                .map_err(|source| SessionError::TcpOwnership { flow_id, source })?
        };
        self.release_replay_usage(
            direction,
            acknowledged.retained_bytes_released(),
            acknowledged.segments_released(),
        )?;
        let finished = self
            .flows
            .get(&flow_id)
            .map(|flow| flow.send.final_accepted() && flow.receive_final_accepted)
            .unwrap_or(false);
        if finished {
            let flow = self
                .flows
                .get(&flow_id)
                .ok_or(SessionError::UnknownFlow(flow_id))?;
            let control = Record::Ack {
                flow_id,
                direction: self.role.peer_receive_direction(),
                next_accepted: flow.receive.accepted(),
                final_accepted: true,
            };
            self.finish_flow(flow_id, FlowFinishReason::Graceful, Some(control))
        } else {
            Ok(Vec::new())
        }
    }

    fn receive_close(
        &mut self,
        flow_id: SessionFlowId,
        direction: Direction,
        final_offset: ByteOffset,
    ) -> Result<Vec<SessionEffect>, SessionError> {
        self.require_peer_receive_direction(direction)?;
        if let Some(tombstone) = self.terminal_tombstones.get(&flow_id) {
            return Ok(self.replay_terminal_control(tombstone));
        }
        self.precheck_flow_live(flow_id)?;
        let may_request_half_close = {
            let flow = self
                .flows
                .get(&flow_id)
                .ok_or(SessionError::UnknownFlow(flow_id))?;
            flow.sink_is_open()
                && final_offset == flow.receive.accepted()
                && flow.pending_half_close.is_none()
                && !flow.receive_final_accepted
        };
        if may_request_half_close && self.next_half_close_id.is_none() {
            return Err(SessionError::HalfCloseCompletionIdExhausted);
        }
        let (accepted, half_close_ready, final_accepted, sink_is_open) = {
            let flow = self.flow_mut(flow_id)?;
            let close = flow
                .receive
                .receive_close(final_offset)
                .map_err(|source| SessionError::TcpOwnership { flow_id, source })?;
            (
                flow.receive.accepted(),
                close.half_close_ready(),
                flow.receive_final_accepted,
                flow.sink_is_open(),
            )
        };
        let mut effects = self.transmit_if_active(Record::Ack {
            flow_id,
            direction,
            next_accepted: accepted,
            final_accepted,
        });
        if sink_is_open
            && half_close_ready
            && !final_accepted
            && let Some(effect) = self.begin_half_close(flow_id)?
        {
            effects.push(effect);
        }
        Ok(effects)
    }

    fn receive_reset(
        &mut self,
        flow_id: SessionFlowId,
        reason: ResetReason,
    ) -> Result<Vec<SessionEffect>, SessionError> {
        if let Some(tombstone) = self.terminal_tombstones.get(&flow_id) {
            return Ok(self.replay_terminal_control(tombstone));
        }
        self.precheck_flow_live(flow_id)?;
        self.precheck_terminal_capacity()?;
        let mut effects = vec![SessionEffect::PeerReset { flow_id, reason }];
        effects.extend(self.finish_flow(flow_id, FlowFinishReason::PeerReset(reason), None)?);
        Ok(effects)
    }

    fn sink_accepted(
        &mut self,
        offer: SinkOffer,
        bytes: usize,
    ) -> Result<Vec<SessionEffect>, SessionError> {
        self.validate_sink_offer(offer)?;
        if bytes == 0 {
            return Err(SessionError::ZeroSinkAcceptance);
        }
        if bytes > offer.len {
            return Err(SessionError::SinkAcceptanceTooLarge {
                offered: offer.len,
                attempted: bytes,
            });
        }
        if self.next_sink_offer_id.is_none() {
            let contiguous = self
                .flows
                .get(&offer.flow_id)
                .ok_or(SessionError::UnknownFlow(offer.flow_id))?
                .receive
                .peek_contiguous(usize::MAX)
                .iter()
                .try_fold(0usize, |total, segment| total.checked_add(segment.len()))
                .ok_or(SessionError::InvariantViolation(
                    "contiguous receive length overflow",
                ))?;
            if contiguous > bytes {
                return Err(SessionError::SinkOfferIdExhausted);
            }
        }

        let (accept_preview, will_request_half_close) = {
            let flow = self
                .flows
                .get(&offer.flow_id)
                .ok_or(SessionError::UnknownFlow(offer.flow_id))?;
            let preview = flow.receive.preview_accept(bytes).map_err(|source| {
                SessionError::TcpOwnership {
                    flow_id: offer.flow_id,
                    source,
                }
            })?;
            (
                preview,
                flow.receive.final_offset() == Some(preview.accepted())
                    && flow.pending_half_close.is_none()
                    && !flow.receive_final_accepted,
            )
        };
        if accept_preview.accepted_bytes() != bytes {
            return Err(SessionError::InvariantViolation(
                "sink offer exceeds contiguous TCP receive ownership",
            ));
        }
        if will_request_half_close && self.next_half_close_id.is_none() {
            return Err(SessionError::HalfCloseCompletionIdExhausted);
        }
        if self.receive_usage.bytes < bytes
            || self.receive_usage.segments < accept_preview.ranges_released()
        {
            return Err(SessionError::InvariantViolation(
                "sink acceptance exceeds global receive ownership",
            ));
        }

        let (accepted, half_close_ready, final_accepted) = {
            let flow = self.flow_mut(offer.flow_id)?;
            let accepted =
                flow.receive
                    .accept(bytes)
                    .map_err(|source| SessionError::TcpOwnership {
                        flow_id: offer.flow_id,
                        source,
                    })?;
            debug_assert_eq!(accepted.accepted_bytes(), accept_preview.accepted_bytes());
            debug_assert_eq!(accepted.ranges_released(), accept_preview.ranges_released());
            flow.pending_sink_offer = None;
            (
                accepted.accepted(),
                flow.receive.half_close_ready(),
                flow.receive_final_accepted,
            )
        };
        self.release_receive_usage(bytes, accept_preview.ranges_released())?;

        let mut effects = self.transmit_if_active(Record::Ack {
            flow_id: offer.flow_id,
            direction: offer.direction,
            next_accepted: accepted,
            final_accepted,
        });
        if half_close_ready
            && !final_accepted
            && let Some(effect) = self.begin_half_close(offer.flow_id)?
        {
            effects.push(effect);
        }
        effects.extend(self.offer_contiguous(offer.flow_id)?);
        Ok(effects)
    }

    fn sink_abandoned(&mut self, offer: SinkOffer) -> Result<Vec<SessionEffect>, SessionError> {
        self.validate_sink_offer(offer)?;
        self.precheck_terminal_capacity()?;
        let control = Record::Reset {
            flow_id: offer.flow_id,
            reason: ResetReason::LocalAbandon,
        };
        let mut effects = self.transmit_if_active(control.clone());
        effects.extend(self.finish_flow(
            offer.flow_id,
            FlowFinishReason::LocalReset(ResetReason::LocalAbandon),
            Some(control),
        )?);
        Ok(effects)
    }

    fn sink_half_closed(
        &mut self,
        completion: SinkHalfClose,
    ) -> Result<Vec<SessionEffect>, SessionError> {
        let flow_id = self.validate_half_close(completion)?;
        let will_finish = self
            .flows
            .get(&flow_id)
            .ok_or(SessionError::UnknownFlow(flow_id))?
            .send
            .final_accepted();
        if will_finish {
            self.precheck_terminal_capacity()?;
        }
        let accepted = {
            let flow = self.flow_mut(flow_id)?;
            flow.pending_half_close = None;
            flow.receive_final_accepted = true;
            flow.receive.accepted()
        };
        let control = Record::Ack {
            flow_id,
            direction: completion.direction,
            next_accepted: accepted,
            final_accepted: true,
        };
        let mut effects = self.transmit_if_active(control.clone());
        if will_finish {
            effects.extend(self.finish_flow(flow_id, FlowFinishReason::Graceful, Some(control))?);
        }
        Ok(effects)
    }

    fn sink_half_close_failed(
        &mut self,
        completion: SinkHalfClose,
    ) -> Result<Vec<SessionEffect>, SessionError> {
        let flow_id = self.validate_half_close(completion)?;
        self.precheck_terminal_capacity()?;
        let reason = ResetReason::LocalAbandon;
        let control = Record::Reset { flow_id, reason };
        let mut effects = self.transmit_if_active(control.clone());
        effects.extend(self.finish_flow(
            flow_id,
            FlowFinishReason::LocalReset(reason),
            Some(control),
        )?);
        Ok(effects)
    }

    fn expire_terminal_grace(
        &mut self,
        terminal: TerminalGrace,
    ) -> Result<Vec<SessionEffect>, SessionError> {
        if terminal.session_id != self.session_id {
            return Err(SessionError::CrossSessionTerminalGrace);
        }
        let tombstone = self
            .terminal_tombstones
            .get(&terminal.flow_id)
            .ok_or(SessionError::UnknownTerminalGrace)?;
        if tombstone.terminal != terminal {
            return Err(SessionError::UnknownTerminalGrace);
        }
        self.terminal_tombstones.remove(&terminal.flow_id);
        Ok(Vec::new())
    }

    fn validate_current_capability(&self, leg: &CommittedLeg) -> Result<(), SessionError> {
        if leg.session_id() != self.session_id {
            return Err(SessionError::CrossSessionLeg {
                expected: self.session_id,
                actual: leg.session_id(),
            });
        }
        if leg.generation() != self.current_leg.generation() {
            return Err(SessionError::LegGenerationMismatch {
                current: self.current_leg.generation().get(),
                actual: leg.generation().get(),
            });
        }
        if leg != &self.current_leg {
            return Err(SessionError::LegCapabilityMismatch);
        }
        if self.phase == SessionPhase::Expired {
            return Err(SessionError::SessionExpired);
        }
        Ok(())
    }

    fn precheck_flow_capacity(&self) -> Result<(), SessionError> {
        if self.flows.len() >= self.config.max_flows() {
            return Err(SessionError::FlowCapacityExceeded {
                max: self.config.max_flows(),
            });
        }
        Ok(())
    }

    fn precheck_flow_live(&self, flow_id: SessionFlowId) -> Result<(), SessionError> {
        if !self.flows.contains_key(&flow_id) {
            let below_high_water = match self.role {
                SessionRole::Client => self
                    .next_local_flow_id
                    .is_none_or(|next| flow_id.get() < next),
                SessionRole::Owner => flow_id.get() <= self.highest_peer_flow_id,
            };
            if self.terminal_tombstones.contains_key(&flow_id) || below_high_water {
                return Err(SessionError::RetiredFlow(flow_id));
            }
            return Err(SessionError::UnknownFlow(flow_id));
        }
        Ok(())
    }

    fn validate_local_flow(&self, local_flow: LocalFlow) -> Result<SessionFlowId, SessionError> {
        if local_flow.session_id != self.session_id {
            return Err(SessionError::CrossSessionLocalFlow);
        }
        self.precheck_flow_live(local_flow.flow_id)?;
        let flow = self
            .flows
            .get(&local_flow.flow_id)
            .ok_or(SessionError::UnknownFlow(local_flow.flow_id))?;
        if flow.local_flow != Some(local_flow) {
            return Err(SessionError::LocalFlowAuthorityMismatch);
        }
        Ok(local_flow.flow_id)
    }

    fn precheck_local_source_open(&self, flow_id: SessionFlowId) -> Result<(), SessionError> {
        let flow = self
            .flows
            .get(&flow_id)
            .ok_or(SessionError::UnknownFlow(flow_id))?;
        if self.role == SessionRole::Owner
            && flow.origin == FlowOrigin::Peer
            && flow.peer_open_result != Some(OpenResultCode::Opened)
        {
            return Err(SessionError::LocalSourceNotOpen { flow_id });
        }
        Ok(())
    }

    fn validate_peer_open_request(
        &self,
        request: PeerOpenRequest,
    ) -> Result<SessionFlowId, SessionError> {
        if request.session_id != self.session_id {
            return Err(SessionError::CrossSessionOpenRequest);
        }
        self.precheck_flow_live(request.flow_id)?;
        let flow = self
            .flows
            .get(&request.flow_id)
            .ok_or(SessionError::UnknownFlow(request.flow_id))?;
        if flow.peer_open_request != Some(request) {
            return Err(SessionError::OpenRequestMismatch);
        }
        Ok(request.flow_id)
    }

    fn validate_half_close(
        &self,
        completion: SinkHalfClose,
    ) -> Result<SessionFlowId, SessionError> {
        if completion.session_id != self.session_id {
            return Err(SessionError::CrossSessionHalfClose);
        }
        self.precheck_flow_live(completion.flow_id)?;
        let flow = self
            .flows
            .get(&completion.flow_id)
            .ok_or(SessionError::UnknownFlow(completion.flow_id))?;
        if flow.pending_half_close != Some(completion) {
            return Err(SessionError::HalfCloseCompletionMismatch);
        }
        Ok(completion.flow_id)
    }

    fn precheck_terminal_capacity(&self) -> Result<(), SessionError> {
        if self.terminal_tombstones.len() >= self.config.max_terminal_tombstones() {
            return Err(SessionError::TerminalTombstoneCapacityExceeded {
                max: self.config.max_terminal_tombstones(),
            });
        }
        if self.next_tombstone_id.is_none() {
            return Err(SessionError::TerminalTombstoneIdExhausted);
        }
        Ok(())
    }

    fn flow_mut(&mut self, flow_id: SessionFlowId) -> Result<&mut FlowState, SessionError> {
        self.flows
            .get_mut(&flow_id)
            .ok_or(SessionError::UnknownFlow(flow_id))
    }

    fn require_local_send_direction(&self, actual: Direction) -> Result<(), SessionError> {
        let expected = self.role.local_send_direction();
        if actual != expected {
            return Err(SessionError::UnexpectedDirection { expected, actual });
        }
        Ok(())
    }

    fn require_peer_receive_direction(&self, actual: Direction) -> Result<(), SessionError> {
        let expected = self.role.peer_receive_direction();
        if actual != expected {
            return Err(SessionError::UnexpectedDirection { expected, actual });
        }
        Ok(())
    }

    fn replay_usage(&self, direction: Direction) -> ReplayBudgetUsage {
        match direction {
            Direction::ClientToTarget => self.client_to_target_replay,
            Direction::TargetToClient => self.target_to_client_replay,
        }
    }

    fn replay_usage_mut(&mut self, direction: Direction) -> &mut ReplayBudgetUsage {
        match direction {
            Direction::ClientToTarget => &mut self.client_to_target_replay,
            Direction::TargetToClient => &mut self.target_to_client_replay,
        }
    }

    fn precheck_replay_budget(
        &self,
        direction: Direction,
        bytes: usize,
        segments: usize,
    ) -> Result<(), SessionError> {
        let usage = self.replay_usage(direction);
        let limit = self.config.replay_limit(direction);
        let attempted_bytes =
            usage
                .bytes
                .checked_add(bytes)
                .ok_or(SessionError::ReplayByteBudgetExceeded {
                    direction,
                    attempted: usize::MAX,
                    max: limit.max_bytes(),
                })?;
        if attempted_bytes > limit.max_bytes() {
            return Err(SessionError::ReplayByteBudgetExceeded {
                direction,
                attempted: attempted_bytes,
                max: limit.max_bytes(),
            });
        }
        let attempted_segments = usage.segments.checked_add(segments).ok_or(
            SessionError::ReplaySegmentBudgetExceeded {
                direction,
                attempted: usize::MAX,
                max: limit.max_segments(),
            },
        )?;
        if attempted_segments > limit.max_segments() {
            return Err(SessionError::ReplaySegmentBudgetExceeded {
                direction,
                attempted: attempted_segments,
                max: limit.max_segments(),
            });
        }
        Ok(())
    }

    fn add_replay_usage(
        &mut self,
        direction: Direction,
        bytes: usize,
        segments: usize,
    ) -> Result<(), SessionError> {
        let usage = self.replay_usage_mut(direction);
        usage.bytes = usage
            .bytes
            .checked_add(bytes)
            .ok_or(SessionError::InvariantViolation(
                "global replay byte accounting overflow",
            ))?;
        usage.segments =
            usage
                .segments
                .checked_add(segments)
                .ok_or(SessionError::InvariantViolation(
                    "global replay segment accounting overflow",
                ))?;
        Ok(())
    }

    fn release_replay_usage(
        &mut self,
        direction: Direction,
        bytes: usize,
        segments: usize,
    ) -> Result<(), SessionError> {
        let usage = self.replay_usage_mut(direction);
        usage.bytes = usage
            .bytes
            .checked_sub(bytes)
            .ok_or(SessionError::InvariantViolation(
                "global replay byte accounting underflow",
            ))?;
        usage.segments =
            usage
                .segments
                .checked_sub(segments)
                .ok_or(SessionError::InvariantViolation(
                    "global replay segment accounting underflow",
                ))?;
        Ok(())
    }

    fn precheck_receive_budget(&self, bytes: usize, ranges: usize) -> Result<(), SessionError> {
        let limit = self.config.receive_budget();
        let attempted_bytes = self.receive_usage.bytes.checked_add(bytes).ok_or(
            SessionError::ReceiveByteBudgetExceeded {
                attempted: usize::MAX,
                max: limit.max_bytes(),
            },
        )?;
        if attempted_bytes > limit.max_bytes() {
            return Err(SessionError::ReceiveByteBudgetExceeded {
                attempted: attempted_bytes,
                max: limit.max_bytes(),
            });
        }
        let attempted_ranges = self.receive_usage.segments.checked_add(ranges).ok_or(
            SessionError::ReceiveRangeBudgetExceeded {
                attempted: usize::MAX,
                max: limit.max_ranges(),
            },
        )?;
        if attempted_ranges > limit.max_ranges() {
            return Err(SessionError::ReceiveRangeBudgetExceeded {
                attempted: attempted_ranges,
                max: limit.max_ranges(),
            });
        }
        Ok(())
    }

    fn add_receive_usage(&mut self, bytes: usize, ranges: usize) -> Result<(), SessionError> {
        self.receive_usage.bytes =
            self.receive_usage
                .bytes
                .checked_add(bytes)
                .ok_or(SessionError::InvariantViolation(
                    "global receive byte accounting overflow",
                ))?;
        self.receive_usage.segments = self.receive_usage.segments.checked_add(ranges).ok_or(
            SessionError::InvariantViolation("global receive range accounting overflow"),
        )?;
        Ok(())
    }

    fn release_receive_usage(&mut self, bytes: usize, ranges: usize) -> Result<(), SessionError> {
        self.receive_usage.bytes =
            self.receive_usage
                .bytes
                .checked_sub(bytes)
                .ok_or(SessionError::InvariantViolation(
                    "global receive byte accounting underflow",
                ))?;
        self.receive_usage.segments = self.receive_usage.segments.checked_sub(ranges).ok_or(
            SessionError::InvariantViolation("global receive range accounting underflow"),
        )?;
        Ok(())
    }

    fn validate_sink_offer(&self, offer: SinkOffer) -> Result<(), SessionError> {
        if offer.session_id != self.session_id {
            return Err(SessionError::CrossSessionSinkOffer);
        }
        if offer.direction != self.role.peer_receive_direction() {
            return Err(SessionError::SinkOfferMismatch);
        }
        let flow = self
            .flows
            .get(&offer.flow_id)
            .ok_or(SessionError::UnknownFlow(offer.flow_id))?;
        if flow.pending_sink_offer != Some(offer) {
            return Err(SessionError::SinkOfferMismatch);
        }
        Ok(())
    }

    fn precheck_sink_activation_after_open(
        &self,
        flow_id: SessionFlowId,
    ) -> Result<(), SessionError> {
        let flow = self
            .flows
            .get(&flow_id)
            .ok_or(SessionError::UnknownFlow(flow_id))?;
        if flow.pending_sink_offer.is_none()
            && !flow
                .receive
                .peek_contiguous(self.config.max_sink_offer_bytes())
                .is_empty()
            && self.next_sink_offer_id.is_none()
        {
            return Err(SessionError::SinkOfferIdExhausted);
        }
        if flow.receive.half_close_ready()
            && flow.pending_half_close.is_none()
            && !flow.receive_final_accepted
            && self.next_half_close_id.is_none()
        {
            return Err(SessionError::HalfCloseCompletionIdExhausted);
        }
        Ok(())
    }

    fn activate_sink_after_open(
        &mut self,
        flow_id: SessionFlowId,
    ) -> Result<Vec<SessionEffect>, SessionError> {
        let mut effects = self.offer_contiguous(flow_id)?;
        let should_half_close = {
            let flow = self
                .flows
                .get(&flow_id)
                .ok_or(SessionError::UnknownFlow(flow_id))?;
            flow.receive.half_close_ready()
                && flow.pending_half_close.is_none()
                && !flow.receive_final_accepted
        };
        if should_half_close && let Some(effect) = self.begin_half_close(flow_id)? {
            effects.push(effect);
        }
        Ok(effects)
    }

    fn offer_contiguous(
        &mut self,
        flow_id: SessionFlowId,
    ) -> Result<Vec<SessionEffect>, SessionError> {
        let segments = {
            let flow = self
                .flows
                .get(&flow_id)
                .ok_or(SessionError::UnknownFlow(flow_id))?;
            if flow.pending_sink_offer.is_some() || !flow.sink_is_open() {
                return Ok(Vec::new());
            }
            flow.receive
                .peek_contiguous(self.config.max_sink_offer_bytes())
        };
        if segments.is_empty() {
            return Ok(Vec::new());
        }
        let len = segments
            .iter()
            .try_fold(0usize, |total, segment| total.checked_add(segment.len()))
            .ok_or(SessionError::InvariantViolation(
                "sink offer length overflow",
            ))?;
        let offset = segments[0].offset();
        let id = self
            .next_sink_offer_id
            .ok_or(SessionError::SinkOfferIdExhausted)?;
        let offer = SinkOffer {
            session_id: self.session_id,
            id,
            flow_id,
            direction: self.role.peer_receive_direction(),
            offset,
            len,
        };
        self.next_sink_offer_id = id.checked_add(1);
        self.flow_mut(flow_id)?.pending_sink_offer = Some(offer);
        Ok(vec![SessionEffect::OfferToSink { offer, segments }])
    }

    fn begin_half_close(
        &mut self,
        flow_id: SessionFlowId,
    ) -> Result<Option<SessionEffect>, SessionError> {
        let flow = self
            .flows
            .get(&flow_id)
            .ok_or(SessionError::UnknownFlow(flow_id))?;
        if flow.receive_final_accepted || flow.pending_half_close.is_some() {
            return Ok(None);
        }
        if !flow.receive.half_close_ready() {
            return Err(SessionError::InvariantViolation(
                "half-close requested before final bytes were accepted",
            ));
        }
        let id = self
            .next_half_close_id
            .ok_or(SessionError::HalfCloseCompletionIdExhausted)?;
        let completion = SinkHalfClose {
            session_id: self.session_id,
            flow_id,
            direction: self.role.peer_receive_direction(),
            final_offset: flow.receive.accepted(),
            completion_id: id,
        };
        self.next_half_close_id = id.checked_add(1);
        self.flow_mut(flow_id)?.pending_half_close = Some(completion);
        Ok(Some(SessionEffect::HalfCloseSink { completion }))
    }

    fn finish_flow(
        &mut self,
        flow_id: SessionFlowId,
        reason: FlowFinishReason,
        replay_control: Option<Record>,
    ) -> Result<Vec<SessionEffect>, SessionError> {
        self.precheck_terminal_capacity()?;
        let direction = self.role.local_send_direction();
        let (target, send_bytes, send_segments, receive_bytes, receive_ranges) = {
            let flow = self
                .flows
                .get(&flow_id)
                .ok_or(SessionError::UnknownFlow(flow_id))?;
            let send = flow.send.snapshot();
            let receive = flow.receive.snapshot();
            (
                flow.target.clone(),
                send.retained_bytes(),
                send.segment_count(),
                receive.buffered_bytes(),
                receive.range_count(),
            )
        };
        let usage = self.replay_usage(direction);
        if usage.bytes < send_bytes || usage.segments < send_segments {
            return Err(SessionError::InvariantViolation(
                "terminal flow release exceeds global replay ownership",
            ));
        }
        if self.receive_usage.bytes < receive_bytes || self.receive_usage.segments < receive_ranges
        {
            return Err(SessionError::InvariantViolation(
                "terminal flow release exceeds global receive ownership",
            ));
        }
        if self.terminal_tombstones.contains_key(&flow_id) {
            return Err(SessionError::InvariantViolation(
                "live flow already has terminal tombstone",
            ));
        }
        let tombstone_id = self
            .next_tombstone_id
            .ok_or(SessionError::TerminalTombstoneIdExhausted)?;
        let terminal = TerminalGrace {
            session_id: self.session_id,
            flow_id,
            tombstone_id,
        };

        self.flows
            .remove(&flow_id)
            .ok_or(SessionError::UnknownFlow(flow_id))?;
        let usage = self.replay_usage_mut(direction);
        usage.bytes -= send_bytes;
        usage.segments -= send_segments;
        self.receive_usage.bytes -= receive_bytes;
        self.receive_usage.segments -= receive_ranges;
        self.next_tombstone_id = tombstone_id.checked_add(1);
        self.terminal_tombstones.insert(
            flow_id,
            TerminalTombstone {
                terminal,
                target,
                replay_control,
            },
        );
        Ok(vec![SessionEffect::FlowFinished {
            flow_id,
            reason,
            terminal,
        }])
    }

    fn replay_terminal_control(&self, tombstone: &TerminalTombstone) -> Vec<SessionEffect> {
        tombstone
            .replay_control
            .clone()
            .map(|control| self.transmit_if_active(control))
            .unwrap_or_default()
    }

    fn transmit_if_active(&self, record: Record) -> Vec<SessionEffect> {
        if self.phase == SessionPhase::Active {
            vec![SessionEffect::Transmit(Frame::new(
                self.current_leg.generation(),
                record,
            ))]
        } else {
            Vec::new()
        }
    }

    fn recovery_effects(&self) -> Vec<SessionEffect> {
        let generation = self.current_leg.generation();
        let mut effects = vec![SessionEffect::LegActivated { generation }];
        let local_direction = self.role.local_send_direction();
        let peer_direction = self.role.peer_receive_direction();

        for tombstone in self.terminal_tombstones.values() {
            if let Some(control) = &tombstone.replay_control {
                effects.push(SessionEffect::Transmit(Frame::new(
                    generation,
                    control.clone(),
                )));
            }
        }

        for (&flow_id, flow) in &self.flows {
            if flow.origin == FlowOrigin::Local && flow.local_open_pending {
                effects.push(SessionEffect::Transmit(Frame::new(
                    generation,
                    Record::Open {
                        flow_id,
                        target: flow.target.clone(),
                    },
                )));
            }
            if flow.origin == FlowOrigin::Peer
                && let Some(result) = flow.peer_open_result
            {
                effects.push(SessionEffect::Transmit(Frame::new(
                    generation,
                    Record::OpenResult { flow_id, result },
                )));
            }

            let receive = flow.receive.snapshot();
            if receive.accepted().get() > 0
                || receive.buffered_bytes() > 0
                || receive.final_offset().is_some()
            {
                effects.push(SessionEffect::Transmit(Frame::new(
                    generation,
                    Record::Ack {
                        flow_id,
                        direction: peer_direction,
                        next_accepted: receive.accepted(),
                        final_accepted: flow.receive_final_accepted,
                    },
                )));
            }

            for segment in flow.send.replay_view() {
                effects.push(SessionEffect::Transmit(Frame::new(
                    generation,
                    Record::Data {
                        flow_id,
                        direction: local_direction,
                        offset: segment.offset(),
                        payload: segment.payload().clone(),
                    },
                )));
            }
            if let Some(final_offset) = flow.send.final_offset()
                && !flow.send.final_accepted()
            {
                effects.push(SessionEffect::Transmit(Frame::new(
                    generation,
                    Record::Close {
                        flow_id,
                        direction: local_direction,
                        final_offset,
                    },
                )));
            }
        }
        effects
    }
}

fn same_session_semantics(current: &CommittedLeg, replacement: &CommittedLeg) -> bool {
    current.owner_identity() == replacement.owner_identity()
        && current.device_principal() == replacement.device_principal()
        && current.transport_binding().alpn() == replacement.transport_binding().alpn()
        && current.session_protocol_version() == replacement.session_protocol_version()
        && current.negotiated_features() == replacement.negotiated_features()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SessionFlowSnapshot {
    send: TcpSendSnapshot,
    receive: TcpReceiveSnapshot,
    has_outstanding_sink_offer: bool,
    has_pending_half_close: bool,
}

impl SessionFlowSnapshot {
    pub const fn send(self) -> TcpSendSnapshot {
        self.send
    }

    pub const fn receive(self) -> TcpReceiveSnapshot {
        self.receive
    }

    pub const fn has_outstanding_sink_offer(self) -> bool {
        self.has_outstanding_sink_offer
    }

    pub const fn has_pending_half_close(self) -> bool {
        self.has_pending_half_close
    }
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum SessionConfigError {
    #[error("session flow capacity must be non-zero")]
    ZeroFlowCapacity,
    #[error("global replay byte capacity must be non-zero")]
    ZeroReplayByteCapacity,
    #[error("global replay segment capacity must be non-zero")]
    ZeroReplaySegmentCapacity,
    #[error("global receive byte capacity must be non-zero")]
    ZeroReceiveByteCapacity,
    #[error("global receive range capacity must be non-zero")]
    ZeroReceiveRangeCapacity,
    #[error("sink offer byte capacity must be non-zero")]
    ZeroSinkOfferCapacity,
    #[error("terminal tombstone capacity must be non-zero")]
    ZeroTerminalTombstoneCapacity,
    #[error("capacity plan produced an invalid TCP window for {direction:?}: {source}")]
    InvalidDerivedTcpWindow {
        direction: Direction,
        #[source]
        source: TcpOwnershipError,
    },
    #[error("derived normalized receive backing bound overflows usize")]
    ReceiveBackingBoundOverflow,
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum SessionError {
    #[error("leg belongs to session {actual:?}, expected {expected:?}")]
    CrossSessionLeg {
        expected: SessionId,
        actual: SessionId,
    },
    #[error("leg generation {actual} does not equal current generation {current}")]
    LegGenerationMismatch { current: u64, actual: u64 },
    #[error("leg capability does not match the committed session capability")]
    LegCapabilityMismatch,
    #[error("frame generation {actual} does not equal current generation {current}")]
    FrameGenerationMismatch { current: u64, actual: u64 },
    #[error("replacement generation {requested} is not current {current} plus one")]
    ReplacementGenerationNotNext { current: u64, requested: u64 },
    #[error("leg generation is exhausted")]
    LegGenerationExhausted,
    #[error("session resume grace expired")]
    SessionExpired,
    #[error("resume grace is not active")]
    ResumeGraceNotActive,
    #[error("peer frame arrived while the session has no active leg")]
    PeerFrameWhileLegless,
    #[error("{0:?} role cannot initiate a Target OPEN")]
    RoleCannotInitiateOpen(SessionRole),
    #[error("{0:?} role cannot resolve a peer Target OPEN")]
    RoleCannotResolvePeerOpen(SessionRole),
    #[error("{0:?} role cannot receive a peer OPEN")]
    UnexpectedPeerOpen(SessionRole),
    #[error("{0:?} role cannot receive a peer OPEN_RESULT")]
    UnexpectedPeerOpenResult(SessionRole),
    #[error("session flow capacity {max} is exhausted")]
    FlowCapacityExceeded { max: usize },
    #[error("session flow id space is exhausted")]
    FlowIdExhausted,
    #[error("local flow authority id space is exhausted")]
    LocalFlowAuthorityExhausted,
    #[error("peer OPEN request id space is exhausted")]
    OpenRequestIdExhausted,
    #[error("peer flow id {attempted} is not above high-water mark {highest}")]
    PeerFlowIdNotMonotonic { highest: u64, attempted: u64 },
    #[error("flow {0:?} has retired terminal ownership")]
    RetiredFlow(SessionFlowId),
    #[error("OPEN for flow {flow_id:?} conflicts with its original Target")]
    FlowTargetConflict { flow_id: SessionFlowId },
    #[error("unknown flow {0:?}")]
    UnknownFlow(SessionFlowId),
    #[error("flow {flow_id:?} has the wrong OPEN origin for this event")]
    FlowOriginMismatch { flow_id: SessionFlowId },
    #[error("flow {flow_id:?} local source is not open yet")]
    LocalSourceNotOpen { flow_id: SessionFlowId },
    #[error("flow {flow_id:?} OPEN_RESULT changed from {recorded:?} to {attempted:?}")]
    ConflictingOpenResult {
        flow_id: SessionFlowId,
        recorded: OpenResultCode,
        attempted: OpenResultCode,
    },
    #[error("expected direction {expected:?}, got {actual:?}")]
    UnexpectedDirection {
        expected: Direction,
        actual: Direction,
    },
    #[error("replay byte budget for {direction:?} exceeded: attempted {attempted}, max {max}")]
    ReplayByteBudgetExceeded {
        direction: Direction,
        attempted: usize,
        max: usize,
    },
    #[error("replay segment budget for {direction:?} exceeded: attempted {attempted}, max {max}")]
    ReplaySegmentBudgetExceeded {
        direction: Direction,
        attempted: usize,
        max: usize,
    },
    #[error("global receive byte budget exceeded: attempted {attempted}, max {max}")]
    ReceiveByteBudgetExceeded { attempted: usize, max: usize },
    #[error("global receive range budget exceeded: attempted {attempted}, max {max}")]
    ReceiveRangeBudgetExceeded { attempted: usize, max: usize },
    #[error("TCP ownership error for flow {flow_id:?}: {source}")]
    TcpOwnership {
        flow_id: SessionFlowId,
        #[source]
        source: TcpOwnershipError,
    },
    #[error("invalid session protocol record: {source}")]
    InvalidProtocolRecord {
        #[source]
        source: ProtocolError,
    },
    #[error("ATTACH records must be consumed by authentication before the session reducer")]
    UnexpectedAttachRecord,
    #[error("sink acceptance must contain at least one byte")]
    ZeroSinkAcceptance,
    #[error("sink accepted {attempted} bytes from an offer of {offered}")]
    SinkAcceptanceTooLarge { offered: usize, attempted: usize },
    #[error("sink offer does not match the one outstanding offer for its flow")]
    SinkOfferMismatch,
    #[error("sink offer id space is exhausted")]
    SinkOfferIdExhausted,
    #[error("half-close completion id space is exhausted")]
    HalfCloseCompletionIdExhausted,
    #[error("terminal tombstone id space is exhausted")]
    TerminalTombstoneIdExhausted,
    #[error("terminal tombstone capacity {max} is exhausted")]
    TerminalTombstoneCapacityExceeded { max: usize },
    #[error("local flow authority belongs to another session")]
    CrossSessionLocalFlow,
    #[error("local flow authority does not match the active flow")]
    LocalFlowAuthorityMismatch,
    #[error("OPEN request authority belongs to another session")]
    CrossSessionOpenRequest,
    #[error("OPEN request authority does not match the pending request")]
    OpenRequestMismatch,
    #[error("sink offer belongs to another session")]
    CrossSessionSinkOffer,
    #[error("half-close completion belongs to another session")]
    CrossSessionHalfClose,
    #[error("half-close completion does not match the pending operation")]
    HalfCloseCompletionMismatch,
    #[error("terminal grace authority belongs to another session")]
    CrossSessionTerminalGrace,
    #[error("terminal grace authority is unknown or stale")]
    UnknownTerminalGrace,
    #[error("session reducer invariant violated: {0}")]
    InvariantViolation(&'static str),
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    use std::time::Duration;

    use super::*;
    use crate::resumable::auth::{
        AttachAlpn, AttachAuthority, AttachCredentials, AttachPolicy, AttachRequest,
        AttachTransportBinding, DevicePrincipal, DeviceSecret, FeatureOffer, OwnerIdentity,
        ResumeSecret, TlsExporterBinding, VersionRange,
    };
    use crate::resumable::capacity::{
        BitsPerSecond, DirectionalRates, ReplayCapacitySpec, ReplayStorageGeometry,
    };
    use crate::resumable::protocol::{AttachNonce, AttachProof, SESSION_PROTOCOL_VERSION};

    struct LegIssuer {
        authority: AttachAuthority,
        session_id: SessionId,
        binding: AttachTransportBinding,
    }

    impl LegIssuer {
        fn new(session_byte: u8) -> Self {
            Self::with_identity(session_byte, 0x31, 0x53)
        }

        fn with_identity(session_byte: u8, owner_byte: u8, principal_byte: u8) -> Self {
            Self::with_semantics(
                session_byte,
                owner_byte,
                principal_byte,
                b"mini-vpn-owned/1",
            )
        }

        fn with_semantics(
            session_byte: u8,
            owner_byte: u8,
            principal_byte: u8,
            alpn_bytes: &[u8],
        ) -> Self {
            Self::with_transport(session_byte, owner_byte, principal_byte, alpn_bytes, 0x42)
        }

        fn with_transport(
            session_byte: u8,
            owner_byte: u8,
            principal_byte: u8,
            alpn_bytes: &[u8],
            exporter_byte: u8,
        ) -> Self {
            let session_id = SessionId::new([session_byte; 16]).unwrap();
            let owner = OwnerIdentity::new([owner_byte; 32]).unwrap();
            let principal = DevicePrincipal::new([principal_byte; 16]).unwrap();
            let alpn = AttachAlpn::new(alpn_bytes).unwrap();
            let binding = AttachTransportBinding::new(
                owner,
                alpn,
                TlsExporterBinding::new([exporter_byte; 32]).unwrap(),
                principal,
            );
            let policy =
                AttachPolicy::new(owner, alpn, principal, SESSION_PROTOCOL_VERSION, 0b1111)
                    .unwrap();
            Self {
                authority: AttachAuthority::new(
                    session_id,
                    LegGeneration::new(1).unwrap(),
                    credentials(),
                    policy,
                ),
                session_id,
                binding,
            }
        }

        fn issue(&self, generation: u64) -> CommittedLeg {
            let request = AttachRequest::new(
                self.session_id,
                LegGeneration::new(generation).unwrap(),
                AttachNonce::new([generation as u8; 16]).unwrap(),
                VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION).unwrap(),
                FeatureOffer::new(0b0111, 0b0001).unwrap(),
            );
            let proof = signing_credentials()
                .prove(&request, &self.binding)
                .unwrap();
            self.authority
                .verify_and_commit(&request, &self.binding, &proof)
                .unwrap()
        }
    }

    fn credentials() -> AttachCredentials {
        AttachCredentials::new(
            DeviceSecret::new([0xde; 32]).unwrap(),
            ResumeSecret::new([0xad; 32]).unwrap(),
        )
        .unwrap()
    }

    fn signing_credentials() -> AttachCredentials {
        credentials()
    }

    fn config() -> SessionConfig {
        config_with_replay(64, 8)
    }

    fn config_with_replay(max_bytes: usize, max_segments: usize) -> SessionConfig {
        SessionConfig::new(
            8,
            32,
            TcpWindowLimits::new(64, 8).unwrap(),
            TcpWindowLimits::new(64, 8).unwrap(),
            ReplayBudgetLimits::new(max_bytes, max_segments).unwrap(),
            ReplayBudgetLimits::new(max_bytes, max_segments).unwrap(),
            ReceiveBudgetLimits::new(128, 32).unwrap(),
            16,
        )
        .unwrap()
    }

    fn config_with_limits(
        max_flows: usize,
        max_tombstones: usize,
        replay_bytes: usize,
        replay_segments: usize,
        receive_bytes: usize,
        receive_ranges: usize,
    ) -> SessionConfig {
        SessionConfig::new(
            max_flows,
            max_tombstones,
            TcpWindowLimits::new(64, 8).unwrap(),
            TcpWindowLimits::new(64, 8).unwrap(),
            ReplayBudgetLimits::new(replay_bytes, replay_segments).unwrap(),
            ReplayBudgetLimits::new(replay_bytes, replay_segments).unwrap(),
            ReceiveBudgetLimits::new(receive_bytes, receive_ranges).unwrap(),
            16,
        )
        .unwrap()
    }

    fn target(port: u16) -> TargetAddr {
        TargetAddr::IpPort(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port)))
    }

    fn frame(leg: CommittedLeg, record: Record) -> Frame {
        Frame::new(leg.generation(), record)
    }

    fn local_open(
        model: &mut SessionModel,
        leg: CommittedLeg,
        target: TargetAddr,
    ) -> SessionFlowId {
        let effects = model
            .reduce(SessionEvent::LocalOpen { leg, target })
            .unwrap();
        match effects.as_slice() {
            [
                SessionEffect::LocalFlowOpened { flow },
                SessionEffect::Transmit(frame),
            ] => match frame.record() {
                Record::Open { flow_id, .. } if flow.flow_id() == *flow_id => *flow_id,
                other => panic!("expected OPEN, got {other:?}"),
            },
            other => panic!("expected local authority and transmit effects, got {other:?}"),
        }
    }

    fn local_flow(model: &SessionModel, flow_id: SessionFlowId) -> LocalFlow {
        model.flows.get(&flow_id).unwrap().local_flow.unwrap()
    }

    fn peer_open(
        model: &mut SessionModel,
        leg: CommittedLeg,
        flow_id: SessionFlowId,
        target: TargetAddr,
    ) -> Vec<SessionEffect> {
        model
            .reduce(SessionEvent::PeerFrame {
                leg,
                frame: frame(leg, Record::Open { flow_id, target }),
            })
            .unwrap()
    }

    fn only_peer_open_request(effects: &[SessionEffect]) -> (PeerOpenRequest, LocalFlow) {
        match effects {
            [SessionEffect::PeerOpenRequested { request, flow, .. }] => (*request, *flow),
            other => panic!("expected one peer OPEN request, got {other:?}"),
        }
    }

    fn peer_opened(
        model: &mut SessionModel,
        leg: CommittedLeg,
        flow_id: SessionFlowId,
        target: TargetAddr,
    ) -> LocalFlow {
        let requested = peer_open(model, leg, flow_id, target);
        let (request, flow) = only_peer_open_request(&requested);
        model
            .reduce(SessionEvent::PeerOpenResolved {
                request,
                result: OpenResultCode::Opened,
            })
            .unwrap();
        flow
    }

    fn only_offer(effects: &[SessionEffect]) -> (SinkOffer, &[TcpDataSegment]) {
        match effects {
            [SessionEffect::OfferToSink { offer, segments }] => (*offer, segments),
            other => panic!("expected one sink offer, got {other:?}"),
        }
    }

    fn transmitted_records(effects: &[SessionEffect]) -> Vec<&Record> {
        effects
            .iter()
            .filter_map(|effect| match effect {
                SessionEffect::Transmit(frame) => Some(frame.record()),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn stale_frame_cross_session_and_nonexact_capability_never_mutate_flows() {
        let issuer = LegIssuer::new(0x11);
        let leg2 = issuer.issue(2);
        let mut model = SessionModel::new(SessionRole::Client, config(), leg2);
        let flow_id = local_open(&mut model, leg2, target(443));
        let before = model.snapshot();
        let flow_before = model.flow_snapshot(flow_id);

        let other = LegIssuer::new(0x99);
        let other_leg = other.issue(2);
        assert!(matches!(
            model.reduce(SessionEvent::PeerFrame {
                leg: other_leg,
                frame: frame(
                    other_leg,
                    Record::Ack {
                        flow_id,
                        direction: Direction::ClientToTarget,
                        next_accepted: ByteOffset::new(0),
                        final_accepted: false,
                    },
                ),
            }),
            Err(SessionError::CrossSessionLeg { .. })
        ));
        assert_eq!(model.snapshot(), before);
        assert_eq!(model.flow_snapshot(flow_id), flow_before);

        let same_session_other_identity = LegIssuer::with_identity(0x11, 0x71, 0x72);
        let forged_capability = same_session_other_identity.issue(2);
        assert!(matches!(
            model.reduce(SessionEvent::PeerFrame {
                leg: forged_capability,
                frame: frame(
                    forged_capability,
                    Record::Ack {
                        flow_id,
                        direction: Direction::ClientToTarget,
                        next_accepted: ByteOffset::new(0),
                        final_accepted: false,
                    },
                ),
            }),
            Err(SessionError::LegCapabilityMismatch)
        ));
        assert_eq!(model.snapshot(), before);
        assert_eq!(model.flow_snapshot(flow_id), flow_before);

        let leg3 = issuer.issue(3);
        model
            .reduce(SessionEvent::ReplacementAttached { leg: leg3 })
            .unwrap();
        let attached = model.snapshot();
        let attached_flow = model.flow_snapshot(flow_id);
        assert!(matches!(
            model.reduce(SessionEvent::PeerFrame {
                leg: leg2,
                frame: frame(
                    leg2,
                    Record::Ack {
                        flow_id,
                        direction: Direction::ClientToTarget,
                        next_accepted: ByteOffset::new(0),
                        final_accepted: false,
                    },
                ),
            }),
            Err(SessionError::LegGenerationMismatch {
                current: 3,
                actual: 2
            })
        ));
        assert_eq!(model.snapshot(), attached);
        assert_eq!(model.flow_snapshot(flow_id), attached_flow);

        assert!(matches!(
            model.reduce(SessionEvent::PeerFrame {
                leg: leg3,
                frame: frame(
                    leg2,
                    Record::Ack {
                        flow_id,
                        direction: Direction::ClientToTarget,
                        next_accepted: ByteOffset::new(0),
                        final_accepted: false,
                    },
                ),
            }),
            Err(SessionError::FrameGenerationMismatch {
                current: 3,
                actual: 2
            })
        ));
        assert_eq!(model.snapshot(), attached);
        assert_eq!(model.flow_snapshot(flow_id), attached_flow);
    }

    #[test]
    fn replacement_requires_stable_alpn_but_allows_a_new_tls_exporter() {
        let issuer = LegIssuer::new(0x12);
        let leg2 = issuer.issue(2);
        let mut model = SessionModel::new(SessionRole::Client, config(), leg2);

        let wrong_alpn = LegIssuer::with_semantics(0x12, 0x31, 0x53, b"mini-vpn-other/1");
        let _wrong_leg2 = wrong_alpn.issue(2);
        let wrong_leg3 = wrong_alpn.issue(3);
        let before = model.snapshot();
        assert_eq!(
            model.reduce(SessionEvent::ReplacementAttached { leg: wrong_leg3 }),
            Err(SessionError::LegCapabilityMismatch)
        );
        assert_eq!(model.snapshot(), before);

        let changed_exporter =
            LegIssuer::with_transport(0x12, 0x31, 0x53, b"mini-vpn-owned/1", 0x67);
        let _changed_leg2 = changed_exporter.issue(2);
        let leg3 = changed_exporter.issue(3);
        assert!(
            model
                .reduce(SessionEvent::ReplacementAttached { leg: leg3 })
                .is_ok()
        );
    }

    #[test]
    fn owner_open_is_idempotent_but_conflict_and_reuse_fail_closed() {
        let issuer = LegIssuer::new(0x11);
        let leg = issuer.issue(2);
        let mut model = SessionModel::new(SessionRole::Owner, config(), leg);
        let flow1 = SessionFlowId::new(1).unwrap();

        let first = peer_open(&mut model, leg, flow1, target(443));
        let (request, local) = only_peer_open_request(&first);
        assert_eq!(request.flow_id(), flow1);
        assert_eq!(local.flow_id(), flow1);
        assert!(peer_open(&mut model, leg, flow1, target(443)).is_empty());
        let before_conflict = model.snapshot();
        assert!(matches!(
            model.reduce(SessionEvent::PeerFrame {
                leg,
                frame: frame(
                    leg,
                    Record::Open {
                        flow_id: flow1,
                        target: target(8443),
                    },
                ),
            }),
            Err(SessionError::FlowTargetConflict { flow_id }) if flow_id == flow1
        ));
        assert_eq!(model.snapshot(), before_conflict);

        model
            .reduce(SessionEvent::PeerFrame {
                leg,
                frame: frame(
                    leg,
                    Record::Reset {
                        flow_id: flow1,
                        reason: ResetReason::TargetFailure,
                    },
                ),
            })
            .unwrap();
        let terminal = model.snapshot();
        assert!(
            model
                .reduce(SessionEvent::PeerFrame {
                    leg,
                    frame: frame(
                        leg,
                        Record::Open {
                            flow_id: flow1,
                            target: target(443),
                        },
                    ),
                })
                .unwrap()
                .is_empty()
        );
        assert_eq!(model.snapshot(), terminal);

        let flow3 = SessionFlowId::new(3).unwrap();
        peer_open(&mut model, leg, flow3, target(443));
        let before_old_new = model.snapshot();
        let flow2 = SessionFlowId::new(2).unwrap();
        assert_eq!(
            model.reduce(SessionEvent::PeerFrame {
                leg,
                frame: frame(
                    leg,
                    Record::Open {
                        flow_id: flow2,
                        target: target(443),
                    },
                ),
            }),
            Err(SessionError::PeerFlowIdNotMonotonic {
                highest: 3,
                attempted: 2,
            })
        );
        assert_eq!(model.snapshot(), before_old_new);
    }

    #[test]
    fn local_data_and_partial_ack_update_exact_global_budget_before_backpressure() {
        let issuer = LegIssuer::new(0x11);
        let leg = issuer.issue(2);
        let mut model = SessionModel::new(SessionRole::Client, config_with_replay(6, 2), leg);
        let flow1 = local_open(&mut model, leg, target(443));
        let flow2 = local_open(&mut model, leg, target(8443));
        let local1 = local_flow(&model, flow1);
        let local2 = local_flow(&model, flow2);

        model
            .reduce(SessionEvent::LocalData {
                flow: local1,
                payload: Bytes::from_static(b"abcd"),
            })
            .unwrap();
        assert_eq!(
            model.snapshot().replay_usage(Direction::ClientToTarget),
            ReplayBudgetUsage {
                bytes: 4,
                segments: 1,
            }
        );
        let before_backpressure = model.snapshot();
        assert_eq!(
            model.reduce(SessionEvent::LocalData {
                flow: local2,
                payload: Bytes::from_static(b"wxyz"),
            }),
            Err(SessionError::ReplayByteBudgetExceeded {
                direction: Direction::ClientToTarget,
                attempted: 8,
                max: 6,
            })
        );
        assert_eq!(model.snapshot(), before_backpressure);
        assert_eq!(
            model.flow_snapshot(flow2).unwrap().send().next_sent(),
            ByteOffset::new(0)
        );

        model
            .reduce(SessionEvent::PeerFrame {
                leg,
                frame: frame(
                    leg,
                    Record::Ack {
                        flow_id: flow1,
                        direction: Direction::ClientToTarget,
                        next_accepted: ByteOffset::new(2),
                        final_accepted: false,
                    },
                ),
            })
            .unwrap();
        assert_eq!(
            model.snapshot().replay_usage(Direction::ClientToTarget),
            ReplayBudgetUsage {
                bytes: 2,
                segments: 1,
            }
        );
        model
            .reduce(SessionEvent::LocalData {
                flow: local2,
                payload: Bytes::from_static(b"wxyz"),
            })
            .unwrap();
        assert_eq!(
            model.snapshot().replay_usage(Direction::ClientToTarget),
            ReplayBudgetUsage {
                bytes: 6,
                segments: 2,
            }
        );

        let before_bad_ack = model.snapshot();
        assert!(matches!(
            model.reduce(SessionEvent::PeerFrame {
                leg,
                frame: frame(
                    leg,
                    Record::Ack {
                        flow_id: flow1,
                        direction: Direction::ClientToTarget,
                        next_accepted: ByteOffset::new(5),
                        final_accepted: false,
                    },
                ),
            }),
            Err(SessionError::TcpOwnership {
                source: TcpOwnershipError::AckBeyondSent { .. },
                ..
            })
        ));
        assert_eq!(model.snapshot(), before_bad_ack);
        assert!(matches!(
            model.reduce(SessionEvent::PeerFrame {
                leg,
                frame: frame(
                    leg,
                    Record::Ack {
                        flow_id: flow1,
                        direction: Direction::ClientToTarget,
                        next_accepted: ByteOffset::new(1),
                        final_accepted: false,
                    },
                ),
            }),
            Err(SessionError::TcpOwnership {
                source: TcpOwnershipError::AckRegression { .. },
                ..
            })
        ));
        assert_eq!(model.snapshot(), before_bad_ack);
    }

    #[test]
    fn gaps_reorder_and_duplicates_offer_once_then_sink_acceptance_acks_exactly() {
        let issuer = LegIssuer::new(0x11);
        let leg = issuer.issue(2);
        let mut model = SessionModel::new(SessionRole::Owner, config(), leg);
        let flow_id = SessionFlowId::new(1).unwrap();
        peer_opened(&mut model, leg, flow_id, target(443));

        let gap = model
            .reduce(SessionEvent::PeerFrame {
                leg,
                frame: frame(
                    leg,
                    Record::Data {
                        flow_id,
                        direction: Direction::ClientToTarget,
                        offset: ByteOffset::new(3),
                        payload: Bytes::from_static(b"def"),
                    },
                ),
            })
            .unwrap();
        assert!(gap.is_empty());
        assert_eq!(
            model.flow_snapshot(flow_id).unwrap().receive().accepted(),
            ByteOffset::new(0)
        );

        let filled = model
            .reduce(SessionEvent::PeerFrame {
                leg,
                frame: frame(
                    leg,
                    Record::Data {
                        flow_id,
                        direction: Direction::ClientToTarget,
                        offset: ByteOffset::new(0),
                        payload: Bytes::from_static(b"abc"),
                    },
                ),
            })
            .unwrap();
        assert!(transmitted_records(&filled).is_empty());
        let (first_offer, segments) = only_offer(&filled);
        assert_eq!(first_offer.offset(), ByteOffset::new(0));
        assert_eq!(first_offer.len(), 6);
        assert_eq!(segments.len(), 2);

        let duplicate = model
            .reduce(SessionEvent::PeerFrame {
                leg,
                frame: frame(
                    leg,
                    Record::Data {
                        flow_id,
                        direction: Direction::ClientToTarget,
                        offset: ByteOffset::new(0),
                        payload: Bytes::from_static(b"abc"),
                    },
                ),
            })
            .unwrap();
        assert!(duplicate.is_empty());
        assert_eq!(model.snapshot().outstanding_sink_offers(), 1);

        let partial = model
            .reduce(SessionEvent::SinkAccepted {
                offer: first_offer,
                bytes: 2,
            })
            .unwrap();
        let records = transmitted_records(&partial);
        assert!(matches!(
            records.as_slice(),
            [Record::Ack {
                next_accepted,
                final_accepted: false,
                ..
            }] if *next_accepted == ByteOffset::new(2)
        ));
        let second_offer = partial
            .iter()
            .find_map(|effect| match effect {
                SessionEffect::OfferToSink { offer, .. } => Some(*offer),
                _ => None,
            })
            .unwrap();
        assert_eq!(second_offer.offset(), ByteOffset::new(2));
        assert_eq!(second_offer.len(), 4);

        let before_old_completion = model.snapshot();
        assert_eq!(
            model.reduce(SessionEvent::SinkAccepted {
                offer: first_offer,
                bytes: 1,
            }),
            Err(SessionError::SinkOfferMismatch)
        );
        assert_eq!(model.snapshot(), before_old_completion);

        model
            .reduce(SessionEvent::SinkAccepted {
                offer: second_offer,
                bytes: 4,
            })
            .unwrap();
        assert_eq!(
            model.flow_snapshot(flow_id).unwrap().receive().accepted(),
            ByteOffset::new(6)
        );
        assert_eq!(model.snapshot().outstanding_sink_offers(), 0);
    }

    #[test]
    fn sink_abandon_releases_receive_ownership_and_resets_without_ack() {
        let issuer = LegIssuer::new(0x11);
        let leg2 = issuer.issue(2);
        let mut model = SessionModel::new(SessionRole::Owner, config(), leg2);
        let flow_id = SessionFlowId::new(1).unwrap();
        peer_opened(&mut model, leg2, flow_id, target(443));
        let effects = model
            .reduce(SessionEvent::PeerFrame {
                leg: leg2,
                frame: frame(
                    leg2,
                    Record::Data {
                        flow_id,
                        direction: Direction::ClientToTarget,
                        offset: ByteOffset::new(0),
                        payload: Bytes::from_static(b"payload"),
                    },
                ),
            })
            .unwrap();
        let (offer, _) = only_offer(&effects);

        let abandoned = model.reduce(SessionEvent::SinkAbandoned { offer }).unwrap();
        let records = transmitted_records(&abandoned);
        assert!(matches!(
            records.as_slice(),
            [Record::Reset {
                flow_id: actual,
                reason: ResetReason::LocalAbandon,
            }] if *actual == flow_id
        ));
        assert!(
            !records
                .iter()
                .any(|record| matches!(record, Record::Ack { .. }))
        );
        assert_eq!(model.snapshot().receive_owned_bytes(), 0);
        assert!(model.flow_snapshot(flow_id).is_none());
        assert_eq!(model.snapshot().terminal_tombstones(), 1);

        model.reduce(SessionEvent::LegLost { leg: leg2 }).unwrap();
        let leg3 = issuer.issue(3);
        let replay = model
            .reduce(SessionEvent::ReplacementAttached { leg: leg3 })
            .unwrap();
        assert!(
            transmitted_records(&replay)
                .iter()
                .any(|record| matches!(record, Record::Reset { .. }))
        );
        assert!(
            !replay
                .iter()
                .any(|effect| matches!(effect, SessionEffect::OfferToSink { .. }))
        );
        assert!(
            !transmitted_records(&replay)
                .iter()
                .any(|record| matches!(record, Record::Ack { .. }))
        );
    }

    #[test]
    fn early_close_half_closes_only_after_sink_acceptance_and_final_ack() {
        let issuer = LegIssuer::new(0x11);
        let leg = issuer.issue(2);
        let mut model = SessionModel::new(SessionRole::Owner, config(), leg);
        let flow_id = SessionFlowId::new(1).unwrap();
        peer_opened(&mut model, leg, flow_id, target(443));

        let early = model
            .reduce(SessionEvent::PeerFrame {
                leg,
                frame: frame(
                    leg,
                    Record::Close {
                        flow_id,
                        direction: Direction::ClientToTarget,
                        final_offset: ByteOffset::new(3),
                    },
                ),
            })
            .unwrap();
        assert!(matches!(
            transmitted_records(&early).as_slice(),
            [Record::Ack {
                next_accepted,
                final_accepted: false,
                ..
            }] if *next_accepted == ByteOffset::new(0)
        ));
        assert!(
            !early
                .iter()
                .any(|effect| matches!(effect, SessionEffect::HalfCloseSink { .. }))
        );

        let data = model
            .reduce(SessionEvent::PeerFrame {
                leg,
                frame: frame(
                    leg,
                    Record::Data {
                        flow_id,
                        direction: Direction::ClientToTarget,
                        offset: ByteOffset::new(0),
                        payload: Bytes::from_static(b"abc"),
                    },
                ),
            })
            .unwrap();
        let (offer, _) = only_offer(&data);
        let accepted = model
            .reduce(SessionEvent::SinkAccepted { offer, bytes: 3 })
            .unwrap();
        assert!(transmitted_records(&accepted).iter().any(|record| matches!(
            record,
            Record::Ack {
                next_accepted,
                final_accepted: false,
                ..
            } if *next_accepted == ByteOffset::new(3)
        )));
        let completion = accepted
            .iter()
            .find_map(|effect| match effect {
                SessionEffect::HalfCloseSink { completion } => Some(*completion),
                _ => None,
            })
            .expect("final bytes must request one sink half-close");

        let duplicate_close = model
            .reduce(SessionEvent::PeerFrame {
                leg,
                frame: frame(
                    leg,
                    Record::Close {
                        flow_id,
                        direction: Direction::ClientToTarget,
                        final_offset: ByteOffset::new(3),
                    },
                ),
            })
            .unwrap();
        assert!(
            transmitted_records(&duplicate_close)
                .iter()
                .any(|record| matches!(
                    record,
                    Record::Ack {
                        final_accepted: false,
                        ..
                    }
                ))
        );
        assert!(
            !duplicate_close
                .iter()
                .any(|effect| matches!(effect, SessionEffect::HalfCloseSink { .. }))
        );

        let half_closed = model
            .reduce(SessionEvent::SinkHalfClosed { completion })
            .unwrap();
        assert!(
            transmitted_records(&half_closed)
                .iter()
                .any(|record| matches!(
                    record,
                    Record::Ack {
                        next_accepted,
                        final_accepted: true,
                        ..
                    } if *next_accepted == ByteOffset::new(3)
                ))
        );
    }

    #[test]
    fn replacement_replays_exact_sender_bytes_and_close_until_final_ack_zero_copy() {
        let issuer = LegIssuer::new(0x11);
        let leg2 = issuer.issue(2);
        let mut model = SessionModel::new(SessionRole::Client, config(), leg2);
        let flow_id = local_open(&mut model, leg2, target(443));
        let local = local_flow(&model, flow_id);
        model
            .reduce(SessionEvent::LocalData {
                flow: local,
                payload: Bytes::from_static(b"abcdef"),
            })
            .unwrap();
        model
            .reduce(SessionEvent::LocalClose { flow: local })
            .unwrap();
        model.reduce(SessionEvent::LegLost { leg: leg2 }).unwrap();
        assert_eq!(model.snapshot().phase(), SessionPhase::Legless);

        let leg3 = issuer.issue(3);
        let replay3 = model
            .reduce(SessionEvent::ReplacementAttached { leg: leg3 })
            .unwrap();
        let (pointer3, len3) = transmitted_records(&replay3)
            .iter()
            .find_map(|record| match record {
                Record::Data { payload, .. } => Some((payload.as_ptr(), payload.len())),
                _ => None,
            })
            .unwrap();
        assert_eq!(len3, 6);
        assert!(transmitted_records(&replay3)
            .iter()
            .any(|record| matches!(record, Record::Close { final_offset, .. } if *final_offset == ByteOffset::new(6))));

        model.reduce(SessionEvent::LegLost { leg: leg3 }).unwrap();
        let leg4 = issuer.issue(4);
        let replay4 = model
            .reduce(SessionEvent::ReplacementAttached { leg: leg4 })
            .unwrap();
        let pointer4 = transmitted_records(&replay4)
            .iter()
            .find_map(|record| match record {
                Record::Data { payload, .. } => Some(payload.as_ptr()),
                _ => None,
            })
            .unwrap();
        assert_eq!(
            pointer3, pointer4,
            "replay Bytes must share retained storage"
        );

        model
            .reduce(SessionEvent::PeerFrame {
                leg: leg4,
                frame: frame(
                    leg4,
                    Record::Ack {
                        flow_id,
                        direction: Direction::ClientToTarget,
                        next_accepted: ByteOffset::new(6),
                        final_accepted: false,
                    },
                ),
            })
            .unwrap();
        model.reduce(SessionEvent::LegLost { leg: leg4 }).unwrap();
        let leg5 = issuer.issue(5);
        let replay5 = model
            .reduce(SessionEvent::ReplacementAttached { leg: leg5 })
            .unwrap();
        assert!(
            !transmitted_records(&replay5)
                .iter()
                .any(|record| matches!(record, Record::Data { .. }))
        );
        assert!(
            transmitted_records(&replay5)
                .iter()
                .any(|record| matches!(record, Record::Close { .. }))
        );

        model
            .reduce(SessionEvent::PeerFrame {
                leg: leg5,
                frame: frame(
                    leg5,
                    Record::Ack {
                        flow_id,
                        direction: Direction::ClientToTarget,
                        next_accepted: ByteOffset::new(6),
                        final_accepted: true,
                    },
                ),
            })
            .unwrap();
        model.reduce(SessionEvent::LegLost { leg: leg5 }).unwrap();
        let leg6 = issuer.issue(6);
        let replay6 = model
            .reduce(SessionEvent::ReplacementAttached { leg: leg6 })
            .unwrap();
        assert!(
            !transmitted_records(&replay6)
                .iter()
                .any(|record| matches!(record, Record::Data { .. } | Record::Close { .. }))
        );
    }

    #[test]
    fn reattach_replays_receive_ack_but_never_duplicates_outstanding_sink_delivery() {
        let issuer = LegIssuer::new(0x11);
        let leg2 = issuer.issue(2);
        let mut model = SessionModel::new(SessionRole::Owner, config(), leg2);
        let flow_id = SessionFlowId::new(1).unwrap();
        peer_opened(&mut model, leg2, flow_id, target(443));
        let delivered = model
            .reduce(SessionEvent::PeerFrame {
                leg: leg2,
                frame: frame(
                    leg2,
                    Record::Data {
                        flow_id,
                        direction: Direction::ClientToTarget,
                        offset: ByteOffset::new(0),
                        payload: Bytes::from_static(b"abc"),
                    },
                ),
            })
            .unwrap();
        let (offer, _) = only_offer(&delivered);
        model.reduce(SessionEvent::LegLost { leg: leg2 }).unwrap();

        let leg3 = issuer.issue(3);
        let replay = model
            .reduce(SessionEvent::ReplacementAttached { leg: leg3 })
            .unwrap();
        assert!(
            !replay
                .iter()
                .any(|effect| matches!(effect, SessionEffect::OfferToSink { .. }))
        );
        assert!(transmitted_records(&replay).iter().any(|record| matches!(
            record,
            Record::Ack {
                next_accepted,
                final_accepted: false,
                ..
            } if *next_accepted == ByteOffset::new(0)
        )));

        let accepted = model
            .reduce(SessionEvent::SinkAccepted { offer, bytes: 3 })
            .unwrap();
        assert!(transmitted_records(&accepted).iter().any(|record| matches!(
            record,
            Record::Ack { next_accepted, .. } if *next_accepted == ByteOffset::new(3)
        )));
        assert_eq!(model.snapshot().outstanding_sink_offers(), 0);
    }

    #[test]
    fn legless_state_retains_bounded_local_data_and_expiry_releases_everything() {
        let issuer = LegIssuer::new(0x11);
        let leg2 = issuer.issue(2);
        let mut model = SessionModel::new(SessionRole::Client, config(), leg2);
        let flow_id = local_open(&mut model, leg2, target(443));
        let local = local_flow(&model, flow_id);
        model.reduce(SessionEvent::LegLost { leg: leg2 }).unwrap();
        let effects = model
            .reduce(SessionEvent::LocalData {
                flow: local,
                payload: Bytes::from_static(b"queued"),
            })
            .unwrap();
        assert!(effects.is_empty());
        assert_eq!(
            model
                .snapshot()
                .replay_usage(Direction::ClientToTarget)
                .bytes(),
            6
        );

        let expired = model
            .reduce(SessionEvent::ResumeGraceExpired { leg: leg2 })
            .unwrap();
        assert_eq!(expired, vec![SessionEffect::SessionExpired]);
        assert_eq!(model.snapshot().phase(), SessionPhase::Expired);
        assert_eq!(model.snapshot().flow_count(), 0);
        assert_eq!(
            model.snapshot().replay_usage(Direction::ClientToTarget),
            ReplayBudgetUsage::default()
        );
        let leg3 = issuer.issue(3);
        assert_eq!(
            model.reduce(SessionEvent::ReplacementAttached { leg: leg3 }),
            Err(SessionError::SessionExpired)
        );
    }

    #[test]
    fn config_exposes_finite_derived_receive_ownership_bound() {
        let receive = TcpWindowLimits::new(1024, 7).unwrap();
        let config = SessionConfig::new(
            5,
            8,
            TcpWindowLimits::new(1024, 7).unwrap(),
            receive,
            ReplayBudgetLimits::new(4096, 32).unwrap(),
            ReplayBudgetLimits::new(4096, 32).unwrap(),
            ReceiveBudgetLimits::new(2048, 9).unwrap(),
            512,
        )
        .unwrap();

        assert_eq!(config.max_receive_owned_bytes(), 2048);
        assert_eq!(config.max_receive_ranges(), 9);
        assert_eq!(config.max_receive_normalized_backing_bytes(), 2048 * 4);
        assert_eq!(
            SessionConfig::new(
                1,
                1,
                TcpWindowLimits::new(1, 1).unwrap(),
                TcpWindowLimits::new(2, 1).unwrap(),
                ReplayBudgetLimits::new(1, 1).unwrap(),
                ReplayBudgetLimits::new(1, 1).unwrap(),
                ReceiveBudgetLimits::new(usize::MAX, 1).unwrap(),
                1,
            ),
            Err(SessionConfigError::ReceiveBackingBoundOverflow)
        );
    }

    #[test]
    fn connect_result_and_local_source_survive_replacement_while_data_waits_for_open() {
        let issuer = LegIssuer::new(0x21);
        let leg2 = issuer.issue(2);
        let mut model = SessionModel::new(SessionRole::Owner, config(), leg2);
        let flow_id = SessionFlowId::new(1).unwrap();
        let requested = peer_open(&mut model, leg2, flow_id, target(443));
        let (request, local) = only_peer_open_request(&requested);

        let buffered = model
            .reduce(SessionEvent::PeerFrame {
                leg: leg2,
                frame: frame(
                    leg2,
                    Record::Data {
                        flow_id,
                        direction: Direction::ClientToTarget,
                        offset: ByteOffset::new(0),
                        payload: Bytes::from_static(b"abc"),
                    },
                ),
            })
            .unwrap();
        assert!(buffered.is_empty(), "DATA must wait for Target OPEN");
        assert_eq!(model.snapshot().receive_owned_bytes(), 3);

        model.reduce(SessionEvent::LegLost { leg: leg2 }).unwrap();
        let leg3 = issuer.issue(3);
        let recovery = model
            .reduce(SessionEvent::ReplacementAttached { leg: leg3 })
            .unwrap();
        assert!(
            !recovery
                .iter()
                .any(|effect| matches!(effect, SessionEffect::OfferToSink { .. }))
        );

        let opened = model
            .reduce(SessionEvent::PeerOpenResolved {
                request,
                result: OpenResultCode::Opened,
            })
            .unwrap();
        assert!(opened.iter().any(|effect| matches!(
            effect,
            SessionEffect::Transmit(frame)
                if frame.leg_generation() == leg3.generation()
                    && matches!(frame.record(), Record::OpenResult { result: OpenResultCode::Opened, .. })
        )));
        let (offer, _) = opened
            .iter()
            .find_map(|effect| match effect {
                SessionEffect::OfferToSink { offer, segments } => Some((*offer, segments)),
                _ => None,
            })
            .expect("successful Target OPEN must release buffered DATA to the sink");

        let outbound = model
            .reduce(SessionEvent::LocalData {
                flow: local,
                payload: Bytes::from_static(b"reply"),
            })
            .unwrap();
        assert!(transmitted_records(&outbound).iter().any(|record| matches!(
            record,
            Record::Data {
                direction: Direction::TargetToClient,
                payload,
                ..
            } if payload.as_ref() == b"reply"
        )));
        model
            .reduce(SessionEvent::SinkAccepted { offer, bytes: 3 })
            .unwrap();

        let failed_id = SessionFlowId::new(2).unwrap();
        let failed_request = peer_open(&mut model, leg3, failed_id, target(8443));
        let (failed_request, _) = only_peer_open_request(&failed_request);
        assert!(
            model
                .reduce(SessionEvent::PeerFrame {
                    leg: leg3,
                    frame: frame(
                        leg3,
                        Record::Data {
                            flow_id: failed_id,
                            direction: Direction::ClientToTarget,
                            offset: ByteOffset::new(0),
                            payload: Bytes::from_static(b"held"),
                        },
                    ),
                })
                .unwrap()
                .is_empty()
        );
        let failed = model
            .reduce(SessionEvent::PeerOpenResolved {
                request: failed_request,
                result: OpenResultCode::TargetRefused,
            })
            .unwrap();
        assert!(
            !failed
                .iter()
                .any(|effect| matches!(effect, SessionEffect::OfferToSink { .. }))
        );
        assert!(failed.iter().any(|effect| matches!(
            effect,
            SessionEffect::FlowFinished {
                flow_id,
                reason: FlowFinishReason::OpenFailed(OpenResultCode::TargetRefused),
                ..
            } if *flow_id == failed_id
        )));
        assert!(model.flow_snapshot(failed_id).is_none());
        assert_eq!(model.snapshot().receive_owned_bytes(), 0);
    }

    #[test]
    fn pre_open_close_waits_for_target_and_opened_activates_half_close_exactly_once() {
        let issuer = LegIssuer::new(0x2b);
        let leg = issuer.issue(2);
        let mut model = SessionModel::new(SessionRole::Owner, config(), leg);
        let opened_id = SessionFlowId::new(1).unwrap();
        let requested = peer_open(&mut model, leg, opened_id, target(443));
        let (request, _) = only_peer_open_request(&requested);

        let early_close = model
            .reduce(SessionEvent::PeerFrame {
                leg,
                frame: frame(
                    leg,
                    Record::Close {
                        flow_id: opened_id,
                        direction: Direction::ClientToTarget,
                        final_offset: ByteOffset::new(0),
                    },
                ),
            })
            .unwrap();
        assert!(
            !early_close
                .iter()
                .any(|effect| matches!(effect, SessionEffect::HalfCloseSink { .. }))
        );

        let opened = model
            .reduce(SessionEvent::PeerOpenResolved {
                request,
                result: OpenResultCode::Opened,
            })
            .unwrap();
        assert_eq!(
            opened
                .iter()
                .filter(|effect| matches!(effect, SessionEffect::HalfCloseSink { .. }))
                .count(),
            1
        );
        let duplicate = model
            .reduce(SessionEvent::PeerFrame {
                leg,
                frame: frame(
                    leg,
                    Record::Close {
                        flow_id: opened_id,
                        direction: Direction::ClientToTarget,
                        final_offset: ByteOffset::new(0),
                    },
                ),
            })
            .unwrap();
        assert!(
            !duplicate
                .iter()
                .any(|effect| matches!(effect, SessionEffect::HalfCloseSink { .. }))
        );

        let rejected_id = SessionFlowId::new(2).unwrap();
        let rejected = peer_open(&mut model, leg, rejected_id, target(8443));
        let (request, _) = only_peer_open_request(&rejected);
        let early_close = model
            .reduce(SessionEvent::PeerFrame {
                leg,
                frame: frame(
                    leg,
                    Record::Close {
                        flow_id: rejected_id,
                        direction: Direction::ClientToTarget,
                        final_offset: ByteOffset::new(0),
                    },
                ),
            })
            .unwrap();
        assert!(
            !early_close
                .iter()
                .any(|effect| matches!(effect, SessionEffect::HalfCloseSink { .. }))
        );
        let rejected = model
            .reduce(SessionEvent::PeerOpenResolved {
                request,
                result: OpenResultCode::TargetRefused,
            })
            .unwrap();
        assert!(
            !rejected
                .iter()
                .any(|effect| matches!(effect, SessionEffect::HalfCloseSink { .. }))
        );
        assert!(model.flow_snapshot(rejected_id).is_none());
    }

    #[test]
    fn owner_local_source_events_require_successful_target_open() {
        let issuer = LegIssuer::new(0x2c);
        let leg = issuer.issue(2);
        let mut model = SessionModel::new(SessionRole::Owner, config(), leg);
        let flow_id = SessionFlowId::new(1).unwrap();
        let requested = peer_open(&mut model, leg, flow_id, target(443));
        let (request, local) = only_peer_open_request(&requested);
        let before = model.snapshot();
        assert_eq!(
            model.reduce(SessionEvent::LocalData {
                flow: local,
                payload: Bytes::from_static(b"early"),
            }),
            Err(SessionError::LocalSourceNotOpen { flow_id })
        );
        assert_eq!(
            model.reduce(SessionEvent::LocalClose { flow: local }),
            Err(SessionError::LocalSourceNotOpen { flow_id })
        );
        assert_eq!(model.snapshot(), before);

        model
            .reduce(SessionEvent::PeerOpenResolved {
                request,
                result: OpenResultCode::Opened,
            })
            .unwrap();
        assert!(
            model
                .reduce(SessionEvent::LocalData {
                    flow: local,
                    payload: Bytes::from_static(b"ready"),
                })
                .is_ok()
        );
        assert!(
            model
                .reduce(SessionEvent::LocalClose { flow: local })
                .is_ok()
        );

        let rejected_id = SessionFlowId::new(2).unwrap();
        let requested = peer_open(&mut model, leg, rejected_id, target(8443));
        let (request, rejected_local) = only_peer_open_request(&requested);
        model
            .reduce(SessionEvent::PeerOpenResolved {
                request,
                result: OpenResultCode::TargetRefused,
            })
            .unwrap();
        assert!(matches!(
            model.reduce(SessionEvent::LocalData {
                flow: rejected_local,
                payload: Bytes::from_static(b"late"),
            }),
            Err(SessionError::RetiredFlow(id)) if id == rejected_id
        ));
    }

    #[test]
    fn open_activation_exhaustion_fails_before_recording_success() {
        let issuer = LegIssuer::new(0x2e);
        let leg = issuer.issue(2);
        let mut model = SessionModel::new(SessionRole::Owner, config(), leg);
        let data_id = SessionFlowId::new(1).unwrap();
        let requested = peer_open(&mut model, leg, data_id, target(443));
        let (data_request, _) = only_peer_open_request(&requested);
        model
            .reduce(SessionEvent::PeerFrame {
                leg,
                frame: frame(
                    leg,
                    Record::Data {
                        flow_id: data_id,
                        direction: Direction::ClientToTarget,
                        offset: ByteOffset::new(0),
                        payload: Bytes::from_static(b"held"),
                    },
                ),
            })
            .unwrap();
        model.next_sink_offer_id = None;
        let before = model.snapshot();
        assert_eq!(
            model.reduce(SessionEvent::PeerOpenResolved {
                request: data_request,
                result: OpenResultCode::Opened,
            }),
            Err(SessionError::SinkOfferIdExhausted)
        );
        assert_eq!(model.snapshot(), before);
        assert_eq!(model.flows[&data_id].peer_open_result, None);
        model.next_sink_offer_id = Some(1);

        let close_id = SessionFlowId::new(2).unwrap();
        let requested = peer_open(&mut model, leg, close_id, target(8443));
        let (close_request, _) = only_peer_open_request(&requested);
        model
            .reduce(SessionEvent::PeerFrame {
                leg,
                frame: frame(
                    leg,
                    Record::Close {
                        flow_id: close_id,
                        direction: Direction::ClientToTarget,
                        final_offset: ByteOffset::new(0),
                    },
                ),
            })
            .unwrap();
        model.next_half_close_id = None;
        let before = model.snapshot();
        assert_eq!(
            model.reduce(SessionEvent::PeerOpenResolved {
                request: close_request,
                result: OpenResultCode::Opened,
            }),
            Err(SessionError::HalfCloseCompletionIdExhausted)
        );
        assert_eq!(model.snapshot(), before);
        assert_eq!(model.flows[&close_id].peer_open_result, None);
    }

    #[test]
    fn global_receive_budget_is_transactional_across_flows_and_releases_on_accept_or_abandon() {
        let issuer = LegIssuer::new(0x22);
        let leg = issuer.issue(2);
        let limits = config_with_limits(2, 4, 64, 8, 6, 4);
        let mut model = SessionModel::new(SessionRole::Owner, limits, leg);
        let flow1 = SessionFlowId::new(1).unwrap();
        let flow2 = SessionFlowId::new(2).unwrap();
        peer_opened(&mut model, leg, flow1, target(443));
        peer_opened(&mut model, leg, flow2, target(8443));

        let first = model
            .reduce(SessionEvent::PeerFrame {
                leg,
                frame: frame(
                    leg,
                    Record::Data {
                        flow_id: flow1,
                        direction: Direction::ClientToTarget,
                        offset: ByteOffset::new(0),
                        payload: Bytes::from_static(b"abcd"),
                    },
                ),
            })
            .unwrap();
        let (offer1, _) = only_offer(&first);
        let before_reject = model.snapshot();
        assert_eq!(
            model.reduce(SessionEvent::PeerFrame {
                leg,
                frame: frame(
                    leg,
                    Record::Data {
                        flow_id: flow2,
                        direction: Direction::ClientToTarget,
                        offset: ByteOffset::new(0),
                        payload: Bytes::from_static(b"wxyz"),
                    },
                ),
            }),
            Err(SessionError::ReceiveByteBudgetExceeded {
                attempted: 8,
                max: 6,
            })
        );
        assert_eq!(model.snapshot(), before_reject);
        assert_eq!(
            model
                .flow_snapshot(flow2)
                .unwrap()
                .receive()
                .buffered_bytes(),
            0
        );

        let accepted = model
            .reduce(SessionEvent::SinkAccepted {
                offer: offer1,
                bytes: 2,
            })
            .unwrap();
        let offer1_remainder = accepted
            .iter()
            .find_map(|effect| match effect {
                SessionEffect::OfferToSink { offer, .. } => Some(*offer),
                _ => None,
            })
            .unwrap();
        assert_eq!(model.snapshot().receive_owned_bytes(), 2);

        let second = model
            .reduce(SessionEvent::PeerFrame {
                leg,
                frame: frame(
                    leg,
                    Record::Data {
                        flow_id: flow2,
                        direction: Direction::ClientToTarget,
                        offset: ByteOffset::new(0),
                        payload: Bytes::from_static(b"wxyz"),
                    },
                ),
            })
            .unwrap();
        let (offer2, _) = only_offer(&second);
        assert_eq!(model.snapshot().receive_owned_bytes(), 6);
        model
            .reduce(SessionEvent::SinkAbandoned { offer: offer2 })
            .unwrap();
        assert_eq!(model.snapshot().receive_owned_bytes(), 2);
        model
            .reduce(SessionEvent::SinkAbandoned {
                offer: offer1_remainder,
            })
            .unwrap();
        assert_eq!(model.snapshot().receive_owned_bytes(), 0);
    }

    #[test]
    fn sink_abandon_and_half_close_failure_remain_valid_across_replacement_without_final_ack() {
        let issuer = LegIssuer::new(0x23);
        let leg2 = issuer.issue(2);
        let mut model = SessionModel::new(SessionRole::Owner, config(), leg2);
        let flow1 = SessionFlowId::new(1).unwrap();
        peer_opened(&mut model, leg2, flow1, target(443));
        let offered = model
            .reduce(SessionEvent::PeerFrame {
                leg: leg2,
                frame: frame(
                    leg2,
                    Record::Data {
                        flow_id: flow1,
                        direction: Direction::ClientToTarget,
                        offset: ByteOffset::new(0),
                        payload: Bytes::from_static(b"owned"),
                    },
                ),
            })
            .unwrap();
        let (offer, _) = only_offer(&offered);
        model.reduce(SessionEvent::LegLost { leg: leg2 }).unwrap();
        let leg3 = issuer.issue(3);
        model
            .reduce(SessionEvent::ReplacementAttached { leg: leg3 })
            .unwrap();
        let abandoned = model.reduce(SessionEvent::SinkAbandoned { offer }).unwrap();
        assert!(
            transmitted_records(&abandoned)
                .iter()
                .any(|record| matches!(
                    record,
                    Record::Reset {
                        reason: ResetReason::LocalAbandon,
                        ..
                    }
                ))
        );
        assert!(
            !transmitted_records(&abandoned)
                .iter()
                .any(|record| matches!(record, Record::Ack { .. }))
        );

        let flow2 = SessionFlowId::new(2).unwrap();
        peer_opened(&mut model, leg3, flow2, target(8443));
        let close = model
            .reduce(SessionEvent::PeerFrame {
                leg: leg3,
                frame: frame(
                    leg3,
                    Record::Close {
                        flow_id: flow2,
                        direction: Direction::ClientToTarget,
                        final_offset: ByteOffset::new(0),
                    },
                ),
            })
            .unwrap();
        let completion = close
            .iter()
            .find_map(|effect| match effect {
                SessionEffect::HalfCloseSink { completion } => Some(*completion),
                _ => None,
            })
            .unwrap();
        let failed = model
            .reduce(SessionEvent::SinkHalfCloseFailed { completion })
            .unwrap();
        assert!(
            transmitted_records(&failed)
                .iter()
                .any(|record| matches!(record, Record::Reset { .. }))
        );
        assert!(!transmitted_records(&failed).iter().any(|record| matches!(
            record,
            Record::Ack {
                final_accepted: true,
                ..
            }
        )));
    }

    #[test]
    fn sequential_graceful_flows_reclaim_live_capacity_and_replay_lost_terminal_ack() {
        let issuer = LegIssuer::new(0x24);
        let mut leg = issuer.issue(2);
        let limits = config_with_limits(1, 1, 64, 8, 64, 8);
        let mut model = SessionModel::new(SessionRole::Client, limits, leg);
        let mut first_flow = None;

        for expected in 1..=3 {
            let flow_id = local_open(&mut model, leg, target(4000 + expected as u16));
            assert_eq!(flow_id.get(), expected);
            first_flow.get_or_insert(flow_id);
            let local = local_flow(&model, flow_id);
            model
                .reduce(SessionEvent::PeerFrame {
                    leg,
                    frame: frame(
                        leg,
                        Record::OpenResult {
                            flow_id,
                            result: OpenResultCode::Opened,
                        },
                    ),
                })
                .unwrap();
            model
                .reduce(SessionEvent::LocalClose { flow: local })
                .unwrap();
            let peer_close = model
                .reduce(SessionEvent::PeerFrame {
                    leg,
                    frame: frame(
                        leg,
                        Record::Close {
                            flow_id,
                            direction: Direction::TargetToClient,
                            final_offset: ByteOffset::new(0),
                        },
                    ),
                })
                .unwrap();
            let completion = peer_close
                .iter()
                .find_map(|effect| match effect {
                    SessionEffect::HalfCloseSink { completion } => Some(*completion),
                    _ => None,
                })
                .unwrap();
            model
                .reduce(SessionEvent::SinkHalfClosed { completion })
                .unwrap();
            let finished = model
                .reduce(SessionEvent::PeerFrame {
                    leg,
                    frame: frame(
                        leg,
                        Record::Ack {
                            flow_id,
                            direction: Direction::ClientToTarget,
                            next_accepted: ByteOffset::new(0),
                            final_accepted: true,
                        },
                    ),
                })
                .unwrap();
            let terminal = finished
                .iter()
                .find_map(|effect| match effect {
                    SessionEffect::FlowFinished {
                        reason: FlowFinishReason::Graceful,
                        terminal,
                        ..
                    } => Some(*terminal),
                    _ => None,
                })
                .expect("two accepted FINs must reclaim live flow ownership");
            assert_eq!(model.snapshot().flow_count(), 0);

            if expected == 1 {
                model.reduce(SessionEvent::LegLost { leg }).unwrap();
                let replacement = issuer.issue(3);
                let replay = model
                    .reduce(SessionEvent::ReplacementAttached { leg: replacement })
                    .unwrap();
                assert!(transmitted_records(&replay).iter().any(|record| matches!(
                    record,
                    Record::Ack {
                        flow_id: replayed,
                        direction: Direction::TargetToClient,
                        next_accepted,
                        final_accepted: true,
                    } if *replayed == flow_id && *next_accepted == ByteOffset::new(0)
                )));
                leg = replacement;
            }
            model
                .reduce(SessionEvent::TerminalGraceExpired { terminal })
                .unwrap();
            assert_eq!(model.snapshot().terminal_tombstones(), 0);
        }

        let old = first_flow.unwrap();
        let before = model.snapshot();
        assert!(matches!(
            model.reduce(SessionEvent::PeerFrame {
                leg,
                frame: frame(
                    leg,
                    Record::Data {
                        flow_id: old,
                        direction: Direction::TargetToClient,
                        offset: ByteOffset::new(0),
                        payload: Bytes::from_static(b"late"),
                    },
                ),
            }),
            Err(SessionError::RetiredFlow(id)) if id == old
        ));
        assert_eq!(model.snapshot(), before);
    }

    #[test]
    fn repeated_failed_peer_opens_do_not_consume_lifetime_flow_capacity() {
        let issuer = LegIssuer::new(0x25);
        let leg = issuer.issue(2);
        let limits = config_with_limits(1, 1, 64, 8, 64, 8);
        let mut model = SessionModel::new(SessionRole::Owner, limits, leg);

        for raw in 1..=3 {
            let flow_id = SessionFlowId::new(raw).unwrap();
            let requested = peer_open(&mut model, leg, flow_id, target(5000 + raw as u16));
            let (request, _) = only_peer_open_request(&requested);
            let failed = model
                .reduce(SessionEvent::PeerOpenResolved {
                    request,
                    result: OpenResultCode::TargetUnreachable,
                })
                .unwrap();
            let terminal = failed
                .iter()
                .find_map(|effect| match effect {
                    SessionEffect::FlowFinished {
                        reason: FlowFinishReason::OpenFailed(OpenResultCode::TargetUnreachable),
                        terminal,
                        ..
                    } => Some(*terminal),
                    _ => None,
                })
                .unwrap();
            assert_eq!(model.snapshot().flow_count(), 0);
            model
                .reduce(SessionEvent::TerminalGraceExpired { terminal })
                .unwrap();
        }
        assert_eq!(model.snapshot().highest_peer_flow_id(), 3);
    }

    #[test]
    fn terminal_overlap_replays_compact_control_without_resurrection_or_redelivery() {
        let issuer = LegIssuer::new(0x2d);
        let leg = issuer.issue(2);
        let mut model = SessionModel::new(SessionRole::Owner, config(), leg);
        let flow_id = SessionFlowId::new(1).unwrap();
        let requested = peer_open(&mut model, leg, flow_id, target(443));
        let (request, _) = only_peer_open_request(&requested);
        model
            .reduce(SessionEvent::PeerOpenResolved {
                request,
                result: OpenResultCode::TargetRefused,
            })
            .unwrap();
        let terminal = model.snapshot();
        assert_eq!(terminal.flow_count(), 0);
        assert_eq!(terminal.terminal_tombstones(), 1);

        let duplicate_records = [
            Record::Open {
                flow_id,
                target: target(443),
            },
            Record::Data {
                flow_id,
                direction: Direction::ClientToTarget,
                offset: ByteOffset::new(0),
                payload: Bytes::from_static(b"stale"),
            },
            Record::Close {
                flow_id,
                direction: Direction::ClientToTarget,
                final_offset: ByteOffset::new(0),
            },
            Record::Ack {
                flow_id,
                direction: Direction::TargetToClient,
                next_accepted: ByteOffset::new(0),
                final_accepted: false,
            },
            Record::Reset {
                flow_id,
                reason: ResetReason::Unspecified,
            },
        ];
        for record in duplicate_records {
            let replay = model
                .reduce(SessionEvent::PeerFrame {
                    leg,
                    frame: frame(leg, record),
                })
                .unwrap();
            assert!(matches!(
                transmitted_records(&replay).as_slice(),
                [Record::OpenResult {
                    flow_id: replayed,
                    result: OpenResultCode::TargetRefused,
                }] if *replayed == flow_id
            ));
            assert_eq!(model.snapshot(), terminal);
        }

        assert_eq!(
            model.reduce(SessionEvent::PeerFrame {
                leg,
                frame: frame(
                    leg,
                    Record::Open {
                        flow_id,
                        target: target(8443),
                    },
                ),
            }),
            Err(SessionError::FlowTargetConflict { flow_id })
        );
        assert_eq!(model.snapshot(), terminal);

        let ignored_id = SessionFlowId::new(2).unwrap();
        peer_opened(&mut model, leg, ignored_id, target(9443));
        model
            .reduce(SessionEvent::PeerFrame {
                leg,
                frame: frame(
                    leg,
                    Record::Reset {
                        flow_id: ignored_id,
                        reason: ResetReason::TargetFailure,
                    },
                ),
            })
            .unwrap();
        let ignored_terminal = model.snapshot();
        assert!(
            model
                .reduce(SessionEvent::PeerFrame {
                    leg,
                    frame: frame(
                        leg,
                        Record::Data {
                            flow_id: ignored_id,
                            direction: Direction::ClientToTarget,
                            offset: ByteOffset::new(0),
                            payload: Bytes::from_static(b"late"),
                        },
                    ),
                })
                .unwrap()
                .is_empty()
        );
        assert_eq!(model.snapshot(), ignored_terminal);
    }

    struct DropOwner {
        bytes: Vec<u8>,
        dropped: Arc<AtomicBool>,
    }

    impl AsRef<[u8]> for DropOwner {
        fn as_ref(&self) -> &[u8] {
            &self.bytes
        }
    }

    impl Drop for DropOwner {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }

    #[test]
    fn transmit_effect_uses_normalized_replay_storage_and_releases_caller_backing() {
        let issuer = LegIssuer::new(0x26);
        let leg2 = issuer.issue(2);
        let mut model = SessionModel::new(SessionRole::Client, config(), leg2);
        let flow_id = local_open(&mut model, leg2, target(443));
        let local = local_flow(&model, flow_id);
        let dropped = Arc::new(AtomicBool::new(false));
        let backing = Bytes::from_owner(DropOwner {
            bytes: vec![0x5a; 4096],
            dropped: Arc::clone(&dropped),
        });
        let caller_slice = backing.slice(100..104);
        let caller_ptr = caller_slice.as_ptr();
        drop(backing);
        assert!(!dropped.load(Ordering::SeqCst));

        let initial = model
            .reduce(SessionEvent::LocalData {
                flow: local,
                payload: caller_slice,
            })
            .unwrap();
        assert!(dropped.load(Ordering::SeqCst));
        let stored_ptr = transmitted_records(&initial)
            .iter()
            .find_map(|record| match record {
                Record::Data { payload, .. } => Some(payload.as_ptr()),
                _ => None,
            })
            .unwrap();
        assert_ne!(stored_ptr, caller_ptr);

        model.reduce(SessionEvent::LegLost { leg: leg2 }).unwrap();
        let leg3 = issuer.issue(3);
        let replay = model
            .reduce(SessionEvent::ReplacementAttached { leg: leg3 })
            .unwrap();
        let replay_ptr = transmitted_records(&replay)
            .iter()
            .find_map(|record| match record {
                Record::Data { payload, .. } => Some(payload.as_ptr()),
                _ => None,
            })
            .unwrap();
        assert_eq!(stored_ptr, replay_ptr);
    }

    #[test]
    fn opaque_tokens_reject_cross_session_use_without_mutation() {
        let issuer_a = LegIssuer::new(0x27);
        let issuer_b = LegIssuer::new(0x28);
        let leg_a = issuer_a.issue(2);
        let leg_b = issuer_b.issue(2);
        let mut a = SessionModel::new(SessionRole::Owner, config(), leg_a);
        let mut b = SessionModel::new(SessionRole::Owner, config(), leg_b);
        let flow_id = SessionFlowId::new(1).unwrap();
        let requested_a = peer_open(&mut a, leg_a, flow_id, target(443));
        let (request_a, local_a) = only_peer_open_request(&requested_a);
        peer_opened(&mut b, leg_b, flow_id, target(443));
        let before = b.snapshot();

        assert_eq!(
            b.reduce(SessionEvent::PeerOpenResolved {
                request: request_a,
                result: OpenResultCode::Opened,
            }),
            Err(SessionError::CrossSessionOpenRequest)
        );
        assert_eq!(
            b.reduce(SessionEvent::LocalData {
                flow: local_a,
                payload: Bytes::from_static(b"x"),
            }),
            Err(SessionError::CrossSessionLocalFlow)
        );
        assert_eq!(b.snapshot(), before);

        a.reduce(SessionEvent::PeerOpenResolved {
            request: request_a,
            result: OpenResultCode::Opened,
        })
        .unwrap();
        let offered_a = a
            .reduce(SessionEvent::PeerFrame {
                leg: leg_a,
                frame: frame(
                    leg_a,
                    Record::Data {
                        flow_id,
                        direction: Direction::ClientToTarget,
                        offset: ByteOffset::new(0),
                        payload: Bytes::from_static(b"x"),
                    },
                ),
            })
            .unwrap();
        let (offer_a, _) = only_offer(&offered_a);
        assert_eq!(
            b.reduce(SessionEvent::SinkAbandoned { offer: offer_a }),
            Err(SessionError::CrossSessionSinkOffer)
        );
        assert_eq!(b.snapshot(), before);
    }

    #[test]
    fn debug_output_redacts_targets_and_payloads_at_event_and_effect_boundaries() {
        let issuer = LegIssuer::new(0x29);
        let leg = issuer.issue(2);
        let secret_host = "secret-internal.example";
        let target = TargetAddr::DomainPort {
            host: secret_host.to_owned(),
            port: 443,
        };
        let event = SessionEvent::LocalOpen {
            leg,
            target: target.clone(),
        };
        assert!(!format!("{event:?}").contains(secret_host));

        let mut model = SessionModel::new(SessionRole::Client, config(), leg);
        let opened = model.reduce(event).unwrap();
        assert!(!format!("{opened:?}").contains(secret_host));
        let flow = opened
            .iter()
            .find_map(|effect| match effect {
                SessionEffect::LocalFlowOpened { flow } => Some(*flow),
                _ => None,
            })
            .unwrap();
        let secret_payload = "TOP_SECRET_SESSION_PAYLOAD";
        let data_event = SessionEvent::LocalData {
            flow,
            payload: Bytes::copy_from_slice(secret_payload.as_bytes()),
        };
        assert!(!format!("{data_event:?}").contains(secret_payload));
        let effects = model.reduce(data_event).unwrap();
        assert!(!format!("{effects:?}").contains(secret_payload));
    }

    #[test]
    fn local_open_rejects_unbounded_target_before_flow_mutation() {
        let issuer = LegIssuer::new(0x2a);
        let leg = issuer.issue(2);
        let mut model = SessionModel::new(SessionRole::Client, config(), leg);
        let before = model.snapshot();
        let target = TargetAddr::DomainPort {
            host: "x".repeat(254),
            port: 443,
        };

        assert!(matches!(
            model.reduce(SessionEvent::LocalOpen { leg, target }),
            Err(SessionError::InvalidProtocolRecord {
                source: ProtocolError::InvalidDomainLength { len: 254, .. }
            })
        ));
        assert_eq!(model.snapshot(), before);
        assert_eq!(model.snapshot().flow_count(), 0);
    }

    #[test]
    fn local_open_normalizes_domain_backing_in_transmit_and_recovery_state() {
        let issuer = LegIssuer::new(0x2c);
        let leg2 = issuer.issue(2);
        let mut model = SessionModel::new(SessionRole::Client, config(), leg2);
        let mut host = String::with_capacity(1024 * 1024);
        host.push_str("small.example");
        assert!(host.capacity() > host.len());

        let opened = model
            .reduce(SessionEvent::LocalOpen {
                leg: leg2,
                target: TargetAddr::DomainPort { host, port: 443 },
            })
            .unwrap();
        let transmitted_capacity = opened
            .iter()
            .find_map(|effect| match effect {
                SessionEffect::Transmit(frame) => match frame.record() {
                    Record::Open {
                        target: TargetAddr::DomainPort { host, .. },
                        ..
                    } => Some((host.len(), host.capacity())),
                    _ => None,
                },
                _ => None,
            })
            .unwrap();
        assert_eq!(transmitted_capacity.1, transmitted_capacity.0);

        model.reduce(SessionEvent::LegLost { leg: leg2 }).unwrap();
        let recovered = model
            .reduce(SessionEvent::ReplacementAttached {
                leg: issuer.issue(3),
            })
            .unwrap();
        let recovered_capacity = recovered
            .iter()
            .find_map(|effect| match effect {
                SessionEffect::Transmit(frame) => match frame.record() {
                    Record::Open {
                        target: TargetAddr::DomainPort { host, .. },
                        ..
                    } => Some((host.len(), host.capacity())),
                    _ => None,
                },
                _ => None,
            })
            .unwrap();
        assert_eq!(recovered_capacity.1, recovered_capacity.0);
    }

    #[test]
    fn peer_frame_rejects_invalid_record_before_flow_lookup_or_mutation() {
        let issuer = LegIssuer::new(0x2b);
        let leg = issuer.issue(2);
        let mut model = SessionModel::new(SessionRole::Owner, config(), leg);
        let before = model.snapshot();
        let flow_id = SessionFlowId::new(1).unwrap();
        let invalid = Frame::new(
            leg.generation(),
            Record::Data {
                flow_id,
                direction: Direction::ClientToTarget,
                offset: ByteOffset::new(0),
                payload: Bytes::new(),
            },
        );

        assert_eq!(
            model.reduce(SessionEvent::PeerFrame {
                leg,
                frame: invalid,
            }),
            Err(SessionError::InvalidProtocolRecord {
                source: ProtocolError::EmptyData,
            })
        );
        assert_eq!(model.snapshot(), before);
    }

    #[test]
    fn auth_layer_records_are_rejected_without_session_mutation() {
        let issuer = LegIssuer::new(0x2a);
        let leg = issuer.issue(2);
        let mut model = SessionModel::new(SessionRole::Client, config(), leg);
        let before = model.snapshot();
        assert_eq!(
            model.reduce(SessionEvent::PeerFrame {
                leg,
                frame: frame(
                    leg,
                    Record::AttachGenerationStatus {
                        session_id: issuer.session_id,
                        requested_generation: leg.generation(),
                        nonce: AttachNonce::new([0x44; 16]).unwrap(),
                    },
                ),
            }),
            Err(SessionError::UnexpectedAttachRecord)
        );
        assert_eq!(model.snapshot(), before);
    }

    #[test]
    fn storage_plan_bridge_configures_exact_100_170_and_240_mbit_limits_for_each_role() {
        let geometry = ReplayStorageGeometry::new(16 * 1024, 32).unwrap();
        for rate in [100_000_000, 170_000_000, 240_000_000] {
            let rate = BitsPerSecond::new(rate).unwrap();
            let rates = DirectionalRates::symmetric(rate);
            let storage = ReplayCapacitySpec::new(
                rates,
                rates,
                Duration::from_secs(1),
                Duration::from_millis(250),
            )
            .unwrap()
            .derive()
            .unwrap()
            .storage_plan(geometry)
            .unwrap();

            for role in [SessionRole::Client, SessionRole::Owner] {
                let config = SessionConfig::from_storage_plan(role, 64, storage, 64 * 1024)
                    .expect("checked storage plan must bridge without widening");
                let per_flow = storage.per_flow();
                let global = storage.global();
                let local = storage_limit(per_flow, role.local_send_direction());
                let peer = storage_limit(per_flow, role.peer_receive_direction());
                let receive_global = storage_limit(global, role.peer_receive_direction());
                assert_eq!(config.max_flows(), geometry.max_flows());
                assert_eq!(config.send_window().max_bytes(), local.max_bytes());
                assert_eq!(config.send_window().max_segments(), local.max_segments());
                assert_eq!(config.receive_window().max_bytes(), peer.max_bytes());
                assert_eq!(config.receive_window().max_segments(), peer.max_segments());
                assert_eq!(
                    config.replay_limit(Direction::ClientToTarget).max_bytes(),
                    global.client_to_target().max_bytes()
                );
                assert_eq!(
                    config
                        .replay_limit(Direction::TargetToClient)
                        .max_segments(),
                    global.target_to_client().max_segments()
                );
                assert_eq!(
                    config.receive_budget().max_bytes(),
                    receive_global.max_bytes()
                );
                assert_eq!(
                    config.receive_budget().max_ranges(),
                    receive_global.max_segments()
                );
            }
        }
    }

    fn check_receive_reservation_trace(operations: &[(usize, usize)]) {
        const CORPUS: &[u8] = b"0123456789abcdef";
        let limits = TcpWindowLimits::new(CORPUS.len(), 16).unwrap();
        let mut tcp = TcpReceiveWindow::new(limits);
        let mut owned = [false; CORPUS.len()];

        for &(start, len) in operations {
            let offset = ByteOffset::new(start as u64);
            let payload = Bytes::copy_from_slice(&CORPUS[start..start + len]);
            let before = tcp.snapshot();
            let reservation = tcp.preview_receive(offset, &payload).unwrap();
            let expected_bytes = owned[start..start + len]
                .iter()
                .filter(|is_owned| !**is_owned)
                .count();
            assert_eq!(reservation.buffered_bytes_added(), expected_bytes);
            assert_eq!(tcp.snapshot(), before);
            let expected_ranges = reservation.ranges_added();
            let received = tcp.commit_receive(reservation, offset, payload).unwrap();
            assert_eq!(received.buffered_bytes_added(), expected_bytes);
            assert_eq!(received.ranges_added(), expected_ranges);
            owned[start..start + len].fill(true);
            assert_eq!(
                tcp.buffered_bytes(),
                owned.iter().filter(|is_owned| **is_owned).count()
            );
        }

        for requested in [1, 3, 2, 5, usize::MAX] {
            let expected = tcp.preview_accept(requested).unwrap();
            let accepted = tcp.accept(requested).unwrap();
            assert_eq!(accepted.accepted(), expected.accepted());
            assert_eq!(accepted.accepted_bytes(), expected.accepted_bytes());
            assert_eq!(accepted.ranges_released(), expected.ranges_released());
        }
        assert_eq!(tcp.buffered_bytes(), 0);

        let payload = Bytes::from_static(b"0123");
        let stale = tcp.preview_receive(ByteOffset::new(0), &payload).unwrap();
        assert_eq!(stale.buffered_bytes_added(), 0);
        let received = tcp
            .commit_receive(stale, ByteOffset::new(0), payload)
            .unwrap();
        assert_eq!(received.buffered_bytes_added(), 0);
        assert_eq!(received.ranges_added(), 0);
    }

    fn permute_receive_operations(
        operations: &mut [(usize, usize)],
        index: usize,
        traces: &mut usize,
    ) {
        if index == operations.len() {
            check_receive_reservation_trace(operations);
            *traces += 1;
            return;
        }
        for candidate in index..operations.len() {
            operations.swap(index, candidate);
            permute_receive_operations(operations, index + 1, traces);
            operations.swap(index, candidate);
        }
    }

    #[test]
    fn receive_reservation_is_transactional_across_all_reorder_overlap_permutations() {
        let mut operations = [(0, 4), (4, 4), (2, 4), (8, 4), (1, 9), (12, 4)];
        let mut traces = 0;
        permute_receive_operations(&mut operations, 0, &mut traces);
        assert_eq!(traces, 720);
    }

    #[test]
    fn proof_type_debug_remains_redacted_through_session_test_helpers() {
        let proof = AttachProof::new([0x77; 32]).unwrap();
        assert_eq!(format!("{proof:?}"), "AttachProof([REDACTED])");
    }
}
