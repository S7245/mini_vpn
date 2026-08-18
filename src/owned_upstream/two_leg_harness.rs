//! Production-path deterministic two-leg harness for Knife16 Task 4.
//!
//! The harness owns one virtual scheduler for transport, supervisor, Target,
//! sink, timer, and cleanup work. Session controllers receive only the
//! byte-oriented [`EncodedLegTransport`] capability; fault decisions remain
//! private to the orchestrator.

#![cfg(test)]

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::time::Duration;

use bytes::Bytes;

use crate::owned_upstream::leg::{AttachResponse, AttachedLeg, LegBoundFrame, PendingAttach};
use crate::owned_upstream::leg_io::{
    EncodedLegTransport, EncodedLegTransportError, LegIoError, LegOutboundQueue,
};
use crate::owned_upstream::owner_target::{
    OwnerTargetConfig, OwnerTargetExecutor, OwnerTargetOutput, OwnerTargetResume,
};
use crate::owned_upstream::session::SessionOwnerCommand;
use crate::owned_upstream::supervisor::{PendingOwnerBootstrap, SessionSupervisor};
use crate::owned_upstream::target::{MemoryTarget, MemoryTargetConfig, MemoryWriteDirective};
use crate::owned_upstream::tcp::{
    FlowPortConfig, HalfCloseDelivery, ResumableTcpDriver, ResumableTcpFlow,
    ResumableTcpPortFactory, SinkDelivery, TerminalAction, TerminalDelivery, TunAction,
};
use crate::owned_upstream::two_leg::{
    ActorId, CapacityError, DeterministicScheduler, EncodedDelivery, EventBudget, EventPhase,
    EventSpec, HarnessFixedWork, HarnessObservedWork, HarnessOwnedByteCategory, HarnessWorkBudget,
    HarnessWorkCategory, HarnessWorkSpec, ScheduleError, ScheduledEvent, SimTime, WireCapacitySpec,
    WireDirection, WireError, WireLane, WireRoute,
};
use crate::resumable::{
    AttachAlpn, AttachAuthority, AttachCredentials, AttachNonce, AttachPolicy, AttachRequest,
    AttachTransportBinding, DevicePrincipal, DeviceSecret, Direction, FeatureOffer,
    FlowFinishReason, Frame, LegGeneration, OpenResultCode, OwnerIdentity, ReceiveBudgetLimits,
    Record, ReplayBudgetLimits, ResumeSecret, SESSION_PROTOCOL_VERSION, SessionConfig,
    SessionEffect, SessionEvent, SessionFlowId, SessionId, TcpWindowLimits, TerminalGrace,
    TlsExporterBinding, VersionRange,
};
use crate::shared::TargetAddr;

const MEMORY_LINK_DELAY: Duration = Duration::from_nanos(1);

enum HarnessAction {
    WireDelivery(EncodedDelivery),
    StartAttach(Frame),
    OwnerBound(LegBoundFrame),
    ClientBound(LegBoundFrame),
    StartOpen,
    SendClientData(Bytes),
    RetryOwnerWrite,
    DrainTarget,
    FeedOwnerRead(Bytes),
    ReadOwnerSource,
    PollClientAction,
    ApplyClientSink {
        delivery: SinkDelivery,
        accepted: usize,
    },
    ApplyClientHalfClose(HalfCloseDelivery),
    ConsumeClientTerminal(TerminalDelivery),
    PollClientDriver,
    CloseClientSource,
    FinishOwnerSource,
    ExpireClientTerminal(TerminalGrace),
    ExpireOwnerTerminal(TerminalGrace),
    ResumeOwnerEffects(OwnerTargetResume),
    FlushClientOutbound,
    FlushOwnerOutbound,
}

impl HarnessAction {
    const fn is_wire_delivery(&self) -> bool {
        matches!(self, Self::WireDelivery(_))
    }
}

/// One global scheduler shared by every deterministic harness actor.
///
/// The first R5 vertical exposes only encoded delivery; supervisor, Target,
/// sink, timer, and cleanup variants join this same enum rather than creating
/// nested schedulers.
struct ByteHarnessScheduler {
    scheduler: DeterministicScheduler<HarnessAction>,
    event_budget: EventBudget,
    next_send_ordinal: u64,
    reject_once_at: Option<u64>,
    work_error: Option<CapacityError>,
}

impl ByteHarnessScheduler {
    fn new(budget: EventBudget) -> Self {
        Self {
            scheduler: DeterministicScheduler::new(budget),
            event_budget: budget,
            next_send_ordinal: 0,
            reject_once_at: None,
            work_error: None,
        }
    }

    fn reject_once_at(&mut self, ordinal: u64) {
        self.reject_once_at = Some(ordinal);
    }

    fn pending_actions(&self) -> usize {
        self.scheduler.pending_events()
    }

    fn pending_bytes(&self) -> usize {
        self.scheduler.pending_bytes()
    }

    fn trace_hash(&self) -> u64 {
        self.scheduler.trace_hash()
    }

    const fn event_budget(&self) -> EventBudget {
        self.event_budget
    }

    fn controller_sender<'a>(
        &'a mut self,
        work_budget: HarnessWorkBudget,
        observed_work: &'a mut HarnessObservedWork,
    ) -> ScheduledEncodedSender<'a> {
        ScheduledEncodedSender {
            harness: self,
            work_budget,
            observed_work,
        }
    }

    fn take_work_error(&mut self) -> Option<CapacityError> {
        self.work_error.take()
    }

    fn pop_next_checked(&mut self) -> Result<Option<ScheduledEvent<HarnessAction>>, ScheduleError> {
        self.scheduler.pop_next_checked()
    }

    fn schedule(
        &mut self,
        deadline: SimTime,
        phase: EventPhase,
        actor: ActorId,
        owned_bytes: usize,
        trace_tag: u64,
        action: HarnessAction,
    ) -> Result<(), ScheduleError> {
        self.scheduler
            .schedule(EventSpec::new(
                deadline,
                phase,
                actor,
                owned_bytes,
                trace_tag,
                action,
            ))
            .map(|_| ())
    }
}

/// Controller-visible transport view. It can submit encoded bytes but cannot
/// observe scheduler order, delivery, fault outcomes, or transport counters.
struct ScheduledEncodedSender<'a> {
    harness: &'a mut ByteHarnessScheduler,
    work_budget: HarnessWorkBudget,
    observed_work: &'a mut HarnessObservedWork,
}

impl EncodedLegTransport for ScheduledEncodedSender<'_> {
    fn send_encoded(
        &mut self,
        now: SimTime,
        route: WireRoute,
        lane: WireLane,
        bytes: Vec<u8>,
    ) -> Result<(), EncodedLegTransportError> {
        let ordinal = self.harness.next_send_ordinal;
        // A rejected submission creates no wire ownership. Its retry is the
        // extra Flush* action charged to FixedActor by the global driver.
        if self.harness.reject_once_at == Some(ordinal) {
            self.harness.reject_once_at = None;
            return Err(EncodedLegTransportError::Wire(
                WireError::LiveMessageCapacityExceeded,
            ));
        }
        let next_ordinal = ordinal
            .checked_add(1)
            .ok_or(EncodedLegTransportError::SubmissionOrdinalExhausted)?;
        let owned_bytes = bytes.len();
        let delivery = EncodedDelivery::from_memory_ingress(route, lane, bytes, ordinal, 0)
            .map_err(EncodedLegTransportError::Wire)?;
        let deadline = now.checked_add(MEMORY_LINK_DELAY).map_err(|_| {
            EncodedLegTransportError::Wire(WireError::Schedule(ScheduleError::TimeWentBackwards))
        })?;
        let charged = self
            .observed_work
            .charged(self.work_budget, HarnessWorkCategory::WireSend, 1)
            .and_then(|work| work.charged(self.work_budget, HarnessWorkCategory::WireDelivery, 1))
            .and_then(|work| {
                work.charged_owned_bytes(
                    self.work_budget,
                    HarnessOwnedByteCategory::WireSend,
                    owned_bytes,
                )
            })
            .and_then(|work| {
                work.charged_owned_bytes(
                    self.work_budget,
                    HarnessOwnedByteCategory::WireDelivery,
                    owned_bytes,
                )
            });
        let charged = match charged {
            Ok(charged) => charged,
            Err(error) => {
                self.harness.work_error = Some(error);
                return Err(EncodedLegTransportError::Wire(WireError::Schedule(
                    ScheduleError::EventBudgetExceeded,
                )));
            }
        };
        self.harness
            .scheduler
            .schedule(EventSpec::new(
                deadline,
                EventPhase::TransportReceive,
                actor_for_route(route),
                owned_bytes,
                wire_trace_tag(route, lane, ordinal),
                HarnessAction::WireDelivery(delivery),
            ))
            .map_err(|error| EncodedLegTransportError::Wire(WireError::Schedule(error)))?;
        *self.observed_work = charged;
        self.harness.next_send_ordinal = next_ordinal;
        Ok(())
    }
}

const fn actor_for_route(route: WireRoute) -> ActorId {
    let route_index = match (route.leg(), route.direction()) {
        (crate::owned_upstream::two_leg::LegId::A, WireDirection::ClientToOwner) => 0,
        (crate::owned_upstream::two_leg::LegId::A, WireDirection::OwnerToClient) => 1,
        (crate::owned_upstream::two_leg::LegId::B, WireDirection::ClientToOwner) => 2,
        (crate::owned_upstream::two_leg::LegId::B, WireDirection::OwnerToClient) => 3,
    };
    ActorId::new(route_index + 1)
}

fn wire_trace_tag(route: WireRoute, lane: WireLane, ordinal: u64) -> u64 {
    let route_tag = u64::from(actor_for_route(route).get());
    let lane_tag = match lane {
        WireLane::Control => 0_u64,
        WireLane::Data => 1_u64,
    };
    ordinal.rotate_left(17) ^ (route_tag << 8) ^ lane_tag
}

const CLIENT_SUPERVISOR: ActorId = ActorId::new(10);
const OWNER_SUPERVISOR: ActorId = ActorId::new(11);
const CLIENT_SINK: ActorId = ActorId::new(12);
const OWNER_TARGET: ActorId = ActorId::new(13);
const CLIENT_TIMER: ActorId = ActorId::new(14);
const OWNER_TIMER: ActorId = ActorId::new(15);
const TERMINAL_GRACE_DELAY: Duration = Duration::from_nanos(32);
const CLIENT_BYTES: &[u8] = b"client-production-bytes";
const OWNER_BYTES: &[u8] = b"owner-production-bytes";

/// Complete finite inputs for the R5 byte baseline. The directional wire
/// declaration uses the larger 23-byte payload; the other direction is one
/// byte shorter. Four ACKs and three attach/open/close records are the exact
/// fixed wire maximum per direction. The 64 fixed actor turns include the
/// bounded attach, timer, close/join, Bound, driver, sink, and flush actions,
/// plus the one transport-pressure retry exercised below.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct R5ScenarioManifest {
    rate_bits_per_second: u64,
    horizon: Duration,
    max_payload_bytes: usize,
    frame_overhead_bytes: usize,
    forced_tail_fragments: usize,
    replay_copies: u8,
    duplicates_per_send: u8,
    fixed_wire_messages_per_direction: usize,
    fixed_wire_bytes_per_direction: usize,
    directions: usize,
    max_positive_accept_pieces: usize,
    max_zero_would_block: usize,
    max_ack_constructs_per_direction: usize,
    fixed_actor_work: HarnessFixedWork,
    max_non_wire_action_bytes: usize,
}

impl R5ScenarioManifest {
    const fn baseline() -> Self {
        Self {
            rate_bits_per_second: 23 * 8,
            horizon: Duration::from_secs(1),
            max_payload_bytes: 23,
            frame_overhead_bytes: 37,
            forced_tail_fragments: 1,
            replay_copies: 0,
            duplicates_per_send: 0,
            fixed_wire_messages_per_direction: 7,
            fixed_wire_bytes_per_direction: 328,
            directions: 2,
            max_positive_accept_pieces: 2,
            max_zero_would_block: 1,
            max_ack_constructs_per_direction: 4,
            fixed_actor_work: HarnessFixedWork::new(5, 0, 2, 10, 0, 2).with_adapter_turns(45),
            max_non_wire_action_bytes: 104,
        }
    }

    fn work_budget(self) -> Result<HarnessWorkBudget, CapacityError> {
        let wire = WireCapacitySpec::new(
            self.rate_bits_per_second,
            self.horizon,
            self.max_payload_bytes,
            self.frame_overhead_bytes,
            self.forced_tail_fragments,
        )?
        .with_fault_copies(self.replay_copies, self.duplicates_per_send)?
        .with_fixed_work(
            self.fixed_wire_messages_per_direction,
            self.fixed_wire_bytes_per_direction,
        )
        .derive()?;
        let max_non_wire_owned_bytes = self
            .fixed_actor_work
            .turns()?
            .checked_mul(self.max_non_wire_action_bytes)
            .ok_or(CapacityError::ArithmeticOverflow)?;
        HarnessWorkSpec::new(
            wire,
            self.directions,
            self.max_positive_accept_pieces,
            self.max_zero_would_block,
            self.max_ack_constructs_per_direction,
            self.fixed_actor_work,
            max_non_wire_owned_bytes,
        )?
        .derive()
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct R5ZeroLedger {
    scheduler_pending_actions: usize,
    scheduler_pending_bytes: usize,
    scheduler_has_work_error: bool,
    outbound_queued_frames: usize,
    outbound_queued_bytes: usize,
    client_flush_scheduled: bool,
    owner_flush_scheduled: bool,
    owner_retry_scheduled: bool,
    target_drain_scheduled: bool,
    target_live_flows: usize,
    target_joined_flows: usize,
    target_buffered_bytes: usize,
    target_queued_write_directives: usize,
    client_live_flows: usize,
    client_terminal_tombstones: usize,
    client_receive_owned_bytes: usize,
    client_receive_ranges: usize,
    client_outstanding_sink_offers: usize,
    client_c2t_replay_bytes: usize,
    client_c2t_replay_segments: usize,
    client_t2c_replay_bytes: usize,
    client_t2c_replay_segments: usize,
    client_supervisor_replay_extents: usize,
    client_supervisor_replay_owned_bytes: usize,
    client_supervisor_poisoned: bool,
    client_supervisor_quarantined_extents: usize,
    client_supervisor_quarantined_owned_bytes: usize,
    owner_live_flows: usize,
    owner_terminal_tombstones: usize,
    owner_receive_owned_bytes: usize,
    owner_receive_ranges: usize,
    owner_outstanding_sink_offers: usize,
    owner_c2t_replay_bytes: usize,
    owner_c2t_replay_segments: usize,
    owner_t2c_replay_bytes: usize,
    owner_t2c_replay_segments: usize,
    owner_supervisor_replay_extents: usize,
    owner_supervisor_replay_owned_bytes: usize,
    owner_supervisor_poisoned: bool,
    owner_supervisor_quarantined_extents: usize,
    owner_supervisor_quarantined_owned_bytes: usize,
    client_port_owned_bytes: usize,
    client_port_owned_segments: usize,
    client_port_pending_downlink_actions: usize,
    client_port_reserved_completion_slots: usize,
    owner_target_flows: usize,
    owner_target_tombstones: usize,
    owner_pending_writes: usize,
    owner_pending_joins: usize,
    owner_target_read_owned_bytes: usize,
    owner_target_read_owned_segments: usize,
    owner_pending_effects: usize,
    owner_needs_resume: bool,
    owner_pending_target_completions: usize,
    owner_aborted_effects: usize,
    owner_aborted_outputs: usize,
}

#[derive(Debug, PartialEq, Eq)]
struct R5BaselineReport {
    work_budget: HarnessWorkBudget,
    observed_work: HarnessObservedWork,
    scheduler_event_budget: EventBudget,
    step_limit: usize,
    target_open_attempts: u64,
    target_opened_flows: u64,
    client_to_target: Vec<u8>,
    target_to_client: Vec<u8>,
    target_zero_accepts: u64,
    client_zero_accepts: u64,
    target_positive_accepts: u64,
    client_positive_accepts: u64,
    target_accepted_bytes: u64,
    client_accepted_bytes: usize,
    client_replay_bytes: usize,
    owner_replay_bytes: usize,
    client_port_owned_bytes: usize,
    owner_port_owned_bytes: usize,
    owner_port_owned_segments: usize,
    target_live_flows: usize,
    target_tombstones: usize,
    client_live_flows: usize,
    owner_live_flows: usize,
    client_terminal_tombstones: usize,
    owner_terminal_tombstones: usize,
    owner_target_flows: usize,
    owner_pending_writes: usize,
    owner_pending_joins: usize,
    client_port_quiescent: bool,
    client_ack_frames: u64,
    owner_ack_frames: u64,
    outbound_queued_frames: usize,
    outbound_queued_bytes: usize,
    outbound_retries: u64,
    scheduler_pending_actions: usize,
    scheduler_pending_bytes: usize,
    zero_ledger: R5ZeroLedger,
    encoded_messages: u64,
    bound_messages: u64,
    steps: usize,
    trace_hash: u64,
}

struct R5BaselineHarness {
    work_budget: HarnessWorkBudget,
    observed_work: HarnessObservedWork,
    scheduler: ByteHarnessScheduler,
    client_leg_io: crate::owned_upstream::leg_io::LegIo,
    owner_leg_io: crate::owned_upstream::leg_io::LegIo,
    client_outbound: LegOutboundQueue,
    owner_outbound: LegOutboundQueue,
    client_flush_scheduled: bool,
    owner_flush_scheduled: bool,
    client_pending: Option<PendingAttach>,
    owner_pending: Option<PendingOwnerBootstrap>,
    client_attached: Option<AttachedLeg>,
    owner_attached: Option<AttachedLeg>,
    client_supervisor: Option<SessionSupervisor>,
    owner: Option<OwnerTargetExecutor<MemoryTarget>>,
    client_port_factory: Option<ResumableTcpPortFactory>,
    client_flow: Option<ResumableTcpFlow>,
    client_driver: Option<ResumableTcpDriver>,
    flow_id: Option<SessionFlowId>,
    target: TargetAddr,
    target_directives_configured: bool,
    owner_retry_scheduled: bool,
    target_drain_scheduled: bool,
    reverse_started: bool,
    close_started: bool,
    client_zero_done: bool,
    client_partial_done: bool,
    client_positive_accepts: u64,
    client_to_target: Vec<u8>,
    target_to_client: Vec<u8>,
    client_ack_frames: u64,
    owner_ack_frames: u64,
    outbound_retries: u64,
    client_terminal_seen: bool,
    owner_terminal_seen: bool,
    steps: usize,
}

impl R5BaselineHarness {
    fn new() -> Result<Self, String> {
        Self::new_with_rejection(None)
    }

    fn new_with_rejection(reject_once_at: Option<u64>) -> Result<Self, String> {
        let binding = baseline_binding()?;
        let limits = crate::owned_upstream::leg_io::LegIoLimits::new(
            1_024,
            128,
            128 * 1_024,
            128,
            128 * 1_024,
        )
        .map_err(|error| error.to_string())?;
        let client_leg_io = crate::owned_upstream::leg_io::LegIo::for_authenticated_transport(
            crate::owned_upstream::two_leg::LegId::A,
            crate::owned_upstream::leg_io::LegEndpointRole::Client,
            binding,
            limits,
        );
        let owner_leg_io = crate::owned_upstream::leg_io::LegIo::for_authenticated_transport(
            crate::owned_upstream::two_leg::LegId::A,
            crate::owned_upstream::leg_io::LegEndpointRole::Owner,
            binding,
            limits,
        );
        let session_id = SessionId::new([0x16; 16]).map_err(|error| error.to_string())?;
        let request = AttachRequest::new(
            session_id,
            LegGeneration::new(2).map_err(|error| error.to_string())?,
            AttachNonce::new([0x62; 16]).map_err(|error| error.to_string())?,
            VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION)
                .map_err(|error| error.to_string())?,
            FeatureOffer::new(0b0111, 0b0001).map_err(|error| error.to_string())?,
        );
        let proof = baseline_credentials()?
            .prove(&request, &binding)
            .map_err(|error| error.to_string())?;
        let client_pending = client_leg_io.established_leg().begin_attach(request);
        let attach_frame = request.to_attach_frame(proof);
        let attach_owned_bytes = attach_frame
            .encode()
            .map_err(|error| error.to_string())?
            .len();
        let authority = AttachAuthority::new(
            session_id,
            LegGeneration::new(1).map_err(|error| error.to_string())?,
            baseline_credentials()?,
            AttachPolicy::new(
                binding.owner_identity(),
                binding.alpn(),
                binding.device_principal(),
                SESSION_PROTOCOL_VERSION,
                0b1111,
            )
            .map_err(|error| error.to_string())?,
        );
        let work_budget = R5ScenarioManifest::baseline()
            .work_budget()
            .map_err(|error| error.to_string())?;
        let mut observed_work = HarnessObservedWork::default();
        let scheduled_observed = observed_work
            .charged_owned_bytes(
                work_budget,
                HarnessOwnedByteCategory::NonWire,
                attach_owned_bytes,
            )
            .map_err(|error| error.to_string())?;
        let mut scheduler = ByteHarnessScheduler::new(work_budget.event_budget());
        if let Some(ordinal) = reject_once_at {
            scheduler.reject_once_at(ordinal);
        }
        scheduler
            .schedule(
                SimTime::ZERO,
                EventPhase::SupervisorCommand,
                CLIENT_SUPERVISOR,
                attach_owned_bytes,
                0x100,
                HarnessAction::StartAttach(attach_frame),
            )
            .map_err(|error| error.to_string())?;
        observed_work = scheduled_observed;
        Ok(Self {
            work_budget,
            observed_work,
            scheduler,
            client_leg_io,
            owner_leg_io,
            client_outbound: LegOutboundQueue::new(16, 16 * 1_024)
                .map_err(|error| error.to_string())?,
            owner_outbound: LegOutboundQueue::new(16, 16 * 1_024)
                .map_err(|error| error.to_string())?,
            client_flush_scheduled: false,
            owner_flush_scheduled: false,
            client_pending: Some(client_pending),
            owner_pending: Some(SessionSupervisor::prepare_owner(
                baseline_session_config()?,
                authority,
            )),
            client_attached: None,
            owner_attached: None,
            client_supervisor: None,
            owner: None,
            client_port_factory: None,
            client_flow: None,
            client_driver: None,
            flow_id: None,
            target: TargetAddr::IpPort(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 443))),
            target_directives_configured: false,
            owner_retry_scheduled: false,
            target_drain_scheduled: false,
            reverse_started: false,
            close_started: false,
            client_zero_done: false,
            client_partial_done: false,
            client_positive_accepts: 0,
            client_to_target: Vec::new(),
            target_to_client: Vec::new(),
            client_ack_frames: 0,
            owner_ack_frames: 0,
            outbound_retries: 0,
            client_terminal_seen: false,
            owner_terminal_seen: false,
            steps: 0,
        })
    }

    fn run(mut self) -> Result<R5BaselineReport, String> {
        while let Some(event) = self
            .scheduler
            .pop_next_checked()
            .map_err(|error| error.to_string())?
        {
            let now = event.key().deadline();
            let action = event.into_payload();
            self.steps = self
                .steps
                .checked_add(1)
                .ok_or_else(|| "R5 step counter overflow".to_owned())?;
            if self.steps > self.work_budget.max_turns() {
                return Err(format!(
                    "R5 exceeded {} scheduler turns",
                    self.work_budget.max_turns()
                ));
            }
            if !action.is_wire_delivery() {
                self.observed_work
                    .charge(self.work_budget, HarnessWorkCategory::FixedActor, 1)
                    .map_err(|error| error.to_string())?;
            }
            self.handle_action(now, action)?;
            self.maybe_schedule_progress(now)?;
        }
        self.finish_report()
    }

    fn handle_action(&mut self, now: SimTime, action: HarnessAction) -> Result<(), String> {
        match action {
            HarnessAction::WireDelivery(delivery) => self.receive_wire(now, delivery),
            HarnessAction::StartAttach(frame) => self.send_client_frame(now, frame),
            HarnessAction::OwnerBound(received) => self.handle_owner_bound(now, received),
            HarnessAction::ClientBound(received) => self.handle_client_bound(now, received),
            HarnessAction::StartOpen => self.start_open(now),
            HarnessAction::SendClientData(payload) => self.send_client_data(now, payload),
            HarnessAction::RetryOwnerWrite => self.retry_owner_write(now),
            HarnessAction::DrainTarget => self.drain_target(now),
            HarnessAction::FeedOwnerRead(payload) => self.feed_owner_read(now, payload),
            HarnessAction::ReadOwnerSource => self.read_owner_source(now),
            HarnessAction::PollClientAction => self.poll_client_action(now),
            HarnessAction::ApplyClientSink { delivery, accepted } => {
                self.apply_client_sink(now, delivery, accepted)
            }
            HarnessAction::ApplyClientHalfClose(delivery) => {
                self.apply_client_half_close(now, delivery)
            }
            HarnessAction::ConsumeClientTerminal(delivery) => {
                self.consume_client_terminal(delivery)
            }
            HarnessAction::PollClientDriver => self.poll_client_driver(now),
            HarnessAction::CloseClientSource => self.close_client_source(now),
            HarnessAction::FinishOwnerSource => self.finish_owner_source(now),
            HarnessAction::ExpireClientTerminal(terminal) => {
                self.expire_client_terminal(now, terminal)
            }
            HarnessAction::ExpireOwnerTerminal(terminal) => {
                self.expire_owner_terminal(now, terminal)
            }
            HarnessAction::ResumeOwnerEffects(resume) => self.resume_owner_effects(now, resume),
            HarnessAction::FlushClientOutbound => self.flush_client_outbound(now),
            HarnessAction::FlushOwnerOutbound => self.flush_owner_outbound(now),
        }
    }

    fn receive_wire(&mut self, now: SimTime, delivery: EncodedDelivery) -> Result<(), String> {
        let retained_bytes = delivery.bytes().len();
        match delivery.route().direction() {
            WireDirection::ClientToOwner => {
                let received = self
                    .owner_leg_io
                    .receive_delivery(delivery)
                    .map_err(|error| error.to_string())?;
                self.schedule(
                    now,
                    EventPhase::SupervisorCommand,
                    OWNER_SUPERVISOR,
                    retained_bytes,
                    0x201,
                    HarnessAction::OwnerBound(received),
                )
            }
            WireDirection::OwnerToClient => {
                let received = self
                    .client_leg_io
                    .receive_delivery(delivery)
                    .map_err(|error| error.to_string())?;
                self.schedule(
                    now,
                    EventPhase::SupervisorCommand,
                    CLIENT_SUPERVISOR,
                    retained_bytes,
                    0x202,
                    HarnessAction::ClientBound(received),
                )
            }
        }
    }

    fn handle_owner_bound(&mut self, now: SimTime, received: LegBoundFrame) -> Result<(), String> {
        if self.owner.is_none() {
            let pending = self
                .owner_pending
                .as_mut()
                .ok_or_else(|| "owner bootstrap state disappeared".to_owned())?;
            let bootstrap = pending
                .accept(self.owner_leg_io.established_leg(), received)
                .map_err(|error| error.to_string())?;
            let (supervisor, attached, acceptance) = bootstrap.into_parts();
            self.owner_attached = Some(attached);
            self.owner = Some(
                OwnerTargetExecutor::new(
                    OwnerTargetConfig::new(256, 1_024, 64).map_err(|error| error.to_string())?,
                    supervisor,
                    MemoryTarget::new(
                        MemoryTargetConfig::new(2, 256, 512, 64, 64, 8)
                            .map_err(|error| error.to_string())?
                            .with_max_terminal_tombstones(8)
                            .map_err(|error| error.to_string())?,
                    ),
                    baseline_port_config()?,
                )
                .map_err(|error| error.to_string())?,
            );
            self.send_owner_frame(now, acceptance)?;
            return Ok(());
        }

        let event = self
            .owner_attached
            .as_ref()
            .ok_or_else(|| "owner exact attached leg disappeared".to_owned())?
            .accept_frame(received)
            .map_err(|error| error.to_string())?;
        self.charge_ack_decode(&event)?;
        let target_before = self.target_write_counters()?;
        let outputs = self
            .owner
            .as_mut()
            .ok_or_else(|| "owner executor disappeared".to_owned())?
            .apply_event(event)
            .map_err(|error| error.to_string())?;
        let target_after = self.target_write_counters()?;
        self.charge_target_write_delta(target_before, target_after)?;
        self.process_owner_outputs(now, outputs)?;
        self.configure_target_directives()?;
        self.schedule_owner_followups(now)
    }

    fn handle_client_bound(&mut self, now: SimTime, received: LegBoundFrame) -> Result<(), String> {
        if let Some(pending) = self.client_pending.take() {
            let AttachResponse::Accepted(attached) = pending
                .validate_response(received)
                .map_err(|error| error.to_string())?
            else {
                return Err("baseline initial attach returned generation status".to_owned());
            };
            let bootstrap =
                SessionSupervisor::bootstrap_client(baseline_session_config()?, attached)
                    .map_err(|error| error.to_string())?;
            let (mut supervisor, attached) = bootstrap.into_parts();
            let port_factory = supervisor
                .mint_tcp_port_factory(baseline_port_config()?)
                .map_err(|error| error.to_string())?;
            self.client_port_factory = Some(port_factory);
            self.client_supervisor = Some(supervisor);
            self.client_attached = Some(attached);
            return self.schedule_next(
                now,
                EventPhase::SupervisorCommand,
                CLIENT_SUPERVISOR,
                0,
                0x301,
                HarnessAction::StartOpen,
            );
        }

        let event = self
            .client_attached
            .as_ref()
            .ok_or_else(|| "client exact attached leg disappeared".to_owned())?
            .accept_frame(received)
            .map_err(|error| error.to_string())?;
        self.charge_ack_decode(&event)?;
        let effects = self
            .client_supervisor
            .as_mut()
            .ok_or_else(|| "client supervisor disappeared".to_owned())?
            .apply_event(event)
            .map_err(|error| error.to_string())?;
        self.process_client_effects(now, effects)
    }

    fn start_open(&mut self, now: SimTime) -> Result<(), String> {
        let event = self
            .client_attached
            .as_ref()
            .ok_or_else(|| "client attached leg missing at OPEN".to_owned())?
            .local_open_event(self.target.clone());
        let effects = self
            .client_supervisor
            .as_mut()
            .ok_or_else(|| "client supervisor missing at OPEN".to_owned())?
            .apply_event(event)
            .map_err(|error| error.to_string())?;
        self.process_client_effects(now, effects)
    }

    fn send_client_data(&mut self, now: SimTime, payload: Bytes) -> Result<(), String> {
        let len = payload.len();
        self.client_flow
            .as_ref()
            .ok_or_else(|| "client flow missing at source DATA".to_owned())?
            .try_send_uplink_with(len, move || payload)
            .map_err(|error| error.to_string())?;
        self.poll_client_driver(now)
    }

    fn poll_client_driver(&mut self, now: SimTime) -> Result<(), String> {
        let input = self
            .client_driver
            .as_mut()
            .ok_or_else(|| "client driver missing".to_owned())?
            .try_recv_next()
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "scheduled client driver turn had no input".to_owned())?;
        let effects = self
            .client_supervisor
            .as_mut()
            .ok_or_else(|| "client supervisor missing".to_owned())?
            .apply_command(SessionOwnerCommand::Driver(input))
            .map_err(|error| error.to_string())?;
        self.process_client_effects(now, effects)
    }

    fn process_client_effects(
        &mut self,
        now: SimTime,
        effects: Vec<SessionEffect>,
    ) -> Result<(), String> {
        for effect in effects {
            match effect {
                SessionEffect::Transmit(frame) => self.send_client_frame(now, frame)?,
                SessionEffect::LocalFlowOpened { flow } => {
                    if self.client_flow.is_some() || self.client_driver.is_some() {
                        return Err("baseline opened more than one client flow".to_owned());
                    }
                    let flow_id = flow.flow_id();
                    let (port, driver) = self
                        .client_port_factory
                        .as_ref()
                        .ok_or_else(|| "client TCP factory was not minted at bootstrap".to_owned())?
                        .open_flow(flow)
                        .map_err(|error| error.to_string())?;
                    self.flow_id = Some(flow_id);
                    self.client_flow = Some(port);
                    self.client_driver = Some(driver);
                }
                SessionEffect::LocalOpenResolved { flow_id, result } => {
                    if Some(flow_id) != self.flow_id || result != OpenResultCode::Opened {
                        return Err(format!("baseline OPEN failed for {flow_id:?}: {result:?}"));
                    }
                    self.schedule_next(
                        now,
                        EventPhase::SupervisorCommand,
                        CLIENT_SUPERVISOR,
                        CLIENT_BYTES.len(),
                        0x302,
                        HarnessAction::SendClientData(Bytes::from_static(CLIENT_BYTES)),
                    )?;
                }
                SessionEffect::OfferToSink { offer, segments } => {
                    self.client_driver
                        .as_ref()
                        .ok_or_else(|| "client driver missing at sink offer".to_owned())?
                        .try_deliver_sink(offer, segments)
                        .map_err(|error| error.to_string())?;
                    self.schedule(
                        now,
                        EventPhase::IoCompletion,
                        CLIENT_SINK,
                        offer.len(),
                        0x401,
                        HarnessAction::PollClientAction,
                    )?;
                }
                SessionEffect::HalfCloseSink { completion } => {
                    self.client_driver
                        .as_ref()
                        .ok_or_else(|| "client driver missing at half-close".to_owned())?
                        .try_deliver_half_close(completion)
                        .map_err(|error| error.to_string())?;
                    self.schedule(
                        now,
                        EventPhase::IoCompletion,
                        CLIENT_SINK,
                        0,
                        0x402,
                        HarnessAction::PollClientAction,
                    )?;
                }
                effect @ SessionEffect::FlowFinished {
                    reason: FlowFinishReason::Graceful,
                    terminal,
                    ..
                } => {
                    self.client_driver
                        .as_mut()
                        .ok_or_else(|| "client driver missing at terminal".to_owned())?
                        .try_deliver_terminal_effect(&effect)
                        .map_err(|error| error.to_string())?;
                    self.schedule(
                        now,
                        EventPhase::IoCompletion,
                        CLIENT_SINK,
                        0,
                        0x403,
                        HarnessAction::PollClientAction,
                    )?;
                    let deadline = now
                        .checked_add(TERMINAL_GRACE_DELAY)
                        .map_err(|error| error.to_string())?;
                    self.schedule(
                        deadline,
                        EventPhase::TimerExpiry,
                        CLIENT_TIMER,
                        0,
                        0x404,
                        HarnessAction::ExpireClientTerminal(terminal),
                    )?;
                }
                SessionEffect::FlowFinished { reason, .. } => {
                    return Err(format!(
                        "baseline client terminated non-gracefully: {reason:?}"
                    ));
                }
                SessionEffect::LegActivated { .. } => {}
                SessionEffect::ResumeGraceStarted { .. }
                | SessionEffect::SessionExpired
                | SessionEffect::ReplayStored { .. }
                | SessionEffect::ReplayAcknowledged { .. }
                | SessionEffect::PeerOpenRequested { .. }
                | SessionEffect::PeerReset { .. } => {
                    return Err(format!("unexpected client effect in R5: {effect:?}"));
                }
            }
        }
        Ok(())
    }

    fn poll_client_action(&mut self, now: SimTime) -> Result<(), String> {
        let action = self
            .client_flow
            .as_mut()
            .ok_or_else(|| "client flow missing at sink turn".to_owned())?
            .try_recv_action()
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "scheduled client sink turn had no action".to_owned())?;
        match action {
            TunAction::Sink(delivery) => {
                let accepted = if !self.client_zero_done {
                    0
                } else if !self.client_partial_done {
                    3.min(delivery.first_segment_slice().len())
                } else {
                    delivery.first_segment_slice().len()
                };
                self.schedule(
                    now,
                    EventPhase::IoCompletion,
                    CLIENT_SINK,
                    delivery.offered_len(),
                    0x405,
                    HarnessAction::ApplyClientSink { delivery, accepted },
                )
            }
            TunAction::HalfClose(delivery) => self.schedule(
                now,
                EventPhase::IoCompletion,
                CLIENT_SINK,
                0,
                0x406,
                HarnessAction::ApplyClientHalfClose(delivery),
            ),
            TunAction::Terminal(delivery) => self.schedule(
                now,
                EventPhase::JoinCleanup,
                CLIENT_SINK,
                0,
                0x407,
                HarnessAction::ConsumeClientTerminal(delivery),
            ),
        }
    }

    fn apply_client_sink(
        &mut self,
        now: SimTime,
        delivery: SinkDelivery,
        accepted: usize,
    ) -> Result<(), String> {
        let category = if accepted == 0 {
            HarnessWorkCategory::ZeroWouldBlock
        } else {
            HarnessWorkCategory::PositiveAccept
        };
        self.observed_work
            .charge(self.work_budget, category, 1)
            .map_err(|error| error.to_string())?;
        if accepted == 0 {
            let ack_before = self.client_ack_frames;
            let delivery = delivery
                .complete_write(0)
                .map_err(|error| error.to_string())?
                .ok_or_else(|| "zero sink acceptance consumed delivery".to_owned())?;
            if self
                .client_driver
                .as_mut()
                .ok_or_else(|| "client driver missing after zero sink".to_owned())?
                .try_recv_next()
                .map_err(|error| error.to_string())?
                .is_some()
            {
                return Err("zero sink acceptance created reducer input".to_owned());
            }
            if self.client_ack_frames != ack_before {
                return Err("zero sink acceptance emitted an ACK".to_owned());
            }
            self.client_zero_done = true;
            return self.schedule_next(
                now,
                EventPhase::IoCompletion,
                CLIENT_SINK,
                delivery.offered_len(),
                0x408,
                HarnessAction::ApplyClientSink {
                    accepted: 3.min(delivery.first_segment_slice().len()),
                    delivery,
                },
            );
        }

        let offered = delivery.offered_len();
        let accepted_bytes = delivery
            .with_live_first_segment(|segment| segment[..accepted].to_vec())
            .map_err(|error| error.to_string())?;
        self.target_to_client.extend(accepted_bytes);
        if accepted < offered {
            self.client_partial_done = true;
        }
        self.client_positive_accepts = self.client_positive_accepts.saturating_add(1);
        if delivery
            .complete_write(accepted)
            .map_err(|error| error.to_string())?
            .is_some()
        {
            return Err("positive sink acceptance retained old delivery".to_owned());
        }
        self.schedule_next(
            now,
            EventPhase::SupervisorCommand,
            CLIENT_SUPERVISOR,
            0,
            0x409,
            HarnessAction::PollClientDriver,
        )
    }

    fn apply_client_half_close(
        &mut self,
        now: SimTime,
        delivery: HalfCloseDelivery,
    ) -> Result<(), String> {
        delivery
            .with_live(|| ())
            .map_err(|error| error.to_string())?;
        delivery.succeed();
        self.schedule_next(
            now,
            EventPhase::SupervisorCommand,
            CLIENT_SUPERVISOR,
            0,
            0x40a,
            HarnessAction::PollClientDriver,
        )
    }

    fn consume_client_terminal(&mut self, delivery: TerminalDelivery) -> Result<(), String> {
        if delivery.action() != TerminalAction::FlowFinished(FlowFinishReason::Graceful) {
            return Err(format!(
                "unexpected client terminal: {:?}",
                delivery.action()
            ));
        }
        self.client_terminal_seen = true;
        drop(delivery);
        Ok(())
    }

    fn retry_owner_write(&mut self, now: SimTime) -> Result<(), String> {
        self.owner_retry_scheduled = false;
        let flow_id = self.required_flow_id()?;
        let target_before = self.target_write_counters()?;
        let outputs = self
            .owner
            .as_mut()
            .ok_or_else(|| "owner missing at write retry".to_owned())?
            .retry_target_write(flow_id)
            .map_err(|error| error.to_string())?;
        let target_after = self.target_write_counters()?;
        self.charge_target_write_delta(target_before, target_after)?;
        if !outputs.iter().any(|output| {
            matches!(output, OwnerTargetOutput::Transmit(frame) if matches!(frame.record(), Record::Ack { .. }))
        }) {
            return Err("positive Target retry emitted no application ACK".to_owned());
        }
        self.process_owner_outputs(now, outputs)?;
        self.schedule_owner_followups(now)
    }

    fn drain_target(&mut self, now: SimTime) -> Result<(), String> {
        self.target_drain_scheduled = false;
        let flow_id = self.required_flow_id()?;
        let bytes = self
            .owner
            .as_mut()
            .ok_or_else(|| "owner missing at Target drain".to_owned())?
            .target_mut()
            .take_written(flow_id, 128)
            .map_err(|error| error.to_string())?;
        self.client_to_target.extend_from_slice(&bytes);
        self.schedule_owner_followups(now)
    }

    fn feed_owner_read(&mut self, now: SimTime, payload: Bytes) -> Result<(), String> {
        let flow_id = self.required_flow_id()?;
        self.owner
            .as_mut()
            .ok_or_else(|| "owner missing at reverse feed".to_owned())?
            .target_mut()
            .feed_read(flow_id, payload)
            .map_err(|error| error.to_string())?;
        self.schedule_next(
            now,
            EventPhase::IoCompletion,
            OWNER_TARGET,
            0,
            0x501,
            HarnessAction::ReadOwnerSource,
        )
    }

    fn read_owner_source(&mut self, now: SimTime) -> Result<(), String> {
        let flow_id = self.required_flow_id()?;
        let outputs = self
            .owner
            .as_mut()
            .ok_or_else(|| "owner missing at Target read".to_owned())?
            .try_read_target(flow_id)
            .map_err(|error| error.to_string())?;
        if outputs.is_empty() {
            return Err("scheduled Target read made no progress".to_owned());
        }
        self.process_owner_outputs(now, outputs)
    }

    fn close_client_source(&mut self, now: SimTime) -> Result<(), String> {
        self.client_flow
            .as_ref()
            .ok_or_else(|| "client flow missing at FIN".to_owned())?
            .try_send_close()
            .map_err(|error| error.to_string())?;
        self.poll_client_driver(now)
    }

    fn finish_owner_source(&mut self, now: SimTime) -> Result<(), String> {
        let flow_id = self.required_flow_id()?;
        let owner = self
            .owner
            .as_mut()
            .ok_or_else(|| "owner missing at reverse FIN".to_owned())?;
        owner
            .target_mut()
            .finish_read(flow_id)
            .map_err(|error| error.to_string())?;
        let outputs = owner
            .try_read_target(flow_id)
            .map_err(|error| error.to_string())?;
        self.process_owner_outputs(now, outputs)
    }

    fn expire_client_terminal(
        &mut self,
        now: SimTime,
        terminal: TerminalGrace,
    ) -> Result<(), String> {
        let effects = self
            .client_supervisor
            .as_mut()
            .ok_or_else(|| "client supervisor missing at terminal expiry".to_owned())?
            .apply_event(SessionEvent::TerminalGraceExpired { terminal })
            .map_err(|error| error.to_string())?;
        self.process_client_effects(now, effects)
    }

    fn expire_owner_terminal(
        &mut self,
        now: SimTime,
        terminal: TerminalGrace,
    ) -> Result<(), String> {
        let outputs = self
            .owner
            .as_mut()
            .ok_or_else(|| "owner missing at terminal expiry".to_owned())?
            .apply_event(SessionEvent::TerminalGraceExpired { terminal })
            .map_err(|error| error.to_string())?;
        self.process_owner_outputs(now, outputs)
    }

    fn resume_owner_effects(
        &mut self,
        now: SimTime,
        resume: OwnerTargetResume,
    ) -> Result<(), String> {
        let target_before = self.target_write_counters()?;
        let outputs = self
            .owner
            .as_mut()
            .ok_or_else(|| "owner missing at effect continuation".to_owned())?
            .resume_pending_effects(resume)
            .map_err(|error| error.to_string())?;
        let target_after = self.target_write_counters()?;
        self.charge_target_write_delta(target_before, target_after)?;
        self.process_owner_outputs(now, outputs)
    }

    fn process_owner_outputs(
        &mut self,
        now: SimTime,
        outputs: Vec<OwnerTargetOutput>,
    ) -> Result<(), String> {
        for output in outputs {
            match output {
                OwnerTargetOutput::Transmit(frame) => self.send_owner_frame(now, frame)?,
                OwnerTargetOutput::TerminalGraceStarted { terminal } => {
                    self.owner_terminal_seen = true;
                    let deadline = now
                        .checked_add(TERMINAL_GRACE_DELAY)
                        .map_err(|error| error.to_string())?;
                    self.schedule(
                        deadline,
                        EventPhase::TimerExpiry,
                        OWNER_TIMER,
                        0,
                        0x601,
                        HarnessAction::ExpireOwnerTerminal(terminal),
                    )?;
                }
                OwnerTargetOutput::LegActivated { .. } => {}
                OwnerTargetOutput::NeedsResume { resume } => {
                    self.schedule(
                        now,
                        EventPhase::IoCompletion,
                        OWNER_TARGET,
                        0,
                        0x602,
                        HarnessAction::ResumeOwnerEffects(resume),
                    )?;
                }
                OwnerTargetOutput::ResumeGraceStarted { .. }
                | OwnerTargetOutput::SessionExpired => {
                    return Err(format!("unexpected owner output in R5: {output:?}"));
                }
            }
        }
        Ok(())
    }

    fn configure_target_directives(&mut self) -> Result<(), String> {
        if self.target_directives_configured {
            return Ok(());
        }
        let Some(flow_id) = self.flow_id else {
            return Ok(());
        };
        let owner = self
            .owner
            .as_mut()
            .ok_or_else(|| "owner missing while configuring Target".to_owned())?;
        if owner.target().snapshot().opened_flows == 0 {
            return Ok(());
        }
        owner
            .target_mut()
            .push_write_directive(flow_id, MemoryWriteDirective::Zero)
            .map_err(|error| error.to_string())?;
        owner
            .target_mut()
            .push_write_directive(
                flow_id,
                MemoryWriteDirective::accept_at_most(3).map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string())?;
        self.target_directives_configured = true;
        Ok(())
    }

    fn schedule_owner_followups(&mut self, now: SimTime) -> Result<(), String> {
        let Some(owner) = self.owner.as_ref() else {
            return Ok(());
        };
        let snapshot = owner.snapshot();
        let target = owner.target().snapshot();
        if snapshot.pending_writes > 0 && !self.owner_retry_scheduled {
            self.owner_retry_scheduled = true;
            self.schedule_next(
                now,
                EventPhase::IoCompletion,
                OWNER_TARGET,
                0,
                0x602,
                HarnessAction::RetryOwnerWrite,
            )?;
        }
        let accepted = usize::try_from(target.write_accepted_bytes)
            .map_err(|_| "Target accepted-byte counter exceeds usize".to_owned())?;
        if accepted > self.client_to_target.len() && !self.target_drain_scheduled {
            self.target_drain_scheduled = true;
            self.schedule_next(
                now,
                EventPhase::IoCompletion,
                OWNER_TARGET,
                0,
                0x603,
                HarnessAction::DrainTarget,
            )?;
        }
        Ok(())
    }

    fn maybe_schedule_progress(&mut self, now: SimTime) -> Result<(), String> {
        self.schedule_owner_followups(now)?;
        if !self.reverse_started
            && self.client_to_target == CLIENT_BYTES
            && self.client_replay_bytes(Direction::ClientToTarget) == 0
        {
            self.reverse_started = true;
            self.schedule_next(
                now,
                EventPhase::IoCompletion,
                OWNER_TARGET,
                OWNER_BYTES.len(),
                0x701,
                HarnessAction::FeedOwnerRead(Bytes::from_static(OWNER_BYTES)),
            )?;
        }
        if !self.close_started
            && self.target_to_client == OWNER_BYTES
            && self.client_to_target == CLIENT_BYTES
            && self.client_replay_bytes(Direction::ClientToTarget) == 0
            && self.owner_replay_bytes(Direction::TargetToClient) == 0
        {
            self.close_started = true;
            self.schedule_next(
                now,
                EventPhase::SupervisorCommand,
                CLIENT_SUPERVISOR,
                0,
                0x702,
                HarnessAction::CloseClientSource,
            )?;
            self.schedule_next(
                now,
                EventPhase::IoCompletion,
                OWNER_TARGET,
                0,
                0x703,
                HarnessAction::FinishOwnerSource,
            )?;
        }
        Ok(())
    }

    fn client_replay_bytes(&self, direction: Direction) -> usize {
        self.client_supervisor
            .as_ref()
            .map(|supervisor| {
                supervisor
                    .snapshot()
                    .session
                    .replay_usage(direction)
                    .bytes()
            })
            .unwrap_or(usize::MAX)
    }

    fn owner_replay_bytes(&self, direction: Direction) -> usize {
        self.owner
            .as_ref()
            .map(|owner| {
                owner
                    .snapshot()
                    .session
                    .session
                    .replay_usage(direction)
                    .bytes()
            })
            .unwrap_or(usize::MAX)
    }

    fn required_flow_id(&self) -> Result<SessionFlowId, String> {
        self.flow_id
            .ok_or_else(|| "baseline flow id missing".to_owned())
    }

    fn charge_ack_decode(&mut self, event: &SessionEvent) -> Result<(), String> {
        if matches!(
            event,
            SessionEvent::PeerFrame { frame, .. } if matches!(frame.record(), Record::Ack { .. })
        ) {
            self.observed_work
                .charge(self.work_budget, HarnessWorkCategory::AckDecode, 1)
                .map_err(|error| error.to_string())?;
        }
        Ok(())
    }

    fn target_write_counters(&self) -> Result<(u64, u64, u64), String> {
        let snapshot = self
            .owner
            .as_ref()
            .ok_or_else(|| "owner missing at Target work accounting".to_owned())?
            .target()
            .snapshot();
        Ok((
            snapshot.write_calls,
            snapshot.write_zero,
            snapshot.write_would_block,
        ))
    }

    fn charge_target_write_delta(
        &mut self,
        before: (u64, u64, u64),
        after: (u64, u64, u64),
    ) -> Result<(), String> {
        let attempts = after
            .0
            .checked_sub(before.0)
            .ok_or_else(|| "Target write-call accounting went backwards".to_owned())?;
        let zero = after
            .1
            .checked_sub(before.1)
            .and_then(|count| {
                after
                    .2
                    .checked_sub(before.2)
                    .and_then(|would_block| count.checked_add(would_block))
            })
            .ok_or_else(|| "Target zero/would-block accounting went backwards".to_owned())?;
        let positive = attempts
            .checked_sub(zero)
            .ok_or_else(|| "Target acceptance accounting exceeded write calls".to_owned())?;
        let zero = usize::try_from(zero)
            .map_err(|_| "Target zero/would-block accounting exceeds usize".to_owned())?;
        let positive = usize::try_from(positive)
            .map_err(|_| "Target positive accounting exceeds usize".to_owned())?;
        let charged = self
            .observed_work
            .charged(self.work_budget, HarnessWorkCategory::ZeroWouldBlock, zero)
            .and_then(|work| {
                work.charged(
                    self.work_budget,
                    HarnessWorkCategory::PositiveAccept,
                    positive,
                )
            })
            .map_err(|error| error.to_string())?;
        self.observed_work = charged;
        Ok(())
    }

    fn send_client_frame(&mut self, now: SimTime, frame: Frame) -> Result<(), String> {
        let charged = if matches!(frame.record(), Record::Ack { .. }) {
            self.observed_work
                .charged(self.work_budget, HarnessWorkCategory::AckConstruct, 1)
                .map_err(|error| error.to_string())?
        } else {
            self.observed_work
        };
        self.client_outbound
            .push(frame)
            .map_err(|error| error.to_string())?;
        if !self.client_flush_scheduled {
            let before = self.observed_work;
            self.observed_work = charged;
            if let Err(error) = self.schedule_next(
                now,
                EventPhase::SupervisorCommand,
                CLIENT_SUPERVISOR,
                0,
                0x801,
                HarnessAction::FlushClientOutbound,
            ) {
                self.observed_work = before;
                return Err(error);
            }
            self.client_flush_scheduled = true;
        } else {
            self.observed_work = charged;
        }
        Ok(())
    }

    fn send_owner_frame(&mut self, now: SimTime, frame: Frame) -> Result<(), String> {
        let charged = if matches!(frame.record(), Record::Ack { .. }) {
            self.observed_work
                .charged(self.work_budget, HarnessWorkCategory::AckConstruct, 1)
                .map_err(|error| error.to_string())?
        } else {
            self.observed_work
        };
        self.owner_outbound
            .push(frame)
            .map_err(|error| error.to_string())?;
        if !self.owner_flush_scheduled {
            let before = self.observed_work;
            self.observed_work = charged;
            if let Err(error) = self.schedule_next(
                now,
                EventPhase::SupervisorCommand,
                OWNER_SUPERVISOR,
                0,
                0x802,
                HarnessAction::FlushOwnerOutbound,
            ) {
                self.observed_work = before;
                return Err(error);
            }
            self.owner_flush_scheduled = true;
        } else {
            self.observed_work = charged;
        }
        Ok(())
    }

    fn flush_client_outbound(&mut self, now: SimTime) -> Result<(), String> {
        self.client_flush_scheduled = false;
        let work_budget = self.work_budget;
        let result = {
            let (queue, endpoint, scheduler, observed_work) = (
                &mut self.client_outbound,
                &mut self.client_leg_io,
                &mut self.scheduler,
                &mut self.observed_work,
            );
            queue.try_flush(
                endpoint,
                &mut scheduler.controller_sender(work_budget, observed_work),
                now,
            )
        };
        if let Some(error) = self.scheduler.take_work_error() {
            return Err(error.to_string());
        }
        match result {
            Ok(Some(submission)) if submission.is_ack => {
                self.client_ack_frames = self.client_ack_frames.saturating_add(1);
            }
            Ok(Some(_)) | Ok(None) => {}
            Err(LegIoError::Transport(_)) => {
                self.outbound_retries = self.outbound_retries.saturating_add(1);
            }
            Err(error) => return Err(error.to_string()),
        }
        if !self.client_outbound.is_empty() {
            self.schedule_next(
                now,
                EventPhase::SupervisorCommand,
                CLIENT_SUPERVISOR,
                0,
                0x803,
                HarnessAction::FlushClientOutbound,
            )?;
            self.client_flush_scheduled = true;
        }
        Ok(())
    }

    fn flush_owner_outbound(&mut self, now: SimTime) -> Result<(), String> {
        self.owner_flush_scheduled = false;
        let work_budget = self.work_budget;
        let result = {
            let (queue, endpoint, scheduler, observed_work) = (
                &mut self.owner_outbound,
                &mut self.owner_leg_io,
                &mut self.scheduler,
                &mut self.observed_work,
            );
            queue.try_flush(
                endpoint,
                &mut scheduler.controller_sender(work_budget, observed_work),
                now,
            )
        };
        if let Some(error) = self.scheduler.take_work_error() {
            return Err(error.to_string());
        }
        match result {
            Ok(Some(submission)) if submission.is_ack => {
                self.owner_ack_frames = self.owner_ack_frames.saturating_add(1);
            }
            Ok(Some(_)) | Ok(None) => {}
            Err(LegIoError::Transport(_)) => {
                self.outbound_retries = self.outbound_retries.saturating_add(1);
            }
            Err(error) => return Err(error.to_string()),
        }
        if !self.owner_outbound.is_empty() {
            self.schedule_next(
                now,
                EventPhase::SupervisorCommand,
                OWNER_SUPERVISOR,
                0,
                0x804,
                HarnessAction::FlushOwnerOutbound,
            )?;
            self.owner_flush_scheduled = true;
        }
        Ok(())
    }

    fn schedule(
        &mut self,
        deadline: SimTime,
        phase: EventPhase,
        actor: ActorId,
        owned_bytes: usize,
        trace_tag: u64,
        action: HarnessAction,
    ) -> Result<(), String> {
        let charged = if action.is_wire_delivery() {
            self.observed_work
        } else {
            self.observed_work
                .charged_owned_bytes(
                    self.work_budget,
                    HarnessOwnedByteCategory::NonWire,
                    owned_bytes,
                )
                .map_err(|error| error.to_string())?
        };
        self.scheduler
            .schedule(deadline, phase, actor, owned_bytes, trace_tag, action)
            .map_err(|error| error.to_string())?;
        self.observed_work = charged;
        Ok(())
    }

    fn schedule_next(
        &mut self,
        now: SimTime,
        phase: EventPhase,
        actor: ActorId,
        owned_bytes: usize,
        trace_tag: u64,
        action: HarnessAction,
    ) -> Result<(), String> {
        let deadline = now
            .checked_add(MEMORY_LINK_DELAY)
            .map_err(|error| error.to_string())?;
        self.schedule(deadline, phase, actor, owned_bytes, trace_tag, action)
    }

    fn finish_report(self) -> Result<R5BaselineReport, String> {
        if !self.close_started || !self.client_terminal_seen || !self.owner_terminal_seen {
            return Err("R5 ended before both graceful terminals".to_owned());
        }
        let client_snapshot = self
            .client_supervisor
            .as_ref()
            .ok_or_else(|| "client supervisor missing at report".to_owned())?
            .snapshot();
        let owner = self
            .owner
            .as_ref()
            .ok_or_else(|| "owner missing at report".to_owned())?;
        let owner_snapshot = owner.snapshot();
        let target = owner.target().snapshot();
        let client_port = self
            .client_flow
            .as_ref()
            .ok_or_else(|| "client flow missing at report".to_owned())?
            .probe()
            .snapshot();
        let client_port_owned_bytes = client_port.uplink_owned_bytes();
        let client_port_quiescent = client_port.is_quiescent();
        let client_leg = self.client_leg_io.counters();
        let owner_leg = self.owner_leg_io.counters();
        let outbound_queued_frames = self
            .client_outbound
            .len()
            .saturating_add(self.owner_outbound.len());
        let outbound_queued_bytes = self
            .client_outbound
            .owned_bytes()
            .saturating_add(self.owner_outbound.owned_bytes());
        let client_session = client_snapshot.session;
        let owner_session = owner_snapshot.session.session;
        let client_c2t_replay = client_session.replay_usage(Direction::ClientToTarget);
        let client_t2c_replay = client_session.replay_usage(Direction::TargetToClient);
        let owner_c2t_replay = owner_session.replay_usage(Direction::ClientToTarget);
        let owner_t2c_replay = owner_session.replay_usage(Direction::TargetToClient);
        let zero_ledger = R5ZeroLedger {
            scheduler_pending_actions: self.scheduler.pending_actions(),
            scheduler_pending_bytes: self.scheduler.pending_bytes(),
            scheduler_has_work_error: self.scheduler.work_error.is_some(),
            outbound_queued_frames,
            outbound_queued_bytes,
            client_flush_scheduled: self.client_flush_scheduled,
            owner_flush_scheduled: self.owner_flush_scheduled,
            owner_retry_scheduled: self.owner_retry_scheduled,
            target_drain_scheduled: self.target_drain_scheduled,
            target_live_flows: target.live_flows,
            target_joined_flows: target.joined_flows,
            target_buffered_bytes: target.buffered_bytes,
            target_queued_write_directives: target.queued_write_directives,
            client_live_flows: client_session.flow_count(),
            client_terminal_tombstones: client_session.terminal_tombstones(),
            client_receive_owned_bytes: client_session.receive_owned_bytes(),
            client_receive_ranges: client_session.receive_ranges(),
            client_outstanding_sink_offers: client_session.outstanding_sink_offers(),
            client_c2t_replay_bytes: client_c2t_replay.bytes(),
            client_c2t_replay_segments: client_c2t_replay.segments(),
            client_t2c_replay_bytes: client_t2c_replay.bytes(),
            client_t2c_replay_segments: client_t2c_replay.segments(),
            client_supervisor_replay_extents: client_snapshot.uplink_replay_extents,
            client_supervisor_replay_owned_bytes: client_snapshot.uplink_replay_owned_bytes,
            client_supervisor_poisoned: client_snapshot.poisoned,
            client_supervisor_quarantined_extents: client_snapshot.quarantined_uplink_extents,
            client_supervisor_quarantined_owned_bytes: client_snapshot
                .quarantined_uplink_owned_bytes,
            owner_live_flows: owner_session.flow_count(),
            owner_terminal_tombstones: owner_session.terminal_tombstones(),
            owner_receive_owned_bytes: owner_session.receive_owned_bytes(),
            owner_receive_ranges: owner_session.receive_ranges(),
            owner_outstanding_sink_offers: owner_session.outstanding_sink_offers(),
            owner_c2t_replay_bytes: owner_c2t_replay.bytes(),
            owner_c2t_replay_segments: owner_c2t_replay.segments(),
            owner_t2c_replay_bytes: owner_t2c_replay.bytes(),
            owner_t2c_replay_segments: owner_t2c_replay.segments(),
            owner_supervisor_replay_extents: owner_snapshot.session.uplink_replay_extents,
            owner_supervisor_replay_owned_bytes: owner_snapshot.session.uplink_replay_owned_bytes,
            owner_supervisor_poisoned: owner_snapshot.session.poisoned,
            owner_supervisor_quarantined_extents: owner_snapshot.session.quarantined_uplink_extents,
            owner_supervisor_quarantined_owned_bytes: owner_snapshot
                .session
                .quarantined_uplink_owned_bytes,
            client_port_owned_bytes,
            client_port_owned_segments: client_port.uplink_owned_segments(),
            client_port_pending_downlink_actions: client_port.pending_downlink_actions(),
            client_port_reserved_completion_slots: client_port.reserved_completion_slots(),
            owner_target_flows: owner_snapshot.target_flows,
            owner_target_tombstones: owner_snapshot.target_tombstones,
            owner_pending_writes: owner_snapshot.pending_writes,
            owner_pending_joins: owner_snapshot.pending_joins,
            owner_target_read_owned_bytes: owner_snapshot.target_read_owned_bytes,
            owner_target_read_owned_segments: owner_snapshot.target_read_owned_segments,
            owner_pending_effects: owner_snapshot.pending_effects,
            owner_needs_resume: owner_snapshot.needs_resume,
            owner_pending_target_completions: owner_snapshot.pending_target_completions,
            owner_aborted_effects: owner_snapshot.aborted_effects,
            owner_aborted_outputs: owner_snapshot.aborted_outputs,
        };
        if zero_ledger != R5ZeroLedger::default() {
            return Err(format!("R5 cleanup ledger is not zero: {zero_ledger:?}"));
        }
        let client_accepted_bytes = self.target_to_client.len();
        let work_budget = self.work_budget;
        let observed_work = self.observed_work;
        work_budget
            .validate_observed(observed_work)
            .map_err(|error| error.to_string())?;
        Ok(R5BaselineReport {
            work_budget,
            observed_work,
            scheduler_event_budget: self.scheduler.event_budget(),
            step_limit: work_budget.max_turns(),
            target_open_attempts: target.open_attempts,
            target_opened_flows: target.opened_flows,
            client_to_target: self.client_to_target,
            target_to_client: self.target_to_client,
            target_zero_accepts: target.write_zero,
            client_zero_accepts: u64::from(self.client_zero_done),
            target_positive_accepts: target
                .write_calls
                .saturating_sub(target.write_zero)
                .saturating_sub(target.write_would_block),
            client_positive_accepts: self.client_positive_accepts,
            target_accepted_bytes: target.write_accepted_bytes,
            client_accepted_bytes,
            client_replay_bytes: client_snapshot
                .session
                .replay_usage(Direction::ClientToTarget)
                .bytes(),
            owner_replay_bytes: owner_snapshot
                .session
                .session
                .replay_usage(Direction::TargetToClient)
                .bytes(),
            client_port_owned_bytes,
            owner_port_owned_bytes: owner_snapshot.target_read_owned_bytes,
            owner_port_owned_segments: owner_snapshot.target_read_owned_segments,
            target_live_flows: target.live_flows,
            target_tombstones: target.joined_flows,
            client_live_flows: client_snapshot.session.flow_count(),
            owner_live_flows: owner_snapshot.session.session.flow_count(),
            client_terminal_tombstones: client_snapshot.session.terminal_tombstones(),
            owner_terminal_tombstones: owner_snapshot.session.session.terminal_tombstones(),
            owner_target_flows: owner_snapshot.target_flows,
            owner_pending_writes: owner_snapshot.pending_writes,
            owner_pending_joins: owner_snapshot.pending_joins,
            client_port_quiescent,
            client_ack_frames: self.client_ack_frames,
            owner_ack_frames: self.owner_ack_frames,
            outbound_queued_frames,
            outbound_queued_bytes,
            outbound_retries: self.outbound_retries,
            scheduler_pending_actions: self.scheduler.pending_actions(),
            scheduler_pending_bytes: self.scheduler.pending_bytes(),
            zero_ledger,
            encoded_messages: client_leg
                .outbound_messages
                .saturating_add(owner_leg.outbound_messages),
            bound_messages: client_leg
                .bound_messages
                .saturating_add(owner_leg.bound_messages),
            steps: self.steps,
            trace_hash: self.scheduler.trace_hash(),
        })
    }
}

fn baseline_binding() -> Result<AttachTransportBinding, String> {
    Ok(AttachTransportBinding::new(
        OwnerIdentity::new([0x21; 32]).map_err(|error| error.to_string())?,
        AttachAlpn::new(b"mini-vpn-owned/1").map_err(|error| error.to_string())?,
        TlsExporterBinding::new([0x55; 32]).map_err(|error| error.to_string())?,
        DevicePrincipal::new([0x34; 16]).map_err(|error| error.to_string())?,
    ))
}

fn baseline_credentials() -> Result<AttachCredentials, String> {
    AttachCredentials::new(
        DeviceSecret::new([0x31; 32]).map_err(|error| error.to_string())?,
        ResumeSecret::new([0x53; 32]).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())
}

fn baseline_session_config() -> Result<SessionConfig, String> {
    SessionConfig::new(
        2,
        8,
        TcpWindowLimits::new(128, 16).map_err(|error| error.to_string())?,
        TcpWindowLimits::new(128, 16).map_err(|error| error.to_string())?,
        ReplayBudgetLimits::new(128, 16).map_err(|error| error.to_string())?,
        ReplayBudgetLimits::new(128, 16).map_err(|error| error.to_string())?,
        ReceiveBudgetLimits::new(256, 32).map_err(|error| error.to_string())?,
        128,
    )
    .map_err(|error| error.to_string())
}

fn baseline_port_config() -> Result<FlowPortConfig, String> {
    FlowPortConfig::new(128, 8, 8, 8).map_err(|error| error.to_string())
}

fn run_r5_baseline() -> Result<R5BaselineReport, String> {
    R5BaselineHarness::new()?.run()
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::owned_upstream::leg_io::{LegEndpointRole, LegIo, LegIoLimits};
    use crate::owned_upstream::two_leg::{
        EventBudget, LegId, SimTime, WireDirection, WireLane, WireRoute,
    };
    use crate::resumable::{
        AttachAlpn, AttachTransportBinding, ByteOffset, DevicePrincipal, Direction, Frame,
        LegGeneration, OwnerIdentity, Record, SessionFlowId, TlsExporterBinding,
    };

    fn test_transport_binding() -> AttachTransportBinding {
        AttachTransportBinding::new(
            OwnerIdentity::new([0x31; 32]).unwrap(),
            AttachAlpn::new(b"mini-vpn-owned/1").unwrap(),
            TlsExporterBinding::new([0x51; 32]).unwrap(),
            DevicePrincipal::new([0x53; 16]).unwrap(),
        )
    }

    struct RejectOnceTransport {
        reject_next: bool,
        accepted: Vec<Vec<u8>>,
    }

    impl EncodedLegTransport for RejectOnceTransport {
        fn send_encoded(
            &mut self,
            _now: SimTime,
            _route: WireRoute,
            _lane: WireLane,
            bytes: Vec<u8>,
        ) -> Result<(), EncodedLegTransportError> {
            if self.reject_next {
                self.reject_next = false;
                return Err(EncodedLegTransportError::Wire(
                    WireError::LiveMessageCapacityExceeded,
                ));
            }
            self.accepted.push(bytes);
            Ok(())
        }
    }

    #[test]
    fn global_scheduler_releases_exactly_one_wire_message_per_turn() {
        let mut harness = ByteHarnessScheduler::new(EventBudget::new(8, 1_024).unwrap());
        let limits = LegIoLimits::new(1_024, 4, 4_096, 4, 4_096).unwrap();
        let binding = test_transport_binding();
        let mut client =
            LegIo::for_authenticated_transport(LegId::A, LegEndpointRole::Client, binding, limits);
        let work_budget = R5ScenarioManifest::baseline().work_budget().unwrap();
        let mut observed_work = HarnessObservedWork::default();
        {
            let mut sender = harness.controller_sender(work_budget, &mut observed_work);

            for offset in [0, 1] {
                client
                    .send_frame(
                        &mut sender,
                        SimTime::ZERO,
                        Frame::try_new(
                            LegGeneration::new(1).unwrap(),
                            Record::Data {
                                flow_id: SessionFlowId::new(1).unwrap(),
                                direction: Direction::ClientToTarget,
                                offset: ByteOffset::new(offset),
                                payload: Bytes::from_static(b"x"),
                            },
                        )
                        .unwrap(),
                    )
                    .unwrap();
            }
        }

        assert_eq!(harness.pending_actions(), 2);
        let HarnessAction::WireDelivery(first) =
            harness.pop_next_checked().unwrap().unwrap().into_payload()
        else {
            panic!("wire-only fixture scheduled a non-wire action")
        };
        assert_eq!(
            first.route(),
            WireRoute::new(LegId::A, WireDirection::ClientToOwner)
        );
        assert_eq!(first.lane(), WireLane::Data);
        assert_eq!(harness.pending_actions(), 1);
        let HarnessAction::WireDelivery(second) =
            harness.pop_next_checked().unwrap().unwrap().into_payload()
        else {
            panic!("wire-only fixture scheduled a non-wire action")
        };
        assert_eq!(second.route(), first.route());
        assert_eq!(second.lane(), WireLane::Data);
        assert_eq!(harness.pending_actions(), 0);
    }

    #[test]
    fn failed_wire_schedule_does_not_commit_any_observed_work() {
        let mut harness = ByteHarnessScheduler::new(EventBudget::new(1, 1_024).unwrap());
        harness
            .schedule(
                SimTime::ZERO,
                EventPhase::SupervisorCommand,
                CLIENT_SUPERVISOR,
                0,
                0xdead,
                HarnessAction::StartOpen,
            )
            .unwrap();
        let work_budget = R5ScenarioManifest::baseline().work_budget().unwrap();
        let mut observed_work = HarnessObservedWork::default();
        let frame = Frame::try_new(
            LegGeneration::new(1).unwrap(),
            Record::Data {
                flow_id: SessionFlowId::new(1).unwrap(),
                direction: Direction::ClientToTarget,
                offset: ByteOffset::new(0),
                payload: Bytes::from_static(b"transactional"),
            },
        )
        .unwrap();
        let encoded = frame.encode().unwrap();

        let error = harness
            .controller_sender(work_budget, &mut observed_work)
            .send_encoded(
                SimTime::ZERO,
                WireRoute::new(LegId::A, WireDirection::ClientToOwner),
                WireLane::Data,
                encoded.to_vec(),
            )
            .unwrap_err();

        assert_eq!(
            error,
            EncodedLegTransportError::Wire(
                WireError::Schedule(ScheduleError::EventBudgetExceeded,)
            )
        );
        assert_eq!(observed_work, HarnessObservedWork::default());
        assert_eq!(harness.next_send_ordinal, 0);
        assert_eq!(harness.pending_actions(), 1);
    }

    #[test]
    fn failed_multi_category_wire_charge_does_not_commit_earlier_categories() {
        let work_budget = R5ScenarioManifest::baseline().work_budget().unwrap();
        let mut harness = ByteHarnessScheduler::new(work_budget.event_budget());
        let mut observed_work = HarnessObservedWork::default();
        observed_work
            .charge(
                work_budget,
                HarnessWorkCategory::WireDelivery,
                work_budget.categories().wire_delivery_turns(),
            )
            .unwrap();
        let before = observed_work;
        let frame = Frame::try_new(
            LegGeneration::new(1).unwrap(),
            Record::Data {
                flow_id: SessionFlowId::new(1).unwrap(),
                direction: Direction::ClientToTarget,
                offset: ByteOffset::new(0),
                payload: Bytes::from_static(b"transactional"),
            },
        )
        .unwrap();

        harness
            .controller_sender(work_budget, &mut observed_work)
            .send_encoded(
                SimTime::ZERO,
                WireRoute::new(LegId::A, WireDirection::ClientToOwner),
                WireLane::Data,
                frame.encode().unwrap().to_vec(),
            )
            .unwrap_err();

        assert_eq!(observed_work, before);
        assert_eq!(
            harness.take_work_error(),
            Some(CapacityError::HarnessWorkCategoryExceeded {
                category: HarnessWorkCategory::WireDelivery,
            })
        );
        assert_eq!(harness.next_send_ordinal, 0);
        assert_eq!(harness.pending_actions(), 0);
    }

    #[test]
    fn rejected_transport_send_retains_exact_bounded_frame_for_retry() {
        let limits = LegIoLimits::new(1_024, 4, 4_096, 4, 4_096).unwrap();
        let mut endpoint = LegIo::for_authenticated_transport(
            LegId::A,
            LegEndpointRole::Client,
            test_transport_binding(),
            limits,
        );
        let frame = Frame::try_new(
            LegGeneration::new(1).unwrap(),
            Record::Data {
                flow_id: SessionFlowId::new(1).unwrap(),
                direction: Direction::ClientToTarget,
                offset: ByteOffset::new(0),
                payload: Bytes::from_static(b"retained"),
            },
        )
        .unwrap();
        let encoded_len = frame.encode().unwrap().len();
        let mut queue = LegOutboundQueue::new(1, encoded_len).unwrap();
        queue.push(frame).unwrap();
        let mut transport = RejectOnceTransport {
            reject_next: true,
            accepted: Vec::new(),
        };

        assert_eq!(
            queue
                .try_flush(&mut endpoint, &mut transport, SimTime::ZERO)
                .unwrap_err(),
            LegIoError::Transport(EncodedLegTransportError::Wire(
                WireError::LiveMessageCapacityExceeded,
            ))
        );
        assert_eq!(queue.len(), 1);
        assert_eq!(queue.owned_bytes(), encoded_len);
        assert_eq!(endpoint.counters().outbound_messages, 0);
        assert!(transport.accepted.is_empty());

        assert!(
            !queue
                .try_flush(&mut endpoint, &mut transport, SimTime::ZERO)
                .unwrap()
                .unwrap()
                .is_ack
        );
        assert_eq!(queue.len(), 0);
        assert_eq!(queue.owned_bytes(), 0);
        assert_eq!(endpoint.counters().outbound_messages, 1);
        assert_eq!(transport.accepted.len(), 1);
        let decoded = Frame::decode_owned_exact(Bytes::from(transport.accepted.remove(0))).unwrap();
        assert!(matches!(
            decoded.record(),
            Record::Data { payload, .. } if payload.as_ref() == b"retained"
        ));
    }

    #[test]
    fn full_outbound_queue_returns_exact_unconsumed_frame_to_producer() {
        let first = Frame::try_new(
            LegGeneration::new(1).unwrap(),
            Record::Data {
                flow_id: SessionFlowId::new(1).unwrap(),
                direction: Direction::ClientToTarget,
                offset: ByteOffset::new(0),
                payload: Bytes::from_static(b"first"),
            },
        )
        .unwrap();
        let second = Frame::try_new(
            LegGeneration::new(1).unwrap(),
            Record::Data {
                flow_id: SessionFlowId::new(1).unwrap(),
                direction: Direction::ClientToTarget,
                offset: ByteOffset::new(5),
                payload: Bytes::from_static(b"secret-returned-frame"),
            },
        )
        .unwrap();
        let second_payload_ptr = match second.record() {
            Record::Data { payload, .. } => payload.as_ptr(),
            _ => unreachable!(),
        };
        let max_bytes = first.encode().unwrap().len() + second.encode().unwrap().len();
        let mut queue = LegOutboundQueue::new(1, max_bytes).unwrap();
        queue.push(first).unwrap();
        let before_len = queue.len();
        let before_bytes = queue.owned_bytes();

        let error = queue.push(second).unwrap_err();
        assert!(matches!(
            error.kind(),
            crate::owned_upstream::leg_io::LegOutboundQueueErrorKind::CapacityExceeded {
                queued_frames: 1,
                ..
            }
        ));
        assert!(!format!("{error:?}").contains("secret-returned-frame"));
        let returned = error.into_frame();
        assert_eq!(queue.len(), before_len);
        assert_eq!(queue.owned_bytes(), before_bytes);
        assert!(matches!(
            returned.record(),
            Record::Data {
                flow_id,
                direction: Direction::ClientToTarget,
                offset,
                payload,
            } if *flow_id == SessionFlowId::new(1).unwrap()
                && *offset == ByteOffset::new(5)
                && payload.as_ref() == b"secret-returned-frame"
                && payload.as_ptr() == second_payload_ptr
        ));
    }

    #[test]
    fn attach_action_is_charged_to_global_scheduler_byte_budget() {
        let binding = baseline_binding().unwrap();
        let request = AttachRequest::new(
            SessionId::new([0x16; 16]).unwrap(),
            LegGeneration::new(2).unwrap(),
            AttachNonce::new([0x62; 16]).unwrap(),
            VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION).unwrap(),
            FeatureOffer::new(0b0111, 0b0001).unwrap(),
        );
        let proof = baseline_credentials()
            .unwrap()
            .prove(&request, &binding)
            .unwrap();
        let frame = request.to_attach_frame(proof);
        let owned_bytes = frame.encode().unwrap().len();
        let mut scheduler =
            ByteHarnessScheduler::new(EventBudget::new(1, owned_bytes.saturating_sub(1)).unwrap());

        assert_eq!(
            scheduler
                .schedule(
                    SimTime::ZERO,
                    EventPhase::SupervisorCommand,
                    CLIENT_SUPERVISOR,
                    owned_bytes,
                    0xdead,
                    HarnessAction::StartAttach(frame),
                )
                .unwrap_err(),
            ScheduleError::ByteBudgetExceeded
        );
        assert_eq!(scheduler.pending_actions(), 0);
        assert_eq!(scheduler.pending_bytes(), 0);
    }

    #[test]
    fn production_baseline_uses_one_derived_budget_and_validates_every_work_category() {
        let budget = R5ScenarioManifest::baseline().work_budget().unwrap();
        let report = run_r5_baseline().unwrap();
        let limits = budget.categories();
        let observed = report.observed_work.categories();

        assert_eq!(report.work_budget, budget);
        assert_eq!(report.scheduler_event_budget, budget.event_budget());
        assert_eq!(report.step_limit, budget.max_turns());
        assert_eq!(budget.validate_observed(report.observed_work), Ok(()));
        assert_eq!(budget.max_wire_send_owned_bytes(), 776);
        assert_eq!(budget.max_wire_delivery_owned_bytes(), 776);
        assert_eq!(budget.max_non_wire_owned_bytes(), 6_656);
        assert_eq!(budget.max_owned_bytes(), 8_208);
        assert_eq!(budget.max_turns(), 118);
        assert_eq!(limits.wire_send_turns(), 16);
        assert_eq!(limits.wire_delivery_turns(), 16);
        assert_eq!(limits.positive_accept_turns(), 4);
        assert_eq!(limits.zero_would_block_turns(), 2);
        assert_eq!(limits.ack_construct_turns(), 8);
        assert_eq!(limits.ack_decode_turns(), 8);
        assert_eq!(limits.fixed_actor_turns(), 64);
        assert_eq!(report.observed_work.wire_send_owned_bytes(), 728);
        assert_eq!(report.observed_work.wire_delivery_owned_bytes(), 728);
        assert_eq!(report.observed_work.non_wire_owned_bytes(), 981);
        assert_eq!(report.observed_work.total_owned_bytes(), Ok(2_437));
        assert_eq!(observed.wire_send_turns(), 16);
        assert_eq!(observed.wire_delivery_turns(), 16);
        assert_eq!(observed.positive_accept_turns(), 4);
        assert_eq!(observed.zero_would_block_turns(), 2);
        assert_eq!(observed.ack_construct_turns(), 8);
        assert_eq!(observed.ack_decode_turns(), 8);
        assert_eq!(observed.fixed_actor_turns(), 55);
        assert!(report.steps <= report.step_limit);
    }

    #[test]
    fn production_byte_baseline_crosses_real_attach_wire_target_and_clean_fin() {
        let report = run_r5_baseline().unwrap();

        assert_eq!(report.target_open_attempts, 1);
        assert_eq!(report.target_opened_flows, 1);
        assert_eq!(report.client_to_target, b"client-production-bytes");
        assert_eq!(report.target_to_client, b"owner-production-bytes");
        assert_eq!(report.target_zero_accepts, 1);
        assert_eq!(report.client_zero_accepts, 1);
        assert!(report.target_positive_accepts >= 2);
        assert!(report.client_positive_accepts >= 2);
        assert_eq!(report.target_accepted_bytes, CLIENT_BYTES.len() as u64);
        assert_eq!(report.client_accepted_bytes, OWNER_BYTES.len());
        assert_eq!(report.client_replay_bytes, 0);
        assert_eq!(report.owner_replay_bytes, 0);
        assert_eq!(report.client_port_owned_bytes, 0);
        assert_eq!(report.owner_port_owned_bytes, 0);
        assert_eq!(report.owner_port_owned_segments, 0);
        assert_eq!(report.target_live_flows, 0);
        assert_eq!(report.target_tombstones, 0);
        assert_eq!(report.client_live_flows, 0);
        assert_eq!(report.owner_live_flows, 0);
        assert_eq!(report.client_terminal_tombstones, 0);
        assert_eq!(report.owner_terminal_tombstones, 0);
        assert_eq!(report.owner_target_flows, 0);
        assert_eq!(report.owner_pending_writes, 0);
        assert_eq!(report.owner_pending_joins, 0);
        assert!(report.client_port_quiescent);
        assert!(report.client_ack_frames > 0);
        assert!(report.owner_ack_frames > 0);
        assert_eq!(report.outbound_queued_frames, 0);
        assert_eq!(report.outbound_queued_bytes, 0);
        assert_eq!(report.outbound_retries, 0);
        assert_eq!(report.scheduler_pending_actions, 0);
        assert_eq!(report.scheduler_pending_bytes, 0);
        assert_eq!(report.zero_ledger, R5ZeroLedger::default());
        assert!(report.encoded_messages >= 10);
        assert!(report.bound_messages >= 10);
        assert!(report.steps < 256);
        assert_ne!(report.trace_hash, 0);
    }

    #[test]
    fn production_baseline_retries_retained_attach_after_transport_pressure() {
        let baseline = run_r5_baseline().unwrap();
        let report = R5BaselineHarness::new_with_rejection(Some(0))
            .unwrap()
            .run()
            .unwrap();
        let baseline_turns = baseline.observed_work.categories();
        let retry_turns = report.observed_work.categories();

        assert_eq!(report.outbound_retries, 1);
        assert_eq!(
            retry_turns.fixed_actor_turns(),
            baseline_turns.fixed_actor_turns() + 1
        );
        assert_eq!(
            retry_turns.wire_send_turns(),
            baseline_turns.wire_send_turns()
        );
        assert_eq!(
            retry_turns.wire_delivery_turns(),
            baseline_turns.wire_delivery_turns()
        );
        assert_eq!(
            retry_turns.positive_accept_turns(),
            baseline_turns.positive_accept_turns()
        );
        assert_eq!(
            retry_turns.zero_would_block_turns(),
            baseline_turns.zero_would_block_turns()
        );
        assert_eq!(
            retry_turns.ack_construct_turns(),
            baseline_turns.ack_construct_turns()
        );
        assert_eq!(
            retry_turns.ack_decode_turns(),
            baseline_turns.ack_decode_turns()
        );
        assert_eq!(
            report.observed_work.wire_send_owned_bytes(),
            baseline.observed_work.wire_send_owned_bytes()
        );
        assert_eq!(
            report.observed_work.wire_delivery_owned_bytes(),
            baseline.observed_work.wire_delivery_owned_bytes()
        );
        assert_eq!(
            report.observed_work.non_wire_owned_bytes(),
            baseline.observed_work.non_wire_owned_bytes()
        );
        assert_eq!(
            report.work_budget.validate_observed(report.observed_work),
            Ok(())
        );
        assert_eq!(report.target_open_attempts, 1);
        assert_eq!(report.client_to_target, CLIENT_BYTES);
        assert_eq!(report.target_to_client, OWNER_BYTES);
        assert_eq!(report.outbound_queued_frames, 0);
        assert_eq!(report.outbound_queued_bytes, 0);
        assert_eq!(report.scheduler_pending_actions, 0);
        assert_eq!(report.scheduler_pending_bytes, 0);
    }
}
