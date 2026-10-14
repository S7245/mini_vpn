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

use crate::owned_upstream::client_controller::{
    ClientControllerDriverApply, ClientInboundDisposition, ClientSessionController,
};
use crate::owned_upstream::leg::{AttachedLeg, LegBoundFrame, PendingInitialAttach};
use crate::owned_upstream::leg_io::{
    AwaitingInitialAttach, EncodedLegTransport, EncodedLegTransportError, LegIoError,
    LegOutboundQueue, OrderedDeliveryToken,
};
use crate::owned_upstream::owner_target::{
    OwnerTargetConfig, OwnerTargetExecutor, OwnerTargetOutput, OwnerTargetResume,
};
use crate::owned_upstream::reverse_switch::{
    OwnerProgressCapacity, OwnerProgressCapacityError, owner_reverse_scan_turns,
};
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
    AttachTransportBinding, DevicePrincipal, DeviceSecret, Direction, FeatureOffer, FeatureSet,
    FlowFinishReason, Frame, LegGeneration, OpenResultCode, OwnerIdentity, ReceiveBudgetLimits,
    Record, ReplayBudgetLimits, ReplayStorageLimit, ReplayStoragePlan, ResumeSecret,
    SESSION_PROTOCOL_VERSION, SessionConfig, SessionEffect, SessionEvent, SessionFlowId, SessionId,
    SessionRole, TcpWindowLimits, TerminalGrace, TlsExporterBinding, VersionRange,
};
use crate::shared::TargetAddr;

const MEMORY_LINK_DELAY: Duration = Duration::from_nanos(1);

/// Compile-time seal for the deterministic legacy harness's raw owner
/// bootstrap decomposition. This test-only module is the sole minter.
pub(super) struct LegacyOwnerBootstrapDecompositionGate {
    _private: (),
}

impl LegacyOwnerBootstrapDecompositionGate {
    fn mint() -> Self {
        Self { _private: () }
    }
}

#[allow(
    clippy::large_enum_variant,
    reason = "the scheduler retains exact non-cloneable bootstrap authority inline without unaccounted heap ownership"
)]
enum HarnessAction {
    WireDelivery {
        delivery: EncodedDelivery,
        completion: Option<OrderedDeliveryToken>,
    },
    StartAttach(PendingInitialAttach),
    OwnerBound(LegBoundFrame),
    ClientBound(LegBoundFrame),
    ClientDelivery(EncodedDelivery),
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
        matches!(self, Self::WireDelivery { .. })
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

impl ScheduledEncodedSender<'_> {
    fn schedule_encoded(
        &mut self,
        now: SimTime,
        route: WireRoute,
        lane: WireLane,
        bytes: Bytes,
        completion: Option<OrderedDeliveryToken>,
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
                HarnessAction::WireDelivery {
                    delivery,
                    completion,
                },
            ))
            .map_err(|error| EncodedLegTransportError::Wire(WireError::Schedule(error)))?;
        *self.observed_work = charged;
        self.harness.next_send_ordinal = next_ordinal;
        Ok(())
    }
}

impl EncodedLegTransport for ScheduledEncodedSender<'_> {
    fn send_encoded(
        &mut self,
        now: SimTime,
        route: WireRoute,
        lane: WireLane,
        bytes: Bytes,
    ) -> Result<(), EncodedLegTransportError> {
        self.schedule_encoded(now, route, lane, bytes, None)
    }

    fn send_attach_ordered_encoded(
        &mut self,
        now: SimTime,
        route: WireRoute,
        lane: WireLane,
        bytes: Bytes,
        completion: OrderedDeliveryToken,
    ) -> Result<(), EncodedLegTransportError> {
        self.schedule_encoded(now, route, lane, bytes, Some(completion))
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum R7CapacityError {
    ZeroMaxFlows,
    ZeroCompletedScanEpisodes,
    ZeroFlowEpisodes,
    ZeroProbeEpisodes,
    ZeroTerminalTombstones,
    OwnerProgressCapacity(OwnerProgressCapacityError),
    ScanAttemptsOverflow,
    ScanAttemptTurnsOverflow {
        turns_per_attempt: usize,
        max_attempts: usize,
    },
    DataCapacity(CapacityError),
    ApplicationAckMessagesOverflow,
    ApplicationAckBytesOverflow,
    WireMessagesOverflow,
    WireBytesOverflow,
    FixedWireRowBytesOverflow {
        kind: R7FixedWireKind,
    },
    FixedWireMessagesOverflow,
    FixedWireBytesOverflow,
    ReplacementRecoveryMessagesOverflow {
        kind: R7FixedWireKind,
    },
    WirePlatformCapacityOverflow,
    LiveByteCapacityOverflow,
    ReplayLiveByteCapacityOverflow,
    ReplayLiveByteCapacityTooSmall {
        required: usize,
        actual: usize,
    },
    SessionConfigDerivationFailed {
        role: SessionRole,
    },
    SessionConfigNotDerivedFromReplayStorage {
        role: SessionRole,
    },
    SessionTerminalTombstoneCapacityMismatch {
        client: usize,
        owner: usize,
    },
    HistoricalByteCapacityOverflow,
    FlowTerminalMessagesOverflow {
        max_flow_episodes: usize,
    },
}

/// Checked reverse-scan work over typed live-plus-terminal progress capacity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct R7OwnerScanCapacity {
    direction_entries: usize,
    turns_per_attempt: usize,
    max_attempts: usize,
    max_turns: usize,
}

impl R7OwnerScanCapacity {
    fn derive(
        progress_capacity: OwnerProgressCapacity,
        episodes: R7ScenarioEpisodeBounds,
    ) -> Result<Self, R7CapacityError> {
        let direction_entries = progress_capacity.max_entries();
        if direction_entries == 0 {
            return Err(R7CapacityError::ZeroMaxFlows);
        }
        if episodes.owner_scans == 0 {
            return Err(R7CapacityError::ZeroCompletedScanEpisodes);
        }
        let max_attempts = episodes
            .owner_scans
            .checked_add(episodes.owner_scan_revision_resets)
            .ok_or(R7CapacityError::ScanAttemptsOverflow)?;
        let turns_per_attempt = owner_reverse_scan_turns(direction_entries);
        let max_turns = turns_per_attempt.checked_mul(max_attempts).ok_or(
            R7CapacityError::ScanAttemptTurnsOverflow {
                turns_per_attempt,
                max_attempts,
            },
        )?;
        Ok(Self {
            direction_entries,
            turns_per_attempt,
            max_attempts,
            max_turns,
        })
    }
}

/// Independent cumulative work bounds for one finite physical scenario.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct R7ScenarioEpisodeBounds {
    owner_scans: usize,
    owner_scan_revision_resets: usize,
    flow_lifecycles: usize,
    probe_rounds: usize,
}

impl R7ScenarioEpisodeBounds {
    const fn new(owner_scans: usize, flow_lifecycles: usize, probe_rounds: usize) -> Self {
        Self {
            owner_scans,
            owner_scan_revision_resets: 0,
            flow_lifecycles,
            probe_rounds,
        }
    }

    const fn with_owner_scan_revision_resets(mut self, max_revision_resets: usize) -> Self {
        self.owner_scan_revision_resets = max_revision_resets;
        self
    }
}

const R7_DATA_FRAME_OVERHEAD_BYTES: usize = 37;

/// Named, non-DATA wire work required by one complete R7 physical scenario.
///
/// Each row is global to the scenario. DATA replay is separately derived from
/// the exact replay-storage plan; fixed rows are encoded once per declared
/// message and may only be duplicated by the bounded wire injector.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(usize)]
enum R7FixedWireKind {
    InitialAttach,
    InitialAttachAccepted,
    Open,
    OpenResult,
    StandbyRegistration,
    StandbyRegistrationAccepted,
    CurrentAProbe,
    CurrentAProbeAck,
    StandbyBProbe,
    StandbyBProbeAck,
    SwitchHint,
    ReplacementAttach,
    ReplacementAttachAccepted,
    GenerationStatus,
    StatusFollowUpAttach,
    FirstBApplicationAck,
    Terminal,
    ReplacementRecoveryTombstone,
    ReplacementRecoveryOpen,
    ReplacementRecoveryOpenResult,
    ReplacementRecoveryAck,
    ReplacementRecoveryClose,
}

impl R7FixedWireKind {
    const COUNT: usize = 22;

    const ALL: [Self; Self::COUNT] = [
        Self::InitialAttach,
        Self::InitialAttachAccepted,
        Self::Open,
        Self::OpenResult,
        Self::StandbyRegistration,
        Self::StandbyRegistrationAccepted,
        Self::CurrentAProbe,
        Self::CurrentAProbeAck,
        Self::StandbyBProbe,
        Self::StandbyBProbeAck,
        Self::SwitchHint,
        Self::ReplacementAttach,
        Self::ReplacementAttachAccepted,
        Self::GenerationStatus,
        Self::StatusFollowUpAttach,
        Self::FirstBApplicationAck,
        Self::Terminal,
        Self::ReplacementRecoveryTombstone,
        Self::ReplacementRecoveryOpen,
        Self::ReplacementRecoveryOpenResult,
        Self::ReplacementRecoveryAck,
        Self::ReplacementRecoveryClose,
    ];

    const fn index(self) -> usize {
        self as usize
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct R7FixedWireRow {
    kind: R7FixedWireKind,
    max_messages: usize,
    encoded_bytes_per_message: usize,
}

impl R7FixedWireRow {
    const fn new(
        kind: R7FixedWireKind,
        max_messages: usize,
        encoded_bytes_per_message: usize,
    ) -> Self {
        Self {
            kind,
            max_messages,
            encoded_bytes_per_message,
        }
    }

    fn encoded_bytes(self) -> Result<usize, R7CapacityError> {
        self.max_messages
            .checked_mul(self.encoded_bytes_per_message)
            .ok_or(R7CapacityError::FixedWireRowBytesOverflow { kind: self.kind })
    }
}

/// Non-DATA records that the two role-local reducers can replay after one or
/// more declared replacement recoveries.
///
/// DATA uses the directional replay-storage segment limits. These controls are
/// independently additive: each endpoint can retain every configured terminal
/// tombstone, while the client and owner each retain one role-specific
/// OPEN/OPEN_RESULT plus ACK/CLOSE control per live flow.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct R7ReplacementRecoveryWireCapacity {
    tombstone_messages: usize,
    open_messages: usize,
    open_result_messages: usize,
    ack_messages: usize,
    close_messages: usize,
}

impl R7ReplacementRecoveryWireCapacity {
    const ZERO: Self = Self {
        tombstone_messages: 0,
        open_messages: 0,
        open_result_messages: 0,
        ack_messages: 0,
        close_messages: 0,
    };

    fn derive(
        sessions: R7SessionCapacityWitness,
        replacement_recoveries: u8,
    ) -> Result<Self, R7CapacityError> {
        let recoveries = usize::from(replacement_recoveries);
        let both_tombstones = sessions
            .client
            .max_terminal_tombstones()
            .checked_add(sessions.owner.max_terminal_tombstones())
            .ok_or(R7CapacityError::ReplacementRecoveryMessagesOverflow {
                kind: R7FixedWireKind::ReplacementRecoveryTombstone,
            })?;
        let both_flows = sessions
            .client
            .max_flows()
            .checked_add(sessions.owner.max_flows())
            .ok_or(R7CapacityError::ReplacementRecoveryMessagesOverflow {
                kind: R7FixedWireKind::ReplacementRecoveryAck,
            })?;
        Ok(Self {
            tombstone_messages: Self::repeat(
                both_tombstones,
                recoveries,
                R7FixedWireKind::ReplacementRecoveryTombstone,
            )?,
            open_messages: Self::repeat(
                sessions.client.max_flows(),
                recoveries,
                R7FixedWireKind::ReplacementRecoveryOpen,
            )?,
            open_result_messages: Self::repeat(
                sessions.owner.max_flows(),
                recoveries,
                R7FixedWireKind::ReplacementRecoveryOpenResult,
            )?,
            ack_messages: Self::repeat(
                both_flows,
                recoveries,
                R7FixedWireKind::ReplacementRecoveryAck,
            )?,
            close_messages: Self::repeat(
                both_flows,
                recoveries,
                R7FixedWireKind::ReplacementRecoveryClose,
            )?,
        })
    }

    fn repeat(
        per_recovery: usize,
        recoveries: usize,
        kind: R7FixedWireKind,
    ) -> Result<usize, R7CapacityError> {
        per_recovery
            .checked_mul(recoveries)
            .ok_or(R7CapacityError::ReplacementRecoveryMessagesOverflow { kind })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct R7FixedWireCapacity {
    rows: [R7FixedWireRow; R7FixedWireKind::COUNT],
    max_messages: usize,
    encoded_once_bytes: usize,
}

impl R7FixedWireCapacity {
    fn derive(
        max_flow_episodes: usize,
        max_terminal_messages: usize,
        max_probe_episodes: usize,
        recovery: R7ReplacementRecoveryWireCapacity,
    ) -> Result<Self, R7CapacityError> {
        // These sizes are the exact current codec envelopes: 12-byte frame
        // header plus each fixed record body. Terminal reserves the larger
        // CLOSE representation (37B rather than the 30B RESET).
        let rows = [
            R7FixedWireRow::new(R7FixedWireKind::InitialAttach, 1, 104),
            R7FixedWireRow::new(R7FixedWireKind::InitialAttachAccepted, 1, 62),
            R7FixedWireRow::new(R7FixedWireKind::Open, max_flow_episodes, 285),
            R7FixedWireRow::new(R7FixedWireKind::OpenResult, max_flow_episodes, 30),
            R7FixedWireRow::new(R7FixedWireKind::StandbyRegistration, 1, 94),
            R7FixedWireRow::new(R7FixedWireKind::StandbyRegistrationAccepted, 1, 62),
            R7FixedWireRow::new(R7FixedWireKind::CurrentAProbe, max_probe_episodes, 60),
            R7FixedWireRow::new(R7FixedWireKind::CurrentAProbeAck, max_probe_episodes, 60),
            R7FixedWireRow::new(R7FixedWireKind::StandbyBProbe, max_probe_episodes, 60),
            R7FixedWireRow::new(R7FixedWireKind::StandbyBProbeAck, max_probe_episodes, 60),
            R7FixedWireRow::new(R7FixedWireKind::SwitchHint, 1, 79),
            R7FixedWireRow::new(R7FixedWireKind::ReplacementAttach, 1, 104),
            R7FixedWireRow::new(R7FixedWireKind::ReplacementAttachAccepted, 1, 62),
            R7FixedWireRow::new(R7FixedWireKind::GenerationStatus, 1, 60),
            R7FixedWireRow::new(R7FixedWireKind::StatusFollowUpAttach, 1, 104),
            R7FixedWireRow::new(R7FixedWireKind::FirstBApplicationAck, 1, 38),
            R7FixedWireRow::new(R7FixedWireKind::Terminal, max_terminal_messages, 37),
            R7FixedWireRow::new(
                R7FixedWireKind::ReplacementRecoveryTombstone,
                recovery.tombstone_messages,
                38,
            ),
            R7FixedWireRow::new(
                R7FixedWireKind::ReplacementRecoveryOpen,
                recovery.open_messages,
                285,
            ),
            R7FixedWireRow::new(
                R7FixedWireKind::ReplacementRecoveryOpenResult,
                recovery.open_result_messages,
                30,
            ),
            R7FixedWireRow::new(
                R7FixedWireKind::ReplacementRecoveryAck,
                recovery.ack_messages,
                38,
            ),
            R7FixedWireRow::new(
                R7FixedWireKind::ReplacementRecoveryClose,
                recovery.close_messages,
                37,
            ),
        ];
        let mut max_messages = 0_usize;
        let mut encoded_once_bytes = 0_usize;
        for row in rows {
            max_messages = max_messages
                .checked_add(row.max_messages)
                .ok_or(R7CapacityError::FixedWireMessagesOverflow)?;
            encoded_once_bytes = encoded_once_bytes
                .checked_add(row.encoded_bytes()?)
                .ok_or(R7CapacityError::FixedWireBytesOverflow)?;
        }
        Ok(Self {
            rows,
            max_messages,
            encoded_once_bytes,
        })
    }

    const fn row(self, kind: R7FixedWireKind) -> R7FixedWireRow {
        self.rows[kind.index()]
    }

    const fn max_messages(self) -> usize {
        self.max_messages
    }

    const fn encoded_once_bytes(self) -> usize {
        self.encoded_once_bytes
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct R7WireFaultBounds {
    max_positive_accept_pieces: usize,
    replay_copies: u8,
    duplicates_per_send: u8,
}

impl R7WireFaultBounds {
    const fn new(
        max_positive_accept_pieces: usize,
        replay_copies: u8,
        duplicates_per_send: u8,
    ) -> Self {
        Self {
            max_positive_accept_pieces,
            replay_copies,
            duplicates_per_send,
        }
    }
}

/// Directional DATA and application-ACK work derived from the same checked
/// replay-storage witness consumed by production session construction.
///
/// `max_segments` is authoritative. Recomputing a single ceil from bytes would
/// lose the storage plan's additive one-tail-per-live-flow allowance.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct R7DirectionalWireCapacity {
    coalesced_record_payload_bytes: usize,
    data_application_bytes: u64,
    data_logical_messages: u64,
    data_tail_segment_allowance: u64,
    data_encoded_once_bytes: u64,
    application_ack_messages: u64,
    application_ack_encoded_once_bytes: u64,
}

impl R7DirectionalWireCapacity {
    fn derive(
        coalesced_record_payload_bytes: usize,
        storage: ReplayStorageLimit,
        max_positive_accept_pieces: usize,
        application_ack_bytes_per_message: u64,
    ) -> Result<Self, R7CapacityError> {
        let data_application_bytes = u64::try_from(storage.max_bytes())
            .map_err(|_| R7CapacityError::WirePlatformCapacityOverflow)?;
        let data_logical_messages = u64::try_from(storage.max_segments())
            .map_err(|_| R7CapacityError::WirePlatformCapacityOverflow)?;
        let data_tail_segment_allowance = u64::try_from(storage.tail_segment_allowance())
            .map_err(|_| R7CapacityError::WirePlatformCapacityOverflow)?;
        let overhead = u64::try_from(R7_DATA_FRAME_OVERHEAD_BYTES)
            .map_err(|_| R7CapacityError::WirePlatformCapacityOverflow)?;
        let data_encoded_once_bytes = data_logical_messages
            .checked_mul(overhead)
            .and_then(|value| value.checked_add(data_application_bytes))
            .ok_or(R7CapacityError::WireBytesOverflow)?;
        let positive_accept_pieces = u64::try_from(max_positive_accept_pieces)
            .map_err(|_| R7CapacityError::WirePlatformCapacityOverflow)?;
        let application_ack_messages = data_logical_messages
            .checked_mul(positive_accept_pieces)
            .and_then(|value| value.checked_add(1))
            .ok_or(R7CapacityError::ApplicationAckMessagesOverflow)?;
        let application_ack_encoded_once_bytes = application_ack_messages
            .checked_mul(application_ack_bytes_per_message)
            .ok_or(R7CapacityError::ApplicationAckBytesOverflow)?;
        Ok(Self {
            coalesced_record_payload_bytes,
            data_application_bytes,
            data_logical_messages,
            data_tail_segment_allowance,
            data_encoded_once_bytes,
            application_ack_messages,
            application_ack_encoded_once_bytes,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct R7WireCapacity {
    client_to_target: R7DirectionalWireCapacity,
    target_to_client: R7DirectionalWireCapacity,
    data_encoded_once_messages: u64,
    data_encoded_once_bytes: u64,
    application_ack_messages: u64,
    application_ack_encoded_once_bytes: u64,
    fixed: R7FixedWireCapacity,
    max_send_messages: u64,
    max_send_bytes: u64,
    max_delivery_messages: u64,
    max_delivery_bytes: u64,
    max_wire_delivery_events: usize,
    max_wire_delivery_owned_bytes: usize,
}

impl R7WireCapacity {
    fn derive(
        storage: ReplayStoragePlan,
        faults: R7WireFaultBounds,
        fixed: R7FixedWireCapacity,
    ) -> Result<Self, R7CapacityError> {
        if faults.max_positive_accept_pieces == 0 {
            return Err(R7CapacityError::DataCapacity(
                CapacityError::ZeroPositiveAcceptancePieces,
            ));
        }
        if faults.replay_copies > 1 {
            return Err(R7CapacityError::DataCapacity(
                CapacityError::TooManyReplayCopies,
            ));
        }
        if faults.duplicates_per_send > 1 {
            return Err(R7CapacityError::DataCapacity(
                CapacityError::TooManyDuplicateCopies,
            ));
        }
        let fixed_messages = u64::try_from(fixed.max_messages())
            .map_err(|_| R7CapacityError::WirePlatformCapacityOverflow)?;
        let fixed_bytes = u64::try_from(fixed.encoded_once_bytes())
            .map_err(|_| R7CapacityError::WirePlatformCapacityOverflow)?;
        let application_ack_bytes_per_message = u64::try_from(
            fixed
                .row(R7FixedWireKind::FirstBApplicationAck)
                .encoded_bytes_per_message,
        )
        .map_err(|_| R7CapacityError::WirePlatformCapacityOverflow)?;
        let coalesced_record_payload_bytes = storage.geometry().coalesced_record_payload_bytes();
        let global = storage.global();
        let client_to_target = R7DirectionalWireCapacity::derive(
            coalesced_record_payload_bytes,
            global.client_to_target(),
            faults.max_positive_accept_pieces,
            application_ack_bytes_per_message,
        )?;
        let target_to_client = R7DirectionalWireCapacity::derive(
            coalesced_record_payload_bytes,
            global.target_to_client(),
            faults.max_positive_accept_pieces,
            application_ack_bytes_per_message,
        )?;
        let data_encoded_once_messages = client_to_target
            .data_logical_messages
            .checked_add(target_to_client.data_logical_messages)
            .ok_or(R7CapacityError::WireMessagesOverflow)?;
        let data_encoded_once_bytes = client_to_target
            .data_encoded_once_bytes
            .checked_add(target_to_client.data_encoded_once_bytes)
            .ok_or(R7CapacityError::WireBytesOverflow)?;
        let application_ack_messages = client_to_target
            .application_ack_messages
            .checked_add(target_to_client.application_ack_messages)
            .ok_or(R7CapacityError::ApplicationAckMessagesOverflow)?;
        let fixed_first_application_ack = u64::try_from(
            fixed
                .row(R7FixedWireKind::FirstBApplicationAck)
                .max_messages,
        )
        .map_err(|_| R7CapacityError::WirePlatformCapacityOverflow)?;
        let ordinary_application_ack_messages = application_ack_messages
            .checked_sub(fixed_first_application_ack)
            .ok_or(R7CapacityError::ApplicationAckMessagesOverflow)?;
        let ordinary_application_ack_bytes = ordinary_application_ack_messages
            .checked_mul(application_ack_bytes_per_message)
            .ok_or(R7CapacityError::ApplicationAckBytesOverflow)?;
        let application_ack_encoded_once_bytes = client_to_target
            .application_ack_encoded_once_bytes
            .checked_add(target_to_client.application_ack_encoded_once_bytes)
            .ok_or(R7CapacityError::ApplicationAckBytesOverflow)?;
        let send_factor = u64::from(faults.replay_copies) + 1;
        let delivery_factor = u64::from(faults.duplicates_per_send) + 1;
        let max_send_messages = data_encoded_once_messages
            .checked_mul(send_factor)
            .and_then(|value| value.checked_add(ordinary_application_ack_messages))
            .and_then(|value| value.checked_add(fixed_messages))
            .ok_or(R7CapacityError::WireMessagesOverflow)?;
        let max_send_bytes = data_encoded_once_bytes
            .checked_mul(send_factor)
            .and_then(|value| value.checked_add(ordinary_application_ack_bytes))
            .and_then(|value| value.checked_add(fixed_bytes))
            .ok_or(R7CapacityError::WireBytesOverflow)?;
        let max_delivery_messages = fixed_messages
            .checked_mul(delivery_factor)
            .and_then(|fixed_delivery| {
                data_encoded_once_messages
                    .checked_mul(send_factor)
                    .and_then(|value| value.checked_mul(delivery_factor))
                    .and_then(|value| {
                        ordinary_application_ack_messages
                            .checked_mul(delivery_factor)
                            .and_then(|ack_delivery| value.checked_add(ack_delivery))
                    })
                    .and_then(|value| value.checked_add(fixed_delivery))
            })
            .ok_or(R7CapacityError::WireMessagesOverflow)?;
        let max_delivery_bytes = fixed_bytes
            .checked_mul(delivery_factor)
            .and_then(|fixed_delivery| {
                data_encoded_once_bytes
                    .checked_mul(send_factor)
                    .and_then(|value| value.checked_mul(delivery_factor))
                    .and_then(|value| {
                        ordinary_application_ack_bytes
                            .checked_mul(delivery_factor)
                            .and_then(|ack_delivery| value.checked_add(ack_delivery))
                    })
                    .and_then(|value| value.checked_add(fixed_delivery))
            })
            .ok_or(R7CapacityError::WireBytesOverflow)?;
        let max_wire_delivery_events = usize::try_from(max_delivery_messages)
            .map_err(|_| R7CapacityError::WirePlatformCapacityOverflow)?;
        let max_wire_delivery_owned_bytes = usize::try_from(max_delivery_bytes)
            .map_err(|_| R7CapacityError::WirePlatformCapacityOverflow)?;
        Ok(Self {
            client_to_target,
            target_to_client,
            data_encoded_once_messages,
            data_encoded_once_bytes,
            application_ack_messages,
            application_ack_encoded_once_bytes,
            fixed,
            max_send_messages,
            max_send_bytes,
            max_delivery_messages,
            max_delivery_bytes,
            max_wire_delivery_events,
            max_wire_delivery_owned_bytes,
        })
    }
}

trait R7SlotIndex: Copy {
    fn index(self) -> usize;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(usize)]
enum R7ClientTimerSlot {
    HardLiveness,
    LocalApplicationAckStall,
    StandbyBProbe,
    TerminalGrace,
}

impl R7ClientTimerSlot {
    const COUNT: usize = 4;
    const ALL: [Self; Self::COUNT] = [
        Self::HardLiveness,
        Self::LocalApplicationAckStall,
        Self::StandbyBProbe,
        Self::TerminalGrace,
    ];
}

impl R7SlotIndex for R7ClientTimerSlot {
    fn index(self) -> usize {
        self as usize
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(usize)]
enum R7OwnerTimerSlot {
    HardLiveness,
    CurrentAProbe,
    StandbyBProbe,
    AggregateReverseStall,
    TerminalGrace,
}

impl R7OwnerTimerSlot {
    const COUNT: usize = 5;
    const ALL: [Self; Self::COUNT] = [
        Self::HardLiveness,
        Self::CurrentAProbe,
        Self::StandbyBProbe,
        Self::AggregateReverseStall,
        Self::TerminalGrace,
    ];
}

impl R7SlotIndex for R7OwnerTimerSlot {
    fn index(self) -> usize {
        self as usize
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(usize)]
enum R7ClientRootSlot {
    CurrentActor,
    StandbyActor,
    AttachCandidateActor,
    RetiredActor,
    ClassifiedBControl,
    OrderedDeliveryWait,
    HintValidator,
    ArbiterIntent,
    PendingAttach,
    TerminalStamp,
    Quarantine,
    JoinState,
    EndpointAuthority,
    SessionAuthority,
    ShutdownAdmission,
}

impl R7ClientRootSlot {
    const COUNT: usize = 15;
    const ALL: [Self; Self::COUNT] = [
        Self::CurrentActor,
        Self::StandbyActor,
        Self::AttachCandidateActor,
        Self::RetiredActor,
        Self::ClassifiedBControl,
        Self::OrderedDeliveryWait,
        Self::HintValidator,
        Self::ArbiterIntent,
        Self::PendingAttach,
        Self::TerminalStamp,
        Self::Quarantine,
        Self::JoinState,
        Self::EndpointAuthority,
        Self::SessionAuthority,
        Self::ShutdownAdmission,
    ];
}

impl R7SlotIndex for R7ClientRootSlot {
    fn index(self) -> usize {
        self as usize
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(usize)]
enum R7OwnerRootSlot {
    CurrentActor,
    StandbyActor,
    AttachCandidateActor,
    RetiredActor,
    ActiveTargetHandoff,
    RetiredTargetHandoff,
    OutputPage,
    OutputContinuation,
    ReverseProgressCursor,
    PendingProbeResponse,
    ProbeFreshness,
    PendingHint,
    GenerationStatus,
    TerminalStamp,
    LeglessHighWater,
    Quarantine,
    JoinState,
    EndpointAuthority,
    SessionAuthority,
    ShutdownAdmission,
}

impl R7OwnerRootSlot {
    const COUNT: usize = 20;
    const ALL: [Self; Self::COUNT] = [
        Self::CurrentActor,
        Self::StandbyActor,
        Self::AttachCandidateActor,
        Self::RetiredActor,
        Self::ActiveTargetHandoff,
        Self::RetiredTargetHandoff,
        Self::OutputPage,
        Self::OutputContinuation,
        Self::ReverseProgressCursor,
        Self::PendingProbeResponse,
        Self::ProbeFreshness,
        Self::PendingHint,
        Self::GenerationStatus,
        Self::TerminalStamp,
        Self::LeglessHighWater,
        Self::Quarantine,
        Self::JoinState,
        Self::EndpointAuthority,
        Self::SessionAuthority,
        Self::ShutdownAdmission,
    ];
}

impl R7SlotIndex for R7OwnerRootSlot {
    fn index(self) -> usize {
        self as usize
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct R7NamedSlotCapacity<D, const N: usize> {
    limits: [usize; N],
    _domain: std::marker::PhantomData<fn(D)>,
}

impl<D: R7SlotIndex, const N: usize> R7NamedSlotCapacity<D, N> {
    const fn new(limits: [usize; N]) -> Self {
        Self {
            limits,
            _domain: std::marker::PhantomData,
        }
    }

    fn limit(self, domain: D) -> usize {
        self.limits[domain.index()]
    }

    fn ledger(self) -> R7NamedSlotLedger<D, N> {
        R7NamedSlotLedger {
            capacity: self,
            live: [0; N],
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct R7NamedSlotLedger<D, const N: usize> {
    capacity: R7NamedSlotCapacity<D, N>,
    live: [usize; N],
}

impl<D: R7SlotIndex, const N: usize> R7NamedSlotLedger<D, N> {
    fn try_admit(&mut self, domain: D, additional: usize) -> Result<(), R7SlotAdmissionError<D>> {
        let index = domain.index();
        let current = self.live[index];
        let attempted =
            current
                .checked_add(additional)
                .ok_or(R7SlotAdmissionError::CountOverflow {
                    domain,
                    current,
                    additional,
                })?;
        let limit = self.capacity.limit(domain);
        if attempted > limit {
            return Err(R7SlotAdmissionError::CapacityExceeded {
                domain,
                attempted,
                limit,
            });
        }
        self.live[index] = attempted;
        Ok(())
    }

    fn release(&mut self, domain: D, count: usize) -> Result<(), R7SlotAdmissionError<D>> {
        let index = domain.index();
        let current = self.live[index];
        let remaining =
            current
                .checked_sub(count)
                .ok_or(R7SlotAdmissionError::ReleaseUnderflow {
                    domain,
                    current,
                    released: count,
                })?;
        self.live[index] = remaining;
        Ok(())
    }

    fn live(self, domain: D) -> usize {
        self.live[domain.index()]
    }

    fn is_exact_zero(&self) -> bool {
        self.live.iter().all(|count| *count == 0)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum R7SlotAdmissionError<D> {
    CountOverflow {
        domain: D,
        current: usize,
        additional: usize,
    },
    CapacityExceeded {
        domain: D,
        attempted: usize,
        limit: usize,
    },
    ReleaseUnderflow {
        domain: D,
        current: usize,
        released: usize,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct R7PhysicalSlotCapacities {
    client_timers: R7NamedSlotCapacity<R7ClientTimerSlot, { R7ClientTimerSlot::COUNT }>,
    owner_timers: R7NamedSlotCapacity<R7OwnerTimerSlot, { R7OwnerTimerSlot::COUNT }>,
    client_root: R7NamedSlotCapacity<R7ClientRootSlot, { R7ClientRootSlot::COUNT }>,
    owner_root: R7NamedSlotCapacity<R7OwnerRootSlot, { R7OwnerRootSlot::COUNT }>,
}

impl R7PhysicalSlotCapacities {
    fn derive(max_terminal_tombstones: usize) -> Self {
        let mut client_timer_limits = [1; R7ClientTimerSlot::COUNT];
        client_timer_limits[R7ClientTimerSlot::TerminalGrace.index()] = max_terminal_tombstones;
        let mut owner_timer_limits = [1; R7OwnerTimerSlot::COUNT];
        owner_timer_limits[R7OwnerTimerSlot::TerminalGrace.index()] = max_terminal_tombstones;
        Self {
            client_timers: R7NamedSlotCapacity::new(client_timer_limits),
            owner_timers: R7NamedSlotCapacity::new(owner_timer_limits),
            client_root: R7NamedSlotCapacity::new([1; R7ClientRootSlot::COUNT]),
            owner_root: R7NamedSlotCapacity::new([1; R7OwnerRootSlot::COUNT]),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(usize)]
enum R7LiveByteDimension {
    ActorChannels,
    ActorFifos,
    OrderedDeliveryWait,
    RootOutputFifo,
    ControllerTargetTurns,
    Replay,
    Continuation,
    PendingControl,
    DriverPort,
    Quarantine,
    TerminalResidual,
    ShutdownResidual,
}

impl R7LiveByteDimension {
    const COUNT: usize = 12;
    const ALL: [Self; Self::COUNT] = [
        Self::ActorChannels,
        Self::ActorFifos,
        Self::OrderedDeliveryWait,
        Self::RootOutputFifo,
        Self::ControllerTargetTurns,
        Self::Replay,
        Self::Continuation,
        Self::PendingControl,
        Self::DriverPort,
        Self::Quarantine,
        Self::TerminalResidual,
        Self::ShutdownResidual,
    ];

    const fn index(self) -> usize {
        self as usize
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(usize)]
enum R7HistoricalByteDimension {
    ReturnedOnRejection,
    TerminalDiscarded,
    FatalDiscarded,
    CancelledDiscarded,
    ShutdownDiscarded,
}

impl R7HistoricalByteDimension {
    const COUNT: usize = 5;
    const ALL: [Self; Self::COUNT] = [
        Self::ReturnedOnRejection,
        Self::TerminalDiscarded,
        Self::FatalDiscarded,
        Self::CancelledDiscarded,
        Self::ShutdownDiscarded,
    ];

    const fn index(self) -> usize {
        self as usize
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum R7ByteDimension {
    Live(R7LiveByteDimension),
    Historical(R7HistoricalByteDimension),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum R7ByteLedgerError {
    CounterOverflow {
        dimension: R7ByteDimension,
        current: usize,
        additional: usize,
    },
    TotalCounterOverflow {
        historical: bool,
    },
    CapacityExceeded {
        dimension: R7ByteDimension,
        attempted: usize,
        limit: usize,
    },
    SettlementUnderflow {
        dimension: R7LiveByteDimension,
        current: usize,
        settled: usize,
    },
    LiveTotalSettlementUnderflow {
        current: usize,
        settled: usize,
    },
}

/// Independently bounded live ownership and sticky settlement history.
///
/// History never contributes to the live-zero predicate. Moving live bytes to
/// history is one transactional settlement: if history cannot admit them, the
/// original live ownership remains unchanged.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct R7ByteLedgerPlan {
    live_limits: [usize; R7LiveByteDimension::COUNT],
    historical_limits: [usize; R7HistoricalByteDimension::COUNT],
    max_live_bytes: usize,
    max_historical_bytes: usize,
}

impl R7ByteLedgerPlan {
    fn new(
        live_limits: [usize; R7LiveByteDimension::COUNT],
        historical_limits: [usize; R7HistoricalByteDimension::COUNT],
    ) -> Result<Self, R7CapacityError> {
        let mut max_live_bytes = 0_usize;
        for limit in live_limits {
            max_live_bytes = max_live_bytes
                .checked_add(limit)
                .ok_or(R7CapacityError::LiveByteCapacityOverflow)?;
        }
        let mut max_historical_bytes = 0_usize;
        for limit in historical_limits {
            max_historical_bytes = max_historical_bytes
                .checked_add(limit)
                .ok_or(R7CapacityError::HistoricalByteCapacityOverflow)?;
        }
        Ok(Self {
            live_limits,
            historical_limits,
            max_live_bytes,
            max_historical_bytes,
        })
    }

    const fn live_limit(self, dimension: R7LiveByteDimension) -> usize {
        self.live_limits[dimension.index()]
    }

    const fn historical_limit(self, dimension: R7HistoricalByteDimension) -> usize {
        self.historical_limits[dimension.index()]
    }

    const fn max_live_bytes(self) -> usize {
        self.max_live_bytes
    }

    const fn max_historical_bytes(self) -> usize {
        self.max_historical_bytes
    }

    const fn ledger(self) -> R7ByteLedger {
        R7ByteLedger {
            plan: self,
            live: [0; R7LiveByteDimension::COUNT],
            historical: [0; R7HistoricalByteDimension::COUNT],
            live_total: 0,
            historical_total: 0,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct R7ByteLedger {
    plan: R7ByteLedgerPlan,
    live: [usize; R7LiveByteDimension::COUNT],
    historical: [usize; R7HistoricalByteDimension::COUNT],
    live_total: usize,
    historical_total: usize,
}

impl R7ByteLedger {
    fn try_add_live(
        &mut self,
        dimension: R7LiveByteDimension,
        additional: usize,
    ) -> Result<(), R7ByteLedgerError> {
        let index = dimension.index();
        let current = self.live[index];
        let next = Self::checked_next(
            R7ByteDimension::Live(dimension),
            current,
            additional,
            self.plan.live_limit(dimension),
        )?;
        let next_total = self
            .live_total
            .checked_add(additional)
            .ok_or(R7ByteLedgerError::TotalCounterOverflow { historical: false })?;
        self.live[index] = next;
        self.live_total = next_total;
        Ok(())
    }

    /// Releases successfully consumed ownership without fabricating discard
    /// history. Rejection/fatal/cancel/shutdown settlement uses the explicit
    /// live-to-history transaction below instead.
    fn release_live(
        &mut self,
        dimension: R7LiveByteDimension,
        bytes: usize,
    ) -> Result<(), R7ByteLedgerError> {
        let (next_live, next_live_total) = self.preflight_live_release(dimension, bytes)?;
        self.live[dimension.index()] = next_live;
        self.live_total = next_live_total;
        Ok(())
    }

    fn settle_live_to_history(
        &mut self,
        live_dimension: R7LiveByteDimension,
        historical_dimension: R7HistoricalByteDimension,
        bytes: usize,
    ) -> Result<(), R7ByteLedgerError> {
        let (next_live, next_live_total) = self.preflight_live_release(live_dimension, bytes)?;
        let (next_history, next_history_total) =
            self.preflight_historical(historical_dimension, bytes)?;
        self.live[live_dimension.index()] = next_live;
        self.live_total = next_live_total;
        self.historical[historical_dimension.index()] = next_history;
        self.historical_total = next_history_total;
        Ok(())
    }

    fn preflight_live_release(
        self,
        dimension: R7LiveByteDimension,
        bytes: usize,
    ) -> Result<(usize, usize), R7ByteLedgerError> {
        let current = self.live[dimension.index()];
        let next = current
            .checked_sub(bytes)
            .ok_or(R7ByteLedgerError::SettlementUnderflow {
                dimension,
                current,
                settled: bytes,
            })?;
        let next_total = self.live_total.checked_sub(bytes).ok_or(
            R7ByteLedgerError::LiveTotalSettlementUnderflow {
                current: self.live_total,
                settled: bytes,
            },
        )?;
        Ok((next, next_total))
    }

    fn checked_next(
        dimension: R7ByteDimension,
        current: usize,
        additional: usize,
        limit: usize,
    ) -> Result<usize, R7ByteLedgerError> {
        let attempted =
            current
                .checked_add(additional)
                .ok_or(R7ByteLedgerError::CounterOverflow {
                    dimension,
                    current,
                    additional,
                })?;
        if attempted > limit {
            return Err(R7ByteLedgerError::CapacityExceeded {
                dimension,
                attempted,
                limit,
            });
        }
        Ok(attempted)
    }

    fn preflight_historical(
        self,
        dimension: R7HistoricalByteDimension,
        additional: usize,
    ) -> Result<(usize, usize), R7ByteLedgerError> {
        let next = Self::checked_next(
            R7ByteDimension::Historical(dimension),
            self.historical[dimension.index()],
            additional,
            self.plan.historical_limit(dimension),
        )?;
        let next_total = self
            .historical_total
            .checked_add(additional)
            .ok_or(R7ByteLedgerError::TotalCounterOverflow { historical: true })?;
        Ok((next, next_total))
    }

    const fn live_bytes(self, dimension: R7LiveByteDimension) -> usize {
        self.live[dimension.index()]
    }

    const fn historical_bytes(self, dimension: R7HistoricalByteDimension) -> usize {
        self.historical[dimension.index()]
    }

    const fn live_total(self) -> usize {
        self.live_total
    }

    const fn historical_total(self) -> usize {
        self.historical_total
    }

    fn live_is_exact_zero(&self) -> bool {
        self.live_total == 0 && self.live.iter().all(|bytes| *bytes == 0)
    }
}

/// Exact reducer-capacity witness shared by both physical session roles.
///
/// The caller cannot substitute a tombstone scalar or a session configuration
/// derived from different replay geometry: both role-local configurations are
/// reconstructed from this exact storage plan before the witness is minted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct R7SessionCapacityWitness {
    replay_storage: ReplayStoragePlan,
    client: SessionConfig,
    owner: SessionConfig,
    max_terminal_tombstones: usize,
}

impl R7SessionCapacityWitness {
    fn derive(
        replay_storage: ReplayStoragePlan,
        client: SessionConfig,
        owner: SessionConfig,
    ) -> Result<Self, R7CapacityError> {
        Self::validate_role(SessionRole::Client, replay_storage, client)?;
        Self::validate_role(SessionRole::Owner, replay_storage, owner)?;
        let client_tombstones = client.max_terminal_tombstones();
        let owner_tombstones = owner.max_terminal_tombstones();
        if client_tombstones != owner_tombstones {
            return Err(R7CapacityError::SessionTerminalTombstoneCapacityMismatch {
                client: client_tombstones,
                owner: owner_tombstones,
            });
        }
        Ok(Self {
            replay_storage,
            client,
            owner,
            max_terminal_tombstones: client_tombstones,
        })
    }

    fn validate_role(
        role: SessionRole,
        replay_storage: ReplayStoragePlan,
        supplied: SessionConfig,
    ) -> Result<(), R7CapacityError> {
        let expected = SessionConfig::from_storage_plan(
            role,
            supplied.max_terminal_tombstones(),
            replay_storage,
            supplied.max_sink_offer_bytes(),
        )
        .map_err(|_| R7CapacityError::SessionConfigDerivationFailed { role })?;
        if supplied != expected {
            return Err(R7CapacityError::SessionConfigNotDerivedFromReplayStorage { role });
        }
        Ok(())
    }
}

/// Necessary-only capacity contract for the future physical R7 byte harness.
///
/// This manifest does not mint actors, hints, permits, a physical incarnation,
/// or a clean-join witness. Its scalar ledgers cannot prove that later roots
/// actually use these capacities. It only freezes finite work/ownership
/// dimensions so a later no-oracle scenario cannot rely on pooled slack or
/// unbounded history.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct R7NoOracleCapacityManifest {
    episodes: R7ScenarioEpisodeBounds,
    sessions: R7SessionCapacityWitness,
    owner_scan: R7OwnerScanCapacity,
    wire: R7WireCapacity,
    slots: R7PhysicalSlotCapacities,
    bytes: R7ByteLedgerPlan,
    required_replay_live_bytes: usize,
}

impl R7NoOracleCapacityManifest {
    fn derive(
        sessions: R7SessionCapacityWitness,
        episodes: R7ScenarioEpisodeBounds,
        wire_faults: R7WireFaultBounds,
        bytes: R7ByteLedgerPlan,
    ) -> Result<Self, R7CapacityError> {
        if episodes.flow_lifecycles == 0 {
            return Err(R7CapacityError::ZeroFlowEpisodes);
        }
        if episodes.probe_rounds == 0 {
            return Err(R7CapacityError::ZeroProbeEpisodes);
        }
        let replay_storage = sessions.replay_storage;
        let progress_capacity = OwnerProgressCapacity::checked(
            replay_storage.geometry().max_flows(),
            sessions.max_terminal_tombstones,
        )
        .map_err(R7CapacityError::OwnerProgressCapacity)?;
        let owner_scan = R7OwnerScanCapacity::derive(progress_capacity, episodes)?;
        let max_terminal_messages = episodes.flow_lifecycles.checked_mul(2).ok_or(
            R7CapacityError::FlowTerminalMessagesOverflow {
                max_flow_episodes: episodes.flow_lifecycles,
            },
        )?;
        let recovery =
            R7ReplacementRecoveryWireCapacity::derive(sessions, wire_faults.replay_copies)?;
        let fixed = R7FixedWireCapacity::derive(
            episodes.flow_lifecycles,
            max_terminal_messages,
            episodes.probe_rounds,
            recovery,
        )?;
        let wire = R7WireCapacity::derive(replay_storage, wire_faults, fixed)?;
        let global_replay = replay_storage.global();
        let required_replay_live_bytes = global_replay
            .client_to_target()
            .max_bytes()
            .checked_add(global_replay.target_to_client().max_bytes())
            .ok_or(R7CapacityError::ReplayLiveByteCapacityOverflow)?;
        let actual_replay_live_bytes = bytes.live_limit(R7LiveByteDimension::Replay);
        if actual_replay_live_bytes < required_replay_live_bytes {
            return Err(R7CapacityError::ReplayLiveByteCapacityTooSmall {
                required: required_replay_live_bytes,
                actual: actual_replay_live_bytes,
            });
        }
        let slots = R7PhysicalSlotCapacities::derive(sessions.max_terminal_tombstones);
        Ok(Self {
            episodes,
            sessions,
            owner_scan,
            wire,
            slots,
            bytes,
            required_replay_live_bytes,
        })
    }
}

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
    client_leg_io: Option<crate::owned_upstream::leg_io::LegIo>,
    owner_leg_io: crate::owned_upstream::leg_io::LegIo,
    client_outbound: Option<LegOutboundQueue>,
    owner_outbound: LegOutboundQueue,
    client_flush_scheduled: bool,
    owner_flush_scheduled: bool,
    client_pending: Option<AwaitingInitialAttach>,
    owner_pending: Option<PendingOwnerBootstrap>,
    client_controller: Option<ClientSessionController>,
    owner_attached: Option<AttachedLeg>,
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
        let attach_frame = request.to_attach_frame(proof);
        let client_pending = client_leg_io
            .established_leg()
            .begin_initial_attach(request, attach_frame)
            .map_err(|error| format!("initial ATTACH binding failed: {error:?}"))?;
        let attach_owned_bytes = client_pending
            .encoded_len()
            .map_err(|error| error.to_string())?;
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
                HarnessAction::StartAttach(client_pending),
            )
            .map_err(|error| error.to_string())?;
        observed_work = scheduled_observed;
        let client_outbound =
            LegOutboundQueue::for_leg(client_leg_io.established_leg(), 16, 16 * 1_024)
                .map_err(|error| error.to_string())?;
        let owner_outbound =
            LegOutboundQueue::for_leg(owner_leg_io.established_leg(), 16, 16 * 1_024)
                .map_err(|error| error.to_string())?;
        Ok(Self {
            work_budget,
            observed_work,
            scheduler,
            client_leg_io: Some(client_leg_io),
            owner_leg_io,
            client_outbound: Some(client_outbound),
            owner_outbound,
            client_flush_scheduled: false,
            owner_flush_scheduled: false,
            client_pending: None,
            owner_pending: Some(SessionSupervisor::prepare_owner_unproven_for_harness(
                baseline_session_config()?,
                authority,
            )),
            client_controller: None,
            owner_attached: None,
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
            HarnessAction::WireDelivery {
                delivery,
                completion,
            } => self.receive_wire(now, delivery, completion),
            HarnessAction::StartAttach(pending) => self.start_initial_attach(now, pending),
            HarnessAction::OwnerBound(received) => self.handle_owner_bound(now, received),
            HarnessAction::ClientBound(received) => self.handle_client_bound(now, received),
            HarnessAction::ClientDelivery(delivery) => self.handle_client_delivery(now, delivery),
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

    fn receive_wire(
        &mut self,
        now: SimTime,
        delivery: EncodedDelivery,
        completion: Option<OrderedDeliveryToken>,
    ) -> Result<(), String> {
        let retained_bytes = delivery.bytes().len();
        match delivery.route().direction() {
            WireDirection::ClientToOwner => {
                let received = self
                    .owner_leg_io
                    .receive_delivery(delivery)
                    .map_err(|error| error.to_string())?;
                if let Some(completion) = completion {
                    completion.mark_delivered();
                }
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
                if self.client_controller.is_some() {
                    if let Some(completion) = completion {
                        completion.mark_delivered();
                    }
                    return self.schedule(
                        now,
                        EventPhase::SupervisorCommand,
                        CLIENT_SUPERVISOR,
                        retained_bytes,
                        0x202,
                        HarnessAction::ClientDelivery(delivery),
                    );
                }
                let received = self
                    .client_leg_io
                    .as_mut()
                    .ok_or_else(|| "raw client endpoint disappeared before bootstrap".to_owned())?
                    .receive_delivery(delivery)
                    .map_err(|error| error.to_string())?;
                if let Some(completion) = completion {
                    completion.mark_delivered();
                }
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

    fn start_initial_attach(
        &mut self,
        now: SimTime,
        pending: PendingInitialAttach,
    ) -> Result<(), String> {
        if self.client_pending.is_some() {
            return Err("initial client ATTACH was scheduled twice".to_owned());
        }
        let awaiting = pending
            .enqueue(
                self.client_outbound
                    .as_mut()
                    .ok_or_else(|| "raw client queue disappeared before bootstrap".to_owned())?,
            )
            .map_err(|error| format!("initial ATTACH queue rejected: {error:?}"))?;
        self.client_pending = Some(awaiting);
        self.schedule_client_outbound_flush(now)
    }

    fn handle_owner_bound(&mut self, now: SimTime, received: LegBoundFrame) -> Result<(), String> {
        if self.owner.is_none() {
            let pending = self
                .owner_pending
                .as_mut()
                .ok_or_else(|| "owner bootstrap state disappeared".to_owned())?;
            let publication = pending
                .accept(
                    self.owner_leg_io.established_leg(),
                    received,
                    &mut self.owner_outbound,
                )
                .map_err(|error| error.to_string())?;
            let bootstrap = publication
                .enqueue_acceptance(&mut self.owner_outbound)
                .map_err(|error| error.to_string())?;
            let legacy_gate = LegacyOwnerBootstrapDecompositionGate::mint();
            let (supervisor, attached) = bootstrap.into_legacy_parts(&legacy_gate);
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
            self.schedule_owner_outbound_flush(now)?;
            return Ok(());
        }

        let event = self
            .owner_attached
            .as_ref()
            .ok_or_else(|| "owner exact attached leg disappeared".to_owned())?
            .accept_frame(received)
            .map_err(|error| error.to_string())?;
        self.charge_ack_decode(matches!(
            &event,
            SessionEvent::PeerFrame { frame, .. }
                if matches!(frame.record(), Record::Ack { .. })
        ))?;
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
        let pending = self
            .client_pending
            .take()
            .ok_or_else(|| "raw client-bound frame arrived after bootstrap".to_owned())?;
        let accepted = pending
            .validate_response(
                self.client_outbound
                    .as_ref()
                    .ok_or_else(|| "raw client queue disappeared before validation".to_owned())?,
                received,
            )
            .map_err(|error| format!("initial response rejected: {error:?}"))?;
        let config = baseline_session_config()?;
        let io = self
            .client_leg_io
            .take()
            .ok_or_else(|| "raw client endpoint disappeared before bootstrap".to_owned())?;
        let queue = self
            .client_outbound
            .take()
            .ok_or_else(|| "raw client queue disappeared before bootstrap".to_owned())?;
        let mut controller =
            ClientSessionController::bootstrap_initial(config, accepted, io, queue)
                .map_err(|error| format!("{error:?}"))?;
        let port_factory = controller
            .mint_tcp_port_factory(baseline_port_config()?)
            .map_err(|error| error.to_string())?;
        self.client_port_factory = Some(port_factory);
        self.client_controller = Some(controller);
        self.schedule_next(
            now,
            EventPhase::SupervisorCommand,
            CLIENT_SUPERVISOR,
            0,
            0x301,
            HarnessAction::StartOpen,
        )
    }

    fn handle_client_delivery(
        &mut self,
        now: SimTime,
        delivery: EncodedDelivery,
    ) -> Result<(), String> {
        let is_ack = matches!(
            Frame::decode_exact(delivery.bytes())
                .map_err(|error| error.to_string())?
                .record(),
            Record::Ack { .. }
        );
        self.charge_ack_decode(is_ack)?;
        let disposition = self
            .client_controller
            .as_mut()
            .ok_or_else(|| "client controller disappeared".to_owned())?
            .receive_current(delivery, FeatureSet::new(0b0111))
            .map_err(|error| error.to_string())?;
        match disposition {
            ClientInboundDisposition::Applied { effects, .. } => {
                self.process_client_effects(now, effects)
            }
            ClientInboundDisposition::ReducerRejected(error) => Err(error.to_string()),
            ClientInboundDisposition::Blocked(_)
            | ClientInboundDisposition::ProbeAcked { .. }
            | ClientInboundDisposition::Retired(_)
            | ClientInboundDisposition::Rejected => {
                Err("baseline client delivery was not applied by its controller".to_owned())
            }
        }
    }

    fn start_open(&mut self, now: SimTime) -> Result<(), String> {
        let effects = self
            .client_controller
            .as_mut()
            .ok_or_else(|| "client controller missing at OPEN".to_owned())?
            .open_local(self.target.clone())
            .ok_or_else(|| "client controller has no active leg at OPEN".to_owned())?
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
            .try_recv_next_for_client_controller()
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "scheduled client driver turn had no input".to_owned())?;
        let applied = self
            .client_controller
            .as_mut()
            .ok_or_else(|| "client controller missing".to_owned())?
            .apply_driver_input(input);
        let effects = match applied {
            ClientControllerDriverApply::Applied(effects) => effects,
            ClientControllerDriverApply::Deferred { block, .. } => {
                return Err(format!("client reducer deferred driver input: {block:?}"));
            }
            ClientControllerDriverApply::Failed(error) => return Err(error.to_string()),
        };
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
            .client_controller
            .as_mut()
            .ok_or_else(|| "client controller missing at terminal expiry".to_owned())?
            .expire_terminal_grace(terminal)
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
        self.client_controller
            .as_ref()
            .map(|controller| {
                controller
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

    fn charge_ack_decode(&mut self, is_ack: bool) -> Result<(), String> {
        if is_ack {
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
        self.client_controller
            .as_mut()
            .ok_or_else(|| "client controller missing at outbound admission".to_owned())?
            .enqueue_current_frame(frame)
            .ok_or_else(|| "client controller has no active outbound queue".to_owned())?
            .map_err(|error| error.to_string())?;
        let before = self.observed_work;
        self.observed_work = charged;
        if let Err(error) = self.schedule_client_outbound_flush(now) {
            self.observed_work = before;
            return Err(error);
        }
        Ok(())
    }

    fn schedule_client_outbound_flush(&mut self, now: SimTime) -> Result<(), String> {
        if self.client_flush_scheduled {
            return Ok(());
        }
        self.schedule_next(
            now,
            EventPhase::SupervisorCommand,
            CLIENT_SUPERVISOR,
            0,
            0x801,
            HarnessAction::FlushClientOutbound,
        )?;
        self.client_flush_scheduled = true;
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
        let before = self.observed_work;
        self.observed_work = charged;
        if let Err(error) = self.schedule_owner_outbound_flush(now) {
            self.observed_work = before;
            return Err(error);
        }
        Ok(())
    }

    fn schedule_owner_outbound_flush(&mut self, now: SimTime) -> Result<(), String> {
        if self.owner_flush_scheduled {
            return Ok(());
        }
        self.schedule_next(
            now,
            EventPhase::SupervisorCommand,
            OWNER_SUPERVISOR,
            0,
            0x802,
            HarnessAction::FlushOwnerOutbound,
        )?;
        self.owner_flush_scheduled = true;
        Ok(())
    }

    fn flush_client_outbound(&mut self, now: SimTime) -> Result<(), String> {
        self.client_flush_scheduled = false;
        let work_budget = self.work_budget;
        let result = if let Some(controller) = self.client_controller.as_mut() {
            controller.try_flush_current(
                &mut self
                    .scheduler
                    .controller_sender(work_budget, &mut self.observed_work),
                now,
            )
        } else {
            let (queue, endpoint, scheduler, observed_work) = (
                &mut self.client_outbound,
                &mut self.client_leg_io,
                &mut self.scheduler,
                &mut self.observed_work,
            );
            queue
                .as_mut()
                .ok_or_else(|| "raw client queue disappeared before flush".to_owned())?
                .try_flush(
                    endpoint
                        .as_mut()
                        .ok_or_else(|| "raw client endpoint disappeared before flush".to_owned())?,
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
        let queue_is_empty = if let Some(controller) = self.client_controller.as_ref() {
            controller.current_queue_is_empty().unwrap_or(true)
        } else {
            self.client_outbound
                .as_ref()
                .is_none_or(LegOutboundQueue::is_empty)
        };
        if !queue_is_empty {
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
        let client_controller = self
            .client_controller
            .as_ref()
            .ok_or_else(|| "client controller missing at report".to_owned())?;
        let client_snapshot = client_controller.snapshot();
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
        let client_leg = client_controller
            .current_counters()
            .ok_or_else(|| "client controller lost its current counters".to_owned())?;
        let owner_leg = self.owner_leg_io.counters();
        let outbound_queued_frames = client_controller
            .current_queued_frames()
            .ok_or_else(|| "client controller lost its current queue".to_owned())?
            .saturating_add(self.owner_outbound.len());
        let outbound_queued_bytes = client_controller
            .current_queued_bytes()
            .ok_or_else(|| "client controller lost its current queue".to_owned())?
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
mod tests;
