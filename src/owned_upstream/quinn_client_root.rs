//! Structured production owner for one resumable Quinn client session.
//!
//! This R6d owner holds the checked plan, sole composition, sole TCP factory,
//! bounded command/driver/effect/timer registries, lifecycle publication, and
//! structural shutdown. TCP OPEN and logical flow shutdown stay behind this
//! same owner; the public TUN entry remains deliberately unchanged.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, VecDeque};
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

#[cfg(test)]
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering};
#[cfg(test)]
use std::sync::{Mutex, mpsc as std_mpsc};

use thiserror::Error;
use tokio::runtime::Handle;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;

use super::client_controller::{
    ClientControllerDriverApply, ClientControllerObservedDriverApply,
    ClientControllerOwnershipLedger, ClientControllerReducerObservations, ClientInboundObservation,
    ClientRecoveryTailSettlement, ClientRecoveryTailSettlementBlock,
};
use super::leg_io::{
    EncodedLegTransportError, LegIoError, LegOutboundQueueErrorKind, OrderedResetDeadlineCommit,
};
use super::quinn_leg_composition::{
    CurrentQuinnActorJoined, QuinnClientSessionComposition, QuinnCompositionProgressError,
    QuinnCurrentEnqueueErrorKind, QuinnCurrentFlushDisposition, QuinnCurrentFlushError,
    QuinnCurrentInboundDrive, QuinnCurrentJoinError, QuinnCurrentProbeDeadline,
    QuinnCurrentProbeQueued, QuinnCurrentProbeSchedulerAuthority, QuinnCurrentTerminalDrive,
    QuinnCurrentTerminalError, QuinnPreparedLocalTcpOpenFailureKind,
    QuinnRecoveryTailSettlementError, QuinnTerminalGraceTimer, QuinnTerminalGraceTimerDrive,
};
use super::reverse_switch::{ClientLocalAckStallError, ClientLocalAckStallSource};
use super::session_clock::{SessionClockError, SessionMonotonicClock};
use super::supervisor::{SessionSupervisorSnapshot, TcpPortFactoryMintError};
use super::tcp::{
    ClientControllerDriverInput, FlowPortConfig, FlowPortError, ResumableTcpDriver,
    ResumableTcpFlow, ResumableTcpPortFactory, TcpDriverLocalAbandonSeal,
};
use super::two_leg::SimTime;
use crate::resumable::{
    DirectionalRates, LegGeneration, ProtocolError, Record, RecoveryBudget, RecoveryBudgetError,
    RecoveryBudgetSpec, ReplayCapacityError, ReplayCapacitySpec, ReplayStorageGeometry,
    ReplayStoragePlan, SessionConfig, SessionConfigError, SessionEffect, SessionFlowId, SessionId,
    SessionRole, normalize_target_for_owned_command,
};
use crate::shared::TargetAddr;

// One exact scheduler authority may exist for current A and one independent
// non-authoritative wake witness may exist for registered B.  The domains are
// stored in different nominal types and named fields below; the arithmetic is
// a capacity proof, not an invitation to pool them by scalar correlation.
const ROOT_CURRENT_A_RECOVERY_TIMER_SLOTS: usize = 1;
const ROOT_STANDBY_B_RECOVERY_WAKE_SLOTS: usize = 1;
// One aggregate client-to-owner application-ACK stall episode exists per
// session, independent of the physical A/B recovery scheduler domains.
const ROOT_LOCAL_ACK_STALL_TIMER_SLOTS: usize = 1;
const ROOT_RECOVERY_TIMER_SLOTS: usize =
    ROOT_CURRENT_A_RECOVERY_TIMER_SLOTS + ROOT_STANDBY_B_RECOVERY_WAKE_SLOTS;
// `SessionConfig::client_incremental_effect_bound` currently proves that one
// reducer turn emits at most two effects. Keep the stack-prepared terminal
// batch tied to that proof instead of allocating on the root hot path.
const ROOT_INCREMENTAL_EFFECT_SLOTS: usize = 2;
// One scan consumes at most one bounded actor record. Keep the synchronous
// category turn small enough that shutdown is re-observed after finite work,
// independently of caller-provided queue and replay capacities.
const MAX_WORK_PER_TURN: usize = 64;

/// Fully checked timing, storage, reducer, mailbox, and scheduler plan. The
/// value is built before a physical actor is created and later compared with
/// the exact config already installed in the composition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct QuinnClientRootPlan {
    recovery: RecoveryBudget,
    storage: ReplayStoragePlan,
    session: SessionConfig,
    terminal_grace: Duration,
    flow_port: FlowPortConfig,
    command_capacity: usize,
    work_budget: usize,
    incremental_effect_bound: usize,
    pending_effect_capacity: usize,
    timer_capacity: usize,
}

impl QuinnClientRootPlan {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn preflight(
        recovery: RecoveryBudgetSpec,
        per_flow_rates: DirectionalRates,
        aggregate_rates: DirectionalRates,
        geometry: ReplayStorageGeometry,
        max_terminal_tombstones: usize,
        terminal_grace: Duration,
        max_sink_offer_bytes: usize,
        flow_port: FlowPortConfig,
        command_capacity: usize,
        work_budget: usize,
    ) -> Result<Self, QuinnClientRootPlanError> {
        if terminal_grace.is_zero() {
            return Err(QuinnClientRootPlanError::ZeroTerminalGrace);
        }
        if u64::try_from(terminal_grace.as_nanos()).is_err() {
            return Err(QuinnClientRootPlanError::TerminalGraceOutsideSimTime {
                value: terminal_grace,
            });
        }
        if command_capacity == 0 {
            return Err(QuinnClientRootPlanError::ZeroCommandCapacity);
        }
        if command_capacity > tokio::sync::Semaphore::MAX_PERMITS {
            return Err(QuinnClientRootPlanError::CommandCapacityTooLarge {
                value: command_capacity,
                max: tokio::sync::Semaphore::MAX_PERMITS,
            });
        }
        if work_budget == 0 {
            return Err(QuinnClientRootPlanError::ZeroWorkBudget);
        }
        if work_budget > MAX_WORK_PER_TURN {
            return Err(QuinnClientRootPlanError::WorkBudgetTooLarge {
                value: work_budget,
                max: MAX_WORK_PER_TURN,
            });
        }

        let recovery = recovery.derive()?;
        let storage =
            ReplayCapacitySpec::from_recovery_budget(per_flow_rates, aggregate_rates, recovery)?
                .derive()?
                .storage_plan(geometry)?;
        if storage.horizon() != recovery.horizon() {
            return Err(QuinnClientRootPlanError::RecoveryStorageHorizonMismatch);
        }
        let session = SessionConfig::from_storage_plan(
            SessionRole::Client,
            max_terminal_tombstones,
            storage,
            max_sink_offer_bytes,
        )?;
        let available_uplink = storage.per_flow().client_to_target().max_bytes();
        if flow_port.uplink_byte_capacity() > available_uplink {
            return Err(QuinnClientRootPlanError::FlowUplinkBytesExceedStorage {
                requested: flow_port.uplink_byte_capacity(),
                available: available_uplink,
            });
        }
        let incremental_effect_bound = session.client_incremental_effect_bound();
        if incremental_effect_bound > ROOT_INCREMENTAL_EFFECT_SLOTS {
            return Err(QuinnClientRootPlanError::IncrementalEffectBoundTooLarge {
                value: incremental_effect_bound,
                max: ROOT_INCREMENTAL_EFFECT_SLOTS,
            });
        }
        let pending_effect_capacity = session
            .client_pending_effect_bound()
            .ok_or(QuinnClientRootPlanError::PendingEffectBoundOverflow)?;
        let timer_capacity = ROOT_RECOVERY_TIMER_SLOTS
            .checked_add(ROOT_LOCAL_ACK_STALL_TIMER_SLOTS)
            .ok_or(QuinnClientRootPlanError::TimerCapacityOverflow)?
            .checked_add(session.max_terminal_tombstones())
            .ok_or(QuinnClientRootPlanError::TimerCapacityOverflow)?;
        let _root_slot_envelope = command_capacity
            .checked_add(work_budget)
            .and_then(|total| total.checked_add(pending_effect_capacity))
            .and_then(|total| total.checked_add(geometry.max_flows()))
            .and_then(|total| total.checked_add(timer_capacity))
            .ok_or(QuinnClientRootPlanError::RootSlotEnvelopeOverflow)?;

        Ok(Self {
            recovery,
            storage,
            session,
            terminal_grace,
            flow_port,
            command_capacity,
            work_budget,
            incremental_effect_bound,
            pending_effect_capacity,
            timer_capacity,
        })
    }

    pub(crate) const fn recovery(self) -> RecoveryBudget {
        self.recovery
    }

    pub(crate) const fn storage(self) -> ReplayStoragePlan {
        self.storage
    }

    pub(crate) const fn session_config(self) -> SessionConfig {
        self.session
    }

    pub(crate) const fn terminal_grace(self) -> Duration {
        self.terminal_grace
    }

    pub(crate) const fn flow_port_config(self) -> FlowPortConfig {
        self.flow_port
    }

    pub(crate) const fn command_capacity(self) -> usize {
        self.command_capacity
    }

    pub(crate) const fn work_budget(self) -> usize {
        self.work_budget
    }

    pub(crate) const fn incremental_effect_bound(self) -> usize {
        self.incremental_effect_bound
    }

    pub(crate) const fn pending_effect_capacity(self) -> usize {
        self.pending_effect_capacity
    }

    pub(crate) const fn timer_capacity(self) -> usize {
        self.timer_capacity
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub(crate) enum QuinnClientRootPlanError {
    #[error(transparent)]
    Recovery(#[from] RecoveryBudgetError),
    #[error(transparent)]
    Replay(#[from] ReplayCapacityError),
    #[error(transparent)]
    Session(#[from] SessionConfigError),
    #[error("root command capacity must be non-zero")]
    ZeroCommandCapacity,
    #[error("root command capacity {value} exceeds Tokio's bound {max}")]
    CommandCapacityTooLarge { value: usize, max: usize },
    #[error("root work budget must be non-zero")]
    ZeroWorkBudget,
    #[error("root work budget {value} exceeds the proved per-turn bound {max}")]
    WorkBudgetTooLarge { value: usize, max: usize },
    #[error("recovery and replay storage plans do not carry the same horizon")]
    RecoveryStorageHorizonMismatch,
    #[error("terminal grace must be non-zero")]
    ZeroTerminalGrace,
    #[error("terminal grace {value:?} exceeds the root SimTime domain")]
    TerminalGraceOutsideSimTime { value: Duration },
    #[error("flow uplink capacity {requested} exceeds checked storage {available}")]
    FlowUplinkBytesExceedStorage { requested: usize, available: usize },
    #[error("client pending-effect bound overflows usize")]
    PendingEffectBoundOverflow,
    #[error("client incremental-effect bound {value} exceeds root stack capacity {max}")]
    IncrementalEffectBoundTooLarge { value: usize, max: usize },
    #[error("root recovery plus terminal timer capacity overflows usize")]
    TimerCapacityOverflow,
    #[error("root bounded-slot envelope overflows usize")]
    RootSlotEnvelopeOverflow,
}

struct QuinnClientRootIdentity;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QuinnClientRootPhase {
    Starting,
    Running,
    ShuttingDown,
    Joined,
    FailedJoined,
    TaskLost,
}

/// One shared lifecycle publisher that remains observable even if the root
/// task is dropped before its first poll. Transitions are serialized by the
/// watch sender itself; handles carry no composition or shutdown authority.
struct RootLifecycle {
    phase: watch::Sender<QuinnClientRootPhase>,
}

impl RootLifecycle {
    fn new() -> Arc<Self> {
        let (phase, _initial_receiver) = watch::channel(QuinnClientRootPhase::Starting);
        Arc::new(Self { phase })
    }

    fn phase(&self) -> QuinnClientRootPhase {
        *self.phase.borrow()
    }

    fn subscribe(&self) -> watch::Receiver<QuinnClientRootPhase> {
        self.phase.subscribe()
    }

    fn transition(
        &self,
        expected: QuinnClientRootPhase,
        next: QuinnClientRootPhase,
    ) -> Result<(), QuinnClientRootPhase> {
        let mut observed = expected;
        let changed = self.phase.send_if_modified(|phase| {
            observed = *phase;
            if *phase != expected {
                return false;
            }
            *phase = next;
            true
        });
        changed.then_some(()).ok_or(observed)
    }

    fn begin_shutdown(&self) -> Result<(), QuinnClientRootPhase> {
        let mut observed = self.phase();
        let changed = self.phase.send_if_modified(|phase| {
            observed = *phase;
            if *phase == QuinnClientRootPhase::ShuttingDown {
                return false;
            }
            if !matches!(
                *phase,
                QuinnClientRootPhase::Starting | QuinnClientRootPhase::Running
            ) {
                return false;
            }
            *phase = QuinnClientRootPhase::ShuttingDown;
            true
        });
        if changed || observed == QuinnClientRootPhase::ShuttingDown {
            Ok(())
        } else {
            Err(observed)
        }
    }

    /// Runs one bounded synchronous live turn under the same watch lock used
    /// by the shutdown CAS. The callback must not await, block, or re-enter
    /// lifecycle mutation. Shutdown therefore linearizes either before the
    /// whole turn or after it; a phase check cannot race the turn itself.
    fn transact_if_running<T>(
        &self,
        transaction: impl FnOnce() -> T,
    ) -> Result<T, QuinnClientRootPhase> {
        let phase = self.phase.borrow();
        if *phase != QuinnClientRootPhase::Running {
            return Err(*phase);
        }
        Ok(transaction())
    }

    /// Input-preserving form for work already dequeued by an async wait. If
    /// shutdown won, the exact input returns without invoking the callback.
    fn transact_if_running_with_input<I, T>(
        &self,
        input: I,
        transaction: impl FnOnce(I) -> T,
    ) -> Result<T, (I, QuinnClientRootPhase)> {
        let phase = self.phase.borrow();
        if *phase != QuinnClientRootPhase::Running {
            return Err((input, *phase));
        }
        Ok(transaction(input))
    }

    fn publish_task_lost(&self) {
        let _changed = self.phase.send_if_modified(|phase| {
            if matches!(
                *phase,
                QuinnClientRootPhase::Joined
                    | QuinnClientRootPhase::FailedJoined
                    | QuinnClientRootPhase::TaskLost
            ) {
                return false;
            }
            *phase = QuinnClientRootPhase::TaskLost;
            true
        });
    }

    fn publish_failed_joined(&self) {
        let _changed = self.phase.send_if_modified(|phase| {
            if matches!(
                *phase,
                QuinnClientRootPhase::Joined
                    | QuinnClientRootPhase::FailedJoined
                    | QuinnClientRootPhase::TaskLost
            ) {
                return false;
            }
            *phase = QuinnClientRootPhase::FailedJoined;
            true
        });
    }
}

/// Linearizes shutdown at the lifecycle CAS, then publishes the watch signal
/// that wakes the root task. Ready arbitration may run between these two
/// operations, so it must treat `ShuttingDown` itself as authoritative rather
/// than requiring the wake signal to have become visible already.
fn begin_root_shutdown_signal(
    lifecycle: &RootLifecycle,
    shutdown: &watch::Sender<RootShutdownSignal>,
    signal: RootShutdownSignal,
    after_lifecycle: impl FnOnce(),
) -> (Result<(), QuinnClientRootPhase>, RootShutdownSignal) {
    let phase = lifecycle.begin_shutdown();
    after_lifecycle();
    let previous = shutdown.send_replace(signal);
    (phase, previous)
}

/// Armed before spawn so dropping an unpolled, panicked, or aborted root
/// future publishes TaskLost instead of leaving observers in Starting/Running.
struct RootTaskLifecycleGuard {
    lifecycle: Arc<RootLifecycle>,
    armed: bool,
}

impl RootTaskLifecycleGuard {
    fn new(lifecycle: Arc<RootLifecycle>) -> Self {
        Self {
            lifecycle,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for RootTaskLifecycleGuard {
    fn drop(&mut self) {
        if self.armed {
            self.lifecycle.publish_task_lost();
        }
    }
}

enum QuinnClientRootCommand {
    OpenTcp {
        target: TargetAddr,
        reply: oneshot::Sender<Result<ResumableTcpFlow, QuinnClientRootOpenFailureKind>>,
    },
}

#[cfg(test)]
#[derive(Default)]
struct RootRunProbe {
    live_scans: AtomicUsize,
    live_waits: AtomicUsize,
    shutdown_cleanup_started: AtomicBool,
}

#[cfg(test)]
impl RootRunProbe {
    fn record_live_scan(&self) {
        self.live_scans.fetch_add(1, AtomicOrdering::Release);
    }

    fn record_live_wait(&self) {
        self.live_waits.fetch_add(1, AtomicOrdering::Release);
    }

    fn mark_shutdown_cleanup_started(&self) {
        self.shutdown_cleanup_started
            .store(true, AtomicOrdering::Release);
    }

    fn live_scans(&self) -> usize {
        self.live_scans.load(AtomicOrdering::Acquire)
    }

    fn live_waits(&self) -> usize {
        self.live_waits.load(AtomicOrdering::Acquire)
    }

    fn shutdown_cleanup_started(&self) -> bool {
        self.shutdown_cleanup_started.load(AtomicOrdering::Acquire)
    }
}

/// Deterministic test-only barrier after the live-loop fast checks and before
/// the authoritative lifecycle transaction for one selected work category.
#[cfg(test)]
#[derive(Default)]
struct RootLiveCategoryHook {
    pause: Mutex<Option<RootLiveCategoryPause>>,
}

#[cfg(test)]
struct RootLiveCategoryPause {
    boundary: RootLiveBoundary,
    entered: std_mpsc::SyncSender<()>,
    release: std_mpsc::Receiver<()>,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RootLiveBoundary {
    Category(RootWorkCategory),
    SelectedCommand,
}

#[cfg(test)]
pub(crate) struct RootLiveCategoryControl {
    entered: std_mpsc::Receiver<()>,
    release: std_mpsc::SyncSender<()>,
}

#[cfg(test)]
impl RootLiveCategoryHook {
    fn arm(&self, boundary: RootLiveBoundary) -> RootLiveCategoryControl {
        let (entered_tx, entered_rx) = std_mpsc::sync_channel(0);
        let (release_tx, release_rx) = std_mpsc::sync_channel(0);
        let mut pause = self
            .pause
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(
            pause.is_none(),
            "only one live-category barrier may be armed"
        );
        *pause = Some(RootLiveCategoryPause {
            boundary,
            entered: entered_tx,
            release: release_rx,
        });
        RootLiveCategoryControl {
            entered: entered_rx,
            release: release_tx,
        }
    }

    fn pause_if_armed(&self, boundary: RootLiveBoundary) {
        let pause = {
            let mut pause = self
                .pause
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if pause
                .as_ref()
                .is_some_and(|armed| armed.boundary == boundary)
            {
                pause.take()
            } else {
                None
            }
        };
        let Some(pause) = pause else {
            return;
        };
        let _entered = pause.entered.send(());
        let _released = pause.release.recv();
    }
}

#[cfg(test)]
impl RootLiveCategoryControl {
    pub(crate) fn wait_until_blocked(&self) {
        self.entered
            .recv_timeout(Duration::from_secs(3))
            .expect("root live-category barrier disappeared before entry");
    }

    pub(crate) fn release(&self) {
        self.release
            .send(())
            .expect("root live-category barrier disappeared before release");
    }
}

#[cfg(test)]
impl Drop for RootLiveCategoryControl {
    fn drop(&mut self) {
        let _released = self.release.try_send(());
    }
}

#[cfg(test)]
#[derive(Default)]
struct RootOpenReplyPublicationHook {
    pause: Mutex<Option<RootOpenReplyPublicationPause>>,
}

#[cfg(test)]
struct RootOpenReplyPublicationPause {
    entered: std_mpsc::SyncSender<()>,
    release: std_mpsc::Receiver<()>,
}

#[cfg(test)]
pub(crate) struct RootOpenReplyPublicationControl {
    entered: std_mpsc::Receiver<()>,
    release: std_mpsc::SyncSender<()>,
}

#[cfg(test)]
impl RootOpenReplyPublicationHook {
    fn arm(&self) -> RootOpenReplyPublicationControl {
        let (entered_tx, entered_rx) = std_mpsc::sync_channel(0);
        let (release_tx, release_rx) = std_mpsc::sync_channel(0);
        let mut pause = self
            .pause
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(pause.is_none(), "only one OPEN reply barrier may be armed");
        *pause = Some(RootOpenReplyPublicationPause {
            entered: entered_tx,
            release: release_rx,
        });
        RootOpenReplyPublicationControl {
            entered: entered_rx,
            release: release_tx,
        }
    }

    fn pause_if_armed(&self) {
        let pause = self
            .pause
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        let Some(pause) = pause else {
            return;
        };
        let _entered = pause.entered.send(());
        let _released = pause.release.recv();
    }
}

#[cfg(test)]
impl RootOpenReplyPublicationControl {
    pub(crate) fn wait_until_blocked(&self) {
        self.entered
            .recv_timeout(Duration::from_secs(3))
            .expect("root OPEN reply barrier disappeared before entry");
    }

    pub(crate) fn release(&self) {
        self.release
            .send(())
            .expect("root OPEN reply barrier disappeared before release");
    }
}

#[cfg(test)]
impl Drop for RootOpenReplyPublicationControl {
    fn drop(&mut self) {
        let _released = self.release.try_send(());
    }
}

#[cfg(test)]
#[derive(Default)]
struct RootShutdownPreParkHook {
    pauses: Mutex<VecDeque<RootShutdownPreParkPause>>,
}

#[cfg(test)]
struct RootShutdownPreParkPause {
    entered: std_mpsc::SyncSender<()>,
    release: std_mpsc::Receiver<()>,
}

#[cfg(test)]
pub(crate) struct RootShutdownPreParkControl {
    entered: std_mpsc::Receiver<()>,
    release: std_mpsc::SyncSender<()>,
}

#[cfg(test)]
impl RootShutdownPreParkHook {
    fn arm(&self) -> RootShutdownPreParkControl {
        let (entered_tx, entered_rx) = std_mpsc::sync_channel(0);
        let (release_tx, release_rx) = std_mpsc::sync_channel(0);
        let mut pauses = self
            .pauses
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        pauses.push_back(RootShutdownPreParkPause {
            entered: entered_tx,
            release: release_rx,
        });
        RootShutdownPreParkControl {
            entered: entered_rx,
            release: release_tx,
        }
    }

    fn pause_if_armed(&self) -> bool {
        let pause = self
            .pauses
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pop_front();
        let Some(pause) = pause else {
            return false;
        };
        let _entered = pause.entered.send(());
        let _released = pause.release.recv();
        true
    }
}

#[cfg(test)]
impl RootShutdownPreParkControl {
    pub(crate) fn wait_until_blocked(&self) {
        self.entered
            .recv_timeout(Duration::from_secs(3))
            .expect("root shutdown pre-park barrier disappeared before entry");
    }

    pub(crate) fn release(&self) {
        self.release
            .send(())
            .expect("root shutdown pre-park barrier disappeared before release");
    }
}

#[cfg(test)]
impl Drop for RootShutdownPreParkControl {
    fn drop(&mut self) {
        // A failing assertion must not strand a Tokio worker in the
        // deterministic synchronous barrier and hang the whole test binary.
        let _released = self.release.try_send(());
    }
}

/// Deterministic test-only barrier at one exact bounded shutdown-slice
/// boundary. This is distinct from the final pre-park hook: a small work
/// budget can split one complete idle scan across several cooperative turns.
#[cfg(test)]
#[derive(Default)]
struct RootShutdownSliceHook {
    pauses: Mutex<VecDeque<RootShutdownSlicePause>>,
}

#[cfg(test)]
struct RootShutdownSlicePause {
    next_category: RootShutdownWorkCategory,
    entered: std_mpsc::SyncSender<()>,
    release: std_mpsc::Receiver<()>,
}

#[cfg(test)]
pub(crate) struct RootShutdownSliceControl {
    entered: std_mpsc::Receiver<()>,
    release: std_mpsc::SyncSender<()>,
}

#[cfg(test)]
impl RootShutdownSliceHook {
    fn arm(&self, next_category: RootShutdownWorkCategory) -> RootShutdownSliceControl {
        let (entered_tx, entered_rx) = std_mpsc::sync_channel(0);
        let (release_tx, release_rx) = std_mpsc::sync_channel(0);
        let mut pauses = self
            .pauses
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        pauses.push_back(RootShutdownSlicePause {
            next_category,
            entered: entered_tx,
            release: release_rx,
        });
        RootShutdownSliceControl {
            entered: entered_rx,
            release: release_tx,
        }
    }

    fn pause_if_armed(&self, next_category: RootShutdownWorkCategory) {
        let pause = {
            let mut pauses = self
                .pauses
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            match pauses.front() {
                Some(pause) if pause.next_category == next_category => pauses.pop_front(),
                _ => None,
            }
        };
        let Some(pause) = pause else {
            return;
        };
        let _entered = pause.entered.send(());
        let _released = pause.release.recv();
    }
}

#[cfg(test)]
impl RootShutdownSliceControl {
    pub(crate) fn wait_until_blocked(&self) {
        self.entered
            .recv_timeout(Duration::from_secs(3))
            .expect("root shutdown slice barrier disappeared before entry");
    }

    pub(crate) fn release(&self) {
        self.release
            .send(())
            .expect("root shutdown slice barrier disappeared before release");
    }
}

#[cfg(test)]
impl Drop for RootShutdownSliceControl {
    fn drop(&mut self) {
        let _released = self.release.try_send(());
    }
}

impl fmt::Debug for QuinnClientRootCommand {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OpenTcp { reply, .. } => formatter
                .debug_struct("OpenTcp")
                .field("target", &"[REDACTED]")
                .field("reply_closed", &reply.is_closed())
                .finish(),
        }
    }
}

trait RootDeadline {
    fn instant(&self) -> SimTime;
}

impl RootDeadline for QuinnTerminalGraceTimer {
    fn instant(&self) -> SimTime {
        QuinnTerminalGraceTimer::instant(self)
    }
}

struct ScheduledRootDeadline<T> {
    instant: SimTime,
    sequence: u64,
    timer: T,
}

impl<T> ScheduledRootDeadline<T> {
    const fn instant(&self) -> SimTime {
        self.instant
    }

    const fn timer(&self) -> &T {
        &self.timer
    }

    fn into_timer(self) -> T {
        self.timer
    }
}

impl<T> PartialEq for ScheduledRootDeadline<T> {
    fn eq(&self, other: &Self) -> bool {
        self.instant == other.instant && self.sequence == other.sequence
    }
}

impl<T> Eq for ScheduledRootDeadline<T> {}

impl<T> PartialOrd for ScheduledRootDeadline<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for ScheduledRootDeadline<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap is a max-heap. Reverse both keys so the earliest absolute
        // deadline and then the oldest stable insertion sequence win.
        other
            .instant
            .cmp(&self.instant)
            .then_with(|| other.sequence.cmp(&self.sequence))
    }
}

enum RootDeadlineHeapAdmissionError<T> {
    LogicalCapacity(T),
    SequenceExhausted(T),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RootDeadlineBatchAdmissionError {
    LogicalCapacity,
    PhysicalCapacity,
    SequenceExhausted,
}

#[derive(Clone, Copy)]
struct RootDeadlineBatchAdmission {
    first_sequence: u64,
    next_sequence: u64,
}

struct RootDeadlineHeap<T> {
    heap: BinaryHeap<ScheduledRootDeadline<T>>,
    logical_capacity: usize,
    next_sequence: u64,
}

impl<T: RootDeadline> RootDeadlineHeap<T> {
    fn preallocate(logical_capacity: usize) -> Result<Self, &'static str> {
        let mut heap = BinaryHeap::new();
        heap.try_reserve_exact(logical_capacity)
            .map_err(|_| "terminal timer registry")?;
        Ok(Self {
            heap,
            logical_capacity,
            next_sequence: 0,
        })
    }

    const fn logical_capacity(&self) -> usize {
        self.logical_capacity
    }

    fn physical_capacity(&self) -> usize {
        self.heap.capacity()
    }

    fn len(&self) -> usize {
        self.heap.len()
    }

    fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    fn next_instant(&self) -> Option<SimTime> {
        self.heap.peek().map(ScheduledRootDeadline::instant)
    }

    fn try_insert(&mut self, timer: T) -> Result<(), RootDeadlineHeapAdmissionError<T>> {
        if self.len() >= self.logical_capacity {
            return Err(RootDeadlineHeapAdmissionError::LogicalCapacity(timer));
        }
        let Some(next_sequence) = self.next_sequence.checked_add(1) else {
            return Err(RootDeadlineHeapAdmissionError::SequenceExhausted(timer));
        };
        let scheduled = ScheduledRootDeadline {
            instant: timer.instant(),
            sequence: self.next_sequence,
            timer,
        };
        self.next_sequence = next_sequence;
        self.heap.push(scheduled);
        Ok(())
    }

    fn preflight_batch(
        &self,
        count: usize,
    ) -> Result<RootDeadlineBatchAdmission, RootDeadlineBatchAdmissionError> {
        if self
            .len()
            .checked_add(count)
            .is_none_or(|needed| needed > self.logical_capacity)
        {
            return Err(RootDeadlineBatchAdmissionError::LogicalCapacity);
        }
        if self.heap.capacity().saturating_sub(self.len()) < count {
            return Err(RootDeadlineBatchAdmissionError::PhysicalCapacity);
        }
        let count =
            u64::try_from(count).map_err(|_| RootDeadlineBatchAdmissionError::SequenceExhausted)?;
        let next_sequence = self
            .next_sequence
            .checked_add(count)
            .ok_or(RootDeadlineBatchAdmissionError::SequenceExhausted)?;
        Ok(RootDeadlineBatchAdmission {
            first_sequence: self.next_sequence,
            next_sequence,
        })
    }

    fn pop_due(&mut self, through: SimTime) -> Option<ScheduledRootDeadline<T>> {
        self.heap
            .peek()
            .is_some_and(|scheduled| scheduled.instant <= through)
            .then(|| self.heap.pop())
            .flatten()
    }

    fn pop_next(&mut self) -> Option<ScheduledRootDeadline<T>> {
        self.heap.pop()
    }

    fn reinsert(
        &mut self,
        scheduled: ScheduledRootDeadline<T>,
    ) -> Result<(), ScheduledRootDeadline<T>> {
        if self.len() >= self.logical_capacity {
            return Err(scheduled);
        }
        self.heap.push(scheduled);
        Ok(())
    }

    fn clear(&mut self) {
        self.heap.clear();
    }
}

impl RootDeadlineHeap<QuinnTerminalGraceTimer> {
    fn reset_delivery_for_queue(
        &self,
        flow_id: SessionFlowId,
    ) -> Option<super::leg_io::OrderedResetDeliveryReceipt> {
        self.heap
            .iter()
            .find_map(|scheduled| scheduled.timer().reset_delivery_for_queue(flow_id))
    }
}

/// Cloneable adapter-facing capability. It contains only the future bounded
/// command lane and read-only lifecycle observation; composition, factory,
/// drivers, timers, and shutdown authority remain in the root tree.
#[derive(Clone)]
pub(crate) struct QuinnClientRootHandle {
    identity: Arc<QuinnClientRootIdentity>,
    commands: mpsc::Sender<QuinnClientRootCommand>,
    lifecycle: Arc<RootLifecycle>,
    #[cfg(test)]
    open_reply_publication: Arc<RootOpenReplyPublicationHook>,
    #[cfg(test)]
    shutdown_pre_park: Arc<RootShutdownPreParkHook>,
    #[cfg(test)]
    shutdown_slice: Arc<RootShutdownSliceHook>,
    #[cfg(test)]
    run_probe: Arc<RootRunProbe>,
    #[cfg(test)]
    live_category: Arc<RootLiveCategoryHook>,
}

impl QuinnClientRootHandle {
    #[cfg(test)]
    pub(crate) fn command_available_capacity_for_test(&self) -> usize {
        self.commands.capacity()
    }

    pub(crate) fn phase(&self) -> QuinnClientRootPhase {
        self.lifecycle.phase()
    }

    pub(crate) fn same_root(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.identity, &other.identity)
    }

    /// Sends one bounded, exact-owned TCP OPEN command. The task receives an
    /// exact-sized normalized Target; this future retains the caller's
    /// original allocation until success, so every error can return it even
    /// if the root task or reply channel disappears.
    pub(crate) async fn open_tcp(
        &self,
        target: TargetAddr,
    ) -> Result<ResumableTcpFlow, QuinnClientRootOpenFailure> {
        let phase = self.phase();
        if phase != QuinnClientRootPhase::Running {
            return Err(QuinnClientRootOpenFailure::new(
                target,
                QuinnClientRootOpenFailureKind::RootNotRunning(phase),
            ));
        }
        let command_target = match normalize_target_for_owned_command(&target) {
            Ok(target) => target,
            Err(error) => {
                return Err(QuinnClientRootOpenFailure::new(
                    target,
                    QuinnClientRootOpenFailureKind::InvalidTarget(error),
                ));
            }
        };
        let (reply, response) = oneshot::channel();
        let command = QuinnClientRootCommand::OpenTcp {
            target: command_target,
            reply,
        };
        if self.commands.send(command).await.is_err() {
            return Err(QuinnClientRootOpenFailure::new(
                target,
                QuinnClientRootOpenFailureKind::CommandLaneClosed,
            ));
        }
        match response.await {
            Ok(Ok(flow)) => Ok(flow),
            Ok(Err(kind)) => Err(QuinnClientRootOpenFailure::new(target, kind)),
            Err(_) => Err(QuinnClientRootOpenFailure::new(
                target,
                QuinnClientRootOpenFailureKind::ReplySourceClosed,
            )),
        }
    }

    pub(crate) async fn wait_terminal_phase(
        &self,
    ) -> Result<QuinnClientRootPhase, QuinnClientRootReadyError> {
        let mut lifecycle = self.lifecycle.subscribe();
        loop {
            let phase = *lifecycle.borrow_and_update();
            if matches!(
                phase,
                QuinnClientRootPhase::Joined
                    | QuinnClientRootPhase::FailedJoined
                    | QuinnClientRootPhase::TaskLost
            ) {
                return Ok(phase);
            }
            lifecycle
                .changed()
                .await
                .map_err(|_| QuinnClientRootReadyError::LifecycleSourceClosed)?;
        }
    }
}

impl fmt::Debug for QuinnClientRootHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QuinnClientRootHandle")
            .field("phase", &self.phase())
            .field("identity", &"[OPAQUE]")
            .finish()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RootShutdownSignal {
    Open,
    Explicit,
    OwnerDropped,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QuinnClientRootShutdownDisposition {
    Started,
    AlreadyStarted,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QuinnClientRootReadyError {
    StoppedBeforeReady(QuinnClientRootPhase),
    LifecycleSourceClosed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum QuinnClientRootOpenFailureKind {
    RootNotRunning(QuinnClientRootPhase),
    InvalidTarget(ProtocolError),
    CommandLaneClosed,
    ReplySourceClosed,
    ReplyCancelledBeforeCommit,
    FlowRegistryCapacity { max: usize },
    PendingEffectsNotDrained,
    Composition(QuinnClientRootCompositionOpenFailureKind),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QuinnClientRootCompositionOpenFailureKind {
    SessionUnavailable,
    RetryableCapacity,
    RetryablePolicy,
    Factory(FlowPortError),
    Invariant,
}

impl QuinnClientRootCompositionOpenFailureKind {
    fn from_composition(kind: QuinnPreparedLocalTcpOpenFailureKind) -> Self {
        match kind {
            QuinnPreparedLocalTcpOpenFailureKind::SessionShuttingDown
            | QuinnPreparedLocalTcpOpenFailureKind::ActorTransportUnavailable(_)
            | QuinnPreparedLocalTcpOpenFailureKind::QueueEndpointLost => Self::SessionUnavailable,
            QuinnPreparedLocalTcpOpenFailureKind::ControllerBlocked(_) => Self::RetryablePolicy,
            QuinnPreparedLocalTcpOpenFailureKind::QueueCapacity
            | QuinnPreparedLocalTcpOpenFailureKind::ReducerCapacity(_) => Self::RetryableCapacity,
            QuinnPreparedLocalTcpOpenFailureKind::Factory(error) => Self::Factory(error),
            QuinnPreparedLocalTcpOpenFailureKind::PhysicalActorMismatch
            | QuinnPreparedLocalTcpOpenFailureKind::QueueInvariant
            | QuinnPreparedLocalTcpOpenFailureKind::ReducerOrSupervisor => Self::Invariant,
        }
    }
}

pub(crate) struct QuinnClientRootOpenFailure {
    target: TargetAddr,
    kind: QuinnClientRootOpenFailureKind,
}

impl QuinnClientRootOpenFailure {
    fn new(target: TargetAddr, kind: QuinnClientRootOpenFailureKind) -> Self {
        Self { target, kind }
    }

    pub(crate) const fn kind(&self) -> &QuinnClientRootOpenFailureKind {
        &self.kind
    }

    pub(crate) fn into_target(self) -> TargetAddr {
        self.target
    }
}

impl fmt::Debug for QuinnClientRootOpenFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QuinnClientRootOpenFailure")
            .field("target", &"[REDACTED]")
            .field("kind", &self.kind)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ClientRootJoined {
    actor_owned_messages: usize,
    actor_owned_bytes: usize,
    actor_owned_terminals: usize,
    factory_owned_bytes: usize,
    factory_owned_segments: usize,
    session_residual_ownership: usize,
    flow_drivers: usize,
    pending_effects: usize,
    timers: usize,
    mailbox_messages: usize,
    controller_ownership: ClientControllerOwnershipLedger,
    root_recovery_timers: RootRecoveryTimerLiveInventory,
    client_progress: RootClientProgressLiveInventory,
    client_progress_shutdown: RootClientProgressShutdownAudit,
    client_observations: RootClientObservationHistory,
}

impl ClientRootJoined {
    pub(crate) const fn actor_owned_messages(self) -> usize {
        self.actor_owned_messages
    }

    pub(crate) const fn actor_owned_bytes(self) -> usize {
        self.actor_owned_bytes
    }

    pub(crate) const fn actor_owned_terminals(self) -> usize {
        self.actor_owned_terminals
    }

    pub(crate) const fn factory_owned_bytes(self) -> usize {
        self.factory_owned_bytes
    }

    pub(crate) const fn factory_owned_segments(self) -> usize {
        self.factory_owned_segments
    }

    pub(crate) const fn session_residual_ownership(self) -> usize {
        self.session_residual_ownership
    }

    pub(crate) const fn flow_drivers(self) -> usize {
        self.flow_drivers
    }

    pub(crate) const fn pending_effects(self) -> usize {
        self.pending_effects
    }

    pub(crate) const fn timers(self) -> usize {
        self.timers
    }

    pub(crate) const fn mailbox_messages(self) -> usize {
        self.mailbox_messages
    }

    /// Includes both live zero-ownership fields and historical settlement
    /// counters. Historical counters remain queryable evidence and do not make
    /// an otherwise clean joined witness nonzero.
    pub(crate) const fn controller_ownership(self) -> ClientControllerOwnershipLedger {
        self.controller_ownership
    }

    pub(crate) const fn root_recovery_timers(self) -> RootRecoveryTimerLiveInventory {
        self.root_recovery_timers
    }

    pub(crate) const fn client_progress(self) -> RootClientProgressLiveInventory {
        self.client_progress
    }

    pub(crate) const fn client_progress_shutdown(self) -> RootClientProgressShutdownAudit {
        self.client_progress_shutdown
    }

    pub(crate) const fn client_observations(self) -> RootClientObservationHistory {
        self.client_observations
    }

    pub(crate) const fn is_zero(self) -> bool {
        self.actor_owned_messages == 0
            && self.actor_owned_bytes == 0
            && self.actor_owned_terminals == 0
            && self.factory_owned_bytes == 0
            && self.factory_owned_segments == 0
            && self.session_residual_ownership == 0
            && self.flow_drivers == 0
            && self.pending_effects == 0
            && self.timers == 0
            && self.mailbox_messages == 0
            && self.controller_ownership.live_is_zero()
            && self.root_recovery_timers.is_zero()
            && self.client_progress.is_zero()
    }
}

#[derive(Debug)]
pub(crate) enum QuinnClientRootOutcome {
    Joined(ClientRootJoined),
    FailedJoined {
        joined: Option<ClientRootJoined>,
        first_failure: Arc<str>,
    },
    TaskLost(Arc<str>),
}

impl QuinnClientRootOutcome {
    pub(crate) const fn clean_joined(&self) -> Option<ClientRootJoined> {
        match self {
            Self::Joined(joined) => Some(*joined),
            Self::FailedJoined { .. } | Self::TaskLost(_) => None,
        }
    }
}

#[derive(Debug)]
pub(crate) enum QuinnClientRootStartFailureKind {
    SessionConfigMismatch,
    CompositionNotQuiescent,
    RuntimeUnavailable,
    Allocation(&'static str),
    CompositionNotRunning,
    Factory(TcpPortFactoryMintError),
}

pub(crate) struct QuinnClientRootStartFailure {
    ownership: Box<(QuinnClientRootPlan, Box<QuinnClientSessionComposition>)>,
    kind: QuinnClientRootStartFailureKind,
}

impl QuinnClientRootStartFailure {
    pub(crate) const fn kind(&self) -> &QuinnClientRootStartFailureKind {
        &self.kind
    }

    pub(crate) fn into_parts(self) -> (QuinnClientRootPlan, Box<QuinnClientSessionComposition>) {
        *self.ownership
    }

    fn new(
        plan: QuinnClientRootPlan,
        composition: Box<QuinnClientSessionComposition>,
        kind: QuinnClientRootStartFailureKind,
    ) -> Self {
        Self {
            ownership: Box::new((plan, composition)),
            kind,
        }
    }
}

impl fmt::Debug for QuinnClientRootStartFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QuinnClientRootStartFailure")
            .field("kind", &self.kind)
            .field("ownership", &"[REDACTED]")
            .finish()
    }
}

struct RootStorage {
    drivers: Vec<ResumableTcpDriver>,
    next_driver: usize,
    deferred_driver_input: Option<ClientControllerDriverInput>,
    aborted_driver_inputs: Vec<ClientControllerDriverInput>,
    pending_effects: VecDeque<SessionEffect>,
    aborted_effects: Option<Vec<SessionEffect>>,
    recovery_timers: RootRecoveryTimerSlots,
    client_progress: RootClientProgressState,
    terminal_timers: RootDeadlineHeap<QuinnTerminalGraceTimer>,
    terminal_publication_pending: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct RootClientProgressLiveInventory {
    sources: usize,
    episodes: usize,
    timer_slots: usize,
    due_authorities: usize,
}

impl RootClientProgressLiveInventory {
    pub(crate) const fn sources(self) -> usize {
        self.sources
    }

    pub(crate) const fn episodes(self) -> usize {
        self.episodes
    }

    pub(crate) const fn timer_slots(self) -> usize {
        self.timer_slots
    }

    pub(crate) const fn due_authorities(self) -> usize {
        self.due_authorities
    }

    pub(crate) const fn is_zero(self) -> bool {
        self.sources == 0
            && self.episodes == 0
            && self.timer_slots == 0
            && self.due_authorities == 0
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct RootClientProgressShutdownAudit {
    discarded_sources: usize,
    discarded_episodes: usize,
    discarded_timer_slots: usize,
    discarded_due_authorities: usize,
}

impl RootClientProgressShutdownAudit {
    pub(crate) const fn discarded_sources(self) -> usize {
        self.discarded_sources
    }

    pub(crate) const fn discarded_episodes(self) -> usize {
        self.discarded_episodes
    }

    pub(crate) const fn discarded_timer_slots(self) -> usize {
        self.discarded_timer_slots
    }

    pub(crate) const fn discarded_due_authorities(self) -> usize {
        self.discarded_due_authorities
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct RootClientObservationHistory {
    replay_became_outstanding: usize,
    flow_direction_terminal: usize,
    session_expired: usize,
    positive_application_acks: usize,
    due_observed: usize,
}

impl RootClientObservationHistory {
    pub(crate) const fn replay_became_outstanding(self) -> usize {
        self.replay_became_outstanding
    }

    pub(crate) const fn flow_direction_terminal(self) -> usize {
        self.flow_direction_terminal
    }

    pub(crate) const fn session_expired(self) -> usize {
        self.session_expired
    }

    pub(crate) const fn positive_application_acks(self) -> usize {
        self.positive_application_acks
    }

    pub(crate) const fn due_observed(self) -> usize {
        self.due_observed
    }

    fn increment(counter: &mut usize) -> Result<(), RootClientProgressError> {
        *counter = counter
            .checked_add(1)
            .ok_or(RootClientProgressError::ObservationHistoryOverflow)?;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RootLocalAckStallTimer {
    deadline: SimTime,
    due_observed: bool,
}

struct RootLocalAckStallTimerSlot {
    timer: Option<RootLocalAckStallTimer>,
}

impl RootLocalAckStallTimerSlot {
    const fn new() -> Self {
        Self { timer: None }
    }

    const fn capacity(&self) -> usize {
        ROOT_LOCAL_ACK_STALL_TIMER_SLOTS
    }

    fn len(&self) -> usize {
        usize::from(self.timer.is_some())
    }

    const fn is_due_observed(&self) -> bool {
        matches!(
            self.timer,
            Some(RootLocalAckStallTimer {
                due_observed: true,
                ..
            })
        )
    }

    fn try_install(&mut self, deadline: SimTime) -> Result<(), SimTime> {
        if self.timer.is_some() {
            return Err(deadline);
        }
        self.timer = Some(RootLocalAckStallTimer {
            deadline,
            due_observed: false,
        });
        Ok(())
    }

    fn refresh(&mut self, deadline: SimTime) -> Result<(), SimTime> {
        let Some(timer) = self.timer.as_mut() else {
            return Err(deadline);
        };
        timer.deadline = deadline;
        timer.due_observed = false;
        Ok(())
    }

    const fn next_wake_deadline(&self) -> Option<SimTime> {
        match self.timer {
            Some(RootLocalAckStallTimer {
                deadline,
                due_observed: false,
            }) => Some(deadline),
            Some(RootLocalAckStallTimer {
                due_observed: true, ..
            })
            | None => None,
        }
    }

    fn observe_due(&mut self, now: SimTime) -> bool {
        let Some(timer) = self.timer.as_mut() else {
            return false;
        };
        if timer.due_observed || now < timer.deadline {
            return false;
        }
        timer.due_observed = true;
        true
    }

    fn clear(&mut self) -> bool {
        self.timer.take().is_some()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RootReplayObservation {
    session_id: SessionId,
    generation: LegGeneration,
    session_became_nonempty: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RootFlowTerminalObservation {
    session_id: SessionId,
    generation: LegGeneration,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RootSessionTerminalObservation {
    session_id: SessionId,
    generation: LegGeneration,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct RootReducerObservationView {
    replay_became_outstanding: Option<RootReplayObservation>,
    flow_direction_terminal: Option<RootFlowTerminalObservation>,
    session_expired: Option<RootSessionTerminalObservation>,
    session_has_outstanding_replay: bool,
}

impl RootReducerObservationView {
    fn from_controller(observations: &ClientControllerReducerObservations) -> Self {
        Self {
            replay_became_outstanding: observations.replay_became_outstanding().map(|fact| {
                RootReplayObservation {
                    session_id: fact.session_id(),
                    generation: fact.generation(),
                    session_became_nonempty: fact.session_became_nonempty(),
                }
            }),
            flow_direction_terminal: observations.flow_direction_terminal().map(|fact| {
                RootFlowTerminalObservation {
                    session_id: fact.session_id(),
                    generation: fact.generation(),
                }
            }),
            session_expired: observations.session_expired().map(|fact| {
                RootSessionTerminalObservation {
                    session_id: fact.session_id(),
                    generation: fact.generation(),
                }
            }),
            session_has_outstanding_replay: observations.session_has_outstanding_replay(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RootPositiveAckObservation {
    arrived_at: SimTime,
    session_id: SessionId,
    generation: LegGeneration,
    session_has_outstanding_replay: bool,
}

impl RootPositiveAckObservation {
    const fn from_inbound(observation: ClientInboundObservation) -> Self {
        match observation {
            ClientInboundObservation::ReplacementRecovered {
                arrived_at,
                session_id,
                generation,
                session_has_outstanding_replay,
                ..
            }
            | ClientInboundObservation::ApplicationAckAdvanced {
                arrived_at,
                session_id,
                generation,
                session_has_outstanding_replay,
                ..
            } => Self {
                arrived_at,
                session_id,
                generation,
                session_has_outstanding_replay,
            },
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RootClientProgressError {
    ObservationHistoryOverflow,
    ObservationCorrelationMismatch,
    MissingAggregateOutstandingEpisode,
    DuplicateAggregateOutstandingEpisode,
    LocalTimerAlreadyOccupied,
    LocalTimerMissing,
    LocalSource(ClientLocalAckStallError),
}

impl From<ClientLocalAckStallError> for RootClientProgressError {
    fn from(error: ClientLocalAckStallError) -> Self {
        Self::LocalSource(error)
    }
}

struct RootClientProgressState {
    source: Option<ClientLocalAckStallSource>,
    timer: RootLocalAckStallTimerSlot,
    history: RootClientObservationHistory,
}

impl RootClientProgressState {
    const fn new() -> Self {
        Self {
            source: None,
            timer: RootLocalAckStallTimerSlot::new(),
            history: RootClientObservationHistory {
                replay_became_outstanding: 0,
                flow_direction_terminal: 0,
                session_expired: 0,
                positive_application_acks: 0,
                due_observed: 0,
            },
        }
    }

    const fn history(&self) -> RootClientObservationHistory {
        self.history
    }

    fn source_matches(&self, session_id: SessionId, generation: LegGeneration) -> bool {
        self.source.as_ref().is_none_or(|source| {
            source.session_id() == session_id && source.generation() == generation
        })
    }

    fn require_source_correlation(
        &self,
        session_id: SessionId,
        generation: LegGeneration,
    ) -> Result<(), RootClientProgressError> {
        if self.source_matches(session_id, generation) {
            Ok(())
        } else {
            Err(RootClientProgressError::ObservationCorrelationMismatch)
        }
    }

    fn observe_replay_became_outstanding(
        &mut self,
        observation: RootReplayObservation,
        observed_at: SimTime,
        stall_after: Duration,
    ) -> Result<(), RootClientProgressError> {
        RootClientObservationHistory::increment(&mut self.history.replay_became_outstanding)?;
        self.require_source_correlation(observation.session_id, observation.generation)?;
        if !observation.session_became_nonempty {
            return match self
                .source
                .as_ref()
                .and_then(ClientLocalAckStallSource::due_at)
            {
                Some(_) if self.timer.len() == 1 => Ok(()),
                _ => Err(RootClientProgressError::MissingAggregateOutstandingEpisode),
            };
        }
        if self
            .source
            .as_ref()
            .and_then(ClientLocalAckStallSource::due_at)
            .is_some()
        {
            return Err(RootClientProgressError::DuplicateAggregateOutstandingEpisode);
        }
        if self.timer.len() != 0 {
            return Err(RootClientProgressError::LocalTimerAlreadyOccupied);
        }
        let source = self.source.get_or_insert_with(|| {
            ClientLocalAckStallSource::new(observation.session_id, observation.generation)
        });
        source.begin_outstanding(observed_at, stall_after)?;
        let deadline = source
            .due_at()
            .ok_or(RootClientProgressError::MissingAggregateOutstandingEpisode)?;
        self.timer
            .try_install(deadline)
            .map_err(|_| RootClientProgressError::LocalTimerAlreadyOccupied)
    }

    fn observe_reducer(
        &mut self,
        observations: RootReducerObservationView,
        observed_at: SimTime,
        stall_after: Duration,
    ) -> Result<(), RootClientProgressError> {
        if let Some(replay) = observations.replay_became_outstanding {
            self.observe_replay_became_outstanding(replay, observed_at, stall_after)?;
        }
        if let Some(terminal) = observations.flow_direction_terminal {
            RootClientObservationHistory::increment(&mut self.history.flow_direction_terminal)?;
            self.require_source_correlation(terminal.session_id, terminal.generation)?;
        }
        if let Some(terminal) = observations.session_expired {
            RootClientObservationHistory::increment(&mut self.history.session_expired)?;
            self.require_source_correlation(terminal.session_id, terminal.generation)?;
            self.clear_session();
        } else if !observations.session_has_outstanding_replay {
            self.clear_outstanding();
        }
        Ok(())
    }

    fn observe_inbound(
        &mut self,
        observation: Option<ClientInboundObservation>,
    ) -> Result<(), RootClientProgressError> {
        let Some(observation) = observation else {
            return Ok(());
        };
        RootClientObservationHistory::increment(&mut self.history.positive_application_acks)?;
        let observation = RootPositiveAckObservation::from_inbound(observation);
        self.require_source_correlation(observation.session_id, observation.generation)?;
        // Actor arrival, not root dequeue time, decides the old absolute
        // deadline.  Exact-deadline progress wins, but a later committed ACK
        // must not refresh or clear an episode the scheduler was already
        // entitled to select.  The reducer fact remains counted above; only
        // its authority to mutate this policy episode is rejected here.
        if self
            .source
            .as_ref()
            .and_then(ClientLocalAckStallSource::due_at)
            .is_some_and(|deadline| observation.arrived_at > deadline)
        {
            return Ok(());
        }
        if !observation.session_has_outstanding_replay {
            self.clear_outstanding();
            return Ok(());
        }
        if self.timer.len() != 1 {
            return Err(RootClientProgressError::LocalTimerMissing);
        }
        let source = self
            .source
            .as_mut()
            .ok_or(RootClientProgressError::MissingAggregateOutstandingEpisode)?;
        source.observe_positive_ack(observation.arrived_at)?;
        let deadline = source
            .due_at()
            .ok_or(RootClientProgressError::MissingAggregateOutstandingEpisode)?;
        self.timer
            .refresh(deadline)
            .map_err(|_| RootClientProgressError::LocalTimerMissing)
    }

    fn clear_outstanding(&mut self) {
        if let Some(source) = self.source.as_mut() {
            source.clear_outstanding();
        }
        self.timer.clear();
    }

    fn clear_session(&mut self) {
        self.source = None;
        self.timer.clear();
    }

    fn observe_due(&mut self, now: SimTime) -> Result<bool, RootClientProgressError> {
        if !self.timer.observe_due(now) {
            return Ok(false);
        }
        RootClientObservationHistory::increment(&mut self.history.due_observed)?;
        Ok(true)
    }

    const fn next_wake_deadline(&self) -> Option<SimTime> {
        self.timer.next_wake_deadline()
    }

    fn live_inventory(&self) -> RootClientProgressLiveInventory {
        RootClientProgressLiveInventory {
            sources: usize::from(self.source.is_some()),
            episodes: usize::from(
                self.source
                    .as_ref()
                    .and_then(ClientLocalAckStallSource::due_at)
                    .is_some(),
            ),
            timer_slots: self.timer.len(),
            due_authorities: usize::from(self.timer.is_due_observed()),
        }
    }

    fn close_for_shutdown(&mut self) -> RootClientProgressShutdownAudit {
        let live = self.live_inventory();
        self.source = None;
        self.timer.clear();
        RootClientProgressShutdownAudit {
            discarded_sources: live.sources,
            discarded_episodes: live.episodes,
            discarded_timer_slots: live.timer_slots,
            discarded_due_authorities: live.due_authorities,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RootCurrentARecoveryKey {
    generation: crate::resumable::LegGeneration,
    sequence: crate::resumable::ProbeSequence,
}

#[derive(Clone)]
pub(crate) struct RootStandbyBRecoveryKey {
    generation: crate::resumable::LegGeneration,
    sequence: crate::resumable::ProbeSequence,
    incarnation: Arc<RootStandbyBRecoveryIncarnation>,
}

/// Process-local identity for one exact armed-B wake. Wire generation and
/// probe sequence are necessary correlation, but a later B can legitimately
/// reuse both scalars. Pointer identity therefore participates in equality so
/// a stale root wake can never expire or settle a reincarnated physical B.
struct RootStandbyBRecoveryIncarnation;

impl RootStandbyBRecoveryKey {
    pub(super) fn mint(
        generation: crate::resumable::LegGeneration,
        sequence: crate::resumable::ProbeSequence,
    ) -> Self {
        Self {
            generation,
            sequence,
            incarnation: Arc::new(RootStandbyBRecoveryIncarnation),
        }
    }

    #[cfg(test)]
    pub(super) fn mint_for_test(
        generation: crate::resumable::LegGeneration,
        sequence: crate::resumable::ProbeSequence,
    ) -> Self {
        Self::mint(generation, sequence)
    }

    pub(super) const fn generation(&self) -> crate::resumable::LegGeneration {
        self.generation
    }

    pub(super) const fn sequence(&self) -> crate::resumable::ProbeSequence {
        self.sequence
    }
}

impl PartialEq for RootStandbyBRecoveryKey {
    fn eq(&self, other: &Self) -> bool {
        self.generation == other.generation
            && self.sequence == other.sequence
            && Arc::ptr_eq(&self.incarnation, &other.incarnation)
    }
}

impl Eq for RootStandbyBRecoveryKey {}

impl fmt::Debug for RootStandbyBRecoveryKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RootStandbyBRecoveryKey")
            .field("generation", &self.generation)
            .field("sequence", &self.sequence)
            .field("incarnation", &"[REDACTED]")
            .finish()
    }
}

trait RootCurrentARecoveryQueuedOwner {
    fn current_a_recovery_key(&self) -> RootCurrentARecoveryKey;
}

impl RootCurrentARecoveryQueuedOwner for QuinnCurrentProbeQueued {
    fn current_a_recovery_key(&self) -> RootCurrentARecoveryKey {
        RootCurrentARecoveryKey {
            generation: self.generation(),
            sequence: self.probe_sequence(),
        }
    }
}

pub(super) trait RootStandbyBRecoveryWakeOwner {
    fn standby_b_recovery_key(&self) -> &RootStandbyBRecoveryKey;
}

/// Opaque process-local timer correlation only. The exact armed-B expiry
/// authority remains inside `QuinnClientSessionComposition`; root cannot
/// commit expiry or reconstruct B ownership from this wake witness.
pub(super) struct RootStandbyBRecoveryWake {
    key: RootStandbyBRecoveryKey,
    deadline: SimTime,
}

impl RootStandbyBRecoveryWake {
    pub(super) const fn new(key: RootStandbyBRecoveryKey, deadline: SimTime) -> Self {
        Self { key, deadline }
    }

    pub(super) const fn deadline(&self) -> SimTime {
        self.deadline
    }

    pub(super) const fn recovery_key(&self) -> &RootStandbyBRecoveryKey {
        &self.key
    }
}

impl RootStandbyBRecoveryWakeOwner for RootStandbyBRecoveryWake {
    fn standby_b_recovery_key(&self) -> &RootStandbyBRecoveryKey {
        &self.key
    }
}

enum RootRecoveryTimerAuthority<Q = QuinnCurrentProbeQueued, D = QuinnCurrentProbeDeadline> {
    Queued {
        key: RootCurrentARecoveryKey,
        queued: Q,
        timeout: Duration,
        publication_pending: bool,
    },
    Armed {
        key: RootCurrentARecoveryKey,
        deadline: D,
        timeout: Duration,
        publication_pending: bool,
    },
}

/// Variant-only view moved through a synchronous scheduler transaction. The
/// fixed slot retains the correlation key, timeout, publication state, and
/// slot index until the callee either accepts the exact authority or returns
/// it. This prevents an ACK or shutdown rejection from losing adapter
/// metadata even if a defensive callee returns the other authority phase.
enum RootRecoverySchedulerAuthority<Q, D> {
    Queued(Q),
    Armed(D),
}

impl<Q, D> fmt::Debug for RootRecoverySchedulerAuthority<Q, D> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RootRecoverySchedulerAuthority([REDACTED])")
    }
}

impl<Q, D> RootRecoveryTimerAuthority<Q, D> {
    const fn key(&self) -> RootCurrentARecoveryKey {
        match self {
            Self::Queued { key, .. } | Self::Armed { key, .. } => *key,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct RootRecoveryTimerLiveInventory {
    queued: usize,
    armed: usize,
    publication_pending: usize,
    standby_b_wakes: usize,
}

impl RootRecoveryTimerLiveInventory {
    pub(crate) const fn current_a_queued(self) -> usize {
        self.queued
    }

    pub(crate) const fn current_a_armed(self) -> usize {
        self.armed
    }

    pub(crate) const fn current_a_publication_pending(self) -> usize {
        self.publication_pending
    }

    pub(crate) const fn standby_b_wakes(self) -> usize {
        self.standby_b_wakes
    }

    pub(crate) const fn queued(self) -> usize {
        self.queued
    }

    pub(crate) const fn armed(self) -> usize {
        self.armed
    }

    pub(crate) const fn publication_pending(self) -> usize {
        self.publication_pending
    }

    pub(crate) const fn is_zero(self) -> bool {
        self.queued == 0
            && self.armed == 0
            && self.publication_pending == 0
            && self.standby_b_wakes == 0
    }

    fn observe<Q, D>(&mut self, authority: &RootRecoveryTimerAuthority<Q, D>) {
        match authority {
            RootRecoveryTimerAuthority::Queued {
                publication_pending,
                ..
            } => {
                self.queued = self.queued.saturating_add(1);
                if *publication_pending {
                    self.publication_pending = self.publication_pending.saturating_add(1);
                }
            }
            RootRecoveryTimerAuthority::Armed {
                publication_pending,
                ..
            } => {
                self.armed = self.armed.saturating_add(1);
                if *publication_pending {
                    self.publication_pending = self.publication_pending.saturating_add(1);
                }
            }
        }
    }

    fn observe_standby_b_wake(&mut self) {
        self.standby_b_wakes = self.standby_b_wakes.saturating_add(1);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RootRecoveryTimerInstallFailureKind {
    Duplicate,
    Capacity,
}

struct RootRecoveryTimerInstallFailure<Q> {
    kind: RootRecoveryTimerInstallFailureKind,
    queued: Q,
    timeout: Duration,
}

impl<Q> RootRecoveryTimerInstallFailure<Q> {
    const fn kind(&self) -> RootRecoveryTimerInstallFailureKind {
        self.kind
    }

    fn into_parts(self) -> (Q, Duration) {
        (self.queued, self.timeout)
    }
}

impl<Q> fmt::Debug for RootRecoveryTimerInstallFailure<Q> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RootRecoveryTimerInstallFailure")
            .field("kind", &self.kind)
            .field("queued", &"[REDACTED]")
            .field("timeout", &self.timeout)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RootRecoveryTimerArmFailureKind {
    Missing,
    AlreadyArmed,
    Arm,
}

struct RootRecoveryTimerArmFailure<E> {
    kind: RootRecoveryTimerArmFailureKind,
    error: Option<E>,
}

impl<E> RootRecoveryTimerArmFailure<E> {
    const fn kind(&self) -> RootRecoveryTimerArmFailureKind {
        self.kind
    }

    fn into_error(self) -> Option<E> {
        self.error
    }
}

impl<E> fmt::Debug for RootRecoveryTimerArmFailure<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RootRecoveryTimerArmFailure")
            .field("kind", &self.kind)
            .field("error", &self.error.as_ref().map(|_| "[REDACTED]"))
            .finish()
    }
}

struct RootRecoveryTimerTaken<Q = QuinnCurrentProbeQueued, D = QuinnCurrentProbeDeadline> {
    slot_index: usize,
    authority: RootRecoveryTimerAuthority<Q, D>,
}

impl<Q, D> RootRecoveryTimerTaken<Q, D> {
    const fn from_parts(slot_index: usize, authority: RootRecoveryTimerAuthority<Q, D>) -> Self {
        Self {
            slot_index,
            authority,
        }
    }

    const fn key(&self) -> RootCurrentARecoveryKey {
        self.authority.key()
    }

    const fn authority(&self) -> &RootRecoveryTimerAuthority<Q, D> {
        &self.authority
    }

    fn into_parts(self) -> (usize, RootRecoveryTimerAuthority<Q, D>) {
        (self.slot_index, self.authority)
    }
}

impl<Q, D> fmt::Debug for RootRecoveryTimerTaken<Q, D> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RootRecoveryTimerTaken")
            .field("slot_index", &self.slot_index)
            .field("key", &self.key())
            .field("authority", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RootRecoveryTimerTakeFailureKind {
    Empty,
    KeyMismatch,
}

#[derive(Debug, PartialEq, Eq)]
enum RootRecoverySchedulerTransactionError<E> {
    Take(RootRecoveryTimerTakeFailureKind),
    Action(E),
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct RootRecoveryTimerHandoffSummary {
    accepted: usize,
    rejected: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RootRecoveryTimerRestoreFailureKind {
    InvalidSlot,
    Duplicate,
    SlotOccupied,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RootRecoveryTimerPublicationFailureKind {
    Empty,
    KeyMismatch,
    NotArmed,
}

struct RootRecoveryTimerRestoreFailure<Q = QuinnCurrentProbeQueued, D = QuinnCurrentProbeDeadline> {
    kind: RootRecoveryTimerRestoreFailureKind,
    taken: RootRecoveryTimerTaken<Q, D>,
}

impl<Q, D> RootRecoveryTimerRestoreFailure<Q, D> {
    const fn kind(&self) -> RootRecoveryTimerRestoreFailureKind {
        self.kind
    }

    fn into_taken(self) -> RootRecoveryTimerTaken<Q, D> {
        self.taken
    }
}

impl<Q, D> fmt::Debug for RootRecoveryTimerRestoreFailure<Q, D> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RootRecoveryTimerRestoreFailure")
            .field("kind", &self.kind)
            .field("taken", &self.taken)
            .finish()
    }
}

struct RootCurrentARecoveryTimerSlot<Q = QuinnCurrentProbeQueued, D = QuinnCurrentProbeDeadline> {
    slots: [Option<RootRecoveryTimerAuthority<Q, D>>; ROOT_CURRENT_A_RECOVERY_TIMER_SLOTS],
}

struct RootCurrentARecoveryShutdownDrain<Q = QuinnCurrentProbeQueued, D = QuinnCurrentProbeDeadline>
{
    slots: [Option<RootRecoveryTimerAuthority<Q, D>>; ROOT_CURRENT_A_RECOVERY_TIMER_SLOTS],
}

impl<Q, D> RootCurrentARecoveryShutdownDrain<Q, D> {
    fn len(&self) -> usize {
        self.slots.iter().flatten().count()
    }

    fn live_inventory(&self) -> RootRecoveryTimerLiveInventory {
        let mut inventory = RootRecoveryTimerLiveInventory::default();
        for authority in self.slots.iter().flatten() {
            inventory.observe(authority);
        }
        inventory
    }

    fn authority_for_key(
        &self,
        key: RootCurrentARecoveryKey,
    ) -> Option<&RootRecoveryTimerAuthority<Q, D>> {
        self.slots
            .iter()
            .flatten()
            .find(|authority| authority.key() == key)
    }

    /// Offers every exact fixed-slot scheduler authority synchronously. An
    /// accepted owner leaves the root array; a rejected owner is rebuilt in
    /// its original slot with the original key, timeout, and publication bit.
    fn try_handoff_all(
        &mut self,
        mut handoff: impl FnMut(
            RootRecoverySchedulerAuthority<Q, D>,
        ) -> Result<(), RootRecoverySchedulerAuthority<Q, D>>,
    ) -> RootRecoveryTimerHandoffSummary {
        let mut summary = RootRecoveryTimerHandoffSummary::default();
        for slot in &mut self.slots {
            let Some(authority) = slot.take() else {
                continue;
            };
            let (key, scheduler, timeout, publication_pending) = match authority {
                RootRecoveryTimerAuthority::Queued {
                    key,
                    queued,
                    timeout,
                    publication_pending,
                } => (
                    key,
                    RootRecoverySchedulerAuthority::Queued(queued),
                    timeout,
                    publication_pending,
                ),
                RootRecoveryTimerAuthority::Armed {
                    key,
                    deadline,
                    timeout,
                    publication_pending,
                } => (
                    key,
                    RootRecoverySchedulerAuthority::Armed(deadline),
                    timeout,
                    publication_pending,
                ),
            };
            match handoff(scheduler) {
                Ok(()) => summary.accepted = summary.accepted.saturating_add(1),
                Err(scheduler) => {
                    *slot = Some(match scheduler {
                        RootRecoverySchedulerAuthority::Queued(queued) => {
                            RootRecoveryTimerAuthority::Queued {
                                key,
                                queued,
                                timeout,
                                publication_pending,
                            }
                        }
                        RootRecoverySchedulerAuthority::Armed(deadline) => {
                            RootRecoveryTimerAuthority::Armed {
                                key,
                                deadline,
                                timeout,
                                publication_pending,
                            }
                        }
                    });
                    summary.rejected = summary.rejected.saturating_add(1);
                }
            }
        }
        summary
    }
}

impl<Q, D> fmt::Debug for RootCurrentARecoveryShutdownDrain<Q, D> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RootRecoveryTimerShutdownDrain")
            .field("inventory", &self.live_inventory())
            .field("authorities", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RootRecoveryTimerShutdownRestoreFailureKind {
    DestinationLive,
}

struct RootCurrentARecoveryShutdownRestoreFailure<
    Q = QuinnCurrentProbeQueued,
    D = QuinnCurrentProbeDeadline,
> {
    kind: RootRecoveryTimerShutdownRestoreFailureKind,
    drain: RootCurrentARecoveryShutdownDrain<Q, D>,
}

impl<Q, D> RootCurrentARecoveryShutdownRestoreFailure<Q, D> {
    const fn kind(&self) -> RootRecoveryTimerShutdownRestoreFailureKind {
        self.kind
    }

    fn into_drain(self) -> RootCurrentARecoveryShutdownDrain<Q, D> {
        self.drain
    }
}

impl<Q, D> fmt::Debug for RootCurrentARecoveryShutdownRestoreFailure<Q, D> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RootRecoveryTimerShutdownRestoreFailure")
            .field("kind", &self.kind)
            .field("drain", &self.drain)
            .finish()
    }
}

impl<Q, D> RootCurrentARecoveryTimerSlot<Q, D> {
    fn new() -> Self {
        Self {
            slots: std::array::from_fn(|_| None),
        }
    }

    const fn capacity(&self) -> usize {
        self.slots.len()
    }

    fn len(&self) -> usize {
        self.slots.iter().flatten().count()
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn live_inventory(&self) -> RootRecoveryTimerLiveInventory {
        let mut inventory = RootRecoveryTimerLiveInventory::default();
        for authority in self.slots.iter().flatten() {
            inventory.observe(authority);
        }
        inventory
    }

    fn try_install_queued(
        &mut self,
        queued: Q,
        timeout: Duration,
    ) -> Result<RootCurrentARecoveryKey, RootRecoveryTimerInstallFailure<Q>>
    where
        Q: RootCurrentARecoveryQueuedOwner,
    {
        let key = queued.current_a_recovery_key();
        if self
            .slots
            .iter()
            .flatten()
            .any(|authority| authority.key() == key)
        {
            return Err(RootRecoveryTimerInstallFailure {
                kind: RootRecoveryTimerInstallFailureKind::Duplicate,
                queued,
                timeout,
            });
        }
        let Some(slot) = self.slots.iter_mut().find(|slot| slot.is_none()) else {
            return Err(RootRecoveryTimerInstallFailure {
                kind: RootRecoveryTimerInstallFailureKind::Capacity,
                queued,
                timeout,
            });
        };
        *slot = Some(RootRecoveryTimerAuthority::Queued {
            key,
            queued,
            timeout,
            publication_pending: false,
        });
        Ok(key)
    }

    fn try_arm_queued<E>(
        &mut self,
        key: RootCurrentARecoveryKey,
        arm: impl FnOnce(Q, Duration) -> Result<D, (Q, E)>,
    ) -> Result<(), RootRecoveryTimerArmFailure<E>> {
        let Some(index) = self.slots.iter().position(|slot| {
            slot.as_ref()
                .is_some_and(|authority| authority.key() == key)
        }) else {
            return Err(RootRecoveryTimerArmFailure {
                kind: RootRecoveryTimerArmFailureKind::Missing,
                error: None,
            });
        };
        let Some(authority) = self.slots[index].take() else {
            return Err(RootRecoveryTimerArmFailure {
                kind: RootRecoveryTimerArmFailureKind::Missing,
                error: None,
            });
        };
        let RootRecoveryTimerAuthority::Queued {
            key,
            queued,
            timeout,
            publication_pending,
        } = authority
        else {
            self.slots[index] = Some(authority);
            return Err(RootRecoveryTimerArmFailure {
                kind: RootRecoveryTimerArmFailureKind::AlreadyArmed,
                error: None,
            });
        };
        match arm(queued, timeout) {
            Ok(deadline) => {
                self.slots[index] = Some(RootRecoveryTimerAuthority::Armed {
                    key,
                    deadline,
                    timeout,
                    publication_pending,
                });
                Ok(())
            }
            Err((queued, error)) => {
                self.slots[index] = Some(RootRecoveryTimerAuthority::Queued {
                    key,
                    queued,
                    timeout,
                    publication_pending,
                });
                Err(RootRecoveryTimerArmFailure {
                    kind: RootRecoveryTimerArmFailureKind::Arm,
                    error: Some(error),
                })
            }
        }
    }

    fn take_by_exact_key(
        &mut self,
        key: RootCurrentARecoveryKey,
    ) -> Result<RootRecoveryTimerTaken<Q, D>, RootRecoveryTimerTakeFailureKind> {
        let Some(slot_index) = self.slots.iter().position(|slot| {
            slot.as_ref()
                .is_some_and(|authority| authority.key() == key)
        }) else {
            return Err(if self.is_empty() {
                RootRecoveryTimerTakeFailureKind::Empty
            } else {
                RootRecoveryTimerTakeFailureKind::KeyMismatch
            });
        };
        let Some(authority) = self.slots[slot_index].take() else {
            return Err(RootRecoveryTimerTakeFailureKind::Empty);
        };
        Ok(RootRecoveryTimerTaken {
            slot_index,
            authority,
        })
    }

    /// Runs one exact scheduler transaction without exposing the now-empty
    /// slot to the action. Action rejection therefore restores infallibly in
    /// place; neither a second lookup nor a fallible public restore can lose
    /// the returned owner.
    fn transact_scheduler_by_exact_key<R, E>(
        &mut self,
        key: RootCurrentARecoveryKey,
        action: impl FnOnce(
            RootRecoverySchedulerAuthority<Q, D>,
        ) -> Result<R, (RootRecoverySchedulerAuthority<Q, D>, E)>,
    ) -> Result<R, RootRecoverySchedulerTransactionError<E>> {
        let taken = self
            .take_by_exact_key(key)
            .map_err(RootRecoverySchedulerTransactionError::Take)?;
        let (slot_index, authority) = taken.into_parts();
        let (authority_key, scheduler, timeout, publication_pending) = match authority {
            RootRecoveryTimerAuthority::Queued {
                key,
                queued,
                timeout,
                publication_pending,
            } => (
                key,
                RootRecoverySchedulerAuthority::Queued(queued),
                timeout,
                publication_pending,
            ),
            RootRecoveryTimerAuthority::Armed {
                key,
                deadline,
                timeout,
                publication_pending,
            } => (
                key,
                RootRecoverySchedulerAuthority::Armed(deadline),
                timeout,
                publication_pending,
            ),
        };
        match action(scheduler) {
            Ok(value) => Ok(value),
            Err((scheduler, error)) => {
                self.slots[slot_index] = Some(match scheduler {
                    RootRecoverySchedulerAuthority::Queued(queued) => {
                        RootRecoveryTimerAuthority::Queued {
                            key: authority_key,
                            queued,
                            timeout,
                            publication_pending,
                        }
                    }
                    RootRecoverySchedulerAuthority::Armed(deadline) => {
                        RootRecoveryTimerAuthority::Armed {
                            key: authority_key,
                            deadline,
                            timeout,
                            publication_pending,
                        }
                    }
                });
                Err(RootRecoverySchedulerTransactionError::Action(error))
            }
        }
    }

    fn restore_exact(
        &mut self,
        taken: RootRecoveryTimerTaken<Q, D>,
    ) -> Result<(), RootRecoveryTimerRestoreFailure<Q, D>> {
        let key = taken.key();
        if self
            .slots
            .iter()
            .flatten()
            .any(|authority| authority.key() == key)
        {
            return Err(RootRecoveryTimerRestoreFailure {
                kind: RootRecoveryTimerRestoreFailureKind::Duplicate,
                taken,
            });
        }
        let Some(slot) = self.slots.get_mut(taken.slot_index) else {
            return Err(RootRecoveryTimerRestoreFailure {
                kind: RootRecoveryTimerRestoreFailureKind::InvalidSlot,
                taken,
            });
        };
        if slot.is_some() {
            return Err(RootRecoveryTimerRestoreFailure {
                kind: RootRecoveryTimerRestoreFailureKind::SlotOccupied,
                taken,
            });
        }
        *slot = Some(taken.authority);
        Ok(())
    }

    fn set_publication_pending(
        &mut self,
        key: RootCurrentARecoveryKey,
        pending: bool,
    ) -> Result<(), RootRecoveryTimerPublicationFailureKind> {
        let Some(authority) = self
            .slots
            .iter_mut()
            .flatten()
            .find(|authority| authority.key() == key)
        else {
            return Err(if self.is_empty() {
                RootRecoveryTimerPublicationFailureKind::Empty
            } else {
                RootRecoveryTimerPublicationFailureKind::KeyMismatch
            });
        };
        let RootRecoveryTimerAuthority::Armed {
            publication_pending,
            ..
        } = authority
        else {
            return Err(RootRecoveryTimerPublicationFailureKind::NotArmed);
        };
        *publication_pending = pending;
        Ok(())
    }

    fn take_for_shutdown(&mut self) -> RootCurrentARecoveryShutdownDrain<Q, D> {
        RootCurrentARecoveryShutdownDrain {
            slots: std::mem::replace(&mut self.slots, std::array::from_fn(|_| None)),
        }
    }

    fn restore_for_shutdown(
        &mut self,
        drain: RootCurrentARecoveryShutdownDrain<Q, D>,
    ) -> Result<(), RootCurrentARecoveryShutdownRestoreFailure<Q, D>> {
        if drain.len() == 0 {
            return Ok(());
        }
        if !self.is_empty() {
            return Err(RootCurrentARecoveryShutdownRestoreFailure {
                kind: RootRecoveryTimerShutdownRestoreFailureKind::DestinationLive,
                drain,
            });
        }
        self.slots = drain.slots;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RootStandbyBRecoveryWakeInstallFailureKind {
    Duplicate,
    Capacity,
}

pub(super) struct RootStandbyBRecoveryWakeInstallFailure<W> {
    kind: RootStandbyBRecoveryWakeInstallFailureKind,
    wake: W,
}

impl<W> RootStandbyBRecoveryWakeInstallFailure<W> {
    const fn kind(&self) -> RootStandbyBRecoveryWakeInstallFailureKind {
        self.kind
    }

    fn into_wake(self) -> W {
        self.wake
    }
}

impl<W> fmt::Debug for RootStandbyBRecoveryWakeInstallFailure<W> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RootStandbyBRecoveryWakeInstallFailure")
            .field("kind", &self.kind)
            .field("wake", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum RootStandbyBRecoveryWakeTakeFailureKind {
    Empty,
    KeyMismatch,
}

struct RootStandbyBRecoveryWakeSlot<W> {
    wake: Option<W>,
}

impl<W> RootStandbyBRecoveryWakeSlot<W> {
    const fn new() -> Self {
        Self { wake: None }
    }

    const fn capacity(&self) -> usize {
        ROOT_STANDBY_B_RECOVERY_WAKE_SLOTS
    }

    fn len(&self) -> usize {
        usize::from(self.wake.is_some())
    }

    const fn is_empty(&self) -> bool {
        self.wake.is_none()
    }

    fn try_install(
        &mut self,
        wake: W,
    ) -> Result<RootStandbyBRecoveryKey, RootStandbyBRecoveryWakeInstallFailure<W>>
    where
        W: RootStandbyBRecoveryWakeOwner,
    {
        let key = wake.standby_b_recovery_key().clone();
        if let Some(existing) = self.wake.as_ref() {
            let kind = if existing.standby_b_recovery_key() == &key {
                RootStandbyBRecoveryWakeInstallFailureKind::Duplicate
            } else {
                RootStandbyBRecoveryWakeInstallFailureKind::Capacity
            };
            return Err(RootStandbyBRecoveryWakeInstallFailure { kind, wake });
        }
        self.wake = Some(wake);
        Ok(key)
    }

    fn take_by_exact_key(
        &mut self,
        key: &RootStandbyBRecoveryKey,
    ) -> Result<W, RootStandbyBRecoveryWakeTakeFailureKind>
    where
        W: RootStandbyBRecoveryWakeOwner,
    {
        let Some(wake) = self.wake.as_ref() else {
            return Err(RootStandbyBRecoveryWakeTakeFailureKind::Empty);
        };
        if wake.standby_b_recovery_key() != key {
            return Err(RootStandbyBRecoveryWakeTakeFailureKind::KeyMismatch);
        }
        self.wake
            .take()
            .ok_or(RootStandbyBRecoveryWakeTakeFailureKind::Empty)
    }

    fn wake_for_key(&self, key: &RootStandbyBRecoveryKey) -> Option<&W>
    where
        W: RootStandbyBRecoveryWakeOwner,
    {
        self.wake
            .as_ref()
            .filter(|wake| wake.standby_b_recovery_key() == key)
    }

    fn try_handoff(
        &mut self,
        handoff: impl FnOnce(W) -> Result<(), W>,
    ) -> RootRecoveryTimerHandoffSummary {
        let Some(wake) = self.wake.take() else {
            return RootRecoveryTimerHandoffSummary::default();
        };
        match handoff(wake) {
            Ok(()) => RootRecoveryTimerHandoffSummary {
                accepted: 1,
                rejected: 0,
            },
            Err(wake) => {
                self.wake = Some(wake);
                RootRecoveryTimerHandoffSummary {
                    accepted: 0,
                    rejected: 1,
                }
            }
        }
    }
}

pub(super) struct RootRecoveryTimerSlots<
    Q = QuinnCurrentProbeQueued,
    D = QuinnCurrentProbeDeadline,
    W = RootStandbyBRecoveryWake,
> {
    current_a: RootCurrentARecoveryTimerSlot<Q, D>,
    standby_b: RootStandbyBRecoveryWakeSlot<W>,
}

struct RootRecoveryTimerShutdownDrain<
    Q = QuinnCurrentProbeQueued,
    D = QuinnCurrentProbeDeadline,
    W = RootStandbyBRecoveryWake,
> {
    current_a: RootCurrentARecoveryShutdownDrain<Q, D>,
    standby_b: RootStandbyBRecoveryWakeSlot<W>,
}

impl<Q, D, W> RootRecoveryTimerShutdownDrain<Q, D, W> {
    fn len(&self) -> usize {
        self.current_a.len().saturating_add(self.standby_b.len())
    }

    fn live_inventory(&self) -> RootRecoveryTimerLiveInventory {
        let mut inventory = self.current_a.live_inventory();
        if !self.standby_b.is_empty() {
            inventory.observe_standby_b_wake();
        }
        inventory
    }

    fn current_a_authority_for_key(
        &self,
        key: RootCurrentARecoveryKey,
    ) -> Option<&RootRecoveryTimerAuthority<Q, D>> {
        self.current_a.authority_for_key(key)
    }

    fn standby_b_wake_for_key(&self, key: &RootStandbyBRecoveryKey) -> Option<&W>
    where
        W: RootStandbyBRecoveryWakeOwner,
    {
        self.standby_b.wake_for_key(key)
    }

    fn try_handoff_current_a(
        &mut self,
        handoff: impl FnMut(
            RootRecoverySchedulerAuthority<Q, D>,
        ) -> Result<(), RootRecoverySchedulerAuthority<Q, D>>,
    ) -> RootRecoveryTimerHandoffSummary {
        self.current_a.try_handoff_all(handoff)
    }

    fn try_handoff_standby_b_wake(
        &mut self,
        handoff: impl FnOnce(W) -> Result<(), W>,
    ) -> RootRecoveryTimerHandoffSummary {
        self.standby_b.try_handoff(handoff)
    }
}

impl<Q, D, W> fmt::Debug for RootRecoveryTimerShutdownDrain<Q, D, W> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RootRecoveryTimerShutdownDrain")
            .field("inventory", &self.live_inventory())
            .field("authorities", &"[REDACTED]")
            .finish()
    }
}

struct RootRecoveryTimerShutdownRestoreFailure<
    Q = QuinnCurrentProbeQueued,
    D = QuinnCurrentProbeDeadline,
    W = RootStandbyBRecoveryWake,
> {
    kind: RootRecoveryTimerShutdownRestoreFailureKind,
    drain: RootRecoveryTimerShutdownDrain<Q, D, W>,
}

impl<Q, D, W> RootRecoveryTimerShutdownRestoreFailure<Q, D, W> {
    const fn kind(&self) -> RootRecoveryTimerShutdownRestoreFailureKind {
        self.kind
    }

    fn into_drain(self) -> RootRecoveryTimerShutdownDrain<Q, D, W> {
        self.drain
    }
}

impl<Q, D, W> fmt::Debug for RootRecoveryTimerShutdownRestoreFailure<Q, D, W> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RootRecoveryTimerShutdownRestoreFailure")
            .field("kind", &self.kind)
            .field("drain", &self.drain)
            .finish()
    }
}

impl<Q, D, W> RootRecoveryTimerSlots<Q, D, W> {
    pub(super) fn new() -> Self {
        Self {
            current_a: RootCurrentARecoveryTimerSlot::new(),
            standby_b: RootStandbyBRecoveryWakeSlot::new(),
        }
    }

    const fn capacity(&self) -> usize {
        self.current_a
            .capacity()
            .saturating_add(self.standby_b.capacity())
    }

    fn len(&self) -> usize {
        self.current_a.len().saturating_add(self.standby_b.len())
    }

    pub(super) fn is_empty(&self) -> bool {
        self.current_a.is_empty() && self.standby_b.is_empty()
    }

    fn live_inventory(&self) -> RootRecoveryTimerLiveInventory {
        let mut inventory = self.current_a.live_inventory();
        if !self.standby_b.is_empty() {
            inventory.observe_standby_b_wake();
        }
        inventory
    }

    fn try_install_current_a_queued(
        &mut self,
        queued: Q,
        timeout: Duration,
    ) -> Result<RootCurrentARecoveryKey, RootRecoveryTimerInstallFailure<Q>>
    where
        Q: RootCurrentARecoveryQueuedOwner,
    {
        self.current_a.try_install_queued(queued, timeout)
    }

    fn try_arm_current_a_queued<E>(
        &mut self,
        key: RootCurrentARecoveryKey,
        arm: impl FnOnce(Q, Duration) -> Result<D, (Q, E)>,
    ) -> Result<(), RootRecoveryTimerArmFailure<E>> {
        self.current_a.try_arm_queued(key, arm)
    }

    fn take_current_a_by_exact_key(
        &mut self,
        key: RootCurrentARecoveryKey,
    ) -> Result<RootRecoveryTimerTaken<Q, D>, RootRecoveryTimerTakeFailureKind> {
        self.current_a.take_by_exact_key(key)
    }

    fn transact_current_a_scheduler_by_exact_key<R, E>(
        &mut self,
        key: RootCurrentARecoveryKey,
        action: impl FnOnce(
            RootRecoverySchedulerAuthority<Q, D>,
        ) -> Result<R, (RootRecoverySchedulerAuthority<Q, D>, E)>,
    ) -> Result<R, RootRecoverySchedulerTransactionError<E>> {
        self.current_a.transact_scheduler_by_exact_key(key, action)
    }

    fn restore_current_a_exact(
        &mut self,
        taken: RootRecoveryTimerTaken<Q, D>,
    ) -> Result<(), RootRecoveryTimerRestoreFailure<Q, D>> {
        self.current_a.restore_exact(taken)
    }

    fn set_current_a_publication_pending(
        &mut self,
        key: RootCurrentARecoveryKey,
        pending: bool,
    ) -> Result<(), RootRecoveryTimerPublicationFailureKind> {
        self.current_a.set_publication_pending(key, pending)
    }

    pub(super) fn try_install_standby_b_wake(
        &mut self,
        wake: W,
    ) -> Result<RootStandbyBRecoveryKey, RootStandbyBRecoveryWakeInstallFailure<W>>
    where
        W: RootStandbyBRecoveryWakeOwner,
    {
        self.standby_b.try_install(wake)
    }

    pub(super) fn take_standby_b_wake_by_exact_key(
        &mut self,
        key: &RootStandbyBRecoveryKey,
    ) -> Result<W, RootStandbyBRecoveryWakeTakeFailureKind>
    where
        W: RootStandbyBRecoveryWakeOwner,
    {
        self.standby_b.take_by_exact_key(key)
    }

    pub(super) fn standby_b_wake_for_key(&self, key: &RootStandbyBRecoveryKey) -> Option<&W>
    where
        W: RootStandbyBRecoveryWakeOwner,
    {
        self.standby_b.wake_for_key(key)
    }

    fn take_for_shutdown(&mut self) -> RootRecoveryTimerShutdownDrain<Q, D, W> {
        RootRecoveryTimerShutdownDrain {
            current_a: self.current_a.take_for_shutdown(),
            standby_b: std::mem::replace(&mut self.standby_b, RootStandbyBRecoveryWakeSlot::new()),
        }
    }

    fn restore_for_shutdown(
        &mut self,
        drain: RootRecoveryTimerShutdownDrain<Q, D, W>,
    ) -> Result<(), RootRecoveryTimerShutdownRestoreFailure<Q, D, W>> {
        if drain.len() == 0 {
            return Ok(());
        }
        if !self.is_empty() {
            return Err(RootRecoveryTimerShutdownRestoreFailure {
                kind: RootRecoveryTimerShutdownRestoreFailureKind::DestinationLive,
                drain,
            });
        }
        self.current_a.slots = drain.current_a.slots;
        self.standby_b = drain.standby_b;
        Ok(())
    }
}

impl RootStorage {
    fn preallocate(plan: QuinnClientRootPlan) -> Result<Self, &'static str> {
        let mut drivers = Vec::new();
        drivers
            .try_reserve_exact(plan.storage.geometry().max_flows())
            .map_err(|_| "flow driver registry")?;
        let mut pending_effects = VecDeque::new();
        pending_effects
            .try_reserve_exact(plan.pending_effect_capacity)
            .map_err(|_| "pending effect queue")?;
        let mut aborted_driver_inputs = Vec::new();
        aborted_driver_inputs
            .try_reserve_exact(plan.storage.geometry().max_flows())
            .map_err(|_| "aborted driver input registry")?;
        let terminal_timers =
            RootDeadlineHeap::preallocate(plan.session_config().max_terminal_tombstones())?;
        Ok(Self {
            drivers,
            next_driver: 0,
            deferred_driver_input: None,
            aborted_driver_inputs,
            pending_effects,
            aborted_effects: None,
            recovery_timers: RootRecoveryTimerSlots::new(),
            client_progress: RootClientProgressState::new(),
            terminal_timers,
            terminal_publication_pending: false,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RootEffectAdmissionFailureKind {
    IncrementalEffectBound,
    PendingEffectLogicalCapacity,
    PendingEffectPhysicalCapacity,
    TerminalDeadlineOverflow,
    TerminalTimerLogicalCapacity,
    TerminalTimerPhysicalCapacity,
    TerminalTimerSequenceExhausted,
}

struct RootEffectAdmissionFailure {
    effects: Vec<SessionEffect>,
    kind: RootEffectAdmissionFailureKind,
}

impl RootEffectAdmissionFailure {
    const fn kind(&self) -> RootEffectAdmissionFailureKind {
        self.kind
    }

    fn into_effects(self) -> Vec<SessionEffect> {
        self.effects
    }

    fn new(effects: Vec<SessionEffect>, kind: RootEffectAdmissionFailureKind) -> Self {
        Self { effects, kind }
    }
}

impl fmt::Debug for RootEffectAdmissionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RootEffectAdmissionFailure")
            .field("kind", &self.kind)
            .field("effect_count", &self.effects.len())
            .finish()
    }
}

fn try_admit_pending_effects_with_terminal_timers(
    pending_effects: &mut VecDeque<SessionEffect>,
    terminal_timers: &mut RootDeadlineHeap<QuinnTerminalGraceTimer>,
    effects: Vec<SessionEffect>,
    plan: QuinnClientRootPlan,
    observed_at: SimTime,
) -> Result<(), RootEffectAdmissionFailure> {
    if effects.len() > plan.incremental_effect_bound()
        || effects.len() > ROOT_INCREMENTAL_EFFECT_SLOTS
    {
        return Err(RootEffectAdmissionFailure::new(
            effects,
            RootEffectAdmissionFailureKind::IncrementalEffectBound,
        ));
    }
    if pending_effects
        .len()
        .checked_add(effects.len())
        .is_none_or(|needed| needed > plan.pending_effect_capacity())
    {
        return Err(RootEffectAdmissionFailure::new(
            effects,
            RootEffectAdmissionFailureKind::PendingEffectLogicalCapacity,
        ));
    }
    if pending_effects
        .capacity()
        .saturating_sub(pending_effects.len())
        < effects.len()
    {
        return Err(RootEffectAdmissionFailure::new(
            effects,
            RootEffectAdmissionFailureKind::PendingEffectPhysicalCapacity,
        ));
    }

    let terminal_count = effects
        .iter()
        .filter(|effect| matches!(effect, SessionEffect::FlowFinished { .. }))
        .count();
    let terminal_deadline = if terminal_count == 0 {
        None
    } else {
        match observed_at.checked_add(plan.terminal_grace()) {
            Ok(deadline) => Some(deadline),
            Err(_) => {
                return Err(RootEffectAdmissionFailure::new(
                    effects,
                    RootEffectAdmissionFailureKind::TerminalDeadlineOverflow,
                ));
            }
        }
    };
    let deadline_admission = match terminal_timers.preflight_batch(terminal_count) {
        Ok(admission) => admission,
        Err(error) => {
            let kind = match error {
                RootDeadlineBatchAdmissionError::LogicalCapacity => {
                    RootEffectAdmissionFailureKind::TerminalTimerLogicalCapacity
                }
                RootDeadlineBatchAdmissionError::PhysicalCapacity => {
                    RootEffectAdmissionFailureKind::TerminalTimerPhysicalCapacity
                }
                RootDeadlineBatchAdmissionError::SequenceExhausted => {
                    RootEffectAdmissionFailureKind::TerminalTimerSequenceExhausted
                }
            };
            return Err(RootEffectAdmissionFailure::new(effects, kind));
        }
    };

    // Materialize the complete timer batch on the stack before either root
    // registry changes. Sequence and absolute-deadline arithmetic therefore
    // cannot fail after the first heap write.
    let mut prepared: [Option<ScheduledRootDeadline<QuinnTerminalGraceTimer>>;
        ROOT_INCREMENTAL_EFFECT_SLOTS] = std::array::from_fn(|_| None);
    let mut terminal_index = 0usize;
    for effect in &effects {
        let SessionEffect::FlowFinished { terminal, .. } = effect else {
            continue;
        };
        let sequence_offset = match u64::try_from(terminal_index) {
            Ok(offset) => offset,
            Err(_) => {
                return Err(RootEffectAdmissionFailure::new(
                    effects,
                    RootEffectAdmissionFailureKind::TerminalTimerSequenceExhausted,
                ));
            }
        };
        let sequence = match deadline_admission
            .first_sequence
            .checked_add(sequence_offset)
        {
            Some(sequence) => sequence,
            None => {
                return Err(RootEffectAdmissionFailure::new(
                    effects,
                    RootEffectAdmissionFailureKind::TerminalTimerSequenceExhausted,
                ));
            }
        };
        let deadline = match terminal_deadline {
            Some(deadline) => deadline,
            None => {
                return Err(RootEffectAdmissionFailure::new(
                    effects,
                    RootEffectAdmissionFailureKind::TerminalDeadlineOverflow,
                ));
            }
        };
        let reset_delivery_required = effects.iter().any(|candidate| {
            matches!(
                candidate,
                SessionEffect::Transmit(frame)
                    if matches!(frame.record(), Record::Reset { flow_id, .. } if *flow_id == terminal.flow_id())
            )
        });
        let timer = if reset_delivery_required {
            QuinnTerminalGraceTimer::new_with_ordered_reset_delivery(*terminal, deadline)
        } else {
            QuinnTerminalGraceTimer::new(*terminal, deadline)
        };
        prepared[terminal_index] = Some(ScheduledRootDeadline {
            instant: deadline,
            sequence,
            timer,
        });
        terminal_index += 1;
    }

    for scheduled in prepared.into_iter().flatten() {
        terminal_timers.heap.push(scheduled);
    }
    terminal_timers.next_sequence = deadline_admission.next_sequence;
    pending_effects.extend(effects);
    Ok(())
}

fn try_admit_pending_effects(
    pending_effects: &mut VecDeque<SessionEffect>,
    effects: Vec<SessionEffect>,
    exact_capacity: usize,
) -> Result<(), Vec<SessionEffect>> {
    if pending_effects
        .len()
        .checked_add(effects.len())
        .is_none_or(|needed| needed > exact_capacity)
    {
        return Err(effects);
    }
    pending_effects.extend(effects);
    Ok(())
}

struct RootTaskBundle {
    plan: QuinnClientRootPlan,
    composition: Box<QuinnClientSessionComposition>,
    factory: ResumableTcpPortFactory,
    clock: SessionMonotonicClock,
    storage: RootStorage,
    commands: mpsc::Receiver<QuinnClientRootCommand>,
    shutdown: watch::Receiver<RootShutdownSignal>,
    lifecycle: Arc<RootLifecycle>,
    lifecycle_guard: RootTaskLifecycleGuard,
    next_work: RootWorkCategory,
    #[cfg(test)]
    open_reply_publication: Arc<RootOpenReplyPublicationHook>,
    #[cfg(test)]
    shutdown_pre_park: Arc<RootShutdownPreParkHook>,
    #[cfg(test)]
    shutdown_slice: Arc<RootShutdownSliceHook>,
    #[cfg(test)]
    run_probe: Arc<RootRunProbe>,
    #[cfg(test)]
    live_category: Arc<RootLiveCategoryHook>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RootWorkCategory {
    Command,
    Effect,
    TerminalTimer,
    LocalStallTimer,
    Driver,
    Flush,
    Inbound,
}

impl RootWorkCategory {
    const COUNT: usize = 7;

    const fn next(self) -> Self {
        match self {
            Self::Command => Self::Effect,
            Self::Effect => Self::TerminalTimer,
            Self::TerminalTimer => Self::LocalStallTimer,
            Self::LocalStallTimer => Self::Driver,
            Self::Driver => Self::Flush,
            Self::Flush => Self::Inbound,
            Self::Inbound => Self::Command,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RootShutdownWorkCategory {
    Effect,
    Driver,
    Flush,
    TerminalTimer,
}

impl RootShutdownWorkCategory {
    const COUNT: usize = 4;

    const fn next(self) -> Self {
        match self {
            Self::Effect => Self::Driver,
            Self::Driver => Self::Flush,
            Self::Flush => Self::TerminalTimer,
            Self::TerminalTimer => Self::Effect,
        }
    }
}

/// Namespace for the synchronous prepare-and-spawn transaction.
pub(crate) struct QuinnClientRoot;

impl QuinnClientRoot {
    pub(crate) fn start(
        plan: QuinnClientRootPlan,
        mut composition: Box<QuinnClientSessionComposition>,
    ) -> Result<StartedQuinnClientRoot, QuinnClientRootStartFailure> {
        if composition.session_config() != plan.session {
            return Err(QuinnClientRootStartFailure::new(
                plan,
                composition,
                QuinnClientRootStartFailureKind::SessionConfigMismatch,
            ));
        }
        if !composition.root_start_is_quiescent() {
            return Err(QuinnClientRootStartFailure::new(
                plan,
                composition,
                QuinnClientRootStartFailureKind::CompositionNotQuiescent,
            ));
        }
        let runtime = match Handle::try_current() {
            Ok(runtime) => runtime,
            Err(_) => {
                return Err(QuinnClientRootStartFailure::new(
                    plan,
                    composition,
                    QuinnClientRootStartFailureKind::RuntimeUnavailable,
                ));
            }
        };
        let storage = match RootStorage::preallocate(plan) {
            Ok(storage) => storage,
            Err(scope) => {
                return Err(QuinnClientRootStartFailure::new(
                    plan,
                    composition,
                    QuinnClientRootStartFailureKind::Allocation(scope),
                ));
            }
        };
        let (commands_tx, commands_rx) = mpsc::channel(plan.command_capacity);
        let (shutdown_tx, shutdown_rx) = watch::channel(RootShutdownSignal::Open);
        let lifecycle = RootLifecycle::new();
        // The actor/composition and root scheduler must observe one endpoint-
        // session epoch. Capture the composition-owned authority before the
        // final factory mint and task spawn; never mint a second, late epoch.
        let clock = composition.session_clock();
        let identity = Arc::new(QuinnClientRootIdentity);
        #[cfg(test)]
        let open_reply_publication = Arc::new(RootOpenReplyPublicationHook::default());
        #[cfg(test)]
        let shutdown_pre_park = Arc::new(RootShutdownPreParkHook::default());
        #[cfg(test)]
        let shutdown_slice = Arc::new(RootShutdownSliceHook::default());
        #[cfg(test)]
        let run_probe = Arc::new(RootRunProbe::default());
        #[cfg(test)]
        let live_category = Arc::new(RootLiveCategoryHook::default());
        let handle = QuinnClientRootHandle {
            identity,
            commands: commands_tx,
            lifecycle: Arc::clone(&lifecycle),
            #[cfg(test)]
            open_reply_publication: Arc::clone(&open_reply_publication),
            #[cfg(test)]
            shutdown_pre_park: Arc::clone(&shutdown_pre_park),
            #[cfg(test)]
            shutdown_slice: Arc::clone(&shutdown_slice),
            #[cfg(test)]
            run_probe: Arc::clone(&run_probe),
            #[cfg(test)]
            live_category: Arc::clone(&live_category),
        };

        // This is the final fallible ownership mutation. From the successful
        // mint through `spawn`, only infallible aggregate moves remain.
        let factory = match composition.mint_tcp_port_factory(plan.flow_port) {
            Some(Ok(factory)) => factory,
            Some(Err(error)) => {
                return Err(QuinnClientRootStartFailure::new(
                    plan,
                    composition,
                    QuinnClientRootStartFailureKind::Factory(error),
                ));
            }
            None => {
                return Err(QuinnClientRootStartFailure::new(
                    plan,
                    composition,
                    QuinnClientRootStartFailureKind::CompositionNotRunning,
                ));
            }
        };

        let task = runtime.spawn(run_root(RootTaskBundle {
            plan,
            composition,
            factory,
            clock,
            storage,
            commands: commands_rx,
            shutdown: shutdown_rx,
            lifecycle: Arc::clone(&lifecycle),
            lifecycle_guard: RootTaskLifecycleGuard::new(lifecycle),
            next_work: RootWorkCategory::Command,
            #[cfg(test)]
            open_reply_publication,
            #[cfg(test)]
            shutdown_pre_park,
            #[cfg(test)]
            shutdown_slice,
            #[cfg(test)]
            run_probe,
            #[cfg(test)]
            live_category,
        }));
        Ok(StartedQuinnClientRoot {
            handle,
            shutdown: shutdown_tx,
            task: Some(task),
            observed: None,
            runtime,
        })
    }
}

/// Non-Clone sole observer/join owner. Waiting borrows this value so cancelled
/// callers cannot consume or detach the underlying task.
pub(crate) struct StartedQuinnClientRoot {
    handle: QuinnClientRootHandle,
    shutdown: watch::Sender<RootShutdownSignal>,
    task: Option<JoinHandle<QuinnClientRootOutcome>>,
    observed: Option<QuinnClientRootOutcome>,
    runtime: Handle,
}

impl StartedQuinnClientRoot {
    #[cfg(test)]
    pub(crate) fn arm_open_reply_publication_block_for_test(
        &self,
    ) -> RootOpenReplyPublicationControl {
        self.handle.open_reply_publication.arm()
    }

    #[cfg(test)]
    pub(crate) fn arm_shutdown_pre_park_block_for_test(&self) -> RootShutdownPreParkControl {
        self.handle.shutdown_pre_park.arm()
    }

    #[cfg(test)]
    pub(crate) fn arm_shutdown_before_terminal_timer_slice_for_test(
        &self,
    ) -> RootShutdownSliceControl {
        self.handle
            .shutdown_slice
            .arm(RootShutdownWorkCategory::TerminalTimer)
    }

    /// Stops lifecycle admission without publishing the watch wake. This
    /// exposes the exact production CAS-before-signal window to integration
    /// tests without changing the production shutdown transaction.
    #[cfg(test)]
    pub(crate) fn begin_shutdown_lifecycle_only_for_test(
        &self,
    ) -> Result<(), QuinnClientRootPhase> {
        self.handle.lifecycle.begin_shutdown()
    }

    #[cfg(test)]
    pub(crate) fn shutdown_signal_is_open_for_test(&self) -> bool {
        *self.shutdown.borrow() == RootShutdownSignal::Open
    }

    #[cfg(test)]
    pub(crate) fn live_scan_count_for_test(&self) -> usize {
        self.handle.run_probe.live_scans()
    }

    #[cfg(test)]
    pub(crate) fn live_wait_count_for_test(&self) -> usize {
        self.handle.run_probe.live_waits()
    }

    #[cfg(test)]
    pub(crate) fn shutdown_cleanup_started_for_test(&self) -> bool {
        self.handle.run_probe.shutdown_cleanup_started()
    }

    #[cfg(test)]
    pub(crate) fn arm_before_live_inbound_for_test(&self) -> RootLiveCategoryControl {
        self.handle
            .live_category
            .arm(RootLiveBoundary::Category(RootWorkCategory::Inbound))
    }

    #[cfg(test)]
    pub(crate) fn arm_after_selected_command_for_test(&self) -> RootLiveCategoryControl {
        self.handle
            .live_category
            .arm(RootLiveBoundary::SelectedCommand)
    }

    pub(crate) async fn wait_ready(
        &self,
    ) -> Result<QuinnClientRootHandle, QuinnClientRootReadyError> {
        // Subscribe before inspecting phase so a publication between the
        // initial check and `changed` cannot be lost.
        let mut lifecycle = self.handle.lifecycle.subscribe();
        loop {
            let phase = *lifecycle.borrow_and_update();
            match phase {
                QuinnClientRootPhase::Starting => {}
                QuinnClientRootPhase::Running => return Ok(self.handle.clone()),
                phase @ (QuinnClientRootPhase::ShuttingDown
                | QuinnClientRootPhase::Joined
                | QuinnClientRootPhase::FailedJoined
                | QuinnClientRootPhase::TaskLost) => {
                    return Err(QuinnClientRootReadyError::StoppedBeforeReady(phase));
                }
            }
            lifecycle
                .changed()
                .await
                .map_err(|_| QuinnClientRootReadyError::LifecycleSourceClosed)?;
        }
    }

    pub(crate) fn begin_shutdown(&mut self) -> QuinnClientRootShutdownDisposition {
        let (_phase, previous) = begin_root_shutdown_signal(
            &self.handle.lifecycle,
            &self.shutdown,
            RootShutdownSignal::Explicit,
            || {},
        );
        if previous == RootShutdownSignal::Open {
            QuinnClientRootShutdownDisposition::Started
        } else {
            QuinnClientRootShutdownDisposition::AlreadyStarted
        }
    }

    pub(crate) async fn join(&mut self) -> &QuinnClientRootOutcome {
        let _disposition = self.begin_shutdown();
        if self.observed.is_none() {
            let observed = match self.task.as_mut() {
                Some(task) => match task.await {
                    Ok(QuinnClientRootOutcome::Joined(joined)) => {
                        match self.handle.lifecycle.transition(
                            QuinnClientRootPhase::ShuttingDown,
                            QuinnClientRootPhase::Joined,
                        ) {
                            Ok(()) => QuinnClientRootOutcome::Joined(joined),
                            Err(phase) => {
                                self.handle.lifecycle.publish_failed_joined();
                                QuinnClientRootOutcome::FailedJoined {
                                    joined: Some(joined),
                                    first_failure: Arc::from(format!(
                                        "clean join observation found lifecycle phase {phase:?}"
                                    )),
                                }
                            }
                        }
                    }
                    Ok(outcome) => outcome,
                    Err(error) => {
                        self.handle.lifecycle.publish_task_lost();
                        QuinnClientRootOutcome::TaskLost(Arc::from(error.to_string()))
                    }
                },
                None => {
                    self.handle.lifecycle.publish_task_lost();
                    QuinnClientRootOutcome::TaskLost(Arc::from("root task owner missing"))
                }
            };
            self.observed = Some(observed);
            self.task = None;
        }
        self.observed.as_ref().expect("root observation stored")
    }
}

impl Drop for StartedQuinnClientRoot {
    fn drop(&mut self) {
        if self.observed.is_some() {
            return;
        }
        let (_phase, _previous) = begin_root_shutdown_signal(
            &self.handle.lifecycle,
            &self.shutdown,
            RootShutdownSignal::OwnerDropped,
            || {},
        );
        if let Some(task) = self.task.take() {
            let lifecycle = Arc::clone(&self.handle.lifecycle);
            let mut janitor_guard = RootTaskLifecycleGuard::new(Arc::clone(&lifecycle));
            self.runtime.spawn(async move {
                match task.await {
                    Ok(QuinnClientRootOutcome::Joined(_))
                    | Ok(QuinnClientRootOutcome::FailedJoined { .. }) => {
                        lifecycle.publish_failed_joined();
                    }
                    Ok(QuinnClientRootOutcome::TaskLost(_)) | Err(_) => {
                        lifecycle.publish_task_lost();
                    }
                }
                janitor_guard.disarm();
            });
        }
    }
}

enum RootScan {
    Idle,
    Progressed,
    Failed(Arc<str>),
}

fn retain_effect_failure(
    bundle: &mut RootTaskBundle,
    effect: SessionEffect,
    message: impl Into<Arc<str>>,
) -> RootScan {
    if bundle.storage.aborted_effects.is_none() {
        bundle.storage.aborted_effects = Some(vec![effect]);
    }
    RootScan::Failed(message.into())
}

fn admit_root_effects(
    bundle: &mut RootTaskBundle,
    effects: Vec<SessionEffect>,
    now: SimTime,
) -> Result<(), Arc<str>> {
    if let Err(failure) = try_admit_pending_effects_with_terminal_timers(
        &mut bundle.storage.pending_effects,
        &mut bundle.storage.terminal_timers,
        effects,
        bundle.plan,
        now,
    ) {
        let kind = failure.kind();
        if bundle.storage.aborted_effects.is_none() {
            bundle.storage.aborted_effects = Some(failure.into_effects());
        }
        return Err(Arc::from(format!(
            "root effect/timer admission invariant: {kind:?}"
        )));
    }
    Ok(())
}

fn process_root_command(bundle: &mut RootTaskBundle, command: QuinnClientRootCommand) -> RootScan {
    match command {
        QuinnClientRootCommand::OpenTcp { target, reply } => {
            if reply.is_closed() {
                drop(target);
                return RootScan::Progressed;
            }
            if bundle.storage.aborted_effects.is_some()
                || !bundle.storage.pending_effects.is_empty()
                || bundle.storage.deferred_driver_input.is_some()
            {
                let _sent = reply.send(Err(
                    QuinnClientRootOpenFailureKind::PendingEffectsNotDrained,
                ));
                drop(target);
                return RootScan::Progressed;
            }
            let max = bundle.plan.storage.geometry().max_flows();
            if bundle.storage.drivers.len() >= max {
                let _sent = reply.send(Err(QuinnClientRootOpenFailureKind::FlowRegistryCapacity {
                    max,
                }));
                drop(target);
                return RootScan::Progressed;
            }
            let committed = match bundle
                .composition
                .open_prepared_tcp(target, &bundle.factory)
            {
                Ok(committed) => committed,
                Err(failure) => {
                    let kind = failure.kind();
                    drop(failure.into_target());
                    let _sent = reply.send(Err(QuinnClientRootOpenFailureKind::Composition(
                        QuinnClientRootCompositionOpenFailureKind::from_composition(kind),
                    )));
                    return RootScan::Progressed;
                }
            };
            let (flow, driver, local_flow, _enqueue_ordinal) = committed.into_parts();
            debug_assert_eq!(flow.local_flow(), local_flow);
            debug_assert_eq!(driver.local_flow(), local_flow);
            debug_assert!(bundle.storage.drivers.capacity() > bundle.storage.drivers.len());
            bundle.storage.drivers.push(driver);

            #[cfg(test)]
            bundle.open_reply_publication.pause_if_armed();

            if let Err(unsent) = reply.send(Ok(flow)) {
                let Ok(flow) = unsent else {
                    return RootScan::Failed(Arc::from(
                        "root OPEN success reply changed into an error after commit",
                    ));
                };
                if let Err(error) = flow.abandon_before_install() {
                    return RootScan::Failed(Arc::from(format!(
                        "cancelled OPEN reply could not retain LocalAbandon: {error}"
                    )));
                }
            }
            RootScan::Progressed
        }
    }
}

fn reject_root_command_after_lifecycle_cutoff(
    command: QuinnClientRootCommand,
    phase: QuinnClientRootPhase,
) {
    match command {
        QuinnClientRootCommand::OpenTcp { target, reply } => {
            drop(target);
            let _sent = reply.send(Err(QuinnClientRootOpenFailureKind::RootNotRunning(phase)));
        }
    }
}

fn apply_root_driver_input(
    bundle: &mut RootTaskBundle,
    input: ClientControllerDriverInput,
    now: SimTime,
) -> RootScan {
    match bundle.composition.apply_driver_input_observed(input) {
        Ok(ClientControllerObservedDriverApply::Applied(observed)) => {
            let (effects, observations) = observed.into_parts();
            finish_observed_reducer_turn(bundle, effects, observations, now)
        }
        Ok(ClientControllerObservedDriverApply::Deferred { input, .. }) => {
            if bundle
                .storage
                .deferred_driver_input
                .replace(input)
                .is_some()
            {
                RootScan::Failed(Arc::from("root replaced an already-retained driver input"))
            } else {
                RootScan::Idle
            }
        }
        Ok(ClientControllerObservedDriverApply::Failed(error)) => {
            RootScan::Failed(Arc::from(format!("root driver reducer failed: {error:?}")))
        }
        Err(rejected) => {
            if bundle
                .storage
                .deferred_driver_input
                .replace(rejected.into_input())
                .is_some()
            {
                RootScan::Failed(Arc::from(
                    "actor rejection replaced an already-retained driver input",
                ))
            } else {
                RootScan::Failed(Arc::from("current actor rejected an exact driver input"))
            }
        }
    }
}

fn root_progress_error(context: &str, error: RootClientProgressError) -> Arc<str> {
    Arc::from(format!("root client progress {context} failed: {error:?}"))
}

/// Consume the fixed reducer observation batch even if the accompanying
/// effect batch later fails checked admission. The reducer turn has already
/// committed, so dropping either half would fabricate a different history.
fn finish_observed_reducer_turn(
    bundle: &mut RootTaskBundle,
    effects: Vec<SessionEffect>,
    observations: ClientControllerReducerObservations,
    now: SimTime,
) -> RootScan {
    let view = RootReducerObservationView::from_controller(&observations);
    let progress = bundle.storage.client_progress.observe_reducer(
        view,
        now,
        bundle.plan.recovery().reverse_last_ack_to_stall(),
    );
    let admission = admit_root_effects(bundle, effects, now);
    match (progress, admission) {
        (Ok(()), Ok(())) => RootScan::Progressed,
        (Err(progress), Ok(())) => {
            RootScan::Failed(root_progress_error("reducer observation", progress))
        }
        (Ok(()), Err(admission)) => RootScan::Failed(admission),
        (Err(progress), Err(admission)) => RootScan::Failed(Arc::from(format!(
            "{}; {admission}",
            root_progress_error("reducer observation", progress)
        ))),
    }
}

/// Actor inbound observations already contain their endpoint-session arrival
/// stamp. This function deliberately has no root-service-time input other than
/// the effect admission timestamp, so delayed scheduling cannot refresh ACK
/// freshness with a later clock sample.
fn finish_observed_inbound_turn(
    bundle: &mut RootTaskBundle,
    effects: Vec<SessionEffect>,
    observation: Option<ClientInboundObservation>,
    effect_observed_at: SimTime,
) -> RootScan {
    let progress = bundle.storage.client_progress.observe_inbound(observation);
    let admission = admit_root_effects(bundle, effects, effect_observed_at);
    match (progress, admission) {
        (Ok(()), Ok(())) => RootScan::Progressed,
        (Err(progress), Ok(())) => {
            RootScan::Failed(root_progress_error("inbound observation", progress))
        }
        (Ok(()), Err(admission)) => RootScan::Failed(admission),
        (Err(progress), Err(admission)) => RootScan::Failed(Arc::from(format!(
            "{}; {admission}",
            root_progress_error("inbound observation", progress)
        ))),
    }
}

fn apply_root_shutdown_driver_input(
    bundle: &mut RootTaskBundle,
    input: ClientControllerDriverInput,
    now: SimTime,
) -> RootScan {
    match bundle.composition.apply_root_shutdown_driver_input(input) {
        Ok(ClientControllerDriverApply::Applied(effects)) => {
            match admit_root_effects(bundle, effects, now) {
                Ok(()) => RootScan::Progressed,
                Err(error) => RootScan::Failed(error),
            }
        }
        Ok(ClientControllerDriverApply::Deferred { input, .. }) => {
            if bundle
                .storage
                .deferred_driver_input
                .replace(input)
                .is_some()
            {
                RootScan::Failed(Arc::from(
                    "shutdown replaced an already-retained driver input",
                ))
            } else {
                RootScan::Idle
            }
        }
        Ok(ClientControllerDriverApply::Failed(error)) => RootScan::Failed(Arc::from(format!(
            "shutdown driver reducer failed: {error:?}"
        ))),
        Err(rejected) => {
            if bundle
                .storage
                .deferred_driver_input
                .replace(rejected.into_input())
                .is_some()
            {
                RootScan::Failed(Arc::from(
                    "shutdown rejection replaced retained driver input",
                ))
            } else {
                RootScan::Failed(Arc::from(
                    "shutdown driver input reached an unsealed factory",
                ))
            }
        }
    }
}

fn drive_one_root_driver(bundle: &mut RootTaskBundle, now: SimTime) -> RootScan {
    if !bundle.storage.pending_effects.is_empty() || bundle.storage.aborted_effects.is_some() {
        return RootScan::Idle;
    }
    if let Some(input) = bundle.storage.deferred_driver_input.take() {
        return apply_root_driver_input(bundle, input, now);
    }
    if bundle.storage.drivers.is_empty() {
        bundle.storage.next_driver = 0;
        return RootScan::Idle;
    }
    let index = bundle.storage.next_driver % bundle.storage.drivers.len();
    bundle.storage.next_driver = (index + 1) % bundle.storage.drivers.len();
    match bundle.storage.drivers[index].try_recv_next_for_client_controller() {
        Ok(Some(input)) => apply_root_driver_input(bundle, input, now),
        Ok(None) => RootScan::Idle,
        Err(FlowPortError::Closed) => {
            match bundle.storage.drivers[index].seal_for_local_abandon() {
                TcpDriverLocalAbandonSeal::Sealed(input) => {
                    apply_root_driver_input(bundle, input, now)
                }
                TcpDriverLocalAbandonSeal::TerminalRetired => {
                    bundle.storage.drivers.swap_remove(index);
                    if bundle.storage.drivers.is_empty() {
                        bundle.storage.next_driver = 0;
                    } else {
                        bundle.storage.next_driver %= bundle.storage.drivers.len();
                    }
                    RootScan::Progressed
                }
                TcpDriverLocalAbandonSeal::PriorResetAlreadyDequeued
                | TcpDriverLocalAbandonSeal::AlreadySealed => RootScan::Idle,
            }
        }
        Err(error) => RootScan::Failed(Arc::from(format!("root TCP driver scan failed: {error}"))),
    }
}

fn find_driver(bundle: &RootTaskBundle, flow_id: crate::resumable::SessionFlowId) -> Option<usize> {
    bundle
        .storage
        .drivers
        .iter()
        .position(|driver| driver.local_flow().flow_id() == flow_id)
}

fn retryable_downlink_pressure(error: FlowPortError) -> bool {
    matches!(
        error,
        FlowPortError::DownlinkLaneFull | FlowPortError::ControlLaneFull
    )
}

fn dispatch_one_root_effect(bundle: &mut RootTaskBundle) -> RootScan {
    if bundle.storage.aborted_effects.is_some() {
        return RootScan::Idle;
    }
    let Some(effect) = bundle.storage.pending_effects.pop_front() else {
        return RootScan::Idle;
    };
    match effect {
        SessionEffect::Transmit(frame) => {
            let reset_delivery = match frame.record() {
                Record::Reset { flow_id, .. } => bundle
                    .storage
                    .terminal_timers
                    .reset_delivery_for_queue(*flow_id),
                _ => None,
            };
            let enqueue = match reset_delivery {
                Some(reset_delivery) => bundle
                    .composition
                    .enqueue_current_reset_frame(frame, reset_delivery),
                None => bundle.composition.enqueue_current_frame(frame),
            };
            match enqueue {
                Ok(()) => RootScan::Progressed,
                Err(error) => {
                    let retryable = matches!(
                        error.kind(),
                        QuinnCurrentEnqueueErrorKind::Queue(kind)
                            if matches!(kind.as_ref(), LegOutboundQueueErrorKind::CapacityExceeded { .. })
                    );
                    let effect = SessionEffect::Transmit(error.into_frame());
                    if retryable {
                        bundle.storage.pending_effects.push_front(effect);
                        RootScan::Idle
                    } else {
                        retain_effect_failure(
                            bundle,
                            effect,
                            Arc::from("root transmit effect hit a terminal current-FIFO rejection"),
                        )
                    }
                }
            }
        }
        effect @ SessionEffect::FlowFinished { flow_id, .. } => {
            let Some(index) = find_driver(bundle, flow_id) else {
                return retain_effect_failure(
                    bundle,
                    effect,
                    Arc::from("root terminal effect has no matching driver"),
                );
            };
            match bundle.storage.drivers[index].try_deliver_terminal_effect(&effect) {
                Ok(_) => {
                    bundle.storage.drivers.swap_remove(index);
                    if bundle.storage.drivers.is_empty() {
                        bundle.storage.next_driver = 0;
                    } else {
                        bundle.storage.next_driver %= bundle.storage.drivers.len();
                    }
                    RootScan::Progressed
                }
                Err(error) if retryable_downlink_pressure(error) => {
                    bundle.storage.pending_effects.push_front(effect);
                    RootScan::Idle
                }
                Err(error) => retain_effect_failure(
                    bundle,
                    effect,
                    Arc::from(format!("root terminal delivery failed: {error}")),
                ),
            }
        }
        SessionEffect::OfferToSink { offer, segments } => {
            let Some(index) = find_driver(bundle, offer.flow_id()) else {
                return retain_effect_failure(
                    bundle,
                    SessionEffect::OfferToSink { offer, segments },
                    Arc::from("root sink offer has no matching driver"),
                );
            };
            match bundle.storage.drivers[index].try_deliver_sink_exact(offer, segments) {
                Ok(()) => RootScan::Progressed,
                Err(failure) => {
                    let kind = failure.kind();
                    let (offer, segments) = failure.into_parts();
                    let effect = SessionEffect::OfferToSink { offer, segments };
                    if retryable_downlink_pressure(kind) {
                        bundle.storage.pending_effects.push_front(effect);
                        RootScan::Idle
                    } else if matches!(
                        kind,
                        FlowPortError::Closed | FlowPortError::DriverAdmissionSealed
                    ) {
                        RootScan::Progressed
                    } else {
                        retain_effect_failure(
                            bundle,
                            effect,
                            Arc::from(format!("root sink delivery failed: {kind}")),
                        )
                    }
                }
            }
        }
        SessionEffect::HalfCloseSink { completion } => {
            let Some(index) = find_driver(bundle, completion.flow_id()) else {
                return retain_effect_failure(
                    bundle,
                    SessionEffect::HalfCloseSink { completion },
                    Arc::from("root half-close has no matching driver"),
                );
            };
            match bundle.storage.drivers[index].try_deliver_half_close_exact(completion) {
                Ok(()) => RootScan::Progressed,
                Err(failure) => {
                    let kind = failure.kind();
                    let completion = failure.into_capability();
                    let effect = SessionEffect::HalfCloseSink { completion };
                    if retryable_downlink_pressure(kind) {
                        bundle.storage.pending_effects.push_front(effect);
                        RootScan::Idle
                    } else if matches!(
                        kind,
                        FlowPortError::Closed | FlowPortError::DriverAdmissionSealed
                    ) {
                        RootScan::Progressed
                    } else {
                        retain_effect_failure(
                            bundle,
                            effect,
                            Arc::from(format!("root half-close delivery failed: {kind}")),
                        )
                    }
                }
            }
        }
        SessionEffect::LegActivated { .. }
        | SessionEffect::ResumeGraceStarted { .. }
        | SessionEffect::SessionExpired
        | SessionEffect::LocalOpenResolved { .. }
        | SessionEffect::PeerReset { .. } => RootScan::Progressed,
        effect @ (SessionEffect::ReplayStored { .. }
        | SessionEffect::ReplayAcknowledged { .. }
        | SessionEffect::LocalFlowOpened { .. }
        | SessionEffect::PeerOpenRequested { .. }) => retain_effect_failure(
            bundle,
            effect,
            Arc::from("root received an effect outside the client adapter contract"),
        ),
    }
}

/// Once whole-session shutdown has sealed future flow admission, ordinary
/// data-plane work cannot outrank adoption of each live driver's exact local
/// abandon. Drop already-reduced DATA/CLOSE/ACK and local sink deliveries;
/// the reducer's subsequent LocalAbandon releases their replay/receive
/// ownership. RESET and FlowFinished remain the sole mandatory terminal
/// chain and continue through the normal exact dispatcher.
fn dispatch_one_shutdown_effect(bundle: &mut RootTaskBundle) -> RootScan {
    let Some(effect) = bundle.storage.pending_effects.front() else {
        return RootScan::Idle;
    };
    if shutdown_effect_is_mandatory(effect) {
        return dispatch_one_root_effect(bundle);
    }
    let _superseded = bundle.storage.pending_effects.pop_front();
    RootScan::Progressed
}

fn shutdown_effect_is_mandatory(effect: &SessionEffect) -> bool {
    matches!(effect, SessionEffect::FlowFinished { .. })
        || matches!(effect, SessionEffect::Transmit(frame) if matches!(frame.record(), Record::Reset { .. }))
}

fn reinsert_terminal_timer(
    timers: &mut RootDeadlineHeap<QuinnTerminalGraceTimer>,
    instant: SimTime,
    sequence: u64,
    timer: QuinnTerminalGraceTimer,
) -> bool {
    timers
        .reinsert(ScheduledRootDeadline {
            instant,
            sequence,
            timer,
        })
        .is_ok()
}

/// Pending effects are not a timer-arbitration lock. In particular, bounded
/// sink/FIFO pressure may keep the head effect retryable past an absolute
/// terminal deadline. The due timer must still inspect the actor publication
/// boundary; any newly emitted effects then go through the ordinary checked
/// batch admission below. A retained admission failure is different: exact
/// effect ownership is already in the abort slot, so no further reducer
/// mutation is legal.
fn terminal_timer_scan_admission(storage: &RootStorage) -> Result<(), Arc<str>> {
    if storage.aborted_effects.is_some() {
        Err(Arc::from(
            "root terminal timer reached retained effect admission failure",
        ))
    } else {
        Ok(())
    }
}

fn live_terminal_wait_deadline(
    next_deadline: Option<SimTime>,
    publication_pending: bool,
) -> Option<SimTime> {
    if publication_pending {
        None
    } else {
        next_deadline
    }
}

fn drive_one_terminal_timer(bundle: &mut RootTaskBundle, now: SimTime) -> RootScan {
    if let Err(error) = terminal_timer_scan_admission(&bundle.storage) {
        return RootScan::Failed(error);
    }
    let Some(scheduled) = bundle.storage.terminal_timers.pop_due(now) else {
        return RootScan::Idle;
    };
    let instant = scheduled.instant;
    let sequence = scheduled.sequence;
    let timer = scheduled.into_timer();
    let drive = bundle
        .composition
        .try_receive_current_before_terminal_grace(timer, now);
    match drive {
        QuinnTerminalGraceTimerDrive::Expired { effects } => {
            bundle.storage.terminal_publication_pending = false;
            match admit_root_effects(bundle, effects, now) {
                Ok(()) => RootScan::Progressed,
                Err(error) => RootScan::Failed(error),
            }
        }
        QuinnTerminalGraceTimerDrive::NotDue(timer) => {
            bundle.storage.terminal_publication_pending = false;
            if reinsert_terminal_timer(
                &mut bundle.storage.terminal_timers,
                instant,
                sequence,
                timer,
            ) {
                RootScan::Idle
            } else {
                RootScan::Failed(Arc::from("root could not restore a popped terminal timer"))
            }
        }
        QuinnTerminalGraceTimerDrive::PublicationPending(timer) => {
            bundle.storage.terminal_publication_pending = true;
            if reinsert_terminal_timer(
                &mut bundle.storage.terminal_timers,
                instant,
                sequence,
                timer,
            ) {
                RootScan::Idle
            } else {
                RootScan::Failed(Arc::from(
                    "root could not restore a publication-blocked terminal timer",
                ))
            }
        }
        QuinnTerminalGraceTimerDrive::Receive { outcome, timer } => {
            bundle.storage.terminal_publication_pending = false;
            if !reinsert_terminal_timer(
                &mut bundle.storage.terminal_timers,
                instant,
                sequence,
                timer,
            ) {
                return RootScan::Failed(Arc::from(
                    "root could not restore inbound-winning terminal timer",
                ));
            }
            match outcome {
                QuinnCurrentInboundDrive::Applied {
                    effects,
                    observation,
                } => finish_observed_inbound_turn(bundle, effects, observation, now),
                QuinnCurrentInboundDrive::ProbeAcked {
                    generation,
                    probe_sequence,
                } => match reconcile_root_probe_ack(bundle, generation, probe_sequence) {
                    Ok(()) => RootScan::Progressed,
                    Err(error) => RootScan::Failed(error),
                },
                QuinnCurrentInboundDrive::Rejected => RootScan::Progressed,
                QuinnCurrentInboundDrive::Idle => RootScan::Idle,
                _ => RootScan::Failed(Arc::from(
                    "terminal timer inbound arbitration returned a non-live disposition",
                )),
            }
        }
        QuinnTerminalGraceTimerDrive::ResetDeliveryDeadlineWon(timer) => {
            bundle.storage.terminal_publication_pending = false;
            let _restored = reinsert_terminal_timer(
                &mut bundle.storage.terminal_timers,
                instant,
                sequence,
                timer,
            );
            RootScan::Failed(Arc::from(
                "terminal RESET delivery exceeded its absolute deadline",
            ))
        }
        QuinnTerminalGraceTimerDrive::ReducerInvariant { timer, error } => {
            bundle.storage.terminal_publication_pending = false;
            let _restored = reinsert_terminal_timer(
                &mut bundle.storage.terminal_timers,
                instant,
                sequence,
                timer,
            );
            RootScan::Failed(Arc::from(format!(
                "terminal timer reducer invariant: {error:?}"
            )))
        }
        QuinnTerminalGraceTimerDrive::ActorFailure { timer, cause } => {
            bundle.storage.terminal_publication_pending = false;
            let _restored = reinsert_terminal_timer(
                &mut bundle.storage.terminal_timers,
                instant,
                sequence,
                timer,
            );
            RootScan::Failed(Arc::from(format!(
                "terminal timer actor failure: {cause:?}"
            )))
        }
        QuinnTerminalGraceTimerDrive::SessionShuttingDown(timer)
        | QuinnTerminalGraceTimerDrive::CompositionInvariant(timer) => {
            bundle.storage.terminal_publication_pending = false;
            let _restored = reinsert_terminal_timer(
                &mut bundle.storage.terminal_timers,
                instant,
                sequence,
                timer,
            );
            RootScan::Failed(Arc::from(
                "terminal timer lost its live composition transaction",
            ))
        }
    }
}

fn force_one_terminal_timer_for_shutdown(bundle: &mut RootTaskBundle, now: SimTime) -> RootScan {
    bundle.storage.terminal_publication_pending = false;
    if let Err(error) = terminal_timer_scan_admission(&bundle.storage) {
        return RootScan::Failed(error);
    }
    let Some(scheduled) = bundle.storage.terminal_timers.pop_next() else {
        return RootScan::Idle;
    };
    let instant = scheduled.instant;
    let sequence = scheduled.sequence;
    let timer = scheduled.into_timer();
    if timer.reset_delivery_pending() {
        if instant > now {
            return if reinsert_terminal_timer(
                &mut bundle.storage.terminal_timers,
                instant,
                sequence,
                timer,
            ) {
                RootScan::Idle
            } else {
                RootScan::Failed(Arc::from(
                    "root could not restore a RESET-bound shutdown timer",
                ))
            };
        }
        match timer.try_commit_reset_delivery_deadline() {
            OrderedResetDeadlineCommit::DeadlineWon => {
                let _restored = reinsert_terminal_timer(
                    &mut bundle.storage.terminal_timers,
                    instant,
                    sequence,
                    timer,
                );
                return RootScan::Failed(Arc::from(
                    "terminal RESET delivery exceeded its absolute deadline",
                ));
            }
            OrderedResetDeadlineCommit::Delivered => {}
        }
    }
    match bundle
        .composition
        .force_expire_terminal_grace_for_root_shutdown(timer)
    {
        QuinnTerminalGraceTimerDrive::Expired { effects } => {
            match admit_root_effects(bundle, effects, now) {
                Ok(()) => RootScan::Progressed,
                Err(error) => RootScan::Failed(error),
            }
        }
        QuinnTerminalGraceTimerDrive::ReducerInvariant { timer, error } => {
            let _restored = reinsert_terminal_timer(
                &mut bundle.storage.terminal_timers,
                instant,
                sequence,
                timer,
            );
            RootScan::Failed(Arc::from(format!(
                "forced shutdown terminal timer reducer invariant: {error:?}"
            )))
        }
        QuinnTerminalGraceTimerDrive::NotDue(timer)
        | QuinnTerminalGraceTimerDrive::PublicationPending(timer)
        | QuinnTerminalGraceTimerDrive::SessionShuttingDown(timer)
        | QuinnTerminalGraceTimerDrive::CompositionInvariant(timer) => {
            let _restored = reinsert_terminal_timer(
                &mut bundle.storage.terminal_timers,
                instant,
                sequence,
                timer,
            );
            RootScan::Failed(Arc::from("forced shutdown terminal timer was not consumed"))
        }
        QuinnTerminalGraceTimerDrive::Receive { timer, .. }
        | QuinnTerminalGraceTimerDrive::ResetDeliveryDeadlineWon(timer)
        | QuinnTerminalGraceTimerDrive::ActorFailure { timer, .. } => {
            let _restored = reinsert_terminal_timer(
                &mut bundle.storage.terminal_timers,
                instant,
                sequence,
                timer,
            );
            RootScan::Failed(Arc::from(
                "forced shutdown timer unexpectedly depended on actor arbitration",
            ))
        }
    }
}

/// Once the physical actor has already failed, an undelivered RESET cannot
/// make future progress. Retire its exact receipt before consuming the
/// semantic tombstone so failed shutdown can still return a zero witness.
/// The original actor/transport error remains the sticky first failure; this
/// path does not relabel physical failure as a RESET deadline failure.
fn force_one_terminal_timer_after_transport_failure(
    bundle: &mut RootTaskBundle,
    now: SimTime,
) -> RootScan {
    bundle.storage.terminal_publication_pending = false;
    if let Err(error) = terminal_timer_scan_admission(&bundle.storage) {
        return RootScan::Failed(error);
    }
    let Some(scheduled) = bundle.storage.terminal_timers.pop_next() else {
        return RootScan::Idle;
    };
    let instant = scheduled.instant;
    let sequence = scheduled.sequence;
    let timer = scheduled.into_timer();
    let _receipt_terminalized = timer.try_commit_reset_delivery_deadline();
    match bundle
        .composition
        .force_expire_terminal_grace_for_root_shutdown(timer)
    {
        QuinnTerminalGraceTimerDrive::Expired { effects } => {
            match admit_root_effects(bundle, effects, now) {
                Ok(()) => RootScan::Progressed,
                Err(error) => RootScan::Failed(error),
            }
        }
        QuinnTerminalGraceTimerDrive::ReducerInvariant { timer, error } => {
            let _restored = reinsert_terminal_timer(
                &mut bundle.storage.terminal_timers,
                instant,
                sequence,
                timer,
            );
            RootScan::Failed(Arc::from(format!(
                "failed-transport terminal timer reducer invariant: {error:?}"
            )))
        }
        QuinnTerminalGraceTimerDrive::NotDue(timer)
        | QuinnTerminalGraceTimerDrive::PublicationPending(timer)
        | QuinnTerminalGraceTimerDrive::SessionShuttingDown(timer)
        | QuinnTerminalGraceTimerDrive::CompositionInvariant(timer)
        | QuinnTerminalGraceTimerDrive::Receive { timer, .. }
        | QuinnTerminalGraceTimerDrive::ResetDeliveryDeadlineWon(timer)
        | QuinnTerminalGraceTimerDrive::ActorFailure { timer, .. } => {
            let _restored = reinsert_terminal_timer(
                &mut bundle.storage.terminal_timers,
                instant,
                sequence,
                timer,
            );
            RootScan::Failed(Arc::from(
                "failed-transport terminal timer was not consumed",
            ))
        }
    }
}

fn flush_current_once(bundle: &mut RootTaskBundle, now: SimTime) -> RootScan {
    let flush = match bundle.composition.try_flush_current(now) {
        Ok(QuinnCurrentFlushDisposition::Submitted) => RootScan::Progressed,
        Ok(
            QuinnCurrentFlushDisposition::Idle
            | QuinnCurrentFlushDisposition::OrderedDeliveryPending,
        ) => RootScan::Idle,
        // The fixed actor owns a separately bounded outbound lane. When that
        // lane is full, LegIo has retained the exact FIFO head and the actor's
        // readiness epoch is the retry authority: the writer advances it only
        // after releasing the count/byte permits. Treating this as fatal would
        // tear down an otherwise healthy session under ordinary QUIC
        // backpressure.
        Err(QuinnCurrentFlushError::LegIo(LegIoError::Transport(
            EncodedLegTransportError::ActorPressure,
        ))) => RootScan::Idle,
        Err(error) => RootScan::Failed(Arc::from(format!("current actor flush failed: {error:?}"))),
    };
    if matches!(flush, RootScan::Failed(_)) {
        return flush;
    }
    match bundle.composition.settle_completed_recovery_tail() {
        Ok(ClientRecoveryTailSettlement::Settled { .. }) => RootScan::Progressed,
        Ok(ClientRecoveryTailSettlement::AlreadySettled) => flush,
        Err(QuinnRecoveryTailSettlementError::Controller(
            ClientRecoveryTailSettlementBlock::DeliveryPending,
        )) => flush,
        Err(error) => RootScan::Failed(Arc::from(format!(
            "current recovery tail settlement failed: {error:?}"
        ))),
    }
}

fn drive_root_category(bundle: &mut RootTaskBundle, now: SimTime) -> RootScan {
    let category = bundle.next_work;
    bundle.next_work = category.next();
    match category {
        RootWorkCategory::Command => match bundle.commands.try_recv() {
            Ok(command) => process_root_command(bundle, command),
            Err(mpsc::error::TryRecvError::Empty) => RootScan::Idle,
            Err(mpsc::error::TryRecvError::Disconnected) => {
                RootScan::Failed(Arc::from("root command authority disappeared"))
            }
        },
        RootWorkCategory::Effect => dispatch_one_root_effect(bundle),
        RootWorkCategory::TerminalTimer => drive_one_terminal_timer(bundle, now),
        RootWorkCategory::LocalStallTimer => {
            match bundle.storage.client_progress.observe_due(now) {
                Ok(true) => RootScan::Progressed,
                Ok(false) => RootScan::Idle,
                Err(error) => RootScan::Failed(root_progress_error("due observation", error)),
            }
        }
        RootWorkCategory::Driver => drive_one_root_driver(bundle, now),
        RootWorkCategory::Flush => flush_current_once(bundle, now),
        RootWorkCategory::Inbound => scan_current_once(bundle, now),
    }
}

fn remove_root_driver(bundle: &mut RootTaskBundle, index: usize) {
    bundle.storage.drivers.swap_remove(index);
    if bundle.storage.drivers.is_empty() {
        bundle.storage.next_driver = 0;
    } else {
        bundle.storage.next_driver %= bundle.storage.drivers.len();
    }
}

/// Shutdown never consumes another ordinary source/completion fact. It first
/// retries an already-dequeued exact input, otherwise seals one unsealed
/// driver and immediately moves the adopted/synthesized reset into the
/// reducer. The pending-effect invariant keeps this one-flow-at-a-time even
/// when terminal-tombstone pressure defers a later reset.
fn drive_one_shutdown_driver(bundle: &mut RootTaskBundle, now: SimTime) -> RootScan {
    if !bundle.storage.pending_effects.is_empty() || bundle.storage.aborted_effects.is_some() {
        return RootScan::Idle;
    }
    if let Some(input) = bundle.storage.deferred_driver_input.take() {
        return apply_root_shutdown_driver_input(bundle, input, now);
    }
    let Some(index) = bundle
        .storage
        .drivers
        .iter()
        .position(|driver| !driver.local_abandon_is_sealed())
    else {
        if bundle.storage.drivers.is_empty() {
            return RootScan::Idle;
        }
        return RootScan::Failed(Arc::from(
            "sealed root driver has no retained reset/effect ownership",
        ));
    };
    match bundle.storage.drivers[index].seal_for_local_abandon() {
        TcpDriverLocalAbandonSeal::Sealed(input) => {
            apply_root_shutdown_driver_input(bundle, input, now)
        }
        TcpDriverLocalAbandonSeal::TerminalRetired => {
            remove_root_driver(bundle, index);
            RootScan::Progressed
        }
        TcpDriverLocalAbandonSeal::PriorResetAlreadyDequeued => RootScan::Failed(Arc::from(
            "shutdown found a dequeued reset outside the root retention slot",
        )),
        TcpDriverLocalAbandonSeal::AlreadySealed => RootScan::Failed(Arc::from(
            "shutdown driver seal changed without the sole root owner",
        )),
    }
}

fn drive_root_shutdown_category(
    bundle: &mut RootTaskBundle,
    category: RootShutdownWorkCategory,
    now: SimTime,
) -> RootScan {
    match category {
        RootShutdownWorkCategory::Effect => dispatch_one_shutdown_effect(bundle),
        RootShutdownWorkCategory::Driver => drive_one_shutdown_driver(bundle, now),
        RootShutdownWorkCategory::Flush => flush_current_once(bundle, now),
        // Whole-session shutdown has sealed every future flow/replacement
        // admission. Once FlowFinished has revoked the local driver, no
        // successor can use its tombstone, so retaining it until the normal
        // recovery grace expires only blocks structural join.
        RootShutdownWorkCategory::TerminalTimer => {
            // Each timer carries its own pointer-identical RESET receipt.
            // Delivered/no-RESET timers may be forced immediately; a pending
            // RESET retains its original absolute deadline. Shutdown commits
            // the pointer-identical delivery receipt directly and never feeds
            // post-seal inbound back through the live reducer transaction.
            force_one_terminal_timer_for_shutdown(bundle, now)
        }
    }
}

fn root_flow_shutdown_is_quiescent(bundle: &RootTaskBundle) -> bool {
    bundle.storage.drivers.is_empty()
        && bundle.storage.deferred_driver_input.is_none()
        && bundle.storage.aborted_driver_inputs.is_empty()
        && bundle.storage.pending_effects.is_empty()
        && bundle.storage.aborted_effects.is_none()
        && bundle.storage.terminal_timers.is_empty()
        && bundle.factory.session_uplink_owned_bytes() == 0
        && bundle.factory.session_uplink_owned_segments() == 0
        && bundle.composition.root_flow_shutdown_is_quiescent()
}

/// Drives the semantic half of shutdown while the current actor remains live.
/// Terminal tokens are consumed after their local driver has been revoked;
/// whole-session shutdown has no future replacement that could use the normal
/// recovery grace. Actor/factory progress cursors remain the non-polling wake
/// sources for FIFO delivery and endpoint ownership release.
async fn drain_root_flows_for_shutdown(bundle: &mut RootTaskBundle) -> Result<(), Arc<str>> {
    let _closed_now = bundle.composition.close_root_flow_admission();
    if bundle.storage.aborted_effects.is_some() || !bundle.storage.aborted_driver_inputs.is_empty()
    {
        return Err(Arc::from(
            "root entered flow shutdown with previously aborted exact work",
        ));
    }

    let mut next = RootShutdownWorkCategory::Effect;
    let mut idle_categories = 0usize;
    // One cursor pair covers one complete run of consecutive idle
    // categories, even when a small work budget splits that run across
    // cooperative slices. Re-capturing at a slice boundary could absorb the
    // only edge produced by a category already scanned in this cycle.
    let mut actor_cursor = bundle.composition.progress_cursor();
    let mut factory_cursor = bundle.factory.progress_cursor();
    loop {
        if root_flow_shutdown_is_quiescent(bundle) {
            return Ok(());
        }
        for _ in 0..bundle.plan.work_budget() {
            let now = bundle
                .clock
                .now()
                .map_err(|error| Arc::from(format!("root shutdown clock failed: {error}")))?;
            let category = next;
            next = category.next();
            match drive_root_shutdown_category(bundle, category, now) {
                RootScan::Idle => {
                    idle_categories = idle_categories.saturating_add(1);
                    if idle_categories >= RootShutdownWorkCategory::COUNT {
                        break;
                    }
                }
                RootScan::Progressed => {
                    idle_categories = 0;
                    actor_cursor = bundle.composition.progress_cursor();
                    factory_cursor = bundle.factory.progress_cursor();
                }
                RootScan::Failed(error) => return Err(error),
            }
            if root_flow_shutdown_is_quiescent(bundle) {
                return Ok(());
            }
        }
        if idle_categories < RootShutdownWorkCategory::COUNT {
            #[cfg(test)]
            if idle_categories > 0
                && next == RootShutdownWorkCategory::TerminalTimer
                && !bundle.composition.root_shutdown_transport_is_pristine()
            {
                bundle.shutdown_slice.pause_if_armed(next);
            }
            tokio::task::yield_now().await;
            continue;
        }

        if bundle.storage.drivers.is_empty()
            && bundle.storage.deferred_driver_input.is_none()
            && bundle.storage.pending_effects.is_empty()
            && bundle.storage.terminal_timers.is_empty()
        {
            if let Some(residual) = session_residual(bundle.composition.snapshot()) {
                return Err(Arc::from(format!(
                    "root logical shutdown lost its progress authority: {residual}"
                )));
            }
            if bundle.factory.session_uplink_owned_bytes() != 0
                || bundle.factory.session_uplink_owned_segments() != 0
            {
                return Err(Arc::from(
                    "root logical shutdown orphaned factory byte/segment ownership",
                ));
            }
            if !bundle.composition.root_shutdown_transport_is_pristine() {
                return Err(Arc::from(
                    "root shutdown transport retained FIFO ownership without terminal authority",
                ));
            }
        }

        #[cfg(test)]
        {
            // Capture a second, test-only cursor at the exact post-scan seam.
            // When armed, the test publishes one final actor edge after this
            // capture. Waiting for that edge before the real park makes the
            // regression deterministic: the production cursor above must
            // still be the pre-scan cursor or this final edge is lost.
            let post_scan_cursor = bundle.composition.progress_cursor();
            if bundle.shutdown_pre_park.pause_if_armed() {
                bundle
                    .composition
                    .wait_for_actor_progress(post_scan_cursor)
                    .await
                    .map_err(progress_failure)?;
            }
        }
        // Shutdown force-arbitrates the exact RESET receipt and clears any
        // stale live-publication gate before every timer scan. Never suppress
        // its absolute deadline behind a live-path hint.
        let wakeable_terminal_deadline = bundle.storage.terminal_timers.next_instant();
        let timer_delay = match (wakeable_terminal_deadline, bundle.clock.now()) {
            (Some(deadline), Ok(now)) if deadline <= now => Some(Duration::ZERO),
            (Some(deadline), Ok(now)) => deadline.checked_duration_since(now).ok(),
            (Some(_), Err(error)) => {
                return Err(Arc::from(format!(
                    "root shutdown clock failed before wait: {error}"
                )));
            }
            (None, _) => None,
        };
        enum ShutdownWake {
            Actor(Result<(), QuinnCompositionProgressError>),
            Factory(
                Result<
                    super::tcp::TcpFactoryProgressCursor,
                    super::tcp::TcpFactoryProgressWaitError,
                >,
            ),
            Timer,
        }
        let wake = tokio::select! {
            biased;
            progress = bundle.composition.wait_for_actor_progress(actor_cursor) => {
                ShutdownWake::Actor(progress.map(|_| ()))
            }
            progress = bundle.factory.wait_for_progress(&factory_cursor) => {
                ShutdownWake::Factory(progress)
            }
            () = async {
                match timer_delay {
                    Some(delay) => tokio::time::sleep(delay).await,
                    None => std::future::pending::<()>().await,
                }
            } => ShutdownWake::Timer,
        };
        idle_categories = 0;
        match wake {
            ShutdownWake::Actor(Ok(())) | ShutdownWake::Factory(Ok(_)) | ShutdownWake::Timer => {}
            ShutdownWake::Actor(Err(error)) => return Err(progress_failure(error)),
            ShutdownWake::Factory(Err(error)) => {
                return Err(Arc::from(format!(
                    "TCP factory progress failed during shutdown: {error}"
                )));
            }
        }
        actor_cursor = bundle.composition.progress_cursor();
        factory_cursor = bundle.factory.progress_cursor();
    }
}

/// A semantic failure may make the reducer path unavailable, but it must not
/// leave already-issued local capabilities live. Seal every remaining driver
/// and retain each exact reset input in preallocated aborted ownership so the
/// final witness can never be misreported as clean.
fn seal_root_drivers_after_failure(bundle: &mut RootTaskBundle) -> Result<(), Arc<str>> {
    for driver in &mut bundle.storage.drivers {
        if driver.local_abandon_is_sealed() {
            continue;
        }
        match driver.seal_for_local_abandon() {
            TcpDriverLocalAbandonSeal::Sealed(input) => {
                if bundle.storage.aborted_driver_inputs.len()
                    >= bundle.plan.storage().geometry().max_flows()
                    || bundle.storage.aborted_driver_inputs.len()
                        >= bundle.storage.aborted_driver_inputs.capacity()
                {
                    return Err(Arc::from(
                        "aborted driver input registry lost its checked capacity",
                    ));
                }
                bundle.storage.aborted_driver_inputs.push(input);
            }
            TcpDriverLocalAbandonSeal::PriorResetAlreadyDequeued
            | TcpDriverLocalAbandonSeal::AlreadySealed
            | TcpDriverLocalAbandonSeal::TerminalRetired => {}
        }
    }
    Ok(())
}

fn take_one_failed_shutdown_effect(bundle: &mut RootTaskBundle) -> Option<SessionEffect> {
    if let Some(effect) = bundle.storage.pending_effects.pop_front() {
        return Some(effect);
    }
    let mut effects = bundle.storage.aborted_effects.take()?;
    if effects.is_empty() {
        return None;
    }
    let effect = effects.remove(0);
    if !effects.is_empty() {
        bundle.storage.aborted_effects = Some(effects);
    }
    Some(effect)
}

/// Once the transport path has failed, its exact unsent frames have an
/// explicit terminal disposition: retain the original root failure, destroy
/// those frames before physical join, and continue local reducer cleanup.
/// Local sink/half-close effects are likewise superseded by the already-sealed
/// driver's reset. `FlowFinished` remains mandatory because it is the local
/// capability revocation and driver-release point.
fn dispatch_one_failed_shutdown_effect(bundle: &mut RootTaskBundle) -> RootScan {
    let Some(effect) = take_one_failed_shutdown_effect(bundle) else {
        return RootScan::Idle;
    };
    match effect {
        effect @ SessionEffect::FlowFinished { flow_id, .. } => {
            let Some(index) = find_driver(bundle, flow_id) else {
                bundle.storage.pending_effects.push_front(effect);
                return RootScan::Failed(Arc::from(
                    "failed shutdown terminal effect has no matching driver",
                ));
            };
            match bundle.storage.drivers[index].try_deliver_terminal_effect(&effect) {
                Ok(_) => {
                    remove_root_driver(bundle, index);
                    RootScan::Progressed
                }
                Err(error) => {
                    bundle.storage.pending_effects.push_front(effect);
                    RootScan::Failed(Arc::from(format!(
                        "failed shutdown could not retire a terminal driver: {error}"
                    )))
                }
            }
        }
        SessionEffect::Transmit(_)
        | SessionEffect::OfferToSink { .. }
        | SessionEffect::HalfCloseSink { .. }
        | SessionEffect::LegActivated { .. }
        | SessionEffect::ResumeGraceStarted { .. }
        | SessionEffect::SessionExpired
        | SessionEffect::LocalOpenResolved { .. }
        | SessionEffect::PeerReset { .. }
        | SessionEffect::ReplayStored { .. }
        | SessionEffect::ReplayAcknowledged { .. }
        | SessionEffect::LocalFlowOpened { .. }
        | SessionEffect::PeerOpenRequested { .. } => RootScan::Progressed,
    }
}

fn failed_shutdown_semantics_are_quiescent(bundle: &RootTaskBundle) -> bool {
    bundle.storage.drivers.is_empty()
        && bundle.storage.deferred_driver_input.is_none()
        && bundle.storage.aborted_driver_inputs.is_empty()
        && bundle.storage.pending_effects.is_empty()
        && bundle
            .storage
            .aborted_effects
            .as_ref()
            .is_none_or(Vec::is_empty)
        && bundle.storage.terminal_timers.is_empty()
        && bundle.factory.session_uplink_owned_bytes() == 0
        && bundle.factory.session_uplink_owned_segments() == 0
        && session_residual(bundle.composition.snapshot()).is_none()
}

/// Actor failure cannot be allowed to strand local flow capabilities. This
/// bounded, non-awaiting fallback uses only the sealed factory, typed driver
/// provenance, reducer, terminal lane, and shutdown-only terminal expiry. It
/// never attempts another transport mutation.
fn drain_root_semantics_after_transport_failure(
    bundle: &mut RootTaskBundle,
) -> Result<(), Arc<str>> {
    let max_flows = bundle.plan.storage().geometry().max_flows();
    let max_turns = max_flows
        .saturating_mul(8)
        .saturating_add(bundle.plan.pending_effect_capacity())
        .saturating_add(bundle.plan.timer_capacity())
        .saturating_add(16);
    for _ in 0..max_turns {
        if failed_shutdown_semantics_are_quiescent(bundle) {
            return Ok(());
        }
        let now = bundle
            .clock
            .now()
            .map_err(|error| Arc::from(format!("failed shutdown clock failed: {error}")))?;
        let scan = if !bundle.storage.pending_effects.is_empty()
            || bundle.storage.aborted_effects.is_some()
        {
            dispatch_one_failed_shutdown_effect(bundle)
        } else if !bundle.storage.terminal_timers.is_empty() {
            force_one_terminal_timer_after_transport_failure(bundle, now)
        } else {
            drive_one_shutdown_driver(bundle, now)
        };
        match scan {
            RootScan::Progressed => {}
            RootScan::Idle => {
                return Err(Arc::from(
                    "failed shutdown semantic drain has no remaining progress authority",
                ));
            }
            RootScan::Failed(error) => return Err(error),
        }
    }
    Err(Arc::from(
        "failed shutdown semantic drain exceeded its checked work envelope",
    ))
}

#[derive(Debug, PartialEq, Eq)]
enum RootReadyPublication {
    Published,
    ShutdownRequested,
    LifecycleConflict(QuinnClientRootPhase),
}

/// Classifies the only legal Ready publication race. The hook is a no-op in
/// production and lets a deterministic test place shutdown exactly after the
/// first Open observation but before the lifecycle transition.
fn arbitrate_root_ready_publication(
    lifecycle: &RootLifecycle,
    shutdown: &watch::Receiver<RootShutdownSignal>,
    after_open_observation: impl FnOnce(),
) -> RootReadyPublication {
    if *shutdown.borrow() != RootShutdownSignal::Open {
        return RootReadyPublication::ShutdownRequested;
    }
    after_open_observation();
    match lifecycle.transition(
        QuinnClientRootPhase::Starting,
        QuinnClientRootPhase::Running,
    ) {
        Ok(()) => RootReadyPublication::Published,
        Err(QuinnClientRootPhase::ShuttingDown) => RootReadyPublication::ShutdownRequested,
        Err(phase) => RootReadyPublication::LifecycleConflict(phase),
    }
}

fn scan_current_once(bundle: &mut RootTaskBundle, now: SimTime) -> RootScan {
    if bundle.storage.aborted_effects.is_some() {
        return RootScan::Failed(Arc::from(
            "root attempted another reducer scan after retained admission failure",
        ));
    }
    if !bundle.storage.pending_effects.is_empty() {
        // The checked pending bound is max(recovery, incremental), not their
        // sum. Dispatch must drain this exact batch before another reducer
        // mutation; the fair scheduler owns that category.
        return RootScan::Idle;
    }
    let mut progressed = false;
    match bundle.composition.try_receive_current() {
        QuinnCurrentInboundDrive::Idle => {}
        QuinnCurrentInboundDrive::Applied {
            effects,
            observation,
        } => match finish_observed_inbound_turn(bundle, effects, observation, now) {
            RootScan::Progressed => progressed = true,
            RootScan::Idle => {}
            RootScan::Failed(error) => return RootScan::Failed(error),
        },
        QuinnCurrentInboundDrive::ProbeAcked {
            generation,
            probe_sequence,
        } => match reconcile_root_probe_ack(bundle, generation, probe_sequence) {
            Ok(()) => progressed = true,
            Err(error) => return RootScan::Failed(error),
        },
        QuinnCurrentInboundDrive::Rejected => progressed = true,
        QuinnCurrentInboundDrive::PolicyBlocked => {
            return RootScan::Failed(Arc::from("current inbound policy blocked"));
        }
        QuinnCurrentInboundDrive::MissingCurrent => {
            return RootScan::Failed(Arc::from("current actor missing"));
        }
        QuinnCurrentInboundDrive::ReducerRejected(error) => {
            return RootScan::Failed(Arc::from(format!("reducer rejected inbound: {error:?}")));
        }
        QuinnCurrentInboundDrive::SessionShuttingDown => {
            return RootScan::Failed(Arc::from("composition shut down before root"));
        }
        QuinnCurrentInboundDrive::CompositionInvariant => {
            return RootScan::Failed(Arc::from("current inbound composition invariant"));
        }
        QuinnCurrentInboundDrive::ActorFailed(error) => {
            return RootScan::Failed(Arc::from(format!("current actor failed: {error:?}")));
        }
    }

    match bundle.composition.try_drive_current_terminal() {
        Ok(QuinnCurrentTerminalDrive::Pending) => {
            if progressed {
                RootScan::Progressed
            } else {
                RootScan::Idle
            }
        }
        Ok(
            QuinnCurrentTerminalDrive::Applied(_)
            | QuinnCurrentTerminalDrive::AlreadyApplied { .. },
        ) => match settle_root_current_terminal_observation(bundle) {
            Ok(()) => RootScan::Failed(Arc::from(
                "current actor terminal applied without replacement",
            )),
            Err(error) => RootScan::Failed(error),
        },
        Err(error) => RootScan::Failed(Arc::from(format!("current terminal failed: {error:?}"))),
    }
}

/// Terminal binding moves the controller's probe/application-ACK observation
/// into a pending settlement before the live root turns the terminal into its
/// sticky shutdown trigger. If that observation names a scheduler authority,
/// return the exact root slot synchronously now; otherwise shutdown would see
/// an Applied terminal whose controller ownership can never reach zero.
fn settle_root_current_terminal_observation(bundle: &mut RootTaskBundle) -> Result<(), Arc<str>> {
    let Some((generation, probe_sequence)) =
        bundle.composition.pending_current_observation_probe_key()
    else {
        return Ok(());
    };
    let key = RootCurrentARecoveryKey {
        generation,
        sequence: probe_sequence,
    };
    let composition = &mut bundle.composition;
    match bundle
        .storage
        .recovery_timers
        .transact_current_a_scheduler_by_exact_key(key, |scheduler| {
            let scheduler = match scheduler {
                RootRecoverySchedulerAuthority::Queued(queued) => {
                    QuinnCurrentProbeSchedulerAuthority::Queued(queued)
                }
                RootRecoverySchedulerAuthority::Armed(deadline) => {
                    QuinnCurrentProbeSchedulerAuthority::Armed(deadline)
                }
            };
            composition
                .settle_pending_current_observation(scheduler)
                .map_err(|failure| {
                    let kind = failure.kind();
                    let scheduler = match failure.into_scheduler() {
                        QuinnCurrentProbeSchedulerAuthority::Queued(queued) => {
                            RootRecoverySchedulerAuthority::Queued(queued)
                        }
                        QuinnCurrentProbeSchedulerAuthority::Armed(deadline) => {
                            RootRecoverySchedulerAuthority::Armed(deadline)
                        }
                    };
                    (
                        scheduler,
                        Arc::from(format!(
                            "current terminal observation settlement failed: {kind:?}"
                        )),
                    )
                })
        }) {
        Ok(settlement) => {
            if settlement.probe_generation() != generation
                || settlement.probe_sequence() != Some(probe_sequence)
            {
                return Err(Arc::from(
                    "current terminal observation returned a mismatched settlement",
                ));
            }
            Ok(())
        }
        Err(RootRecoverySchedulerTransactionError::Take(kind)) => Err(Arc::from(format!(
            "current terminal observation has no exact root scheduler authority: {kind:?}"
        ))),
        Err(RootRecoverySchedulerTransactionError::Action(error)) => Err(error),
    }
}

/// Completes ACK ownership only when root can atomically return the exact
/// queued/armed scheduler capability correlated by the wire ACK. Any missing,
/// wrong-correlation, controller rejection, or history-overflow path restores
/// the full root slot in place before reporting failure.
fn reconcile_root_probe_ack(
    bundle: &mut RootTaskBundle,
    generation: crate::resumable::LegGeneration,
    probe_sequence: crate::resumable::ProbeSequence,
) -> Result<(), Arc<str>> {
    let key = RootCurrentARecoveryKey {
        generation,
        sequence: probe_sequence,
    };
    let composition = &mut bundle.composition;
    match bundle
        .storage
        .recovery_timers
        .transact_current_a_scheduler_by_exact_key(key, |scheduler| {
            let scheduler = match scheduler {
                RootRecoverySchedulerAuthority::Queued(queued) => {
                    QuinnCurrentProbeSchedulerAuthority::Queued(queued)
                }
                RootRecoverySchedulerAuthority::Armed(deadline) => {
                    QuinnCurrentProbeSchedulerAuthority::Armed(deadline)
                }
            };
            composition
                .reconcile_current_probe_ack(scheduler, generation, probe_sequence)
                .map_err(|failure| {
                    let kind = failure.kind();
                    let scheduler = match failure.into_scheduler() {
                        QuinnCurrentProbeSchedulerAuthority::Queued(queued) => {
                            RootRecoverySchedulerAuthority::Queued(queued)
                        }
                        QuinnCurrentProbeSchedulerAuthority::Armed(deadline) => {
                            RootRecoverySchedulerAuthority::Armed(deadline)
                        }
                    };
                    (
                        scheduler,
                        Arc::from(format!(
                            "current probe ACK scheduler reconciliation failed: {kind:?}"
                        )),
                    )
                })
        }) {
        Ok(settlement) => {
            if settlement.generation() != generation
                || settlement.probe_sequence() != probe_sequence
            {
                return Err(Arc::from(
                    "current probe ACK reconciliation returned a mismatched settlement",
                ));
            }
            Ok(())
        }
        Err(RootRecoverySchedulerTransactionError::Take(kind)) => Err(Arc::from(format!(
            "current probe ACK has no exact root scheduler authority: {kind:?}"
        ))),
        Err(RootRecoverySchedulerTransactionError::Action(error)) => Err(error),
    }
}

async fn run_root(mut bundle: RootTaskBundle) -> QuinnClientRootOutcome {
    let (mut first_failure, live_admitted) =
        match bundle.composition.transact_root_ready_publication(|| {
            arbitrate_root_ready_publication(&bundle.lifecycle, &bundle.shutdown, || {})
        }) {
            Ok(Some(RootReadyPublication::Published)) => (None, true),
            Ok(Some(RootReadyPublication::ShutdownRequested)) => (None, false),
            Ok(Some(RootReadyPublication::LifecycleConflict(phase))) => (
                Some(Arc::from(format!(
                    "root Ready publication observed invalid lifecycle phase {phase:?}"
                ))),
                false,
            ),
            Ok(None) => (
                Some(Arc::from("composition rejected root Ready publication")),
                false,
            ),
            Err(error) => (
                Some(Arc::from(format!(
                    "actor failure won root Ready publication: {error:?}"
                ))),
                false,
            ),
        };

    let mut idle_categories = 0usize;
    // Preserve the subscribe/check/park cursor across cooperative slices.
    // `work_budget` may be smaller than the category ring, so only concrete
    // progress (or a completed wait) begins a fresh idle-scan epoch.
    let mut actor_cursor = bundle.composition.progress_cursor();
    let mut factory_cursor = bundle.factory.progress_cursor();
    'live: while live_admitted
        && first_failure.is_none()
        && bundle.lifecycle.phase() == QuinnClientRootPhase::Running
        && *bundle.shutdown.borrow() == RootShutdownSignal::Open
    {
        for _ in 0..bundle.plan.work_budget {
            // The lifecycle CAS is the semantic cutoff. The watch publication
            // is only the async wake and can lag behind it, so neither the
            // pre-Ready race nor a mid-batch shutdown may service one more
            // command, timer, driver, flush, or inbound actor record.
            if bundle.lifecycle.phase() != QuinnClientRootPhase::Running
                || *bundle.shutdown.borrow() != RootShutdownSignal::Open
            {
                break;
            }
            #[cfg(test)]
            bundle
                .live_category
                .pause_if_armed(RootLiveBoundary::Category(bundle.next_work));
            let lifecycle = Arc::clone(&bundle.lifecycle);
            let scan = match lifecycle.transact_if_running(|| {
                let now = bundle.clock.now()?;
                #[cfg(test)]
                bundle.run_probe.record_live_scan();
                Ok::<RootScan, SessionClockError>(drive_root_category(&mut bundle, now))
            }) {
                Ok(Ok(scan)) => scan,
                Ok(Err(error)) => {
                    first_failure =
                        Some(Arc::from(format!("root monotonic clock failed: {error}")));
                    break 'live;
                }
                Err(QuinnClientRootPhase::ShuttingDown) => break 'live,
                Err(phase) => {
                    first_failure = Some(Arc::from(format!(
                        "root live turn observed invalid lifecycle phase {phase:?}"
                    )));
                    break 'live;
                }
            };
            match scan {
                RootScan::Idle => {
                    idle_categories = idle_categories.saturating_add(1);
                    if idle_categories >= RootWorkCategory::COUNT {
                        break;
                    }
                }
                RootScan::Progressed => {
                    idle_categories = 0;
                    actor_cursor = bundle.composition.progress_cursor();
                    factory_cursor = bundle.factory.progress_cursor();
                }
                RootScan::Failed(error) => {
                    first_failure = Some(error);
                    break;
                }
            }
        }
        if first_failure.is_some() {
            break;
        }
        if idle_categories < RootWorkCategory::COUNT {
            // The category budget bounds one synchronous turn. Cooperative
            // yield prevents either a small configured budget or continuous
            // category progress from monopolizing this Tokio worker.
            tokio::task::yield_now().await;
            continue;
        }
        let wakeable_terminal_deadline = live_terminal_wait_deadline(
            bundle.storage.terminal_timers.next_instant(),
            bundle.storage.terminal_publication_pending,
        );
        let wakeable_local_stall_deadline = bundle.storage.client_progress.next_wake_deadline();
        let wakeable_deadline = match (wakeable_terminal_deadline, wakeable_local_stall_deadline) {
            (Some(terminal), Some(local)) => Some(terminal.min(local)),
            (Some(deadline), None) | (None, Some(deadline)) => Some(deadline),
            (None, None) => None,
        };
        let timer_delay = match (wakeable_deadline, bundle.clock.now()) {
            (Some(deadline), Ok(now)) if deadline <= now => Some(Duration::ZERO),
            (Some(deadline), Ok(now)) => deadline.checked_duration_since(now).ok(),
            (Some(_), Err(error)) => {
                first_failure = Some(Arc::from(format!(
                    "root monotonic clock failed before wait: {error}"
                )));
                continue;
            }
            (None, _) => None,
        };
        enum RootWake {
            Shutdown(Result<(), watch::error::RecvError>),
            Actor(Result<(), QuinnCompositionProgressError>),
            Factory(
                Result<
                    super::tcp::TcpFactoryProgressCursor,
                    super::tcp::TcpFactoryProgressWaitError,
                >,
            ),
            Timer,
            Command(Option<QuinnClientRootCommand>),
        }
        #[cfg(test)]
        bundle.run_probe.record_live_wait();
        let wake = tokio::select! {
            biased;
            changed = bundle.shutdown.changed() => {
                RootWake::Shutdown(changed)
            }
            progress = bundle.composition.wait_for_actor_progress(actor_cursor) => {
                RootWake::Actor(progress.map(|_| ()))
            }
            progress = bundle.factory.wait_for_progress(&factory_cursor) => {
                RootWake::Factory(progress)
            }
            () = async {
                match timer_delay {
                    Some(delay) => tokio::time::sleep(delay).await,
                    None => std::future::pending::<()>().await,
                }
            } => {
                RootWake::Timer
            }
            command = bundle.commands.recv() => RootWake::Command(command)
        };
        idle_categories = 0;
        match wake {
            RootWake::Shutdown(changed) => {
                if changed.is_err() {
                    first_failure = Some(Arc::from("root shutdown authority disappeared"));
                }
            }
            RootWake::Actor(Err(error)) => first_failure = Some(progress_failure(error)),
            RootWake::Factory(Err(error)) => {
                first_failure = Some(Arc::from(format!("TCP factory progress failed: {error}")));
            }
            RootWake::Command(Some(command)) => {
                #[cfg(test)]
                bundle
                    .live_category
                    .pause_if_armed(RootLiveBoundary::SelectedCommand);
                let lifecycle = Arc::clone(&bundle.lifecycle);
                match lifecycle.transact_if_running_with_input(command, |command| {
                    process_root_command(&mut bundle, command)
                }) {
                    Ok(RootScan::Failed(error)) => first_failure = Some(error),
                    Ok(RootScan::Idle | RootScan::Progressed) => {}
                    Err((command, QuinnClientRootPhase::ShuttingDown)) => {
                        reject_root_command_after_lifecycle_cutoff(
                            command,
                            QuinnClientRootPhase::ShuttingDown,
                        );
                        break 'live;
                    }
                    Err((command, phase)) => {
                        reject_root_command_after_lifecycle_cutoff(command, phase);
                        first_failure = Some(Arc::from(format!(
                            "selected root command observed invalid lifecycle phase {phase:?}"
                        )));
                        break 'live;
                    }
                }
            }
            RootWake::Command(None) => {
                first_failure = Some(Arc::from("root command authority disappeared"));
            }
            RootWake::Actor(Ok(())) | RootWake::Factory(Ok(_)) | RootWake::Timer => {}
        }
        actor_cursor = bundle.composition.progress_cursor();
        factory_cursor = bundle.factory.progress_cursor();
    }

    #[cfg(test)]
    bundle.run_probe.mark_shutdown_cleanup_started();

    if let Err(phase) = bundle.lifecycle.begin_shutdown() {
        first_failure.get_or_insert_with(|| {
            Arc::from(format!(
                "root shutdown observed invalid lifecycle phase {phase:?}"
            ))
        });
    }
    bundle.commands.close();
    while let Ok(command) = bundle.commands.try_recv() {
        match command {
            QuinnClientRootCommand::OpenTcp { target, reply } => {
                drop(target);
                let _sent = reply.send(Err(QuinnClientRootOpenFailureKind::RootNotRunning(
                    QuinnClientRootPhase::ShuttingDown,
                )));
            }
        }
    }

    // Lifecycle cutoff precedes all structural awaits. The local stall source
    // and its scheduler witness grant no transport authority, so shutdown
    // retires them synchronously and carries both the zero-live inventory and
    // historical discard audit into the joined witness.
    let client_progress_shutdown = bundle.storage.client_progress.close_for_shutdown();
    let client_progress_inventory = bundle.storage.client_progress.live_inventory();
    let client_observations = bundle.storage.client_progress.history();

    // Move every exact scheduler capability across the composition boundary
    // synchronously, before shutdown creates its first await. The composition
    // has one checked slot because the controller has one live current probe;
    // any second, stale, or mismatched root slot stays here as an exact
    // fail-closed residual instead of being dropped.
    let mut recovery_timer_shutdown = bundle.storage.recovery_timers.take_for_shutdown();
    let recovery_handoff = recovery_timer_shutdown.try_handoff_current_a(|scheduler| {
        let scheduler = match scheduler {
            RootRecoverySchedulerAuthority::Queued(queued) => {
                QuinnCurrentProbeSchedulerAuthority::Queued(queued)
            }
            RootRecoverySchedulerAuthority::Armed(deadline) => {
                QuinnCurrentProbeSchedulerAuthority::Armed(deadline)
            }
        };
        bundle
            .composition
            .retain_current_probe_scheduler_for_shutdown(scheduler)
            .map_err(|scheduler| match scheduler {
                QuinnCurrentProbeSchedulerAuthority::Queued(queued) => {
                    RootRecoverySchedulerAuthority::Queued(queued)
                }
                QuinnCurrentProbeSchedulerAuthority::Armed(deadline) => {
                    RootRecoverySchedulerAuthority::Armed(deadline)
                }
            })
    });
    if recovery_handoff.rejected != 0 {
        first_failure.get_or_insert_with(|| {
            Arc::from(format!(
                "root shutdown retained {} unreconciled recovery scheduler authorities",
                recovery_handoff.rejected
            ))
        });
    }
    // B's root entry is only a typed Tokio wake witness.  The armed deadline
    // and expiry commit authority never leave the composition, so shutdown
    // may synchronously retire this scheduling witness without fabricating a
    // second B owner.  The composition's structural join still owns the exact
    // B-state settlement and must fail closed if that state is not retireable.
    let standby_wake_retirement =
        recovery_timer_shutdown.try_handoff_standby_b_wake(|_wake| Ok(()));
    debug_assert_eq!(standby_wake_retirement.rejected, 0);

    if let Err(error) = drain_root_flows_for_shutdown(&mut bundle).await {
        first_failure.get_or_insert(error);
        if let Err(error) = drain_root_semantics_after_transport_failure(&mut bundle) {
            first_failure.get_or_insert(error);
            if let Err(error) = seal_root_drivers_after_failure(&mut bundle) {
                first_failure.get_or_insert(error);
            }
        }
    }

    let mailbox_messages = bundle.commands.len();
    let flow_drivers = bundle.storage.drivers.len();
    let pending_effects = bundle
        .storage
        .pending_effects
        .len()
        .saturating_add(bundle.storage.aborted_effects.as_ref().map_or(0, Vec::len))
        .saturating_add(usize::from(bundle.storage.deferred_driver_input.is_some()))
        .saturating_add(bundle.storage.aborted_driver_inputs.len());
    let recovery_timer_inventory = recovery_timer_shutdown.live_inventory();
    let timers = recovery_timer_shutdown
        .len()
        .saturating_add(bundle.storage.terminal_timers.len());
    bundle.storage.pending_effects.clear();
    bundle.storage.aborted_effects = None;
    bundle.storage.deferred_driver_input = None;
    bundle.storage.aborted_driver_inputs.clear();
    bundle.storage.terminal_timers.clear();
    bundle.storage.terminal_publication_pending = false;
    bundle.storage.drivers.clear();

    let composition_outcome = bundle.composition.join_all_for_shutdown().await;
    let controller_ownership = composition_outcome.controller_ownership_ledger();
    let (joined, composition_join_error) = composition_outcome.into_parts();
    let snapshot = bundle.composition.snapshot();
    let (witness, actor_terminal_failure) = match joined {
        Some(actor) => {
            let terminal_failure = actor.terminal_failure();
            (
                Some(joined_witness(
                    actor,
                    &bundle.factory,
                    snapshot,
                    flow_drivers,
                    pending_effects,
                    timers,
                    mailbox_messages,
                    controller_ownership,
                    recovery_timer_inventory,
                    client_progress_inventory,
                    client_progress_shutdown,
                    client_observations,
                )),
                terminal_failure,
            )
        }
        None => (None, false),
    };
    let residual_error = witness
        .filter(|witness| !witness.is_zero())
        .map(|witness| Arc::from(format!("root joined with residual ownership: {witness:?}")));
    let session_error = session_residual(snapshot);
    first_failure = first_failure
        .or(composition_join_error.map(join_error))
        .or(residual_error)
        .or(session_error);
    if actor_terminal_failure {
        first_failure
            .get_or_insert_with(|| Arc::from("current actor joined after terminal failure"));
    }
    // This read is intentionally after every structural await. If a cancelled
    // join observer drops Started while cleanup is in flight, OwnerDropped is
    // the latest authority and can never be reclassified as a clean join.
    let shutdown_signal = *bundle.shutdown.borrow();
    if shutdown_signal == RootShutdownSignal::OwnerDropped {
        first_failure
            .get_or_insert_with(|| Arc::from("root owner dropped before join observation"));
    }

    match (first_failure, witness) {
        (None, Some(joined)) => {
            // Structural completion alone is insufficient to publish Joined:
            // the non-Clone Started owner must successfully observe this
            // JoinHandle result. Drop/janitor paths can therefore never mint a
            // clean lifecycle state.
            bundle.lifecycle_guard.disarm();
            QuinnClientRootOutcome::Joined(joined)
        }
        (failure, joined) => {
            let failure = failure.unwrap_or_else(|| Arc::from("root structural witness missing"));
            let _terminal = bundle.lifecycle.transition(
                QuinnClientRootPhase::ShuttingDown,
                QuinnClientRootPhase::FailedJoined,
            );
            bundle.lifecycle_guard.disarm();
            QuinnClientRootOutcome::FailedJoined {
                joined,
                first_failure: failure,
            }
        }
    }
}

fn joined_witness(
    actor: CurrentQuinnActorJoined,
    factory: &ResumableTcpPortFactory,
    session: SessionSupervisorSnapshot,
    flow_drivers: usize,
    pending_effects: usize,
    timers: usize,
    mailbox_messages: usize,
    controller_ownership: ClientControllerOwnershipLedger,
    root_recovery_timers: RootRecoveryTimerLiveInventory,
    client_progress: RootClientProgressLiveInventory,
    client_progress_shutdown: RootClientProgressShutdownAudit,
    client_observations: RootClientObservationHistory,
) -> ClientRootJoined {
    ClientRootJoined {
        actor_owned_messages: actor.owned_messages(),
        actor_owned_bytes: actor.owned_bytes(),
        actor_owned_terminals: actor.owned_terminals(),
        factory_owned_bytes: factory.session_uplink_owned_bytes(),
        factory_owned_segments: factory.session_uplink_owned_segments(),
        session_residual_ownership: session_residual_ownership(session),
        flow_drivers,
        pending_effects,
        timers,
        mailbox_messages,
        controller_ownership,
        root_recovery_timers,
        client_progress,
        client_progress_shutdown,
        client_observations,
    }
}

fn progress_failure(error: QuinnCompositionProgressError) -> Arc<str> {
    Arc::from(format!("composition progress failed: {error:?}"))
}

fn join_error(error: QuinnCurrentJoinError) -> Arc<str> {
    Arc::from(format!("composition structural join failed: {error:?}"))
}

fn session_residual(snapshot: SessionSupervisorSnapshot) -> Option<Arc<str>> {
    if session_residual_ownership(snapshot) == 0 {
        None
    } else {
        Some(Arc::from(format!(
            "session ownership remained at root join: {snapshot:?}"
        )))
    }
}

fn session_residual_ownership(snapshot: SessionSupervisorSnapshot) -> usize {
    let session = snapshot.session;
    let client_replay = session.replay_usage(crate::resumable::Direction::ClientToTarget);
    let target_replay = session.replay_usage(crate::resumable::Direction::TargetToClient);
    let replay_bytes = client_replay.bytes().saturating_add(target_replay.bytes());
    let replay_segments = client_replay
        .segments()
        .saturating_add(target_replay.segments());
    session
        .flow_count()
        .saturating_add(replay_bytes)
        .saturating_add(replay_segments)
        .saturating_add(session.receive_owned_bytes())
        .saturating_add(session.receive_ranges())
        .saturating_add(session.outstanding_sink_offers())
        .saturating_add(session.terminal_tombstones())
        .saturating_add(snapshot.uplink_replay_extents)
        .saturating_add(snapshot.uplink_replay_owned_bytes)
        .saturating_add(snapshot.quarantined_uplink_extents)
        .saturating_add(snapshot.quarantined_uplink_owned_bytes)
        .saturating_add(usize::from(snapshot.owner_catch_up_permit))
        .saturating_add(usize::from(snapshot.owner_catch_up_permit_armed))
        .saturating_add(usize::from(snapshot.poisoned))
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
    use std::time::Duration;

    use bytes::Bytes;

    use super::*;
    use crate::resumable::{
        AttachAlpn, AttachAuthority, AttachCredentials, AttachNonce, AttachPolicy, AttachRequest,
        AttachTransportBinding, BitsPerSecond, ByteOffset, DevicePrincipal, DeviceSecret,
        Direction, FeatureOffer, Frame, LegGeneration, OwnerIdentity, Record, RecoveryStageBudgets,
        ReplayHorizon, ResetReason, ResumeSecret, SESSION_PROTOCOL_VERSION, SessionEvent,
        SessionFlowId, SessionId, SessionModel, TlsExporterBinding, VersionRange,
    };
    use crate::shared::TargetAddr;

    mod root_client_observations;
    mod root_recovery_scheduler_integration;
    mod typed_recovery_domains;
    mod typed_recovery_timers;

    const NON_POWER_OF_TWO_EFFECT_BOUND: usize = 3;

    struct FakeDeadline {
        deadline: SimTime,
        payload: Bytes,
    }

    impl RootDeadline for FakeDeadline {
        fn instant(&self) -> SimTime {
            self.deadline
        }
    }

    fn data_effect(marker: u8) -> SessionEffect {
        SessionEffect::Transmit(
            Frame::try_new(
                LegGeneration::new(1).expect("nonzero test generation"),
                Record::Data {
                    flow_id: SessionFlowId::new(1).expect("nonzero test flow"),
                    direction: Direction::TargetToClient,
                    offset: ByteOffset::new(u64::from(marker)),
                    payload: Bytes::from(vec![marker]),
                },
            )
            .expect("one-byte DATA effect is valid"),
        )
    }

    fn data_payload(effect: &SessionEffect) -> &Bytes {
        match effect {
            SessionEffect::Transmit(frame) => match frame.record() {
                Record::Data { payload, .. } => payload,
                other => panic!("test effect changed record kind: {other:?}"),
            },
            other => panic!("test effect changed variant: {other:?}"),
        }
    }

    fn test_leg_for_session(session_byte: u8) -> crate::resumable::CommittedLeg {
        let session_id = SessionId::new([session_byte; 16]).expect("nonzero test session");
        let owner = OwnerIdentity::new([0x31; 32]).expect("nonzero test owner");
        let principal = DevicePrincipal::new([0x41; 16]).expect("nonzero test device principal");
        let alpn = AttachAlpn::new(b"mini-vpn-owned/1").expect("valid test ALPN");
        let binding = AttachTransportBinding::new(
            owner,
            alpn,
            TlsExporterBinding::new([0x51; 32]).expect("nonzero exporter binding"),
            principal,
        );
        let credentials = AttachCredentials::new(
            DeviceSecret::new([0x61; 32]).expect("nonzero device secret"),
            ResumeSecret::new([0x71; 32]).expect("nonzero resume secret"),
        )
        .expect("distinct test secrets");
        let request = AttachRequest::new(
            session_id,
            LegGeneration::new(2).expect("nonzero generation"),
            AttachNonce::new([0x81; 16]).expect("nonzero attach nonce"),
            VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION)
                .expect("one supported protocol version"),
            FeatureOffer::new(0b11, 0b01).expect("valid feature offer"),
        );
        let proof = credentials
            .prove(&request, &binding)
            .expect("test request is signable");
        AttachAuthority::new(
            session_id,
            LegGeneration::new(1).expect("nonzero prior generation"),
            credentials,
            AttachPolicy::new(owner, alpn, principal, SESSION_PROTOCOL_VERSION, 0b11)
                .expect("valid attach policy"),
        )
        .verify_and_commit(&request, &binding, &proof)
        .expect("authenticated test leg")
    }

    fn terminal_test_plan(max_terminal_tombstones: usize) -> QuinnClientRootPlan {
        let horizon = ReplayHorizon::new(Duration::from_millis(450), Duration::from_millis(50))
            .expect("nonzero exact recovery horizon");
        let recovery = RecoveryBudgetSpec::new(
            horizon,
            RecoveryStageBudgets {
                hard_liveness_after_normal_ack: Duration::from_millis(250),
                reverse_stall_after_normal_ack: Duration::from_millis(100),
                authenticated_hint: Duration::from_millis(150),
                attach_install: Duration::from_millis(100),
                replay_to_first_application_ack: Duration::from_millis(100),
            },
        );
        let rate = BitsPerSecond::new(1_024).expect("nonzero test rate");
        QuinnClientRootPlan::preflight(
            recovery,
            DirectionalRates::symmetric(rate),
            DirectionalRates::symmetric(rate),
            ReplayStorageGeometry::new(8, 4).expect("bounded flow geometry"),
            max_terminal_tombstones,
            Duration::from_millis(500),
            64,
            FlowPortConfig::new(64, 8, 8, 8).expect("bounded flow port"),
            4,
            8,
        )
        .expect("valid terminal test plan")
    }

    fn terminal_effects(session_byte: u8, plan: QuinnClientRootPlan) -> Vec<SessionEffect> {
        let leg = test_leg_for_session(session_byte);
        let mut model = SessionModel::new(SessionRole::Client, plan.session_config(), leg);
        let opened = model
            .reduce(SessionEvent::LocalOpen {
                leg,
                target: TargetAddr::IpPort(SocketAddr::V4(SocketAddrV4::new(
                    Ipv4Addr::LOCALHOST,
                    443,
                ))),
            })
            .expect("local OPEN is reducible");
        let flow = opened
            .iter()
            .find_map(|effect| match effect {
                SessionEffect::LocalFlowOpened { flow } => Some(*flow),
                _ => None,
            })
            .expect("local OPEN returns its exact flow authority");
        model
            .reduce(SessionEvent::LocalReset {
                flow,
                reason: ResetReason::LocalAbandon,
            })
            .expect("local abandon produces terminal effects")
    }

    #[test]
    fn live_publication_pending_suppresses_only_the_live_timer_wake() {
        let deadline = SimTime::from_nanos(7);
        assert_eq!(
            live_terminal_wait_deadline(Some(deadline), false),
            Some(deadline)
        );
        assert_eq!(
            live_terminal_wait_deadline(Some(deadline), true),
            None,
            "a due timer blocked behind predecode publication must park on actor progress",
        );
        assert_eq!(live_terminal_wait_deadline(None, true), None);
    }

    #[test]
    fn shutdown_supersedes_ordinary_effects_but_keeps_the_exact_terminal_chain() {
        assert!(!shutdown_effect_is_mandatory(&data_effect(1)));
        let plan = terminal_test_plan(4);
        let effects = terminal_effects(0x21, plan);
        let mut saw_reset = false;
        let mut saw_finished = false;
        for effect in &effects {
            match effect {
                SessionEffect::Transmit(frame)
                    if matches!(frame.record(), Record::Reset { .. }) =>
                {
                    saw_reset = true;
                    assert!(shutdown_effect_is_mandatory(effect));
                }
                SessionEffect::FlowFinished { .. } => {
                    saw_finished = true;
                    assert!(shutdown_effect_is_mandatory(effect));
                }
                _ => assert!(
                    !shutdown_effect_is_mandatory(effect),
                    "only RESET plus FlowFinished survive root shutdown supersession",
                ),
            }
        }
        assert!(saw_reset && saw_finished);
    }

    #[test]
    fn lifecycle_shutdown_cas_prevents_ready_in_the_presignal_gap() {
        let lifecycle = RootLifecycle::new();
        let lifecycle_for_shutdown = Arc::clone(&lifecycle);
        let (shutdown_tx, shutdown_rx) = watch::channel(RootShutdownSignal::Open);
        let (entered_tx, entered_rx) = std_mpsc::sync_channel(0);
        // Capacity one makes release non-blocking even if the shutdown thread
        // has not reached its bounded receive yet.
        let (release_tx, release_rx) = std_mpsc::sync_channel(1);
        let shutdown = std::thread::spawn(move || {
            begin_root_shutdown_signal(
                &lifecycle_for_shutdown,
                &shutdown_tx,
                RootShutdownSignal::Explicit,
                || {
                    entered_tx
                        .send(())
                        .expect("shutdown gap observer remains live");
                    release_rx
                        .recv_timeout(Duration::from_secs(3))
                        .expect("shutdown gap is released within the test bound");
                },
            )
        });

        entered_rx
            .recv_timeout(Duration::from_secs(3))
            .expect("shutdown reaches the lifecycle-before-signal gap");
        assert_eq!(
            *shutdown_rx.borrow(),
            RootShutdownSignal::Open,
            "the tracer is inside the exact lifecycle-before-signal window",
        );
        let publication = arbitrate_root_ready_publication(&lifecycle, &shutdown_rx, || {});
        assert_eq!(publication, RootReadyPublication::ShutdownRequested);
        assert_eq!(lifecycle.phase(), QuinnClientRootPhase::ShuttingDown);
        release_tx
            .send(())
            .expect("shutdown gap receiver remains live");
        let (phase, previous) = shutdown.join().expect("shutdown publisher remains live");
        assert_eq!(phase, Ok(()));
        assert_eq!(previous, RootShutdownSignal::Open);
        assert_eq!(*shutdown_rx.borrow(), RootShutdownSignal::Explicit);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn aborting_a_polled_running_root_future_publishes_task_lost() {
        let lifecycle = RootLifecycle::new();
        lifecycle
            .transition(
                QuinnClientRootPhase::Starting,
                QuinnClientRootPhase::Running,
            )
            .expect("the test root enters Running");
        let guard = RootTaskLifecycleGuard::new(Arc::clone(&lifecycle));
        let task = tokio::spawn(async move {
            let _guard = guard;
            std::future::pending::<()>().await;
        });
        tokio::task::yield_now().await;

        task.abort();
        let error = task
            .await
            .expect_err("the injected root task abort is observable");
        assert!(error.is_cancelled());
        assert_eq!(lifecycle.phase(), QuinnClientRootPhase::TaskLost);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn invalid_root_open_returns_the_exact_caller_target_without_queueing_a_command() {
        let lifecycle = RootLifecycle::new();
        lifecycle
            .transition(
                QuinnClientRootPhase::Starting,
                QuinnClientRootPhase::Running,
            )
            .expect("test handle enters Running");
        let (commands, mut receiver) = mpsc::channel(1);
        let handle = QuinnClientRootHandle {
            identity: Arc::new(QuinnClientRootIdentity),
            commands,
            lifecycle,
            open_reply_publication: Arc::new(RootOpenReplyPublicationHook::default()),
            shutdown_pre_park: Arc::new(RootShutdownPreParkHook::default()),
            shutdown_slice: Arc::new(RootShutdownSliceHook::default()),
            run_probe: Arc::new(RootRunProbe::default()),
            live_category: Arc::new(RootLiveCategoryHook::default()),
        };
        let mut host = String::with_capacity(1_024);
        host.extend(std::iter::repeat_n('x', 254));
        let host_ptr = host.as_ptr();
        let host_capacity = host.capacity();

        let failure = handle
            .open_tcp(TargetAddr::DomainPort { host, port: 443 })
            .await
            .expect_err("oversized target fails before command ownership is cloned");
        assert!(matches!(
            failure.kind(),
            QuinnClientRootOpenFailureKind::InvalidTarget(ProtocolError::InvalidDomainLength {
                len: 254,
                max: 253
            })
        ));
        let TargetAddr::DomainPort { host, port } = failure.into_target() else {
            panic!("invalid domain target changed address family")
        };
        assert_eq!(port, 443);
        assert_eq!(host.as_ptr(), host_ptr);
        assert_eq!(host.capacity(), host_capacity);
        assert!(matches!(
            receiver.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
    }

    #[test]
    fn timer_capacity_covers_fixed_recovery_and_every_terminal_tombstone() {
        const TERMINAL_TOMBSTONES: usize = 5;
        let horizon = ReplayHorizon::new(Duration::from_millis(450), Duration::from_millis(50))
            .expect("nonzero exact recovery horizon");
        let recovery = RecoveryBudgetSpec::new(
            horizon,
            RecoveryStageBudgets {
                hard_liveness_after_normal_ack: Duration::from_millis(250),
                reverse_stall_after_normal_ack: Duration::from_millis(100),
                authenticated_hint: Duration::from_millis(150),
                attach_install: Duration::from_millis(100),
                replay_to_first_application_ack: Duration::from_millis(100),
            },
        );
        let rate = BitsPerSecond::new(1_024).expect("nonzero test rate");
        let plan = QuinnClientRootPlan::preflight(
            recovery,
            DirectionalRates::symmetric(rate),
            DirectionalRates::symmetric(rate),
            ReplayStorageGeometry::new(8, 4).expect("bounded flow geometry"),
            TERMINAL_TOMBSTONES,
            Duration::from_millis(500),
            64,
            FlowPortConfig::new(64, 8, 8, 8).expect("bounded flow port"),
            4,
            8,
        )
        .expect("the checked timer geometry is valid");

        assert_eq!(
            plan.timer_capacity(),
            ROOT_RECOVERY_TIMER_SLOTS + TERMINAL_TOMBSTONES,
        );
        assert_eq!(plan.incremental_effect_bound(), 2);
        assert_eq!(plan.terminal_grace(), Duration::from_millis(500));
        assert_eq!(
            plan.pending_effect_capacity(),
            plan.session_config()
                .client_pending_effect_bound()
                .expect("the checked client pending-effect bound fits usize"),
        );
        let storage = RootStorage::preallocate(plan).expect("all timer slots preallocate");
        assert!(storage.recovery_timers.is_empty());
        assert_eq!(
            storage.recovery_timers.capacity(),
            ROOT_RECOVERY_TIMER_SLOTS
        );
        assert!(storage.terminal_timers.is_empty());
        assert_eq!(
            storage.terminal_timers.logical_capacity(),
            TERMINAL_TOMBSTONES
        );
        assert!(
            storage.terminal_timers.physical_capacity() >= TERMINAL_TOMBSTONES,
            "the typed terminal registry is fully preallocated"
        );
    }

    #[test]
    fn terminal_grace_must_fit_the_nonzero_sim_time_domain() {
        let horizon = ReplayHorizon::new(Duration::from_millis(450), Duration::from_millis(50))
            .expect("nonzero exact recovery horizon");
        let recovery = RecoveryBudgetSpec::new(
            horizon,
            RecoveryStageBudgets {
                hard_liveness_after_normal_ack: Duration::from_millis(250),
                reverse_stall_after_normal_ack: Duration::from_millis(100),
                authenticated_hint: Duration::from_millis(150),
                attach_install: Duration::from_millis(100),
                replay_to_first_application_ack: Duration::from_millis(100),
            },
        );
        let rate = BitsPerSecond::new(1_024).expect("nonzero test rate");
        let preflight = |terminal_grace| {
            QuinnClientRootPlan::preflight(
                recovery,
                DirectionalRates::symmetric(rate),
                DirectionalRates::symmetric(rate),
                ReplayStorageGeometry::new(8, 4).expect("bounded flow geometry"),
                4,
                terminal_grace,
                64,
                FlowPortConfig::new(64, 8, 8, 8).expect("bounded flow port"),
                4,
                8,
            )
        };

        assert_eq!(
            preflight(Duration::ZERO),
            Err(QuinnClientRootPlanError::ZeroTerminalGrace),
        );
        let outside_sim_time = Duration::from_secs(u64::MAX);
        assert_eq!(
            preflight(outside_sim_time),
            Err(QuinnClientRootPlanError::TerminalGraceOutsideSimTime {
                value: outside_sim_time,
            }),
        );
    }

    #[test]
    fn deadline_heap_is_logically_bounded_and_returns_exact_rejected_ownership() {
        let mut deadlines = RootDeadlineHeap::preallocate(2).expect("two exact timer slots");
        assert_eq!(deadlines.logical_capacity(), 2);
        assert!(deadlines.is_empty());

        for (deadline, marker) in [(20, 0x20), (10, 0x10)] {
            assert!(
                deadlines
                    .try_insert(FakeDeadline {
                        deadline: SimTime::from_nanos(deadline),
                        payload: Bytes::from(vec![marker]),
                    })
                    .is_ok()
            );
        }
        let rejected = FakeDeadline {
            deadline: SimTime::from_nanos(5),
            payload: Bytes::from_static(b"exact-rejected-timer-backing"),
        };
        let rejected_ptr = rejected.payload.as_ptr();
        let returned = match deadlines.try_insert(rejected) {
            Err(RootDeadlineHeapAdmissionError::LogicalCapacity(timer)) => timer,
            Err(RootDeadlineHeapAdmissionError::SequenceExhausted(_)) => {
                panic!("small heap cannot exhaust its stable sequence")
            }
            Ok(()) => panic!("logical N + 1 must fail despite allocator over-reservation"),
        };
        assert_eq!(returned.payload.as_ptr(), rejected_ptr);
        assert_eq!(deadlines.len(), 2);
        assert!(deadlines.pop_due(SimTime::from_nanos(9)).is_none());

        let first = deadlines
            .pop_due(SimTime::from_nanos(10))
            .expect("the earliest timer is due");
        assert_eq!(first.instant(), SimTime::from_nanos(10));
        assert_eq!(first.timer().payload.as_ref(), &[0x10]);
        let first_ptr = first.timer().payload.as_ptr();
        assert!(deadlines.reinsert(first).is_ok());
        let first = deadlines
            .pop_due(SimTime::from_nanos(10))
            .expect("reinsertion retains the same absolute deadline");
        assert_eq!(first.timer().payload.as_ptr(), first_ptr);
    }

    #[test]
    fn equal_deadlines_leave_the_heap_in_stable_insertion_order() {
        let mut deadlines = RootDeadlineHeap::preallocate(2).expect("two exact timer slots");
        for marker in [1_u8, 2_u8] {
            assert!(
                deadlines
                    .try_insert(FakeDeadline {
                        deadline: SimTime::from_nanos(7),
                        payload: Bytes::from(vec![marker]),
                    })
                    .is_ok()
            );
        }

        for expected in [1_u8, 2_u8] {
            let scheduled = deadlines
                .pop_due(SimTime::from_nanos(7))
                .expect("equal-deadline timer remains due");
            assert_eq!(scheduled.into_timer().payload.as_ref(), &[expected]);
        }
        assert!(deadlines.is_empty());
    }

    #[test]
    fn terminal_effect_batch_commits_queue_and_one_fixed_deadline_together() {
        let plan = terminal_test_plan(2);
        let mut storage = RootStorage::preallocate(plan).expect("bounded root storage");
        let pending_physical_capacity = storage.pending_effects.capacity();
        let terminal_physical_capacity = storage.terminal_timers.physical_capacity();
        let observed_at = SimTime::from_nanos(17);
        let expected_deadline = observed_at
            .checked_add(plan.terminal_grace())
            .expect("small terminal deadline");
        let effects = terminal_effects(0x91, plan);
        assert_eq!(effects.len(), plan.incremental_effect_bound());
        assert_eq!(
            effects
                .iter()
                .filter(|effect| matches!(effect, SessionEffect::FlowFinished { .. }))
                .count(),
            1
        );

        try_admit_pending_effects_with_terminal_timers(
            &mut storage.pending_effects,
            &mut storage.terminal_timers,
            effects,
            plan,
            observed_at,
        )
        .expect("the complete incremental batch fits both registries");

        assert_eq!(
            storage.pending_effects.len(),
            plan.incremental_effect_bound()
        );
        assert_eq!(storage.terminal_timers.len(), 1);
        assert_eq!(
            storage.pending_effects.capacity(),
            pending_physical_capacity
        );
        assert_eq!(
            storage.terminal_timers.physical_capacity(),
            terminal_physical_capacity,
            "successful admission cannot grow either preallocated registry"
        );
        assert!(
            storage
                .terminal_timers
                .pop_due(SimTime::from_nanos(expected_deadline.as_nanos() - 1))
                .is_none()
        );
        let due = storage
            .terminal_timers
            .pop_due(expected_deadline)
            .expect("the first absolute deadline is exact");
        assert_eq!(due.instant(), expected_deadline);
    }

    #[test]
    fn retryable_pending_effects_do_not_starve_an_absolute_terminal_timer() {
        let plan = terminal_test_plan(1);
        let mut storage = RootStorage::preallocate(plan).expect("bounded root storage");
        try_admit_pending_effects_with_terminal_timers(
            &mut storage.pending_effects,
            &mut storage.terminal_timers,
            terminal_effects(0xa4, plan),
            plan,
            SimTime::ZERO,
        )
        .expect("one pending batch and its exact timer fit together");
        assert!(!storage.pending_effects.is_empty());
        assert!(!storage.terminal_timers.is_empty());
        assert_eq!(terminal_timer_scan_admission(&storage), Ok(()));

        storage.aborted_effects = Some(Vec::new());
        assert!(
            terminal_timer_scan_admission(&storage).is_err(),
            "retained exact effects still fail closed before another reducer mutation",
        );
    }

    #[test]
    fn terminal_timer_n_plus_one_returns_the_exact_effect_batch_without_partial_commit() {
        let plan = terminal_test_plan(1);
        let mut storage = RootStorage::preallocate(plan).expect("bounded root storage");
        try_admit_pending_effects_with_terminal_timers(
            &mut storage.pending_effects,
            &mut storage.terminal_timers,
            terminal_effects(0x92, plan),
            plan,
            SimTime::from_nanos(5),
        )
        .expect("the one terminal slot is admissible");
        storage.pending_effects.clear();

        let rejected = terminal_effects(0x93, plan);
        let batch_ptr = rejected.as_ptr();
        let failure = try_admit_pending_effects_with_terminal_timers(
            &mut storage.pending_effects,
            &mut storage.terminal_timers,
            rejected,
            plan,
            SimTime::from_nanos(6),
        )
        .expect_err("terminal timer logical N + 1 fails closed");
        assert_eq!(
            failure.kind(),
            RootEffectAdmissionFailureKind::TerminalTimerLogicalCapacity
        );
        let returned = failure.into_effects();

        assert!(storage.pending_effects.is_empty());
        assert_eq!(storage.terminal_timers.len(), 1);
        assert_eq!(returned.len(), plan.incremental_effect_bound());
        assert_eq!(
            returned.as_ptr(),
            batch_ptr,
            "capacity rejection returns the exact Vec allocation"
        );
    }

    #[test]
    fn terminal_deadline_overflow_returns_every_effect_without_mutating_either_registry() {
        let plan = terminal_test_plan(1);
        let mut storage = RootStorage::preallocate(plan).expect("bounded root storage");
        let effects = terminal_effects(0x94, plan);

        let failure = try_admit_pending_effects_with_terminal_timers(
            &mut storage.pending_effects,
            &mut storage.terminal_timers,
            effects,
            plan,
            SimTime::from_nanos(u64::MAX),
        )
        .expect_err("an absolute deadline outside SimTime fails before either write");

        assert_eq!(
            failure.kind(),
            RootEffectAdmissionFailureKind::TerminalDeadlineOverflow
        );
        assert_eq!(
            failure.into_effects().len(),
            plan.incremental_effect_bound()
        );
        assert!(storage.pending_effects.is_empty());
        assert!(storage.terminal_timers.is_empty());
    }

    #[test]
    fn every_effect_timer_preflight_failure_is_transactional_and_ownership_exact() {
        let plan = terminal_test_plan(2);

        let mut pending = VecDeque::new();
        let mut terminal_timers = RootDeadlineHeap::preallocate(2).expect("two terminal slots");
        let physical_effects = vec![data_effect(0x31)];
        let physical_ptr = physical_effects.as_ptr();
        let physical = try_admit_pending_effects_with_terminal_timers(
            &mut pending,
            &mut terminal_timers,
            physical_effects,
            plan,
            SimTime::ZERO,
        )
        .expect_err("an unallocated pending queue fails before mutation");
        assert_eq!(
            physical.kind(),
            RootEffectAdmissionFailureKind::PendingEffectPhysicalCapacity
        );
        let physical_effects = physical.into_effects();
        assert_eq!(physical_effects.as_ptr(), physical_ptr);
        assert!(pending.is_empty());
        assert!(terminal_timers.is_empty());

        let mut pending = VecDeque::with_capacity(plan.pending_effect_capacity());
        let mut terminal_timers = RootDeadlineHeap {
            heap: BinaryHeap::new(),
            logical_capacity: 2,
            next_sequence: 0,
        };
        let physical_terminal_effects = terminal_effects(0x95, plan);
        let terminal_ptr = physical_terminal_effects.as_ptr();
        let terminal_physical = try_admit_pending_effects_with_terminal_timers(
            &mut pending,
            &mut terminal_timers,
            physical_terminal_effects,
            plan,
            SimTime::ZERO,
        )
        .expect_err("an unallocated terminal heap fails before mutation");
        assert_eq!(
            terminal_physical.kind(),
            RootEffectAdmissionFailureKind::TerminalTimerPhysicalCapacity
        );
        let returned_terminal_effects = terminal_physical.into_effects();
        assert_eq!(returned_terminal_effects.as_ptr(), terminal_ptr);
        assert!(pending.is_empty());
        assert!(terminal_timers.is_empty());

        let mut pending = VecDeque::with_capacity(plan.pending_effect_capacity());
        let mut terminal_timers = RootDeadlineHeap::preallocate(2).expect("two terminal slots");
        terminal_timers.next_sequence = u64::MAX;
        let sequenced_terminal_effects = terminal_effects(0x96, plan);
        let terminal_ptr = sequenced_terminal_effects.as_ptr();
        let sequence = try_admit_pending_effects_with_terminal_timers(
            &mut pending,
            &mut terminal_timers,
            sequenced_terminal_effects,
            plan,
            SimTime::ZERO,
        )
        .expect_err("stable sequence exhaustion fails before mutation");
        assert_eq!(
            sequence.kind(),
            RootEffectAdmissionFailureKind::TerminalTimerSequenceExhausted
        );
        let returned_sequenced_effects = sequence.into_effects();
        assert_eq!(returned_sequenced_effects.as_ptr(), terminal_ptr);
        assert!(pending.is_empty());
        assert!(terminal_timers.is_empty());

        let mut pending = VecDeque::with_capacity(plan.pending_effect_capacity());
        let mut terminal_timers = RootDeadlineHeap::preallocate(2).expect("two terminal slots");
        let oversized = vec![data_effect(1), data_effect(2), data_effect(3)];
        let oversized_ptr = oversized.as_ptr();
        let incremental = try_admit_pending_effects_with_terminal_timers(
            &mut pending,
            &mut terminal_timers,
            oversized,
            plan,
            SimTime::ZERO,
        )
        .expect_err("incremental N + 1 fails before mutation");
        assert_eq!(
            incremental.kind(),
            RootEffectAdmissionFailureKind::IncrementalEffectBound
        );
        let returned_oversized = incremental.into_effects();
        assert_eq!(returned_oversized.as_ptr(), oversized_ptr);
        assert!(pending.is_empty());
        assert!(terminal_timers.is_empty());

        let mut pending = VecDeque::with_capacity(plan.pending_effect_capacity());
        for marker in 0..plan.pending_effect_capacity() {
            pending.push_back(data_effect(
                u8::try_from(marker).expect("the checked test bound fits one-byte markers"),
            ));
        }
        let mut terminal_timers = RootDeadlineHeap::preallocate(2).expect("two terminal slots");
        let logical_effects = vec![data_effect(0x7e)];
        let logical_ptr = logical_effects.as_ptr();
        let logical = try_admit_pending_effects_with_terminal_timers(
            &mut pending,
            &mut terminal_timers,
            logical_effects,
            plan,
            SimTime::ZERO,
        )
        .expect_err("pending logical N + 1 fails before timer mutation");
        assert_eq!(
            logical.kind(),
            RootEffectAdmissionFailureKind::PendingEffectLogicalCapacity
        );
        let returned_logical = logical.into_effects();
        assert_eq!(returned_logical.as_ptr(), logical_ptr);
        assert_eq!(pending.len(), plan.pending_effect_capacity());
        assert!(terminal_timers.is_empty());
    }

    #[test]
    fn work_budget_above_internal_turn_bound_fails_preflight() {
        let horizon = ReplayHorizon::new(Duration::from_millis(450), Duration::from_millis(50))
            .expect("nonzero exact recovery horizon");
        let recovery = RecoveryBudgetSpec::new(
            horizon,
            RecoveryStageBudgets {
                hard_liveness_after_normal_ack: Duration::from_millis(250),
                reverse_stall_after_normal_ack: Duration::from_millis(100),
                authenticated_hint: Duration::from_millis(150),
                attach_install: Duration::from_millis(100),
                replay_to_first_application_ack: Duration::from_millis(100),
            },
        );
        let rate = BitsPerSecond::new(1_024).expect("nonzero test rate");
        let first_unproved = MAX_WORK_PER_TURN
            .checked_add(1)
            .expect("internal turn bound leaves room for its rejection tracer");

        for requested in [first_unproved, usize::MAX] {
            let error = QuinnClientRootPlan::preflight(
                recovery,
                DirectionalRates::symmetric(rate),
                DirectionalRates::symmetric(rate),
                ReplayStorageGeometry::new(8, 4).expect("bounded flow geometry"),
                4,
                Duration::from_millis(500),
                64,
                FlowPortConfig::new(64, 8, 8, 8).expect("bounded flow port"),
                4,
                requested,
            )
            .expect_err("an unproven synchronous work turn must fail before root start");

            assert_eq!(
                error,
                QuinnClientRootPlanError::WorkBudgetTooLarge {
                    value: requested,
                    max: MAX_WORK_PER_TURN,
                }
            );
        }
    }

    #[test]
    fn non_power_of_two_pending_effect_bound_rejects_n_plus_one_without_losing_ownership() {
        let mut pending_effects = VecDeque::with_capacity(8);
        assert!(
            pending_effects.capacity() > NON_POWER_OF_TWO_EFFECT_BOUND,
            "the tracer must distinguish physical allocation from the logical bound",
        );

        let admitted = (0..NON_POWER_OF_TWO_EFFECT_BOUND)
            .map(|marker| data_effect(u8::try_from(marker).expect("small marker")))
            .collect();
        try_admit_pending_effects(
            &mut pending_effects,
            admitted,
            NON_POWER_OF_TWO_EFFECT_BOUND,
        )
        .expect("the exact logical bound remains admissible");

        let rejected_effect = data_effect(0x7f);
        let rejected_payload = data_payload(&rejected_effect).as_ptr();
        let rejected = try_admit_pending_effects(
            &mut pending_effects,
            vec![rejected_effect],
            NON_POWER_OF_TWO_EFFECT_BOUND,
        )
        .expect_err("effect N + 1 must fail closed despite spare physical capacity");

        assert_eq!(pending_effects.len(), NON_POWER_OF_TWO_EFFECT_BOUND);
        assert_eq!(rejected.len(), 1, "the rejected effect remains owned");
        assert_eq!(
            data_payload(&rejected[0]).as_ptr(),
            rejected_payload,
            "fail-closed admission returns the exact payload allocation",
        );

        drop(rejected);
        pending_effects.clear();
        assert!(
            pending_effects.is_empty(),
            "cleanup releases every admitted effect"
        );
    }
}
