//! Byte-level transport boundary for one authenticated resumable leg.
//!
//! The adapter is deliberately the only path from a protocol [`Frame`] to
//! [`TwoLegWire`] and from an [`EncodedDelivery`] to a [`LegBoundFrame`].
//! Fault injection therefore observes and mutates only encoded bytes.  A
//! successfully decoded frame receives provenance from this endpoint's exact
//! process-local [`EstablishedLeg`] and from no numeric leg identifier.

use bytes::Bytes;
use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use thiserror::Error;

use crate::owned_upstream::client_controller::{
    ClientControllerBootstrapGate, ClientControllerTransportGate, ClientRecoveryTerminalGate,
};
use crate::owned_upstream::leg::{
    AttachedLeg, BoundInbound, EstablishedLeg, ExactLegTerminal, LegBoundFrame,
    LegOutboundQueueLease, LegProvenanceError, LegSeal, LegTransportEndpoint, LegTransportReporter,
    PendingInitialAttach,
};
use crate::owned_upstream::quinn_leg_actor::{
    QuinnLegActorInboundGate, QuinnLegActorTransportGate,
};
use crate::owned_upstream::quinn_leg_composition::QuinnLegCompositionGate;
use crate::owned_upstream::standby::RegisteredAttachCausalDeliveryGate;
use crate::owned_upstream::two_leg::{
    EncodedDelivery, FaultAction, LegId, SimTime, TwoLegWire, WireCapacity, WireCounters,
    WireDirection, WireError, WireLane, WireRoute,
};
use crate::resumable::{
    ATTACH_ACCEPTED_ENCODED_BYTES, AttachNonce, AttachRequest, AttachTransportBinding,
    DecodedFrame, Direction, FeatureSet, Frame, LegControlFrame, LegGeneration, ProtocolError,
    Record, SessionEffect, SessionFlowId, SessionId,
};

/// Local endpoint role for one authenticated transport connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LegEndpointRole {
    Client,
    Owner,
}

impl LegEndpointRole {
    const fn outbound_direction(self) -> WireDirection {
        match self {
            Self::Client => WireDirection::ClientToOwner,
            Self::Owner => WireDirection::OwnerToClient,
        }
    }

    const fn inbound_direction(self) -> WireDirection {
        match self {
            Self::Client => WireDirection::OwnerToClient,
            Self::Owner => WireDirection::ClientToOwner,
        }
    }
}

/// Finite lifetime work admitted by one local leg endpoint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct LegIoLimits {
    max_frame_bytes: u64,
    max_outbound_messages: u64,
    max_outbound_bytes: u64,
    max_inbound_messages: u64,
    max_inbound_bytes: u64,
}

impl LegIoLimits {
    pub(crate) fn new(
        max_frame_bytes: usize,
        max_outbound_messages: u64,
        max_outbound_bytes: u64,
        max_inbound_messages: u64,
        max_inbound_bytes: u64,
    ) -> Result<Self, LegIoConfigError> {
        let max_frame_bytes = u64::try_from(max_frame_bytes)
            .map_err(|_| LegIoConfigError::PlatformCapacityOverflow)?;
        if max_frame_bytes == 0 {
            return Err(LegIoConfigError::ZeroFrameBound);
        }
        if max_outbound_messages == 0 || max_inbound_messages == 0 {
            return Err(LegIoConfigError::ZeroMessageBudget);
        }
        if max_outbound_bytes < max_frame_bytes || max_inbound_bytes < max_frame_bytes {
            return Err(LegIoConfigError::FrameExceedsByteBudget);
        }
        Ok(Self {
            max_frame_bytes,
            max_outbound_messages,
            max_outbound_bytes,
            max_inbound_messages,
            max_inbound_bytes,
        })
    }

    pub(crate) fn from_wire_capacity(
        wire_capacity: WireCapacity,
        max_frame_bytes: usize,
    ) -> Result<Self, LegIoConfigError> {
        Self::new(
            max_frame_bytes,
            wire_capacity.max_send_messages(),
            wire_capacity.max_send_bytes(),
            wire_capacity.max_delivery_messages(),
            wire_capacity.max_delivery_bytes(),
        )
    }

    pub(crate) const fn max_frame_bytes(self) -> u64 {
        self.max_frame_bytes
    }
}

/// Unforgeable authority for the sole production `EstablishedLeg` source.
/// The fixed Quinn gate may reach `LegIo`, but only this module can turn it
/// into authenticated leg identity plus the terminal reporter.
pub(super) struct LegIoAuthenticatedTransportGate(());

impl LegIoAuthenticatedTransportGate {
    const fn mint_for_leg_io() -> Self {
        Self(())
    }
}

/// Process-local identity shared only between the physical actor owner and
/// the exact `LegIo` installed into the policy controller. Pointer identity,
/// never a route/generation/integer, is the comparison authority.
struct PhysicalLegIncarnationIdentity;

/// Runtime half retained by one fixed Quinn actor and moved once into its
/// joined witness. It is intentionally neither `Clone` nor `Copy`.
pub(super) struct PhysicalLegRuntimeIncarnation(Arc<PhysicalLegIncarnationIdentity>);

/// Policy half stored only inside the `LegIo` minted for that same actor.
/// Moving `LegIo` through active/retired controller states moves this half
/// without creating another source of truth.
pub(super) struct PhysicalLegPolicyIncarnation(Arc<PhysicalLegIncarnationIdentity>);

/// Non-cloneable persistent binding to one exact physical actor incarnation
/// and its sole outbound FIFO. It can be minted only from a pointer-identical
/// runtime/policy pair whose queue still belongs to that exact EstablishedLeg.
pub(super) struct PhysicalLegActorBinding {
    actor: Weak<PhysicalLegIncarnationIdentity>,
    queue: Weak<LegOutboundQueueIdentity>,
}

impl PhysicalLegRuntimeIncarnation {
    pub(super) fn mint_for_quinn_actor(
        _gate: &QuinnLegActorTransportGate,
    ) -> (Self, PhysicalLegPolicyIncarnation) {
        let identity = Arc::new(PhysicalLegIncarnationIdentity);
        (
            Self(Arc::clone(&identity)),
            PhysicalLegPolicyIncarnation(identity),
        )
    }

    fn matches_policy(&self, policy: &PhysicalLegPolicyIncarnation) -> bool {
        Arc::ptr_eq(&self.0, &policy.0)
    }
}

impl fmt::Debug for PhysicalLegRuntimeIncarnation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PhysicalLegRuntimeIncarnation([REDACTED])")
    }
}

impl fmt::Debug for PhysicalLegPolicyIncarnation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PhysicalLegPolicyIncarnation([REDACTED])")
    }
}

impl fmt::Debug for PhysicalLegActorBinding {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PhysicalLegActorBinding([REDACTED])")
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub(crate) enum LegIoConfigError {
    #[error("leg frame bound must be nonzero")]
    ZeroFrameBound,
    #[error("leg message budgets must be nonzero")]
    ZeroMessageBudget,
    #[error("leg frame bound exceeds a directional byte budget")]
    FrameExceedsByteBudget,
    #[error("leg I/O capacity exceeds platform limits")]
    PlatformCapacityOverflow,
    #[error("leg outbound frame queue must admit at least one frame")]
    ZeroOutboundQueueFrames,
    #[error("leg outbound frame queue must own at least one byte")]
    ZeroOutboundQueueBytes,
    #[error("leg outbound frame queue storage could not be preallocated")]
    OutboundQueueStorageUnavailable,
    #[error("authenticated leg already minted its sole outbound queue")]
    OutboundQueueAlreadyMinted,
}

/// Exact, finite accounting for one endpoint.  Each receive rejection is a
/// subset of `inbound_messages`, so adversarial malformed input cannot grow a
/// diagnostic counter beyond the configured lifetime budget.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct LegIoCounters {
    pub(crate) outbound_messages: u64,
    pub(crate) outbound_bytes: u64,
    pub(crate) outbound_control_messages: u64,
    pub(crate) outbound_data_messages: u64,
    pub(crate) inbound_messages: u64,
    pub(crate) inbound_bytes: u64,
    pub(crate) bound_messages: u64,
    pub(crate) bound_bytes: u64,
    pub(crate) bound_session_messages: u64,
    pub(crate) bound_leg_control_messages: u64,
    pub(crate) wrong_route_rejections: u64,
    pub(crate) wrong_lane_rejections: u64,
    pub(crate) decode_rejections: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ClientRecoveryBudgetSnapshot {
    pub(crate) reserved_outbound_messages: u64,
    pub(crate) reserved_outbound_bytes: u64,
}

/// Controller-internal distinction between a physical submission and queue
/// progress caused by completion of the preceding ordered delivery.  Either
/// can make a pressure-blocked recovery cursor eligible for one new turn.
pub(super) struct ClientLegFlushProgress {
    submission: Option<OutboundSubmission>,
    ordered_delivery_completed: bool,
}

impl ClientLegFlushProgress {
    pub(super) const fn recovery_admission_ready(&self) -> bool {
        self.submission.is_some() || self.ordered_delivery_completed
    }

    pub(super) fn into_submission(self) -> Option<OutboundSubmission> {
        self.submission
    }
}

/// One non-cloneable local endpoint of an authenticated leg.
pub(crate) struct LegIo {
    leg_id: LegId,
    role: LegEndpointRole,
    established: EstablishedLeg,
    physical_incarnation: Option<PhysicalLegPolicyIncarnation>,
    limits: LegIoLimits,
    counters: LegIoCounters,
    reserved_outbound_messages: u64,
    reserved_outbound_bytes: u64,
}

/// Opaque one-shot submission minted only by [`LegIo`]. Production transport
/// implementations may inspect it after receiving it, but controller code
/// cannot construct arbitrary encoded bytes or bypass the exact queue.
pub(crate) struct EncodedLegSubmission {
    now: SimTime,
    route: WireRoute,
    lane: WireLane,
    bytes: Bytes,
    ordered_completion: Option<OrderedDeliveryToken>,
}

impl EncodedLegSubmission {
    pub(crate) fn into_parts(
        self,
    ) -> (
        SimTime,
        WireRoute,
        WireLane,
        Bytes,
        Option<OrderedDeliveryToken>,
    ) {
        (
            self.now,
            self.route,
            self.lane,
            self.bytes,
            self.ordered_completion,
        )
    }
}

struct OrderedDeliveryState {
    delivered: AtomicBool,
    client_recovery: Option<Arc<ClientRecoveryDeliveryState>>,
    tracked_reset: Option<Arc<OrderedResetDeliveryState>>,
}

/// Test-only scheduler control for the production-valid instant between the
/// two independent RESET-delivery snapshot loads used by replacement
/// preflight. The gate is thread-local so unrelated parallel replacement tests
/// cannot observe or consume it.
#[cfg(test)]
pub(crate) struct ClientResetRebindSnapshotGate {
    first_load_reached: AtomicBool,
    release_after_delivery: AtomicBool,
}

#[cfg(test)]
impl ClientResetRebindSnapshotGate {
    pub(crate) const fn new() -> Self {
        Self {
            first_load_reached: AtomicBool::new(false),
            release_after_delivery: AtomicBool::new(false),
        }
    }

    pub(crate) fn first_load_reached(&self) -> bool {
        self.first_load_reached.load(Ordering::Acquire)
    }

    pub(crate) fn release_after_delivery(&self) {
        self.release_after_delivery.store(true, Ordering::Release);
    }

    fn wait_between_loads(&self, delivery_state: &OrderedDeliveryState) {
        self.first_load_reached.store(true, Ordering::Release);
        while !self.release_after_delivery.load(Ordering::Acquire) {
            std::thread::yield_now();
        }
        // The real actor delivery child publishes the shared root decision
        // before this per-attempt bit. Waiting for the latter makes the torn
        // snapshot interleaving deterministic without forging either write.
        while !delivery_state.delivered.load(Ordering::Acquire) {
            std::thread::yield_now();
        }
    }
}

#[cfg(test)]
thread_local! {
    static CLIENT_RESET_REBIND_SNAPSHOT_GATE:
        std::cell::RefCell<Option<Arc<ClientResetRebindSnapshotGate>>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
struct ClientResetRebindSnapshotGateGuard;

#[cfg(test)]
impl Drop for ClientResetRebindSnapshotGateGuard {
    fn drop(&mut self) {
        CLIENT_RESET_REBIND_SNAPSHOT_GATE.with(|slot| {
            slot.replace(None);
        });
    }
}

#[cfg(test)]
pub(crate) fn with_client_reset_rebind_snapshot_gate_for_test<R>(
    gate: Arc<ClientResetRebindSnapshotGate>,
    run: impl FnOnce() -> R,
) -> R {
    CLIENT_RESET_REBIND_SNAPSHOT_GATE.with(|slot| {
        assert!(
            slot.replace(Some(gate)).is_none(),
            "one replacement thread cannot arm two RESET snapshot gates",
        );
    });
    let _guard = ClientResetRebindSnapshotGateGuard;
    run()
}

#[cfg(test)]
fn wait_on_client_reset_rebind_snapshot_gate(delivery_state: &OrderedDeliveryState) {
    let gate = CLIENT_RESET_REBIND_SNAPSHOT_GATE.with(|slot| slot.borrow().as_ref().cloned());
    if let Some(gate) = gate {
        gate.wait_between_loads(delivery_state);
    }
}

const RESET_DELIVERY_PENDING: u8 = 1;
const RESET_DELIVERY_DELIVERED: u8 = 2;
const RESET_DELIVERY_DEADLINE_WON: u8 = 3;
const RESET_DELIVERY_REBINDING: u8 = 4;

/// Root-level linearization word for one reducer RESET and its absolute
/// terminal deadline. A physical leg delivery state may point at this word,
/// but it remains a distinct allocation so normal replacement can rebind the
/// same deadline authority to B while retaining B's own recovery counter.
struct OrderedResetDeliveryState {
    flow_id: SessionFlowId,
    deadline_state: AtomicU8,
}

/// Pointer-identical observer shared only by one reducer RESET effect, its
/// exact FIFO/delivery token, and the corresponding terminal timer. Numeric
/// flow identity is validation; this allocation's identity is the race
/// authority.
pub(super) struct OrderedResetDeliveryReceipt {
    state: Arc<OrderedResetDeliveryState>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum OrderedResetDeadlineCommit {
    Delivered,
    DeadlineWon,
}

impl OrderedResetDeliveryReceipt {
    pub(super) fn pending(flow_id: SessionFlowId) -> Self {
        Self {
            state: Arc::new(OrderedResetDeliveryState {
                flow_id,
                deadline_state: AtomicU8::new(RESET_DELIVERY_PENDING),
            }),
        }
    }

    pub(super) fn flow_id(&self) -> SessionFlowId {
        self.state.flow_id
    }

    pub(super) fn clone_for_queue(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
        }
    }

    pub(super) fn try_commit_deadline(&self) -> OrderedResetDeadlineCommit {
        loop {
            match self.state.deadline_state.load(Ordering::Acquire) {
                RESET_DELIVERY_DELIVERED => return OrderedResetDeadlineCommit::Delivered,
                RESET_DELIVERY_DEADLINE_WON => {
                    return OrderedResetDeadlineCommit::DeadlineWon;
                }
                RESET_DELIVERY_PENDING => {
                    if self
                        .state
                        .deadline_state
                        .compare_exchange(
                            RESET_DELIVERY_PENDING,
                            RESET_DELIVERY_DEADLINE_WON,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        return OrderedResetDeadlineCommit::DeadlineWon;
                    }
                }
                RESET_DELIVERY_REBINDING => std::hint::spin_loop(),
                _ => return OrderedResetDeadlineCommit::DeadlineWon,
            }
        }
    }

    pub(super) fn deadline_won(&self) -> bool {
        self.state.deadline_state.load(Ordering::Acquire) == RESET_DELIVERY_DEADLINE_WON
    }

    pub(super) fn is_pending(&self) -> bool {
        self.state.deadline_state.load(Ordering::Acquire) == RESET_DELIVERY_PENDING
    }

    fn into_state(self) -> Arc<OrderedDeliveryState> {
        Arc::new(OrderedDeliveryState {
            delivered: AtomicBool::new(false),
            client_recovery: None,
            tracked_reset: Some(self.state),
        })
    }

    fn is_pending_for(&self, flow_id: SessionFlowId) -> bool {
        self.flow_id() == flow_id
            && self.state.deadline_state.load(Ordering::Acquire) == RESET_DELIVERY_PENDING
    }
}

/// Pre-commit recovery delivery fact shared by the exact prepared frames and
/// the eventual controller tail. Only each frame's ordered token can reduce
/// this count.
struct ClientRecoveryDeliveryState {
    undelivered_frames: AtomicU64,
}

struct OrderedDeliveryWait(Arc<OrderedDeliveryState>);

/// Non-cloneable session-frame completion authority. Its private constructor
/// prevents leg-control delivery from being presented as attach/recovery
/// completion before both enter the opaque transport submission.
pub(crate) struct OrderedSessionDeliveryToken(Arc<OrderedDeliveryState>);

/// Non-cloneable leg-control completion authority. Keeping this type distinct
/// from [`OrderedSessionDeliveryToken`] prevents a queue branch from mixing a
/// standby-control barrier with attach/recovery completion.
pub(crate) struct OrderedLegControlDeliveryToken(Arc<OrderedDeliveryState>);

/// The exact completion carried by one opaque ordered submission. Variants
/// preserve the queue-side class distinction while the sole transport actor
/// serializes both classes through the same route and send ordinal.
pub(crate) enum OrderedDeliveryToken {
    Session(OrderedSessionDeliveryToken),
    LegControl(OrderedLegControlDeliveryToken),
}

impl OrderedDeliveryWait {
    fn state_pair() -> (Self, Arc<OrderedDeliveryState>) {
        let state = Arc::new(OrderedDeliveryState {
            delivered: AtomicBool::new(false),
            client_recovery: None,
            tracked_reset: None,
        });
        (Self(Arc::clone(&state)), state)
    }

    fn session_pair() -> (Self, OrderedSessionDeliveryToken) {
        let (wait, state) = Self::state_pair();
        (wait, OrderedSessionDeliveryToken(state))
    }

    fn leg_control_pair() -> (Self, OrderedLegControlDeliveryToken) {
        let (wait, state) = Self::state_pair();
        (wait, OrderedLegControlDeliveryToken(state))
    }

    fn is_delivered(&self) -> bool {
        // FIFO serialization is per physical submission attempt. The shared
        // RESET word arbitrates only the root timer: reading it here would let
        // A's late completion release an unrelated B wait, or let a won
        // deadline keep an actually delivered B wait pending forever.
        self.0.delivered.load(Ordering::Acquire)
    }

    fn belongs_to_client_recovery(&self, recovery: &Arc<ClientRecoveryDeliveryState>) -> bool {
        self.0
            .client_recovery
            .as_ref()
            .is_some_and(|owned| Arc::ptr_eq(owned, recovery))
    }
}

fn reset_flow_id(frame: &Frame) -> Option<SessionFlowId> {
    match frame.record() {
        Record::Reset { flow_id, .. } => Some(*flow_id),
        _ => None,
    }
}

impl OrderedDeliveryToken {
    pub(crate) fn mark_delivered(self) {
        let state = match self {
            Self::Session(token) => token.0,
            Self::LegControl(token) => token.0,
        };
        if let Some(tracked_reset) = &state.tracked_reset {
            loop {
                match tracked_reset.deadline_state.load(Ordering::Acquire) {
                    RESET_DELIVERY_PENDING => {
                        if tracked_reset
                            .deadline_state
                            .compare_exchange(
                                RESET_DELIVERY_PENDING,
                                RESET_DELIVERY_DELIVERED,
                                Ordering::AcqRel,
                                Ordering::Acquire,
                            )
                            .is_ok()
                        {
                            break;
                        }
                    }
                    RESET_DELIVERY_REBINDING => std::hint::spin_loop(),
                    RESET_DELIVERY_DELIVERED | RESET_DELIVERY_DEADLINE_WON => break,
                    _ => break,
                }
            }
        }
        if !state.delivered.swap(true, Ordering::AcqRel)
            && let Some(client_recovery) = &state.client_recovery
        {
            let previous = client_recovery
                .undelivered_frames
                .fetch_sub(1, Ordering::AcqRel);
            debug_assert!(previous > 0);
        }
    }
}

/// Controller-visible byte transport. Raw byte submission exists only under
/// `cfg(test)` for deterministic legacy harnesses. Production callers can
/// invoke the trait only with an opaque [`EncodedLegSubmission`] minted by
/// [`LegIo`], so holding a sender is not direct-send authority.
pub(crate) trait EncodedLegTransport {
    fn submit_encoded(
        &mut self,
        submission: EncodedLegSubmission,
    ) -> Result<(), EncodedLegTransportError> {
        #[cfg(test)]
        {
            let (now, route, lane, bytes, ordered_completion) = submission.into_parts();
            match ordered_completion {
                Some(completion) => {
                    self.send_attach_ordered_encoded(now, route, lane, bytes, completion)
                }
                None => self.send_encoded(now, route, lane, bytes),
            }
        }
        #[cfg(not(test))]
        {
            let _ = submission;
            Err(EncodedLegTransportError::SubmissionCapabilityUnsupported)
        }
    }

    /// Compatibility surface for deterministic test transports only.
    #[cfg(test)]
    fn send_encoded(
        &mut self,
        now: SimTime,
        route: WireRoute,
        lane: WireLane,
        bytes: Bytes,
    ) -> Result<(), EncodedLegTransportError>;

    /// Ordered leg traffic is fail-closed unless the transport explicitly
    /// supplies actual delivery completion across Control/Data lanes.
    #[cfg(test)]
    fn send_attach_ordered_encoded(
        &mut self,
        _now: SimTime,
        _route: WireRoute,
        _lane: WireLane,
        _bytes: Bytes,
        _completion: OrderedDeliveryToken,
    ) -> Result<(), EncodedLegTransportError> {
        Err(EncodedLegTransportError::OrderedSubmissionUnsupported)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LegOutboundSessionPhase {
    AwaitingAttachAcceptance,
    AttachAcceptanceReserved,
    AttachRequestPending,
    AttachStatusPending,
    AttachRecovery,
    ClientReplacementRecovery,
    ClientRecoveryTerminal,
    OwnerRecoveryTerminal,
    Active,
}

/// Audited cold ownership snapshot for one exact physical FIFO. Terminal
/// cleanup may return it as discard evidence; actor join borrows it to fail
/// closed before dropping any queue ownership. The independently summed byte
/// count prevents a stale scalar from turning a non-empty queue into a false
/// zero witness.
#[must_use = "cold FIFO ownership inventory must be consumed by its audit path"]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct LegOutboundQueueColdInventory {
    phase: LegOutboundSessionPhase,
    queued_frames: usize,
    summed_frame_bytes: usize,
    stored_owned_bytes: usize,
    ordered_delivery_wait: bool,
}

impl LegOutboundQueueColdInventory {
    #[cfg(test)]
    pub(super) const fn phase_label_for_test(&self) -> &'static str {
        match self.phase {
            LegOutboundSessionPhase::AwaitingAttachAcceptance => "AwaitingAttachAcceptance",
            LegOutboundSessionPhase::AttachAcceptanceReserved => "AttachAcceptanceReserved",
            LegOutboundSessionPhase::AttachRequestPending => "AttachRequestPending",
            LegOutboundSessionPhase::AttachStatusPending => "AttachStatusPending",
            LegOutboundSessionPhase::AttachRecovery => "AttachRecovery",
            LegOutboundSessionPhase::ClientReplacementRecovery => "ClientReplacementRecovery",
            LegOutboundSessionPhase::ClientRecoveryTerminal => "ClientRecoveryTerminal",
            LegOutboundSessionPhase::OwnerRecoveryTerminal => "OwnerRecoveryTerminal",
            LegOutboundSessionPhase::Active => "Active",
        }
    }

    pub(super) const fn queued_frames(&self) -> usize {
        self.queued_frames
    }

    pub(super) const fn summed_frame_bytes(&self) -> usize {
        self.summed_frame_bytes
    }

    pub(super) const fn stored_owned_bytes(&self) -> usize {
        self.stored_owned_bytes
    }

    pub(super) const fn ordered_delivery_wait(&self) -> bool {
        self.ordered_delivery_wait
    }

    pub(super) const fn was_active(&self) -> bool {
        matches!(self.phase, LegOutboundSessionPhase::Active)
    }

    /// Exact precommit witness predicate for retiring one current physical
    /// owner. Both byte views remain explicit so a stale stored scalar can
    /// never turn a non-empty FIFO into an apparent zero.
    pub(super) const fn is_exact_active_zero(&self) -> bool {
        self.was_active()
            && self.queued_frames == 0
            && self.summed_frame_bytes == 0
            && self.stored_owned_bytes == 0
            && !self.ordered_delivery_wait
    }
}

/// Shutdown-only retirement result for the sole ordered-delivery wait.
///
/// This transition deliberately has no transport parameter, so it cannot
/// submit a queued frame or synthesize a peer-visible delivery. `Pending` and
/// `Residual` are borrow-only observations of the independently recomputed
/// cold inventory; only `RetiredDelivered` mutates the queue.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum LegOutboundShutdownWaitRetirement {
    Clean,
    RetiredDelivered,
    Pending,
    Residual {
        queued_frames: usize,
        queued_bytes: usize,
        ordered_delivery_wait: bool,
    },
}

/// Exclusive post-fatal borrow of the exact authenticated owner FIFO. The
/// immutable preflight validates actor-leg-terminal identity and snapshots the
/// queue by independently summing every frame. Only a successful terminal
/// bind consumes this guard and clears queue ownership.
pub(super) struct OwnerTerminalQueueCleanup<'a> {
    queue: &'a mut LegOutboundQueue,
    discarded: LegOutboundQueueColdInventory,
}

impl OwnerTerminalQueueCleanup<'_> {
    pub(super) fn commit(self) -> LegOutboundQueueColdInventory {
        self.queue.frames.clear();
        self.queue.owned_bytes = 0;
        self.queue.ordered_delivery_wait = None;
        self.queue.session_phase = LegOutboundSessionPhase::OwnerRecoveryTerminal;
        self.discarded
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum AttachRequestDeliveryProof {
    LocalDelivery,
    ExactAuthenticatedResponse,
}

enum QueuedProtocolFrame {
    Session(Frame),
    PreparedClientRecovery {
        frame: Frame,
        encoded: Bytes,
        delivery_state: Arc<OrderedDeliveryState>,
    },
    LegControl {
        frame: LegControlFrame,
        features: FeatureSet,
    },
}

struct QueuedOutboundFrame {
    frame: QueuedProtocolFrame,
    bytes: usize,
    ordered: bool,
    delivery_state: Option<Arc<OrderedDeliveryState>>,
    budget: OutboundBudget,
}

enum OutboundBudget {
    Ordinary,
    ClientRecoveryReserved(Arc<ClientRecoveryBudgetState>),
}

/// Bounded production-shared owner of encoded protocol frames awaiting one
/// leg submission. A failed transport call leaves the exact frame at the
/// front, so DATA and control effects cannot be consumed by transient
/// pressure before the transport accepts their bytes.
pub(crate) struct LegOutboundQueue {
    seal: Option<LegSeal>,
    endpoint: Option<Weak<LegTransportEndpoint>>,
    _lease: Option<Arc<LegOutboundQueueLease>>,
    identity: Arc<LegOutboundQueueIdentity>,
    next_enqueue_ordinal: u64,
    session_phase: LegOutboundSessionPhase,
    ordered_delivery_wait: Option<OrderedDeliveryWait>,
    max_frames: usize,
    max_bytes: usize,
    owned_bytes: usize,
    frames: VecDeque<QueuedOutboundFrame>,
}

/// Exclusive logical admission for one ordinary session frame. All fallible
/// validation, capacity math, endpoint checks, and queue storage allocation
/// happen before this value exists. Its mutable queue borrow prevents any
/// intervening FIFO mutation; consuming it is therefore an allocation-free,
/// non-rejecting move of the exact frame.
pub(super) struct PreparedOrdinaryFrameAdmission<'queue> {
    queue: &'queue mut LegOutboundQueue,
    frame: Frame,
    expected_phase: LegOutboundSessionPhase,
    ordinal: u64,
    next_ordinal: u64,
    bytes: usize,
    next_owned_bytes: usize,
    ordered: bool,
    reset_delivery: Option<OrderedResetDeliveryReceipt>,
    activate_on_commit: bool,
}

impl PreparedOrdinaryFrameAdmission<'_> {
    /// Moves the exact preflighted frame into the already-preallocated FIFO.
    /// The exclusive queue borrow makes every stored counter/phase witness
    /// stable between preparation and this commit.
    pub(super) fn commit(self) -> u64 {
        let Self {
            queue,
            frame,
            expected_phase,
            ordinal,
            next_ordinal,
            bytes,
            next_owned_bytes,
            ordered,
            reset_delivery,
            activate_on_commit,
        } = self;
        debug_assert_eq!(queue.session_phase, expected_phase);
        debug_assert_eq!(queue.next_enqueue_ordinal, ordinal);
        debug_assert_eq!(queue.owned_bytes.checked_add(bytes), Some(next_owned_bytes));
        debug_assert!(queue.frames.len() < queue.max_frames);
        debug_assert!(next_owned_bytes <= queue.max_bytes);
        debug_assert!(queue.frames.capacity().saturating_sub(queue.frames.len()) >= 1);
        queue.frames.push_back(QueuedOutboundFrame {
            frame: QueuedProtocolFrame::Session(frame),
            bytes,
            ordered,
            delivery_state: reset_delivery.map(OrderedResetDeliveryReceipt::into_state),
            budget: OutboundBudget::Ordinary,
        });
        queue.owned_bytes = next_owned_bytes;
        queue.next_enqueue_ordinal = next_ordinal;
        if activate_on_commit {
            queue.session_phase = LegOutboundSessionPhase::Active;
        }
        ordinal
    }

    /// Returns the exact still-uncommitted frame while leaving every queue
    /// counter, phase, and FIFO slot untouched.
    pub(super) fn into_frame(self) -> Frame {
        self.frame
    }
}

/// Controller-owned endpoint and its sole ordered FIFO. Fields deliberately
/// stay private so an installed client leg cannot be sent, received, or
/// flushed around [`super::client_controller::ClientSessionController`].
pub(crate) struct ClientLegTransport {
    io: LegIo,
    queue: LegOutboundQueue,
}

/// Fixed-size historical evidence produced when a normal replacement
/// explicitly supersedes the old current FIFO. The class sums are checked
/// against an independent cold byte inventory before the exact frames and
/// ordered wait are released.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct ClientOutboundQueueSupersessionAudit {
    queued_frames: usize,
    owned_bytes: usize,
    ordered_delivery_wait: bool,
    queued_delivery_states: usize,
    tracked_reset_deliveries: usize,
    session_frames: usize,
    session_bytes: usize,
    recovery_frames: usize,
    recovery_bytes: usize,
    leg_control_frames: usize,
    leg_control_bytes: usize,
}

impl ClientOutboundQueueSupersessionAudit {
    pub(super) const fn queued_frames(self) -> usize {
        self.queued_frames
    }

    pub(super) const fn owned_bytes(self) -> usize {
        self.owned_bytes
    }

    pub(super) const fn ordered_delivery_wait(self) -> bool {
        self.ordered_delivery_wait
    }

    pub(super) const fn tracked_reset_deliveries(self) -> usize {
        self.tracked_reset_deliveries
    }

    pub(super) const fn queued_delivery_states(self) -> usize {
        self.queued_delivery_states
    }
}

/// Non-cloneable precommit source. It owns the exact old endpoint/FIFO until
/// the accepted-B reducer transaction either returns it unchanged or invokes
/// the synchronous supersession commit.
pub(super) struct PreparedRetiredClientTransport {
    io: LegIo,
    queue: LegOutboundQueue,
    audit: ClientOutboundQueueSupersessionAudit,
}

impl PreparedRetiredClientTransport {
    pub(super) fn restore(self) -> ClientLegTransport {
        ClientLegTransport {
            io: self.io,
            queue: self.queue,
        }
    }

    pub(super) fn commit(
        mut self,
    ) -> (RetiredClientTransport, ClientOutboundQueueSupersessionAudit) {
        // Preparation owns the queue exclusively, so no frame/counter field
        // can change before this non-fallible commit. Ordered delivery's
        // atomic completion bit may change, but the wait capability remains
        // exactly one owned slot until it is retired here.
        self.queue.frames.clear();
        self.queue.owned_bytes = 0;
        self.queue.ordered_delivery_wait = None;
        let audit = self.audit;
        drop(self.queue);
        (RetiredClientTransport { io: self.io }, audit)
    }

    /// Rebinds each still-pending root RESET deadline authority held by A's
    /// unflushed FIFO to the exact same-flow RESET in B's precommit recovery.
    /// The old queue remains untouched, and every shape/count/state check
    /// completes before the recovery-local delivery states are mutated.
    pub(super) fn rebind_queued_reset_deliveries(
        &self,
        recovery: &mut PreparedClientRecovery,
    ) -> Result<(), ClientRecoveryResetRebindError> {
        self.queue
            .rebind_tracked_reset_deliveries(self.audit, recovery)
    }
}

/// Cold-path ownership snapshot for a controller-held current FIFO. Keeping
/// the queue phase in the same witness prevents a zero-frame recovery or
/// attach transaction from being mistaken for an empty active transport.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct ClientLegTransportResidual {
    active: bool,
    queued_frames: usize,
    owned_bytes: usize,
    ordered_delivery_wait: bool,
}

impl ClientLegTransportResidual {
    pub(super) const fn active(self) -> bool {
        self.active
    }

    pub(super) const fn queued_frames(self) -> usize {
        self.queued_frames
    }

    pub(super) const fn owned_bytes(self) -> usize {
        self.owned_bytes
    }

    pub(super) const fn ordered_delivery_wait(self) -> bool {
        self.ordered_delivery_wait
    }

    const fn is_pristine(self) -> bool {
        self.active
            && self.queued_frames == 0
            && self.owned_bytes == 0
            && !self.ordered_delivery_wait
    }
}

/// Retired transport keeps only the old endpoint's decode authority. Its
/// outbound queue is dropped atomically with replacement publication.
pub(super) struct RetiredClientTransport {
    io: LegIo,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ClientLegTransportBuildErrorKind {
    WrongSoleQueue,
}

pub(crate) struct ClientLegTransportBuildFailure {
    io: LegIo,
    queue: LegOutboundQueue,
    kind: ClientLegTransportBuildErrorKind,
}

impl ClientLegTransportBuildFailure {
    pub(crate) const fn kind(&self) -> ClientLegTransportBuildErrorKind {
        self.kind
    }

    pub(crate) fn into_parts(self) -> (LegIo, LegOutboundQueue) {
        (self.io, self.queue)
    }
}

impl fmt::Debug for ClientLegTransportBuildFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClientLegTransportBuildFailure")
            .field("kind", &self.kind)
            .field("transport", &"[REDACTED]")
            .finish()
    }
}

impl ClientLegTransport {
    /// Closes over caller-owned pieces only after proving the queue is the
    /// endpoint's already-minted sole FIFO. A mismatch returns both inputs.
    #[allow(
        clippy::result_large_err,
        reason = "exact construction failure returns the endpoint and non-cloneable sole queue"
    )]
    pub(super) fn from_parts(
        io: LegIo,
        mut queue: LegOutboundQueue,
        _gate: ClientControllerTransportGate,
    ) -> Result<Self, ClientLegTransportBuildFailure> {
        if !queue.binds_established_leg_identity(io.established_leg()) {
            return Err(ClientLegTransportBuildFailure {
                io,
                queue,
                kind: ClientLegTransportBuildErrorKind::WrongSoleQueue,
            });
        }
        Ok(Self { io, queue })
    }

    #[allow(
        clippy::result_large_err,
        reason = "activation failure returns the exact accepted attach and sole queue ownership without boxing"
    )]
    pub(super) fn activate_initial_client(
        &mut self,
        accepted: AcceptedInitialAttach,
        _gate: &ClientControllerBootstrapGate,
    ) -> Result<AttachedLeg, AcceptedInitialAttachActivationFailure> {
        accepted.activate_on_queue(&mut self.queue)
    }

    #[allow(
        clippy::result_large_err,
        reason = "queue admission failure returns the exact outbound frame without boxing"
    )]
    pub(super) fn push_frame(&mut self, frame: Frame) -> Result<(), LegOutboundQueueError> {
        self.queue.push(frame)
    }

    pub(super) fn push_frame_with_reset_delivery(
        &mut self,
        frame: Frame,
        reset_delivery: OrderedResetDeliveryReceipt,
    ) -> Result<(), LegOutboundQueueError> {
        match self
            .queue
            .prepare_ordinary_frame_with_reset_delivery(frame, Some(reset_delivery))
        {
            Ok(reservation) => {
                reservation.commit();
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    #[allow(
        clippy::result_large_err,
        reason = "ordinary-frame rejection returns the exact unconsumed frame without boxing"
    )]
    pub(super) fn prepare_ordinary_frame(
        &mut self,
        frame: Frame,
    ) -> Result<PreparedOrdinaryFrameAdmission<'_>, LegOutboundQueueError> {
        self.queue.prepare_ordinary_frame(frame)
    }

    /// Admits one ordered control record through the exact policy-side FIFO
    /// retained by the composition root. The gate prevents an arbitrary
    /// sibling from turning the controller bundle into a second actor port.
    #[allow(
        clippy::result_large_err,
        reason = "the error must return the exact unconsumed control frame"
    )]
    pub(super) fn push_ordered_leg_control(
        &mut self,
        frame: LegControlFrame,
        features: FeatureSet,
        _gate: &QuinnLegCompositionGate,
    ) -> Result<LegControlEnqueued, LegControlQueueError> {
        let seal = self.io.established_leg().standby_seal();
        self.queue.push_leg_control(&seal, frame, features)
    }

    pub(super) fn reserve_registered_client_recovery(
        &mut self,
        request: &CausallyDeliveredAttachRequest,
        recovery: &PreparedClientRecovery,
    ) -> Result<ClientRecoveryReservation, ClientRecoveryReserveError> {
        self.queue
            .reserve_registered_client_recovery(&mut self.io, request, recovery)
    }

    pub(super) fn reserve_catch_up_client_recovery(
        &mut self,
        request: &CatchUpAttachEnqueued,
        recovery: &PreparedClientRecovery,
    ) -> Result<ClientRecoveryReservation, ClientRecoveryReserveError> {
        self.queue
            .reserve_catch_up_client_recovery(&mut self.io, request, recovery)
    }

    pub(super) fn release_client_recovery(
        &mut self,
        reservation: ClientRecoveryReservation,
    ) -> Result<(), ClientRecoveryReservation> {
        self.queue
            .release_client_recovery(&mut self.io, reservation)
    }

    #[allow(
        clippy::result_large_err,
        reason = "recovery admission failure returns the exact reservation and prepared frame without boxing"
    )]
    pub(super) fn push_client_recovery_frame(
        &mut self,
        reservation: InstalledClientRecoveryReservation,
        prepared: PreparedClientRecoveryFrame,
    ) -> Result<InstalledClientRecoveryReservation, ClientRecoveryEnqueueFailure> {
        self.queue.push_client_recovery_frame(reservation, prepared)
    }

    pub(super) fn finish_client_recovery_admission(
        &mut self,
        reservation: InstalledClientRecoveryReservation,
    ) -> Result<ClientRecoveryTail, ClientRecoveryFinishFailure> {
        self.queue.finish_client_recovery_admission(reservation)
    }

    /// Controller-only terminal cleanup for an installed recovery. The raw
    /// endpoint and sole queue never escape their closed transport bundle.
    pub(super) fn discard_installed_client_recovery_after_terminal(
        &mut self,
        reservation: InstalledClientRecoveryReservation,
        terminal: &ClientRecoveryTerminalGate,
    ) -> Result<(), InstalledClientRecoveryReservation> {
        self.queue.discard_installed_client_recovery_after_terminal(
            &mut self.io,
            reservation,
            terminal,
        )
    }

    /// Controller-only terminal cleanup for an admitted recovery tail. The
    /// raw endpoint and sole queue remain inside the closed transport bundle.
    pub(super) fn discard_client_recovery_tail_after_terminal(
        &mut self,
        tail: ClientRecoveryTail,
        terminal: &ClientRecoveryTerminalGate,
    ) -> Result<(), ClientRecoveryTail> {
        self.queue
            .discard_client_recovery_tail_after_terminal(&mut self.io, tail, terminal)
    }

    /// Consumes one physically delivered recovery tail only through the exact
    /// endpoint/FIFO bundle retained by this controller transport. The caller
    /// must separately prove that this bundle is still installed in the
    /// current physical actor before reaching this seam.
    pub(super) fn settle_delivered_client_recovery_tail(
        &mut self,
        tail: ClientRecoveryTail,
        expected_generation: LegGeneration,
    ) -> Result<ClientRecoveryTailSettled, ClientRecoveryTailSettlementFailure> {
        self.queue
            .settle_delivered_client_recovery_tail(&mut self.io, tail, expected_generation)
    }

    pub(super) fn try_flush<T: EncodedLegTransport + ?Sized>(
        &mut self,
        transport: &mut T,
        now: SimTime,
    ) -> Result<ClientLegFlushProgress, LegIoError> {
        let ordered_delivery_completed = self
            .queue
            .ordered_delivery_wait
            .as_ref()
            .is_some_and(OrderedDeliveryWait::is_delivered);
        let submission = self.queue.try_flush(&mut self.io, transport, now)?;
        Ok(ClientLegFlushProgress {
            submission,
            ordered_delivery_completed,
        })
    }

    pub(super) fn receive_classified_delivery(
        &mut self,
        delivery: EncodedDelivery,
        installed_features: FeatureSet,
    ) -> Result<BoundInbound, LegIoError> {
        self.io
            .receive_classified_delivery(delivery, installed_features)
    }

    pub(super) const fn outbound_route(&self) -> WireRoute {
        self.io.outbound_route()
    }

    pub(super) const fn inbound_route(&self) -> WireRoute {
        self.io.inbound_route()
    }

    pub(super) const fn recovery_budget_snapshot(&self) -> ClientRecoveryBudgetSnapshot {
        self.io.client_recovery_budget_snapshot()
    }

    /// Read-only production witness for transferring this controller-owned
    /// transport into the first root slice. A frame already admitted to the
    /// FIFO, or an ordered submission whose delivery wait has not yet been
    /// retired, is live ownership and therefore not a quiescent start state.
    pub(super) fn root_start_is_pristine(&self) -> bool {
        self.queue.transport_residual().is_none()
    }

    /// Returns the exact controller-side ownership that actor join counters
    /// cannot observe. The transport and FIFO remain retained by the caller;
    /// this copyable value is only a shutdown rejection witness.
    pub(super) fn shutdown_residual(&self) -> Option<ClientLegTransportResidual> {
        self.queue.transport_residual()
    }

    #[cfg(test)]
    pub(super) const fn counters(&self) -> LegIoCounters {
        self.io.counters()
    }

    #[cfg(test)]
    pub(super) fn queue_is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    #[cfg(test)]
    pub(super) fn queued_frames(&self) -> usize {
        self.queue.len()
    }

    #[cfg(test)]
    pub(super) const fn queued_bytes(&self) -> usize {
        self.queue.owned_bytes()
    }

    /// Deterministically exposes the production-valid instant after the
    /// root RESET delivery decision linearizes but before the delivery child
    /// publishes completion to this physical attempt's FIFO waiter.
    #[cfg(test)]
    pub(super) fn force_submitted_reset_root_delivered_before_attempt_for_test(&self) -> bool {
        self.queue
            .force_submitted_reset_root_delivered_before_attempt_for_test()
    }

    pub(super) fn recovery_tail_belongs_here(&self, tail: &ClientRecoveryTail) -> bool {
        tail.belongs_to_queue(&self.queue)
    }

    #[cfg(test)]
    pub(super) fn recovery_tail_inventory(
        &self,
        tail: &ClientRecoveryTail,
    ) -> ClientRecoveryTailInventory {
        ClientRecoveryTailInventory {
            live_tails: 1,
            undelivered_frames: tail
                .delivery_state
                .undelivered_frames
                .load(Ordering::Acquire),
            matching_queue: tail.belongs_to_queue(&self.queue),
            matching_endpoint: tail.endpoint_matches_queue(&self.queue),
            matching_generation: Some(tail.generation),
            endpoint_open: tail.endpoint_is_open(),
            matching_ordered_waits: usize::from(
                self.queue
                    .ordered_delivery_wait
                    .as_ref()
                    .is_some_and(|wait| wait.belongs_to_client_recovery(&tail.delivery_state)),
            ),
            reservation_messages: tail
                .io_reservation
                .state
                .remaining_messages
                .load(Ordering::Acquire),
            reservation_bytes: tail
                .io_reservation
                .state
                .remaining_bytes
                .load(Ordering::Acquire),
        }
    }

    pub(super) fn installed_recovery_belongs_here(
        &self,
        reservation: &InstalledClientRecoveryReservation,
    ) -> bool {
        reservation.belongs_to_queue(&self.queue)
    }

    pub(super) fn into_retired(self) -> RetiredClientTransport {
        let Self { io, queue } = self;
        drop(queue);
        RetiredClientTransport { io }
    }

    /// Moves the whole current transport into a restorable supersession
    /// source only after independently checking every FIFO class and byte.
    pub(super) fn prepare_normal_supersession(
        self,
    ) -> Result<PreparedRetiredClientTransport, Self> {
        let Self { io, queue } = self;
        let Some(audit) = queue.normal_supersession_audit() else {
            return Err(Self { io, queue });
        };
        Ok(PreparedRetiredClientTransport { io, queue, audit })
    }

    pub(super) fn belongs_to_physical_actor(
        &self,
        runtime: &PhysicalLegRuntimeIncarnation,
        gate: &QuinnLegCompositionGate,
    ) -> bool {
        self.io.belongs_to_physical_actor(runtime, gate)
    }

    pub(super) fn into_parts(
        self,
        _gate: ClientControllerTransportGate,
    ) -> (LegIo, LegOutboundQueue) {
        (self.io, self.queue)
    }
}

impl RetiredClientTransport {
    pub(super) fn receive_classified_delivery(
        &mut self,
        delivery: EncodedDelivery,
        installed_features: FeatureSet,
    ) -> Result<BoundInbound, LegIoError> {
        self.io
            .receive_classified_delivery(delivery, installed_features)
    }

    pub(super) const fn inbound_route(&self) -> WireRoute {
        self.io.inbound_route()
    }

    pub(super) fn belongs_to_physical_actor(
        &self,
        runtime: &PhysicalLegRuntimeIncarnation,
        gate: &QuinnLegCompositionGate,
    ) -> bool {
        self.io.belongs_to_physical_actor(runtime, gate)
    }
}

impl fmt::Debug for ClientLegTransport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ClientLegTransport([REDACTED])")
    }
}

impl fmt::Debug for RetiredClientTransport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RetiredClientTransport([REDACTED])")
    }
}

struct LegOutboundQueueIdentity;

impl PhysicalLegActorBinding {
    pub(super) fn try_bind(
        runtime: &PhysicalLegRuntimeIncarnation,
        io: &LegIo,
        queue: &LegOutboundQueue,
    ) -> Option<Self> {
        let policy = io.physical_incarnation.as_ref()?;
        if !runtime.matches_policy(policy)
            || !queue.belongs_to_established_leg(io.established_leg())
        {
            return None;
        }
        Some(Self {
            actor: Arc::downgrade(&runtime.0),
            queue: Arc::downgrade(&queue.identity),
        })
    }

    pub(super) fn matches(
        &self,
        runtime: &PhysicalLegRuntimeIncarnation,
        io: &LegIo,
        queue: &LegOutboundQueue,
    ) -> bool {
        self.actor
            .upgrade()
            .is_some_and(|actor| Arc::ptr_eq(&actor, &runtime.0))
            && io
                .physical_incarnation
                .as_ref()
                .is_some_and(|policy| runtime.matches_policy(policy))
            && self.belongs_to_queue(queue)
            && queue.belongs_to_established_leg(io.established_leg())
    }

    pub(super) fn belongs_to_queue(&self, queue: &LegOutboundQueue) -> bool {
        self.queue
            .upgrade()
            .is_some_and(|identity| Arc::ptr_eq(&identity, &queue.identity))
    }

    pub(super) fn actor_is_live(&self) -> bool {
        self.actor.upgrade().is_some()
    }

    /// Joined-actor counterpart to [`Self::matches`].  Structural join has
    /// already destroyed the sole FIFO and policy half, so the retained
    /// binding must name the exact runtime while its queue identity is no
    /// longer live.  This prevents a runtime-only scalar match from settling
    /// a still-running or foreign physical owner.
    pub(super) fn matches_joined_actor(&self, runtime: &PhysicalLegRuntimeIncarnation) -> bool {
        let exact_actor = self
            .actor
            .upgrade()
            .is_some_and(|actor| Arc::ptr_eq(&actor, &runtime.0));
        exact_actor && Arc::strong_count(&runtime.0) == 1 && self.queue.upgrade().is_none()
    }
}

impl LegOutboundQueue {
    /// Independently inventories every cold FIFO ownership source without
    /// changing queue state. A stale byte scalar, overflow, or capacity split
    /// fails closed instead of allowing shutdown to mint a false zero witness.
    pub(super) fn cold_inventory(&self) -> Option<LegOutboundQueueColdInventory> {
        let summed_frame_bytes = self
            .frames
            .iter()
            .try_fold(0_usize, |sum, frame| sum.checked_add(frame.bytes))?;
        if summed_frame_bytes != self.owned_bytes
            || self.frames.len() > self.max_frames
            || self.owned_bytes > self.max_bytes
        {
            return None;
        }
        Some(LegOutboundQueueColdInventory {
            phase: self.session_phase,
            queued_frames: self.frames.len(),
            summed_frame_bytes,
            stored_owned_bytes: self.owned_bytes,
            ordered_delivery_wait: self.ordered_delivery_wait.is_some(),
        })
    }

    fn normal_supersession_audit(&self) -> Option<ClientOutboundQueueSupersessionAudit> {
        let inventory = self.cold_inventory()?;
        if !inventory.was_active() {
            return None;
        }
        let mut session_frames = 0usize;
        let mut session_bytes = 0usize;
        let mut recovery_frames = 0usize;
        let mut recovery_bytes = 0usize;
        let mut leg_control_frames = 0usize;
        let mut leg_control_bytes = 0usize;
        let mut queued_delivery_states = 0usize;
        let mut tracked_reset_deliveries = 0usize;
        for queued in &self.frames {
            match &queued.frame {
                QueuedProtocolFrame::Session(_) => {
                    session_frames = session_frames.checked_add(1)?;
                    session_bytes = session_bytes.checked_add(queued.bytes)?;
                }
                QueuedProtocolFrame::PreparedClientRecovery { .. } => {
                    recovery_frames = recovery_frames.checked_add(1)?;
                    recovery_bytes = recovery_bytes.checked_add(queued.bytes)?;
                }
                QueuedProtocolFrame::LegControl { .. } => {
                    leg_control_frames = leg_control_frames.checked_add(1)?;
                    leg_control_bytes = leg_control_bytes.checked_add(queued.bytes)?;
                }
            }
            let delivery_state = match (&queued.frame, queued.delivery_state.as_ref()) {
                (QueuedProtocolFrame::PreparedClientRecovery { delivery_state, .. }, None) => {
                    Some(delivery_state)
                }
                (QueuedProtocolFrame::PreparedClientRecovery { .. }, Some(_)) => return None,
                (_, delivery_state) => delivery_state,
            };
            if let Some(delivery_state) = delivery_state {
                queued_delivery_states = queued_delivery_states.checked_add(1)?;
                if let Some(tracked_reset) = &delivery_state.tracked_reset {
                    let frame = match &queued.frame {
                        QueuedProtocolFrame::Session(frame)
                        | QueuedProtocolFrame::PreparedClientRecovery { frame, .. } => frame,
                        QueuedProtocolFrame::LegControl { .. } => return None,
                    };
                    if reset_flow_id(frame) != Some(tracked_reset.flow_id) {
                        return None;
                    }
                    tracked_reset_deliveries = tracked_reset_deliveries.checked_add(1)?;
                }
            }
        }
        if let Some(delivery_state) = self.ordered_delivery_wait.as_ref().map(|wait| &wait.0)
            && delivery_state.tracked_reset.is_some()
        {
            tracked_reset_deliveries = tracked_reset_deliveries.checked_add(1)?;
        }
        let classified_frames = session_frames
            .checked_add(recovery_frames)?
            .checked_add(leg_control_frames)?;
        let classified_bytes = session_bytes
            .checked_add(recovery_bytes)?
            .checked_add(leg_control_bytes)?;
        let delivery_authority_capacity =
            queued_delivery_states.checked_add(usize::from(inventory.ordered_delivery_wait()))?;
        if classified_frames != inventory.queued_frames()
            || classified_bytes != inventory.summed_frame_bytes()
            || inventory.summed_frame_bytes() != inventory.stored_owned_bytes()
            || queued_delivery_states > inventory.queued_frames()
            || tracked_reset_deliveries > delivery_authority_capacity
        {
            return None;
        }
        Some(ClientOutboundQueueSupersessionAudit {
            queued_frames: inventory.queued_frames(),
            owned_bytes: inventory.summed_frame_bytes(),
            ordered_delivery_wait: inventory.ordered_delivery_wait(),
            queued_delivery_states,
            tracked_reset_deliveries,
            session_frames,
            session_bytes,
            recovery_frames,
            recovery_bytes,
            leg_control_frames,
            leg_control_bytes,
        })
    }

    fn visit_tracked_reset_deliveries(
        &self,
        mut visit: impl FnMut(
            SessionFlowId,
            &Arc<OrderedResetDeliveryState>,
            &Arc<OrderedDeliveryState>,
            bool,
        ) -> Result<(), ClientRecoveryResetRebindError>,
    ) -> Result<usize, ClientRecoveryResetRebindError> {
        let mut count = 0usize;
        for queued in &self.frames {
            let delivery_state = match (&queued.frame, queued.delivery_state.as_ref()) {
                (QueuedProtocolFrame::PreparedClientRecovery { delivery_state, .. }, None) => {
                    Some(delivery_state)
                }
                (QueuedProtocolFrame::PreparedClientRecovery { .. }, Some(_)) => {
                    return Err(ClientRecoveryResetRebindError::SupersededDeliveryShape);
                }
                (_, delivery_state) => delivery_state,
            };
            let Some(delivery_state) = delivery_state else {
                continue;
            };
            let Some(tracked_reset) = &delivery_state.tracked_reset else {
                continue;
            };
            let frame = match &queued.frame {
                QueuedProtocolFrame::Session(frame)
                | QueuedProtocolFrame::PreparedClientRecovery { frame, .. } => frame,
                QueuedProtocolFrame::LegControl { .. } => {
                    return Err(ClientRecoveryResetRebindError::SupersededDeliveryShape);
                }
            };
            if reset_flow_id(frame) != Some(tracked_reset.flow_id) {
                return Err(ClientRecoveryResetRebindError::SupersededFlowMismatch {
                    tracked: tracked_reset.flow_id,
                    frame: reset_flow_id(frame),
                });
            }
            count = count
                .checked_add(1)
                .ok_or(ClientRecoveryResetRebindError::CounterOverflow)?;
            visit(tracked_reset.flow_id, tracked_reset, delivery_state, false)?;
        }
        if let Some(delivery_state) = self.ordered_delivery_wait.as_ref().map(|wait| &wait.0)
            && let Some(tracked_reset) = &delivery_state.tracked_reset
        {
            count = count
                .checked_add(1)
                .ok_or(ClientRecoveryResetRebindError::CounterOverflow)?;
            visit(tracked_reset.flow_id, tracked_reset, delivery_state, true)?;
        }
        Ok(count)
    }

    fn rebind_tracked_reset_deliveries(
        &self,
        audit: ClientOutboundQueueSupersessionAudit,
        recovery: &mut PreparedClientRecovery,
    ) -> Result<(), ClientRecoveryResetRebindError> {
        let actual = self.visit_tracked_reset_deliveries(
            |flow_id, tracked_reset, delivery_state, submitted_wait| {
                // The delivery child publishes root Delivered before its
                // per-attempt FIFO bit. Read in the reverse order so the only
                // cross-word lag this snapshot can observe is the valid
                // `(Delivered, false)` state; `(Pending, true)` cannot be
                // synthesized by a delivery completing between the loads.
                let locally_delivered = delivery_state.delivered.load(Ordering::Acquire);
                #[cfg(test)]
                wait_on_client_reset_rebind_snapshot_gate(delivery_state);
                let deadline_state = tracked_reset.deadline_state.load(Ordering::Acquire);
                match (submitted_wait, deadline_state, locally_delivered) {
                    (false, RESET_DELIVERY_PENDING, false)
                    | (true, RESET_DELIVERY_PENDING, false)
                    // Root delivery is the linearization point that defeats
                    // the timer. The independent delivery child publishes the
                    // per-attempt FIFO completion immediately afterwards, so
                    // replacement may legitimately observe this one-way lag.
                    // The second pass sees Delivered and skips rebinding; A's
                    // retained actor still owns and drains its exact wait.
                    | (true, RESET_DELIVERY_DELIVERED, false)
                    | (true, RESET_DELIVERY_DELIVERED, true) => {}
                    (_, RESET_DELIVERY_DEADLINE_WON, _) => {
                        return Err(ClientRecoveryResetRebindError::DeadlineWon { flow_id });
                    }
                    (_, RESET_DELIVERY_REBINDING, _) => {
                        return Err(ClientRecoveryResetRebindError::RebindBusy { flow_id });
                    }
                    _ => {
                        return Err(ClientRecoveryResetRebindError::DeliveryStateMismatch {
                            flow_id,
                        });
                    }
                }

                let mut old_flow_matches = 0usize;
                let mut old_authority_matches = 0usize;
                self.visit_tracked_reset_deliveries(|candidate_flow, candidate, _, _| {
                    if candidate_flow == flow_id {
                        old_flow_matches = old_flow_matches
                            .checked_add(1)
                            .ok_or(ClientRecoveryResetRebindError::CounterOverflow)?;
                    }
                    if Arc::ptr_eq(candidate, tracked_reset) {
                        old_authority_matches = old_authority_matches
                            .checked_add(1)
                            .ok_or(ClientRecoveryResetRebindError::CounterOverflow)?;
                    }
                    Ok(())
                })?;
                if old_flow_matches != 1 {
                    return Err(ClientRecoveryResetRebindError::DuplicateSupersededFlow {
                        flow_id,
                    });
                }
                if old_authority_matches != 1 {
                    return Err(
                        ClientRecoveryResetRebindError::DuplicateSupersededAuthority { flow_id },
                    );
                }

                let mut recovery_matches = 0usize;
                for effect in &recovery.effects {
                    let PreparedClientRecoveryEffect::Transmit(frame) = effect else {
                        continue;
                    };
                    if reset_flow_id(&frame.frame) == Some(flow_id) {
                        recovery_matches = recovery_matches
                            .checked_add(1)
                            .ok_or(ClientRecoveryResetRebindError::CounterOverflow)?;
                        if frame.delivery_state.tracked_reset.is_some() {
                            return Err(
                                ClientRecoveryResetRebindError::RecoveryDeliveryAlreadyTracked {
                                    flow_id,
                                },
                            );
                        }
                        if Arc::strong_count(&frame.delivery_state) != 1 {
                            return Err(ClientRecoveryResetRebindError::RecoveryDeliveryAliased {
                                flow_id,
                            });
                        }
                    }
                }
                match recovery_matches {
                    1 => Ok(()),
                    0 => Err(ClientRecoveryResetRebindError::MissingRecoveryReset { flow_id }),
                    _ => Err(ClientRecoveryResetRebindError::DuplicateRecoveryReset { flow_id }),
                }
            },
        )?;
        if actual != audit.tracked_reset_deliveries() {
            return Err(ClientRecoveryResetRebindError::AuditMismatch {
                expected: audit.tracked_reset_deliveries(),
                actual,
            });
        }

        let mut rebound = 0usize;
        self.visit_tracked_reset_deliveries(|flow_id, tracked_reset, _, _| {
            loop {
                match tracked_reset.deadline_state.compare_exchange(
                    RESET_DELIVERY_PENDING,
                    RESET_DELIVERY_REBINDING,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => break,
                    Err(RESET_DELIVERY_DELIVERED) => return Ok(()),
                    Err(RESET_DELIVERY_DEADLINE_WON) => {
                        return Err(ClientRecoveryResetRebindError::DeadlineWon { flow_id });
                    }
                    Err(RESET_DELIVERY_REBINDING) => std::hint::spin_loop(),
                    Err(_) => {
                        return Err(ClientRecoveryResetRebindError::DeliveryStateMismatch {
                            flow_id,
                        });
                    }
                }
            }

            let target = recovery.effects.iter_mut().find_map(|effect| {
                let PreparedClientRecoveryEffect::Transmit(frame) = effect else {
                    return None;
                };
                (reset_flow_id(&frame.frame) == Some(flow_id)).then_some(frame)
            });
            let Some(target) = target else {
                tracked_reset
                    .deadline_state
                    .store(RESET_DELIVERY_PENDING, Ordering::Release);
                return Err(ClientRecoveryResetRebindError::MissingRecoveryReset { flow_id });
            };
            let Some(delivery_state) = Arc::get_mut(&mut target.delivery_state) else {
                tracked_reset
                    .deadline_state
                    .store(RESET_DELIVERY_PENDING, Ordering::Release);
                return Err(ClientRecoveryResetRebindError::RecoveryDeliveryAliased { flow_id });
            };
            delivery_state.tracked_reset = Some(Arc::clone(tracked_reset));
            tracked_reset
                .deadline_state
                .store(RESET_DELIVERY_PENDING, Ordering::Release);
            rebound = rebound
                .checked_add(1)
                .ok_or(ClientRecoveryResetRebindError::CounterOverflow)?;
            Ok(())
        })?;
        recovery.rebound_reset_deliveries = rebound;
        Ok(())
    }

    #[cfg(test)]
    fn force_submitted_reset_root_delivered_before_attempt_for_test(&self) -> bool {
        let Some(delivery_state) = self.ordered_delivery_wait.as_ref().map(|wait| &wait.0) else {
            return false;
        };
        let Some(tracked_reset) = delivery_state.tracked_reset.as_ref() else {
            return false;
        };
        !delivery_state.delivered.load(Ordering::Acquire)
            && tracked_reset
                .deadline_state
                .compare_exchange(
                    RESET_DELIVERY_PENDING,
                    RESET_DELIVERY_DELIVERED,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
    }

    /// Retires only an already-delivered ordered wait after the composition
    /// has published its lifecycle cutoff. The exact leg/queue identity and
    /// independently summed cold inventory are checked before any mutation.
    ///
    /// Unlike `try_flush`, this function cannot reach a transport and can
    /// never submit, acknowledge, or discard a queued frame. A pending wait or
    /// any residual frame leaves every queue field unchanged for a later
    /// shutdown retry.
    pub(super) fn try_retire_delivered_wait_for_shutdown(
        &mut self,
        leg: &EstablishedLeg,
    ) -> Option<LegOutboundShutdownWaitRetirement> {
        if !self.belongs_to_established_leg(leg) {
            return None;
        }
        let inventory = self.cold_inventory()?;
        if inventory.queued_frames() != 0 || inventory.summed_frame_bytes() != 0 {
            return Some(LegOutboundShutdownWaitRetirement::Residual {
                queued_frames: inventory.queued_frames(),
                queued_bytes: inventory.summed_frame_bytes(),
                ordered_delivery_wait: inventory.ordered_delivery_wait(),
            });
        }
        let Some(wait) = self.ordered_delivery_wait.as_ref() else {
            return Some(LegOutboundShutdownWaitRetirement::Clean);
        };
        if !wait.is_delivered() {
            return Some(LegOutboundShutdownWaitRetirement::Pending);
        }
        drop(self.ordered_delivery_wait.take());
        Some(LegOutboundShutdownWaitRetirement::RetiredDelivered)
    }

    /// Starts an exact owner terminal cleanup without changing queue state.
    /// The returned exclusive guard makes commit infallible and prevents a
    /// different queue from being substituted between preflight and bind.
    pub(super) fn begin_owner_terminal_cleanup(
        &mut self,
        leg: &EstablishedLeg,
        terminal: &ExactLegTerminal,
    ) -> Option<OwnerTerminalQueueCleanup<'_>> {
        let seal = self.seal.as_ref()?;
        let endpoint = self.endpoint.as_ref()?;
        let leg_endpoint = leg.endpoint_liveness();
        if !leg.belongs_to_transport(seal)
            || !Weak::ptr_eq(endpoint, &leg_endpoint)
            || !terminal.belongs_to_transport(seal, endpoint)
        {
            return None;
        }
        let discarded = self.cold_inventory()?;
        Some(OwnerTerminalQueueCleanup {
            queue: self,
            discarded,
        })
    }

    /// Mints one queue bound to the exact authenticated transport leg.
    pub(crate) fn for_leg(
        leg: &EstablishedLeg,
        max_frames: usize,
        max_bytes: usize,
    ) -> Result<Self, LegIoConfigError> {
        let mut queue = Self::with_seal(None, None, max_frames, max_bytes)?;
        let Some((seal, endpoint, lease)) = leg.try_claim_outbound_queue() else {
            return Err(LegIoConfigError::OutboundQueueAlreadyMinted);
        };
        queue.seal = Some(seal);
        queue.endpoint = Some(endpoint);
        queue._lease = Some(lease);
        Ok(queue)
    }

    /// Closed-composition identity check for the exact policy endpoint that
    /// owns this sole FIFO. A route or generation match is insufficient:
    /// both the process-local seal and endpoint allocation must agree.
    pub(super) fn belongs_to_established_leg(&self, leg: &EstablishedLeg) -> bool {
        let Some(seal) = self.seal.as_ref() else {
            return false;
        };
        let Some(endpoint) = self.endpoint.as_ref() else {
            return false;
        };
        let expected_endpoint = leg.endpoint_liveness();
        leg.belongs_to_transport(seal)
            && Weak::ptr_eq(endpoint, &expected_endpoint)
            && endpoint.upgrade().is_some()
    }

    /// Legacy deterministic harnesses predate exact-leg queues. This
    /// constructor is test-only and binds once on first acceptance or flush;
    /// production code has no unsealed queue constructor.
    #[cfg(test)]
    pub(crate) fn new(max_frames: usize, max_bytes: usize) -> Result<Self, LegIoConfigError> {
        Self::with_seal(None, None, max_frames, max_bytes)
    }

    fn with_seal(
        seal: Option<LegSeal>,
        endpoint: Option<Weak<LegTransportEndpoint>>,
        max_frames: usize,
        max_bytes: usize,
    ) -> Result<Self, LegIoConfigError> {
        if max_frames == 0 {
            return Err(LegIoConfigError::ZeroOutboundQueueFrames);
        }
        if max_bytes == 0 {
            return Err(LegIoConfigError::ZeroOutboundQueueBytes);
        }
        let mut frames = VecDeque::new();
        frames
            .try_reserve_exact(max_frames)
            .map_err(|_| LegIoConfigError::OutboundQueueStorageUnavailable)?;
        Ok(Self {
            seal,
            endpoint,
            _lease: None,
            identity: Arc::new(LegOutboundQueueIdentity),
            next_enqueue_ordinal: 1,
            session_phase: LegOutboundSessionPhase::AwaitingAttachAcceptance,
            ordered_delivery_wait: None,
            max_frames,
            max_bytes,
            owned_bytes: 0,
            frames,
        })
    }

    #[allow(
        clippy::result_large_err,
        reason = "the error must return the exact unconsumed frame without a hot-path allocation"
    )]
    pub(crate) fn push(&mut self, frame: Frame) -> Result<(), LegOutboundQueueError> {
        match self.prepare_ordinary_frame(frame) {
            Ok(reservation) => {
                reservation.commit();
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    /// Performs every fallible ordinary-frame queue check while the producer
    /// still owns its reducer transaction. The returned reservation borrows
    /// this queue exclusively through its later allocation-free commit.
    #[allow(
        clippy::result_large_err,
        reason = "ordinary-frame rejection returns the exact unconsumed frame without boxing"
    )]
    pub(super) fn prepare_ordinary_frame(
        &mut self,
        frame: Frame,
    ) -> Result<PreparedOrdinaryFrameAdmission<'_>, LegOutboundQueueError> {
        self.prepare_ordinary_frame_with_reset_delivery(frame, None)
    }

    pub(super) fn prepare_ordinary_frame_with_reset_delivery(
        &mut self,
        frame: Frame,
        reset_delivery: Option<OrderedResetDeliveryReceipt>,
    ) -> Result<PreparedOrdinaryFrameAdmission<'_>, LegOutboundQueueError> {
        if reset_delivery.as_ref().is_some_and(|receipt| {
            reset_flow_id(&frame) != Some(receipt.flow_id())
                || !receipt.is_pending_for(receipt.flow_id())
        }) {
            return Err(LegOutboundQueueError::new(
                frame,
                LegOutboundQueueErrorKind::ResetDeliveryReceiptMismatch,
            ));
        }
        let restricted = match frame.record() {
            Record::Attach { .. } => {
                Some(LegOutboundQueueErrorKind::RegisteredAttachAdmissionRequired)
            }
            Record::AttachAccepted { .. } => {
                Some(LegOutboundQueueErrorKind::AttachAcceptanceAdmissionRequired)
            }
            Record::AttachGenerationStatus { .. } => {
                Some(LegOutboundQueueErrorKind::AuthenticatedAttachStatusAdmissionRequired)
            }
            _ => None,
        };
        if let Some(kind) = restricted {
            return Err(LegOutboundQueueError::new(frame, kind));
        }
        let blocked_kind = match self.session_phase {
            LegOutboundSessionPhase::AttachAcceptanceReserved => {
                Some(LegOutboundQueueErrorKind::AttachAcceptanceReserved)
            }
            LegOutboundSessionPhase::AttachRequestPending => {
                Some(LegOutboundQueueErrorKind::AttachRequestPending)
            }
            LegOutboundSessionPhase::AttachStatusPending => {
                Some(LegOutboundQueueErrorKind::AttachStatusPending)
            }
            LegOutboundSessionPhase::ClientReplacementRecovery => {
                Some(LegOutboundQueueErrorKind::ClientReplacementRecoveryPending)
            }
            LegOutboundSessionPhase::ClientRecoveryTerminal => {
                Some(LegOutboundQueueErrorKind::ClientRecoveryTerminal)
            }
            LegOutboundSessionPhase::OwnerRecoveryTerminal => {
                Some(LegOutboundQueueErrorKind::OwnerRecoveryTerminal)
            }
            LegOutboundSessionPhase::AwaitingAttachAcceptance
            | LegOutboundSessionPhase::AttachRecovery
            | LegOutboundSessionPhase::Active => None,
        };
        if let Some(kind) = blocked_kind {
            return Err(LegOutboundQueueError::new(frame, kind));
        }
        if self.endpoint_is_lost() {
            return Err(LegOutboundQueueError::new(
                frame,
                LegOutboundQueueErrorKind::EndpointLost,
            ));
        }
        let bytes = match frame.encode() {
            Ok(encoded) => encoded.len(),
            Err(error) => {
                return Err(LegOutboundQueueError::new(
                    frame,
                    LegOutboundQueueErrorKind::InvalidFrame(error),
                ));
            }
        };
        let ordinal = self.next_enqueue_ordinal;
        let Some(next_ordinal) = ordinal.checked_add(1) else {
            return Err(LegOutboundQueueError::new(
                frame,
                LegOutboundQueueErrorKind::EnqueueOrdinalExhausted,
            ));
        };
        let Some(next_owned_bytes) = self.owned_bytes.checked_add(bytes) else {
            return Err(LegOutboundQueueError::new(
                frame,
                LegOutboundQueueErrorKind::ByteCountOverflow,
            ));
        };
        if self.frames.len() >= self.max_frames || next_owned_bytes > self.max_bytes {
            return Err(LegOutboundQueueError::new(
                frame,
                LegOutboundQueueErrorKind::CapacityExceeded {
                    queued_frames: self.frames.len(),
                    queued_bytes: self.owned_bytes,
                    incoming_bytes: bytes,
                },
            ));
        }
        debug_assert!(self.frames.capacity().saturating_sub(self.frames.len()) >= 1);
        let expected_phase = self.session_phase;
        // RESET is terminal session control. During an otherwise immediate
        // whole-session shutdown the reducer tombstone must not be cleared
        // and the actor cancelled until the exact peer delivery is observed;
        // local QUIC-stream submission alone is not that boundary.
        let ordered = self.session_phase == LegOutboundSessionPhase::AttachRecovery
            || matches!(frame.record(), Record::Reset { .. });
        Ok(PreparedOrdinaryFrameAdmission {
            queue: self,
            frame,
            expected_phase,
            ordinal,
            next_ordinal,
            bytes,
            next_owned_bytes,
            ordered,
            reset_delivery,
            activate_on_commit: expected_phase == LegOutboundSessionPhase::AwaitingAttachAcceptance,
        })
    }

    /// Admits one leg-control record into this authenticated leg's sole FIFO.
    /// Leg-control leaves the session phase unchanged, so a drained standby
    /// handshake may precede the first ATTACH_ACCEPTED without creating a
    /// second queue or bypassing the ordered transport actor.
    #[allow(
        clippy::result_large_err,
        reason = "the error must return the exact unconsumed control frame"
    )]
    pub(super) fn push_leg_control(
        &mut self,
        seal: &LegSeal,
        frame: LegControlFrame,
        features: FeatureSet,
    ) -> Result<LegControlEnqueued, LegControlQueueError> {
        if !self.binds_seal(seal) {
            return Err(LegControlQueueError::new(
                frame,
                LegControlQueueErrorKind::WrongLeg,
            ));
        }
        if self.endpoint_is_lost() {
            return Err(LegControlQueueError::new(
                frame,
                LegControlQueueErrorKind::EndpointLost,
            ));
        }
        if matches!(
            self.session_phase,
            LegOutboundSessionPhase::AttachAcceptanceReserved
                | LegOutboundSessionPhase::AttachRequestPending
                | LegOutboundSessionPhase::AttachStatusPending
                | LegOutboundSessionPhase::AttachRecovery
                | LegOutboundSessionPhase::ClientReplacementRecovery
                | LegOutboundSessionPhase::ClientRecoveryTerminal
                | LegOutboundSessionPhase::OwnerRecoveryTerminal
        ) {
            return Err(LegControlQueueError::new(
                frame,
                match self.session_phase {
                    LegOutboundSessionPhase::AttachAcceptanceReserved => {
                        LegControlQueueErrorKind::AttachAcceptanceReserved
                    }
                    LegOutboundSessionPhase::AttachRequestPending => {
                        LegControlQueueErrorKind::AttachRequestPending
                    }
                    LegOutboundSessionPhase::AttachStatusPending => {
                        LegControlQueueErrorKind::AttachStatusPending
                    }
                    LegOutboundSessionPhase::AttachRecovery => {
                        LegControlQueueErrorKind::AttachRecoveryPending
                    }
                    LegOutboundSessionPhase::ClientReplacementRecovery => {
                        LegControlQueueErrorKind::ClientReplacementRecoveryPending
                    }
                    LegOutboundSessionPhase::ClientRecoveryTerminal => {
                        LegControlQueueErrorKind::ClientRecoveryTerminal
                    }
                    LegOutboundSessionPhase::OwnerRecoveryTerminal => {
                        LegControlQueueErrorKind::OwnerRecoveryTerminal
                    }
                    LegOutboundSessionPhase::AwaitingAttachAcceptance
                    | LegOutboundSessionPhase::Active => unreachable!(),
                },
            ));
        }
        let delivery_state = Arc::new(OrderedDeliveryState {
            delivered: AtomicBool::new(false),
            client_recovery: None,
            tracked_reset: None,
        });
        let ordinal = self.push_control_inner(frame, features)?;
        if let Some(queued) = self.frames.back_mut() {
            queued.delivery_state = Some(Arc::clone(&delivery_state));
        }
        Ok(LegControlEnqueued {
            queue: Arc::downgrade(&self.identity),
            endpoint: self.endpoint.clone(),
            ordinal,
            delivery_state,
        })
    }

    /// Admits the exact signed ATTACH derived from a registered standby into
    /// that transport's sole FIFO. Unlike generic session admission, this
    /// transition remains pre-active until the correlated response is
    /// validated and installed by the client recovery typestate.
    #[allow(
        clippy::result_large_err,
        reason = "bounded pressure must return the exact signed ATTACH frame"
    )]
    pub(super) fn push_registered_attach_request(
        &mut self,
        seal: &LegSeal,
        request: AttachRequest,
        frame: Frame,
    ) -> Result<AttachRequestEnqueued, LegOutboundQueueError> {
        if !self.binds_seal(seal) {
            return Err(LegOutboundQueueError::new(
                frame,
                LegOutboundQueueErrorKind::WrongLeg,
            ));
        }
        if self.endpoint_is_lost() {
            return Err(LegOutboundQueueError::new(
                frame,
                LegOutboundQueueErrorKind::EndpointLost,
            ));
        }
        let carried = match AttachRequest::from_attach_frame(&frame) {
            Ok((carried, _proof)) => carried,
            Err(_) => {
                return Err(LegOutboundQueueError::new(
                    frame,
                    LegOutboundQueueErrorKind::NotAttachRequest,
                ));
            }
        };
        if carried != request {
            return Err(LegOutboundQueueError::new(
                frame,
                LegOutboundQueueErrorKind::MismatchedAttachRequest,
            ));
        }
        if self
            .ordered_delivery_wait
            .as_ref()
            .is_some_and(OrderedDeliveryWait::is_delivered)
        {
            self.ordered_delivery_wait = None;
        }
        if self.session_phase != LegOutboundSessionPhase::AwaitingAttachAcceptance
            || !self.frames.is_empty()
            || self.owned_bytes != 0
            || self.ordered_delivery_wait.is_some()
        {
            return Err(LegOutboundQueueError::new(
                frame,
                LegOutboundQueueErrorKind::AttachRequestNotFirst {
                    next_ordinal: self.next_enqueue_ordinal,
                    queued_frames: self.frames.len(),
                    queued_bytes: self.owned_bytes,
                },
            ));
        }
        let delivery_state = Arc::new(OrderedDeliveryState {
            delivered: AtomicBool::new(false),
            client_recovery: None,
            tracked_reset: None,
        });
        let ordinal = self.push_inner(frame, true)?;
        if let Some(queued) = self.frames.back_mut() {
            queued.delivery_state = Some(Arc::clone(&delivery_state));
        }
        self.session_phase = LegOutboundSessionPhase::AttachRequestPending;
        Ok(AttachRequestEnqueued {
            queue: Arc::downgrade(&self.identity),
            endpoint: self.endpoint.clone(),
            ordinal,
            request,
            delivery_state,
        })
    }

    /// Dedicated initial-bootstrap admission. It deliberately returns a
    /// distinct receipt so neither registered-standby recovery nor a generic
    /// ATTACH producer can be substituted for the initial client authority.
    #[allow(
        clippy::result_large_err,
        reason = "bounded pressure returns the exact signed initial ATTACH"
    )]
    fn push_initial_attach_request(
        &mut self,
        seal: &LegSeal,
        request: AttachRequest,
        frame: Frame,
    ) -> Result<InitialAttachRequestEnqueued, LegOutboundQueueError> {
        self.push_registered_attach_request(seal, request, frame)
            .map(InitialAttachRequestEnqueued)
    }

    /// Dedicated ordered admission for a stale request re-signed onto a fresh
    /// status-only transport after the registered B request was delivered and
    /// its sole validator was lost.
    #[allow(
        clippy::result_large_err,
        reason = "bounded pressure returns the exact signed status-retry frame"
    )]
    pub(super) fn push_status_retry_attach_request(
        &mut self,
        seal: &LegSeal,
        request: AttachRequest,
        frame: Frame,
    ) -> Result<StatusRetryAttachEnqueued, LegOutboundQueueError> {
        self.push_registered_attach_request(seal, request, frame)
            .map(StatusRetryAttachEnqueued)
    }

    /// Dedicated ordered admission for the exact successor derived from an
    /// authenticated high-water status. It cannot activate the queue before
    /// the correlated acceptance is validated by client typestate.
    #[allow(
        clippy::result_large_err,
        reason = "bounded pressure returns the exact signed catch-up frame"
    )]
    pub(super) fn push_catch_up_attach_request(
        &mut self,
        seal: &LegSeal,
        request: AttachRequest,
        frame: Frame,
    ) -> Result<CatchUpAttachEnqueued, LegOutboundQueueError> {
        self.push_registered_attach_request(seal, request, frame)
            .map(CatchUpAttachEnqueued)
    }

    /// Admits a proof-authenticated stale/exhausted ATTACH status through the
    /// exact status leg's sole FIFO. The queue remains non-active and the
    /// returned receipt exposes actual ordered delivery completion.
    #[allow(
        clippy::result_large_err,
        reason = "bounded pressure returns the exact correlated status frame"
    )]
    pub(super) fn push_authenticated_attach_status(
        &mut self,
        seal: &LegSeal,
        frame: Frame,
    ) -> Result<AttachStatusEnqueued, LegOutboundQueueError> {
        let (generation, session_id, requested_generation, nonce) = match frame.record() {
            Record::AttachGenerationStatus {
                session_id,
                requested_generation,
                nonce,
            } => (
                frame.leg_generation(),
                *session_id,
                *requested_generation,
                *nonce,
            ),
            _ => {
                return Err(LegOutboundQueueError::new(
                    frame,
                    LegOutboundQueueErrorKind::NotAttachStatus,
                ));
            }
        };
        if !self.binds_seal(seal) {
            return Err(LegOutboundQueueError::new(
                frame,
                LegOutboundQueueErrorKind::WrongLeg,
            ));
        }
        if self.endpoint_is_lost() {
            return Err(LegOutboundQueueError::new(
                frame,
                LegOutboundQueueErrorKind::EndpointLost,
            ));
        }
        if self
            .ordered_delivery_wait
            .as_ref()
            .is_some_and(OrderedDeliveryWait::is_delivered)
        {
            self.ordered_delivery_wait = None;
        }
        if self.session_phase != LegOutboundSessionPhase::AwaitingAttachAcceptance
            || !self.frames.is_empty()
            || self.owned_bytes != 0
            || self.ordered_delivery_wait.is_some()
        {
            return Err(LegOutboundQueueError::new(
                frame,
                LegOutboundQueueErrorKind::AttachStatusNotFirst {
                    next_ordinal: self.next_enqueue_ordinal,
                    queued_frames: self.frames.len(),
                    queued_bytes: self.owned_bytes,
                },
            ));
        }
        let delivery_state = Arc::new(OrderedDeliveryState {
            delivered: AtomicBool::new(false),
            client_recovery: None,
            tracked_reset: None,
        });
        let ordinal = self.push_inner(frame, true)?;
        if let Some(queued) = self.frames.back_mut() {
            queued.delivery_state = Some(Arc::clone(&delivery_state));
        }
        self.session_phase = LegOutboundSessionPhase::AttachStatusPending;
        Ok(AttachStatusEnqueued {
            queue: Arc::downgrade(&self.identity),
            endpoint: self.endpoint.clone(),
            ordinal,
            generation,
            session_id,
            requested_generation,
            nonce,
            delivery_state,
        })
    }

    /// Exclusively reserves this exact leg's sole empty pre-attach FIFO before
    /// the owner generation CAS. The reservation is non-cloneable and must be
    /// either explicitly released before a failed transaction or carried by
    /// the installed publication into the same queue's acceptance admission.
    pub(super) fn reserve_attach_acceptance(
        &mut self,
        leg: &EstablishedLeg,
    ) -> Result<AttachAcceptanceReservation, AttachAcceptanceReserveError> {
        if !self.binds_established_leg(leg) {
            return Err(AttachAcceptanceReserveError::WrongLeg);
        }
        if self.endpoint_is_lost() {
            return Err(AttachAcceptanceReserveError::EndpointLost);
        }
        if self
            .ordered_delivery_wait
            .as_ref()
            .is_some_and(OrderedDeliveryWait::is_delivered)
        {
            self.ordered_delivery_wait = None;
        }
        if self.session_phase != LegOutboundSessionPhase::AwaitingAttachAcceptance
            || !self.frames.is_empty()
            || self.owned_bytes != 0
            || self.ordered_delivery_wait.is_some()
        {
            return Err(AttachAcceptanceReserveError::NotAwaiting {
                queued_frames: self.frames.len(),
                queued_bytes: self.owned_bytes,
                ordered_delivery_pending: self.ordered_delivery_wait.is_some(),
            });
        }
        let Some(seal) = self.seal.as_ref().map(LegSeal::share) else {
            return Err(AttachAcceptanceReserveError::WrongLeg);
        };
        let ordinal = self.next_enqueue_ordinal;
        let Some(next_ordinal) = ordinal.checked_add(1) else {
            return Err(AttachAcceptanceReserveError::EnqueueOrdinalExhausted);
        };
        let Some(next_owned_bytes) = self.owned_bytes.checked_add(ATTACH_ACCEPTED_ENCODED_BYTES)
        else {
            return Err(AttachAcceptanceReserveError::ByteCountOverflow);
        };
        if self.frames.len() >= self.max_frames || next_owned_bytes > self.max_bytes {
            return Err(AttachAcceptanceReserveError::CapacityExceeded {
                queued_frames: self.frames.len(),
                queued_bytes: self.owned_bytes,
                incoming_bytes: ATTACH_ACCEPTED_ENCODED_BYTES,
            });
        }
        let delivery_state = Arc::new(OrderedDeliveryState {
            delivered: AtomicBool::new(false),
            client_recovery: None,
            tracked_reset: None,
        });
        self.session_phase = LegOutboundSessionPhase::AttachAcceptanceReserved;
        Ok(AttachAcceptanceReservation {
            queue: Arc::downgrade(&self.identity),
            endpoint: self.endpoint.clone(),
            seal,
            ordinal,
            next_ordinal,
            next_owned_bytes,
            delivery_state,
        })
    }

    /// Admits the exact correlated ATTACH_ACCEPTED into this FIFO and mints a
    /// non-cloneable proof of that queue transition. Recovery code cannot
    /// manufacture this receipt from a bare frame or from another record
    /// kind, and queue pressure returns the exact unconsumed frame.
    #[allow(
        clippy::result_large_err,
        reason = "the error must return the exact unconsumed frame without a hot-path allocation"
    )]
    #[cfg(test)]
    pub(crate) fn push_attach_acceptance(
        &mut self,
        attached: &AttachedLeg,
        frame: Frame,
    ) -> Result<AttachAcceptanceEnqueued, LegOutboundQueueError> {
        self.push_attach_acceptance_in_phase(
            attached,
            frame,
            LegOutboundSessionPhase::AwaitingAttachAcceptance,
        )
    }

    /// Consumes the wire acceptance only through the exact pre-CAS queue
    /// reservation retained by the installed owner publication.
    #[allow(
        clippy::result_large_err,
        reason = "reserved admission failure must return both exact reservation and frame ownership"
    )]
    pub(super) fn push_reserved_attach_acceptance(
        &mut self,
        reservation: AttachAcceptanceReservation,
        attached: &AttachedLeg,
        frame: Frame,
    ) -> Result<AttachAcceptanceEnqueued, ReservedAttachAcceptanceError> {
        if !reservation.belongs_to_queue(self)
            || !reservation.belongs_to_attached(attached)
            || !reservation.endpoint_matches(self)
            || !reservation.matches_reserved_state(self)
        {
            return Err(ReservedAttachAcceptanceError::new(
                reservation,
                frame,
                LegOutboundQueueErrorKind::WrongAttachAcceptanceReservation,
            ));
        }
        let (generation, session_id, nonce) = match frame.record() {
            Record::AttachAccepted {
                session_id, nonce, ..
            } => (frame.leg_generation(), *session_id, *nonce),
            _ => {
                return Err(ReservedAttachAcceptanceError::new(
                    reservation,
                    frame,
                    LegOutboundQueueErrorKind::NotAttachAcceptance,
                ));
            }
        };
        if !self.binds_attached_leg(attached) {
            return Err(ReservedAttachAcceptanceError::new(
                reservation,
                frame,
                LegOutboundQueueErrorKind::WrongLeg,
            ));
        }
        if self.endpoint_is_lost() {
            return Err(ReservedAttachAcceptanceError::new(
                reservation,
                frame,
                LegOutboundQueueErrorKind::EndpointLost,
            ));
        }
        if !attached.matches_attach_acceptance(&frame) {
            return Err(ReservedAttachAcceptanceError::new(
                reservation,
                frame,
                LegOutboundQueueErrorKind::MismatchedAttachAcceptance,
            ));
        }
        let delivery_state = Arc::clone(&reservation.delivery_state);
        let ordinal = reservation.ordinal;
        self.frames.push_back(QueuedOutboundFrame {
            frame: QueuedProtocolFrame::Session(frame),
            bytes: ATTACH_ACCEPTED_ENCODED_BYTES,
            ordered: true,
            delivery_state: Some(Arc::clone(&delivery_state)),
            budget: OutboundBudget::Ordinary,
        });
        self.owned_bytes = reservation.next_owned_bytes;
        self.next_enqueue_ordinal = reservation.next_ordinal;
        self.session_phase = LegOutboundSessionPhase::AttachRecovery;
        Ok(AttachAcceptanceEnqueued {
            queue: Arc::downgrade(&self.identity),
            endpoint: self.endpoint.clone(),
            ordinal,
            generation,
            session_id,
            nonce,
            delivery_state,
        })
    }

    /// Initial owner bootstrap has no recovery effects. The exact reserved
    /// acceptance admission and transition to Active therefore happen under
    /// one mutable queue borrow; capacity pressure can never ask the owner to
    /// repeat its already-committed generation CAS.
    #[allow(
        clippy::result_large_err,
        reason = "post-CAS pressure must return the exact reservation and acceptance frame"
    )]
    pub(super) fn push_reserved_initial_attach_acceptance(
        &mut self,
        reservation: AttachAcceptanceReservation,
        attached: &AttachedLeg,
        frame: Frame,
    ) -> Result<AttachAcceptanceEnqueued, ReservedAttachAcceptanceError> {
        let acceptance = self.push_reserved_attach_acceptance(reservation, attached, frame)?;
        debug_assert_eq!(self.session_phase, LegOutboundSessionPhase::AttachRecovery);
        self.session_phase = LegOutboundSessionPhase::Active;
        Ok(acceptance)
    }

    #[allow(
        clippy::result_large_err,
        reason = "bounded acceptance pressure must return the exact unconsumed frame"
    )]
    fn push_attach_acceptance_in_phase(
        &mut self,
        attached: &AttachedLeg,
        frame: Frame,
        required_phase: LegOutboundSessionPhase,
    ) -> Result<AttachAcceptanceEnqueued, LegOutboundQueueError> {
        let (generation, session_id, nonce) = match frame.record() {
            Record::AttachAccepted {
                session_id, nonce, ..
            } => (frame.leg_generation(), *session_id, *nonce),
            _ => {
                return Err(LegOutboundQueueError::new(
                    frame,
                    LegOutboundQueueErrorKind::NotAttachAcceptance,
                ));
            }
        };
        if !self.binds_attached_leg(attached) {
            return Err(LegOutboundQueueError::new(
                frame,
                LegOutboundQueueErrorKind::WrongLeg,
            ));
        }
        if self.endpoint_is_lost() {
            return Err(LegOutboundQueueError::new(
                frame,
                LegOutboundQueueErrorKind::EndpointLost,
            ));
        }
        if !attached.matches_attach_acceptance(&frame) {
            return Err(LegOutboundQueueError::new(
                frame,
                LegOutboundQueueErrorKind::MismatchedAttachAcceptance,
            ));
        }
        if self
            .ordered_delivery_wait
            .as_ref()
            .is_some_and(OrderedDeliveryWait::is_delivered)
        {
            self.ordered_delivery_wait = None;
        }
        // This is the first *session/recovery* admission, not necessarily the
        // queue's absolute ordinal. Future STANDBY leg-control may use this
        // sole FIFO first, provided that earlier leg-control has completed and
        // leaves this phase unchanged. A prior generic Frame permanently moves
        // the queue to Active and is still rejected here.
        if self.session_phase != required_phase
            || !self.frames.is_empty()
            || self.owned_bytes != 0
            || self.ordered_delivery_wait.is_some()
        {
            return Err(LegOutboundQueueError::new(
                frame,
                LegOutboundQueueErrorKind::AttachAcceptanceNotFirst {
                    next_ordinal: self.next_enqueue_ordinal,
                    queued_frames: self.frames.len(),
                    queued_bytes: self.owned_bytes,
                },
            ));
        }
        let delivery_state = Arc::new(OrderedDeliveryState {
            delivered: AtomicBool::new(false),
            client_recovery: None,
            tracked_reset: None,
        });
        let ordinal = self.push_inner(frame, true)?;
        if let Some(queued) = self.frames.back_mut() {
            queued.delivery_state = Some(Arc::clone(&delivery_state));
        }
        self.session_phase = LegOutboundSessionPhase::AttachRecovery;
        Ok(AttachAcceptanceEnqueued {
            queue: Arc::downgrade(&self.identity),
            endpoint: self.endpoint.clone(),
            ordinal,
            generation,
            session_id,
            nonce,
            delivery_state,
        })
    }

    #[allow(
        clippy::result_large_err,
        reason = "the error must return the exact unconsumed frame without a hot-path allocation"
    )]
    fn push_inner(&mut self, frame: Frame, ordered: bool) -> Result<u64, LegOutboundQueueError> {
        if self.endpoint_is_lost() {
            return Err(LegOutboundQueueError::new(
                frame,
                LegOutboundQueueErrorKind::EndpointLost,
            ));
        }
        let bytes = match frame.encode() {
            Ok(encoded) => encoded.len(),
            Err(source) => {
                return Err(LegOutboundQueueError::new(
                    frame,
                    LegOutboundQueueErrorKind::InvalidFrame(source),
                ));
            }
        };
        let ordinal = self.next_enqueue_ordinal;
        let Some(next_ordinal) = ordinal.checked_add(1) else {
            return Err(LegOutboundQueueError::new(
                frame,
                LegOutboundQueueErrorKind::EnqueueOrdinalExhausted,
            ));
        };
        let Some(next_bytes) = self.owned_bytes.checked_add(bytes) else {
            return Err(LegOutboundQueueError::new(
                frame,
                LegOutboundQueueErrorKind::ByteCountOverflow,
            ));
        };
        if self.frames.len() >= self.max_frames || next_bytes > self.max_bytes {
            return Err(LegOutboundQueueError::new(
                frame,
                LegOutboundQueueErrorKind::CapacityExceeded {
                    queued_frames: self.frames.len(),
                    queued_bytes: self.owned_bytes,
                    incoming_bytes: bytes,
                },
            ));
        }
        self.frames.push_back(QueuedOutboundFrame {
            frame: QueuedProtocolFrame::Session(frame),
            bytes,
            ordered,
            delivery_state: None,
            budget: OutboundBudget::Ordinary,
        });
        self.owned_bytes = next_bytes;
        self.next_enqueue_ordinal = next_ordinal;
        Ok(ordinal)
    }

    #[allow(
        clippy::result_large_err,
        reason = "the error must return the exact unconsumed control frame"
    )]
    fn push_control_inner(
        &mut self,
        frame: LegControlFrame,
        features: FeatureSet,
    ) -> Result<u64, LegControlQueueError> {
        let bytes = match frame.encode(features) {
            Ok(encoded) => encoded.len(),
            Err(source) => {
                return Err(LegControlQueueError::new(
                    frame,
                    LegControlQueueErrorKind::InvalidFrame(source),
                ));
            }
        };
        let ordinal = self.next_enqueue_ordinal;
        let Some(next_ordinal) = ordinal.checked_add(1) else {
            return Err(LegControlQueueError::new(
                frame,
                LegControlQueueErrorKind::EnqueueOrdinalExhausted,
            ));
        };
        let Some(next_bytes) = self.owned_bytes.checked_add(bytes) else {
            return Err(LegControlQueueError::new(
                frame,
                LegControlQueueErrorKind::ByteCountOverflow,
            ));
        };
        if self.frames.len() >= self.max_frames || next_bytes > self.max_bytes {
            return Err(LegControlQueueError::new(
                frame,
                LegControlQueueErrorKind::CapacityExceeded {
                    queued_frames: self.frames.len(),
                    queued_bytes: self.owned_bytes,
                    incoming_bytes: bytes,
                },
            ));
        }
        self.frames.push_back(QueuedOutboundFrame {
            frame: QueuedProtocolFrame::LegControl { frame, features },
            bytes,
            ordered: true,
            delivery_state: None,
            budget: OutboundBudget::Ordinary,
        });
        self.owned_bytes = next_bytes;
        self.next_enqueue_ordinal = next_ordinal;
        Ok(ordinal)
    }

    pub(crate) fn try_flush<T: EncodedLegTransport + ?Sized>(
        &mut self,
        endpoint: &mut LegIo,
        transport: &mut T,
        now: SimTime,
    ) -> Result<Option<OutboundSubmission>, LegIoError> {
        if self.endpoint_is_lost() {
            return Err(LegIoError::OutboundQueueEndpointLost);
        }
        if !self.binds_established_leg(endpoint.established_leg()) {
            return Err(LegIoError::WrongOutboundQueueLeg);
        }
        if self
            .ordered_delivery_wait
            .as_ref()
            .is_some_and(|wait| !wait.is_delivered())
        {
            return Err(LegIoError::OrderedDeliveryPending);
        }
        self.ordered_delivery_wait = None;
        let Some(queued) = self.frames.front() else {
            return Ok(None);
        };
        let is_ack = matches!(
            &queued.frame,
            QueuedProtocolFrame::Session(frame)
                | QueuedProtocolFrame::PreparedClientRecovery { frame, .. }
                if matches!(frame.record(), Record::Ack { .. })
        );
        let wait = match &queued.frame {
            QueuedProtocolFrame::Session(frame) => {
                if queued.ordered {
                    let (wait, completion) = match queued.delivery_state.as_ref() {
                        Some(state) => (
                            OrderedDeliveryWait(Arc::clone(state)),
                            OrderedSessionDeliveryToken(Arc::clone(state)),
                        ),
                        None => OrderedDeliveryWait::session_pair(),
                    };
                    endpoint.send_retained_frame(
                        transport,
                        now,
                        frame,
                        Some(completion),
                        &queued.budget,
                        Some(queued.bytes),
                    )?;
                    Some(wait)
                } else {
                    endpoint.send_retained_frame(
                        transport,
                        now,
                        frame,
                        None,
                        &queued.budget,
                        Some(queued.bytes),
                    )?;
                    None
                }
            }
            QueuedProtocolFrame::PreparedClientRecovery {
                frame,
                encoded,
                delivery_state,
            } => {
                debug_assert!(queued.ordered);
                let wait = OrderedDeliveryWait(Arc::clone(delivery_state));
                let completion = OrderedSessionDeliveryToken(Arc::clone(delivery_state));
                endpoint.send_retained_preencoded_frame(
                    transport,
                    now,
                    frame,
                    encoded.clone(),
                    completion,
                    &queued.budget,
                    queued.bytes,
                )?;
                Some(wait)
            }
            QueuedProtocolFrame::LegControl { frame, features } => {
                debug_assert!(queued.ordered);
                let (wait, completion) = match queued.delivery_state.as_ref() {
                    Some(state) => (
                        OrderedDeliveryWait(Arc::clone(state)),
                        OrderedLegControlDeliveryToken(Arc::clone(state)),
                    ),
                    None => OrderedDeliveryWait::leg_control_pair(),
                };
                endpoint
                    .send_retained_control_frame(transport, now, frame, *features, completion)?;
                Some(wait)
            }
        };
        let queued = self.frames.pop_front().ok_or(LegIoError::CounterOverflow)?;
        self.owned_bytes = self
            .owned_bytes
            .checked_sub(queued.bytes)
            .ok_or(LegIoError::CounterOverflow)?;
        self.ordered_delivery_wait = wait.filter(|wait| !wait.is_delivered());
        Ok(Some(OutboundSubmission { is_ack }))
    }

    /// Ends the admission phase after every recovery effect has entered this
    /// exact queue. Already queued recovery frames retain their ordered flag;
    /// the completion wait also blocks every later frame until the final
    /// ordered submission is actually delivered.
    pub(crate) fn finish_attach_recovery(&mut self, acceptance: &AttachAcceptanceEnqueued) -> bool {
        if self.session_phase != LegOutboundSessionPhase::AttachRecovery
            || !acceptance.queue_is_live()
            || !acceptance.belongs_to_queue(self)
        {
            return false;
        }
        self.session_phase = LegOutboundSessionPhase::Active;
        true
    }

    pub(super) fn reserve_registered_client_recovery(
        &mut self,
        endpoint: &mut LegIo,
        request: &CausallyDeliveredAttachRequest,
        recovery: &PreparedClientRecovery,
    ) -> Result<ClientRecoveryReservation, ClientRecoveryReserveError> {
        self.reserve_client_recovery(
            endpoint,
            &request.enqueued,
            recovery,
            AttachRequestDeliveryProof::ExactAuthenticatedResponse,
        )
    }

    pub(super) fn reserve_catch_up_client_recovery(
        &mut self,
        endpoint: &mut LegIo,
        request: &CatchUpAttachEnqueued,
        recovery: &PreparedClientRecovery,
    ) -> Result<ClientRecoveryReservation, ClientRecoveryReserveError> {
        self.reserve_client_recovery(
            endpoint,
            &request.0,
            recovery,
            AttachRequestDeliveryProof::LocalDelivery,
        )
    }

    fn reserve_client_recovery(
        &mut self,
        endpoint: &mut LegIo,
        request: &AttachRequestEnqueued,
        recovery: &PreparedClientRecovery,
        delivery_proof: AttachRequestDeliveryProof,
    ) -> Result<ClientRecoveryReservation, ClientRecoveryReserveError> {
        let capacity = recovery.capacity;
        if !request.belongs_to_queue(self) {
            return Err(ClientRecoveryReserveError::WrongQueue);
        }
        if !request.queue_is_live() || self.endpoint_is_lost() {
            return Err(ClientRecoveryReserveError::EndpointOrQueueLost);
        }
        let locally_delivered = request.was_delivered();
        if !locally_delivered && delivery_proof == AttachRequestDeliveryProof::LocalDelivery {
            return Err(ClientRecoveryReserveError::RequestNotDelivered);
        }
        if recovery.generation != request.request.requested_generation() {
            return Err(ClientRecoveryReserveError::WrongGeneration);
        }
        if self.ordered_delivery_wait.as_ref().is_some_and(|wait| {
            request.owns_delivery_wait(wait)
                && (wait.is_delivered()
                    || delivery_proof == AttachRequestDeliveryProof::ExactAuthenticatedResponse)
        }) {
            // Both evidence forms retire only this request's pointer-identical
            // wait. For causal acceptance, a late local notice may still mark
            // its shared state without touching the installed FIFO.
            self.ordered_delivery_wait = None;
        }
        if self.session_phase != LegOutboundSessionPhase::AttachRequestPending
            || !self.frames.is_empty()
            || self.owned_bytes != 0
            || self.ordered_delivery_wait.is_some()
        {
            return Err(ClientRecoveryReserveError::NotAwaitingReplacement);
        }
        if capacity.frame_count > 0 && capacity.largest_frame_bytes > self.max_bytes {
            return Err(ClientRecoveryReserveError::FrameCannotFit {
                largest_frame_bytes: capacity.largest_frame_bytes,
                queue_max_bytes: self.max_bytes,
            });
        }
        let frame_count = u64::try_from(capacity.frame_count)
            .map_err(|_| ClientRecoveryReserveError::OrdinalExhausted)?;
        let reserved_end_ordinal = self
            .next_enqueue_ordinal
            .checked_add(frame_count)
            .ok_or(ClientRecoveryReserveError::OrdinalExhausted)?;
        let Some(seal) = self.seal.as_ref() else {
            return Err(ClientRecoveryReserveError::WrongEndpoint);
        };
        if !endpoint.established_leg().belongs_to_transport(seal) {
            return Err(ClientRecoveryReserveError::WrongEndpoint);
        }
        self.frames
            .try_reserve_exact(capacity.frame_count.min(self.max_frames))
            .map_err(|_| ClientRecoveryReserveError::StorageUnavailable)?;
        let io_reservation = endpoint.reserve_client_recovery_capacity(seal, capacity)?;
        self.session_phase = LegOutboundSessionPhase::ClientReplacementRecovery;
        Ok(ClientRecoveryReservation {
            queue: Arc::downgrade(&self.identity),
            endpoint: self.endpoint.clone(),
            generation: recovery.generation,
            start_ordinal: self.next_enqueue_ordinal,
            reserved_end_ordinal,
            initial_frames: capacity.frame_count,
            initial_bytes: capacity.encoded_bytes,
            remaining_frames: capacity.frame_count,
            remaining_bytes: capacity.encoded_bytes,
            delivery_state: Arc::clone(&recovery.delivery_state),
            io_reservation,
        })
    }

    /// Restores a pre-commit reservation only while no recovery frame has
    /// entered the queue. Installed reservations have a distinct type and no
    /// release operation.
    pub(super) fn release_client_recovery(
        &mut self,
        endpoint: &mut LegIo,
        reservation: ClientRecoveryReservation,
    ) -> Result<(), ClientRecoveryReservation> {
        if self.session_phase != LegOutboundSessionPhase::ClientReplacementRecovery
            || !reservation.belongs_to_queue(self)
            || self.next_enqueue_ordinal != reservation.start_ordinal
            || reservation.remaining_frames != reservation.initial_frames
            || reservation.remaining_bytes != reservation.initial_bytes
            || !self.frames.is_empty()
            || self.owned_bytes != 0
        {
            return Err(reservation);
        }
        let ClientRecoveryReservation {
            queue,
            endpoint: endpoint_weak,
            generation,
            start_ordinal,
            reserved_end_ordinal,
            initial_frames,
            initial_bytes,
            remaining_frames,
            remaining_bytes,
            delivery_state,
            io_reservation,
        } = reservation;
        if let Err(io_reservation) = endpoint.release_client_recovery_capacity(io_reservation) {
            return Err(ClientRecoveryReservation {
                queue,
                endpoint: endpoint_weak,
                generation,
                start_ordinal,
                reserved_end_ordinal,
                initial_frames,
                initial_bytes,
                remaining_frames,
                remaining_bytes,
                delivery_state,
                io_reservation,
            });
        }
        self.session_phase = LegOutboundSessionPhase::AttachRequestPending;
        Ok(())
    }

    #[allow(
        clippy::result_large_err,
        reason = "recovery admission failure returns the exact reservation and prepared frame without boxing"
    )]
    pub(super) fn push_client_recovery_frame(
        &mut self,
        reservation: InstalledClientRecoveryReservation,
        prepared: PreparedClientRecoveryFrame,
    ) -> Result<InstalledClientRecoveryReservation, ClientRecoveryEnqueueFailure> {
        let encoded_bytes = prepared.encoded.len();
        if self.session_phase != LegOutboundSessionPhase::ClientReplacementRecovery
            || !reservation.belongs_to_queue(self)
        {
            return Err(ClientRecoveryEnqueueFailure::new(
                reservation,
                prepared,
                ClientRecoveryEnqueueErrorKind::WrongQueue,
            ));
        }
        if !reservation.endpoint_is_open() || self.endpoint_is_lost() {
            return Err(ClientRecoveryEnqueueFailure::new(
                reservation,
                prepared,
                ClientRecoveryEnqueueErrorKind::EndpointOrQueueLost,
            ));
        }
        if prepared.frame.leg_generation() != reservation.generation {
            return Err(ClientRecoveryEnqueueFailure::new(
                reservation,
                prepared,
                ClientRecoveryEnqueueErrorKind::GenerationMismatch,
            ));
        }
        if reservation.remaining_frames == 0
            || encoded_bytes == 0
            || encoded_bytes > reservation.remaining_bytes
            || encoded_bytes > self.max_bytes
            || self.next_enqueue_ordinal >= reservation.reserved_end_ordinal
        {
            return Err(ClientRecoveryEnqueueFailure::new(
                reservation,
                prepared,
                ClientRecoveryEnqueueErrorKind::CapacityMismatch,
            ));
        }
        if self
            .ordered_delivery_wait
            .as_ref()
            .is_some_and(OrderedDeliveryWait::is_delivered)
        {
            self.ordered_delivery_wait = None;
        }
        let Some(next_bytes) = self.owned_bytes.checked_add(encoded_bytes) else {
            return Err(ClientRecoveryEnqueueFailure::new(
                reservation,
                prepared,
                ClientRecoveryEnqueueErrorKind::CapacityMismatch,
            ));
        };
        if self.ordered_delivery_wait.is_some()
            || self.frames.len() >= self.max_frames
            || next_bytes > self.max_bytes
        {
            return Err(ClientRecoveryEnqueueFailure::new(
                reservation,
                prepared,
                ClientRecoveryEnqueueErrorKind::Pressure,
            ));
        }
        let PreparedClientRecoveryFrame {
            frame,
            encoded,
            delivery_state,
        } = prepared;
        debug_assert!(self.frames.len() < self.frames.capacity());
        self.frames.push_back(QueuedOutboundFrame {
            frame: QueuedProtocolFrame::PreparedClientRecovery {
                frame,
                encoded,
                delivery_state,
            },
            bytes: encoded_bytes,
            ordered: true,
            delivery_state: None,
            budget: OutboundBudget::ClientRecoveryReserved(Arc::clone(
                &reservation.io_reservation.state,
            )),
        });
        self.owned_bytes = next_bytes;
        self.next_enqueue_ordinal += 1;
        Ok(InstalledClientRecoveryReservation {
            queue: reservation.queue,
            endpoint: reservation.endpoint,
            generation: reservation.generation,
            reserved_end_ordinal: reservation.reserved_end_ordinal,
            remaining_frames: reservation.remaining_frames - 1,
            remaining_bytes: reservation.remaining_bytes - encoded_bytes,
            delivery_state: reservation.delivery_state,
            io_reservation: reservation.io_reservation,
        })
    }

    pub(super) fn finish_client_recovery_admission(
        &mut self,
        reservation: InstalledClientRecoveryReservation,
    ) -> Result<ClientRecoveryTail, ClientRecoveryFinishFailure> {
        if self.session_phase != LegOutboundSessionPhase::ClientReplacementRecovery
            || !reservation.belongs_to_queue(self)
        {
            return Err(ClientRecoveryFinishFailure::new(
                reservation,
                ClientRecoveryFinishErrorKind::WrongQueue,
            ));
        }
        if !reservation.endpoint_is_open() || self.endpoint_is_lost() {
            return Err(ClientRecoveryFinishFailure::new(
                reservation,
                ClientRecoveryFinishErrorKind::EndpointOrQueueLost,
            ));
        }
        if reservation.remaining_frames != 0
            || reservation.remaining_bytes != 0
            || self.next_enqueue_ordinal != reservation.reserved_end_ordinal
        {
            return Err(ClientRecoveryFinishFailure::new(
                reservation,
                ClientRecoveryFinishErrorKind::RecoveryIncomplete,
            ));
        }
        self.session_phase = LegOutboundSessionPhase::Active;
        Ok(ClientRecoveryTail {
            queue: reservation.queue,
            endpoint: reservation.endpoint,
            generation: reservation.generation,
            delivery_state: reservation.delivery_state,
            io_reservation: reservation.io_reservation,
        })
    }

    pub(super) fn discard_installed_client_recovery_after_terminal(
        &mut self,
        endpoint: &mut LegIo,
        reservation: InstalledClientRecoveryReservation,
        _terminal: &ClientRecoveryTerminalGate,
    ) -> Result<(), InstalledClientRecoveryReservation> {
        if self.session_phase != LegOutboundSessionPhase::ClientReplacementRecovery
            || !reservation.belongs_to_queue(self)
            || !self.endpoint_is_lost()
        {
            return Err(reservation);
        }
        let InstalledClientRecoveryReservation {
            queue,
            endpoint: endpoint_weak,
            generation,
            reserved_end_ordinal,
            remaining_frames,
            remaining_bytes,
            delivery_state,
            io_reservation,
        } = reservation;
        if let Err(io_reservation) = endpoint.release_client_recovery_capacity(io_reservation) {
            return Err(InstalledClientRecoveryReservation {
                queue,
                endpoint: endpoint_weak,
                generation,
                reserved_end_ordinal,
                remaining_frames,
                remaining_bytes,
                delivery_state,
                io_reservation,
            });
        }
        self.frames.clear();
        self.owned_bytes = 0;
        self.ordered_delivery_wait = None;
        self.session_phase = LegOutboundSessionPhase::ClientRecoveryTerminal;
        Ok(())
    }

    pub(super) fn discard_client_recovery_tail_after_terminal(
        &mut self,
        endpoint: &mut LegIo,
        tail: ClientRecoveryTail,
        _terminal: &ClientRecoveryTerminalGate,
    ) -> Result<(), ClientRecoveryTail> {
        if self.session_phase != LegOutboundSessionPhase::Active
            || !tail.belongs_to_queue(self)
            || !self.endpoint_is_lost()
        {
            return Err(tail);
        }
        let ClientRecoveryTail {
            queue,
            endpoint: endpoint_weak,
            generation,
            delivery_state,
            io_reservation,
        } = tail;
        if let Err(io_reservation) = endpoint.release_client_recovery_capacity(io_reservation) {
            return Err(ClientRecoveryTail {
                queue,
                endpoint: endpoint_weak,
                generation,
                delivery_state,
                io_reservation,
            });
        }
        self.frames.clear();
        self.owned_bytes = 0;
        self.ordered_delivery_wait = None;
        self.session_phase = LegOutboundSessionPhase::ClientRecoveryTerminal;
        Ok(())
    }

    fn settle_delivered_client_recovery_tail(
        &mut self,
        endpoint: &mut LegIo,
        tail: ClientRecoveryTail,
        expected_generation: LegGeneration,
    ) -> Result<ClientRecoveryTailSettled, ClientRecoveryTailSettlementFailure> {
        let reject = |tail, kind| ClientRecoveryTailSettlementFailure { tail, kind };
        if self.session_phase != LegOutboundSessionPhase::Active {
            return Err(reject(
                tail,
                ClientRecoveryTailSettlementErrorKind::QueueNotActive,
            ));
        }
        if !tail.belongs_to_queue(self) {
            return Err(reject(
                tail,
                ClientRecoveryTailSettlementErrorKind::WrongQueue,
            ));
        }
        if !self.belongs_to_established_leg(endpoint.established_leg())
            || !tail.endpoint_matches_queue(self)
        {
            return Err(reject(
                tail,
                ClientRecoveryTailSettlementErrorKind::WrongEndpoint,
            ));
        }
        if tail.generation != expected_generation {
            return Err(reject(
                tail,
                ClientRecoveryTailSettlementErrorKind::WrongGeneration,
            ));
        }
        if self.endpoint_is_lost() || !tail.endpoint_is_open() {
            return Err(reject(
                tail,
                ClientRecoveryTailSettlementErrorKind::EndpointLost,
            ));
        }
        if !tail.actual_delivery_complete() {
            return Err(reject(
                tail,
                ClientRecoveryTailSettlementErrorKind::DeliveryPending,
            ));
        }

        let exact_recovery_wait = self
            .ordered_delivery_wait
            .as_ref()
            .is_some_and(|wait| wait.belongs_to_client_recovery(&tail.delivery_state));
        if exact_recovery_wait
            && !self
                .ordered_delivery_wait
                .as_ref()
                .is_some_and(OrderedDeliveryWait::is_delivered)
        {
            return Err(reject(
                tail,
                ClientRecoveryTailSettlementErrorKind::DeliveryPending,
            ));
        }
        let ClientRecoveryTail {
            queue,
            endpoint: endpoint_weak,
            generation,
            delivery_state,
            io_reservation,
        } = tail;
        if let Err(io_reservation) = endpoint.release_client_recovery_capacity(io_reservation) {
            return Err(reject(
                ClientRecoveryTail {
                    queue,
                    endpoint: endpoint_weak,
                    generation,
                    delivery_state,
                    io_reservation,
                },
                ClientRecoveryTailSettlementErrorKind::ReservationMismatch,
            ));
        }
        if exact_recovery_wait {
            drop(self.ordered_delivery_wait.take());
        }
        Ok(ClientRecoveryTailSettled { generation })
    }

    /// Consumes the delivered initial request receipt and activates only its
    /// exact sole queue. This transition allocates and enqueues nothing, so a
    /// successful response cannot be re-run because of local pressure.
    pub(crate) fn finish_initial_client_attach(
        &mut self,
        request: &InitialAttachRequestEnqueued,
    ) -> Result<(), InitialAttachActivationError> {
        if !request.belongs_to_queue(self) {
            return Err(InitialAttachActivationError::WrongQueue);
        }
        if !request.queue_is_live() || self.endpoint_is_lost() {
            return Err(InitialAttachActivationError::EndpointOrQueueLost);
        }
        if !request.was_delivered() {
            return Err(InitialAttachActivationError::RequestNotDelivered);
        }
        if self.session_phase != LegOutboundSessionPhase::AttachRequestPending
            || !self.frames.is_empty()
            || self.owned_bytes != 0
            || self
                .ordered_delivery_wait
                .as_ref()
                .is_some_and(|wait| !wait.is_delivered())
        {
            return Err(InitialAttachActivationError::NotAwaitingInitialAttach);
        }
        self.ordered_delivery_wait = None;
        self.session_phase = LegOutboundSessionPhase::Active;
        Ok(())
    }

    fn transport_residual(&self) -> Option<ClientLegTransportResidual> {
        let residual = ClientLegTransportResidual {
            active: self.session_phase == LegOutboundSessionPhase::Active,
            queued_frames: self.frames.len(),
            owned_bytes: self.owned_bytes,
            ordered_delivery_wait: self.ordered_delivery_wait.is_some(),
        };
        (!residual.is_pristine()).then_some(residual)
    }

    pub(crate) fn len(&self) -> usize {
        self.frames.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }

    pub(crate) const fn owned_bytes(&self) -> usize {
        self.owned_bytes
    }

    #[cfg(test)]
    pub(super) fn mark_active_for_test(&mut self) -> bool {
        if self.session_phase != LegOutboundSessionPhase::AwaitingAttachAcceptance
            || !self.frames.is_empty()
            || self.owned_bytes != 0
            || self.ordered_delivery_wait.is_some()
        {
            return false;
        }
        self.session_phase = LegOutboundSessionPhase::Active;
        true
    }

    fn binds_attached_leg(&mut self, attached: &AttachedLeg) -> bool {
        match &self.seal {
            Some(seal) => attached.belongs_to_transport(seal),
            None => {
                #[cfg(test)]
                {
                    let Some((seal, lease)) = attached.try_claim_unbound_test_queue() else {
                        return false;
                    };
                    self.seal = Some(seal);
                    self._lease = Some(lease);
                    true
                }
                #[cfg(not(test))]
                {
                    false
                }
            }
        }
    }

    fn binds_established_leg(&mut self, established: &EstablishedLeg) -> bool {
        if self.endpoint_is_lost() {
            return false;
        }
        self.binds_established_leg_identity(established)
    }

    /// Checks or claims only the endpoint/FIFO identity. Bootstrap must
    /// compare every carried identity before liveness so a dead acceptance
    /// cannot misclassify an unrelated live queue as its own lost endpoint.
    fn binds_established_leg_identity(&mut self, established: &EstablishedLeg) -> bool {
        match &self.seal {
            Some(seal) => established.belongs_to_transport(seal),
            None => {
                let Some((seal, endpoint, lease)) = established.try_claim_outbound_queue() else {
                    return false;
                };
                self.seal = Some(seal);
                self.endpoint = Some(endpoint);
                self._lease = Some(lease);
                true
            }
        }
    }

    fn binds_seal(&self, seal: &LegSeal) -> bool {
        self.seal
            .as_ref()
            .is_some_and(|owned| owned.same_connection(seal))
    }

    fn endpoint_is_lost(&self) -> bool {
        self.endpoint.as_ref().is_some_and(|endpoint| {
            endpoint
                .upgrade()
                .is_none_or(|endpoint| !endpoint.is_open())
        })
    }
}

/// Non-cloneable proof that one exact ATTACH_ACCEPTED crossed a bounded FIFO
/// admission point. The weak queue identity makes controller teardown
/// observable without extending the queue's lifetime solely through a
/// recovery token.
pub(crate) struct AttachAcceptanceEnqueued {
    queue: Weak<LegOutboundQueueIdentity>,
    endpoint: Option<Weak<LegTransportEndpoint>>,
    ordinal: u64,
    generation: LegGeneration,
    session_id: SessionId,
    nonce: AttachNonce,
    delivery_state: Arc<OrderedDeliveryState>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ClientRecoveryCapacity {
    frame_count: usize,
    encoded_bytes: usize,
    largest_frame_bytes: usize,
}

/// Exact reducer recovery transformed and encoded before the reducer commit.
/// Neither callers nor the queue can substitute an arbitrary frame/length
/// pair after this value has been minted.
pub(super) struct PreparedClientRecovery {
    generation: LegGeneration,
    capacity: ClientRecoveryCapacity,
    client_local_replay: PreparedClientReplayRanges,
    delivery_state: Arc<ClientRecoveryDeliveryState>,
    rebound_reset_deliveries: usize,
    effects: Vec<PreparedClientRecoveryEffect>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct ClientRecoverySupersessionManifest {
    generation: LegGeneration,
    frame_count: usize,
    encoded_bytes: usize,
    client_local_replay_bytes: usize,
    rebound_reset_deliveries: usize,
}

impl ClientRecoverySupersessionManifest {
    pub(super) const fn generation(self) -> LegGeneration {
        self.generation
    }

    pub(super) const fn frame_count(self) -> usize {
        self.frame_count
    }

    pub(super) const fn encoded_bytes(self) -> usize {
        self.encoded_bytes
    }

    pub(super) const fn client_local_replay_bytes(self) -> usize {
        self.client_local_replay_bytes
    }

    pub(super) const fn rebound_reset_deliveries(self) -> usize {
        self.rebound_reset_deliveries
    }
}

pub(super) struct PreparedClientReplayRanges {
    data_bytes: usize,
    ranges: Vec<ClientReplayRange>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ClientReplayRange {
    flow_id: crate::resumable::SessionFlowId,
    direction: Direction,
    start: crate::resumable::ByteOffset,
    end: crate::resumable::ByteOffset,
}

impl PreparedClientReplayRanges {
    pub(super) const fn data_bytes(&self) -> usize {
        self.data_bytes
    }

    pub(super) const fn range_count(&self) -> usize {
        self.ranges.len()
    }

    pub(super) fn intersects_ack_delta(
        &self,
        flow_id: crate::resumable::SessionFlowId,
        direction: Direction,
        previous_accepted: crate::resumable::ByteOffset,
        next_accepted: crate::resumable::ByteOffset,
    ) -> bool {
        previous_accepted < next_accepted
            && self.ranges.iter().any(|range| {
                range.flow_id == flow_id
                    && range.direction == direction
                    && previous_accepted < range.end
                    && range.start < next_accepted
            })
    }
}

pub(super) enum PreparedClientRecoveryEffect {
    LegActivated { generation: LegGeneration },
    Transmit(PreparedClientRecoveryFrame),
}

pub(super) struct PreparedClientRecoveryFrame {
    frame: Frame,
    encoded: Bytes,
    delivery_state: Arc<OrderedDeliveryState>,
}

impl PreparedClientRecovery {
    pub(super) fn preflight(
        recovery: Vec<SessionEffect>,
        expected_generation: LegGeneration,
        effect_bound: usize,
    ) -> Result<Self, ClientRecoveryPlanError> {
        if recovery.is_empty() || recovery.len() > effect_bound {
            return Err(ClientRecoveryPlanError::EffectBound {
                actual: recovery.len(),
                max: effect_bound,
            });
        }
        let mut effects = Vec::new();
        effects
            .try_reserve_exact(recovery.len())
            .map_err(|_| ClientRecoveryPlanError::StorageUnavailable)?;
        let mut client_local_replay_ranges = Vec::new();
        client_local_replay_ranges
            .try_reserve_exact(recovery.len().saturating_sub(1))
            .map_err(|_| ClientRecoveryPlanError::StorageUnavailable)?;
        let mut frame_count = 0usize;
        let mut encoded_bytes = 0usize;
        let mut largest_frame_bytes = 0usize;
        let mut client_local_replay_data_bytes = 0usize;
        let delivery_state = Arc::new(ClientRecoveryDeliveryState {
            undelivered_frames: AtomicU64::new(0),
        });
        for (index, effect) in recovery.into_iter().enumerate() {
            match (index, effect) {
                (0, SessionEffect::LegActivated { generation })
                    if generation == expected_generation =>
                {
                    effects.push(PreparedClientRecoveryEffect::LegActivated { generation });
                }
                (0, _) => return Err(ClientRecoveryPlanError::MissingExactActivation),
                (_, SessionEffect::Transmit(frame))
                    if frame.leg_generation() == expected_generation =>
                {
                    if let Record::Data {
                        flow_id,
                        direction: Direction::ClientToTarget,
                        offset,
                        payload,
                    } = frame.record()
                    {
                        let end = offset
                            .checked_advance(payload.len())
                            .map_err(ClientRecoveryPlanError::InvalidFrame)?;
                        client_local_replay_data_bytes = client_local_replay_data_bytes
                            .checked_add(payload.len())
                            .ok_or(ClientRecoveryPlanError::CounterOverflow)?;
                        client_local_replay_ranges.push(ClientReplayRange {
                            flow_id: *flow_id,
                            direction: Direction::ClientToTarget,
                            start: *offset,
                            end,
                        });
                    }
                    let encoded = frame
                        .encode()
                        .map_err(ClientRecoveryPlanError::InvalidFrame)?;
                    let len = encoded.len();
                    frame_count = frame_count
                        .checked_add(1)
                        .ok_or(ClientRecoveryPlanError::CounterOverflow)?;
                    encoded_bytes = encoded_bytes
                        .checked_add(len)
                        .ok_or(ClientRecoveryPlanError::CounterOverflow)?;
                    largest_frame_bytes = largest_frame_bytes.max(len);
                    effects.push(PreparedClientRecoveryEffect::Transmit(
                        PreparedClientRecoveryFrame {
                            frame,
                            encoded,
                            delivery_state: Arc::new(OrderedDeliveryState {
                                delivered: AtomicBool::new(false),
                                client_recovery: Some(Arc::clone(&delivery_state)),
                                tracked_reset: None,
                            }),
                        },
                    ));
                }
                (_, SessionEffect::Transmit(_)) => {
                    return Err(ClientRecoveryPlanError::WrongGeneration);
                }
                (_, _) => return Err(ClientRecoveryPlanError::UnexpectedEffect),
            }
        }
        let undelivered_frames =
            u64::try_from(frame_count).map_err(|_| ClientRecoveryPlanError::CounterOverflow)?;
        delivery_state
            .undelivered_frames
            .store(undelivered_frames, Ordering::Release);
        Ok(Self {
            generation: expected_generation,
            capacity: ClientRecoveryCapacity {
                frame_count,
                encoded_bytes,
                largest_frame_bytes,
            },
            client_local_replay: PreparedClientReplayRanges {
                data_bytes: client_local_replay_data_bytes,
                ranges: client_local_replay_ranges,
            },
            delivery_state,
            rebound_reset_deliveries: 0,
            effects,
        })
    }

    pub(super) const fn generation(&self) -> LegGeneration {
        self.generation
    }

    pub(super) const fn supersession_manifest(&self) -> ClientRecoverySupersessionManifest {
        ClientRecoverySupersessionManifest {
            generation: self.generation,
            frame_count: self.capacity.frame_count,
            encoded_bytes: self.capacity.encoded_bytes,
            client_local_replay_bytes: self.client_local_replay.data_bytes,
            rebound_reset_deliveries: self.rebound_reset_deliveries,
        }
    }

    pub(super) fn into_effects_and_client_local_replay(
        self,
    ) -> (
        std::vec::IntoIter<PreparedClientRecoveryEffect>,
        PreparedClientReplayRanges,
    ) {
        (self.effects.into_iter(), self.client_local_replay)
    }
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub(super) enum ClientRecoveryPlanError {
    #[error("client recovery effect count {actual} is outside 1..={max}")]
    EffectBound { actual: usize, max: usize },
    #[error("client recovery does not begin with its exact LegActivated effect")]
    MissingExactActivation,
    #[error("client recovery contains a frame from another generation")]
    WrongGeneration,
    #[error("client recovery contains a non-wire effect after activation")]
    UnexpectedEffect,
    #[error("client recovery frame failed shared codec validation: {0}")]
    InvalidFrame(ProtocolError),
    #[error("client recovery manifest counter overflowed")]
    CounterOverflow,
    #[error(transparent)]
    ResetDeliveryRebind(#[from] ClientRecoveryResetRebindError),
    #[error("client recovery manifest storage could not be reserved")]
    StorageUnavailable,
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub(super) enum ClientRecoveryResetRebindError {
    #[error("superseded RESET delivery state has an impossible queue shape")]
    SupersededDeliveryShape,
    #[error("superseded RESET frame flow {frame:?} does not match tracked flow {tracked:?}")]
    SupersededFlowMismatch {
        tracked: SessionFlowId,
        frame: Option<SessionFlowId>,
    },
    #[error("superseded RESET flow {flow_id:?} has inconsistent local/root delivery state")]
    DeliveryStateMismatch { flow_id: SessionFlowId },
    #[error("superseded RESET flow {flow_id:?} already lost its absolute deadline")]
    DeadlineWon { flow_id: SessionFlowId },
    #[error("superseded RESET flow {flow_id:?} is already being rebound")]
    RebindBusy { flow_id: SessionFlowId },
    #[error("superseded RESET flow {flow_id:?} occurs more than once")]
    DuplicateSupersededFlow { flow_id: SessionFlowId },
    #[error("superseded RESET authority for flow {flow_id:?} occurs more than once")]
    DuplicateSupersededAuthority { flow_id: SessionFlowId },
    #[error("replacement recovery omitted RESET flow {flow_id:?}")]
    MissingRecoveryReset { flow_id: SessionFlowId },
    #[error("replacement recovery duplicated RESET flow {flow_id:?}")]
    DuplicateRecoveryReset { flow_id: SessionFlowId },
    #[error("replacement RESET flow {flow_id:?} already owns another root delivery authority")]
    RecoveryDeliveryAlreadyTracked { flow_id: SessionFlowId },
    #[error("replacement RESET flow {flow_id:?} delivery state escaped before publication")]
    RecoveryDeliveryAliased { flow_id: SessionFlowId },
    #[error("RESET rebind counter overflowed")]
    CounterOverflow,
    #[error("RESET supersession audit expected {expected} authorities but rescanned {actual}")]
    AuditMismatch { expected: usize, actual: usize },
}

/// Exact queue phase and cumulative recovery budget reserved before the
/// client reducer installs a replacement generation.
pub(super) struct ClientRecoveryReservation {
    queue: Weak<LegOutboundQueueIdentity>,
    endpoint: Option<Weak<LegTransportEndpoint>>,
    generation: LegGeneration,
    start_ordinal: u64,
    reserved_end_ordinal: u64,
    initial_frames: usize,
    initial_bytes: usize,
    remaining_frames: usize,
    remaining_bytes: usize,
    delivery_state: Arc<ClientRecoveryDeliveryState>,
    io_reservation: LegIoClientRecoveryReservation,
}

impl ClientRecoveryReservation {
    pub(super) fn belongs_to_queue(&self, queue: &LegOutboundQueue) -> bool {
        self.queue
            .upgrade()
            .is_some_and(|identity| Arc::ptr_eq(&identity, &queue.identity))
    }

    pub(super) fn queue_was_lost(&self) -> bool {
        self.queue.upgrade().is_none()
    }

    fn endpoint_is_open(&self) -> bool {
        self.endpoint.as_ref().is_none_or(|endpoint| {
            endpoint
                .upgrade()
                .is_some_and(|endpoint| endpoint.is_open())
        })
    }

    /// Consumed only after the reducer commit. The returned installed token
    /// deliberately has no release path back to AttachRequestPending.
    pub(super) fn into_installed(self) -> InstalledClientRecoveryReservation {
        let Self {
            queue,
            endpoint,
            generation,
            start_ordinal: _,
            reserved_end_ordinal,
            initial_frames: _,
            initial_bytes: _,
            remaining_frames,
            remaining_bytes,
            delivery_state,
            io_reservation,
        } = self;
        InstalledClientRecoveryReservation {
            queue,
            endpoint,
            generation,
            reserved_end_ordinal,
            remaining_frames,
            remaining_bytes,
            delivery_state,
            io_reservation,
        }
    }
}

impl fmt::Debug for ClientRecoveryReservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClientRecoveryReservation")
            .field("queue_live", &self.queue.upgrade().is_some())
            .field("endpoint_open", &self.endpoint_is_open())
            .field("remaining_frames", &self.remaining_frames)
            .field("remaining_bytes", &self.remaining_bytes)
            .finish()
    }
}

struct LegIoClientRecoveryReservation {
    seal: LegSeal,
    state: Arc<ClientRecoveryBudgetState>,
}

struct ClientRecoveryBudgetState {
    remaining_messages: AtomicU64,
    remaining_bytes: AtomicU64,
}

/// Post-commit queue and endpoint capacity. It can advance or become terminal,
/// but can never reactivate the pre-replacement AttachRequestPending phase.
pub(super) struct InstalledClientRecoveryReservation {
    queue: Weak<LegOutboundQueueIdentity>,
    endpoint: Option<Weak<LegTransportEndpoint>>,
    generation: LegGeneration,
    reserved_end_ordinal: u64,
    remaining_frames: usize,
    remaining_bytes: usize,
    delivery_state: Arc<ClientRecoveryDeliveryState>,
    io_reservation: LegIoClientRecoveryReservation,
}

impl InstalledClientRecoveryReservation {
    pub(super) fn belongs_to_queue(&self, queue: &LegOutboundQueue) -> bool {
        self.queue
            .upgrade()
            .is_some_and(|identity| Arc::ptr_eq(&identity, &queue.identity))
    }

    pub(super) fn queue_was_lost(&self) -> bool {
        self.queue.upgrade().is_none()
    }

    fn endpoint_is_open(&self) -> bool {
        self.endpoint.as_ref().is_none_or(|endpoint| {
            endpoint
                .upgrade()
                .is_some_and(|endpoint| endpoint.is_open())
        })
    }

    pub(super) fn discard_after_terminal_queue_loss(
        self,
        endpoint: &mut LegIo,
        _terminal: &ClientRecoveryTerminalGate,
    ) -> Result<(), Self> {
        if !self.queue_was_lost() {
            return Err(self);
        }
        let Self {
            queue,
            endpoint: endpoint_weak,
            generation,
            reserved_end_ordinal,
            remaining_frames,
            remaining_bytes,
            delivery_state,
            io_reservation,
        } = self;
        if let Err(io_reservation) = endpoint.release_client_recovery_capacity(io_reservation) {
            return Err(Self {
                queue,
                endpoint: endpoint_weak,
                generation,
                reserved_end_ordinal,
                remaining_frames,
                remaining_bytes,
                delivery_state,
                io_reservation,
            });
        }
        Ok(())
    }
}

impl fmt::Debug for InstalledClientRecoveryReservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InstalledClientRecoveryReservation")
            .field("queue_live", &self.queue.upgrade().is_some())
            .field("endpoint_open", &self.endpoint_is_open())
            .field("generation", &self.generation)
            .field("remaining_frames", &self.remaining_frames)
            .field("remaining_bytes", &self.remaining_bytes)
            .finish()
    }
}

/// Actual-delivery tail retained after every recovery frame has entered the
/// exact FIFO. The client leg may be active, while this token keeps lifetime
/// endpoint budget reclaimable if the queue or endpoint terminates before all
/// queued recovery frames are exactly delivered.
pub(super) struct ClientRecoveryTail {
    queue: Weak<LegOutboundQueueIdentity>,
    endpoint: Option<Weak<LegTransportEndpoint>>,
    generation: LegGeneration,
    delivery_state: Arc<ClientRecoveryDeliveryState>,
    io_reservation: LegIoClientRecoveryReservation,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ClientRecoveryTailInventory {
    pub(crate) live_tails: usize,
    pub(crate) undelivered_frames: u64,
    pub(crate) matching_queue: bool,
    pub(crate) matching_endpoint: bool,
    pub(crate) matching_generation: Option<LegGeneration>,
    pub(crate) endpoint_open: bool,
    pub(crate) matching_ordered_waits: usize,
    pub(crate) reservation_messages: u64,
    pub(crate) reservation_bytes: u64,
}

impl ClientRecoveryTail {
    pub(super) fn actual_delivery_complete(&self) -> bool {
        self.delivery_state
            .undelivered_frames
            .load(Ordering::Acquire)
            == 0
    }

    pub(super) fn belongs_to_queue(&self, queue: &LegOutboundQueue) -> bool {
        self.queue
            .upgrade()
            .is_some_and(|identity| Arc::ptr_eq(&identity, &queue.identity))
    }

    pub(super) fn queue_was_lost(&self) -> bool {
        self.queue.upgrade().is_none()
    }

    fn endpoint_matches_queue(&self, queue: &LegOutboundQueue) -> bool {
        match (&self.endpoint, &queue.endpoint) {
            (Some(expected), Some(actual)) => Weak::ptr_eq(expected, actual),
            (None, None) => true,
            (Some(_), None) | (None, Some(_)) => false,
        }
    }

    fn endpoint_is_open(&self) -> bool {
        self.endpoint.as_ref().is_none_or(|endpoint| {
            endpoint
                .upgrade()
                .is_some_and(|endpoint| endpoint.is_open())
        })
    }

    pub(super) fn discard_after_terminal_queue_loss(
        self,
        endpoint: &mut LegIo,
        _terminal: &ClientRecoveryTerminalGate,
    ) -> Result<(), Self> {
        if !self.queue_was_lost() {
            return Err(self);
        }
        let Self {
            queue,
            endpoint: endpoint_weak,
            generation,
            delivery_state,
            io_reservation,
        } = self;
        if let Err(io_reservation) = endpoint.release_client_recovery_capacity(io_reservation) {
            return Err(Self {
                queue,
                endpoint: endpoint_weak,
                generation,
                delivery_state,
                io_reservation,
            });
        }
        Ok(())
    }
}

impl fmt::Debug for ClientRecoveryTail {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClientRecoveryTail")
            .field("queue_live", &self.queue.upgrade().is_some())
            .field("generation", &self.generation)
            .field("actual_delivery_complete", &self.actual_delivery_complete())
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub(super) enum ClientRecoveryTailSettlementErrorKind {
    #[error("client recovery tail queue is not active")]
    QueueNotActive,
    #[error("client recovery tail belongs to another sole queue")]
    WrongQueue,
    #[error("client recovery tail belongs to another endpoint")]
    WrongEndpoint,
    #[error("client recovery tail belongs to another generation")]
    WrongGeneration,
    #[error("client recovery tail endpoint was lost")]
    EndpointLost,
    #[error("client recovery tail still owns undelivered frames")]
    DeliveryPending,
    #[error("client recovery tail reservation does not belong to this endpoint")]
    ReservationMismatch,
}

pub(super) struct ClientRecoveryTailSettlementFailure {
    tail: ClientRecoveryTail,
    kind: ClientRecoveryTailSettlementErrorKind,
}

impl ClientRecoveryTailSettlementFailure {
    pub(super) const fn kind(&self) -> ClientRecoveryTailSettlementErrorKind {
        self.kind
    }

    pub(super) fn into_tail(self) -> ClientRecoveryTail {
        self.tail
    }
}

impl fmt::Debug for ClientRecoveryTailSettlementFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClientRecoveryTailSettlementFailure")
            .field("kind", &self.kind)
            .field("tail", &self.tail)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct ClientRecoveryTailSettled {
    generation: LegGeneration,
}

impl ClientRecoveryTailSettled {
    pub(super) const fn generation(self) -> LegGeneration {
        self.generation
    }
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub(super) enum ClientRecoveryReserveError {
    #[error("client recovery request belongs to another sole queue")]
    WrongQueue,
    #[error("client recovery generation does not match its exact request")]
    WrongGeneration,
    #[error("client recovery endpoint or sole queue was lost")]
    EndpointOrQueueLost,
    #[error("client recovery endpoint does not own the exact sole queue")]
    WrongEndpoint,
    #[error("client recovery request was not actually delivered")]
    RequestNotDelivered,
    #[error("client recovery queue is not awaiting a replacement")]
    NotAwaitingReplacement,
    #[error(
        "client recovery frame cannot fit queue ({largest_frame_bytes} bytes > {queue_max_bytes})"
    )]
    FrameCannotFit {
        largest_frame_bytes: usize,
        queue_max_bytes: usize,
    },
    #[error(
        "client recovery frame cannot fit endpoint ({largest_frame_bytes} bytes > {endpoint_max_frame_bytes})"
    )]
    EndpointFrameCannotFit {
        largest_frame_bytes: usize,
        endpoint_max_frame_bytes: u64,
    },
    #[error("client recovery exceeds the endpoint lifetime message budget")]
    EndpointMessageBudgetExceeded,
    #[error("client recovery exceeds the endpoint lifetime byte budget")]
    EndpointByteBudgetExceeded,
    #[error("client recovery counter capacity is exhausted")]
    CounterOverflow,
    #[error("client recovery enqueue ordinal range is exhausted")]
    OrdinalExhausted,
    #[error("client recovery queue storage could not be reserved")]
    StorageUnavailable,
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub(super) enum ClientRecoveryEnqueueErrorKind {
    #[error("client recovery belongs to another sole queue")]
    WrongQueue,
    #[error("client recovery endpoint or sole queue was lost")]
    EndpointOrQueueLost,
    #[error("client recovery queue remains under bounded pressure")]
    Pressure,
    #[error("client recovery frame belongs to another generation")]
    GenerationMismatch,
    #[error("client recovery frame disagrees with its preflight manifest")]
    CapacityMismatch,
}

pub(super) struct ClientRecoveryEnqueueFailure {
    reservation: InstalledClientRecoveryReservation,
    prepared: PreparedClientRecoveryFrame,
    kind: ClientRecoveryEnqueueErrorKind,
}

impl ClientRecoveryEnqueueFailure {
    fn new(
        reservation: InstalledClientRecoveryReservation,
        prepared: PreparedClientRecoveryFrame,
        kind: ClientRecoveryEnqueueErrorKind,
    ) -> Self {
        Self {
            reservation,
            prepared,
            kind,
        }
    }

    pub(super) const fn kind(&self) -> ClientRecoveryEnqueueErrorKind {
        self.kind
    }

    pub(super) fn into_parts(
        self,
    ) -> (
        InstalledClientRecoveryReservation,
        PreparedClientRecoveryFrame,
    ) {
        (self.reservation, self.prepared)
    }
}

impl fmt::Debug for ClientRecoveryEnqueueFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClientRecoveryEnqueueFailure")
            .field("reservation", &self.reservation)
            .field("kind", &self.kind)
            .field("encoded_bytes", &self.prepared.encoded.len())
            .field("frame", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub(super) enum ClientRecoveryFinishErrorKind {
    #[error("client recovery belongs to another sole queue")]
    WrongQueue,
    #[error("client recovery endpoint or sole queue was lost")]
    EndpointOrQueueLost,
    #[error("client recovery still owns unqueued effects")]
    RecoveryIncomplete,
}

pub(super) struct ClientRecoveryFinishFailure {
    reservation: InstalledClientRecoveryReservation,
    kind: ClientRecoveryFinishErrorKind,
}

impl ClientRecoveryFinishFailure {
    fn new(
        reservation: InstalledClientRecoveryReservation,
        kind: ClientRecoveryFinishErrorKind,
    ) -> Self {
        Self { reservation, kind }
    }

    pub(super) const fn kind(&self) -> ClientRecoveryFinishErrorKind {
        self.kind
    }

    pub(super) fn into_reservation(self) -> InstalledClientRecoveryReservation {
        self.reservation
    }
}

impl fmt::Debug for ClientRecoveryFinishFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClientRecoveryFinishFailure")
            .field("reservation", &self.reservation)
            .field("kind", &self.kind)
            .finish()
    }
}

/// Distinct ordered receipt for the initial client ATTACH. It cannot be
/// substituted with a registered B, status retry, or catch-up request.
pub(crate) struct InitialAttachRequestEnqueued(AttachRequestEnqueued);

/// Signed initial ATTACH whose sole queue admission has not yet occurred.
impl PendingInitialAttach {
    #[allow(
        clippy::result_large_err,
        reason = "bounded pressure returns the exact signed initial ATTACH"
    )]
    pub(crate) fn enqueue(
        mut self,
        queue: &mut LegOutboundQueue,
    ) -> Result<AwaitingInitialAttach, InitialAttachEnqueueFailure> {
        match queue.push_initial_attach_request(&self.queue_seal, self.request, self.frame) {
            Ok(request_receipt) => Ok(AwaitingInitialAttach {
                pending: self.pending,
                request_receipt,
            }),
            Err(error) => {
                let kind = error.kind().clone();
                self.frame = error.into_frame();
                Err(InitialAttachEnqueueFailure {
                    pending: self,
                    kind,
                })
            }
        }
    }
}

/// Initial ATTACH queue rejection retaining the exact signed request.
pub(crate) struct InitialAttachEnqueueFailure {
    pending: PendingInitialAttach,
    kind: LegOutboundQueueErrorKind,
}

impl InitialAttachEnqueueFailure {
    pub(crate) const fn kind(&self) -> &LegOutboundQueueErrorKind {
        &self.kind
    }

    pub(crate) fn into_pending(self) -> PendingInitialAttach {
        self.pending
    }
}

impl fmt::Debug for InitialAttachEnqueueFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InitialAttachEnqueueFailure")
            .field("kind", &self.kind)
            .field("pending", &"[REDACTED]")
            .finish()
    }
}

/// Initial request whose ordered delivery and exact response provenance must
/// be consumed together.
pub(crate) struct AwaitingInitialAttach {
    pending: crate::owned_upstream::leg::PendingAttach,
    request_receipt: InitialAttachRequestEnqueued,
}

impl AwaitingInitialAttach {
    #[allow(
        clippy::result_large_err,
        reason = "response rejection returns exact pending and received ownership"
    )]
    pub(crate) fn validate_response(
        self,
        queue: &LegOutboundQueue,
        received: LegBoundFrame,
    ) -> Result<AcceptedInitialAttach, InitialAttachResponseFailure> {
        if !self.request_receipt.queue_is_live() {
            return Err(InitialAttachResponseFailure::new(
                self,
                received,
                InitialAttachResponseErrorKind::EndpointOrQueueLost,
            ));
        }
        if !self.request_receipt.belongs_to_queue(queue) {
            return Err(InitialAttachResponseFailure::new(
                self,
                received,
                InitialAttachResponseErrorKind::WrongQueue,
            ));
        }
        if !self.request_receipt.was_delivered() {
            return Err(InitialAttachResponseFailure::new(
                self,
                received,
                InitialAttachResponseErrorKind::RequestNotDelivered,
            ));
        }
        let Self {
            pending,
            request_receipt,
        } = self;
        match pending.validate_initial_acceptance_preserving(received) {
            Ok(attached) => Ok(AcceptedInitialAttach {
                attached,
                request_receipt,
            }),
            Err(failure) => {
                let (pending, received, kind) = failure.into_parts();
                Err(InitialAttachResponseFailure::new(
                    Self {
                        pending,
                        request_receipt,
                    },
                    received,
                    kind.into(),
                ))
            }
        }
    }
}

impl fmt::Debug for AwaitingInitialAttach {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AwaitingInitialAttach([REDACTED])")
    }
}

/// Queue-bound initial acceptance. The attached leg remains private until
/// the same exact request queue performs its allocation-free Active
/// transition.
pub(crate) struct AcceptedInitialAttach {
    attached: AttachedLeg,
    request_receipt: InitialAttachRequestEnqueued,
}

impl AcceptedInitialAttach {
    pub(crate) fn generation(&self) -> LegGeneration {
        self.attached.generation()
    }

    pub(crate) fn queue_is_live(&self) -> bool {
        self.request_receipt.queue_is_live()
    }

    #[allow(
        clippy::result_large_err,
        reason = "activation failure returns the exact queue-bound initial acceptance"
    )]
    fn activate_on_queue(
        self,
        queue: &mut LegOutboundQueue,
    ) -> Result<AttachedLeg, AcceptedInitialAttachActivationFailure> {
        if !self.request_receipt.belongs_to_queue(queue) {
            return Err(AcceptedInitialAttachActivationFailure::new(
                self,
                InitialAttachActivationError::WrongQueue,
            ));
        }
        if !self.request_receipt.queue_is_live() || !self.attached.transport_is_open() {
            return Err(AcceptedInitialAttachActivationFailure::new(
                self,
                InitialAttachActivationError::EndpointOrQueueLost,
            ));
        }
        if let Err(kind) = queue.finish_initial_client_attach(&self.request_receipt) {
            return Err(AcceptedInitialAttachActivationFailure::new(self, kind));
        }
        Ok(self.attached)
    }

    #[cfg(test)]
    pub(crate) fn activate_on_queue_for_test(
        self,
        queue: &mut LegOutboundQueue,
    ) -> Result<AttachedLeg, AcceptedInitialAttachActivationFailure> {
        self.activate_on_queue(queue)
    }
}

impl fmt::Debug for AcceptedInitialAttach {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AcceptedInitialAttach([REDACTED])")
    }
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub(crate) enum InitialAttachResponseErrorKind {
    #[error("initial ATTACH response belongs to another sole queue")]
    WrongQueue,
    #[error("initial ATTACH endpoint or sole queue was lost")]
    EndpointOrQueueLost,
    #[error("initial ATTACH request has not completed ordered delivery")]
    RequestNotDelivered,
    #[error("initial ATTACH response was rejected")]
    Rejected,
}

impl From<LegProvenanceError> for InitialAttachResponseErrorKind {
    fn from(_: LegProvenanceError) -> Self {
        Self::Rejected
    }
}

pub(crate) struct InitialAttachResponseFailure {
    awaiting: AwaitingInitialAttach,
    received: LegBoundFrame,
    kind: InitialAttachResponseErrorKind,
}

impl InitialAttachResponseFailure {
    fn new(
        awaiting: AwaitingInitialAttach,
        received: LegBoundFrame,
        kind: InitialAttachResponseErrorKind,
    ) -> Self {
        Self {
            awaiting,
            received,
            kind,
        }
    }

    pub(crate) const fn kind(&self) -> InitialAttachResponseErrorKind {
        self.kind
    }

    pub(crate) fn into_parts(self) -> (AwaitingInitialAttach, LegBoundFrame) {
        (self.awaiting, self.received)
    }
}

impl fmt::Debug for InitialAttachResponseFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InitialAttachResponseFailure")
            .field("kind", &self.kind)
            .field("ownership", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub(crate) enum InitialAttachActivationError {
    #[error("initial ATTACH activation belongs to another sole queue")]
    WrongQueue,
    #[error("initial ATTACH endpoint or sole queue was lost")]
    EndpointOrQueueLost,
    #[error("initial ATTACH request has not completed ordered delivery")]
    RequestNotDelivered,
    #[error("initial ATTACH queue is not awaiting its correlated response")]
    NotAwaitingInitialAttach,
}

pub(crate) struct AcceptedInitialAttachActivationFailure {
    accepted: AcceptedInitialAttach,
    kind: InitialAttachActivationError,
}

impl AcceptedInitialAttachActivationFailure {
    fn new(accepted: AcceptedInitialAttach, kind: InitialAttachActivationError) -> Self {
        Self { accepted, kind }
    }

    pub(crate) const fn kind(&self) -> InitialAttachActivationError {
        self.kind
    }

    pub(crate) fn into_accepted(self) -> AcceptedInitialAttach {
        self.accepted
    }
}

impl fmt::Debug for AcceptedInitialAttachActivationFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AcceptedInitialAttachActivationFailure")
            .field("kind", &self.kind)
            .field("accepted", &"[REDACTED]")
            .finish()
    }
}

/// Non-cloneable proof that one registered standby's exact signed ATTACH
/// entered its sole bounded FIFO without making that pre-acceptance queue
/// active.
pub(super) struct AttachRequestEnqueued {
    queue: Weak<LegOutboundQueueIdentity>,
    endpoint: Option<Weak<LegTransportEndpoint>>,
    ordinal: u64,
    request: AttachRequest,
    delivery_state: Arc<OrderedDeliveryState>,
}

/// Exact registered ATTACH request upgraded only by its authenticated
/// accepted response. This non-cloneable state is causal peer-delivery proof
/// even when the transport's redundant local completion notice is delayed.
pub(super) struct CausallyDeliveredAttachRequest {
    enqueued: AttachRequestEnqueued,
    _proof: RegisteredAttachCausalDeliveryGate,
}

/// Exact ordered request receipt for the fresh C status-retry transport.
pub(super) struct StatusRetryAttachEnqueued(AttachRequestEnqueued);

/// Exact ordered request receipt for the fresh D catch-up transport.
pub(super) struct CatchUpAttachEnqueued(AttachRequestEnqueued);

/// Exact ordered-delivery receipt for a status-only attach response.
pub(super) struct AttachStatusEnqueued {
    queue: Weak<LegOutboundQueueIdentity>,
    endpoint: Option<Weak<LegTransportEndpoint>>,
    ordinal: u64,
    generation: LegGeneration,
    session_id: SessionId,
    requested_generation: LegGeneration,
    nonce: AttachNonce,
    delivery_state: Arc<OrderedDeliveryState>,
}

/// Exclusive pre-CAS claim on one exact leg's sole empty outbound FIFO.
/// Dropping this value never changes queue state: callers must explicitly
/// release a failed transaction or transfer it into an installed publication.
pub(super) struct AttachAcceptanceReservation {
    queue: Weak<LegOutboundQueueIdentity>,
    endpoint: Option<Weak<LegTransportEndpoint>>,
    seal: LegSeal,
    ordinal: u64,
    next_ordinal: u64,
    next_owned_bytes: usize,
    delivery_state: Arc<OrderedDeliveryState>,
}

/// Ownership-preserving rejection from reserved acceptance admission.
/// Neither the exact reservation nor its correlated frame can be lost under
/// pressure, wrong-queue routing, or endpoint failure.
pub(super) struct ReservedAttachAcceptanceError {
    reservation: AttachAcceptanceReservation,
    frame: Frame,
    kind: LegOutboundQueueErrorKind,
}

impl ReservedAttachAcceptanceError {
    fn new(
        reservation: AttachAcceptanceReservation,
        frame: Frame,
        kind: LegOutboundQueueErrorKind,
    ) -> Self {
        Self {
            reservation,
            frame,
            kind,
        }
    }

    pub(super) const fn kind(&self) -> &LegOutboundQueueErrorKind {
        &self.kind
    }

    pub(super) fn into_parts(self) -> (AttachAcceptanceReservation, Frame) {
        (self.reservation, self.frame)
    }
}

impl fmt::Debug for ReservedAttachAcceptanceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReservedAttachAcceptanceError")
            .field("kind", &self.kind)
            .field("reservation", &self.reservation)
            .field("frame", &"[REDACTED]")
            .finish()
    }
}

impl AttachAcceptanceReservation {
    pub(super) fn belongs_to_queue(&self, queue: &LegOutboundQueue) -> bool {
        self.queue
            .upgrade()
            .is_some_and(|identity| Arc::ptr_eq(&identity, &queue.identity))
    }

    pub(super) fn belongs_to_leg(&self, leg: &EstablishedLeg) -> bool {
        leg.belongs_to_transport(&self.seal)
    }

    fn belongs_to_attached(&self, attached: &AttachedLeg) -> bool {
        attached.belongs_to_transport(&self.seal)
    }

    fn endpoint_matches(&self, queue: &LegOutboundQueue) -> bool {
        match (&self.endpoint, &queue.endpoint) {
            (Some(expected), Some(actual)) => Weak::ptr_eq(expected, actual),
            (None, None) => true,
            (Some(_), None) | (None, Some(_)) => false,
        }
    }

    fn matches_reserved_state(&self, queue: &LegOutboundQueue) -> bool {
        queue.session_phase == LegOutboundSessionPhase::AttachAcceptanceReserved
            && queue.frames.is_empty()
            && queue.owned_bytes == 0
            && queue.ordered_delivery_wait.is_none()
            && queue.next_enqueue_ordinal == self.ordinal
            && self.next_ordinal == self.ordinal.checked_add(1).unwrap_or(self.ordinal)
            && self.next_owned_bytes == ATTACH_ACCEPTED_ENCODED_BYTES
            && self.next_owned_bytes <= queue.max_bytes
            && queue.max_frames >= 1
    }

    pub(super) fn endpoint_is_open(&self) -> bool {
        self.endpoint.as_ref().is_none_or(|endpoint| {
            endpoint
                .upgrade()
                .is_some_and(|endpoint| endpoint.is_open())
        })
    }

    /// Restores only its exact queue to the pre-attach phase. A mismatch
    /// returns this same non-cloneable reservation and leaves both queues
    /// unchanged/fail-closed.
    pub(super) fn release(
        self,
        queue: &mut LegOutboundQueue,
    ) -> Result<(), AttachAcceptanceReservation> {
        if !self.belongs_to_queue(queue)
            || !self.endpoint_matches(queue)
            || !self.matches_reserved_state(queue)
        {
            return Err(self);
        }
        queue.session_phase = LegOutboundSessionPhase::AwaitingAttachAcceptance;
        Ok(())
    }
}

impl fmt::Debug for AttachAcceptanceReservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AttachAcceptanceReservation")
            .field("queue_live", &self.queue.upgrade().is_some())
            .field("endpoint_open", &self.endpoint_is_open())
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub(crate) enum AttachAcceptanceReserveError {
    #[error("attach acceptance reservation belongs to another leg or queue")]
    WrongLeg,
    #[error("attach acceptance reservation endpoint is terminal")]
    EndpointLost,
    #[error("attach acceptance reservation byte counter overflowed")]
    ByteCountOverflow,
    #[error("attach acceptance reservation enqueue ordinal is exhausted")]
    EnqueueOrdinalExhausted,
    #[error(
        "attach acceptance reservation capacity exceeded ({queued_frames} frames/{queued_bytes} bytes, incoming {incoming_bytes} bytes)"
    )]
    CapacityExceeded {
        queued_frames: usize,
        queued_bytes: usize,
        incoming_bytes: usize,
    },
    #[error(
        "attach acceptance reservation requires an empty awaiting queue ({queued_frames} frames/{queued_bytes} bytes; ordered pending={ordered_delivery_pending})"
    )]
    NotAwaiting {
        queued_frames: usize,
        queued_bytes: usize,
        ordered_delivery_pending: bool,
    },
}

impl AttachStatusEnqueued {
    pub(super) fn queue_is_live(&self) -> bool {
        self.queue.upgrade().is_some()
            && self.endpoint.as_ref().is_none_or(|endpoint| {
                endpoint
                    .upgrade()
                    .is_some_and(|endpoint| endpoint.is_open())
            })
    }

    pub(super) fn belongs_to_queue(&self, queue: &LegOutboundQueue) -> bool {
        self.queue
            .upgrade()
            .is_some_and(|identity| Arc::ptr_eq(&identity, &queue.identity))
    }

    pub(super) fn was_delivered(&self) -> bool {
        self.delivery_state.delivered.load(Ordering::Acquire)
    }
}

impl fmt::Debug for AttachStatusEnqueued {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let _correlation = (self.session_id, self.requested_generation, self.nonce);
        formatter
            .debug_struct("AttachStatusEnqueued")
            .field("ordinal", &self.ordinal)
            .field("generation", &self.generation)
            .field("correlation", &"[REDACTED]")
            .field("queue_live", &self.queue_is_live())
            .field("delivered", &self.was_delivered())
            .finish()
    }
}

impl AttachRequestEnqueued {
    pub(super) fn queue_is_live(&self) -> bool {
        self.queue.upgrade().is_some()
            && self.endpoint.as_ref().is_none_or(|endpoint| {
                endpoint
                    .upgrade()
                    .is_some_and(|endpoint| endpoint.is_open())
            })
    }

    pub(super) fn belongs_to_queue(&self, queue: &LegOutboundQueue) -> bool {
        self.queue
            .upgrade()
            .is_some_and(|identity| Arc::ptr_eq(&identity, &queue.identity))
    }

    pub(super) fn matches_request(&self, request: AttachRequest) -> bool {
        self.request == request
    }

    pub(super) fn was_delivered(&self) -> bool {
        self.delivery_state.delivered.load(Ordering::Acquire)
    }

    fn owns_delivery_wait(&self, wait: &OrderedDeliveryWait) -> bool {
        Arc::ptr_eq(&self.delivery_state, &wait.0)
    }

    pub(super) fn into_causally_delivered(
        self,
        proof: RegisteredAttachCausalDeliveryGate,
    ) -> CausallyDeliveredAttachRequest {
        CausallyDeliveredAttachRequest {
            enqueued: self,
            _proof: proof,
        }
    }
}

impl CausallyDeliveredAttachRequest {
    pub(super) fn queue_is_live(&self) -> bool {
        self.enqueued.queue_is_live()
    }
}

impl InitialAttachRequestEnqueued {
    pub(crate) fn queue_is_live(&self) -> bool {
        self.0.queue_is_live()
    }

    pub(crate) fn belongs_to_queue(&self, queue: &LegOutboundQueue) -> bool {
        self.0.belongs_to_queue(queue)
    }

    pub(crate) fn was_delivered(&self) -> bool {
        self.0.was_delivered()
    }
}

impl fmt::Debug for InitialAttachRequestEnqueued {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("InitialAttachRequestEnqueued")
            .field(&self.0)
            .finish()
    }
}

impl StatusRetryAttachEnqueued {
    pub(super) fn queue_is_live(&self) -> bool {
        self.0.queue_is_live()
    }

    pub(super) fn belongs_to_queue(&self, queue: &LegOutboundQueue) -> bool {
        self.0.belongs_to_queue(queue)
    }

    pub(super) fn was_delivered(&self) -> bool {
        self.0.was_delivered()
    }
}

impl fmt::Debug for StatusRetryAttachEnqueued {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("StatusRetryAttachEnqueued")
            .field(&self.0)
            .finish()
    }
}

impl CatchUpAttachEnqueued {
    pub(super) fn queue_is_live(&self) -> bool {
        self.0.queue_is_live()
    }

    pub(super) fn belongs_to_queue(&self, queue: &LegOutboundQueue) -> bool {
        self.0.belongs_to_queue(queue)
    }

    pub(super) fn was_delivered(&self) -> bool {
        self.0.was_delivered()
    }
}

impl fmt::Debug for CatchUpAttachEnqueued {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("CatchUpAttachEnqueued")
            .field(&self.0)
            .finish()
    }
}

impl fmt::Debug for AttachRequestEnqueued {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AttachRequestEnqueued")
            .field("ordinal", &self.ordinal)
            .field("generation", &self.request.requested_generation())
            .field("correlation", &"[REDACTED]")
            .field("queue_live", &self.queue_is_live())
            .field("delivered", &self.was_delivered())
            .finish()
    }
}

impl AttachAcceptanceEnqueued {
    pub(crate) fn queue_is_live(&self) -> bool {
        self.queue.upgrade().is_some()
            && self.endpoint.as_ref().is_none_or(|endpoint| {
                endpoint
                    .upgrade()
                    .is_some_and(|endpoint| endpoint.is_open())
            })
    }

    pub(crate) fn belongs_to_queue(&self, queue: &LegOutboundQueue) -> bool {
        self.queue
            .upgrade()
            .is_some_and(|identity| Arc::ptr_eq(&identity, &queue.identity))
    }

    pub(crate) fn was_delivered(&self) -> bool {
        self.delivery_state.delivered.load(Ordering::Acquire)
    }
}

impl fmt::Debug for AttachAcceptanceEnqueued {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let _redacted_correlation = (self.session_id, self.nonce);
        formatter
            .debug_struct("AttachAcceptanceEnqueued")
            .field("ordinal", &self.ordinal)
            .field("generation", &self.generation)
            .field("correlation", &"[REDACTED]")
            .field("queue_live", &self.queue_is_live())
            .field("delivered", &self.was_delivered())
            .finish()
    }
}

/// Non-cloneable proof that one exact leg-control record entered this leg's
/// sole bounded FIFO. It witnesses queue/endpoint liveness without keeping
/// either resource alive on behalf of a registration capability.
pub(super) struct LegControlEnqueued {
    queue: Weak<LegOutboundQueueIdentity>,
    endpoint: Option<Weak<LegTransportEndpoint>>,
    ordinal: u64,
    delivery_state: Arc<OrderedDeliveryState>,
}

impl LegControlEnqueued {
    pub(super) fn queue_is_live(&self) -> bool {
        self.queue.upgrade().is_some()
            && self.endpoint.as_ref().is_none_or(|endpoint| {
                endpoint
                    .upgrade()
                    .is_some_and(|endpoint| endpoint.is_open())
            })
    }

    pub(super) fn belongs_to_queue(&self, queue: &LegOutboundQueue) -> bool {
        self.queue
            .upgrade()
            .is_some_and(|identity| Arc::ptr_eq(&identity, &queue.identity))
    }

    pub(super) fn was_delivered(&self) -> bool {
        self.delivery_state.delivered.load(Ordering::Acquire)
    }
}

impl fmt::Debug for LegControlEnqueued {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LegControlEnqueued")
            .field("ordinal", &self.ordinal)
            .field("queue_live", &self.queue_is_live())
            .field("delivered", &self.was_delivered())
            .finish()
    }
}

/// Control enqueue rejection returns the exact unconsumed wire fact. Debug
/// deliberately omits all authentication and correlation fields.
pub(super) struct LegControlQueueError {
    kind: LegControlQueueErrorKind,
    frame: LegControlFrame,
}

impl LegControlQueueError {
    fn new(frame: LegControlFrame, kind: LegControlQueueErrorKind) -> Self {
        Self { kind, frame }
    }

    pub(super) const fn kind(&self) -> &LegControlQueueErrorKind {
        &self.kind
    }

    pub(super) fn into_frame(self) -> LegControlFrame {
        self.frame
    }
}

impl fmt::Debug for LegControlQueueError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LegControlQueueError")
            .field("kind", &self.kind)
            .field("frame", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum LegControlQueueErrorKind {
    InvalidFrame(ProtocolError),
    ByteCountOverflow,
    EnqueueOrdinalExhausted,
    EndpointLost,
    WrongLeg,
    AttachAcceptanceReserved,
    AttachRequestPending,
    AttachStatusPending,
    AttachRecoveryPending,
    ClientReplacementRecoveryPending,
    ClientRecoveryTerminal,
    OwnerRecoveryTerminal,
    CapacityExceeded {
        queued_frames: usize,
        queued_bytes: usize,
        incoming_bytes: usize,
    },
}

impl fmt::Debug for LegOutboundQueue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LegOutboundQueue")
            .field("max_frames", &self.max_frames)
            .field("max_bytes", &self.max_bytes)
            .field("queued_frames", &self.len())
            .field("owned_bytes", &self.owned_bytes)
            .field("session_phase", &self.session_phase)
            .field(
                "ordered_delivery_pending",
                &self.ordered_delivery_wait.is_some(),
            )
            .finish()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct OutboundSubmission {
    pub(crate) is_ack: bool,
}

/// Non-cloneable enqueue rejection that returns the exact unconsumed frame to
/// its producer. Debug deliberately omits the frame and all record content.
pub(crate) struct LegOutboundQueueError {
    kind: LegOutboundQueueErrorKind,
    frame: Frame,
}

impl LegOutboundQueueError {
    fn new(frame: Frame, kind: LegOutboundQueueErrorKind) -> Self {
        Self { kind, frame }
    }

    pub(crate) const fn kind(&self) -> &LegOutboundQueueErrorKind {
        &self.kind
    }

    pub(crate) fn into_frame(self) -> Frame {
        self.frame
    }
}

impl fmt::Debug for LegOutboundQueueError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LegOutboundQueueError")
            .field("kind", &self.kind)
            .field("frame", &"[REDACTED]")
            .finish()
    }
}

impl fmt::Display for LegOutboundQueueError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            LegOutboundQueueErrorKind::InvalidFrame(source) => {
                write!(formatter, "outbound frame is invalid: {source}")
            }
            LegOutboundQueueErrorKind::ByteCountOverflow => {
                formatter.write_str("outbound frame byte ownership overflow")
            }
            LegOutboundQueueErrorKind::EnqueueOrdinalExhausted => {
                formatter.write_str("outbound frame enqueue ordinal is exhausted")
            }
            LegOutboundQueueErrorKind::EndpointLost => {
                formatter.write_str("outbound queue transport endpoint was dropped")
            }
            LegOutboundQueueErrorKind::ResetDeliveryReceiptMismatch => formatter
                .write_str("RESET frame does not match its exact terminal delivery receipt"),
            LegOutboundQueueErrorKind::NotAttachAcceptance => {
                formatter.write_str("outbound frame is not an ATTACH_ACCEPTED record")
            }
            LegOutboundQueueErrorKind::RegisteredAttachAdmissionRequired => {
                formatter.write_str("outbound ATTACH requires registered-standby admission")
            }
            LegOutboundQueueErrorKind::AttachAcceptanceAdmissionRequired => {
                formatter.write_str("outbound ATTACH_ACCEPTED requires exact acceptance admission")
            }
            LegOutboundQueueErrorKind::AuthenticatedAttachStatusAdmissionRequired => formatter
                .write_str("outbound generation status requires authenticated status admission"),
            LegOutboundQueueErrorKind::AttachRequestPending => {
                formatter.write_str("outbound registered ATTACH response is still pending")
            }
            LegOutboundQueueErrorKind::AttachAcceptanceReserved => {
                formatter.write_str("outbound attach acceptance queue is reserved")
            }
            LegOutboundQueueErrorKind::AttachStatusPending => {
                formatter.write_str("outbound authenticated generation status is pending")
            }
            LegOutboundQueueErrorKind::ClientReplacementRecoveryPending => {
                formatter.write_str("outbound client replacement recovery is pending")
            }
            LegOutboundQueueErrorKind::ClientRecoveryTerminal => {
                formatter.write_str("outbound client recovery endpoint is terminal")
            }
            LegOutboundQueueErrorKind::OwnerRecoveryTerminal => {
                formatter.write_str("outbound owner recovery endpoint is terminal")
            }
            LegOutboundQueueErrorKind::NotAttachRequest => {
                formatter.write_str("outbound frame is not an ATTACH record")
            }
            LegOutboundQueueErrorKind::NotAttachStatus => {
                formatter.write_str("outbound frame is not an ATTACH_GENERATION_STATUS record")
            }
            LegOutboundQueueErrorKind::WrongLeg => {
                formatter.write_str("outbound frame belongs to a different transport leg")
            }
            LegOutboundQueueErrorKind::WrongAttachAcceptanceReservation => formatter
                .write_str("outbound acceptance reservation belongs to another queue or leg"),
            LegOutboundQueueErrorKind::MismatchedAttachRequest => {
                formatter.write_str("outbound ATTACH does not match its registered request")
            }
            LegOutboundQueueErrorKind::MismatchedAttachAcceptance => {
                formatter.write_str("outbound ATTACH_ACCEPTED does not match its attached leg")
            }
            LegOutboundQueueErrorKind::AttachAcceptanceNotFirst {
                next_ordinal,
                queued_frames,
                queued_bytes,
            } => write!(
                formatter,
                "outbound ATTACH_ACCEPTED must be the first session/recovery admission after completed leg-control: next ordinal {next_ordinal}, {queued_frames} frames/{queued_bytes} bytes queued"
            ),
            LegOutboundQueueErrorKind::AttachRequestNotFirst {
                next_ordinal,
                queued_frames,
                queued_bytes,
            } => write!(
                formatter,
                "outbound registered ATTACH must follow completed standby control: next ordinal {next_ordinal}, {queued_frames} frames/{queued_bytes} bytes queued"
            ),
            LegOutboundQueueErrorKind::AttachStatusNotFirst {
                next_ordinal,
                queued_frames,
                queued_bytes,
            } => write!(
                formatter,
                "outbound authenticated generation status must be first: next ordinal {next_ordinal}, {queued_frames} frames/{queued_bytes} bytes queued"
            ),
            LegOutboundQueueErrorKind::CapacityExceeded {
                queued_frames,
                queued_bytes,
                incoming_bytes,
            } => write!(
                formatter,
                "outbound frame queue capacity exceeded: {queued_frames} frames/{queued_bytes} bytes queued, incoming {incoming_bytes} bytes"
            ),
        }
    }
}

impl std::error::Error for LegOutboundQueueError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.kind {
            LegOutboundQueueErrorKind::InvalidFrame(source) => Some(source),
            LegOutboundQueueErrorKind::ByteCountOverflow
            | LegOutboundQueueErrorKind::EnqueueOrdinalExhausted
            | LegOutboundQueueErrorKind::EndpointLost
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
            | LegOutboundQueueErrorKind::AttachRequestNotFirst { .. }
            | LegOutboundQueueErrorKind::AttachStatusNotFirst { .. }
            | LegOutboundQueueErrorKind::AttachAcceptanceNotFirst { .. }
            | LegOutboundQueueErrorKind::CapacityExceeded { .. } => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum LegOutboundQueueErrorKind {
    InvalidFrame(ProtocolError),
    ByteCountOverflow,
    EnqueueOrdinalExhausted,
    EndpointLost,
    ResetDeliveryReceiptMismatch,
    NotAttachAcceptance,
    RegisteredAttachAdmissionRequired,
    AttachAcceptanceAdmissionRequired,
    AuthenticatedAttachStatusAdmissionRequired,
    AttachAcceptanceReserved,
    AttachRequestPending,
    AttachStatusPending,
    ClientReplacementRecoveryPending,
    ClientRecoveryTerminal,
    OwnerRecoveryTerminal,
    NotAttachRequest,
    NotAttachStatus,
    WrongLeg,
    WrongAttachAcceptanceReservation,
    MismatchedAttachRequest,
    MismatchedAttachAcceptance,
    AttachRequestNotFirst {
        next_ordinal: u64,
        queued_frames: usize,
        queued_bytes: usize,
    },
    AttachStatusNotFirst {
        next_ordinal: u64,
        queued_frames: usize,
        queued_bytes: usize,
    },
    AttachAcceptanceNotFirst {
        next_ordinal: u64,
        queued_frames: usize,
        queued_bytes: usize,
    },
    CapacityExceeded {
        queued_frames: usize,
        queued_bytes: usize,
        incoming_bytes: usize,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct MemoryFaultDirective {
    route: WireRoute,
    lane: WireLane,
    action: FaultAction,
}

/// Finite, preconfigured fault schedule owned by the deterministic memory
/// orchestrator, never by a session controller.
pub(crate) struct MemoryFaultScript {
    max_directives: usize,
    directives: BTreeMap<u64, MemoryFaultDirective>,
}

impl MemoryFaultScript {
    pub(crate) fn new(max_directives: usize) -> Self {
        Self {
            max_directives,
            directives: BTreeMap::new(),
        }
    }

    pub(crate) fn insert(
        &mut self,
        submission_ordinal: u64,
        route: WireRoute,
        lane: WireLane,
        action: FaultAction,
    ) -> Result<(), MemoryFaultScriptError> {
        if self.directives.contains_key(&submission_ordinal) {
            return Err(MemoryFaultScriptError::DuplicateOrdinal);
        }
        if self.directives.len() >= self.max_directives {
            return Err(MemoryFaultScriptError::CapacityExceeded);
        }
        self.directives.insert(
            submission_ordinal,
            MemoryFaultDirective {
                route,
                lane,
                action,
            },
        );
        Ok(())
    }
}

impl fmt::Debug for MemoryFaultScript {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MemoryFaultScript")
            .field("configured_directives", &self.directives.len())
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub(crate) enum MemoryFaultScriptError {
    #[error("memory fault-script capacity exceeded")]
    CapacityExceeded,
    #[error("memory fault-script submission ordinal is duplicated")]
    DuplicateOrdinal,
}

/// Byte-only deterministic adapter.  Controllers receive this only through
/// [`EncodedLegTransport`]; the test orchestrator configures the fault script
/// before constructing it and separately drives delivery/virtual time.
pub(crate) struct MemoryLegTransport {
    wire: TwoLegWire,
    fault_script: MemoryFaultScript,
    next_submission_ordinal: u64,
    ordered_deliveries: [Option<MemoryOrderedDelivery>; 4],
}

struct MemoryOrderedDelivery {
    send_ordinal: u64,
    completion: OrderedDeliveryToken,
}

impl MemoryLegTransport {
    pub(crate) fn new(wire: TwoLegWire, fault_script: MemoryFaultScript) -> Self {
        Self {
            wire,
            fault_script,
            next_submission_ordinal: 0,
            ordered_deliveries: std::array::from_fn(|_| None),
        }
    }

    pub(crate) fn advance_one_due(&mut self, through: SimTime) -> Result<bool, WireError> {
        self.wire.advance_one_due(through)
    }

    pub(crate) fn advance_idle_to(&mut self, deadline: SimTime) -> Result<(), WireError> {
        self.wire.advance_idle_to(deadline)
    }

    pub(crate) fn release_hold(
        &mut self,
        token: u64,
        release_at: SimTime,
    ) -> Result<(), WireError> {
        self.wire.release_hold(token, release_at)
    }

    pub(crate) fn try_recv_next(
        &mut self,
        route: WireRoute,
    ) -> Result<Option<EncodedDelivery>, WireError> {
        let delivery = self.wire.try_recv_next(route)?;
        let slot = &mut self.ordered_deliveries[memory_route_index(route)];
        let completed = delivery.as_ref().is_some_and(|delivery| {
            slot.as_ref()
                .is_some_and(|pending| pending.send_ordinal == delivery.send_ordinal())
        });
        if let Some(pending) = completed.then(|| slot.take()).flatten() {
            pending.completion.mark_delivered();
        }
        Ok(delivery)
    }

    pub(crate) const fn wire_counters(&self) -> WireCounters {
        self.wire.counters()
    }

    pub(crate) fn remaining_fault_directives(&self) -> usize {
        self.fault_script.directives.len()
    }

    /// Borrows only the controller-visible transmit capability.  The returned
    /// view has no fault, delivery, virtual-time, or physical-counter API.
    pub(crate) fn controller_sender(&mut self) -> MemoryLegSender<'_> {
        MemoryLegSender { transport: self }
    }

    /// Raw byte injection exists only for transport-boundary fail-closed unit
    /// tests.  Session/controller code has no matching trait operation.
    #[cfg(test)]
    fn inject_encoded_test_message(
        &mut self,
        now: SimTime,
        route: WireRoute,
        lane: WireLane,
        bytes: Vec<u8>,
    ) -> Result<(), WireError> {
        self.wire
            .send(now, route, lane, bytes, FaultAction::Pass)
            .map(|_| ())
    }
}

/// Narrow controller handle into the deterministic memory transport.
pub(crate) struct MemoryLegSender<'a> {
    transport: &'a mut MemoryLegTransport,
}

#[cfg(test)]
impl MemoryLegSender<'_> {
    fn send_encoded_inner(
        &mut self,
        now: SimTime,
        route: WireRoute,
        lane: WireLane,
        bytes: Bytes,
    ) -> Result<u64, EncodedLegTransportError> {
        if self.transport.ordered_deliveries[memory_route_index(route)].is_some() {
            return Err(EncodedLegTransportError::OrderedDeliveryPending { route });
        }
        let ordinal = self.transport.next_submission_ordinal;
        let next_ordinal = ordinal
            .checked_add(1)
            .ok_or(EncodedLegTransportError::SubmissionOrdinalExhausted)?;
        let action = if let Some(directive) = self.transport.fault_script.directives.get(&ordinal) {
            if directive.route != route || directive.lane != lane {
                return Err(EncodedLegTransportError::FaultScriptMismatch {
                    ordinal,
                    expected_route: directive.route,
                    actual_route: route,
                    expected_lane: directive.lane,
                    actual_lane: lane,
                });
            }
            directive.action
        } else {
            FaultAction::Pass
        };

        let outcome = self
            .transport
            .wire
            .send(now, route, lane, bytes, action)
            .map_err(EncodedLegTransportError::Wire)?;
        self.transport.fault_script.directives.remove(&ordinal);
        self.transport.next_submission_ordinal = next_ordinal;
        Ok(outcome.send_ordinal())
    }
}

impl EncodedLegTransport for MemoryLegSender<'_> {
    #[cfg(test)]
    fn send_encoded(
        &mut self,
        now: SimTime,
        route: WireRoute,
        lane: WireLane,
        bytes: Bytes,
    ) -> Result<(), EncodedLegTransportError> {
        self.send_encoded_inner(now, route, lane, bytes).map(|_| ())
    }

    #[cfg(test)]
    fn send_attach_ordered_encoded(
        &mut self,
        now: SimTime,
        route: WireRoute,
        lane: WireLane,
        bytes: Bytes,
        completion: OrderedDeliveryToken,
    ) -> Result<(), EncodedLegTransportError> {
        if self.transport.ordered_deliveries[memory_route_index(route)].is_some() {
            return Err(EncodedLegTransportError::OrderedDeliveryPending { route });
        }
        let ordinal = self.transport.next_submission_ordinal;
        if self
            .transport
            .fault_script
            .directives
            .get(&ordinal)
            .is_some_and(|directive| {
                directive.route == route
                    && directive.lane == lane
                    && matches!(
                        directive.action,
                        FaultAction::Drop | FaultAction::Duplicate | FaultAction::ReorderAdjacent
                    )
            })
        {
            return Err(EncodedLegTransportError::UnreliableOrderedFault { ordinal });
        }
        let send_ordinal = self.send_encoded_inner(now, route, lane, bytes)?;
        self.transport.ordered_deliveries[memory_route_index(route)] =
            Some(MemoryOrderedDelivery {
                send_ordinal,
                completion,
            });
        Ok(())
    }
}

const fn memory_route_index(route: WireRoute) -> usize {
    match (route.leg(), route.direction()) {
        (LegId::A, WireDirection::ClientToOwner) => 0,
        (LegId::A, WireDirection::OwnerToClient) => 1,
        (LegId::B, WireDirection::ClientToOwner) => 2,
        (LegId::B, WireDirection::OwnerToClient) => 3,
    }
}

impl fmt::Debug for MemoryLegSender<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("MemoryLegSender([REDACTED])")
    }
}

impl fmt::Debug for MemoryLegTransport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MemoryLegTransport")
            .field("next_submission_ordinal", &self.next_submission_ordinal)
            .field(
                "remaining_fault_directives",
                &self.remaining_fault_directives(),
            )
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub(crate) enum EncodedLegTransportError {
    #[error("encoded leg wire operation failed: {0}")]
    Wire(WireError),
    #[error(
        "fault script mismatch at submission {ordinal}: expected {expected_route:?}/{expected_lane:?}, got {actual_route:?}/{actual_lane:?}"
    )]
    FaultScriptMismatch {
        ordinal: u64,
        expected_route: WireRoute,
        actual_route: WireRoute,
        expected_lane: WireLane,
        actual_lane: WireLane,
    },
    #[error("encoded transport has not implemented ordered leg delivery completion")]
    OrderedSubmissionUnsupported,
    #[error("encoded transport submission requires a queue-minted opaque capability")]
    SubmissionCapabilityUnsupported,
    #[error("ordered delivery remains pending on route {route:?}")]
    OrderedDeliveryPending { route: WireRoute },
    #[error("fault action at submission {ordinal} cannot preserve ordered leg delivery")]
    UnreliableOrderedFault { ordinal: u64 },
    #[error("encoded leg submission ordinal exhausted")]
    SubmissionOrdinalExhausted,
    #[error("fixed Quinn actor outbound count or byte capacity is full")]
    ActorPressure,
    #[error("fixed Quinn actor outbound lane is closed")]
    ActorClosed,
}

impl LegIo {
    fn authenticated_transport_parts(
        leg_id: LegId,
        role: LegEndpointRole,
        binding: AttachTransportBinding,
        physical_incarnation: Option<PhysicalLegPolicyIncarnation>,
        limits: LegIoLimits,
    ) -> (Self, LegTransportReporter) {
        let (established, reporter) =
            EstablishedLeg::for_leg_io_authenticated_transport_with_reporter(
                LegIoAuthenticatedTransportGate::mint_for_leg_io(),
                binding,
            );
        (
            Self {
                leg_id,
                role,
                established,
                physical_incarnation,
                limits,
                counters: LegIoCounters::default(),
                reserved_outbound_messages: 0,
                reserved_outbound_bytes: 0,
            },
            reporter,
        )
    }

    /// Fixed Quinn actor construction always transfers the sole terminal
    /// reporter.  Unlike the deterministic compatibility constructor below,
    /// this signature is identical in test and release builds.
    pub(super) fn for_quinn_leg_actor(
        _gate: QuinnLegActorTransportGate,
        physical_incarnation: PhysicalLegPolicyIncarnation,
        leg_id: LegId,
        role: LegEndpointRole,
        binding: AttachTransportBinding,
        limits: LegIoLimits,
    ) -> (Self, LegTransportReporter) {
        Self::authenticated_transport_parts(
            leg_id,
            role,
            binding,
            Some(physical_incarnation),
            limits,
        )
    }

    /// Legacy deterministic harness construction cannot exist in production.
    #[cfg(test)]
    pub(crate) fn for_authenticated_transport(
        leg_id: LegId,
        role: LegEndpointRole,
        binding: AttachTransportBinding,
        limits: LegIoLimits,
    ) -> Self {
        Self::authenticated_transport_parts(leg_id, role, binding, None, limits).0
    }

    #[cfg(test)]
    pub(crate) fn for_authenticated_transport_with_reporter(
        leg_id: LegId,
        role: LegEndpointRole,
        binding: AttachTransportBinding,
        limits: LegIoLimits,
    ) -> (Self, LegTransportReporter) {
        Self::authenticated_transport_parts(leg_id, role, binding, None, limits)
    }

    pub(super) fn belongs_to_physical_actor(
        &self,
        runtime: &PhysicalLegRuntimeIncarnation,
        _gate: &QuinnLegCompositionGate,
    ) -> bool {
        self.physical_incarnation
            .as_ref()
            .is_some_and(|policy| runtime.matches_policy(policy))
    }

    pub(crate) const fn outbound_route(&self) -> WireRoute {
        WireRoute::new(self.leg_id, self.role.outbound_direction())
    }

    pub(crate) const fn inbound_route(&self) -> WireRoute {
        WireRoute::new(self.leg_id, self.role.inbound_direction())
    }

    pub(crate) const fn established_leg(&self) -> &EstablishedLeg {
        &self.established
    }

    pub(crate) const fn counters(&self) -> LegIoCounters {
        self.counters
    }

    pub(crate) const fn client_recovery_budget_snapshot(&self) -> ClientRecoveryBudgetSnapshot {
        ClientRecoveryBudgetSnapshot {
            reserved_outbound_messages: self.reserved_outbound_messages,
            reserved_outbound_bytes: self.reserved_outbound_bytes,
        }
    }

    /// Encodes before the byte-only wire can apply a fault action.
    #[cfg(test)]
    pub(crate) fn send_frame<T: EncodedLegTransport + ?Sized>(
        &mut self,
        transport: &mut T,
        now: SimTime,
        frame: Frame,
    ) -> Result<(), LegIoError> {
        self.send_retained_frame(
            transport,
            now,
            &frame,
            None,
            &OutboundBudget::Ordinary,
            None,
        )
    }

    /// Attempts one encoded submission while ownership of the protocol frame
    /// remains with the caller. A bounded controller queue can therefore retry
    /// the exact same DATA/control frame after transient transport pressure;
    /// endpoint counters advance only after the transport accepts it.
    fn send_retained_frame<T: EncodedLegTransport + ?Sized>(
        &mut self,
        transport: &mut T,
        now: SimTime,
        frame: &Frame,
        ordered_completion: Option<OrderedSessionDeliveryToken>,
        budget: &OutboundBudget,
        expected_bytes: Option<usize>,
    ) -> Result<(), LegIoError> {
        let lane = lane_for_record(frame.record());
        let encoded = frame.encode().map_err(LegIoError::Encode)?;
        if matches!(budget, OutboundBudget::ClientRecoveryReserved(_))
            && expected_bytes != Some(encoded.len())
        {
            return Err(LegIoError::ClientRecoveryReservationMismatch);
        }
        let encoded_len = u64::try_from(encoded.len()).map_err(|_| LegIoError::CounterOverflow)?;
        let (messages, bytes) = match budget {
            OutboundBudget::Ordinary => self.preflight_outbound(encoded_len)?,
            OutboundBudget::ClientRecoveryReserved(state) => {
                self.preflight_reserved_client_recovery(state, encoded_len)?
            }
        };
        transport
            .submit_encoded(EncodedLegSubmission {
                now,
                route: self.outbound_route(),
                lane,
                bytes: encoded,
                ordered_completion: ordered_completion.map(OrderedDeliveryToken::Session),
            })
            .map_err(LegIoError::Transport)?;
        if let OutboundBudget::ClientRecoveryReserved(state) = budget {
            let previous_messages = state.remaining_messages.fetch_sub(1, Ordering::AcqRel);
            let previous_bytes = state
                .remaining_bytes
                .fetch_sub(encoded_len, Ordering::AcqRel);
            debug_assert!(previous_messages >= 1 && previous_bytes >= encoded_len);
            self.reserved_outbound_messages = self
                .reserved_outbound_messages
                .checked_sub(1)
                .ok_or(LegIoError::CounterOverflow)?;
            self.reserved_outbound_bytes = self
                .reserved_outbound_bytes
                .checked_sub(encoded_len)
                .ok_or(LegIoError::CounterOverflow)?;
        }
        self.counters.outbound_messages = messages;
        self.counters.outbound_bytes = bytes;
        match lane {
            WireLane::Control => {
                self.counters.outbound_control_messages += 1;
            }
            WireLane::Data => {
                self.counters.outbound_data_messages += 1;
            }
        }
        Ok(())
    }

    /// Submits the exact bytes and completion state minted by recovery
    /// preflight. `Bytes::clone` at the queue boundary retains the same
    /// backing allocation, so transport pressure cannot force a second
    /// encoding, copy, or completion allocation after generation commit.
    fn send_retained_preencoded_frame<T: EncodedLegTransport + ?Sized>(
        &mut self,
        transport: &mut T,
        now: SimTime,
        frame: &Frame,
        encoded: Bytes,
        ordered_completion: OrderedSessionDeliveryToken,
        budget: &OutboundBudget,
        expected_bytes: usize,
    ) -> Result<(), LegIoError> {
        debug_assert!(matches!(budget, OutboundBudget::ClientRecoveryReserved(_)));
        if encoded.len() != expected_bytes {
            return Err(LegIoError::ClientRecoveryReservationMismatch);
        }
        let encoded_len = u64::try_from(encoded.len()).map_err(|_| LegIoError::CounterOverflow)?;
        let OutboundBudget::ClientRecoveryReserved(state) = budget else {
            return Err(LegIoError::ClientRecoveryReservationMismatch);
        };
        let (messages, bytes) = self.preflight_reserved_client_recovery(state, encoded_len)?;
        let lane = lane_for_record(frame.record());
        transport
            .submit_encoded(EncodedLegSubmission {
                now,
                route: self.outbound_route(),
                lane,
                bytes: encoded,
                ordered_completion: Some(OrderedDeliveryToken::Session(ordered_completion)),
            })
            .map_err(LegIoError::Transport)?;
        let previous_messages = state.remaining_messages.fetch_sub(1, Ordering::AcqRel);
        let previous_bytes = state
            .remaining_bytes
            .fetch_sub(encoded_len, Ordering::AcqRel);
        debug_assert!(previous_messages >= 1 && previous_bytes >= encoded_len);
        self.reserved_outbound_messages = self
            .reserved_outbound_messages
            .checked_sub(1)
            .ok_or(LegIoError::CounterOverflow)?;
        self.reserved_outbound_bytes = self
            .reserved_outbound_bytes
            .checked_sub(encoded_len)
            .ok_or(LegIoError::CounterOverflow)?;
        self.counters.outbound_messages = messages;
        self.counters.outbound_bytes = bytes;
        match lane {
            WireLane::Control => self.counters.outbound_control_messages += 1,
            WireLane::Data => self.counters.outbound_data_messages += 1,
        }
        Ok(())
    }

    fn send_retained_control_frame<T: EncodedLegTransport + ?Sized>(
        &mut self,
        transport: &mut T,
        now: SimTime,
        frame: &LegControlFrame,
        features: FeatureSet,
        ordered_completion: OrderedLegControlDeliveryToken,
    ) -> Result<(), LegIoError> {
        let encoded = frame.encode(features).map_err(LegIoError::Encode)?;
        let encoded_len = u64::try_from(encoded.len()).map_err(|_| LegIoError::CounterOverflow)?;
        let (messages, bytes) = self.preflight_outbound(encoded_len)?;
        transport
            .submit_encoded(EncodedLegSubmission {
                now,
                route: self.outbound_route(),
                lane: WireLane::Control,
                bytes: encoded,
                ordered_completion: Some(OrderedDeliveryToken::LegControl(ordered_completion)),
            })
            .map_err(LegIoError::Transport)?;
        self.counters.outbound_messages = messages;
        self.counters.outbound_bytes = bytes;
        self.counters.outbound_control_messages = self
            .counters
            .outbound_control_messages
            .checked_add(1)
            .ok_or(LegIoError::CounterOverflow)?;
        Ok(())
    }

    /// Validates transport metadata, decodes one exact byte message, validates
    /// its production lane, and only then mints this endpoint's provenance.
    pub(crate) fn receive_delivery(
        &mut self,
        delivery: EncodedDelivery,
    ) -> Result<LegBoundFrame, LegIoError> {
        match self.receive_classified_delivery(delivery, FeatureSet::default())? {
            BoundInbound::Session(frame) => Ok(frame),
            BoundInbound::LegControl(_) => Err(LegIoError::UnexpectedLegControl),
        }
    }

    /// Feature-aware classified receive boundary. The installed feature set
    /// is supplied by local authenticated session state, never taken from the
    /// incoming record itself.
    pub(crate) fn receive_classified_delivery(
        &mut self,
        delivery: EncodedDelivery,
        installed_features: FeatureSet,
    ) -> Result<BoundInbound, LegIoError> {
        let actual_route = delivery.route();
        let actual_lane = delivery.lane();
        let encoded = delivery.into_bytes();
        self.receive_classified_parts(actual_route, actual_lane, encoded, installed_features)
    }

    /// Actor-only ingress boundary for one record read from the fixed Quinn
    /// leg. The private gate mint prevents sibling adapters from presenting
    /// caller-created bytes as authenticated actor ingress.
    pub(crate) fn receive_quinn_actor_delivery(
        &mut self,
        _gate: &QuinnLegActorInboundGate,
        actual_route: WireRoute,
        actual_lane: WireLane,
        encoded: Bytes,
        installed_features: FeatureSet,
    ) -> Result<BoundInbound, LegIoError> {
        self.receive_classified_parts(actual_route, actual_lane, encoded, installed_features)
    }

    fn receive_classified_parts(
        &mut self,
        actual_route: WireRoute,
        actual_lane: WireLane,
        encoded: Bytes,
        installed_features: FeatureSet,
    ) -> Result<BoundInbound, LegIoError> {
        let encoded_len = u64::try_from(encoded.len()).map_err(|_| LegIoError::CounterOverflow)?;
        let (messages, bytes) = self.preflight_inbound(encoded_len)?;
        self.counters.inbound_messages = messages;
        self.counters.inbound_bytes = bytes;

        let expected_route = self.inbound_route();
        if actual_route != expected_route {
            self.counters.wrong_route_rejections += 1;
            return Err(LegIoError::WrongRoute {
                expected: expected_route,
                actual: actual_route,
            });
        }

        let decoded = match DecodedFrame::decode_owned_exact(encoded, installed_features) {
            Ok(frame) => frame,
            Err(error) => {
                self.counters.decode_rejections += 1;
                return Err(LegIoError::Decode(error));
            }
        };
        let expected_lane = match &decoded {
            DecodedFrame::Session(frame) => lane_for_record(frame.record()),
            DecodedFrame::LegControl(_) => WireLane::Control,
        };
        if actual_lane != expected_lane {
            self.counters.wrong_lane_rejections += 1;
            return Err(LegIoError::WrongLane {
                expected: expected_lane,
                actual: actual_lane,
            });
        }

        self.counters.bound_messages += 1;
        self.counters.bound_bytes = self
            .counters
            .bound_bytes
            .checked_add(encoded_len)
            .ok_or(LegIoError::CounterOverflow)?;
        match decoded {
            DecodedFrame::Session(frame) => {
                self.counters.bound_session_messages = self
                    .counters
                    .bound_session_messages
                    .checked_add(1)
                    .ok_or(LegIoError::CounterOverflow)?;
                Ok(BoundInbound::Session(
                    self.established.bind_received_frame(frame),
                ))
            }
            DecodedFrame::LegControl(frame) => {
                self.counters.bound_leg_control_messages = self
                    .counters
                    .bound_leg_control_messages
                    .checked_add(1)
                    .ok_or(LegIoError::CounterOverflow)?;
                Ok(BoundInbound::LegControl(
                    self.established.bind_received_control_frame(frame),
                ))
            }
        }
    }

    fn reserve_client_recovery_capacity(
        &mut self,
        seal: &LegSeal,
        capacity: ClientRecoveryCapacity,
    ) -> Result<LegIoClientRecoveryReservation, ClientRecoveryReserveError> {
        if !self.established.belongs_to_transport(seal) {
            return Err(ClientRecoveryReserveError::WrongEndpoint);
        }
        let largest_frame_bytes = u64::try_from(capacity.largest_frame_bytes)
            .map_err(|_| ClientRecoveryReserveError::CounterOverflow)?;
        if largest_frame_bytes > self.limits.max_frame_bytes {
            return Err(ClientRecoveryReserveError::EndpointFrameCannotFit {
                largest_frame_bytes: capacity.largest_frame_bytes,
                endpoint_max_frame_bytes: self.limits.max_frame_bytes,
            });
        }
        let frame_count = u64::try_from(capacity.frame_count)
            .map_err(|_| ClientRecoveryReserveError::CounterOverflow)?;
        let encoded_bytes = u64::try_from(capacity.encoded_bytes)
            .map_err(|_| ClientRecoveryReserveError::CounterOverflow)?;
        let reserved_messages = self
            .reserved_outbound_messages
            .checked_add(frame_count)
            .ok_or(ClientRecoveryReserveError::CounterOverflow)?;
        let admitted_messages = self
            .counters
            .outbound_messages
            .checked_add(reserved_messages)
            .ok_or(ClientRecoveryReserveError::CounterOverflow)?;
        if admitted_messages > self.limits.max_outbound_messages {
            return Err(ClientRecoveryReserveError::EndpointMessageBudgetExceeded);
        }
        let reserved_bytes = self
            .reserved_outbound_bytes
            .checked_add(encoded_bytes)
            .ok_or(ClientRecoveryReserveError::CounterOverflow)?;
        let admitted_bytes = self
            .counters
            .outbound_bytes
            .checked_add(reserved_bytes)
            .ok_or(ClientRecoveryReserveError::CounterOverflow)?;
        if admitted_bytes > self.limits.max_outbound_bytes {
            return Err(ClientRecoveryReserveError::EndpointByteBudgetExceeded);
        }
        self.reserved_outbound_messages = reserved_messages;
        self.reserved_outbound_bytes = reserved_bytes;
        Ok(LegIoClientRecoveryReservation {
            seal: seal.share(),
            state: Arc::new(ClientRecoveryBudgetState {
                remaining_messages: AtomicU64::new(frame_count),
                remaining_bytes: AtomicU64::new(encoded_bytes),
            }),
        })
    }

    fn release_client_recovery_capacity(
        &mut self,
        reservation: LegIoClientRecoveryReservation,
    ) -> Result<(), LegIoClientRecoveryReservation> {
        let messages = reservation.state.remaining_messages.load(Ordering::Acquire);
        let bytes = reservation.state.remaining_bytes.load(Ordering::Acquire);
        if !self.established.belongs_to_transport(&reservation.seal)
            || messages > self.reserved_outbound_messages
            || bytes > self.reserved_outbound_bytes
        {
            return Err(reservation);
        }
        reservation
            .state
            .remaining_messages
            .store(0, Ordering::Release);
        reservation
            .state
            .remaining_bytes
            .store(0, Ordering::Release);
        self.reserved_outbound_messages -= messages;
        self.reserved_outbound_bytes -= bytes;
        Ok(())
    }

    fn preflight_reserved_client_recovery(
        &self,
        state: &ClientRecoveryBudgetState,
        len: u64,
    ) -> Result<(u64, u64), LegIoError> {
        if len > self.limits.max_frame_bytes {
            return Err(LegIoError::FrameTooLarge {
                len,
                max: self.limits.max_frame_bytes,
            });
        }
        if self.reserved_outbound_messages == 0
            || len > self.reserved_outbound_bytes
            || state.remaining_messages.load(Ordering::Acquire) == 0
            || len > state.remaining_bytes.load(Ordering::Acquire)
        {
            return Err(LegIoError::ClientRecoveryReservationMismatch);
        }
        let messages = self
            .counters
            .outbound_messages
            .checked_add(1)
            .ok_or(LegIoError::CounterOverflow)?;
        let bytes = self
            .counters
            .outbound_bytes
            .checked_add(len)
            .ok_or(LegIoError::CounterOverflow)?;
        if messages > self.limits.max_outbound_messages || bytes > self.limits.max_outbound_bytes {
            return Err(LegIoError::ClientRecoveryReservationMismatch);
        }
        Ok((messages, bytes))
    }

    fn preflight_outbound(&self, len: u64) -> Result<(u64, u64), LegIoError> {
        if len > self.limits.max_frame_bytes {
            return Err(LegIoError::FrameTooLarge {
                len,
                max: self.limits.max_frame_bytes,
            });
        }
        let messages = self
            .counters
            .outbound_messages
            .checked_add(1)
            .ok_or(LegIoError::CounterOverflow)?;
        let admitted_messages = messages
            .checked_add(self.reserved_outbound_messages)
            .ok_or(LegIoError::CounterOverflow)?;
        if admitted_messages > self.limits.max_outbound_messages {
            return Err(LegIoError::OutboundMessageBudgetExceeded);
        }
        let bytes = self
            .counters
            .outbound_bytes
            .checked_add(len)
            .ok_or(LegIoError::CounterOverflow)?;
        let admitted_bytes = bytes
            .checked_add(self.reserved_outbound_bytes)
            .ok_or(LegIoError::CounterOverflow)?;
        if admitted_bytes > self.limits.max_outbound_bytes {
            return Err(LegIoError::OutboundByteBudgetExceeded);
        }
        Ok((messages, bytes))
    }

    fn preflight_inbound(&self, len: u64) -> Result<(u64, u64), LegIoError> {
        if len > self.limits.max_frame_bytes {
            return Err(LegIoError::FrameTooLarge {
                len,
                max: self.limits.max_frame_bytes,
            });
        }
        let messages = self
            .counters
            .inbound_messages
            .checked_add(1)
            .ok_or(LegIoError::CounterOverflow)?;
        if messages > self.limits.max_inbound_messages {
            return Err(LegIoError::InboundMessageBudgetExceeded);
        }
        let bytes = self
            .counters
            .inbound_bytes
            .checked_add(len)
            .ok_or(LegIoError::CounterOverflow)?;
        if bytes > self.limits.max_inbound_bytes {
            return Err(LegIoError::InboundByteBudgetExceeded);
        }
        Ok((messages, bytes))
    }
}

impl fmt::Debug for LegIo {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("LegIo([REDACTED])")
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub(crate) enum LegIoError {
    #[error("leg frame encoding failed: {0}")]
    Encode(ProtocolError),
    #[error("leg frame decoding failed: {0}")]
    Decode(ProtocolError),
    #[error("leg encoded transport operation failed: {0}")]
    Transport(EncodedLegTransportError),
    #[error("leg encoded frame is too large: {len} bytes exceeds {max}")]
    FrameTooLarge { len: u64, max: u64 },
    #[error("leg outbound message budget exceeded")]
    OutboundMessageBudgetExceeded,
    #[error("leg outbound byte budget exceeded")]
    OutboundByteBudgetExceeded,
    #[error("leg inbound message budget exceeded")]
    InboundMessageBudgetExceeded,
    #[error("leg inbound byte budget exceeded")]
    InboundByteBudgetExceeded,
    #[error("leg I/O counter overflow")]
    CounterOverflow,
    #[error("classified leg-control record reached the session-only receive API")]
    UnexpectedLegControl,
    #[error("outbound queue belongs to a different authenticated transport leg")]
    WrongOutboundQueueLeg,
    #[error("outbound queue's authenticated transport endpoint was dropped")]
    OutboundQueueEndpointLost,
    #[error("outbound queue is waiting for exact ordered transport delivery")]
    OrderedDeliveryPending,
    #[error("client recovery endpoint reservation disagrees with its queued frame")]
    ClientRecoveryReservationMismatch,
    #[error("encoded delivery used wrong route: expected {expected:?}, got {actual:?}")]
    WrongRoute {
        expected: WireRoute,
        actual: WireRoute,
    },
    #[error("encoded delivery used wrong lane: expected {expected:?}, got {actual:?}")]
    WrongLane {
        expected: WireLane,
        actual: WireLane,
    },
}

const fn lane_for_record(record: &Record) -> WireLane {
    match record {
        Record::Data { .. } | Record::Close { .. } => WireLane::Data,
        Record::Attach { .. }
        | Record::AttachAccepted { .. }
        | Record::AttachGenerationStatus { .. }
        | Record::Open { .. }
        | Record::OpenResult { .. }
        | Record::Ack { .. }
        | Record::Reset { .. } => WireLane::Control,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::owned_upstream::leg::{
        AttachResponse, CaughtUpAttachedLeg, CaughtUpLegTerminalMismatch, ExactLegTerminal,
        LegProvenanceError, LegTransportReporter, LegTransportTerminalReason, PendingLegLoss,
    };
    use crate::owned_upstream::supervisor::SessionSupervisor;
    use crate::owned_upstream::two_leg::{WireBounds, WireCapacity, WireCapacitySpec};
    use crate::resumable::{
        AttachAlpn, AttachAuthority, AttachCredentials, AttachNonce, AttachPolicy, AttachRequest,
        AttachTransportBinding, ByteOffset, DevicePrincipal, DeviceSecret, Direction, FeatureOffer,
        FeatureSet, Frame, LegControlRecord, LegGeneration, OwnerIdentity, ReceiveBudgetLimits,
        Record, ReplayBudgetLimits, ResumeSecret, SESSION_PROTOCOL_VERSION, STANDBY_CONTROL_V1,
        SessionConfig, SessionFlowId, SessionId, StandbyNonce, TcpWindowLimits, TlsExporterBinding,
        VersionRange,
    };
    use std::time::Duration;

    fn binding() -> AttachTransportBinding {
        AttachTransportBinding::new(
            OwnerIdentity::new([0x31; 32]).unwrap(),
            AttachAlpn::new(b"mini-vpn-owned/1").unwrap(),
            TlsExporterBinding::new([0x42; 32]).unwrap(),
            DevicePrincipal::new([0x53; 16]).unwrap(),
        )
    }

    fn request() -> AttachRequest {
        AttachRequest::new(
            SessionId::new([0x11; 16]).unwrap(),
            LegGeneration::new(2).unwrap(),
            AttachNonce::new([0x22; 16]).unwrap(),
            VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION).unwrap(),
            FeatureOffer::new(0b111, 0b001).unwrap(),
        )
    }

    fn credentials() -> AttachCredentials {
        AttachCredentials::new(
            DeviceSecret::new([0x71; 32]).unwrap(),
            ResumeSecret::new([0x72; 32]).unwrap(),
        )
        .unwrap()
    }

    fn authority() -> AttachAuthority {
        AttachAuthority::new(
            request().session_id(),
            LegGeneration::new(1).unwrap(),
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

    fn wire_capacity() -> WireCapacity {
        WireCapacitySpec::new(80_000, Duration::from_secs(1), 256, 128, 1)
            .unwrap()
            .with_fault_copies(1, 1)
            .unwrap()
            .derive()
            .unwrap()
    }

    fn wire() -> TwoLegWire {
        TwoLegWire::new(WireBounds::new(wire_capacity(), 64, 32_000, 4, 4).unwrap())
    }

    fn memory_transport() -> MemoryLegTransport {
        MemoryLegTransport::new(wire(), MemoryFaultScript::new(0))
    }

    fn release_due_events_through(wire: &mut MemoryLegTransport, through: SimTime) {
        while wire.advance_one_due(through).unwrap() {}
        wire.advance_idle_to(through).unwrap();
    }

    fn limits() -> LegIoLimits {
        LegIoLimits::from_wire_capacity(wire_capacity(), 1_024).unwrap()
    }

    fn endpoint(leg: LegId, role: LegEndpointRole) -> LegIo {
        LegIo::for_authenticated_transport(leg, role, binding(), limits())
    }

    fn send(
        endpoint: &mut LegIo,
        transport: &mut MemoryLegTransport,
        now: SimTime,
        frame: Frame,
    ) -> Result<(), LegIoError> {
        endpoint.send_frame(&mut transport.controller_sender(), now, frame)
    }

    fn data_frame(offset: u64, payload: &'static [u8]) -> Frame {
        Frame::try_new(
            LegGeneration::new(2).unwrap(),
            Record::Data {
                flow_id: SessionFlowId::new(7).unwrap(),
                direction: Direction::TargetToClient,
                offset: ByteOffset::new(offset),
                payload: Bytes::from_static(payload),
            },
        )
        .unwrap()
    }

    #[test]
    fn prepared_ordinary_frame_reservation_is_logically_inert_until_infallible_commit() {
        let frame = data_frame(0, b"reserved-open-slot");
        let encoded_len = frame.encode().unwrap().len();
        let mut queue = LegOutboundQueue::new(2, encoded_len * 2).unwrap();
        let allocated_capacity = queue.frames.capacity();
        assert!(allocated_capacity >= 2);

        let reservation = queue.prepare_ordinary_frame(frame).unwrap();
        let frame = reservation.into_frame();
        assert!(queue.is_empty());
        assert_eq!(queue.owned_bytes, 0);
        assert_eq!(queue.next_enqueue_ordinal, 1);
        assert_eq!(
            queue.session_phase,
            LegOutboundSessionPhase::AwaitingAttachAcceptance
        );
        assert_eq!(queue.frames.capacity(), allocated_capacity);

        let reservation = queue.prepare_ordinary_frame(frame).unwrap();
        assert_eq!(reservation.commit(), 1);
        assert_eq!(queue.len(), 1);
        assert_eq!(queue.owned_bytes, encoded_len);
        assert_eq!(queue.next_enqueue_ordinal, 2);
        assert_eq!(queue.session_phase, LegOutboundSessionPhase::Active);
        assert_eq!(queue.frames.capacity(), allocated_capacity);
    }

    #[test]
    fn exact_reset_delivery_receipt_linearizes_delivered_against_deadline() {
        let flow_id = SessionFlowId::new(7).unwrap();
        let reset = Frame::try_new(
            LegGeneration::new(2).unwrap(),
            Record::Reset {
                flow_id,
                reason: crate::resumable::ResetReason::LocalAbandon,
            },
        )
        .unwrap();
        let max_bytes = reset.encode().unwrap().len();
        let timer_receipt = OrderedResetDeliveryReceipt::pending(flow_id);
        let queue_receipt = timer_receipt.clone_for_queue();
        let mut queue = LegOutboundQueue::new(1, max_bytes).unwrap();
        queue.session_phase = LegOutboundSessionPhase::Active;
        queue
            .prepare_ordinary_frame_with_reset_delivery(reset, Some(queue_receipt))
            .unwrap()
            .commit();
        let delivery_state = Arc::clone(
            queue.frames[0]
                .delivery_state
                .as_ref()
                .expect("tracked RESET moves the pointer-identical state into the FIFO"),
        );
        assert!(Arc::ptr_eq(
            delivery_state
                .tracked_reset
                .as_ref()
                .expect("tracked RESET keeps the pointer-identical root state"),
            &timer_receipt.state,
        ));

        OrderedDeliveryToken::Session(OrderedSessionDeliveryToken(delivery_state)).mark_delivered();
        assert_eq!(
            timer_receipt.try_commit_deadline(),
            OrderedResetDeadlineCommit::Delivered,
        );

        let deadline_receipt = OrderedResetDeliveryReceipt::pending(flow_id);
        let late_delivery = deadline_receipt.clone_for_queue().into_state();
        assert_eq!(
            deadline_receipt.try_commit_deadline(),
            OrderedResetDeadlineCommit::DeadlineWon,
        );
        OrderedDeliveryToken::Session(OrderedSessionDeliveryToken(late_delivery)).mark_delivered();
        assert_eq!(
            deadline_receipt.try_commit_deadline(),
            OrderedResetDeadlineCommit::DeadlineWon,
            "a late DELIVERED completion cannot rewrite the absolute-deadline winner",
        );

        let interleaved_receipt = OrderedResetDeliveryReceipt::pending(flow_id);
        let interleaved_state = interleaved_receipt.clone_for_queue().into_state();
        let interleaved_wait = OrderedDeliveryWait(Arc::clone(&interleaved_state));
        interleaved_state
            .tracked_reset
            .as_ref()
            .expect("tracked RESET owns one root state")
            .deadline_state
            .compare_exchange(
                RESET_DELIVERY_PENDING,
                RESET_DELIVERY_DELIVERED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .expect("the exact delivery CAS wins before the generic bool update");
        assert!(!interleaved_state.delivered.load(Ordering::Acquire));
        assert!(
            !interleaved_wait.is_delivered(),
            "a root-level Delivered decision cannot forge this attempt's FIFO delivery",
        );
        OrderedDeliveryToken::Session(OrderedSessionDeliveryToken(interleaved_state))
            .mark_delivered();
        assert!(interleaved_wait.is_delivered());
    }

    #[test]
    fn prepared_ordinary_frame_reservation_owns_only_the_exact_admitted_frame() {
        let reserved = data_frame(0, b"x");
        let substitute = data_frame(1, b"a much larger same-generation frame");
        let reserved_len = reserved.encode().unwrap().len();
        let substitute_len = substitute.encode().unwrap().len();
        let reserved_ptr = match reserved.record() {
            Record::Data { payload, .. } => payload.as_ptr(),
            _ => unreachable!(),
        };
        let substitute_ptr = match substitute.record() {
            Record::Data { payload, .. } => payload.as_ptr(),
            _ => unreachable!(),
        };
        assert!(substitute_len > reserved_len);
        let mut queue = LegOutboundQueue::new(2, reserved_len).unwrap();

        let reservation = queue.prepare_ordinary_frame(reserved).unwrap();
        reservation.commit();
        assert!(matches!(
            &queue.frames.front().unwrap().frame,
            QueuedProtocolFrame::Session(frame)
                if matches!(frame.record(), Record::Data { payload, .. }
                    if payload.as_ptr() == reserved_ptr)
        ));
        let rejected = match queue.prepare_ordinary_frame(substitute) {
            Ok(_) => panic!("the larger frame bypassed exact byte admission"),
            Err(error) => error,
        };
        assert!(matches!(
            rejected.kind(),
            LegOutboundQueueErrorKind::CapacityExceeded {
                queued_frames: 1,
                queued_bytes,
                incoming_bytes,
            } if *queued_bytes == reserved_len && *incoming_bytes == substitute_len
        ));
        let returned = rejected.into_frame();

        assert!(matches!(
            returned.record(),
            Record::Data { payload, .. } if payload.as_ptr() == substitute_ptr
        ));
        assert_eq!(queue.owned_bytes, reserved_len);
        assert!(queue.owned_bytes <= queue.max_bytes);
    }

    #[test]
    fn prepared_ordinary_frame_capacity_rejection_leaves_exact_fifo_state_unchanged() {
        let first = data_frame(0, b"first");
        let second = data_frame(5, b"second");
        let max_bytes = first.encode().unwrap().len() + second.encode().unwrap().len();
        let mut queue = LegOutboundQueue::new(1, max_bytes).unwrap();
        queue.push(first).unwrap();
        let before_bytes = queue.owned_bytes;
        let before_ordinal = queue.next_enqueue_ordinal;

        let error = match queue.prepare_ordinary_frame(second) {
            Ok(_) => panic!("the full queue admitted a second frame"),
            Err(error) => error,
        };
        assert!(matches!(
            error.kind(),
            LegOutboundQueueErrorKind::CapacityExceeded {
                queued_frames: 1,
                queued_bytes,
                ..
            } if *queued_bytes == before_bytes
        ));
        let second = error.into_frame();
        assert_eq!(queue.len(), 1);
        assert_eq!(queue.owned_bytes, before_bytes);
        assert_eq!(queue.next_enqueue_ordinal, before_ordinal);
        assert_eq!(queue.session_phase, LegOutboundSessionPhase::Active);
        assert!(matches!(second.record(), Record::Data { .. }));
    }

    #[test]
    fn ordinary_reset_keeps_the_fifo_non_pristine_until_exact_peer_delivery() {
        let mut owner = endpoint(LegId::B, LegEndpointRole::Owner);
        let route = owner.outbound_route();
        let mut queue = LegOutboundQueue::for_leg(owner.established_leg(), 1, 1_024).unwrap();
        queue
            .push(
                Frame::try_new(
                    LegGeneration::new(2).unwrap(),
                    Record::Reset {
                        flow_id: SessionFlowId::new(7).unwrap(),
                        reason: crate::resumable::ResetReason::LocalAbandon,
                    },
                )
                .unwrap(),
            )
            .unwrap();

        let mut faults = MemoryFaultScript::new(1);
        faults
            .insert(0, route, WireLane::Control, FaultAction::Hold { token: 47 })
            .unwrap();
        let mut transport = MemoryLegTransport::new(wire(), faults);
        assert!(
            queue
                .try_flush(
                    &mut owner,
                    &mut transport.controller_sender(),
                    SimTime::ZERO,
                )
                .unwrap()
                .is_some(),
        );
        assert!(queue.frames.is_empty());
        assert!(queue.ordered_delivery_wait.is_some());
        assert_eq!(
            queue
                .try_flush(
                    &mut owner,
                    &mut transport.controller_sender(),
                    SimTime::ZERO,
                )
                .unwrap_err(),
            LegIoError::OrderedDeliveryPending,
        );

        transport.release_hold(47, SimTime::ZERO).unwrap();
        release_due_events_through(&mut transport, SimTime::ZERO);
        let delivered = transport.try_recv_next(route).unwrap().unwrap();
        assert_eq!(delivered.send_ordinal(), 0);
        assert!(
            queue
                .try_flush(
                    &mut owner,
                    &mut transport.controller_sender(),
                    SimTime::ZERO,
                )
                .unwrap()
                .is_none(),
            "the exact positive delivery retires the RESET wait",
        );
        assert!(queue.ordered_delivery_wait.is_none());
    }

    fn attached_and_acceptance_for(endpoint: &LegIo) -> (AttachedLeg, Frame) {
        let request = request();
        let transport = binding();
        let proof = credentials().prove(&request, &transport).unwrap();
        let authenticated = endpoint
            .established_leg()
            .authenticate_initial_owner_attach(
                endpoint
                    .established_leg()
                    .bind_received_frame(request.to_attach_frame(proof)),
                &authority(),
            )
            .unwrap();
        let (_, attached, acceptance) = authenticated.into_owner_parts(session_config());
        (attached, acceptance)
    }

    fn attached_for(endpoint: &LegIo) -> AttachedLeg {
        attached_and_acceptance_for(endpoint).0
    }

    #[test]
    fn attach_records_and_reserved_acceptance_cannot_use_generic_queue_admission() {
        let exact_leg = endpoint(LegId::B, LegEndpointRole::Owner);
        let mut queue = LegOutboundQueue::for_leg(exact_leg.established_leg(), 8, 8_192).unwrap();
        let (attached, acceptance) = attached_and_acceptance_for(&exact_leg);
        let attach = {
            let request = request();
            let proof = credentials().prove(&request, &binding()).unwrap();
            request.to_attach_frame(proof)
        };
        let status = Frame::try_new(
            LegGeneration::new(2).unwrap(),
            Record::AttachGenerationStatus {
                session_id: request().session_id(),
                requested_generation: request().requested_generation(),
                nonce: request().nonce(),
            },
        )
        .unwrap();

        for (frame, expected) in [
            (
                attach,
                LegOutboundQueueErrorKind::RegisteredAttachAdmissionRequired,
            ),
            (
                acceptance.clone(),
                LegOutboundQueueErrorKind::AttachAcceptanceAdmissionRequired,
            ),
            (
                status,
                LegOutboundQueueErrorKind::AuthenticatedAttachStatusAdmissionRequired,
            ),
        ] {
            let error = queue.push(frame).unwrap_err();
            assert_eq!(error.kind(), &expected);
            let _returned_frame = error.into_frame();
            assert!(queue.is_empty());
            assert_eq!(
                queue.session_phase,
                LegOutboundSessionPhase::AwaitingAttachAcceptance
            );
        }

        let reservation = queue
            .reserve_attach_acceptance(exact_leg.established_leg())
            .unwrap();
        let data = data_frame(0, b"held-by-reservation");
        let error = queue.push(data).unwrap_err();
        assert_eq!(
            error.kind(),
            &LegOutboundQueueErrorKind::AttachAcceptanceReserved
        );
        let _returned_frame = error.into_frame();
        let control = LegControlFrame::try_new(
            LegGeneration::new(2).unwrap(),
            LegControlRecord::StandbyAccepted {
                session_id: request().session_id(),
                standby_nonce: StandbyNonce::new([0x33; 16]).unwrap(),
                selected_version: SESSION_PROTOCOL_VERSION,
                features: STANDBY_CONTROL_V1,
            },
            STANDBY_CONTROL_V1,
        )
        .unwrap();
        let control_error = queue
            .push_leg_control(
                &exact_leg.established_leg().standby_seal(),
                control,
                STANDBY_CONTROL_V1,
            )
            .unwrap_err();
        assert_eq!(
            control_error.kind(),
            &LegControlQueueErrorKind::AttachAcceptanceReserved
        );
        let _returned_control = control_error.into_frame();

        let wrong_endpoint = endpoint(LegId::A, LegEndpointRole::Owner);
        let mut wrong_queue =
            LegOutboundQueue::for_leg(wrong_endpoint.established_leg(), 8, 8_192).unwrap();
        let error = wrong_queue
            .push_reserved_attach_acceptance(reservation, &attached, acceptance)
            .unwrap_err();
        assert_eq!(
            error.kind(),
            &LegOutboundQueueErrorKind::WrongAttachAcceptanceReservation
        );
        let (reservation, acceptance) = error.into_parts();
        reservation.release(&mut queue).unwrap();
        assert_eq!(
            queue.session_phase,
            LegOutboundSessionPhase::AwaitingAttachAcceptance
        );
        let _returned_acceptance = acceptance;

        assert!(queue.push(data_frame(0, b"now-active")).is_ok());
        assert!(matches!(
            queue.reserve_attach_acceptance(exact_leg.established_leg()),
            Err(AttachAcceptanceReserveError::NotAwaiting { .. })
        ));
    }

    #[test]
    fn attach_acceptance_reservation_owns_exact_slot_bytes_and_ordinal_before_commit() {
        let too_small_leg = endpoint(LegId::A, LegEndpointRole::Owner);
        let mut too_small = LegOutboundQueue::for_leg(
            too_small_leg.established_leg(),
            1,
            ATTACH_ACCEPTED_ENCODED_BYTES - 1,
        )
        .unwrap();
        assert_eq!(
            too_small
                .reserve_attach_acceptance(too_small_leg.established_leg())
                .unwrap_err(),
            AttachAcceptanceReserveError::CapacityExceeded {
                queued_frames: 0,
                queued_bytes: 0,
                incoming_bytes: ATTACH_ACCEPTED_ENCODED_BYTES,
            }
        );
        assert!(too_small.is_empty());
        assert_eq!(too_small.owned_bytes(), 0);

        let exact_leg = endpoint(LegId::B, LegEndpointRole::Owner);
        let mut exact = LegOutboundQueue::for_leg(
            exact_leg.established_leg(),
            1,
            ATTACH_ACCEPTED_ENCODED_BYTES,
        )
        .unwrap();
        let (attached, acceptance) = attached_and_acceptance_for(&exact_leg);
        let reservation = exact
            .reserve_attach_acceptance(exact_leg.established_leg())
            .unwrap();
        reservation.release(&mut exact).unwrap();
        let reservation = exact
            .reserve_attach_acceptance(exact_leg.established_leg())
            .unwrap();
        let receipt = exact
            .push_reserved_attach_acceptance(reservation, &attached, acceptance)
            .unwrap();
        assert!(receipt.belongs_to_queue(&exact));
        assert_eq!(exact.len(), 1);
        assert_eq!(exact.owned_bytes(), ATTACH_ACCEPTED_ENCODED_BYTES);
        assert_eq!(exact.next_enqueue_ordinal, 2);

        let exhausted_leg = endpoint(LegId::A, LegEndpointRole::Owner);
        let mut exhausted = LegOutboundQueue::for_leg(
            exhausted_leg.established_leg(),
            1,
            ATTACH_ACCEPTED_ENCODED_BYTES,
        )
        .unwrap();
        exhausted.next_enqueue_ordinal = u64::MAX;
        assert_eq!(
            exhausted
                .reserve_attach_acceptance(exhausted_leg.established_leg())
                .unwrap_err(),
            AttachAcceptanceReserveError::EnqueueOrdinalExhausted
        );
        assert!(exhausted.is_empty());
        assert_eq!(exhausted.owned_bytes(), 0);
    }

    #[test]
    fn initial_client_acceptance_requires_actual_delivery_and_the_same_live_queue() {
        let transport_binding = binding();
        let mut client = endpoint(LegId::A, LegEndpointRole::Client);
        let mut owner = endpoint(LegId::A, LegEndpointRole::Owner);
        let request = request();
        let proof = credentials().prove(&request, &transport_binding).unwrap();
        let pending = client
            .established_leg()
            .begin_attach(request)
            .bind_initial_frame(request.to_attach_frame(proof))
            .unwrap();
        assert_eq!(format!("{pending:?}"), "PendingInitialAttach([REDACTED])");
        let mut queue = LegOutboundQueue::for_leg(client.established_leg(), 4, 4_096).unwrap();
        let awaiting = pending.enqueue(&mut queue).unwrap();
        assert_eq!(format!("{awaiting:?}"), "AwaitingInitialAttach([REDACTED])");

        let committed = authority()
            .verify_and_commit(&request, &transport_binding, &proof)
            .unwrap();
        let received = client
            .established_leg()
            .bind_received_frame(committed.attach_accepted_frame());
        let error = awaiting.validate_response(&queue, received).unwrap_err();
        assert_eq!(
            error.kind(),
            InitialAttachResponseErrorKind::RequestNotDelivered
        );
        assert_eq!(
            format!("{error:?}"),
            "InitialAttachResponseFailure { kind: RequestNotDelivered, ownership: \"[REDACTED]\" }"
        );
        let (awaiting, received) = error.into_parts();

        let mut wire = memory_transport();
        queue
            .try_flush(&mut client, &mut wire.controller_sender(), SimTime::ZERO)
            .unwrap()
            .unwrap();
        release_due_events_through(&mut wire, SimTime::ZERO);
        let delivery = wire.try_recv_next(owner.inbound_route()).unwrap().unwrap();
        owner.receive_delivery(delivery).unwrap();

        let accepted = awaiting.validate_response(&queue, received).unwrap();
        assert_eq!(accepted.generation(), request.requested_generation());
        assert_eq!(format!("{accepted:?}"), "AcceptedInitialAttach([REDACTED])");

        let wrong_endpoint = endpoint(LegId::B, LegEndpointRole::Client);
        let mut wrong_queue =
            LegOutboundQueue::for_leg(wrong_endpoint.established_leg(), 4, 4_096).unwrap();
        let error = accepted.activate_on_queue(&mut wrong_queue).unwrap_err();
        assert_eq!(error.kind(), InitialAttachActivationError::WrongQueue);
        assert_eq!(
            format!("{error:?}"),
            "AcceptedInitialAttachActivationFailure { kind: WrongQueue, accepted: \"[REDACTED]\" }"
        );
        let accepted = error.into_accepted();
        let attached = accepted.activate_on_queue(&mut queue).unwrap();
        assert_eq!(attached.generation(), request.requested_generation());
        assert!(queue.push(data_frame(0, b"active-after-initial")).is_ok());
    }

    #[test]
    fn initial_client_queue_loss_returns_exact_pending_and_response_fail_closed() {
        let transport_binding = binding();
        let client = endpoint(LegId::A, LegEndpointRole::Client);
        let request = request();
        let proof = credentials().prove(&request, &transport_binding).unwrap();
        let pending = client
            .established_leg()
            .begin_attach(request)
            .bind_initial_frame(request.to_attach_frame(proof))
            .unwrap();
        let mut queue = LegOutboundQueue::for_leg(client.established_leg(), 4, 4_096).unwrap();
        let awaiting = pending.enqueue(&mut queue).unwrap();
        drop(queue);

        let other = endpoint(LegId::B, LegEndpointRole::Client);
        let other_queue = LegOutboundQueue::for_leg(other.established_leg(), 4, 4_096).unwrap();
        let committed = authority()
            .verify_and_commit(&request, &transport_binding, &proof)
            .unwrap();
        let response = client
            .established_leg()
            .bind_received_frame(committed.attach_accepted_frame());
        let error = awaiting
            .validate_response(&other_queue, response)
            .unwrap_err();
        assert_eq!(
            error.kind(),
            InitialAttachResponseErrorKind::EndpointOrQueueLost
        );
        assert!(format!("{error:?}").contains("ownership: \"[REDACTED]\""));
        let (awaiting, response) = error.into_parts();
        assert_eq!(format!("{awaiting:?}"), "AwaitingInitialAttach([REDACTED])");
        assert_eq!(format!("{response:?}"), "LegBoundFrame([REDACTED])");
    }

    struct RetryablePressureTransport;

    impl EncodedLegTransport for RetryablePressureTransport {
        fn send_encoded(
            &mut self,
            _now: SimTime,
            _route: WireRoute,
            _lane: WireLane,
            _bytes: Bytes,
        ) -> Result<(), EncodedLegTransportError> {
            Err(EncodedLegTransportError::Wire(
                WireError::LiveMessageCapacityExceeded,
            ))
        }
    }

    #[test]
    fn exact_transport_terminal_immediately_closes_a_live_queue_without_dropping_leg_io() {
        let (endpoint, reporter) = LegIo::for_authenticated_transport_with_reporter(
            LegId::A,
            LegEndpointRole::Client,
            binding(),
            limits(),
        );
        let mut queue = LegOutboundQueue::for_leg(endpoint.established_leg(), 1, 1_024).unwrap();

        assert!(!queue.endpoint_is_lost());
        assert_eq!(format!("{reporter:?}"), "LegTransportReporter([REDACTED])");
        let terminal = reporter
            .report(LegTransportTerminalReason::PeerClosed)
            .unwrap();

        assert!(queue.endpoint_is_lost());
        let error = queue.push(data_frame(0, b"must-stay-owned")).unwrap_err();
        assert_eq!(error.kind(), &LegOutboundQueueErrorKind::EndpointLost);
        assert_eq!(terminal.reason(), LegTransportTerminalReason::PeerClosed);
        assert_eq!(format!("{terminal:?}"), "ExactLegTerminal([REDACTED])");
        assert_eq!(endpoint.counters(), LegIoCounters::default());
    }

    #[test]
    fn attached_leg_consumes_only_its_exact_terminal_and_wrong_leg_returns_both() {
        let (endpoint_a, reporter_a) = LegIo::for_authenticated_transport_with_reporter(
            LegId::A,
            LegEndpointRole::Client,
            binding(),
            limits(),
        );
        let (endpoint_b, reporter_b) = LegIo::for_authenticated_transport_with_reporter(
            LegId::B,
            LegEndpointRole::Client,
            binding(),
            limits(),
        );
        let attached_a = attached_for(&endpoint_a);
        let attached_b = attached_for(&endpoint_b);
        let terminal_b = reporter_b
            .report(LegTransportTerminalReason::Reset)
            .unwrap();

        let mismatch = attached_a.bind_terminal(terminal_b).unwrap_err();
        assert_eq!(format!("{mismatch:?}"), "LegTerminalMismatch([REDACTED])");
        let (attached_a, terminal_b) = mismatch.into_parts();

        let pending_b = attached_b.bind_terminal(terminal_b).unwrap();
        assert_eq!(pending_b.reason(), LegTransportTerminalReason::Reset);
        assert!(matches!(
            pending_b.into_event(),
            crate::resumable::SessionEvent::LegLost { leg }
                if leg.generation() == LegGeneration::new(2).unwrap()
        ));

        let pending_a = attached_a
            .bind_terminal(
                reporter_a
                    .report(LegTransportTerminalReason::FatalIo)
                    .unwrap(),
            )
            .unwrap();
        assert_eq!(format!("{pending_a:?}"), "PendingLegLoss([REDACTED])");
        assert!(matches!(
            pending_a.into_event(),
            crate::resumable::SessionEvent::LegLost { leg }
                if leg.generation() == LegGeneration::new(2).unwrap()
        ));
    }

    #[test]
    fn retryable_transport_pressure_cannot_transition_or_mint_terminal_state() {
        let (mut endpoint, reporter) = LegIo::for_authenticated_transport_with_reporter(
            LegId::A,
            LegEndpointRole::Client,
            binding(),
            limits(),
        );
        let mut queue = LegOutboundQueue::for_leg(endpoint.established_leg(), 1, 1_024).unwrap();
        queue.push(data_frame(0, b"retry-exactly")).unwrap();

        assert_eq!(
            queue.try_flush(
                &mut endpoint,
                &mut RetryablePressureTransport,
                SimTime::ZERO,
            ),
            Err(LegIoError::Transport(EncodedLegTransportError::Wire(
                WireError::LiveMessageCapacityExceeded,
            )))
        );
        assert!(!queue.endpoint_is_lost());
        assert_eq!(queue.len(), 1);

        let _terminal = reporter.report(LegTransportTerminalReason::PeerClosed);
        assert!(queue.endpoint_is_lost());
    }

    #[test]
    fn consuming_pending_leg_loss_releases_the_last_endpoint_owner() {
        let (endpoint, reporter) = LegIo::for_authenticated_transport_with_reporter(
            LegId::A,
            LegEndpointRole::Client,
            binding(),
            limits(),
        );
        let endpoint_liveness = endpoint.established_leg().endpoint_liveness();
        let attached = attached_for(&endpoint);
        let pending = attached
            .bind_terminal(
                reporter
                    .report(LegTransportTerminalReason::PeerClosed)
                    .unwrap(),
            )
            .unwrap();

        drop(endpoint);
        assert!(endpoint_liveness.upgrade().is_some());
        let event = pending.into_event();
        assert!(matches!(
            event,
            crate::resumable::SessionEvent::LegLost { .. }
        ));
        assert!(endpoint_liveness.upgrade().is_none());
    }

    #[test]
    fn terminal_acceptance_receipt_cannot_finish_recovery_on_its_live_queue() {
        let (endpoint, reporter) = LegIo::for_authenticated_transport_with_reporter(
            LegId::A,
            LegEndpointRole::Owner,
            binding(),
            limits(),
        );
        let (attached, acceptance) = attached_and_acceptance_for(&endpoint);
        let mut queue = LegOutboundQueue::for_leg(endpoint.established_leg(), 1, 1_024).unwrap();
        let receipt = queue.push_attach_acceptance(&attached, acceptance).unwrap();
        assert!(receipt.queue_is_live());

        let _terminal = reporter.report(LegTransportTerminalReason::Reset);

        assert!(!receipt.queue_is_live());
        assert!(!queue.finish_attach_recovery(&receipt));
    }

    fn standby_accepted_control(generation: LegGeneration) -> LegControlFrame {
        let features = FeatureSet::STANDBY_CONTROL_V1;
        LegControlFrame::try_new(
            generation,
            LegControlRecord::StandbyAccepted {
                session_id: SessionId::new([0x11; 16]).unwrap(),
                standby_nonce: StandbyNonce::new([0x33; 16]).unwrap(),
                selected_version: SESSION_PROTOCOL_VERSION,
                features,
            },
            features,
        )
        .unwrap()
    }

    #[test]
    fn frame_crosses_encode_wire_decode_and_exact_endpoint_seal() {
        let transport = binding();
        let mut client = LegIo::for_authenticated_transport(
            LegId::A,
            LegEndpointRole::Client,
            transport,
            limits(),
        );
        let mut owner = LegIo::for_authenticated_transport(
            LegId::A,
            LegEndpointRole::Owner,
            transport,
            limits(),
        );
        let request = request();
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
        let mut wire = memory_transport();

        client_queue
            .try_flush(&mut client, &mut wire.controller_sender(), SimTime::ZERO)
            .unwrap()
            .unwrap();
        release_due_events_through(&mut wire, SimTime::ZERO);
        let delivery = wire.try_recv_next(owner.inbound_route()).unwrap().unwrap();
        let received = owner.receive_delivery(delivery).unwrap();
        let mut pending_owner =
            SessionSupervisor::prepare_owner_unproven_for_test(session_config(), authority());
        let owner_bootstrap = pending_owner
            .accept(owner.established_leg(), received, &mut owner_queue)
            .unwrap()
            .enqueue_acceptance(&mut owner_queue)
            .unwrap();
        let (owner_supervisor, owner_attached) = owner_bootstrap.into_parts();
        assert_eq!(owner_supervisor.snapshot().session.generation().get(), 2);
        assert_eq!(owner_attached.generation().get(), 2);

        owner_queue
            .try_flush(&mut owner, &mut wire.controller_sender(), SimTime::ZERO)
            .unwrap()
            .unwrap();
        release_due_events_through(&mut wire, SimTime::ZERO);
        let delivery = wire.try_recv_next(client.inbound_route()).unwrap().unwrap();
        let response = client.receive_delivery(delivery).unwrap();
        let accepted = awaiting.validate_response(&client_queue, response).unwrap();
        let client_bootstrap = SessionSupervisor::bootstrap_client_for_test(
            session_config(),
            accepted,
            &mut client_queue,
        )
        .unwrap();
        let (client_supervisor, client_attached) = client_bootstrap.into_parts();
        assert_eq!(client_supervisor.snapshot().session.generation().get(), 2);
        assert_eq!(client_attached.generation().get(), 2);

        assert_eq!(client.counters().outbound_messages, 1);
        assert_eq!(client.counters().bound_messages, 1);
        assert_eq!(owner.counters().outbound_messages, 1);
        assert_eq!(owner.counters().bound_messages, 1);
    }

    #[test]
    fn wrong_endpoint_route_and_wire_lane_fail_closed() {
        let mut client_a = endpoint(LegId::A, LegEndpointRole::Client);
        let mut owner_b = endpoint(LegId::B, LegEndpointRole::Owner);
        let request = request();
        let proof = credentials().prove(&request, &binding()).unwrap();
        let mut wire = memory_transport();

        send(
            &mut client_a,
            &mut wire,
            SimTime::ZERO,
            request.to_attach_frame(proof),
        )
        .unwrap();
        release_due_events_through(&mut wire, SimTime::ZERO);
        let delivery = wire
            .try_recv_next(WireRoute::new(LegId::A, WireDirection::ClientToOwner))
            .unwrap()
            .unwrap();
        assert!(matches!(
            owner_b.receive_delivery(delivery),
            Err(LegIoError::WrongRoute {
                expected,
                actual,
            }) if expected == WireRoute::new(LegId::B, WireDirection::ClientToOwner)
                && actual == WireRoute::new(LegId::A, WireDirection::ClientToOwner)
        ));
        assert_eq!(owner_b.counters().wrong_route_rejections, 1);
        assert_eq!(owner_b.counters().bound_messages, 0);

        let mut owner_a = endpoint(LegId::A, LegEndpointRole::Owner);
        let encoded = data_frame(0, b"wrong-lane").encode().unwrap();
        wire.inject_encoded_test_message(
            SimTime::ZERO,
            owner_a.inbound_route(),
            WireLane::Control,
            encoded.to_vec(),
        )
        .unwrap();
        release_due_events_through(&mut wire, SimTime::ZERO);
        let delivery = wire
            .try_recv_next(owner_a.inbound_route())
            .unwrap()
            .unwrap();
        assert!(matches!(
            owner_a.receive_delivery(delivery),
            Err(LegIoError::WrongLane {
                expected: WireLane::Data,
                actual: WireLane::Control,
            })
        ));
        assert_eq!(owner_a.counters().wrong_lane_rejections, 1);
        assert_eq!(owner_a.counters().bound_messages, 0);
    }

    #[test]
    fn truncated_and_corrupt_bytes_are_rejected_before_exact_seal_binding() {
        let mut owner = endpoint(LegId::A, LegEndpointRole::Owner);
        let route = owner.inbound_route();
        let frame = data_frame(0, b"malformed-wire");
        let mut truncated = frame.encode().unwrap().to_vec();
        truncated.pop();
        let mut corrupt = frame.encode().unwrap().to_vec();
        corrupt[0] ^= 0xff;
        let mut wire = memory_transport();

        wire.inject_encoded_test_message(SimTime::ZERO, route, WireLane::Data, truncated)
            .unwrap();
        wire.inject_encoded_test_message(SimTime::ZERO, route, WireLane::Data, corrupt)
            .unwrap();
        release_due_events_through(&mut wire, SimTime::ZERO);

        assert!(matches!(
            owner.receive_delivery(wire.try_recv_next(route).unwrap().unwrap()),
            Err(LegIoError::Decode(
                crate::resumable::ProtocolError::Truncated { .. }
            ))
        ));
        assert!(matches!(
            owner.receive_delivery(wire.try_recv_next(route).unwrap().unwrap()),
            Err(LegIoError::Decode(
                crate::resumable::ProtocolError::InvalidMagic
            ))
        ));
        assert_eq!(owner.counters().inbound_messages, 2);
        assert_eq!(owner.counters().decode_rejections, 2);
        assert_eq!(owner.counters().bound_messages, 0);
        assert_eq!(owner.counters().bound_bytes, 0);
    }

    #[test]
    fn identical_binding_on_legs_a_and_b_does_not_merge_exact_local_seals() {
        let mut client_a = endpoint(LegId::A, LegEndpointRole::Client);
        let mut owner_a = endpoint(LegId::A, LegEndpointRole::Owner);
        let mut client_b = endpoint(LegId::B, LegEndpointRole::Client);
        let mut owner_b = endpoint(LegId::B, LegEndpointRole::Owner);
        let request = request();
        let proof = credentials().prove(&request, &binding()).unwrap();
        let pending = client_a
            .established_leg()
            .begin_attach(request)
            .bind_initial_frame(request.to_attach_frame(proof))
            .unwrap();
        let mut client_queue =
            LegOutboundQueue::for_leg(client_a.established_leg(), 4, 4_096).unwrap();
        let awaiting = pending.enqueue(&mut client_queue).unwrap();
        let mut owner_queue =
            LegOutboundQueue::for_leg(owner_a.established_leg(), 4, 4_096).unwrap();
        let mut wire = memory_transport();

        client_queue
            .try_flush(&mut client_a, &mut wire.controller_sender(), SimTime::ZERO)
            .unwrap()
            .unwrap();
        release_due_events_through(&mut wire, SimTime::ZERO);
        let attach = owner_a
            .receive_delivery(
                wire.try_recv_next(owner_a.inbound_route())
                    .unwrap()
                    .unwrap(),
            )
            .unwrap();
        let mut pending_owner =
            SessionSupervisor::prepare_owner_unproven_for_test(session_config(), authority());
        let owner_bootstrap = pending_owner
            .accept(owner_a.established_leg(), attach, &mut owner_queue)
            .unwrap()
            .enqueue_acceptance(&mut owner_queue)
            .unwrap();
        let (_owner_supervisor, _owner_attached) = owner_bootstrap.into_parts();
        owner_queue
            .try_flush(&mut owner_a, &mut wire.controller_sender(), SimTime::ZERO)
            .unwrap()
            .unwrap();
        release_due_events_through(&mut wire, SimTime::ZERO);
        let response = client_a
            .receive_delivery(
                wire.try_recv_next(client_a.inbound_route())
                    .unwrap()
                    .unwrap(),
            )
            .unwrap();
        let accepted = awaiting.validate_response(&client_queue, response).unwrap();
        let (_client_supervisor, attached_a) = SessionSupervisor::bootstrap_client_for_test(
            session_config(),
            accepted,
            &mut client_queue,
        )
        .unwrap()
        .into_parts();

        send(
            &mut owner_b,
            &mut wire,
            SimTime::ZERO,
            data_frame(0, b"leg-b-only"),
        )
        .unwrap();
        release_due_events_through(&mut wire, SimTime::ZERO);
        let from_b = client_b
            .receive_delivery(
                wire.try_recv_next(client_b.inbound_route())
                    .unwrap()
                    .unwrap(),
            )
            .unwrap();
        assert!(matches!(
            attached_a.accept_frame(from_b),
            Err(LegProvenanceError::WrongLeg)
        ));
    }

    #[test]
    fn blackout_drop_and_duplicate_apply_before_decode_only_to_encoded_bytes() {
        let mut owner = endpoint(LegId::A, LegEndpointRole::Owner);
        let mut client = endpoint(LegId::A, LegEndpointRole::Client);
        let route = owner.outbound_route();
        let mut raw_wire = wire();
        raw_wire
            .add_blackout(route, SimTime::from_nanos(10), SimTime::from_nanos(20))
            .unwrap();
        let mut faults = MemoryFaultScript::new(2);
        faults
            .insert(0, route, WireLane::Data, FaultAction::Drop)
            .unwrap();
        faults
            .insert(1, route, WireLane::Data, FaultAction::Duplicate)
            .unwrap();
        let mut wire = MemoryLegTransport::new(raw_wire, faults);

        send(
            &mut owner,
            &mut wire,
            SimTime::from_nanos(1),
            data_frame(0, b"drop"),
        )
        .unwrap();
        send(
            &mut owner,
            &mut wire,
            SimTime::from_nanos(2),
            data_frame(4, b"duplicate"),
        )
        .unwrap();
        send(
            &mut owner,
            &mut wire,
            SimTime::from_nanos(10),
            data_frame(13, b"blackout"),
        )
        .unwrap();
        send(
            &mut owner,
            &mut wire,
            SimTime::from_nanos(20),
            data_frame(21, b"after"),
        )
        .unwrap();
        release_due_events_through(&mut wire, SimTime::from_nanos(20));

        let mut received = 0;
        while let Some(delivery) = wire.try_recv_next(route).unwrap() {
            client.receive_delivery(delivery).unwrap();
            received += 1;
        }
        assert_eq!(received, 3);
        assert_eq!(client.counters().bound_messages, 3);
        assert_eq!(client.counters().decode_rejections, 0);

        let wire_counters = wire.wire_counters();
        assert_eq!(wire_counters.submitted_messages, 4);
        assert_eq!(wire_counters.physical_messages, 3);
        assert_eq!(wire_counters.delivered_messages, 3);
        assert_eq!(wire_counters.dropped_messages, 2);
        assert_eq!(wire_counters.blackout_drops, 1);
        assert_eq!(wire_counters.duplicate_copies, 1);
    }

    #[test]
    fn finite_endpoint_budget_rejects_without_submitting_an_extra_message() {
        let limits = LegIoLimits::new(1_024, 1, 4_096, 1, 4_096).unwrap();
        let mut owner =
            LegIo::for_authenticated_transport(LegId::A, LegEndpointRole::Owner, binding(), limits);
        let mut wire = memory_transport();
        send(
            &mut owner,
            &mut wire,
            SimTime::ZERO,
            data_frame(0, b"first"),
        )
        .unwrap();
        assert_eq!(
            send(
                &mut owner,
                &mut wire,
                SimTime::ZERO,
                data_frame(5, b"second"),
            ),
            Err(LegIoError::OutboundMessageBudgetExceeded)
        );
        assert_eq!(owner.counters().outbound_messages, 1);
        assert_eq!(wire.wire_counters().submitted_messages, 1);
    }

    #[test]
    fn preconfigured_fault_script_mismatch_fails_before_wire_submission() {
        let mut owner = endpoint(LegId::A, LegEndpointRole::Owner);
        let mut faults = MemoryFaultScript::new(1);
        faults
            .insert(
                0,
                WireRoute::new(LegId::A, WireDirection::ClientToOwner),
                WireLane::Data,
                FaultAction::Drop,
            )
            .unwrap();
        let mut wire = MemoryLegTransport::new(wire(), faults);

        assert!(matches!(
            send(
                &mut owner,
                &mut wire,
                SimTime::ZERO,
                data_frame(0, b"not-the-scripted-route"),
            ),
            Err(LegIoError::Transport(
                EncodedLegTransportError::FaultScriptMismatch { ordinal: 0, .. }
            ))
        ));
        assert_eq!(owner.counters().outbound_messages, 0);
        assert_eq!(wire.wire_counters().submitted_messages, 0);
        assert_eq!(wire.remaining_fault_directives(), 1);
    }

    #[test]
    fn attach_acceptance_admission_mints_a_live_fifo_receipt_only_for_that_record() {
        let leg = EstablishedLeg::for_authenticated_transport(binding());
        let request = request();
        let proof = credentials().prove(&request, &binding()).unwrap();
        let authenticated = leg
            .authenticate_initial_owner_attach(
                leg.bind_received_frame(request.to_attach_frame(proof)),
                &authority(),
            )
            .unwrap();
        let (_, attached, acceptance) = authenticated.into_owner_parts(session_config());
        let encoded_len = acceptance.encode().unwrap().len();
        let mut queue = LegOutboundQueue::for_leg(&leg, 1, encoded_len).unwrap();
        let receipt = queue.push_attach_acceptance(&attached, acceptance).unwrap();
        assert!(receipt.queue_is_live());
        assert_eq!(queue.len(), 1);
        assert!(!format!("{receipt:?}").contains("11111111"));

        let data = data_frame(0, b"not-an-acceptance");
        let payload_ptr = match data.record() {
            Record::Data { payload, .. } => payload.as_ptr(),
            _ => unreachable!(),
        };
        let error = queue.push_attach_acceptance(&attached, data).unwrap_err();
        assert_eq!(
            error.kind(),
            &LegOutboundQueueErrorKind::NotAttachAcceptance
        );
        assert_eq!(queue.len(), 1);
        assert!(matches!(
            error.into_frame().record(),
            Record::Data { payload, .. } if payload.as_ptr() == payload_ptr
        ));

        drop(leg);
        assert!(!receipt.queue_is_live());
        let error = queue.push(data_frame(0, b"endpoint-gone")).unwrap_err();
        assert_eq!(error.kind(), &LegOutboundQueueErrorKind::EndpointLost);
        assert_eq!(queue.len(), 1);
        drop(queue);
        assert!(!receipt.queue_is_live());
    }

    #[test]
    fn held_attach_acceptance_blocks_cross_lane_recovery_until_delivery() {
        let mut owner = endpoint(LegId::A, LegEndpointRole::Owner);
        let request = request();
        let proof = credentials().prove(&request, &binding()).unwrap();
        let authenticated = owner
            .established_leg()
            .authenticate_initial_owner_attach(
                owner
                    .established_leg()
                    .bind_received_frame(request.to_attach_frame(proof)),
                &authority(),
            )
            .unwrap();
        let (_, attached, acceptance) = authenticated.into_owner_parts(session_config());
        let route = owner.outbound_route();
        let mut queue = LegOutboundQueue::for_leg(owner.established_leg(), 2, 2_048).unwrap();
        queue.push_attach_acceptance(&attached, acceptance).unwrap();
        queue
            .push(data_frame(0, b"must-follow-acceptance-delivery"))
            .unwrap();

        let mut faults = MemoryFaultScript::new(1);
        faults
            .insert(0, route, WireLane::Control, FaultAction::Hold { token: 41 })
            .unwrap();
        let mut transport = MemoryLegTransport::new(wire(), faults);

        queue
            .try_flush(
                &mut owner,
                &mut transport.controller_sender(),
                SimTime::ZERO,
            )
            .unwrap()
            .unwrap();
        let blocked = queue.try_flush(
            &mut owner,
            &mut transport.controller_sender(),
            SimTime::ZERO,
        );
        assert!(
            blocked.is_err(),
            "DATA-lane recovery must not submit while Control-lane acceptance is held"
        );
        assert_eq!(queue.len(), 1);
        assert_eq!(owner.counters().outbound_messages, 1);
        assert_eq!(transport.wire_counters().submitted_messages, 1);
        assert!(transport.try_recv_next(route).unwrap().is_none());

        transport.release_hold(41, SimTime::ZERO).unwrap();
        release_due_events_through(&mut transport, SimTime::ZERO);
        let acceptance = transport.try_recv_next(route).unwrap().unwrap();
        assert!(matches!(
            Frame::decode_owned_exact(Bytes::from(acceptance.into_bytes()))
                .unwrap()
                .record(),
            Record::AttachAccepted { .. }
        ));

        queue
            .try_flush(
                &mut owner,
                &mut transport.controller_sender(),
                SimTime::ZERO,
            )
            .unwrap()
            .unwrap();
        assert!(queue.is_empty());
        release_due_events_through(&mut transport, SimTime::ZERO);
        let recovery = transport.try_recv_next(route).unwrap().unwrap();
        assert!(matches!(
            Frame::decode_owned_exact(Bytes::from(recovery.into_bytes()))
                .unwrap()
                .record(),
            Record::Data { .. }
        ));
    }

    fn assert_leg_control_blocks_later_acceptance(
        fault_action: FaultAction,
        completion_at: SimTime,
    ) {
        let mut owner = endpoint(LegId::B, LegEndpointRole::Owner);
        let request = request();
        let proof = credentials().prove(&request, &binding()).unwrap();
        let authenticated = owner
            .established_leg()
            .authenticate_initial_owner_attach(
                owner
                    .established_leg()
                    .bind_received_frame(request.to_attach_frame(proof)),
                &authority(),
            )
            .unwrap();
        let (_, attached, acceptance) = authenticated.into_owner_parts(session_config());
        let features = FeatureSet::STANDBY_CONTROL_V1;
        let standby_accepted = standby_accepted_control(attached.generation());
        let seal = owner.established_leg().standby_seal();
        let route = owner.outbound_route();
        let mut queue = LegOutboundQueue::for_leg(owner.established_leg(), 2, 2_048).unwrap();
        let control_receipt = queue
            .push_leg_control(&seal, standby_accepted, features)
            .unwrap();
        assert_eq!(control_receipt.ordinal, 1);

        let mut faults = MemoryFaultScript::new(1);
        faults
            .insert(0, route, WireLane::Control, fault_action)
            .unwrap();
        let mut transport = MemoryLegTransport::new(wire(), faults);
        queue
            .try_flush(
                &mut owner,
                &mut transport.controller_sender(),
                SimTime::ZERO,
            )
            .unwrap()
            .unwrap();
        assert!(queue.is_empty());
        assert!(transport.try_recv_next(route).unwrap().is_none());

        let blocked = queue
            .push_attach_acceptance(&attached, acceptance)
            .unwrap_err();
        assert!(matches!(
            blocked.kind(),
            LegOutboundQueueErrorKind::AttachAcceptanceNotFirst {
                next_ordinal: 2,
                queued_frames: 0,
                queued_bytes: 0,
            }
        ));
        assert_eq!(owner.counters().outbound_messages, 1);
        assert_eq!(transport.wire_counters().submitted_messages, 1);

        if let FaultAction::Hold { token } = fault_action {
            transport.release_hold(token, completion_at).unwrap();
        }
        release_due_events_through(&mut transport, completion_at);
        let delivered_control = transport.try_recv_next(route).unwrap().unwrap();
        let DecodedFrame::LegControl(delivered_control) =
            DecodedFrame::decode_owned_exact(Bytes::from(delivered_control.into_bytes()), features)
                .unwrap()
        else {
            panic!("exact held/delayed leg-control changed protocol class");
        };
        assert_eq!(delivered_control.leg_generation(), attached.generation());
        assert!(matches!(
            delivered_control.record(),
            LegControlRecord::StandbyAccepted {
                session_id,
                standby_nonce,
                selected_version: SESSION_PROTOCOL_VERSION,
                features: delivered_features,
            } if *session_id == request.session_id()
                && *standby_nonce == StandbyNonce::new([0x33; 16]).unwrap()
                && *delivered_features == features
        ));

        let acceptance_receipt = queue
            .push_attach_acceptance(&attached, blocked.into_frame())
            .unwrap();
        assert_eq!(acceptance_receipt.ordinal, 2);
        queue
            .try_flush(
                &mut owner,
                &mut transport.controller_sender(),
                completion_at,
            )
            .unwrap()
            .unwrap();
        assert_eq!(transport.wire_counters().submitted_messages, 2);
    }

    #[test]
    fn held_or_delayed_leg_control_blocks_later_acceptance_until_exact_delivery() {
        assert_leg_control_blocks_later_acceptance(FaultAction::Hold { token: 42 }, SimTime::ZERO);
        assert_leg_control_blocks_later_acceptance(
            FaultAction::Delay {
                release_at: SimTime::from_nanos(10),
            },
            SimTime::from_nanos(10),
        );
    }

    fn assert_unreliable_control_fault_does_not_mint_completion(action: FaultAction) {
        let mut owner = endpoint(LegId::B, LegEndpointRole::Owner);
        let features = FeatureSet::STANDBY_CONTROL_V1;
        let seal = owner.established_leg().standby_seal();
        let route = owner.outbound_route();
        let mut queue = LegOutboundQueue::for_leg(owner.established_leg(), 1, 1_024).unwrap();
        let receipt = queue
            .push_leg_control(
                &seal,
                standby_accepted_control(LegGeneration::new(2).unwrap()),
                features,
            )
            .unwrap();
        let retained_bytes = queue.owned_bytes();
        let mut faults = MemoryFaultScript::new(1);
        faults.insert(0, route, WireLane::Control, action).unwrap();
        let mut rejected_transport = MemoryLegTransport::new(wire(), faults);

        assert!(matches!(
            queue.try_flush(
                &mut owner,
                &mut rejected_transport.controller_sender(),
                SimTime::ZERO,
            ),
            Err(LegIoError::Transport(
                EncodedLegTransportError::UnreliableOrderedFault { ordinal: 0 }
            ))
        ));
        assert_eq!(queue.len(), 1);
        assert_eq!(queue.owned_bytes(), retained_bytes);
        assert!(receipt.queue_is_live());
        assert_eq!(owner.counters().outbound_messages, 0);
        assert_eq!(rejected_transport.wire_counters().submitted_messages, 0);

        let mut retry_transport = memory_transport();
        queue
            .try_flush(
                &mut owner,
                &mut retry_transport.controller_sender(),
                SimTime::ZERO,
            )
            .unwrap()
            .unwrap();
        release_due_events_through(&mut retry_transport, SimTime::ZERO);
        let delivered = retry_transport.try_recv_next(route).unwrap().unwrap();
        assert_eq!(delivered.send_ordinal(), 0);
        assert!(matches!(
            DecodedFrame::decode_owned_exact(Bytes::from(delivered.into_bytes()), features)
                .unwrap(),
            DecodedFrame::LegControl(_)
        ));
        assert!(
            queue
                .try_flush(
                    &mut owner,
                    &mut retry_transport.controller_sender(),
                    SimTime::ZERO,
                )
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn drop_duplicate_or_reorder_cannot_mint_or_permanently_block_control_completion() {
        for action in [
            FaultAction::Drop,
            FaultAction::Duplicate,
            FaultAction::ReorderAdjacent,
        ] {
            assert_unreliable_control_fault_does_not_mint_completion(action);
        }
    }

    #[test]
    fn wrong_send_ordinal_cannot_complete_an_ordered_leg_control() {
        let mut owner = endpoint(LegId::B, LegEndpointRole::Owner);
        let features = FeatureSet::STANDBY_CONTROL_V1;
        let seal = owner.established_leg().standby_seal();
        let route = owner.outbound_route();
        let mut queue = LegOutboundQueue::for_leg(owner.established_leg(), 1, 1_024).unwrap();
        queue
            .push_leg_control(
                &seal,
                standby_accepted_control(LegGeneration::new(2).unwrap()),
                features,
            )
            .unwrap();
        let mut faults = MemoryFaultScript::new(1);
        faults
            .insert(0, route, WireLane::Control, FaultAction::Hold { token: 43 })
            .unwrap();
        let mut transport = MemoryLegTransport::new(wire(), faults);
        queue
            .try_flush(
                &mut owner,
                &mut transport.controller_sender(),
                SimTime::ZERO,
            )
            .unwrap()
            .unwrap();

        // A later same-route delivery cannot discharge the token for the
        // held earlier send ordinal.
        transport
            .inject_encoded_test_message(SimTime::ZERO, route, WireLane::Control, vec![0xaa])
            .unwrap();
        release_due_events_through(&mut transport, SimTime::ZERO);
        let unrelated = transport.try_recv_next(route).unwrap().unwrap();
        assert_eq!(unrelated.send_ordinal(), 1);
        assert_eq!(
            queue
                .try_flush(
                    &mut owner,
                    &mut transport.controller_sender(),
                    SimTime::ZERO,
                )
                .unwrap_err(),
            LegIoError::OrderedDeliveryPending
        );

        transport.release_hold(43, SimTime::ZERO).unwrap();
        release_due_events_through(&mut transport, SimTime::ZERO);
        let exact = transport.try_recv_next(route).unwrap().unwrap();
        assert_eq!(exact.send_ordinal(), 0);
        assert!(transport.ordered_deliveries[memory_route_index(route)].is_none());
        assert!(
            queue
                .try_flush(
                    &mut owner,
                    &mut transport.controller_sender(),
                    SimTime::ZERO,
                )
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn endpoint_loss_makes_a_pending_control_receipt_inert_despite_live_token() {
        let mut owner = endpoint(LegId::B, LegEndpointRole::Owner);
        let features = FeatureSet::STANDBY_CONTROL_V1;
        let seal = owner.established_leg().standby_seal();
        let route = owner.outbound_route();
        let mut queue = LegOutboundQueue::for_leg(owner.established_leg(), 1, 1_024).unwrap();
        let receipt = queue
            .push_leg_control(
                &seal,
                standby_accepted_control(LegGeneration::new(2).unwrap()),
                features,
            )
            .unwrap();
        let mut faults = MemoryFaultScript::new(1);
        faults
            .insert(0, route, WireLane::Control, FaultAction::Hold { token: 44 })
            .unwrap();
        let mut transport = MemoryLegTransport::new(wire(), faults);
        queue
            .try_flush(
                &mut owner,
                &mut transport.controller_sender(),
                SimTime::ZERO,
            )
            .unwrap()
            .unwrap();
        assert!(receipt.queue_is_live());
        assert!(queue.ordered_delivery_wait.is_some());

        drop(owner);
        assert!(!receipt.queue_is_live());
        assert!(queue.endpoint_is_lost());
        let mut replacement = endpoint(LegId::B, LegEndpointRole::Owner);
        assert_eq!(
            queue
                .try_flush(
                    &mut replacement,
                    &mut transport.controller_sender(),
                    SimTime::ZERO,
                )
                .unwrap_err(),
            LegIoError::OutboundQueueEndpointLost
        );

        transport.release_hold(44, SimTime::ZERO).unwrap();
        release_due_events_through(&mut transport, SimTime::ZERO);
        let _delivered_after_retirement = transport.try_recv_next(route).unwrap().unwrap();
        assert!(!receipt.queue_is_live());
        assert!(queue.endpoint_is_lost());
        drop(queue);
        assert!(!receipt.queue_is_live());
    }

    #[test]
    fn attach_acceptance_rejects_a_prefilled_same_leg_queue_without_mutation() {
        let leg = EstablishedLeg::for_authenticated_transport(binding());
        let request = request();
        let proof = credentials().prove(&request, &binding()).unwrap();
        let authenticated = leg
            .authenticate_initial_owner_attach(
                leg.bind_received_frame(request.to_attach_frame(proof)),
                &authority(),
            )
            .unwrap();
        let (_, attached, acceptance) = authenticated.into_owner_parts(session_config());
        let acceptance_nonce = attached.nonce();
        let mut queue = LegOutboundQueue::for_leg(&leg, 2, 2_048).unwrap();
        queue.push(data_frame(0, b"must-not-overtake")).unwrap();
        let before_bytes = queue.owned_bytes();

        let error = queue
            .push_attach_acceptance(&attached, acceptance)
            .unwrap_err();

        assert!(matches!(
            error.kind(),
            LegOutboundQueueErrorKind::AttachAcceptanceNotFirst {
                next_ordinal: 2,
                queued_frames: 1,
                queued_bytes,
            } if *queued_bytes == before_bytes
        ));
        assert_eq!(queue.len(), 1);
        assert_eq!(queue.owned_bytes(), before_bytes);
        assert!(matches!(
            error.into_frame().record(),
            Record::AttachAccepted { nonce, .. } if *nonce == acceptance_nonce
        ));
    }

    #[test]
    fn attach_acceptance_is_first_session_admission_not_absolute_queue_ordinal() {
        let leg = EstablishedLeg::for_authenticated_transport(binding());
        let request = request();
        let proof = credentials().prove(&request, &binding()).unwrap();
        let authenticated = leg
            .authenticate_initial_owner_attach(
                leg.bind_received_frame(request.to_attach_frame(proof)),
                &authority(),
            )
            .unwrap();
        let (_, attached, acceptance) = authenticated.into_owner_parts(session_config());
        let mut queue = LegOutboundQueue::for_leg(&leg, 2, 2_048).unwrap();

        // Models one earlier, fully completed LegControlFrame. The R6-L
        // unified outbound enum will advance this same absolute ordinal while
        // deliberately leaving AwaitingAttachAcceptance unchanged.
        queue.next_enqueue_ordinal = 2;
        let receipt = queue.push_attach_acceptance(&attached, acceptance).unwrap();

        assert_eq!(receipt.ordinal, 2);
        assert_eq!(queue.session_phase, LegOutboundSessionPhase::AttachRecovery);
        assert_eq!(queue.len(), 1);
    }

    #[test]
    fn authenticated_leg_mints_only_one_queue_even_after_queue_drop() {
        let leg = EstablishedLeg::for_authenticated_transport(binding());
        let queue = LegOutboundQueue::for_leg(&leg, 1, 1_024).unwrap();

        assert_eq!(
            LegOutboundQueue::for_leg(&leg, 1, 1_024).unwrap_err(),
            LegIoConfigError::OutboundQueueAlreadyMinted
        );
        drop(queue);
        assert_eq!(
            LegOutboundQueue::for_leg(&leg, 1, 1_024).unwrap_err(),
            LegIoConfigError::OutboundQueueAlreadyMinted
        );
    }

    #[test]
    fn attach_acceptance_admission_rejects_mismatched_correlation() {
        let leg = EstablishedLeg::for_authenticated_transport(binding());
        let request = request();
        let proof = credentials().prove(&request, &binding()).unwrap();
        let authenticated = leg
            .authenticate_initial_owner_attach(
                leg.bind_received_frame(request.to_attach_frame(proof)),
                &authority(),
            )
            .unwrap();
        let (_, attached, _) = authenticated.into_owner_parts(session_config());
        let mismatched = Frame::try_new(
            attached.generation(),
            Record::AttachAccepted {
                session_id: SessionId::new([0x11; 16]).unwrap(),
                nonce: AttachNonce::new([0x99; 16]).unwrap(),
                selected_version: SESSION_PROTOCOL_VERSION,
                features: FeatureSet::new(0b001),
            },
        )
        .unwrap();
        let mut queue = LegOutboundQueue::for_leg(&leg, 1, 1_024).unwrap();
        let error = queue
            .push_attach_acceptance(&attached, mismatched)
            .unwrap_err();

        assert_eq!(
            error.kind(),
            &LegOutboundQueueErrorKind::MismatchedAttachAcceptance
        );
        assert!(queue.is_empty());
        assert!(matches!(
            error.into_frame().record(),
            Record::AttachAccepted { nonce, .. } if *nonce == AttachNonce::new([0x99; 16]).unwrap()
        ));
    }

    #[test]
    fn outbound_queue_rejects_wrong_leg_flush_without_consuming_the_frame() {
        let mut endpoint_a = endpoint(LegId::A, LegEndpointRole::Client);
        let mut endpoint_b = endpoint(LegId::B, LegEndpointRole::Client);
        let mut queue = LegOutboundQueue::for_leg(endpoint_a.established_leg(), 1, 1_024).unwrap();
        queue.push(data_frame(0, b"exact-retry")).unwrap();
        let before_bytes = queue.owned_bytes();
        let mut transport = memory_transport();

        assert_eq!(
            queue
                .try_flush(
                    &mut endpoint_b,
                    &mut transport.controller_sender(),
                    SimTime::ZERO,
                )
                .unwrap_err(),
            LegIoError::WrongOutboundQueueLeg
        );
        assert_eq!(queue.len(), 1);
        assert_eq!(queue.owned_bytes(), before_bytes);
        assert_eq!(endpoint_b.counters().outbound_messages, 0);
        assert_eq!(transport.wire_counters().submitted_messages, 0);

        queue
            .try_flush(
                &mut endpoint_a,
                &mut transport.controller_sender(),
                SimTime::ZERO,
            )
            .unwrap()
            .unwrap();
        assert!(queue.is_empty());
        assert_eq!(endpoint_a.counters().outbound_messages, 1);
        assert_eq!(transport.wire_counters().submitted_messages, 1);
    }

    // Ambiguous inference fails to compile if the endpoint authority ever
    // becomes Clone.  The exact transport seal must remain single-owner.
    trait AmbiguousIfClone<Marker> {
        fn marker() {}
    }
    impl<T: ?Sized> AmbiguousIfClone<()> for T {}
    impl<T: Clone> AmbiguousIfClone<u8> for T {}
    const _: fn() = || {
        let _ = <LegIo as AmbiguousIfClone<_>>::marker;
        let _ = <PendingInitialAttach as AmbiguousIfClone<_>>::marker;
        let _ = <AwaitingInitialAttach as AmbiguousIfClone<_>>::marker;
        let _ = <AcceptedInitialAttach as AmbiguousIfClone<_>>::marker;
        let _ = <InitialAttachEnqueueFailure as AmbiguousIfClone<_>>::marker;
        let _ = <InitialAttachResponseFailure as AmbiguousIfClone<_>>::marker;
        let _ = <AcceptedInitialAttachActivationFailure as AmbiguousIfClone<_>>::marker;
        let _ = <CausallyDeliveredAttachRequest as AmbiguousIfClone<_>>::marker;
        let _ = <AttachAcceptanceEnqueued as AmbiguousIfClone<_>>::marker;
        let _ = <AttachAcceptanceReservation as AmbiguousIfClone<_>>::marker;
        let _ = <ReservedAttachAcceptanceError as AmbiguousIfClone<_>>::marker;
        let _ = <LegControlEnqueued as AmbiguousIfClone<_>>::marker;
        let _ = <OrderedSessionDeliveryToken as AmbiguousIfClone<_>>::marker;
        let _ = <OrderedLegControlDeliveryToken as AmbiguousIfClone<_>>::marker;
        let _ = <OrderedDeliveryToken as AmbiguousIfClone<_>>::marker;
        let _ = <LegTransportReporter as AmbiguousIfClone<_>>::marker;
        let _ = <ExactLegTerminal as AmbiguousIfClone<_>>::marker;
        let _ = <PhysicalLegRuntimeIncarnation as AmbiguousIfClone<_>>::marker;
        let _ = <PhysicalLegPolicyIncarnation as AmbiguousIfClone<_>>::marker;
        let _ = <PendingLegLoss as AmbiguousIfClone<_>>::marker;
        let _ = <CaughtUpAttachedLeg as AmbiguousIfClone<_>>::marker;
        let _ = <CaughtUpLegTerminalMismatch as AmbiguousIfClone<_>>::marker;
        let _ = <PreparedOrdinaryFrameAdmission<'static> as AmbiguousIfClone<_>>::marker;
    };

    #[test]
    fn endpoint_debug_redacts_authenticated_transport_authority() {
        let endpoint = endpoint(LegId::A, LegEndpointRole::Client);
        assert_eq!(format!("{endpoint:?}"), "LegIo([REDACTED])");
    }
}
