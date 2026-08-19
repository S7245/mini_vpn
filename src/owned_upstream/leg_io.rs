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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use thiserror::Error;

use crate::owned_upstream::leg::{
    AttachedLeg, BoundInbound, EstablishedLeg, LegBoundFrame, LegOutboundQueueLease,
    LegProvenanceError, LegSeal, LegTransportEndpoint, LegTransportReporter, PendingInitialAttach,
};
use crate::owned_upstream::two_leg::{
    EncodedDelivery, FaultAction, LegId, SimTime, TwoLegWire, WireCapacity, WireCounters,
    WireDirection, WireError, WireLane, WireRoute,
};
use crate::resumable::{
    ATTACH_ACCEPTED_ENCODED_BYTES, AttachNonce, AttachRequest, AttachTransportBinding,
    DecodedFrame, FeatureSet, Frame, LegControlFrame, LegGeneration, ProtocolError, Record,
    SessionId,
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

/// One non-cloneable local endpoint of an authenticated leg.
pub(crate) struct LegIo {
    leg_id: LegId,
    role: LegEndpointRole,
    established: EstablishedLeg,
    limits: LegIoLimits,
    counters: LegIoCounters,
}

/// Opaque one-shot submission minted only by [`LegIo`]. Production transport
/// implementations may inspect it after receiving it, but controller code
/// cannot construct arbitrary encoded bytes or bypass the exact queue.
pub(crate) struct EncodedLegSubmission {
    now: SimTime,
    route: WireRoute,
    lane: WireLane,
    bytes: Vec<u8>,
    ordered_completion: Option<OrderedDeliveryToken>,
}

impl EncodedLegSubmission {
    pub(crate) fn into_parts(
        self,
    ) -> (
        SimTime,
        WireRoute,
        WireLane,
        Vec<u8>,
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
        self.0.delivered.load(Ordering::Acquire)
    }
}

impl OrderedDeliveryToken {
    pub(crate) fn mark_delivered(self) {
        let state = match self {
            Self::Session(token) => token.0,
            Self::LegControl(token) => token.0,
        };
        state.delivered.store(true, Ordering::Release);
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
        bytes: Vec<u8>,
    ) -> Result<(), EncodedLegTransportError>;

    /// Ordered leg traffic is fail-closed unless the transport explicitly
    /// supplies actual delivery completion across Control/Data lanes.
    #[cfg(test)]
    fn send_attach_ordered_encoded(
        &mut self,
        _now: SimTime,
        _route: WireRoute,
        _lane: WireLane,
        _bytes: Vec<u8>,
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
    Active,
}

enum QueuedProtocolFrame {
    Session(Frame),
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

struct LegOutboundQueueIdentity;

impl LegOutboundQueue {
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
            frames: VecDeque::new(),
        })
    }

    #[allow(
        clippy::result_large_err,
        reason = "the error must return the exact unconsumed frame without a hot-path allocation"
    )]
    pub(crate) fn push(&mut self, frame: Frame) -> Result<(), LegOutboundQueueError> {
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
            LegOutboundSessionPhase::AwaitingAttachAcceptance
            | LegOutboundSessionPhase::AttachRecovery
            | LegOutboundSessionPhase::Active => None,
        };
        if let Some(kind) = blocked_kind {
            return Err(LegOutboundQueueError::new(frame, kind));
        }
        let ordered = self.session_phase == LegOutboundSessionPhase::AttachRecovery;
        self.push_inner(frame, ordered).map(|_| {
            if self.session_phase == LegOutboundSessionPhase::AwaitingAttachAcceptance {
                self.session_phase = LegOutboundSessionPhase::Active;
            }
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
                    LegOutboundSessionPhase::AwaitingAttachAcceptance
                    | LegOutboundSessionPhase::Active => unreachable!(),
                },
            ));
        }
        let delivery_state = Arc::new(OrderedDeliveryState {
            delivered: AtomicBool::new(false),
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
        self.session_phase = LegOutboundSessionPhase::AttachAcceptanceReserved;
        Ok(AttachAcceptanceReservation {
            queue: Arc::downgrade(&self.identity),
            endpoint: self.endpoint.clone(),
            seal,
            ordinal,
            next_ordinal,
            next_owned_bytes,
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
        let ordinal = reservation.ordinal;
        self.frames.push_back(QueuedOutboundFrame {
            frame: QueuedProtocolFrame::Session(frame),
            bytes: ATTACH_ACCEPTED_ENCODED_BYTES,
            ordered: true,
            delivery_state: None,
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
        let ordinal = self.push_inner(frame, true)?;
        self.session_phase = LegOutboundSessionPhase::AttachRecovery;
        Ok(AttachAcceptanceEnqueued {
            queue: Arc::downgrade(&self.identity),
            endpoint: self.endpoint.clone(),
            ordinal,
            generation,
            session_id,
            nonce,
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
            QueuedProtocolFrame::Session(frame) if matches!(frame.record(), Record::Ack { .. })
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
                    endpoint.send_retained_frame(transport, now, frame, Some(completion))?;
                    Some(wait)
                } else {
                    endpoint.send_retained_frame(transport, now, frame, None)?;
                    None
                }
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
    pub(super) fn activate_on_queue(
        self,
        queue: &mut LegOutboundQueue,
    ) -> Result<AttachedLeg, AcceptedInitialAttachActivationFailure> {
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
            | LegOutboundQueueErrorKind::NotAttachAcceptance
            | LegOutboundQueueErrorKind::RegisteredAttachAdmissionRequired
            | LegOutboundQueueErrorKind::AttachAcceptanceAdmissionRequired
            | LegOutboundQueueErrorKind::AuthenticatedAttachStatusAdmissionRequired
            | LegOutboundQueueErrorKind::AttachAcceptanceReserved
            | LegOutboundQueueErrorKind::AttachRequestPending
            | LegOutboundQueueErrorKind::AttachStatusPending
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
    NotAttachAcceptance,
    RegisteredAttachAdmissionRequired,
    AttachAcceptanceAdmissionRequired,
    AuthenticatedAttachStatusAdmissionRequired,
    AttachAcceptanceReserved,
    AttachRequestPending,
    AttachStatusPending,
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
        bytes: Vec<u8>,
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
        bytes: Vec<u8>,
    ) -> Result<(), EncodedLegTransportError> {
        self.send_encoded_inner(now, route, lane, bytes).map(|_| ())
    }

    #[cfg(test)]
    fn send_attach_ordered_encoded(
        &mut self,
        now: SimTime,
        route: WireRoute,
        lane: WireLane,
        bytes: Vec<u8>,
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
}

impl LegIo {
    fn authenticated_transport_parts(
        leg_id: LegId,
        role: LegEndpointRole,
        binding: AttachTransportBinding,
        limits: LegIoLimits,
    ) -> (Self, LegTransportReporter) {
        let (established, reporter) =
            EstablishedLeg::for_authenticated_transport_with_reporter(binding);
        (
            Self {
                leg_id,
                role,
                established,
                limits,
                counters: LegIoCounters::default(),
            },
            reporter,
        )
    }

    /// Production construction always transfers the sole terminal reporter
    /// to the concrete authenticated transport actor.
    #[cfg(not(test))]
    pub(crate) fn for_authenticated_transport(
        leg_id: LegId,
        role: LegEndpointRole,
        binding: AttachTransportBinding,
        limits: LegIoLimits,
    ) -> (Self, LegTransportReporter) {
        Self::authenticated_transport_parts(leg_id, role, binding, limits)
    }

    /// Legacy deterministic harness construction cannot exist in production.
    #[cfg(test)]
    pub(crate) fn for_authenticated_transport(
        leg_id: LegId,
        role: LegEndpointRole,
        binding: AttachTransportBinding,
        limits: LegIoLimits,
    ) -> Self {
        Self::authenticated_transport_parts(leg_id, role, binding, limits).0
    }

    #[cfg(test)]
    pub(crate) fn for_authenticated_transport_with_reporter(
        leg_id: LegId,
        role: LegEndpointRole,
        binding: AttachTransportBinding,
        limits: LegIoLimits,
    ) -> (Self, LegTransportReporter) {
        Self::authenticated_transport_parts(leg_id, role, binding, limits)
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

    /// Encodes before the byte-only wire can apply a fault action.
    #[cfg(test)]
    pub(crate) fn send_frame<T: EncodedLegTransport + ?Sized>(
        &mut self,
        transport: &mut T,
        now: SimTime,
        frame: Frame,
    ) -> Result<(), LegIoError> {
        self.send_retained_frame(transport, now, &frame, None)
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
    ) -> Result<(), LegIoError> {
        let lane = lane_for_record(frame.record());
        let encoded = frame.encode().map_err(LegIoError::Encode)?;
        let encoded_len = u64::try_from(encoded.len()).map_err(|_| LegIoError::CounterOverflow)?;
        let (messages, bytes) = self.preflight_outbound(encoded_len)?;
        transport
            .submit_encoded(EncodedLegSubmission {
                now,
                route: self.outbound_route(),
                lane,
                bytes: encoded.to_vec(),
                ordered_completion: ordered_completion.map(OrderedDeliveryToken::Session),
            })
            .map_err(LegIoError::Transport)?;
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
                bytes: encoded.to_vec(),
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

        let decoded =
            match DecodedFrame::decode_owned_exact(Bytes::from(encoded), installed_features) {
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
        if messages > self.limits.max_outbound_messages {
            return Err(LegIoError::OutboundMessageBudgetExceeded);
        }
        let bytes = self
            .counters
            .outbound_bytes
            .checked_add(len)
            .ok_or(LegIoError::CounterOverflow)?;
        if bytes > self.limits.max_outbound_bytes {
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
            _bytes: Vec<u8>,
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
        let mut pending_owner = SessionSupervisor::prepare_owner(session_config(), authority());
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
        let client_bootstrap =
            SessionSupervisor::bootstrap_client(session_config(), accepted, &mut client_queue)
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
        let mut pending_owner = SessionSupervisor::prepare_owner(session_config(), authority());
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
        let (_client_supervisor, attached_a) =
            SessionSupervisor::bootstrap_client(session_config(), accepted, &mut client_queue)
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
        let _ = <AttachAcceptanceEnqueued as AmbiguousIfClone<_>>::marker;
        let _ = <AttachAcceptanceReservation as AmbiguousIfClone<_>>::marker;
        let _ = <ReservedAttachAcceptanceError as AmbiguousIfClone<_>>::marker;
        let _ = <LegControlEnqueued as AmbiguousIfClone<_>>::marker;
        let _ = <OrderedSessionDeliveryToken as AmbiguousIfClone<_>>::marker;
        let _ = <OrderedLegControlDeliveryToken as AmbiguousIfClone<_>>::marker;
        let _ = <OrderedDeliveryToken as AmbiguousIfClone<_>>::marker;
        let _ = <LegTransportReporter as AmbiguousIfClone<_>>::marker;
        let _ = <ExactLegTerminal as AmbiguousIfClone<_>>::marker;
        let _ = <PendingLegLoss as AmbiguousIfClone<_>>::marker;
        let _ = <CaughtUpAttachedLeg as AmbiguousIfClone<_>>::marker;
        let _ = <CaughtUpLegTerminalMismatch as AmbiguousIfClone<_>>::marker;
    };

    #[test]
    fn endpoint_debug_redacts_authenticated_transport_authority() {
        let endpoint = endpoint(LegId::A, LegEndpointRole::Client);
        assert_eq!(format!("{endpoint:?}"), "LegIo([REDACTED])");
    }
}
