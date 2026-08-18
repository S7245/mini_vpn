//! Typed provenance for frames received from one authenticated transport leg.
//!
//! The eventual Quinn adapter is responsible for constructing one
//! [`EstablishedLeg`] after a full TLS handshake and for calling
//! [`EstablishedLeg::bind_received_frame`] only with bytes decoded from that
//! exact connection.  This module deliberately contains no Quinn, socket, or
//! 0-RTT behavior.

use crate::resumable::{
    AttachAuthority, AttachReject, AttachRequest, AttachTransportBinding,
    AuthenticatedStandbyRegistration, CommittedLeg, Frame, GenerationCatchUp,
    GenerationResynchronization, LegControlFrame, LegGeneration, PendingGenerationCatchUp, Record,
    SessionConfig, SessionEffect, SessionError, SessionEvent, SessionModel, SessionRole,
    StandbyNonce, StandbyRegistrationReject, StandbyRegistrationRequest,
};
use crate::shared::TargetAddr;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock, Weak};
use thiserror::Error;

/// One authenticated connection's process-local, unforgeable identity.
///
/// There is intentionally no numeric identifier: equality is the identity of
/// the live allocation shared by values minted from the same connection.
pub(super) struct LegSeal(Arc<LegSealInner>);

struct LegSealInner {
    outbound_queue_claimed: AtomicBool,
    outbound_queue_lease: OnceLock<Weak<LegOutboundQueueLease>>,
}

/// Exact lifetime witness held only by the sole outbound queue minted for a
/// transport leg. The seal keeps only a weak reference, so a pending attach
/// can distinguish a live queue from a permanently lost claimed queue without
/// gaining authority to recreate it.
pub(super) struct LegOutboundQueueLease;

/// Liveness lease held only by the authenticated transport endpoint. Queue
/// receipts retain a weak reference, so dropping `LegIo`/`EstablishedLeg`
/// cannot leave apparently sendable recovery authority behind.
pub(super) struct LegTransportEndpoint {
    terminal: AtomicBool,
}

impl LegTransportEndpoint {
    fn open() -> Self {
        Self {
            terminal: AtomicBool::new(false),
        }
    }

    pub(super) fn is_open(&self) -> bool {
        !self.terminal.load(Ordering::Acquire)
    }
}

/// Observes the exact authenticated transport lease without extending it.
/// Terminal state is authoritative even while the actor or a terminal token
/// still retains the endpoint allocation.
pub(super) fn transport_endpoint_is_open(endpoint: &Weak<LegTransportEndpoint>) -> bool {
    endpoint
        .upgrade()
        .is_some_and(|endpoint| endpoint.is_open())
}

/// Closed set of terminal facts that the authenticated transport actor may
/// report. Transient pressure and retryable I/O deliberately have no variant.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LegTransportTerminalReason {
    PeerClosed,
    Reset,
    FatalIo,
}

/// Sole process-local authority for reporting one authenticated endpoint's
/// terminal transition. The concrete transport actor owns this non-cloneable
/// value; session/controller code receives only the resulting exact fact.
pub(crate) struct LegTransportReporter {
    seal: LegSeal,
    endpoint: Arc<LegTransportEndpoint>,
}

impl LegTransportReporter {
    pub(crate) fn report(self, reason: LegTransportTerminalReason) -> ExactLegTerminal {
        let was_terminal = self.endpoint.terminal.swap(true, Ordering::AcqRel);
        debug_assert!(
            !was_terminal,
            "the sole non-cloneable transport reporter cannot report twice"
        );
        ExactLegTerminal {
            seal: self.seal,
            endpoint: self.endpoint,
            reason,
        }
    }
}

impl fmt::Debug for LegTransportReporter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("LegTransportReporter([REDACTED])")
    }
}

/// Exact, once-minted terminal fact for one authenticated endpoint.
pub(crate) struct ExactLegTerminal {
    seal: LegSeal,
    endpoint: Arc<LegTransportEndpoint>,
    reason: LegTransportTerminalReason,
}

impl ExactLegTerminal {
    pub(crate) const fn reason(&self) -> LegTransportTerminalReason {
        self.reason
    }
}

impl fmt::Debug for ExactLegTerminal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ExactLegTerminal([REDACTED])")
    }
}

impl LegSeal {
    fn fresh() -> Self {
        Self(Arc::new(LegSealInner {
            outbound_queue_claimed: AtomicBool::new(false),
            outbound_queue_lease: OnceLock::new(),
        }))
    }

    pub(super) fn share(&self) -> Self {
        Self(Arc::clone(&self.0))
    }

    pub(super) fn same_connection(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    fn try_claim_outbound_queue(&self) -> Option<Arc<LegOutboundQueueLease>> {
        let claimed = self
            .0
            .outbound_queue_claimed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok();
        if !claimed {
            return None;
        }
        let lease = Arc::new(LegOutboundQueueLease);
        self.0
            .outbound_queue_lease
            .set(Arc::downgrade(&lease))
            .ok()
            .map(|()| lease)
    }

    pub(super) fn outbound_queue_was_lost(&self) -> bool {
        self.0.outbound_queue_claimed.load(Ordering::Acquire)
            && self
                .0
                .outbound_queue_lease
                .get()
                .is_some_and(|lease| lease.upgrade().is_none())
    }
}

/// Authenticated facts and opaque identity of one live transport connection.
///
/// Construction and receive binding stay restricted to the parent
/// `owned_upstream` adapter so arbitrary library users cannot bless a frame as
/// transport-authenticated.
pub(crate) struct EstablishedLeg {
    seal: LegSeal,
    endpoint: Arc<LegTransportEndpoint>,
    binding: AttachTransportBinding,
}

impl EstablishedLeg {
    pub(super) fn for_authenticated_transport_with_reporter(
        binding: AttachTransportBinding,
    ) -> (Self, LegTransportReporter) {
        let seal = LegSeal::fresh();
        let endpoint = Arc::new(LegTransportEndpoint::open());
        let reporter = LegTransportReporter {
            seal: seal.share(),
            endpoint: Arc::clone(&endpoint),
        };
        (
            Self {
                seal,
                endpoint,
                binding,
            },
            reporter,
        )
    }

    /// Legacy construction is confined to deterministic tests. Production
    /// must retain the sole reporter returned by `LegIo` construction.
    #[cfg(test)]
    pub(super) fn for_authenticated_transport(binding: AttachTransportBinding) -> Self {
        Self::for_authenticated_transport_with_reporter(binding).0
    }

    pub(crate) fn begin_attach(&self, request: AttachRequest) -> PendingAttach {
        PendingAttach {
            seal: self.seal.share(),
            endpoint: Arc::downgrade(&self.endpoint),
            binding: self.binding,
            request,
        }
    }

    pub(super) fn bind_received_frame(&self, frame: Frame) -> LegBoundFrame {
        LegBoundFrame {
            seal: self.seal.share(),
            frame,
        }
    }

    pub(super) fn bind_received_control_frame(
        &self,
        frame: LegControlFrame,
    ) -> LegBoundControlFrame {
        LegBoundControlFrame {
            seal: self.seal.share(),
            frame,
        }
    }

    /// Atomically mints the sole outbound queue authority for this live
    /// transport endpoint. The claim remains consumed after queue drop so an
    /// ordered stream cannot be silently replaced by another FIFO.
    pub(super) fn try_claim_outbound_queue(
        &self,
    ) -> Option<(
        LegSeal,
        Weak<LegTransportEndpoint>,
        Arc<LegOutboundQueueLease>,
    )> {
        self.seal
            .try_claim_outbound_queue()
            .map(|lease| (self.seal.share(), Arc::downgrade(&self.endpoint), lease))
    }

    pub(super) fn belongs_to_transport(&self, seal: &LegSeal) -> bool {
        self.seal.same_connection(seal)
    }

    pub(super) fn standby_seal(&self) -> LegSeal {
        self.seal.share()
    }

    pub(super) const fn standby_transport_binding(&self) -> AttachTransportBinding {
        self.binding
    }

    /// Non-owning liveness witness for the exact authenticated endpoint. This
    /// lets an installed attach distinguish "queue not minted yet" from "the
    /// endpoint was destroyed before its sole queue could be minted."
    pub(super) fn endpoint_liveness(&self) -> Weak<LegTransportEndpoint> {
        Arc::downgrade(&self.endpoint)
    }

    /// Authenticates and commits the initial owner ATTACH on this exact
    /// transport without exposing the committed reducer capability.
    ///
    /// The retry-safe pending bootstrap retains authority ownership. After a
    /// successful CAS, every remaining step needed to construct the initial
    /// owner model is infallible and no acceptance can be published before the
    /// supervisor consumes this opaque value.
    pub(super) fn authenticate_initial_owner_attach(
        &self,
        received: LegBoundFrame,
        authority: &AttachAuthority,
    ) -> Result<AuthenticatedInitialOwnerAttach, LegProvenanceError> {
        if !self.seal.same_connection(&received.seal) {
            return Err(LegProvenanceError::WrongLeg);
        }

        let LegBoundFrame { seal, frame } = received;
        let committed = authority
            .verify_and_commit_frame(&frame, &self.binding)
            .map_err(LegProvenanceError::from)?;
        Ok(AuthenticatedInitialOwnerAttach {
            attached: AttachedLeg {
                seal,
                endpoint: Arc::downgrade(&self.endpoint),
                committed,
            },
            acceptance: committed.attach_accepted_frame(),
        })
    }

    /// Authenticates, model-preflights, and commits an ATTACH received on this
    /// exact transport.
    ///
    /// The model preflight token retains its exclusive mutable borrow through
    /// the authority CAS and is consumed immediately afterward. The returned
    /// capability, correlated response, and recovery effects therefore exist
    /// only after model installation; this method still performs no wire I/O.
    pub(crate) fn transact_owner_attach(
        &self,
        received: LegBoundFrame,
        authority: &AttachAuthority,
        model: &mut SessionModel,
    ) -> Result<Result<OwnerAttachTransaction, SessionError>, LegProvenanceError> {
        if !self.seal.same_connection(&received.seal) {
            return Err(LegProvenanceError::WrongLeg);
        }

        let LegBoundFrame { seal, frame } = received;
        match authority
            .preflight_model_and_commit_or_resynchronize_frame(&frame, &self.binding, model)
            .map_err(LegProvenanceError::from)?
        {
            Ok(Ok((committed, recovery))) => Ok(Ok(OwnerAttachTransaction::Installed {
                attached: AttachedLeg {
                    seal,
                    endpoint: Arc::downgrade(&self.endpoint),
                    committed,
                },
                acceptance: committed.attach_accepted_frame(),
                recovery,
            })),
            Ok(Err(error)) => Ok(Err(error)),
            Err(resynchronization) => Ok(Ok(OwnerAttachTransaction::Resynchronize {
                status: resynchronization.status_frame(),
            })),
        }
    }
}

/// Exact initial owner commit whose authority, model seed, and response have
/// not yet been published separately.
///
/// This value is intentionally opaque and non-cloneable. Only the session
/// supervisor may consume it, preventing a byte-path harness from acquiring a
/// bare [`CommittedLeg`] or bypassing owner-model installation.
pub(super) struct AuthenticatedInitialOwnerAttach {
    attached: AttachedLeg,
    acceptance: Frame,
}

impl AuthenticatedInitialOwnerAttach {
    pub(super) fn into_owner_parts(
        self,
        config: SessionConfig,
    ) -> (SessionModel, AttachedLeg, Frame) {
        let model = SessionModel::new(SessionRole::Owner, config, self.attached.committed);
        (model, self.attached, self.acceptance)
    }
}

impl fmt::Debug for AuthenticatedInitialOwnerAttach {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AuthenticatedInitialOwnerAttach([REDACTED])")
    }
}

impl fmt::Debug for EstablishedLeg {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("EstablishedLeg([REDACTED])")
    }
}

/// One attach attempt awaiting a response on the connection that sent it.
///
/// The type is intentionally neither `Clone` nor publicly constructible.  A
/// validation attempt consumes it, so a response cannot mint two capabilities.
pub(crate) struct PendingAttach {
    seal: LegSeal,
    endpoint: Weak<LegTransportEndpoint>,
    binding: AttachTransportBinding,
    request: AttachRequest,
}

impl PendingAttach {
    pub(crate) fn validate_response(
        self,
        response: LegBoundFrame,
    ) -> Result<AttachResponse, LegProvenanceError> {
        if !self.seal.same_connection(&response.seal) {
            return Err(LegProvenanceError::WrongLeg);
        }

        let Self {
            seal,
            endpoint,
            binding,
            request,
        } = self;
        match response.frame.record() {
            Record::AttachAccepted { .. } => request
                .validate_accepted_frame(&binding, &response.frame)
                .map(|committed| {
                    AttachResponse::Accepted(AttachedLeg {
                        seal,
                        endpoint,
                        committed,
                    })
                })
                .map_err(LegProvenanceError::from),
            Record::AttachGenerationStatus { .. } => request
                .validate_generation_status_frame(&binding, &response.frame)
                .map(|status| {
                    AttachResponse::GenerationStatus(LegGenerationStatus { seal, status })
                })
                .map_err(LegProvenanceError::from),
            _ => Err(LegProvenanceError::Rejected),
        }
    }
}

impl fmt::Debug for PendingAttach {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PendingAttach([REDACTED])")
    }
}

/// A decoded frame whose provenance was minted by the exact connection's
/// sole receive adapter.  Fields and constructors remain private.
pub(crate) struct LegBoundFrame {
    seal: LegSeal,
    frame: Frame,
}

/// Classified inbound facts retain disjoint protocol namespaces after exact
/// transport provenance is minted. Leg-control facts cannot be passed to the
/// session reducer without an explicit impossible type conversion.
pub(crate) enum BoundInbound {
    Session(LegBoundFrame),
    LegControl(LegBoundControlFrame),
}

/// One decoded leg-control frame bound to the exact authenticated connection
/// that carried its bytes. Construction remains private to [`EstablishedLeg`].
pub(crate) struct LegBoundControlFrame {
    seal: LegSeal,
    frame: LegControlFrame,
}

impl LegBoundControlFrame {
    pub(super) fn into_parts(self) -> (LegSeal, LegControlFrame) {
        (self.seal, self.frame)
    }
}

/// Authenticated generation status tied to the exact connection that carried
/// the correlated response.
///
/// This non-cloneable wrapper is the only adapter entry into generation
/// catch-up. It retains the status-leg seal until it is consumed while the
/// follow-up attach receives an independent exact-leg seal.
pub(crate) struct LegGenerationStatus {
    seal: LegSeal,
    status: GenerationResynchronization,
}

impl LegGenerationStatus {
    pub(crate) fn current_generation(&self) -> LegGeneration {
        self.status.current_generation()
    }

    pub(crate) fn requested_generation(&self) -> LegGeneration {
        self.status.requested_generation()
    }

    pub(crate) fn transport_binding(&self) -> AttachTransportBinding {
        self.status.transport_binding()
    }

    pub(crate) fn begin_catch_up(
        self,
        follow_up_leg: &EstablishedLeg,
        nonce: crate::resumable::AttachNonce,
    ) -> Result<PendingCatchUpAttach, LegProvenanceError> {
        let pending = self.status.begin_catch_up(nonce)?;
        Ok(PendingCatchUpAttach {
            _status_seal: self.seal,
            follow_up_seal: follow_up_leg.seal.share(),
            follow_up_endpoint: follow_up_leg.endpoint_liveness(),
            follow_up_binding: follow_up_leg.binding,
            pending,
        })
    }
}

impl fmt::Debug for LegGenerationStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("LegGenerationStatus([REDACTED])")
    }
}

/// Status-derived follow-up attach bound to one exact follow-up connection.
pub(crate) struct PendingCatchUpAttach {
    _status_seal: LegSeal,
    follow_up_seal: LegSeal,
    follow_up_endpoint: Weak<LegTransportEndpoint>,
    follow_up_binding: AttachTransportBinding,
    pending: PendingGenerationCatchUp,
}

impl PendingCatchUpAttach {
    pub(crate) fn request(&self) -> AttachRequest {
        self.pending.request()
    }

    pub(crate) fn transport_binding(&self) -> AttachTransportBinding {
        self.follow_up_binding
    }

    pub(crate) fn validate_response(
        self,
        response: LegBoundFrame,
    ) -> Result<CaughtUpAttachedLeg, LegProvenanceError> {
        if !self.follow_up_seal.same_connection(&response.seal) {
            return Err(LegProvenanceError::WrongLeg);
        }
        if !matches!(response.frame.record(), Record::AttachAccepted { .. }) {
            return Err(LegProvenanceError::Rejected);
        }
        let catch_up = self
            .pending
            .validate_accepted_frame(&self.follow_up_binding, &response.frame)?;
        Ok(CaughtUpAttachedLeg {
            seal: self.follow_up_seal,
            endpoint: self.follow_up_endpoint,
            catch_up,
        })
    }
}

impl fmt::Debug for PendingCatchUpAttach {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PendingCatchUpAttach([REDACTED])")
    }
}

/// Successfully caught-up data-plane authority bound to the exact follow-up
/// transport connection.
pub(crate) struct CaughtUpAttachedLeg {
    seal: LegSeal,
    endpoint: Weak<LegTransportEndpoint>,
    catch_up: GenerationCatchUp,
}

impl CaughtUpAttachedLeg {
    pub(crate) fn generation(&self) -> LegGeneration {
        self.catch_up.accepted_leg().generation()
    }

    pub(super) fn transport_is_open(&self) -> bool {
        transport_endpoint_is_open(&self.endpoint)
    }

    /// Consumes a status-derived installed leg only for the terminal fact
    /// minted by its exact follow-up transport.
    #[allow(
        clippy::result_large_err,
        reason = "a mismatch must return both exact non-cloneable capabilities"
    )]
    pub(crate) fn bind_terminal(
        self,
        terminal: ExactLegTerminal,
    ) -> Result<PendingLegLoss, CaughtUpLegTerminalMismatch> {
        if !self.seal.same_connection(&terminal.seal) || terminal.endpoint.is_open() {
            return Err(CaughtUpLegTerminalMismatch {
                caught_up: self,
                terminal,
            });
        }
        Ok(PendingLegLoss {
            leg: self.catch_up.accepted_leg(),
            terminal,
        })
    }

    pub(crate) fn replacement_caught_up_event(&self) -> SessionEvent {
        SessionEvent::ReplacementCaughtUp {
            catch_up: self.catch_up,
        }
    }

    pub(crate) fn accept_frame(
        &self,
        received: LegBoundFrame,
    ) -> Result<SessionEvent, LegProvenanceError> {
        if !self.seal.same_connection(&received.seal) {
            return Err(LegProvenanceError::WrongLeg);
        }
        if is_attach_record(received.frame.record()) {
            return Err(LegProvenanceError::Rejected);
        }
        Ok(SessionEvent::PeerFrame {
            leg: self.catch_up.accepted_leg(),
            frame: received.frame,
        })
    }
}

impl fmt::Debug for CaughtUpAttachedLeg {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CaughtUpAttachedLeg([REDACTED])")
    }
}

/// Ownership-preserving rejection for a status-derived leg paired with a
/// terminal fact from another authenticated connection.
pub(crate) struct CaughtUpLegTerminalMismatch {
    caught_up: CaughtUpAttachedLeg,
    terminal: ExactLegTerminal,
}

impl CaughtUpLegTerminalMismatch {
    pub(crate) fn into_parts(self) -> (CaughtUpAttachedLeg, ExactLegTerminal) {
        (self.caught_up, self.terminal)
    }
}

impl fmt::Debug for CaughtUpLegTerminalMismatch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CaughtUpLegTerminalMismatch([REDACTED])")
    }
}

/// Successfully attached, connection-bound data-plane authority.
///
/// The authenticated [`CommittedLeg`] deliberately remains private inside
/// this non-`Clone` wrapper.  Adapters can mint reducer events only through
/// the methods below, and peer frames must carry the exact same process-local
/// connection seal that authenticated the ATTACH response.
pub(crate) struct AttachedLeg {
    seal: LegSeal,
    endpoint: Weak<LegTransportEndpoint>,
    committed: CommittedLeg,
}

impl AttachedLeg {
    pub(crate) fn generation(&self) -> LegGeneration {
        self.committed.generation()
    }

    pub(crate) fn nonce(&self) -> crate::resumable::AttachNonce {
        self.committed.nonce()
    }

    pub(crate) fn transport_binding(&self) -> AttachTransportBinding {
        self.committed.transport_binding()
    }

    pub(super) fn transport_is_open(&self) -> bool {
        transport_endpoint_is_open(&self.endpoint)
    }

    pub(super) fn standby_registration_request(
        &self,
        nonce: StandbyNonce,
    ) -> Result<StandbyRegistrationRequest, StandbyRegistrationReject> {
        StandbyRegistrationRequest::from_installed_leg(&self.committed, nonce)
    }

    pub(super) fn authenticate_standby_registration(
        &self,
        authority: &AttachAuthority,
        binding: &AttachTransportBinding,
        frame: &LegControlFrame,
    ) -> Result<AuthenticatedStandbyRegistration, StandbyRegistrationReject> {
        authority.verify_standby_registration_frame(&self.committed, binding, frame)
    }

    pub(super) fn belongs_to_transport(&self, seal: &LegSeal) -> bool {
        self.seal.same_connection(seal)
    }

    /// Shares only the process-local seal needed by the supervisor's active
    /// owner barrier; it exposes no committed reducer capability.
    pub(super) fn active_transport_seal(&self) -> LegSeal {
        self.seal.share()
    }

    #[cfg(test)]
    pub(super) fn try_claim_unbound_test_queue(
        &self,
    ) -> Option<(LegSeal, Arc<LegOutboundQueueLease>)> {
        self.seal
            .try_claim_outbound_queue()
            .map(|lease| (self.seal.share(), lease))
    }

    pub(super) fn outbound_queue_was_lost(&self) -> bool {
        self.seal.outbound_queue_was_lost()
    }

    pub(super) fn matches_attach_acceptance(&self, frame: &Frame) -> bool {
        frame.leg_generation() == self.committed.generation()
            && matches!(
                frame.record(),
                Record::AttachAccepted {
                    session_id,
                    nonce,
                    selected_version,
                    features,
                } if *session_id == self.committed.session_id()
                    && *nonce == self.committed.nonce()
                    && *selected_version == self.committed.session_protocol_version()
                    && features.bits() == self.committed.negotiated_features()
            )
    }

    /// Seeds the client reducer while retaining this exact live-leg seal.
    /// The committed capability never leaves the provenance wrapper.
    pub(super) fn initial_client_model(&self, config: SessionConfig) -> SessionModel {
        SessionModel::new(SessionRole::Client, config, self.committed)
    }

    pub(crate) fn replacement_attached_event(&self) -> SessionEvent {
        SessionEvent::ReplacementAttached {
            leg: self.committed,
        }
    }

    /// Consumes the installed leg only when the sole transport reporter
    /// supplied a terminal fact for this exact process-local connection.
    #[allow(
        clippy::result_large_err,
        reason = "a mismatch must return both exact non-cloneable capabilities"
    )]
    pub(crate) fn bind_terminal(
        self,
        terminal: ExactLegTerminal,
    ) -> Result<PendingLegLoss, LegTerminalMismatch> {
        if !self.seal.same_connection(&terminal.seal) || terminal.endpoint.is_open() {
            return Err(LegTerminalMismatch {
                attached: self,
                terminal,
            });
        }
        Ok(PendingLegLoss {
            leg: self.committed,
            terminal,
        })
    }

    pub(crate) fn resume_grace_expired_event(&self) -> SessionEvent {
        SessionEvent::ResumeGraceExpired {
            leg: self.committed,
        }
    }

    pub(crate) fn local_open_event(&self, target: TargetAddr) -> SessionEvent {
        SessionEvent::LocalOpen {
            leg: self.committed,
            target,
        }
    }

    pub(crate) fn accept_frame(
        &self,
        received: LegBoundFrame,
    ) -> Result<SessionEvent, LegProvenanceError> {
        if !self.seal.same_connection(&received.seal) {
            return Err(LegProvenanceError::WrongLeg);
        }
        if is_attach_record(received.frame.record()) {
            return Err(LegProvenanceError::Rejected);
        }
        Ok(SessionEvent::PeerFrame {
            leg: self.committed,
            frame: received.frame,
        })
    }
}

impl fmt::Debug for AttachedLeg {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AttachedLeg([REDACTED])")
    }
}

/// Exact installed-leg loss awaiting its single reducer transition.
pub(crate) struct PendingLegLoss {
    leg: CommittedLeg,
    terminal: ExactLegTerminal,
}

impl PendingLegLoss {
    pub(crate) const fn reason(&self) -> LegTransportTerminalReason {
        self.terminal.reason()
    }

    pub(crate) fn into_event(self) -> SessionEvent {
        let Self { leg, terminal } = self;
        debug_assert!(!terminal.endpoint.is_open());
        SessionEvent::LegLost { leg }
    }
}

impl fmt::Debug for PendingLegLoss {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PendingLegLoss([REDACTED])")
    }
}

/// Exact ownership-preserving rejection for a terminal fact from another
/// authenticated connection.
pub(crate) struct LegTerminalMismatch {
    attached: AttachedLeg,
    terminal: ExactLegTerminal,
}

impl LegTerminalMismatch {
    pub(crate) fn into_parts(self) -> (AttachedLeg, ExactLegTerminal) {
        (self.attached, self.terminal)
    }
}

impl fmt::Debug for LegTerminalMismatch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("LegTerminalMismatch([REDACTED])")
    }
}

impl fmt::Debug for LegBoundFrame {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("LegBoundFrame([REDACTED])")
    }
}

impl fmt::Debug for LegBoundControlFrame {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("LegBoundControlFrame([REDACTED])")
    }
}

impl fmt::Debug for BoundInbound {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Session(_) => formatter.write_str("BoundInbound::Session([REDACTED])"),
            Self::LegControl(_) => formatter.write_str("BoundInbound::LegControl([REDACTED])"),
        }
    }
}

pub(crate) enum AttachResponse {
    Accepted(AttachedLeg),
    GenerationStatus(LegGenerationStatus),
}

/// Owner-side result of consuming one exact-leg ATTACH frame.
///
/// Only a committed result carries data-plane authority.  A resynchronization
/// result contains solely its correlated status frame.
#[allow(
    clippy::large_enum_variant,
    reason = "exact non-cloneable attach ownership stays inline without adding allocation to the successful control path"
)]
pub(crate) enum OwnerAttachTransaction {
    Installed {
        attached: AttachedLeg,
        acceptance: Frame,
        recovery: Vec<SessionEffect>,
    },
    Resynchronize {
        status: Frame,
    },
}

impl fmt::Debug for OwnerAttachTransaction {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Installed { .. } => {
                formatter.write_str("OwnerAttachTransaction::Installed([REDACTED])")
            }
            Self::Resynchronize { .. } => {
                formatter.write_str("OwnerAttachTransaction::Resynchronize([REDACTED])")
            }
        }
    }
}

impl fmt::Debug for AttachResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Accepted(_) => formatter.write_str("AttachResponse::Accepted([REDACTED])"),
            Self::GenerationStatus(_) => {
                formatter.write_str("AttachResponse::GenerationStatus([REDACTED])")
            }
        }
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub(crate) enum LegProvenanceError {
    #[error("attach response arrived on a different authenticated transport leg")]
    WrongLeg,
    #[error("attach response rejected")]
    Rejected,
}

impl From<AttachReject> for LegProvenanceError {
    fn from(_: AttachReject) -> Self {
        Self::Rejected
    }
}

fn is_attach_record(record: &Record) -> bool {
    matches!(
        record,
        Record::Attach { .. }
            | Record::AttachAccepted { .. }
            | Record::AttachGenerationStatus { .. }
    )
}
