//! Exact-leg standby registration for one already-installed resumable session.
//!
//! This module owns only the bounded registration typestate. It cannot commit
//! an attach generation, mutate the session reducer, or mint data-plane
//! authority.

use super::leg::{
    AttachResponse, AttachedLeg, BoundInbound, CaughtUpAttachedLeg, EstablishedLeg,
    ExactLegTerminal, LegBoundFrame, LegGenerationStatus, LegProvenanceError, LegSeal,
    LegTransportEndpoint, LostPendingAttach, PendingAttach, PendingCatchUpAttach,
};
use super::leg_io::{
    AttachAcceptanceReservation, AttachAcceptanceReserveError, AttachRequestEnqueued,
    CatchUpAttachEnqueued, LegControlEnqueued, LegControlQueueErrorKind, LegOutboundQueue,
    LegOutboundQueueErrorKind, StatusRetryAttachEnqueued,
};
use crate::resumable::{
    AttachAuthority, AttachCredentials, AttachNonce, AttachRequest, AttachTransportBinding,
    AuthenticatedStandbyRegistration, FeatureOffer, FeatureSet, Frame, LegControlFrame,
    LegControlRecord, LegGeneration, SessionId, StandbyNonce, StandbyRegistrationReject,
    VersionRange,
};
use std::fmt;
use std::sync::Weak;
use thiserror::Error;

fn endpoint_is_open(endpoint: &Weak<LegTransportEndpoint>) -> bool {
    endpoint
        .upgrade()
        .is_some_and(|endpoint| endpoint.is_open())
}

/// One-shot authority minted only while consuming an exact registered
/// standby. Its private fields prevent another `owned_upstream` sibling from
/// synthesizing a raw response validator from an arbitrary established leg.
pub(super) struct RegisteredPendingAttachAuthority {
    seal: LegSeal,
    endpoint: Weak<LegTransportEndpoint>,
    binding: AttachTransportBinding,
    request: AttachRequest,
}

impl RegisteredPendingAttachAuthority {
    fn new(
        seal: LegSeal,
        endpoint: Weak<LegTransportEndpoint>,
        binding: AttachTransportBinding,
        request: AttachRequest,
    ) -> Self {
        Self {
            seal,
            endpoint,
            binding,
            request,
        }
    }

    pub(super) fn into_parts(
        self,
    ) -> (
        LegSeal,
        Weak<LegTransportEndpoint>,
        AttachTransportBinding,
        AttachRequest,
    ) {
        (self.seal, self.endpoint, self.binding, self.request)
    }
}

/// Delivery gate minted only after a typed B/C request receipt has proved
/// actual transport completion.
pub(super) struct RegisteredAttachResponseGate {
    _private: (),
}

impl RegisteredAttachResponseGate {
    fn delivered() -> Self {
        Self { _private: () }
    }
}

/// Gate proving that catch-up derivation began from the opaque registered
/// generation-status wrapper which still owns its exact C receipt.
pub(super) struct RegisteredCatchUpBeginGate {
    _private: (),
}

impl RegisteredCatchUpBeginGate {
    fn from_registered_status() -> Self {
        Self { _private: () }
    }
}

/// Gate allowing only the registered wrapper to reassemble internally signed
/// catch-up parts with the retained status receipt.
pub(super) struct RegisteredCatchUpAssemblyGate {
    _private: (),
}

impl RegisteredCatchUpAssemblyGate {
    fn from_registered_status() -> Self {
        Self { _private: () }
    }
}

/// Delivery gate minted only after the exact D queue receipt is live and its
/// signed ATTACH bytes were actually transported.
pub(super) struct RegisteredCatchUpResponseGate {
    _private: (),
}

impl RegisteredCatchUpResponseGate {
    fn delivered() -> Self {
        Self { _private: () }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct StandbyContract {
    session_id: SessionId,
    generation: LegGeneration,
    nonce: StandbyNonce,
    selected_version: u16,
    features: FeatureSet,
    binding: AttachTransportBinding,
}

impl StandbyContract {
    fn from_registration(
        frame: &LegControlFrame,
        binding: AttachTransportBinding,
    ) -> Result<Self, StandbyRegistrationError> {
        let LegControlRecord::StandbyRegister {
            session_id,
            standby_nonce,
            selected_version,
            features,
            ..
        } = frame.record()
        else {
            return Err(StandbyRegistrationError::Rejected);
        };
        Ok(Self {
            session_id: *session_id,
            generation: frame.leg_generation(),
            nonce: *standby_nonce,
            selected_version: *selected_version,
            features: *features,
            binding,
        })
    }

    fn from_authenticated(authentication: &AuthenticatedStandbyRegistration) -> Self {
        Self {
            session_id: authentication.session_id(),
            generation: authentication.current_generation(),
            nonce: authentication.standby_nonce(),
            selected_version: authentication.selected_version(),
            features: authentication.features(),
            binding: authentication.transport_binding(),
        }
    }

    fn acceptance_frame(self) -> Result<LegControlFrame, StandbyRegistrationReject> {
        LegControlFrame::try_new(
            self.generation,
            LegControlRecord::StandbyAccepted {
                session_id: self.session_id,
                standby_nonce: self.nonce,
                selected_version: self.selected_version,
                features: self.features,
            },
            self.features,
        )
        .map_err(|_| StandbyRegistrationReject::Rejected)
    }

    fn matches_acceptance(self, frame: &LegControlFrame) -> bool {
        frame.leg_generation() == self.generation
            && matches!(
                frame.record(),
                LegControlRecord::StandbyAccepted {
                    session_id,
                    standby_nonce,
                    selected_version,
                    features,
                } if *session_id == self.session_id
                    && *standby_nonce == self.nonce
                    && *selected_version == self.selected_version
                    && *features == self.features
            )
    }

    fn matches_exact_next_attach(self, request: AttachRequest) -> bool {
        let Some(next) = self.generation.get().checked_add(1) else {
            return false;
        };
        request.session_id() == self.session_id
            && request.requested_generation().get() == next
            && request.versions().min() == self.selected_version
            && request.versions().max() == self.selected_version
            && request.features().offered() == self.features.bits()
            && request.features().required() == self.features.bits()
    }
}

impl fmt::Debug for StandbyContract {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StandbyContract")
            .field("correlation", &"[REDACTED]")
            .field("generation", &self.generation)
            .field("selected_version", &self.selected_version)
            .field("features", &self.features)
            .field("transport_binding", &"[REDACTED]")
            .finish()
    }
}

/// Client registration request signed for one exact candidate transport. The
/// request is retained until it enters that leg's sole bounded FIFO.
pub(crate) struct PendingStandbyRegistration {
    seal: LegSeal,
    endpoint: Weak<LegTransportEndpoint>,
    contract: StandbyContract,
    frame: LegControlFrame,
}

impl PendingStandbyRegistration {
    pub(crate) fn begin(
        active: &AttachedLeg,
        candidate: &EstablishedLeg,
        nonce: StandbyNonce,
        credentials: &AttachCredentials,
    ) -> Result<Self, StandbyRegistrationError> {
        let seal = candidate.standby_seal();
        let binding = candidate.standby_transport_binding();
        if active.belongs_to_transport(&seal)
            || active.transport_binding() == binding
            || seal.outbound_queue_was_lost()
        {
            return Err(StandbyRegistrationError::Rejected);
        }
        let endpoint = candidate.endpoint_liveness();
        if !endpoint_is_open(&endpoint) {
            return Err(StandbyRegistrationError::EndpointLost);
        }
        let request = active
            .standby_registration_request(nonce)
            .map_err(|_| StandbyRegistrationError::Rejected)?;
        let proof = credentials
            .prove_standby_registration(&request, &binding)
            .map_err(|_| StandbyRegistrationError::Rejected)?;
        let frame = request
            .to_frame(proof)
            .map_err(|_| StandbyRegistrationError::Rejected)?;
        let contract = StandbyContract::from_registration(&frame, binding)?;
        Ok(Self {
            seal,
            endpoint,
            contract,
            frame,
        })
    }

    #[allow(
        clippy::result_large_err,
        reason = "bounded pressure must return the exact non-cloneable registration without allocating"
    )]
    pub(crate) fn enqueue(
        mut self,
        queue: &mut LegOutboundQueue,
    ) -> Result<AwaitingStandbyAccepted, StandbyEnqueueFailure> {
        if !endpoint_is_open(&self.endpoint) {
            return Err(StandbyEnqueueFailure::new(
                self,
                LegControlQueueErrorKind::EndpointLost,
            ));
        }
        let retained = self.frame.clone();
        match queue.push_leg_control(&self.seal, self.frame, self.contract.features) {
            Ok(receipt) => Ok(AwaitingStandbyAccepted {
                seal: self.seal,
                endpoint: self.endpoint,
                contract: self.contract,
                retry_frame: retained,
                receipt,
            }),
            Err(error) => {
                let kind = error.kind().clone();
                self.frame = error.into_frame();
                Err(StandbyEnqueueFailure::new(self, kind))
            }
        }
    }
}

impl fmt::Debug for PendingStandbyRegistration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PendingStandbyRegistration([REDACTED])")
    }
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub(crate) enum StandbyRegistrationError {
    #[error("standby registration rejected")]
    Rejected,
    #[error("standby transport endpoint was lost")]
    EndpointLost,
}

pub(crate) struct StandbyEnqueueFailure {
    pending: PendingStandbyRegistration,
    kind: LegControlQueueErrorKind,
}

impl StandbyEnqueueFailure {
    fn new(pending: PendingStandbyRegistration, kind: LegControlQueueErrorKind) -> Self {
        Self { pending, kind }
    }

    pub(crate) const fn kind(&self) -> &LegControlQueueErrorKind {
        &self.kind
    }

    pub(crate) fn into_pending(self) -> PendingStandbyRegistration {
        self.pending
    }
}

impl fmt::Debug for StandbyEnqueueFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StandbyEnqueueFailure")
            .field("kind", &self.kind)
            .field("pending", &"[REDACTED]")
            .finish()
    }
}

/// One successfully queued registration awaiting its exact B acceptance.
pub(crate) struct AwaitingStandbyAccepted {
    seal: LegSeal,
    endpoint: Weak<LegTransportEndpoint>,
    contract: StandbyContract,
    retry_frame: LegControlFrame,
    receipt: LegControlEnqueued,
}

impl AwaitingStandbyAccepted {
    pub(crate) fn retry(&mut self, queue: &mut LegOutboundQueue) -> Result<(), StandbyRetryError> {
        if !self.receipt.queue_is_live() || !self.receipt.belongs_to_queue(queue) {
            return Err(StandbyRetryError::QueueLost);
        }
        queue
            .push_leg_control(&self.seal, self.retry_frame.clone(), self.contract.features)
            .map(|_| ())
            .map_err(|error| StandbyRetryError::Queue(error.kind().clone()))
    }

    #[allow(
        clippy::result_large_err,
        reason = "failed consuming validation must return the exact awaiting capability"
    )]
    pub(crate) fn validate(
        self,
        inbound: BoundInbound,
    ) -> Result<ClientRegisteredStandby, StandbyAcceptanceFailure> {
        if !self.receipt.queue_is_live() {
            return Err(StandbyAcceptanceFailure::new(
                self,
                StandbyAcceptanceErrorKind::QueueLost,
            ));
        }
        let BoundInbound::LegControl(inbound) = inbound else {
            return Err(StandbyAcceptanceFailure::new(
                self,
                StandbyAcceptanceErrorKind::Rejected,
            ));
        };
        let (seal, frame) = inbound.into_parts();
        if !self.seal.same_connection(&seal) || !self.contract.matches_acceptance(&frame) {
            return Err(StandbyAcceptanceFailure::new(
                self,
                StandbyAcceptanceErrorKind::Rejected,
            ));
        }
        Ok(ClientRegisteredStandby {
            seal: self.seal,
            endpoint: self.endpoint,
            contract: self.contract,
            receipt: self.receipt,
        })
    }
}

impl fmt::Debug for AwaitingStandbyAccepted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AwaitingStandbyAccepted([REDACTED])")
    }
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub(crate) enum StandbyRetryError {
    #[error("standby registration queue was lost")]
    QueueLost,
    #[error("standby registration retry queue rejected the record: {0:?}")]
    Queue(LegControlQueueErrorKind),
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub(crate) enum StandbyAcceptanceErrorKind {
    #[error("standby acceptance rejected")]
    Rejected,
    #[error("standby acceptance queue was lost")]
    QueueLost,
}

pub(crate) struct StandbyAcceptanceFailure {
    awaiting: AwaitingStandbyAccepted,
    kind: StandbyAcceptanceErrorKind,
}

impl StandbyAcceptanceFailure {
    fn new(awaiting: AwaitingStandbyAccepted, kind: StandbyAcceptanceErrorKind) -> Self {
        Self { awaiting, kind }
    }

    pub(crate) const fn kind(&self) -> StandbyAcceptanceErrorKind {
        self.kind
    }

    pub(crate) fn into_awaiting(self) -> AwaitingStandbyAccepted {
        self.awaiting
    }
}

impl fmt::Debug for StandbyAcceptanceFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StandbyAcceptanceFailure")
            .field("kind", &self.kind)
            .field("awaiting", &"[REDACTED]")
            .finish()
    }
}

/// Client-side registered standby. It deliberately exposes no session event
/// or attached-leg conversion.
pub(crate) struct ClientRegisteredStandby {
    seal: LegSeal,
    endpoint: Weak<LegTransportEndpoint>,
    contract: StandbyContract,
    receipt: LegControlEnqueued,
}

impl ClientRegisteredStandby {
    pub(crate) fn is_live(&self) -> bool {
        let _exact_identity = (&self.seal, self.contract);
        endpoint_is_open(&self.endpoint) && self.receipt.queue_is_live()
    }

    /// Consumes the exact registered B capability and derives its sole
    /// next-generation ATTACH. Session, generation, negotiated version,
    /// features, and transport binding come only from the authenticated
    /// standby contract; callers supply only a fresh correlation nonce and
    /// the signing credentials.
    #[allow(
        clippy::result_large_err,
        reason = "begin rejection returns the exact non-cloneable registered standby"
    )]
    pub(crate) fn begin_exact_next_attach(
        self,
        nonce: AttachNonce,
        credentials: &AttachCredentials,
    ) -> Result<PendingRegisteredAttach, RegisteredAttachBeginFailure> {
        if !self.is_live() {
            return Err(RegisteredAttachBeginFailure::new(
                self,
                RegisteredAttachBeginErrorKind::EndpointOrQueueLost,
            ));
        }
        let Some(next_generation) = self.contract.generation.get().checked_add(1) else {
            return Err(RegisteredAttachBeginFailure::new(
                self,
                RegisteredAttachBeginErrorKind::GenerationExhausted,
            ));
        };
        let requested_generation = match LegGeneration::new(next_generation) {
            Ok(generation) => generation,
            Err(_) => {
                return Err(RegisteredAttachBeginFailure::new(
                    self,
                    RegisteredAttachBeginErrorKind::GenerationExhausted,
                ));
            }
        };
        let versions = match VersionRange::new(
            self.contract.selected_version,
            self.contract.selected_version,
        ) {
            Ok(versions) => versions,
            Err(_) => {
                return Err(RegisteredAttachBeginFailure::new(
                    self,
                    RegisteredAttachBeginErrorKind::Rejected,
                ));
            }
        };
        let features =
            match FeatureOffer::new(self.contract.features.bits(), self.contract.features.bits()) {
                Ok(features) => features,
                Err(_) => {
                    return Err(RegisteredAttachBeginFailure::new(
                        self,
                        RegisteredAttachBeginErrorKind::Rejected,
                    ));
                }
            };
        let request = AttachRequest::new(
            self.contract.session_id,
            requested_generation,
            nonce,
            versions,
            features,
        );
        let proof = match credentials.prove(&request, &self.contract.binding) {
            Ok(proof) => proof,
            Err(_) => {
                return Err(RegisteredAttachBeginFailure::new(
                    self,
                    RegisteredAttachBeginErrorKind::Rejected,
                ));
            }
        };
        let frame = request.to_attach_frame(proof);
        let authority = RegisteredPendingAttachAuthority::new(
            self.seal.share(),
            self.endpoint.clone(),
            self.contract.binding,
            request,
        );
        Ok(PendingRegisteredAttach {
            seal: self.seal,
            endpoint: self.endpoint,
            request,
            frame,
            registration: self.receipt,
            authority,
        })
    }

    #[allow(
        clippy::result_large_err,
        reason = "live retirement rejection must preserve the exact non-cloneable capability"
    )]
    pub(crate) fn retire_lost(self) -> Result<RetiredClientStandby, Self> {
        if self.is_live() {
            Err(self)
        } else {
            Ok(RetiredClientStandby)
        }
    }
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub(crate) enum RegisteredAttachBeginErrorKind {
    #[error("registered standby endpoint or sole queue was lost")]
    EndpointOrQueueLost,
    #[error("registered standby generation is exhausted")]
    GenerationExhausted,
    #[error("registered standby attach derivation was rejected")]
    Rejected,
}

pub(crate) struct RegisteredAttachBeginFailure {
    registered: ClientRegisteredStandby,
    kind: RegisteredAttachBeginErrorKind,
}

impl RegisteredAttachBeginFailure {
    fn new(registered: ClientRegisteredStandby, kind: RegisteredAttachBeginErrorKind) -> Self {
        Self { registered, kind }
    }

    pub(crate) const fn kind(&self) -> RegisteredAttachBeginErrorKind {
        self.kind
    }

    pub(crate) fn into_registered(self) -> ClientRegisteredStandby {
        self.registered
    }
}

impl fmt::Debug for RegisteredAttachBeginFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RegisteredAttachBeginFailure")
            .field("kind", &self.kind)
            .field("registered", &"[REDACTED]")
            .finish()
    }
}

/// Exact registered-standby ATTACH retained until the sole B queue admits
/// its signed bytes. Neither the request nor its frame is exposed to the
/// controller.
pub(crate) struct PendingRegisteredAttach {
    seal: LegSeal,
    endpoint: Weak<LegTransportEndpoint>,
    request: AttachRequest,
    frame: Frame,
    registration: LegControlEnqueued,
    authority: RegisteredPendingAttachAuthority,
}

impl PendingRegisteredAttach {
    pub(crate) fn encoded_len(&self) -> Result<usize, crate::resumable::ProtocolError> {
        self.frame.encode().map(|encoded| encoded.len())
    }

    #[allow(
        clippy::result_large_err,
        reason = "bounded queue pressure returns the exact registered attach"
    )]
    pub(crate) fn enqueue(
        mut self,
        queue: &mut LegOutboundQueue,
    ) -> Result<AwaitingRegisteredAttach, RegisteredAttachEnqueueFailure> {
        if !endpoint_is_open(&self.endpoint)
            || !self.registration.queue_is_live()
            || !self.registration.belongs_to_queue(queue)
        {
            return Err(RegisteredAttachEnqueueFailure::new(
                self,
                LegOutboundQueueErrorKind::EndpointLost,
            ));
        }
        match queue.push_registered_attach_request(&self.seal, self.request, self.frame) {
            Ok(request_receipt) => Ok(AwaitingRegisteredAttach {
                pending: PendingAttach::for_registered_standby(self.authority),
                request_receipt,
            }),
            Err(error) => {
                let kind = error.kind().clone();
                self.frame = error.into_frame();
                Err(RegisteredAttachEnqueueFailure::new(self, kind))
            }
        }
    }
}

impl fmt::Debug for PendingRegisteredAttach {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PendingRegisteredAttach([REDACTED])")
    }
}

pub(crate) struct RegisteredAttachEnqueueFailure {
    pending: PendingRegisteredAttach,
    kind: LegOutboundQueueErrorKind,
}

impl RegisteredAttachEnqueueFailure {
    fn new(pending: PendingRegisteredAttach, kind: LegOutboundQueueErrorKind) -> Self {
        Self { pending, kind }
    }

    pub(crate) const fn kind(&self) -> &LegOutboundQueueErrorKind {
        &self.kind
    }

    pub(crate) fn into_pending(self) -> PendingRegisteredAttach {
        self.pending
    }
}

impl fmt::Debug for RegisteredAttachEnqueueFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RegisteredAttachEnqueueFailure")
            .field("kind", &self.kind)
            .field("pending", &"[REDACTED]")
            .finish()
    }
}

/// Registered B request whose exact queue admission and response provenance
/// must be consumed together.
pub(crate) struct AwaitingRegisteredAttach {
    pending: PendingAttach,
    request_receipt: AttachRequestEnqueued,
}

impl AwaitingRegisteredAttach {
    #[allow(
        clippy::result_large_err,
        reason = "invalid response returns exact pending and received ownership"
    )]
    pub(crate) fn validate_response(
        self,
        inbound: BoundInbound,
    ) -> Result<RegisteredAttachResponse, RegisteredAttachResponseFailure> {
        if !self.request_receipt.queue_is_live() {
            return Err(RegisteredAttachResponseFailure::new(
                self,
                inbound,
                RegisteredAttachResponseErrorKind::QueueLost,
            ));
        }
        if !self.request_receipt.was_delivered() {
            return Err(RegisteredAttachResponseFailure::new(
                self,
                inbound,
                RegisteredAttachResponseErrorKind::RequestNotDelivered,
            ));
        }
        let BoundInbound::Session(received) = inbound else {
            return Err(RegisteredAttachResponseFailure::new(
                self,
                inbound,
                RegisteredAttachResponseErrorKind::Rejected,
            ));
        };
        let Self {
            pending,
            request_receipt,
        } = self;
        match pending.validate_registered_response_preserving(
            received,
            RegisteredAttachResponseGate::delivered(),
        ) {
            Ok(AttachResponse::Accepted(attached)) => Ok(RegisteredAttachResponse::Accepted(
                AcceptedClientRegisteredAttach {
                    attached,
                    request_receipt,
                },
            )),
            Ok(AttachResponse::GenerationStatus(status)) => Ok(
                RegisteredAttachResponse::GenerationStatus(RegisteredAttachGenerationStatus {
                    status,
                    request_receipt: AttachStatusRequestReceipt::Registered(request_receipt),
                }),
            ),
            Err(failure) => {
                let (pending, received, kind) = failure.into_parts();
                Err(RegisteredAttachResponseFailure::new(
                    Self {
                        pending,
                        request_receipt,
                    },
                    BoundInbound::Session(received),
                    kind.into(),
                ))
            }
        }
    }

    /// Retires the old B validator only for its exact terminal fact after the
    /// signed request completed ordered transport delivery. The returned
    /// authority can re-sign that immutable request once for a fresh status
    /// leg; a late B acceptance has no remaining validator capability.
    #[allow(
        clippy::result_large_err,
        reason = "mismatch returns exact awaiting request plus terminal fact"
    )]
    pub(crate) fn into_status_retry_after_terminal(
        self,
        terminal: ExactLegTerminal,
    ) -> Result<RegisteredAttachStatusRetryAuthority, RegisteredAttachTerminalRetryFailure> {
        if !self.request_receipt.was_delivered() {
            return Err(RegisteredAttachTerminalRetryFailure::new(
                self,
                terminal,
                RegisteredAttachStatusRetryErrorKind::RequestNotDelivered,
            ));
        }
        let Self {
            pending,
            request_receipt,
        } = self;
        match pending.into_lost_after_terminal(terminal) {
            Ok(lost) => {
                drop(request_receipt);
                Ok(RegisteredAttachStatusRetryAuthority { lost })
            }
            Err(failure) => {
                let (pending, terminal) = failure.into_parts();
                Err(RegisteredAttachTerminalRetryFailure::new(
                    Self {
                        pending,
                        request_receipt,
                    },
                    terminal,
                    RegisteredAttachStatusRetryErrorKind::WrongTerminal,
                ))
            }
        }
    }

    /// Queue-loss counterpart to [`Self::into_status_retry_after_terminal`].
    /// Merely holding a live B or an unminted queue cannot authorize a retry.
    #[allow(
        clippy::result_large_err,
        reason = "live or undelivered rejection returns the exact awaiting request"
    )]
    pub(crate) fn into_status_retry_after_queue_loss(
        self,
    ) -> Result<RegisteredAttachStatusRetryAuthority, RegisteredAttachQueueLossRetryFailure> {
        if !self.request_receipt.was_delivered() || self.request_receipt.queue_is_live() {
            let kind = if self.request_receipt.was_delivered() {
                RegisteredAttachStatusRetryErrorKind::OriginalTransportStillLive
            } else {
                RegisteredAttachStatusRetryErrorKind::RequestNotDelivered
            };
            return Err(RegisteredAttachQueueLossRetryFailure::new(self, kind));
        }
        let Self {
            pending,
            request_receipt,
        } = self;
        match pending.into_lost_after_queue_loss() {
            Ok(lost) => {
                drop(request_receipt);
                Ok(RegisteredAttachStatusRetryAuthority { lost })
            }
            Err(pending) => Err(RegisteredAttachQueueLossRetryFailure::new(
                Self {
                    pending,
                    request_receipt,
                },
                RegisteredAttachStatusRetryErrorKind::OriginalTransportStillLive,
            )),
        }
    }
}

impl fmt::Debug for AwaitingRegisteredAttach {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AwaitingRegisteredAttach([REDACTED])")
    }
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub(crate) enum RegisteredAttachStatusRetryErrorKind {
    #[error("registered attach request was not actually delivered")]
    RequestNotDelivered,
    #[error("terminal fact belongs to another authenticated transport")]
    WrongTerminal,
    #[error("registered attach transport and sole queue remain live")]
    OriginalTransportStillLive,
    #[error("fresh status retry transport was rejected")]
    FreshStatusTransportRejected,
}

pub(crate) struct RegisteredAttachTerminalRetryFailure {
    awaiting: AwaitingRegisteredAttach,
    terminal: ExactLegTerminal,
    kind: RegisteredAttachStatusRetryErrorKind,
}

impl RegisteredAttachTerminalRetryFailure {
    fn new(
        awaiting: AwaitingRegisteredAttach,
        terminal: ExactLegTerminal,
        kind: RegisteredAttachStatusRetryErrorKind,
    ) -> Self {
        Self {
            awaiting,
            terminal,
            kind,
        }
    }

    pub(crate) const fn kind(&self) -> RegisteredAttachStatusRetryErrorKind {
        self.kind
    }

    pub(crate) fn into_parts(self) -> (AwaitingRegisteredAttach, ExactLegTerminal) {
        (self.awaiting, self.terminal)
    }
}

impl fmt::Debug for RegisteredAttachTerminalRetryFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RegisteredAttachTerminalRetryFailure")
            .field("kind", &self.kind)
            .field("ownership", &"[REDACTED]")
            .finish()
    }
}

pub(crate) struct RegisteredAttachQueueLossRetryFailure {
    awaiting: AwaitingRegisteredAttach,
    kind: RegisteredAttachStatusRetryErrorKind,
}

impl RegisteredAttachQueueLossRetryFailure {
    fn new(awaiting: AwaitingRegisteredAttach, kind: RegisteredAttachStatusRetryErrorKind) -> Self {
        Self { awaiting, kind }
    }

    pub(crate) const fn kind(&self) -> RegisteredAttachStatusRetryErrorKind {
        self.kind
    }

    pub(crate) fn into_awaiting(self) -> AwaitingRegisteredAttach {
        self.awaiting
    }
}

impl fmt::Debug for RegisteredAttachQueueLossRetryFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RegisteredAttachQueueLossRetryFailure")
            .field("kind", &self.kind)
            .field("awaiting", &"[REDACTED]")
            .finish()
    }
}

/// Delivered B request whose original response validator has been consumed by
/// exact terminal/queue-loss evidence. It can bind only one fresh C transport.
pub(crate) struct RegisteredAttachStatusRetryAuthority {
    lost: LostPendingAttach,
}

impl RegisteredAttachStatusRetryAuthority {
    #[allow(
        clippy::result_large_err,
        reason = "fresh status leg rejection returns the exact retry authority"
    )]
    pub(crate) fn bind_fresh_status_leg(
        self,
        leg: &EstablishedLeg,
        credentials: &AttachCredentials,
    ) -> Result<PendingRegisteredAttachStatusRetry, RegisteredAttachStatusRetryBindFailure> {
        match self.lost.bind_fresh_status_leg(leg, credentials) {
            Ok(signed) => {
                let (seal, pending, request, frame) = signed.into_parts();
                Ok(PendingRegisteredAttachStatusRetry {
                    seal,
                    pending,
                    request,
                    frame,
                })
            }
            Err(failure) => {
                let (lost, _source) = failure.into_parts();
                Err(RegisteredAttachStatusRetryBindFailure {
                    authority: Self { lost },
                    kind: RegisteredAttachStatusRetryErrorKind::FreshStatusTransportRejected,
                })
            }
        }
    }
}

impl fmt::Debug for RegisteredAttachStatusRetryAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RegisteredAttachStatusRetryAuthority([REDACTED])")
    }
}

pub(crate) struct RegisteredAttachStatusRetryBindFailure {
    authority: RegisteredAttachStatusRetryAuthority,
    kind: RegisteredAttachStatusRetryErrorKind,
}

impl RegisteredAttachStatusRetryBindFailure {
    pub(crate) const fn kind(&self) -> RegisteredAttachStatusRetryErrorKind {
        self.kind
    }

    pub(crate) fn into_authority(self) -> RegisteredAttachStatusRetryAuthority {
        self.authority
    }
}

impl fmt::Debug for RegisteredAttachStatusRetryBindFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RegisteredAttachStatusRetryBindFailure")
            .field("kind", &self.kind)
            .field("authority", &"[REDACTED]")
            .finish()
    }
}

pub(crate) struct PendingRegisteredAttachStatusRetry {
    seal: LegSeal,
    pending: PendingAttach,
    request: AttachRequest,
    frame: Frame,
}

impl PendingRegisteredAttachStatusRetry {
    #[allow(
        clippy::result_large_err,
        reason = "bounded pressure returns the exact signed status retry"
    )]
    pub(crate) fn enqueue(
        mut self,
        queue: &mut LegOutboundQueue,
    ) -> Result<AwaitingRegisteredAttachStatus, RegisteredAttachStatusRetryEnqueueFailure> {
        match queue.push_status_retry_attach_request(&self.seal, self.request, self.frame) {
            Ok(receipt) => Ok(AwaitingRegisteredAttachStatus {
                pending: self.pending,
                request_receipt: receipt,
            }),
            Err(error) => {
                let kind = error.kind().clone();
                self.frame = error.into_frame();
                Err(RegisteredAttachStatusRetryEnqueueFailure {
                    pending: self,
                    kind,
                })
            }
        }
    }
}

impl fmt::Debug for PendingRegisteredAttachStatusRetry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PendingRegisteredAttachStatusRetry([REDACTED])")
    }
}

pub(crate) struct RegisteredAttachStatusRetryEnqueueFailure {
    pending: PendingRegisteredAttachStatusRetry,
    kind: LegOutboundQueueErrorKind,
}

impl RegisteredAttachStatusRetryEnqueueFailure {
    pub(crate) const fn kind(&self) -> &LegOutboundQueueErrorKind {
        &self.kind
    }

    pub(crate) fn into_pending(self) -> PendingRegisteredAttachStatusRetry {
        self.pending
    }
}

impl fmt::Debug for RegisteredAttachStatusRetryEnqueueFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RegisteredAttachStatusRetryEnqueueFailure")
            .field("kind", &self.kind)
            .field("pending", &"[REDACTED]")
            .finish()
    }
}

pub(crate) struct AwaitingRegisteredAttachStatus {
    pending: PendingAttach,
    request_receipt: StatusRetryAttachEnqueued,
}

impl AwaitingRegisteredAttachStatus {
    #[allow(
        clippy::result_large_err,
        reason = "wrong status returns exact pending request and inbound fact"
    )]
    pub(crate) fn validate_status(
        self,
        inbound: BoundInbound,
    ) -> Result<RegisteredAttachGenerationStatus, RegisteredAttachStatusResponseFailure> {
        if !self.request_receipt.queue_is_live() {
            return Err(RegisteredAttachStatusResponseFailure::new(
                self,
                inbound,
                RegisteredAttachResponseErrorKind::QueueLost,
            ));
        }
        if !self.request_receipt.was_delivered() {
            return Err(RegisteredAttachStatusResponseFailure::new(
                self,
                inbound,
                RegisteredAttachResponseErrorKind::RequestNotDelivered,
            ));
        }
        let BoundInbound::Session(received) = inbound else {
            return Err(RegisteredAttachStatusResponseFailure::new(
                self,
                inbound,
                RegisteredAttachResponseErrorKind::Rejected,
            ));
        };
        let Self {
            pending,
            request_receipt,
        } = self;
        match pending.validate_registered_generation_status_preserving(
            received,
            RegisteredAttachResponseGate::delivered(),
        ) {
            Ok(status) => Ok(RegisteredAttachGenerationStatus {
                status,
                request_receipt: AttachStatusRequestReceipt::StatusRetry(request_receipt),
            }),
            Err(failure) => {
                let (pending, received, kind) = failure.into_parts();
                Err(RegisteredAttachStatusResponseFailure::new(
                    Self {
                        pending,
                        request_receipt,
                    },
                    BoundInbound::Session(received),
                    kind.into(),
                ))
            }
        }
    }
}

impl fmt::Debug for AwaitingRegisteredAttachStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AwaitingRegisteredAttachStatus([REDACTED])")
    }
}

pub(crate) struct RegisteredAttachStatusResponseFailure {
    awaiting: AwaitingRegisteredAttachStatus,
    inbound: BoundInbound,
    kind: RegisteredAttachResponseErrorKind,
}

impl RegisteredAttachStatusResponseFailure {
    fn new(
        awaiting: AwaitingRegisteredAttachStatus,
        inbound: BoundInbound,
        kind: RegisteredAttachResponseErrorKind,
    ) -> Self {
        Self {
            awaiting,
            inbound,
            kind,
        }
    }

    pub(crate) const fn kind(&self) -> RegisteredAttachResponseErrorKind {
        self.kind
    }

    pub(crate) fn into_parts(self) -> (AwaitingRegisteredAttachStatus, BoundInbound) {
        (self.awaiting, self.inbound)
    }
}

impl fmt::Debug for RegisteredAttachStatusResponseFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RegisteredAttachStatusResponseFailure")
            .field("kind", &self.kind)
            .field("ownership", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub(crate) enum RegisteredAttachResponseErrorKind {
    #[error("registered attach response was rejected")]
    Rejected,
    #[error("registered attach sole queue was lost")]
    QueueLost,
    #[error("registered attach request has not completed exact transport delivery")]
    RequestNotDelivered,
}

impl From<LegProvenanceError> for RegisteredAttachResponseErrorKind {
    fn from(_: LegProvenanceError) -> Self {
        Self::Rejected
    }
}

pub(crate) struct RegisteredAttachResponseFailure {
    awaiting: AwaitingRegisteredAttach,
    inbound: BoundInbound,
    kind: RegisteredAttachResponseErrorKind,
}

impl RegisteredAttachResponseFailure {
    fn new(
        awaiting: AwaitingRegisteredAttach,
        inbound: BoundInbound,
        kind: RegisteredAttachResponseErrorKind,
    ) -> Self {
        Self {
            awaiting,
            inbound,
            kind,
        }
    }

    pub(crate) const fn kind(&self) -> RegisteredAttachResponseErrorKind {
        self.kind
    }

    pub(crate) fn into_parts(self) -> (AwaitingRegisteredAttach, BoundInbound) {
        (self.awaiting, self.inbound)
    }
}

impl fmt::Debug for RegisteredAttachResponseFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RegisteredAttachResponseFailure")
            .field("kind", &self.kind)
            .field("ownership", &"[REDACTED]")
            .finish()
    }
}

/// C1b never exposes a bare `AttachedLeg` from a registered request. C1c's
/// dedicated client installation consumes these wrappers with the exact
/// request-queue receipt still attached.
pub(crate) enum RegisteredAttachResponse {
    Accepted(AcceptedClientRegisteredAttach),
    GenerationStatus(RegisteredAttachGenerationStatus),
}

pub(crate) struct AcceptedClientRegisteredAttach {
    attached: AttachedLeg,
    request_receipt: AttachRequestEnqueued,
}

impl AcceptedClientRegisteredAttach {
    pub(crate) fn generation(&self) -> LegGeneration {
        self.attached.generation()
    }

    pub(crate) fn request_queue_is_live(&self) -> bool {
        self.request_receipt.queue_is_live()
    }
}

pub(crate) struct RegisteredAttachGenerationStatus {
    status: LegGenerationStatus,
    request_receipt: AttachStatusRequestReceipt,
}

enum AttachStatusRequestReceipt {
    Registered(AttachRequestEnqueued),
    StatusRetry(StatusRetryAttachEnqueued),
}

impl RegisteredAttachGenerationStatus {
    pub(crate) fn current_generation(&self) -> LegGeneration {
        self.status.current_generation()
    }

    pub(crate) fn request_queue_is_live(&self) -> bool {
        match &self.request_receipt {
            AttachStatusRequestReceipt::Registered(receipt) => receipt.queue_is_live(),
            AttachStatusRequestReceipt::StatusRetry(receipt) => receipt.queue_is_live(),
        }
    }

    #[allow(
        clippy::result_large_err,
        reason = "begin rejection returns the exact status and queue receipt"
    )]
    pub(crate) fn begin_catch_up(
        self,
        follow_up_leg: &EstablishedLeg,
        nonce: AttachNonce,
        credentials: &AttachCredentials,
    ) -> Result<PendingRegisteredCatchUpAttach, RegisteredCatchUpBeginFailure> {
        let Self {
            status,
            request_receipt,
        } = self;
        match status.begin_registered_signed_catch_up(
            follow_up_leg,
            nonce,
            credentials,
            RegisteredCatchUpBeginGate::from_registered_status(),
        ) {
            Ok(signed) => {
                let (seal, pending, request, frame) = signed
                    .into_registered_parts(RegisteredCatchUpAssemblyGate::from_registered_status());
                Ok(PendingRegisteredCatchUpAttach {
                    seal,
                    pending,
                    request,
                    frame,
                    status_request_receipt: request_receipt,
                })
            }
            Err(failure) => {
                let (status, _source) = failure.into_parts();
                Err(RegisteredCatchUpBeginFailure {
                    status: Self {
                        status,
                        request_receipt,
                    },
                })
            }
        }
    }
}

pub(crate) struct RegisteredCatchUpBeginFailure {
    status: RegisteredAttachGenerationStatus,
}

impl RegisteredCatchUpBeginFailure {
    pub(crate) fn into_status(self) -> RegisteredAttachGenerationStatus {
        self.status
    }
}

impl fmt::Debug for RegisteredCatchUpBeginFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RegisteredCatchUpBeginFailure([REDACTED])")
    }
}

pub(crate) struct PendingRegisteredCatchUpAttach {
    seal: LegSeal,
    pending: PendingCatchUpAttach,
    request: AttachRequest,
    frame: Frame,
    status_request_receipt: AttachStatusRequestReceipt,
}

impl PendingRegisteredCatchUpAttach {
    #[allow(
        clippy::result_large_err,
        reason = "bounded pressure returns the exact signed catch-up request"
    )]
    pub(crate) fn enqueue(
        mut self,
        queue: &mut LegOutboundQueue,
    ) -> Result<AwaitingRegisteredCatchUpAttach, RegisteredCatchUpEnqueueFailure> {
        match queue.push_catch_up_attach_request(&self.seal, self.request, self.frame) {
            Ok(request_receipt) => Ok(AwaitingRegisteredCatchUpAttach {
                pending: self.pending,
                status_request_receipt: self.status_request_receipt,
                request_receipt,
            }),
            Err(error) => {
                let kind = error.kind().clone();
                self.frame = error.into_frame();
                Err(RegisteredCatchUpEnqueueFailure {
                    pending: self,
                    kind,
                })
            }
        }
    }
}

impl fmt::Debug for PendingRegisteredCatchUpAttach {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PendingRegisteredCatchUpAttach([REDACTED])")
    }
}

pub(crate) struct RegisteredCatchUpEnqueueFailure {
    pending: PendingRegisteredCatchUpAttach,
    kind: LegOutboundQueueErrorKind,
}

impl RegisteredCatchUpEnqueueFailure {
    pub(crate) const fn kind(&self) -> &LegOutboundQueueErrorKind {
        &self.kind
    }

    pub(crate) fn into_pending(self) -> PendingRegisteredCatchUpAttach {
        self.pending
    }
}

impl fmt::Debug for RegisteredCatchUpEnqueueFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RegisteredCatchUpEnqueueFailure")
            .field("kind", &self.kind)
            .field("pending", &"[REDACTED]")
            .finish()
    }
}

pub(crate) struct AwaitingRegisteredCatchUpAttach {
    pending: PendingCatchUpAttach,
    status_request_receipt: AttachStatusRequestReceipt,
    request_receipt: CatchUpAttachEnqueued,
}

impl AwaitingRegisteredCatchUpAttach {
    #[allow(
        clippy::result_large_err,
        reason = "wrong acceptance returns exact catch-up request and inbound fact"
    )]
    pub(crate) fn validate_response(
        self,
        inbound: BoundInbound,
    ) -> Result<AcceptedClientRegisteredCatchUp, RegisteredCatchUpResponseFailure> {
        if !self.request_receipt.queue_is_live() {
            return Err(RegisteredCatchUpResponseFailure::new(
                self,
                inbound,
                RegisteredAttachResponseErrorKind::QueueLost,
            ));
        }
        if !self.request_receipt.was_delivered() {
            return Err(RegisteredCatchUpResponseFailure::new(
                self,
                inbound,
                RegisteredAttachResponseErrorKind::RequestNotDelivered,
            ));
        }
        let BoundInbound::Session(received) = inbound else {
            return Err(RegisteredCatchUpResponseFailure::new(
                self,
                inbound,
                RegisteredAttachResponseErrorKind::Rejected,
            ));
        };
        let Self {
            pending,
            status_request_receipt,
            request_receipt,
        } = self;
        match pending.validate_registered_response_preserving(
            received,
            RegisteredCatchUpResponseGate::delivered(),
        ) {
            Ok(attached) => Ok(AcceptedClientRegisteredCatchUp {
                attached,
                status_request_receipt,
                request_receipt,
            }),
            Err(failure) => {
                let (pending, received, kind) = failure.into_parts();
                Err(RegisteredCatchUpResponseFailure::new(
                    Self {
                        pending,
                        status_request_receipt,
                        request_receipt,
                    },
                    BoundInbound::Session(received),
                    kind.into(),
                ))
            }
        }
    }

    /// Retires a delivered follow-up validator only for its exact terminal
    /// and recovers the original stale correlation retained by the catch-up
    /// authority. This does not expose or reconstruct a caller-controlled raw
    /// request.
    #[allow(
        clippy::result_large_err,
        reason = "mismatch returns exact catch-up request plus terminal fact"
    )]
    pub(crate) fn into_status_retry_after_terminal(
        self,
        terminal: ExactLegTerminal,
    ) -> Result<RegisteredAttachStatusRetryAuthority, RegisteredCatchUpTerminalRetryFailure> {
        if !self.request_receipt.was_delivered() {
            return Err(RegisteredCatchUpTerminalRetryFailure::new(
                self,
                terminal,
                RegisteredAttachStatusRetryErrorKind::RequestNotDelivered,
            ));
        }
        let Self {
            pending,
            status_request_receipt,
            request_receipt,
        } = self;
        match pending.into_original_lost_after_terminal(terminal) {
            Ok(lost) => {
                drop((status_request_receipt, request_receipt));
                Ok(RegisteredAttachStatusRetryAuthority { lost })
            }
            Err(failure) => {
                let (pending, terminal) = failure.into_parts();
                Err(RegisteredCatchUpTerminalRetryFailure::new(
                    Self {
                        pending,
                        status_request_receipt,
                        request_receipt,
                    },
                    terminal,
                    RegisteredAttachStatusRetryErrorKind::WrongTerminal,
                ))
            }
        }
    }

    /// Queue-loss counterpart to [`Self::into_status_retry_after_terminal`].
    /// A live or not-yet-delivered D remains the sole response validator and
    /// is returned unchanged.
    #[allow(
        clippy::result_large_err,
        reason = "live or undelivered rejection returns the exact catch-up request"
    )]
    pub(crate) fn into_status_retry_after_queue_loss(
        self,
    ) -> Result<RegisteredAttachStatusRetryAuthority, RegisteredCatchUpQueueLossRetryFailure> {
        if !self.request_receipt.was_delivered() || self.request_receipt.queue_is_live() {
            let kind = if self.request_receipt.was_delivered() {
                RegisteredAttachStatusRetryErrorKind::OriginalTransportStillLive
            } else {
                RegisteredAttachStatusRetryErrorKind::RequestNotDelivered
            };
            return Err(RegisteredCatchUpQueueLossRetryFailure::new(self, kind));
        }
        let Self {
            pending,
            status_request_receipt,
            request_receipt,
        } = self;
        match pending.into_original_lost_after_queue_loss() {
            Ok(lost) => {
                drop((status_request_receipt, request_receipt));
                Ok(RegisteredAttachStatusRetryAuthority { lost })
            }
            Err(pending) => Err(RegisteredCatchUpQueueLossRetryFailure::new(
                Self {
                    pending,
                    status_request_receipt,
                    request_receipt,
                },
                RegisteredAttachStatusRetryErrorKind::OriginalTransportStillLive,
            )),
        }
    }
}

impl fmt::Debug for AwaitingRegisteredCatchUpAttach {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AwaitingRegisteredCatchUpAttach([REDACTED])")
    }
}

pub(crate) struct RegisteredCatchUpTerminalRetryFailure {
    awaiting: AwaitingRegisteredCatchUpAttach,
    terminal: ExactLegTerminal,
    kind: RegisteredAttachStatusRetryErrorKind,
}

impl RegisteredCatchUpTerminalRetryFailure {
    fn new(
        awaiting: AwaitingRegisteredCatchUpAttach,
        terminal: ExactLegTerminal,
        kind: RegisteredAttachStatusRetryErrorKind,
    ) -> Self {
        Self {
            awaiting,
            terminal,
            kind,
        }
    }

    pub(crate) const fn kind(&self) -> RegisteredAttachStatusRetryErrorKind {
        self.kind
    }

    pub(crate) fn into_parts(self) -> (AwaitingRegisteredCatchUpAttach, ExactLegTerminal) {
        (self.awaiting, self.terminal)
    }
}

impl fmt::Debug for RegisteredCatchUpTerminalRetryFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RegisteredCatchUpTerminalRetryFailure")
            .field("kind", &self.kind)
            .field("ownership", &"[REDACTED]")
            .finish()
    }
}

pub(crate) struct RegisteredCatchUpQueueLossRetryFailure {
    awaiting: AwaitingRegisteredCatchUpAttach,
    kind: RegisteredAttachStatusRetryErrorKind,
}

impl RegisteredCatchUpQueueLossRetryFailure {
    fn new(
        awaiting: AwaitingRegisteredCatchUpAttach,
        kind: RegisteredAttachStatusRetryErrorKind,
    ) -> Self {
        Self { awaiting, kind }
    }

    pub(crate) const fn kind(&self) -> RegisteredAttachStatusRetryErrorKind {
        self.kind
    }

    pub(crate) fn into_awaiting(self) -> AwaitingRegisteredCatchUpAttach {
        self.awaiting
    }
}

impl fmt::Debug for RegisteredCatchUpQueueLossRetryFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RegisteredCatchUpQueueLossRetryFailure")
            .field("kind", &self.kind)
            .field("awaiting", &"[REDACTED]")
            .finish()
    }
}

pub(crate) struct RegisteredCatchUpResponseFailure {
    awaiting: AwaitingRegisteredCatchUpAttach,
    inbound: BoundInbound,
    kind: RegisteredAttachResponseErrorKind,
}

impl RegisteredCatchUpResponseFailure {
    fn new(
        awaiting: AwaitingRegisteredCatchUpAttach,
        inbound: BoundInbound,
        kind: RegisteredAttachResponseErrorKind,
    ) -> Self {
        Self {
            awaiting,
            inbound,
            kind,
        }
    }

    pub(crate) const fn kind(&self) -> RegisteredAttachResponseErrorKind {
        self.kind
    }

    pub(crate) fn into_parts(self) -> (AwaitingRegisteredCatchUpAttach, BoundInbound) {
        (self.awaiting, self.inbound)
    }
}

impl fmt::Debug for RegisteredCatchUpResponseFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RegisteredCatchUpResponseFailure")
            .field("kind", &self.kind)
            .field("ownership", &"[REDACTED]")
            .finish()
    }
}

pub(crate) struct AcceptedClientRegisteredCatchUp {
    attached: CaughtUpAttachedLeg,
    status_request_receipt: AttachStatusRequestReceipt,
    request_receipt: CatchUpAttachEnqueued,
}

impl AcceptedClientRegisteredCatchUp {
    pub(crate) fn generation(&self) -> LegGeneration {
        self.attached.generation()
    }

    pub(crate) fn follow_up_queue_is_live(&self) -> bool {
        self.request_receipt.queue_is_live()
    }
}

impl fmt::Debug for AcceptedClientRegisteredCatchUp {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let _status_receipt = &self.status_request_receipt;
        formatter.write_str("AcceptedClientRegisteredCatchUp([REDACTED])")
    }
}

impl fmt::Debug for RegisteredAttachResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Accepted(_) => {
                formatter.write_str("RegisteredAttachResponse::Accepted([REDACTED])")
            }
            Self::GenerationStatus(_) => {
                formatter.write_str("RegisteredAttachResponse::GenerationStatus([REDACTED])")
            }
        }
    }
}

impl fmt::Debug for AcceptedClientRegisteredAttach {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AcceptedClientRegisteredAttach([REDACTED])")
    }
}

impl fmt::Debug for RegisteredAttachGenerationStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RegisteredAttachGenerationStatus([REDACTED])")
    }
}

impl fmt::Debug for ClientRegisteredStandby {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ClientRegisteredStandby([REDACTED])")
    }
}

pub(crate) struct RetiredClientStandby;

impl fmt::Debug for RetiredClientStandby {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RetiredClientStandby([REDACTED])")
    }
}

/// Exact owner-side authentication result. It combines the consuming HMAC
/// capability with the process-local seal that carried the bytes.
pub(crate) struct ExactAuthenticatedStandby {
    seal: LegSeal,
    endpoint: Weak<LegTransportEndpoint>,
    authentication: AuthenticatedStandbyRegistration,
}

impl ExactAuthenticatedStandby {
    pub(crate) fn authenticate(
        active: &AttachedLeg,
        candidate: &EstablishedLeg,
        inbound: BoundInbound,
        authority: &AttachAuthority,
    ) -> Result<Self, StandbyRegistrationReject> {
        let BoundInbound::LegControl(inbound) = inbound else {
            return Err(StandbyRegistrationReject::Rejected);
        };
        let (received_seal, frame) = inbound.into_parts();
        let candidate_seal = candidate.standby_seal();
        let binding = candidate.standby_transport_binding();
        let endpoint = candidate.endpoint_liveness();
        if !candidate_seal.same_connection(&received_seal)
            || active.belongs_to_transport(&candidate_seal)
            || active.transport_binding() == binding
            || !endpoint_is_open(&endpoint)
            || candidate_seal.outbound_queue_was_lost()
        {
            return Err(StandbyRegistrationReject::Rejected);
        }
        let authentication =
            active.authenticate_standby_registration(authority, &binding, &frame)?;
        Ok(Self {
            seal: candidate_seal,
            endpoint,
            authentication,
        })
    }

    fn contract(&self) -> StandbyContract {
        StandbyContract::from_authenticated(&self.authentication)
    }

    fn same_registration(&self, other: &Self) -> bool {
        self.seal.same_connection(&other.seal) && self.authentication == other.authentication
    }
}

impl fmt::Debug for ExactAuthenticatedStandby {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ExactAuthenticatedStandby([REDACTED])")
    }
}

struct OwnerRegisteredStandby {
    candidate: ExactAuthenticatedStandby,
    receipt: LegControlEnqueued,
}

impl OwnerRegisteredStandby {
    fn is_live(&self) -> bool {
        endpoint_is_open(&self.candidate.endpoint) && self.receipt.queue_is_live()
    }
}

impl fmt::Debug for OwnerRegisteredStandby {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OwnerRegisteredStandby([REDACTED])")
    }
}

/// Exactly-one bounded owner slot. A successful acceptance enqueue precedes
/// infallible installation; conflicts never mutate either the slot or queue.
pub(crate) struct OwnerStandbySlot {
    registered: Option<OwnerRegisteredStandby>,
}

impl OwnerStandbySlot {
    pub(crate) const fn new() -> Self {
        Self { registered: None }
    }

    pub(crate) fn registered_is_live(&self) -> bool {
        self.registered
            .as_ref()
            .is_some_and(OwnerRegisteredStandby::is_live)
    }

    #[allow(
        clippy::result_large_err,
        reason = "bounded pressure must return the exact authenticated standby capability"
    )]
    pub(crate) fn admit(
        &mut self,
        candidate: ExactAuthenticatedStandby,
        queue: &mut LegOutboundQueue,
    ) -> Result<OwnerStandbyAdmission, OwnerStandbyAdmissionFailure> {
        let duplicate = match self.registered.as_ref() {
            None => false,
            Some(registered) if registered.candidate.same_registration(&candidate) => true,
            Some(_) => return Err(OwnerStandbyAdmissionFailure::rejected()),
        };
        if !endpoint_is_open(&candidate.endpoint) {
            return Err(OwnerStandbyAdmissionFailure::retry(
                candidate,
                LegControlQueueErrorKind::EndpointLost,
            ));
        }
        let contract = candidate.contract();
        let acceptance = contract
            .acceptance_frame()
            .map_err(|_| OwnerStandbyAdmissionFailure::rejected())?;
        let receipt = match queue.push_leg_control(&candidate.seal, acceptance, contract.features) {
            Ok(receipt) => receipt,
            Err(error) => {
                let kind = error.kind().clone();
                return Err(OwnerStandbyAdmissionFailure::retry(candidate, kind));
            }
        };
        if duplicate {
            drop(candidate);
            Ok(OwnerStandbyAdmission::Reacknowledged)
        } else {
            self.registered = Some(OwnerRegisteredStandby { candidate, receipt });
            Ok(OwnerStandbyAdmission::Installed)
        }
    }

    pub(crate) fn retire_lost(&mut self) -> Option<RetiredOwnerStandby> {
        if self.registered_is_live() {
            return None;
        }
        self.registered.take().map(|_| RetiredOwnerStandby)
    }

    /// Removes the exact registered B authority only after a read-only gate
    /// proves its delivered registration acceptance, live queue/endpoint,
    /// transport seal/binding, and immutable exact-next request contract.
    /// The returned non-cloneable gate restores the slot on every non-install
    /// outcome and is consumed only after supervisor installation succeeds.
    pub(super) fn begin_exact_next_attach(
        &mut self,
        leg: &EstablishedLeg,
        received: &LegBoundFrame,
        queue: &mut LegOutboundQueue,
    ) -> Result<OwnerRegisteredAttachGate, OwnerRegisteredAttachGateError> {
        let registered = self
            .registered
            .as_ref()
            .ok_or(OwnerRegisteredAttachGateError::Rejected)?;
        if !registered.is_live()
            || !registered.receipt.was_delivered()
            || !registered.receipt.belongs_to_queue(queue)
            || !leg.belongs_to_transport(&registered.candidate.seal)
            || !received.belongs_to_transport(leg)
            || leg.standby_transport_binding() != registered.candidate.contract().binding
        {
            return Err(OwnerRegisteredAttachGateError::Rejected);
        }
        let request = received
            .attach_request()
            .map_err(|_| OwnerRegisteredAttachGateError::Rejected)?;
        let contract = registered.candidate.contract();
        if !contract.matches_exact_next_attach(request) {
            return Err(OwnerRegisteredAttachGateError::Rejected);
        }
        let reservation = queue
            .reserve_attach_acceptance(leg)
            .map_err(OwnerRegisteredAttachGateError::QueueReservation)?;
        let Some(registered) = self.registered.take() else {
            let _retained = reservation.release(queue).err();
            return Err(OwnerRegisteredAttachGateError::Rejected);
        };
        Ok(OwnerRegisteredAttachGate {
            registered,
            contract,
            request,
            reservation,
        })
    }
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub(crate) enum OwnerRegisteredAttachGateError {
    #[error("owner registered standby does not authorize this exact ATTACH")]
    Rejected,
    #[error("owner registered standby queue reservation failed: {0}")]
    QueueReservation(AttachAcceptanceReserveError),
}

/// Temporary exclusive ownership of the sole registered B slot during one
/// synchronous owner transaction. It is intentionally non-cloneable.
pub(super) struct OwnerRegisteredAttachGate {
    registered: OwnerRegisteredStandby,
    contract: StandbyContract,
    request: AttachRequest,
    reservation: AttachAcceptanceReservation,
}

impl OwnerRegisteredAttachGate {
    pub(super) fn current_generation(&self) -> LegGeneration {
        self.contract.generation
    }

    pub(super) fn matches_transaction(
        &self,
        leg: &EstablishedLeg,
        received: &LegBoundFrame,
    ) -> bool {
        self.registered.is_live()
            && self.registered.receipt.was_delivered()
            && self.reservation.endpoint_is_open()
            && self.reservation.belongs_to_leg(leg)
            && leg.belongs_to_transport(&self.registered.candidate.seal)
            && leg.standby_transport_binding() == self.contract.binding
            && received.belongs_to_transport(leg)
            && received
                .attach_request()
                .is_ok_and(|request| request == self.request)
    }

    #[allow(
        clippy::result_large_err,
        reason = "restore failure must retain the exact registered slot and queue reservation gate"
    )]
    pub(super) fn restore(
        self,
        slot: &mut OwnerStandbySlot,
        queue: &mut LegOutboundQueue,
    ) -> Result<(), OwnerRegisteredAttachGate> {
        if slot.registered.is_some() {
            return Err(self);
        }
        let Self {
            registered,
            contract,
            request,
            reservation,
        } = self;
        match reservation.release(queue) {
            Ok(()) => {
                slot.registered = Some(registered);
                Ok(())
            }
            Err(reservation) => Err(Self {
                registered,
                contract,
                request,
                reservation,
            }),
        }
    }

    pub(super) fn consume_after_install(
        self,
        attached: &AttachedLeg,
    ) -> AttachAcceptanceReservation {
        debug_assert!(attached.belongs_to_transport(&self.registered.candidate.seal));
        debug_assert_eq!(attached.generation(), self.request.requested_generation());
        debug_assert_eq!(attached.nonce(), self.request.nonce());
        debug_assert_eq!(attached.transport_binding(), self.contract.binding);
        self.reservation
    }
}

impl fmt::Debug for OwnerRegisteredAttachGate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OwnerRegisteredAttachGate([REDACTED])")
    }
}

impl fmt::Debug for OwnerStandbySlot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OwnerStandbySlot")
            .field("occupied", &self.registered.is_some())
            .field("live", &self.registered_is_live())
            .finish()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OwnerStandbyAdmission {
    Installed,
    Reacknowledged,
}

pub(crate) struct OwnerStandbyAdmissionFailure {
    kind: OwnerStandbyAdmissionErrorKind,
    retry: Option<ExactAuthenticatedStandby>,
}

impl OwnerStandbyAdmissionFailure {
    fn rejected() -> Self {
        Self {
            kind: OwnerStandbyAdmissionErrorKind::Rejected,
            retry: None,
        }
    }

    fn retry(candidate: ExactAuthenticatedStandby, kind: LegControlQueueErrorKind) -> Self {
        Self {
            kind: OwnerStandbyAdmissionErrorKind::Queue(kind),
            retry: Some(candidate),
        }
    }

    pub(crate) const fn kind(&self) -> &OwnerStandbyAdmissionErrorKind {
        &self.kind
    }

    pub(crate) fn into_retry(self) -> Option<ExactAuthenticatedStandby> {
        self.retry
    }
}

impl fmt::Debug for OwnerStandbyAdmissionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OwnerStandbyAdmissionFailure")
            .field("kind", &self.kind)
            .field("retry", &self.retry.as_ref().map(|_| "[REDACTED]"))
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum OwnerStandbyAdmissionErrorKind {
    Rejected,
    Queue(LegControlQueueErrorKind),
}

pub(crate) struct RetiredOwnerStandby;

impl fmt::Debug for RetiredOwnerStandby {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RetiredOwnerStandby([REDACTED])")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::owned_upstream::leg::{
        AttachResponse, EstablishedLeg, LegTransportTerminalReason, OwnerAttachTransaction,
    };
    use crate::owned_upstream::leg_io::{
        LegEndpointRole, LegIo, LegIoLimits, LegOutboundQueue, MemoryFaultScript,
        MemoryLegTransport,
    };
    use crate::owned_upstream::owner_target::{
        OwnerTargetAttachPublication, OwnerTargetConfig, OwnerTargetError, OwnerTargetExecutor,
    };
    use crate::owned_upstream::supervisor::SessionSupervisor;
    use crate::owned_upstream::target::{MemoryTarget, MemoryTargetConfig};
    use crate::owned_upstream::tcp::FlowPortConfig;
    use crate::owned_upstream::two_leg::{
        LegId, SimTime, TwoLegWire, WireBounds, WireCapacity, WireCapacitySpec,
    };
    use crate::resumable::{
        AttachAlpn, AttachAuthority, AttachCredentials, AttachNonce, AttachPolicy, AttachRequest,
        AttachTransportBinding, DevicePrincipal, DeviceSecret, FeatureOffer, LegGeneration,
        OwnerIdentity, ReceiveBudgetLimits, ReplayBudgetLimits, ResumeSecret,
        SESSION_PROTOCOL_VERSION, STANDBY_CONTROL_V1, SessionConfig, SessionId, StandbyNonce,
        TcpWindowLimits, TlsExporterBinding, VersionRange,
    };
    use std::time::Duration;

    const FEATURES: u64 = STANDBY_CONTROL_V1.bits() | 0b0111;

    fn binding(exporter: u8) -> AttachTransportBinding {
        AttachTransportBinding::new(
            OwnerIdentity::new([0x31; 32]).unwrap(),
            AttachAlpn::new(b"mini-vpn-owned/1").unwrap(),
            TlsExporterBinding::new([exporter; 32]).unwrap(),
            DevicePrincipal::new([0x53; 16]).unwrap(),
        )
    }

    fn credentials() -> AttachCredentials {
        AttachCredentials::new(
            DeviceSecret::new([0x71; 32]).unwrap(),
            ResumeSecret::new([0x72; 32]).unwrap(),
        )
        .unwrap()
    }

    fn config() -> SessionConfig {
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

    fn authority() -> AttachAuthority {
        AttachAuthority::new(
            SessionId::new([0x11; 16]).unwrap(),
            LegGeneration::new(1).unwrap(),
            credentials(),
            AttachPolicy::new(
                binding(0x42).owner_identity(),
                binding(0x42).alpn(),
                binding(0x42).device_principal(),
                SESSION_PROTOCOL_VERSION,
                FEATURES,
            )
            .unwrap(),
        )
    }

    fn active_pair() -> (
        AttachAuthority,
        crate::resumable::SessionModel,
        crate::owned_upstream::leg::AttachedLeg,
        crate::owned_upstream::leg::AttachedLeg,
    ) {
        let authority = authority();
        let transport = binding(0x42);
        let request = AttachRequest::new(
            SessionId::new([0x11; 16]).unwrap(),
            LegGeneration::new(2).unwrap(),
            AttachNonce::new([0x22; 16]).unwrap(),
            VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION).unwrap(),
            FeatureOffer::new(FEATURES, STANDBY_CONTROL_V1.bits()).unwrap(),
        );
        let client_a = EstablishedLeg::for_authenticated_transport(transport);
        let owner_a = EstablishedLeg::for_authenticated_transport(transport);
        let pending = client_a.begin_attach(request);
        let proof = credentials().prove(&request, &transport).unwrap();
        let authenticated = owner_a
            .authenticate_initial_owner_attach(
                owner_a.bind_received_frame(request.to_attach_frame(proof)),
                &authority,
            )
            .unwrap();
        let (model, owner_active, acceptance) = authenticated.into_owner_parts(config());
        let AttachResponse::Accepted(client_active) = pending
            .validate_response(client_a.bind_received_frame(acceptance))
            .unwrap()
        else {
            panic!("initial attach changed response kind");
        };
        (authority, model, client_active, owner_active)
    }

    fn capacity() -> WireCapacity {
        WireCapacitySpec::new(80_000, Duration::from_secs(1), 256, 128, 1)
            .unwrap()
            .with_fault_copies(1, 1)
            .unwrap()
            .derive()
            .unwrap()
    }

    fn limits() -> LegIoLimits {
        LegIoLimits::from_wire_capacity(capacity(), 1_024).unwrap()
    }

    fn transport() -> MemoryLegTransport {
        MemoryLegTransport::new(
            TwoLegWire::new(WireBounds::new(capacity(), 64, 32_000, 4, 4).unwrap()),
            MemoryFaultScript::new(0),
        )
    }

    fn release(transport: &mut MemoryLegTransport) {
        while transport.advance_one_due(SimTime::ZERO).unwrap() {}
        transport.advance_idle_to(SimTime::ZERO).unwrap();
    }

    #[test]
    fn terminal_candidate_cannot_begin_registration_while_leg_io_is_still_owned() {
        let (_authority, _model, client_active, _owner_active) = active_pair();
        let (candidate, reporter) = LegIo::for_authenticated_transport_with_reporter(
            LegId::B,
            LegEndpointRole::Client,
            binding(0x99),
            limits(),
        );
        let _terminal = reporter.report(LegTransportTerminalReason::PeerClosed);

        assert_eq!(
            PendingStandbyRegistration::begin(
                &client_active,
                candidate.established_leg(),
                StandbyNonce::new([0x33; 16]).unwrap(),
                &credentials(),
            )
            .unwrap_err(),
            StandbyRegistrationError::EndpointLost
        );
        assert_eq!(candidate.counters(), Default::default());
    }

    #[test]
    fn owner_authentication_rejects_terminal_candidate_with_live_leg_io() {
        let (authority, _model, client_active, owner_active) = active_pair();
        let client_b = LegIo::for_authenticated_transport(
            LegId::B,
            LegEndpointRole::Client,
            binding(0x99),
            limits(),
        );
        let (owner_b, reporter) = LegIo::for_authenticated_transport_with_reporter(
            LegId::B,
            LegEndpointRole::Owner,
            binding(0x99),
            limits(),
        );
        let pending = PendingStandbyRegistration::begin(
            &client_active,
            client_b.established_leg(),
            StandbyNonce::new([0x34; 16]).unwrap(),
            &credentials(),
        )
        .unwrap();
        let inbound = BoundInbound::LegControl(
            owner_b
                .established_leg()
                .bind_received_control_frame(pending.frame),
        );
        let _terminal = reporter.report(LegTransportTerminalReason::Reset);

        assert_eq!(
            ExactAuthenticatedStandby::authenticate(
                &owner_active,
                owner_b.established_leg(),
                inbound,
                &authority,
            )
            .unwrap_err(),
            StandbyRegistrationReject::Rejected
        );
    }

    #[test]
    fn terminal_report_immediately_retires_both_registered_standby_receipts() {
        let (authority, _model, client_active, owner_active) = active_pair();
        let (mut client_b, client_reporter) = LegIo::for_authenticated_transport_with_reporter(
            LegId::B,
            LegEndpointRole::Client,
            binding(0x99),
            limits(),
        );
        let (mut owner_b, owner_reporter) = LegIo::for_authenticated_transport_with_reporter(
            LegId::B,
            LegEndpointRole::Owner,
            binding(0x99),
            limits(),
        );
        let mut client_queue =
            LegOutboundQueue::for_leg(client_b.established_leg(), 2, 2_048).unwrap();
        let mut owner_queue =
            LegOutboundQueue::for_leg(owner_b.established_leg(), 2, 2_048).unwrap();
        let mut transport = transport();
        let (awaiting, candidate) = deliver_registration(
            &client_active,
            &owner_active,
            &mut client_b,
            &mut owner_b,
            &mut client_queue,
            &mut transport,
            &authority,
            0x35,
        );
        let mut slot = OwnerStandbySlot::new();
        assert_eq!(
            slot.admit(candidate, &mut owner_queue).unwrap(),
            OwnerStandbyAdmission::Installed
        );
        let accepted = deliver_acceptance(
            &mut owner_b,
            &mut client_b,
            &mut owner_queue,
            &mut transport,
        );
        let registered = awaiting.validate(accepted).unwrap();
        assert!(registered.is_live());
        assert!(slot.registered_is_live());

        let _client_terminal = client_reporter.report(LegTransportTerminalReason::PeerClosed);
        let _owner_terminal = owner_reporter.report(LegTransportTerminalReason::FatalIo);

        assert!(!registered.is_live());
        assert!(registered.retire_lost().is_ok());
        assert!(!slot.registered_is_live());
        assert!(slot.retire_lost().is_some());
        assert!(client_queue.is_empty());
        assert!(owner_queue.is_empty());
    }

    #[allow(clippy::too_many_arguments)]
    fn deliver_registration(
        active_client: &AttachedLeg,
        active_owner: &AttachedLeg,
        client_b: &mut LegIo,
        owner_b: &mut LegIo,
        client_queue: &mut LegOutboundQueue,
        transport: &mut MemoryLegTransport,
        authority: &AttachAuthority,
        nonce: u8,
    ) -> (AwaitingStandbyAccepted, ExactAuthenticatedStandby) {
        let pending = PendingStandbyRegistration::begin(
            active_client,
            client_b.established_leg(),
            StandbyNonce::new([nonce; 16]).unwrap(),
            &credentials(),
        )
        .unwrap();
        let awaiting = pending.enqueue(client_queue).unwrap();
        client_queue
            .try_flush(client_b, &mut transport.controller_sender(), SimTime::ZERO)
            .unwrap()
            .unwrap();
        release(transport);
        let inbound = owner_b
            .receive_classified_delivery(
                transport
                    .try_recv_next(owner_b.inbound_route())
                    .unwrap()
                    .unwrap(),
                FeatureSet::new(FEATURES),
            )
            .unwrap();
        let candidate = ExactAuthenticatedStandby::authenticate(
            active_owner,
            owner_b.established_leg(),
            inbound,
            authority,
        )
        .unwrap();
        (awaiting, candidate)
    }

    fn deliver_acceptance(
        owner_b: &mut LegIo,
        client_b: &mut LegIo,
        owner_queue: &mut LegOutboundQueue,
        transport: &mut MemoryLegTransport,
    ) -> BoundInbound {
        owner_queue
            .try_flush(owner_b, &mut transport.controller_sender(), SimTime::ZERO)
            .unwrap()
            .unwrap();
        release(transport);
        client_b
            .receive_classified_delivery(
                transport
                    .try_recv_next(client_b.inbound_route())
                    .unwrap()
                    .unwrap(),
                FeatureSet::new(FEATURES),
            )
            .unwrap()
    }

    #[test]
    fn exact_b_registration_crosses_bytes_without_changing_generation_or_model() {
        let (authority, model, client_active, owner_active) = active_pair();
        let generation_before = authority.current_generation();
        let model_before = model.snapshot();
        let mut client_b = LegIo::for_authenticated_transport(
            LegId::B,
            LegEndpointRole::Client,
            binding(0x99),
            limits(),
        );
        let mut owner_b = LegIo::for_authenticated_transport(
            LegId::B,
            LegEndpointRole::Owner,
            binding(0x99),
            limits(),
        );
        let mut client_queue =
            LegOutboundQueue::for_leg(client_b.established_leg(), 4, 4_096).unwrap();
        let mut owner_queue =
            LegOutboundQueue::for_leg(owner_b.established_leg(), 4, 4_096).unwrap();
        let mut transport = transport();
        let pending = PendingStandbyRegistration::begin(
            &client_active,
            client_b.established_leg(),
            StandbyNonce::new([0x33; 16]).unwrap(),
            &credentials(),
        )
        .unwrap();
        let awaiting = pending.enqueue(&mut client_queue).unwrap();

        client_queue
            .try_flush(
                &mut client_b,
                &mut transport.controller_sender(),
                SimTime::ZERO,
            )
            .unwrap()
            .unwrap();
        release(&mut transport);
        let inbound = owner_b
            .receive_classified_delivery(
                transport
                    .try_recv_next(owner_b.inbound_route())
                    .unwrap()
                    .unwrap(),
                crate::resumable::FeatureSet::new(FEATURES),
            )
            .unwrap();
        let candidate = ExactAuthenticatedStandby::authenticate(
            &owner_active,
            owner_b.established_leg(),
            inbound,
            &authority,
        )
        .unwrap();
        let mut slot = OwnerStandbySlot::new();
        assert_eq!(
            slot.admit(candidate, &mut owner_queue).unwrap(),
            OwnerStandbyAdmission::Installed
        );

        owner_queue
            .try_flush(
                &mut owner_b,
                &mut transport.controller_sender(),
                SimTime::ZERO,
            )
            .unwrap()
            .unwrap();
        release(&mut transport);
        let accepted = client_b
            .receive_classified_delivery(
                transport
                    .try_recv_next(client_b.inbound_route())
                    .unwrap()
                    .unwrap(),
                crate::resumable::FeatureSet::new(FEATURES),
            )
            .unwrap();
        let registered = awaiting.validate(accepted).unwrap();

        assert!(registered.is_live());
        assert!(slot.registered_is_live());
        assert_eq!(authority.current_generation(), generation_before);
        assert_eq!(model.snapshot(), model_before);
    }

    #[test]
    fn lost_acceptance_retries_same_exact_registration_without_replacing_the_slot() {
        let (authority, model, client_active, owner_active) = active_pair();
        let generation_before = authority.current_generation();
        let model_before = model.snapshot();
        let mut client_b = LegIo::for_authenticated_transport(
            LegId::B,
            LegEndpointRole::Client,
            binding(0x99),
            limits(),
        );
        let mut owner_b = LegIo::for_authenticated_transport(
            LegId::B,
            LegEndpointRole::Owner,
            binding(0x99),
            limits(),
        );
        let mut client_queue =
            LegOutboundQueue::for_leg(client_b.established_leg(), 4, 4_096).unwrap();
        let mut owner_queue =
            LegOutboundQueue::for_leg(owner_b.established_leg(), 4, 4_096).unwrap();
        let mut transport = transport();
        let (mut awaiting, first) = deliver_registration(
            &client_active,
            &owner_active,
            &mut client_b,
            &mut owner_b,
            &mut client_queue,
            &mut transport,
            &authority,
            0x33,
        );
        let mut slot = OwnerStandbySlot::new();
        assert_eq!(
            slot.admit(first, &mut owner_queue).unwrap(),
            OwnerStandbyAdmission::Installed
        );
        assert_eq!(usize::from(slot.registered.is_some()), 1);

        // The first acceptance crosses the sole queue and transport, but the
        // client loses it before validation.
        let _lost = deliver_acceptance(
            &mut owner_b,
            &mut client_b,
            &mut owner_queue,
            &mut transport,
        );
        awaiting.retry(&mut client_queue).unwrap();
        client_queue
            .try_flush(
                &mut client_b,
                &mut transport.controller_sender(),
                SimTime::ZERO,
            )
            .unwrap()
            .unwrap();
        release(&mut transport);
        let repeated = owner_b
            .receive_classified_delivery(
                transport
                    .try_recv_next(owner_b.inbound_route())
                    .unwrap()
                    .unwrap(),
                FeatureSet::new(FEATURES),
            )
            .unwrap();
        let repeated = ExactAuthenticatedStandby::authenticate(
            &owner_active,
            owner_b.established_leg(),
            repeated,
            &authority,
        )
        .unwrap();

        assert_eq!(
            slot.admit(repeated, &mut owner_queue).unwrap(),
            OwnerStandbyAdmission::Reacknowledged
        );
        assert_eq!(usize::from(slot.registered.is_some()), 1);
        let accepted = deliver_acceptance(
            &mut owner_b,
            &mut client_b,
            &mut owner_queue,
            &mut transport,
        );
        assert!(awaiting.validate(accepted).unwrap().is_live());
        assert!(slot.registered_is_live());
        assert_eq!(authority.current_generation(), generation_before);
        assert_eq!(model.snapshot(), model_before);
    }

    #[test]
    fn nonce_or_exact_transport_conflicts_are_inert_to_slot_queue_and_session_state() {
        let (authority, model, client_active, owner_active) = active_pair();
        let generation_before = authority.current_generation();
        let model_before = model.snapshot();
        let mut client_b = LegIo::for_authenticated_transport(
            LegId::B,
            LegEndpointRole::Client,
            binding(0x99),
            limits(),
        );
        let mut owner_b = LegIo::for_authenticated_transport(
            LegId::B,
            LegEndpointRole::Owner,
            binding(0x99),
            limits(),
        );
        let mut client_queue =
            LegOutboundQueue::for_leg(client_b.established_leg(), 4, 4_096).unwrap();
        let mut owner_queue =
            LegOutboundQueue::for_leg(owner_b.established_leg(), 4, 4_096).unwrap();
        let mut transport_b = transport();
        let (_awaiting, first) = deliver_registration(
            &client_active,
            &owner_active,
            &mut client_b,
            &mut owner_b,
            &mut client_queue,
            &mut transport_b,
            &authority,
            0x33,
        );
        let mut slot = OwnerStandbySlot::new();
        assert_eq!(
            slot.admit(first, &mut owner_queue).unwrap(),
            OwnerStandbyAdmission::Installed
        );

        let (_awaiting_conflict, nonce_conflict) = deliver_registration(
            &client_active,
            &owner_active,
            &mut client_b,
            &mut owner_b,
            &mut client_queue,
            &mut transport_b,
            &authority,
            0x34,
        );
        let queue_before = (owner_queue.len(), owner_queue.owned_bytes());
        let rejected = slot.admit(nonce_conflict, &mut owner_queue).unwrap_err();
        assert_eq!(rejected.kind(), &OwnerStandbyAdmissionErrorKind::Rejected);
        assert!(rejected.into_retry().is_none());
        assert_eq!((owner_queue.len(), owner_queue.owned_bytes()), queue_before);
        assert!(slot.registered_is_live());

        let mut client_c = LegIo::for_authenticated_transport(
            LegId::A,
            LegEndpointRole::Client,
            binding(0x98),
            limits(),
        );
        let mut owner_c = LegIo::for_authenticated_transport(
            LegId::A,
            LegEndpointRole::Owner,
            binding(0x98),
            limits(),
        );
        let mut client_c_queue =
            LegOutboundQueue::for_leg(client_c.established_leg(), 2, 2_048).unwrap();
        let mut transport_c = transport();
        let (_awaiting_conflict, transport_conflict) = deliver_registration(
            &client_active,
            &owner_active,
            &mut client_c,
            &mut owner_c,
            &mut client_c_queue,
            &mut transport_c,
            &authority,
            0x33,
        );
        let rejected = slot
            .admit(transport_conflict, &mut owner_queue)
            .unwrap_err();
        assert_eq!(rejected.kind(), &OwnerStandbyAdmissionErrorKind::Rejected);
        assert!(rejected.into_retry().is_none());
        assert_eq!((owner_queue.len(), owner_queue.owned_bytes()), queue_before);
        assert!(slot.registered_is_live());
        assert_eq!(authority.current_generation(), generation_before);
        assert_eq!(model.snapshot(), model_before);
    }

    #[test]
    fn classifier_and_owner_fail_closed_without_feature_exact_seal_or_valid_proof() {
        let (authority, _model, client_active, owner_active) = active_pair();
        let mut client_b = LegIo::for_authenticated_transport(
            LegId::B,
            LegEndpointRole::Client,
            binding(0x99),
            limits(),
        );
        let mut owner_b = LegIo::for_authenticated_transport(
            LegId::B,
            LegEndpointRole::Owner,
            binding(0x99),
            limits(),
        );
        let wrong_owner_seal = EstablishedLeg::for_authenticated_transport(binding(0x99));
        let mut client_queue =
            LegOutboundQueue::for_leg(client_b.established_leg(), 4, 4_096).unwrap();
        let mut transport_b = transport();
        let pending = PendingStandbyRegistration::begin(
            &client_active,
            client_b.established_leg(),
            StandbyNonce::new([0x33; 16]).unwrap(),
            &credentials(),
        )
        .unwrap();
        let mut awaiting = pending.enqueue(&mut client_queue).unwrap();
        client_queue
            .try_flush(
                &mut client_b,
                &mut transport_b.controller_sender(),
                SimTime::ZERO,
            )
            .unwrap()
            .unwrap();
        release(&mut transport_b);
        let feature_rejection = owner_b
            .receive_classified_delivery(
                transport_b
                    .try_recv_next(owner_b.inbound_route())
                    .unwrap()
                    .unwrap(),
                FeatureSet::default(),
            )
            .unwrap_err();
        assert!(matches!(
            feature_rejection,
            crate::owned_upstream::leg_io::LegIoError::Decode(
                crate::resumable::ProtocolError::StandbyControlNotNegotiated { negotiated: 0 }
            )
        ));
        assert_eq!(owner_b.counters().bound_leg_control_messages, 0);
        assert_eq!(owner_b.counters().bound_session_messages, 0);

        awaiting.retry(&mut client_queue).unwrap();
        client_queue
            .try_flush(
                &mut client_b,
                &mut transport_b.controller_sender(),
                SimTime::ZERO,
            )
            .unwrap()
            .unwrap();
        release(&mut transport_b);
        let bound_to_b = owner_b
            .receive_classified_delivery(
                transport_b
                    .try_recv_next(owner_b.inbound_route())
                    .unwrap()
                    .unwrap(),
                FeatureSet::new(FEATURES),
            )
            .unwrap();
        assert_eq!(
            ExactAuthenticatedStandby::authenticate(
                &owner_active,
                &wrong_owner_seal,
                bound_to_b,
                &authority,
            )
            .unwrap_err(),
            StandbyRegistrationReject::Rejected
        );
        assert_eq!(owner_b.counters().bound_leg_control_messages, 1);
        assert_eq!(owner_b.counters().bound_session_messages, 0);

        let mut client_c = LegIo::for_authenticated_transport(
            LegId::A,
            LegEndpointRole::Client,
            binding(0x98),
            limits(),
        );
        let mut owner_c = LegIo::for_authenticated_transport(
            LegId::A,
            LegEndpointRole::Owner,
            binding(0x98),
            limits(),
        );
        let mut client_c_queue =
            LegOutboundQueue::for_leg(client_c.established_leg(), 2, 2_048).unwrap();
        let wrong_credentials = AttachCredentials::new(
            DeviceSecret::new([0x81; 32]).unwrap(),
            ResumeSecret::new([0x82; 32]).unwrap(),
        )
        .unwrap();
        let pending = PendingStandbyRegistration::begin(
            &client_active,
            client_c.established_leg(),
            StandbyNonce::new([0x44; 16]).unwrap(),
            &wrong_credentials,
        )
        .unwrap();
        let _awaiting_wrong_proof = pending.enqueue(&mut client_c_queue).unwrap();
        let mut transport_c = transport();
        client_c_queue
            .try_flush(
                &mut client_c,
                &mut transport_c.controller_sender(),
                SimTime::ZERO,
            )
            .unwrap()
            .unwrap();
        release(&mut transport_c);
        let wrong_proof = owner_c
            .receive_classified_delivery(
                transport_c
                    .try_recv_next(owner_c.inbound_route())
                    .unwrap()
                    .unwrap(),
                FeatureSet::new(FEATURES),
            )
            .unwrap();
        assert_eq!(
            ExactAuthenticatedStandby::authenticate(
                &owner_active,
                owner_c.established_leg(),
                wrong_proof,
                &authority,
            )
            .unwrap_err(),
            StandbyRegistrationReject::Rejected
        );
        assert_eq!(authority.current_generation(), 2);
    }

    #[test]
    fn queue_pressure_returns_exact_client_and_owner_capabilities_for_retry() {
        let (authority, model, client_active, owner_active) = active_pair();
        let generation_before = authority.current_generation();
        let model_before = model.snapshot();
        let mut client_b = LegIo::for_authenticated_transport(
            LegId::B,
            LegEndpointRole::Client,
            binding(0x99),
            limits(),
        );
        let mut owner_b = LegIo::for_authenticated_transport(
            LegId::B,
            LegEndpointRole::Owner,
            binding(0x99),
            limits(),
        );
        let mut client_queue =
            LegOutboundQueue::for_leg(client_b.established_leg(), 1, 4_096).unwrap();
        let mut owner_queue =
            LegOutboundQueue::for_leg(owner_b.established_leg(), 1, 4_096).unwrap();
        let mut transport = transport();
        let pending = PendingStandbyRegistration::begin(
            &client_active,
            client_b.established_leg(),
            StandbyNonce::new([0x33; 16]).unwrap(),
            &credentials(),
        )
        .unwrap();
        let exact_request = pending.frame.clone();
        client_queue
            .push_leg_control(
                &pending.seal,
                exact_request.clone(),
                pending.contract.features,
            )
            .unwrap();

        let pressure = pending.enqueue(&mut client_queue).unwrap_err();
        assert!(matches!(
            pressure.kind(),
            LegControlQueueErrorKind::CapacityExceeded {
                queued_frames: 1,
                ..
            }
        ));
        let pending = pressure.into_pending();
        assert_eq!(pending.frame, exact_request);
        client_queue
            .try_flush(
                &mut client_b,
                &mut transport.controller_sender(),
                SimTime::ZERO,
            )
            .unwrap()
            .unwrap();
        release(&mut transport);
        let _discarded_pressure_filler = transport
            .try_recv_next(owner_b.inbound_route())
            .unwrap()
            .unwrap();

        let awaiting = pending.enqueue(&mut client_queue).unwrap();
        client_queue
            .try_flush(
                &mut client_b,
                &mut transport.controller_sender(),
                SimTime::ZERO,
            )
            .unwrap()
            .unwrap();
        release(&mut transport);
        let inbound = owner_b
            .receive_classified_delivery(
                transport
                    .try_recv_next(owner_b.inbound_route())
                    .unwrap()
                    .unwrap(),
                FeatureSet::new(FEATURES),
            )
            .unwrap();
        let candidate = ExactAuthenticatedStandby::authenticate(
            &owner_active,
            owner_b.established_leg(),
            inbound,
            &authority,
        )
        .unwrap();
        let acceptance_filler = candidate.contract().acceptance_frame().unwrap();
        owner_queue
            .push_leg_control(
                &candidate.seal,
                acceptance_filler,
                candidate.contract().features,
            )
            .unwrap();
        let mut slot = OwnerStandbySlot::new();
        let pressure = slot.admit(candidate, &mut owner_queue).unwrap_err();
        assert!(matches!(
            pressure.kind(),
            OwnerStandbyAdmissionErrorKind::Queue(LegControlQueueErrorKind::CapacityExceeded {
                queued_frames: 1,
                ..
            })
        ));
        assert!(slot.registered.is_none());
        let candidate = pressure.into_retry().unwrap();
        owner_queue
            .try_flush(
                &mut owner_b,
                &mut transport.controller_sender(),
                SimTime::ZERO,
            )
            .unwrap()
            .unwrap();
        release(&mut transport);
        let _discarded_pressure_filler = transport
            .try_recv_next(client_b.inbound_route())
            .unwrap()
            .unwrap();

        assert_eq!(
            slot.admit(candidate, &mut owner_queue).unwrap(),
            OwnerStandbyAdmission::Installed
        );
        let accepted = deliver_acceptance(
            &mut owner_b,
            &mut client_b,
            &mut owner_queue,
            &mut transport,
        );
        assert!(awaiting.validate(accepted).unwrap().is_live());
        assert!(slot.registered_is_live());
        assert_eq!(authority.current_generation(), generation_before);
        assert_eq!(model.snapshot(), model_before);
    }

    #[test]
    fn endpoint_or_sole_queue_loss_is_explicit_and_requires_retirement() {
        let (authority, model, client_active, owner_active) = active_pair();
        let generation_before = authority.current_generation();
        let model_before = model.snapshot();

        let lost_before_enqueue = LegIo::for_authenticated_transport(
            LegId::B,
            LegEndpointRole::Client,
            binding(0x91),
            limits(),
        );
        let pending = PendingStandbyRegistration::begin(
            &client_active,
            lost_before_enqueue.established_leg(),
            StandbyNonce::new([0x31; 16]).unwrap(),
            &credentials(),
        )
        .unwrap();
        let mut lost_queue =
            LegOutboundQueue::for_leg(lost_before_enqueue.established_leg(), 1, 1_024).unwrap();
        drop(lost_before_enqueue);
        let lost = pending.enqueue(&mut lost_queue).unwrap_err();
        assert_eq!(lost.kind(), &LegControlQueueErrorKind::EndpointLost);
        assert!(format!("{lost:?}").contains("[REDACTED]"));
        let _exact_unconsumed_pending = lost.into_pending();

        let client_waiting = LegIo::for_authenticated_transport(
            LegId::B,
            LegEndpointRole::Client,
            binding(0x92),
            limits(),
        );
        let mut waiting_queue =
            LegOutboundQueue::for_leg(client_waiting.established_leg(), 1, 1_024).unwrap();
        let pending = PendingStandbyRegistration::begin(
            &client_active,
            client_waiting.established_leg(),
            StandbyNonce::new([0x32; 16]).unwrap(),
            &credentials(),
        )
        .unwrap();
        let mut awaiting = pending.enqueue(&mut waiting_queue).unwrap();
        drop(waiting_queue);
        let alternate = LegIo::for_authenticated_transport(
            LegId::A,
            LegEndpointRole::Client,
            binding(0x93),
            limits(),
        );
        let mut alternate_queue =
            LegOutboundQueue::for_leg(alternate.established_leg(), 1, 1_024).unwrap();
        assert_eq!(
            awaiting.retry(&mut alternate_queue),
            Err(StandbyRetryError::QueueLost)
        );

        let mut client_b = LegIo::for_authenticated_transport(
            LegId::B,
            LegEndpointRole::Client,
            binding(0x99),
            limits(),
        );
        let mut owner_b = LegIo::for_authenticated_transport(
            LegId::B,
            LegEndpointRole::Owner,
            binding(0x99),
            limits(),
        );
        let mut client_queue =
            LegOutboundQueue::for_leg(client_b.established_leg(), 2, 2_048).unwrap();
        let mut owner_queue =
            LegOutboundQueue::for_leg(owner_b.established_leg(), 2, 2_048).unwrap();
        let mut transport = transport();
        let (awaiting, candidate) = deliver_registration(
            &client_active,
            &owner_active,
            &mut client_b,
            &mut owner_b,
            &mut client_queue,
            &mut transport,
            &authority,
            0x33,
        );
        let mut slot = OwnerStandbySlot::new();
        assert_eq!(
            slot.admit(candidate, &mut owner_queue).unwrap(),
            OwnerStandbyAdmission::Installed
        );
        let accepted = deliver_acceptance(
            &mut owner_b,
            &mut client_b,
            &mut owner_queue,
            &mut transport,
        );
        let registered = awaiting.validate(accepted).unwrap();
        assert!(registered.is_live());
        assert!(slot.registered_is_live());

        drop(client_queue);
        assert!(!registered.is_live());
        assert!(registered.retire_lost().is_ok());
        assert!(slot.retire_lost().is_none());
        drop(owner_b);
        assert!(!slot.registered_is_live());
        assert!(slot.retire_lost().is_some());
        assert!(slot.registered.is_none());
        assert_eq!(authority.current_generation(), generation_before);
        assert_eq!(model.snapshot(), model_before);
    }

    #[test]
    fn acceptance_consumes_only_exact_b_and_every_correlation_field() {
        let (_authority, _model, client_active, _owner_active) = active_pair();
        let client_b = LegIo::for_authenticated_transport(
            LegId::B,
            LegEndpointRole::Client,
            binding(0x99),
            limits(),
        );
        let mut client_queue =
            LegOutboundQueue::for_leg(client_b.established_leg(), 2, 2_048).unwrap();
        let pending = PendingStandbyRegistration::begin(
            &client_active,
            client_b.established_leg(),
            StandbyNonce::new([0x33; 16]).unwrap(),
            &credentials(),
        )
        .unwrap();
        let mut awaiting = pending.enqueue(&mut client_queue).unwrap();
        let contract = awaiting.contract;
        let accepted = |generation, session_id, nonce, version, features| {
            LegControlFrame::try_new(
                generation,
                LegControlRecord::StandbyAccepted {
                    session_id,
                    standby_nonce: nonce,
                    selected_version: version,
                    features,
                },
                FeatureSet::new(FEATURES),
            )
            .unwrap()
        };
        let wrong = [
            accepted(
                LegGeneration::new(contract.generation.get() + 1).unwrap(),
                contract.session_id,
                contract.nonce,
                contract.selected_version,
                contract.features,
            ),
            accepted(
                contract.generation,
                SessionId::new([0x12; 16]).unwrap(),
                contract.nonce,
                contract.selected_version,
                contract.features,
            ),
            accepted(
                contract.generation,
                contract.session_id,
                StandbyNonce::new([0x34; 16]).unwrap(),
                contract.selected_version,
                contract.features,
            ),
            accepted(
                contract.generation,
                contract.session_id,
                contract.nonce,
                contract.selected_version + 1,
                contract.features,
            ),
            accepted(
                contract.generation,
                contract.session_id,
                contract.nonce,
                contract.selected_version,
                FeatureSet::new(contract.features.bits() ^ 0b1),
            ),
        ];
        for frame in wrong {
            let inbound = BoundInbound::LegControl(
                client_b
                    .established_leg()
                    .bind_received_control_frame(frame),
            );
            let failure = awaiting.validate(inbound).unwrap_err();
            assert_eq!(failure.kind(), StandbyAcceptanceErrorKind::Rejected);
            awaiting = failure.into_awaiting();
        }

        let wrong_leg = EstablishedLeg::for_authenticated_transport(binding(0x99));
        let inbound = BoundInbound::LegControl(
            wrong_leg.bind_received_control_frame(contract.acceptance_frame().unwrap()),
        );
        let failure = awaiting.validate(inbound).unwrap_err();
        assert_eq!(failure.kind(), StandbyAcceptanceErrorKind::Rejected);
        awaiting = failure.into_awaiting();

        let request = AttachRequest::new(
            contract.session_id,
            LegGeneration::new(contract.generation.get() + 1).unwrap(),
            AttachNonce::new([0x45; 16]).unwrap(),
            VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION).unwrap(),
            FeatureOffer::new(FEATURES, STANDBY_CONTROL_V1.bits()).unwrap(),
        );
        let proof = credentials().prove(&request, &binding(0x99)).unwrap();
        let session_fact = BoundInbound::Session(
            client_b
                .established_leg()
                .bind_received_frame(request.to_attach_frame(proof)),
        );
        let failure = awaiting.validate(session_fact).unwrap_err();
        assert_eq!(failure.kind(), StandbyAcceptanceErrorKind::Rejected);
        awaiting = failure.into_awaiting();

        let exact = BoundInbound::LegControl(
            client_b
                .established_leg()
                .bind_received_control_frame(contract.acceptance_frame().unwrap()),
        );
        assert!(awaiting.validate(exact).unwrap().is_live());
    }

    #[test]
    fn real_leg_control_keeps_the_sole_queue_open_for_ordered_attach_acceptance() {
        let (authority, model, client_active, owner_active) = active_pair();
        let mut client_b = LegIo::for_authenticated_transport(
            LegId::B,
            LegEndpointRole::Client,
            binding(0x99),
            limits(),
        );
        let mut owner_b = LegIo::for_authenticated_transport(
            LegId::B,
            LegEndpointRole::Owner,
            binding(0x99),
            limits(),
        );
        let mut client_queue =
            LegOutboundQueue::for_leg(client_b.established_leg(), 4, 4_096).unwrap();
        let mut owner_queue =
            LegOutboundQueue::for_leg(owner_b.established_leg(), 4, 4_096).unwrap();
        let mut transport = transport();
        let (awaiting, candidate) = deliver_registration(
            &client_active,
            &owner_active,
            &mut client_b,
            &mut owner_b,
            &mut client_queue,
            &mut transport,
            &authority,
            0x33,
        );
        let mut slot = OwnerStandbySlot::new();
        assert_eq!(
            slot.admit(candidate, &mut owner_queue).unwrap(),
            OwnerStandbyAdmission::Installed
        );
        let standby_accepted = deliver_acceptance(
            &mut owner_b,
            &mut client_b,
            &mut owner_queue,
            &mut transport,
        );
        let registered = awaiting.validate(standby_accepted).unwrap();

        let supervisor =
            SessionSupervisor::new_owner_with_active(model, authority, &owner_active).unwrap();
        let mut executor = OwnerTargetExecutor::new(
            OwnerTargetConfig::new(64, 1_024, 64).unwrap(),
            supervisor,
            MemoryTarget::new(MemoryTargetConfig::new(4, 256, 512, 128, 128, 16).unwrap()),
            FlowPortConfig::new(64, 8, 8, 8).unwrap(),
        )
        .unwrap();

        let pending_attach = registered
            .begin_exact_next_attach(AttachNonce::new([0x44; 16]).unwrap(), &credentials())
            .unwrap();
        let awaiting_attach = pending_attach.enqueue(&mut client_queue).unwrap();
        client_queue
            .try_flush(
                &mut client_b,
                &mut transport.controller_sender(),
                SimTime::ZERO,
            )
            .unwrap()
            .unwrap();
        release(&mut transport);
        let received_attach = owner_b
            .receive_delivery(
                transport
                    .try_recv_next(owner_b.inbound_route())
                    .unwrap()
                    .unwrap(),
            )
            .unwrap();
        let registered_owner = slot.registered.as_ref().unwrap();
        let later_control = registered_owner
            .candidate
            .contract()
            .acceptance_frame()
            .unwrap();
        let registered_features = registered_owner.candidate.contract().features;
        let owner_b_seal = owner_b.established_leg().standby_seal();
        let OwnerTargetAttachPublication::Installed { pending } = executor
            .accept_registered_owner_attach(
                &mut slot,
                owner_b.established_leg(),
                received_attach,
                &mut owner_queue,
            )
            .unwrap()
        else {
            panic!("exact B attach unexpectedly resynchronized");
        };
        assert!(!slot.registered_is_live());
        assert!(slot.registered.is_none());
        let recovery = pending.enqueue_acceptance(&mut owner_queue).unwrap();
        let error = owner_queue
            .push_leg_control(&owner_b_seal, later_control, registered_features)
            .unwrap_err();
        assert_eq!(
            error.kind(),
            &LegControlQueueErrorKind::AttachRecoveryPending
        );
        let later_control = error.into_frame();
        let recovery_outputs = executor
            .execute_attached_recovery(recovery, &mut owner_queue)
            .unwrap();
        assert!(recovery_outputs.iter().any(|output| matches!(
            output,
            crate::owned_upstream::owner_target::OwnerTargetRecoveryOutput::RecoveryCompleted {
                attached
            } if attached.generation() == LegGeneration::new(3).unwrap()
        )));

        owner_queue
            .try_flush(
                &mut owner_b,
                &mut transport.controller_sender(),
                SimTime::ZERO,
            )
            .unwrap()
            .unwrap();
        owner_queue
            .push_leg_control(&owner_b_seal, later_control, registered_features)
            .unwrap();
        assert_eq!(
            owner_queue.try_flush(
                &mut owner_b,
                &mut transport.controller_sender(),
                SimTime::ZERO,
            ),
            Err(crate::owned_upstream::leg_io::LegIoError::OrderedDeliveryPending)
        );

        release(&mut transport);
        let accepted = client_b
            .receive_delivery(
                transport
                    .try_recv_next(client_b.inbound_route())
                    .unwrap()
                    .unwrap(),
            )
            .unwrap();
        assert!(matches!(
            awaiting_attach
                .validate_response(BoundInbound::Session(accepted))
                .unwrap(),
            RegisteredAttachResponse::Accepted(_)
        ));
        owner_queue
            .try_flush(
                &mut owner_b,
                &mut transport.controller_sender(),
                SimTime::ZERO,
            )
            .unwrap()
            .unwrap();
        assert!(owner_queue.is_empty());
        assert!(!slot.registered_is_live());
        assert_eq!(executor.snapshot().session.session.generation().get(), 3);

        let duplicate = AttachRequest::new(
            SessionId::new([0x11; 16]).unwrap(),
            LegGeneration::new(3).unwrap(),
            AttachNonce::new([0x44; 16]).unwrap(),
            VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION).unwrap(),
            FeatureOffer::new(FEATURES, FEATURES).unwrap(),
        );
        let duplicate_proof = credentials().prove(&duplicate, &binding(0x99)).unwrap();
        let duplicate = owner_b
            .established_leg()
            .bind_received_frame(duplicate.to_attach_frame(duplicate_proof));
        let before_duplicate = executor.snapshot();
        let returned = match executor.accept_registered_owner_attach(
            &mut slot,
            owner_b.established_leg(),
            duplicate,
            &mut owner_queue,
        ) {
            Err(OwnerTargetError::RejectedRegisteredOwnerAttach { received, .. }) => received,
            other => panic!("consumed registered slot must reject duplicate, got {other:?}"),
        };
        drop(returned);
        assert_eq!(executor.snapshot(), before_duplicate);
    }

    #[test]
    fn registered_lost_acceptance_catch_up_crosses_b_c_d_bytes_without_raw_requests() {
        let (authority, model, client_active, owner_active) = active_pair();
        let (mut client_b, client_b_reporter) = LegIo::for_authenticated_transport_with_reporter(
            LegId::B,
            LegEndpointRole::Client,
            binding(0x99),
            limits(),
        );
        let mut owner_b = LegIo::for_authenticated_transport(
            LegId::B,
            LegEndpointRole::Owner,
            binding(0x99),
            limits(),
        );
        let mut client_b_queue =
            LegOutboundQueue::for_leg(client_b.established_leg(), 4, 4_096).unwrap();
        let mut owner_b_queue =
            LegOutboundQueue::for_leg(owner_b.established_leg(), 4, 4_096).unwrap();
        let mut transport_b = transport();
        let (awaiting_registration, candidate) = deliver_registration(
            &client_active,
            &owner_active,
            &mut client_b,
            &mut owner_b,
            &mut client_b_queue,
            &mut transport_b,
            &authority,
            0x33,
        );
        let mut slot = OwnerStandbySlot::new();
        assert_eq!(
            slot.admit(candidate, &mut owner_b_queue).unwrap(),
            OwnerStandbyAdmission::Installed
        );
        let registered = awaiting_registration
            .validate(deliver_acceptance(
                &mut owner_b,
                &mut client_b,
                &mut owner_b_queue,
                &mut transport_b,
            ))
            .unwrap();
        let supervisor =
            SessionSupervisor::new_owner_with_active(model, authority, &owner_active).unwrap();
        let mut executor = OwnerTargetExecutor::new(
            OwnerTargetConfig::new(64, 1_024, 64).unwrap(),
            supervisor,
            MemoryTarget::new(MemoryTargetConfig::new(4, 256, 512, 128, 128, 16).unwrap()),
            FlowPortConfig::new(64, 8, 8, 8).unwrap(),
        )
        .unwrap();

        // Registered B derives, signs, queues, and delivers generation 3
        // without exposing an AttachRequest or Frame to this controller test.
        let awaiting_b = registered
            .begin_exact_next_attach(AttachNonce::new([0x44; 16]).unwrap(), &credentials())
            .unwrap()
            .enqueue(&mut client_b_queue)
            .unwrap();
        client_b_queue
            .try_flush(
                &mut client_b,
                &mut transport_b.controller_sender(),
                SimTime::ZERO,
            )
            .unwrap()
            .unwrap();
        release(&mut transport_b);
        let received_b = owner_b
            .receive_delivery(
                transport_b
                    .try_recv_next(owner_b.inbound_route())
                    .unwrap()
                    .unwrap(),
            )
            .unwrap();
        let OwnerTargetAttachPublication::Installed { pending } = executor
            .accept_registered_owner_attach(
                &mut slot,
                owner_b.established_leg(),
                received_b,
                &mut owner_b_queue,
            )
            .unwrap()
        else {
            panic!("registered B must install generation 3");
        };
        let recovery = pending.enqueue_acceptance(&mut owner_b_queue).unwrap();
        executor
            .execute_attached_recovery(recovery, &mut owner_b_queue)
            .unwrap();
        owner_b_queue
            .try_flush(
                &mut owner_b,
                &mut transport_b.controller_sender(),
                SimTime::ZERO,
            )
            .unwrap()
            .unwrap();
        release(&mut transport_b);
        assert_eq!(executor.snapshot().session.session.generation().get(), 3);

        // B's acceptance is lost. Its exact terminal fact converts only this
        // delivered request into a non-cloneable stale-status retry authority.
        let client_b_terminal = client_b_reporter
            .report(LegTransportTerminalReason::FatalIo)
            .unwrap();
        let mut retry = awaiting_b
            .into_status_retry_after_terminal(client_b_terminal)
            .unwrap();

        let base_c = binding(0x9a);
        for wrong_identity_binding in [
            AttachTransportBinding::new(
                OwnerIdentity::new([0x32; 32]).unwrap(),
                base_c.alpn(),
                TlsExporterBinding::new([0xe1; 32]).unwrap(),
                base_c.device_principal(),
            ),
            AttachTransportBinding::new(
                base_c.owner_identity(),
                AttachAlpn::new(b"mini-vpn-wrong/1").unwrap(),
                TlsExporterBinding::new([0xe2; 32]).unwrap(),
                base_c.device_principal(),
            ),
            AttachTransportBinding::new(
                base_c.owner_identity(),
                base_c.alpn(),
                TlsExporterBinding::new([0xe3; 32]).unwrap(),
                DevicePrincipal::new([0x54; 16]).unwrap(),
            ),
        ] {
            let wrong_identity_c =
                EstablishedLeg::for_authenticated_transport(wrong_identity_binding);
            retry = retry
                .bind_fresh_status_leg(&wrong_identity_c, &credentials())
                .unwrap_err()
                .into_authority();
        }
        let (terminal_c, terminal_c_reporter) =
            EstablishedLeg::for_authenticated_transport_with_reporter(binding(0xa4));
        let _terminal_c_fact = terminal_c_reporter
            .report(LegTransportTerminalReason::Reset)
            .unwrap();
        retry = retry
            .bind_fresh_status_leg(&terminal_c, &credentials())
            .unwrap_err()
            .into_authority();
        retry = retry
            .bind_fresh_status_leg(client_b.established_leg(), &credentials())
            .unwrap_err()
            .into_authority();

        // Fresh C internally re-signs B's immutable request contract, uses
        // C's sole ordered queue, and receives a typed generation status.
        let mut client_c = LegIo::for_authenticated_transport(
            LegId::A,
            LegEndpointRole::Client,
            binding(0x9a),
            limits(),
        );
        let mut owner_c = LegIo::for_authenticated_transport(
            LegId::A,
            LegEndpointRole::Owner,
            binding(0x9a),
            limits(),
        );
        let mut client_c_queue =
            LegOutboundQueue::for_leg(client_c.established_leg(), 4, 4_096).unwrap();
        let mut owner_c_queue =
            LegOutboundQueue::for_leg(owner_c.established_leg(), 4, 4_096).unwrap();
        let mut transport_c = transport();
        let pending_c = retry
            .bind_fresh_status_leg(client_c.established_leg(), &credentials())
            .unwrap();
        let wrong_c = EstablishedLeg::for_authenticated_transport(binding(0x9b));
        let mut wrong_c_queue = LegOutboundQueue::for_leg(&wrong_c, 4, 4_096).unwrap();
        let pending_c = pending_c
            .enqueue(&mut wrong_c_queue)
            .unwrap_err()
            .into_pending();
        let awaiting_c = pending_c.enqueue(&mut client_c_queue).unwrap();
        client_c_queue
            .try_flush(
                &mut client_c,
                &mut transport_c.controller_sender(),
                SimTime::ZERO,
            )
            .unwrap()
            .unwrap();
        release(&mut transport_c);
        let received_c = owner_c
            .receive_delivery(
                transport_c
                    .try_recv_next(owner_c.inbound_route())
                    .unwrap()
                    .unwrap(),
            )
            .unwrap();
        let enqueued_status = executor
            .resynchronize_owner_attach(owner_c.established_leg(), received_c)
            .unwrap()
            .enqueue(&mut owner_c_queue)
            .unwrap();
        owner_c_queue
            .try_flush(
                &mut owner_c,
                &mut transport_c.controller_sender(),
                SimTime::ZERO,
            )
            .unwrap()
            .unwrap();
        release(&mut transport_c);
        let received_status = client_c
            .receive_delivery(
                transport_c
                    .try_recv_next(client_c.inbound_route())
                    .unwrap()
                    .unwrap(),
            )
            .unwrap();
        executor
            .arm_owner_catch_up_after_status_delivery(enqueued_status, &owner_c_queue)
            .unwrap();
        let mut status = awaiting_c
            .validate_status(BoundInbound::Session(received_status))
            .unwrap();

        let base_d = binding(0x9d);
        for wrong_identity_binding in [
            AttachTransportBinding::new(
                OwnerIdentity::new([0x32; 32]).unwrap(),
                base_d.alpn(),
                TlsExporterBinding::new([0xe4; 32]).unwrap(),
                base_d.device_principal(),
            ),
            AttachTransportBinding::new(
                base_d.owner_identity(),
                AttachAlpn::new(b"mini-vpn-wrong/1").unwrap(),
                TlsExporterBinding::new([0xe5; 32]).unwrap(),
                base_d.device_principal(),
            ),
            AttachTransportBinding::new(
                base_d.owner_identity(),
                base_d.alpn(),
                TlsExporterBinding::new([0xe6; 32]).unwrap(),
                DevicePrincipal::new([0x54; 16]).unwrap(),
            ),
        ] {
            let wrong_identity_d =
                EstablishedLeg::for_authenticated_transport(wrong_identity_binding);
            status = status
                .begin_catch_up(
                    &wrong_identity_d,
                    AttachNonce::new([0x55; 16]).unwrap(),
                    &credentials(),
                )
                .unwrap_err()
                .into_status();
        }
        let (terminal_d, terminal_d_reporter) =
            EstablishedLeg::for_authenticated_transport_with_reporter(binding(0xa5));
        let _terminal_d_fact = terminal_d_reporter
            .report(LegTransportTerminalReason::FatalIo)
            .unwrap();
        status = status
            .begin_catch_up(
                &terminal_d,
                AttachNonce::new([0x55; 16]).unwrap(),
                &credentials(),
            )
            .unwrap_err()
            .into_status();
        status = status
            .begin_catch_up(
                client_c.established_leg(),
                AttachNonce::new([0x55; 16]).unwrap(),
                &credentials(),
            )
            .unwrap_err()
            .into_status();

        // The status capability derives and signs only generation 4 for fresh
        // D, which again must cross its exact sole ordered queue.
        let (mut client_d, client_d_reporter) = LegIo::for_authenticated_transport_with_reporter(
            LegId::B,
            LegEndpointRole::Client,
            binding(0x9d),
            limits(),
        );
        let mut owner_d = LegIo::for_authenticated_transport(
            LegId::B,
            LegEndpointRole::Owner,
            binding(0x9d),
            limits(),
        );
        let mut client_d_queue =
            LegOutboundQueue::for_leg(client_d.established_leg(), 4, 4_096).unwrap();
        let mut owner_d_queue =
            LegOutboundQueue::for_leg(owner_d.established_leg(), 4, 4_096).unwrap();
        let mut transport_d = transport();
        let pending_d = status
            .begin_catch_up(
                client_d.established_leg(),
                AttachNonce::new([0x55; 16]).unwrap(),
                &credentials(),
            )
            .unwrap();
        let wrong_d = EstablishedLeg::for_authenticated_transport(binding(0x9e));
        let mut wrong_d_queue = LegOutboundQueue::for_leg(&wrong_d, 4, 4_096).unwrap();
        let pending_d = pending_d
            .enqueue(&mut wrong_d_queue)
            .unwrap_err()
            .into_pending();
        let awaiting_d = pending_d.enqueue(&mut client_d_queue).unwrap();
        let failure = awaiting_d.into_status_retry_after_queue_loss().unwrap_err();
        assert_eq!(
            failure.kind(),
            RegisteredAttachStatusRetryErrorKind::RequestNotDelivered
        );
        let awaiting_d = failure.into_awaiting();
        client_d_queue
            .try_flush(
                &mut client_d,
                &mut transport_d.controller_sender(),
                SimTime::ZERO,
            )
            .unwrap()
            .unwrap();
        release(&mut transport_d);
        let received_d = owner_d
            .receive_delivery(
                transport_d
                    .try_recv_next(owner_d.inbound_route())
                    .unwrap()
                    .unwrap(),
            )
            .unwrap();
        let failure = awaiting_d.into_status_retry_after_queue_loss().unwrap_err();
        assert_eq!(
            failure.kind(),
            RegisteredAttachStatusRetryErrorKind::OriginalTransportStillLive
        );
        let awaiting_d = failure.into_awaiting();
        let OwnerTargetAttachPublication::Installed { pending } = executor
            .accept_catch_up_owner_attach(owner_d.established_leg(), received_d, &mut owner_d_queue)
            .unwrap()
        else {
            panic!("fresh D must install generation 4");
        };
        let recovery = pending.enqueue_acceptance(&mut owner_d_queue).unwrap();
        executor
            .execute_attached_recovery(recovery, &mut owner_d_queue)
            .unwrap();
        owner_d_queue
            .try_flush(
                &mut owner_d,
                &mut transport_d.controller_sender(),
                SimTime::ZERO,
            )
            .unwrap()
            .unwrap();
        release(&mut transport_d);

        // D's generation-4 acceptance is lost as well. A wrong terminal must
        // return both exact values; only D's own terminal can retire its
        // delivered validator and recover the original B/gen3 correlation.
        let (_wrong_d, wrong_d_reporter) =
            EstablishedLeg::for_authenticated_transport_with_reporter(binding(0xa6));
        let wrong_d_terminal = wrong_d_reporter
            .report(LegTransportTerminalReason::FatalIo)
            .unwrap();
        let failure = awaiting_d
            .into_status_retry_after_terminal(wrong_d_terminal)
            .unwrap_err();
        assert_eq!(
            failure.kind(),
            RegisteredAttachStatusRetryErrorKind::WrongTerminal
        );
        let (awaiting_d, _wrong_d_terminal) = failure.into_parts();
        let client_d_terminal = client_d_reporter
            .report(LegTransportTerminalReason::FatalIo)
            .unwrap();
        let retry = awaiting_d
            .into_status_retry_after_terminal(client_d_terminal)
            .unwrap();
        assert_eq!(executor.snapshot().session.session.generation().get(), 4);
        assert!(!executor.snapshot().session.owner_catch_up_permit);

        // Fresh C2 must internally re-sign the original B/gen3 stale
        // correlation. Re-signing D/gen4 would derive the wrong local floor
        // and cannot validate the final generation-5 client catch-up.
        let mut client_c2 = LegIo::for_authenticated_transport(
            LegId::A,
            LegEndpointRole::Client,
            binding(0xa7),
            limits(),
        );
        let mut owner_c2 = LegIo::for_authenticated_transport(
            LegId::A,
            LegEndpointRole::Owner,
            binding(0xa7),
            limits(),
        );
        let mut client_c2_queue =
            LegOutboundQueue::for_leg(client_c2.established_leg(), 4, 4_096).unwrap();
        let mut owner_c2_queue =
            LegOutboundQueue::for_leg(owner_c2.established_leg(), 4, 4_096).unwrap();
        let mut transport_c2 = transport();
        let awaiting_c2 = retry
            .bind_fresh_status_leg(client_c2.established_leg(), &credentials())
            .unwrap()
            .enqueue(&mut client_c2_queue)
            .unwrap();
        client_c2_queue
            .try_flush(
                &mut client_c2,
                &mut transport_c2.controller_sender(),
                SimTime::ZERO,
            )
            .unwrap()
            .unwrap();
        release(&mut transport_c2);
        let received_c2 = owner_c2
            .receive_delivery(
                transport_c2
                    .try_recv_next(owner_c2.inbound_route())
                    .unwrap()
                    .unwrap(),
            )
            .unwrap();
        let enqueued_status = executor
            .resynchronize_owner_attach(owner_c2.established_leg(), received_c2)
            .unwrap()
            .enqueue(&mut owner_c2_queue)
            .unwrap();
        owner_c2_queue
            .try_flush(
                &mut owner_c2,
                &mut transport_c2.controller_sender(),
                SimTime::ZERO,
            )
            .unwrap()
            .unwrap();
        release(&mut transport_c2);
        let received_status = client_c2
            .receive_delivery(
                transport_c2
                    .try_recv_next(client_c2.inbound_route())
                    .unwrap()
                    .unwrap(),
            )
            .unwrap();
        executor
            .arm_owner_catch_up_after_status_delivery(enqueued_status, &owner_c2_queue)
            .unwrap();
        let status = awaiting_c2
            .validate_status(BoundInbound::Session(received_status))
            .unwrap();
        assert_eq!(status.current_generation().get(), 4);

        // The second status derives exactly E/gen5 on another fresh sole
        // queue. The resulting wrapper retains the E request receipt and the
        // original client floor inside its catch-up authority.
        let mut client_e = LegIo::for_authenticated_transport(
            LegId::B,
            LegEndpointRole::Client,
            binding(0xa8),
            limits(),
        );
        let mut owner_e = LegIo::for_authenticated_transport(
            LegId::B,
            LegEndpointRole::Owner,
            binding(0xa8),
            limits(),
        );
        let mut client_e_queue =
            LegOutboundQueue::for_leg(client_e.established_leg(), 4, 4_096).unwrap();
        let mut owner_e_queue =
            LegOutboundQueue::for_leg(owner_e.established_leg(), 4, 4_096).unwrap();
        let mut transport_e = transport();
        let awaiting_e = status
            .begin_catch_up(
                client_e.established_leg(),
                AttachNonce::new([0x66; 16]).unwrap(),
                &credentials(),
            )
            .unwrap()
            .enqueue(&mut client_e_queue)
            .unwrap();
        client_e_queue
            .try_flush(
                &mut client_e,
                &mut transport_e.controller_sender(),
                SimTime::ZERO,
            )
            .unwrap()
            .unwrap();
        release(&mut transport_e);
        let received_e = owner_e
            .receive_delivery(
                transport_e
                    .try_recv_next(owner_e.inbound_route())
                    .unwrap()
                    .unwrap(),
            )
            .unwrap();
        let OwnerTargetAttachPublication::Installed { pending } = executor
            .accept_catch_up_owner_attach(owner_e.established_leg(), received_e, &mut owner_e_queue)
            .unwrap()
        else {
            panic!("fresh E must install generation 5")
        };
        let recovery = pending.enqueue_acceptance(&mut owner_e_queue).unwrap();
        executor
            .execute_attached_recovery(recovery, &mut owner_e_queue)
            .unwrap();
        owner_e_queue
            .try_flush(
                &mut owner_e,
                &mut transport_e.controller_sender(),
                SimTime::ZERO,
            )
            .unwrap()
            .unwrap();
        release(&mut transport_e);
        let accepted = awaiting_e
            .validate_response(BoundInbound::Session(
                client_e
                    .receive_delivery(
                        transport_e
                            .try_recv_next(client_e.inbound_route())
                            .unwrap()
                            .unwrap(),
                    )
                    .unwrap(),
            ))
            .unwrap();
        assert_eq!(accepted.generation().get(), 5);
        assert!(accepted.follow_up_queue_is_live());
        assert_eq!(executor.snapshot().session.session.generation().get(), 5);
        assert!(!executor.snapshot().session.owner_catch_up_permit);
    }

    #[test]
    fn status_retry_consumes_only_delivered_exact_terminal_or_lost_sole_queue() {
        let (authority, _model, client_active, owner_active) = active_pair();
        let mut client_b = LegIo::for_authenticated_transport(
            LegId::B,
            LegEndpointRole::Client,
            binding(0xa1),
            limits(),
        );
        let mut owner_b = LegIo::for_authenticated_transport(
            LegId::B,
            LegEndpointRole::Owner,
            binding(0xa1),
            limits(),
        );
        let mut client_b_queue =
            LegOutboundQueue::for_leg(client_b.established_leg(), 4, 4_096).unwrap();
        let mut owner_b_queue =
            LegOutboundQueue::for_leg(owner_b.established_leg(), 4, 4_096).unwrap();
        let mut transport_b = transport();
        let (awaiting_registration, candidate) = deliver_registration(
            &client_active,
            &owner_active,
            &mut client_b,
            &mut owner_b,
            &mut client_b_queue,
            &mut transport_b,
            &authority,
            0x71,
        );
        let mut slot = OwnerStandbySlot::new();
        slot.admit(candidate, &mut owner_b_queue).unwrap();
        let registered = awaiting_registration
            .validate(deliver_acceptance(
                &mut owner_b,
                &mut client_b,
                &mut owner_b_queue,
                &mut transport_b,
            ))
            .unwrap();
        let mut awaiting = registered
            .begin_exact_next_attach(AttachNonce::new([0x72; 16]).unwrap(), &credentials())
            .unwrap()
            .enqueue(&mut client_b_queue)
            .unwrap();

        let failure = awaiting.into_status_retry_after_queue_loss().unwrap_err();
        assert_eq!(
            failure.kind(),
            RegisteredAttachStatusRetryErrorKind::RequestNotDelivered
        );
        awaiting = failure.into_awaiting();

        let (_wrong_leg, wrong_reporter) =
            EstablishedLeg::for_authenticated_transport_with_reporter(binding(0xa2));
        let wrong_terminal = wrong_reporter
            .report(LegTransportTerminalReason::Reset)
            .unwrap();
        let failure = awaiting
            .into_status_retry_after_terminal(wrong_terminal)
            .unwrap_err();
        assert_eq!(
            failure.kind(),
            RegisteredAttachStatusRetryErrorKind::RequestNotDelivered
        );
        let (returned, wrong_terminal) = failure.into_parts();
        awaiting = returned;

        client_b_queue
            .try_flush(
                &mut client_b,
                &mut transport_b.controller_sender(),
                SimTime::ZERO,
            )
            .unwrap()
            .unwrap();
        release(&mut transport_b);
        let _delivered_request = owner_b
            .receive_delivery(
                transport_b
                    .try_recv_next(owner_b.inbound_route())
                    .unwrap()
                    .unwrap(),
            )
            .unwrap();

        let failure = awaiting
            .into_status_retry_after_terminal(wrong_terminal)
            .unwrap_err();
        assert_eq!(
            failure.kind(),
            RegisteredAttachStatusRetryErrorKind::WrongTerminal
        );
        let (returned, _wrong_terminal) = failure.into_parts();
        awaiting = returned;

        let failure = awaiting.into_status_retry_after_queue_loss().unwrap_err();
        assert_eq!(
            failure.kind(),
            RegisteredAttachStatusRetryErrorKind::OriginalTransportStillLive
        );
        awaiting = failure.into_awaiting();
        drop(client_b_queue);
        assert!(client_b.established_leg().transport_is_open());
        let authority = awaiting.into_status_retry_after_queue_loss().unwrap();

        let fresh_c = EstablishedLeg::for_authenticated_transport(binding(0xa3));
        let mut fresh_c_queue = LegOutboundQueue::for_leg(&fresh_c, 4, 4_096).unwrap();
        authority
            .bind_fresh_status_leg(&fresh_c, &credentials())
            .unwrap()
            .enqueue(&mut fresh_c_queue)
            .unwrap();
        assert_eq!(fresh_c_queue.len(), 1);
    }

    #[test]
    fn terminal_report_serializes_with_attach_control_and_poison_fails_closed() {
        let (leg, reporter) =
            EstablishedLeg::for_authenticated_transport_with_reporter(binding(0xb1));
        let transition = leg.lock_terminal_transition().unwrap();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (completed_tx, completed_rx) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            completed_tx
                .send(reporter.report(LegTransportTerminalReason::PeerClosed))
                .unwrap();
        });
        started_rx.recv().unwrap();
        assert!(matches!(
            completed_rx.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ));
        assert!(leg.transport_is_open());
        drop(transition);
        let terminal = completed_rx.recv().unwrap().unwrap();
        worker.join().unwrap();
        assert_eq!(terminal.reason(), LegTransportTerminalReason::PeerClosed);
        assert!(!leg.transport_is_open());

        let (poisoned, poisoned_reporter) =
            EstablishedLeg::for_authenticated_transport_with_reporter(binding(0xb2));
        poisoned.poison_terminal_transition_for_test();
        assert!(matches!(
            poisoned.lock_terminal_transition(),
            Err(crate::owned_upstream::leg::LegTerminalTransitionError::Poisoned)
        ));
        assert!(!poisoned.transport_is_open());
        assert!(matches!(
            poisoned_reporter.report(LegTransportTerminalReason::FatalIo),
            Err(crate::owned_upstream::leg::LegTerminalTransitionError::Poisoned)
        ));
    }

    #[test]
    fn standby_typestate_debug_is_redacted() {
        let (authority, _model, client_active, owner_active) = active_pair();
        let mut client_b = LegIo::for_authenticated_transport(
            LegId::B,
            LegEndpointRole::Client,
            binding(0x99),
            limits(),
        );
        let mut owner_b = LegIo::for_authenticated_transport(
            LegId::B,
            LegEndpointRole::Owner,
            binding(0x99),
            limits(),
        );
        let mut client_queue =
            LegOutboundQueue::for_leg(client_b.established_leg(), 2, 2_048).unwrap();
        let mut owner_queue =
            LegOutboundQueue::for_leg(owner_b.established_leg(), 2, 2_048).unwrap();
        let mut transport = transport();
        let pending = PendingStandbyRegistration::begin(
            &client_active,
            client_b.established_leg(),
            StandbyNonce::new([0x33; 16]).unwrap(),
            &credentials(),
        )
        .unwrap();
        let mut debug = vec![format!("{pending:?}")];
        let awaiting = pending.enqueue(&mut client_queue).unwrap();
        debug.push(format!("{awaiting:?}"));
        client_queue
            .try_flush(
                &mut client_b,
                &mut transport.controller_sender(),
                SimTime::ZERO,
            )
            .unwrap()
            .unwrap();
        release(&mut transport);
        let inbound = owner_b
            .receive_classified_delivery(
                transport
                    .try_recv_next(owner_b.inbound_route())
                    .unwrap()
                    .unwrap(),
                FeatureSet::new(FEATURES),
            )
            .unwrap();
        debug.push(format!("{inbound:?}"));
        let candidate = ExactAuthenticatedStandby::authenticate(
            &owner_active,
            owner_b.established_leg(),
            inbound,
            &authority,
        )
        .unwrap();
        debug.push(format!("{candidate:?}"));
        let mut slot = OwnerStandbySlot::new();
        slot.admit(candidate, &mut owner_queue).unwrap();
        debug.push(format!("{slot:?}"));

        for rendered in debug {
            assert!(rendered.contains("[REDACTED]") || rendered.starts_with("OwnerStandbySlot"));
            assert!(!rendered.contains("mini-vpn-owned/1"));
            assert!(!rendered.contains("1111111111111111"));
            assert!(!rendered.contains("3333333333333333"));
            assert!(!rendered.contains("9999999999999999"));
            assert!(!rendered.contains("7171717171717171"));
        }
    }

    // Every authority-bearing registration stage is consuming. Ambiguous
    // inference fails to compile if any of these types gains `Clone`.
    trait AmbiguousIfClone<Marker> {
        fn marker() {}
    }
    impl<T: ?Sized> AmbiguousIfClone<()> for T {}
    impl<T: Clone> AmbiguousIfClone<u8> for T {}
    const _: fn() = || {
        let _ = <PendingStandbyRegistration as AmbiguousIfClone<_>>::marker;
        let _ = <AwaitingStandbyAccepted as AmbiguousIfClone<_>>::marker;
        let _ = <ClientRegisteredStandby as AmbiguousIfClone<_>>::marker;
        let _ = <ExactAuthenticatedStandby as AmbiguousIfClone<_>>::marker;
        let _ = <OwnerRegisteredStandby as AmbiguousIfClone<_>>::marker;
        let _ = <OwnerStandbySlot as AmbiguousIfClone<_>>::marker;
        let _ = <LegControlEnqueued as AmbiguousIfClone<_>>::marker;
        let _ = <RegisteredPendingAttachAuthority as AmbiguousIfClone<_>>::marker;
        let _ = <RegisteredAttachResponseGate as AmbiguousIfClone<_>>::marker;
        let _ = <RegisteredCatchUpBeginGate as AmbiguousIfClone<_>>::marker;
        let _ = <RegisteredCatchUpAssemblyGate as AmbiguousIfClone<_>>::marker;
        let _ = <RegisteredCatchUpResponseGate as AmbiguousIfClone<_>>::marker;
        let _ = <AcceptedClientRegisteredCatchUp as AmbiguousIfClone<_>>::marker;
    };
}
