//! Exact-leg standby registration for one already-installed resumable session.
//!
//! This module owns only the bounded registration typestate. It cannot commit
//! an attach generation, mutate the session reducer, or mint data-plane
//! authority.

use super::client_controller::ClientControllerInstallGate;
use super::leg::{
    AttachedLeg, BoundInbound, ClientActiveLeg, ClientInstallTransaction, EstablishedLeg,
    ExactLegTerminal, InstalledLegProbeSource, LegBoundControlFrame, LegBoundFrame,
    LegGenerationStatus, LegProvenanceError, LegSeal, LegTransportEndpoint, LostPendingAttach,
    PendingAttach, PendingCatchUpAttach, RegisteredPendingAttachResponse,
    UninstalledRegisteredAttach, UninstalledRegisteredCatchUp,
};
use super::leg_io::{
    AttachAcceptanceReservation, AttachAcceptanceReserveError, AttachRequestEnqueued,
    CatchUpAttachEnqueued, CausallyDeliveredAttachRequest, ClientLegTransport,
    ClientRecoveryPlanError, ClientRecoveryReservation, ClientRecoveryReserveError,
    LegControlEnqueued, LegControlQueueErrorKind, LegOutboundQueue, LegOutboundQueueErrorKind,
    PreparedClientRecovery, StatusRetryAttachEnqueued,
};
use super::supervisor::SessionSupervisor;
use super::two_leg::{SimTime, SimTimeError};
use crate::resumable::{
    AttachAuthority, AttachCredentials, AttachNonce, AttachRequest, AttachTransportBinding,
    AuthenticatedStandbyRegistration, Direction, FeatureOffer, FeatureSet, Frame, LegControlFrame,
    LegControlRecord, LegEpochNonce, LegGeneration, ProbeSequence, STANDBY_CONTROL_V1, SessionId,
    StandbyNonce, StandbyRegistrationReject, SwitchHintCause, VersionRange,
};
use std::fmt;
use std::sync::{Arc, Weak};
use std::time::Duration;
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

/// Response-validation gate minted only while consuming a typed B/C request
/// receipt. Each caller separately requires either local delivery or the
/// stronger causal evidence of an exact authenticated acceptance.
pub(super) struct RegisteredAttachResponseGate {
    _private: (),
}

impl RegisteredAttachResponseGate {
    fn typed_request() -> Self {
        Self { _private: () }
    }
}

/// One-shot authority minted only after the exact authenticated registered
/// ATTACH response validates as accepted. The corresponding response could
/// not exist unless the peer had received this exact signed request.
pub(super) struct RegisteredAttachCausalDeliveryGate {
    _private: (),
}

impl RegisteredAttachCausalDeliveryGate {
    fn exact_accepted_response() -> Self {
        Self { _private: () }
    }
}

/// One-shot proof that only the opaque registered accepted wrapper may turn
/// an uninstalled response into stale-status retry authority.
pub(super) struct AcceptedRegisteredLossGate {
    _private: (),
}

impl AcceptedRegisteredLossGate {
    fn uninstalled() -> Self {
        Self { _private: () }
    }
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub(super) enum AcceptedClientInstallErrorKind {
    #[error("accepted client endpoint was lost before installation")]
    EndpointLost,
    #[error("accepted client terminal transition lock was poisoned")]
    TerminalTransitionPoisoned,
    #[error("client supervisor rejected the replacement before commit")]
    SupervisorRejected,
    #[error("client recovery manifest storage remains retryable")]
    RecoveryPlanRetryable,
    #[error("client recovery manifest violated the bounded recovery contract")]
    RecoveryPlanInvariant,
    #[error("client recovery reservation remains retryable")]
    RecoveryReservationRetryable,
    #[error("client recovery reservation observed terminal standby transport")]
    RecoveryReservationTerminal,
    #[error("client recovery reservation violated the standby transport contract")]
    RecoveryReservationInvariant,
}

enum ClientInstallPrepareError {
    Plan(ClientRecoveryPlanError),
    Reservation(ClientRecoveryReserveError),
}

fn classify_client_recovery_plan_error(
    error: &ClientRecoveryPlanError,
) -> AcceptedClientInstallErrorKind {
    match error {
        ClientRecoveryPlanError::StorageUnavailable => {
            AcceptedClientInstallErrorKind::RecoveryPlanRetryable
        }
        ClientRecoveryPlanError::EffectBound { .. }
        | ClientRecoveryPlanError::MissingExactActivation
        | ClientRecoveryPlanError::WrongGeneration
        | ClientRecoveryPlanError::UnexpectedEffect
        | ClientRecoveryPlanError::InvalidFrame(_)
        | ClientRecoveryPlanError::CounterOverflow
        | ClientRecoveryPlanError::ResetDeliveryRebind(_) => {
            AcceptedClientInstallErrorKind::RecoveryPlanInvariant
        }
    }
}

fn classify_client_recovery_reservation_error(
    error: &ClientRecoveryReserveError,
) -> AcceptedClientInstallErrorKind {
    match error {
        ClientRecoveryReserveError::RequestNotDelivered
        | ClientRecoveryReserveError::StorageUnavailable => {
            AcceptedClientInstallErrorKind::RecoveryReservationRetryable
        }
        ClientRecoveryReserveError::EndpointOrQueueLost => {
            AcceptedClientInstallErrorKind::RecoveryReservationTerminal
        }
        ClientRecoveryReserveError::WrongQueue
        | ClientRecoveryReserveError::WrongGeneration
        | ClientRecoveryReserveError::WrongEndpoint
        | ClientRecoveryReserveError::NotAwaitingReplacement
        | ClientRecoveryReserveError::FrameCannotFit { .. }
        | ClientRecoveryReserveError::EndpointFrameCannotFit { .. }
        | ClientRecoveryReserveError::EndpointMessageBudgetExceeded
        | ClientRecoveryReserveError::EndpointByteBudgetExceeded
        | ClientRecoveryReserveError::CounterOverflow
        | ClientRecoveryReserveError::OrdinalExhausted => {
            AcceptedClientInstallErrorKind::RecoveryReservationInvariant
        }
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

    fn probe_frame(
        self,
        probe_sequence: ProbeSequence,
    ) -> Result<LegControlFrame, crate::resumable::ProtocolError> {
        LegControlFrame::try_new(
            self.generation,
            LegControlRecord::LegProbe {
                session_id: self.session_id,
                leg_epoch_nonce: LegEpochNonce::from(self.nonce),
                probe_sequence,
            },
            self.features,
        )
    }

    fn matches_probe_ack(self, frame: &LegControlFrame, probe_sequence: ProbeSequence) -> bool {
        frame.leg_generation() == self.generation
            && matches!(
                frame.record(),
                LegControlRecord::LegProbeAck {
                    session_id,
                    leg_epoch_nonce,
                    probe_sequence: received_sequence,
                } if *session_id == self.session_id
                    && *leg_epoch_nonce == LegEpochNonce::from(self.nonce)
                    && *received_sequence == probe_sequence
            )
    }
}

#[derive(Clone, Copy)]
struct ExactLegProbeContract {
    session_id: SessionId,
    generation: LegGeneration,
    epoch_nonce: LegEpochNonce,
    features: FeatureSet,
}

impl ExactLegProbeContract {
    fn from_registered(contract: StandbyContract) -> Self {
        Self {
            session_id: contract.session_id,
            generation: contract.generation,
            epoch_nonce: LegEpochNonce::from(contract.nonce),
            features: contract.features,
        }
    }

    fn matching_probe_sequence(self, frame: &LegControlFrame) -> Option<ProbeSequence> {
        if frame.leg_generation() != self.generation {
            return None;
        }
        match frame.record() {
            LegControlRecord::LegProbe {
                session_id,
                leg_epoch_nonce,
                probe_sequence,
            } if *session_id == self.session_id && *leg_epoch_nonce == self.epoch_nonce => {
                Some(*probe_sequence)
            }
            _ => None,
        }
    }

    fn probe_ack_frame(
        self,
        probe_sequence: ProbeSequence,
    ) -> Result<LegControlFrame, crate::resumable::ProtocolError> {
        LegControlFrame::try_new(
            self.generation,
            LegControlRecord::LegProbeAck {
                session_id: self.session_id,
                leg_epoch_nonce: self.epoch_nonce,
                probe_sequence,
            },
            self.features,
        )
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

/// Authenticated, exact-leg authority shared by the client and owner roles for
/// replying to peer liveness probes. It can be minted only from an
/// authenticated installed-current or registered-standby source and carries
/// no freshness authority.
pub(super) struct ExactLegProbeResponder {
    seal: LegSeal,
    endpoint: Weak<LegTransportEndpoint>,
    contract: ExactLegProbeContract,
}

impl ExactLegProbeResponder {
    fn from_registered_parts(
        seal: &LegSeal,
        endpoint: &Weak<LegTransportEndpoint>,
        contract: StandbyContract,
    ) -> Self {
        Self {
            seal: seal.share(),
            endpoint: endpoint.clone(),
            contract: ExactLegProbeContract::from_registered(contract),
        }
    }

    pub(super) fn from_installed(source: InstalledLegProbeSource) -> Self {
        let (seal, endpoint, session_id, generation, epoch_nonce, features) = source.into_parts();
        Self {
            seal,
            endpoint,
            contract: ExactLegProbeContract {
                session_id,
                generation,
                epoch_nonce,
                features,
            },
        }
    }

    /// Classifies only an exact-contract `LEG_PROBE`. Every other classified
    /// inbound fact is returned unchanged for the existing protocol path.
    pub(super) fn classify(&self, inbound: BoundInbound) -> ExactLegProbeResponse {
        let probe_sequence = match &inbound {
            BoundInbound::LegControl(bound) => {
                let (received_seal, frame) = bound.parts();
                if !self.seal.same_connection(received_seal) {
                    None
                } else {
                    self.contract.matching_probe_sequence(frame)
                }
            }
            BoundInbound::Session(_) => None,
        };
        let Some(probe_sequence) = probe_sequence else {
            return ExactLegProbeResponse::Unhandled(inbound);
        };
        let Ok(frame) = self.contract.probe_ack_frame(probe_sequence) else {
            return ExactLegProbeResponse::Unhandled(inbound);
        };
        ExactLegProbeResponse::Pending(PendingExactLegProbeAck {
            inbound,
            endpoint: self.endpoint.clone(),
            contract: self.contract,
            probe_sequence,
            frame,
        })
    }
}

impl fmt::Debug for ExactLegProbeResponder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ExactLegProbeResponder([REDACTED])")
    }
}

/// Exact input-preserving classification result. `Unhandled` retains the
/// original namespace/provenance value; `Pending` retains that same inbound
/// until the derived ACK enters the exact leg FIFO.
pub(super) enum ExactLegProbeResponse {
    Pending(PendingExactLegProbeAck),
    Unhandled(BoundInbound),
}

/// One exact classified probe plus its contract-derived ACK. Queue pressure,
/// endpoint loss, and wrong-queue attempts return this value intact.
pub(super) struct PendingExactLegProbeAck {
    inbound: BoundInbound,
    endpoint: Weak<LegTransportEndpoint>,
    contract: ExactLegProbeContract,
    probe_sequence: ProbeSequence,
    frame: LegControlFrame,
}

impl PendingExactLegProbeAck {
    pub(super) const fn probe_sequence(&self) -> ProbeSequence {
        self.probe_sequence
    }

    #[allow(
        clippy::result_large_err,
        reason = "probe ACK pressure must return the exact classified inbound and derived ACK"
    )]
    pub(super) fn enqueue(
        self,
        queue: &mut LegOutboundQueue,
    ) -> Result<ExactLegProbeAckEnqueued, ExactLegProbeAckEnqueueFailure> {
        if !endpoint_is_open(&self.endpoint) {
            return Err(ExactLegProbeAckEnqueueFailure::new(
                self,
                LegControlQueueErrorKind::EndpointLost,
            ));
        }
        let Self {
            inbound,
            endpoint,
            contract,
            probe_sequence,
            frame,
        } = self;
        let seal = match &inbound {
            BoundInbound::LegControl(bound) => bound.parts().0,
            BoundInbound::Session(_) => {
                return Err(ExactLegProbeAckEnqueueFailure::new(
                    Self {
                        inbound,
                        endpoint,
                        contract,
                        probe_sequence,
                        frame,
                    },
                    LegControlQueueErrorKind::WrongLeg,
                ));
            }
        };
        match queue.push_leg_control(seal, frame, contract.features) {
            Ok(_receipt) => Ok(ExactLegProbeAckEnqueued { probe_sequence }),
            Err(error) => {
                let kind = error.kind().clone();
                Err(ExactLegProbeAckEnqueueFailure::new(
                    Self {
                        inbound,
                        endpoint,
                        contract,
                        probe_sequence,
                        frame: error.into_frame(),
                    },
                    kind,
                ))
            }
        }
    }
}

impl fmt::Debug for PendingExactLegProbeAck {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PendingExactLegProbeAck([REDACTED])")
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct ExactLegProbeAckEnqueued {
    probe_sequence: ProbeSequence,
}

impl ExactLegProbeAckEnqueued {
    pub(super) const fn probe_sequence(self) -> ProbeSequence {
        self.probe_sequence
    }
}

pub(super) struct ExactLegProbeAckEnqueueFailure {
    pending: PendingExactLegProbeAck,
    kind: LegControlQueueErrorKind,
}

impl ExactLegProbeAckEnqueueFailure {
    fn new(pending: PendingExactLegProbeAck, kind: LegControlQueueErrorKind) -> Self {
        Self { pending, kind }
    }

    pub(super) const fn kind(&self) -> &LegControlQueueErrorKind {
        &self.kind
    }

    pub(super) fn into_pending(self) -> PendingExactLegProbeAck {
        self.pending
    }
}

impl fmt::Debug for ExactLegProbeAckEnqueueFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExactLegProbeAckEnqueueFailure")
            .field("kind", &self.kind)
            .field("pending", &"[REDACTED]")
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
    pub(super) const fn installed_features(&self) -> FeatureSet {
        self.contract.features
    }

    pub(super) fn shutdown_pending_frame_bytes(
        &self,
    ) -> Result<usize, crate::resumable::ProtocolError> {
        self.frame
            .encode(self.contract.features)
            .map(|encoded| encoded.len())
    }

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

    /// Controller-owned form of standby registration. It preserves the same
    /// exact candidate checks while using the installed client authority
    /// without re-exposing a bare `AttachedLeg` from the controller.
    pub(super) fn begin_from_client_active(
        active: &ClientActiveLeg,
        candidate: &EstablishedLeg,
        nonce: StandbyNonce,
        credentials: &AttachCredentials,
    ) -> Result<Self, StandbyRegistrationError> {
        let seal = candidate.standby_seal();
        let binding = candidate.standby_transport_binding();
        if active.belongs_to_transport(candidate)
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
    pub(super) fn shutdown_retry_frame_bytes(
        &self,
    ) -> Result<usize, crate::resumable::ProtocolError> {
        self.retry_frame
            .encode(self.contract.features)
            .map(|encoded| encoded.len())
    }

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

    pub(super) const fn installed_features(&self) -> FeatureSet {
        self.contract.features
    }

    pub(super) fn belongs_to_actor_parts(
        &self,
        leg: &EstablishedLeg,
        queue: &LegOutboundQueue,
    ) -> bool {
        self.is_live()
            && leg.belongs_to_transport(&self.seal)
            && self.receipt.belongs_to_queue(queue)
    }

    /// Mints the client-role responder from the installed authenticated B
    /// contract. The responder can acknowledge probes but cannot mint probe
    /// freshness or consume the registered standby owner.
    pub(super) fn exact_leg_probe_responder(&self) -> ExactLegProbeResponder {
        ExactLegProbeResponder::from_registered_parts(&self.seal, &self.endpoint, self.contract)
    }

    /// Consumes the exact registered B owner into one exact probe submission.
    /// Registration alone remains unprobed and cannot mint freshness.
    #[allow(
        clippy::result_large_err,
        reason = "probe construction failure returns the exact non-cloneable registered standby"
    )]
    pub(super) fn begin_exact_probe(
        self,
        probe_sequence: ProbeSequence,
    ) -> Result<PendingClientRegisteredStandbyProbe, RegisteredStandbyProbeBeginFailure> {
        if !self.is_live() {
            return Err(RegisteredStandbyProbeBeginFailure::new(
                self,
                RegisteredStandbyProbeBeginErrorKind::EndpointOrQueueLost,
            ));
        }
        let frame = match self.contract.probe_frame(probe_sequence) {
            Ok(frame) => frame,
            Err(_) => {
                return Err(RegisteredStandbyProbeBeginFailure::new(
                    self,
                    RegisteredStandbyProbeBeginErrorKind::Rejected,
                ));
            }
        };
        Ok(PendingClientRegisteredStandbyProbe {
            registered: self,
            probe_sequence,
            frame,
        })
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
pub(crate) enum RegisteredStandbyProbeBeginErrorKind {
    #[error("registered standby endpoint or sole queue was lost before probe construction")]
    EndpointOrQueueLost,
    #[error("registered standby probe construction was rejected")]
    Rejected,
}

/// Construction failure retaining the exact unprobed registered B owner.
pub(super) struct RegisteredStandbyProbeBeginFailure {
    registered: ClientRegisteredStandby,
    kind: RegisteredStandbyProbeBeginErrorKind,
}

impl RegisteredStandbyProbeBeginFailure {
    fn new(
        registered: ClientRegisteredStandby,
        kind: RegisteredStandbyProbeBeginErrorKind,
    ) -> Self {
        Self { registered, kind }
    }

    pub(super) const fn kind(&self) -> RegisteredStandbyProbeBeginErrorKind {
        self.kind
    }

    pub(super) fn into_registered(self) -> ClientRegisteredStandby {
        self.registered
    }
}

impl fmt::Debug for RegisteredStandbyProbeBeginFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RegisteredStandbyProbeBeginFailure")
            .field("kind", &self.kind)
            .field("registered", &"[REDACTED]")
            .finish()
    }
}

/// Exact registered B plus one unsubmitted `LEG_PROBE`. The frame and B owner
/// remain together until the exact B queue accepts the record.
pub(super) struct PendingClientRegisteredStandbyProbe {
    registered: ClientRegisteredStandby,
    probe_sequence: ProbeSequence,
    frame: LegControlFrame,
}

impl PendingClientRegisteredStandbyProbe {
    pub(super) fn shutdown_pending_frame_bytes(
        &self,
    ) -> Result<usize, crate::resumable::ProtocolError> {
        self.frame
            .encode(self.registered.contract.features)
            .map(|encoded| encoded.len())
    }

    #[allow(
        clippy::result_large_err,
        reason = "bounded probe pressure returns the exact pending probe and registered B owner"
    )]
    pub(super) fn enqueue(
        mut self,
        queue: &mut LegOutboundQueue,
    ) -> Result<AwaitingClientRegisteredStandbyProbeAck, RegisteredStandbyProbeEnqueueFailure> {
        if !self.registered.is_live() {
            return Err(RegisteredStandbyProbeEnqueueFailure::new(
                self,
                LegControlQueueErrorKind::EndpointLost,
            ));
        }
        let features = self.registered.contract.features;
        let seal = self.registered.seal.share();
        match queue.push_leg_control(&seal, self.frame, features) {
            Ok(receipt) => Ok(AwaitingClientRegisteredStandbyProbeAck {
                registered: self.registered,
                probe_sequence: self.probe_sequence,
                receipt,
            }),
            Err(error) => {
                let kind = error.kind().clone();
                self.frame = error.into_frame();
                Err(RegisteredStandbyProbeEnqueueFailure::new(self, kind))
            }
        }
    }

    pub(super) fn into_registered(self) -> ClientRegisteredStandby {
        self.registered
    }
}

impl fmt::Debug for PendingClientRegisteredStandbyProbe {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PendingClientRegisteredStandbyProbe([REDACTED])")
    }
}

/// Retryable probe enqueue failure retaining the exact frame and registered B.
pub(super) struct RegisteredStandbyProbeEnqueueFailure {
    pending: PendingClientRegisteredStandbyProbe,
    kind: LegControlQueueErrorKind,
}

impl RegisteredStandbyProbeEnqueueFailure {
    fn new(pending: PendingClientRegisteredStandbyProbe, kind: LegControlQueueErrorKind) -> Self {
        Self { pending, kind }
    }

    pub(super) const fn kind(&self) -> &LegControlQueueErrorKind {
        &self.kind
    }

    pub(super) fn into_pending(self) -> PendingClientRegisteredStandbyProbe {
        self.pending
    }
}

impl fmt::Debug for RegisteredStandbyProbeEnqueueFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RegisteredStandbyProbeEnqueueFailure")
            .field("kind", &self.kind)
            .field("pending", &"[REDACTED]")
            .finish()
    }
}

/// One physically queued B probe awaiting the exact-seal, exact-correlation
/// `LEG_PROBE_ACK`. A matching peer ACK is itself causal proof that this exact
/// probe crossed the writer even if its local DELIVERED notice is delayed.
pub(super) struct AwaitingClientRegisteredStandbyProbeAck {
    registered: ClientRegisteredStandby,
    probe_sequence: ProbeSequence,
    receipt: LegControlEnqueued,
}

impl AwaitingClientRegisteredStandbyProbeAck {
    /// Arms the timeout only after the exact B writer has positively
    /// completed this probe. Queue admission alone cannot start the clock.
    #[allow(
        clippy::result_large_err,
        reason = "arm rejection returns the exact awaiting B probe owner"
    )]
    pub(super) fn arm_after_delivery(
        self,
        now: SimTime,
        timeout: Duration,
    ) -> Result<ArmedClientRegisteredStandbyProbe, RegisteredStandbyProbeArmFailure> {
        let kind = if timeout.is_zero() {
            Some(RegisteredStandbyProbeArmErrorKind::InvalidTimeout)
        } else if !self.receipt.queue_is_live() || !self.registered.is_live() {
            Some(RegisteredStandbyProbeArmErrorKind::QueueOrEndpointLost)
        } else if !self.receipt.was_delivered() {
            Some(RegisteredStandbyProbeArmErrorKind::NotDelivered)
        } else {
            None
        };
        if let Some(kind) = kind {
            return Err(RegisteredStandbyProbeArmFailure::new(self, kind));
        }
        let deadline = match now.checked_add(timeout) {
            Ok(deadline) => deadline,
            Err(error) => {
                return Err(RegisteredStandbyProbeArmFailure::new(
                    self,
                    RegisteredStandbyProbeArmErrorKind::Deadline(error),
                ));
            }
        };
        Ok(ArmedClientRegisteredStandbyProbe {
            awaiting: self,
            deadline,
        })
    }

    #[allow(
        clippy::result_large_err,
        reason = "probe ACK rejection returns the exact awaiting B owner and inbound fact"
    )]
    pub(super) fn validate_exact_ack(
        self,
        inbound: BoundInbound,
        acknowledged_at: SimTime,
        fresh_until: SimTime,
    ) -> Result<FreshClientRegisteredStandby, RegisteredStandbyProbeAckFailure> {
        let kind = if !self.receipt.queue_is_live() || !self.registered.is_live() {
            Some(RegisteredStandbyProbeAckErrorKind::QueueOrEndpointLost)
        } else {
            match &inbound {
                BoundInbound::LegControl(bound) => {
                    let (received_seal, frame) = bound.parts();
                    (!self.registered.seal.same_connection(received_seal)
                        || !self
                            .registered
                            .contract
                            .matches_probe_ack(frame, self.probe_sequence))
                    .then_some(RegisteredStandbyProbeAckErrorKind::Rejected)
                }
                BoundInbound::Session(_) => Some(RegisteredStandbyProbeAckErrorKind::Rejected),
            }
        };
        if let Some(kind) = kind {
            return Err(RegisteredStandbyProbeAckFailure::new(self, inbound, kind));
        }
        Ok(FreshClientRegisteredStandby {
            registered: self.registered,
            probe_sequence: self.probe_sequence,
            probe_receipt: self.receipt,
            acknowledged_at,
            fresh_until,
        })
    }
}

impl fmt::Debug for AwaitingClientRegisteredStandbyProbeAck {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AwaitingClientRegisteredStandbyProbeAck([REDACTED])")
    }
}

/// Exact B probe whose timeout began only after positive writer completion.
/// The non-cloneable wrapper is the sole expiry authority.
pub(super) struct ArmedClientRegisteredStandbyProbe {
    awaiting: AwaitingClientRegisteredStandbyProbeAck,
    deadline: SimTime,
}

impl ArmedClientRegisteredStandbyProbe {
    pub(super) const fn deadline(&self) -> SimTime {
        self.deadline
    }

    #[allow(
        clippy::result_large_err,
        reason = "early expiry returns the exact armed B probe authority"
    )]
    pub(super) fn expire_if_due(self, now: SimTime) -> Result<ClientRegisteredStandby, Self> {
        if now < self.deadline {
            return Err(self);
        }
        Ok(self.awaiting.registered)
    }

    #[allow(
        clippy::result_large_err,
        reason = "probe ACK rejection returns the exact armed B owner and inbound fact"
    )]
    pub(super) fn validate_exact_ack(
        self,
        inbound: BoundInbound,
        received_at: SimTime,
        fresh_until: SimTime,
    ) -> Result<FreshClientRegisteredStandby, RegisteredStandbyArmedProbeAckFailure> {
        let deadline = self.deadline;
        if !self.awaiting.receipt.queue_is_live() || !self.awaiting.registered.is_live() {
            return Err(RegisteredStandbyArmedProbeAckFailure {
                failure: RegisteredStandbyProbeAckFailure::new(
                    self.awaiting,
                    inbound,
                    RegisteredStandbyProbeAckErrorKind::QueueOrEndpointLost,
                ),
                deadline,
            });
        }
        if received_at > deadline {
            return Err(RegisteredStandbyArmedProbeAckFailure {
                failure: RegisteredStandbyProbeAckFailure::new(
                    self.awaiting,
                    inbound,
                    RegisteredStandbyProbeAckErrorKind::DeadlineExpired,
                ),
                deadline,
            });
        }
        self.awaiting
            .validate_exact_ack(inbound, received_at, fresh_until)
            .map_err(|failure| RegisteredStandbyArmedProbeAckFailure { failure, deadline })
    }
}

impl fmt::Debug for ArmedClientRegisteredStandbyProbe {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ArmedClientRegisteredStandbyProbe([REDACTED])")
    }
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub(crate) enum RegisteredStandbyProbeArmErrorKind {
    #[error("registered standby probe timeout must be nonzero")]
    InvalidTimeout,
    #[error("registered standby probe queue or endpoint was lost before arming")]
    QueueOrEndpointLost,
    #[error("registered standby probe writer has not completed exact delivery")]
    NotDelivered,
    #[error("registered standby probe deadline calculation failed: {0}")]
    Deadline(SimTimeError),
}

pub(super) struct RegisteredStandbyProbeArmFailure {
    awaiting: AwaitingClientRegisteredStandbyProbeAck,
    kind: RegisteredStandbyProbeArmErrorKind,
}

impl RegisteredStandbyProbeArmFailure {
    fn new(
        awaiting: AwaitingClientRegisteredStandbyProbeAck,
        kind: RegisteredStandbyProbeArmErrorKind,
    ) -> Self {
        Self { awaiting, kind }
    }

    pub(super) const fn kind(&self) -> RegisteredStandbyProbeArmErrorKind {
        self.kind
    }

    pub(super) fn into_awaiting(self) -> AwaitingClientRegisteredStandbyProbeAck {
        self.awaiting
    }
}

impl fmt::Debug for RegisteredStandbyProbeArmFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RegisteredStandbyProbeArmFailure")
            .field("kind", &self.kind)
            .field("awaiting", &"[REDACTED]")
            .finish()
    }
}

pub(super) struct RegisteredStandbyArmedProbeAckFailure {
    failure: RegisteredStandbyProbeAckFailure,
    deadline: SimTime,
}

impl RegisteredStandbyArmedProbeAckFailure {
    pub(super) const fn kind(&self) -> RegisteredStandbyProbeAckErrorKind {
        self.failure.kind()
    }

    pub(super) fn into_parts(self) -> (ArmedClientRegisteredStandbyProbe, BoundInbound) {
        let (awaiting, inbound) = self.failure.into_parts();
        (
            ArmedClientRegisteredStandbyProbe {
                awaiting,
                deadline: self.deadline,
            },
            inbound,
        )
    }
}

impl fmt::Debug for RegisteredStandbyArmedProbeAckFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RegisteredStandbyArmedProbeAckFailure")
            .field("kind", &self.kind())
            .field("ownership", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub(crate) enum RegisteredStandbyProbeAckErrorKind {
    #[error("registered standby probe queue or endpoint was lost")]
    QueueOrEndpointLost,
    #[error("registered standby probe ACK arrived after the exact armed deadline")]
    DeadlineExpired,
    #[error("registered standby probe ACK did not match the exact B probe")]
    Rejected,
}

/// ACK validation failure retaining both the exact B probe state and the
/// classified inbound fact so the composition can route or discard it.
pub(super) struct RegisteredStandbyProbeAckFailure {
    awaiting: AwaitingClientRegisteredStandbyProbeAck,
    inbound: BoundInbound,
    kind: RegisteredStandbyProbeAckErrorKind,
}

impl RegisteredStandbyProbeAckFailure {
    fn new(
        awaiting: AwaitingClientRegisteredStandbyProbeAck,
        inbound: BoundInbound,
        kind: RegisteredStandbyProbeAckErrorKind,
    ) -> Self {
        Self {
            awaiting,
            inbound,
            kind,
        }
    }

    pub(super) const fn kind(&self) -> RegisteredStandbyProbeAckErrorKind {
        self.kind
    }

    pub(super) fn into_parts(self) -> (AwaitingClientRegisteredStandbyProbeAck, BoundInbound) {
        (self.awaiting, self.inbound)
    }
}

impl fmt::Debug for RegisteredStandbyProbeAckFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RegisteredStandbyProbeAckFailure")
            .field("kind", &self.kind)
            .field("awaiting", &"[REDACTED]")
            .field("inbound", &"[REDACTED]")
            .finish()
    }
}

/// Non-cloneable proof that exact registered B recently completed one
/// bidirectional wire probe. The time is observation only; this wrapper is
/// the authority required by the silent-loss attach entry.
pub(super) struct FreshClientRegisteredStandby {
    registered: ClientRegisteredStandby,
    probe_sequence: ProbeSequence,
    probe_receipt: LegControlEnqueued,
    acknowledged_at: SimTime,
    fresh_until: SimTime,
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub(super) enum FreshStandbySwitchHintEvidenceErrorKind {
    #[error("the held SWITCH_HINT did not arrive through the exact registered B actor")]
    WrongActor,
    #[error("standby control was not negotiated for the exact registered B")]
    FeatureNotNegotiated,
    #[error("the held SWITCH_HINT names the wrong leg generation")]
    WrongGeneration,
    #[error("the held B record is not a SWITCH_HINT")]
    WrongRecordKind,
    #[error("the held SWITCH_HINT names the wrong session")]
    WrongSession,
    #[error("the held SWITCH_HINT names the wrong standby nonce")]
    WrongStandbyNonce,
    #[error("the exact registered B probe was not fresh at actor arrival")]
    Stale,
    #[error("the exact registered B endpoint or receipt is no longer live")]
    EndpointOrQueueLost,
}

impl FreshClientRegisteredStandby {
    pub(super) const fn fresh_until(&self) -> SimTime {
        self.fresh_until
    }

    pub(super) fn is_fresh_at(&self, now: SimTime) -> bool {
        now >= self.acknowledged_at
            && now <= self.fresh_until
            && self.registered.is_live()
            && self.probe_receipt.queue_is_live()
    }

    /// Borrow-only physical evidence for one actor-held B `SWITCH_HINT`.
    /// This validates the connection seal and authenticated registration
    /// contract before the composition may expose any semantic hint fields.
    /// Freshness is checked at the immutable actor arrival timestamp, not at
    /// the later root-service time.
    pub(super) fn validate_exact_switch_hint(
        &self,
        inbound: &LegBoundControlFrame,
        arrived_at: SimTime,
        expected_generation: LegGeneration,
        expected_features: FeatureSet,
    ) -> Result<(), FreshStandbySwitchHintEvidenceErrorKind> {
        let (received_seal, frame) = inbound.parts();
        if !self.registered.seal.same_connection(received_seal) {
            return Err(FreshStandbySwitchHintEvidenceErrorKind::WrongActor);
        }
        if expected_features != self.registered.contract.features
            || !expected_features.contains(STANDBY_CONTROL_V1)
        {
            return Err(FreshStandbySwitchHintEvidenceErrorKind::FeatureNotNegotiated);
        }
        if frame.leg_generation() != expected_generation
            || frame.leg_generation() != self.registered.contract.generation
        {
            return Err(FreshStandbySwitchHintEvidenceErrorKind::WrongGeneration);
        }
        let LegControlRecord::SwitchHint {
            session_id,
            standby_nonce,
            ..
        } = frame.record()
        else {
            return Err(FreshStandbySwitchHintEvidenceErrorKind::WrongRecordKind);
        };
        if *session_id != self.registered.contract.session_id {
            return Err(FreshStandbySwitchHintEvidenceErrorKind::WrongSession);
        }
        if *standby_nonce != self.registered.contract.nonce {
            return Err(FreshStandbySwitchHintEvidenceErrorKind::WrongStandbyNonce);
        }
        if arrived_at < self.acknowledged_at || arrived_at > self.fresh_until {
            return Err(FreshStandbySwitchHintEvidenceErrorKind::Stale);
        }
        if !self.registered.is_live() || !self.probe_receipt.queue_is_live() {
            return Err(FreshStandbySwitchHintEvidenceErrorKind::EndpointOrQueueLost);
        }
        Ok(())
    }

    /// Silent-loss-only attach entry. The private fresh wrapper is consumed;
    /// a timestamp or registration owner by itself cannot call this method.
    #[allow(
        clippy::result_large_err,
        reason = "silent attach rejection returns the exact fresh B owner"
    )]
    pub(super) fn begin_exact_next_attach_for_silent_loss(
        self,
        now: SimTime,
        nonce: AttachNonce,
        credentials: &AttachCredentials,
    ) -> Result<PendingRegisteredAttach, FreshRegisteredAttachBeginFailure> {
        if now > self.fresh_until {
            return Err(FreshRegisteredAttachBeginFailure::new(
                self,
                FreshRegisteredAttachBeginErrorKind::Stale,
            ));
        }
        if !self.is_fresh_at(now) {
            return Err(FreshRegisteredAttachBeginFailure::new(
                self,
                FreshRegisteredAttachBeginErrorKind::EndpointOrQueueLost,
            ));
        }
        let Self {
            registered,
            probe_sequence,
            probe_receipt,
            acknowledged_at,
            fresh_until,
        } = self;
        match registered.begin_exact_next_attach(nonce, credentials) {
            Ok(pending) => Ok(pending),
            Err(error) => {
                let kind = FreshRegisteredAttachBeginErrorKind::Registered(error.kind());
                Err(FreshRegisteredAttachBeginFailure::new(
                    Self {
                        registered: error.into_registered(),
                        probe_sequence,
                        probe_receipt,
                        acknowledged_at,
                        fresh_until,
                    },
                    kind,
                ))
            }
        }
    }

    pub(super) fn into_registered(self) -> ClientRegisteredStandby {
        self.registered
    }
}

impl fmt::Debug for FreshClientRegisteredStandby {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let _redacted_fields = (self.probe_sequence, self.acknowledged_at, self.fresh_until);
        formatter.write_str("FreshClientRegisteredStandby([REDACTED])")
    }
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub(crate) enum FreshRegisteredAttachBeginErrorKind {
    #[error("registered standby probe freshness expired")]
    Stale,
    #[error("fresh registered standby endpoint or queue was lost")]
    EndpointOrQueueLost,
    #[error("fresh registered standby ordinary attach derivation failed: {0}")]
    Registered(RegisteredAttachBeginErrorKind),
}

/// Silent attach rejection retaining the exact fresh B owner.
pub(super) struct FreshRegisteredAttachBeginFailure {
    fresh: FreshClientRegisteredStandby,
    kind: FreshRegisteredAttachBeginErrorKind,
}

impl FreshRegisteredAttachBeginFailure {
    fn new(fresh: FreshClientRegisteredStandby, kind: FreshRegisteredAttachBeginErrorKind) -> Self {
        Self { fresh, kind }
    }

    pub(super) const fn kind(&self) -> FreshRegisteredAttachBeginErrorKind {
        self.kind
    }

    pub(super) fn into_fresh(self) -> FreshClientRegisteredStandby {
        self.fresh
    }
}

impl fmt::Debug for FreshRegisteredAttachBeginFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FreshRegisteredAttachBeginFailure")
            .field("kind", &self.kind)
            .field("fresh", &"[REDACTED]")
            .finish()
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
        // An authenticated response bound to this exact request is causal
        // proof that the peer received it. Do not lose a valid response only
        // because the redundant local DELIVERED lane is scheduled later.
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
            RegisteredAttachResponseGate::typed_request(),
        ) {
            Ok(RegisteredPendingAttachResponse::Accepted(accepted)) => {
                let request_receipt = request_receipt.into_causally_delivered(
                    RegisteredAttachCausalDeliveryGate::exact_accepted_response(),
                );
                Ok(RegisteredAttachResponse::Accepted(
                    AcceptedClientRegisteredAttach {
                        accepted,
                        request_receipt,
                    },
                ))
            }
            Ok(RegisteredPendingAttachResponse::GenerationStatus(status)) => Ok(
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
            RegisteredAttachResponseGate::typed_request(),
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
    accepted: UninstalledRegisteredAttach,
    request_receipt: CausallyDeliveredAttachRequest,
}

/// Copy-only shutdown witness minted only after the exact uninstalled
/// acceptance and its causally-delivered request receipt are consumed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct AcceptedClientRegisteredAttachShutdownDiscard {
    generation: LegGeneration,
    accepted_authorities: usize,
    request_receipts: usize,
}

impl AcceptedClientRegisteredAttachShutdownDiscard {
    pub(crate) const fn generation(self) -> LegGeneration {
        self.generation
    }

    pub(crate) const fn accepted_authorities(self) -> usize {
        self.accepted_authorities
    }

    pub(crate) const fn request_receipts(self) -> usize {
        self.request_receipts
    }
}

impl AcceptedClientRegisteredAttach {
    pub(crate) fn generation(&self) -> LegGeneration {
        self.accepted.generation()
    }

    pub(crate) fn request_queue_is_live(&self) -> bool {
        self.request_receipt.queue_is_live()
    }

    /// Shutdown never installs this accepted leg. Consume both exact typed
    /// owners before publishing a countable discard witness.
    pub(super) fn discard_for_shutdown(self) -> AcceptedClientRegisteredAttachShutdownDiscard {
        let generation = self.accepted.generation();
        let Self {
            accepted,
            request_receipt,
        } = self;
        drop((accepted, request_receipt));
        AcceptedClientRegisteredAttachShutdownDiscard {
            generation,
            accepted_authorities: 1,
            request_receipts: 1,
        }
    }

    #[allow(
        clippy::result_large_err,
        reason = "install failure returns the exact accepted attach and transport ownership without boxing"
    )]
    pub(super) fn install_with_controller<P>(
        self,
        supervisor: &mut SessionSupervisor,
        transport: ClientLegTransport,
        effect_bound: usize,
        gate: ClientControllerInstallGate,
        publish: impl FnOnce(
            ClientActiveLeg,
            ClientLegTransport,
            PreparedClientRecovery,
            ClientRecoveryReservation,
        ) -> P,
    ) -> Result<P, AcceptedClientRegisteredInstallFailure> {
        self.install_with_controller_source(
            supervisor,
            transport,
            effect_bound,
            gate,
            (),
            |(), _| Ok(()),
            |(), active, transport, recovery, reservation| {
                publish(active, transport, recovery, reservation)
            },
        )
        .map_err(|(failure, ())| failure)
    }

    /// Source-carrying counterpart for the post-loss transaction. It keeps
    /// terminal-bound A beside accepted B until the reducer commit invokes
    /// `publish`; every earlier rejection returns both exact owners.
    #[allow(
        clippy::result_large_err,
        reason = "post-loss rejection returns accepted B, transport, and exact retired-A source"
    )]
    pub(super) fn install_with_controller_source<C, P>(
        self,
        supervisor: &mut SessionSupervisor,
        mut transport: ClientLegTransport,
        effect_bound: usize,
        gate: ClientControllerInstallGate,
        controller_source: C,
        prepare_controller_source: impl FnOnce(
            &C,
            &mut PreparedClientRecovery,
        ) -> Result<(), ClientRecoveryPlanError>,
        publish: impl FnOnce(
            C,
            ClientActiveLeg,
            ClientLegTransport,
            PreparedClientRecovery,
            ClientRecoveryReservation,
        ) -> P,
    ) -> Result<P, (AcceptedClientRegisteredInstallFailure, C)> {
        let Some(transition) = self.accepted.install_transition(&gate) else {
            return Err((
                AcceptedClientRegisteredInstallFailure::new(
                    self,
                    transport,
                    AcceptedClientInstallErrorKind::EndpointLost,
                ),
                controller_source,
            ));
        };
        let terminal_transition = match transition.lock() {
            Ok(guard) => guard,
            Err(_) => {
                return Err((
                    AcceptedClientRegisteredInstallFailure::new(
                        self,
                        transport,
                        AcceptedClientInstallErrorKind::TerminalTransitionPoisoned,
                    ),
                    controller_source,
                ));
            }
        };
        if !transition.endpoint_is_open() {
            return Err((
                AcceptedClientRegisteredInstallFailure::new(
                    self,
                    transport,
                    AcceptedClientInstallErrorKind::EndpointLost,
                ),
                controller_source,
            ));
        }
        let generation = self.accepted.generation();
        let Self {
            accepted,
            request_receipt,
        } = self;
        let transaction = accepted.commit_client_install(
            supervisor,
            |effects| {
                let mut recovery =
                    PreparedClientRecovery::preflight(effects, generation, effect_bound)
                        .map_err(ClientInstallPrepareError::Plan)?;
                prepare_controller_source(&controller_source, &mut recovery)
                    .map_err(ClientInstallPrepareError::Plan)?;
                let reservation = transport
                    .reserve_registered_client_recovery(&request_receipt, &recovery)
                    .map_err(ClientInstallPrepareError::Reservation)?;
                Ok((recovery, reservation))
            },
            |active, prepared| (active, prepared),
            &gate,
        );
        let result =
            match transaction {
                ClientInstallTransaction::Installed((active, (recovery, reservation))) => Ok(
                    publish(controller_source, active, transport, recovery, reservation),
                ),
                ClientInstallTransaction::PrepareRejected { accepted, error } => {
                    let kind = match error {
                        ClientInstallPrepareError::Plan(error) => {
                            classify_client_recovery_plan_error(&error)
                        }
                        ClientInstallPrepareError::Reservation(error) => {
                            classify_client_recovery_reservation_error(&error)
                        }
                    };
                    Err((
                        AcceptedClientRegisteredInstallFailure::new(
                            Self {
                                accepted,
                                request_receipt,
                            },
                            transport,
                            kind,
                        ),
                        controller_source,
                    ))
                }
                ClientInstallTransaction::SupervisorRejected { accepted, error } => {
                    drop(error);
                    Err((
                        AcceptedClientRegisteredInstallFailure::new(
                            Self {
                                accepted,
                                request_receipt,
                            },
                            transport,
                            AcceptedClientInstallErrorKind::SupervisorRejected,
                        ),
                        controller_source,
                    ))
                }
            };
        drop(terminal_transition);
        result
    }

    #[allow(
        clippy::result_large_err,
        reason = "wrong terminal returns exact accepted ownership and terminal fact"
    )]
    pub(crate) fn into_status_retry_after_terminal(
        self,
        terminal: ExactLegTerminal,
    ) -> Result<RegisteredAttachStatusRetryAuthority, AcceptedRegisteredAttachTerminalRetryFailure>
    {
        let Self {
            accepted,
            request_receipt,
        } = self;
        match accepted
            .into_status_retry_after_terminal(terminal, AcceptedRegisteredLossGate::uninstalled())
        {
            Ok(lost) => {
                drop(request_receipt);
                Ok(RegisteredAttachStatusRetryAuthority { lost })
            }
            Err(failure) => {
                let (accepted, terminal) = failure.into_parts();
                Err(AcceptedRegisteredAttachTerminalRetryFailure::new(
                    Self {
                        accepted,
                        request_receipt,
                    },
                    terminal,
                ))
            }
        }
    }

    #[allow(
        clippy::result_large_err,
        reason = "live queue returns exact accepted ownership"
    )]
    pub(crate) fn into_status_retry_after_queue_loss(
        self,
    ) -> Result<RegisteredAttachStatusRetryAuthority, AcceptedRegisteredAttachQueueLossRetryFailure>
    {
        let Self {
            accepted,
            request_receipt,
        } = self;
        match accepted.into_status_retry_after_queue_loss(AcceptedRegisteredLossGate::uninstalled())
        {
            Ok(lost) => {
                drop(request_receipt);
                Ok(RegisteredAttachStatusRetryAuthority { lost })
            }
            Err(accepted) => Err(AcceptedRegisteredAttachQueueLossRetryFailure::new(Self {
                accepted,
                request_receipt,
            })),
        }
    }
}

pub(super) struct AcceptedClientRegisteredInstallFailure {
    accepted: AcceptedClientRegisteredAttach,
    transport: ClientLegTransport,
    kind: AcceptedClientInstallErrorKind,
}

impl AcceptedClientRegisteredInstallFailure {
    fn new(
        accepted: AcceptedClientRegisteredAttach,
        transport: ClientLegTransport,
        kind: AcceptedClientInstallErrorKind,
    ) -> Self {
        Self {
            accepted,
            transport,
            kind,
        }
    }

    pub(super) const fn kind(&self) -> AcceptedClientInstallErrorKind {
        self.kind
    }

    pub(super) fn into_parts(self) -> (AcceptedClientRegisteredAttach, ClientLegTransport) {
        (self.accepted, self.transport)
    }
}

impl fmt::Debug for AcceptedClientRegisteredInstallFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AcceptedClientRegisteredInstallFailure")
            .field("kind", &self.kind)
            .field("accepted", &"[REDACTED]")
            .finish()
    }
}

pub(crate) struct AcceptedRegisteredAttachTerminalRetryFailure {
    accepted: AcceptedClientRegisteredAttach,
    terminal: ExactLegTerminal,
}

impl AcceptedRegisteredAttachTerminalRetryFailure {
    fn new(accepted: AcceptedClientRegisteredAttach, terminal: ExactLegTerminal) -> Self {
        Self { accepted, terminal }
    }

    pub(crate) const fn kind(&self) -> RegisteredAttachStatusRetryErrorKind {
        RegisteredAttachStatusRetryErrorKind::WrongTerminal
    }

    pub(crate) fn into_parts(self) -> (AcceptedClientRegisteredAttach, ExactLegTerminal) {
        (self.accepted, self.terminal)
    }
}

impl fmt::Debug for AcceptedRegisteredAttachTerminalRetryFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AcceptedRegisteredAttachTerminalRetryFailure")
            .field("kind", &self.kind())
            .field("ownership", &"[REDACTED]")
            .finish()
    }
}

pub(crate) struct AcceptedRegisteredAttachQueueLossRetryFailure {
    accepted: AcceptedClientRegisteredAttach,
}

impl AcceptedRegisteredAttachQueueLossRetryFailure {
    fn new(accepted: AcceptedClientRegisteredAttach) -> Self {
        Self { accepted }
    }

    pub(crate) const fn kind(&self) -> RegisteredAttachStatusRetryErrorKind {
        RegisteredAttachStatusRetryErrorKind::OriginalTransportStillLive
    }

    pub(crate) fn into_accepted(self) -> AcceptedClientRegisteredAttach {
        self.accepted
    }
}

impl fmt::Debug for AcceptedRegisteredAttachQueueLossRetryFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AcceptedRegisteredAttachQueueLossRetryFailure")
            .field("kind", &self.kind())
            .field("accepted", &"[REDACTED]")
            .finish()
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
            Ok(accepted) => Ok(AcceptedClientRegisteredCatchUp {
                accepted,
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
    accepted: UninstalledRegisteredCatchUp,
    status_request_receipt: AttachStatusRequestReceipt,
    request_receipt: CatchUpAttachEnqueued,
}

impl AcceptedClientRegisteredCatchUp {
    pub(crate) fn generation(&self) -> LegGeneration {
        self.accepted.generation()
    }

    pub(crate) fn follow_up_queue_is_live(&self) -> bool {
        self.request_receipt.queue_is_live()
    }

    #[allow(
        clippy::result_large_err,
        reason = "catch-up install failure returns the exact accepted attach, transport, and controller source without boxing"
    )]
    pub(super) fn install_with_controller<C, P>(
        self,
        supervisor: &mut SessionSupervisor,
        mut transport: ClientLegTransport,
        effect_bound: usize,
        gate: ClientControllerInstallGate,
        controller_source: C,
        publish: impl FnOnce(
            C,
            ClientActiveLeg,
            ClientLegTransport,
            PreparedClientRecovery,
            ClientRecoveryReservation,
        ) -> P,
    ) -> Result<P, (AcceptedClientRegisteredCatchUpInstallFailure, C)> {
        let Some(transition) = self.accepted.install_transition(&gate) else {
            return Err((
                AcceptedClientRegisteredCatchUpInstallFailure::new(
                    self,
                    transport,
                    AcceptedClientInstallErrorKind::EndpointLost,
                ),
                controller_source,
            ));
        };
        let terminal_transition = match transition.lock() {
            Ok(guard) => guard,
            Err(_) => {
                return Err((
                    AcceptedClientRegisteredCatchUpInstallFailure::new(
                        self,
                        transport,
                        AcceptedClientInstallErrorKind::TerminalTransitionPoisoned,
                    ),
                    controller_source,
                ));
            }
        };
        if !transition.endpoint_is_open() {
            return Err((
                AcceptedClientRegisteredCatchUpInstallFailure::new(
                    self,
                    transport,
                    AcceptedClientInstallErrorKind::EndpointLost,
                ),
                controller_source,
            ));
        }
        let generation = self.accepted.generation();
        let Self {
            accepted,
            status_request_receipt,
            request_receipt,
        } = self;
        let transaction = accepted.commit_client_install(
            supervisor,
            |effects| {
                let recovery = PreparedClientRecovery::preflight(effects, generation, effect_bound)
                    .map_err(ClientInstallPrepareError::Plan)?;
                let reservation = transport
                    .reserve_catch_up_client_recovery(&request_receipt, &recovery)
                    .map_err(ClientInstallPrepareError::Reservation)?;
                Ok((recovery, reservation))
            },
            |active, prepared| (active, prepared),
            &gate,
        );
        let result =
            match transaction {
                ClientInstallTransaction::Installed((active, (recovery, reservation))) => Ok(
                    publish(controller_source, active, transport, recovery, reservation),
                ),
                ClientInstallTransaction::PrepareRejected { accepted, error } => {
                    let kind = match error {
                        ClientInstallPrepareError::Plan(error) => {
                            classify_client_recovery_plan_error(&error)
                        }
                        ClientInstallPrepareError::Reservation(error) => {
                            classify_client_recovery_reservation_error(&error)
                        }
                    };
                    Err((
                        AcceptedClientRegisteredCatchUpInstallFailure::new(
                            Self {
                                accepted,
                                status_request_receipt,
                                request_receipt,
                            },
                            transport,
                            kind,
                        ),
                        controller_source,
                    ))
                }
                ClientInstallTransaction::SupervisorRejected { accepted, error } => {
                    drop(error);
                    Err((
                        AcceptedClientRegisteredCatchUpInstallFailure::new(
                            Self {
                                accepted,
                                status_request_receipt,
                                request_receipt,
                            },
                            transport,
                            AcceptedClientInstallErrorKind::SupervisorRejected,
                        ),
                        controller_source,
                    ))
                }
            };
        drop(terminal_transition);
        result
    }

    #[allow(
        clippy::result_large_err,
        reason = "wrong terminal returns exact accepted catch-up ownership and terminal fact"
    )]
    pub(crate) fn into_status_retry_after_terminal(
        self,
        terminal: ExactLegTerminal,
    ) -> Result<RegisteredAttachStatusRetryAuthority, AcceptedRegisteredCatchUpTerminalRetryFailure>
    {
        let Self {
            accepted,
            status_request_receipt,
            request_receipt,
        } = self;
        match accepted
            .into_status_retry_after_terminal(terminal, AcceptedRegisteredLossGate::uninstalled())
        {
            Ok(lost) => {
                drop((status_request_receipt, request_receipt));
                Ok(RegisteredAttachStatusRetryAuthority { lost })
            }
            Err(failure) => {
                let (accepted, terminal) = failure.into_parts();
                Err(AcceptedRegisteredCatchUpTerminalRetryFailure::new(
                    Self {
                        accepted,
                        status_request_receipt,
                        request_receipt,
                    },
                    terminal,
                ))
            }
        }
    }

    #[allow(
        clippy::result_large_err,
        reason = "live D queue returns exact accepted catch-up ownership"
    )]
    pub(crate) fn into_status_retry_after_queue_loss(
        self,
    ) -> Result<RegisteredAttachStatusRetryAuthority, AcceptedRegisteredCatchUpQueueLossRetryFailure>
    {
        let Self {
            accepted,
            status_request_receipt,
            request_receipt,
        } = self;
        match accepted.into_status_retry_after_queue_loss(AcceptedRegisteredLossGate::uninstalled())
        {
            Ok(lost) => {
                drop((status_request_receipt, request_receipt));
                Ok(RegisteredAttachStatusRetryAuthority { lost })
            }
            Err(accepted) => Err(AcceptedRegisteredCatchUpQueueLossRetryFailure::new(Self {
                accepted,
                status_request_receipt,
                request_receipt,
            })),
        }
    }
}

pub(super) struct AcceptedClientRegisteredCatchUpInstallFailure {
    accepted: AcceptedClientRegisteredCatchUp,
    transport: ClientLegTransport,
    kind: AcceptedClientInstallErrorKind,
}

impl AcceptedClientRegisteredCatchUpInstallFailure {
    fn new(
        accepted: AcceptedClientRegisteredCatchUp,
        transport: ClientLegTransport,
        kind: AcceptedClientInstallErrorKind,
    ) -> Self {
        Self {
            accepted,
            transport,
            kind,
        }
    }

    pub(super) const fn kind(&self) -> AcceptedClientInstallErrorKind {
        self.kind
    }

    pub(super) fn into_parts(self) -> (AcceptedClientRegisteredCatchUp, ClientLegTransport) {
        (self.accepted, self.transport)
    }
}

impl fmt::Debug for AcceptedClientRegisteredCatchUpInstallFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AcceptedClientRegisteredCatchUpInstallFailure")
            .field("kind", &self.kind)
            .field("accepted", &"[REDACTED]")
            .finish()
    }
}

pub(crate) struct AcceptedRegisteredCatchUpTerminalRetryFailure {
    accepted: AcceptedClientRegisteredCatchUp,
    terminal: ExactLegTerminal,
}

impl AcceptedRegisteredCatchUpTerminalRetryFailure {
    fn new(accepted: AcceptedClientRegisteredCatchUp, terminal: ExactLegTerminal) -> Self {
        Self { accepted, terminal }
    }

    pub(crate) const fn kind(&self) -> RegisteredAttachStatusRetryErrorKind {
        RegisteredAttachStatusRetryErrorKind::WrongTerminal
    }

    pub(crate) fn into_parts(self) -> (AcceptedClientRegisteredCatchUp, ExactLegTerminal) {
        (self.accepted, self.terminal)
    }
}

impl fmt::Debug for AcceptedRegisteredCatchUpTerminalRetryFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AcceptedRegisteredCatchUpTerminalRetryFailure")
            .field("kind", &self.kind())
            .field("ownership", &"[REDACTED]")
            .finish()
    }
}

pub(crate) struct AcceptedRegisteredCatchUpQueueLossRetryFailure {
    accepted: AcceptedClientRegisteredCatchUp,
}

impl AcceptedRegisteredCatchUpQueueLossRetryFailure {
    fn new(accepted: AcceptedClientRegisteredCatchUp) -> Self {
        Self { accepted }
    }

    pub(crate) const fn kind(&self) -> RegisteredAttachStatusRetryErrorKind {
        RegisteredAttachStatusRetryErrorKind::OriginalTransportStillLive
    }

    pub(crate) fn into_accepted(self) -> AcceptedClientRegisteredCatchUp {
        self.accepted
    }
}

impl fmt::Debug for AcceptedRegisteredCatchUpQueueLossRetryFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AcceptedRegisteredCatchUpQueueLossRetryFailure")
            .field("kind", &self.kind())
            .field("accepted", &"[REDACTED]")
            .finish()
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
    hint_registration: Arc<OwnerStandbyHintRegistrationIdentity>,
    unclaimed_hint_binding: Option<OwnerStandbyHintBinding>,
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

/// Process-local identity minted only after the exact registered B acceptance
/// entered its authenticated FIFO.  Keeping the identity private prevents a
/// value-equivalent registration on another transport from satisfying an
/// owner hint transaction.
struct OwnerStandbyHintRegistrationIdentity;

/// Once-claimable owner-hint authority for one exact registered B.
///
/// The slot retains the matching process-local identity.  This handle is
/// intentionally non-cloneable and has no scalar constructor: one physical
/// registration can therefore install at most one owner hint issuer.
pub(super) struct OwnerStandbyHintBinding {
    registration: Arc<OwnerStandbyHintRegistrationIdentity>,
    contract: OwnerStandbyHintContract,
}

impl OwnerStandbyHintBinding {
    pub(super) const fn contract(&self) -> OwnerStandbyHintContract {
        self.contract
    }

    #[cfg(test)]
    pub(crate) fn for_policy_test(
        session_id: SessionId,
        generation: LegGeneration,
        standby_nonce: StandbyNonce,
        features: FeatureSet,
    ) -> Self {
        Self {
            registration: Arc::new(OwnerStandbyHintRegistrationIdentity),
            contract: OwnerStandbyHintContract {
                session_id,
                generation,
                standby_nonce,
                features,
            },
        }
    }
}

impl fmt::Debug for OwnerStandbyHintBinding {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OwnerStandbyHintBinding([REDACTED])")
    }
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub(super) enum OwnerStandbyHintBindingClaimError {
    #[error("no authenticated registered owner standby exists")]
    NoRegisteredStandby,
    #[error("the registered owner standby acceptance is not delivered")]
    RegistrationAcceptanceNotDelivered,
    #[error("the registered owner standby endpoint or exact FIFO was lost")]
    EndpointOrQueueLost,
    #[error("the registered owner standby does not name this exact FIFO")]
    WrongQueue,
    #[error("the exact registered owner standby hint binding was already claimed")]
    AlreadyClaimed,
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub(super) enum OwnerStandbyHintEvidenceError {
    #[error("no authenticated registered owner standby exists")]
    NoRegisteredStandby,
    #[error("the hint issuer belongs to another physical standby registration")]
    WrongRegistrationBinding,
    #[error("the registered owner standby endpoint or exact FIFO was lost")]
    EndpointOrQueueLost,
    #[error("the exact registered owner standby probe is not fresh")]
    ProbeNotFresh,
}

/// Copy-only correlation exported to the closed owner composition after the
/// physical B registration and its ordered queue have both been authenticated.
/// It carries no transport authority and cannot enqueue a hint by itself.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct OwnerStandbyHintContract {
    session_id: SessionId,
    generation: LegGeneration,
    standby_nonce: StandbyNonce,
    features: FeatureSet,
}

impl OwnerStandbyHintContract {
    pub(super) const fn session_id(self) -> SessionId {
        self.session_id
    }

    pub(super) const fn generation(self) -> LegGeneration {
        self.generation
    }

    pub(super) const fn standby_nonce(self) -> StandbyNonce {
        self.standby_nonce
    }

    pub(super) const fn features(self) -> FeatureSet {
        self.features
    }
}

/// Physical B queue-admission rejection. The exact frame remains nested here
/// until the owner hint issuer restores it or commits a terminal settlement.
pub(super) struct OwnerStandbyHintEnqueueFailure {
    frame: LegControlFrame,
    kind: OwnerStandbyHintEnqueueErrorKind,
}

impl OwnerStandbyHintEnqueueFailure {
    fn new(frame: LegControlFrame, kind: OwnerStandbyHintEnqueueErrorKind) -> Self {
        Self { frame, kind }
    }

    pub(super) const fn kind(&self) -> &OwnerStandbyHintEnqueueErrorKind {
        &self.kind
    }

    pub(super) fn into_parts(self) -> (LegControlFrame, OwnerStandbyHintEnqueueErrorKind) {
        (self.frame, self.kind)
    }
}

impl fmt::Debug for OwnerStandbyHintEnqueueFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OwnerStandbyHintEnqueueFailure")
            .field("kind", &self.kind)
            .field("frame", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum OwnerStandbyHintEnqueueErrorKind {
    Evidence(OwnerStandbyHintEvidenceError),
    WrongCorrelation,
    Queue(LegControlQueueErrorKind),
}

/// Exactly-one bounded owner slot. A successful acceptance enqueue precedes
/// infallible installation; conflicts never mutate either the slot or queue.
pub(crate) struct OwnerStandbySlot {
    registered: Option<OwnerRegisteredStandby>,
    probe: OwnerStandbyProbeState,
    retained_attach_restore: Option<Box<OwnerRegisteredAttachRollback>>,
}

impl OwnerStandbySlot {
    pub(crate) const fn new() -> Self {
        Self {
            registered: None,
            probe: OwnerStandbyProbeState::Idle,
            retained_attach_restore: None,
        }
    }

    pub(crate) fn registered_is_live(&self) -> bool {
        self.registered
            .as_ref()
            .is_some_and(OwnerRegisteredStandby::is_live)
    }

    /// Moves the one issuer authority out of an installed registration only
    /// after the acceptance reached the exact still-live FIFO.  The slot keeps
    /// the matching process-local identity for every later prepare/admit
    /// revalidation.
    pub(super) fn take_exact_hint_binding(
        &mut self,
        queue: &LegOutboundQueue,
    ) -> Result<OwnerStandbyHintBinding, OwnerStandbyHintBindingClaimError> {
        let registered = self
            .registered
            .as_mut()
            .ok_or(OwnerStandbyHintBindingClaimError::NoRegisteredStandby)?;
        if !registered.is_live() || !registered.receipt.queue_is_live() {
            return Err(OwnerStandbyHintBindingClaimError::EndpointOrQueueLost);
        }
        if !registered.receipt.belongs_to_queue(queue) {
            return Err(OwnerStandbyHintBindingClaimError::WrongQueue);
        }
        if !registered.receipt.was_delivered() {
            return Err(OwnerStandbyHintBindingClaimError::RegistrationAcceptanceNotDelivered);
        }
        registered
            .unclaimed_hint_binding
            .take()
            .ok_or(OwnerStandbyHintBindingClaimError::AlreadyClaimed)
    }

    fn registered_for_hint_binding(
        &self,
        binding: &OwnerStandbyHintBinding,
    ) -> Result<&OwnerRegisteredStandby, OwnerStandbyHintEvidenceError> {
        let registered = self
            .registered
            .as_ref()
            .ok_or(OwnerStandbyHintEvidenceError::NoRegisteredStandby)?;
        let contract = registered.candidate.contract();
        let binding_contract = binding.contract;
        if !Arc::ptr_eq(&registered.hint_registration, &binding.registration)
            || contract.session_id != binding_contract.session_id
            || contract.generation != binding_contract.generation
            || contract.nonce != binding_contract.standby_nonce
            || contract.features != binding_contract.features
        {
            return Err(OwnerStandbyHintEvidenceError::WrongRegistrationBinding);
        }
        if !registered.is_live() || !registered.receipt.queue_is_live() {
            return Err(OwnerStandbyHintEvidenceError::EndpointOrQueueLost);
        }
        Ok(registered)
    }

    /// Same-clock physical evidence used before constructing or reusing one
    /// owner SWITCH_HINT.  Freshness belongs solely to this exact slot.
    pub(super) fn validate_exact_hint_binding_at(
        &self,
        binding: &OwnerStandbyHintBinding,
        now: SimTime,
    ) -> Result<(), OwnerStandbyHintEvidenceError> {
        let registered = self.registered_for_hint_binding(binding)?;
        let fresh = matches!(
            &self.probe,
            OwnerStandbyProbeState::Fresh {
                registration,
                receipt,
                acknowledged_at,
                fresh_until,
                ..
            } if Arc::ptr_eq(registration, &registered.hint_registration)
                && Arc::ptr_eq(registration, &binding.registration)
                && now >= *acknowledged_at
                && now <= *fresh_until
                && receipt.queue_is_live()
        );
        if !fresh {
            return Err(OwnerStandbyHintEvidenceError::ProbeNotFresh);
        }
        Ok(())
    }

    /// Returns the immutable authenticated correlation only while the exact B
    /// endpoint and its registration-acceptance receipt remain live.
    pub(super) fn hint_contract(&self) -> Option<OwnerStandbyHintContract> {
        let registered = self.registered.as_ref()?;
        if !registered.is_live() {
            return None;
        }
        let contract = registered.candidate.contract();
        Some(OwnerStandbyHintContract {
            session_id: contract.session_id,
            generation: contract.generation,
            standby_nonce: contract.nonce,
            features: contract.features,
        })
    }

    /// Moves one policy-constructed SWITCH_HINT through the exact physical B
    /// seal and sole FIFO. Every rejection returns the original frame; no
    /// decoded or scalar-only admission path exists.
    #[allow(
        clippy::result_large_err,
        reason = "bounded pressure must return the exact authenticated hint frame"
    )]
    pub(super) fn enqueue_exact_switch_hint(
        &self,
        binding: &OwnerStandbyHintBinding,
        now: SimTime,
        frame: LegControlFrame,
        queue: &mut LegOutboundQueue,
    ) -> Result<LegControlEnqueued, OwnerStandbyHintEnqueueFailure> {
        let registered = match self.registered_for_hint_binding(binding) {
            Ok(registered) => registered,
            Err(kind) => {
                return Err(OwnerStandbyHintEnqueueFailure::new(
                    frame,
                    OwnerStandbyHintEnqueueErrorKind::Evidence(kind),
                ));
            }
        };
        if let Err(kind) = self.validate_exact_hint_binding_at(binding, now) {
            return Err(OwnerStandbyHintEnqueueFailure::new(
                frame,
                OwnerStandbyHintEnqueueErrorKind::Evidence(kind),
            ));
        }
        if !registered.receipt.belongs_to_queue(queue) {
            return Err(OwnerStandbyHintEnqueueFailure::new(
                frame,
                OwnerStandbyHintEnqueueErrorKind::Evidence(
                    OwnerStandbyHintEvidenceError::EndpointOrQueueLost,
                ),
            ));
        }
        let contract = registered.candidate.contract();
        let correlated = frame.leg_generation() == contract.generation
            && matches!(
                frame.record(),
                LegControlRecord::SwitchHint {
                    session_id,
                    standby_nonce,
                    direction: Direction::TargetToClient,
                    cause: SwitchHintCause::ReverseApplicationAckStall,
                    ..
                } if *session_id == contract.session_id && *standby_nonce == contract.nonce
            );
        if !correlated {
            return Err(OwnerStandbyHintEnqueueFailure::new(
                frame,
                OwnerStandbyHintEnqueueErrorKind::WrongCorrelation,
            ));
        }
        match queue.push_leg_control(&registered.candidate.seal, frame, contract.features) {
            Ok(receipt) => Ok(receipt),
            Err(error) => {
                let kind = error.kind().clone();
                Err(OwnerStandbyHintEnqueueFailure::new(
                    error.into_frame(),
                    OwnerStandbyHintEnqueueErrorKind::Queue(kind),
                ))
            }
        }
    }

    /// Read-only physical adoption gate for the closed Quinn owner
    /// composition. The slot must name the actor's exact transport and sole
    /// FIFO; semantic registration equality alone is insufficient.
    pub(super) fn registered_matches_actor(
        &self,
        leg: &EstablishedLeg,
        queue: &LegOutboundQueue,
    ) -> bool {
        self.registered.as_ref().is_some_and(|registered| {
            registered.is_live()
                && registered.receipt.belongs_to_queue(queue)
                && leg.belongs_to_transport(&registered.candidate.seal)
        })
    }

    /// Derives and queues one exact probe from the authenticated registered B
    /// contract. Queue pressure retains the same frame and sequence inside the
    /// sole owner slot, so retry cannot mint a second probe obligation.
    pub(crate) fn drive_exact_probe(
        &mut self,
        probe_sequence: ProbeSequence,
        queue: &mut LegOutboundQueue,
    ) -> Result<OwnerStandbyProbeDrive, OwnerStandbyProbeDriveError> {
        let registered = self
            .registered
            .as_ref()
            .ok_or(OwnerStandbyProbeDriveError::NoRegisteredStandby)?;
        if !registered.is_live()
            || !registered.receipt.belongs_to_queue(queue)
            || !endpoint_is_open(&registered.candidate.endpoint)
        {
            return Err(OwnerStandbyProbeDriveError::EndpointOrQueueLost);
        }
        let pending = match std::mem::replace(&mut self.probe, OwnerStandbyProbeState::Idle) {
            OwnerStandbyProbeState::Idle => {
                let frame = registered
                    .candidate
                    .contract()
                    .probe_frame(probe_sequence)
                    .map_err(|_| OwnerStandbyProbeDriveError::ProtocolConstruction)?;
                OwnerPendingStandbyProbe {
                    registration: Arc::clone(&registered.hint_registration),
                    probe_sequence,
                    frame,
                }
            }
            OwnerStandbyProbeState::Pending(pending)
                if pending.probe_sequence == probe_sequence =>
            {
                pending
            }
            state @ OwnerStandbyProbeState::Pending(_) => {
                self.probe = state;
                return Err(OwnerStandbyProbeDriveError::DifferentProbePending);
            }
            state @ OwnerStandbyProbeState::Awaiting { .. } => {
                self.probe = state;
                return Err(OwnerStandbyProbeDriveError::ProbeAckPending);
            }
            state @ OwnerStandbyProbeState::Armed { .. } => {
                self.probe = state;
                return Err(OwnerStandbyProbeDriveError::ProbeAckPending);
            }
            state @ OwnerStandbyProbeState::Fresh { .. } => {
                self.probe = state;
                return Err(OwnerStandbyProbeDriveError::FreshnessActive);
            }
        };
        let seal = registered.candidate.seal.share();
        let features = registered.candidate.contract().features;
        match queue.push_leg_control(&seal, pending.frame, features) {
            Ok(receipt) => {
                self.probe = OwnerStandbyProbeState::Awaiting {
                    registration: Arc::clone(&pending.registration),
                    probe_sequence,
                    receipt,
                };
                Ok(OwnerStandbyProbeDrive::Enqueued { probe_sequence })
            }
            Err(error) => {
                let kind = error.kind().clone();
                self.probe = OwnerStandbyProbeState::Pending(OwnerPendingStandbyProbe {
                    registration: pending.registration,
                    probe_sequence,
                    frame: error.into_frame(),
                });
                Err(OwnerStandbyProbeDriveError::Queue(kind))
            }
        }
    }

    /// Starts the exact probe deadline only after positive writer delivery.
    /// The queued receipt and correlation remain in this slot on every
    /// rejection, so retry cannot replace either sequence or deadline.
    pub(super) fn arm_exact_probe_after_delivery(
        &mut self,
        now: SimTime,
        timeout: Duration,
    ) -> Result<OwnerStandbyProbeArmDrive, OwnerStandbyProbeArmError> {
        let state = std::mem::replace(&mut self.probe, OwnerStandbyProbeState::Idle);
        let (registration, probe_sequence, receipt) = match state {
            OwnerStandbyProbeState::Awaiting {
                registration,
                probe_sequence,
                receipt,
            } => (registration, probe_sequence, receipt),
            OwnerStandbyProbeState::Armed {
                registration,
                probe_sequence,
                receipt,
                deadline,
            } => {
                self.probe = OwnerStandbyProbeState::Armed {
                    registration,
                    probe_sequence,
                    receipt,
                    deadline,
                };
                return Ok(OwnerStandbyProbeArmDrive::AlreadyArmed {
                    probe_sequence,
                    deadline,
                });
            }
            state => {
                self.probe = state;
                return Err(OwnerStandbyProbeArmError::WrongState);
            }
        };
        if timeout.is_zero() {
            self.probe = OwnerStandbyProbeState::Awaiting {
                registration,
                probe_sequence,
                receipt,
            };
            return Err(OwnerStandbyProbeArmError::InvalidTimeout);
        }
        let registered_is_live = self.registered.as_ref().is_some_and(|registered| {
            registered.is_live() && Arc::ptr_eq(&registration, &registered.hint_registration)
        });
        if !registered_is_live || !receipt.queue_is_live() {
            self.probe = OwnerStandbyProbeState::Awaiting {
                registration,
                probe_sequence,
                receipt,
            };
            return Err(OwnerStandbyProbeArmError::EndpointOrQueueLost);
        }
        if !receipt.was_delivered() {
            self.probe = OwnerStandbyProbeState::Awaiting {
                registration,
                probe_sequence,
                receipt,
            };
            return Ok(OwnerStandbyProbeArmDrive::NotDelivered);
        }
        let deadline = match now.checked_add(timeout) {
            Ok(deadline) => deadline,
            Err(error) => {
                self.probe = OwnerStandbyProbeState::Awaiting {
                    registration,
                    probe_sequence,
                    receipt,
                };
                return Err(OwnerStandbyProbeArmError::Clock(error));
            }
        };
        self.probe = OwnerStandbyProbeState::Armed {
            registration,
            probe_sequence,
            receipt,
            deadline,
        };
        Ok(OwnerStandbyProbeArmDrive::Armed {
            probe_sequence,
            deadline,
        })
    }

    /// Commits only the exact armed probe correlation represented by the
    /// composition-owned semantic timer. A retry before the absolute deadline
    /// or with a mismatched timer leaves the non-cloneable receipt in place.
    pub(super) fn expire_exact_probe_if_due(
        &mut self,
        expected_sequence: ProbeSequence,
        expected_deadline: SimTime,
        now: SimTime,
    ) -> Result<OwnerStandbyProbeExpiryDrive, OwnerStandbyProbeExpiryError> {
        let state = std::mem::replace(&mut self.probe, OwnerStandbyProbeState::Idle);
        let OwnerStandbyProbeState::Armed {
            registration,
            probe_sequence,
            receipt,
            deadline,
        } = state
        else {
            self.probe = state;
            return Err(OwnerStandbyProbeExpiryError::WrongState);
        };
        if probe_sequence != expected_sequence || deadline != expected_deadline {
            self.probe = OwnerStandbyProbeState::Armed {
                registration,
                probe_sequence,
                receipt,
                deadline,
            };
            return Err(OwnerStandbyProbeExpiryError::WrongCorrelation);
        }
        if now < deadline {
            self.probe = OwnerStandbyProbeState::Armed {
                registration,
                probe_sequence,
                receipt,
                deadline,
            };
            return Ok(OwnerStandbyProbeExpiryDrive::Pending);
        }
        drop((registration, receipt));
        Ok(OwnerStandbyProbeExpiryDrive::Expired)
    }

    /// Accepts freshness only from the exact registered B seal and the exact
    /// probe sequence currently owned by this slot. The actor-captured arrival
    /// time becomes the sole freshness baseline.
    #[allow(
        clippy::result_large_err,
        reason = "wrong physical probe ACK returns the exact classified inbound fact"
    )]
    pub(crate) fn observe_exact_probe_ack(
        &mut self,
        inbound: BoundInbound,
        arrived_at: SimTime,
        fresh_for: Duration,
    ) -> Result<OwnerStandbyProbeFreshened, OwnerStandbyProbeAckFailure> {
        if fresh_for.is_zero() {
            return Err(OwnerStandbyProbeAckFailure::new(
                inbound,
                OwnerStandbyProbeAckErrorKind::InvalidFreshnessWindow,
            ));
        }
        let (registration, probe_sequence, receipt, deadline) =
            match std::mem::replace(&mut self.probe, OwnerStandbyProbeState::Idle) {
                OwnerStandbyProbeState::Awaiting {
                    registration,
                    probe_sequence,
                    receipt,
                } => (registration, probe_sequence, receipt, None),
                OwnerStandbyProbeState::Armed {
                    registration,
                    probe_sequence,
                    receipt,
                    deadline,
                } => (registration, probe_sequence, receipt, Some(deadline)),
                state => {
                    self.probe = state;
                    return Err(OwnerStandbyProbeAckFailure::new(
                        inbound,
                        OwnerStandbyProbeAckErrorKind::NoAwaitingProbe,
                    ));
                }
            };
        if let Some(expired_deadline) = deadline
            && arrived_at > expired_deadline
        {
            drop((registration, probe_sequence, receipt, expired_deadline));
            return Err(OwnerStandbyProbeAckFailure::new(
                inbound,
                OwnerStandbyProbeAckErrorKind::DeadlineExpired,
            ));
        }
        let rejection = match self.registered.as_ref() {
            Some(registered)
                if registered.is_live()
                    && receipt.queue_is_live()
                    && Arc::ptr_eq(&registration, &registered.hint_registration) =>
            {
                match &inbound {
                    BoundInbound::LegControl(bound) => {
                        let (received_seal, frame) = bound.parts();
                        (!registered.candidate.seal.same_connection(received_seal)
                            || !registered
                                .candidate
                                .contract()
                                .matches_probe_ack(frame, probe_sequence))
                        .then_some(OwnerStandbyProbeAckErrorKind::Rejected)
                    }
                    BoundInbound::Session(_) => Some(OwnerStandbyProbeAckErrorKind::Rejected),
                }
            }
            _ => Some(OwnerStandbyProbeAckErrorKind::EndpointOrQueueLost),
        };
        if let Some(kind) = rejection {
            self.probe = match deadline {
                Some(deadline) => OwnerStandbyProbeState::Armed {
                    registration,
                    probe_sequence,
                    receipt,
                    deadline,
                },
                None => OwnerStandbyProbeState::Awaiting {
                    registration,
                    probe_sequence,
                    receipt,
                },
            };
            return Err(OwnerStandbyProbeAckFailure::new(inbound, kind));
        }
        let fresh_until = match arrived_at.checked_add(fresh_for) {
            Ok(fresh_until) => fresh_until,
            Err(error) => {
                self.probe = match deadline {
                    Some(deadline) => OwnerStandbyProbeState::Armed {
                        registration,
                        probe_sequence,
                        receipt,
                        deadline,
                    },
                    None => OwnerStandbyProbeState::Awaiting {
                        registration,
                        probe_sequence,
                        receipt,
                    },
                };
                return Err(OwnerStandbyProbeAckFailure::new(
                    inbound,
                    OwnerStandbyProbeAckErrorKind::Clock(error),
                ));
            }
        };
        self.probe = OwnerStandbyProbeState::Fresh {
            registration,
            probe_sequence,
            receipt,
            acknowledged_at: arrived_at,
            fresh_until,
        };
        Ok(OwnerStandbyProbeFreshened {
            probe_sequence,
            fresh_until,
        })
    }

    pub(crate) fn probe_is_fresh_at(&self, now: SimTime) -> bool {
        let Some(registered) = self.registered.as_ref() else {
            return false;
        };
        matches!(
            &self.probe,
            OwnerStandbyProbeState::Fresh {
                registration,
                receipt,
                acknowledged_at,
                fresh_until,
                ..
            } if Arc::ptr_eq(registration, &registered.hint_registration)
                && now >= *acknowledged_at
                && now <= *fresh_until
                && receipt.queue_is_live()
                && registered.is_live()
        )
    }

    /// Retires only an already-expired freshness witness. Equal-boundary
    /// freshness remains valid; pending/awaiting probes are never consumed by
    /// a freshness check.
    pub(super) fn expire_probe_freshness_at(&mut self, now: SimTime) -> Option<ProbeSequence> {
        let state = std::mem::replace(&mut self.probe, OwnerStandbyProbeState::Idle);
        match state {
            OwnerStandbyProbeState::Fresh {
                probe_sequence,
                fresh_until,
                ..
            } if now > fresh_until => Some(probe_sequence),
            state => {
                self.probe = state;
                None
            }
        }
    }

    /// Owner-role entry into the same responder used by the client. Missing,
    /// stale, or mismatched registrations return the exact inbound fact to the
    /// caller instead of manufacturing an ACK.
    pub(super) fn classify_exact_leg_probe(&self, inbound: BoundInbound) -> ExactLegProbeResponse {
        let Some(registered) = self.registered.as_ref() else {
            return ExactLegProbeResponse::Unhandled(inbound);
        };
        let responder = ExactLegProbeResponder::from_registered_parts(
            &registered.candidate.seal,
            &registered.candidate.endpoint,
            registered.candidate.contract(),
        );
        responder.classify(inbound)
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
        if self.retained_attach_restore.is_some() {
            return Err(OwnerStandbyAdmissionFailure::rejected(candidate));
        }
        let duplicate = match self.registered.as_ref() {
            None => false,
            Some(registered) if registered.candidate.same_registration(&candidate) => true,
            Some(_) => return Err(OwnerStandbyAdmissionFailure::rejected(candidate)),
        };
        if !endpoint_is_open(&candidate.endpoint) {
            return Err(OwnerStandbyAdmissionFailure::retry(
                candidate,
                LegControlQueueErrorKind::EndpointLost,
            ));
        }
        let contract = candidate.contract();
        let acceptance = match contract.acceptance_frame() {
            Ok(acceptance) => acceptance,
            Err(_) => return Err(OwnerStandbyAdmissionFailure::rejected(candidate)),
        };
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
            let contract = candidate.contract();
            let hint_registration = Arc::new(OwnerStandbyHintRegistrationIdentity);
            let unclaimed_hint_binding = Some(OwnerStandbyHintBinding {
                registration: Arc::clone(&hint_registration),
                contract: OwnerStandbyHintContract {
                    session_id: contract.session_id,
                    generation: contract.generation,
                    standby_nonce: contract.nonce,
                    features: contract.features,
                },
            });
            self.registered = Some(OwnerRegisteredStandby {
                candidate,
                receipt,
                hint_registration,
                unclaimed_hint_binding,
            });
            self.probe = OwnerStandbyProbeState::Idle;
            Ok(OwnerStandbyAdmission::Installed)
        }
    }

    pub(crate) fn retire_lost(&mut self) -> Option<RetiredOwnerStandby> {
        if self.registered_is_live() {
            return None;
        }
        let discarded = self.retirement_inventory()?;
        self.registered.take().map(|_| {
            self.probe = OwnerStandbyProbeState::Idle;
            RetiredOwnerStandby { discarded }
        })
    }

    /// Consumes a hard terminal only when it names this slot's exact
    /// authenticated registered transport. Unlike `registered_matches_actor`,
    /// this check deliberately remains usable after endpoint liveness is gone.
    pub(super) fn retire_exact_terminal(
        &mut self,
        terminal: ExactLegTerminal,
    ) -> Result<RetiredOwnerStandby, OwnerStandbyTerminalRetireFailure> {
        let Some(registered) = self.registered.as_ref() else {
            return Err(OwnerStandbyTerminalRetireFailure { terminal });
        };
        if !terminal
            .belongs_to_transport(&registered.candidate.seal, &registered.candidate.endpoint)
        {
            return Err(OwnerStandbyTerminalRetireFailure { terminal });
        }
        let Some(discarded) = self.retirement_inventory() else {
            return Err(OwnerStandbyTerminalRetireFailure { terminal });
        };
        let Some(registered) = self.registered.take() else {
            return Err(OwnerStandbyTerminalRetireFailure { terminal });
        };
        self.probe = OwnerStandbyProbeState::Idle;
        drop((registered, terminal));
        Ok(RetiredOwnerStandby { discarded })
    }

    fn retirement_inventory(&self) -> Option<OwnerStandbyRetirementInventory> {
        let registered = self.registered.as_ref()?;
        let (pending_probe_frames, pending_probe_bytes, probe_receipts, armed_timers, freshness) =
            match &self.probe {
                OwnerStandbyProbeState::Idle => (0, 0, 0, 0, 0),
                OwnerStandbyProbeState::Pending(pending) => (
                    1,
                    pending
                        .frame
                        .encode(registered.candidate.contract().features)
                        .ok()?
                        .len(),
                    0,
                    0,
                    0,
                ),
                OwnerStandbyProbeState::Awaiting { .. } => (0, 0, 1, 0, 0),
                OwnerStandbyProbeState::Armed { .. } => (0, 0, 1, 1, 0),
                OwnerStandbyProbeState::Fresh { .. } => (0, 0, 1, 0, 1),
            };
        Some(OwnerStandbyRetirementInventory {
            registered_authorities: 1,
            registration_receipts: 1,
            hint_registration_identities: 1,
            unclaimed_hint_bindings: usize::from(registered.unclaimed_hint_binding.is_some()),
            pending_probe_frames,
            pending_probe_bytes,
            probe_receipts,
            armed_timers,
            freshness_witnesses: freshness,
        })
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
        if self.retained_attach_restore.is_some() {
            return Err(OwnerRegisteredAttachGateError::Rejected);
        }
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
        let probe = std::mem::replace(&mut self.probe, OwnerStandbyProbeState::Idle);
        Ok(OwnerRegisteredAttachGate {
            registered,
            probe,
            contract,
            request,
            reservation,
        })
    }

    fn restore_registered_attach_rollback(
        &mut self,
        rollback: OwnerRegisteredAttachRollback,
        queue: &mut LegOutboundQueue,
    ) -> Result<(), OwnerRegisteredAttachRestoreError> {
        // A gate can be minted only while this fixed retained slot is vacant,
        // and taking `registered` prevents a second gate from being minted.
        // Install the whole rollback before any fallible validation so a
        // value-only error can never discard its linear authorities.
        if self.retained_attach_restore.is_some() {
            return Err(OwnerRegisteredAttachRestoreError::SlotChanged);
        }
        self.retained_attach_restore = Some(Box::new(rollback));
        if self.registered.is_some() || !matches!(self.probe, OwnerStandbyProbeState::Idle) {
            return Err(OwnerRegisteredAttachRestoreError::SlotChanged);
        }
        let Some(rollback) = self.retained_attach_restore.take() else {
            return Err(OwnerRegisteredAttachRestoreError::SlotChanged);
        };
        let rollback = *rollback;
        let OwnerRegisteredAttachRollback {
            registered,
            probe,
            contract,
            request,
            reservation,
        } = rollback;
        match reservation.release(queue) {
            Ok(()) => {
                self.registered = Some(registered);
                self.probe = probe;
                Ok(())
            }
            Err(reservation) => {
                self.retained_attach_restore = Some(Box::new(OwnerRegisteredAttachRollback {
                    registered,
                    probe,
                    contract,
                    request,
                    reservation,
                }));
                Err(OwnerRegisteredAttachRestoreError::QueueReservation)
            }
        }
    }

    pub(super) fn has_retained_attach_restore(&self) -> bool {
        self.retained_attach_restore.is_some()
    }

    pub(super) fn retry_retained_attach_restore(
        &mut self,
        queue: &mut LegOutboundQueue,
    ) -> Result<(), OwnerRegisteredAttachRestoreError> {
        let Some(rollback) = self.retained_attach_restore.take() else {
            return Err(OwnerRegisteredAttachRestoreError::NoRetainedRollback);
        };
        self.restore_registered_attach_rollback(*rollback, queue)
    }
}

struct OwnerPendingStandbyProbe {
    registration: Arc<OwnerStandbyHintRegistrationIdentity>,
    probe_sequence: ProbeSequence,
    frame: LegControlFrame,
}

enum OwnerStandbyProbeState {
    Idle,
    Pending(OwnerPendingStandbyProbe),
    Awaiting {
        registration: Arc<OwnerStandbyHintRegistrationIdentity>,
        probe_sequence: ProbeSequence,
        receipt: LegControlEnqueued,
    },
    Armed {
        registration: Arc<OwnerStandbyHintRegistrationIdentity>,
        probe_sequence: ProbeSequence,
        receipt: LegControlEnqueued,
        deadline: SimTime,
    },
    Fresh {
        registration: Arc<OwnerStandbyHintRegistrationIdentity>,
        probe_sequence: ProbeSequence,
        receipt: LegControlEnqueued,
        acknowledged_at: SimTime,
        fresh_until: SimTime,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OwnerStandbyProbeDrive {
    Enqueued { probe_sequence: ProbeSequence },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum OwnerStandbyProbeArmDrive {
    NotDelivered,
    Armed {
        probe_sequence: ProbeSequence,
        deadline: SimTime,
    },
    AlreadyArmed {
        probe_sequence: ProbeSequence,
        deadline: SimTime,
    },
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub(super) enum OwnerStandbyProbeArmError {
    #[error("owner standby probe timeout must be nonzero")]
    InvalidTimeout,
    #[error("owner standby probe endpoint or queue was lost before arming")]
    EndpointOrQueueLost,
    #[error("owner standby probe deadline clock is invalid: {0}")]
    Clock(SimTimeError),
    #[error("owner standby probe is not awaiting writer delivery")]
    WrongState,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum OwnerStandbyProbeExpiryDrive {
    Pending,
    Expired,
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub(super) enum OwnerStandbyProbeExpiryError {
    #[error("owner standby probe is not armed")]
    WrongState,
    #[error("owner standby probe timer does not match the exact armed correlation")]
    WrongCorrelation,
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub(crate) enum OwnerStandbyProbeDriveError {
    #[error("no authenticated registered owner standby exists")]
    NoRegisteredStandby,
    #[error("registered owner standby endpoint or queue was lost")]
    EndpointOrQueueLost,
    #[error("another exact owner standby probe is pending")]
    DifferentProbePending,
    #[error("an owner standby probe ACK is already pending")]
    ProbeAckPending,
    #[error("owner standby freshness is still active")]
    FreshnessActive,
    #[error("owner standby probe construction failed")]
    ProtocolConstruction,
    #[error("owner standby probe queue rejected the exact frame: {0:?}")]
    Queue(LegControlQueueErrorKind),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct OwnerStandbyProbeFreshened {
    probe_sequence: ProbeSequence,
    fresh_until: SimTime,
}

impl OwnerStandbyProbeFreshened {
    pub(crate) const fn probe_sequence(self) -> ProbeSequence {
        self.probe_sequence
    }

    pub(crate) const fn fresh_until(self) -> SimTime {
        self.fresh_until
    }
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub(crate) enum OwnerStandbyProbeAckErrorKind {
    #[error("owner standby probe freshness window must be nonzero")]
    InvalidFreshnessWindow,
    #[error("no exact owner standby probe ACK is pending")]
    NoAwaitingProbe,
    #[error("registered owner standby endpoint or queue was lost")]
    EndpointOrQueueLost,
    #[error("owner standby probe ACK arrived after the exact armed deadline")]
    DeadlineExpired,
    #[error("owner standby probe ACK did not match the exact physical B probe")]
    Rejected,
    #[error("owner standby probe freshness clock is invalid: {0}")]
    Clock(SimTimeError),
}

pub(crate) struct OwnerStandbyProbeAckFailure {
    inbound: BoundInbound,
    kind: OwnerStandbyProbeAckErrorKind,
}

impl OwnerStandbyProbeAckFailure {
    fn new(inbound: BoundInbound, kind: OwnerStandbyProbeAckErrorKind) -> Self {
        Self { inbound, kind }
    }

    pub(crate) const fn kind(&self) -> OwnerStandbyProbeAckErrorKind {
        self.kind
    }

    pub(crate) fn into_inbound(self) -> BoundInbound {
        self.inbound
    }
}

impl fmt::Debug for OwnerStandbyProbeAckFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OwnerStandbyProbeAckFailure")
            .field("kind", &self.kind)
            .field("inbound", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub(crate) enum OwnerRegisteredAttachGateError {
    #[error("owner registered standby does not authorize this exact ATTACH")]
    Rejected,
    #[error("owner registered standby queue reservation failed: {0}")]
    QueueReservation(AttachAcceptanceReserveError),
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub(super) enum OwnerRegisteredAttachRestoreError {
    #[error("owner registered standby changed while its ATTACH gate was exclusive")]
    SlotChanged,
    #[error("owner registered ATTACH reservation did not belong to the restoring FIFO")]
    QueueReservation,
    #[error("owner registered standby has no retained ATTACH rollback")]
    NoRetainedRollback,
}

struct OwnerRegisteredAttachRollback {
    registered: OwnerRegisteredStandby,
    probe: OwnerStandbyProbeState,
    contract: StandbyContract,
    request: AttachRequest,
    reservation: AttachAcceptanceReservation,
}

/// Temporary exclusive ownership of the sole registered B slot during one
/// synchronous owner transaction. It is intentionally non-cloneable.
pub(super) struct OwnerRegisteredAttachGate {
    registered: OwnerRegisteredStandby,
    probe: OwnerStandbyProbeState,
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

    pub(super) fn restore(
        self,
        slot: &mut OwnerStandbySlot,
        queue: &mut LegOutboundQueue,
    ) -> Result<(), OwnerRegisteredAttachRestoreError> {
        let Self {
            registered,
            probe,
            contract,
            request,
            reservation,
        } = self;
        slot.restore_registered_attach_rollback(
            OwnerRegisteredAttachRollback {
                registered,
                probe,
                contract,
                request,
                reservation,
            },
            queue,
        )
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
            .field(
                "attach_restore_retained",
                &self.retained_attach_restore.is_some(),
            )
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
    fn rejected(candidate: ExactAuthenticatedStandby) -> Self {
        Self {
            kind: OwnerStandbyAdmissionErrorKind::Rejected,
            retry: Some(candidate),
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct OwnerStandbyRetirementInventory {
    registered_authorities: usize,
    registration_receipts: usize,
    hint_registration_identities: usize,
    unclaimed_hint_bindings: usize,
    pending_probe_frames: usize,
    pending_probe_bytes: usize,
    probe_receipts: usize,
    armed_timers: usize,
    freshness_witnesses: usize,
}

impl OwnerStandbyRetirementInventory {
    pub(crate) const fn registered_authorities(self) -> usize {
        self.registered_authorities
    }

    pub(crate) const fn registration_receipts(self) -> usize {
        self.registration_receipts
    }

    pub(crate) const fn hint_registration_identities(self) -> usize {
        self.hint_registration_identities
    }

    pub(crate) const fn unclaimed_hint_bindings(self) -> usize {
        self.unclaimed_hint_bindings
    }

    pub(crate) const fn pending_probe_frames(self) -> usize {
        self.pending_probe_frames
    }

    pub(crate) const fn pending_probe_bytes(self) -> usize {
        self.pending_probe_bytes
    }

    pub(crate) const fn probe_receipts(self) -> usize {
        self.probe_receipts
    }

    pub(crate) const fn armed_timers(self) -> usize {
        self.armed_timers
    }

    pub(crate) const fn freshness_witnesses(self) -> usize {
        self.freshness_witnesses
    }
}

pub(crate) struct RetiredOwnerStandby {
    discarded: OwnerStandbyRetirementInventory,
}

impl RetiredOwnerStandby {
    pub(crate) const fn discarded(&self) -> OwnerStandbyRetirementInventory {
        self.discarded
    }
}

/// Exact standby-terminal mismatch. The slot remains unchanged and the sole
/// terminal is returned for actor-envelope restoration.
pub(super) struct OwnerStandbyTerminalRetireFailure {
    terminal: ExactLegTerminal,
}

impl OwnerStandbyTerminalRetireFailure {
    pub(super) fn into_terminal(self) -> ExactLegTerminal {
        self.terminal
    }
}

impl fmt::Debug for OwnerStandbyTerminalRetireFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OwnerStandbyTerminalRetireFailure([REDACTED])")
    }
}

impl fmt::Debug for RetiredOwnerStandby {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RetiredOwnerStandby([REDACTED])")
    }
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use crate::owned_upstream::client_controller::ClientSessionController;
    use crate::owned_upstream::leg::{
        AttachResponse, EstablishedLeg, LegTransportReporter, LegTransportTerminalReason,
        OwnerAttachTransaction,
    };
    use crate::owned_upstream::leg_io::{
        EncodedLegTransport, EncodedLegTransportError, LegEndpointRole, LegIo, LegIoLimits,
        LegOutboundQueue, MemoryFaultScript, MemoryLegTransport, OrderedDeliveryToken,
    };
    use crate::owned_upstream::owner_target::{
        OwnerTargetAttachPublication, OwnerTargetConfig, OwnerTargetError, OwnerTargetExecutor,
    };
    use crate::owned_upstream::supervisor::SessionSupervisor;
    use crate::owned_upstream::target::{MemoryTarget, MemoryTargetConfig};
    use crate::owned_upstream::tcp::FlowPortConfig;
    use crate::owned_upstream::two_leg::{
        EncodedDelivery, LegId, SimTime, TwoLegWire, WireBounds, WireCapacity, WireCapacitySpec,
        WireLane, WireRoute,
    };
    use crate::resumable::{
        AttachAlpn, AttachAuthority, AttachCredentials, AttachNonce, AttachPolicy, AttachRequest,
        AttachTransportBinding, DevicePrincipal, DeviceSecret, FeatureOffer, FeatureSet, Frame,
        LegGeneration, OwnerIdentity, ProtocolError, ReceiveBudgetLimits, Record,
        ReplayBudgetLimits, ResumeSecret, SESSION_PROTOCOL_VERSION, STANDBY_CONTROL_V1,
        SessionConfig, SessionId, StandbyNonce, TcpWindowLimits, TlsExporterBinding, VersionRange,
    };
    use bytes::Bytes;
    use std::time::Duration;

    pub(super) const FEATURES: u64 = STANDBY_CONTROL_V1.bits() | 0b0111;

    #[test]
    fn client_install_prepare_error_matrix_separates_pressure_terminal_and_invariant() {
        assert_eq!(
            classify_client_recovery_plan_error(&ClientRecoveryPlanError::StorageUnavailable),
            AcceptedClientInstallErrorKind::RecoveryPlanRetryable,
        );
        for invariant in [
            ClientRecoveryPlanError::EffectBound { actual: 2, max: 1 },
            ClientRecoveryPlanError::MissingExactActivation,
            ClientRecoveryPlanError::WrongGeneration,
            ClientRecoveryPlanError::UnexpectedEffect,
            ClientRecoveryPlanError::InvalidFrame(ProtocolError::InvalidMagic),
            ClientRecoveryPlanError::CounterOverflow,
        ] {
            assert_eq!(
                classify_client_recovery_plan_error(&invariant),
                AcceptedClientInstallErrorKind::RecoveryPlanInvariant,
            );
        }

        for retryable in [
            ClientRecoveryReserveError::RequestNotDelivered,
            ClientRecoveryReserveError::StorageUnavailable,
        ] {
            assert_eq!(
                classify_client_recovery_reservation_error(&retryable),
                AcceptedClientInstallErrorKind::RecoveryReservationRetryable,
            );
        }
        assert_eq!(
            classify_client_recovery_reservation_error(
                &ClientRecoveryReserveError::EndpointOrQueueLost,
            ),
            AcceptedClientInstallErrorKind::RecoveryReservationTerminal,
        );
        for invariant in [
            ClientRecoveryReserveError::WrongQueue,
            ClientRecoveryReserveError::WrongGeneration,
            ClientRecoveryReserveError::WrongEndpoint,
            ClientRecoveryReserveError::NotAwaitingReplacement,
            ClientRecoveryReserveError::FrameCannotFit {
                largest_frame_bytes: 2,
                queue_max_bytes: 1,
            },
            ClientRecoveryReserveError::EndpointFrameCannotFit {
                largest_frame_bytes: 2,
                endpoint_max_frame_bytes: 1,
            },
            ClientRecoveryReserveError::EndpointMessageBudgetExceeded,
            ClientRecoveryReserveError::EndpointByteBudgetExceeded,
            ClientRecoveryReserveError::CounterOverflow,
            ClientRecoveryReserveError::OrdinalExhausted,
        ] {
            assert_eq!(
                classify_client_recovery_reservation_error(&invariant),
                AcceptedClientInstallErrorKind::RecoveryReservationInvariant,
            );
        }
    }

    pub(super) fn binding(exporter: u8) -> AttachTransportBinding {
        AttachTransportBinding::new(
            OwnerIdentity::new([0x31; 32]).unwrap(),
            AttachAlpn::new(b"mini-vpn-owned/1").unwrap(),
            TlsExporterBinding::new([exporter; 32]).unwrap(),
            DevicePrincipal::new([0x53; 16]).unwrap(),
        )
    }

    pub(super) fn credentials() -> AttachCredentials {
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

    pub(super) fn active_pair() -> (
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

    pub(super) fn limits() -> LegIoLimits {
        LegIoLimits::from_wire_capacity(capacity(), 1_024).unwrap()
    }

    pub(super) fn transport() -> MemoryLegTransport {
        MemoryLegTransport::new(
            TwoLegWire::new(WireBounds::new(capacity(), 64, 32_000, 4, 4).unwrap()),
            MemoryFaultScript::new(0),
        )
    }

    pub(super) fn release(transport: &mut MemoryLegTransport) {
        while transport.advance_one_due(SimTime::ZERO).unwrap() {}
        transport.advance_idle_to(SimTime::ZERO).unwrap();
    }

    fn initial_client_controller() -> ClientSessionController {
        let transport_binding = binding(0x42);
        let mut client = LegIo::for_authenticated_transport(
            LegId::A,
            LegEndpointRole::Client,
            transport_binding,
            limits(),
        );
        let mut owner = LegIo::for_authenticated_transport(
            LegId::A,
            LegEndpointRole::Owner,
            transport_binding,
            limits(),
        );
        let request = AttachRequest::new(
            SessionId::new([0x11; 16]).unwrap(),
            LegGeneration::new(2).unwrap(),
            AttachNonce::new([0x22; 16]).unwrap(),
            VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION).unwrap(),
            FeatureOffer::new(FEATURES, STANDBY_CONTROL_V1.bits()).unwrap(),
        );
        let proof = credentials().prove(&request, &transport_binding).unwrap();
        let pending = client
            .established_leg()
            .begin_attach(request)
            .bind_initial_frame(request.to_attach_frame(proof))
            .unwrap();
        let mut client_queue =
            LegOutboundQueue::for_leg(client.established_leg(), 4, 4_096).unwrap();
        let awaiting = pending.enqueue(&mut client_queue).unwrap();
        let mut owner_queue = LegOutboundQueue::for_leg(owner.established_leg(), 4, 4_096).unwrap();
        let mut transport = transport();

        client_queue
            .try_flush(
                &mut client,
                &mut transport.controller_sender(),
                SimTime::ZERO,
            )
            .unwrap()
            .unwrap();
        release(&mut transport);
        let request_delivery = transport
            .try_recv_next(owner.inbound_route())
            .unwrap()
            .unwrap();
        let received = owner.receive_delivery(request_delivery).unwrap();
        let mut pending_owner =
            SessionSupervisor::prepare_owner_unproven_for_test(config(), authority());
        let owner_bootstrap = pending_owner
            .accept(owner.established_leg(), received, &mut owner_queue)
            .unwrap()
            .enqueue_acceptance(&mut owner_queue)
            .unwrap();

        owner_queue
            .try_flush(
                &mut owner,
                &mut transport.controller_sender(),
                SimTime::ZERO,
            )
            .unwrap()
            .unwrap();
        release(&mut transport);
        let acceptance_delivery = transport
            .try_recv_next(client.inbound_route())
            .unwrap()
            .unwrap();
        let response = client.receive_delivery(acceptance_delivery).unwrap();
        let accepted = awaiting.validate_response(&client_queue, response).unwrap();
        drop(owner_bootstrap);
        ClientSessionController::bootstrap_initial(config(), accepted, client, client_queue)
            .unwrap()
    }

    struct WithheldAttachDelivery {
        submission: Option<(WireRoute, WireLane, Bytes)>,
        completion: Option<OrderedDeliveryToken>,
    }

    impl WithheldAttachDelivery {
        const fn new() -> Self {
            Self {
                submission: None,
                completion: None,
            }
        }
    }

    impl EncodedLegTransport for WithheldAttachDelivery {
        fn send_encoded(
            &mut self,
            _now: SimTime,
            _route: WireRoute,
            _lane: WireLane,
            _bytes: Bytes,
        ) -> Result<(), EncodedLegTransportError> {
            panic!("registered ATTACH must retain ordered delivery authority")
        }

        fn send_attach_ordered_encoded(
            &mut self,
            _now: SimTime,
            route: WireRoute,
            lane: WireLane,
            bytes: Bytes,
            completion: OrderedDeliveryToken,
        ) -> Result<(), EncodedLegTransportError> {
            assert!(self.submission.is_none());
            assert!(self.completion.is_none());
            self.submission = Some((route, lane, bytes));
            self.completion = Some(completion);
            Ok(())
        }
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
    pub(super) fn deliver_registration(
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

    pub(super) fn deliver_acceptance(
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

    struct RegisteredStandbyProbeScenario {
        registered: ClientRegisteredStandby,
        contract: StandbyContract,
        client_b: LegIo,
        owner_b: LegIo,
        client_queue: LegOutboundQueue,
        transport: MemoryLegTransport,
    }

    fn registered_standby_for_probe(max_client_frames: usize) -> RegisteredStandbyProbeScenario {
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
            LegOutboundQueue::for_leg(client_b.established_leg(), max_client_frames, 4_096)
                .unwrap();
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
        let registered = awaiting
            .validate(deliver_acceptance(
                &mut owner_b,
                &mut client_b,
                &mut owner_queue,
                &mut transport,
            ))
            .unwrap();
        let contract = registered.contract;
        RegisteredStandbyProbeScenario {
            registered,
            contract,
            client_b,
            owner_b,
            client_queue,
            transport,
        }
    }

    fn exact_standby_probe_ack(
        leg: &EstablishedLeg,
        contract: StandbyContract,
        probe_sequence: ProbeSequence,
    ) -> BoundInbound {
        BoundInbound::LegControl(
            leg.bind_received_control_frame(
                LegControlFrame::try_new(
                    contract.generation,
                    LegControlRecord::LegProbeAck {
                        session_id: contract.session_id,
                        leg_epoch_nonce: LegEpochNonce::from(contract.nonce),
                        probe_sequence,
                    },
                    contract.features,
                )
                .unwrap(),
            ),
        )
    }

    #[test]
    fn registered_b_exact_ack_is_delivery_proof_before_local_notice() {
        let RegisteredStandbyProbeScenario {
            registered,
            contract,
            client_b,
            mut owner_b,
            mut client_queue,
            mut transport,
        } = registered_standby_for_probe(2);
        let probe_sequence = ProbeSequence::new(7).unwrap();
        let pending = registered.begin_exact_probe(probe_sequence).unwrap();
        assert_eq!(
            format!("{pending:?}"),
            "PendingClientRegisteredStandbyProbe([REDACTED])"
        );
        let awaiting = pending.enqueue(&mut client_queue).unwrap();

        let wrong_sequence = ProbeSequence::new(8).unwrap();
        let wrong_ack =
            exact_standby_probe_ack(client_b.established_leg(), contract, wrong_sequence);
        let failure = awaiting
            .validate_exact_ack(wrong_ack, SimTime::from_nanos(10), SimTime::from_nanos(20))
            .unwrap_err();
        assert_eq!(failure.kind(), RegisteredStandbyProbeAckErrorKind::Rejected);
        let (awaiting, returned_wrong_ack) = failure.into_parts();
        assert!(matches!(returned_wrong_ack, BoundInbound::LegControl(_)));

        let exact_ack =
            exact_standby_probe_ack(client_b.established_leg(), contract, probe_sequence);
        let fresh = awaiting
            .validate_exact_ack(exact_ack, SimTime::from_nanos(10), SimTime::from_nanos(20))
            .unwrap();
        assert_eq!(fresh.fresh_until(), SimTime::from_nanos(20));
        assert!(fresh.is_fresh_at(SimTime::from_nanos(20)));
        assert!(
            fresh
                .begin_exact_next_attach_for_silent_loss(
                    SimTime::from_nanos(20),
                    AttachNonce::new([0x44; 16]).unwrap(),
                    &credentials(),
                )
                .is_ok()
        );

        // No local writer flush or DELIVERED publication was needed: the
        // exact peer ACK is the stronger causal delivery witness.
        let _unused_fixture_parts = (&mut owner_b, &mut client_queue, &mut transport);
    }

    #[test]
    fn registered_b_probe_arms_after_delivery_and_expiry_only_demotes_b() {
        let RegisteredStandbyProbeScenario {
            registered,
            contract,
            mut client_b,
            mut owner_b,
            mut client_queue,
            mut transport,
        } = registered_standby_for_probe(2);
        let pending = registered
            .begin_exact_probe(ProbeSequence::new(11).unwrap())
            .unwrap();
        let awaiting = pending.enqueue(&mut client_queue).unwrap();
        let failure = awaiting
            .arm_after_delivery(SimTime::from_nanos(100), Duration::from_nanos(50))
            .unwrap_err();
        assert_eq!(
            failure.kind(),
            RegisteredStandbyProbeArmErrorKind::NotDelivered
        );
        let awaiting = failure.into_awaiting();

        client_queue
            .try_flush(
                &mut client_b,
                &mut transport.controller_sender(),
                SimTime::ZERO,
            )
            .unwrap()
            .unwrap();
        release(&mut transport);
        let _probe = owner_b
            .receive_classified_delivery(
                transport
                    .try_recv_next(owner_b.inbound_route())
                    .unwrap()
                    .expect("the owner consumes the exact delivered probe"),
                contract.features,
            )
            .unwrap();
        let armed = awaiting
            .arm_after_delivery(SimTime::from_nanos(100), Duration::from_nanos(50))
            .unwrap();
        assert_eq!(armed.deadline(), SimTime::from_nanos(150));
        let armed = armed
            .expire_if_due(SimTime::from_nanos(149))
            .expect_err("deadline-1ns cannot consume the B probe authority");
        let registered = armed
            .expire_if_due(SimTime::from_nanos(150))
            .expect("the exact deadline demotes B without a session loss");
        assert!(
            registered
                .begin_exact_probe(ProbeSequence::new(12).unwrap())
                .is_ok()
        );
    }

    #[test]
    fn armed_registered_b_accepts_exact_deadline_ack_but_rejects_deadline_plus_one() {
        fn validate_at(received_at: SimTime) -> Option<RegisteredStandbyProbeAckErrorKind> {
            let RegisteredStandbyProbeScenario {
                registered,
                contract,
                mut client_b,
                mut owner_b,
                mut client_queue,
                mut transport,
            } = registered_standby_for_probe(2);
            let probe_sequence = ProbeSequence::new(12).unwrap();
            let awaiting = registered
                .begin_exact_probe(probe_sequence)
                .unwrap()
                .enqueue(&mut client_queue)
                .unwrap();
            client_queue
                .try_flush(
                    &mut client_b,
                    &mut transport.controller_sender(),
                    SimTime::ZERO,
                )
                .unwrap()
                .unwrap();
            release(&mut transport);
            let _probe = owner_b
                .receive_classified_delivery(
                    transport
                        .try_recv_next(owner_b.inbound_route())
                        .unwrap()
                        .expect("owner receives the exact armed probe"),
                    contract.features,
                )
                .unwrap();
            let armed = awaiting
                .arm_after_delivery(SimTime::from_nanos(100), Duration::from_nanos(50))
                .unwrap();
            let fresh_until = received_at.checked_add(Duration::from_nanos(200)).unwrap();
            armed
                .validate_exact_ack(
                    exact_standby_probe_ack(client_b.established_leg(), contract, probe_sequence),
                    received_at,
                    fresh_until,
                )
                .err()
                .map(|failure| failure.kind())
        }

        assert_eq!(validate_at(SimTime::from_nanos(150)), None);
        assert_eq!(
            validate_at(SimTime::from_nanos(151)),
            Some(RegisteredStandbyProbeAckErrorKind::DeadlineExpired),
        );
    }

    #[test]
    fn stale_registered_b_freshness_returns_the_exact_owner() {
        let RegisteredStandbyProbeScenario {
            registered,
            contract,
            mut client_b,
            mut owner_b,
            mut client_queue,
            mut transport,
        } = registered_standby_for_probe(2);
        let probe_sequence = ProbeSequence::new(9).unwrap();
        let awaiting = registered
            .begin_exact_probe(probe_sequence)
            .unwrap()
            .enqueue(&mut client_queue)
            .unwrap();
        client_queue
            .try_flush(
                &mut client_b,
                &mut transport.controller_sender(),
                SimTime::ZERO,
            )
            .unwrap()
            .unwrap();
        release(&mut transport);
        let _probe = owner_b
            .receive_classified_delivery(
                transport
                    .try_recv_next(owner_b.inbound_route())
                    .unwrap()
                    .unwrap(),
                contract.features,
            )
            .unwrap();
        let fresh = awaiting
            .validate_exact_ack(
                exact_standby_probe_ack(client_b.established_leg(), contract, probe_sequence),
                SimTime::from_nanos(10),
                SimTime::from_nanos(10),
            )
            .unwrap();

        let failure = fresh
            .begin_exact_next_attach_for_silent_loss(
                SimTime::from_nanos(11),
                AttachNonce::new([0x45; 16]).unwrap(),
                &credentials(),
            )
            .unwrap_err();
        assert_eq!(failure.kind(), FreshRegisteredAttachBeginErrorKind::Stale);
        let fresh = failure.into_fresh();
        assert!(fresh.into_registered().is_live());
    }

    #[test]
    fn registered_b_probe_queue_pressure_returns_the_full_pending_owner() {
        let RegisteredStandbyProbeScenario {
            registered,
            contract,
            client_b,
            owner_b,
            mut client_queue,
            transport,
        } = registered_standby_for_probe(1);
        let _keep_exact_endpoints_live = (&client_b, &owner_b, &transport);
        let seal = registered.seal.share();
        let _filler = client_queue
            .push_leg_control(
                &seal,
                contract.acceptance_frame().unwrap(),
                contract.features,
            )
            .unwrap();
        let failure = registered
            .begin_exact_probe(ProbeSequence::new(10).unwrap())
            .unwrap()
            .enqueue(&mut client_queue)
            .unwrap_err();
        assert!(matches!(
            failure.kind(),
            LegControlQueueErrorKind::CapacityExceeded { .. }
        ));
        assert!(failure.into_pending().into_registered().is_live());
    }

    struct DeliveredRegisteredAttachScenario {
        authority: AttachAuthority,
        model: crate::resumable::SessionModel,
        awaiting: AwaitingRegisteredAttach,
        acceptance: BoundInbound,
        io: LegIo,
        queue: LegOutboundQueue,
        reporter: LegTransportReporter,
    }

    fn delivered_registered_attach() -> DeliveredRegisteredAttachScenario {
        delivered_registered_attach_with_max_frames(4)
    }

    fn delivered_registered_attach_with_max_frames(
        max_frames: usize,
    ) -> DeliveredRegisteredAttachScenario {
        let (authority, mut model, client_active, owner_active) = active_pair();
        let (mut client_b, client_b_reporter) = LegIo::for_authenticated_transport_with_reporter(
            LegId::B,
            LegEndpointRole::Client,
            binding(0xc1),
            limits(),
        );
        let mut owner_b = LegIo::for_authenticated_transport(
            LegId::B,
            LegEndpointRole::Owner,
            binding(0xc1),
            limits(),
        );
        let mut client_b_queue =
            LegOutboundQueue::for_leg(client_b.established_leg(), max_frames, 4_096).unwrap();
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
            0x43,
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
        let awaiting = registered
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
        let received = owner_b
            .receive_delivery(
                transport_b
                    .try_recv_next(owner_b.inbound_route())
                    .unwrap()
                    .unwrap(),
            )
            .unwrap();
        let OwnerAttachTransaction::Installed { acceptance, .. } = owner_b
            .established_leg()
            .transact_owner_attach_for_provenance_test(received, &authority, &mut model)
            .unwrap()
            .unwrap()
        else {
            panic!("registered B must install generation 3");
        };
        DeliveredRegisteredAttachScenario {
            authority,
            model,
            awaiting,
            acceptance: BoundInbound::Session(
                client_b.established_leg().bind_received_frame(acceptance),
            ),
            io: client_b,
            queue: client_b_queue,
            reporter: client_b_reporter,
        }
    }

    struct CausallyAcceptedRegisteredAttachScenario {
        awaiting: AwaitingRegisteredAttach,
        acceptance: BoundInbound,
        io: LegIo,
        queue: LegOutboundQueue,
        delayed_delivery: OrderedDeliveryToken,
    }

    fn causally_accepted_registered_attach() -> CausallyAcceptedRegisteredAttachScenario {
        let (authority, mut model, client_active, owner_active) = active_pair();
        let mut client_b = LegIo::for_authenticated_transport(
            LegId::B,
            LegEndpointRole::Client,
            binding(0xc8),
            limits(),
        );
        let mut owner_b = LegIo::for_authenticated_transport(
            LegId::B,
            LegEndpointRole::Owner,
            binding(0xc8),
            limits(),
        );
        let mut client_b_queue =
            LegOutboundQueue::for_leg(client_b.established_leg(), 4, 4_096).unwrap();
        let mut owner_b_queue =
            LegOutboundQueue::for_leg(owner_b.established_leg(), 4, 4_096).unwrap();
        let mut registration_transport = transport();
        let (awaiting_registration, candidate) = deliver_registration(
            &client_active,
            &owner_active,
            &mut client_b,
            &mut owner_b,
            &mut client_b_queue,
            &mut registration_transport,
            &authority,
            0x48,
        );
        let mut slot = OwnerStandbySlot::new();
        slot.admit(candidate, &mut owner_b_queue).unwrap();
        let registered = awaiting_registration
            .validate(deliver_acceptance(
                &mut owner_b,
                &mut client_b,
                &mut owner_b_queue,
                &mut registration_transport,
            ))
            .unwrap();
        let awaiting = registered
            .begin_exact_next_attach(AttachNonce::new([0x49; 16]).unwrap(), &credentials())
            .unwrap()
            .enqueue(&mut client_b_queue)
            .unwrap();

        let mut withheld = WithheldAttachDelivery::new();
        client_b_queue
            .try_flush(&mut client_b, &mut withheld, SimTime::ZERO)
            .unwrap()
            .unwrap();
        assert!(!awaiting.request_receipt.was_delivered());
        let (route, lane, bytes) = withheld.submission.take().unwrap();
        let received = owner_b
            .receive_delivery(
                EncodedDelivery::from_memory_ingress(route, lane, bytes, 0, 0).unwrap(),
            )
            .unwrap();
        let OwnerAttachTransaction::Installed { acceptance, .. } = owner_b
            .established_leg()
            .transact_owner_attach_for_provenance_test(received, &authority, &mut model)
            .unwrap()
            .unwrap()
        else {
            panic!("registered B must install generation 3");
        };
        CausallyAcceptedRegisteredAttachScenario {
            awaiting,
            acceptance: BoundInbound::Session(
                client_b.established_leg().bind_received_frame(acceptance),
            ),
            io: client_b,
            queue: client_b_queue,
            delayed_delivery: withheld.completion.take().unwrap(),
        }
    }

    #[test]
    fn exact_accepted_response_commits_before_local_attach_delivery_notice() {
        let CausallyAcceptedRegisteredAttachScenario {
            awaiting,
            acceptance,
            io,
            queue,
            delayed_delivery,
        } = causally_accepted_registered_attach();
        assert!(!awaiting.request_receipt.was_delivered());

        let wrong_acceptance = BoundInbound::Session(
            io.established_leg().bind_received_frame(
                Frame::try_new(
                    LegGeneration::new(3).unwrap(),
                    Record::AttachAccepted {
                        session_id: SessionId::new([0x11; 16]).unwrap(),
                        nonce: AttachNonce::new([0x4a; 16]).unwrap(),
                        selected_version: SESSION_PROTOCOL_VERSION,
                        features: FeatureSet::new(FEATURES),
                    },
                )
                .unwrap(),
            ),
        );
        let failure = awaiting.validate_response(wrong_acceptance).unwrap_err();
        assert_eq!(failure.kind(), RegisteredAttachResponseErrorKind::Rejected);
        let (awaiting, returned_wrong_acceptance) = failure.into_parts();
        assert!(!awaiting.request_receipt.was_delivered());
        drop(returned_wrong_acceptance);

        let RegisteredAttachResponse::Accepted(accepted) =
            awaiting.validate_response(acceptance).unwrap()
        else {
            panic!("exact registered response changed kind");
        };
        assert!(accepted.request_queue_is_live());

        let mut controller = initial_client_controller();
        let started = controller
            .begin_registered_replacement(accepted, io, queue)
            .expect("the exact authenticated response is causal request-delivery proof");
        assert_eq!(started.generation, LegGeneration::new(3).unwrap());

        // The independently delayed local notice remains valid after commit;
        // it was never needed to forge or race the exact acceptance.
        delayed_delivery.mark_delivered();
        assert_eq!(controller.generation(), LegGeneration::new(3).unwrap());
    }

    pub(in crate::owned_upstream) struct AcceptedRegisteredClientTransportFixture {
        pub(in crate::owned_upstream) accepted: AcceptedClientRegisteredAttach,
        pub(in crate::owned_upstream) io: LegIo,
        pub(in crate::owned_upstream) queue: LegOutboundQueue,
        pub(in crate::owned_upstream) reporter: LegTransportReporter,
    }

    /// Closed test composition root for a genuinely registered B.  Sibling
    /// controller tests receive only the opaque accepted capability and its
    /// exact endpoint/FIFO; no raw attached leg or reducer candidate escapes.
    pub(in crate::owned_upstream) fn accepted_registered_client_transport_fixture(
        max_frames: usize,
    ) -> AcceptedRegisteredClientTransportFixture {
        let DeliveredRegisteredAttachScenario {
            awaiting,
            acceptance,
            io,
            queue,
            reporter,
            ..
        } = delivered_registered_attach_with_max_frames(max_frames);
        let RegisteredAttachResponse::Accepted(accepted) =
            awaiting.validate_response(acceptance).unwrap()
        else {
            panic!("registered B response changed kind");
        };
        AcceptedRegisteredClientTransportFixture {
            accepted,
            io,
            queue,
            reporter,
        }
    }

    fn accepted_registered_attach() -> (
        AcceptedClientRegisteredAttach,
        LegOutboundQueue,
        LegTransportReporter,
    ) {
        let DeliveredRegisteredAttachScenario {
            awaiting,
            acceptance,
            io: _io,
            queue,
            reporter,
            ..
        } = delivered_registered_attach();
        let RegisteredAttachResponse::Accepted(accepted) =
            awaiting.validate_response(acceptance).unwrap()
        else {
            panic!("registered B response changed kind");
        };
        (accepted, queue, reporter)
    }

    pub(in crate::owned_upstream) struct AcceptedRegisteredCatchUpClientTransportFixture {
        pub(in crate::owned_upstream) accepted: AcceptedClientRegisteredCatchUp,
        pub(in crate::owned_upstream) io: LegIo,
        pub(in crate::owned_upstream) queue: LegOutboundQueue,
        pub(in crate::owned_upstream) reporter: LegTransportReporter,
    }

    /// Closed test composition root for the complete lost-B, status-C, and
    /// accepted-D path. Sibling controller tests receive only the opaque
    /// catch-up acceptance and its exact endpoint/FIFO.
    pub(in crate::owned_upstream) fn accepted_registered_catch_up_client_transport_fixture(
        max_frames: usize,
    ) -> AcceptedRegisteredCatchUpClientTransportFixture {
        let DeliveredRegisteredAttachScenario {
            authority,
            mut model,
            awaiting,
            acceptance: _lost_acceptance,
            io: _client_b,
            queue: client_b_queue,
            reporter: client_b_reporter,
        } = delivered_registered_attach();
        let terminal = client_b_reporter
            .report(LegTransportTerminalReason::FatalIo)
            .unwrap();
        let retry = awaiting.into_status_retry_after_terminal(terminal).unwrap();
        drop(client_b_queue);

        let mut client_c = LegIo::for_authenticated_transport(
            LegId::A,
            LegEndpointRole::Client,
            binding(0xc3),
            limits(),
        );
        let mut owner_c = LegIo::for_authenticated_transport(
            LegId::A,
            LegEndpointRole::Owner,
            binding(0xc3),
            limits(),
        );
        let mut client_c_queue =
            LegOutboundQueue::for_leg(client_c.established_leg(), 4, 4_096).unwrap();
        let mut transport_c = transport();
        let awaiting_c = retry
            .bind_fresh_status_leg(client_c.established_leg(), &credentials())
            .unwrap()
            .enqueue(&mut client_c_queue)
            .unwrap();
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
        let status_frame = owner_c
            .established_leg()
            .authenticate_owner_resynchronization(received_c, &authority)
            .unwrap()
            .status_frame();
        let status = awaiting_c
            .validate_status(BoundInbound::Session(
                client_c.established_leg().bind_received_frame(status_frame),
            ))
            .unwrap();

        let (mut client_d, client_d_reporter) = LegIo::for_authenticated_transport_with_reporter(
            LegId::B,
            LegEndpointRole::Client,
            binding(0xc4),
            limits(),
        );
        let mut owner_d = LegIo::for_authenticated_transport(
            LegId::B,
            LegEndpointRole::Owner,
            binding(0xc4),
            limits(),
        );
        let mut client_d_queue =
            LegOutboundQueue::for_leg(client_d.established_leg(), max_frames, 4_096).unwrap();
        let mut transport_d = transport();
        let awaiting_d = status
            .begin_catch_up(
                client_d.established_leg(),
                AttachNonce::new([0x55; 16]).unwrap(),
                &credentials(),
            )
            .unwrap()
            .enqueue(&mut client_d_queue)
            .unwrap();
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
        let OwnerAttachTransaction::Installed { acceptance, .. } = owner_d
            .established_leg()
            .transact_owner_attach_for_provenance_test(received_d, &authority, &mut model)
            .unwrap()
            .unwrap()
        else {
            panic!("catch-up D must install generation 4");
        };
        let accepted = awaiting_d
            .validate_response(BoundInbound::Session(
                client_d.established_leg().bind_received_frame(acceptance),
            ))
            .unwrap();
        AcceptedRegisteredCatchUpClientTransportFixture {
            accepted,
            io: client_d,
            queue: client_d_queue,
            reporter: client_d_reporter,
        }
    }

    fn accepted_registered_catch_up() -> (
        AcceptedClientRegisteredCatchUp,
        LegOutboundQueue,
        LegTransportReporter,
    ) {
        let AcceptedRegisteredCatchUpClientTransportFixture {
            accepted,
            io,
            queue,
            reporter,
        } = accepted_registered_catch_up_client_transport_fixture(4);
        drop(io);
        (accepted, queue, reporter)
    }

    fn assert_status_retry_wire_correlation(
        authority: RegisteredAttachStatusRetryAuthority,
        expected_generation: u64,
        expected_nonce: u8,
    ) {
        let mut client = LegIo::for_authenticated_transport(
            LegId::A,
            LegEndpointRole::Client,
            binding(0xcf),
            limits(),
        );
        let mut owner = LegIo::for_authenticated_transport(
            LegId::A,
            LegEndpointRole::Owner,
            binding(0xcf),
            limits(),
        );
        let mut queue = LegOutboundQueue::for_leg(client.established_leg(), 4, 4_096).unwrap();
        let mut transport = transport();
        let _awaiting = authority
            .bind_fresh_status_leg(client.established_leg(), &credentials())
            .unwrap()
            .enqueue(&mut queue)
            .unwrap();
        queue
            .try_flush(
                &mut client,
                &mut transport.controller_sender(),
                SimTime::ZERO,
            )
            .unwrap()
            .unwrap();
        release(&mut transport);
        let received = owner
            .receive_delivery(
                transport
                    .try_recv_next(owner.inbound_route())
                    .unwrap()
                    .unwrap(),
            )
            .unwrap();
        let request = received.attach_request().unwrap();
        assert_eq!(request.requested_generation().get(), expected_generation);
        assert_eq!(
            request.nonce(),
            AttachNonce::new([expected_nonce; 16]).unwrap()
        );
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
        assert!(rejected.into_retry().is_some());
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
        assert!(rejected.into_retry().is_some());
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
                handoff
            } if handoff.generation() == LegGeneration::new(3).unwrap()
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
    fn accepted_registered_attach_terminal_recovers_exact_correlation_and_preserves_mismatch() {
        let (accepted, _queue, reporter) = accepted_registered_attach();
        let (_wrong_leg, wrong_reporter) =
            EstablishedLeg::for_authenticated_transport_with_reporter(binding(0xc2));
        let wrong_terminal = wrong_reporter
            .report(LegTransportTerminalReason::Reset)
            .unwrap();

        let failure = accepted
            .into_status_retry_after_terminal(wrong_terminal)
            .unwrap_err();
        assert_eq!(
            failure.kind(),
            RegisteredAttachStatusRetryErrorKind::WrongTerminal
        );
        let (accepted, returned_terminal) = failure.into_parts();
        assert_eq!(accepted.generation().get(), 3);
        assert_eq!(
            returned_terminal.reason(),
            LegTransportTerminalReason::Reset
        );

        let terminal = reporter
            .report(LegTransportTerminalReason::FatalIo)
            .unwrap();
        let retry = accepted.into_status_retry_after_terminal(terminal).unwrap();
        assert_status_retry_wire_correlation(retry, 3, 0x44);
    }

    #[test]
    fn accepted_registered_attach_queue_loss_recovers_only_after_exact_queue_is_gone() {
        let (accepted, queue, _reporter) = accepted_registered_attach();

        let failure = accepted.into_status_retry_after_queue_loss().unwrap_err();
        assert_eq!(
            failure.kind(),
            RegisteredAttachStatusRetryErrorKind::OriginalTransportStillLive
        );
        let accepted = failure.into_accepted();
        assert_eq!(accepted.generation().get(), 3);

        drop(queue);
        let retry = accepted.into_status_retry_after_queue_loss().unwrap();
        assert_status_retry_wire_correlation(retry, 3, 0x44);
    }

    #[test]
    fn accepted_registered_catch_up_terminal_recovers_original_b_and_preserves_mismatch() {
        let (accepted, _queue, reporter) = accepted_registered_catch_up();
        let (_wrong_leg, wrong_reporter) =
            EstablishedLeg::for_authenticated_transport_with_reporter(binding(0xc5));
        let wrong_terminal = wrong_reporter
            .report(LegTransportTerminalReason::Reset)
            .unwrap();

        let failure = accepted
            .into_status_retry_after_terminal(wrong_terminal)
            .unwrap_err();
        assert_eq!(
            failure.kind(),
            RegisteredAttachStatusRetryErrorKind::WrongTerminal
        );
        let (accepted, returned_terminal) = failure.into_parts();
        assert_eq!(accepted.generation().get(), 4);
        assert_eq!(
            returned_terminal.reason(),
            LegTransportTerminalReason::Reset
        );

        let terminal = reporter
            .report(LegTransportTerminalReason::FatalIo)
            .unwrap();
        let retry = accepted.into_status_retry_after_terminal(terminal).unwrap();
        assert_status_retry_wire_correlation(retry, 3, 0x44);
    }

    #[test]
    fn accepted_registered_catch_up_queue_loss_recovers_original_b_only_after_d_queue_loss() {
        let (accepted, queue, _reporter) = accepted_registered_catch_up();

        let failure = accepted.into_status_retry_after_queue_loss().unwrap_err();
        assert_eq!(
            failure.kind(),
            RegisteredAttachStatusRetryErrorKind::OriginalTransportStillLive
        );
        let accepted = failure.into_accepted();
        assert_eq!(accepted.generation().get(), 4);

        drop(queue);
        let retry = accepted.into_status_retry_after_queue_loss().unwrap();
        assert_status_retry_wire_correlation(retry, 3, 0x44);
    }

    #[test]
    fn accepted_registered_loss_typestates_redact_debug_payloads() {
        let (accepted, queue, _reporter) = accepted_registered_attach();
        let mut rendered = vec![format!("{accepted:?}")];
        let (_wrong_leg, wrong_reporter) =
            EstablishedLeg::for_authenticated_transport_with_reporter(binding(0xc6));
        let terminal = wrong_reporter
            .report(LegTransportTerminalReason::Reset)
            .unwrap();
        let failure = accepted
            .into_status_retry_after_terminal(terminal)
            .unwrap_err();
        rendered.push(format!("{failure:?}"));
        let (accepted, _terminal) = failure.into_parts();
        let failure = accepted.into_status_retry_after_queue_loss().unwrap_err();
        rendered.push(format!("{failure:?}"));
        let accepted = failure.into_accepted();
        drop((accepted, queue));

        let (accepted, queue, _reporter) = accepted_registered_catch_up();
        rendered.push(format!("{accepted:?}"));
        let (_wrong_leg, wrong_reporter) =
            EstablishedLeg::for_authenticated_transport_with_reporter(binding(0xc7));
        let terminal = wrong_reporter
            .report(LegTransportTerminalReason::PeerClosed)
            .unwrap();
        let failure = accepted
            .into_status_retry_after_terminal(terminal)
            .unwrap_err();
        rendered.push(format!("{failure:?}"));
        let (accepted, _terminal) = failure.into_parts();
        let failure = accepted.into_status_retry_after_queue_loss().unwrap_err();
        rendered.push(format!("{failure:?}"));
        let accepted = failure.into_accepted();
        drop((accepted, queue));

        for rendered in rendered {
            assert!(rendered.contains("[REDACTED]"));
            assert!(!rendered.contains("mini-vpn-owned/1"));
            assert!(!rendered.contains("4444444444444444"));
            assert!(!rendered.contains("5555555555555555"));
            assert!(!rendered.contains("7171717171717171"));
        }
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
        let _ = <RegisteredAttachCausalDeliveryGate as AmbiguousIfClone<_>>::marker;
        let _ = <AcceptedRegisteredLossGate as AmbiguousIfClone<_>>::marker;
        let _ = <RegisteredCatchUpBeginGate as AmbiguousIfClone<_>>::marker;
        let _ = <RegisteredCatchUpAssemblyGate as AmbiguousIfClone<_>>::marker;
        let _ = <RegisteredCatchUpResponseGate as AmbiguousIfClone<_>>::marker;
        let _ = <UninstalledRegisteredAttach as AmbiguousIfClone<_>>::marker;
        let _ = <UninstalledRegisteredCatchUp as AmbiguousIfClone<_>>::marker;
        let _ = <AcceptedClientRegisteredAttach as AmbiguousIfClone<_>>::marker;
        let _ = <AcceptedClientRegisteredCatchUp as AmbiguousIfClone<_>>::marker;
        let _ = <AcceptedRegisteredAttachTerminalRetryFailure as AmbiguousIfClone<_>>::marker;
        let _ = <AcceptedRegisteredAttachQueueLossRetryFailure as AmbiguousIfClone<_>>::marker;
        let _ = <AcceptedRegisteredCatchUpTerminalRetryFailure as AmbiguousIfClone<_>>::marker;
        let _ = <AcceptedRegisteredCatchUpQueueLossRetryFailure as AmbiguousIfClone<_>>::marker;
    };
}

#[cfg(test)]
#[path = "standby/tests/freshness.rs"]
mod freshness_tests;

#[cfg(test)]
#[path = "standby/tests/attach_probe_rollback.rs"]
mod attach_probe_rollback_tests;
