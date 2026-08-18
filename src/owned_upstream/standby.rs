//! Exact-leg standby registration for one already-installed resumable session.
//!
//! This module owns only the bounded registration typestate. It cannot commit
//! an attach generation, mutate the session reducer, or mint data-plane
//! authority.

use super::leg::{AttachedLeg, BoundInbound, EstablishedLeg, LegSeal, LegTransportEndpoint};
use super::leg_io::{LegControlEnqueued, LegControlQueueErrorKind, LegOutboundQueue};
use crate::resumable::{
    AttachAuthority, AttachCredentials, AttachTransportBinding, AuthenticatedStandbyRegistration,
    FeatureSet, LegControlFrame, LegControlRecord, LegGeneration, SessionId, StandbyNonce,
    StandbyRegistrationReject,
};
use std::fmt;
use std::sync::Weak;
use thiserror::Error;

fn endpoint_is_open(endpoint: &Weak<LegTransportEndpoint>) -> bool {
    endpoint
        .upgrade()
        .is_some_and(|endpoint| endpoint.is_open())
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
    contract: StandbyContract,
    receipt: LegControlEnqueued,
}

impl ClientRegisteredStandby {
    pub(crate) fn is_live(&self) -> bool {
        let _exact_identity = (&self.seal, self.contract);
        self.receipt.queue_is_live()
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
        let (authority, mut model, client_active, owner_active) = active_pair();
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

        let request = AttachRequest::new(
            SessionId::new([0x11; 16]).unwrap(),
            LegGeneration::new(3).unwrap(),
            AttachNonce::new([0x44; 16]).unwrap(),
            VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION).unwrap(),
            FeatureOffer::new(FEATURES, STANDBY_CONTROL_V1.bits()).unwrap(),
        );
        let pending_attach = client_b.established_leg().begin_attach(request);
        let proof = credentials().prove(&request, &binding(0x99)).unwrap();
        client_queue.push(request.to_attach_frame(proof)).unwrap();
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
        let OwnerAttachTransaction::Installed {
            attached,
            acceptance,
            recovery,
        } = owner_b
            .established_leg()
            .transact_owner_attach(received_attach, &authority, &mut model)
            .unwrap()
            .unwrap()
        else {
            panic!("exact B attach unexpectedly resynchronized");
        };
        assert!(matches!(
            recovery.as_slice(),
            [crate::resumable::SessionEffect::LegActivated { generation }]
                if *generation == LegGeneration::new(3).unwrap()
        ));
        let attach_receipt = owner_queue
            .push_attach_acceptance(&attached, acceptance)
            .unwrap();

        let registered_owner = slot.registered.as_ref().unwrap();
        let later_control = registered_owner
            .candidate
            .contract()
            .acceptance_frame()
            .unwrap();
        let error = owner_queue
            .push_leg_control(
                &registered_owner.candidate.seal,
                later_control,
                registered_owner.candidate.contract().features,
            )
            .unwrap_err();
        assert_eq!(
            error.kind(),
            &LegControlQueueErrorKind::AttachRecoveryPending
        );
        let later_control = error.into_frame();
        assert!(owner_queue.finish_attach_recovery(&attach_receipt));

        owner_queue
            .try_flush(
                &mut owner_b,
                &mut transport.controller_sender(),
                SimTime::ZERO,
            )
            .unwrap()
            .unwrap();
        owner_queue
            .push_leg_control(
                &registered_owner.candidate.seal,
                later_control,
                registered_owner.candidate.contract().features,
            )
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
            pending_attach.validate_response(accepted).unwrap(),
            AttachResponse::Accepted(_)
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
        assert!(registered.is_live());
        assert!(slot.registered_is_live());
        assert_eq!(authority.current_generation(), 3);
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
    };
}
