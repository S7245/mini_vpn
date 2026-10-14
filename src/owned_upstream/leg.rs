//! Typed provenance for frames received from one authenticated transport leg.
//!
//! Authenticated adapters mint EstablishedLeg after TLS and bind only that
//! connection's decoded frames. No Quinn, socket or 0-RTT behavior lives here.
#[path = "leg/cold_inventory.rs"]
mod cold_inventory;
use super::client_controller::ClientControllerInstallGate;
use super::leg_io::LegIoAuthenticatedTransportGate;
use super::standby::{
    AcceptedRegisteredLossGate, RegisteredAttachResponseGate, RegisteredCatchUpAssemblyGate,
    RegisteredCatchUpBeginGate, RegisteredCatchUpResponseGate, RegisteredPendingAttachAuthority,
};
use super::supervisor::{SessionSupervisor, SessionSupervisorError};
use crate::resumable::{
    AttachAuthority, AttachCredentials, AttachReject, AttachRequest, AttachTransportBinding,
    AuthenticatedAttachStatus, AuthenticatedStandbyRegistration, CommittedLeg, FeatureSet, Frame,
    GenerationCatchUp, GenerationResynchronization, LegControlFrame, LegEpochNonce, LegGeneration,
    PendingGenerationCatchUp, Record, SessionConfig, SessionEffect, SessionError, SessionEvent,
    SessionId, SessionModel, SessionRole, StandbyNonce, StandbyRegistrationReject,
    StandbyRegistrationRequest,
};
use crate::shared::TargetAddr;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, Weak};
use thiserror::Error;

fn same_stable_transport_identity(
    left: AttachTransportBinding,
    right: AttachTransportBinding,
) -> bool {
    left.owner_identity() == right.owner_identity()
        && left.alpn() == right.alpn()
        && left.device_principal() == right.device_principal()
}

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
    terminal_transition: Mutex<()>,
}

/// Exact endpoint lease used only by the closed client installation
/// transaction. Holding its guard excludes the sole terminal reporter across
/// queue reservation, reducer commit, old-leg retirement, and cursor publish.
pub(super) struct ClientInstallTransition {
    endpoint: Arc<LegTransportEndpoint>,
}

impl ClientInstallTransition {
    pub(super) fn lock(&self) -> Result<MutexGuard<'_, ()>, LegTerminalTransitionError> {
        self.endpoint.lock_terminal_transition()
    }

    pub(super) fn endpoint_is_open(&self) -> bool {
        self.endpoint.is_open()
    }
}

impl LegTransportEndpoint {
    fn open() -> Self {
        Self {
            terminal: AtomicBool::new(false),
            terminal_transition: Mutex::new(()),
        }
    }

    pub(super) fn is_open(&self) -> bool {
        !self.terminal.load(Ordering::Acquire)
    }

    pub(super) fn lock_terminal_transition(
        &self,
    ) -> Result<MutexGuard<'_, ()>, LegTerminalTransitionError> {
        self.terminal_transition.lock().map_err(|_| {
            self.terminal.store(true, Ordering::Release);
            LegTerminalTransitionError::Poisoned
        })
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
    LocalCancelled,
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
    pub(crate) fn report(
        self,
        reason: LegTransportTerminalReason,
    ) -> Result<ExactLegTerminal, LegTerminalTransitionError> {
        let _transition = self.endpoint.lock_terminal_transition()?;
        let was_terminal = self.endpoint.terminal.swap(true, Ordering::AcqRel);
        debug_assert!(
            !was_terminal,
            "the sole non-cloneable transport reporter cannot report twice"
        );
        drop(_transition);
        Ok(ExactLegTerminal {
            seal: self.seal,
            endpoint: self.endpoint,
            reason,
        })
    }
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub(crate) enum LegTerminalTransitionError {
    #[error("authenticated transport terminal transition lock is poisoned")]
    Poisoned,
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

    /// Read-only exact-transport check for a typestate that retained the
    /// authenticated seal and endpoint without retaining an AttachedLeg.
    /// This remains valid after the endpoint has transitioned terminal.
    pub(super) fn belongs_to_transport(
        &self,
        seal: &LegSeal,
        endpoint: &Weak<LegTransportEndpoint>,
    ) -> bool {
        seal.same_connection(&self.seal)
            && Weak::ptr_eq(endpoint, &Arc::downgrade(&self.endpoint))
            && !self.endpoint.is_open()
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
    fn authenticated_transport_parts(
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

    pub(super) fn for_leg_io_authenticated_transport_with_reporter(
        _gate: LegIoAuthenticatedTransportGate,
        binding: AttachTransportBinding,
    ) -> (Self, LegTransportReporter) {
        Self::authenticated_transport_parts(binding)
    }

    #[cfg(test)]
    pub(super) fn for_authenticated_transport_with_reporter(
        binding: AttachTransportBinding,
    ) -> (Self, LegTransportReporter) {
        Self::authenticated_transport_parts(binding)
    }

    /// Legacy construction is confined to deterministic tests. Production
    /// must retain the sole reporter returned by `LegIo` construction.
    #[cfg(test)]
    pub(super) fn for_authenticated_transport(binding: AttachTransportBinding) -> Self {
        Self::authenticated_transport_parts(binding).0
    }

    fn pending_attach(&self, request: AttachRequest) -> PendingAttach {
        PendingAttach {
            seal: self.seal.share(),
            endpoint: Arc::downgrade(&self.endpoint),
            binding: self.binding,
            request,
        }
    }

    /// Binds one signed initial ATTACH directly into its closed queue-admission
    /// typestate. Production callers never receive the intermediate raw
    /// validator that can mint a bare `AttachedLeg`.
    #[allow(
        clippy::result_large_err,
        reason = "a mismatched initial frame returns the exact request and signed frame"
    )]
    pub(crate) fn begin_initial_attach(
        &self,
        request: AttachRequest,
        frame: Frame,
    ) -> Result<PendingInitialAttach, InitialAttachBeginFailure> {
        let carried = match AttachRequest::from_attach_frame(&frame) {
            Ok((carried, _proof)) => carried,
            Err(_) => return Err(InitialAttachBeginFailure { request, frame }),
        };
        if carried != request {
            return Err(InitialAttachBeginFailure { request, frame });
        }
        let pending = self.pending_attach(request);
        Ok(PendingInitialAttach {
            queue_seal: pending.seal.share(),
            request,
            pending,
            frame,
        })
    }

    /// Raw attach construction remains only for protocol/provenance tests.
    /// Production initial, registered, status, and catch-up paths each expose
    /// their own queue-bound typestate instead.
    #[cfg(test)]
    pub(crate) fn begin_attach(&self, request: AttachRequest) -> PendingAttach {
        self.pending_attach(request)
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

    pub(super) fn lock_terminal_transition(
        &self,
    ) -> Result<MutexGuard<'_, ()>, LegTerminalTransitionError> {
        self.endpoint.lock_terminal_transition()
    }

    pub(super) fn transport_is_open(&self) -> bool {
        self.endpoint.is_open()
    }

    #[cfg(test)]
    pub(super) fn poison_terminal_transition_for_test(&self) {
        let endpoint = Arc::clone(&self.endpoint);
        let _caught = std::panic::catch_unwind(move || {
            let _guard = endpoint
                .terminal_transition
                .lock()
                .expect("test intentionally acquires the unpoisoned terminal lock");
            panic!("intentional terminal transition poison");
        });
    }

    /// Exact initial ATTACH returns opaque ownership to Supervisor. The pending
    /// bootstrap retains retry authority; post-CAS construction is infallible.
    pub(super) fn authenticate_initial_owner_attach(
        &self,
        received: LegBoundFrame,
        authority: &AttachAuthority,
    ) -> Result<AuthenticatedInitialOwnerAttach, LegProvenanceError> {
        self.authenticate_initial_owner_attach_preserving(received, authority)
            .map_err(InitialOwnerAttachAuthenticationFailure::into_kind)
    }

    /// Initial proof/seal rejection returns the original inbound capability.
    #[allow(
        clippy::result_large_err,
        reason = "authentication rejection returns the exact inbound frame capability"
    )]
    pub(super) fn authenticate_initial_owner_attach_preserving(
        &self,
        received: LegBoundFrame,
        authority: &AttachAuthority,
    ) -> Result<AuthenticatedInitialOwnerAttach, InitialOwnerAttachAuthenticationFailure> {
        if !self.seal.same_connection(&received.seal) {
            return Err(InitialOwnerAttachAuthenticationFailure::new(
                received,
                LegProvenanceError::WrongLeg,
            ));
        }

        let LegBoundFrame { seal, frame } = received;
        let committed = match authority.verify_and_commit_frame(&frame, &self.binding) {
            Ok(committed) => committed,
            Err(error) => {
                return Err(InitialOwnerAttachAuthenticationFailure::new(
                    LegBoundFrame { seal, frame },
                    error.into(),
                ));
            }
        };
        Ok(AuthenticatedInitialOwnerAttach {
            attached: AttachedLeg {
                seal,
                endpoint: Arc::downgrade(&self.endpoint),
                committed,
            },
            acceptance: committed.attach_accepted_frame(),
        })
    }

    /// Exact ATTACH preflight exclusively borrows Model through CAS/install.
    /// Recovery storage rejection returns the original inbound frame; installed
    /// capability/response/effects are published together without wire I/O.
    pub(super) fn transact_owner_attach_from_supervisor(
        &self,
        received: LegBoundFrame,
        authority: &AttachAuthority,
        model: &mut SessionModel,
    ) -> Result<Result<OwnerAttachTransaction, OwnerModelPreflightError>, LegProvenanceError> {
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
            Ok(Err(
                source @ (SessionError::EffectStorageLayout | SessionError::EffectStorageReserve),
            )) => Ok(Err(OwnerModelPreflightError::Storage {
                source,
                received: LegBoundFrame { seal, frame },
            })),
            Ok(Err(error)) => Ok(Err(OwnerModelPreflightError::Existing(error))),
            Err(resynchronization) => Ok(Ok(OwnerAttachTransaction::Resynchronize {
                status: resynchronization.status_frame(),
            })),
        }
    }

    /// Test-only byte/provenance floor with the same owning rejection contract.
    /// Production commits stay behind Supervisor/Target gates.
    #[cfg(test)]
    pub(crate) fn transact_owner_attach_for_provenance_test(
        &self,
        received: LegBoundFrame,
        authority: &AttachAuthority,
        model: &mut SessionModel,
    ) -> Result<Result<OwnerAttachTransaction, OwnerModelPreflightError>, LegProvenanceError> {
        self.transact_owner_attach_from_supervisor(received, authority, model)
    }

    /// Stale/exhausted ATTACH yields status only: no Model, CAS or attached leg.
    pub(super) fn authenticate_owner_resynchronization(
        &self,
        received: LegBoundFrame,
        authority: &AttachAuthority,
    ) -> Result<AuthenticatedAttachStatus, LegProvenanceError> {
        if !self.seal.same_connection(&received.seal) {
            return Err(LegProvenanceError::WrongLeg);
        }
        authority
            .authenticate_resynchronization_frame(&received.frame, &self.binding)
            .map_err(LegProvenanceError::from)
    }
}

/// Opaque, non-cloneable initial commit; only Supervisor consumes its model,
/// authority and response together, never a harness bypass or bare CommittedLeg.
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

pub(super) struct InitialOwnerAttachAuthenticationFailure {
    received: LegBoundFrame,
    kind: LegProvenanceError,
}

impl InitialOwnerAttachAuthenticationFailure {
    fn new(received: LegBoundFrame, kind: LegProvenanceError) -> Self {
        Self { received, kind }
    }

    fn into_kind(self) -> LegProvenanceError {
        self.kind
    }

    pub(super) fn into_parts(self) -> (LegBoundFrame, LegProvenanceError) {
        (self.received, self.kind)
    }
}

impl fmt::Debug for InitialOwnerAttachAuthenticationFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InitialOwnerAttachAuthenticationFailure")
            .field("kind", &self.kind)
            .field("received", &"[REDACTED]")
            .finish()
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
    /// Binds the signed bytes for the one initial client ATTACH to the exact
    /// pending validator that created them.  This is deliberately distinct
    /// from registered-standby admission: an initial transport has no prior
    /// standby registration capability to consume.
    #[allow(
        clippy::result_large_err,
        reason = "a mismatched signed frame returns both exact non-cloneable inputs"
    )]
    pub(crate) fn bind_initial_frame(
        self,
        frame: Frame,
    ) -> Result<PendingInitialAttach, PendingInitialAttachBindingFailure> {
        let carried = match AttachRequest::from_attach_frame(&frame) {
            Ok((request, _proof)) => request,
            Err(_) => {
                return Err(PendingInitialAttachBindingFailure {
                    pending: self,
                    frame,
                });
            }
        };
        if carried != self.request {
            return Err(PendingInitialAttachBindingFailure {
                pending: self,
                frame,
            });
        }
        Ok(PendingInitialAttach {
            queue_seal: self.seal.share(),
            request: self.request,
            pending: self,
            frame,
        })
    }

    pub(super) fn for_registered_standby(authority: RegisteredPendingAttachAuthority) -> Self {
        let (seal, endpoint, binding, request) = authority.into_parts();
        Self {
            seal,
            endpoint,
            binding,
            request,
        }
    }

    #[cfg(test)]
    pub(crate) fn validate_response(
        self,
        response: LegBoundFrame,
    ) -> Result<AttachResponse, LegProvenanceError> {
        self.validate_response_inner(response)
            .map_err(PendingAttachValidationFailure::into_kind)
    }

    /// Registered-only response validation. The private standby-minted gate
    /// proves this exact response is being reconciled with its retained
    /// request receipt before any raw leg value can be derived here.
    #[allow(
        clippy::result_large_err,
        reason = "rejected validation returns the exact pending attach and received frame"
    )]
    pub(super) fn validate_registered_response_preserving(
        self,
        response: LegBoundFrame,
        _gate: RegisteredAttachResponseGate,
    ) -> Result<RegisteredPendingAttachResponse, PendingAttachValidationFailure> {
        let status_retry_after_loss = AcceptedAttachStatusRetry {
            seal: self.seal.share(),
            endpoint: self.endpoint.clone(),
            binding: self.binding,
            request: self.request,
        };
        match self.validate_response_inner(response)? {
            AttachResponse::Accepted(attached) => Ok(RegisteredPendingAttachResponse::Accepted(
                UninstalledRegisteredAttach {
                    attached,
                    status_retry_after_loss,
                },
            )),
            AttachResponse::GenerationStatus(status) => {
                Ok(RegisteredPendingAttachResponse::GenerationStatus(status))
            }
        }
    }

    #[allow(
        clippy::result_large_err,
        reason = "rejected validation returns the exact pending attach and received frame"
    )]
    fn validate_response_inner(
        self,
        response: LegBoundFrame,
    ) -> Result<AttachResponse, PendingAttachValidationFailure> {
        if !self.seal.same_connection(&response.seal) {
            return Err(PendingAttachValidationFailure::new(
                self,
                response,
                LegProvenanceError::WrongLeg,
            ));
        }
        enum Validated {
            Accepted(CommittedLeg),
            GenerationStatus(GenerationResynchronization),
        }
        let validated = match response.frame.record() {
            Record::AttachAccepted { .. } => self
                .request
                .validate_accepted_frame(&self.binding, &response.frame)
                .map(Validated::Accepted),
            Record::AttachGenerationStatus { .. } => self
                .request
                .validate_generation_status_frame(&self.binding, &response.frame)
                .map(Validated::GenerationStatus),
            _ => Err(AttachReject::Rejected),
        };
        let validated = match validated {
            Ok(validated) => validated,
            Err(error) => {
                return Err(PendingAttachValidationFailure::new(
                    self,
                    response,
                    error.into(),
                ));
            }
        };
        let Self {
            seal,
            endpoint,
            binding: _,
            request,
        } = self;
        match validated {
            Validated::Accepted(committed) => Ok(AttachResponse::Accepted(AttachedLeg {
                seal,
                endpoint,
                committed,
            })),
            Validated::GenerationStatus(status) => {
                Ok(AttachResponse::GenerationStatus(LegGenerationStatus {
                    seal,
                    status,
                    status_request: request,
                }))
            }
        }
    }

    /// Validates only the initial correlated ATTACH_ACCEPTED branch while
    /// retaining both exact inputs on every rejection.  Initial bootstrap has
    /// no installed session from which a generation-status recovery could be
    /// authorized.
    #[allow(
        clippy::result_large_err,
        reason = "rejected validation returns the exact pending attach and received frame"
    )]
    pub(super) fn validate_initial_acceptance_preserving(
        self,
        response: LegBoundFrame,
    ) -> Result<AttachedLeg, PendingAttachValidationFailure> {
        if !self.seal.same_connection(&response.seal) {
            return Err(PendingAttachValidationFailure::new(
                self,
                response,
                LegProvenanceError::WrongLeg,
            ));
        }
        if !matches!(response.frame.record(), Record::AttachAccepted { .. }) {
            return Err(PendingAttachValidationFailure::new(
                self,
                response,
                LegProvenanceError::Rejected,
            ));
        }
        let committed = match self
            .request
            .validate_accepted_frame(&self.binding, &response.frame)
        {
            Ok(committed) => committed,
            Err(error) => {
                return Err(PendingAttachValidationFailure::new(
                    self,
                    response,
                    error.into(),
                ));
            }
        };
        let Self {
            seal,
            endpoint,
            binding: _,
            request: _,
        } = self;
        Ok(AttachedLeg {
            seal,
            endpoint,
            committed,
        })
    }

    /// Validates only the correlated generation-status branch while retaining
    /// both exact inputs on every rejection. A status-retry leg must never
    /// accept an ATTACH_ACCEPTED as a shortcut around the owner permit.
    #[allow(
        clippy::result_large_err,
        reason = "rejected validation returns the exact pending attach and received frame"
    )]
    pub(super) fn validate_registered_generation_status_preserving(
        self,
        response: LegBoundFrame,
        _gate: RegisteredAttachResponseGate,
    ) -> Result<LegGenerationStatus, PendingAttachValidationFailure> {
        if !self.seal.same_connection(&response.seal) {
            return Err(PendingAttachValidationFailure::new(
                self,
                response,
                LegProvenanceError::WrongLeg,
            ));
        }
        if !matches!(
            response.frame.record(),
            Record::AttachGenerationStatus { .. }
        ) {
            return Err(PendingAttachValidationFailure::new(
                self,
                response,
                LegProvenanceError::Rejected,
            ));
        }
        let status = match self
            .request
            .validate_generation_status_frame(&self.binding, &response.frame)
        {
            Ok(status) => status,
            Err(error) => {
                return Err(PendingAttachValidationFailure::new(
                    self,
                    response,
                    error.into(),
                ));
            }
        };
        let Self {
            seal,
            endpoint: _,
            binding: _,
            request,
        } = self;
        Ok(LegGenerationStatus {
            seal,
            status,
            status_request: request,
        })
    }

    /// Converts a delivered request into stale-retry authority only for the
    /// terminal fact minted by this exact authenticated transport.
    #[allow(
        clippy::result_large_err,
        reason = "a mismatch returns the exact pending request and terminal fact"
    )]
    pub(super) fn into_lost_after_terminal(
        self,
        terminal: ExactLegTerminal,
    ) -> Result<LostPendingAttach, PendingAttachTerminalMismatch> {
        if !self.seal.same_connection(&terminal.seal)
            || !Weak::ptr_eq(&self.endpoint, &Arc::downgrade(&terminal.endpoint))
            || terminal.endpoint.is_open()
        {
            return Err(PendingAttachTerminalMismatch {
                pending: self,
                terminal,
            });
        }
        Ok(LostPendingAttach {
            seal: self.seal,
            binding: self.binding,
            request: self.request,
        })
    }

    /// Converts a delivered request only after its once-claimed sole queue is
    /// provably gone. The old pending validator is consumed by this move.
    #[allow(
        clippy::result_large_err,
        reason = "a live queue rejection must return the exact non-cloneable pending attach"
    )]
    pub(super) fn into_lost_after_queue_loss(self) -> Result<LostPendingAttach, Self> {
        if !self.seal.outbound_queue_was_lost() {
            return Err(self);
        }
        Ok(LostPendingAttach {
            seal: self.seal,
            binding: self.binding,
            request: self.request,
        })
    }
}

/// Registered-only response result. Its accepted branch keeps the exact
/// delivered correlation private until either installation or proven loss.
pub(super) enum RegisteredPendingAttachResponse {
    Accepted(UninstalledRegisteredAttach),
    GenerationStatus(LegGenerationStatus),
}

/// Accepted registered B that has not yet crossed the client installation
/// transaction. The fallback and attached-leg authorities move together.
pub(super) struct UninstalledRegisteredAttach {
    attached: AttachedLeg,
    status_retry_after_loss: AcceptedAttachStatusRetry,
}

pub(super) enum ClientInstallTransaction<A, P, E> {
    Installed(P),
    PrepareRejected {
        accepted: A,
        error: E,
    },
    SupervisorRejected {
        accepted: A,
        error: SessionSupervisorError,
    },
}

impl UninstalledRegisteredAttach {
    pub(super) fn generation(&self) -> LegGeneration {
        self.attached.generation()
    }

    pub(super) fn install_transition(
        &self,
        _gate: &ClientControllerInstallGate,
    ) -> Option<ClientInstallTransition> {
        self.attached
            .endpoint
            .upgrade()
            .map(|endpoint| ClientInstallTransition { endpoint })
    }

    pub(super) fn commit_client_install<T, E, P>(
        self,
        supervisor: &mut SessionSupervisor,
        prepare: impl FnOnce(Vec<SessionEffect>) -> Result<T, E>,
        publish: impl FnOnce(ClientActiveLeg, T) -> P,
        _gate: &ClientControllerInstallGate,
    ) -> ClientInstallTransaction<Self, P, E> {
        let installed = match supervisor
            .install_prepared_client_replacement(self.attached.committed, prepare)
        {
            Ok(Ok(installed)) => installed,
            Ok(Err(error)) => {
                return ClientInstallTransaction::PrepareRejected {
                    accepted: self,
                    error,
                };
            }
            Err(error) => {
                return ClientInstallTransaction::SupervisorRejected {
                    accepted: self,
                    error,
                };
            }
        };
        let (prepared, commit) = installed;
        let Self {
            attached,
            status_retry_after_loss,
        } = self;
        drop((commit, status_retry_after_loss));
        let AttachedLeg {
            seal,
            endpoint,
            committed,
        } = attached;
        let active = ClientActiveLeg {
            seal,
            endpoint,
            committed,
        };
        ClientInstallTransaction::Installed(publish(active, prepared))
    }

    #[allow(
        clippy::result_large_err,
        reason = "wrong terminal returns the exact accepted capability and terminal fact"
    )]
    pub(super) fn into_status_retry_after_terminal(
        self,
        terminal: ExactLegTerminal,
        _gate: AcceptedRegisteredLossGate,
    ) -> Result<LostPendingAttach, UninstalledRegisteredAttachTerminalMismatch> {
        let Self {
            attached,
            status_retry_after_loss,
        } = self;
        match status_retry_after_loss.into_lost_after_terminal(terminal) {
            Ok(lost) => {
                drop(attached);
                Ok(lost)
            }
            Err(failure) => {
                let (status_retry_after_loss, terminal) = failure.into_parts();
                Err(UninstalledRegisteredAttachTerminalMismatch {
                    accepted: Self {
                        attached,
                        status_retry_after_loss,
                    },
                    terminal,
                })
            }
        }
    }

    #[allow(
        clippy::result_large_err,
        reason = "live queue returns the exact accepted capability"
    )]
    pub(super) fn into_status_retry_after_queue_loss(
        self,
        _gate: AcceptedRegisteredLossGate,
    ) -> Result<LostPendingAttach, Self> {
        let Self {
            attached,
            status_retry_after_loss,
        } = self;
        match status_retry_after_loss.into_lost_after_queue_loss() {
            Ok(lost) => {
                drop(attached);
                Ok(lost)
            }
            Err(status_retry_after_loss) => Err(Self {
                attached,
                status_retry_after_loss,
            }),
        }
    }
}

impl fmt::Debug for UninstalledRegisteredAttach {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("UninstalledRegisteredAttach([REDACTED])")
    }
}

pub(super) struct UninstalledRegisteredAttachTerminalMismatch {
    accepted: UninstalledRegisteredAttach,
    terminal: ExactLegTerminal,
}

impl UninstalledRegisteredAttachTerminalMismatch {
    pub(super) fn into_parts(self) -> (UninstalledRegisteredAttach, ExactLegTerminal) {
        (self.accepted, self.terminal)
    }
}

impl fmt::Debug for UninstalledRegisteredAttachTerminalMismatch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("UninstalledRegisteredAttachTerminalMismatch([REDACTED])")
    }
}

/// Exact immutable correlation retained only while an accepted registered
/// response remains uninstalled.
struct AcceptedAttachStatusRetry {
    seal: LegSeal,
    endpoint: Weak<LegTransportEndpoint>,
    binding: AttachTransportBinding,
    request: AttachRequest,
}

impl AcceptedAttachStatusRetry {
    #[allow(
        clippy::result_large_err,
        reason = "wrong terminal returns the exact fallback and terminal fact"
    )]
    fn into_lost_after_terminal(
        self,
        terminal: ExactLegTerminal,
    ) -> Result<LostPendingAttach, AcceptedAttachStatusRetryTerminalMismatch> {
        if !self.seal.same_connection(&terminal.seal)
            || !Weak::ptr_eq(&self.endpoint, &Arc::downgrade(&terminal.endpoint))
            || terminal.endpoint.is_open()
        {
            return Err(AcceptedAttachStatusRetryTerminalMismatch {
                retry: self,
                terminal,
            });
        }
        Ok(LostPendingAttach {
            seal: self.seal,
            binding: self.binding,
            request: self.request,
        })
    }

    #[allow(
        clippy::result_large_err,
        reason = "queue-loss mismatch returns the exact non-cloneable retry authority without boxing"
    )]
    fn into_lost_after_queue_loss(self) -> Result<LostPendingAttach, Self> {
        if !self.seal.outbound_queue_was_lost() {
            return Err(self);
        }
        Ok(LostPendingAttach {
            seal: self.seal,
            binding: self.binding,
            request: self.request,
        })
    }
}

impl fmt::Debug for AcceptedAttachStatusRetry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AcceptedAttachStatusRetry([REDACTED])")
    }
}

struct AcceptedAttachStatusRetryTerminalMismatch {
    retry: AcceptedAttachStatusRetry,
    terminal: ExactLegTerminal,
}

impl AcceptedAttachStatusRetryTerminalMismatch {
    fn into_parts(self) -> (AcceptedAttachStatusRetry, ExactLegTerminal) {
        (self.retry, self.terminal)
    }
}

impl fmt::Debug for AcceptedAttachStatusRetryTerminalMismatch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AcceptedAttachStatusRetryTerminalMismatch([REDACTED])")
    }
}

/// Exact delivered ATTACH whose original transport can no longer validate a
/// response. It may only re-sign the same immutable request for one fresh
/// status-only transport.
pub(super) struct LostPendingAttach {
    seal: LegSeal,
    binding: AttachTransportBinding,
    request: AttachRequest,
}

impl LostPendingAttach {
    #[allow(
        clippy::result_large_err,
        reason = "fresh-leg rejection returns the exact lost request authority"
    )]
    pub(super) fn bind_fresh_status_leg(
        self,
        leg: &EstablishedLeg,
        credentials: &AttachCredentials,
    ) -> Result<SignedStatusRetryAttach, LostPendingAttachRebindFailure> {
        if self.seal.same_connection(&leg.seal)
            || self.binding == leg.binding
            || !same_stable_transport_identity(self.binding, leg.binding)
            || !leg.transport_is_open()
            || leg.seal.outbound_queue_was_lost()
        {
            return Err(LostPendingAttachRebindFailure {
                lost: self,
                kind: LegProvenanceError::Rejected,
            });
        }
        let proof = match credentials.prove(&self.request, &leg.binding) {
            Ok(proof) => proof,
            Err(_error) => {
                return Err(LostPendingAttachRebindFailure {
                    lost: self,
                    kind: LegProvenanceError::Rejected,
                });
            }
        };
        let request = self.request;
        Ok(SignedStatusRetryAttach {
            queue_seal: leg.seal.share(),
            pending: PendingAttach {
                seal: leg.seal.share(),
                endpoint: Arc::downgrade(&leg.endpoint),
                binding: leg.binding,
                request,
            },
            request,
            frame: request.to_attach_frame(proof),
        })
    }
}

impl fmt::Debug for LostPendingAttach {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("LostPendingAttach([REDACTED])")
    }
}

pub(super) struct LostPendingAttachRebindFailure {
    lost: LostPendingAttach,
    kind: LegProvenanceError,
}

impl LostPendingAttachRebindFailure {
    pub(super) fn into_parts(self) -> (LostPendingAttach, LegProvenanceError) {
        (self.lost, self.kind)
    }
}

impl fmt::Debug for LostPendingAttachRebindFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LostPendingAttachRebindFailure")
            .field("kind", &self.kind)
            .field("lost", &"[REDACTED]")
            .finish()
    }
}

pub(super) struct SignedStatusRetryAttach {
    queue_seal: LegSeal,
    pending: PendingAttach,
    request: AttachRequest,
    frame: Frame,
}

impl SignedStatusRetryAttach {
    pub(super) fn into_parts(self) -> (LegSeal, PendingAttach, AttachRequest, Frame) {
        (self.queue_seal, self.pending, self.request, self.frame)
    }
}

impl fmt::Debug for SignedStatusRetryAttach {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SignedStatusRetryAttach([REDACTED])")
    }
}

pub(super) struct PendingAttachTerminalMismatch {
    pending: PendingAttach,
    terminal: ExactLegTerminal,
}

impl PendingAttachTerminalMismatch {
    pub(super) fn into_parts(self) -> (PendingAttach, ExactLegTerminal) {
        (self.pending, self.terminal)
    }
}

impl fmt::Debug for PendingAttachTerminalMismatch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PendingAttachTerminalMismatch([REDACTED])")
    }
}

/// Ownership-preserving response rejection used by registered-standby
/// typestate. Legacy callers may discard it through `validate_response`, but
/// C1b never loses either side of the exact correlation.
pub(super) struct PendingAttachValidationFailure {
    pending: PendingAttach,
    response: LegBoundFrame,
    kind: LegProvenanceError,
}

impl PendingAttachValidationFailure {
    fn new(pending: PendingAttach, response: LegBoundFrame, kind: LegProvenanceError) -> Self {
        Self {
            pending,
            response,
            kind,
        }
    }

    fn into_kind(self) -> LegProvenanceError {
        self.kind
    }

    pub(super) fn into_parts(self) -> (PendingAttach, LegBoundFrame, LegProvenanceError) {
        (self.pending, self.response, self.kind)
    }
}

impl fmt::Debug for PendingAttachValidationFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PendingAttachValidationFailure")
            .field("kind", &self.kind)
            .field("ownership", &"[REDACTED]")
            .finish()
    }
}

impl fmt::Debug for PendingAttach {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PendingAttach([REDACTED])")
    }
}

/// Initial client ATTACH retained until the exact transport's sole queue
/// admits its signed bytes.  It is neither a registered-standby request nor a
/// generic session frame, and it exposes no raw request/proof accessors.
pub(crate) struct PendingInitialAttach {
    pub(super) queue_seal: LegSeal,
    pub(super) pending: PendingAttach,
    pub(super) request: AttachRequest,
    pub(super) frame: Frame,
}

impl PendingInitialAttach {
    pub(crate) fn encoded_len(&self) -> Result<usize, crate::resumable::ProtocolError> {
        self.frame.encode().map(|encoded| encoded.len())
    }
}

impl fmt::Debug for PendingInitialAttach {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PendingInitialAttach([REDACTED])")
    }
}

/// Exact binding rejection for a signed initial frame that does not carry the
/// pending validator's immutable request.
pub(crate) struct PendingInitialAttachBindingFailure {
    pending: PendingAttach,
    frame: Frame,
}

/// Exact input return from the closed initial constructor. It deliberately
/// exposes only the caller-owned request and signed frame, never the raw
/// validator capable of minting a bare attached leg.
pub(crate) struct InitialAttachBeginFailure {
    request: AttachRequest,
    frame: Frame,
}

impl InitialAttachBeginFailure {
    pub(crate) fn into_parts(self) -> (AttachRequest, Frame) {
        (self.request, self.frame)
    }
}

impl fmt::Debug for InitialAttachBeginFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("InitialAttachBeginFailure([REDACTED])")
    }
}

impl PendingInitialAttachBindingFailure {
    pub(crate) fn into_parts(self) -> (PendingAttach, Frame) {
        (self.pending, self.frame)
    }
}

impl fmt::Debug for PendingInitialAttachBindingFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PendingInitialAttachBindingFailure([REDACTED])")
    }
}

/// New storage failures retain the exact frame; existing errors keep old mapping.
#[derive(Debug)]
pub(crate) enum OwnerModelPreflightError {
    Existing(SessionError),
    Storage {
        source: SessionError,
        received: LegBoundFrame,
    },
}

/// A decoded frame whose provenance was minted by the exact connection's
/// sole receive adapter.  Fields and constructors remain private.
pub(crate) struct LegBoundFrame {
    seal: LegSeal,
    frame: Frame,
}

impl LegBoundFrame {
    pub(super) fn belongs_to_transport(&self, leg: &EstablishedLeg) -> bool {
        leg.belongs_to_transport(&self.seal)
    }

    pub(super) fn attach_request(&self) -> Result<AttachRequest, LegProvenanceError> {
        AttachRequest::from_attach_frame(&self.frame)
            .map(|(request, _proof)| request)
            .map_err(LegProvenanceError::from)
    }

    #[cfg(test)]
    pub(crate) fn into_frame_for_test(self) -> Frame {
        self.frame
    }
}

/// Classified inbound facts retain disjoint protocol namespaces after exact
/// transport provenance is minted. Leg-control facts cannot be passed to the
/// session reducer without an explicit impossible type conversion.
pub(crate) enum BoundInbound {
    Session(LegBoundFrame),
    LegControl(LegBoundControlFrame),
}

/// Read-only responder source minted only from an authenticated installed
/// current leg. It shares exact transport identity and protocol correlation,
/// but carries no reducer, liveness, or switch authority.
pub(super) struct InstalledLegProbeSource {
    seal: LegSeal,
    endpoint: Weak<LegTransportEndpoint>,
    session_id: SessionId,
    generation: LegGeneration,
    epoch_nonce: LegEpochNonce,
    features: FeatureSet,
}

impl InstalledLegProbeSource {
    pub(super) fn into_parts(
        self,
    ) -> (
        LegSeal,
        Weak<LegTransportEndpoint>,
        SessionId,
        LegGeneration,
        LegEpochNonce,
        FeatureSet,
    ) {
        (
            self.seal,
            self.endpoint,
            self.session_id,
            self.generation,
            self.epoch_nonce,
            self.features,
        )
    }
}

impl fmt::Debug for InstalledLegProbeSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("InstalledLegProbeSource([REDACTED])")
    }
}

/// One decoded leg-control frame bound to the exact authenticated connection
/// that carried its bytes. Construction remains private to [`EstablishedLeg`].
pub(crate) struct LegBoundControlFrame {
    seal: LegSeal,
    frame: LegControlFrame,
}

impl LegBoundControlFrame {
    pub(super) fn parts(&self) -> (&LegSeal, &LegControlFrame) {
        (&self.seal, &self.frame)
    }

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
    status_request: AttachRequest,
}

impl LegGenerationStatus {
    pub(crate) fn current_generation(&self) -> LegGeneration {
        self.status.current_generation()
    }

    pub(crate) fn requested_generation(&self) -> LegGeneration {
        self.status.requested_generation()
    }

    #[cfg(test)]
    pub(crate) fn transport_binding(&self) -> AttachTransportBinding {
        self.status.transport_binding()
    }

    #[cfg(test)]
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
            status_request: self.status_request,
            pending,
        })
    }

    /// Consumes the exact status and internally derives/signs its sole
    /// high-water successor for a distinct fresh transport. No raw request or
    /// binding leaves this adapter boundary.
    #[allow(
        clippy::result_large_err,
        reason = "begin rejection returns the exact status capability"
    )]
    pub(super) fn begin_registered_signed_catch_up(
        self,
        follow_up_leg: &EstablishedLeg,
        nonce: crate::resumable::AttachNonce,
        credentials: &AttachCredentials,
        _gate: RegisteredCatchUpBeginGate,
    ) -> Result<SignedPendingCatchUpAttach, LegGenerationCatchUpBeginFailure> {
        if self.seal.same_connection(&follow_up_leg.seal)
            || self.status.transport_binding() == follow_up_leg.binding
            || !same_stable_transport_identity(
                self.status.transport_binding(),
                follow_up_leg.binding,
            )
            || !follow_up_leg.transport_is_open()
            || follow_up_leg.seal.outbound_queue_was_lost()
        {
            return Err(LegGenerationCatchUpBeginFailure {
                status: self,
                kind: LegProvenanceError::Rejected,
            });
        }
        let pending = match self.status.begin_catch_up(nonce) {
            Ok(pending) => pending,
            Err(_error) => {
                return Err(LegGenerationCatchUpBeginFailure {
                    status: self,
                    kind: LegProvenanceError::Rejected,
                });
            }
        };
        let request = pending.request();
        let proof = match credentials.prove(&request, &follow_up_leg.binding) {
            Ok(proof) => proof,
            Err(_error) => {
                return Err(LegGenerationCatchUpBeginFailure {
                    status: self,
                    kind: LegProvenanceError::Rejected,
                });
            }
        };
        Ok(SignedPendingCatchUpAttach {
            queue_seal: follow_up_leg.seal.share(),
            pending: PendingCatchUpAttach {
                _status_seal: self.seal,
                follow_up_seal: follow_up_leg.seal.share(),
                follow_up_endpoint: follow_up_leg.endpoint_liveness(),
                follow_up_binding: follow_up_leg.binding,
                status_request: self.status_request,
                pending,
            },
            request,
            frame: request.to_attach_frame(proof),
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
    status_request: AttachRequest,
    pending: PendingGenerationCatchUp,
}

impl PendingCatchUpAttach {
    #[cfg(test)]
    pub(crate) fn request(&self) -> AttachRequest {
        self.pending.request()
    }

    #[cfg(test)]
    pub(crate) fn transport_binding(&self) -> AttachTransportBinding {
        self.follow_up_binding
    }

    #[cfg(test)]
    pub(crate) fn validate_response(
        self,
        response: LegBoundFrame,
    ) -> Result<CaughtUpAttachedLeg, LegProvenanceError> {
        self.validate_response_inner(response)
            .map_err(PendingCatchUpValidationFailure::into_kind)
    }

    /// Registered-only D acceptance validation. The private delivery gate is
    /// minted only while consuming the opaque awaiting wrapper that retains
    /// both C and D queue receipts.
    #[allow(
        clippy::result_large_err,
        reason = "wrong response returns exact catch-up and inbound ownership"
    )]
    pub(super) fn validate_registered_response_preserving(
        self,
        response: LegBoundFrame,
        _gate: RegisteredCatchUpResponseGate,
    ) -> Result<UninstalledRegisteredCatchUp, PendingCatchUpValidationFailure> {
        let status_retry_after_loss = AcceptedAttachStatusRetry {
            seal: self.follow_up_seal.share(),
            endpoint: self.follow_up_endpoint.clone(),
            binding: self.follow_up_binding,
            request: self.status_request,
        };
        let attached = self.validate_response_inner(response)?;
        Ok(UninstalledRegisteredCatchUp {
            attached,
            status_retry_after_loss,
        })
    }

    #[allow(
        clippy::result_large_err,
        reason = "wrong response returns exact catch-up and inbound ownership"
    )]
    fn validate_response_inner(
        self,
        response: LegBoundFrame,
    ) -> Result<CaughtUpAttachedLeg, PendingCatchUpValidationFailure> {
        if !self.follow_up_seal.same_connection(&response.seal) {
            return Err(PendingCatchUpValidationFailure::new(
                self,
                response,
                LegProvenanceError::WrongLeg,
            ));
        }
        if !matches!(response.frame.record(), Record::AttachAccepted { .. }) {
            return Err(PendingCatchUpValidationFailure::new(
                self,
                response,
                LegProvenanceError::Rejected,
            ));
        }
        let Self {
            _status_seal,
            follow_up_seal,
            follow_up_endpoint,
            follow_up_binding,
            status_request,
            pending,
        } = self;
        let catch_up =
            match pending.validate_accepted_frame_preserving(&follow_up_binding, &response.frame) {
                Ok(catch_up) => catch_up,
                Err((pending, error)) => {
                    return Err(PendingCatchUpValidationFailure::new(
                        Self {
                            _status_seal,
                            follow_up_seal,
                            follow_up_endpoint,
                            follow_up_binding,
                            status_request,
                            pending,
                        },
                        response,
                        error.into(),
                    ));
                }
            };
        Ok(CaughtUpAttachedLeg {
            seal: follow_up_seal,
            endpoint: follow_up_endpoint,
            catch_up,
        })
    }

    /// Converts a delivered, lost follow-up request into authority to retry
    /// the original stale correlation on one fresh status transport. The
    /// original request remains inside `PendingGenerationCatchUp`; callers
    /// never receive a raw request or binding.
    #[allow(
        clippy::result_large_err,
        reason = "a mismatch returns the exact catch-up validator and terminal fact"
    )]
    pub(super) fn into_original_lost_after_terminal(
        self,
        terminal: ExactLegTerminal,
    ) -> Result<LostPendingAttach, PendingCatchUpTerminalMismatch> {
        if !self.follow_up_seal.same_connection(&terminal.seal)
            || !Weak::ptr_eq(
                &self.follow_up_endpoint,
                &Arc::downgrade(&terminal.endpoint),
            )
            || terminal.endpoint.is_open()
        {
            return Err(PendingCatchUpTerminalMismatch {
                pending: self,
                terminal,
            });
        }
        let Self {
            _status_seal: _,
            follow_up_seal,
            follow_up_endpoint: _,
            follow_up_binding,
            status_request,
            pending: _,
        } = self;
        Ok(LostPendingAttach {
            seal: follow_up_seal,
            binding: follow_up_binding,
            request: status_request,
        })
    }

    /// Queue-loss counterpart to [`Self::into_original_lost_after_terminal`].
    /// The once-claimed D queue must be gone before its validator can be
    /// converted into another status-only retry.
    #[allow(
        clippy::result_large_err,
        reason = "a live queue rejection returns the exact catch-up validator"
    )]
    pub(super) fn into_original_lost_after_queue_loss(self) -> Result<LostPendingAttach, Self> {
        if !self.follow_up_seal.outbound_queue_was_lost() {
            return Err(self);
        }
        let Self {
            _status_seal: _,
            follow_up_seal,
            follow_up_endpoint: _,
            follow_up_binding,
            status_request,
            pending: _,
        } = self;
        Ok(LostPendingAttach {
            seal: follow_up_seal,
            binding: follow_up_binding,
            request: status_request,
        })
    }
}

/// Accepted registered D that remains outside the client installation
/// transaction. Proven D loss recovers only the original stale B correlation.
pub(super) struct UninstalledRegisteredCatchUp {
    attached: CaughtUpAttachedLeg,
    status_retry_after_loss: AcceptedAttachStatusRetry,
}

impl UninstalledRegisteredCatchUp {
    pub(super) fn generation(&self) -> LegGeneration {
        self.attached.generation()
    }

    pub(super) fn install_transition(
        &self,
        _gate: &ClientControllerInstallGate,
    ) -> Option<ClientInstallTransition> {
        self.attached
            .endpoint
            .upgrade()
            .map(|endpoint| ClientInstallTransition { endpoint })
    }

    pub(super) fn commit_client_install<T, E, P>(
        self,
        supervisor: &mut SessionSupervisor,
        prepare: impl FnOnce(Vec<SessionEffect>) -> Result<T, E>,
        publish: impl FnOnce(ClientActiveLeg, T) -> P,
        _gate: &ClientControllerInstallGate,
    ) -> ClientInstallTransaction<Self, P, E> {
        let installed =
            match supervisor.install_prepared_client_catch_up(self.attached.catch_up, prepare) {
                Ok(Ok(installed)) => installed,
                Ok(Err(error)) => {
                    return ClientInstallTransaction::PrepareRejected {
                        accepted: self,
                        error,
                    };
                }
                Err(error) => {
                    return ClientInstallTransaction::SupervisorRejected {
                        accepted: self,
                        error,
                    };
                }
            };
        let (prepared, commit) = installed;
        let Self {
            attached,
            status_retry_after_loss,
        } = self;
        let CaughtUpAttachedLeg {
            seal,
            endpoint,
            catch_up,
        } = attached;
        let committed = catch_up.accepted_leg();
        drop((commit, status_retry_after_loss));
        let active = ClientActiveLeg {
            seal,
            endpoint,
            committed,
        };
        ClientInstallTransaction::Installed(publish(active, prepared))
    }

    #[allow(
        clippy::result_large_err,
        reason = "wrong terminal returns the exact accepted catch-up and terminal fact"
    )]
    pub(super) fn into_status_retry_after_terminal(
        self,
        terminal: ExactLegTerminal,
        _gate: AcceptedRegisteredLossGate,
    ) -> Result<LostPendingAttach, UninstalledRegisteredCatchUpTerminalMismatch> {
        let Self {
            attached,
            status_retry_after_loss,
        } = self;
        match status_retry_after_loss.into_lost_after_terminal(terminal) {
            Ok(lost) => {
                drop(attached);
                Ok(lost)
            }
            Err(failure) => {
                let (status_retry_after_loss, terminal) = failure.into_parts();
                Err(UninstalledRegisteredCatchUpTerminalMismatch {
                    accepted: Self {
                        attached,
                        status_retry_after_loss,
                    },
                    terminal,
                })
            }
        }
    }

    #[allow(
        clippy::result_large_err,
        reason = "live D queue returns the exact accepted catch-up"
    )]
    pub(super) fn into_status_retry_after_queue_loss(
        self,
        _gate: AcceptedRegisteredLossGate,
    ) -> Result<LostPendingAttach, Self> {
        let Self {
            attached,
            status_retry_after_loss,
        } = self;
        match status_retry_after_loss.into_lost_after_queue_loss() {
            Ok(lost) => {
                drop(attached);
                Ok(lost)
            }
            Err(status_retry_after_loss) => Err(Self {
                attached,
                status_retry_after_loss,
            }),
        }
    }
}

impl fmt::Debug for UninstalledRegisteredCatchUp {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("UninstalledRegisteredCatchUp([REDACTED])")
    }
}

pub(super) struct UninstalledRegisteredCatchUpTerminalMismatch {
    accepted: UninstalledRegisteredCatchUp,
    terminal: ExactLegTerminal,
}

impl UninstalledRegisteredCatchUpTerminalMismatch {
    pub(super) fn into_parts(self) -> (UninstalledRegisteredCatchUp, ExactLegTerminal) {
        (self.accepted, self.terminal)
    }
}

impl fmt::Debug for UninstalledRegisteredCatchUpTerminalMismatch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("UninstalledRegisteredCatchUpTerminalMismatch([REDACTED])")
    }
}

pub(super) struct PendingCatchUpTerminalMismatch {
    pending: PendingCatchUpAttach,
    terminal: ExactLegTerminal,
}

impl PendingCatchUpTerminalMismatch {
    pub(super) fn into_parts(self) -> (PendingCatchUpAttach, ExactLegTerminal) {
        (self.pending, self.terminal)
    }
}

impl fmt::Debug for PendingCatchUpTerminalMismatch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PendingCatchUpTerminalMismatch([REDACTED])")
    }
}

pub(super) struct SignedPendingCatchUpAttach {
    queue_seal: LegSeal,
    pending: PendingCatchUpAttach,
    request: AttachRequest,
    frame: Frame,
}

impl SignedPendingCatchUpAttach {
    pub(super) fn into_registered_parts(
        self,
        _gate: RegisteredCatchUpAssemblyGate,
    ) -> (LegSeal, PendingCatchUpAttach, AttachRequest, Frame) {
        (self.queue_seal, self.pending, self.request, self.frame)
    }
}

impl fmt::Debug for SignedPendingCatchUpAttach {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SignedPendingCatchUpAttach([REDACTED])")
    }
}

pub(super) struct LegGenerationCatchUpBeginFailure {
    status: LegGenerationStatus,
    kind: LegProvenanceError,
}

impl LegGenerationCatchUpBeginFailure {
    pub(super) fn into_parts(self) -> (LegGenerationStatus, LegProvenanceError) {
        (self.status, self.kind)
    }
}

impl fmt::Debug for LegGenerationCatchUpBeginFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LegGenerationCatchUpBeginFailure")
            .field("kind", &self.kind)
            .field("status", &"[REDACTED]")
            .finish()
    }
}

pub(super) struct PendingCatchUpValidationFailure {
    pending: PendingCatchUpAttach,
    response: LegBoundFrame,
    kind: LegProvenanceError,
}

impl PendingCatchUpValidationFailure {
    fn new(
        pending: PendingCatchUpAttach,
        response: LegBoundFrame,
        kind: LegProvenanceError,
    ) -> Self {
        Self {
            pending,
            response,
            kind,
        }
    }

    fn into_kind(self) -> LegProvenanceError {
        self.kind
    }

    pub(super) fn into_parts(self) -> (PendingCatchUpAttach, LegBoundFrame, LegProvenanceError) {
        (self.pending, self.response, self.kind)
    }
}

impl fmt::Debug for PendingCatchUpValidationFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PendingCatchUpValidationFailure")
            .field("kind", &self.kind)
            .field("ownership", &"[REDACTED]")
            .finish()
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

/// Controller-owned installed client leg. Normal and catch-up acceptances are
/// normalized here so both retain the full local-open, receive, terminal, and
/// retirement surface.
pub(crate) struct ClientActiveLeg {
    seal: LegSeal,
    endpoint: Weak<LegTransportEndpoint>,
    committed: CommittedLeg,
}

impl ClientActiveLeg {
    pub(super) fn from_initial(attached: AttachedLeg, _gate: ClientControllerInstallGate) -> Self {
        let AttachedLeg {
            seal,
            endpoint,
            committed,
        } = attached;
        Self {
            seal,
            endpoint,
            committed,
        }
    }

    pub(crate) fn generation(&self) -> LegGeneration {
        self.committed.generation()
    }

    pub(super) fn committed_leg(&self) -> CommittedLeg {
        self.committed
    }

    pub(super) fn probe_session_id(&self) -> crate::resumable::SessionId {
        self.committed.session_id()
    }

    pub(super) fn probe_epoch_nonce(&self) -> crate::resumable::LegEpochNonce {
        self.committed.nonce().into()
    }

    pub(super) fn share_probe_seal(&self) -> LegSeal {
        self.seal.share()
    }

    pub(super) fn installed_leg_probe_source(&self) -> InstalledLegProbeSource {
        InstalledLegProbeSource {
            seal: self.seal.share(),
            endpoint: self.endpoint.clone(),
            session_id: self.committed.session_id(),
            generation: self.committed.generation(),
            epoch_nonce: self.committed.nonce().into(),
            features: FeatureSet::new(self.committed.negotiated_features()),
        }
    }

    pub(super) fn belongs_to_transport(&self, transport: &EstablishedLeg) -> bool {
        transport.belongs_to_transport(&self.seal)
    }

    pub(super) fn transport_binding(&self) -> AttachTransportBinding {
        self.committed.transport_binding()
    }

    pub(super) fn standby_registration_request(
        &self,
        nonce: StandbyNonce,
    ) -> Result<StandbyRegistrationRequest, StandbyRegistrationReject> {
        StandbyRegistrationRequest::from_installed_leg(&self.committed, nonce)
    }

    pub(crate) fn local_open_event(&self, target: TargetAddr) -> SessionEvent {
        SessionEvent::LocalOpen {
            leg: self.committed,
            target,
        }
    }

    pub(super) fn accept_inbound(
        &self,
        inbound: BoundInbound,
    ) -> Result<SessionEvent, ClientActiveInboundFailure> {
        let BoundInbound::Session(received) = inbound else {
            return Err(ClientActiveInboundFailure { inbound });
        };
        if !self.seal.same_connection(&received.seal) || is_attach_record(received.frame.record()) {
            return Err(ClientActiveInboundFailure {
                inbound: BoundInbound::Session(received),
            });
        }
        Ok(SessionEvent::PeerFrame {
            leg: self.committed,
            frame: received.frame,
        })
    }

    pub(super) fn retire(self) -> RetiredClientLeg {
        RetiredClientLeg {
            seal: self.seal,
            endpoint: self.endpoint,
            generation: self.committed.generation(),
        }
    }

    #[allow(
        clippy::result_large_err,
        reason = "wrong terminal returns the exact active client leg and terminal fact"
    )]
    pub(super) fn bind_terminal_retaining_retired(
        self,
        terminal: ExactLegTerminal,
    ) -> Result<(TerminalBoundRetiredClientLeg, PendingClientLegLoss), ClientActiveTerminalMismatch>
    {
        if !self.matches_terminal(&terminal) {
            return Err(ClientActiveTerminalMismatch {
                active: self,
                terminal,
            });
        }
        let retired = RetiredClientLeg {
            seal: self.seal.share(),
            endpoint: self.endpoint.clone(),
            generation: self.committed.generation(),
        };
        let reason = terminal.reason();
        let terminal_bound = TerminalBoundRetiredClientLeg { retired, terminal };
        let pending = PendingClientLegLoss {
            leg: self.committed,
            reason,
        };
        Ok((terminal_bound, pending))
    }

    /// Borrow-only preflight for the shutdown transaction that may have to
    /// release a fatal recovery tail before consuming the terminal. The
    /// subsequent bind is synchronous, so a true result cannot become stale.
    pub(super) fn matches_terminal(&self, terminal: &ExactLegTerminal) -> bool {
        self.seal.same_connection(&terminal.seal)
            && Weak::ptr_eq(&self.endpoint, &Arc::downgrade(&terminal.endpoint))
            && !terminal.endpoint.is_open()
    }
}

impl fmt::Debug for ClientActiveLeg {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ClientActiveLeg([REDACTED])")
    }
}

pub(super) struct ClientActiveInboundFailure {
    inbound: BoundInbound,
}

impl ClientActiveInboundFailure {
    pub(super) fn into_inbound(self) -> BoundInbound {
        self.inbound
    }
}

impl fmt::Debug for ClientActiveInboundFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ClientActiveInboundFailure([REDACTED])")
    }
}

pub(super) struct ClientActiveTerminalMismatch {
    active: ClientActiveLeg,
    terminal: ExactLegTerminal,
}

impl ClientActiveTerminalMismatch {
    pub(super) fn into_parts(self) -> (ClientActiveLeg, ExactLegTerminal) {
        (self.active, self.terminal)
    }
}

impl fmt::Debug for ClientActiveTerminalMismatch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ClientActiveTerminalMismatch([REDACTED])")
    }
}

/// Bounded inert receive seal for a superseded client transport. It cannot
/// construct a `SessionEvent`; it only proves and discards late data-plane
/// frames or the exact terminal fact for its retired connection.
pub(crate) struct RetiredClientLeg {
    seal: LegSeal,
    endpoint: Weak<LegTransportEndpoint>,
    generation: LegGeneration,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RetiredClientInboundKind {
    Data,
    Ack,
    Close,
    Reset,
}

impl RetiredClientLeg {
    pub(crate) fn generation(&self) -> LegGeneration {
        self.generation
    }

    pub(super) fn discard_late_inbound(
        &self,
        inbound: BoundInbound,
    ) -> Result<RetiredClientInboundKind, BoundInbound> {
        let BoundInbound::Session(received) = inbound else {
            return Err(inbound);
        };
        if !self.seal.same_connection(&received.seal) {
            return Err(BoundInbound::Session(received));
        }
        let kind = match received.frame.record() {
            Record::Data { .. } => RetiredClientInboundKind::Data,
            Record::Ack { .. } => RetiredClientInboundKind::Ack,
            Record::Close { .. } => RetiredClientInboundKind::Close,
            Record::Reset { .. } => RetiredClientInboundKind::Reset,
            _ => return Err(BoundInbound::Session(received)),
        };
        drop(received);
        Ok(kind)
    }

    pub(super) fn discard_terminal(
        &self,
        terminal: ExactLegTerminal,
    ) -> Result<(), ExactLegTerminal> {
        if !self.seal.same_connection(&terminal.seal)
            || !Weak::ptr_eq(&self.endpoint, &Arc::downgrade(&terminal.endpoint))
            || terminal.endpoint.is_open()
        {
            return Err(terminal);
        }
        Ok(())
    }
}

/// Exact terminal-bound cleanup authority for a current client transport that
/// has already become logically lost. The terminal remains here while the
/// separate logical-loss capability is consumed by the reducer, so later
/// physical retired cleanup never needs a cloned or second terminal fact.
pub(crate) struct TerminalBoundRetiredClientLeg {
    retired: RetiredClientLeg,
    terminal: ExactLegTerminal,
}

impl TerminalBoundRetiredClientLeg {
    pub(crate) fn generation(&self) -> LegGeneration {
        self.retired.generation()
    }

    pub(crate) const fn reason(&self) -> LegTransportTerminalReason {
        self.terminal.reason()
    }

    pub(super) fn discard_late_inbound(
        &self,
        inbound: BoundInbound,
    ) -> Result<RetiredClientInboundKind, BoundInbound> {
        self.retired.discard_late_inbound(inbound)
    }
}

impl fmt::Debug for TerminalBoundRetiredClientLeg {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TerminalBoundRetiredClientLeg")
            .field("generation", &self.generation())
            .field("reason", &self.reason())
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for RetiredClientLeg {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RetiredClientLeg")
            .field("generation", &self.generation)
            .finish_non_exhaustive()
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

    pub(super) fn session_protocol_version(&self) -> u16 {
        self.committed.session_protocol_version()
    }

    pub(super) fn negotiated_features(&self) -> u64 {
        self.committed.negotiated_features()
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

    pub(super) fn installed_leg_probe_source(&self) -> InstalledLegProbeSource {
        InstalledLegProbeSource {
            seal: self.seal.share(),
            endpoint: self.endpoint.clone(),
            session_id: self.committed.session_id(),
            generation: self.committed.generation(),
            epoch_nonce: self.committed.nonce().into(),
            features: FeatureSet::new(self.committed.negotiated_features()),
        }
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
        if !self.matches_terminal(&terminal) {
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

    /// Borrow-only exact-terminal preflight for a logically superseded owner
    /// leg.  Unlike [`Self::bind_terminal`], this check cannot mint a pending
    /// `LegLost` capability; the retired-owner transaction consumes both
    /// inputs only after its replacement and physical actor have also been
    /// validated.
    pub(super) fn matches_terminal(&self, terminal: &ExactLegTerminal) -> bool {
        self.seal.same_connection(&terminal.seal)
            && Weak::ptr_eq(&self.endpoint, &Arc::downgrade(&terminal.endpoint))
            && !terminal.endpoint.is_open()
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

    /// Borrow-only reducer input used by terminal cleanup precommit. Keeping
    /// the exact terminal fact in this capability lets a rejected reducer
    /// transition restore the whole pending loss instead of reconstructing it
    /// from a value-only `LegLost` event.
    pub(crate) fn event(&self) -> SessionEvent {
        SessionEvent::LegLost { leg: self.leg }
    }

    pub(crate) fn into_event(self) -> SessionEvent {
        let event = self.event();
        let Self { leg: _, terminal } = self;
        debug_assert!(!terminal.endpoint.is_open());
        event
    }
}

impl fmt::Debug for PendingLegLoss {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PendingLegLoss([REDACTED])")
    }
}

/// Exact client-leg reducer authority separated from its terminal-bound
/// physical cleanup authority. Consuming this value can emit `LegLost` once,
/// but cannot consume or recreate the retained terminal fact.
pub(crate) struct PendingClientLegLoss {
    leg: CommittedLeg,
    reason: LegTransportTerminalReason,
}

impl PendingClientLegLoss {
    pub(crate) const fn reason(&self) -> LegTransportTerminalReason {
        self.reason
    }

    pub(crate) fn into_event(self) -> SessionEvent {
        SessionEvent::LegLost { leg: self.leg }
    }
}

impl fmt::Debug for PendingClientLegLoss {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PendingClientLegLoss([REDACTED])")
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
