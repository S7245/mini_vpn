//! Typed provenance for frames received from one authenticated transport leg.
//!
//! The eventual Quinn adapter is responsible for constructing one
//! [`EstablishedLeg`] after a full TLS handshake and for calling
//! [`EstablishedLeg::bind_received_frame`] only with bytes decoded from that
//! exact connection.  This module deliberately contains no Quinn, socket, or
//! 0-RTT behavior.

use crate::resumable::{
    AttachAuthority, AttachReject, AttachRequest, AttachTransportBinding, CommittedLeg, Frame,
    GenerationCatchUp, GenerationResynchronization, LegGeneration, PendingGenerationCatchUp,
    Record, SessionConfig, SessionEffect, SessionError, SessionEvent, SessionModel, SessionRole,
};
use crate::shared::TargetAddr;
use std::fmt;
use std::sync::Arc;
use thiserror::Error;

/// One authenticated connection's process-local, unforgeable identity.
///
/// There is intentionally no numeric identifier: equality is the identity of
/// the live allocation shared by values minted from the same connection.
struct LegSeal(Arc<LegSealInner>);

struct LegSealInner;

impl LegSeal {
    fn fresh() -> Self {
        Self(Arc::new(LegSealInner))
    }

    fn share(&self) -> Self {
        Self(Arc::clone(&self.0))
    }

    fn same_connection(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

/// Authenticated facts and opaque identity of one live transport connection.
///
/// Construction and receive binding stay restricted to the parent
/// `owned_upstream` adapter so arbitrary library users cannot bless a frame as
/// transport-authenticated.
pub(crate) struct EstablishedLeg {
    seal: LegSeal,
    binding: AttachTransportBinding,
}

impl EstablishedLeg {
    pub(super) fn for_authenticated_transport(binding: AttachTransportBinding) -> Self {
        Self {
            seal: LegSeal::fresh(),
            binding,
        }
    }

    pub(crate) fn begin_attach(&self, request: AttachRequest) -> PendingAttach {
        PendingAttach {
            seal: self.seal.share(),
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
            attached: AttachedLeg { seal, committed },
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
                attached: AttachedLeg { seal, committed },
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
            binding,
            request,
        } = self;
        match response.frame.record() {
            Record::AttachAccepted { .. } => request
                .validate_accepted_frame(&binding, &response.frame)
                .map(|committed| AttachResponse::Accepted(AttachedLeg { seal, committed }))
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
    catch_up: GenerationCatchUp,
}

impl CaughtUpAttachedLeg {
    pub(crate) fn generation(&self) -> LegGeneration {
        self.catch_up.accepted_leg().generation()
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

/// Successfully attached, connection-bound data-plane authority.
///
/// The authenticated [`CommittedLeg`] deliberately remains private inside
/// this non-`Clone` wrapper.  Adapters can mint reducer events only through
/// the methods below, and peer frames must carry the exact same process-local
/// connection seal that authenticated the ATTACH response.
pub(crate) struct AttachedLeg {
    seal: LegSeal,
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

    pub(crate) fn leg_lost_event(&self) -> SessionEvent {
        SessionEvent::LegLost {
            leg: self.committed,
        }
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

impl fmt::Debug for LegBoundFrame {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("LegBoundFrame([REDACTED])")
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
