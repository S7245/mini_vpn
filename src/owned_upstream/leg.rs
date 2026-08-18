//! Typed provenance for frames received from one authenticated transport leg.
//!
//! The eventual Quinn adapter is responsible for constructing one
//! [`EstablishedLeg`] after a full TLS handshake and for calling
//! [`EstablishedLeg::bind_received_frame`] only with bytes decoded from that
//! exact connection.  This module deliberately contains no Quinn, socket, or
//! 0-RTT behavior.

use crate::resumable::{
    AttachReject, AttachRequest, AttachTransportBinding, CommittedLeg, Frame,
    GenerationResynchronization, LegGeneration, Record, SessionEvent,
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
                .map(AttachResponse::GenerationStatus)
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
        if matches!(
            received.frame.record(),
            Record::Attach { .. }
                | Record::AttachAccepted { .. }
                | Record::AttachGenerationStatus { .. }
        ) {
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
    GenerationStatus(GenerationResynchronization),
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
