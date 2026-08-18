//! Pure authenticated-attach authority for Knife16 resumable sessions.
//!
//! TLS and device provisioning stay outside this module.  The caller supplies
//! a fixed-size TLS exporter binding and the identity facts established by the
//! authenticated transport.  This module binds those facts into one bounded,
//! canonical HMAC transcript and grants a new leg generation exactly once.

use super::protocol::{
    AttachNonce, AttachProof, FRAME_PROTOCOL_VERSION, FeatureSet, Frame, LegControlFrame,
    LegControlRecord, LegGeneration, ProtocolError, Record, SESSION_PROTOCOL_VERSION,
    STANDBY_CONTROL_V1, SessionId, StandbyNonce, StandbyProof,
};
use super::session::{SessionEffect, SessionError, SessionModel};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use thiserror::Error;

type HmacSha256 = Hmac<Sha256>;
type ModelCommitResult = Result<(CommittedLeg, Vec<SessionEffect>), SessionError>;
type ResynchronizableModelCommit = Result<ModelCommitResult, GenerationResynchronization>;
type ModelAttachResult = Result<ResynchronizableModelCommit, AttachReject>;

const ATTACH_PROTOCOL_CONTEXT: &[u8] = b"mini_vpn/resumable/attach/v1";
const STANDBY_REGISTER_PROTOCOL_CONTEXT: &[u8] = b"mini_vpn/resumable/standby-register/v1";
const RESUME_PROOF_CONTEXT: &[u8] = b"resume-authority";
const DEVICE_PROOF_CONTEXT: &[u8] = b"device-authority";
const SECRET_BYTES: usize = 32;
const OWNER_IDENTITY_BYTES: usize = 32;
const EXPORTER_BINDING_BYTES: usize = 32;
const DEVICE_PRINCIPAL_BYTES: usize = 16;
pub const MAX_ATTACH_ALPN_BYTES: usize = 32;
pub const MAX_ATTACH_TRANSCRIPT_BYTES: usize = 256;
pub const MAX_STANDBY_REGISTER_TRANSCRIPT_BYTES: usize = 320;

#[derive(PartialEq, Eq)]
struct Secret32([u8; SECRET_BYTES]);

impl Secret32 {
    fn new(
        value: [u8; SECRET_BYTES],
        zero_error: AttachConfigError,
    ) -> Result<Self, AttachConfigError> {
        if value == [0; SECRET_BYTES] {
            return Err(zero_error);
        }
        Ok(Self(value))
    }

    fn expose(&self) -> &[u8; SECRET_BYTES] {
        &self.0
    }
}

impl fmt::Debug for Secret32 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("[REDACTED]")
    }
}

/// Per-device credential material.  Its bytes are intentionally private and
/// its `Debug` representation never includes key material.
pub struct DeviceSecret(Secret32);

impl DeviceSecret {
    pub fn new(value: [u8; SECRET_BYTES]) -> Result<Self, AttachConfigError> {
        Secret32::new(value, AttachConfigError::ZeroDeviceSecret).map(Self)
    }
}

impl fmt::Debug for DeviceSecret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeviceSecret([REDACTED])")
    }
}

/// Session-specific resume authority.  It is independent from the device
/// credential and is minted only after the original authenticated session.
pub struct ResumeSecret(Secret32);

impl ResumeSecret {
    pub fn new(value: [u8; SECRET_BYTES]) -> Result<Self, AttachConfigError> {
        Secret32::new(value, AttachConfigError::ZeroResumeSecret).map(Self)
    }
}

impl fmt::Debug for ResumeSecret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ResumeSecret([REDACTED])")
    }
}

/// The stable cryptographic identity of the expected session owner.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct OwnerIdentity([u8; OWNER_IDENTITY_BYTES]);

impl OwnerIdentity {
    pub fn new(value: [u8; OWNER_IDENTITY_BYTES]) -> Result<Self, AttachConfigError> {
        if value == [0; OWNER_IDENTITY_BYTES] {
            return Err(AttachConfigError::ZeroOwnerIdentity);
        }
        Ok(Self(value))
    }

    pub fn as_bytes(&self) -> &[u8; OWNER_IDENTITY_BYTES] {
        &self.0
    }
}

impl fmt::Debug for OwnerIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OwnerIdentity([REDACTED])")
    }
}

/// The device principal established before session attach.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct DevicePrincipal([u8; DEVICE_PRINCIPAL_BYTES]);

impl DevicePrincipal {
    pub fn new(value: [u8; DEVICE_PRINCIPAL_BYTES]) -> Result<Self, AttachConfigError> {
        if value == [0; DEVICE_PRINCIPAL_BYTES] {
            return Err(AttachConfigError::ZeroDevicePrincipal);
        }
        Ok(Self(value))
    }

    pub fn as_bytes(&self) -> &[u8; DEVICE_PRINCIPAL_BYTES] {
        &self.0
    }
}

impl fmt::Debug for DevicePrincipal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DevicePrincipal([REDACTED])")
    }
}

/// A fixed-length value exported from the exact TLS 1.3 leg.  It is not a
/// wire field: the transport adapter must obtain it from that live channel.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct TlsExporterBinding([u8; EXPORTER_BINDING_BYTES]);

impl TlsExporterBinding {
    pub fn new(value: [u8; EXPORTER_BINDING_BYTES]) -> Result<Self, AttachConfigError> {
        if value == [0; EXPORTER_BINDING_BYTES] {
            return Err(AttachConfigError::ZeroExporterBinding);
        }
        Ok(Self(value))
    }

    fn as_bytes(&self) -> &[u8; EXPORTER_BINDING_BYTES] {
        &self.0
    }
}

impl fmt::Debug for TlsExporterBinding {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("TlsExporterBinding([REDACTED])")
    }
}

/// Bounded ALPN selected by the exact TLS leg.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct AttachAlpn {
    bytes: [u8; MAX_ATTACH_ALPN_BYTES],
    len: u8,
}

impl AttachAlpn {
    pub fn new(value: &[u8]) -> Result<Self, AttachConfigError> {
        if value.is_empty() {
            return Err(AttachConfigError::EmptyAlpn);
        }
        if value.len() > MAX_ATTACH_ALPN_BYTES {
            return Err(AttachConfigError::AlpnTooLong {
                len: value.len(),
                max: MAX_ATTACH_ALPN_BYTES,
            });
        }
        let mut bytes = [0; MAX_ATTACH_ALPN_BYTES];
        bytes[..value.len()].copy_from_slice(value);
        Ok(Self {
            bytes,
            len: value.len() as u8,
        })
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[..usize::from(self.len)]
    }
}

impl fmt::Debug for AttachAlpn {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("AttachAlpn")
            .field(&String::from_utf8_lossy(self.as_bytes()))
            .finish()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VersionRange {
    min: u16,
    max: u16,
}

impl VersionRange {
    pub fn new(min: u16, max: u16) -> Result<Self, AttachConfigError> {
        if min == 0 || min > max {
            return Err(AttachConfigError::InvalidVersionRange { min, max });
        }
        Ok(Self { min, max })
    }

    pub const fn min(self) -> u16 {
        self.min
    }

    pub const fn max(self) -> u16 {
        self.max
    }

    pub const fn contains(self, version: u16) -> bool {
        self.min <= version && version <= self.max
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FeatureOffer {
    offered: u64,
    required: u64,
}

impl FeatureOffer {
    pub fn new(offered: u64, required: u64) -> Result<Self, AttachConfigError> {
        if required & !offered != 0 {
            return Err(AttachConfigError::RequiredFeaturesNotOffered { offered, required });
        }
        Ok(Self { offered, required })
    }

    pub const fn offered(self) -> u64 {
        self.offered
    }

    pub const fn required(self) -> u64 {
        self.required
    }
}

/// Facts taken from the authenticated TLS leg rather than from attach wire
/// bytes.  Mutating any field invalidates an already-created proof.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AttachTransportBinding {
    owner_identity: OwnerIdentity,
    alpn: AttachAlpn,
    exporter: TlsExporterBinding,
    device_principal: DevicePrincipal,
}

impl AttachTransportBinding {
    pub const fn new(
        owner_identity: OwnerIdentity,
        alpn: AttachAlpn,
        exporter: TlsExporterBinding,
        device_principal: DevicePrincipal,
    ) -> Self {
        Self {
            owner_identity,
            alpn,
            exporter,
            device_principal,
        }
    }

    pub const fn owner_identity(&self) -> OwnerIdentity {
        self.owner_identity
    }

    pub const fn alpn(&self) -> AttachAlpn {
        self.alpn
    }

    pub const fn device_principal(&self) -> DevicePrincipal {
        self.device_principal
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AttachRequest {
    session_id: SessionId,
    requested_generation: LegGeneration,
    nonce: AttachNonce,
    versions: VersionRange,
    features: FeatureOffer,
}

impl AttachRequest {
    pub const fn new(
        session_id: SessionId,
        requested_generation: LegGeneration,
        nonce: AttachNonce,
        versions: VersionRange,
        features: FeatureOffer,
    ) -> Self {
        Self {
            session_id,
            requested_generation,
            nonce,
            versions,
            features,
        }
    }

    pub const fn session_id(&self) -> SessionId {
        self.session_id
    }

    pub const fn requested_generation(&self) -> LegGeneration {
        self.requested_generation
    }

    pub const fn nonce(&self) -> AttachNonce {
        self.nonce
    }

    pub const fn versions(&self) -> VersionRange {
        self.versions
    }

    pub const fn features(&self) -> FeatureOffer {
        self.features
    }

    /// Builds the only wire ATTACH representation of this signed request.
    /// Keeping generation and negotiation fields behind this conversion avoids
    /// adapters signing one request and manually encoding another.
    pub fn to_attach_frame(self, proof: AttachProof) -> Frame {
        Frame::new(
            self.requested_generation,
            Record::Attach {
                session_id: self.session_id,
                nonce: self.nonce,
                min_version: self.versions.min,
                max_version: self.versions.max,
                offered_features: FeatureSet::new(self.features.offered),
                required_features: FeatureSet::new(self.features.required),
                proof,
            },
        )
    }

    /// Extracts the exact authenticated request carried by a decoded ATTACH.
    /// The frame generation is the requested generation by construction.
    pub fn from_attach_frame(frame: &Frame) -> Result<(Self, AttachProof), AttachReject> {
        let Record::Attach {
            session_id,
            nonce,
            min_version,
            max_version,
            offered_features,
            required_features,
            proof,
        } = frame.record()
        else {
            return Err(AttachReject::Rejected);
        };
        let versions =
            VersionRange::new(*min_version, *max_version).map_err(|_| AttachReject::Rejected)?;
        let features = FeatureOffer::new(offered_features.bits(), required_features.bits())
            .map_err(|_| AttachReject::Rejected)?;
        Ok((
            Self::new(
                *session_id,
                frame.leg_generation(),
                *nonce,
                versions,
                features,
            ),
            *proof,
        ))
    }

    /// Validates the server's exact ATTACH_ACCEPTED response and mints the
    /// client-side capability for that authenticated transport leg.
    ///
    /// This pure validator assumes `frame` arrived on the exact authenticated
    /// TLS leg represented by `binding`. The transport adapter must preserve
    /// that frame-to-leg association; this model then verifies the request's
    /// exact session, nonce, generation, and negotiated result.
    pub(crate) fn validate_accepted_frame(
        &self,
        binding: &AttachTransportBinding,
        frame: &Frame,
    ) -> Result<CommittedLeg, AttachReject> {
        if frame.leg_generation() != self.requested_generation {
            return Err(AttachReject::Rejected);
        }
        let Record::AttachAccepted {
            session_id,
            nonce,
            selected_version,
            features,
        } = frame.record()
        else {
            return Err(AttachReject::Rejected);
        };
        let negotiated_features = features.bits();
        if *session_id != self.session_id
            || *nonce != self.nonce
            || !self.versions.contains(*selected_version)
            || negotiated_features & !self.features.offered != 0
            || self.features.required & !negotiated_features != 0
        {
            return Err(AttachReject::Rejected);
        }
        Ok(CommittedLeg {
            session_id: self.session_id,
            generation: self.requested_generation,
            nonce: self.nonce,
            transport_binding: *binding,
            session_protocol_version: *selected_version,
            negotiated_features,
        })
    }

    /// Validates an authenticated generation-status response for this exact
    /// attach attempt.  The echoed session, requested generation, and nonce
    /// prevent a response for another (or older) request from minting a
    /// resynchronization capability.
    pub(crate) fn validate_generation_status_frame(
        &self,
        binding: &AttachTransportBinding,
        frame: &Frame,
    ) -> Result<GenerationResynchronization, AttachReject> {
        let Record::AttachGenerationStatus {
            session_id,
            requested_generation,
            nonce,
        } = frame.record()
        else {
            return Err(AttachReject::Rejected);
        };
        if *session_id != self.session_id
            || *requested_generation != self.requested_generation
            || *nonce != self.nonce
        {
            return Err(AttachReject::Rejected);
        }
        Ok(GenerationResynchronization {
            session_id: self.session_id,
            current_generation: frame.leg_generation(),
            requested_generation: self.requested_generation,
            nonce: self.nonce,
            transport_binding: *binding,
            versions: self.versions,
            features: self.features,
        })
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct InstalledSessionContract {
    session_id: SessionId,
    current_generation: LegGeneration,
    selected_version: u16,
    features: FeatureSet,
    owner_identity: OwnerIdentity,
    alpn: AttachAlpn,
    device_principal: DevicePrincipal,
}

impl InstalledSessionContract {
    fn from_leg(leg: &CommittedLeg) -> Result<Self, StandbyRegistrationReject> {
        let features = FeatureSet::new(leg.negotiated_features());
        if !features.contains(STANDBY_CONTROL_V1) {
            return Err(StandbyRegistrationReject::Rejected);
        }
        let binding = leg.transport_binding();
        Ok(Self {
            session_id: leg.session_id(),
            current_generation: leg.generation(),
            selected_version: leg.session_protocol_version(),
            features,
            owner_identity: binding.owner_identity(),
            alpn: binding.alpn(),
            device_principal: binding.device_principal(),
        })
    }
}

/// Exact standby registration request derived from an already-installed leg.
/// It authenticates a second transport without granting replacement authority.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct StandbyRegistrationRequest {
    installed: InstalledSessionContract,
    nonce: StandbyNonce,
}

impl StandbyRegistrationRequest {
    pub fn from_installed_leg(
        installed_leg: &CommittedLeg,
        nonce: StandbyNonce,
    ) -> Result<Self, StandbyRegistrationReject> {
        Ok(Self {
            installed: InstalledSessionContract::from_leg(installed_leg)?,
            nonce,
        })
    }

    pub fn to_frame(
        self,
        proof: StandbyProof,
    ) -> Result<LegControlFrame, StandbyRegistrationReject> {
        LegControlFrame::try_new(
            self.installed.current_generation,
            LegControlRecord::StandbyRegister {
                session_id: self.installed.session_id,
                standby_nonce: self.nonce,
                selected_version: self.installed.selected_version,
                features: self.installed.features,
                proof,
            },
            self.installed.features,
        )
        .map_err(|_| StandbyRegistrationReject::Rejected)
    }

    pub const fn standby_nonce(&self) -> StandbyNonce {
        self.nonce
    }
}

impl fmt::Debug for StandbyRegistrationRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StandbyRegistrationRequest")
            .field("authentication", &"[REDACTED]")
            .field("current_generation", &self.installed.current_generation)
            .field("selected_version", &self.installed.selected_version)
            .field("features", &self.installed.features)
            .finish()
    }
}

/// Authenticated registration of a distinct standby transport.  This is not
/// an attach capability and cannot advance or replace the installed leg.
#[derive(PartialEq, Eq)]
pub struct AuthenticatedStandbyRegistration {
    request: StandbyRegistrationRequest,
    transport_binding: AttachTransportBinding,
}

impl AuthenticatedStandbyRegistration {
    pub const fn session_id(&self) -> SessionId {
        self.request.installed.session_id
    }

    pub const fn current_generation(&self) -> LegGeneration {
        self.request.installed.current_generation
    }

    pub const fn standby_nonce(&self) -> StandbyNonce {
        self.request.standby_nonce()
    }

    pub const fn selected_version(&self) -> u16 {
        self.request.installed.selected_version
    }

    pub const fn features(&self) -> FeatureSet {
        self.request.installed.features
    }

    pub const fn transport_binding(&self) -> AttachTransportBinding {
        self.transport_binding
    }
}

impl fmt::Debug for AuthenticatedStandbyRegistration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthenticatedStandbyRegistration")
            .field("authentication", &"[REDACTED]")
            .field("current_generation", &self.current_generation())
            .field("selected_version", &self.selected_version())
            .field("features", &self.features())
            .finish()
    }
}

/// Immutable session-side attach policy.  The TLS exporter remains leg-local
/// and is supplied separately in [`AttachTransportBinding`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AttachPolicy {
    owner_identity: OwnerIdentity,
    alpn: AttachAlpn,
    device_principal: DevicePrincipal,
    session_protocol_version: u16,
    supported_features: u64,
}

impl AttachPolicy {
    pub fn new(
        owner_identity: OwnerIdentity,
        alpn: AttachAlpn,
        device_principal: DevicePrincipal,
        session_protocol_version: u16,
        supported_features: u64,
    ) -> Result<Self, AttachConfigError> {
        if session_protocol_version != SESSION_PROTOCOL_VERSION {
            return Err(AttachConfigError::UnsupportedProtocolVersion(
                session_protocol_version,
            ));
        }
        Ok(Self {
            owner_identity,
            alpn,
            device_principal,
            session_protocol_version,
            supported_features,
        })
    }
}

/// The two independent authorities needed to attach a replacement leg.
pub struct AttachCredentials {
    device_secret: DeviceSecret,
    resume_secret: ResumeSecret,
}

impl AttachCredentials {
    pub fn new(
        device_secret: DeviceSecret,
        resume_secret: ResumeSecret,
    ) -> Result<Self, AttachConfigError> {
        if device_secret.0 == resume_secret.0 {
            return Err(AttachConfigError::SecretsNotIndependent);
        }
        Ok(Self {
            device_secret,
            resume_secret,
        })
    }

    pub fn prove(
        &self,
        request: &AttachRequest,
        binding: &AttachTransportBinding,
    ) -> Result<AttachProof, AttachConfigError> {
        let transcript = encode_transcript(request, binding);
        let proof = self.proof_mac(&transcript)?.finalize().into_bytes().into();
        AttachProof::new(proof).map_err(AttachConfigError::InvalidProofEncoding)
    }

    pub fn prove_standby_registration(
        &self,
        request: &StandbyRegistrationRequest,
        binding: &AttachTransportBinding,
    ) -> Result<StandbyProof, AttachConfigError> {
        let transcript = encode_standby_register_transcript(request, binding);
        let proof = self
            .standby_proof_mac(&transcript)?
            .finalize()
            .into_bytes()
            .into();
        StandbyProof::new(proof).map_err(AttachConfigError::InvalidStandbyProofEncoding)
    }

    fn verifies(
        &self,
        request: &AttachRequest,
        binding: &AttachTransportBinding,
        proof: &AttachProof,
    ) -> bool {
        let transcript = encode_transcript(request, binding);
        self.proof_mac(&transcript)
            .map(|mac| mac.verify_slice(proof.as_bytes()).is_ok())
            .unwrap_or(false)
    }

    fn proof_mac(&self, transcript: &[u8]) -> Result<HmacSha256, AttachConfigError> {
        let mut resume_mac = <HmacSha256 as Mac>::new_from_slice(self.resume_secret.0.expose())
            .map_err(|_| AttachConfigError::InvalidHmacKey)?;
        resume_mac.update(ATTACH_PROTOCOL_CONTEXT);
        resume_mac.update(RESUME_PROOF_CONTEXT);
        resume_mac.update(transcript);
        let resume_tag = resume_mac.finalize().into_bytes();

        let mut device_mac = <HmacSha256 as Mac>::new_from_slice(self.device_secret.0.expose())
            .map_err(|_| AttachConfigError::InvalidHmacKey)?;
        device_mac.update(ATTACH_PROTOCOL_CONTEXT);
        device_mac.update(DEVICE_PROOF_CONTEXT);
        // `resume_tag` already authenticates the complete transcript.  The
        // outer MAC adds the independent device authority without hashing the
        // transcript a second time.
        device_mac.update(&resume_tag);
        Ok(device_mac)
    }

    fn verifies_standby_registration(
        &self,
        request: &StandbyRegistrationRequest,
        binding: &AttachTransportBinding,
        proof: &StandbyProof,
    ) -> bool {
        let transcript = encode_standby_register_transcript(request, binding);
        self.standby_proof_mac(&transcript)
            .map(|mac| mac.verify_slice(proof.as_bytes()).is_ok())
            .unwrap_or(false)
    }

    fn standby_proof_mac(&self, transcript: &[u8]) -> Result<HmacSha256, AttachConfigError> {
        let mut resume_mac = <HmacSha256 as Mac>::new_from_slice(self.resume_secret.0.expose())
            .map_err(|_| AttachConfigError::InvalidHmacKey)?;
        resume_mac.update(STANDBY_REGISTER_PROTOCOL_CONTEXT);
        resume_mac.update(RESUME_PROOF_CONTEXT);
        resume_mac.update(transcript);
        let resume_tag = resume_mac.finalize().into_bytes();

        let mut device_mac = <HmacSha256 as Mac>::new_from_slice(self.device_secret.0.expose())
            .map_err(|_| AttachConfigError::InvalidHmacKey)?;
        device_mac.update(STANDBY_REGISTER_PROTOCOL_CONTEXT);
        device_mac.update(DEVICE_PROOF_CONTEXT);
        device_mac.update(&resume_tag);
        Ok(device_mac)
    }
}

impl fmt::Debug for AttachCredentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AttachCredentials")
            .field("device_secret", &self.device_secret)
            .field("resume_secret", &self.resume_secret)
            .finish()
    }
}

/// Capability returned only by a successful, atomic attach commit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CommittedLeg {
    session_id: SessionId,
    generation: LegGeneration,
    nonce: AttachNonce,
    transport_binding: AttachTransportBinding,
    session_protocol_version: u16,
    negotiated_features: u64,
}

impl CommittedLeg {
    pub const fn session_id(&self) -> SessionId {
        self.session_id
    }

    pub const fn generation(&self) -> LegGeneration {
        self.generation
    }

    pub const fn nonce(&self) -> AttachNonce {
        self.nonce
    }

    pub const fn owner_identity(&self) -> OwnerIdentity {
        self.transport_binding.owner_identity
    }

    pub const fn device_principal(&self) -> DevicePrincipal {
        self.transport_binding.device_principal
    }

    /// Exact authenticated channel facts from which this capability was
    /// minted. Secret and exporter bytes remain redacted by their leaf types.
    pub const fn transport_binding(&self) -> AttachTransportBinding {
        self.transport_binding
    }

    pub const fn session_protocol_version(&self) -> u16 {
        self.session_protocol_version
    }

    pub const fn negotiated_features(&self) -> u64 {
        self.negotiated_features
    }

    /// Builds the sole wire acceptance corresponding to this committed leg.
    pub fn attach_accepted_frame(self) -> Frame {
        Frame::new(
            self.generation,
            Record::AttachAccepted {
                session_id: self.session_id,
                nonce: self.nonce,
                selected_version: self.session_protocol_version,
                features: FeatureSet::new(self.negotiated_features),
            },
        )
    }
}

/// Proof-valid outcome of processing an ATTACH frame.  A status response is
/// returned only after the same authentication, identity, and protocol checks
/// required for a commit; unknown sessions and bad proofs remain generic
/// public rejections.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AttachFrameOutcome {
    Committed(CommittedLeg),
    Resynchronize(GenerationResynchronization),
}

/// Client/server capability tied to one authenticated attach attempt and TLS
/// leg.  It can emit the correlated wire status and derive only the exact next
/// generation, never mutate the server generation itself.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GenerationResynchronization {
    session_id: SessionId,
    current_generation: LegGeneration,
    requested_generation: LegGeneration,
    nonce: AttachNonce,
    transport_binding: AttachTransportBinding,
    versions: VersionRange,
    features: FeatureOffer,
}

impl GenerationResynchronization {
    pub const fn current_generation(&self) -> LegGeneration {
        self.current_generation
    }

    pub const fn requested_generation(&self) -> LegGeneration {
        self.requested_generation
    }

    pub const fn transport_binding(&self) -> AttachTransportBinding {
        self.transport_binding
    }

    /// Builds the sole status record corresponding to this authenticated
    /// attempt.  The current generation lives in the frame envelope; request
    /// correlation fields are repeated in the body.  The record deliberately
    /// has no second application MAC and must travel on the exact authenticated
    /// TLS leg represented by [`Self::transport_binding`].
    pub fn status_frame(self) -> Frame {
        Frame::new(
            self.current_generation,
            Record::AttachGenerationStatus {
                session_id: self.session_id,
                requested_generation: self.requested_generation,
                nonce: self.nonce,
            },
        )
    }

    /// Derives a fresh attach for exactly `current_generation + 1`, retaining
    /// the authenticated session's version and feature offer.  Callers must
    /// supply a fresh nonce and sign the returned request for the live leg.
    pub fn next_request(self, nonce: AttachNonce) -> Result<AttachRequest, AttachReject> {
        if nonce == self.nonce {
            return Err(AttachReject::Rejected);
        }
        let next = self
            .current_generation
            .get()
            .checked_add(1)
            .ok_or(AttachReject::GenerationExhausted)?;
        let requested_generation =
            LegGeneration::new(next).map_err(|_| AttachReject::GenerationExhausted)?;
        Ok(AttachRequest::new(
            self.session_id,
            requested_generation,
            nonce,
            self.versions,
            self.features,
        ))
    }

    /// Starts the only bounded catch-up sequence accepted by the client
    /// reducer: the original correlated request must be the client's exact
    /// next generation, the server reports its authenticated high-water mark,
    /// and the follow-up request is exactly one generation after that mark.
    ///
    /// This permits recovery after more than one lost acceptance without
    /// granting a caller a bare generation-jump API. The returned pending value
    /// retains the correlated original request/status facts until the exact
    /// high-water successor is accepted.
    pub fn begin_catch_up(
        self,
        nonce: AttachNonce,
    ) -> Result<PendingGenerationCatchUp, AttachReject> {
        if self.current_generation.get() < self.requested_generation.get() {
            return Err(AttachReject::Rejected);
        }
        let expected_local_generation = self
            .requested_generation
            .get()
            .checked_sub(1)
            .and_then(|generation| LegGeneration::new(generation).ok())
            .ok_or(AttachReject::Rejected)?;
        let request = self.next_request(nonce)?;
        Ok(PendingGenerationCatchUp {
            original_requested_generation: self.requested_generation,
            original_nonce: self.nonce,
            observed_generation: self.current_generation,
            expected_local_generation,
            status_transport_binding: self.transport_binding,
            request,
        })
    }
}

/// Correlated client-side attach attempt that may mint one generation
/// catch-up capability after its exact acceptance is validated.
///
/// Construction is restricted to [`GenerationResynchronization::begin_catch_up`]
/// and fields are private so an adapter cannot invent a skipped-generation
/// transition.  It is intentionally not `Clone` or `Copy`.
pub struct PendingGenerationCatchUp {
    original_requested_generation: LegGeneration,
    original_nonce: AttachNonce,
    observed_generation: LegGeneration,
    expected_local_generation: LegGeneration,
    status_transport_binding: AttachTransportBinding,
    request: AttachRequest,
}

impl PendingGenerationCatchUp {
    /// The exact request that must be proved and sent on the authenticated leg.
    pub const fn request(&self) -> AttachRequest {
        self.request
    }

    /// Validates the exact status-derived acceptance and mints the sole typed
    /// authority that can advance a lagging client session model. `binding`
    /// belongs to this follow-up attach and may differ from the authenticated
    /// leg that carried the generation status; the transport adapter must bind
    /// each response to its own exact connection seal.
    pub(crate) fn validate_accepted_frame(
        self,
        binding: &AttachTransportBinding,
        frame: &Frame,
    ) -> Result<GenerationCatchUp, AttachReject> {
        let accepted_leg = self.request.validate_accepted_frame(binding, frame)?;
        let expected_accepted = self
            .observed_generation
            .get()
            .checked_add(1)
            .ok_or(AttachReject::GenerationExhausted)?;
        if accepted_leg.generation().get() != expected_accepted {
            return Err(AttachReject::Rejected);
        }
        Ok(GenerationCatchUp {
            expected_local_generation: self.expected_local_generation,
            original_requested_generation: self.original_requested_generation,
            original_nonce: self.original_nonce,
            observed_generation: self.observed_generation,
            status_transport_binding: self.status_transport_binding,
            accepted_leg,
        })
    }
}

impl fmt::Debug for PendingGenerationCatchUp {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PendingGenerationCatchUp")
            .field("expected_local_generation", &self.expected_local_generation)
            .field("observed_generation", &self.observed_generation)
            .field("requested_generation", &self.request.requested_generation())
            .finish_non_exhaustive()
    }
}

/// Opaque proof that one or more lost acceptances were observed through an
/// authenticated high-water status and that its exact successor was accepted.
/// The status and successor may use distinct authenticated transport legs;
/// both bindings remain captured in this capability. Private construction
/// prevents generation jumps without that exact chain.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct GenerationCatchUp {
    expected_local_generation: LegGeneration,
    original_requested_generation: LegGeneration,
    original_nonce: AttachNonce,
    observed_generation: LegGeneration,
    status_transport_binding: AttachTransportBinding,
    accepted_leg: CommittedLeg,
}

impl GenerationCatchUp {
    pub(crate) const fn session_id(&self) -> SessionId {
        self.accepted_leg.session_id()
    }

    pub(crate) const fn expected_local_generation(&self) -> LegGeneration {
        self.expected_local_generation
    }

    pub(crate) const fn original_requested_generation(&self) -> LegGeneration {
        self.original_requested_generation
    }

    pub(crate) const fn observed_generation(&self) -> LegGeneration {
        self.observed_generation
    }

    pub(crate) const fn accepted_leg(&self) -> CommittedLeg {
        self.accepted_leg
    }
}

impl fmt::Debug for GenerationCatchUp {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GenerationCatchUp")
            .field("expected_local_generation", &self.expected_local_generation)
            .field(
                "original_requested_generation",
                &self.original_requested_generation,
            )
            .field("observed_generation", &self.observed_generation)
            .field("accepted_generation", &self.accepted_leg.generation())
            .field("correlation", &"[REDACTED]")
            .finish()
    }
}

/// One session's single generation authority.  `compare_exchange` makes two
/// concurrent, correctly authenticated requests for the same next generation
/// race safely: exactly one commits and the loser observes `GenerationNotNext`.
pub struct AttachAuthority {
    session_id: SessionId,
    current_generation: AtomicU64,
    credentials: AttachCredentials,
    policy: AttachPolicy,
}

/// Proof-valid exact-next attach that has not advanced generation authority.
///
/// This capability never crosses the authentication boundary.  The owner
/// adapter supplies a read-only model preflight, then this module consumes the
/// prepared value in the compare-exchange.  Keeping preparation private makes
/// it impossible for an adapter to synthesize a commit from bare generation
/// numbers or to roll back a failed model installation.
struct PreparedAttach {
    expected_current_generation: u64,
    committed: CommittedLeg,
}

impl AttachAuthority {
    pub fn new(
        session_id: SessionId,
        current_generation: LegGeneration,
        credentials: AttachCredentials,
        policy: AttachPolicy,
    ) -> Self {
        Self {
            session_id,
            current_generation: AtomicU64::new(current_generation.get()),
            credentials,
            policy,
        }
    }

    pub fn current_generation(&self) -> u64 {
        self.current_generation.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) const fn session_id(&self) -> SessionId {
        self.session_id
    }

    /// Verifies that an already-installed model leg belongs to this exact
    /// authority policy. The exporter is intentionally leg-local and may
    /// change across replacements; every stable authenticated and negotiated
    /// session fact must agree.
    #[cfg(test)]
    pub(crate) fn matches_existing_leg_semantics(&self, leg: &CommittedLeg) -> bool {
        let binding = leg.transport_binding();
        leg.session_id() == self.session_id
            && binding.owner_identity() == self.policy.owner_identity
            && binding.alpn() == self.policy.alpn
            && binding.device_principal() == self.policy.device_principal
            && leg.session_protocol_version() == self.policy.session_protocol_version
            && leg.negotiated_features() & !self.policy.supported_features == 0
    }

    /// Authenticates and commits the exact fields present in a decoded ATTACH
    /// frame. This strict entry preserves the original generation-error API;
    /// adapters that must recover a lost acceptance should use
    /// [`Self::verify_and_commit_or_resynchronize_frame`].
    pub fn verify_and_commit_frame(
        &self,
        frame: &Frame,
        binding: &AttachTransportBinding,
    ) -> Result<CommittedLeg, AttachReject> {
        let (request, proof) = AttachRequest::from_attach_frame(frame)?;
        self.verify_and_commit(&request, binding, &proof)
    }

    /// Authenticates the exact decoded ATTACH and either commits it or returns
    /// an authenticated current-generation status.  Generation state is never
    /// disclosed for a malformed frame, unknown session, wrong transport
    /// identity, unsupported negotiation, or invalid proof.
    pub fn verify_and_commit_or_resynchronize_frame(
        &self,
        frame: &Frame,
        binding: &AttachTransportBinding,
    ) -> Result<AttachFrameOutcome, AttachReject> {
        let (request, proof) = AttachRequest::from_attach_frame(frame)?;
        match self.verify_and_commit(&request, binding, &proof) {
            Ok(committed) => Ok(AttachFrameOutcome::Committed(committed)),
            Err(AttachReject::GenerationNotNext { current, .. }) => self
                .resynchronization(&request, binding, current)
                .map(AttachFrameOutcome::Resynchronize),
            Err(AttachReject::GenerationExhausted) => self
                .resynchronization(&request, binding, self.current_generation())
                .map(AttachFrameOutcome::Resynchronize),
            Err(AttachReject::Rejected) => Err(AttachReject::Rejected),
        }
    }

    pub fn verify_and_commit(
        &self,
        request: &AttachRequest,
        binding: &AttachTransportBinding,
        proof: &AttachProof,
    ) -> Result<CommittedLeg, AttachReject> {
        let prepared = self.prepare(request, binding, proof)?;
        self.commit_prepared(prepared)
    }

    /// Authenticates a standby transport against the exact installed session
    /// contract without invoking attach preparation or generation mutation.
    pub fn verify_standby_registration_frame(
        &self,
        installed_leg: &CommittedLeg,
        binding: &AttachTransportBinding,
        frame: &LegControlFrame,
    ) -> Result<AuthenticatedStandbyRegistration, StandbyRegistrationReject> {
        let installed = InstalledSessionContract::from_leg(installed_leg)?;
        let LegControlRecord::StandbyRegister {
            session_id,
            standby_nonce,
            selected_version,
            features,
            proof,
        } = frame.record()
        else {
            return Err(StandbyRegistrationReject::Rejected);
        };
        let request = StandbyRegistrationRequest {
            installed: InstalledSessionContract {
                session_id: *session_id,
                current_generation: frame.leg_generation(),
                selected_version: *selected_version,
                features: *features,
                owner_identity: installed.owner_identity,
                alpn: installed.alpn,
                device_principal: installed.device_principal,
            },
            nonce: *standby_nonce,
        };
        let binding_matches_installed = binding.owner_identity == installed.owner_identity
            && binding.alpn == installed.alpn
            && binding.device_principal == installed.device_principal;
        let installed_matches_authority = installed.session_id == self.session_id
            && installed.current_generation.get() == self.current_generation()
            && installed.owner_identity == self.policy.owner_identity
            && installed.alpn == self.policy.alpn
            && installed.device_principal == self.policy.device_principal
            && installed.selected_version == self.policy.session_protocol_version
            && installed.features.bits() & !self.policy.supported_features == 0;
        // Verify the MAC before combining public checks, matching ATTACH's
        // rejection shape rather than making field validity a cheap oracle.
        let proof_valid = self
            .credentials
            .verifies_standby_registration(&request, binding, proof);
        if !(proof_valid
            && request.installed == installed
            && binding_matches_installed
            && installed_matches_authority)
        {
            return Err(StandbyRegistrationReject::Rejected);
        }
        Ok(AuthenticatedStandbyRegistration {
            request,
            transport_binding: *binding,
        })
    }

    /// Authenticates a decoded ATTACH, invokes the owner's read-only model
    /// preflight before generation mutation, then commits with one CAS.
    ///
    /// The nested result deliberately keeps the private [`PreparedAttach`]
    /// inside this module:
    ///
    /// - outer `Err` is a public authentication/protocol rejection;
    /// - outer `Ok(Err(_))` is authenticated generation resynchronization;
    /// - `Ok(Ok(Err(_)))` is model-preflight rejection with no CAS;
    /// - `Ok(Ok(Ok(_)))` is the single committed winner plus the recovery
    ///   effects produced by consuming its opaque model-install token.
    pub(crate) fn preflight_model_and_commit_or_resynchronize_frame(
        &self,
        frame: &Frame,
        binding: &AttachTransportBinding,
        model: &mut SessionModel,
    ) -> ModelAttachResult {
        self.preflight_model_and_commit_with_hook(frame, binding, model, || {})
    }

    /// Test-hookable implementation kept private to the authentication module.
    /// The hook can coordinate a deterministic CAS race but cannot replace the
    /// mandatory model preflight or manufacture its opaque installation token.
    fn preflight_model_and_commit_with_hook(
        &self,
        frame: &Frame,
        binding: &AttachTransportBinding,
        model: &mut SessionModel,
        before_commit: impl FnOnce(),
    ) -> ModelAttachResult {
        let (request, proof) = AttachRequest::from_attach_frame(frame)?;
        let prepared = match self.prepare(&request, binding, &proof) {
            Ok(prepared) => prepared,
            Err(AttachReject::GenerationNotNext { current, .. }) => {
                return self.resynchronization(&request, binding, current).map(Err);
            }
            Err(AttachReject::GenerationExhausted) => {
                return self
                    .resynchronization(&request, binding, self.current_generation())
                    .map(Err);
            }
            Err(AttachReject::Rejected) => return Err(AttachReject::Rejected),
        };

        let model_preflight = match model.preflight_owner_replacement(prepared.committed) {
            Ok(preflight) => preflight,
            Err(error) => return Ok(Ok(Err(error))),
        };
        before_commit();
        match self.commit_prepared(prepared) {
            Ok(committed) => Ok(Ok(Ok((committed, model_preflight.install())))),
            Err(AttachReject::GenerationNotNext { current, .. }) => {
                self.resynchronization(&request, binding, current).map(Err)
            }
            Err(AttachReject::GenerationExhausted) => self
                .resynchronization(&request, binding, self.current_generation())
                .map(Err),
            Err(AttachReject::Rejected) => Err(AttachReject::Rejected),
        }
    }

    fn prepare(
        &self,
        request: &AttachRequest,
        binding: &AttachTransportBinding,
        proof: &AttachProof,
    ) -> Result<PreparedAttach, AttachReject> {
        let proof_valid = self.credentials.verifies(request, binding, proof);
        let identity_valid = request.session_id == self.session_id
            && binding.owner_identity == self.policy.owner_identity
            && binding.alpn == self.policy.alpn
            && binding.device_principal == self.policy.device_principal;
        let protocol_valid = request
            .versions
            .contains(self.policy.session_protocol_version)
            && request.features.required & !self.policy.supported_features == 0;
        if !(proof_valid && identity_valid && protocol_valid) {
            return Err(AttachReject::Rejected);
        }

        let current = self.current_generation.load(Ordering::Acquire);
        let Some(expected) = current.checked_add(1) else {
            return Err(AttachReject::GenerationExhausted);
        };
        let requested = request.requested_generation.get();
        if requested != expected {
            return Err(AttachReject::GenerationNotNext { current, requested });
        }

        Ok(PreparedAttach {
            expected_current_generation: current,
            committed: CommittedLeg {
                session_id: self.session_id,
                generation: request.requested_generation,
                nonce: request.nonce,
                transport_binding: *binding,
                session_protocol_version: self.policy.session_protocol_version,
                negotiated_features: request.features.offered & self.policy.supported_features,
            },
        })
    }

    fn commit_prepared(&self, prepared: PreparedAttach) -> Result<CommittedLeg, AttachReject> {
        let requested = prepared.committed.generation().get();
        match self.current_generation.compare_exchange(
            prepared.expected_current_generation,
            requested,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => Ok(prepared.committed),
            Err(actual) => Err(AttachReject::GenerationNotNext {
                current: actual,
                requested,
            }),
        }
    }

    fn resynchronization(
        &self,
        request: &AttachRequest,
        binding: &AttachTransportBinding,
        current: u64,
    ) -> Result<GenerationResynchronization, AttachReject> {
        let current_generation = LegGeneration::new(current).map_err(|_| AttachReject::Rejected)?;
        Ok(GenerationResynchronization {
            session_id: request.session_id,
            current_generation,
            requested_generation: request.requested_generation,
            nonce: request.nonce,
            transport_binding: *binding,
            versions: request.versions,
            features: request.features,
        })
    }
}

impl fmt::Debug for AttachAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AttachAuthority")
            .field("session_id", &self.session_id)
            .field("current_generation", &self.current_generation())
            .field("credentials", &self.credentials)
            .field("policy", &self.policy)
            .finish()
    }
}

fn encode_transcript(request: &AttachRequest, binding: &AttachTransportBinding) -> Vec<u8> {
    let mut transcript = Vec::with_capacity(MAX_ATTACH_TRANSCRIPT_BYTES);
    transcript.extend_from_slice(ATTACH_PROTOCOL_CONTEXT);
    transcript.extend_from_slice(&FRAME_PROTOCOL_VERSION.to_be_bytes());
    transcript.extend_from_slice(binding.owner_identity.as_bytes());
    transcript.push(binding.alpn.len);
    transcript.extend_from_slice(binding.alpn.as_bytes());
    transcript.extend_from_slice(binding.exporter.as_bytes());
    transcript.extend_from_slice(binding.device_principal.as_bytes());
    transcript.extend_from_slice(request.session_id.as_bytes());
    transcript.extend_from_slice(&request.requested_generation.get().to_be_bytes());
    transcript.extend_from_slice(request.nonce.as_bytes());
    transcript.extend_from_slice(&request.versions.min.to_be_bytes());
    transcript.extend_from_slice(&request.versions.max.to_be_bytes());
    transcript.extend_from_slice(&request.features.offered.to_be_bytes());
    transcript.extend_from_slice(&request.features.required.to_be_bytes());
    debug_assert!(transcript.len() <= MAX_ATTACH_TRANSCRIPT_BYTES);
    transcript
}

fn encode_standby_register_transcript(
    request: &StandbyRegistrationRequest,
    binding: &AttachTransportBinding,
) -> Vec<u8> {
    let installed = request.installed;
    let mut transcript = Vec::with_capacity(MAX_STANDBY_REGISTER_TRANSCRIPT_BYTES);
    transcript.extend_from_slice(STANDBY_REGISTER_PROTOCOL_CONTEXT);
    transcript.extend_from_slice(&FRAME_PROTOCOL_VERSION.to_be_bytes());
    transcript.extend_from_slice(installed.session_id.as_bytes());
    transcript.extend_from_slice(&installed.current_generation.get().to_be_bytes());
    transcript.extend_from_slice(&installed.selected_version.to_be_bytes());
    transcript.extend_from_slice(&installed.features.bits().to_be_bytes());
    transcript.extend_from_slice(installed.owner_identity.as_bytes());
    transcript.push(installed.alpn.len);
    transcript.extend_from_slice(installed.alpn.as_bytes());
    transcript.extend_from_slice(installed.device_principal.as_bytes());
    transcript.extend_from_slice(binding.owner_identity.as_bytes());
    transcript.push(binding.alpn.len);
    transcript.extend_from_slice(binding.alpn.as_bytes());
    transcript.extend_from_slice(binding.exporter.as_bytes());
    transcript.extend_from_slice(binding.device_principal.as_bytes());
    transcript.extend_from_slice(request.nonce.as_bytes());
    debug_assert!(transcript.len() <= MAX_STANDBY_REGISTER_TRANSCRIPT_BYTES);
    transcript
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum AttachConfigError {
    #[error("device secret must not be all-zero")]
    ZeroDeviceSecret,
    #[error("resume secret must not be all-zero")]
    ZeroResumeSecret,
    #[error("device and resume secrets must be independent")]
    SecretsNotIndependent,
    #[error("owner identity must not be all-zero")]
    ZeroOwnerIdentity,
    #[error("device principal must not be all-zero")]
    ZeroDevicePrincipal,
    #[error("TLS exporter binding must not be all-zero")]
    ZeroExporterBinding,
    #[error("attach ALPN must not be empty")]
    EmptyAlpn,
    #[error("attach ALPN length {len} exceeds {max}")]
    AlpnTooLong { len: usize, max: usize },
    #[error("invalid version range {min}..={max}")]
    InvalidVersionRange { min: u16, max: u16 },
    #[error("required feature bits {required:#x} are not included in offered bits {offered:#x}")]
    RequiredFeaturesNotOffered { offered: u64, required: u64 },
    #[error("unsupported protocol version {0}")]
    UnsupportedProtocolVersion(u16),
    #[error("HMAC rejected a fixed-size key")]
    InvalidHmacKey,
    #[error("HMAC output did not form a legal attach proof: {0}")]
    InvalidProofEncoding(ProtocolError),
    #[error("HMAC output did not form a legal standby registration proof: {0}")]
    InvalidStandbyProofEncoding(ProtocolError),
}

/// Public standby-authentication failure.  All malformed, unknown, stale, and
/// unauthenticated inputs are deliberately indistinguishable.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum StandbyRegistrationReject {
    #[error("standby registration rejected")]
    Rejected,
}

/// Public attach failure.  Unknown session and invalid authentication are
/// deliberately indistinguishable.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum AttachReject {
    #[error("attach rejected")]
    Rejected,
    #[error("requested leg generation {requested} is not current {current} plus one")]
    GenerationNotNext { current: u64, requested: u64 },
    #[error("leg generation is exhausted")]
    GenerationExhausted,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resumable::{
        ReceiveBudgetLimits, ReplayBudgetLimits, SessionConfig, SessionRole, TcpWindowLimits,
    };
    use std::sync::{Arc, Barrier};

    const SUPPORTED_FEATURES: u64 = 0b1111;

    fn session(byte: u8) -> SessionId {
        SessionId::new([byte; 16]).unwrap()
    }

    fn nonce(byte: u8) -> AttachNonce {
        AttachNonce::new([byte; 16]).unwrap()
    }

    fn owner(byte: u8) -> OwnerIdentity {
        OwnerIdentity::new([byte; 32]).unwrap()
    }

    fn principal(byte: u8) -> DevicePrincipal {
        DevicePrincipal::new([byte; 16]).unwrap()
    }

    fn exporter(byte: u8) -> TlsExporterBinding {
        TlsExporterBinding::new([byte; 32]).unwrap()
    }

    fn credentials() -> AttachCredentials {
        AttachCredentials::new(
            DeviceSecret::new([0xde; 32]).unwrap(),
            ResumeSecret::new([0xad; 32]).unwrap(),
        )
        .unwrap()
    }

    fn signer() -> AttachCredentials {
        credentials()
    }

    fn binding() -> AttachTransportBinding {
        binding_with_exporter(0x42)
    }

    fn binding_with_exporter(exporter_byte: u8) -> AttachTransportBinding {
        AttachTransportBinding::new(
            owner(0x31),
            AttachAlpn::new(b"mini-vpn-owned/1").unwrap(),
            exporter(exporter_byte),
            principal(0x53),
        )
    }

    fn policy() -> AttachPolicy {
        let binding = binding();
        AttachPolicy::new(
            binding.owner_identity(),
            binding.alpn(),
            binding.device_principal(),
            SESSION_PROTOCOL_VERSION,
            SUPPORTED_FEATURES,
        )
        .unwrap()
    }

    fn request(
        session_id: SessionId,
        generation: u64,
        nonce_byte: u8,
        versions: VersionRange,
        features: FeatureOffer,
    ) -> AttachRequest {
        AttachRequest::new(
            session_id,
            LegGeneration::new(generation).unwrap(),
            nonce(nonce_byte),
            versions,
            features,
        )
    }

    fn normal_request() -> AttachRequest {
        request(
            session(0x11),
            2,
            0x22,
            VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION).unwrap(),
            FeatureOffer::new(0b0111, 0b0001).unwrap(),
        )
    }

    fn authority_at(generation: u64) -> AttachAuthority {
        AttachAuthority::new(
            session(0x11),
            LegGeneration::new(generation).unwrap(),
            credentials(),
            policy(),
        )
    }

    fn standby_authority_and_installed() -> (
        AttachAuthority,
        CommittedLeg,
        AttachTransportBinding,
        AttachTransportBinding,
    ) {
        let negotiated_features = STANDBY_CONTROL_V1.bits() | 0b0111;
        let binding_a = binding_with_exporter(0x42);
        let authority = AttachAuthority::new(
            session(0x11),
            LegGeneration::new(1).unwrap(),
            credentials(),
            AttachPolicy::new(
                binding_a.owner_identity(),
                binding_a.alpn(),
                binding_a.device_principal(),
                SESSION_PROTOCOL_VERSION,
                negotiated_features,
            )
            .unwrap(),
        );
        let attach = request(
            session(0x11),
            2,
            0x22,
            VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION).unwrap(),
            FeatureOffer::new(negotiated_features, STANDBY_CONTROL_V1.bits()).unwrap(),
        );
        let installed = authority
            .verify_and_commit(
                &attach,
                &binding_a,
                &signer().prove(&attach, &binding_a).unwrap(),
            )
            .unwrap();
        (authority, installed, binding_a, binding_with_exporter(0x99))
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

    fn owner_model_at_generation_two() -> SessionModel {
        let authority = authority_at(1);
        let request = normal_request();
        let binding = binding();
        let proof = signer().prove(&request, &binding).unwrap();
        let leg = authority
            .verify_and_commit(&request, &binding, &proof)
            .unwrap();
        SessionModel::new(SessionRole::Owner, session_config(), leg)
    }

    #[test]
    fn authenticated_attach_commits_exact_next_generation() {
        let authority = authority_at(1);
        let request = normal_request();
        let binding = binding();
        let proof = signer().prove(&request, &binding).unwrap();

        let committed = authority
            .verify_and_commit(&request, &binding, &proof)
            .unwrap();

        assert_eq!(committed.session_id(), session(0x11));
        assert_eq!(committed.generation().get(), 2);
        assert_eq!(committed.nonce(), nonce(0x22));
        assert_eq!(committed.owner_identity(), owner(0x31));
        assert_eq!(committed.device_principal(), principal(0x53));
        assert_eq!(
            committed.session_protocol_version(),
            SESSION_PROTOCOL_VERSION
        );
        assert_eq!(committed.negotiated_features(), 0b0111);
        assert_eq!(authority.current_generation(), 2);
    }

    #[test]
    fn authenticated_standby_registers_a_distinct_b_exporter_without_advancing_generation() {
        let (authority, installed, binding_a, binding_b) = standby_authority_and_installed();
        assert_ne!(binding_a, binding_b);
        let standby = StandbyRegistrationRequest::from_installed_leg(
            &installed,
            StandbyNonce::new([0x33; 16]).unwrap(),
        )
        .unwrap();
        let standby_proof = signer()
            .prove_standby_registration(&standby, &binding_b)
            .unwrap();
        let frame = standby.to_frame(standby_proof).unwrap();
        let before = authority.current_generation();

        let authenticated = authority
            .verify_standby_registration_frame(&installed, &binding_b, &frame)
            .unwrap();

        assert_eq!(authenticated.session_id(), installed.session_id());
        assert_eq!(authenticated.current_generation(), installed.generation());
        assert_eq!(
            authenticated.standby_nonce(),
            StandbyNonce::new([0x33; 16]).unwrap()
        );
        assert_eq!(authenticated.selected_version(), SESSION_PROTOCOL_VERSION);
        assert_eq!(
            authenticated.features().bits(),
            STANDBY_CONTROL_V1.bits() | 0b0111
        );
        assert_eq!(authenticated.transport_binding(), binding_b);
        assert_eq!(authority.current_generation(), before);
    }

    #[test]
    fn standby_authentication_debug_redacts_all_identity_and_correlation_material() {
        let (authority, installed, _binding_a, binding_b) = standby_authority_and_installed();
        let standby = StandbyRegistrationRequest::from_installed_leg(
            &installed,
            StandbyNonce::new([0x33; 16]).unwrap(),
        )
        .unwrap();
        let proof = signer()
            .prove_standby_registration(&standby, &binding_b)
            .unwrap();
        let authenticated = authority
            .verify_standby_registration_frame(
                &installed,
                &binding_b,
                &standby.to_frame(proof).unwrap(),
            )
            .unwrap();

        for debug in [format!("{standby:?}"), format!("{authenticated:?}")] {
            assert!(debug.contains("[REDACTED]"));
            assert!(!debug.contains("mini-vpn-owned/1"));
            assert!(!debug.contains("1111111111111111"));
            assert!(!debug.contains("3333333333333333"));
            assert!(!debug.contains("9999999999999999"));
        }
        assert_eq!(format!("{proof:?}"), "StandbyProof([REDACTED])");
    }

    #[test]
    fn every_standby_transcript_field_mutation_has_one_public_rejection() {
        let (authority, installed, _binding_a, binding_b) = standby_authority_and_installed();
        let standby = StandbyRegistrationRequest::from_installed_leg(
            &installed,
            StandbyNonce::new([0x33; 16]).unwrap(),
        )
        .unwrap();
        let proof = signer()
            .prove_standby_registration(&standby, &binding_b)
            .unwrap();
        let original_features = FeatureSet::new(installed.negotiated_features());
        let register = |generation, session_id, standby_nonce, selected_version, features| {
            LegControlFrame::try_new(
                LegGeneration::new(generation).unwrap(),
                LegControlRecord::StandbyRegister {
                    session_id,
                    standby_nonce,
                    selected_version,
                    features,
                    proof,
                },
                original_features,
            )
            .unwrap()
        };
        let changed_frames = [
            register(
                2,
                session(0x99),
                StandbyNonce::new([0x33; 16]).unwrap(),
                SESSION_PROTOCOL_VERSION,
                original_features,
            ),
            register(
                3,
                installed.session_id(),
                StandbyNonce::new([0x33; 16]).unwrap(),
                SESSION_PROTOCOL_VERSION,
                original_features,
            ),
            register(
                2,
                installed.session_id(),
                StandbyNonce::new([0x34; 16]).unwrap(),
                SESSION_PROTOCOL_VERSION,
                original_features,
            ),
            register(
                2,
                installed.session_id(),
                StandbyNonce::new([0x33; 16]).unwrap(),
                SESSION_PROTOCOL_VERSION + 1,
                original_features,
            ),
            register(
                2,
                installed.session_id(),
                StandbyNonce::new([0x33; 16]).unwrap(),
                SESSION_PROTOCOL_VERSION,
                FeatureSet::new(original_features.bits() & !0b0010),
            ),
            register(
                2,
                installed.session_id(),
                StandbyNonce::new([0x33; 16]).unwrap(),
                SESSION_PROTOCOL_VERSION,
                FeatureSet::new(original_features.bits() | 0x20),
            ),
        ];
        for changed in changed_frames {
            assert_eq!(
                authority.verify_standby_registration_frame(&installed, &binding_b, &changed,),
                Err(StandbyRegistrationReject::Rejected)
            );
            assert_eq!(authority.current_generation(), 2);
        }

        let wrong_kind = LegControlFrame::try_new(
            installed.generation(),
            LegControlRecord::StandbyAccepted {
                session_id: installed.session_id(),
                standby_nonce: StandbyNonce::new([0x33; 16]).unwrap(),
                selected_version: SESSION_PROTOCOL_VERSION,
                features: original_features,
            },
            original_features,
        )
        .unwrap();
        assert_eq!(
            authority.verify_standby_registration_frame(&installed, &binding_b, &wrong_kind),
            Err(StandbyRegistrationReject::Rejected)
        );
        assert_eq!(authority.current_generation(), 2);

        let changed_bindings = [
            AttachTransportBinding::new(
                owner(0x99),
                binding_b.alpn(),
                exporter(0x99),
                binding_b.device_principal(),
            ),
            AttachTransportBinding::new(
                binding_b.owner_identity(),
                AttachAlpn::new(b"other-alpn/1").unwrap(),
                exporter(0x99),
                binding_b.device_principal(),
            ),
            binding_with_exporter(0x98),
            AttachTransportBinding::new(
                binding_b.owner_identity(),
                binding_b.alpn(),
                exporter(0x99),
                principal(0x99),
            ),
        ];
        let frame = standby.to_frame(proof).unwrap();
        for changed in changed_bindings {
            assert_eq!(
                authority.verify_standby_registration_frame(&installed, &changed, &frame),
                Err(StandbyRegistrationReject::Rejected)
            );
            assert_eq!(authority.current_generation(), 2);
        }

        let installed_binding = installed.transport_binding();
        let changed_installed = [
            CommittedLeg {
                session_id: session(0x99),
                ..installed
            },
            CommittedLeg {
                generation: LegGeneration::new(3).unwrap(),
                ..installed
            },
            CommittedLeg {
                transport_binding: AttachTransportBinding::new(
                    owner(0x99),
                    installed_binding.alpn(),
                    exporter(0x42),
                    installed_binding.device_principal(),
                ),
                ..installed
            },
            CommittedLeg {
                transport_binding: AttachTransportBinding::new(
                    installed_binding.owner_identity(),
                    AttachAlpn::new(b"other-alpn/1").unwrap(),
                    exporter(0x42),
                    installed_binding.device_principal(),
                ),
                ..installed
            },
            CommittedLeg {
                transport_binding: AttachTransportBinding::new(
                    installed_binding.owner_identity(),
                    installed_binding.alpn(),
                    exporter(0x42),
                    principal(0x99),
                ),
                ..installed
            },
            CommittedLeg {
                session_protocol_version: SESSION_PROTOCOL_VERSION + 1,
                ..installed
            },
            CommittedLeg {
                negotiated_features: original_features.bits() | 0x20,
                ..installed
            },
        ];
        for changed in changed_installed {
            assert_eq!(
                authority.verify_standby_registration_frame(&changed, &binding_b, &frame),
                Err(StandbyRegistrationReject::Rejected)
            );
            assert_eq!(authority.current_generation(), 2);
        }
    }

    #[test]
    fn attach_and_standby_proof_bytes_cannot_cross_authentication_domains() {
        let (authority, installed, _binding_a, binding_b) = standby_authority_and_installed();
        let standby = StandbyRegistrationRequest::from_installed_leg(
            &installed,
            StandbyNonce::new([0x33; 16]).unwrap(),
        )
        .unwrap();
        let standby_proof = signer()
            .prove_standby_registration(&standby, &binding_b)
            .unwrap();
        let attach = request(
            installed.session_id(),
            3,
            0x44,
            VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION).unwrap(),
            FeatureOffer::new(installed.negotiated_features(), STANDBY_CONTROL_V1.bits()).unwrap(),
        );
        let attach_proof = signer().prove(&attach, &binding_b).unwrap();

        let standby_from_attach = StandbyProof::new(*attach_proof.as_bytes()).unwrap();
        assert_eq!(
            authority.verify_standby_registration_frame(
                &installed,
                &binding_b,
                &standby.to_frame(standby_from_attach).unwrap(),
            ),
            Err(StandbyRegistrationReject::Rejected)
        );

        let attach_from_standby = AttachProof::new(*standby_proof.as_bytes()).unwrap();
        assert_eq!(
            authority.verify_and_commit(&attach, &binding_b, &attach_from_standby),
            Err(AttachReject::Rejected)
        );
        assert_eq!(authority.current_generation(), 2);
    }

    #[test]
    fn standby_transcript_and_nested_hmac_match_independent_known_answer() {
        // Generated independently with Python stdlib `hmac`/`hashlib` from
        // the documented field order and standby-only domain.
        const TRANSCRIPT_HEX: &str = "6d696e695f76706e2f726573756d61626c652f7374616e6462792d72656769737465722f76310001111111111111111111111111111111110000000000000002000100000000000000173131313131313131313131313131313131313131313131313131313131313131106d696e692d76706e2d6f776e65642f31535353535353535353535353535353533131313131313131313131313131313131313131313131313131313131313131106d696e692d76706e2d6f776e65642f3199999999999999999999999999999999999999999999999999999999999999995353535353535353535353535353535333333333333333333333333333333333";
        const PROOF_HEX: &str = "378a10fbc2fe0980de9709056343a48c4488baaf064ade5e86fd43d097aa817a";
        let (_authority, installed, _binding_a, binding_b) = standby_authority_and_installed();
        let standby = StandbyRegistrationRequest::from_installed_leg(
            &installed,
            StandbyNonce::new([0x33; 16]).unwrap(),
        )
        .unwrap();

        assert_eq!(
            encode_standby_register_transcript(&standby, &binding_b),
            decode_hex(TRANSCRIPT_HEX)
        );
        assert_eq!(
            signer()
                .prove_standby_registration(&standby, &binding_b)
                .unwrap()
                .as_bytes()
                .as_slice(),
            decode_hex(PROOF_HEX)
        );
    }

    #[test]
    fn every_bound_transcript_field_invalidates_an_existing_proof() {
        let authority = authority_at(1);
        let original_request = normal_request();
        let original_binding = binding();
        let proof = signer()
            .prove(&original_request, &original_binding)
            .unwrap();

        let changed_requests = [
            request(
                session(0x99),
                2,
                0x22,
                original_request.versions(),
                original_request.features(),
            ),
            request(
                original_request.session_id(),
                3,
                0x22,
                original_request.versions(),
                original_request.features(),
            ),
            request(
                original_request.session_id(),
                2,
                0x99,
                original_request.versions(),
                original_request.features(),
            ),
            request(
                original_request.session_id(),
                2,
                0x22,
                VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION + 1).unwrap(),
                original_request.features(),
            ),
            request(
                original_request.session_id(),
                2,
                0x22,
                original_request.versions(),
                FeatureOffer::new(0b1111, 0b0001).unwrap(),
            ),
            request(
                original_request.session_id(),
                2,
                0x22,
                original_request.versions(),
                FeatureOffer::new(0b0111, 0b0010).unwrap(),
            ),
        ];
        for changed in changed_requests {
            assert_eq!(
                authority.verify_and_commit(&changed, &original_binding, &proof),
                Err(AttachReject::Rejected)
            );
            assert_eq!(authority.current_generation(), 1);
        }

        let changed_bindings = [
            AttachTransportBinding::new(
                owner(0x99),
                original_binding.alpn(),
                exporter(0x42),
                original_binding.device_principal(),
            ),
            AttachTransportBinding::new(
                original_binding.owner_identity(),
                AttachAlpn::new(b"other-alpn/1").unwrap(),
                exporter(0x42),
                original_binding.device_principal(),
            ),
            AttachTransportBinding::new(
                original_binding.owner_identity(),
                original_binding.alpn(),
                exporter(0x99),
                original_binding.device_principal(),
            ),
            AttachTransportBinding::new(
                original_binding.owner_identity(),
                original_binding.alpn(),
                exporter(0x42),
                principal(0x99),
            ),
        ];
        for changed in changed_bindings {
            assert_eq!(
                authority.verify_and_commit(&original_request, &changed, &proof),
                Err(AttachReject::Rejected)
            );
            assert_eq!(authority.current_generation(), 1);
        }

        assert!(
            authority
                .verify_and_commit(&original_request, &original_binding, &proof)
                .is_ok()
        );
    }

    #[test]
    fn unknown_session_and_bad_proof_have_the_same_public_rejection() {
        let authority = authority_at(1);
        let binding = binding();
        let normal = normal_request();
        let unknown = request(session(0x99), 2, 0x22, normal.versions(), normal.features());
        let unknown_proof = signer().prove(&unknown, &binding).unwrap();
        let bad_proof = AttachProof::new([0x77; 32]).unwrap();

        assert_eq!(
            authority.verify_and_commit(&unknown, &binding, &unknown_proof),
            Err(AttachReject::Rejected)
        );
        assert_eq!(
            authority.verify_and_commit(&normal, &binding, &bad_proof),
            Err(AttachReject::Rejected)
        );
        assert_eq!(
            authority.verify_and_commit_frame(&unknown.to_attach_frame(unknown_proof), &binding),
            Err(AttachReject::Rejected)
        );
        assert_eq!(
            authority.verify_and_commit_frame(&normal.to_attach_frame(bad_proof), &binding),
            Err(AttachReject::Rejected)
        );
        assert_eq!(authority.current_generation(), 1);
    }

    #[test]
    fn both_independent_secret_authorities_are_required() {
        let authority = authority_at(1);
        let request = normal_request();
        let binding = binding();
        let wrong_device = AttachCredentials::new(
            DeviceSecret::new([0xdf; 32]).unwrap(),
            ResumeSecret::new([0xad; 32]).unwrap(),
        )
        .unwrap();
        let wrong_resume = AttachCredentials::new(
            DeviceSecret::new([0xde; 32]).unwrap(),
            ResumeSecret::new([0xae; 32]).unwrap(),
        )
        .unwrap();

        for proof in [
            wrong_device.prove(&request, &binding).unwrap(),
            wrong_resume.prove(&request, &binding).unwrap(),
        ] {
            assert_eq!(
                authority.verify_and_commit(&request, &binding, &proof),
                Err(AttachReject::Rejected)
            );
            assert_eq!(authority.current_generation(), 1);
        }
    }

    #[test]
    fn concurrent_same_generation_attach_commits_once_and_replay_is_stale() {
        let authority = Arc::new(authority_at(1));
        let request = normal_request();
        let binding = binding();
        let proof = signer().prove(&request, &binding).unwrap();
        let barrier = Arc::new(Barrier::new(3));

        let results = std::thread::scope(|scope| {
            let mut joins = Vec::new();
            for _ in 0..2 {
                let authority = authority.clone();
                let barrier = barrier.clone();
                joins.push(scope.spawn(move || {
                    barrier.wait();
                    authority.verify_and_commit(&request, &binding, &proof)
                }));
            }
            barrier.wait();
            joins
                .into_iter()
                .map(|join| join.join().unwrap())
                .collect::<Vec<_>>()
        });

        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(result, Err(AttachReject::GenerationNotNext { .. })))
                .count(),
            1
        );
        assert_eq!(authority.current_generation(), 2);
        assert!(matches!(
            authority.verify_and_commit(&request, &binding, &proof),
            Err(AttachReject::GenerationNotNext {
                current: 2,
                requested: 2
            })
        ));
    }

    #[test]
    fn concurrent_preflighted_attaches_have_exactly_one_commit_winner() {
        let authority = Arc::new(authority_at(2));
        let request = request(
            session(0x11),
            3,
            0x33,
            VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION).unwrap(),
            FeatureOffer::new(0b0111, 0b0001).unwrap(),
        );
        let binding = binding();
        let proof = signer().prove(&request, &binding).unwrap();
        let barrier = Arc::new(Barrier::new(3));
        let mut model_a = owner_model_at_generation_two();
        let mut model_b = owner_model_at_generation_two();

        let results = std::thread::scope(|scope| {
            let authority_a = Arc::clone(&authority);
            let barrier_a = Arc::clone(&barrier);
            let model_a = &mut model_a;
            let attempt_a = scope.spawn(move || {
                authority_a.preflight_model_and_commit_with_hook(
                    &request.to_attach_frame(proof),
                    &binding,
                    model_a,
                    || {
                        // Both proof-valid candidates and both real model
                        // tokens exist before either authority CAS proceeds.
                        barrier_a.wait();
                    },
                )
            });
            let authority_b = Arc::clone(&authority);
            let barrier_b = Arc::clone(&barrier);
            let model_b = &mut model_b;
            let attempt_b = scope.spawn(move || {
                authority_b.preflight_model_and_commit_with_hook(
                    &request.to_attach_frame(proof),
                    &binding,
                    model_b,
                    || {
                        barrier_b.wait();
                    },
                )
            });
            barrier.wait();
            [attempt_a.join().unwrap(), attempt_b.join().unwrap()]
        });

        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(result, Ok(Ok(Ok((_committed, _recovery))))))
                .count(),
            1
        );
        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(result, Ok(Err(_status))))
                .count(),
            1
        );
        assert_eq!(authority.current_generation(), 3);
        assert_eq!(
            [
                model_a.snapshot().generation().get(),
                model_b.snapshot().generation().get()
            ]
            .into_iter()
            .filter(|generation| *generation == 3)
            .count(),
            1
        );
    }

    #[test]
    fn owner_attach_transaction_rejects_client_model_before_authority_commit() {
        let authority = authority_at(2);
        let request = request(
            session(0x11),
            3,
            0x33,
            VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION).unwrap(),
            FeatureOffer::new(0b0111, 0b0001).unwrap(),
        );
        let binding = binding();
        let proof = signer().prove(&request, &binding).unwrap();
        let owner_model = owner_model_at_generation_two();
        let mut model = SessionModel::new(
            SessionRole::Client,
            session_config(),
            owner_model.current_leg(),
        );
        let before = model.snapshot();

        let outcome = authority
            .preflight_model_and_commit_or_resynchronize_frame(
                &request.to_attach_frame(proof),
                &binding,
                &mut model,
            )
            .unwrap()
            .unwrap();

        assert_eq!(
            outcome,
            Err(SessionError::RoleCannotAcceptOwnerAttach(
                SessionRole::Client
            ))
        );
        assert_eq!(authority.current_generation(), 2);
        assert_eq!(model.snapshot(), before);
    }

    #[test]
    fn stale_and_skipped_generations_fail_without_mutation() {
        let authority = authority_at(7);
        let binding = binding();
        for generation in [7, 9] {
            let request = request(
                session(0x11),
                generation,
                generation as u8,
                VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION).unwrap(),
                FeatureOffer::new(1, 1).unwrap(),
            );
            let proof = signer().prove(&request, &binding).unwrap();
            assert!(matches!(
                authority.verify_and_commit(&request, &binding, &proof),
                Err(AttachReject::GenerationNotNext {
                    current: 7,
                    requested
                }) if requested == generation
            ));
            assert_eq!(authority.current_generation(), 7);
        }
    }

    #[test]
    fn generation_overflow_fails_without_mutation() {
        let authority = authority_at(u64::MAX);
        let binding = binding();
        let request = request(
            session(0x11),
            u64::MAX,
            0x44,
            VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION).unwrap(),
            FeatureOffer::new(1, 1).unwrap(),
        );
        let proof = signer().prove(&request, &binding).unwrap();

        assert_eq!(
            authority.verify_and_commit(&request, &binding, &proof),
            Err(AttachReject::GenerationExhausted)
        );
        assert_eq!(authority.current_generation(), u64::MAX);
    }

    #[test]
    fn secrets_exporter_and_authority_debug_are_redacted() {
        let device = DeviceSecret::new([0xde; 32]).unwrap();
        let resume = ResumeSecret::new([0xad; 32]).unwrap();
        assert_eq!(format!("{device:?}"), "DeviceSecret([REDACTED])");
        assert_eq!(format!("{resume:?}"), "ResumeSecret([REDACTED])");
        assert_eq!(
            format!("{:?}", exporter(0x42)),
            "TlsExporterBinding([REDACTED])"
        );

        let authority = authority_at(1);
        let debug = format!("{authority:?}");
        assert!(debug.contains("device_secret: DeviceSecret([REDACTED])"));
        assert!(debug.contains("resume_secret: ResumeSecret([REDACTED])"));
        assert!(!debug.contains("222"));
        assert!(!debug.contains("173"));
    }

    #[test]
    fn attach_inputs_are_bounded_and_secrets_must_be_independent() {
        assert_eq!(
            AttachAlpn::new(&[b'x'; MAX_ATTACH_ALPN_BYTES + 1]),
            Err(AttachConfigError::AlpnTooLong {
                len: MAX_ATTACH_ALPN_BYTES + 1,
                max: MAX_ATTACH_ALPN_BYTES
            })
        );
        assert_eq!(
            FeatureOffer::new(0b0001, 0b0010),
            Err(AttachConfigError::RequiredFeaturesNotOffered {
                offered: 0b0001,
                required: 0b0010
            })
        );
        assert!(matches!(
            AttachCredentials::new(
                DeviceSecret::new([0x55; 32]).unwrap(),
                ResumeSecret::new([0x55; 32]).unwrap(),
            ),
            Err(AttachConfigError::SecretsNotIndependent)
        ));
        assert!(
            encode_transcript(&normal_request(), &binding()).len() <= MAX_ATTACH_TRANSCRIPT_BYTES
        );
    }

    #[test]
    fn attach_wire_and_auth_models_form_one_exact_end_to_end_negotiation() {
        let authority = authority_at(1);
        let request = normal_request();
        let binding = binding();
        let proof = signer().prove(&request, &binding).unwrap();

        let encoded_attach = request.to_attach_frame(proof).encode().unwrap();
        let decoded_attach = Frame::decode_owned_exact(encoded_attach).unwrap();
        let (wire_request, wire_proof) = AttachRequest::from_attach_frame(&decoded_attach).unwrap();
        assert_eq!(wire_request, request);
        assert_eq!(wire_proof, proof);

        let server_leg = authority
            .verify_and_commit_frame(&decoded_attach, &binding)
            .unwrap();
        let encoded_accepted = server_leg.attach_accepted_frame().encode().unwrap();
        let decoded_accepted = Frame::decode_owned_exact(encoded_accepted).unwrap();
        let client_leg = request
            .validate_accepted_frame(&binding, &decoded_accepted)
            .unwrap();

        assert_eq!(client_leg, server_leg);
        assert_eq!(client_leg.transport_binding(), binding);
        assert_eq!(authority.current_generation(), 2);
    }

    #[test]
    fn server_frame_entry_authenticates_the_exact_decoded_attach_fields() {
        let authority = authority_at(1);
        let original = normal_request();
        let binding = binding();
        let proof = signer().prove(&original, &binding).unwrap();

        let changed = request(
            original.session_id(),
            original.requested_generation().get(),
            0x99,
            original.versions(),
            original.features(),
        );
        let changed_frame =
            Frame::decode_owned_exact(changed.to_attach_frame(proof).encode().unwrap()).unwrap();
        assert_eq!(
            authority.verify_and_commit_frame(&changed_frame, &binding),
            Err(AttachReject::Rejected)
        );

        let wrong_kind = Frame::new(
            original.requested_generation(),
            Record::AttachAccepted {
                session_id: original.session_id(),
                nonce: original.nonce(),
                selected_version: SESSION_PROTOCOL_VERSION,
                features: FeatureSet::new(original.features().offered()),
            },
        );
        assert_eq!(
            authority.verify_and_commit_frame(&wrong_kind, &binding),
            Err(AttachReject::Rejected)
        );
        assert_eq!(authority.current_generation(), 1);
    }

    #[test]
    fn client_rejects_mutated_attach_acceptance_without_minting_a_leg() {
        let request = normal_request();
        let binding = binding();
        let accepted = |generation, selected_version, features| {
            Frame::new(
                LegGeneration::new(generation).unwrap(),
                Record::AttachAccepted {
                    session_id: request.session_id(),
                    nonce: request.nonce(),
                    selected_version,
                    features: FeatureSet::new(features),
                },
            )
        };

        let mutations = [
            accepted(3, SESSION_PROTOCOL_VERSION, 0b0111),
            accepted(2, SESSION_PROTOCOL_VERSION + 1, 0b0111),
            accepted(2, SESSION_PROTOCOL_VERSION, 0b1111),
            accepted(2, SESSION_PROTOCOL_VERSION, 0b0110),
            request.to_attach_frame(AttachProof::new([0x77; 32]).unwrap()),
        ];
        for mutation in mutations {
            assert_eq!(
                request.validate_accepted_frame(&binding, &mutation),
                Err(AttachReject::Rejected)
            );
        }
    }

    #[test]
    fn attach_acceptance_requires_exact_session_and_nonce_correlation() {
        let request = normal_request();
        let binding = binding();
        let accepted_for = |session_id, accepted_nonce| {
            Frame::new(
                request.requested_generation(),
                Record::AttachAccepted {
                    session_id,
                    nonce: accepted_nonce,
                    selected_version: SESSION_PROTOCOL_VERSION,
                    features: FeatureSet::new(request.features().offered()),
                },
            )
        };

        assert!(
            request
                .validate_accepted_frame(
                    &binding,
                    &accepted_for(request.session_id(), request.nonce()),
                )
                .is_ok()
        );
        assert_eq!(
            request
                .validate_accepted_frame(&binding, &accepted_for(session(0x99), request.nonce()),),
            Err(AttachReject::Rejected)
        );
        assert_eq!(
            request.validate_accepted_frame(
                &binding,
                &accepted_for(request.session_id(), nonce(0x99)),
            ),
            Err(AttachReject::Rejected)
        );
    }

    #[test]
    fn lost_attach_request_retries_exact_next_generation_without_resynchronizing() {
        let authority = authority_at(1);
        let lost = normal_request();
        let replacement_binding = binding_with_exporter(0x43);
        let retry = request(
            lost.session_id(),
            lost.requested_generation().get(),
            0x23,
            lost.versions(),
            lost.features(),
        );
        let proof = signer().prove(&retry, &replacement_binding).unwrap();
        let decoded =
            Frame::decode_owned_exact(retry.to_attach_frame(proof).encode().unwrap()).unwrap();

        let outcome = authority
            .verify_and_commit_or_resynchronize_frame(&decoded, &replacement_binding)
            .unwrap();
        let AttachFrameOutcome::Committed(server_leg) = outcome else {
            panic!("an unobserved request must not advance server generation");
        };
        let client_leg = retry
            .validate_accepted_frame(&replacement_binding, &server_leg.attach_accepted_frame())
            .unwrap();

        assert_eq!(client_leg, server_leg);
        assert_eq!(authority.current_generation(), 2);
    }

    #[test]
    fn lost_attach_response_resynchronizes_then_commits_exact_next_generation() {
        let authority = authority_at(1);
        let original = normal_request();
        let original_binding = binding();
        let original_proof = signer().prove(&original, &original_binding).unwrap();
        let original_frame =
            Frame::decode_owned_exact(original.to_attach_frame(original_proof).encode().unwrap())
                .unwrap();
        let first = authority
            .verify_and_commit_or_resynchronize_frame(&original_frame, &original_binding)
            .unwrap();
        assert!(matches!(first, AttachFrameOutcome::Committed(_)));
        assert_eq!(authority.current_generation(), 2);
        // The first ATTACH_ACCEPTED is deliberately lost here.

        let replacement_binding = binding_with_exporter(0x43);
        let retry = request(
            original.session_id(),
            original.requested_generation().get(),
            0x23,
            original.versions(),
            original.features(),
        );
        let retry_proof = signer().prove(&retry, &replacement_binding).unwrap();
        let retry_frame =
            Frame::decode_owned_exact(retry.to_attach_frame(retry_proof).encode().unwrap())
                .unwrap();
        let outcome = authority
            .verify_and_commit_or_resynchronize_frame(&retry_frame, &replacement_binding)
            .unwrap();
        let AttachFrameOutcome::Resynchronize(server_status) = outcome else {
            panic!("a proof-valid stale generation must return authenticated status");
        };
        assert_eq!(authority.current_generation(), 2);

        let decoded_status =
            Frame::decode_owned_exact(server_status.status_frame().encode().unwrap()).unwrap();
        let client_status = retry
            .validate_generation_status_frame(&replacement_binding, &decoded_status)
            .unwrap();
        assert_eq!(client_status, server_status);
        assert_eq!(client_status.current_generation().get(), 2);
        assert_eq!(client_status.transport_binding(), replacement_binding);

        let next = client_status.next_request(nonce(0x24)).unwrap();
        assert_eq!(next.requested_generation().get(), 3);
        let next_proof = signer().prove(&next, &replacement_binding).unwrap();
        let next_frame =
            Frame::decode_owned_exact(next.to_attach_frame(next_proof).encode().unwrap()).unwrap();
        let outcome = authority
            .verify_and_commit_or_resynchronize_frame(&next_frame, &replacement_binding)
            .unwrap();
        let AttachFrameOutcome::Committed(server_leg) = outcome else {
            panic!("the status-derived exact next generation must commit");
        };
        assert_eq!(authority.current_generation(), 3);
        let accepted =
            Frame::decode_owned_exact(server_leg.attach_accepted_frame().encode().unwrap())
                .unwrap();
        assert!(
            next.validate_accepted_frame(&replacement_binding, &accepted)
                .is_ok()
        );

        let stale = authority
            .verify_and_commit_or_resynchronize_frame(&retry_frame, &replacement_binding)
            .unwrap();
        assert!(matches!(stale, AttachFrameOutcome::Resynchronize(_)));
        assert_eq!(authority.current_generation(), 3);
    }

    #[test]
    fn catch_up_authority_binds_authenticated_high_water_across_successive_legs() {
        let far_ahead = authority_at(4);
        let original = normal_request();
        let status_binding = binding();
        let proof = signer().prove(&original, &status_binding).unwrap();
        let AttachFrameOutcome::Resynchronize(status) = far_ahead
            .verify_and_commit_or_resynchronize_frame(
                &original.to_attach_frame(proof),
                &status_binding,
            )
            .unwrap()
        else {
            panic!("a stale authenticated request must return status");
        };
        let client_status = original
            .validate_generation_status_frame(&status_binding, &status.status_frame())
            .unwrap();
        assert_eq!(client_status.current_generation().get(), 4);
        assert_eq!(client_status.requested_generation().get(), 2);
        let pending = client_status.begin_catch_up(nonce(0x61)).unwrap();
        let next = pending.request();
        assert_eq!(next.requested_generation().get(), 5);
        let accepted_binding = binding_with_exporter(0x99);
        let proof = signer().prove(&next, &accepted_binding).unwrap();
        let AttachFrameOutcome::Committed(accepted) = far_ahead
            .verify_and_commit_or_resynchronize_frame(
                &next.to_attach_frame(proof),
                &accepted_binding,
            )
            .unwrap()
        else {
            panic!("the authenticated high-water successor must commit");
        };
        let accepted_frame = accepted.attach_accepted_frame();
        let exact = pending
            .validate_accepted_frame(&accepted_binding, &accepted_frame)
            .unwrap();
        assert_eq!(exact.expected_local_generation().get(), 1);
        assert_eq!(exact.original_requested_generation().get(), 2);
        assert_eq!(exact.observed_generation().get(), 4);
        assert_eq!(exact.status_transport_binding, status_binding);
        assert_eq!(exact.accepted_leg().transport_binding(), accepted_binding);
        assert_eq!(exact.accepted_leg(), accepted);

        let impossible_status = Frame::new(
            LegGeneration::new(1).unwrap(),
            Record::AttachGenerationStatus {
                session_id: original.session_id(),
                requested_generation: original.requested_generation(),
                nonce: original.nonce(),
            },
        );
        let below_request = original
            .validate_generation_status_frame(&status_binding, &impossible_status)
            .unwrap();
        assert!(matches!(
            below_request.begin_catch_up(nonce(0x62)),
            Err(AttachReject::Rejected)
        ));
    }

    #[test]
    fn generation_status_requires_valid_auth_and_exact_request_correlation() {
        let authority = authority_at(2);
        let binding = binding();
        let original = normal_request();
        let valid_proof = signer().prove(&original, &binding).unwrap();
        let bad_proof = AttachProof::new([0x77; 32]).unwrap();
        let unknown = request(
            session(0x99),
            original.requested_generation().get(),
            0x22,
            original.versions(),
            original.features(),
        );
        let unknown_proof = signer().prove(&unknown, &binding).unwrap();

        assert_eq!(
            authority.verify_and_commit_or_resynchronize_frame(
                &original.to_attach_frame(bad_proof),
                &binding,
            ),
            Err(AttachReject::Rejected)
        );
        assert_eq!(
            authority.verify_and_commit_or_resynchronize_frame(
                &unknown.to_attach_frame(unknown_proof),
                &binding,
            ),
            Err(AttachReject::Rejected)
        );

        let AttachFrameOutcome::Resynchronize(status) = authority
            .verify_and_commit_or_resynchronize_frame(
                &original.to_attach_frame(valid_proof),
                &binding,
            )
            .unwrap()
        else {
            panic!("valid stale attach must receive generation status");
        };
        let status_frame = status.status_frame();
        let Record::AttachGenerationStatus {
            session_id,
            requested_generation,
            nonce: status_nonce,
        } = status_frame.record()
        else {
            panic!("status capability emitted the wrong record");
        };
        let mutations = [
            Frame::new(
                status.current_generation(),
                Record::AttachGenerationStatus {
                    session_id: session(0x99),
                    requested_generation: *requested_generation,
                    nonce: *status_nonce,
                },
            ),
            Frame::new(
                status.current_generation(),
                Record::AttachGenerationStatus {
                    session_id: *session_id,
                    requested_generation: LegGeneration::new(3).unwrap(),
                    nonce: *status_nonce,
                },
            ),
            Frame::new(
                status.current_generation(),
                Record::AttachGenerationStatus {
                    session_id: *session_id,
                    requested_generation: *requested_generation,
                    nonce: nonce(0x99),
                },
            ),
            Frame::new(
                status.current_generation(),
                Record::AttachAccepted {
                    session_id: original.session_id(),
                    nonce: original.nonce(),
                    selected_version: SESSION_PROTOCOL_VERSION,
                    features: FeatureSet::new(original.features().offered()),
                },
            ),
        ];
        for mutation in mutations {
            assert_eq!(
                original.validate_generation_status_frame(&binding, &mutation),
                Err(AttachReject::Rejected)
            );
        }
        assert_eq!(authority.current_generation(), 2);
    }

    #[test]
    fn exhausted_authenticated_status_cannot_derive_or_commit_a_generation() {
        let authority = authority_at(u64::MAX);
        let binding = binding();
        let original = request(
            session(0x11),
            u64::MAX,
            0x44,
            VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION).unwrap(),
            FeatureOffer::new(1, 1).unwrap(),
        );
        let proof = signer().prove(&original, &binding).unwrap();

        let AttachFrameOutcome::Resynchronize(status) = authority
            .verify_and_commit_or_resynchronize_frame(&original.to_attach_frame(proof), &binding)
            .unwrap()
        else {
            panic!("an exhausted proof-valid authority must return bounded status");
        };
        let decoded = Frame::decode_owned_exact(status.status_frame().encode().unwrap()).unwrap();
        let client_status = original
            .validate_generation_status_frame(&binding, &decoded)
            .unwrap();

        assert_eq!(client_status.current_generation().get(), u64::MAX);
        assert_eq!(
            client_status.next_request(nonce(0x45)),
            Err(AttachReject::GenerationExhausted)
        );
        assert_eq!(authority.current_generation(), u64::MAX);
    }

    #[test]
    fn generation_resynchronization_rejects_reusing_the_attach_nonce() {
        let authority = authority_at(2);
        let binding = binding();
        let original = normal_request();
        let proof = signer().prove(&original, &binding).unwrap();
        let AttachFrameOutcome::Resynchronize(status) = authority
            .verify_and_commit_or_resynchronize_frame(&original.to_attach_frame(proof), &binding)
            .unwrap()
        else {
            panic!("a proof-valid stale attach must return generation status");
        };

        assert_eq!(
            status.next_request(original.nonce()),
            Err(AttachReject::Rejected)
        );
        assert_eq!(authority.current_generation(), 2);
    }

    #[test]
    fn attach_transcript_and_nested_hmac_match_independent_known_answer() {
        // Generated once with Python stdlib `hmac`/`hashlib`; these literals do
        // not reuse this module's transcript or HMAC implementation.
        const TRANSCRIPT_HEX: &str = "6d696e695f76706e2f726573756d61626c652f6174746163682f763100013131313131313131313131313131313131313131313131313131313131313131106d696e692d76706e2d6f776e65642f31424242424242424242424242424242424242424242424242424242424242424253535353535353535353535353535353111111111111111111111111111111110000000000000002222222222222222222222222222222220001000100000000000000070000000000000001";
        const PROOF_HEX: &str = "48a657e42cdd46ed1334297189cb6982559a2e99c287cbfa746ef4d607bb7b3f";
        let request = normal_request();
        let binding = binding();

        assert_eq!(
            encode_transcript(&request, &binding),
            decode_hex(TRANSCRIPT_HEX)
        );
        let proof = signer().prove(&request, &binding).unwrap();
        assert_eq!(proof.as_bytes().as_slice(), decode_hex(PROOF_HEX));
    }

    // A verified standby registration is a consuming owner-side capability,
    // not a copyable wire fact. Ambiguous inference fails to compile if it
    // ever gains a `Clone` implementation.
    trait AmbiguousIfClone<Marker> {
        fn marker() {}
    }
    impl<T: ?Sized> AmbiguousIfClone<()> for T {}
    impl<T: Clone> AmbiguousIfClone<u8> for T {}
    const _: fn() = || {
        let _ = <AuthenticatedStandbyRegistration as AmbiguousIfClone<_>>::marker;
    };

    fn decode_hex(hex: &str) -> Vec<u8> {
        assert_eq!(hex.len() % 2, 0);
        hex.as_bytes()
            .chunks_exact(2)
            .map(|pair| {
                let pair = std::str::from_utf8(pair).unwrap();
                u8::from_str_radix(pair, 16).unwrap()
            })
            .collect()
    }
}
