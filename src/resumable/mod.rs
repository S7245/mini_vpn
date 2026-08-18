//! Transport-independent ownership model for Knife16 resumable Upstream sessions.
//!
//! The public interface contains protocol facts only. QUIC, Tokio, sockets,
//! timers, and the eventual `OwnedUpstream` adapter remain outside this module.

mod auth;
mod capacity;
mod protocol;
mod session;
mod tcp;

pub use auth::{
    AttachAlpn, AttachAuthority, AttachConfigError, AttachCredentials, AttachFrameOutcome,
    AttachPolicy, AttachReject, AttachRequest, AttachTransportBinding, CommittedLeg,
    DevicePrincipal, DeviceSecret, FeatureOffer, GenerationResynchronization,
    MAX_ATTACH_ALPN_BYTES, MAX_ATTACH_TRANSCRIPT_BYTES, OwnerIdentity, ResumeSecret,
    TlsExporterBinding, VersionRange,
};
pub use capacity::{
    BitsPerSecond, DirectionalByteLimits, DirectionalRates, DirectionalReplayStorageLimits,
    ReplayCapacityError, ReplayCapacityPlan, ReplayCapacitySpec, ReplayCopyAllocationBounds,
    ReplayHorizon, ReplayStorageGeometry, ReplayStorageLimit, ReplayStoragePlan,
};

pub use protocol::{
    AttachNonce, AttachProof, ByteOffset, Direction, FRAME_HEADER_BYTES, FRAME_PROTOCOL_VERSION,
    FeatureSet, Frame, LegGeneration, MAX_DATA_PAYLOAD_BYTES, OpenResultCode, ProtocolError,
    Record, ResetReason, SESSION_PROTOCOL_VERSION, SessionFlowId, SessionId, ValidatedFrameHeader,
};
pub use session::{
    FlowFinishReason, LocalFlow, PeerOpenRequest, ReceiveBudgetLimits, ReplayBudgetLimits,
    ReplayBudgetUsage, SessionConfig, SessionConfigError, SessionEffect, SessionError,
    SessionEvent, SessionFlowSnapshot, SessionModel, SessionPhase, SessionRole, SessionSnapshot,
    SinkHalfClose, SinkOffer, TerminalGrace,
};
pub use tcp::{
    TcpDataSegment, TcpOwnershipError, TcpReceiveAbandon, TcpReceiveAccept, TcpReceiveClose,
    TcpReceiveData, TcpReceiveDisposition, TcpReceiveSnapshot, TcpReceiveWindow,
    TcpSendAcknowledge, TcpSendAppend, TcpSendClose, TcpSendSnapshot, TcpSendWindow,
    TcpWindowLimits,
};
